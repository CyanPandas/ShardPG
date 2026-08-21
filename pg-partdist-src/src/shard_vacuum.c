/*
 * shard_vacuum.c
 *
 * 分片级 vacuum 的页面动作（设计 §6.4，P5 T5.3）。当前实现 ③ xmax 消毒
 * （T5.3a）。
 *
 * ★ 为什么不走内核的 heap_prepare_freeze_tuple（动手前的勘察结论）
 *
 * 设计 §6.4 ③ 原文说"即 heap_prepare_freeze_tuple 的 xmax 处置**换形态
 * 重现**"。勘察后确认"换形态"是必须的，不是可选的：原生
 * heap_prepare_freeze_tuple 从头到尾拿元组 xid 与 `VacuumCutoffs` 里的
 * **原生** relfrozenxid / OldestXmin 比较，并按 checkflags 去查**原生**
 * clog；分片元组的 xmin/xmax 是分片 xid，三处比较全是异宇宙比较，且
 * 结尾的 heap_tuple_should_freeze 还会把分片 xid 喂进 relfrozenxid
 * 跟踪器（污染原生回卷账本）。给它开分叉等于把整个函数改写。
 *
 * 而**执行侧**完全可以复用：`heap_freeze_execute_prepared()` 是 extern
 * 的，只吃一组已经算好的 HeapTupleFreeze 计划，负责改页 + 发内核原样的
 * XLOG_HEAP2_FREEZE_PAGE 记录。于是本模块自算计划、交它执行 ——
 * **策略在扩展、WAL 由内核发**。这样 follower 收到的就是内核标准记录，
 * heap2_redo 逐字节回放，不新增任何记录格式（§6.7 的硬要求）。
 *
 * ⇒ **T5.3a 不需要内核补丁。** 这修正了 T5.3 方案里"三类动作全部需要内核
 *   补丁"的判断（见 TX_TSO_MVCC_DEV_PLAN.md T5.3a 实施记要）。
 *
 * 另两处必须与内核保持一致的细节：
 *   - 清 xmax 的 infomask 变换逐字照抄 heap_prepare_freeze_tuple 的
 *     freeze_xmax 分支（清 HEAP_XMAX_BITS、置 HEAP_XMAX_INVALID、清
 *     HEAP_HOT_UPDATED / HEAP_KEYS_UPDATED）；
 *   - checkflags 恒 0。那两项检查（HEAP_FREEZE_CHECK_XMIN_COMMITTED /
 *     _XMAX_ABORTED）在 heap_freeze_execute_prepared 里查的是**原生**
 *     clog，喂分片 xid 进去是纯粹的误判源。
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/xloginsert.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "shard_clog.h"
#include "shard_vacuum.h"
#include "shard_xid.h"

/*
 * 两个页面动作的共同开场：认分片、拒绝无意义/后退的 trunc_before、取当前
 * 持久截断点。返回 false = 本趟无事可做（调用者直接返回）。
 */
static bool
shard_vacuum_begin(Relation rel, TransactionId trunc_before,
				   Oid *shard_out, TransactionId *cur_tb_out)
{
	Oid			relid = RelationGetRelid(rel);
	Oid			shard = ShardXidLookupByOid(relid);
	TransactionId cur_tb;
	TransactionId cur_vx;

	if (!OidIsValid(shard))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("关系 %u 不是分片打标表，无分片 vacuum 可做", relid)));

	/* 一条都不清：InvalidTransactionId / 小于首个可用号，直接返回 */
	if (!TransactionIdIsValid(trunc_before) || trunc_before <= FIRST_SHARD_XID)
		return false;

	ShardVacuumGetWatermarks(shard, &cur_tb, &cur_vx);

	/*
	 * 截断点不许后退：给一个比现状还小的 trunc_before 是调用者算错了，
	 * 静默照做会让"判不出来就不动"的分支吞掉本该报错的输入。
	 */
	if (TransactionIdIsValid(cur_tb) && trunc_before < cur_tb)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("分片 %u 的 trunc_before %u 小于当前截断点 %u",
						shard, trunc_before, cur_tb)));

	*shard_out = shard;
	*cur_tb_out = cur_tb;
	return true;
}

/*
 * 查一个**已确认落在截断点以下**的分片 xid 的判决，并处置"未决"这一格。
 *
 * 未决（RUNNING / PREPARED / 稀疏空洞）分两种，区别对待是要点：
 *   - 落在 [cur_tb, trunc_before) ⇒ ERROR。§6.3 的前缀扫描遇未决即停，
 *     调用者不可能算出跨过未决条目的 target；到这里说明 trunc_before 给错
 *     了，fail-closed 而不是"判不出来就不动"—— 后者会把本该报错的输入
 *     静默吞掉。
 *   - 落在 cur_tb 以下 ⇒ 照原样返回。其 clog 已被上一轮截断，判不出来了；
 *     这是过去时的既成事实（只可能来自上一轮违反顺序铁律），调用者一律
 *     按"不动"处理。
 */
static TxnStatus
shard_vacuum_status(Oid shard, TransactionId xid, const char *field,
					TransactionId trunc_before, TransactionId cur_tb)
{
	TxnStatus	st = ShardClogReadStatus(shard, xid);

	if (st != TXN_COMMITTED && st != TXN_ABORTED && xid >= cur_tb)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("分片 %u 的 %s %u 在 [%u, %u) 内仍未决（状态 %d）",
						shard, field, xid, cur_tb, trunc_before, (int) st),
				 errdetail("§6.3 的前缀扫描遇 RUNNING/PREPARED/空洞即停，"
						   "调用者不可能算出跨过未决条目的截断点。"),
				 errhint("trunc_before 应取 ShardVacuumComputeTarget() 的结果 + 1。")));

	return st;
}

/*
 * 判一个元组要不要消毒 xmax；要则把清空计划写进 *frz（offset 由调用者填）。
 * cur_tb = 该分片**当前**的持久截断点，用于区分"判不出来"与"调用者给错"。
 */
static bool
sanitize_plan_for_tuple(Oid shard, HeapTupleHeader tuple,
						TransactionId trunc_before, TransactionId cur_tb,
						HeapTupleFreeze *frz)
{
	TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);
	TxnStatus	st;

	/* 已经没有 xmax 了：无事可做（也含被上一趟消过毒的） */
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) ||
		!TransactionIdIsNormal(xmax))
		return false;

	/*
	 * multixact 不该出现在分片表上：补丁 0006 对分片表强制走
	 * HEAP_XMAX_INVALID 简单路径（原生机器会拿分片 xid 误组 multixact，
	 * P1 实测过 "new multixact has more than one updating member"）。
	 * 撞见即上游破防 —— 这里若照常处置，等于把分片 xid 当 multi 号解释。
	 */
	if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("分片 %u 的元组带 multixact xmax %u —— 分片表不应产生 multixact",
						shard, xmax),
				 errdetail("补丁 0006 对分片表强制 HEAP_XMAX_INVALID 简单路径；"
						   "出现 multi 说明该路径被绕过。")));

	/* 截断点以上：clog 还查得到，不归本动作管（设计 §6.4 ③） */
	if (xmax >= trunc_before)
		return false;

	if (!HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
	{
		st = shard_vacuum_status(shard, xmax, "xmax", trunc_before, cur_tb);

		/*
		 * COMMITTED：真删除，留给动作 ②；免查区把它读成"已提交的删除"恰好
		 * 正确。未决且在 cur_tb 以下（助手已放行的那一格）：判不出来，不动。
		 */
		if (st != TXN_ABORTED)
			return false;
	}
	/* else: lock-only —— 锁随事务结束释放，无论提交中止都该清 */

	/* 逐字照抄 heap_prepare_freeze_tuple 的 freeze_xmax 分支 */
	frz->xmax = InvalidTransactionId;
	frz->t_infomask2 = tuple->t_infomask2 & ~(HEAP_HOT_UPDATED | HEAP_KEYS_UPDATED);
	frz->t_infomask = (tuple->t_infomask & ~HEAP_XMAX_BITS) | HEAP_XMAX_INVALID;
	frz->frzflags = 0;
	frz->checkflags = 0;		/* ★ 恒 0：那两项检查查的是原生 clog */
	return true;
}

void
ShardVacuumSanitizeXmax(Relation rel, TransactionId trunc_before,
						ShardVacuumPageStats *stats)
{
	Oid			shard;
	TransactionId cur_tb;
	BlockNumber nblocks;
	BlockNumber blkno;

	memset(stats, 0, sizeof(*stats));

	if (!shard_vacuum_begin(rel, trunc_before, &shard, &cur_tb))
		return;

	nblocks = RelationGetNumberOfBlocks(rel);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber off;
		OffsetNumber maxoff;
		HeapTupleFreeze frozen[MaxHeapTuplesPerPage];
		int			nfrozen = 0;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buf);
		stats->pages_scanned++;

		if (PageIsNew(page) || PageIsEmpty(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		itemid = PageGetItemId(page, off);
			HeapTupleHeader tuple;

			if (!ItemIdIsNormal(itemid))
				continue;

			tuple = (HeapTupleHeader) PageGetItem(page, itemid);
			if (sanitize_plan_for_tuple(shard, tuple, trunc_before, cur_tb,
										&frozen[nfrozen]))
			{
				frozen[nfrozen].offset = off;
				nfrozen++;
			}
		}

		if (nfrozen > 0)
		{
			/*
			 * snapshotConflictHorizon 传 Invalid：该值只在 hot standby 的
			 * ResolveRecoveryConflictWithSnapshot 里用于杀查询，而分片副本
			 * 走 pg_parwal 物理回放（非 hot standby），且消毒只会让元组
			 * **更可见**，不存在需要杀掉的快照冲突。
			 */
			heap_freeze_execute_prepared(rel, buf, InvalidTransactionId,
										 frozen, nfrozen);
			stats->pages_dirtied++;
			stats->tuples_touched += nfrozen;
		}

		UnlockReleaseBuffer(buf);
	}
}

/* ================================================================== */
/* §6.4 ①：删中止 xmin 的元组（T5.3b）                                 */
/* ================================================================== */

/*
 * ★ 为什么是"两条记录"而不是一条
 *
 * 原生对**无索引**表的回收就是两步（vacuumlazy.c：nindexes == 0 时
 * lazy_scan_prune 之后立刻 lazy_vacuum_heap_page）：
 *   ① XLOG_HEAP2_PRUNE  —— LP_NORMAL ⇒ LP_DEAD（根元组）或 LP_UNUSED
 *                          （已脱链的 heap-only 元组）；
 *   ② XLOG_HEAP2_VACUUM —— LP_DEAD ⇒ LP_UNUSED + 截行指针数组。
 * 这个分工是硬的，不是风格问题：heap_page_prune_execute 的 nowunused 只接受
 * heap-only 元组（带存储的根元组的 TID 可能仍被索引引用），而
 * heap_xlog_vacuum 的 redo 又要求目标行指针**已经是** LP_DEAD。想一步到位
 * 就必然踩其中一边的不变式。照抄原生的两步，redo 侧一行不用改。
 *
 * 页面变换全部交给内核的 heap_page_prune_execute()（redo 侧调的是同一个
 * 函数），本模块只负责"选哪些行"与组装记录 —— 与 ③ 一样是"策略在扩展"。
 *
 * ★ 不碰 pd_prune_xid 与 PD_PAGE_FULL
 *
 * 原生 heap_page_prune 在 leader 侧会更新这两个提示，而 heap_xlog_prune 的
 * redo 明确不管（"we don't worry about updating the page's prunability
 * hints"）—— 即**原生自己就在这两个字段上主副本分叉**，靠 heap_mask() 把
 * 它们掩掉。分片表已禁 on-access 剪枝（补丁 0005），这两个提示对我们毫无
 * 用处，因此干脆不动：与 redo 逐字一致，比原生还紧。
 */
void
ShardVacuumRemoveAbortedXmin(Relation rel, TransactionId trunc_before,
							 ShardVacuumPageStats *stats)
{
	Oid			shard;
	TransactionId cur_tb;
	BlockNumber nblocks;
	BlockNumber blkno;
	List	   *indexes;
	bool		is_catalog;

	memset(stats, 0, sizeof(*stats));

	if (!shard_vacuum_begin(rel, trunc_before, &shard, &cur_tb))
		return;

	/*
	 * 索引两阶段是 T5.3c。带索引的关系走到这里会把索引项留成悬空指针，
	 * fail-closed。（P1 起分片打标表一律无索引：CREATE INDEX / REINDEX 都被
	 * ShardXidUtilityGuard 拦着，所以这条正常永不触发。）
	 */
	indexes = RelationGetIndexList(rel);
	if (indexes != NIL)
	{
		list_free(indexes);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("分片 %u 的关系带索引 —— 删元组需要索引两阶段（T5.3c，尚未实现）",
						shard)));
	}

	is_catalog = RelationIsAccessibleInLogicalDecoding(rel);
	nblocks = RelationGetNumberOfBlocks(rel);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber off;
		OffsetNumber maxoff;
		OffsetNumber nowdead[MaxHeapTuplesPerPage];
		OffsetNumber nowunused[MaxHeapTuplesPerPage];
		int			ndead = 0;
		int			nunused = 0;
		Size		freespace;

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, blkno);

		/*
		 * 回收行指针必须持 cleanup lock（否则并发扫描手里的 TID 会指向被
		 * 复用的槽）。用条件版本：拿不到就跳过，不阻塞在别人的 pin 上。
		 * 跳过 ⇒ 本趟不完整 ⇒ 截断不得进行（由 T5.4 的门禁按
		 * pages_skipped 判定）。
		 */
		if (!ConditionalLockBufferForCleanup(buf))
		{
			ReleaseBuffer(buf);
			stats->pages_skipped++;
			continue;
		}

		page = BufferGetPage(buf);
		stats->pages_scanned++;

		if (PageIsNew(page) || PageIsEmpty(page))
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		/*
		 * 分片表从未跑过 vacuum，PD_ALL_VISIBLE 无从被置上；真置上了说明有
		 * 别的通路动过这张表，此时删元组还得同步清 vm 位 —— 本函数不做，
		 * fail-closed。
		 */
		if (PageIsAllVisible(page))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("分片 %u 第 %u 页带 PD_ALL_VISIBLE —— 分片表不应出现",
							shard, blkno)));

		maxoff = PageGetMaxOffsetNumber(page);
		for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
		{
			ItemId		itemid = PageGetItemId(page, off);
			HeapTupleHeader tuple;
			TransactionId xmin;

			if (!ItemIdIsNormal(itemid))
				continue;

			tuple = (HeapTupleHeader) PageGetItem(page, itemid);
			xmin = HeapTupleHeaderGetRawXmin(tuple);

			/* 冻结/无效 xmin：不是分片 xid，不归本动作管 */
			if (!TransactionIdIsNormal(xmin))
				continue;

			/* 截断点以上：clog 还查得到，本趟不动（设计 §6.4 趟范围） */
			if (xmin >= trunc_before)
				continue;

			if (shard_vacuum_status(shard, xmin, "xmin",
									trunc_before, cur_tb) != TXN_ABORTED)
				continue;

			/* —— 这条元组是中止插入，必须删 —— */

			if (HeapTupleHeaderIsHeapOnly(tuple))
			{
				/*
				 * 还挂在 HOT 链上（自己也被 HOT 更新过）就不能直接置
				 * LP_UNUSED。这只可能出现在"③ 还没跑"的时候：HOT_UPDATED
				 * 的有效性以 HEAP_XMAX_INVALID == 0 为前提，而这条元组的
				 * xmax 必是同一个中止事务的号，③ 一清就解除了。
				 * 留到下一趟，并如实计数——本趟不完整。
				 */
				if (HeapTupleHeaderIsHotUpdated(tuple))
				{
					stats->tuples_deferred++;
					continue;
				}
				nowunused[nunused++] = off;
			}
			else
				nowdead[ndead++] = off;
		}

		if (ndead == 0 && nunused == 0)
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		/* ---- 第一步：PRUNE 记录 ---- */
		START_CRIT_SECTION();

		heap_page_prune_execute(buf, NULL, 0, nowdead, ndead,
								nowunused, nunused);
		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
		{
			xl_heap_prune xlrec;
			XLogRecPtr	recptr;

			/*
			 * snapshotConflictHorizon 传 Invalid：它只服务 hot standby 的
			 * 查询冲突解决，而(a) 分片副本走 pg_parwal 物理回放、不是 hot
			 * standby；(b) 被删的是**中止插入**的元组，对任何快照、任何
			 * 时刻都不可见，本就不存在需要杀掉的读者。
			 */
			xlrec.snapshotConflictHorizon = InvalidTransactionId;
			xlrec.nredirected = 0;
			xlrec.ndead = (uint16) ndead;
			xlrec.isCatalogRel = is_catalog;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfHeapPrune);
			XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
			if (ndead > 0)
				XLogRegisterBufData(0, (char *) nowdead,
									ndead * sizeof(OffsetNumber));
			if (nunused > 0)
				XLogRegisterBufData(0, (char *) nowunused,
									nunused * sizeof(OffsetNumber));

			recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_PRUNE);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		/* ---- 第二步：LP_DEAD ⇒ LP_UNUSED（无索引，不需索引两阶段）---- */
		if (ndead > 0)
		{
			START_CRIT_SECTION();

			for (int i = 0; i < ndead; i++)
				ItemIdSetUnused(PageGetItemId(page, nowdead[i]));
			PageTruncateLinePointerArray(page);
			MarkBufferDirty(buf);

			if (RelationNeedsWAL(rel))
			{
				xl_heap_vacuum xlrec;
				XLogRecPtr	recptr;

				xlrec.nunused = (uint16) ndead;

				XLogBeginInsert();
				XLogRegisterData((char *) &xlrec, SizeOfHeapVacuum);
				XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
				XLogRegisterBufData(0, (char *) nowdead,
									ndead * sizeof(OffsetNumber));

				recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_VACUUM);
				PageSetLSN(page, recptr);
			}

			END_CRIT_SECTION();
		}

		stats->pages_dirtied++;
		stats->tuples_touched += ndead + nunused;

		freespace = PageGetHeapFreeSpace(page);
		UnlockReleaseBuffer(buf);
		RecordPageWithFreeSpace(rel, blkno, freespace);
	}
}

/* ---- SQL 包装（T5.5 编排与验收的调用点）---- */
PG_FUNCTION_INFO_V1(partdist_shard_sanitize_xmax);
Datum
partdist_shard_sanitize_xmax(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TransactionId trunc_before = (TransactionId) PG_GETARG_INT64(1);
	Relation	rel;
	ShardVacuumPageStats st;
	Datum		values[3];
	bool		nulls[3] = {false, false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	/* 与原生 vacuum 同级：允许读写并发，排斥另一趟 vacuum */
	rel = relation_open(relid, ShareUpdateExclusiveLock);
	PG_TRY();
	{
		ShardVacuumSanitizeXmax(rel, trunc_before, &st);
	}
	PG_FINALLY();
	{
		relation_close(rel, ShareUpdateExclusiveLock);
	}
	PG_END_TRY();

	values[0] = Int64GetDatum(st.pages_scanned);
	values[1] = Int64GetDatum(st.pages_dirtied);
	values[2] = Int64GetDatum(st.tuples_touched);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(partdist_shard_remove_aborted);
Datum
partdist_shard_remove_aborted(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TransactionId trunc_before = (TransactionId) PG_GETARG_INT64(1);
	Relation	rel;
	ShardVacuumPageStats st;
	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	rel = relation_open(relid, ShareUpdateExclusiveLock);
	PG_TRY();
	{
		ShardVacuumRemoveAbortedXmin(rel, trunc_before, &st);
	}
	PG_FINALLY();
	{
		relation_close(rel, ShareUpdateExclusiveLock);
	}
	PG_END_TRY();

	values[0] = Int64GetDatum(st.pages_scanned);
	values[1] = Int64GetDatum(st.pages_dirtied);
	values[2] = Int64GetDatum(st.pages_skipped);
	values[3] = Int64GetDatum(st.tuples_touched);
	values[4] = Int64GetDatum(st.tuples_deferred);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}
