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

#include "access/genam.h"			/* T5.8：索引两阶段 */
#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "catalog/index.h"
#include "access/htup_details.h"
#include "access/xlog.h"			/* T5.5：落标记前刷 WAL */
#include "access/xloginsert.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/freespace.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "storage/itemptr.h"
#include "utils/memutils.h"		/* maintenance_work_mem */
#include "utils/relcache.h"

#include "shard_clog.h"
#include "shard_vacuum.h"
#include "shard_xid.h"

/*
 * 两个页面动作的共同开场：认分片、拒绝无意义/后退的 trunc_before、取当前
 * 持久截断点。返回 false = 本趟无事可做（调用者直接返回）。
 */
/*
 * 给定分片 oid 的校验部分。
 *
 * 拆出这个变体是为了 **TOAST 关系**：它的元组同样被打分片 xid
 * （`ShardXidRelidLookup` 对 RELKIND_TOASTVALUE 按 pg_toast_<owner> 解出属主），
 * 但 `ShardXidLookupByOid(toast_oid)` 依赖后端本地的 toast_map 是否已被填过，
 * 不可靠。所以处置 TOAST 时由调用方**直接把属主的分片 oid 传进来**。
 */
static bool
shard_vacuum_begin_for(Oid shard, TransactionId trunc_before,
					   TransactionId *cur_tb_out)
{
	TransactionId cur_tb;
	TransactionId cur_vx;
	TransactionId next_xid;

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

	/*
	 * ★ 上界 fail-closed：不许清理/截断到一个**从未发出过**的号。
	 * 这条输入错误的后果很重 —— 整个已用 xid 空间落进免查区，此后每一行
	 * 新写入的 xmin 都会被读成"早已提交、对一切快照可见"，中止事务的行也
	 * 一并复活。正常来源（ShardVacuumComputeTarget 的结果 + 1）永远不会
	 * 越界：它只在**已落账**的条目上前进，遇空洞即停。
	 */
	next_xid = ShardXidNextToIssue(shard);
	if (next_xid > 0 && trunc_before > next_xid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("分片 %u 的 trunc_before %u 超过下一个待发号 %u",
						shard, trunc_before, next_xid),
				 errdetail("清理到一个从未发出过的号会把整个已用 xid 空间推进"
						   "免查隐式冻结区。"),
				 errhint("trunc_before 应取 ShardVacuumComputeTarget() 的结果 + 1。")));

	*cur_tb_out = cur_tb;
	return true;
}

static bool
shard_vacuum_begin(Relation rel, TransactionId trunc_before,
				   Oid *shard_out, TransactionId *cur_tb_out)
{
	Oid			relid = RelationGetRelid(rel);
	Oid			shard = ShardXidLookupByOid(relid);

	if (!OidIsValid(shard))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("关系 %u 不是分片打标表，无分片 vacuum 可做", relid)));

	*shard_out = shard;
	return shard_vacuum_begin_for(shard, trunc_before, cur_tb_out);
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
static void shard_vacuum_sanitize_pass(Relation rel, Oid shard,
									   TransactionId cur_tb,
									   TransactionId trunc_before,
									   ShardVacuumPageStats *stats);

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

static void
shard_vacuum_sanitize_pass(Relation rel, Oid shard, TransactionId cur_tb,
						   TransactionId trunc_before,
						   ShardVacuumPageStats *stats)
{
	BlockNumber nblocks;
	BlockNumber blkno;

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
/* 两个删元组动作的判据差异全在这里 */
typedef enum ShardVacuumPruneMode
{
	SVP_ABORTED_XMIN,			/* §6.4 ①：xmin 中止 */
	SVP_DEAD_XMAX				/* §6.4 ②：xmax 已提交 */
} ShardVacuumPruneMode;

/*
 * 判一条元组在本模式下是不是该删。两个模式的定义域天然不相交
 * （xmin 中止的行不可能被谁删过 —— 看不见的行删不了），所以两趟互不干扰。
 */
static bool
prune_tuple_is_dead(Oid shard, HeapTupleHeader tuple, ShardVacuumPruneMode mode,
					TransactionId trunc_before, TransactionId cur_tb)
{
	if (mode == SVP_ABORTED_XMIN)
	{
		TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple);

		/* 冻结/无效 xmin：不是分片 xid，不归本动作管 */
		if (!TransactionIdIsNormal(xmin))
			return false;
		/* 截断点以上：clog 还查得到，本趟不动（设计 §6.4 趟范围） */
		if (xmin >= trunc_before)
			return false;

		return shard_vacuum_status(shard, xmin, "xmin",
								   trunc_before, cur_tb) == TXN_ABORTED;
	}
	else
	{
		TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);
		ShardClogSlot slot;

		if ((tuple->t_infomask & HEAP_XMAX_INVALID) ||
			!TransactionIdIsNormal(xmax))
			return false;		/* 从没被删过 —— 设计 §6.4 ②："零页面动作" */

		if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("分片 %u 的元组带 multixact xmax %u —— 分片表不应产生 multixact",
							shard, xmax)));

		/* lock-only 是锁不是删除；它归 ③ 消毒，不归本动作删 */
		if (HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
			return false;

		if (xmax >= trunc_before)
			return false;

		if (shard_vacuum_status(shard, xmax, "xmax",
								trunc_before, cur_tb) != TXN_COMMITTED)
			return false;		/* 中止的删除 ⇒ 行是活的（③ 会消毒它的 xmax） */

		/*
		 * 设计 §6.4 ② 的判据原文是"xmax 已提交**且 commit_ts < GlobalSafeTs**"。
		 * 这里不再取一次 GlobalSafeTs，而是靠 trunc_before 的构造来保证：
		 * §6.3 的前缀扫描只让 `commit_ts > 0 && commit_ts < GlobalSafeTs` 的
		 * COMMITTED 条目过关，所以**截断点以下的 COMMITTED 天然满足该条件**
		 * （GlobalSafeTs 单调不减，当时成立则永远成立）。
		 *
		 * 这条推理只在 trunc_before 确实来自 ShardVacuumComputeTarget 时成立，
		 * 因此把它的反命题做成守卫：commit_ts == 0 的 COMMITTED 槽是 T5.2 明确
		 * 会**挡住前缀**的形态（P2 遗留数据），它出现在截断点以下就说明
		 * trunc_before 是硬塞进来的 —— fail-closed，绝不按"已提交"删元组。
		 */
		if (!ShardClogReadSlot(shard, xmax, &slot) || (int64) slot.commit_ts <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("分片 %u 的 xmax %u 已提交但 commit_ts 为 0，却落在截断点 %u 以下",
							shard, xmax, trunc_before),
					 errdetail("§6.3 的前缀扫描会挡住 commit_ts=0 的已提交条目，"
							   "它不可能出现在正确算出的截断点以下。"),
					 errhint("trunc_before 应取 ShardVacuumComputeTarget() 的结果 + 1。")));

		return true;
	}
}

void
ShardVacuumSanitizeXmax(Relation rel, TransactionId trunc_before,
						ShardVacuumPageStats *stats)
{
	Oid			shard;
	TransactionId cur_tb;

	memset(stats, 0, sizeof(*stats));
	if (!shard_vacuum_begin(rel, trunc_before, &shard, &cur_tb))
		return;
	shard_vacuum_sanitize_pass(rel, shard, cur_tb, trunc_before, stats);
}

/* ================================================================== */
/* 索引两阶段（设计 §6.4 ②"含索引项清理，两阶段"，T5.8）              */
/* ================================================================== */

/*
 * 死 TID 批。按扫描序收集，天然按 (blkno, offnum) 升序 —— 两个消费者都靠这个：
 *   ① index_bulk_delete 的回调二分查找；
 *   ② 第三阶段按块分组回收行指针。
 */
typedef struct ShardVacuumDeadItems
{
	int			max_items;
	int			num_items;
	ItemPointerData *items;
} ShardVacuumDeadItems;

static int
shard_vacuum_tid_cmp(const void *a, const void *b)
{
	return ItemPointerCompare((ItemPointer) a, (ItemPointer) b);
}

/* index_bulk_delete 的回调：TID 在死名单里就删掉这条索引项 */
static bool
shard_vacuum_tid_reaped(ItemPointer itemptr, void *state)
{
	ShardVacuumDeadItems *d = (ShardVacuumDeadItems *) state;

	return bsearch(itemptr, d->items, (size_t) d->num_items,
				   sizeof(ItemPointerData), shard_vacuum_tid_cmp) != NULL;
}

/*
 * 第二阶段：逐个索引删掉指向死 TID 的索引项。
 *
 * 全部交给 index_bulk_delete / index_vacuum_cleanup —— 与原生 vacuum 走的是
 * 同一对入口，索引侧的页面变更与 WAL 记录都由索引 AM 自己发，本模块不碰。
 * 这与 ①③ 的路子一致：**策略在扩展，页面变换与 WAL 用内核的。**
 */
static void
shard_vacuum_indexes(Relation rel, ShardVacuumDeadItems *dead)
{
	List	   *indexoidlist;
	ListCell   *lc;

	if (dead->num_items == 0)
		return;

	indexoidlist = RelationGetIndexList(rel);
	foreach(lc, indexoidlist)
	{
		Relation	ind = index_open(lfirst_oid(lc), RowExclusiveLock);
		IndexVacuumInfo ivinfo;
		IndexBulkDeleteResult *istat;

		memset(&ivinfo, 0, sizeof(ivinfo));
		ivinfo.index = ind;
		ivinfo.heaprel = rel;
		ivinfo.analyze_only = false;
		ivinfo.report_progress = false;
		ivinfo.estimated_count = true;
		ivinfo.message_level = DEBUG2;
		ivinfo.num_heap_tuples = rel->rd_rel->reltuples;
		ivinfo.strategy = NULL;

		istat = index_bulk_delete(&ivinfo, NULL, shard_vacuum_tid_reaped, dead);
		istat = index_vacuum_cleanup(&ivinfo, istat);
		if (istat != NULL)
			pfree(istat);

		index_close(ind, RowExclusiveLock);
	}
	list_free(indexoidlist);
}

/*
 * 第三阶段：把死 TID 的行指针 LP_DEAD ⇒ LP_UNUSED（XLOG_HEAP2_VACUUM）。
 *
 * 必须在第二阶段之后：反过来先回收行指针、后清索引，中间崩溃就会留下**指向
 * 已被复用的行指针**的索引项 —— 索引扫到一条无关的新元组。这条次序正是设计
 * §6.4 ② 写"两阶段（收集死 TID → 清索引 → 回收行指针）"的原因。
 *
 * 拿不到 cleanup lock 则跳过该页并计数：此时索引项已经删了、行指针还留作
 * LP_DEAD —— **这是安全的中间态**（LP_DEAD 不被任何索引项指向，扫描也不会
 * 返回它），下一趟扫描会把既有的 LP_DEAD 一并收进死名单再回收。
 */
static void
shard_vacuum_heap_pass2(Relation rel, ShardVacuumDeadItems *dead,
						ShardVacuumPageStats *stats)
{
	int			idx = 0;

	while (idx < dead->num_items)
	{
		BlockNumber blkno = ItemPointerGetBlockNumber(&dead->items[idx]);
		OffsetNumber unused[MaxHeapTuplesPerPage];
		int			nunused = 0;
		Buffer		buf;
		Page		page;
		Size		freespace;
		int			i;

		while (idx < dead->num_items &&
			   ItemPointerGetBlockNumber(&dead->items[idx]) == blkno)
			unused[nunused++] = ItemPointerGetOffsetNumber(&dead->items[idx++]);

		CHECK_FOR_INTERRUPTS();

		buf = ReadBuffer(rel, blkno);
		if (!ConditionalLockBufferForCleanup(buf))
		{
			ReleaseBuffer(buf);
			stats->pages_skipped++;
			continue;
		}
		page = BufferGetPage(buf);

		START_CRIT_SECTION();

		for (i = 0; i < nunused; i++)
			ItemIdSetUnused(PageGetItemId(page, unused[i]));
		PageTruncateLinePointerArray(page);
		MarkBufferDirty(buf);

		if (RelationNeedsWAL(rel))
		{
			xl_heap_vacuum xlrec;
			XLogRecPtr	recptr;

			xlrec.nunused = (uint16) nunused;

			XLogBeginInsert();
			XLogRegisterData((char *) &xlrec, SizeOfHeapVacuum);
			XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
			XLogRegisterBufData(0, (char *) unused,
								nunused * sizeof(OffsetNumber));

			recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_VACUUM);
			PageSetLSN(page, recptr);
		}

		END_CRIT_SECTION();

		freespace = PageGetHeapFreeSpace(page);
		UnlockReleaseBuffer(buf);
		RecordPageWithFreeSpace(rel, blkno, freespace);
	}

	dead->num_items = 0;		/* 本批结算完毕 */
}

/* 一批死 TID 的收尾：清索引 → 回收行指针 */
static void
shard_vacuum_flush_batch(Relation rel, ShardVacuumDeadItems *dead,
						 ShardVacuumPageStats *stats)
{
	if (dead->num_items == 0)
		return;
	shard_vacuum_indexes(rel, dead);
	shard_vacuum_heap_pass2(rel, dead, stats);
}

/* ================================================================== */
/* 页面趟主体                                                          */
/* ================================================================== */

static void
shard_vacuum_prune_pass(Relation rel, Oid shard, TransactionId cur_tb,
						TransactionId trunc_before, ShardVacuumPruneMode mode,
						ShardVacuumPageStats *stats)
{
	BlockNumber nblocks;
	BlockNumber blkno;
	bool		is_catalog;
	bool		has_index;
	List	   *indexoidlist;
	ShardVacuumDeadItems dead;

	indexoidlist = RelationGetIndexList(rel);
	has_index = (indexoidlist != NIL);
	list_free(indexoidlist);

	/*
	 * 死名单容量按 maintenance_work_mem 算（与原生同源）。装满就地结算一批
	 * （清索引 + 回收行指针）再继续扫 —— 与原生 vacuum 的多趟一模一样。
	 */
	dead.items = NULL;
	dead.num_items = 0;
	dead.max_items = 0;
	if (has_index)
	{
		long		n = (long) maintenance_work_mem * 1024L /
			(long) sizeof(ItemPointerData);

		dead.max_items = (int) Min(Max(n, 1024L), (long) (MaxAllocSize / sizeof(ItemPointerData)));
		dead.items = (ItemPointerData *)
			palloc(sizeof(ItemPointerData) * (Size) dead.max_items);
	}

	is_catalog = RelationIsAccessibleInLogicalDecoding(rel);
	nblocks = RelationGetNumberOfBlocks(rel);

	for (blkno = 0; blkno < nblocks; blkno++)
	{
		Buffer		buf;
		Page		page;
		OffsetNumber off;
		OffsetNumber maxoff;
		OffsetNumber newdead[MaxHeapTuplesPerPage];
		OffsetNumber newunused[MaxHeapTuplesPerPage];
		OffsetNumber alldead[MaxHeapTuplesPerPage];
		int			ndead = 0;
		int			nunused = 0;
		int			nall = 0;
		int			i;
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

			/*
			 * 既有的 LP_DEAD：上一趟清完索引之前崩溃、或第三阶段跳过该页
			 * 留下的中间态。**必须一并收走**，否则它们永远不再被扫描看见
			 * （扫描只认 LP_NORMAL），行指针就此泄漏。
			 */
			if (ItemIdIsDead(itemid))
			{
				alldead[nall++] = off;
				continue;
			}

			if (!ItemIdIsNormal(itemid))
				continue;

			tuple = (HeapTupleHeader) PageGetItem(page, itemid);

			if (!prune_tuple_is_dead(shard, tuple, mode, trunc_before, cur_tb))
				continue;

			if (HeapTupleHeaderIsHeapOnly(tuple))
			{
				/*
				 * ① 的保守格：还挂在 HOT 链上（自己也被 HOT 更新过）就先不
				 * 动。这只出现在"③ 还没跑"的时候 —— HOT_UPDATED 的有效性
				 * 以 HEAP_XMAX_INVALID == 0 为前提，而这条元组的 xmax 必是
				 * 同一个中止事务的号，③ 一清就解除了。留到下一趟，如实计数。
				 *
				 * ★ ② **不能**照此办理：committed 的 xmax 没有任何后续动作
				 *   会去清它的 HEAP_HOT_UPDATED，一推迟就是永远推迟，而
				 *   "本趟不完整"又禁止截断 —— HOT 链会把截断永久钉死。
				 */
				if (mode == SVP_ABORTED_XMIN && HeapTupleHeaderIsHotUpdated(tuple))
				{
					stats->tuples_deferred++;
					continue;
				}
				/* heap-only 元组没有索引项指向它，可以直接置 LP_UNUSED */
				newunused[nunused++] = off;
			}
			else
			{
				/*
				 * ★ 带索引的关系上，"死根元组 + 链上还有活的后继"必须做成
				 * LP_REDIRECT（把根的行指针指向第一个活成员），否则索引项
				 * 被删掉之后那条活行就再也扫不到了。**本实现不产生
				 * LP_REDIRECT**，撞见即 fail-closed。
				 *
				 * 无索引的关系上不存在这个问题（页外没有任何东西引用行指针，
				 * 顺序扫描逐个访问 LP_NORMAL，被摘掉根的 heap-only 元组照样
				 * 读得到）—— 那条路 T5.3b/c 已验过。
				 *
				 * TOAST 关系也不存在：TOAST 元组只被 INSERT / DELETE，
				 * 从不 UPDATE，天然没有 HOT 链。
				 */
				if (has_index && HeapTupleHeaderIsHotUpdated(tuple))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("分片 %u 第 %u 页第 %u 项是带 HOT 链的死根元组，"
									"而该关系有索引", shard, blkno, off),
							 errdetail("清掉它的索引项会让链上仍然活着的后继元组"
									   "无法经索引访问；正确做法是把根做成 "
									   "LP_REDIRECT —— 本实现尚未产生 LP_REDIRECT。"),
							 errhint("分片打标表在 P1 起禁建索引；TOAST 关系不产生 "
									 "HOT 链。撞见这条说明有第三种情形，须先补 "
									 "LP_REDIRECT。")));

				newdead[ndead++] = off;
				alldead[nall++] = off;
			}
		}

		if (ndead == 0 && nunused == 0 && nall == 0)
		{
			UnlockReleaseBuffer(buf);
			continue;
		}

		/* ---- 第一阶段：PRUNE 记录 ---- */
		if (ndead > 0 || nunused > 0)
		{
			START_CRIT_SECTION();

			heap_page_prune_execute(buf, NULL, 0, newdead, ndead,
									newunused, nunused);
			MarkBufferDirty(buf);

			if (RelationNeedsWAL(rel))
			{
				xl_heap_prune xlrec;
				XLogRecPtr	recptr;

				/*
				 * snapshotConflictHorizon 传 Invalid：它只服务 hot standby 的
				 * 查询冲突解决，而(a) 分片副本走 pg_parwal 物理回放、不是 hot
				 * standby；(b) 被删的元组要么是**中止插入**（对任何快照任何
				 * 时刻都不可见），要么是**已提交删除且 commit_ts <
				 * GlobalSafeTs**（按 §6.2 的定义没有活跃快照还需要它）。
				 */
				xlrec.snapshotConflictHorizon = InvalidTransactionId;
				xlrec.nredirected = 0;
				xlrec.ndead = (uint16) ndead;
				xlrec.isCatalogRel = is_catalog;

				XLogBeginInsert();
				XLogRegisterData((char *) &xlrec, SizeOfHeapPrune);
				XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
				if (ndead > 0)
					XLogRegisterBufData(0, (char *) newdead,
										ndead * sizeof(OffsetNumber));
				if (nunused > 0)
					XLogRegisterBufData(0, (char *) newunused,
										nunused * sizeof(OffsetNumber));

				recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_PRUNE);
				PageSetLSN(page, recptr);
			}

			END_CRIT_SECTION();

			stats->tuples_touched += ndead + nunused;
		}

		if (!has_index)
		{
			/* ---- 无索引：立刻 LP_DEAD ⇒ LP_UNUSED，不需要索引两阶段 ---- */
			if (nall > 0)
			{
				START_CRIT_SECTION();

				for (i = 0; i < nall; i++)
					ItemIdSetUnused(PageGetItemId(page, alldead[i]));
				PageTruncateLinePointerArray(page);
				MarkBufferDirty(buf);

				if (RelationNeedsWAL(rel))
				{
					xl_heap_vacuum xlrec;
					XLogRecPtr	recptr;

					xlrec.nunused = (uint16) nall;

					XLogBeginInsert();
					XLogRegisterData((char *) &xlrec, SizeOfHeapVacuum);
					XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
					XLogRegisterBufData(0, (char *) alldead,
										nall * sizeof(OffsetNumber));

					recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_VACUUM);
					PageSetLSN(page, recptr);
				}

				END_CRIT_SECTION();
			}
		}

		stats->pages_dirtied++;
		freespace = PageGetHeapFreeSpace(page);
		UnlockReleaseBuffer(buf);
		RecordPageWithFreeSpace(rel, blkno, freespace);

		if (has_index && nall > 0)
		{
			/* 死 TID 进批，等本批攒满或扫完再统一清索引、回收行指针 */
			for (i = 0; i < nall; i++)
			{
				if (dead.num_items >= dead.max_items)
					shard_vacuum_flush_batch(rel, &dead, stats);
				ItemPointerSet(&dead.items[dead.num_items], blkno, alldead[i]);
				dead.num_items++;
			}
		}
	}

	if (has_index)
	{
		shard_vacuum_flush_batch(rel, &dead, stats);
		pfree(dead.items);
	}
}

void
ShardVacuumRemoveAbortedXmin(Relation rel, TransactionId trunc_before,
							 ShardVacuumPageStats *stats)
{
	Oid			shard;
	TransactionId cur_tb;

	memset(stats, 0, sizeof(*stats));
	if (!shard_vacuum_begin(rel, trunc_before, &shard, &cur_tb))
		return;
	shard_vacuum_prune_pass(rel, shard, cur_tb, trunc_before,
							SVP_ABORTED_XMIN, stats);
}

void
ShardVacuumRemoveDeadTuples(Relation rel, TransactionId trunc_before,
							ShardVacuumPageStats *stats)
{
	Oid			shard;
	TransactionId cur_tb;

	memset(stats, 0, sizeof(*stats));
	if (!shard_vacuum_begin(rel, trunc_before, &shard, &cur_tb))
		return;
	shard_vacuum_prune_pass(rel, shard, cur_tb, trunc_before,
							SVP_DEAD_XMAX, stats);
}

/* ================================================================== */
/* T5.4：一整趟页面动作 —— 顺序铁律的凭据来源                          */
/* ================================================================== */

/* 对一张关系（主堆或它的 TOAST）按 ③→①→② 跑一遍，计数累加进 stats */
static void
sweep_one_rel(Relation rel, Oid shard, TransactionId cur_tb,
			  TransactionId trunc_before, ShardVacuumPageStats *stats,
			  int64 *sanitized, int64 *removed_aborted, int64 *removed_dead)
{
	ShardVacuumPageStats one;

	/* ③ 消毒 —— 必须最先：① 的推迟格要靠它清 xmax 才解除 */
	memset(&one, 0, sizeof(one));
	shard_vacuum_sanitize_pass(rel, shard, cur_tb, trunc_before, &one);
	*sanitized += one.tuples_touched;
	stats->pages_scanned += one.pages_scanned;
	stats->pages_dirtied += one.pages_dirtied;

	/* ① 删中止 xmin 的元组 */
	memset(&one, 0, sizeof(one));
	shard_vacuum_prune_pass(rel, shard, cur_tb, trunc_before,
							SVP_ABORTED_XMIN, &one);
	*removed_aborted += one.tuples_touched;
	stats->pages_scanned += one.pages_scanned;
	stats->pages_dirtied += one.pages_dirtied;
	stats->pages_skipped += one.pages_skipped;
	stats->tuples_deferred += one.tuples_deferred;

	/* ② 删已提交删除的死元组 */
	memset(&one, 0, sizeof(one));
	shard_vacuum_prune_pass(rel, shard, cur_tb, trunc_before,
							SVP_DEAD_XMAX, &one);
	*removed_dead += one.tuples_touched;
	stats->pages_scanned += one.pages_scanned;
	stats->pages_dirtied += one.pages_dirtied;
	stats->pages_skipped += one.pages_skipped;
	stats->tuples_deferred += one.tuples_deferred;
}

bool
ShardVacuumSweep(Relation rel, TransactionId trunc_before,
				 ShardVacuumPageStats *stats,
				 int64 *sanitized, int64 *removed_aborted,
				 int64 *removed_dead)
{
	Oid			shard;
	TransactionId cur_tb;
	TransactionId cur_vx;

	memset(stats, 0, sizeof(*stats));
	*sanitized = *removed_aborted = *removed_dead = 0;

	if (!shard_vacuum_begin(rel, trunc_before, &shard, &cur_tb))
		return false;

	sweep_one_rel(rel, shard, cur_tb, trunc_before, stats,
				  sanitized, removed_aborted, removed_dead);

	/*
	 * ★ TOAST 关系同属这个分片的 xid 宇宙，必须一起清（T5.8）。
	 *
	 * 依据：`ShardXidRelidLookup()` 对 RELKIND_TOASTVALUE 按 pg_toast_<owner>
	 * 解出属主，属主在名单里就返回**属主的** shard oid —— 也就是说 TOAST
	 * 元组一样被打分片 xid、一样会积累三类垃圾。而 TOAST 关系天生带一个
	 * btree 索引，所以这一段正是索引两阶段的正主用例。
	 *
	 * 分片 oid 直接沿用属主的，不再 `ShardXidLookupByOid(toast_oid)` ——
	 * 后者依赖后端本地的 toast_map 是否已被填过，不可靠。
	 */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
	{
		Relation	toastrel = table_open(rel->rd_rel->reltoastrelid,
										  ShareUpdateExclusiveLock);

		PG_TRY();
		{
			sweep_one_rel(toastrel, shard, cur_tb, trunc_before, stats,
						  sanitized, removed_aborted, removed_dead);
		}
		PG_FINALLY();
		{
			table_close(toastrel, ShareUpdateExclusiveLock);
		}
		PG_END_TRY();
	}

	stats->tuples_touched = *sanitized + *removed_aborted + *removed_dead;

	/*
	 * 不干净就不落标记。跳页（拿不到 cleanup lock）与推迟都意味着这一趟没有
	 * 覆盖到全部页面/元组，此时落标记等于对 ShardClogTruncate 说谎。
	 */
	if (stats->pages_skipped > 0 || stats->tuples_deferred > 0)
		return false;

	/*
	 * ★★ 落标记之前必须先把页面改动的 WAL 刷到盘上（T5.5 动手前查出的自身
	 * 缺陷，2026-08-21）。
	 *
	 * 三类动作发的是普通 WAL 记录，只进 WAL 缓冲区；而
	 * ShardVacuumSetWatermarks 走的是水位文件，**当场 fsync**。两者之间崩溃
	 * 一次，结果是：标记说"页面已清到 trunc_before"，而那些页面改动随未刷的
	 * WAL 一起没了 —— 恢复后 ShardClogTruncate 认这个标记、照常截断，
	 * **中止事务的幽灵行当场复活**。这正是顺序铁律要防的那件事，却发生在
	 * 铁律自己的实现里。
	 *
	 * 刷到"此刻的插入位置"即可覆盖本趟发出的全部记录（顺带多刷一点别的
	 * 后端的，无害）。这条与"先推水位、后删文件"是同一族次序要求：
	 * **任何一个'已经做完'的持久断言，都不能先于它所断言的那件事持久。**
	 */
	XLogFlush(GetXLogInsertRecPtr());

	/*
	 * 落"趟完"标记（设计 §6.5 两态之二："趟完、截断前"）。
	 * 不变式 clog_truncate_before <= shard_vacuum_xid 由落盘出口统一守；
	 * trunc_before 不许后退已在 shard_vacuum_begin 里拦过。
	 */
	ShardVacuumGetWatermarks(shard, &cur_tb, &cur_vx);
	if (trunc_before > cur_vx)
		ShardVacuumSetWatermarks(shard, cur_tb, trunc_before);

	return true;
}

/* ================================================================== */
/* T5.5：两态恢复（设计 §6.5）                                         */
/* ================================================================== */

/*
 * 页面趟不按 xid 推进（垃圾散布在任意页），所以 shard_vacuum_xid 只有两个
 * 有意义的取值，恢复也只有两条路：
 *
 *   ① tb == vx  ——「没有未完成的趟」。趟中崩溃就落在这一格：整趟的页面动作
 *      一条标记都没留下，下一轮**整趟重来**即可。重来是安全的，因为三类
 *      动作各自幂等（已消毒的 xmax 是 0、已删的行指针不再是 LP_NORMAL，
 *      再跑一遍全是空操作 —— 套件里逐条验过）。本函数对这一格**什么都不做**。
 *
 *   ② tb < vx   ——「趟完了、截断还没做」。页面动作已经全部完成并持久
 *      （落标记前刷过 WAL，见 ShardVacuumSweep 里的 XLogFlush），
 *      **只补做截断**，绝不重跑页面趟。
 *
 * 不变式 clog_truncate_before <= shard_vacuum_xid 由水位落盘出口统一守，
 * 所以不存在第三种取值。
 *
 * 返回本次做了什么（见 ShardVacuumRecoverAction）。幂等：连做两次，
 * 第二次必然回 NOTHING。
 */
int
ShardVacuumRecover(Oid shard)
{
	TransactionId tb;
	TransactionId vx;

	ShardVacuumGetWatermarks(shard, &tb, &vx);

	if (!TransactionIdIsValid(vx) || vx <= tb)
		return SHARD_VACUUM_RECOVER_NOTHING;

	ereport(DEBUG1,
			(errmsg("pg_partdist: 分片 %u 处于「趟完未截断」态（%u < %u），补做截断",
					shard, tb, vx)));

	(void) ShardClogTruncate(shard, vx);
	return SHARD_VACUUM_RECOVER_TRUNCATE;
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

PG_FUNCTION_INFO_V1(partdist_shard_remove_dead);
Datum
partdist_shard_remove_dead(PG_FUNCTION_ARGS)
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
		ShardVacuumRemoveDeadTuples(rel, trunc_before, &st);
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

PG_FUNCTION_INFO_V1(partdist_shard_vacuum_sweep);
Datum
partdist_shard_vacuum_sweep(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	TransactionId trunc_before = (TransactionId) PG_GETARG_INT64(1);
	Relation	rel;
	ShardVacuumPageStats st;
	int64		san = 0,
				rab = 0,
				rdead = 0;
	bool		swept = false;
	Datum		values[6];
	bool		nulls[6] = {false, false, false, false, false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	rel = relation_open(relid, ShareUpdateExclusiveLock);
	PG_TRY();
	{
		swept = ShardVacuumSweep(rel, trunc_before, &st, &san, &rab, &rdead);
	}
	PG_FINALLY();
	{
		relation_close(rel, ShareUpdateExclusiveLock);
	}
	PG_END_TRY();

	values[0] = BoolGetDatum(swept);
	values[1] = Int64GetDatum(san);
	values[2] = Int64GetDatum(rab);
	values[3] = Int64GetDatum(rdead);
	values[4] = Int64GetDatum(st.pages_skipped);
	values[5] = Int64GetDatum(st.tuples_deferred);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(partdist_shard_vacuum_recover);
Datum
partdist_shard_vacuum_recover(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	int			act = ShardVacuumRecover(shard);

	PG_RETURN_TEXT_P(cstring_to_text(
		(act == SHARD_VACUUM_RECOVER_TRUNCATE) ? "truncated" : "nothing"));
}
