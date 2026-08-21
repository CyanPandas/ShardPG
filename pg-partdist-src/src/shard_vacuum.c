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
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "shard_clog.h"
#include "shard_vacuum.h"
#include "shard_xid.h"

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
		st = ShardClogReadStatus(shard, xmax);

		if (st == TXN_COMMITTED)
			return false;		/* 真删除，留给动作 ②；免查区读成"已提交删除"正确 */

		if (st != TXN_ABORTED)
		{
			/*
			 * RUNNING / PREPARED / 空洞。落在 [cur_tb, trunc_before) 里就是
			 * 调用者给错了 trunc_before —— §6.3 前缀扫描遇未决即停，算不出
			 * 跨过它的 target。fail-closed。
			 */
			if (xmax >= cur_tb)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("分片 %u 的 xmax %u 在 [%u, %u) 内仍未决（状态 %d）",
								shard, xmax, cur_tb, trunc_before, (int) st),
						 errdetail("§6.3 的前缀扫描遇 RUNNING/PREPARED/空洞即停，"
								   "调用者不可能算出跨过未决条目的截断点。"),
						 errhint("trunc_before 应取 ShardVacuumComputeTarget() 的结果 + 1。")));

			/*
			 * cur_tb 以下：clog 已被上一轮截断，判不出来了。留着是唯一
			 * 安全动作（只可能来自上一轮违反顺序铁律，属过去时既成事实）。
			 */
			return false;
		}
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
	Oid			relid = RelationGetRelid(rel);
	Oid			shard = ShardXidLookupByOid(relid);
	TransactionId cur_tb;
	TransactionId cur_vx;
	BlockNumber nblocks;
	BlockNumber blkno;

	stats->pages_scanned = 0;
	stats->pages_dirtied = 0;
	stats->tuples_touched = 0;

	if (!OidIsValid(shard))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("关系 %u 不是分片打标表，无分片 vacuum 可做", relid)));

	/* 一条都不清：InvalidTransactionId / 小于首个可用号，直接返回 */
	if (!TransactionIdIsValid(trunc_before) || trunc_before <= FIRST_SHARD_XID)
		return;

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
