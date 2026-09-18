/*
 * raft_boundary.c — SQL boundary functions consumed by the pg_raft
 * control plane.
 *
 * pg_raft (control plane) never touches pg_parwal internals directly;
 * it reaches the pg_partdist data plane only through these three
 * partdist-schema SQL functions:
 *
 *   get_partition_flush_lsn(partition_id)
 *       Latest partition_lsn durably written in local
 *       pg_parwal/<partition_id>/ — used as the failover switch point.
 *
 *   get_follower_applied_part_lsn(partition_id)
 *       Local follower replay progress from partdist.follower_partition_map.
 *       A secondary whose applied_part_lsn is behind the switch point must
 *       not be promoted.
 *
 *   partwal_notify_primary_switch(partition_id, old, new, switch_orig_lsn)
 *       Called after a Raft OP_PARTITION_PRIMARY entry is applied.
 *       ★ 2026-09-04（批次 #7）：**不再是 placeholder**。此前它整个函数体只有
 *       一句 ereport(LOG)，注释写着「follower replay is design-only」——
 *       也就是 raft 把主权翻过来之后数据面什么都没做。现在它做 FRD §11 步骤 5
 *       的数据面角色交接：升主解除副本读闸门并撤下回放 armed，降级收回身份
 *       （fail-closed）。仍未实装的是路由层本身。
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xlog_internal.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/guc.h"		/* GetConfigOption：读 pg_raft.node_id */
#include "utils/memutils.h"	/* TopMemoryContext：P7-N4 稀疏索引 */
#include "utils/pg_lsn.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/rmgr.h"
#include "access/xlogreader.h"	/* P7-N18：解码 DATA 记录取分片 xid */
#include "access/xlog_internal.h"	/* wal_segment_size */
#include "access/xlogrecord.h"	/* XLogRecord / SizeOfXLogRecord */
#include "miscadmin.h"			/* CHECK_FOR_INTERRUPTS */
#include "catalog/pg_type.h"
#include "utils/array.h"

#include "partition_wal.h"
#include "partition_wal_header.h"
#include "partition_wal_writer.h"
#include "partwal_sync.h"		/* PartWALCtl：truncate 与追加者互斥 */
#include "dtx_record.h"			/* DTX-2PC 记录载荷（DTX_2PC_DESIGN.md §5） */
#include "shard_clog.h"			/* P7-N12：升主节点把 in-doubt 决议落进分片 clog */
#include "shard_xid.h"			/* P7-N18：follower append 时提前接住发号水位 */
#include "shard_fileset.h"	/* 批次 #10：PartDistRoutePromote */
#include "shard_replay.h"		/* 批次 #7：角色交接改副本身份/armed */
#include "global_mvcc.h"		/* MakeGlobalXid / PartDistLocalNodeId */

PG_FUNCTION_INFO_V1(pg_partdist_get_partition_flush_lsn);
PG_FUNCTION_INFO_V1(pg_partdist_get_follower_applied_part_lsn);
PG_FUNCTION_INFO_V1(pg_partdist_partwal_notify_primary_switch);

Datum
pg_partdist_get_partition_flush_lsn(PG_FUNCTION_ARGS)
{
	Oid		partition_id = PG_GETARG_OID(0);
	uint64	last_lsn;

	last_lsn = GetLastWrittenPartitionLSN(partition_id);
	PG_RETURN_INT64((int64) last_lsn);
}

Datum
pg_partdist_get_follower_applied_part_lsn(PG_FUNCTION_ARGS)
{
	Oid				partition_id = PG_GETARG_OID(0);
	StringInfoData	sql;
	int				ret;
	bool			isnull;
	int64			applied_lsn = 0;

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errmsg("pg_partdist: SPI_connect failed in get_follower_applied_part_lsn")));

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT applied_part_lsn "
					 "FROM partdist.follower_partition_map "
					 "WHERE partition_id = %u",
					 partition_id);
	ret = SPI_execute(sql.data, true, 1);
	pfree(sql.data);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		applied_lsn = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
												  SPI_tuptable->tupdesc,
												  1,
												  &isnull));
		if (isnull)
			applied_lsn = 0;
	}

	SPI_finish();
	PG_RETURN_INT64(applied_lsn);
}

/*
 * raft_local_node_id —— 本节点的 **raft** 编号。
 *
 * ★ 不能拿 PartDistLocalNodeId() 顶替：那是 Citus 的 groupid（本集群 worker1=1），
 * 而 partition_map.primary_node 用的是 pg_raft.node_id（本集群 worker1=2）。
 * 两套编号差 1，混用会让"我是不是新主"整个判反。
 * 读的是另一个扩展的 GUC，属只读耦合，不触碰 pg_raft 的代码。
 */
static int
raft_local_node_id(void)
{
	const char *v = GetConfigOption("pg_raft.node_id", true, false);

	return (v != NULL) ? atoi(v) : -1;
}

/* 全局 shardid → 本节点该分片的本地 OID；本节点不承载则 InvalidOid */
static Oid
local_oid_for_shard(Oid global_shard_id)
{
	StringInfoData sql;
	int			ret;
	bool		isnull;
	Oid			loid = InvalidOid;

	if (SPI_connect() != SPI_OK_CONNECT)
		return InvalidOid;

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT local_oid FROM partdist.shard_identity "
					 "WHERE global_shard_id = %u", global_shard_id);
	ret = SPI_execute(sql.data, true, 1);
	pfree(sql.data);

	if (ret == SPI_OK_SELECT && SPI_processed > 0)
	{
		Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
								1, &isnull);

		if (!isnull)
			loid = DatumGetObjectId(d);
	}
	SPI_finish();
	return loid;
}

/*
 * partwal_notify_primary_switch —— **数据面的角色交接**（FRD §11 步骤 5）。
 *
 * ★★ 这个函数此前整个函数体只有一句 ereport(LOG)，文件头注释白纸黑字写着
 * 「Placeholder until the real role-switch / replay handover lands」。
 * 也就是说 raft 把主权翻过来之后，**数据面什么都没做** —— 真正的升主动作
 * （追平 / 推 WAL 位点 / 认领）被挂在 pg_raft_promote_prepare 里，那是**上报
 * 之前**的路，不是交接。降级方向更是完全没人管。
 *
 * 现在把它做实。它由 group0 的 apply 调用，而 group0 的 apply 在**每个成员
 * 节点**上都跑，所以每个节点都能就地判断自己的新角色 —— 这正是交接该发生的
 * 地方，也是 promote_prepare 那条路根本够不着的（它只在**当选者**上跑）。
 *
 * 两个方向：
 *   - 升主：本节点成为该分片的主 ⇒ 它不再是任何人的副本，解除 T6.3c 那道
 *     读闸门；同时**撤掉回放槽位的 armed**，否则一次误触发的 replay_catchup
 *     会拿别人的流盖掉自己的表。
 *   - 降级：本节点交出主权 ⇒ **立刻收回"已升主"身份**，读闸门重新合上。
 *     方向是 fail-closed 的：交出去之后本地这份数据随时可能过期，宁可拦住。
 *
 * ★ 不在这里自动重新 arm 回放。降级归队要不要直接按游标追平，取决于有没有
 * 发生过快路径分叉（DTX_2PC_DESIGN.md §9.5：分叉的旧 leader 必须重做物理基线
 * 才能重新参选）。那个判断不属于交接，硬塞进来就是拿正确性换省事。
 */
Datum
pg_partdist_partwal_notify_primary_switch(PG_FUNCTION_ARGS)
{
	Oid			partition_id = PG_GETARG_OID(0);
	int32		old_primary_node = PG_GETARG_INT32(1);
	int32		new_primary_node = PG_GETARG_INT32(2);
	XLogRecPtr	switch_orig_lsn = PG_GETARG_LSN(3);
	int			me = raft_local_node_id();
	Oid			loid;

	ereport(LOG,
			(errmsg("pg_partdist: primary switch notified for partition %u: %d -> %d at %X/%X",
					partition_id,
					old_primary_node,
					new_primary_node,
					LSN_FORMAT_ARGS(switch_orig_lsn))));

	if (me <= 0)
	{
		ereport(WARNING,
				(errmsg("pg_partdist: 取不到本节点的 raft 编号（pg_raft.node_id），"
						"分片 %u 的角色交接被跳过", partition_id),
				 errdetail("交接靠比对 primary_node 与本节点编号来判方向，"
						   "拿不到编号就无从判断 —— 宁可不做，也不猜。")));
		PG_RETURN_VOID();
	}

	loid = local_oid_for_shard(partition_id);
	if (!OidIsValid(loid))
		PG_RETURN_VOID();		/* 本节点不承载该分片：与我无关 */

	if (new_primary_node == me)
	{
		/*
		 * ★ 批次 #10：走 FRD §11 步骤 5 点名的入口 —— 它除了置角色，还把本地
		 * fileset 登记进捕获反向哈希。不做这一步，新主对外服务的**第一条写**
		 * 要等 EnsurePartWALRegistered 惰性登记才进流；在交接点显式做掉，
		 * 那个窗口就没了。
		 */
		/*
		 * ★ P7-T8（2026-09-14）：交接广播只在副本的 locmap 可能对着别人的文件号时发。
		 *   · old_primary_node != 0 —— 真正的切主：前任主可能已让副本改配对到它的号，
		 *     哪怕这次当选的是最初的 leader 也要广播；
		 *   · old_primary_node == 0 —— 首次登记：本节点若**不是副本**（没有配对过
		 *     locmap 的回放槽位），它就是 fileset 源头、副本本就按它配对，广播多余；
		 *     本节点若是副本（首次选举被副本抢到），它的号与源头不同，照样广播。
		 *   必须在 PartDistRoutePromoteEx 置"已升主"**之前**判：ShardReplicaIsLocal
		 *   对已升主的槽位返回 false。
		 *   修复前：首次登记时副本常常还没就绪，广播提案凑不齐多数派 ⇒ plsn=1 被拒两次
		 *   （txn_layer_r2 [3] 实测 quorum_drops +2、last_drop_plsn=1），孤儿重推的
		 *   ERROR 还会打断这次 apply。P7-T8 丢提案额度超限的**稳定**来源另有一处
		 *   （[9] 窗口里的心跳自动修复），见 P7_REMEDIATION_PLAN 该条。
		 */
		PartDistRoutePromoteEx(loid,
							   old_primary_node != 0 || ShardReplicaIsLocal(loid));
		ereport(LOG,
				(errmsg("pg_partdist: 分片 %u（本地 OID %u）已接管为主，"
						"解除副本读闸门；此后拒绝对它触发回放",
						partition_id, loid)));

		/*
		 * ★ P7-N25：前任主若没有回放槽位（原始 placement 主从没当过副本），它从此收着本节点
		 * 的流却无从回放、按 R-P4-15 永远不可升主。拉一个一次性工作者**逐个检查其余全部成员**，
		 * 没有 armed 槽位的替它重供基线（不只查 old_primary：条目里的前任主可能滞后失真，P7-N27）。
		 * 只在"确有前任"（非首次登记）时拉；自己重新登记自己（old == me）不管。
		 */
		if (old_primary_node > 0 && old_primary_node != me)
			PartDistLaunchReprovision((int64) partition_id, 0);	/* 0 = 逐个检查全部其余成员 */
	}
	/*
	 * ★ P7-N27：不能只信条目里的 old_primary —— 它取自提案方的 partition_map，会滞后成 0
	 * 或指向更早的主。真正要纠正的状态是"本节点还以为自己是主"，那就直接看本地的
	 * 持久"已升主"标记：新主不是我、而我还标着已升主 ⇒ 我就是被取代的那个。
	 */
	else if (old_primary_node == me || ShardPromotedMarkRead(loid))
	{
		ShardReplicaSetPromoted(loid, false);
		ereport(LOG,
				(errmsg("pg_partdist: 分片 %u（本地 OID %u）主权已交给节点 %d，"
						"本节点收回「已升主」身份，读闸门重新合上",
						partition_id, loid, new_primary_node)),
				errdetail("本地这份数据自此可能落后于新主；要重新作为副本参与，"
						  "须先判定有无快路径分叉（§9.5），必要时重做物理基线。"));
	}

	PG_RETURN_VOID();
}

/* ------------------------------------------------------------------ */
/* P7-N12（之二）— 升主节点把 in-doubt 决议映射回分片 xid 并落账          */
/* ------------------------------------------------------------------ */

/*
 * 缺陷现场（2026-09-17 跨组转账 + 切主验收）：新主的升主序列跑 dtx_close_indoubt，
 * 找到判决后只追加一条 DTX_COMMIT/ABORT 记录 —— 它既不写本节点的分片 clog，回放侧
 * 也不认（分片 clog 只在回放**带分片 xid 的 MARKER** 时落判决，shard_replay.c）。
 * 于是新主自己和它后面的副本对这笔事务永远停在 PREPARED：已提交的行不可见、
 * 转账的一侧丢了（总额 24000→24002，最终主上 4 个 PREPARED 残留，复制失败告警 0 条）。
 *
 * 映射依据：参与者 PREPARE 时先追加 DTX_PREPARE(dtxid)，紧接着追加带分片 xid 尾的
 * PREPARE MARKER（dtx_participant.c），两条记录的**头部 gxid 相同**（同一笔事务的
 * 本地 xid）。按 dtxid 找到 DTX_PREPARE 的头部 gxid，再在其后找同 gxid 的 MARKER，
 * 尾块里就是分片 xid。
 */
static bool partwal_find_record(Oid partition_id, uint64 target,
								PartWALRecord *out_rec, char **out_data);

PG_FUNCTION_INFO_V1(pg_partdist_partwal_prepare_sxid);
PG_FUNCTION_INFO_V1(pg_partdist_shard_verdict_apply);

Datum
pg_partdist_partwal_prepare_sxid(PG_FUNCTION_ARGS)
{
	Oid				partition_id = PG_GETARG_OID(0);
	int64			dtxid = PG_GETARG_INT64(1);
	int64			upto = PG_GETARG_INT64(2);
	TupleDesc		tupdesc;
	Datum			values[2];
	bool			nulls[2];
	HeapTuple		tuple;
	uint64			p;
	uint64			hit_plsn = 0;
	GlobalTransactionId hdr_gxid = 0;
	TransactionId	sxid = InvalidTransactionId;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR, (errmsg("partwal_prepare_sxid: 返回类型必须是 record")));
	tupdesc = BlessTupleDesc(tupdesc);

	/* 1) 找 DTX_PREPARE(dtxid) 的头部 gxid */
	for (p = 1; p <= (uint64) upto && hit_plsn == 0; p++)
	{
		PartWALRecord	rec;
		char		   *data = NULL;

		if (!partwal_find_record(partition_id, p, &rec, &data))
			continue;
		if ((rec.flags & PARTWAL_FLAG_DTX) != 0 && data != NULL &&
			rec.data_len >= sizeof(DtxRecordPayload) &&
			rec.info == DTX_PREPARE &&
			((DtxRecordPayload *) data)->dtxid == (uint64) dtxid)
		{
			hit_plsn = p;
			hdr_gxid = rec.gxid;
		}
		if (data != NULL)
			pfree(data);
		CHECK_FOR_INTERRUPTS();
	}

	/* 2) 其后 64 条内找同 gxid、带分片 xid 尾的 MARKER */
	for (p = hit_plsn + 1; hit_plsn != 0 && p <= hit_plsn + 64 && p <= (uint64) upto &&
		 !TransactionIdIsValid(sxid); p++)
	{
		PartWALRecord	rec;
		char		   *data = NULL;

		if (!partwal_find_record(partition_id, p, &rec, &data))
			continue;
		if ((rec.flags & PARTWAL_FLAG_MARKER) != 0 && rec.gxid == hdr_gxid &&
			data != NULL && rec.data_len >= sizeof(TxnMarkerPayload))
		{
			TxnMarkerPayload *m = (TxnMarkerPayload *) data;
			Size			  base = TxnMarkerPayloadSize(m->nsubxacts);

			if ((m->flags & PARTWAL_MARKER_HAS_SHARD_XID) != 0 &&
				rec.data_len >= base + sizeof(TransactionId))
				memcpy(&sxid, data + base, sizeof(TransactionId));
		}
		if (data != NULL)
			pfree(data);
	}

	memset(nulls, 0, sizeof(nulls));
	if (!TransactionIdIsNormal(sxid))
	{
		nulls[0] = true;
		nulls[1] = true;
	}
	values[0] = Int64GetDatum((int64) sxid);
	values[1] = Int64GetDatum((int64) hdr_gxid);
	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * shard_verdict_apply(partition_id, sxid, hdr_gxid, committed, commit_ts) → bool
 *
 * 与 leader 侧 dtx_pending.c 的 dtx_replicate_verdict 同构：先落本地分片 clog
 * （start_ts 取 PREPARED 槽里的），再追加带分片 xid 的判决 MARKER 并 flush 一轮
 * ——本节点此刻是该组 leader（升主序列），副本随流得到同一本账。
 * 复制失败不上抛（本地账已落是底线），返回 false。
 */
Datum
pg_partdist_shard_verdict_apply(PG_FUNCTION_ARGS)
{
	Oid				partition_id = PG_GETARG_OID(0);
	TransactionId	sxid = (TransactionId) PG_GETARG_INT64(1);
	GlobalTransactionId hdr_gxid = (GlobalTransactionId) PG_GETARG_INT64(2);
	bool			committed = PG_GETARG_BOOL(3);
	int64			cts = PG_GETARG_INT64(4);
	ShardClogSlot	slot;
	int64			sts = 0;
	char		   *payload = NULL;
	uint32			payload_len = 0;
	bool			ok = false;

	if (!OidIsValid(partition_id) || !TransactionIdIsNormal(sxid))
		PG_RETURN_BOOL(false);

	if (ShardClogReadSlot(partition_id, sxid, &slot))
		sts = (int64) slot.start_ts;
	ShardClogSetVerdict(partition_id, sxid, committed, committed ? cts : 0);

	PG_TRY();
	{
		payload = PartWALBuildVerdictMarker(sts, committed ? cts : 0, &payload_len);
		PartWALAppendMarkerForShardXid(partition_id, GxidLocalXid(hdr_gxid),
									   committed ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT,
									   payload, payload_len, sxid);
		PartWALNoteTouchedPartition(partition_id);
		PartWALFlush(InvalidXLogRecPtr, false);
		ok = true;
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		MemoryContextSwitchTo(TopMemoryContext);
		ed = CopyErrorData();
		FlushErrorState();
		ereport(WARNING,
				(errmsg("pg_partdist: 升主闭合 in-doubt：分片 %u xid %u 判决已落本地 clog，"
						"但判决标记未能复制给副本：%s",
						partition_id, sxid, ed->message)));
		FreeErrorData(ed);
	}
	PG_END_TRY();
	if (payload != NULL)
		pfree(payload);
	PG_RETURN_BOOL(ok);
}

/* ------------------------------------------------------------------ */
/* P2 — 数据面 Raft 组的 parwal 边界函数                                */
/* ------------------------------------------------------------------ */

/*
 * 分区 Raft 组把 parwal 记录当作 Raft entry 复制：
 *
 *   partwal_read_record(partition_id, partition_lsn)
 *       Leader 侧：按 partition_lsn 从本节点 pg_parwal/<partition_id>/ 读出
 *       一条完整记录（头部字段 + 原始 WAL 字节），交给 pg_raft 打包成 entry。
 *
 *   partwal_follower_append(partition_id, partition_lsn, orig_lsn,
 *                           rmid, info, flags, gxid, data)
 *       Follower 侧的"平凡 apply"：把收到的记录**原样落盘**到本节点自己的
 *       pg_parwal/<partition_id>/（本节点 local_oid 与 leader 不同，由调用方
 *       用 P0 的 local_partition_for_shard(global_shard_id) 解析），fsync 后
 *       返回本地写入的 partition_lsn。此处不做 redo —— 物理回放是 P3。
 *
 *   follower_set_applied_part_lsn(partition_id, lsn)
 *       推进 follower_partition_map.applied_part_lsn。这是该表**第一个真实的
 *       C 写入方**：在此之前进度列恒为占位 0，切主安全线无从比较。
 */
PG_FUNCTION_INFO_V1(pg_partdist_partwal_read_record);
PG_FUNCTION_INFO_V1(pg_partdist_partwal_follower_append);
PG_FUNCTION_INFO_V1(pg_partdist_partwal_follower_append_fresh);
PG_FUNCTION_INFO_V1(pg_partdist_follower_set_applied_part_lsn);
PG_FUNCTION_INFO_V1(pg_partdist_partwal_truncate_to);

/*
 * 在 pg_parwal/<partition_id>/ 的段文件里定位 partition_lsn == target 的记录。
 * 命中则填充 *out_rec 并把原始字节 palloc 到 *out_data，返回 true。
 *
 * 这是**全扫描**实现（列目录 → 段名排序 → 从第一段开头逐条读），单次 O(n)。
 * P7-N4 之后它只作为 partwal_find_record 的兜底，见下方稀疏索引。
 */
static bool
partwal_find_record_scan(Oid partition_id, uint64 target,
						 PartWALRecord *out_rec, char **out_data)
{
	char			dirpath[MAXPGPATH];
	DIR			   *dir;
	struct dirent  *de;
	char			segfiles[256][MAXPGPATH];
	int				nfiles = 0;
	int				i,
					j;
	bool			found = false;

	snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
			 DataDir, PARTITION_WAL_DIR, partition_id);

	dir = AllocateDir(dirpath);
	if (dir == NULL)
		return false;

	while ((de = ReadDir(dir, dirpath)) != NULL && nfiles < 256)
	{
		if (IsXLogFileName(de->d_name))
		{
			strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
			nfiles++;
		}
	}
	FreeDir(dir);

	if (nfiles == 0)
		return false;

	/* 段文件名升序即 partition_lsn 升序 */
	for (i = 0; i < nfiles - 1; i++)
		for (j = i + 1; j < nfiles; j++)
			if (strcmp(segfiles[i], segfiles[j]) > 0)
			{
				char		tmp[MAXPGPATH];

				strlcpy(tmp, segfiles[i], MAXPGPATH);
				strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
				strlcpy(segfiles[j], tmp, MAXPGPATH);
			}

	for (i = 0; i < nfiles && !found; i++)
	{
		char			filepath[MAXPGPATH];
		int				fd;
		PartWALRecord	rec;
		ssize_t			nb;

		snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
		fd = OpenTransientFile(filepath, O_RDONLY | PG_BINARY);
		if (fd < 0)
			continue;

		while ((nb = read(fd, &rec, sizeof(PartWALRecord)))
			   == (ssize_t) sizeof(PartWALRecord))
		{
			if (rec.magic != PARTWAL_MAGIC)
				break;

			if (rec.partition_id == partition_id &&
				rec.partition_lsn == target)
			{
				char   *buf = NULL;

				if (rec.data_len > 0)
				{
					buf = (char *) palloc(rec.data_len);
					if (read(fd, buf, rec.data_len) != (ssize_t) rec.data_len)
					{
						pfree(buf);
						break;
					}
				}
				*out_rec = rec;
				*out_data = buf;
				found = true;
				break;
			}

			if (rec.data_len > 0 &&
				lseek(fd, (off_t) rec.data_len, SEEK_CUR) < 0)
				break;
		}

		CloseTransientFile(fd);
	}

	return found;
}

/* ------------------------------------------------------------------ */
/* P7-N4 / P7-N7：按 plsn 查记录的后端本地稀疏索引                      */
/* ------------------------------------------------------------------ */

/*
 * 上面的全扫描单次 O(n)，却有三个热调用方：
 *   · 复制：leader 每复制一条记录要读两次（data_propose_one 取记录头、
 *     data_entry_fetch_hex 取字节），follower 追加时查重再读一次 ⇒ 写满 n 条总代价 O(n²)。
 *     2026-09-17 实测同一组吞吐从每秒二十多条掉到个位数，1500 行的跨组事务跑了 229 s；
 *   · 升主前置的 dtx_close_indoubt：对 1..flush_lsn 每个 plsn 各读一次 ⇒ O(n²)，
 *     1300 条记录要 51 s；
 *   · 升主前置的快路径分叉检查：从尾部往回读 200 条，每条都从流开头扫起。
 *   后两者叠加让升主前置跑到 69 s，是 P7-N4 活锁的成本面。
 *
 * 做法：每个 backend 为最近用过的若干分区各记一份稀疏索引 —— 段文件名列表 + 每隔
 * PWIDX_STRIDE 条记录一个检查点 (plsn, 段, 偏移, 载荷长度)。查 target 时二分找
 * plsn ≤ target 的最近检查点，**先回读该偏移处的记录头逐字段核对**，通过才从那里
 * 往后顺序读（通常不超过 STRIDE 条就命中）；没有索引时从流开头读并顺手建索引。
 *
 * 正确性只依赖一条既有不变式：**同一分区流里记录按 plsn 严格递增排列，段文件名升序
 * 即 plsn 升序**（全扫描"找到第一条就返回"同样依赖它）。追加者对 plsn ≤ 本地最大编号
 * 的记录只做两件事：当重传丢弃，或先 TruncatePartWALTo(plsn-1) 再写
 * （partwal_follower_append）—— 都不会在更小的编号前面插入。所以一个核对通过的检查点
 * 之前不可能有 ≥ 它的编号，从它往后顺序读到的第一条 == target 的记录，就是全扫描会
 * 返回的那一条；读到的永远是文件**此刻**的内容，不是缓存。
 *
 * 兜底：检查点核对不过（截断、段被删、文件被重写成别的内容）⇒ 丢掉该分区索引；
 * 快路径没找到 ⇒ **一律退回全扫描**。最坏情况就是改动前的行为。索引只在本 backend
 * 内有效（TopMemoryContext），不进共享内存，不需要任何跨进程失效通知。
 */
#define PWIDX_STRIDE		32
#define PWIDX_MAX_PARTS		8
#define PWIDX_MAX_POINTS	131072		/* ≈ 420 万条记录；满了不再加点，仍从最后一点往后读 */
#define PWIDX_MAX_SEGS		256			/* 与全扫描一致 */
#define PWIDX_SEGNAME_LEN	32

typedef struct PwIdxPoint
{
	uint64		plsn;
	off_t		off;
	uint32		data_len;
	int			seg;
} PwIdxPoint;

typedef struct PwIdx
{
	Oid			partition_id;	/* InvalidOid = 空槽 */
	uint64		last_use;
	int			nsegs;
	char		segs[PWIDX_MAX_SEGS][PWIDX_SEGNAME_LEN];
	int			npoints;
	int			cap;
	PwIdxPoint *points;			/* plsn 严格升序 */
} PwIdx;

static PwIdx pwidx_slots[PWIDX_MAX_PARTS];
static uint64 pwidx_clock = 0;

bool		partwal_record_index = true;	/* GUC pg_partdist.partwal_record_index */

static void
pwidx_drop(PwIdx *ix)
{
	if (ix->points != NULL)
		pfree(ix->points);
	memset(ix, 0, sizeof(PwIdx));
}

static int
pwidx_segname_cmp(const void *a, const void *b)
{
	return strcmp((const char *) a, (const char *) b);
}

/* 列出分区目录下的段文件名（升序）。失败/为空返回 -1 / 0。 */
static int
pwidx_list_segs(Oid partition_id, char segs[][PWIDX_SEGNAME_LEN])
{
	char			dirpath[MAXPGPATH];
	DIR			   *dir;
	struct dirent  *de;
	int				n = 0;

	snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
			 DataDir, PARTITION_WAL_DIR, partition_id);
	dir = AllocateDir(dirpath);
	if (dir == NULL)
		return -1;
	while ((de = ReadDir(dir, dirpath)) != NULL && n < PWIDX_MAX_SEGS)
	{
		if (IsXLogFileName(de->d_name))
		{
			strlcpy(segs[n], de->d_name, PWIDX_SEGNAME_LEN);
			n++;
		}
	}
	FreeDir(dir);
	if (n > 1)
		qsort(segs, n, PWIDX_SEGNAME_LEN, pwidx_segname_cmp);
	return n;
}

/* 取（或新建并建段表）该分区的索引槽；新建时按 LRU 淘汰。 */
static PwIdx *
pwidx_get(Oid partition_id)
{
	PwIdx	   *victim = NULL;
	int			i;

	for (i = 0; i < PWIDX_MAX_PARTS; i++)
	{
		PwIdx	   *ix = &pwidx_slots[i];

		if (ix->partition_id == partition_id)
		{
			ix->last_use = ++pwidx_clock;
			return ix;
		}
		if (victim == NULL || ix->partition_id == InvalidOid ||
			(victim->partition_id != InvalidOid && ix->last_use < victim->last_use))
			victim = ix;
	}

	pwidx_drop(victim);
	victim->nsegs = pwidx_list_segs(partition_id, victim->segs);
	if (victim->nsegs <= 0)
	{
		memset(victim, 0, sizeof(PwIdx));
		return NULL;
	}
	victim->partition_id = partition_id;
	victim->last_use = ++pwidx_clock;
	return victim;
}

static void
pwidx_add_point(PwIdx *ix, uint64 plsn, int seg, off_t off, uint32 data_len)
{
	if (ix->npoints > 0 && plsn <= ix->points[ix->npoints - 1].plsn)
		return;
	if (ix->npoints >= PWIDX_MAX_POINTS)
		return;
	if (ix->npoints >= ix->cap)
	{
		int			ncap = (ix->cap == 0) ? 256 : ix->cap * 2;

		if (ncap > PWIDX_MAX_POINTS)
			ncap = PWIDX_MAX_POINTS;
		if (ix->points == NULL)
			ix->points = (PwIdxPoint *)
				MemoryContextAlloc(TopMemoryContext, sizeof(PwIdxPoint) * ncap);
		else
			ix->points = (PwIdxPoint *)
				repalloc(ix->points, sizeof(PwIdxPoint) * ncap);
		ix->cap = ncap;
	}
	ix->points[ix->npoints].plsn = plsn;
	ix->points[ix->npoints].seg = seg;
	ix->points[ix->npoints].off = off;
	ix->points[ix->npoints].data_len = data_len;
	ix->npoints++;
}

/*
 * 越过已知段表末尾时重新列目录：已知的段名必须原样是新列表的前缀，
 * 否则（段被删/改名）返回 false，调用方丢弃索引。
 */
static bool
pwidx_refresh_segs(PwIdx *ix)
{
	char		fresh[PWIDX_MAX_SEGS][PWIDX_SEGNAME_LEN];
	int			n = pwidx_list_segs(ix->partition_id, fresh);
	int			i;

	if (n < ix->nsegs)
		return false;
	for (i = 0; i < ix->nsegs; i++)
		if (strcmp(fresh[i], ix->segs[i]) != 0)
			return false;
	for (i = ix->nsegs; i < n; i++)
		strlcpy(ix->segs[i], fresh[i], PWIDX_SEGNAME_LEN);
	ix->nsegs = n;
	return true;
}

/*
 * 快路径。返回 1 = 命中（已填 out）；0 = 没找到（调用方退回全扫描）；
 * -1 = 索引失效（已丢弃，调用方退回全扫描）。
 */
static int
partwal_find_record_indexed(Oid partition_id, uint64 target,
							PartWALRecord *out_rec, char **out_data)
{
	PwIdx	   *ix = pwidx_get(partition_id);
	int			seg = 0;
	off_t		off = 0;
	bool		extend;
	int			since;

	if (ix == NULL)
		return 0;

	if (ix->npoints > 0)
	{
		int			lo = 0,
					hi = ix->npoints - 1,
					best = -1;

		if (target < ix->points[0].plsn)
			return 0;			/* 比流里最早一条还小：不存在，交全扫描确认 */
		while (lo <= hi)
		{
			int			mid = (lo + hi) / 2;

			if (ix->points[mid].plsn <= target)
			{
				best = mid;
				lo = mid + 1;
			}
			else
				hi = mid - 1;
		}
		seg = ix->points[best].seg;
		off = ix->points[best].off;
		/* 只有从最后一个检查点往后读时才顺手延长索引 */
		extend = (best == ix->npoints - 1);
		since = 0;

		/* 核对检查点：该偏移处必须还是那一条记录 */
		{
			char		filepath[MAXPGPATH];
			int			fd;
			PartWALRecord rec;
			bool		ok = false;

			snprintf(filepath, MAXPGPATH, "%s/%s/%u/%s", DataDir,
					 PARTITION_WAL_DIR, partition_id, ix->segs[seg]);
			fd = OpenTransientFile(filepath, O_RDONLY | PG_BINARY);
			if (fd >= 0)
			{
				if (lseek(fd, off, SEEK_SET) == off &&
					read(fd, &rec, sizeof(PartWALRecord)) == (ssize_t) sizeof(PartWALRecord) &&
					rec.magic == PARTWAL_MAGIC &&
					rec.partition_id == partition_id &&
					rec.partition_lsn == ix->points[best].plsn &&
					rec.data_len == ix->points[best].data_len)
					ok = true;
				CloseTransientFile(fd);
			}
			if (!ok)
			{
				pwidx_drop(ix);
				return -1;
			}
		}
	}
	else
	{
		extend = true;
		since = PWIDX_STRIDE;	/* 流里第一条记录也记成检查点 */
	}

	for (;;)
	{
		char		filepath[MAXPGPATH];
		int			fd;
		PartWALRecord rec;

		if (seg >= ix->nsegs && !pwidx_refresh_segs(ix))
		{
			pwidx_drop(ix);
			return -1;
		}
		if (seg >= ix->nsegs)
			return 0;			/* 读到流尾也没有 */

		snprintf(filepath, MAXPGPATH, "%s/%s/%u/%s", DataDir,
				 PARTITION_WAL_DIR, partition_id, ix->segs[seg]);
		fd = OpenTransientFile(filepath, O_RDONLY | PG_BINARY);
		if (fd < 0)
		{
			pwidx_drop(ix);
			return -1;
		}
		if (off > 0 && lseek(fd, off, SEEK_SET) != off)
		{
			CloseTransientFile(fd);
			pwidx_drop(ix);
			return -1;
		}

		while (read(fd, &rec, sizeof(PartWALRecord)) == (ssize_t) sizeof(PartWALRecord))
		{
			off_t		rec_off = off;

			if (rec.magic != PARTWAL_MAGIC)
				break;			/* 与全扫描一致：本段到此为止 */
			off += (off_t) sizeof(PartWALRecord) + (off_t) rec.data_len;

			if (rec.partition_id == partition_id)
			{
				if (extend && ++since >= PWIDX_STRIDE)
				{
					pwidx_add_point(ix, rec.partition_lsn, seg, rec_off, rec.data_len);
					since = 0;
				}

				if (rec.partition_lsn == target)
				{
					char	   *buf = NULL;

					if (rec.data_len > 0)
					{
						buf = (char *) palloc(rec.data_len);
						if (read(fd, buf, rec.data_len) != (ssize_t) rec.data_len)
						{
							pfree(buf);
							CloseTransientFile(fd);
							return 0;
						}
					}
					CloseTransientFile(fd);
					*out_rec = rec;
					*out_data = buf;
					return 1;
				}
				if (rec.partition_lsn > target)
				{
					CloseTransientFile(fd);
					return 0;	/* 严格递增：后面不会再有 target */
				}
			}

			if (rec.data_len > 0 &&
				lseek(fd, (off_t) rec.data_len, SEEK_CUR) < 0)
				break;
		}
		CloseTransientFile(fd);
		seg++;
		off = 0;
	}
}

static bool
partwal_find_record(Oid partition_id, uint64 target,
					PartWALRecord *out_rec, char **out_data)
{
	if (partwal_record_index &&
		partwal_find_record_indexed(partition_id, target, out_rec, out_data) == 1)
		return true;
	return partwal_find_record_scan(partition_id, target, out_rec, out_data);
}

Datum
pg_partdist_partwal_read_record(PG_FUNCTION_ARGS)
{
	Oid				partition_id = PG_GETARG_OID(0);
	int64			partition_lsn = PG_GETARG_INT64(1);
	PartWALRecord	rec;
	char		   *data = NULL;
	TupleDesc		tupdesc;
	Datum			values[6];
	bool			nulls[6];
	HeapTuple		tuple;
	bytea		   *payload;

	if (partition_lsn <= 0)
		PG_RETURN_NULL();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errmsg("partwal_read_record: 返回类型必须是 record")));
	tupdesc = BlessTupleDesc(tupdesc);

	if (!partwal_find_record(partition_id, (uint64) partition_lsn, &rec, &data))
		PG_RETURN_NULL();

	payload = (bytea *) palloc(VARHDRSZ + rec.data_len);
	SET_VARSIZE(payload, VARHDRSZ + rec.data_len);
	if (rec.data_len > 0)
		memcpy(VARDATA(payload), data, rec.data_len);

	/*
	 * gxid 走 int64 传输：节点号最多 16 位、本地 xid 48 位，合起来 64 位里
	 * 最高位恒为 0，所以塞进有符号 bigint 不会变负数。
	 *
	 * 2.0 老段流由 PartWALRecordGxid() 归一成 node=0 的 gxid —— 直接整读
	 * rec.gxid 会把 36..39 的结构体填充垃圾当成节点号。
	 */
	memset(nulls, 0, sizeof(nulls));
	values[0] = LSNGetDatum(rec.orig_lsn);
	values[1] = Int32GetDatum((int32) rec.rmid);
	values[2] = Int32GetDatum((int32) rec.info);
	values[3] = Int32GetDatum((int32) rec.flags);
	values[4] = Int64GetDatum((int64) PartWALRecordGxid(&rec));
	values[5] = PointerGetDatum(payload);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	if (data != NULL)
		pfree(data);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

PG_FUNCTION_INFO_V1(pg_partdist_shard_absorb_tail_xids);

/*
 * shard_absorb_tail_xids(partition_id, from_plsn, upto_plsn) → int
 *
 * ★ P7-N18（2026-09-18，之二）：升主前把 (from_plsn, upto_plsn] 这截**已复制、可能
 * 还没 apply** 的分区流里出现过的分片 xid 全部吸收进发号水位。
 *
 * 为什么需要它：MARKER 的发号水位 follower append 时已抬（见 follower_append_impl），
 * 但**单分片 heap 写的分片 xid 盖在 DATA 记录的 xmin/xmax 上、只由回放侧 redo 时喂进影子**
 * （ShardReplayNoteDataShardXid）。双重快切 + 可用性放行时新主 applied 远落后于 flush，
 * 那截未 redo 的 DATA 尾巴里的分片 xid 既不在水位、也不在影子 ⇒ ClaimOnPromote 取
 * Max(水位,影子) 仍偏小 ⇒ 新主重发旧宇宙用过的号 ⇒ 老元组"复活"成重复行（实测账户 700011
 * 被复读、总额 +1006）。这里在升主前置里一次性把尾巴扫一遍、解码 DATA 记录取分片 xid，
 * 连同 MARKER 尾块的发号水位，一并抬过 —— ClaimOnPromote 随后 [claim_wm, watermark)
 * 把这些号里没有提交标记的改判 ABORTED（老元组保持不可见），发号也不再撞。
 *
 * 只扫 (from_plsn, upto] —— 调用方传 from = 本节点回放 applied 游标（其下的号 redo 时
 * 已进影子），upto = flush。DATA 解码复用回放侧同一条路径（DecodeXLogRecord +
 * ShardDataRecordShardXid）。返回吸收到的记录条数（诊断用）。
 */
Datum
pg_partdist_shard_absorb_tail_xids(PG_FUNCTION_ARGS)
{
	Oid				partition_id = PG_GETARG_OID(0);
	int64			from = PG_GETARG_INT64(1);
	int64			upto = PG_GETARG_INT64(2);
	uint64			p;
	XLogReaderState *reader;
	TransactionId	max_next = InvalidTransactionId;   /* 要保证的 next_xid 下界 */
	int				n = 0;

	if (upto <= 0)
		PG_RETURN_INT32(0);
	if (from < 0)
		from = 0;

	reader = XLogReaderAllocate(wal_segment_size, NULL, XL_ROUTINE(), NULL);
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("shard_absorb_tail_xids: 无法分配 XLogReader")));

	for (p = (uint64) (from + 1); p <= (uint64) upto; p++)
	{
		PartWALRecord	rec;
		char		   *data = NULL;
		TransactionId	cand = InvalidTransactionId;

		if (!partwal_find_record(partition_id, p, &rec, &data))
			continue;

		if ((rec.flags & PARTWAL_FLAG_MARKER) != 0 && data != NULL &&
			rec.data_len >= sizeof(TxnMarkerPayload))
		{
			TxnMarkerPayload *m = (TxnMarkerPayload *) data;

			if (rec.data_len >= (uint32) TxnMarkerPayloadSizeEx(m->nsubxacts, m->flags))
			{
				if ((m->flags & PARTWAL_MARKER_HAS_ALLOC_WM) != 0)
					cand = (TransactionId) *TxnMarkerAllocWmPtr(m);        /* 已是 next_xid */
				else if ((m->flags & PARTWAL_MARKER_HAS_SHARD_XID) != 0)
					cand = (TransactionId) (*TxnMarkerShardXidPtr(m) + 1);
			}
		}
		else if (data != NULL &&
				 (rec.flags & (PARTWAL_FLAG_MARKER | PARTWAL_FLAG_CTRL |
							   PARTWAL_FLAG_DTX)) == 0 &&
				 (rec.rmid == RM_HEAP_ID || rec.rmid == RM_HEAP2_ID) &&
				 rec.data_len >= SizeOfXLogRecord)
		{
			XLogRecord *xrec = (XLogRecord *) data;

			if (xrec->xl_tot_len == rec.data_len)
			{
				DecodedXLogRecord *dec;
				char			  *err = NULL;

				dec = (DecodedXLogRecord *)
					palloc(DecodeXLogRecordRequiredSpace(xrec->xl_tot_len));
				if (DecodeXLogRecord(reader, dec, xrec, rec.orig_lsn, &err))
				{
					TransactionId sx = ShardDataRecordShardXid(partition_id, xrec, dec);

					if (TransactionIdIsNormal(sx))
						cand = sx + 1;         /* 水位 = 已发号 + 1 */
				}
				pfree(dec);
			}
		}

		if (data != NULL)
			pfree(data);

		if (TransactionIdIsNormal(cand) &&
			(!TransactionIdIsValid(max_next) || cand > max_next))
		{
			max_next = cand;
			n++;
		}
		CHECK_FOR_INTERRUPTS();
	}

	XLogReaderFree(reader);

	if (TransactionIdIsNormal(max_next))
		ShardXidRaiseAllocWatermark(partition_id, max_next);

	PG_RETURN_INT32(n);
}

static Datum follower_append_impl(FunctionCallInfo fcinfo, bool fresh_append);

Datum
pg_partdist_partwal_follower_append(PG_FUNCTION_ARGS)
{
	return follower_append_impl(fcinfo, false);
}

/*
 * T7.24：与上面同参数，但声明"这条是新追加"（落在本节点 raft 日志末尾之后）。
 * 另起入口而不是给原函数加第 9 个参数 —— 加参数要先 DROP 旧签名，9 个节点
 * 滚动升级期间会出现"新 pg_raft 调旧签名"的重载歧义窗口。
 */
Datum
pg_partdist_partwal_follower_append_fresh(PG_FUNCTION_ARGS)
{
	return follower_append_impl(fcinfo, true);
}

static Datum
follower_append_impl(FunctionCallInfo fcinfo, bool fresh_append)
{
	Oid					partition_id = PG_GETARG_OID(0);
	int64				partition_lsn = PG_GETARG_INT64(1);
	XLogRecPtr			orig_lsn = PG_GETARG_LSN(2);
	int32				rmid = PG_GETARG_INT32(3);
	int32				info = PG_GETARG_INT32(4);
	int32				flags = PG_GETARG_INT32(5);
	int64				gxid = PG_GETARG_INT64(6);
	bytea			   *data = PG_GETARG_BYTEA_PP(7);
	PartitionWALWriter *writer;
	volatile bool		foreign_heap_rec = false;	/* P7-N23 */

	/*
	 * ★ 与其余段文件写入者互斥（见 AppendDtxRecord 里那段长注释）。
	 *
	 * 本函数虽然按 leader 指定的编号落盘、不做自增分配，但它同样要
	 * 读-改-写 checkpoint：writer 从 checkpoint 播种 last_partition_lsn，
	 * 落盘后回写。与并发的 PartWALFlush 交错会让"顺序追加"被误判成
	 * 重传去重（silently 丢字节）或空洞（ERROR 拒绝 ack）。
	 */
	if (PartWALCtl != NULL)
		LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		/*
		 * relfilenode 只用于 checkpoint 记账；follower 侧沿用 partition_id 本身，
		 * 与 demux 崩溃恢复对该目录的既有约定一致。
		 */
		writer = CreatePartitionWALWriter(partition_id, (RelFileNumber) partition_id);
		if (writer == NULL)
			ereport(ERROR,
					(errmsg("partwal_follower_append: 无法为分区 %u 创建写入器",
							partition_id)));

		/*
		 * ★★ R-P4-13 的真根因（2026-09-13 定因）：**去重只按编号、不看内容**。
		 *
		 * AppendPartWALRecordAt 对 `plsn <= 本地已有最大编号` 一律当"重传"丢弃。
		 * 可本地那一条未必是这一条：
		 *   · 某节点当 leader 时写了本地 DATA（[A] 先落盘），提案却没提交
		 *     （失多数派 / 丢了领导权）—— discard_uncommitted_entry 按设计
		 *     **不截 parwal 字节**（"留作孤儿等重推"，防误截并发事务的记录）；
		 *   · 新 leader 的日志是完整的（Leader Completeness），它把同一个
		 *     plsn 分给了**另一条**已提交的记录（实测是 DTX 判决）；
		 *   · 复制到旧 leader 这里：编号 <= 本地最大 ⇒ 判"重传" ⇒ 丢弃。
		 *   于是**副本流里躺着一条从未提交的 DATA，冒充那条已提交的判决**。
		 *   apply 照推游标，按 plsn 去读判决却读到 DATA，决议行从此登记不上 ——
		 *   这就是 R-P4-13 "日志游标齐平、本地却无决议行、且无任何告警"。
		 *   实测现场：:5433（分片本体）的 plsn=15 是 flags=1 info=0，而 raft
		 *   日志说 plsn=15 是 flags=8 info=2。
		 *
		 *   决议行缺失只是露出来的症状；同一机制下，被顶替的若是 DATA，
		 *   回放就会在副本上重放一条从未提交的物理变更。
		 *
		 * 修法：编号已存在时**逐字段核对**。一致 ⇒ 真重传，照旧去重；不一致 ⇒
		 * 本地那条是孤儿，按已提交的这条为准：截到 plsn-1 再写。
		 *
		 * 为什么"以传进来的为准"一定对：新 leader 日志完整，它能把这个编号分给
		 * 别的记录，说明**没有任何已提交记录用过这个编号** —— 本地那条必然未提交。
		 * （这与 raft 日志冲突截断时同步截 parwal 是同一条规则，只是那条路径只在
		 * 日志里真的有冲突条目时才会触发，而孤儿字节对应的日志条目早已被 discard。）
		 */
		if ((uint64) partition_lsn <= writer->last_partition_lsn)
		{
			PartWALRecord	old;
			char		   *old_data = NULL;
			bool			same = false;

			if (partwal_find_record(partition_id, (uint64) partition_lsn,
									&old, &old_data))
			{
				same = (old.flags == (uint8) flags &&
						old.info == (uint8) info &&
						old.rmid == (uint8) rmid &&
						old.orig_lsn == orig_lsn &&
						old.data_len == (uint32) VARSIZE_ANY_EXHDR(data) &&
						(old.data_len == 0 ||
						 memcmp(old_data, VARDATA_ANY(data), old.data_len) == 0));
				if (old_data != NULL)
					pfree(old_data);
			}

			/*
			 * ★★ 只有**新追加**时才许截（2026-09-13 实测纠正第一版）。
			 *
			 * 第一版不分情形一律"截到 plsn-1 再写"。那**只截了分区流、没截
			 * raft 日志**：若这条是 leader 的**重传**（条目本来就在本节点
			 * raft 日志里），流里更靠后的记录可能是已提交、已 ack 的 ——
			 * 截掉之后 leader 按 match_index 不会重发，那段字节就永久丢了。
			 * 实测：一次重传 plsn=2 触发替换，把 3..51 一并截掉，
			 * dtx_replay_tx1 由 83/0 掉到 76/7。
			 *
			 * 两种情形分开处置：
			 *   · 新追加：本节点 raft 日志里没有比它更靠后的条目，分区流里
			 *     更靠后的记录**必然**是孤儿（它们对应的日志条目早被 discard）
			 *     ⇒ 截掉是安全的。R-P4-13 正是这一种。
			 *   · 重传：不截。本地字节与已提交记录不一致是**已经发生**的分叉
			 *     （改动前它会被静默去重吞掉），标记分叉交给既有修复路径
			 *     （重做物理基线会清标），并照常 ack —— 不 ack 只会让 leader
			 *     无限重试同一条，复制卡死。
			 */
			if (!same && !fresh_append)
			{
				char reason[256];

				snprintf(reason, sizeof(reason),
						 "follower_append: plsn=%lld 本地字节（flags=%u info=%u）与重传的已提交记录"
						 "（flags=%d info=%d）不一致",
						 (long long) partition_lsn, (unsigned) old.flags,
						 (unsigned) old.info, flags, info);
				ShardMarkDiverged(partition_id, reason);
				ereport(WARNING,
						(errmsg("partwal_follower_append: 分区 %u 的 plsn=%lld 与重传的已提交记录不一致，"
								"已标记分叉（不截断）", partition_id, (long long) partition_lsn),
						 errdetail("%s", reason),
						 errhint("该副本需重做物理基线（partdist.repair_diverged_shards()）。")));
			}
			else if (!same)
			{
				ereport(WARNING,
						(errmsg("partwal_follower_append: 分区 %u 的 plsn=%lld 本地是孤儿记录"
								"（flags=%u info=%u），被已提交的记录（flags=%d info=%d）取代",
								partition_id, (long long) partition_lsn,
								(unsigned) old.flags, (unsigned) old.info,
								flags, info),
						 errdetail("新追加：本节点 raft 日志里没有更靠后的条目，分区流里 %lld 之后"
								   "的记录必然是未提交提案留下的孤儿；按已提交记录为准截断重写。",
								   (long long) partition_lsn - 1)));
				DestroyPartitionWALWriter(writer);
				if (!TruncatePartWALTo(partition_id, (RelFileNumber) partition_id,
									   (uint64) partition_lsn - 1))
					ereport(ERROR,
							(errmsg("partwal_follower_append: 分区 %u 截到 %lld 失败，拒绝 ack",
									partition_id, (long long) partition_lsn - 1)));
				writer = CreatePartitionWALWriter(partition_id,
												  (RelFileNumber) partition_id);
				if (writer == NULL)
					ereport(ERROR,
							(errmsg("partwal_follower_append: 截断后无法为分区 %u 重建写入器",
									partition_id)));
			}
		}

		/*
		 * **按 leader 指定的 partition_lsn 落盘**，而不是本地自增。
		 *
		 * 本节点同时是若干分区的 primary、又是另一些分区的 secondary，同一个
		 * pg_parwal 目录树下既有本地 demux 写入、也有 follower 收到的复制流。
		 * 若 follower 用本地计数器，编号空间会和 leader 的永久错位，
		 * applied_part_lsn 指向的记录在本地根本不存在。
		 *
		 * 返回值：真正写入返回该编号；重传去重（已落过盘）也返回该编号 ——
		 * 对调用方而言"这条已持久化"是同一个结论，都应当 ack。
		 */
		/*
		 * ★ P7-N23：返回值 = 是否真的落了字节（重传去重返回 false）。真落下的一条
		 * DATA/MARKER 必然是**别的 leader** 写的 —— 本节点自己当主时的记录走 demux
		 * 本地落盘，从不经这里。CTRL/DTX 不算：它们不改堆也不改提交状态，
		 * dtx_close_indoubt 直接从流里读 DTX。
		 */
		if (AppendPartWALRecordAt(writer,
								  (uint64) partition_lsn,
								  orig_lsn,
								  (uint8) rmid,
								  (uint8) info,
								  VARDATA_ANY(data),
								  (uint32) VARSIZE_ANY_EXHDR(data),
								  (GlobalTransactionId) gxid,
								  (uint8) flags) &&
			(flags & (PARTWAL_FLAG_CTRL | PARTWAL_FLAG_DTX)) == 0)
			foreign_heap_rec = true;

		/*
		 * ★ P7-N18（2026-09-18，shardpg-test 线解冻）：**在 append 时就抬发号水位**。
		 *
		 * 缺陷：leader 把它的发号水位（HAS_ALLOC_WM）/分片 xid（HAS_SHARD_XID）盖在
		 * MARKER 尾块里，但原来只有 redo 侧（shard_replay.c）读它抬水位。升主前置的追平
		 * 上界只到本节点 applied_part_lsn（get_follower_applied_part_lsn）—— 2 vCPU 饱和、
		 * in_txn_replication 跳过推进、或 p_force 兜底放行时，[applied+1, flush] 这截
		 * **已复制、未 apply** 的尾巴里的分片 xid 没进水位；新主 ShardXidClaimOnPromote 取
		 * Max(水位, 影子) 仍是陈旧值 ⇒ 重发旧主已用过、已提交的分片 xid ⇒ 分片 clog 撞号、
		 * 判决落错事务 ⇒ 2PC 跨分片原子性破坏、切主后总额漂移（N18 正例 N12 run 5 +2）。
		 *
		 * 修法（候选①的落点）：这条记录此刻已按 leader 编号落盘、马上要 fsync 后 ack，
		 * 与字节**同等持久**。就在这里把它尾块携带的发号水位接住 —— 未 apply 的尾巴由此
		 * 一并覆盖，ClaimOnPromote 无需改动即正确。RaiseAllocWatermark 只增不减、幂等，
		 * 只在真正抬高时才落盘（≈ 每个新分片 xid 一次，与 leader 发号同频），重传/去重路径
		 * 上重复调用无副作用。锁序：本处持 PartWALCtl->lock，内部另取 ShardXidCtl->lock，
		 * 无反向嵌套（发号路径取 ShardXidCtl 后即释放再写 parwal）。
		 */
		if ((flags & PARTWAL_FLAG_MARKER) != 0)
		{
			char   *mbody = VARDATA_ANY(data);
			Size	mlen = VARSIZE_ANY_EXHDR(data);

			if (mlen >= sizeof(TxnMarkerPayload))
			{
				TxnMarkerPayload *m = (TxnMarkerPayload *) mbody;

				if (mlen >= TxnMarkerPayloadSizeEx(m->nsubxacts, m->flags))
				{
					if ((m->flags & PARTWAL_MARKER_HAS_ALLOC_WM) != 0)
					{
						TransactionId wm = (TransactionId) *TxnMarkerAllocWmPtr(m);

						if (TransactionIdIsNormal(wm))
							ShardXidRaiseAllocWatermark(partition_id, wm);
					}
					else if ((m->flags & PARTWAL_MARKER_HAS_SHARD_XID) != 0)
					{
						TransactionId sx = (TransactionId) *TxnMarkerShardXidPtr(m);

						if (TransactionIdIsNormal(sx))
							ShardXidRaiseAllocWatermark(partition_id, sx + 1);
					}
				}
			}
		}

		/* 必须在 ack 之前落盘：多数派 ack == 多数派字节已持久化 */
		FlushPartitionWALWriter(writer, true);
		DestroyPartitionWALWriter(writer);
	}
	PG_FINALLY();
	{
		if (PartWALCtl != NULL)
			LWLockRelease(PartWALCtl->lock);
	}
	PG_END_TRY();

	/* 放掉 PartWALCtl->lock 之后再取 ReplayCtl->lock：不嵌套，零锁序风险 */
	if (foreign_heap_rec)
		ShardReplicaNoteForeignAppend(partition_id);

	PG_RETURN_INT64(partition_lsn);
}

/*
 * partwal_truncate_to — 供数据面 Raft 日志截断时同步截断 parwal。
 */
Datum
pg_partdist_partwal_truncate_to(PG_FUNCTION_ARGS)
{
	Oid		partition_id = PG_GETARG_OID(0);
	int64	keep_upto = PG_GETARG_INT64(1);
	bool	ok;

	/*
	 * ★ 与 PartWALFlush 的追加者互斥（2026-08-03 修）。
	 *
	 * 本函数由 group_propose 的失败路径调用（失多数派回滚该条字节），
	 * 此前完全无锁：截断段文件、改写 checkpoint 时，并发 backend 正持
	 * PartWALCtl->lock 往同一分区追加并写自己的 checkpoint —— 两边用同名
	 * checkpoint.tmp / fileset.tmp，实测出现 rename ENOENT 告警，更坏的
	 * 情形是截断点计算基于正在被追加的文件。调用点在复制挂钩里，彼时
	 * PartWALFlush 已释放锁，这里再取不会自锁。
	 */
	if (PartWALCtl != NULL)
		LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		ok = TruncatePartWALTo(partition_id,
							   (RelFileNumber) partition_id,
							   (uint64) keep_upto);
	}
	PG_FINALLY();
	{
		if (PartWALCtl != NULL)
			LWLockRelease(PartWALCtl->lock);
	}
	PG_END_TRY();

	PG_RETURN_BOOL(ok);
}

Datum
pg_partdist_follower_set_applied_part_lsn(PG_FUNCTION_ARGS)
{
	Oid				partition_id = PG_GETARG_OID(0);
	int64			applied_lsn = PG_GETARG_INT64(1);
	StringInfoData	sql;
	int				ret;

	if (SPI_connect() != SPI_OK_CONNECT)
		PG_RETURN_BOOL(false);

	/*
	 * local_relname / global_shard_id 取自 P0 的 shard_identity；该表还没这个
	 * 分片时退化成 OID 文本，保证进度列在任何情况下都能推进（它才是本函数的
	 * 职责，命名只是附带信息）。
	 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO partdist.follower_partition_map "
					 "(partition_id, local_relname, applied_part_lsn, global_shard_id) "
					 "SELECT %u, coalesce(si.local_relname, %u::text), %lld, si.global_shard_id "
					 "FROM (SELECT 1) d "
					 "LEFT JOIN partdist.shard_identity si ON si.local_oid = %u "
					 "ON CONFLICT (partition_id) DO UPDATE SET "
					 "applied_part_lsn = GREATEST(partdist.follower_partition_map.applied_part_lsn, "
					 "EXCLUDED.applied_part_lsn), "
					 "global_shard_id = coalesce(EXCLUDED.global_shard_id, "
					 "partdist.follower_partition_map.global_shard_id)",
					 partition_id, partition_id, (long long) applied_lsn,
					 partition_id);
	ret = SPI_execute(sql.data, false, 0);
	pfree(sql.data);

	SPI_finish();
	PG_RETURN_BOOL(ret == SPI_OK_INSERT);
}

/* ------------------------------------------------------------------ */
/* DTX-2PC — 分布式事务记录的写入与解析                                 */
/* ------------------------------------------------------------------ */

/*
 * partwal_append_dtx_record(partition_id, kind, dtxid, coord_gsid,
 *                           commit_ts, verdict, participants[])
 *
 * 在本节点该分区的 parwal 流里追加一条 DTX 记录并 fsync，返回分配到的
 * partition_lsn。设计依据 DTX_2PC_DESIGN.md §5。
 *
 * 记录随后由 prepare 接线的复制挂钩（pg_raft_partwal_replicate）按普通增量
 * 复制出去 —— DTX 记录与 DATA 记录共用同一个 partition_lsn 序号空间和同一条
 * 复制通道，flags 保证它在 follower 侧不被误当作 WAL 字节。
 */
uint64
AppendDtxRecord(Oid partition_id, int kind, int64 dtxid, int64 coord_gsid,
				int64 commit_ts, int32 verdict,
				const int64 *partvals, int nparts,
				TransactionId xid)
{
	PartitionWALWriter *writer;
	DtxRecordPayload   *payload;
	Size				paylen;
	uint64				assigned;

	/* 参与者清单只有 DECISION 记录携带（§5.2） */
	if (kind != DTX_DECISION)
		nparts = 0;

	paylen = DtxPayloadSize(nparts);
	payload = (DtxRecordPayload *) palloc0(paylen);
	payload->dtxid = (uint64) dtxid;
	payload->coord_gsid = coord_gsid;
	payload->commit_ts = (uint64) commit_ts;
	payload->verdict = (kind == DTX_DECISION) ? (uint32) verdict : 0;
	payload->nparticipants = (uint32) nparts;
	if (nparts > 0)
		memcpy(DtxPayloadParticipants(payload), partvals,
			   sizeof(int64) * nparts);

	/*
	 * ★ 必须持 PartWALCtl->lock（2026-08-08 修，实测复现）。
	 *
	 * partition_lsn 不是从共享内存分配的：CreatePartitionWALWriter 从**磁盘
	 * checkpoint 文件**播种 writer->last_partition_lsn，AppendPartWALRecord
	 * 在**进程私有内存**里 +1，落盘后再回写 checkpoint —— 一次彻头彻尾的
	 * read-modify-write。本函数此前是全树唯一不加锁的事务外写入者，于是：
	 *
	 *   backend A（本函数，PRE_PREPARE 写 DTX 记录）读到 checkpoint=100
	 *   backend B（PartWALFlush，持锁 drain）也读到 100，分配 101、写盘、回写
	 *   backend A 继续，也分配 101 —— 两条记录同号，且 expected==0 走自增分支，
	 *   AppendPartWALRecordAt 的重号校验根本碰不到，**静默**。
	 *
	 * 后果是静默的副本分歧：按编号回读只返回第一条，另一条永不进 Raft、
	 * 永不到 follower，而流里没有空洞、没有任何报错。实测在一张 2 分片表上
	 * 并发跑 120 笔跨分片 2PC + 120 笔单分片 INSERT，604 条记录只有 582 个
	 * 不同编号 —— 22 条重号，每条都是"一条 DTX + 一条 DATA"。
	 *
	 * 调用点（PRE_PREPARE 的 PartDistDtxPrePrepareFinish、COMMIT PREPARED 的
	 * 阶段 3、以及 SQL 入口）此时都不持有该锁，不会自锁。
	 */
	if (PartWALCtl != NULL)
		LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);
	PG_TRY();
	{
		InitPartitionWALDirectory(partition_id);
		writer = CreatePartitionWALWriter(partition_id, (RelFileNumber) partition_id);
		if (writer == NULL)
			ereport(ERROR,
					(errmsg("partwal_append_dtx_record: 无法为分区 %u 创建写入器",
							partition_id)));

		/*
		 * orig_lsn 恒为 0：DTX 记录不是 WAL 记录，没有 leader 侧 end LSN。
		 * 回放侧按 flags 在分派处就把它路由走，永不进 rm_redo，也永不用它盖页 LSN。
		 * info 存 DtxRecordKind（不是 XLog info）；rmid 存 RM_XACT_ID 仅为可读性。
		 *
		 * xid：PREPARE 标记要带本分区上的本地 top-level xid —— 升主回放时，
		 * "这笔 in-doubt 的本地事务属于哪个全局事务"就只剩这一条线索（DATA 记录
		 * 里只有 xid，没有 dtx 信息）。恢复守护补写的 COMMIT/ABORT 标记不在原事务
		 * 里，传 InvalidTransactionId。
		 */
		AppendPartWALRecord(writer,
							InvalidXLogRecPtr,
							(uint8) RM_XACT_ID,
							(uint8) kind,
							(const char *) payload,
							(uint32) paylen,
							TransactionIdIsValid(xid)
								? MakeGlobalXid(PartDistLocalNodeId(), xid)
								: InvalidGlobalXid,
							PARTWAL_FLAG_DTX);
		FlushPartitionWALWriter(writer, true);
		assigned = writer->last_partition_lsn;
		DestroyPartitionWALWriter(writer);
	}
	PG_FINALLY();
	{
		if (PartWALCtl != NULL)
			LWLockRelease(PartWALCtl->lock);
	}
	PG_END_TRY();
	pfree(payload);

	return assigned;
}

PG_FUNCTION_INFO_V1(pg_partdist_partwal_append_dtx_record);

Datum
pg_partdist_partwal_append_dtx_record(PG_FUNCTION_ARGS)
{
	Oid					partition_id = PG_GETARG_OID(0);
	int32				kind = PG_GETARG_INT32(1);
	int64				dtxid = PG_GETARG_INT64(2);
	int64				coord_gsid = PG_GETARG_INT64(3);
	int64				commit_ts = PG_GETARG_INT64(4);
	int32				verdict = PG_GETARG_INT32(5);
	ArrayType		   *parts = PG_ARGISNULL(6) ? NULL : PG_GETARG_ARRAYTYPE_P(6);
	int					nparts = 0;
	int64			   *partvals = NULL;
	uint64				assigned;

	if (!DtxRecordKindIsValid(kind))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("partwal_append_dtx_record: 非法的记录子类型 %d", kind),
				 errdetail("合法值：1=PREPARE 2=DECISION 3=COMMIT 4=ABORT 5=FORGET。")));

	if (kind == DTX_DECISION &&
		verdict != (int32) DTX_VERDICT_COMMIT && verdict != (int32) DTX_VERDICT_ABORT)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("partwal_append_dtx_record: DECISION 记录的 verdict 必须是 1(COMMIT) 或 2(ABORT)，实际 %d",
						verdict)));

	if (parts != NULL)
	{
		Datum  *elems;
		bool   *nulls;
		int		n;
		int		i;

		if (ARR_NDIM(parts) > 1)
			ereport(ERROR,
					(errmsg("partwal_append_dtx_record: participants 必须是一维数组")));
		deconstruct_array(parts, INT8OID, 8, true, 'd', &elems, &nulls, &n);
		partvals = (int64 *) palloc(sizeof(int64) * (n > 0 ? n : 1));
		for (i = 0; i < n; i++)
			if (!nulls[i])
				partvals[nparts++] = DatumGetInt64(elems[i]);
	}

	assigned = AppendDtxRecord(partition_id, kind, dtxid, coord_gsid,
							   commit_ts, verdict, partvals, nparts,
							   InvalidTransactionId);

	PG_RETURN_INT64((int64) assigned);
}

/*
 * partwal_read_dtx_record(partition_id, partition_lsn)
 *
 * 把一条 DTX 记录解析成可读字段，供运维与回归断言用。
 * 该 partition_lsn 上不是 DTX 记录时返回 NULL（调用方据此判别记录类型）。
 */
PG_FUNCTION_INFO_V1(pg_partdist_partwal_read_dtx_record);

Datum
pg_partdist_partwal_read_dtx_record(PG_FUNCTION_ARGS)
{
	Oid					partition_id = PG_GETARG_OID(0);
	int64				partition_lsn = PG_GETARG_INT64(1);
	PartWALRecord		rec;
	char			   *data = NULL;
	DtxRecordPayload   *payload;
	TupleDesc			tupdesc;
	Datum				values[6];
	bool				nulls[6];
	HeapTuple			tuple;
	ArrayType		   *parts_arr;
	Datum			   *part_datums;
	uint32				i;

	if (partition_lsn <= 0)
		PG_RETURN_NULL();
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errmsg("partwal_read_dtx_record: 返回类型必须是 record")));
	tupdesc = BlessTupleDesc(tupdesc);

	if (!partwal_find_record(partition_id, (uint64) partition_lsn, &rec, &data))
		PG_RETURN_NULL();

	/* 分类一律以 flags 判定，不以 data_len 判定（FRD §4.1 契约） */
	if ((rec.flags & PARTWAL_FLAG_DTX) == 0)
	{
		if (data != NULL)
			pfree(data);
		PG_RETURN_NULL();
	}

	if (data == NULL || rec.data_len < sizeof(DtxRecordPayload))
		ereport(ERROR,
				(errmsg("partwal_read_dtx_record: 分区 %u plsn %lld 的 DTX 载荷过短(%u 字节)",
						partition_id, (long long) partition_lsn, rec.data_len)));

	payload = (DtxRecordPayload *) data;
	if (rec.data_len != DtxPayloadSize(payload->nparticipants))
		ereport(ERROR,
				(errmsg("partwal_read_dtx_record: 分区 %u plsn %lld 的 DTX 载荷长度不符"
						"（头部 %u，按 nparticipants=%u 应为 %zu）",
						partition_id, (long long) partition_lsn, rec.data_len,
						payload->nparticipants,
						DtxPayloadSize(payload->nparticipants))));

	part_datums = (Datum *) palloc(sizeof(Datum) *
								   (payload->nparticipants > 0 ? payload->nparticipants : 1));
	for (i = 0; i < payload->nparticipants; i++)
		part_datums[i] = Int64GetDatum(DtxPayloadParticipants(payload)[i]);
	parts_arr = construct_array(part_datums, (int) payload->nparticipants,
								INT8OID, 8, true, 'd');

	memset(nulls, 0, sizeof(nulls));
	values[0] = Int32GetDatum((int32) rec.info);           /* kind        */
	values[1] = Int64GetDatum((int64) payload->dtxid);
	values[2] = Int64GetDatum(payload->coord_gsid);
	values[3] = Int64GetDatum((int64) payload->commit_ts);
	values[4] = Int32GetDatum((int32) payload->verdict);
	values[5] = PointerGetDatum(parts_arr);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	pfree(data);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
