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
 *       Placeholder until the real role-switch / replay handover lands:
 *       follower replay is design-only in shardpg-3.0
 *       (docs/FOLLOWER_REPLAY_DESIGN.md), so this only logs the event.
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
#include "utils/pg_lsn.h"

#include <fcntl.h>
#include <unistd.h>

#include "access/rmgr.h"
#include "catalog/pg_type.h"
#include "utils/array.h"

#include "partition_wal.h"
#include "partition_wal_header.h"
#include "partition_wal_writer.h"
#include "partwal_sync.h"		/* PartWALCtl：truncate 与追加者互斥 */
#include "dtx_record.h"			/* DTX-2PC 记录载荷（DTX_2PC_DESIGN.md §5） */

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

Datum
pg_partdist_partwal_notify_primary_switch(PG_FUNCTION_ARGS)
{
	Oid			partition_id = PG_GETARG_OID(0);
	int32		old_primary_node = PG_GETARG_INT32(1);
	int32		new_primary_node = PG_GETARG_INT32(2);
	XLogRecPtr	switch_orig_lsn = PG_GETARG_LSN(3);

	ereport(LOG,
			(errmsg("pg_partdist: primary switch notified for partition %u: %d -> %d at %X/%X",
					partition_id,
					old_primary_node,
					new_primary_node,
					LSN_FORMAT_ARGS(switch_orig_lsn))));

	PG_RETURN_VOID();
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
 *   partwal_follower_append(partition_id, orig_lsn, rmid, info, xid, data)
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
PG_FUNCTION_INFO_V1(pg_partdist_follower_set_applied_part_lsn);
PG_FUNCTION_INFO_V1(pg_partdist_partwal_truncate_to);

/*
 * 在 pg_parwal/<partition_id>/ 的段文件里定位 partition_lsn == target 的记录。
 * 命中则填充 *out_rec 并把原始字节 palloc 到 *out_data，返回 true。
 */
static bool
partwal_find_record(Oid partition_id, uint64 target,
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

	memset(nulls, 0, sizeof(nulls));
	values[0] = LSNGetDatum(rec.orig_lsn);
	values[1] = Int32GetDatum((int32) rec.rmid);
	values[2] = Int32GetDatum((int32) rec.info);
	values[3] = Int64GetDatum((int64) rec.xid);
	/*
	 * ★ flags 必须随记录一起返回（DTX_2PC_DESIGN.md §5.5）：复制通道丢了它，
	 * DTX/标记记录到了 follower 就退化成 DATA 记录，升主回放时会被当作
	 * WAL 字节喂给 rm_redo —— PANIC 或静默损坏。
	 */
	values[4] = Int32GetDatum((int32) rec.flags);
	values[5] = PointerGetDatum(payload);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	if (data != NULL)
		pfree(data);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
pg_partdist_partwal_follower_append(PG_FUNCTION_ARGS)
{
	Oid					partition_id = PG_GETARG_OID(0);
	int64				partition_lsn = PG_GETARG_INT64(1);
	XLogRecPtr			orig_lsn = PG_GETARG_LSN(2);
	int32				rmid = PG_GETARG_INT32(3);
	int32				info = PG_GETARG_INT32(4);
	int64				xid = PG_GETARG_INT64(5);
	bytea			   *data = PG_GETARG_BYTEA_PP(6);
	int32				rec_flags = PG_GETARG_INT32(7);
	PartitionWALWriter *writer;

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
	(void) AppendPartWALRecordAt(writer,
								 (uint64) partition_lsn,
								 orig_lsn,
								 (uint8) rmid,
								 (uint8) info,
								 (uint8) rec_flags,
								 VARDATA_ANY(data),
								 (uint32) VARSIZE_ANY_EXHDR(data),
								 (TransactionId) xid);

	/* 必须在 ack 之前落盘：多数派 ack == 多数派字节已持久化 */
	FlushPartitionWALWriter(writer, true);
	DestroyPartitionWALWriter(writer);

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
						PARTWAL_FLAG_DTX,
						(const char *) payload,
						(uint32) paylen,
						xid);
	FlushPartitionWALWriter(writer, true);
	assigned = writer->last_partition_lsn;
	DestroyPartitionWALWriter(writer);
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
