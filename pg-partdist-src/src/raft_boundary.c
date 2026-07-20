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

#include "partition_wal.h"
#include "partition_wal_writer.h"

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
	Datum			values[5];
	bool			nulls[5];
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
	values[4] = PointerGetDatum(payload);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	if (data != NULL)
		pfree(data);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

Datum
pg_partdist_partwal_follower_append(PG_FUNCTION_ARGS)
{
	Oid					partition_id = PG_GETARG_OID(0);
	XLogRecPtr			orig_lsn = PG_GETARG_LSN(1);
	int32				rmid = PG_GETARG_INT32(2);
	int32				info = PG_GETARG_INT32(3);
	int64				xid = PG_GETARG_INT64(4);
	bytea			   *data = PG_GETARG_BYTEA_PP(5);
	PartitionWALWriter *writer;
	uint64				written_lsn;

	/*
	 * relfilenode 只用于 checkpoint 记账；follower 侧沿用 partition_id 本身，
	 * 与 demux 崩溃恢复对该目录的既有约定一致。
	 */
	writer = CreatePartitionWALWriter(partition_id, (RelFileNumber) partition_id);
	if (writer == NULL)
		ereport(ERROR,
				(errmsg("partwal_follower_append: 无法为分区 %u 创建写入器",
						partition_id)));

	AppendPartWALRecord(writer,
						orig_lsn,
						(uint8) rmid,
						(uint8) info,
						VARDATA_ANY(data),
						(uint32) VARSIZE_ANY_EXHDR(data),
						(TransactionId) xid);

	/* 必须在 ack 之前落盘：多数派 ack == 多数派字节已持久化 */
	FlushPartitionWALWriter(writer, true);
	written_lsn = writer->last_partition_lsn;
	DestroyPartitionWALWriter(writer);

	PG_RETURN_INT64((int64) written_lsn);
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
