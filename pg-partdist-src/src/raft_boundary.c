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

#include "executor/spi.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/pg_lsn.h"

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
