/*
 * partition_wal.h
 * Public API for pg_parwal directory management and the hook handlers
 * (parwal-2.0: custom RMGR removed, shmem queue introduced).
 */
#ifndef PARTITION_WAL_H
#define PARTITION_WAL_H

#include "pg_partdist.h"
#include "partition_wal_header.h"
#include "partwal_sync.h"

#include "access/xlogdefs.h"
#include "executor/executor.h"
#include "fmgr.h"

/* ------------------------------------------------------------------ */
/* Constants                                                            */
/* ------------------------------------------------------------------ */

#define PARTITION_WAL_DIR   "pg_parwal"     /* directory name under DataDir */
#define PARTITION_WAL_MODE  0700            /* permissions for new dirs     */

/* ------------------------------------------------------------------ */
/* Directory management                                                 */
/* ------------------------------------------------------------------ */

/*
 * InitPartitionWALDirectory — create pg_parwal/ and pg_parwal/<partition_id>/
 * under DataDir.  Safe to call multiple times (idempotent).
 */
extern void  InitPartitionWALDirectory(Oid partition_id);

/*
 * InitPartitionWALAndRegister — create directory and register the partition's
 * relfilenode in the shmem sync hash so the WAL insert hook can identify it.
 * Called from object_access hook on shard table creation.
 */
extern void  InitPartitionWALAndRegister(Oid partition_id);

/*
 * GetPartitionWALPath — return a palloc'd relative path string of the form
 * "pg_parwal/<partition_id>/<segname>" for the given segment number.
 * The caller owns the returned string.
 */
extern char *GetPartitionWALPath(Oid partition_id, XLogSegNo segno);

/*
 * CleanupPartitionWAL — remove segment files in pg_parwal/<partition_id>/
 * whose end-LSN is at or below keepPtr.
 */
extern void  CleanupPartitionWAL(Oid partition_id, XLogRecPtr keepPtr);

/* ------------------------------------------------------------------ */
/* Citus shard detection                                               */
/* ------------------------------------------------------------------ */

extern bool IsCitusShardName(const char *name);
extern bool IsCitusShardTable(Oid relid);
extern bool ShouldWritePartWAL(Oid relid, const char *rel_alias);
extern void EnsurePartWALRegistered(Oid relid);

/* ------------------------------------------------------------------ */
/* Hook handlers                                                        */
/* ------------------------------------------------------------------ */

#include "tcop/utility.h"

extern void pg_partdist_process_utility(PlannedStmt *pstmt,
                                        const char *queryString,
                                        bool readOnlyTree,
                                        ProcessUtilityContext context,
                                        ParamListInfo params,
                                        QueryEnvironment *queryEnv,
                                        DestReceiver *dest,
                                        QueryCompletion *qc);

/* ------------------------------------------------------------------ */
/* Test helpers                                                         */
/* ------------------------------------------------------------------ */

/*
 * ResetPartitionWALState — for regression tests only.
 * Deletes all pg_parwal/<partition_id>/ files and resets the checkpoint
 * file so the next demux run starts fresh with partition_lsn = 1.
 */
extern void ResetPartitionWALState(Oid partition_id);

/* ------------------------------------------------------------------ */
/* Shared-memory lifecycle                                              */
/* ------------------------------------------------------------------ */

#define PARTWAL_LOCK_TRANCHE    "pg_partdist_wal"

/* Called from shmem_request_hook */
extern void RequestPartitionWALShmem(void);
extern Size PartitionWALShmemSize(void);
extern void PartitionWALShmemInit(void);

/* ------------------------------------------------------------------ */
/* SQL-callable function declarations                                   */
/* ------------------------------------------------------------------ */

extern Datum pg_partdist_init_partition_wal(PG_FUNCTION_ARGS);
extern Datum pg_partdist_partition_wal_exists(PG_FUNCTION_ARGS);
extern Datum pg_partdist_partition_wal_path(PG_FUNCTION_ARGS);
extern Datum pg_partdist_cleanup_partition_wal(PG_FUNCTION_ARGS);
extern Datum pg_partdist_check_partition_wal(PG_FUNCTION_ARGS);
extern Datum pg_partdist_verify_partition_wal(PG_FUNCTION_ARGS);

/* write_partition_wal_record: test helper that enqueues a queue entry */
extern Datum pg_partdist_write_partition_wal_record(PG_FUNCTION_ARGS);

/* alloc_partition_lsn: kept for SQL compatibility; now reads checkpoint */
extern Datum pg_partdist_alloc_partition_lsn(PG_FUNCTION_ARGS);

#endif /* PARTITION_WAL_H */
