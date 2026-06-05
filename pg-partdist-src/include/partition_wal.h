/*
 * partition_wal.h
 * Public API for pg_parwal directory management, record writing, and
 * the custom WAL resource manager.
 */
#ifndef PARTITION_WAL_H
#define PARTITION_WAL_H

#include "pg_partdist.h"
#include "partition_wal_header.h"

#include "access/xlogdefs.h"
#include "executor/executor.h"
#include "fmgr.h"

/* ------------------------------------------------------------------ */
/* Constants                                                            */
/* ------------------------------------------------------------------ */

#define PARTITION_WAL_DIR   "pg_parwal"     /* directory name under DataDir */
#define PARTITION_WAL_MODE  0700            /* permissions for new dirs     */

/* Custom RMGR info bytes (low 4 bits of xl_info) */
#define PARTWAL_RMGR_INFO_DATA   0x00       /* normal data / checkpoint record */
#define PARTWAL_RMGR_INFO_SKIP   0x10       /* skip / gap record               */

/* ------------------------------------------------------------------ */
/* Directory management (D2.1.1)                                        */
/* ------------------------------------------------------------------ */

/*
 * InitPartitionWALDirectory — create pg_parwal/ and pg_parwal/<partition_id>/
 * under DataDir.  Safe to call multiple times (idempotent).
 */
extern void  InitPartitionWALDirectory(Oid partition_id);

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
/* Per-partition LSN allocation                                         */
/* ------------------------------------------------------------------ */

/*
 * AllocPartitionLSN — atomically return-and-increment the per-partition
 * sequence counter for partition_id.  First allocation returns 1.
 * Requires shared memory to be initialised.
 */
extern uint64 AllocPartitionLSN(Oid partition_id);

/* ------------------------------------------------------------------ */
/* WAL record writing (D2.1.3)                                          */
/* ------------------------------------------------------------------ */

/*
 * WritePartitionWALRecord — write a PartWALHeader record to the global WAL
 * (via the custom RMGR) and mirror it into the appropriate pg_parwal/N/
 * segment file.
 *
 * orig_lsn: the global WAL LSN of the DML that triggered this record
 *           (typically XactLastRecEnd captured before this call).
 *
 * Returns the global LSN assigned to the new custom RMGR record, or
 * InvalidXLogRecPtr if WAL insertion is not currently allowed.
 */
extern XLogRecPtr WritePartitionWALRecord(Oid partition_id, uint8 flags,
                                          XLogRecPtr orig_lsn);

/* ------------------------------------------------------------------ */
/* Custom RMGR registration                                             */
/* ------------------------------------------------------------------ */

/* Call once from _PG_init (inside shared_preload_libraries). */
extern void RegisterPartitionWALRmgr(void);

/*
 * ResetPartitionWALState — for regression tests only.
 * Deletes all pg_parwal/<partition_id>/ files and resets the shmem LSN
 * counter to 0 so the next AllocPartitionLSN returns 1 again.
 */
extern void ResetPartitionWALState(Oid partition_id);

/* ------------------------------------------------------------------ */
/* Shared-memory lifecycle                                              */
/* ------------------------------------------------------------------ */

#define PARTWAL_LOCK_TRANCHE    "pg_partdist_wal"

/* Call from shmem_request_hook to reserve space and LWLock tranche. */
extern void RequestPartitionWALShmem(void);

extern Size PartitionWALShmemSize(void);
extern void PartitionWALShmemInit(void);

/* ------------------------------------------------------------------ */
/* Citus shard detection                                               */
/* ------------------------------------------------------------------ */

/*
 * IsCitusShardTable — true if relid's syscache name ends with _<N>
 * where N is at least 4 decimal digits (Citus shard table naming convention).
 * Safe to call from executor hooks; uses get_rel_name (syscache lookup).
 */
extern bool IsCitusShardTable(Oid relid);

/*
 * ShouldWritePartWAL — true if a PartWAL record should be written for
 * the given (relid, rel_alias) pair.  Checks partition_map, Citus shard
 * naming, and pg_dist_partition membership.
 */
extern bool ShouldWritePartWAL(Oid relid, const char *rel_alias);

/* ------------------------------------------------------------------ */
/* ExecutorFinish hook handler                                          */
/* ------------------------------------------------------------------ */

extern void pg_partdist_executor_finish(QueryDesc *queryDesc);

/* ------------------------------------------------------------------ */
/* ProcessUtility hook handler (bulk INSERT / COPY interception)       */
/* ------------------------------------------------------------------ */

#include "tcop/utility.h"

/*
 * pg_partdist_process_utility — called after every utility statement
 * completes successfully.  Detects COPY FROM targeting a Citus shard
 * table and writes the corresponding PartWALHeader record so that the
 * Demux worker can track bulk-inserted data.
 *
 * prev_hook: the previous hook in the chain (already invoked by the
 *            caller; passed only for the chain-call check below).
 *
 * This function must be called AFTER the prev hook has returned without
 * error so that failed/rolled-back COPY operations do not produce stale
 * PartWAL records.
 */
extern void pg_partdist_process_utility(PlannedStmt *pstmt,
                                        const char *queryString,
                                        bool readOnlyTree,
                                        ProcessUtilityContext context,
                                        ParamListInfo params,
                                        QueryEnvironment *queryEnv,
                                        DestReceiver *dest,
                                        QueryCompletion *qc);

/* ------------------------------------------------------------------ */
/* SQL-callable function declarations                                   */
/* ------------------------------------------------------------------ */

extern Datum pg_partdist_init_partition_wal(PG_FUNCTION_ARGS);
extern Datum pg_partdist_partition_wal_exists(PG_FUNCTION_ARGS);
extern Datum pg_partdist_partition_wal_path(PG_FUNCTION_ARGS);
extern Datum pg_partdist_cleanup_partition_wal(PG_FUNCTION_ARGS);
extern Datum pg_partdist_alloc_partition_lsn(PG_FUNCTION_ARGS);
extern Datum pg_partdist_write_partition_wal_record(PG_FUNCTION_ARGS);
extern Datum pg_partdist_check_partition_wal(PG_FUNCTION_ARGS);
extern Datum pg_partdist_verify_partition_wal(PG_FUNCTION_ARGS);

#endif /* PARTITION_WAL_H */
