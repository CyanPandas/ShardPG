/*
 * demux_worker.h
 * Public API for the pg_partdist Demux background worker (Milestone 2.2).
 *
 * The Demux Worker runs as a PostgreSQL background worker under the postmaster.
 * It reads the local pg_wal stream, identifies records emitted by the custom
 * RM_EXPERIMENTAL_ID RMGR (which carry PartWALHeader payloads), and routes
 * each record into the appropriate pg_parwal/<partition_id>/ segment file via a
 * buffered PartitionWALWriter.  It deduplicates against records already written
 * by the synchronous M2.1 path so that no record appears twice.
 */
#ifndef DEMUX_WORKER_H
#define DEMUX_WORKER_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "fmgr.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"

/* ------------------------------------------------------------------ */
/* Compile-time limits                                                  */
/* ------------------------------------------------------------------ */

#define DEMUX_MAX_PARTITIONS    512     /* max concurrent PartitionWALWriter slots  */
#define DEMUX_SLEEP_MS          100     /* sleep interval when WAL is exhausted (ms) */
#define DEMUX_PROGRESS_MAGIC    UINT32_C(0x44455855)  /* "DEMU" */
#define DEMUX_PROGRESS_FILE     "pg_parwal/.demux_progress"
#define DEMUX_SHMEM_NAME        "pg_partdist_demux_state"
#define DEMUX_LOCK_TRANCHE      "pg_partdist_demux"

/* Rolling latency window stored in shared memory */
#define DEMUX_LATENCY_SAMPLES   1024

/* ------------------------------------------------------------------ */
/* Shared memory state (visible to SQL functions)                       */
/* ------------------------------------------------------------------ */

typedef struct DemuxSharedState
{
    LWLock     *lock;
    XLogRecPtr  last_processed_lsn;     /* last WAL LSN fully processed */
    bool        worker_active;          /* true while the BGW is running */

    /* Rolling latency samples in microseconds (circular buffer) */
    int64       latency_buf[DEMUX_LATENCY_SAMPLES];
    int         latency_head;           /* next write slot */
    int64       latency_count;          /* total samples ever recorded */
} DemuxSharedState;

extern DemuxSharedState *DemuxState;

/* ------------------------------------------------------------------ */
/* Life-cycle (called from _PG_init / shmem hooks)                     */
/* ------------------------------------------------------------------ */

extern void   RegisterDemuxWorker(void);
extern void   RequestDemuxShmem(void);
extern void   DemuxShmemInit(void);
extern Size   DemuxShmemSize(void);

/* ------------------------------------------------------------------ */
/* Background worker entry point (symbol looked up by postmaster)       */
/* Must be exported even when the .so is built with -fvisibility=hidden */
/* ------------------------------------------------------------------ */

extern PGDLLEXPORT void DemuxWorkerMain(Datum arg);

/* ------------------------------------------------------------------ */
/* SQL-callable function declarations (registered in pg_partdist--1.0) */
/* ------------------------------------------------------------------ */

extern Datum pg_partdist_demux_progress(PG_FUNCTION_ARGS);
extern Datum pg_partdist_demux_latency_stats(PG_FUNCTION_ARGS);
extern Datum pg_partdist_count_parwal_records(PG_FUNCTION_ARGS);
extern Datum pg_partdist_read_all_headers(PG_FUNCTION_ARGS);
extern Datum pg_partdist_demux_flush(PG_FUNCTION_ARGS);

#endif /* DEMUX_WORKER_H */
