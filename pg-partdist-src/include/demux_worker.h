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
#include "port/atomics.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/wait_event.h"

/* ------------------------------------------------------------------ */
/* Compile-time limits                                                  */
/* ------------------------------------------------------------------ */

#define DEMUX_MAX_PARTITIONS    512     /* max concurrent PartitionWALWriter slots  */
#define DEMUX_SLEEP_MS          50      /* sleep interval when WAL is exhausted (ms) */
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

    /*
     * last_processed_lsn — set ONLY by the Demux worker, AFTER all parwal
     * buffers for the current batch have been flushed to disk.  Used by
     * demux_flush() as a reliable "data-on-disk" barrier.
     */
    XLogRecPtr  last_processed_lsn;

    /*
     * last_committed_lsn — updated eagerly by the commit callback and by
     * WritePartitionWALRecord immediately after XLogFlush.  Used by
     * demux_progress() and the latency-measurement query so that
     * pg_current_wal_flush_lsn() == last_committed_lsn is visible within
     * the same transaction that committed the insert.
     */
    XLogRecPtr  last_committed_lsn;

    bool        worker_active;          /* true while the BGW is running */
    bool        recovery_complete;      /* set to true once crash recovery BGW has finished */
    Latch      *demux_latch;            /* set at startup; backends call SetLatch to wake demux */

    /*
     * drop_notice_gen — T7.8（P7-D1）：**已提交**的"分片表被 DROP"代次。
     *
     * 为什么必须放 shmem 而不是后端局部变量：删表的那条连接往往当场就断了，
     * 后端局部标志会随它一起消失，通知就永远发不出去。代次放共享内存，
     * 任何后端在下一条语句开头看到代次变了都会补扫一遍 —— 扫描本身是
     * 幂等的（判据是持久证据 `pg_parwal/<oid>/fileset` 存在而表已不在），
     * 谁先扫到都一样。
     */
    pg_atomic_uint32 drop_notice_gen;

    /*
     * drop_notice_swept —— 已经被某个后端扫掉的代次。两者用 CAS 配对，
     * 保证**一代只扫一次**：否则每个后端各有一份"已消费代次"，谁先跑到语句
     * 边界谁就扫，实测同一条通知在 1 ms 内被 4 个后端各发了一遍（幂等所以
     * 无害，但白跑 4 次 raft 提案）。
     */
    pg_atomic_uint32 drop_notice_swept;

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

extern Datum pg_partdist_demux_is_ready(PG_FUNCTION_ARGS);
extern Datum pg_partdist_demux_progress(PG_FUNCTION_ARGS);
extern Datum pg_partdist_demux_latency_stats(PG_FUNCTION_ARGS);
extern Datum pg_partdist_count_parwal_records(PG_FUNCTION_ARGS);
extern Datum pg_partdist_read_all_headers(PG_FUNCTION_ARGS);
extern Datum pg_partdist_demux_flush(PG_FUNCTION_ARGS);

#endif /* DEMUX_WORKER_H */
