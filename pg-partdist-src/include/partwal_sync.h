/*
 * partwal_sync.h
 *
 * parwal-2.0 synchronous write path — shared memory ring buffer.
 *
 * Architecture mirrors PostgreSQL's WAL buffer design:
 *
 *   XLogInsert()      writes WAL to XLogCtlData shared ring buffer
 *   XLogFlush(lsn)    writes from shared buffer to disk and fsyncs
 *
 *   PartWALInsert()   writes descriptors to PartWALCtl ring buffer
 *   PartWALFlush(lsn) writes from ring buffer to pg_parwal and fsyncs
 *   PartWALAbort()    discards this backend's pending slots without I/O
 *
 * wal_insert_hook fires inside XLogInsert() after the record is placed into
 * XLogCtl's buffer.  PartWALInsert() records a small descriptor into
 * PartWALCtl's ring buffer.  At XACT_EVENT_PRE_COMMIT the backend calls
 * PartWALFlush() — strictly before XLogFlush() writes the commit record —
 * maintaining the atomicity invariant: pg_parwal fsync < pg_wal commit fsync.
 *
 * Group commit: if another backend already flushed past our LSN,
 * PartWALFlush() returns with zero I/O.
 */
#ifndef PARTWAL_SYNC_H
#define PARTWAL_SYNC_H

#include "postgres.h"
#include "access/rmgr.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "common/relpath.h"
#include "storage/lwlock.h"
#include "partition_wal_writer.h"

/* ------------------------------------------------------------------ */
/* Tunables                                                             */
/* ------------------------------------------------------------------ */

/* Ring buffer capacity.  Each slot ~28 bytes; 8192 slots ~224 KB. */
#define PARTWAL_BUFFER_SLOTS    8192

/* Relfilenode -> partition_id shmem hash capacity */
#define PARTWAL_RELFHASH_SIZE   1024

/* ------------------------------------------------------------------ */
/* Shared memory structures (mirrors XLogCtlData layout)               */
/* ------------------------------------------------------------------ */

/*
 * One entry in the PartWAL ring buffer.
 * Written by PartWALInsert(), consumed by PartWALFlush(), or discarded by
 * PartWALAbort().
 */
typedef struct PartWALSlot
{
    Oid             partition_id;
    RelFileNumber   relfilenode;
    XLogRecPtr      orig_lsn;       /* end_lsn from wal_insert_hook */
    XLogRecPtr      start_lsn;      /* 记录起始 LSN(ProcLastRecPtr)。group-commit
                                     * 场景下本 backend 消费 peer 槽位时，其字节
                                     * 不在本 backend 的 pending 数组里，须按此
                                     * 起点从 pg_wal 回读 —— 否则流里出现
                                     * data_len=0 的 DATA 记录，物理回放断链 */
    TransactionId   xid;            /* originating transaction ID */
    uint8           rmid;
    uint8           info;
    bool            valid;          /* true = unconsumed */
    int             backend_id;     /* MyBackendId of inserting backend */
} PartWALSlot;

/*
 * PartWAL shared control structure -- mirrors XLogCtlData.
 * One lock covers both the ring buffer and the relfilenode hash.
 */
typedef struct PartWALCtlData
{
    LWLock        *lock;            /* protects all fields below */
    int            write_pos;       /* next slot to write (wraps at PARTWAL_BUFFER_SLOTS) */
    XLogRecPtr     flushed_upto;    /* highest orig_lsn written to pg_parwal */
    PartWALSlot    slots[PARTWAL_BUFFER_SLOTS];
} PartWALCtlData;

extern PGDLLIMPORT PartWALCtlData *PartWALCtl;

/* ------------------------------------------------------------------ */
/* Shared-memory lifecycle                                              */
/* ------------------------------------------------------------------ */

extern void  RequestPartWALSyncShmem(void);
extern Size  PartWALSyncShmemSize(void);
extern void  PartWALSyncShmemInit(void);

/* ------------------------------------------------------------------ */
/* Partition registration                                               */
/* ------------------------------------------------------------------ */

extern void  PartWALSyncRegister(Oid partition_id, RelFileNumber relfilenode);

/* 探测 relfilenode 是否已注册（backend 自动注册路径的低成本去重用） */
extern bool  PartWALSyncIsRegistered(RelFileNumber relfilenode);

/* ------------------------------------------------------------------ */
/* Per-backend WAL content capture (for full-body pg_parwal records)   */
/* ------------------------------------------------------------------ */

/*
 * PartWALPendingContent — one captured WAL record body per PartWALInsert
 * call that matched a tracked partition.  Stored in a per-backend dynamic
 * array (TopMemoryContext) and consumed by PartWALFlush().
 *
 * orig_lsn matches the slot's orig_lsn for lookup at flush time.
 * data is a palloc'd copy of the raw XLogRecord bytes (data_len bytes).
 */
typedef struct PartWALPendingContent
{
    XLogRecPtr      orig_lsn;
    TransactionId   xid;        /* originating transaction ID */
    char           *data;       /* palloc'd in TopMemoryContext */
    uint32          data_len;
} PartWALPendingContent;

/* ------------------------------------------------------------------ */
/* Core write-path API -- mirrors pg_wal naming                        */
/* ------------------------------------------------------------------ */

/*
 * PartWALInsert -- installed as wal_insert_hook (mirrors XLogInsert).
 *
 * Writes one ring-buffer slot per WAL record per partition.  Also captures
 * a copy of the raw WAL record bytes (record_data / record_len) into a
 * per-backend pending array so PartWALFlush() can embed the full body in
 * pg_parwal segment files, enabling self-contained replica replay.
 *
 * Breaks after the first matching block reference, preventing duplicate
 * slots for cross-page updates.  Skips XLOG_HEAP_CONFIRM records.
 */
extern void  PartWALInsert(XLogRecPtr end_lsn,
                            RmgrId rmid,
                            uint8 info,
                            const WALInsertBlockRef *blocks,
                            int nblocks,
                            const char *record_data,
                            uint32 record_len);

/*
 * PartWALFlush -- drain ring-buffer slots to pg_parwal and fsync.
 *                 Mirrors XLogFlush(lsn).
 *
 * Writes all valid slots with orig_lsn <= upto_lsn to pg_parwal segment
 * files, fsyncs, then marks them consumed.  Pass InvalidXLogRecPtr to flush
 * up to the per-backend max LSN tracked by PartWALInsert().
 *
 * Group commit: if PartWALCtl->flushed_upto >= upto_lsn, returns with no I/O.
 *
 * Called at XACT_EVENT_PRE_COMMIT and XACT_EVENT_PRE_PREPARE.
 */
extern void  PartWALFlush(XLogRecPtr upto_lsn);

/*
 * PartWALAbort -- invalidate this backend's pending ring-buffer slots.
 * No disk I/O.  Called at XACT_EVENT_ABORT.
 */
extern void  PartWALAbort(void);

/* ------------------------------------------------------------------ */
/* WAL range scan (used by DemuxCrashRecovery)                         */
/* ------------------------------------------------------------------ */

extern XLogRecPtr ScanWALRangeForPartition(PartitionWALWriter *writer,
                                            XLogRecPtr start_lsn,
                                            XLogRecPtr end_lsn);

#endif /* PARTWAL_SYNC_H */
