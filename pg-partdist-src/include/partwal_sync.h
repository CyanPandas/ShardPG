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

/*
 * 枚举本节点当前捕获的全部分区 OID（去重）。DDL 之后的 fileset diff 用；
 * 返回写进 out 的个数。
 */
extern int   PartWALSyncListPartitions(Oid *out, int max);

/*
 * 给一个分区追加一条 CTRL 控制记录并就地复制（FRD §7.7/§12）。
 * opcode 落在头部的 info 字段（PARTWAL_CTRL_*）。
 */
extern void  PartWALAppendCtrl(Oid partition_id, uint8 opcode,
                               const char *payload, uint32 payload_len);

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
 * Group commit: if PartWALCtl->flushed_upto >= upto_lsn, another backend
 * already wrote our bytes — the drain is skipped, but the commit marker and
 * the replication hook still run (they are per-transaction, not per-slot).
 *
 * write_marker: 是否给本事务追加 COMMIT 标记（PARTWAL_FLAG_MARKER）。
 *   PRE_COMMIT  → true：此刻提交已成定局，标记即事务在 follower 上的可见性依据。
 *   PRE_PREPARE → false：2PC 的 prepared 事务还可能被 ROLLBACK PREPARED，
 *                 此时写 COMMITTED 会让最终回滚的数据在 follower 上变可见。
 *                 不写标记的后果只是该事务在 follower 上保持"未决 = 不可见"，
 *                 语义安全。补齐 PREPARE/COMMIT PREPARED 两段式标记是后续工作。
 *
 * Called at XACT_EVENT_PRE_COMMIT and XACT_EVENT_PRE_PREPARE.
 */
extern void  PartWALFlush(XLogRecPtr upto_lsn, bool write_marker);

/*
 * PartWALAbort -- invalidate this backend's pending ring-buffer slots, and —
 * 若本事务的字节已经落进段文件（group commit，或 PRE_COMMIT 之后才失败）——
 * 补一条 ABORT 标记，好让 follower 侧这批 xid 有终态。
 * Called at XACT_EVENT_ABORT.
 */
extern void  PartWALAbort(void);

/*
 * PartWALEndTxn -- 清掉本事务的 backend 本地记账（已落盘 LSN + 涉及分区）。
 * 成功路径专用，在 XACT_EVENT_COMMIT / XACT_EVENT_PREPARE 调用；
 * 中止路径由 PartWALAbort 自己收尾（它要先补标记再清）。
 */
extern void  PartWALEndTxn(void);

/* ------------------------------------------------------------------ */
/* WAL range scan (used by DemuxCrashRecovery)                         */
/* ------------------------------------------------------------------ */

extern XLogRecPtr ScanWALRangeForPartition(PartitionWALWriter *writer,
                                            XLogRecPtr start_lsn,
                                            XLogRecPtr end_lsn);

#endif /* PARTWAL_SYNC_H */
