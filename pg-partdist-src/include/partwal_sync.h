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
#define PARTWAL_LOST_MAX 16

typedef struct PartWALCtlData
{
    LWLock        *lock;            /* protects all fields below */
    int            write_pos;       /* next slot to write (wraps at PARTWAL_BUFFER_SLOTS) */
    XLogRecPtr     flushed_upto;    /* highest orig_lsn written to pg_parwal */
    TimestampTz    freeze_last_check;  /* §13 约束 5：上次冻结账目检查的时刻。
                                         * **必须是节点级共享的**，不能是 backend
                                         * 本地 static —— 见 PartWALFreezeCheckDue */

    /*
     * T7.27（P7-W2）：捕获环的背压与溢出记账。
     *
     * nvalid          未消费槽位数。写路径钩子据此判断要不要就地排空（背压）。
     * ring_overwrites 累计覆盖次数；覆盖 = 有记录没进分区流 = 副本少一条。
     * lost_parts      被覆盖记录所属的分区（去重），由下一次 PartWALFlush 取走并
     *                 打分叉标记；装不下即 lost_overflow（那时对全部分区打标）。
     * diverged_pending 有新的分叉标记待自动修复（心跳工作者看它决定要不要自连
     *                 调 repair_diverged_shards）。启动时置 true，覆盖重启前留下的标记。
     */
    int            nvalid;
    uint64         ring_overwrites;
    uint64         backpressure_flushes;
    int            nlost;
    bool           lost_overflow;
    Oid            lost_parts[PARTWAL_LOST_MAX];
    bool           diverged_pending;
    TimestampTz    repair_last_try;

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

/* 本 backend 当前事务是否还有未落盘的分区记录（冻结账目发射用它回避交织） */
extern bool  PartWALHasPendingRecords(void);

/*
 * PartWALFreezeCheckDue — 冻结账目检查的**节点级**限流（§13 约束 5）。
 *
 * 距上次检查已超过 interval_ms 则返回 true 并就地把水位推到现在（compare-and-set
 * 在 PartWALCtl->lock 下完成，所以并发的多个 backend 里只有一个能过）。
 *
 * ★ 为什么这个状态必须在共享内存里、不能是 backend 本地 static：
 * 本地 static 的初值是 0 = "从没查过"，于是**每个新连接的第一次提交都会无视
 * 间隔立刻发射**。R1 用例开几十条短命 psql 连接，实测就变成几十次发射，而且
 * 恰好和 VACUUM 灌 Raft 日志环的窗口重叠 —— 128 槽的环被顶爆
 * （"log ring full, cannot append"），VACUUM 事务中止，而它的物理截断在
 * leader 上已经做掉了，follower 却永远收不到那条 SMGR 截断记录：**永久分叉**。
 * 表现是 R1 的 TOAST 文件大小两侧对不上。
 */
extern bool  PartWALFreezeCheckDue(int interval_ms);

/*
 * T7.27（P7-W2）捕获环背压：由补丁 0005 的写路径钩子（heap 插入/更新/删除开头，
 * 不在临界区、不持 buffer 锁）调用。环占用过高水位、且本后端有待排空记录时，
 * 就地 PartWALFlush(Invalid, false) —— 大事务不再能把环写爆。
 */
extern int   partwal_ring_high_water;
extern void  PartWALBackpressure(void);

/* 有新的分叉标记待修复（ShardMarkDiverged 调）；到期判定（心跳调，节点级限流） */
extern void  PartWALNoteDivergedPending(void);
extern bool  PartWALDivergedRepairDue(int interval_ms);

/*
 * 给一个分区追加一条 CTRL 控制记录并就地复制（FRD §7.7/§12）。
 * opcode 落在头部的 info 字段（PARTWAL_CTRL_*）。
 *
 * 返回本条 CTRL 被分配到的 partition_lsn。T6.1 的全量基线要拿它当
 * `base_part_lsn` 交给 follower —— "从这一条开始重放，之前的一律不看"。
 * 不需要这个值的调用方忽略返回值即可。
 */
extern uint64 PartWALAppendCtrl(Oid partition_id, uint8 opcode,
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
/* 触达分区集合（DTX_2PC_DESIGN.md §9.1）                               */
/* ------------------------------------------------------------------ */

/*
 * 本事务写过哪些分区。集合在 PartWALInsert() 时按 backend 登记，
 * 由 PartWALEndTxn()（COMMIT/PREPARE 事件）与 PartWALAbort() 清空 ——
 * 复制挂钩是幂等区间式的，PartWALFlush() 触发复制**不清**集合，
 * 因此同一事务内 flush 之后 PartWALCopyTouched 仍取得到快照。
 *
 * PartWALCopyTouched: 拷贝一份当前集合，返回元素个数；out 由调用方
 *   palloc/pfree（传 NULL 只问个数）。
 * PartWALNoteTouchedPartition: 手工登记一个分区，供"flush 之后又追加了
 *   记录（如 DTX_PREPARE 标记）、需要再复制一轮"的路径使用。
 */
extern int   PartWALCopyTouched(Oid *out, int max);
extern int   PartWALTouchedCount(void);
extern void  PartWALNoteTouchedPartition(Oid partition_id);

/* ------------------------------------------------------------------ */
/* 2PC 事务标记（DTX_2PC_DESIGN.md §3.3 阶段 3）                        */
/* ------------------------------------------------------------------ */

/*
 * 组装一条 MARKER 载荷（TxnMarkerPayload）。
 *   with_children  = 是否收集本事务的已提交子事务清单
 *   with_commit_ts = 是否带提交时间戳（PREPARE 标记不带：还没提交）
 * 返回 palloc 的缓冲区，调用方负责 pfree。
 */
extern char *PartWALBuildMarkerPayload(bool with_children, bool with_commit_ts,
                                       uint32 *out_len);

/*
 * 给指定分区独立追加一条 MARKER 并 fsync，标记的事务由 xid 显式给出。
 * op = XLOG_XACT_PREPARE / XLOG_XACT_COMMIT / XLOG_XACT_ABORT。
 * 内部自取 PartWALCtl->lock；调用方不得已持有。
 */
extern void  PartWALAppendMarkerFor(Oid partition_id, TransactionId xid,
                                    uint8 op,
                                    const char *payload, uint32 payload_len);

/*
 * ★ T7.1（R-P6-15）：判决标记的显式版本。
 *
 * 上面两个函数都从**当前事务**取值：载荷的 flags 由 `ShardXidXactCount() > 0`
 * 决定、分片 xid 由 `ShardXidMineForShard()` 取、时间戳由 TSO 客户端取。
 * 这套取法对"写分片的那笔事务自己发标记"成立，对**判决落账时补发标记**不成立
 * ——那时跑在另一个事务里（COMMIT PREPARED 的语句、清扫工作者、恢复守护），
 * 三个来源全是空的，组装出来的就是不带分片 xid 的 24 字节旧格式，
 * 回放侧 `ShardClogSetVerdict` 因此被跳过（R-P6-15：切主后 2PC 提交的行永久不可见）。
 *
 * 于是这一对函数把三个值全部**显式传入**：
 *   PartWALBuildVerdictMarker   —— start_ts / commit_ts 由调用方给（决议的权威值）
 *   PartWALAppendMarkerForShardXid —— 分片 xid 由调用方给（未决登记里的 pairs）
 *
 * 载荷恒带 HAS_SHARD_XID | HAS_ALLOC_WM 两个尾巴；alloc_wm 仍在追加时按分区
 * 现取（`ShardXidNextToIssue`，与事务无关，任何语境下都成立）。
 */
extern char *PartWALBuildVerdictMarker(int64 start_ts, int64 commit_ts,
                                       uint32 *out_len);
extern void  PartWALAppendMarkerForShardXid(Oid partition_id, TransactionId xid,
                                            uint8 op,
                                            const char *payload,
                                            uint32 payload_len,
                                            TransactionId shard_xid);

/* ------------------------------------------------------------------ */
/* WAL range scan (used by DemuxCrashRecovery)                         */
/* ------------------------------------------------------------------ */

extern XLogRecPtr ScanWALRangeForPartition(PartitionWALWriter *writer,
                                            XLogRecPtr start_lsn,
                                            XLogRecPtr end_lsn);

#endif /* PARTWAL_SYNC_H */
