/*
 * partwal_sync.c
 *
 * parwal-2.0 synchronous PartWAL write path — shared memory ring buffer.
 *
 * Design mirrors PostgreSQL's WAL buffer (XLogCtlData):
 *
 *   XLogInsert()   -> PartWALInsert(): write descriptor to PartWALCtl.slots[]
 *   XLogFlush(lsn) -> PartWALFlush(lsn): drain slots to pg_parwal, fsync
 *
 * wal_insert_hook fires inside XLogInsert() after the WAL record is placed
 * into XLogCtl's shared buffer.  PartWALInsert() writes a small descriptor
 * (partition_id, relfilenode, end_lsn, rmid, info) to the PartWALCtl ring
 * buffer.  At XACT_EVENT_PRE_COMMIT/PRE_PREPARE the backend calls
 * PartWALFlush() which drains the ring buffer to pg_parwal files and fsyncs
 * -- strictly before XLogFlush() writes the pg_wal commit record, maintaining
 * the [A] pg_parwal fsync < [B] pg_wal commit fsync atomicity invariant.
 *
 * Group commit: PartWALFlush() also writes any other backends' valid slots
 * with orig_lsn <= upto_lsn.  If flushed_upto >= upto_lsn another backend
 * already covered our records -- return immediately with zero I/O.
 *
 * Duplicate-block fix: PartWALInsert() breaks after the first matching block
 * reference per WAL record, preventing duplicate entries for cross-page
 * UPDATEs whose WAL record carries the same relfilenode in multiple blocks.
 *
 * ScanWALRangeForPartition() is kept for DemuxCrashRecovery: reads pg_wal
 * segments and writes data_len>0 records for any records missed at runtime.
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "partwal_sync.h"
#include "partition_wal.h"
#include "partition_wal_header.h"
#include "partition_wal_writer.h"
#include "demux_worker.h"
#include "shard_fileset.h"

#include "access/heapam_xlog.h"
#include "access/rmgr.h"
#include "access/xact.h"
#include "catalog/storage_xlog.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/*
 * 切主重构·prepare 接线（计划文档 §4 阶段 3 四步设计的第 2 步）。
 *
 * pg_partdist 不依赖 pg_raft，复制动作经 PostgreSQL rendezvous variable 注入：
 * pg_raft 的 _PG_init 把它的复制函数指针写进
 * "partdist_partwal_replicate_hook"；PartWALFlush 在 [A]（本地 parwal fsync）
 * 完成后、[B]（pg_wal 提交 fsync）之前，对本事务涉及的每个分区调用它。
 * 未装载 pg_raft / 未启用 raft 时指针为空，零开销。
 * 挂钩内部凑不齐多数派会 ERROR —— 事务在 prepare 即中止（步骤 4 语义）。
 */
typedef void (*PartWALReplicateHook) (Oid partition_id);
static void **partwal_replicate_hook_rv = NULL;

/*
 * ★ 触达分区集合：per-backend，在 PartWALInsert 时登记（DTX_2PC_DESIGN.md §9.1）
 *
 * 此前这个集合是在 PartWALFlush 里从环形缓冲区反推的（只收
 * slot->backend_id == MyBackendId 的槽位）。那个做法有一个正确性漏洞：
 * group commit 下并发 backend 会顺带把本事务的记录写进段文件并把槽位置
 * valid=false，于是
 *   (a) 本 backend 走 flushed_upto >= upto_lsn 的提前返回分支，复制挂钩
 *       根本不被调用；
 *   (b) 即使不提前返回，槽位已被消费，反推也拿不到分区号。
 * 结果是"本事务已 prepared 但字节从未达到多数派"。无 2PC 时它只表现为
 * 复制延后（下一次写入补齐），有 2PC 时协调者会据此写 COMMIT 决议 —— 丢数据。
 *
 * 改为在插入侧登记后，集合与"本事务写过哪些分区"严格对应，与谁做的落盘无关。
 * 数组按需增长（palloc 在 TopMemoryContext，与 partwal_pending 同生命周期），
 * 由 PartWALFlush / PartWALAbort 负责清空。
 */
static Oid *partwal_touched       = NULL;
static int  partwal_touched_count = 0;
static int  partwal_touched_cap   = 0;

static void PartWALNoteTouched(Oid partition_id);
static void PartWALResetTouched(void);
static void PartWALReplicateTouched(void);

/* ================================================================== */
/* Shmem names / tranche                                               */
/* ================================================================== */

#define PARTWAL_CTL_SHMEM_NAME    "pg_partdist_wal_ctl"
#define PARTWAL_HASH_SHMEM_NAME   "pg_partdist_relfile_hash"
#define PARTWAL_SYNC_LOCK_TRANCHE "pg_partdist_sync"

/* ================================================================== */
/* RelFileNumber -> partition_id lookup hash                           */
/* ================================================================== */

typedef struct PartWALRelEntry
{
    RelFileNumber relfilenode;  /* hash key */
    Oid           partition_id;
} PartWALRelEntry;

/* Shared control block (ring buffer + flushed_upto + lock) */
PartWALCtlData *PartWALCtl    = NULL;

/* Relfilenode -> partition_id hash (protected by PartWALCtl->lock) */
static HTAB    *PartWALRelHash = NULL;

/*
 * Per-backend max LSN tracked during PartWALInsert().
 * Mirrors XLogInsert's "insertion point" tracking.
 * Passed implicitly to PartWALFlush() when upto_lsn == InvalidXLogRecPtr.
 */
static XLogRecPtr partwal_my_max_lsn = InvalidXLogRecPtr;

/*
 * Per-backend WAL content capture array.
 *
 * Each PartWALInsert() call that matches a tracked partition allocates a
 * copy of the raw XLogRecord bytes (in TopMemoryContext) and appends an
 * entry here.  PartWALFlush() looks up entries by orig_lsn and passes the
 * bytes to AppendPartWALRecord() so pg_parwal records carry the full WAL
 * body (data_len > 0), enabling self-contained replica replay.
 *
 * Entries are freed and the count reset by FreePartWALPendingContent(),
 * called at the end of PartWALFlush() and PartWALAbort().
 *
 * Group-commit slots (backend_id != MyBackendId) are written with
 * data_len = 0 because their content lives in the peer backend's array.
 */
static PartWALPendingContent *partwal_pending      = NULL;
static int                    partwal_pending_count = 0;
static int                    partwal_pending_cap   = 0;

static void
FreePartWALPendingContent(void)
{
    int i;
    for (i = 0; i < partwal_pending_count; i++)
    {
        if (partwal_pending[i].data != NULL)
        {
            pfree(partwal_pending[i].data);
            partwal_pending[i].data = NULL;
        }
    }
    partwal_pending_count = 0;
    /* Keep the array allocated; capacity is reused across transactions. */
}

/* ------------------------------------------------------------------ */
/* 触达分区集合(DTX_2PC_DESIGN.md §9.1)                                */
/* ------------------------------------------------------------------ */

/*
 * PartWALNoteTouched — 登记"本事务写过分区 partition_id"。
 *
 * 必须在**释放 PartWALCtl->lock 之后**调用：这里会 palloc，而 palloc 在
 * LWLock 下不安全（与本文件捕获 WAL 字节的做法同理）。
 * 集合通常只有 1~2 个元素（一个事务很少跨很多分片），线性去重足够。
 */
static void
PartWALNoteTouched(Oid partition_id)
{
    int i;

    for (i = 0; i < partwal_touched_count; i++)
        if (partwal_touched[i] == partition_id)
            return;

    if (partwal_touched_count >= partwal_touched_cap)
    {
        int           new_cap = (partwal_touched_cap == 0)
                                ? 8 : partwal_touched_cap * 2;
        MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

        partwal_touched = (partwal_touched == NULL)
            ? palloc(new_cap * sizeof(Oid))
            : repalloc(partwal_touched, new_cap * sizeof(Oid));
        MemoryContextSwitchTo(old);
        partwal_touched_cap = new_cap;
    }

    partwal_touched[partwal_touched_count++] = partition_id;
}

static void
PartWALResetTouched(void)
{
    partwal_touched_count = 0;
    /* 保留数组本身，容量跨事务复用 */
}

/*
 * 对外的触达集合读写接口（DTX 接线用，声明见 partwal_sync.h）。
 *
 * PartWALCopyTouched 必须在 PartWALFlush() **之前**调用：flush 末尾的
 * PartWALReplicateTouched() 会把集合清空。
 */
int
PartWALTouchedCount(void)
{
    return partwal_touched_count;
}

int
PartWALCopyTouched(Oid *out, int max)
{
    int n = partwal_touched_count;

    if (out != NULL && max > 0)
    {
        int copy = (n < max) ? n : max;

        memcpy(out, partwal_touched, copy * sizeof(Oid));
    }
    return n;
}

void
PartWALNoteTouchedPartition(Oid partition_id)
{
    PartWALNoteTouched(partition_id);
}

/*
 * PartWALReplicateTouched — 对本事务触达的每个分区调用复制挂钩。
 *
 * 调用点必须满足：[A]（这些分区的 parwal 记录已落盘 fsync）已经完成，
 * [B]（本事务提交/prepare 记录的 XLogFlush）尚未发生，且已释放
 * PartWALCtl->lock（挂钩内部有网络往返，不能占着锁）。
 *
 * 挂钩 ERROR（写栅栏 / 凑不齐多数派）即事务中止 —— 这正是 prepare 语义。
 * 集合在**进入循环前**清空：挂钩若 ERROR，事务转入 abort 路径，
 * PartWALAbort 会再清一次，两处都不会把陈旧分区带进下一个事务。
 */
static void
PartWALReplicateTouched(void)
{
    PartWALReplicateHook fn;
    Oid                  local[8];
    Oid                 *list = local;
    int                  n = partwal_touched_count;
    int                  i;

    if (n == 0)
        return;

    if (partwal_replicate_hook_rv == NULL)
        partwal_replicate_hook_rv =
            find_rendezvous_variable("partdist_partwal_replicate_hook");
    fn = (PartWALReplicateHook) *partwal_replicate_hook_rv;

    if (fn == NULL)
    {
        PartWALResetTouched();
        return;                 /* 未装载 pg_raft：零开销，行为同接线前 */
    }

    /* 先把集合复制出来再清空，避免挂钩内部（经 SPI）反向触发写入时读到脏状态 */
    if (n > (int) lengthof(local))
        list = palloc(n * sizeof(Oid));
    memcpy(list, partwal_touched, n * sizeof(Oid));
    PartWALResetTouched();

    for (i = 0; i < n; i++)
        fn(list[i]);

    if (list != local)
        pfree(list);
}

/* ================================================================== */
/* Shmem lifecycle                                                     */
/* ================================================================== */

void
RequestPartWALSyncShmem(void)
{
    RequestAddinShmemSpace(sizeof(PartWALCtlData));
    RequestAddinShmemSpace(hash_estimate_size(PARTWAL_RELFHASH_SIZE,
                                               sizeof(PartWALRelEntry)));
    RequestNamedLWLockTranche(PARTWAL_SYNC_LOCK_TRANCHE, 1);
}

Size
PartWALSyncShmemSize(void)
{
    return sizeof(PartWALCtlData) +
           hash_estimate_size(PARTWAL_RELFHASH_SIZE, sizeof(PartWALRelEntry));
}

void
PartWALSyncShmemInit(void)
{
    bool    found;
    HASHCTL ctl;

    PartWALCtl = (PartWALCtlData *)
        ShmemInitStruct(PARTWAL_CTL_SHMEM_NAME,
                        sizeof(PartWALCtlData),
                        &found);
    if (!found)
    {
        PartWALCtl->lock         = &GetNamedLWLockTranche(PARTWAL_SYNC_LOCK_TRANCHE)[0].lock;
        PartWALCtl->write_pos    = 0;
        PartWALCtl->flushed_upto = InvalidXLogRecPtr;
        memset(PartWALCtl->slots, 0, sizeof(PartWALCtl->slots));
    }

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(RelFileNumber);
    ctl.entrysize = sizeof(PartWALRelEntry);

    PartWALRelHash = ShmemInitHash(PARTWAL_HASH_SHMEM_NAME,
                                    PARTWAL_RELFHASH_SIZE,
                                    PARTWAL_RELFHASH_SIZE,
                                    &ctl,
                                    HASH_ELEM | HASH_BLOBS | HASH_SHARED_MEM);
}

/* ================================================================== */
/* Registration                                                        */
/* ================================================================== */

void
PartWALSyncRegister(Oid partition_id, RelFileNumber relfilenode)
{
    PartWALRelEntry *entry;
    bool             found;

    if (PartWALRelHash == NULL || PartWALCtl == NULL)
        return;
    if (!RelFileNumberIsValid(relfilenode))
        return;

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);
    entry = (PartWALRelEntry *)
        hash_search(PartWALRelHash, &relfilenode, HASH_ENTER, &found);
    entry->partition_id = partition_id;
    LWLockRelease(PartWALCtl->lock);
}

bool
PartWALSyncIsRegistered(RelFileNumber relfilenode)
{
    bool found = false;

    if (PartWALRelHash == NULL || PartWALCtl == NULL)
        return false;
    if (!RelFileNumberIsValid(relfilenode))
        return false;

    LWLockAcquire(PartWALCtl->lock, LW_SHARED);
    (void) hash_search(PartWALRelHash, &relfilenode, HASH_FIND, &found);
    LWLockRelease(PartWALCtl->lock);
    return found;
}

/* ================================================================== */
/* PartWALInsert -- mirrors XLogInsert()                               */
/*                                                                     */
/* Called from wal_insert_hook after each XLogInsert().                */
/* Writes one slot to the shared ring buffer.                          */
/* Breaks after the first matching block (duplicate-block fix).        */
/* ================================================================== */

void
PartWALInsert(XLogRecPtr end_lsn,
              RmgrId rmid,
              uint8 info,
              const WALInsertBlockRef *blocks,
              int nblocks,
              const char *record_data,
              uint32 record_len)
{
    int              i;
    PartWALRelEntry *entry = NULL;
    bool             found = false;
    PartWALSlot     *slot;
    bool             wrote_slot = false;
    RelFileNumber    match_rfn = InvalidRelFileNumber;
    Oid              matched_partition = InvalidOid;
    RelFileLocator   smgr_loc;

    if (PartWALCtl == NULL || PartWALRelHash == NULL)
        return;

    /* Skip speculative-insert confirmation records */
    if (rmid == RM_HEAP_ID &&
        (info & ~XLR_INFO_MASK) == XLOG_HEAP_CONFIRM)
        return;

    /*
     * 无块引用路径（FRD §5.2 特判）：RM_SMGR create/truncate 不注册任何
     * buffer，locator 在 main data 里。补丁 0001-v2 让钩子对这类记录以
     * nblocks == 0 触发；这里在加锁前先把 locator 解析出来。
     */
    if (nblocks == 0)
    {
        if (rmid != RM_SMGR_ID ||
            !SmgrRecordGetLocator(record_data, record_len, info, &smgr_loc))
            return;
    }

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);

    if (nblocks == 0)
    {
        entry = (PartWALRelEntry *)
            hash_search(PartWALRelHash, &smgr_loc.relNumber,
                        HASH_FIND, &found);
        if (found)
            match_rfn = smgr_loc.relNumber;
    }
    else
    {
        /*
         * fileset 化捕获（FRD §5.2）：任一 block 的 relNumber 命中反向映射
         * 即归入该 shard 的流。不再按 forkno 过滤 —— VM/FSM fork 与主关系
         * 同 relNumber，天然命中。
         */
        for (i = 0; i < nblocks; i++)
        {
            entry = (PartWALRelEntry *)
                hash_search(PartWALRelHash, &blocks[i].rlocator.relNumber,
                            HASH_FIND, &found);
            if (found)
            {
                match_rfn = blocks[i].rlocator.relNumber;
                break;
            }
        }
    }

    if (found)
    {
        /*
         * Write to ring buffer.  If the target slot is still valid another
         * backend's unconsumed record would be lost -- this should not happen
         * with PARTWAL_BUFFER_SLOTS = 8192 under normal load.
         */
        slot = &PartWALCtl->slots[PartWALCtl->write_pos];
        if (slot->valid)
            ereport(WARNING,
                    (errmsg("pg_partdist: PartWAL ring buffer full at slot %d; "
                            "overwriting unconsumed entry",
                            PartWALCtl->write_pos)));

        slot->partition_id = entry->partition_id;
        slot->relfilenode  = match_rfn;

        /*
         * 记下分区号供出锁后登记触达集合 —— entry 指向共享哈希表，
         * 释放锁之后不得再解引用。
         */
        matched_partition  = entry->partition_id;
        slot->orig_lsn     = end_lsn;
        slot->start_lsn    = ProcLastRecPtr;    /* 本条记录的起始 LSN */
        slot->xid          = GetCurrentTransactionIdIfAny();
        slot->rmid         = rmid;
        slot->info         = info;
        slot->valid        = true;
        slot->backend_id   = MyBackendId;

        PartWALCtl->write_pos =
            (PartWALCtl->write_pos + 1) % PARTWAL_BUFFER_SLOTS;

        /* Track per-backend max LSN for PartWALFlush(InvalidXLogRecPtr) */
        if (partwal_my_max_lsn == InvalidXLogRecPtr ||
            end_lsn > partwal_my_max_lsn)
            partwal_my_max_lsn = end_lsn;

        /* One slot per WAL record per partition -- avoids duplicate entries
         * for cross-page UPDATEs that carry the same relfilenode in multiple
         * block references. */
        wrote_slot = true;
    }

    LWLockRelease(PartWALCtl->lock);

    /*
     * ★ 登记触达分区（DTX_2PC_DESIGN.md §9.1）。必须在这里而不是 flush 时
     * 从环形缓冲区反推 —— group commit 下槽位可能被并发 backend 消费掉。
     * 出锁后调用（内部 palloc）。
     */
    if (wrote_slot && OidIsValid(matched_partition))
        PartWALNoteTouched(matched_partition);

    /*
     * Capture WAL record body outside the LWLock (palloc is not safe under
     * LWLock).  The copy is stored in TopMemoryContext so it outlives the
     * current portal/transaction context and is available at PRE_COMMIT.
     *
     * Only captured when this backend wrote a slot (wrote_slot) and the
     * caller supplied a non-empty record buffer.  Group-commit slots written
     * by another backend get data_len = 0 at flush time (their content lives
     * in the peer backend's partwal_pending array).
     */
    if (wrote_slot && record_data != NULL && record_len > 0)
    {
        MemoryContext old;
        char         *copy;

        /* Grow the pending array if needed (doubles each time) */
        if (partwal_pending_count >= partwal_pending_cap)
        {
            int new_cap = (partwal_pending_cap == 0) ? 64
                                                     : partwal_pending_cap * 2;
            old = MemoryContextSwitchTo(TopMemoryContext);
            partwal_pending = (partwal_pending == NULL)
                ? palloc(new_cap * sizeof(PartWALPendingContent))
                : repalloc(partwal_pending,
                           new_cap * sizeof(PartWALPendingContent));
            MemoryContextSwitchTo(old);
            partwal_pending_cap = new_cap;
        }

        old  = MemoryContextSwitchTo(TopMemoryContext);
        copy = palloc(record_len);
        memcpy(copy, record_data, record_len);
        MemoryContextSwitchTo(old);

        partwal_pending[partwal_pending_count].orig_lsn  = end_lsn;
        partwal_pending[partwal_pending_count].xid       = GetCurrentTransactionIdIfAny();
        partwal_pending[partwal_pending_count].data      = copy;
        partwal_pending[partwal_pending_count].data_len  = record_len;
        partwal_pending_count++;
    }
}

/* ================================================================== */
/* 原始 WAL 记录字节装配                                               */
/* ================================================================== */

static void PartWALOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
                               TimeLineID *tli_p);
static void PartWALCloseSegment(XLogReaderState *state);
static int  PartWALReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr,
                            int reqLen, XLogRecPtr targetRecPtr, char *readBuf);

/*
 * AssembleRawWALRecord — 从 pg_wal 段文件按 start_lsn 装配一条记录的
 * **原始连续字节**（跨页时剥掉后续页的页头），并按 xl_crc 校验。
 *
 * 为什么不能用 reader->readRecordBuf：PG16 只有**跨页**记录才装配进
 * readRecordBuf；单页记录的 record 指针直指页缓冲(readBuf)，此时
 * readRecordBuf 里是陈旧字节。旧 demux 崩溃恢复路径正踩此坑 —— 单页
 * 记录写进 parwal 的 payload 是垃圾，仅因下游只校验过头部而未暴露。
 *
 * 返回 palloc 的缓冲（调用方负责 pfree）；失败返回 NULL。
 * 调用前提：该 LSN 区间已 XLogFlush 落盘。
 */
static char *
AssembleRawWALRecord(XLogReaderState *state, XLogRecPtr start_lsn,
                     uint32 total_len)
{
    char       *buf;
    char        pagebuf[XLOG_BLCKSZ];
    uint32      copied = 0;
    XLogRecPtr  cur = start_lsn;
    pg_crc32c   crc;
    XLogRecord *rechdr;

    if (total_len < SizeOfXLogRecord)
        return NULL;

    buf = palloc(total_len);

    while (copied < total_len)
    {
        XLogRecPtr pagestart = cur - (cur % XLOG_BLCKSZ);
        uint32     off_in_page;
        uint32     hdrsz;
        uint32     avail;
        uint32     n;

        if (PartWALReadPage(state, pagestart, XLOG_BLCKSZ, cur, pagebuf) < 0)
        {
            pfree(buf);
            return NULL;
        }

        hdrsz       = XLogPageHeaderSize((XLogPageHeader) pagebuf);
        off_in_page = (uint32) (cur % XLOG_BLCKSZ);
        if (off_in_page < hdrsz)
            off_in_page = hdrsz;    /* 续段从页头之后开始 */

        avail = XLOG_BLCKSZ - off_in_page;
        n     = Min(avail, total_len - copied);
        memcpy(buf + copied, pagebuf + off_in_page, n);
        copied += n;
        cur = pagestart + XLOG_BLCKSZ;
    }

    /* CRC 校验（与 ValidXLogRecord 同算法），装配错误在此 fail-fast */
    rechdr = (XLogRecord *) buf;
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, buf + SizeOfXLogRecord, total_len - SizeOfXLogRecord);
    COMP_CRC32C(crc, buf, offsetof(XLogRecord, xl_crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, rechdr->xl_crc))
    {
        ereport(WARNING,
                (errmsg("pg_partdist: 原始 WAL 记录装配 CRC 不符 @%X/%08X",
                        LSN_FORMAT_ARGS(start_lsn))));
        pfree(buf);
        return NULL;
    }

    return buf;
}

/*
 * ReadRawWALRecordAt — 定位 + 校验 + 装配 start_lsn 处的记录。
 * group-commit 场景下为 peer backend 的槽位补齐字节（其 pending 内容
 * 在 peer 的私有数组里，本 backend 拿不到）。
 */
static bool
ReadRawWALRecordAt(XLogRecPtr start_lsn, XLogRecPtr expect_end_lsn,
                   char **out_buf, uint32 *out_len, TransactionId *out_xid)
{
    static XLogReaderState *raw_reader = NULL;
    XLogRecord *record;
    char       *errormsg = NULL;
    char       *buf;
    MemoryContext oldctx;

    /*
     * ★ reader 与其全部内部缓冲必须活在 TopMemoryContext（2026-08-03 修）。
     *
     * 本函数只在 PRE_COMMIT 的 PartWALFlush 里被调用（group-commit 场景为
     * peer backend 的槽位回读字节），彼时 CurrentMemoryContext 是**事务级**
     * 上下文。此前 XLogReaderAllocate 未切换 context：static 指针跨事务存活，
     * 而 reader 结构与内部缓冲（readBuf/readRecordBuf/errormsg_buf）随事务
     * 结束被释放 —— 第二次进来就是 use-after-free，写穿的是**下一个事务**
     * 复用同一块内存后放进去的 palloc chunk。症状：并发写入下 backend 报
     * `pfree called with invalid pointer 0x...`（每次同一地址：fork 出的
     * backend 分配序列相同）或直接 SIGSEGV，节点整体进 crash recovery。
     * 串行负载永远走不到这条回读路径，所以此前从未暴露。
     *
     * 同理，XLogReadRecord 期间的**懒分配**（decode_buffer 首次读取时创建、
     * readRecordBuf 遇长记录扩容）也发生在"调用时"的 context 里，所以整个
     * 读取过程都必须在 TopMemoryContext 下执行，不能只包 Allocate。
     * ERROR 逃逸时 context 由事务中止路径统一恢复，无需 PG_TRY。
     */
    oldctx = MemoryContextSwitchTo(TopMemoryContext);

    if (raw_reader == NULL)
    {
        raw_reader = XLogReaderAllocate(wal_segment_size, NULL,
                                        XL_ROUTINE(.page_read     = PartWALReadPage,
                                                   .segment_open  = PartWALOpenSegment,
                                                   .segment_close = PartWALCloseSegment),
                                        NULL);
        if (raw_reader == NULL)
        {
            MemoryContextSwitchTo(oldctx);
            return false;
        }
    }

    XLogBeginRead(raw_reader, start_lsn);
    record = XLogReadRecord(raw_reader, &errormsg);
    MemoryContextSwitchTo(oldctx);
    if (record == NULL)
    {
        ereport(WARNING,
                (errmsg("pg_partdist: 无法读取 peer WAL 记录 @%X/%08X: %s",
                        LSN_FORMAT_ARGS(start_lsn),
                        errormsg ? errormsg : "(no message)")));
        return false;
    }

    if (expect_end_lsn != InvalidXLogRecPtr &&
        raw_reader->EndRecPtr != expect_end_lsn)
    {
        ereport(WARNING,
                (errmsg("pg_partdist: peer WAL 记录端点不符 @%X/%08X "
                        "(读到 %X/%08X, 期望 %X/%08X)",
                        LSN_FORMAT_ARGS(start_lsn),
                        LSN_FORMAT_ARGS(raw_reader->EndRecPtr),
                        LSN_FORMAT_ARGS(expect_end_lsn))));
        return false;
    }

    buf = AssembleRawWALRecord(raw_reader, start_lsn, record->xl_tot_len);
    if (buf == NULL)
        return false;

    *out_buf = buf;
    *out_len = record->xl_tot_len;
    *out_xid = XLogRecGetXid(raw_reader);
    return true;
}

/* ================================================================== */
/* PartWALFlush -- mirrors XLogFlush(lsn)                             */
/*                                                                     */
/* Drains all valid ring-buffer slots with orig_lsn <= upto_lsn to    */
/* pg_parwal segment files and fsyncs.                                 */
/* Group commit: returns immediately if flushed_upto >= upto_lsn.     */
/* ================================================================== */

#define PARTWAL_WRITER_CACHE_MAX  32

void
PartWALFlush(XLogRecPtr upto_lsn)
{
    int                 i;
    Oid                 cache_partition[PARTWAL_WRITER_CACHE_MAX];
    PartitionWALWriter *cache_writer[PARTWAL_WRITER_CACHE_MAX];
    int                 ncached = 0;
    XLogRecPtr          last_lsn = InvalidXLogRecPtr;

    /* Default: flush up to the per-backend tracked max LSN */
    if (upto_lsn == InvalidXLogRecPtr)
        upto_lsn = partwal_my_max_lsn;

    if (upto_lsn == InvalidXLogRecPtr)
    {
        partwal_my_max_lsn = InvalidXLogRecPtr;

        /*
         * 本 backend 没有经 WAL 插入挂钩产生过待落盘记录，但触达集合仍可能
         * 非空 —— DTX 接线会在第一次 flush 之后**直接**往段文件追加
         * DTX_PREPARE 标记并重新登记触达分区，正是靠这条路径把标记复制出去。
         * 复制挂钩取的是"当前 flush 点"，直接追加的记录已经在盘上，覆盖成立。
         */
        PartWALReplicateTouched();
        return;
    }

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);

    /*
     * Group commit: another backend already wrote our records to pg_parwal.
     * Mirrors XLogFlush()'s "already flushed" early-return check.
     */
    if (PartWALCtl->flushed_upto != InvalidXLogRecPtr &&
        PartWALCtl->flushed_upto >= upto_lsn)
    {
        LWLockRelease(PartWALCtl->lock);
        partwal_my_max_lsn = InvalidXLogRecPtr;
        FreePartWALPendingContent();

        /*
         * ★ 让路窗口修复（DTX_2PC_DESIGN.md §9.1）：本事务的记录已由并发
         * backend 写进段文件并 fsync（flushed_upto 是在持锁状态下、writer
         * 析构即 fsync 之后才推进的，所以这里的 >= 判定蕴含"已落盘"），
         * [A] 对本事务同样成立 —— 因此**必须照常复制**，不能直接返回。
         *
         * 修复前这条路径静默跳过复制，事务照样 prepared 成功，字节却从未
         * 达到多数派。挂钩内部按"当前 flush 点"取增量上界，而该上界必然
         * >= 本事务记录的 partition_lsn（我们的记录已经在盘上了），所以
         * 覆盖是安全的。
         */
        PartWALReplicateTouched();
        return;
    }

    /*
     * 先把 pg_wal 刷到 upto_lsn：group-commit 场景下我们会消费 peer backend
     * 的槽位，其记录字节不在本 backend 的 pending 数组里，只能按 start_lsn
     * 从 pg_wal 回读 —— 回读的前提是字节已落盘。时序不变式不受影响：
     * 这里刷的只是数据记录（[B] 是本事务提交记录的 XLogFlush，尚未发生），
     * [A] parwal fsync 仍然先于 [B]。
     */
    XLogFlush(upto_lsn);

    PG_TRY();
    {
        for (i = 0; i < PARTWAL_BUFFER_SLOTS; i++)
        {
            PartWALSlot        *slot = &PartWALCtl->slots[i];
            PartitionWALWriter *writer = NULL;
            int                 j;
            bool                cache_it;

            if (!slot->valid)
                continue;
            if (slot->orig_lsn > upto_lsn)
                continue;

            /* Look for cached writer for this partition */
            for (j = 0; j < ncached; j++)
            {
                if (cache_partition[j] == slot->partition_id)
                {
                    writer = cache_writer[j];
                    break;
                }
            }

            if (writer == NULL)
            {
                InitPartitionWALDirectory(slot->partition_id);
                writer = CreatePartitionWALWriter(slot->partition_id,
                                                  slot->relfilenode);
                if (writer == NULL)
                    ereport(ERROR,
                            (errmsg("pg_partdist: could not create WAL writer "
                                    "for partition %u", slot->partition_id)));

                cache_it = (ncached < PARTWAL_WRITER_CACHE_MAX);
                if (cache_it)
                {
                    cache_partition[ncached] = slot->partition_id;
                    cache_writer[ncached]    = writer;
                    ncached++;
                }
            }
            else
                cache_it = true;

            /*
             * Look up the WAL record body captured by PartWALInsert().
             * 本 backend 的槽位从 pending 数组取；peer backend 的槽位
             * （group-commit）按 start_lsn 从 pg_wal 回读 —— 流必须
             * 自包含（data_len > 0），data_len=0 的 DATA 记录会让
             * 物理回放断链（FRD §5/§7）。
             */
            {
                const char    *wal_data  = NULL;
                uint32         wal_len   = 0;
                TransactionId  wal_xid   = slot->xid;
                char          *read_buf  = NULL;

                if (slot->backend_id == MyBackendId)
                {
                    int k;
                    for (k = 0; k < partwal_pending_count; k++)
                    {
                        if (partwal_pending[k].orig_lsn == slot->orig_lsn)
                        {
                            wal_data = partwal_pending[k].data;
                            wal_len  = partwal_pending[k].data_len;
                            wal_xid  = partwal_pending[k].xid;
                            break;
                        }
                    }
                }

                if (wal_data == NULL &&
                    slot->start_lsn != InvalidXLogRecPtr)
                {
                    uint32        rlen = 0;
                    TransactionId rxid = InvalidTransactionId;

                    if (ReadRawWALRecordAt(slot->start_lsn, slot->orig_lsn,
                                           &read_buf, &rlen, &rxid))
                    {
                        wal_data = read_buf;
                        wal_len  = rlen;
                        wal_xid  = rxid;
                    }
                }

                AppendPartWALRecord(writer, slot->orig_lsn,
                                    slot->rmid, slot->info,
                                    PARTWAL_FLAG_DATA,
                                    wal_data, wal_len, wal_xid);

                if (read_buf != NULL)
                    pfree(read_buf);

                /*
                 * 触达集合不再在这里从环形缓冲区反推 —— 见
                 * PartWALNoteTouched()（PartWALInsert 时按 backend 登记）。
                 * 反推的问题：group commit 下本事务的槽位可能已被并发
                 * backend 消费，反推不到；而 peer 的槽位又不属于本事务。
                 */
            }
            last_lsn = slot->orig_lsn;

            slot->valid = false;  /* consumed */

            if (!cache_it)
                DestroyPartitionWALWriter(writer);
        }

        /* Flush and close all cached writers (includes fsync) */
        for (i = 0; i < ncached; i++)
            DestroyPartitionWALWriter(cache_writer[i]);
        ncached = 0;
    }
    PG_CATCH();
    {
        /* Close any still-open cached writers without fsync */
        for (i = 0; i < ncached; i++)
        {
            if (cache_writer[i] != NULL && cache_writer[i]->fd >= 0)
            {
                close(cache_writer[i]->fd);
                cache_writer[i]->fd = -1;
            }
            pfree(cache_writer[i]);
        }
        LWLockRelease(PartWALCtl->lock);
        /* Leave partwal_my_max_lsn set so PartWALAbort() can clean up slots */
        PG_RE_THROW();
    }
    PG_END_TRY();

    /* Update high-water mark */
    if (PartWALCtl->flushed_upto == InvalidXLogRecPtr ||
        PartWALCtl->flushed_upto < upto_lsn)
        PartWALCtl->flushed_upto = upto_lsn;

    LWLockRelease(PartWALCtl->lock);

    /* Advance last_processed_lsn for demux_progress() / demux_flush() */
    if (DemuxState != NULL && last_lsn != InvalidXLogRecPtr)
    {
        LWLockAcquire(DemuxState->lock, LW_EXCLUSIVE);
        if (DemuxState->last_processed_lsn == InvalidXLogRecPtr ||
            DemuxState->last_processed_lsn < last_lsn)
            DemuxState->last_processed_lsn = last_lsn;
        LWLockRelease(DemuxState->lock);
    }

    FreePartWALPendingContent();
    partwal_my_max_lsn = InvalidXLogRecPtr;

    /*
     * [A] 已完成（本地 parwal 已 fsync）。在 [B]（提交/prepare 记录的
     * XLogFlush）之前，把本事务涉及分区的新记录复制到各自的 raft 组。
     * 锁已全部释放，网络往返不占 PartWALCtl；挂钩 ERROR 即事务中止。
     */
    PartWALReplicateTouched();
}

/* ================================================================== */
/* PartWALAbort -- discard this backend's pending ring-buffer slots    */
/* ================================================================== */

void
PartWALAbort(void)
{
    int i;

    PartWALResetTouched();

    if (partwal_my_max_lsn == InvalidXLogRecPtr)
        return;  /* no inserts from this backend */

    if (PartWALCtl == NULL)
    {
        partwal_my_max_lsn = InvalidXLogRecPtr;
        return;
    }

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);

    for (i = 0; i < PARTWAL_BUFFER_SLOTS; i++)
    {
        PartWALSlot *slot = &PartWALCtl->slots[i];
        if (slot->valid && slot->backend_id == MyBackendId)
            slot->valid = false;
    }

    LWLockRelease(PartWALCtl->lock);
    FreePartWALPendingContent();
    partwal_my_max_lsn = InvalidXLogRecPtr;
}

/* ================================================================== */
/* WAL reader callbacks (used by ScanWALRangeForPartition)             */
/* ================================================================== */

static void PartWALOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
                               TimeLineID *tli_p);
static void PartWALCloseSegment(XLogReaderState *state);

static int
PartWALReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr,
                int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
    off_t      offset;
    ssize_t    count;
    XLogSegNo  segno;
    TimeLineID tli = 1;

    XLByteToSeg(targetPagePtr, segno, state->segcxt.ws_segsize);
    if (state->seg.ws_file < 0 || state->seg.ws_segno != segno)
    {
        if (state->seg.ws_file >= 0)
            PartWALCloseSegment(state);
        PartWALOpenSegment(state, segno, &tli);
    }

    if (state->seg.ws_file < 0)
        return -1;

    offset = XLogSegmentOffset(targetPagePtr, state->segcxt.ws_segsize);
    if (lseek(state->seg.ws_file, (off_t) offset, SEEK_SET) < 0)
        return -1;

    count = read(state->seg.ws_file, readBuf, XLOG_BLCKSZ);

    /*
     * 短读按失败处理（page_read 回调的契约是"至少 reqLen 字节"）。
     * 此前 count < reqLen 也当成功返回，readBuf 尾部是未初始化内存 ——
     * AssembleRawWALRecord 有 CRC 兜底，但 XLogReadRecord 路径会拿它当
     * 页头解析。wal_init_zero=on 时段文件预分配为整段，正常读不满页的
     * 只有文件被并发回收/截断的窗口，此时就该失败重来。
     */
    return (count < reqLen) ? -1 : (int) count;
}

static void
PartWALOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
                   TimeLineID *tli_p)
{
    char fname[MAXPGPATH];
    char path[MAXPGPATH];

    *tli_p = 1;
    XLogFileName(fname, *tli_p, nextSegNo, state->segcxt.ws_segsize);
    snprintf(path, MAXPGPATH, "%s/pg_wal/%s", DataDir, fname);

    state->seg.ws_file = open(path, O_RDONLY | PG_BINARY, 0);
    if (state->seg.ws_file < 0)
    {
        ereport(DEBUG1,
                (errmsg("pg_partdist sync: WAL segment \"%s\" not found",
                        path)));
        return;
    }
    state->seg.ws_segno = nextSegNo;
    state->seg.ws_tli   = *tli_p;
}

static void
PartWALCloseSegment(XLogReaderState *state)
{
    if (state->seg.ws_file >= 0)
    {
        close(state->seg.ws_file);
        state->seg.ws_file = -1;
    }
}

/* ================================================================== */
/* WAL range scan (for crash recovery only)                            */
/* ================================================================== */

/*
 * RecordTouchesPartition — 记录是否属于该分区的物理子流。
 *
 * 判据与运行时捕获(PartWALInsert)一致：任一 block 的 relNumber 经
 * shmem 反向哈希命中该 partition_id（fileset 化，覆盖索引/TOAST/VM/FSM）；
 * 无块引用的 RM_SMGR 记录按 main data 里的 locator 特判（FRD §5.2）。
 */
static bool
RecordTouchesPartition(XLogReaderState *reader, Oid partition_id)
{
    int blk;
    int max_blk;

    if (!XLogRecHasAnyBlockRefs(reader))
    {
        RelFileLocator loc;
        xl_smgr_truncate trunc;
        xl_smgr_create   create;
        uint8   op;

        if (XLogRecGetRmid(reader) != RM_SMGR_ID)
            return false;

        op = XLogRecGetInfo(reader) & XLR_RMGR_INFO_MASK;
        if (op == XLOG_SMGR_TRUNCATE &&
            XLogRecGetDataLen(reader) >= sizeof(trunc))
        {
            memcpy(&trunc, XLogRecGetData(reader), sizeof(trunc));
            loc = trunc.rlocator;
        }
        else if (op == XLOG_SMGR_CREATE &&
                 XLogRecGetDataLen(reader) >= sizeof(create))
        {
            memcpy(&create, XLogRecGetData(reader), sizeof(create));
            loc = create.rlocator;
        }
        else
            return false;

        {
            PartWALRelEntry *entry;
            bool             found;

            LWLockAcquire(PartWALCtl->lock, LW_SHARED);
            entry = (PartWALRelEntry *)
                hash_search(PartWALRelHash, &loc.relNumber, HASH_FIND, &found);
            LWLockRelease(PartWALCtl->lock);
            return found && entry->partition_id == partition_id;
        }
    }

    max_blk = XLogRecMaxBlockId(reader);

    for (blk = 0; blk <= max_blk; blk++)
    {
        RelFileLocator rlocator;
        ForkNumber     fork;
        BlockNumber    blkno;

        if (XLogRecGetBlockTagExtended(reader, (uint8) blk, &rlocator, &fork,
                                        &blkno, NULL))
        {
            PartWALRelEntry *entry;
            bool             found;

            LWLockAcquire(PartWALCtl->lock, LW_SHARED);
            entry = (PartWALRelEntry *)
                hash_search(PartWALRelHash, &rlocator.relNumber,
                            HASH_FIND, &found);
            LWLockRelease(PartWALCtl->lock);
            if (found && entry->partition_id == partition_id)
                return true;
        }
    }
    return false;
}

XLogRecPtr
ScanWALRangeForPartition(PartitionWALWriter *writer,
                          XLogRecPtr start_lsn,
                          XLogRecPtr end_lsn)
{
    XLogReaderState *reader;
    XLogRecord      *record;
    char            *errormsg;
    XLogRecPtr       last_read   = start_lsn;
    TimeLineID       tli         = 1;
    int              null_streak = 0;
    int              records_written  = 0;
    int              records_scanned  = 0;

    if (start_lsn == InvalidXLogRecPtr || end_lsn == InvalidXLogRecPtr)
        return last_read;

    /* Clamp to the current flush frontier */
    {
        XLogRecPtr flush_lsn = GetFlushRecPtr(&tli);
        if (end_lsn > flush_lsn)
            end_lsn = flush_lsn;
    }

    if (start_lsn >= end_lsn)
        return last_read;

    reader = XLogReaderAllocate(wal_segment_size, NULL,
                                XL_ROUTINE(.page_read     = PartWALReadPage,
                                           .segment_open  = PartWALOpenSegment,
                                           .segment_close = PartWALCloseSegment),
                                NULL);
    if (reader == NULL)
    {
        ereport(WARNING,
                (errmsg("pg_partdist sync: could not allocate WAL reader "
                        "for partition %u", writer->partition_id)));
        return last_read;
    }

    XLogBeginRead(reader, start_lsn);

    while (reader->EndRecPtr < end_lsn)
    {
        uint8 rmid;

        errormsg = NULL;

        PG_TRY();
        {
            record = XLogReadRecord(reader, &errormsg);
        }
        PG_CATCH();
        {
            FlushErrorState();
            PartWALCloseSegment(reader);
            reader->readRecordBuf     = NULL;
            reader->readRecordBufSize = 0;
            record = NULL;
        }
        PG_END_TRY();

        if (record == NULL)
        {
            if (errormsg != NULL)
                ereport(DEBUG1,
                        (errmsg("pg_partdist sync: WAL read error for "
                                "partition %u at %X/%08X: %s",
                                writer->partition_id,
                                LSN_FORMAT_ARGS(reader->EndRecPtr),
                                errormsg)));
            null_streak++;
            if (null_streak > 3)
                break;
            continue;
        }

        null_streak    = 0;
        last_read      = reader->EndRecPtr;
        records_scanned++;

        if (!RecordTouchesPartition(reader, writer->partition_id))
            continue;

        rmid = XLogRecGetRmid(reader);

        /* Skip speculative-insert confirmation records */
        if (rmid == RM_HEAP_ID &&
            (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_HEAP_CONFIRM)
            continue;

        /*
         * Skip records already written (dedup guard).
         * orig_lsn 语义 = end LSN（FRD §4.2），比较也用 EndRecPtr。
         */
        if (reader->EndRecPtr <= writer->last_wal_lsn)
            continue;

        PG_TRY();
        {
            /*
             * 原始字节必须重新装配：readRecordBuf 仅对跨页记录有效，
             * 单页记录的原始字节在页缓冲里（见 AssembleRawWALRecord）。
             */
            char *raw = AssembleRawWALRecord(reader, reader->ReadRecPtr,
                                             record->xl_tot_len);

            if (raw != NULL)
            {
                AppendPartWALRecord(writer,
                                    reader->EndRecPtr,   /* end LSN, §4.2 */
                                    rmid,
                                    XLogRecGetInfo(reader),
                                    PARTWAL_FLAG_DATA,
                                    raw,
                                    record->xl_tot_len,
                                    XLogRecGetXid(reader));
                pfree(raw);
                records_written++;
            }
            else
                ereport(WARNING,
                        (errmsg("pg_partdist sync: 无法装配原始记录 "
                                "for partition %u at LSN %X/%08X",
                                writer->partition_id,
                                LSN_FORMAT_ARGS(reader->ReadRecPtr))));
        }
        PG_CATCH();
        {
            FlushErrorState();
            ereport(WARNING,
                    (errmsg("pg_partdist sync: failed to write record "
                            "for partition %u at LSN %X/%08X",
                            writer->partition_id,
                            LSN_FORMAT_ARGS(reader->ReadRecPtr))));
        }
        PG_END_TRY();
    }

    PartWALCloseSegment(reader);
    XLogReaderFree(reader);

    ereport(DEBUG1,
            (errmsg("pg_partdist sync: scan done pid=%u rfn=%u "
                    "start=%X/%08X end=%X/%08X scanned=%d written=%d",
                    writer->partition_id, writer->relfilenode,
                    LSN_FORMAT_ARGS(start_lsn),
                    LSN_FORMAT_ARGS(end_lsn),
                    records_scanned, records_written)));

    return last_read;
}
