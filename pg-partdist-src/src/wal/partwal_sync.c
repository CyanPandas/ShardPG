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
#include "utils/timestamp.h"        /* GetCurrentTimestamp（TSO 就位前的时间源）*/
#include "tso.h"                    /* T4.4：TsoMarkerCommitTs 换源 */

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
 * 本事务已由 PartWALFlush 落盘的最大 LSN。partwal_my_max_lsn 在 flush 结束时
 * 就被清掉（它表达的是"还有未落盘的插入"），但中止路径需要知道"字节其实已经
 * 在盘上了"才能决定要不要补 ABORT 标记 —— 那个事实由这个变量保存到事务结束。
 */
static XLogRecPtr partwal_my_flushed_lsn = InvalidXLogRecPtr;

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

/*
 * 本事务（本 backend）碰过的分区集合 —— MARKER、复制挂钩与 DTX 接线的
 * 唯一依据（DTX_2PC_DESIGN.md §9.1）。
 *
 * 为什么不像原来那样从 PartWALCtl->slots 里推导：group commit 下 peer backend
 * 可能已经把本事务的槽位消费掉了，此时本 backend 的 PartWALFlush 会在
 * "flushed_upto >= upto_lsn" 处提前返回，一个槽位都遍历不到 —— 于是既不写
 * 提交标记、也不触发复制，而字节其实已经落在段文件里了。改成 backend 本地
 * 记账后，谁写的盘都不影响"本事务碰了哪些分区"这个事实。
 *
 * 数组按需翻倍，不设静默上限：漏掉一个分区就是漏掉一份提交标记，
 * follower 侧该事务永远不可见。
 */
static Oid *partwal_my_touched  = NULL;
static int  partwal_my_ntouched = 0;
static int  partwal_my_touched_cap = 0;

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

    for (i = 0; i < partwal_my_ntouched; i++)
        if (partwal_my_touched[i] == partition_id)
            return;

    if (partwal_my_ntouched >= partwal_my_touched_cap)
    {
        int           new_cap = (partwal_my_touched_cap == 0)
                                ? 8 : partwal_my_touched_cap * 2;
        MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

        partwal_my_touched = (partwal_my_touched == NULL)
            ? palloc(new_cap * sizeof(Oid))
            : repalloc(partwal_my_touched, new_cap * sizeof(Oid));
        MemoryContextSwitchTo(old);
        partwal_my_touched_cap = new_cap;
    }

    partwal_my_touched[partwal_my_ntouched++] = partition_id;
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
    return partwal_my_ntouched;
}

int
PartWALCopyTouched(Oid *out, int max)
{
    int n = partwal_my_ntouched;

    if (out != NULL && max > 0)
    {
        int copy = (n < max) ? n : max;

        memcpy(out, partwal_my_touched, copy * sizeof(Oid));
    }
    return n;
}

void
PartWALNoteTouchedPartition(Oid partition_id)
{
    PartWALNoteTouched(partition_id);
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
        PartWALCtl->lock              = &GetNamedLWLockTranche(PARTWAL_SYNC_LOCK_TRANCHE)[0].lock;
        PartWALCtl->write_pos         = 0;
        PartWALCtl->flushed_upto      = InvalidXLogRecPtr;
        PartWALCtl->freeze_last_check = 0;
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
    Oid              match_partition = InvalidOid;
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

        /*
         * 记下分区号供出锁后登记触达集合 —— entry 指向共享哈希表，
         * 释放锁之后不得再解引用。
         */
        match_partition    = entry->partition_id;
        slot->partition_id = match_partition;
        slot->relfilenode  = match_rfn;
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
     * 登记"本事务碰过这个分区"。放在锁外：数组是 backend 本地的，
     * 且 palloc 不能在 LWLock 下做 —— 所以 partition_id 在锁内先取到局部变量，
     * 不能出锁后再解 entry（shmem 哈希项出锁即不保证有效）。
     */
    if (wrote_slot && OidIsValid(match_partition))
        PartWALNoteTouched(match_partition);

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

/*
 * 排空顺序的排序缓冲与比较器（见 PartWALFlush 里 drain_order 的长注释）。
 *
 * 数组是文件级 static 而不是栈上变量：PARTWAL_BUFFER_SLOTS 条 int 有 32KB，
 * 放栈上太大；也不能 palloc —— 这段代码持 PartWALCtl->lock，而本文件的规矩是
 * 锁下不 palloc。static 在这里安全：PartWALFlush 全程持排他锁，同一进程内
 * 不会重入，跨进程各有各的副本。
 */
static int drain_order[PARTWAL_BUFFER_SLOTS];

static int
PartWALSlotCmp(const void *a, const void *b)
{
    const PartWALSlot *sa = &PartWALCtl->slots[*(const int *) a];
    const PartWALSlot *sb = &PartWALCtl->slots[*(const int *) b];

    if (sa->orig_lsn < sb->orig_lsn)
        return -1;
    if (sa->orig_lsn > sb->orig_lsn)
        return 1;
    if (sa->start_lsn < sb->start_lsn)
        return -1;
    if (sa->start_lsn > sb->start_lsn)
        return 1;
    return 0;
}

/*
 * PartWALBuildTxnMarker — 组装本事务的 MARKER 载荷（FRD §4.3）。
 *
 * 返回 palloc 出来的缓冲区，*out_len 是 24 + 4*nsubxacts。
 * 时间戳源已收口（T4.4）：TsoMarkerCommitTs —— 遗留模式=本地 TimestampTz
 * （微秒，与先前逐字节一致），TSO 模式=本事务暂存的 TSO commit_ts；
 * 字段宽度与磁盘格式都不动。
 */
char *
PartWALBuildMarkerPayload(bool with_children, bool with_commit_ts,
                          uint32 *out_len)
{
    TransactionId  *children = NULL;
    int             nchildren;
    char           *buf;
    TxnMarkerPayload *m;

    /*
     * 已提交子事务清单。中止的子事务**不在**这个列表里 —— 于是它们的 gxid
     * 在增强型 CLOG 中没有 COMMITTED 记录，天然不可见，SAVEPOINT 回滚语义
     * 由"缺席"表达，不需要额外的中止清单。
     */
    nchildren = with_children ? xactGetCommittedChildren(&children) : 0;
    if (nchildren < 0)
        nchildren = 0;

    *out_len = (uint32) TxnMarkerPayloadSize(nchildren);
    buf = palloc0(*out_len);        /* palloc0：reserved 与尾部必须是确定字节 */

    m = (TxnMarkerPayload *) buf;
    m->start_ts  = (uint64) GetCurrentTransactionStartTimestamp();
    m->commit_ts = with_commit_ts ? (uint64) TsoMarkerCommitTs()
                                  : UINT64CONST(0);
    m->nsubxacts = (uint32) nchildren;
    m->reserved  = 0;

    if (nchildren > 0)
        memcpy(TxnMarkerSubxacts(m), children,
               (size_t) nchildren * sizeof(TransactionId));

    return buf;
}

static char *
PartWALBuildTxnMarker(bool committed, uint32 *out_len)
{
    /* 单机路径：提交才收集子事务、才带提交时间戳 */
    return PartWALBuildMarkerPayload(committed, committed, out_len);
}

/*
 * PartWALAppendTxnMarker — 给一个分区追加一条 MARKER 记录。
 *
 * 调用方必须持有 PartWALCtl->lock：partition_lsn 的分配是"读盘上的
 * last_partition_lsn 再 +1"，并发写同一分区不加锁会分配出相同编号。
 *
 * orig_lsn 取本事务数据记录的最大 LSN（此刻提交记录还没写进 pg_wal，
 * 拿不到真正的提交 LSN）：既保证 orig_lsn 在流内单调，也让标记与它所
 * 标记的数据落在同一个段文件里（段号由 orig_lsn 换算）。
 */
static void
PartWALAppendTxnMarker(PartitionWALWriter *writer, XLogRecPtr orig_lsn,
                       TransactionId xid, bool committed,
                       const char *payload, uint32 payload_len)
{
    AppendPartWALRecord(writer, orig_lsn,
                        RM_XACT_ID,
                        committed ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT,
                        payload, payload_len,
                        MakeGlobalXid(PartDistLocalNodeId(), xid),
                        PARTWAL_FLAG_MARKER);
}

/*
 * PartWALAppendMarkerFor — 给指定分区独立追加一条 MARKER 并 fsync（2PC 用）。
 *
 * 与 PartWALFlush 内联的那条标记写入路径的区别，全在"标记属于谁"：
 * 单机事务的标记 xid 就是当前事务的 xid，可以就地取；而 2PC 的
 * COMMIT/ABORT 标记是在 **COMMIT PREPARED 那条语句自己的事务**里补写的，
 * 被标记的是那笔早已 prepared 的事务，xid 必须由调用方显式给出
 * （见 DTX_2PC_DESIGN.md §3.3 阶段 3）。
 *
 * op 取 XLOG_XACT_PREPARE / XLOG_XACT_COMMIT / XLOG_XACT_ABORT。
 *
 * orig_lsn 传 0：标记不是 WAL 记录。段号因此沿用 writer 从 checkpoint
 * 恢复出来的当前段（AppendPartWALRecordAt 对 orig_lsn==0 有专门守卫），
 * 不会把标记甩到 1 号段去。
 *
 * 自己取 PartWALCtl->lock —— 调用方（PRE_PREPARE 接线、阶段 3）此刻都不持有。
 */
void
PartWALAppendMarkerFor(Oid partition_id, TransactionId xid, uint8 op,
                       const char *payload, uint32 payload_len)
{
    PartitionWALWriter *w;

    if (!TransactionIdIsValid(xid))
        return;                 /* 无 xid 可标记：无账可记 */

    if (PartWALCtl != NULL)
        LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);
    PG_TRY();
    {
        InitPartitionWALDirectory(partition_id);
        w = CreatePartitionWALWriter(partition_id, InvalidRelFileNumber);
        if (w == NULL)
            ereport(ERROR,
                    (errmsg("pg_partdist: 无法为分区 %u 打开 writer 写 2PC 标记",
                            partition_id)));

        AppendPartWALRecord(w, InvalidXLogRecPtr,
                            RM_XACT_ID, op,
                            payload, payload_len,
                            MakeGlobalXid(PartDistLocalNodeId(), xid),
                            PARTWAL_FLAG_MARKER);
        DestroyPartitionWALWriter(w);   /* flush + fsync */
    }
    PG_FINALLY();
    {
        if (PartWALCtl != NULL)
            LWLockRelease(PartWALCtl->lock);
    }
    PG_END_TRY();
}

static void PartWALReplicateTouched(void);

/*
 * PartWALHasPendingRecords — 本 backend 当前事务是否还有未落盘的分区记录。
 *
 * 冻结账目发射（§13 约束 5）用它来**回避**：正在写分片流的事务里插一条 CTRL，
 * 会让 CTRL 的 partition_lsn 排到那些逻辑上更早的 DATA 前面，流序错乱。
 */
bool
PartWALHasPendingRecords(void)
{
    return partwal_my_max_lsn != InvalidXLogRecPtr;
}

/*
 * PartWALFreezeCheckDue — 冻结账目检查的节点级限流（§13 约束 5）。
 * 语义与实现动机见 partwal_sync.h 的声明处注释。
 */
bool
PartWALFreezeCheckDue(int interval_ms)
{
    TimestampTz now = GetCurrentTimestamp();
    bool        due;

    if (PartWALCtl == NULL)
        return false;

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);
    due = (PartWALCtl->freeze_last_check == 0) ||
          (interval_ms <= 0) ||
          TimestampDifferenceExceeds(PartWALCtl->freeze_last_check, now,
                                     interval_ms);
    if (due)
        PartWALCtl->freeze_last_check = now;
    LWLockRelease(PartWALCtl->lock);

    return due;
}

/*
 * PartWALSyncListPartitions — 收集本节点当前捕获的全部分区 OID（去重）。
 *
 * 反向哈希的键是 relfilenode、值是 partition_id，一个分区有多个成员
 * （主堆/索引/TOAST），所以要去重。返回写进 out 的个数。
 *
 * 用途：DDL 之后需要"把本地每个 shard 的 fileset 重新算一遍再 diff"
 * （shard_fileset.c 的 §12 发射路径）。不从 pg_parwal 目录枚举是因为
 * 目录里也有本节点作为 follower 持有的副本流，那些 shard 的 fileset
 * 不归本节点维护。
 */
int
PartWALSyncListPartitions(Oid *out, int max)
{
    HASH_SEQ_STATUS  seq;
    PartWALRelEntry *entry;
    int              n = 0;

    if (PartWALRelHash == NULL || PartWALCtl == NULL || max <= 0)
        return 0;

    LWLockAcquire(PartWALCtl->lock, LW_SHARED);
    hash_seq_init(&seq, PartWALRelHash);
    while ((entry = (PartWALRelEntry *) hash_seq_search(&seq)) != NULL)
    {
        int i;

        for (i = 0; i < n; i++)
            if (out[i] == entry->partition_id)
                break;
        if (i < n)
            continue;

        if (n >= max)
        {
            hash_seq_term(&seq);
            break;
        }
        out[n++] = entry->partition_id;
    }
    LWLockRelease(PartWALCtl->lock);

    return n;
}

/*
 * PartWALAppendCtrl — 给一个分区追加一条 CTRL 控制记录（FRD §7.7/§12）。
 *
 * 与 MARKER 的两点不同：
 *   1. orig_lsn 取**当前 WAL 插入位置**，不是本事务数据记录的最大 LSN ——
 *      控制记录之后紧跟着的是它自己触发的那批 FPI（新文件内容），那些记录的
 *      orig_lsn 必然更大。取当前插入位置才能让"CTRL 在前、FPI 在后"这个
 *      顺序同时体现在 partition_lsn 和 orig_lsn 两个维度上，段号也不会倒挂。
 *   2. 顺手把该分区登进本事务的 touched 表 —— 纯 DDL 事务可能一条 DML 都没有，
 *      不登记的话复制挂钩根本不会遍历到它，控制记录只落本地不进 Raft。
 *
 * 调用方必须**不持有** PartWALCtl->lock（本函数自己取）。
 */
void
PartWALAppendCtrl(Oid partition_id, uint8 opcode,
                  const char *payload, uint32 payload_len)
{
    XLogRecPtr    ctrl_lsn = GetXLogInsertRecPtr();
    TransactionId my_xid   = GetCurrentTransactionIdIfAny();

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);
    PG_TRY();
    {
        PartitionWALWriter *w;

        InitPartitionWALDirectory(partition_id);
        w = CreatePartitionWALWriter(partition_id, InvalidRelFileNumber);
        if (w == NULL)
            ereport(ERROR,
                    (errmsg("pg_partdist: 无法为分区 %u 打开 writer 写控制记录",
                            partition_id)));

        AppendPartWALRecord(w, ctrl_lsn,
                            PARTWAL_CTRL_RMID, opcode,
                            payload, payload_len,
                            MakeGlobalXid(PartDistLocalNodeId(), my_xid),
                            PARTWAL_FLAG_CTRL);
        DestroyPartitionWALWriter(w);       /* flush + fsync */
    }
    PG_CATCH();
    {
        LWLockRelease(PartWALCtl->lock);
        PG_RE_THROW();
    }
    PG_END_TRY();
    LWLockRelease(PartWALCtl->lock);

    PartWALNoteTouched(partition_id);

    /*
     * 就地复制一次，不等 PRE_COMMIT 的那次 flush —— 纯结构变更事务
     * （典型是 DROP INDEX）不产生任何 DATA 记录，PartWALFlush 会在
     * "本 backend 无插入"分支直接返回，控制记录就只落本地、永远进不了 Raft。
     */
    PartWALReplicateTouched();
}

/*
 * PartWALReplicateTouched — 对本事务碰过的每个分区调一次复制挂钩。
 *
 * 挂钩是区间式的（[last_data_plsn+1, get_partition_flush_lsn]），所以重复调用
 * 只会把"这次新落盘的那些记录"送走，没有重放代价。
 */
static void
PartWALReplicateTouched(void)
{
    PartWALReplicateHook fn;
    int                  i;

    if (partwal_my_ntouched <= 0)
        return;

    if (partwal_replicate_hook_rv == NULL)
        partwal_replicate_hook_rv =
            find_rendezvous_variable("partdist_partwal_replicate_hook");
    fn = (PartWALReplicateHook) *partwal_replicate_hook_rv;
    if (fn == NULL)
        return;

    for (i = 0; i < partwal_my_ntouched; i++)
        fn(partwal_my_touched[i]);
}

void
PartWALFlush(XLogRecPtr upto_lsn, bool write_marker)
{
    int                 i;
    Oid                 cache_partition[PARTWAL_WRITER_CACHE_MAX];
    PartitionWALWriter *cache_writer[PARTWAL_WRITER_CACHE_MAX];
    int                 ncached = 0;
    XLogRecPtr          last_lsn = InvalidXLogRecPtr;
    XLogRecPtr          marker_lsn;
    TransactionId       my_xid;
    char               *marker_payload = NULL;
    uint32              marker_len = 0;
    bool                drain_needed = true;

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
         * 复制挂钩取的是"当前 flush 点"，直接追加的记录已经在盘上，覆盖成立；
         * 挂钩是区间式的，空集合 / 无新字节时等价于 no-op。集合本身不在这里
         * 清（归 PartWALEndTxn / PartWALAbort），与 D1 纯结构变更路径同规则。
         */
        PartWALReplicateTouched();
        return;
    }

    /*
     * 标记记录的 orig_lsn 取本事务数据记录的最大 LSN —— 段号由 orig_lsn 换算，
     * 这样标记与它所标记的数据落在同一个段文件里。（此刻提交记录还没写进
     * pg_wal，拿不到真正的提交 LSN。）ntouched > 0 时 partwal_my_max_lsn 必然
     * 有效；退一步用 upto_lsn 兜底，避免 Invalid 把标记甩到 1 号段去。
     */
    marker_lsn = (partwal_my_max_lsn != InvalidXLogRecPtr)
                 ? partwal_my_max_lsn : upto_lsn;
    my_xid     = GetCurrentTransactionIdIfAny();

    /*
     * 载荷在加锁前组装：xactGetCommittedChildren / GetCurrentTimestamp 都不
     * 适合放在 LWLock 下，palloc 更不行。
     */
    if (write_marker && partwal_my_ntouched > 0 && TransactionIdIsValid(my_xid))
        marker_payload = PartWALBuildTxnMarker(true, &marker_len);

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);

    /*
     * Group commit: another backend already wrote our records to pg_parwal.
     * Mirrors XLogFlush()'s "already flushed" early-return check.
     *
     * 注意这里**不能直接 return**：字节虽然是别人写的，本事务的提交标记
     * 和复制触发仍然得由本 backend 负责（见 partwal_my_touched 的注释）。
     */
    if (PartWALCtl->flushed_upto != InvalidXLogRecPtr &&
        PartWALCtl->flushed_upto >= upto_lsn)
        drain_needed = false;

    /*
     * 先把 pg_wal 刷到 upto_lsn：group-commit 场景下我们会消费 peer backend
     * 的槽位，其记录字节不在本 backend 的 pending 数组里，只能按 start_lsn
     * 从 pg_wal 回读 —— 回读的前提是字节已落盘。时序不变式不受影响：
     * 这里刷的只是数据记录（[B] 是本事务提交记录的 XLogFlush，尚未发生），
     * [A] parwal fsync 仍然先于 [B]。
     */
    if (drain_needed)
        XLogFlush(upto_lsn);

    PG_TRY();
    {
        /*
         * ★ 排空顺序必须按 orig_lsn，不能按环槽下标（2026-08-08 修）。
         *
         * write_pos 每 PARTWAL_BUFFER_SLOTS 条回绕一次。一笔事务的记录跨过
         * 下标 0 时（例如占了 8190、8191、0、1 四个槽），按下标遍历会**先**
         * 排空 0、1 —— 而那两条的 orig_lsn 更大。于是更新的 WAL 记录拿到更小的
         * partition_lsn，段号又由 orig_lsn 换算，结果是"段文件名升序 ≠ plsn 升序"。
         *
         * 回放器有 qsort 兜底不受影响，但 TruncatePartWALTo / partwal_find_record /
         * GetLastWrittenPartitionLSN 三处都硬依赖那个不变式：倒置会让截断删掉
         * 本该保留的记录，留下永久空洞，replay worker 从此无限重启。
         *
         * 排序键取 (orig_lsn, start_lsn)：同一条 WAL 记录可能命中多个分区各占
         * 一个槽，orig_lsn 相同，用 start_lsn 兜底保持确定性。
         */
        int  nsorted = 0;

        for (i = 0; drain_needed && i < PARTWAL_BUFFER_SLOTS; i++)
        {
            PartWALSlot *slot = &PartWALCtl->slots[i];

            if (!slot->valid)
                continue;
            if (slot->orig_lsn > upto_lsn)
                continue;
            drain_order[nsorted++] = i;
        }
        if (nsorted > 1)
            qsort(drain_order, nsorted, sizeof(int), PartWALSlotCmp);

        for (i = 0; i < nsorted; i++)
        {
            PartWALSlot        *slot = &PartWALCtl->slots[drain_order[i]];
            PartitionWALWriter *writer = NULL;
            int                 j;
            bool                cache_it;

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

                /*
                 * gxid 在这里合成，而不是在捕获点（PartWALInsert）。
                 * 捕获点跑在 XLogInsert 内部，那里不允许碰目录，而节点号要
                 * 扫 pg_dist_local_group —— 所以捕获侧只记 32 位本地 xid，
                 * 到 flush 路径（已在事务上下文里）再补上高 16 位来源节点。
                 */
                AppendPartWALRecord(writer, slot->orig_lsn,
                                    slot->rmid, slot->info,
                                    wal_data, wal_len,
                                    MakeGlobalXid(PartDistLocalNodeId(), wal_xid),
                                    PARTWAL_FLAG_DATA);

                if (read_buf != NULL)
                    pfree(read_buf);
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
    /*
     * 记下"本事务已经有字节落盘"，并保留 partwal_my_touched 到事务真正结束。
     *
     * 本函数返回后事务仍可能失败 —— 最常见的就是下面那个复制挂钩自己 ERROR
     * （凑不齐多数派 / 本节点已不是该组 leader）。那时 DATA 记录已经在段文件
     * 里了，PartWALAbort 必须还认得出"哪些分区要补 ABORT 标记"，所以这里不能
     * 像以前那样把状态清干净。清理改由 PartWALEndTxn() 在 COMMIT/PREPARE 事件
     * 里做（见 pg_partdist.c 的 PartWALXactCallback）。
     */
    partwal_my_flushed_lsn = upto_lsn;
    partwal_my_max_lsn = InvalidXLogRecPtr;

    /*
     * [A] 已完成（本地 parwal 已 fsync）。在 [B]（提交/prepare 记录的
     * XLogFlush）之前，把本事务涉及分区的新记录复制到各自的 raft 组。
     * 锁已全部释放，网络往返不占 PartWALCtl；挂钩 ERROR 即事务中止。
     */
    PartWALReplicateTouched();

    /*
     * COMMIT 标记**在 DATA 复制成功之后**才写，因此有一条强不变式：
     *
     *     段流里出现某个 gxid 的 COMMIT 标记 ⇒ 该事务的 DATA 记录已经
     *     在多数派上持久化。
     *
     * 反过来如果先写标记再复制，上面那个挂钩一旦 ERROR（凑不齐多数派、
     * 本节点已不是 leader），事务中止时就得再补一条 ABORT 标记 ——
     * 同一个 gxid 先 COMMIT 后 ABORT，回放侧只要停在两者中间就会把一个
     * 已回滚的事务判成可见。把顺序倒过来就没有这个中间态。
     *
     * 代价是标记要重开一次 writer 再 fsync 一次；相对每条记录一次带 fsync
     * 的 Raft 往返，这点开销可以忽略。
     */
    if (marker_payload != NULL)
    {
        LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);
        PG_TRY();
        {
            int t;

            for (t = 0; t < partwal_my_ntouched; t++)
            {
                PartitionWALWriter *w;

                InitPartitionWALDirectory(partwal_my_touched[t]);
                w = CreatePartitionWALWriter(partwal_my_touched[t],
                                             InvalidRelFileNumber);
                if (w == NULL)
                    ereport(ERROR,
                            (errmsg("pg_partdist: 无法为分区 %u 打开 writer "
                                    "写提交标记", partwal_my_touched[t])));
                PartWALAppendTxnMarker(w, marker_lsn, my_xid, true,
                                       marker_payload, marker_len);
                DestroyPartitionWALWriter(w);   /* flush + fsync */
            }
        }
        PG_CATCH();
        {
            LWLockRelease(PartWALCtl->lock);
            PG_RE_THROW();
        }
        PG_END_TRY();
        LWLockRelease(PartWALCtl->lock);

        pfree(marker_payload);

        /* 再跑一次挂钩，把刚落盘的标记送出去（区间式，只会多出这一条） */
        PartWALReplicateTouched();
    }
}

/*
 * PartWALEndTxn — 事务真正结束（COMMIT / PREPARE 事件）后清掉本事务的记账。
 *
 * 与 PartWALAbort 的分工：中止路径要先补 ABORT 标记再清，所以它自己清；
 * 这里只负责成功路径。
 */
void
PartWALEndTxn(void)
{
    partwal_my_flushed_lsn = InvalidXLogRecPtr;
    partwal_my_ntouched    = 0;
}

/* ================================================================== */
/* PartWALAbort -- discard this backend's pending ring-buffer slots    */
/* ================================================================== */

void
PartWALAbort(void)
{
    int             i;
    XLogRecPtr      my_max;
    TransactionId   my_xid = GetCurrentTransactionIdIfAny();
    char           *payload = NULL;
    uint32          payload_len = 0;
    bool            already_on_disk;

    /*
     * 两条来源：还没 flush 过（partwal_my_max_lsn）、或者已经 flush 完了但
     * 事务在其之后才失败（partwal_my_flushed_lsn，典型是复制挂钩 ERROR）。
     */
    my_max = (partwal_my_max_lsn != InvalidXLogRecPtr)
             ? partwal_my_max_lsn : partwal_my_flushed_lsn;

    if (my_max == InvalidXLogRecPtr)
    {
        PartWALEndTxn();
        return;  /* no inserts from this backend */
    }

    if (PartWALCtl == NULL)
    {
        partwal_my_max_lsn = InvalidXLogRecPtr;
        PartWALEndTxn();
        return;
    }

    /* 载荷在加锁前 palloc —— 与 PartWALFlush 同理 */
    if (partwal_my_ntouched > 0 && TransactionIdIsValid(my_xid))
        payload = PartWALBuildTxnMarker(false, &payload_len);

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);

    /*
     * 本事务的字节是否已经落进段文件？两种情况会：
     *   1) group commit —— peer backend 的 PartWALFlush 顺手写掉了我们的槽位；
     *   2) 本事务已过 PRE_COMMIT（parwal 写完），之后才失败中止。
     * 两种情况下丢弃槽位都为时已晚：follower 已经/即将拿到这些 DATA 记录。
     */
    already_on_disk = (PartWALCtl->flushed_upto != InvalidXLogRecPtr &&
                       PartWALCtl->flushed_upto >= my_max);

    for (i = 0; i < PARTWAL_BUFFER_SLOTS; i++)
    {
        PartWALSlot *slot = &PartWALCtl->slots[i];
        if (slot->valid && slot->backend_id == MyBackendId)
            slot->valid = false;
    }

    /*
     * 补一条 ABORT 标记（commit_ts = 0）。没有它，follower 上这批 DATA 记录
     * 的 gxid 在增强型 CLOG 里始终是"无状态"，和"尚未提交"分不开 ——
     * 空间无法回收，可见性判断也没有终态。
     *
     * 整段包在 PG_TRY 里并降级为 WARNING：这里已经在事务中止路径上，
     * 再抛 ERROR 会升级成 FATAL。写不下去时 follower 侧只是继续把这批 xid
     * 当作未决（不可见，语义安全），下次该分区有写入时标记会被补上复制。
     */
    if (already_on_disk && payload != NULL)
    {
        PG_TRY();
        {
            int     t;

            for (t = 0; t < partwal_my_ntouched; t++)
            {
                PartitionWALWriter *w;

                InitPartitionWALDirectory(partwal_my_touched[t]);
                w = CreatePartitionWALWriter(partwal_my_touched[t],
                                             InvalidRelFileNumber);
                if (w == NULL)
                    continue;
                PartWALAppendTxnMarker(w, my_max, my_xid, false,
                                       payload, payload_len);
                DestroyPartitionWALWriter(w);   /* flush + fsync */
            }
        }
        PG_CATCH();
        {
            ErrorData *ed;

            MemoryContextSwitchTo(TopMemoryContext);
            ed = CopyErrorData();
            FlushErrorState();
            ereport(WARNING,
                    (errmsg("pg_partdist: 中止标记落盘失败：%s", ed->message),
                     errdetail("该事务的 xid 在 follower 侧将保持未决状态，"
                               "直到同分区的下一次写入把标记带过去。")));
            FreeErrorData(ed);
        }
        PG_END_TRY();
    }

    LWLockRelease(PartWALCtl->lock);
    if (payload != NULL)
        pfree(payload);
    FreePartWALPendingContent();
    partwal_my_max_lsn = InvalidXLogRecPtr;
    PartWALEndTxn();
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
                                    raw,
                                    record->xl_tot_len,
                                    MakeGlobalXid(PartDistLocalNodeId(),
                                                  XLogRecGetXid(reader)),
                                    PARTWAL_FLAG_DATA);
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
