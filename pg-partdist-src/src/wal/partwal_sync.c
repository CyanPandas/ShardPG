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

#include "access/heapam_xlog.h"
#include "access/rmgr.h"
#include "access/xact.h"
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

#define PARTWAL_FLUSH_TOUCHED_MAX 64

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
    PartWALRelEntry *entry;
    bool             found;
    PartWALSlot     *slot;
    bool             wrote_slot = false;

    if (PartWALCtl == NULL || PartWALRelHash == NULL)
        return;

    /* Skip speculative-insert confirmation records */
    if (rmid == RM_HEAP_ID &&
        (info & ~XLR_INFO_MASK) == XLOG_HEAP_CONFIRM)
        return;

    LWLockAcquire(PartWALCtl->lock, LW_EXCLUSIVE);

    for (i = 0; i < nblocks; i++)
    {
        if (blocks[i].forkno != MAIN_FORKNUM)
            continue;

        entry = (PartWALRelEntry *)
            hash_search(PartWALRelHash, &blocks[i].rlocator.relNumber,
                        HASH_FIND, &found);
        if (!found)
            continue;

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
        slot->relfilenode  = blocks[i].rlocator.relNumber;
        slot->orig_lsn     = end_lsn;
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
        break;
    }

    LWLockRelease(PartWALCtl->lock);

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
    Oid                 touched[PARTWAL_FLUSH_TOUCHED_MAX];
    int                 n_touched = 0;

    /* Default: flush up to the per-backend tracked max LSN */
    if (upto_lsn == InvalidXLogRecPtr)
        upto_lsn = partwal_my_max_lsn;

    if (upto_lsn == InvalidXLogRecPtr)
    {
        partwal_my_max_lsn = InvalidXLogRecPtr;
        return;  /* no inserts from this backend */
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
        return;
    }

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
             * Only available for this backend's own slots; group-commit
             * slots (backend_id != MyBackendId) are written with data_len=0.
             */
            {
                const char    *wal_data = NULL;
                uint32         wal_len  = 0;
                TransactionId  wal_xid  = slot->xid;

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

                AppendPartWALRecord(writer, slot->orig_lsn,
                                    slot->rmid, slot->info,
                                    wal_data, wal_len, wal_xid);

                /* 只登记本 backend（= 本事务）写入的分区，供末尾的复制挂钩用 */
                if (slot->backend_id == MyBackendId)
                {
                    int t;

                    for (t = 0; t < n_touched; t++)
                        if (touched[t] == slot->partition_id)
                            break;
                    if (t == n_touched && n_touched < PARTWAL_FLUSH_TOUCHED_MAX)
                        touched[n_touched++] = slot->partition_id;
                }
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
    if (n_touched > 0)
    {
        PartWALReplicateHook fn;

        if (partwal_replicate_hook_rv == NULL)
            partwal_replicate_hook_rv =
                find_rendezvous_variable("partdist_partwal_replicate_hook");
        fn = (PartWALReplicateHook) *partwal_replicate_hook_rv;
        if (fn != NULL)
        {
            for (i = 0; i < n_touched; i++)
                fn(touched[i]);
        }
    }
}

/* ================================================================== */
/* PartWALAbort -- discard this backend's pending ring-buffer slots    */
/* ================================================================== */

void
PartWALAbort(void)
{
    int i;

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
    return (count < 0) ? -1 : (int) count;
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

static bool
RecordTouchesRelfilenode(XLogReaderState *reader, RelFileNumber target_rfn)
{
    int blk;
    int max_blk;

    if (!XLogRecHasAnyBlockRefs(reader))
        return false;

    max_blk = XLogRecMaxBlockId(reader);

    for (blk = 0; blk <= max_blk; blk++)
    {
        RelFileLocator rlocator;
        ForkNumber     fork;
        BlockNumber    blkno;

        if (XLogRecGetBlockTagExtended(reader, (uint8) blk, &rlocator, &fork,
                                        &blkno, NULL))
        {
            if (rlocator.relNumber == target_rfn)
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

        if (!XLogRecHasAnyBlockRefs(reader))
            continue;

        if (!RecordTouchesRelfilenode(reader, writer->relfilenode))
            continue;

        rmid = XLogRecGetRmid(reader);

        /* Skip speculative-insert confirmation records */
        if (rmid == RM_HEAP_ID &&
            (XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == XLOG_HEAP_CONFIRM)
            continue;

        /* Skip records already written (dedup guard) */
        if (reader->ReadRecPtr <= writer->last_wal_lsn)
            continue;

        PG_TRY();
        {
            AppendPartWALRecord(writer,
                                reader->ReadRecPtr,
                                rmid,
                                XLogRecGetInfo(reader),
                                reader->readRecordBuf,
                                record->xl_tot_len,
                                XLogRecGetXid(reader));
            records_written++;
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
