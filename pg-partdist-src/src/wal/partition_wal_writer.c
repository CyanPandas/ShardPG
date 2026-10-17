/*
 * partition_wal_writer.c
 * Buffered writer for pg_parwal/<partition_id>/ segment files (parwal-2.0).
 *
 * Each record written to a segment file consists of:
 *   PartWALRecord header (fixed size)
 *   followed by data_len bytes of raw XLogRecord content
 *
 * The checkpoint file at pg_parwal/<partition_id>/checkpoint tracks:
 *   - relfilenode of the partition (for crash recovery without DB access)
 *   - last_wal_lsn: the last orig_lsn written
 *   - last_part_lsn: the last partition_lsn written
 *
 * Segment files use the same naming convention as PostgreSQL's native pg_wal
 * (XLogFileName with timeline=1), keyed on the orig_lsn of the record.
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "partwal_sync.h"		/* 快速模式 checkpoint 缓存在 PartWALCtl 里 */
#include "partition_wal_writer.h"
#include "partition_wal.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "partwal_recent.h"

/* ================================================================== */
/* P7-N41 group commit：批内延迟 fsync                                  */
/* ================================================================== */

/*
 * follower 收一批 AppendEntries（pg_raft_append_entries_batch）时，每条记录各走一次
 * partwal_follower_append：原来每条都"写字节 + fsync 段文件 + 写 checkpoint + fsync"。
 * 批内打开延迟模式后：
 *   · 字节照写（进 OS 缓存），段文件 fsync 与 checkpoint 落盘推迟；
 *   · 本 backend 内"checkpoint 现在是多少"由这张表回答（ReadPartWALCheckpoint 先查它），
 *     下一条记录的写入器据此播种，编号照样连续；
 *   · PartWALDeferSyncEnd：每个分区把碰过的段文件各 fsync 一次，再把 checkpoint 持久化。
 * 盘上的 checkpoint 只在字节 fsync 之后才前移 —— 崩在批中间，盘上状态与"这批没收到"一致
 * （尾部多出的未 ack 字节与原逐条路径崩在 write 与 fsync 之间是同一种情形）。
 * 只在 backend 本地；事务结束（提交或中止）一律清空，见 PartWALDeferSyncReset。
 */
#define PARTWAL_DEFER_MAX_PARTS 16
#define PARTWAL_DEFER_MAX_SEGS  8

typedef struct PartWALDeferEnt
{
    Oid             partition_id;
    RelFileNumber   relfilenode;
    XLogRecPtr      last_wal_lsn;
    uint64          last_part_lsn;
    int             nsegs;
    XLogSegNo       segs[PARTWAL_DEFER_MAX_SEGS];
} PartWALDeferEnt;

bool PartWALDeferSync = false;
static PartWALDeferEnt defer_ents[PARTWAL_DEFER_MAX_PARTS];
static int  n_defer_ents = 0;

static PartWALDeferEnt *
defer_find(Oid partition_id)
{
    int i;

    for (i = 0; i < n_defer_ents; i++)
        if (defer_ents[i].partition_id == partition_id)
            return &defer_ents[i];
    return NULL;
}

/* 记下一次被推迟的 fsync；表满 / 段太多返回 false（调用方当场 fsync） */
static bool
defer_note(PartitionWALWriter *w)
{
    PartWALDeferEnt *e = defer_find(w->partition_id);
    int              i;

    if (e == NULL)
    {
        if (n_defer_ents >= PARTWAL_DEFER_MAX_PARTS)
            return false;
        e = &defer_ents[n_defer_ents++];
        memset(e, 0, sizeof(*e));
        e->partition_id = w->partition_id;
    }
    for (i = 0; i < e->nsegs; i++)
        if (e->segs[i] == w->current_segno)
            break;
    if (i == e->nsegs)
    {
        if (e->nsegs >= PARTWAL_DEFER_MAX_SEGS)
            return false;
        e->segs[e->nsegs++] = w->current_segno;
    }
    e->relfilenode = w->relfilenode;
    e->last_wal_lsn = w->last_wal_lsn;
    e->last_part_lsn = w->last_partition_lsn;
    return true;
}

static void
defer_sync_ent(PartWALDeferEnt *e)
{
    int i;

    for (i = 0; i < e->nsegs; i++)
    {
        char   *rel = GetPartitionWALPath(e->partition_id, e->segs[i]);
        char    abspath[MAXPGPATH];
        int     fd;

        snprintf(abspath, MAXPGPATH, "%s/%s", DataDir, rel);
        pfree(rel);
        fd = open(abspath, O_WRONLY, 0);
        if (fd < 0)
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: 批量落盘：打不开段文件 \"%s\": %m", abspath)));
        if (!partwal_fast_commit && pg_fsync(fd) != 0)
        {
            close(fd);
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: 批量落盘：fsync \"%s\" 失败: %m", abspath)));
        }
        close(fd);
    }
    WritePartWALCheckpoint(e->partition_id, e->relfilenode,
                           e->last_wal_lsn, e->last_part_lsn);
}

void
PartWALDeferSyncBegin(void)
{
    n_defer_ents = 0;
    PartWALDeferSync = true;
}

/* 返回 fsync 过的分区数；任何 fsync 失败 ERROR */
int
PartWALDeferSyncEnd(void)
{
    int i;
    int n = n_defer_ents;

    PartWALDeferSync = false;
    for (i = 0; i < n_defer_ents; i++)
        defer_sync_ent(&defer_ents[i]);
    n_defer_ents = 0;
    return n;
}

/* 某分区要被别的路径（截断）直接改盘上状态之前，先把它的推迟部分落实 */
void
PartWALDeferSyncPartition(Oid partition_id)
{
    PartWALDeferEnt *e;

    if (n_defer_ents == 0 || (e = defer_find(partition_id)) == NULL)
        return;
    defer_sync_ent(e);
    *e = defer_ents[--n_defer_ents];
}

/* 事务结束（提交 / 中止）：丢弃延迟状态（未 ack，盘上 checkpoint 未前移，无害） */
void
PartWALDeferSyncReset(void)
{
    PartWALDeferSync = false;
    n_defer_ents = 0;
}

/* ================================================================== */
/* Checkpoint file I/O                                                  */
/* ================================================================== */

/* ---- checkpoint 节点级缓存（单副本快速模式 / 多副本组提交模式，见 partwal_sync.h） ---- */

/*
 * 多副本组提交（2026-10-16）：pg_partdist.partwal_group_fsync。快速模式下本来就不 fsync，
 * 这里只管多副本（非快速）模式。
 */
bool partwal_group_fsync = true;

bool
PartWALLazySync(void)
{
    return partwal_group_fsync && !partwal_fast_commit;
}

/* checkpoint 以节点级缓存为准的模式（文件只是重启时的提示） */
static inline bool
ckpt_cached_mode(void)
{
    return partwal_fast_commit || PartWALLazySync();
}

/* 调用方持 ckpt_mutex；返回下标，没有返回 -1 */
static int
ckpt_cache_find_locked(Oid partition_id)
{
    int i;

    for (i = 0; i < PartWALCtl->ckpt_n; i++)
        if (PartWALCtl->ckpt[i].partition_id == partition_id)
            return i;
    return -1;
}

/* 2026-10-17：分区编号回退（截断）代数，供顺序读游标判作废；未登记的分区返回 0 */
uint32
PartWALTruncGen(Oid partition_id)
{
    uint32  g = 0;
    int     i;

    if (PartWALCtl == NULL)
        return 0;
    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i >= 0)
        g = PartWALCtl->ckpt[i].trunc_gen;
    SpinLockRelease(&PartWALCtl->ckpt_mutex);
    return g;
}

static bool
ckpt_cache_get(Oid partition_id, PartWALCheckpointFile *out)
{
    int i;
    bool hit = false;

    if (PartWALCtl == NULL)
        return false;
    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i >= 0)
    {
        out->magic = PARTWAL_CHECKPOINT_MAGIC;
        out->relfilenode = PartWALCtl->ckpt[i].relfilenode;
        out->last_wal_lsn = PartWALCtl->ckpt[i].last_wal_lsn;
        out->last_part_lsn = PartWALCtl->ckpt[i].last_part_lsn;
        hit = true;
    }
    SpinLockRelease(&PartWALCtl->ckpt_mutex);
    return hit;
}

/*
 * "已持久"的写入：调用方刚 fsync 过（或这是重启后从盘上重建的值）。
 * 返回是否已存进缓存（满了返回 false，调用方须退回写文件）。
 *
 * synced_part_lsn 只在"没有组提交欠账"时跟着前移 —— 调用方 fsync 的只是它自己那一段，
 * 别的后端 write() 了、还没 fsync 的字节可能落在另一个段里。编号回退（截断）时
 * synced 一并回退。
 */
static bool
ckpt_cache_put(Oid partition_id, RelFileNumber relfilenode,
               XLogRecPtr last_wal_lsn, uint64 last_part_lsn)
{
    int i;
    bool ok = false;

    if (PartWALCtl == NULL)
        return false;
    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i < 0 && PartWALCtl->ckpt_n < PARTWAL_CKPT_CACHE_MAX)
    {
        i = PartWALCtl->ckpt_n++;
        memset(&PartWALCtl->ckpt[i], 0, sizeof(PartWALCkptCacheEnt));
        PartWALCtl->ckpt[i].partition_id = partition_id;
        PartWALCtl->ckpt[i].synced_part_lsn = last_part_lsn;
    }
    if (i >= 0)
    {
        PartWALCkptCacheEnt *e = &PartWALCtl->ckpt[i];

        if (last_part_lsn < e->last_part_lsn)
            e->trunc_gen++;
        e->relfilenode = relfilenode;
        e->last_wal_lsn = last_wal_lsn;
        e->last_part_lsn = last_part_lsn;
        /*
         * 欠账为 0 且没有组提交者正拿着欠账在 fsync ⇒ 到 last 为止都已落盘。后一个条件是
         * 2026-10-16 审查补的：组提交者接手欠账时把 lo/hi 清零，fsync 做完前这里若只看
         * lo == 0，会把 synced 推过它还没落盘的字节。
         */
        if ((e->lo_unsynced_segno == 0 && e->sync_inflight == 0) ||
            last_part_lsn < e->synced_part_lsn)
        {
            e->synced_part_lsn = last_part_lsn;
            e->lo_unsynced_segno = 0;
            e->hi_segno = 0;
        }
        ok = true;
    }
    SpinLockRelease(&PartWALCtl->ckpt_mutex);
    return ok;
}

/*
 * 只 write()、尚未 fsync 的写入（组提交模式）：推进编号，记下段欠账。
 * 调用方持 PartWALCtl->lock（编号分配的互斥靠它）。满了返回 false。
 */
static bool
ckpt_cache_note_written(Oid partition_id, RelFileNumber relfilenode,
                        XLogRecPtr last_wal_lsn, uint64 last_part_lsn,
                        XLogSegNo lo_seg, XLogSegNo hi_seg)
{
    int i;
    bool ok = false;

    if (PartWALCtl == NULL)
        return false;
    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i >= 0)
    {
        PartWALCkptCacheEnt *e = &PartWALCtl->ckpt[i];

        e->relfilenode = relfilenode;
        e->last_wal_lsn = last_wal_lsn;
        e->last_part_lsn = last_part_lsn;
        if (lo_seg != 0)
        {
            if (e->lo_unsynced_segno == 0 || lo_seg < e->lo_unsynced_segno)
                e->lo_unsynced_segno = lo_seg;
            if (hi_seg > e->hi_segno)
                e->hi_segno = hi_seg;
        }
        ok = true;
    }
    SpinLockRelease(&PartWALCtl->ckpt_mutex);
    return ok;
}

uint64
PartWALCachedLastPLSN(Oid partition_id)
{
    PartWALCheckpointFile chk;

    if (ckpt_cache_get(partition_id, &chk))
        return chk.last_part_lsn;
    return 0;
}

static void partwal_scan_tail(Oid partition_id, uint64 *max_plsn_out,
                              XLogRecPtr *wal_of_max_out, bool fsync_files);
static uint64 partwal_scan_max_plsn(Oid partition_id);

/* 把一组值写进 checkpoint 文件（tmp + rename）；do_fsync=false 时只作重启提示 */
static void
write_ckpt_file(Oid partition_id, RelFileNumber relfilenode,
                XLogRecPtr last_wal_lsn, uint64 last_part_lsn, bool do_fsync)
{
    char                  dirpath[MAXPGPATH];
    char                  path[MAXPGPATH];
    char                  tmp[MAXPGPATH];
    int                   fd;
    PartWALCheckpointFile chk;
    ssize_t               nb;

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);
    snprintf(path, MAXPGPATH, "%s/%s", dirpath, PARTWAL_CHECKPOINT_FILENAME);
    /* 临时文件名按进程区分：组提交模式下写提示文件不持 PartWALCtl->lock */
    snprintf(tmp,  MAXPGPATH, "%s.tmp.%d", path, (int) MyProcPid);

    chk.magic          = PARTWAL_CHECKPOINT_MAGIC;
    chk.relfilenode    = relfilenode;
    chk.last_wal_lsn   = last_wal_lsn;
    chk.last_part_lsn  = last_part_lsn;

    fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0)
    {
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: could not open checkpoint temp file "
                        "\"%s\": %m", tmp)));
        return;
    }

    do {
        nb = write(fd, &chk, sizeof(chk));
    } while (nb < 0 && errno == EINTR);

    if (do_fsync && pg_fsync(fd) != 0)
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: fsync failed for checkpoint \"%s\": %m",
                        tmp)));

    close(fd);

    if (nb == (ssize_t) sizeof(chk))
    {
        if (rename(tmp, path) != 0)
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: could not rename checkpoint "
                            "\"%s\" to \"%s\": %m", tmp, path)));
    }
    else
    {
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: short write to checkpoint \"%s\"",
                        tmp)));
        (void) unlink(tmp);
    }
}

void
WritePartWALCheckpoint(Oid partition_id,
                       RelFileNumber relfilenode,
                       XLogRecPtr last_wal_lsn,
                       uint64 last_part_lsn)
{
    /*
     * 快速模式 / 多副本组提交模式：checkpoint 只进节点级缓存，不写文件 —— 原先每个分区每轮
     * flush 都要"开临时文件 + 写 + fsync + rename"，全在 PartWALCtl->lock 之下。
     * 缓存满时退回写文件。
     */
    if (ckpt_cached_mode() &&
        ckpt_cache_put(partition_id, relfilenode, last_wal_lsn, last_part_lsn))
        return;
    if (!ckpt_cached_mode())
        (void) ckpt_cache_put(partition_id, relfilenode, last_wal_lsn, last_part_lsn);

    write_ckpt_file(partition_id, relfilenode, last_wal_lsn, last_part_lsn,
                    !partwal_fast_commit);
}

/* 重启后第一次取用：缓存里没有才填（不许覆盖并发写者已推进的值，否则编号回退、重发） */
static void
ckpt_cache_populate(Oid partition_id, const PartWALCheckpointFile *chk)
{
    int i;

    if (PartWALCtl == NULL)
        return;
    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i < 0 && PartWALCtl->ckpt_n < PARTWAL_CKPT_CACHE_MAX)
    {
        PartWALCkptCacheEnt *e = &PartWALCtl->ckpt[PartWALCtl->ckpt_n++];

        memset(e, 0, sizeof(*e));
        e->partition_id = partition_id;
        e->relfilenode = chk->relfilenode;
        e->last_wal_lsn = chk->last_wal_lsn;
        e->last_part_lsn = chk->last_part_lsn;
        e->synced_part_lsn = chk->last_part_lsn;
    }
    SpinLockRelease(&PartWALCtl->ckpt_mutex);
}

bool
ReadPartWALCheckpoint(Oid partition_id, PartWALCheckpointFile *out)
{
    char    path[MAXPGPATH];
    int     fd;
    ssize_t nb;

    /* P7-N41：批内延迟模式下，本 backend 看到的是尚未落盘的最新 checkpoint */
    if (n_defer_ents > 0)
    {
        int i;

        for (i = 0; i < n_defer_ents; i++)
            if (defer_ents[i].partition_id == partition_id)
            {
                out->magic = PARTWAL_CHECKPOINT_MAGIC;
                out->relfilenode = defer_ents[i].relfilenode;
                out->last_wal_lsn = defer_ents[i].last_wal_lsn;
                out->last_part_lsn = defer_ents[i].last_part_lsn;
                return true;
            }
    }

    if (ckpt_cached_mode() && ckpt_cache_get(partition_id, out))
        return true;

    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, partition_id,
             PARTWAL_CHECKPOINT_FILENAME);

    {
        bool    file_ok = false;

        fd = open(path, O_RDONLY, 0);
        if (fd >= 0)
        {
            nb = read(fd, out, sizeof(*out));
            close(fd);
            file_ok = (nb == (ssize_t) sizeof(*out) &&
                       out->magic == PARTWAL_CHECKPOINT_MAGIC);
        }

        if (ckpt_cached_mode())
        {
            /*
             * 缓存未命中（重启后第一次取用该分区）：文件可能停在缓存模式开启前（或只是不 fsync 的
             * 提示），段里实际写到的编号以扫描为准，二者取大 —— 不能重发 partition_lsn。
             * last_wal_lsn 同理取扫描到的末条记录（demux 崩溃恢复按它决定从哪里重扫 pg_wal，
             * 陈旧值会把已在流里的记录再追加一遍）。
             *
             * 组提交模式下顺手把各段 fsync 一遍：PostgreSQL 进程崩溃后页缓存里可能还躺着
             * 没 fsync 的尾巴，扫到了就当已持久（复制挂钩会把它们送出去、leader 计自己一票）。
             */
            uint64      scanned = 0;
            XLogRecPtr  scanned_wal = InvalidXLogRecPtr;

            partwal_scan_tail(partition_id, &scanned, &scanned_wal, PartWALLazySync());

            if (!file_ok)
            {
                if (scanned == 0)
                    return false;
                memset(out, 0, sizeof(*out));
                out->magic = PARTWAL_CHECKPOINT_MAGIC;
            }
            /*
             * 段里有记录就以扫描为准，**不与文件取大**（2026-10-16 审查）：缓存模式下文件只是
             * 不 fsync、按秒节流的提示，截断（编号回退）后它可能仍停在截断前更大的值，取大会
             * 让重启后的编号跳过一段 ⇒ 流内空洞、复制读不到记录。段全空时才信文件。
             */
            if (scanned > 0)
            {
                out->last_part_lsn = scanned;
                if (scanned_wal != InvalidXLogRecPtr)
                    out->last_wal_lsn = scanned_wal;
            }
            ckpt_cache_populate(partition_id, out);
            /* 并发者可能已经先填过（并已推进）：以缓存为准 */
            (void) ckpt_cache_get(partition_id, out);
            return true;
        }
        return file_ok;
    }
}


/* ================================================================== */
/* Segment file helpers                                                 */
/* ================================================================== */

/*
 * OpenWriterSegment — open (or create) the segment file for segno.
 * Closes any previously open fd first.
 * On open, seeks to the end and truncates any partial record tail.
 */
static void
OpenWriterSegment(PartitionWALWriter *writer, XLogSegNo segno)
{
    char *rel;
    char  relpath[MAXPGPATH];
    char  abspath[MAXPGPATH];
    int   fd;

    if (writer->fd >= 0)
    {
        /* P7-N41：批内延迟模式下换段，旧段的字节当场 fsync（延迟表只记当前段） */
        if (PartWALDeferSync && pg_fsync(writer->fd) != 0)
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: fsync failed for partition %u WAL: %m",
                            writer->partition_id)));
        close(writer->fd);
        writer->fd = -1;
    }

    rel = GetPartitionWALPath(writer->partition_id, segno);
    strlcpy(relpath, rel, MAXPGPATH);
    pfree(rel);

    snprintf(abspath, MAXPGPATH, "%s/%s", DataDir, relpath);

    /* O_APPEND for safe concurrent operation */
    fd = open(abspath, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: could not open partition WAL segment "
                        "\"%s\": %m", abspath)));

    /*
     * In parwal-2.0 records are variable-length (header + data_len bytes).
     * We cannot truncate to a fixed-size boundary like the old code did.
     * The file is simply opened at its current end (O_APPEND does this).
     * Partial records at the tail of a crashed file are skipped by the
     * reader because they will have invalid magic or truncated data.
     */

    writer->fd            = fd;
    writer->current_segno = segno;
    if (writer->first_segno == 0 || segno < writer->first_segno)
        writer->first_segno = segno;
    writer->enospc_stalled = false;
}

/* ================================================================== */
/* Public API                                                           */
/* ================================================================== */

/*
 * GetLastWrittenPartitionLSN — scan pg_parwal/<partition_id>/ segment files
 * and return the highest partition_lsn found (0 if none).
 *
 * We read PartWALRecord headers sequentially, skipping data payloads.
 * To avoid O(records) cost, we first try the checkpoint file.
 */
uint64
GetLastWrittenPartitionLSN(Oid partition_id)
{
    PartWALCheckpointFile chk;

    /* Fast path: checkpoint file has the answer */
    if (ReadPartWALCheckpoint(partition_id, &chk))
        return chk.last_part_lsn;

    /*
     * Slow path: no checkpoint file (first run or after reset).
     * Scan all segment files.
     */
    return partwal_scan_max_plsn(partition_id);
}

/*
 * 扫段求本分区流里最大的 partition_lsn，以及编号最大的那条非零 orig_lsn（= 写入器会记进
 * checkpoint 的 last_wal_lsn）。**全部段都扫** —— 段名序不等于编号序（切主后新旧主两套
 * WAL 坐标并存，见 TruncatePartWALTo 的 P7-N1 之二），只扫末段会漏。
 *
 * 2026-10-16：按 1 MB 块 pread 解析记录头（原来每条记录一次 read + 一次 lseek，
 * 40 万条记录的流要近百万次系统调用；重启后每个分区第一次写入都要付一次，且在
 * PartWALCtl->lock 之下）。残尾（magic 不对）视为该段结束。
 */
#define PARTWAL_SCAN_BUF (1024 * 1024)

static void
partwal_scan_tail(Oid partition_id, uint64 *max_plsn_out,
                  XLogRecPtr *wal_of_max_out, bool fsync_files)
{
    char            dirpath[MAXPGPATH];
    PartWALSegName *segfiles;
    int             nfiles = 0;
    int             i;
    uint64          max_lsn = 0;
    uint64          wal_plsn = 0;
    XLogRecPtr      wal_of_max = InvalidXLogRecPtr;
    char           *buf;

    *max_plsn_out = 0;
    *wal_of_max_out = InvalidXLogRecPtr;

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    /* 2026-10-12：全部段、升序（原来最多 256 段，之后的段被漏掉） */
    segfiles = PartWALListSegments(partition_id, &nfiles);
    if (nfiles == 0)
        return;

    buf = palloc(PARTWAL_SCAN_BUF);
    for (i = 0; i < nfiles; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        off_t         off = 0;
        off_t         buf_off = 0;
        ssize_t       buf_len = 0;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY, 0);
        if (fd < 0)
            continue;

        for (;;)
        {
            PartWALRecord rec;

            if (off < buf_off || off + (off_t) sizeof(PartWALRecord) > buf_off + buf_len)
            {
                do {
                    buf_len = pread(fd, buf, PARTWAL_SCAN_BUF, off);
                } while (buf_len < 0 && errno == EINTR);
                buf_off = off;
                if (buf_len < (ssize_t) sizeof(PartWALRecord))
                    break;
            }
            memcpy(&rec, buf + (off - buf_off), sizeof(PartWALRecord));
            if (rec.magic != PARTWAL_MAGIC)
                break;                  /* 残尾：本段到此为止 */
            if (rec.partition_id == partition_id)
            {
                if (rec.partition_lsn > max_lsn)
                    max_lsn = rec.partition_lsn;
                if (rec.orig_lsn != InvalidXLogRecPtr && rec.partition_lsn >= wal_plsn)
                {
                    wal_plsn = rec.partition_lsn;
                    wal_of_max = rec.orig_lsn;
                }
            }
            off += (off_t) sizeof(PartWALRecord) + (off_t) rec.data_len;
        }

        if (fsync_files && pg_fsync(fd) != 0)
        {
            close(fd);
            pfree(buf);
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: 重建 checkpoint 缓存时 fsync \"%s\" 失败: %m",
                            filepath)));
        }
        close(fd);
    }
    pfree(buf);

    *max_plsn_out = max_lsn;
    *wal_of_max_out = wal_of_max;
}

static uint64
partwal_scan_max_plsn(Oid partition_id)
{
    uint64      plsn;
    XLogRecPtr  wal;

    partwal_scan_tail(partition_id, &plsn, &wal, false);
    return plsn;
}

/*
 * CreatePartitionWALWriter — allocate a writer; reads checkpoint for state.
 */
PartitionWALWriter *
CreatePartitionWALWriter(Oid partition_id, RelFileNumber relfilenode)
{
    PartitionWALWriter    *w;
    PartWALCheckpointFile  chk;

    /* Ensure the directory exists */
    InitPartitionWALDirectory(partition_id);

    /* 不清零 256 KB 缓冲区（每条 follower 记录、每次 flush 都要建一个写入器）；字段逐个赋值 */
    w = (PartitionWALWriter *) MemoryContextAlloc(TopMemoryContext,
                                                   sizeof(PartitionWALWriter));
    w->partition_id    = partition_id;
    w->first_segno     = 0;
    w->last_partition_lsn = 0;
    w->last_wal_lsn    = InvalidXLogRecPtr;
    w->relfilenode     = relfilenode;
    w->fd              = -1;
    w->current_segno   = 0;
    w->buf_used        = 0;
    w->enospc_stalled  = false;
    w->last_stall_time = 0;

    if (ReadPartWALCheckpoint(partition_id, &chk))
    {
        w->last_partition_lsn = chk.last_part_lsn;
        w->last_wal_lsn       = chk.last_wal_lsn;
        /* Trust checkpoint relfilenode for crash recovery */
        if (!RelFileNumberIsValid(relfilenode) && RelFileNumberIsValid(chk.relfilenode))
            w->relfilenode = chk.relfilenode;
    }
    else
    {
        w->last_partition_lsn = GetLastWrittenPartitionLSN(partition_id);
        w->last_wal_lsn       = InvalidXLogRecPtr;
    }

    return w;
}

/*
 * FlushPartitionWALWriter — write all buffered bytes to the current segment.
 * If with_fsync, also fsync and update the checkpoint file.
 */
bool partwal_fast_commit = false;

void
FlushPartitionWALWriter(PartitionWALWriter *writer, bool with_fsync)
{
    ssize_t written;
    int     remaining;
    char   *ptr;

    if (writer->fd < 0)
        return;

    /*
     * 注意：这里**不能**在 buf_used == 0 时提前返回。超过缓冲区大小的记录走
     * AppendPartWALRecord 的直写分支，写完就 return，buf_used 仍是 0；若此处
     * 提前返回，那条记录既不会 fsync 也不会更新 checkpoint —— 陈旧的
     * checkpoint 会让下一个写入器重新发放同一个 partition_lsn。
     * remaining == 0 时下面的 while 循环自然不执行，fsync/checkpoint 照常进行。
     */
    ptr       = writer->buffer;
    remaining = writer->buf_used;

    while (remaining > 0)
    {
        ssize_t this_written;

        do {
            this_written = write(writer->fd, ptr, remaining);
        } while (this_written < 0 && errno == EINTR);

        if (this_written < 0)
        {
            if (errno == ENOSPC)
            {
                close(writer->fd);
                writer->fd = -1;
                ereport(ERROR,
                        (errcode(ERRCODE_DISK_FULL),
                         errmsg("pg_partdist: disk full writing partition WAL "
                                "for partition %u", writer->partition_id)));
            }
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: could not write partition WAL: %m")));
        }

        written    = this_written;
        ptr       += written;
        remaining -= (int) written;
    }

    /* 已 write() 进段文件的记录登记进节点级缓存（按 plsn 回读的快路径） */
    if (writer->buf_used > 0)
        PartWALRecentInsertBuffer(writer->partition_id, writer->buffer, writer->buf_used);

    /* P7-N41：批内延迟 —— 记账后由 PartWALDeferSyncEnd 统一 fsync + 落 checkpoint */
    if (with_fsync && PartWALDeferSync && defer_note(writer))
        with_fsync = false;

    if (with_fsync)
    {
        /*
         * fsync 失败必须是 ERROR 而非 WARNING：数据面 Raft 的 follower 依赖
         * "落盘成功才 ack" 来保证「多数派提交 == 多数派已持久化」。若这里只
         * 告警，一次 EIO 仍会产生 ack，多数派提交就不再蕴含持久化。
         */
        if (!partwal_fast_commit && pg_fsync(writer->fd) != 0)
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: fsync failed for partition %u WAL: %m",
                            writer->partition_id)));

        /* Update checkpoint after a durable flush */
        WritePartWALCheckpoint(writer->partition_id,
                               writer->relfilenode,
                               writer->last_wal_lsn,
                               writer->last_partition_lsn);
    }

    writer->buf_used = 0;
}

/*
 * AppendPartWALRecord — buffer one PartWALRecord (header + raw_data bytes).
 *
 * Switches segment file when orig_lsn crosses a WAL segment boundary.
 * Flushes buffer automatically when it is full.
 * Increments last_partition_lsn (it starts from wherever the writer was
 * initialised, so new records get consecutive partition_lsn values).
 */
void
AppendPartWALRecord(PartitionWALWriter *writer,
                    XLogRecPtr orig_lsn,
                    uint8 rmid,
                    uint8 info,
                    const char *raw_data,
                    uint32 data_len,
                    GlobalTransactionId gxid,
                    uint8 flags)
{
    /* expected == 0：本地自增分配（leader / demux 侧的原有语义）。 */
    (void) AppendPartWALRecordAt(writer, 0, orig_lsn, rmid, info,
                                 raw_data, data_len, gxid, flags);
}

/*
 * AppendPartWALRecordAt — 按**指定的** partition_lsn 落一条记录。
 *
 * expected == 0        : 本地自增分配（leader 侧原语义）。
 * expected == last + 1 : 正常追加，记录被打上 expected 这个编号。
 * expected <= last     : 该编号已经落过盘 —— 幂等 no-op，返回 false。
 *                        （leader 因 next_index 回退而重传时会走到这里；若不
 *                        去重就会写入第二份物理副本，redo 下即双重回放。）
 * expected >  last + 1 : 出现空洞，直接 ERROR。物理回放要求流完整有序无 gap，
 *                        宁可不 ack 让 leader 回退补齐，也不能留下空洞。
 *
 * 返回值 = 本次是否真的写入了字节。
 */
bool
AppendPartWALRecordAt(PartitionWALWriter *writer,
                      uint64 expected_partition_lsn,
                      XLogRecPtr orig_lsn,
                      uint8 rmid,
                      uint8 info,
                      const char *raw_data,
                      uint32 data_len,
                      GlobalTransactionId gxid,
                      uint8 flags)
{
    XLogSegNo     new_segno;
    PartWALRecord rec;
    uint64        assigned_plsn;
    uint32        total_size;

    if (expected_partition_lsn == 0)
        assigned_plsn = writer->last_partition_lsn + 1;
    else if (expected_partition_lsn == writer->last_partition_lsn + 1)
        assigned_plsn = expected_partition_lsn;
    else if (expected_partition_lsn <= writer->last_partition_lsn)
        return false;           /* 重传去重 */
    else
        ereport(ERROR,
                (errmsg("pg_partdist: 分区 %u 的 partition_lsn 出现空洞："
                        "期望 %llu，本地只到 %llu",
                        writer->partition_id,
                        (unsigned long long) expected_partition_lsn,
                        (unsigned long long) writer->last_partition_lsn)));

    /*
     * 段文件号：DATA 记录按 orig_lsn 映射；**非 WAL 记录（DTX / 标记，
     * orig_lsn 恒为 0）必须跟随当前段**，绝不能另开一个段号。
     *
     * 为什么：段文件按文件名排序即是回放顺序（verify_partition_wal 与物理回放
     * 都这么读），而 partition_lsn 的连续性是逐段累加判定的。原先无条件把
     * orig_lsn==0 映射到段 1，于是一条 plsn=7 的 DTX_PREPARE 会被写进
     * 000000010000000000000001，而它所标记的 plsn=1..6 的 DATA 在
     * 000000010000000000000007 —— 按文件名读出来就是 7,8,1,2,3,4,5,6，
     * "标记排在它标记的数据前面"，verify 直接判损坏。
     * （2026-08-04：2PC 接线让 DTX 记录第一次和 DATA 落进同一个分区流，
     * 这条潜伏缺陷才现形；raft_14/16 的 follower 断言抓到。）
     *
     * 取当前段的三级依据：写入器正开着的段 → checkpoint 里最后一条 DATA 的
     * orig_lsn 所在段 → 都没有（该分区只写过 DTX 记录）才回落到 1。
     */
    if (orig_lsn == InvalidXLogRecPtr)
    {
        if (writer->current_segno != 0)
            new_segno = writer->current_segno;
        else if (writer->last_wal_lsn != InvalidXLogRecPtr)
            XLByteToSeg(writer->last_wal_lsn, new_segno, wal_segment_size);
        else
            new_segno = 1;
    }
    else
        XLByteToSeg(orig_lsn, new_segno, wal_segment_size);

    if (new_segno == 0)
        new_segno = 1;

    /* Flush and switch segment file when boundary crossed */
    if (writer->current_segno != 0 && new_segno != writer->current_segno)
    {
        FlushPartitionWALWriter(writer, false);
        OpenWriterSegment(writer, new_segno);
    }
    else if (writer->fd < 0)
    {
        OpenWriterSegment(writer, new_segno);
    }

    total_size = (uint32) sizeof(PartWALRecord) + data_len;

    /* Flush buffer if it would overflow */
    if (writer->buf_used + (int) total_size > PARWAL_WRITER_BUFFER_SIZE)
        FlushPartitionWALWriter(writer, false);

    /* If total_size itself exceeds the buffer, write directly */
    if ((int) total_size > PARWAL_WRITER_BUFFER_SIZE)
    {
        ssize_t   nb;
        ssize_t   remaining;
        char     *ptr;

        /* Build and write header directly */
        rec.magic         = PARTWAL_MAGIC;
        rec.partition_id  = writer->partition_id;
        rec.orig_lsn      = orig_lsn;
        rec.partition_lsn = assigned_plsn;
        writer->last_partition_lsn = assigned_plsn;
        rec.rmid          = rmid;
        rec.info          = info;
        rec.version       = PARTWAL_RECORD_VERSION_3;
        rec.flags         = flags;
        rec.data_len      = data_len;
        rec.gxid          = gxid;

        ptr       = (char *) &rec;
        remaining = sizeof(PartWALRecord);
        while (remaining > 0)
        {
            do { nb = write(writer->fd, ptr, remaining); }
            while (nb < 0 && errno == EINTR);
            if (nb <= 0)
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("pg_partdist: write error for partition %u: %m",
                                writer->partition_id)));
            ptr += nb;
            remaining -= nb;
        }

        if (data_len > 0 && raw_data != NULL)
        {
            ptr       = (char *) raw_data;
            remaining = (ssize_t) data_len;
            while (remaining > 0)
            {
                do { nb = write(writer->fd, ptr, remaining); }
                while (nb < 0 && errno == EINTR);
                if (nb <= 0)
                    ereport(ERROR,
                            (errcode_for_file_access(),
                             errmsg("pg_partdist: write error (data) for "
                                    "partition %u: %m",
                                    writer->partition_id)));
                ptr += nb;
                remaining -= nb;
            }
        }

        /*
         * 非 WAL 记录（orig_lsn==0）**不得**清掉 last_wal_lsn ——
         * 它是"当前段号"的持久化依据（见上面的段选择注释），被 0 覆盖后
         * checkpoint 也跟着变 0，下一个写入器就找不回当前段了。
         */
        if (orig_lsn != InvalidXLogRecPtr)
            writer->last_wal_lsn = orig_lsn;
        return true;
    }

    /* Build header */
    rec.magic         = PARTWAL_MAGIC;
    rec.partition_id  = writer->partition_id;
    rec.orig_lsn      = orig_lsn;
    rec.partition_lsn = assigned_plsn;
    writer->last_partition_lsn = assigned_plsn;
    rec.rmid          = rmid;
    rec.info          = info;
    rec.version       = PARTWAL_RECORD_VERSION_3;
    rec.flags         = flags;
    rec.data_len      = data_len;
    rec.gxid          = gxid;

    /* Copy header into buffer */
    memcpy(writer->buffer + writer->buf_used, &rec, sizeof(PartWALRecord));
    writer->buf_used += sizeof(PartWALRecord);

    /* Copy data into buffer */
    if (data_len > 0 && raw_data != NULL)
    {
        memcpy(writer->buffer + writer->buf_used, raw_data, data_len);
        writer->buf_used += (int) data_len;
    }

    if (orig_lsn != InvalidXLogRecPtr)
        writer->last_wal_lsn = orig_lsn;
    return true;
}

/*
 * DestroyPartitionWALWriter — flush, fsync (with checkpoint), close, pfree.
 */
void
DestroyPartitionWALWriter(PartitionWALWriter *writer)
{
    if (writer == NULL)
        return;

    FlushPartitionWALWriter(writer, true);  /* final flush with fsync + checkpoint */

    if (writer->fd >= 0)
    {
        close(writer->fd);
        writer->fd = -1;
    }

    pfree(writer);
}

/* ================================================================== */
/* 多副本组提交 fsync（pg_partdist.partwal_group_fsync，2026-10-16）      */
/* ================================================================== */

/*
 * 组提交模式下关闭写入器：只把缓冲 write() 进段文件，编号与段欠账记进节点级缓存，
 * **不 fsync、不写 checkpoint 文件**。调用方持 PartWALCtl->lock；之后（出锁）要么
 * PartWALSyncPartitionUpTo 等它落盘，要么（中止路径）交给同分区下一次组提交顺带落盘。
 * 返回该分区当前最后分配的编号。非组提交模式等同 DestroyPartitionWALWriter。
 */
uint64
PartWALWriterFinishLazy(PartitionWALWriter *writer)
{
    uint64  plsn;

    if (writer == NULL)
        return 0;
    plsn = writer->last_partition_lsn;
    if (!PartWALLazySync())
    {
        DestroyPartitionWALWriter(writer);
        return plsn;
    }

    FlushPartitionWALWriter(writer, false);
    if (writer->first_segno != 0 &&
        !ckpt_cache_note_written(writer->partition_id, writer->relfilenode,
                                 writer->last_wal_lsn, plsn,
                                 writer->first_segno, writer->current_segno))
    {
        /* 缓存里没有该分区（新分区 / 缓存满）：退回原来的同步路径，由它建缓存项或写文件 */
        if (writer->fd < 0)
            OpenWriterSegment(writer, writer->current_segno);
        FlushPartitionWALWriter(writer, true);
    }

    if (writer->fd >= 0)
    {
        close(writer->fd);
        writer->fd = -1;
    }
    pfree(writer);
    return plsn;
}

static LWLockPadded *partsync_locks = NULL;

static LWLock *
partsync_lock(Oid partition_id)
{
    if (partsync_locks == NULL)
        partsync_locks = GetNamedLWLockTranche("pg_partdist_partsync");
    return &partsync_locks[partition_id % PARTWAL_PARTSYNC_LOCKS].lock;
}

/*
 * 等分区 partition_id 已写下的字节落盘到至少 target_plsn（0 = 到目前写下的全部）。
 * 不持 PartWALCtl->lock 调用（会 fsync）。持分区组提交锁期间只做 fsync，不取别的锁。
 */
void
PartWALSyncPartitionUpTo(Oid partition_id, uint64 target_plsn)
{
    LWLock     *lk;
    int         i;
    uint64      synced;
    uint64      w;
    XLogSegNo   lo;
    XLogSegNo   hi;
    XLogSegNo   seg;
    TimestampTz now;
    bool        write_hint = false;
    RelFileNumber rfn = InvalidRelFileNumber;
    XLogRecPtr  wal = InvalidXLogRecPtr;
    uint32      gen;

    if (!PartWALLazySync() || PartWALCtl == NULL)
        return;

    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i < 0)
    {
        SpinLockRelease(&PartWALCtl->ckpt_mutex);
        return;                 /* 不在缓存里的分区走的是同步路径，已落盘 */
    }
    synced = PartWALCtl->ckpt[i].synced_part_lsn;
    if (target_plsn == 0)
        target_plsn = PartWALCtl->ckpt[i].last_part_lsn;
    SpinLockRelease(&PartWALCtl->ckpt_mutex);
    if (synced >= target_plsn)
        return;

    lk = partsync_lock(partition_id);
    LWLockAcquire(lk, LW_EXCLUSIVE);

    /* 排队期间别人可能已经替我们落过盘；否则接手全部欠账（之后的新写入重新记账） */
    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i < 0 || PartWALCtl->ckpt[i].synced_part_lsn >= target_plsn)
    {
        SpinLockRelease(&PartWALCtl->ckpt_mutex);
        LWLockRelease(lk);
        return;
    }
    w = PartWALCtl->ckpt[i].last_part_lsn;
    lo = PartWALCtl->ckpt[i].lo_unsynced_segno;
    hi = PartWALCtl->ckpt[i].hi_segno;
    PartWALCtl->ckpt[i].lo_unsynced_segno = 0;
    PartWALCtl->ckpt[i].hi_segno = 0;
    PartWALCtl->ckpt[i].sync_inflight++;
    gen = PartWALCtl->ckpt[i].trunc_gen;
    SpinLockRelease(&PartWALCtl->ckpt_mutex);

    PG_TRY();
    {
        if (lo != 0)
        {
            if (hi < lo)
                hi = lo;
            for (seg = lo; seg <= hi; seg++)
            {
                char   *rel = GetPartitionWALPath(partition_id, seg);
                char    abspath[MAXPGPATH];
                int     fd;

                snprintf(abspath, MAXPGPATH, "%s/%s", DataDir, rel);
                pfree(rel);
                fd = open(abspath, O_RDWR, 0);
                if (fd < 0)
                {
                    if (errno == ENOENT)
                        continue;       /* 跨段区间里没写过的段号 */
                    ereport(ERROR,
                            (errcode_for_file_access(),
                             errmsg("pg_partdist: 组提交 fsync：打不开段文件 \"%s\": %m", abspath)));
                }
                if (pg_fsync(fd) != 0)
                {
                    int save_errno = errno;

                    close(fd);
                    errno = save_errno;
                    /* fsync 失败后页缓存状态不可信（内核可能已丢弃脏页），与内核同规则升 PANIC */
                    ereport(data_sync_elevel(ERROR),
                            (errcode_for_file_access(),
                             errmsg("pg_partdist: 组提交 fsync \"%s\" 失败: %m", abspath)));
                }
                close(fd);
            }
        }
    }
    PG_CATCH();
    {
        /* 欠账还回去，下一个调用者重做 */
        SpinLockAcquire(&PartWALCtl->ckpt_mutex);
        i = ckpt_cache_find_locked(partition_id);
        if (i >= 0)
        {
            if (lo != 0)
            {
                if (PartWALCtl->ckpt[i].lo_unsynced_segno == 0 ||
                    lo < PartWALCtl->ckpt[i].lo_unsynced_segno)
                    PartWALCtl->ckpt[i].lo_unsynced_segno = lo;
                if (hi > PartWALCtl->ckpt[i].hi_segno)
                    PartWALCtl->ckpt[i].hi_segno = hi;
            }
            if (PartWALCtl->ckpt[i].sync_inflight > 0)
                PartWALCtl->ckpt[i].sync_inflight--;
        }
        SpinLockRelease(&PartWALCtl->ckpt_mutex);
        LWLockRelease(lk);
        PG_RE_THROW();
    }
    PG_END_TRY();

    now = GetCurrentTimestamp();
    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i >= 0)
    {
        PartWALCkptCacheEnt *e = &PartWALCtl->ckpt[i];

        if (e->sync_inflight > 0)
            e->sync_inflight--;
        /*
         * 期间若被截断（编号回退）：w 可能指向截断后重新长出来、还没 fsync 的字节，既不推进
         * synced 也不写提示（提示里的旧编号会盖掉截断时落盘的值）。
         */
        if (e->trunc_gen == gen && w > e->synced_part_lsn && w <= e->last_part_lsn)
            e->synced_part_lsn = w;
        /* checkpoint 文件只作重启提示：每个分区每秒至多写一次，不 fsync */
        if (e->trunc_gen == gen && TimestampDifferenceExceeds(e->file_ts, now, 1000))
        {
            e->file_ts = now;
            write_hint = true;
            rfn = e->relfilenode;
            wal = e->last_wal_lsn;
            w = e->synced_part_lsn;
        }
    }
    SpinLockRelease(&PartWALCtl->ckpt_mutex);
    LWLockRelease(lk);

    if (write_hint && RelFileNumberIsValid(rfn))
        write_ckpt_file(partition_id, rfn, wal, w, false);
}

/*
 * 本分区"已落盘"的最大编号 —— 复制只许送到这里（get_partition_flush_lsn 返回它）。
 * 组提交模式下先把已写下的字节组提交落盘，保证调用方刚写下的记录一定被覆盖。
 * follower 批内（延迟 fsync）看的是本后端自己的延迟表，与原语义相同。
 */
uint64
PartWALDurablePartitionLSN(Oid partition_id)
{
    PartWALCheckpointFile chk;
    uint64      v = 0;
    int         i;

    if (n_defer_ents > 0)
    {
        PartWALDeferEnt *e = defer_find(partition_id);

        if (e != NULL)
            return e->last_part_lsn;
    }
    if (!PartWALLazySync() || PartWALCtl == NULL)
        return GetLastWrittenPartitionLSN(partition_id);

    if (!ReadPartWALCheckpoint(partition_id, &chk))
        return 0;
    PartWALSyncPartitionUpTo(partition_id, 0);

    SpinLockAcquire(&PartWALCtl->ckpt_mutex);
    i = ckpt_cache_find_locked(partition_id);
    if (i >= 0)
        v = PartWALCtl->ckpt[i].synced_part_lsn;
    SpinLockRelease(&PartWALCtl->ckpt_mutex);
    if (i < 0)
        return chk.last_part_lsn;
    return v;
}

/* ================================================================== */
/* Truncation (数据面 Raft 日志截断的对应动作)                          */
/* ================================================================== */

/*
 * TruncatePartWALTo — 丢弃 partition_lsn > keep_upto_plsn 的所有记录。
 *
 * 为什么必须有这个函数：Raft 截断掉一条未提交条目后，已经 ack 过该条目的
 * follower 上那份字节仍留在 pg_parwal 里，占着一个 partition_lsn。切主之后
 * 新 leader 会把**不同的**记录写到同一个编号上，此时 AppendPartWALRecordAt
 * 的幂等去重（expected <= last 即跳过）反而会保留旧字节 —— 物理回放就会
 * 重放错误的内容。所以日志截断必须同步截断 parwal。
 *
 * 做法：逐段逐记录剔除 partition_lsn > keep_upto_plsn 的记录（整段删 / 截尾 / 重写），
 * 并回退 checkpoint。**不按段名顺序推断 plsn 顺序**，见函数体内 P7-N1 之二的说明。
 *
 * 返回是否真的截断了内容。
 */
bool
TruncatePartWALTo(Oid partition_id, RelFileNumber relfilenode,
                  uint64 keep_upto_plsn)
{
    /*
     * ★ P7-N1 之二（2026-09-18）：**不许按段文件名顺序推断 plsn 顺序**。
     *
     * 原实现把段文件按名字排序，找到"名字序里第一条 plsn > keep 的记录"，截断该段、
     * 并把**名字排在后面的段整个删掉**。前提是"段名升序即 plsn 升序"——段名取自记录的
     * orig_lsn 所在 pg_wal 段，而一个分区流里的记录可能来自**不同节点的 WAL 坐标系**：
     * 被降级的原始主，自己当主时写的记录按**本机** LSN 落段，降级后收的新主记录按
     * **新主** LSN 落段，两套坐标互不相干。实测（N25 回归，2026-09-18）：A 自己的旧记录和
     * 一条孤儿（它降级后仍尝试发冻结账目、写下又被拒）在 …36 段，新主 B 发来的已提交
     * 记录 141–147 在 …47 段；替换孤儿要截到 147 ⇒ 名字序里先在 …36 碰到孤儿 148 ⇒
     * 截 …36、把 …47 整个删掉 ⇒ 已提交的 141–147 灰飞烟灭 ⇒ `流内空洞：期望 142，
     * 下一条已存在的是 148`。空洞让该副本永远追不上（P7-N32 的起点）。
     *
     * 现在逐段、逐记录判：
     *   · 段内没有 > keep 的记录 ⇒ 不动；
     *   · 段内全是 > keep ⇒ 删；
     *   · 段内 > keep 的记录都在尾部 ⇒ 截到第一条 > keep 处；
     *   · 段内 > keep 之后还夹着 ≤ keep 的记录 ⇒ 只保留 ≤ keep 的记录**重写**该段。
     * checkpoint 回退用的 last_kept_lsn 取**全体段里 plsn ≤ keep 的最大那条**的 orig_lsn，
     * 同样不依赖段名顺序。残尾（magic 不对）照旧视为该段结束，不另做处理。
     */
    char           dirpath[MAXPGPATH];
    PartWALSegName *segfiles;
    int            nfiles = 0;
    int            i;
    bool           truncated = false;
    uint64         best_kept_plsn = 0;
    XLogRecPtr     last_kept_lsn = InvalidXLogRecPtr;

    /* P7-N41：截断要按盘上状态算，先把本分区批内推迟的 fsync / checkpoint 落实 */
    PartWALDeferSyncPartition(partition_id);
    PartWALRecentInvalidateAll();       /* 截断开始前就让缓存失效（结束时再换一次代） */

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    /* 2026-10-12：全部段、升序（原来最多 256 段且不排序，第 257 段起的记录截不到） */
    segfiles = PartWALListSegments(partition_id, &nfiles);
    if (nfiles == 0)
        return false;

    for (i = 0; i < nfiles; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALRecord rec;
        ssize_t       nb;
        off_t         off = 0;
        off_t         first_bad = -1;   /* 段内第一条 > keep 的偏移 */
        bool          mixed = false;     /* 其后还有 ≤ keep 的记录 */
        bool          any_kept = false;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = OpenTransientFile(filepath, O_RDONLY | PG_BINARY);
        if (fd < 0)
            continue;

        while ((nb = read(fd, &rec, sizeof(PartWALRecord)))
               == (ssize_t) sizeof(PartWALRecord))
        {
            if (rec.magic != PARTWAL_MAGIC)
                break;                  /* 残尾：本段到此为止 */

            if (rec.partition_lsn > keep_upto_plsn)
            {
                if (first_bad < 0)
                    first_bad = off;
            }
            else
            {
                any_kept = true;
                if (first_bad >= 0)
                    mixed = true;
                if (rec.partition_lsn >= best_kept_plsn &&
                    rec.orig_lsn != InvalidXLogRecPtr)
                {
                    best_kept_plsn = rec.partition_lsn;
                    last_kept_lsn = rec.orig_lsn;
                }
            }
            off += (off_t) (sizeof(PartWALRecord) + rec.data_len);
            if (lseek(fd, off, SEEK_SET) != off)
                break;
        }
        CloseTransientFile(fd);

        if (first_bad < 0)
            continue;                   /* 本段没有要丢的 */

        if (!any_kept)
        {
            /* 全段都 > keep：整段删 */
            if (unlink(filepath) == 0)
                truncated = true;
            else if (errno != ENOENT)
                ereport(WARNING,
                        (errcode_for_file_access(),
                         errmsg("pg_partdist: 无法删除段文件 \"%s\": %m", filepath)));
        }
        else if (!mixed)
        {
            /* > keep 的都在尾部：截到第一条处 */
            fd = OpenTransientFile(filepath, O_RDWR | PG_BINARY);
            if (fd >= 0)
            {
                if (ftruncate(fd, first_bad) != 0)
                    ereport(WARNING,
                            (errcode_for_file_access(),
                             errmsg("pg_partdist: 无法截断 \"%s\" 到 %lld: %m",
                                    filepath, (long long) first_bad)));
                else
                {
                    if (pg_fsync(fd) != 0)
                        ereport(WARNING,
                                (errcode_for_file_access(),
                                 errmsg("pg_partdist: 截断后 fsync 失败 \"%s\": %m",
                                        filepath)));
                    truncated = true;
                }
                CloseTransientFile(fd);
            }
        }
        else
        {
            /* > keep 之后还夹着 ≤ keep：只保留 ≤ keep 的记录重写本段 */
            char    tmppath[MAXPGPATH];
            int     in_fd;
            int     out_fd;
            bool    ok = true;

            snprintf(tmppath, MAXPGPATH, "%s.trunc.tmp", filepath);
            in_fd = OpenTransientFile(filepath, O_RDONLY | PG_BINARY);
            out_fd = OpenTransientFile(tmppath, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
            if (in_fd < 0 || out_fd < 0)
                ok = false;
            while (ok && (nb = read(in_fd, &rec, sizeof(PartWALRecord)))
                   == (ssize_t) sizeof(PartWALRecord))
            {
                char   *buf = NULL;

                if (rec.magic != PARTWAL_MAGIC)
                    break;
                if (rec.data_len > 0)
                {
                    buf = palloc(rec.data_len);
                    if (read(in_fd, buf, rec.data_len) != (ssize_t) rec.data_len)
                    {
                        pfree(buf);
                        ok = false;
                        break;
                    }
                }
                if (rec.partition_lsn <= keep_upto_plsn)
                {
                    if (write(out_fd, &rec, sizeof(rec)) != (ssize_t) sizeof(rec) ||
                        (rec.data_len > 0 &&
                         write(out_fd, buf, rec.data_len) != (ssize_t) rec.data_len))
                        ok = false;
                }
                if (buf != NULL)
                    pfree(buf);
            }
            if (ok && pg_fsync(out_fd) != 0)
                ok = false;
            if (in_fd >= 0)
                CloseTransientFile(in_fd);
            if (out_fd >= 0)
                CloseTransientFile(out_fd);
            if (ok && durable_rename(tmppath, filepath, WARNING) == 0)
                truncated = true;
            else
            {
                (void) unlink(tmppath);
                ereport(WARNING,
                        (errmsg("pg_partdist: 重写段文件 \"%s\"（剔除 plsn > %llu）失败",
                                filepath, (unsigned long long) keep_upto_plsn)));
            }
        }
    }

    /* 编号回退：缓存里 > keep 的条目从此可能与文件不符，整体换代 */
    PartWALRecentInvalidateAll();

    if (truncated)
    {
        WritePartWALCheckpoint(partition_id, relfilenode,
                               last_kept_lsn, keep_upto_plsn);
        /*
         * 缓存模式下 WritePartWALCheckpoint 只改节点级缓存，盘上提示文件仍是截断前更大的
         * 编号；重启后段全被删光（扫描为 0）时会按它续号 ⇒ 编号跳空。截断是罕见动作，
         * 这里把文件同步改写并 fsync。
         */
        if (ckpt_cached_mode() && RelFileNumberIsValid(relfilenode))
            write_ckpt_file(partition_id, relfilenode, last_kept_lsn, keep_upto_plsn,
                            !partwal_fast_commit);
    }

    return truncated;
}
