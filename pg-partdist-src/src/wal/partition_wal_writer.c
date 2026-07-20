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

#include "partition_wal_writer.h"
#include "partition_wal.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/memutils.h"

/* ================================================================== */
/* Checkpoint file I/O                                                  */
/* ================================================================== */

void
WritePartWALCheckpoint(Oid partition_id,
                       RelFileNumber relfilenode,
                       XLogRecPtr last_wal_lsn,
                       uint64 last_part_lsn)
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
    snprintf(tmp,  MAXPGPATH, "%s.tmp", path);

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

    if (pg_fsync(fd) != 0)
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

bool
ReadPartWALCheckpoint(Oid partition_id, PartWALCheckpointFile *out)
{
    char    path[MAXPGPATH];
    int     fd;
    ssize_t nb;

    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, partition_id,
             PARTWAL_CHECKPOINT_FILENAME);

    fd = open(path, O_RDONLY, 0);
    if (fd < 0)
        return false;

    nb = read(fd, out, sizeof(*out));
    close(fd);

    if (nb != (ssize_t) sizeof(*out))
        return false;
    if (out->magic != PARTWAL_CHECKPOINT_MAGIC)
        return false;

    return true;
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
    {
        char           dirpath[MAXPGPATH];
        DIR           *dir;
        struct dirent *de;
        char           segfiles[256][MAXPGPATH];
        int            nfiles = 0;
        int            i, j;
        uint64         max_lsn = 0;

        snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
                 DataDir, PARTITION_WAL_DIR, partition_id);

        dir = opendir(dirpath);
        if (dir == NULL)
            return 0;

        while ((de = readdir(dir)) != NULL && nfiles < 256)
        {
            if (IsXLogFileName(de->d_name))
            {
                strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
                nfiles++;
            }
        }
        closedir(dir);

        if (nfiles == 0)
            return 0;

        /* Sort ascending */
        for (i = 0; i < nfiles - 1; i++)
            for (j = i + 1; j < nfiles; j++)
                if (strcmp(segfiles[i], segfiles[j]) > 0)
                {
                    char tmp[MAXPGPATH];
                    strlcpy(tmp,         segfiles[i], MAXPGPATH);
                    strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                    strlcpy(segfiles[j], tmp,         MAXPGPATH);
                }

        for (i = 0; i < nfiles; i++)
        {
            char          filepath[MAXPGPATH];
            int           fd;
            PartWALRecord rec;
            ssize_t       nb;

            snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
            fd = open(filepath, O_RDONLY, 0);
            if (fd < 0)
                continue;

            while ((nb = read(fd, &rec, sizeof(PartWALRecord)))
                   == (ssize_t) sizeof(PartWALRecord))
            {
                if (rec.magic == PARTWAL_MAGIC &&
                    rec.partition_id == partition_id &&
                    rec.partition_lsn > max_lsn)
                    max_lsn = rec.partition_lsn;

                /* Skip variable-length payload */
                if (rec.data_len > 0)
                {
                    if (lseek(fd, (off_t) rec.data_len, SEEK_CUR) < 0)
                        break;
                }
            }

            close(fd);
        }

        return max_lsn;
    }
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

    w = (PartitionWALWriter *) MemoryContextAllocZero(TopMemoryContext,
                                                       sizeof(PartitionWALWriter));
    w->partition_id    = partition_id;
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

    if (with_fsync)
    {
        /*
         * fsync 失败必须是 ERROR 而非 WARNING：数据面 Raft 的 follower 依赖
         * "落盘成功才 ack" 来保证「多数派提交 == 多数派已持久化」。若这里只
         * 告警，一次 EIO 仍会产生 ack，多数派提交就不再蕴含持久化。
         */
        if (pg_fsync(writer->fd) != 0)
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
                    TransactionId xid)
{
    /* expected == 0：本地自增分配（leader / demux 侧的原有语义）。 */
    (void) AppendPartWALRecordAt(writer, 0, orig_lsn, rmid, info,
                                 raw_data, data_len, xid);
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
                      TransactionId xid)
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

    /* Map orig_lsn to a segment number */
    if (orig_lsn == InvalidXLogRecPtr)
        new_segno = 1;
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
        rec.version       = PARTWAL_RECORD_VERSION_2;
        rec.flags         = 0;
        rec.data_len      = data_len;
        rec.xid           = xid;

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
    rec.version       = PARTWAL_RECORD_VERSION_2;
    rec.flags         = 0;
    rec.data_len      = data_len;
    rec.xid           = xid;

    /* Copy header into buffer */
    memcpy(writer->buffer + writer->buf_used, &rec, sizeof(PartWALRecord));
    writer->buf_used += sizeof(PartWALRecord);

    /* Copy data into buffer */
    if (data_len > 0 && raw_data != NULL)
    {
        memcpy(writer->buffer + writer->buf_used, raw_data, data_len);
        writer->buf_used += (int) data_len;
    }

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
 * 做法：定位第一条 partition_lsn > keep_upto_plsn 的记录的起始偏移，把该段
 * 文件 ftruncate 到这个偏移，删除其后的所有段文件，并回退 checkpoint。
 *
 * 返回是否真的截断了内容。
 */
bool
TruncatePartWALTo(Oid partition_id, RelFileNumber relfilenode,
                  uint64 keep_upto_plsn)
{
    char           dirpath[MAXPGPATH];
    DIR           *dir;
    struct dirent *de;
    char           segfiles[256][MAXPGPATH];
    int            nfiles = 0;
    int            i, j;
    bool           truncated = false;
    bool           cut_found = false;
    int            cut_file = -1;
    off_t          cut_off = 0;
    XLogRecPtr     last_kept_lsn = InvalidXLogRecPtr;

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = AllocateDir(dirpath);
    if (dir == NULL)
        return false;

    while ((de = ReadDir(dir, dirpath)) != NULL && nfiles < 256)
    {
        if (IsXLogFileName(de->d_name))
        {
            strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
            nfiles++;
        }
    }
    FreeDir(dir);

    if (nfiles == 0)
        return false;

    /* 段文件名升序即 partition_lsn 升序 */
    for (i = 0; i < nfiles - 1; i++)
        for (j = i + 1; j < nfiles; j++)
            if (strcmp(segfiles[i], segfiles[j]) > 0)
            {
                char tmp[MAXPGPATH];

                strlcpy(tmp,         segfiles[i], MAXPGPATH);
                strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                strlcpy(segfiles[j], tmp,         MAXPGPATH);
            }

    /* 第一遍：找到切点（第一条 plsn > keep_upto 的记录的起始偏移） */
    for (i = 0; i < nfiles && !cut_found; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALRecord rec;
        ssize_t       nb;
        off_t         off = 0;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = OpenTransientFile(filepath, O_RDONLY | PG_BINARY);
        if (fd < 0)
            continue;

        while ((nb = read(fd, &rec, sizeof(PartWALRecord)))
               == (ssize_t) sizeof(PartWALRecord))
        {
            if (rec.magic != PARTWAL_MAGIC)
                break;                  /* 尾部残record，就地当作切点 */

            if (rec.partition_lsn > keep_upto_plsn)
            {
                cut_found = true;
                cut_file  = i;
                cut_off   = off;
                break;
            }

            last_kept_lsn = rec.orig_lsn;
            off += (off_t) (sizeof(PartWALRecord) + rec.data_len);
            if (lseek(fd, off, SEEK_SET) != off)
                break;
        }

        CloseTransientFile(fd);
    }

    if (!cut_found)
        return false;                   /* 本地没有超出 keep_upto 的内容 */

    /* 第二遍：截断切点所在文件，删除其后的段文件 */
    for (i = cut_file; i < nfiles; i++)
    {
        char filepath[MAXPGPATH];

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);

        if (i == cut_file && cut_off > 0)
        {
            int fd = OpenTransientFile(filepath, O_RDWR | PG_BINARY);

            if (fd >= 0)
            {
                if (ftruncate(fd, cut_off) != 0)
                    ereport(WARNING,
                            (errcode_for_file_access(),
                             errmsg("pg_partdist: 无法截断 \"%s\" 到 %lld: %m",
                                    filepath, (long long) cut_off)));
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
            /* 切点在段首（cut_off == 0）时该段整体删除 */
            if (unlink(filepath) == 0)
                truncated = true;
            else if (errno != ENOENT)
                ereport(WARNING,
                        (errcode_for_file_access(),
                         errmsg("pg_partdist: 无法删除段文件 \"%s\": %m",
                                filepath)));
        }
    }

    if (truncated)
        WritePartWALCheckpoint(partition_id, relfilenode,
                               last_kept_lsn, keep_upto_plsn);

    return truncated;
}
