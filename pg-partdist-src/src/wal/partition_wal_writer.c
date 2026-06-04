/*
 * partition_wal_writer.c
 * Buffered writer for pg_parwal/<partition_id>/ segment files (Milestone 2.2).
 *
 * Each PartitionWALWriter holds a 256 KB write buffer.  Records from the Demux
 * Worker accumulate in the buffer and are flushed:
 *   • when the buffer is full
 *   • when the orig_node_lsn crosses a WAL segment boundary
 *   • when the writer is destroyed (DestroyPartitionWALWriter)
 *
 * Segment files use the same naming convention as PostgreSQL's native pg_wal
 * (XLogFileName with timeline=1), keyed on the orig_node_lsn of the record.
 *
 * GetLastWrittenPartitionLSN scans the on-disk segment files to find the
 * highest partition_lsn already stored, allowing the Demux Worker to skip
 * records that the synchronous M2.1 path already wrote.
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
/* Internal helpers                                                     */
/* ================================================================== */

/*
 * OpenWriterSegment — open (or create) the segment file for segno.
 * Closes any previously open fd first.
 */
static void
OpenWriterSegment(PartitionWALWriter *writer, XLogSegNo segno)
{
    char relpath[MAXPGPATH];
    char abspath[MAXPGPATH];
    char *rel;
    int   fd;

    /* Close current file if different segment */
    if (writer->fd >= 0)
    {
        if (pg_fsync(writer->fd) != 0)
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: could not fsync partition WAL file: %m")));
        close(writer->fd);
        writer->fd = -1;
    }

    rel = GetPartitionWALPath(writer->partition_id, segno);
    strlcpy(relpath, rel, MAXPGPATH);
    pfree(rel);

    snprintf(abspath, MAXPGPATH, "%s/%s", DataDir, relpath);

    /* O_APPEND for safety when M2.1 direct path writes concurrently */
    fd = open(abspath, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: could not open partition WAL segment "
                        "\"%s\": %m", abspath)));

    writer->fd            = fd;
    writer->current_segno = segno;
    writer->write_position = 0;
    writer->enospc_stalled = false;
}

/* ================================================================== */
/* Public API                                                           */
/* ================================================================== */

/*
 * GetLastWrittenPartitionLSN — scan pg_parwal/<partition_id>/ and return the
 * highest partition_lsn recorded (0 if directory absent or empty).
 *
 * We read only the last sizeof(PartWALHeader) bytes of each file so this
 * stays O(segments) not O(records).
 */
uint64
GetLastWrittenPartitionLSN(Oid partition_id)
{
    char          dirpath[MAXPGPATH];
    DIR          *dir;
    struct dirent *de;
    char          segfiles[256][MAXPGPATH];
    int           nfiles = 0;
    int           i, j;
    uint64        max_lsn = 0;

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

    /* Sort descending — we only need the last segment */
    for (i = 0; i < nfiles - 1; i++)
        for (j = i + 1; j < nfiles; j++)
            if (strcmp(segfiles[i], segfiles[j]) < 0)
            {
                char tmp[MAXPGPATH];
                strlcpy(tmp, segfiles[i], MAXPGPATH);
                strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                strlcpy(segfiles[j], tmp, MAXPGPATH);
            }

    /* Walk all files in descending order until we find a record */
    for (i = 0; i < nfiles && max_lsn == 0; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        off_t         sz;
        PartWALHeader header;
        ssize_t       nb;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY, 0);
        if (fd < 0)
            continue;

        sz = lseek(fd, 0, SEEK_END);
        if (sz >= (off_t) sizeof(PartWALHeader))
        {
            /* Read last complete record */
            if (lseek(fd,
                      sz - (off_t)(sz % sizeof(PartWALHeader) == 0
                                   ? sizeof(PartWALHeader)
                                   : sz % sizeof(PartWALHeader)),
                      SEEK_SET) >= 0)
            {
                nb = read(fd, &header, sizeof(PartWALHeader));
                if (nb == (ssize_t) sizeof(PartWALHeader) &&
                    header.magic == PARTWAL_MAGIC &&
                    header.partition_id == partition_id)
                {
                    max_lsn = header.partition_lsn;
                }
            }

            /* If last-record attempt failed, scan for highest lsn in file */
            if (max_lsn == 0)
            {
                lseek(fd, 0, SEEK_SET);
                while ((nb = read(fd, &header, sizeof(PartWALHeader)))
                       == (ssize_t) sizeof(PartWALHeader))
                {
                    if (header.magic == PARTWAL_MAGIC &&
                        header.partition_id == partition_id &&
                        header.partition_lsn > max_lsn)
                        max_lsn = header.partition_lsn;
                }
            }
        }
        close(fd);
    }

    return max_lsn;
}

/*
 * CreatePartitionWALWriter — allocate a writer; initialise the dedup watermark
 * from whatever is already on disk for this partition.
 */
PartitionWALWriter *
CreatePartitionWALWriter(Oid partition_id)
{
    PartitionWALWriter *w;

    /* Ensure the pg_parwal/<partition_id>/ directory exists */
    InitPartitionWALDirectory(partition_id);

    w = (PartitionWALWriter *) MemoryContextAllocZero(TopMemoryContext,
                                                       sizeof(PartitionWALWriter));
    w->partition_id       = partition_id;
    w->fd                 = -1;
    w->current_segno      = 0;
    w->buf_used           = 0;
    w->enospc_stalled     = false;
    w->last_partition_lsn = GetLastWrittenPartitionLSN(partition_id);

    return w;
}

/*
 * FlushPartitionWALWriter — write all buffered bytes to the current segment
 * file and fsync.
 */
void
FlushPartitionWALWriter(PartitionWALWriter *writer)
{
    ssize_t written;
    int     remaining;
    char   *ptr;

    if (writer->buf_used == 0)
        return;

    if (writer->fd < 0)
        return;

    ptr       = writer->buffer;
    remaining = writer->buf_used;

    while (remaining > 0)
    {
        do {
            written = write(writer->fd, ptr, remaining);
        } while (written < 0 && errno == EINTR);

        if (written < 0)
        {
            if (errno == ENOSPC)
            {
                ereport(ERROR,
                        (errcode(ERRCODE_DISK_FULL),
                         errmsg("pg_partdist: disk full writing partition WAL "
                                "for partition %u", writer->partition_id)));
            }
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: could not write partition WAL: %m")));
        }

        ptr       += written;
        remaining -= (int) written;
    }

    if (pg_fsync(writer->fd) != 0)
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: fsync failed for partition %u WAL: %m",
                        writer->partition_id)));

    writer->write_position += writer->buf_used;
    writer->buf_used        = 0;
}

/*
 * WritePartitionWAL — buffer one PartWALHeader.
 *
 * Switches segment file when orig_node_lsn crosses a WAL segment boundary,
 * which keeps pg_parwal segment files aligned with the main pg_wal segments.
 */
void
WritePartitionWAL(PartitionWALWriter *writer, const PartWALHeader *header)
{
    XLogSegNo new_segno;

    /* Map this record's orig_node_lsn to a segment number */
    if (header->orig_node_lsn == InvalidXLogRecPtr)
        new_segno = 1;
    else
        XLByteToSeg(header->orig_node_lsn, new_segno, wal_segment_size);

    if (new_segno == 0)
        new_segno = 1;

    /* Flush and switch segment file when boundary crossed */
    if (writer->current_segno != 0 && new_segno != writer->current_segno)
    {
        FlushPartitionWALWriter(writer);
        OpenWriterSegment(writer, new_segno);
    }
    else if (writer->fd < 0)
    {
        /* First write — open the initial segment */
        OpenWriterSegment(writer, new_segno);
    }

    /* Flush if buffer is full */
    if (writer->buf_used + (int) sizeof(PartWALHeader) > PARWAL_WRITER_BUFFER_SIZE)
        FlushPartitionWALWriter(writer);

    memcpy(writer->buffer + writer->buf_used, header, sizeof(PartWALHeader));
    writer->buf_used          += sizeof(PartWALHeader);
    writer->last_partition_lsn = header->partition_lsn;
}

/*
 * DestroyPartitionWALWriter — flush, fsync, close, pfree.
 */
void
DestroyPartitionWALWriter(PartitionWALWriter *writer)
{
    if (writer == NULL)
        return;

    FlushPartitionWALWriter(writer);

    if (writer->fd >= 0)
    {
        if (pg_fsync(writer->fd) != 0)
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: final fsync failed for partition %u: %m",
                            writer->partition_id)));
        close(writer->fd);
        writer->fd = -1;
    }

    pfree(writer);
}
