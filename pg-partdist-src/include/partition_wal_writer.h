/*
 * partition_wal_writer.h
 * Buffered per-partition segment-file writer used by the Demux Worker.
 */
#ifndef PARTITION_WAL_WRITER_H
#define PARTITION_WAL_WRITER_H

#include "postgres.h"
#include "partition_wal_header.h"
#include "access/xlogdefs.h"

/* 256 KB write buffer per partition */
#define PARWAL_WRITER_BUFFER_SIZE   (256 * 1024)

typedef struct PartitionWALWriter
{
    Oid         partition_id;
    int         fd;                     /* open segment file descriptor, -1 if none */
    XLogRecPtr  write_position;         /* next byte offset within segment file */
    uint64      last_partition_lsn;     /* highest partition_lsn written so far */
    XLogSegNo   current_segno;          /* segment number of currently open file */

    /* Write buffer — records accumulate here until flushed */
    char        buffer[PARWAL_WRITER_BUFFER_SIZE];
    int         buf_used;               /* bytes currently in buffer */

    bool        enospc_stalled;         /* true while disk is full */
} PartitionWALWriter;

/*
 * CreatePartitionWALWriter — allocate and initialise a writer.
 * Scans pg_parwal/<partition_id>/ to find the highest partition_lsn already
 * written (by the synchronous M2.1 path or a previous Demux Worker run) so
 * that we can skip duplicates.
 */
extern PartitionWALWriter *CreatePartitionWALWriter(Oid partition_id);

/*
 * DestroyPartitionWALWriter — flush pending buffer, fsync, close fd, pfree.
 */
extern void DestroyPartitionWALWriter(PartitionWALWriter *writer);

/*
 * WritePartitionWAL — buffer one PartWALHeader for the given partition.
 * Flushes automatically on segment boundary or when buffer is full.
 * Handles ENOSPC by stalling writes for the affected partition.
 */
extern void WritePartitionWAL(PartitionWALWriter *writer,
                               const PartWALHeader *header);

/*
 * FlushPartitionWALWriter — write buffered data to disk and fsync.
 */
extern void FlushPartitionWALWriter(PartitionWALWriter *writer);

/*
 * GetLastWrittenPartitionLSN — scan pg_parwal/<partition_id>/ and return the
 * highest partition_lsn already on disk (0 if none).
 */
extern uint64 GetLastWrittenPartitionLSN(Oid partition_id);

#endif /* PARTITION_WAL_WRITER_H */
