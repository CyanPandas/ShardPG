/*
 * partition_wal_writer.h
 * Buffered per-partition segment-file writer for parwal-2.0.
 *
 * Writes self-contained PartWALRecord entries (header + raw WAL data)
 * to pg_parwal/<partition_id>/<segname> files.
 * Also manages the per-partition checkpoint file.
 */
#ifndef PARTITION_WAL_WRITER_H
#define PARTITION_WAL_WRITER_H

#include "postgres.h"
#include "partition_wal_header.h"
#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "utils/timestamp.h"

/* 256 KB write buffer per partition */
#define PARWAL_WRITER_BUFFER_SIZE   (256 * 1024)

/* Name of the checkpoint file within each partition directory */
#define PARTWAL_CHECKPOINT_FILENAME "checkpoint"

typedef struct PartitionWALWriter
{
    Oid             partition_id;
    RelFileNumber   relfilenode;         /* relfilenode of the partition table */
    int             fd;                  /* open segment file descriptor, -1 if none */
    XLogSegNo       current_segno;       /* segment number of currently open file */
    uint64          last_partition_lsn;  /* highest partition_lsn written so far */
    XLogRecPtr      last_wal_lsn;        /* last orig_lsn written (for checkpoint) */

    /* Write buffer — records accumulate here until flushed */
    char            buffer[PARWAL_WRITER_BUFFER_SIZE];
    int             buf_used;            /* bytes currently in buffer */

    bool            enospc_stalled;      /* true while disk is full */
    TimestampTz     last_stall_time;     /* when enospc_stalled was last set */
} PartitionWALWriter;

/*
 * CreatePartitionWALWriter — allocate and initialise a writer.
 * Reads the checkpoint file to determine last_partition_lsn and last_wal_lsn.
 */
extern PartitionWALWriter *CreatePartitionWALWriter(Oid partition_id,
                                                    RelFileNumber relfilenode);

/*
 * DestroyPartitionWALWriter — flush pending buffer, fsync, update checkpoint,
 * close fd, pfree.
 */
extern void DestroyPartitionWALWriter(PartitionWALWriter *writer);

/*
 * AppendPartWALRecord — buffer one PartWALRecord (header + raw_data bytes)
 * for the given partition.  Flushes automatically on segment boundary or
 * when buffer is full.  Updates last_partition_lsn and last_wal_lsn.
 */
extern void AppendPartWALRecord(PartitionWALWriter *writer,
                                XLogRecPtr orig_lsn,
                                uint8 rmid,
                                uint8 info,
                                const char *raw_data,
                                uint32 data_len,
                                TransactionId xid);

/*
 * FlushPartitionWALWriter — write buffered data to disk.
 * If with_fsync is true, also calls pg_fsync() and updates the checkpoint file.
 */
extern void FlushPartitionWALWriter(PartitionWALWriter *writer, bool with_fsync);

/*
 * WritePartWALCheckpoint — write the checkpoint file for a partition.
 * Called after flushing to record last_wal_lsn and last_part_lsn for
 * crash recovery.
 */
extern void WritePartWALCheckpoint(Oid partition_id,
                                   RelFileNumber relfilenode,
                                   XLogRecPtr last_wal_lsn,
                                   uint64 last_part_lsn);

/*
 * ReadPartWALCheckpoint — read the checkpoint file for a partition.
 * Returns false if the file does not exist or has invalid magic.
 */
extern bool ReadPartWALCheckpoint(Oid partition_id,
                                  PartWALCheckpointFile *out);

/*
 * GetLastWrittenPartitionLSN — scan pg_parwal/<partition_id>/ segment files
 * and return the highest partition_lsn on disk (0 if none).
 * Reads PartWALRecord headers; skips variable-length data portions.
 */
extern uint64 GetLastWrittenPartitionLSN(Oid partition_id);

#endif /* PARTITION_WAL_WRITER_H */
