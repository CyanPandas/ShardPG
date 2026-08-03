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
 *
 * gxid  由**调用方**合成（MakeGlobalXid(PartDistLocalNodeId(), xid)）：
 *       捕获点在 XLogInsert 内部，那里不能碰目录，节点号只能等到 flush 路径
 *       上再解析。写入器只负责把它原样落进头部。
 * flags PARTWAL_FLAG_* 记录类别；0 与 PARTWAL_FLAG_DATA 等价（DATA 是缺省类）。
 */
extern void AppendPartWALRecord(PartitionWALWriter *writer,
                                XLogRecPtr orig_lsn,
                                uint8 rmid,
                                uint8 info,
                                const char *raw_data,
                                uint32 data_len,
                                GlobalTransactionId gxid,
                                uint8 flags);

/*
 * AppendPartWALRecordAt — 按指定 partition_lsn 落盘（数据面 Raft follower 用）。
 * expected == 0 等价于 AppendPartWALRecord（本地自增）。
 * expected <= 本地已有 → 幂等 no-op 返回 false；出现空洞 → ERROR。
 */
extern bool AppendPartWALRecordAt(PartitionWALWriter *writer,
                                  uint64 expected_partition_lsn,
                                  XLogRecPtr orig_lsn,
                                  uint8 rmid,
                                  uint8 info,
                                  const char *raw_data,
                                  uint32 data_len,
                                  GlobalTransactionId gxid,
                                  uint8 flags);

/*
 * TruncatePartWALTo — 丢弃 partition_lsn > keep_upto_plsn 的记录并回退
 * checkpoint。Raft 日志截断时必须同步调用，否则被截断条目的字节会滞留，
 * 而新 leader 会把不同记录写到同一个 partition_lsn 上。
 */
extern bool TruncatePartWALTo(Oid partition_id,
                              RelFileNumber relfilenode,
                              uint64 keep_upto_plsn);

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
