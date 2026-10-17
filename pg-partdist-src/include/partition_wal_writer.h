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

    XLogSegNo       first_segno;         /* 本写入器打开过的最小段号（组提交 fsync 的欠账下界），0 = 未开过 */
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
 * 单副本快速提交（GUC pg_partdist.single_replica_fast_commit，默认 off）：分区 WAL 段与
 * checkpoint 文件照常写入但不 fsync。只能在全部数据组都是单成员（REPLICAS=0）时开启 ——
 * 多副本下 follower 依赖"落盘成功才 ack"。持久化以 pg_wal 为准；崩溃可能丢分区流尾部。
 */
extern bool partwal_fast_commit;

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
 *
 * flags：记录分类（PARTWAL_FLAG_*）。follower 复制路径必须把 leader 的 flags
 * 原样透传 —— 丢了它，DTX/标记记录在副本上会退化成 DATA 记录，升主回放时
 * 被当作 WAL 字节喂给 rm_redo（DTX_2PC_DESIGN.md §5.5）。
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
 * P7-N41 group commit：follower 收一批 AppendEntries 时批内延迟 fsync（backend 本地）。
 * Begin 打开；End 对碰过的段各 fsync 一次并持久化 checkpoint（失败 ERROR）；
 * SyncPartition 让某分区提前落实（截断前用）；Reset 在事务结束时丢弃状态。
 */
extern bool PartWALDeferSync;
extern void PartWALDeferSyncBegin(void);
extern int  PartWALDeferSyncEnd(void);
extern void PartWALDeferSyncPartition(Oid partition_id);
extern void PartWALDeferSyncReset(void);

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
extern uint32 PartWALTruncGen(Oid partition_id);   /* 2026-10-17 */
extern bool ReadPartWALCheckpoint(Oid partition_id,
                                  PartWALCheckpointFile *out);

/*
 * GetLastWrittenPartitionLSN — scan pg_parwal/<partition_id>/ segment files
 * and return the highest partition_lsn on disk (0 if none).
 * Reads PartWALRecord headers; skips variable-length data portions.
 */
extern uint64 GetLastWrittenPartitionLSN(Oid partition_id);

/*
 * 2026-10-16 多副本组提交（pg_partdist.partwal_group_fsync，默认 on；单副本快速模式不走这里）。
 *
 * 原来每个写入器关闭时都"fsync 段文件 + 写 checkpoint 文件再 fsync"，全在节点级
 * PartWALCtl->lock 之下：一笔 NewOrder 触达 5 个分区、数据与提交标记各一轮，就是
 * 20 次 fsync 串在全节点唯一一把锁上（机械盘上每次 5–10 ms ⇒ 全节点每秒十来笔提交）。
 * 现在：
 *   · 锁内只 write()，编号与段欠账记进节点级 checkpoint 缓存（缓存即权威，文件随缓存
 *     未命中时以"文件 / 扫段"二者取大重建，与单副本快速模式同一套规则）；
 *   · 出锁后 PartWALSyncPartitionUpTo 按分区组提交 fsync —— 一次 fsync 带走并发事务
 *     写下的全部字节，排队者醒来发现已越过自己的编号就直接返回；
 *   · 复制只送"已 fsync"的前缀（get_partition_flush_lsn 返回 synced），leader 自己那一票
 *     仍然意味着本地已落盘，"多数派 ack ⇔ 多数派已持久化"不变。
 */
extern bool   partwal_group_fsync;
extern bool   PartWALLazySync(void);
extern uint64 PartWALWriterFinishLazy(PartitionWALWriter *writer);
extern void   PartWALSyncPartitionUpTo(Oid partition_id, uint64 target_plsn);
extern uint64 PartWALDurablePartitionLSN(Oid partition_id);
extern uint64 PartWALCachedLastPLSN(Oid partition_id);

#endif /* PARTITION_WAL_WRITER_H */
