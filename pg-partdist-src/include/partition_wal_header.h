/*
 * partition_wal_header.h
 * On-disk record layout for pg_parwal segment files (parwal-3.0).
 *
 * Every record in a pg_parwal/<partition_id>/<segname> file begins with a
 * PartWALRecord header, followed by data_len bytes of raw WAL record content.
 *
 * A per-partition checkpoint file ("pg_parwal/<partition_id>/checkpoint")
 * stores recovery metadata (relfilenode, last processed LSN, last partition LSN).
 */
#ifndef PARTITION_WAL_HEADER_H
#define PARTITION_WAL_HEADER_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "global_mvcc.h"

/* ------------------------------------------------------------------ */
/* Magic and constants                                                  */
/* ------------------------------------------------------------------ */

#define PARTWAL_MAGIC            UINT32_C(0x50415254)   /* "PART" */
#define PARTWAL_CHECKPOINT_MAGIC UINT32_C(0x43484B50)   /* "CHKP" */

/* ------------------------------------------------------------------ */
/* PartWALRecord — self-contained on-disk record (parwal-3.0)          */
/* ------------------------------------------------------------------ */

/*
 * PartWALRecord
 *
 * Every record written to pg_parwal/<partition_id>/<segname> begins with
 * this fixed-size header, immediately followed by data_len bytes of raw
 * XLogRecord content (copied verbatim from pg_wal).
 *
 * orig_lsn:       original pg_wal LSN of the record.
 * partition_lsn:  per-partition strictly-monotone counter (1-based).
 *                 Tests check this by name; it MUST be preserved exactly.
 * rmid:           XLog resource manager ID of the original record.
 * info:           XLog info flags of the original record.
 * version:        record format version (PARTWAL_RECORD_VERSION_*).
 * flags:          PARTWAL_FLAG_* bits — record class (DATA / MARKER / CTRL).
 * data_len:       bytes of payload following this header.  For DATA records
 *                 this is the raw XLogRecord; for MARKER records it is a
 *                 TxnMarkerPayload (R2-b).
 * gxid:           **全局**事务标识（FRD §9.1）：高 16 位来源节点号，低 48 位
 *                 该节点的本地 xid。2.0 时代这里是 32 位本地 xid —— 一个节点
 *                 承载多个 leader 的副本时，两个 leader 的 xid 1000 无法区分，
 *                 事务层（xid_map / pg_gclog）就没法记账。
 *                 放在头部而不是从 body 解析：group-commit 的 data_len=0
 *                 记录也必须带得动事务号。
 *
 * 布局（sizeof = 40，与 2.0 逐字节等长）：magic 0 / partition_id 4 /
 * orig_lsn 8 / partition_lsn 16 / rmid 24 / info 25 / version 26 / flags 27 /
 * data_len 28 / gxid 32。2.0 的 xid 占 32..35、36..39 是填充，3.0 把这 8 字节
 * 合成一个 uint64，因此**记录大小与所有既有偏移都不变**，只有 32..39 的语义变了。
 */
typedef struct PartWALRecord
{
    uint32                  magic;          /* PARTWAL_MAGIC = 0x50415254       */
    Oid                     partition_id;   /* which partition this belongs to  */
    XLogRecPtr              orig_lsn;       /* original pg_wal LSN              */
    uint64                  partition_lsn;  /* per-partition sequence (1-based) */
    uint8                   rmid;           /* XLog resource manager ID         */
    uint8                   info;           /* XLog info flags                  */
    uint8                   version;        /* PARTWAL_RECORD_VERSION_*         */
    uint8                   flags;          /* PARTWAL_FLAG_* bits              */
    uint32                  data_len;       /* bytes of payload after header    */
    GlobalTransactionId     gxid;           /* (node_id << 48) | local xid      */
} PartWALRecord;

/* Record format versions */
#define PARTWAL_RECORD_VERSION_1    UINT8_C(1)   /* legacy: _pad occupied bytes 26-27 */
#define PARTWAL_RECORD_VERSION_2    UINT8_C(2)   /* parwal-2.0: 32-bit local xid      */
#define PARTWAL_RECORD_VERSION_3    UINT8_C(3)   /* parwal-3.0: 64-bit gxid + flags   */

/* ------------------------------------------------------------------ */
/* PartWALCheckpointFile — per-partition checkpoint                    */
/* ------------------------------------------------------------------ */

/*
 * PartWALCheckpointFile
 *
 * Stored at pg_parwal/<partition_id>/checkpoint.
 * Read by the demux on startup for crash recovery: if last_wal_lsn is
 * valid and less than current flush LSN, rescan WAL from last_wal_lsn
 * forward for this partition's relfilenode.
 */
typedef struct PartWALCheckpointFile
{
    uint32          magic;          /* PARTWAL_CHECKPOINT_MAGIC             */
    RelFileNumber   relfilenode;    /* relfilenode of this partition table  */
    XLogRecPtr      last_wal_lsn;  /* last pg_wal LSN processed            */
    uint64          last_part_lsn; /* last partition_lsn written           */
} PartWALCheckpointFile;

/* ------------------------------------------------------------------ */
/* Backward-compatibility aliases used by SQL read functions           */
/* ------------------------------------------------------------------ */

/*
 * The SQL functions check_partition_wal / verify_partition_wal /
 * count_parwal_records / read_all_headers read PartWALRecord structs
 * from pg_parwal files.  They access the header fields directly.
 * No separate "PartWALHeader" type is needed in 2.0.
 */

/* ------------------------------------------------------------------ */
/* Record class flags                                                   */
/* ------------------------------------------------------------------ */

/*
 * 记录分三类，互斥地占 flags 的低 3 位：
 *
 *   DATA   —— 载荷是 leader 原样的 XLogRecord 字节，回放侧交给 rmgr redo。
 *   MARKER —— 载荷是 TxnMarkerPayload（R2-b）：事务的开始/提交/回滚及子事务
 *             清单。事务层靠它把物理记录归拢成一个全局事务，而不是从
 *             RM_XACT 的原始字节里反解 —— leader 的 commit 记录带着 leader
 *             本地的 xid/子事务/时间戳，直接 redo 会污染本节点的 CLOG。
 *   CTRL   —— 控制记录（切主栅栏、段边界等），不参与 redo，只推进游标。
 *
 * 未置任何类别位的记录按 DATA 处理 —— 2.0 段流全部落在这一档（其 flags 恒为 0）。
 */
#define PARTWAL_FLAG_DATA        UINT8_C(0x01)
#define PARTWAL_FLAG_MARKER      UINT8_C(0x02)
#define PARTWAL_FLAG_CTRL        UINT8_C(0x04)

#define PARTWAL_FLAG_CLASS_MASK  UINT8_C(0x07)

#define PartWALRecordIsMarker(rec)  (((rec)->flags & PARTWAL_FLAG_MARKER) != 0)
#define PartWALRecordIsCtrl(rec)    (((rec)->flags & PARTWAL_FLAG_CTRL) != 0)
#define PartWALRecordIsData(rec)    \
    (((rec)->flags & (PARTWAL_FLAG_MARKER | PARTWAL_FLAG_CTRL)) == 0)

/*
 * PartWALRecordGxid — 统一取事务号，屏蔽 2.0/3.0 的差异。
 *
 * 2.0 记录的 32..35 是 32 位本地 xid、36..39 是**未初始化的结构体填充**，
 * 按 uint64 整读会带进垃圾高位。所以低版本走定长 4 字节读（该偏移与字节序
 * 无关地就是旧的 xid 字段），节点号补 0。
 *
 * 注意判据是 **version**，不是节点号：协调节点的 Citus group id 就是 0，
 * 于是"node 0 的 gxid"既可能是 2.0 兼容值、也可能是协调节点的真实事务。
 * 要判断一条记录是否带得动事务层信息，一律看 version >= 3。
 */
static inline GlobalTransactionId
PartWALRecordGxid(const PartWALRecord *rec)
{
    uint32  xid32;

    if (rec->version >= PARTWAL_RECORD_VERSION_3)
        return rec->gxid;

    memcpy(&xid32, ((const char *) rec) + offsetof(PartWALRecord, gxid),
           sizeof(uint32));
    return (GlobalTransactionId) xid32;
}

/* ------------------------------------------------------------------ */
/* TxnMarkerPayload — MARKER 记录的载荷（FRD §4.3）                     */
/* ------------------------------------------------------------------ */

/*
 * 事务标记记录的载荷，由 leader 的提交/中止路径生成。
 *
 * 头部字段的约定：
 *   flags 含 PARTWAL_FLAG_MARKER；rmid = RM_XACT_ID；
 *   info  = XLOG_XACT_COMMIT 或 XLOG_XACT_ABORT（按 info & XLOG_XACT_OPMASK 判别）；
 *   gxid  = 顶层事务的全局事务号；子事务的 gxid 由 follower 按
 *           MakeGlobalXid(GxidNodeId(header->gxid), subxacts[i]) 合成。
 *
 * 为什么不透传 leader 原始的 RM_XACT 记录：那条记录里的 xid、子事务清单、
 * 时间戳全是 **leader 本地**的，xact_redo 会直接写本节点的原生 CLOG —— 而
 * 回放引入的 xid 在本地 CLOG 里根本没有对应页（§7.6）。标记记录把这些信息
 * 显式重述一遍，交给增强型 CLOG 记账。
 *
 * reserved 是**显式**补齐位，不是编译器填充：结构体整体直写磁盘，留未初始化
 * 的填充洞会让同一逻辑内容产生不同字节（parwal-2.0 的 xid 尾部填充就踩过这个
 * 坑，见 PartWALRecordGxid 的注释）。
 *
 * data_len == sizeof(TxnMarkerPayload) + nsubxacts * sizeof(TransactionId)
 *          == 24 + 4 * nsubxacts
 */
typedef struct TxnMarkerPayload
{
    uint64      start_ts;       /* 事务启动时间戳（TSO 就位前取本地 TimestampTz）*/
    uint64      commit_ts;      /* 提交时间戳；ABORT 标记中为 0                  */
    uint32      nsubxacts;      /* 已提交子事务数；无 SAVEPOINT 时为 0           */
    uint32      reserved;       /* 显式补齐，恒为 0                              */
    /* TransactionId subxacts[nsubxacts] 紧随其后（leader 侧本地 xid） */
} TxnMarkerPayload;

#define TxnMarkerPayloadSize(nsub) \
    (sizeof(TxnMarkerPayload) + (size_t) (nsub) * sizeof(TransactionId))

#define TxnMarkerSubxacts(m) \
    ((TransactionId *) ((char *) (m) + sizeof(TxnMarkerPayload)))

#endif /* PARTITION_WAL_HEADER_H */
