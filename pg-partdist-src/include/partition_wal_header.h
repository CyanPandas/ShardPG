/*
 * partition_wal_header.h
 * On-disk record layout for pg_parwal segment files (parwal-2.0).
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

/* ------------------------------------------------------------------ */
/* Magic and constants                                                  */
/* ------------------------------------------------------------------ */

#define PARTWAL_MAGIC            UINT32_C(0x50415254)   /* "PART" */
#define PARTWAL_CHECKPOINT_MAGIC UINT32_C(0x43484B50)   /* "CHKP" */

/* ------------------------------------------------------------------ */
/* PartWALRecord — new self-contained on-disk record (parwal-2.0)      */
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
 * version:        record format version (1=legacy _pad, 2=xid present).
 * flags:          reserved for future use (e.g. PARTWAL_FLAG_TOAST_FLAT).
 * data_len:       bytes of raw XLogRecord payload following this header.
 * xid:            transaction ID of the originating transaction.
 *                 Used by follower replay to group multi-record transactions
 *                 into a single local commit.  Stored in the header (not
 *                 parsed from the body) so group-commit records with
 *                 data_len=0 still carry a usable xid.
 */
typedef struct PartWALRecord
{
    uint32          magic;          /* PARTWAL_MAGIC = 0x50415254               */
    Oid             partition_id;   /* which partition this record belongs to   */
    XLogRecPtr      orig_lsn;       /* original pg_wal LSN                      */
    uint64          partition_lsn;  /* per-partition monotone sequence (1-based)*/
    uint8           rmid;           /* XLog resource manager ID                 */
    uint8           info;           /* XLog info flags                          */
    uint8           version;        /* format version: PARTWAL_RECORD_VERSION_* */
    uint8           flags;          /* PARTWAL_FLAG_* bits (reserved, set to 0) */
    uint32          data_len;       /* bytes of raw WAL data following header   */
    TransactionId   xid;            /* originating transaction ID               */
} PartWALRecord;

/* Record format versions */
#define PARTWAL_RECORD_VERSION_1    UINT8_C(1)   /* legacy: _pad occupied bytes 26-27 */
#define PARTWAL_RECORD_VERSION_2    UINT8_C(2)   /* current: version+flags+xid present */

/* ------------------------------------------------------------------ */
/* 记录分类：一律以 flags 判定，不以 data_len 判定                      */
/*                                                                      */
/* FRD §4.1 定的格式契约。此前 flags 恒为 0、分类靠"载荷是不是原始      */
/* XLogRecord"隐式判断；DTX-2PC 引入非 WAL 载荷的记录后必须显式化。     */
/*                                                                      */
/* 兼容性：**flags == 0 视同 DATA**（存量段文件里的记录都是 0）。       */
/* 新写入的数据记录会显式带上 PARTWAL_FLAG_DATA，判定请用               */
/* PartWALRecordIsData() 而不是直接比较。                               */
/* ------------------------------------------------------------------ */

#define PARTWAL_FLAG_DATA        UINT8_C(0x01)   /* 载荷 = 原始 XLogRecord 字节 */
#define PARTWAL_FLAG_MARKER      UINT8_C(0x02)   /* FRD 预留：单机事务标记(R2)  */
#define PARTWAL_FLAG_CTRL        UINT8_C(0x04)   /* FRD 预留：控制记录(FRD §12) */
#define PARTWAL_FLAG_DTX         UINT8_C(0x08)   /* 分布式事务记录(DTX-2PC §5)  */

/* 非 DATA 的记录一律不得喂给 rm_redo —— 它们的载荷不是 XLogRecord */
#define PARTWAL_FLAG_NON_DATA_MASK \
    (PARTWAL_FLAG_MARKER | PARTWAL_FLAG_CTRL | PARTWAL_FLAG_DTX)

#define PartWALRecordIsData(flags) \
    (((flags) & PARTWAL_FLAG_NON_DATA_MASK) == 0)

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

#endif /* PARTITION_WAL_HEADER_H */
