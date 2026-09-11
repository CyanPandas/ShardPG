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
#include "shard_fileset.h"

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
/* Record class flags —— 记录分类一律以 flags 判定，不以 data_len 判定  */
/* ------------------------------------------------------------------ */

/*
 * 记录分四类，互斥地占 flags 的低 4 位：
 *
 *   DATA   —— 载荷是 leader 原样的 XLogRecord 字节，回放侧交给 rmgr redo。
 *   MARKER —— 载荷是 TxnMarkerPayload（R2-b）：事务的开始/提交/回滚及子事务
 *             清单。事务层靠它把物理记录归拢成一个全局事务，而不是从
 *             RM_XACT 的原始字节里反解 —— leader 的 commit 记录带着 leader
 *             本地的 xid/子事务/时间戳，直接 redo 会污染本节点的 CLOG。
 *   CTRL   —— 控制记录（切主栅栏、段边界、fileset/freeze 同步等），
 *             不参与 redo，只推进游标。
 *   DTX    —— 分布式事务记录（DTX-2PC §5）：载荷是 DtxRecord，子类型放在
 *             头部 info 字段。
 *
 * 未置任何类别位的记录按 DATA 处理 —— 2.0 段流全部落在这一档（其 flags 恒为 0）。
 *
 * 非 DATA 的记录一律不得喂给 rm_redo —— 它们的载荷不是 XLogRecord。
 * IsData 的判定式必须同时排除 MARKER/CTRL/DTX 三个位：漏掉任何一个，该类
 * 记录就会被当作原始 XLogRecord 送进 GetRmgr(rmid).rm_redo。
 */
#define PARTWAL_FLAG_DATA        UINT8_C(0x01)
#define PARTWAL_FLAG_MARKER      UINT8_C(0x02)
#define PARTWAL_FLAG_CTRL        UINT8_C(0x04)
#define PARTWAL_FLAG_DTX         UINT8_C(0x08)

#define PARTWAL_FLAG_CLASS_MASK  UINT8_C(0x0F)

#define PARTWAL_FLAG_NON_DATA_MASK \
    (PARTWAL_FLAG_MARKER | PARTWAL_FLAG_CTRL | PARTWAL_FLAG_DTX)

#define PartWALRecordIsMarker(rec)  (((rec)->flags & PARTWAL_FLAG_MARKER) != 0)
#define PartWALRecordIsCtrl(rec)    (((rec)->flags & PARTWAL_FLAG_CTRL) != 0)
#define PartWALRecordIsDtx(rec)     (((rec)->flags & PARTWAL_FLAG_DTX) != 0)
#define PartWALRecordIsData(rec)    \
    (((rec)->flags & PARTWAL_FLAG_NON_DATA_MASK) == 0)

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
/*
 * U-P5-1（设计 §5.3）：标记额外携带**本分区的分片 xid**，好让 follower 把
 * 分片 clog 也由流重建 —— 设计原文「落账顺序锁定在分片流的 plsn 序上 ⇒
 * 每个副本重放同一个流得到同一本账」。在此之前 follower 只写增强型 clog
 * （按 gxid 索引），`pg_shard_clog/<oid>` 在它上面根本不存在，于是升主后
 * 每个分片 xid 都读成空洞＝RUNNING＝不可见。
 *
 * 兼容做法与 FREEZE_UPDATE 同款：把原来那个「显式补齐、恒为 0」的
 * `reserved` 改作 `flags`。**不含分片写的事务不置位**，载荷字节与既有格式
 * 逐字节相同（R1/R2/TX1 时代的流继续有效）。
 *
 * 只带**一个** xid 而不是 (shard, xid) 对列表：MARKER 是**逐分区**追加的，
 * 而一个分区就是一个分片 —— follower 用自己的本地分片 oid（ctx->shard_oid）
 * 落账即可，不需要跨节点翻译 leader 的 oid。0 = 本分区没有分片写。
 */
#define PARTWAL_MARKER_HAS_SHARD_XID    UINT32_C(0x0001)

/*
 * U-P5-1 之二：再带上 leader 的**发号水位**（下一个待发号）。
 *
 * 为什么光有分片 xid 不够：升主后发号器要知道"在用 xid 的上界"。只靠 clog 里
 * 的终局判决去跳号，覆盖不了这一格 —— 事务的字节被别人的 group commit 顺带
 * 刷进了流、随后本后端**崩溃**（不是中止），于是既没有 COMMIT 也没有 ABORT
 * 标记，而元组已经到了 follower；新主对该号读到空洞，会把它重新发出去，
 * 新事务的判决就落到了那批孤儿元组上。带上水位就没有这一格。
 *
 * 单独占一个标志位而不是扩 0x0001 的尾块：0x0001 今天刚上线，容器里已经有
 * 按 4 字节尾写下的段文件；改尾块尺寸会让那些流的长度校验当场 ERROR。
 * 多一个位比多一次"为什么回放报长度不符"便宜。
 */
#define PARTWAL_MARKER_HAS_ALLOC_WM     UINT32_C(0x0002)
/*
 * PARTWAL_MARKER_STS_IS_TSO —— T7.11（R-P6-18）：`start_ts` 装的是 **TSO 的号**，
 * 不是本地墙钟。
 *
 * 为什么非要一位来分辨：这个字段是**双宇宙**的。TSO 是从 1 开始的小整数，
 * 墙钟是 ~8.4e14 的微秒数，光看值猜量级是能猜，但那是猜。消费侧
 * （`shard_visibility.c` §4.2 三态第一支）拿它与读者快照比：
 *     slot.start_ts <= my_ts
 * 墙钟值恒 > 任何 TSO 快照 ⇒ 第一支恒假 ⇒ **第三支"问协调者"永远不执行**，
 * 已提交的 in-doubt 行就一直不可见。反过来 leader 自己那条
 * `ShardClogSetPrepared(..., TsoGetStartTs(), ...)` 在遗留模式存的是 **0**
 * （见 shard_clog.h「遗留模式 0」的约定）—— 同一个事务，leader 存 0、
 * 副本存墙钟，两边对不上。
 *
 * 置位规则：**值来自 TSO 才置**。不置位就等于"这不是 TSO 宇宙的数"，回放侧
 * 据此给分片 clog 落 0（遗留模式），而 `start_ts` 原值仍照旧进增强型 CLOG
 * 供诊断 —— 两个消费者要的东西不一样，别互相迁就。
 *
 * 不影响载荷长度：长度只由 HAS_SHARD_XID / HAS_ALLOC_WM 决定（见
 * TxnMarkerPayloadTailWords），所以这一位是纯增量的。
 */
#define PARTWAL_MARKER_STS_IS_TSO       UINT32_C(0x0004)
#define PARTWAL_MARKER_KNOWN_FLAGS \
    (PARTWAL_MARKER_HAS_SHARD_XID | PARTWAL_MARKER_HAS_ALLOC_WM | \
     PARTWAL_MARKER_STS_IS_TSO)

typedef struct TxnMarkerPayload
{
    uint64      start_ts;       /* 事务启动时间戳（TSO 就位前取本地 TimestampTz）*/
    uint64      commit_ts;      /* 提交时间戳；ABORT 标记中为 0                  */
    uint32      nsubxacts;      /* 已提交子事务数；无 SAVEPOINT 时为 0           */
    uint32      flags;          /* 原 reserved；旧记录恒 0                       */
    /* TransactionId subxacts[nsubxacts] 紧随其后（leader 侧本地 xid） */
    /* flags & PARTWAL_MARKER_HAS_SHARD_XID 时，其后再跟一个 uint32 分片 xid */
} TxnMarkerPayload;

#define TxnMarkerPayloadSize(nsub) \
    (sizeof(TxnMarkerPayload) + (size_t) (nsub) * sizeof(TransactionId))

/* 尾块按标志位逐个追加：[分片xid][发号水位] */
#define TxnMarkerTailWords(flags) \
    ((((flags) & PARTWAL_MARKER_HAS_SHARD_XID) ? 1 : 0) + \
     (((flags) & PARTWAL_MARKER_HAS_ALLOC_WM)  ? 1 : 0))

#define TxnMarkerPayloadSizeEx(nsub, flags) \
    (TxnMarkerPayloadSize(nsub) + \
     (size_t) TxnMarkerTailWords(flags) * sizeof(uint32))

/* 尾块起点：紧跟在 subxacts[nsubxacts] 之后 */
#define TxnMarkerTail(m) \
    ((uint32 *) ((char *) (m) + TxnMarkerPayloadSize(((const TxnMarkerPayload *) (m))->nsubxacts)))

/* 分片 xid 恒在尾块首位（存在时） */
#define TxnMarkerShardXidPtr(m)     (TxnMarkerTail(m))

/* 发号水位跟在分片 xid 之后（两者都存在时） */
#define TxnMarkerAllocWmPtr(m) \
    (TxnMarkerTail(m) + \
     ((((const TxnMarkerPayload *) (m))->flags & PARTWAL_MARKER_HAS_SHARD_XID) ? 1 : 0))


#define TxnMarkerSubxacts(m) \
    ((TransactionId *) ((char *) (m) + sizeof(TxnMarkerPayload)))

/* ------------------------------------------------------------------ */
/* CTRL 记录：控制通道（FRD §7.7/§12）                                  */
/* ------------------------------------------------------------------ */

/*
 * CTRL 记录的头部字段约定：
 *   flags 含 PARTWAL_FLAG_CTRL；rmid = PARTWAL_CTRL_RMID；info = opcode。
 *
 * rmid 用 0xFF 这个**哨兵值**而不是某个真实 rmgr id：CTRL 记录压根不进
 * rm_redo，派发判据是 flags 的类别位（见 PartWALRecordIsCtrl）。把它设成
 * 一个不可能被当成 rmgr 用的值，是为了万一将来有人误按 rmid 分派时立刻炸掉，
 * 而不是安静地走进某个 rmgr 的 redo。
 *
 * gxid 取发出该记录的事务的 gxid（DDL 事务），仅供排障对账；CTRL 的应用
 * **不经过**事务判决 —— 它在流内是无条件生效的（见 §12 的已知窗口）。
 */
#define PARTWAL_CTRL_RMID            UINT8_C(0xFF)

/* opcode（放在头部的 info 字段） */
#define PARTWAL_CTRL_FILESET_UPDATE  UINT8_C(0x01)
#define PARTWAL_CTRL_FREEZE_UPDATE   UINT8_C(0x02)

/*
 * T7.2（R-P6-17，2026-09-09）：**分片 clog 随物理基线一起搬**。
 *
 * 缺陷：`shard_baseline_emit` 只灌页面 + 抬发号水位，不搬 `pg_shard_clog/<oid>`；
 * 而基线游标之前的 MARKER 不再回放 ⇒ 在**已有数据之后**才供给的副本，对基线
 * 之前提交的每一个分片 xid 都**没有判决** ⇒ 那些行读成 RUNNING（不可见），
 * 该副本一旦升主，`shard_claim_on_promote` 还会把它们改判 ABORTED ⇒ **丢行**。
 * 此前只能靠"先供给、后写数据"的运维纪律绕过去。
 *
 * 载荷：一段**连续**的分片 clog 槽（每槽 32 字节，与 ShardClogSlot 逐字节同构）。
 * 基线发射方按块切（见 SHARD_CLOG_BASELINE_CHUNK），follower 收到就原样写进
 * 自己的 `pg_shard_clog/<本地 oid>` —— 键是分片 xid，与 oid 无关，所以不需要
 * 任何翻译。幂等：同一块重放两次写的是同样的字节。
 *
 * 顺序：必须排在 FULL_BASELINE 的 CTRL **之后**（follower 先换表再落账），
 * 与那批 FPI 谁先谁后都不影响正确性（clog 与页面互不引用）。
 */
#define PARTWAL_CTRL_SHARD_CLOG      UINT8_C(0x03)

/*
 * T7.8（P7-D1，2026-09-09）：**leader DROP 了这个分片**。
 *
 * 缺陷：leader 侧 DROP TABLE 之后，副本一侧**完全静默** —— 壳表、回放槽位、
 * `pg_parwal/<oid>` 目录全都留着，永不回收。回收判据是"OID 不在本地 pg_class"，
 * 而副本的壳表恰恰是本地真表，判据天然不成立（`shard_fileset.c` 原注释：
 * "副本侧的处置需要另一个 opcode，属后续工作；这里保持沉默"）。
 *
 * 语义：收到即"本分片到此为止"。follower 的处置是**停流 + 摘槽位**，
 * 但**不删壳表也不删目录** —— 删表是 DDL，副本上没人授权它做；而且运维可能
 * 正想留着那份数据做取证。摘掉槽位与 armed 之后，那张壳表就是一张普通本地表，
 * `DROP TABLE` 由运维决定什么时候执行（回收动作因此是**可审计**的）。
 *
 * 载荷为空（分片 oid 已在记录头里）。幂等：重复应用就是重复停流。
 */
#define PARTWAL_CTRL_SHARD_DROP      UINT8_C(0x04)

typedef struct PartWALCtrlShardClog
{
    uint32      first_sxid;     /* 本块第一个分片 xid                        */
    uint32      nslots;         /* 本块槽数                                  */
    /* 其后紧跟 nslots 个 32 字节槽（与 ShardClogSlot 同构） */
} PartWALCtrlShardClog;

#define PARTWAL_SHARD_CLOG_SLOT_BYTES  32

#define PartWALCtrlShardClogSize(n) \
    (sizeof(PartWALCtrlShardClog) + \
     (size_t) (n) * PARTWAL_SHARD_CLOG_SLOT_BYTES)

#define PartWALCtrlShardClogSlots(p) \
    ((char *) (p) + sizeof(PartWALCtrlShardClog))

/*
 * FILESET_UPDATE 载荷：leader 侧 fileset 变更后的**全量**新描述。
 *
 * 为什么是全量而不是增量：follower 侧的应用必须幂等（崩溃后从游标重放会
 * 再次走到这条记录），全量描述天然幂等 —— 无论应用几次，结果都是"loc_map
 * 等于这份描述"。增量描述要求恰好应用一次，与 §8.4 的重放语义冲突。
 *
 * flags 里 NEEDS_REBASELINE 的含义：leader 判断新文件太大、不适合把内容
 * 以 FPI 形式灌进流（见 GUC pg_partdist.fileset_inline_max_blocks），
 * 于是只发结构变更通知，内容要 follower 自己重做物理基线。follower 见到
 * 该位一律停在栅栏上，不会把"结构换了但内容没跟上"的状态当成正常。
 *
 * reserved 是**显式**补齐位（同 TxnMarkerPayload 的理由：结构体直写磁盘，
 * 未初始化的编译器填充会让同一逻辑内容产生不同字节）。
 *
 * data_len == sizeof(PartWALCtrlFilesetUpdate) + nrels * sizeof(ShardFileSetRel)
 *          == 8 + 16 * nrels
 */
typedef struct PartWALCtrlFilesetUpdate
{
    uint32      nrels;          /* 新 fileset 的成员数                       */
    uint16      flags;          /* PARTWAL_FSUPD_*                           */
    uint16      reserved;       /* 显式补齐，恒为 0                          */
    /* ShardFileSetRel rels[nrels] 紧随其后（leader 侧文件号 + role/ord） */
} PartWALCtrlFilesetUpdate;

#define PARTWAL_FSUPD_NEEDS_REBASELINE  UINT16_C(0x0001)

/*
 * T6.1（P6）：**全量物理基线**。
 *
 * 语义：本条 CTRL 之后紧跟着的是该 shard **全部成员、全部 fork 的完整 FPI**，
 * 而不是"只有变了的那几个成员"。follower 收到它时必须把 fileset 里**每一个**
 * 本地文件截成 0 块，再让后面那批 FPI 把内容重建出来。
 *
 * 为什么复用 FILESET_UPDATE 而不新开 opcode：这条记录要做的事
 * （宣告 fileset + 截断 + 等 FPI 灌内容）与 DDL 变更**逐字一致**，差别只在
 * "截断哪些成员"。新开 opcode 等于把同一段 follower 逻辑抄第二遍。
 *
 * 与 NEEDS_REBASELINE 的关系：那一位是"内容**没**随流来，你得自己想办法"；
 * 这一位是"内容**全部**随流来了"。两位互斥，同时置位即协议错误。
 */
#define PARTWAL_FSUPD_FULL_BASELINE     UINT16_C(0x0002)

/*
 * T7.3（R-P6-16，2026-09-09）：**主权交接**的文件号重绑。
 *
 * 语义：分区换主了。新主的写入此后带的是**它自己的** relfilenumber，而其余副本
 * 的 locmap 仍然对着旧主的号 —— 不重绑就报「未知 relfilelocator（fileset 漏
 * 登记）」，该副本从此放不了新主的流，也就**失去再次当选的资格**，直到从新主
 * 重新供给。这正是 R-P6-16。
 *
 * 与另外两位的**本质区别：内容一个字节都没变**。副本手上的文件是它自己回放出来
 * 的、与新主同源；要换的只是"leader 侧文件号 → 本地文件号"这张映射。所以
 * follower 收到这一位时：
 *   · **绝不截断**任何本地文件（DDL/基线那两条路径都截，这里截了就是把副本
 *     的数据清空后等一批永远不会来的 FPI）；
 *   · 不推进 base_part_lsn（配对的起效游标没有变化）；
 *   · 只按 (role, ord) 重建 locmap 的配对。
 *
 * 与 NEEDS_REBASELINE / FULL_BASELINE 三者互斥：那两位说的是"内容要重来"，
 * 这一位说的是"内容不动、只换号"。
 */
#define PARTWAL_FSUPD_PRIMARY_HANDOVER  UINT16_C(0x0004)

#define PARTWAL_FSUPD_KNOWN_FLAGS \
    (PARTWAL_FSUPD_NEEDS_REBASELINE | PARTWAL_FSUPD_FULL_BASELINE | \
     PARTWAL_FSUPD_PRIMARY_HANDOVER)

#define PartWALCtrlFilesetUpdateSize(n) \
    (sizeof(PartWALCtrlFilesetUpdate) + (size_t) (n) * sizeof(ShardFileSetRel))

#define PartWALCtrlFilesetRels(u) \
    ((ShardFileSetRel *) ((char *) (u) + sizeof(PartWALCtrlFilesetUpdate)))

StaticAssertDecl(sizeof(PartWALCtrlFilesetUpdate) == 8,
                 "PartWALCtrlFilesetUpdate 必须是 8 字节（CTRL 磁盘格式）");
StaticAssertDecl(sizeof(ShardFileSetRel) == 16,
                 "ShardFileSetRel 必须是 16 字节且无填充洞（随 CTRL 直写磁盘）");

/* ------------------------------------------------------------------ */
/* FREEZE_UPDATE 载荷：leader 的冻结账目（FRD §13 约束 5，D2）          */
/* ------------------------------------------------------------------ */

/*
 * 为什么要同步这两个**目录字段**：
 *
 * 副本壳表的元组物理上确实被冻结了（leader 的 freeze 记录随流回放，元组字节
 * 两侧一致），但 `pg_class.relfrozenxid` 这个目录字段在 follower 上没人维护 ——
 * 它停在建壳表那一刻的值。而 follower 的 nextXid 会被回放水位不断拉高
 * （§7.5），于是 age(relfrozenxid) 无界增长，迟早越过
 * autovacuum_freeze_max_age。
 *
 * 越过之后 **autovacuum_enabled = off 会被忽略**（autovacuum.c:3196
 * `if (!av_enabled && !force_vacuum)`，§13 约束 5 已实测），副本壳表照样被
 * 强制 anti-wraparound vacuum 扫到 —— 而它的元组带的是**外来节点**的 xid，
 * 本地 clog 对它们一无所知。更糟的是那次 vacuum 会写本地 WAL 碰副本文件，
 * 把 §13 约束 12 那个洞重新打开。
 *
 * 为什么直接搬 leader 的原值是**真话**（这正是本方案优于"直接推本地值"之处）：
 * relfrozenxid = X 的语义是"本关系内不存在比 X 更老的未冻结 xid"。副本的堆页
 * 与 leader 逐字节一致 ⇒ 同一句话在 follower 上同样成立。搬过来不是编造，
 * 而是把一个本来就为真、只是没人写下来的事实补上。
 *
 * 只对**堆**有意义：索引的 pg_class.relfrozenxid 恒为 0。所以载荷里只会出现
 * SHARD_REL_MAIN 与 SHARD_REL_TOAST 两种 role。
 *
 * data_len == sizeof(PartWALCtrlFreezeUpdate) + nrels * sizeof(PartWALFreezeEntry)
 *          == 8 + 12 * nrels
 */
typedef struct PartWALFreezeEntry
{
    uint8       role;           /* ShardRelRole：只可能是 MAIN 或 TOAST */
    uint8       ord;
    uint16      reserved;       /* 显式补齐，恒为 0 */
    uint32      relfrozenxid;   /* leader 的 pg_class.relfrozenxid */
    uint32      relminmxid;     /* leader 的 pg_class.relminmxid   */
} PartWALFreezeEntry;

/*
 * T5.4b-2（设计 §6.7）：分片 vacuum 的两个水位也走这条通道。
 *
 * **不新增 opcode，复用 FREEZE_UPDATE 换语义** —— 设计原文如此，而语义上也
 * 恰好贴切：`clog_truncate_before` 就是分片 xid 宇宙里的**隐式 freeze 点**，
 * 与 relfrozenxid 是同一类"leader 的冻结账目"。
 *
 * 兼容做法：把原来那个"显式补齐、恒为 0"的 reserved 字段改作 flags。
 * 旧记录 flags == 0 ⇒ 没有尾块 ⇒ 长度校验与旧算法逐字节等价，
 * R1/D2 时代写下的 FREEZE_UPDATE 继续有效。置位时在 rels[] 之后追加一个
 * 8 字节的 PartWALFreezeVacuumWm。
 *
 * 两本账相互独立：vacuum 截断发的记录 nrels == 0（只带水位块），
 * D2 的冻结账目发的记录 flags == 0（只带 rels）。因此 nrels 允许为 0，
 * 但**仅当**水位块存在 —— 两者皆空的记录没有意义，仍旧拒收。
 */
#define PARTWAL_FREEZE_HAS_VACUUM_WM    UINT32_C(0x0001)

typedef struct PartWALCtrlFreezeUpdate
{
    uint32      nrels;
    uint32      flags;          /* 原 reserved；旧记录恒 0 */
    /* PartWALFreezeEntry rels[nrels] 紧随其后 */
    /* flags & PARTWAL_FREEZE_HAS_VACUUM_WM 时，其后再跟一个 PartWALFreezeVacuumWm */
} PartWALCtrlFreezeUpdate;

typedef struct PartWALFreezeVacuumWm
{
    uint32      clog_truncate_before;   /* 免查隐式冻结区上界（开区间） */
    uint32      shard_vacuum_xid;       /* 两态恢复标记 */
} PartWALFreezeVacuumWm;

#define PartWALCtrlFreezeUpdateSize(n) \
    (sizeof(PartWALCtrlFreezeUpdate) + (size_t) (n) * sizeof(PartWALFreezeEntry))

#define PartWALCtrlFreezeUpdateSizeEx(n, has_wm) \
    (PartWALCtrlFreezeUpdateSize(n) + \
     ((has_wm) ? sizeof(PartWALFreezeVacuumWm) : 0))

/* 水位尾块的位置：紧跟在 rels[nrels] 之后 */
#define PartWALCtrlFreezeVacuumWm(u) \
    ((PartWALFreezeVacuumWm *) ((char *) (u) + \
        PartWALCtrlFreezeUpdateSize(((const PartWALCtrlFreezeUpdate *) (u))->nrels)))

#define PartWALCtrlFreezeRels(u) \
    ((PartWALFreezeEntry *) ((char *) (u) + sizeof(PartWALCtrlFreezeUpdate)))

StaticAssertDecl(sizeof(PartWALCtrlFreezeUpdate) == 8,
                 "PartWALCtrlFreezeUpdate 必须是 8 字节（CTRL 磁盘格式）");
StaticAssertDecl(sizeof(PartWALFreezeEntry) == 12,
                 "PartWALFreezeEntry 必须是 12 字节且无填充洞");
StaticAssertDecl(sizeof(PartWALFreezeVacuumWm) == 8,
                 "PartWALFreezeVacuumWm 必须是 8 字节（CTRL 磁盘格式）");

#endif /* PARTITION_WAL_HEADER_H */
