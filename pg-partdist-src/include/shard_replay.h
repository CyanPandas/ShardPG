/*
 * shard_replay.h
 *
 * Follower 物理回放（FRD §7，R1 子集：纯物理回放闭环，无 xid_map）。
 *
 * 进程形态（FRD §7 + §13.10）：
 *   - launcher（静态注册的 bgworker）扫描 ReplayCtl 槽位，按需拉起
 *     replay worker（动态 bgworker，池大小 = GUC pg_partdist.replay_workers）。
 *   - worker 认领 shard 走排他 CAS（claimed_by == 0 → MyProcPid），
 *     回放循环入口断言持有认领权 —— EB_SKIP_EXTENSION_LOCK 写死在
 *     xlogutils.c 扩展文件路径里，单写者破防 = 静默堆损坏。
 *
 * loc_map（leader 文件号 → 本地文件号）经 SQL 注册并持久化到
 * pg_parwal/<shard_oid>/locmap，worker 无 catalog 也能重建。
 */
#ifndef SHARD_REPLAY_H
#define SHARD_REPLAY_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "port/pg_crc32c.h"
#include "storage/lwlock.h"
#include "storage/relfilelocator.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "shard_fileset.h"
#include "shard_xidmap.h"

/* ------------------------------------------------------------------ */
/* loc_map                                                              */
/* ------------------------------------------------------------------ */

typedef struct LocMapEntry
{
    RelFileLocator  leader_loc;     /* hash key：record 里携带的就是它 */
    RelFileLocator  local_loc;
} LocMapEntry;

#define REPLAY_LOCMAP_MAGIC     UINT32_C(0x4C4D4150)    /* "LMAP" */
#define REPLAY_LOCMAP_FILENAME  "locmap"

typedef struct ReplayLocMapFile
{
    uint32          magic;
    Oid             shard_oid;      /* 本地 shard（分区）OID = 目录名 */
    int32           npairs;
    LocMapEntry     pairs[SHARD_FILESET_MAX_RELS];
} ReplayLocMapFile;

/* ------------------------------------------------------------------ */
/* apply checkpoint（FRD §8.4）                                        */
/* ------------------------------------------------------------------ */

/*
 * 文件布局：ShardApplyCheckpoint 头 + nxidmap 个 XidMapEntry。
 *
 * CRC 覆盖「头（crc 字段置 0）+ 全部条目」—— 只校验头的话，快照被截断
 * 或写坏时头依旧自洽，重启后 xid_map 静默残缺，可见性判定就此错位。
 *
 * 版本号仍是 3：nxidmap == 0 时新算法与旧的"只算头"逐字节等价，
 * 所以 R1 时代写下的 checkpoint（nxidmap 恒 0）继续有效，
 * 不会因为升级 R2 就把所有 follower 打回从头重放。
 */
#define APPLY_CHECKPOINT_MAGIC     UINT32_C(0x4143484B) /* "ACHK" */
#define APPLY_CHECKPOINT_VERSION   3
#define APPLY_CHECKPOINT_FILENAME  "apply_checkpoint"

typedef struct ShardApplyCheckpoint
{
    uint32          magic;              /* APPLY_CHECKPOINT_MAGIC            */
    uint32          version;            /* APPLY_CHECKPOINT_VERSION          */
    Oid             shard_oid;
    uint64          durable_part_lsn;   /* 此游标前的效果已全部持久化        */
    XLogRecPtr      max_orig_lsn;       /* 已应用的最大 leader end LSN(§11)  */
    uint64          max_replayed_fxid;  /* 升主水位 W 的持久化来源（§7.5）   */
    uint64          resume_segno;       /* 恢复扫描起点提示（仅优化）        */
    uint64          resume_offset;
    uint32          nxidmap;            /* 随后的 xid_map 快照条目数         */
    uint32          crc;                /* 头(crc=0) + 全部条目的 CRC32C     */
} ShardApplyCheckpoint;

/*
 * entries：读时若非 NULL，成功后 *entries 指向 palloc 出来的 nxidmap 条
 * 快照（nxidmap == 0 时为 NULL），由调用方 pfree；写时为待落盘的快照，
 * chk->nxidmap == 0 时可传 NULL。
 */
extern bool ReadApplyCheckpoint(Oid shard_oid, ShardApplyCheckpoint *out,
                                XidMapEntry **entries);
extern void WriteApplyCheckpoint(const ShardApplyCheckpoint *chk,
                                 const XidMapEntry *entries);

/* ------------------------------------------------------------------ */
/* 回放上下文（FRD §7.1，R1 子集）                                     */
/* ------------------------------------------------------------------ */

typedef struct ShardReplayCtx
{
    Oid                shard_oid;
    HTAB              *loc_map;            /* leader_loc → local_loc          */
    int                nlocal;             /* 本地文件（去重后）数            */
    RelFileLocator     local_locs[SHARD_FILESET_MAX_RELS];
    XLogReaderState   *reader;             /* 仅作 DecodeXLogRecord 容器，
                                            * 读回调全 NULL                   */
    HTAB              *xid_map;            /* local_xid → gxid（§9.2）        */
    FullTransactionId  max_replayed_fxid;  /* 已回放的最大 xid（64 位，升主
                                            * 水位 W 的来源）。用 64 位是因为
                                            * 32 位跨 epoch 回卷后 "xid <= W"
                                            * 的比较会失效，而路由表里的
                                            * watermark 本就是 FullTransactionId,
                                            * 两处类型必须一致（§7.1 注）     */
    uint64             applied_part_lsn;   /* 已应用游标（内存值）            */
    uint64             durable_part_lsn;   /* 已持久化游标                    */
    XLogRecPtr         max_orig_lsn;       /* 已应用的最大 leader end LSN     */
    uint64             records_since_ckpt;
    TimestampTz        last_ckpt_time;

    /* 段文件流读取状态 */
    int                seg_fd;
    uint64             seg_segno;          /* 当前段文件号（文件名序）        */
    off_t              seg_off;
} ShardReplayCtx;

/*
 * 触发式追平（惰性回放的入口）。
 *
 * pg_raft 在选举胜出、准备把该分区提升为 primary 时调用它：把 bound 设成
 * **该组已提交的最大 partition_lsn（commit_index）**，同步等待追平完成，
 * 之后才对外服务。这样"回放上界"在触发时刻由调用方精确给定，不需要回放
 * 模块自己去猜 —— 持续回放形态下"可能放到未提交条目"的风险不复存在。
 *
 * pg_partdist 在 _PG_init 把本函数指针写进 rendezvous variable
 * "partdist_replay_catchup_hook"，pg_raft 取用即可，二者无编译期依赖。
 *
 * 返回追平后的 applied_part_lsn；失败 ereport(ERROR)。
 */
extern uint64 ShardReplayCatchUp(Oid shard_oid, uint64 bound, int timeout_ms);

/* ------------------------------------------------------------------ */
/* 事务号水位（FRD §7.5）                                              */
/* ------------------------------------------------------------------ */

/*
 * 把本地 nextXid 拉到严格大于 xid。
 *
 * 不能直接用核内的 AdvanceNextFullTransactionIdPastXid()：varsup.c 里那个
 * 带 Assert(AmStartupProcess() || !IsUnderPostmaster) —— 它无锁读 nextXid，
 * 只对 startup 进程安全。replay worker 是普通 bgworker，assert 构建下直接崩。
 * 本变体逻辑相同，但读-改-写全程持 XidGenLock。
 *
 * 目的：本地后续分配的 xid 必须严格大于所有回放引入的 xid，否则升主后
 * "xid <= W → 查 xid_map"的判定会把新事务误路由到旧 leader 的命名空间。
 */
extern void PartDistAdvanceNextXidPastXid(TransactionId xid);

/* 逐条记录登记：更新 xid_map 并抬高 ctx->max_replayed_fxid */
extern void ShardReplayNoteXid(ShardReplayCtx *ctx, GlobalTransactionId gxid);

extern void ShardReplayInitXidMap(ShardReplayCtx *ctx);
extern void ShardReplayRestoreXidMap(ShardReplayCtx *ctx,
                                     const XidMapEntry *ents, uint32 nents);
extern void ShardReplayAdvanceWatermark(ShardReplayCtx *ctx);

typedef uint64 (*ShardReplayCatchUpFn) (Oid shard_oid, uint64 bound,
                                        int timeout_ms);

/* ------------------------------------------------------------------ */
/* 共享内存：认领槽位 + 副本文件豁免哈希（补丁 0002 的判据）           */
/* ------------------------------------------------------------------ */

#define REPLAY_MAX_SHARDS 64

/* replay_enable（= armed）的持久化标记文件（pg_parwal/<oid>/ 下） */
#define REPLAY_ENABLED_FILENAME "replay_enabled"

#define REPLAY_ERRMSG_LEN 160

/*
 * 回放状态机（惰性形态）。
 *
 * 平时停在 IDLE —— **不做任何 redo**，副本只是 P2 平凡 apply 落下来的字节。
 * 触发（replay_catchup / 升主）把 target_plsn 抬到目标位置，worker 转入
 * CATCHING_UP 追平，完成后回到 IDLE。追平出错停在 FAILED 并留下 errmsg。
 */
typedef enum ReplayState
{
    REPLAY_IDLE = 0,
    REPLAY_CATCHING_UP = 1,
    REPLAY_FAILED = 2
} ReplayState;

typedef struct ReplayShardSlot
{
    Oid     shard_oid;      /* InvalidOid = 空槽 */
    int     claimed_by;     /* 0 = 未认领；否则为持有 worker 的 PID（§13.10） */
    bool    armed;          /* 允许被触发；false = 连触发都不受理 */

    uint64  target_plsn;    /* 触发目标；<= applied 即无待办（惰性的核心） */
    uint64  applied;        /* worker 单写、其他进程只读 */
    int     state;          /* ReplayState */
    uint64  generation;     /* 每次触发 +1，供调用方区分轮次 */
    char    errmsg[REPLAY_ERRMSG_LEN];

    /* 本地副本文件号 —— 补丁 0002 豁免哈希的数据源 */
    int           nlocs;
    RelFileNumber locs[SHARD_FILESET_MAX_RELS];
} ReplayShardSlot;

typedef struct ReplayCtlData
{
    LWLock *lock;
    int     nreplicas;      /* 有 locs 的槽位数；0 = 豁免钩子快速返回 */
    ReplayShardSlot slots[REPLAY_MAX_SHARDS];
} ReplayCtlData;

extern PGDLLIMPORT ReplayCtlData *ReplayCtl;

extern void RequestReplayShmem(void);
extern void ReplayShmemInit(void);

/* 补丁 0002 的钩子实现：relNumber 命中副本豁免哈希 → 跳过 XLogFlush */
extern bool PartDistFlushExemptHook(const RelFileLocator *rlocator);

/* launcher 注册 + GUC 定义（_PG_init 调用） */
extern void RegisterReplayLauncher(void);
extern void DefineReplayGUCs(void);

/* GUC 值 */
extern int  replay_workers;
extern int  replay_naptime_ms;
extern int  replay_checkpoint_interval_ms;
extern int  replay_checkpoint_records;
extern bool replay_trust_local_segments;
extern int  replay_debug_delay_ms;
extern bool replay_debug_trace;

#define REPLAY_TRACE(...) \
    do { if (replay_debug_trace) elog(LOG, __VA_ARGS__); } while (0)

/* bgworker 入口（PGDLLEXPORT：-fvisibility=hidden 下须显式导出，
 * 否则 postmaster 侧 load_external_function 找不到符号） */
extern PGDLLEXPORT void ReplayLauncherMain(Datum arg) pg_attribute_noreturn();
extern PGDLLEXPORT void ReplayWorkerMain(Datum arg) pg_attribute_noreturn();

/* 回放核心（shard_replay.c） */
extern void ShardReplayRun(ShardReplayCtx *ctx, uint64 bound);
extern bool ShardReplayLoadLocMap(ShardReplayCtx *ctx);
extern void ShardReplayDoCheckpoint(ShardReplayCtx *ctx);

#endif /* SHARD_REPLAY_H */
