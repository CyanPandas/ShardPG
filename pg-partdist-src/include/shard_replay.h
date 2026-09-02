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
#include "partition_wal_header.h"   /* PartWALFreezeEntry */

/* ------------------------------------------------------------------ */
/* loc_map                                                              */
/* ------------------------------------------------------------------ */

/*
 * role/ord 是**配对键**，不是冗余信息：leader 侧 DDL 换了文件号之后
 * （VACUUM FULL / REINDEX / TRUNCATE），FILESET_UPDATE 控制记录带来的是
 * 一份新的 leader 文件号清单，follower 必须据此重新配对 —— 而 replay worker
 * 是无 catalog 访问的 bgworker，没法现场 BuildShardFileSet 去查"本地哪个
 * 关系是第 2 个索引"。把 (role, ord) 一起持久化进 locmap，换表就成了纯粹的
 * 文件内查表，不碰 catalog（D1-c）。
 */
typedef struct LocMapEntry
{
    RelFileLocator  leader_loc;     /* hash key：record 里携带的就是它 */
    RelFileLocator  local_loc;
    uint8           role;           /* ShardRelRole，与 local_loc 同侧    */
    uint8           ord;            /* 同 role 内序号                     */
    uint16          reserved;       /* 显式补齐，恒为 0                   */
} LocMapEntry;

StaticAssertDecl(sizeof(LocMapEntry) == 28,
                 "LocMapEntry 必须是 28 字节且无填充洞（随 locmap 直写磁盘）");

#define REPLAY_LOCMAP_MAGIC     UINT32_C(0x4C4D4150)    /* "LMAP" */
#define REPLAY_LOCMAP_VERSION   UINT32_C(3)             /* v3：加 base_part_lsn */
#define REPLAY_LOCMAP_FILENAME  "locmap"

/*
 * v1（R1/L1/R2 时代）没有 version 字段、每对 24 字节，整文件 780 字节；
 * v2 是 912 字节；v3 是 920。三者长度两两不同，read() 的返回长度就是可靠的
 * 判别器 —— 装载失败时提示重跑 replay_set_locmap()，不做原地升级：v1 文件里
 * 没有 role/ord，v2 文件里没有 base_part_lsn，凭空都补不出来（那正是新版本
 * 存在的理由）。
 *
 * ★ v3 新增 base_part_lsn（T6.2）：**本配对自哪个 partition_lsn 起有效**。
 *
 * 设计 §13 约束 2 要求"副本必须由 leader shard 物理拷贝初始化（拷贝时记下
 * partition_lsn 静止点，增量从该游标重放追齐）"。此前只实装了前半句：locmap
 * 只回答"哪个文件对哪个文件"，从不回答"从哪个游标开始"，而缺省游标 0 是一句
 * **没人建立、也没人校验**的断言 ——「本地文件 == leader 在流起点时的文件」。
 * R-P4-20 的第一半就是它：断言不成立时，从 0 重放会把早期记录灌到一个内容
 * 对不上的文件上，页面太短即不可捕获的 PANIC。
 *
 * v3 把这句断言**写进文件**：
 *   base_part_lsn == 0 —— 显式声明"这条流从关系的出生点开始"。
 *                          `replay_set_locmap` 会**核对本地关系确为空**，
 *                          断言至少不是一眼假的。
 *   base_part_lsn >  0 —— 由 `partdist.shard_baseline_emit()`（T6.1）给出，
 *                          自那条全量基线 CTRL 起重放。
 * 无 apply_checkpoint 时，认领游标取 base_part_lsn 而不再是 0。
 */
typedef struct ReplayLocMapFile
{
    uint32          magic;
    uint32          version;        /* REPLAY_LOCMAP_VERSION */
    Oid             shard_oid;      /* 本地 shard（分区）OID = 目录名 */
    int32           npairs;
    uint64          base_part_lsn;  /* v3：本配对自哪个游标起有效（见上） */
    LocMapEntry     pairs[SHARD_FILESET_MAX_RELS];
} ReplayLocMapFile;

/*
 * locmap 的原子持久化（tmp + fsync + rename）。replay_set_locmap（首次配对）
 * 与 CTRL:FILESET_UPDATE 的应用（换表）共用，避免两处各写一遍写岔。
 */
extern void ShardReplayWriteLocMap(const ReplayLocMapFile *lm);

/*
 * 读 + 校验 locmap 文件。失败返回 false，*reason 指向说明原因的静态串
 * （可传 NULL）。三个读点（回放主循环、槽位 locs 装载、CTRL 换表）共用，
 * 免得 v1/v2 的判别条件在三处各写一遍、各漏一条。
 */
extern bool ShardReplayReadLocMap(Oid shard_oid, ReplayLocMapFile *lm,
                                  const char **reason);

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

/* 槽位与回放上下文共用同一个错误串长度（前者原样拷后者） */
#define REPLAY_ERRMSG_LEN 160

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
    uint64             base_part_lsn;      /* T6.2：本配对起效游标（locmap
                                            * v3）。无 apply_checkpoint 时
                                            * 认领从它起，而不再是 0        */
    uint64             applied_part_lsn;   /* 已应用游标（内存值）            */
    uint64             durable_part_lsn;   /* 已持久化游标                    */
    XLogRecPtr         max_orig_lsn;       /* 已应用的最大 leader end LSN     */
    uint64             records_since_ckpt;
    TimestampTz        last_ckpt_time;

    /*
     * 结构栅栏（§12）：应用到一条 FILESET_UPDATE 却发现本地结构对不上时置位，
     * 应用循环随即跳出且**不推进游标**。worker 据此把槽位置成
     * REPLAY_NEEDS_STRUCT 并把原因摆到 errmsg 上。
     */
    bool               needs_struct;
    char               struct_errmsg[REPLAY_ERRMSG_LEN];

    /*
     * 待发布的冻结账目（§13 约束 5，D2）。
     *
     * ★ replay worker **访问不了 catalog**：它的连接是
     * BackgroundWorkerInitializeConnection(NULL, NULL, 0) —— 故意不选数据库
     * （那么写是为了走完 BaseInit，不是为了读目录）。在这里碰 pg_class 会
     * 当场 "cannot read pg_class without having selected a database" FATAL，
     * 而 worker 一 FATAL 就重启、从游标重放、再撞同一条记录 —— 无限崩溃循环。
     * （这个坑是实测撞出来的，别再把目录写回搬进 worker。）
     *
     * 所以 worker 只负责把值搬到共享内存槽位（ReplayShardSlot.freeze），
     * 真正写 pg_class 由 replay_catchup() 的**调用方**完成 —— 那是一个正常
     * backend，有数据库、有事务。惰性形态下回放只可能由 replay_catchup
     * 触发，所以一定有这么一个调用方在。
     */
    int                pending_freeze_n;
    PartWALFreezeEntry pending_freeze[SHARD_FILESET_MAX_RELS];

    /*
     * T5.4b-2（设计 §6.7）：随 FREEZE_UPDATE 捎来的分片 vacuum 两水位。
     * 与冻结账目同款处置——worker 里只暂存，落盘交给 replay_catchup 的调用方
     * （写水位文件要开事务/取 shmem 锁，worker 不合适）。
     */
    bool               pending_vacuum_wm;
    TransactionId      pending_trunc_before;
    TransactionId      pending_vacuum_xid;

    /* U-P5-1 之二：随 MARKER 捎来的 leader 发号水位（取 max，checkpoint 落盘）*/
    TransactionId      pending_alloc_wm;

    /* 建 ctx 时槽位上的 locmap 代次；与槽位不符即须重建（见 ReplayShardSlot）*/
    uint64             locmap_gen;

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

/*
 * 回放状态机（惰性形态）。
 *
 * 平时停在 IDLE —— **不做任何 redo**，副本只是 P2 平凡 apply 落下来的字节。
 * 触发（replay_catchup / 升主）把 target_plsn 抬到目标位置，worker 转入
 * CATCHING_UP 追平，完成后回到 IDLE。追平出错停在 FAILED 并留下 errmsg。
 *
 * NEEDS_STRUCT 与 FAILED 的区别是**可恢复性**，值得单列一个状态：
 * 前者是"leader 的结构变了、本地 shell 表还没跟上"（CREATE/DROP INDEX），
 * 游标**停在该 CTRL 记录之前**、一个字节都没多应用，人工把本地结构补齐并
 * 重跑 replay_set_locmap() 之后原地就能继续；后者是回放本身出了错，
 * 通常意味着这个副本要重做。混成一个状态，运维就分不清"补个索引就行"
 * 和"这副本废了"。
 */
typedef enum ReplayState
{
    REPLAY_IDLE = 0,
    REPLAY_CATCHING_UP = 1,
    REPLAY_FAILED = 2,
    REPLAY_NEEDS_STRUCT = 3
} ReplayState;

typedef struct ReplayShardSlot
{
    Oid     shard_oid;      /* InvalidOid = 空槽 */
    int     claimed_by;     /* 0 = 未认领；否则为持有 worker 的 PID（§13.10） */
    bool    armed;          /* 允许被触发；false = 连触发都不受理 */

    /*
     * locmap 代次：每次 replay_set_locmap() 重建配对就 +1。worker 拿它和
     * ctx 里那份比对，不同就重建 ctx —— 否则运维在结构栅栏之后补完结构、
     * 重跑了 replay_set_locmap()，worker 仍抱着内存里那张旧 loc_map，
     * 再触发一次还是撞同一道栅栏。
     * （CTRL 换表是 worker 自己写的 locmap，不动这个计数。）
     */
    uint64  locmap_gen;

    uint64  target_plsn;    /* 触发目标；<= applied 即无待办（惰性的核心） */
    uint64  applied;        /* worker 单写、其他进程只读 */
    int     state;          /* ReplayState */
    uint64  generation;     /* 每次触发 +1，供调用方区分轮次 */
    char    errmsg[REPLAY_ERRMSG_LEN];

    /* 本地副本文件号 —— 补丁 0002 豁免哈希的数据源 */
    int           nlocs;
    RelFileNumber locs[SHARD_FILESET_MAX_RELS];

    /*
     * worker 发布、replay_catchup 的调用方消费的冻结账目（§13 约束 5）。
     * worker 碰不了 catalog（见 ShardReplayCtx.pending_freeze 的说明），
     * 只能经这里把值交给一个有数据库的普通 backend 去写 pg_class。
     */
    int                freeze_n;
    PartWALFreezeEntry freeze[SHARD_FILESET_MAX_RELS];

    /* T5.4b-2：同上，分片 vacuum 两水位的交接位 */
    bool               vacuum_wm_valid;
    TransactionId      vacuum_trunc_before;
    TransactionId      vacuum_xid;

    /* U-P5-1 之二：leader 发号水位的交接位（0 = 无待落盘的值）*/
    TransactionId      alloc_wm;
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

/*
 * 从 locmap 文件重新灌一遍槽位的本地文件号。CTRL:FILESET_UPDATE 换表之后
 * 必须调用 —— 豁免钩子认的是槽位里那份 locs[]，不刷新的话新文件号的脏页
 * 会走进 XLogFlush(leader LSN)，那个位置本地 pg_wal 根本没有。
 */
extern void ReplaySlotRefreshLocs(Oid shard_oid);

/*
 * 回收"关系已不存在"的回放槽位与 pg_parwal 目录（槽位上限 REPLAY_MAX_SHARDS）。
 * **必须在有数据库连接的 backend 里调用** —— 判据要查 pg_class，而 launcher
 * 只有 SHMEM_ACCESS。被活着的 worker 认领的槽位一律不动。
 */
extern void ReplayReclaimStale(int grace_secs, int *slots_freed,
                               int *dirs_removed);

/* launcher 注册 + GUC 定义（_PG_init 调用） */
extern void RegisterReplayLauncher(void);
extern void DefineReplayGUCs(void);

/* GUC 值 */
extern int  replay_workers;
extern int  replay_reclaim_grace_secs;
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
