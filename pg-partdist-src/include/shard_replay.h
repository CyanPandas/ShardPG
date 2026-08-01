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
/* apply checkpoint（FRD §8.4；R1: nxidmap 恒 0）                      */
/* ------------------------------------------------------------------ */

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
    uint64          max_replayed_fxid;  /* R2 起使用；R1 恒 0                */
    uint64          resume_segno;       /* 恢复扫描起点提示（仅优化）        */
    uint64          resume_offset;
    uint32          nxidmap;            /* R1 恒 0                           */
    uint32          crc;                /* 头（crc 字段置 0 计算）的 CRC32C  */
} ShardApplyCheckpoint;

extern bool ReadApplyCheckpoint(Oid shard_oid, ShardApplyCheckpoint *out);
extern void WriteApplyCheckpoint(const ShardApplyCheckpoint *chk);

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
 * 回放上界回调（FRD §6）：返回该 shard 已 committed 的最大 partition_lsn。
 * 生产环境由 pg_raft 经 rendezvous variable "partdist_replay_bound_hook"
 * 注入（commit_index）；测试模式 GUC replay_trust_local_segments=on 时
 * 用本地段文件内容代替，使回放模块可脱离 Raft 独立开发验收。
 */
typedef uint64 (*ShardReplayBoundFn) (Oid shard_oid);

/* ------------------------------------------------------------------ */
/* 共享内存：认领槽位 + 副本文件豁免哈希（补丁 0002 的判据）           */
/* ------------------------------------------------------------------ */

#define REPLAY_MAX_SHARDS 64

/* replay_enable 的持久化标记文件（pg_parwal/<oid>/ 下） */
#define REPLAY_ENABLED_FILENAME "replay_enabled"

typedef struct ReplayShardSlot
{
    Oid     shard_oid;      /* InvalidOid = 空槽 */
    int     claimed_by;     /* 0 = 未认领；否则为持有 worker 的 PID（§13.10） */
    bool    enabled;
    uint64  applied;        /* worker 单写、其他进程只读（status 展示用） */

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
