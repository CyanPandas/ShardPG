/*
 * shard_fileset.h
 *
 * ShardFileSet — "shard 的物理文件集合"（FRD §5）。
 *
 * 一个 shard 在物理上不止一个文件：主堆、全部索引、TOAST 堆、TOAST 索引。
 * 物理回放要求凡是修改这些文件字节的 WAL 记录一条不漏（完整物理子流），
 * 因此捕获判据必须从"主堆单一 relfilenode"升级为"fileset 任一成员命中"。
 *
 * leader 侧：BuildShardFileSet()（catalog 遍历，需 backend 事务上下文）
 *            → RegisterShardFileSet()（写入 shmem 反向哈希 + 持久化到
 *              pg_parwal/<shard_oid>/fileset，供无 catalog 的 bgworker 重建）。
 * follower 侧：同一结构经 SQL 注册为 loc_map 的 leader 半边（FRD §7.1）。
 *
 * 角色 + 序号（role, ord）是 leader/follower 文件配对的键：两侧各自按
 * "主堆 → 索引(定义序) → TOAST 堆 → TOAST 索引" 枚举，同 (role, ord) 配对。
 */
#ifndef SHARD_FILESET_H
#define SHARD_FILESET_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "storage/relfilelocator.h"

/* 关系在 fileset 中的角色（配对键的高位） */
typedef enum ShardRelRole
{
    SHARD_REL_MAIN = 0,         /* 主堆 */
    SHARD_REL_INDEX = 1,        /* 普通索引，ord = 定义序（OID 升序） */
    SHARD_REL_TOAST = 2,        /* TOAST 堆 */
    SHARD_REL_TOAST_INDEX = 3   /* TOAST 索引 */
} ShardRelRole;

typedef struct ShardFileSetRel
{
    RelFileLocator  loc;
    uint8           role;       /* ShardRelRole */
    uint8           ord;        /* 同 role 内序号（索引定义序），其余为 0 */
    uint16          _pad;
} ShardFileSetRel;

/* 单 shard 的物理文件数上限：主堆 + TOAST 堆/索引 + 29 个索引，足够 */
#define SHARD_FILESET_MAX_RELS  32

typedef struct ShardFileSet
{
    uint32          magic;      /* SHARD_FILESET_MAGIC */
    Oid             shard_oid;  /* = partition_id = pg_parwal 目录名 */
    int32           nrels;
    ShardFileSetRel rels[SHARD_FILESET_MAX_RELS];
} ShardFileSet;

#define SHARD_FILESET_MAGIC     UINT32_C(0x46534554)    /* "FSET" */

/*
 * BuildShardFileSet — catalog 遍历收齐 shard 的全部物理文件。
 * 只能在有事务/catalog 访问的 backend 里调用。返回成员数，shard 不存在返回 -1。
 */
extern int  BuildShardFileSet(Oid shard_oid, ShardFileSet *fs);

/*
 * BuildShardFileSetEx — 同上，另把各成员的**关系 OID** 按同序填进 relids
 * （数组长度须 >= SHARD_FILESET_MAX_RELS，可传 NULL 表示不要）。
 * fileset 本身只存 RelFileLocator，而 log_newpage_range() 要 Relation。
 */
extern int  BuildShardFileSetEx(Oid shard_oid, ShardFileSet *fs, Oid *relids);

/*
 * RegisterShardFileSet — fileset 全体成员写入 shmem 反向哈希
 * (relNumber → shard_oid)，并原子落盘 pg_parwal/<oid>/fileset。
 */
extern void RegisterShardFileSet(const ShardFileSet *fs);

/*
 * LoadShardFileSet / LoadAllShardFileSets — 从持久化文件重建注册。
 * 无 catalog 依赖；demux/replay bgworker 启动时调用。
 */
extern bool LoadShardFileSet(Oid shard_oid, ShardFileSet *fs);
extern void LoadAllShardFileSets(void);

/*
 * SmgrRecordGetLocator — 从原始 XLogRecord 字节中解析 RM_SMGR 记录
 * (XLOG_SMGR_CREATE / XLOG_SMGR_TRUNCATE) 的 RelFileLocator。
 *
 * 这类记录不注册任何 buffer（storage.c 只 XLogRegisterData），blocks[]
 * 恒空，locator 在 main data 里 —— 捕获侧必须走本函数特判（FRD §5.2）。
 */
extern bool SmgrRecordGetLocator(const char *record_data, uint32 record_len,
                                 uint8 info, RelFileLocator *out);

/* ------------------------------------------------------------------ */
/* §12：DDL 引起的 fileset 变更                                        */
/* ------------------------------------------------------------------ */

/*
 * ShardFilesetNoteMaybeChanged — "刚跑完一条可能改 relfilenode 的语句"。
 * ProcessUtility_hook 在语句执行完之后调用，只置一个 backend 本地标记。
 *
 * ShardFilesetMaybeEmitUpdates — 在 XACT_EVENT_PRE_COMMIT 调用：若标记为真，
 * 重算本节点每个 shard 的 fileset 并与持久化版本 diff，有变化的走
 * "排空 → 注册 → 追加 CTRL:FILESET_UPDATE → 灌新文件 FPI"。
 * 标记为假时直接返回，普通 DML 路径零开销。
 */
extern void ShardFilesetNoteMaybeChanged(void);
extern void ShardFilesetMaybeEmitUpdates(void);

/* GUC：单次 fileset 变更最多把多少个块以 FPI 形式灌进流（超限只发通知） */
extern int  fileset_inline_max_blocks;

/* ------------------------------------------------------------------ */
/* T6.1（P6）：全量物理基线                                            */
/* ------------------------------------------------------------------ */

/*
 * ShardBaselineEmit — 把该 shard 的全部字节重新灌进自己的分区流，返回这次
 * 基线的起点 partition_lsn（= 那条 CTRL 的编号）。把它交给 follower 当
 * `base_part_lsn`：从这一条开始重放，之前的记录一律不看。
 *
 * 实装的是设计 §13 约束 2 后半句（"拷贝时记下 partition_lsn 静止点"）——
 * 此前只有前半句，locmap 从不配对起点游标。
 */
extern uint64 ShardBaselineEmit(Oid shard_oid);

/* ------------------------------------------------------------------ */
/* §13 约束 5：冻结账目同步（D2）                                      */
/* ------------------------------------------------------------------ */

/*
 * ShardFreezeMaybeEmitUpdates — 在 XACT_EVENT_PRE_COMMIT 调用：按时间间隔
 * 把本节点每个 shard 的 pg_class.relfrozenxid / relminmxid 与持久化基线 diff，
 * 有变化就发一条 CTRL:FREEZE_UPDATE。
 *
 * 用时间驱动而不是 D1 那套"DDL 后置脏标记"，是因为 **autovacuum 推进
 * relfrozenxid 不走 ProcessUtility**，脏标记对它完全无效。
 */
extern void ShardFreezeMaybeEmitUpdates(void);

/*
 * T5.4b-2（设计 §6.7）：把本分片的两个 vacuum 水位作为 CTRL 记录发进分区流。
 * 复用 FREEZE_UPDATE 通道换语义，不新增 opcode（clog_truncate_before 就是
 * 分片 xid 宇宙里的隐式 freeze 点，与 relfrozenxid 同属"leader 的冻结账目"）。
 * **尽力而为**：发不出去只会让 follower 的免查区落后（安全方向），且发的是
 * 绝对值，下一轮截断自然补上；绝不把调用方的 vacuum 带下水。
 */
extern void ShardVacuumEmitWatermarkCtrl(Oid shard_oid,
										 TransactionId trunc_before,
										 TransactionId vacuum_xid);

/*
 * ShardFreezeNoteUserActivity — "本 backend 执行了一条用户语句"。
 *
 * 由 ExecutorStart / ProcessUtility 两个钩子调用，是冻结发射器的**白名单**开关。
 * 没有它，发射器会跑进新连接的 InitPostgres 引导事务里 —— 那时 backend 还没
 * 初始化完，一路走到 pg_raft 的 SPI 读取会 SIGSEGV 打死整个节点。
 * 详见 shard_fileset.c 里 ShardFreezeMaybeEmitUpdates 的守卫注释。
 */
extern void ShardFreezeNoteUserActivity(void);

/* GUC：两次冻结账目检查的最小间隔（毫秒）；0 = 每个事务都查（测试用） */
extern int  freeze_sync_interval_ms;

/* §13 约束 13 的"检测"：分叉标记（非事务性，落 pg_parwal/<oid>/diverged） */
extern void  ShardMarkDiverged(Oid shard_oid, const char *reason);
extern char *ShardDivergedReason(Oid shard_oid);
extern void  ShardClearDiverged(Oid shard_oid);

/* 批次 #9："本节点已是该分片的主"的持久记号（重启后闸门要认得） */
extern void  ShardPromotedMarkWrite(Oid shard_oid, bool promoted);
extern bool  ShardPromotedMarkRead(Oid shard_oid);

/* FRD §11 步骤 5：升主的角色切换（角色 + 捕获登记） */
extern void  PartDistRoutePromote(Oid shard_oid);

/*
 * T7.3（R-P6-16）：升主后把本节点的 fileset 广播出去，让其余副本把 locmap
 * 重绑到新主的文件号上（`PARTWAL_FSUPD_PRIMARY_HANDOVER`：只重绑、不截断、
 * 不发 FPI）。由 PartDistRoutePromote 调用；失败只 WARNING。
 */
extern void  PartDistEmitFilesetHandover(Oid shard_oid);

#endif /* SHARD_FILESET_H */
