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

#endif /* SHARD_FILESET_H */
