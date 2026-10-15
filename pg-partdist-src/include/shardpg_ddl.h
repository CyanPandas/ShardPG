/*
 * shardpg_ddl.h — ShardPG 建表 / 删表一条龙（P7-N87 / N88 / N89）
 *
 *   CREATE TABLE t (..., PRIMARY KEY (...), SHARD KEY (列) [SHARDS n] [REPLICAS n] [COLOCATE WITH 表]);
 *       语法由内核补丁 0016 解析成命名空间 "shardpg" 的 reloption；这里摘下后执行：
 *       建表 + 分布（同一事务）→ 提交 → 建 Raft 组、选主、供副本、打标 → 提交。
 *
 *   DROP TABLE t;
 *       删表与"记下待清理分片"同一事务提交，之后拆组、删副本表、删登记、回收残留。
 *       在事务块里删表时只记队列，提交后由 shardpg.cleanup_dropped() 清。
 */
#ifndef SHARDPG_DDL_H
#define SHARDPG_DDL_H

#include "tcop/utility.h"

extern int  shardpg_shard_count;
extern int  shardpg_replicas;

/* 注册 shardpg.* 参数（_PG_init 里调） */
extern void ShardpgDdlDefineGucs(void);

/* 执行语句本身（含 pg_partdist 的事后登记），由 pg_partdist.c 提供 */
typedef void (*ShardpgRunChainFn) (PlannedStmt *pstmt, const char *queryString,
                                   bool readOnlyTree, ProcessUtilityContext context,
                                   ParamListInfo params, QueryEnvironment *queryEnv,
                                   DestReceiver *dest, QueryCompletion *qc);

/*
 * 截下 SHARD KEY 建表与分片表的 DROP。返回 true 表示语句已经执行完（含链上执行），
 * 调用方不要再执行；返回 false 表示与 ShardPG 无关，照常执行。
 */
extern bool ShardpgDdlIntercept(PlannedStmt *pstmt, const char *queryString,
                                bool readOnlyTree, ProcessUtilityContext context,
                                ParamListInfo params, QueryEnvironment *queryEnv,
                                DestReceiver *dest, QueryCompletion *qc,
                                ShardpgRunChainFn run_chain);

#endif   /* SHARDPG_DDL_H */
