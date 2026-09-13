/*
 * shard_guard.h — T4.6：§9.2 第 3 层禁用项拦截（见 shard_guard.c 头注释）
 */
#ifndef SHARD_GUARD_H
#define SHARD_GUARD_H

#include "postgres.h"
#include "nodes/plannodes.h"

extern void ShardGuardCheckPlan(PlannedStmt *pstmt);

/* T7.10：引用表运行期写守卫。由 planner_hook 在 Citus 改写之前调用。 */
extern void ShardGuardCheckReferenceWrite(Oid relid);

/* T7.22（R-P6-14）：逻辑复制协议入口的禁令 */
extern void ShardGuardInstallAuthHook(void);
extern int	ShardGuardTerminateLogicalWalsenders(void);

#endif							/* SHARD_GUARD_H */
