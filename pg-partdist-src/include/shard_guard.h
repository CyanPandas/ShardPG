/*
 * shard_guard.h — T4.6：§9.2 第 3 层禁用项拦截（见 shard_guard.c 头注释）
 */
#ifndef SHARD_GUARD_H
#define SHARD_GUARD_H

#include "postgres.h"
#include "nodes/plannodes.h"

extern void ShardGuardCheckPlan(PlannedStmt *pstmt);

#endif							/* SHARD_GUARD_H */
