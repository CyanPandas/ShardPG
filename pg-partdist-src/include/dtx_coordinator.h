#ifndef DTX_COORDINATOR_H
#define DTX_COORDINATOR_H

#include "postgres.h"
#include "executor/executor.h"
#include "tcop/utility.h"

extern bool shardpg_first_shard_coordinator;
extern void DtxCoordinatorInit(void);
extern void DtxRequireRouterHooks(void);
extern void DtxEnlistPlan(PlannedStmt *plan);
extern int DtxBranchPartitions(const Oid **partitions);
extern bool DtxHandoffUtility(Node *stmt, ProcessUtilityContext context,
                              QueryCompletion *qc);
extern void DtxScheduleDecision(int64 dtxid, int verdict, int64 commit_ts);
extern void DtxFinishPrepared(const char *gid, bool committed);

#endif
