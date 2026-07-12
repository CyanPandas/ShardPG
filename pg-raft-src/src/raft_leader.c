#include "pg_raft.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"

int  pg_raft_node_id = 1;
int  pg_raft_probe_interval_ms = 3000;
int  pg_raft_probe_fail_threshold = 2;
int  pg_raft_leader_lease_ms = 10000;

RaftLeaderShmem *RaftLeaderShmemData = NULL;
LWLock           *RaftLeaderLock = NULL;

Size
pg_raft_shmem_size(void)
{
    return MAXALIGN(sizeof(RaftLeaderShmem)) + pg_raft_consensus_shmem_size();
}

void
pg_raft_shmem_request(void)
{
    RequestAddinShmemSpace(pg_raft_shmem_size());
    RequestNamedLWLockTranche("pg_raft", 1);
}

void
pg_raft_shmem_startup(void)
{
    bool found;
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    RaftLeaderShmemData = (RaftLeaderShmem *)
        ShmemInitStruct("pg_raft_leader", pg_raft_shmem_size(), &found);

    if (!found)
    {
        RaftLeaderShmemData->leader_node_id = 1;
        RaftLeaderShmemData->current_term = 1;
        RaftLeaderShmemData->lease_until = 0;
    }

    RaftLeaderLock = &GetNamedLWLockTranche("pg_raft")[0].lock;

    /* 真 Raft 共识的共享内存段 */
    pg_raft_consensus_shmem_init();

    LWLockRelease(AddinShmemInitLock);
}

bool
pg_raft_is_leader_local(void)
{
    /*
     * raft_enabled=on：控制面 Leader 严格跟随本扩展内置的纯 C Raft 共识结果。
     * Leader 节点宕机后，剩余节点经多数派重新选举，新 Leader 节点的 pg_raft 会
     * 立刻在此返回 true，从而接管控制面（探测 + failover）。
     */
    if (pg_raft_raft_enabled)
        return pg_raft_consensus_is_leader();

    /*
     * 回退（未启用纯 C Raft）：单节点开发模式下控制面 Leader 固定在 node_id=1。
     */
    if (pg_raft_node_id == 1)
    {
        pg_raft_try_acquire_leader();
        return true;
    }
    return false;
}

void
pg_raft_try_acquire_leader(void)
{
    TimestampTz now = GetCurrentTimestamp();
    TimestampTz new_lease = now + (pg_raft_leader_lease_ms * 1000L);

    if (RaftLeaderShmemData == NULL)
        return;

    LWLockAcquire(RaftLeaderLock, LW_EXCLUSIVE);
    if (RaftLeaderShmemData->lease_until < now ||
        RaftLeaderShmemData->leader_node_id == pg_raft_node_id)
    {
        RaftLeaderShmemData->leader_node_id = pg_raft_node_id;
        RaftLeaderShmemData->current_term += 1;
        RaftLeaderShmemData->lease_until = new_lease;
    }
    LWLockRelease(RaftLeaderLock);
}
