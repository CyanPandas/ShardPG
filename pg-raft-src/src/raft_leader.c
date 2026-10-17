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
int  pg_raft_self_connect_timeout_s = 10;   /* P7-N8 */
/* TPC-C 分支：多机部署 */
bool pg_raft_loopback_node_names = true;    /* coordinator/master/worker1..3 ⇒ 127.0.0.1 */
int  pg_raft_peer_connect_timeout_s = 1;    /* 连别的节点（探活 / RPC）的建连超时 */
bool pg_raft_single_member_fast_commit = false;
/* 2026-10-16：多成员数据组 hardstate 里只有 commit/applied 变化时不 fsync（见 persist_hard_state_unlocked） */
bool pg_raft_data_hardstate_lazy = true;
/* 2026-10-16：一笔事务触达的全部数据组一轮复制、多数派即返回（见 pg_raft_partwal_replicate_many） */
bool pg_raft_replicate_many = true;
bool pg_raft_replicate_pipeline = false;  /* 2026-10-17：流水线复制（发送与等待解耦；试验性，默认关） */
bool pg_raft_replicate_stream = true;     /* 2026-10-17：节点级有序复制流（见 raft_consensus.c pg_raft_stream_main） */
int  pg_raft_stream_batch_entries = 512;  /* 复制流：每批每组最多几条 */
int  pg_raft_stream_inflight = 8;         /* 复制流：每个副本最多几批在途 */
int  pg_raft_replicate_batch_max = 128;      /* P7-N41 */
int  pg_raft_probe_fail_threshold = 2;
int  pg_raft_leader_lease_ms = 10000;
int  pg_raft_coordinator_node_id = 1;

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
    /* 2026-10-16：hardstate 文件写入按组散列串行化（见 persist_hard_state_unlocked） */
    RequestNamedLWLockTranche("pg_raft_hardstate", RAFT_HS_LOCKS);
}

void
pg_raft_shmem_startup(void)
{
    bool found;
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    RaftLeaderShmemData = (RaftLeaderShmem *)
        ShmemInitStruct("pg_raft_leader", MAXALIGN(sizeof(RaftLeaderShmem)), &found);

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
