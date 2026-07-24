#ifndef PG_RAFT_H
#define PG_RAFT_H

#include "postgres.h"
#include "executor/spi.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"

/* SQL 可调用函数里再次进入 SPI 时，可能返回 SPI_ERROR_CONNECT（表示已经连接）。 */
static inline bool
pg_raft_spi_begin(bool *must_finish)
{
    int rc = SPI_connect();

    if (rc == SPI_OK_CONNECT)
    {
        *must_finish = true;
        return true;
    }
    if (rc == SPI_ERROR_CONNECT)
    {
        *must_finish = false;
        return true;
    }
    *must_finish = false;
    return false;
}

static inline void
pg_raft_spi_end(bool must_finish)
{
    if (must_finish)
        SPI_finish();
}

#define PG_RAFT_OP_NODE_STATUS       "OP_NODE_STATUS"
#define PG_RAFT_OP_PARTITION_PRIMARY "OP_PARTITION_PRIMARY"

#define PG_RAFT_MAX_HOSTNAME 256

typedef struct RaftLeaderShmem
{
    int            leader_node_id;
    int64          current_term;
    TimestampTz    lease_until;
} RaftLeaderShmem;

extern int  pg_raft_node_id;
extern int  pg_raft_probe_interval_ms;
extern int  pg_raft_probe_fail_threshold;
extern int  pg_raft_leader_lease_ms;
/* 协调节点(master)：group 0 leader 优先落于此节点；且不得作为数据组成员。0=不指定 */
extern int  pg_raft_coordinator_node_id;

/* 纯 C Raft 选举共识（raft_consensus.c） */
extern bool  pg_raft_raft_enabled;
extern char *pg_raft_peers;
extern int   pg_raft_election_timeout_ms;
extern int   pg_raft_heartbeat_ms;

extern Size pg_raft_consensus_shmem_size(void);
extern void pg_raft_consensus_shmem_init(void);
extern void pg_raft_consensus_tick(void);
extern bool pg_raft_consensus_is_leader(void);
extern int  pg_raft_consensus_leader_id(void);
extern int64 pg_raft_consensus_term(void);
extern int64 pg_raft_consensus_commit_index(void);
extern int64 pg_raft_consensus_last_applied(void);
extern int64 pg_raft_consensus_propose(const char *op_type, const char *payload);
extern void pg_raft_consensus_apply_pending(void);
/* prepare 接线：PartWALFlush 经 rendezvous "partdist_partwal_replicate_hook" 调用 */
extern void pg_raft_partwal_replicate(Oid partition_id);

extern RaftLeaderShmem *RaftLeaderShmemData;
extern LWLock *RaftLeaderLock;

extern int64 pg_raft_propose_node_status_internal(int node_id, const char *status);
extern int64 pg_raft_propose_partition_primary_internal(Oid partition_id, int primary_node,
                                                        const char *secondary_nodes_json,
                                                        int old_primary_node,
                                                        uint64 switch_partition_lsn,
                                                        const char *switch_orig_lsn,
                                                        int64 primary_term);
extern void pg_raft_topology_probe_and_failover(void);
extern void pg_raft_failover_partitions_for_node(int down_node_id);
extern void pg_raft_rejoin_partitions_for_node(int up_node_id);

extern Size pg_raft_shmem_size(void);
extern void pg_raft_shmem_request(void);
extern void pg_raft_shmem_startup(void);

extern bool pg_raft_is_leader_local(void);
extern void pg_raft_try_acquire_leader(void);
extern void pg_raft_topology_monitor_main(Datum main_arg);

extern bool pg_raft_apply_node_status(int node_id, const char *status);
extern bool pg_raft_apply_partition_primary(Oid partition_id, int primary_node,
                                            const char *secondaries_array_literal,
                                            int old_primary_node,
                                            uint64 switch_partition_lsn,
                                            const char *switch_orig_lsn,
                                            int64 primary_term);
extern bool pg_raft_apply_payload_sql(const char *op_type, const char *payload_json);
extern bool pg_raft_replicate_to_peers(const char *op_type, const char *payload_json);

extern void pg_raft_format_conninfo(const char *hostname, int port,
                                    char *conninfo, size_t len);

#endif /* PG_RAFT_H */
