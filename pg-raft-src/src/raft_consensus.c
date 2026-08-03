/*
 * raft_consensus.c — 纯 C Raft：Leader 选举 + 日志复制 + 多数派提交
 *
 * P1（分区级 Raft 组）起，本文件的状态机不再是"每节点一份单例"，而是
 * **按 group_id 索引的集合**（见 docs/raft_module_revision_plan.md §11.5）：
 *
 *   group 0            控制面 Raft（拓扑/成员/failover 决议），行为与组化前完全一致
 *   group > 0          数据面分区组，group_id == partdist.shard_identity.global_shard_id
 *                      （即 Citus shardid，跨节点一致；见 P0）
 *
 * 每组各自持有 state/term/voted_for/leader_id/election_deadline、日志与复制游标、
 * 成员集与 HardState 文件；单个 BGW tick 多路复用全部活跃组。
 *
 * 节点间 RPC（libpq → 普通 client backend，不碰 SPI/Citus/Go）：
 *   RequestVote : pg_raft_rpc('RV <term> <candidate> <last_idx> <last_term> [<group>]')
 *   AppendEntries(心跳): pg_raft_rpc('AE <term> <leader> 0 0 [<group>]')
 *   AppendEntries(带日志): pg_raft_append_entries(term, leader, prev_idx, prev_term,
 *                     leader_commit, entry_idx, entry_term, entry_op, entry_payload,
 *                     [group_id])
 *   group 参数缺省为 0，故与组化前的调用方保持二进制/线协议兼容。
 *
 * 日志存本节点共享内存 pg_raft_log；提交后通过 pg_raft_apply_payload_sql 写入
 * partdist 元数据表，使任意 Leader 节点都能接管控制面。
 */
#include "pg_raft.h"

#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "lib/stringinfo.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#define RAFT_FOLLOWER  0
#define RAFT_CANDIDATE 1
#define RAFT_LEADER    2

/* 1 coordinator + 16 worker 拓扑共 17 个 group0 成员；parse_peers 超限会静默丢弃 */
#define RAFT_MAX_PEERS     32
#define RAFT_LOG_CAPACITY  128
#define RAFT_OP_LEN        32
#define RAFT_PAYLOAD_MAX   768
#define RAFT_HARDSTATE_MAGIC   UINT32_C(0x52484654)
/*
 * v2 起 hardstate 增加 last_applied。v1 文件仍可读（last_applied 视为 0），
 * 避免升级时丢掉 current_term/voted_for 造成任期回退。
 */
#define RAFT_HARDSTATE_VERSION 2
#define RAFT_HARDSTATE_VERSION_MIN 1
/* v1 布局 = v2 去掉末尾的 last_applied，用 offsetof 取以免手算漏掉结构体填充 */
#define RAFT_HARDSTATE_V1_SIZE  offsetof(RaftHardStateFile, last_applied)

/* group 0 = 控制面；其余为数据面分区组 */
#define RAFT_CONTROL_GROUP  INT64CONST(0)
#define RAFT_MAX_GROUPS     32

typedef struct RaftConsensusShmem
{
    slock_t      mutex;
    int          state;
    int64        current_term;
    int          voted_for;
    int          leader_id;
    TimestampTz  election_deadline;
} RaftConsensusShmem;

typedef struct RaftLogEntry
{
    int64  index;
    int64  term;
    char   op_type[RAFT_OP_LEN];
    char   payload[RAFT_PAYLOAD_MAX];
} RaftLogEntry;

typedef struct RaftLogShmem
{
    slock_t      mutex;
    int64        last_log_index;
    int64        commit_index;
    int64        last_applied;
    int64        peer_next_index[RAFT_MAX_PEERS];
    int64        peer_match_index[RAFT_MAX_PEERS];
    bool         repl_inited;
    bool         apply_in_progress;  /* 串行化 apply，保证 N 先于 N+1 生效 */
    RaftLogEntry ring[RAFT_LOG_CAPACITY];
} RaftLogShmem;

/*
 * 一个 Raft 组的完整状态。members[] 存成员节点 id（空表示"全体 peers"，
 * 控制面即如此）；复制游标仍按全局 peer 槽位下标寻址，成员集只作过滤，
 * 这样 peer_next_index/peer_match_index 与 peers[] 天然对齐。
 */
typedef struct RaftGroupState
{
    bool                in_use;
    int64               group_id;
    bool                hs_loaded;      /* HardState 是否已从文件恢复进 shmem */
    bool                report_pending; /* 数据组新任 leader 尚未向控制面登记（tick 里重试投递） */
    int64               last_data_plsn; /* 本组已成功 propose 的最大 partition_lsn（prepare 接线的增量下界；重启后从环回推，幂等兜底） */
    bool                replicate_in_progress; /* 本组的 prepare 复制正在进行（串行化，见 replicate_claim） */
    int                 replicate_pid;         /* 持有者 backend PID；持有者消失时由等待者回收 */
    int                 n_members;
    int                 members[RAFT_MAX_PEERS];
    RaftConsensusShmem  cons;
    RaftLogShmem        log;
} RaftGroupState;

typedef struct RaftGroupTable
{
    slock_t         mutex;              /* 仅保护注册表本身（in_use/group_id/members） */
    int             n_groups;
    RaftGroupState  groups[RAFT_MAX_GROUPS];
} RaftGroupTable;

/* 组上下文：把"选中的那一组"显式传递，避免任何隐式当前组全局量 */
typedef struct RaftGroupCtx
{
    int64               group_id;
    RaftGroupState     *g;
    RaftConsensusShmem *cons;
    RaftLogShmem       *log;
} RaftGroupCtx;

typedef struct RaftPeer
{
    int   node_id;
    char  host[256];
    int   port;
} RaftPeer;

typedef struct RaftHardStateFile
{
    uint32 magic;
    uint32 version;
    int64  current_term;
    int32  voted_for;
    int64  commit_index;
    int64  last_applied;
} RaftHardStateFile;

bool  pg_raft_raft_enabled = false;
char *pg_raft_peers = NULL;
int   pg_raft_election_timeout_ms = 1500;
int   pg_raft_heartbeat_ms = 400;

static RaftGroupTable *RaftGroups = NULL;

static RaftPeer peers[RAFT_MAX_PEERS];
static int      n_peers = 0;
static bool     peers_parsed = false;
static bool     groups_restored = false;

PG_FUNCTION_INFO_V1(pg_raft_rpc);
PG_FUNCTION_INFO_V1(pg_raft_append_entries);
PG_FUNCTION_INFO_V1(pg_raft_apply_committed);
PG_FUNCTION_INFO_V1(pg_raft_group_create);
PG_FUNCTION_INFO_V1(pg_raft_group_drop);
PG_FUNCTION_INFO_V1(pg_raft_group_reset);
PG_FUNCTION_INFO_V1(pg_raft_group_status);
PG_FUNCTION_INFO_V1(pg_raft_group_propose);
PG_FUNCTION_INFO_V1(pg_raft_data_propose);

static void restore_hard_state_if_needed(RaftGroupCtx *ctx);
static void persist_hard_state_unlocked(RaftGroupCtx *ctx);
static void current_last_log_info_locked(RaftGroupCtx *ctx, int64 *last_idx,
                                         int64 *last_term);
static bool candidate_log_is_up_to_date_locked(RaftGroupCtx *ctx,
                                               int64 cand_last_idx,
                                               int64 cand_last_term);
static void parse_peers(void);
static bool raft_group_ctx(int64 group_id, RaftGroupCtx *ctx);
static bool raft_group_ensure(int64 group_id, const int *members, int n_members,
                              RaftGroupCtx *ctx);
static bool group_membership_known(RaftGroupCtx *ctx);
static bool group_resolve_membership(RaftGroupCtx *ctx);
static void restore_groups_if_needed(void);
static char *data_entry_fetch_hex(RaftGroupCtx *ctx, int64 partition_lsn);
static bool data_entry_store(RaftGroupCtx *ctx, const char *payload,
                             const char *data_hex);
static int64 group_local_partition(RaftGroupCtx *ctx);

/*
 * 数据组的 entry 携带真实 parwal 字节，取字节要走 SPI，因此只有 client backend
 * 路径（propose / flush_replication）能发数据条目；BGW tick 无 SPI，对数据组只
 * 发心跳，落后的 follower 在下一次 propose 时由 flush_replication 追平。
 */
static bool data_shipping_allowed = false;

static bool raft_persist_spi_begin(bool *spi_owned);
static void raft_persist_spi_end(bool spi_owned);
static int64 entry_partition_lsn(const char *payload);
static void data_group_try_report(RaftGroupCtx *ctx);

/* ---- 共享内存 ---- */

Size
pg_raft_consensus_shmem_size(void)
{
    return MAXALIGN(sizeof(RaftGroupTable));
}

static void
raft_group_init_slot(RaftGroupState *g, int64 group_id,
                     const int *members, int n_members)
{
    int i;

    memset(g, 0, sizeof(*g));
    g->in_use = true;
    g->group_id = group_id;
    g->hs_loaded = false;

    g->n_members = 0;
    if (members != NULL)
    {
        for (i = 0; i < n_members && g->n_members < RAFT_MAX_PEERS; i++)
            g->members[g->n_members++] = members[i];
    }

    SpinLockInit(&g->cons.mutex);
    g->cons.state = RAFT_FOLLOWER;
    g->cons.current_term = 0;
    g->cons.voted_for = 0;
    g->cons.leader_id = 0;
    g->cons.election_deadline = 0;

    SpinLockInit(&g->log.mutex);
    g->log.last_log_index = 0;
    g->log.commit_index = 0;
    g->log.last_applied = 0;
    g->log.repl_inited = false;
    g->log.apply_in_progress = false;

    g->last_data_plsn = 0;
    g->replicate_in_progress = false;
    g->replicate_pid = 0;
}

void
pg_raft_consensus_shmem_init(void)
{
    bool found;

    RaftGroups = (RaftGroupTable *)
        ShmemInitStruct("pg_raft_groups", pg_raft_consensus_shmem_size(), &found);

    if (!found)
    {
        memset(RaftGroups, 0, sizeof(RaftGroupTable));
        SpinLockInit(&RaftGroups->mutex);
        RaftGroups->n_groups = 0;
        /* 控制面组恒存在，且成员为全体 peers（members 留空表示全体） */
        raft_group_init_slot(&RaftGroups->groups[0], RAFT_CONTROL_GROUP, NULL, 0);
        RaftGroups->n_groups = 1;
    }
}

/* 填充 ctx；组不存在返回 false */
static bool
raft_group_ctx(int64 group_id, RaftGroupCtx *ctx)
{
    int i;

    if (RaftGroups == NULL)
        return false;

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];

        if (g->in_use && g->group_id == group_id)
        {
            ctx->group_id = group_id;
            ctx->g = g;
            ctx->cons = &g->cons;
            ctx->log = &g->log;
            return true;
        }
    }
    return false;
}

/*
 * 取得组；不存在则创建。Follower 从 leader 的 RPC 里第一次听说某个数据组时，
 * 也走这里自动建组。
 */
static bool
raft_group_ensure(int64 group_id, const int *members, int n_members,
                  RaftGroupCtx *ctx)
{
    int i;

    if (RaftGroups == NULL)
        return false;

    if (raft_group_ctx(group_id, ctx))
    {
        /* 已存在：允许更新成员集（控制面下发配置） */
        if (members != NULL && n_members > 0)
        {
            SpinLockAcquire(&RaftGroups->mutex);
            ctx->g->n_members = 0;
            for (i = 0; i < n_members && ctx->g->n_members < RAFT_MAX_PEERS; i++)
                ctx->g->members[ctx->g->n_members++] = members[i];
            SpinLockRelease(&RaftGroups->mutex);
        }
        return true;
    }

    SpinLockAcquire(&RaftGroups->mutex);
    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        if (!RaftGroups->groups[i].in_use)
        {
            raft_group_init_slot(&RaftGroups->groups[i], group_id,
                                 members, n_members);
            RaftGroups->n_groups++;
            SpinLockRelease(&RaftGroups->mutex);
            elog(LOG, "pg_raft: 创建 Raft 组 %lld（槽位 %d）",
                 (long long) group_id, i);
            return raft_group_ctx(group_id, ctx);
        }
    }
    SpinLockRelease(&RaftGroups->mutex);

    elog(WARNING, "pg_raft: Raft 组数量已达上限 %d，无法创建组 %lld",
         RAFT_MAX_GROUPS, (long long) group_id);
    return false;
}

/* 控制面组的便捷入口：所有旧的单例 API 都落到它 */
static bool
raft_control_ctx(RaftGroupCtx *ctx)
{
    return raft_group_ctx(RAFT_CONTROL_GROUP, ctx);
}

/*
 * 从控制面下发的 partition_map 导出某数据组的成员集（DTX_2PC_DESIGN.md §9.2）。
 *
 * partition_map 由 group 0 的 apply 在**每个节点**各写一份（计划文档 §13），
 * 所以这是本地可读、无需新增 RPC 的权威来源 —— 正是"成员集必须来自控制面下发，
 * 不能从收到的报文里推断"的落地形态。
 *
 * 成员集 = {primary_node} ∪ secondary_nodes，剔除协调节点（master 不作数据副本）。
 * 返回成员数；0 表示导不出来（该分区尚未登记，例如全新组的首次选举之前——
 * 那种情况必须由 pg_raft_group_create(gid, members) 显式给出）。
 *
 * 需要 SPI，只能在 SQL/客户端 backend 路径调用，**不能在 BGW tick 里调**。
 */
static int
group_members_from_partition_map(int64 group_id, int *members)
{
    StringInfoData sql;
    bool  spi_owned;
    bool  isnull;
    int   n = 0;

    if (group_id == RAFT_CONTROL_GROUP || group_id <= 0)
        return 0;
    if (!raft_persist_spi_begin(&spi_owned))
        return 0;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT primary_node, "
                     "       COALESCE(array_to_string(secondary_nodes, ','), '') "
                     "  FROM partdist.partition_map WHERE partition_id = %llu::oid",
                     (unsigned long long) group_id);

    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d;
        int   primary_node = 0;
        char *sec = NULL;

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            primary_node = DatumGetInt32(d);
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
        if (!isnull)
            sec = TextDatumGetCString(d);

        if (primary_node > 0 && primary_node != pg_raft_coordinator_node_id)
            members[n++] = primary_node;

        if (sec != NULL && sec[0] != '\0')
        {
            char *tok, *saveptr = NULL;

            for (tok = strtok_r(sec, ",", &saveptr);
                 tok != NULL && n < RAFT_MAX_PEERS;
                 tok = strtok_r(NULL, ",", &saveptr))
            {
                int id = atoi(tok);
                int j;

                if (id <= 0 || id == pg_raft_coordinator_node_id)
                    continue;
                for (j = 0; j < n; j++)
                    if (members[j] == id)
                        break;
                if (j == n)
                    members[n++] = id;
            }
        }
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return n;
}

/*
 * 成员集未知时尝试从 partition_map 补齐。返回补齐后是否已知。
 * 仅可在 SPI 可用的路径调用。
 */
static bool
group_resolve_membership(RaftGroupCtx *ctx)
{
    int members[RAFT_MAX_PEERS];
    int n;

    if (group_membership_known(ctx))
        return true;

    n = group_members_from_partition_map(ctx->group_id, members);
    if (n <= 0)
        return false;

    SpinLockAcquire(&RaftGroups->mutex);
    ctx->g->n_members = 0;
    while (ctx->g->n_members < n)
    {
        ctx->g->members[ctx->g->n_members] = members[ctx->g->n_members];
        ctx->g->n_members++;
    }
    SpinLockRelease(&RaftGroups->mutex);

    elog(LOG, "pg_raft: 组 %lld 的成员集从 partition_map 导出，共 %d 个成员",
         (long long) ctx->group_id, n);
    return true;
}

/*
 * ★ 成员集语义（2026-08-03 修，DTX_2PC_DESIGN.md §9.2）
 *
 * 旧语义把 `n_members == 0` 重载为"全体节点"。这对**控制面组**成立（组 0 的
 * 成员本来就是全部节点），对**数据组永远不成立**——一个分片的副本集必然是全体
 * 节点的真子集。实测后果（9 节点环境，以 SQL 默认的 NULL 成员集建组）：
 *   · cluster_size 算成 9、多数派算成 5，而只有 3 个节点持有该分片的数据；
 *   · 该组向**全集群**广播 RequestVote/AppendEntries，非副本节点收到后
 *     照样 hearsay 自动建组（同样是空成员集）并参与投票；
 *   · 实测真正持有数据的 worker 反而掉成 term=0 的 follower，分片彻底不可用，
 *     且非副本节点可以赢得它根本没有数据的分片的领导权。
 *   · 更本质的危险：同一个组在不同节点上有两套不相交的多数派定义
 *     （3 副本算 2/3，hearsay 节点算 5/9），Leader Completeness 失去交集保证。
 *
 * 新语义：数据组的 `n_members == 0` 表示**成员集未知**，而不是"全体"。未知即
 * **不参与**：不竞选、不发心跳、不投票、不提案——fail-stop 而不是 fail-open。
 * 成员集的权威来源是控制面下发到每个节点的 `partdist.partition_map`
 * （见计划文档 §13），由 group_members_from_partition_map() 在 SPI 可用的
 * 路径上自动导出；导不出来时必须由运维显式 pg_raft_group_create(gid, members)。
 */
static bool
group_membership_known(RaftGroupCtx *ctx)
{
    return ctx->group_id == RAFT_CONTROL_GROUP || ctx->g->n_members > 0;
}

/* 该节点是否为本组成员（控制面组的空成员集仍表示全体节点） */
static bool
group_has_member(RaftGroupCtx *ctx, int node_id)
{
    int i;

    if (ctx->g->n_members <= 0)
        return ctx->group_id == RAFT_CONTROL_GROUP;
    for (i = 0; i < ctx->g->n_members; i++)
        if (ctx->g->members[i] == node_id)
            return true;
    return false;
}

/* 本组的集群规模（用于多数派计算）；数据组成员集未知时返回 0 */
static int
group_cluster_size(RaftGroupCtx *ctx)
{
    if (ctx->g->n_members > 0)
        return ctx->g->n_members;
    if (ctx->group_id != RAFT_CONTROL_GROUP)
        return 0;               /* 成员集未知：规模无从谈起 */
    return (n_peers > 0) ? n_peers : 1;
}

static int
cluster_majority(RaftGroupCtx *ctx)
{
    int size = group_cluster_size(ctx);

    /*
     * 规模未知时返回一个**不可能达到**的票数（ack 数上限是 n_peers <=
     * RAFT_MAX_PEERS）。调用方本应在更早的门禁处就已返回，这里是兜底：
     * 万一有路径漏网，也必须 fail-closed（永远提交不了），
     * 绝不能退化成 0/2+1 = 1 而"一票自行提交"。
     */
    if (size <= 0)
        return RAFT_MAX_PEERS + 1;
    return size / 2 + 1;
}

/* ---- 日志辅助 ---- */

static RaftLogEntry *log_slot(RaftGroupCtx *ctx, int64 index);


/* ---- P2：数据组条目（parwal 记录）---- */

/*
 * 数据组的 Raft entry 是一条**描述符**（小 JSON，塞得进 RAFT_PAYLOAD_MAX），
 * 真实 WAL 字节随同一次 AppendEntries 以 bytea 参数下发：
 *
 *   {"partition_lsn":N,"orig_lsn":"X/Y","rmid":R,"info":I,"xid":T,"nbytes":B}
 *
 * 这样既复用了全部既有 Raft 机制（ring / prev_log 一致性检查 / 多数派提交 /
 * raft_log 持久化），又不必把任意长的字节流塞进 768 字节的 payload。
 * follower 必须**先把字节落盘 fsync 再 ack**，故"多数派提交"即"多数派已持久化"。
 */
#define RAFT_OP_PARWAL "OP_PARWAL"

/* 从描述符 JSON 里抠出 partition_lsn（不引 jsonb，简单扫描即可） */
static int64
entry_partition_lsn(const char *payload)
{
    const char *p;
    long long   v = 0;

    if (payload == NULL)
        return 0;
    p = strstr(payload, "\"partition_lsn\"");
    if (p == NULL)
        return 0;
    p = strchr(p, ':');
    if (p == NULL)
        return 0;
    if (sscanf(p + 1, " %lld", &v) != 1)
        return 0;
    return (int64) v;
}

/* 本节点承载该组分片的本地 OID（P0：global_shard_id -> local_oid） */
static int64
group_local_partition(RaftGroupCtx *ctx)
{
    StringInfoData sql;
    bool           spi_owned;
    bool           isnull;
    int64          local_oid = 0;

    if (!raft_persist_spi_begin(&spi_owned))
        return 0;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partdist.local_partition_for_shard(%lld)::bigint",
                     (long long) ctx->group_id);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        local_oid = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                                SPI_tuptable->tupdesc,
                                                1, &isnull));
        if (isnull)
            local_oid = 0;
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return local_oid;
}

/* Leader 侧：按 partition_lsn 取出本节点 parwal 记录的原始字节（hex） */
static char *
data_entry_fetch_hex(RaftGroupCtx *ctx, int64 partition_lsn)
{
    StringInfoData sql;
    bool           spi_owned;
    bool           isnull;
    char          *hex = NULL;
    int64          local_oid;

    if (partition_lsn <= 0)
        return NULL;

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0)
        return NULL;

    if (!raft_persist_spi_begin(&spi_owned))
        return NULL;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT encode(data, 'hex') FROM partdist.partwal_read_record(%u::oid, %lld)",
                     (unsigned) local_oid, (long long) partition_lsn);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);
        if (!isnull)
        {
            MemoryContext oldctx = MemoryContextSwitchTo(TopTransactionContext);
            hex = TextDatumGetCString(d);
            MemoryContextSwitchTo(oldctx);
        }
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return hex;
}

/*
 * Follower 侧：把随 AppendEntries 收到的字节原样落盘到**本节点自己的**
 * pg_parwal/<local_oid>/（local_oid 与 leader 不同，由 P0 映射解析）。
 * 返回 false 表示未能落盘 —— 此时不得 ack。
 */
static bool
data_entry_store(RaftGroupCtx *ctx, const char *payload, const char *data_hex)
{
    StringInfoData sql;
    bool           spi_owned;
    bool           ok = false;
    int64          local_oid;
    char           orig_lsn[64];
    long long      xid = 0;
    int            rmid = 0;
    int            info = 0;
    const char    *p;

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0)
    {
        elog(WARNING, "pg_raft: group %lld 在本节点没有对应分片，无法落盘",
             (long long) ctx->group_id);
        return false;
    }

    orig_lsn[0] = '\0';
    p = strstr(payload, "\"orig_lsn\"");
    if (p != NULL && (p = strchr(p, ':')) != NULL)
        (void) sscanf(p + 1, " \"%63[^\"]\"", orig_lsn);
    if (orig_lsn[0] == '\0')
        strlcpy(orig_lsn, "0/0", sizeof(orig_lsn));

    p = strstr(payload, "\"rmid\"");
    if (p != NULL && (p = strchr(p, ':')) != NULL)
        (void) sscanf(p + 1, " %d", &rmid);
    p = strstr(payload, "\"info\"");
    if (p != NULL && (p = strchr(p, ':')) != NULL)
        (void) sscanf(p + 1, " %d", &info);
    p = strstr(payload, "\"xid\"");
    if (p != NULL && (p = strchr(p, ':')) != NULL)
        (void) sscanf(p + 1, " %lld", &xid);

    if (!raft_persist_spi_begin(&spi_owned))
        return false;

    initStringInfo(&sql);
    /*
     * 必须把 leader 指定的 partition_lsn 一并传下去：follower 按该编号落盘，
     * 否则本地自增计数器会和 leader 的编号空间错位（本节点同一 pg_parwal 树下
     * 还有它作为 primary 的本地 demux 写入），applied_part_lsn 将指向本地
     * 不存在的记录。
     */
    appendStringInfo(&sql,
                     "SELECT partdist.partwal_follower_append("
                     "%u::oid, %lld::bigint, %s::pg_lsn, %d, %d, %lld::bigint, "
                     "decode(%s, 'hex'))",
                     (unsigned) local_oid,
                     (long long) entry_partition_lsn(payload),
                     quote_literal_cstr(orig_lsn),
                     rmid, info, xid,
                     quote_literal_cstr(data_hex != NULL ? data_hex : ""));
    ok = (SPI_execute(sql.data, false, 1) == SPI_OK_SELECT && SPI_processed > 0);
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (!ok)
        elog(WARNING, "pg_raft: group %lld parwal 落盘失败，拒绝 ack",
             (long long) ctx->group_id);
    return ok;
}

/*
 * 数据组的"平凡 apply"：不 redo，只把 applied_part_lsn 推到该条目的
 * partition_lsn。这补上了 follower_partition_map.applied_part_lsn 长期
 * "有表无写入方"的缺口，切主安全线从此比的是真实进度而非占位 0。
 */
static bool
data_entry_apply(RaftGroupCtx *ctx, const RaftLogEntry *e)
{
    StringInfoData sql;
    bool           spi_owned;
    int64          plsn = entry_partition_lsn(e->payload);
    int64          local_oid;
    bool           ok;

    if (plsn <= 0)
        return true;            /* 无进度可推，视为已应用 */

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0)
        return false;           /* P0 映射还没建好，重试而不是跳过 */

    if (!raft_persist_spi_begin(&spi_owned))
        return false;           /* 拿不到 SPI（如 BGW），下轮再来 */

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partdist.follower_set_applied_part_lsn(%u::oid, %lld)",
                     (unsigned) local_oid, (long long) plsn);
    ok = (SPI_execute(sql.data, false, 1) == SPI_OK_SELECT);
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return ok;
}

/*
 * 数据组的 Raft 日志截断必须同步截断 parwal 字节。
 *
 * 否则：被截断条目的字节仍留在本节点 pg_parwal 里占着某个 partition_lsn，
 * 而切主后新 leader 会把**不同的**记录写到同一个编号上；此时
 * AppendPartWALRecordAt 的重传去重（expected <= last 即跳过）反而会保留旧
 * 字节，物理回放就会重放错误内容。
 *
 * keep_upto_plsn 取"截断后仍保留的最后一条数据条目的 partition_lsn"。
 */
static void
data_group_truncate_parwal(RaftGroupCtx *ctx, int64 keep_upto_plsn)
{
    StringInfoData sql;
    bool           spi_owned;
    int64          local_oid;

    if (ctx->group_id == RAFT_CONTROL_GROUP)
        return;
    if (keep_upto_plsn < 0)
        return;

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0)
        return;

    if (!raft_persist_spi_begin(&spi_owned))
        return;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partdist.partwal_truncate_to(%u::oid, %lld)",
                     (unsigned) local_oid, (long long) keep_upto_plsn);
    (void) SPI_execute(sql.data, false, 1);
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
}

static void
hard_state_path(RaftGroupCtx *ctx, char *path, size_t pathlen)
{
    /* 控制面沿用原路径，保证组化前后的崩溃恢复（raft_09）语义不变 */
    if (ctx->group_id == RAFT_CONTROL_GROUP)
        snprintf(path, pathlen, "%s/pg_raft_hardstate", DataDir);
    else
        snprintf(path, pathlen, "%s/pg_raft_hardstate.%lld", DataDir,
                 (long long) ctx->group_id);
}

static bool
persist_hard_state_values(RaftGroupCtx *ctx, int64 current_term, int voted_for,
                          int64 commit_index, int64 last_applied)
{
    char              path[MAXPGPATH];
    char              tmppath[MAXPGPATH];
    RaftHardStateFile hs;
    int               fd;
    ssize_t           written;

    hard_state_path(ctx, path, sizeof(path));

    /*
     * 临时文件名必须**按进程唯一**。
     *
     * persist_hard_state_unlocked() 在读完 term/vote 后就释放了自旋锁，文件
     * I/O 完全没有跨进程同步：BGW tick 与走 prepare 路径的 client backend 可以
     * 同时进入本函数。若共用固定的 "<path>.tmp"：
     *   1. 两者 open(O_TRUNC) 到同一个文件、各写各的；
     *   2. 先完成者 rename 走，后到者的 rename 报 ENOENT ——
     *      **该次 hardstate 没有落盘，而 Raft 已经据其 term/vote 行动了**；
     *      崩溃重启后可能在同一任期内重复投票。
     *   3. 失败分支的 unlink(tmppath) 还会误删第三个写入者正在写的 tmp，
     *      把单次丢失放大成连环丢失。
     * 实测后果：L1 验收中 group 102027 连环改选（node2→node3→node4），
     * 原 placement 节点丢主后写入被拒，下游误判成回放缺陷。
     */
    snprintf(tmppath, sizeof(tmppath), "%s.tmp.%d", path, (int) MyProcPid);

    hs.magic = RAFT_HARDSTATE_MAGIC;
    hs.version = RAFT_HARDSTATE_VERSION;
    hs.current_term = current_term;
    hs.voted_for = voted_for;
    hs.commit_index = commit_index;
    hs.last_applied = last_applied;

    fd = open(tmppath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0)
    {
        elog(WARNING, "pg_raft: 无法打开 hardstate 临时文件 \"%s\": %m", tmppath);
        return false;
    }

    written = write(fd, &hs, sizeof(hs));
    if (written != (ssize_t) sizeof(hs))
    {
        elog(WARNING, "pg_raft: 写入 hardstate 失败 \"%s\": %m", tmppath);
        close(fd);
        unlink(tmppath);
        return false;
    }

    if (pg_fsync(fd) != 0)
    {
        elog(WARNING, "pg_raft: fsync hardstate 失败 \"%s\": %m", tmppath);
        close(fd);
        unlink(tmppath);
        return false;
    }

    close(fd);

    if (rename(tmppath, path) != 0)
    {
        elog(WARNING, "pg_raft: 重命名 hardstate 文件失败 \"%s\": %m", path);
        unlink(tmppath);
        return false;
    }

    return true;
}

static void
persist_hard_state_unlocked(RaftGroupCtx *ctx)
{
    int64 current_term;
    int   voted_for;
    int64 commit_index;
    int64 last_applied;

    SpinLockAcquire(&ctx->cons->mutex);
    current_term = ctx->cons->current_term;
    voted_for = ctx->cons->voted_for;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    commit_index = ctx->log->commit_index;
    last_applied = ctx->log->last_applied;
    SpinLockRelease(&ctx->log->mutex);

    (void) persist_hard_state_values(ctx, current_term, voted_for, commit_index,
                                     last_applied);
}

static void
restore_hard_state_if_needed(RaftGroupCtx *ctx)
{
    char              path[MAXPGPATH];
    RaftHardStateFile hs;
    int               fd;
    ssize_t           nread;

    if (ctx->g->hs_loaded)
        return;

    ctx->g->hs_loaded = true;
    hard_state_path(ctx, path, sizeof(path));

    fd = open(path, O_RDONLY, 0);
    if (fd < 0)
        return;

    memset(&hs, 0, sizeof(hs));
    nread = read(fd, &hs, sizeof(hs));
    close(fd);

    /*
     * v1 文件比 v2 短一个 int64，只能按 v1 的长度读到。先做长度/魔数/版本
     * 校验，通过之后再决定 last_applied 是否可信 —— 顺序反了就会读到未初始化
     * 内存。memset 保证 v1 情况下 last_applied 天然为 0。
     */
    if (nread < (ssize_t) RAFT_HARDSTATE_V1_SIZE ||
        hs.magic != RAFT_HARDSTATE_MAGIC ||
        hs.version < RAFT_HARDSTATE_VERSION_MIN ||
        hs.version > RAFT_HARDSTATE_VERSION ||
        (hs.version >= RAFT_HARDSTATE_VERSION &&
         nread != (ssize_t) sizeof(hs)))
    {
        elog(WARNING, "pg_raft: 忽略损坏的 hardstate 文件 \"%s\"", path);
        return;
    }

    if (hs.version < RAFT_HARDSTATE_VERSION)
        hs.last_applied = 0;    /* v1 没有该字段，退化为"从 0 起重放" */

    SpinLockAcquire(&ctx->cons->mutex);
    if (hs.current_term > ctx->cons->current_term)
        ctx->cons->current_term = hs.current_term;
    ctx->cons->voted_for = hs.voted_for;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    if (hs.commit_index > ctx->log->commit_index)
        ctx->log->commit_index = hs.commit_index;
    if (hs.last_applied > ctx->log->last_applied)
        ctx->log->last_applied = hs.last_applied;
    if (ctx->log->last_applied > ctx->log->commit_index)
        ctx->log->last_applied = ctx->log->commit_index;
    SpinLockRelease(&ctx->log->mutex);
}

static bool
raft_persist_spi_begin(bool *spi_owned)
{
    if (!pg_raft_spi_begin(spi_owned))
        return false;
    if (*spi_owned)
        PushActiveSnapshot(GetTransactionSnapshot());
    return true;
}

static void
raft_persist_spi_end(bool spi_owned)
{
    if (spi_owned)
    {
        PopActiveSnapshot();
        pg_raft_spi_end(true);
    }
}

/* partdist.raft_log 是否已带 group_id 列（组化后的形态） */
static bool
raft_log_table_ready(void)
{
    return (SPI_execute("SELECT 1 FROM information_schema.columns "
                        "WHERE table_schema = 'partdist' "
                        "AND table_name = 'raft_log' "
                        "AND column_name = 'group_id'",
                        true, 1) == SPI_OK_SELECT && SPI_processed > 0);
}

static void
delete_log_entry_sql(RaftGroupCtx *ctx, int64 index)
{
    StringInfoData sql;
    bool           spi_owned;

    if (index <= 0 || !raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "DELETE FROM partdist.raft_log "
                     "WHERE group_id = %lld AND log_index = %lld",
                     (long long) ctx->group_id, (long long) index);
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);

    raft_persist_spi_end(spi_owned);
}

static RaftLogEntry *
log_slot(RaftGroupCtx *ctx, int64 index)
{
    if (index <= 0)
        return NULL;
    return &ctx->log->ring[(index - 1) % RAFT_LOG_CAPACITY];
}

static bool
log_get_entry_locked(RaftGroupCtx *ctx, int64 index, RaftLogEntry *out)
{
    RaftLogEntry *e;

    if (index <= 0 || index > ctx->log->last_log_index)
        return false;
    e = log_slot(ctx, index);
    if (e->index != index)
        return false;
    *out = *e;
    return true;
}

static void
current_last_log_info_locked(RaftGroupCtx *ctx, int64 *last_idx, int64 *last_term)
{
    RaftLogEntry entry;

    *last_idx = ctx->log->last_log_index;
    *last_term = 0;

    if (*last_idx <= 0)
        return;

    if (log_get_entry_locked(ctx, *last_idx, &entry))
        *last_term = entry.term;
}

static bool
candidate_log_is_up_to_date_locked(RaftGroupCtx *ctx, int64 cand_last_idx,
                                   int64 cand_last_term)
{
    int64 local_last_idx;
    int64 local_last_term;

    current_last_log_info_locked(ctx, &local_last_idx, &local_last_term);

    if (cand_last_term != local_last_term)
        return cand_last_term > local_last_term;
    return cand_last_idx >= local_last_idx;
}

static int64
log_append_locked(RaftGroupCtx *ctx, int64 term, const char *op_type,
                  const char *payload)
{
    int64         idx;
    RaftLogEntry *e;

    if (ctx->log->last_log_index - ctx->log->last_applied >= RAFT_LOG_CAPACITY - 1)
    {
        elog(WARNING, "pg_raft: group %lld log ring full, cannot append",
             (long long) ctx->group_id);
        return 0;
    }

    idx = ctx->log->last_log_index + 1;
    e = log_slot(ctx, idx);
    e->index = idx;
    e->term = term;
    strlcpy(e->op_type, op_type, RAFT_OP_LEN);
    strlcpy(e->payload, payload, RAFT_PAYLOAD_MAX);
    ctx->log->last_log_index = idx;
    return idx;
}

static void
log_truncate_after_locked(RaftGroupCtx *ctx, int64 index)
{
    if (index < ctx->log->last_log_index)
        ctx->log->last_log_index = index;
    if (ctx->log->commit_index > index)
        ctx->log->commit_index = index;
    if (ctx->log->last_applied > index)
        ctx->log->last_applied = index;
}

/*
 * 应用一条已提交条目。
 * 控制面（group 0）走 partdist 元数据表；数据组的 apply 在 P2 落地（平凡 apply：
 * 只落盘段文件 + 推进 applied_part_lsn），P1 阶段仅推进 last_applied 游标。
 */
static bool
apply_one_entry(RaftGroupCtx *ctx, const RaftLogEntry *e)
{
    if (ctx->group_id == RAFT_CONTROL_GROUP)
    {
        /*
         * 控制面**保持组化前的语义：失败也推进游标**。这里刻意不做重试 ——
         * 控制面 apply 写的是幂等的 partdist 元数据表，而一条永久失败的
         * payload 若卡住游标，整个 failover 通道都会停摆，代价远大于漏一条。
         * 数据面相反：漏一条 redo 就是堆表分叉，所以下面必须重试。
         */
        if (!pg_raft_apply_payload_sql(e->op_type, e->payload))
            elog(WARNING, "pg_raft: 控制面条目 %s 应用失败，按原语义跳过",
                 e->op_type);
        return true;
    }
    if (strcmp(e->op_type, RAFT_OP_PARWAL) == 0)
        return data_entry_apply(ctx, e);
    return true;        /* 组内的非数据条目（如 OP_TEST）无副作用 */
}

static void
persist_log_entry_sql(RaftGroupCtx *ctx, int64 index, int64 term,
                      const char *op_type, const char *payload, bool committed)
{
    StringInfoData sql;
    bool           spi_owned;

    if (!raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO partdist.raft_log "
                     "(group_id, log_index, term, op_type, payload, committed) "
                     "VALUES (%lld, %lld, %lld, %s, %s::jsonb, %s) "
                     "ON CONFLICT (group_id, log_index) DO UPDATE SET "
                     "term = EXCLUDED.term, op_type = EXCLUDED.op_type, "
                     "payload = EXCLUDED.payload, committed = partdist.raft_log.committed OR EXCLUDED.committed",
                     (long long) ctx->group_id,
                     (long long) index,
                     (long long) term,
                     quote_literal_cstr(op_type),
                     quote_literal_cstr(payload),
                     committed ? "true" : "false");
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);

    raft_persist_spi_end(spi_owned);
}

static void
mark_log_committed_sql(RaftGroupCtx *ctx, int64 upto_index)
{
    StringInfoData sql;
    bool           spi_owned;

    if (upto_index <= 0 || !raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "UPDATE partdist.raft_log SET committed = true "
                     "WHERE group_id = %lld AND log_index <= %lld AND committed = false",
                     (long long) ctx->group_id, (long long) upto_index);
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);

    raft_persist_spi_end(spi_owned);
}

/*
 * 从 partdist.raft_group 恢复组注册表（重启后 shmem 是空的，只有 group 0）。
 * 与日志恢复同理，只能在有 SPI 的 backend 路径里做。
 */
static void
restore_groups_if_needed(void)
{
    bool spi_owned;
    int  ret;
    int  i;

    if (groups_restored || RaftGroups == NULL)
        return;

    if (!raft_persist_spi_begin(&spi_owned))
        return;

    if (SPI_execute("SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema = 'partdist' AND table_name = 'raft_group'",
                    true, 1) != SPI_OK_SELECT || SPI_processed == 0)
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    ret = SPI_execute("SELECT group_id, coalesce(array_to_string(members, ','), '') "
                      "FROM partdist.raft_group ORDER BY group_id", true, 0);
    if (ret != SPI_OK_SELECT)
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    for (i = 0; i < (int) SPI_processed; i++)
    {
        HeapTuple    tup = SPI_tuptable->vals[i];
        TupleDesc    desc = SPI_tuptable->tupdesc;
        bool         isnull;
        int64        gid;
        char        *mstr;
        int          members[RAFT_MAX_PEERS];
        int          n_members = 0;
        char        *tok;
        char        *saveptr = NULL;
        RaftGroupCtx ctx;

        gid = DatumGetInt64(SPI_getbinval(tup, desc, 1, &isnull));
        if (isnull || gid == RAFT_CONTROL_GROUP)
            continue;
        mstr = TextDatumGetCString(SPI_getbinval(tup, desc, 2, &isnull));

        for (tok = strtok_r(mstr, ",", &saveptr);
             tok != NULL && n_members < RAFT_MAX_PEERS;
             tok = strtok_r(NULL, ",", &saveptr))
            members[n_members++] = atoi(tok);

        if (raft_group_ensure(gid, members, n_members, &ctx))
        {
            /*
             * 注册表里可能是空成员集（历史行数据，或建组时成员集由
             * partition_map 导出而注册表只存了 '{}'）。重启后必须重新导出，
             * 否则该组恢复成"成员集未知"而永久不参与选举（DTX_2PC_DESIGN.md §9.2）。
             * 本函数本就在 SPI 可用的路径上，可以直接查 partition_map。
             */
            (void) group_resolve_membership(&ctx);
        }
        pfree(mstr);
    }

    raft_persist_spi_end(spi_owned);
    groups_restored = true;
}

/*
 * Load durable log state into shared memory after postmaster restart.
 * This is intentionally called only from SQL/client backend paths, not from
 * the Raft BGWorker, because restoring requires SPI and Citus hooks.
 */
static void
restore_persistent_log_if_needed(RaftGroupCtx *ctx)
{
    bool           spi_owned;
    int            ret;
    int            i;
    bool           clamped_hardstate = false;
    StringInfoData sql;

    SpinLockAcquire(&ctx->log->mutex);
    if (ctx->log->last_log_index > 0)
    {
        SpinLockRelease(&ctx->log->mutex);
        return;
    }
    SpinLockRelease(&ctx->log->mutex);

    if (!raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT log_index, term, op_type, payload::text, committed "
                     "FROM partdist.raft_log WHERE group_id = %lld ORDER BY log_index",
                     (long long) ctx->group_id);
    ret = SPI_execute(sql.data, true, 0);
    pfree(sql.data);
    if (ret != SPI_OK_SELECT)
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    SpinLockAcquire(&ctx->log->mutex);
    for (i = 0; i < (int) SPI_processed; i++)
    {
        HeapTuple     tup = SPI_tuptable->vals[i];
        TupleDesc     desc = SPI_tuptable->tupdesc;
        bool          isnull;
        int64         idx;
        int64         term;
        char         *op;
        char         *payload;
        bool          committed;
        RaftLogEntry *e;

        idx = DatumGetInt64(SPI_getbinval(tup, desc, 1, &isnull));
        term = DatumGetInt64(SPI_getbinval(tup, desc, 2, &isnull));
        op = TextDatumGetCString(SPI_getbinval(tup, desc, 3, &isnull));
        payload = TextDatumGetCString(SPI_getbinval(tup, desc, 4, &isnull));
        committed = DatumGetBool(SPI_getbinval(tup, desc, 5, &isnull));

        e = log_slot(ctx, idx);
        e->index = idx;
        e->term = term;
        strlcpy(e->op_type, op, RAFT_OP_LEN);
        strlcpy(e->payload, payload, RAFT_PAYLOAD_MAX);

        if (idx > ctx->log->last_log_index)
            ctx->log->last_log_index = idx;
        if (committed && idx > ctx->log->commit_index)
            ctx->log->commit_index = idx;

        pfree(op);
        pfree(payload);
    }
    /*
     * 如果 SQL 日志被重建或清空，但 hardstate 文件仍保留旧 commit_index，
     * 不能让 last_applied 越过真实日志末端，否则新日志会永远跳过 apply。
     */
    if (ctx->log->commit_index > ctx->log->last_log_index)
    {
        ctx->log->commit_index = ctx->log->last_log_index;
        clamped_hardstate = true;
    }
    if (ctx->log->last_applied > ctx->log->commit_index)
        ctx->log->last_applied = ctx->log->commit_index;

    /*
     * 这里**不能**再无条件 last_applied = commit_index。
     *
     * 原逻辑假定"元数据表也是持久的，重放没必要"。对控制面成立，对数据面不成立：
     * 崩溃时"已提交但未 apply"的条目重启后会被直接跳过而非重放，物理回放下就是
     * 堆表永久分叉且无任何告警。last_applied 现在随 hardstate 持久化（v2），
     * 由 restore_hard_state_if_needed() 恢复，这里只做不越界的钳制。
     */
    if (ctx->log->last_applied > ctx->log->commit_index)
        ctx->log->last_applied = ctx->log->commit_index;
    SpinLockRelease(&ctx->log->mutex);

    raft_persist_spi_end(spi_owned);
    if (clamped_hardstate)
        persist_hard_state_unlocked(ctx);
}

static void
group_apply_pending(RaftGroupCtx *ctx)
{
    RaftLogEntry e;

    for (;;)
    {
        int64 idx;
        bool  applied_ok;

        SpinLockAcquire(&ctx->log->mutex);
        if (ctx->log->last_applied >= ctx->log->commit_index)
        {
            SpinLockRelease(&ctx->log->mutex);
            break;
        }
        if (ctx->log->apply_in_progress)
        {
            /* 另一个 backend 正在 apply 本组，交给它按序做完 */
            SpinLockRelease(&ctx->log->mutex);
            break;
        }
        idx = ctx->log->last_applied + 1;
        if (!log_get_entry_locked(ctx, idx, &e))
        {
            int64 oldest = 0;
            bool  skipped = false;

            /*
             * 环里已没有这条：重启后 SQL 日志长于环容量时，restore 只能把最新
             * RAFT_LOG_CAPACITY 条灌回环，恢复出的 last_applied 若低于环窗口，
             * apply 会在这里永久卡死；容量检查（last_log_index - last_applied）
             * 随之恒满，本节点从此拒收一切新条目——环满诚实拒绝(不再假 ack)后
             * 这条链在回归里整体停摆过一次。控制面语义本就是"元数据表持久、
             * 跳过安全"(见 apply_one_entry)，把游标快进到环内最老一条继续。
             * 数据面不允许跳——漏一条 redo 就是堆表分叉，维持卡住等追平通道。
             */
            if (ctx->group_id == RAFT_CONTROL_GROUP)
            {
                RaftLogEntry probe;
                int64        lo = ctx->log->last_log_index - RAFT_LOG_CAPACITY + 1;
                int64        p;

                /*
                 * 环不一定是满的（节点的 SQL 日志可能只覆盖它在线期间收到的
                 * 一段），不能按容量倒推窗口起点；在 [下一条, commit] 里向前
                 * 扫描第一条真实存在的条目（至多 CAPACITY 次探测）。
                 */
                if (lo < idx + 1)
                    lo = idx + 1;
                for (p = lo; p <= ctx->log->commit_index; p++)
                {
                    if (log_get_entry_locked(ctx, p, &probe))
                    {
                        ctx->log->last_applied = p - 1;
                        oldest = p;
                        skipped = true;
                        break;
                    }
                }
            }
            SpinLockRelease(&ctx->log->mutex);
            if (skipped)
            {
                elog(WARNING,
                     "pg_raft: group 0 apply 游标 %lld 已滑出环窗口，快进到 %lld"
                     "（被跳过的控制面条目已反映在持久化元数据表/快照中）",
                     (long long) idx, (long long) oldest);
                persist_hard_state_unlocked(ctx);
                continue;
            }
            break;
        }
        ctx->log->apply_in_progress = true;
        SpinLockRelease(&ctx->log->mutex);

        /*
         * **游标只在 apply 成功之后才推进**。原先是先推进再 apply，一旦
         * apply 抛错或进程在两者之间死掉，这条就被永久标记为已应用却从未
         * 生效；物理回放下这等于静默丢一条 redo。
         */
        applied_ok = apply_one_entry(ctx, &e);

        SpinLockAcquire(&ctx->log->mutex);
        ctx->log->apply_in_progress = false;
        if (applied_ok && ctx->log->last_applied < idx)
            ctx->log->last_applied = idx;
        SpinLockRelease(&ctx->log->mutex);

        if (!applied_ok)
            break;      /* 下一轮 tick 重试同一条 */

        persist_hard_state_unlocked(ctx);
    }
}

void
pg_raft_consensus_apply_pending(void)
{
    RaftGroupCtx ctx;

    if (!raft_control_ctx(&ctx))
        return;
    /* client backend 路径：重启后先把文件/SQL 日志里的状态恢复进 shmem 再 apply */
    restore_hard_state_if_needed(&ctx);
    restore_persistent_log_if_needed(&ctx);
    group_apply_pending(&ctx);
}

static void
advance_commit_index_locked(RaftGroupCtx *ctx, int64 leader_commit)
{
    int64 n = leader_commit;

    if (n > ctx->log->last_log_index)
        n = ctx->log->last_log_index;
    if (n > ctx->log->commit_index)
        ctx->log->commit_index = n;
}

static void
init_leader_replication(RaftGroupCtx *ctx)
{
    int64 last;
    int   i;

    SpinLockAcquire(&ctx->log->mutex);
    last = ctx->log->last_log_index + 1;
    for (i = 0; i < RAFT_MAX_PEERS; i++)
    {
        ctx->log->peer_next_index[i] = last;
        ctx->log->peer_match_index[i] = 0;
    }
    ctx->log->repl_inited = true;
    SpinLockRelease(&ctx->log->mutex);
}

/* ---- peers ---- */

static void
parse_peers(void)
{
    char *buf;
    char *tok;
    char *saveptr = NULL;

    if (peers_parsed)
        return;

    n_peers = 0;
    if (pg_raft_peers == NULL || pg_raft_peers[0] == '\0')
    {
        peers_parsed = true;
        return;
    }

    buf = pstrdup(pg_raft_peers);
    for (tok = strtok_r(buf, ",", &saveptr);
         tok != NULL && n_peers < RAFT_MAX_PEERS;
         tok = strtok_r(NULL, ",", &saveptr))
    {
        int  id = 0;
        int  port = 0;
        char host[256];

        if (sscanf(tok, " %d@%255[^:]:%d", &id, host, &port) == 3)
        {
            peers[n_peers].node_id = id;
            strlcpy(peers[n_peers].host, host, sizeof(peers[n_peers].host));
            peers[n_peers].port = port;
            n_peers++;
        }
    }
    pfree(buf);
    peers_parsed = true;
}

/* 该 peer 槽位是否参与本组复制（非本节点，且属于本组成员集） */
static bool
peer_in_group(RaftGroupCtx *ctx, int slot)
{
    if (peers[slot].node_id == pg_raft_node_id)
        return false;
    return group_has_member(ctx, peers[slot].node_id);
}

static void
reset_election_deadline_locked(RaftGroupCtx *ctx)
{
    long base = pg_raft_election_timeout_ms;
    long lo = base;          /* 缺省窗口 [base, 2*base) */
    long span = base;
    long jitter;

    /*
     * 控制面 leader 优先落在协调节点(master)：给它一个与其他节点完全不相交的
     * 更短选举窗口 [2b/3, b)，其他节点仍是 [b, 2b)——协调节点在世时总是先超时、
     * 先集齐多数派；它宕机时其他节点照常接管。这是偏好不是保证：worker 接任后
     * 心跳会不断刷新协调节点的 deadline，领导权不会自动抢回，直到下一次改选。
     * 下限 2b/3（缺省 1000ms）仍是心跳间隔(400ms)的 2.5 倍，不会误触发选举。
     */
    if (ctx->group_id == RAFT_CONTROL_GROUP &&
        pg_raft_coordinator_node_id > 0 &&
        pg_raft_node_id == pg_raft_coordinator_node_id)
    {
        lo = (base * 2) / 3;
        span = base - lo;
    }
    jitter = (span > 0) ? (random() % span) : 0;

    ctx->cons->election_deadline =
        GetCurrentTimestamp() + (lo + jitter) * 1000L;
}

/* ---- libpq RPC ---- */

static bool
parse_resp2(const char *val, int64 *term, int *flag)
{
    return (val != NULL &&
            sscanf(val, "%ld %d", (long *) term, flag) == 2);
}

/*
 * 对端不可达时的退避窗口（每进程）。
 *
 * 组化后一次 tick 要为 N 个组各发一轮 RPC；若某对端宕机，每个组都要各自吃一次
 * connect_timeout（~1s），组一多就会把 tick 拖垮，导致控制面心跳发不出去、
 * 引发无谓改选（§11.6「选举风暴」）。这里按**对端**而非按组记录失败：一个组
 * 撞到不可达对端后，同一退避窗口内其余组直接跳过该对端。
 */
static TimestampTz peer_backoff_until[RAFT_MAX_PEERS];

static int
peer_slot_of(RaftPeer *p)
{
    return (int) (p - peers);
}

static bool
peer_in_backoff(RaftPeer *p)
{
    int slot = peer_slot_of(p);

    if (slot < 0 || slot >= RAFT_MAX_PEERS)
        return false;
    return (peer_backoff_until[slot] != 0 &&
            GetCurrentTimestamp() < peer_backoff_until[slot]);
}

static void
peer_mark_result(RaftPeer *p, bool reachable)
{
    int slot = peer_slot_of(p);

    if (slot < 0 || slot >= RAFT_MAX_PEERS)
        return;

    if (reachable)
        peer_backoff_until[slot] = 0;
    else
        peer_backoff_until[slot] =
            GetCurrentTimestamp() + (int64) pg_raft_heartbeat_ms * 1000L;
}

/*
 * 按对端缓存的 libpq 连接（每进程）。
 *
 * 组化前每 tick 只有一组、每对端一次 PQconnectdb 尚可忍受；N 个组之后，每轮
 * tick 的建连次数是 组数 × 对端数，握手开销会把 tick 周期撑爆，进而拖慢控制面
 * 心跳、让依赖时序的回归偶发失败。这里把连接按对端复用（§11.6 #2「心跳按对端
 * 合并」的第一步：合并的是连接，报文仍按组发），连接坏掉时丢弃重建。
 */
static PGconn *peer_conn[RAFT_MAX_PEERS];

static void
peer_conn_reset(int slot)
{
    if (slot < 0 || slot >= RAFT_MAX_PEERS)
        return;
    if (peer_conn[slot] != NULL)
    {
        PQfinish(peer_conn[slot]);
        peer_conn[slot] = NULL;
    }
}

static PGconn *
peer_conn_get(RaftPeer *p)
{
    int   slot = peer_slot_of(p);
    char  conninfo[512];

    if (slot < 0 || slot >= RAFT_MAX_PEERS)
        return NULL;

    if (peer_conn[slot] != NULL)
    {
        if (PQstatus(peer_conn[slot]) == CONNECTION_OK)
            return peer_conn[slot];
        peer_conn_reset(slot);
    }

    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%d dbname=postgres user=postgres "
             "connect_timeout=1 application_name=pg_raft_rpc",
             p->host, p->port);

    peer_conn[slot] = PQconnectdb(conninfo);
    if (PQstatus(peer_conn[slot]) != CONNECTION_OK)
    {
        peer_conn_reset(slot);
        return NULL;
    }
    return peer_conn[slot];
}

static bool
send_sql_rpc(RaftPeer *p, const char *sql, bool honor_backoff,
             int64 *resp_term, int *resp_flag)
{
    PGconn     *conn;
    PGresult   *res;
    bool        ok = false;

    /*
     * 退避只对心跳/复制生效。选举必须逐个真问一遍：候选人少收一票就可能选不出
     * leader，把无谓的等待放大成控制面的长时间无主。
     */
    if (honor_backoff && peer_in_backoff(p))
        return false;

    conn = peer_conn_get(p);
    if (conn == NULL)
    {
        peer_mark_result(p, false);
        return false;
    }

    res = PQexec(conn, sql);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
        ok = parse_resp2(PQgetvalue(res, 0, 0), resp_term, resp_flag);
    else if (PQstatus(conn) != CONNECTION_OK)
    {
        /* 对端重启/断链：丢弃缓存连接，下次重连 */
        PQclear(res);
        peer_conn_reset(peer_slot_of(p));
        peer_mark_result(p, false);
        return false;
    }

    PQclear(res);
    peer_mark_result(p, true);
    return ok;
}

static bool
send_rpc_msg(RaftPeer *p, const char *msg, int64 *resp_term, int *resp_flag)
{
    char sql[256];

    snprintf(sql, sizeof(sql), "SELECT partdist.pg_raft_rpc('%s')", msg);
    return send_sql_rpc(p, sql, false, resp_term, resp_flag);
}

static bool
send_append_entries_rpc(RaftGroupCtx *ctx, RaftPeer *p, int64 term, int leader_id,
                        int64 prev_idx, int64 prev_term, int64 leader_commit,
                        int64 entry_idx, int64 entry_term,
                        const char *entry_op, const char *entry_payload,
                        const char *entry_data_hex,
                        int64 *resp_term, int *resp_flag)
{
    StringInfoData sql;
    bool           ok;

    initStringInfo(&sql);
    if (entry_idx > 0 && entry_op != NULL && entry_payload != NULL)
    {
        appendStringInfo(&sql,
                         "SELECT partdist.pg_raft_append_entries("
                         "%lld::bigint, %d, %lld::bigint, %lld::bigint, %lld::bigint, "
                         "%lld::bigint, %lld::bigint, %s, %s, %lld::bigint, ",
                         (long long) term, leader_id,
                         (long long) prev_idx, (long long) prev_term,
                         (long long) leader_commit,
                         (long long) entry_idx, (long long) entry_term,
                         quote_literal_cstr(entry_op),
                         quote_literal_cstr(entry_payload),
                         (long long) ctx->group_id);
        if (entry_data_hex != NULL)
            appendStringInfo(&sql, "decode(%s, 'hex'))",
                             quote_literal_cstr(entry_data_hex));
        else
            appendStringInfoString(&sql, "NULL)");
    }
    else
    {
        appendStringInfo(&sql,
                         "SELECT partdist.pg_raft_append_entries("
                         "%lld::bigint, %d, %lld::bigint, %lld::bigint, %lld::bigint, "
                         "NULL, NULL, NULL, NULL, %lld::bigint, NULL)",
                         (long long) term, leader_id,
                         (long long) prev_idx, (long long) prev_term,
                         (long long) leader_commit,
                         (long long) ctx->group_id);
    }

    ok = send_sql_rpc(p, sql.data, true, resp_term, resp_flag);
    pfree(sql.data);
    return ok;
}

static void
step_down_if_higher(RaftGroupCtx *ctx, int64 their_term)
{
    bool need_persist = false;

    SpinLockAcquire(&ctx->cons->mutex);
    if (their_term > ctx->cons->current_term)
    {
        ctx->cons->current_term = their_term;
        ctx->cons->state = RAFT_FOLLOWER;
        ctx->cons->voted_for = 0;
        ctx->cons->leader_id = 0;
        SpinLockAcquire(&ctx->log->mutex);
        ctx->log->repl_inited = false;
        SpinLockRelease(&ctx->log->mutex);
        reset_election_deadline_locked(ctx);
        need_persist = true;
    }
    SpinLockRelease(&ctx->cons->mutex);

    if (need_persist)
        persist_hard_state_unlocked(ctx);
}

/* 根据 match_index 计算可提交的最大 index（多数派已复制） */
static int64
compute_new_commit_index(RaftGroupCtx *ctx, int64 current_term)
{
    int64 matches[RAFT_MAX_PEERS + 1];
    int   n = 0;
    int   i;
    int64 idx;
    RaftLogEntry e;

    matches[n++] = ctx->log->last_log_index;
    for (i = 0; i < n_peers; i++)
    {
        if (!peer_in_group(ctx, i))
            continue;
        matches[n++] = ctx->log->peer_match_index[i];
    }

    /* 简单选择排序找第 majority 大的值 */
    for (i = 0; i < n - 1; i++)
    {
        int j;
        for (j = i + 1; j < n; j++)
        {
            if (matches[j] > matches[i])
            {
                int64 t = matches[i];
                matches[i] = matches[j];
                matches[j] = t;
            }
        }
    }

    idx = matches[cluster_majority(ctx) - 1];
    if (idx <= ctx->log->commit_index)
        return ctx->log->commit_index;

    if (log_get_entry_locked(ctx, idx, &e) && e.term == current_term)
        return idx;
    return ctx->log->commit_index;
}

static void
replicate_to_peer(RaftGroupCtx *ctx, int peer_slot)
{
    int64 term;
    int   leader_id = pg_raft_node_id;
    int64 prev_idx;
    int64 prev_term = 0;
    int64 leader_commit;
    RaftLogEntry entry;
    int64 rt;
    int   ok_flag;
    bool  has_entry;
    bool  repl_ready;
    char *data_hex = NULL;

    SpinLockAcquire(&ctx->cons->mutex);
    if (ctx->cons->state != RAFT_LEADER)
    {
        SpinLockRelease(&ctx->cons->mutex);
        return;
    }
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    if (!ctx->log->repl_inited)
    {
        int64 last = ctx->log->last_log_index + 1;
        int   j;

        for (j = 0; j < RAFT_MAX_PEERS; j++)
        {
            ctx->log->peer_next_index[j] = last;
            ctx->log->peer_match_index[j] = 0;
        }
        ctx->log->repl_inited = true;
    }
    repl_ready = ctx->log->repl_inited;
    leader_commit = ctx->log->commit_index;
    prev_idx = ctx->log->peer_next_index[peer_slot] - 1;
    if (prev_idx > 0)
    {
        RaftLogEntry *pe = log_slot(ctx, prev_idx);
        if (pe->index == prev_idx)
            prev_term = pe->term;
    }
    has_entry = (ctx->log->peer_next_index[peer_slot] <= ctx->log->last_log_index &&
                 log_get_entry_locked(ctx, ctx->log->peer_next_index[peer_slot], &entry));
    SpinLockRelease(&ctx->log->mutex);

    if (!repl_ready)
        return;

    /*
     * 数据组的条目必须带上真实 parwal 字节；取字节要 SPI，BGW tick 里拿不到，
     * 此时降级为心跳（不推进 next_index），等下一次 propose 再追平。
     */
    if (has_entry && strcmp(entry.op_type, RAFT_OP_PARWAL) == 0)
    {
        if (!data_shipping_allowed)
            has_entry = false;
        else
        {
            data_hex = data_entry_fetch_hex(ctx, entry_partition_lsn(entry.payload));
            if (data_hex == NULL)
                has_entry = false;
        }
    }

    if (has_entry)
    {
        if (!send_append_entries_rpc(ctx, &peers[peer_slot], term, leader_id,
                                     prev_idx, prev_term, leader_commit,
                                     entry.index, entry.term,
                                     entry.op_type, entry.payload, data_hex,
                                     &rt, &ok_flag))
            return;
    }
    else
    {
        if (!send_append_entries_rpc(ctx, &peers[peer_slot], term, leader_id,
                                     prev_idx, prev_term, leader_commit,
                                     0, 0, NULL, NULL, NULL, &rt, &ok_flag))
            return;
    }

    if (rt > term)
    {
        step_down_if_higher(ctx, rt);
        return;
    }

    if (ok_flag)
    {
        SpinLockAcquire(&ctx->log->mutex);
        if (has_entry)
        {
            ctx->log->peer_match_index[peer_slot] = entry.index;
            ctx->log->peer_next_index[peer_slot] = entry.index + 1;
        }
        else
            ctx->log->peer_match_index[peer_slot] = prev_idx;
        SpinLockRelease(&ctx->log->mutex);
    }
    else
    {
        SpinLockAcquire(&ctx->log->mutex);
        if (ctx->log->peer_next_index[peer_slot] > 1)
            ctx->log->peer_next_index[peer_slot]--;
        SpinLockRelease(&ctx->log->mutex);
    }
}

static void
leader_replicate_and_commit(RaftGroupCtx *ctx)
{
    int   i;
    int64 new_commit;
    int64 term;
    bool  ready;

    SpinLockAcquire(&ctx->log->mutex);
    ready = ctx->log->repl_inited;
    SpinLockRelease(&ctx->log->mutex);

    if (!ready)
        init_leader_replication(ctx);

    for (i = 0; i < n_peers; i++)
    {
        if (!peer_in_group(ctx, i))
            continue;
        replicate_to_peer(ctx, i);
    }

    SpinLockAcquire(&ctx->cons->mutex);
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    new_commit = compute_new_commit_index(ctx, term);
    if (new_commit > ctx->log->commit_index)
        ctx->log->commit_index = new_commit;
    SpinLockRelease(&ctx->log->mutex);
}

static void
send_heartbeats(RaftGroupCtx *ctx)
{
    leader_replicate_and_commit(ctx);
}

/* ---- 选举 ---- */

/*
 * 数据组新任 leader 向控制面登记（切主重构的上报半程）。
 *
 * BGW tick 上下文无 SPI，投递走 libpq：向 group 0 当前 leader（常态是 master）
 * 调 partdist.pg_raft_report_data_leader(gid, self, term, secondaries)。返回
 * >0（已登记）或 -1（无需登记）时清除 report_pending；0 或投递失败则保留标志，
 * 下个 tick 重试——控制面短暂无主/换主/网络抖动都天然容错。任期栅栏保证重复
 * 投递与迟到投递无害。
 */
static void
data_group_try_report(RaftGroupCtx *ctx)
{
    RaftGroupCtx ctx0;
    int          leader0;
    int64        term;
    int          state;
    int          sec[RAFT_MAX_PEERS];
    int          n_sec = 0;
    int          i;
    int          slot = -1;
    char         arrbuf[192];
    char         sql[384];
    int          off;
    PGconn      *conn;
    PGresult    *res;

    SpinLockAcquire(&ctx->cons->mutex);
    state = ctx->cons->state;
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);
    if (state != RAFT_LEADER)
    {
        ctx->g->report_pending = false;
        return;
    }

    if (!raft_group_ctx(RAFT_CONTROL_GROUP, &ctx0))
        return;
    SpinLockAcquire(&ctx0.cons->mutex);
    leader0 = ctx0.cons->leader_id;
    SpinLockRelease(&ctx0.cons->mutex);
    if (leader0 <= 0)
        return;                 /* 控制面暂无主，下个 tick 重试 */

    /*
     * secondaries = 本组成员集去掉自己。成员集未知（hearsay 自动建组，members
     * 为空）时退化为"全体 peers 去掉自己与协调节点"——协调节点永不作数据副本。
     */
    if (ctx->g->n_members > 0)
    {
        for (i = 0; i < ctx->g->n_members && n_sec < RAFT_MAX_PEERS; i++)
            if (ctx->g->members[i] != pg_raft_node_id)
                sec[n_sec++] = ctx->g->members[i];
    }
    else
    {
        for (i = 0; i < n_peers && n_sec < RAFT_MAX_PEERS; i++)
            if (peers[i].node_id != pg_raft_node_id &&
                peers[i].node_id != pg_raft_coordinator_node_id)
                sec[n_sec++] = peers[i].node_id;
    }

    off = snprintf(arrbuf, sizeof(arrbuf), "ARRAY[");
    for (i = 0; i < n_sec && off < (int) sizeof(arrbuf) - 16; i++)
        off += snprintf(arrbuf + off, sizeof(arrbuf) - off, "%s%d",
                        i ? "," : "", sec[i]);
    snprintf(arrbuf + off, sizeof(arrbuf) - off, "]::int[]");

    snprintf(sql, sizeof(sql),
             "SELECT partdist.pg_raft_report_data_leader(%lld, %d, %lld, %s)",
             (long long) ctx->group_id, pg_raft_node_id, (long long) term, arrbuf);

    for (i = 0; i < n_peers; i++)
        if (peers[i].node_id == leader0)
        {
            slot = i;
            break;
        }
    if (slot < 0)
        return;

    if (peer_in_backoff(&peers[slot]))
        return;
    conn = peer_conn_get(&peers[slot]);
    if (conn == NULL)
    {
        peer_mark_result(&peers[slot], false);
        return;
    }

    res = PQexec(conn, sql);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
    {
        long long v = atoll(PQgetvalue(res, 0, 0));

        if (v != 0)
        {
            ctx->g->report_pending = false;
            elog(LOG,
                 "pg_raft: group %lld leader node %d term %lld 已向控制面(node %d)登记 (ret=%lld)",
                 (long long) ctx->group_id, pg_raft_node_id,
                 (long long) term, leader0, v);
        }
        peer_mark_result(&peers[slot], true);
    }
    else
    {
        if (PQstatus(conn) != CONNECTION_OK)
            peer_conn_reset(slot);
        peer_mark_result(&peers[slot], false);
    }
    PQclear(res);
}

static void
start_election(RaftGroupCtx *ctx)
{
    int64 term;
    int64 last_log_idx = 0;
    int64 last_log_term = 0;
    int   votes = 1;
    int   majority = cluster_majority(ctx);
    char  msg[160];
    int   i;
    bool  won = false;

    SpinLockAcquire(&ctx->log->mutex);
    ctx->log->repl_inited = false;
    current_last_log_info_locked(ctx, &last_log_idx, &last_log_term);
    SpinLockRelease(&ctx->log->mutex);

    SpinLockAcquire(&ctx->cons->mutex);
    ctx->cons->current_term += 1;
    ctx->cons->state = RAFT_CANDIDATE;
    ctx->cons->voted_for = pg_raft_node_id;
    ctx->cons->leader_id = 0;
    term = ctx->cons->current_term;
    reset_election_deadline_locked(ctx);
    SpinLockRelease(&ctx->cons->mutex);
    persist_hard_state_unlocked(ctx);

    elog(LOG, "pg_raft: group %lld node %d 发起选举 term=%ld (需 %d 票)",
         (long long) ctx->group_id, pg_raft_node_id, (long) term, majority);

    snprintf(msg, sizeof(msg), "RV %lld %d %lld %lld %lld",
             (long long) term, pg_raft_node_id,
             (long long) last_log_idx, (long long) last_log_term,
             (long long) ctx->group_id);

    for (i = 0; i < n_peers; i++)
    {
        int64 rt = 0;
        int   granted = 0;

        if (!peer_in_group(ctx, i))
            continue;

        if (!send_rpc_msg(&peers[i], msg, &rt, &granted))
            continue;

        if (rt > term)
        {
            step_down_if_higher(ctx, rt);
            return;
        }
        if (granted)
            votes++;
    }

    if (votes >= majority)
    {
        SpinLockAcquire(&ctx->cons->mutex);
        if (ctx->cons->state == RAFT_CANDIDATE &&
            ctx->cons->current_term == term)
        {
            ctx->cons->state = RAFT_LEADER;
            ctx->cons->leader_id = pg_raft_node_id;
            won = true;
        }
        SpinLockRelease(&ctx->cons->mutex);
    }

    if (won)
    {
        elog(LOG, "pg_raft: group %lld node %d 当选 LEADER term=%ld (%d/%d 票)",
             (long long) ctx->group_id, pg_raft_node_id, (long) term,
             votes, group_cluster_size(ctx));
        persist_hard_state_unlocked(ctx);
        init_leader_replication(ctx);
        send_heartbeats(ctx);

        /*
         * 切主重构：数据组自治选出新 leader 后须向控制面登记（分区副本/节点信息
         * 上报 master，master 落到路由层）。BGW tick 无 SPI，登记走 libpq 投递，
         * 这里只置标志，由 group_tick 重试直到送达。
         */
        if (ctx->group_id != RAFT_CONTROL_GROUP)
            ctx->g->report_pending = true;
    }
}

/* 单组的一次状态机推进 */
static void
group_tick(RaftGroupCtx *ctx)
{
    int         state;
    TimestampTz deadline;
    TimestampTz now;

    /* 不是本组成员的节点不参与该组的选举/心跳 */
    if (!group_has_member(ctx, pg_raft_node_id))
        return;

    /*
     * 先从文件恢复 HardState（只读文件、无 SPI，BGW 可做）。否则重启后 BGW
     * 可能在任何 SQL 路径触发恢复之前就发起选举，把全零的 term/commit/
     * last_applied 持久化回文件，抹掉真实历史。
     */
    restore_hard_state_if_needed(ctx);

    now = GetCurrentTimestamp();

    SpinLockAcquire(&ctx->cons->mutex);
    state = ctx->cons->state;
    deadline = ctx->cons->election_deadline;
    if (deadline == 0)
    {
        reset_election_deadline_locked(ctx);
        deadline = ctx->cons->election_deadline;
    }
    SpinLockRelease(&ctx->cons->mutex);

    if (state == RAFT_LEADER)
    {
        send_heartbeats(ctx);
        if (ctx->group_id != RAFT_CONTROL_GROUP && ctx->g->report_pending)
            data_group_try_report(ctx);
        return;
    }

    if (now >= deadline)
        start_election(ctx);
}

/*
 * BGW 的一次 tick：多路复用全部活跃组。
 * 控制面（group 0）永远第一个推进，保证其行为与组化前一致。
 */
void
pg_raft_consensus_tick(void)
{
    int i;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        return;

    parse_peers();
    if (n_peers == 0)
        return;

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];
        RaftGroupCtx    ctx;

        if (!g->in_use)
            continue;

        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;

        group_tick(&ctx);
    }
}

/* ---- Propose（Leader client backend 调用） ---- */

/* 同步复制单条日志；返回收到确认的节点数（含 Leader 自身） */
static int
sync_replicate_index(RaftGroupCtx *ctx, int64 idx, int64 term)
{
    RaftLogEntry entry;
    int64        prev_idx;
    int64        prev_term = 0;
    int64        leader_commit;
    int          acks = 1;
    int          i;
    char        *data_hex = NULL;

    SpinLockAcquire(&ctx->log->mutex);
    if (!log_get_entry_locked(ctx, idx, &entry))
    {
        SpinLockRelease(&ctx->log->mutex);
        return 0;
    }
    prev_idx = idx - 1;
    if (prev_idx > 0)
    {
        RaftLogEntry *pe = log_slot(ctx, prev_idx);
        if (pe->index == prev_idx)
            prev_term = pe->term;
    }
    leader_commit = ctx->log->commit_index;
    if (!ctx->log->repl_inited)
    {
        int64 last = ctx->log->last_log_index + 1;
        int   j;

        for (j = 0; j < RAFT_MAX_PEERS; j++)
        {
            ctx->log->peer_next_index[j] = last;
            ctx->log->peer_match_index[j] = 0;
        }
        ctx->log->repl_inited = true;
    }
    SpinLockRelease(&ctx->log->mutex);

    /* 只有 parwal 数据条目才需要随行字节；组内的其它条目按普通描述符复制 */
    if (strcmp(entry.op_type, RAFT_OP_PARWAL) == 0)
    {
        data_hex = data_entry_fetch_hex(ctx, entry_partition_lsn(entry.payload));
        if (data_hex == NULL)
        {
            elog(WARNING,
                 "pg_raft: group %lld 无法读取 idx=%lld 对应的 parwal 记录，放弃复制",
                 (long long) ctx->group_id, (long long) idx);
            return 0;
        }
    }

    for (i = 0; i < n_peers; i++)
    {
        int64 rt = 0;
        int   ok = 0;

        if (!peer_in_group(ctx, i))
            continue;

        if (!send_append_entries_rpc(ctx, &peers[i], term, pg_raft_node_id,
                                     prev_idx, prev_term, leader_commit,
                                     entry.index, entry.term,
                                     entry.op_type, entry.payload, data_hex,
                                     &rt, &ok))
            continue;

        if (rt > term)
        {
            step_down_if_higher(ctx, rt);
            return acks;
        }
        if (ok)
        {
            acks++;
            SpinLockAcquire(&ctx->log->mutex);
            ctx->log->peer_match_index[i] = idx;
            ctx->log->peer_next_index[i] = idx + 1;
            SpinLockRelease(&ctx->log->mutex);
        }
    }
    return acks;
}

/*
 * 提交后尽力推送到所有可达 Follower（不仅多数派）。
 * 控制面元数据应在各存活节点最终一致，便于任意节点当选 Leader 后接管。
 */
static void
flush_replication(RaftGroupCtx *ctx, int64 idx, int64 term)
{
    int round;
    int i;

    for (round = 0; round < 10; round++)
    {
        bool pending = false;

        for (i = 0; i < n_peers; i++)
        {
            int64 match;

            if (!peer_in_group(ctx, i))
                continue;

            SpinLockAcquire(&ctx->log->mutex);
            match = ctx->log->peer_match_index[i];
            if (match < idx)
            {
                if (ctx->log->peer_next_index[i] > idx)
                    ctx->log->peer_next_index[i] = idx;
                pending = true;
            }
            SpinLockRelease(&ctx->log->mutex);

            if (match < idx)
                replicate_to_peer(ctx, i);
        }

        if (!pending)
            break;
    }
}

static void
discard_uncommitted_entry(RaftGroupCtx *ctx, int64 idx)
{
    int   i;

    if (idx <= 0)
        return;

    SpinLockAcquire(&ctx->log->mutex);
    if (idx > ctx->log->commit_index && idx == ctx->log->last_log_index)
    {
        log_truncate_after_locked(ctx, idx - 1);
        for (i = 0; i < RAFT_MAX_PEERS; i++)
        {
            if (ctx->log->peer_match_index[i] > ctx->log->last_log_index)
                ctx->log->peer_match_index[i] = ctx->log->last_log_index;
            if (ctx->log->peer_next_index[i] > ctx->log->last_log_index + 1)
                ctx->log->peer_next_index[i] = ctx->log->last_log_index + 1;
        }
    }
    SpinLockRelease(&ctx->log->mutex);

    /*
     * ★ leader 侧失败回滚**不再截断 parwal 字节**（2026-08-03 修，raft_17
     * 阶段二实测抓获的丢数据）。
     *
     * 此前这里按被丢弃条目的 plsn 调 partwal_truncate_to(plsn-1)。问题：
     * parwal 流里 plsn 之后可能已经躺着**并发事务**在 [A] 落盘的记录 ——
     * 截断连它们一起删。受害 backend 的复制挂钩随后看到
     * last_data_plsn >= flush_lsn（游标被回滚），循环空转、静默返回，
     * 其事务**带着"已复制"的假象提交**，而它的数据既不在本地 parwal
     * 也没到任何 follower（raft_17 阶段二实测：失多数派下 13/240 行
     * 如此漏网提交）。
     *
     * 现在失败条目的字节留在盘上成为"孤儿"：last_data_plsn 未推进，
     * 下一次复制（同一或另一 backend）会按同一 plsn 重新 propose 同一
     * 字节 —— 恢复多数派后自然收敛；仍失多数派则照样失败、事务照样
     * 中止。代价是 parwal 流里可能存有**已中止事务**的 DATA 记录
     * （"中止事务不在 plsn 空间留渣"的说法作废）——这与 2PC 设计一致：
     * DATA 记录本就可能属于中止的事务，可见性由标记/决议闭合
     * （DTX_2PC_DESIGN.md §5、FRD §7.6），当前阶段 follower 只存字节
     * 不回放，无正确性影响。
     *
     * follower 侧的 AppendEntries 冲突截断（handle_append_entries）
     * **保留** —— 那才是必须的：换 leader 后同一 plsn 会承载不同记录，
     * 幂等去重会错误保留旧字节。
     */

    delete_log_entry_sql(ctx, idx);
    persist_hard_state_unlocked(ctx);
}

static int64
group_propose(RaftGroupCtx *ctx, const char *op_type, const char *payload)
{
    int64 idx;
    int64 term;
    int64 committed_upto;
    int   acks;
    int   majority;
    bool  is_leader;

    SpinLockAcquire(&ctx->cons->mutex);
    is_leader = (ctx->cons->state == RAFT_LEADER);
    SpinLockRelease(&ctx->cons->mutex);
    if (!is_leader)
        return 0;

    restore_persistent_log_if_needed(ctx);
    parse_peers();
    majority = cluster_majority(ctx);

    /*
     * 先把已提交的积压 apply 掉再 append：重启后恢复出的 last_applied 可能
     * 远低于环窗口，此时容量检查(last_log_index - last_applied)会误判环满、
     * 拒绝一切新提案；group_apply_pending 里的控制面快进会先把游标追平。
     */
    group_apply_pending(ctx);

    SpinLockAcquire(&ctx->cons->mutex);
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    idx = log_append_locked(ctx, term, op_type, payload);
    SpinLockRelease(&ctx->log->mutex);

    if (idx <= 0)
        return 0;

    persist_log_entry_sql(ctx, idx, term, op_type, payload, false);
    data_shipping_allowed = true;
    acks = sync_replicate_index(ctx, idx, term);

    SpinLockAcquire(&ctx->log->mutex);
    if (acks >= majority && idx > ctx->log->commit_index)
        ctx->log->commit_index = idx;
    else
    {
        int64 nc = compute_new_commit_index(ctx, term);
        if (nc > ctx->log->commit_index)
            ctx->log->commit_index = nc;
    }
    committed_upto = ctx->log->commit_index;
    SpinLockRelease(&ctx->log->mutex);
    persist_hard_state_unlocked(ctx);

    if (committed_upto < idx)
    {
        elog(LOG,
             "pg_raft: group %lld reject propose idx=%lld term=%lld because quorum ack is insufficient (%d/%d)",
             (long long) ctx->group_id,
             (long long) idx,
             (long long) term,
             acks,
             majority);
        data_shipping_allowed = false;
        discard_uncommitted_entry(ctx, idx);
        return 0;
    }

    /* 多数派提交后，继续推送到所有 Follower 再 apply（控制面需全节点一致） */
    mark_log_committed_sql(ctx, committed_upto);
    flush_replication(ctx, idx, term);
    data_shipping_allowed = false;
    group_apply_pending(ctx);
    return idx;
}

int64
pg_raft_consensus_propose(const char *op_type, const char *payload)
{
    RaftGroupCtx ctx;

    if (!pg_raft_raft_enabled || !raft_control_ctx(&ctx))
        return 0;

    return group_propose(&ctx, op_type, payload);
}

/* ---- 观测（控制面语义，供既有调用方使用） ---- */

bool
pg_raft_consensus_is_leader(void)
{
    RaftGroupCtx ctx;
    bool         r;

    if (!raft_control_ctx(&ctx))
        return false;

    SpinLockAcquire(&ctx.cons->mutex);
    r = (ctx.cons->state == RAFT_LEADER);
    SpinLockRelease(&ctx.cons->mutex);
    return r;
}

int
pg_raft_consensus_leader_id(void)
{
    RaftGroupCtx ctx;
    int          id;

    if (!raft_control_ctx(&ctx))
        return 0;

    SpinLockAcquire(&ctx.cons->mutex);
    id = (ctx.cons->state == RAFT_LEADER)
        ? pg_raft_node_id : ctx.cons->leader_id;
    SpinLockRelease(&ctx.cons->mutex);
    return id;
}

int64
pg_raft_consensus_term(void)
{
    RaftGroupCtx ctx;
    int64        t;

    if (!raft_control_ctx(&ctx))
        return 0;

    SpinLockAcquire(&ctx.cons->mutex);
    t = ctx.cons->current_term;
    SpinLockRelease(&ctx.cons->mutex);
    return t;
}

int64
pg_raft_consensus_commit_index(void)
{
    RaftGroupCtx ctx;
    int64        c;

    if (!raft_control_ctx(&ctx))
        return 0;

    SpinLockAcquire(&ctx.log->mutex);
    c = ctx.log->commit_index;
    SpinLockRelease(&ctx.log->mutex);
    return c;
}

int64
pg_raft_consensus_last_applied(void)
{
    RaftGroupCtx ctx;
    int64        a;

    if (!raft_control_ctx(&ctx))
        return 0;

    SpinLockAcquire(&ctx.log->mutex);
    a = ctx.log->last_applied;
    SpinLockRelease(&ctx.log->mutex);
    return a;
}

/* ---- AppendEntries 接收方 ---- */

static bool
handle_append_entries(RaftGroupCtx *ctx, int64 in_term, int leader_id,
                      int64 prev_idx, int64 prev_term,
                      int64 leader_commit,
                      int64 entry_idx, int64 entry_term,
                      const char *entry_op, const char *entry_payload,
                      const char *entry_data_hex,
                      int64 *out_term, int *success)
{
    RaftLogEntry prev;
    bool         has_entry = (entry_idx > 0 && entry_op != NULL && entry_payload != NULL);
    int64        commit_to_mark;
    int64        conflict_plsn = -1;

    *success = 0;
    restore_hard_state_if_needed(ctx);
    restore_persistent_log_if_needed(ctx);

    SpinLockAcquire(&ctx->cons->mutex);
    if (in_term > ctx->cons->current_term)
    {
        ctx->cons->current_term = in_term;
        ctx->cons->state = RAFT_FOLLOWER;
        ctx->cons->voted_for = 0;
        ctx->cons->leader_id = 0;
    }
    *out_term = ctx->cons->current_term;

    if (in_term < ctx->cons->current_term)
    {
        SpinLockRelease(&ctx->cons->mutex);
        return true;
    }

    ctx->cons->state = RAFT_FOLLOWER;
    ctx->cons->leader_id = leader_id;
    reset_election_deadline_locked(ctx);
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);

    if (prev_idx > 0)
    {
        if (!log_get_entry_locked(ctx, prev_idx, &prev) || prev.term != prev_term)
        {
            SpinLockRelease(&ctx->log->mutex);
            return true;
        }
    }

    if (has_entry)
    {
        if (entry_idx <= ctx->log->last_log_index)
        {
            RaftLogEntry *exist = log_slot(ctx, entry_idx);
            if (exist->index == entry_idx &&
                (exist->term != entry_term ||
                 strcmp(exist->op_type, entry_op) != 0 ||
                 strcmp(exist->payload, entry_payload) != 0))
            {
                if (strcmp(exist->op_type, RAFT_OP_PARWAL) == 0)
                    conflict_plsn = entry_partition_lsn(exist->payload);
                log_truncate_after_locked(ctx, entry_idx - 1);
            }
        }
        if (entry_idx == ctx->log->last_log_index + 1)
        {
            /*
             * 环满时 append 会失败；此时绝不能 ack（success 保持 0），否则
             * leader 会把一条本节点根本没有的条目计入多数派——提交点可能
             * 覆盖到少数派都不持有的日志，破坏 Leader Completeness。
             * 不 ack 则 leader 按失败路径退避重试，待 apply 推进腾出环位。
             */
            if (log_append_locked(ctx, entry_term, entry_op, entry_payload) <= 0)
            {
                SpinLockRelease(&ctx->log->mutex);
                return true;
            }
            SpinLockRelease(&ctx->log->mutex);
            persist_log_entry_sql(ctx, entry_idx, entry_term, entry_op, entry_payload, false);
            SpinLockAcquire(&ctx->log->mutex);
        }
        else if (entry_idx <= ctx->log->last_log_index)
        {
            RaftLogEntry *e = log_slot(ctx, entry_idx);
            if (e->index != entry_idx)
            {
                SpinLockRelease(&ctx->log->mutex);
                return true;
            }
            SpinLockRelease(&ctx->log->mutex);
            persist_log_entry_sql(ctx, entry_idx, entry_term, entry_op, entry_payload, false);
            SpinLockAcquire(&ctx->log->mutex);
        }
        else
        {
            SpinLockRelease(&ctx->log->mutex);
            return true;
        }
    }

    advance_commit_index_locked(ctx, leader_commit);
    commit_to_mark = ctx->log->commit_index;
    SpinLockRelease(&ctx->log->mutex);

    /*
     * 日志冲突截断后，必须**先**把 parwal 里对应的字节也截掉，再写新字节。
     * 否则新 leader 复用同一个 partition_lsn 写入不同记录时，去重逻辑会把
     * 旧字节当成"已落盘"而跳过，物理回放就会重放被截断的内容。
     */
    if (conflict_plsn > 0)
        data_group_truncate_parwal(ctx, conflict_plsn - 1);

    /*
     * 数据组：**先把字节落盘 fsync，再 ack**。落盘失败就不 ack，leader 便无法
     * 把这条计入多数派，从而保证"已提交 == 多数派已持久化"。
     */
    if (has_entry && strcmp(entry_op, RAFT_OP_PARWAL) == 0)
    {
        if (!data_entry_store(ctx, entry_payload, entry_data_hex))
            return true;
    }

    persist_hard_state_unlocked(ctx);

    if (commit_to_mark > 0)
        mark_log_committed_sql(ctx, commit_to_mark);

    *success = 1;
    group_apply_pending(ctx);
    return true;
}

Datum
pg_raft_append_entries(PG_FUNCTION_ARGS)
{
    int64  in_term = PG_GETARG_INT64(0);
    int    leader_id = PG_GETARG_INT32(1);
    int64  prev_idx = PG_GETARG_INT64(2);
    int64  prev_term = PG_GETARG_INT64(3);
    int64  leader_commit = PG_GETARG_INT64(4);
    int64  entry_idx = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT64(5);
    int64  entry_term = PG_ARGISNULL(6) ? 0 : PG_GETARG_INT64(6);
    char  *entry_op = PG_ARGISNULL(7) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(7));
    char  *entry_payload = PG_ARGISNULL(8) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(8));
    int64  group_id = (PG_NARGS() > 9 && !PG_ARGISNULL(9))
                        ? PG_GETARG_INT64(9) : RAFT_CONTROL_GROUP;
    char  *entry_data_hex = NULL;
    int64  out_term = 0;
    int    success = 0;
    char   out[64];
    RaftGroupCtx ctx;

    if (RaftGroups == NULL || !pg_raft_raft_enabled)
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));

    parse_peers();

    /* follower 可能是第一次听说这个数据组：按 leader 的通告自动建组 */
    if (!raft_group_ensure(group_id, NULL, 0, &ctx))
    {
        if (entry_op)
            pfree(entry_op);
        if (entry_payload)
            pfree(entry_payload);
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));
    }

    /*
     * 机会性补齐成员集（DTX_2PC_DESIGN.md §9.2）；补不上也照常收条目。
     * 理由同 pg_raft_rpc：leader 只向自己成员集里的节点发 AppendEntries，
     * 多数派算术是 leader 按它自己的成员集做的，本节点落盘+ack 不会让谁算错。
     * 成员集未知只剥夺**主动**参与（竞选/当选/提案），不剥夺被动接收 ——
     * 否则 hearsay 引导路径被砍，全新分片永远建不起来。
     */
    (void) group_resolve_membership(&ctx);

    if (PG_NARGS() > 10 && !PG_ARGISNULL(10))
    {
        bytea *raw = PG_GETARG_BYTEA_PP(10);
        int    len = VARSIZE_ANY_EXHDR(raw);

        entry_data_hex = (char *) palloc(len * 2 + 1);
        (void) hex_encode(VARDATA_ANY(raw), len, entry_data_hex);
        entry_data_hex[len * 2] = '\0';
    }

    (void) handle_append_entries(&ctx, in_term, leader_id, prev_idx, prev_term,
                                 leader_commit, entry_idx, entry_term,
                                 entry_op, entry_payload, entry_data_hex,
                                 &out_term, &success);

    if (entry_op)
        pfree(entry_op);
    if (entry_payload)
        pfree(entry_payload);
    if (entry_data_hex)
        pfree(entry_data_hex);

    snprintf(out, sizeof(out), "%lld %d", (long long) out_term, success);
    PG_RETURN_TEXT_P(cstring_to_text(out));
}

Datum
pg_raft_apply_committed(PG_FUNCTION_ARGS)
{
    (void) fcinfo;
    pg_raft_consensus_apply_pending();
    PG_RETURN_BOOL(true);
}

Datum
pg_raft_rpc(PG_FUNCTION_ARGS)
{
    char  *msg;
    char   type[8];
    int64  in_term = 0;
    int    in_node = 0;
    int64  in_last_idx = 0;
    int64  in_last_term = 0;
    long long in_group = 0;
    int    nparsed;
    int64  my_term;
    int    flag = 0;
    bool   need_persist = false;
    char   out[64];
    RaftGroupCtx ctx;

    if (RaftGroups == NULL || !pg_raft_raft_enabled)
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));

    msg = text_to_cstring(PG_GETARG_TEXT_PP(0));

    /* 第 6 个 token 是 group_id，缺省 0（兼容组化前的报文） */
    nparsed = sscanf(msg, "%7s %lld %d %lld %lld %lld",
                     type,
                     (long long *) &in_term,
                     &in_node,
                     (long long *) &in_last_idx,
                     (long long *) &in_last_term,
                     &in_group);
    if (nparsed < 3)
    {
        pfree(msg);
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));
    }
    if (nparsed < 6)
        in_group = 0;

    parse_peers();

    if (!raft_group_ensure((int64) in_group, NULL, 0, &ctx))
    {
        pfree(msg);
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));
    }

    /*
     * 机会性地从控制面 partition_map 补齐成员集（DTX_2PC_DESIGN.md §9.2）：
     * 补上了本节点就能主动参与该组（竞选/心跳）；补不上也**照常应答**。
     *
     * 为什么这里不能拒绝应答：候选人只会向**它自己成员集里的节点**发
     * RequestVote（peer_in_group 过滤），所以收到 RV 就意味着对方认为本节点
     * 是成员；票数算术是候选人按它自己的（已知的）成员集做的，本节点投票
     * 不会让任何人算错多数派。反过来，拒绝应答会砍掉一条**有意设计**的引导
     * 路径：数据组按计划文档 §11.5.2 的约定"只在 placement 节点先建组，
     * 其余成员靠 hearsay 自动建组后再补 create 固化成员集"，首次选举时
     * partition_map 尚无登记（登记正是由当选 leader 上报产生的），
     * 一律拒绝会让全新分片永远选不出 leader（实测 raft_14/15/16/17 全挂）。
     *
     * 真正的危险是**成员集未知的节点主动竞选/当选**（它会把多数派算成全体
     * 节点并向全集群广播）——那条路由 group_tick 里的 group_has_member 门禁
     * 挡住，见 group_membership_known() 的注释。
     */
    (void) group_resolve_membership(&ctx);

    restore_hard_state_if_needed(&ctx);
    SpinLockAcquire(&ctx.cons->mutex);
    SpinLockAcquire(&ctx.log->mutex);

    if (in_term > ctx.cons->current_term)
    {
        ctx.cons->current_term = in_term;
        ctx.cons->state = RAFT_FOLLOWER;
        ctx.cons->voted_for = 0;
        ctx.cons->leader_id = 0;
        need_persist = true;
    }

    my_term = ctx.cons->current_term;

    if (strcmp(type, "RV") == 0)
    {
        if (in_term >= my_term &&
            (ctx.cons->voted_for == 0 ||
             ctx.cons->voted_for == in_node) &&
            candidate_log_is_up_to_date_locked(&ctx, in_last_idx, in_last_term))
        {
            ctx.cons->voted_for = in_node;
            ctx.cons->state = RAFT_FOLLOWER;
            reset_election_deadline_locked(&ctx);
            need_persist = true;
            flag = 1;
        }
    }
    else if (strcmp(type, "AE") == 0)
    {
        if (in_term >= my_term)
        {
            ctx.cons->state = RAFT_FOLLOWER;
            ctx.cons->leader_id = in_node;
            reset_election_deadline_locked(&ctx);
            need_persist = true;
            flag = 1;
        }
    }

    my_term = ctx.cons->current_term;
    SpinLockRelease(&ctx.log->mutex);
    SpinLockRelease(&ctx.cons->mutex);

    if (need_persist)
        persist_hard_state_unlocked(&ctx);

    pfree(msg);
    snprintf(out, sizeof(out), "%ld %d", (long) my_term, flag);
    PG_RETURN_TEXT_P(cstring_to_text(out));
}

/* ---- 组管理 SQL 接口 ---- */

/* 从 int[] 参数抽取成员节点 id */
static int
extract_members(ArrayType *arr, int *out)
{
    Datum *elems;
    bool  *nulls;
    int    nelems;
    int    n = 0;
    int    i;

    if (arr == NULL)
        return 0;

    deconstruct_array(arr, INT4OID, 4, true, TYPALIGN_INT,
                      &elems, &nulls, &nelems);
    for (i = 0; i < nelems && n < RAFT_MAX_PEERS; i++)
    {
        if (!nulls[i])
            out[n++] = DatumGetInt32(elems[i]);
    }
    return n;
}

Datum
pg_raft_group_create(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    ArrayType   *arr = PG_ARGISNULL(1) ? NULL : PG_GETARG_ARRAYTYPE_P(1);
    int          members[RAFT_MAX_PEERS];
    int          n_members;
    RaftGroupCtx ctx;

    if (RaftGroups == NULL)
        PG_RETURN_BOOL(false);
    if (group_id <= 0)
        ereport(ERROR,
                (errmsg("pg_raft: group_id 必须 > 0（0 为控制面组，自动存在）")));

    parse_peers();
    n_members = extract_members(arr, members);

    /*
     * ★ 不接受"成员集未知"的数据组（DTX_2PC_DESIGN.md §9.2）。
     * p_members 的 SQL 默认值是 NULL，此前会直接建出一个 n_members == 0 的组，
     * 而旧语义把它当"全体节点"——该组于是向全集群广播选举、把多数派算成 5/9，
     * 实测导致真正的数据持有者掉成 follower、分片不可用。
     * 现在：先从控制面 partition_map 导出；导不出来就直接报错，不建这种组。
     */
    if (n_members == 0)
        n_members = group_members_from_partition_map(group_id, members);

    if (n_members == 0)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("pg_raft: 数据组 %lld 的成员集未知，拒绝建组",
                        (long long) group_id),
                 errdetail("数据组的多数派必须按真实副本集计算；分片副本集是全体节点的"
                           "真子集，空成员集不能当作\"全体节点\"。"),
                 errhint("显式给出成员集：partdist.pg_raft_group_create(%lld, ARRAY[...]::int[])；"
                         "或先让控制面把该分区登记进 partdist.partition_map。",
                         (long long) group_id)));

    /* master 只做协调与登记，不作任何分区的数据副本（主/从都不行） */
    if (pg_raft_coordinator_node_id > 0)
    {
        int i;

        for (i = 0; i < n_members; i++)
            if (members[i] == pg_raft_coordinator_node_id)
                ereport(ERROR,
                        (errmsg("pg_raft: 协调节点 %d 不作数据副本，不能加入数据组 %lld 的成员集",
                                pg_raft_coordinator_node_id, (long long) group_id)));
    }

    if (!raft_group_ensure(group_id, members, n_members, &ctx))
        PG_RETURN_BOOL(false);

    PG_RETURN_BOOL(true);
}

Datum
pg_raft_group_drop(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    RaftGroupCtx ctx;
    char         path[MAXPGPATH];

    if (RaftGroups == NULL)
        PG_RETURN_BOOL(false);
    if (group_id == RAFT_CONTROL_GROUP)
        ereport(ERROR, (errmsg("pg_raft: 控制面组 0 不可删除")));

    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_BOOL(false);

    /*
     * 连同 HardState 文件一起删除：否则 group_id 被回收重建时会继承旧 term，
     * 新组一上来就带着历史任期。
     */
    hard_state_path(&ctx, path, sizeof(path));
    if (unlink(path) != 0 && errno != ENOENT)
        elog(WARNING, "pg_raft: 删除 hardstate 文件 \"%s\" 失败: %m", path);

    SpinLockAcquire(&RaftGroups->mutex);
    ctx.g->in_use = false;
    RaftGroups->n_groups--;
    SpinLockRelease(&RaftGroups->mutex);

    PG_RETURN_BOOL(true);
}

Datum
pg_raft_group_propose(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    char        *op_type = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char        *payload = text_to_cstring(PG_GETARG_TEXT_PP(2));
    RaftGroupCtx ctx;
    int64        idx;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_INT64(0);

    parse_peers();
    restore_groups_if_needed();

    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_INT64(0);

    idx = group_propose(&ctx, op_type, payload);

    pfree(op_type);
    pfree(payload);
    PG_RETURN_INT64(idx);
}

Datum
pg_raft_group_status(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc      tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext   per_query_ctx;
    MemoryContext   oldcontext;
    int             i;
    static const char *state_names[] = {"follower", "candidate", "leader"};

    if (rsinfo == NULL || !(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errmsg("pg_raft_group_status: 需要 materialize 模式")));
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errmsg("pg_raft_group_status: 返回类型必须是 record")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    if (RaftGroups == NULL)
        PG_RETURN_VOID();

    parse_peers();
    restore_groups_if_needed();

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];
        RaftGroupCtx    ctx;
        Datum           values[8];
        bool            nulls[8];
        int             state;
        int64           term;
        int             leader_id;
        int64           last_idx;
        int64           commit_idx;
        int64           applied;

        if (!g->in_use)
            continue;

        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;

        SpinLockAcquire(&ctx.cons->mutex);
        state = ctx.cons->state;
        term = ctx.cons->current_term;
        leader_id = ctx.cons->leader_id;
        SpinLockRelease(&ctx.cons->mutex);

        SpinLockAcquire(&ctx.log->mutex);
        last_idx = ctx.log->last_log_index;
        commit_idx = ctx.log->commit_index;
        applied = ctx.log->last_applied;
        SpinLockRelease(&ctx.log->mutex);

        memset(nulls, 0, sizeof(nulls));
        values[0] = Int64GetDatum(g->group_id);
        values[1] = CStringGetTextDatum(
            (state >= 0 && state <= 2) ? state_names[state] : "unknown");
        values[2] = Int64GetDatum(term);
        values[3] = Int32GetDatum((state == RAFT_LEADER) ? pg_raft_node_id : leader_id);
        values[4] = Int64GetDatum(last_idx);
        values[5] = Int64GetDatum(commit_idx);
        values[6] = Int64GetDatum(applied);
        values[7] = Int32GetDatum(group_cluster_size(&ctx));

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    PG_RETURN_VOID();
}

/*
 * 数据条目提交核心：把本节点 pg_parwal 里 partition_lsn 这条记录作为 Raft entry
 * 提交到 ctx 组。返回 Raft log index，0 表示失败（非 leader / 记录不存在 /
 * 未达多数派）。成功时推进本组的 last_data_plsn（prepare 接线的增量下界）。
 * 重复 propose 同一 plsn 无害：follower 落盘幂等，apply 单调。
 */
static int64
data_propose_one(RaftGroupCtx *ctx, int64 partition_lsn)
{
    StringInfoData payload;
    StringInfoData sql;
    bool         spi_owned;
    bool         isnull;
    char        *orig_lsn = NULL;
    char         orig_lsn_buf[32];
    int          rmid = 0;
    int          info = 0;
    int64        xid = 0;
    int64        nbytes = 0;
    int64        local_oid;
    int64        idx;

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0)
        ereport(ERROR,
                (errmsg("pg_raft: group %lld 在本节点没有对应分片（先跑 partdist.rebuild_shard_identity()）",
                        (long long) ctx->group_id)));

    /* 读出记录头部字段，组装描述符 */
    if (!raft_persist_spi_begin(&spi_owned))
        return 0;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT orig_lsn::text, rmid, info, xid, length(data) "
                     "FROM partdist.partwal_read_record(%u::oid, %lld)",
                     (unsigned) local_oid, (long long) partition_lsn);
    if (SPI_execute(sql.data, true, 1) != SPI_OK_SELECT || SPI_processed == 0)
    {
        pfree(sql.data);
        raft_persist_spi_end(spi_owned);
        elog(WARNING,
             "pg_raft: 组 %lld 读不到本地 parwal 记录 plsn=%lld(local_oid=%lld)——"
             "该 partition_lsn 在本节点不存在",
             (long long) ctx->group_id, (long long) partition_lsn,
             (long long) local_oid);
        return 0;
    }
    /*
     * ★ 逐列判 NULL（2026-08-03 修，gdb 实锤的 SIGSEGV）。
     *
     * partwal_read_record 查不到记录时 PG_RETURN_NULL() —— 在
     * `SELECT ... FROM f(...)` 形态下这不是"零行"，而是**一行全 NULL**
     * （SPI_processed == 1），上面的 SPI_processed == 0 挡不住它。
     * 此前对第 1 列直接 TextDatumGetCString(0) → text_to_cstring(NULL)
     * → pg_detoast_datum_packed 解引用空指针，si_addr=0x0。
     *
     * 何时会读到不存在的 plsn：历史上是 leader 失败回滚的
     * partwal_truncate_to 把并发事务的字节一起截掉（该截断已随本次修复
     * 移除，见 discard_uncommitted_entry）；今后仍可能出现的场景是
     * 归队 follower 被 AppendEntries 冲突截断后本地游标短暂超前，以及
     * 段文件损坏/被运维清理。无论成因，正确行为都是按"记录不存在"
     * 返回 0 —— 调用方 ERROR、事务中止；绝不是崩掉整个节点。
     */
    {
        Datum d;
        bool  null1;

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &null1);
        if (!null1)
        {
            /*
             * ★ 拷出 SPI 上下文（顺手修的同族隐患）：TextDatumGetCString 的
             * 结果分配在 SPI proc context 里，raft_persist_spi_end（内部
             * SPI_finish）会释放它 —— 旧代码在 end 之后仍拿它拼 payload，
             * 是潜伏的 use-after-free，只是该内存至今未被覆写过。
             */
            strlcpy(orig_lsn_buf, TextDatumGetCString(d), sizeof(orig_lsn_buf));
            orig_lsn = orig_lsn_buf;
        }
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
        rmid = isnull ? 0 : DatumGetInt32(d);
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull);
        info = isnull ? 0 : DatumGetInt32(d);
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &isnull);
        xid = isnull ? 0 : DatumGetInt64(d);
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5, &isnull);
        nbytes = isnull ? 0 : DatumGetInt32(d);

        if (null1)
        {
            pfree(sql.data);
            raft_persist_spi_end(spi_owned);
            elog(WARNING,
                 "pg_raft: 组 %lld 本地 parwal 无记录 plsn=%lld(local_oid=%lld，"
                 "可能已被失多数派回滚截断)，按 propose 失败处理",
                 (long long) ctx->group_id, (long long) partition_lsn,
                 (long long) local_oid);
            return 0;
        }
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"partition_lsn\":%lld,\"orig_lsn\":\"%s\",\"rmid\":%d,"
                     "\"info\":%d,\"xid\":%lld,\"nbytes\":%lld}",
                     (long long) partition_lsn, orig_lsn, rmid, info,
                     (long long) xid, (long long) nbytes);

    idx = group_propose(ctx, RAFT_OP_PARWAL, payload.data);

    pfree(payload.data);

    if (idx <= 0)
    {
        int state;
        int64 last_idx, commit_idx, applied;

        SpinLockAcquire(&ctx->cons->mutex);
        state = ctx->cons->state;
        SpinLockRelease(&ctx->cons->mutex);
        SpinLockAcquire(&ctx->log->mutex);
        last_idx = ctx->log->last_log_index;
        commit_idx = ctx->log->commit_index;
        applied = ctx->log->last_applied;
        SpinLockRelease(&ctx->log->mutex);

        elog(WARNING,
             "pg_raft: 组 %lld propose plsn=%lld 失败(state=%d last_log_index=%lld "
             "commit_index=%lld last_applied=%lld 环容量=%d 成员数=%d)",
             (long long) ctx->group_id, (long long) partition_lsn, state,
             (long long) last_idx, (long long) commit_idx, (long long) applied,
             RAFT_LOG_CAPACITY, group_cluster_size(ctx));
    }
    else if (partition_lsn > ctx->g->last_data_plsn)
        ctx->g->last_data_plsn = partition_lsn;
    return idx;
}

/*
 * pg_raft_data_propose(group_id, partition_lsn)
 *
 * 在数据组的 leader 上调用：把本节点 pg_parwal 里 partition_lsn 这条记录作为
 * Raft entry 提交到该组。返回分配到的 Raft log index，0 表示失败（非 leader /
 * 记录不存在 / 未达多数派）。
 *
 * 提交成功即意味着：该条记录的**原始字节已在多数派节点 fsync 落盘**，且各节点
 * 的 applied_part_lsn 已推进到该 partition_lsn（平凡 apply，不含 redo）。
 */
Datum
pg_raft_data_propose(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    int64        partition_lsn = PG_GETARG_INT64(1);
    RaftGroupCtx ctx;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_INT64(0);
    if (group_id <= 0)
        ereport(ERROR, (errmsg("pg_raft: 数据条目只能提交到 group_id > 0 的数据组")));

    parse_peers();
    restore_groups_if_needed();

    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_INT64(0);

    PG_RETURN_INT64(data_propose_one(&ctx, partition_lsn));
}

/*
 * 本组 prepare 复制的串行化（DTX_2PC_DESIGN.md §9.1）。
 *
 * 为什么需要：让路窗口修复之后，并发 backend 不再各自提前返回，而是**都会**
 * 对同一个分区调用复制挂钩。若不串行，两个 backend 会同时读到同一个
 * last_data_plsn 并各自 propose 同一段 plsn —— 字节层面无害（follower 落盘幂等、
 * apply 单调），但每条重复提案都白占一个 RAFT_LOG_CAPACITY(128) 的环槽位，
 * 高并发下会把环烧满并触发背压。串行之后，后到者进入临界区时 last_data_plsn
 * 已被推进，循环直接空转返回。
 *
 * 回收：正常路径由 PG_FINALLY 释放（覆盖 ERROR）；持有者 FATAL/被杀时其
 * PGPROC 消失，等待者按 BackendPidGetProc() 回收（liveness 检查必须在**出
 * 自旋锁之后**做 —— 它内部要拿 ProcArrayLock，自旋锁下不允许再取 LWLock）。
 * 硬崩溃走 postmaster 全局重启，shmem 重建，无残留。
 *
 * 复用 RaftGroups->mutex：它原本只护注册表（in_use/group_id/members），
 * 这里扩到"每组的复制认领位"。二者都是短临界区、无嵌套取锁，安全。
 */
#define RAFT_REPLICATE_CLAIM_TIMEOUT_MS  60000

static void
replicate_claim(RaftGroupCtx *ctx)
{
    long waited_us = 0;

    for (;;)
    {
        bool got = false;
        int  holder = 0;

        SpinLockAcquire(&RaftGroups->mutex);
        if (!ctx->g->replicate_in_progress)
        {
            ctx->g->replicate_in_progress = true;
            ctx->g->replicate_pid = MyProcPid;
            got = true;
        }
        else
            holder = ctx->g->replicate_pid;
        SpinLockRelease(&RaftGroups->mutex);

        if (got)
            return;

        /* 持有者还活着吗？（出锁后做：BackendPidGetProc 会取 ProcArrayLock） */
        if (holder != 0 && holder != MyProcPid &&
            BackendPidGetProc(holder) == NULL)
        {
            SpinLockAcquire(&RaftGroups->mutex);
            if (ctx->g->replicate_in_progress &&
                ctx->g->replicate_pid == holder)
            {
                ctx->g->replicate_pid = MyProcPid;
                got = true;
            }
            SpinLockRelease(&RaftGroups->mutex);

            if (got)
            {
                elog(WARNING,
                     "pg_raft: 组 %lld 的复制认领位由已消失的 backend %d 持有，已回收",
                     (long long) ctx->group_id, holder);
                return;
            }
        }

        if (waited_us >= RAFT_REPLICATE_CLAIM_TIMEOUT_MS * 1000L)
            ereport(ERROR,
                    (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                     errmsg("pg_raft: 等待组 %lld 的复制认领位超过 %d ms，prepare 失败",
                            (long long) ctx->group_id,
                            RAFT_REPLICATE_CLAIM_TIMEOUT_MS),
                     errdetail("持有者 backend %d 可能卡在对端 RPC 上。", holder)));

        CHECK_FOR_INTERRUPTS();
        pg_usleep(1000L);       /* 1ms */
        waited_us += 1000L;
    }
}

static void
replicate_release(RaftGroupCtx *ctx)
{
    SpinLockAcquire(&RaftGroups->mutex);
    if (ctx->g->replicate_in_progress && ctx->g->replicate_pid == MyProcPid)
    {
        ctx->g->replicate_in_progress = false;
        ctx->g->replicate_pid = 0;
    }
    SpinLockRelease(&RaftGroups->mutex);
}

/*
 * prepare 接线（计划文档 §4 阶段 3 四步设计的第 2 步）。
 *
 * 由 pg_partdist 的 PartWALFlush 在事务 PRE_COMMIT / PRE_PREPARE 时经
 * rendezvous variable 调用（[A] 本地 parwal fsync 之后、[B] pg_wal 提交 fsync
 * 之前），对本事务涉及的每个分区：若其存在数据组，把新落盘的记录逐条 propose
 * 给该组（一条 record 一次备份）。
 *
 * 语义：
 *   - 分区无数据组：直接返回，行为与接线前完全一致（合成分区/未纳管分片）。
 *   - 组存在但本节点不是 leader：ERROR —— 写栅栏。切主后旧 primary 上仍在途的
 *     事务在 prepare 即被拒绝，不会产生分叉写入。
 *   - 复制凑不齐多数派：ERROR —— 事务中止（步骤 4：多数派持久化才算 prepared）；
 *     group_propose 的失败路径会连带回滚 leader 侧该条目的 ring/SQL/parwal 字节。
 *   - 增量下界取本组 last_data_plsn（重启后从环内最后一条 OP_PARWAL 回推），
 *     配合幂等落盘，重复/回退都无害；顺带把组建立前的历史记录自动补齐复制。
 */
void
pg_raft_partwal_replicate(Oid partition_id)
{
    RaftGroupCtx ctx;
    StringInfoData sql;
    bool         spi_owned;
    bool         isnull;
    int64        gid = 0;
    int64        cur = 0;
    int64        last;
    int64        plsn;
    int          state;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        return;

    parse_peers();
    if (n_peers == 0)
        return;
    restore_groups_if_needed();

    if (!raft_persist_spi_begin(&spi_owned))
        return;
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partdist.global_id_for_partition(%u::oid)::bigint, "
                     "       partdist.get_partition_flush_lsn(%u::oid)",
                     (unsigned) partition_id, (unsigned) partition_id);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d;

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            gid = DatumGetInt64(d);
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
        if (!isnull)
            cur = DatumGetInt64(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (gid <= 0)
        return;                 /* 非全局分片（无 shard_identity）：不纳管 */
    if (!raft_group_ctx(gid, &ctx))
        return;                 /* 无数据组：行为与接线前一致 */

    /*
     * ★ 成员集未知的数据组拒绝参与 prepare（DTX_2PC_DESIGN.md §9.2）。
     * 先尝试从控制面 partition_map 导出；仍未知则 **ERROR 中止事务**，
     * 而不是"跳过复制照常提交" —— 后者等于让写入在没有任何多数派保证的
     * 情况下返回成功，正是本项目刚修掉的那类丢数据形态。
     */
    if (!group_resolve_membership(&ctx))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 分区 %u(组 %lld)的成员集未知，写入被拒",
                        partition_id, (long long) gid),
                 errdetail("数据组的多数派必须按真实副本集计算；partition_map 中没有该分区的登记，"
                           "无法确定副本集。"),
                 errhint("先让控制面登记该分区，或在各副本节点执行 "
                         "partdist.pg_raft_group_create(%lld, ARRAY[...]::int[])。",
                         (long long) gid)));

    restore_hard_state_if_needed(&ctx);
    restore_persistent_log_if_needed(&ctx);

    SpinLockAcquire(&ctx.cons->mutex);
    state = ctx.cons->state;
    SpinLockRelease(&ctx.cons->mutex);
    if (state != RAFT_LEADER)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 分区 %u(组 %lld)的本地写入被拒：本节点不是该分区组的 leader",
                        partition_id, (long long) gid),
                 errdetail("分区主副本可能已切换，请经路由层重试。")));

    if (cur <= 0)
        return;

    /*
     * cur 是取自本地 parwal 的当前 flush 点，它必然 >= 本事务刚落盘的那些记录的
     * partition_lsn —— group commit 让路时本事务的记录是被并发 backend 写下去的，
     * 但"写下去"发生在 flushed_upto 推进之前，所以到这里 cur 已经覆盖它们。
     * 因此"复制到 cur"是覆盖本事务的安全上界（DTX_2PC_DESIGN.md §9.1）。
     */
    replicate_claim(&ctx);
    PG_TRY();
    {
        /*
         * 增量下界必须在**进入临界区之后**重新读取：等待期间并发 backend
         * 很可能已经把这一段（含本事务的记录）复制完了，此时循环空转即返回。
         */
        last = ctx.g->last_data_plsn;
        if (last == 0)
        {
            int64 p;

            /* 重启后运行期游标为 0：从环内最后一条 OP_PARWAL 回推 */
            SpinLockAcquire(&ctx.log->mutex);
            for (p = ctx.log->last_log_index; p > 0 &&
                 p > ctx.log->last_log_index - RAFT_LOG_CAPACITY; p--)
            {
                RaftLogEntry e;

                if (log_get_entry_locked(&ctx, p, &e) &&
                    strcmp(e.op_type, RAFT_OP_PARWAL) == 0)
                {
                    last = entry_partition_lsn(e.payload);
                    break;
                }
            }
            SpinLockRelease(&ctx.log->mutex);
            if (last > 0)
                ctx.g->last_data_plsn = last;
        }

        for (plsn = last + 1; plsn <= cur; plsn++)
        {
            /*
             * group_propose 只在拿到多数派 ack 之后才返回 idx > 0，而 follower
             * 是**先 fsync 再 ack** 的（运输层加固 §11.5.1 #1/#3）——所以
             * "返回成功" 严格等价于 "该条目已在多数派持久化"，即用户方案
             * 阶段 1 第 4 步的 prepared 语义。不需要再单独等 commit_index。
             */
            int64 idx = data_propose_one(&ctx, plsn);

            if (idx <= 0)
                ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                         errmsg("pg_raft: 分区 %u(组 %lld) record %lld 复制未达多数派，prepare 失败，事务中止",
                                partition_id, (long long) gid, (long long) plsn)));
        }
    }
    PG_FINALLY();
    {
        replicate_release(&ctx);
    }
    PG_END_TRY();
}

/*
 * pg_raft_group_reset() — 丢弃本节点全部数据组（group 0 保留），连同它们的
 * HardState 文件。
 *
 * 存在的理由：组的 drop 是**节点本地**的，而对端只要还留着该组就会经 RV/AE 把它
 * 传播回来，且带着上一轮的 term。于是"丢弃后用同一个 group_id 重建"会得到一个
 * 起始 term 落后于对端记忆的新组，选举会被反复压制 —— 回归里表现为偶发的
 * "组选不出 leader / 条目复制不出去"。测试与运维重置场景需要一个能把本节点
 * 数据组彻底清干净的入口；真正的成员/组生命周期管理仍待控制面决议下发(§11.6 #3)。
 */
Datum
pg_raft_group_reset(PG_FUNCTION_ARGS)
{
    int i;
    int dropped = 0;

    (void) fcinfo;

    if (RaftGroups == NULL)
        PG_RETURN_INT32(0);

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];
        RaftGroupCtx    ctx;
        char            path[MAXPGPATH];

        if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP)
            continue;

        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;

        hard_state_path(&ctx, path, sizeof(path));
        if (unlink(path) != 0 && errno != ENOENT)
            elog(WARNING, "pg_raft: 删除 hardstate 文件 \"%s\" 失败: %m", path);

        SpinLockAcquire(&RaftGroups->mutex);
        g->in_use = false;
        RaftGroups->n_groups--;
        SpinLockRelease(&RaftGroups->mutex);
        dropped++;
    }

    PG_RETURN_INT32(dropped);
}
