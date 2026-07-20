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

#define RAFT_MAX_PEERS     16
#define RAFT_LOG_CAPACITY  128
#define RAFT_OP_LEN        32
#define RAFT_PAYLOAD_MAX   768
#define RAFT_HARDSTATE_MAGIC   UINT32_C(0x52484654)
#define RAFT_HARDSTATE_VERSION 1

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

/* 该节点是否为本组成员（members 为空表示全体节点） */
static bool
group_has_member(RaftGroupCtx *ctx, int node_id)
{
    int i;

    if (ctx->g->n_members <= 0)
        return true;
    for (i = 0; i < ctx->g->n_members; i++)
        if (ctx->g->members[i] == node_id)
            return true;
    return false;
}

/* 本组的集群规模（用于多数派计算） */
static int
group_cluster_size(RaftGroupCtx *ctx)
{
    if (ctx->g->n_members > 0)
        return ctx->g->n_members;
    return (n_peers > 0) ? n_peers : 1;
}

static int
cluster_majority(RaftGroupCtx *ctx)
{
    return group_cluster_size(ctx) / 2 + 1;
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
    appendStringInfo(&sql,
                     "SELECT partdist.partwal_follower_append("
                     "%u::oid, %s::pg_lsn, %d, %d, %lld::bigint, decode(%s, 'hex'))",
                     (unsigned) local_oid,
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
static void
data_entry_apply(RaftGroupCtx *ctx, const RaftLogEntry *e)
{
    StringInfoData sql;
    bool           spi_owned;
    int64          plsn = entry_partition_lsn(e->payload);
    int64          local_oid;

    if (plsn <= 0)
        return;

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0)
        return;

    if (!raft_persist_spi_begin(&spi_owned))
        return;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partdist.follower_set_applied_part_lsn(%u::oid, %lld)",
                     (unsigned) local_oid, (long long) plsn);
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
                          int64 commit_index)
{
    char              path[MAXPGPATH];
    char              tmppath[MAXPGPATH];
    RaftHardStateFile hs;
    int               fd;
    ssize_t           written;

    hard_state_path(ctx, path, sizeof(path));
    snprintf(tmppath, sizeof(tmppath), "%s.tmp", path);

    hs.magic = RAFT_HARDSTATE_MAGIC;
    hs.version = RAFT_HARDSTATE_VERSION;
    hs.current_term = current_term;
    hs.voted_for = voted_for;
    hs.commit_index = commit_index;

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

    SpinLockAcquire(&ctx->cons->mutex);
    current_term = ctx->cons->current_term;
    voted_for = ctx->cons->voted_for;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    commit_index = ctx->log->commit_index;
    SpinLockRelease(&ctx->log->mutex);

    (void) persist_hard_state_values(ctx, current_term, voted_for, commit_index);
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

    nread = read(fd, &hs, sizeof(hs));
    close(fd);

    if (nread != (ssize_t) sizeof(hs) ||
        hs.magic != RAFT_HARDSTATE_MAGIC ||
        hs.version != RAFT_HARDSTATE_VERSION)
    {
        elog(WARNING, "pg_raft: 忽略损坏的 hardstate 文件 \"%s\"", path);
        return;
    }

    SpinLockAcquire(&ctx->cons->mutex);
    if (hs.current_term > ctx->cons->current_term)
        ctx->cons->current_term = hs.current_term;
    ctx->cons->voted_for = hs.voted_for;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    if (hs.commit_index > ctx->log->commit_index)
        ctx->log->commit_index = hs.commit_index;
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
static void
apply_one_entry(RaftGroupCtx *ctx, const RaftLogEntry *e)
{
    if (ctx->group_id == RAFT_CONTROL_GROUP)
        (void) pg_raft_apply_payload_sql(e->op_type, e->payload);
    else if (strcmp(e->op_type, RAFT_OP_PARWAL) == 0)
        data_entry_apply(ctx, e);
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

        (void) raft_group_ensure(gid, members, n_members, &ctx);
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
     * Metadata tables are durable too. Avoid replaying old committed entries
     * on every restart; missed entries are still appended by the current leader
     * through AppendEntries and then applied normally.
     */
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

        SpinLockAcquire(&ctx->log->mutex);
        if (ctx->log->last_applied >= ctx->log->commit_index)
        {
            SpinLockRelease(&ctx->log->mutex);
            break;
        }
        idx = ctx->log->last_applied + 1;
        if (!log_get_entry_locked(ctx, idx, &e))
        {
            SpinLockRelease(&ctx->log->mutex);
            break;
        }
        ctx->log->last_applied = idx;
        SpinLockRelease(&ctx->log->mutex);

        apply_one_entry(ctx, &e);
    }
}

void
pg_raft_consensus_apply_pending(void)
{
    RaftGroupCtx ctx;

    if (!raft_control_ctx(&ctx))
        return;
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
    long jitter = (base > 0) ? (random() % base) : 0;

    ctx->cons->election_deadline =
        GetCurrentTimestamp() + (base + jitter) * 1000L;
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
    int i;

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
                log_truncate_after_locked(ctx, entry_idx - 1);
        }
        if (entry_idx == ctx->log->last_log_index + 1)
        {
            log_append_locked(ctx, entry_term, entry_op, entry_payload);
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
    StringInfoData payload;
    StringInfoData sql;
    bool         spi_owned;
    bool         isnull;
    char        *orig_lsn = NULL;
    int          rmid = 0;
    int          info = 0;
    int64        xid = 0;
    int64        nbytes = 0;
    int64        local_oid;
    int64        idx;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_INT64(0);
    if (group_id <= 0)
        ereport(ERROR, (errmsg("pg_raft: 数据条目只能提交到 group_id > 0 的数据组")));

    parse_peers();
    restore_groups_if_needed();

    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_INT64(0);

    local_oid = group_local_partition(&ctx);
    if (local_oid <= 0)
        ereport(ERROR,
                (errmsg("pg_raft: group %lld 在本节点没有对应分片（先跑 partdist.rebuild_shard_identity()）",
                        (long long) group_id)));

    /* 读出记录头部字段，组装描述符 */
    if (!raft_persist_spi_begin(&spi_owned))
        PG_RETURN_INT64(0);

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT orig_lsn::text, rmid, info, xid, length(data) "
                     "FROM partdist.partwal_read_record(%u::oid, %lld)",
                     (unsigned) local_oid, (long long) partition_lsn);
    if (SPI_execute(sql.data, true, 1) != SPI_OK_SELECT || SPI_processed == 0)
    {
        pfree(sql.data);
        raft_persist_spi_end(spi_owned);
        PG_RETURN_INT64(0);
    }
    orig_lsn = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
                                                 SPI_tuptable->tupdesc, 1, &isnull));
    rmid = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                       SPI_tuptable->tupdesc, 2, &isnull));
    info = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                       SPI_tuptable->tupdesc, 3, &isnull));
    xid = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                      SPI_tuptable->tupdesc, 4, &isnull));
    nbytes = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                         SPI_tuptable->tupdesc, 5, &isnull));
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"partition_lsn\":%lld,\"orig_lsn\":\"%s\",\"rmid\":%d,"
                     "\"info\":%d,\"xid\":%lld,\"nbytes\":%lld}",
                     (long long) partition_lsn, orig_lsn, rmid, info,
                     (long long) xid, (long long) nbytes);

    idx = group_propose(&ctx, RAFT_OP_PARWAL, payload.data);

    pfree(payload.data);
    PG_RETURN_INT64(idx);
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
