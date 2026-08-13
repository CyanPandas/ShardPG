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

#include "access/xact.h"             /* 内核补丁 0004：pre_record_commit_hook */
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "postmaster/postmaster.h"   /* PostPortNumber：dtx 恢复连回本节点 */
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "lib/stringinfo.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/guc.h"               /* application_name：MX 闸门（T4.4） */

#include <errno.h>
#include <signal.h>
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

/* data_entry_apply 返回 false 的原因（写进 RaftLogShmem.last_apply_fail） */
#define RAFT_APPLYFAIL_NONE       0
#define RAFT_APPLYFAIL_NO_PART    1   /* group_local_partition() <= 0：P0 映射没建好 */
#define RAFT_APPLYFAIL_NO_SPI     2   /* 拿不到 SPI */
#define RAFT_APPLYFAIL_SPI_EXEC   3   /* follower_set_applied_part_lsn 执行失败 */
#define RAFT_OP_LEN        32
#define RAFT_PAYLOAD_MAX   768

/*
 * parwal 记录分类位的**镜像**（权威定义在
 * pg-partdist-src/include/partition_wal_header.h 的 PARTWAL_FLAG_*）。
 *
 * pg_raft 有意不在编译期依赖 pg_partdist 的头文件（两个扩展只经 SQL 边界函数
 * 和 rendezvous variable 交互），所以这里只镜像一个值：数据组复制时 flags 是
 * **不透明透传**的——leader 从 partwal_read_record 读出多少就原样写回
 * partwal_follower_append，pg_raft 不解释它。唯一需要的常量是"描述符里没有
 * flags 时（旧 leader 发来的条目）按什么处理"，取 DATA(0x01)。
 * 若 partition_wal_header.h 改了 DATA 位的取值，这里必须同步。
 */
#define PARTWAL_FLAG_DATA  1
#define PARTWAL_FLAG_DTX   8

/*
 * ★ 用户事务内的复制路径不做 inline apply（2026-08-03 实测的 2PC 死锁）。
 *
 * 症状：分片上存在 prepared 事务时，`dtx_decide` **永久阻塞**在
 * `Lock / transactionid`。
 *
 * 根因：prepare 路径在**用户事务内**触发复制（PartWALFlush 的挂钩），而
 * group_propose 末尾的 group_apply_pending() 会 UPSERT
 * `partdist.follower_partition_map` 的进度行；该事务随后进入 PREPARED 状态，
 * **这把行锁就一直被持有**。而 2PC 恰恰要求事务在等决议期间保持 prepared——
 * 协调者的 dtx_decide 自己复制时再触发 apply，就撞上那把锁，永久等待。
 * 这不是夹具问题：只要"prepare 后保持 in-doubt、再做决议"这个 2PC 基本形态
 * 成立，就必然撞上。
 *
 * 处置：用户事务内的复制路径（prepare 挂钩、dtx 决议）跳过 inline apply。
 * 进度游标是**单调幂等**的，推迟到下一次 client backend 路径再推进无损；
 * 代价只是 applied_part_lsn 比 commit_index 落后一小段，而它本就允许滞后
 * （切主安全线比的是"谁追平了"）。commit_index 与多数派持久化不受影响——
 * **提交点语义完全不变**。
 */
static bool in_txn_replication = false;
#define RAFT_HARDSTATE_MAGIC   UINT32_C(0x52484654)
/*
 * v2 起 hardstate 增加 last_applied。v1 文件仍可读（last_applied 视为 0），
 * 避免升级时丢掉 current_term/voted_for 造成任期回退。
 * v3 起再增加 base_index/base_term —— 日志压缩的基点（快照 last_included_*）。
 * 低版本文件读上来时基点为 0，等价于"没压缩过"，语义与压缩前完全一致。
 */
#define RAFT_HARDSTATE_VERSION 3
#define RAFT_HARDSTATE_VERSION_MIN 1
/* v1 布局 = v2 去掉末尾的 last_applied，用 offsetof 取以免手算漏掉结构体填充 */
#define RAFT_HARDSTATE_V1_SIZE  offsetof(RaftHardStateFile, last_applied)
#define RAFT_HARDSTATE_V2_SIZE  offsetof(RaftHardStateFile, base_index)

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
    /*
     * 日志压缩基点：index <= base_index 的条目已被快照取代并从
     * partdist.raft_log 删除，环里也不再保证有。base_term 是 base_index
     * 那一条的 term —— prev 一致性检查与选举的"日志新旧"比较在压缩点上
     * 只能靠它（条目本身已经没有了）。0/0 表示从未压缩过。
     */
    int64        base_index;
    int64        base_term;
    int64        peer_next_index[RAFT_MAX_PEERS];
    int64        peer_match_index[RAFT_MAX_PEERS];
    bool         repl_inited;
    int          apply_owner_pid;   /* 串行化 apply 的认领（0 = 无人持有），
                                     * 保证 N 先于 N+1 生效。
                                     *
                                     * ★ 为什么是 PID 而不是 bool（#39）：持有者
                                     * 可能死在三条路上，归还机制各不相同 ——
                                     *   ERROR   → PG_CATCH 归还（longjmp 可接）；
                                     *   FATAL   → proc_exit 跑 before_shmem_exit
                                     *             回调但**不走 PG_CATCH**，且
                                     *             postmaster 对 FATAL 退出不重置
                                     *             shmem —— bool 版在这里永久泄漏，
                                     *             该组 apply 从此停摆（实测：对端
                                     *             节点重置引发的 Connection reset
                                     *             FATAL 正是 h 轮回归全线卡死的根因）；
                                     *   kill -9 → postmaster 整体重置节点、重建
                                     *             shmem，天然清零，无需处理。
                                     * 回调只清 **owner == MyProcPid** 的认领，
                                     * 所以必须存 PID。**没有探活、没有抢占** ——
                                     * 抢占一个还活着的持有者会破坏单写者不变式
                                     * （上一次尝试因此回退）。*/

    /*
     * 背压与丢弃计数（§13 约束 13）。**这几个数字是"副本是否还可信"的唯一
     * 线索**：数据面的提案一旦被丢弃，discard_uncommitted_entry 会把 leader
     * 自己段里那条也截掉，于是 leader 的 flush_lsn 退回、follower 显示"已追平"
     * ——而那条记录代表的物理变更（典型是 VACUUM 的尾部截断）**在 leader 上
     * 已经durable 且不随事务回滚**。分叉就此无痕。有了计数，至少能查出来。
     */
    int64        ring_full_waits;   /* 因环满而阻塞等 apply 的次数 */
    int64        ring_full_drops;   /* 等到超时仍无空位、提案被丢弃的次数 */
    int64        quorum_drops;      /* 多数派不足导致条目被丢弃的次数 */
    int64        last_drop_plsn;    /* 最近一次被丢弃的数据条目的 partition_lsn */
    int          last_apply_fail;   /* data_entry_apply 最近一次返回 false 的原因，
                                     * 见 RAFT_APPLYFAIL_*。apply 推不动时这是唯一
                                     * 线索 —— 三条 false 分支从日志上完全看不出
                                     * 区别，而它们的处置完全不同。 */

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
    bool                log_restored;   /* SQL 日志是否已灌回环（不能用 last_log_index>0 判） */
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
    int64  base_index;      /* v3：日志压缩基点 = 快照 last_included_index */
    int64  base_term;       /* v3：该条目的 term = 快照 last_included_term */
} RaftHardStateFile;

bool  pg_raft_raft_enabled = false;
bool  pg_raft_dtx_2pc_enabled = true;
int   pg_raft_dtx_recover_interval_ms = 10000;
int   pg_raft_dtx_recover_timeout_ms  = 30000;
/* 升主前置：每 tick 最多推进多久 / 多久之后按可用性优先放行（见 data_group_promote_prepare） */
int   pg_raft_promote_catchup_slice_ms    = 2000;
int   pg_raft_promote_catchup_deadline_ms = 60000;
char *pg_raft_peers = NULL;
int   pg_raft_election_timeout_ms = 1500;
int   pg_raft_heartbeat_ms = 400;
int   pg_raft_propose_wait_ms = 10000;
int   pg_raft_catchup_interval_ms = 5000;
int   pg_raft_compact_threshold = 500;

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
static void replicate_group_upto(RaftGroupCtx *ctx, int64 cur_plsn, Oid partition_id);
static void restore_groups_if_needed(void);
static char *data_entry_fetch_hex(RaftGroupCtx *ctx, int64 partition_lsn);
static bool data_entry_store(RaftGroupCtx *ctx, const char *payload,
                             const char *data_hex);
static int64 group_local_partition(RaftGroupCtx *ctx);

/*
 * 「当前是否处于 SPI 可用的语境」。两处依赖它：
 *
 *  1. 数据组的 entry 携带真实 parwal 字节，取字节要走 SPI（partwal_read_record）；
 *  2. 环外条目回读 —— 环只有 RAFT_LOG_CAPACITY 条，更老的条目只能从
 *     partdist.raft_log 读回来（log_get_entry_ext）。
 *
 * BGW tick 里没有 SPI，两件都做不了：对数据组只发心跳，对落后超过环容量的
 * follower 只能空转。追平由 client backend 语境驱动 —— propose 路径，或
 * 后台追平通道 pg_raft_catchup()（TopologyMonitor 经 libpq 自连触发）。
 */
static bool raft_spi_ctx = false;

static bool raft_persist_spi_begin(bool *spi_owned);
static void group_apply_pending(RaftGroupCtx *ctx);
static bool dtx_dtxid_from_gid(const char *gid, int64 *dtxid);
static void dtx_ack_sweep(void);
static void dtx_forget_sweep(void);
static int  dtx_gc_dist_transaction(void);
static void raft_persist_spi_end(bool spi_owned);
static int64 entry_partition_lsn(const char *payload);
/* 批量 apply 随批携带的 DTX 登记项（§6.2/§9.7 逐条语义的保留通道） */
typedef struct DtxApplyItem
{
    int64 plsn;
    int   info;         /* 2=DECISION 5=FORGET */
} DtxApplyItem;
static bool  data_apply_advance(RaftGroupCtx *ctx, int64 plsn,
                                const DtxApplyItem *dtx_items, int ndtx);
static void  data_apply_dtx_one(RaftGroupCtx *ctx, int64 local_oid,
                                int64 plsn, int info);
static int entry_record_flags(const char *payload);
static int entry_record_info(const char *payload);
static void data_group_try_report(RaftGroupCtx *ctx);
static bool replicate_try_claim(RaftGroupCtx *ctx);
static void replicate_release(RaftGroupCtx *ctx);
static void step_down_if_higher(RaftGroupCtx *ctx, int64 their_term);
static void control_maybe_compact(RaftGroupCtx *ctx);

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
    g->log.apply_owner_pid = 0;
    g->log.ring_full_waits = 0;
    g->log.ring_full_drops = 0;
    g->log.quorum_drops = 0;
    g->log.last_drop_plsn = 0;
    g->log.last_apply_fail = RAFT_APPLYFAIL_NONE;

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
 *   {"partition_lsn":N,"orig_lsn":"X/Y","rmid":R,"info":I,
 *    "flags":F,"gxid":G,"nbytes":B}
 *
 * 这样既复用了全部既有 Raft 机制（ring / prev_log 一致性检查 / 多数派提交 /
 * raft_log 持久化），又不必把任意长的字节流塞进 768 字节的 payload。
 * follower 必须**先把字节落盘 fsync 再 ack**，故"多数派提交"即"多数派已持久化"。
 */
#define RAFT_OP_PARWAL "OP_PARWAL"

/* 从描述符 JSON 里抠出一个整型字段（不引 jsonb，简单扫描即可） */
static int
entry_int_field(const char *payload, const char *key, int dflt)
{
    const char *p;
    int         v = 0;

    if (payload == NULL)
        return dflt;
    p = strstr(payload, key);
    if (p == NULL)
        return dflt;
    p = strchr(p, ':');
    if (p == NULL)
        return dflt;
    if (sscanf(p + 1, " %d", &v) != 1)
        return dflt;
    return v;
}

/*
 * 描述符里的 flags / info。旧 leader 发来的条目可能没有 "flags"，
 * 按 DATA 处理 —— 与 PartWALRecordIsData() 对 flags==0 的兼容判定一致。
 */
static int
entry_record_flags(const char *payload)
{
    return entry_int_field(payload, "\"flags\"", PARTWAL_FLAG_DATA);
}

static int
entry_record_info(const char *payload)
{
    return entry_int_field(payload, "\"info\"", 0);
}

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
    long long      gxid = 0;
    int            rmid = 0;
    int            info = 0;
    int            flags = 0;
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
    p = strstr(payload, "\"flags\"");
    if (p != NULL && (p = strchr(p, ':')) != NULL)
        (void) sscanf(p + 1, " %d", &flags);

    /*
     * "gxid" 是 parwal-3.0 的字段名；仍在途的 2.0 描述符只有 "xid"，按老语义
     * 当作节点号 0 的 gxid 收下 —— 值域上就是 gxid 的低位，无需换算。
     * 注意不能反过来用 strstr("\"xid\"") 兜底匹配 "gxid"：子串会误命中。
     */
    p = strstr(payload, "\"gxid\"");
    if (p == NULL)
        p = strstr(payload, ",\"xid\"");
    if (p != NULL && (p = strchr(p, ':')) != NULL)
        (void) sscanf(p + 1, " %lld", &gxid);

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
                     "%u::oid, %lld::bigint, %s::pg_lsn, %d, %d, %d, %lld::bigint, "
                     "decode(%s, 'hex'))",
                     (unsigned) local_oid,
                     (long long) entry_partition_lsn(payload),
                     quote_literal_cstr(orig_lsn),
                     rmid, info, flags, gxid,
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
 * data_apply_dtx_one — 单条 DTX 记录的本地登记（调用方持有 SPI）。
 *
 * ★ DECISION（info==2，DTX_2PC_DESIGN.md §6.2）：每个组成员在 apply 它时
 * 各自把决议登记进本地 partdist.dtx_decision。这正是"协调权随 Raft 选举
 * 自动转移"的落地点：协调组切主后，新 leader 手里天然就有全表，
 * dtx_status 立刻可答，不需要任何状态搬迁。载荷由 partwal_read_dtx_record
 * 从**本节点刚落盘的字节**解析，因此 follower 上的登记与 leader 逐字段
 * 相同。ON CONFLICT DO NOTHING —— 决议槽一次性，重复 apply 幂等。
 *
 * ★ FORGET（info==5，§9.7）：presumed abort 的标准收尾。协调组 leader 在
 * acked ⊇ participants 后追加它并复制到多数派；每个成员 apply 它时把该
 * dtxid 的决议行从本地索引删除 —— 删除经由组日志复制，所以**全体成员
 * 同步回收**，选举转移后也不留分叉的表。此后按协议不会再有人来问这笔
 * 决议（全部写过的参与者都已闭合并留标记）。
 */
static void
data_apply_dtx_one(RaftGroupCtx *ctx, int64 local_oid, int64 plsn, int info)
{
    StringInfoData sql;

    if (info == 5)
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "DELETE FROM partdist.dtx_decision dd "
                         "USING partdist.partwal_read_dtx_record(%u::oid, %lld) d "
                         "WHERE dd.dtxid = d.dtxid",
                         (unsigned) local_oid, (long long) plsn);
        if (SPI_execute(sql.data, false, 0) != SPI_OK_DELETE)
            elog(WARNING,
                 "pg_raft: 组 %lld apply FORGET 记录（plsn=%lld）删除决议行失败",
                 (long long) ctx->group_id, (long long) plsn);
        pfree(sql.data);
    }
    else if (info == 2)
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "INSERT INTO partdist.dtx_decision"
                         "(dtxid, coord_gsid, verdict, commit_ts, participants, decided_plsn) "
                         "SELECT d.dtxid, d.coord_gsid, d.verdict, d.commit_ts, "
                         "       coalesce(d.participants, '{}'::bigint[]), %lld "
                         "FROM partdist.partwal_read_dtx_record(%u::oid, %lld) d "
                         "WHERE d.dtxid IS NOT NULL "
                         "ON CONFLICT (dtxid) DO NOTHING",
                         (long long) plsn, (unsigned) local_oid,
                         (long long) plsn);
        if (SPI_execute(sql.data, false, 0) != SPI_OK_INSERT)
            elog(WARNING,
                 "pg_raft: 组 %lld 的 DECISION 记录(plsn=%lld)登记进 dtx_decision 失败",
                 (long long) ctx->group_id, (long long) plsn);
        pfree(sql.data);
    }
}

/*
 * 数据组的"平凡 apply"：不 redo，只把 applied_part_lsn 推到该条目的
 * partition_lsn。这补上了 follower_partition_map.applied_part_lsn 长期
 * "有表无写入方"的缺口，切主安全线从此比的是真实进度而非占位 0。
 */
static bool
data_apply_advance(RaftGroupCtx *ctx, int64 plsn,
                   const DtxApplyItem *dtx_items, int ndtx)
{
    StringInfoData sql;
    bool           spi_owned;
    int64          local_oid;
    bool           ok;

    if (plsn <= 0)
        return true;            /* 无进度可推，视为已应用 */

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0)
    {
        ctx->log->last_apply_fail = RAFT_APPLYFAIL_NO_PART;
        return false;           /* P0 映射还没建好，重试而不是跳过 */
    }

    if (!raft_persist_spi_begin(&spi_owned))
    {
        ctx->log->last_apply_fail = RAFT_APPLYFAIL_NO_SPI;
        return false;           /* 拿不到 SPI（如 BGW），下轮再来 */
    }

    /*
     * ★ 只有这条会与 prepared 事务撞锁，所以只跳过它（in_txn_replication）。
     *
     * 冲突链：prepare 路径在用户事务内触发复制 → apply → UPSERT
     * `follower_partition_map` 的进度行 → 事务进入 PREPARED，**行锁一直被持有**
     * → 协调者 dtx_decide 的复制再触发 apply 撞上同一行 → 永久阻塞。
     *
     * 初版是"用户事务内整个跳过 group_apply_pending"，太粗暴：写入负载下每次
     * propose 都在用户事务里，`last_applied` 永不推进，环容量检查
     * （last_log_index - last_applied）很快判满、拒收新条目 —— raft_17 实测
     * follower 卡死在 127（RAFT_LOG_CAPACITY=128）。
     *
     * 现在只跳过这条 SQL，**游标照常推进、DTX 决议索引照常维护**，
     * 环不会被撑满。代价：本节点这张表里的 applied_part_lsn 在纯 2PC 负载下
     * 会滞后（follower 侧不受影响——它们的 apply 跑在 pg_raft_append_entries
     * 这个顶层 SQL 调用里，不在任何 prepared 事务内）。该列服务于切主候选
     * 筛选，滞后只会让本节点显得"没追平"，是保守方向，不会误判为已追平。
     */
    if (in_txn_replication)
        ok = true;
    else
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "SELECT partdist.follower_set_applied_part_lsn(%u::oid, %lld)",
                         (unsigned) local_oid, (long long) plsn);
        ok = (SPI_execute(sql.data, false, 1) == SPI_OK_SELECT);
        pfree(sql.data);
    }

    /*
     * ★ DTX DECISION/FORGET 登记必须**逐条**处理，不随批量合并吞掉
     * （#39 批量 apply 与 DTX §6.2/§9.7 的合并适配，2026-08-13）。
     * 游标推进合并成一次没问题 —— 中间值没有读者；但 DECISION(info=2)
     * 要把决议登记进本地 partdist.dtx_decision、FORGET(info=5) 要删掉
     * 对应行，每条都有独立副作用。批量扫描按日志序收集 (plsn, info)
     * 清单随批传入，这里按同一顺序落账；载荷由 partwal_read_dtx_record
     * 从**本节点已落盘的字节**解析，只需要 plsn，不需要日志条目本身。
     */
    if (ok)
    {
        int i;

        for (i = 0; i < ndtx; i++)
            data_apply_dtx_one(ctx, local_oid, dtx_items[i].plsn,
                               dtx_items[i].info);
    }

    raft_persist_spi_end(spi_owned);
    ctx->log->last_apply_fail = ok ? RAFT_APPLYFAIL_NONE : RAFT_APPLYFAIL_SPI_EXEC;
    return ok;
}

static bool
data_entry_apply(RaftGroupCtx *ctx, const RaftLogEntry *e)
{
    DtxApplyItem it;
    int          nit = 0;
    int64        plsn = entry_partition_lsn(e->payload);
    int          info = entry_record_info(e->payload);

    if ((entry_record_flags(e->payload) & PARTWAL_FLAG_DTX) != 0 &&
        (info == 2 || info == 5))
    {
        it.plsn = plsn;
        it.info = info;
        nit = 1;
    }
    return data_apply_advance(ctx, plsn, nit ? &it : NULL, nit);
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
    SpinLockAcquire(&ctx->log->mutex);
    hs.base_index = ctx->log->base_index;
    hs.base_term = ctx->log->base_term;
    SpinLockRelease(&ctx->log->mutex);

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

    if (hs.version < 2)
        hs.last_applied = 0;    /* v1 没有该字段，退化为"从 0 起重放" */
    if (hs.version < 3)
        hs.base_index = hs.base_term = 0;   /* v1/v2：从未压缩过 */

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
    if (hs.base_index > ctx->log->base_index)
    {
        ctx->log->base_index = hs.base_index;
        ctx->log->base_term = hs.base_term;
    }
    /*
     * 压缩基点之前的条目已经不存在了，三个游标都不能落在基点之下 ——
     * 否则 apply 会去取一条永远取不到的条目而永久卡住。
     */
    if (ctx->log->last_log_index < ctx->log->base_index)
        ctx->log->last_log_index = ctx->log->base_index;
    if (ctx->log->commit_index < ctx->log->base_index)
        ctx->log->commit_index = ctx->log->base_index;
    if (ctx->log->last_applied < ctx->log->base_index)
        ctx->log->last_applied = ctx->log->base_index;
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

/*
 * 冲突截断时把 SQL 日志里 index 之后的行一并删掉。
 *
 * 环内截断只改 last_log_index，SQL 行照旧留着 —— 在有了 log_get_entry_sql
 * 回读之后，这些被截断的行会被重新读出来当成"本节点持有的条目"，重启的
 * restore 也会把它们灌回环。必须与环的截断同时发生。
 */
static void
delete_log_entries_after_sql(RaftGroupCtx *ctx, int64 index)
{
    StringInfoData sql;
    bool           spi_owned;

    if (index < 0 || !raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "DELETE FROM partdist.raft_log "
                     "WHERE group_id = %lld AND log_index > %lld",
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

/*
 * 从 partdist.raft_log 读回一条**已滑出环窗口**的条目。
 *
 * 环只有 RAFT_LOG_CAPACITY(128) 条，而 SQL 日志是全量持久的。此前没有这条
 * 回读路径，于是落后超过环容量的 follower **永远追不上**：leader 侧
 * log_get_entry_locked 在槽位被覆盖时失败 → prev_term 取到 0 → follower 拒绝
 * → next_index 一路退到 1 → 此后只发心跳、match 恒 0，静默卡死（§12.3.B.6
 * 记录的"某节点 group0 恒 0/0/0"就是这个形态）。字节其实一直都在盘上。
 *
 * 调用方必须处于 SPI 可用语境（raft_spi_ctx），且不得持有 log->mutex。
 */
static bool
log_get_entry_sql(RaftGroupCtx *ctx, int64 index, RaftLogEntry *out)
{
    StringInfoData sql;
    bool           spi_owned;
    bool           found = false;

    if (index <= 0)
        return false;
    if (!raft_persist_spi_begin(&spi_owned))
        return false;
    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return false;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT term, op_type, payload::text "
                     "FROM partdist.raft_log WHERE group_id = %lld AND log_index = %lld",
                     (long long) ctx->group_id, (long long) index);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        HeapTuple tup = SPI_tuptable->vals[0];
        TupleDesc desc = SPI_tuptable->tupdesc;
        bool      isnull;
        char     *op;
        char     *payload;

        out->index = index;
        out->term = DatumGetInt64(SPI_getbinval(tup, desc, 1, &isnull));
        op = TextDatumGetCString(SPI_getbinval(tup, desc, 2, &isnull));
        payload = TextDatumGetCString(SPI_getbinval(tup, desc, 3, &isnull));
        strlcpy(out->op_type, op, RAFT_OP_LEN);
        strlcpy(out->payload, payload, RAFT_PAYLOAD_MAX);
        pfree(op);
        pfree(payload);
        found = true;
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return found;
}

/*
 * 两级取条目：先查环（无锁开销最小），环外再落到 SQL 日志。
 * **不得**在持有 log->mutex 时调用（SQL 那一级要走 SPI）。
 * 非 SPI 语境（BGW tick）退化为只查环，行为与加这条回读之前完全一致。
 */
static bool
log_get_entry_ext(RaftGroupCtx *ctx, int64 index, RaftLogEntry *out)
{
    bool got;

    SpinLockAcquire(&ctx->log->mutex);
    got = log_get_entry_locked(ctx, index, out);
    SpinLockRelease(&ctx->log->mutex);

    if (got || !raft_spi_ctx)
        return got;

    return log_get_entry_sql(ctx, index, out);
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
    else if (*last_idx == ctx->log->base_index)
        *last_term = ctx->log->base_term;   /* 末尾恰好就是压缩基点 */
}

/*
 * 取 index 处的 term。压缩基点上的条目已被删除，只剩 base_term 可用 ——
 * prev 一致性检查与选举的日志新旧比较都必须认它，否则一压缩就没人能通过
 * 基点处的 prev 检查（也就没人能再当选或被复制）。
 *
 * 返回 false = 本节点无从判断（比基点还老，或是尚未持有的空洞）。
 * 调用方**不得**持有 log->mutex：SQL 那一级要走 SPI。
 */
static bool
log_term_at(RaftGroupCtx *ctx, int64 index, int64 *term)
{
    RaftLogEntry e;
    int64        base_idx;
    int64        base_term;

    if (index <= 0)
    {
        *term = 0;
        return true;
    }

    SpinLockAcquire(&ctx->log->mutex);
    base_idx = ctx->log->base_index;
    base_term = ctx->log->base_term;
    SpinLockRelease(&ctx->log->mutex);

    if (index == base_idx)
    {
        *term = base_term;
        return true;
    }

    if (log_get_entry_ext(ctx, index, &e))
    {
        *term = e.term;
        return true;
    }
    return false;
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

/*
 * wait_for_log_room — 环满时**阻塞等 apply 追上**，而不是直接丢弃提案（§13 约束 13）。
 *
 * 为什么必须是等而不是拒：数据面的提案一旦被拒，pg_partdist 侧的复制挂钩报错、
 * 事务中止 —— 可对 VACUUM 这类调用方，它的物理变更（尾部截断、页面冻结）**在
 * leader 上已经 durable 且不随事务回滚**。于是 leader 短了、follower 没短，
 * 而 discard 还会把 leader 段里那条也截掉，分叉从此无痕。
 * 把"拒绝"换成"背压"，写入侧慢下来，数据不丢。
 *
 * 等待是安全的，不会自锁 —— 实测（1 分片 3 副本，分批 INSERT + VACUUM FREEZE）
 * 环深度会瞬时冲到 86 但每次都回落到 0：**apply 是健康的，环满是突发流量的
 * 瞬时现象**。会让 apply 真正停住的曾是认领在 FATAL 上泄漏（#39，已由常驻
 * before_shmem_exit 回调归还）和 data_entry_apply 持续返回 false（超时兜底）。
 *
 * 每轮先 group_apply_pending 主动排空（本 backend 能推就自己推），推不动
 * （另一个 backend 正持有 apply 认领）才睡 1ms 再看。
 */
static bool
wait_for_log_room(RaftGroupCtx *ctx)
{
    TimestampTz start = GetCurrentTimestamp();
    bool        waited = false;

    for (;;)
    {
        bool has_room;

        group_apply_pending(ctx);

        SpinLockAcquire(&ctx->log->mutex);
        has_room = (ctx->log->last_log_index - ctx->log->last_applied
                    < RAFT_LOG_CAPACITY - 1);
        if (!has_room && !waited)
            ctx->log->ring_full_waits++;
        SpinLockRelease(&ctx->log->mutex);

        if (has_room)
            return true;

        waited = true;

        if (pg_raft_propose_wait_ms <= 0)
            return false;       /* 0 = 不等，保留旧行为（诊断用） */
        if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(),
                                       pg_raft_propose_wait_ms))
            return false;

        CHECK_FOR_INTERRUPTS();     /* 用户取消 / 关库要能打断 */
        pg_usleep(1000L);
    }
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

    /*
     * ★ 早退条件必须是"是否已经灌过"，**不能**是 last_log_index > 0（2026-08-05 修）。
     * 有了日志压缩之后，重启时 restore_hard_state_if_needed 会先把 last_log_index
     * 顶到压缩基点（基点之前的条目已被快照取代，三个游标都不能落在它之下），
     * 于是这里会误判成"已经有日志了"而**整段跳过**：表里 base+1..N 的尾巴永远
     * 灌不回环。对 follower 只是要 leader 重发一遍；对**重启后重新当选的 leader**
     * 就是灾难 —— 它以为自己的日志止于基点，会拿新内容去覆盖 base+1.. 这些**已经
     * 提交**的位置，直接破坏 Leader Completeness。
     */
    if (ctx->g->log_restored)
        return;

    /*
     * 旗只在**真的灌完**之后才置（下面每条失败路径都原样返回、留待下次重试）。
     * 提前置的话，一次拿不到 SPI 或建表还没跑完就把这一组永久标成"已恢复"，
     * 尾巴就再也灌不回来了 —— 那正是本函数要防的事故。
     */
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
    /* 压缩基点之前的条目已不存在，游标不能落在它之下（否则 apply 取不到条目） */
    if (ctx->log->last_log_index < ctx->log->base_index)
        ctx->log->last_log_index = ctx->log->base_index;
    if (ctx->log->commit_index < ctx->log->base_index)
        ctx->log->commit_index = ctx->log->base_index;
    if (ctx->log->last_applied < ctx->log->base_index)
        ctx->log->last_applied = ctx->log->base_index;
    SpinLockRelease(&ctx->log->mutex);

    ctx->g->log_restored = true;
    raft_persist_spi_end(spi_owned);
    if (clamped_hardstate)
        persist_hard_state_unlocked(ctx);
}

/*
 * 把一条 apply 包进子事务里执行（2026-08-05 修）。
 *
 * 起因：apply 体里任何一条 SQL 抛错（实测是一条 payload 违反
 * node_map_status_check 的"毒丸"条目）都会直接从 apply_one_entry 里
 * ereport 出去，而 apply_in_progress 是在它**之前**置上、之后才清的 ——
 * 于是这面旗永久留在 true，本节点此后每次 group_apply_pending 都在
 * "另一个 backend 正在 apply" 这一分支立即返回：**apply 游标从此永久冻结、
 * 无任何告警**。实测九个节点全部卡在同一个 index 上，而 commit_index 照常
 * 前进；有了日志压缩之后更糟——压缩点也跟着不动，节点既追不上也压不了。
 *
 * 处置与 apply_one_entry 的既有语义对齐：控制面"元数据表持久、跳过安全"，
 * 抛错按跳过处理（游标照常推进，只记 WARNING）；数据面漏一条 redo 就是分叉，
 * 抛错按失败处理（游标不推进，下一轮重试），但**旗一定清掉**。
 */
static bool
apply_one_entry_guarded(RaftGroupCtx *ctx, const RaftLogEntry *e)
{
    MemoryContext oldcxt = CurrentMemoryContext;
    ResourceOwner oldowner = CurrentResourceOwner;
    bool          ok;

    BeginInternalSubTransaction(NULL);
    PG_TRY();
    {
        ok = apply_one_entry(ctx, e);
        ReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcxt);
        CurrentResourceOwner = oldowner;
    }
    PG_CATCH();
    {
        ErrorData *edata;

        MemoryContextSwitchTo(oldcxt);
        edata = CopyErrorData();
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcxt);
        CurrentResourceOwner = oldowner;

        ok = (ctx->group_id == RAFT_CONTROL_GROUP);
        elog(WARNING,
             "pg_raft: group %lld 的条目 %lld(%s) apply 抛错：%s —— %s",
             (long long) ctx->group_id, (long long) e->index, e->op_type,
             edata->message,
             ok ? "控制面按既有语义跳过" : "数据面保留游标，下轮重试");
        FreeErrorData(edata);
    }
    PG_END_TRY();

    return ok;
}

/*
 * raft_apply_claim_release_on_exit — 本进程退出时归还它还持有的 apply 认领。
 *
 * 常驻 before_shmem_exit 回调，每 backend 首次进入 group_apply_pending 时注册
 * 一次。它覆盖的正是 bool 版泄漏的那条路：FATAL（proc_exit 跑该回调，但既不走
 * PG_CATCH、postmaster 也不重置 shmem）。正常路径下认领早已归还，这里空转。
 *
 * 只清 owner == MyProcPid 的组 —— 别人的认领一个不碰。
 * 不许在这里 ereport(ERROR)：退出路径上抛错会递归。
 */
static void
raft_apply_claim_release_on_exit(int code, Datum arg)
{
    int i;

    if (RaftGroups == NULL)
        return;

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];

        if (!g->in_use)
            continue;
        SpinLockAcquire(&g->log.mutex);
        if (g->log.apply_owner_pid == MyProcPid)
            g->log.apply_owner_pid = 0;
        SpinLockRelease(&g->log.mutex);
    }
}

static bool apply_exit_cb_registered = false;

static void
group_apply_pending(RaftGroupCtx *ctx)
{
    RaftLogEntry e;

    /*
     * 注册要在**取任何自旋锁之前**做：before_shmem_exit 在回调表满时会
     * ereport(ERROR)，在锁下抛错 = 锁永不归还。每 backend 只注册一次。
     */
    if (!apply_exit_cb_registered)
    {
        before_shmem_exit(raft_apply_claim_release_on_exit, (Datum) 0);
        apply_exit_cb_registered = true;
    }

    /*
     * ★★★ 用户事务块里的 apply **照常做**（2026-08-13，#39 重放合并决策）。
     *
     * #39 存档原本在这里放了 IsTransactionBlock() 闸门：事务块 backend 只等
     * 不 apply，apply 委托给"干净上下文"。但那次冻结恰好停在委托目标
     * （bgworker apply）建成之前 —— BGW tick 没有 SPI 干不了，而
     * pg_raft_catchup 是 leader→follower 的**补发**通道、不做本地 apply。
     * 闸门落地而委托缺位的结果是 leader 的 apply 全靠自动提交语句偶发排空：
     * r1 重放实测显式事务洪水下 applied 纹丝不动（last_log=127 commit=127
     * applied=0，认领无人持有、apply 未报错），环满 → 背压等满 10s → 丢弃。
     *
     * 闸门当年要破的跨节点等待环——
     *     组 X 的 apply ← prepared 事务 T1 持有的 follower_partition_map 行锁
     *     T1 的 COMMIT PREPARED ← 其它参与者 prepare ← 组 X 的环有空位
     * ——TX 期已由 in_txn_replication 外科式拆掉：事务内 apply 只跳过那条
     * 撞锁的 UPSERT，游标照常推进（见 data_apply_advance），环不会被撑满，
     * 行锁根本不进用户事务。故闸门撤除，保留 #39 的背压/认领/批量/计数。
     */

    for (;;)
    {
        int64 idx;
        int64 batch_end;
        int64 batch_plsn;
        DtxApplyItem dtx_items[RAFT_LOG_CAPACITY];
        int   ndtx;
        bool  applied_ok;

        SpinLockAcquire(&ctx->log->mutex);
        if (ctx->log->last_applied >= ctx->log->commit_index)
        {
            SpinLockRelease(&ctx->log->mutex);
            break;
        }
        if (ctx->log->apply_owner_pid != 0)
        {
            /* 另一个 backend 正在 apply 本组，交给它按序做完。
             * 刻意**不做** owner == MyProcPid 的例外：本函数不可重入，
             * 加了例外反而把不可重入悄悄改成可重入（上次因此回退）。 */
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
        ctx->log->apply_owner_pid = MyProcPid;

        /*
         * ★ 数据组把 [idx, commit_index] 的连续段**合并成一次游标推进**。
         *
         * 数据条目的 apply = follower_set_applied_part_lsn 单调推进游标，
         * 逐条推与只推到最后一条**语义等价** —— 中间值没有任何读者。
         * 而逐条推的代价是每条一次 SPI(UPDATE + WAL)：实测 FPI 洪水下
         * apply 只有 ~13 条/秒，128 槽的环持续饱和，总有提案等满
         * propose_wait_ms 被丢弃(m 轮 6 次丢弃全部由此而来，每次丢弃 =
         * 副本永久分叉)。合并后一批一次 SPI，apply 成本除以批长。
         *
         * 只认环里**连续**存在的条目(log_get_entry_locked 失败即止)；
         * plsn 取批内最大值 —— 重复 propose 同一 plsn 时序上乱不了
         * (follower_set_applied 本就单调)。控制面不合并：它的 apply
         * 写的是各不相同的元数据表，逐条语义必须保留。
         * entry_partition_lsn 是纯字符串扫描(无 palloc/elog)，锁下可用；
         * 至多 CAPACITY 次，几十微秒。
         */
        batch_end  = idx;
        batch_plsn = 0;
        ndtx       = 0;
        if (ctx->group_id != RAFT_CONTROL_GROUP)
        {
            RaftLogEntry be;
            int64        p;
            int64        pl;

            /*
             * DTX DECISION/FORGET 逐条语义的保留：扫描时按日志序收集
             * (plsn, info)，随批传给 data_apply_advance 逐条落账。
             * entry_record_flags/info 与 entry_partition_lsn 同为纯字符串
             * 扫描（strstr/sscanf，无 palloc/elog），锁下可用。
             */
            if (strcmp(e.op_type, RAFT_OP_PARWAL) == 0)
            {
                batch_plsn = entry_partition_lsn(e.payload);
                if ((entry_record_flags(e.payload) & PARTWAL_FLAG_DTX) != 0)
                {
                    int inf = entry_record_info(e.payload);

                    if (inf == 2 || inf == 5)
                    {
                        dtx_items[ndtx].plsn = batch_plsn;
                        dtx_items[ndtx].info = inf;
                        ndtx++;
                    }
                }
            }
            for (p = idx + 1; p <= ctx->log->commit_index; p++)
            {
                if (!log_get_entry_locked(ctx, p, &be))
                    break;
                if (strcmp(be.op_type, RAFT_OP_PARWAL) == 0)
                {
                    pl = entry_partition_lsn(be.payload);
                    if (pl > batch_plsn)
                        batch_plsn = pl;
                    if ((entry_record_flags(be.payload) & PARTWAL_FLAG_DTX) != 0)
                    {
                        int inf = entry_record_info(be.payload);

                        if ((inf == 2 || inf == 5) && ndtx < RAFT_LOG_CAPACITY)
                        {
                            dtx_items[ndtx].plsn = pl;
                            dtx_items[ndtx].info = inf;
                            ndtx++;
                        }
                    }
                }
                batch_end = p;
            }
        }
        SpinLockRelease(&ctx->log->mutex);

        /*
         * **游标只在 apply 成功之后才推进**。原先是先推进再 apply，一旦
         * apply 抛错或进程在两者之间死掉，这条就被永久标记为已应用却从未
         * 生效；物理回放下这等于静默丢一条 redo。
         */
        /*
         * ★★ apply 认领必须在抛错路径上归还。控制面走 apply_one_entry_guarded
         * （子事务内接 ERROR，正常返回 bool），但数据面的 data_apply_advance
         * 走 SPI（follower_set_applied_part_lsn），任何一次 ERROR 都是
         * longjmp —— 后面的 SpinLockAcquire 根本不会执行。
         *
         * PG_CATCH 只接 ERROR；FATAL 由常驻 before_shmem_exit 回调归还
         * （raft_apply_claim_release_on_exit），kill -9 由 postmaster 的节点
         * 重置清零 —— 三条死亡路径各有各的归还机制。修此缺陷前实测：
         *     last_log=2767 commit=2767 applied=2640（差恰好 127）
         *     apply 最近一次失败原因：无
         * 即条目全都提交了、apply 也没报错，纯粹是没人能拿到认领。
         */
        PG_TRY();
        {
            if (ctx->group_id == RAFT_CONTROL_GROUP)
                applied_ok = apply_one_entry_guarded(ctx, &e);
            else if (batch_plsn > 0 || ndtx > 0)
                applied_ok = data_apply_advance(ctx, batch_plsn,
                                                dtx_items, ndtx);
            else
                applied_ok = true;      /* 批内没有带 plsn 的数据条目 */
        }
        PG_CATCH();
        {
            SpinLockAcquire(&ctx->log->mutex);
            if (ctx->log->apply_owner_pid == MyProcPid)
                ctx->log->apply_owner_pid = 0;
            SpinLockRelease(&ctx->log->mutex);
            PG_RE_THROW();
        }
        PG_END_TRY();

        SpinLockAcquire(&ctx->log->mutex);
        if (ctx->log->apply_owner_pid == MyProcPid)
            ctx->log->apply_owner_pid = 0;
        if (applied_ok && ctx->log->last_applied < batch_end)
            ctx->log->last_applied = batch_end;
        SpinLockRelease(&ctx->log->mutex);

        if (!applied_ok)
            break;      /* 下一轮 tick 重试同一条 */

        persist_hard_state_unlocked(ctx);
    }

    control_maybe_compact(ctx);
}

/*
 * 控制面日志压缩（计划文档 §4 阶段 1 的第二个 ❌）。
 *
 * partdist.raft_log 此前只增不删：一轮回归就 240+ 行，重启时
 * restore_persistent_log_if_needed 还要全表读回。压缩把 last_applied 之前的
 * 行删掉，并把基点 (base_index, base_term) 记进 hardstate —— 删掉的那一段之后
 * 只能靠 InstallSnapshot 传输，这正是快照与压缩必须同期落地的原因。
 *
 * 只做控制面：它的状态机是 partdist 的两张元数据表，`partdist.raft_snapshot`
 * 在每次 apply 时已经把整张表存下来了，压缩点上的状态天然可得。数据组的状态机
 * 在 P3 之前就是 parwal 字节流本身，压缩它的正解是日志外部化（§11.5.2 #2），
 * 不是在这里删行。
 *
 * 触发点选在 apply 之后：此时一定在 SPI 可用的语境里（控制面 apply 本身就要
 * 写元数据表），且 last_applied 刚刚推进过。
 */
static void
control_maybe_compact(RaftGroupCtx *ctx)
{
    StringInfoData sql;
    bool           spi_owned;
    int64          applied;
    int64          base_idx;
    int64          keep_term = 0;

    if (ctx->group_id != RAFT_CONTROL_GROUP)
        return;
    if (pg_raft_compact_threshold <= 0)
        return;

    SpinLockAcquire(&ctx->log->mutex);
    applied = ctx->log->last_applied;
    base_idx = ctx->log->base_index;
    SpinLockRelease(&ctx->log->mutex);

    if (applied - base_idx < pg_raft_compact_threshold)
        return;

    /*
     * 基点那一条的 term 必须先拿到：压缩之后它就没了，而 prev 一致性检查与
     * 选举的日志新旧比较都还要用它。取不到就**不压缩**（宁可日志长一点）。
     */
    if (!log_term_at(ctx, applied, &keep_term) || keep_term <= 0)
        return;

    if (!raft_persist_spi_begin(&spi_owned))
        return;
    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    /*
     * 快照必须**先于**删行落库：反过来一旦中间崩溃，日志没了、快照也没有，
     * 落后成员就再也追不上了。raft_snapshot 是 upsert，重复执行无害。
     */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO partdist.raft_snapshot "
                     "(singleton, last_included_index, last_included_term, node_map, partition_map, updated_at) "
                     "SELECT 1, %lld, %lld, "
                     "COALESCE((SELECT jsonb_agg(to_jsonb(n) ORDER BY n.node_id) FROM partdist.node_map n), '[]'::jsonb), "
                     "COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.partition_id) FROM partdist.partition_map p), '[]'::jsonb), "
                     "now() "
                     "ON CONFLICT (singleton) DO UPDATE SET "
                     "last_included_index = EXCLUDED.last_included_index, "
                     "last_included_term = EXCLUDED.last_included_term, "
                     "node_map = EXCLUDED.node_map, "
                     "partition_map = EXCLUDED.partition_map, updated_at = now() "
                     "WHERE partdist.raft_snapshot.last_included_index <= EXCLUDED.last_included_index",
                     (long long) applied, (long long) keep_term);
    if (SPI_execute(sql.data, false, 0) < 0)
    {
        pfree(sql.data);
        raft_persist_spi_end(spi_owned);
        return;
    }
    resetStringInfo(&sql);

    appendStringInfo(&sql,
                     "DELETE FROM partdist.raft_log "
                     "WHERE group_id = 0 AND log_index <= %lld",
                     (long long) applied);
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    SpinLockAcquire(&ctx->log->mutex);
    if (applied > ctx->log->base_index)
    {
        ctx->log->base_index = applied;
        ctx->log->base_term = keep_term;
    }
    SpinLockRelease(&ctx->log->mutex);

    persist_hard_state_unlocked(ctx);

    elog(LOG, "pg_raft: 组 0 日志已压缩到 index=%lld term=%lld（%lld 条已由快照取代）",
         (long long) applied, (long long) keep_term,
         (long long) (applied - base_idx));
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

/*
 * 追平提示：直接问对端「你这一组的 last_log_index 是多少」。
 *
 * 为什么需要：本实现的 AppendEntries 响应里没有 conflict hint，`next_index` 只能
 * 一次退一格。而新当选的 leader 会把所有 peer 的 next_index 初始化成 last+1，
 * 于是一个落后 N 条的成员要先花 N 个 RPC 才探回到它真正持有的位置 —— 落后几百条
 * 时，一次追平还没探完就可能被下一次选举打断，重来一遍，看起来像永远追不上。
 *
 * 只用于后台追平通道，且**只作起点提示**：AE 的 prev 一致性检查照旧，对端若在
 * 该位置上持有不同的条目仍会拒绝，leader 继续按老办法逐格回退。因此这里读到
 * 陈旧或错误的值都不影响正确性，只影响快慢。
 */
static int64
peer_last_log_index(RaftGroupCtx *ctx, RaftPeer *p)
{
    PGconn   *conn;
    PGresult *res;
    char      sql[192];
    int64     val = -1;

    if (peer_in_backoff(p))
        return -1;

    conn = peer_conn_get(p);
    if (conn == NULL)
    {
        peer_mark_result(p, false);
        return -1;
    }

    snprintf(sql, sizeof(sql),
             "SELECT last_log_index FROM partdist.pg_raft_group_status() "
             "WHERE group_id = %lld",
             (long long) ctx->group_id);

    res = PQexec(conn, sql);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
        !PQgetisnull(res, 0, 0))
        val = strtoll(PQgetvalue(res, 0, 0), NULL, 10);
    else if (PQstatus(conn) != CONNECTION_OK)
    {
        PQclear(res);
        peer_conn_reset(peer_slot_of(p));
        peer_mark_result(p, false);
        return -1;
    }
    PQclear(res);
    peer_mark_result(p, true);
    return val;
}

/*
 * InstallSnapshot 的发送侧（计划文档 §4 阶段 1 / §12.4 #7）。
 *
 * 只服务**控制面（组 0）**：它的状态机就是 partdist.node_map + partition_map，
 * 有紧凑表示，日志压缩掉的那一段只能靠它传输。数据组的状态机在 P3（物理回放）
 * 之前就是 parwal 字节流本身，"快照内容"与"日志内容"是同一份东西，装快照等于
 * 重放日志 —— 那样的快照只会是个假机制，所以这里不做。
 *
 * 载荷从 partdist.raft_snapshot 读（apply 时一直在写），因此**必须有 SPI**；
 * 调用方保证在 client backend 语境里（追平通道 / propose 路径）。
 * 用 PQexecParams 传参而不是拼串：node_map/partition_map 是任意长 JSON。
 */
static bool
send_install_snapshot(RaftGroupCtx *ctx, RaftPeer *p, int64 term, int64 base_idx)
{
    StringInfoData sql;
    PGconn        *conn;
    PGresult      *res;
    bool           spi_owned;
    bool           isnull;
    char          *snap_idx = NULL;
    char          *snap_term = NULL;
    char          *node_map = NULL;
    char          *partition_map = NULL;
    char           s_term[32];
    char           s_leader[16];
    const char    *params[6];
    int64          rt = 0;
    int            ok_flag = 0;
    bool           sent = false;
    MemoryContext  oldctx;

    if (ctx->group_id != RAFT_CONTROL_GROUP)
        return false;
    if (peer_in_backoff(p))
        return false;
    if (!raft_persist_spi_begin(&spi_owned))
        return false;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT last_included_index::text, last_included_term::text, "
                     "       node_map::text, partition_map::text "
                     "  FROM partdist.raft_snapshot WHERE singleton = 1 "
                     "   AND last_included_index >= %lld",
                     (long long) base_idx);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        HeapTuple tup = SPI_tuptable->vals[0];
        TupleDesc desc = SPI_tuptable->tupdesc;

        oldctx = MemoryContextSwitchTo(TopTransactionContext);
        snap_idx = TextDatumGetCString(SPI_getbinval(tup, desc, 1, &isnull));
        snap_term = TextDatumGetCString(SPI_getbinval(tup, desc, 2, &isnull));
        node_map = TextDatumGetCString(SPI_getbinval(tup, desc, 3, &isnull));
        partition_map = TextDatumGetCString(SPI_getbinval(tup, desc, 4, &isnull));
        MemoryContextSwitchTo(oldctx);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (snap_idx == NULL || node_map == NULL || partition_map == NULL)
    {
        elog(WARNING,
             "pg_raft: 组 0 已压缩到 %lld，但快照表里没有可用载荷，无法为落后成员装快照",
             (long long) base_idx);
        return false;
    }

    conn = peer_conn_get(p);
    if (conn == NULL)
    {
        peer_mark_result(p, false);
        return false;
    }

    snprintf(s_term, sizeof(s_term), "%lld", (long long) term);
    snprintf(s_leader, sizeof(s_leader), "%d", pg_raft_node_id);
    params[0] = s_term;
    params[1] = s_leader;
    params[2] = snap_idx;
    params[3] = snap_term;
    params[4] = node_map;
    params[5] = partition_map;

    res = PQexecParams(conn,
                       "SELECT partdist.pg_raft_install_snapshot("
                       "$1::bigint, $2::int, $3::bigint, $4::bigint, $5::text, $6::text)",
                       6, NULL, params, NULL, NULL, 0);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
        sent = parse_resp2(PQgetvalue(res, 0, 0), &rt, &ok_flag);
    else if (PQstatus(conn) != CONNECTION_OK)
    {
        PQclear(res);
        peer_conn_reset(peer_slot_of(p));
        peer_mark_result(p, false);
        return false;
    }
    else
        elog(WARNING, "pg_raft: InstallSnapshot 到 node %d 失败: %s",
             p->node_id, PQerrorMessage(conn));
    PQclear(res);
    peer_mark_result(p, true);

    if (!sent)
        return false;

    if (rt > term)
    {
        step_down_if_higher(ctx, rt);
        return true;            /* 已降级，本轮到此为止 */
    }

    if (ok_flag)
    {
        int   slot = peer_slot_of(p);
        int64 installed;

        /*
         * 游标要按**快照行里的 index** 记，不能按调用方传进来的 base_idx。
         * 取载荷的 WHERE 是 `last_included_index >= base_idx`：两者之间又压缩了
         * 一轮的话，对端装到的是更新的那个点。此时若按 base_idx 记 match/next，
         * leader 下一轮会去补 base_idx+1 —— 那条在对端已随压缩删掉，prev 检查
         * 取不到 term 只能拒绝，白白退一轮 nextIndex 才重新触发快照。
         */
        installed = strtoll(snap_idx, NULL, 10);
        if (installed < base_idx)
            installed = base_idx;

        SpinLockAcquire(&ctx->log->mutex);
        if (ctx->log->peer_match_index[slot] < installed)
            ctx->log->peer_match_index[slot] = installed;
        if (ctx->log->peer_next_index[slot] < installed + 1)
            ctx->log->peer_next_index[slot] = installed + 1;
        SpinLockRelease(&ctx->log->mutex);
        elog(LOG, "pg_raft: 已向 node %d 安装组 0 快照（last_included_index=%lld）",
             p->node_id, (long long) installed);
        return true;
    }
    return false;
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
    int64 next_idx;
    int64 last_idx;
    int64 base_idx;
    int64 leader_commit;
    RaftLogEntry entry;
    int64 rt;
    int   ok_flag;
    bool  has_entry;
    bool  prev_known = false;
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
        {
            prev_term = pe->term;
            prev_known = true;
        }
    }
    else
        prev_known = true;
    next_idx = ctx->log->peer_next_index[peer_slot];
    last_idx = ctx->log->last_log_index;
    base_idx = ctx->log->base_index;
    if (prev_idx > 0 && prev_idx == base_idx)
    {
        prev_term = ctx->log->base_term;    /* prev 恰好是压缩基点 */
        prev_known = true;
    }
    has_entry = (next_idx <= last_idx &&
                 log_get_entry_locked(ctx, next_idx, &entry));
    SpinLockRelease(&ctx->log->mutex);

    if (!repl_ready)
        return;

    /*
     * 对端需要的条目已被压缩掉（next_index 落在基点或更早）：日志里没有了，
     * 只能装快照。需要 SPI 读快照载荷，因此只在 client backend 语境下做；
     * BGW tick 里就先跳过，等追平通道那一轮。
     */
    if (base_idx > 0 && next_idx <= base_idx && raft_spi_ctx)
    {
        if (send_install_snapshot(ctx, &peers[peer_slot], term, base_idx))
            return;
    }

    /*
     * 环外回退：peer 落后超过环容量时，prev 与待发条目都已滑出环窗口。
     * SPI 语境（propose / pg_raft_catchup）下从 SQL 日志读回来，这样落后
     * 任意远的 follower 都能被逐条追平；BGW tick 无 SPI，维持原行为
     * （prev_term=0 → 对端拒 → next_index 递减 → 仅心跳），不改变时序语义。
     */
    if (raft_spi_ctx)
    {
        RaftLogEntry old;

        if (!prev_known && log_get_entry_sql(ctx, prev_idx, &old))
        {
            prev_term = old.term;
            prev_known = true;
        }
        if (!has_entry && next_idx <= last_idx &&
            log_get_entry_sql(ctx, next_idx, &entry))
            has_entry = true;
    }

    /*
     * prev 无从取证时不能硬发：prev_term 会当成 0 送出去，对端必拒，
     * 白白把 next_index 往下推。不发条目、只发心跳（保持对端选举计时器）。
     */
    if (!prev_known)
        has_entry = false;

    /*
     * 数据组的条目必须带上真实 parwal 字节；取字节要 SPI，BGW tick 里拿不到，
     * 此时降级为心跳（不推进 next_index），等下一次 propose 再追平。
     */
    if (has_entry && strcmp(entry.op_type, RAFT_OP_PARWAL) == 0)
    {
        if (!raft_spi_ctx)
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
 * data_group_promote_prepare — 上报前把本节点该分片"准备成主"。
 *
 * 做两件事（实现在 partdist.pg_raft_promote_prepare，见 pg_raft--1.0.sql）：
 * 把惰性回放追平到 Raft 已提交位点、闭合 in-doubt 分布式事务。
 *
 * 三个约束决定了它必须长这样：
 *   a) 本函数跑在 BGW tick 里，**没有 SPI**，只能走 libpq 自连接（与 DTX 参与
 *      登记、恢复守护跑 COMMIT PREPARED 同一手法）；
 *   b) tick 还要给其余各组发心跳，**不能久占** —— 所以每次只推进
 *      pg_raft.promote_catchup_slice_ms 毫秒，没追完返回 false，下个 tick 接着追。
 *      惰性回放平时一条 redo 都不做，升主时的积压可能很大，必须切片；
 *   c) 追不平就永不上报会让分片**永久无主**，比读到旧数据更糟。所以设
 *      pg_raft.promote_catchup_deadline_ms 兜底：超过它就带 WARNING 放行，
 *      把"可用性优先"这个取舍显式化，而不是让它静默发生。
 *
 * 截止期按 (group_id, 首次尝试时刻) 记在 BGW 进程本地 —— 单进程、组数有上限，
 * 不值得为它动共享内存结构。进程重启即重新计时，语义上等价于重新开始追平。
 */
static bool
data_group_promote_prepare(int64 group_id)
{
    static struct {
        int64       group_id;
        TimestampTz first_try;
    } deadline_state[RAFT_MAX_GROUPS];
    static bool  deadline_init = false;

    PGconn      *conn;
    PGresult    *res;
    char         selfconn[256];
    char         sql[256];
    bool         ok = false;
    int          verdict = 0;
    int          i;
    int          slot = -1;
    int          free_slot = -1;
    TimestampTz  now = GetCurrentTimestamp();

    if (!deadline_init)
    {
        memset(deadline_state, 0, sizeof(deadline_state));
        deadline_init = true;
    }

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        if (deadline_state[i].group_id == group_id)
        {
            slot = i;
            break;
        }
        if (free_slot < 0 && deadline_state[i].group_id == 0)
            free_slot = i;
    }
    if (slot < 0 && free_slot >= 0)
    {
        slot = free_slot;
        deadline_state[slot].group_id = group_id;
        deadline_state[slot].first_try = now;
    }

    pg_raft_format_conninfo("127.0.0.1", PostPortNumber, selfconn, sizeof(selfconn));
    conn = PQconnectdb(selfconn);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(WARNING, "pg_raft: 升主前置连回本节点失败: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    snprintf(sql, sizeof(sql),
             "SELECT partdist.pg_raft_promote_prepare(%lld, %d)",
             (long long) group_id, pg_raft_promote_catchup_slice_ms);

    res = PQexec(conn, sql);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
        verdict = atoi(PQgetvalue(res, 0, 0));
    else
        elog(WARNING, "pg_raft: 升主前置执行失败(组 %lld): %s",
             (long long) group_id, PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);

    ok = (verdict == 1);

    /*
     * ★ verdict < 0 = 检测到快路径分叉（DTX_2PC_DESIGN.md §9.5，第 6 步 b）。
     *
     * 与"还没追平"不是一回事：追不平是**暂时**状态，可以被 deadline 按
     * 可用性优先放行；分叉是**已知事实** —— 本节点段流里有它自己写的 COMMIT
     * 标记，而那笔事务在本地 CLOG 里并没有提交。放行等于让一个已知与组分叉的
     * 副本当上主库，比"该分片暂时无主"坏得多。所以这里**清掉截止期计时并直接
     * 返回**，绕过下面那段兜底逻辑，永不放行。
     *
     * 恢复手段是重做物理基线（惰性回放的 re-baseline 路径），完成后该分片
     * 重新 replay_enable 即可再次参选 —— 那时段流与本地 CLOG 不再矛盾。
     */
    if (verdict < 0)
    {
        if (slot >= 0)
            deadline_state[slot].group_id = 0;
        return false;
    }

    if (ok)
    {
        if (slot >= 0)
            deadline_state[slot].group_id = 0;   /* 释放，下次升主重新计时 */
        return true;
    }

    if (slot >= 0 &&
        TimestampDifferenceExceeds(deadline_state[slot].first_try, now,
                                   pg_raft_promote_catchup_deadline_ms))
    {
        elog(WARNING,
             "pg_raft: 组 %lld 升主前置超过 %d ms 仍未完成，按可用性优先放行上报"
             "（该副本可能尚未追平，升主后读到的可能是旧数据）",
             (long long) group_id, pg_raft_promote_catchup_deadline_ms);
        deadline_state[slot].group_id = 0;
        return true;
    }

    return false;
}

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
     * ★ 与惰性回放 promotion 路径合流（DTX_2PC_DESIGN.md §0.0 第 6 步 a）。
     *
     * 上报**之前**先把本节点该分片的物理回放追平到 Raft 已提交位点，并闭合
     * in-doubt 分布式事务。没做完就不上报 —— 控制面 apply 是"登记 partition_map
     * → 翻 pg_dist_placement"一气呵成的，不上报就等于不翻路由，
     * "追不平不对外服务"这条承诺由此天然成立，且完全不阻塞 group 0。
     */
    if (!data_group_promote_prepare(ctx->group_id))
        return;                 /* 还没追平/闭合完，保留 report_pending 下个 tick 继续 */

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

/*
 * pg_raft_consensus_apply_all_data — 遍历全部数据组做一次 apply drain。
 * 专供 topology monitor 的心跳 tick 调用（外层已包好事务）；与
 * group_apply_pending 的认领机制天然互斥，和 RPC backend 并发安全。
 */
void
pg_raft_consensus_apply_all_data(void)
{
    int i;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        return;

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];
        RaftGroupCtx    ctx;

        if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP)
            continue;

        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;
        group_apply_pending(&ctx);
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
        RaftLogEntry dropped;

        /*
         * ★ #39 留痕（§13 约束 13）：数据条目因多数派不足被丢弃的计数。
         * 8/3 起 leader 侧不再截断 parwal（字节留作孤儿、同 plsn 重新
         * propose，多数派恢复后自然收敛），丢弃不再直接等于无痕分叉，
         * 但丢弃频度仍是复制健康度的直接观测口，flow_stats 靠它。
         * entry_partition_lsn 纯字符串扫描，锁下可用。
         */
        if (log_get_entry_locked(ctx, idx, &dropped) &&
            strcmp(dropped.op_type, RAFT_OP_PARWAL) == 0)
        {
            ctx->log->quorum_drops++;
            ctx->log->last_drop_plsn = entry_partition_lsn(dropped.payload);
        }

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
     *
     * 环仍然满就**阻塞等**（背压），不再直接丢提案 —— 见 wait_for_log_room。
     */
    if (!wait_for_log_room(ctx))
    {
        int64       waits, drops, last_idx, commit_idx, applied;
        int         failr;
        int         owner;
        const char *failtxt;

        SpinLockAcquire(&ctx->log->mutex);
        ctx->log->ring_full_drops++;
        waits      = ctx->log->ring_full_waits;
        drops      = ctx->log->ring_full_drops;
        last_idx   = ctx->log->last_log_index;
        commit_idx = ctx->log->commit_index;
        applied    = ctx->log->last_applied;
        failr      = ctx->log->last_apply_fail;
        owner      = ctx->log->apply_owner_pid;
        SpinLockRelease(&ctx->log->mutex);

        switch (failr)
        {
            case RAFT_APPLYFAIL_NO_PART:  failtxt = "本节点没有该组对应的分片（P0 映射）"; break;
            case RAFT_APPLYFAIL_NO_SPI:   failtxt = "拿不到 SPI"; break;
            case RAFT_APPLYFAIL_SPI_EXEC: failtxt = "follower_set_applied_part_lsn 执行失败"; break;
            default:                      failtxt = "无（apply 未报失败，可能是被其他 backend 长时间占着）"; break;
        }

        ereport(WARNING,
                (errmsg("pg_raft: group %lld 日志环等待 %d ms 仍无空位，提案被丢弃"
                        "（累计 等待=%lld 丢弃=%lld）",
                        (long long) ctx->group_id, pg_raft_propose_wait_ms,
                        (long long) waits, (long long) drops),
                 errdetail("last_log=%lld commit=%lld applied=%lld；认领持有者 pid=%d；"
                           "apply 最近一次失败原因：%s",
                           (long long) last_idx, (long long) commit_idx,
                           (long long) applied, owner, failtxt),
                 errhint("数据组丢提案会让副本与 leader 分叉风险上升（字节留作孤儿等重推），"
                         "请查 partdist.pg_raft_group_flow_stats()。")));
        return 0;
    }

    SpinLockAcquire(&ctx->cons->mutex);
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    idx = log_append_locked(ctx, term, op_type, payload);
    SpinLockRelease(&ctx->log->mutex);

    if (idx <= 0)
        return 0;

    persist_log_entry_sql(ctx, idx, term, op_type, payload, false);
    raft_spi_ctx = true;
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
        raft_spi_ctx = false;
        discard_uncommitted_entry(ctx, idx);
        return 0;
    }

    /* 多数派提交后，继续推送到所有 Follower 再 apply（控制面需全节点一致） */
    mark_log_committed_sql(ctx, committed_upto);
    flush_replication(ctx, idx, term);
    raft_spi_ctx = false;
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

    /*
     * prev 一致性检查也要能看到环外条目（对端正在追平我这个落后很远的节点时，
     * prev 必然已滑出我的环窗口）。log_get_entry_ext 内部自己取/放锁，
     * 因此必须在进入下面的临界区之前做完。RPC 处理跑在真正的 client backend
     * 里，SPI 天然可用 —— 这一侧不受 BGW 无 SPI 的限制。
     */
    if (prev_idx > 0)
    {
        int64 my_prev_term;
        bool  prev_ok;

        raft_spi_ctx = true;
        prev_ok = (log_term_at(ctx, prev_idx, &my_prev_term) &&
                   my_prev_term == prev_term);
        raft_spi_ctx = false;

        if (!prev_ok)
            return true;
    }

    SpinLockAcquire(&ctx->log->mutex);

    if (has_entry)
    {
        bool already_present = false;

        if (entry_idx <= ctx->log->last_log_index)
        {
            RaftLogEntry *exist = log_slot(ctx, entry_idx);
            RaftLogEntry  old;
            bool          have_old = false;

            if (exist->index == entry_idx)
            {
                old = *exist;
                have_old = true;
            }
            else
            {
                /*
                 * 环外的老条目：leader 正在补发我早就持有的一段。此时不能按
                 * "槽位对不上"直接拒 —— 那正是落后超过环容量的节点永远追不上
                 * 的第二道闸（第一道在 leader 侧的 log_get_entry_sql 回读）。
                 * 也不能盲信：与 SQL 日志里的那一行逐字段比对再判定。
                 */
                SpinLockRelease(&ctx->log->mutex);
                raft_spi_ctx = true;
                have_old = log_get_entry_sql(ctx, entry_idx, &old);
                raft_spi_ctx = false;
                SpinLockAcquire(&ctx->log->mutex);

                if (!have_old)
                {
                    /* 环外且日志里也没有 = 空洞，拒绝，让 leader 继续回退 */
                    SpinLockRelease(&ctx->log->mutex);
                    return true;
                }
            }

            /*
             * 冲突判据只看 term —— 这既是 Raft 的原始规则（同 index 同 term 的
             * 条目必然出自同一个 leader、内容相同），也是这里**必须**这么写的
             * 现实原因：`partdist.raft_log.payload` 是 jsonb，读回来的文本被
             * 规范化过（键序、冒号后的空格），与 leader 线上发来的原始 JSON
             * 文本逐字节不等。按文本比就会把每一条环外条目都判成冲突并截断，
             * 实测把控制面日志从 347 条削到 35 条（2026-08-05 实测踩到）。
             * 同理，重启后 restore 灌回环里的也是规范化文本，环内比文本一样不可靠。
             */
            if (old.term != entry_term)
            {
                if (strcmp(old.op_type, RAFT_OP_PARWAL) == 0)
                    conflict_plsn = entry_partition_lsn(old.payload);
                log_truncate_after_locked(ctx, entry_idx - 1);
                /*
                 * SQL 行必须与环同时截断：有了环外回读之后，留在表里的旧行
                 * 会被重新读出来当作"本节点持有的条目"，重启 restore 也会把
                 * 它们灌回环。删除必须发生在下面的 append 之前。
                 */
                SpinLockRelease(&ctx->log->mutex);
                delete_log_entries_after_sql(ctx, entry_idx - 1);
                SpinLockAcquire(&ctx->log->mutex);
            }
            else
                already_present = true;
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
        else if (already_present)
        {
            /*
             * 已持有完全相同的这一条（环内或环外）——重传。不重复 append，
             * 但**继续往下走**：数据组还要确认字节确实在盘上（SQL 行先于
             * 字节写，崩溃可能只留下行），data_entry_store 幂等，落成功才 ack。
             */
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

PG_FUNCTION_INFO_V1(pg_raft_install_snapshot);

/*
 * InstallSnapshot 的接收侧（控制面专用，见 send_install_snapshot 的注释）。
 *
 * 处理顺序按 Raft 论文 §7：
 *   1. term 落后直接拒（只回自己的 term）；
 *   2. 认 leader、重置选举计时器（快照传输期间不该被误判为失联）；
 *   3. **若本地在 last_included_index 上恰好持有同 term 的条目**，保留其后的
 *      日志尾巴（对端只是想让我跳过前面那段）；否则整段日志作废 —— 那意味着
 *      我持有的是另一条历史；
 *   4. 用快照内容整体替换状态机（node_map / partition_map），并把路由层
 *      同步一遍；
 *   5. 基点、三个游标、hardstate 落定。
 *
 * 这一整套跑在 RPC 的 client backend 里，SPI 天然可用。
 */
Datum
pg_raft_install_snapshot(PG_FUNCTION_ARGS)
{
    int64        in_term = PG_GETARG_INT64(0);
    int          leader_id = PG_GETARG_INT32(1);
    int64        last_idx = PG_GETARG_INT64(2);
    int64        last_term = PG_GETARG_INT64(3);
    char        *node_map = text_to_cstring(PG_GETARG_TEXT_PP(4));
    char        *partition_map = text_to_cstring(PG_GETARG_TEXT_PP(5));
    RaftGroupCtx ctx;
    int64        my_term_at_idx = 0;
    bool         keep_tail;
    int64        out_term = 0;
    int          success = 0;
    char         out[64];

    if (RaftGroups == NULL || !pg_raft_raft_enabled)
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));

    parse_peers();
    if (!raft_control_ctx(&ctx))
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));

    restore_hard_state_if_needed(&ctx);
    restore_persistent_log_if_needed(&ctx);

    SpinLockAcquire(&ctx.cons->mutex);
    if (in_term > ctx.cons->current_term)
    {
        ctx.cons->current_term = in_term;
        ctx.cons->voted_for = 0;
    }
    out_term = ctx.cons->current_term;
    if (in_term < ctx.cons->current_term)
    {
        SpinLockRelease(&ctx.cons->mutex);
        snprintf(out, sizeof(out), "%lld 0", (long long) out_term);
        PG_RETURN_TEXT_P(cstring_to_text(out));
    }
    ctx.cons->state = RAFT_FOLLOWER;
    ctx.cons->leader_id = leader_id;
    reset_election_deadline_locked(&ctx);
    SpinLockRelease(&ctx.cons->mutex);

    /* 已经不比快照旧就什么都不用做（重发幂等） */
    SpinLockAcquire(&ctx.log->mutex);
    if (ctx.log->base_index >= last_idx)
    {
        SpinLockRelease(&ctx.log->mutex);
        snprintf(out, sizeof(out), "%lld 1", (long long) out_term);
        PG_RETURN_TEXT_P(cstring_to_text(out));
    }
    SpinLockRelease(&ctx.log->mutex);

    raft_spi_ctx = true;
    keep_tail = (log_term_at(&ctx, last_idx, &my_term_at_idx) &&
                 my_term_at_idx == last_term);
    raft_spi_ctx = false;

    if (!pg_raft_apply_snapshot_state(node_map, partition_map))
    {
        snprintf(out, sizeof(out), "%lld 0", (long long) out_term);
        PG_RETURN_TEXT_P(cstring_to_text(out));
    }

    /*
     * 日志行：保留尾巴时只删基点及之前；整段作废时连尾巴一起删 ——
     * 那段尾巴是另一条历史，留着会在 restore 时被灌回环。
     */
    if (!keep_tail)
        delete_log_entries_after_sql(&ctx, last_idx);
    {
        StringInfoData sql;
        bool           spi_owned;

        if (raft_persist_spi_begin(&spi_owned))
        {
            if (raft_log_table_ready())
            {
                initStringInfo(&sql);
                appendStringInfo(&sql,
                                 "DELETE FROM partdist.raft_log "
                                 "WHERE group_id = 0 AND log_index <= %lld",
                                 (long long) last_idx);
                (void) SPI_execute(sql.data, false, 0);
                pfree(sql.data);
            }
            raft_persist_spi_end(spi_owned);
        }
    }

    SpinLockAcquire(&ctx.log->mutex);
    ctx.log->base_index = last_idx;
    ctx.log->base_term = last_term;
    if (!keep_tail || ctx.log->last_log_index < last_idx)
        ctx.log->last_log_index = last_idx;
    if (ctx.log->commit_index < last_idx)
        ctx.log->commit_index = last_idx;
    /*
     * apply 游标**回拨**到 last_idx，不是只往上抬。状态机刚被整表替换成
     * "截至 last_idx 的那一份"，保留下来的尾巴（last_idx+1..）必须在它上面
     * 重放一遍才对得上；只抬不降的话，本地 last_applied 若已经越过 last_idx，
     * 那段尾巴就永远不会再 apply —— 状态机反而**倒退**成快照那一刻的样子。
     * 控制面的 apply 全是幂等 upsert（partition_map 还带任期栅栏），重放无害。
     */
    ctx.log->last_applied = last_idx;
    SpinLockRelease(&ctx.log->mutex);

    persist_hard_state_unlocked(&ctx);
    success = 1;

    elog(LOG, "pg_raft: 已安装组 0 快照 last_included=(%lld,%lld)，%s",
         (long long) last_idx, (long long) last_term,
         keep_tail ? "保留其后的日志尾巴" : "整段日志作废");

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
        Datum           values[10];
        bool            nulls[10];
        int             state;
        int64           term;
        int             leader_id;
        int64           last_idx;
        int64           commit_idx;
        int64           applied;
        int64           base_idx;
        int64           base_term;

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
        base_idx = ctx.log->base_index;
        base_term = ctx.log->base_term;
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
        values[8] = Int64GetDatum(base_idx);
        values[9] = Int64GetDatum(base_term);

        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    PG_RETURN_VOID();
}

/*
 * pg_raft_group_flow_stats — 每组的背压 / 丢弃计数（§13 约束 13）。
 *
 * **单独开一个函数而不是往 pg_raft_group_status() 加列**：改返回类型要 DROP +
 * CREATE，而这套 Citus 集群上动扩展对象的代价见 patches/README；新增函数是
 * 纯增量，老调用方一行不用改。
 *
 * 怎么读这几个数：
 *   ring_full_waits > 0   写入速度短暂超过 apply，已经靠背压吸收，正常。
 *   ring_full_drops > 0   背压等到超时仍无空位 ⇒ **有提案被丢弃**。
 *   quorum_drops    > 0   多数派不足导致条目被丢弃，且 leader 段里那条也被截掉。
 *
 * 后两个非零就意味着：该分区的副本**可能已与 leader 永久分叉**（leader 的物理
 * 变更不随事务回滚），需要重做物理基线。last_drop_plsn 给出最早的怀疑点。
 */
PG_FUNCTION_INFO_V1(pg_raft_group_flow_stats);
Datum
pg_raft_group_flow_stats(PG_FUNCTION_ARGS)
{
    ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc        tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext    per_query_ctx;
    MemoryContext    oldcontext;
    int              i;

    if (rsinfo == NULL || !(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR, (errmsg("pg_raft_group_flow_stats: 需要 materialize 模式")));
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errmsg("pg_raft_group_flow_stats: 返回类型必须是 record")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    MemoryContextSwitchTo(oldcontext);

    if (RaftGroups == NULL)
        PG_RETURN_VOID();

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];
        Datum           values[7];
        bool            nulls[7];
        int64           depth, waits, drops, qdrops, last_plsn;

        if (!g->in_use)
            continue;

        SpinLockAcquire(&g->log.mutex);
        depth     = g->log.last_log_index - g->log.last_applied;
        waits     = g->log.ring_full_waits;
        drops     = g->log.ring_full_drops;
        qdrops    = g->log.quorum_drops;
        last_plsn = g->log.last_drop_plsn;
        SpinLockRelease(&g->log.mutex);

        memset(nulls, 0, sizeof(nulls));
        values[0] = Int64GetDatum(g->group_id);
        values[1] = Int64GetDatum(depth);
        values[2] = Int32GetDatum(RAFT_LOG_CAPACITY);
        values[3] = Int64GetDatum(waits);
        values[4] = Int64GetDatum(drops);
        values[5] = Int64GetDatum(qdrops);
        values[6] = Int64GetDatum(last_plsn);

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
    int          flags = 0;
    int64        gxid = 0;
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
                     "SELECT orig_lsn::text, rmid, info, gxid, length(data), flags "
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
        gxid = isnull ? 0 : DatumGetInt64(d);
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5, &isnull);
        nbytes = isnull ? 0 : DatumGetInt32(d);
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 6, &isnull);
        flags = isnull ? PARTWAL_FLAG_DATA : DatumGetInt32(d);

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

    /*
     * 描述符里带的是 **gxid**（含来源节点号）与记录类别 flags：follower 落盘时
     * 必须原样写回头部。若只传 32 位本地 xid，同一节点上两个 leader 的副本在
     * 事务层就会撞号（FRD §9.1）。
     */
    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"partition_lsn\":%lld,\"orig_lsn\":\"%s\",\"rmid\":%d,"
                     "\"info\":%d,\"flags\":%d,\"gxid\":%lld,\"nbytes\":%lld}",
                     (long long) partition_lsn, orig_lsn, rmid, info, flags,
                     (long long) gxid, (long long) nbytes);

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
 * 后台追平通道（计划文档 §12.4 #5 的后半段）。
 *
 * 缺口：数据条目只在 client backend 的 propose 路径下发，环外条目也只有 SPI
 * 语境才读得回来。于是**没有写入流量时，落后的 follower 不会自行收敛** ——
 * 掉线重启的副本要等下一笔业务写入才被顺带补齐；一直没有写入就一直不补。
 * 落后超过环容量（128）时更糟：连下一笔写入也补不动，静默永久卡死。
 *
 * 本函数就是那条通道：由 TopologyMonitor 经 libpq 自连周期触发（与
 * force_probe / dtx_recover_prepared 同一手法），因而跑在**真正的 client
 * backend** 里 —— SPI 可用，既能读 parwal 字节，也能回读环外条目。
 *
 * 语义边界（务必保持）：
 *   - 只补发**已存在**的条目，不产生新提案，不改变"多数派才提交"的语义；
 *     commit_index 仍由 compute_new_commit_index 按多数派 match 推进。
 *   - 只在本节点是该组 leader 时做事；非 leader 组直接跳过。
 *   - 与 prepare 路径共用复制认领位，但**取不到就跳过**（非阻塞）：追平是
 *     尽力而为的后台工作，绝不能去和事务提交路径抢锁、更不能让它等待。
 *   - 每组每轮的补发条数有上限，避免一次调用把 tick 拖得过久。
 */
#define RAFT_CATCHUP_MAX_ROUNDS  256

PG_FUNCTION_INFO_V1(pg_raft_catchup);

Datum
pg_raft_catchup(PG_FUNCTION_ARGS)
{
    int   i;
    int64 shipped = 0;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_INT64(0);

    parse_peers();
    if (n_peers == 0)
        PG_RETURN_INT64(0);

    restore_groups_if_needed();

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];
        RaftGroupCtx    ctx;
        int             state;
        int             round;

        if (!g->in_use)
            continue;

        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;

        SpinLockAcquire(&ctx.cons->mutex);
        state = ctx.cons->state;
        SpinLockRelease(&ctx.cons->mutex);
        if (state != RAFT_LEADER)
            continue;

        if (!group_membership_known(&ctx))
            continue;

        if (!replicate_try_claim(&ctx))
            continue;

        raft_spi_ctx = true;
        PG_TRY();
        {
            int p0;

            restore_persistent_log_if_needed(&ctx);

            /* 先按对端自报的 last_log_index 给 next_index 一个起点（只降不升） */
            for (p0 = 0; p0 < n_peers; p0++)
            {
                int64 hint;
                int64 cur_next;

                if (!peer_in_group(&ctx, p0))
                    continue;

                SpinLockAcquire(&ctx.log->mutex);
                cur_next = ctx.log->peer_next_index[p0];
                SpinLockRelease(&ctx.log->mutex);

                if (cur_next <= 1)
                    continue;

                hint = peer_last_log_index(&ctx, &peers[p0]);
                if (hint < 0)
                    continue;

                SpinLockAcquire(&ctx.log->mutex);
                if (ctx.log->peer_next_index[p0] > hint + 1)
                    ctx.log->peer_next_index[p0] = hint + 1;
                SpinLockRelease(&ctx.log->mutex);
            }

            for (round = 0; round < RAFT_CATCHUP_MAX_ROUNDS; round++)
            {
                int64 last_idx;
                bool  any_behind = false;
                bool  any_change = false;
                int   p;

                CHECK_FOR_INTERRUPTS();

                SpinLockAcquire(&ctx.log->mutex);
                last_idx = ctx.log->last_log_index;
                SpinLockRelease(&ctx.log->mutex);

                for (p = 0; p < n_peers; p++)
                {
                    int64 before;
                    int64 after;
                    int64 next_before;
                    int64 next_after;

                    if (!peer_in_group(&ctx, p))
                        continue;

                    SpinLockAcquire(&ctx.log->mutex);
                    before = ctx.log->peer_match_index[p];
                    next_before = ctx.log->peer_next_index[p];
                    SpinLockRelease(&ctx.log->mutex);

                    if (before >= last_idx)
                        continue;

                    any_behind = true;

                    /* 不可达对端在退避窗口里，send_sql_rpc 自己会跳过 */
                    replicate_to_peer(&ctx, p);

                    SpinLockAcquire(&ctx.log->mutex);
                    after = ctx.log->peer_match_index[p];
                    next_after = ctx.log->peer_next_index[p];
                    SpinLockRelease(&ctx.log->mutex);

                    if (after > before)
                        shipped += after - before;
                    /*
                     * next_index **下探**同样算进展：新当选的 leader 会把所有
                     * peer 的 next_index 初始化成 last+1，落后很远的成员要先被
                     * 一步步探回到它真正持有的位置（本实现的 AppendEntries 响应
                     * 不带 conflict hint），这一段一条都发不出去。若把"没发出
                     * 条目"当成没进展而收手，一次调用只能下探一格，追平会慢到
                     * 看起来像没生效 —— 首版实测就是这样卡在 0。
                     */
                    if (after != before || next_after != next_before)
                        any_change = true;
                }

                /* 全都追平了，或者这一轮什么都没动（对端不可达）：收手 */
                if (!any_behind || !any_change)
                    break;
            }

            /* 补齐后重算提交点：落后成员归队可能让更老的条目刚刚够多数派 */
            leader_replicate_and_commit(&ctx);
            group_apply_pending(&ctx);
        }
        PG_FINALLY();
        {
            raft_spi_ctx = false;
            replicate_release(&ctx);
        }
        PG_END_TRY();
    }

    PG_RETURN_INT64(shipped);
}

/*
 * 本节点是否是**任何一个**组的 leader（纯 shmem 读，BGW 可调）。
 * TopologyMonitor 用它决定要不要为追平通道开一条自连接。
 */
bool
pg_raft_any_group_leader_local(void)
{
    int i;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        return false;

    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];
        int             state;

        if (!g->in_use)
            continue;

        SpinLockAcquire(&g->cons.mutex);
        state = g->cons.state;
        SpinLockRelease(&g->cons.mutex);

        if (state == RAFT_LEADER)
            return true;
    }
    return false;
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

/*
 * 非阻塞版认领：取不到就返回 false。后台追平通道用它 —— 追平是尽力而为的
 * 后台工作，绝不能排队等在事务提交路径（prepare 复制）后面。
 */
static bool
replicate_try_claim(RaftGroupCtx *ctx)
{
    bool got = false;

    SpinLockAcquire(&RaftGroups->mutex);
    if (!ctx->g->replicate_in_progress)
    {
        ctx->g->replicate_in_progress = true;
        ctx->g->replicate_pid = MyProcPid;
        got = true;
    }
    SpinLockRelease(&RaftGroups->mutex);

    return got;
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
 * 把本组的 parwal 增量复制到多数派，直到（含）cur_plsn。
 *
 * 调用方必须已持有本组的复制认领位（replicate_claim）。任何一条未达多数派
 * 即 ERROR —— 对 prepare 路径是"事务中止"，对决议路径是"决议未成立"。
 *
 * 增量下界必须在**进入临界区之后**读取：等待认领位期间，并发 backend 很可能
 * 已经把这一段复制完了，此时循环空转即返回。
 *
 * 为什么决议记录也必须走"整段增量"而不是只 propose 自己那一条：follower 的
 * AppendPartWALRecordAt 遇到 plsn 空洞会 ERROR（不留洞是物理回放的前提），
 * 单独 propose 决议那一条会因为前面缺记录而拿不到 ack。
 */
static void
replicate_group_upto(RaftGroupCtx *ctx, int64 cur_plsn, Oid partition_id)
{
    int64 last;
    int64 plsn;

    last = ctx->g->last_data_plsn;
    if (last == 0)
    {
        int64 p;

        /* 重启后运行期游标为 0：从环内最后一条 OP_PARWAL 回推 */
        SpinLockAcquire(&ctx->log->mutex);
        for (p = ctx->log->last_log_index; p > 0 &&
             p > ctx->log->last_log_index - RAFT_LOG_CAPACITY; p--)
        {
            RaftLogEntry e;

            if (log_get_entry_locked(ctx, p, &e) &&
                strcmp(e.op_type, RAFT_OP_PARWAL) == 0)
            {
                last = entry_partition_lsn(e.payload);
                break;
            }
        }
        SpinLockRelease(&ctx->log->mutex);
        if (last > 0)
            ctx->g->last_data_plsn = last;
    }

    for (plsn = last + 1; plsn <= cur_plsn; plsn++)
    {
        /*
         * group_propose 只在拿到多数派 ack 之后才返回 idx > 0，而 follower
         * 是**先 fsync 再 ack** 的（运输层加固 §11.5.1 #1/#3）——所以
         * "返回成功" 严格等价于 "该条目已在多数派持久化"。
         * prepare 路径靠它得到 prepared 语义；决议路径靠它得到**提交点**。
         */
        int64 idx = data_propose_one(ctx, plsn);

        if (idx <= 0)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: 分区 %u(组 %lld) record %lld 未达多数派",
                            partition_id, (long long) ctx->group_id,
                            (long long) plsn)));
    }
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
    in_txn_replication = true;
    PG_TRY();
    {
        /*
         * 增量下界必须在**进入临界区之后**重新读取：等待期间并发 backend
         * 很可能已经把这一段（含本事务的记录）复制完了，此时循环空转即返回。
         */
        replicate_group_upto(&ctx, cur, partition_id);
    }
    PG_FINALLY();
    {
        in_txn_replication = false;
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

/* ================================================================== */
/* DTX-2PC 决议层（DTX_2PC_DESIGN.md §6）                              */
/* ================================================================== */

/*
 * dtx_write_decision — 在协调组写一条 DECISION 记录并复制到多数派。
 *
 * **这是全局提交点**：只有 replicate_group_upto 返回（= 该记录已在协调组的
 * 多数派 fsync 落盘）之后，事务才算正式提交（用户方案阶段 2 第 4 步）。
 * 任何一步失败都 ERROR —— 决议不成立，调用方不得向客户端返回成功。
 *
 * 调用方必须已确认本节点是协调组 leader，并已持有复制认领位。
 * 返回该 DECISION 记录的 partition_lsn。
 */
static int64
dtx_write_decision(RaftGroupCtx *ctx, int64 local_oid, int64 dtxid,
                   int32 verdict, uint64 commit_ts,
                   const char *participants_sql)
{
    StringInfoData sql;
    bool  spi_owned;
    bool  isnull;
    int64 plsn = 0;

    if (!raft_persist_spi_begin(&spi_owned))
        ereport(ERROR, (errmsg("pg_raft: dtx 决议需要 SPI")));

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partdist.partwal_append_dtx_record("
                     "%u::oid, 2, %lld::bigint, %lld::bigint, %llu::bigint, %d, %s)",
                     (unsigned) local_oid, (long long) dtxid,
                     (long long) ctx->group_id, (unsigned long long) commit_ts,
                     verdict, participants_sql);
    if (SPI_execute(sql.data, false, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);
        if (!isnull)
            plsn = DatumGetInt64(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (plsn <= 0)
        ereport(ERROR,
                (errmsg("pg_raft: 组 %lld 写 DECISION 记录失败（dtxid=%lld）",
                        (long long) ctx->group_id, (long long) dtxid)));

    /* ★ 提交点：这一步返回即"决议已在协调组多数派持久化" */
    replicate_group_upto(ctx, plsn, (Oid) local_oid);

    /*
     * 本地索引。其余成员由 data_entry_apply 在 apply 该条目时各自写入，
     * 因此协调组切主后新 leader 手里天然有全表（§6.2）。
     */
    if (raft_persist_spi_begin(&spi_owned))
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "INSERT INTO partdist.dtx_decision"
                         "(dtxid, coord_gsid, verdict, commit_ts, participants, decided_plsn) "
                         "VALUES (%lld, %lld, %d, %llu, %s, %lld) "
                         "ON CONFLICT (dtxid) DO NOTHING",
                         (long long) dtxid, (long long) ctx->group_id, verdict,
                         (unsigned long long) commit_ts, participants_sql,
                         (long long) plsn);
        (void) SPI_execute(sql.data, false, 0);
        pfree(sql.data);
        raft_persist_spi_end(spi_owned);
    }
    return plsn;
}

/* 查已有决议；返回 verdict，0 = 尚无决议 */
static int32
dtx_lookup_decision(int64 dtxid)
{
    StringInfoData sql;
    bool  spi_owned;
    bool  isnull;
    int32 verdict = 0;

    if (!raft_persist_spi_begin(&spi_owned))
        return 0;
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT verdict FROM partdist.dtx_decision WHERE dtxid = %lld",
                     (long long) dtxid);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);
        if (!isnull)
            verdict = DatumGetInt16(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return verdict;
}

/*
 * 协调组的公共前置：解析组、确认成员集已知、确认本节点是 leader。
 * 不是 leader 返回 false（调用方应返回 NULL，由上层按 partition_map 重新寻址）。
 */
static bool
dtx_coord_ctx(int64 coord_gsid, RaftGroupCtx *ctx, int64 *local_oid)
{
    int state;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        return false;
    parse_peers();
    restore_groups_if_needed();

    if (!raft_group_ctx(coord_gsid, ctx))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 协调组 %lld 在本节点不存在",
                        (long long) coord_gsid)));
    if (!group_resolve_membership(ctx))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 协调组 %lld 的成员集未知，无法做决议",
                        (long long) coord_gsid)));

    restore_hard_state_if_needed(ctx);
    restore_persistent_log_if_needed(ctx);

    SpinLockAcquire(&ctx->cons->mutex);
    state = ctx->cons->state;
    SpinLockRelease(&ctx->cons->mutex);
    if (state != RAFT_LEADER)
        return false;

    *local_oid = group_local_partition(ctx);
    if (*local_oid <= 0)
        ereport(ERROR,
                (errmsg("pg_raft: 协调组 %lld 在本节点没有对应分片",
                        (long long) coord_gsid)));
    return true;
}

/* 把 int8[] 渲染成可嵌进 SQL 的字面量；NULL/空 → '{}'::bigint[] */
static char *
dtx_participants_sql(ArrayType *arr)
{
    StringInfoData buf;
    Datum *elems;
    bool  *nulls;
    int    n = 0;
    int    i;
    bool   first = true;

    initStringInfo(&buf);
    appendStringInfoString(&buf, "ARRAY[");
    if (arr != NULL)
    {
        deconstruct_array(arr, INT8OID, 8, true, 'd', &elems, &nulls, &n);
        for (i = 0; i < n; i++)
        {
            if (nulls[i])
                continue;
            if (!first)
                appendStringInfoChar(&buf, ',');
            appendStringInfo(&buf, "%lld", (long long) DatumGetInt64(elems[i]));
            first = false;
        }
    }
    appendStringInfoString(&buf, "]::bigint[]");
    return buf.data;
}

/*
 * partdist.dtx_decide(coord_gsid, dtxid, verdict, participants[]) → int
 *
 * 在协调组 leader 上执行。返回最终生效的 verdict（1=COMMIT 2=ABORT）；
 * 本节点不是协调组 leader 时返回 NULL —— 调用方据此按 partition_map 重新寻址。
 *
 * **决议槽一次性**：若该 dtxid 已有决议（例如恢复守护抢先写了 ABORT），
 * 直接返回已有的那个，不覆盖。这是推定中止与正常提交路径并发时的收敛点。
 */
PG_FUNCTION_INFO_V1(pg_raft_dtx_decide);

Datum
pg_raft_dtx_decide(PG_FUNCTION_ARGS)
{
    int64        coord_gsid = PG_GETARG_INT64(0);
    int64        dtxid = PG_GETARG_INT64(1);
    int32        verdict = PG_GETARG_INT32(2);
    ArrayType   *parts = PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3);
    int64        p_commit_ts = (PG_NARGS() > 4 && !PG_ARGISNULL(4))
                               ? PG_GETARG_INT64(4) : 0;
    RaftGroupCtx ctx;
    int64        local_oid = 0;
    int32        existing;
    char        *parts_sql;
    uint64       commit_ts;

    if (verdict != 1 && verdict != 2)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("pg_raft: verdict 必须是 1(COMMIT) 或 2(ABORT)")));

    if (!dtx_coord_ctx(coord_gsid, &ctx, &local_oid))
        PG_RETURN_NULL();

    existing = dtx_lookup_decision(dtxid);
    if (existing != 0)
        PG_RETURN_INT32(existing);

    /*
     * T4.4 换源：驱动节点在"全部票齐"点从 TSO 取的 commit_ts 经参数传入
     * （p_commit_ts > 0），决议记录原子携带之 —— 多数派落盘即提交点（§2-6）。
     * 未配置 TSO 的遗留路径 p_commit_ts = 0：沿用协调者本地时钟（§9.8），
     * 行为与换源前逐字节一致。决议的原子性来自 Raft 多数派，不依赖时间戳。
     */
    commit_ts = (verdict == 1)
        ? (p_commit_ts > 0 ? (uint64) p_commit_ts : (uint64) GetCurrentTimestamp())
        : 0;
    parts_sql = dtx_participants_sql(parts);

    replicate_claim(&ctx);
    in_txn_replication = true;
    PG_TRY();
    {
        /* 进入临界区后再查一次：等待期间可能已被恢复守护写了决议 */
        existing = dtx_lookup_decision(dtxid);
        if (existing == 0)
            (void) dtx_write_decision(&ctx, local_oid, dtxid, verdict,
                                      commit_ts, parts_sql);
        else
            verdict = existing;
    }
    PG_FINALLY();
    {
        in_txn_replication = false;
        replicate_release(&ctx);
    }
    PG_END_TRY();

    pfree(parts_sql);
    PG_RETURN_INT32(verdict);
}

/*
 * partdist.dtx_status(coord_gsid, dtxid) → int
 *
 * 参与者恢复时查询决议。**推定中止**（§2.2）：查无决议时**先写一条
 * ABORT DECISION 并达多数派**，再返回 2。
 *
 * 这一步不能省 —— 否则"问的时候没有、答完之后原提交路径又把 COMMIT 写进去"
 * 会让同一事务出现两个互相矛盾的结论。先写后答之后，决议槽已被 ABORT 占住，
 * 后到的 COMMIT 会被 dtx_decide 的一次性检查挡下。
 *
 * 本节点不是协调组 leader 时返回 NULL。
 */
PG_FUNCTION_INFO_V1(pg_raft_dtx_status);

Datum
pg_raft_dtx_status(PG_FUNCTION_ARGS)
{
    int64        coord_gsid = PG_GETARG_INT64(0);
    int64        dtxid = PG_GETARG_INT64(1);
    RaftGroupCtx ctx;
    int64        local_oid = 0;
    int32        existing;
    char        *parts_sql;

    if (!dtx_coord_ctx(coord_gsid, &ctx, &local_oid))
        PG_RETURN_NULL();

    existing = dtx_lookup_decision(dtxid);
    if (existing != 0)
        PG_RETURN_INT32(existing);

    parts_sql = dtx_participants_sql(NULL);
    replicate_claim(&ctx);
    in_txn_replication = true;
    PG_TRY();
    {
        existing = dtx_lookup_decision(dtxid);
        if (existing == 0)
        {
            (void) dtx_write_decision(&ctx, local_oid, dtxid, 2 /* ABORT */,
                                      0, parts_sql);
            existing = 2;
        }
    }
    PG_FINALLY();
    {
        in_txn_replication = false;
        replicate_release(&ctx);
    }
    PG_END_TRY();

    pfree(parts_sql);
    PG_RETURN_INT32(existing);
}

/* ================================================================== */
/* DTX-2PC 恢复守护（参与者侧，DTX_2PC_DESIGN.md §7）                  */
/* ================================================================== */

/*
 * gid 编码：shardpg_dtx_<dtxid>_<coord_gsid>（§5.4）
 *
 * 为什么把 dtx 信息编进 gid：参与者**崩溃重启后**唯一还在的线索就是
 * pg_prepared_xacts —— 它是 PG 原生持久化的。任何放在本地表里的映射都可能
 * 与 prepared 事务不同步，而 gid 与 prepared 事务同生共死。
 */
static bool
dtx_parse_gid(const char *gid, int64 *dtxid, int64 *coord_gsid)
{
    long long d = 0, c = 0;

    if (gid == NULL)
        return false;
    if (sscanf(gid, "shardpg_dtx_%lld_%lld", &d, &c) != 2)
        return false;
    if (d <= 0 || c <= 0)
        return false;
    *dtxid = (int64) d;
    *coord_gsid = (int64) c;
    return true;
}

/*
 * 向协调组现任 leader 问决议。
 *
 * 寻址走 partdist.partition_map[coord_gsid].primary_node —— 切主重构（§13）
 * 保证它随数据组自治选举实时更新，因此**协调者宕机后寻址自动指向新 leader**，
 * 这正是决议放数据组、而不是放某个固定 worker 的收益。
 *
 * 返回 1=COMMIT / 2=ABORT / 0=问不到（对端不是 leader、连不上、元数据没追平），
 * 0 时调用方保持 prepared 不动，下轮再问 —— 绝不擅自决定。
 */
static int
dtx_ask_coordinator(int64 coord_gsid, int64 dtxid)
{
    StringInfoData sql;
    bool      spi_owned;
    bool      isnull;
    int       coord_node = 0;
    int       slot = -1;
    int       i;
    int       verdict = 0;
    char      conninfo[256];
    char      qry[192];
    PGconn   *conn;
    PGresult *res;

    /* 1) 查协调组的现任 primary 节点 */
    if (!raft_persist_spi_begin(&spi_owned))
        return 0;
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT primary_node FROM partdist.partition_map "
                     " WHERE partition_id = %llu::oid",
                     (unsigned long long) coord_gsid);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);
        if (!isnull)
            coord_node = DatumGetInt32(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (coord_node <= 0)
        return 0;               /* 元数据还没追平：下轮再问 */

    /* 本节点就是协调者：直接本地调，省一次往返 */
    if (coord_node == pg_raft_node_id)
    {
        if (!raft_persist_spi_begin(&spi_owned))
            return 0;
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "SELECT partdist.dtx_status(%lld, %lld)",
                         (long long) coord_gsid, (long long) dtxid);
        if (SPI_execute(sql.data, false, 1) == SPI_OK_SELECT && SPI_processed > 0)
        {
            Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                    SPI_tuptable->tupdesc, 1, &isnull);
            if (!isnull)
                verdict = DatumGetInt32(d);
        }
        pfree(sql.data);
        raft_persist_spi_end(spi_owned);
        return verdict;
    }

    /* 2) 远端：走 libpq */
    for (i = 0; i < n_peers; i++)
        if (peers[i].node_id == coord_node)
        {
            slot = i;
            break;
        }
    if (slot < 0)
        return 0;

    pg_raft_format_conninfo(peers[slot].host, peers[slot].port,
                            conninfo, sizeof(conninfo));
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        PQfinish(conn);
        return 0;               /* 协调者不可达：保持 prepared，下轮再问 */
    }
    snprintf(qry, sizeof(qry), "SELECT partdist.dtx_status(%lld, %lld)",
             (long long) coord_gsid, (long long) dtxid);
    res = PQexec(conn, qry);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
        !PQgetisnull(res, 0, 0))
        verdict = atoi(PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);
    return verdict;
}

/*
 * 未走本项目 2PC 的 prepared 事务（快路径 / 写集 ≤ 1 组 / 无纳管分片）按
 * **Citus 原生规则**闭合：提交点是 master 的本地提交，凭据是
 * pg_dist_transaction 里有没有该 gid 的**已提交**行。
 *
 * 为什么必须由我们来做：§9.4 关掉了 Citus 自己的 2PC 恢复（它会把我们决议为
 * ABORT 的事务无条件 COMMIT，造成分叉），关掉之后这一类事务就没人收尾了。
 * 两条规则各管各的：coord_gsid 有值 ⇒ 协调组权威；为空 ⇒ Citus 规则。
 *
 * 推定中止的时间窗与 Citus 自己的恢复同源：master 还在跑这笔事务时行尚未提交，
 * 但那时 prepared 事务的年龄也还没到超时。超时设保守即可，不影响正确性边界。
 *
 * 返回 1=COMMIT / 2=ABORT / 0=问不到（master 不可达，保持 prepared）。
 */
static int
dtx_ask_citus_coordinator(const char *gid)
{
    int       slot = -1;
    int       i;
    int       verdict = 0;
    char      conninfo[256];
    PGconn   *conn;
    PGresult *res;
    char     *quoted;
    StringInfoData qry;

    if (pg_raft_coordinator_node_id <= 0)
        return 0;

    for (i = 0; i < n_peers; i++)
        if (peers[i].node_id == pg_raft_coordinator_node_id)
        {
            slot = i;
            break;
        }
    if (slot < 0)
        return 0;

    pg_raft_format_conninfo(peers[slot].host, peers[slot].port,
                            conninfo, sizeof(conninfo));
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        PQfinish(conn);
        return 0;
    }

    /*
     * ★ initiator 存活栅栏（2026-08-04 审查补上）。
     *
     * pg_dist_transaction 的行在 master **本地提交之前不可见**。若 master 只是
     * 慢（发起 backend 还活着、尚未走到本地提交），凭"行不可见"就推定中止，
     * 会把一笔 master 随后会成功提交的事务在参与者上回滚 —— 分叉提交。
     * Citus 自己的恢复靠共享内存里的活跃分布式事务号拦这个窗口；我们按 §9.4
     * 把它关掉了，就必须自己补等价物：gid 里恰好编着发起 backend 的 pid
     * （citus_<group>_<pid>_<txn>_<conn>），它还在 master 的 pg_stat_activity
     * 里就先不动，下轮再看。pid 复用只会造成多等一轮，是保守方向。
     *
     * ★ 顺序敏感：必须**先**查 pid、**后**查行。pid 已消失意味着
     * "若它曾提交，提交必先于退出"，随后的行查询必然看得见该提交；
     * 反过来先查行再查 pid，就存在"查行时未提交、查 pid 前刚提交并退出"
     * 的窗口 —— 行不可见 + pid 不在 ⇒ 误判 ABORT，丢一笔已提交事务。
     */
    {
        long long g_group = 0, g_pid = 0, g_txn = 0, g_conn = 0;

        if (sscanf(gid, "citus_%lld_%lld_%lld_%lld",
                   &g_group, &g_pid, &g_txn, &g_conn) == 4 && g_pid > 0)
        {
            char alive_qry[128];
            bool alive = false;

            snprintf(alive_qry, sizeof(alive_qry),
                     "SELECT count(*) FROM pg_stat_activity WHERE pid = %lld",
                     g_pid);
            res = PQexec(conn, alive_qry);
            if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
                !PQgetisnull(res, 0, 0))
                alive = (atoi(PQgetvalue(res, 0, 0)) > 0);
            else
                alive = true;   /* 查不动就当活着：保守方向，下轮再来 */
            PQclear(res);

            if (alive)
            {
                PQfinish(conn);
                return 0;
            }
        }
    }

    quoted = PQescapeLiteral(conn, gid, strlen(gid));
    if (quoted == NULL)
    {
        PQfinish(conn);
        return 0;
    }
    initStringInfo(&qry);
    appendStringInfo(&qry,
                     "SELECT count(*) FROM pg_catalog.pg_dist_transaction WHERE gid = %s",
                     quoted);
    PQfreemem(quoted);

    res = PQexec(conn, qry.data);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
        !PQgetisnull(res, 0, 0))
        verdict = (atoi(PQgetvalue(res, 0, 0)) > 0) ? 1 : 2;
    PQclear(res);
    pfree(qry.data);
    PQfinish(conn);
    return verdict;
}

/*
 * 按 gid 取本节点的参与登记：coord_gsid（0 = master 未下发过）与本节点在这笔
 * 事务里写过的分区组。返回 false 表示查无此登记（legacy gid / 该表不存在）。
 *
 * ★ coord_gsid 为 NULL 的语义是推定中止的支点：master 严格"先下发协调组、
 * 后做决议"，所以查到 NULL 就意味着**决议必然还没做过**。
 */
static bool
dtx_lookup_participant(const char *gid, int64 *dtxid, int64 *coord_gsid,
                       int64 *gsids, int max_gsids, int *ngsids)
{
    StringInfoData sql;
    bool           found = false;
    char          *quoted;

    *dtxid = 0;
    *coord_gsid = 0;
    *ngsids = 0;

    quoted = quote_literal_cstr(gid);
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT dtxid, coalesce(coord_gsid, 0), gsids "
                     "  FROM partdist.dtx_participant WHERE gid = %s", quoted);
    pfree(quoted);

    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool   isnull;
        Datum  d;

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
        {
            *dtxid = DatumGetInt64(d);
            found = true;
        }
        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
        if (!isnull)
            *coord_gsid = DatumGetInt64(d);

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull);
        if (!isnull)
        {
            ArrayType *arr = DatumGetArrayTypeP(d);
            Datum     *elems;
            bool      *nulls;
            int        n = 0;
            int        i;

            deconstruct_array(arr, INT8OID, 8, true, 'd', &elems, &nulls, &n);
            for (i = 0; i < n && *ngsids < max_gsids; i++)
                if (!nulls[i])
                    gsids[(*ngsids)++] = DatumGetInt64(elems[i]);
        }
    }
    pfree(sql.data);
    return found;
}

/*
 * partdist.dtx_recover_prepared(timeout_ms) → int
 *
 * 参与者侧的恢复守护（§7）：扫描本节点上**超时未闭合**的 prepared 事务，
 * 向协调组问决议并据此 COMMIT/ROLLBACK PREPARED，同时在该分区的 parwal 流里
 * 补写 DTX_COMMIT / DTX_ABORT 标记。返回本轮处理掉的事务数。
 *
 * 超时只影响"多久开始问"，不影响正确性：与正常路径（master 驱动的阶段 3）
 * 并发执行也安全——COMMIT PREPARED 对同一 gid 只会成功一次，标记记录按
 * dtxid 幂等。设保守一点是为了别和正常路径抢答。
 *
 * 问不到决议（协调者不可达/正在选举/元数据没追平）时**保持 prepared 不动**，
 * 绝不擅自决定——推定中止的权力只在协调组手里（由它先写 ABORT 决议达多数派）。
 */
PG_FUNCTION_INFO_V1(pg_raft_dtx_recover_prepared);

Datum
pg_raft_dtx_recover_prepared(PG_FUNCTION_ARGS)
{
    int32          timeout_ms = PG_ARGISNULL(0) ? 30000 : PG_GETARG_INT32(0);
    StringInfoData sql;
    bool           spi_owned;
    int            handled = 0;
    int            ntodo = 0;
    int            i;
    char         **gids = NULL;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_INT32(0);
    parse_peers();

    /* 1) 取出待处理的 gid 列表（先取完再逐个处理：处理会改 pg_prepared_xacts） */
    if (!raft_persist_spi_begin(&spi_owned))
        PG_RETURN_INT32(0);
    initStringInfo(&sql);
    /*
     * 用 strpos 而不是 LIKE：gid 前缀里含 '_'（LIKE 的单字符通配符），
     * 而 '%' 又要在 appendStringInfo 里转义，两层转义极易写错 ——
     * 初版写成 LIKE 'shardpg_dtx=%%' ESCAPE '='，转义后是字面量 "shardpg_dtx%"，
     * **永远匹配不到任何 gid**，恢复守护静默空转。
     */
    appendStringInfo(&sql,
                     "SELECT gid FROM pg_prepared_xacts "
                     " WHERE (strpos(gid, 'shardpg_dtx_') = 1 OR strpos(gid, 'citus_') = 1) "
                     "   AND prepared < now() - interval '%d milliseconds' "
                     " ORDER BY prepared LIMIT 64",
                     timeout_ms);
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        MemoryContext old = MemoryContextSwitchTo(CurTransactionContext);

        ntodo = (int) SPI_processed;
        gids = (char **) palloc(sizeof(char *) * ntodo);
        for (i = 0; i < ntodo; i++)
            gids[i] = SPI_getvalue(SPI_tuptable->vals[i],
                                   SPI_tuptable->tupdesc, 1);
        MemoryContextSwitchTo(old);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    /* 2) 逐个问决议并闭合 */
    for (i = 0; i < ntodo; i++)
    {
        int64 dtxid = 0;
        int64 coord_gsid = 0;
        int64 mygsids[64];
        int   nmygsids = 0;
        int   verdict;
        int   k;

        /*
         * 决议的寻址依据，按优先级：
         *   a) partdist.dtx_participant（正常路径，Citus gid 走这里）——
         *      coord_gsid 有值 ⇒ 协调组权威；为 NULL ⇒ 决议必然未做过，
         *      按 Citus 原生规则闭合（快路径 / 无纳管分片的事务）；
         *   b) gid 自带的 shardpg_dtx_<dtxid>_<coord>（机制测试用的自造 gid）。
         */
        if (!raft_persist_spi_begin(&spi_owned))
            break;
        (void) dtx_lookup_participant(gids[i], &dtxid, &coord_gsid,
                                      mygsids, lengthof(mygsids), &nmygsids);
        raft_persist_spi_end(spi_owned);

        /*
         * ★ 协调组的来源有两个，**gid 里编的那个优先**（§5.4）：
         * 登记行里 coord_gsid 为 NULL 只说明"master 没下发过"，
         * 而 shardpg_dtx_<dtxid>_<coord> 这种 gid 本身就带着协调组。
         * 初版写成"登记行查到了就不再看 gid"，于是自造 gid 的事务被当成
         * 未走 2PC、按 Citus 规则去问 pg_dist_transaction（那里当然没有），
         * 结果**该提交的事务被推定中止回滚**（raft_21 A 段抓到）。
         * 真正的 Citus gid 解析不出协调组，coord_gsid 仍为 0，照旧走 Citus 规则。
         */
        if (coord_gsid <= 0)
        {
            int64 g_dtxid = 0;
            int64 g_coord = 0;

            if (dtx_parse_gid(gids[i], &g_dtxid, &g_coord))
            {
                if (dtxid == 0)
                    dtxid = g_dtxid;
                coord_gsid = g_coord;
            }
        }
        /*
         * ★ 登记缺失的 citus gid 也必须能闭合（2026-08-04 审查补上）。
         * 走到这里的 gid 只可能是 citus_% / shardpg_dtx_%（扫描已过滤）。
         * citus gid 查无登记的来源：该 prepared 事务产生时接线是关的
         * （dtx_2pc_enabled=off），或历史版本的异步登记随崩溃丢失。
         * 原实现在这里直接 continue —— 这类事务从此**没有任何人**收尾
         * （Citus 自己的恢复已被 §9.4 关掉），prepared 事务连同行锁永久滞留。
         * 处置：从 citus gid 解析出 dtxid（只为日志可读；verdict 本身按 gid
         * 查 pg_dist_transaction，不需要 dtxid），按 Citus 规则闭合；
         * 无登记 ⇒ 无 gsids ⇒ 不补标记。
         */
        if (dtxid == 0 && !dtx_dtxid_from_gid(gids[i], &dtxid))
            continue;           /* 不认识的 gid：完全不碰 */

        if (coord_gsid > 0)
            verdict = dtx_ask_coordinator(coord_gsid, dtxid);
        else
            verdict = dtx_ask_citus_coordinator(gids[i]);

        if (verdict != 1 && verdict != 2)
            continue;           /* 问不到：保持 prepared，下轮再来 */

        if (!raft_persist_spi_begin(&spi_owned))
            break;

        /*
         * 标记要补在**本节点自己写过的**分区组上，不是协调组上 ——
         * 一个参与者通常并不承载协调组的分片。没有登记（legacy gid）时退回
         * 协调组，保持旧行为。
         */
        if (nmygsids == 0 && coord_gsid > 0)
            mygsids[nmygsids++] = coord_gsid;

        /*
         * ★ COMMIT/ROLLBACK PREPARED **不能经 SPI 执行**：它们不允许出现在
         * 事务块里，而 SQL 函数体永远在调用方的事务里 —— SPI 跑必然报
         * "COMMIT PREPARED cannot run inside a transaction block"。
         * 只能像 TopologyMonitor 的 self-probe 那样，经 libpq 连回本节点，
         * 让它作为**顶层语句**执行。
         *
         * 先闭合事务、再补标记记录。顺序无关正确性（决议已在协调组持久化，
         * 是终局的），但闭合优先能尽快释放 prepared 事务持有的锁。
         */
        {
            char      selfconn[256];
            char      cmd[256];
            PGconn   *sc;
            PGresult *sres;

            pg_raft_format_conninfo("127.0.0.1", PostPortNumber,
                                    selfconn, sizeof(selfconn));
            sc = PQconnectdb(selfconn);
            if (PQstatus(sc) != CONNECTION_OK)
            {
                elog(WARNING, "pg_raft: dtx 恢复：连回本节点失败: %s",
                     PQerrorMessage(sc));
                PQfinish(sc);
                raft_persist_spi_end(spi_owned);
                continue;
            }
            snprintf(cmd, sizeof(cmd), "%s PREPARED '%s'",
                     (verdict == 1) ? "COMMIT" : "ROLLBACK", gids[i]);
            sres = PQexec(sc, cmd);
            if (PQresultStatus(sres) == PGRES_COMMAND_OK)
                handled++;
            else
                elog(WARNING, "pg_raft: dtx 恢复：%s 失败: %s",
                     cmd, PQerrorMessage(sc));
            PQclear(sres);
            PQfinish(sc);
        }

        for (k = 0; k < nmygsids; k++)
        {
            int64 local_oid = 0;
            bool  isnull;

            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "SELECT partdist.local_partition_for_shard(%lld)",
                             (long long) mygsids[k]);
            if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
            {
                Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                        SPI_tuptable->tupdesc, 1, &isnull);

                if (!isnull)
                    local_oid = DatumGetInt64(d);
            }
            pfree(sql.data);

            if (local_oid <= 0)
                continue;       /* 本节点不承载这个分片：没有 parwal 流可补 */

            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "SELECT partdist.partwal_append_dtx_record("
                             "%u::oid, %d, %lld::bigint, %lld::bigint)",
                             (unsigned) local_oid,
                             (verdict == 1) ? 3 : 4,   /* DTX_COMMIT / DTX_ABORT */
                             (long long) dtxid, (long long) coord_gsid);
            (void) SPI_execute(sql.data, false, 1);
            pfree(sql.data);
        }
        raft_persist_spi_end(spi_owned);

        elog(LOG, "pg_raft: dtx 恢复：gid=%s 决议=%s，已闭合",
             gids[i], (verdict == 1) ? "COMMIT" : "ABORT");
    }

    /*
     * 回执清扫（§9.7 acked）：本节点已闭合（prepared 已不在）但登记还在的行，
     * 向协调组 leader 回执本节点写过的组；被接受后删除登记行。协调组 leader
     * 收齐全部参与组的回执后写 FORGET 记录（dtx_ack 内触发），决议行随 apply
     * 在**全体成员**上删除 —— 决议的 GC 由此闭环。coord 为 NULL 的行（快路径/
     * 机制测试）没有决议、无回执可言，留给 dtx_gc_participant 按龄清理。
     */
    dtx_ack_sweep();

    /*
     * FORGET 重试清扫：acked 已收齐但 FORGET 尚未写成（当时失多数派等）的
     * 决议行，由现任协调组 leader 补写。acked 是 leader 本地的 GC 提示，
     * 选举转移会丢——那些行保守地留在表里（正确性不受影响），见 §9.7。
     */
    dtx_forget_sweep();

    /*
     * 顺手做参与登记的 GC（§9.7）：dtx_gc_participant 此前没有任何自动调用方
     * ——prepare 失败留下的孤儿行、已闭合事务的行会无限累积（2026-08-04 审查
     * 发现，与"恢复守护无人调用"同一类缺口）。它只删"已无对应 prepared 事务
     * 且超龄（默认 1h）"的行，有 prepared 在就绝不删（那是恢复寻址的唯一线索），
     * 安全幂等；失败就等下轮。
     */
    if (raft_persist_spi_begin(&spi_owned))
    {
        (void) SPI_execute("SELECT partdist.dtx_gc_participant()", false, 1);
        raft_persist_spi_end(spi_owned);
    }

    /*
     * pg_dist_transaction 的 GC（§9.4 残留边界 → §9.7）：只在 Citus 协调节点、
     * 每 6 轮（约 1 分钟）一次。Citus 的恢复本来兼任这张表的 GC，被 §9.4 关掉
     * 后行只增不减；而它是 citus 规则的真相源，删错一行 = 把仍在等收尾的事务
     * 错判成 ABORT，所以删除条件是"发起 backend 已死 && 所有节点都已无该 gid
     * 的 prepared 事务"，任一节点不可达即整轮放弃。
     */
    if (pg_raft_coordinator_node_id > 0 &&
        pg_raft_node_id == pg_raft_coordinator_node_id)
    {
        static int gc_tick = 0;

        if (++gc_tick >= 6)
        {
            gc_tick = 0;
            (void) dtx_gc_dist_transaction();
        }
    }

    PG_RETURN_INT32(handled);
}

/* ================================================================== */
/* DTX-2PC 决议 GC：回执（acked）与 FORGET（DTX_2PC_DESIGN.md §9.7）    */
/* ================================================================== */

/*
 * 与 dtx_coord_ctx 同一套前置，但**任何不满足都静默返回 false**，不 ERROR。
 * 清扫路径用：清扫是尽力而为的后台工作，一个组的异常不该杀掉整轮守护。
 */
static bool
dtx_coord_ctx_soft(int64 coord_gsid, RaftGroupCtx *ctx, int64 *local_oid)
{
    int state;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        return false;
    if (!raft_group_ctx(coord_gsid, ctx))
        return false;
    if (!group_resolve_membership(ctx))
        return false;

    restore_hard_state_if_needed(ctx);
    restore_persistent_log_if_needed(ctx);

    SpinLockAcquire(&ctx->cons->mutex);
    state = ctx->cons->state;
    SpinLockRelease(&ctx->cons->mutex);
    if (state != RAFT_LEADER)
        return false;

    *local_oid = group_local_partition(ctx);
    return (*local_oid > 0);
}

/*
 * 在协调组日志里追加一条 FORGET 记录并复制到多数派。
 * 调用方必须已确认 leader 身份并持有复制认领位。
 * 决议行的删除**不在这里做**——由每个成员（含 leader 自己）apply 该条目时
 * 执行，删除因此走与写入相同的复制路径，全体成员同步回收。
 */
static void
dtx_forget_append(RaftGroupCtx *ctx, int64 local_oid, int64 dtxid)
{
    StringInfoData sql;
    bool  spi_owned;
    bool  isnull;
    int64 plsn = 0;

    if (!raft_persist_spi_begin(&spi_owned))
        ereport(ERROR, (errmsg("pg_raft: dtx FORGET 需要 SPI")));
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partdist.partwal_append_dtx_record("
                     "%u::oid, 5, %lld::bigint, %lld::bigint)",
                     (unsigned) local_oid, (long long) dtxid,
                     (long long) ctx->group_id);
    if (SPI_execute(sql.data, false, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);

        if (!isnull)
            plsn = DatumGetInt64(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (plsn <= 0)
        ereport(ERROR,
                (errmsg("pg_raft: 组 %lld 写 FORGET 记录失败（dtxid=%lld）",
                        (long long) ctx->group_id, (long long) dtxid)));

    replicate_group_upto(ctx, plsn, (Oid) local_oid);
}

/*
 * partdist.dtx_ack(coord_gsid, dtxid, gsids[]) → bool
 *
 * 参与者回执（§9.7）：在协调组 leader 上把 gsids 并进该决议的 acked。
 * 非 leader 返回 NULL（调用方按 partition_map 重新寻址）。
 * 行不存在返回 true —— 已被 FORGET（或从未有决议），对回执方都算闭环。
 * acked 收齐（⊇ participants 且非空）即写 FORGET 记录复制到多数派；
 * FORGET 失败则 ERROR，acked 已记下（leader 本地），下轮清扫重试。
 */
PG_FUNCTION_INFO_V1(pg_raft_dtx_ack);

Datum
pg_raft_dtx_ack(PG_FUNCTION_ARGS)
{
    int64        coord_gsid = PG_GETARG_INT64(0);
    int64        dtxid = PG_GETARG_INT64(1);
    ArrayType   *arr = PG_ARGISNULL(2) ? NULL : PG_GETARG_ARRAYTYPE_P(2);
    RaftGroupCtx ctx;
    int64        local_oid = 0;
    char        *gsids_sql;
    StringInfoData sql;
    bool         spi_owned;
    bool         row_exists = false;
    bool         complete = false;

    if (!dtx_coord_ctx(coord_gsid, &ctx, &local_oid))
        PG_RETURN_NULL();

    gsids_sql = dtx_participants_sql(arr);

    if (!raft_persist_spi_begin(&spi_owned))
        ereport(ERROR, (errmsg("pg_raft: dtx_ack 需要 SPI")));
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "UPDATE partdist.dtx_decision SET acked = "
                     " (SELECT coalesce(array_agg(DISTINCT t.x ORDER BY t.x), '{}'::bigint[]) "
                     "    FROM unnest(acked || %s) AS t(x)) "
                     " WHERE dtxid = %lld "
                     " RETURNING acked @> participants AND participants <> '{}'::bigint[]",
                     gsids_sql, (long long) dtxid);
    if (SPI_execute(sql.data, false, 1) == SPI_OK_UPDATE_RETURNING &&
        SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);

        row_exists = true;
        if (!isnull)
            complete = DatumGetBool(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    pfree(gsids_sql);

    if (row_exists && complete)
    {
        replicate_claim(&ctx);
        in_txn_replication = true;
        PG_TRY();
        {
            dtx_forget_append(&ctx, local_oid, dtxid);
        }
        PG_FINALLY();
        {
            in_txn_replication = false;
            replicate_release(&ctx);
        }
        PG_END_TRY();
    }

    PG_RETURN_BOOL(true);
}

/*
 * 回执清扫（守护每轮调用）。逐行经 libpq 送达协调组 leader（对自己也走
 * libpq——统一软失败语义，一行失败不影响其余），被接受后删登记行。
 */
static void
dtx_ack_sweep(void)
{
    StringInfoData sql;
    bool           spi_owned;
    int            n = 0;
    int            i;
    char         **gids = NULL;
    long long     *dtxids = NULL;
    long long     *coords = NULL;
    char         **gsids_txt = NULL;

    if (!raft_persist_spi_begin(&spi_owned))
        return;
    initStringInfo(&sql);
    appendStringInfoString(&sql,
        "SELECT p.dtxid, p.gid, p.coord_gsid, p.gsids::text "
        "  FROM partdist.dtx_participant p "
        " WHERE p.coord_gsid IS NOT NULL "
        "   AND p.noted_at < now() - interval '5 seconds' "
        "   AND NOT EXISTS (SELECT 1 FROM pg_prepared_xacts x WHERE x.gid = p.gid) "
        " LIMIT 16");
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        MemoryContext old = MemoryContextSwitchTo(CurTransactionContext);

        n = (int) SPI_processed;
        gids = (char **) palloc(sizeof(char *) * n);
        dtxids = (long long *) palloc(sizeof(long long) * n);
        coords = (long long *) palloc(sizeof(long long) * n);
        gsids_txt = (char **) palloc(sizeof(char *) * n);
        for (i = 0; i < n; i++)
        {
            char *v;

            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            dtxids[i] = v ? atoll(v) : 0;
            gids[i] = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3);
            coords[i] = v ? atoll(v) : 0;
            gsids_txt[i] = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4);
        }
        MemoryContextSwitchTo(old);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    for (i = 0; i < n; i++)
    {
        int       coord_node = 0;
        int       slot = -1;
        int       j;
        char      conninfo[256];
        StringInfoData qry;
        PGconn   *conn;
        PGresult *res;
        bool      acked_ok = false;

        if (dtxids[i] <= 0 || coords[i] <= 0 ||
            gids[i] == NULL || gsids_txt[i] == NULL)
            continue;

        /* 协调组现任 leader */
        if (!raft_persist_spi_begin(&spi_owned))
            break;
        initStringInfo(&qry);
        appendStringInfo(&qry,
                         "SELECT primary_node FROM partdist.partition_map "
                         " WHERE partition_id = %llu::oid",
                         (unsigned long long) coords[i]);
        if (SPI_execute(qry.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                    1, &isnull);

            if (!isnull)
                coord_node = DatumGetInt32(d);
        }
        pfree(qry.data);
        raft_persist_spi_end(spi_owned);
        if (coord_node <= 0)
            continue;

        if (coord_node == pg_raft_node_id)
            pg_raft_format_conninfo("127.0.0.1", PostPortNumber,
                                    conninfo, sizeof(conninfo));
        else
        {
            for (j = 0; j < n_peers; j++)
                if (peers[j].node_id == coord_node)
                {
                    slot = j;
                    break;
                }
            if (slot < 0)
                continue;
            pg_raft_format_conninfo(peers[slot].host, peers[slot].port,
                                    conninfo, sizeof(conninfo));
        }

        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) != CONNECTION_OK)
        {
            PQfinish(conn);
            continue;
        }
        initStringInfo(&qry);
        appendStringInfo(&qry,
                         "SELECT partdist.dtx_ack(%lld, %lld, '%s'::bigint[])",
                         coords[i], dtxids[i], gsids_txt[i]);
        res = PQexec(conn, qry.data);
        if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
            !PQgetisnull(res, 0, 0) &&
            strcmp(PQgetvalue(res, 0, 0), "t") == 0)
            acked_ok = true;
        PQclear(res);
        PQfinish(conn);
        pfree(qry.data);

        if (!acked_ok)
            continue;           /* 非 leader / 不可达：下轮再来 */

        /* 回执已被 leader 记下（或已 FORGET）：登记行的使命结束 */
        if (!raft_persist_spi_begin(&spi_owned))
            break;
        initStringInfo(&qry);
        appendStringInfo(&qry,
                         "DELETE FROM partdist.dtx_participant "
                         " WHERE dtxid = %lld AND gid = %s",
                         dtxids[i], quote_literal_cstr(gids[i]));
        (void) SPI_execute(qry.data, false, 0);
        pfree(qry.data);
        raft_persist_spi_end(spi_owned);
    }
}

/*
 * FORGET 重试清扫：本地 dtx_decision 里 acked 已收齐但行还在的决议，
 * 若本节点仍是该协调组的 leader 就补写 FORGET。
 */
static void
dtx_forget_sweep(void)
{
    StringInfoData sql;
    bool           spi_owned;
    int            n = 0;
    int            i;
    long long     *dtxids = NULL;
    long long     *coords = NULL;

    if (!raft_persist_spi_begin(&spi_owned))
        return;
    initStringInfo(&sql);
    appendStringInfoString(&sql,
        "SELECT dtxid, coord_gsid FROM partdist.dtx_decision "
        " WHERE participants <> '{}'::bigint[] AND acked @> participants "
        " LIMIT 8");
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        MemoryContext old = MemoryContextSwitchTo(CurTransactionContext);

        n = (int) SPI_processed;
        dtxids = (long long *) palloc(sizeof(long long) * n);
        coords = (long long *) palloc(sizeof(long long) * n);
        for (i = 0; i < n; i++)
        {
            char *v;

            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            dtxids[i] = v ? atoll(v) : 0;
            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
            coords[i] = v ? atoll(v) : 0;
        }
        MemoryContextSwitchTo(old);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    for (i = 0; i < n; i++)
    {
        RaftGroupCtx ctx;
        int64        local_oid = 0;

        if (dtxids[i] <= 0 || coords[i] <= 0)
            continue;
        if (!dtx_coord_ctx_soft(coords[i], &ctx, &local_oid))
            continue;           /* 不是这组的 leader：由现任 leader 负责 */

        replicate_claim(&ctx);
        in_txn_replication = true;
        PG_TRY();
        {
            dtx_forget_append(&ctx, local_oid, dtxids[i]);
        }
        PG_FINALLY();
        {
            in_txn_replication = false;
            replicate_release(&ctx);
        }
        PG_END_TRY();
    }
}

/*
 * pg_dist_transaction 的 GC（§9.7）。返回删除行数；-1 = 本轮放弃
 * （有节点不可达，保守不删）。删除条件（缺一不可）：
 *   1) 发起 backend 已死（gid 里编的 pid 不在本机 pg_stat_activity）——
 *      活着说明 master 可能还没走完提交，行随时会被用到；
 *   2) 每个 peer 节点都确认无该 gid 的 prepared 事务 —— 有 prepared 在，
 *      这行就是它按 citus 规则收尾的真相源，删了会被错判成 ABORT。
 */
static int
dtx_gc_dist_transaction(void)
{
    StringInfoData sql;
    bool           spi_owned;
    int            n = 0;
    int            i, j;
    char         **gids = NULL;
    bool          *keep = NULL;
    int            deleted = 0;

    if (!raft_persist_spi_begin(&spi_owned))
        return -1;

    /* 表不存在（非 Citus 库）直接返回 */
    initStringInfo(&sql);
    appendStringInfoString(&sql,
        "SELECT to_regclass('pg_catalog.pg_dist_transaction') IS NOT NULL");
    {
        bool present = false;

        if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                    1, &isnull);

            present = !isnull && DatumGetBool(d);
        }
        pfree(sql.data);
        if (!present)
        {
            raft_persist_spi_end(spi_owned);
            return 0;
        }
    }

    /*
     * 候选 = 发起 backend 已死的行。存活栅栏在同一条 SQL 里做（都在本机）：
     * gid 第二段是发起 backend 的 pid。
     */
    initStringInfo(&sql);
    appendStringInfoString(&sql,
        "SELECT DISTINCT t.gid FROM pg_catalog.pg_dist_transaction t "
        " WHERE (split_part(t.gid, '_', 3) ~ '^[0-9]+$') "
        "   AND NOT EXISTS (SELECT 1 FROM pg_stat_activity a "
        "                    WHERE a.pid = split_part(t.gid, '_', 3)::int) "
        " LIMIT 128");
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        MemoryContext old = MemoryContextSwitchTo(CurTransactionContext);

        n = (int) SPI_processed;
        gids = (char **) palloc(sizeof(char *) * n);
        keep = (bool *) palloc0(sizeof(bool) * n);
        for (i = 0; i < n; i++)
            gids[i] = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
        MemoryContextSwitchTo(old);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (n == 0)
        return 0;

    /* 逐节点确认无 prepared；任一节点不可达 ⇒ 整轮放弃 */
    parse_peers();
    for (j = 0; j < n_peers; j++)
    {
        char      conninfo[256];
        PGconn   *conn;
        PGresult *res;
        StringInfoData qry;
        int       r;

        pg_raft_format_conninfo(peers[j].host, peers[j].port,
                                conninfo, sizeof(conninfo));
        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) != CONNECTION_OK)
        {
            elog(DEBUG1, "pg_raft: dist_transaction GC：节点 %d 不可达，本轮放弃",
                 peers[j].node_id);
            PQfinish(conn);
            return -1;
        }

        initStringInfo(&qry);
        appendStringInfoString(&qry,
            "SELECT gid FROM pg_prepared_xacts WHERE gid = ANY(ARRAY[");
        for (i = 0; i < n; i++)
        {
            char *esc = PQescapeLiteral(conn, gids[i], strlen(gids[i]));

            if (esc == NULL)
                continue;
            appendStringInfo(&qry, "%s%s", (i == 0) ? "" : ",", esc);
            PQfreemem(esc);
        }
        appendStringInfoString(&qry, "]::text[])");

        res = PQexec(conn, qry.data);
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            PQclear(res);
            PQfinish(conn);
            pfree(qry.data);
            return -1;
        }
        for (r = 0; r < PQntuples(res); r++)
        {
            const char *g = PQgetvalue(res, r, 0);

            for (i = 0; i < n; i++)
                if (strcmp(gids[i], g) == 0)
                    keep[i] = true;
        }
        PQclear(res);
        PQfinish(conn);
        pfree(qry.data);
    }

    /* 删除全网确认已闭合的行 */
    if (!raft_persist_spi_begin(&spi_owned))
        return -1;
    for (i = 0; i < n; i++)
    {
        if (keep[i] || gids[i] == NULL)
            continue;
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "DELETE FROM pg_catalog.pg_dist_transaction WHERE gid = %s",
                         quote_literal_cstr(gids[i]));
        if (SPI_execute(sql.data, false, 0) == SPI_OK_DELETE)
            deleted += (int) SPI_processed;
        pfree(sql.data);
    }
    raft_persist_spi_end(spi_owned);

    if (deleted > 0)
        elog(LOG, "pg_raft: dist_transaction GC：清理 %d 行（全网确认已闭合）", deleted);
    return deleted;
}

/* SQL 包装：测试与手工运维入口 */
PG_FUNCTION_INFO_V1(pg_raft_dtx_gc_dist_transaction);

Datum
pg_raft_dtx_gc_dist_transaction(PG_FUNCTION_ARGS)
{
    (void) fcinfo;
    parse_peers();
    PG_RETURN_INT32(dtx_gc_dist_transaction());
}

/* ================================================================== */
/* DTX-2PC 升主 in-doubt 闭合（DTX_2PC_DESIGN.md §9.6，机制先行）        */
/* ================================================================== */

/*
 * 全网找一笔事务的决议。返回 1=COMMIT 2=ABORT 0=找不到（保持 in-doubt）。
 *
 * 优先级（每一级都是"能给出终局答案才返回"）：
 *   a) 本地登记里有 coord_gsid ⇒ 问协调组现任 leader（dtx_status，含推定
 *      中止的权威——查无决议时它会先写 ABORT 达多数派再答复）；
 *   b) 本地 dtx_decision（本节点是协调组成员时 apply 已建好索引）；
 *   c) 广播全部 peer 的 dtx_decision —— 决议在协调组多数派上都有索引行，
 *      任何一个成员可达即命中；
 *   d) citus 形态的 dtxid：按前缀查 master 的 pg_dist_transaction（带发起者
 *      存活栅栏）。**只有"行在 ⇒ COMMIT"是终局的**；行不在不能推出 ABORT
 *      ——决议可能存在于此刻不可达的协调组里，而我们不知道协调组是谁、
 *      也就无法把推定中止**写下来**（§2.2：先写 ABORT 决议再答复）。
 *
 * 找不到就返回 0，调用方保持 in-doubt。这与 §7 的守护同一条纪律：
 * 拿不到权威答案绝不擅自决定。
 */
static int
dtx_resolve_verdict_anywhere(int64 dtxid, int64 reg_coord)
{
    StringInfoData sql;
    bool           spi_owned;
    int            verdict = 0;
    int            j;

    /* a) 登记里的协调组：权威通道 */
    if (reg_coord > 0)
    {
        verdict = dtx_ask_coordinator(reg_coord, dtxid);
        if (verdict == 1 || verdict == 2)
            return verdict;
    }

    /* b) 本地决议索引 */
    if (raft_persist_spi_begin(&spi_owned))
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "SELECT verdict FROM partdist.dtx_decision WHERE dtxid = %lld",
                         (long long) dtxid);
        if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                    1, &isnull);

            if (!isnull)
                verdict = (int) DatumGetInt16(d);
        }
        pfree(sql.data);
        raft_persist_spi_end(spi_owned);
        if (verdict == 1 || verdict == 2)
            return verdict;
    }

    /* c) 广播 peer 的决议索引 */
    for (j = 0; j < n_peers; j++)
    {
        char      conninfo[256];
        char      qry[128];
        PGconn   *conn;
        PGresult *res;

        if (peers[j].node_id == pg_raft_node_id)
            continue;
        pg_raft_format_conninfo(peers[j].host, peers[j].port,
                                conninfo, sizeof(conninfo));
        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) != CONNECTION_OK)
        {
            PQfinish(conn);
            continue;
        }
        snprintf(qry, sizeof(qry),
                 "SELECT verdict FROM partdist.dtx_decision WHERE dtxid = %lld",
                 (long long) dtxid);
        res = PQexec(conn, qry);
        if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
            !PQgetisnull(res, 0, 0))
            verdict = atoi(PQgetvalue(res, 0, 0));
        PQclear(res);
        PQfinish(conn);
        if (verdict == 1 || verdict == 2)
            return verdict;
    }

    /* d) citus 形态：按 gid 前缀问 master（只认 COMMIT） */
    {
        long long g_group = (long long) ((((uint64) dtxid) >> 55) & 0xFF);
        long long g_pid = (long long) ((((uint64) dtxid) >> 33) & 0x3FFFFF);
        long long g_txn = (long long) (((uint64) dtxid) & 0x1FFFFFFFFLL);

        if (g_pid > 0 && g_txn > 0 && pg_raft_coordinator_node_id > 0)
        {
            int       slot = -1;
            char      conninfo[256];
            char      qry[256];
            PGconn   *conn;
            PGresult *res;

            for (j = 0; j < n_peers; j++)
                if (peers[j].node_id == pg_raft_coordinator_node_id)
                {
                    slot = j;
                    break;
                }
            if (slot >= 0)
            {
                pg_raft_format_conninfo(peers[slot].host, peers[slot].port,
                                        conninfo, sizeof(conninfo));
                conn = PQconnectdb(conninfo);
                if (PQstatus(conn) == CONNECTION_OK)
                {
                    bool alive = true;

                    /* 发起者存活栅栏（先查 pid 后查行，顺序敏感，§9.4） */
                    snprintf(qry, sizeof(qry),
                             "SELECT count(*) FROM pg_stat_activity WHERE pid = %lld",
                             g_pid);
                    res = PQexec(conn, qry);
                    if (PQresultStatus(res) == PGRES_TUPLES_OK &&
                        PQntuples(res) == 1 && !PQgetisnull(res, 0, 0))
                        alive = (atoi(PQgetvalue(res, 0, 0)) > 0);
                    PQclear(res);

                    if (!alive)
                    {
                        snprintf(qry, sizeof(qry),
                                 "SELECT count(*) FROM pg_catalog.pg_dist_transaction "
                                 " WHERE gid LIKE 'citus\\_%lld\\_%lld\\_%lld\\_%%'",
                                 g_group, g_pid, g_txn);
                        res = PQexec(conn, qry);
                        if (PQresultStatus(res) == PGRES_TUPLES_OK &&
                            PQntuples(res) == 1 && !PQgetisnull(res, 0, 0) &&
                            atoi(PQgetvalue(res, 0, 0)) > 0)
                            verdict = 1;
                        PQclear(res);
                    }
                    PQfinish(conn);
                }
                else
                    PQfinish(conn);
            }
        }
    }

    return verdict;
}

/*
 * partdist.dtx_close_indoubt(partition_id) → int
 *
 * 对本节点该分区的 parwal 流做 §9.6 的 in-doubt 闭合：找出"有 DTX_PREPARE、
 * 无 DECISION/COMMIT/ABORT"的 dtxid，全网求决议，找到就补写闭合标记。
 * 返回本轮闭合的事务数；找不到决议的保持 in-doubt（NOTICE 报数），
 * 调用方（升主序列/重试循环）下轮再来。
 *
 * 机制先行（§10.1）：函数本身不依赖惰性回放——它按**事务粒度**问决议补标记，
 * 不做任何元组级可见性判定。升主序列落地时在追平之后、对外服务之前调它。
 * 补写的标记本轮不主动复制（升主语境下本节点即将成为 leader，随后的
 * propose/心跳会带出去）。
 */
PG_FUNCTION_INFO_V1(pg_raft_dtx_close_indoubt);

Datum
pg_raft_dtx_close_indoubt(PG_FUNCTION_ARGS)
{
    Oid            partition_id = PG_GETARG_OID(0);
    StringInfoData sql;
    bool           spi_owned;
    int            n = 0;
    int            i;
    long long     *dtxids = NULL;
    int            closed = 0;
    int            unresolved = 0;

    if (!pg_raft_raft_enabled)
        PG_RETURN_INT32(0);
    parse_peers();

    /* 1) in-doubt 清单：PREPARE 可见、闭合（DECISION/COMMIT/ABORT）不可见 */
    if (!raft_persist_spi_begin(&spi_owned))
        PG_RETURN_INT32(0);
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "WITH recs AS ("
        "  SELECT d.kind, d.dtxid"
        "    FROM generate_series(1, partdist.get_partition_flush_lsn(%u::oid)) g"
        "    LEFT JOIN LATERAL partdist.partwal_read_dtx_record(%u::oid, g) d ON true"
        "   WHERE d.dtxid IS NOT NULL) "
        "SELECT dtxid FROM recs WHERE kind = 1 "
        "EXCEPT "
        "SELECT dtxid FROM recs WHERE kind IN (2, 3, 4) "
        "LIMIT 64",
        (unsigned) partition_id, (unsigned) partition_id);
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        MemoryContext old = MemoryContextSwitchTo(CurTransactionContext);

        n = (int) SPI_processed;
        dtxids = (long long *) palloc(sizeof(long long) * n);
        for (i = 0; i < n; i++)
        {
            char *v = SPI_getvalue(SPI_tuptable->vals[i],
                                   SPI_tuptable->tupdesc, 1);

            dtxids[i] = v ? atoll(v) : 0;
        }
        MemoryContextSwitchTo(old);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    /* 2) 逐笔求决议、补标记 */
    for (i = 0; i < n; i++)
    {
        long long reg_coord = 0;
        int       verdict;

        if (dtxids[i] <= 0)
            continue;

        if (raft_persist_spi_begin(&spi_owned))
        {
            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "SELECT coord_gsid FROM partdist.dtx_participant "
                             " WHERE dtxid = %lld AND coord_gsid IS NOT NULL LIMIT 1",
                             dtxids[i]);
            if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
            {
                bool  isnull;
                Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                        SPI_tuptable->tupdesc, 1, &isnull);

                if (!isnull)
                    reg_coord = (long long) DatumGetInt64(d);
            }
            pfree(sql.data);
            raft_persist_spi_end(spi_owned);
        }

        verdict = dtx_resolve_verdict_anywhere((int64) dtxids[i], (int64) reg_coord);
        if (verdict != 1 && verdict != 2)
        {
            unresolved++;
            continue;
        }

        if (raft_persist_spi_begin(&spi_owned))
        {
            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "SELECT partdist.partwal_append_dtx_record("
                             "%u::oid, %d, %lld::bigint, %lld::bigint)",
                             (unsigned) partition_id,
                             (verdict == 1) ? 3 : 4,
                             dtxids[i], reg_coord);
            if (SPI_execute(sql.data, false, 1) == SPI_OK_SELECT)
                closed++;
            pfree(sql.data);
            raft_persist_spi_end(spi_owned);
        }
        elog(LOG, "pg_raft: in-doubt 闭合：分区 %u dtxid=%lld → %s",
             partition_id, dtxids[i], (verdict == 1) ? "COMMIT" : "ABORT");
    }

    if (unresolved > 0)
        ereport(NOTICE,
                (errmsg("pg_raft: 分区 %u 仍有 %d 笔 in-doubt 事务找不到决议，保持不动",
                        partition_id, unresolved)));

    PG_RETURN_INT32(closed);
}

/* ================================================================== */
/* DTX-2PC master 侧驱动（DTX_2PC_DESIGN.md §3.3 / §9.3）              */
/* ================================================================== */

/*
 * 挂在内核补丁 0004 的 pre_record_commit_hook 上：Citus 已把
 * PREPARE TRANSACTION 发给全部参与者并收齐应答，本地 commit record 尚未写入。
 * 这里 ereport(ERROR) 仍然能把整个事务干净地翻进 abort 路径。
 *
 * 三步（顺序是正确性的一部分，不能调换）：
 *   1) 逐参与节点读 partdist.dtx_local_participant(dtxid) 合并出**真实写集**
 *      —— 只读参与者返回空数组，天然被剔除（§8.3）；
 *   2) 写集 ≤ 1 组 ⇒ 快路径，不做决议直接返回（§3.4）；否则算
 *      coord_gsid = participants[dtxid % n]，并**先**把它下发到全部参与节点；
 *   3) 到协调组现任 leader 上调 partdist.dtx_decide() —— 该决议记录在协调组
 *      达多数派持久化即为**全局提交点**，返回 COMMIT 之后本函数才放行，
 *      客户端随后收到的 COMMIT 成功因此是有多数派保证的。
 *
 * 为什么第 2 步的下发必须严格早于第 3 步：参与者崩溃重启后只能靠
 * partdist.dtx_participant.coord_gsid 找协调组。若先决议后下发，就会出现
 * "全局已 COMMIT、参与者却查不到协调组"的不可解状态；反过来则永远安全 ——
 * coord_gsid 为 NULL 蕴含决议尚未做过，推定中止（回滚）是正确答案。
 */

/* Citus gid → dtxid。**镜像**实现，权威在 pg-partdist-src/src/dtx/dtx_participant.c
 * 的 DtxidFromGid()：两个扩展之间无编译期依赖，改一处必须同步改另一处。 */
static bool
dtx_dtxid_from_gid(const char *gid, int64 *dtxid)
{
    long long group = 0, pid = 0, txn = 0, conn = 0;
    long long d = 0, c = 0;

    if (gid == NULL)
        return false;

    if (sscanf(gid, "citus_%lld_%lld_%lld_%lld", &group, &pid, &txn, &conn) == 4)
    {
        if (pid <= 0 || txn <= 0)
            return false;
        *dtxid = ((int64) (group & 0xFF) << 55) |
                 ((int64) (pid & 0x3FFFFF) << 33) |
                 ((int64) (txn & 0x1FFFFFFFFLL));
        return (*dtxid > 0);
    }
    if (sscanf(gid, "shardpg_dtx_%lld_%lld", &d, &c) == 2 && d > 0)
    {
        *dtxid = (int64) d;
        return true;
    }
    return false;
}

/* ---- 每 backend 的 libpq 连接缓存（每事务重连太贵） ---- */
#define DTX_CONN_CACHE_MAX 16

typedef struct DtxConnCacheEntry
{
    char    host[NAMEDATALEN];
    int     port;
    PGconn *conn;
} DtxConnCacheEntry;

static DtxConnCacheEntry dtx_conns[DTX_CONN_CACHE_MAX];
static int               dtx_nconns = 0;

static PGconn *
dtx_get_conn(const char *host, int port)
{
    char conninfo[256];
    int  i;
    int  slot = -1;

    for (i = 0; i < dtx_nconns; i++)
        if (dtx_conns[i].port == port && strcmp(dtx_conns[i].host, host) == 0)
        {
            slot = i;
            break;
        }

    if (slot >= 0)
    {
        if (PQstatus(dtx_conns[slot].conn) == CONNECTION_OK)
            return dtx_conns[slot].conn;
        PQreset(dtx_conns[slot].conn);
        if (PQstatus(dtx_conns[slot].conn) == CONNECTION_OK)
            return dtx_conns[slot].conn;
        PQfinish(dtx_conns[slot].conn);
        dtx_conns[slot].conn = NULL;
    }

    pg_raft_format_conninfo(host, port, conninfo, sizeof(conninfo));

    if (slot < 0)
    {
        if (dtx_nconns >= DTX_CONN_CACHE_MAX)
        {
            /* 缓存满：牺牲第 0 槽（拓扑规模远小于 16，实际到不了这里） */
            PQfinish(dtx_conns[0].conn);
            slot = 0;
        }
        else
            slot = dtx_nconns++;
        strlcpy(dtx_conns[slot].host, host, NAMEDATALEN);
        dtx_conns[slot].port = port;
    }

    dtx_conns[slot].conn = PQconnectdb(conninfo);
    if (PQstatus(dtx_conns[slot].conn) != CONNECTION_OK)
    {
        char *msg = pstrdup(PQerrorMessage(dtx_conns[slot].conn));

        PQfinish(dtx_conns[slot].conn);
        dtx_conns[slot].conn = NULL;
        if (slot == dtx_nconns - 1)
            dtx_nconns--;
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                 errmsg("pg_raft: DTX 驱动连不上参与节点 %s:%d", host, port),
                 errdetail("%s", msg)));
    }
    return dtx_conns[slot].conn;
}

/* 把 int8[] 的文本形式 "{1,2,3}" / "{}" 解析进 out，返回元素个数 */
static int
dtx_parse_int8_array(const char *text, int64 *out, int max)
{
    const char *p = text;
    int         n = 0;

    if (p == NULL)
        return 0;
    while (*p != '\0' && *p != '{')
        p++;
    if (*p == '{')
        p++;
    while (*p != '\0' && *p != '}' && n < max)
    {
        char *end = NULL;
        long long v;

        while (*p == ',' || *p == ' ')
            p++;
        if (*p == '\0' || *p == '}')
            break;
        v = strtoll(p, &end, 10);
        if (end == p)
            break;
        out[n++] = (int64) v;
        p = end;
    }
    return n;
}

static int
dtx_cmp_int64(const void *a, const void *b)
{
    int64 x = *(const int64 *) a;
    int64 y = *(const int64 *) b;

    return (x < y) ? -1 : ((x > y) ? 1 : 0);
}

/* 在远端节点上执行一条返回单值的 SQL；返回值需 pfree，NULL 表示 SQL NULL */
static char *
dtx_remote_scalar(const char *host, int port, const char *sql)
{
    PGconn   *conn = dtx_get_conn(host, port);
    PGresult *res = PQexec(conn, sql);
    char     *out = NULL;

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        char *msg = pstrdup(PQerrorMessage(conn));

        PQclear(res);
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                 errmsg("pg_raft: DTX 驱动在 %s:%d 上执行失败", host, port),
                 errdetail("%s", msg),
                 errhint("SQL: %s", sql)));
    }
    if (PQntuples(res) == 1 && !PQgetisnull(res, 0, 0))
        out = pstrdup(PQgetvalue(res, 0, 0));
    PQclear(res);
    return out;
}

#define DTX_MAX_PARTICIPANT_NODES  RAFT_MAX_PEERS
#define DTX_MAX_PARTICIPANT_GROUPS 512

/*
 * 本库是否装了 Citus 的两张目录表。必须在 SPI 已连接、快照已压好的状态下调用。
 *
 * 缓存策略：只缓存"有"。缓存"没有"会让同一 session 里后建的 citus 扩展
 * 永远看不见；而"有"之后再消失只可能是 DROP EXTENSION citus，那种情况下
 * 本节点也早就不是 Citus 协调者了。
 */
static bool dtx_citus_present = false;

static bool
dtx_citus_catalog_present(void)
{
    bool isnull;

    if (dtx_citus_present)
        return true;

    if (SPI_execute("SELECT to_regclass('pg_catalog.pg_dist_transaction') IS NOT NULL "
                    "   AND to_regclass('pg_catalog.pg_dist_node') IS NOT NULL",
                    true, 1) != SPI_OK_SELECT || SPI_processed == 0)
        return false;

    dtx_citus_present = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
                                                   SPI_tuptable->tupdesc, 1, &isnull))
                        && !isnull;
    return dtx_citus_present;
}

typedef struct DtxNodeAddr
{
    char host[NAMEDATALEN];
    int  port;
} DtxNodeAddr;

/*
 * dtx_master_try_write_abort — 尽力在协调组写一条**显式** ABORT 决议。
 *
 * 何时该调：master 已经把 coord_gsid 下发给全体参与者、但决议没能做成的
 * 每一条失败出口。此后参与者收尾时会去协调组问决议；有这条显式记录，
 * 它们当场就能得到 ABORT，不必等恢复守护的超时（pg_raft.dtx_recover_timeout_ms，
 * 默认 30s）到点后由 dtx_status 反向把 ABORT 创造出来。
 *
 * **纯优化，不是正确性所必需**：写不成也没关系，presumed abort 兜底
 * （DTX_2PC_DESIGN.md §2.2/§7）—— 决议槽一次性，先到的那条获胜，
 * 这里写的 ABORT 与恢复路径后来写的 ABORT 是同一个结论。
 *
 * 因此全程吞掉错误：调用点马上就要 ereport(ERROR) 抛出真正的失败原因，
 * 不能让"补写 ABORT 失败"这种次要错误把它盖掉。
 *
 * 覆盖不到的一类失败要说清楚：**参与者 prepare 失败**时 master 的
 * CommitTransaction() 根本没走到，本函数所在的 pre_record_commit_hook
 * 从不触发。那条路径不需要决议 —— Citus 会同步对全体已 prepared 的连接发
 * ROLLBACK PREPARED，没有参与者会滞留 in-doubt（除非它同时崩溃，
 * 而那正是 presumed abort 的适用场景）。
 */
static void
dtx_master_try_write_abort(int64 coord_gsid, int64 dtxid,
                           const int64 *parts, int nparts)
{
    PG_TRY();
    {
        bool  spi_owned;
        int   coord_node = 0;
        int   slot = -1;
        int   i;
        char  qry[512];
        char *r;
        int   n;
        StringInfoData s;

        /*
         * ★ 本块内的提前退出一律 goto done，**不能 return**。
         *
         * 从 PG_TRY() 块里 return 会跳过 PG_END_TRY()，而恢复
         * PG_exception_stack 的正是 PG_END_TRY()。于是该指针继续指向本函数
         * 这个**已经退栈**的帧里的 _local_sigjmp_buf，之后同一 backend 里任何
         * 一次 ereport(ERROR) 都会 siglongjmp 进死帧 —— glibc 的
         * _FORTIFY_SOURCE 检查到栈指针方向不对就 abort()：
         *     *** longjmp causes uninitialized stack frame ***: terminated
         *     server process was terminated by signal 6: Aborted
         *     DETAIL: Failed process was running: COMMIT;
         * 2026-08-10 在全新复现环境 pg-citus-tx2 上实测到（coordinator 崩、
         * TX1 的跨分区事务提交失败）。偶发：要先走到这几个提前退出之一，
         * 之后同一 backend 再发生一次 ERROR 才触发，所以暖环境连跑四轮全量
         * 都没碰上。
         *
         * 三个退出点都没有待清理的资源（raft_persist_spi_end 在下面已调过），
         * 所以 goto 与原来的 return 语义等价，只是让 PG_END_TRY() 必定执行。
         */
        if (!raft_persist_spi_begin(&spi_owned))
            goto done;
        initStringInfo(&s);
        appendStringInfo(&s,
                         "SELECT primary_node FROM partdist.partition_map "
                         " WHERE partition_id = %llu::oid",
                         (unsigned long long) coord_gsid);
        if (SPI_execute(s.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                    SPI_tuptable->tupdesc, 1, &isnull);

            if (!isnull)
                coord_node = DatumGetInt32(d);
        }
        pfree(s.data);
        raft_persist_spi_end(spi_owned);

        if (coord_node <= 0)
            goto done;

        parse_peers();
        for (i = 0; i < n_peers; i++)
            if (peers[i].node_id == coord_node)
            {
                slot = i;
                break;
            }
        if (slot < 0)
            goto done;

        n = snprintf(qry, sizeof(qry),
                     "SELECT partdist.dtx_decide(%lld, %lld, 2, ARRAY[",
                     (long long) coord_gsid, (long long) dtxid);
        for (i = 0; i < nparts && n < (int) sizeof(qry) - 32; i++)
            n += snprintf(qry + n, sizeof(qry) - n, "%s%lld",
                          (i == 0) ? "" : ",", (long long) parts[i]);
        snprintf(qry + n, sizeof(qry) - n, "]::bigint[])");

        r = dtx_remote_scalar(peers[slot].host, peers[slot].port, qry);
        if (r != NULL)
        {
            elog(LOG, "pg_raft: dtx %lld 已写入显式 ABORT 决议（协调组 %lld，verdict=%s）",
                 (long long) dtxid, (long long) coord_gsid, r);
            pfree(r);
        }
done:
        ;   /* 统一出口：落到这里就走 PG_END_TRY()，不绕过它 */
    }
    PG_CATCH();
    {
        FlushErrorState();
        elog(LOG,
             "pg_raft: dtx %lld 补写显式 ABORT 决议未成功，退回推定中止"
             "（参与者将在恢复守护超时后从协调组取到 ABORT）",
             (long long) dtxid);
    }
    PG_END_TRY();
}

static void
dtx_master_pre_record_commit(void)
{
    StringInfoData  sql;
    bool            spi_owned;
    int64           dtxid = 0;
    DtxNodeAddr     nodes[DTX_MAX_PARTICIPANT_NODES];
    int             nnodes = 0;
    int64           parts[DTX_MAX_PARTICIPANT_GROUPS];
    int             nparts = 0;
    int64           coord_gsid;
    int             coord_node = 0;
    int             i, j;
    int             verdict = 0;
    int64           dtx_cts = 0;

    if (!pg_raft_raft_enabled || !pg_raft_dtx_2pc_enabled)
        return;
    /*
     * T4.4 决议搬迁（§9.1 MX 定案）：整条 DTX 链在**哪个节点驱动事务就在
     * 哪个节点成立**（本地 pg_dist_transaction 就是驱动者自己写的），协调
     * 节点身份判据随之撤除 —— MX worker 驱动的 2PC 同样在此做决议。
     *
     * 性能闸门的替补：① Citus 内部任务连接（application_name 前缀
     * "citus_internal"）只当参与者、永远不是驱动者，先行退出 —— 参与侧
     * 每条任务提交零开销；② 其余本地提交靠下面 pg_dist_transaction 的
     * xmin 探针过滤 —— 该表平时为空（GC 及时），探针是一次只读小查询
     * （代价挂 R-P4-3 风险单持续观察）。
     */
    if (application_name != NULL &&
        strncmp(application_name, "citus_internal", 14) == 0)
        return;
    if (!IsTransactionState() || GetTopTransactionIdIfAny() == InvalidTransactionId)
        return;

    /* ---- 1) 本事务刚写进 pg_dist_transaction 的行 = 已 prepare 的参与节点 ---- */
    if (!raft_persist_spi_begin(&spi_owned))
        return;

    /*
     * ★ 本库不一定装了 Citus。pg_raft/pg_partdist 在
     * shared_preload_libraries 里，因此**每个库**的每次提交都会进这个 hook ——
     * 包括一个全新的、连 citus 扩展都还没建的库。直接引用
     * pg_catalog.pg_dist_transaction 会在解析期就报 relation does not exist，
     * 于是连 `CREATE EXTENSION pg_partdist` 本身都提交不了
     * （2026-08-04：正是 raft_19 A 段的全新库冒烟抓到的 —— 常规回归全都跑在
     * 已装 Citus 的库上，完全静默）。
     */
    if (!dtx_citus_catalog_present())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfoString(
        &sql,
        "SELECT t.gid, n.nodename, n.nodeport "
        "  FROM pg_catalog.pg_dist_transaction t "
        "  JOIN pg_catalog.pg_dist_node n "
        "    ON n.groupid = t.groupid AND n.noderole = 'primary' "
        " WHERE t.xmin = pg_catalog.pg_current_xact_id()::xid");

    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        for (i = 0; i < (int) SPI_processed; i++)
        {
            char *gid = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            char *host = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
            char *portstr = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3);
            int   port = (portstr != NULL) ? atoi(portstr) : 0;
            bool  dup = false;

            if (dtxid == 0 && !dtx_dtxid_from_gid(gid, &dtxid))
                continue;           /* 不是我们认识的 gid 形态 */
            if (host == NULL || port <= 0)
                continue;

            for (j = 0; j < nnodes; j++)
                if (nodes[j].port == port && strcmp(nodes[j].host, host) == 0)
                {
                    dup = true;
                    break;
                }
            if (!dup && nnodes < DTX_MAX_PARTICIPANT_NODES)
            {
                strlcpy(nodes[nnodes].host, host, NAMEDATALEN);
                nodes[nnodes].port = port;
                nnodes++;
            }
        }
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (dtxid == 0 || nnodes == 0)
        return;                     /* 不是跨节点的 Citus 2PC 事务 */

    /* ---- 2) 逐节点取真实写集（只读参与者返回空数组，自动剔除） ---- */
    for (i = 0; i < nnodes; i++)
    {
        char   qry[128];
        char  *arr;
        int64  got[DTX_MAX_PARTICIPANT_GROUPS];
        int    ngot;

        snprintf(qry, sizeof(qry),
                 "SELECT partdist.dtx_local_participant(%lld)", (long long) dtxid);
        arr = dtx_remote_scalar(nodes[i].host, nodes[i].port, qry);
        if (arr == NULL)
            continue;
        ngot = dtx_parse_int8_array(arr, got, DTX_MAX_PARTICIPANT_GROUPS);
        pfree(arr);

        for (j = 0; j < ngot && nparts < DTX_MAX_PARTICIPANT_GROUPS; j++)
        {
            int  k;
            bool dup = false;

            for (k = 0; k < nparts; k++)
                if (parts[k] == got[j])
                {
                    dup = true;
                    break;
                }
            if (!dup)
                parts[nparts++] = got[j];
        }
    }

    /*
     * 快路径（§3.4）：写集 ≤ 1 个分区组时不做决议。
     * （下面 dtx_master_try_write_abort 的前置声明见文件上方。）
     * 0 组 = 没写任何纳管分片（退化为接线前行为）；
     * 1 组 = 该组自己的 quorum 已经覆盖本事务的全部数据，再走一轮决议没有
     * 任何额外保证，只有额外延迟。
     */
    if (nparts <= 1)
        return;

    qsort(parts, nparts, sizeof(int64), dtx_cmp_int64);
    coord_gsid = parts[(uint64) dtxid % (uint64) nparts];

    /* ---- 3) 先下发协调组（必须严格早于决议，理由见函数头注释） ---- */
    for (i = 0; i < nnodes; i++)
    {
        char  qry[160];
        char *r;

        snprintf(qry, sizeof(qry),
                 "SELECT partdist.dtx_note_coord(%lld, %lld)",
                 (long long) dtxid, (long long) coord_gsid);
        r = dtx_remote_scalar(nodes[i].host, nodes[i].port, qry);
        if (r != NULL)
            pfree(r);
    }

    /*
     * 从这里往下，coord_gsid 已经下发给全体参与者了 —— 它们此后一旦需要收尾，
     * 会去协调组问决议。所以本段的**每一条失败出口**都要先尽力写一条显式
     * ABORT 决议再抛错，否则参与者只能等恢复守护的超时（默认 30s）到点后
     * 由 dtx_status 反向把 ABORT 创造出来。语义两者相同，差的是这段等待。
     */

    /* ---- 3.5) 决议 commit_ts："全部票齐"点取号（§2-4 两时机） ---- */
    {
        static void **tso_dts_rv = NULL;

        if (tso_dts_rv == NULL)
            tso_dts_rv = find_rendezvous_variable("partdist_tso_dtx_decision_ts_fn");
        if (*tso_dts_rv != NULL)
        {
            int64 (*fn)(void) = (int64 (*)(void)) *tso_dts_rv;

            PG_TRY();
            {
                dtx_cts = fn();     /* 未配置 TSO = 0（决议侧回退本地时钟） */
            }
            PG_CATCH();
            {
                /* fail-closed：TSO 不可达 ⇒ 先尽力写显式 ABORT 再抛（本段纪律） */
                dtx_master_try_write_abort(coord_gsid, dtxid, parts, nparts);
                PG_RE_THROW();
            }
            PG_END_TRY();
        }
    }

    /* ---- 4) 到协调组现任 leader 上做决议 ---- */
    if (!raft_persist_spi_begin(&spi_owned))
        ereport(ERROR,
                (errmsg("pg_raft: DTX 决议无法建立 SPI 连接")));
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT primary_node FROM partdist.partition_map "
                     " WHERE partition_id = %llu::oid",
                     (unsigned long long) coord_gsid);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);

        if (!isnull)
            coord_node = DatumGetInt32(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (coord_node <= 0)
    {
        dtx_master_try_write_abort(coord_gsid, dtxid, parts, nparts);
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 分布式事务 %lld 的协调组 %lld 查不到现任 leader",
                        (long long) dtxid, (long long) coord_gsid),
                 errdetail("partdist.partition_map 尚未追平该组的主节点登记。")));
    }

    parse_peers();
    {
        int   slot = -1;
        char  qry[512];
        char *r;
        int   n;

        for (i = 0; i < n_peers; i++)
            if (peers[i].node_id == coord_node)
            {
                slot = i;
                break;
            }
        if (slot < 0)
        {
            dtx_master_try_write_abort(coord_gsid, dtxid, parts, nparts);
            ereport(ERROR,
                    (errmsg("pg_raft: 协调组 %lld 的 leader 节点 %d 不在 pg_raft.peers 里",
                            (long long) coord_gsid, coord_node)));
        }

        n = snprintf(qry, sizeof(qry),
                     "SELECT partdist.dtx_decide(%lld, %lld, 1, ARRAY[",
                     (long long) coord_gsid, (long long) dtxid);
        for (i = 0; i < nparts && n < (int) sizeof(qry) - 64; i++)
            n += snprintf(qry + n, sizeof(qry) - n, "%s%lld",
                          (i == 0) ? "" : ",", (long long) parts[i]);
        snprintf(qry + n, sizeof(qry) - n, "]::bigint[], %lld)",
                 (long long) dtx_cts);

        r = dtx_remote_scalar(peers[slot].host, peers[slot].port, qry);
        if (r == NULL)
        {
            dtx_master_try_write_abort(coord_gsid, dtxid, parts, nparts);
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: 节点 %d 不是协调组 %lld 的 leader，无法做决议",
                            coord_node, (long long) coord_gsid),
                     errhint("协调组正在选举或 partition_map 未追平；重试本事务即可。")));
        }
        verdict = atoi(r);
        pfree(r);
    }

    if (verdict != 1)
        ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                 errmsg("pg_raft: 分布式事务 %lld 的全局决议是 ABORT", (long long) dtxid),
                 errdetail("协调组 %lld 的日志里已有一条 ABORT 决议（多半是恢复守护"
                           "在推定中止时抢先写入）。决议槽一次性，不可翻盘。",
                           (long long) coord_gsid)));

    elog(DEBUG1, "pg_raft: dtx %lld COMMIT（协调组 %lld，参与组 %d 个）",
         (long long) dtxid, (long long) coord_gsid, nparts);
}

/*
 * 参与者自治登记的落地实现（pg_partdist 经 rendezvous variable
 * "partdist_dtx_note_participant_hook" 调用）。
 *
 * 必须写在**独立事务**里：调用点在 XACT_EVENT_PRE_PREPARE，本事务下一步就要
 * 变成 prepared，写在里面就永远不会有别的会话看得见。独立事务在 PG 里只能靠
 * libpq 自连接完成（与恢复守护跑 COMMIT PREPARED 同一手法）。
 *
 * ★ 登记必须**同步提交**（2026-08-04 审查改正）。初版用了
 * synchronous_commit=off，理由是"崩溃会让 prepared 事务连同登记一起消失"——
 * 这个理由**不成立**：登记发生在 [B]（PREPARE TRANSACTION 的 WAL fsync）
 * **之前**，节点在 [B] 之后崩溃时 prepared 事务是持久的，而异步提交的登记行
 * 可能没落盘。后果有两层：a) master 收齐 ack 后来读写集，本节点答空 ⇒
 * 写集缺一块，协调组选取与 participants[] 都按错的集合算；b) 本节点重启后
 * 这笔 prepared 事务查无登记 ⇒ 拿不到 coord_gsid，只能退回 Citus 规则，
 * 而它的数据其实归协调组决议管辖。同步提交的持久序恰好压住 [B]：
 * 登记落盘 < [B] < ack ⇒ "prepared 存在 ⇒ 登记必在"。一次小事务的 fsync
 * 换写集完整性，值得。
 */
bool
pg_raft_dtx_note_participant(int64 dtxid, const char *gid,
                             const int64 *gsids, int ngsids)
{
    StringInfoData sql;
    PGconn        *conn;
    PGresult      *res;
    bool           ok = false;
    int            i;
    char           selfconn[256];

    if (gid == NULL)
        return true;

    pg_raft_format_conninfo("127.0.0.1", PostPortNumber, selfconn, sizeof(selfconn));
    conn = PQconnectdb(selfconn);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(WARNING, "pg_raft: DTX 参与登记连回本节点失败: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }

    initStringInfo(&sql);
    appendStringInfoString(&sql, "SELECT partdist.dtx_note_participant(");
    appendStringInfo(&sql, "%lld, ", (long long) dtxid);
    {
        char *q = PQescapeLiteral(conn, gid, strlen(gid));

        if (q == NULL)
        {
            pfree(sql.data);
            PQfinish(conn);
            return false;
        }
        appendStringInfoString(&sql, q);
        PQfreemem(q);
    }
    appendStringInfoString(&sql, ", ARRAY[");
    for (i = 0; i < ngsids; i++)
        appendStringInfo(&sql, "%s%lld", (i == 0) ? "" : ",", (long long) gsids[i]);
    appendStringInfoString(&sql, "]::bigint[])");

    res = PQexec(conn, sql.data);
    ok = (PQresultStatus(res) == PGRES_TUPLES_OK ||
          PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok)
        elog(WARNING, "pg_raft: DTX 参与登记失败: %s", PQerrorMessage(conn));
    PQclear(res);
    pfree(sql.data);
    PQfinish(conn);
    return ok;
}

/* _PG_init 调用：装内核补丁 0004 的挂点 + 参与登记挂点 */
void
pg_raft_dtx_install_hooks(void)
{
    void **rv;

    pre_record_commit_hook = dtx_master_pre_record_commit;

    rv = find_rendezvous_variable("partdist_dtx_note_participant_hook");
    *rv = (void *) pg_raft_dtx_note_participant;
}
