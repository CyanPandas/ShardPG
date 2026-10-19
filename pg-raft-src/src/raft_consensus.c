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
#include "access/transam.h"          /* 2026-10-18 方案 B：锚点 full xid */
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
#include "libpq/pqformat.h"          /* 2026-10-17 复制流：二进制批消息 */
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "port/pg_bswap.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "catalog/pg_type.h"
#include "catalog/pg_namespace.h"
#include "access/shard_stamp.h"	/* 补丁 0014：shard_elr_inhibit */
#include "pgstat.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"           /* P7-N41：TopMemoryContext */
#include "utils/resowner.h"           /* P7-N41：合并心跳逐组子事务 */
#include "utils/guc.h"               /* application_name：MX 闸门（T4.4） */

#include <sys/select.h>              /* R-P4-7：有界 RPC 的 select 轮询 */
#include <poll.h>

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
/*
 * 2026-10-16：128 → 1024。数据组一条分区 WAL 记录就是一个条目，一笔 NewOrder 在 order_line
 * 组里就有十来条；128 条的环只装得下十来笔事务，三副本下慢的那个副本落后两三轮就出了环窗口，
 * 提交路径补发不了，只能靠后台追平通道持着认领位同步追（实测提交整组排队）。
 * 代价：每组 1024 × ~0.8 KB ≈ 0.8 MB 共享内存（64 组约 53 MB）。
 */
#define RAFT_LOG_CAPACITY  1024

/* data_entry_apply 返回 false 的原因（写进 RaftLogShmem.last_apply_fail） */
#define RAFT_APPLYFAIL_NONE       0
#define RAFT_APPLYFAIL_NO_PART    1   /* group_local_partition() <= 0：P0 映射没建好 */
#define RAFT_APPLYFAIL_NO_SPI     2   /* 拿不到 SPI */
#define RAFT_APPLYFAIL_SPI_EXEC   3   /* follower_set_applied_part_lsn 执行失败 */
#define RAFT_OP_LEN        32

/* 2026-10-16 诊断：认领位用途（写进 shmem，等太久时连同持有者一起打日志） */
#define RAFT_CLAIM_UNKNOWN      0
#define RAFT_CLAIM_COMMIT_MANY  1
#define RAFT_CLAIM_CATCHUP      2
#define RAFT_CLAIM_DTX_DECIDE   3
#define RAFT_CLAIM_DTX_STATUS   4
#define RAFT_CLAIM_DTX_ACK      5
#define RAFT_CLAIM_DTX_FORGET   6
#define RAFT_CLAIM_COMMIT_ONE   7
static int raft_claim_purpose = RAFT_CLAIM_UNKNOWN;
static const char *const raft_claim_purpose_name[] =
    {"unknown", "commit(many)", "catchup", "dtx_decide", "dtx_status", "dtx_ack", "dtx_forget", "commit(one)"};

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
/*
 * T7.23（P7-R2）：组数上限不再是编译期常量。
 *
 * 原来 `RAFT_MAX_GROUPS = 32` 写死在共享内存布局里，每节点至多 31 个数据组 ——
 * 一张 32 分片的分布表就能把整个节点的组表吃满，再建组只剩一条 WARNING。
 * 现在由 PGC_POSTMASTER GUC `pg_raft.max_groups` 决定组表大小；默认仍是 32，
 * 与改动前逐字节同构，不调它就什么都不变。
 *
 * 为什么不顺手把上限调很高：每个组带一个 RAFT_LOG_CAPACITY 条、每条约 0.8 KB
 * 的日志环，约 104 KB/组 —— 256 个组就是 26 MB 共享内存，9 节点 234 MB。
 * 那是一个要按机器配置来定的数，不该由代码替运维决定。真正把单组成本降下来
 * 需要日志外部化（E1–E4，曾整条回滚），那是另一件事。
 */
#define RAFT_MAX_GROUPS_DEFAULT 32
int         pg_raft_max_groups = RAFT_MAX_GROUPS_DEFAULT;
#define RAFT_MAX_GROUPS     (RaftGroups != NULL ? RaftGroups->max_groups : pg_raft_max_groups)

typedef struct RaftConsensusShmem
{
    slock_t      mutex;
    int          state;
    int64        current_term;
    int          voted_for;
    int          leader_id;
    TimestampTz  election_deadline;
    /*
     * 受控切主（pg_raft_group_campaign）：下一个 tick 无条件发起一次选举，由 tick 读后清零。
     * 不能只把 election_deadline 置到期：现任 leader 的下一次心跳（AE）会
     * reset_election_deadline_locked() 把它推回去，而心跳与本节点 tick 同为 1 s 周期，
     * 实测一次置位常被心跳抢先抹掉、选举根本没发起（N23 回归 [2] 首跑）。
     */
    bool         campaign_pending;
    /*
     * P7-N50（2026-10-03）：最近一次收到**现任 leader** 的 AppendEntries / 心跳的时刻。
     * PreVote 用：这之后一个选举超时之内，本节点不支持任何人预选（leader 明明活着）。
     */
    TimestampTz  last_leader_contact;
} RaftConsensusShmem;

typedef struct RaftLogEntry
{
    int64  index;
    int64  term;
    /*
     * 2026-10-16：把这条写进本节点 raft_log 的事务（follower 批量收条目时置位，其余为
     * Invalid）。同组下一批流水线到达时据此等前一批的事务提交 —— 见 rm_wait_predecessor。
     */
    TransactionId pxid;
    char   op_type[RAFT_OP_LEN];
    char   payload[RAFT_PAYLOAD_MAX];
} RaftLogEntry;

/* ★ P7-N37：控制面条目 apply 连续失败多久之后才放弃（按老语义跳过） */
#define RAFT_CTRL_APPLY_RETRY_MS 60000

/* ★ P7-N39：控制面补提本任期空条目的重试间隔 */
#define RAFT_CTRL_NOOP_RETRY_MS  5000

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
    TimestampTz  peer_last_ack[RAFT_MAX_PEERS];  /* 对端最近一次应答 AppendEntries RPC 的时刻
                                                 * （接受或拒绝都算"活着"；P7-W6 多数派存活
                                                 * 探测，见 pg_raft_group_quorum_alive）。0 = 从未 */
    TimestampTz  peer_last_fail[RAFT_MAX_PEERS]; /* 对端最近一次 RPC 失败（连不上/超时）的时刻。
                                                 * fail 晚于 ack ⇒ 判死，停机的 follower 一个
                                                 * 心跳周期内就能判出，不用等时间窗过期 */
    bool         repl_inited;
    uint64       trunc_gen;         /* 2026-10-16：环尾每被截掉一次 +1（迟到的复制回执据此判作废） */
    int64        leader_durable_index;  /* 2026-10-17 流水线：leader 自己的 raft_log 行已提交落盘到的 index（自投票上限） */
    int64        leader_self_hole;      /* 2026-10-17 流水线：自写行失败的最小 index（0 = 无；修好前自投票不越过它） */
    int64        want_plsn;             /* 2026-10-17 复制流：提交者登记的"要追加到"的 plsn（管理进程替排不上认领位的人追加） */
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

    /*
     * ★ P7-N37（2026-09-20）：控制面 apply 抛错后的重试账本。
     *
     * 原语义是"控制面条目 apply 抛错 ⇒ 当作已应用、游标照推"——一次**瞬时**
     * 失败（锁超时、死锁、临时资源不足）就能让这个节点**永久**错过一条登记。
     * 实测：协调者 apply OP_PARTITION_PRIMARY 撞上一把行锁、300 ms 锁超时，
     * 这条"谁是主"就此丢失，它的路由从此指着旧主，直到该分片下一次切主。
     * （P7-N34 那条"回滚后由下一次 AE 重做"的注释，在老语义下其实并不成立。）
     *
     * 现在：同一条目连续失败先保留游标重试，只有连续失败超过
     * RAFT_CTRL_APPLY_RETRY_MS 才按老语义跳过 —— 真·毒条目仍然堵不死控制面，
     * 瞬时故障不再静默丢登记。计数必须放共享内存：每次 AE RPC 是**不同的
     * backend**，进程内静态变量每轮都归零，等于永不放弃。
     */
    int64        ctrl_fail_index;   /* 正在重试的控制面条目 index（0 = 无） */
    int          ctrl_fail_count;   /* 它连续失败了几次 */
    TimestampTz  ctrl_fail_since;   /* 第一次失败的时刻 */

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
    TimestampTz         last_rm_round;         /* 提交路径（replicate_many）最近一轮完成的时刻（后台追平据此避让） */
    int64               committed_plsn;        /* 流水线：已提交（多数派）的最大数据 plsn（缓存，见 rm_committed_plsn） */
    int64               committed_plsn_idx;    /* 上面那个值是按哪个 commit_index 算的 */
    int                 replicate_purpose;     /* 持有者拿认领位的用途（RAFT_CLAIM_*，诊断用） */
    TimestampTz         replicate_since;       /* 持有者拿到认领位的时刻（诊断用） */
    int                 n_members;
    int                 members[RAFT_MAX_PEERS];

    /*
     * T7.20（P7-R1）：成员变更。
     *
     * 走 Raft 论文 §4.1 的**一次只加/减一个节点**，不做 joint consensus ——
     * 后者只有在需要"原子地换掉多个成员"时才必要，而单节点变更已经能证明
     * 安全：任意一步里新旧两个成员集的多数派**必然相交**（只差一个成员，
     * 两个多数派不可能不相交），所以不会出现两个互不相交的多数派各自选出
     * leader。代价是"三换三"要做三次，而收益是实现面小一个数量级 ——
     * 在一个全簇共识层上，这个取舍偏向能审查得过来的那一边。
     *
     * 生效时机是 **append 而不是 commit**（Raft 的原始规则）：条目一进日志
     * 就按新成员集算多数派。反过来（等提交再生效）会让提交这条 CONFIG 本身
     * 所需的多数派仍按旧集算，而旧集可能已经不再是权威 —— 那正是论文里
     * 反复强调的不安全点。
     *
     * 因为"同时只允许一个未提交的变更"，未决状态只需记一条：
     *   pending_cfg_index > 0 ⇒ 有一条已 append、未提交的 CONFIG；
     *   committed_members[]  ⇒ 回滚目标（日志被截断时退回这里）。
     */
    bool                config_from_log;    /* 成员集已由日志接管，partition_map 不再覆盖 */
    int64               pending_cfg_index;  /* 未提交的 CONFIG 条目 index；0 = 无 */
    int                 n_committed_members;
    int                 committed_members[RAFT_MAX_PEERS];
    /*
     * P7-N41：hardstate 落盘去重。合并心跳之后每台 worker 每秒要处理 ~100 个组的心跳，
     * 原来每条心跳都 open/write/fsync/rename 一次 hardstate —— 而绝大多数心跳里
     * term/vote/commit/applied 一个都没变。这里记下"文件里现在是什么"，没变就不写。
     * 受 cons.mutex 保护。只有在**唯一写者**完成时才记账：并发写者的 rename 先后
     * 不可知，有过重叠就把记账作废（下一次必写），保证 hs_saved_valid ⇒ 文件内容 == hs_saved_*。
     */
    int                 hs_writers;
    bool                hs_overlap;
    bool                hs_saved_valid;
    int64               hs_saved_term;
    int                 hs_saved_vote;
    int64               hs_saved_commit;
    int64               hs_saved_applied;
    /*
     * 2026-10-16（pg_raft.data_hardstate_lazy）：上一次 **fsync 过** 的 term / vote / 压缩基点。
     * 数据组这三项与它相同、只有 commit / applied 变化时，文件每秒至多重写一次且不 fsync。
     * 受 cons.mutex 保护；文件写入本身由 pg_raft_hardstate 锁按组串行化。
     */
    bool                hs_dur_valid;
    int64               hs_dur_term;
    int                 hs_dur_vote;
    int64               hs_dur_base;
    TimestampTz         hs_lazy_ts;
    /*
     * P7-N41：raft_log.committed 已标到哪。原来每条心跳都跑一次
     * UPDATE ... WHERE committed=false（百组 × 每秒），绝大多数一行都不改。
     * 该列只是重启恢复 commit_index 的辅助（权威在 hardstate），按水位跳过即可；
     * 截断 / 删行时水位退回。
     */
    int64               sql_marked_upto;
    /*
     * P7-N57（2026-10-04）：本节点**已知**没有该组的分片副本（拒收过数据条目，或当选后
     * 升主前置判了不可升主）。这种成员不主动竞选 —— 否则两个都没数据的成员之间互相投票、
     * 轮流当选再让位，唯一有数据的成员抽签式地才轮得到（演示二实测 105499 号组从任期 1
     * 漂到 23）。收到数据条目且本地已有分片时清掉。只在 tick 里读、在 RPC 后端里写，布尔量无需加锁。
     */
    bool                no_local_shard;
    /*
     * ★ P7-N46 根因（2026-10-04）：SQL 日志要从哪个 index 起按环重写（0 = 不需要）。
     * 写 raft_log 行的事务（AppendEntries RPC / 提案会话）后来回滚时，行随之撤销，
     * 环里的条目却还在 ⇒ SQL 日志出洞。回滚回调在这里记下最小受影响 index，
     * 下一个有 SPI 的落盘点（persist_log_entry_sql）按环把那一段补写回去。
     */
    int64               repersist_from;
    /*
     * ★ P7-N62（2026-10-04）：本组最近一次以 leader 身份跑 tick 的时刻（0 = 本次启动以来没当过）。
     * 供 partdist 的读闸门判"本机此刻（或刚刚）是不是该组 leader"：被断电后重新拉起的旧主
     * 本地还带着持久的"已升主"标记，路由若仍指着它，它会拿断电前的旧数据回答读。
     */
    TimestampTz         leader_seen_at;
    RaftConsensusShmem  cons;
    RaftLogShmem        log;
} RaftGroupState;

typedef struct RaftGroupTable
{
    slock_t         mutex;              /* 仅保护注册表本身（in_use/group_id/members） */
    int             n_groups;
    int             max_groups;         /* T7.23：建表时的 pg_raft.max_groups，此后恒定 */
    /*
     * ★ P7-N21（2026-09-19）：显式拆过的组的墓碑。hearsay（收到对端 RV/AE 时按对端通告
     * 自动建组）不许把墓碑里的组再建出来；显式 pg_raft_group_create 清墓碑。受 mutex 保护。
     */
    int64           tomb_gid[64];
    TimestampTz     tomb_at[64];
    int             tomb_next;
    /* 2026-10-17 复制流：管理进程（pg_raft_stream_main）的 pid / latch，提交者等待的条件变量 */
    pid_t           stream_pid;
    Latch          *stream_latch;
    ConditionVariable stream_cv;
    pid_t           stream_in_pid[64];  /* 副本端：来自各 leader（按节点号取模）的当前复制流会话 pid */
    /*
     * 2026-10-17（路线 A）：节点地址覆盖表。节点号不变、物理地址随切主迁移（备库提升后接替原节点号）。
     * pg_raft.peers 是启动参数改不了，这里按节点号覆盖其地址；addr_gen 每改一次 +1，parse_peers 据此重套。
     * 受 mutex 保护。内容来自 partdist.node_address（控制面 OP_NODE_ADDRESS 写入，维护进程定期同步）。
     */
    uint32          addr_gen;
    int             addr_n;
    int             addr_node[RAFT_MAX_PEERS];
    int             addr_port[RAFT_MAX_PEERS];
    char            addr_host[RAFT_MAX_PEERS][256];
    pg_atomic_uint64 stream_wait_n;     /* 提交路径等复制提交：次数 / 合计微秒 / 最大微秒（管理进程每 10 s 清零） */
    pg_atomic_uint64 stream_wait_us;
    pg_atomic_uint64 stream_wait_max;
    RaftGroupState  groups[FLEXIBLE_ARRAY_MEMBER];
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
/* 2026-10-18：原子性验证用的暂停点（默认关；见 dtx_test_pause_after_decide） */
int   pg_raft_dtx_test_pause_point    = 0;
int   pg_raft_dtx_test_pause_ms       = 0;
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
PG_FUNCTION_INFO_V1(pg_raft_group_campaign);
PG_FUNCTION_INFO_V1(pg_raft_group_reset);
PG_FUNCTION_INFO_V1(pg_raft_group_status);
PG_FUNCTION_INFO_V1(pg_raft_group_propose);
PG_FUNCTION_INFO_V1(pg_raft_data_propose);
PG_FUNCTION_INFO_V1(pg_raft_group_change_member);

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
                              RaftGroupCtx *ctx, bool hearsay);
static bool group_membership_known(RaftGroupCtx *ctx);
static bool group_resolve_membership(RaftGroupCtx *ctx);
static void replicate_group_upto(RaftGroupCtx *ctx, int64 cur_plsn, Oid partition_id);
static bool rs_stream_active(void);       /* 2026-10-17 复制流 */
static bool rs_stream_data(RaftGroupCtx *ctx);
static void rs_replicate_claimed(RaftGroupCtx *ctx, int64 cur_plsn, Oid partition_id);
static void pg_raft_partwal_replicate_stream(const Oid *parts, int nparts, bool *failed_out);
#define RS_MSG_MAGIC 0x52535431          /* "RST1"：复制流批消息 */
static void restore_groups_if_needed(void);
static char *data_entry_fetch_hex(RaftGroupCtx *ctx, int64 partition_lsn);
static bool data_entry_present_locally(RaftGroupCtx *ctx, const char *payload);
static bool data_entry_store(RaftGroupCtx *ctx, const char *payload,
                             const char *data_hex, bool fresh_append);
static int64 group_local_partition(RaftGroupCtx *ctx);

/*
 * 2026-10-16：follower 批量收条目（pg_raft_append_entries_batch）期间的批内状态。
 * 原来每条条目各自跑七八条 SPI（查本地分片号 ×3、查本地流末尾、写 raft_log 行、标 committed、
 * apply 推进……），每条都要过一遍 Citus 规划器，实测每条约 10 ms —— 这就是三副本下每个组
 * 每秒只能复制约一百条记录的天花板。批内：本地分片号与本地流末尾各查一次后在批内维护，
 * raft_log 行攒到批尾一条多行 INSERT，hardstate / 标 committed / apply 在批尾各做一次。
 * 只在本函数自己的调用栈内有效，出口（含 ERROR）一律清掉。
 */
static bool   raft_batch_active = false;
static int64  raft_batch_lp_gid = 0;
static int64  raft_batch_lp_oid = 0;
static int64  raft_batch_flush = -1;        /* 本地流已写到的 plsn（批内维护，-1 = 未知） */
static int    raft_batch_nrows = 0;
static int64  raft_batch_row_idx[RAFT_LOG_CAPACITY];
static char  *raft_batch_row_pl[RAFT_LOG_CAPACITY];
static int64  raft_batch_row_term = 0;

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
static bool raft_config_persist(RaftGroupCtx *ctx, const char *payload);
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
    return MAXALIGN(offsetof(RaftGroupTable, groups) +
                    mul_size((Size) pg_raft_max_groups, sizeof(RaftGroupState)));
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
    g->log.ctrl_fail_index = 0;
    g->log.ctrl_fail_count = 0;
    g->log.ctrl_fail_since = 0;
    memset(g->log.peer_last_ack, 0, sizeof(g->log.peer_last_ack));
    memset(g->log.peer_last_fail, 0, sizeof(g->log.peer_last_fail));

    g->last_data_plsn = 0;
    g->committed_plsn = 0;
    g->committed_plsn_idx = 0;
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
        memset(RaftGroups, 0, pg_raft_consensus_shmem_size());
        SpinLockInit(&RaftGroups->mutex);
        RaftGroups->max_groups = pg_raft_max_groups;
        RaftGroups->n_groups = 0;
        ConditionVariableInit(&RaftGroups->stream_cv);
        pg_atomic_init_u64(&RaftGroups->stream_wait_n, 0);
        pg_atomic_init_u64(&RaftGroups->stream_wait_us, 0);
        pg_atomic_init_u64(&RaftGroups->stream_wait_max, 0);
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
/* P7-N21：拆组墓碑（见 raft_group_ensure） */
static void
raft_tombstone_add(int64 group_id)
{
    if (RaftGroups == NULL || group_id == RAFT_CONTROL_GROUP)
        return;
    SpinLockAcquire(&RaftGroups->mutex);
    RaftGroups->tomb_gid[RaftGroups->tomb_next] = group_id;
    RaftGroups->tomb_at[RaftGroups->tomb_next] = GetCurrentTimestamp();
    RaftGroups->tomb_next = (RaftGroups->tomb_next + 1) % lengthof(RaftGroups->tomb_gid);
    SpinLockRelease(&RaftGroups->mutex);
}

static void
raft_tombstone_clear(int64 group_id)
{
    int i;

    if (RaftGroups == NULL)
        return;
    SpinLockAcquire(&RaftGroups->mutex);
    for (i = 0; i < lengthof(RaftGroups->tomb_gid); i++)
        if (RaftGroups->tomb_gid[i] == group_id)
            RaftGroups->tomb_gid[i] = 0;
    SpinLockRelease(&RaftGroups->mutex);
}

static bool
raft_group_ensure(int64 group_id, const int *members, int n_members,
                  RaftGroupCtx *ctx, bool hearsay)
{
    int i;

    if (RaftGroups == NULL)
        return false;

    if (raft_group_ctx(group_id, ctx))
    {
        /* 已存在：允许更新成员集（控制面下发配置） */
        if (members != NULL && n_members > 0)
        {
            /*
             * ★★ T7.20：成员集一旦由日志接管，这里**不许再覆盖**。
             *
             * 实测踩到（2026-09-12）：`restore_groups_if_needed()` 的
             * `groups_restored` 是**每后端**静态量 —— 于是每开一个新 backend
             * 就会拿 `partdist.raft_group` 里那一行去重置 shmem 成员集。
             * 成员变更刚把成员集改成 {2,3,4,5} 并提交，紧接着一条普通查询
             * （新 backend）就把它打回 {2,3,4}，而且**一声不响**。
             * 症状是"加成员报成功、隔一会儿又变回去"，看起来像共识没生效。
             *
             * 注册表是**重启**恢复成员集的来源，不是运行期的权威；运行期的
             * 权威是日志。两者只在 apply 那一刻由 raft_config_persist 对齐。
             */
            SpinLockAcquire(&RaftGroups->mutex);
            if (!ctx->g->config_from_log)
            {
                ctx->g->n_members = 0;
                for (i = 0; i < n_members && ctx->g->n_members < RAFT_MAX_PEERS; i++)
                    ctx->g->members[ctx->g->n_members++] = members[i];
            }
            SpinLockRelease(&RaftGroups->mutex);
        }
        return true;
    }

    /*
     * ★ P7-N21（2026-09-19）：刚被显式拆掉的组，不许 hearsay 再建出来。
     *
     * 拆组是节点本地动作，在途的 RV/AE 会在已拆的节点上把组按通告**再建一个空壳**：
     * 日志为空、last_data_plsn = 0。实测（09-18 15:16）清场时 102756/102757 在三台上被
     * 这样复活，:5434 当上这个"僵尸组"的 leader；同一时刻它正在 DROP 该组分片表的壳，
     * 提交路径上的 DROP 通知要经该组复制 ⇒ replicate_group_upto 从 plsn 1 起把整条
     * 分区流历史（800 条）逐条重提，领导权还在三台间来回抖，一条 DROP 卡了 13 分钟
     * （更早一次 fm_* 表几千条历史，卡 56 分钟）—— 这就是 P7-N21。
     * 显式建组（pg_raft_group_create）清墓碑，合法重建不受影响；墓碑 10 分钟后失效。
     */
    if (hearsay)
    {
        bool tomb = false;

        SpinLockAcquire(&RaftGroups->mutex);
        for (i = 0; i < lengthof(RaftGroups->tomb_gid); i++)
            if (RaftGroups->tomb_gid[i] == group_id &&
                !TimestampDifferenceExceeds(RaftGroups->tomb_at[i],
                                            GetCurrentTimestamp(), 600 * 1000))
            {
                tomb = true;
                break;
            }
        SpinLockRelease(&RaftGroups->mutex);
        if (tomb)
            return false;
    }
    else
        raft_tombstone_clear(group_id);

    SpinLockAcquire(&RaftGroups->mutex);

    /*
     * ★ P7-N36（2026-09-20）：**在锁内复查一次**。
     *
     * 上面那次"组在不在"的检查（raft_group_ctx）是**不持锁**的，而分配空槽在锁内 ——
     * 两个后端同时为同一个 group_id 走到这里，就会各插一个槽位。节点重启时最容易撞上：
     * 每个后端都会 restore_groups_if_needed()（groups_restored 是**每后端**静态量），
     * 同时对端的 AE/RV 又按 hearsay 建组。实测 w2 重启后同一毫秒里两个后端各建了一次
     * 组 102895（槽位 2 与槽位 4）。
     *
     * 后果不是"多占一个槽位"这么轻：多出来的那个槽是**独立的状态机**，它自己的选举
     * 计时器一到就发起竞选、抬高任期，把该组真正的 leader 一次次逼下台（实测一个组的
     * 任期被抬到 12，写入反复撞"本节点不是该分区组的 leader"）。
     */
    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        if (RaftGroups->groups[i].in_use &&
            RaftGroups->groups[i].group_id == group_id)
        {
            SpinLockRelease(&RaftGroups->mutex);
            return raft_group_ctx(group_id, ctx);    /* 别人刚建好，用它那个 */
        }
    }

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

    /*
     * T7.20：成员集一旦由日志接管，**partition_map 不再是权威**。
     * 走到这里说明当前成员集为空，而 config_from_log 为真意味着日志里那条
     * CONFIG 把成员减到了 0 —— 那是不该发生的（变更接口拦了空集），
     * 此时从 partition_map 补一份回去只会制造两套不一致的多数派定义。
     */
    if (ctx->g->config_from_log)
        return false;

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

/* T7.20（P7-R1）：成员变更条目。payload = {"members":[a,b,c]} */
#define RAFT_OP_CONFIG "OP_CONFIG"

/*
 * 从 CONFIG 载荷里解出成员列表。
 *
 * 只扫**第一个 `[` 与其后第一个 `]` 之间**的整数。这么写是有意的：载荷是本模块
 * 自己生成的、形如 `{"members":[2,3,4]}`，而它会经 `partdist.raft_log.payload`
 * （jsonb）往返一次被规范化（键序、空格都可能变），按文本比对或按固定偏移取值
 * 都不可靠 —— 只认括号里的数字，两种形态都能吃下。
 * 反过来这也是一条约束：**CONFIG 载荷里不许再出现第二个数组或别的数字字段**。
 */
static int
raft_config_parse(const char *payload, int *out)
{
    const char *p = payload;
    int         n = 0;

    if (payload == NULL)
        return 0;
    while (*p && *p != '[')
        p++;
    if (*p != '[')
        return 0;
    p++;
    while (*p && *p != ']' && n < RAFT_MAX_PEERS)
    {
        if (*p >= '0' && *p <= '9')
        {
            int v = 0;

            while (*p >= '0' && *p <= '9')
                v = v * 10 + (*p++ - '0');
            out[n++] = v;
        }
        else
            p++;
    }
    return n;
}

/* 把成员列表写成 CONFIG 载荷（JSON 数组，供 jsonb 列收下） */
static void
raft_config_format(const int *members, int n, StringInfo out)
{
    int i;

    appendStringInfoString(out, "{\"members\":[");
    for (i = 0; i < n; i++)
        appendStringInfo(out, "%s%d", i ? "," : "", members[i]);
    appendStringInfoString(out, "]}");
}

/*
 * 一条 CONFIG 条目进了日志 —— **立刻**生效（Raft 的 append-time 规则）。
 *
 * 不在 log->mutex 里做：成员集由 RaftGroups->mutex 罩着，两把自旋锁嵌套没有
 * 必要也不安全，调用方一律在释放日志锁之后再调本函数。
 */
static void
raft_config_apply(RaftGroupCtx *ctx, int64 index, const char *payload)
{
    int members[RAFT_MAX_PEERS];
    int n = raft_config_parse(payload, members);
    int i;

    if (n <= 0)
    {
        elog(WARNING, "pg_raft: 组 %lld 的 CONFIG 条目 %lld 解不出成员集，忽略",
             (long long) ctx->group_id, (long long) index);
        return;
    }

    SpinLockAcquire(&RaftGroups->mutex);
    /* 第一次被日志接管时，把当时的成员集记作"已提交态"，作为回滚目标 */
    if (!ctx->g->config_from_log)
    {
        ctx->g->n_committed_members = ctx->g->n_members;
        for (i = 0; i < ctx->g->n_members; i++)
            ctx->g->committed_members[i] = ctx->g->members[i];
        ctx->g->config_from_log = true;
    }
    ctx->g->n_members = n;
    for (i = 0; i < n; i++)
        ctx->g->members[i] = members[i];
    ctx->g->pending_cfg_index = index;
    SpinLockRelease(&RaftGroups->mutex);

    elog(LOG, "pg_raft: 组 %lld 成员集经日志条目 %lld 生效，共 %d 个成员（未提交）",
         (long long) ctx->group_id, (long long) index, n);
}

/*
 * 日志被截断到 new_last 之后：未提交的那条 CONFIG 若被截掉，成员集必须退回
 * 上一个**已提交**的配置。
 *
 * 不退回的后果不是"多算一个成员"，而是**本节点与其它节点对多数派的定义不一致** ——
 * 与 group_membership_known() 头注释里那个 hearsay 事故同一个形态。
 */
static void
raft_config_rollback(RaftGroupCtx *ctx, int64 new_last)
{
    bool reverted = false;
    int  i;

    /* 无锁快路径：见 raft_config_note_commit 的注释 */
    if (ctx->g->pending_cfg_index == 0)
        return;

    SpinLockAcquire(&RaftGroups->mutex);
    if (ctx->g->pending_cfg_index > 0 && ctx->g->pending_cfg_index > new_last)
    {
        ctx->g->n_members = ctx->g->n_committed_members;
        for (i = 0; i < ctx->g->n_committed_members; i++)
            ctx->g->members[i] = ctx->g->committed_members[i];
        ctx->g->pending_cfg_index = 0;
        reverted = true;
    }
    SpinLockRelease(&RaftGroups->mutex);

    if (reverted)
        elog(LOG, "pg_raft: 组 %lld 的未提交成员变更随日志截断（到 %lld）回滚",
             (long long) ctx->group_id, (long long) new_last);
}

/* 未提交的 CONFIG 落在提交点之下 ⇒ 变更定案，当前成员集升为"已提交态" */
static void
raft_config_note_commit(RaftGroupCtx *ctx, int64 commit_index)
{
    bool settled = false;
    int  i;

    /*
     * ★★ 无锁快路径，**不是优化而是必需**（2026-09-12 实测，A/B 对照定因）。
     *
     * 本函数挂在两条**最热**的路上：每一次 group_propose（提交路径上，每条
     * parwal 记录一次）和每一次 pg_raft_append_entries（每个心跳每个组一次）。
     * 第一版无条件去抢 `RaftGroups->mutex` —— 那是**全局**的组表自旋锁，被
     * 全部 32 个组、全部 backend 共用。在这台 2 核机上，把它压进提交路径的
     * 结果是心跳被挤掉、选举频繁触发：实测 `replica_gate_p6` 由 33/0 掉到
     * 23/10 与 32/1（两轮），症状是"分区组 leader 就位"刚断言完，紧接着的
     * 本地写入就被"本节点不是该分区组的 leader"拒掉。改前的同一套 33/0。
     *
     * pending_cfg_index 只在持锁时被写，无锁读它做**早退判据**是安全的：
     * 读到 0 说明此刻没有未决变更，不必进锁；真有变更时读到非 0，再进锁复核。
     * 成员变更是罕见操作，慢路径多一次锁毫无代价。
     */
    if (ctx->g->pending_cfg_index == 0)
        return;

    SpinLockAcquire(&RaftGroups->mutex);
    if (ctx->g->pending_cfg_index > 0 &&
        ctx->g->pending_cfg_index <= commit_index)
    {
        ctx->g->n_committed_members = ctx->g->n_members;
        for (i = 0; i < ctx->g->n_members; i++)
            ctx->g->committed_members[i] = ctx->g->members[i];
        ctx->g->pending_cfg_index = 0;
        settled = true;
    }
    SpinLockRelease(&RaftGroups->mutex);

    if (settled)
    {
        StringInfoData payload;

        elog(LOG, "pg_raft: 组 %lld 的成员变更已提交，成员集定案",
             (long long) ctx->group_id);

        /*
         * ★ 定案即落注册表，**不等 apply**。
         *   apply 只在拿得到 apply 认领的那个 backend 上推进，follower 上
         *   未必及时跑到；而注册表是重启恢复成员集的来源 —— 落后一拍就意味着
         *   "重启后恢复出旧成员集"，也就是两套多数派定义。
         *   raft_config_persist 内部拿不到 SPI 会自己返回 false，不致报错。
         */
        SpinLockAcquire(&RaftGroups->mutex);
        i = ctx->g->n_members;
        SpinLockRelease(&RaftGroups->mutex);
        if (i > 0)
        {
            int cur[RAFT_MAX_PEERS];
            int k;

            SpinLockAcquire(&RaftGroups->mutex);
            for (k = 0; k < i && k < RAFT_MAX_PEERS; k++)
                cur[k] = ctx->g->members[k];
            SpinLockRelease(&RaftGroups->mutex);

            initStringInfo(&payload);
            raft_config_format(cur, i, &payload);
            (void) raft_config_persist(ctx, payload.data);
            pfree(payload.data);
        }
    }
}

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

    if (raft_batch_active && raft_batch_lp_gid == ctx->group_id && raft_batch_lp_oid > 0)
        return raft_batch_lp_oid;

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
    if (raft_batch_active && local_oid > 0)
    {
        raft_batch_lp_gid = ctx->group_id;
        raft_batch_lp_oid = local_oid;
    }
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
 * ★ 2026-10-12：follower 侧 —— leader 发来"只有头、没有字节"的数据条目（leader 本地 pg_parwal 里
 * 那条记录没了，见 replicate_to_peer）时，看本节点自己的流里是不是**已经有这条**：同一个
 * partition_lsn、头部（orig_lsn / rmid / info / flags / gxid）逐项相同 ⇒ 字节早已在本地落盘，
 * 可以收下条目并 ack（"多数派 ack == 多数派字节已持久化"不变）。否则 false（不收、不 ack）。
 *
 * 实测（演示二 P059）：条目 31..35 是原始主 w3 自己当主时写的、已提交；w1/w4 的日志里有、
 * 本地流里的字节却被半程的全量基线截掉了；w3 的日志被回滚掉了这几条、字节却都在。
 * 原来 leader 取不到字节就只发心跳，w3 永远追不上、整组无主。
 */
/* 2026-10-12：本节点流里最后一条已落盘记录的 partition_lsn（-1 = 查不到） */
static int64
data_local_flush_plsn(RaftGroupCtx *ctx)
{
    StringInfoData sql;
    bool           spi_owned;
    int64          local_oid;
    int64          v = -1;

    if (raft_batch_active && raft_batch_flush >= 0)
        return raft_batch_flush;
    local_oid = group_local_partition(ctx);

    if (local_oid <= 0 || !raft_persist_spi_begin(&spi_owned))
        return -1;
    initStringInfo(&sql);
    appendStringInfo(&sql, "SELECT partdist.get_partition_flush_lsn(%u::oid)", (unsigned) local_oid);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

        if (!isnull)
            v = DatumGetInt64(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    if (raft_batch_active)
        raft_batch_flush = v;
    return v;
}

static bool
data_entry_present_locally(RaftGroupCtx *ctx, const char *payload)
{
    StringInfoData sql;
    bool           spi_owned;
    bool           same = false;
    int64          local_oid;
    int64          plsn = entry_partition_lsn(payload);
    char           orig_lsn[64];
    long long      gxid = 0;
    int            rmid = 0;
    int            info = 0;
    int            flags = 0;
    const char    *p;

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0 || plsn <= 0)
        return false;

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
    p = strstr(payload, "\"gxid\"");
    if (p != NULL && (p = strchr(p, ':')) != NULL)
        (void) sscanf(p + 1, " %lld", &gxid);

    if (!raft_persist_spi_begin(&spi_owned))
        return false;
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT (r.orig_lsn = '%s'::pg_lsn AND r.rmid = %d AND r.info = %d "
                     "        AND r.flags = %d AND r.gxid = %lld)::text "
                     "FROM partdist.partwal_read_record(%u::oid, %lld) r",
                     orig_lsn, rmid, info, flags, gxid,
                     (unsigned) local_oid, (long long) plsn);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

        same = !isnull && strcmp(TextDatumGetCString(d), "true") == 0;
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return same;
}

/*
 * 2026-10-17 复制流：副本端按组攒记录、批尾一次落盘（pg_partdist 的按组批量入口，
 * 一次 PartWALCtl->lock、一个写入器）。逐条走 SQL 入口时每条都要单独抢那把全节点排他锁，
 * 本节点自己的提交也在抢 —— 复制流在副本上是单会话顺序处理，这些等待会全部串行累加。
 * 攒着的记录在三种时刻必须先落下：批尾（ack 之前）、批内 apply 之前、冲突截断之前。
 */
typedef void (*PartWALFollowerAppendBatchFn) (Oid partition_id, int n, const int64 *plsn,
                                              const XLogRecPtr *orig_lsn, const int *rmid,
                                              const int *info, const int *flags,
                                              const int64 *gxid, char *const *data,
                                              const uint32 *data_len, const bool *fresh);
static PartWALFollowerAppendBatchFn rs_dq_hook = NULL;
static bool         rs_dq_active = false;
static Oid          rs_dq_oid = InvalidOid;
static int          rs_dq_n = 0;
static int          rs_dq_cap = 0;
static int64       *rs_dq_plsn;
static XLogRecPtr  *rs_dq_orig;
static int         *rs_dq_rmid, *rs_dq_info, *rs_dq_flags;
static int64       *rs_dq_gxid;
static char       **rs_dq_data;
static uint32      *rs_dq_len;
static bool        *rs_dq_fresh;

static bool
rs_dq_begin(void)
{
    if (rs_dq_hook == NULL)
    {
        void **p = find_rendezvous_variable("partdist_partwal_follower_append_batch_hook");

        rs_dq_hook = (PartWALFollowerAppendBatchFn) *p;
    }
    rs_dq_n = 0;
    rs_dq_oid = InvalidOid;
    rs_dq_active = (rs_dq_hook != NULL);
    return rs_dq_active;
}

static void
rs_dq_flush(void)
{
    int n = rs_dq_n;

    if (!rs_dq_active || n == 0)
        return;
    rs_dq_n = 0;
    rs_dq_hook(rs_dq_oid, n, rs_dq_plsn, rs_dq_orig, rs_dq_rmid, rs_dq_info, rs_dq_flags,
               rs_dq_gxid, rs_dq_data, rs_dq_len, rs_dq_fresh);
}

static void
rs_dq_end(void)
{
    rs_dq_active = false;
    rs_dq_n = 0;
    rs_dq_oid = InvalidOid;
}

static void
rs_dq_push(Oid part, int64 plsn, XLogRecPtr orig, int rmid, int info, int flags, int64 gxid,
           char *data, uint32 len, bool fresh)
{
    if (rs_dq_n > 0 && part != rs_dq_oid)
        rs_dq_flush();
    if (rs_dq_n >= rs_dq_cap)
    {
        int nc = Max(rs_dq_cap * 2, 256);

#define RS_DQ_GROW(arr, T) \
        arr = (T *) (rs_dq_cap == 0 ? MemoryContextAlloc(TopMemoryContext, sizeof(T) * nc) \
                                    : repalloc(arr, sizeof(T) * nc))
        RS_DQ_GROW(rs_dq_plsn, int64);
        RS_DQ_GROW(rs_dq_orig, XLogRecPtr);
        RS_DQ_GROW(rs_dq_rmid, int);
        RS_DQ_GROW(rs_dq_info, int);
        RS_DQ_GROW(rs_dq_flags, int);
        RS_DQ_GROW(rs_dq_gxid, int64);
        RS_DQ_GROW(rs_dq_data, char *);
        RS_DQ_GROW(rs_dq_len, uint32);
        RS_DQ_GROW(rs_dq_fresh, bool);
#undef RS_DQ_GROW
        rs_dq_cap = nc;
    }
    rs_dq_oid = part;
    rs_dq_plsn[rs_dq_n] = plsn;
    rs_dq_orig[rs_dq_n] = orig;
    rs_dq_rmid[rs_dq_n] = rmid;
    rs_dq_info[rs_dq_n] = info;
    rs_dq_flags[rs_dq_n] = flags;
    rs_dq_gxid[rs_dq_n] = gxid;
    rs_dq_data[rs_dq_n] = data;
    rs_dq_len[rs_dq_n] = len;
    rs_dq_fresh[rs_dq_n] = fresh;
    rs_dq_n++;
}

/*
 * Follower 侧：把随 AppendEntries 收到的字节原样落盘到**本节点自己的**
 * pg_parwal/<local_oid>/（local_oid 与 leader 不同，由 P0 映射解析）。
 * 返回 false 表示未能落盘 —— 此时不得 ack。
 */
static bool
data_entry_store(RaftGroupCtx *ctx, const char *payload, const char *data_hex,
                 bool fresh_append)
{
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

    /* 复制流批内：攒起来，批尾按组一次落盘（见 rs_dq_*） */
    if (rs_dq_active)
    {
        size_t      hlen = (data_hex != NULL) ? strlen(data_hex) : 0;
        char       *raw = (char *) palloc(hlen / 2 + 1);
        int64       sp = entry_partition_lsn(payload);

        if (hlen > 0)
            (void) hex_decode(data_hex, hlen, raw);
        rs_dq_push((Oid) local_oid, sp,
                   DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(orig_lsn))),
                   rmid, info, flags, (int64) gxid, raw, (uint32) (hlen / 2), fresh_append);
        if (raft_batch_active && raft_batch_flush >= 0 && (fresh_append || sp > raft_batch_flush))
            raft_batch_flush = sp;
        return true;
    }

    if (!raft_persist_spi_begin(&spi_owned))
        return false;

    /*
     * 必须把 leader 指定的 partition_lsn 一并传下去：follower 按该编号落盘，
     * 否则本地自增计数器会和 leader 的编号空间错位（本节点同一 pg_parwal 树下
     * 还有它作为 primary 的本地 demux 写入），applied_part_lsn 将指向本地
     * 不存在的记录。
     *
     * T7.24（R-P4-13 根因）：新追加走 _fresh 入口。编号被本地孤儿记录占住时，
     * 只有新追加才许在 parwal 侧截断重写 —— 本节点 raft 日志里没有更靠后的
     * 条目，流里更靠后的记录必然是孤儿；重传则只标分叉、不截断
     * （截了 raft 日志却没截，leader 不会重发，那段字节就丢了）。
     *
     * 2026-10-16：改用按后端缓存的预备计划 + 参数（字节在 C 里解十六进制）。原来每条
     * 条目拼一条 SQL 文本、过一遍解析与 Citus 规划器，是 follower 每条约 1 ms 的大头。
     */
    {
        static SPIPlanPtr des_plan[2] = {NULL, NULL};
        int         which = fresh_append ? 1 : 0;
        Datum       vals[8];
        char        nulls[9] = "        ";
        size_t      hlen = (data_hex != NULL) ? strlen(data_hex) : 0;
        bytea      *raw = (bytea *) palloc(VARHDRSZ + hlen / 2);

        SET_VARSIZE(raw, VARHDRSZ + hlen / 2);
        if (hlen > 0)
            (void) hex_decode(data_hex, hlen, VARDATA(raw));
        if (des_plan[which] == NULL)
        {
            Oid         argtypes[8] = {OIDOID, INT8OID, PG_LSNOID, INT4OID, INT4OID,
                                       INT4OID, INT8OID, BYTEAOID};
            SPIPlanPtr  pl;

            pl = SPI_prepare(which
                             ? "SELECT partdist.partwal_follower_append_fresh($1, $2, $3, $4, $5, $6, $7, $8)"
                             : "SELECT partdist.partwal_follower_append($1, $2, $3, $4, $5, $6, $7, $8)",
                             8, argtypes);
            if (pl == NULL)
                elog(ERROR, "pg_raft: 预备 follower 落盘语句失败：%s",
                     SPI_result_code_string(SPI_result));
            if (SPI_keepplan(pl) != 0)
                elog(ERROR, "pg_raft: 保存 follower 落盘计划失败");
            des_plan[which] = pl;
        }
        vals[0] = ObjectIdGetDatum((Oid) local_oid);
        vals[1] = Int64GetDatum(entry_partition_lsn(payload));
        vals[2] = DirectFunctionCall1(pg_lsn_in, CStringGetDatum(orig_lsn));
        vals[3] = Int32GetDatum(rmid);
        vals[4] = Int32GetDatum(info);
        vals[5] = Int32GetDatum(flags);
        vals[6] = Int64GetDatum((int64) gxid);
        vals[7] = PointerGetDatum(raw);
        ok = (SPI_execute_plan(des_plan[which], vals, nulls, false, 1) == SPI_OK_SELECT &&
              SPI_processed > 0);
        pfree(raw);
    }
    raft_persist_spi_end(spi_owned);

    if (!ok)
        elog(WARNING, "pg_raft: group %lld parwal 落盘失败，拒绝 ack",
             (long long) ctx->group_id);
    else if (raft_batch_active && raft_batch_flush >= 0)
    {
        int64 sp = entry_partition_lsn(payload);

        /* 新追加：流尾就是它（_fresh 可能截掉了更靠后的孤儿）；重传：去重 no-op，只升不降 */
        if (fresh_append || sp > raft_batch_flush)
            raft_batch_flush = sp;
    }
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
        else if (SPI_processed == 0)
        {
            /*
             * ★ R-P4-13 诊断：插入 0 行此前**一声不响** —— SPI_OK_INSERT 照样
             *   返回，于是"apply 游标推进了、决议行却没有"这件事从日志上完全
             *   看不出来（原登记里"日志里没有任何 DECISION 登记痕迹，既无成功
             *   记录也无失败告警"说的正是这一格）。
             *   0 行有两种可能，处置完全相反：① 行已经在（ON CONFLICT 生效，
             *   正常）；② 本节点字节里**读不出**这条 DTX 记录（WHERE d.dtxid IS
             *   NOT NULL 把它滤掉）—— 那才是缺陷。分开报。
             */
            bool readable = false;
            bool exists = false;

            pfree(sql.data);
            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "SELECT (d.dtxid IS NOT NULL), "
                             "       EXISTS (SELECT 1 FROM partdist.dtx_decision dd "
                             "               WHERE dd.dtxid = d.dtxid) "
                             "FROM partdist.partwal_read_dtx_record(%u::oid, %lld) d",
                             (unsigned) local_oid, (long long) plsn);
            if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
            {
                bool isnull;

                readable = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
                                                      SPI_tuptable->tupdesc, 1, &isnull)) && !isnull;
                exists = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
                                                    SPI_tuptable->tupdesc, 2, &isnull)) && !isnull;
            }
            if (!readable)
                elog(WARNING,
                     "pg_raft: R-P4-13 组 %lld 的 DECISION 条目(plsn=%lld)在本节点字节里读不出"
                     "（local_oid=%u）—— 决议行未登记",
                     (long long) ctx->group_id, (long long) plsn, (unsigned) local_oid);
            else if (!exists)
                elog(WARNING,
                     "pg_raft: R-P4-13 组 %lld 的 DECISION 条目(plsn=%lld)读得出却插不进"
                     "（local_oid=%u）",
                     (long long) ctx->group_id, (long long) plsn, (unsigned) local_oid);
        }
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

/* ---------------- 2026-10-17（路线 A）：pg_raft 的 WAL 资源管理器 ---------------- */
#define RAFT_RMGR_ID            148
#define RAFT_RMGR_NAME          "pg_raft"
#define XLOG_RAFT_HARDSTATE     0x00

typedef struct xl_raft_hardstate
{
    int64               group_id;
    RaftHardStateFile   hs;
} xl_raft_hardstate;

static void
raft_hs_path_for(int64 group_id, char *path, size_t pathlen)
{
    if (group_id == RAFT_CONTROL_GROUP)
        snprintf(path, pathlen, "%s/pg_raft_hardstate", DataDir);
    else
        snprintf(path, pathlen, "%s/pg_raft_hardstate.%lld", DataDir, (long long) group_id);
}

static bool
raft_wal_wait_durable(XLogRecPtr lsn)
{
    XLogRecPtr flushed;
    bool       defined;

    XLogFlush(lsn);
    if (!SyncRepRequested() || WalSndCtl == NULL)
        return true;
    defined = (((volatile WalSndCtlData *) WalSndCtl)->sync_standbys_status & SYNC_STANDBY_DEFINED) != 0;
    if (!defined)
        return true;
    HOLD_INTERRUPTS();
    SyncRepWaitForLSN(lsn, false);
    RESUME_INTERRUPTS();
    LWLockAcquire(SyncRepLock, LW_SHARED);
    flushed = ((volatile WalSndCtlData *) WalSndCtl)->lsn[SYNC_REP_WAIT_FLUSH];
    LWLockRelease(SyncRepLock);
    return flushed >= lsn;
}

static void
raft_wal_log_hardstate(int64 group_id, const RaftHardStateFile *hs)
{
    xl_raft_hardstate xl;
    XLogRecPtr        lsn;

    if (RecoveryInProgress() || !XLogInsertAllowed())
        return;
    memset(&xl, 0, sizeof(xl));
    xl.group_id = group_id;
    xl.hs = *hs;
    XLogBeginInsert();
    XLogRegisterData((char *) &xl, sizeof(xl));
    lsn = XLogInsert(RAFT_RMGR_ID, XLOG_RAFT_HARDSTATE);
    if (!raft_wal_wait_durable(lsn))
        elog(WARNING, "pg_raft: 组 %lld 的 hardstate WAL 未确认到达同步备库",
             (long long) group_id);
}

static void
raft_rmgr_redo(XLogReaderState *record)
{
    uint8               info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    xl_raft_hardstate  *xl = (xl_raft_hardstate *) XLogRecGetData(record);
    char                path[MAXPGPATH];
    char                tmppath[MAXPGPATH];
    int                 fd;

    if (info != XLOG_RAFT_HARDSTATE)
        elog(PANIC, "raft_rmgr_redo: unknown op code %u", info);
    raft_hs_path_for(xl->group_id, path, sizeof(path));
    snprintf(tmppath, sizeof(tmppath), "%s.redo", path);
    fd = open(tmppath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0)
    {
        elog(WARNING, "pg_raft: 回放 hardstate 打不开 \"%s\": %m", tmppath);
        return;
    }
    if (write(fd, &xl->hs, sizeof(xl->hs)) != (ssize_t) sizeof(xl->hs) || pg_fsync(fd) != 0)
    {
        elog(WARNING, "pg_raft: 回放 hardstate 写入失败 \"%s\": %m", tmppath);
        close(fd);
        unlink(tmppath);
        return;
    }
    close(fd);
    if (rename(tmppath, path) != 0)
        elog(WARNING, "pg_raft: 回放 hardstate 改名失败 \"%s\": %m", path);
}

static void
raft_rmgr_desc(StringInfo buf, XLogReaderState *record)
{
    xl_raft_hardstate *xl = (xl_raft_hardstate *) XLogRecGetData(record);

    appendStringInfo(buf, "group %lld term %lld vote %d base %lld/%lld",
                     (long long) xl->group_id, (long long) xl->hs.current_term,
                     xl->hs.voted_for, (long long) xl->hs.base_index,
                     (long long) xl->hs.base_term);
}

static const char *
raft_rmgr_identify(uint8 info)
{
    return ((info & ~XLR_INFO_MASK) == XLOG_RAFT_HARDSTATE) ? "HARDSTATE" : NULL;
}

static const RmgrData raft_rmgr = {
    .rm_name = RAFT_RMGR_NAME,
    .rm_redo = raft_rmgr_redo,
    .rm_desc = raft_rmgr_desc,
    .rm_identify = raft_rmgr_identify,
};

void
pg_raft_register_rmgr(void)
{
    RegisterCustomRmgr(RAFT_RMGR_ID, &raft_rmgr);
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

/*
 * 2026-10-16：数据组 lazy hardstate 的提示文件。term / vote / 压缩基点没变、只有 commit /
 * applied 前移时写这里（不 fsync）；主文件只在 fsync 后才被替换。崩溃（含掉电）最坏丢掉
 * 提示 —— commit / applied 退回主文件里的值，Raft 本就允许；term / vote 不会因为一次未
 * fsync 的 rename 而丢。重启时提示只在与主文件同 term / vote / 基点时采信。
 */
static void
hard_state_hint_path(RaftGroupCtx *ctx, char *path, size_t pathlen)
{
    char base[MAXPGPATH];

    hard_state_path(ctx, base, sizeof(base));
    snprintf(path, pathlen, "%s.hint", base);
}

static bool
persist_hard_state_values_ex(RaftGroupCtx *ctx, int64 current_term, int voted_for,
                             int64 commit_index, int64 last_applied, bool do_fsync)
{
    char              path[MAXPGPATH];
    char              tmppath[MAXPGPATH];
    RaftHardStateFile hs;
    int               fd;
    ssize_t           written;

    if (!do_fsync && ctx->group_id != RAFT_CONTROL_GROUP)
        hard_state_hint_path(ctx, path, sizeof(path));
    else
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

    /*
     * 单副本快速提交（pg_raft.single_member_fast_commit）：单成员数据组的 hardstate 照写
     * （last_applied 必须跟上，否则重启后环被判满），但不 fsync —— 每次提交一次。
     * 控制面组与多成员组不受影响。
     */
    if (do_fsync &&
        !(pg_raft_single_member_fast_commit && ctx->group_id != RAFT_CONTROL_GROUP &&
          group_cluster_size(ctx) == 1) &&
        pg_fsync(fd) != 0)
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

    /* 主文件已带着最新的 commit / applied 落盘：旧提示作废（否则重启时可能被它的更大值盖过） */
    if (do_fsync && ctx->group_id != RAFT_CONTROL_GROUP)
    {
        char hpath[MAXPGPATH];

        hard_state_hint_path(ctx, hpath, sizeof(hpath));
        (void) unlink(hpath);
    }

    /*
     * 2026-10-17（路线 A）：持久的 hardstate（任期 / 投票 / 压缩基点）也记一条 WAL，并等它到达
     * 同步备库之后才返回（调用方随后才回复投票 / 推进任期）。物理备库回放时写出同样的文件，
     * 备库提升接替本节点号时等同于"本节点重启"，不会在已投过票的任期里再投一次。
     */
    if (do_fsync)
        raft_wal_log_hardstate(ctx->group_id, &hs);

    return true;
}

static LWLockPadded *raft_hs_locks = NULL;

static LWLock *
raft_hs_lock(int64 group_id)
{
    if (raft_hs_locks == NULL)
        raft_hs_locks = GetNamedLWLockTranche("pg_raft_hardstate");
    return &raft_hs_locks[(uint64) group_id % RAFT_HS_LOCKS].lock;
}

static void
persist_hard_state_unlocked(RaftGroupCtx *ctx)
{
    int64 current_term;
    int   voted_for;
    int64 commit_index;
    int64 last_applied;
    int64 base_index;
    bool  ok;
    bool  lazy = false;
    LWLock *lk;
    TimestampTz now = 0;

    /*
     * 2026-10-16：文件写入按组串行化，且**持锁后才读值** —— term / commit / applied 都单调，
     * 于是文件内容随写入顺序单调，不会出现"后 rename 的是旧值"（原先并发写者只能靠
     * hs_overlap 作废记账，文件本身仍可能被旧值覆盖）。
     */
    if (pg_raft_data_hardstate_lazy && ctx->group_id != RAFT_CONTROL_GROUP)
        now = GetCurrentTimestamp();
    lk = raft_hs_lock(ctx->group_id);
    LWLockAcquire(lk, LW_EXCLUSIVE);

    SpinLockAcquire(&ctx->cons->mutex);
    current_term = ctx->cons->current_term;
    voted_for = ctx->cons->voted_for;
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    commit_index = ctx->log->commit_index;
    last_applied = ctx->log->last_applied;
    base_index = ctx->log->base_index;
    SpinLockRelease(&ctx->log->mutex);

    /* P7-N41：文件里已经是这组值 ⇒ 不必再写 */
    SpinLockAcquire(&ctx->cons->mutex);
    if (ctx->g->hs_saved_valid &&
        ctx->g->hs_saved_term == current_term &&
        ctx->g->hs_saved_vote == voted_for &&
        ctx->g->hs_saved_commit == commit_index &&
        ctx->g->hs_saved_applied == last_applied)
    {
        SpinLockRelease(&ctx->cons->mutex);
        LWLockRelease(lk);
        return;
    }

    /*
     * 数据组：term / vote / 压缩基点与上次 fsync 的相同 ⇒ 只有 commit / applied 变了。
     * Raft 只要求前三项持久（commitIndex 本是易失量，重启后由 raft_log.committed 与 leader
     * 心跳恢复；last_applied 陈旧只会让幂等的 apply 重做）。原来数据组每批复制在 leader
     * 与每个 follower 上各 fsync 一到两次 hardstate，机械盘上是提交路径的大头之一。
     */
    if (pg_raft_data_hardstate_lazy && ctx->group_id != RAFT_CONTROL_GROUP &&
        ctx->g->hs_dur_valid &&
        ctx->g->hs_dur_term == current_term &&
        ctx->g->hs_dur_vote == voted_for &&
        ctx->g->hs_dur_base == base_index)
    {
        /*
         * 每组每秒至多写一次提示；但 last_applied 前移超过环容量的 1/4 时提前写 —— 重启后
         * applied 落后环太多，restore 灌回的条目会把环判满（last_log - applied ≥ 容量）。
         */
        /*
         * 2026-10-17 复制流：副本在一个会话里顺序处理全部组，每次重写提示文件（open/rename）
         * 在机械盘繁忙时动辄上百毫秒，会直接拖住整条流 —— 放宽到 10 s / 半个环。
         */
        if (!TimestampDifferenceExceeds(ctx->g->hs_lazy_ts, now,
                                        rs_stream_active() ? 10000 : 1000) &&
            last_applied - ctx->g->hs_saved_applied <
            (rs_stream_active() ? RAFT_LOG_CAPACITY / 2 : RAFT_LOG_CAPACITY / 4))
        {
            SpinLockRelease(&ctx->cons->mutex);
            LWLockRelease(lk);
            return;
        }
        ctx->g->hs_lazy_ts = now;
        lazy = true;
    }
    ctx->g->hs_saved_valid = false;
    SpinLockRelease(&ctx->cons->mutex);

    ok = persist_hard_state_values_ex(ctx, current_term, voted_for, commit_index,
                                      last_applied, !lazy);

    SpinLockAcquire(&ctx->cons->mutex);
    if (ok)
    {
        ctx->g->hs_saved_valid = true;
        ctx->g->hs_saved_term = current_term;
        ctx->g->hs_saved_vote = voted_for;
        ctx->g->hs_saved_commit = commit_index;
        ctx->g->hs_saved_applied = last_applied;
        if (!lazy)
        {
            ctx->g->hs_dur_valid = true;
            ctx->g->hs_dur_term = current_term;
            ctx->g->hs_dur_vote = voted_for;
            ctx->g->hs_dur_base = base_index;
        }
    }
    SpinLockRelease(&ctx->cons->mutex);
    LWLockRelease(lk);
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

    /* lazy hardstate 的提示文件：同 term / vote / 基点时取其更大的 commit / applied */
    if (ctx->group_id != RAFT_CONTROL_GROUP && hs.version == RAFT_HARDSTATE_VERSION)
    {
        char              hpath[MAXPGPATH];
        RaftHardStateFile hint;
        int               hfd;

        hard_state_hint_path(ctx, hpath, sizeof(hpath));
        hfd = open(hpath, O_RDONLY, 0);
        if (hfd >= 0)
        {
            memset(&hint, 0, sizeof(hint));
            if (read(hfd, &hint, sizeof(hint)) == (ssize_t) sizeof(hint) &&
                hint.magic == RAFT_HARDSTATE_MAGIC &&
                hint.version == RAFT_HARDSTATE_VERSION &&
                hint.current_term == hs.current_term &&
                hint.voted_for == hs.voted_for &&
                hint.base_index == hs.base_index &&
                hint.base_term == hs.base_term)
            {
                if (hint.commit_index > hs.commit_index)
                    hs.commit_index = hint.commit_index;
                if (hint.last_applied > hs.last_applied)
                    hs.last_applied = hint.last_applied;
            }
            close(hfd);
        }
    }

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
    /*
     * 正结果按后端缓存：information_schema.columns 是重视图，单次数毫秒，而这里在
     * 数据组提交路径上每个分区每轮都要调（写 raft_log 前）。表一旦就绪（扩展装好、
     * 列已加上）在后端生命周期内不会再"不就绪"；负结果不缓存，装扩展前后照常重查。
     */
    static bool ready = false;

    if (ready)
        return true;
    ready = (SPI_execute("SELECT 1 FROM information_schema.columns "
                         "WHERE table_schema = 'partdist' "
                         "AND table_name = 'raft_log' "
                         "AND column_name = 'group_id'",
                         true, 1) == SPI_OK_SELECT && SPI_processed > 0);
    return ready;
}

static void logtrack_note(int64 group_id, int64 index);   /* P7-N46，定义见下 */

static void
delete_log_entry_sql(RaftGroupCtx *ctx, int64 index)
{
    StringInfoData sql;
    bool           spi_owned;

    if (ctx->g->sql_marked_upto > index - 1)
        ctx->g->sql_marked_upto = index - 1;   /* P7-N41：水位随删行退回 */

    if (index <= 0 || !raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    logtrack_note(ctx->group_id, index);    /* P7-N46：删行回滚会让旧行复活 */
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

    if (ctx->g->sql_marked_upto > index)
        ctx->g->sql_marked_upto = index;   /* P7-N41：水位随删行退回 */

    if (index < 0 || !raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    logtrack_note(ctx->group_id, index + 1);    /* P7-N46：删行回滚会让旧行复活 */
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
        out->pxid = InvalidTransactionId;
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

/* 非 Invalid 时 log_append_locked 把它记进新条目（仅 follower 批量收条目期间置位） */
static TransactionId raft_append_pxid = InvalidTransactionId;

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
    e->pxid = raft_append_pxid;
    strlcpy(e->op_type, op_type, RAFT_OP_LEN);
    strlcpy(e->payload, payload, RAFT_PAYLOAD_MAX);
    ctx->log->last_log_index = idx;
    return idx;
}

static void
log_truncate_after_locked(RaftGroupCtx *ctx, int64 index)
{
    if (index < ctx->log->last_log_index)
    {
        ctx->log->last_log_index = index;
        ctx->log->trunc_gen++;
    }
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
/* ★ P7-N34：本后端正在跑控制面 apply（见 replicate_claim 的等待上限） */
static bool raft_in_control_apply = false;

static bool
apply_one_entry(RaftGroupCtx *ctx, const RaftLogEntry *e)
{
    if (ctx->group_id == RAFT_CONTROL_GROUP)
    {
        bool save_in_apply = raft_in_control_apply;

        /*
         * 控制面**保持组化前的语义：失败也推进游标**。这里刻意不做重试 ——
         * 控制面 apply 写的是幂等的 partdist 元数据表，而一条永久失败的
         * payload 若卡住游标，整个 failover 通道都会停摆，代价远大于漏一条。
         * 数据面相反：漏一条 redo 就是堆表分叉，所以下面必须重试。
         */
        /*
         * ★ P7-N34：标出"正在跑控制面 apply" —— 这段里握着 partition_map 的行锁，
         * 交接广播要等该分片的复制认领位；等不到时必须**快速放手**（回滚重做），
         * 不能握着行锁等满 60 s 把写入一起拖死。标记只影响 replicate_claim 的等待上限。
         */
        raft_in_control_apply = true;
        PG_TRY();
        {
            if (!pg_raft_apply_payload_sql(e->op_type, e->payload))
                elog(WARNING, "pg_raft: 控制面条目 %s 应用失败，按原语义跳过",
                     e->op_type);
        }
        PG_FINALLY();
        {
            raft_in_control_apply = save_in_apply;
        }
        PG_END_TRY();
        return true;
    }
    if (strcmp(e->op_type, RAFT_OP_PARWAL) == 0)
        return data_entry_apply(ctx, e);
    if (strcmp(e->op_type, RAFT_OP_CONFIG) == 0)
        return raft_config_persist(ctx, e->payload);
    return true;        /* 组内的非数据条目（如 OP_TEST）无副作用 */
}

/*
 * T7.20（P7-R1）：已提交的成员集落进注册表 `partdist.raft_group.members`。
 *
 * 为什么要落：shmem 里的成员集活不过一次重启，而**重启后恢复出错误的成员集
 * 就是两套多数派定义** —— 本节点按旧集算、别人按新集算。注册表本来就是重启
 * 恢复成员集的来源（restore_groups_if_needed 读它），把它写对即可，
 * 不引入第二份持久结构。
 *
 * 只在 apply（= 该条目已提交）时调：未提交的变更不该留下持久痕迹，
 * 它随时可能被截断回滚。
 */
static bool
raft_config_persist(RaftGroupCtx *ctx, const char *payload)
{
    int            members[RAFT_MAX_PEERS];
    int            n;
    int            i;
    StringInfoData sql;
    bool           spi_owned;

    n = raft_config_parse(payload, members);
    if (n <= 0)
        return true;            /* 解不出来就别写坏值；上游已发过 WARNING */

    if (!raft_persist_spi_begin(&spi_owned))
        return false;           /* 拿不到 SPI：数据组会重试 apply */

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO partdist.raft_group (group_id, members) "
                     "VALUES (%lld, ARRAY[", (long long) ctx->group_id);
    for (i = 0; i < n; i++)
        appendStringInfo(&sql, "%s%d", i ? "," : "", members[i]);
    appendStringInfoString(&sql,
                     "]::integer[]) ON CONFLICT (group_id) DO UPDATE "
                     "SET members = EXCLUDED.members");
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);

    raft_persist_spi_end(spi_owned);
    return true;
}

/*
 * ★ P7-N46 根因：本后端当前事务里动过哪些组的 raft_log 行（组 → 最小 index）。
 * 事务 / 子事务回滚时交给共享内存的 repersist_from；提交时清空。
 */
#define RAFT_LOGTRACK_MAX 16
static int64 logtrack_gid[RAFT_LOGTRACK_MAX];
static int64 logtrack_min[RAFT_LOGTRACK_MAX];
static int   logtrack_n = 0;
static bool  logtrack_overflow = false;
static bool  logtrack_cb_registered = false;

static void
logtrack_flush_to_shmem(void)
{
    int i;
    int k;

    if (RaftGroups == NULL)
        return;
    for (k = 0; k < RAFT_MAX_GROUPS; k++)
    {
        RaftGroupState *g = &RaftGroups->groups[k];
        int64           from = 0;

        if (!g->in_use)
            continue;
        if (logtrack_overflow)
            from = 1;           /* 记不下了：整组按环重写（只重写环里还在的那段） */
        else
            for (i = 0; i < logtrack_n; i++)
                if (logtrack_gid[i] == g->group_id &&
                    (from == 0 || logtrack_min[i] < from))
                    from = logtrack_min[i];
        if (from <= 0)
            continue;
        SpinLockAcquire(&g->log.mutex);
        if (g->repersist_from == 0 || from < g->repersist_from)
            g->repersist_from = from;
        if (g->sql_marked_upto > from - 1)
            g->sql_marked_upto = from - 1;  /* committed 标记也一起撤销了 */
        SpinLockRelease(&g->log.mutex);
    }
}

static void
logtrack_xact_cb(XactEvent event, void *arg)
{
    if (logtrack_n == 0 && !logtrack_overflow)
        return;
    if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
        logtrack_flush_to_shmem();
    if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT ||
        event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT ||
        event == XACT_EVENT_PREPARE)
    {
        logtrack_n = 0;
        logtrack_overflow = false;
    }
}

static void
logtrack_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
                    SubTransactionId parentSubid, void *arg)
{
    /* 子事务回滚同样撤销其中写的行。不区分是哪一层写的：多重写一段是无害的 */
    if (event == SUBXACT_EVENT_ABORT_SUB && (logtrack_n > 0 || logtrack_overflow))
        logtrack_flush_to_shmem();
}

static void
logtrack_note(int64 group_id, int64 index)
{
    int i;

    if (!logtrack_cb_registered)
    {
        RegisterXactCallback(logtrack_xact_cb, NULL);
        RegisterSubXactCallback(logtrack_subxact_cb, NULL);
        logtrack_cb_registered = true;
    }
    if (index <= 0)
        index = 1;
    for (i = 0; i < logtrack_n; i++)
        if (logtrack_gid[i] == group_id)
        {
            if (index < logtrack_min[i])
                logtrack_min[i] = index;
            return;
        }
    if (logtrack_n >= RAFT_LOGTRACK_MAX)
    {
        logtrack_overflow = true;
        return;
    }
    logtrack_gid[logtrack_n] = group_id;
    logtrack_min[logtrack_n] = index;
    logtrack_n++;
}

/*
 * 按环把 [repersist_from, last_log_index] 补写回 raft_log，并删掉 last_log_index 之后
 * 被回滚"复活"的旧行。环窗口之外的那段已经无从补起（与修前一致，靠 N46 自愈的快照兜底）。
 * 调用方已持 SPI。
 */
static void
repersist_from_ring(RaftGroupCtx *ctx)
{
    int64          from;
    int64          last;
    int64          floor_idx;
    int64          commit;
    int64          idx;
    int64          orig;
    int            n = 0;
    StringInfoData sql;

    SpinLockAcquire(&ctx->log->mutex);
    from = ctx->g->repersist_from;
    last = ctx->log->last_log_index;
    commit = ctx->log->commit_index;
    floor_idx = last - RAFT_LOG_CAPACITY + 1;
    if (floor_idx <= ctx->log->base_index)
        floor_idx = ctx->log->base_index + 1;
    SpinLockRelease(&ctx->log->mutex);
    if (from <= 0)
        return;
    orig = from;
    if (from < floor_idx)
        from = floor_idx;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "DELETE FROM partdist.raft_log WHERE group_id = %lld AND log_index > %lld",
                     (long long) ctx->group_id, (long long) last);
    (void) SPI_execute(sql.data, false, 0);

    for (idx = from; idx <= last; idx++)
    {
        RaftLogEntry e;
        bool         got;

        SpinLockAcquire(&ctx->log->mutex);
        got = log_get_entry_locked(ctx, idx, &e);
        SpinLockRelease(&ctx->log->mutex);
        if (!got)
            continue;
        resetStringInfo(&sql);
        appendStringInfo(&sql,
                         "INSERT INTO partdist.raft_log "
                         "(group_id, log_index, term, op_type, payload, committed) "
                         "VALUES (%lld, %lld, %lld, %s, %s::jsonb, %s) "
                         "ON CONFLICT (group_id, log_index) DO UPDATE SET "
                         "term = EXCLUDED.term, op_type = EXCLUDED.op_type, "
                         "payload = EXCLUDED.payload, committed = EXCLUDED.committed",
                         (long long) ctx->group_id, (long long) idx, (long long) e.term,
                         quote_literal_cstr(e.op_type), quote_literal_cstr(e.payload),
                         idx <= commit ? "true" : "false");
        (void) SPI_execute(sql.data, false, 0);
        n++;
    }
    pfree(sql.data);

    /* 本次补写自己也可能回滚 —— 先登记，回滚时回调会把标记重新立起来 */
    logtrack_note(ctx->group_id, from);
    SpinLockAcquire(&ctx->log->mutex);
    if (ctx->g->repersist_from == orig)
        ctx->g->repersist_from = 0;     /* 期间又有更早的标记立起来就留着，下次再补 */
    SpinLockRelease(&ctx->log->mutex);
    elog(LOG, "pg_raft: group %lld SQL 日志按环补写 %lld..%lld 共 %d 条（此前承载它们的事务回滚，P7-N46）",
         (long long) ctx->group_id, (long long) from, (long long) last, n);
}

static void
persist_log_entry_sql(RaftGroupCtx *ctx, int64 index, int64 term,
                      const char *op_type, const char *payload, bool committed)
{
    StringInfoData sql;
    bool           spi_owned;

    /* follower 批内：攒到批尾一条多行 INSERT（同组、同任期、OP_PARWAL、未提交、index 递增） */
    if (raft_batch_active && !committed && strcmp(op_type, RAFT_OP_PARWAL) == 0 &&
        raft_batch_nrows < RAFT_LOG_CAPACITY &&
        (raft_batch_nrows == 0 || (term == raft_batch_row_term &&
                                   index > raft_batch_row_idx[raft_batch_nrows - 1])))
    {
        raft_batch_row_term = term;
        raft_batch_row_idx[raft_batch_nrows] = index;
        raft_batch_row_pl[raft_batch_nrows] = pstrdup(payload);
        raft_batch_nrows++;
        return;
    }

    if (!raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    if (ctx->g->repersist_from > 0)
        repersist_from_ring(ctx);
    logtrack_note(ctx->group_id, index);

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

/*
 * persist_log_entry_sql 的批量版：一批条目一条多行 INSERT（每条一次 SPI 解析/规划/执行
 * 原先占数据组提交路径的大头）。语义与逐条调用相同。
 */
static void
persist_log_entries_sql_batch(RaftGroupCtx *ctx, int n, const int64 *idx, int64 term,
                              const char *op_type, char **payload, bool committed)
{
    StringInfoData sql;
    bool           spi_owned;
    int            k;
    char          *op_lit;

    if (n <= 0)
        return;
    if (n == 1)
    {
        persist_log_entry_sql(ctx, idx[0], term, op_type, payload[0], committed);
        return;
    }
    if (!raft_persist_spi_begin(&spi_owned))
        return;
    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }
    if (ctx->g->repersist_from > 0)
        repersist_from_ring(ctx);
    for (k = 0; k < n; k++)
        logtrack_note(ctx->group_id, idx[k]);

    op_lit = quote_literal_cstr(op_type);
    initStringInfo(&sql);
    appendStringInfoString(&sql,
                           "INSERT INTO partdist.raft_log "
                           "(group_id, log_index, term, op_type, payload, committed) VALUES ");
    for (k = 0; k < n; k++)
        appendStringInfo(&sql, "%s(%lld, %lld, %lld, %s, %s::jsonb, %s)",
                         k == 0 ? "" : ", ",
                         (long long) ctx->group_id, (long long) idx[k], (long long) term,
                         op_lit, quote_literal_cstr(payload[k]),
                         committed ? "true" : "false");
    appendStringInfoString(&sql,
                           " ON CONFLICT (group_id, log_index) DO UPDATE SET "
                           "term = EXCLUDED.term, op_type = EXCLUDED.op_type, "
                           "payload = EXCLUDED.payload, committed = partdist.raft_log.committed OR EXCLUDED.committed");
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
}

static void
mark_log_committed_sql(RaftGroupCtx *ctx, int64 upto_index)
{
    StringInfoData sql;
    bool           spi_owned;

    if (upto_index <= 0 || upto_index <= ctx->g->sql_marked_upto ||
        !raft_persist_spi_begin(&spi_owned))
        return;

    if (!raft_log_table_ready())
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    /*
     * 下界：sql_marked_upto 以下已全部标记（删行/回滚时水位随之退回，见 P7-N41 各处）。
     * 原先没有下界，每次提交都把本组全部历史日志沿 (group_id, log_index) 扫一遍——
     * 组里几万条时单次 >10 ms 且持续变慢。
     */
    if (ctx->group_id != RAFT_CONTROL_GROUP)
    {
        /*
         * 2026-10-16：数据组标 committed 不等行锁（SKIP LOCKED）。这一列只是重启恢复时抬
         * commit_index 的提示（权威在 hardstate；只有 restore 读它），而 follower 上同组并发的
         * 几批 AppendEntries 事务会互相 upsert / 标记对方的行 —— 普通 UPDATE 在这里与 upsert
         * 成环，实测副本端报 deadlock detected、整批失败。被跳过的行就留着 false。
         */
        appendStringInfo(&sql,
                         "UPDATE partdist.raft_log r SET committed = true "
                         "FROM (SELECT log_index FROM partdist.raft_log "
                         "      WHERE group_id = %lld AND log_index > %lld AND log_index <= %lld "
                         "      AND committed = false FOR UPDATE SKIP LOCKED) c "
                         "WHERE r.group_id = %lld AND r.log_index = c.log_index",
                         (long long) ctx->group_id,
                         (long long) Max(ctx->g->sql_marked_upto, 0),
                         (long long) upto_index,
                         (long long) ctx->group_id);
    }
    else
        appendStringInfo(&sql,
                         "UPDATE partdist.raft_log SET committed = true "
                         "WHERE group_id = %lld AND log_index > %lld AND log_index <= %lld "
                         "AND committed = false",
                         (long long) ctx->group_id,
                         (long long) Max(ctx->g->sql_marked_upto, 0),
                         (long long) upto_index);
    if (SPI_execute(sql.data, false, 0) == SPI_OK_UPDATE &&
        upto_index > ctx->g->sql_marked_upto)
        ctx->g->sql_marked_upto = upto_index;
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

        if (raft_group_ensure(gid, members, n_members, &ctx, false))
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
        e->pxid = InvalidTransactionId;
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
        /* P7-N37：这条终于过去了，清掉重试账本（比较也放锁内，别在锁外读 int64） */
        if (ok && ctx->group_id == RAFT_CONTROL_GROUP)
        {
            SpinLockAcquire(&ctx->log->mutex);
            if (ctx->log->ctrl_fail_index == e->index)
            {
                ctx->log->ctrl_fail_index = 0;
                ctx->log->ctrl_fail_count = 0;
                ctx->log->ctrl_fail_since = 0;
            }
            SpinLockRelease(&ctx->log->mutex);
        }
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

        /*
         * ★ P7-N37：控制面不再"一错就跳过"。同一条目先重试，连续失败超过
         * RAFT_CTRL_APPLY_RETRY_MS 才跳过。
         */
        if (ctx->group_id == RAFT_CONTROL_GROUP)
        {
            TimestampTz now = GetCurrentTimestamp();
            int         tries;
            long        secs;
            int         usecs;

            SpinLockAcquire(&ctx->log->mutex);
            if (ctx->log->ctrl_fail_index != e->index)
            {
                ctx->log->ctrl_fail_index = e->index;
                ctx->log->ctrl_fail_count = 0;
                ctx->log->ctrl_fail_since = now;
            }
            tries = ++ctx->log->ctrl_fail_count;
            TimestampDifference(ctx->log->ctrl_fail_since, now, &secs, &usecs);
            SpinLockRelease(&ctx->log->mutex);

            ok = (secs * 1000 + usecs / 1000 >= RAFT_CTRL_APPLY_RETRY_MS);
            elog(WARNING,
                 "pg_raft: group 0 的条目 %lld(%s) apply 抛错：%s —— %s"
                 "（第 %d 次，已持续 %ld s，P7-N37）",
                 (long long) e->index, e->op_type, edata->message,
                 ok ? "重试已超时，按既有语义跳过（本节点将永久错过这条登记）"
                    : "保留游标，下轮重试",
                 tries, secs);
            if (ok)
            {
                SpinLockAcquire(&ctx->log->mutex);
                ctx->log->ctrl_fail_index = 0;
                ctx->log->ctrl_fail_count = 0;
                ctx->log->ctrl_fail_since = 0;
                SpinLockRelease(&ctx->log->mutex);
            }
        }
        else
        {
            ok = false;
            elog(WARNING,
                 "pg_raft: group %lld 的条目 %lld(%s) apply 抛错：%s —— "
                 "数据面保留游标，下轮重试",
                 (long long) ctx->group_id, (long long) e->index, e->op_type,
                 edata->message);
        }
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

/*
 * ★ P7-N29（2026-09-19）：控制面 apply 的游标跟着事务走。
 *
 * 控制面条目在 apply_one_entry_guarded 的**子事务**里写元数据表（partition_map 等），
 * 子事务提交进的是**外层事务** —— 通常是承载这次 AppendEntries / 上报的 RPC 会话事务。
 * 而 last_applied 推进的是共享内存、并立刻落 hardstate 文件，都不随事务回滚。外层事务
 * 后来一 ERROR / 客户端断连 FATAL（实测：apply 里的主权交接广播等复制认领位 60 s，RPC
 * 客户端早已超时断开，`FATAL: connection to client lost`），这次调用里 apply 过的条目的
 * 元数据修改全部撤销，游标却已越过它们，永不重放 ⇒ 该节点的 partition_map 与日志永久分叉
 * （worker3 读到的"上一条登记"停在 term 2，连带把 P7-N27 的首版修法带偏）。
 *
 * 做法：本事务第一次推进控制面游标时记下推进前的值与事务嵌套层；事务（或记录层及以上的
 * 子事务）回滚时把游标退回去并重写 hardstate，让这些条目下次重新 apply。控制面 apply
 * 按设计幂等（元数据表 UPSERT + 任期栅栏，见 apply_one_entry 的注释），重放无副作用之虞。
 */
static int64 ctrl_xact_applied_from = -1;   /* -1 = 本事务未推进过控制面游标 */
static int64 ctrl_pending_base_idx = -1;    /* P7-N59：本事务里做了压缩、待提交后推进的基点 */
static int64 ctrl_pending_base_term = 0;
static int   ctrl_xact_nest_level = 0;
static bool  ctrl_xact_cb_registered = false;

static void
ctrl_apply_rewind(const char *why)
{
    RaftGroupCtx ctx;
    int64        from = ctrl_xact_applied_from;
    int64        cur;

    ctrl_xact_applied_from = -1;
    if (from < 0 || RaftGroups == NULL || !raft_group_ctx(RAFT_CONTROL_GROUP, &ctx))
        return;

    SpinLockAcquire(&ctx.log->mutex);
    cur = ctx.log->last_applied;
    if (cur > from)
        ctx.log->last_applied = from;
    SpinLockRelease(&ctx.log->mutex);

    if (cur > from)
    {
        persist_hard_state_unlocked(&ctx);
        elog(WARNING,
             "pg_raft: 承载控制面 apply 的%s回滚，apply 游标从 %lld 退回 %lld，"
             "这些条目将重新 apply（P7-N29：否则元数据被撤销而游标不退，永久分叉）",
             why, (long long) cur, (long long) from);
    }
}

static void
ctrl_apply_xact_cb(XactEvent event, void *arg)
{
    (void) arg;
    switch (event)
    {
        case XACT_EVENT_ABORT:
        case XACT_EVENT_PARALLEL_ABORT:
            ctrl_apply_rewind("事务");
            ctrl_pending_base_idx = -1;     /* P7-N59：快照行与删行都回滚了，基点不动 */
            break;
        case XACT_EVENT_COMMIT:
        case XACT_EVENT_PARALLEL_COMMIT:
        case XACT_EVENT_PREPARE:
            ctrl_xact_applied_from = -1;
            if (ctrl_pending_base_idx > 0 && RaftGroups != NULL)
            {
                RaftGroupCtx bctx;

                if (raft_group_ctx(RAFT_CONTROL_GROUP, &bctx))
                {
                    SpinLockAcquire(&bctx.log->mutex);
                    if (ctrl_pending_base_idx > bctx.log->base_index)
                    {
                        bctx.log->base_index = ctrl_pending_base_idx;
                        bctx.log->base_term = ctrl_pending_base_term;
                    }
                    SpinLockRelease(&bctx.log->mutex);
                    persist_hard_state_unlocked(&bctx);
                }
            }
            ctrl_pending_base_idx = -1;
            break;
        default:
            break;
    }
}

static void
ctrl_apply_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
                      SubTransactionId parentSubid, void *arg)
{
    (void) mySubid;
    (void) parentSubid;
    (void) arg;
    if (ctrl_xact_applied_from < 0)
        return;
    if (event == SUBXACT_EVENT_ABORT_SUB &&
        GetCurrentTransactionNestLevel() <= ctrl_xact_nest_level)
        ctrl_apply_rewind("子事务");
    else if (event == SUBXACT_EVENT_COMMIT_SUB &&
             GetCurrentTransactionNestLevel() == ctrl_xact_nest_level)
        ctrl_xact_nest_level--;     /* 效果并入父事务，从此跟父事务走 */
}

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
    if (!ctrl_xact_cb_registered)
    {
        RegisterXactCallback(ctrl_apply_xact_cb, NULL);
        RegisterSubXactCallback(ctrl_apply_subxact_cb, NULL);
        ctrl_xact_cb_registered = true;
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
        {
            /* P7-N29：记下本事务第一次推进前的控制面游标 */
            if (ctx->group_id == RAFT_CONTROL_GROUP && ctrl_xact_applied_from < 0 &&
                IsTransactionState())
            {
                ctrl_xact_applied_from = ctx->log->last_applied;
                ctrl_xact_nest_level = GetCurrentTransactionNestLevel();
            }
            ctx->log->last_applied = batch_end;
        }
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

    /*
     * ★ P7-N59（2026-10-04）：基点等**本事务提交**之后再推进（ctrl_apply_xact_cb）。
     * 原来这里立刻改共享内存并落 hardstate；而写快照、删日志行都在调用方事务里 ——
     * 那个事务后来回滚（N29 的场景），快照行与删行一起撤销，基点却已越过去：
     * 实测 leader 基点 6149、快照表停在 4001，落后成员既读不到日志（环外、已当作删掉）
     * 也装不上快照，master 与 :5435 的控制面永远停在 5129。
     */
    ctrl_pending_base_idx = applied;
    ctrl_pending_base_term = keep_term;

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
    ctx->log->leader_durable_index = ctx->log->last_log_index;   /* 当选时日志里已有的条目视为已落盘 */
    ctx->log->leader_self_hole = 0;
    SpinLockRelease(&ctx->log->mutex);
}

/* ---- peers ---- */

static void peer_conn_reset(int slot);
static uint32 peers_addr_gen = 0;

/* 把共享内存里的节点地址覆盖套到 peers[] 上（地址变了的槽位丢掉缓存连接） */
static bool addr_initial_parse = false;   /* 进程首次解析 peers：覆盖照套，但不逐个后端打日志 */

static void
apply_addr_overrides(void)
{
    uint32  gen;
    int     n, i, j;
    int     node[RAFT_MAX_PEERS];
    int     port[RAFT_MAX_PEERS];
    char    host[RAFT_MAX_PEERS][256];

    if (RaftGroups == NULL)
        return;
    SpinLockAcquire(&RaftGroups->mutex);
    gen = RaftGroups->addr_gen;
    n = Min(RaftGroups->addr_n, RAFT_MAX_PEERS);
    for (i = 0; i < n; i++)
    {
        node[i] = RaftGroups->addr_node[i];
        port[i] = RaftGroups->addr_port[i];
        strlcpy(host[i], RaftGroups->addr_host[i], sizeof(host[i]));
    }
    SpinLockRelease(&RaftGroups->mutex);
    if (gen == peers_addr_gen)
        return;
    for (i = 0; i < n; i++)
        for (j = 0; j < n_peers; j++)
            if (peers[j].node_id == node[i] &&
                (peers[j].port != port[i] || strcmp(peers[j].host, host[i]) != 0))
            {
                elog(addr_initial_parse ? DEBUG1 : LOG, "pg_raft: 节点 %d 的地址 %s:%d → %s:%d（节点地址覆盖）",
                     node[i], peers[j].host, peers[j].port, host[i], port[i]);
                strlcpy(peers[j].host, host[i], sizeof(peers[j].host));
                peers[j].port = port[i];
                peer_conn_reset(j);
            }
    peers_addr_gen = gen;
}

static void
parse_peers(void)
{
    char *buf;
    char *tok;
    char *saveptr = NULL;

    if (peers_parsed)
    {
        if (RaftGroups != NULL && RaftGroups->addr_gen != peers_addr_gen)
            apply_addr_overrides();
        return;
    }

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
    peers_addr_gen = 0;
    addr_initial_parse = true;
    apply_addr_overrides();
    addr_initial_parse = false;
}

/* 本节点共享内存里设一条节点地址覆盖（幂等；地址没变不涨代数） */
void
pg_raft_set_node_address_internal(int node_id, const char *host, int port)
{
    int i;
    bool changed = false;

    if (RaftGroups == NULL || node_id <= 0 || host == NULL || host[0] == '\0' || port <= 0)
        return;
    SpinLockAcquire(&RaftGroups->mutex);
    for (i = 0; i < RaftGroups->addr_n; i++)
        if (RaftGroups->addr_node[i] == node_id)
            break;
    if (i == RaftGroups->addr_n && i < RAFT_MAX_PEERS)
    {
        RaftGroups->addr_n++;
        RaftGroups->addr_node[i] = node_id;
        RaftGroups->addr_host[i][0] = '\0';
        RaftGroups->addr_port[i] = 0;
    }
    if (i < RAFT_MAX_PEERS &&
        (RaftGroups->addr_port[i] != port || strcmp(RaftGroups->addr_host[i], host) != 0))
    {
        strlcpy(RaftGroups->addr_host[i], host, sizeof(RaftGroups->addr_host[i]));
        RaftGroups->addr_port[i] = port;
        RaftGroups->addr_gen++;
        changed = true;
    }
    SpinLockRelease(&RaftGroups->mutex);
    if (changed)
        parse_peers();
}

/* 本节点：按 partdist.node_address 重灌地址覆盖（调用方在事务里；维护进程定期调，重启后也能恢复） */
int
pg_raft_sync_node_addresses_internal(void)
{
    bool spi_owned;
    int  n = 0;
    uint64 r;

    if (!raft_persist_spi_begin(&spi_owned))
        return 0;
    if (SPI_execute("SELECT node_id, host, port FROM partdist.node_address "
                    "WHERE to_regclass('partdist.node_address') IS NOT NULL", true, 0) == SPI_OK_SELECT)
    {
        int     ids[RAFT_MAX_PEERS];
        int     ports[RAFT_MAX_PEERS];
        char   *hosts[RAFT_MAX_PEERS];
        int     k;

        for (r = 0; r < SPI_processed && n < RAFT_MAX_PEERS; r++)
        {
            bool  isnull;
            char *h;

            ids[n] = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 1, &isnull));
            h = SPI_getvalue(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 2);
            ports[n] = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 3, &isnull));
            if (h == NULL)
                continue;
            hosts[n] = pstrdup(h);
            n++;
        }
        raft_persist_spi_end(spi_owned);
        for (k = 0; k < n; k++)
            pg_raft_set_node_address_internal(ids[k], hosts[k], ports[k]);
        return n;
    }
    raft_persist_spi_end(spi_owned);
    return 0;
}

PG_FUNCTION_INFO_V1(pg_raft_set_node_address);
/* pg_raft_set_node_address(node_id, host, port)：只改本节点（表 + 共享内存）。正常经控制面 OP_NODE_ADDRESS 走。 */
Datum
pg_raft_set_node_address(PG_FUNCTION_ARGS)
{
    int     node_id = PG_GETARG_INT32(0);
    char   *host = text_to_cstring(PG_GETARG_TEXT_PP(1));
    int     port = PG_GETARG_INT32(2);
    bool    spi_owned;
    StringInfoData sql;

    if (raft_persist_spi_begin(&spi_owned))
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "INSERT INTO partdist.node_address (node_id, host, port, updated_at) "
                         "VALUES (%d, %s, %d, now()) ON CONFLICT (node_id) DO UPDATE SET "
                         "host = EXCLUDED.host, port = EXCLUDED.port, updated_at = now(); "
                         "UPDATE partdist.node_map SET hostname = %s, port = %d WHERE node_id = %d",
                         node_id, quote_literal_cstr(host), port,
                         quote_literal_cstr(host), port, node_id);
        (void) SPI_execute(sql.data, false, 0);
        raft_persist_spi_end(spi_owned);
    }
    pg_raft_set_node_address_internal(node_id, host, port);
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_raft_sync_node_addresses);
Datum
pg_raft_sync_node_addresses(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(pg_raft_sync_node_addresses_internal());
}

PG_FUNCTION_INFO_V1(pg_raft_propose_node_address);
/* pg_raft_propose_node_address(node_id, host, port)：经控制面日志改节点地址（须在控制面 leader 上调） */
Datum
pg_raft_propose_node_address(PG_FUNCTION_ARGS)
{
    int     node_id = PG_GETARG_INT32(0);
    char   *host = text_to_cstring(PG_GETARG_TEXT_PP(1));
    int     port = PG_GETARG_INT32(2);
    int     tli = (PG_NARGS() > 3) ? PG_GETARG_INT32(3) : 0;
    StringInfoData payload;
    int64   idx;

    initStringInfo(&payload);
    appendStringInfo(&payload, "{\"node_id\": %d, \"host\": \"%s\", \"port\": %d, \"tli\": %d}",
                     node_id, host, port, tli);
    idx = pg_raft_consensus_propose(PG_RAFT_OP_NODE_ADDRESS, payload.data);
    if (idx <= 0)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 节点地址提议失败（本节点不是控制面 leader，或无多数派）")));
    PG_RETURN_INT64(idx);
}

/*
 * 该 peer 槽位是否参与本组复制（非本节点，且属于本组成员集）。
 *
 * ★ T7.20：成员变更未提交期间，**旧成员集里的节点也要继续收**（Raft 论文
 *   §4.2.2 的建议）。被移除的那个节点如果收不到"把它移除"的那条 CONFIG，
 *   它会一直以为自己还在组里 —— 于是继续按本组的选举超时发起竞选，反复打扰
 *   一个它已经不属于的组。让它收到那条记录，它自己就退出了。
 *   注意这**不影响多数派计算** —— 多数派只看当前成员集（group_cluster_size），
 *   这里放宽的只是"发给谁"。
 */
static bool
peer_in_group(RaftGroupCtx *ctx, int slot)
{
    int i;

    if (peers[slot].node_id == pg_raft_node_id)
        return false;
    if (group_has_member(ctx, peers[slot].node_id))
        return true;

    if (ctx->g->pending_cfg_index > 0)
        for (i = 0; i < ctx->g->n_committed_members; i++)
            if (ctx->g->committed_members[i] == peers[slot].node_id)
                return true;
    return false;
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
             "connect_timeout=%d application_name=pg_raft_rpc",
             p->host, p->port, pg_raft_peer_connect_timeout_s);

    peer_conn[slot] = PQconnectdb(conninfo);
    if (PQstatus(peer_conn[slot]) != CONNECTION_OK)
    {
        peer_conn_reset(slot);
        return NULL;
    }
    return peer_conn[slot];
}

/*
 * R-P4-7（2026-08-14 解冻批次 #2）：有界 RPC 执行。
 *
 * PQexec 对"活着但不应答"的对端（半死、恢复中假接受、网络黑洞）可阻塞
 * 到分钟级；持有复制认领位的 backend 卡在这里，整组 prepare/决议随之窒息
 * （崩溃矩阵行 1 实测：两笔"等待复制认领位超过 60000 ms"）。异步发送 +
 * 100ms 粒度轮询截止：超时返回 NULL，调用方按 RPC 失败处置并**重置缓存
 * 连接**（连接上还挂着在途查询，绝不可复用）。CHECK_FOR_INTERRUPTS 保持
 * 可打断——从 ERROR 出去时调用链的 PG_FINALLY 会归还认领位。
 */
#define RAFT_RPC_EXEC_TIMEOUT_MS 5000

static bool
pq_wait_ready_bounded(PGconn *conn, TimestampTz start, int timeout_ms)
{
    while (PQisBusy(conn))
    {
        int         sock = PQsocket(conn);
        fd_set      rf;
        struct timeval tv;

        if (sock < 0 ||
            TimestampDifferenceExceeds(start, GetCurrentTimestamp(),
                                       timeout_ms))
            return false;
        FD_ZERO(&rf);
        FD_SET(sock, &rf);
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        (void) select(sock + 1, &rf, NULL, NULL, &tv);
        CHECK_FOR_INTERRUPTS();
        if (!PQconsumeInput(conn))
            return false;
    }
    return true;
}

static PGresult *
pq_result_bounded(PGconn *conn, TimestampTz start, int timeout_ms)
{
    PGresult   *first;
    PGresult   *r;

    if (!pq_wait_ready_bounded(conn, start, timeout_ms))
        return NULL;
    first = PQgetResult(conn);
    for (;;)                    /* 排空到 NULL；排空半途超时也作失败 */
    {
        if (!pq_wait_ready_bounded(conn, start, timeout_ms))
        {
            if (first)
                PQclear(first);
            return NULL;
        }
        r = PQgetResult(conn);
        if (r == NULL)
            break;
        PQclear(r);
    }
    return first;
}

static PGresult *
pq_exec_bounded(PGconn *conn, const char *sql)
{
    if (!PQsendQuery(conn, sql))
        return NULL;
    return pq_result_bounded(conn, GetCurrentTimestamp(),
                             RAFT_RPC_EXEC_TIMEOUT_MS);
}

static PGresult *
pq_exec_params_bounded(PGconn *conn, const char *sql, int nparams,
                       const char *const *values)
{
    if (!PQsendQueryParams(conn, sql, nparams, NULL, values, NULL, NULL, 0))
        return NULL;
    return pq_result_bounded(conn, GetCurrentTimestamp(),
                             RAFT_RPC_EXEC_TIMEOUT_MS);
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

    res = pq_exec_bounded(conn, sql);
    if (res == NULL)
    {
        /* 超时/坏连接：在途查询挂在连接上，必须弃用重连 */
        peer_conn_reset(peer_slot_of(p));
        peer_mark_result(p, false);
        return false;
    }
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

    res = pq_exec_bounded(conn, sql);
    if (res == NULL)
    {
        peer_conn_reset(peer_slot_of(p));
        peer_mark_result(p, false);
        return -1;
    }
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
send_install_snapshot(RaftGroupCtx *ctx, RaftPeer *p, int64 term, int64 base_idx, bool fresh)
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
                     (long long) (fresh ? PG_INT64_MAX : base_idx));   /* fresh：跳过旧快照，下面现做 */
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
        /*
         * P7-N59 兜底：快照表落后于基点（压缩时的事务回滚过）。用本节点当前的控制面状态
         * （已 apply 到 last_applied 的 node_map / partition_map）现做一份快照再发 ——
         * 控制面快照本来就只是这两张表在某个已 apply 位点上的样子。
         */
        int64 applied;
        int64 aterm = 0;

        SpinLockAcquire(&ctx->log->mutex);
        applied = ctx->log->last_applied;
        SpinLockRelease(&ctx->log->mutex);
        if (applied >= base_idx && log_term_at(ctx, applied, &aterm) && aterm > 0 &&
            raft_persist_spi_begin(&spi_owned))
        {
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
                             "RETURNING last_included_index::text, last_included_term::text, node_map::text, partition_map::text",
                             (long long) applied, (long long) aterm);
            if (SPI_execute(sql.data, false, 0) == SPI_OK_INSERT_RETURNING && SPI_processed > 0)
            {
                HeapTuple tup = SPI_tuptable->vals[0];
                TupleDesc desc = SPI_tuptable->tupdesc;

                oldctx = MemoryContextSwitchTo(TopTransactionContext);
                snap_idx = TextDatumGetCString(SPI_getbinval(tup, desc, 1, &isnull));
                snap_term = TextDatumGetCString(SPI_getbinval(tup, desc, 2, &isnull));
                node_map = TextDatumGetCString(SPI_getbinval(tup, desc, 3, &isnull));
                partition_map = TextDatumGetCString(SPI_getbinval(tup, desc, 4, &isnull));
                MemoryContextSwitchTo(oldctx);
                elog(LOG, "pg_raft: 组 0 快照表落后于基点 %lld，按当前已 apply 位点 %lld 现做一份快照发给落后成员",
                     (long long) base_idx, (long long) applied);
            }
            pfree(sql.data);
            raft_persist_spi_end(spi_owned);
        }
    }
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

    res = pq_exec_params_bounded(conn,
                       "SELECT partdist.pg_raft_install_snapshot("
                       "$1::bigint, $2::int, $3::bigint, $4::bigint, $5::text, $6::text)",
                       6, params);
    if (res == NULL)
    {
        peer_conn_reset(peer_slot_of(p));
        peer_mark_result(p, false);
        return false;
    }
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

/*
 * P7-N14（2026-09-17）：本节点当选却**不可升主**（升主前置返回 -1：没有本地副本 /
 * 快路径分叉 / 带陈旧数据回归的旧主），主动让位。
 *
 * 不让位的后果实测过：旧主 :5433 丢主后再次赢得选举，升主前置每轮 -1、永不登记，
 * 而它照发心跳，其余成员选不上 —— partition_map 仍指向前任、写入全被写栅栏拒，
 * 该分片无主直到选举碰巧抖到别人（term 5→6→7，120 s 内未登记）。设计原话
 * "只排除这一个节点，同组其余成员照常可当选"要成立，被排除的节点必须退出。
 *
 * 做法：降为 follower、清 leader_id、复制游标作废，选举截止期推远
 * RAFT_UNPROMOTABLE_BACKOFF 个选举周期 —— 其余成员先超时先竞选，本节点期间只投票。
 * term 不动（不是被更高 term 压下去的），HardState 里 term/voted_for 没变，不必持久化。
 * 多数派保证至少还有一个成员持有全部已提交条目，它当选后本节点照常跟随。
 * 若本节点重做基线后重新具备资格，退避到期照常参选。
 */
#define RAFT_UNPROMOTABLE_BACKOFF   5

/*
 * ★ P7-N17（2026-09-17，N12 复现 run 4）：退避 5 个周期不够。2 vCPU 饱和时同步 RPC 把
 * tick 拖到 6 s 以上，新主的心跳断档 ⇒ 让位的旧主一到退避期满就再次赢选举
 * （它的日志最长）⇒ 升主前置再 -1 ⇒ 再让位 …… 每一轮都把真正的新主打回 follower、
 * 其在途升主前置作废重来，180 s 内登记不上。
 *
 * 升主前置返回 -1 的三种情形（没有本地副本 / 快路径分叉 / 收了 WAL 却没有回放槽位）
 * 都不会自己好起来，只有重做物理基线（provision_shard_replica ⇒ 回放槽位重新 armed）
 * 才能恢复资格。所以：让位后记一个 BGW 本地标记，选举截止期到了先查槽位
 * （一条 SPI，只在"曾让位 + 截止期已到"时才跑），没 armed 就只推后一个周期、只投票；
 * armed 了清标记照常参选。5 个周期的退避仍保留，作为查询失败时的最小静默。
 */
/*
 * ★ P7-N17 修正（2026-09-18）：撤掉"让位后查回放槽位再决定是否参选"的 SPI 检查。
 *
 * 原实现 unpromotable_still() 在 **BGW tick 里直接 SPI**（partdist.replay_status），
 * 而 tick 没有活动事务：raft_persist_spi_begin → GetTransactionSnapshot() 在无事务
 * 上下文里解引用事务状态，段错误，pg_raft 后台 worker 反复 signal 11（实测切主抖动期
 * 每分钟崩一次）。而且它是**冗余**的 —— raft_abdicate_unpromotable 已经把 election_deadline
 * 推远 5 个选举周期，退避期内根本不会走到 start_election，SPI 检查从不改变结果、只添崩溃面。
 * 现回到 N14 纯时间退避：让位即推远截止期，到期照常参选；仍不可升主就再让位再退避。
 */
static void
raft_abdicate_unpromotable(RaftGroupCtx *ctx)
{
    bool was_leader;

    ctx->g->no_local_shard = true;     /* P7-N57：让位之后也别再自己竞选，等供副本 */

    SpinLockAcquire(&ctx->cons->mutex);
    was_leader = (ctx->cons->state == RAFT_LEADER);
    if (was_leader)
    {
        ctx->cons->state = RAFT_FOLLOWER;
        ctx->cons->leader_id = 0;
        SpinLockAcquire(&ctx->log->mutex);
        ctx->log->repl_inited = false;
        SpinLockRelease(&ctx->log->mutex);
        ctx->cons->election_deadline =
            GetCurrentTimestamp() +
            (long) pg_raft_election_timeout_ms * RAFT_UNPROMOTABLE_BACKOFF * 1000L;
    }
    SpinLockRelease(&ctx->cons->mutex);

    if (was_leader)
    {
        ctx->g->report_pending = false;
        elog(LOG,
             "pg_raft: 组 %lld 本节点不可升主（升主前置返回 -1），主动让位并退避 %d ms，"
             "让其余有副本的成员当选",
             (long long) ctx->group_id,
             pg_raft_election_timeout_ms * RAFT_UNPROMOTABLE_BACKOFF);
    }
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

    /*
     * 2026-10-17 流水线：数据组 leader 的自投票只算到自己的 raft_log 行已提交落盘的位置。
     * 流水线把 leader 行放到独立连接里自提交（rm_pipe_send），这之前那些条目只在环里，
     * leader 崩溃重启就没了 —— 不能算它一票。
     */
    if ((pg_raft_replicate_pipeline && ctx->group_id != RAFT_CONTROL_GROUP) || rs_stream_data(ctx))
    {
        int64 self = Min(ctx->log->leader_durable_index, ctx->log->last_log_index);

        if (ctx->log->leader_self_hole > 0 && self >= ctx->log->leader_self_hole)
            self = ctx->log->leader_self_hole - 1;
        matches[n++] = self;
    }
    else
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


/*
 * ───────────── P7-N41（2026-10-03）：心跳按对端合并 ─────────────
 *
 * 128 分片组 × 副本因子 3 的演示环境里（每台 worker ~96 个组、其中 ~32 个当 leader），
 * 一次 tick 要串行发「leader 组数 × 对端数」≈ 64 条同步 RPC，再加上每条心跳在对端
 * fsync 一次 hardstate 文件 —— 2 vCPU 上实测 tick 6–13 s（选举超时 6 s），
 * 控制面 group 0 一小时改选上千次（term 1026），数据组 leader 不停漂移，
 * 供副本因"本节点不是主"失败，128 组建了 40 分钟没建完。
 *
 * 做法（§11.6 #2「心跳按对端合并」的第二步：连接早已合并，这里合并报文）：
 *   · BGW tick 期间打开收集模式；replicate_to_peer 走到"只发心跳"那一支时不立刻发，
 *     把 (组, term, prev_idx, prev_term, commit) 记进该对端的批次；
 *     要带条目的（控制面条目 / CONFIG / NOOP）照旧单发，语义不变；
 *   · 全部组推进完后，对每个对端**一条** RPC（pg_raft_heartbeat_multi），
 *     各对端**并发**发出、统一等回；
 *   · 回执逐组按 replicate_to_peer 原来的规则处置（rt>term 让位 / ok 推 match / 拒则 next--），
 *     只对"发出时的 term 仍是本组当前 term 且仍是 leader"的组生效。
 * RPC 数从 组数×对端数 降到 对端数；接收端在一个 backend 里逐组处理，每组一个子事务隔离。
 */
typedef struct HbItem
{
    int     gidx;           /* RaftGroups->groups 下标 */
    int64   group_id;
    int64   term;
    int64   prev_idx;
    int64   prev_term;
    int64   commit;
} HbItem;

static bool    hb_collecting = false;
static bool    raft_in_hb_multi = false;    /* 本后端正在处理一条合并心跳（接收端） */
static HbItem *hb_items[RAFT_MAX_PEERS];
static int     hb_n[RAFT_MAX_PEERS];
static int     hb_cap = 0;
/* 对端执行合并心跳报错（老版本没有这个函数等）后，这段时间内对它退回逐组单发 */
static TimestampTz hb_unsupported_until[RAFT_MAX_PEERS];

/* 收集模式下把一条纯心跳记进对端批次；返回 false = 没在收集（调用方照旧单发） */
static bool
hb_defer(RaftGroupCtx *ctx, int peer_slot, int64 term, int64 prev_idx,
         int64 prev_term, int64 leader_commit)
{
    HbItem *it;

    if (!hb_collecting || peer_slot < 0 || peer_slot >= RAFT_MAX_PEERS)
        return false;
    if (hb_unsupported_until[peer_slot] != 0 &&
        GetCurrentTimestamp() < hb_unsupported_until[peer_slot])
        return false;
    if (hb_n[peer_slot] >= hb_cap)
        return false;           /* 满了（不应发生：容量 = max_groups）就退回单发 */
    it = &hb_items[peer_slot][hb_n[peer_slot]++];
    it->gidx = (int) (ctx->g - RaftGroups->groups);
    it->group_id = ctx->group_id;
    it->term = term;
    it->prev_idx = prev_idx;
    it->prev_term = prev_term;
    it->commit = leader_commit;
    return true;
}
/* P7-W6：记下对端最近一次应答 / 失败的时刻（多数派存活探测的原始数据） */
static void
peer_stamp(RaftGroupCtx *ctx, int peer_slot, bool responded)
{
    TimestampTz now = GetCurrentTimestamp();

    if (peer_slot < 0 || peer_slot >= RAFT_MAX_PEERS)
        return;
    SpinLockAcquire(&ctx->log->mutex);
    if (responded)
        ctx->log->peer_last_ack[peer_slot] = now;
    else
        ctx->log->peer_last_fail[peer_slot] = now;
    SpinLockRelease(&ctx->log->mutex);
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
    bool  keepalive = false;
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
        ctx->log->leader_durable_index = ctx->log->last_log_index;   /* 当选时日志里已有的条目视为已落盘 */
        ctx->log->leader_self_hole = 0;
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
        if (send_install_snapshot(ctx, &peers[peer_slot], term, base_idx, false))
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

        /*
         * ★ P7-N46 自愈（2026-10-04）：控制面要发的条目环里没有、SQL 日志里也没有（承载
         * apply 的事务回滚时连带撤销了那几行，日志出现洞）⇒ 逐条追平永远卡在洞上。
         * 改为现做一份当前已 apply 位点的快照发过去（控制面快照就是两张元数据表的样子）。
         */
        if (!has_entry && next_idx <= last_idx && ctx->group_id == RAFT_CONTROL_GROUP)
        {
            int64 applied_now;

            SpinLockAcquire(&ctx->log->mutex);
            applied_now = ctx->log->last_applied;
            SpinLockRelease(&ctx->log->mutex);
            if (applied_now >= next_idx &&
                send_install_snapshot(ctx, &peers[peer_slot], term, base_idx, true))
            {
                elog(LOG, "pg_raft: 控制面日志在 %lld 处有洞，改用现做的快照（到 %lld）追平节点 %d",
                     (long long) next_idx, (long long) applied_now, peers[peer_slot].node_id);
                return;
            }
        }
    }

    /*
     * prev 无从取证时不能硬发：prev_term 会当成 0 送出去，对端必拒，
     * 白白把 next_index 往下推。不发条目、只发心跳（保持对端选举计时器）。
     *
     * ★ P7-N45（2026-10-03）：上面这段注释说"不往下推"，代码却照样推了 —— 心跳仍带着
     * prev_idx / prev_term=0 发出去，对端必拒，下面的失败分支 next_index--。BGW 每个 tick
     * 一次，落后到环外的 follower 被越推越远；追平通道每 5 s 补几条，两边对冲，
     * 永远追不上（实测 :5435 的控制面日志停在 2184、leader 已到 2198，next_index 被推到 2004；
     * 它因此没 apply 到"本节点当选"那条登记，读闸门一直不开）。
     * 现在改发**纯保活**：prev=0/0、leader_commit=0 —— 对端不做一致性检查、也推不动提交点
     * （见 handle_append_entries 的提交上限），只刷新选举计时器；回执不碰 next/match。
     */
    if (!prev_known)
    {
        has_entry = false;
        keepalive = true;
        prev_idx = 0;
        prev_term = 0;
        leader_commit = 0;
    }

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
            /*
             * 2026-10-12：本地取不到字节也照发（只带头）。原来降级为心跳 —— 字节被截掉的 leader
             * 永远追不平别人，哪怕对方流里本来就有这条（P059）。对方没有同一条就不收不 ack，
             * 与原来一样不推进；有就收下（见 data_entry_present_locally）。
             */
            data_hex = data_entry_fetch_hex(ctx, entry_partition_lsn(entry.payload));
        }
    }

    if (has_entry)
    {
        if (!send_append_entries_rpc(ctx, &peers[peer_slot], term, leader_id,
                                     prev_idx, prev_term, leader_commit,
                                     entry.index, entry.term,
                                     entry.op_type, entry.payload, data_hex,
                                     &rt, &ok_flag))
        {
            peer_stamp(ctx, peer_slot, false);
            return;
        }
    }
    else
    {
        /* P7-N41：tick 里的纯心跳并进对端批次，tick 末尾一条 RPC 发完 */
        if (hb_defer(ctx, peer_slot, term, prev_idx, prev_term, leader_commit))
            return;
        if (!send_append_entries_rpc(ctx, &peers[peer_slot], term, leader_id,
                                     prev_idx, prev_term, leader_commit,
                                     0, 0, NULL, NULL, NULL, &rt, &ok_flag))
        {
            peer_stamp(ctx, peer_slot, false);
            return;
        }
    }
    peer_stamp(ctx, peer_slot, true);   /* P7-W6：RPC 有回应（接受或拒绝）= 对端活着 */

    if (rt > term)
    {
        step_down_if_higher(ctx, rt);
        return;
    }

    if (keepalive)
        return;                 /* P7-N45：保活心跳的回执不说明对端日志在哪 */

    if (ok_flag)
    {
        SpinLockAcquire(&ctx->log->mutex);
        if (has_entry)
        {
            ctx->log->peer_match_index[peer_slot] = entry.index;
            ctx->log->peer_next_index[peer_slot] = entry.index + 1;
        }
        else if (!(pg_raft_replicate_pipeline && ctx->group_id != RAFT_CONTROL_GROUP) &&
                 !rs_stream_data(ctx))
            ctx->log->peer_match_index[peer_slot] = prev_idx;   /* 流水线下数据组心跳不抬 match（见 hb_apply_one） */
        SpinLockRelease(&ctx->log->mutex);
    }
    else if (!has_entry && rs_stream_data(ctx))
    {
        /* 复制流：心跳的 prev 是乐观推进后的 next-1（那一批可能还在路上），被拒不说明对端缺什么；
         * 回退由复制流按回包处理 */
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

/* P7-N41：打开收集模式（BGW tick 开头调；容量按 max_groups 一次性分配） */
static void
hb_begin(void)
{
    int i;

    if (hb_cap < RAFT_MAX_GROUPS)
    {
        for (i = 0; i < RAFT_MAX_PEERS; i++)
        {
            if (hb_items[i] != NULL)
                pfree(hb_items[i]);
            hb_items[i] = (HbItem *) MemoryContextAllocZero(TopMemoryContext,
                                                            sizeof(HbItem) * RAFT_MAX_GROUPS);
        }
        hb_cap = RAFT_MAX_GROUPS;
    }
    for (i = 0; i < RAFT_MAX_PEERS; i++)
        hb_n[i] = 0;
    hb_collecting = true;
}

/* 一条回执落到本组：规则与 replicate_to_peer 的纯心跳分支一致 */
static void
hb_apply_one(int slot, const HbItem *it, bool responded, int64 rt, int ok)
{
    RaftGroupState *g;
    RaftGroupCtx    ctx;
    bool            same;

    if (it->gidx < 0 || it->gidx >= RAFT_MAX_GROUPS)
        return;
    g = &RaftGroups->groups[it->gidx];
    if (!g->in_use || g->group_id != it->group_id)
        return;                 /* 组在 tick 期间被拆掉 / 槽位换了人 */
    ctx.group_id = g->group_id;
    ctx.g = g;
    ctx.cons = &g->cons;
    ctx.log = &g->log;

    peer_stamp(&ctx, slot, responded);
    if (!responded)
        return;
    if (rt > it->term)
    {
        step_down_if_higher(&ctx, rt);
        return;
    }

    SpinLockAcquire(&ctx.cons->mutex);
    same = (ctx.cons->state == RAFT_LEADER && ctx.cons->current_term == it->term);
    SpinLockRelease(&ctx.cons->mutex);
    if (!same)
        return;                 /* 发出后本组已换 term / 下台：旧回执作废 */

    SpinLockAcquire(&ctx.log->mutex);
    if (ok)
    {
        /*
         * 2026-10-17：流水线下数据组的心跳回执不抬 match —— 心跳的 prev 是乐观推进后的 next-1，
         * 对端只要环里有就回 ok，而那一批可能还没在对端提交落盘。match 只认带条目的批量回执。
         */
        if (it->prev_idx > ctx.log->peer_match_index[slot] &&
            !(pg_raft_replicate_pipeline && ctx.group_id != RAFT_CONTROL_GROUP) &&
            !rs_stream_data(&ctx))
            ctx.log->peer_match_index[slot] = it->prev_idx;
    }
    else if (it->prev_idx > 0 && ctx.log->peer_next_index[slot] > 1 && !rs_stream_data(&ctx))
        ctx.log->peer_next_index[slot]--;   /* P7-N45：prev=0 的保活被拒不说明日志位置 */
    SpinLockRelease(&ctx.log->mutex);
}

/*
 * P7-N41：关掉收集模式，把各对端批次各用一条 RPC **并发**发出、统一收回执。
 * 回执格式：每组一行 "group_id term ok"，换行分隔。
 */
static void
hb_flush(void)
{
    PGconn        *conns[RAFT_MAX_PEERS];
    bool           sent[RAFT_MAX_PEERS];
    TimestampTz    start;
    int            s;
    int            k;

    hb_collecting = false;
    start = GetCurrentTimestamp();

    for (s = 0; s < n_peers && s < RAFT_MAX_PEERS; s++)
    {
        StringInfoData sql;

        conns[s] = NULL;
        sent[s] = false;
        if (hb_n[s] == 0)
            continue;
        if (peer_in_backoff(&peers[s]) ||
            (conns[s] = peer_conn_get(&peers[s])) == NULL)
        {
            if (conns[s] == NULL && !peer_in_backoff(&peers[s]))
                peer_mark_result(&peers[s], false);
            for (k = 0; k < hb_n[s]; k++)
                hb_apply_one(s, &hb_items[s][k], false, 0, 0);
            continue;
        }

        initStringInfo(&sql);
        appendStringInfo(&sql, "SELECT partdist.pg_raft_heartbeat_multi(%d, '", pg_raft_node_id);
        for (k = 0; k < hb_n[s]; k++)
        {
            HbItem *it = &hb_items[s][k];

            appendStringInfo(&sql, "%s%lld %lld %lld %lld %lld", k ? ";" : "",
                             (long long) it->group_id, (long long) it->term,
                             (long long) it->prev_idx, (long long) it->prev_term,
                             (long long) it->commit);
        }
        appendStringInfoString(&sql, "')");
        sent[s] = (PQsendQuery(conns[s], sql.data) == 1);
        pfree(sql.data);
        if (!sent[s])
        {
            peer_conn_reset(s);
            peer_mark_result(&peers[s], false);
            for (k = 0; k < hb_n[s]; k++)
                hb_apply_one(s, &hb_items[s][k], false, 0, 0);
        }
    }

    for (s = 0; s < n_peers && s < RAFT_MAX_PEERS; s++)
    {
        PGresult   *res;
        bool        got = false;

        if (!sent[s])
            continue;
        res = pq_result_bounded(conns[s], start, RAFT_RPC_EXEC_TIMEOUT_MS);
        if (res != NULL && PQresultStatus(res) == PGRES_TUPLES_OK &&
            PQntuples(res) == 1 && !PQgetisnull(res, 0, 0))
        {
            char   *p = PQgetvalue(res, 0, 0);

            got = true;
            peer_mark_result(&peers[s], true);
            /* 回执与请求同序，但按 group_id 对位更稳：逐行找回对应的请求项 */
            while (*p)
            {
                long long gid, rt;
                int       ok, used = 0;

                if (sscanf(p, "%lld %lld %d%n", &gid, &rt, &ok, &used) != 3 || used == 0)
                    break;
                for (k = 0; k < hb_n[s]; k++)
                    if (hb_items[s][k].group_id == gid)
                    {
                        hb_apply_one(s, &hb_items[s][k], true, (int64) rt, ok);
                        break;
                    }
                p += used;
                while (*p == '\n' || *p == ' ')
                    p++;
            }
        }
        else if (res != NULL &&
                 PQresultStatus(res) == PGRES_FATAL_ERROR)
        {
            char    errmsg_buf[256];

            /*
             * 对端老版本（没有这个函数）或执行出错：对端活着。本批心跳当场逐组补发，
             * 之后 60 s 内对它退回逐组单发 —— 绝不能让心跳因为合并通道坏了而整批丢掉。
             */
            static TimestampTz last_warn = 0;

            got = true;
            peer_mark_result(&peers[s], true);
            hb_unsupported_until[s] = GetCurrentTimestamp() + (int64) 60000 * 1000;
            strlcpy(errmsg_buf, PQresultErrorMessage(res), sizeof(errmsg_buf));
            PQclear(res);
            res = NULL;
            for (k = 0; k < hb_n[s]; k++)
            {
                HbItem         *it = &hb_items[s][k];
                RaftGroupState *g;
                RaftGroupCtx    gctx;
                int64           rt = 0;
                int             ok = 0;
                bool            sent_ok;

                if (it->gidx < 0 || it->gidx >= RAFT_MAX_GROUPS)
                    continue;
                g = &RaftGroups->groups[it->gidx];
                if (!g->in_use || g->group_id != it->group_id)
                    continue;
                gctx.group_id = g->group_id;
                gctx.g = g;
                gctx.cons = &g->cons;
                gctx.log = &g->log;
                sent_ok = send_append_entries_rpc(&gctx, &peers[s], it->term, pg_raft_node_id,
                                                  it->prev_idx, it->prev_term, it->commit,
                                                  0, 0, NULL, NULL, NULL, &rt, &ok);
                hb_apply_one(s, it, sent_ok, rt, ok);
            }
            if (last_warn == 0 || TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 30000))
            {
                last_warn = GetCurrentTimestamp();
                elog(WARNING, "pg_raft: 合并心跳发往节点 %d 失败（60 s 内对它逐组单发）：%s",
                     peers[s].node_id, errmsg_buf);
            }
        }
        if (res != NULL)
            PQclear(res);
        if (!got)
        {
            peer_conn_reset(s);
            peer_mark_result(&peers[s], false);
            for (k = 0; k < hb_n[s]; k++)
                hb_apply_one(s, &hb_items[s][k], false, 0, 0);
        }
    }

    /* 回执可能推进了 match：本节点为 leader 的组重算一次提交点 */
    for (s = 0; s < RAFT_MAX_GROUPS; s++)
    {
        RaftGroupState *g = &RaftGroups->groups[s];
        RaftGroupCtx    ctx;
        int64           term;
        int64           nc;
        bool            leader;

        if (!g->in_use)
            continue;
        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;
        SpinLockAcquire(&ctx.cons->mutex);
        leader = (ctx.cons->state == RAFT_LEADER);
        term = ctx.cons->current_term;
        SpinLockRelease(&ctx.cons->mutex);
        if (!leader)
            continue;
        SpinLockAcquire(&ctx.log->mutex);
        if (ctx.log->repl_inited)
        {
            nc = compute_new_commit_index(&ctx, term);
            if (nc > ctx.log->commit_index)
                ctx.log->commit_index = nc;
        }
        SpinLockRelease(&ctx.log->mutex);
    }
}

/* ---- 选举 ---- */

/*
 * ───────────── P7-R5（2026-09-15）→ P7-N4（2026-09-17）：升主前置与登记不许阻塞 tick ─────────────
 *
 * 数据组新 leader 在 tick 里要做两件慢事：自连本节点跑升主前置（追平、推进 WAL 位点、闭合
 * in-doubt、修分叉、认领），再向 group0 leader 登记（一次同步复制到全体节点的 group0 提案 +
 * 协调者 apply）。
 *
 * P7-R5 的做法是"异步发送 + 等待期间每 heartbeat_ms 发一轮心跳 + 上界（升主前置 slice+8000 ms、
 * 登记 30 s），超时即 PQfinish、下个 tick 重来"。2026-09-17 跨组事务并发试跑证明这在长流上
 * 会**活锁**：
 *   ① 升主前置实测 69 s/次，远超 10 s 上界 —— 服务端每次都真的跑完了，结果却被丢掉，
 *      下一轮从头再来，永远登记不上，路由一直指着旧主；
 *   ② 超时路径直接 return，**不走截止期判断**，60 s 兜底永不触发；
 *   ③ 超时只 PQfinish 不 PQcancel，服务端会话照跑，同组升主前置实测堆到 6 个，互相拖慢；
 *   ④ 下一轮 PQconnectdb **阻塞**重连（新 backend 要加载 Citus/partdist/raft），堆满会话的
 *      节点上要好几秒，期间一个心跳都不发 —— 本节点领导的其他组随之被推翻，连锁漂主。
 *
 * 现在改成**跨 tick 的非阻塞状态机**，tick 里对这两件事只做"看一眼、推一步"：
 *   · 建连用 PQconnectStart/PQconnectPoll，套接字没就绪就留到下个 tick；
 *   · 语句发出去后每个 tick 只 PQconsumeInput + PQisBusy，**结果一定被收下**，
 *     不论它跑多久（有 5 分钟硬上界防服务端真的卡死，超了才 PQcancel 重来）；
 *   · 在途操作绑定 (组, term)；每个 tick 先扫一遍，组不在了 / 不再是该 term 的 leader
 *     ⇒ PQcancel 取消（别让一个已下台的节点把"认领分片"做完）；
 *   · 同一 (组, term) 升主前置返回 1 之后记为"已准备"，登记没成功只重发登记，不再重跑前置；
 *   · 截止期按 (组, term) 从首次发出算起，在每次拿到"还没追平"时判；
 *     pg_raft.promote_catchup_deadline_ms = 0 表示**永不兜底**（原实现是立即放行，与 GUC
 *     描述相反，P7-N6）；兜底改为调 pg_raft_promote_prepare_ex(..., force=true)：
 *     仍尽力追一片，但不再以追平为前提，**其余升主步骤照做**（原兜底一步都没做）。
 * 每条连接一次只挂一条在途语句，多个组要升主时自然轮流；tick 不再为它们等待，
 * 心跳按正常 tick 节奏发。
 */
#define RAFT_PROMOTE_INFLIGHT_MAX_MS   300000   /* 单次升主前置硬上界：防服务端真卡死 */
#define RAFT_REPORT_EXEC_TIMEOUT_MS     30000   /* 单次登记上界 */
#define RAFT_ACONN_CONNECT_TIMEOUT_MS   30000   /* 非阻塞建连上界 */

typedef enum
{
    RAFT_ACONN_IDLE = 0,        /* 无连接 */
    RAFT_ACONN_CONNECTING,      /* PQconnectStart 之后、握手未完 */
    RAFT_ACONN_READY,           /* 已连上、无在途语句 */
    RAFT_ACONN_BUSY             /* 有在途语句 */
} RaftAconnState;

typedef enum
{
    RAFT_ACONN_PENDING = 0,
    RAFT_ACONN_DONE,
    RAFT_ACONN_BROKEN
} RaftAconnPoll;

typedef struct RaftAsyncConn
{
    const char *tag;                    /* 日志用 */
    PGconn     *conn;
    RaftAconnState state;
    PostgresPollingStatusType poll;
    TimestampTz started;                /* CONNECTING：发起建连；BUSY：发出语句 */
    int         target_node;            /* 连到哪个节点（登记连接随 group0 leader 变） */
    int64       owner_group;            /* BUSY 时：语句属于哪个组 */
    int64       owner_term;             /* BUSY 时：发出时该组的 term */
    PGresult   *first_res;              /* 已收到、尚未交给调用方的首个结果 */
} RaftAsyncConn;

/*
 * ★ P7-N54（2026-10-03）：升主前置与登记各用一个**小连接池**，不再全节点只有一条。
 * 原来一个节点同一时间只有一条在途的升主前置（和一条登记）：一台 worker 一口气当选几十个组
 * （建 128 组、或别的节点宕机后接手）时，其余组排队等几分钟才登记上路由；期间供副本被拒
 * （"本节点不是分片的主 / 控制面登记的主=未登记"），任期还可能再变一轮。
 * 每条连接仍然一次只挂一条在途语句、绑定 (组, term)；池子大小 RAFT_ACONN_POOL。
 */
#define RAFT_ACONN_POOL 4
static RaftAsyncConn promote_pool[RAFT_ACONN_POOL] = {{"升主前置"}, {"升主前置"}, {"升主前置"}, {"升主前置"}};
static RaftAsyncConn report_pool[RAFT_ACONN_POOL] = {{"登记"}, {"登记"}, {"登记"}, {"登记"}};
/* ★ P7-N39：控制面新 leader 补提"本任期空条目"用的连接（tick 里不能做 SPI，走本机 libpq） */
static RaftAsyncConn ctrl_noop_aconn = {"控制面空条目"};

static void
aconn_reset(RaftAsyncConn *ac, bool cancel)
{
    if (ac->conn != NULL)
    {
        if (cancel && ac->state == RAFT_ACONN_BUSY)
        {
            PGcancel *cn = PQgetCancel(ac->conn);
            char      errbuf[256];

            if (cn != NULL)
            {
                (void) PQcancel(cn, errbuf, sizeof(errbuf));
                PQfreeCancel(cn);
            }
        }
        PQfinish(ac->conn);
    }
    if (ac->first_res != NULL)
        PQclear(ac->first_res);
    ac->conn = NULL;
    ac->state = RAFT_ACONN_IDLE;
    ac->target_node = 0;
    ac->owner_group = 0;
    ac->owner_term = 0;
    ac->first_res = NULL;
}

/*
 * 推进建连。返回 1 = 可以发语句；0 = 还在握手（下个 tick 再推）；-1 = 失败（已重置）。
 * 调用方保证当前不是 BUSY。绝不阻塞：只在套接字已就绪时调 PQconnectPoll。
 */
static int
aconn_ensure(RaftAsyncConn *ac, const char *host, int port, int target_node)
{
    if (ac->state == RAFT_ACONN_READY)
    {
        if (ac->target_node == target_node && PQstatus(ac->conn) == CONNECTION_OK)
            return 1;
        aconn_reset(ac, false);
    }
    else if (ac->state == RAFT_ACONN_CONNECTING && ac->target_node != target_node)
        aconn_reset(ac, false);     /* 握手途中目标换了（如 group0 换主）：重连新目标 */

    if (ac->state == RAFT_ACONN_IDLE)
    {
        char conninfo[512];

        pg_raft_format_conninfo(host, port, conninfo, sizeof(conninfo));
        ac->conn = PQconnectStart(conninfo);
        if (ac->conn == NULL || PQstatus(ac->conn) == CONNECTION_BAD)
        {
            elog(WARNING, "pg_raft: %s专用连接 %s:%d 发起失败: %s", ac->tag, host, port,
                 ac->conn ? PQerrorMessage(ac->conn) : "PQconnectStart 返回 NULL");
            aconn_reset(ac, false);
            return -1;
        }
        ac->state = RAFT_ACONN_CONNECTING;
        ac->poll = PGRES_POLLING_WRITING;
        ac->started = GetCurrentTimestamp();
        ac->target_node = target_node;
    }

    for (;;)
    {
        int            sock;
        fd_set         rf, wf;
        struct timeval tv;

        if (ac->poll == PGRES_POLLING_OK)
        {
            ac->state = RAFT_ACONN_READY;
            return 1;
        }
        sock = PQsocket(ac->conn);
        if (ac->poll == PGRES_POLLING_FAILED || sock < 0)
        {
            elog(WARNING, "pg_raft: %s专用连接 %s:%d 失败: %s", ac->tag, host, port,
                 PQerrorMessage(ac->conn));
            aconn_reset(ac, false);
            return -1;
        }
        FD_ZERO(&rf);
        FD_ZERO(&wf);
        if (ac->poll == PGRES_POLLING_READING)
            FD_SET(sock, &rf);
        else
            FD_SET(sock, &wf);
        tv.tv_sec = 0;
        tv.tv_usec = 0;
        if (select(sock + 1, &rf, &wf, NULL, &tv) <= 0)
            break;              /* 没就绪：留到下个 tick */
        ac->poll = PQconnectPoll(ac->conn);
    }

    if (TimestampDifferenceExceeds(ac->started, GetCurrentTimestamp(),
                                   RAFT_ACONN_CONNECT_TIMEOUT_MS))
    {
        elog(WARNING, "pg_raft: %s专用连接 %s:%d 握手超过 %d ms，放弃重连",
             ac->tag, host, port, RAFT_ACONN_CONNECT_TIMEOUT_MS);
        aconn_reset(ac, false);
        return -1;
    }
    return 0;
}

static bool
aconn_send(RaftAsyncConn *ac, const char *sql, int64 group_id, int64 term)
{
    if (!PQsendQuery(ac->conn, sql))
    {
        elog(WARNING, "pg_raft: %s发送失败(组 %lld): %s", ac->tag,
             (long long) group_id, PQerrorMessage(ac->conn));
        aconn_reset(ac, false);
        return false;
    }
    ac->state = RAFT_ACONN_BUSY;
    ac->started = GetCurrentTimestamp();
    ac->owner_group = group_id;
    ac->owner_term = term;
    return true;
}

/*
 * 非阻塞收结果。DONE 时 *out 为首个结果（可能是错误结果；也可能为 NULL = 没有结果），
 * 调用方负责 PQclear；连接回到 READY。BROKEN 时连接已不可用，调用方须 aconn_reset。
 */
static RaftAconnPoll
aconn_poll(RaftAsyncConn *ac, PGresult **out)
{
    *out = NULL;
    if (!PQconsumeInput(ac->conn))
        return RAFT_ACONN_BROKEN;
    while (!PQisBusy(ac->conn))
    {
        PGresult *r = PQgetResult(ac->conn);

        if (r == NULL)
        {
            *out = ac->first_res;
            ac->first_res = NULL;
            ac->state = RAFT_ACONN_READY;
            ac->owner_group = 0;
            ac->owner_term = 0;
            return RAFT_ACONN_DONE;
        }
        if (ac->first_res == NULL)
            ac->first_res = r;
        else
            PQclear(r);
    }
    return RAFT_ACONN_PENDING;
}

/* 在途语句所属的组若已不在、或不再是发出时那个 term 的 leader ⇒ 取消 */
static void
aconn_sweep_one(RaftAsyncConn *ac)
{
    RaftGroupCtx ctx;
    int          state;
    int64        term;

    if (ac->state != RAFT_ACONN_BUSY)
        return;
    if (!raft_group_ctx(ac->owner_group, &ctx))
    {
        elog(LOG, "pg_raft: 组 %lld 已不存在，取消在途%s", (long long) ac->owner_group, ac->tag);
        aconn_reset(ac, true);
        return;
    }
    SpinLockAcquire(&ctx.cons->mutex);
    state = ctx.cons->state;
    term = ctx.cons->current_term;
    SpinLockRelease(&ctx.cons->mutex);
    if (state != RAFT_LEADER || term != ac->owner_term)
    {
        elog(LOG, "pg_raft: 组 %lld 已不再是 term %lld 的 leader（现 term %lld），取消在途%s",
             (long long) ac->owner_group, (long long) ac->owner_term, (long long) term, ac->tag);
        aconn_reset(ac, true);
    }
}

static void report_sweep_orphan(int k);     /* P7-N71，定义在批量登记处 */

static void
promote_async_sweep(void)
{
    int i;

    for (i = 0; i < RAFT_ACONN_POOL; i++)
    {
        aconn_sweep_one(&promote_pool[i]);
        aconn_sweep_one(&report_pool[i]);
        report_sweep_orphan(i);     /* P7-N71 */
    }
    aconn_sweep_one(&ctrl_noop_aconn);
}

/*
 * P7-N54：从池里给 (组, term) 挑一条连接：它自己在途的那条优先；否则挑空闲的
 * （已连上 > 正在握手 > 没连），都被别的组占着就返回 NULL（排队）。
 */
static RaftAsyncConn *
aconn_pick(RaftAsyncConn *pool, int64 group, int64 term)
{
    RaftAsyncConn *best = NULL;
    int            best_rank = -1;
    int            i;

    for (i = 0; i < RAFT_ACONN_POOL; i++)
    {
        int rank;

        if (pool[i].state == RAFT_ACONN_BUSY)
        {
            if (pool[i].owner_group == group && pool[i].owner_term == term)
                return &pool[i];
            continue;
        }
        rank = (pool[i].state == RAFT_ACONN_READY) ? 3 :
               (pool[i].state == RAFT_ACONN_CONNECTING) ? 2 : 1;
        if (rank > best_rank)
        {
            best = &pool[i];
            best_rank = rank;
        }
    }
    return best;
}

/*
 * 每个 (组, term) 的升主进度，BGW 进程本地（单进程、条数不超过 max_groups，
 * 不值得动共享内存）。进程重启即重新计时，语义上等价于重新开始追平。
 */
typedef struct PromoteSlot
{
    int64       group_id;       /* 0 = 空 */
    int64       term;
    TimestampTz first_try;      /* 本 term 首次发出升主前置；0 = 尚未发出 */
    bool        prepared;       /* 本 term 升主前置已返回 1，只差登记 */
    bool        force;          /* 已过截止期，下一轮走兜底 */
    bool        queued_logged;  /* "连接被别的组占着"只报一次 */
} PromoteSlot;

static PromoteSlot *promote_slots = NULL;
static int          promote_nslots = 0;

static PromoteSlot *
promote_slot_get(int64 group_id, int64 term)
{
    int          i;
    PromoteSlot *free_slot = NULL;

    if (promote_slots == NULL)
    {
        promote_nslots = RAFT_MAX_GROUPS;
        promote_slots = (PromoteSlot *)
            MemoryContextAllocZero(TopMemoryContext,
                                   sizeof(PromoteSlot) * (Size) promote_nslots);
    }
    for (i = 0; i < promote_nslots; i++)
    {
        PromoteSlot *ps = &promote_slots[i];

        if (ps->group_id == group_id)
        {
            if (ps->term != term)
            {
                memset(ps, 0, sizeof(PromoteSlot));
                ps->group_id = group_id;
                ps->term = term;
            }
            return ps;
        }
        if (free_slot == NULL && ps->group_id == 0)
            free_slot = ps;
    }
    /*
     * 没有空槽：回收"组已不在、或本节点已不是该 term 的 leader"的槽。
     * 正常路径里槽在登记成功 / 丢领导权时就清了；漏网的是"当选后、登记前组被删"
     * —— 测试会反复建删组，不回收的话积满 max_groups 条之后升主就再也发不出去。
     */
    for (i = 0; free_slot == NULL && i < promote_nslots; i++)
    {
        PromoteSlot *ps = &promote_slots[i];
        RaftGroupCtx gctx;
        bool         stale = true;

        if (raft_group_ctx(ps->group_id, &gctx))
        {
            SpinLockAcquire(&gctx.cons->mutex);
            stale = !(gctx.cons->state == RAFT_LEADER && gctx.cons->current_term == ps->term);
            SpinLockRelease(&gctx.cons->mutex);
        }
        if (stale)
        {
            memset(ps, 0, sizeof(PromoteSlot));
            free_slot = ps;
        }
    }
    if (free_slot != NULL)
    {
        free_slot->group_id = group_id;
        free_slot->term = term;
    }
    return free_slot;
}

static void
promote_slot_clear(int64 group_id)
{
    int i;

    for (i = 0; promote_slots != NULL && i < promote_nslots; i++)
        if (promote_slots[i].group_id == group_id)
            memset(&promote_slots[i], 0, sizeof(PromoteSlot));
}

/*
 * data_group_promote_prepare — 上报前把本节点该分片"准备成主"（非阻塞）。
 *
 * 做的事在 partdist.pg_raft_promote_prepare_ex（见 pg_raft--1.0.sql）：把惰性回放追平到 Raft
 * 已提交位点、推进本地 WAL 位点、闭合 in-doubt 分布式事务、修分叉、认领无主 RUNNING。
 * 本函数只管"发出去、收回来、判截止期"。
 *
 * 返回 1 = 本 term 已准备好（可以登记）；0 = 还没好（在途 / 排队 / 没追平）；
 * -1 = 永不放行（没有本地副本 / 快路径分叉 / 收了 WAL 却没有回放槽位）。
 */
static int
data_group_promote_prepare(RaftGroupCtx *ctx, int64 term)
{
    PromoteSlot *ps = promote_slot_get(ctx->group_id, term);
    TimestampTz  now = GetCurrentTimestamp();
    char         sql[256];
    int          verdict = 0;

    RaftAsyncConn *pa;

    if (ps == NULL)
        return 0;               /* 按 max_groups 分配，不会满；防御 */
    if (ps->prepared)
        return 1;

    pa = aconn_pick(promote_pool, ctx->group_id, term);
    if (pa == NULL)
    {
        /* 池子里的连接全被别的组占着：排队，报一次 */
        if (!ps->queued_logged)
        {
            ps->queued_logged = true;
            elog(LOG, "pg_raft: 组 %lld term %lld 升主前置排队：%d 条连接都被别的组占用",
                 (long long) ctx->group_id, (long long) term, RAFT_ACONN_POOL);
        }
        return 0;
    }

    if (pa->state == RAFT_ACONN_BUSY)
    {
        PGresult     *res = NULL;
        RaftAconnPoll pr;

        if (pa->owner_group != ctx->group_id || pa->owner_term != term)
        {
            /* 连接被别的组占着：排队。run 4 里这条静默路径让人查了半天，报一次 */
            if (!ps->queued_logged)
            {
                ps->queued_logged = true;
                elog(LOG, "pg_raft: 组 %lld term %lld 升主前置排队：连接被组 %lld term %lld 占用",
                     (long long) ctx->group_id, (long long) term,
                     (long long) pa->owner_group, (long long) pa->owner_term);
            }
            return 0;
        }

        pr = aconn_poll(pa, &res);
        if (pr == RAFT_ACONN_PENDING)
        {
            if (TimestampDifferenceExceeds(pa->started, now,
                                           RAFT_PROMOTE_INFLIGHT_MAX_MS))
            {
                elog(WARNING,
                     "pg_raft: 组 %lld 单次升主前置已运行超过 %d ms，取消后重来",
                     (long long) ctx->group_id, RAFT_PROMOTE_INFLIGHT_MAX_MS);
                aconn_reset(pa, true);
            }
            return 0;
        }
        if (pr == RAFT_ACONN_BROKEN)
        {
            elog(WARNING, "pg_raft: 升主前置连接失效(组 %lld): %s，下个 tick 重来",
                 (long long) ctx->group_id, PQerrorMessage(pa->conn));
            aconn_reset(pa, false);
            return 0;
        }

        if (res != NULL && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
        {
            verdict = atoi(PQgetvalue(res, 0, 0));
            elog(LOG, "pg_raft: 组 %lld term %lld 升主前置返回 %d（本次 %ld ms，本 term 首发至今 %ld ms）",
                 (long long) ctx->group_id, (long long) term, verdict,
                 (long) ((now - pa->started) / 1000),
                 ps->first_try ? (long) ((now - ps->first_try) / 1000) : 0L);
        }
        else
            elog(WARNING, "pg_raft: 升主前置执行失败(组 %lld): %s",
                 (long long) ctx->group_id,
                 res != NULL ? PQresultErrorMessage(res) : "无结果");
        if (res != NULL)
            PQclear(res);

        /*
         * ★ verdict < 0 = 永不放行（DTX_2PC_DESIGN.md §9.5 第 6 步 b / R-P4-15 / R-P7-1）。
         * 与"还没追平"不是一回事：追不平是暂时状态，可以兜底；分叉/无副本是已知事实，
         * 放行等于让一个已知有问题的副本当主。清掉计时，交给重做基线后再参选。
         */
        if (verdict < 0)
        {
            ps->first_try = 0;
            ps->force = false;
            return -1;
        }
        if (verdict == 1)
        {
            /* P7-N70：兜底轮也只有真正追平才返回 1（追不平不放行），这里不再是"跳过追平" */
            if (ps->force)
                elog(LOG,
                     "pg_raft: 组 %lld 升主前置超过截止期 %d ms 后追平完成，上报登记（P7-N70）",
                     (long long) ctx->group_id, pg_raft_promote_catchup_deadline_ms);
            ps->prepared = true;
            return 1;
        }

        /* verdict == 0：这一片没追平 */
        if (!ps->force && pg_raft_promote_catchup_deadline_ms > 0 &&
            ps->first_try != 0 &&
            TimestampDifferenceExceeds(ps->first_try, now,
                                       pg_raft_promote_catchup_deadline_ms))
        {
            ps->force = true;
            elog(WARNING,
                 "pg_raft: 组 %lld 升主前置超过 %d ms 仍未追平，转入兜底：继续追平，追平毫无进展则让位（P7-N70：追不平不放行）",
                 (long long) ctx->group_id, pg_raft_promote_catchup_deadline_ms);
        }
        /* 连接已回到 READY，本 tick 直接发下一轮 */
    }

    if (pa->state == RAFT_ACONN_BUSY)
        return 0;
    if (aconn_ensure(pa, "127.0.0.1", PostPortNumber, pg_raft_node_id) != 1)
        return 0;

    snprintf(sql, sizeof(sql),
             "SELECT partdist.pg_raft_promote_prepare_ex(%lld, %d, %s)",
             (long long) ctx->group_id, pg_raft_promote_catchup_slice_ms,
             ps->force ? "true" : "false");
    if (aconn_send(pa, sql, ctx->group_id, term))
    {
        if (ps->first_try == 0)
            ps->first_try = now;
        elog(LOG, "pg_raft: 组 %lld term %lld 升主前置已发出%s",
             (long long) ctx->group_id, (long long) term, ps->force ? "（兜底 force）" : "");
    }
    return 0;
}

/*
 * ★ P7-N58：收集本节点上"升主前置已完成、仍待登记、仍是同一任期 leader"的其他组，
 * 拼成批量登记的项（不含 self）。最多 RAFT_REPORT_PIGGYBACK_MAX 个 —— 批条目要装进
 * 768 字节的环条目。返回收集到的个数；items 总是初始化过，由调用方 pfree。
 */
#define RAFT_REPORT_PIGGYBACK_MAX 12

/*
 * 每条登记连接上在途批次捎带了哪些组（不含连接的 owner 组本身）。只在该连接仍 BUSY
 * 且 owner 与记录一致时有效 —— 连接被重置 / 复用后记录自然作废，不用到处挂钩清理。
 */
static int64 batch_owner_gid[RAFT_ACONN_POOL];
static int64 batch_owner_term[RAFT_ACONN_POOL];
static int   batch_n[RAFT_ACONN_POOL];
static int64 batch_gid[RAFT_ACONN_POOL][RAFT_REPORT_PIGGYBACK_MAX];
static int64 batch_term[RAFT_ACONN_POOL][RAFT_REPORT_PIGGYBACK_MAX];

static bool
batch_slot_valid(int k)
{
    /*
     * ★ P7-N71：在途超过两倍登记超时的批次不再算数 —— 正常情况下主人的 tick 30 s 就会
     * 取消重来；超过这个时长只能是没人来收结果了，别让被捎带的组为它干等。
     */
    return batch_n[k] > 0 &&
        report_pool[k].state == RAFT_ACONN_BUSY &&
        report_pool[k].owner_group == batch_owner_gid[k] &&
        report_pool[k].owner_term == batch_owner_term[k] &&
        !TimestampDifferenceExceeds(report_pool[k].started, GetCurrentTimestamp(),
                                    2 * RAFT_REPORT_EXEC_TIMEOUT_MS);
}

/* 该组（该任期）自己是不是某条登记连接上在途语句的主人 */
static bool
group_owns_inflight_report(int64 gid, int64 term)
{
    int k;

    for (k = 0; k < RAFT_ACONN_POOL; k++)
        if (report_pool[k].state == RAFT_ACONN_BUSY &&
            report_pool[k].owner_group == gid && report_pool[k].owner_term == term)
            return true;
    return false;
}

/*
 * ★ P7-N71（2026-10-05）：登记连接上的在途语句，主人已经不需要登记了（被别的批次捎带着
 * 登记完、report_pending 已清）⇒ 主人的 tick 再也不会来收这条连接的结果，连接永远 BUSY，
 * 上面记着的批次也永远"有效"，被它捎带的组一直以为"正被在途批次带着"而不自己报 ——
 * 实测组 107661 因此 15 分钟以上登记不上，读写全被闸门挡住。
 * 取消这条连接、作废批次记录；被捎带的组保持待登记，自己重报（重复提案由任期栅栏挡掉）。
 */
static void
report_sweep_orphan(int k)
{
    RaftAsyncConn *ra = &report_pool[k];
    RaftGroupCtx   c;

    if (ra->state != RAFT_ACONN_BUSY)
        return;
    if (raft_group_ctx(ra->owner_group, &c) && c.g->report_pending)
        return;                 /* 主人还在等结果，交给它自己收 */
    elog(LOG, "pg_raft: 组 %lld 已登记完成，取消它挂在登记连接上的在途语句（被捎带的 %d 个组改为自己重报，P7-N71）",
         (long long) ra->owner_group, batch_n[k]);
    batch_n[k] = 0;
    aconn_reset(ra, true);
}

/* 该组（该任期）是否正被别的在途批次捎带着 */
static bool
group_in_inflight_batch(int64 gid, int64 term)
{
    int k;
    int j;

    for (k = 0; k < RAFT_ACONN_POOL; k++)
    {
        if (!batch_slot_valid(k))
            continue;
        for (j = 0; j < batch_n[k]; j++)
            if (batch_gid[k][j] == gid && batch_term[k][j] == term)
                return true;
    }
    return false;
}

/* 批次已提交：被捎带的组若仍在同一任期当 leader，就地清掉待登记 */
static void
batch_mark_registered(int k)
{
    int j;

    for (j = 0; j < batch_n[k]; j++)
    {
        RaftGroupCtx c;
        int          st;
        int64        t;

        if (!raft_group_ctx(batch_gid[k][j], &c))
            continue;
        SpinLockAcquire(&c.cons->mutex);
        st = c.cons->state;
        t = c.cons->current_term;
        SpinLockRelease(&c.cons->mutex);
        if (st == RAFT_LEADER && t == batch_term[k][j] && c.g->report_pending)
        {
            c.g->report_pending = false;
            promote_slot_clear(batch_gid[k][j]);
        }
    }
    batch_n[k] = 0;
}

static int
report_collect_piggyback(int64 self_gid, StringInfo items, int64 *gids, int64 *terms)
{
    int i;
    int n = 0;

    initStringInfo(items);
    for (i = 0; promote_slots != NULL && i < promote_nslots &&
                n < RAFT_REPORT_PIGGYBACK_MAX; i++)
    {
        PromoteSlot *ps = &promote_slots[i];
        RaftGroupCtx c;
        int          st;
        int64        t;
        int          k;
        int          m = 0;

        if (ps->group_id == 0 || ps->group_id == self_gid || !ps->prepared)
            continue;
        if (group_in_inflight_batch(ps->group_id, ps->term))
            continue;           /* 已经被别的在途批次带着 */
        if (group_owns_inflight_report(ps->group_id, ps->term))
            continue;           /* P7-N71：它自己有在途登记，结果得由它自己来收 */
        if (!raft_group_ctx(ps->group_id, &c) || !c.g->report_pending)
            continue;
        SpinLockAcquire(&c.cons->mutex);
        st = c.cons->state;
        t = c.cons->current_term;
        SpinLockRelease(&c.cons->mutex);
        if (st != RAFT_LEADER || t != ps->term)
            continue;
        if (c.g->n_members <= 0)
            continue;           /* 成员集未知的走单条入口（退化规则在那边） */

        appendStringInfo(items, "%s[%lld,%d,%lld,[", n ? "," : "",
                         (long long) ps->group_id, pg_raft_node_id, (long long) t);
        for (k = 0; k < c.g->n_members; k++)
            if (c.g->members[k] != pg_raft_node_id)
                appendStringInfo(items, "%s%d", m++ ? "," : "", c.g->members[k]);
        appendStringInfoString(items, "]]");
        gids[n] = ps->group_id;
        terms[n] = t;
        n++;
    }
    return n;
}

/*
 * 数据组新任 leader 向控制面登记（切主重构的上报半程，非阻塞）。
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
    RaftAsyncConn *ra;
    RaftGroupCtx ctx0;
    int          leader0;
    int64        term;
    int          state;
    int          sec[RAFT_MAX_PEERS];
    int          n_sec = 0;
    int          i;
    int          slot = -1;
    char         arrbuf[192];
    char         sql[1024];
    int64        pb_gid[RAFT_REPORT_PIGGYBACK_MAX];
    int64        pb_term[RAFT_REPORT_PIGGYBACK_MAX];
    int          n_pb = 0;
    int          off;
    PGresult    *res = NULL;
    RaftAconnPoll pr;
    int          cr;

    SpinLockAcquire(&ctx->cons->mutex);
    state = ctx->cons->state;
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);
    if (state != RAFT_LEADER)
    {
        ctx->g->report_pending = false;
        promote_slot_clear(ctx->group_id);
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
    {
        int pr = data_group_promote_prepare(ctx, term);

        if (pr < 0)
        {
            /* P7-N14：不可升主就别占着 leader 位 */
            raft_abdicate_unpromotable(ctx);
            promote_slot_clear(ctx->group_id);
            return;
        }
        if (pr != 1)
            return;             /* 还没好，保留 report_pending 下个 tick 继续 */
    }

    /* P7-N58：本组正被别的在途批次捎带着 ⇒ 等那一批的结果，别再自己另提一遍 */
    if (group_in_inflight_batch(ctx->group_id, term))
        return;

    /* P7-N54：从登记连接池挑一条；全被别的组占着就排队 */
    ra = aconn_pick(report_pool, ctx->group_id, term);
    if (ra == NULL)
        return;

    /* 登记连接上若挂着本组本 term 的在途登记：只收结果 */
    if (ra->state == RAFT_ACONN_BUSY)
    {
        if (ra->owner_group != ctx->group_id || ra->owner_term != term)
            return;             /* 别的组在登记：排队 */

        for (i = 0; i < n_peers; i++)
            if (peers[i].node_id == ra->target_node)
            {
                slot = i;
                break;
            }

        pr = aconn_poll(ra, &res);
        if (pr == RAFT_ACONN_PENDING)
        {
            if (TimestampDifferenceExceeds(ra->started, GetCurrentTimestamp(),
                                           RAFT_REPORT_EXEC_TIMEOUT_MS))
            {
                elog(WARNING,
                     "pg_raft: group %lld 向控制面(node %d)登记超过 %d ms 未返回，取消后重来",
                     (long long) ctx->group_id, ra->target_node,
                     RAFT_REPORT_EXEC_TIMEOUT_MS);
                aconn_reset(ra, true);
                if (slot >= 0)
                    peer_mark_result(&peers[slot], false);
            }
            return;
        }
        if (pr == RAFT_ACONN_BROKEN)
        {
            elog(WARNING, "pg_raft: group %lld 登记连接失效: %s，下个 tick 重试",
                 (long long) ctx->group_id, PQerrorMessage(ra->conn));
            aconn_reset(ra, false);
            if (slot >= 0)
                peer_mark_result(&peers[slot], false);
            return;
        }
        if (res != NULL && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
        {
            long long v = atoll(PQgetvalue(res, 0, 0));

            if (v != 0)
            {
                int bk = (int) (ra - report_pool);

                /* P7-N58：批次已提交（或全部无需登记）⇒ 被捎带的组一并清掉待登记 */
                if (bk >= 0 && bk < RAFT_ACONN_POOL && batch_n[bk] > 0 &&
                    batch_owner_gid[bk] == ctx->group_id &&
                    batch_owner_term[bk] == term)
                    batch_mark_registered(bk);
                ctx->g->report_pending = false;
                promote_slot_clear(ctx->group_id);
                elog(LOG,
                     "pg_raft: group %lld leader node %d term %lld 已向控制面(node %d)登记 (ret=%lld)",
                     (long long) ctx->group_id, pg_raft_node_id,
                     (long long) term, ra->target_node, v);
            }
            if (slot >= 0)
                peer_mark_result(&peers[slot], true);
        }
        else
        {
            elog(WARNING, "pg_raft: group %lld 登记执行失败: %s",
                 (long long) ctx->group_id,
                 res != NULL ? PQresultErrorMessage(res) : "无结果");
            if (slot >= 0)
                peer_mark_result(&peers[slot], false);
        }
        if (res != NULL)
            PQclear(res);
        return;
    }

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

    /*
     * ★ P7-N58：捎带。本节点上还有别的组升主前置已完成、只差登记 ⇒ 合成一次批量登记
     * （master 侧合成一条控制面条目）。被捎带的组照样留着 report_pending，下个 tick
     * 自己再报一次时 master 按任期栅栏直接回 -1（一次 SPI 查询，不进日志）。
     */
    {
        StringInfoData items;
        int            extra;

        extra = report_collect_piggyback(ctx->group_id, &items, pb_gid, pb_term);
        n_pb = extra;

        if (extra > 0)
        {
            char one[256];
            int  k;

            /* 本组放第一项（批条目装不下时先保本组） */
            snprintf(one, sizeof(one), "[%lld,%d,%lld,[", (long long) ctx->group_id,
                     pg_raft_node_id, (long long) term);
            for (k = 0; k < n_sec; k++)
                snprintf(one + strlen(one), sizeof(one) - strlen(one), "%s%d",
                         k ? "," : "", sec[k]);
            strlcat(one, "]]", sizeof(one));
            snprintf(sql, sizeof(sql),
                     "SELECT partdist.pg_raft_report_data_leader_batch('[%s,%s]')",
                     one, items.data);
        }
        pfree(items.data);
    }

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

    cr = aconn_ensure(ra, peers[slot].host, peers[slot].port, leader0);
    if (cr < 0)
    {
        peer_mark_result(&peers[slot], false);
        return;
    }
    if (cr == 0)
        return;                 /* 握手未完，下个 tick 再推 */
    batch_n[(int) (ra - report_pool)] = 0;     /* 这条连接上的旧批次记录作废 */
    if (aconn_send(ra, sql, ctx->group_id, term) && n_pb > 0)
    {
        int bk = (int) (ra - report_pool);

        batch_owner_gid[bk] = ctx->group_id;
        batch_owner_term[bk] = term;
        memcpy(batch_gid[bk], pb_gid, sizeof(int64) * n_pb);
        memcpy(batch_term[bk], pb_term, sizeof(int64) * n_pb);
        batch_n[bk] = n_pb;
        elog(LOG, "pg_raft: group %lld 登记捎带本节点另外 %d 个待登记的组（P7-N58）",
             (long long) ctx->group_id, n_pb);
    }
}


/*
 * ★ P7-N26（2026-09-18）：当选即把本组的复制增量下界抬到**日志里最后一条数据条目**。
 *
 * last_data_plsn 是运行期游标，只在"本节点当 leader、propose 成功"时推进；它**不随
 * 丢主清零**。于是一个当过主、又当回主的节点，拿着上一任期的旧值开写：
 * replicate_group_upto 从 旧值+1 起把**前任主写的整段尾巴**（本节点早已作为 follower
 * 收进日志）逐条重新 propose，每条一次同步往返，全程持着复制认领位 —— 此间所有写入
 * 排队等认领位，超 60 s 即 `等待组 N 的复制认领位超过 60000 ms，prepare 失败`。
 * 实测（N25 回归 [5]，组 102768）：A 在 term 3 把 B 在 term 2 写的 plsn 140–216 共 77 条
 * 原样重提，fileset 交接广播卡了 110 s，协调者那笔 INSERT 失败。
 *
 * 那段尾巴已在本节点日志里，新 leader 的常规 AppendEntries（init_leader_replication 设好
 * next_index）自会把它复制齐，不需要也不应该作为新条目再提一遍。只抬不降：环里找不到
 * 数据条目（全被挤出窗口）时维持原值，退回修前行为（多提几条，正确性无损）。
 * BGW 可调：纯共享内存，环内读取不走 SPI（与 replicate_group_upto 的回推同一写法）。
 */
static void
data_group_refresh_last_plsn(RaftGroupCtx *ctx)
{
    int64   p;
    int64   last = 0;

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

    if (last > ctx->g->last_data_plsn)
    {
        elog(LOG, "pg_raft: 组 %lld 当选后把复制下界从 plsn %lld 抬到 %lld"
             "（前任写的尾巴已在本节点日志里，由常规 AppendEntries 复制，不再逐条重提）",
             (long long) ctx->group_id, (long long) ctx->g->last_data_plsn,
             (long long) last);
        ctx->g->last_data_plsn = last;
    }
}

static void
start_election(RaftGroupCtx *ctx, bool forced)
{
    int64 term;
    int64 last_log_idx = 0;
    int64 last_log_term = 0;
    int   votes = 1;
    int   majority = cluster_majority(ctx);
    char  msg[160];
    int   i;
    bool  won = false;

    /*
     * ★ P7-N50（2026-10-03）：PreVote（Raft 论文第 9.6 节）。
     *
     * 演示二实测：断电的 w2 拉起来后，自动归队的批量重供把各节点 CPU 吃满，w2 19 s 没收到心跳，
     * 超时竞选、把任期加一 —— 现任 leader 一收到更高任期就下台，w2 以 3/3 票拿走 32 个组的主，
     * 刚恢复的集群又是一轮全面切主、重供。Raft 原文管这叫 disruptive server。
     *
     * 做法：真选举之前先**预选**：带"假如我竞选的任期 term+1"问一圈，**谁都不改状态**；
     * 对方在一个选举超时之内收到过现任 leader 的心跳（或自己就是 leader）就不支持。
     * 预选凑不齐多数派就不竞选、任期不动，等现任 leader 的心跳把自己拉回来。
     * 受控切主（pg_raft_group_campaign ⇒ forced）照旧直接选，不走预选。
     */
    if (!forced)
    {
        int64 cur_term;
        int   pre = 1;

        SpinLockAcquire(&ctx->log->mutex);
        current_last_log_info_locked(ctx, &last_log_idx, &last_log_term);
        SpinLockRelease(&ctx->log->mutex);
        SpinLockAcquire(&ctx->cons->mutex);
        cur_term = ctx->cons->current_term;
        SpinLockRelease(&ctx->cons->mutex);

        snprintf(msg, sizeof(msg), "PV %lld %d %lld %lld %lld",
                 (long long) (cur_term + 1), pg_raft_node_id,
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
            if (rt > cur_term + 1)
            {
                step_down_if_higher(ctx, rt);
                return;
            }
            if (granted)
                pre++;
        }
        if (pre < majority)
        {
            SpinLockAcquire(&ctx->cons->mutex);
            reset_election_deadline_locked(ctx);
            SpinLockRelease(&ctx->cons->mutex);
            elog(DEBUG1, "pg_raft: group %lld node %d 预选只得 %d/%d 票（现任 leader 仍在），不竞选",
                 (long long) ctx->group_id, pg_raft_node_id, pre, majority);
            return;
        }
    }

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
        if (ctx->group_id != RAFT_CONTROL_GROUP)
            data_group_refresh_last_plsn(ctx);
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

/*
 * ★ P7-N39（2026-09-23）：控制面（0 号组）新 leader 补提一条**本任期空条目**。
 *
 * 缺陷：Raft 不允许 leader 直接提交前任任期的条目（只能靠提交一条本任期的条目把它们
 * 顺带带上）。数据组在升主前置里已经这么做了（P7-N35），**控制面没有人做**。于是换了
 * 任期之后，上一任期留下的尾巴会一直停在未提交、也就一直不 apply —— 那可能是一条
 * OP_PARTITION_PRIMARY（"谁是这个分片的主"），意味着各节点的 partition_map 和路由
 * 就此停在旧值，直到下一次有人往控制面写东西才被顺带带上。
 *
 * 实测（2026-09-23）：0 号组停在 last=1154 / commit=1153，1154 是任期 677 的
 * OP_PARTITION_PRIMARY，而 leader 已经在任期 678；这个状态下 citus_add_node 的
 * 控制面写入一直排在后面，整条命令挂死。手工 propose 一条 OP_NOOP 之后
 * commit 立刻推到 1155、5 个节点全部 applied。
 *
 * 做法与登记/升主前置同源：tick 里没有 SPI，把 SQL 经本机 libpq 异步发出去，
 * 收到结果再清状态；失败就下个 tick 重来（RAFT_CTRL_NOOP_RETRY_MS 限一下频率）。
 * 只在"确有前任任期尾巴"时才发 —— 本任期自己的在途条目会自然提交，不用打扰。
 */
static void
ctrl_commit_tail_noop(RaftGroupCtx *ctx)
{
    static TimestampTz last_try = 0;
    TimestampTz now = GetCurrentTimestamp();
    int64       last_idx, commit_idx, term, tail_term = 0;
    bool        got_tail;
    RaftLogEntry e;

    if (ctrl_noop_aconn.state == RAFT_ACONN_BUSY)
    {
        PGresult     *res = NULL;
        RaftAconnPoll pr = aconn_poll(&ctrl_noop_aconn, &res);

        if (pr == RAFT_ACONN_PENDING)
        {
            if (TimestampDifferenceExceeds(ctrl_noop_aconn.started, now,
                                           RAFT_PROMOTE_INFLIGHT_MAX_MS))
            {
                elog(WARNING, "pg_raft: 控制面空条目已发出超过 %d ms，取消后重来",
                     RAFT_PROMOTE_INFLIGHT_MAX_MS);
                aconn_reset(&ctrl_noop_aconn, true);
            }
            return;
        }
        if (pr == RAFT_ACONN_BROKEN)
        {
            elog(WARNING, "pg_raft: 控制面空条目连接失效：%s，下个 tick 重来",
                 PQerrorMessage(ctrl_noop_aconn.conn));
            aconn_reset(&ctrl_noop_aconn, false);
            return;
        }
        if (res != NULL && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
            elog(LOG, "pg_raft: 控制面本任期空条目已提交（idx=%s）——上一任期的尾巴随之提交（P7-N39）",
                 PQgetvalue(res, 0, 0));
        else
            elog(WARNING, "pg_raft: 控制面空条目提交失败：%s",
                 res != NULL ? PQresultErrorMessage(res) : "无结果");
        if (res != NULL)
            PQclear(res);
        return;
    }

    SpinLockAcquire(&ctx->log->mutex);
    last_idx = ctx->log->last_log_index;
    commit_idx = ctx->log->commit_index;
    got_tail = (last_idx > commit_idx) && log_get_entry_locked(ctx, last_idx, &e);
    if (got_tail)
        tail_term = e.term;
    SpinLockRelease(&ctx->log->mutex);

    if (last_idx <= commit_idx)
        return;                 /* 没有尾巴 */

    SpinLockAcquire(&ctx->cons->mutex);
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);

    /* 尾巴是本任期自己刚写的 ⇒ 正常在途，别打扰；只管前任任期留下的 */
    if (!got_tail || tail_term >= term)
        return;
    if (last_try != 0 && !TimestampDifferenceExceeds(last_try, now, RAFT_CTRL_NOOP_RETRY_MS))
        return;
    last_try = now;

    if (aconn_ensure(&ctrl_noop_aconn, "127.0.0.1", PostPortNumber, pg_raft_node_id) != 1)
        return;
    if (aconn_send(&ctrl_noop_aconn,
                   "SELECT partdist.pg_raft_group_propose(0, 'OP_NOOP', '{}')::text",
                   RAFT_CONTROL_GROUP, term))
        elog(LOG, "pg_raft: 控制面有上一任期(%lld)的尾巴未提交（idx %lld > commit %lld，本任期 %lld），"
                  "补提一条本任期空条目（P7-N39）",
             (long long) tail_term, (long long) last_idx, (long long) commit_idx, (long long) term);
}

/* 单组的一次状态机推进 */
static void
group_tick(RaftGroupCtx *ctx)
{
    int         state;
    TimestampTz deadline;
    TimestampTz now;
    bool        campaign;

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
    campaign = ctx->cons->campaign_pending;
    ctx->cons->campaign_pending = false;
    SpinLockRelease(&ctx->cons->mutex);

    if (state == RAFT_LEADER)
    {
        ctx->g->leader_seen_at = now;   /* P7-N62 */
        send_heartbeats(ctx);
        if (ctx->group_id != RAFT_CONTROL_GROUP && ctx->g->report_pending)
            data_group_try_report(ctx);
        else if (ctx->group_id == RAFT_CONTROL_GROUP)
            ctrl_commit_tail_noop(ctx);     /* P7-N39 */
        return;
    }

    /* P7-N57：已知没有本地分片的成员不主动竞选（受控切主 campaign 照旧） */
    if (!campaign && now >= deadline && ctx->group_id != RAFT_CONTROL_GROUP &&
        ctx->g->no_local_shard)
    {
        SpinLockAcquire(&ctx->cons->mutex);
        reset_election_deadline_locked(ctx);
        SpinLockRelease(&ctx->cons->mutex);
        return;
    }

    if (campaign || now >= deadline)
        start_election(ctx, campaign);
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

    /* P7-N4：先清掉已失主 / 已换 term 的组留下的在途升主前置与登记 */
    promote_async_sweep();

    {
        /*
         * ★ P7-N17 仪表：一次 tick 串行推进全部组，任何一个组的同步 RPC 被慢 peer
         * 拖住，本节点作为 leader 的**所有组**心跳都断档。run 4 里 group 0 每 15 s
         * 改选一次、数据组新主登记不上，日志里却没有任何一行能说明"tick 卡了多久"。
         * 超过选举超时的一半就 WARNING（10 s 限流），并点名最慢的组。
         */
        TimestampTz t0 = GetCurrentTimestamp();
        TimestampTz t_prev = t0;
        long        slowest_ms = 0;
        int64       slowest_gid = 0;
        static TimestampTz last_stall_warn = 0;
        long        total_ms;
        long        flush_ms = 0;

        hb_begin();             /* P7-N41：纯心跳先收集，循环后按对端合并发送 */
        for (i = 0; i < RAFT_MAX_GROUPS; i++)
        {
            RaftGroupState *g = &RaftGroups->groups[i];
            RaftGroupCtx    ctx;
            TimestampTz     t_now;
            long            ms;

            if (!g->in_use)
                continue;

            ctx.group_id = g->group_id;
            ctx.g = g;
            ctx.cons = &g->cons;
            ctx.log = &g->log;

            group_tick(&ctx);

            t_now = GetCurrentTimestamp();
            ms = (long) ((t_now - t_prev) / 1000);
            if (ms > slowest_ms)
            {
                slowest_ms = ms;
                slowest_gid = g->group_id;
            }
            t_prev = t_now;
        }
        {
            TimestampTz tf = GetCurrentTimestamp();

            hb_flush();
            t_prev = GetCurrentTimestamp();
            flush_ms = (long) ((t_prev - tf) / 1000);
        }
        total_ms = (long) ((t_prev - t0) / 1000);
        if (total_ms > pg_raft_election_timeout_ms / 2 &&
            (last_stall_warn == 0 || TimestampDifferenceExceeds(last_stall_warn, t_prev, 10000)))
        {
            last_stall_warn = t_prev;
            elog(WARNING,
                 "pg_raft: 共识 tick 耗时 %ld ms（选举超时 %d ms）：最慢的组 %lld 占 %ld ms，合并心跳往返 %ld ms；"
                 "期间本节点作为 leader 的各组心跳断档，follower 可能超时改选（P7-N17：同步 RPC 被慢 peer 拖住 / CPU 饱和）",
                 total_ms, pg_raft_election_timeout_ms, (long long) slowest_gid, slowest_ms, flush_ms);
        }
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
        ctx->log->leader_durable_index = ctx->log->last_log_index;   /* 当选时日志里已有的条目视为已落盘 */
        ctx->log->leader_self_hole = 0;
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

/*
 * expect_term > 0：只丢该任期的条目（调用方释放锁之后才来丢，期间本节点若已失主、
 * 环尾换成了新任期的条目，就不许动它）。0 = 不核对（原调用方）。
 */
static void
discard_uncommitted_entry_term(RaftGroupCtx *ctx, int64 idx, int64 expect_term)
{
    int   i;

    if (idx <= 0)
        return;


    SpinLockAcquire(&ctx->log->mutex);
    if (expect_term > 0)
    {
        RaftLogEntry *te = log_slot(ctx, idx);

        if (te->index != idx || te->term != expect_term)
        {
            SpinLockRelease(&ctx->log->mutex);
            return;
        }
    }
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

    /* T7.20：被丢弃的若是那条未提交的 CONFIG，成员集必须退回已提交态 */
    raft_config_rollback(ctx, idx - 1);

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

static void
discard_uncommitted_entry(RaftGroupCtx *ctx, int64 idx)
{
    discard_uncommitted_entry_term(ctx, idx, 0);
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

    /*
     * T7.20：CONFIG 条目在 **append 那一刻**生效，并且**必须重算多数派** ——
     * 上面那个 majority 是按旧成员集算的，而提交这条 CONFIG 本身要用的就是
     * 新成员集（Raft 的 append-time 规则）。沿用旧值会出现"按旧集算够了票、
     * 按新集其实不够"的提交，正是论文里反复强调的不安全点。
     */
    if (strcmp(op_type, RAFT_OP_CONFIG) == 0)
    {
        raft_config_apply(ctx, idx, payload);
        majority = cluster_majority(ctx);
    }

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

    /* T7.20：提交点越过那条 CONFIG ⇒ 成员变更定案 */
    raft_config_note_commit(ctx, committed_upto);

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

/*
 * pg_raft_group_committed_plsn(group_id) → bigint
 *
 * ★ P7-N35（2026-09-20）：本节点 raft 日志里**已提交**的最大数据位点（OP_PARWAL 的 partition_lsn）。
 *
 * 升主追平的上界原本只取 follower_partition_map.applied_part_lsn —— 那是"本节点已经 apply 到哪"，
 * 它由本节点自己的 apply 推进，CPU 忙、事务内复制跳过推进、或 apply 卡在某个锁上时都会落后。
 * 落后的那一截里若有**提交标记**，升主就不回放它，随后认领把那个分片 xid 判成 ABORTED ——
 * 已确认的提交消失。已提交的条目按定义是多数派持久化过的，回放它们不违反"不碰未提交条目"。
 */
PG_FUNCTION_INFO_V1(pg_raft_group_committed_plsn);
Datum
pg_raft_group_committed_plsn(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    RaftGroupCtx ctx;
    int64        idx;
    int64        commit_idx;
    int64        floor_idx;
    int64        plsn = 0;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_INT64(0);

    parse_peers();
    restore_groups_if_needed();
    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_INT64(0);

    SpinLockAcquire(&ctx.log->mutex);
    commit_idx = ctx.log->commit_index;
    floor_idx = ctx.log->last_log_index - RAFT_LOG_CAPACITY + 1;
    if (floor_idx < 1)
        floor_idx = 1;
    for (idx = commit_idx; idx >= floor_idx; idx--)
    {
        RaftLogEntry e;

        if (log_get_entry_locked(&ctx, idx, &e) &&
            strcmp(e.op_type, RAFT_OP_PARWAL) == 0)
        {
            plsn = entry_partition_lsn(e.payload);
            break;
        }
    }
    SpinLockRelease(&ctx.log->mutex);

    PG_RETURN_INT64(plsn);
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

/* P7-N56：两段 JSON 文本忽略空白后是否相同（控制面条目内容比对用） */
static bool
payload_text_equal(const char *a, const char *b)
{
    while (*a || *b)
    {
        while (*a == ' ' || *a == '\t' || *a == '\n')
            a++;
        while (*b == ' ' || *b == '\t' || *b == '\n')
            b++;
        if (*a != *b)
            return false;
        if (*a)
        {
            a++;
            b++;
        }
    }
    return true;
}

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
    int64        cfg_commit = 0;         /* T7.20：提交点快照，用于成员变更定案 */
    bool         fresh_append = false;   /* T7.24：本条是否落在本地日志末尾之后 */
    int64        conflict_plsn = -1;
    bool         data_preexisting = false;  /* 2026-10-12：不带字节的数据条目、本地已有同一条 */

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
    ctx->cons->last_leader_contact = GetCurrentTimestamp();   /* P7-N50：PreVote 依据 */
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

    /*
     * ★ P7-N42（2026-10-03，128 组建组实测）：本节点还没有这个分片（副本尚未供给）时，
     * 数据条目**连日志都不许进**。
     *
     * 原来先 append 进日志、再落字节，落不下就不 ack —— 但条目已经留在本节点日志里了。
     * leader 因凑不齐多数派把它自己那条丢掉（discard_uncommitted_entry），于是
     * **唯一有数据的 placement 日志最短**（0 条），两个还没供副本的成员各有 1 条：
     * 选举里 placement 永远拿不到票（日志不够新），没数据的成员当选后升主前置 -1、
     * 让位、退避后再当选…… 实测组 103985 卡到任期 23 仍无主，供副本永远发不出去。
     * 拒在 append 之前：没数据的成员日志保持与 leader 一致，placement 照常当选。
     */
    if (has_entry && strcmp(entry_op, RAFT_OP_PARWAL) == 0)
        ctx->g->no_local_shard = (group_local_partition(ctx) <= 0);   /* P7-N57 */
    if (has_entry && strcmp(entry_op, RAFT_OP_PARWAL) == 0 && ctx->g->no_local_shard)
    {
        static TimestampTz last_note = 0;

        if (last_note == 0 || TimestampDifferenceExceeds(last_note, GetCurrentTimestamp(), 10000))
        {
            last_note = GetCurrentTimestamp();
            elog(LOG, "pg_raft: group %lld 在本节点还没有分片副本，数据条目 idx=%lld 不进日志、不 ack（等 provision_shard_replica）",
                 (long long) ctx->group_id, (long long) entry_idx);
        }
        return true;
    }

    /*
     * 2026-10-12：leader 发来的数据条目不带字节（它本地那条没了，见 data_entry_present_locally）。
     * 本节点流里已有同一条才收；没有就像上面一样**拒在 append 之前**，日志与提交点都不动。
     */
    if (has_entry && strcmp(entry_op, RAFT_OP_PARWAL) == 0 && entry_data_hex == NULL)
    {
        rs_dq_flush();
        if (!data_entry_present_locally(ctx, entry_payload))
        {
            elog(LOG, "pg_raft: group %lld 数据条目 idx=%lld 没带字节、本节点流里也没有同一条，不收不 ack",
                 (long long) ctx->group_id, (long long) entry_idx);
            return true;
        }
        data_preexisting = true;
    }

    /*
     * 2026-10-12：带字节的数据条目，若落盘会在本地流里留空洞（编号 > 本地末尾 + 1），
     * **拒在 append 之前**（干净地回 ok=0），而不是让 partwal_follower_append 抛 ERROR。
     * ERROR 会让整条 RPC 失败，leader 当成"对端不通"原地重发同一条、永不回退；
     * 干净的拒绝才会让 leader 按 Raft 回退 next_index，把空洞那几条（连字节）补发过来。
     * 实测（演示二 P059）：w1/w4 日志里有 31..35、流里字节只到 29，此后每条新数据都"出现空洞"，
     * 两个副本永远 ack 不了，新主连重供基线都提交不了。
     */
    if (has_entry && strcmp(entry_op, RAFT_OP_PARWAL) == 0 && entry_data_hex != NULL)
    {
        int64 eplsn = entry_partition_lsn(entry_payload);
        int64 flushed = eplsn > 1 ? data_local_flush_plsn(ctx) : -1;

        if (flushed >= 0 && eplsn > flushed + 1)
        {
            static TimestampTz last_gap_note = 0;

            if (last_gap_note == 0 ||
                TimestampDifferenceExceeds(last_gap_note, GetCurrentTimestamp(), 10000))
            {
                last_gap_note = GetCurrentTimestamp();
                elog(LOG, "pg_raft: group %lld 数据条目 idx=%lld 的 plsn=%lld 会在本地流里留空洞（本地只到 %lld），"
                     "拒收让 leader 回退补发",
                     (long long) ctx->group_id, (long long) entry_idx,
                     (long long) eplsn, (long long) flushed);
            }
            return true;
        }
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
            bool          old_in_ring = false;

            if (exist->index == entry_idx)
            {
                old = *exist;
                have_old = true;
                old_in_ring = true;
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
            /*
             * ★ P7-N56（2026-10-04 定稿）：控制面同 index、同任期、**内容不同** 也算冲突。
             * 控制面 leader 提案凑不齐多数派时会丢掉自己那条未提交的尾巴，同一任期的下一条
             * 提案就落在同一个 index 上（违反 Leader Append-Only 的那一步，留着是为了不让
             * leader 的环被凑不齐多数派的条目堆满）。已收下旧条目的 follower 若按"同 index
             * 同任期 = 同一条"处理，会 apply 旧内容 ⇒ 节点间控制面分叉（实测 master 少了 5 个
             * 分区的主）。这种条目在本节点必然还没提交（提交点受 leader_commit 封顶），截掉换新安全。
             * 只对控制面比内容：数据组重提的是同一 plsn 的同一份字节，而 jsonb 规范化前后的
             * 文本不同，比文本会误判并截掉 parwal。比较时忽略空白（环里可能是规范化过的文本）。
             */
            if (old.term == entry_term && ctx->group_id == RAFT_CONTROL_GROUP &&
                old_in_ring && entry_idx > ctx->log->commit_index &&
                !payload_text_equal(old.payload, entry_payload))
                old.term = -1;          /* 走下面的冲突截断 */

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
                /* T7.20：截掉的那段里若有未提交的 CONFIG，成员集必须退回 */
                raft_config_rollback(ctx, entry_idx - 1);
                SpinLockAcquire(&ctx->log->mutex);
            }
            else
                already_present = true;
        }

        if (entry_idx == ctx->log->last_log_index + 1)
        {
            fresh_append = true;
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
            /* T7.20：follower 也在 append 那一刻生效，与 leader 同一规则 */
            if (strcmp(entry_op, RAFT_OP_CONFIG) == 0)
                raft_config_apply(ctx, entry_idx, entry_payload);
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
            /* 行由本事务重写过：批量路径下改记本事务（原写入者可能已中止） */
            if (TransactionIdIsValid(raft_append_pxid))
            {
                RaftLogEntry *x = log_slot(ctx, entry_idx);

                if (x->index == entry_idx)
                    x->pxid = raft_append_pxid;
            }
        }
        else
        {
            SpinLockRelease(&ctx->log->mutex);
            return true;
        }
    }

    /*
     * ★ P7-N45（2026-10-03）：提交点只能推到"本次确认一致的最后一条"（Raft 原文：
     * min(leaderCommit, index of last new entry)）——带条目时是这条，纯心跳时是 prev。
     * 原来推到 min(leader_commit, 本地 last_log_index)：本地尾巴若还挂着前任期没提交的
     * 分叉条目，而 leader 的 prev 落在分叉点之前，那些分叉条目会被当成已提交 apply 掉。
     * 也正因为有这条上限，prev=0 的保活心跳（见 replicate_to_peer）推不动提交点。
     */
    advance_commit_index_locked(ctx, Min(leader_commit, has_entry ? entry_idx : prev_idx));
    cfg_commit = ctx->log->commit_index;
    commit_to_mark = ctx->log->commit_index;
    SpinLockRelease(&ctx->log->mutex);

    /* T7.20：提交点越过那条 CONFIG ⇒ 成员变更定案 */
    raft_config_note_commit(ctx, cfg_commit);

    /*
     * 日志冲突截断后，必须**先**把 parwal 里对应的字节也截掉，再写新字节。
     * 否则新 leader 复用同一个 partition_lsn 写入不同记录时，去重逻辑会把
     * 旧字节当成"已落盘"而跳过，物理回放就会重放被截断的内容。
     */
    if (conflict_plsn > 0)
    {
        rs_dq_flush();
        data_group_truncate_parwal(ctx, conflict_plsn - 1);
    }

    /*
     * 数据组：**先把字节落盘 fsync，再 ack**。落盘失败就不 ack，leader 便无法
     * 把这条计入多数派，从而保证"已提交 == 多数派已持久化"。
     */
    /* 2026-10-12：字节本来就在本地 —— 但若刚才的冲突截断把它截掉了，就不能 ack */
    if (data_preexisting && conflict_plsn > 0 &&
        conflict_plsn <= entry_partition_lsn(entry_payload))
    {
        elog(LOG, "pg_raft: group %lld 数据条目 idx=%lld 的本地字节被冲突截断截掉了，这一轮不 ack",
             (long long) ctx->group_id, (long long) entry_idx);
        return true;
    }
    if (has_entry && strcmp(entry_op, RAFT_OP_PARWAL) == 0 && !data_preexisting)
    {
        if (!data_entry_store(ctx, entry_payload, entry_data_hex, fresh_append))
        {
            /*
             * P7-N42：刚 append 的新条目字节没落下 —— 把它从日志里撤掉（只撤未提交的），
             * 免得本节点带着"有日志无数据"的条目参加选举、把日志比有数据的成员还长。
             */
            if (fresh_append)
            {
                bool undone = false;

                SpinLockAcquire(&ctx->log->mutex);
                if (ctx->log->last_log_index == entry_idx && entry_idx > ctx->log->commit_index)
                {
                    log_truncate_after_locked(ctx, entry_idx - 1);
                    undone = true;
                }
                SpinLockRelease(&ctx->log->mutex);
                if (undone)
                {
                    if (raft_batch_active && raft_batch_nrows > 0 &&
                        raft_batch_row_idx[raft_batch_nrows - 1] == entry_idx)
                        raft_batch_nrows--;     /* 攒着还没写的那一行也撤掉 */
                    delete_log_entry_sql(ctx, entry_idx);
                }
            }
            return true;
        }
    }

    if (!raft_batch_active)
        persist_hard_state_unlocked(ctx);

    /*
     * P7-N41：合并心跳里不标 committed。这一列只是重启恢复的提示（权威在 hardstate），
     * 而这条 UPDATE 会撞上同组并发的批量 AppendEntries 事务持有的行锁 —— 一条合并心跳
     * 里排在后面的组全被它挡住，对端超时改选（演示二灌数据时实测两组因此换主、整笔提交失败）。
     * 标记交给带条目的 AppendEntries / propose 路径去做。
     */
    if (commit_to_mark > 0 && !raft_in_hb_multi && !raft_batch_active)
        mark_log_committed_sql(ctx, commit_to_mark);

    *success = 1;
    /* P7-N41：合并心跳里数据组不在这里 apply（同理撞行锁）；topology monitor 周期性 apply_all_data 会做 */
    if ((!raft_in_hb_multi || ctx->group_id == RAFT_CONTROL_GROUP) && !raft_batch_active)
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
    if (!raft_group_ensure(group_id, NULL, 0, &ctx, true))   /* hearsay */
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

    /* 带条目时记下本事务（见 rm_wait_predecessor）；纯心跳不必分配 xid */
    if (entry_op != NULL && ctx.group_id != RAFT_CONTROL_GROUP)
        raft_append_pxid = GetTopTransactionId();
    PG_TRY();
    {
        (void) handle_append_entries(&ctx, in_term, leader_id, prev_idx, prev_term,
                                     leader_commit, entry_idx, entry_term,
                                     entry_op, entry_payload, entry_data_hex,
                                     &out_term, &success);
    }
    PG_FINALLY();
    {
        raft_append_pxid = InvalidTransactionId;
    }
    PG_END_TRY();

    if (entry_op)
        pfree(entry_op);
    if (entry_payload)
        pfree(entry_payload);
    if (entry_data_hex)
        pfree(entry_data_hex);

    snprintf(out, sizeof(out), "%lld %d", (long long) out_term, success);
    PG_RETURN_TEXT_P(cstring_to_text(out));
}



PG_FUNCTION_INFO_V1(pg_raft_append_entries_batch);

/*
 * 等同组前一批在本节点落定：prev 已进环，（数据组）本地流已落到 first_plsn-1，且写 prev 那条
 * raft_log 行的事务已经结束。至多 500 ms；等不到返回 false（调用方拒收）。
 *
 * 最后一条是 2026-10-16 审查补的：前一批（另一条连接、另一个会话）的 raft_log 行还没提交时，
 * 本批若照常处理并 ack，leader 会按本批的 last 推进 match_index —— 那等于替前一批也 ack 了；
 * 本节点此刻崩溃，前一批的行随未提交事务消失，日志出现空洞，而 leader 已据此计过多数派。
 * 所以等它的事务结束：提交了 ⇒ 前缀持久；中止了 ⇒ 让本事务按环把那一段补写回去（随本事务
 * 提交才持久，ack 在提交之后）。
 */
static bool
rm_wait_predecessor(RaftGroupCtx *ctx, int64 prev_idx, int64 first_plsn, bool may_wait)
{
    TimestampTz start = 0;

    restore_hard_state_if_needed(ctx);
    restore_persistent_log_if_needed(ctx);
    for (;;)
    {
        int64           last;
        TransactionId   pxid = InvalidTransactionId;
        bool            ring_ok;

        SpinLockAcquire(&ctx->log->mutex);
        last = ctx->log->last_log_index;
        if (last >= prev_idx && prev_idx > 0)
        {
            RaftLogEntry *pe = log_slot(ctx, prev_idx);

            if (pe->index == prev_idx)
                pxid = pe->pxid;
        }
        SpinLockRelease(&ctx->log->mutex);
        ring_ok = (last >= prev_idx &&
                   (first_plsn <= 1 || data_local_flush_plsn(ctx) >= first_plsn - 1));
        if (ring_ok)
        {
            if (!TransactionIdIsNormal(pxid) ||
                TransactionIdEquals(pxid, GetTopTransactionIdIfAny()))
                return true;
            /*
             * 早于 clog 截断点的 xid 查不了状态（会 ERROR）。那么老的写入者必然早已结束；
             * 若是中止，logtrack 的 abort 回调早就立过补写标记。
             */
            if (TransactionIdPrecedes(pxid, ShmemVariableCache->oldestClogXid))
                return true;
            if (!TransactionIdIsInProgress(pxid))
            {
                if (TransactionIdDidCommit(pxid))
                {
                    /* 已提交：清掉，免得这条在环里一直挂着 xid 直到 clog 被截 */
                    SpinLockAcquire(&ctx->log->mutex);
                    {
                        RaftLogEntry *pe = log_slot(ctx, prev_idx);

                        if (pe->index == prev_idx && TransactionIdEquals(pe->pxid, pxid))
                            pe->pxid = InvalidTransactionId;
                    }
                    SpinLockRelease(&ctx->log->mutex);
                    return true;
                }

                /*
                 * 前一批的事务中止了：它写的 raft_log 行随之消失，环里的条目还在。让本事务
                 * 按环把这一段补写回去（P7-N46 的补写机制，persist 时触发），本事务提交即
                 * 持久 —— 与 abort 回调立的标记相同，只是不等那个回调（它在事务退出 ProcArray
                 * 之后才跑）。补写区间取环能覆盖的最大范围，多写的行是幂等的 upsert。
                 */
                SpinLockAcquire(&ctx->log->mutex);
                {
                    int64         from = Max(prev_idx - RAFT_LOG_CAPACITY + 1, 1);
                    RaftLogEntry *pe = log_slot(ctx, prev_idx);

                    if (ctx->g->repersist_from == 0 || from < ctx->g->repersist_from)
                        ctx->g->repersist_from = from;
                    if (pe->index == prev_idx && TransactionIdEquals(pe->pxid, pxid))
                        pe->pxid = InvalidTransactionId;
                }
                SpinLockRelease(&ctx->log->mutex);
                return true;
            }
        }
        /* 复制流按序到达：前驱不在就是真缺（前一批被拒 / 没提交），不等，立即拒收让 leader 回退 */
        if (!may_wait)
            return false;
        if (start == 0)
            start = GetCurrentTimestamp();
        else if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), 500))
            return false;       /* 等不到：拒收，leader 退回从缺口补发 */
        CHECK_FOR_INTERRUPTS();
        pg_usleep(1000L);
    }
}

/*
 * pg_raft_append_entries_batch(term, leader_id, prev_idx, prev_term, leader_commit, group_id,
 *                              idx bigint[], entry_term bigint, payloads text[], data_hex text[]) → "term ok"
 *
 * P7-N41 group commit 的接收端：一批连续的 OP_PARWAL 条目，逐条走 handle_append_entries
 * （prev 依次串起来），parwal 字节在批内只写不 fsync（partwal_defer_sync_begin），
 * 全批写完**一次 fsync**（partwal_defer_sync_end）之后才回 ok=1。
 * 任何一条被拒（prev 不符、环满、落盘失败）⇒ ok=0；已落下的前缀无害（下次重传按"已持有"处理）。
 * fsync 失败 ⇒ ERROR ⇒ RPC 失败 ⇒ 不 ack。
 */
/*
 * 一组一批的收条目核心（2026-10-17 从 pg_raft_append_entries_batch 抽出，复制流也用它）。
 * own_defer = true 时自己开 / 收"批内延迟 fsync"（单组接口）；复制流在外层对整条消息只开收一次。
 * 返回是否整批收下；*out_term 填本节点任期。
 */
/* 2026-10-17 复制流副本端分段计时（微秒；pg_raft_stream_apply 每条消息清零、慢批时打印） */
static int64 rs_t_hae = 0, rs_t_dq = 0, rs_t_rows = 0, rs_t_tail = 0, rs_t_pre = 0;
static int64 rs_t_hs = 0, rs_t_mark = 0, rs_t_apply = 0;

static bool
rs_batch_core(RaftGroupCtx *ctx, int64 in_term, int leader_id, int64 prev_idx, int64 prev_term,
              int64 leader_commit, int n, const int64 *idx, int64 entry_term,
              char **pl, char **hx, int64 *out_term_p, bool own_defer, int *n_acc)
{
    int          k = 0;
    int64        out_term = 0;
    int          success = 0;
    bool         deferred = false;
    bool         spi_owned;
    TimestampTz  t_start = GetCurrentTimestamp();

    /*
     * 2026-10-16 流水线：同组下一批可能比上一批先到本节点（不同连接、不同会话）。prev 还没进环、
     * 或前一批的字节 / raft_log 行还没落定时短等它。只在本任期 leader 发来时等。
     * （复制流按序到达，前一批要么已提交、要么就在本事务里，这里直接通过。）
     */
    if (prev_idx > 0 && n > 0)
    {
        int64       first_plsn = entry_partition_lsn(pl[0]);
        int64       cur_term;

        SpinLockAcquire(&ctx->cons->mutex);
        cur_term = ctx->cons->current_term;
        SpinLockRelease(&ctx->cons->mutex);
        if (n_acc != NULL)
            *n_acc = 0;
        if (in_term >= cur_term && !rm_wait_predecessor(ctx, prev_idx, first_plsn, own_defer))
        {
            *out_term_p = cur_term;
            return false;
        }
    }

    /*
     * 环里放不下这一批：先把已提交的 apply 掉腾地方（2026-10-17）。批内 apply 推迟到批尾之后，
     * follower 的 last_applied 只在"本批至少收下一条"时才前移；环一满就再也收不下、也就再也
     * 不 apply —— 实测流水线下数据组整组卡死（follower last_log - last_applied = 1023）。
     */
    {
        int64 room;

        SpinLockAcquire(&ctx->log->mutex);
        room = RAFT_LOG_CAPACITY - 1 - (ctx->log->last_log_index - ctx->log->last_applied);
        SpinLockRelease(&ctx->log->mutex);
        if (room < n)
            group_apply_pending(ctx);
    }
    rs_t_pre += GetCurrentTimestamp() - t_start;

    /* 本批进环（或重写了行）的条目记下本事务，同组后一批据此等我们提交（见 rm_wait_predecessor） */
    raft_append_pxid = GetTopTransactionId();
    raft_batch_active = true;
    raft_batch_lp_gid = 0;
    raft_batch_lp_oid = 0;
    raft_batch_flush = -1;
    raft_batch_nrows = 0;
    PG_TRY();
    {
        if (own_defer && raft_persist_spi_begin(&spi_owned))
        {
            deferred = (SPI_execute("SELECT partdist.partwal_defer_sync_begin()", false, 1) == SPI_OK_SELECT);
            raft_persist_spi_end(spi_owned);
        }
        if (!own_defer)
            (void) rs_dq_begin();

        for (k = 0; k < n; k++)
        {
            /*
             * 2026-10-17 复制流：批里条目多、环剩的地方不够时，按已知提交点就地 apply 腾地方
             * （攒着的 raft_log 行是拷贝，不受环槽位复用影响）。
             */
            if (!own_defer)
            {
                int64 room;

                SpinLockAcquire(&ctx->log->mutex);
                room = RAFT_LOG_CAPACITY - 1 - (ctx->log->last_log_index - ctx->log->last_applied);
                SpinLockRelease(&ctx->log->mutex);
                if (room <= 0)
                {
                    rs_dq_flush();
                    group_apply_pending(ctx);
                }
            }
            success = 0;
            {
                TimestampTz th = GetCurrentTimestamp();

                (void) handle_append_entries(ctx, in_term, leader_id, prev_idx, prev_term,
                                             leader_commit, idx[k], entry_term,
                                             RAFT_OP_PARWAL, pl[k], hx[k], &out_term, &success);
                rs_t_hae += GetCurrentTimestamp() - th;
            }
            if (!success)
                break;
            prev_idx = idx[k];
            prev_term = entry_term;
        }

        /* 批尾：攒下的字节按组一次落盘；raft_log 行一条多行 INSERT，hardstate / 标 committed / apply 各一次 */
        {
            TimestampTz t1 = GetCurrentTimestamp();

            rs_dq_flush();
            rs_t_dq += GetCurrentTimestamp() - t1;
        }
        rs_dq_end();
        raft_batch_active = false;
        if (raft_batch_nrows > 0)
        {
            TimestampTz t1 = GetCurrentTimestamp();

            persist_log_entries_sql_batch(ctx, raft_batch_nrows, raft_batch_row_idx,
                                          raft_batch_row_term, RAFT_OP_PARWAL,
                                          raft_batch_row_pl, false);
            raft_batch_nrows = 0;
            rs_t_rows += GetCurrentTimestamp() - t1;
        }
        /* 一条都没收下（环满 / 拒收）也照样 apply，否则环满时永远腾不出地方 */
        {
            int64 commit_now;
            TimestampTz t1 = GetCurrentTimestamp();

            TimestampTz t2;

            if (k > 0)
                persist_hard_state_unlocked(ctx);
            t2 = GetCurrentTimestamp();
            rs_t_hs += t2 - t1;
            SpinLockAcquire(&ctx->log->mutex);
            commit_now = ctx->log->commit_index;
            SpinLockRelease(&ctx->log->mutex);
            /*
             * 复制流（!own_defer）下不逐行标 committed：这一列只是重启时抬 commit_index 的提示
             * （Raft 里 commitIndex 本是易失量，重启后由 leader 心跳重新推进，hardstate 里也有近似值），
             * 而这条 UPDATE 要先给每行加锁再改、每步都过分区 WAL 的插入钩子，实测单批 1–3 s。
             */
            if (k > 0 && commit_now > 0 && own_defer)
                mark_log_committed_sql(ctx, commit_now);
            rs_t_mark += GetCurrentTimestamp() - t2;
            t2 = GetCurrentTimestamp();
            group_apply_pending(ctx);
            rs_t_apply += GetCurrentTimestamp() - t2;
            rs_t_tail += GetCurrentTimestamp() - t1;
        }
    }
    PG_FINALLY();
    {
        raft_append_pxid = InvalidTransactionId;
        raft_batch_active = false;
        raft_batch_nrows = 0;
        rs_dq_end();
    }
    PG_END_TRY();

    /* 全批一次 fsync；失败即 ERROR（不 ack） */
    if (deferred && raft_persist_spi_begin(&spi_owned))
    {
        if (SPI_execute("SELECT partdist.partwal_defer_sync_end()", false, 1) != SPI_OK_SELECT)
            ereport(ERROR, (errmsg("pg_raft: 批量落盘 fsync 失败")));
        raft_persist_spi_end(spi_owned);
    }

    {
        long ms = (long) ((GetCurrentTimestamp() - t_start) / 1000);

        if (ms > 2000)
            elog(LOG, "pg_raft: group %lld 批量收 %d 条用了 %ld ms（落下 %d 条）",
                 (long long) ctx->group_id, n, ms, k);
    }
    *out_term_p = out_term;
    if (n_acc != NULL)
        *n_acc = k;
    return (k == n && success);
}

Datum
pg_raft_append_entries_batch(PG_FUNCTION_ARGS)
{
    int64        in_term = PG_GETARG_INT64(0);
    int          leader_id = PG_GETARG_INT32(1);
    int64        prev_idx = PG_GETARG_INT64(2);
    int64        prev_term = PG_GETARG_INT64(3);
    int64        leader_commit = PG_GETARG_INT64(4);
    int64        group_id = PG_GETARG_INT64(5);
    ArrayType   *a_idx = PG_GETARG_ARRAYTYPE_P(6);
    int64        entry_term = PG_GETARG_INT64(7);
    ArrayType   *a_pl = PG_GETARG_ARRAYTYPE_P(8);
    ArrayType   *a_hex = PG_GETARG_ARRAYTYPE_P(9);
    Datum       *d_idx, *d_pl, *d_hex;
    bool        *n_idx, *n_pl, *n_hex;
    int          n, n2, n3, k;
    int64        out_term = 0;
    bool         ok;
    char         out[64];
    RaftGroupCtx ctx;
    int64       *idx;
    char       **pl;
    char       **hx;

    if (RaftGroups == NULL || !pg_raft_raft_enabled || group_id <= 0)
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));
    parse_peers();
    if (!raft_group_ensure(group_id, NULL, 0, &ctx, true))   /* hearsay */
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));
    (void) group_resolve_membership(&ctx);

    deconstruct_array(a_idx, INT8OID, 8, FLOAT8PASSBYVAL, TYPALIGN_DOUBLE, &d_idx, &n_idx, &n);
    deconstruct_array(a_pl, TEXTOID, -1, false, TYPALIGN_INT, &d_pl, &n_pl, &n2);
    deconstruct_array(a_hex, TEXTOID, -1, false, TYPALIGN_INT, &d_hex, &n_hex, &n3);
    if (n <= 0 || n != n2 || n != n3)
        ereport(ERROR, (errmsg("pg_raft: 批量 AppendEntries 数组长度不一致（%d/%d/%d）", n, n2, n3)));

    idx = (int64 *) palloc(sizeof(int64) * n);
    pl = (char **) palloc(sizeof(char *) * n);
    hx = (char **) palloc(sizeof(char *) * n);
    for (k = 0; k < n; k++)
    {
        if (n_idx[k] || n_pl[k] || n_hex[k])
            break;
        idx[k] = DatumGetInt64(d_idx[k]);
        pl[k] = TextDatumGetCString(d_pl[k]);
        hx[k] = TextDatumGetCString(d_hex[k]);
    }
    n = k;

    ok = rs_batch_core(&ctx, in_term, leader_id, prev_idx, prev_term, leader_commit,
                       n, idx, entry_term, pl, hx, &out_term, true, NULL);
    snprintf(out, sizeof(out), "%lld %d", (long long) out_term, (ok && n > 0) ? 1 : 0);
    PG_RETURN_TEXT_P(cstring_to_text(out));
}

/*
 * pg_raft_stream_apply(msg bytea) → text —— 复制流的接收端（2026-10-17）。
 *
 * 一条消息装着 leader 本轮要发给本节点的全部组的条目（二进制，见 rs_build_batch）：
 *   int32 magic, int32 leader_id, int32 ngroups,
 *   每组：int64 gid, term, prev_idx, prev_term, leader_commit, entry_term; int32 n;
 *        每条：int64 idx; int32 len + payload; int32 len + 原始记录字节
 * 逐组走 rs_batch_core（与单组批量接口同一套语义），整条消息只开收一次"批内延迟 fsync"：
 * 全部组的字节写完一次 fsync，随本语句的事务一次提交 —— 回包在提交之后。
 * 复制流按序到达（同一连接、同一会话），不存在"后一批先到"。
 * 返回每组一段 "gid term ok last_log_index acc;"（acc = 本批收下的前缀的最后一条 index，0 = 一条没收）。
 */
PG_FUNCTION_INFO_V1(pg_raft_stream_apply);

Datum
pg_raft_stream_apply(PG_FUNCTION_ARGS)
{
    bytea          *raw = PG_GETARG_BYTEA_PP(0);
    StringInfoData  msg;
    StringInfoData  out;
    int             magic, leader_id, ngroups, gi;
    bool            spi_owned;
    bool            deferred = false;
    TimestampTz     t0 = GetCurrentTimestamp();
    TimestampTz     t_sync = 0;
    int             nent = 0;

    rs_t_hae = rs_t_dq = rs_t_rows = rs_t_tail = rs_t_pre = 0;
    rs_t_hs = rs_t_mark = rs_t_apply = 0;

    initStringInfo(&out);
    if (RaftGroups == NULL || !pg_raft_raft_enabled)
        PG_RETURN_TEXT_P(cstring_to_text(""));
    parse_peers();

    msg.data = VARDATA_ANY(raw);
    msg.len = VARSIZE_ANY_EXHDR(raw);
    msg.maxlen = msg.len;
    msg.cursor = 0;
    magic = pq_getmsgint(&msg, 4);
    if (magic != RS_MSG_MAGIC)
        ereport(ERROR, (errmsg("pg_raft: 复制流消息格式不认识（magic %x）", magic)));
    leader_id = pq_getmsgint(&msg, 4);
    ngroups = pq_getmsgint(&msg, 4);

    /*
     * 同一 leader 只许一条复制流会话在干活：leader 一批超时就断开重连，而本节点上旧会话还在跑
     * （要等到回结果时才发现客户端没了），它持着的行会把新会话卡住、一路连锁。新会话第一批到来时
     * 把旧会话终止（其未提交的部分随之回滚；环里留下的条目由前驱检查按"中止"补写，见 rm_wait_predecessor）。
     */
    {
        int     slot_i = (leader_id & 63);
        pid_t   oldpid;

        SpinLockAcquire(&RaftGroups->mutex);
        oldpid = RaftGroups->stream_in_pid[slot_i];
        RaftGroups->stream_in_pid[slot_i] = MyProcPid;
        SpinLockRelease(&RaftGroups->mutex);
        if (oldpid != 0 && oldpid != MyProcPid && BackendPidGetProc(oldpid) != NULL)
        {
            int waited = 0;

            elog(LOG, "pg_raft: 来自节点 %d 的新复制流会话接手，终止旧会话 %d", leader_id, (int) oldpid);
            (void) kill(oldpid, SIGTERM);
            while (BackendPidGetProc(oldpid) != NULL && waited < 5000)
            {
                CHECK_FOR_INTERRUPTS();
                pg_usleep(10000L);
                waited += 10;
            }
        }
    }

    if (raft_persist_spi_begin(&spi_owned))
    {
        deferred = (SPI_execute("SELECT partdist.partwal_defer_sync_begin()", false, 1) == SPI_OK_SELECT);
        raft_persist_spi_end(spi_owned);
    }

    for (gi = 0; gi < ngroups; gi++)
    {
        int64        gid = pq_getmsgint64(&msg);
        int64        term = pq_getmsgint64(&msg);
        int64        prev_idx = pq_getmsgint64(&msg);
        int64        prev_term = pq_getmsgint64(&msg);
        int64        leader_commit = pq_getmsgint64(&msg);
        int64        entry_term = pq_getmsgint64(&msg);
        int          n = pq_getmsgint(&msg, 4);
        int64       *idx = (int64 *) palloc(sizeof(int64) * Max(n, 1));
        char       **pl = (char **) palloc(sizeof(char *) * Max(n, 1));
        char       **hx = (char **) palloc(sizeof(char *) * Max(n, 1));
        int          k;
        RaftGroupCtx ctx;
        int64        out_term = 0;
        int64        last = -1;
        bool         ok = false;
        int          nacc = 0;

        nent += n;

        for (k = 0; k < n; k++)
        {
            int         plen, dlen;
            const char *pp, *dp;

            idx[k] = pq_getmsgint64(&msg);
            plen = pq_getmsgint(&msg, 4);
            pp = pq_getmsgbytes(&msg, plen);
            pl[k] = pnstrdup(pp, plen);
            dlen = pq_getmsgint(&msg, 4);
            dp = pq_getmsgbytes(&msg, dlen);
            hx[k] = (char *) palloc((Size) dlen * 2 + 1);
            (void) hex_encode(dp, dlen, hx[k]);
            hx[k][(Size) dlen * 2] = '\0';
        }
        if (gid > 0 && n > 0 && raft_group_ensure(gid, NULL, 0, &ctx, true))
        {
            (void) group_resolve_membership(&ctx);
            ok = rs_batch_core(&ctx, term, leader_id, prev_idx, prev_term, leader_commit,
                               n, idx, entry_term, pl, hx, &out_term, false, &nacc);
            SpinLockAcquire(&ctx.log->mutex);
            last = ctx.log->last_log_index;
            SpinLockRelease(&ctx.log->mutex);
        }
        appendStringInfo(&out, "%lld %lld %d %lld %lld;", (long long) gid, (long long) out_term,
                         ok ? 1 : 0, (long long) last,
                         (long long) (nacc > 0 ? idx[nacc - 1] : 0));
        for (k = 0; k < n; k++)
        {
            pfree(pl[k]);
            pfree(hx[k]);
        }
        pfree(idx);
        pfree(pl);
        pfree(hx);
    }

    /* 全部组一次 fsync；失败即 ERROR（整条消息不 ack） */
    t_sync = GetCurrentTimestamp();
    if (deferred && raft_persist_spi_begin(&spi_owned))
    {
        if (SPI_execute("SELECT partdist.partwal_defer_sync_end()", false, 1) != SPI_OK_SELECT)
            ereport(ERROR, (errmsg("pg_raft: 复制流批量落盘 fsync 失败")));
        raft_persist_spi_end(spi_owned);
    }
    {
        TimestampTz now = GetCurrentTimestamp();
        long ms = (long) ((now - t0) / 1000);
        static TimestampTz st_at = 0;
        static int64 st_n = 0, st_sum = 0, st_max = 0, st_ent = 0, st_sync = 0;

        st_n++;
        st_sum += now - t0;
        st_ent += nent;
        st_sync += now - t_sync;
        if (now - t0 > st_max)
            st_max = now - t0;
        if (st_at == 0)
            st_at = now;
        else if (TimestampDifferenceExceeds(st_at, now, 10000))
        {
            elog(LOG, "pg_raft: 复制流接收（来自节点 %d）10 s：%lld 批 %lld 条，均 %lld ms（其中 fsync 均 %lld ms），最大 %lld ms",
                 leader_id, (long long) st_n, (long long) st_ent,
                 (long long) (st_sum / st_n / 1000), (long long) (st_sync / st_n / 1000),
                 (long long) (st_max / 1000));
            st_at = now;
            st_n = st_sum = st_max = st_ent = st_sync = 0;
        }

        if (ms > 1000)
            elog(LOG, "pg_raft: 复制流一批（来自节点 %d，%d 组 %d 条，%d 字节）处理用了 %ld ms："
                 "前置 %lld / 逐条收 %lld / 落字节 %lld / 写日志行 %lld / 提交点+apply %lld"
                 "（hardstate %lld / 标记 %lld / apply %lld）/ fsync %lld ms",
                 leader_id, ngroups, nent, msg.len, ms,
                 (long long) rs_t_pre / 1000, (long long) rs_t_hae / 1000,
                 (long long) rs_t_dq / 1000, (long long) rs_t_rows / 1000,
                 (long long) rs_t_tail / 1000, (long long) rs_t_hs / 1000,
                 (long long) rs_t_mark / 1000, (long long) rs_t_apply / 1000,
                 (long long) (now - t_sync) / 1000);
    }
    PG_RETURN_TEXT_P(cstring_to_text(out.data));
}
PG_FUNCTION_INFO_V1(pg_raft_heartbeat_multi);

/*
 * pg_raft_heartbeat_multi(leader_id int, req text) → text
 *
 * P7-N41：合并心跳的接收端。req = 若干项 "group_id term prev_idx prev_term commit"，
 * 分号分隔；逐组按 pg_raft_append_entries 的纯心跳路径处理（hearsay 建组、补成员集、
 * handle_append_entries），回执每组一行 "group_id term ok"。
 * 每组一个子事务：一个组出错（坏日志、SPI 失败）只记该组 ok=0，不连累同批其他组。
 */
Datum
pg_raft_heartbeat_multi(PG_FUNCTION_ARGS)
{
    int             leader_id = PG_GETARG_INT32(0);
    char           *req = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char           *p = req;
    StringInfoData  out;
    MemoryContext   oldcxt = CurrentMemoryContext;
    ResourceOwner   oldowner = CurrentResourceOwner;

    initStringInfo(&out);
    if (RaftGroups == NULL || !pg_raft_raft_enabled)
        PG_RETURN_TEXT_P(cstring_to_text(""));
    parse_peers();

    /*
     * 兜底：任何一组撞上锁都不许把整条合并心跳拖住（后面的组会因此超时改选）。
     * 选举计时器在 handle_append_entries 开头就已刷新（共享内存，不随子事务回滚），
     * 锁等超时只让这一组本轮 ok=0。
     */
    (void) set_config_option("lock_timeout", "300", PGC_USERSET, PGC_S_SESSION,
                             GUC_ACTION_LOCAL, true, 0, false);

    while (*p)
    {
        long long    gid, term, prev_idx, prev_term, commit;
        int          used = 0;
        int64        out_term = 0;
        int          success = 0;
        RaftGroupCtx ctx;

        if (sscanf(p, "%lld %lld %lld %lld %lld%n",
                   &gid, &term, &prev_idx, &prev_term, &commit, &used) != 5 || used == 0)
            break;
        p += used;
        while (*p == ';' || *p == ' ')
            p++;

        BeginInternalSubTransaction(NULL);
        MemoryContextSwitchTo(oldcxt);
        PG_TRY();
        {
            if (raft_group_ensure((int64) gid, NULL, 0, &ctx, true))   /* hearsay */
            {
                (void) group_resolve_membership(&ctx);
                raft_in_hb_multi = true;
                (void) handle_append_entries(&ctx, (int64) term, leader_id,
                                             (int64) prev_idx, (int64) prev_term,
                                             (int64) commit, 0, 0, NULL, NULL, NULL,
                                             &out_term, &success);
                raft_in_hb_multi = false;
            }
            ReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(oldcxt);
            CurrentResourceOwner = oldowner;
        }
        PG_CATCH();
        {
            ErrorData *ed;

            MemoryContextSwitchTo(oldcxt);
            ed = CopyErrorData();
            FlushErrorState();
            RollbackAndReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(oldcxt);
            CurrentResourceOwner = oldowner;
            raft_spi_ctx = false;
            raft_in_hb_multi = false;
            elog(WARNING, "pg_raft: 合并心跳里组 %lld 处理失败：%s", gid, ed->message);
            FreeErrorData(ed);
            success = 0;
        }
        PG_END_TRY();

        appendStringInfo(&out, "%lld %lld %d\n", gid, (long long) out_term, success);
    }

    pfree(req);
    PG_RETURN_TEXT_P(cstring_to_text(out.data));
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

    if (!raft_group_ensure((int64) in_group, NULL, 0, &ctx, true))   /* hearsay */
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

    /*
     * ★ 2026-10-12（安全性）：投票之前先把持久日志灌回内存。
     *
     * 重启后数据组的日志环是空的，要等某条有 SPI 的路径（AppendEntries、提案……）才从
     * partdist.raft_log 灌回来；投票路径原来只恢复 HardState、不恢复日志 ——
     * 于是刚重启的节点拿"空日志"去比候选人的日志新旧，**谁来都投**。实测（演示二 P059 整簇重启后）：
     * 日志只有 30 条的候选人拿到了日志有 35 条（31..35 已提交）的成员的票，当选后用新任期条目
     * 把那几条**已提交**的条目截掉重写 —— 违反 Raft 的 Leader Completeness。
     * 灌不回来（SPI 拿不到等）就不投：拒投只影响活性（下一轮再来），投错是安全性事故。
     */
    restore_persistent_log_if_needed(&ctx);
    if (!ctx.g->log_restored &&
        (strcmp(type, "RV") == 0 || strcmp(type, "PV") == 0))
    {
        SpinLockAcquire(&ctx.cons->mutex);
        my_term = ctx.cons->current_term;
        SpinLockRelease(&ctx.cons->mutex);
        elog(LOG, "pg_raft: 组 %lld 本节点日志尚未从持久层灌回，暂不应答 %s（节点 %d）",
             in_group, type, in_node);
        pfree(msg);
        snprintf(out, sizeof(out), "%ld 0", (long) my_term);
        PG_RETURN_TEXT_P(cstring_to_text(out));
    }

    /* P7-N50：预选 —— 只回答"会不会投"，不改任何状态（任期、投票、计时器都不动） */
    if (strcmp(type, "PV") == 0)
    {
        bool leader_alive;

        SpinLockAcquire(&ctx.cons->mutex);
        SpinLockAcquire(&ctx.log->mutex);
        my_term = ctx.cons->current_term;
        leader_alive = (ctx.cons->state == RAFT_LEADER) ||
            (ctx.cons->leader_id != 0 && ctx.cons->last_leader_contact != 0 &&
             !TimestampDifferenceExceeds(ctx.cons->last_leader_contact, GetCurrentTimestamp(),
                                         pg_raft_election_timeout_ms));
        if (in_term > my_term && !leader_alive &&
            candidate_log_is_up_to_date_locked(&ctx, in_last_idx, in_last_term))
            flag = 1;
        SpinLockRelease(&ctx.log->mutex);
        SpinLockRelease(&ctx.cons->mutex);
        pfree(msg);
        snprintf(out, sizeof(out), "%ld %d", (long) my_term, flag);
        PG_RETURN_TEXT_P(cstring_to_text(out));
    }

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
            ctx.cons->last_leader_contact = GetCurrentTimestamp();   /* P7-N50 */
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

    if (!raft_group_ensure(group_id, members, n_members, &ctx, false))
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
    hard_state_hint_path(&ctx, path, sizeof(path));
    (void) unlink(path);
    hard_state_path(&ctx, path, sizeof(path));
    if (unlink(path) != 0 && errno != ENOENT)
        elog(WARNING, "pg_raft: 删除 hardstate 文件 \"%s\" 失败: %m", path);

    SpinLockAcquire(&RaftGroups->mutex);
    ctx.g->in_use = false;
    RaftGroups->n_groups--;
    SpinLockRelease(&RaftGroups->mutex);
    raft_tombstone_add(group_id);       /* P7-N21：不许 hearsay 马上把它再建出来 */

    PG_RETURN_BOOL(true);
}

/*
 * pg_raft_group_campaign —— 受控切主：让**本节点**在下一个 tick 就对**这一个组**发起选举。
 *
 * ★ 2026-09-18（扩展验收 [9] 第 2 轮 FAIL 定因）：此前唯一的切主手段是节点级 GUC ——
 *   旧主 heartbeat_ms 调到 60 s、目标 election_timeout 调到 300 ms。这两个旋钮都作用于
 *   **节点上的全部组**：旧主当主的每个组一起断心跳，目标又以 300 ms 超时在 2 vCPU 抖动下
 *   顺手抢走第三台的组。实测一次"切组 102747"把 6 个组全部压到 :5435，其升主前置在同一条
 *   异步连接上串行排队，102747 排在一个卡满 60 s 的组后面，还没轮到就丢了领导权。
 *
 * 做法：给该组置 campaign_pending，本节点下一个 tick 无条件发起一次选举（一次性，tick 读后
 * 清零；没选上由调用方再发）。不用"把选举截止期置到期"：现任 leader 的心跳会把它推回去。
 * 投票规则不变（Raft 原样：更高任期 + 日志至少一样新即投），旧主收到更高任期的 RV 自行降为
 * follower，别的组一概不受影响。本节点已是 leader、或不是该组成员时不做任何事。返回是否已置位。
 */
Datum
pg_raft_group_campaign(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    RaftGroupCtx ctx;
    bool         armed = false;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_BOOL(false);
    if (group_id == RAFT_CONTROL_GROUP)
        ereport(ERROR, (errmsg("pg_raft: 控制面组 0 不走受控切主")));
    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_BOOL(false);
    if (!group_has_member(&ctx, pg_raft_node_id))
        PG_RETURN_BOOL(false);

    SpinLockAcquire(&ctx.cons->mutex);
    if (ctx.cons->state != RAFT_LEADER)
    {
        ctx.cons->campaign_pending = true;
        armed = true;
    }
    SpinLockRelease(&ctx.cons->mutex);

    if (armed)
        ereport(LOG,
                (errmsg("pg_raft: 组 %lld 受控切主：本节点 %d 下一个 tick 发起选举",
                        (long long) group_id, pg_raft_node_id)));
    PG_RETURN_BOOL(armed);
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

/*
 * pg_raft_group_change_member(group_id, node_id, add) → text
 *
 * T7.20（P7-R1）：**成员变更的安全路径**。在此之前唯一的办法是在每个节点上
 * 各自重调 `pg_raft_group_create(gid, 新成员集)` —— 那是**没有协调的**：
 * 变更期间不同节点持有不同的成员集，于是同一个组在不同节点上有两套不相交的
 * 多数派定义，Leader Completeness 失去交集保证（与 group_membership_known()
 * 头注释里那个 hearsay 事故同一个形态，只是换了触发方式）。
 *
 * 现在走日志：一条 CONFIG 条目复制到多数派，各节点在 **append 那一刻**同步
 * 切到新成员集。四道门禁：
 *   ① 必须是本组 leader —— 只有 leader 能往日志里 append；
 *   ② 成员集必须已知 —— 未知即不参与（§9.2），更谈不上变更；
 *   ③ **同时只允许一个未提交的变更** —— 这是单节点变更安全性的前提：
 *      任意时刻只有相邻两个配置存在，它们的多数派必然相交；
 *   ④ **一次只加/减一个** —— 同上。要"三换三"就调三次。
 * 另加一条实际约束：不许把成员集减到空（那会让本组永久不可用）。
 */
Datum
pg_raft_group_change_member(PG_FUNCTION_ARGS)
{
    int64          group_id = PG_GETARG_INT64(0);
    int            node_id  = PG_GETARG_INT32(1);
    bool           add      = PG_GETARG_BOOL(2);
    RaftGroupCtx   ctx;
    int            cur[RAFT_MAX_PEERS];
    int            ncur = 0;
    int            next[RAFT_MAX_PEERS];
    int            nnext = 0;
    int            i;
    bool           present = false;
    bool           is_leader;
    int64          pending;
    int64          commit_idx;
    int64          idx;
    StringInfoData payload;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("pg_raft 未启用")));

    parse_peers();
    restore_groups_if_needed();

    if (!raft_group_ctx(group_id, &ctx))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("pg_raft: 本节点上没有组 %lld", (long long) group_id)));

    /* ① 只有 leader 能改 */
    SpinLockAcquire(&ctx.cons->mutex);
    is_leader = (ctx.cons->state == RAFT_LEADER);
    SpinLockRelease(&ctx.cons->mutex);
    if (!is_leader)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 本节点不是组 %lld 的 leader，成员变更只能在 leader 上发起",
                        (long long) group_id),
                 errhint("用 partdist.pg_raft_group_status() 找到当前 leader 再重试。")));

    /* ② 成员集必须已知 */
    if (!group_membership_known(&ctx))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 组 %lld 的成员集未知，不能变更",
                        (long long) group_id),
                 errdetail("成员集未知的组不参与选举与提案（DTX_2PC_DESIGN.md §9.2）。"),
                 errhint("先用 partdist.pg_raft_group_create(gid, ARRAY[...]) 交底。")));

    SpinLockAcquire(&RaftGroups->mutex);
    ncur = ctx.g->n_members;
    for (i = 0; i < ncur; i++)
        cur[i] = ctx.g->members[i];
    pending = ctx.g->pending_cfg_index;
    SpinLockRelease(&RaftGroups->mutex);

    SpinLockAcquire(&ctx.log->mutex);
    commit_idx = ctx.log->commit_index;
    SpinLockRelease(&ctx.log->mutex);

    /* ③ 同时只允许一个未提交的变更 */
    if (pending > 0 && pending > commit_idx)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 组 %lld 还有一个未提交的成员变更（日志条目 %lld，提交点 %lld）",
                        (long long) group_id, (long long) pending,
                        (long long) commit_idx),
                 errdetail("单节点成员变更的安全性前提是「任意时刻只有相邻两个配置」——"
                           "并发两个变更会让两个多数派不再必然相交。"),
                 errhint("等上一个变更提交后再发起下一个。")));

    /* ④ 一次只加/减一个 */
    for (i = 0; i < ncur; i++)
        if (cur[i] == node_id)
            present = true;

    if (add && present)
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                 errmsg("pg_raft: 节点 %d 已经是组 %lld 的成员",
                        node_id, (long long) group_id)));
    if (!add && !present)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("pg_raft: 节点 %d 不是组 %lld 的成员",
                        node_id, (long long) group_id)));
    if (!add && ncur <= 1)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("pg_raft: 不许把组 %lld 的成员集减到空",
                        (long long) group_id),
                 errdetail("空成员集 = 该组永久不可用（没有任何节点能凑出多数派）。")));

    for (i = 0; i < ncur; i++)
        if (add || cur[i] != node_id)
            next[nnext++] = cur[i];
    if (add)
        next[nnext++] = node_id;

    initStringInfo(&payload);
    raft_config_format(next, nnext, &payload);

    idx = group_propose(&ctx, RAFT_OP_CONFIG, payload.data);
    pfree(payload.data);

    if (idx <= 0)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 组 %lld 的成员变更未能提交（多数派不足或日志环满）",
                        (long long) group_id),
                 errdetail("变更已按 append-time 规则生效过又被回滚，成员集保持变更前的样子。"),
                 errhint("查 partdist.pg_raft_group_flow_stats() 看丢弃计数。")));

    PG_RETURN_TEXT_P(cstring_to_text(psprintf(
        "group %lld: %s node %d, members now %d, log index %lld",
        (long long) group_id, add ? "added" : "removed", node_id,
        nnext, (long long) idx)));
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
/*
 * pg_raft_group_quorum_alive(group_id) → boolean（P7-W6，2026-09-15）
 *
 * 本节点是该组 leader，且"自己 + 最近 2 个选举超时内应答过 AppendEntries 的
 * 成员"够多数派，才返回 true。给 partdist 的分叉标记自动修复用：标记多半正是
 * 多数派刚丢时打下的，立刻重发基线必败（每次被拒两次提案、CTRL/FPI 先落进本地
 * 流成孤儿）。判据用**时间**而不用 match_index：停机的 follower 停机前把一切都
 * ack 过了，match_index 看起来很健康。应答/失败的时刻由 replicate_to_peer 记
 * （心跳也走它），一个心跳周期内就能判出停机的 follower。
 */
static bool group_quorum_alive_excl(int64 gid, int exclude_node);

PG_FUNCTION_INFO_V1(pg_raft_group_quorum_alive);
Datum
pg_raft_group_quorum_alive(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(group_quorum_alive_excl(PG_GETARG_INT64(0), 0));
}

/*
 * pg_raft_group_quorum_alive_excl(group_id, exclude_node) → boolean（2026-10-12）
 *
 * 同上，但**不把 exclude_node 算作存活**。给自动归队用：给某个成员重供基线时，基线条目
 * 要靠"除它以外"的成员凑多数派（它正被截空重建，指望不上）。凑不齐还硬发，基线提案一直
 * 拿不到票，期间独占该组的复制认领位，用户写入全部排队直到超时（实测 commit_survives_failover_p7n35：
 * 4 成员组一台宕机一台被锁，新主上任即给第四台重供，写入卡 60 s 报"等待复制认领位超时"）。
 */
PG_FUNCTION_INFO_V1(pg_raft_group_quorum_alive_excl);
Datum
pg_raft_group_quorum_alive_excl(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(group_quorum_alive_excl(PG_GETARG_INT64(0), PG_GETARG_INT32(1)));
}

static bool
group_quorum_alive_excl(int64 gid, int exclude_node)
{
    RaftGroupCtx ctx;
    TimestampTz  acks[RAFT_MAX_PEERS];
    TimestampTz  fails[RAFT_MAX_PEERS];
    TimestampTz  now = GetCurrentTimestamp();
    int          state;
    int          alive = 1;          /* 自己 */
    int          majority;
    int          i;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        return false;
    parse_peers();
    if (!raft_group_ctx(gid, &ctx))
        return false;

    SpinLockAcquire(&ctx.cons->mutex);
    state = ctx.cons->state;
    SpinLockRelease(&ctx.cons->mutex);
    if (state != RAFT_LEADER)
        return false;

    majority = cluster_majority(&ctx);
    SpinLockAcquire(&ctx.log->mutex);
    memcpy(acks, ctx.log->peer_last_ack, sizeof(acks));
    memcpy(fails, ctx.log->peer_last_fail, sizeof(fails));
    SpinLockRelease(&ctx.log->mutex);

    for (i = 0; i < n_peers && i < RAFT_MAX_PEERS; i++)
    {
        if (peers[i].node_id == pg_raft_node_id || !peer_in_group(&ctx, i) ||
            peers[i].node_id == exclude_node)
            continue;
        /* 最近一次应答晚于最近一次失败，且应答不算太久（2 个选举超时） */
        if (acks[i] != 0 && acks[i] > fails[i] &&
            !TimestampDifferenceExceeds(acks[i], now,
                                        2 * pg_raft_election_timeout_ms))
            alive++;
    }
    return alive >= majority;
}

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
 * ───────────── P7-N41（2026-10-03）：数据组 group commit（批量复制 + 批量刷盘） ─────────────
 *
 * 原路径一条 parwal 记录一轮：leader 追加 → 逐个对端同步 RPC（串行）→ 对端各自
 * 「落字节 fsync + 写 checkpoint fsync + 提交 raft_log 那一行（WAL fsync）」→ ack →
 * leader 提交、写 hardstate（fsync）、apply。一笔 INSERT 事务通常 3 条记录（heap / btree /
 * 提交标记），副本因子 3 时就是 3 轮 × 2 对端 × 3 次 fsync。
 *
 * 现在 replicate_group_upto 一次把该组**积压的全部记录**（上限 pg_raft.replicate_batch_max）
 * 作为一批：
 *   · leader 一次追加 n 条（同一次持锁，index 连续）；
 *   · 一条多条目 AppendEntries（pg_raft_append_entries_batch）**并发**发给全部成员；
 *   · 对端整批落盘，**最后 fsync 一次**（partwal_defer_sync_begin/end），全批都落了才 ack；
 *   · 多数派 ack 后一次提交、一次 hardstate、一次 apply。
 * 这就是"刷盘前的缓冲区"：并发事务在 parwal 里排队的记录，被下一个拿到复制认领位的
 * backend 一并带走（与 PG 自己的 WAL group commit 同一思路）。
 *
 * 语义不变：ack ⇔ 对端整批已 fsync；提交点只越过多数派持有的前缀；
 * 凑不齐多数派的尾巴按原规则丢弃（字节留作孤儿等重推）。
 * pg_raft.replicate_batch_max = 1 退回逐条路径（A/B 对比用）。
 */
#define GC_MAX_PEERS RAFT_MAX_PEERS

/* 一批条目发给一个对端的 SQL */
static void
gc_build_batch_sql(StringInfo sql, RaftGroupCtx *ctx, int64 term, int64 prev_idx,
                   int64 prev_term, int64 leader_commit, int n, const int64 *idx,
                   int64 entry_term, const char *const *payload, const char *const *hex)
{
    int k;

    appendStringInfo(sql,
                     "SELECT partdist.pg_raft_append_entries_batch(%lld::bigint, %d, %lld::bigint, "
                     "%lld::bigint, %lld::bigint, %lld::bigint, ARRAY[",
                     (long long) term, pg_raft_node_id, (long long) prev_idx,
                     (long long) prev_term, (long long) leader_commit,
                     (long long) ctx->group_id);
    for (k = 0; k < n; k++)
        appendStringInfo(sql, "%s%lld", k ? "," : "", (long long) idx[k]);
    appendStringInfo(sql, "]::bigint[], %lld::bigint, ARRAY[", (long long) entry_term);
    for (k = 0; k < n; k++)
    {
        appendStringInfoString(sql, k ? "," : "");
        appendStringInfoString(sql, quote_literal_cstr(payload[k]));
    }
    appendStringInfoString(sql, "]::text[], ARRAY[");
    for (k = 0; k < n; k++)
    {
        appendStringInfoString(sql, k ? "," : "");
        appendStringInfoString(sql, quote_literal_cstr(hex[k]));
    }
    appendStringInfoString(sql, "]::text[])");
}

/*
 * 把 [first, first+n) 这 n 条（全部是本任期刚追加的 OP_PARWAL）并发发给各成员。
 * 返回 ack 数（含 leader 自己）。
 */
static int
gc_replicate_batch(RaftGroupCtx *ctx, int64 term, int64 first, int n,
                   const int64 *idx, const char *const *payload, const char *const *hex)
{
    PGconn        *conns[GC_MAX_PEERS];
    bool           sent[GC_MAX_PEERS];
    int64          prev_idx = first - 1;
    int64          prev_term = 0;
    int64          leader_commit;
    int64          last = first + n - 1;
    TimestampTz    start;
    int            acks = 1;
    int            i;
    int            timeout_ms;
    bool           stepped = false;
    StringInfoData sql;

    SpinLockAcquire(&ctx->log->mutex);
    if (prev_idx > 0)
    {
        RaftLogEntry *pe = log_slot(ctx, prev_idx);

        if (pe->index == prev_idx)
            prev_term = pe->term;
        else if (prev_idx == ctx->log->base_index)
            prev_term = ctx->log->base_term;
    }
    leader_commit = ctx->log->commit_index;
    if (!ctx->log->repl_inited)
    {
        int64 nx = ctx->log->last_log_index + 1;
        int   j;

        for (j = 0; j < RAFT_MAX_PEERS; j++)
        {
            ctx->log->peer_next_index[j] = nx;
            ctx->log->peer_match_index[j] = 0;
        }
        ctx->log->repl_inited = true;
        ctx->log->leader_durable_index = ctx->log->last_log_index;   /* 当选时日志里已有的条目视为已落盘 */
        ctx->log->leader_self_hole = 0;
    }
    SpinLockRelease(&ctx->log->mutex);

    initStringInfo(&sql);
    gc_build_batch_sql(&sql, ctx, term, prev_idx, prev_term, leader_commit, n, idx, term, payload, hex);

    /*
     * 批的上限按条数放宽：对端每条都要走一遍 SQL 落行 + parwal 写 + apply，
     * 2 vCPU 满载时 32 条一批实测会超过单条 RPC 的 5 s 上限 —— 超时即按"没确认"处理，
     * 两个成员同时超时整批就凑不齐多数派（实测 2000 行的种子插入因此失败）。
     */
    timeout_ms = Min(RAFT_RPC_EXEC_TIMEOUT_MS + 300 * n, 30000);

    start = GetCurrentTimestamp();
    for (i = 0; i < n_peers && i < GC_MAX_PEERS; i++)
    {
        conns[i] = NULL;
        sent[i] = false;
        if (!peer_in_group(ctx, i) || peer_in_backoff(&peers[i]))
            continue;
        conns[i] = peer_conn_get(&peers[i]);
        if (conns[i] == NULL)
        {
            peer_mark_result(&peers[i], false);
            peer_stamp(ctx, i, false);
            continue;
        }
        sent[i] = (PQsendQuery(conns[i], sql.data) == 1);
        if (!sent[i])
        {
            peer_conn_reset(i);
            peer_mark_result(&peers[i], false);
            peer_stamp(ctx, i, false);
        }
    }
    pfree(sql.data);

    for (i = 0; i < n_peers && i < GC_MAX_PEERS; i++)
    {
        PGresult *res;
        int64     rt = 0;
        int       ok = 0;
        bool      parsed = false;

        if (!sent[i])
            continue;
        res = pq_result_bounded(conns[i], start, timeout_ms);
        if (res != NULL && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
            parsed = parse_resp2(PQgetvalue(res, 0, 0), &rt, &ok);
        if (res == NULL || (!parsed && PQstatus(conns[i]) != CONNECTION_OK))
        {
            static TimestampTz last_warn = 0;

            if (res == NULL &&
                (last_warn == 0 || TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 10000)))
            {
                last_warn = GetCurrentTimestamp();
                elog(WARNING, "pg_raft: group %lld 批量复制 %d 条到节点 %d 超时（上限 %d ms），按该成员未确认处理",
                     (long long) ctx->group_id, n, peers[i].node_id, timeout_ms);
            }
            if (res != NULL)
                PQclear(res);
            peer_conn_reset(i);
            peer_mark_result(&peers[i], false);
            peer_stamp(ctx, i, false);
            continue;
        }
        if (!parsed)
            elog(WARNING, "pg_raft: group %lld 批量复制到节点 %d 出错：%s",
                 (long long) ctx->group_id, peers[i].node_id, PQresultErrorMessage(res));
        PQclear(res);
        peer_mark_result(&peers[i], true);
        peer_stamp(ctx, i, true);
        if (!parsed)
            continue;
        if (rt > term)
        {
            stepped = true;
            step_down_if_higher(ctx, rt);
            continue;
        }
        if (ok)
        {
            acks++;
            SpinLockAcquire(&ctx->log->mutex);
            if (last > ctx->log->peer_match_index[i])
                ctx->log->peer_match_index[i] = last;
            ctx->log->peer_next_index[i] = last + 1;
            SpinLockRelease(&ctx->log->mutex);
        }
    }
    return stepped ? 1 : acks;
}

/*
 * P7-N41：追平通道也走批量。
 *
 * 原来追平一个落后 N 条的成员要 N 次单条 RPC —— 追平跑在 topology monitor 进程里，
 * 和共识 tick 是同一个进程：重启后补发 264 条实测用了一分半钟，期间本节点当 leader 的
 * 所有组心跳全停、升主前置的自连握手也超时（新主两分半钟才登记上路由）。
 * 这里把 next_index 起、同一任期、连续的 OP_PARWAL 条目一次发一批（上限 replicate_batch_max）；
 * 条件不满足（非数据条目、prev 取不到证、跨任期）就返回 false，由调用方走原来的单条路径。
 */
static bool
catchup_batch_to_peer(RaftGroupCtx *ctx, int slot)
{
    int64        term;
    int64        next_idx;
    int64        last_idx;
    int64        prev_idx;
    int64        prev_term = 0;
    int64        leader_commit;
    int64        eterm = 0;
    int64        idx[RAFT_LOG_CAPACITY];
    char        *payload[RAFT_LOG_CAPACITY];
    char        *hex[RAFT_LOG_CAPACITY];
    int          n = 0;
    int          maxn = Min(pg_raft_replicate_batch_max, RAFT_LOG_CAPACITY);
    bool         prev_known = false;
    StringInfoData sql;
    PGconn      *conn;
    PGresult    *res;
    int64        rt = 0;
    int          ok = 0;
    bool         parsed = false;
    int          k;

    if (maxn <= 1 || !raft_spi_ctx || peer_in_backoff(&peers[slot]))
        return false;

    SpinLockAcquire(&ctx->cons->mutex);
    term = ctx->cons->current_term;
    if (ctx->cons->state != RAFT_LEADER)
    {
        SpinLockRelease(&ctx->cons->mutex);
        return false;
    }
    SpinLockRelease(&ctx->cons->mutex);

    SpinLockAcquire(&ctx->log->mutex);
    next_idx = ctx->log->peer_next_index[slot];
    last_idx = ctx->log->last_log_index;
    leader_commit = ctx->log->commit_index;
    prev_idx = next_idx - 1;
    if (prev_idx == 0)
        prev_known = true;
    else if (prev_idx == ctx->log->base_index)
    {
        prev_term = ctx->log->base_term;
        prev_known = true;
    }
    else
    {
        RaftLogEntry *pe = log_slot(ctx, prev_idx);

        if (pe->index == prev_idx)
        {
            prev_term = pe->term;
            prev_known = true;
        }
    }
    SpinLockRelease(&ctx->log->mutex);

    if (next_idx <= 0 || next_idx > last_idx || last_idx - next_idx < 1)
        return false;           /* 只差一条就走单条路径 */
    if (!prev_known)
    {
        RaftLogEntry old;

        if (!log_get_entry_sql(ctx, prev_idx, &old))
            return false;
        prev_term = old.term;
    }

    for (k = 0; k < maxn && next_idx + k <= last_idx; k++)
    {
        RaftLogEntry e;
        bool         got;

        SpinLockAcquire(&ctx->log->mutex);
        got = log_get_entry_locked(ctx, next_idx + k, &e);
        SpinLockRelease(&ctx->log->mutex);
        if (!got && !log_get_entry_sql(ctx, next_idx + k, &e))
            break;
        if (strcmp(e.op_type, RAFT_OP_PARWAL) != 0)
            break;
        if (n > 0 && e.term != eterm)
            break;              /* 一批只带一个任期 */
        hex[n] = data_entry_fetch_hex(ctx, entry_partition_lsn(e.payload));
        if (hex[n] == NULL)
            break;
        eterm = e.term;
        idx[n] = e.index;
        payload[n] = pstrdup(e.payload);
        n++;
    }
    if (n < 2)
        return false;

    initStringInfo(&sql);
    gc_build_batch_sql(&sql, ctx, term, prev_idx, prev_term, leader_commit, n, idx, eterm,
                       (const char *const *) payload, (const char *const *) hex);

    conn = peer_conn_get(&peers[slot]);
    if (conn == NULL)
    {
        pfree(sql.data);
        peer_mark_result(&peers[slot], false);
        return false;
    }
    if (PQsendQuery(conn, sql.data) != 1)
    {
        pfree(sql.data);
        peer_conn_reset(slot);
        return false;
    }
    pfree(sql.data);
    res = pq_result_bounded(conn, GetCurrentTimestamp(),
                            Min(RAFT_RPC_EXEC_TIMEOUT_MS + 300 * n, 30000));
    if (res == NULL)
    {
        peer_conn_reset(slot);
        peer_mark_result(&peers[slot], false);
        peer_stamp(ctx, slot, false);
        return false;
    }
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
        parsed = parse_resp2(PQgetvalue(res, 0, 0), &rt, &ok);
    PQclear(res);
    if (!parsed)
        return false;
    peer_mark_result(&peers[slot], true);
    peer_stamp(ctx, slot, true);
    if (rt > term)
    {
        step_down_if_higher(ctx, rt);
        return true;
    }
    if (!ok)
        return false;           /* 让单条路径按原规则回退 next_index */

    SpinLockAcquire(&ctx->log->mutex);
    if (idx[n - 1] > ctx->log->peer_match_index[slot])
        ctx->log->peer_match_index[slot] = idx[n - 1];
    ctx->log->peer_next_index[slot] = idx[n - 1] + 1;
    SpinLockRelease(&ctx->log->mutex);
    return true;
}

/*
 * 批量 propose：n 条 OP_PARWAL 描述符（payload）+ 字节（hex）。
 * 返回从第一条起连续提交成功的条数（0 = 一条都没成）。
 */
static int
group_propose_batch(RaftGroupCtx *ctx, int n, char **payload, char **hex)
{
    bool   single_member;
    int64  idx[RAFT_LOG_CAPACITY];
    int64  term;
    int64  first;
    int64  committed_upto;
    int64  last;
    int    acks;
    int    majority;
    int    k;
    int    room;
    bool   is_leader;

    SpinLockAcquire(&ctx->cons->mutex);
    is_leader = (ctx->cons->state == RAFT_LEADER);
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);
    if (!is_leader || n <= 0)
        return 0;

    restore_persistent_log_if_needed(ctx);
    parse_peers();
    majority = cluster_majority(ctx);

    if (!wait_for_log_room(ctx))
        return 0;               /* 交给逐条路径去报"环满"那条详尽的 WARNING */

    /* 一次持锁追加，保证 index 连续（同组复制认领位已串行化 propose，这里再加一道） */
    SpinLockAcquire(&ctx->log->mutex);
    room = (int) (RAFT_LOG_CAPACITY - 1 - (ctx->log->last_log_index - ctx->log->last_applied));
    if (n > room)
        n = room;
    for (k = 0; k < n; k++)
    {
        idx[k] = log_append_locked(ctx, term, RAFT_OP_PARWAL, payload[k]);
        if (idx[k] <= 0)
            break;
    }
    SpinLockRelease(&ctx->log->mutex);
    n = k;
    if (n <= 0)
        return 0;
    first = idx[0];
    last = idx[n - 1];

    /*
     * 单成员组（多数派 = 1，单副本模式）：本地落盘即提交。raft_log 行与用户数据在同一个
     * 本地事务里，事务提交 ⇔ 条目持久化 ⇔ Raft 提交，所以直接写成 committed，
     * 省掉随后那次 mark_log_committed_sql 的 UPDATE。事务回滚时行随之消失，
     * 与原路径一样由 repersist_from_ring 按环补写。
     */
    single_member = (majority == 1);
    persist_log_entries_sql_batch(ctx, n, idx, term, RAFT_OP_PARWAL, payload, single_member);

    raft_spi_ctx = true;
    acks = gc_replicate_batch(ctx, term, first, n, idx,
                              (const char *const *) payload, (const char *const *) hex);

    SpinLockAcquire(&ctx->log->mutex);
    if (acks >= majority && last > ctx->log->commit_index)
        ctx->log->commit_index = last;
    else
    {
        int64 nc = compute_new_commit_index(ctx, term);

        if (nc > ctx->log->commit_index)
            ctx->log->commit_index = nc;
    }
    committed_upto = ctx->log->commit_index;
    SpinLockRelease(&ctx->log->mutex);
    persist_hard_state_unlocked(ctx);

    if (committed_upto < last)
    {
        int64 j;

        elog(LOG,
             "pg_raft: group %lld 批量 propose idx=%lld..%lld term=%lld 多数派不足（%d/%d），丢弃未提交的尾巴",
             (long long) ctx->group_id, (long long) first, (long long) last,
             (long long) term, acks, majority);
        raft_spi_ctx = false;
        for (j = last; j > committed_upto && j >= first; j--)
            discard_uncommitted_entry(ctx, j);
        if (committed_upto < first)
            return 0;
        raft_spi_ctx = true;
    }

    if (single_member && committed_upto >= last)
    {
        if (last > ctx->g->sql_marked_upto)
            ctx->g->sql_marked_upto = last;    /* 已按 committed 写入，无需再 UPDATE */
    }
    else
        mark_log_committed_sql(ctx, Min(committed_upto, last));
    flush_replication(ctx, Min(committed_upto, last), term);
    raft_spi_ctx = false;
    group_apply_pending(ctx);
    return (int) (Min(committed_upto, last) - first + 1);
}

/*
 * 批量版 data_propose_one：一次 SPI 读出 [lo, hi] 的描述符与字节，批量 propose。
 * 返回从 lo 起连续成功的条数；0 = 第一条就没成（调用方按原规则 ERROR）。
 */
static int
data_propose_batch(RaftGroupCtx *ctx, int64 lo, int64 hi)
{
    StringInfoData sql;
    bool           spi_owned;
    int64          local_oid;
    int            n = 0;
    int            want = (int) (hi - lo + 1);
    int            done;
    char         **payload;
    char         **hex;
    MemoryContext  oldcxt;
    uint64         r;

    local_oid = group_local_partition(ctx);
    if (local_oid <= 0)
        ereport(ERROR,
                (errmsg("pg_raft: group %lld 在本节点没有对应分片（先跑 partdist.rebuild_shard_identity()）",
                        (long long) ctx->group_id)));
    if (!raft_persist_spi_begin(&spi_owned))
        return 0;

    payload = (char **) MemoryContextAllocZero(TopTransactionContext, sizeof(char *) * want);
    hex = (char **) MemoryContextAllocZero(TopTransactionContext, sizeof(char *) * want);

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT s, r.orig_lsn::text, r.rmid, r.info, r.gxid, length(r.data), r.flags, "
                     "       encode(r.data, 'hex') "
                     "FROM generate_series(%lld::bigint, %lld::bigint) s "
                     "LEFT JOIN LATERAL partdist.partwal_read_record(%u::oid, s) r ON true "
                     "ORDER BY s",
                     (long long) lo, (long long) hi, (unsigned) local_oid);
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT)
    {
        oldcxt = MemoryContextSwitchTo(TopTransactionContext);
        for (r = 0; r < SPI_processed && n < want; r++)
        {
            HeapTuple  tup = SPI_tuptable->vals[r];
            TupleDesc  td = SPI_tuptable->tupdesc;
            bool       isnull;
            Datum      d;
            char      *orig;
            int64      plsn;
            int        rmid, info, flags;
            int64      gxid, nbytes;
            StringInfoData pl;

            plsn = DatumGetInt64(SPI_getbinval(tup, td, 1, &isnull));
            d = SPI_getbinval(tup, td, 2, &isnull);
            if (isnull || plsn != lo + n)
                break;          /* 记录不存在：停在这里，剩下的交给逐条路径去报错 */
            orig = TextDatumGetCString(d);
            d = SPI_getbinval(tup, td, 3, &isnull);  rmid = isnull ? 0 : DatumGetInt32(d);
            d = SPI_getbinval(tup, td, 4, &isnull);  info = isnull ? 0 : DatumGetInt32(d);
            d = SPI_getbinval(tup, td, 5, &isnull);  gxid = isnull ? 0 : DatumGetInt64(d);
            d = SPI_getbinval(tup, td, 6, &isnull);  nbytes = isnull ? 0 : DatumGetInt32(d);
            d = SPI_getbinval(tup, td, 7, &isnull);  flags = isnull ? PARTWAL_FLAG_DATA : DatumGetInt32(d);
            d = SPI_getbinval(tup, td, 8, &isnull);
            if (isnull)
                break;
            hex[n] = TextDatumGetCString(d);

            initStringInfo(&pl);
            appendStringInfo(&pl,
                             "{\"partition_lsn\":%lld,\"orig_lsn\":\"%s\",\"rmid\":%d,"
                             "\"info\":%d,\"flags\":%d,\"gxid\":%lld,\"nbytes\":%lld}",
                             (long long) plsn, orig, rmid, info, flags,
                             (long long) gxid, (long long) nbytes);
            payload[n] = pl.data;
            n++;
        }
        MemoryContextSwitchTo(oldcxt);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    if (n == 0)
        return 0;
    done = group_propose_batch(ctx, n, payload, hex);
    if (done > 0 && lo + done - 1 > ctx->g->last_data_plsn)
        ctx->g->last_data_plsn = lo + done - 1;
    return done;
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
        bool            hot;

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

        /*
         * 2026-10-17：复制流管着的数据组整组不碰 —— 这里会抢认领位（挡住提交者追加）、按副本自报的
         * last 下调 next_index（打乱复制流的乐观推进）、再用单条 RPC 补发（副本上与复制流会话抢同一组
         * 的行锁，实测互相卡住 400 s）。落后（环内或环外）一律由复制流按序补。
         */
        if (rs_stream_data(&ctx))
            continue;

        /*
         * 2026-10-16：有提交流量的组（提交路径最近 3 s 内跑过一轮）由提交路径自己补发落后
         * 副本（窗口内按 next_index 补缺口），这里只管它补不到的 —— 落后超出窗口的成员。
         * 原来不分冷热：慢副本按设计总落后几批（多数派只等快的那个），本通道每 5 s 把每个
         * 主组的认领位拿去做同步 RPC 追平，提交路径整组排队（三副本实测 25 tps）。
         */
        hot = (g->last_rm_round != 0 &&
               !TimestampDifferenceExceeds(g->last_rm_round, GetCurrentTimestamp(), 3000));
        if (hot)
        {
            bool far_behind = false;
            int  p1;

            SpinLockAcquire(&ctx.log->mutex);
            for (p1 = 0; p1 < n_peers; p1++)
                if (peer_in_group(&ctx, p1) &&
                    ctx.log->peer_next_index[p1] + RAFT_LOG_CAPACITY / 2 <= ctx.log->last_log_index)
                    far_behind = true;
            SpinLockRelease(&ctx.log->mutex);
            if (!far_behind)
                continue;
        }

        raft_claim_purpose = RAFT_CLAIM_CATCHUP;
        if (!replicate_try_claim(&ctx))
            continue;

        raft_spi_ctx = true;
        PG_TRY();
        {
            int p0;

            restore_persistent_log_if_needed(&ctx);

            /*
             * ★ R-P4-12（2026-08-15 解冻批次 #4，周期化）：先把**本节点自己**
             * 的 apply 积压排空，再去补发给 follower。
             *
             * 新当选的 leader 可能正是尚未 apply 某条决议的成员（决议在多数派
             * 上、选举合法）；它的本地 dtx_decision 缺那一行，dtx_peek 的
             * leader 门控又只认本地表 ⇒ 谁都问不到判决。首版把 drain 挂在
             * dtx_peek 里，只在"有人来问"时才追平——没人问的窗口依旧空转
             * （实测 T4.7 的 M4/Q3 在 3 轮里各红过 1–2 次，形态一致）。
             * 挂到这里就变成**周期性自愈**：monitor 每 catchup_interval_ms
             * 触发一次，且此处已确认本节点是该组 leader、已持复制认领、
             * 日志已恢复——正是排空的最佳时机。group_apply_pending 幂等，
             * 无积压时是一次空转。
             */
            group_apply_pending(&ctx);

            /* 先按对端自报的 last_log_index 给 next_index 一个起点（只降不升；热组不问，见上） */
            for (p0 = 0; p0 < n_peers && !hot; p0++)
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

            /* 热组：提交路径在排队等这把认领位，至多两轮就放手（落后窗口外的成员慢慢追） */
            for (round = 0; round < (hot ? 2 : RAFT_CATCHUP_MAX_ROUNDS); round++)
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
                    /* 热组：窗口内的落后由提交路径补，这里不碰 */
                    if (hot && next_before + RAFT_LOG_CAPACITY / 2 > last_idx)
                        continue;
                    /*
                     * 复制流管着的数据组：落后（环内或环外）一律由复制流按序补 —— 两条连接同时补
                     * 同一组，副本上两个会话会抢同一组的行锁，实测把复制流单批拖到十几秒。
                     */
                    if (rs_stream_data(&ctx))
                        continue;

                    any_behind = true;

                    /* 不可达对端在退避窗口里，send_sql_rpc 自己会跳过 */
                    if (!catchup_batch_to_peer(&ctx, p))    /* P7-N41：能成批就成批 */
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

            /* 补齐后重算提交点：落后成员归队可能让更老的条目刚刚够多数派（热组由提交路径推进） */
            if (!hot)
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
 * ★ P7-N47：本节点上某数据组此刻的角色与任期（纯共享内存）。
 * 返回 false = 本节点没有这个组的状态（未加载 / 不是成员），调用方别据此下结论。
 */
static bool raft_role_restoring = false;   /* 防重入：灌注册表的 SPI 查询本身也会开表 */

bool
pg_raft_data_group_role(int64 group_id, bool *is_leader, int64 *term,
                        TimestampTz *leader_seen_at)
{
    int i;

    if (!pg_raft_raft_enabled || RaftGroups == NULL || group_id <= 0)
        return false;
    for (i = 0; i < RAFT_MAX_GROUPS; i++)
    {
        RaftGroupState *g = &RaftGroups->groups[i];

        if (!g->in_use || g->group_id != group_id)
            continue;
        SpinLockAcquire(&g->cons.mutex);
        *is_leader = (g->cons.state == RAFT_LEADER);
        *term = g->cons.current_term;
        SpinLockRelease(&g->cons.mutex);
        if (leader_seen_at)
            *leader_seen_at = g->leader_seen_at;
        return true;
    }

    /*
     * 没找到：重启后组注册表是各后端首次走 SQL 路径时惰性灌回共享内存的。读闸门恰恰
     * 要管"刚拉起"这个窗口，所以本后端还没灌过就就地灌一次再查（需要事务 + SPI）。
     */
    if (!groups_restored && IsTransactionState() && !raft_role_restoring)
    {
        bool found;

        raft_role_restoring = true;
        PG_TRY();
        {
            parse_peers();
            restore_groups_if_needed();
        }
        PG_FINALLY();
        {
            raft_role_restoring = false;
        }
        PG_END_TRY();
        found = false;
        for (i = 0; i < RAFT_MAX_GROUPS && !found; i++)
        {
            RaftGroupState *g = &RaftGroups->groups[i];

            if (!g->in_use || g->group_id != group_id)
                continue;
            SpinLockAcquire(&g->cons.mutex);
            *is_leader = (g->cons.state == RAFT_LEADER);
            *term = g->cons.current_term;
            SpinLockRelease(&g->cons.mutex);
            if (leader_seen_at)
                *leader_seen_at = g->leader_seen_at;
            found = true;
        }
        return found;
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
/*
 * ★ P7-N34（2026-09-20）：控制面 apply 里等认领位要**短**。
 *
 * 控制面那条"登记为主"的 apply 跑在 AE 的 RPC 事务里：它先 UPSERT partition_map
 * （握着那一行的行锁），再走 partwal_notify_primary_switch 发交接广播 —— 广播要复制，
 * 于是在**握着行锁**的状态下等该分片的认领位。此刻若有写入先拿到认领位，两边就互等：
 * 认领位是本扩展自己在 shmem 里的位，PG 的死锁检测看不见它，只能等超时。实测宕机切主后
 * 新主上写入被卡到 60 s 超时，反复几轮共约 6 分钟不可写。
 *
 * 让 apply 这一侧先放手：等不到就快速失败，事务回滚（apply 游标由 P7-N29 退回），
 * 下一次 AE 心跳自然重做，代价是几秒；而占着认领位的那笔写入立刻能走完。
 */
#define RAFT_REPLICATE_CLAIM_APPLY_MS    3000

/*
 * 本后端已持有的认领位（组 → 重入深度）。
 *
 * 认领位原本不可重入：持有者自己再申请一次会去等自己，直到 60 s 超时报错。
 * 复制路径上会嵌套（基线/交接广播里再触发一次复制），事务 ERROR 后也可能留下
 * "shmem 记着我、本后端却已经不在那段代码里"的残留。两种情形都不该等自己。
 */
#define RAFT_MAX_SELF_CLAIMS 64    /* 2026-10-16：多组一轮复制一次要同时持有一笔事务触达的全部组 */
static struct { int64 gid; int depth; } raft_self_claims[RAFT_MAX_SELF_CLAIMS];

static int *
self_claim_depth(int64 group_id, bool create)
{
    int i;
    int free_i = -1;

    for (i = 0; i < RAFT_MAX_SELF_CLAIMS; i++)
    {
        if (raft_self_claims[i].depth > 0 && raft_self_claims[i].gid == group_id)
            return &raft_self_claims[i].depth;
        if (raft_self_claims[i].depth == 0 && free_i < 0)
            free_i = i;
    }
    if (!create || free_i < 0)
        return NULL;
    raft_self_claims[free_i].gid = group_id;
    raft_self_claims[free_i].depth = 0;
    return &raft_self_claims[free_i].depth;
}

static void
replicate_claim(RaftGroupCtx *ctx)
{
    long waited_us = 0;
    int  last_holder = 0;
    int  holder_purpose = 0;
    TimestampTz holder_since = 0;
    int  timeout_ms = raft_in_control_apply ? RAFT_REPLICATE_CLAIM_APPLY_MS
                                            : RAFT_REPLICATE_CLAIM_TIMEOUT_MS;
    int *depth = self_claim_depth(ctx->group_id, false);

    if (depth != NULL)          /* 已经是自己持有：重入，不要等自己 */
    {
        (*depth)++;
        return;
    }

    for (;;)
    {
        bool got = false;
        int  holder = 0;

        TimestampTz now_ts = GetCurrentTimestamp();

        SpinLockAcquire(&RaftGroups->mutex);
        if (!ctx->g->replicate_in_progress)
        {
            ctx->g->replicate_in_progress = true;
            ctx->g->replicate_pid = MyProcPid;
            ctx->g->replicate_purpose = raft_claim_purpose;
            ctx->g->replicate_since = now_ts;
            got = true;
        }
        else
        {
            holder = ctx->g->replicate_pid;
            holder_purpose = ctx->g->replicate_purpose;
            holder_since = ctx->g->replicate_since;
            if (holder == MyProcPid)
            {
                /* shmem 记着我、本后端却没有在册深度 ⇒ 上一笔事务 ERROR 后的残留，收回继续用 */
                ctx->g->replicate_in_progress = true;
                got = true;
            }
        }
        SpinLockRelease(&RaftGroups->mutex);

        if (got)
        {
            depth = self_claim_depth(ctx->group_id, true);
            if (depth != NULL)
                *depth = 1;
            if (waited_us >= 500000L)
            {
                static TimestampTz last_log = 0;

                if (last_log == 0 ||
                    TimestampDifferenceExceeds(last_log, GetCurrentTimestamp(), 2000))
                {
                    last_log = GetCurrentTimestamp();
                    elog(LOG, "pg_raft: [claim] 组 %lld 等认领位 %ld ms（用途 %s）；最后看到的持有者 pid %d 用途 %s、已持有 %ld ms",
                         (long long) ctx->group_id, waited_us / 1000,
                         raft_claim_purpose_name[raft_claim_purpose],
                         last_holder, raft_claim_purpose_name[holder_purpose],
                         holder_since ? (long) ((GetCurrentTimestamp() - holder_since) / 1000) : -1L);
                }
            }
            return;
        }
        if (holder != 0)
            last_holder = holder;

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
                depth = self_claim_depth(ctx->group_id, true);
                if (depth != NULL)
                    *depth = 1;
                return;
            }
        }

        if (waited_us >= (long) timeout_ms * 1000L)
            ereport(ERROR,
                    (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                     errmsg("pg_raft: 等待组 %lld 的复制认领位超过 %d ms，%s",
                            (long long) ctx->group_id, timeout_ms,
                            raft_in_control_apply ? "本次控制面 apply 放弃（回滚后由下一次 AE 重做，P7-N34）"
                                                  : "prepare 失败"),
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
    int *depth = self_claim_depth(ctx->group_id, false);

    if (depth != NULL)          /* 已经是自己持有：重入（P7-N34） */
    {
        (*depth)++;
        return true;
    }

    {
        TimestampTz now = GetCurrentTimestamp();
        int         holder = 0;

        SpinLockAcquire(&RaftGroups->mutex);
        if (!ctx->g->replicate_in_progress || ctx->g->replicate_pid == MyProcPid)
        {
            ctx->g->replicate_in_progress = true;
            ctx->g->replicate_pid = MyProcPid;
            ctx->g->replicate_purpose = raft_claim_purpose;
            ctx->g->replicate_since = now;
            got = true;
        }
        else
            holder = ctx->g->replicate_pid;
        SpinLockRelease(&RaftGroups->mutex);

        /*
         * 2026-10-17：持有者已经不在了（FATAL 退出不走 PG_FINALLY）就收回 —— 与阻塞版
         * replicate_claim 同一判据。流水线提交路径只用这个非阻塞版，不收回的话死者留下的
         * 认领位会让该组此后每笔提交都等满超时。
         */
        if (!got && holder != 0 && BackendPidGetProc(holder) == NULL)
        {
            SpinLockAcquire(&RaftGroups->mutex);
            if (ctx->g->replicate_in_progress && ctx->g->replicate_pid == holder)
            {
                ctx->g->replicate_pid = MyProcPid;
                ctx->g->replicate_purpose = raft_claim_purpose;
                ctx->g->replicate_since = now;
                got = true;
            }
            SpinLockRelease(&RaftGroups->mutex);
            if (got)
                elog(WARNING, "pg_raft: 组 %lld 的复制认领位由已消失的 backend %d 持有，已回收",
                     (long long) ctx->group_id, holder);
        }
    }

    if (got)
    {
        depth = self_claim_depth(ctx->group_id, true);
        if (depth != NULL)
            *depth = 1;
    }
    return got;
}

static void
replicate_release(RaftGroupCtx *ctx)
{
    int *depth = self_claim_depth(ctx->group_id, false);

    if (depth != NULL)
    {
        if (*depth > 1)         /* 嵌套：外层还要用，别真放（P7-N34） */
        {
            (*depth)--;
            return;
        }
        *depth = 0;
    }

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

    /*
     * 单副本快速提交：单成员数据组完全不做 Raft 提议，只推进游标。
     *
     * 没有 follower，组日志没有读者；而提议要经 SPI 从段文件把记录读回来组装条目 ——
     * 后端本地的稀疏索引在 dtx_decide 这类经池化连接进来的后端里几乎总是空的，
     * 退化成整条分区流扫描（128 终端实测写一条决议平均 155 ms，在协调组认领位里
     * 串行，发起方平均等 700 ms）。各条路径的持久性另有承载：
     *   - 数据记录：pg_wal（快速模式的前提）；
     *   - DTX 决议：dtx_write_decision 直接写 partdist.dtx_decision，随 dtx_decide
     *     的事务提交（WAL fsync）后才回给发起方 —— 这就是单成员组上的"多数派持久化"；
     *   - FORGET：dtx_forget_append 在此模式下直接删决议行（原先靠 leader apply 删）。
     */
    if (pg_raft_single_member_fast_commit && group_cluster_size(ctx) == 1)
    {
        if (cur_plsn > ctx->g->last_data_plsn)
            ctx->g->last_data_plsn = cur_plsn;
        return;
    }

    /* 2026-10-17 复制流：追加进环、交管理进程发送、等组提交点覆盖 */
    if (rs_stream_active())
    {
        rs_replicate_claimed(ctx, cur_plsn, partition_id);
        return;
    }

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
        int64 idx;

        /*
         * P7-N41 group commit：积压不止一条就整批走（上限 replicate_batch_max）；
         * 批量路径一条都没成时落回下面的逐条路径，由它按原规则报错。
         */
        if (pg_raft_replicate_batch_max > 1 && cur_plsn > plsn)
        {
            int64 hi = Min(cur_plsn, plsn + pg_raft_replicate_batch_max - 1);
            int   done;

            CHECK_FOR_INTERRUPTS();
            done = data_propose_batch(ctx, plsn, hi);
            if (done > 0)
            {
                plsn += done - 1;       /* 循环尾的 plsn++ 补上最后一格 */
                continue;
            }
        }

        /*
         * P7-N21：历史补发可能很长（冷启动的组从 plsn 1 起逐条重提），每条都给取消/终止
         * 一个响应点 —— 否则只能 immediate 重启回收。认领位由调用方的 PG_FINALLY 归还。
         */
        CHECK_FOR_INTERRUPTS();

        /*
         * group_propose 只在拿到多数派 ack 之后才返回 idx > 0，而 follower
         * 是**先 fsync 再 ack** 的（运输层加固 §11.5.1 #1/#3）——所以
         * "返回成功" 严格等价于 "该条目已在多数派持久化"。
         * prepare 路径靠它得到 prepared 语义；决议路径靠它得到**提交点**。
         */
        idx = data_propose_one(ctx, plsn);

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

    /*
     * ★ 单副本快速提交：单成员组只做写栅栏（leader 检查），不 SPI。
     * 原路径每个触达分区每次提交都要 SPI 查组号 + flush 点（快速模式下 flush 点无用），
     * 一笔 NewOrder 五次。分区 → 全局分片号是建表时定死的映射，按后端缓存；
     * 未命中才查一次。成员数未知（0）或多成员组照常走下面的完整路径。
     */
    if (pg_raft_single_member_fast_commit)
    {
#define FAST_GID_CACHE 1024
        static Oid   fc_part[FAST_GID_CACHE];
        static int64 fc_gid[FAST_GID_CACHE];
        static int   fc_n = 0;
        int64        fgid = 0;
        int          k;

        for (k = 0; k < fc_n; k++)
            if (fc_part[k] == partition_id)
            {
                fgid = fc_gid[k];
                break;
            }
        if (k == fc_n && raft_persist_spi_begin(&spi_owned))
        {
            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "SELECT partdist.global_id_for_partition(%u::oid)::bigint",
                             (unsigned) partition_id);
            if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
            {
                Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

                if (!isnull)
                    fgid = DatumGetInt64(d);
            }
            pfree(sql.data);
            raft_persist_spi_end(spi_owned);
            if (fgid > 0 && fc_n < FAST_GID_CACHE)
            {
                fc_part[fc_n] = partition_id;
                fc_gid[fc_n] = fgid;
                fc_n++;
            }
        }
        if (fgid <= 0)
            return;                 /* 非全局分片：不纳管 */
        if (raft_group_ctx(fgid, &ctx) && group_cluster_size(&ctx) == 1)
        {
            SpinLockAcquire(&ctx.cons->mutex);
            state = ctx.cons->state;
            SpinLockRelease(&ctx.cons->mutex);
            if (state != RAFT_LEADER)
                ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                         errmsg("pg_raft: 分区 %u(组 %lld)的本地写入被拒：本节点不是该分区组的 leader",
                                partition_id, (long long) fgid),
                         errdetail("分区主副本可能已切换，请经路由层重试。")));
            return;
        }
        /* 组未就绪 / 多成员：走完整路径 */
    }

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
     * ★ 单副本快速提交：单成员数据组的提交路径不做 Raft 复制。
     *
     * 没有 follower，这一步只是记账（raft_log 行 + 环 + hardstate），却要占组的复制认领位
     * 串行执行，并从段文件回读记录 —— 32 终端实测 1/3 的后端睡在认领位上、另有一批读者
     * 等这些提交落定。in-doubt 闭合扫的是 parwal 流而不是 raft_log；DTX 决议走
     * dtx_write_decision 自己的同步复制，不经过这里。
     *
     * 只把复制游标推进到 cur：之后真正需要复制时（决议记录）只送新增的那几条，
     * 不会回头补历史。写栅栏（上面的 leader 检查）照旧。
     */
    if (pg_raft_single_member_fast_commit && group_cluster_size(&ctx) == 1)
    {
        if (cur > ctx.g->last_data_plsn)
            ctx.g->last_data_plsn = cur;
        return;
    }

    /*
     * cur 是取自本地 parwal 的当前 flush 点，它必然 >= 本事务刚落盘的那些记录的
     * partition_lsn —— group commit 让路时本事务的记录是被并发 backend 写下去的，
     * 但"写下去"发生在 flushed_upto 推进之前，所以到这里 cur 已经覆盖它们。
     * 因此"复制到 cur"是覆盖本事务的安全上界（DTX_2PC_DESIGN.md §9.1）。
     */
    raft_claim_purpose = RAFT_CLAIM_COMMIT_ONE;
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

/* ================================================================== */
/* 2026-10-16：多组一轮复制（多副本提交路径）                          */
/* ================================================================== */

/*
 * 原路径（pg_raft_partwal_replicate，逐分区）：一笔事务触达 G 个分区组，就按顺序做 G 次
 * "认领 → SPI 查组号 / 读记录 / 写 raft_log → 对每个副本一条 RPC → 等**全部**副本回 →
 * 写 hardstate / 标 committed / apply → flush_replication 同步追平未回的副本"。延迟是
 * G × 最慢副本的一轮，且最慢的副本（机械盘、同机还有别的节点）决定一切。
 *
 * 这里把一笔事务的全部组合成**一轮**：
 *   · 按组号顺序认领（多组持有不会互相死锁）；SPI 合并成 4 次（查组 / 读记录 / 写日志行 /
 *     标 committed），与组数无关；
 *   · 发给每个副本节点的是**一条**多语句请求（各组一条 append_entries_batch，同一隐式事务，
 *     对端一次 WAL 提交）—— RPC 数 = 副本节点数，与分区数无关；
 *   · 各组拿到**多数派**即提交（RF=3 等 1 个副本，RF=5 等 2 个），没回的副本留作在途，
 *     本后端下次用这条连接时再收尾并据此推进 match_index；
 *   · 不再在提交路径里同步追平慢副本（flush_replication）。落后的副本在下一轮里按它的
 *     next_index 补发缺口（窗口内、同任期），超出窗口交后台追平通道。
 * 语义不变：多数派 ack ⇔ 多数派已持久化（对端整批 fsync + 事务提交后才回）；
 * 凑不齐多数派的组按原规则丢弃未提交尾巴、本事务 ERROR 中止。
 */
#define RM_MAX_GROUPS   64
#define RM_CONNS        2
#define RM_CATCHUP_WIN  (RAFT_LOG_CAPACITY - 8)
#define RM_STALE_MS     60000       /* 在途请求这么久没回：判失败、重连（对端卡死时别永远占着连接） */

typedef struct RmPendGroup
{
    int64   gid;
    int64   first;          /* 本次发给该对端的第一条 index */
    int64   last;
    int64   term;
    uint64  gen;            /* 发出时该组的 trunc_gen：其后环尾被截过 ⇒ 这条回执作废 */
    int     gslot;          /* 本轮 RmGroup 下标（-1 = 已不属于进行中的一轮） */
} RmPendGroup;

typedef struct RmConn
{
    PGconn     *conn;
    bool        busy;           /* 有在途请求，结果未取完 */
    bool        current;        /* 属于本后端正在进行的一轮 */
    TimestampTz sent_at;        /* 在途请求的发出时刻 */
    int         ngroups;
    RmPendGroup groups[RM_MAX_GROUPS];
    int         nres;
    bool        any_error;
    int64       rterm[RM_MAX_GROUPS];
    int         rok[RM_MAX_GROUPS];
} RmConn;

static RmConn rm_conns[RAFT_MAX_PEERS][RM_CONNS];

static void
rm_conn_fail(RmConn *c, int slot)
{
    if (c->conn != NULL)
        PQfinish(c->conn);
    c->conn = NULL;
    c->busy = false;
    c->any_error = true;
    peer_mark_result(&peers[slot], false);
}

/*
 * 收一条在途请求的结果。block=false 只收已到的；返回 true = 已收完（或已判失败）。
 * 多语句请求：每条语句一个结果，最后 NULL（ReadyForQuery，对端隐式事务已提交）。
 */
static bool
rm_conn_poll(RmConn *c, int slot, bool block, TimestampTz deadline)
{
    if (!c->busy)
        return true;
    for (;;)
    {
        int             sock;
        struct pollfd   pfd;
        long            remain_ms;

        if (!PQconsumeInput(c->conn))
        {
            rm_conn_fail(c, slot);
            return true;
        }
        while (!PQisBusy(c->conn))
        {
            PGresult   *r = PQgetResult(c->conn);

            if (r == NULL)
            {
                c->busy = false;
                return true;
            }
            if (PQresultStatus(r) == PGRES_TUPLES_OK && PQntuples(r) == 1 &&
                c->nres < c->ngroups)
            {
                int64   rt = 0;
                int     ok = 0;

                if (!parse_resp2(PQgetvalue(r, 0, 0), &rt, &ok))
                {
                    ok = 0;
                    c->any_error = true;
                }
                c->rterm[c->nres] = rt;
                c->rok[c->nres] = ok;
                c->nres++;
            }
            else
            {
                static TimestampTz last_warn = 0;

                if (PQresultStatus(r) == PGRES_FATAL_ERROR &&
                    (last_warn == 0 ||
                     TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 10000)))
                {
                    last_warn = GetCurrentTimestamp();
                    elog(WARNING, "pg_raft: 多组复制到节点 %d 出错：%s",
                         peers[slot].node_id, PQresultErrorMessage(r));
                }
                c->any_error = true;
            }
            PQclear(r);
        }
        if (!block)
            return false;
        remain_ms = (long) ((deadline - GetCurrentTimestamp()) / 1000);
        if (remain_ms <= 0)
        {
            static TimestampTz last_warn = 0;

            if (last_warn == 0 ||
                TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 10000))
            {
                last_warn = GetCurrentTimestamp();
                elog(WARNING, "pg_raft: 多组复制到节点 %d 超时，按该成员未确认处理",
                     peers[slot].node_id);
            }
            rm_conn_fail(c, slot);
            return true;
        }
        sock = PQsocket(c->conn);
        if (sock < 0)
        {
            rm_conn_fail(c, slot);
            return true;
        }
        /* poll 而不是 select：后端开着大量文件时 libpq 套接字号可能超过 FD_SETSIZE */
        pfd.fd = sock;
        pfd.events = POLLIN;
        pfd.revents = 0;
        (void) poll(&pfd, 1, (int) Min(remain_ms, 100));
        CHECK_FOR_INTERRUPTS();
    }
}

/*
 * 已收完的请求：把各组的确认 / 拒绝落进共享的 match / next_index。
 *
 * 回执可能迟到很久（留作在途的请求由本后端下一次用这条连接时才收尾），期间本节点可能已经
 * 丢了 leader、换了任期，环里同一 index 上也可能已是别的条目。所以只在"仍是发出时那个任期的
 * leader、且 match 指向的环内条目正是那个任期的"时才采信 —— 否则这条确认对当前日志没有意义。
 */
static void
rm_conn_apply(RmConn *c, int slot)
{
    int i;

    for (i = 0; i < c->ngroups; i++)
    {
        RmPendGroup *pg = &c->groups[i];
        RaftGroupCtx ctx;
        bool         ok = (!c->any_error && i < c->nres && c->rok[i]);
        int64        rt = (i < c->nres) ? c->rterm[i] : 0;
        bool         same_term;

        if (!raft_group_ctx(pg->gid, &ctx))
            continue;
        if (rt > pg->term)
        {
            step_down_if_higher(&ctx, rt);
            continue;
        }
        SpinLockAcquire(&ctx.cons->mutex);
        same_term = (ctx.cons->state == RAFT_LEADER && ctx.cons->current_term == pg->term);
        SpinLockRelease(&ctx.cons->mutex);
        if (!same_term)
            continue;

        SpinLockAcquire(&ctx.log->mutex);
        if (ok)
        {
            int64         m = Min(pg->last, ctx.log->last_log_index);
            RaftLogEntry *le = (m > 0) ? log_slot(&ctx, m) : NULL;

            /*
             * 同任期里 leader 自己丢过尾巴（多数派不足）之后，同一 index 可能已换成别的条目
             * （同任期的 CONFIG 等）：只看 term 分不出来，靠截断代数分。
             */
            if (le != NULL && le->index == m && le->term == pg->term &&
                ctx.log->trunc_gen == pg->gen)
            {
                if (m > ctx.log->peer_match_index[slot])
                    ctx.log->peer_match_index[slot] = m;
                if (ctx.log->peer_next_index[slot] < m + 1)
                    ctx.log->peer_next_index[slot] = m + 1;
            }
        }
        else if (ctx.log->peer_next_index[slot] > pg->first)
            ctx.log->peer_next_index[slot] = pg->first;     /* 下一轮从缺口补发 */
        SpinLockRelease(&ctx.log->mutex);
        peer_stamp(&ctx, slot, i < c->nres);
    }
    if (!c->any_error)
        peer_mark_result(&peers[slot], true);
    c->ngroups = 0;
    c->nres = 0;
    c->any_error = false;
    c->current = false;
}

/*
 * 取一条空闲的复制连接（顺手收掉已完成 / 超时的在途请求）。
 * 全在途时：block=false 返回 NULL 并置 *all_busy；block=true 等最早发出的那条收完（或到
 * deadline 判失败）再用它。连不上返回 NULL。
 */
static RmConn *
rm_conn_get(int slot, bool block, TimestampTz deadline, bool *all_busy)
{
    int         k;
    char        conninfo[512];
    TimestampTz now = GetCurrentTimestamp();
    RmConn     *c = NULL;

    *all_busy = false;
    for (k = 0; k < RM_CONNS; k++)
    {
        RmConn *x = &rm_conns[slot][k];

        if (!x->busy)
            continue;
        if (rm_conn_poll(x, slot, false, 0))
            rm_conn_apply(x, slot);
        else if (TimestampDifferenceExceeds(x->sent_at, now, RM_STALE_MS))
        {
            elog(WARNING, "pg_raft: 多组复制到节点 %d 的在途请求 %d s 未回，判失败并重连",
                 peers[slot].node_id, RM_STALE_MS / 1000);
            rm_conn_fail(x, slot);
            rm_conn_apply(x, slot);
        }
    }
    for (k = 0; k < RM_CONNS && c == NULL; k++)
        if (!rm_conns[slot][k].busy)
            c = &rm_conns[slot][k];
    if (c == NULL)
    {
        if (!block)
        {
            *all_busy = true;
            return NULL;
        }
        c = &rm_conns[slot][0];
        for (k = 1; k < RM_CONNS; k++)
            if (rm_conns[slot][k].sent_at < c->sent_at)
                c = &rm_conns[slot][k];
        (void) rm_conn_poll(c, slot, true, deadline);       /* 收完，或判超时、断开 */
        rm_conn_apply(c, slot);
        if (c->conn == NULL)
            return NULL;        /* 等到本轮截止都没回：本轮不再给它发（新请求也赶不上） */
    }
    if (c->ngroups > 0)
        rm_conn_apply(c, slot);         /* 失败收尾留下的账 */
    if (c->conn != NULL && PQstatus(c->conn) == CONNECTION_OK)
        return c;
    if (c->conn != NULL)
        PQfinish(c->conn);
    c->conn = NULL;
    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%d dbname=postgres user=postgres "
             "connect_timeout=%d application_name=pg_raft_repl",
             peers[slot].host, peers[slot].port, pg_raft_peer_connect_timeout_s);
    c->conn = PQconnectdb(conninfo);
    if (PQstatus(c->conn) != CONNECTION_OK)
    {
        PQfinish(c->conn);
        c->conn = NULL;
        peer_mark_result(&peers[slot], false);
        return NULL;
    }
    return c;
}

typedef struct RmGroup
{
    Oid          part;              /* 本地分区 oid */
    int64        gid;
    int64        cur;               /* 已落盘上界（get_partition_flush_lsn） */
    RaftGroupCtx ctx;
    int          majority;
    int64        done_plsn;         /* 本调用里已确认多数派持久的 plsn 上界（失主后不再推进共享游标） */
    /* 本轮 */
    bool         active;
    int64        lo, hi;            /* 本轮新提的 plsn 区间 */
    int64        read_lo;           /* 补发段读字节的起点（= lo 表示没有补发段） */
    int64        cread_hi;          /* 补发段读字节的终点（read_lo..cread_hi，均 < lo） */
    int64        send_upto;         /* 流水线：本轮要追加到的上界（≥ cur，认领后刷新） */
    int64        term0;             /* 流水线：门禁时的任期；等待期间任期变了 / 失主 ⇒ 失败 */
    bool         done;              /* 流水线：已在 term0 内确认覆盖 */
    bool         resend;            /* 流水线：本轮只补发（没有新记录，落后成员按 match 重发） */
    int64        seen_commit;       /* 流水线：上次看到的 commit_index（判"有没有进展"） */
    TimestampTz  progress_at;       /* 流水线：commit_index 最近一次前移（或开始等）的时刻 */
    int64        self_from;         /* 流水线：本轮 leader 自写 raft_log 行的起点（0 = 本轮没写） */
    int64        catchup_from[RAFT_MAX_PEERS];  /* 0 = 不发；否则发给该对端的第一条 index */
    int64        catchup_to[RAFT_MAX_PEERS];    /* 0 = 发到本轮 last；否则只补到这条（分段补发） */
    int64        term;
    int64        first, last;       /* 本轮新条目的 index 区间 */
    int64        leader_commit;
    int          nnew;
    char       **desc;              /* 新条目描述符，按 plsn - lo 下标 */
    char       **hex;               /* 新条目字节，按 plsn - lo 下标 */
    int          nhex;
    char       **chex;              /* 补发段字节，按 plsn - read_lo 下标（plsn < lo） */
    int          nchex;
    int          nsent;             /* 本轮发出了请求的副本数 */
    int          acks;
    bool         failed;
    int64        fail_plsn;
} RmGroup;

static int
rm_group_cmp(const void *a, const void *b)
{
    int64 x = ((const RmGroup *) a)->gid;
    int64 y = ((const RmGroup *) b)->gid;

    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

/* 与 replicate_group_upto 相同：重启后运行期游标为 0 时从环内最后一条 OP_PARWAL 回推 */
static int64
rm_last_data_plsn(RaftGroupCtx *ctx)
{
    int64 last = ctx->g->last_data_plsn;

    if (last == 0)
    {
        int64 p;

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
    return last;
}

/*
 * 落后副本 slot 的补发起点：next_index 落在本轮新条目之前、窗口内、中间全是本任期的
 * OP_PARWAL 且 prev 可取证 ⇒ 返回 next_index，并把 *plsn 置为那条的 partition_lsn；
 * 否则返回 0（交后台追平）。调用方持 log->mutex。
 */
static int64
rm_catchup_start_locked(RaftGroupCtx *ctx, int slot, int64 first_new, int64 term,
                        int64 *plsn)
{
    int64        nx = ctx->log->peer_next_index[slot];
    int64        i;
    RaftLogEntry e;

    if (nx <= 0 || nx >= first_new)
        return 0;
    if (first_new - nx > RM_CATCHUP_WIN)
        return 0;
    if (nx - 1 > 0 && nx - 1 != ctx->log->base_index)
    {
        if (!log_get_entry_locked(ctx, nx - 1, &e))
            return 0;
    }
    /* 环窗口可达上千条：锁下逐条只看槽位（不拷整条） */
    for (i = nx; i < first_new; i++)
    {
        RaftLogEntry *x = log_slot(ctx, i);

        if (x->index != i || x->term != term ||
            strcmp(x->op_type, RAFT_OP_PARWAL) != 0)
            return 0;
        if (i == nx)
            *plsn = entry_partition_lsn(x->payload);
    }
    return nx;
}

/*
 * ⑧ 给副本 slot 发本轮各组的条目：一条多语句请求（各组一条 append_entries_batch，组号升序）。
 * 返回 1 = 已发出；0 = 没有要发的 / 连不上 / 发送失败；-1 = 本后端到它的连接全在途
 * （仅 block=false）。
 */
static int
rm_send_slot(RmGroup *gs, int ng, int slot, bool block, TimestampTz deadline,
             StringInfo sql)
{
    RmConn *c = NULL;
    int     nreq = 0;
    int     i;
    int     q;

    resetStringInfo(sql);
    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];
        int64    st;
        int64    end;
        int64    prev_idx, prev_term = 0;
        int      n, j;
        int64   *idx;
        char   **pl;
        char   **hx;
        RaftLogEntry *ents;         /* 补发段条目在锁下拷出（锁下不许 palloc） */
        bool     ok = true;

        if (!g->active || !peer_in_group(&g->ctx, slot))
            continue;
        st = g->catchup_from[slot];
        if (st <= 0 || st > g->first)
            continue;           /* 落后太多：交后台追平 */
        end = (g->catchup_to[slot] > 0) ? g->catchup_to[slot] : g->last;
        if (end < st)
            continue;           /* 只补发的一轮里本就同步的成员：没东西可发 */
        if (c == NULL)
        {
            bool all_busy;

            c = rm_conn_get(slot, block, deadline, &all_busy);
            if (c == NULL)
                return all_busy ? -1 : 0;
            c->ngroups = 0;
            c->nres = 0;
            c->any_error = false;
        }
        n = (int) (end - st + 1);
        idx = (int64 *) palloc(sizeof(int64) * n);
        pl = (char **) palloc(sizeof(char *) * n);
        hx = (char **) palloc(sizeof(char *) * n);
        ents = (st < g->first)
            ? (RaftLogEntry *) palloc(sizeof(RaftLogEntry) * (g->first - st)) : NULL;
        prev_idx = st - 1;
        SpinLockAcquire(&g->ctx.log->mutex);
        if (prev_idx > 0)
        {
            RaftLogEntry *pe = log_slot(&g->ctx, prev_idx);

            if (pe->index == prev_idx)
                prev_term = pe->term;
            else if (prev_idx == g->ctx.log->base_index)
                prev_term = g->ctx.log->base_term;
            else
                ok = false;
        }
        for (j = 0; ok && j < n; j++)
        {
            int64        ix = st + j;
            int64        plsn;
            RaftLogEntry e;

            idx[j] = ix;
            if (ix >= g->first)
            {
                pl[j] = g->desc[ix - g->first];
                plsn = g->lo + (ix - g->first);
            }
            else
            {
                /* 补发段整批按本任期发出：环里那条必须仍是本任期的（④ 之后环可能被新 leader 改写） */
                if (!log_get_entry_locked(&g->ctx, ix, &e) || e.term != g->term)
                {
                    ok = false;
                    break;
                }
                ents[j] = e;
                pl[j] = ents[j].payload;
                plsn = entry_partition_lsn(e.payload);
            }
            if (plsn >= g->lo && plsn - g->lo < g->nhex)
                hx[j] = g->hex[plsn - g->lo];
            else if (plsn >= g->read_lo && plsn < g->lo &&
                     plsn - g->read_lo < g->nchex)
                hx[j] = g->chex[plsn - g->read_lo];
            else
                ok = false;     /* 补发段读不全：本轮不发它 */
        }
        if (ok && g->ctx.log->peer_next_index[slot] >= st &&
            g->ctx.log->peer_next_index[slot] <= end)
            g->ctx.log->peer_next_index[slot] = end + 1;       /* 乐观推进（流水线） */
        c->groups[nreq].gen = g->ctx.log->trunc_gen;
        SpinLockRelease(&g->ctx.log->mutex);
        if (!ok)
            continue;
        gc_build_batch_sql(sql, &g->ctx, g->term, prev_idx, prev_term,
                           g->leader_commit, n, idx, g->term,
                           (const char *const *) pl, (const char *const *) hx);
        appendStringInfoString(sql, ";");
        c->groups[nreq].gid = g->gid;
        c->groups[nreq].first = st;
        c->groups[nreq].last = end;
        c->groups[nreq].term = g->term;
        c->groups[nreq].gslot = i;
        nreq++;
    }
    if (c == NULL || nreq == 0)
        return 0;
    c->ngroups = nreq;
    if (PQsendQuery(c->conn, sql->data) != 1)
    {
        rm_conn_fail(c, slot);
        rm_conn_apply(c, slot);         /* 乐观推进的 next_index 退回 */
        return 0;
    }
    c->busy = true;
    c->current = true;
    c->sent_at = GetCurrentTimestamp();
    for (q = 0; q < nreq; q++)
        if (c->groups[q].last >= gs[c->groups[q].gslot].last)
            gs[c->groups[q].gslot].nsent++;
    return 1;
}

/*
 * 2026-10-16：多组一次性认领（全有或全无）。本后端对其中任何一组已有重入深度时不走这里
 * （返回 false，调用方按组号顺序逐个认领）。拿到返回 true，并按 replicate_claim 的约定记好
 * 自持深度。
 */
static bool
rm_try_claim_all(RmGroup *gs, int ng)
{
    int     i;
    bool    all_free = true;

    for (i = 0; i < ng; i++)
        if (self_claim_depth(gs[i].ctx.group_id, false) != NULL)
            return false;
    SpinLockAcquire(&RaftGroups->mutex);
    for (i = 0; i < ng && all_free; i++)
        if (gs[i].ctx.g->replicate_in_progress && gs[i].ctx.g->replicate_pid != MyProcPid)
            all_free = false;
    if (all_free)
    {
        TimestampTz now = GetCurrentTimestamp();

        for (i = 0; i < ng; i++)
        {
            gs[i].ctx.g->replicate_in_progress = true;
            gs[i].ctx.g->replicate_pid = MyProcPid;
            gs[i].ctx.g->replicate_purpose = RAFT_CLAIM_COMMIT_MANY;
            gs[i].ctx.g->replicate_since = now;
        }
    }
    SpinLockRelease(&RaftGroups->mutex);
    if (!all_free)
        return false;
    for (i = 0; i < ng; i++)
    {
        int *depth = self_claim_depth(gs[i].ctx.group_id, true);

        if (depth != NULL)
            *depth = 1;
    }
    return true;
}

/* 本次要送的记录是否已全部被别人那一轮送到多数派（组提交：那就不用认领了） */
static bool
rm_all_covered(RmGroup *gs, int ng)
{
    int i;

    for (i = 0; i < ng; i++)
        if (Max(rm_last_data_plsn(&gs[i].ctx), gs[i].done_plsn) < gs[i].cur)
            return false;
    return true;
}

/* ================================================================== */
/* 2026-10-17：流水线复制（pg_raft.replicate_pipeline）                  */
/* ================================================================== */

/*
 * 一轮一轮模式（上面的 pg_raft_partwal_replicate_many 主体）在整轮复制期间——包括等对端
 * 落盘回 ack 的那一两百毫秒——持着组的复制认领位，同一组同一时刻只能有一轮在途；一笔
 * 事务碰 4–5 个组，组集合两两重叠，实测每"车道"每秒只走几笔事务（三副本 TPC-C ~700 tpmC，
 * 同机 Citus + 流复制 10 万+）。
 *
 * 这里改成同步复制的老办法：
 *   · 发送：认领位只罩住"刷新上界 → 读记录 → 追加进环 → 写 raft_log 行 → 发出请求"这一段，
 *     发完即放（不等回包）。下一个人马上能发下一轮 —— 同组多轮在途，follower 按 prev
 *     顺序收（rm_wait_predecessor）。刷新上界让这一轮把排队者的记录一并带走（组提交）。
 *   · 等待：本事务只看"组的提交点（多数派）是否已覆盖本事务的记录"（rm_committed_plsn ≥ cur）。
 *     谁收到回包谁推进提交点（Raft 原规则 compute_new_commit_index：按各成员 match_index、
 *     只提交本任期条目），所有等待者共用。
 *   · 自己发出的请求自己收：连接按后端私有，回包只能由发出者处理；发出者自己的记录就在
 *     那一轮里，它本来就要等，不会撂下别人。
 *   · 失败：不再"丢未提交尾巴"（尾巴后面可能已有别人的在途轮）。等到超时或本节点失主仍没
 *     覆盖 ⇒ ERROR，事务中止 —— 那几条条目若之后仍被提交，属于原设计"标记那一轮失败"
 *     同一类（中止路径的 ABORT 标记已落盘、随下一轮送出）。
 */
#define RM_PIPE_WAIT_MS 30000

/* 环尾最后一条数据条目的 plsn（= 已追加的上界）；环里没有数据条目时退回运行期游标 */
static int64
rm_appended_plsn(RaftGroupCtx *ctx)
{
    int64 p;
    int64 last = 0;

    SpinLockAcquire(&ctx->log->mutex);
    for (p = ctx->log->last_log_index; p > 0 &&
         p > ctx->log->last_log_index - RAFT_LOG_CAPACITY; p--)
    {
        RaftLogEntry *x = log_slot(ctx, p);

        if (x->index != p)
            break;
        if (strcmp(x->op_type, RAFT_OP_PARWAL) == 0)
        {
            last = entry_partition_lsn(x->payload);
            break;
        }
    }
    SpinLockRelease(&ctx->log->mutex);
    if (last == 0)
        last = ctx->g->last_data_plsn;
    return last;
}

/* 已提交（多数派持久）的最大数据 plsn：从 commit_index 往回找最近一条数据条目，按 commit_index 缓存 */
static int64
rm_committed_plsn(RaftGroupCtx *ctx)
{
    int64 ci;
    int64 p;
    int64 v = 0;
    int64 floor_idx;

    SpinLockAcquire(&ctx->log->mutex);
    ci = ctx->log->commit_index;
    if (ci == ctx->g->committed_plsn_idx)
    {
        v = ctx->g->committed_plsn;
        SpinLockRelease(&ctx->log->mutex);
        return v;
    }
    floor_idx = Max(ci - RAFT_LOG_CAPACITY, 0);
    if (ctx->g->committed_plsn_idx < ci && ctx->g->committed_plsn_idx > floor_idx)
        floor_idx = ctx->g->committed_plsn_idx;
    for (p = ci; p > floor_idx; p--)
    {
        RaftLogEntry *x = log_slot(ctx, p);

        if (x->index != p)
            break;
        if (strcmp(x->op_type, RAFT_OP_PARWAL) == 0)
        {
            v = entry_partition_lsn(x->payload);
            break;
        }
    }
    if (v > ctx->g->committed_plsn)
        ctx->g->committed_plsn = v;
    ctx->g->committed_plsn_idx = ci;
    v = ctx->g->committed_plsn;
    SpinLockRelease(&ctx->log->mutex);
    /* last_data_plsn 仍是"已复制（多数派）"游标（老路径 / DTX 决议路径按它取下界），只随提交前移 */
    if (v > ctx->g->last_data_plsn)
        ctx->g->last_data_plsn = v;
    return v;
}

/* 按各成员 match_index 推进提交点（仍是本任期 leader 时）；推进了就标记 / apply / hardstate */
static void
rm_pipe_advance(RaftGroupCtx *ctx)
{
    bool    leader;
    int64   term;
    int64   nc;
    bool    advanced = false;

    SpinLockAcquire(&ctx->cons->mutex);
    leader = (ctx->cons->state == RAFT_LEADER);
    term = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);
    if (!leader)
        return;
    SpinLockAcquire(&ctx->log->mutex);
    nc = compute_new_commit_index(ctx, term);
    if (nc > ctx->log->commit_index)
    {
        ctx->log->commit_index = nc;
        advanced = true;
    }
    SpinLockRelease(&ctx->log->mutex);
    if (!advanced)
        return;
    persist_hard_state_unlocked(ctx);
    mark_log_committed_sql(ctx, nc);
    group_apply_pending(ctx);
}

/*
 * 收本后端全部连接上已到的回包：落进 match / next_index，再对涉及的组推进提交点。
 * wait_ms > 0 且有在途请求时先 poll 等最多这么久。
 */
static void
rm_pipe_harvest(int wait_ms, bool advance)
{
    struct pollfd pfds[RAFT_MAX_PEERS * RM_CONNS];
    int         npfd = 0;
    int         slot, k, q;
    TimestampTz now;

    if (wait_ms > 0)
    {
        for (slot = 0; slot < n_peers; slot++)
            for (k = 0; k < RM_CONNS; k++)
            {
                RmConn *c = &rm_conns[slot][k];

                if (c->busy && c->conn != NULL && PQsocket(c->conn) >= 0)
                {
                    pfds[npfd].fd = PQsocket(c->conn);
                    pfds[npfd].events = POLLIN;
                    pfds[npfd].revents = 0;
                    npfd++;
                }
            }
        if (npfd > 0)
            (void) poll(pfds, npfd, wait_ms);
        else
            pg_usleep((long) wait_ms * 1000L);
    }

    now = GetCurrentTimestamp();
    for (slot = 0; slot < n_peers; slot++)
        for (k = 0; k < RM_CONNS; k++)
        {
            RmConn *c = &rm_conns[slot][k];
            int64   gids[RM_MAX_GROUPS];
            int     ngid = 0;

            if (c->busy)
            {
                if (!rm_conn_poll(c, slot, false, 0))
                {
                    if (!TimestampDifferenceExceeds(c->sent_at, now, RAFT_RPC_EXEC_TIMEOUT_MS * 4))
                        continue;
                    elog(WARNING, "pg_raft: 流水线复制到节点 %d 的请求 %d s 未回，判失败并重连",
                         peers[slot].node_id, RAFT_RPC_EXEC_TIMEOUT_MS * 4 / 1000);
                    rm_conn_fail(c, slot);
                }
            }
            if (c->ngroups == 0)
                continue;
            for (q = 0; q < c->ngroups && ngid < RM_MAX_GROUPS; q++)
                gids[ngid++] = c->groups[q].gid;
            rm_conn_apply(c, slot);
            for (q = 0; q < ngid && advance; q++)
            {
                RaftGroupCtx gctx;

                if (raft_group_ctx(gids[q], &gctx))
                    rm_pipe_advance(&gctx);
            }
        }
}

/* ---- leader 自写 raft_log 行的本地独立连接（每后端一条） ---- */
static PGconn *rm_self_conn = NULL;
static bool    rm_self_busy = false;
static bool    rm_self_ok = false;

static bool rm_self_wait(void);

static bool
rm_self_send(const char *sqltext)
{
    if (rm_self_busy)
        (void) rm_self_wait();
    if (rm_self_conn != NULL && PQstatus(rm_self_conn) != CONNECTION_OK)
    {
        PQfinish(rm_self_conn);
        rm_self_conn = NULL;
    }
    if (rm_self_conn == NULL)
    {
        char conninfo[256];

        snprintf(conninfo, sizeof(conninfo),
                 "host=127.0.0.1 port=%d dbname=postgres user=postgres "
                 "connect_timeout=%d application_name=pg_raft_self",
                 PostPortNumber, pg_raft_peer_connect_timeout_s);
        rm_self_conn = PQconnectdb(conninfo);
        if (PQstatus(rm_self_conn) != CONNECTION_OK)
        {
            elog(WARNING, "pg_raft: 流水线 leader 自写连接建不起来：%s", PQerrorMessage(rm_self_conn));
            PQfinish(rm_self_conn);
            rm_self_conn = NULL;
            return false;
        }
    }
    if (PQsendQuery(rm_self_conn, sqltext) != 1)
    {
        PQfinish(rm_self_conn);
        rm_self_conn = NULL;
        return false;
    }
    rm_self_busy = true;
    rm_self_ok = true;
    return true;
}

/* 等自写那条语句提交完（最多 30 s）。返回是否成功 */
static bool
rm_self_wait(void)
{
    TimestampTz start = GetCurrentTimestamp();

    while (rm_self_busy)
    {
        struct pollfd pfd;

        if (!PQconsumeInput(rm_self_conn))
        {
            rm_self_ok = false;
            rm_self_busy = false;
            PQfinish(rm_self_conn);
            rm_self_conn = NULL;
            break;
        }
        while (rm_self_busy && !PQisBusy(rm_self_conn))
        {
            PGresult *r = PQgetResult(rm_self_conn);

            if (r == NULL)
            {
                rm_self_busy = false;
                break;
            }
            if (PQresultStatus(r) != PGRES_COMMAND_OK)
            {
                static TimestampTz last_warn = 0;

                rm_self_ok = false;
                if (last_warn == 0 ||
                    TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 10000))
                {
                    last_warn = GetCurrentTimestamp();
                    elog(WARNING, "pg_raft: 流水线 leader 自写 raft_log 行失败：%s",
                         PQresultErrorMessage(r));
                }
            }
            PQclear(r);
        }
        if (!rm_self_busy)
            break;
        if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), 30000))
        {
            elog(WARNING, "pg_raft: 流水线 leader 自写 raft_log 行 30 s 未完成，断开重连");
            rm_self_ok = false;
            rm_self_busy = false;
            PQfinish(rm_self_conn);
            rm_self_conn = NULL;
            break;
        }
        pfd.fd = PQsocket(rm_self_conn);
        pfd.events = POLLIN;
        pfd.revents = 0;
        (void) poll(&pfd, 1, 50);
        CHECK_FOR_INTERRUPTS();
    }
    return rm_self_ok;
}

/* 自写失败：本轮各组从本轮第一条起记为洞（自投票不越过它，下一轮从洞补写） */
static void
rm_self_mark_hole(RmGroup *gs, int ng)
{
    int i;

    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];
        int64    from;

        if (!g->active || g->nnew <= 0)
            continue;
        from = g->self_from > 0 ? g->self_from : g->first;
        SpinLockAcquire(&g->ctx.log->mutex);
        if (g->ctx.log->leader_self_hole == 0 || from < g->ctx.log->leader_self_hole)
            g->ctx.log->leader_self_hole = from;
        SpinLockRelease(&g->ctx.log->mutex);
    }
}

/*
 * 发一轮：对 claimed[] 标出的组（调用方已持认领位）刷新上界 → 读 → 追加 → 写日志行 → 发出。
 * 不等回包。返回发出了请求的组数。
 */
static int
rm_pipe_send(RmGroup *gs, int ng, const bool *claimed, MemoryContext rcxt, StringInfo sql,
             int *fail_out)
{
    int             i, k, slot;
    bool            spi_owned;
    MemoryContext   spicxt;
    int             spi_rc;
    int             nactive = 0;
    int             maxn = 0;
    int             timeout_ms;
    TimestampTz     deadline;
    bool            busy_skip[RAFT_MAX_PEERS];
    TimestampTz     t0 = GetCurrentTimestamp();
    bool            self_sent = false;

    MemoryContextReset(rcxt);
    MemoryContextSwitchTo(rcxt);

    /* 刷新上界：把排队者已落盘的记录一并带走（组提交） */
    if (raft_persist_spi_begin(&spi_owned))
    {
        resetStringInfo(sql);
        appendStringInfoString(sql, "SELECT u.o::int, partdist.get_partition_flush_lsn(u.p) FROM unnest(ARRAY[");
        k = 0;
        for (i = 0; i < ng; i++)
            if (claimed[i])
                appendStringInfo(sql, "%s%u", k++ ? "," : "", (unsigned) gs[i].part);
        appendStringInfoString(sql, "]::oid[], ARRAY[");
        k = 0;
        for (i = 0; i < ng; i++)
            if (claimed[i])
                appendStringInfo(sql, "%s%d", k++ ? "," : "", i + 1);
        appendStringInfoString(sql, "]::int[]) u(p, o)");
        if (SPI_execute(sql->data, true, 0) == SPI_OK_SELECT)
        {
            uint64 r;

            for (r = 0; r < SPI_processed; r++)
            {
                bool    n1, n2;
                Datum   d1 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 1, &n1);
                Datum   d2 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 2, &n2);
                int     o;

                if (n1 || n2)
                    continue;
                o = DatumGetInt32(d1) - 1;
                if (o >= 0 && o < ng && DatumGetInt64(d2) > gs[o].send_upto)
                    gs[o].send_upto = DatumGetInt64(d2);
            }
        }
        raft_persist_spi_end(spi_owned);
        MemoryContextSwitchTo(rcxt);
    }

    /* ④ 本轮区间与落后副本的补发起点（同一轮一轮模式） */
    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];
        int64    last;
        bool     is_leader;
        int64    first_new, kmax;
        int64    big_st[RAFT_MAX_PEERS];
        int64    big_plsn[RAFT_MAX_PEERS];

        g->active = false;
        g->nsent = 0;
        g->nnew = 0;
        if (!claimed[i])
            continue;
        last = rm_appended_plsn(&g->ctx);
        if (last >= g->send_upto && !g->resend)
            continue;
        if (last < g->send_upto && !wait_for_log_room(&g->ctx))
            continue;
        SpinLockAcquire(&g->ctx.cons->mutex);
        is_leader = (g->ctx.cons->state == RAFT_LEADER);
        g->term = g->ctx.cons->current_term;
        SpinLockRelease(&g->ctx.cons->mutex);
        if (!is_leader)
            continue;
        g->lo = last + 1;
        g->hi = (last < g->send_upto)
            ? Min(g->send_upto, g->lo + pg_raft_replicate_batch_max - 1)
            : last;                 /* 只补发：没有新记录（hi < lo，新记录区间为空） */
        g->read_lo = g->lo;
        g->cread_hi = g->lo - 1;
        g->active = true;
        nactive++;

        SpinLockAcquire(&g->ctx.log->mutex);
        if (!g->ctx.log->repl_inited)
        {
            int64 nx = g->ctx.log->last_log_index + 1;
            int   j;

            for (j = 0; j < RAFT_MAX_PEERS; j++)
            {
                g->ctx.log->peer_next_index[j] = nx;
                g->ctx.log->peer_match_index[j] = 0;
            }
            g->ctx.log->repl_inited = true;
            g->ctx.log->leader_durable_index = g->ctx.log->last_log_index;   /* 当选时日志里已有的条目视为已落盘 */
            g->ctx.log->leader_self_hole = 0;
        }
        /*
         * 只补发（等了一阵提交点不动）：落后成员的 next_index 退回 match + 1 重发 —— 那一段可能
         * 在途丢了（对端拒收 / 连接失败 / 乐观推进后请求没发成）。重复送到的条目对端按"已持有"处理。
         */
        if (g->resend)
        {
            for (slot = 0; slot < n_peers; slot++)
                if (peer_in_group(&g->ctx, slot) &&
                    g->ctx.log->peer_match_index[slot] < g->ctx.log->last_log_index &&
                    g->ctx.log->peer_next_index[slot] > g->ctx.log->peer_match_index[slot] + 1)
                    g->ctx.log->peer_next_index[slot] = g->ctx.log->peer_match_index[slot] + 1;
        }
        first_new = g->ctx.log->last_log_index + 1;
        kmax = (int64) pg_raft_replicate_batch_max * 2;
        for (slot = 0; slot < n_peers; slot++)
        {
            int64 plsn = 0;
            int64 nx;
            int64 st;

            g->catchup_from[slot] = 0;
            g->catchup_to[slot] = 0;
            big_st[slot] = 0;
            big_plsn[slot] = 0;
            if (!peer_in_group(&g->ctx, slot))
                continue;
            nx = g->ctx.log->peer_next_index[slot];
            if (nx >= first_new)
            {
                g->catchup_from[slot] = first_new;
                continue;
            }
            st = rm_catchup_start_locked(&g->ctx, slot, first_new, g->term, &plsn);
            if (st <= 0 || plsn <= 0 || plsn >= g->lo)
                continue;
            if (g->hi - plsn + 1 <= kmax)
            {
                g->catchup_from[slot] = st;
                if (plsn < g->read_lo)
                    g->read_lo = plsn;
            }
            else
            {
                big_st[slot] = st;
                big_plsn[slot] = plsn;
            }
        }
        if (g->read_lo == g->lo)
        {
            int64 base = 0;

            for (slot = 0; slot < n_peers; slot++)
            {
                if (big_st[slot] <= 0 || (base != 0 && big_plsn[slot] != base))
                    continue;
                base = big_plsn[slot];
                g->catchup_from[slot] = big_st[slot];
                g->catchup_to[slot] = Min(big_st[slot] + kmax - 1, first_new - 1);
                g->read_lo = base;
                g->cread_hi = Min(base + kmax - 1, g->lo - 1);
            }
        }
        else
            g->cread_hi = g->lo - 1;
        SpinLockRelease(&g->ctx.log->mutex);
    }
    if (nactive == 0)
        return 0;

    /* ⑤ 一次 SPI 读出各组 [read_lo, cread_hi] ∪ [lo, hi] 的描述与字节 */
    if (!raft_persist_spi_begin(&spi_owned))
        ereport(ERROR, (errmsg("pg_raft: 流水线复制拿不到 SPI")));
    spicxt = MemoryContextSwitchTo(rcxt);
    resetStringInfo(sql);
    appendStringInfoString(sql,
                           "SELECT g.k, s, r.orig_lsn::text, r.rmid, r.info, r.gxid, "
                           "       length(r.data), r.flags, encode(r.data, 'hex') "
                           "FROM (VALUES ");
    k = 0;
    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];

        if (!g->active)
            continue;
        if (g->read_lo < g->lo)
            appendStringInfo(sql, "%s(%d, %u::oid, %lld::bigint, %lld::bigint)",
                             k++ ? "," : "", i, (unsigned) g->part,
                             (long long) g->read_lo, (long long) g->cread_hi);
        appendStringInfo(sql, "%s(%d, %u::oid, %lld::bigint, %lld::bigint)",
                         k++ ? "," : "", i, (unsigned) g->part,
                         (long long) g->lo, (long long) g->hi);
        g->nhex = 0;
        g->nchex = 0;
        g->nnew = 0;
        g->hex = (char **) palloc0(sizeof(char *) * Max(g->hi - g->lo + 1, 1));
        g->desc = (char **) palloc0(sizeof(char *) * Max(g->hi - g->lo + 1, 1));
        g->chex = (g->lo > g->read_lo)
            ? (char **) palloc0(sizeof(char *) * (g->cread_hi - g->read_lo + 1)) : NULL;
    }
    appendStringInfoString(sql,
                           ") g(k, o, lo, hi) "
                           "CROSS JOIN LATERAL generate_series(g.lo, g.hi) s "
                           "LEFT JOIN LATERAL partdist.partwal_read_record(g.o, s) r ON true "
                           "ORDER BY g.k, s");
    spi_rc = SPI_execute(sql->data, true, 0);
    MemoryContextSwitchTo(rcxt);
    if (spi_rc == SPI_OK_SELECT)
    {
        uint64      r;
        TupleDesc   td = SPI_tuptable->tupdesc;

        for (r = 0; r < SPI_processed; r++)
        {
            HeapTuple   tup = SPI_tuptable->vals[r];
            bool        isnull;
            int         gi = DatumGetInt32(SPI_getbinval(tup, td, 1, &isnull));
            int64       plsn = DatumGetInt64(SPI_getbinval(tup, td, 2, &isnull));
            RmGroup    *g = &gs[gi];
            Datum       d;
            char       *orig;
            int         rmid, info, flags;
            int64       gxid, nbytes;
            StringInfoData pl;

            if (plsn < g->lo ? (plsn != g->read_lo + g->nchex || plsn > g->cread_hi)
                             : (plsn != g->lo + g->nhex))
                continue;
            d = SPI_getbinval(tup, td, 3, &isnull);
            if (isnull)
                continue;
            orig = TextDatumGetCString(d);
            d = SPI_getbinval(tup, td, 4, &isnull);  rmid = isnull ? 0 : DatumGetInt32(d);
            d = SPI_getbinval(tup, td, 5, &isnull);  info = isnull ? 0 : DatumGetInt32(d);
            d = SPI_getbinval(tup, td, 6, &isnull);  gxid = isnull ? 0 : DatumGetInt64(d);
            d = SPI_getbinval(tup, td, 7, &isnull);  nbytes = isnull ? 0 : DatumGetInt32(d);
            d = SPI_getbinval(tup, td, 8, &isnull);  flags = isnull ? PARTWAL_FLAG_DATA : DatumGetInt32(d);
            d = SPI_getbinval(tup, td, 9, &isnull);
            if (isnull)
                continue;
            if (plsn < g->lo)
            {
                g->chex[g->nchex++] = TextDatumGetCString(d);
                continue;
            }
            g->hex[g->nhex++] = TextDatumGetCString(d);
            initStringInfo(&pl);
            appendStringInfo(&pl,
                             "{\"partition_lsn\":%lld,\"orig_lsn\":\"%s\",\"rmid\":%d,"
                             "\"info\":%d,\"flags\":%d,\"gxid\":%lld,\"nbytes\":%lld}",
                             (long long) plsn, orig, rmid, info, flags,
                             (long long) gxid, (long long) nbytes);
            g->desc[g->nnew++] = pl.data;
        }
    }
    MemoryContextSwitchTo(spicxt);
    raft_persist_spi_end(spi_owned);

    /* ⑥ 追加进环；推进"已追加"游标 */
    raft_append_pxid = GetTopTransactionId();
    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];
        int      j;
        int      room;
        bool     still;

        if (!g->active)
            continue;
        SpinLockAcquire(&g->ctx.cons->mutex);
        still = (g->ctx.cons->state == RAFT_LEADER && g->ctx.cons->current_term == g->term);
        SpinLockRelease(&g->ctx.cons->mutex);
        if (!still)
        {
            g->active = false;
            continue;
        }
        if (g->hi < g->lo)
        {
            /* 只补发：不追加新条目，补发段照常按 catchup_from 发 */
            SpinLockAcquire(&g->ctx.log->mutex);
            g->first = g->ctx.log->last_log_index + 1;
            g->last = g->ctx.log->last_log_index;
            g->leader_commit = g->ctx.log->commit_index;
            SpinLockRelease(&g->ctx.log->mutex);
            continue;
        }
        if (g->nnew == 0)
        {
            g->active = false;
            elog(WARNING, "pg_raft: 组 %lld 读不到本地 parwal 记录 plsn=%lld（local_oid=%u）",
                 (long long) g->gid, (long long) g->lo, g->part);
            if (fail_out != NULL)
                *fail_out = i;          /* 与一轮一轮模式相同：立即失败，不原地空转 */
            continue;
        }
        SpinLockAcquire(&g->ctx.log->mutex);
        room = (int) (RAFT_LOG_CAPACITY - 1 -
                      (g->ctx.log->last_log_index - g->ctx.log->last_applied));
        if (g->nnew > room)
            g->nnew = room;
        if (g->ctx.log->last_log_index > 0)
        {
            RaftLogEntry *te = log_slot(&g->ctx, g->ctx.log->last_log_index);

            if (te->index == g->ctx.log->last_log_index && te->term > g->term)
                g->nnew = 0;
        }
        g->first = g->ctx.log->last_log_index + 1;
        for (j = 0; j < g->nnew; j++)
            if (log_append_locked(&g->ctx, g->term, RAFT_OP_PARWAL, g->desc[j]) <= 0)
                break;
        g->nnew = j;
        g->last = g->first + g->nnew - 1;
        g->leader_commit = g->ctx.log->commit_index;
        SpinLockRelease(&g->ctx.log->mutex);
        if (g->nnew <= 0)
        {
            g->active = false;
            continue;
        }
        if (g->nnew > maxn)
            maxn = g->nnew;
    }
    raft_append_pxid = InvalidTransactionId;

    /*
     * ⑦ leader 的 raft_log 行：走独立的本地连接、自己提交（2026-10-17）。
     * 以前写在用户事务里 —— 流水线下这笔事务可能还要等别的组几十秒、也可能中止，期间别人的
     * 提交已经把这些条目算上了 leader 一票；leader 一崩，行没了，那一票就是假的。还会与其它
     * 在途事务互等行锁。现在与下面发给副本的请求并行写、在放认领位之前等它提交，成功了才把
     * leader_durable_index（自投票上限）推到本轮末尾；失败记"洞"，下一轮从洞开始补写。
     */
    {
        bool    ready = false;
        bool    first_row = true;

        if (raft_persist_spi_begin(&spi_owned))
        {
            ready = raft_log_table_ready();
            raft_persist_spi_end(spi_owned);
            MemoryContextSwitchTo(rcxt);
        }
        resetStringInfo(sql);
        appendStringInfoString(sql,
                               "INSERT INTO partdist.raft_log "
                               "(group_id, log_index, term, op_type, payload, committed) VALUES ");
        for (i = 0; ready && i < ng; i++)
        {
            RmGroup *g = &gs[i];
            int64    hole;
            int64    ix;

            g->self_from = 0;
            if (!g->active || g->nnew <= 0)
                continue;
            SpinLockAcquire(&g->ctx.log->mutex);
            hole = g->ctx.log->leader_self_hole;
            SpinLockRelease(&g->ctx.log->mutex);
            g->self_from = (hole > 0 && hole < g->first) ? hole : g->first;
            for (ix = g->self_from; ix <= g->last; ix++)
            {
                RaftLogEntry e;
                bool         got = true;
                const char  *pl;
                int64        et;

                if (ix >= g->first)
                {
                    pl = g->desc[ix - g->first];
                    et = g->term;
                }
                else
                {
                    SpinLockAcquire(&g->ctx.log->mutex);
                    got = log_get_entry_locked(&g->ctx, ix, &e);
                    SpinLockRelease(&g->ctx.log->mutex);
                    if (!got)
                        continue;
                    pl = e.payload;
                    et = e.term;
                }
                appendStringInfo(sql, "%s(%lld, %lld, %lld, %s, %s::jsonb, %s)",
                                 first_row ? "" : ", ",
                                 (long long) g->gid, (long long) ix,
                                 (long long) et, quote_literal_cstr(got && ix < g->first ? e.op_type : RAFT_OP_PARWAL),
                                 quote_literal_cstr(pl),
                                 g->majority == 1 ? "true" : "false");
                first_row = false;
            }
        }
        appendStringInfoString(sql,
                               " ON CONFLICT (group_id, log_index) DO UPDATE SET "
                               "term = EXCLUDED.term, op_type = EXCLUDED.op_type, "
                               "payload = EXCLUDED.payload, "
                               "committed = partdist.raft_log.committed OR EXCLUDED.committed");
        self_sent = (!first_row && rm_self_send(sql->data));
        if (!first_row && !self_sent)
            rm_self_mark_hole(gs, ng);
    }

    /* ⑧ 发出（不等回包）。到某副本的连接全在途且有组因此一个都没发出去时，等一条空出来再发 */
    timeout_ms = Min(RAFT_RPC_EXEC_TIMEOUT_MS + 300 * maxn, 30000);
    deadline = GetCurrentTimestamp() + (int64) timeout_ms * 1000;
    for (slot = 0; slot < n_peers; slot++)
    {
        busy_skip[slot] = false;
        if (peers[slot].node_id == pg_raft_node_id || peer_in_backoff(&peers[slot]))
            continue;
        busy_skip[slot] = (rm_send_slot(gs, ng, slot, false, deadline, sql) < 0);
    }
    for (slot = 0; slot < n_peers; slot++)
    {
        bool need = false;

        if (!busy_skip[slot])
            continue;
        for (i = 0; i < ng && !need; i++)
            if (gs[i].active && peer_in_group(&gs[i].ctx, slot) &&
                gs[i].nsent + 1 < gs[i].majority)
                need = true;
        if (need)
            (void) rm_send_slot(gs, ng, slot, true, deadline, sql);
    }
    /* leader 自写的 raft_log 行：放认领位之前等它提交（同组的自写因此按轮次顺序落定） */
    if (self_sent)
    {
        if (rm_self_wait())
        {
            for (i = 0; i < ng; i++)
            {
                RmGroup *g = &gs[i];

                if (!g->active || g->nnew <= 0 || g->self_from <= 0)
                    continue;
                SpinLockAcquire(&g->ctx.log->mutex);
                if (g->ctx.log->leader_self_hole > 0 && g->ctx.log->leader_self_hole >= g->self_from)
                    g->ctx.log->leader_self_hole = 0;
                if (g->last > g->ctx.log->leader_durable_index)
                    g->ctx.log->leader_durable_index = g->last;
                SpinLockRelease(&g->ctx.log->mutex);
            }
        }
        else
            rm_self_mark_hole(gs, ng);
    }

    /* 发出的请求不属于"本轮计票"（流水线下回包统一由 rm_pipe_harvest 收） */
    for (slot = 0; slot < n_peers; slot++)
        for (k = 0; k < RM_CONNS; k++)
            rm_conns[slot][k].current = false;

    k = 0;
    for (i = 0; i < ng; i++)
        if (gs[i].active)
        {
            gs[i].ctx.g->last_rm_round = GetCurrentTimestamp();
            k++;
        }
    if (TimestampDifferenceExceeds(t0, GetCurrentTimestamp(), 200))
    {
        static TimestampTz last_log = 0;

        if (last_log == 0 || TimestampDifferenceExceeds(last_log, GetCurrentTimestamp(), 2000))
        {
            last_log = GetCurrentTimestamp();
            elog(LOG, "pg_raft: [pipe-send] %d 组 共 %ld ms", k,
                 (long) ((GetCurrentTimestamp() - t0) / 1000));
        }
    }
    return k;
}

static void
pg_raft_partwal_replicate_pipe(const Oid *parts, int nparts, bool *failed_out)
{
    RmGroup        *gs;
    int             ng = 0;
    int             i, k;
    StringInfoData  sql;
    bool            spi_owned;
    MemoryContext   rmcxt;
    MemoryContext   rcxt;
    MemoryContext   oldcxt;
    bool           *claimed;
    volatile bool   normal_exit = false;
    TimestampTz     start = GetCurrentTimestamp();
    TimestampTz     t_log = start;
    const char     *fail_why = NULL;
    int             fail_i = -1;

    rmcxt = AllocSetContextCreate(CurrentMemoryContext, "pg_raft replicate_pipe",
                                  ALLOCSET_DEFAULT_SIZES);
    rcxt = AllocSetContextCreate(rmcxt, "pg_raft replicate_pipe round",
                                 ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(rmcxt);
    gs = (RmGroup *) palloc0(sizeof(RmGroup) * nparts);
    claimed = (bool *) palloc0(sizeof(bool) * nparts);

    /* ① 一次 SPI：组号与已落盘上界（同一轮一轮模式） */
    initStringInfo(&sql);
    if (!raft_persist_spi_begin(&spi_owned))
    {
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(rmcxt);
        return;
    }
    appendStringInfoString(&sql,
                           "SELECT u.p, partdist.global_id_for_partition(u.p)::bigint, "
                           "       partdist.get_partition_flush_lsn(u.p) "
                           "FROM unnest(ARRAY[");
    for (i = 0; i < nparts; i++)
        appendStringInfo(&sql, "%s%u", i ? "," : "", (unsigned) parts[i]);
    appendStringInfoString(&sql, "]::oid[]) WITH ORDINALITY u(p, o) ORDER BY u.o");
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT)
    {
        uint64 r;

        for (r = 0; r < SPI_processed; r++)
        {
            bool    n1, n2, n3;
            Datum   d1 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 1, &n1);
            Datum   d2 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 2, &n2);
            Datum   d3 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 3, &n3);

            if (n1 || n2 || DatumGetInt64(d2) <= 0)
                continue;
            gs[ng].part = DatumGetObjectId(d1);
            gs[ng].gid = DatumGetInt64(d2);
            gs[ng].cur = n3 ? 0 : DatumGetInt64(d3);
            ng++;
        }
    }
    raft_persist_spi_end(spi_owned);
    MemoryContextSwitchTo(rmcxt);

    /* ② 逐组门禁（同一轮一轮模式；被拒的分区标进 failed_out） */
    k = 0;
    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];
        int      state = -1;
        bool     known;

        if (!raft_group_ctx(g->gid, &g->ctx))
            continue;
        known = group_resolve_membership(&g->ctx);
        if (known)
        {
            restore_hard_state_if_needed(&g->ctx);
            restore_persistent_log_if_needed(&g->ctx);
            SpinLockAcquire(&g->ctx.cons->mutex);
            state = g->ctx.cons->state;
            SpinLockRelease(&g->ctx.cons->mutex);
        }
        if ((!known || state != RAFT_LEADER) && failed_out != NULL)
        {
            int p;

            for (p = 0; p < nparts; p++)
                if (parts[p] == g->part)
                    failed_out[p] = true;
        }
        if (!known)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: 分区 %u(组 %lld)的成员集未知，写入被拒",
                            g->part, (long long) g->gid)));
        if (state != RAFT_LEADER)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: 分区 %u(组 %lld)的本地写入被拒：本节点不是该分区组的 leader",
                            g->part, (long long) g->gid),
                     errdetail("分区主副本可能已切换，请经路由层重试。")));
        if (g->cur <= 0)
            continue;
        g->majority = cluster_majority(&g->ctx);
        g->send_upto = g->cur;
        SpinLockAcquire(&g->ctx.cons->mutex);
        g->term0 = g->ctx.cons->current_term;
        SpinLockRelease(&g->ctx.cons->mutex);
        g->progress_at = GetCurrentTimestamp();
        if (k != i)
            gs[k] = *g;
        k++;
    }
    ng = k;
    if (ng == 0)
    {
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(rmcxt);
        return;
    }
    qsort(gs, ng, sizeof(RmGroup), rm_group_cmp);

    PG_TRY();
    {
        in_txn_replication = true;
        for (;;)
        {
            bool    all_done = true;
            int     nclaim = 0;

            /* 收回包、推进提交点 */
            rm_pipe_harvest(0, true);

            /*
             * 覆盖了没有。先确认仍是门禁时那个任期的 leader，再信提交点 —— 失主后本节点的环尾会被
             * 新 leader 截掉、同一批 plsn 换成别人的记录，提交点越过它们不代表本事务的记录在多数派上。
             * 在 term0 内确认过覆盖的组记为 done，之后任期再变也不影响（那时已多数派持久化）。
             */
            for (i = 0; i < ng; i++)
            {
                RmGroup *g = &gs[i];
                bool     ok_term;
                int64    ci;

                if (g->done)
                    continue;
                SpinLockAcquire(&g->ctx.cons->mutex);
                ok_term = (g->ctx.cons->state == RAFT_LEADER &&
                           g->ctx.cons->current_term == g->term0);
                SpinLockRelease(&g->ctx.cons->mutex);
                if (!ok_term)
                {
                    all_done = false;
                    fail_why = "本节点已不是该分区组的 leader（或任期已变）";
                    fail_i = i;
                    continue;
                }
                /* 回包可能是别的后端收的（它只落了 match_index 就走了）：自己按 match_index 推一把 */
                rm_pipe_advance(&g->ctx);
                if (rm_committed_plsn(&g->ctx) >= g->cur)
                {
                    SpinLockAcquire(&g->ctx.cons->mutex);
                    ok_term = (g->ctx.cons->state == RAFT_LEADER &&
                               g->ctx.cons->current_term == g->term0);
                    SpinLockRelease(&g->ctx.cons->mutex);
                    if (ok_term)
                    {
                        g->done = true;
                        continue;
                    }
                }
                all_done = false;
                SpinLockAcquire(&g->ctx.log->mutex);
                ci = g->ctx.log->commit_index;
                SpinLockRelease(&g->ctx.log->mutex);
                if (ci != g->seen_commit)
                {
                    g->seen_commit = ci;
                    g->progress_at = GetCurrentTimestamp();
                }
            }
            if (all_done || fail_why != NULL)
                break;
            if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), RM_PIPE_WAIT_MS))
            {
                fail_why = "等多数派确认超时";
                for (i = 0; i < ng; i++)
                    if (!gs[i].done)
                        fail_i = i;
                break;
            }

            /*
             * 还没追加到 cur 的组：认领位空着就拿下（不等），发一轮，发完即放。
             * 已追加、但提交点 300 ms 没动的组：发一轮"只补发"（那一段可能在途丢了 / 被拒收）。
             */
            for (i = 0; i < ng; i++)
            {
                claimed[i] = false;
                gs[i].resend = false;
                if (gs[i].done)
                    continue;
                if (rm_appended_plsn(&gs[i].ctx) >= gs[i].cur)
                {
                    if (!TimestampDifferenceExceeds(gs[i].progress_at, GetCurrentTimestamp(), 300))
                        continue;
                    gs[i].resend = true;
                }
                if (self_claim_depth(gs[i].ctx.group_id, false) != NULL)
                    continue;
                raft_claim_purpose = RAFT_CLAIM_COMMIT_MANY;
                if (replicate_try_claim(&gs[i].ctx))
                {
                    claimed[i] = true;
                    nclaim++;
                    if (gs[i].resend)
                        gs[i].progress_at = GetCurrentTimestamp();     /* 下一次补发至少再隔 300 ms */
                }
            }
            if (nclaim > 0)
            {
                int send_fail = -1;

                PG_TRY(2);
                {
                    (void) rm_pipe_send(gs, ng, claimed, rcxt, &sql, &send_fail);
                }
                PG_FINALLY(2);
                {
                    raft_append_pxid = InvalidTransactionId;
                    for (i = ng - 1; i >= 0; i--)
                        if (claimed[i])
                        {
                            replicate_release(&gs[i].ctx);
                            claimed[i] = false;
                        }
                    MemoryContextSwitchTo(rmcxt);
                }
                PG_END_TRY(2);
                if (send_fail >= 0)
                {
                    fail_why = "读不到本地 parwal 记录";
                    fail_i = send_fail;
                    break;
                }
            }

            /* 等回包（有在途请求就 poll，最多 2 ms） */
            rm_pipe_harvest(2, true);
            CHECK_FOR_INTERRUPTS();

            if (TimestampDifferenceExceeds(t_log, GetCurrentTimestamp(), 5000))
            {
                t_log = GetCurrentTimestamp();
                elog(LOG, "pg_raft: [pipe-wait] 已等 %ld ms，%d 组",
                     (long) ((t_log - start) / 1000), ng);
            }
        }
        normal_exit = true;
    }
    PG_FINALLY();
    {
        /*
         * 异常退出：本后端发出、还没收的回包别人可能正等着（连接是本后端私有的）。在限时
         * 内把它们收掉，只落 match / next_index（错误处理途中不跑 SQL；提交点由等待者自己
         * 按 match_index 推进）。
         */
        if (!normal_exit)
        {
            TimestampTz h0 = GetCurrentTimestamp();
            int         slot2, k2;
            bool        any;

            do
            {
                rm_pipe_harvest(5, false);
                any = false;
                for (slot2 = 0; slot2 < n_peers; slot2++)
                    for (k2 = 0; k2 < RM_CONNS; k2++)
                        if (rm_conns[slot2][k2].busy)
                            any = true;
            } while (any && !TimestampDifferenceExceeds(h0, GetCurrentTimestamp(), 1000));
        }
        if (!normal_exit && failed_out != NULL)
            for (i = 0; i < ng; i++)
                if (!gs[i].done)
                    for (k = 0; k < nparts; k++)
                        if (parts[k] == gs[i].part)
                            failed_out[k] = true;
        in_txn_replication = false;
        MemoryContextSwitchTo(oldcxt);
    }
    PG_END_TRY();

    if (fail_why != NULL && fail_i >= 0)
    {
        Oid     fp = gs[fail_i].part;
        int64   fg = gs[fail_i].gid;
        int64   fc = gs[fail_i].cur;

        if (failed_out != NULL)
            for (i = 0; i < ng; i++)
                if (!gs[i].done)
                    for (k = 0; k < nparts; k++)
                        if (parts[k] == gs[i].part)
                            failed_out[k] = true;
        MemoryContextDelete(rmcxt);
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 分区 %u(组 %lld) record %lld 未达多数派（%s）",
                        fp, (long long) fg, (long long) fc, fail_why)));
    }
    MemoryContextDelete(rmcxt);
}

/* ================================================================== */
/* 2026-10-17：节点级有序复制流（pg_raft.replicate_stream）               */
/* ================================================================== */

/*
 * 结构（与 PostgreSQL 流复制同一思路，每个分片组仍是独立的 Raft 组）：
 *
 *   提交的后端：分区流落盘 → 认领位空着就把本组新记录追加进内存环（只罩住追加）→
 *               登记 want_plsn → 唤醒复制管理进程 → 睡在条件变量上，等各组提交点覆盖自己。
 *   复制管理进程（每节点一个，pg_raft_stream_main）：
 *     · 对每个副本节点一条长连接（libpq 管道模式，严格按序），把本节点当主的各组从
 *       next_index 起的新条目打成一批（pg_raft_stream_apply 的二进制消息）不停地发；
 *     · 收回包（同一连接上按序回来）→ 落 match / next_index → 按 Raft 规则推进各组提交点
 *       （leader 自投票只算到 leader_durable_index）→ 广播条件变量；
 *     · leader 自己的 raft_log 行在本进程的独立事务里写、提交落盘后才推进 leader_durable_index；
 *     · 提交点前移后做 hardstate / 标 committed / apply。
 *   副本：每条流一个会话、按到达顺序逐批处理，一批（含多个组）一次分区流 fsync + 一次提交。
 *
 * 认领位只用来串行化"往环里追加"（保证同组条目按 plsn 连续）：提交的后端、决议路径
 * （持认领位调 replicate_group_upto）、管理进程谁拿到谁追加；发送不需要认领位。
 */
#define RS_QMAX             32
#define RS_ROWS_MAX         16384
#define RS_MSG_MAX_ENTRIES  2048            /* 一条消息（全部组合计）最多几条：副本按序处理，太大会拖住整条流 */
#define RS_MSG_MAX_BYTES    (4 * 1024 * 1024)
#define RS_PEER_MAX_INFLIGHT_BYTES (32 * 1024 * 1024)   /* 每副本在途字节上限：慢副本不读时 libpq 输出缓冲会成倍扩张 */
#define RS_LOCAL_OID_CAP    1024

typedef bool (*PartWALReadHookFn) (Oid partition_id, uint64 plsn, XLogRecPtr *orig_lsn,
                                   int *rmid, int *info, int *flags, int64 *gxid,
                                   char **data, uint32 *data_len);
static PartWALReadHookFn rs_read_hook = NULL;

static bool
rs_read_record(Oid part, uint64 plsn, XLogRecPtr *orig, int *rmid, int *info, int *flags,
               int64 *gxid, char **data, uint32 *len)
{
    if (rs_read_hook == NULL)
    {
        void **p = find_rendezvous_variable("partdist_partwal_read_hook");

        rs_read_hook = (PartWALReadHookFn) *p;
        if (rs_read_hook == NULL)
            return false;
    }
    return rs_read_hook(part, plsn, orig, rmid, info, flags, gxid, data, len);
}

/*
 * 管理进程的"本轮记录缓存"：同一轮循环里追加时读过的记录、发给第一个副本时读过的字节，
 * 发给另一个副本时直接复用。只在管理进程里启用（rs_rc_cxt 非空），每轮开头清空。
 */
typedef struct RsRecKey
{
    Oid         part;
    uint64      plsn;
} RsRecKey;

typedef struct RsRecEnt
{
    RsRecKey    key;
    XLogRecPtr  orig;
    int         rmid, info, flags;
    int64       gxid;
    char       *data;
    uint32      len;
} RsRecEnt;

static HTAB         *rs_rc = NULL;
static MemoryContext rs_rc_cxt = NULL;

static void
rs_rc_reset(MemoryContext cxt)
{
    HASHCTL ctl;

    rs_rc_cxt = cxt;
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize = sizeof(RsRecKey);
    ctl.entrysize = sizeof(RsRecEnt);
    ctl.hcxt = cxt;
    rs_rc = hash_create("pg_raft stream record cache", 4096, &ctl,
                        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* 读一条记录（管理进程里走本轮缓存）。*data 归缓存所有时 *owned = false，调用方不得释放 */
static bool
rs_read_record_c(Oid part, uint64 plsn, XLogRecPtr *orig, int *rmid, int *info, int *flags,
                 int64 *gxid, char **data, uint32 *len, bool *owned)
{
    RsRecKey    key;
    RsRecEnt   *e;
    bool        found;

    *owned = true;
    if (rs_rc == NULL)
        return rs_read_record(part, plsn, orig, rmid, info, flags, gxid, data, len);
    memset(&key, 0, sizeof(key));
    key.part = part;
    key.plsn = plsn;
    e = (RsRecEnt *) hash_search(rs_rc, &key, HASH_FIND, &found);
    if (!found)
    {
        MemoryContext old = MemoryContextSwitchTo(rs_rc_cxt);
        XLogRecPtr  o;
        int         rm, inf, fl;
        int64       gx;
        char       *d = NULL;
        uint32      l = 0;
        bool        ok = rs_read_record(part, plsn, &o, &rm, &inf, &fl, &gx, &d, &l);

        MemoryContextSwitchTo(old);
        if (!ok)
            return false;
        e = (RsRecEnt *) hash_search(rs_rc, &key, HASH_ENTER, &found);
        e->orig = o;
        e->rmid = rm;
        e->info = inf;
        e->flags = fl;
        e->gxid = gx;
        e->data = d;
        e->len = l;
    }
    *orig = e->orig;
    *rmid = e->rmid;
    *info = e->info;
    *flags = e->flags;
    *gxid = e->gxid;
    *data = e->data;
    *len = e->len;
    *owned = false;
    return true;
}

/* 复制流是否在用：开关开着且管理进程活着 */
static bool
rs_stream_active(void)
{
    return pg_raft_replicate_stream && RaftGroups != NULL && RaftGroups->stream_pid != 0;
}

/* 数据组且复制流在用：心跳不碰 match/next，leader 自投票只算到 leader_durable_index */
static bool
rs_stream_data(RaftGroupCtx *ctx)
{
    return ctx->group_id != RAFT_CONTROL_GROUP && rs_stream_active();
}

static void
rs_kick(void)
{
    if (RaftGroups != NULL && RaftGroups->stream_pid != 0 && RaftGroups->stream_latch != NULL)
        SetLatch(RaftGroups->stream_latch);
}

/*
 * 持认领位调用：把本组 (已追加, upto] 的记录追加进环（读记录走 pg_partdist 的 C 钩子）。
 * 只追加本节点仍是 term 任期 leader 时；环满就追加到满为止。返回追加到的 plsn。
 */
static int64
rs_append_upto(RaftGroupCtx *ctx, Oid local_oid, int64 upto, int64 term)
{
    int64       appended = rm_appended_plsn(ctx);
    int64       p;
    int         n = 0;
    int         i;
    char      **desc;
    bool        leader;
    int64       room;

    /* 组提交：把已登记的别人的记录一并带上 */
    SpinLockAcquire(&ctx->log->mutex);
    if (ctx->log->want_plsn > upto)
        upto = ctx->log->want_plsn;
    SpinLockRelease(&ctx->log->mutex);
    if (appended >= upto || local_oid == InvalidOid)
        return appended;
    SpinLockAcquire(&ctx->cons->mutex);
    leader = (ctx->cons->state == RAFT_LEADER && ctx->cons->current_term == term);
    SpinLockRelease(&ctx->cons->mutex);
    if (!leader)
        return appended;

    desc = (char **) palloc(sizeof(char *) * pg_raft_stream_batch_entries);
    for (p = appended + 1; p <= upto && n < pg_raft_stream_batch_entries; p++)
    {
        XLogRecPtr  orig;
        int         rmid, info, flags;
        int64       gxid;
        char       *data = NULL;
        uint32      len = 0;
        bool        owned;

        if (!rs_read_record_c(local_oid, (uint64) p, &orig, &rmid, &info, &flags, &gxid,
                              &data, &len, &owned))
            break;
        desc[n++] = psprintf("{\"partition_lsn\":%lld,\"orig_lsn\":\"%X/%X\",\"rmid\":%d,"
                             "\"info\":%d,\"flags\":%d,\"gxid\":%lld,\"nbytes\":%lld}",
                             (long long) p, LSN_FORMAT_ARGS(orig), rmid, info, flags,
                             (long long) gxid, (long long) len);
        if (data != NULL && owned)
            pfree(data);
    }
    if (n == 0)
    {
        pfree(desc);
        return appended;
    }

    SpinLockAcquire(&ctx->log->mutex);
    room = RAFT_LOG_CAPACITY - 1 - (ctx->log->last_log_index - ctx->log->last_applied);
    if (n > room)
        n = (int) Max(room, 0);
    /* 当选后复制状态还没初始化：初始化时把环里已有的条目视为 leader 已落盘，先别往里追加 */
    if (!ctx->log->repl_inited)
        n = 0;
    if (ctx->log->last_log_index > 0)
    {
        RaftLogEntry *te = log_slot(ctx, ctx->log->last_log_index);

        if (te->index == ctx->log->last_log_index && te->term > term)
            n = 0;
    }
    for (i = 0; i < n; i++)
        if (log_append_locked(ctx, term, RAFT_OP_PARWAL, desc[i]) <= 0)
            break;
    n = i;
    SpinLockRelease(&ctx->log->mutex);
    pfree(desc);
    return appended + n;
}

/* 组提交点（多数派）是否已覆盖 cur，且本节点仍是 term0 任期 leader。*why 填失败原因 */
static int
rs_covered(RaftGroupCtx *ctx, int64 cur, int64 term0)
{
    bool ok_term;

    SpinLockAcquire(&ctx->cons->mutex);
    ok_term = (ctx->cons->state == RAFT_LEADER && ctx->cons->current_term == term0);
    SpinLockRelease(&ctx->cons->mutex);
    if (!ok_term)
        return -1;                      /* 失主 / 换任期 */
    if (rm_committed_plsn(ctx) < cur)
        return 0;
    SpinLockAcquire(&ctx->cons->mutex);
    ok_term = (ctx->cons->state == RAFT_LEADER && ctx->cons->current_term == term0);
    SpinLockRelease(&ctx->cons->mutex);
    return ok_term ? 1 : -1;
}

static void
rs_register_want(RaftGroupCtx *ctx, int64 cur)
{
    SpinLockAcquire(&ctx->log->mutex);
    if (cur > ctx->log->want_plsn)
        ctx->log->want_plsn = cur;
    SpinLockRelease(&ctx->log->mutex);
    ctx->g->last_rm_round = GetCurrentTimestamp();
}

/*
 * 持认领位的老路径（replicate_group_upto：DTX 决议、逐分区复制）在复制流下的走法：
 * 自己把记录追加进环、登记、等覆盖。失败 ERROR（与老路径"未达多数派"同一报错）。
 */
static void
rs_replicate_claimed(RaftGroupCtx *ctx, int64 cur_plsn, Oid partition_id)
{
    int64       term0;
    bool        leader;
    TimestampTz start = GetCurrentTimestamp();
    int         st = 0;

    SpinLockAcquire(&ctx->cons->mutex);
    leader = (ctx->cons->state == RAFT_LEADER);
    term0 = ctx->cons->current_term;
    SpinLockRelease(&ctx->cons->mutex);
    if (!leader)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 分区 %u(组 %lld) record %lld 未达多数派（本节点已不是 leader）",
                        partition_id, (long long) ctx->group_id, (long long) cur_plsn)));
    while (rm_appended_plsn(ctx) < cur_plsn)
    {
        int64 before = rm_appended_plsn(ctx);

        if (rs_append_upto(ctx, partition_id, cur_plsn, term0) <= before)
            break;
    }
    rs_register_want(ctx, cur_plsn);
    rs_kick();

    ConditionVariablePrepareToSleep(&RaftGroups->stream_cv);
    for (;;)
    {
        st = rs_covered(ctx, cur_plsn, term0);
        if (st != 0)
            break;
        if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), RM_PIPE_WAIT_MS))
            break;
        /* 环满时这里追加不全：再试一把（apply 已推进就能接着追加） */
        if (rm_appended_plsn(ctx) < cur_plsn)
        {
            (void) rs_append_upto(ctx, partition_id, cur_plsn, term0);
            rs_kick();
        }
        (void) ConditionVariableTimedSleep(&RaftGroups->stream_cv, 20, PG_WAIT_EXTENSION);
        CHECK_FOR_INTERRUPTS();
    }
    ConditionVariableCancelSleep();
    if (st != 1)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 分区 %u(组 %lld) record %lld 未达多数派（%s）",
                        partition_id, (long long) ctx->group_id, (long long) cur_plsn,
                        st < 0 ? "本节点已不是该分区组的 leader（或任期已变）" : "等多数派确认超时")));
}

/* 提交路径（多组）在复制流下的走法 */
static void
pg_raft_partwal_replicate_stream(const Oid *parts, int nparts, bool *failed_out)
{
    RmGroup        *gs;
    int             ng = 0;
    int             i, k;
    StringInfoData  sql;
    bool            spi_owned;
    MemoryContext   rmcxt;
    MemoryContext   oldcxt;
    TimestampTz     start = GetCurrentTimestamp();
    const char     *fail_why = NULL;
    int             fail_i = -1;

    rmcxt = AllocSetContextCreate(CurrentMemoryContext, "pg_raft replicate_stream",
                                  ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(rmcxt);
    gs = (RmGroup *) palloc0(sizeof(RmGroup) * nparts);

    /* ① 一次 SPI：组号与已落盘上界（同一轮一轮模式） */
    initStringInfo(&sql);
    if (!raft_persist_spi_begin(&spi_owned))
    {
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(rmcxt);
        return;
    }
    appendStringInfoString(&sql,
                           "SELECT u.p, partdist.global_id_for_partition(u.p)::bigint, "
                           "       partdist.get_partition_flush_lsn(u.p) "
                           "FROM unnest(ARRAY[");
    for (i = 0; i < nparts; i++)
        appendStringInfo(&sql, "%s%u", i ? "," : "", (unsigned) parts[i]);
    appendStringInfoString(&sql, "]::oid[]) WITH ORDINALITY u(p, o) ORDER BY u.o");
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT)
    {
        uint64 r;

        for (r = 0; r < SPI_processed; r++)
        {
            bool    n1, n2, n3;
            Datum   d1 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 1, &n1);
            Datum   d2 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 2, &n2);
            Datum   d3 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 3, &n3);

            if (n1 || n2 || DatumGetInt64(d2) <= 0)
                continue;
            gs[ng].part = DatumGetObjectId(d1);
            gs[ng].gid = DatumGetInt64(d2);
            gs[ng].cur = n3 ? 0 : DatumGetInt64(d3);
            ng++;
        }
    }
    raft_persist_spi_end(spi_owned);
    MemoryContextSwitchTo(rmcxt);

    /* ② 逐组门禁（同一轮一轮模式；被拒的分区标进 failed_out） */
    k = 0;
    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];
        int      state = -1;
        bool     known;

        if (!raft_group_ctx(g->gid, &g->ctx))
            continue;
        known = group_resolve_membership(&g->ctx);
        if (known)
        {
            restore_hard_state_if_needed(&g->ctx);
            restore_persistent_log_if_needed(&g->ctx);
            SpinLockAcquire(&g->ctx.cons->mutex);
            state = g->ctx.cons->state;
            g->term0 = g->ctx.cons->current_term;
            SpinLockRelease(&g->ctx.cons->mutex);
        }
        if ((!known || state != RAFT_LEADER) && failed_out != NULL)
        {
            int p;

            for (p = 0; p < nparts; p++)
                if (parts[p] == g->part)
                    failed_out[p] = true;
        }
        if (!known)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: 分区 %u(组 %lld)的成员集未知，写入被拒",
                            g->part, (long long) g->gid)));
        if (state != RAFT_LEADER)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: 分区 %u(组 %lld)的本地写入被拒：本节点不是该分区组的 leader",
                            g->part, (long long) g->gid),
                     errdetail("分区主副本可能已切换，请经路由层重试。")));
        if (g->cur <= 0)
            continue;
        if (k != i)
            gs[k] = *g;
        k++;
    }
    ng = k;
    if (ng == 0)
    {
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(rmcxt);
        return;
    }
    qsort(gs, ng, sizeof(RmGroup), rm_group_cmp);

    /* ③ 能拿到认领位就自己追加（只罩住追加），拿不到交给持有者 / 管理进程；登记、唤醒 */
    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];

        if (rm_appended_plsn(&g->ctx) < g->cur)
        {
            raft_claim_purpose = RAFT_CLAIM_COMMIT_MANY;
            if (replicate_try_claim(&g->ctx))
            {
                PG_TRY();
                {
                    (void) rs_append_upto(&g->ctx, g->part, g->cur, g->term0);
                }
                PG_FINALLY();
                {
                    replicate_release(&g->ctx);
                }
                PG_END_TRY();
            }
        }
        rs_register_want(&g->ctx, g->cur);
    }
    rs_kick();

    /* ④ 等各组提交点覆盖本事务的记录 */
    ConditionVariablePrepareToSleep(&RaftGroups->stream_cv);
    for (;;)
    {
        bool all_done = true;

        for (i = 0; i < ng; i++)
        {
            int st;

            if (gs[i].done)
                continue;
            st = rs_covered(&gs[i].ctx, gs[i].cur, gs[i].term0);
            if (st == 1)
            {
                gs[i].done = true;
                continue;
            }
            all_done = false;
            if (st < 0)
            {
                fail_why = "本节点已不是该分区组的 leader（或任期已变）";
                fail_i = i;
            }
        }
        if (all_done || fail_why != NULL)
            break;
        if (TimestampDifferenceExceeds(start, GetCurrentTimestamp(), RM_PIPE_WAIT_MS))
        {
            fail_why = "等多数派确认超时";
            for (i = 0; i < ng; i++)
                if (!gs[i].done)
                    fail_i = i;
            break;
        }
        (void) ConditionVariableTimedSleep(&RaftGroups->stream_cv, 20, PG_WAIT_EXTENSION);
        CHECK_FOR_INTERRUPTS();
    }
    ConditionVariableCancelSleep();
    {
        uint64 us = (uint64) (GetCurrentTimestamp() - start);
        uint64 mx = pg_atomic_read_u64(&RaftGroups->stream_wait_max);

        pg_atomic_fetch_add_u64(&RaftGroups->stream_wait_n, 1);
        pg_atomic_fetch_add_u64(&RaftGroups->stream_wait_us, us);
        while (us > mx && !pg_atomic_compare_exchange_u64(&RaftGroups->stream_wait_max, &mx, us))
            ;
    }

    if (fail_why != NULL && fail_i >= 0)
    {
        Oid     fp = gs[fail_i].part;
        int64   fg = gs[fail_i].gid;
        int64   fc = gs[fail_i].cur;

        if (failed_out != NULL)
            for (i = 0; i < ng; i++)
                if (!gs[i].done)
                    for (k = 0; k < nparts; k++)
                        if (parts[k] == gs[i].part)
                            failed_out[k] = true;
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(rmcxt);
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 分区 %u(组 %lld) record %lld 未达多数派（%s）",
                        fp, (long long) fg, (long long) fc, fail_why)));
    }
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(rmcxt);
}

/* ---------------------------- 复制管理进程 ---------------------------- */

typedef struct RsSent
{
    int64       gid;
    int64       first;
    int64       last;
    int64       term;           /* 发出时的 leader 任期 */
    int64       eterm;          /* 这一段条目的任期 */
    uint64      gen;            /* 发出时的截断代数 */
} RsSent;

typedef struct RsBatchQ
{
    int         ng;
    int         bytes;
    RsSent     *s;
    TimestampTz sent_at;
    bool        stmt_done;
    bool        null_seen;
    bool        failed;
    char       *ack;            /* 语句结果（"gid term ok last;..."）；等 Sync（隐式事务提交）回来才生效 */
} RsBatchQ;

typedef struct RsPeerSt
{
    PGconn     *conn;
    int         state;          /* 0 = 未连，1 = 连接中，2 = 就绪 */
    TimestampTz retry_at;
    RsBatchQ    q[RS_QMAX];
    int         qh;
    int         qn;
    int64       qbytes;         /* 在途批的字节合计 */
    int64       st_n, st_sum_us, st_max_us, st_ent;    /* 10 s 统计：批数、往返合计 / 最大、条数 */
} RsPeerSt;

static RsPeerSt     rs_peers[RAFT_MAX_PEERS];
static MemoryContext rs_cxt = NULL;
static int64        rs_oid_gid[RS_LOCAL_OID_CAP];
static Oid          rs_oid_val[RS_LOCAL_OID_CAP];

static Oid
rs_local_oid(int gidx, int64 gid)
{
    if (gidx < 0 || gidx >= RS_LOCAL_OID_CAP)
        return InvalidOid;
    return (rs_oid_gid[gidx] == gid) ? rs_oid_val[gidx] : InvalidOid;
}

static void
rs_on_exit(int code, Datum arg)
{
    if (RaftGroups != NULL && RaftGroups->stream_pid == MyProcPid)
    {
        RaftGroups->stream_pid = 0;
        RaftGroups->stream_latch = NULL;
    }
}

/* 某副本连接失败：在途批次的组 next_index 退回 match+1，断开，过一会儿重连 */
static void
rs_peer_fail(int slot, const char *why)
{
    RsPeerSt   *ps = &rs_peers[slot];
    static TimestampTz last_warn = 0;

    if (last_warn == 0 || TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 5000))
    {
        last_warn = GetCurrentTimestamp();
        elog(WARNING, "pg_raft: 复制流到节点 %d 断开（%s），重连后从 match+1 重发",
             peers[slot].node_id, why);
    }
    while (ps->qn > 0)
    {
        RsBatchQ   *b = &ps->q[ps->qh];
        int         j;

        for (j = 0; j < b->ng; j++)
        {
            RaftGroupCtx ctx;

            if (!raft_group_ctx(b->s[j].gid, &ctx))
                continue;
            SpinLockAcquire(&ctx.log->mutex);
            if (ctx.log->peer_next_index[slot] > ctx.log->peer_match_index[slot] + 1)
                ctx.log->peer_next_index[slot] = ctx.log->peer_match_index[slot] + 1;
            SpinLockRelease(&ctx.log->mutex);
        }
        if (b->s != NULL)
            pfree(b->s);
        b->s = NULL;
        if (b->ack != NULL)
            pfree(b->ack);
        b->ack = NULL;
        ps->qh = (ps->qh + 1) % RS_QMAX;
        ps->qn--;
    }
    if (ps->conn != NULL)
        PQfinish(ps->conn);
    ps->conn = NULL;
    ps->state = 0;
    ps->qh = 0;
    ps->qbytes = 0;
    ps->retry_at = GetCurrentTimestamp() + 500 * 1000;
}

/* 连接状态机（非阻塞建连，挂掉的副本不拖住别的副本） */
static void
rs_peer_connect(int slot)
{
    RsPeerSt   *ps = &rs_peers[slot];

    if (ps->state == 0)
    {
        char conninfo[512];

        if (GetCurrentTimestamp() < ps->retry_at)
            return;
        snprintf(conninfo, sizeof(conninfo),
                 "host=%s port=%d dbname=postgres user=postgres "
                 "connect_timeout=%d application_name=pg_raft_stream",
                 peers[slot].host, peers[slot].port, pg_raft_peer_connect_timeout_s);
        ps->conn = PQconnectStart(conninfo);
        if (ps->conn == NULL || PQstatus(ps->conn) == CONNECTION_BAD)
        {
            rs_peer_fail(slot, "建连失败");
            return;
        }
        ps->state = 1;
        ps->retry_at = GetCurrentTimestamp() + (int64) pg_raft_peer_connect_timeout_s * 1000000;
    }
    if (ps->state == 1)
    {
        PostgresPollingStatusType st = PQconnectPoll(ps->conn);

        if (st == PGRES_POLLING_OK)
        {
            if (PQsetnonblocking(ps->conn, 1) != 0 || PQenterPipelineMode(ps->conn) != 1)
            {
                rs_peer_fail(slot, "进入管道模式失败");
                return;
            }
            ps->state = 2;
        }
        else if (st == PGRES_POLLING_FAILED ||
                 GetCurrentTimestamp() > ps->retry_at)
            rs_peer_fail(slot, PQerrorMessage(ps->conn));
    }
}

/* 一条回包落到一组上 */
static void
rs_apply_ack(int slot, const RsSent *s, int64 rt, int ok, int64 flast, int64 acc)
{
    RaftGroupCtx ctx;
    bool         same;

    if (!raft_group_ctx(s->gid, &ctx))
        return;
    peer_stamp(&ctx, slot, true);
    if (rt > s->term)
    {
        step_down_if_higher(&ctx, rt);
        return;
    }
    SpinLockAcquire(&ctx.cons->mutex);
    same = (ctx.cons->state == RAFT_LEADER && ctx.cons->current_term == s->term);
    SpinLockRelease(&ctx.cons->mutex);
    if (!same)
        return;
    SpinLockAcquire(&ctx.log->mutex);
    /* 只收下了前缀（环满 / 中途被拒）：前缀照常算确认，next 退到前缀之后 */
    if (!ok && acc >= s->first && acc < s->last)
    {
        int64         m = Min(acc, ctx.log->last_log_index);
        RaftLogEntry *le = (m > 0) ? log_slot(&ctx, m) : NULL;
        bool          out_of_ring = (le != NULL && le->index != m &&
                                     m <= ctx.log->last_log_index - RAFT_LOG_CAPACITY + 1 &&
                                     m <= ctx.log->last_applied);

        if (le != NULL && (out_of_ring || (le->index == m && le->term == s->eterm)) &&
            ctx.log->trunc_gen == s->gen && m > ctx.log->peer_match_index[slot])
            ctx.log->peer_match_index[slot] = m;
        if (ctx.log->peer_next_index[slot] > acc + 1)
            ctx.log->peer_next_index[slot] = Max(acc + 1, ctx.log->peer_match_index[slot] + 1);
    }
    else if (ok)
    {
        int64         m = Min(s->last, ctx.log->last_log_index);
        RaftLogEntry *le = (m > 0) ? log_slot(&ctx, m) : NULL;
        /* 环外回读续发的那段：在 leader 上已 apply（已提交），槽位早已复用，不必再比任期 */
        bool          out_of_ring = (le != NULL && le->index != m &&
                                     m <= ctx.log->last_log_index - RAFT_LOG_CAPACITY + 1 &&
                                     m <= ctx.log->last_applied);

        if (le != NULL && (out_of_ring || (le->index == m && le->term == s->eterm)) &&
            ctx.log->trunc_gen == s->gen)
        {
            if (m > ctx.log->peer_match_index[slot])
                ctx.log->peer_match_index[slot] = m;
            if (ctx.log->peer_next_index[slot] < m + 1)
                ctx.log->peer_next_index[slot] = m + 1;
        }
    }
    else if (flast < 0)
    {
        /* 这一批没送达 / 没提交（语句或提交报错）：从这一批开头重发 */
        int64 nn = Max(ctx.log->peer_match_index[slot] + 1, s->first);

        if (ctx.log->peer_next_index[slot] > nn)
            ctx.log->peer_next_index[slot] = nn;
    }
    else
    {
        int64 nn = Max(ctx.log->peer_match_index[slot] + 1, Min(flast + 1, s->first));

        if (nn >= s->first)
            nn = Max(ctx.log->peer_match_index[slot] + 1, s->first - 32);
        if (nn < 1)
            nn = 1;
        if (ctx.log->peer_next_index[slot] > nn)
            ctx.log->peer_next_index[slot] = nn;
    }
    SpinLockRelease(&ctx.log->mutex);
}

/* 收某副本已到的回包（按序） */
static void
rs_peer_receive(int slot)
{
    RsPeerSt   *ps = &rs_peers[slot];

    if (ps->state != 2 || ps->qn == 0)
        return;
    if (!PQconsumeInput(ps->conn))
    {
        rs_peer_fail(slot, PQerrorMessage(ps->conn));
        return;
    }
    while (ps->qn > 0)
    {
        RsBatchQ   *b = &ps->q[ps->qh];
        PGresult   *r;
        ExecStatusType st;

        if (PQisBusy(ps->conn))
            break;
        r = PQgetResult(ps->conn);
        if (r == NULL)
        {
            if (b->stmt_done && !b->null_seen)
            {
                b->null_seen = true;
                continue;
            }
            break;
        }
        st = PQresultStatus(r);
        if (st == PGRES_PIPELINE_SYNC)
        {
            /*
             * Sync 回来 = 副本上隐式事务已提交（raft_log 行随 WAL 落盘；分区流字节在语句里已 fsync）。
             * 结果行在 Sync 之前就到了，那时还没提交 —— 只能到这里才算确认。
             */
            int j;

            PQclear(r);
            if (!b->failed && b->ack != NULL)
            {
                const char *v = b->ack;

                j = 0;
                while (*v && j < b->ng)
                {
                    long long gid, rt, last, acc;
                    int       ok, used = 0;

                    if (sscanf(v, "%lld %lld %d %lld %lld;%n", &gid, &rt, &ok, &last, &acc, &used) != 5 || used == 0)
                        break;
                    v += used;
                    if (gid == b->s[j].gid)
                        rs_apply_ack(slot, &b->s[j], rt, ok, last, acc);
                    j++;
                }
            }
            else
            {
                for (j = 0; j < b->ng; j++)
                    rs_apply_ack(slot, &b->s[j], 0, 0, -1, 0);
            }
            {
                int64 rtt = GetCurrentTimestamp() - b->sent_at;
                int   jj;

                ps->st_n++;
                ps->st_sum_us += rtt;
                if (rtt > ps->st_max_us)
                    ps->st_max_us = rtt;
                for (jj = 0; jj < b->ng && b->s != NULL; jj++)
                    ps->st_ent += b->s[jj].last - b->s[jj].first + 1;
            }
            if (b->ack != NULL)
                pfree(b->ack);
            b->ack = NULL;
            if (b->s != NULL)
                pfree(b->s);
            b->s = NULL;
            ps->qbytes -= b->bytes;
            ps->qh = (ps->qh + 1) % RS_QMAX;
            ps->qn--;
            continue;
        }
        if (st == PGRES_TUPLES_OK && !b->stmt_done && PQntuples(r) == 1)
        {
            b->stmt_done = true;
            b->ack = MemoryContextStrdup(rs_cxt, PQgetvalue(r, 0, 0));
        }
        else
        {
            static TimestampTz last_warn = 0;

            b->stmt_done = true;
            b->failed = true;
            if (last_warn == 0 || TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 5000))
            {
                last_warn = GetCurrentTimestamp();
                elog(WARNING, "pg_raft: 复制流到节点 %d 的一批失败（%s）：%s",
                     peers[slot].node_id, PQresStatus(st), PQresultErrorMessage(r));
            }
        }
        PQclear(r);
    }
    if (ps->qn > 0 &&
        TimestampDifferenceExceeds(ps->q[ps->qh].sent_at, GetCurrentTimestamp(), 15000))
        rs_peer_fail(slot, "一批 15 s 未回");
}

/*
 * 副本落后到内存环之外：从 leader 自己的 raft_log 行回读 [nx-1, nx+maxn-1]（同 PG 流复制
 * 从磁盘上的 WAL 段回读给慢备库）。只取从 nx 起连续、同任期、全是数据条目的一段；
 * *prev_term 填 nx-1 那条的任期。返回条数（0 = 这回取不到）。在本进程自己的短事务里做。
 */
static int
rs_fetch_sql(int64 gid, int64 nx, int maxn, int64 *prev_term, int64 *eterm,
             int64 *eidx, char **epl)
{
    MemoryContext   old = CurrentMemoryContext;
    StringInfoData  sql;
    int             n = 0;
    uint64          r;

    StartTransactionCommand();
    PushActiveSnapshot(GetTransactionSnapshot());
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        PopActiveSnapshot();
        CommitTransactionCommand();
        MemoryContextSwitchTo(old);
        return 0;
    }
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT log_index, term, op_type, payload::text FROM partdist.raft_log "
                     "WHERE group_id = %lld AND log_index >= %lld AND log_index <= %lld "
                     "ORDER BY log_index",
                     (long long) gid, (long long) Max(nx - 1, 1), (long long) (nx + maxn - 1));
    *prev_term = 0;
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT)
    {
        int64 expect = (nx > 1) ? nx - 1 : nx;

        for (r = 0; r < SPI_processed; r++)
        {
            HeapTuple   tup = SPI_tuptable->vals[r];
            TupleDesc   td = SPI_tuptable->tupdesc;
            bool        isnull;
            int64       li = DatumGetInt64(SPI_getbinval(tup, td, 1, &isnull));
            int64       te = DatumGetInt64(SPI_getbinval(tup, td, 2, &isnull));
            char       *op = SPI_getvalue(tup, td, 3);
            char       *pl = SPI_getvalue(tup, td, 4);

            if (li != expect)
                break;          /* 不连续（行缺）：取到这里为止 */
            expect++;
            if (li == nx - 1)
            {
                *prev_term = te;
                continue;
            }
            if (op == NULL || pl == NULL || strcmp(op, RAFT_OP_PARWAL) != 0 ||
                (n > 0 && te != *eterm) || strlen(pl) >= RAFT_PAYLOAD_MAX)
                break;
            if (n == 0)
                *eterm = te;
            eidx[n] = li;
            epl[n] = MemoryContextStrdup(old, pl);
            n++;
        }
        /* nx-1 那条必须在（nx = 1 时没有 prev） */
        if (nx > 1 && *prev_term == 0)
            n = 0;
    }
    pfree(sql.data);
    SPI_finish();
    PopActiveSnapshot();
    CommitTransactionCommand();
    MemoryContextSwitchTo(old);
    return n;
}

/* 给某副本打一批并发出 */
static void
rs_peer_send(int slot)
{
    RsPeerSt       *ps = &rs_peers[slot];
    StringInfoData  msg;
    RsSent         *sent;
    int             ng = 0;
    int             gi, gstep;
    int             ngroups_pos;
    int             msg_entries = 0;
    int             batch_max = Min(pg_raft_stream_batch_entries, RAFT_LOG_CAPACITY / 2);
    int             nsql = 0;
    static int      rr = 0;

    if (ps->state != 2 || ps->qn >= Min(pg_raft_stream_inflight, RS_QMAX) ||
        ps->qbytes >= RS_PEER_MAX_INFLIGHT_BYTES)
        return;

    initStringInfo(&msg);
    pq_sendint32(&msg, RS_MSG_MAGIC);
    pq_sendint32(&msg, pg_raft_node_id);
    ngroups_pos = msg.len;
    pq_sendint32(&msg, 0);
    sent = (RsSent *) MemoryContextAlloc(rs_cxt, sizeof(RsSent) * RAFT_MAX_GROUPS);

    rr = (rr + 1) % Max(RAFT_MAX_GROUPS, 1);       /* 轮转起点：消息装满时别总饿着排在后面的组 */
    for (gstep = 0; gstep < RAFT_MAX_GROUPS; gstep++)
    {
        RaftGroupState *g = &RaftGroups->groups[(rr + gstep) % RAFT_MAX_GROUPS];
        RaftGroupCtx    ctx;
        int64           term;
        bool            leader;
        int64           last, nx, end, prev_idx, prev_term = 0, eterm = 0, floor_idx, commit_now;
        int64           k, cnt;
        int64          *eidx;
        char          **epl;
        Oid             loid;
        uint64          gen;
        bool            ok = true;

        if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP)
            continue;
        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;
        SpinLockAcquire(&ctx.cons->mutex);
        leader = (ctx.cons->state == RAFT_LEADER);
        term = ctx.cons->current_term;
        SpinLockRelease(&ctx.cons->mutex);
        if (!leader || !peer_in_group(&ctx, slot))
            continue;
        gi = (rr + gstep) % RAFT_MAX_GROUPS;
        loid = rs_local_oid(gi, g->group_id);
        if (loid == InvalidOid)
            continue;

        SpinLockAcquire(&ctx.log->mutex);
        if (!ctx.log->repl_inited)
        {
            int64 nxi = ctx.log->last_log_index + 1;
            int   j;

            for (j = 0; j < RAFT_MAX_PEERS; j++)
            {
                ctx.log->peer_next_index[j] = nxi;
                ctx.log->peer_match_index[j] = 0;
            }
            ctx.log->repl_inited = true;
            ctx.log->leader_durable_index = ctx.log->last_log_index;
            ctx.log->leader_self_hole = 0;
        }
        last = ctx.log->last_log_index;
        nx = ctx.log->peer_next_index[slot];
        commit_now = ctx.log->commit_index;
        SpinLockRelease(&ctx.log->mutex);
        if (msg_entries >= RS_MSG_MAX_ENTRIES || msg.len >= RS_MSG_MAX_BYTES)
            break;
        if (nx > last || nx <= 0)
            continue;
        floor_idx = last - RAFT_LOG_CAPACITY + 2;
        prev_idx = nx - 1;
        if (nx < floor_idx)
        {
            /*
             * 落后超出内存环：从 leader 的 raft_log 行回读续发（同 PG 流复制从 WAL 段回读给慢备库）。
             * 每条消息至多带一组、≤256 条这样的回读：副本按序处理整条消息，追历史的大段会挡在新提交前面。
             * 环外的条目在 leader 上必然已 apply（环槽位要 apply 后才复用），即已提交，不会再被截。
             */
            if (nsql >= 1)
                continue;
            nsql++;
            cnt = Min(Min(batch_max, 256), RS_MSG_MAX_ENTRIES - msg_entries);
            if (cnt <= 0)
                continue;
            eidx = (int64 *) palloc(sizeof(int64) * cnt);
            epl = (char **) palloc(sizeof(char *) * cnt);
            SpinLockAcquire(&ctx.log->mutex);
            gen = ctx.log->trunc_gen;
            SpinLockRelease(&ctx.log->mutex);
            cnt = rs_fetch_sql(g->group_id, nx, (int) cnt, &prev_term, &eterm, eidx, epl);
            if (cnt <= 0)
                continue;
        }
        else
        {
            /*
             * 窗口：不发超出"提交点 + 环容量 - 64"的条目。副本只能按它知道的提交点 apply 腾环
             * （批内环满时就地 apply），发得再靠前它的环会满、只能拒收。
             */
            end = Min(last, nx + batch_max - 1);
            end = Min(end, Max(commit_now, ctx.log->last_applied) + RAFT_LOG_CAPACITY - 64);
            end = Min(end, nx + (RS_MSG_MAX_ENTRIES - msg_entries) - 1);
            if (end < nx)
                continue;
            cnt = end - nx + 1;
            eidx = (int64 *) palloc(sizeof(int64) * cnt);
            epl = (char **) palloc(sizeof(char *) * cnt);
            for (k = 0; k < cnt; k++)
                epl[k] = (char *) palloc(RAFT_PAYLOAD_MAX);

            /* 锁下按槽位拷：同任期、全是数据条目的一段（遇到别的就停） */
            SpinLockAcquire(&ctx.log->mutex);
            gen = ctx.log->trunc_gen;
            if (prev_idx > 0)
            {
                RaftLogEntry *pe = log_slot(&ctx, prev_idx);

                if (pe->index == prev_idx)
                    prev_term = pe->term;
                else if (prev_idx == ctx.log->base_index)
                    prev_term = ctx.log->base_term;
                else
                    ok = false;
            }
            for (k = 0; ok && k < cnt; k++)
            {
                RaftLogEntry *x = log_slot(&ctx, nx + k);

                if (x->index != nx + k || strcmp(x->op_type, RAFT_OP_PARWAL) != 0)
                    break;
                if (k == 0)
                    eterm = x->term;
                else if (x->term != eterm)
                    break;
                eidx[k] = x->index;
                strlcpy(epl[k], x->payload, RAFT_PAYLOAD_MAX);
            }
            cnt = ok ? k : 0;
            SpinLockRelease(&ctx.log->mutex);
            if (cnt <= 0)
                continue;
        }

        /* 字节：按 plsn 从缓存 / 段文件取；取不到就截在那里 */
        {
            int64   startpos = msg.len;
            int64   wrote = 0;

            pq_sendint64(&msg, g->group_id);
            pq_sendint64(&msg, term);
            pq_sendint64(&msg, prev_idx);
            pq_sendint64(&msg, prev_term);
            {
                int64 lc;

                SpinLockAcquire(&ctx.log->mutex);
                lc = ctx.log->commit_index;
                SpinLockRelease(&ctx.log->mutex);
                pq_sendint64(&msg, lc);
            }
            pq_sendint64(&msg, eterm);
            {
                int npos = msg.len;

                pq_sendint32(&msg, 0);
                for (k = 0; k < cnt; k++)
                {
                    XLogRecPtr  orig;
                    int         rmid, info, flags;
                    int64       gxid;
                    char       *data = NULL;
                    uint32      dlen = 0;
                    int64       plsn = entry_partition_lsn(epl[k]);
                    int         plen = (int) strlen(epl[k]);
                    bool        owned = true;

                    if (plsn <= 0 ||
                        !rs_read_record_c(loid, (uint64) plsn, &orig, &rmid, &info, &flags,
                                          &gxid, &data, &dlen, &owned))
                        break;
                    /* 防御：一条记录不该大过 64 MB（读到坏长度时宁可这一轮不发） */
                    if (dlen > 64 * 1024 * 1024)
                    {
                        static TimestampTz last_warn = 0;

                        if (last_warn == 0 ||
                            TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 10000))
                        {
                            last_warn = GetCurrentTimestamp();
                            elog(WARNING, "pg_raft: 复制流读到组 %lld plsn=%lld 的记录长度 %u 不合理，这一轮不发",
                                 (long long) g->group_id, (long long) plsn, (unsigned) dlen);
                        }
                        if (data != NULL && owned)
                            pfree(data);
                        break;
                    }
                    pq_sendint64(&msg, eidx[k]);
                    pq_sendint32(&msg, plen);
                    pq_sendbytes(&msg, epl[k], plen);
                    pq_sendint32(&msg, (int) dlen);
                    if (dlen > 0)
                        pq_sendbytes(&msg, data, (int) dlen);
                    if (data != NULL && owned)
                        pfree(data);
                    wrote++;
                    if (msg.len >= RS_MSG_MAX_BYTES)
                        break;
                }
                if (wrote == 0)
                {
                    msg.len = (int) startpos;       /* 一条都没取到：撤掉这一组 */
                    msg.data[msg.len] = '\0';
                    continue;
                }
                {
                    uint32 nbe = pg_hton32((uint32) wrote);

                    memcpy(msg.data + npos, &nbe, 4);
                }
            }
            sent[ng].gid = g->group_id;
            sent[ng].first = nx;
            sent[ng].last = nx + wrote - 1;
            sent[ng].term = term;
            sent[ng].eterm = eterm;
            sent[ng].gen = gen;
            ng++;
            msg_entries += (int) wrote;
            /* 乐观推进：管道里后面的批接着往后发 */
            SpinLockAcquire(&ctx.log->mutex);
            if (ctx.log->peer_next_index[slot] == nx)
                ctx.log->peer_next_index[slot] = nx + wrote;
            SpinLockRelease(&ctx.log->mutex);
        }
    }

    if (ng == 0)
    {
        pfree(sent);
        pfree(msg.data);
        return;
    }
    {
        uint32 nbe = pg_hton32((uint32) ng);

        memcpy(msg.data + ngroups_pos, &nbe, 4);
    }
    {
        const char *vals[1];
        int         lens[1];
        int         fmts[1];
        Oid         types[1];
        RsBatchQ   *b;

        if (msg.len > 2 * RS_MSG_MAX_BYTES)
            elog(LOG, "pg_raft: 复制流到节点 %d 的一批 %d 字节（%d 组），超过预期上限",
                 peers[slot].node_id, msg.len, ng);
        vals[0] = msg.data;
        lens[0] = msg.len;
        fmts[0] = 1;
        types[0] = BYTEAOID;
        if (PQsendQueryParams(ps->conn, "SELECT partdist.pg_raft_stream_apply($1)", 1,
                              types, vals, lens, fmts, 0) != 1 ||
            PQpipelineSync(ps->conn) != 1)
        {
            int j;

            /* 没发出去：乐观推进退回 */
            for (j = 0; j < ng; j++)
            {
                RaftGroupCtx ctx;

                if (!raft_group_ctx(sent[j].gid, &ctx))
                    continue;
                SpinLockAcquire(&ctx.log->mutex);
                if (ctx.log->peer_next_index[slot] > sent[j].first)
                    ctx.log->peer_next_index[slot] = sent[j].first;
                SpinLockRelease(&ctx.log->mutex);
            }
            pfree(sent);
            rs_peer_fail(slot, PQerrorMessage(ps->conn));
            return;
        }
        b = &ps->q[(ps->qh + ps->qn) % RS_QMAX];
        b->ng = ng;
        b->s = sent;
        b->sent_at = GetCurrentTimestamp();
        b->stmt_done = false;
        b->null_seen = false;
        b->failed = false;
        b->ack = NULL;
        b->bytes = msg.len;
        ps->qbytes += msg.len;
        ps->qn++;
    }
    pfree(msg.data);
}

/* 本节点当主的数据组：推进提交点（leader 自投票只算到 leader_durable_index）。返回推进了的组数 */
static int
rs_advance_all(bool *advanced)
{
    int gi;
    int n = 0;

    for (gi = 0; gi < RAFT_MAX_GROUPS; gi++)
    {
        RaftGroupState *g = &RaftGroups->groups[gi];
        RaftGroupCtx    ctx;
        bool            leader;
        int64           term, nc;

        advanced[gi] = false;
        if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP)
            continue;
        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;
        SpinLockAcquire(&ctx.cons->mutex);
        leader = (ctx.cons->state == RAFT_LEADER);
        term = ctx.cons->current_term;
        SpinLockRelease(&ctx.cons->mutex);
        if (!leader)
            continue;
        SpinLockAcquire(&ctx.log->mutex);
        nc = compute_new_commit_index(&ctx, term);
        if (nc > ctx.log->commit_index)
        {
            ctx.log->commit_index = nc;
            advanced[gi] = true;
            n++;
        }
        SpinLockRelease(&ctx.log->mutex);
    }
    if (n > 0)
        ConditionVariableBroadcast(&RaftGroups->stream_cv);
    return n;
}

/*
 * 本进程的本地事务：补本地分区号、把 leader 自己的 raft_log 行写下并提交（之后才推进
 * leader_durable_index）、对提交点前移 / apply 落后的组做 hardstate、标记与 apply。
 */
static void
rs_local_work(const bool *advanced)
{
    int             gi;
    int             nrows = 0;
    int64          *row_to;
    int64          *row_term_chk;
    bool            spi_owned;
    Datum          *d_gid, *d_idx, *d_term, *d_op, *d_pl;
    static SPIPlanPtr rows_plan = NULL;

    row_to = (int64 *) palloc0(sizeof(int64) * RAFT_MAX_GROUPS);
    row_term_chk = (int64 *) palloc0(sizeof(int64) * RAFT_MAX_GROUPS);
    d_gid = (Datum *) palloc(sizeof(Datum) * RS_ROWS_MAX);
    d_idx = (Datum *) palloc(sizeof(Datum) * RS_ROWS_MAX);
    d_term = (Datum *) palloc(sizeof(Datum) * RS_ROWS_MAX);
    d_op = (Datum *) palloc(sizeof(Datum) * RS_ROWS_MAX);
    d_pl = (Datum *) palloc(sizeof(Datum) * RS_ROWS_MAX);

    StartTransactionCommand();
    (void) set_config_option("lock_timeout", "500", PGC_USERSET, PGC_S_SESSION,
                             GUC_ACTION_LOCAL, true, 0, false);
    PushActiveSnapshot(GetTransactionSnapshot());
    if (!groups_restored)
        restore_groups_if_needed();
    if (!raft_persist_spi_begin(&spi_owned) || !raft_log_table_ready())
    {
        if (spi_owned)
            raft_persist_spi_end(spi_owned);
        PopActiveSnapshot();
        CommitTransactionCommand();
        return;
    }
    raft_persist_spi_end(spi_owned);

    for (gi = 0; gi < RAFT_MAX_GROUPS; gi++)
    {
        RaftGroupState *g = &RaftGroups->groups[gi];
        RaftGroupCtx    ctx;
        bool            leader;
        int64           from, to, ix;

        if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP)
            continue;
        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;
        SpinLockAcquire(&ctx.cons->mutex);
        leader = (ctx.cons->state == RAFT_LEADER);
        SpinLockRelease(&ctx.cons->mutex);
        if (!leader)
            continue;
        if (gi < RS_LOCAL_OID_CAP && rs_oid_gid[gi] != g->group_id)
        {
            int64 lo = group_local_partition(&ctx);

            if (lo > 0)
            {
                rs_oid_gid[gi] = g->group_id;
                rs_oid_val[gi] = (Oid) lo;
            }
        }
        SpinLockAcquire(&ctx.log->mutex);
        if (!ctx.log->repl_inited)
        {
            SpinLockRelease(&ctx.log->mutex);
            continue;               /* 发送阶段会初始化（并把当选时的日志视为已落盘） */
        }
        from = (ctx.log->leader_self_hole > 0) ? ctx.log->leader_self_hole
                                               : ctx.log->leader_durable_index + 1;
        to = ctx.log->last_log_index;
        if (from < to - RAFT_LOG_CAPACITY + 2)
            from = to - RAFT_LOG_CAPACITY + 2;
        if (to - from + 1 > RS_ROWS_MAX - nrows)
            to = from + (RS_ROWS_MAX - nrows) - 1;
        SpinLockRelease(&ctx.log->mutex);
        for (ix = from; ix <= to; ix++)
        {
            RaftLogEntry e;
            bool         got;

            SpinLockAcquire(&ctx.log->mutex);
            got = log_get_entry_locked(&ctx, ix, &e);
            SpinLockRelease(&ctx.log->mutex);
            if (!got)
                break;
            d_gid[nrows] = Int64GetDatum(g->group_id);
            d_idx[nrows] = Int64GetDatum(ix);
            d_term[nrows] = Int64GetDatum(e.term);
            d_op[nrows] = CStringGetTextDatum(e.op_type);
            d_pl[nrows] = CStringGetTextDatum(e.payload);
            nrows++;
            row_to[gi] = ix;
            row_term_chk[gi] = e.term;
        }
        if (nrows >= RS_ROWS_MAX)
            break;
    }
    /* 预备语句 + 数组参数：解析 / 规划成本与行数无关（原来拼一条几千行的 VALUES 文本，单次上百毫秒） */
    if (nrows > 0 && raft_persist_spi_begin(&spi_owned))
    {
        Datum   vals[5];
        char    nulls[6] = "     ";

        if (rows_plan == NULL)
        {
            Oid         argtypes[5] = {INT8ARRAYOID, INT8ARRAYOID, INT8ARRAYOID,
                                       TEXTARRAYOID, TEXTARRAYOID};
            SPIPlanPtr  pl = SPI_prepare("INSERT INTO partdist.raft_log "
                                         "(group_id, log_index, term, op_type, payload, committed) "
                                         "SELECT u.g, u.i, u.t, u.o, u.p::jsonb, false "
                                         "FROM unnest($1, $2, $3, $4, $5) AS u(g, i, t, o, p) "
                                         "ON CONFLICT (group_id, log_index) DO UPDATE SET "
                                         "term = EXCLUDED.term, op_type = EXCLUDED.op_type, "
                                         "payload = EXCLUDED.payload, "
                                         "committed = partdist.raft_log.committed OR EXCLUDED.committed",
                                         5, argtypes);

            if (pl == NULL || SPI_keepplan(pl) != 0)
                elog(ERROR, "pg_raft: 复制流预备 leader 日志行语句失败");
            rows_plan = pl;
        }
        vals[0] = PointerGetDatum(construct_array(d_gid, nrows, INT8OID, 8, FLOAT8PASSBYVAL, TYPALIGN_DOUBLE));
        vals[1] = PointerGetDatum(construct_array(d_idx, nrows, INT8OID, 8, FLOAT8PASSBYVAL, TYPALIGN_DOUBLE));
        vals[2] = PointerGetDatum(construct_array(d_term, nrows, INT8OID, 8, FLOAT8PASSBYVAL, TYPALIGN_DOUBLE));
        vals[3] = PointerGetDatum(construct_array(d_op, nrows, TEXTOID, -1, false, TYPALIGN_INT));
        vals[4] = PointerGetDatum(construct_array(d_pl, nrows, TEXTOID, -1, false, TYPALIGN_INT));
        if (SPI_execute_plan(rows_plan, vals, nulls, false, 0) != SPI_OK_INSERT)
            elog(ERROR, "pg_raft: 复制流写 leader 日志行失败");
        raft_persist_spi_end(spi_owned);
    }
    PopActiveSnapshot();
    CommitTransactionCommand();         /* 落盘：此后这些行才算 leader 自己的一票 */

    for (gi = 0; nrows > 0 && gi < RAFT_MAX_GROUPS; gi++)
    {
        RaftGroupState *g = &RaftGroups->groups[gi];
        RaftLogEntry   *le;

        if (row_to[gi] <= 0 || !g->in_use)
            continue;
        SpinLockAcquire(&g->log.mutex);
        le = &g->log.ring[(row_to[gi] - 1) % RAFT_LOG_CAPACITY];
        if (le->index == row_to[gi] && le->term == row_term_chk[gi])
        {
            if (row_to[gi] > g->log.leader_durable_index)
                g->log.leader_durable_index = row_to[gi];
            g->log.leader_self_hole = 0;
        }
        SpinLockRelease(&g->log.mutex);
    }

    /* 提交点前移 / apply 落后的组：hardstate、标 committed、apply */
    {
        bool any = false;

        for (gi = 0; gi < RAFT_MAX_GROUPS; gi++)
        {
            RaftGroupState *g = &RaftGroups->groups[gi];

            if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP)
                continue;
            if (advanced[gi] || g->log.last_applied < g->log.commit_index)
            {
                any = true;
                break;
            }
        }
        if (!any)
            return;
    }
    /*
     * 每组一个短事务，且**绝不等行锁**（lock_timeout）：leader 的 apply 会登记 DTX 决议行，
     * 而写同一行的发起事务此刻可能正等着本进程推进提交点 —— 在这里等它就是互等（实测管理进程
     * 卡满 30 s、整节点所有组的复制随之停摆）。撞锁就跳过这一组，下一轮再来；提交点的推进
     * 不依赖 apply（只有环满才依赖），那个事务提交后锁自然放开。
     *
     * 这一段只写可恢复的东西（hardstate 里的 commit/applied、raft_log.committed 标记、apply 游标）：
     * 丢了由 leader 心跳 / 幂等 apply 补回（同 data_hardstate_lazy 的论证），提交不必等 WAL fsync。
     * leader 自己的日志行在上一个事务里、照常同步提交。副本组的 apply 由复制流会话自己做。
     */
    for (gi = 0; gi < RAFT_MAX_GROUPS; gi++)
    {
        RaftGroupState *g = &RaftGroups->groups[gi];
        RaftGroupCtx    ctx;
        int64           ci;
        bool            leader;
        MemoryContext   oldcxt = CurrentMemoryContext;

        if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP)
            continue;
        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;
        SpinLockAcquire(&ctx.cons->mutex);
        leader = (ctx.cons->state == RAFT_LEADER);
        SpinLockRelease(&ctx.cons->mutex);
        if (!leader)
            continue;
        SpinLockAcquire(&ctx.log->mutex);
        ci = ctx.log->commit_index;
        SpinLockRelease(&ctx.log->mutex);
        if (!advanced[gi] && ctx.log->last_applied >= ci)
            continue;
        /*
         * leader 自己的日志行还没落到提交点：先别 apply（apply 之后环槽位就可能被复用，
         * 那几条还没写进 raft_log 的条目就再也写不进去了）。多数派可以只靠两个副本凑齐，
         * 所以提交点可能跑在 leader 自己的落盘点前面。
         */
        if (ctx.log->leader_durable_index < ci)
            continue;
        PG_TRY();
        {
            StartTransactionCommand();
            (void) set_config_option("synchronous_commit", "off", PGC_USERSET, PGC_S_SESSION,
                                     GUC_ACTION_LOCAL, true, 0, false);
            (void) set_config_option("lock_timeout", "50", PGC_USERSET, PGC_S_SESSION,
                                     GUC_ACTION_LOCAL, true, 0, false);
            PushActiveSnapshot(GetTransactionSnapshot());
            if (advanced[gi])
                persist_hard_state_unlocked(&ctx);      /* 不逐行标 committed，理由同副本端（rs_batch_core） */
            /*
             * leader 自己的 apply 不写 follower_partition_map.applied_part_lsn（同老路径在提交事务里
             * apply 时的 in_txn_replication 语义）：那一列是"本节点作为副本收到并回放到哪"，写在主上
             * 会让本节点日后的升主前置误判"收了分区 WAL 却没有回放槽位"而拒绝升主（实测建组首轮选举
             * 原主被拒、换别的节点当选，共置分片的主从此不在同一节点）。
             */
            in_txn_replication = true;
            PG_TRY(2);
            {
                group_apply_pending(&ctx);
            }
            PG_FINALLY(2);
            {
                in_txn_replication = false;
            }
            PG_END_TRY(2);
            PopActiveSnapshot();
            CommitTransactionCommand();
        }
        PG_CATCH();
        {
            static TimestampTz last_warn = 0;
            ErrorData  *ed;

            MemoryContextSwitchTo(oldcxt);
            ed = CopyErrorData();
            FlushErrorState();
            AbortCurrentTransaction();
            if (last_warn == 0 || TimestampDifferenceExceeds(last_warn, GetCurrentTimestamp(), 5000))
            {
                last_warn = GetCurrentTimestamp();
                elog(LOG, "pg_raft: 复制流管理进程 apply 组 %lld 未成（下一轮重试）：%s",
                     (long long) g->group_id, ed->message);
            }
            FreeErrorData(ed);
        }
        PG_END_TRY();
        MemoryContextSwitchTo(oldcxt);
    }
}

/* 管理进程替排不上认领位的提交者追加（want_plsn 已登记、还没进环的） */
static void
rs_append_wanted(void)
{
    int gi;

    for (gi = 0; gi < RAFT_MAX_GROUPS; gi++)
    {
        RaftGroupState *g = &RaftGroups->groups[gi];
        RaftGroupCtx    ctx;
        bool            leader;
        int64           term, want;
        Oid             loid;

        if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP)
            continue;
        ctx.group_id = g->group_id;
        ctx.g = g;
        ctx.cons = &g->cons;
        ctx.log = &g->log;
        SpinLockAcquire(&ctx.cons->mutex);
        leader = (ctx.cons->state == RAFT_LEADER);
        term = ctx.cons->current_term;
        SpinLockRelease(&ctx.cons->mutex);
        if (!leader)
            continue;
        SpinLockAcquire(&ctx.log->mutex);
        want = ctx.log->want_plsn;
        SpinLockRelease(&ctx.log->mutex);
        if (want <= 0 || rm_appended_plsn(&ctx) >= want)
            continue;
        loid = rs_local_oid(gi, g->group_id);
        if (loid == InvalidOid)
            continue;
        raft_claim_purpose = RAFT_CLAIM_COMMIT_MANY;
        if (!replicate_try_claim(&ctx))
            continue;
        PG_TRY();
        {
            (void) rs_append_upto(&ctx, loid, want, term);
        }
        PG_FINALLY();
        {
            replicate_release(&ctx);
        }
        PG_END_TRY();
    }
}

/* 每 10 s 一行：各副本连接 / 在途批数，以及提交点落后最多的几组（诊断用） */
static void
rs_status_log(void)
{
    static TimestampTz last = 0;
    StringInfoData     b;
    int                slot, gi, k;
    int64              worst_lag[3] = {0, 0, 0};
    int                worst_gi[3] = {-1, -1, -1};

    if (last != 0 && !TimestampDifferenceExceeds(last, GetCurrentTimestamp(), 10000))
        return;
    last = GetCurrentTimestamp();
    initStringInfo(&b);
    for (slot = 0; slot < n_peers; slot++)
        if (peers[slot].node_id != pg_raft_node_id)
        {
            RsPeerSt *ps = &rs_peers[slot];

            appendStringInfo(&b, "节点%d:%s/在途%d/批%lld 条%lld 往返均%lldms 最大%lldms ",
                             peers[slot].node_id,
                             ps->state == 2 ? "就绪" : ps->state == 1 ? "连接中" : "断开",
                             ps->qn, (long long) ps->st_n, (long long) ps->st_ent,
                             (long long) (ps->st_n > 0 ? ps->st_sum_us / ps->st_n / 1000 : 0),
                             (long long) (ps->st_max_us / 1000));
            ps->st_n = ps->st_sum_us = ps->st_max_us = ps->st_ent = 0;
        }
    for (gi = 0; gi < RAFT_MAX_GROUPS; gi++)
    {
        RaftGroupState *g = &RaftGroups->groups[gi];
        int64           lag;

        if (!g->in_use || g->group_id == RAFT_CONTROL_GROUP || g->cons.state != RAFT_LEADER)
            continue;
        lag = g->log.last_log_index - g->log.commit_index;
        for (k = 0; k < 3; k++)
            if (lag > worst_lag[k])
            {
                int j;

                for (j = 2; j > k; j--)
                {
                    worst_lag[j] = worst_lag[j - 1];
                    worst_gi[j] = worst_gi[j - 1];
                }
                worst_lag[k] = lag;
                worst_gi[k] = gi;
                break;
            }
    }
    for (k = 0; k < 3; k++)
    {
        RaftGroupState *g;

        if (worst_gi[k] < 0)
            continue;
        g = &RaftGroups->groups[worst_gi[k]];
        appendStringInfo(&b, "| 组%lld last=%lld commit=%lld applied=%lld durable=%lld want=%lld",
                         (long long) g->group_id, (long long) g->log.last_log_index,
                         (long long) g->log.commit_index, (long long) g->log.last_applied,
                         (long long) g->log.leader_durable_index, (long long) g->log.want_plsn);
        for (slot = 0; slot < n_peers; slot++)
            if (peers[slot].node_id != pg_raft_node_id && g->log.peer_match_index[slot] > 0)
                appendStringInfo(&b, " n%d:m%lld/n%lld", peers[slot].node_id,
                                 (long long) g->log.peer_match_index[slot],
                                 (long long) g->log.peer_next_index[slot]);
    }
    {
        uint64 wn = pg_atomic_exchange_u64(&RaftGroups->stream_wait_n, 0);
        uint64 wus = pg_atomic_exchange_u64(&RaftGroups->stream_wait_us, 0);
        uint64 wmx = pg_atomic_exchange_u64(&RaftGroups->stream_wait_max, 0);

        appendStringInfo(&b, "| 提交等复制 %llu 次 均%llums 最大%llums",
                         (unsigned long long) wn,
                         (unsigned long long) (wn > 0 ? wus / wn / 1000 : 0),
                         (unsigned long long) (wmx / 1000));
    }
    elog(LOG, "pg_raft: 复制流状态 %s", b.data);
    pfree(b.data);
}

PGDLLEXPORT void pg_raft_stream_main(Datum main_arg);

void
pg_raft_stream_main(Datum main_arg)
{
    MemoryContext   iter_cxt;
    bool           *advanced;

    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    rs_cxt = AllocSetContextCreate(TopMemoryContext, "pg_raft stream", ALLOCSET_DEFAULT_SIZES);
    iter_cxt = AllocSetContextCreate(TopMemoryContext, "pg_raft stream iter", ALLOCSET_DEFAULT_SIZES);
    advanced = (bool *) MemoryContextAllocZero(rs_cxt, sizeof(bool) * Max(RAFT_MAX_GROUPS, 1));
    memset(rs_peers, 0, sizeof(rs_peers));
    memset(rs_oid_gid, 0, sizeof(rs_oid_gid));
    on_shmem_exit(rs_on_exit, (Datum) 0);
    RaftGroups->stream_latch = MyLatch;
    pg_write_barrier();
    RaftGroups->stream_pid = MyProcPid;
    elog(LOG, "pg_raft: 复制流管理进程启动（每副本一批最多 %d 条/组，在途 %d 批）",
         pg_raft_stream_batch_entries, pg_raft_stream_inflight);

    for (;;)
    {
        int     slot;
        bool    inflight = false;
        MemoryContext old;

        for (slot = 0; slot < n_peers; slot++)
            if (rs_peers[slot].qn > 0)
                inflight = true;
        (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         inflight ? 1 : 20, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();
        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        if (!pg_raft_raft_enabled)
            continue;

        rs_rc = NULL;
        MemoryContextReset(iter_cxt);
        old = MemoryContextSwitchTo(iter_cxt);
        rs_rc_reset(iter_cxt);
        PG_TRY();
        {
            parse_peers();
            rs_status_log();
            /* 收回包 → 推进 */
            for (slot = 0; slot < n_peers; slot++)
            {
                if (peers[slot].node_id == pg_raft_node_id)
                    continue;
                rs_peer_connect(slot);
                rs_peer_receive(slot);
            }
            (void) rs_advance_all(advanced);
            /* 替排不上认领位的提交者追加；发 */
            rs_append_wanted();
            for (slot = 0; slot < n_peers; slot++)
            {
                if (peers[slot].node_id == pg_raft_node_id)
                    continue;
                rs_peer_send(slot);
                if (rs_peers[slot].state == 2 && PQflush(rs_peers[slot].conn) < 0)
                    rs_peer_fail(slot, PQerrorMessage(rs_peers[slot].conn));
            }
            /* leader 自写日志行（落盘后才推进自投票上限）、apply；再推进一次 */
            {
                bool *adv2 = (bool *) palloc0(sizeof(bool) * Max(RAFT_MAX_GROUPS, 1));
                int   gi;

                rs_local_work(advanced);
                (void) rs_advance_all(adv2);
                for (gi = 0; gi < RAFT_MAX_GROUPS; gi++)
                    advanced[gi] = adv2[gi];
            }
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(iter_cxt);
            EmitErrorReport();
            FlushErrorState();
            AbortCurrentTransaction();
            raft_append_pxid = InvalidTransactionId;
            raft_batch_active = false;
            raft_batch_nrows = 0;
            pg_usleep(100000L);
        }
        PG_END_TRY();
        MemoryContextSwitchTo(old);
    }
}


void
pg_raft_partwal_replicate_many(const Oid *parts, int nparts, bool *failed_out)
{
    RmGroup        *gs;
    int             ng = 0;
    int             i, k, slot;
    StringInfoData  sql;
    bool            spi_owned;
    bool            any_failed = false;
    Oid             fail_part = InvalidOid;
    int64           fail_gid = 0;
    int64           fail_plsn = 0;
    MemoryContext   rmcxt;
    MemoryContext   rcxt;
    MemoryContext   oldcxt;
    MemoryContext   spicxt;
    int             spi_rc;
    volatile int    nclaimed = 0;
    volatile bool   normal_exit = false;

    if (!pg_raft_raft_enabled || RaftGroups == NULL || nparts <= 0)
        return;
    parse_peers();
    if (n_peers == 0)
        return;
    restore_groups_if_needed();

    /* 单副本快速模式 / 关闭 / 超大触达集：逐分区的老路径 */
    if (pg_raft_single_member_fast_commit || !pg_raft_replicate_many ||
        nparts > RM_MAX_GROUPS)
    {
        for (i = 0; i < nparts; i++)
        {
            if (failed_out != NULL)
                failed_out[i] = true;       /* 失败时 ERROR 出去，标的就是这一个 */
            pg_raft_partwal_replicate(parts[i]);
            if (failed_out != NULL)
                failed_out[i] = false;
        }
        return;
    }

    if (rs_stream_active())
    {
        pg_raft_partwal_replicate_stream(parts, nparts, failed_out);
        return;
    }
    if (pg_raft_replicate_pipeline)
    {
        pg_raft_partwal_replicate_pipe(parts, nparts, failed_out);
        return;
    }

    rmcxt = AllocSetContextCreate(CurrentMemoryContext, "pg_raft replicate_many",
                                  ALLOCSET_DEFAULT_SIZES);
    /* 每轮的读出字节、描述符、请求数组都在这里，轮首整体重置（一笔事务可能跑很多轮） */
    rcxt = AllocSetContextCreate(rmcxt, "pg_raft replicate_many round",
                                 ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(rmcxt);
    gs = (RmGroup *) palloc0(sizeof(RmGroup) * nparts);

    /*
     * ① 一次 SPI：组号与已落盘上界。
     * 内存纪律：本函数的一切分配都在 rmcxt / rcxt 里（SPI 连接期间 CurrentMemoryContext 是
     * SPI 的过程上下文，SPI_finish 时整个释放），所以 SPI 期间的分配先切回自己的上下文。
     */
    initStringInfo(&sql);
    if (!raft_persist_spi_begin(&spi_owned))
    {
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(rmcxt);
        return;
    }
    appendStringInfoString(&sql,
                           "SELECT u.p, partdist.global_id_for_partition(u.p)::bigint, "
                           "       partdist.get_partition_flush_lsn(u.p) "
                           "FROM unnest(ARRAY[");
    for (i = 0; i < nparts; i++)
        appendStringInfo(&sql, "%s%u", i ? "," : "", (unsigned) parts[i]);
    appendStringInfoString(&sql, "]::oid[]) WITH ORDINALITY u(p, o) ORDER BY u.o");
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT)
    {
        uint64 r;

        for (r = 0; r < SPI_processed; r++)
        {
            bool    n1, n2, n3;
            Datum   d1 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 1, &n1);
            Datum   d2 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 2, &n2);
            Datum   d3 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 3, &n3);

            if (n1 || n2 || DatumGetInt64(d2) <= 0)
                continue;           /* 非全局分片：不纳管 */
            gs[ng].part = DatumGetObjectId(d1);
            gs[ng].gid = DatumGetInt64(d2);
            gs[ng].cur = n3 ? 0 : DatumGetInt64(d3);
            ng++;
        }
    }
    raft_persist_spi_end(spi_owned);

    /*
     * ② 逐组门禁：与 pg_raft_partwal_replicate 相同的写栅栏与成员集要求。
     * 在这里被拒的分区与老路径一样标进 failed_out（调用方据此打分叉标记）。
     */
    k = 0;
    for (i = 0; i < ng; i++)
    {
        RmGroup *g = &gs[i];
        int      state = -1;
        bool     known;

        if (!raft_group_ctx(g->gid, &g->ctx))
            continue;               /* 无数据组：行为与接线前一致 */
        known = group_resolve_membership(&g->ctx);
        if (known)
        {
            restore_hard_state_if_needed(&g->ctx);
            restore_persistent_log_if_needed(&g->ctx);
            SpinLockAcquire(&g->ctx.cons->mutex);
            state = g->ctx.cons->state;
            SpinLockRelease(&g->ctx.cons->mutex);
        }
        if ((!known || state != RAFT_LEADER) && failed_out != NULL)
        {
            int p;

            for (p = 0; p < nparts; p++)
                if (parts[p] == g->part)
                    failed_out[p] = true;
        }
        if (!known)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: 分区 %u(组 %lld)的成员集未知，写入被拒",
                            g->part, (long long) g->gid),
                     errdetail("数据组的多数派必须按真实副本集计算；partition_map 中没有该分区的登记，"
                               "无法确定副本集。")));
        if (state != RAFT_LEADER)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: 分区 %u(组 %lld)的本地写入被拒：本节点不是该分区组的 leader",
                            g->part, (long long) g->gid),
                     errdetail("分区主副本可能已切换，请经路由层重试。")));
        if (g->cur <= 0)
            continue;
        g->majority = cluster_majority(&g->ctx);
        if (k != i)
            gs[k] = *g;
        k++;
    }
    ng = k;
    if (ng == 0)
    {
        MemoryContextSwitchTo(oldcxt);
        MemoryContextDelete(rmcxt);
        return;
    }
    qsort(gs, ng, sizeof(RmGroup), rm_group_cmp);

    PG_TRY();
    {
        /*
         * ③ 认领（2026-10-16 改）：
         *   · 排队期间不断看一眼"我的记录是否已被别人那一轮送到多数派"——组提交下这是
         *     常态，命中就一把认领位都不拿，直接返回；
         *   · 认领全有或全无：不再"拿着前几组、等后几组"（那会把只需要前几组的人也挡住，
         *     实测排队 0.5–1 s）；
         *   · 200 ms 还凑不齐就退回按组号顺序逐个认领（防饥饿；顺序认领不会互相死锁）。
         */
        {
            long    waited_us = 0;
            bool    covered = false;

            for (;;)
            {
                if (rm_all_covered(gs, ng))
                {
                    covered = true;
                    break;
                }
                if (rm_try_claim_all(gs, ng))
                {
                    nclaimed = ng;
                    break;
                }
                if (waited_us >= 200000L)
                {
                    for (i = 0; i < ng; i++)
                    {
                        raft_claim_purpose = RAFT_CLAIM_COMMIT_MANY;
                        replicate_claim(&gs[i].ctx);
                        nclaimed++;
                    }
                    break;
                }
                CHECK_FOR_INTERRUPTS();
                pg_usleep(500L);        /* 全局组表自旋锁上几百个后端轮询：别太密 */
                waited_us += 500L;
            }
            if (covered)
            {
                normal_exit = true;
                goto rm_done;
            }
        }
        in_txn_replication = true;

        /*
         * ③b 拿到认领位之后把上界刷新到此刻的落盘点（2026-10-16，组提交）。
         * ① 里的 cur 是排队之前取的；排队期间别的后端又写下了记录，它们正排在后面等同一把
         * 认领位。原来每个后端只送到自己那个陈旧的 cur，于是一个接一个各跑一轮，组提交
         * 摊不开（实测每组每秒十来条记录、认领位排队上秒）。刷新后这一轮把积压一并送走，
         * 后面的人拿到认领位发现 last ≥ 自己的 cur，直接返回。上界只升不降；多送的是
         * 已在本地流里落盘的记录，语义与别人送它们完全相同。
         */
        if (raft_persist_spi_begin(&spi_owned))
        {
            resetStringInfo(&sql);
            appendStringInfoString(&sql,
                                   "SELECT u.o::int, partdist.get_partition_flush_lsn(u.p) "
                                   "FROM unnest(ARRAY[");
            for (i = 0; i < ng; i++)
                appendStringInfo(&sql, "%s%u", i ? "," : "", (unsigned) gs[i].part);
            appendStringInfoString(&sql, "]::oid[]) WITH ORDINALITY u(p, o)");
            if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT)
            {
                uint64 r;

                for (r = 0; r < SPI_processed; r++)
                {
                    bool    n1, n2;
                    Datum   d1 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 1, &n1);
                    Datum   d2 = SPI_getbinval(SPI_tuptable->vals[r], SPI_tuptable->tupdesc, 2, &n2);
                    int     o;

                    if (n1 || n2)
                        continue;
                    o = DatumGetInt32(d1) - 1;
                    if (o >= 0 && o < ng && DatumGetInt64(d2) > gs[o].cur)
                        gs[o].cur = DatumGetInt64(d2);
                }
            }
            raft_persist_spi_end(spi_owned);
        }

        for (;;)
        {
            int     nactive = 0;
            int     maxn = 0;
            int     timeout_ms;
            TimestampTz deadline;
            bool    busy_skip[RAFT_MAX_PEERS];
            TimestampTz t0 = GetCurrentTimestamp();
            TimestampTz t_read = 0, t_persist = 0, t_send = 0, t_wait = 0;
            int     nent_round = 0;

            MemoryContextReset(rcxt);
            MemoryContextSwitchTo(rcxt);

            /* ④ 本轮区间与落后副本的补发起点 */
            for (i = 0; i < ng; i++)
            {
                RmGroup *g = &gs[i];
                int64    last = Max(rm_last_data_plsn(&g->ctx), g->done_plsn);
                bool     is_leader;

                g->active = false;
                g->acks = 0;
                g->nsent = 0;
                if (g->failed || last >= g->cur)
                    continue;
                if (!wait_for_log_room(&g->ctx))
                {
                    g->failed = true;
                    g->fail_plsn = last + 1;
                    continue;
                }
                SpinLockAcquire(&g->ctx.cons->mutex);
                is_leader = (g->ctx.cons->state == RAFT_LEADER);
                g->term = g->ctx.cons->current_term;
                SpinLockRelease(&g->ctx.cons->mutex);
                if (!is_leader)
                {
                    g->failed = true;
                    g->fail_plsn = last + 1;
                    continue;
                }
                g->lo = last + 1;
                g->hi = Min(g->cur, g->lo + pg_raft_replicate_batch_max - 1);
                g->read_lo = g->lo;
                g->active = true;
                nactive++;

                SpinLockAcquire(&g->ctx.log->mutex);
                if (!g->ctx.log->repl_inited)
                {
                    int64 nx = g->ctx.log->last_log_index + 1;
                    int   j;

                    for (j = 0; j < RAFT_MAX_PEERS; j++)
                    {
                        g->ctx.log->peer_next_index[j] = nx;
                        g->ctx.log->peer_match_index[j] = 0;
                    }
                    g->ctx.log->repl_inited = true;
                    g->ctx.log->leader_durable_index = g->ctx.log->last_log_index;   /* 当选时日志里已有的条目视为已落盘 */
                    g->ctx.log->leader_self_hole = 0;
                }
                g->cread_hi = g->lo - 1;
                {
                    int64 big_st[RAFT_MAX_PEERS];
                    int64 big_plsn[RAFT_MAX_PEERS];
                    int64 first_new = g->ctx.log->last_log_index + 1;
                    int64 kmax = (int64) pg_raft_replicate_batch_max * 2;

                    for (slot = 0; slot < n_peers; slot++)
                    {
                        int64 plsn = 0;
                        int64 nx;
                        int64 st;

                        g->catchup_from[slot] = 0;
                        g->catchup_to[slot] = 0;
                        big_st[slot] = 0;
                        big_plsn[slot] = 0;
                        if (!peer_in_group(&g->ctx, slot))
                            continue;
                        nx = g->ctx.log->peer_next_index[slot];
                        if (nx >= first_new)
                        {
                            g->catchup_from[slot] = first_new;  /* 同步中（含乐观推进） */
                            continue;
                        }
                        st = rm_catchup_start_locked(&g->ctx, slot, first_new, g->term, &plsn);
                        if (st <= 0 || plsn <= 0 || plsn >= g->lo)
                            continue;           /* 环窗口外 / 夹着别的条目：交后台追平 */
                        if (g->hi - plsn + 1 <= kmax)
                        {
                            /* 缺口小：补发段连同本轮新条目一起发（补发段一直读到 lo-1） */
                            g->catchup_from[slot] = st;
                            if (plsn < g->read_lo)
                                g->read_lo = plsn;
                        }
                        else
                        {
                            big_st[slot] = st;
                            big_plsn[slot] = plsn;
                        }
                    }

                    /*
                     * 缺口大（2026-10-16）：只补 kmax 条老条目、不带本轮新条目 —— 这条请求的确认
                     * 不计入本轮多数派，但慢副本每轮都能往前追一段，不再被甩出环窗口、落到后台
                     * 追平通道（那条通道要持着认领位做同步 RPC）。为不在一轮里读两段不相连的字节，
                     * 本轮已有小缺口补发时不做；否则只给读起点相同的那些落后副本做。
                     */
                    if (g->read_lo == g->lo)
                    {
                        int64 base = 0;

                        for (slot = 0; slot < n_peers; slot++)
                        {
                            if (big_st[slot] <= 0 || (base != 0 && big_plsn[slot] != base))
                                continue;
                            base = big_plsn[slot];
                            g->catchup_from[slot] = big_st[slot];
                            g->catchup_to[slot] = Min(big_st[slot] + kmax - 1, first_new - 1);
                            g->read_lo = base;
                            g->cread_hi = Min(base + kmax - 1, g->lo - 1);
                        }
                    }
                    else
                        g->cread_hi = g->lo - 1;
                }
                SpinLockRelease(&g->ctx.log->mutex);
            }
            if (nactive == 0)
                break;

            /* ⑤ 一次 SPI 读出各组 [read_lo, hi] 的描述与字节 */
            if (!raft_persist_spi_begin(&spi_owned))
                ereport(ERROR, (errmsg("pg_raft: 多组复制拿不到 SPI")));
            spicxt = MemoryContextSwitchTo(rcxt);
            resetStringInfo(&sql);
            appendStringInfoString(&sql,
                                   "SELECT g.k, s, r.orig_lsn::text, r.rmid, r.info, r.gxid, "
                                   "       length(r.data), r.flags, encode(r.data, 'hex') "
                                   "FROM (VALUES ");
            k = 0;
            for (i = 0; i < ng; i++)
            {
                RmGroup *g = &gs[i];

                if (!g->active)
                    continue;
                /* 补发段（read_lo..cread_hi）与本轮新条目（lo..hi）各一行，可能不相连 */
                if (g->read_lo < g->lo)
                    appendStringInfo(&sql, "%s(%d, %u::oid, %lld::bigint, %lld::bigint)",
                                     k++ ? "," : "", i, (unsigned) g->part,
                                     (long long) g->read_lo, (long long) g->cread_hi);
                appendStringInfo(&sql, "%s(%d, %u::oid, %lld::bigint, %lld::bigint)",
                                 k++ ? "," : "", i, (unsigned) g->part,
                                 (long long) g->lo, (long long) g->hi);
                g->nhex = 0;
                g->nchex = 0;
                g->nnew = 0;
                g->hex = (char **) palloc0(sizeof(char *) * (g->hi - g->lo + 1));
                g->desc = (char **) palloc0(sizeof(char *) * (g->hi - g->lo + 1));
                g->chex = (g->lo > g->read_lo)
                    ? (char **) palloc0(sizeof(char *) * (g->cread_hi - g->read_lo + 1)) : NULL;
            }
            appendStringInfoString(&sql,
                                   ") g(k, o, lo, hi) "
                                   "CROSS JOIN LATERAL generate_series(g.lo, g.hi) s "
                                   "LEFT JOIN LATERAL partdist.partwal_read_record(g.o, s) r ON true "
                                   "ORDER BY g.k, s");
            spi_rc = SPI_execute(sql.data, true, 0);
            /*
             * 2026-10-16 修：SPI_execute 返回时把当前内存上下文切回了 SPI 的过程上下文，
             * 下面拷出的十六进制载荷与描述 JSON 若分配在那里，raft_persist_spi_end() 一放
             * 就成了悬空指针 —— 随后写环、写 raft_log、发副本用的全是被覆写的内存
             * （实测："gxid":5629499534716# 截断、FILESET 交接广播 JSON 报错、副本回放全断）。
             */
            MemoryContextSwitchTo(rcxt);
            if (spi_rc == SPI_OK_SELECT)
            {
                uint64      r;
                TupleDesc   td = SPI_tuptable->tupdesc;

                for (r = 0; r < SPI_processed; r++)
                {
                    HeapTuple   tup = SPI_tuptable->vals[r];
                    bool        isnull;
                    int         gi = DatumGetInt32(SPI_getbinval(tup, td, 1, &isnull));
                    int64       plsn = DatumGetInt64(SPI_getbinval(tup, td, 2, &isnull));
                    RmGroup    *g = &gs[gi];
                    Datum       d;
                    char       *orig;
                    int         rmid, info, flags;
                    int64       gxid, nbytes;
                    StringInfoData pl;

                    /* 补发段与新条目各自要求从起点连续，前面断了后面的不算 */
                    if (plsn < g->lo ? (plsn != g->read_lo + g->nchex || plsn > g->cread_hi)
                                     : (plsn != g->lo + g->nhex))
                        continue;
                    d = SPI_getbinval(tup, td, 3, &isnull);
                    if (isnull)
                        continue;           /* 记录不存在：本组到此为止 */
                    orig = TextDatumGetCString(d);
                    d = SPI_getbinval(tup, td, 4, &isnull);  rmid = isnull ? 0 : DatumGetInt32(d);
                    d = SPI_getbinval(tup, td, 5, &isnull);  info = isnull ? 0 : DatumGetInt32(d);
                    d = SPI_getbinval(tup, td, 6, &isnull);  gxid = isnull ? 0 : DatumGetInt64(d);
                    d = SPI_getbinval(tup, td, 7, &isnull);  nbytes = isnull ? 0 : DatumGetInt32(d);
                    d = SPI_getbinval(tup, td, 8, &isnull);  flags = isnull ? PARTWAL_FLAG_DATA : DatumGetInt32(d);
                    d = SPI_getbinval(tup, td, 9, &isnull);
                    if (isnull)
                        continue;
                    if (plsn < g->lo)
                    {
                        g->chex[g->nchex++] = TextDatumGetCString(d);
                        continue;
                    }
                    g->hex[g->nhex++] = TextDatumGetCString(d);
                    {
                        initStringInfo(&pl);
                        appendStringInfo(&pl,
                                         "{\"partition_lsn\":%lld,\"orig_lsn\":\"%s\",\"rmid\":%d,"
                                         "\"info\":%d,\"flags\":%d,\"gxid\":%lld,\"nbytes\":%lld}",
                                         (long long) plsn, orig, rmid, info, flags,
                                         (long long) gxid, (long long) nbytes);
                        g->desc[g->nnew++] = pl.data;
                    }
                }
            }
            MemoryContextSwitchTo(spicxt);
            raft_persist_spi_end(spi_owned);
            t_read = GetCurrentTimestamp();

            /*
             * ⑥ 追加进环（同组认领下 index 连续）；读不到新记录 / 已不是本任期 leader 的组本轮失败。
             * 新条目记下本事务（raft_log 行随它提交）：本节点日后降为 follower、同组下一批以这些
             * 条目为 prev 到达时，rm_wait_predecessor 据此等它结束。
             */
            raft_append_pxid = GetTopTransactionId();
            for (i = 0; i < ng; i++)
            {
                RmGroup *g = &gs[i];
                int      j;
                int      room;
                bool     still;

                if (!g->active)
                    continue;
                if (g->nnew == 0)
                {
                    g->active = false;
                    g->failed = true;
                    g->fail_plsn = g->lo;
                    elog(WARNING, "pg_raft: 组 %lld 读不到本地 parwal 记录 plsn=%lld（local_oid=%u）",
                         (long long) g->gid, (long long) g->lo, g->part);
                    continue;
                }
                SpinLockAcquire(&g->ctx.cons->mutex);
                still = (g->ctx.cons->state == RAFT_LEADER &&
                         g->ctx.cons->current_term == g->term);
                SpinLockRelease(&g->ctx.cons->mutex);
                if (!still)
                {
                    g->active = false;
                    g->failed = true;
                    g->fail_plsn = g->lo;
                    continue;
                }
                SpinLockAcquire(&g->ctx.log->mutex);
                room = (int) (RAFT_LOG_CAPACITY - 1 -
                              (g->ctx.log->last_log_index - g->ctx.log->last_applied));
                if (g->nnew > room)
                    g->nnew = room;
                /* 上面核对任期之后若已失主、环尾进了新任期的条目，不许再接旧任期的 */
                if (g->ctx.log->last_log_index > 0)
                {
                    RaftLogEntry *te = log_slot(&g->ctx, g->ctx.log->last_log_index);

                    if (te->index == g->ctx.log->last_log_index && te->term > g->term)
                        g->nnew = 0;
                }
                g->first = g->ctx.log->last_log_index + 1;
                for (j = 0; j < g->nnew; j++)
                    if (log_append_locked(&g->ctx, g->term, RAFT_OP_PARWAL, g->desc[j]) <= 0)
                        break;
                g->nnew = j;
                g->last = g->first + g->nnew - 1;
                g->leader_commit = g->ctx.log->commit_index;
                /* 新条目之前的同步副本：next_index 恰为 first（④ 记的是追加前的 first_new） */
                SpinLockRelease(&g->ctx.log->mutex);
                if (g->nnew <= 0)
                {
                    g->active = false;
                    g->failed = true;
                    g->fail_plsn = g->lo;
                    continue;
                }
                if (g->nnew > maxn)
                    maxn = g->nnew;
            }

            raft_append_pxid = InvalidTransactionId;

            /* ⑦ 一次 SPI 写全部组的 raft_log 行（随本事务提交；回滚时由环补写，P7-N46） */
            if (!raft_persist_spi_begin(&spi_owned))
                ereport(ERROR, (errmsg("pg_raft: 多组复制拿不到 SPI")));
            if (raft_log_table_ready())
            {
                bool first_row = true;

                resetStringInfo(&sql);
                appendStringInfoString(&sql,
                                       "INSERT INTO partdist.raft_log "
                                       "(group_id, log_index, term, op_type, payload, committed) VALUES ");
                for (i = 0; i < ng; i++)
                {
                    RmGroup *g = &gs[i];
                    int      j;

                    if (!g->active)
                        continue;
                    if (g->ctx.g->repersist_from > 0)
                        repersist_from_ring(&g->ctx);
                    for (j = 0; j < g->nnew; j++)
                    {
                        logtrack_note(g->gid, g->first + j);
                        appendStringInfo(&sql, "%s(%lld, %lld, %lld, %s, %s::jsonb, %s)",
                                         first_row ? "" : ", ",
                                         (long long) g->gid, (long long) (g->first + j),
                                         (long long) g->term, quote_literal_cstr(RAFT_OP_PARWAL),
                                         quote_literal_cstr(g->desc[j]),
                                         g->majority == 1 ? "true" : "false");
                        first_row = false;
                    }
                }
                appendStringInfoString(&sql,
                                       " ON CONFLICT (group_id, log_index) DO UPDATE SET "
                                       "term = EXCLUDED.term, op_type = EXCLUDED.op_type, "
                                       "payload = EXCLUDED.payload, "
                                       "committed = partdist.raft_log.committed OR EXCLUDED.committed");
                if (!first_row)
                    (void) SPI_execute(sql.data, false, 0);
            }
            raft_persist_spi_end(spi_owned);
            t_persist = GetCurrentTimestamp();

            /*
             * ⑧ 每个副本节点一条多语句请求。到某副本的连接全在途（它比多数派慢）就先跳过它；
             * 跳过之后若有组一个请求都没发出去、凑不齐多数派，再回头等一条连接空出来发给它 ——
             * 宁可等慢副本，也不让本可成功的提交因为"没发"而失败（失败会打分叉标记、触发重做基线）。
             */
            timeout_ms = Min(RAFT_RPC_EXEC_TIMEOUT_MS + 300 * maxn, 30000);
            deadline = GetCurrentTimestamp() + (int64) timeout_ms * 1000;
            for (slot = 0; slot < n_peers; slot++)
            {
                busy_skip[slot] = false;
                if (peers[slot].node_id == pg_raft_node_id || peer_in_backoff(&peers[slot]))
                    continue;
                busy_skip[slot] = (rm_send_slot(gs, ng, slot, false, deadline, &sql) < 0);
            }
            for (slot = 0; slot < n_peers; slot++)
            {
                bool need = false;

                if (!busy_skip[slot])
                    continue;
                for (i = 0; i < ng && !need; i++)
                    if (gs[i].active && peer_in_group(&gs[i].ctx, slot) &&
                        gs[i].nsent + 1 < gs[i].majority)
                        need = true;
                if (need)
                    (void) rm_send_slot(gs, ng, slot, true, deadline, &sql);
            }
            t_send = GetCurrentTimestamp();

            /* ⑨ 等到每组都有多数派（或全部回齐 / 超时）；没回的副本留作在途 */
            for (;;)
            {
                bool    all_ok = true;
                bool    any_busy = false;
                struct pollfd pfds[RAFT_MAX_PEERS * RM_CONNS];
                int     npfd = 0;

                for (i = 0; i < ng; i++)
                    if (gs[i].active && gs[i].acks + 1 < gs[i].majority)
                        all_ok = false;
                if (all_ok)
                    break;

                for (slot = 0; slot < n_peers; slot++)
                    for (k = 0; k < RM_CONNS; k++)
                    {
                        RmConn *c = &rm_conns[slot][k];

                        if (!c->current)
                            continue;
                        if (c->busy && !rm_conn_poll(c, slot, false, 0))
                        {
                            int sock = PQsocket(c->conn);

                            if (GetCurrentTimestamp() >= deadline)
                            {
                                (void) rm_conn_poll(c, slot, true, deadline);   /* 判超时、重置连接 */
                            }
                            else if (sock >= 0)
                            {
                                pfds[npfd].fd = sock;
                                pfds[npfd].events = POLLIN;
                                pfds[npfd].revents = 0;
                                npfd++;
                                any_busy = true;
                                continue;
                            }
                        }
                        /* 已收完：本轮计票，再落进共享状态 */
                        {
                            int q;

                            for (q = 0; q < c->ngroups; q++)
                            {
                                int gi = c->groups[q].gslot;

                                if (gi >= 0 && gi < ng && gs[gi].active &&
                                    c->groups[q].last >= gs[gi].last &&    /* 分段补发不计票 */
                                    !c->any_error && q < c->nres && c->rok[q] &&
                                    c->rterm[q] <= gs[gi].term)
                                    gs[gi].acks++;
                            }
                            rm_conn_apply(c, slot);
                        }
                    }
                if (!any_busy)
                    break;
                (void) poll(pfds, npfd, 50);
                CHECK_FOR_INTERRUPTS();
            }
            t_wait = GetCurrentTimestamp();
            /* 本轮没回的请求留作在途：以后不再计入本轮 */
            for (slot = 0; slot < n_peers; slot++)
                for (k = 0; k < RM_CONNS; k++)
                {
                    RmConn *c = &rm_conns[slot][k];
                    int     q;

                    if (!c->current)
                        continue;
                    c->current = false;
                    for (q = 0; q < c->ngroups; q++)
                        c->groups[q].gslot = -1;
                }

            /* ⑩ 逐组提交 / 丢尾 */
            for (i = 0; i < ng; i++)
            {
                RmGroup *g = &gs[i];
                int64    committed_upto = 0;
                bool     quorum = (g->acks + 1 >= g->majority);
                bool     still;

                if (!g->active)
                    continue;
                g->ctx.g->last_rm_round = GetCurrentTimestamp();

                /*
                 * 本轮途中丢了 leader 身份 / 换了任期：环与提交点已归新任期管，这里既不推提交点
                 * 也不丢尾巴（丢的可能是新 leader 写进来的条目）。本轮拿到的多数派确认仍然成立
                 * —— 本任期的条目已在多数派持久化，按 Raft 就是已提交 —— 只是不再碰共享状态；
                 * 没拿到的按失败处理。
                 */
                SpinLockAcquire(&g->ctx.cons->mutex);
                still = (g->ctx.cons->state == RAFT_LEADER &&
                         g->ctx.cons->current_term == g->term);
                SpinLockRelease(&g->ctx.cons->mutex);
                if (still)
                {
                    RaftLogEntry *le;

                    SpinLockAcquire(&g->ctx.log->mutex);
                    le = log_slot(&g->ctx, g->last);
                    if (g->ctx.log->last_log_index < g->last ||
                        le->index != g->last || le->term != g->term)
                        still = false;
                    else
                    {
                        if (quorum && g->last > g->ctx.log->commit_index)
                            g->ctx.log->commit_index = g->last;
                        else
                        {
                            int64 nc = compute_new_commit_index(&g->ctx, g->term);

                            if (nc > g->ctx.log->commit_index)
                                g->ctx.log->commit_index = nc;
                        }
                        committed_upto = g->ctx.log->commit_index;
                    }
                    SpinLockRelease(&g->ctx.log->mutex);
                }
                if (!still)
                {
                    if (quorum)
                        g->done_plsn = g->lo + (g->last - g->first);
                    else
                    {
                        g->failed = true;
                        g->fail_plsn = g->lo;
                    }
                    continue;
                }
                persist_hard_state_unlocked(&g->ctx);

                if (committed_upto < g->last)
                {
                    int64 j;
                    int64 keep = Max(committed_upto, g->first - 1);

                    elog(LOG,
                         "pg_raft: group %lld 多组复制 idx=%lld..%lld term=%lld 多数派不足（%d/%d），丢弃未提交的尾巴",
                         (long long) g->gid, (long long) g->first, (long long) g->last,
                         (long long) g->term, g->acks + 1, g->majority);
                    for (j = g->last; j > keep; j--)
                        discard_uncommitted_entry_term(&g->ctx, j, g->term);
                    if (committed_upto < g->first)
                    {
                        g->failed = true;
                        g->fail_plsn = g->lo;
                        continue;
                    }
                    g->last = committed_upto;
                }
                if (g->majority == 1)
                {
                    if (g->last > g->ctx.g->sql_marked_upto)
                        g->ctx.g->sql_marked_upto = g->last;
                }
                else
                    mark_log_committed_sql(&g->ctx, g->last);
                g->done_plsn = g->lo + (g->last - g->first);
                if (g->done_plsn > g->ctx.g->last_data_plsn)
                    g->ctx.g->last_data_plsn = g->done_plsn;
                group_apply_pending(&g->ctx);
            }

            /* 诊断：慢轮的分段耗时（每后端每 2 s 至多一条） */
            {
                TimestampTz t_end = GetCurrentTimestamp();

                for (i = 0; i < ng; i++)
                    if (gs[i].active)
                        nent_round += gs[i].nnew;
                if (TimestampDifferenceExceeds(t0, t_end, 300))
                {
                    static TimestampTz last_log = 0;

                    if (last_log == 0 || TimestampDifferenceExceeds(last_log, t_end, 2000))
                    {
                        last_log = t_end;
                        elog(LOG, "pg_raft: [round] %d 组 %d 条 共 %ld ms：读 %ld / 写日志行 %ld / 发 %ld / 等多数派 %ld / 提交 %ld",
                             nactive, nent_round, (long) ((t_end - t0) / 1000),
                             (long) ((t_read - t0) / 1000), (long) ((t_persist - t_read) / 1000),
                             (long) ((t_send - t_persist) / 1000), (long) ((t_wait - t_send) / 1000),
                             (long) ((t_end - t_wait) / 1000));
                    }
                }
            }

            for (i = 0; i < ng; i++)
                if (gs[i].failed)
                {
                    any_failed = true;
                    fail_part = gs[i].part;
                    fail_gid = gs[i].gid;
                    fail_plsn = gs[i].fail_plsn;
                    if (failed_out != NULL)
                        for (k = 0; k < nparts; k++)
                            if (parts[k] == gs[i].part)
                                failed_out[k] = true;
                }
            if (any_failed)
                break;
        }
        normal_exit = true;
rm_done:
        ;
    }
    PG_FINALLY();
    {
        /* 本轮异常退出时，挂着的请求也不再属于任何一轮 */
        for (slot = 0; slot < RAFT_MAX_PEERS; slot++)
            for (k = 0; k < RM_CONNS; k++)
            {
                int q;

                if (!rm_conns[slot][k].current)
                    continue;
                rm_conns[slot][k].current = false;
                for (q = 0; q < rm_conns[slot][k].ngroups; q++)
                    rm_conns[slot][k].groups[q].gslot = -1;
            }
        /*
         * 异常退出（取消、SPI 出错……）：还没复制到 cur 的分区与老路径一样标进 failed_out ——
         * 它们的字节已在本地流里，调用方据此判断要不要打分叉标记。
         */
        if (!normal_exit && failed_out != NULL)
            for (i = 0; i < ng; i++)
                if (Max(rm_last_data_plsn(&gs[i].ctx), gs[i].done_plsn) < gs[i].cur)
                    for (k = 0; k < nparts; k++)
                        if (parts[k] == gs[i].part)
                            failed_out[k] = true;
        raft_append_pxid = InvalidTransactionId;
        in_txn_replication = false;
        for (i = nclaimed - 1; i >= 0; i--)
            replicate_release(&gs[i].ctx);
        MemoryContextSwitchTo(oldcxt);
    }
    PG_END_TRY();
    MemoryContextDelete(rmcxt);

    if (any_failed)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 分区 %u(组 %lld) record %lld 未达多数派",
                        fail_part, (long long) fail_gid, (long long) fail_plsn)));
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

        hard_state_hint_path(&ctx, path, sizeof(path));
        (void) unlink(path);
        hard_state_path(&ctx, path, sizeof(path));
        if (unlink(path) != 0 && errno != ENOENT)
            elog(WARNING, "pg_raft: 删除 hardstate 文件 \"%s\" 失败: %m", path);

        SpinLockAcquire(&RaftGroups->mutex);
        g->in_use = false;
        RaftGroups->n_groups--;
        SpinLockRelease(&RaftGroups->mutex);
        raft_tombstone_add(ctx.group_id);   /* P7-N21 */
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
static struct
{
    bool    set;
    int64   dtxid;
    int     verdict;
    int64   commit_ts;
} pending_bcast;

static int64
dtx_write_decision(RaftGroupCtx *ctx, int64 local_oid, int64 dtxid,
                   int32 verdict, uint64 commit_ts,
                   const char *participants_sql, int64 anchor_xid)
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
                         "(dtxid, coord_gsid, verdict, commit_ts, participants, decided_plsn, anchor_xid) "
                         "VALUES (%lld, %lld, %d, %llu, %s, %lld, %lld) "
                         "ON CONFLICT (dtxid) DO NOTHING",
                         (long long) dtxid, (long long) ctx->group_id, verdict,
                         (unsigned long long) commit_ts, participants_sql,
                         (long long) plsn, (long long) anchor_xid);
        (void) SPI_execute(sql.data, false, 0);
        pfree(sql.data);
        raft_persist_spi_end(spi_owned);
    }

    /*
     * T4.5②：决议主动广播（§3.3）。**在提交点之后**触发，纯优化——推不到
     * 的参与者由读者问询/清扫拉取收敛。实现在 pg_partdist（经 rendezvous
     * "partdist_dtx_broadcast_fn"），失败一律吞掉，绝不影响已提交事务。
     */
    {
        static void **bcast_rv = NULL;

        if (bcast_rv == NULL)
            bcast_rv = find_rendezvous_variable("partdist_dtx_broadcast_fn");
        /*
         * 2026-10-18（方案 B）：待发起方确认的 COMMIT（verdict=3）不广播 —— 它此刻还不是
         * 判决。生效后由协调组节点上的 COMMIT PREPARED 钩子推送（PartDistDtxOnFinishPrepared），
         * 或由恢复守护问到发起方后经 dtx_status 推送。
         */
        if (*bcast_rv != NULL && (verdict == 1 || verdict == 2))
        {
            /*
             * ★ TPC-C 分支：不在这里发 —— 调用方此刻还持着协调组的复制认领位，
             * 广播是对每个参与者的远程 RPC，夹在认领位里会把本组所有决议串在别人的
             * 网络往返后面（128 终端实测 dtx_decide 的认领位等待占 worker 采样近 1/5）。
             * 暂存，由调用方放掉认领位后 dtx_flush_pending_broadcast() 发出。
             */
            pending_bcast.set = true;
            pending_bcast.dtxid = dtxid;
            pending_bcast.verdict = (int) verdict;
            pending_bcast.commit_ts = (int64) commit_ts;
        }
    }

    return plsn;
}

/* 发出 dtx_write_decision 暂存的决议广播（尽力而为，失败吞掉）；调用方须已放掉认领位 */
static void
dtx_flush_pending_broadcast(void)
{
    static void **bcast_rv = NULL;

    if (!pending_bcast.set)
        return;
    pending_bcast.set = false;
    if (bcast_rv == NULL)
        bcast_rv = find_rendezvous_variable("partdist_dtx_broadcast_fn");
    if (*bcast_rv != NULL)
    {
        void (*bcast) (int64, int, int64) =
            (void (*) (int64, int, int64)) *bcast_rv;
        MemoryContext bcxt = CurrentMemoryContext;

        PG_TRY();
        {
            bcast(pending_bcast.dtxid, pending_bcast.verdict, pending_bcast.commit_ts);
        }
        PG_CATCH();
        {
            ErrorData  *ed;

            MemoryContextSwitchTo(TopMemoryContext);
            ed = CopyErrorData();
            FlushErrorState();  /* 广播是尽力而为，绝不打穿提交路径 */
            MemoryContextSwitchTo(bcxt);
            elog(LOG, "pg_raft: 决议广播失败（dtxid=%lld）：%s",
                 (long long) pending_bcast.dtxid, ed->message);
            FreeErrorData(ed);
        }
        PG_END_TRY();
    }
}

/*
 * 查已有决议；返回**有效** verdict：0 = 尚无决议，1/2 = COMMIT/ABORT，
 * 3 = 待发起方确认的 COMMIT 尚未生效（2026-10-18 方案 B：verdict=3 按锚点
 * prepared 事务的结局折算，见 partdist.dtx_verdict_effective）。
 */
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
                     "SELECT partdist.dtx_verdict_effective(verdict, anchor_xid) "
                     "  FROM partdist.dtx_decision WHERE dtxid = %lld",
                     (long long) dtxid);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);
        if (!isnull)
            verdict = DatumGetInt32(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return verdict;
}

/*
 * 2026-10-18（方案 B）：待确认 COMMIT 的锚点 —— 本节点上这笔分布式事务的 prepared
 * 事务（full xid）。协调组取自远端参与者的写集，它的 leader 就是写了该组分片、
 * 正持着 prepared 事务的那台节点。多条连接（多个 gid）时优先取写过协调组的那条，
 * 同一节点上的几条由 Citus 一起 COMMIT / ROLLBACK PREPARED，取哪条结局都一样。
 * xid 取自参与登记（PREPARE 时由 prepare 的那个后端顺手记下，见 pg_raft_dtx_note_participant），
 * 不扫 pg_prepared_xacts —— 这里在发起方持热行锁的提交路径上、还占着协调组认领位。
 * 只认仍在进行中（= 还 prepared 着）的：已被回滚的不能当锚点。
 * 返回 0 = 本节点没有这笔事务的 prepared 事务（调用方按 ABORT 处理）。
 */
static int64
dtx_local_anchor_xid(int64 dtxid, int64 coord_gsid)
{
    StringInfoData sql;
    bool           spi_owned;
    int64          anchor = 0;

    if (!raft_persist_spi_begin(&spi_owned))
        return 0;
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT p.local_xid FROM partdist.dtx_participant p "
                     " WHERE p.dtxid = %lld AND p.local_xid > 0 "
                     "   AND pg_catalog.pg_xact_status(p.local_xid::text::xid8) = 'in progress' "
                     " ORDER BY (%lld = ANY (p.gsids)) DESC, p.gid LIMIT 1",
                     (long long) dtxid, (long long) coord_gsid);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);

        if (!isnull)
            anchor = DatumGetInt64(d);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);
    return anchor;
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
 *
 * 2026-10-18（方案 B）：verdict=3 = COMMIT 待发起方确认。发起方在本地提交记录之前
 * 只能写这一档（写 1 正是原子性缺陷的成因）。记下本节点这笔事务的 prepared 事务作
 * 锚点后返回 3；本节点已经没有它的 prepared 事务（被恢复守护按推定中止回滚了）就
 * 写 ABORT 并返回 2，发起方随之中止。verdict=1 的直接 COMMIT 只留给机制测试的
 * 自造 gid（发起方没有本地写）。
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
    int64        anchor_xid = 0;

    if (verdict != 1 && verdict != 2 && verdict != 3)
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("pg_raft: verdict 必须是 1(COMMIT)、2(ABORT) 或 3(COMMIT 待发起方确认)")));

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
    commit_ts = (verdict == 1 || verdict == 3)
        ? (p_commit_ts > 0 ? (uint64) p_commit_ts : (uint64) GetCurrentTimestamp())
        : 0;
    parts_sql = dtx_participants_sql(parts);

    raft_claim_purpose = RAFT_CLAIM_DTX_DECIDE;
    replicate_claim(&ctx);
    in_txn_replication = true;
    PG_TRY();
    {
        /* 进入临界区后再查一次：等待期间可能已被恢复守护写了决议 */
        existing = dtx_lookup_decision(dtxid);
        if (existing == 0 && verdict == 3)
        {
            /*
             * 锚点在认领位里取：恢复守护的推定中止（dtx_status）也要先拿认领位、
             * 先写 ABORT 才回滚 prepared，所以这里看得到 prepared ⇒ 它不会再被
             * 推定中止回滚；看不到 ⇒ 已被回滚（或从未存在），只能 ABORT。
             */
            anchor_xid = dtx_local_anchor_xid(dtxid, coord_gsid);
            if (anchor_xid <= 0)
            {
                elog(LOG, "pg_raft: dtx %lld 在协调组 %lld 的节点上找不到 prepared 事务，判 ABORT",
                     (long long) dtxid, (long long) coord_gsid);
                verdict = 2;
                commit_ts = 0;
            }
        }
        if (existing == 0)
            (void) dtx_write_decision(&ctx, local_oid, dtxid, verdict,
                                      commit_ts, parts_sql, anchor_xid);
        else
            verdict = existing;
    }
    PG_FINALLY();
    {
        in_txn_replication = false;
        replicate_release(&ctx);
    }
    PG_END_TRY();
    dtx_flush_pending_broadcast();

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
/*
 * pg_raft_group_drain_apply(group_id) → int
 *
 * R-P4-12（2026-08-15 解冻批次 #4）：把**本节点自己**这一组的 apply 积压
 * 排空，返回排空后的 last_applied。
 *
 * 为什么需要：杀掉协调组 leader 后，新当选的 leader 可能正是**尚未 apply
 * 该决议**的成员（决议在多数派上、选举合法）。此时它的本地
 * partdist.dtx_decision 里没有那一行，dtx_peek 的 leader 门控又只认本地表
 * ⇒ 谁都问不到判决，in-doubt 收敛被拖到"选举 + 追平"之外（实测 90s 未收敛）。
 *
 * pg_raft_catchup() 治不了这个：它是 leader→follower 的**补发**通道，推的是
 * 别人，推不动 leader 自己。这里直接调 group_apply_pending —— 它本就是
 * "把已提交但未 apply 的条目按序 apply 掉"，对 leader 同样适用，且幂等。
 *
 * 由 dtx_peek 在读表**之前**调用：应答前先把自己追平，答案才是权威的。
 */
/*
 * pg_raft_group_peer_decision(group_id, dtxid) → int（verdict，0=未知）
 *
 * R-P4-13 绕行（2026-08-15）：本地决议行缺失时，向**组内其他成员**只读拉取
 * 该决议的 verdict/commit_ts。
 *
 * 背景：决议条目在多数派上、日志游标也全 applied，但个别成员的本地
 * partdist.dtx_decision 仍可能缺行（R-P4-13，apply 侧未走 DTX 登记分支，
 * 根因待查）。此时若它恰好当选 leader，dtx_peek 的 leader 门控只认本地表
 * ⇒ 无人可答。既然决议本身在多数派上是**确定的事实**，就直接去问成员 ——
 * 这不改变正确性边界（只读、不写推定中止、不改变"决议一次性"语义），
 * 只是把"本地有没有那一行"从可用性的单点依赖上摘掉。
 *
 * 只读远端 partdist.dtx_decision（不调 dtx_peek，避免递归与 leader 门控）。
 * 任一成员答出 1/2 即返回；全部不可达或都没有该行则返回 0。
 */
PG_FUNCTION_INFO_V1(pg_raft_group_peer_decision);

Datum
pg_raft_group_peer_decision(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    int64        dtxid = PG_GETARG_INT64(1);
    RaftGroupCtx ctx;
    int          verdict = 0;
    int64        cts = 0;
    int          i;

    if (!pg_raft_raft_enabled || RaftGroups == NULL || dtxid == 0)
        PG_RETURN_INT32(0);

    parse_peers();
    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_INT32(0);
    if (!group_resolve_membership(&ctx))
        PG_RETURN_INT32(0);

    for (i = 0; i < n_peers && verdict == 0; i++)
    {
        char        conninfo[256];
        char        qry[192];
        PGconn     *conn;
        PGresult   *res;

        if (!peer_in_group(&ctx, i))
            continue;
        if (peers[i].node_id == pg_raft_node_id)
            continue;           /* 本地已经查过了，调用方才来问的 */

        pg_raft_format_conninfo(peers[i].host, peers[i].port,
                                conninfo, sizeof(conninfo));
        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) != CONNECTION_OK)
        {
            PQfinish(conn);
            continue;           /* 不可达就问下一个 */
        }
        /* 待确认的 COMMIT 在对端按它自己的锚点折算（2026-10-18 方案 B） */
        snprintf(qry, sizeof(qry),
                 "SELECT partdist.dtx_verdict_effective(verdict, anchor_xid), commit_ts "
                 "  FROM partdist.dtx_decision WHERE dtxid = %lld", (long long) dtxid);
        res = PQexec(conn, qry);
        if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
            !PQgetisnull(res, 0, 0))
        {
            verdict = atoi(PQgetvalue(res, 0, 0));
            if (!PQgetisnull(res, 0, 1))
                cts = strtoll(PQgetvalue(res, 0, 1), NULL, 10);
        }
        PQclear(res);
        PQfinish(conn);
    }

    if (verdict != 1 && verdict != 2)
        PG_RETURN_INT32(0);

    /*
     * 顺带把学到的决议补进本地表（幂等、ON CONFLICT DO NOTHING）——
     * 下次就不必再远程问一趟。失败不影响返回值。
     */
    {
        StringInfoData sql;
        bool           spi_owned;

        if (raft_persist_spi_begin(&spi_owned))
        {
            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "INSERT INTO partdist.dtx_decision"
                             "(dtxid, coord_gsid, verdict, commit_ts, participants, decided_plsn) "
                             "VALUES (%lld, %lld, %d, %lld, '{}'::bigint[], 0) "
                             "ON CONFLICT (dtxid) DO NOTHING",
                             (long long) dtxid, (long long) group_id,
                             verdict, (long long) cts);
            (void) SPI_execute(sql.data, false, 0);
            pfree(sql.data);
            raft_persist_spi_end(spi_owned);
        }
    }

    PG_RETURN_INT32(verdict);
}

PG_FUNCTION_INFO_V1(pg_raft_group_drain_apply);

Datum
pg_raft_group_drain_apply(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    RaftGroupCtx ctx;
    int64        applied = -1;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_INT64(-1);

    parse_peers();
    restore_groups_if_needed();
    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_INT64(-1);        /* 本节点没有该组：无从追平，静默 */

    restore_hard_state_if_needed(&ctx);
    restore_persistent_log_if_needed(&ctx);

    /*
     * 排空自身积压。group_apply_pending 内部自带认领与错误处置（#39），
     * 这里不额外包 PG_TRY —— 让它的既有语义原样生效。
     */
    group_apply_pending(&ctx);

    SpinLockAcquire(&ctx.log->mutex);
    applied = ctx.log->last_applied;
    SpinLockRelease(&ctx.log->mutex);

    PG_RETURN_INT64(applied);
}

/*
 * 2026-10-18（方案 B）：问发起方（2PC 驱动者）它那笔本地事务的结局。
 *
 * Citus gid = citus_<group>_<pid>_<txn>_<conn>：group 定位发起节点（pg_dist_node，
 * 切主后由 citus_update_node 改到新主），pid + txn 定位发起 backend 上的那笔分布式事务。
 *   1) 那笔事务还在跑（get_all_active_transactions 里有 pid + txn）⇒ 0（未决）；
 *   2) 否则**另起语句**看本地 pg_dist_transaction 里这笔事务的行：Citus 在发起方本地
 *      事务里写这些行，行与本地提交同生共死 —— 在 ⇒ 1（COMMIT），不在 ⇒ 2（ABORT）。
 *   先 1 后 2 是正确性的一部分：反过来就有"查行时还没提交、查活跃时刚提交完"的窗口，
 *   会把已提交答成 ABORT。行的 GC（dtx_gc_dist_transaction）只删全网已无该 gid
 *   prepared 事务的行，而来问的协调组节点正持着这笔事务的 prepared 事务，答复前删不掉。
 * 答 COMMIT 之前，在同一会话里提交一笔带 xid 的空事务（同步提交）：它的提交记录排在
 * 发起方那条之后，刷盘 / 同步备库确认了它，发起方的提交也就持久了 —— 不会把一笔随后
 * 因切主丢失的提交答成 COMMIT。
 * 连不上、超时、任何失败 ⇒ 0：调用方保持未决，下轮再问，绝不擅自决定。
 */
/* 持久化栅栏期间收到 WARNING（同步复制等待被取消：本地已提交、未必已复制）就不算数 */
static void
dtx_notice_warned(void *arg, const PGresult *res)
{
    const char *sev = PQresultErrorField(res, PG_DIAG_SEVERITY_NONLOCALIZED);

    if (sev != NULL && strcmp(sev, "WARNING") == 0)
        *(bool *) arg = true;
}

static int
dtx_ask_driver(long long g_group, long long g_pid, long long g_txn)
{
    StringInfoData sql;
    bool      spi_owned;
    bool      have_node = false;
    char      host[NAMEDATALEN];
    int       port = 0;
    char      conninfo[512];
    char      qry[256];
    PGconn   *conn;
    PGresult *res;
    int       verdict = 0;
    bool      active = true;
    bool      warned = false;
    long long nrows = -1;

    if (g_group < 0 || g_pid <= 0 || g_txn <= 0)
        return 0;

    /* 1) 发起节点地址 */
    if (!raft_persist_spi_begin(&spi_owned))
        return 0;
    if (SPI_execute("SELECT to_regclass('pg_catalog.pg_dist_node') IS NOT NULL",
                    true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

        have_node = !isnull && DatumGetBool(d);
    }
    if (have_node)
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "SELECT nodename, nodeport FROM pg_catalog.pg_dist_node "
                         " WHERE groupid = %lld AND noderole = 'primary' "
                         " ORDER BY isactive DESC, nodeid LIMIT 1", g_group);
        have_node = false;
        if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
        {
            char *h = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
            char *pt = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2);

            if (h != NULL && pt != NULL && atoi(pt) > 0)
            {
                strlcpy(host, h, sizeof(host));
                port = atoi(pt);
                have_node = true;
            }
        }
        pfree(sql.data);
    }
    raft_persist_spi_end(spi_owned);
    if (!have_node)
        return 0;

    pg_raft_format_conninfo(host, port, conninfo, sizeof(conninfo));
    strlcat(conninfo, " options='-c statement_timeout=5000'", sizeof(conninfo));
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        PQfinish(conn);
        return 0;
    }

    /* 2) 还在跑吗（查不动 get_all_active_transactions 就退回 pid 存活，保守方向） */
    snprintf(qry, sizeof(qry),
             "SELECT count(*) FROM get_all_active_transactions() "
             " WHERE process_id = %lld AND transaction_number = %lld",
             g_pid, g_txn);
    res = PQexec(conn, qry);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        PQclear(res);
        snprintf(qry, sizeof(qry),
                 "SELECT count(*) FROM pg_stat_activity WHERE pid = %lld", g_pid);
        res = PQexec(conn, qry);
    }
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
        !PQgetisnull(res, 0, 0))
        active = (atoll(PQgetvalue(res, 0, 0)) > 0);
    PQclear(res);
    if (active)
    {
        PQfinish(conn);
        return 0;
    }

    /* 3) 本地行在不在（新快照）；答 COMMIT 前先过一道持久化栅栏 */
    res = PQexec(conn, "BEGIN ISOLATION LEVEL READ COMMITTED");
    if (PQresultStatus(res) == PGRES_COMMAND_OK)
    {
        PQclear(res);
        snprintf(qry, sizeof(qry),
                 "SELECT count(*) FROM pg_catalog.pg_dist_transaction "
                 " WHERE gid LIKE 'citus\\_%lld\\_%lld\\_%lld\\_%%'",
                 g_group, g_pid, g_txn);
        res = PQexec(conn, qry);
        if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
            !PQgetisnull(res, 0, 0))
            nrows = atoll(PQgetvalue(res, 0, 0));
        PQclear(res);
        if (nrows > 0)
        {
            res = PQexec(conn, "SELECT pg_current_xact_id()");
            if (PQresultStatus(res) != PGRES_TUPLES_OK)
                nrows = -1;
            PQclear(res);
        }
        (void) PQsetNoticeReceiver(conn, dtx_notice_warned, &warned);
        res = PQexec(conn, "COMMIT");
        if (PQresultStatus(res) == PGRES_COMMAND_OK && nrows >= 0 && !warned)
            verdict = (nrows > 0) ? 1 : 2;
    }
    PQclear(res);
    PQfinish(conn);

    if (verdict != 0)
        elog(LOG, "pg_raft: dtx 问发起方（组 %lld、pid %lld、txn %lld）：本地事务%s",
             g_group, g_pid, g_txn, (verdict == 1) ? "已提交 ⇒ COMMIT" : "未提交 ⇒ ABORT");
    return verdict;
}

/* dtxid 打包的是 citus gid 的前三段（group 8 位 | pid 22 位 | txn 33 位，见 dtx_dtxid_from_gid） */
static int
dtx_ask_driver_of_dtxid(int64 dtxid)
{
    return dtx_ask_driver((long long) ((((uint64) dtxid) >> 55) & 0xFF),
                          (long long) ((((uint64) dtxid) >> 33) & 0x3FFFFF),
                          (long long) (((uint64) dtxid) & 0x1FFFFFFFFLL));
}

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
    if (existing == 3)
    {
        /*
         * 2026-10-18（方案 B）：待发起方确认的 COMMIT 迟迟没生效（锚点 prepared 事务还在）
         * —— 发起方崩溃、或 COMMIT PREPARED 没送到。问发起方本地事务的结局，问到就返回
         * 并推给参与方；问不到返回 0，恢复守护保持 prepared，下轮再问。
         *
         * ★ 结论**不写回决议行**。初版在这里 UPDATE 成 1/2，实测自死锁：恢复守护一轮
         * 在同一个事务里先调本函数（行锁一直持到这轮结束），再经 libpq 连回本机调
         * dtx_ack 改同一行 —— 两个会话互等，PG 的死锁检测看不见，守护从此卡死、
         * 后面的 in-doubt 全没人收尾。不写回也不丢正确性：本节点的锚点 prepared 事务
         * 随后由恢复守护按这个结论提交 / 回滚，判决随锚点生效；锚点有结局之前，发起方
         * pg_dist_transaction 里这笔事务的行回收不掉（见 dtx_ask_driver），再问还是同一个答案。
         */
        int v = dtx_ask_driver_of_dtxid(dtxid);

        if (v != 1 && v != 2)
            PG_RETURN_INT32(0);
        elog(LOG, "pg_raft: dtx %lld 待确认判决经问发起方定为 %s（协调组 %lld）",
             (long long) dtxid, (v == 1) ? "COMMIT" : "ABORT", (long long) coord_gsid);
        {
            StringInfoData qsql;
            bool           spi_owned;
            int64          cts = 0;

            if (v == 1 && raft_persist_spi_begin(&spi_owned))
            {
                initStringInfo(&qsql);
                appendStringInfo(&qsql,
                                 "SELECT commit_ts FROM partdist.dtx_decision WHERE dtxid = %lld",
                                 (long long) dtxid);
                if (SPI_execute(qsql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
                {
                    bool  isnull;
                    Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                            1, &isnull);

                    if (!isnull)
                        cts = DatumGetInt64(d);
                }
                pfree(qsql.data);
                raft_persist_spi_end(spi_owned);
            }
            if (v == 2 || cts > 0)
            {
                pending_bcast.set = true;
                pending_bcast.dtxid = dtxid;
                pending_bcast.verdict = v;
                pending_bcast.commit_ts = (v == 1) ? cts : 0;
                dtx_flush_pending_broadcast();
            }
        }
        PG_RETURN_INT32(v);
    }
    if (existing != 0)
        PG_RETURN_INT32(existing);

    parts_sql = dtx_participants_sql(NULL);
    raft_claim_purpose = RAFT_CLAIM_DTX_STATUS;
    replicate_claim(&ctx);
    in_txn_replication = true;
    PG_TRY();
    {
        existing = dtx_lookup_decision(dtxid);
        if (existing == 0)
        {
            (void) dtx_write_decision(&ctx, local_oid, dtxid, 2 /* ABORT */,
                                      0, parts_sql, 0);
            existing = 2;
        }
        else if (existing == 3)
            existing = 0;       /* 等待认领位期间发起方刚写下待确认 COMMIT：本轮按未决答 */
    }
    PG_FINALLY();
    {
        in_txn_replication = false;
        replicate_release(&ctx);
    }
    PG_END_TRY();
    dtx_flush_pending_broadcast();

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
    long long g_group = 0, g_pid = 0, g_txn = 0, g_conn = 0;

    /*
     * 2026-10-18（方案 B）：改为问**发起节点**（gid 第一段的 group），不再固定问
     * Citus 协调者。函数下推后 2PC 由 worker 驱动，pg_dist_transaction 的行写在那台
     * worker 本地；原来去协调者上查 pid 与行，pid 栅栏查错了机器、行也永远查不到 ——
     * 发起方只是慢（还没走到下发协调组）时会被误判 ABORT 回滚，发起方随后照常提交，
     * 就是一半提交。存活栅栏也从"pid 还在"改成"那笔分布式事务还在跑"（池化连接的
     * backend 常驻不退，按 pid 判会让收尾一直拖着）。顺序与持久化栅栏见 dtx_ask_driver。
     */
    if (sscanf(gid, "citus_%lld_%lld_%lld_%lld", &g_group, &g_pid, &g_txn, &g_conn) != 4)
        return 0;
    return dtx_ask_driver(g_group, g_pid, g_txn);
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

        /*
         * ★ R-P4-9（2026-08-14 解冻批次 #3）：**闭合之前**先把判决落进
         * 本节点的 TX2 分片 clog。
         *
         * 缺此一步的实测形态：参与者崩溃且本组已切主时，守护问到决议并
         * COMMIT PREPARED 闭合成功，但下面补标记的本地写被写栅栏拒
         * （非 leader 不得本地写）——判决没进分片 clog，而原生 prepared
         * 已消失、2PC 段随之作废、未决登记失去载体，那些行**永久不可见**。
         *
         * 顺序必须是"先落账后闭合"：落账失败可整轮重来（登记还在），
         * 闭合后失败则无从补救。落账本身幂等（与广播/清扫同一函数）。
         * 未装 pg_partdist / 遗留模式（登记恒空）时该调用返回 0，行为不变。
         */
        {
            static int (**apply_rv)(int64, int, int64) = NULL;

            if (apply_rv == NULL)
                apply_rv = (int (**)(int64, int, int64))
                    find_rendezvous_variable("partdist_dtx_apply_decision_fn");
            if (*apply_rv != NULL && dtxid != 0)
            {
                int   n_applied = 0;
                int64 dcts = 0;
                bool  ts_isnull = true;

                /*
                 * commit_ts 从**本地** dtx_decision 取：决议条目经组日志
                 * 复制、各成员 apply 时各自登记，所以本节点若参与过这笔
                 * 事务、其协调组决议必已 apply 到本地（§6.2）。取不到就
                 * 传 0——落账侧对 ABORT 本就忽略 ts，对 COMMIT 传 0 会被
                 * 视为遗留语义（对一切快照可见），因此**取不到时不落账**，
                 * 留给拉取通道带着正确 ts 收敛。
                 */
                initStringInfo(&sql);
                appendStringInfo(&sql,
                                 "SELECT commit_ts FROM partdist.dtx_decision "
                                 " WHERE dtxid = %lld", (long long) dtxid);
                if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT &&
                    SPI_processed > 0)
                {
                    Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                            SPI_tuptable->tupdesc, 1,
                                            &ts_isnull);

                    if (!ts_isnull)
                        dcts = DatumGetInt64(d);
                }
                pfree(sql.data);

                /*
                 * 本地取不到 ⇒ 向协调组 leader 补取一次（决议本就是问来的，
                 * dtx_peek 是 T4.5 的只读窥视口，绝不写推定中止）。
                 * 实测必要性：参与者崩溃重启后本地 dtx_decision 尚未 apply
                 * 回来（决议写在切主后的新 leader 上），只查本地必然落空，
                 * 判决就永远落不进分片 clog（R-P4-9 首版即栽在这里）。
                 */
                if (verdict == 1 && dcts <= 0 && coord_gsid > 0)
                {
                    int   cnode = 0;
                    int   cslot = -1;
                    int   ci;

                    initStringInfo(&sql);
                    appendStringInfo(&sql,
                                     "SELECT primary_node FROM partdist.partition_map "
                                     " WHERE partition_id = %llu::oid",
                                     (unsigned long long) coord_gsid);
                    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT &&
                        SPI_processed > 0)
                    {
                        Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                                SPI_tuptable->tupdesc, 1,
                                                &ts_isnull);

                        if (!ts_isnull)
                            cnode = DatumGetInt32(d);
                    }
                    pfree(sql.data);

                    for (ci = 0; ci < n_peers && cnode > 0; ci++)
                        if (peers[ci].node_id == cnode)
                        {
                            cslot = ci;
                            break;
                        }
                    if (cslot >= 0 || cnode == pg_raft_node_id)
                    {
                        char      pconn[256];
                        char      pqry[192];
                        PGconn   *pc;
                        PGresult *pres;

                        if (cnode == pg_raft_node_id)
                            pg_raft_format_conninfo("127.0.0.1", PostPortNumber,
                                                    pconn, sizeof(pconn));
                        else
                            pg_raft_format_conninfo(peers[cslot].host,
                                                    peers[cslot].port,
                                                    pconn, sizeof(pconn));
                        pc = PQconnectdb(pconn);
                        if (PQstatus(pc) == CONNECTION_OK)
                        {
                            snprintf(pqry, sizeof(pqry),
                                     "SELECT commit_ts FROM partdist.dtx_peek(%lld, %lld) WHERE verdict IN (1, 2)",
                                     (long long) coord_gsid, (long long) dtxid);
                            pres = PQexec(pc, pqry);
                            if (PQresultStatus(pres) == PGRES_TUPLES_OK &&
                                PQntuples(pres) == 1 && !PQgetisnull(pres, 0, 0))
                                dcts = strtoll(PQgetvalue(pres, 0, 0), NULL, 10);
                            PQclear(pres);
                        }
                        PQfinish(pc);
                    }
                }

                /*
                 * ★ 落账拿不到 commit_ts（协调组正在切主、dtx_peek 的 leader
                 * 门控合法拒答等）⇒ **本轮不闭合**，留着 prepared 与登记，
                 * 守护下一轮重来。闭合是不可逆的（原生 prepared 一消失，
                 * 2PC 段作废、登记失去载体），而多等一轮只是延迟——两害相权，
                 * 宁可慢，不可丢。ABORT 不需要 ts，照常闭合。
                 */
                /*
                 * 2026-10-17：Citus 原生 gid、本节点没有登记任何分片写集（coord_gsid 为 0、
                 * mygsids 为空）—— 例如 citus_set_coordinator_host / citus_update_node 这类
                 * 只改 Citus 目录的分布式事务。它没有分片 xid 要落判决，也就不需要 commit_ts；
                 * 按 Citus 规则（pg_dist_transaction）问到的 COMMIT 直接闭合。
                 * 原来这里同样"取不到 commit_ts ⇒ 本轮不闭合"，而这类事务永远取不到，
                 * prepared 连同目录行锁永久滞留（协调者切主实验里 pg_dist_node 被锁死）。
                 */
                if (verdict == 1 && dcts <= 0 && coord_gsid <= 0 && nmygsids == 0)
                {
                    elog(LOG,
                         "pg_raft: dtx 恢复：dtxid=%lld 无分片写集（Citus 目录事务），按 Citus 决议直接闭合",
                         (long long) dtxid);
                }
                else if (verdict == 1 && dcts <= 0)
                {
                    elog(LOG,
                         "pg_raft: dtx 恢复：dtxid=%lld 取不到 commit_ts，"
                         "本轮不闭合（保留 prepared 与登记，下轮重试）",
                         (long long) dtxid);
                    raft_persist_spi_end(spi_owned);
                    continue;
                }

                if (verdict == 2 || dcts > 0)
                {
                    PG_TRY();
                    {
                        n_applied = (*apply_rv)(dtxid, (int) verdict, dcts);
                    }
                    PG_CATCH();
                    {
                        FlushErrorState();  /* 落账失败不阻断闭合：下轮重来 */
                        n_applied = -1;
                    }
                    PG_END_TRY();
                    if (n_applied > 0)
                        elog(LOG,
                             "pg_raft: dtx 恢复：dtxid=%lld 判决先落 TX2 分片 clog"
                             "（%d 笔，cts=%lld）",
                             (long long) dtxid, n_applied, (long long) dcts);
                }
            }
        }

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
    /*
     * ★ TPC-C 分支：每个节点、每轮都跑（函数下推后 2PC 驱动者是 worker，行写在
     * worker 本地的 pg_dist_transaction 里）。原先"static 计数满 6 轮才跑一次"
     * 从未触发过：恢复守护每轮经**新建的**自连接调用本函数，计数在新后端里
     * 永远从 0 开始。
     */
    (void) dtx_gc_dist_transaction();

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

    /* 单副本快速提交：单成员组不提议，没有 apply 来删行 —— leader 直接删 */
    if (pg_raft_single_member_fast_commit && group_cluster_size(ctx) == 1 &&
        raft_persist_spi_begin(&spi_owned))
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "DELETE FROM partdist.dtx_decision WHERE dtxid = %lld",
                         (long long) dtxid);
        (void) SPI_execute(sql.data, false, 0);
        pfree(sql.data);
        raft_persist_spi_end(spi_owned);
    }
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
                     " RETURNING acked @> participants AND participants <> '{}'::bigint[] "
                     "       AND partdist.dtx_verdict_effective(verdict, anchor_xid) IN (1, 2)",
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
        raft_claim_purpose = RAFT_CLAIM_DTX_ACK;
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
    bool         (*dtx_pending_check)(int64) = NULL;

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

    /*
     * R-P4-5（2026-08-14 解冻批次 #2）：回执后移。tx 时代的闭合判据是
     * "原生 prepared 已不在"，但 TX2 的分片 clog 终局要靠决议收敛——
     * 回执→FORGET 抢在收敛之前会把决议提前遗忘（实测目击）。经 rendezvous
     * 问 pg_partdist 的未决登记：该 dtxid 在本节点还挂着就跳过本轮回执。
     * 未装 pg_partdist / 遗留模式（登记恒空）行为与从前逐字节一致。
     */
    {
        static bool (**dtx_pending_check_rv)(int64) = NULL;

        if (dtx_pending_check_rv == NULL)
            dtx_pending_check_rv = (bool (**)(int64))
                find_rendezvous_variable("partdist_dtx_pending_check_fn");
        dtx_pending_check = *dtx_pending_check_rv;
    }

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

        if (dtx_pending_check != NULL && dtx_pending_check(dtxids[i]))
            continue;       /* R-P4-5：本节点 TX2 收敛未完成，回执后移 */

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
        "   AND partdist.dtx_verdict_effective(verdict, anchor_xid) IN (1, 2) "
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

        raft_claim_purpose = RAFT_CLAIM_DTX_FORGET;
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
    /*
     * ★ TPC-C 分支：候选不再要求"发起 backend 已死"。函数下推后 2PC 由 worker 上
     * 的池化连接驱动，这些 backend 常驻不退 —— 行永远不进候选，表只增不减
     * （128 终端几轮下来 4 万行），而每笔 2PC 驱动者的提交探针都要扫它。
     * 去掉它是安全的：本查询只看得见已提交的行，Citus 写这行时全部 PREPARE 都已
     * 成功；此后"全网（含本机）都没有这个 gid 的 prepared" ⇔ COMMIT PREPARED 已
     * 全部做完，行再无用处。下面的逐节点确认原样保留，任一节点不可达整轮放弃。
     */
    initStringInfo(&sql);
    appendStringInfoString(&sql,
        "SELECT DISTINCT t.gid FROM pg_catalog.pg_dist_transaction t "
        " LIMIT 5000");
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

    /* 删除全网确认已闭合的行（一条 DELETE 批量删，候选上限已提到 5000） */
    if (!raft_persist_spi_begin(&spi_owned))
        return -1;
    {
        bool        any = false;

        initStringInfo(&sql);
        appendStringInfoString(&sql,
            "DELETE FROM pg_catalog.pg_dist_transaction WHERE gid = ANY(ARRAY[");
        for (i = 0; i < n; i++)
        {
            if (keep[i] || gids[i] == NULL)
                continue;
            appendStringInfo(&sql, "%s%s", any ? "," : "",
                             quote_literal_cstr(gids[i]));
            any = true;
        }
        appendStringInfoString(&sql, "]::text[])");
        if (any && SPI_execute(sql.data, false, 0) == SPI_OK_DELETE)
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
        /* 待确认的 COMMIT（verdict=3）按锚点折算，锚点就在本节点（2026-10-18 方案 B） */
        appendStringInfo(&sql,
                         "SELECT partdist.dtx_verdict_effective(verdict, anchor_xid) "
                         "  FROM partdist.dtx_decision WHERE dtxid = %lld",
                         (long long) dtxid);
        if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                    1, &isnull);

            if (!isnull)
                verdict = DatumGetInt32(d);
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
        char      qry[192];
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
                 "SELECT partdist.dtx_verdict_effective(verdict, anchor_xid) "
                 "  FROM partdist.dtx_decision WHERE dtxid = %lld",
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

    /*
     * d) citus 形态：问发起节点（2026-10-18 起按 gid 的 group 定位发起方，不再固定问
     *    Citus 协调者，见 dtx_ask_driver）。**只认 COMMIT**：走到这里的分片可能早已
     *    COMMIT PREPARED、pg_dist_transaction 的行也已被 GC，查不到行不等于没提交。
     */
    verdict = (dtx_ask_driver_of_dtxid(dtxid) == 1) ? 1 : 0;

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
/*
 * partdist.pg_raft_all_data_peers_reachable() → bool
 *
 * ★ P7-N52（之四，2026-10-05）：全部数据节点（除 master）此刻是否都连得上、能执行语句。
 *
 * 读者在"本机无未决登记、就地闭合后全网仍找不到判决"时据此决定能不能按"未决"处理。
 * 找不到判决不等于没有判决：协调组 leader 刚写下 COMMIT 决议（客户端已收到提交成功）
 * 就断电，follower 已持有那条记录却还没 apply 进 dtx_decision，选出新 leader 之前全网
 * 都查不到 —— 而那台断电的节点此刻必然连不上。所以：全部可达 ⇒ 查不到即未决；
 * 有节点不可达 ⇒ 结论不可信，读者报可重试错误。
 */
/*
 * partdist.pg_raft_group_leader_settled(group_id) → bool
 *
 * ★ P7-N65（2026-10-05）：本节点是该组 leader，且**已经提交过本任期的条目**、apply 也追到了
 * 提交点 —— 只有这时它说"我这里没有某条决议"才算数（Raft 的 ReadIndex 前提）。
 *
 * 新 leader 刚上任时，前任期留下的条目（可能正是一条已被客户端确认的 COMMIT 决议）还没
 * 提交、也就没被任何节点 apply；它要等升主前置补提本任期空条目（N35）才顺带提交 ——
 * 升主前置要排队、要追平，实测这个窗口长达 5 分钟。窗口里 dtx_peek 回答"未决"，读者就把
 * 已提交的事务当成未提交，同一快照部分可见（总额少 160）。
 */
PG_FUNCTION_INFO_V1(pg_raft_group_leader_settled);

Datum
pg_raft_group_leader_settled(PG_FUNCTION_ARGS)
{
    int64        group_id = PG_GETARG_INT64(0);
    RaftGroupCtx ctx;
    int          state;
    int64        term;
    int64        commit_idx;
    int64        applied;
    int64        cterm = -1;
    RaftLogEntry e;

    if (!pg_raft_raft_enabled || RaftGroups == NULL)
        PG_RETURN_BOOL(false);
    parse_peers();
    restore_groups_if_needed();
    if (!raft_group_ctx(group_id, &ctx))
        PG_RETURN_BOOL(false);

    SpinLockAcquire(&ctx.cons->mutex);
    state = ctx.cons->state;
    term = ctx.cons->current_term;
    SpinLockRelease(&ctx.cons->mutex);
    if (state != RAFT_LEADER)
        PG_RETURN_BOOL(false);

    SpinLockAcquire(&ctx.log->mutex);
    commit_idx = ctx.log->commit_index;
    applied = ctx.log->last_applied;
    if (commit_idx > 0 && log_get_entry_locked(&ctx, commit_idx, &e))
        cterm = e.term;
    SpinLockRelease(&ctx.log->mutex);

    PG_RETURN_BOOL(cterm == term && applied >= commit_idx);
}

PG_FUNCTION_INFO_V1(pg_raft_all_data_peers_reachable);

Datum
pg_raft_all_data_peers_reachable(PG_FUNCTION_ARGS)
{
    int i;

    if (!pg_raft_raft_enabled)
        PG_RETURN_BOOL(true);
    parse_peers();
    for (i = 0; i < n_peers; i++)
    {
        char      conninfo[256];
        PGconn   *conn;
        PGresult *res;
        bool      ok;

        if (peers[i].node_id == pg_raft_node_id ||
            peers[i].node_id == pg_raft_coordinator_node_id)
            continue;
        pg_raft_format_conninfo(peers[i].host, peers[i].port,
                                conninfo, sizeof(conninfo));
        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) != CONNECTION_OK)
        {
            PQfinish(conn);
            PG_RETURN_BOOL(false);
        }
        res = PQexec(conn, "SELECT 1");
        ok = (PQresultStatus(res) == PGRES_TUPLES_OK);
        PQclear(res);
        PQfinish(conn);
        if (!ok)
            PG_RETURN_BOOL(false);
    }
    PG_RETURN_BOOL(true);
}

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
    int           *sverd = NULL;        /* 流内已有的闭合记录：1=COMMIT 2=ABORT 0=无 */
    long long     *scts = NULL;         /* 流内闭合记录带的 commit_ts */
    long long     *sxids = NULL;        /* 映射出的分片 xid；0 = 映射不到（遗留宇宙） */
    long long     *hgx = NULL;          /* 该事务 PREPARE 记录的头部 gxid */
    int            closed = 0;
    int            unresolved = 0;

    if (!pg_raft_raft_enabled)
        PG_RETURN_INT32(0);
    parse_peers();

    /*
     * 1) in-doubt 清单 —— ★ P7-N12（之二，2026-09-17）改从**分片 clog** 出发。
     *
     * 原判据"流里有 DTX_PREPARE、无 DTX_COMMIT/ABORT"漏掉了一整类：升主序列此前只往流里
     * 追加一条闭合记录，本节点的分片 clog 从不更新、回放侧也不认那条记录（分片 clog 只在
     * 回放带分片 xid 的 MARKER 时落判决）。于是"流里已闭合、clog 仍 PREPARED"的事务永远
     * 不可见，且再也不会被本函数看见（实测：3 个 PREPARED 残留，本函数只报 1 笔 in-doubt）。
     *
     * 现在：列出全部 DTX_PREPARE；按 DTX_PREPARE 与 PREPARE MARKER 共享的头部 gxid 把 dtxid
     * 映射回分片 xid（partdist.partwal_prepare_sxid）；**分片 clog 仍 PREPARED 的一律处理**，
     * 判决优先取流里已有的闭合记录（本地、权威、不用 RPC），没有才问决议。
     * 映射不到分片 xid 的（遗留宇宙 / 未打标）沿用旧判据。
     */
    if (!raft_persist_spi_begin(&spi_owned))
        PG_RETURN_INT32(0);
    initStringInfo(&sql);
    appendStringInfo(&sql,
        "WITH recs AS ("
        "  SELECT g AS plsn, d.kind, d.dtxid, d.commit_ts"
        "    FROM generate_series(1, partdist.get_partition_flush_lsn(%u::oid)) g"
        "    LEFT JOIN LATERAL partdist.partwal_read_dtx_record(%u::oid, g) d ON true"
        "   WHERE d.dtxid IS NOT NULL), "
        "prep AS (SELECT DISTINCT dtxid FROM recs WHERE kind = 1), "
        "cl AS (SELECT dtxid, max(CASE kind WHEN 3 THEN 1 WHEN 4 THEN 2 ELSE 0 END) AS v, "
        "              max(CASE WHEN kind = 3 THEN commit_ts ELSE 0 END) AS cts "
        "         FROM recs WHERE kind IN (3, 4) GROUP BY dtxid) "
        "SELECT p.dtxid, coalesce(cl.v, 0), coalesce(cl.cts, 0), "
        "       coalesce(m.sxid, 0), coalesce(m.hdr_gxid, 0) "
        "  FROM prep p LEFT JOIN cl ON cl.dtxid = p.dtxid "
        "  LEFT JOIN LATERAL partdist.partwal_prepare_sxid(%u::oid, p.dtxid, "
        "                       partdist.get_partition_flush_lsn(%u::oid)) m ON true "
        " WHERE (m.sxid IS NOT NULL AND partdist.shard_clog_status(%u::oid, m.sxid) = 1) "
        "    OR (m.sxid IS NULL AND cl.v IS NULL) "
        " LIMIT 64",
        (unsigned) partition_id, (unsigned) partition_id,
        (unsigned) partition_id, (unsigned) partition_id, (unsigned) partition_id);
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        MemoryContext old = MemoryContextSwitchTo(CurTransactionContext);

        n = (int) SPI_processed;
        dtxids = (long long *) palloc(sizeof(long long) * n);
        sverd = (int *) palloc(sizeof(int) * n);
        scts = (long long *) palloc(sizeof(long long) * n);
        sxids = (long long *) palloc(sizeof(long long) * n);
        hgx = (long long *) palloc(sizeof(long long) * n);
        for (i = 0; i < n; i++)
        {
            char *v;

            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
            dtxids[i] = v ? atoll(v) : 0;
            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);
            sverd[i] = v ? atoi(v) : 0;
            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 3);
            scts[i] = v ? atoll(v) : 0;
            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4);
            sxids[i] = v ? atoll(v) : 0;
            v = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 5);
            hgx[i] = v ? atoll(v) : 0;
        }
        MemoryContextSwitchTo(old);
    }
    pfree(sql.data);
    raft_persist_spi_end(spi_owned);

    /* 2) 逐笔求决议、落分片 clog + 判决 MARKER、补闭合记录 */
    for (i = 0; i < n; i++)
    {
        long long reg_coord = 0;
        int       verdict = sverd[i];
        int64     cts = (int64) scts[i];

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

        /*
         * 流里没有闭合记录，或有 COMMIT 却没带 commit_ts（旧格式）：先经
         * partdist.dtx_inquire 问（拿得到 commit_ts），问不到再走四级寻址。
         */
        if ((verdict != 1 && verdict != 2) || (verdict == 1 && cts == 0))
        {
            int   iv = 0;
            int64 ic = 0;

            if (reg_coord > 0 && raft_persist_spi_begin(&spi_owned))
            {
                initStringInfo(&sql);
                appendStringInfo(&sql,
                                 "SELECT verdict, commit_ts FROM partdist.dtx_inquire(%lld, %lld)",
                                 reg_coord, dtxids[i]);
                if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
                {
                    bool  n1, n2;
                    Datum d1 = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &n1);
                    Datum d2 = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &n2);

                    if (!n1)
                        iv = DatumGetInt32(d1);
                    if (!n2)
                        ic = DatumGetInt64(d2);
                }
                pfree(sql.data);
                raft_persist_spi_end(spi_owned);
            }
            if (iv == 1 || iv == 2)
            {
                if (verdict != 1 && verdict != 2)
                    verdict = iv;
                if (verdict == 1 && cts == 0)
                    cts = ic;
            }
            if (verdict != 1 && verdict != 2)
                verdict = dtx_resolve_verdict_anywhere((int64) dtxids[i], (int64) reg_coord);
        }
        if (verdict != 1 && verdict != 2)
        {
            unresolved++;
            continue;
        }

        /* 分片 clog + 带分片 xid 的判决 MARKER（与 leader 侧 dtx_replicate_verdict 同构） */
        if (sxids[i] > 0 && raft_persist_spi_begin(&spi_owned))
        {
            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "SELECT partdist.shard_verdict_apply(%u::oid, %lld::bigint, %lld::bigint, %s, %lld::bigint)",
                             (unsigned) partition_id, sxids[i], hgx[i],
                             (verdict == 1) ? "true" : "false", (long long) cts);
            (void) SPI_execute(sql.data, false, 1);
            pfree(sql.data);
            raft_persist_spi_end(spi_owned);
        }

        /* 流里还没有闭合记录才补一条（带 commit_ts，供后来者直接取） */
        if (sverd[i] == 0 && raft_persist_spi_begin(&spi_owned))
        {
            initStringInfo(&sql);
            appendStringInfo(&sql,
                             "SELECT partdist.partwal_append_dtx_record("
                             "%u::oid, %d, %lld::bigint, %lld::bigint, %lld::bigint)",
                             (unsigned) partition_id,
                             (verdict == 1) ? 3 : 4,
                             dtxids[i], reg_coord, (long long) cts);
            (void) SPI_execute(sql.data, false, 1);
            pfree(sql.data);
            raft_persist_spi_end(spi_owned);
        }
        closed++;
        elog(LOG, "pg_raft: in-doubt 闭合：分区 %u dtxid=%lld → %s（分片 xid %lld，%s）",
             partition_id, dtxids[i], (verdict == 1) ? "COMMIT" : "ABORT",
             sxids[i], sxids[i] > 0 ? "已落分片 clog 并发判决 MARKER" : "映射不到分片 xid，只追加 DTX 记录");
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
 *   2) 写集 0 组 ⇒ 未写纳管分片，直接返回；≥ 1 组（含单组 2PC，P7-N10）都算
 *      coord_gsid = participants[dtxid % n]，并**先**把它下发到全部参与节点；
 *   3) 到协调组现任 leader 上调 partdist.dtx_decide() —— 该决议记录在协调组
 *      达多数派持久化即为**全局提交点**，返回 COMMIT 之后本函数才放行，
 *      客户端随后收到的 COMMIT 成功因此是有多数派保证的。
 *
 * ★ 2026-10-18（方案 B，原子性修复）：第 3 步只写 verdict=3（"COMMIT，待发起方确认"）。
 *   本函数跑在本地提交记录**之前**，原先在这里写 COMMIT，本节点在写提交记录前崩溃时，
 *   参与方照判决提交、本地这一半却被回滚（云上满负载杀 worker 12 次复现 3 次）。
 *   现在判决在本地提交持久之后才生效：Citus 随后发的 COMMIT PREPARED 让协调组节点上的
 *   锚点 prepared 事务提交，判决随之生效；本节点崩溃则由恢复守护问本节点的
 *   pg_dist_transaction 定结局（dtx_status → dtx_ask_driver）。不多刷盘、不多往返。
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

/*
 * TPC-C 分支：本事务是否往 pg_dist_transaction 插过行（= Citus 2PC 驱动者）。
 *
 * 原先每笔写事务提交都跑一次 "WHERE t.xmin = pg_current_xact_id()" 的 SPI 探针 ——
 * 那是对 pg_dist_transaction 的顺序扫描，表里攒着全部未回收的 2PC 记录，
 * 128 终端实测吃掉 worker 约 1/4 的 CPU（perf：ExecInterpExpr / heapgettup /
 * heap_getsysattr / xid8toxid 一串）。改为先看 pgstat 的本事务插入计数（O(1)、
 * 无 SPI）：计数为 0 即不是驱动者，直接返回。
 *
 * 返回 true = 不能排除（计数 > 0，或统计未开启 / 表不存在无法判断）—— 调用方
 * 照旧走 SPI 探针。宁可多探一次，绝不漏判：漏判 = 决议永远不做 = 行永久不可见。
 */
static bool
dtx_maybe_wrote_dist_transaction(void)
{
    Oid         dist_txn_relid;
    PgStat_TableStatus *tab;
    PgStat_TableXactStatus *trans;
    PgStat_Counter n = 0;

    if (!pgstat_track_counts)
        return true;
    /* 每次查 syscache（命中即哈希一次）：不缓存 OID，免得 Citus 重建后拿着旧号漏判 */
    dist_txn_relid = get_relname_relid("pg_dist_transaction", PG_CATALOG_NAMESPACE);
    if (!OidIsValid(dist_txn_relid))
        return true;            /* 未装 Citus：交给原路径判（它自己会退出） */
    tab = find_tabstat_entry(dist_txn_relid);
    if (tab == NULL)
        return false;
    for (trans = tab->trans; trans != NULL; trans = trans->upper)
        n += trans->tuples_inserted;
    return n > 0;
}

/*
 * 2026-10-18：原子性验证用的暂停点（pg_raft.dtx_test_pause_point / _ms，超级用户、默认关）。
 * 让 2PC 发起方停在两个崩溃窗口里，测试趁机杀掉节点，验证判决与本地提交绑定：
 *   1 = 判决（待确认 COMMIT）已写进协调组、本地提交记录还没写 —— 杀掉后应整笔 ABORT；
 *   2 = 本地已提交、Citus 还没发 COMMIT PREPARED —— 杀掉后应整笔 COMMIT。
 * 第 2 点挂在本模块的事务回调上：回调后注册先调用，排在 Citus 发 COMMIT PREPARED 之前。
 */
static bool  dtx_test_cb_registered = false;
static int64 dtx_test_armed_dtxid = 0;

static void
dtx_test_xact_cb(XactEvent event, void *arg)
{
    (void) arg;
    if (dtx_test_armed_dtxid == 0)
        return;
    if (event == XACT_EVENT_COMMIT && pg_raft_dtx_test_pause_ms > 0)
    {
        elog(LOG, "pg_raft: [测试暂停点 2] dtx %lld 本地已提交、COMMIT PREPARED 未发，暂停 %d ms",
             (long long) dtx_test_armed_dtxid, pg_raft_dtx_test_pause_ms);
        pg_usleep((long) pg_raft_dtx_test_pause_ms * 1000L);
    }
    if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT ||
        event == XACT_EVENT_PARALLEL_COMMIT || event == XACT_EVENT_PARALLEL_ABORT)
        dtx_test_armed_dtxid = 0;
}

static void
dtx_test_pause_after_decide(int64 dtxid)
{
    if (pg_raft_dtx_test_pause_ms <= 0)
        return;
    if (pg_raft_dtx_test_pause_point == 1)
    {
        elog(LOG, "pg_raft: [测试暂停点 1] dtx %lld 判决已写、本地提交记录未写，暂停 %d ms",
             (long long) dtxid, pg_raft_dtx_test_pause_ms);
        pg_usleep((long) pg_raft_dtx_test_pause_ms * 1000L);
    }
    else if (pg_raft_dtx_test_pause_point == 2)
    {
        if (!dtx_test_cb_registered)
        {
            RegisterXactCallback(dtx_test_xact_cb, NULL);
            dtx_test_cb_registered = true;
        }
        dtx_test_armed_dtxid = dtxid;
    }
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
    int             probe_rows = 0;     /* T7.1：探针命中的 pg_dist_transaction 行数 */

    /*
     * ★ T7.1 期改（2026-09-09）：这条原本是无条件 LOG，每个库的每次提交都打一行
     * （协调者日志里几千行"挂钩已进入"），而它**只说进来了、不说为什么走掉** ——
     * 决议没做时从日志上完全看不出卡在哪一步。降到 DEBUG1，改由下面的探针结果
     * 行承担诊断（与 §13 约束 13 那条"三个 return false 在日志上看不出区别"
     * 同一教训）。
     */
    elog(DEBUG1, "pg_raft: [R11] 挂钩已进入 raft=%d dtx2pc=%d",
         pg_raft_raft_enabled ? 1 : 0, pg_raft_dtx_2pc_enabled ? 1 : 0);
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
    /*
     * ★ TPC-C 分支：去掉"application_name 以 citus_internal 开头即退出"的闸门。
     * 函数下推（create_distributed_function）后，事务函数在 worker 的 citus_internal
     * 任务连接里执行；它碰到远程仓库时，**这个 worker 就是 2PC 驱动者**。闸门让它
     * 从不写决议 —— 远程参与者的分片槽位永远停在 PREPARED（实测下推后频繁出现
     * "判决暂时无法确定"与等锁路径的孤儿 RUNNING）。是不是驱动者改由下面
     * pg_dist_transaction 的 xmin 探针判定（本事务写了 2PC 记录才命中），参与侧每次
     * 提交多一次只读小查询；只读事务在下一行就已退出。
     */
    if (!IsTransactionState() || GetTopTransactionIdIfAny() == InvalidTransactionId)
        return;

    if (!dtx_maybe_wrote_dist_transaction())
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

    probe_rows = -1;
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        probe_rows = (int) SPI_processed;
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

    /*
     * ★ T7.1 期加（2026-09-09）：探针结果留痕。
     * `probe_rows == 0/-1` = 本事务没往 pg_dist_transaction 写行 ⇒ 不是 2PC 驱动者，
     * 这是绝大多数提交的正常去向，DEBUG1 即可；
     * 探针**有行却仍然 dtxid=0 或 nnodes=0** 则是真异常（gid 形态不认识、
     * 或 pg_dist_node 里找不到对应 primary），必须 LOG 出来 ——
     * 否则表现就是"2PC 事务提交成功但决议永远不出现、行永久不可见"，
     * 而日志上一片安静（2026-09-09 在 pg-test 环境实测踩过一整轮）。
     */
    /* 补丁 0014：2PC 驱动者的本地提交必须先刷盘，Citus 才能发 COMMIT PREPARED */
    if (probe_rows > 0)
        shard_elr_inhibit = true;
    if (probe_rows > 0)
        elog(LOG, "pg_raft: [R11] 2PC 探针命中 %d 行：dtxid=" INT64_FORMAT " nnodes=%d",
             probe_rows, dtxid, nnodes);
    else
        elog(DEBUG1, "pg_raft: [R11] 2PC 探针无行（本事务不是 2PC 驱动者）");

    if (dtxid == 0 || nnodes == 0)
        return;                     /* 不是跨节点的 Citus 2PC 事务 */

    /*
     * ★ TPC-C 分支快路径：只有一个参与节点时，一次往返在它上面做完
     * "取写集 → 选协调组 → 下发协调组 → 决议"（partdist.dtx_local_note_decide，
     * 同一事务一次提交）。原路径是三次往返 + 参与节点上两次提交 fsync，全部夹在
     * 本事务持有热行锁（warehouse/district）的提交路径里 —— 等待采样里挡住
     * 别人的 Payment 有三成正卡在这段远程收尾上。
     * 决议时间戳提前到这里取：此刻 Citus 的 PREPARE 已全部成功，仍是"票齐"之后。
     * 对端不是协调组 leader 或任何失败：远端事务整体回滚（协调组也没下发），
     * 回落到下面的常规路径，语义不变。
     */
    if (nnodes == 1)
    {
        static void **tso_fast_rv = NULL;
        int64       fast_cts = 0;
        PGconn     *fconn;
        PGresult   *fres;
        char        fqry[160];

        if (tso_fast_rv == NULL)
            tso_fast_rv = find_rendezvous_variable("partdist_tso_dtx_decision_ts_fn");
        if (*tso_fast_rv != NULL)
            fast_cts = ((int64 (*)(void)) *tso_fast_rv) ();   /* 失败直接 ERROR：尚未下发任何东西 */

        snprintf(fqry, sizeof(fqry),
                 "SELECT partdist.dtx_local_note_decide(%lld, %lld)",
                 (long long) dtxid, (long long) fast_cts);
        fconn = dtx_get_conn(nodes[0].host, nodes[0].port);
        fres = PQexec(fconn, fqry);
        if (PQresultStatus(fres) == PGRES_TUPLES_OK && PQntuples(fres) == 1 &&
            !PQgetisnull(fres, 0, 0))
        {
            int     fv = atoi(PQgetvalue(fres, 0, 0));

            PQclear(fres);
            if (fv == 0)
                return;             /* 写集为空：不需要决议（同下面 nparts == 0） */
            /* 3 = 待确认 COMMIT 已记下（方案 B）；1 只会出现在旧版对端上 */
            if (fv != 3 && fv != 1)
                ereport(ERROR,
                        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                         errmsg("pg_raft: 分布式事务 %lld 的全局决议是 ABORT", (long long) dtxid),
                         errdetail("协调组日志里已有一条 ABORT 决议（多半是恢复守护在推定中止时抢先写入）。")));
            elog(DEBUG1, "pg_raft: dtx %lld COMMIT（单参与节点快路径）", (long long) dtxid);
            dtx_test_pause_after_decide(dtxid);
            return;
        }
        elog(DEBUG1, "pg_raft: dtx %lld 单参与节点快路径未成，回落常规路径：%s",
             (long long) dtxid, PQerrorMessage(fconn));
        PQclear(fres);
        if (PQtransactionStatus(fconn) != PQTRANS_IDLE)
        {
            PGresult *rb = PQexec(fconn, "ROLLBACK");

            PQclear(rb);
        }
    }

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
     * 写集 0 组 = 没写任何纳管分片，退化为接线前行为（Citus 原生规则收尾）。
     *
     * ★ P7-N10（2026-09-17）：这里原本是 `nparts <= 1`，把"写集只有 1 组"也当
     * §3.4 快路径直接返回。那条快路径说的是 **router 单分片 1PC**：根本没有
     * PREPARE，本函数在上面的 pg_dist_transaction 探针处就已经退出了，走不到这里。
     * 能走到这里的 1 组事务都是 **2PC 在用**（典型：协调者侧 INSERT…SELECT 只命中
     * 一个分片；多分片 UPDATE 只有一个分片真改到行）—— 参与者已经 PREPARE、
     * 分片 xid 记成 PREPARED、未决登记等着决议；COMMIT PREPARED 按 T4.5 不写终局。
     * 不做决议 = 判决永远不来 = **提交成功的行在打标表上永久不可见**，且
     * 恢复守护只看 pg_prepared_xacts，救不回来（协调者早就 COMMIT PREPARED 了）。
     * "该组自己的 quorum 已覆盖全部数据"只说明字节持久了，可见性要的是
     * 判决 + commit_ts，与组数无关。单组就按多组的退化情形照常下发协调组、
     * 做决议、广播 —— 多一轮决议的代价只落在本来就在付 2PC 的事务上。
     */
    if (nparts == 0)
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
        /*
         * ★ P7-N68 注：下发失败时 dtx_remote_scalar 直接 ERROR（本事务随之中止、不会走到决议），
         * dtx_note_coord 也恒返回整数 —— "先下发到全部参与者、再决议"是严格成立的。参与者本地
         * "有登记、协调组号为空"因此是权威的"还没决议"（dtx_inquire_core 据此答复）。
         */
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

        /* 3 = COMMIT 待本节点确认（方案 B）：本地提交持久之前判决不生效 */
        n = snprintf(qry, sizeof(qry),
                     "SELECT partdist.dtx_decide(%lld, %lld, 3, ARRAY[",
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

    if (verdict != 3 && verdict != 1)
        ereport(ERROR,
                (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                 errmsg("pg_raft: 分布式事务 %lld 的全局决议是 ABORT", (long long) dtxid),
                 errdetail("协调组 %lld 的日志里已有一条 ABORT 决议（多半是恢复守护"
                           "在推定中止时抢先写入）。决议槽一次性，不可翻盘。",
                           (long long) coord_gsid)));

    elog(DEBUG1, "pg_raft: dtx %lld COMMIT（协调组 %lld，参与组 %d 个）",
         (long long) dtxid, (long long) coord_gsid, nparts);
    dtx_test_pause_after_decide(dtxid);
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
static PGconn *note_selfconn = NULL;
static bool note_selfconn_async = false;    /* 该连接已 SET synchronous_commit=off */

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

    /*
     * ★ TPC-C 分支：自连接按 backend 缓存。原来每次 PREPARE 都 PQconnectdb 一次
     * = 本节点 fork 一个新后端、走一遍认证与 relcache 预热，128 终端下这是每笔
     * 2PC 的固定毫秒级开销。连接上每条语句自动提交，独立事务的语义不变。
     */
    if (note_selfconn != NULL && PQstatus(note_selfconn) != CONNECTION_OK)
    {
        PQfinish(note_selfconn);
        note_selfconn = NULL;
    }
    if (note_selfconn == NULL)
    {
        pg_raft_format_conninfo("127.0.0.1", PostPortNumber, selfconn, sizeof(selfconn));
        note_selfconn = PQconnectdb(selfconn);
        note_selfconn_async = false;
        if (PQstatus(note_selfconn) != CONNECTION_OK)
        {
            elog(WARNING, "pg_raft: DTX 参与登记连回本节点失败: %s", PQerrorMessage(note_selfconn));
            PQfinish(note_selfconn);
            note_selfconn = NULL;
            return false;
        }
    }
    conn = note_selfconn;
    /*
     * ★ TPC-C 分支：登记行异步提交。"prepared 存在 ⇒ 登记必在"由 WAL 次序保证：
     * 登记的提交记录先于本事务的 PREPARE 记录写进同一条 pg_wal，PREPARE 刷盘时
     * 它一并落盘；PREPARE 之前崩溃则 prepared 本身不存在。每笔 2PC 省参与节点
     * 一次 fsync（夹在发起端持热行锁的提交路径里）。
     */
    if (!note_selfconn_async)
    {
        PGresult   *sr = PQexec(conn, "SET synchronous_commit TO off");

        note_selfconn_async = (PQresultStatus(sr) == PGRES_COMMAND_OK);
        PQclear(sr);
    }

    initStringInfo(&sql);
    appendStringInfoString(&sql, "SELECT partdist.dtx_note_participant(");
    appendStringInfo(&sql, "%lld, ", (long long) dtxid);
    {
        char *q = PQescapeLiteral(conn, gid, strlen(gid));

        if (q == NULL)
        {
            pfree(sql.data);
            return false;
        }
        appendStringInfoString(&sql, q);
        PQfreemem(q);
    }
    appendStringInfoString(&sql, ", ARRAY[");
    for (i = 0; i < ngsids; i++)
        appendStringInfo(&sql, "%s%lld", (i == 0) ? "" : ",", (long long) gsids[i]);
    /*
     * 2026-10-18（方案 B）：顺带记下本后端即将 prepare 的事务（full xid）—— 本节点若被选作
     * 协调组，它就是待确认决议的锚点（dtx_local_anchor_xid），省得在提交路径上扫 pg_prepared_xacts。
     */
    appendStringInfo(&sql, "]::bigint[], %llu::bigint)",
                     (unsigned long long) U64FromFullTransactionId(GetTopFullTransactionIdIfAny()));

    res = PQexec(conn, sql.data);
    ok = (PQresultStatus(res) == PGRES_TUPLES_OK ||
          PQresultStatus(res) == PGRES_COMMAND_OK);
    if (!ok)
        elog(WARNING, "pg_raft: DTX 参与登记失败: %s", PQerrorMessage(conn));
    PQclear(res);
    pfree(sql.data);
    if (!ok && PQtransactionStatus(conn) != PQTRANS_IDLE)
    {
        PQfinish(note_selfconn);    /* 状态不明的连接不复用 */
        note_selfconn = NULL;
    }
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
