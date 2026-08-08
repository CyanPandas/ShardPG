/*
 * enhanced_clog.h
 *
 * 增强型 CLOG（pg_gclog）—— 全局事务的提交判决账本（FRD §9.3）。
 *
 * 为什么不能用原生 clog：回放引入的 xid 是**别的节点**分配的。follower 拿元组
 * 的 xmin（32 位本地 xid）去查自己的 clog，查到的是它自己历史上那个碰巧同号的
 * 事务 —— 实测就是这样多读出一行被 ROLLBACK TO 掉的数据。原生 clog 里这段区间
 * 甚至根本没有页（nextXid 被推进但从未 ExtendCLOG，§13 约束 4）。
 *
 * 作用域是**来源节点**，不是分区 —— 与 xid_map 正好相反，这个不对称是有意的：
 *
 *   xid_map   键 = local_xid（32 位），**每分区一张**。local_xid 单独拿出来有
 *             歧义（甲组 leader 的 1000 号和乙组的 1000 号是两笔事务），是"分区"
 *             这个上下文告诉你它来自哪个节点。
 *   pg_gclog  键 = gxid（64 位），**每来源节点一套**。gxid 本身已全局唯一，不需
 *             要再消歧；而且同一节点当 leader 的多个 shard **必须共享**同一份
 *             账本 —— 它们用的是那个节点同一个 xid 计数器，node 1 的 xid 1000
 *             就是一笔事务，不管记录从哪个 shard 的流里过来。
 *
 * 读路径（R3）因此是两跳：tuple.xmin --查所在分区 xid_map--> gxid --查 gclog--> 状态。
 *
 * 布局：pg_gclog/<node_id>/<8 位十六进制段号>，段内是**扁平的定长槽数组**，
 * 槽号 = local_xid 在本段内的偏移，直接寻址，无页概念。文件稀疏 —— 只有真正
 * 回放过的 xid 才落盘，中间的洞由文件系统负责。
 */
#ifndef ENHANCED_CLOG_H
#define ENHANCED_CLOG_H

#include "postgres.h"

#include "global_mvcc.h"

typedef enum TxnStatus
{
    TXN_RUNNING   = 0,      /* 也是"槽位从未写过"的读出值 —— 见下 */
    TXN_PREPARED  = 1,
    TXN_COMMITTED = 2,
    TXN_ABORTED   = 3
} TxnStatus;

/*
 * 一个事务的判决槽。
 *
 * **与 FRD §9.3 的偏差（实测修正）**：文档给的是 16 字节槽、时间戳各 6 字节
 * （48 位）。48 位放不下 TimestampTz —— 它是"自 2000-01-01 起的微秒数"，
 * 2^48 µs ≈ 8.9 年，**2008 年就溢出了**；实测当前值 839,057,670,472,675，
 * 是 2^48 = 281,474,976,710,656 的三倍。截断不会报错，只会把提交顺序悄悄弄乱，
 * 而 §10 的可见性判据正是 `commit_ts <= 快照 start_ts`。
 * 因此这里放宽到 24 字节、时间戳各 8 字节。TSO 就位后只换取值来源，不动布局。
 *
 * reserved 是显式补齐位（槽直写磁盘，不留未初始化的洞，与
 * TxnMarkerPayload / XidMapEntry 同源的教训）。
 *
 * **全零槽 = TXN_RUNNING = 未决 = 不可见**，这正是稀疏文件空洞读出来的样子，
 * 也正是我们想要的默认值：没有判决的事务一律当作还没提交，语义安全。
 */
typedef struct EnhancedClogSlot
{
    uint64      start_ts;       /* 事务启动时间戳                      */
    uint64      commit_ts;      /* 提交时间戳；ABORTED 时为 0          */
    uint32      status;         /* TxnStatus                           */
    uint32      parent_xid;     /* 见下；0 = 顶层事务（旧槽的补齐位）  */
} EnhancedClogSlot;

/*
 * parent_xid —— 子事务到顶层事务的链接，语义抄内核 pg_subtrans。
 *
 * 为什么需要它：2PC 事务的判决分两次到达。PREPARE 时我们知道整棵提交树
 * （顶层 + 已提交子事务清单），但还不知道判决；COMMIT PREPARED 时知道了
 * 判决，却拿不到子事务清单 —— 那条语句跑在**另一个事务**里，
 * xactGetCommittedChildren() 返回的是它自己的（空）清单。
 *
 * 于是 PREPARE 标记把整棵树写成 TXN_PREPARED，并给每个子事务槽记下
 * parent_xid；COMMIT/ABORT 标记只需写顶层一条，读路径遇到
 * "status==TXN_PREPARED 且 parent_xid!=0" 时改问父亲。一跳即可 ——
 * xactGetCommittedChildren() 返回的是**拍平**的全部后代。
 *
 * 兼容性：该字段就是原来的 reserved，历史槽一律为 0 = 无父 = 按自身状态
 * 判定，与改动前逐字节等价。
 */

#define GCLOG_SLOT_SIZE         ((uint32) sizeof(EnhancedClogSlot))

/* 每段容纳的 xid 数：1M 槽 × 24B = 24MB/段（稀疏，实占远小于此） */
#define GCLOG_XIDS_PER_SEGMENT  (UINT32_C(1) << 20)

#define GCLOG_DIR               "pg_gclog"

/*
 * 写一条判决。同一个 gxid 被重复写入是**幂等**的 —— 回放重放同一条 MARKER
 * 会算出同样的内容（崩溃恢复正是靠这一点，见 §8.4）。
 *
 * 只写不 fsync；落盘时机由 §8.4 推进协议第 2 步给出：apply checkpoint 写游标
 * **之前**调用 EnhancedClogSync()。顺序反了就会出现"元组在、判决没了"的空账。
 */
extern void EnhancedClogWriteStatus(GlobalTransactionId gxid,
                                    uint64 start_ts, uint64 commit_ts,
                                    TxnStatus status);

/*
 * 同上，但额外记下 parent_xid（顶层事务的**本地** xid，与 gxid 同节点）。
 * 只有 2PC 的 PREPARE 标记会用到；parent_xid=0 时与上面那个完全等价。
 */
extern void EnhancedClogWriteStatusWithParent(GlobalTransactionId gxid,
                                              uint64 start_ts, uint64 commit_ts,
                                              TxnStatus status,
                                              TransactionId parent_xid);

/* 读一条判决。未写过的槽返回 true 且 status = TXN_RUNNING（空洞语义）。
 * 段文件不存在同样按空洞处理。R3 读路径的入口，本期供验收用例核账。 */
extern bool EnhancedClogReadStatus(GlobalTransactionId gxid,
                                   TxnStatus *status,
                                   uint64 *start_ts, uint64 *commit_ts);

/* fsync 本进程写过、尚未落盘的全部段（§8.4 步骤 2） */
extern void EnhancedClogSync(void);

/* 进程退出/换 shard 时释放缓存的 fd */
extern void EnhancedClogCloseAll(void);

#endif /* ENHANCED_CLOG_H */
