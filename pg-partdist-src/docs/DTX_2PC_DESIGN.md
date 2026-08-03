# 分布式事务两阶段提交设计（DTX-2PC v1）

> **定位**：本文是跨分区事务原子提交的实现依据，与
> `pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md`（FRD，物理回放）、
> `pg-raft-src/docs/raft_module_revision_plan.md`（raft 计划）并列。
>
> **它取代了 raft 计划里的旧 2PC 设计**：计划 §4 阶段 3 与 §5 把
> `OP_PREPARE_DECISION` / `OP_COMMIT_DECISION` 设计成**控制面（group 0）日志操作**，
> 本文改为**决议进数据组**——group 0 只保留"拓扑登记处 + 配置权威"的职责（§13）。
> 理由见 §3.2。
>
> 代码基线：`shardpg-4.0` @ `468a518`（parwal-2.0 记录头、prepare 接线 §14 已落地、
> P3 物理回放未开始、惰性回放形态已定稿）。凡本文引用的代码事实均在该提交上复核过。

---

## 0. 一句话概括

事务涉及的每个分区组各自用**已有的分区 Raft 组**完成 prepare（数据字节达多数派
持久化）；**决议只写一条**，落在**从写集中按 `hash(dtxid)` 选出的那个分区组**的
日志里，该条记录达多数派即为全局提交点；随后各组异步追加 COMMIT/ABORT 标记。
决议缺失一律视为 ABORT（presumed abort）。单分区事务不走 2PC。

---

## 1. 与现状的关系：哪些已经有了

用户给出的三阶段流程里，**Prepare 阶段的四步在 `shardpg-4.0` 上已经落地**
（raft 计划 §14 "事务 prepare 阶段接线"，验收 raft_16）：

| 用户描述的 Prepare 步骤 | 现状 |
|---|---|
| 1) Leader 把 DATA Record 写 `pg_parwal/Si/` 并分配 `partition_lsn`，不含提交标记 | ✅ `wal_insert_hook` 捕获 + `PartWALFlush` 落盘 fsync（[A]）。commit record 属 RM_XACT，本就不进 parwal，"不含提交标记"自动成立 |
| 2) Leader 把该记录作为 Raft Log Entry 发给所有 Follower | ✅ `PartWALFlush` 末尾经 rendezvous 挂钩调 `pg_raft_partwal_replicate()`，逐条 `data_propose_one` |
| 3) Leader 与 Follower 都写本地 `pg_parwal` 并 fsync | ✅ follower **先 fsync 再 ack**（运输层加固 §11.5.1 #1/#2） |
| 4) 达多数派后进入 prepared | ⚠️ 多数派语义有了，但**有一个正确性漏洞**（§9.1 让路窗口），且没有"prepared 状态"这个事务状态机本体 |

所以本文真正要新建的是：**决议层（阶段 2）、标记层（阶段 3）、恢复层，以及把
阶段 1 的漏洞补上**。运输层一行都不用重写。

---

## 2. 对用户原始方案的四处修正

### 2.1 协调者定义为"组"，不是"worker"，且必须取自本事务写集

原方案：*Master 根据第一个目标分区 Leader 选择协调节点，协调者 worker 用其所属
Partition Raft 作事务状态日志。*

一个 worker 领导几十个组，"其所属 Partition Raft"不唯一，必须钉死一个。钉在
**本事务写集内的某个分区组**（而不是该 worker 名下任意组）白拿三个好处：

1. **省一轮**：该组既是决议持有者又是参与者，DECISION 记录**同时充当它自己的
   COMMIT 标记**（§5.3），阶段 2 与阶段 3 对这一组合并。
2. **协调权自动跟随切主**：协调组 leader 挂了，组内自治选举出新 leader，凭 Raft
   选举限制新 leader 必然持有全部已提交条目 ⇒ **决议不可能丢**。用户设想的
   "新 Leader 继续做协调者"由此免费获得，且不需要额外的状态搬迁。
3. **恢复寻址复用已有元数据**：参与者要找协调者时，查
   `partdist.partition_map` 的 `primary_node`（切主重构 §13 已保证它随自治选举
   实时更新并落到 `pg_dist_placement`）即可。

**防热点**（用户担心的"连续事务总访问某分区 ⇒ 该节点一直做协调者"）：
协调组 = `participants[hash(dtxid) mod n_participants]`（参与组按 `global_shard_id`
升序排列后取模）。同一事务的所有节点独立计算得同一结果，无需协商；不同事务
均匀散布。master 不领导任何数据组（§13 硬约束），天然不会被选中，与"master 不作
数据副本"自洽。

### 2.2 协调者宕机走 presumed abort，不做"续跑"

原方案：*协调者宕机 → 新分区 Leader 继续做这个事务的协调者，其他分区向新 Leader
汇报 prepared 或直接回滚重开。*

"续跑"要求新 leader 知道参与者清单，就得在 prepare **之前**先把清单持久化进协调组
——多一轮同步写，代价压在每一个事务的关键路径上，而收益只在故障路径。标准解法是
**推定中止**：

- 协调组日志里**查无 DECISION 即为 ABORT**。
- 参与者恢复时来问，协调组 leader **先在本组日志里写一条 ABORT DECISION 并达多数派，
  再答复**。这一步不能省——否则"问的时候没有、答完之后原提交路径又把 COMMIT 写进去"
  会造成同一事务两个决议。
- 决议槽一次性：**第一条进入协调组日志的决议获胜**，apply 侧按 `dtxid` 去重
  （§6.2），后到的同 dtxid 决议被忽略并告警。

代价是"协调者恰好在 decide 前宕机"的事务会被中止而不是救活。这类事务本来就还没
向客户端返回成功，中止完全合法；prepare 很快，重试便宜。

### 2.3 参与者不向协调者汇报 prepared，决议驱动权留在 master

原方案：*子事务路由至各 worker 时携带协调者节点信息，以便相关节点向协调者汇报
prepared 状态；协调者和 master 通信得知事务涉及哪些分区。*

master 本来就：(a) 持有客户端连接，(b) 知道完整写集（是它做的路由），(c) 会同步
收到每个参与者 `PREPARE TRANSACTION` 的 SQL 应答——**这个应答就是 prepared 汇报**。
再让每个 worker 单独向协调者发一轮汇报，是纯增的 N 次网络跳数，并且引入"协调者
还要反过来问 master 写集"的环形依赖。

因此：**正常路径由 master 收齐 ack 后调一次 `dtx_decide()`**；协调组的职责收窄为
两件事——**决议的持久化点**与**恢复期的权威应答方**。用户设想的汇报通道只在恢复
路径需要，由参与者侧的恢复守护进程（§7）承担。

### 2.4 阶段 3 的"Apply"按惰性回放改写

原方案：*副本收到 COMMIT Marker 后，将之前 prepared 的 DATA Record 正式 Apply 到
Heap/Index，使事务修改对外可见。*

这是**持续回放**的心智模型。本项目已定稿**惰性回放**（FRD 文首"形态修正"，
2026-08-01 用户确认）：follower 平时一条 redo 都不做，副本只是"冷字节"，
**也不服务读**；真正的 redo 只发生在升主追平那一刻。所以：

- **follower 侧**，收到 COMMIT 标记 = 把标记字节落盘 + 记账（`applied_part_lsn`
  推进）。没有"使之可见"这回事——它对外本来就不可见。
- **leader 侧**，事务是被 PostgreSQL 原生执行的，可见性由本地
  `COMMIT PREPARED` 完成，与标记记录无关。
- **升主时**，追平回放会顺序遇到 DATA → PREPARE 标记 → COMMIT 标记，此时才把
  事务状态写进增强型 CLOG（FRD §7.6），可见性在那一刻建立。

正确性不受影响：全局提交结果在阶段 2 已持久化，**Apply 不在客户端返回的关键路径上**
——这正是用户原文的判断，只是"Apply"的落点从 follower 常驻回放挪到了升主追平。

---

## 3. 协议

### 3.1 身份

两层，不要混：

| 标识 | 分配者 | 作用域 | 服务于 |
|---|---|---|---|
| `dtxid`（uint64） | master，事务初始化时 | 全集群唯一 | 2PC 决议寻址 |
| `coord_gsid`（int64） | master，写集确定后计算 | 该事务 | 指出决议写在哪个组 |
| `gxid`（FRD §9.1，`node<<48\|xid`） | 各 worker 本地 | 单分区内 | 回放层 MVCC（R2/R3） |

`dtxid` 直接复用 Citus 的 distributed transaction id（`initiator_node_identifier`
+ `transaction_number`，编码进 uint64），避免再造一个分配器。
PREPARE 标记负责把 `dtxid` 与该分区上的本地 top-level `xid` 绑起来——这是升主后
"in-doubt 事务属于哪个全局事务"的唯一线索（DATA 记录里只有 `xid`，没有 dtx 信息）。

### 3.2 为什么决议不放控制面（group 0）

raft 计划原设计把 `OP_COMMIT_DECISION` 放 group 0。三条理由推翻：

1. **写放大与单点**：group 0 leader 常态在 master，每个跨分区事务都要在 master 上
   做一次 Raft 多数派写。master 本就是 Citus 单点协调（§13.6 #4），把每事务的提交
   点也压上去，故障域和吞吐都变差。
2. **决议与数据不共命运**：group 0 的成员是全部 9 节点，数据组成员是分片副本集
   （3 个 worker）。决议在 group 0 提交、数据在数据组提交，两个多数派互不蕴含，
   恢复时要做跨组交叉判定。放进参与组则**决议与该组数据在同一条日志上定序**，
   语义塌缩成单组问题。
3. **切主已经把"找协调者"解决了**：§13 的自治选举 + 上报 + 落
   `pg_dist_placement` 让"某组现任 leader 是谁"成为随时可查的元数据。放数据组不
   引入新的服务发现问题。

形态上与 Spanner 一致（commit record 写在 coordinator Paxos group，而 coordinator
group 就是参与组之一），与 TiKV（decision 写 primary region 的 Raft）同源。

### 3.3 正常路径

```
master                          coord group Sc            其他参与组 Si
  │
  ├─ 分配 dtxid，路由子事务
  │   （携带 dtxid + coord_gsid）
  │
  ├─ 阶段1：对每个参与 worker 发 PREPARE TRANSACTION
  │       worker 侧 PRE_PREPARE:
  │         [A] parwal 落盘 fsync（DATA 批 + PREPARE 标记同一次 fsync）
  │          ↓  ensure_replicated(组, 本事务最大 plsn)      ← §9.1 修正后的语义
  │          ↓  等 commit_index ≥ 该 plsn（= 多数派已 fsync）
  │         [B] pg_wal 写 prepare 记录并 fsync
  │       返回成功 ⇒ 该组 prepared
  │
  ├─ 收齐全部 ack ──────────────▶ 阶段2：dtx_decide(dtxid, COMMIT, participants[])
  │                                  propose DECISION 到 Sc 的日志
  │                                  ├─ 多数派持久化 ⇒ **全局提交点**
  │                                  └─ 该记录同时是 Sc 自己的 COMMIT 标记
  │◀──────────────────────────────┘
  ├─ 向客户端返回 COMMIT 成功                       ← 关键路径到此为止
  │
  └─ 阶段3（异步）：对每个参与组发 COMMIT PREPARED ─────────▶ 追加 COMMIT 标记
                                                              （随下一次 flush 复制）
```

任一参与者 prepare 失败 → master 对全体 `ROLLBACK PREPARED`，并向已复制过数据的组
补 ABORT 标记；**不写 DECISION**（推定中止已经覆盖它，写 ABORT DECISION 只是为了
让恢复守护少等一个超时，是优化不是必需）。

### 3.4 快路径：单分区事务不走 2PC

绝大多数流量是 router query（带分区键的 INSERT/UPDATE/DELETE）。这类事务：

- **不分配协调组，不写 PREPARE/DECISION 记录**；
- 该组自己的 COMMIT 标记与本事务的 DATA 批**合并在同一次 flush、同一轮
  AppendEntries** 里达多数派——一轮搞定，开销与现状（raft_16 的形态）持平；
- 提交点定义见 §9.5（有一个需要拍板的窗口）。

判定：参与组数 ≤ 1 即快路径。**只读参与分片不计入**（§8.3）。

---

## 4. 协调组的选取

```c
/* 参与组按 global_shard_id 升序排列后取模，各节点独立计算得同一结果 */
coord_gsid = participants_sorted[ dtxid % n_participants ];
```

- master 在收齐写集后计算，随 `PREPARE TRANSACTION` 的 gid 下发给各参与者
  （见 §5.4 gid 编码），参与者无需再问。
- `n_participants` 取**真实写入**的组数（只读参与者已剔除，§8.3）。
- 协调组必须是**有数据组且本事务真的写了**的分区——不能选未纳管分区（那里没有
  Raft 日志可写决议）。若写集中所有分区都未纳管（无数据组），整个事务退化为
  现状行为（无 2PC，与接线前一致）。

---

## 5. 记录格式

### 5.1 沿用 parwal-2.0 的 40 字节头，不改布局

现状 `PartWALRecord`（`include/partition_wal_header.h`）为 40 字节，
`flags` 字段目前只用了 `PARTWAL_FLAG_DATA = 0x01`，其余位空闲。FRD v3 已为
parwal-3.0 预留 `MARKER = 0x02` / `CTRL = 0x04`。本设计**再取一位**：

```c
#define PARTWAL_FLAG_DATA      UINT8_C(0x01)   /* 已有：载荷=原始 XLogRecord   */
#define PARTWAL_FLAG_MARKER    UINT8_C(0x02)   /* FRD 预留：事务标记（R2）     */
#define PARTWAL_FLAG_CTRL      UINT8_C(0x04)   /* FRD 预留：控制记录（§12）    */
#define PARTWAL_FLAG_DTX       UINT8_C(0x08)   /* 本设计：分布式事务记录       */
```

**分类一律以 `flags` 判定，不以 `data_len` 判定**（FRD §4.1 已定的契约）。
DTX 记录带 `PARTWAL_FLAG_DTX`，回放侧在 flags 分派处就被路由走，**永不进入
`rm_redo`**，因此不需要动 `ShardReplaySkippable` 的 rmid/info 白名单。

头字段取值约定：

| 字段 | DTX 记录取值 |
|---|---|
| `rmid` | `RM_XACT_ID`（便于人眼与工具识别；回放侧不据此分派） |
| `info` | `DtxRecordKind`（见下），**不是** XLog info |
| `version` | 保持 `PARTWAL_RECORD_VERSION_2`；parwal-3.0 落地时随头一起升 3 |
| `orig_lsn` | **`InvalidXLogRecPtr`（0）** —— DTX 记录不是 WAL 记录，没有 leader 侧 end LSN。回放侧禁止用它盖页 LSN（本就不进 redo） |
| `xid` | 该分区上本事务的**本地 top-level xid**；无本地 xid（如纯决议记录写在未写数据的组）时为 0 |
| `partition_lsn` | 与 DATA 记录同一序号空间，正常递增 |
| `data_len` | `sizeof(DtxRecordPayload) + 8 * nparticipants` |

### 5.2 载荷

```c
/* include/dtx_record.h */
typedef enum
{
    DTX_PREPARE  = 1,   /* 参与组：本事务在本组已 prepared          */
    DTX_DECISION = 2,   /* 协调组：全局决议（verdict 见下）         */
    DTX_COMMIT   = 3,   /* 参与组：提交标记                         */
    DTX_ABORT    = 4    /* 参与组：中止标记                         */
} DtxRecordKind;        /* 存进头部 info 字段 */

typedef struct DtxRecordPayload
{
    uint64  dtxid;            /* 全局事务号                                    */
    int64   coord_gsid;       /* 协调组 global_shard_id；DECISION 中 = 自身    */
    uint64  commit_ts;        /* 提交时间戳；ABORT/PREPARE 为 0（TSO 未建时
                                 用协调者本地时钟，见 §9.8）                   */
    uint32  verdict;          /* 仅 DECISION 有效：1=COMMIT, 2=ABORT           */
    uint32  nparticipants;    /* 仅 DECISION 有效；其余为 0                    */
    /* int64 participants[nparticipants] 紧随其后：参与组的 global_shard_id 升序 */
} DtxRecordPayload;           /* sizeof = 32 */
```

`participants[]` 只在 DECISION 里出现，供阶段 3 的驱动与 GC 判定（§9.7）使用。
PREPARE 记录**不**带参与者清单——那正是 §2.2 拒绝"续跑"所省掉的东西。

### 5.3 DECISION 兼作协调组的 COMMIT 标记

协调组既是决议持有者又是参与者。一条 `DTX_DECISION{verdict=COMMIT}` 对本组而言
语义完全等价于 `DTX_COMMIT`，因此**协调组不再单独写 COMMIT 标记**。回放侧对
`DTX_DECISION` 的处理是：先记决议，再对 `coord_gsid == 本组` 的情形附加执行
COMMIT 标记的动作（写增强型 CLOG）。这是 §2.1 说的"省一轮"。

### 5.4 PREPARE TRANSACTION 的 gid 编码

参与者需要在**崩溃重启后**仅凭 `pg_prepared_xacts` 就能算出该找谁问决议，
所以 dtx 信息必须编进 gid（gid 是 PG 原生持久化的）：

```
shardpg_dtx_<dtxid>_<coord_gsid>
```

Citus 自己的 gid 形如 `citus_<initiator>_<pid>_<txn>_<shardid>`；我们不复用它，
而是在 pg_partdist 侧接管 gid 生成（§9.3 的挂点一并处理）。恢复守护解析该 gid
即可得到 `(dtxid, coord_gsid)`，无需任何本地表——**这是重启后唯一可靠的线索**。

### 5.5 复制通道需要携带 flags（签名变更）

**这是一处必须做的接口改动**：`partdist.partwal_read_record(oid, plsn)` 目前返回
`(orig_lsn, rmid, info, xid, data)`，**不含 `flags`**；`partwal_follower_append()`
也不接受 flags，落盘时恒写 DATA。于是 DTX 记录复制到 follower 后会退化成 DATA 记录，
升主回放时被当作 WAL 字节喂给 `rm_redo` —— PANIC 或静默损坏。

改动清单（改签名的连带项见 raft 计划 §5 的"维护约束"，2026-07-21 踩过一次）：

1. `partwal_read_record` 增加返回列 `flags`；
2. `partwal_follower_append` 增加参数 `p_flags`，透传给 `AppendPartWALRecordAt`；
3. `data_propose_one` 的 JSON 描述符增加 `"flags"` 键；
4. 同步修改：`COMMENT ON FUNCTION` 的参数列表、`setup-raft.sh` 的
   `ensure_boundary_functions` 及其 cleanup 段、
   `pg-install/share/postgresql/extension/` 下的已安装副本；
5. **验收必须包含全新库 `CREATE EXTENSION` 冒烟**——已装扩展不会重跑安装脚本，
   这类不一致在常规回归里完全静默。

---

## 6. 状态机与决议存储

### 6.1 参与者侧

```
        写入DATA           PREPARE标记达多数派        收到决议
  (none) ──────▶ WRITING ──────────────────────▶ PREPARED ──────▶ COMMITTED / ABORTED
                    │                                │
                    └── prepare 失败 ────▶ ABORTED ◀──┘（推定中止）
```

`PREPARED` 是唯一需要外部信息才能推进的状态，也是恢复守护（§7）的扫描对象。
本地表达就是 PG 原生的 prepared transaction（`pg_prepared_xacts`），不另造状态表。

### 6.2 协调组侧：决议索引表

DECISION 记录是权威，但按 dtxid 顺序扫段文件太慢。协调组的 **apply 路径**在每个
组成员上维护一张索引表：

```sql
CREATE TABLE partdist.dtx_decision (
    dtxid         BIGINT PRIMARY KEY,
    coord_gsid    BIGINT      NOT NULL,
    verdict       SMALLINT    NOT NULL,   -- 1=COMMIT, 2=ABORT
    commit_ts     BIGINT      NOT NULL,
    participants  BIGINT[]    NOT NULL,
    decided_plsn  BIGINT      NOT NULL,   -- 该 DECISION 记录的 partition_lsn
    acked         BIGINT[]    NOT NULL DEFAULT '{}',  -- 已回执的参与组（GC 用）
    decided_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

- **每个组成员都建**（apply 在全体成员上执行），所以切主后新 leader 手里天然有全表
  ——这正是 §2.1 好处 2 的落地形态。
- 写入走 `INSERT ... ON CONFLICT (dtxid) DO NOTHING` ⇒ **决议槽一次性**，重复 apply
  与迟到的第二条决议都被吞掉；`DO NOTHING` 命中时若 verdict 不同则 WARNING
  （正常情况下不该发生，发生即协议 bug）。
- 与控制面 apply 一样，整段包在子事务里，任何 SQL 错误只回滚它自己，
  **不允许打穿数据面 apply 游标**（§13.4 #1 的教训）。但注意数据面 apply 失败
  **必须重试**（漏一条 redo 即分叉），不能照抄控制面的"失败也推进"。
- 崩溃恢复：表本身是 WAL 保护的普通表；若与日志不一致（表落后），由数据组的
  apply 从 `last_applied` 重放补齐——与其他 apply 副作用同模型。

### 6.3 两个 SQL 入口

```sql
-- 在协调组 leader 上执行；propose DECISION 并等多数派。
-- 返回 true=已决议为 COMMIT，false=已决议为 ABORT（含"抢先被推定中止"）。
partdist.dtx_decide(p_dtxid BIGINT, p_verdict INT, p_participants BIGINT[]) → BOOLEAN

-- 参与者恢复时查询；无决议时**先写 ABORT DECISION 达多数派**再返回。
-- 返回 1=COMMIT, 2=ABORT；非 leader 返回 NULL（调用方按 partition_map 重新寻址）。
partdist.dtx_status(p_dtxid BIGINT) → INT
```

两者都必须在**独立事务**里运行（不能嵌在用户事务内），否则会踩计划 §14.3 #2
的"raft 日志 SQL 行与用户事务同命"。`dtx_decide` 由 master 经 libpq 单独调用，
`dtx_status` 由恢复守护调用，天然满足。

---

## 7. 恢复（presumed abort）

每个 worker 一个恢复守护（复用现有 BGW tick，不新起进程——`max_worker_processes`
在本环境实测为 8，见 FRD §7 的告诫）：

```
每 N 秒：
  for each p in pg_prepared_xacts where gid LIKE 'shardpg_dtx_%' and age > timeout:
      (dtxid, coord_gsid) := parse_gid(p.gid)
      coord_node := partition_map[coord_gsid].primary_node     ← 切主重构保证实时
      verdict := libpq(coord_node, "SELECT partdist.dtx_status(dtxid)")
      if verdict is NULL:  下轮重试（协调组正在选举 / 元数据未追平）
      elif verdict = COMMIT: COMMIT PREPARED p.gid;  追加 DTX_COMMIT 标记
      else:                  ROLLBACK PREPARED p.gid; 追加 DTX_ABORT 标记
```

要点：

- **超时只影响"多久开始问"，不影响正确性**。设得保守（如 30s）以免与正常路径的
  阶段 3 抢答；两者并发执行也安全，因为 `COMMIT PREPARED` 对同一 gid 只会成功一次，
  标记记录按 `dtxid` 幂等去重。
- **协调组切主间隙**：`dtx_decide` / `dtx_status` 打到非 leader 上会返回 NULL 或被
  写栅栏拒绝，守护重新读 `partition_map` 寻址即可。旧 leader 的幽灵提议由既有的
  任期栅栏 + Raft 选举限制拦下。
- **master 侧驱动 vs 守护侧驱动的分工**：正常路径由 master 推进阶段 3；master 挂了
  之后由守护兜底。两者收敛到同一结果。

---

## 8. 场景适用

### 8.1 批量插入（`INSERT INTO dist SELECT` / `COPY`）

Citus 把它转成对每个分片的 `COPY <shard> FROM STDIN`，走
`ProcessUtility_hook`（里程碑 2.4 已捕获）而非 ExecutorFinish。写多分片 ⇒ 标准 2PC。

**性能要点**：修完 §9.1 之后 prepare 语义从"逐条 propose"变成"确保复制推进到 X
并等 commit_index ≥ X"，并发写天然批量化，对批量插入是**改善**。真正的瓶颈是
现在"一条 record 一次 `data_propose_one` 一次 RPC"——批量场景要做 AppendEntries
多条打包/流水线。**这是性能项，不是正确性项**，可在 2PC 之后单独立项。

### 8.2 带分区键的 UPDATE

Citus router 到单分片 ⇒ **快路径**（§3.4），一轮 quorum，不碰协调组。
修改分区键值的 UPDATE 被 Citus 本身拒绝（或需拆成 delete+insert），后者按多分片
2PC 兜住。

### 8.3 未给分区键的 UPDATE / DELETE

广播到该表全部分片，是最重的形态。两处优化：

1. **只读参与者剔除**：命中 0 行的分片没有产生任何 parwal 记录，
   **不进参与者清单、不写 PREPARE/COMMIT 标记、PREPARE 直接 ack**。判据现成：
   `PartWALFlush` 的 per-backend touched 集合（§9.1 要新建的那个）为空即只读。
   选择性高的 WHERE 条件下，实际参与组数远小于分片数。
2. **协调组 hash 散布**（§4）：避免固定分片长期背协调开销。

最坏情况（真的每个分片都改了行）没有捷径：N 组并行 prepare + 1 次决议。
这与任何 2PC 系统同量级。

### 8.4 DDL 与 reference 表

- **reference 表与数据组不兼容**（计划 §14.3 #3 既有结论）：它在每个节点都是本地
  主写，非 leader 节点的写入会被写栅栏拒绝。reference 表不建数据组，因此
  **不参与 2PC**，维持现状。
- **DDL** 走 FRD §12 的 FILESET_UPDATE 控制记录路径，不在本设计范围。

---

## 9. 隐患清单

按严重度排序。**#0 是新发现的头号阻断项**；#1 与 #2 是开工前必须先修的。

### 9.0 【已修复】并发写入路径的三个隐藏缺陷（2026-08-03 发现并全部修复）

首次用并发负载（6 会话单行 autocommit INSERT 打同一分片）压 prepare 路径时连环
暴露。此前所有用例（raft_13/14/16、P0 回归）都是**串行**写入，三条路径全都从未
被真正执行过。逐个记录（都有 gdb backtrace 或决定性复现支撑）：

**缺陷 1：`ReadRawWALRecordAt` 的 reader use-after-free（segfault 主犯）。**
group-commit 场景下 backend X 替 peer 落盘时需按 `start_lsn` 从 pg_wal 回读字节，
所用的 `static XLogReaderState *raw_reader` 是**懒分配**的——而首次调用发生在
PRE_COMMIT 回调里，彼时 `CurrentMemoryContext` 是**事务级**上下文：static 指针
跨事务存活，reader 结构与内部缓冲却随事务结束被释放。第二次进来就是 UAF，写穿
的是下一个事务复用同一内存后的 palloc chunk。症状：`pfree called with invalid
pointer 0x...`（每次同一地址——fork 出的 backend 分配序列相同）或直接 SIGSEGV。
**修复**：整个分配 + `XLogReadRecord` 过程切到 `TopMemoryContext`（读取期间的
懒分配——decode buffer、readRecordBuf 扩容——同样必须覆盖，只包 Allocate 不够）。
最小复现（不需要任何 raft 组）修复前 3/3 崩、修复后 3/3 干净。
顺带修掉 `PartWALReadPage` 把短读当成功、以及提前返回路径不释放
`partwal_pending` 的两个次级问题。

**缺陷 2：`data_propose_one` 对 NULL 行解引用（失多数派场景的 segfault）。**
gdb backtrace：`pg_detoast_datum_packed ← text_to_cstring ← data_propose_one`，
`si_addr=0x0`。`partwal_read_record` 查不到记录时 `PG_RETURN_NULL()`——在
`SELECT ... FROM f(...)` 形态下这是**一行全 NULL**（`SPI_processed==1`），
而调用方 5 个 `SPI_getbinval` 一个 `isnull` 都没检查。**修复**：逐列判 NULL，
记录缺失按 propose 失败返回 0（事务中止，与失多数派语义一致）；顺带修掉
`orig_lsn` 字符串在 `SPI_finish` 之后仍被使用的潜伏 UAF。

**缺陷 3：leader 侧失败回滚截断并发事务的字节（丢数据，raft_17 阶段二抓获）。**
propose 失败时 `discard_uncommitted_entry` 按被丢弃条目的 plsn 调
`partwal_truncate_to(plsn-1)`——但 parwal 流里 plsn 之后可能已躺着**并发事务**
在 [A] 落盘的记录，截断连它们一起删。受害 backend 的复制挂钩随后看到
`last_data_plsn >= flush_lsn`，空转返回，其事务**带着"已复制"的假象提交**——
数据既不在本地 parwal 也没到任何 follower（实测失多数派下 13/240 行如此漏网）。
**修复**：leader 侧失败只回滚 Raft 日志条目（ring + SQL 行），**字节不截断**，
失败条留作孤儿、下次复制按同一 plsn 重新 propose；follower 侧 AppendEntries
冲突截断保留（换 leader 后同一 plsn 承载不同记录，那个截断才是必须的）。
语义变更：**"中止事务不在 plsn 空间留渣"作废**——parwal 流里可以有中止事务的
DATA 记录，可见性由标记/决议闭合（§5、FRD §7.6），与 2PC 的设计本就一致。
另外 `partwal_truncate_to` 补上了与 `PartWALFlush` 追加者的
`PartWALCtl->lock` 互斥（此前完全无锁，实测出现 checkpoint.tmp rename 竞争）。

**方法论教训**：崩溃会重置 shmem 组表，之后的 INSERT 因"查不到数据组"跳过
复制挂钩并提交成功——症状与让路窗口一模一样。**任何并发验收都必须先甄别
"burst 期间是否有节点崩溃"**（raft_17 两个阶段都做了这层甄别）。

### 9.1 【最高】group-commit 让路窗口：从性能弱化升级为正确性 bug

**现状**（`src/wal/partwal_sync.c:558-568`）：

```c
if (PartWALCtl->flushed_upto != InvalidXLogRecPtr &&
    PartWALCtl->flushed_upto >= upto_lsn)
{
    LWLockRelease(PartWALCtl->lock);
    partwal_my_max_lsn = InvalidXLogRecPtr;
    return;                       /* ← 直接返回，复制挂钩根本没被调用 */
}
```

并发 backend 顺带把本事务的记录落了盘，本 backend 提前返回，**复制挂钩不触发**。
更糟的是 `touched[]` 只登记 `slot->backend_id == MyBackendId` 的槽位（同文件
`:678-685`），而槽位已被 peer 消费置 `valid=false` ⇒ **本 backend 事后也无从知道
自己写过哪些分区**。

现状下这只是"复制延后到该分区下一次写入"（计划 §14.3 #1 记为性能弱化）。
一旦引入 2PC，它变成：**PREPARE 返回成功、但这些字节从未达到多数派** ⇒
协调者据此写 COMMIT 决议 ⇒ 参与组多数派上根本没有这个事务的数据。**这是丢数据。**

> **★ 怎么测它（2026-08-03 实测定稿）**：**看终态测不出来**——漏掉的记录会在
> 该分区下一次非提前返回的 flush 里被增量下界顺带补齐，burst 后 leader 与
> follower 几乎总是收敛，实测未修复构建照样通过"follower 逐字节追平"判据
> （raft_17 阶段一即如此，两个构建都 3/3）。概率性判据（并发碰撞、失多数派下
> 并发写）灵敏度不可控，且本轮定位曾被"装错构建"污染出过错误结论
> （docker cp 的 mtime 陷阱，见交接台账），一律不采信。
>
> **有效判据是确定性构造一次让路**（raft_17 阶段二）：长事务 P 写 B 组后
> `pg_sleep`；并发短事务 Q 写同 worker 的 A 组，其 flush 顺带消费 P 的槽位并把
> `flushed_upto` 推过 P；P 提交时**必然**走提前返回分支。**真对照实验**
> （同环境同用例，仅换 `.so` 且以 nm 符号验证过构建身份）：
> 让路窗口未修 ⇒ 阶段二确定性失败——P 提交成功、其 2 条记录（heap+btree）
> 只在 leader（`leader=43, follower=41,41`），挂钩未被调用、B 组零 propose、
> 此后不写 B 无自愈；修复后 ⇒ 同一位置 `43/43✔/43✔`。全程多数派健康、秒级、
> 无概率成分。"失多数派 + 并发写 ⇒ 提交成功行数 == 0"保留为 raft_17 阶段三
> ——它是 2PC prepare 性质的回归（raft_16 第 2 步的并发版本），不是让路窗口的
> 检测器。
>
> 归因更正：此前记录的"13/240 漏网"发生在让路窗口**已修复**的构建上，
> 真凶是 §9.0 缺陷 3（失败者截断并发事务字节 → 受害者挂钩空转），不是本缺陷。

**修法**（两处，缺一不可）：

1. **触达集合改为 per-backend 在 insert 时记录**，不再从 flush 时的 ring 反推。
   `partwal_pending[]` 已经是 per-backend 且在 `PartWALInsert` 时填充
   （`:359-381`），给它加一个 `Oid partition_id` 字段即可，零额外数据结构。
2. **两条路径都调复制挂钩**：提前返回前也要对本 backend 的触达集合调一次。
   这时本事务的记录已被 peer 写进段文件，`get_partition_flush_lsn(oid)` 返回的
   flush 点**必然 ≥ 本事务记录的 plsn**，所以"复制到当前 flush 点"是安全的上界。

**顺带把 prepare 语义收紧**（用户方案的第 4 步）：把
`pg_raft_partwal_replicate()` 从"我来逐条 propose 我的范围"改成
**"确保本组复制已推进到 ≥ X，并等 `commit_index` 覆盖它"**。Raft 日志的前缀性质
保证：`commit_index` 覆盖某条目 ⇒ 它之前的全部条目都已在多数派持久化。
好处有三：并发下不再有两个 backend 抢同一段 plsn 重复 propose（现状会白烧 ring
槽位，`RAFT_LOG_CAPACITY=128`）；批量场景天然合批；语义与用户描述的
"达多数派持久化后进入 prepared"精确对应。

### 9.2 【最高】成员集不随 RPC 传播 ⇒ 多数派算错

计划 §12.3.A.3 的既有阻断项，对 2PC 是**正确性前提**：自动建组（hearsay）的
follower 成员集为空，按**全体 peers** 算多数派。分区副本集是全体节点的真子集
（本环境 8 worker，副本集通常 3），于是"多数派"算的是 5/9 而不是 2/3——
prepare 和 decision 的 quorum 都是假的。

**过渡方案**（不等 `OP_CONFIG_CHANGE` 落地）：所有数据组必须经显式
`pg_raft_group_create(gid, members[])` 建立，并在 `group_propose` 入口断言
成员集非空；成员集为空的组**拒绝**参与 2PC（退化为无 2PC 的现状行为）。
**正解**仍是控制面 `OP_CONFIG_CHANGE` 下发。

### 9.3 【高】master 侧 decide 的挂点：需要内核补丁 0004

`dtx_decide` 必须严格发生在"Citus 发完全部 `PREPARE TRANSACTION` 之后、
向客户端返回之前"。

**为什么不能用 XactCallback**：回调是 LIFO 顺序。pg_partdist 在 Citus 之后加载，
它注册的 `XACT_EVENT_PRE_COMMIT` 回调会跑在 **Citus 的之前**——那时 Citus 还没发
PREPARE，我们无从知道 prepared 是否成功。而调整加载顺序这条路是堵死的：
Citus 强制要求自己排 `shared_preload_libraries` 第一位。

**方案：内核补丁 0004**，在 `CommitTransaction()` 里、`CallXactCallbacks(XACT_EVENT_PRE_COMMIT)`
**之后**、`RecordTransactionCommit()` **之前**加一个 hook：

```c
/* xact.c */
typedef void (*pre_record_commit_hook_type)(void);
extern PGDLLIMPORT pre_record_commit_hook_type pre_record_commit_hook;
```

这个位置的关键性质：**此时本地 commit record 尚未写入，hook 里 ERROR 还能干净地
中止整个事务**（参与者的 prepared 事务随后被推定中止）。若放在 post-commit
（`XACT_EVENT_COMMIT`），本地已经提交，决议失败时无法回头——**绝对不能放那里**。

补丁落地后**必须重编 PG 并把 `pg-install/` 一并提交**（`468a518` 的教训：
仓库跟踪的预编译 pg-install 与 `patches/` 不同步会让一键复现直接断在编译）。

### 9.4 【高】Citus 自带的 2PC 恢复会与我们打架

Citus 把 `pg_dist_transaction` 当决议真相源：master 本地提交成功后，它的
maintenance daemon 会对残留的 prepared 事务无条件 `COMMIT PREPARED`。若我们的
协调组决议是 ABORT（例如 `dtx_decide` 失败后事务中止），Citus 的恢复会造成
**分叉提交**——一部分参与者按我们的决议回滚、另一部分被 Citus 提交。

**处置**：`citus.recover_2pc_interval = -1`（关闭 Citus 的 2PC 恢复），
由 §7 的守护统一处置；`pg_dist_transaction` 降级为参与者提示，
**权威真相源 = 协调组日志**。这条要写进环境搭建脚本（`reproduce-env.sh` 的
postgresql.conf 段）与验收前置检查。

### 9.5 【中】快路径的提交点定义需要拍板

单分区事务沿用现有 `[A] → quorum → [B]` 时序时存在一个窗口：标记与数据已在组内
达多数派，但 leader 本地 `[B]`（pg_wal commit fsync）之前崩溃 ⇒ **组内认为已提交、
leader 本地事务却中止**，leader 与自己的组分叉。

两个解法：

- **(a) 单分区也走本地隐式 PREPARE**：提交点 = 组日志。最干净，但每事务多一次
  prepare 开销，快路径的意义削掉一半。
- **(b) 沿用现时序，在故障路径付代价**：接受该窗口，规定**旧 leader 崩溃后归队时，
  对检测到状态分叉的分片强制重做物理基线**。惰性回放本来就支持 re-baseline
  （FRD §8.5 的 fail-safe 路径），代价只在故障路径付。

**建议先 (b)，留 (a) 作升级路径。** 无论选哪个，规则都必须写进 FRD §11 的
升主/归队序列，否则归队路径会静默带着分叉数据。

### 9.6 【中】升主序列要加一步"清 in-doubt"

FRD §11 的六步收尾之后、该分片对外服务之前，必须插入：

```
扫描本组已追平的记录，找出「有 DATA + DTX_PREPARE 标记，但无 DTX_COMMIT/ABORT 标记」
的 dtxid：
  - 逐一向 coord_gsid 的现任 leader 调 dtx_status()，按结果补写标记；
  - 连 PREPARE 标记都没有的（DATA 写了一半就崩）→ 直接补 ABORT 标记。
```

**这一步不依赖 R2/R3**：它是**按事务粒度**求决议并写标记，不是元组粒度的可见性
判定。所以 2PC **不新增**对 R3 的阻塞；同时它也**不解除**既有阻塞——
promoted 副本可读仍然卡在 xid_map + 增强型 CLOG（FRD §14.2 的 R4←R3 硬阻断
原样有效）。**这一点必须写清楚，避免"2PC 做完就能切主读写"的误判。**

### 9.7 【中】容量与 GC

- DECISION / 标记记录同样占 `RAFT_LOG_CAPACITY=128` 的 ring 槽位与 plsn 空间。
- presumed abort 要求 **COMMIT DECISION 必须保留到全部参与组回执之后**才能随日志
  截断被 GC（`dtx_decision.acked` 就是为此存在）——否则一个恢复慢的参与者来问时
  决议已被截断，会被错误地推定中止，而它的本地事务其实该提交。**这是丢一致性，
  不是丢性能。**
- 这两条都指向计划 §12.4 #3"数据组日志外部化到 parwal"。2PC 会把它从容量问题
  加速变成正确性问题，应提前排期。

### 9.8 【中】锁滞留与 TSO 缺位

- prepared 事务持锁直到决议为止。协调组不可达时挂起时长比现状长，需要：恢复守护
  超时参数化、`pg_prepared_xacts` 年龄监控、复核 Citus 死锁检测在 prepared 窗口
  变宽后的行为。
- **TSO 未建**：`commit_ts` 先填协调者本地时钟。决议的原子性不依赖它
  （原子性来自 Raft 多数派）；全局快照一致性等 R3/TSO 立项时再收紧。
  FRD §7.6 的可见性判据 `commit_ts ≤ 快照 start_ts` 在 TSO 落地前不成立，
  这与 R3 未实装是同一件事，不构成新增阻断。

### 9.9 【低】raft 日志 SQL 行与用户事务同命

计划 §14.3 #2。DECISION 不受影响（`dtx_decide` 在独立事务里）；
PREPARE 标记在用户事务内 propose 仍有窄窗口，与现状同级风险。
短期靠 follower 落盘幂等 + 增量下界容忍，根治靠日志外部化。

---

## 10. 落地顺序与验收

| 步 | 内容 | 验收 |
|---|---|---|
| 0 ✅ | **§9.0 三缺陷修复**（reader UAF / NULL 行解引用 / leader 截断丢数据 + truncate 加锁） | 无组并发 burst 修复前 3/3 崩、修复后 3/3 干净；raft_17 全程无崩溃；缺陷 3 由旧判据抓获（13/240 丢数据）后复测归零 |
| 1 ✅ | **修让路窗口**（§9.1）：per-backend 触达集合 + 两条路径都触发挂钩 + 本组复制串行化 | **raft_17 三阶段**（判据演进见 §9.1 方框）：阶段一并发终态多数派、阶段二确定性让路（长事务 P 被让路后其记录仍须达多数派——未修复构建在此必败）、阶段三失多数派提交行数=0 |
| 2 | **成员集显式化**（§9.2）：过渡断言，空成员集组拒绝参与 2PC | 副本集为全体真子集的组，quorum 按真实成员数计算 |
| 3 | **记录格式**（§5）：`PARTWAL_FLAG_DTX` + `DtxRecordPayload` + `partwal_read_record`/`partwal_follower_append` 携带 flags | 全新库 `CREATE EXTENSION` 冒烟；follower 侧 DTX 记录 flags 保真 |
| 4 | **决议层**（§6）：`dtx_decision` 表 + `dtx_decide`/`dtx_status` + apply 索引维护；**补丁 0004** + master 挂点；关 Citus 2PC 恢复 | **raft_18**：prepare 后 decide 前杀协调者 → 全体推定中止；decide 落盘后杀协调者 → 参与者经恢复得 COMMIT |
| 5 | **恢复守护 + 快路径 + 只读参与者剔除** | **raft_19**：协调组切主后决议仍可查、任期栅栏拦下旧 leader 的幽灵决议；单分区事务不产生 PREPARE/DECISION 记录 |
| 6 | **升主 in-doubt 清理**（§9.6）+ 快路径分叉归队规则（§9.5） | 与惰性回放的 promotion 路径合流验收 |

**不回退基线**：`run-raft-tests.sh` 现 32/32（raft_01–16）+ P0 10/10 + 全新库
`CREATE EXTENSION` 冒烟，每步都必须保持全绿。

---

## 11. 对既有文档的修订要求

本文定稿后，下列段落已过时，须同步修改：

**`pg-raft-src/docs/raft_module_revision_plan.md`**
- §4 阶段 3 "其余（2PC 决议，暂缓）"：`OP_PREPARE_DECISION`/`OP_COMMIT_DECISION`
  作为**控制面**操作的设计被本文 §3.2 取代；"暂缓"解除。
- §5 控制面日志操作表：两个 OP 从"阶段 3 未实现"改为"已废弃，见 DTX_2PC_DESIGN"。
- §8 风险表 "2PC 决议丢失会导致部分提交 → 通过控制面 Raft 复制"：对策改为数据组决议。
- §11.6 #4 "跨分区 2PC ... 需协调者跨组屏障"：由本文回答。
- §12.4 #7 与 §14.3 #1：前者的 2PC 条目指向本文；后者从"边界"升级为
  **正确性阻断项**（§9.1）。

**`pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md`**
- §4.3 `TxnMarkerPayload`：需与本文 §5.2 的 `DtxRecordPayload` 对齐——
  标记记录要能携带 `dtxid`，且 PREPARE 是新增的标记类型。
- §7.6 只处理 COMMIT/ABORT，须补 in-doubt（PREPARE 已见、决议未见）的处理；
  §9.3 枚举里现成的 `TXN_PREPARED` 正好用上。
- §11 升主六步：插入 §9.6 的"清 in-doubt"步骤，以及 §9.5 快路径分叉的归队规则。
