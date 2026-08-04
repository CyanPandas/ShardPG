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
>
> **实施进度（2026-08-04）**：§10 的第 0–5 步全部落地并验收（raft_17–22）。
> 跨分区事务**已经真的走 2PC**：内核补丁 0004 开的 `pre_record_commit_hook` 把决议
> 接进了客户端提交路径，决议在协调组达多数派持久化即为全局提交点。
> 剩第 6 步（升主 in-doubt 清理 + 快路径分叉归队规则），与惰性回放的 promotion
> 路径合流时再做。

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
阶段 1 的漏洞补上**。运输层一行都不用重写——**实际实施下来确实如此**：
`pg_raft_partwal_replicate` / `data_propose_one` / AppendEntries 这条链路
在整个 2PC 落地过程中只改了"记录头要带 flags"（§5.5），语义一行没动。

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

### 3.5 为什么参与者仍要写本地 prepare 记录（[B] 不能省）

有一个自然的疑问：字节已经在 parwal 达多数派持久化了（[A] + quorum），
参与者还要不要再付 `PREPARE TRANSACTION` 的 pg_wal fsync（[B]）？
**要，而且在当前架构下没有商量余地**（2026-08-04 审查时把理由写死在这里）：

1. **[B] 承载的是参与者本地的 in-doubt 事务本体**，parwal 承载不了。
   leader 上事务是被 PostgreSQL 原生执行的：堆/索引改动的可见性、持有的锁、
   `pg_prepared_xacts` 这条崩溃后唯一的线索（§5.4 的 gid 就持久化在这里），
   全部由 prepare 的 WAL 记录背书。**本项目没有任何"从 parwal 回放进本地堆"
   的路径**——demux 崩溃恢复的方向是反的（pg_wal → parwal），惰性回放只服务
   follower 升主，leader 自己的恢复完全依赖 pg_wal。

2. **只 fsync parwal 的具体灾难形态**：参与者在 ack prepare 之后崩溃。
   PG 崩溃恢复查无 prepare 记录 ⇒ 本地事务按中止清理；而组内多数派已持久化、
   协调组可能已写下 COMMIT 决议 ⇒ **主副本自己的堆与自己的组日志分叉**，
   且它作为 leader 重启后继续对外服务——follower 手里都有的字节，唯独主没有，
   静默丢数据。这不是概率问题，是该窗口下的必然结果。

3. **[A] < [B] 这个顺序本身就是不变式**（§3.3）：它保证"leader 本地已持久化的
   事务状态，必然已经在组日志里且已复制到多数派"。follower 升主因此不需要问
   旧主任何问题。把 [B] 删掉或异步化，都会制造"本地已 prepared、组内没有"的
   反例，切主安全性整个塌掉。

4. **PG 里也没有"只省 fsync 不省记录"的开关**：`PREPARE TRANSACTION` 强制
   同步落盘（不受 synchronous_commit 影响），要省就只能整个不用原生 2PC——
   那等价于"日志即数据库"：leader 重启时从 parwal 自回放重建状态。那是
   §9.5(a) 的方向，前置是 R2/R3（xid_map + 增强型 CLOG），现在不存在。

成本视角：[B] 是一次 pg_wal fsync，与本组其他事务的提交记录共享 group commit；
真正贵的是 quorum RTT，不是它。

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

> **★ 实施时踩到的两个坑（2026-08-03，都会静默失败）**：
> 1. **`CREATE OR REPLACE` 改不了返回类型**。`partwal_read_record` 加了 OUT 列，
>    直接 REPLACE 报 `cannot change return type of existing function`，必须先 DROP。
> 2. **这两个函数是 pg_partdist 的扩展成员**，直接 `DROP FUNCTION` 会被
>    `cannot drop function ... because extension pg_partdist requires it` 拒绝，
>    必须先 `ALTER EXTENSION pg_partdist DROP FUNCTION ...` 解除归属再 DROP。
>
> 要命的是 `setup-raft.sh` 的 cleanup 段整体是 `ON_ERROR_STOP=0` 且输出重定向的
> ——两个错误都**无声无息**，结果是既有库里留下新旧两个 `partwal_follower_append`
> 重载、而 `partwal_read_record` 根本没更新。**唯一能发现它的就是全新库冒烟**
> （已固化为 raft_19 A 段）。setup-raft.sh 的这段已改写为 DO 块：
> 先 ALTER EXTENSION 解除归属、再 DROP，真失败时 `RAISE WARNING` 出声。
> 顺带把几条一直在静默失败的历史 DROP 行（同样撞扩展成员）一并折叠进去。
>
> 另：`pg_partdist` 在 `shared_preload_libraries` 里，`make install` 之后**必须
> 重启节点**新 `.so` 才生效——否则 `CREATE EXTENSION` 会报
> `could not find function "..." in file`，看起来像符号没导出，其实是旧镜像。

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

> **落地形态（2026-08-04，raft_21 H 验收）**：不新起进程——每个节点的
> TopologyMonitor 按 `pg_raft.dtx_recover_interval_ms`（默认 10s，0=关）经
> libpq 自连接调 `partdist.dtx_recover_prepared(pg_raft.dtx_recover_timeout_ms)`
> （默认 30s），与 self-probe 同一手法（BGW 内不做 SPI；该函数内部还要开
> libpq 自连接跑 `COMMIT/ROLLBACK PREPARED`，天然要求顶层语境）。
> 每个节点都跑：恢复是参与者本地的事，与是否 leader 无关；无 prepared 事务时
> 空转一条 SELECT。**在此之前守护只能手工 SQL 调用——master 崩溃后 in-doubt
> 会连同行锁一直挂着，这是 5a 的一个实打实的缺口（审查时补上）。**

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

按严重度排序。#0 / #1 / #2 / #2.5 于 2026-08-03 修复并验收；
#2.6 / #3 / #4 于 2026-08-04 修复并验收；#5 起为尚未处理项。

### 9.2.5 【已修复】prepared 事务的行锁把决议路径锁死（2026-08-03，实施第 5 步时发现）

**症状**：分片上存在 prepared 事务时，`dtx_decide` **永久阻塞**在
`Lock / transactionid`（等那个 prepared 事务结束）。

**根因**：prepare 路径在**用户事务内**触发复制（`PartWALFlush` 的挂钩），而
`group_propose` 末尾的 inline `group_apply_pending()` 会 UPSERT
`partdist.follower_partition_map` 的进度行。该事务随后进入 PREPARED 状态，
**这把行锁就一直被持有**。

而 2PC 的基本形态恰恰是"prepare 之后保持 in-doubt、再由协调者做决议"——
协调者 `dtx_decide` 自己的复制又会触发 apply、去 UPSERT 同一行，于是撞上
prepared 事务的行锁，永久等待。**这不是测试夹具的问题**：只要 2PC 成立，
这条死锁就必然出现，它会阻断整个端到端流程。

**修复（收敛到唯一真正冲突的那条语句）**：用户事务内的复制路径
（prepare 挂钩、`dtx_decide`、`dtx_status`）跳过 `data_entry_apply` 里
**对 `follower_partition_map` 的进度 UPSERT**（`in_txn_replication` 标志），
其余照旧——**游标照常推进、DTX 决议索引照常维护**。

> **★ 两次修错，都值得记下来：**
> 1. 第一版只挡 `group_propose` **尾部**的 `group_apply_pending()`。不够 ——
>    它**开头**还有一个（"先把积压 apply 掉再 append"，防重启后误判环满），
>    同样跑在用户事务里。`dtx_decide` 依旧阻塞，靠 `pg_locks` 看到它正持着
>    `partdist.follower_partition_map` 的 `RowExclusiveLock` 才定位到。
> 2. 第二版改成"用户事务内整个跳过 apply"。**这个代价我严重低估了**：
>    写入负载下每次 propose 都在用户事务里，`last_applied` 于是**永不推进**，
>    环容量检查（`last_log_index - last_applied`）很快判满、拒收新条目 ——
>    raft_17 实测 follower 卡死在 **127**（`RAFT_LOG_CAPACITY = 128`）。
>    这不是"重启后偶发"，是稳定复现的吞吐塌陷。
>
> 最终收敛到只跳过那一条 UPSERT：它是**唯一**会与 prepared 事务撞锁的东西。

代价：本节点这张表里的 `applied_part_lsn` 在纯 2PC 负载下会滞后。
**follower 侧不受影响**——它们的 apply 跑在 `pg_raft_append_entries` 这个顶层
SQL 调用里，不在任何 prepared 事务内。该列服务于切主候选筛选，滞后只会让本
节点显得"没追平"，是**保守方向**，不会把没追平的误判成已追平。
**`commit_index` 与多数派持久化不受影响——提交点语义完全不变。**

> 教训与 §9.0 同源：**只有真的把形态跑起来，设计上的隐含耦合才会暴露**。
> 这条在"决议层单独验收"（raft_20，没有 prepared 事务参与）时完全测不出来，
> 必须等 raft_21 把 prepared 事务和决议放到一起才现形。

### 9.2.6 【已修复】DTX 记录被写进段号 1，排到了它所标记的数据前面（2026-08-04）

**症状**：跨分区事务跑通之后，raft_14 / raft_16 的 follower 断言
`partition_lsn 非严格单调或记录损坏（1..9 应连续无洞）`。**leader 上同样失败**，
所以不是复制通道的问题。dump 记录头看到的顺序是 `7,8,1,2,3,4,5,6`，
其中 7、8 的 `orig_lsn` 是 `0/0` —— 正是两条 DTX 记录（PREPARE + COMMIT）。

**根因**：`AppendPartWALRecordAt()` 用 `orig_lsn` 映射段文件号，而
`orig_lsn == InvalidXLogRecPtr` 被无条件映射到**段 1**：

```c
if (orig_lsn == InvalidXLogRecPtr)
    new_segno = 1;                     /* ← DTX 记录恒落段 1 */
else
    XLByteToSeg(orig_lsn, new_segno, wal_segment_size);
```

DATA 记录按各自的 pg_wal LSN 落在段 7，DTX 记录落在段 1。段文件**按文件名排序
即是回放顺序**（`verify_partition_wal` 与物理回放都这么读），于是标记物理上排在
它所标记的数据之前。这不只是让校验函数报错——升主回放会先看到 COMMIT 标记、
再看到 DATA，FRD §7.6 建立可见性的顺序整个反过来。

**为什么现在才现形**：DTX 记录此前只在 raft_19/20/21 里出现，那些用例要么用
专门的空分区，要么按 plsn 直接定位读取，从不做"按文件顺序扫一遍"的判定。
**2PC 接线让 DTX 记录第一次和 DATA 落进同一条分区流**，潜伏缺陷才暴露。
这与 §9.0 是同一条教训：**只有真的把形态跑起来，隐含耦合才会暴露**。

**修复**（`partition_wal_writer.c`，两处，缺一不可）：

1. `orig_lsn == 0` 时**跟随当前段**，三级依据：写入器正开着的段 →
   checkpoint 里最后一条 DATA 的 `orig_lsn` 所在段 → 都没有才回落到 1。
2. `last_wal_lsn` **不得被 `orig_lsn == 0` 覆盖**——它是"当前段号"的持久化依据，
   被 0 覆盖后 checkpoint 也变 0，下一个写入器就找不回当前段了。

修复后同一构造下 leader 与两个 follower 的记录序列均为 `1..8` 且 `verify=t`。

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

### 9.2 【已修复】成员集未知时被当作"全体节点" ⇒ 多数派算错（2026-08-03）

计划 §12.3.A.3 的既有阻断项，对 2PC 是**正确性前提**。

**根因**：`n_members == 0` 被重载为"全体节点"。这对**控制面组**成立（组 0 的
成员本来就是全部节点），对**数据组永远不成立**——一个分片的副本集必然是全体
节点的真子集。

**实测后果**（9 节点环境，以 SQL 默认的 `p_members = NULL` 建组）：

| 观察 | 数值 |
|---|---|
| `cluster_size` / 多数派 | 9 / 5（实际只有 3 个节点持有该分片数据） |
| 非副本节点 | 收到广播的 RequestVote 后 **hearsay 自动建组**（同样空成员集）并参与投票 |
| 真正的数据持有者 | 被挤成 `term=0` 的 follower，**分片彻底不可用** |
| 写入 | `record 1 复制未达多数派` —— 三个副本全在也写不进去 |

比"算错"更本质的危险：同一个组在不同节点上存在**两套不相交的多数派定义**
（3 副本节点算 2/3、hearsay 节点算 5/9），Leader Completeness 失去交集保证；
非副本节点还可以赢得它根本没有数据的分片的领导权。

**修复**（`shardpg-4.0`，raft_18 验收）：

1. **语义改为 fail-stop，但只剥夺"主动参与"**：数据组的 `n_members == 0` 表示
   **成员集未知**，而非"全体"。未知的节点**不竞选、不当选、不提案**；
   `group_has_member()` 对数据组的空成员集返回 false，于是 `group_tick` 与
   `peer_in_group` 天然把整条广播链掐死在源头。
   `cluster_majority()` 在规模未知时返回一个**不可能达到**的票数（fail-closed
   兜底，绝不退化成 `0/2+1 = 1` 而"一票自行提交"）。

   > **★ 边界必须划在"主动 vs 被动"上，不能一刀切（实测教训）。**
   > 初版把 `pg_raft_rpc` / `pg_raft_append_entries` 也一并拒绝，结果
   > raft_14/15/16/17 全挂——因为数据组的引导流程（计划 §11.5.2）是
   > **"只在 placement 节点先建组，其余成员靠 hearsay 自动建组后再补 create
   > 固化成员集"**，而首次选举时 partition_map 尚无登记（登记正是由当选
   > leader 上报产生的，鸡生蛋），一律拒绝会让全新分片永远选不出 leader。
   >
   > 正确的边界：**被动应答是安全的**——候选人/leader 只会向**它自己成员集里
   > 的节点**发 RV/AE（`peer_in_group` 过滤），所以收到报文即意味着对方认为
   > 本节点是成员；而多数派算术是**对方**按它自己（已知的）成员集做的，
   > 本节点投票或落盘+ack 不会让任何人算错。真正危险的只有"成员集未知的节点
   > **主动竞选/当选**"——它会把多数派算成全体节点并向全集群广播。
   > 所以只需堵住 `group_tick` 那一条路，RPC 路径改为**机会性补齐、补不上照常应答**。
2. **成员集的权威来源 = 控制面下发的 `partdist.partition_map`**。它由 group 0
   的 apply 在**每个节点**各写一份（计划 §13），因此是本地可读、无需新增 RPC
   的真相源——正是"成员集必须来自控制面下发，不能从收到的报文里推断"的落地
   形态。`group_members_from_partition_map()` 取
   `{primary_node} ∪ secondary_nodes`（剔除协调节点），在所有 SPI 可用的路径
   （`pg_raft_rpc` / `pg_raft_append_entries` / `pg_raft_partwal_replicate` /
   `restore_groups_if_needed` / `pg_raft_group_create`）上自动导出并缓存进 shmem。
3. **堵住源头**：`pg_raft_group_create(gid)` 的 `p_members` 默认值是 NULL，
   此前会直接建出未知成员集的组。现在先尝试从 partition_map 导出，导不出来
   就**报错拒绝建组**并给出 hint，不再留下这种组。
4. **prepare 路径拒绝参与**：成员集未知时 `pg_raft_partwal_replicate` **ERROR
   中止事务**，而不是"跳过复制照常提交"——后者等于让写入在没有任何多数派保证
   的情况下返回成功，正是本项目刚修掉的那类丢数据形态。

**残留边界**：全新分片的**首次**选举发生在 partition_map 有登记之前
（登记是由当选 leader 上报产生的，鸡生蛋），因此首次建组仍必须显式给出成员集。
成员**变更**（扩缩副本）也还没有 joint consensus，正解仍是控制面
`OP_CONFIG_CHANGE` 下发 + 配置作为日志条目复制。

### 9.3 【已落地】master 侧 decide 的挂点：内核补丁 0004（2026-08-04）

`dtx_decide` 必须严格发生在"Citus 发完全部 `PREPARE TRANSACTION` 之后、
向客户端返回之前"。

**为什么不能用 XactCallback**：回调是 LIFO 顺序。pg_partdist 在 Citus 之后加载，
它注册的 `XACT_EVENT_PRE_COMMIT` 回调会跑在 **Citus 的之前**——那时 Citus 还没发
PREPARE，我们无从知道 prepared 是否成功。而调整加载顺序这条路是堵死的：
Citus 强制要求自己排 `shared_preload_libraries` 第一位。

**方案：内核补丁 0004**（`patches/0004-pre-record-commit-hook.patch`），在
`CommitTransaction()` 里、`CallXactCallbacks(XACT_EVENT_PRE_COMMIT)` **之后**、
`RecordTransactionCommit()` **之前**加一个 hook：

```c
/* xact.c */
typedef void (*pre_record_commit_hook_type)(void);
extern PGDLLIMPORT pre_record_commit_hook_type pre_record_commit_hook;
```

这个位置的关键性质：**此时本地 commit record 尚未写入，hook 里 ERROR 还能干净地
中止整个事务**（参与者的 prepared 事务随后被推定中止）。若放在 post-commit
（`XACT_EVENT_COMMIT`），本地已经提交，决议失败时无法回头——**绝对不能放那里**。
并行 worker 不触发（它提交的不是自己的分布式事务）。

#### 9.3.1 写集从哪来：参与者自治登记 `partdist.dtx_participant`

补丁只解决"何时决议"，不解决"决议给谁"。master 必须知道**真实写集**
（哪些分区组真的被写了）才能算协调组，而它自己不知道——是各 worker 知道。
Citus 的连接对象在 pg_partdist 里够不着，所以走一张表：

```
worker 侧（PREPARE TRANSACTION 的接线，dtx_participant.c）
  ProcessUtility 截下 PREPARE TRANSACTION '<gid>'   ← 唯一能同时看到 gid 和触达集合的位置
  XACT_EVENT_PRE_PREPARE:
     PartWALFlush()                                  ← [A] DATA 落盘 + 复制到多数派
     每个触达分区追加 DTX_PREPARE 标记（带本地 top-level xid）
     PartWALFlush()                                  ← 标记也推到多数派
     经 libpq **独立事务**写 partdist.dtx_participant(dtxid, gid, gsids[])
  → 之后 PG 才写 prepare 记录并 fsync（[B]），[A] < [B] 不变

master 侧（pre_record_commit_hook，raft_consensus.c 的 dtx_master_pre_record_commit）
  1) 从 pg_dist_transaction 里取**本事务刚插入的行**（xmin = 当前 xid）→ 参与节点清单
  2) 逐节点 SELECT partdist.dtx_local_participant(dtxid) → 合并成写集
  3) 写集 ≤ 1 组 ⇒ 快路径，直接返回（§3.4）
  4) coord_gsid = participants_sorted[dtxid % n]，**先**下发到全部参与节点
     （partdist.dtx_note_coord，同步提交）
  5) 到协调组现任 leader 上 partdist.dtx_decide(...) —— 返回 COMMIT 才放行
```

**dtxid 从 Citus 的 gid 推导**，不新造分配器：gid 形如
`citus_<group>_<pid>_<txnnum>_<conn>`，同一笔分布式事务发往不同 worker 的 gid
只有末段 `<conn>` 不同，前三段完全一致 ⇒ 各节点独立解析即得同一个 dtxid，
零协商。打包成 `group(8) | pid(22) | txnnum(33)`。**pid 那 22 位不能省**：
Citus 只保证 txnnum "自本次重启以来"唯一，协调节点重启后会重号，
光用 (group, txnnum) 会让重启前后的两笔事务在 `dtx_decision` 上撞主键。

**第 4 步为什么必须严格早于第 5 步**：参与者崩溃重启后，只能靠
`dtx_participant.coord_gsid` 找协调组。先决议后下发会造出"全局已 COMMIT、
参与者却查不到协调组"的**不可解**状态；反过来则永远安全——
`coord_gsid IS NULL` 蕴含**决议必然还没做过**，推定中止（回滚）就是正确答案。
这是整个恢复路径的支点。

**只读参与者也要登记**（gsids 为空数组）。它不进写集（§8.3 的剔除照常成立），
但**必须**能拿到 coord_gsid：它可能改过非纳管的表（reference 表 / 普通表），
崩溃后既查不到协调组又擅自回滚就是分叉。

**阶段 3 的挂点**：master 拿到决议后本来就要对每个参与者发
`COMMIT/ROLLBACK PREPARED`，worker 在 ProcessUtility 里截下这条语句、
在执行**前**把 `DTX_COMMIT`/`DTX_ABORT` 标记补进各触达组的 parwal 流——
零额外往返。协调组跳过 COMMIT 标记（§5.3 DECISION 兼任）。
标记的复制是 best-effort（§3.3 原文就是"随下一次 flush 复制"），
复制失败绝不能把 `COMMIT PREPARED` 带崩。

> **★ 实施时踩到的坑（2026-08-04）**
> 1. **`XACT_EVENT_PRE_PREPARE` 里不能直接 SPI**。它跑在 `PrepareTransaction()`
>    的 `PreCommit_Portals(true)` **之后**，portal 已关、活动快照已弹空，
>    直接 `SPI_execute` 报 `cannot execute SQL without an outer snapshot or portal`。
>    必须自己 `PushActiveSnapshot(GetTransactionSnapshot())`（pg_raft 的
>    `raft_persist_spi_begin()` 一直是这么做的）。**这是第一次真的跑通端到端
>    2PC 时立刻撞上的**——机制单测（raft_19/20/21）全都从顶层 SQL 调用，
>    压根走不到这条路径。
> 2. **`cleanup_raft_loose_objects` 漏了 dtx 三函数**。`dtx_decide` /
>    `dtx_status` / `dtx_recover_prepared` 是 pg_raft 扩展成员，一旦有过
>    "扩展被 DROP、函数被单独 CREATE OR REPLACE 重建"的历史就变成游离对象，
>    此后每次 `CREATE EXTENSION pg_raft` 都整体失败于
>    `function ... is not a member of extension "pg_raft"` ——
>    表现是 `raft_log` 与全部 `pg_raft_*` 函数一起消失。
>    与 §5.5 记的是同一类坑：**扩展成员的签名迁移必须显式解除归属再重建**。
> 3. **不能假设 Citus 的 placement 轮转顺序**。`citus.shard_count = 2` 时两个
>    分片完全可能落在同一节点，跨节点事务根本构造不出来；raft_22 改为
>    `shard_count = 8` 再用 `get_shard_id_for_distribution_column()` 反查出
>    两个分属不同节点的分片。
> 4. **两个数据组的成员集必须互不相交**。相交时可能选出同一个 leader，
>    而 leader 上报会改写 `pg_dist_placement` 把两个分片挪到同一节点；
>    更麻烦的是当选者手里只有一张 `CREATE TABLE LIKE` 出来的**空副本表**
>    （惰性回放尚未实装），后续断言全乱。夹具照 raft_16 的手法
>    "先只在 primary 上建组、等它当选、再补建到 follower"。

### 9.4 【已落地】Citus 自带的 2PC 恢复会与我们打架（2026-08-04）

Citus 把 `pg_dist_transaction` 当决议真相源：master 本地提交成功后，它的
maintenance daemon 会对残留的 prepared 事务无条件 `COMMIT PREPARED`。若我们的
协调组决议是 ABORT（例如 `dtx_decide` 失败后事务中止），Citus 的恢复会造成
**分叉提交**——一部分参与者按我们的决议回滚、另一部分被 Citus 提交。

**处置**：`citus.recover_2pc_interval = -1`（关闭 Citus 的 2PC 恢复），
由 §7 的守护统一处置；`pg_dist_transaction` 降级为参与者提示，
**权威真相源 = 协调组日志**。已写进 `reproduce-env.sh` 与 `setup-raft.sh` 的
postgresql.conf 段，并作为 raft_22 A 段的前置检查（逐节点断言为 -1）。

**关掉之后谁来收尾**：`partdist.dtx_recover_prepared()` 按两条规则分流——

| 本节点 `dtx_participant.coord_gsid` | 提交点 | 收尾依据 |
|---|---|---|
| 有值 | 协调组的 DECISION 记录达多数派 | `dtx_status(coord_gsid, dtxid)`；问不到就保持 prepared |
| 为 NULL（快路径 / 无纳管分片 / master 在下发前就挂了） | master 的本地提交 | Citus 原生规则：协调节点的 `pg_dist_transaction` 里有没有该 gid 的**已提交**行 |

第二条是 `dtx_ask_citus_coordinator()`，它把 Citus 恢复守护的判据搬了过来——
关掉 Citus 的守护之后，这类事务原本就没人管了。推定中止的时间窗与 Citus 自己的
恢复同源：master 还在跑这笔事务时行尚未提交，但那时 prepared 事务的年龄也还没到
超时，超时设保守即可。

**顺带修掉的一个真实缺陷**：恢复守护此前把补标记的目标写成
`local_partition_for_shard(coord_gsid)` —— 参与者通常**并不承载协调组的分片**，
于是标记根本写不出去（raft_21 恰好用"参与者即协调组"的夹具，测不出来）。
现在改为写到 `dtx_participant.gsids` 里本节点自己触达过的每一个组。

**2026-08-04 审查补上的三块（都有真对照，raft_21 E/F/G/H 验收）**：

1. **登记缺失的 citus gid 不许永久滞留**。初版恢复循环对"查无登记的 citus gid"
   直接 continue——而 Citus 自己的恢复已经被本节关掉，这类事务从此**没有任何人**
   收尾，prepared 事务连同行锁永久滞留。来源：prepared 事务产生时接线是关的
   （`dtx_2pc_enabled=off`）等。处置：从 citus gid 解析 dtxid，按 Citus 规则闭合。
2. **initiator 存活栅栏**。`pg_dist_transaction` 的行在 master **本地提交之前
   不可见**——master 只是慢（发起 backend 还活着）时，凭"行不可见"推定中止会把
   一笔 master 随后会提交的事务在参与者上回滚，分叉提交。Citus 自己的恢复靠
   共享内存里的活跃分布式事务号拦这个窗口；关掉它就必须自己补：gid 里编着发起
   backend 的 pid，它还在 master 的 `pg_stat_activity` 里就保持 prepared 不动。
   **查询顺序是正确性的一部分：先查 pid、后查行**——pid 已消失蕴含"若曾提交，
   提交先于退出"，随后的行查询必然看得见；反序存在"查行时未提交、查 pid 前刚
   提交并退出"的丢提交窗口。pid 复用只造成多等一轮，保守方向。
3. **参与登记必须同步提交**。初版用 `synchronous_commit=off`，理由是"崩溃会让
   prepared 事务连同登记一起消失"——不成立：登记发生在 [B] **之前**，节点在
   [B] 之后崩溃时 prepared 事务持久而异步登记行可能没落盘 ⇒ master 算出缺块的
   写集、参与者拿不到协调组寻址。同步提交的持久序恰好压住 [B]：
   登记落盘 < [B] < ack ⇒ "prepared 存在 ⇒ 登记必在"。

**残留边界（记录在案，未处置）**：

- **混合写集的分叉窗口**：一笔事务同时写了纳管分片与非纳管表（普通表/未建组
  分片）时，纳管部分归协调组决议管辖、非纳管部分归 master 本地提交管辖。
  决议 COMMIT 之后、master 本地 commit record 落盘之前崩溃 ⇒ 两套规则给出
  相反答案（纳管提交、非纳管回滚）。纯纳管写集无此问题（决议即唯一真相）。
  根治要么禁止混合写集，要么让非纳管部分也挂到决议上——留给第 6 步一并考虑。
- **`pg_dist_transaction` 无人清理**：Citus 的恢复本来兼任 GC（prepared 事务
  闭合后删除对应行），关掉后行只增不减。不能简单按年龄删——它是 citus 规则的
  真相源，删早了会把仍在等待收尾的事务错判成 ABORT。正确的 GC 条件是
  "所有参与节点都已无该 gid 的 prepared 事务"，见 §9.7。

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

> **落地现状（2026-08-04）**：快路径的**判定**已经在跑——master 侧驱动算完写集后
> `nparts <= 1` 直接返回，不做决议、不写 DECISION（raft_22 D 段验收：单分区
> UPDATE 前后决议数不变、parwal 里不出现任何 DTX 标记）。**提交点的定义仍按 (b)**，
> 即沿用现时序、把代价留在故障路径；(a) 与归队规则仍未实装，见第 6 步。
> 恢复侧对这类事务已有明确归属：`coord_gsid IS NULL` ⇒ 按 Citus 原生规则收尾（§9.4）。

### 9.6 【机制已落地】升主序列要加一步"清 in-doubt"（2026-08-04 机制先行）

FRD §11 的六步收尾之后、该分片对外服务之前，必须插入：

```
扫描本组已追平的记录，找出「有 DATA + DTX_PREPARE 标记，但无 DTX_COMMIT/ABORT 标记」
的 dtxid：
  - 逐一向 coord_gsid 的现任 leader 调 dtx_status()，按结果补写标记；
  - 连 PREPARE 标记都没有的（DATA 写了一半就崩）→ 直接补 ABORT 标记。
```

> **落地形态：`partdist.dtx_close_indoubt(partition_id)`（raft_23 验收）。**
> 实现时发现原文"向 coord_gsid 的现任 leader 调 dtx_status"有一个缺口：
> **PREPARE 标记里的 coord_gsid 恒为 0**（prepare 时 master 还没算出协调组），
> 而升主的 follower 本地也没有 `dtx_participant` 登记（它是节点本地表）。
> 所以求决议是一个四级阶梯，每级都只在"能给出终局答案"时返回：
>
> 1. 本地登记里有 coord ⇒ `dtx_status` 权威通道（含推定中止先落库）；
> 2. 本地 `dtx_decision`（本节点恰是协调组成员时 apply 已建好索引）；
> 3. **广播全部 peer 的 `dtx_decision`**——决议在协调组多数派上都有索引行，
>    任何一个成员可达即命中；
> 4. citus 形态的 dtxid 按前缀查 master 的 `pg_dist_transaction`（带发起者
>    存活栅栏），**只认 COMMIT**——行不在推不出 ABORT。
>
> 四级都落空 ⇒ **保持 in-doubt 不动**（决议可能在此刻不可达的协调组里，而我们
> 不知道协调组是谁、也就无法把推定中止**写下来**）——与 §7 守护同一条纪律。
> 剩给第 6 步的只是"插进升主序列 + 与追平回放合流的端到端验收"。

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

> **2026-08-04 审查的精确化**：上面第二条的风险比字面上小一档——`dtx_status` /
> `dtx_lookup_decision` 读的是 **`partdist.dtx_decision` 索引表**（普通 WAL 表，
> 由每个成员的 apply 各自维护），不是 ring。ring 截断丢的是日志条目本体，
> 已 apply 的决议行仍在、随选举转移仍可查。真正残留的窗口是"某成员落后到
> 截断点之外、从未 apply 过该决议、又当选 leader"——没有 InstallSnapshot 时
> 这样的成员本就追不上（现状它会永远卡住而不是当选），所以当前形态下该窗口
> 实际不可达；日志外部化/快照落地时要重新审视这一段。

> 同批审查还发现 `dtx_gc_participant()` 也没有任何自动调用方（与"恢复守护
> 无人调用"同一类缺口——机制在、没接线）。已顺手接进恢复守护的每轮尾部：
> 它只删"已无对应 prepared 事务且超龄"的登记行，安全幂等。

**审查时新增的两个 GC 欠账 → 2026-08-04 当天落地（raft_20 F / raft_21 I 验收）**：

1. **`pg_dist_transaction` 的 GC**（`partdist.dtx_gc_dist_transaction()`，
   接在守护里、仅 Citus 协调节点、约每分钟一批 128 行）。删除条件缺一不可：
   发起 backend 已死（gid 里的 pid 不在 master 的 `pg_stat_activity`）**且**
   所有节点确认无该 gid 的 prepared 事务；任一节点不可达 ⇒ 整轮放弃（返回 -1）。
   它是 citus 规则的真相源，删错一行 = 把等收尾的事务错判成 ABORT。
   落地当天首轮即清掉 100+ 行积压。
2. **`acked[]` 回执 + `DTX_FORGET`（kind 5）**——presumed abort 的标准收尾：
   - 参与者闭合后，守护的**回执清扫**把本节点写过的组经 `partdist.dtx_ack`
     报给协调组 leader，被接受后删除登记行（登记的使命至此结束）；
   - leader 把回执并进本地行的 `acked`；**收齐**（acked ⊇ participants）即追加
     FORGET 记录并复制到多数派；
   - **每个成员 apply FORGET 时删除本地决议行**——删除走与写入相同的复制路径，
     全体成员同步回收，不会出现"leader 删了、follower 留着"的表分叉。
     FORGET 之后按协议不会再有人来问这笔决议（所有写过的参与者都已闭合留标记）。
   - 已知边界：`acked` 是 **leader 本地**的 GC 提示，不随日志复制——回执窗口
     恰逢选举转移时，新 leader 的 acked 从空开始而参与者登记已删、不会再回执，
     那些决议行保守地留在表里（正确性不受影响，泄漏有界且极小）。根治要把
     回执本身也做成日志记录，量级不值得，记录在案。

### 9.7.1 【中】一笔事务现在跑三轮复制，落后 follower 的窗口变宽（2026-08-04 实测）

接线后，一笔跨分区事务在每个参与组上要凑**三次**多数派：

| 轮次 | 内容 | 触发点 |
|---|---|---|
| 1 | DATA 批 | `PartWALFlush()`（PRE_PREPARE） |
| 2 | `DTX_PREPARE` 标记 | 追加标记后再 flush 一次（PRE_PREPARE） |
| 3 | `DTX_COMMIT`/`DTX_ABORT` 标记 | `COMMIT/ROLLBACK PREPARED` 的 ProcessUtility 挂点，**best-effort** |

三轮各自凑各自的多数派，**同一个 follower 完全可能连续两轮都不在多数派里**而短暂
落后一两条；补齐靠 leader 下一次心跳按 `next_index` 推送。终态一致（实测三节点
最终完全相同），但"写完立刻看"的一次性快照判据在接线后变得对时序敏感——
`run-raft-tests.sh` 的 raft_16 终态判据因此改成**有界重试**（30s 窗口内收敛即通过）。

这不是新缺陷，而是把既有的"**缺后台追平通道**"（计划 §12.4）暴露得更明显：
无写入流量时落后 follower 不自行收敛，只能等下一次写入或心跳带。
**阶段 3 的标记复制是 best-effort（§3.3 原文即"随下一次 flush 复制"），
失败不会影响正确性**——全局结果在阶段 2 已持久化，标记只是让升主回放少问一次
协调组。真要收紧，正解是补后台追平通道，而不是把阶段 3 拉进关键路径。

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
| 2 ✅ | **成员集显式化**（§9.2）：未知成员集 fail-stop + 从控制面 `partition_map` 自动导出 + 建组入口堵源头 | **raft_18 四条判据全过**：A 未知成员集建组被拒且不留残组；B 有登记时自动导出 `cluster_size=3`；C **quorum 按真实成员数**（3 全在可写 / 停 1 个 2/3 仍可写 / 停 2 个 1/3 必败）；D 非副本节点不被拖入。真对照（nm 验证构建身份）：修复前 A 处 `group_create` 返回 `t` 建出 `cluster_size=9` 的组 |
| 3 ✅ | **记录格式**（§5）：`PARTWAL_FLAG_DTX` + `DtxRecordPayload` + `partwal_read_record`/`partwal_follower_append` 携带 flags | **raft_19 四段全过**：A 全新库 `CREATE EXTENSION` + 四个函数签名；B leader 侧 DATA `flags=1`、DTX `flags=8`/`orig_lsn=0`/info 载子类型、DECISION 载荷往返；C 对 DATA 调 `read_dtx` 返回 NULL；D **两个 follower 的 flags/info 序列与 leader 完全一致** |
| 4a ✅ | **决议层本体**（§6）：`dtx_decision` 表 + `dtx_decide`/`dtx_status` + apply 索引维护 | **raft_20 五段全过**：A 非协调组 leader 调 decide 返回 NULL 且不留痕；B **COMMIT 决议返回后 DECISION 记录与索引在协调组全部成员上均在**（= 已在多数派持久化，全局提交点）；C 决议槽一次性；D 推定中止先写 ABORT 再答复、此后 COMMIT 无法翻盘；E **协调组切主后两笔决议仍可查**（协调权随 Raft 选举自动转移，无需状态搬迁） |
| 4b ✅ | **补丁 0004 + master 挂点 + 关 Citus 2PC 恢复**（§9.3 / §9.4）：`pre_record_commit_hook` + 参与者自治登记 `dtx_participant` + master 侧写集收集/协调组选取/决议 + 阶段 3 标记 + `citus.recover_2pc_interval=-1` | **raft_22 A/B/C/F**：A 二进制导出 `pre_record_commit_hook` 且逐节点 `recover_2pc_interval=-1`；B 跨分区事务提交后协调组有且仅有一条 COMMIT 决议、participants 恰为真实写集、协调组=`participants_sorted[dtxid % n]`、且**决议在协调组每个成员上都在**；C 三阶段在 parwal 流里逐条可见（参与组 `DATA…,PREPARE,COMMIT`／协调组 `DATA…,PREPARE,DECISION` 且**无**单独 COMMIT 标记），PREPARE 携带本地 top-level xid 且排在 DATA 之后；F **协调组失去多数派** ⇒ 事务**提交失败**且无行可见（不允许部分提交），并断言失败原因文本含"多数派"（钉死失败点在 2PC 路径）。判据构造踩过**两个**坑（2026-08-04）：① 只停协调组 leader 不够——组内还剩 2/3 会自治选出新 leader 并上报改写路由，决议照样做得出来、事务**应该**成功（这正是 §2.1 好处 2）；② 停哪两个也有讲究——member 列表头一个是协调组自己的 primary，同时是数据分片的主节点，停它会让 INSERT 在**路由/连接**阶段就失败，测的是"主挂了写不进"这个与 2PC 无关的性质。必须停两个**非主**成员：数据主全活着、远程写全成功，失败只可能发生在协调组自身分片的 prepare 复制凑不齐多数派（1/3）。"prepare 全成、只饿死 decide"无法从外部确定性构造（两者共用同一 quorum、都在同一条 COMMIT 语句内完成），decide 自身的多数派语义由 raft_20 B 在机制层验收。**真对照**：`pg_raft.dtx_2pc_enabled=off` 重跑，B 段确定性失败（"找不到 DECISION 记录"）。**不回退基线在这一步抓到两个真缺陷**：raft_19 A 段（全新库冒烟）抓到 hook 在没装 Citus 的库里直接引用 `pg_dist_transaction`；raft_14/16 抓到 §9.2.6 的段号错位 |
| 5a ✅ | **恢复守护**（§7）：`partdist.dtx_recover_prepared(timeout_ms)` + **自动接线**（TopologyMonitor 周期自触发，2026-08-04 审查补上） | **raft_21 八段**：A 有 COMMIT 决议 ⇒ 提交、数据可见、补 `DTX_COMMIT` 标记；B 从未决议 ⇒ 经 `dtx_status` 推定中止、回滚、补 `DTX_ABORT` 标记且**决议已落库**；C 未超时的不被触碰（不与正常路径抢答）；D **协调组不可达 ⇒ 保持 prepared 不动**，绝不擅自决定；E 登记缺失的 citus gid 按 Citus 规则闭合、**不许永久滞留**；F Citus 规则提交侧（master 有提交记录 ⇒ 参与者提交）；G **initiator 存活栅栏**（发起 backend 活着 ⇒ 绝不推定中止，防慢 master 分叉）；H **守护自动闭合**（无任何手工 SQL）。E/G 均有真对照：修复前构建 E 处 prepared 永久滞留、G 处活发起者的事务被误回滚 |
| 5b ✅ | **快路径 + 只读参与者剔除**（§3.4 / §8.3） | **raft_22 D/E**：D 单分区事务前后决议数不变、parwal 里不出现任何 DTX 标记（开销与接线前持平）；E 广播 UPDATE 打到全部 8 个分片、只有 1 个分片真改到行 ⇒ 写集只剩 1 组、**不产生决议**，同时只读参与者仍留下 `gsids='{}'` 的登记（7 条空写集 / 1 条真写集）——剔除的是"进不进写集"，不是"登不登记"，后者是它崩溃后自解的唯一线索 |

| 5c ✅ | **不依赖合流的第 6 步前置**（2026-08-04）：决议 GC（acked 回执 + `DTX_FORGET`，§9.7）＋ `pg_dist_transaction` GC（§9.7）＋ **in-doubt 闭合函数机制先行**（`dtx_close_indoubt`，§9.6）＋ 混合写集告警（§9.4） | **raft_20 F**：回执部分收齐不删、收齐即 FORGET、全体成员随 apply 同步回收、迟到回执幂等；**raft_21 I**：GC 只删"发起者已死 + 全网确认无 prepared"的行，发起者活着/有 prepared/节点不可达都保守不删；**raft_23 五段**：A 登记通道推定中止先落库（流 `1,2,4`——协调组即自身时决议兼任闭合）、B 广播 peers 命中 COMMIT、C citus 前缀规则、D **查不到就不动且幂等**、E 混合写集 WARNING |
| 6 | **升主 in-doubt 清理接线**（§9.6，函数本体已在 5c）+ 快路径分叉归队规则（§9.5） | 与惰性回放的 promotion 路径合流验收 |

> **★ 5b 为什么等到 4b 之后才做（2026-08-03 的判断，2026-08-04 兑现）**：这两项都是
> **master 侧的驱动决策**——"参与组数 ≤ 1 就不走 2PC"（§3.4）与"命中 0 行的
> 分片不进参与者清单"（§8.3），都发生在 master 决定要不要发起 2PC 的那一刻。
> 4b 未落地时实现它们等于实现一段**没有调用者、也无法观测**的代码。
> worker 侧的检测机制（触达集合为空 ⇒ 只读）在 §9.1 的修复里就已就位，
> 4b 一落地就直接用上了，两项各自只是驱动里的一个判断。

**不回退基线**：`run-raft-tests.sh` 现 raft_01–22 + P0 10/10 + 全新库
`CREATE EXTENSION` 冒烟，每步都必须保持全绿。

### 10.1 第 6 步的依赖边界，与可以先做的事（2026-08-04 审查时厘清）

第 6 步的两件事里，**真正被惰性回放挡住的只有"接进升主序列 + 端到端验收"**：

- §9.6 的 **in-doubt 扫描与闭合本身不依赖回放**（该节自己写明"不依赖 R2/R3"）——
  它是对一段 parwal 流按事务粒度找"有 DATA+PREPARE、无 COMMIT/ABORT"的 dtxid、
  问 `dtx_status`、补标记。这个函数（如 `partdist.dtx_close_indoubt(partition_id)`）
  可以**现在**实现并单测：夹具现成（协调组失多数派让事务中止，参与组的流里就
  留下孤儿 PREPARE）。等升主路径落地时它只是被多调一次。
- §9.5(b) 的分叉归队 re-baseline 规则是升主/归队路径的事，确实要等合流。

**可先于第 6 步做的清单**（2026-08-04 当天除日志外部化外全部完成）：

1. ~~恢复守护自动接线~~ —— ✅ 审查轮已做（§7 落地形态，raft_21 H）。
2. ~~§9.7 两个 GC 欠账~~ —— ✅ `pg_dist_transaction` GC + acked/`DTX_FORGET`
   （§9.7 落地记录，raft_21 I / raft_20 F）。
3. **计划 §12.4 #3 日志外部化** —— **仍未做，是剩余项里唯一的大头**：
   不依赖回放，但要重写数据组日志的存储层（ring/SQL 行 → parwal 段文件 +
   shmem 游标），是独立的多天量级里程碑，应单独立项排期；2PC 已把它从容量
   问题升级为正确性问题（§9.7、§9.9 的根治都指向它）。InstallSnapshot 与它
   配套考虑（用户此前定的"留待后续"指快照）。
4. ~~§9.6 的闭合函数机制先行~~ —— ✅ `dtx_close_indoubt`（raft_23），
   升主合流时只剩接线与端到端验收。
5. ~~§9.4 混合写集的约束层表态~~ —— ✅ worker 侧 PREPARE 时 WARNING
   （raft_23 E；检测边界：只有分片形名的非纳管表可见，普通表写测不到）。

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
