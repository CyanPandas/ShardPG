# ShardPG+ Raft 模块修订计划

> **本文档是层层追加形成的**：§1–§10 是早期路线，§11 起是分区级 Raft 的落地方案，
> §12–§14 是各专题的交接说明。**要看"现在到哪了"，只读 §0；要看某个专题的来龙去脉，
> 按 §0 末尾的导航跳转。**各章节内的历史结论一律保留（含被推翻的判断与踩坑记录），
> 它们是后续决策的依据，不要因为"过时"而删除。

---

## 0. 当前状态速览（2026-08-08）

代码基线：**`shardpg-TX`** —— §15 描述的那次双向合并**已经完成**
（合并提交 `f58437f`，`shardpg-replay@fd8e99d` × `shardpg-4.0@fadf2eb`）。
回归基线：`run-raft-tests.sh` **56/56**（拓扑 1c+8w）+ `test_shard_identity_p0.sh`
**10/10** + 回放六套件（canary 7 / R1 56 / L1 48 / R2 48 / D1 81 / D2 15）
+ 跨线联测 TX1 51 / TX2 35 / TX3 19，**全零 FAIL**。

> **合并已完成 —— §15 从"给执行合并的人看"变成"合并实际是怎么做的"。**
> 逐文件决策与踩到的坑见 **§15.6**（新增）。合并之后又交付了三批：
> 4 个运行期缺陷修复（`46df57d`/`4ddcd96`）、2PC 阶段 3 的 COMMIT 标记
> （`4ddcd96`）、升主合流 + 显式 ABORT 决议 + plsn 分配顺序（`7711bc1`）。
> 2PC 侧的完整叙述见 `pg-partdist-src/docs/DTX_2PC_DESIGN.md` **§12**。

> ⏪ **2026-08-07：数据组日志外部化（§11.10 的 E1–E4）已整条回滚。**
> 代码回到 `ace619a` 的形态 —— **项目中不存在任何日志外部化**。
> 回滚范围与验证见 **§0.4**。交接以此状态为准。

### 0.1 已经做完的（每项都有回归覆盖）

| 能力 | 落地形态 | 回归 |
|---|---|---|
| 控制面 Raft 安全语义 | 选举限制、任期栅栏、HardState 持久化（现 **v3**） | raft_01–11 |
| **切主重构** | 分区组内自治选举 → 上报 group 0 登记（任期栅栏）→ 落各节点 `pg_dist_placement` | raft_14/15（§13） |
| **事务 prepare 接线** | `PartWALFlush` 挂钩自动逐条 `data_propose_one`，无手工 propose | raft_16（§14） |
| 数据面多组（P1/P2） | 每分区一个 Raft 组，组间选举/复制隔离；平凡 apply 推进 `applied_part_lsn` | raft_12/13 |
| 成员集显式化 | 空成员集语义由"全体节点"改为"**未知**"；权威成员集从 `partition_map` 导出 | raft_18 |
| **DTX-2PC 第 0–5 步** | 决议进数据组；内核补丁 0004 把决议接进客户端提交路径 | raft_17–23（详见 2PC 文档） |
| **后台追平通道** | `pg_raft_catchup()` + TopologyMonitor 经 libpq 自触发；含环外条目回读 | raft_24 |
| **控制面日志压缩 + InstallSnapshot** | `compact_threshold` 触发压缩；基点随 HardState v3 持久化 | raft_25 |

> ⏪ 曾经出现在本表的「数据组日志外部化 E1–E3」**已于 2026-08-07 整条回滚**，
> 连同 raft_26/27/28 三个用例一并移除（回归基线 59 → 56）。详见 §0.4。

### 0.2 尚未做的（按阻断强度排序）

> **★ 2026-09-09 回填：本表写于 2026-08-08，其后 P3–P6 六期把其中四行做掉或
> 取代掉了。** 逐行复核结论写在「09-09 复核」列；**仍然成立的是 #3 / #5 / #6 /
> #8 / #9 五行**，它们连同处置计划见
> `pg-partdist-src/docs/P7_REMEDIATION_PLAN.md` §1.6（批次 6，须解冻本模块才能开工）。

| # | 缺口 | 阻断了什么 | 依据 | **09-09 复核** |
|---|---|---|---|---|
| **1** | **集群级 xid 区间租约** | **P3 的硬门槛**。各分区 leader 独立分配 xid，而 clog 是节点全局共享的 —— 一个节点托管来自不同 leader 的多个分区副本时，相同 xid 在本地 clog 相撞。FRD 明确写着方案 A 落地前"多 shard 共存于一个 follower 的配置不可上线"，**而这正是本架构的默认形态** | §11.5.2 #1、§12.4 #2 | ✅ **已被取代**：TX-TSO-MVCC 改为**每分片一个独立 xid 宇宙**（DESIGN §5），xmin/xmax 直接存分片 xid、每分片一本 clog，节点全局 clog 相撞的前提不存在了。本行作废 |
| **2** | **P3 物理回放本体** | follower 堆表始终为空 ⇒ 切主后新主没有历史数据 ⇒ 切主只能"机制先行"，生产语义未放行 | §12.4 #6、§13.6 #1 | ✅ **已做**：R1 物理回放闭环 + L1 惰性回放 + D1/D2 控制通道全部交付并有门禁套件 |
| **3** | **数据组日志外部化 E1–E4（整条）** ⏪ **已回滚，退回未开工** | 每节点最多 31 个数据组，与"每节点几十上百分区"直接冲突（见 §0.3）。E1–E3 曾落地又于 2026-08-07 整条回滚，**现状等同从未做过**；`raft_log` 双写与 shmem ring 都原样保留 | §0.4、§11.10（设计留档） | ⏸ **仍成立**：现状等同从未做过，`RAFT_MAX_GROUPS=32` 因此仍卡着（§0.3）。见 P7 计划 P7-R2 |
| **4** | **2PC 第 6 步** | 升主 in-doubt 清理的**接线**（函数本体 `dtx_close_indoubt` 已有）+ 快路径分叉归队规则；需与惰性回放 promotion 路径合流 | 2PC 文档 §9.5/§9.6 | ✅ **已做**：升主 in-doubt 清理已接线（`dtx_close_indoubt` 四级），DTX-2PC 全链路有 tx1–tx4 四套验收。**但快路径分叉的归队仍是人工重做基线**（P7 计划 P7-E3） |
| **5** | **成员变更 joint consensus** | 只能建组时定死成员集，副本集变更（扩缩容、换节点）无安全路径 | §2.3 | ⏸ **仍成立，且后果比当时写的更重**：`raft_consensus.c` 无 `joint`/`ConfChange`/`add_member`（grep 零命中）；连带 §9.2 把 Citus 全部搬运器禁掉，理由是"搬分片走 raft 成员变更专项"，**而专项不存在 ⇒ 集群拓扑事实上不可变更**。见 P7 计划 P7-R1 |
| 6 | 数据组的压缩 / InstallSnapshot | 现在数据组"快照内容"与"日志内容"是同一份东西，装快照等于重放日志 —— 要等 P3 才谈得上 | §4 阶段 1 | ⏸ **仍成立**：P3 已到，本项可以谈了；`raft_log` SQL 行与用户事务同命（DTX §9.9）也归这条 |
| 7 | `partwal_notify_primary_switch()` 真实化 | 仍是日志占位，未做真实角色/进度切换 | §12.4 #7 | ✅ **已做**：批次 #7（2026-09-04）落地真实角色交接（升主解闸门 / 降级收身份），批次 #10 补上路由层本身。函数体已不是日志占位 |
| 8 | 单 BGW tick 多路复用全部组 | 组数上去后数据组会挤占控制面心跳，需报文级心跳合并 + 最小堆调度 | §11.5.2 #4 | ⏸ **仍成立**：组数上去后仍会挤占控制面心跳 |
| 9 | 无 PreVote | 重启成员会以更高 term 打断在任 leader，选举无谓抖动（测试需容忍领导权漂移） | §11.5.2 | ⏸ **仍成立**：grep `prevote` 零命中 |
| 10 | 两份 FRD 已分叉 | 临时环境 16KB 旧版 vs 主仓库 28KB v2，P3 开工前须合并 | §11.5.2 #5、§12.4 #1 | ✅ **已解决**：主仓库 FRD 是唯一版本（本次已回填至与代码对齐） |
| 11 | TSO 缺位 | `commit_ts` 先填本地时钟；全局快照一致性等 R3/TSO | 2PC 文档 §9.8 | ✅ **已做**：P3 交付 TSO（`tso.c` / `tso_client.c`）+ GlobalSafeTs 双通道/租约/栅栏。**但 TSO 单点无 HA 是 v1 显式裁定**（DESIGN §2.4），且 RPC 缺 schema 前缀是活缺陷（R-P6-20） |

### 0.3 为什么 `RAFT_MAX_GROUPS` 还卡在 32

**根因是 shmem 定长预分配，不是算法限制**（代码在 `raft_consensus.c`）：

```c
#define RAFT_MAX_GROUPS   32
#define RAFT_LOG_CAPACITY 128
#define RAFT_PAYLOAD_MAX  768

typedef struct RaftLogEntry {
    int64 index; int64 term;              /* 16 B  */
    char  op_type[32];                    /* 32 B  */
    char  payload[768];                   /* 768 B */
} RaftLogEntry;                           /* = 816 B */

typedef struct RaftLogShmem { ... RaftLogEntry ring[RAFT_LOG_CAPACITY]; ... };
typedef struct RaftGroupTable { ... RaftGroupState groups[RAFT_MAX_GROUPS]; };
```

一组的 ring = `128 × 816 B ≈ 102 KB`，整张表 `32 × ≈105 KB ≈ 3.3 MB`，
由 `ShmemInitStruct("pg_raft_groups", MAXALIGN(sizeof(RaftGroupTable)))` **一次性、
编译期定长**分配 —— 不按需增长，空槽位照样占内存。

于是「调大常量」不是解法：500 个分区组 ⇒ 约 51 MB shmem，且绝大多数槽位是空的。
**正解是让数据组不再持有 ring**（§11.10 E4），但那要先把 ring 承载的东西搬走
——**外部化已于 2026-08-07 整条回滚（§0.4），所以现在 ring 仍然承载全部内容**：
日志内容在环里、`raft_log` 里各有一份，(term, plsn) 无外部映射，日志末端靠
`raft_log` 重建。抬常量的前置条件一件都不具备。

> 顺带澄清一个易混点：ring **不是追平的上限** —— 落后超过 128 条的成员可以靠
> 回读 `partdist.raft_log` 重建环外条目来追平（raft_24 覆盖）。它是**内存占用**的上限。

### 0.4 ⏪ 数据组日志外部化：整条线已回滚（2026-08-07）

**用户指令**：E1–E3 也全部回滚，按"无任何日志外部化"的状态交接。

**回滚方式**：`git revert` 三个代码提交（`6181655` E3 / `bd4cfd8` E2 / `d4ff6d9` E1），
保留历史不改写。验证口径是 `git diff ace619a -- . ':!*docs/*'` **为空**
——代码逐字节回到外部化开工之前。

**回滚掉了什么**：

| 曾经引入 | 现状 |
|---|---|
| `partdist.raft_log_runs` 段式边界表 | 表定义已从 `sql/pg_raft--1.0.sql` 与 `setup-raft.sh` 移除 |
| `partdist.pg_raft_entry_from_parwal(group_id, index)` | 函数已移除 |
| `log_get_entry_parwal()` / `log_get_entry_durable()` 的 parwal 分派 | 移除；数据组环外条目**回到从 `partdist.raft_log` 回读** |
| `persist_log_entry_sql` 对数据组 `OP_PARWAL` 的写入分流 | 移除；**所有条目一律照写 `raft_log`**（双写恢复） |
| `restore_data_log_from_parwal()` / `data_entry_store_guarded()` | 移除 |
| HardState **v4**（`last_log_index` 字段） | 退回 **v3**；日志末端重新由 `raft_log` 恢复 |
| 用例 raft_26 / raft_27 / raft_28 | 三个文件删除，`run-raft-tests.sh` 摘除；回归基线 59 → **56** |

**回滚的运行期影响（重要，接手必读）**：

1. **磁盘上遗留的 v4 hardstate 会被整个丢弃。** v3 代码的校验是
   `hs.version > RAFT_HARDSTATE_VERSION` ⇒ 打 WARNING "忽略损坏的 hardstate 文件"
   并 `return` —— term / voted_for / commit_index / last_applied / base_index
   **全部丢失**，该组从零状态起。这不是崩溃，是静默丢状态（丢 `voted_for`
   直接破坏选举安全性）。
2. **E3 期间写入的数据组条目在 `raft_log` 里没有行。** 回滚后代码回去读
   `raft_log`，那些条目读不出来，表现为数据组日志凭空变短。
3. 结论：**回滚后不能在旧数据上直接跑，必须重建 `pg-cluster-data`。**
   本次已重建并复跑回归（见下）。

**回滚后的验证**：编译无 error/warning；`nm` 确认产物中无外部化符号；
就地重建 9 节点 `pg-cluster-data`（group0 leader=node1，1s 收敛）；
`run-raft-tests.sh` **56/56**、`test_shard_identity_p0.sh` **10/10**。

**回滚对其他工作的影响**：

- **不阻断物理回放（P3）**。P3 依赖的是"段文件里有完整、连续、已达多数派的
  字节流"，而这由 demux 落盘 + Raft 多数派提供，与日志存在哪儿无关。
- **不阻断 2PC 第 6 步**。
- **让合并变简单了**：外部化曾经带来的头号合并冲突（`pg_raft_entry_from_parwal`
  硬编码 `w.xid`，与 replay 侧改名后的 `gxid` 相撞）**随回滚一并消失**，见 §15.2。
- **2PC 文档 §9.9 退回未解决**：`raft_log` 双写恢复，"raft 日志 SQL 行与用户事务
  同命"这条隐患回到 E3 之前的状态。
- **规模化仍然阻断**：`RAFT_MAX_GROUPS = 32` 不变，且现在连前置条件都没了。

### 0.5 文档导航

| 想看什么 | 去哪 |
|---|---|
| 两层 Raft 的定位与目标架构 | §3、§11.1–§11.4 |
| 分区级 Raft 的工程分期（P0–P3） | §11.5 |
| **数据组日志外部化（E1–E4）** ⏪ 已回滚 | §0.4（回滚范围与影响）、§11.10（设计留档，未落地） |
| **与物理回放分支合并的交接要点** | §15 |
| 切主重构：自治选举 → 上报 → 路由层 | §13 |
| 事务 prepare 接线 | §14 |
| 物理回放（P3）交接说明 | §12 |
| 全部回归用例的判据与真对照 | §6 |
| 踩过的坑与对策 | §8 风险表 |
| **跨分区 2PC** | `pg-partdist-src/docs/DTX_2PC_DESIGN.md` |
| **follower 物理回放** | `pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md` |

---

## 1. 计划定位

本计划用于替代旧的“pgraft spike + 控制面 MVP”路线，作为 `CyanPandas/ShardPG` 分支
`shardpg-3.0`（raft4 四节点临时环境）的对照文档。文中凡出现 `/home/pxr/pg-citus-cluster`
路径或“3 节点”拓扑之处，均为 2026-07-12 之前的历史环境描述，**一律以 shardpg-3.0 的
四节点环境为准**（master:5432 + worker1-3:5433-5435，多数派 3/4）。

当前主线不再恢复 pgElephant/pgraft Go bridge，而是继续推进项目内已有的纯 C `pg_raft`。

**定位已于 2026-07-17 修订（§11）**：早期版本写的是“Raft 只做控制面共识、切主决议，不另起
数据面复制框架”。自 P1/P2 落地后不再成立 —— Raft 现在是**两层**：控制面（组 0）管拓扑与
决议，数据面**每分区一个 Raft 组**负责该分区 parwal 字节的复制与定序。这不是“重复造一套
数据面”，而是把 `pg_parwal/<partition>/` 里**已经存在**的每分区单调日志直接当作该组的
Raft log 来运输；物理回放（redo）是骑在其上的应用层，见 §11.1。

核心要求：主从切换不能只更新 `partition_map`。**该要求已于 2026-07-24 由 §13 的切主重构
兑现**：新 primary 由分区组内**多数派自治选举**产生（选举限制天然只让日志最全的副本当选），
经上报登记进 group 0（任期栅栏防迟到/重复），并落到路由层（每节点本地
`pg_dist_placement`）。旧的"控制面指定式切主"仅对 `primary_term = 0` 的历史分区保留。

## 2. 项目状态（历史快照：2026-07-24 @ `6cee2aa`）

> **最新状态一律以 §0 为准。**本节保留 2026-07-24 那一刻的快照与其后逐条追加的
> 修订痕迹（划线=已解决），因为后面很多设计判断是在这份快照上做的，抹掉就看不懂
> 当时为什么那样选。

### 2.1 架构前提（后续所有设计必须遵守）

集群由多个工作节点构成，数据被划分为若干逻辑分区（Partition）并在节点间分布式部署。
**每个工作节点同时承担多重职责：既是部分分区的主副本（Primary Replica），也是另一些分区的
从副本（Secondary Replica）。** 由此推出三条硬约束，贯穿全文：

1. **不存在全局“主节点”。** 数据面领导权是**按分区**的；只有控制面（组 0）有唯一 leader。
   任何“本节点是不是 leader”的判断都必须带上分区/组限定。
2. **一个节点会同时托管来自不同 leader 的多个分区副本。** 所有节点级共享资源（clog、xid
   空间、shmem 组表、BGW tick 预算）都会成为跨分区的争用点 —— §11.5.2 的阻断项全部源于此。
3. **对某个给定分区，一个节点要么是 primary 要么是 secondary，不会两者皆是。** secondary
   不会本地产出该分区的 WAL，因此该分区的 `partition_lsn` 编号空间在副本上只由 leader 指定
   （这是运输层加固第 1 条的依据，也是 raft_13 夹具曾经出错的原因，见 §11.5.1）。

### 2.2 已具备

- **控制面（组 0，职责=登记处+配置权威，见 §13）**：RequestVote（带
  `last_log_index/last_log_term` 日志新旧检查）、随机 election timeout（协调节点偏置窗口，
  leader 常态落 master）、心跳、AppendEntries（`nextIndex/matchIndex` + `prev_log` 一致性）、
  多数派 `commit_index`、HardState 落盘、更高 term 降级、TopologyMonitor（节点状态标注）。
- **切主（2026-07-24 重构，§13）**：数据组自治选举 → 新 leader 经
  `pg_raft_report_data_leader` 上报 group 0 leader → 任期栅栏（`partition_map.primary_term`）
  登记 → 每节点 apply 同步本地 `partition_map` 与真实 Citus 分片的 `pg_dist_placement`
  （落路由层）。安全性由选举限制（新 leader 必持全部已提交日志）+ 任期栅栏保证。
  旧"控制面指定式"通道（`switch_partition_lsn` 候选过滤那套）只对 `primary_term = 0`
  的历史/合成分区保留，随夹具迁移退役。
- **全局分片身份（P0）**：`partdist.shard_identity` 以 Citus `shardid` 为
  `global_shard_id`，可把一个组 id 解析为各节点各自的本地 OID，跨节点 `applied_part_lsn`
  比较自此才有意义。
- **分区级 Raft 组（P1）**：`RaftGroupTable` 每组一份共识/日志状态；组 0 = 控制面且行为与
  组化前逐字节一致；`raft_log` 按 `(group_id, log_index)` 分命名空间。
- **数据组复制（P2 + 运输层加固）**：真实 parwal 字节按 leader 指定的 `partition_lsn`
  复制到多数派、fsync 后才 ack、重传幂等、截断触达字节、`last_applied` 持久化。
- **`pg_partdist` 边界函数**（`src/raft_boundary.c`，7 个）：见 §5。
- **环满安全**：follower 环满时诚实拒绝（不再假 ack 造假多数派）；控制面 apply 游标
  滑出环窗口时按"元数据持久、跳过安全"语义窗口内快进（§13.4）。
- **事务 prepare 接线（§14）**：写入提交前自动逐条复制到分区组（[A] 后、[B] 前），
  多数派持久化才算 prepared，失多数派事务中止；顺带获得旧 primary 写栅栏。
- **并发下的 prepare 多数派保证（2026-08-03，DTX-2PC 第 1 步）**：修掉 group commit
  让路窗口——触达分区集合改为 `PartWALInsert` 时 per-backend 登记，`PartWALFlush`
  的**提前返回路径也调复制挂钩**；本组的复制在 pg_raft 侧串行化（认领位 +
  持有者消失时回收），消除并发 backend 抢同一段 plsn 的重复提案。
  回归 `raft_17`，含修复前必失败的对照实验。
- **跨分区事务 2PC（2026-08-04，DTX-2PC 第 2–5 步）**：记录格式（`PARTWAL_FLAG_DTX`）、
  决议层（`dtx_decide`/`dtx_status`，决议在协调组达多数派即为全局提交点）、
  **内核补丁 0004 的 `pre_record_commit_hook`**（把决议接进客户端提交路径）、
  参与者自治登记与恢复守护、快路径与只读参与者剔除。回归 `raft_19`–`raft_22`，
  含 `pg_raft.dtx_2pc_enabled=off` 的对照实验。
- **后台追平通道（2026-08-05，§12.4 #5 的后半段）**：`partdist.pg_raft_catchup()`
  由 TopologyMonitor 按 `pg_raft.catchup_interval_ms`（缺省 5s，0=关）经 libpq 自连
  触发，对本节点为 leader 的每个组把落后成员逐条补齐。**必须跑在 client backend**
  ——取 parwal 字节与回读环外条目都要 SPI，BGW tick 两样都没有。同时补上
  **环外条目从 `partdist.raft_log` 回读**（leader 取条目 + follower 的 prev 检查
  与冲突判定），落后超过 `RAFT_LOG_CAPACITY=128` 的成员从此能追平。
  回归 `raft_24`，含两级对照（关通道必不收敛 / 只摘掉环外回读则 >128 段确定性失败）。
- **控制面日志压缩 + InstallSnapshot（2026-08-05）**：`pg_raft.compact_threshold`
  触发压缩（快照先落库、再删 `raft_log` 行、基点随 hardstate v3 持久化），
  落后到压缩点之前的成员由 `partdist.pg_raft_install_snapshot()` 整体装载
  （两张元数据表整表替换 + 路由层同步）。回归 `raft_25`，含真对照
  （只摘掉发送侧则落后成员确定性冻结在原进度）。
- 回归基线：`pg-raft-src/run-raft-tests.sh` **raft_01–22 全绿**；
  `pg-partdist-src/tests/test_shard_identity_p0.sh` **10/10**；
  全新库 `CREATE EXTENSION` 冒烟通过（raft_19 A 段，2026-08-04 正是它抓到
  master 侧 hook 在没装 Citus 的库里直接引用 `pg_dist_transaction` 的缺陷）。

### 2.3 当时的缺口（含其后的修订痕迹）

> 逐条对照最新状态请看 §0.2。下面保留原文与划线修订，是为了留住【某项是何时、
> 因为什么被关闭的】这条线索。


- **物理回放（P3）未开始**：apply 仍是“平凡 apply”，只推进
  `follower_partition_map.applied_part_lsn`，不做 `rm_redo`，follower 堆表不含数据。
  **由此，切主后的路由切换是"机制先行"**——新主壳表没有历史数据，生产语义要等回放
  追平后才能放行切换（§13.6 #1）。
- **xid/clog 跨 leader 冲突未解**（§11.5.2 #1，最硬的阻断项）。
- `RAFT_MAX_GROUPS = 32`，与“每节点托管几十上百分区”冲突；日志仍在定长 ring
  （`RAFT_LOG_CAPACITY = 128`，落后超容量时靠拒写背压），未外部化到 parwal，无日志压缩。
  **→ 2026-08-05/06 部分关闭**：控制面压缩 + InstallSnapshot 已落地（raft_25，**未回滚**）。
  数据组日志外部化 E1–E3 也曾落地，但**已于 2026-08-07 整条回滚**（§0.4）——
  日志重新是"shmem ring + `partdist.raft_log` 双写"，未外部化。
  **`RAFT_MAX_GROUPS` 仍是 32**，且现在连抬它的前置条件都不具备，见 §0.3。
- ~~**组成员集不随 RPC 传播**：自动建组的 follower 成员集为空，按全体 peers 算多数派；~~
  **→ 2026-08-03 已修**（`DTX_2PC_DESIGN.md` §9.2，回归 raft_18）：数据组的空成员集
  语义由"全体节点"改为"**未知**"，未知的节点不竞选/不当选/不提案（**被动应答
  仍照旧**——否则 §11.5.2 约定的 hearsay 引导路径会被砍掉，全新分片永远选不出
  leader，初版一刀切时 raft_14/15/16/17 全挂）；权威成员集改为从控制面下发的
  `partdist.partition_map` 本地导出。
  残留：全新分片首次选举（登记尚不存在）仍须显式给成员集；成员**变更**仍缺
  joint consensus。原文后半句仍成立：
  上报路径在成员集未知时退化为"全体 peers 去掉自己与协调节点"（§13.6 #2）。
- ~~`raft_snapshot` 仍只是控制面元数据快照表，**没有 Raft InstallSnapshot RPC**。~~
  **→ 2026-08-05 已补（仅控制面）**：`pg_raft_install_snapshot()` RPC + 发送侧，
  与日志压缩同期落地（raft_25）。数据组的快照要等 P3 —— 在此之前它的"快照内容"
  与"日志内容"是同一份东西，装快照等于重放日志。
- `partwal_notify_primary_switch()` 仍是日志占位，未做真实角色切换。
- ~~`pg_raft_data_propose()` 未接入写入路径~~ **→ 2026-07-24 已接入事务 prepare 路径**
  （§14，PartWALFlush 挂钩自动逐条 propose）。~~仍缺**后台追平通道**~~
  **→ 2026-08-05 已补**（`partdist.pg_raft_catchup()` + TopologyMonitor 自触发，
  回归 raft_24）：无写入流量时落后 follower 现在会自行收敛，落后超过环容量也能追平。
- ~~2PC 的 commit 决议尚未实现。~~ **→ 2026-08-04 端到端落地**：
  设计定稿于 `pg-partdist-src/docs/DTX_2PC_DESIGN.md`（DTX-2PC v1），
  **决议放数据组，不走控制面**（理由见该文 §3.2）。该文 §10 的第 0–5 步全部完成、
  回归 raft_17–22：让路窗口修复 → 成员集显式化 → 记录格式 → 决议层 →
  **内核补丁 0004（`pre_record_commit_hook`）把决议接进客户端提交路径** →
  参与者恢复守护 + 快路径 + 只读参与者剔除。
  **跨分区事务现在真的走 2PC，提交点是决议记录在协调组达多数派持久化的那一刻。**
  剩该文第 6 步（升主 in-doubt 清理、快路径分叉归队规则），需与惰性回放的
  promotion 路径合流。

## 3. 目标架构

master 是协调节点（Citus coordinator）：持权威元数据、group 0 leader 优先落于此、
**不作任何分区的数据副本**。每个 worker 内部同时跑着**控制面组的一个成员**和**若干
数据面组**，其中一部分数据组它是 leader（该分区的 primary），另一部分它是 follower
（该分区的 secondary）：

```mermaid
flowchart TB
  subgraph M["master (协调节点, 不持数据副本)"]
    C0["控制面 组0<br/><b>leader(偏置)</b><br/>登记处+配置权威"]
    PD[("pg_dist_placement<br/>partition_map")]
    C0 --> PD
  end
  subgraph N1["worker1"]
    C1["控制面 组0<br/>(follower)"]
    D1A["数据组 shard#1<br/><b>leader</b>"]
    D1B["数据组 shard#2<br/>follower"]
    P1[("pg_parwal/&lt;oid&gt;")]
    D1A --> P1
    D1B --> P1
  end
  subgraph N2["worker2"]
    C2["控制面 组0<br/>(follower)"]
    D2A["数据组 shard#1<br/>follower"]
    D2B["数据组 shard#2<br/><b>leader</b>"]
    P2[("pg_parwal/&lt;oid&gt;")]
    D2A --> P2
    D2B --> P2
  end
  D1A -->|AppendEntries + 原始 WAL 字节| D2A
  D2B -->|AppendEntries + 原始 WAL 字节| D1B
  D1A -.->|"当选后上报 (gid, node, term)<br/>pg_raft_report_data_leader"| C0
  D2B -.->|当选后上报| C0
  C0 -->|"OP_PARTITION_PRIMARY(带 primary_term)<br/>group0 复制, 每节点 apply 落<br/>partition_map + pg_dist_placement"| C1
  C0 --> C2
```

注意图中 worker1 对 shard#1 是 leader、对 shard#2 是 follower，worker2 恰好相反 ——
这正是 §2.1 的架构前提，**数据面没有全局主节点**；master 只是控制面/路由的协调点。

分层原则（2026-07-24 起的职责边界，§13.1）：

- **控制面 Raft（组 0，成员=全部 4 节点，leader 偏置 master）**：**登记处 + 配置权威**——
  接收数据组 leader 的当选上报、任期栅栏裁决、经 raft 日志把结果同步到每个节点
  （含路由层 `pg_dist_placement`）；节点状态、组配置/成员集（待下发机制）、快照；
  后续的 xid 区间租约、2PC 恢复等全局服务。**控制面不决定数据面谁当 leader，
  也不按分区拆分。**
- **数据面 Raft（每分区一组，成员=该分片副本所在 worker，绝不含 master）**：自治选举
  （leader 即该分区 primary）、该分区 parwal 记录的复制与定序、当选后主动上报。
  日志内容是 `pg_parwal/<partition>/` 中已存在的每分区单调记录，Raft 只负责运输、
  定序和多数派持久化，**不重新设计一套记录格式**。
- **运输层 vs 应用层**：数据组给出的是“这条记录已在多数派 fsync 落盘且定序”；把字节变成
  堆表内容的 redo 是骑在其上的应用层（P3），二者解耦验证，见 §11.1。
- **切主安全线**：对有数据组的分区，由 Raft 选举限制内生保证（新 leader 必持全部已提交
  条目）+ `primary_term` 任期栅栏（迟到/重复登记拦下）；旧的
  `switch_partition_lsn`/`applied_part_lsn` 候选过滤仅服务 `primary_term = 0` 的遗留分区。
- **一致性边界**：登记结果写 `partition_map` 与本地 `pg_dist_placement`（每节点一份，
  由 group 0 的 apply 完成，不是全节点强同步——多数派提交+全员 apply，宕机者重放追平）；
  成员集必须由控制面下发，不能从收到的报文里推断（当前缺口，§11.5.2 #3）。

## 4. 阶段目标

> **阶段完成度速览（2026-07-24）**：阶段 0 ✅ / 阶段 1 ⚠️ 大部完成（缺 InstallSnapshot
> 与日志压缩；环满假 ack 已修）/ 阶段 2 ✅ 且其"控制面指定式切主"已被 §13 的
> "自治选举+上报登记"取代（切主通知仍为占位）/ 阶段 3 ⚠️ 运输部分完成并经 raft_14 按
> 真实分片验收、redo 与 2PC 未做 / 阶段 4 ⚠️ P0+P1+P2+切主重构完成、P3 未启动。

### 阶段 0：归档旧路线并清理文档 ✅ 已完成

目标：把 pgraft Go bridge 从当前实施路径中移除，仅保留为历史调研资料。

工作项：

- 清理旧 pgraft spike 文档、脚本和容器挂载入口，不再保留为当前仓库的运行入口。
- 从主文档中删除或弱化 `pgraft_is_leader()`、`pgraft_kv_put/get`、Go runtime、`shared_preload_libraries='pgraft'` 作为实施步骤的描述。
- 保留并更新 `README-RAFT.md`、`setup-raft.sh`、`run-tests.sh`、`docs/interfaces/partwalmgr_raft.idl`。
- 不删除 PostgreSQL 数据目录、构建产物和 `pg-cluster-data` 运行状态文件，除非后续单独执行工作区清理。

验收标准：

- README 明确写明当前主线是纯 C `pg_raft`。
- 旧 pgraft 资料不再作为仓库入口；需要历史记录时从 Git 历史查看。
- 过时 demo、组会展示、raft10 或备份脚本不再作为推荐入口。

### 阶段 1：补齐纯 C Raft 安全语义 ⚠️ 大部完成

目标：将当前 `raft_consensus.c` 从 demo 级共识推进到“最小安全 Raft 子集”。

工作项：

- ✅ 扩展 RequestVote RPC：`term, candidate_id, last_log_index, last_log_term`。
- ✅ 加入投票前的日志新旧检查，只有日志至少一样新的候选人才授票。
- ✅ 持久化 HardState：`current_term`、`voted_for`、`commit_index` 必须先落盘，再响应
  RequestVote 或 AppendEntries。（2026-07-20 升级为文件 v2，增加 `last_applied`，兼容读 v1。）
- ✅ 完善 follower catch-up：落后 follower 恢复后通过 `nextIndex/matchIndex` 追赶。
- ✅ **snapshot install（2026-08-05 落地，仅控制面）**：`partdist.pg_raft_install_snapshot()`
  RPC + `send_install_snapshot()` 发送侧。此前 `raft_snapshot` 只是一张**没有消费者**
  的表（每次 apply 都在写，却既没有 RPC 也没有按 `last_included_index` 删行），
  即"有快照内容、无快照机制"。
  > 边界（2026-08-05 同日确认）：落后超 ring 容量**本身不再需要快照** —— 追平通道
  > 会从 `partdist.raft_log` 回读环外条目（§12.4 #5，回归 raft_24）。快照的必要性
  > 因此是"**压缩的前提**"：一旦按 `last_included_index` 删行，被删那段就只剩快照
  > 一条路。两者是一件事的两半，同期落地，回归 raft_25。
  > **数据组不做**：它的状态机在 P3 之前就是 parwal 字节流本身，"快照内容"与
  > "日志内容"是同一份东西，装快照等于重放日志 —— 那只会是个假机制。
- ✅ 完善 leader 宕机重选：新 leader 必须拥有所有已提交日志，旧 leader 恢复后必须降级为
  follower（raft_08 / raft_11）。
- ⚠️ **替换固定 ring buffer 的长期假设，补上日志截断/压缩策略**：
  **压缩已于 2026-08-05 落地（控制面）**：`pg_raft.compact_threshold`（缺省 500）
  触发 `control_maybe_compact()`——先把快照连同 `(last_included_index,
  last_included_term)` 写进 `partdist.raft_snapshot`，再删 `raft_log` 里
  `<= last_applied` 的行，基点随 hardstate v3 持久化。回归 raft_25。
  **定长 ring 本体仍在**（`RAFT_LOG_CAPACITY = 128`）：
  > 环不再是**追平**的上限（环外条目已能从 `raft_log` 回读，§12.4 #5），
  > 但仍是 **shmem 占用**（每组 128×800B ≈ 100KB，`RAFT_MAX_GROUPS=32` 的由来）
  > 与**提交路径开销**（每条数据条目一次 `raft_log` INSERT + 一次 committed
  > UPDATE）的上限。**数据组的压缩也仍然没有**——正解是日志外部化到 parwal
  > （§11.5.2 #2），不是在数据组里删行。

验收标准：

- 多数派不可用时 propose 不提交。
- 日志落后的节点无法赢得选举。
- 旧 leader 恢复后不会覆盖新 leader 已提交状态。
- follower 掉线恢复后能通过日志或快照追平。

### 阶段 2：控制面 failover 正确性与新版 PartWAL 进度对接 ✅ 已完成（已被 §13 取代为遗留通道）

目标（当时）：把分区 primary 切换从“改元数据”升级为“Raft 决议 + 新版 PartWAL 进度证明 + 切主通知”。

> **2026-07-24 起本阶段的"控制面指定式切主"降级为遗留通道**：数据组落地后，有组的分区
> （`primary_term > 0`）切主完全由组内自治选举 + 上报登记完成（§13），下述流程只对
> `primary_term = 0` 的历史/合成分区生效，随夹具迁移退役。历史落地情况：流程 1–6 全部
> 完成（回归 raft_02 / raft_07 / raft_10）。**第 7 步“follower replay 随新 primary 完成
> 角色和进度刷新”未完成** —— `applied_part_lsn` 自 P2 起已是真值（由数据组 apply 推进），
> 但 `partwal_notify_primary_switch()` 至今仍只写日志，不做真实角色切换；
> 真正的角色切换依赖 P3 物理回放落地。

`OP_PARTITION_PRIMARY` payload（2026-07-24 增加 `primary_term` 任期栅栏字段）：

```json
{
  "partition_id": 9102,
  "old_primary_node": 3,
  "primary_node": 1,
  "secondary_nodes": [2],
  "primary_term": 8,
  "switch_partition_lsn": 123,
  "switch_orig_lsn": "0/16B6A20"
}
```

切主流程：

1. TopologyMonitor 检测到 primary 故障后，生成一次 failover 提议。
2. 当前 leader 从新版 PartWAL 进度接口读取候选副本的 `applied_part_lsn` 或等价 ACK 进度。
3. 只有已经追平到 `switch_partition_lsn` 的副本才允许成为新的 primary；若没有进度来源，则拒绝自动提升并记录告警。
4. leader 提交 `OP_PARTITION_PRIMARY`。
5. apply 阶段更新 `partition_map`，并刷新 `pg_partdist` 缓存。
6. apply 完成后调用新版切主通知接口；若上游尚未提供，则保留并实现 `partwal_notify_primary_switch(partition_id, old_primary, new_primary, switch_orig_lsn)` 作为 Raft 与 PartWAL 的边界。
7. follower replay / `follower_partition_map` 随新 primary 完成角色和进度刷新。

验收标准：

- 只切换受故障 primary 影响的分区。
- 未追平 `switch_partition_lsn` 的 secondary 永远不能被提升。
- 切主前后 `partition_lsn` 保持单调。
- 旧 primary 恢复后只能以 secondary 身份回归。

### 阶段 3：接入 shardpg-2.0 数据面同步与 2PC 决议 ✅ 2026-08-04 完成（redo 回放另计）

目标：基于 `shardpg-2.0` 已有 PartWAL 同步写入和 follower replay 设计，实现方案文档要求的真正多副本同步，而不是在 Raft 模块里重新设计一套数据面。

> 落地情况：**跨节点日志传输已由分区级 Raft 组实现**（P2 + 运输层加固，§11.5/§11.5.1）——
> 原计划设想的 WalSender/WalReceiver 流式通道**不再需要**，AppendEntries 本身就是那条通道，
> 且天然带多数派语义。
> **2PC 决议已于 2026-08-04 端到端落地**（`DTX_2PC_DESIGN.md` §10 第 0–5 步，
> 回归 raft_17–22）：决议不进控制面，写在写集内按 `hash(dtxid)` 选出的分区组日志里，
> 该记录达多数派持久化即为全局提交点；内核补丁 0004 的 `pre_record_commit_hook`
> 把它接进了客户端提交路径。仍未完成的是 **redo 回放（P3）**，以及升主时的
> in-doubt 清理（该文 §9.6，与惰性回放的 promotion 路径合流时再做）。

PartWAL 接入链路：

- ✅ Primary 写入本地 `pg_wal`，`wal_insert_hook` 通过 `PartWALInsert` 捕获分区相关 WAL 记录。
- ✅ `XACT_EVENT_PRE_COMMIT` / `XACT_EVENT_PRE_PREPARE` 调用 `PartWALFlush`，保证 `pg_parwal` fsync 早于事务提交 WAL fsync。
- ✅ **跨节点传输**：该分区的 Raft 组 leader 读出记录，随 AppendEntries 下发给成员；
  follower 先按 leader 指定的 `partition_lsn` 落盘并 fsync，再 ack。
- ❌ **follower 侧回放**：按 `FOLLOWER_REPLAY_DESIGN.md` 做 redo 尚未开始（P3）；
  当前只推进 `follower_partition_map.applied_part_lsn`，不改堆表。
- ✅ Raft 控制面读取 follower 进度，作为切主候选筛选的依据（进度自 P2 起为真值）。
- ❌ **无后台追平通道**：数据条目只在 client backend 的 propose 路径下发，
  且 `pg_raft_data_propose()` 尚未接入 demux/写入路径，目前仅回归在调用。

事务语义 —— **prepare 阶段的权威设计（2026-07-24 用户定稿）**：

事务的 prepare 阶段，对事务涉及的每个分片组 Si **并行**执行：

1. Si 的 Leader 把该分片的数据修改记录（DATA Record，即 heap WAL 内容）写入
   `pg_parwal/Si/` 段文件并分配 `partition_lsn`。此时的 DATA Record 只含数据变更、
   不含提交标记，处于**预备态**。
   → 机制已具备：`wal_insert_hook` 捕获 + PRE_COMMIT 落盘 fsync（`[A]<[B]` 不变式）；
   parwal 天然只收该分区的 heap 记录，commit record 属 XACT rmgr 不入 parwal，
   "不含提交标记"自动成立。
2. Leader 将该 DATA Record 作为 Raft Log Entry 发送给 Si 的所有 Followers。
   → ✅ **已自动挂接（2026-07-24 当日完成，见 §14）**：PartWALFlush 在 [A] 之后、
   [B] 之前经 rendezvous 挂钩对本事务涉及的每个分区逐条 propose；
   凑不齐多数派即 ERROR，事务在 prepare 中止。验收 raft_16。
3. Leader 和 Followers 收到日志后均写入本地 `pg_parwal` 并执行 fsync。
   → 已具备并验收：follower **先 fsync 再 ack**（P2 + 运输层加固），
   leader 侧 PRE_COMMIT fsync。
4. 该日志在 Si 达到 Raft 多数派持久化后，进入 prepared 状态。
   → 多数派语义已具备并验收：多数派提交 == 多数派已持久化（raft_13 反例：失多数派
   propose 必须失败）。**"prepared 状态"作为事务状态机本体已于 2026-08-04 落地**：
   参与者侧就是 PG 原生的 prepared transaction，配一条 `DTX_PREPARE` 标记记录
   （携带 dtxid ↔ 本地 top-level xid 的绑定）+ `partdist.dtx_participant` 登记，
   状态机的推进由协调组的决议驱动（`DTX_2PC_DESIGN.md` §6.1，回归 raft_22 C）。

其余（2PC 决议）——**2026-08-03 定稿，下述控制面方案已作废**：

> ~~Commit WAL 和协调者最终决议必须在**控制面** Raft 复制到多数派后才能成功返回；
> 控制面 Raft 新增 `OP_PREPARE_DECISION` 和 `OP_COMMIT_DECISION`。~~
>
> **被 `pg-partdist-src/docs/DTX_2PC_DESIGN.md` 取代。** 决议不进控制面，而是写在
> **本事务写集内按 `hash(dtxid)` 选出的那个分区组**的日志里，该记录达多数派即为
> 全局提交点。三条理由（该文 §3.2）：(1) 控制面 leader 常态在 master，每个跨分区
> 事务都压一次多数派写会让单点更单点；(2) group 0 成员是全部 9 节点、数据组成员是
> 3 副本子集，两个多数派互不蕴含，恢复要做跨组交叉判定；(3) §13 的自治选举+上报
> 已让"某组现任 leader 是谁"成为随时可查的元数据，放数据组不引入新的服务发现问题。
> 形态与 Spanner（commit record 写 coordinator Paxos group）、TiKV（decision 写
> primary region）一致。

- 决议记录、PREPARE/COMMIT 标记的字节格式见该文 §5；协调组选取见 §4；
  协调者宕机走 **presumed abort**（查无决议即中止）而非"新 leader 续跑"，见 §2.2。
- 接口继续与 `GlobalXID`、TSO、增强 CLOG 保持对齐（TSO 未建时 `commit_ts` 先用
  协调者本地时钟，决议原子性不依赖它，见该文 §9.8）。

验收标准：

- prepare / commit 未达到 quorum ACK 前，客户端不能收到成功。
- primary 故障后，新 primary 能恢复 prepared / committed 决议。
- 跨分区事务不会出现部分提交。

### 阶段 4：长期演进为每分区独立 Raft group

目标：将每个 partition replica set 演进为独立 Raft group。

> 具体工程分期、共享内存/日志存储模型、调度器、全局分片身份与"运输定序层 vs
> 回放应用层"的定位,见 §11《分区级 Raft 组落地方案(2026-07-17)》。本节仅保留方向性描述。

设计方向：

- 每个 partition 独占一个 Raft group，成员为该分区的 primary + secondaries。
- partition primary 就是该 partition 的 Raft leader。
- 数据日志可以直接作为 Raft log entry，或以 Raft 有序方式绑定 PartWAL record 提交。
- 控制面 Raft 继续负责拓扑、成员变更和故障感知，数据面 Raft 负责该分区日志提交。

验收标准：

- 单个分区的 leader 切换不影响其他分区。
- 写入只在该分区多数派提交后成功。
- 成员变更通过 joint consensus 或等价安全机制完成。

## 5. 接口规划

### Raft RPC

- ✅ `RequestVote(group_id, term, candidate_id, last_log_index, last_log_term)`
- ✅ `AppendEntries(group_id, term, leader_id, prev_log_index, prev_log_term, leader_commit, entries[])`
  —— 数据组条目额外随行一个 `bytea` 原始 WAL 字节参数。
- ✅ `pg_raft_install_snapshot(term, leader_id, last_included_index, last_included_term,
  node_map, partition_map)` —— **2026-08-05 落地，仅控制面（组 0）**。载荷是
  `partdist.raft_snapshot` 里的两张元数据表；发送侧在 leader 发现某成员的
  `nextIndex` 落到压缩基点及更早时自动调用（需要 SPI，所以挂在追平通道的
  client backend 语境上）。数据组不做——理由见 §4 阶段 1。
  > 两条**靠推理定下、未被用例直接覆盖**的不变式（都只在竞态下才现形，构造不出
  > 确定性夹具，因此不写进判据、也不算已验证）：
  > ① 发送侧记 `matchIndex/nextIndex` 用的是**快照行里的 `last_included_index`**，
  >    不是调用时读到的 `base_index` —— 取载荷的条件是 `>= base_index`，两者之间
  >    又压缩一轮的话对端装到的是更新的点，按旧点记游标会去补一条对端已删的条目，
  >    白退一轮 nextIndex 才重新触发快照（能自愈，只是浪费一轮）。
  > ② 接收侧装完把 `last_applied` **回拨**到 `last_included_index`（不是只往上抬）：
  >    状态机刚被整表替换成"截至该点"的那一份，保留下来的尾巴必须重放才对得上；
  >    只抬不降的话，本地游标若已越过该点，尾巴永不再 apply，状态机反而倒退。
  >    控制面 apply 全是幂等 upsert，重放无害。
- ⚠️ 报文里的 `group_id` 缺省 0（向后兼容），但**成员集不在报文里传播**，是当前缺口。

### 控制面日志操作

- ✅ `OP_NODE_STATUS`
- ✅ `OP_PARTITION_PRIMARY`（2026-07-24 起 payload 含 `primary_term`；apply 带任期栅栏，
  真实 Citus 分片同步更新本地 `pg_dist_placement`——单放置守卫 + 子事务隔离）
- 🚫 ~~`OP_PREPARE_DECISION`~~ / ~~`OP_COMMIT_DECISION`~~ **已废弃，不会实现**：
  2PC 决议改由数据组承载，控制面不参与（见 `DTX_2PC_DESIGN.md` §3.2）。
  新增的是**数据组内**的记录类型（`PARTWAL_FLAG_DTX` + `DtxRecordPayload`，
  该文 §5）与两个 SQL 入口 `partdist.dtx_decide` / `partdist.dtx_status`（§6.3）。
  **→ 2026-08-03 已落地并经 raft_20 验收**（决议记录在协调组达多数派持久化即为
  全局提交点）。**→ 2026-08-04 接进客户端提交路径**：内核补丁 0004
  （`pre_record_commit_hook`，在 `CommitTransaction()` 里、全部
  `XACT_EVENT_PRE_COMMIT` 回调之后、`RecordTransactionCommit()` 之前）+
  参与者自治登记 `partdist.dtx_participant` + master 侧写集收集/协调组选取/决议，
  经 raft_22 端到端验收。**跨分区事务现在真的走 2PC。**
- ⚠️ `OP_CONFIG_CHANGE`（成员**变更**仍依赖它）。**成员集下发已不依赖它**：
  2026-08-03 起数据组成员集从控制面已下发的 `partdist.partition_map` 本地导出
  （`DTX_2PC_DESIGN.md` §9.2），无需新增 RPC。
- ✅ 数据组内部条目类型：`OP_PARWAL`（描述符 + 随行字节；数据字节的门控按 `op_type` 判定，
  **不能**按 `group_id > 0` 判定，否则数据组里的普通条目复制不出去）

### 切主重构新增接口（2026-07-24，§13）

- ✅ `pg_raft_report_data_leader(p_group_id BIGINT, p_leader_node INT, p_term BIGINT,
  p_secondary_nodes INT[]) → BIGINT`：数据组新任 leader 的登记入口，须在 group 0 leader
  上执行；返回 >0=已提名 / -1=无需登记 / 0=非 group 0 leader（上报方据此重试或停止）。
- ✅ GUC `pg_raft.coordinator_node_id`（缺省 1）：协调节点 —— group 0 选举偏置窗口
  [2b/3, b)、数据组成员集/登记双重排除。
- ✅ `partdist.partition_map.primary_term` 列：任期栅栏；0 = 尚无数据组接管
  （遗留通道的生效条件）。

### PartWAL 对接接口（`pg-partdist-src/src/raft_boundary.c`，schema `partdist`）

**调用约定**：所有函数都收**本节点的** `partition_id`（= 分片表本地 OID = `pg_parwal/` 目录名）。
各节点 OID 不同，调用方必须先用 P0 的 `local_partition_for_shard(global_shard_id)` 把组 id
解析成本节点的 OID —— 这是“一节点多角色”架构下最容易出错的一步。

进度与切主（阶段 2 落地）：

| 函数 | 语义 | 状态 |
|------|------|------|
| `get_partition_flush_lsn(OID) → BIGINT` | 本节点该分区最新已持久化 `partition_lsn` | ✅ |
| `get_follower_applied_part_lsn(OID) → BIGINT` | 本节点该分区 follower 的 `applied_part_lsn` | ✅ |
| `partwal_notify_primary_switch(OID, INT, INT, PG_LSN) → void` | `OP_PARTITION_PRIMARY` apply 成功后调用 | ⚠️ 仍是日志占位 |

数据组复制（P2 + 运输层加固落地）：

| 函数 | 语义 | 状态 |
|------|------|------|
| `partwal_read_record(OID, BIGINT) → (orig_lsn, rmid, info, xid, data)` | leader 按 `partition_lsn` 读出整条记录 | ✅ |
| `partwal_follower_append(OID, BIGINT, PG_LSN, INT, INT, BIGINT, BYTEA) → BIGINT` | follower **按 leader 指定的 `partition_lsn`** 落盘 + fsync；重传幂等 no-op，空洞 ERROR | ✅ |
| `partwal_truncate_to(OID, BIGINT) → BOOLEAN` | Raft 日志截断时同步截断 parwal 字节 | ✅ |
| `follower_set_applied_part_lsn(OID, BIGINT) → BOOLEAN` | 单调推进进度游标 | ✅ |

全局分片身份（P0 落地）：`shard_global_id(OID)` / `rebuild_shard_identity()` /
`register_shard_identity(OID)` / `local_partition_for_shard(BIGINT)` / `global_id_for_partition(OID)`。

> **维护约束**：`partwal_follower_append` 的签名在运输层加固中变过一次（新增第 2 个参数
> `p_partition_lsn`）。改签名时必须同步改 `COMMENT ON FUNCTION` 的参数列表、`setup-raft.sh`
> 的 `ensure_boundary_functions` 与其 cleanup 段，以及 `pg-install/share/postgresql/extension/`
> 下的已安装副本。**已装扩展不会重跑安装脚本**，所以这类不一致在回归里是静默的——
> 只有在全新库上 `CREATE EXTENSION` 才会暴露。（2026-07-21 即因此修了一次遗漏的 COMMENT。）

## 6. 测试计划

现有基线：**`pg-raft-src/run-raft-tests.sh` 共 32 项断言全绿（raft_01–16）**，
外加 `pg-partdist-src/tests/test_shard_identity_p0.sh` 10/10 与全新库
`CREATE EXTENSION pg_partdist` + `pg_raft` 冒烟。
（入口已从旧的 `run-tests.sh` 改为 `run-raft-tests.sh`；该脚本**必须在宿主机上跑**，
它内部自己调 `docker`，但 `-f` 引用的测试 SQL 路径是容器内路径——新增测试文件要
`docker cp` 进容器。）

| 用例 | 覆盖场景 | 状态 |
|------|------|------|
| `raft_01_leader_election` | 选举收敛，唯一 leader | ✅ |
| `raft_02_failover_partition` | primary 故障只切受影响分区 | ✅ |
| `raft_03_split_brain_guard` | 脑裂防护 | ✅ |
| `raft_04_topology_monitor` | 拓扑感知与自动 failover | ✅ |
| `raft_05_no_majority_reject_commit` | 少于多数派拒绝提交，无脏 apply | ✅ |
| `raft_06_stale_requestvote_rejected` | 日志落后候选人竞选失败 | ✅ |
| `raft_07_uncaught_up_secondary_not_promoted` | 未追平副本不得晋升 | ✅ |
| `raft_08_old_leader_rejoins_as_follower` | 旧 leader 恢复后降级 | ✅ |
| `raft_09_hardstate_crash_recovery` | 崩溃重启 term 不回退、已提交日志不丢 | ✅ |
| `raft_10_most_caught_up_secondary_promoted` | 最追平副本被提升，切换点=真实 flush 进度 | ✅ |
| `raft_11_old_leader_log_catchup` | 旧 leader 回归后追平已提交决议 | ✅ |
| `raft_12_multi_group_isolation` | 多组独立选举、日志按组隔离 | ✅ |
| ↳ raft_12 夹具于 2026-08-03 随 §9.2 调整 | 两处旧写法依赖的正是被修掉的不安全行为：① 以**空成员集**建组（现已报错拒绝）⇒ 改为显式 `ARRAY[2,3,4]`；② 在**控制面 leader（常态是 master）**上断言"两个数据组均可见"——master 永不作数据副本，旧写法能过只因空成员集会让组经 hearsay 撒到全集群 ⇒ 改到数据组成员节点上断言。顺带修掉"非 leader 提交被拒"挑到组外节点的问题（那里组根本不存在，propose 同样返回 0，是"对的结果、错的原因"） | — |
| `raft_13_data_group_replication` | 数据组多数派提交、字节级一致、失多数派拒写（reference 夹具，保留作运输层回归） | ✅ |
| `raft_14_hash_shard_secondary_backup` | **真实哈希分片 (a) 形态**：多记录逐条 propose、follower 逐字节指纹一致、`partition_lsn` 1..N 连续无洞、一条 record 一次备份、不回放（壳表 0 行）、初次登记（primary/term/secondaries 不含 master）、路由层一致、master 无分片身份 | ✅ |
| `raft_15_self_election_failover` | **切主全链路**：停主 → 组内自治选举 → 上报登记 → 每节点 `partition_map`+`pg_dist_placement` 落新主（任期递增、master 不入 secondaries）→ 旧主重启以 follower 归队、登记不回退 | ✅ |
| `raft_16_prepare_auto_replicate` | **prepare 接线（§14，全程无手工 propose）**：仅 INSERT 即自动逐条复制、逐字节一致；失多数派 INSERT 必败（prepare 中止）行数不变；恢复后自动追平。**两处终态判据于 2026-08-04 改为有界重试（30s 窗口）**：2PC 接线后一笔事务要跑三轮复制（DATA／PREPARE 标记／COMMIT 标记），各自凑各自的多数派，同一个 follower 可能连续两轮都不在多数派里而短暂落后，靠 leader 下一次心跳补齐——终态一致但到达得比固定 `sleep 2` 晚。这是"缺后台追平通道"（§12.4）的既有边界被放大，不是新缺陷（`DTX_2PC_DESIGN.md` §9.7.1） | ✅ |
| `raft_17_concurrent_prepare_quorum` | **并发 prepare 的多数派保证（§14.3 #1 的让路窗口）**，用例本体在 `test/raft_17_concurrent_prepare_quorum.sh`（可独立跑），三阶段：① 同 worker 两数据组 + 8 会话并发单行 INSERT，断言持有完整前缀的成员数 >= 多数派（不是"全体追平"——Raft 只保证 quorum，且尚无后台追平通道）；② **确定性让路**：长事务 P 写 B 组后 pg_sleep，并发短事务 Q 的 flush 顺带消费其槽位并推过 flushed_upto，P 提交必走提前返回分支——断言被让路的 P 的记录仍达多数派；③ 失多数派 + 并发写，断言提交成功行数 == 0（2PC prepare 性质回归）。两个 burst 阶段均先甄别节点崩溃再断言。**真对照实验**（同用例仅换 .so、nm 验证构建身份）：让路窗口未修的构建在阶段二确定性失败（P 提交成功而记录只在 leader：43 vs 41/41），修复版 43/43/43 | ✅ |
| `raft_18_membership_explicit` | **成员集显式化与真实多数派（§11.5.2 #3）**，用例本体在 `test/raft_18_membership_explicit.sh`（可独立跑），四条确定性判据：A 成员集未知（NULL 且 partition_map 无登记）时**建组必须报错拒绝**且不留残组；B 有控制面登记时 NULL 建组**自动导出**成员集，`cluster_size=3` 而非 9；C **quorum 按真实成员数**——3 成员全在可写、停 1 个（2/3）仍可写、停 2 个（1/3）必败；D 非副本节点不被 hearsay 拖入该组。真对照（nm 验证构建身份）：修复前 A 处 `group_create` 返回 `t`，建出 `cluster_size=9` 的组、向全集群广播选举、真正的数据持有者反被挤成 follower。**注意 C 不断言"某个特定节点当选"**——Raft 不保证哪个成员赢，用例动态发现 leader 与待停 follower（初版硬断言 worker1 当选，实测 worker3 先超时先当选而误报） | ✅ |
| `raft_19_dtx_record_format` | **DTX-2PC 记录格式与 flags 端到端保真（`DTX_2PC_DESIGN.md` §5）**，用例本体在 `test/raft_19_dtx_record_format.sh`（可独立跑），四段：A **全新库 `CREATE EXTENSION` 冒烟 + 四个函数签名**（这是唯一能抓到签名不一致的检查，本次实施踩到两次）；B leader 侧 DATA `flags=1`、DTX `flags=8`/`orig_lsn=0`/`info` 载子类型、DECISION 载荷（dtxid/coord/ts/verdict/participants[]）完整往返；C 对 DATA 记录调 `partwal_read_dtx_record` 返回 NULL（分类以 flags 判定）；D **follower 侧 flags/info 序列与 leader 完全一致** | ✅ |
| `raft_20_dtx_decision` | **DTX-2PC 决议层（`DTX_2PC_DESIGN.md` §6）**，用例本体在 `test/raft_20_dtx_decision.sh`（可独立跑），五段：A 非协调组 leader 调 `dtx_decide` 返回 NULL 且不留痕；B **COMMIT 决议返回后 DECISION 记录与索引表在协调组全部成员上均在**——返回即"已在多数派持久化"，这就是全局提交点；C 决议槽一次性（对同一 dtxid 再决议 ABORT 仍返回 1）；D **推定中止**：查无决议时 `dtx_status` 先写 ABORT 达多数派再答 2，此后 COMMIT 无法翻盘；E **协调组切主后两笔决议仍可查**——索引表由各成员 apply 时各自维护，协调权随 Raft 选举自动转移，无需状态搬迁；F **回执与 FORGET**（§9.7 决议 GC，2026-08-04）：部分回执不删且 acked 记账、收齐即写 FORGET 记录复制到多数派、**各成员 apply 时同步删除决议行**（删除走与写入相同的复制路径）、迟到回执幂等不复活 | ✅ |
| `raft_21_dtx_recovery` | **DTX-2PC 参与者侧恢复守护（`DTX_2PC_DESIGN.md` §7）**，用例本体在 `test/raft_21_dtx_recovery.sh`（可独立跑），八段：A 协调组已有 COMMIT 决议 ⇒ 恢复守护提交 prepared 事务、数据可见、补 `DTX_COMMIT` 标记；B 从未决议 ⇒ 经 `dtx_status` **推定中止**、回滚、补 `DTX_ABORT` 标记且**决议已落库**（推定中止是写下来的，不是隐含的）；C 未超时的 prepared 事务不被触碰（不与正常路径抢答）；D **协调组不可达 ⇒ 保持 prepared 不动**——绝不擅自决定；E **登记缺失的 citus gid 按 Citus 规则闭合、不许永久滞留**（Citus 自己的恢复已被关掉，没人兜底就是行锁永久滞留；夹具必须显式删登记行——手工 PREPARE 也会被参与者接线自动登记，不删的话测不到这条路径，2026-08-04 真对照抓出）；F Citus 规则提交侧（master 的 `pg_dist_transaction` 有该 gid ⇒ 参与者提交）；G **initiator 存活栅栏**（发起 backend 还活着 ⇒ 绝不推定中止——`pg_dist_transaction` 的行在 master 本地提交前不可见，"慢 master"会被误回滚成分叉；用 master 的 checkpointer pid 当永活发起者，确定性无竞态）；H **守护自动闭合**（TopologyMonitor 周期自触发，无任何手工 SQL）。**E/G 均有真对照**：修复前构建 E 处 prepared 永久滞留、G 处活发起者的事务被误回滚；I **`pg_dist_transaction` 的 GC**（§9.7，2026-08-04）：只删"发起者已死 + 全网确认无 prepared"的行，发起者活着/仍有 prepared/任一节点不可达都保守不删（不可达返回 -1 整轮放弃） | ✅ |
| `raft_23_dtx_close_indoubt` | **升主 in-doubt 闭合机制（`DTX_2PC_DESIGN.md` §9.6，机制先行）+ 混合写集告警（§9.4）**，用例本体在 `test/raft_23_dtx_close_indoubt.sh`（可独立跑），五段：A 有登记 ⇒ `dtx_status` 权威通道，推定中止先落库，流为 `1,2,4`（协调组即自身时 ABORT 决议兼任闭合）；B 无登记、决议在**成员不相交的另一个组** ⇒ 广播 peers 的决议索引命中，闭合 COMMIT；C citus 形态 dtxid ⇒ 前缀查 `pg_dist_transaction`（带发起者存活栅栏，只认 COMMIT）；D **四级阶梯全落空 ⇒ 保持 in-doubt 不动、重复调用幂等**——不知道协调组是谁就无法把推定中止写下来，绝不无凭据闭合；E 纳管+非纳管混合写集在 PREPARE 时收到 WARNING。剩给第 6 步的只是把函数插进升主序列与合流验收 | ✅ |
| `raft_24_background_catchup` | **后台追平通道（§12.4 #5）**，用例本体在 `test/raft_24_background_catchup.sh`（可独立跑），四段：A **关掉通道（`catchup_interval_ms=0`）+ 无任何写入 ⇒ 必须不收敛**——这一半就是对照，没有它，B 段的收敛无法归因；B 打开通道（reload 生效，顺带走一遍 SIGHUP 通路）后仍不写入任何数据，落后成员在窗口内条数+逐字节指纹追平；C **落后 >128 条（超环容量）也能追平**，走的是环外条目从 `partdist.raft_log` 回读；D 追平**不产生新条目**（`last_log_index` 前后不变）且收敛后再调返回 0。**夹具两个硬约束**：① 段文件基线必须 `partwal_truncate_to(oid,0)` 清零——OID 复用会让新壳表继承上一轮的记录（raft_13 老教训，本次实测踩到）；② **不能假定领导权落在 placement 节点、更不能假定它不变**——起步时三成员日志都空谁先超时谁当选，且本实现无 PreVote，成员重启会带更高 term 竞选把在任 leader 打成 follower 一轮，所以每次写入前都重新发现 leader 并等路由层跟上。**真对照**（同用例仅换 .so）：只把 `log_get_entry_sql` 摘掉重编，A/B 照过、C 确定性失败（follower 卡在 41、leader 401） | ✅ |
| `raft_25_snapshot_compaction` | **控制面日志压缩 + InstallSnapshot（§4 阶段 1 / §12.4 #7）**，用例本体在 `test/raft_25_snapshot_compaction.sh`（可独立跑），五段：A 压缩真的发生（基点推进、`raft_log` 里 `<= 基点` 的行为 0、快照行的 `(index, term)` 与基点**成对**一致——配错对会让接收方的 prev 检查永久错位）且压缩后新提案照常提交；B **落后到压缩点之前的成员靠快照追平**；C 装完快照状态机一致（只比**由日志决定**的列：`last_heartbeat`/`updated_at` 是各节点本地 `now()`，比它们是在测设计从未承诺的东西）；D 基点跨重启不丢（hardstate v3）；E **压缩之后重启不丢日志尾巴**（全节点重启后新条目不得落在已存在的 index 上——按压缩前的恢复写法会直接覆盖已提交条目，是 in-build 对照）。**B 的构造有个坑（首版实测踩到）**：压缩只删 SQL 行，shmem 环里最近 128 条还在，leader 照样逐条补得上——只推进 30 条时，把 `send_install_snapshot()` 摘掉的对照构建**也照样通过**（假测试）。必须推进 **>128 条**让环绕过去，那些条目才真的不复存在。**E 的构造也有个坑**：造尾巴之前必须先把 `compact_threshold` 顶高冻住基点，否则那几条 apply 完立刻又触发一次压缩、基点直接追到日志末端，尾巴长度为 0 —— 集群安静时必然如此，只有恰好有后台探测补上新条目才侥幸成立（首次跑套件正是这样蒙混过去的，独立重跑才暴露；用例里留了"尾巴为空即判构造无效"的自检）。**真对照**（同用例仅换 .so）：摘掉发送侧后 B 段确定性失败（落后成员冻结在 539，leader 已到 756） | ✅ |
| ~~`raft_26_log_runs_externalize`~~ ⏪ **已随 E1–E3 回滚删除（§0.4）；以下为历史留档** | **数据组日志外部化 E1（§11.10）**，用例本体在 `test/raft_26_log_runs_externalize.sh`（可独立跑），五段：A **重建等价**（每条 `raft_log` 与 `pg_raft_entry_from_parwal()` 重建出来的 (term, payload) 逐条相同 —— 比 jsonb 不比文本，payload 列是 jsonb，读回来已被规范化，比文本会假失败）；B **run 真的是段**（连续同任期的 N 条只占 1 行，退化成逐条一行就只是把 `raft_log` 换了张表）；C **跳号提案失败且不留痕**；D follower 侧同样成立；E **换届另起一段**（停掉 leader 让剩下两个成员在更高任期里选出新 leader，再提案一条）。**判据构造被实测推翻过一次**：首版 C 段写的是【跳号会另起一个 run】，跑出来才发现那是**不存在的状态** —— `AppendPartWALRecordAt()` 对 `expected > last + 1` 直接 ERROR（【物理回放要求流完整有序无 gap】），follower 落不了盘就不 ack、leader 凑不齐多数派、条目被丢弃。改判真实存在的性质之后，反而抓出一个真缺陷：run 写在复制之前，回滚路径不清它就会留下指向不存在条目的段起点。**三重对照**：① 不维护 run ⇒ A 段 27/27 全错；② 退化成逐条一行 ⇒ B 段确定性失败（27 行）；③ in-build：回滚不清 run ⇒ C 段确定性失败 | ✅ |
| ~~`raft_27_data_catchup_from_parwal`~~ ⏪ **已随 E1–E3 回滚删除（§0.4）；以下为历史留档** | **数据组日志外部化 E2（§11.10）**，用例本体在 `test/raft_27_data_catchup_from_parwal.sh`（可独立跑），三段：A **落后超过环容量的数据组成员靠重建追平**（成员停机期间提案 140 条 > `RAFT_LOG_CAPACITY`=128，最老那几十条已滑出环窗口，只能从段文件重建），且判据要求**字节真的落盘**（victim 段文件记录数 == leader 日志末端，不只是 `raft_log` 行数对上）；B **缺口必须真的超过环容量**（否则退化成环内追平，把重建摘掉也照样通过 —— raft_25 B 段就这么假过一次）；C 追平后 victim 上每条条目与它自己按 run 重建的结果逐条一致。**真对照**（同用例仅换 .so）：`log_get_entry_parwal()` 直接返回 false ⇒ A 段确定性失败（victim 冻结在 0，leader 已到 140） | ✅ |
| ~~`raft_28_data_log_no_sql_rows`~~ ⏪ **已随 E1–E3 回滚删除（§0.4）；以下为历史留档** | **数据组日志外部化 E3（§11.10）**，用例本体在 `test/raft_28_data_log_no_sql_rows.sh`（可独立跑），三段：A **写路径真的瘦了**（提案 N 条之后该组在 `partdist.raft_log` 里是 0 行，而 run 表有行、三个成员段文件各 N 条字节）；B **重启单个成员末端不丢**（末端此后只有 hardstate v4 一个来源）；C **全节点重启后新条目不得落在已存在的 index 上**（Leader Completeness 前置）。**夹具两个硬约束**：① 三成员一律 `partwal_truncate_to(oid,0)` 后由 leader 用 `write_partition_wal_record` 自造 N 条 —— 不能依赖 demux 产出多少条，更不能假定 `pg_parwal/<oid>/` 是干净的（被 DROP 的分片表留下的目录不回收，新表拿到复用 OID 就继承上一轮记录，首版实测报【段文件里 21 条记录，应为 20】）；② 造下一条要提案的记录必须用 `write_partition_wal_record` 本地自增，**不能提案段文件里编号最大的那条** —— follower 追加要求 plsn == 本地 last+1，跳号直接 ERROR、不 ack、凑不齐多数派（首版实测返回 0）。**真对照**：`persist_hard_state_values()` 里把 `hs.last_log_index` 写成 0（= v3 行为）重编 ⇒ B 段确定性失败（重启后末端 1，应为 20） | ✅ |
| `raft_22_dtx_end_to_end` | **DTX-2PC 端到端（`DTX_2PC_DESIGN.md` §3.3/§3.4/§5.3/§8.3/§9.3）**，用例本体在 `test/raft_22_dtx_end_to_end.sh`（可独立跑），六段：A 前置——二进制导出 `pre_record_commit_hook` 且逐节点 `citus.recover_2pc_interval=-1`；B **真实跨分区事务**提交后协调组有且仅有一条 COMMIT 决议、participants 恰为真实写集、协调组 == `participants_sorted[dtxid % n]`、**且决议在协调组每个成员上都在**；C 三阶段在 parwal 流里逐条可见（参与组 `DATA…,PREPARE,COMMIT` ／协调组 `DATA…,PREPARE,DECISION` 且**无**单独 COMMIT 标记），PREPARE 携带本地 top-level xid 且排在 DATA 之后；D **快路径**：单分区事务不写决议也不写标记；E **只读参与者剔除**：广播 UPDATE 打到全部 8 个分片但只有 1 个真改到行 ⇒ 不产生决议，同时只读参与者仍留 `gsids='{}'` 的登记（它崩溃后自解的唯一线索）；F **协调组失去多数派** ⇒ 事务**提交失败**且无行可见（不允许部分提交），并断言失败原因含"多数派"。**判据构造有两个坑**：① 只停协调组 leader 不行——组内还剩 2/3 会自治选出新 leader 并上报改写路由，决议照样做得出来、事务本就该成功（那正是"协调权随选举转移"在起作用）；② 也不能把协调组的 primary 算进被停的两个里——它同时是数据分片的主节点，停它测的是"主挂了写不进"这个与 2PC 无关的性质。必须停两个**非主**成员：数据主全活着，失败只可能落在 prepare 复制凑不齐多数派。**真对照**：`pg_raft.dtx_2pc_enabled=off` 重跑，B 段确定性失败 | ✅ |

> **run-raft-tests.sh 已于 2026-08-03 改为拓扑自适应**：按容器 `pg-cluster-data/`
> 下的实际目录探测协调节点目录名（raft4 是 `master`，pg_citus_raft 是
> `coordinator`）与 worker 数，节点 id ↔ 端口按 `BASE_PORT + N - 1` 算术推导。
> 因此同一套用例可在 4 节点（raft4）与 9 节点（pg_citus_raft）环境跑。
> 可用 `CONTAINER` / `COORD_DIR` / `N_WORKERS` / `BASE_PORT` 覆盖。
> quorum 编排本身与节点数无关（raft_05 停"除 leader 外全部节点"；数据组用例用
> 显式 3 成员组），所以只需要改映射函数。

> **⚠️ 套件不是幂等的，连跑多轮会积累状态（2026-08-04 实测）**：每轮都会新建
> Citus 分片、建/弃数据组、并因 leader 上报改写 `pg_dist_placement`；被 DROP 的
> 分片表留下的 `pg_parwal/<oid>/` 目录不会回收（实测单节点 170+ 个）。连跑到第
> 六轮时出现一批**与本次改动无关**的失败：控制面上报链路超时、`pg_dist_placement`
> 与数据组 leader 分叉导致"本节点不是该分区组的 leader"、raft_17 的长事务夹具
> 建不起来。就地重建 `pg-cluster-data`（initdb + Citus 接线 + setup-raft）之后
> 同一份代码全绿。
>
> **判据**：排查回归失败前先看它是不是只在"连跑很多轮之后"出现——是的话先重建
> 环境再复现，否则很容易把环境噪声误读成代码缺陷（本轮差点误判两次）。
> 重建脚本参照 `reproduce-env.sh` 的 `[4/6]`–`[6/6]` 三段；`reproduce-env.sh up`
> 本身会**重新克隆并覆盖工作区**，有未提交改动时不能直接用。
>
> 该类别的已见形态清单（都是"积累环境失败、重建后同代码全绿"）：控制面上报
> 超时、placement 与数据组 leader 分叉、raft_17 长事务夹具建不起来、raft_04/09
> 的 propose 单发失败，以及一次 **raft_16 字节指纹不一致**（第 7 轮套件时
> follower 5435 与 leader 等条数不同指纹、30s 重试不收敛；重建后未再现，
> 定向复现器 dbg16 亦未复现——**记录在案**：若再次出现，用逐 plsn 差异先定位
> 是哪条记录、哪个字段）。

> **✅ 并发写入路径三缺陷已定位并修复（2026-08-03，详见 `DTX_2PC_DESIGN.md` §9.0）**
> 首次并发压 prepare 路径连环暴露、当日全部修复：
> ① `ReadRawWALRecordAt` 的 static reader 在事务上下文里懒分配 → 跨事务 UAF
> （gdb 前无 raft 组也 3/3 必崩的 segfault 主犯；修复=分配与整个读取过程切
> TopMemoryContext）；② `data_propose_one` 对 `partwal_read_record` 的
> 全 NULL 行不判 `isnull` 直接 `TextDatumGetCString` → 空指针解引用
> （gdb backtrace 实锤，si_addr=0x0；失多数派回滚后读被截断的 plsn 触发）；
> ③ **leader 侧失败回滚截断并发事务已落盘的字节** → 受害事务的挂钩空转、
> 带着"已复制"假象提交（丢数据，raft_17 阶段二抓获 13/240）——修复=leader 失败
> 只回滚 Raft 日志条目、字节留作孤儿重新 propose；follower 冲突截断保留。
> 方法论教训已固化进 raft_17：**并发验收必须先甄别 burst 期间是否有节点崩溃**
> ——崩溃重置 shmem 组表后 INSERT 会跳过挂钩提交成功，症状与让路窗口无法区分。

> **raft_15/raft_10 在 9 节点环境下的偶发失败已修（2026-08-03，harness 时序）**：
> 根因都是断言窗口按 4 节点标定——控制面语义本就是"多数派提交 + 全员**最终**
> apply"（§13.2），9 节点 group0（多数派 5/9、8 对端心跳/退避竞争 tick）下单个
> 节点的 apply 滞后窗口显著变大。修法不放宽断言本体：raft_15 改为**按节点带
> 20s 有界重试**地跑同一断言文件；raft_10 的决议等待窗口 4s → 30s，并且
> harness 不再吞 SQL 错误输出（失败时保留尾行便于诊断）。
> 修后 9 节点全量 **52/52 全绿**（raft_01–21）。
> raft_04（拓扑监控）在 §9.2 那一轮偶发失败过一次，同构建重跑即过，仍是既知的时序 flake。

仍缺的场景：

- ⚠️→✅ ~~follower 掉线且落后超 ring 容量后追平~~ **→ 2026-08-05 由 raft_24 C 段覆盖**
  （走环外条目回读，不依赖快照）。**仍缺的是快照本身**：一旦开始压缩日志，
  被删掉那一段就只能靠 InstallSnapshot 传输 —— 届时要补"日志已压缩、成员落后到
  压缩点之前"的用例。
- ❌ prepare / commit 在 quorum ACK 前不能向客户端返回成功（依赖阶段 3 的 2PC 决议）。
- ❌ **一个节点同时是 A 分区 leader、B 分区 follower 的混合角色场景**——数据组用例的组
  成员目前都是全体 worker，没有覆盖“副本集是全体节点真子集”的真实拓扑，
  而这正是 §11.5.2 #3 多数派算错的暴露条件。
- ⚠️→✅ 全新库 `CREATE EXTENSION` 冒烟：2026-07-24 记为"已入常规验收流程"，
  **但实际上 `run-raft-tests.sh` 里从来没有这一项**（2026-08-03 核实）。
  它恰恰是唯一能抓到边界函数签名不一致的检查——本次改 `partwal_read_record`/
  `partwal_follower_append` 签名时连踩两次静默失败，全靠它暴露。
  现已真正固化为 **raft_19 的 A 段**。
- ✅ ~~raft_13 夹具重做~~ 已由 raft_14 以真实"一主多从"分片放置补齐（2026-07-24）；
  raft_13 保留原 reference 夹具作运输层回归。

## 7. 文件清理策略

保留并更新（路径以 shardpg-3.0 现状为准，旧的 `pg-citus-cluster/` 根目录布局已废弃）：

- `pg-raft-src/README-RAFT.md`
- `pg-raft-src/setup-raft.sh`、`pg-raft-src/run-raft-tests.sh`
- `pg-raft-src/docs/raft_module_revision_plan.md`（本文档）
- `pg-raft-src/docs/pg_partdist_sync_change_log.md`（**对 pg-partdist-src 的每一次改动都必须
  在此追加记录**，它是 raft 侧与 pg_partdist 侧之间的交接台账）
- `pg-raft-src/docs/INTEGRATION-citus-shard-parwal.md`
- `pg-raft-src/docs/interfaces/partwalmgr_raft.idl`
- `pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md`（P3 的设计依据；~~两份分叉副本~~
  **已于 shardpg-replay 合并为 v3.1 单一版本，`shardpg-4.0` 继承，§11.5.2 #5 关闭**）
- `pg-partdist-src/docs/DTX_2PC_DESIGN.md`（**新增 2026-08-03**：跨分区事务 2PC 的
  设计依据，取代本文 §4 阶段 3 的控制面决议方案）

归档或删除候选：

- 旧 pgraft spike 文档和脚本。
- 不再作为入口的 failover demo、组会 demo、raft10 demo 或备份脚本。
- 仍在描述“只做协调节点本地 apply”的旧 MVP 说明。

明确不清理：

- `pg-cluster-data/`
- `pg-install/`
- `.so` / `.o` 等构建产物，除非明确要求工作区整理
- 本地 PostgreSQL 运行时状态文件

## 8. 风险与对策

| 风险 | 对策 | 状态 |
|------|------|------|
| 纯 C Raft 仍有 demo 级实现痕迹 | 先补齐最小安全 Raft 子集，再扩大数据面 | ✅ 已缓解（31/31 基线；环满假 ack、apply 滑窗停摆等历史隐患已在 §13.4 修复） |
| 只改 `partition_map` 会提升陈旧副本 | 有数据组的分区：选举限制 + `primary_term` 任期栅栏（§13）；遗留分区：绑定 `switch_partition_lsn` 候选过滤 | ✅ 已落地 |
| 切主结果不达路由层（双真相源） | `OP_PARTITION_PRIMARY` apply 在每节点同步本地 `pg_dist_placement`（单放置守卫+子事务隔离） | ✅ 已落地（P3 前为"机制先行"） |
| PartWAL 复制链路未打通 | 由分区级 Raft 组承担运输，复用 `partwal_sync` 与 `applied_part_lsn` | ✅ 已落地 |
| 2PC 决议丢失会导致部分提交 | ~~通过控制面 Raft 复制~~ **改为写入协调组（写集内 `hash(dtxid)` 选出的分区组）的日志并等 quorum**；查无决议即推定中止（`DTX_2PC_DESIGN.md`） | ✅ 2026-08-04 端到端落地，回归 raft_20/21/22 |
| Citus 自带 2PC 恢复与协调组决议打架 ⇒ 分叉提交 | `citus.recover_2pc_interval = -1`，改由 `partdist.dtx_recover_prepared()` 统一收尾：有协调组的按决议、快路径的退回 Citus 原生规则（`DTX_2PC_DESIGN.md` §9.4） | ✅ 已落地，raft_22 A 段前置断言 |
| **TopologyMonitor 不处理 SIGHUP ⇒ 全部 pg_raft.\* GUC 对 reload 静默无效** | BGWorker 默认不接 SIGHUP；补 `pqsignal(SIGHUP, SignalHandlerForConfigReload)` + 循环里 `ProcessConfigFile`。此前 `raft_enabled`/心跳/选举超时改完 reload 只对新 backend 生效、守护进程要重启才认 —— 直到 raft_21 H（依赖 reload 降 dtx 守护超时）在负载下超时才暴露 | ✅ 2026-08-04 修复 |
| **压缩之后重启会丢日志尾巴 ⇒ 覆盖已提交条目（2026-08-05 落地压缩时发现并修掉）** | 日志恢复原先按"`last_log_index > 0` 就说明已经有日志了"早退。有了压缩之后，重启时 hardstate 会先把 `last_log_index` 顶到基点（基点之前的条目已被快照取代，游标不能落在它之下），于是恢复被**整段跳过**，表里 `base+1..N` 的尾巴永远灌不回环。对 follower 只是要 leader 重发；对**重启后重新当选的 leader** 就是灾难——它以为日志止于基点，会拿新内容覆盖已提交的 `base+1..`，直接破坏 Leader Completeness。修复=改用每组一个 `log_restored` 标志判"是否已灌过"，并在灌完后把三个游标夹到基点之上。回归 raft_25 E 段（**in-build 对照**：按旧写法重启后新条目会落在已存在的 index 上） | ✅ 已修 |
| **apply 抛错 ⇒ `apply_in_progress` 永久为真 ⇒ 该节点 apply 游标静默冻结** | 旗在 `apply_one_entry` **之前**置上、之后才清，中间任何 ereport 都会让它永远留在 true，此后每次 `group_apply_pending` 都在"另一个 backend 正在 apply"分支立即返回。实测：一条 payload 违反 `node_map_status_check` 的条目让**九个节点全部**卡在同一 index（`commit_index` 照常前进，无任何告警）；有了压缩之后更糟——基点也跟着不动，既追不上也压不了。修复=把 apply 包进子事务（`apply_one_entry_guarded`），控制面按既有"跳过安全"语义跳过、数据面保留游标下轮重试，**旗一定清掉** | ✅ 2026-08-05 修复 |
| **demux 异步落盘 ⇒ 段文件清零后又长出一条（2026-08-05 raft_28 实测；用例已随 §0.4 回滚，但这条教训对任何操作段文件的夹具都成立）** | 夹具里 `INSERT` 提交之后 demux 未必已经把记录写进段文件；此刻 `partwal_truncate_to(oid,0)` 清零，随后那条才落盘 —— 段文件凭空多出一条，判据报【21 条，应为 20】。**清零之后必须复查、连续两次读到 0 才算干净**（`truncate_stable`）。与 OID 复用继承上一轮记录是同一类坑的两个来源：一个是上一轮的残留，一个是本轮还没写完 | ✅ 已在 raft_27/28 夹具修正 |
| **只按【组】判要不要外部化 ⇒ 非 OP_PARWAL 条目静默丢失（2026-08-05 raft_12 判出；⏪ 缺陷与修复都在 E3 里，已随 §0.4 回滚）** | E3 首版对**所有**数据组条目停写 `raft_log`，但只给 `OP_PARWAL` 记 run。数据组还有公开路径 `pg_raft_group_propose(group_id, op_type, payload)` 可提任意 op_type 的条目（raft_12 用的就是 `OP_TEST`），这类条目**在段文件里没有第二份** —— 于是既不在 `raft_log` 也不在 run 里，重启后重建不出来、末端被下调，条目静默丢失。判据本身没有过时：对 `OP_TEST` 条目【行数 == last_log_index】本就该成立，是【OP_PARWAL 的行是冗余的】被错误推广成了【数据组的行都是冗余的】。修复=按 op_type 分流：只有 `OP_PARWAL` 外部化，其余照旧写 `raft_log`；读路径先按 run 重建、取不到再回退 `raft_log` | ✅ 2026-08-05 修 |
| **follower 落盘失败时末端虚高 ⇒ 可能凭一条并不持有的条目赢下选举（2026-08-05 raft_26 D 段实测抓获；⏪ 该缺陷与它的修复都在 E2/E3 里，已随 §0.4 一并回滚 —— 但 `AppendPartWALRecordAt` 遇空洞是 ereport(ERROR) 而非返回 false 这个事实不变，凡是用它落盘的新代码都要防这一手）** | `data_entry_store()` 里那条 SPI_execute 调的是 `partwal_follower_append`，而 `AppendPartWALRecordAt` 遇到 plsn 空洞是 **ereport(ERROR)** 而非返回 false —— 它直接 longjmp 出整个 RPC 函数，调用点 `if (!data_entry_store(...))` 里的收尾一行都执行不到。于是 follower 的环与 `last_log_index` 已经推进、字节却没落盘，末端反而比 leader 还多一条（实测 follower 28 / leader 27）。危害不在复制（leader 会重发），而在**选举**：日志新旧比较看的就是 `last_log_index`。修复=把落盘包进子事务（`data_entry_store_guarded`）使错误可捕获，捕获后把刚 append 的那一条从环与 run 里撤掉 | ✅ 2026-08-05 修 |
| **后台追平通道会悄悄填掉用例要测的缺口 ⇒ 假测试（2026-08-05 raft_27 实测；用例已随 §0.4 回滚，教训对任何测「落后多少」的用例都成立）** | TopologyMonitor 每 `catchup_interval_ms` 自触发一轮追平，成员一起来就被补上几十条；等用例测缺口时已从 140 缩到 99，低于环容量 128，于是【只能靠环外重建】的前提不再成立 —— 把重建摘掉也照样通过。**凡是要测【落后多少】的用例，夹具必须先 `catchup_interval_ms=0` 关掉后台通道**，让追平只能由显式调用完成，归因才干净 | ✅ 已在 raft_27 夹具修正 |
| **按 payload 文本判日志冲突 ⇒ 误截断（2026-08-05 自造并当场修掉）** | `raft_log.payload` 是 **jsonb**，读回来的文本被规范化过（键序、冒号后空格），与 leader 线上发来的原始 JSON 逐字节不等。环外回读一落地，"同 index 但文本不同即冲突"的写法就把**每一条**环外条目判成冲突并截断——实测把控制面日志从 347 条削到 35 条。改回 Raft 原始规则：**冲突只看 term**（同 index 同 term 必出自同一 leader）。同理，重启后 restore 灌回环里的也是规范化文本，环内比文本一样不可靠 | ✅ 已修 |
| **group-commit 让路窗口使 prepare 的多数派保证失效** | per-backend 触达集合 + 提前返回路径也触发复制挂钩；prepare 语义改为"等 `commit_index` 覆盖本事务记录"（`DTX_2PC_DESIGN.md` §9.1） | ✅ 已修，回归 raft_17（确定性让路构造） |
| 每分区 Raft group 实现成本高 | 分期落地 P0→P3，控制面保持为组 0 不回退 | ✅ P0–P2 完成 |
| **一节点托管多分区副本 ⇒ 不同 leader 的 xid 在本地 clog 相撞** | **必须先做集群级 xid 区间租约**；在此之前多分区共存于一节点的 redo 配置不可上线 | ❌ **最硬阻断项**，见 §11.5.2 #1 |
| **分区数超过 `RAFT_MAX_GROUPS=32` / ring 撑爆 shmem** | 数据组日志外部化到 parwal 段文件，shmem 只留游标 | ❌ 未做 |
| **成员集不随 RPC 传播 ⇒ 多数派按全体 peers 算错** | 空成员集语义改为"未知"并 fail-stop；成员集从控制面已下发的 `partition_map` 本地导出（`DTX_2PC_DESIGN.md` §9.2） | ✅ 已修，回归 raft_18 |
| 数据组数量挤占控制面心跳 | 连接复用 + 对端级退避（已做）；报文级心跳合并 + 最小堆调度（待做） | ⚠️ 部分缓解 |
| 边界函数签名变更在回归中静默失效 | 改签名同步改 `COMMENT`/`setup-raft.sh`/已安装副本；补全新库 `CREATE EXTENSION` 冒烟 | ⚠️ 已踩过一次 |
| 两份 `FOLLOWER_REPLAY_DESIGN.md` 分叉导致 P3 依据不一致 | P3 开工前先合并 | ✅ 已合并为 v3.1（shardpg-replay → 4.0 继承） |

## 9. 近期任务清单

### 已完成

1. 编译同步后的 `pg_partdist`，修正 `PG_CONFIG` 路径和新版依赖问题。
2. 审查并修正 `pg_raft` 与新版 `pg_partdist` 的编译/链接兼容性。
3. 为 Raft 增加读取本地 `switch_partition_lsn` 和 follower `applied_part_lsn` 的最小接口。
4. 在 `pg_raft_failover_partitions_for_node()` 中加入“未追平副本不能晋升”的候选过滤。
5. 在 `OP_PARTITION_PRIMARY` apply 后接入 `partwal_notify_primary_switch` 边界函数。
6. 修复 `raft_04_topology_monitor.sql` 触发的 leader backend 崩溃：
   - hardstate 持久化不再发生在 PostgreSQL spinlock 内。
   - `OP_PARTITION_PRIMARY` apply 不再在同一 SPI 连接里嵌套执行更新。
   - 跨 `SPI_finish()` 使用的分区、secondary、LSN 字符串已复制到调用者内存上下文，避免分区号变成 0 或 old_primary 读到垃圾值。
7. 已完成最小回归：`raft_01_leader_election.sql`、`raft_02_failover_partition.sql`、`raft_03_split_brain_guard.sql`、`raft_04_topology_monitor.sql` 均通过。
8. 已补齐“少于多数派不能提交”的控制面语义：
   - `pg_raft_consensus_propose()` 在未拿到多数派时会回滚本地未提交尾日志，不再让未提交提议残留并在之后被偷偷提交。
   - `raft_05_no_majority_reject_commit.sql` 已验证无多数派时 propose 失败，且 `node_map` 不会出现脏 apply。
9. 已补齐“日志落后候选者不能赢得投票”的回归：
   - `raft_06_stale_requestvote_rejected.sql` 已验证带陈旧 `last_log_index/last_log_term` 的 RequestVote 被拒绝。
   - 同时清理测试中注入的临时 `node_id=98`，避免后台 TopologyMonitor 对幽灵节点持续探测。
10. 已补齐“未追平 PartWAL 副本不能晋升”的回归：
   - `raft_07_uncaught_up_secondary_not_promoted.sql` 先写出真实 `switch_partition_lsn`，再验证 `applied_part_lsn` 未追平的 secondary 不会被自动提升。
   - `run-tests.sh` 已增加对应场景，并在 Raft 段结束后清理临时节点 `77/98/99` 与测试分区，避免和后续 DDL 回归互相干扰。
11. 已补齐“leader 宕机后重新选举、旧 leader 恢复后降级”为自动回归：
   - `raft_08_old_leader_rejoins_as_follower.sql` 已验证旧 leader 重启后只能以 follower 身份回归，且本地 propose 会被 `only leader` 拒绝。
   - `run-tests.sh` 已增加自动停旧 leader、等待新 leader、恢复旧 leader 的编排，并补充入口自恢复：缺失的 PostgreSQL 节点会自动拉起；缺失的 `pg_raft` 扩展或未收敛的 leader 会自动执行 `setup-raft.sh` 恢复。
12. ~~当前全量验收通过：`bash /home/pxr/pg-citus-cluster/run-tests.sh`，71 通过、0 失败。~~
   **该条为 2026-07-12 前的历史环境记录，已作废。** 当前基线以 §6 为准
   （2026-07-24：raft_01–15 共 31/31 + P0 10/10 + 全新库 CREATE EXTENSION 冒烟）。

### 下一步（2026-07-24 整理，历史明细见 git 历史与 §10/§11）

历史"下一步"里可完成的项均已完成并入上表/各节（HardState 崩溃恢复回归、切换点取数、
旧 leader 追平回归、启停噪声、P0/P1/P2、运输层加固、切主重构）。当前真实待办只有两条线：

1. **P3 前置序列**——按 §12.4 的顺序执行：合并 FRD → 集群级 xid 区间租约 →
   数据组日志外部化(解 `RAFT_MAX_GROUPS`/ring/压缩) → 成员集经控制面下发
   (`OP_CONFIG_CHANGE`，完成后补"副本集真子集"与"混合角色"回归) →
   propose 接入写入路径 + 后台追平通道 → P3 redo 本体。
   其中"propose 接入写入路径"的目标形态即 §4 阶段 3 的 **prepare 四步设计**
   （2026-08-03 起四步全部自动挂接，回归 raft_16/17）。
2. ~~**2PC 决议**~~ **✅ 2026-08-04 端到端完成**（`DTX_2PC_DESIGN.md` §10 第 0–5 步，
   回归 raft_17–22）。剩该文第 6 步：**升主 in-doubt 清理**（§9.6）与
   **快路径分叉归队规则**（§9.5），两者都必须与惰性回放的 promotion 路径合流才谈得上验收，
   因此并入上面第 1 条的 P3 序列。

⚠️ 历史教训存档：2026-07-18 曾写"至此物理回放前提具备"，2026-07-20 审查推翻——
运输层前提具备，但 xid/clog、组容量、成员集等架构级前提未满足，P3 不能直接开工，
判定详见 §12。

## 10. shardpg-3.0 集成状态与审查差距(2026-07-12)

### 集成状态

pg_raft 已从 raft2.0 分支同步进 `CyanPandas/ShardPG` 的 `shardpg-3.0` 分支(`pg-raft-src/`),
并完成四节点适配(master:5432 + worker1-3:5433-5435,多数派 3/4;本文档前文所述
"3 节点"拓扑与 `/home/pxr/pg-citus-cluster` 路径均为历史环境,以 shardpg-3.0 的
raft4 四节点环境为准):

- 三个 PartWAL 边界函数(`get_partition_flush_lsn` / `get_follower_applied_part_lsn` /
  `partwal_notify_primary_switch`)已在 `pg-partdist-src/src/raft_boundary.c` 正式落地,
  不再依赖独立 adapter 拷贝。
- `pg_raft_format_conninfo()` 支持 worker3:5435 与 master 别名,并改为优先使用
  `node_map` 中的端口。
- `setup-raft.sh` / `run-raft-tests.sh` / raft_02 / raft_04 / raft_07 已按四节点改写。
- 四节点回归基线:raft_01–raft_08 全部通过(23 项断言);数据面无负载回归 10/10 通过。
- 近期任务清单"下一步"第 1 项已完成:`raft_09_hardstate_crash_recovery.sql` 验证
  follower immediate 崩溃重启后 term 不回退、已提交日志不丢、不自立为 leader、
  复制链路恢复可追平新决议;harness 同时校验 `pg_raft_hardstate` 文件在盘。
  第 5 项(启停 FATAL 噪声)已通过 node_start 前置 pg_isready 探测解决。
- 2026-07-15 增量:"下一步"第 3 项的切换点取数/候选选择部分与第 4 项完成——
  failover 切换点优先远程读旧 primary 的真实 flush 进度(不可达回退 leader 本地,
  无进度源记 WARNING);候选人在追平者中选 `applied_part_lsn` 最大者。新增回归
  `raft_10_most_caught_up_secondary_promoted.sql`(最追平副本被提升 + 决议 payload
  的 switch_partition_lsn 等于真实写入路径 flush 进度)与
  `raft_11_old_leader_log_catchup.sql`(旧 leader 停机期间的已提交决议在回归后
  追平并 apply)。四节点回归基线更新为 raft_01–raft_11 共 27 项断言全过。
  同时:根目录 RAFT2_HANDOFF.md / raft_module_revision_plan.md /
  pg_partdist_sync_change_log.md 移入 `docs/`;
  `docs/INTEGRATION-citus-shard-parwal.md` 按 shardpg-3.0 现状重写。

### 审查差距(留待后续,按优先级)

> **2026-07-20 复核**：#3 已由 P0 关闭；#1 已由 P2 + 运输层加固**部分**关闭
> （字节现在真的会跨节点流动并在多数派 fsync，但仍不进堆表）；#2 的结论已被 P2 取代
> （不再需要恢复流式分发）。逐条批注见各条末尾。

1. **主从切换目前只到元数据层**:shardpg-3.0 的 follower 物理回放仍是设计稿
   (`pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md`),`follower_partition_map`
   有表无写入方,跨节点分区日志传输(WalSender/Receiver)不存在。因此
   `OP_PARTITION_PRIMARY` 决议能正确切 `partition_map.primary_node` 并满足
   切主安全线,但数据不会真正流向新 primary;`partwal_notify_primary_switch`
   仍为日志占位。对应本计划阶段 2 → 3 的过渡,依赖数据面 follower replay 落地。

   → **2026-07-20 更新（部分关闭）**：跨节点分区日志传输**已经存在**了 —— 不是
   WalSender/Receiver，而是分区级 Raft 组的 AppendEntries（P2 + 运输层加固）。
   `follower_partition_map` 也有了真实写入方。**仍未关闭的部分**：字节只落到副本的
   `pg_parwal/`，不进堆表（P3），且 `partwal_notify_primary_switch` 仍是占位。

2. **方案文档与 3.0 实现的差异,以代码为准**:方案文档描述的是
   "Demux Worker 流式解复用 + WalSender DATA/SKIP + PartWALReceiver/PartWALApply"
   流水线;3.0 实际是 parwal-2.0 同步写路径(`wal_insert_hook` 捕获 +
   PRE_COMMIT 同步落盘 `pg_parwal/<oid>/`,Demux 退化为一次性崩溃恢复,
   无流复制、无 SKIP 记录)。Raft 依赖的抓手(`partition_lsn` 单调序号、
   段文件、`GetLastWrittenPartitionLSN`)在两种路径下语义一致,不影响控制面;
   但阶段 3 的"日志分发/接收"设计需按同步写路径重新对齐,SKIP 机制仅在
   恢复流式分发时才需要。

   → **2026-07-20 更新（已被 P2 取代）**：日志分发不再走"重新对齐流式管道"这条路，
   而是由分区 Raft 组的 AppendEntries 承担；SKIP 记录**不需要**了 —— 组成员集本身就
   限定了哪些节点该收哪个分区，无需在流里标记跳过。

3. **partition_id 的全局身份问题**:3.0 的 `pg_parwal/<partition_id>` 目录名
   使用分片表在本节点的 OID,而 OID 是每节点独立分配的;副本机制落地后,
   同一逻辑分片在不同节点上 OID 不同,Raft 元数据(`partition_map.partition_id`)
   需要一个全局一致的分片标识与各节点本地 OID 的映射
   (`follower_partition_map.local_relname` 已为此预留)。切主决议与进度比对
   必须基于全局标识,否则跨节点 `applied_part_lsn` 比较无意义。

   → **✅ 2026-07-17 已关闭**：即 P0，`partdist.shard_identity` 以 Citus `shardid` 为
   `global_shard_id`，`local_partition_for_shard()` 负责解析到各节点本地 OID。
   遗留：`FOLLOWER_REPLAY_DESIGN.md` 仍以本地 OID 为 shard 主键，未与之对齐
   （§11.5.2 #6）。

## 11. 分区级 Raft 组落地方案(2026-07-17)

本节把阶段 4 从"方向性描述"细化为可执行的工程分期,作为后续在 `shardpg-3.0`
临时环境推进"一个分区 = 一个 Raft 组"的对照文档。所有结论基于 `9ebc091` 的代码盘点。

### 11.1 定位:运输定序层 vs 回放应用层

必须先纠正一个常见误解:**分区 Raft 组不是"垫在物理回放下面的一小步",而是物理回放
的运输+定序层;物理回放(redo apply)是骑在它上面、消费其已提交条目的应用层。** 二者
互补,做完前者不会自动得到后者。好处是二者可**解耦验证**:先用"平凡 apply"(follower
只落盘段文件 + 把 `applied_part_lsn` 当字节游标推进)跑通 Raft 组的复制/定序/多数派提交,
再把平凡 apply 换成真正的 `rm_redo`。

### 11.2 现状盘点:Raft 侧全是"每节点单例"

分区组的核心改造 = 把下列单例变成"按组 id 索引的集合":

| 当前(单例) | 分区组需要 | 代码位置 |
|------|------|------|
| 一个 `RaftConsensusShmem`(单 state/term/leader) | 每组一份 | `raft_consensus.c` 结构体 |
| 一个 `RaftLogShmem` + `ring[128]` + `peer_*[16]` | 每组一份日志与复制游标 | 同上 |
| 一个 `pg_raft_hardstate` 文件 | 每组一份 term/voted_for/commit | `RaftHardStateFile` |
| 一张 `raft_log`(`log_index` 全局 UNIQUE) | 按组分命名空间 | `pg_raft--1.0.sql` |
| 一个 BGW tick / 一个 `election_deadline` | N 组各自的选举 + 心跳 | `pg_raft_consensus_tick` |
| `RAFT_PAYLOAD_MAX = 768`(为 JSON 元数据设计) | 数据条目是任意长 WAL 字节流,装不下 | `#define` |
| `RAFT_LOG_CAPACITY = 128` ring,无截断/无 snapshot install | 多组下被放大,需外部化存储 | 同上 |

### 11.3 有利基础(为什么可行)

- **数据面本就按分区分片**:`pg_parwal/<partition_id>/` 每分区一套段文件、
  `PartitionWALWriter` 每分区一个写入器、`partition_lsn` 为每分区单调序号
  (设计上即"该 shard 的 Raft log index")。**每个分区组的日志在 leader 上已物理存在。**
- **记录格式已是自洽 entry**:`PartWALRecHeader` + 原始 `XLogRecord` 字节,本就为当
  Raft entry 复制而设计。
- **算法零件齐全且已验证**:选举(带日志新旧检查)、AppendEntries(`nextIndex/matchIndex`
  + `prev_log` 一致性)、多数派 `commit_index`、HardState 落盘、更高 term 降级,均已 27/27
  过。分区组 = 把同一套算法实例化 N 份。
- **RPC 是无状态 libpq 调用**:`pg_raft_rpc` / `pg_raft_append_entries` 加一个 group 参数
  是机械改动。

### 11.4 目标架构:两层,不合并

- **控制面 Raft(保留现状,group 0)**:管拓扑、成员集(`partition_map` 的
  `primary_node/secondary_nodes[]`)、配置/成员变更、故障感知。**不要把控制面本身拆成
  per-partition。**
- **数据面 per-partition Raft 组**:成员 = 该分片的副本所在节点集,配置由控制面下发;
  日志后端 = **parwal 段文件**(不进 shmem ring),shmem 只保存每组的
  index/commit_index/`peer_next/match_index` 等游标与易失状态。
- **边界**:分区组的成员/主变更结果仍回写 `partition_map` 并复用现有的切主安全线
  (`switch_partition_lsn` + `applied_part_lsn` 过滤)。

### 11.5 工程分期

**P0 — 全局分片身份映射(唯一"现在就能做且不碰回放"的基础工作,必须最先落地)** ✅ 已完成 2026-07-17
- 建立 `global_shard_id ↔ (node_id, local_oid, relfilenode)` 映射;填充
  `follower_partition_map.local_relname` / 一张分片注册表。
- 落地(pg_partdist 扩展 `sql/pg_partdist--1.0.sql`):**global_shard_id 直接采用
  Citus `shardid`**(分片表命名 `<rel>_<shardid>`,`pg_dist_shard` 各节点同步,是天然
  跨节点一致键)。新增 `partdist.shard_identity(global_shard_id PK, local_oid,
  relfilenode, local_relname, logical_relid)`、`follower_partition_map.global_shard_id`
  列,及 5 个函数:`shard_global_id(oid)`、`rebuild_shard_identity()`(幂等,自带剪枝)、
  `register_shard_identity(oid)`、`local_partition_for_shard(bigint)`、
  `global_id_for_partition(oid)`。用 Citus `shard_name()` 反查避免解析表名后缀;
  扫描 `pg_class` 处置 `citus.override_table_visibility=false`(否则分片被 Citus 隐藏)。
- 回归 `pg-partdist-src/tests/test_shard_identity_p0.sh`:覆盖(各 worker 注册数==本地分片数)、
  往返(`shard_global_id`↔`local_partition_for_shard` 互逆)、跨节点(reference 表同一
  shardid 在三 worker 有各异本地 OID、协调节点 NULL)、剪枝,共 10/10;raft_01–11 仍 27/27。
- 理由:一个分区组的成员是"同一逻辑分片在不同节点上的副本",而各节点用**独立本地 OID**
  命名它(`pg_parwal/<OID>`);组成员、AppendEntries 寻址、跨节点 `applied_part_lsn` 比较
  全依赖它。**这也是当前控制面 failover 安全线跨节点比进度的隐含前提。**
- 验收:同一逻辑分片在四节点上可由全局 id 唯一定位;`applied_part_lsn` 跨节点可比;
  现有 raft_01–11 全绿(仅新增映射,不改控制面语义)。

**P1 — Raft 核心"组化"重构** ✅ 已完成 2026-07-18
- 单例结构 → 按 `group_id` 索引的集合：`RaftGroupTable`(定长 `RAFT_MAX_GROUPS=32` 槽位)
  持有每组一份 `RaftConsensusShmem`(state/term/voted_for/leader_id/election_deadline)与
  `RaftLogShmem`(ring/commit_index/`peer_next_index`/`peer_match_index`)；所有内部函数
  改为显式接收 `RaftGroupCtx`，不留任何隐式"当前组"全局量。
- **group 0 = 控制面**，行为与组化前逐字节一致：HardState 仍写 `$PGDATA/pg_raft_hardstate`
  (数据组写 `pg_raft_hardstate.<gid>`)，既有 SQL API(`pg_raft_consensus_*`)全部落到 group 0。
- 日志按组分命名空间：`partdist.raft_log` 增 `group_id` 列，唯一索引由 `(log_index)` 改为
  `(group_id, log_index)`；新增 `partdist.raft_group` 注册表使数据组重启后可恢复。
- 每组独立成员集(`members[]`，空=全体 peers)，多数派按本组规模算；非成员节点不参与该组
  选举/心跳。**follower 首次从 RV/AE 里听说某组时自动建组**，故只需在一个节点建组即可引导。
- 调度器：单 BGW tick 多路复用全部活跃组(group 0 优先)。§11.6 #2「选举风暴」的两项
  实测必需对策(都是被回归偶发失败逼出来的)：
  1. **按对端复用 libpq 连接**(`peer_conn[]`)。组化前每 tick 只有一组、每对端一次
     `PQconnectdb` 尚可忍受；N 组之后建连次数是 `组数 × 对端数`，握手开销把 tick 周期
     撑爆，进而拖慢控制面心跳，raft_04/raft_11 这类时序敏感用例开始偶发失败。
  2. **对端级 RPC 退避**(`peer_backoff_until[]`)：某组撞到不可达对端后，同一窗口内其余
     组直接跳过该对端，避免 N 组各吃一次 `connect_timeout`。**退避只对心跳/复制生效，
     选举豁免** —— 候选人少收一票就可能选不出 leader，把无谓等待放大成长时间无主。
  报文级心跳合并 / 最小堆调度 / 领导权共置仍是后续优化。
- 新增 SQL：`pg_raft_group_create(gid, members[])` / `pg_raft_group_drop(gid)`(连带删除该组
  HardState 文件，避免 gid 回收时继承旧 term) / `pg_raft_group_status()` / `pg_raft_group_propose()`；
  `pg_raft_append_entries` 与 `pg_raft_rpc` 报文各加一个缺省 0 的 group 参数(向后兼容)。
- 顺带修掉 `pg_raft_shmem_startup` 用整段大小去分配 `pg_raft_leader` 的超额分配。
- 回归 `test/sql/raft_12_multi_group_isolation.sql`：两个数据组在不同节点各自选出 leader
  (组间领导权独立)、日志按组隔离(数据组条目不串入 group 0)、非 leader 提交被拒、
  数据组已提交条目 apply 追平。raft_01–11 全绿。

**已知缺口(留给后续期)**：组成员集只在本节点生效，不随 RPC 传播 —— 自动建组的 follower
成员集为空(按全体 peers 算多数派)；`pg_raft_group_drop` 也只作用于本节点，对端仍可能把
该组重新传播回来。由此派生一条**运维/测试上的硬约束**：**不要复用已用过的 group_id** ——
本节点丢弃后，对端仍记着该组上一轮的 term，用同一 id 重建会得到起始 term 落后于对端记忆
的新组，选举被反复压制(回归里表现为"组选不出 leader / 条目复制不出去"的偶发失败)。
为此提供 `pg_raft_group_reset()`(清空本节点全部数据组：shmem 状态 + HardState 文件 +
注册表 + 日志)，回归用它在每轮开始/结束时归零。成员与组生命周期的正解仍是控制面决议
下发(§11.6 #3)。

**P2 — 数据组复制 + 平凡 apply(到达"物理回放前提"状态)** ✅ 已完成 2026-07-18
- **entry = 描述符 + 随行字节**：Raft entry 本身是一条小 JSON 描述符
  `{"partition_lsn","orig_lsn","rmid","info","xid","nbytes"}`(塞得进 `RAFT_PAYLOAD_MAX`)，
  真实 WAL 字节作为 `bytea` 参数随同一次 AppendEntries 下发。这样**完整复用**了既有
  ring / `prev_log` 一致性检查 / 多数派提交 / `raft_log` 持久化，又不必把任意长字节流塞进
  768 字节 payload(§11.6 #1 的落地形态：字节的权威存储仍是 parwal 段文件，Raft 只搬运)。
- **先落盘再 ack**：follower 收到条目后，先用 `partdist.partwal_follower_append()` 把字节
  原样写入**本节点自己的** `pg_parwal/<local_oid>/` 并 fsync，成功才 ack ——
  故"多数派提交"严格等价于"多数派已持久化"。本节点 local_oid 由 **P0 的
  `local_partition_for_shard(global_shard_id)`** 解析(各节点 OID 不同，这正是 P0 的用处)。
- **平凡 apply**：apply 只把 `follower_partition_map.applied_part_lsn` 推进到该条目的
  `partition_lsn`，**不做 redo**。这是该列**第一个真实的 C 写入方** —— 在此之前它恒为
  占位 0，切主安全线的跨节点进度比较无从谈起。
- pg_partdist 新增三个 parwal 边界函数(`src/raft_boundary.c`)：`partwal_read_record()`
  (leader 按 partition_lsn 读出记录) / `partwal_follower_append()`(follower 原样落盘) /
  `follower_set_applied_part_lsn()`(单调推进进度，缺列信息从 `shard_identity` 补齐)。
  pg_raft 新增 `pg_raft_data_propose(group_id, partition_lsn)`。
- 取字节要 SPI，而 BGW tick 没有 SPI：数据条目只在 client backend 路径(propose /
  `flush_replication`)下发，tick 对数据组只发心跳；落后的 follower 在下一次 propose 时追平。
- 回归 `test/sql/raft_13_data_group_replication.sql`(以 Citus reference 表分片为组，
  三 worker 天然是成员集)：多数派提交、**字节逐字节落到 follower 自己的 pg_parwal**
  (md5+长度与 leader 一致，实测 1329 字节记录完全相同)、`applied_part_lsn` 真实推进到 1、
  失去多数派(3 成员停 2)时 `data_propose` 必须返回 0 且进度不推进。

**运输骨架已具备，但"物理回放前提"尚未完全满足**（2026-07-20 审查修正；下列 5 项已在
同日的"运输层加固"中全部落地，见 §11.5.1）：分区级 Raft 组
确实能把真实 parwal 字节复制到多数派、按 log_index 定序、fsync 后才 ack，进度列也由真实
写入方推进。但下列 5 项**都在运输层**，目前被"平凡 apply 是幂等且单调的"这一性质掩盖，
一旦换成 redo 就会暴露。**P3 开工前必须先修**：

1. **follower 未采用 leader 的 partition_lsn**（最关键）。`data_entry_store()` 调用
   `partwal_follower_append()` 时不传描述符里的 `partition_lsn`，follower 侧
   `AppendPartWALRecord` 用的是本地自增计数器；而 `data_entry_apply()` 写进
   `applied_part_lsn` 的却是 **leader 的** plsn。两者只在"双方目录都从空开始且全程同步"
   时才巧合相等（raft_13 正是这种情形，plsn=1）。本节点 demux worker 的任何本地写入、
   任何重传或孤儿条目都会让两个编号空间永久错位，届时 `applied_part_lsn` 指向的记录
   在本地并不存在。需让 `partwal_follower_append` 接受并强制 `expected == local_last + 1`。
2. **follower append 不幂等**。`handle_append_entries` 的"条目已存在"分支会继续落到
   `data_entry_store()`，leader 因 `peer_next_index` 回退而重传时会写入第二份物理副本。
   redo 下即双重回放。
3. **`last_applied` 未持久化**，重启时 `restore_persistent_log_if_needed()` 直接
   `last_applied = commit_index`。崩溃时"已提交未 apply"的条目重启后被**跳过**而非重放，
   redo 下就是堆表永久分叉且无告警。需持久化，或从 `applied_part_lsn` 反推。
4. **apply 与游标推进不原子**。`last_applied = idx` 在锁内先行、`apply_one_entry()` 在锁外
   后做，apply 失败会被吞掉且不重试，并发 caller 还可能乱序执行 apply 体。
5. **截断不触达 parwal**。多数派不足时 `discard_uncommitted_entry()` 只回滚 leader 的 ring
   和 SQL 行，已 ack 的 follower 上那份字节仍留在盘上，本地 LSN 空间被永久占用。

次要项：fsync 失败目前只是 WARNING 仍会 ack（应升为 ERROR）；>256KB 记录走直写分支会绕过
flush/checkpoint；描述符用 `strstr/sscanf` 解析，畸形时静默取 0。

另需注意：`pg_raft_data_propose()` 目前**只有测试在调用**，尚未接入 demux/写入路径；且数据
条目只在 propose 路径下发，没有后台追平通道 —— 无 propose 流量时落后 follower 不会自行收敛。

#### 11.5.1 运输层加固(2026-07-20 完成，回归 29/29)

上面 5 项 + fsync 语义已全部落地。**架构前提(务必先读)**：集群由多个工作节点构成，
数据划分为若干逻辑分区并分布部署；**每个节点同时是部分分区的 primary、又是另一些分区的
secondary**。因此没有全局"主节点"，数据面领导权是**按分区**的；只有控制面(组 0)有唯一
leader。下面每一条都是这个前提逼出来的。

1. **follower 按 leader 指定的 partition_lsn 落盘**。`partwal_follower_append` 增加
   `p_partition_lsn` 参数，写入器新增 `AppendPartWALRecordAt()`：
   `expected == last+1` 正常写；`expected <= last` **幂等 no-op**(重传去重)；
   `expected > last+1` **ERROR**(不留空洞，让 leader 回退补齐)。
   原先 follower 用本地自增计数器，而 `applied_part_lsn` 存的是 leader 的编号，
   两个编号空间只在"双方目录都从空开始"时巧合相等。
2. **重传幂等**：leader 因 `peer_next_index` 回退而重发时不再写入第二份物理副本。
3. **`last_applied` 随 hardstate 持久化**(文件 v2，兼容读 v1)；重启不再无条件
   `last_applied = commit_index`。此前崩溃时"已提交未 apply"的条目会被**跳过**，
   redo 下即堆表静默分叉。
4. **apply 与游标推进原子化**：`apply_in_progress` 串行化，游标只在 apply 成功后推进，
   失败下轮重试。**控制面刻意保持旧语义(失败也推进)** —— 控制面写的是幂等元数据表，
   一条永久失败的 payload 若卡住游标会让整个 failover 通道停摆，代价远大于漏一条；
   数据面相反，漏一条 redo 就是分叉，所以必须重试。
5. **截断触达 parwal**：新增 `TruncatePartWALTo()` / `partwal_truncate_to()`，
   Raft 日志截断(多数派不足丢弃、AppendEntries 冲突)时同步截断字节。
   必要性：切主后新 leader 会把**不同的**记录写到同一个 partition_lsn 上，旧字节若滞留，
   第 1 条的幂等去重反而会保留错误内容。
6. **fsync 失败由 WARNING 升为 ERROR**；并修复超过缓冲区(256KB)的记录走直写分支时
   `FlushPartitionWALWriter` 因 `buf_used == 0` 提前返回、导致既不 fsync 也不更新
   checkpoint(陈旧 checkpoint 会重新发放同一个 partition_lsn)的问题。

**加固过程中暴露的夹具错误(重要)**：raft_13 原先用 Citus **reference 表**做夹具。
reference 表在**每个**节点都是本地主写，follower 的 `pg_parwal/<oid>/` 里已有它自己
demux 产出的记录，与"leader 的 plsn=1"直接相撞。旧代码"通过"是假阳性——follower 把
leader 的字节追加到**另一个编号**上，再从那个编号读回来比 md5，测的是"我刚写的字节等于
我刚写的字节"。真实架构下不会有此冲突(对某分区要么 primary 要么 secondary，secondary
不本地产出该分区 WAL)，故夹具改为先 `partwal_truncate_to(oid, 0)` 清空 follower 的本地
记录以模拟"纯 secondary"。**结论：reference 表不适合做数据组夹具**，后续应改用真正的
"一主多从"分片放置。

#### 11.5.2 P3 之前仍未解决的架构级阻断项

以下**不属于运输层**，但同样是物理回放的前提，且都与"一节点多角色"直接冲突：

1. **xid 与 clog 冲突(最硬)**。见 `FOLLOWER_REPLAY_DESIGN.md` §9：重放后 tuple 的
   `xmin/xmax` 是 **leader 的 xid**，而 clog 是**节点全局**共享的。各分区 leader 独立
   分配 xid ⇒ 一个节点托管来自**不同 leader** 的多个分区副本时，相同 xid 会在本地 clog
   相撞(A 组已提交、B 组同值已回滚，只能记一个)。FRD 明确写着"方案 A(集群统一 xid 区间
   批发)落地前，多 shard 共存于一个 follower 的配置**不可上线**"——**而这正是本架构的
   默认形态**。必须先做集群级 xid 区间租约。
2. **`RAFT_MAX_GROUPS = 32`**：每节点最多 31 个数据组，与"每节点托管几十上百分区"直接
   冲突。单纯调大会让 shmem 随组数线性膨胀(每组一个 128 条 × 768B 的 ring)，正确解法是
   "数据组日志外部化到 parwal 段文件，shmem 只留游标" —— **设计已于 2026-08-05 定稿，
   见 §11.10**（段式边界表 `raft_log_runs` 提供 term 与 index↔plsn 对应，载荷从
   `PartWALRecord` 头部重建；分 E1–E4 四期落地）。⏪ **该设计目前一件都未落地**：
   E1–E3 曾实现又于 2026-08-07 整条回滚（§0.4），E4 从未开工。
3. **组成员集不随 RPC 传播**：自动建组的 follower 成员集为空，按**全体 peers** 算多数派。
   分区副本集是全体节点的**子集**，所以这在真实拓扑下算出的多数派是错的。成员集必须来自
   控制面下发的 `partition_map`，而不是"从收到的报文里听说"。
4. **单 BGW tick 多路复用全部组**：数据组数量会挤占控制面心跳(实测已导致 raft_04/11
   偶发失败，当前靠连接复用 + 退避缓解)。组数上去后需要报文级心跳合并 + 最小堆调度。
5. **两份 `FOLLOWER_REPLAY_DESIGN.md` 已分叉**：临时环境(3.0)里是 16KB 旧版，主仓库
   工作区里是 28KB v2(含 §9 xid 分析)。P3 开工前必须先合并，否则设计依据不一致。
6. FRD 仍以本节点 OID(`partition_id`)为 shard 主键，未与 P0 的 `global_shard_id` 对齐。

**P3 — 真·物理回放(按用户要求暂不启动)**
- 把 P2 的平凡 apply 换成 `DecodeXLogRecord → 改写 RelFileLocator → rm_redo`
  (见 `FOLLOWER_REPLAY_DESIGN.md`);`partwal_notify_primary_switch` 从日志占位升级为
  真实角色/进度切换。
- 验收:follower 堆表与 leader 收敛一致;切主后新 primary 拥有切换点前全部已提交数据。

### 11.6 关键设计问题清单(P1/P2 展开时需逐条定稿)

1. **日志存储模型**:数据组 entry = 对 parwal 段文件中某 `partition_lsn` 记录的引用,
   而非把字节塞进 `RaftLogEntry.payload`(768 上限)。需定义"以 parwal 为 Raft log
   后端"的读写/截断接口。
2. **调度与选举风暴**:心跳按对端节点合并;election_deadline 用最小堆;评估领导权共置。
3. **成员变更**:分区副本集变更走 joint consensus 或"控制面决议 + 数据组配置热更"的等价
   安全机制,避免脑裂。
4. **跨分区 2PC** ✅ **2026-08-03 已由 `DTX_2PC_DESIGN.md` 回答**:不做"协调者跨组
   屏障",而是把决议写进**写集内的某一个参与组**(`hash(dtxid)` 选取)——决议与该组
   数据在同一条日志上定序,跨组问题塌缩为单组问题;协调权随该组 Raft 选举自动转移。
5. **崩溃恢复**:每组 HardState 与 parwal checkpoint 的一致性;重启后各组独立恢复。

### 11.7 风险与对策

| 风险 | 对策 |
|------|------|
| 单例→N 组是结构性重写,易引入回归 | 控制面保持为 group 0 且行为不变,raft_01–11 作为不回退基线;分期落地 |
| 上千组心跳/选举风暴 | 心跳按节点合并、最小堆调度、领导权共置 / lease |
| shmem 随组数膨胀 | 数据组日志外部化到 parwal 段文件,shmem 只留游标 |
| 全局身份缺失导致跨节点比较无意义 | P0 先行,作为所有后续期的硬前提 |
| 误把"分区组"当成回放本身 | 明确 P2 用平凡 apply 验证运输层,P3 才做 redo |

### 11.8 测试计划增量(随分期补充)

- P0:全局分片 id 唯一定位 + 跨节点 `applied_part_lsn` 可比;raft_01–11 不回退。
- P1:N 组独立选举/复制隔离性;单组 leader 切换不影响他组;控制面基线全绿。
  → 已落地 `raft_12_multi_group_isolation.sql`。
- P2:数据组多数派提交前客户端不返回成功;follower `applied_part_lsn` 真实推进;
  未追平副本不得晋升(复用切主安全线,进度改为真值)。
  → 已落地 `raft_13_data_group_replication.sql`(含字节级一致性与失去多数派的拒写)。
  回归基线自 27/27 提升为 **29/29**(raft_01–13)。
- P3:follower 堆表与 leader 一致;切主后数据不丢。

### 11.9 与既有阶段的映射

P0/P1 对应阶段 4 的结构前置;P2 兑现阶段 2→3 过渡中"数据真正流向副本 + 进度真实化";
P3 兑现阶段 3 的多副本同步语义与 §10 审查差距 #1(主从切换落到数据层)。§10 审查差距
#3(全局身份)即 P0。

### 11.10 数据组日志外部化到 parwal（设计，2026-08-05 定稿）⏪ **整条线已回滚（2026-08-07）**

> ⏪ **回滚声明**：本节 E1–E4 **在代码中一件都不存在**。E1/E2/E3 曾于 2026-08-05/06
> 落地并通过 raft_26/27/28，已于 2026-08-07 按用户指令整条 `git revert`；E4 从未开工。
> 回滚范围、运行期影响与验证见 **§0.4**。
>
> **本节以下内容是设计留档，不是待办清单，也不描述当前代码。** 保留它有两个用处：
> 一是里面记录的几条结论是踩坑得来的（`log_index ≡ partition_lsn` 不是不变式、
> 日志末端不能从段文件推出来、已提交条目的 plsn 天然连续、run 必须随失败回滚一起删），
> 将来若解冻可直接复用；二是理解"为什么 `RAFT_MAX_GROUPS` 抬不上去"需要这段推导。
> 各分期下方标注的 ✅ 是**当时**的状态，现已全部回滚 —— 读的时候请以本框为准。


§11.5.2 #2 把 `RAFT_MAX_GROUPS = 32` 列为阻断项，并指出正解是"外部化到 parwal 段文件，
shmem 只留游标"。这里把它展开成可实施的设计。

**要解决的两件事**（不是一件）：

1. **shmem 随组数线性膨胀**：每组一个 `RAFT_LOG_CAPACITY=128 × sizeof(RaftLogEntry)=800B`
   ≈ 100KB 的 ring，32 组就是 3.2MB，而目标形态是每节点几十上百个分区组。
2. **提交路径上的双写**：每条数据条目一次 `partdist.raft_log` INSERT + 一次
   `committed` UPDATE，而**同一份内容已经在 parwal 段文件里躺着了**。

**为什么可行（已核对代码）**：数据组的每条 Raft 条目都恰好对应一条 parwal 记录 ——
连 DTX 决议也是先 `partwal_append_dtx_record()` 落成带 `PARTWAL_FLAG_DTX` 的记录、
再走同一条数据提案路径（`raft_consensus.c` 判 DTX 用的就是 `flags` + `info`）。
而条目载荷那串描述符
`{partition_lsn, orig_lsn, rmid, info, xid, nbytes, flags}` 与 `PartWALRecord`
头部字段**逐个对应**（`nbytes` = `data_len`）。也就是说数据组的 Raft 日志内容
**已经是** parwal 记录流，`raft_log` 里存的那份是纯冗余。

**唯一缺失的是 `term`**，以及 index↔plsn 的对应关系。

> ⚠️ **`log_index ≡ partition_lsn` 不是现成的不变式**（2026-08-05 核对代码后修正了
> 早先的判断）。`data_propose_one(group_id, partition_lsn)` 的 plsn 由调用方显式指定，
> 而 index 由 `log_append_locked` 独立分配成 `last_log_index + 1`：raft_13 就是在一个
> 已经有 plsn 1..20 的分片上建组、只提案 plsn=1，两者只是**碰巧**都等于 1。把它升级成
> 强制不变式（建组时把基点设成当前最大 plsn、提案必须严格 +1）当然也能成立，但会波及
> 一批既有用例，且 plsn 一旦因失败路径跳号就再也接不上。

**设计：段式边界表（run），而不是逐条映射。**

```sql
partdist.raft_log_runs(group_id, start_index, start_plsn, term)   -- PK(group_id, start_index)
```

一个 run 表示"从 `start_index` 起、连续若干条条目，其 plsn 从 `start_plsn` 起同步 +1，
term 恒为 `term`"，一直延伸到下一个 run 的 `start_index`。于是：

- 条目 i 的 `term` = 满足 `start_index <= i` 的最大那一行的 `term`；
- 条目 i 的 plsn = `start_plsn + (i - start_index)`，载荷从
  `partwal_read_record(local_oid, plsn)` 的**头部字段**重建，与今天写进
  `raft_log.payload` 的那串描述符逐字节等价。

**只在两种时刻追加一行 run**：term 变了（换届，每届一行），或 plsn 不连续。稳态下
一个任期只有一行 —— 这就是它比"逐条存映射"省的地方，也是它不需要
`log_index ≡ partition_lsn` 这个强不变式的原因：一个组从 plsn=k 起步、或中途跳号，
都只是多开一个 run，不是错误。

> **已提交条目的 plsn 其实天然连续**（2026-08-05 写 raft_26 时实测确认，E1 首版把
> "跳号另起一段"写成判据，跑出来才发现那是个**不存在的状态**）：
> `AppendPartWALRecordAt()` 对 `expected > last + 1` 直接 ereport(ERROR) ——
> 注释写得很清楚，"物理回放要求流完整有序无 gap"。于是 follower 落不了盘就不 ack、
> leader 凑不齐多数派、该条目被 `discard_uncommitted_entry` 丢弃。**所以段边界在
> 实际运行中只来自换届**，plsn 分支是防御性的（它守的是"组从 plsn=k 起步"这一类
> 起点不为 1 的情形，以及将来若放宽段文件约束）。
>
> 连带的一条硬约束：run 是在 `persist_log_entry_sql` 里、**先于复制**就写下去的，
> 所以失败回滚路径（`delete_log_entry_sql`）必须把它一起删 —— 否则会留下一个指向
> 不存在条目的段起点，此后所有 >= 它的 index 都按错的 plsn 重建。raft_26 C 段就是
> 守这一条的（in-build 对照：不清 run 的版本确定性失败）。

**落地分期**（每期自带对照用例，不合并提交）：

- **E1 边界表 + 双写核对** —— ✅ **2026-08-05 落地，回归 raft_26**：建表
  （`partdist.raft_log_runs`）、`persist_log_entry_sql` 里统一维护 run（leader 的
  propose、follower 的 append、重传都经过它）、新增
  `partdist.pg_raft_entry_from_parwal(group_id, index)` 按 run 反查并重建载荷。
  此期**不删**任何既有写入，用例逐条比对重建结果与 `raft_log` 里存的那条
  （比 jsonb 不比文本 —— payload 列是 jsonb，读回来已被规范化）。
  三重对照：不维护 run ⇒ A 段全错；退化成逐条一行 ⇒ B 段失败；回滚不清 run
  ⇒ C 段失败。
- **E2 读路径切换** —— ✅ **2026-08-05 落地，回归 raft_27**：环外那一级由
  `log_get_entry_durable()` 分派 —— 控制面仍回读 `partdist.raft_log`，数据组走
  `log_get_entry_parwal()`（按 run 反查 + 记录头部重建）。三个调用点一并切换：
  `log_get_entry_ext`、`replicate_to_peer` 的环外回退（prev 与待发条目）、
  `handle_append_entries` 的环外冲突判定。数据组从此**只认段文件**，重建不出来就
  当作"本节点没有这一条"，不去 `raft_log` 里捞可能与字节不一致的影子。
  （`data_entry_fetch_hex` 本来就是从 parwal 读字节，无需改动。）
  对照：把 `log_get_entry_parwal()` 摘掉重编 ⇒ victim 确定性冻结在 0，leader 已到 140。
  > 遗留：E2 之后，**E1 之前就存在的数据组条目**（那时还没有 run 行）重建不出来。
  > 全新集群无此问题；存量集群升级需要一次性回填 run（尚未做，E3 一并处理）。
- **E3 写路径瘦身** —— ✅ **2026-08-05 落地，回归 raft_28**：数据组提交路径去掉
  `partdist.raft_log` 的 INSERT / committed UPDATE / DELETE（`persist_log_entry_sql`
  只剩维护 run，`mark_log_committed_sql` 直接跳过，两个 delete 只删 run），
  重启恢复改走 `restore_data_log_from_parwal()`。

  > **日志末端不能从段文件推出来**（做 E2 时核对发现，早先"shmem 只留游标"的说法
  > 在这一点上是错的）。直觉做法是"`last_log_index` = 末个 run 的
  > `start_index + (max_plsn - start_plsn)`"，但 **leader 的段文件里躺着大量还没被
  > 提案的记录** —— demux 按本地提交不停地写，提案是另一条路径按需追上去的。
  > raft_27 的夹具就是现成的证据：段文件里 seed 了 160 条，只提案了 140 条。
  > 拿 `max(partition_lsn)` 当日志末端会**凭空多出 20 条**，重启后这个 leader 会
  > 以为自己持有从未复制过的条目，直接破坏 Leader Completeness。
  >
  > 落地方案：`last_log_index` 随 **hardstate v4** 持久化 —— 它本来就在每条 append
  > 之后被 `persist_hard_state_unlocked()` 写一次，加一个字段是零额外开销，而
  > `raft_log` 那份逐条 INSERT 正是要被它替换掉的东西。恢复因此是：末端取自
  > hardstate，(term, plsn) 映射取自 run，环里最近 ≤128 条按需重建；重建不出来时
  > **把末端下调到能连续重建出来的位置并告警**，而不是硬撑着声称持有 —— 下调是
  > 保守方向（只会显得更旧，leader 重发即可），反过来才会破坏 Leader Completeness。
  >
  > **E2 遗留的存量回填就此消解**：数据组不再读 `raft_log`，老行留着不碍事；
  > 没有 run 行的老条目在重启时重建不出来，会被上面那条"下调末端 + 告警"接住，
  > 表现为该成员日志变短、由 leader 重发补齐，而不是静默错位。

- **E4 去环 + 抬上限** —— 🧊 **未开工，已冻结**：数据组不再分配 ring（shmem 只留游标），
  `RAFT_MAX_GROUPS` 随之抬高。用例：建 N（远大于 32）个数据组仍能选举与复制。
  **解冻需项目负责人明确指令**（§0.4）。

**已知风险**：
- 段文件被运维清理/损坏时，日志就真的没了 —— 今天 `raft_log` 还能兜底。E3 之后
  数据组的持久性完全押在 parwal 上，需要在 E3 一并确认段文件的保留策略与
  `partwal_truncate_to` 的调用点没有能删掉未提交前缀的路径。
- 数据组的压缩/快照仍然不做（§4 阶段 1）：外部化之后"删日志"等价于删段文件，
  而段文件在 P3 物理回放之前就是数据本身。

---

## 12. 物理回放(P3)交接说明(2026-07-21)

本节是**做物理回放之前的交接文档**：接手的人只读本节即可知道“做到哪了、卡在哪、下一步做
什么”。判定口径为提交 `d06c821` 的代码实测结果，不是设计意图。

### 12.1 结论先行：日志复制是否已满足物理回放的前提？

**运输层：是。非运输层：否。整体结论 —— P3 还不能直接开工。**

这个区分很重要，因为 2026-07-18 曾经下过一个“至此物理回放前提具备”的结论，**那是错的**，
已在 2026-07-20 的审查中推翻。当时错在只看了“字节能不能复制过去”，没看“换成 redo 后这些
字节会不会被正确地、恰好一次地、在正确的编号上回放”。平凡 apply 恰好是**幂等且单调**的，
把五个运输层缺陷全都掩盖了 —— 这是本项目到目前为止最值得记住的一次教训：
**用一个过于宽容的消费者去验证运输层，会得到虚假的绿灯。**

| 前提 | 要求 | 状态 |
|------|------|------|
| 字节能跨节点送达副本 | AppendEntries 携带原始 WAL 字节 | ✅ |
| 送达顺序确定 | 每分区 Raft log 定序，`prev_log` 一致性检查 | ✅ |
| 提交语义 = 持久化语义 | follower 先 fsync 到自己的 `pg_parwal` 再 ack | ✅ |
| **副本上的编号与 leader 一致** | follower 按 leader 指定的 `partition_lsn` 落盘 | ✅ 加固后 |
| **重复投递不产生第二份** | 重传幂等 no-op | ✅ 加固后 |
| **崩溃后不漏放已提交条目** | `last_applied` 持久化（hardstate v2） | ✅ 加固后 |
| **apply 失败不静默吞掉** | 游标只在 apply 成功后推进，失败重试 | ✅ 加固后（数据面） |
| **回滚的字节不残留** | 截断触达 parwal 字节 | ✅ 加固后 |
| **回放产生的 xid 在本地 clog 不冲突** | 集群级 xid 区间租约 | ❌ **未做，最硬** |
| **一个节点能托管足够多的分区组** | 组数不受 32 限制、日志不占 shmem | ❌ 未做 |
| **多数派按真实副本集计算** | 成员集由控制面下发 | ❌ 未做 |
| **P3 设计依据唯一且与 P0 对齐** | FRD 单一副本、以 `global_shard_id` 为键 | ❌ 未做 |

一句话：**“把字节安全送到副本并按正确编号落盘”这件事已经做完了；“把字节变成堆表内容”
所依赖的节点级资源隔离还一件都没做。**

### 12.2 已完成的部分

**P0 全局分片身份（2026-07-17）** —— `partdist.shard_identity(global_shard_id PK, local_oid,
relfilenode, local_relname, logical_relid)`，`global_shard_id` 直接采用 Citus `shardid`
（`pg_dist_shard` 各节点同步，天然跨节点一致）。核心用途是把一个 Raft 组 id 解析成**本节点**
的分片 OID —— 因为同一逻辑分区在各节点的 OID 不同，而 `pg_parwal/` 目录就是用 OID 命名的。
没有它，跨节点比 `applied_part_lsn` 是没有意义的。

**P1 Raft 核心组化（2026-07-18）** —— 单例结构变成按 `group_id` 索引的集合；**组 0 = 控制面，
行为与组化前逐字节一致**；`raft_log` 按 `(group_id, log_index)` 分命名空间。调度上被回归实测
逼出两项必需对策：按对端复用 libpq 连接、对端级 RPC 退避（**退避只对心跳/复制生效，选举豁免**
—— 候选人少收一票就可能长时间无主）。

**P2 数据组复制 + 平凡 apply（2026-07-18）** —— entry = 小 JSON 描述符 + 随行 `bytea` 原始字节，
从而完整复用既有 ring / 一致性检查 / 多数派提交，又不必把任意长字节流塞进 768 字节 payload。
follower 先落盘再 ack。apply 只推进 `applied_part_lsn`，**不做 redo**。

**运输层加固（2026-07-20，`d06c821`）** —— 见 §11.5.1 六条。其中最关键的是第 1 条：
follower 必须按 **leader 指定的** `partition_lsn` 落盘，而不是本地自增。原因直接来自架构前提
（§2.1 #2/#3）：一个节点的 `pg_parwal/` 目录树下既有它作为 primary 的本地 demux 写入，也有它
作为 secondary 的复制流，用本地计数器会让两个编号空间永久错位，届时 `applied_part_lsn` 指向的
记录在本地根本不存在。

**回归**：29/29（raft_01–13，连续三轮稳定）+ P0 10/10。
（2026-07-24 起基线为 **31/31**，含切主重构的 raft_14/15，见 §13.5。）

### 12.3 存在的问题

按“是否阻断 P3”分两类。

**A. 阻断 P3，必须先解决：**

1. **xid / clog 跨 leader 冲突（最硬）。** 重放后 tuple 的 `xmin/xmax` 是 **leader 的 xid**，
   而 clog 是**节点全局**共享的。各分区 leader 独立分配 xid，所以一个节点托管来自**不同
   leader** 的多个分区副本时，相同 xid 值会在本地 clog 相撞 —— A 组说它已提交、B 组说同值
   已回滚，而 clog 每个 xid 只能记一个状态。`FOLLOWER_REPLAY_DESIGN.md` §9 明确写着
   “集群统一 xid 区间批发落地前，多 shard 共存于一个 follower 的配置**不可上线**”。
   **而按 §2.1，多 shard 共存于一个节点正是本架构的默认形态，不是边缘情况。**
   必须先做集群级 xid 区间租约。
2. **`RAFT_MAX_GROUPS = 32`**，即每节点最多 31 个数据组。单纯调大只会把墙往后挪 ——
   每组一个 128 × 768B 的 shmem ring，shmem 随组数线性膨胀。正解是把数据组日志外部化到
   parwal 段文件，shmem 只留游标（§11.8 早已列出，P1 落地时没做）。
3. **组成员集不随 RPC 传播。** 自动建组的 follower 成员集为空，于是按**全体 peers** 算多数派。
   真实拓扑下一个分区的副本集是全体节点的**真子集**，所以这个多数派是算错的。成员集必须来自
   控制面下发的 `partition_map`，而不是“从收到的报文里听说”。
4. **两份 `FOLLOWER_REPLAY_DESIGN.md` 已分叉**：临时环境（3.0）里是 16KB 旧版，主仓库工作区
   里是 28KB v2（含 §9 xid 分析，未提交）。P3 的设计依据必须唯一。且 FRD 仍以本节点 OID 为
   shard 主键，未与 P0 的 `global_shard_id` 对齐。

**B. 不阻断 P3，但会限制可用性：**

5. ~~`pg_raft_data_propose()` **只有回归在调用**，尚未接入 demux/写入路径；数据条目只在 client
   backend 路径下发（取字节需要 SPI，而 BGW tick 没有 SPI），**没有后台追平通道** ——
   无 propose 流量时落后的 follower 不会自行收敛。~~
   接入已于 2026-07-24 完成（§4 阶段 3 的 **prepare 四步设计**）；
   **追平通道已于 2026-08-05 补齐**（`partdist.pg_raft_catchup()`，见 §12.4 #5）。
6. **无 InstallSnapshot**。~~日志落后超 ring 容量时靠拒写背压，落后太多的副本无法追平。~~
   **2026-08-05 起追平不再依赖它**（环外条目从 `partdist.raft_log` 回读）；
   它现在是**日志压缩的前提**：一旦按 `last_included_index` 删行，删掉的那一段
   就只能靠快照传输。

   > **运维告警（2026-08-03 实测踩到）**：这条缺口在**运维误操作**下会把控制面
   > 打成半瘫。任何清空 `partdist.raft_log` 的动作（最容易中招的是
   > `setup-raft.sh` 的 `cleanup_raft_loose_objects()`——它第一句就是
   > `DROP EXTENSION pg_raft CASCADE`，会连带删表）都必须**全节点同时做并全部重启**，
   > 否则已重启的节点回到 index=0、未重启的仍在几百，前者永远追不上。
   > 症状：某个节点（本次是 coordinator）group0 恒为 0/0/0 而其余节点正常，
   > 登记到不了 master、`pg_dist_placement` 不切、大批用例连锁失败。
   > 控制面重置顺序：先 `ALTER SYSTEM SET pg_raft.raft_enabled=off` + reload
   > （否则 TopologyMonitor 每秒把清掉的条目写回去），再清表，再停机删
   > `pg_raft_hardstate*`，最后恢复 GUC 重启。
7. `partwal_notify_primary_switch()` 仍是日志占位，切主后不做真实角色切换。
8. **raft_13 夹具用的是 Citus reference 表**，而 reference 表在每个节点都是本地主写，与
   “secondary 不本地产出该分区 WAL”的前提相悖（§11.5.1 的假阳性教训）。数据组用例应改用
   真正的“一主多从”分片放置，并补一个“同节点混合角色”的用例。
   **→ 2026-07-24 已补 raft_14**：真实哈希分布分片 + 同构壳表从副本（(a) 形态），多记录
   逐条 propose、逐字节校验、连续性与"不回放"断言（见 §13）。raft_13 保留作运输层回归；
   "同节点混合角色"用例仍待补。
9. ~~**缺少全新库上的 `CREATE EXTENSION` 冒烟检查**~~ **→ 2026-07-24 已入常规验收流程**。
   历史背景：回归环境总是复用已装扩展，`setup-raft.sh` 又只补建函数、不重跑安装脚本，
   因此安装脚本内部的不一致是**完全静默**的。2026-07-21 即实测到一例：运输层加固改了
   `partwal_follower_append` 的签名，但 `COMMENT ON FUNCTION` 仍写旧的 6 参数形式，
   导致全新库 `CREATE EXTENSION pg_partdist` 直接 ERROR，而 29/29 回归全绿。已修复。

### 12.4 后续内容（建议顺序）

顺序是有依据的：先统一设计依据，再拆掉节点级资源冲突，最后才动 redo。

1. **合并两份 FRD**，并把其中的 shard 主键从本地 OID 改为 `global_shard_id`。
   —— 不做这步，后面所有设计讨论都在两个不同的文本上进行。
2. **集群级 xid 区间租约**（FRD §9 方案 A）。这是 P3 真正的门槛，工作量也最大：
   需要一个跨集群的 xid 区间分配者，各分区 leader 只在自己租到的区间内分配 xid，
   保证任一节点上来自不同 leader 的 xid 不重叠。**这一项完成前，P3 做出来也不可上线。**
3. **数据组日志外部化到 parwal + 解除 `RAFT_MAX_GROUPS` 限制**。顺带解决 ring 容量与
   日志压缩（阶段 1 遗留的两项 ❌ 也一并关闭）。
4. ~~**成员集经控制面下发**（`OP_CONFIG_CHANGE`），多数派按真实副本集计算。~~
   **→ 2026-08-03 已完成**（走 `partition_map` 导出而非新增 RPC，见
   `DTX_2PC_DESIGN.md` §9.2；回归 raft_18 覆盖"副本集是全体真子集"场景）。
   仍缺：成员**变更**的 joint consensus、"同节点混合角色"回归。
   完成后补“副本集是全体节点真子集”和“同节点混合角色”的回归。
5. ~~**接入真实写入路径**：把 `pg_raft_data_propose()` 挂到 demux/写入路径上，
   并补一条后台追平通道（需解决 BGW 无 SPI 的取字节问题）。~~
   **✅ 2026-08-05 全部完成**。接入见 §14（2026-07-24）；追平通道的落地形态：
   - "BGW 无 SPI"不是绕过而是**换语境** —— TopologyMonitor 经 libpq 自连调
     `partdist.pg_raft_catchup()`，于是整段追平跑在真正的 client backend 里
     （与 `pg_raft_force_probe` / `dtx_recover_prepared` 同一手法）。
   - 顺带补上**环外条目回读**：`log_get_entry_sql()` 从 `partdist.raft_log` 取
     已滑出环窗口的条目，leader 侧取条目、follower 侧 prev 检查与冲突判定都走它。
     此前落后超过 128 条的成员会**静默永久卡死**（prev_term 取 0 → 对端拒 →
     next_index 退到 1 → 只剩心跳），字节其实一直都在盘上。
   - `peer_last_log_index()` 追平提示：本实现的 AE 响应没有 conflict hint，
     `next_index` 一次只能退一格；新 leader 又把它初始化成 last+1，落后几百条时
     一轮追平还没探完就可能被下一次选举打断。改为先问对端"你的 last_log_index"，
     只作**起点**（prev 检查照旧），把 O(N) 次探测压成一次查询。
   - 语义边界：只补发**已存在**的条目，不产生新提案；提交点仍按多数派推进；
     与 prepare 路径共用复制认领位但**取不到就跳过**（非阻塞，绝不与事务抢锁）。
   - 回归 raft_24（两级对照：关通道必不收敛 / 只摘掉环外回读则 >128 段确定性失败）。
6. **P3 物理回放本体**：把平凡 apply 换成 `DecodeXLogRecord → 改写 RelFileLocator → rm_redo`。
   验收：follower 堆表与 leader 收敛一致；切主后新 primary 拥有切换点前的全部已提交数据。
7. ~~其余：InstallSnapshot~~ **✅ 2026-08-05 完成（控制面）**：压缩 + InstallSnapshot
   同期落地，回归 raft_25。剩 `partwal_notify_primary_switch` 真实化，以及
   **数据组**的压缩/快照——前者的正解是日志外部化（#3），后者要等 P3
   （在此之前数据组的"快照内容"与"日志内容"是同一份东西）。

> **2026-08-03 顺序修正**：2PC 不再排在最后，也不依赖 P3 —— 见
> `pg-partdist-src/docs/DTX_2PC_DESIGN.md` §10 的六步落地顺序。其中第 1 步
> （修 group-commit 让路窗口）是**独立于 P3 的正确性修复**，应立即执行；
> 第 2 步（成员集显式化）与本节 #4 是同一件事，2PC 与 P3 共用。
> 反过来，2PC **不解除** R3/R4 的既有阻断（该文 §9.6）。

> **交接提示**：对 `pg-partdist-src`（即 raft 之外）的每一次改动，都必须在
> `pg-raft-src/docs/pg_partdist_sync_change_log.md` 追加记录，写明时间、文件、目的与对
> Raft/failover/PartWAL 行为的影响。该台账目前已记到 2026-07-21。

## 13. 切主机制重构：自治选举 → 上报登记 → 落路由层（2026-07-24）

### 13.1 动机与职责重划

此前的切主是**控制面指定式**：group 0 leader 探测到节点宕机后，由
`pg_raft_failover_partitions_for_node()` 单方面挑选最追平的 secondary 写进
`partition_map`。分区级数据组落地（P1/P2）之后这个机制出现两个根本问题：

1. **与数据组自治选举冲突（双真相源）**。数据组用与控制面完全相同的选举逻辑，主副本所在
   节点宕机后，组内 follower 会在选举超时内自行选出新 leader；控制面若再"指定"一个，可能
   指定到没有赢得组内选举的节点，`partition_map` 与真实 leader 分叉。
2. **切主结果从未到达路由层**。raft 只写 `partdist.partition_map`，而 Citus 实际路由依据
   `pg_dist_placement` —— 两域之间没有桥。

重构后的职责边界（与 TiKV/PD、YugabyteDB/YB-Master 的形态一致）：

- **数据组**：自治选举（谁当 leader 由组内多数派决定，选举限制天然把主给最追平者）、
  日志复制、以及**新任 leader 主动上报**。
- **控制面（group 0）**：不再决定数据面 leader，只做**登记处 + 配置权威**——接收上报、
  任期栅栏裁决、经 raft 日志把结果同步到每个节点（含路由层）；以及既有的节点状态、
  组配置、快照职责。master 不作任何分区的数据副本。

### 13.2 机制

```
主副本所在节点宕机
  → 组内 follower 选举超时([1.5s,3s) 随机)自治选出新 leader（无控制面参与）
  → become-leader 置 report_pending（BGW tick 无 SPI，不能就地上报）
  → tick 经 libpq 向 group 0 当前 leader 调 partdist.pg_raft_report_data_leader(
        gid, self, term, secondaries)，失败/暂无主则每 tick 重试
  → 接收端（常态是 master）校验：term>0、新主不是协调节点、任期不回退（预检），
     然后把 OP_PARTITION_PRIMARY（含 primary_term）提进 group 0
  → group 0 多数派提交后，每个成员节点各自 apply：
       a) 任期栅栏 upsert 本地 partition_map（WHERE primary_term <= EXCLUDED.primary_term）
       b) partition_id 是真实 Citus shardid 时，UPDATE 本地 pg_dist_placement 指向
          新主所在 Citus group（按 node_map.port ↔ pg_dist_node.nodeport 映射；
          UPDATE 触发 Citus 的 dist_placement_cache_invalidate 失效缓存）
```

要点：

- **任期栅栏**（`partition_map.primary_term`）：迟到的旧任期登记、重复登记、以及旧
  "指定式"通道的 term=0 提案都被 apply 的 WHERE 拦下；栅栏在 apply 里，group 0 各成员
  按同一日志序执行，判定天然一致。
- **路由元数据的"每节点一份"由 raft 本身完成**：不是全节点强同步（那会让任一节点宕机
  阻塞元数据更新——恰恰在最需要更新的时刻），而是**多数派提交 + 全员 apply**；
  宕机节点恢复后由日志重放追平。
- **协调节点排除**：新 GUC `pg_raft.coordinator_node_id`（缺省 1）。`pg_raft_group_create`
  拒绝把协调节点放进数据组成员集；上报接收端拒绝把协调节点登记为主，混入 secondaries
  时剔除并告警。
- **group 0 leader 优先落 master**：协调节点在 group 0 用不相交的更短选举窗口
  [2b/3, b)（缺省 [1000,1500)ms，其他节点 [1500,3000)ms），在世时总是先超时先当选。
  **是偏好不是保证**：master 宕机时 worker 照常接管；master 回归后不会自动抢回，
  直到下一次改选（不做主动 leader transfer，避免无谓扰动）。
- **旧通道降级而非删除**：`pg_raft_failover_partitions_for_node()` / rejoin 只对
  `primary_term = 0` 的分区（无数据组的历史/合成夹具）生效；有数据组的分区完全由
  自治选举 + 上报接管。raft_02/04/07/10/11 等旧回归因此语义不变。旧通道随夹具迁移
  逐步退役。
- **环满静默 ack 安全修复（顺带）**：`handle_append_entries` 此前忽略 `log_append_locked`
  的失败返回，环满时条目没落下却回 `success=1`，leader 会把不存在的复制计入多数派——
  提交点可能覆盖到少数派都不持有的日志，破坏 Leader Completeness。现失败即不 ack，
  leader 走退避重试，待 apply 推进腾出环位。

### 13.3 验收

- **raft_14（复制半程）**：单分片哈希分布表（rf=1），组成员 {2,3,4}，leader=placement
  worker；其余 worker 建同构壳表 + `rebuild_shard_identity()`（(a) 形态）；经 master 路由
  写入 5 行，leader parwal 逐条 propose；断言 follower：逐字节指纹一致、1..N 连续无洞、
  一条 record 一次备份（条数==N）、`applied_part_lsn` 追平、壳表 0 行（不回放）、
  partition_map 登记（primary/term/secondaries 不含 master）与本地 pg_dist_placement
  一致；master 全程无该分片身份。
- **raft_15（切主全程）**：停掉主副本节点 → 等自治选举+上报 → master 与两个存活 worker
  各自断言：新主 ∈ worker、任期严格递增、旧主仍在 secondaries（成员集静态）、本地
  pg_dist_placement 已切到新主；旧主重启后以 follower 归队、登记不被回退。

### 13.4 验收过程中暴露并修复的历史隐患

三个都是被旧行为掩盖的真问题，是环满**诚实拒绝**（不再假 ack）之后连锁显形的：

1. **桥接误伤多放置分片**：reference 表同一 shardid 在 `pg_dist_placement` 有多行
   （每节点组一行），直接 UPDATE 会撞 `(shardid, groupid)` 唯一键。raft_13 的
   reference 夹具组当选上报时实测踩中——修复：桥接只对**单放置**分片生效，且整体
   包在子事务里，任何意外 SQL 错误只回滚桥接自身并告警，**不允许打穿控制面 apply**
   （否则一条毒丸日志会卡死游标，之后所有控制面决议全部停摆——首轮回归实测到）。
2. **重启后 apply 游标滑出环窗口 → 集群拒收一切新提案**：SQL 日志长于环容量
   （128）时，restore 只能把最新一段灌回环；恢复出的 `last_applied` 若低于环内最老
   条目，apply 在"取下一条"处永久卡死，容量检查随之恒满。旧代码靠假 ack 掩盖；
   现按控制面"元数据表持久、跳过安全"的既有语义，把游标**快进到环内实际存在的最
   老条目**（窗口内向前扫描，兼容不满的环——节点的 SQL 日志可能只覆盖它在线期间
   的一段，实测有节点只有 293..348）。数据面不快进（漏 redo 即分叉），维持卡住等
   追平通道（InstallSnapshot，§12.4 #7）。
3. **BGW 选举可能先于恢复、把零值写回 hardstate**：重启后 BGW tick 可能在任何
   SQL 路径触发恢复之前就发起选举并持久化全零的 term/commit/last_applied。修复：
   `group_tick` 先 `restore_hard_state_if_needed`（纯文件读、无 SPI，BGW 可做）；
   `pg_raft_consensus_apply_pending` / `group_propose` 入口补齐日志恢复与先-apply-
   后-append 的顺序。

> 这一节印证 §12.1 的教训的另一半：**用一个过于宽容的运输层去伺候消费者，
> 也会得到虚假的绿灯。** 假 ack 消失后，上面每一条都从"从未见过的问题"变成
> "分钟级复现"。

### 13.5 验收结果

raft_01–15 全量 **31/31**（含新 raft_14/raft_15），全新库 `CREATE EXTENSION
pg_partdist` + `pg_raft` 冒烟通过（§12.3.B.9 要求的检查已入常规流程）。

### 13.6 诚实边界（本次不解决）

1. **路由切换是"机制先行"**：P3 回放落地前，新主壳表没有历史数据，路由切过去还不能
   服务读写。生产语义要等回放追平后再放行切换（§12.4 #6 的验收项）。
2. **成员集传播仍是阻断项**（§12.3.A.3）：上报时成员集未知（hearsay 建组）会退化为
   "全体 peers 去掉自己与协调节点"，真实拓扑下需要控制面下发成员集后才严格正确。
3. ~~一致性时序：复制仍在事务提交后由测试驱动~~ **→ 2026-07-24 已解决（§14）**：
   复制现在卡在 [A]（parwal 落盘）之后、[B]（pg_wal 提交 fsync）之前，
   即"pgparwal 落盘后、pgwal 落盘前"的目标时序。
4. master 目前是单点协调（Citus coordinator 本就如此）；master 的 HA（流复制热备）
   不在本阶段范围。

## 14. 事务 prepare 阶段接线（2026-07-24）

把 §4 阶段 3 的四步 prepare 设计中缺失的第 2 步（复制自动挂接）落地。至此四步全部就位：
写入 → 本地 parwal 落盘 fsync（[A]）→ **逐条复制到分区组并等多数派**（新增）→
pg_wal 提交 fsync（[B]）。复制严格发生在 [A] 之后、[B] 之前 ——
即"pgparwal 落盘后、pgwal 落盘前"的一致性时序。

### 14.1 机制

- **挂点**：`PartWALFlush()`（事务 PRE_COMMIT / PRE_PREPARE 唯一调用方）在完成本事务
  记录的落盘 fsync 并释放全部锁之后，对**本 backend 本事务**涉及的每个分区调用复制挂钩。
- **跨扩展注入**：pg_partdist 不依赖 pg_raft。挂钩经 PostgreSQL rendezvous variable
  `"partdist_partwal_replicate_hook"` 注入：pg_raft 的 `_PG_init`（shared_preload 阶段）
  写入函数指针 `pg_raft_partwal_replicate`；未装载/未启用 raft 时指针为空，零开销、
  行为与接线前完全一致。
- **复制驱动**（`pg_raft_partwal_replicate(partition_oid)`）：
  本地 OID → `global_id_for_partition` → 有无数据组（无则直接返回，未纳管分区不受影响）
  → 增量范围 = `(组内 last_data_plsn, 当前 flush lsn]`，逐条 `data_propose_one`
  （一条 record 一次备份）。`last_data_plsn` 随成功 propose 推进，重启后从环内最后一条
  OP_PARWAL 回推；配合 follower 落盘幂等，重复/回退都无害，且**组建立前的历史记录会在
  首次写入时自动补齐复制**。
- **步骤 4 语义（quorum 即 prepared）**：任何一条记录未达多数派 → ERROR → 事务在
  prepare 中止。~~`group_propose` 失败路径连带回滚 leader 侧该条目的 ring/SQL/parwal
  字节（运输层加固 #5），中止事务不在 plsn 空间留渣。~~
  **→ 2026-08-03 语义修订**：leader 侧失败回滚只撤 Raft 日志条目（ring + SQL 行），
  **parwal 字节不截断**——原来的截断会连带删掉并发事务已在 [A] 落盘的记录，
  受害事务的挂钩因增量游标空转而带着"已复制"假象提交（丢数据，raft_17 阶段二
  实锤 13/240）。失败条字节留作孤儿、下次复制按同一 plsn 重新 propose；
  "中止事务不留渣"作废：parwal 流中允许存在中止事务的 DATA 记录，可见性由
  标记/决议闭合（`DTX_2PC_DESIGN.md` §5、§9.0 缺陷 3）。follower 侧
  AppendEntries 冲突截断（运输层加固 #5 的另一半）保留不变。
- **写栅栏（顺带获得）**：分区有数据组而本节点不是该组 leader 时，本地写入在 prepare
  即被 ERROR 拒绝 —— 切主后旧 primary 上仍在途的事务无法提交，不产生分叉写入。

### 14.2 验收（raft_16，全程无手工 propose）

1. 正向：仅 INSERT（经 master 路由），follower parwal 自动出现逐字节一致、
   1..N 连续无洞的备份，`applied_part_lsn` 追平，壳表 0 行（不回放）；
2. 失多数派：停两个 follower 后 INSERT 必须失败（prepare 中止），行数不变；
3. 恢复：follower 回归后再 INSERT，连同中断期间的增量自动追平，终态逐字节一致。

### 14.3 边界（本次不解决）

1. **group commit 让路窗口** —— ⚠️ **2026-08-03 重新定级：不是边界，是正确性阻断项。**
   若本事务的记录被并发的其他 backend 顺带落盘（`flushed_upto` 已覆盖时本 backend
   提前返回），该事务不会触发复制挂钩；且 `touched[]` 只登记
   `slot->backend_id == MyBackendId` 的槽位，而槽位已被 peer 消费置 `valid=false`，
   ⇒ **本 backend 事后也无从知道自己写过哪些分区**。
   在当前（无 2PC）形态下这只是"复制延后到该分区下一次写入"；
   **一旦 2PC 落地，它变成"PREPARE 返回成功但字节从未达多数派"⇒ 协调者据此写
   COMMIT 决议 ⇒ 参与组多数派上根本没有该事务的数据 ⇒ 丢数据。**
   修法（`DTX_2PC_DESIGN.md` §9.1，两处缺一不可）：(a) 触达集合改为在
   `PartWALInsert` 时 per-backend 记录（给已有的 `partwal_pending[]` 加一个
   `partition_id` 字段即可，零额外结构）；(b) **提前返回路径也要调复制挂钩**。
   顺带把 prepare 语义从"逐条 propose 我的范围"收紧为"确保复制推进到 ≥ X 并等
   `commit_index` 覆盖"——消除并发 backend 抢同一段 plsn 的重复 propose，
   批量场景天然合批。
2. **raft 日志 SQL 行与用户事务同命**：propose 在 PRE_COMMIT 内经 SPI 写
   `partdist.raft_log`，若事务在 propose 之后仍中止（后续回调 ERROR 等窄窗口），
   leader 的 SQL 日志行随之回滚而 shmem 环仍在 —— 与既有 propose-in-txn 路径同级的
   已知风险，正解是持久化通道与用户事务解耦（随日志外部化一并处理）。
3. **reference 表与数据组不兼容**（既有结论的新表现）：reference 表每节点本地主写，
   若为其建组，非 leader 节点的本地写入会被写栅栏拒绝。数据组只应服务
   "一主多从"的哈希分片。

---

## 15. 与物理回放分支（`shardpg-replay`）合并的交接要点（2026-08-07）

本节写给**执行合并的人**。判定口径为两个远端分支尖端的实测 diff，不是设计意图。

### 15.1 谱系与合并方向

```
shardpg-replay @ 1df5fde ──┬──> shardpg-4.0  @ 6181655   （Raft/2PC 线，18 个独有提交）
                           └──> shardpg-replay @ e2a5a7d （物理回放线，6 个独有提交）
```

`shardpg-4.0` 是 2026-08-01 从当时的 replay 尖端 `1df5fde` 拉出的，因此**两边共享
L1/R1 的全部成果**，此后各自前进。这是一次真正的双向合并，不是快进。

**replay 侧 6 个独有提交**（`git log shardpg-4.0..shardpg-replay`）：

| 提交 | 内容 | 与本线的关系 |
|---|---|---|
| `6624c08` | **R2 事务层**：parwal-**3.0** gxid 头 + MARKER 标记 + `xid_map` + 增强型 CLOG | ⚠️ **改了记录头格式**，见 §15.2 |
| `f90c05b` | **D1**：`CTRL:FILESET_UPDATE` + 结构栅栏 | ⚠️ **启用了 `PARTWAL_FLAG_CTRL`**，见 §15.2 |
| `071bfbe` | 修复：本地 pg_wal 崩溃恢复无条件覆盖回放结果（btree 元页丢失） | 无冲突，纯收益 |
| `0375999` | **D2**：`CTRL:FREEZE_UPDATE` 同步 `relfrozenxid` | 同 D1 |
| `4740239` | 仓库 `pg-install` 补上补丁 0001v2/0002 + `reproduce-env.sh` 前置自检 | ⚠️ 与 4.0 侧的 `468a518` 同源不同做法 |
| `e2a5a7d` | `reproduce-env.sh` 的 V6 按实际 follower 数自适应 | 4.0 是 1c+8w，**应当采纳** |

**4.0 侧 18 个独有提交**：切主重构（§13）、prepare 接线（§14）、DTX-2PC 第 0–5 步、
成员集显式化、后台追平通道、控制面压缩 + InstallSnapshot。
（外部化 E1–E3 的三个提交也在其中，但已被 2026-08-07 的三个 revert 抵消，
合并时净效果为零 —— 见 §0.4。）

### 15.2 ⚠️ 必炸清单（合并后不修就一定坏，按确定性排序）

> ⏪ **原 #1 已随外部化回滚而消失（2026-08-07）**。它曾是本清单里唯一"确定会坏"的一条：
> replay 侧 R2 把 `partwal_read_record` 的 OUT 列 `xid` 改名 `gxid`，而本线 E1 引入的
> `pg_raft_entry_from_parwal()` 硬编码了 `w.xid`，合并后必报"列不存在"、数据组环外
> 重建全线失效。**该函数现已随 E1 一并删除，冲突点不复存在**，`partwal_read_record`
> 的调用方回到只有 C 代码一处（`data_entry_fetch_hex`，按列序号取值，改名不影响）。
> 保留这段记录，是因为若将来解冻外部化，这个坑会原样回来。

**#1 `PARTWAL_FLAG_CLASS_MASK` 不含 DTX 位，会把 DTX 记录喂给 `rm_redo`。**

- replay 侧：`PARTWAL_FLAG_CLASS_MASK = 0x07`（DATA/MARKER/CTRL），
  `PartWALRecordIsData(rec)` = `(flags & (MARKER|CTRL)) == 0`
- 4.0 侧：新增 `PARTWAL_FLAG_DTX = 0x08`，
  `PARTWAL_FLAG_NON_DATA_MASK = MARKER|CTRL|DTX`

两边的 `PartWALRecordIsData` 判定式必须**取并集**。若沿用 replay 侧那份，
`PARTWAL_FLAG_DTX` 的记录会被判成 DATA，其载荷（`DtxRecord` 结构）被当作原始
`XLogRecord` 送进 `GetRmgr(rmid).rm_redo` —— 直接 PANIC 或写坏数据页。
另外注意两边的宏名不同（`CLASS_MASK` vs `NON_DATA_MASK`），机械合并容易只留一个。

**#2 `PartWALRecord` 尾部 8 字节的含义在两个版本间不同，混版段文件会读出垃圾。**

两边 `sizeof(PartWALRecord)` **都是 40 字节**（4.0 侧 36 字节字段 + 4 字节尾部填充），
但偏移 32–39 的含义不同：

| 版本 | 32–35 | 36–39 |
|---|---|---|
| v2（4.0 侧当前） | `xid`（32 位） | 填充 |
| v3（replay 侧 R2） | `gxid` 低半 | `gxid` 高半 |

按偏移解析的代码必须先判 `version`（replay 侧已有
`if (rec->version >= PARTWAL_RECORD_VERSION_3)` 的分支，合并时保留它）。
**合并后首次启动前建议清空 `pg_parwal/`**：跨版本混流的段文件没有真实价值，
留着只会制造难查的偶发。

**#3 `raft_consensus.c` 双向修改同一处 —— 但语义一致，别机械选边。**

replay 侧 `6624c08` 也改了 `data_propose_one`：描述符从 `{...,"xid":T,...}` 变成
`{...,"flags":F,"gxid":G,...}`，**并且独立修掉了与本线 `97d5421` 相同的那个
`partwal_read_record` 返回全 NULL 行导致 `TextDatumGetCString(NULL)` SIGSEGV 打死节点
的缺陷**（两边注释几乎逐字相同）。**这个修复在本线属于 2PC 第 1b 步、不属于外部化，
2026-08-07 的回滚没有碰它**（已复核：`raft_consensus.c` 里逐列判 NULL 与
"拷出 SPI 上下文"两处都在）。合并时取 replay 侧的字段方案 + 本线的其余改动，
**不要**因为"两边都改了同一段"就整块选一边 —— 本线在同一文件里还有切主、
追平通道、DTX-2PC 的大量改动。

**#4 `partwal_sync.c` 是最难的一处冲突（replay +527 行，4.0 侧重写了 `PartWALFlush`）。**

本线在 `PartWALFlush` 里做的是正确性修复（§9.1 group-commit 让路窗口：提前返回路径
也必须调复制挂钩、触达集合改 per-backend 记录），replay 侧做的是 R2 事务层接线。
两者都在同一函数体内。**必须逐段人工合并**，且合并后要专门确认这条不变式仍成立：

> 动作 [A]（parwal 落盘 fsync + 复制到多数派）严格先于动作 [B]（pg_wal 提交 fsync），
> **且每一条返回成功的 prepare 都触发过复制挂钩**（含 group-commit 提前返回路径）。

这条不变式一旦破掉，症状是"PREPARE 返回成功但字节从未达多数派"，
测试**未必**当场变红（`DTX_2PC_DESIGN.md` §9.1 记录了它当初是怎么潜伏的）。

**#5 `CTRL` 分类在两边的状态相反。**

本线文档一直写"`PARTWAL_FLAG_CTRL` 是零使用点的预留位"——这在 4.0 分支上属实，
但 replay 侧 D1/D2 已经在用它承载 `FILESET_UPDATE` / `FREEZE_UPDATE`。
合并后所有"CTRL 是预留"的表述都不再成立（方案 docx 已按此调整，见 §15.5）。

### 15.3 冲突文件与处理原则

| 文件 | 冲突性质 | 原则 |
|---|---|---|
| `include/partition_wal_header.h` | 头格式 v2 vs v3、flags 位空间 | **取 replay 的 v3 头 + 本线的 DTX 位**，判定宏取并集（§15.2 #1/#2） |
| `src/wal/partwal_sync.c` | 双向大改同一函数 | 逐段人工合并，事后验 [A]<[B] 不变式（§15.2 #5） |
| `src/raft_consensus.c` | 双向大改 | 取 replay 的描述符字段方案，其余保留本线（§15.2 #4） |
| `sql/pg_partdist--1.0.sql` | R2 改了 `partwal_read_record` 的 OUT 列名 | 取 replay 侧。⏪ 外部化回滚后 SQL 侧**已无**依赖该列名的调用方，连锁改动消失 |
| `src/raft_boundary.c` | 两边都加了写入路径 | 取并集：DTX 记录写入（本线）+ CTRL 记录写入（replay） |
| `pg-install/`、`patches/` | 两边各自补过 | **取 replay 的 `4740239`**：它带 `scripts/check_pg_install_patched.sh` 前置自检，比本线 `468a518` 的做法更稳 |
| `pg-raft-src/reproduce-env.sh` | V6 断言 | 取 replay 的自适应版本（`e2a5a7d`），4.0 是 1c+8w 拓扑 |
| `docs/*.md` | 各自追加 | **全部保留双方内容**，不要相互覆盖 |
| 外部化相关（`raft_log_runs`、`pg_raft_entry_from_parwal`、hardstate v4） | — | ⏪ **不存在**：已整条回滚（§0.4）。若合并时冒出这些名字，说明选错了 revert 前的版本 |

### 15.4 合并后的验收门槛

按顺序跑，**任何一项不过就不要往下走**：

1. 编译：`pg_partdist` + `pg_raft` 干净编过（注意 `docker cp` 的 mtime 陷阱 —— cp 后必须
   `touch` 再 `make`，否则装的是旧 `.so`）；`make install` 之后**必须重启节点**。
2. `pg-raft-src/test_shard_identity_p0.sh` —— **10/10**（需环境里已存在分布式表；
   raft 套件末尾会 DROP 掉自建的分布式表，所以要先建一张再跑）。
3. `pg-raft-src/run-raft-tests.sh` —— **56/56**（1c+8w）。
   注意用 `CONTAINER=pg-citus-raft-container` 覆盖，默认值是 raft4 环境的容器名。
4. replay 侧的 `pg-partdist-src/tests/`：`test_lazy_replay_l1.sh`、
   `test_follower_replay_r1.sh`、`test_txn_layer_r2.sh`、`test_ddl_fileset_d1.sh`、
   `test_freeze_sync_d2.sh`、`test_local_wal_conflict.sh`。
5. **跨线联测（新增，两边都没有）**：一笔**跨分区 2PC 事务**提交后，
   follower 侧物理回放能把该事务的数据落成与 leader 逐字节一致的堆文件。
   这是两条线交汇处唯一没有现成用例覆盖的地方 —— DTX 记录会流经回放器，
   而回放器此前从未见过 `PARTWAL_FLAG_DTX`。

> 全部测试脚本**必须在宿主机跑**（脚本内部用 `docker exec`）。在容器里跑不会报错，
> 而是每条查询返回空串，表现为"30s 内没有 leader"之类的假失败。

### 15.5 文档与实现的已知偏差（合并方需知）

方案 docx（`兼容PostgreSQL的多分区高可用数据库方案.docx`）2026-08-07 已按实测校正过
副本同步相关章节（§2.3 整节、§2.2.2/2.2.4 记录格式、§2.4.1/2.4.3 提交点、§3 核心工作流）。

**校正的口径分两类，合并时不要搞混**：

| docx 位置 | 按哪个口径写的 | 说明 |
|---|---|---|
| §2.3 副本同步机制（整节） | **两条线共同的现状** | Raft 复制、多数派语义、fsync-then-ack、接收侧无独立进程 —— 合并后不变 |
| §2.4.1 / §2.4.3 / §3 提交点与工作流 | **4.0 侧现状** | 2PC 只在本线，合并后不变 |
| §2.2.2 CTRL 分类 | ⚠️ **合并后的目标态** | 写成"用于 DDL 文件集合映射同步 + 冻结水位同步"，这是 replay 侧 D1/D2 的实装形态；**在 4.0 分支上 CTRL 仍是零使用点** |
| §2.2.4 `version` = 3 | ⚠️ **合并后的目标态** | 4.0 分支当前是 **2**；合并 R2 之后才是 3 |
| §2.2.4 `gxid` 64 位 | ⚠️ **合并后的目标态** | 4.0 分支当前是 32 位 `xid`；合并 R2 之后才是 `gxid` |

即：**docx 已经按"合并完成后"的形态写好，合并前的 4.0 分支上有上述三处超前**。
这是有意为之——docx 是对外方案文档，不是分支状态快照，且原稿本来就写的 3.0/gxid。
若需要一份严格对应 4.0 分支现状的版本，把这三处改回 2 / 32 位 `xid` / "预留分类" 即可。

**另有两处 docx 描述属于设计态、当前两条线上都尚未实现**，合并时不要误当作已完成：

- **TSO**：全文多处出现 `start_ts` / `commit_ts` 的申请流程。项目里没有 TSO 服务，
  `DtxRecord.commit_ts` 是恒传 0 的预留字段。
- **§2.4.2 / §2.4.4 的全局 MVCC 与增强型 CLOG**：replay 侧 R2 已落地
  `enhanced_clog.c`，但与 TSO、全局快照的接线未做；docx 里"仅当满足…才可见"的
  判定式仍是设计描述。

这两项归 MVCC/事务处理分工，本线未触碰。


---

## 15.6 合并的实际执行记录（2026-08-07/08，事后补记）

§15.1–§15.5 是合并**之前**写给执行者的交接。合并已于 2026-08-07 完成，本节记
实际发生了什么 —— 与预判不符的地方尤其值得看。

### 与预判一致的

§15.2 的必炸清单基本都命中了：记录头的 flags 位空间（取 replay v3 的 64 位
`gxid` + 本线的 `PARTWAL_FLAG_DTX`，判定宏取并集）、`partwal_sync.c` 的双向大改、
`raft_consensus.c` 描述符字段、`raft_boundary.c` 取并集，处理原则都按 §15.3 执行。

### 预判之外的（四个运行期缺陷，全部是合并缝隙或合并放大）

门禁全绿之后做定向审查才发现的，**没有一条会被当时已有的用例抓到**：

1. **DTX 记录被喂进 `rm_redo`**（§15.4 第 5 项点名的两线交汇盲区，实锤）。
   `shard_replay.c` 五阶段回放的分派处，非 MARKER/CTRL 一律落进
   `ApplyDataRecord`；DTX 载荷是 `DtxRecord` 不是 `XLogRecord`，且它的 rmid
   恰好是 `RM_XACT_ID`，看起来像正常 xact 记录。已补显式 DTX 分支（只推游标）。
2. **`partition_lsn` 重号（最严重，实测复现）**。`AppendDtxRecord` 与
   `partwal_follower_append` 是仅有的两个**不持 `PartWALCtl->lock`** 就写段文件的
   事务外写入者，而 plsn 由**磁盘 checkpoint 播种、进程私有内存 +1** 分配 ——
   两个写者读到同一份 checkpoint 就发同一个号，`expected==0` 走自增分支、
   重号校验碰不到，**静默**。实测：一张 2 分片表上并发跑 120 笔跨分片 2PC +
   120 笔单分片 INSERT，604 条记录只有 582 个不同编号（22 条重号，每条都是
   "一条 DTX + 一条 DATA"）。后果是按编号回读只返回首条，另一条永不进 Raft ⇒
   **副本静默分歧，流里没有空洞、没有任何报错**。修法是把两者纳入同一把锁；
   复跑同一脚本 846 条记录、846 个不同编号、0 重号。
3. **`setup-raft.sh` 把 4.0 旧签名覆盖装回集群库**。`ensure_boundary_functions()`
   仍按 `(OUT xid, OUT flags)` / `(p_xid, p_data, p_flags)` 重建，而同脚本的
   `DO $mig$` 会先 DROP 掉正确的 3.0 声明、`CREATE EXTENSION IF NOT EXISTS`
   又补不回来。踩到即 `data_propose_one` 报 `column "gxid" does not exist`
   ⇒ **整个数据组 propose 全线中止**；`follower_append` 因参数类型表不同会形成
   **重载**而非替换，走旧重载会把 bytea 指针当 gxid 读 ⇒ 段错误。
   **危险在于 `run-raft-tests.sh` 在缺 pg_raft 或未收敛时会自动调它** ——
   一次普通测试就能毒化集群。已改回 3.0 布局并新增 `DO $readd$` 把重建的函数
   认回扩展（否则留成游离对象，下次 DROP/CREATE EXTENSION 循环整体失败）。
4. **`TruncatePartWALTo` 把 checkpoint 的 `last_wal_lsn` 写成 0**。截断扫描无条件
   `last_kept_lsn = rec.orig_lsn`，漏了写入侧同场景的 `orig_lsn != Invalid` 守卫。
   2PC 负载下流尾常年是 `orig_lsn=0` 的 DTX 记录，切点落上面即清零 ⇒ 段号错乱
   （§9 里 2026-08-04 修过的老缺陷复活）+ `DemuxCrashRecovery` 跳过 WAL 重扫描。

另有一条**已知未修**：`PartWALFlush` 的 drain 曾按环槽下标序分配 plsn，跨环回绕
时更新的记录会拿到更小的编号 —— 这条已于 `7711bc1` 改为按 `(orig_lsn, start_lsn)`
排序后落盘。

### 三处测试判据在合并后过期（非产品缺陷，但会误导）

- `raft_19` B 段断言"leader 侧所有记录 flags 都是 1" —— R2 起每笔事务尾部有
  COMMIT MARKER（flags=2），已改为 `flags ∈ {1,2,4}` + 另断言确实存在 flags=1；
- `raft_22` C 段用旧列名 `xid` 查 `partwal_read_record`（parwal-3.0 已改名 `gxid`），
  取值恒为空串；后来又因 §12.2 新增的两段式标记，其 kind 序列判据从
  `...,1,3` 变成 `...,1,0,3,0`，已改为校验 **DTX 子序列**；
- `test_shard_identity_p0` [C] 假设"同一 reference 分片在各节点的本地 OID 互异" ——
  对称新建集群的 OID 计数器几乎同步，互异只是有机分化后的偶然现象，已改为
  对照 `pg_class` 真值。

**教训**：`psql -Atc` 执行多语句时会把 `SET` 等命令标签一并打进输出，取值必须
`tail -n1`；否则拿到的是标签而不是结果，且断言会以一种看起来合理的方式失败。

### 运维上的两个新坑

1. **探针留下的 `pg_dist_transaction` 积压会打挂 `raft_21`**。本环境按 DTX 设计
   关掉了 Citus 原生 2PC 恢复（`citus.recover_2pc_interval = -1`），所以每笔跨分区
   事务都在表里留一行，只能靠 `partdist.dtx_gc_dist_transaction()` 收。一次 240 笔
   的并发探针攒到 818 行，超过 `raft_21` [I] 的有界循环上限（6 轮 × 128 = 768 个
   候选），该用例遂失败。**跑完任何 2PC 压力探针，循环调 GC 到返回 0 再跑 raft 套件。**
2. **新增记录类型会打破按 kind 序列/记录条数断言的旧用例**（上文 `raft_22` 即此）。
   凡是按条数或整串序列断言的脚本，加新记录类型时都要扫一遍。

---

## T7.20（P7-R1）成员变更的安全路径 —— 2026-09-12，在库

**状态：在库**（未回退）。改动集中在 `src/raft_consensus.c` + `sql/pg_raft--1.0.sql`。

### 改之前是什么样

改一个组的成员集只有一条路：在**每个节点上各自**重调
`pg_raft_group_create(gid, 新成员集)`。这条路没有任何协调 —— 变更期间不同节点
持有不同的成员集，于是同一个组在不同节点上有**两套不相交的多数派定义**，
Leader Completeness 失去交集保证。这与 `group_membership_known()` 头注释里记的
那次 hearsay 事故是同一个形态，只是换了触发方式：那次是"空成员集被当成全体"，
这次是"变更中途两边不一致"。

### 选的是哪条路，以及为什么不是 joint consensus

走 **Raft 论文 §4.1 的单节点变更**（一次只加或只减一个），不做 joint consensus。

- 安全性来源：单节点变更下，相邻两个配置的多数派**必然相交**（只差一个成员，
  两个多数派不可能不相交），因此不会出现两个互不相交的多数派各自选出 leader。
- joint consensus 只有在需要"原子地换掉多个成员"时才必要，而实现面大一个数量级
  （两阶段配置、两套多数派同时生效、C_old,new → C_new 的接力）。
- 在一个全簇共识层上，这个取舍偏向**能审查得过来**的那一边。代价是"三换三"
  要做三次。

### 机制

1. 新条目类型 `OP_CONFIG`，载荷 `{"members":[a,b,c]}`（jsonb 列收得下）。
2. **append-time 生效**（Raft 原始规则）：条目一进日志就按新成员集算多数派，
   leader 与 follower 同一规则。等提交再生效是不安全的 —— 提交这条 CONFIG
   本身所需的多数派会仍按旧集算。
   · leader 侧因此必须在 apply 之后**重算 majority**，沿用调用开头那个旧值
     会出现"按旧集算够票、按新集其实不够"的提交。
3. **同时只允许一个未提交的变更**，于是未决状态只需记一条：
   `pending_cfg_index` + `committed_members[]`（回滚目标）。
4. 日志截断（冲突截断 / leader 侧 `discard_uncommitted_entry`）时按
   `pending_cfg_index > new_last` 回滚成员集。不回滚的后果不是"多算一个成员"，
   而是本节点与其它节点对多数派的定义不一致。
5. 提交后落 `partdist.raft_group.members`（重启恢复成员集的来源）。

### 实测踩到的两处，都记在代码注释里

**① 注册表恢复路径会把日志覆盖掉。** `restore_groups_if_needed()` 的
`groups_restored` 是**每后端**静态量 —— 每开一个新 backend 就会拿注册表那一行
重置 shmem 成员集。成员变更刚提交，紧接着一条普通查询就把它打回旧集，**一声不响**。
症状是"加成员报成功、隔一会儿又变回去"，看起来像共识没生效。
修法：`raft_group_ensure()` 里加 `config_from_log` 门禁 —— 注册表是**重启**恢复的
来源，不是运行期的权威；运行期的权威是日志，两者只在 apply 那一刻对齐。
另外定案时立刻落注册表，不等 apply（apply 只在拿到认领的那个 backend 上推进，
follower 未必及时跑到；落后一拍 = 重启后恢复出旧成员集）。

**② 被移除的节点收不到"把它移除"的那条记录。** leader 在 append 那一刻就不再把
它当成员，按朴素实现它从此收不到任何 AppendEntries，于是**永远以为自己还在组里**，
继续按本组的选举超时反复竞选 —— 论文 §4.2.2 点名的 "removed server disruption"。
修法：变更未提交期间，`peer_in_group()` 让**旧成员集里的节点继续收**；
这只放宽"发给谁"，多数派仍只看新成员集。

### 验收

`tests/test_raft_membership_r1.sh` **18/0**，已并入门禁。关键断言不是"接口返回
成功"，而是**各节点对规模的看法同步变化**（`cluster_size` 逐节点比对）——
"两套多数派定义"只有在这个观测面上才看得出来。另含四道门禁的负向用例
（非 leader / 加已在的 / 减不在的 / 减到空）。

### ★ 上线时被 A/B 对照抓出来的一处性能回归（已修，务必记住）

第一版把 `raft_config_note_commit()` 无条件挂在两条**最热**的路上 ——
每一次 `group_propose`（提交路径上，每条 parwal 记录一次）和每一次
`pg_raft_append_entries`（每个心跳每个组一次）—— 而它一进去就抢
`RaftGroups->mutex`：那是**全局**的组表自旋锁，32 个组、全部 backend 共用。

在这台 2 核机上，把一把全局自旋锁压进提交路径的后果不是"慢一点"，而是
**心跳被挤掉、选举频繁触发**：

| 套件 | 改动前 | 第一版 | 加无锁快路径后 |
|---|---|---|---|
| `replica_gate_p6` | **33/0** | 23/10、32/1（两轮） | **33/0** |
| `promote_p6` | 39/0 | 37/2（一轮） | **39/0** |

症状极具迷惑性：「分区组 leader 就位」刚断言通过，紧接着的本地写入就被
`本节点不是该分区组的 leader` 拒掉 —— 看起来像切主逻辑坏了，其实是
leader 在这两句之间真的换了人。

**定因办法**：`git stash` 掉 raft 改动、重编、跑同样两套做 A/B。这比继续读代码
有效得多 —— 我当时已经把每一处新增调用都论证成"非 CONFIG 路径上等价"，
论证本身没错，错在没考虑**锁本身的代价**。

修法：`pending_cfg_index` 只在持锁时被写，**无锁读它做早退判据是安全的** ——
读到 0 就直接返回，不进锁；真有未决变更时读到非 0，再进锁复核。成员变更是
罕见操作，慢路径多一次锁毫无代价。`raft_config_rollback()` 同样处理。

**可复用的教训**：给共识层加钩子时，"逻辑上等价"不等于"代价上等价"。
凡是挂在提交路径或心跳路径上的新代码，**默认路径必须无锁**。

### 同批复核里另一条：`handover_provision_p7` 改前改后都红

改前 25/8、改后 29/4 —— **不是本次改动引入的**，与 P7-D3（半删分片把分布表
锁死在协调者上）的已知形态一致，该套件的注释里也记着同一个坑。单列不混算。
