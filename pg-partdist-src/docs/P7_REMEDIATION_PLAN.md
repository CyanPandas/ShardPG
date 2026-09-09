# P7 补足计划：把 P6 审计出的缺陷逐条收口（2026-09-09）

> **这份计划怎么来的**：`P6_EXIT_AUDIT.md`（2026-09-06 全项目缺陷盘点）+
> `P6_PENDING_DECISIONS.md`（2026-09-07 待裁清单，含演示实跑新撞出的 7 条）。
> 本文在 **2026-09-09 逐条复核代码之后**成文 —— 审计给的是"当时的现状"，本文
> 给的是"**今天的现状 + 接下来怎么补**"。复核结论见 §1 的"09-09 复核"列。
>
> **执行环境**：容器 `pg-test-container`（1 coordinator + 8 worker），工作区
> `~/shardpg-test-work`，分支 `shardpg-test`。**不碰 `shardpg-TX2` 与演示材料**
> （见根目录 `PG_TEST_ENV.md`）。
>
> **一句话结论**：P6 没有结项，而且**结不了**——三条 ❌ 里有两条是"出口动作
> 没做"（全量没跑、裁定没下），第三条是文档没对齐（本次已补，见 §5）。真正的
> 产品缺陷有 **21 条**，其中 **4 条是"已提交的数据在切主后不可见/丢失"级别**，
> 必须先修，且都在 pg-partdist 侧、不需要解冻 raft。

---

## 0 口径与优先级依据

排序只用一个判据：**这条缺陷会不会让已经提交成功的数据变得不正确或不可见**。

| 级别 | 定义 | 处置 |
|---|---|---|
| ★★★ | 已提交数据在正常运维动作（切主/供给）后**不可见或丢失**，且不自愈 | 批次 1，先于一切 |
| ★★ | 集群进入**不可用/不可恢复**状态（节点停摆、资源耗尽、分片永久失去 HA） | 批次 2 |
| ★ | 限制面/守卫面有洞：能绕过去，绕过去就踩上面两级 | 批次 3 |
| ◐ | 出口动作、门禁、文档 | 批次 4 |
| ○ | 生产化缺件（自动启动器、截断），有手工替代 | 批次 5 |
| ⏸ | 需要解冻 raft 或属独立专项 | 批次 6，须用户裁定 |

---

## 1 缺陷总账（21 条产品缺陷 + 8 条工程债）

### 1.1 ★★★ 数据正确性（批次 1）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **R-P6-15** | **2PC 阶段 3 的 COMMIT 标记不带分片 xid**：`dtx_participant.c:467` 在 `COMMIT PREPARED` 里调 `PartWALBuildMarkerPayload()`，而该函数按 `ShardXidXactCount() > 0` 决定带不带分片 xid 尾（`partwal_sync.c:723-727`）——`COMMIT PREPARED` 跑在另一个没碰过分片表的事务里，计数恒 0 ⇒ 24 字节旧格式 | **未修**（代码原样） | 副本分片 clog 对每笔跨分片事务**永远停在 PREPARED**；决议被 FORGET 回收后 `dtx_close_indoubt` 四级全落空 ⇒ **切主后 2PC 提交的行在新主上永久不可见**（09-06 实跑取证） |
| **R-P6-17** | **物理基线不搬分片 clog**：`shard_fileset.c` 全文零处引用 `pg_shard_clog`/`ShardClog`，`shard_baseline_emit` 只灌页面 + 抬发号水位 | **未修**（grep 零命中） | 在已有数据之后才供给的副本，对基线之前提交的分片 xid **没有判决**；升主后那些行是 RUNNING ⇒ 被 `shard_claim_on_promote` 改判 ABORTED ⇒ **丢行** |
| **R-P6-16** | **升主不发 `FILESET_UPDATE`**：`PartDistRoutePromote()`（`shard_fileset.c:1838`）只做"角色 + 捕获"两件事，不广播新主的 relfilenumber | **未修**（函数体 12 行，无 CTRL 发射） | 其余副本 locmap 仍对着旧主文件号 ⇒ `replay_catchup` 报"未知 relfilelocator" ⇒ **切主一次，该分片其余副本全部失去再次当选资格**，直到从新主重新供给 |
| **R-P6-21** | **供给/升主不携带"打标身份"**：`shard_fileset.c`/`raft_boundary.c` 均无 `ShardMvccSetAdd`；`mvcc_set` 只由 `partdist_set_shard_mvcc()` 或重启扫目录装载 | **未修**（grep 零命中） | 升主后的新主若未事先手工加白名单又未重启，**写入不打标、读走原生路径** —— `handover_provision_p7` [3b] 与 `promote_catchup_tx3` [4] 都是在这个状态下通过的，**通过的原因是错的** |

### 1.2 ★★ 可用性 / 资源（批次 2）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **R-P6-19** | 回放 worker **泄漏目录描述符**：`exceeded maxAllocatedDescs (328)`，某条 `AllocateDir` 路径缺 `FreeDir` | 未修（六个文件里 `AllocateDir`/`FreeDir` 计数配平，说明泄漏在**异常提前返回**路径上，需逐条走查） | 该节点此后**所有回放失败**，直到 worker 重启 |
| **R-P6-20** | TSO 客户端 RPC 用裸函数名：`tso_client.c:416/442/454/502/556` 发 `SELECT partdist_tso_start_ts(...)`，而函数装在 `partdist` 模式 | **未修**（5 处原样） | 协调者默认 `search_path` 不含 `partdist` ⇒ 取号/心跳/commit_ts/safe_ts 全部 `function does not exist` ⇒ **全簇分片写 fail-closed**。演示环境靠 `ALTER DATABASE ... SET search_path` 绕过 |
| **R-P6-4** | 分配器 shmem 槽位 `SHARD_XID_MAX_SLOTS = 64`/节点，**无产品侧回收**：`shard_xid.c` 零处释放路径 | 未修（`include/shard_xid.h:30`；grep 释放零命中） | 建删 64 个分片后该节点**再也建不了分片**；门禁靠"移走水位文件 + 重启"规避（`run_p6_exit.sh:165-170` 直接 `mv`） |
| **P7-D1** | **leader `DROP TABLE` ⇒ 副本侧静默**：`shard_fileset.c:929-933` 明写"属后续工作；这里保持沉默" | 未修 | 壳表 + 回放槽位 + `pg_parwal/<oid>` **永不回收**（回收判据是"OID 不在本地 `pg_class`"，而壳表是本地真表） |

### 1.3 ★ 限制面 / 守卫面（批次 3）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **P7-G1** | §9.2 第 3 层禁用清单**漏 8 个同类 Citus UDF**：`citus_split_shard_by_split_points`、`isolate_tenant_to_new_shard`、`citus_drain_node`、`master_move_shard_placement`、`master_copy_shard_placement`、`replicate_table_shards`、`citus_schema_move`、`alter_table_set_access_method` | 未修（`shard_guard.c:50-80` 清单实为 7 个 citus UDF + 5 个逻辑解码入口） | 它们同样亲手搬/读分片数据，绕过去就是静默错读或与 raft 放置冲突 |
| **P7-G2** | **引用表运行期写：零守卫、零断言**，DESIGN §10 那行还挂着"【需核实现状】" | 未修（`shard_guard.c` 零命中 `reference`） | §10 写"建表后只读"，实际拦不住 |
| **R-P6-18** | MARKER 的 `start_ts` 是墙钟（`partwal_sync.c:733` `GetCurrentTransactionStartTimestamp()`），不是 TSO start_ts | **未修** | R-P3-2「双 ts 宇宙串线」成真：升主后 §4.2 三态处置拿它与 TSO 快照比，**恒为"跳过"** |
| **P7-G3** | DESIGN §10 缺 4 行、错 1 行 | **本次已补**，见 §5 | — |

### 1.4 ◐ 出口动作 / 门禁（批次 4）

| ID | 事项 | 09-09 复核 |
|---|---|---|
| **P7-E1** | **31 套全量在最新二进制上一次没跑过**（`run_p6_exit.sh` `SUITES` 实数 31；最近一次全量是 T6.8 的 28 套 1191/31，其后批次 #7–#11 改了回放 redo、路由层、`promote_prepare`、整个 PG 二进制、发号起点） | 未跑。出口清单原文的"29 套"口径也已过时 |
| **P7-E2** | OPS 8 套按 3 节点布局写，9 节点上会停错节点且不复原 | 未改造，默认不进门禁 |
| **P7-E3** | 三个基线消费者 e2e：初始配对 ✓、永久分叉 ✓、**快路径分叉 ✗**（`test_fastpath_divergence_tx4.sh` 只验到 `promote_prepare` 返回 -1，无重做基线步骤） | 未补 |
| **P7-E4** | `replica_gate_p6` **没有施加 anti-wraparound 压力**，验的是"relfrozenxid 同步 + 闸门拦截" | 命题已被 T6.3c 改写；清单原文的实验未做 |
| **P7-E5** | R-P6-13 拓扑抖动"等不到只告警不阻塞"，批次里带着"60s 未收敛、存疑"跑完 | 未改判据 |

### 1.5 ○ 生产化缺件（批次 5）

| ID | 事项 | 09-09 复核 |
|---|---|---|
| **P7-V1** | 分片 vacuum **无自动启动器**，到龄只 WARNING（`shard_xid.c:1381`） | 未做 |
| **P7-V2** | 分片 vacuum **无尾部截断**（`shard_vacuum.c` 无 `smgrtruncate`/`RelationTruncate`） | 未做 |
| **P7-V3** | 两处覆盖缺口：clog 整段删除分支；停在页面循环中间的崩溃 | 未做（需生产路径故障注入点） |
| **P7-V4** | 打标登记全手工（`partdist_set_shard_mvcc` 逐分片逐节点，无建表钩子/事件触发器） | 未做；与 R-P6-21 同源，一并做 |

### 1.6 ⏸ 需解冻 / 独立专项（批次 6，**须用户裁定**）

| ID | 事项 | 为什么单列 |
|---|---|---|
| **P7-R1** | Raft **成员变更无安全路径**（无 joint consensus）⇒ 连带 §9.2 禁掉全部 Citus 搬运器 ⇒ **集群拓扑事实上不可变更** | 改 `pg-raft-src` 核心，触碰冻结模块 |
| **P7-R2** | `RAFT_MAX_GROUPS = 32` 定长 shmem ⇒ 每节点 ≤ 31 个数据组；且"给一张分布表的全部分片建组 + 供副本"无自动化 | 同上；前置是日志外部化（已整条回滚） |
| **R-P4-13** | apply 侧决议登记缺失，仅 `dtx_peek` 绕行（`pg_raft--1.0.sql:146, 1349`） | 根因在 pg_raft |
| **P7-R3** | 物理基线**上限 1 GB**（`fileset_inline_max_blocks = 131072`，超出显式 ERROR）⇒ 大于 1 GB 的分片既不能供给也不能修复分叉 | 需流式基线，独立专项 |
| **P7-R4** | leader 任何一次 DDL ⇒ follower 停在结构栅栏，需人工等价 DDL + 重跑 `replay_set_locmap` | 需 DDL 自动跟随通道，独立专项 |
| **R-P6-14** | 逻辑解码禁令只覆盖 SQL，walsender `START_REPLICATION` 绕得过 | 真堵要在 `LogicalDecodingProcessRecord` 加判据 = 内核补丁面 |

### 1.7 工程债（穿插在各批次里做，不单列批次）

`write_router.c` milestone 1.2 死代码仍编入并暴露为 UDF；`kill(pid,0)` 探活在回放侧
仍在用（raft 侧已因僵尸失真而弃用，两处口径不一）；改共享结构体后增量 make 不可靠
（R-P6-7 根因）**无自动守卫**；`REPLAY_MAX_SHARDS = 64` 定长槽位；单事务 DROP > 16 张
打标表即 ERROR；`dtx_close_indoubt` 四级落空无告警面；GlobalSafeTs 被钉死无监控面；
根 `README.md` 仍是 2.0 三节点文档（**本次已补导读**，见 §5）。

---

## 2 批次分解

### 批次 1：数据正确性四条（★★★，最高优先）

**入口条件**：`pg-test-container` 就绪；不需要解冻 raft（四条全在 pg-partdist 侧）。

| 任务 | 修法 | 验收 |
|---|---|---|
| **T7.1** R-P6-15 COMMIT 标记带分片 xid | `COMMIT PREPARED` 路径不能靠 `ShardXidXactCount()`——它跑在别的事务里。改为**从 2PC 状态文件 / 参与登记里取回该分片在 PREPARE 时用的分片 xid**，显式置 `PARTWAL_MARKER_HAS_SHARD_XID` 并回填。注意 `PartWALMarkerSetShardXid` 是逐分区回填的，取值要按分区来 | 新套件：跨分片 2PC 提交 → 副本 `sclog_full` 该 xid = COMMITTED（不是 PREPARED）→ 杀主 → 新主上那两行**可见**。并入 `dtx_replay_tx1` 的切主联测 |
| **T7.2** R-P6-17 基线带分片 clog | 二选一：① 基线 CTRL 增子类型，随页面一起搬 `pg_shard_clog/<oid>` 段；② 供给流程里单独传 clog 文件并在落地前校验。**推荐 ①**（与 D1 的 fileset 灌注同源，且天然覆盖"修复分叉"消费者） | 新套件：先写 40 行 → 再供给副本 → 副本 `sclog_full(基线前的 xid)` 有判决 → 升主后 40 行**全部可见**（现状是丢） |
| **T7.3** R-P6-16 升主发 `FILESET_UPDATE` | `PartDistRoutePromote` 用 D1 机制发 `FILESET_UPDATE{old = 本地 locmap 里旧主的文件号, new = 本地 fileset}`。注意发射点要在"捕获登记"之后、对外服务之前 | 扩 `handover_provision_p7` [3b]：不止断言"另一副本**收到**新主的记录"，要断言**回放成功**（`replay_catchup` 无"未知 relfilelocator"），且该副本随后**仍有当选资格** |
| **T7.4** R-P6-21 + P7-V4 打标身份随供给/升主走 | `provision_shard_replica` / `PartDistRoutePromote` 里 `ShardMvccSetAdd` + 落白名单；顺带把"建分布表即自动打标"接到事件触发器或 `create_distributed_table` 挂点上 | 新套件：**不手工加白名单、不重启**，供给 → 升主 → 新主写入**走打标路径**（`route_status` 显示 promoted + captured，写入带分片 xid）。同时把 p7 [3b] / tx3 [4] 的"通过原因错了"这一层补上断言 |

**批次出口**：四条各有新套件；`dtx_replay_tx1` / `promote_catchup_tx3` /
`handover_provision_p7` / `dtx_commit_marker_tx2` 四套回归零 FAIL。

### 批次 2：可用性与资源三条 + DROP 回收

| 任务 | 修法 | 验收 |
|---|---|---|
| **T7.5** R-P6-20 TSO RPC 加 schema 前缀 | 5 处 RPC 文本改成 `partdist.partdist_tso_*`。**一行改，收益最大**：修完演示文档里的 `ALTER DATABASE ... SET search_path` 绕法可以删掉 | 协调者 `search_path` 恢复默认后，取号/心跳/commit_ts/safe_ts 全通；`tso_si_p3` 单跑绿 |
| **T7.6** R-P6-19 目录 fd 泄漏 | 逐条走查六个文件的 `AllocateDir` 调用点，找异常提前返回（`continue`/`ereport(ERROR)`/早 `return`）没配 `FreeDir` 的那条；修完顺手把这类路径统一成 `PG_TRY` 或 `on_proc_exit` | 新用例：同节点连续 50 轮 `replay_catchup`（三槽位），`maxAllocatedDescs` 不涨 |
| **T7.7** R-P6-4 分配器槽位回收 | 判据与 R-P6-9 的 `ShardMvccSetRemove()` 同源：DROP 提交时摘 OID，这里同时**放槽位 + 删水位文件**。做完把 `run_p6_exit.sh:165-170` 的 `mv` 净场层撤掉 | 新用例：建删 100 个分片，槽位占用回到基线；撤掉净场后门禁仍绿 |
| **T7.8** P7-D1 leader DROP 的副本侧回收 | 新 CTRL opcode `FILESET_DROP{oid}`：follower 收到后停流、摘槽位、删壳表与 `pg_parwal/<oid>`。**注意**：不能沿用"OID 不在本地 pg_class"判据（壳表是本地真表） | 新用例：leader `DROP TABLE` → 副本壳表消失、槽位释放、目录回收；未收到 CTRL 的副本保持现状（不误删） |

### 批次 3：守卫面与限制面

| 任务 | 修法 | 验收 |
|---|---|---|
| **T7.9** P7-G1 禁用清单补 8 个 UDF | 加名字进 `shard_banned_funcs[]`。注意 `citus_drain_node` 内部直接调 C 函数、不经 ExecutorStart，**加名字挡不住**——要么额外挂点，要么在 §10 显式记为"挡不住，靠规程" | `negative_p6` 每个新名字一条负向断言 |
| **T7.10** P7-G2 引用表运行期写 | 先裁定"拦"还是"接受"（§3）。若拦：在 `shard_guard` 里按 `pg_dist_partition.partmethod='n'` 判引用表并拒写 | 拦则一条负向断言；接受则 §10 改写为"不拦，靠规程"并给出后果 |
| **T7.11** R-P6-18 MARKER 用 TSO start_ts | `PartWALBuildMarkerPayload` 改用 TSO 的 start_ts。**注意兼容**：旧流里的墙钟值要能被识别（加 flag 位或按量级判别），否则升主后三态处置会把老记录判错 | 副本 PREPARED 槽 `sts` 与 leader TSO 同宇宙；§4.2 三态处置第一支不再恒"跳过" |

### 批次 4：出口动作（做完前三批再跑，否则白跑）

| 任务 | 内容 |
|---|---|
| **T7.12** 全量 31 套一次跑完 | 跑前：确认宿主内存（`pg-citus-tx2-container` 必须停）、磁盘（当前 / 已用 93%，剩 7.1 G，**先清 tx2 容器 68.5 GB 可写层**）。最坏 9.5 h，建议分 P 段跑并逐段记录 |
| **T7.13** OPS 8 套去向 | 改造成拓扑无关（按 `pg_dist_node` 动态取节点）并入门禁，或正式退役并在文档写明。**须裁定** |
| **T7.14** P7-E3 tx4 补基线闭环 | 给 `fastpath_divergence_tx4` 补"重做基线 → 重新参选 → 当选"一段，闭合"三个基线消费者各一条 e2e" |
| **T7.15** P7-E4 replica_gate 压力 | 要么补真实 anti-wraparound 注入，要么把出口清单原文改成 T6.3c 改写后的命题。**须裁定** |
| **T7.16** P7-E5 拓扑抖动判据 | "等不到就阻塞"而不是"只告警"，消灭"批次红、单跑绿"的伪信号 |

### 批次 5：生产化缺件

`T7.17` 分片 vacuum 自动启动器（P7-V1）；`T7.18` 尾部截断（P7-V2）；
`T7.19` 两处覆盖缺口的故障注入点（P7-V3）。三条都是"生产不可用的硬伤"里
最不紧急的一档 —— 有手工替代，但没有它们不能说交付。

### 批次 6：需裁定的专项（不在本期默认范围）

P7-R1 成员变更、P7-R2 组数上限与建组自动化、R-P4-13、P7-R3 流式基线、
P7-R4 DDL 自动跟随、R-P6-14 内核层堵解码。**每一条都要么解冻 raft、要么是
独立专项**，须用户明确批准范围后才开工。

---

## 3 需要你裁定的 6 条（其余我按上表默认执行）

| # | 要裁的 | 我的建议 |
|---|---|---|
| 1 | **批次 6 是否开工**（成员变更 / 组数上限 / R-P4-13 三条要解冻 `pg-raft-src`） | 本期**不开**；先把批次 1–4 做完，拿到一次干净的全量再谈。但要知道：不做 P7-R1，**集群拓扑就是不可变更的**，这是"能不能上生产"级别的限制 |
| 2 | **引用表运行期写**：拦 / 接受 | 拦。零守卫 + §10 写着"只读" = 文档在骗人 |
| 3 | **OPS 8 套**：改造进门禁 / 退役 | 改造。它们验的是运维动作（停节点、切主），恰恰是本期缺陷最集中的面 |
| 4 | **`replica_gate_p6`**：补真实回卷压力 / 接受改写后的命题 | 接受改写。真实 anti-wraparound 注入代价高，而 T6.3c 的命题（relfrozenxid 同步 + 闸门）逻辑上更强 |
| 5 | **全量 31 套什么时候跑**（最坏 9.5 h，且要先腾磁盘） | 批次 1–3 做完之后跑一次，别现在跑 —— 现在跑出来的数字，改完就作废 |
| 6 | **`citus_drain_node` 这类"加名字挡不住"的** | 记进 §10 的"挡不住，靠规程"，不假装拦住了 |

---

## 4 出口标准（P7 什么时候算完）

- [ ] 批次 1 四条各有**新套件**，且"切主后已提交数据可见"这条命题有端到端取证
- [ ] 批次 2 四条修完，`run_p6_exit.sh` 的净场层（`mv` 水位文件）可以**撤掉**且门禁仍绿
- [ ] 批次 3 三条修完，§10 每条限制各有一条负向断言（"挡不住"的显式写明）
- [ ] **31 套全量一次跑完、零 FAIL**，且是在最终二进制上跑的
- [ ] "不做的事"九行 + 本文 §3 六条**逐条有裁定记录**，无一以挂账形态留存
- [ ] 三份设计文档（DESIGN / DEV PLAN / FRD）与代码逐条对齐（本次已做一轮，见 §5；
      批次 1–5 每改一处代码同步回填）

---

## 5 本次（2026-09-09）已完成的文档回填

审计 §四列的 10 处不对齐，本次逐处复核后回填：

| # | 位置 | 处置 |
|---|---|---|
| 1 | `TX_TSO_MVCC_DESING.md:12` 头部"未实施，未动任何代码" | **已改**为真实实施状态 + 指向本文 |
| 2 | `FOLLOWER_REPLAY_DESIGN.md` §10 `PartDistRoutePromote` 签名 | **已改**为实装签名 `(Oid)` + 说明水位为何不进签名 |
| 3 | `FOLLOWER_REPLAY_DESIGN.md:1101` "仍未实装：路由层本身" | **已改**：批次 #10 已实装；并就地记下它仍缺 `FILESET_UPDATE`（R-P6-16） |
| 4 | `FOLLOWER_REPLAY_DESIGN.md` §14.2 "R4 硬阻断于 R3" | **已改**：新宇宙（打标分片）不再依赖 R3，只剩遗留宇宙 |
| 5 | `FOLLOWER_REPLAY_DESIGN.md` §13 约束 13 "pg_raft 侧修法已整体回退" | **已改**：T4.4 已重放 #39，`wait_for_log_room` / `apply_owner_pid` / `flow_stats` 都在当前代码里（实测命中 4/11/13 处） |
| 6 | `raft_module_revision_plan.md` §0.2 "尚未做的"表 | **已改**：#1/#2/#4/#7 四行已被取代或已做，逐行标注 |
| 7 | `TX_TSO_MVCC_DEV_PLAN.md` "不做的事"表 | **已改**：补"实况（09-09 复核）"列；分片 clog 物理删除已做、Proxy 已被 MX 取代 |
| 8 | `TX_TSO_MVCC_DESING.md` §10 | **已补** 4 行（子事务禁写、打标后禁 CREATE INDEX + LP_REDIRECT、回卷未实现、物理基线 1 GB 上限），**已改** 2 行（Citus 路由写、引用表） |
| 9 | `P6_PRECHECK.md:226` R-P4-20 "★ 未修" | **已改**为"已处置（遏制层 + 基线工具），无根除" |
| 10 | 根 `README.md` | **已补**导读段：当前形态（9 节点 / TX-TSO-MVCC）、门禁入口、文档地图 |

**审计 §四之外，本次顺带查出并回填的 5 处**（都是"做完没回填"或"状态已过时"）：

| 位置 | 处置 |
|---|---|
| `DTX_2PC_DESIGN.md` §9.6 标题「机制已落地，**接线未做**」 | **已改**：`pg_raft--1.0.sql:539` 的升主路径里有 `PERFORM partdist.dtx_close_indoubt(...)`，且顺序正确（排在 `shard_claim_on_promote` 之前）。**接线做了** |
| `DTX_2PC_DESIGN.md` §9.5「(a) 与归队规则仍未实装」 | **已细化**：归队规则做了一半 —— 分叉**检测**已落地，**重做基线仍全靠人工** |
| `P6_PRECHECK.md` 结论段「23 条里开口的只有三条」 | **已改**：今天是"一条半"——R-P4-20 已处置、R-P4-22 已定性，只有 R-P4-13 完全开口 |
| `P6_PRECHECK.md` 附表 R-P4-22 行「★ 新观测、未查」 | **已改**为已定性（`ring_full_drops` 归零、`quorum_drops` 未消除） |
| `FOLLOWER_REPLAY_DESIGN.md` §13「`flow_stats` 不存在时打跳过」 | **已改**：`flow_stats` 已在库，`health_check_no_drops` **真正生效**。⚠️ 连带一条要复核的：此前"全 PASS"的轮次里，丢弃断言到底跑没跑过 |

另外：`P6_PENDING_DECISIONS.md` 附录里的 7 条演示缺陷，本次**正式登记进
DEV PLAN §5 风险登记簿**（R-P6-15 ~ R-P6-21），不再只存在于附录里；
`P6_EXIT_AUDIT.md` 与 `P6_PENDING_DECISIONS.md` 各加了一段"本文之后发生的事"，
免得下次有人拿旧结论当现状。

---

*本文是 P6 审计的执行侧续篇。审计只盘点、不修改；本文负责把每一条带到收口。*
