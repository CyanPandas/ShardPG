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
> 产品缺陷有 **22 条**，其中 **4 条是"已提交的数据在切主后不可见/丢失"级别**，
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

## 1 缺陷总账（**22 条**产品缺陷 + 8 条工程债；R-P6-22 于 2026-09-09 查证时新增）

### 1.1 ★★★ 数据正确性（批次 1）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **R-P6-15** | **2PC 阶段 3 的 COMMIT 标记不带分片 xid**：`dtx_participant.c:467` 在 `COMMIT PREPARED` 里调 `PartWALBuildMarkerPayload()`，而该函数按 `ShardXidXactCount() > 0` 决定带不带分片 xid 尾（`partwal_sync.c:723-727`）——`COMMIT PREPARED` 跑在另一个没碰过分片表的事务里，计数恒 0 ⇒ 24 字节旧格式 | **✅ 已修（2026-09-09，T7.1）**：判决标记改由判决落账那一刻补发（`dtx_pending.c` `dtx_replicate_verdict()`）。验收 `test_dtx_verdict_marker_p7.sh` **34/0**，决定性取证：副本 `st=2 cts=13` 与 leader 的 `cts=13` 逐字节相同（修复前恒为 st=1） | 副本分片 clog 对每笔跨分片事务**永远停在 PREPARED**；决议被 FORGET 回收后 `dtx_close_indoubt` 四级全落空 ⇒ **切主后 2PC 提交的行在新主上永久不可见**（09-06 实跑取证） |
| **R-P6-17** | **物理基线不搬分片 clog**：`shard_fileset.c` 全文零处引用 `pg_shard_clog`/`ShardClog`，`shard_baseline_emit` 只灌页面 + 抬发号水位 | **未修**（grep 零命中） | 在已有数据之后才供给的副本，对基线之前提交的分片 xid **没有判决**；升主后那些行是 RUNNING ⇒ 被 `shard_claim_on_promote` 改判 ABORTED ⇒ **丢行** |
| **R-P6-16** | **升主不发 `FILESET_UPDATE`**：`PartDistRoutePromote()`（`shard_fileset.c:1838`）只做"角色 + 捕获"两件事，不广播新主的 relfilenumber | **✅ 已修（2026-09-09，T7.3）**：`PartDistEmitFilesetHandover()` + 新标志位 `PARTWAL_FSUPD_PRIMARY_HANDOVER`（只重绑、不截断、不发 FPI）。验收 `test_promote_handover_p7.sh` **35/0** | 其余副本 locmap 仍对着旧主文件号 ⇒ `replay_catchup` 报"未知 relfilelocator" ⇒ **切主一次，该分片其余副本全部失去再次当选资格**，直到从新主重新供给 |
| **R-P6-21** | **供给/升主不携带"打标身份"**：`shard_fileset.c`/`raft_boundary.c` 均无 `ShardMvccSetAdd`；`mvcc_set` 只由 `partdist_set_shard_mvcc()` 或重启扫目录装载 | **✅ 已修（2026-09-09，T7.4）**：升主时按持久证据（`pg_shard_clog/<oid>` 存在）继承打标身份。验收同上，实测新主写入 xmin=4（分片 xid，非原生大 xid） | 升主后的新主若未事先手工加白名单又未重启，**写入不打标、读走原生路径** —— `handover_provision_p7` [3b] 与 `promote_catchup_tx3` [4] 都是在这个状态下通过的，**通过的原因是错的** |

### 1.2 ★★ 可用性 / 资源（批次 2）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **R-P6-19** | 回放 worker **泄漏目录描述符**：`exceeded maxAllocatedDescs (328)`，某条 `AllocateDir` 路径缺 `FreeDir` | 未修（六个文件里 `AllocateDir`/`FreeDir` 计数配平，说明泄漏在**异常提前返回**路径上，需逐条走查） | 该节点此后**所有回放失败**，直到 worker 重启 |
| **R-P6-20** | TSO 客户端 RPC 用裸函数名：`tso_client.c` 5 处发 `SELECT partdist_tso_start_ts(...)`，而函数装在 `partdist` 模式 | **✅ 已修（2026-09-09，T7.5）**：5 处改全限定名。修前在 pg-test 环境实测复现（`function partdist_tso_start_ts(integer, bigint) does not exist` → `TSO 不可达或拒绝服务`）；修后默认 `search_path` 下取号 1→2→3→4 单调，日志零 `does not exist` | 协调者默认 `search_path` 不含 `partdist` ⇒ 取号/心跳/commit_ts/safe_ts 全部 `function does not exist` ⇒ **全簇分片写 fail-closed**。演示环境靠 `ALTER DATABASE ... SET search_path` 绕过 |
| **R-P6-4** | 分配器 shmem 槽位 `SHARD_XID_MAX_SLOTS = 64`/节点，**无产品侧回收**：`shard_xid.c` 零处释放路径 | 未修（`include/shard_xid.h:30`；grep 释放零命中） | 建删 64 个分片后该节点**再也建不了分片**；门禁靠"移走水位文件 + 重启"规避（`run_p6_exit.sh:165-170` 直接 `mv`） |
| **P7-D1** | **leader `DROP TABLE` ⇒ 副本侧静默**：`shard_fileset.c:929-933` 明写"属后续工作；这里保持沉默" | 未修 | 壳表 + 回放槽位 + `pg_parwal/<oid>` **永不回收**（回收判据是"OID 不在本地 `pg_class`"，而壳表是本地真表） |

### 1.3 ★ 限制面 / 守卫面（批次 3）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **P7-G1** | §9.2 第 3 层禁用清单**漏 8 个同类 Citus UDF**：`citus_split_shard_by_split_points`、`isolate_tenant_to_new_shard`、`citus_drain_node`、`master_move_shard_placement`、`master_copy_shard_placement`、`replicate_table_shards`、`citus_schema_move`、`alter_table_set_access_method` | 未修（`shard_guard.c:50-80` 清单实为 7 个 citus UDF + 5 个逻辑解码入口；11 个 UDF 在 Citus 13.1 里全是 C 函数，实测在库） | 它们同样亲手搬/读分片数据，绕过去就是静默错读或与 raft 放置冲突。**注意补名字只堵入口，不堵通路**（§3.1），而且在补名字之前先得让守卫在协调者上真的生效（R-P6-22，§3.2） |
| **P7-G2** | **引用表运行期写：零守卫、零断言**，DESIGN §10 那行还挂着"【需核实现状】" | 未修（`shard_guard.c` 零命中 `reference`） | §10 写"建表后只读"，实际拦不住 |
| **R-P6-18** | MARKER 的 `start_ts` 是墙钟（`partwal_sync.c:733` `GetCurrentTransactionStartTimestamp()`），不是 TSO start_ts | **未修** | R-P3-2「双 ts 宇宙串线」成真：升主后 §4.2 三态处置拿它与 TSO 快照比，**恒为"跳过"** |
| **R-P6-22** | **禁用清单在协调者上根本不生效**：`ShardGuardCheckPlan` 的快门是 `ShardGatingActive()`（本节点 `shard_relids` 非空 或 shmem `mvcc_n>0`），而打标表长在 worker、协调者两者皆空 ⇒ 只在协调者上调用的 `citus_rebalance_start` / `citus_drain_node` / `undistribute_table` 等**一条都不触发**。`negative_p6` 全绿是因为夹具用一个 worker 上的表 OID 给协调者开了闸门（`:64-66`） | **2026-09-09 新发现**（本次查证第 6 条时撞出，实测三节点白名单全空） | §9.2 第 3 层禁用整层在生产形态下熄火 —— 比"漏 8 个名字"严重得多。见 §3.2 |
| **P7-G3** | DESIGN §10 缺 4 行、错 1 行 | **本次已补**，见 §5 | — |

### 1.4 ◐ 出口动作 / 门禁（批次 4）

| ID | 事项 | 09-09 复核 |
|---|---|---|
| **P7-E1** | **31 套全量在最新二进制上一次没跑过**（`run_p6_exit.sh` `SUITES` 实数 31；最近一次全量是 T6.8 的 28 套 1191/31，其后批次 #7–#11 改了回放 redo、路由层、`promote_prepare`、整个 PG 二进制、发号起点） | 未跑。出口清单原文的"29 套"口径也已过时 |
| **P7-E2** | OPS 8 套按 3 节点布局写，9 节点上会停错节点且不复原 | **已裁定：改造进门禁**（2026-09-09）。拓扑无关化后并入 `SUITES`，口径 31 → 39 套。T7.13 |
| **P7-E3** | 三个基线消费者 e2e：初始配对 ✓、永久分叉 ✓、**快路径分叉 ✗**（`test_fastpath_divergence_tx4.sh` 只验到 `promote_prepare` 返回 -1，无重做基线步骤） | 未补 |
| **P7-E4** | `replica_gate_p6` **没有施加 anti-wraparound 压力**，验的是"relfrozenxid 同步 + 闸门拦截" | **已裁定接受**（2026-09-09）：采用 T6.3c 改写后的命题，出口清单原文改掉，本条关闭 |
| **P7-E5** | R-P6-13 拓扑抖动"等不到只告警不阻塞"，批次里带着"60s 未收敛、存疑"跑完 | 未改判据 |

### 1.5 ○ 生产化缺件（批次 5）

| ID | 事项 | 09-09 复核 |
|---|---|---|
| **P7-V1** | 分片 vacuum **无自动启动器**，到龄只 WARNING（`shard_xid.c:1381`） | 未做 |
| **P7-V2** | 分片 vacuum **无尾部截断**（`shard_vacuum.c` 无 `smgrtruncate`/`RelationTruncate`） | 未做 |
| **P7-V3** | 两处覆盖缺口：clog 整段删除分支；停在页面循环中间的崩溃 | 未做（需生产路径故障注入点） |
| **P7-V4** | 打标登记全手工（`partdist_set_shard_mvcc` 逐分片逐节点，无建表钩子/事件触发器） | 未做；与 R-P6-21 同源，一并做 |

### 1.6 ⏸ 独立专项（批次 6，**2026-09-09 已裁定：做，排在批次 1–5 之后**）

| ID | 事项 | 为什么单列 |
|---|---|---|
| **P7-R1** | Raft **成员变更无安全路径**（无 joint consensus）⇒ 连带 §9.2 禁掉全部 Citus 搬运器 ⇒ **集群拓扑事实上不可变更** | 改 `pg-raft-src` 核心。★ 2026-09-09 起本线**不再需要逐次申请解冻**，按 §6 纪律直接改 |
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
| **T7.9** P7-G1 禁用清单补 8 个 UDF + **R-P6-22 闸门改判据** | **先做 R-P6-22**：Citus 运维类禁令的闸门从 `ShardGatingActive()`（本节点有没有打标表）改为**集群级判据** —— 否则补再多名字，在协调者上也是熄火的（§3.2）。再补 8 个名字进 `shard_banned_funcs[]` | `negative_p6` **撤掉给协调者设白名单那两行**（`:64-66`）之后，六项运维禁令断言仍全绿；每个新名字一条负向断言 |
| **T7.10** P7-G2 引用表运行期写 | 先裁定"拦"还是"接受"（§3）。若拦：在 `shard_guard` 里按 `pg_dist_partition.partmethod='n'` 判引用表并拒写 | 拦则一条负向断言；接受则 §10 改写为"不拦，靠规程"并给出后果 |
| **T7.11** R-P6-18 MARKER 用 TSO start_ts | `PartWALBuildMarkerPayload` 改用 TSO 的 start_ts。**注意兼容**：旧流里的墙钟值要能被识别（加 flag 位或按量级判别），否则升主后三态处置会把老记录判错 | 副本 PREPARED 槽 `sts` 与 leader TSO 同宇宙；§4.2 三态处置第一支不再恒"跳过" |

### 批次 4：出口动作（做完前三批再跑，否则白跑）

| 任务 | 内容 |
|---|---|
| **T7.12** 全量一次跑完 | **⏸ 已裁定暂缓（2026-09-09）**：批次 1–3 落地后再排。口径 = 31 套 + OPS 8 套改造后 = **39 套**。跑前：停 `pg-citus-tx2-container` 腾内存、先清它 68.5 GB 可写层腾磁盘（当前 / 已用 93%）。最坏 9.5 h+，建议分 P 段跑并逐段记录 |
| **T7.13** OPS 8 套改造 | **已裁定：改造进门禁**（2026-09-09）。改成拓扑无关：节点按 `pg_dist_node` 动态取，停/起节点后必须复原。并入 `run_p6_exit.sh` 的 `SUITES`，门禁口径 31 → 39 套 |
| **T7.14** P7-E3 tx4 补基线闭环 | 给 `fastpath_divergence_tx4` 补"重做基线 → 重新参选 → 当选"一段，闭合"三个基线消费者各一条 e2e" |
| ~~T7.15~~ P7-E4 replica_gate 压力 | **已裁定接受改写后的命题（2026-09-09），本条关闭**。动作只剩一个：把 DEV PLAN §3.8 出口清单该条原文改掉（本次已改），不补压力注入 |
| **T7.16** P7-E5 拓扑抖动判据 | "等不到就阻塞"而不是"只告警"，消灭"批次红、单跑绿"的伪信号 |

### 批次 5：生产化缺件

`T7.17` 分片 vacuum 自动启动器（P7-V1）；`T7.18` 尾部截断（P7-V2）；
`T7.19` 两处覆盖缺口的故障注入点（P7-V3）。三条都是"生产不可用的硬伤"里
最不紧急的一档 —— 有手工替代，但没有它们不能说交付。

### 批次 6：独立专项（**已裁定：做，排在批次 1–5 之后**）

P7-R1 成员变更（joint consensus）、P7-R2 组数上限与"给一张表全部分片建组 + 供副本"
的自动化、R-P4-13 apply 侧决议登记、P7-R3 流式基线、P7-R4 DDL 自动跟随、
R-P6-14 在 `LogicalDecodingProcessRecord` 层堵解码。

**★ 2026-09-09 起 `pg-raft-src` 在本线不再冻结**：该改就改，不必逐次申请。
代价是 raft 是全局共识层，一处改错影响面是整簇 —— 纪律见 §6。

**批次内建议顺序**：P7-R1（成员变更，解掉"拓扑不可变更"）→ P7-R2（组数上限，
前置是日志外部化 E1–E4，那条整条回滚过，要重新立项）→ R-P4-13 → P7-R3 → P7-R4
→ R-P6-14（内核补丁面，放最后）。

---

## 3 裁定结果（2026-09-09 用户逐条裁定，已生效）

| # | 事项 | **裁定** | 落地 |
|---|---|---|---|
| 1 | 批次 6（成员变更 / 组数上限 / R-P4-13 等需动 `pg-raft-src` 的） | **做，排在批次 1–5 之后开工**；★ **raft 改动不再需要逐次申请解冻** —— 本线（`shardpg-test`）上"该改就改" | 批次 6 由"待批准"变为**已排期**；`pg-raft-src` 的修改纪律见 §6 |
| 2 | 引用表运行期写 | **拦** | T7.10 定为实装守卫；DESIGN §10 该行由"纸面约定"改为"禁（守卫 P7 T7.10 实装中）" |
| 3 | OPS 8 套 | **改造进门禁**（拓扑无关化，按 `pg_dist_node` 动态取节点） | T7.13；门禁口径由 31 套变为 **39 套** |
| 4 | `replica_gate_p6` 的 anti-wraparound 压力 | **接受 T6.3c 改写后的命题**（relfrozenxid 已同步 + 闸门拦截），把出口清单原文改掉 | T7.15 关闭；P6 出口清单该条改判 ✅ |
| 5 | 31 套全量什么时候跑 | **暂时先不跑** | T7.12 挂起，等批次 1–3 落地后再排 |
| 6 | `citus_drain_node` 这类"加名字挡不住"的 | **先搁置**（本次只解释清楚 + 留证据，不动代码） | 见 §3.1；新发现另登记 **R-P6-22** |

### 3.1 关于第 6 条：为什么"加名字"这件事本身是次要问题

**先纠正我上一版的措辞。** 我在 §1.3 与 DESIGN §10 里写过"`citus_drain_node`
内部直接调 C 函数、不经 ExecutorStart，**加名字也挡不住**"——**这句话不准确**，
本次查证后改正：把 `citus_drain_node` 加进清单，`SELECT citus_drain_node(...)`
这条 SQL **是挡得住的**（守卫按函数名匹配计划树里的 `FuncExpr`，这一条走
FunctionScan / targetlist，都在覆盖面内）。

"挡不住"的真正含义是**守卫的性质**：它是**入口黑名单**，不是**数据通路守卫**。

1. **同一个物理动作有多个入口**。11 个 UDF 在 Citus 13.1 里**全是 C 函数**
   （实测 `pg_proc.prolang = c`，`prosrc` 就是函数名）。`citus_drain_node` 的函数体
   在 C 层直接走搬迁逻辑，**不会**再发一条 `SELECT citus_move_shard_placement(...)`
   的 SQL —— 所以只禁 `citus_move_shard_placement` 时，drain 照搬不误。加了名字
   只是把**这一个**入口堵上；下一个我们没想到的入口（或 Citus 升级新增的）照旧。
2. **真正搬数据的是 worker 侧的内部函数**（`worker_*` / `citus_internal_*` 系列），
   它们不在清单里。清单拦的是"发起动作的那句话"，不是"动作本身"。

### 3.2 ★ 查证时撞出的新缺陷：R-P6-22 —— 这道守卫在**协调者上根本不生效**

比第 6 条严重得多，**本次新发现**，已登记：

`ShardGuardCheckPlan()` 在走禁用清单之前有一道快门（`shard_guard.c:238`）：

```c
if (!ShardGatingActive())
    return;                 /* 无打标表：下面的禁用项检查零成本返回 */
```

而 `ShardGatingActive()` = `shard_relids` GUC 非空 **或** shmem `mvcc_n > 0`
（`shard_xid.c:248`）。**分片打标表长在 worker 上，协调者既没有打标表、也没人给它
设白名单** —— 实测本环境三个节点 `SHOW pg_partdist.shard_relids` 全为空。

⇒ **`citus_rebalance_start` / `citus_drain_node` / `undistribute_table` 这些
恰恰只在协调者上调用的 UDF，禁令在生产形态下一条都不会触发。**

**那 `negative_p6` 为什么全绿？** 因为夹具**替它把闸门打开了**
（`test_negative_p6.sh:64-66`）：

```bash
# ★ 协调者侧也要打标：Citus 运维六项的闸门判据是 ShardGatingActive()，
#   白名单只设在 worker 上时，协调者那支根本不生效（首版因此取空）。
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OID}'"
```

而这个 `$OID` 是 **worker 5433 上那张本地表 `t66neg` 的 OID** —— 在协调者上它压根
不指向任何打标表。**夹具用一个不相干的 OID 把守卫开起来，然后断言守卫生效。**
脚本作者当时就把机制写在注释里了（"首版因此取空"），但没有意识到这句注释同时
证明了**生产形态下这道守卫是熄火的**。

这正是"**假声明**"的另一种形态：不是函数声明了却不存在（R-P6-12），而是
**守卫存在、测试全绿、生产不生效**。

**修法方向**（归入 T7.9，与补 8 个名字同批）：Citus 运维类禁令的闸门不能用
`ShardGatingActive()`（那是"本节点有没有打标表"），应改为**集群级判据** ——
本集群是否存在打标分片（查 `partdist.partition_map` / `pg_dist_partition`
或一个显式的集群级开关 GUC）。改完 `negative_p6` 必须**撤掉给协调者设白名单
那两行**，断言仍然绿才算数。

## 4 出口标准（P7 什么时候算完）

- [ ] 批次 1 四条各有**新套件**，且"切主后已提交数据可见"这条命题有端到端取证
- [ ] 批次 2 四条修完，`run_p6_exit.sh` 的净场层（`mv` 水位文件）可以**撤掉**且门禁仍绿
- [ ] 批次 3 修完：R-P6-22 闸门改判据（`negative_p6` **撤掉给协调者设白名单那两行**
      后仍全绿）+ 8 个 UDF 名字 + 引用表守卫 + MARKER 用 TSO start_ts；
      §10 每条限制各有一条负向断言
- [ ] OPS 8 套改造成拓扑无关并入门禁（口径 31 → **39 套**）
- [ ] **39 套全量一次跑完、零 FAIL**，且是在最终二进制上跑的（时机已裁定为
      批次 1–3 落地之后）
- [ ] 批次 5 生产化三条（vacuum 自动启动器 / 尾部截断 / 覆盖缺口）
- [ ] 批次 6 六条（含 raft 侧）
- [x] 本文 §3 六条**已逐条裁定**（2026-09-09）
- [ ] "不做的事"九行：去向已定（裁掉 2 / 划掉 1 / 接受 2 / 做 3 / 归批次 6 一条），
      **差一份把"接受"类写清楚的运维规程**（升主 60s deadline 放行、环容量靠手工修复、
      1 GB 基线上限、回卷停发线）
- [ ] 三份设计文档（DESIGN / DEV PLAN / FRD）与代码逐条对齐（本次已做一轮，见 §5；
      **此后每改一处代码同步回填**，见 §6）

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

## 6 执行纪律（2026-09-09 用户明确要求，本期全程有效）

### 6.1 改完就回填文档，不许攒

**每一条缺陷修完的收尾动作里必须包含"改哪份文档的哪一段"**，与代码同一个提交。
理由是本次回填自己给出的证据：审计 §四那 10 处不对齐里，**有两处是批次 #10 自己
做完没回填**——路由层实装了、文档还写着"仍未实装"；一个月后再看，谁也分不清
哪句是现状、哪句是历史。攒着回填 = 下一次审计。

对应关系（改这里 ⇒ 回填那里）：

| 改动落点 | 必须同步的文档 |
|---|---|
| `src/wal/`、`src/replay/`、locmap / fileset / 基线 | `FOLLOWER_REPLAY_DESIGN.md`（§10 接口签名、§11 升主序列、§12 限制、§13 约束） |
| `src/dtx/`、决议 / 2PC / MARKER | `DTX_2PC_DESIGN.md`（§9 各小节的"落地现状"块） |
| `src/shard_xid.c`、`shard_clog.c`、`shard_visibility.c`、`tso*.c`、`shard_vacuum.c` | `TX_TSO_MVCC_DESING.md`（§5/§6/§7；**限制项一律进 §10**） |
| `src/shard_guard.c`、禁用清单、守卫闸门 | `TX_TSO_MVCC_DESING.md` §9.2 + §10 |
| `pg-raft-src/` 任何改动 | `raft_module_revision_plan.md` §0.1/§0.2（把行从"尚未做"挪走，别只在提交说明里写） |
| 任务完成、验收数字 | `TX_TSO_MVCC_DEV_PLAN.md`（批次记要 + §5 台账状态） |

### 6.2 接口必须**真的可用**，不许假声明

**判据：新加/改动的任何 SQL 可见接口，必须在真集群上被调用成功过一次，
并在验收脚本里留下断言。** 只写 `CREATE FUNCTION` 声明、只改头文件、只在
设计文档里写签名——都不算做完。

这条是踩出来的，本项目已有**三种**"假可用"形态：

1. **声明了但库里没有**（R-P6-12）：`sql/pg_partdist--1.0.sql` 声明 82 个函数，
   集群里 10 个不存在 —— 因为扩展 SQL 只在 `CREATE EXTENSION` 时执行一次，
   后加的声明没人补。任何"SQL 文件里声明了就一定能调"的脚本都会翻车。
   **动作**：改完 SQL 文件跑 `scripts/refresh_extension_sql.sh`，再从**每个相关
   节点**实调一次。
2. **函数在、但调用方拼错了名字**（R-P6-20）：`tso_client.c` 发的是裸函数名，
   而函数装在 `partdist` 模式里 ⇒ 全簇取号 fail-closed。**动作**：跨模式调用一律
   写全限定名，并在默认 `search_path` 下实测。
3. **守卫在、测试绿、生产不生效**（R-P6-22，本次新发现）：`negative_p6` 靠夹具
   给协调者设了个不相干的白名单 OID 才让守卫触发。**动作**：负向用例的前置条件
   必须是**生产上真会成立的状态**；靠夹具"把闸门掰开"再断言，等于没验。

配套的两条老纪律继续有效：**零个检查会静默 PASS**（逐项循环必须有计数守卫）、
**`docker exec` 会吞掉 while 循环的 stdin**（容器调用一律 `</dev/null`）。

### 6.3 `pg-raft-src` 解冻后的修改纪律

冻结取消**不等于**可以随手改。raft 是全簇共识层，一处改错的影响面是整个集群：

- 改共享结构体（`RaftGroupState` / `RaftLogEntry` / shmem 布局）**必须 clean rebuild**
  ——增量 make 会留下按旧结构编译的 .o，运行期栈踩踏，症状伪装成"回放缺陷"
  （R-P6-7 的根因就是这个，查了一整轮才用栈回溯定位）。
- 每次改动至少跑一轮 raft 侧回归 + 一轮切主套件（`promote_p6` / `promote_catchup_tx3`
  / `handover_provision_p7`），**不能只跑改动点附近那一套**。
- 历史上 pg_raft 的修改被整条回滚过一次（2026-08-05 流控 #39），回滚又在 T4.4 被
  重放 —— 每次改动在 `raft_module_revision_plan.md` 里留一行状态，别让"在库 / 已回退"
  再次错位。

---

*本文是 P6 审计的执行侧续篇。审计只盘点、不修改；本文负责把每一条带到收口。
§3 的裁定已生效，§6 的纪律与代码改动同等约束力。*
