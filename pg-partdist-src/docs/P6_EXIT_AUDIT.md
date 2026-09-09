# P6 出口审计与全项目缺陷盘点（2026-09-06）

审计对象：`shardpg-tx2-work` @ `152279b`（批次 #10 之后）。方法：先按 DEV PLAN
§3.8「P6 出口清单」12 条逐条取证，再按模块（raft / 回放 / 同步 / 2PC / TSO-MVCC /
vacuum / 内核补丁 / 测试体系）扫代码与文档，**凡"实测"均为本次亲自跑过或读过的
代码行**，凡文档陈旧均给出行号。本文只盘点、不修改任何产品代码或既有文档。

> **★ 2026-09-09 后续（本文之后发生的事，读本文前先看这段）**
>
> 1. **§四那 10 处文档不对齐已全部回填**（批次 #12），清单见
>    `P7_REMEDIATION_PLAN.md` §5。回填时**发现本文自己有一处已过时**：§四 #5 说
>    FRD §13 约束 13 写着"pg_raft 侧修法已整体回退"——文字确实还在，但**代码里
>    那些修法在库**（T4.4 已重放 #39；09-09 实测 `wait_for_log_room` 4 处、
>    `apply_owner_pid` 11 处、`flow_stats` 13 处）。
> 2. **§三之外还有 7 条**：2026-09-06/07 多分片演示实跑撞出的缺陷，当时按"只写演示
>    文档"的指示撤回、只留在 `P6_PENDING_DECISIONS.md` 附录里。**其中三条是 ★★★**
>    （切主/供给后已提交数据不可见或丢失）。现已正式登记为
>    **R-P6-15 ~ R-P6-21**（DEV PLAN §5 台账）。
> 3. **本文 §五"建议的收口顺序"已展开成可执行计划**：
>    `P7_REMEDIATION_PLAN.md`（6 个批次 + 6 条待裁 + 出口标准）。
> 4. 除批次 #11（发号起点收成旧主真实 next_xid）外，**没有任何产品代码因本文而改动**
>    ——§三列的缺陷 09-09 逐条复核，**全部依然成立**。

---

## 一、结论先行

**P6 没有结项。** 出口清单 12 条一个都没勾（`TX_TSO_MVCC_DEV_PLAN.md` §3.8
"P6 出口清单"全部仍是 `- [ ]`），逐条核验后：**✅ 5 条、◐ 4 条、❌ 3 条**。

三条 ❌ 都不是"某个功能没做"，而是**出口动作本身没做**：
1. **"全量一次跑完、零 FAIL"从未达成过**。最近一次全量是 T6.8（28 套 1191/31）；
   此后批次 #7–#10 改了回放 redo 路径、路由层、`pg_raft_promote_prepare`、以及
   **整个 PG 二进制（clean rebuild）**，但只跑过子集（批次 #9：6 套 230/1；
   批次 #10：13 套 551/1，红的 `tso_si_p3` 修后单跑 39/0）。当前门禁 31 套
   （`run_p6_exit.sh` `SUITES` 计数 31；OPS 8 套按 R-P6-1 第二次修订默认不进），
   出口清单里写的"29 套"口径未更新，31 套在最新二进制上**一次都没跑过**。
2. **"不做的事逐条有裁定"一条都没裁**。那张表的表头至今是"建议去向（**待裁**）"，
   9 行无一有裁定记录（其中一行还陈旧了：分片 clog 物理删除其实已做）。
3. **三份文档与代码不对齐**，本次查出 10 处（§四），其中两处是**批次 #10 自己
   做完没回填**的。

---

## 二、P6 出口清单逐条核验

| # | 清单条目 | 核验 | 证据 |
|---|---|---|---|
| 1 | T6.0 前置核查落档，风险登记簿无陈旧状态 | ◐ | 落档 ✓（`P6_PRECHECK.md`）。台账仍有陈旧：**R-P4-20** 附表第 226 行仍标"★ 未修"，而 FRD §13 约束 12 已写"已按合并立项处置完毕（T6.3a/b/c）"、约束 2 已闭合 —— 根因两面都处置了，**没有人给它一个收口裁定**；**R-P4-13**（apply 侧决议登记缺失）仍是"已绕行、根因未闭"（`pg_raft--1.0.sql:1349` 的 `dtx_peek` 绕行） |
| 2 | 物理基线重做工具可用，三个消费者各一条端到端用例 | ◐ | 工具 ✓。消费者：初始配对 ✓（`shard_baseline_p6` / `handover_provision_p7`）；永久分叉 ✓（`divergence_mark_p8` 完整闭环）；**快路径分叉 ✗** —— `test_fastpath_divergence_tx4.sh` 只验到 `promote_prepare` 返回 -1，**没有任何重做基线的步骤**（grep `baseline|provision` 零命中），DTX §12.5 末尾"尚未接成一键操作"仍成立 |
| 3 | 无基线游标的 locmap 配对被拒绝 | ✅ | `locmap_base_p6` 23/0 → 25/0 |
| 4 | "页太短"⇒ shard 停摆、节点不重置 | ✅ | `offnum_guard_p6` 25/0 |
| 5 | 升主后写入 + 本地崩溃恢复不丢（0010） | ✅ | `promote_p6` [2d] kill -9 取证，39/0 |
| 6 | 副本壳表在 **anti-wraparound 压力下**不被本地 WAL 触碰 | ◐ | 套件**没有施加任何 anti-wraparound 压力**（`test_replica_gate_p6.sh` 全文无 `freeze_max_age`/`age(` 注入），验的是"约束 5 relfrozenxid 已同步 + 闸门拦截"。T6.3c 记要把命题改写了（前提不成立故收缩），逻辑成立，但**清单原文的实验没做**。另有已记残留：遗留宇宙副本的读会设 hint bit（本地写） |
| 7 | 升主后无主 RUNNING 落定；U-P5-1 窗口被认领兜住 | ✅ | `promote_p6` 39/0 含判别断言 + 三条阴性 |
| 8 | 回放推进分片分配器（§5.5） | ✅ | 双宇宙：新宇宙跳过原生 nextXid（`shard_replay.c:1152/2055` 只剩遗留流走 `PartDistAdvanceNextXidPastXid`），`xid_watermark_p6` 17/0 |
| 9 | §10 每条限制各一条负向断言 | ◐ | 有断言的：SERIALIZABLE / FOR SHARE（p1）、逻辑解码 ×2、`synchronous_commit=off`、CIC / CLUSTER / VACUUM FULL（`shard_clog_p2` + `negative_p6`）、rebalancer 系 ×4、未 join 的 PREPARE。**没有的**：引用表运行期写（`negative_p6:170` 只是"取证可得"，**代码里也没有守卫**）；"Citus 路由写 P1 禁"一行已被 P4 的 MX 推翻，§10 未更新；**§10 根本没列**的真实限制见 §三-5 |
| 10 | 全量 29 套一次跑完、零 FAIL | ❌ | 见"结论先行"第 1 条。31 套超时总和 9.5h（最坏） |
| 11 | "不做的事"逐条有裁定 | ❌ | 见 §三-9 逐行实况 |
| 12 | 三份设计文档与代码逐条对齐 | ❌ | 见 §四 |

---

## 三、缺陷与未完成部分（按模块）

标注：**[缺陷]** = 代码行为与设计/预期不符；**[未做]** = 设计写了、代码没有；
**[边界]** = 已裁定接受、但要写进 §10 或运维规程；**[债]** = 测试/文档/工程债。

### 3.1 Raft（`pg-raft-src`，冻结模块）

| 条目 | 性质 | 证据 |
|---|---|---|
| **成员变更无安全路径**：组成员建组时定死，扩缩容/换节点/搬分片没有 joint consensus | [未做] | `raft_consensus.c` 无 `joint`/`ConfChange`/`add_member`（grep 零命中）；raft plan §0.2 #5。**连带后果**：§9.2 把 Citus 搬运器全禁了，理由是"搬分片走 raft 成员变更专项"，而专项不存在 ⇒ **集群拓扑事实上不可变更** |
| **`RAFT_MAX_GROUPS = 32` 定长 shmem** ⇒ 每节点 ≤ 31 个数据组；每分片一组的架构下一个节点托管不了几十个分片 | [未做] | `raft_consensus.c:124`；raft plan §0.3（外部化 E1–E4 已整条回滚，前置条件一件都没有）。当前 tx2 集群 `partition_map` 仅 2 行 / 17 个分片 —— HA 覆盖是**逐分片手工建组 + 手工供给**，没有"给一张分布表的全部分片建组并供副本"的自动化（`setup-raft.sh`/两份 SQL 均无） |
| 日志环 `RAFT_LOG_CAPACITY=128`，背压超时后仍丢弃；`lazy_truncate_heap` 的物理截断不随回滚撤销 ⇒ 分叉 | [缺陷·已遏制] | `raft_consensus.c:1552 wait_for_log_room`、`:3883–3919`。批次 #8/#9 补了**检测**（`diverged` 标记）+ **一键修复**（`repair_diverged_shards`），**容量未提、无自动修复**（FRD §13.13 末段） |
| `quorum_drops` 路径：`discard_uncommitted_entry` 截 ring 但不再截 parwal 字节（8/3 起） | [边界] | `raft_consensus.c:3789–3830`。丢弃计数 ≠ 分叉，但仍是唯一健康度观测口 |
| 升主 deadline 兜底：`promote_catchup_deadline_ms=60000` 到点后**未追平也放行** | [边界] | `raft_consensus.c:262, 3230–3336`。"可用性优先"是显式取舍，但意味着 60s 后可能选出**落后**副本（-1 的分叉/无副本仍永久拒绝）。须进运维规程 |
| **R-P4-13** apply 侧决议登记缺失，仅 `dtx_peek` 拉取绕行 | [缺陷·已绕行] | `pg_raft--1.0.sql:146, 1349` |
| 无 PreVote：重启成员以更高 term 打断在任 leader | [未做] | grep `prevote` 零命中；raft plan §0.2 #9 |
| 数据组无压缩/InstallSnapshot；`raft_log` SQL 行与用户事务同命（DTX §9.9） | [未做] | raft plan §0.2 #6、§0.4 |
| 单 BGW tick 多路复用全部组（组数上去后挤占控制面心跳） | [未做] | raft plan §0.2 #8 |

### 3.2 物理回放（`src/replay/`）

| 条目 | 性质 | 证据 |
|---|---|---|
| **leader `DROP TABLE` ⇒ 副本侧静默**：无 opcode，follower 的壳表 + 回放槽位 + `pg_parwal/<oid>` **永不回收** —— 回收判据是"OID 不在本地 `pg_class`"，而壳表是本地真表 | [缺陷] | `shard_fileset.c:929–933`（"属后续工作；这里保持沉默"）；FRD §12.4 |
| **leader 任何一次 DDL ⇒ follower 停在结构栅栏，需人工等价 DDL + 重跑 `replay_set_locmap`** | [边界·运维成本极高] | `replay_worker.c:701–709, 1082–1086`；FRD §12.4 "结构补齐是人工的"。供给只在初始时 `LIKE INCLUDING ALL`，之后不跟 |
| **物理基线上限 1 GB**：`fileset_inline_max_blocks = 131072`，超出显式 ERROR ⇒ **大于 1 GB 的分片既不能供给、也不能修复分叉** —— 没有流式基线 | [未做] | `shard_fileset.c:158, 268–275, 376` |
| 遗留宇宙（R1/R2/L1 时代）副本：R3（`PartDistResolveGxid` / `HeapTupleSatisfiesGlobalMVCC` / `ShardRouteEntry`）**从未实装**，升主后不可读；读会设 hint bit 写本地 WAL（约束 12 残留） | [未做·可裁掉] | grep 四个符号仅剩 `shard_xidmap.h:9` 一条注释；FRD §14.2。新宇宙（打标分片）下 promoted 分片可读已由 `promote_catchup_tx3` / `handover_provision_p7` 实证 —— **R3 的必要性只剩遗留宇宙**，应裁定"遗留副本退役"而不是继续挂着 |
| 快路径分叉 / 降级归队：检测到分叉只 `replay_disable` + WARNING；降级分支只收回身份；**归队重做基线全靠人工** | [未做] | `pg_raft--1.0.sql` promote_prepare 分叉分支；`raft_boundary.c:230–240` |
| `REPLAY_MAX_SHARDS = 64` 定长槽位 | [边界] | `shard_replay.h:275` |
| CTRL 记录非事务性：`FILESET_UPDATE` 在 PRE_COMMIT 之后失败的窗口（D1 显式选择记录不解决） | [边界] | FRD §13 约束 11 |
| 撕裂页仅 checksum + 重基线兜底（sidecar 未做） | [边界] | FRD §13 约束 9 |
| 认领探活用 `kill(pid, 0)`（`replay_worker.c:855`）—— FRD §13.13 自己记过容器里僵尸让它失真，raft 侧因此不探活；回放侧还在用 | [债] | 常驻 `on_shmem_exit` 归还已覆盖 FATAL，风险窄，但两处口径不一 |

### 3.3 同步 / 捕获 / 门禁（`src/wal/`、`shard_guard.c`）

| 条目 | 性质 | 证据 |
|---|---|---|
| **§9.2 第 3 层禁用清单漏了 8 个同类 UDF**（同样亲手搬/读分片数据，且都在 Citus 13.1 库里存在）：`citus_split_shard_by_split_points`、`isolate_tenant_to_new_shard`、`citus_drain_node`、`master_move_shard_placement`、`master_copy_shard_placement`、`replicate_table_shards`、`citus_schema_move`、`alter_table_set_access_method` | [缺陷] | `shard_guard.c:52–75` 清单 vs 协调者 `pg_proc` 实查。按名匹配，`master_*` 旧名与 `drain_node`（内部直接调 C 函数、不经 ExecutorStart）都绕得过 |
| 逻辑解码禁令只覆盖 SQL；walsender `START_REPLICATION` 绕得过（R-P6-14） | [缺陷·已定性] | 需内核层判据，未做 |
| **引用表运行期写：§10 说"建表后只读"，代码零守卫、测试零断言** | [未做] | grep `reference|引用表` 在 `shard_guard.c`/`shard_xid.c` 零命中 |
| 禁用清单不看 joinqual / 索引 quals | [边界] | `shard_guard.c` 注释自陈；低风险 |
| 单事务 DROP > 16 张打标表即 ERROR（`SHARD_CLOG_PENDING_DROPS_MAX`）—— `DROP SCHEMA ... CASCADE` 会撞 | [边界] | `shard_clog.c:357–381` |

### 3.4 2PC / DTX

| 条目 | 性质 | 证据 |
|---|---|---|
| 快路径 `[A]→quorum→[B]` 窗口：接受 + 检测 + **修复手工**（同 3.2） | [边界] | DTX §9.5 / §12.5 |
| `acked[]` 只在 leader 本地，选举窗口内决议行泄漏（有界） | [边界] | DTX §9.7 |
| Prepare 三层串行（组间 / 组内记录 / peer），每条记录一次往返 | [性能] | DTX §12.4 |
| 协调组不可达时 prepared 持锁无上限；恢复守护超时参数化、`pg_prepared_xacts` 年龄监控未做 | [未做] | DTX §9.8 |
| R-P4-10：不经决议流程的分片写（纯 Citus 2PC）判决永不落账 —— 记为"适用边界"，但"P5 评估是否纳入 fail-closed"**没做** | [未做] | 风险簿 8g |
| `dtx_close_indoubt` 四级落空 ⇒ 保持 in-doubt（正确），但无告警面 | [债] | DTX §9.6 |

### 3.5 TSO / 事务 / MVCC（`tso*.c`、`shard_xid.c`、`shard_visibility.c`）

| 条目 | 性质 | 证据 |
|---|---|---|
| **TSO 单点、无 HA**：master 重启 = 整簇重建（boot 标记只把"静默重发号"变成"响亮停摆"） | [边界·v1 裁定] | DESIGN §2.4；`tso.c:107–171` |
| **子事务禁写分片表**（SAVEPOINT / plpgsql `EXCEPTION` 块内写打标表 ⇒ ERROR）—— 设计 §5.4 承诺的 per-shard 子 xid + parent 链**未实现**（`parent_xid` 恒 0），**且 §10 没有这一行** | [未做·§10 漏列] | `shard_xid.c:1213–1220`；`shard_clog.h:51` |
| **回卷未实现**：分片 xid 是线性计数器，全仓普通 `<`/`>=`；靠阶段 2 停发线（2^31 − 边距）让线性假设成立 | [边界·§10 漏列] | T5.6 记要 |
| **`SHARD_XID_MAX_SLOTS = 64`/节点，分配器 shmem 槽位无产品侧回收**：DROP 只删水位文件与 `mvcc_set` 登记，槽位不放；门禁靠**移走水位文件 + 重启**规避（`run_p6_exit.sh:165–170` 是脚本直接 `mv`） | [缺陷] | `shard_xid.h:30`；`shard_xid.c` 无任何释放路径（grep 零命中）；R-P6-4 |
| **打标登记全手工**：`partdist_set_shard_mvcc(rel,true)` 逐分片、逐节点；无建表钩子/事件触发器；18 个验收脚本各自手工调 | [未做] | `pg_partdist--1.0.sql:1276`；`src/*.c` 无 EVENT TRIGGER / `create_distributed_table` 挂点 |
| 打标后 `CREATE INDEX` 被拦（索引须在打标前建）；`LP_REDIRECT` 不产生，撞见即 ERROR —— **§10 没这一行** | [边界·§10 漏列] | T5.3c / T5.8 记要；`shard_vacuum.c:715–736` |
| GlobalSafeTs 被钉死（长事务/失联节点）无监控告警面 | [未做] | DESIGN §7 "告警体系须纳入" |
| hint bits 禁用 ⇒ 中间带读放大 | [边界·已裁定] | DESIGN §4.5 |

### 3.6 Vacuum / GC（`shard_vacuum.c`）

| 条目 | 性质 | 证据 |
|---|---|---|
| **无自动启动器**：到龄只 WARNING，分片 vacuum 只能手工 `shard_vacuum_sweep` | [未做·出口表第 1 行] | `shard_xid.c:1381` |
| **无尾部截断**：不把文件尾部还给 OS | [未做·出口表第 4 行] | `shard_vacuum.c` 无 `smgrtruncate`/`RelationTruncate` |
| 两处覆盖缺口：clog 整段删除分支；真正停在页面循环中间的崩溃 | [债·出口表第 3 行] | T5.7 挂账 ⑥ |
| 死元组清除与前缀截断未解耦（一个 RUNNING 阻挡者钉死整分片回收） | [边界·已记后续优化] | DESIGN §6.3 |

### 3.7 内核补丁 / 构建

| 条目 | 性质 | 证据 |
|---|---|---|
| 10 个补丁是硬依赖；`pg-install/bin/postgres` 必须与补丁成对提交 | [纪律] | `patches/README.md` |
| **改共享结构体后增量 make 不可靠**（1218 个陈旧 .o 造成运行期栈踩踏，R-P6-7）—— 纪律已写，**没有自动守卫**（无"头文件比 .o 新即拒装"的检查） | [债] | `patches/README.md` 构建纪律一节 |
| 0010 的三项运维裁定（归档断链 / 不支持原生备库）是"接受并规定动作"，不是消除 | [边界] | `patches/README.md` |
| R-P6-14 真堵要在 `LogicalDecodingProcessRecord` 层加判据 —— 内核补丁面 | [未做] | 风险簿 24 |

### 3.8 测试体系 / 环境

| 条目 | 性质 | 证据 |
|---|---|---|
| 31 套全量在最新二进制上未跑过一次（见 §一） | [债·门禁] | `scratchpad/fin.log`、`reg10.log` |
| OPS 8 套按 3 节点布局写，在 9 节点上会停错节点且不复原，退出门禁未改造 | [债] | `run_p6_exit.sh:83–101` |
| replay / tx / tx2 三套 9 节点集群互斥；`freeze_sync_d2` 等靠 `CONTAINER` 覆盖 | [债] | R-P6-1 |
| R-P6-13 拓扑抖动"等不到只告警不阻塞"—— 批次 #10 的 `reg10.log` 里 `handover_provision_p7` 就带着"60s 未收敛、存疑"跑完 | [债] | `reg10.log` |
| 宿主 3.9 GB 内存跑 9 节点：本日一次后台任务被系统因低内存杀掉；全量 9.5h（最坏）需先确认资源 | [环境] | 本次会话实测 |
| `write_router.c` 是 milestone 1.2 死代码（`ROUTE_REMOTE` 直接报错），仍编入并暴露为 `pg_partdist_route_write()` | [债] | `write_router.c:42, 156`；`pg_partdist.c:717–733` |

### 3.9 "不做的事"九行逐行实况（出口清单第 11 条）

| 行 | 实况 | 建议裁定 |
|---|---|---|
| 分片 vacuum 自动启动器 | **未做** | 做（生产不可用的硬伤） |
| 回卷 | **未做**，§10 无行 | 降级为已知边界，**补进 §10** |
| 两处覆盖缺口 | **未做** | 故障注入专项 |
| 尾部截断 | **未做** | 做 |
| LP_REDIRECT | **未做**，§10 无行 | 补进 §10（连同"打标后禁 CREATE INDEX"） |
| §13 约束 13 环容量 | 检测 + 一键修复 **已做**；容量未提、无自动修复 | 裁定"检测+手工修复"为 v1 终态并写进运维规程 |
| 分片 clog 物理删除 | **已做**（`ShardClogAtCommit` 提交时 `rmtree`，`shard_clog.c:392–412`）—— 表陈旧 | 划掉 |
| Proxy 守护组件 | 实质已由 Citus MX 取代（T4.2"Proxy 粘性路由并入"） | 正式从方案裁掉，DESIGN §1.1 同步改 |
| R3 可读性 | 新宇宙已不依赖 R3（tx3/p7 实证）；只剩遗留宇宙 | 裁定"遗留副本退役、R3 不进 P6" |

---

## 四、文档与代码不对齐（10 处）

| # | 位置 | 问题 |
|---|---|---|
| 1 | `TX_TSO_MVCC_DESING.md:12` | 头部状态仍写"**未实施，未动任何代码**" |
| 2 | `FOLLOWER_REPLAY_DESIGN.md:1012` | §10 签名 `PartDistRoutePromote(Oid, FullTransactionId watermark)`，实装是 `PartDistRoutePromote(Oid)`（`shard_fileset.h`） |
| 3 | `FOLLOWER_REPLAY_DESIGN.md:1101` | §11 步骤 5 注仍写"**仍未实装**：路由层本身……`wal_insert_hook` 捕获新流" —— 批次 #10 已做完（p7 33/0），**做完没回填** |
| 4 | `FOLLOWER_REPLAY_DESIGN.md` §14.2 | "R4 硬阻断于 R3"对新宇宙已不成立（promoted 分片可读已实证），对遗留宇宙仍成立，未区分 |
| 5 | `FOLLOWER_REPLAY_DESIGN.md` §13 约束 13 中段 | "pg_raft 侧修法已整体回退，当前代码 = 稳定版 + isnull 修复"—— T4.4 已按用户裁定 (b) 重放 #39（`1c22a31`+`455c935`），`wait_for_log_room` / `apply_owner_pid` / `flow_stats` 都在当前代码里 |
| 6 | `raft_module_revision_plan.md` §0.2（2026-08-08） | "尚未做的"表整体陈旧：#1 集群级 xid 区间租约已被分片 xid 宇宙取代、#2 P3 回放本体已做、#4 2PC 第 6 步已做、#7 `notify_primary_switch` 真实化已做 |
| 7 | `TX_TSO_MVCC_DEV_PLAN.md` "不做的事"表 | 分片 clog 物理删除已做；Proxy 已被 MX 取代 |
| 8 | `TX_TSO_MVCC_DESING.md` §10 | 缺行：子事务禁写、打标后禁 CREATE INDEX / LP_REDIRECT、回卷未实现；"Citus 路由写 P1 禁"一行已过时（P4 MX 后 Citus 驱动的 2PC 写是正路） |
| 9 | `P6_PRECHECK.md:226` vs FRD §13 约束 12 | R-P4-20 一处"★ 未修"、一处"处置完毕"，状态不一致 |
| 10 | 根 `README.md` | 仍是 2.0 三节点文档，零处提及 tx2 / 9 节点 / TX-TSO-MVCC / 门禁入口 |

---

## 五、建议的收口顺序（不代做）

1. **先跑 31 套全量**（确认宿主内存后；最坏 9.5h）—— 没有它，其余都是纸面。
2. **出口动作**：九行"不做的事"逐行裁定；12 个勾按上表实况勾/不勾；"29 套"改为
   当前口径并写明 OPS 8 套的去向。
3. **文档回填 10 处**（§四），其中 #2/#3 是批次 #10 自己欠的。
4. **小改动、高收益**：禁用清单补 8 个 UDF；引用表写守卫 + 负向断言；§10 补 4 行。
5. **真缺陷排序**（按生产影响）：leader DROP 静默 → DDL 全人工 → 1 GB 基线上限 →
   分配器槽位 64 无回收 → 成员变更缺失 → vacuum 自动启动器 → 快路径分叉归队自动化。
