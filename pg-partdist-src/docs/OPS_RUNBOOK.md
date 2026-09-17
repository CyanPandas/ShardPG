# 运维规程：已裁定接受的边界

> 版本：2026-09-17，对应代码 `shardpg-test` @ `c47f9ce`。
> 用途：P7 出口清单里"差一份把'接受'类写清楚的运维规程"那一项。DEV PLAN §3.8"不做的事"九行里，
> 裁定为"接受"的边界不修，但运维必须知道**它在哪、盯什么、何时动手、怎么做、不许做什么**。
>
> 标注：**✅实测** = 在 `pg-test-container`（1c+3w）上调通/测过；**📖读码** = 读代码确认、未实测；
> **⚠️待核实** = 读码推断出的风险，尚无实测证据。GUC 值均已在集群 `pg_settings` 核对。

---

## 0. 日常巡检（每个 worker 上执行）

| 看什么 | SQL | 正常 | 异常时去 |
|---|---|---|---|
| 各组日志环深度 / 丢弃 | `SELECT * FROM partdist.pg_raft_group_flow_stats() WHERE group_id<>0;` | `ring_depth` 个位数，三个丢弃计数不增长 | §2 |
| 捕获环 | `SELECT * FROM partdist.partwal_ring_stats();` | `overwrites=0`、`diverged_pending=f` | §2.3 |
| 组主与路由是否一致 | 各 worker `pg_raft_group_status()` 的 leader，对照协调者 `partdist.partition_map.primary_node` 与 `pg_dist_placement` | 三者一致 | §1、§7.1 |
| 升主是否卡住 | 日志 grep `升主前置` | 无 | §1、§7.1 |
| 分片 xid 龄 | `SELECT * FROM partdist.shard_xid_age('<分片表>'::regclass);` | `phase=0` | §3 |
| 悬挂的两阶段事务 | `SELECT gid, prepared FROM pg_prepared_xacts;` | 无长期存在的条目 | §1 |
| 分叉标记 | `SELECT partdist.shard_divergence('<分片表>'::regclass::oid);` | `NULL` | §2.2 |

所有计数器都是**共享内存、每节点各记各的**，节点重启、组重建后清零（`raft_group_init_slot`），**没有 SQL 清零入口**。
看趋势要自己留快照。✅实测（接口均已调通）

---

## 1. 升主截止期兜底放行

**边界**：数据组新 leader 向控制面登记前，要先跑"升主前置" `partdist.pg_raft_promote_prepare(gid, slice_ms)`：
回放追平 → 推进本地 WAL 位点 → 闭合 in-doubt 分布式事务 → 修复分叉 → 认领分片。
追不平时每个 tick 只推进一片；累计超过截止期仍返回"未完成"，就**带 WARNING 放行登记**（可用性优先），
新主可能尚未追平。📖读码（`raft_consensus.c` `data_group_promote_prepare`，SQL 见 `pg_raft--1.0.sql`）

**参数**（均 SIGHUP）：

| GUC | 默认 | 范围 | 说明 |
|---|---|---|---|
| `pg_raft.promote_catchup_deadline_ms` | 60000 | 0–3600000 | 截止期 |
| `pg_raft.promote_catchup_slice_ms` | 2000 | 100–60000 | 每片追平时长；tick 等待上限 = 它 + 8000 ms |

**盯什么**：

- 放行日志：`pg_raft: 组 N 升主前置超过 %d ms 仍未完成，按可用性优先放行上报（该副本可能尚未追平…）`
- 卡住日志：`pg_raft: 升主前置超过 %d ms 未返回或连接失效(组 N)，下个 tick 重试`（**连续出现即是 §7.1 的活锁**）
- 没有计数器、没有视图，只能靠日志。

**何时动手 / 怎么做**：

1. 看到"放行"：该分片新主可能缺数据。对照旧主（若还活着）核对行数/校验和；有差异则以多数派为准重做基线
   （leader 上 `partdist.shard_baseline_emit('<分片表>'::regclass)`）。
2. 看到"未返回或连接失效"连续出现：见 §7.1，**不要**指望截止期兜底——这条路径不计入截止期。

**不许做 / 注意**：

- ❌ 不要把 `promote_catchup_deadline_ms` 设成 0 期望"永不放行"。GUC 描述和 DTX_2PC_DESIGN 都写"设 0 表示永不放行"，
  但代码判据是 `TimestampDifferenceExceeds(first_try, now, 0)`，PG16 实现为 `diff >= 0`，**恒真 ⇒ 第一次没追平就立即放行**。
  ✅读 PG 源码确认（`timestamp.c`），文档与代码相反。
- ⚠️ 放行那一轮 SQL 返回的是 0，**后面四步（推进 WAL 位点、闭合 in-doubt、修复分叉、认领分片）一步都没执行**，
  而升主后 `replay_catchup` 直接拒绝（`已升主，不再回放别人的流`）。📖读码确认"没执行"；后果（缺数据无路径补、崩溃后升主写入丢失风险回归）⚠️待核实。
  **所以"放行"之后必须人工核对，而不是当作已完成。**
- ⚠️ 截止期计时槽在"中途丢掉 leader"时不清零，下次再当选沿用旧起点，可能第一片没追平就直接放行。📖读码推断

---

## 2. 日志环容量与提案丢弃（FRD §13 约束 13）

### 2.1 Raft 日志环：128 条/组 —— 2026-09-17 实测结论：**不限制事务大小，也不限制跨组事务**

**真实语义**（📖读码 + ✅实测）：

- 环是每组 `RAFT_LOG_CAPACITY = 128` 条的共享内存数组，约束的是 **"已追加但尚未应用"** 的条数
  （`ring_depth = last_log_index - last_applied`），不是事务大小，也不是 follower 落后量
  （滑出环的条目可从 `partdist.raft_log` 回读追平）。
- 数据写入的复制**按组串行**（`replicate_claim`），且**逐条同步**：追加 → 写 SQL 日志 → 同步等多数派 → 提交 → 应用，
  然后才轮到下一条。所以正常情况下 leader 的环深度只有 1–2。
- 环满（深度 ≥127）时先阻塞等 `pg_raft.propose_wait_ms`（默认 10000，SIGHUP），超时丢提案，调用方 `ERROR`、**事务中止**
  （不会带着"已复制"的假象提交）。

**实测**（1c+3w，3 分片表每片一个 3 成员组，每台 worker 都是一个组的主 + 另两个组的从；采样间隔 0.5 s）：

| 负载 | 结果 | 环深度峰值 | 环满等待/丢弃 | 多数派丢弃 | 捕获环覆盖 |
|---|---|---|---|---|---|
| 单事务 INSERT 1500 行横跨 3 组 | 成功，每组日志推进 517–534 条（远超 128） | **2** | 0 / 0 | 0 | 0 |
| 8 会话并发各 10 个跨组事务（插 6 行 + 改 6 行） | 70 成功 / 10 失败（失败原因是 §7.1 切主活锁，非环） | **2** | 0 / 0 | 0 | 0 |
| 跨 3 组事务夹一个重复主键 | 整条回滚，一行不落 | — | — | — | — |

**环真正会满的只有两种情形**，调大容量都不解决问题：

1. **apply 卡住**（最常见是 follower 没有本地分片站点：日志刷 `group N 在本节点没有对应分片，无法落盘`）——
   127 条后必满，调大只是晚一点满。正解是用 `partdist.provision_shard_replica()` 把副本供好。
2. **单组并发提案 >127**：复制已按组串行，只有控制面/决议类提案会并发追加；`max_connections=100` 下不会触发。

**结论：不改容量。** 若将来确需调整，可照 T7.23（`pg_raft.max_groups`）把它改成 PGC_POSTMASTER GUC；
代价是共享内存 ≈ 0.8 KB × 容量 × 组数 × 节点数，且要 clean rebuild + 跑切主套件。

**盯什么 / 何时动手**：

- `ring_full_waits` 增长：apply 变慢，查丢弃 WARNING 的 errdetail 里的 `apply 最近一次失败原因`
  （`本节点没有该组对应的分片` / `拿不到 SPI` / `follower_set_applied_part_lsn 执行失败` / 被其他 backend 长时间占着）。
  `last_apply_fail` 没有 SQL 出口，只在这条 WARNING 里。
- `ring_full_drops` / `quorum_drops` 增长：有事务因此中止；数据条目被丢时 `last_drop_plsn` 记下位置（环满丢弃**不写**它）。

### 2.2 分叉标记与修复

- 标记是文件 `pg_parwal/<oid>/diverged`，`partdist.shard_divergence(oid)` 查询（无标记返回 NULL）。
  写入来源五处：复制挂钩失败、捕获环溢出、peer WAL 回读失败、事务中止时排空失败、副本收到内容不一致的重传。📖读码
- **自动修复** ✅（T7.27 起）：`pg_partdist.auto_repair_diverged=on`，心跳按 `pg_partdist.auto_repair_interval_s=60` 节流调用
  `partdist.repair_diverged_shards()`；多数派不在（`pg_raft_group_quorum_alive()` 为假）时**等**而不是发基线（P7-W6）。
  结果非零时日志：`pg_partdist: 分叉标记自动修复：repaired=… skipped=… stale_cleaned=…`。
- **手工修复**（超级用户）：leader 上 `SELECT partdist.repair_diverged_shards();`，或针对单分片
  `SELECT partdist.shard_baseline_emit('<分片表>'::regclass);`。
- ⚠️ **副本侧的标记自动修不掉**（读码推断）：副本不是 leader，永远走"等"；leader 本机没标记也不会主动发基线。
  处置：leader 上 `shard_baseline_emit`，确认副本追平后，在副本上 `SELECT partdist.shard_clear_divergence(oid);`（只清标，不修复）。

### 2.3 捕获环：8192 槽/节点（全节点共享）

- 写路径（堆插入/更新/删除）在占用过 `pg_partdist.partwal_ring_high_water`（默认 50%，SIGHUP）时就地排空 ——
  **单个大事务写不爆环**（P7-W2/T7.27）。
- 不经写钩子的批量写（如大表 VACUUM）仍可能覆盖：`overwrites` 增长、分区被打分叉标记，交 §2.2 自动修复。

---

## 3. 分片 xid 回卷未实现：停发线

**边界**：分片 xid 是线性计数，没有取模回卷（DESIGN §7 写的 mod 2^32 未实现）。靠"龄"两级线让线性假设成立。📖读码

| GUC（SIGHUP） | 默认 | 到线行为 |
|---|---|---|
| `pg_partdist.shard_vacuum_max_age` | 200000000 | 发号时 WARNING `分片 %u 的 xid 龄 %u 已达 shard_vacuum_max_age %d`（每后端每分片一次），心跳自动触发分片 vacuum |
| `pg_partdist.shard_xid_stop_age` | 2146483648（2^31−10^6） | ERROR `分片 %u 的 xid 龄 %u 达到停发线 %d，该分片进只读`；读照常，截断点推进后恢复 |
| `pg_partdist.shard_vacuum_auto` | on | 自动分片 vacuum 开关 |

**盯什么**：`partdist.shard_xid_age(oid)` 的 `age/phase`；`partdist.shard_vacuum_watermarks(oid)`。

**快到线怎么办**：`partdist.shard_vacuum_target(oid, safe_ts, ceiling)` 查截断点被谁挡住（`stop_reason`）。
超龄 RUNNING 可以强杀；**PREPARED 未决的不许单方中止**（等决议或走 in-doubt 闭合）。没有回卷自救这条路。

**⚠️ 停发线只管"龄"，不管绝对号**（读码推断）：截断后龄会回落，但 next_xid 单调增长。另有两道绝对上限无预警：
发号逼近 `0xFFFF0000` 时 ERROR `…32 位 xid 逼近上限…P1 不支持回卷`；持久化水位 ≥ `0x80000000` 时，
下次重新挂槽（如重启）会被报成**水位文件损坏**（`…水位值不合理…`）。即一个分片累计发出约 2^31 个 xid 后重启即出问题。
gxid 是 48 位序号（耗尽 ERROR，无告警）；plsn 是 uint64，未见上限检查。

---

## 4. 升主之后必须重取基础备份（补丁 0010 运维裁定 (a)）

**边界**：升主时 `partdist.advance_wal_past_shard()` 会触发一次立即检查点，并把本地 WAL 插入位点跳到 leader 坐标所在段的下一段起点。
跳过的段永远不产生文件 ⇒ **原生归档/PITR 在升主处断链**：从升主前的基础备份出发，只能恢复到旧段末尾。📖读码

**判据**：看日志 `pg_partdist: 本地 WAL 插入位点自 X 推进到 Y（越过 leader 坐标 Z）`，DETAIL 末句
`但原生归档/PITR 在此处断链，须以本次升主为界重取基础备份。` ✅实测（09-17 升主时出现过）。
不是每次升主都跳：位点已越过目标（日志 `…无需推进`）、没有回放游标、走了 §1 的兜底放行，这三种都不跳。

**怎么做**：看到"推进到"日志 ⇒ 该节点立即重取基础备份，并把旧备份标记为"只能恢复到升主之前"。
补丁 0010 (c)：参与 raft 的节点**不支持挂原生物理备库**。当前测试集群 `archive_mode=off`、`wal_level=replica`。

**不许做**：❌ 用升主之前的基础备份做跨升主点的 PITR。

---

## 5. 打标分片禁 CREATE INDEX / REINDEX；LP_REDIRECT 未实现

**边界**：分片表登记为打标（分片级 MVCC）后，本节点禁止对它 `CREATE INDEX` / `REINDEX`（直接点名 `pg_toast_<oid>` 也拦）：
ERROR `P1: %s 不允许作用于分片打标表 "%s"`，DETAIL `原生 clog 会误判分片 xid…回收/重写路径会删除活元组。`📖读码

**正确顺序**：建表 → **建好全部索引** → 再登记打标 → 写入数据。登记后再建索引会被上面的守卫拒绝
（有无安全的补建路径未核实，别自行尝试撤销打标再建）。

**LP_REDIRECT**：分片 vacuum 遇到"带 HOT 链的死根元组且该关系有索引"时 ERROR
`分片 %u 第 %u 页第 %u 项是带 HOT 链的死根元组，而该关系有索引`。

**⚠️ 带索引的打标表会被 HOT UPDATE 卡住分片 vacuum**（`test_shard_vacuum_p5.sh` 可复现）：一次已提交的 HOT UPDATE
就会撞上面这个 ERROR，截断点推不动，xid 龄一路涨向 §3 的停发线。**对有索引、会被 UPDATE 的打标表，要把 §3 的龄当重点告警。**
DEV PLAN 里"用户索引仍被禁、只有 TOAST 可达"的理由与现行"先建索引再登记"的流程自相矛盾，待设计侧裁定。

未实测：`ALTER TABLE ... ADD PRIMARY KEY` 能否绕过该守卫。

---

## 6. 子事务禁写分片表

**边界**：`SAVEPOINT` / PL/pgSQL `EXCEPTION` 块内写打标分片表一律 ERROR
`P1 不支持在子事务（SAVEPOINT/EXCEPTION 块）内写分片打标表`，DETAIL `分片 %u 的整事务只绑定一个分片 xid，无法表达子事务局部回滚。`
（DESIGN §5.4 承诺的子事务支持未实现）。📖读码

**对应用的要求**：不要在带 `EXCEPTION` 的 PL/pgSQL 函数、ORM 的嵌套事务/保存点里写分片表；
需要"部分失败重试"的逻辑放到应用层整事务重试。

---

## 7. 截至 2026-09-17 会直接影响运维的未修缺陷

### 7.1 ★ 升主登记活锁（跨组事务高负载下切主后，路由永久指向旧主）✅实测

- **现象**：8 会话并发跨组事务时，08:31:43 组 102616 的主从 :5434 漂到 :5435；此后 :5435 每 ~17 s 打一次
  `升主前置超过 10000 ms 未返回或连接失效`，**从未登记成功**，`partition_map` 与 `pg_dist_placement` 一直指向旧主，
  写该分片的事务报 `本节点不是该分区组的 leader` 失败。
- **量化**（`track_functions=all`，90 s 窗口）：升主前置**平均 69 s/次**，其中 `dtx_close_indoubt` 平均 **51 s**；
  而 tick 只等 `slice + 8000 = 10000 ms`。
- **三处叠加成活锁**（📖读码确认）：
  1. 服务端完整跑完一遍 > 客户端超时 ⇒ 结果被丢弃，下一轮从头再来；
  2. 超时路径 `res == NULL` 直接返回，**不走截止期判断** ⇒ §1 的 60 s 兜底永远不触发；
  3. 超时只 `PQfinish` 不取消服务端查询 ⇒ 被抛弃的会话继续跑，实测同一组堆积到 **6 个**并发升主前置，互相拖慢。
- **连锁**：随后 102617、102616 又先后漂到 :5434、:5433，新主陷入同样循环（等待期间照发心跳的 P7-R5 并没能阻止连锁，机理未坐实）。
- **临时处置**：没有安全的纯运维绕行。可选：暂停写入降负载，让升主前置在 10 s 内跑完；
  或调大 `pg_raft.promote_catchup_slice_ms`（tick 等待上限随之变大，但 tick 阻塞更久、影响同节点其他组心跳，需评估）。
- **修复方向**（待立项）：tick 侧非阻塞跨 tick 轮询、超时计入截止期、放弃时 `PQcancel`、升主前置按 (组, term) 幂等快速返回、
  `dtx_close_indoubt` 复用连接并对查无决议的事务退避。

### 7.2 升主瞬间副本回放跳过记录（待定因）✅观测到

09-17 08:34:07 :5434 接手组 102617 时：先收到全量物理基线并截空本地 4 个文件，紧接着 plsn 1249–1252 四条记录因
`记录越出本地关系边界，跳过该记录（…实际块数=0）` 被跳过，随后该节点完成升主。夹具表已拆，**无法核实升主后的数据是否缺失**。
跨组事务验收要加一项：切主后在新主上逐分片核对行数与校验和。

### 7.3 其余登记项

见 `P7_REMEDIATION_PLAN.md` §1.11（09-15 全量回归 18 条 FAIL 的分类，含 `replica_gate_p6` 副本流内空洞真缺陷）。

---

## 附：文档与代码不一致清单（待回填）

| 文档位置 | 写的 | 实际 |
|---|---|---|
| `pg_raft.c:1151` GUC 描述、DTX_2PC_DESIGN.md:1358 | deadline 设 0 = 永不放行 | 立即放行（§1） |
| DESIGN:357 | 兜底放行前先扫已收未放记录头推水位 | 代码无此动作 |
| FRD:1784-1787、:1802-1805；DEV PLAN:5115；P7 计划"没有自动修复"；`shard_fileset.c:2288` 注释 | 没有自动修复 | T7.27 起心跳自动修复 |
| FRD:1905-1911；`raft_consensus.c:5576` 注释 | 丢弃会截掉 leader 的 parwal 字节 | 08-03 起不截（`discard_uncommitted_entry`） |
| `shard_divergence` 的 SQL COMMENT | 只列复制挂钩失败一种来源 | 五种来源（§2.2） |
| DESIGN §7 | 分片 xid mod 2^32 | 线性，无回卷（§3） |
| DESIGN §10（:664）、P7 计划出口清单 | 1 GB 基线上限需写规程 | T7.21 流式基线后已不存在（仅 chunk=0 时保留）；残留边界是 DDL 改动块数超 `fileset_inline_max_blocks` 时置 NEEDS_REBASELINE |
| DESIGN §10 引用的 `shard_vacuum.c:715-736`、`shard_xid.c:1213-1220`；P6_EXIT_AUDIT.md:77 | 行号 | 已漂移 |
