# 运维规程：已裁定接受的边界

> 版本：2026-09-17，对应代码 `shardpg-test` @ `d9e792a`（§1、§2.4、§7.1 随 P7-N4/N6 修复更新）。
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

**边界**：数据组新 leader 向控制面登记前，要先跑"升主前置" `partdist.pg_raft_promote_prepare_ex(gid, slice_ms, force)`：
回放追平 → 推进本地 WAL 位点 → 闭合 in-doubt 分布式事务 → 修复分叉 → 认领分片。
追不平时每轮只推进一片；自首次发出起累计超过截止期仍"未追平"，就转入**兜底**：不再以追平为前提、
其余升主步骤照做，然后放行登记（可用性优先），新主可能尚未追平。
📖读码（`raft_consensus.c` `data_group_promote_prepare` / `data_group_try_report`，SQL 见 `pg_raft--1.0.sql`）

> **2026-09-17 起（`d9e792a`，P7-N4/P7-N6）行为有三处变化**，旧日志与旧文档按旧语义理解：
> ① 后台 tick 不再同步等待升主前置与登记 —— 发出后每个 tick 只非阻塞收结果，结果一定被收下；
> ② `promote_catchup_deadline_ms = 0` 现在是**永不兜底**（修复前实为"第一次没追平就立即放行"）；
> ③ 兜底现在**照做**推进 WAL / 闭合 in-doubt / 修分叉 / 认领（修复前一步都没做）。分叉或无副本（返回 -1）兜底绕不过。

**参数**（均 SIGHUP）：

| GUC | 默认 | 范围 | 说明 |
|---|---|---|---|
| `pg_raft.promote_catchup_deadline_ms` | 60000 | 0–3600000 | 截止期，按 (组, term) 自首次发出升主前置起算；0 = 永不兜底 |
| `pg_raft.promote_catchup_slice_ms` | 2000 | 100–60000 | 每轮追平时长 |

另有两个编译期上界：单次升主前置 300 s（超了 `PQcancel` 重来）、单次登记 30 s。

**盯什么**（日志，无计数器/视图）：

| 日志 | 级别 | 含义 |
|---|---|---|
| `group N leader node X term T 已向控制面(node 1)登记` | LOG | 正常结束。当选（`当选 LEADER term=`）到这条之间就是登记耗时 |
| `组 N 升主前置超过 %d ms 仍未追平，转入兜底：下一轮不再以追平为前提` | WARNING | 进入兜底 |
| `组 N 升主前置超过 %d ms 仍未追平，已按可用性优先放行上报（跳过了追平要求，其余升主步骤已执行；…）` | WARNING | 兜底放行，**新主可能缺数据** |
| `组 N 单次升主前置已运行超过 300000 ms，取消后重来` | WARNING | 升主前置本身卡住（锁等待之类），要查 |
| `组 N 已不再是 term T 的 leader（现 term T'），取消在途升主前置/登记` | LOG | 升主途中又被推翻，正常 |
| `分片 (组 N) 拒绝升主：…` / `检测到快路径分叉` | WARNING | 返回 -1，永不放行，需重做物理基线 |
| ~~`升主前置超过 %d ms 未返回或连接失效`~~ | — | 修复前的活锁特征，新代码不再打；若再见到说明节点跑的是旧 .so |

**何时动手 / 怎么做**：

1. 看到"兜底放行"：该分片新主可能缺数据。对照旧主（若还活着）核对行数/校验和；有差异则以多数派为准重做基线
   （leader 上 `partdist.shard_baseline_emit('<分片表>'::regclass)`）。
2. 当选之后迟迟不见"已登记"：先看是不是反复"不再是 term T 的 leader"（选举抖动，查 CPU 与 `election_timeout_ms`），
   再看有没有"单次升主前置已运行超过"（卡在某一步）。
3. 不想在任何情况下放行未追平的副本（宁可分片暂时无主）：把 `promote_catchup_deadline_ms` 设为 0。

**参考耗时**（✅实测，1c+3w、2 vCPU、流长 1569 条、`test_promote_register_p7n4.sh`）：当选 → 登记 **12.7 s**。
修复前同一场景 150 s 内从未登记（升主前置单次 69 s，其中 `dtx_close_indoubt` 51 s）。

**注意**：
- 升主途中如果节点丢了领导权，在途的升主前置会被取消，但**已经执行完的步骤不会回滚**（比如已经推进了 WAL 位点、
  已经认领了无主 RUNNING）。这些步骤本身幂等，下一个真正当选的节点会再做一遍。

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

### 2.4 分区流按编号读记录的稀疏索引（2026-09-17，P7-N4/N7）

复制取字节、follower 查重、升主前置的 in-doubt 扫描与分叉检查，都按 plsn 从分区流里读记录。原实现每读一条都从流开头扫起
（长流上整体退化成 O(n²)），现在每个 backend 为最近用过的分区记一份稀疏索引（每 32 条一个检查点），**检查点先核对记录头才用，
核对不过或没找到一律退回原全扫描**。

| GUC | 默认 | 级别 | 说明 |
|---|---|---|---|
| `pg_partdist.partwal_record_index` | on | USERSET | off = 退回原全扫描。只用于排障与正确性对照 |

✅实测（流长 1569 条）：升序/降序/乱序全量读与关索引读逐条指纹一致；抽样读每条 13.3 ms → 0.56 ms。
怀疑读到错误记录时：在同一会话里 `SET pg_partdist.partwal_record_index = off` 重读对照。

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

## 5b. 参与 Raft 组的分布表必须先打标

**边界**：只有**打标**（分片级 MVCC，`partdist.set_table_shard_mvcc`）的分片，切主后新主上的数据才可信。
未打标的分片副本属于"遗留宇宙"：元组里是旧主的原生 xid，升主后新主拿自己的 clog 判可见性，结果不可信
（FOLLOWER_REPLAY_DESIGN §14.2）。**运行期没有拦截**（P7-N9）：未打标的表照样能建组、供副本、切主、读写，只是读到的数据不对。
✅实测（09-17）：未打标表在跨组负载下切主后，账户总额 24000 变 26000，新主本地读与协调者读同一分片结果不同。

**怎么做**：建表 → 建组并供副本 → **表还空着时**在协调者上 `SELECT * FROM partdist.set_table_shard_mvcc('<表>');`
（返回每个分片一行，status 应为 `registered …`，重跑为 `already`）→ 再写入。非空表会被拒绝登记。
核对：各主节点 `SELECT partdist.shard_mvcc_status(partdist.local_partition_for_shard(<shardid>));` 应为
`registered=yes evidence=yes replica=no`，副本上为 `registered=no evidence=yes replica=yes`。

**不许做**：❌ 给未打标、已有数据的分布表建 Raft 组并依赖切主。

**打标表上的分布式事务**（✅实测）：
- 写多个分片的事务必须加入全局事务：事务开头 `SET LOCAL citus.propagate_set_commands = 'local'; SET LOCAL pg_partdist.join_info = '<gxid>,<start_ts>,<协调组>'`，
  其中 gxid / start_ts 在协调者上用 `partdist.partdist_gxid_next()` / `partdist.partdist_tso_client_start_ts()` 取；TSO 须已配置
  （协调者 `pg_partdist.tso_master=on`，全部节点 `pg_partdist.tso_conninfo`）。不带 join 的 2PC 写会在 PREPARE 被拒（安全）。
- 打标行**不是提交即可见**，要等协调组判决收敛（后台自动，最多数秒；排障时可手动 `SELECT partdist.dtx_pending_sweep()`）。
- ❌ **P7-N10（未修）**：在带 join 的全局事务里，**不要用 `INSERT … SELECT` 往单个分片写**（或任何"写集只有一个分片却走 2PC"的写法）——
  会报告提交成功、数据却永久不可见，切主时还会被改判中止。改用多行 `VALUES`，或让事务真正跨多个分片。

---

## 6. 子事务禁写分片表

**边界**：`SAVEPOINT` / PL/pgSQL `EXCEPTION` 块内写打标分片表一律 ERROR
`P1 不支持在子事务（SAVEPOINT/EXCEPTION 块）内写分片打标表`，DETAIL `分片 %u 的整事务只绑定一个分片 xid，无法表达子事务局部回滚。`
（DESIGN §5.4 承诺的子事务支持未实现）。📖读码

**对应用的要求**：不要在带 `EXCEPTION` 的 PL/pgSQL 函数、ORM 的嵌套事务/保存点里写分片表；
需要"部分失败重试"的逻辑放到应用层整事务重试。

---

## 7. 截至 2026-09-17 会直接影响运维的缺陷

### 7.1 升主登记活锁 —— **已修（2026-09-17，`d9e792a`）**

跨组事务高负载下切主后，新 leader 反复"升主前置超过 10000 ms 未返回或连接失效"、从未登记，路由一直指着旧主，写该分片全失败，
并连锁把同节点领导的其他组拖下马。根因与修法见 `P7_REMEDIATION_PLAN.md` §1.11 / T7.35；运维侧的变化见本文 §1。
确定性复现用例 `test_promote_register_p7n4.sh`：修复前 10/6，修复后 19/0（当选→登记 12.7 s）。

### 7.2 升主瞬间副本回放跳过记录（待定因）✅观测到

09-17 08:34:07 :5434 接手组 102617 时：先收到全量物理基线并截空本地 4 个文件，紧接着 plsn 1249–1252 四条记录因
`记录越出本地关系边界，跳过该记录（…实际块数=0）` 被跳过，随后该节点完成升主。夹具表已拆，**无法核实升主后的数据是否缺失**。
跨组事务验收要加一项：切主后在新主上逐分片核对行数与校验和。

### 7.3 跨组分布式事务（2026-09-17 验收，均未修）

- **P7-N10（P0）**：带 join 的全局事务里写集只有一个分片却走 2PC（`INSERT…SELECT`）⇒ 提交成功、数据永久不可见。**规避**：用多行 VALUES（见 §5b）。
- **P7-N12（P0）**：跨组并发事务期间发生切主（自发漂主或人工切主）后，已观测到账户总额不守恒、新旧主数据不一致、旧主打标身份未收回。
  **规避**：目前没有运维绕行；在修复前，**不要在承载跨组事务的业务上依赖切主后的数据正确性**；切主后以多数派重做基线并核对。
- **P7-N11（P1）**：跨组 2PC 负载下频繁 `分片快照被栅栏作废：…未能向 TSO 续租`，事务失败需重试。**规避**：应用层对该错误重试；降低并发。
- **P7-N13（P1）**：2PC 回滚后副本分片 clog 永久"运行中"，在副本/新主上读到对应元组报 `…是无主 RUNNING`。**规避**：重试；必要时对该分片重做基线。
- 其余（09-15 全量回归 18 条的分类、P7-N1 副本流内空洞等）见 `P7_REMEDIATION_PLAN.md` §1.11。

## 附：文档与代码不一致清单（待回填）

| 文档位置 | 写的 | 实际 |
|---|---|---|
| `pg_raft.c:1151` GUC 描述、DTX_2PC_DESIGN.md:1358 | deadline 设 0 = 永不放行 | ~~立即放行~~ **已按文档修正（`d9e792a`），代码与文档现在一致** |
| DESIGN:357 | 兜底放行前先扫已收未放记录头推水位 | 代码无此动作 |
| FRD:1784-1787、:1802-1805；DEV PLAN:5115；P7 计划"没有自动修复"；`shard_fileset.c:2288` 注释 | 没有自动修复 | T7.27 起心跳自动修复 |
| FRD:1905-1911；`raft_consensus.c:5576` 注释 | 丢弃会截掉 leader 的 parwal 字节 | 08-03 起不截（`discard_uncommitted_entry`） |
| `shard_divergence` 的 SQL COMMENT | 只列复制挂钩失败一种来源 | 五种来源（§2.2） |
| DESIGN §7 | 分片 xid mod 2^32 | 线性，无回卷（§3） |
| DESIGN §10（:664）、P7 计划出口清单 | 1 GB 基线上限需写规程 | T7.21 流式基线后已不存在（仅 chunk=0 时保留）；残留边界是 DDL 改动块数超 `fileset_inline_max_blocks` 时置 NEEDS_REBASELINE |
| DESIGN §10 引用的 `shard_vacuum.c:715-736`、`shard_xid.c:1213-1220`；P6_EXIT_AUDIT.md:77 | 行号 | 已漂移 |
