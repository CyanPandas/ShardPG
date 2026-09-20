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
| `组 N 本节点不可升主（升主前置返回 -1），主动让位并退避 X ms，让其余有副本的成员当选` | LOG | 紧跟上一条：该节点已让位（P7-N14）。若同组反复出现且始终没有别的节点登记成功，说明**没有一个成员有资格**（都没供副本 / 都分叉），要人工重做基线 |
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
- ~~P7-N10~~ **已修（2026-09-17，`4ce9775`）**：写集只有一个分片却走 2PC 的全局事务（如 `INSERT … SELECT` 只命中一片）
  此前会"提交成功、数据永久不可见"；现在照常做决议。跑旧 .so 的节点仍有此问题——升级后无需改应用。

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

> **2026-09-17 补：定因了。** 这是 P7-N16 的另一个截面：副本收到全量物理基线后把全部成员截成 0 块，FPI 还没到齐
> 就被选成主 —— 升主前置看它 armed 且已追平（那批 FPI 从未被 Raft 提交），放行。N12 复现 run 3 抓到完整现场：
> 新主的 pkey 文件 0 字节，所有走索引的语句报 `could not read block 0 … read only 0 of 8192 bytes`。见 §7.3 与 §8。

### 7.3 跨组分布式事务（2026-09-17 验收，均未修）

- ~~P7-N10（P0）~~ **已修（`4ce9775`）**。
- **P7-N12（P0）两个丢失机理已修（2026-09-17，`3a2fc1f` + 本批），但切主后仍可能小额总额漂移（→ P7-N18，未修）**：
  升主序列现在会把每一笔分片 clog 仍 PREPARED 的事务落判决并复制给副本；验收单次切主全绿（run 4 总额守恒、零残留）。
  **但一组内短时间连切两次**仍会因分片 xid 重号（N18）丢掉转账的一侧。
  **规避（修 N18 前）**：跨组事务承载的分片，同一组两次切主之间留足追平时间（等新主 `replay_status().applied` 追平到 Raft 提交上界再切下一次）；不要在负载峰值连续切主。
- **P7-N18（P0，未修）**：分片 xid 跨任期重号 ⇒ 判决落错事务 ⇒ 2PC 跨分片原子性破坏。
  **识别**：切主后不变量漂移，但各主本地自洽、无 PREPARED 残留；跨节点对读同一分片 xid 的 `shard_clog_status_full` 出现"同 xid 不同 st、不同 sts"。
  **处置**：以协调者/多数派为准重对账，对受影响分片重做基线。根因在分片 xid 分配层（冻结模块核心），修法待定。
- **P7-N11（P1）**：跨组 2PC 负载下频繁 `分片快照被栅栏作废：…未能向 TSO 续租`，事务失败需重试。**规避**：应用层对该错误重试；降低并发。
- **P7-N13（P1）**：2PC 回滚的判决没送到副本，副本分片 clog 永久 PREPARED；副本升主后若决议已被遗忘则永远未决。与 N12 同一通道。
- **P7-N15（P1）**：`provision_shard_replica` 发物理基线期间 leader 心跳断档，follower 选举超时（默认 6 s）后另选新主，
  供给失败、半程副本悬在"截断待 FPI"。**规避**：供副本前把该组所有成员的 `pg_raft.election_timeout_ms` 抬到 15000
  （`ALTER SYSTEM` + reload），供完 RESET；供副本期间不要做任何切主。
- **P7-N16（P0）**：升主前置放行"基线已截断、FPI 未到齐"的副本，新主是空壳（索引 0 字节；基线前已有行则堆也是 0 块 ⇒ 整片丢）。
  **识别**：升主后该分片任何走索引的语句报 `could not read block 0 in file "base/<db>/<relfilenode>": read only 0 of 8192 bytes`，
  新主日志里紧挨着有 `收到全量物理基线，已截断全部 N 个本地文件，等待 FPI 重建` 而没有后续 FPI 应用。
  **处置**：立即把主切回持有完整数据的成员（旧主若未被交接覆盖）并对空壳节点重做基线；若旧主已按新主 fileset 交接，数据只能从基础备份找回（§4）。
  **规避**：同 N15 —— 根本上不要让供给中的副本参选。
- 其余（09-15 全量回归 18 条的分类、P7-N1 副本流内空洞等）见 `P7_REMEDIATION_PLAN.md` §1.11。

### 7.4 受控切主 / 被降级的旧主（2026-09-18）

- **受控切主只动一个组**：在目标节点上 `SELECT partdist.pg_raft_group_campaign(<group_id>);`，每 5 s 重发一次直到该节点
  `pg_raft_group_status()` 里该组 `state='leader'`，再等协调者 `partdist.partition_map.primary_node` 变成目标节点。
  ❌ **不要**再用"旧主 `pg_raft.heartbeat_ms` 调大 + 目标 `pg_raft.election_timeout_ms` 调小"——两个 GUC 作用于**节点上全部组**，
  实测一次把 6 个组全压到同一台，升主前置串行排队，目标组排不到号就丢主（P7-N24）。
- **切主目标必须有 armed 回放槽位**：`SELECT armed FROM partdist.replay_status() WHERE shard = partdist.local_partition_for_shard(<gid>)` 为 `t`。
  被降级的原始主本来没有（P7-N25）。**2026-09-18 起自动处理**：新主登记后会拉起一次性工作者（日志 `已拉起自动归队工作者`），
  前任主没有槽位就替它重供基线（`自动归队 … 第 N 次：done: …`），通常几秒内完成；旧主宕机时每 15 s 重试约 5 分钟。
  关掉：`pg_partdist.auto_reprovision_demoted = off`。自动路径失败或关掉时，人工在当前主上 `SELECT partdist.reprovision_demoted(<gid>, <node_id>)`
  （会先判定要不要供）或直接 `provision_shard_replica(<gid>, <node_id>)`。
- **P7-N26（已修）识别**：旧版本里切回一个当过主的节点后，写入报 `等待组 N 的复制认领位超过 60000 ms，prepare 失败`，持有者是新主上正在做
  主权交接广播的 backend；该组 `partdist.raft_log` 新任期里出现与上一任期同 plsn 的大批 OP_PARWAL。新版本当选即打 `当选后把复制下界从 plsn X 抬到 Y`。
- **P7-N27（已修）识别**：旧版本里 `apply partition primary … old_primary=0 new_primary=N` 出现在一个早已登记过主的分区上，且被取代的旧主日志里
  没有 `主权已交给节点 N`——它本地仍以为自己是主。新版本 apply 时以本地上一条登记为准，打 `登记条目里的前任主=0 与…不一致，以后者为准`。
- **P7-N32（已修）回放流有空洞的节点不再被兜底登记**：日志 `回放流有空洞、永久追不上，拒绝升主（兜底也不放行…）` + `主动让位`，由追平的成员当选；
  新主登记后的自动归队会把"armed 但回放 failed"的成员重供基线（日志 `没有可用回放槽位（未 armed 或回放 failed）`）。旧版本的危险形态：切主后读到的行数变少、
  某节点日志有 `升主前置超过 60000 ms 仍未追平，转入兜底` 且其回放报 `流内空洞`——那一刻登记的是残缺数据，须从健康副本重建。
- **P7-N1 之二（已修）空洞成因**：旧版本在被降级的原始主上替换孤儿记录时按段名顺序截断，会删掉新主发来的已提交记录（`流内空洞：期望 N，下一条已存在的是 M`）。
  新版本逐段逐记录截断；旧版本遇到此症状：在当前在册主上 `SELECT partdist.provision_shard_replica(<gid>, <该节点>)` 重供。
- **P7-N21（已修，2026-09-19）清场时 `DROP TABLE` 卡几分钟到几十分钟**：旧版本的形态是 DROP 长时间 active、`wait_event` 为空、CPU 高、杀不掉——
  那不是自旋，是**被 hearsay 复活的空壳组**在提交路径上把整条分区流历史逐条重提（每条一次 RPC；`/proc/<pid>/io` 的 syscr 不计 socket 收发，所以看着像零 syscall）。
  新版本拆组记墓碑（10 分钟内在途心跳 / 投票不会把它再建出来），且重提循环查中断。旧版本遇到：在**所有**成员上拆掉该组后再等它返回，或 `pg_ctl -m immediate restart` 该节点。
  新版本拆过组的节点收到心跳时不再打 `创建 Raft 组 N`；要把组合法建回来就显式 `SELECT partdist.pg_raft_group_create(<gid>, <成员>)`（清墓碑）。
  **通用取证工具仍保留**：`ALTER SYSTEM SET pg_partdist.debug_sigusr2_backtrace = on` + reload（在要取证的语句开始**之前**开），卡住后
  `docker exec -u postgres <容器> kill -USR2 <pid>`，日志里出现 `pg_partdist: SIGUSR2 采样 pid N 栈回溯：`，进程继续跑；
  `xxx.so(+0xOFF)` 用 `gdb -batch -ex 'info symbol 0xOFF' <该 .so 路径>` 解析。
- **P7-N29（已修）控制面 apply 随事务回滚而丢失**：新版本回滚时打 WARNING `承载控制面 apply 的事务回滚，apply 游标从 X 退回 Y，这些条目将重新 apply`，属正常自愈。
  旧版本的症状：某节点 `partdist.partition_map` 缺行 / 停在旧主，而 `pg_raft_group_status()` 的 group 0 applied 与其它节点相同——须重建该节点的控制面状态。
- **P7-N31（已修）切主一瞬的拒读**：新版本切主时经协调者的读可能**多等最多 15 s**（等新主本地登记生效），不再报
  `不允许在本节点上对副本壳表…执行查询`。若切主后仍持续拒读超过 15 s，说明新主本地登记没落地（看它的 `partition_map` 与 group 0 applied）。
- **识别 P7-N22（已修，旧版本才会见到）**：副本回放日志每 250 ms 一条 `追平失败: unexpected data beyond EOF in block N of relation …`，
  多见于"当过主又降回副本"的节点收到全量基线之后；或同类节点回放静默跳过页面、逐字节比对不一致。旧版本处置：重启该节点的回放 worker
  （整节点重启）后重供基线；升级到含 `ReplayForgetCachedSizes` 的版本即根除。
- **识别 P7-N23（已修）**：前任主再次当选后日志连续出现 `升主追平 … 失败: replay_catchup: shard … 已升主，不再回放别人的流`，
  60 s 后才 `兜底 force` 登记。新版本改为一条 LOG `本节点仍持已升主身份且未收到他人写的记录，… 跳过追平`，立即登记；
  若它升主以来收到过他人写的数据/提交记录，则仍按原路追平/兜底（不放宽）。

- **P7-N33（已修，2026-09-20）切主窗口里旧主的拒读**：与 N31 是同一条缝的另一侧——N31 是新主还没 apply 就被路由指过来，N33 是**旧主已经 apply、协调者的路由还没挪走**。
  新版本：旧主刚降级、且本地还没回放过新主的流时**放行读**（这一格就是它交出主权那一刻的已提交状态），一旦回放推进或宽限到期即恢复拒读。
  宽限 GUC：`pg_partdist.demoted_read_grace_ms`（默认 `10s`，PGC_SIGHUP，设 0 = 修前行为）。
  ⚠️ 它只保「降级 → 被新主重新供成副本」这一段；副本流一到、本地回放一推进，宽限按设计失效（那之后它的页正在被新基线覆盖，本来就不能读）。
  若切主后拒读持续更久，是协调者那份登记没 apply 上去（见 N37），按下一条排查。
- **P7-N34（已修，2026-09-20）宕机切主后分钟级不可写**：旧版本的形态是新主上的写入报 `等待组 N 的复制认领位超过 60000 ms，prepare 失败`，
  反复几轮共约 6 分钟；持位者是升主的 `repair_diverged_shards`，等位者是**握着 partition_map 行锁**的控制面 apply（同一后端时就是在等自己）。
  新版本：认领位可重入、持有者是自己时直接回收、控制面 apply 里等位上限 3 s（日志 `复制认领位超过 3000 ms … 本次控制面 apply 放弃（回滚后由下一次 AE 重做，P7-N34）`，属正常让路，不是故障）。
  巡检口径：**全集群不该再出现 `复制认领位超过 60000 ms`**；出现即回到旧版本行为或另有长事务持位。
- **P7-N35（已修，2026-09-20，P0）切主丢已确认的提交**：旧版本的形态是宕机切主后，客户端已收到成功的最后一笔不见了，
  新主的分片 clog 里那个分片 xid = `st=3 ABORTED`，而新主分区流里该 plsn 的提交标记明明带着这个 xid；新主升主日志停在 `追平至 <较小的位点>`。
  新版本：升主前置先提交一条本任期空条目，追平上界取 `max(本地已应用位点, partdist.pg_raft_group_committed_plsn(<gid>))`，
  抬高时打 LOG `追平上界按已提交位点抬高：applied=X → committed=Y（P7-N35）`。
  取证用：`SELECT partdist.pg_raft_group_committed_plsn(<gid>)`（Raft 已提交的最大数据位点）对比 `partdist.get_follower_applied_part_lsn(<loid>)`（本地 apply 游标）。
- **P7-N36（已修，2026-09-20）重启后同一个组两个槽位**：旧版本的形态是日志里同一毫秒两行 `创建 Raft 组 N（槽位 i）`，
  多出来的空壳反复竞选把任期顶上去。排查：`SELECT group_id, count(*) FROM partdist.pg_raft_group_status() GROUP BY 1 HAVING count(*)>1;`
  旧版本的处置是再重启一次该节点（槽位只在共享内存里）；新版本建组在锁内复查，不会再出现。
- **P7-N37（已修，2026-09-20）控制面 apply 一错就跳过条目**：旧版本里控制面条目 apply 抛错也算「已应用」、游标照推，
  一次瞬时失败（锁超时/死锁/临时资源不足）就让该节点**永久**错过这条登记——典型后果是它的 `partdist.partition_map` 停在旧主、路由指错节点，直到该分片下一次切主。
  旧版本的形态：日志 `… apply 抛错：<原因> —— 控制面按既有语义跳过`，此后该节点 partition_map 与其它节点分歧而 group 0 的 applied 却一样。
  新版本：同一条目保留游标重试，日志 `… —— 保留游标，下轮重试（第 N 次，已持续 M s，P7-N37）`；连续失败超过 60 s 才跳过并打
  `重试已超时，按既有语义跳过（本节点将永久错过这条登记）` —— **看到这一条就要人工核对该节点的 partition_map**。
  **代价要知道**：重试期间该节点的控制面游标停在这一条上，后面的 group 0 条目一起排队（最长 60 s 这个节点的登记是旧的）。
  这与数据面一直以来的语义一致（apply 不成功不推游标），换来的是"不再静默丢登记"。

## 8. 扩展升级（换 .so）的固定顺序

1. `scripts/sync_build.sh`（带守卫：内容比对同步、头文件变了全量重编、导出符号核对）。
2. **看协调者 `SHOW pg_partdist.tso_master`**：为 on 时先 `rm $PGDATA/pg_tso_boot`（§0 规则 1 的整簇版），否则重启后 TSO 拒绝发号，
   随后**任何**触及打标分片的 2PC —— 包括 Citus `DROP TABLE` 下发给 worker 的 `COMMIT PREPARED` —— 取不到 commit_ts、fail-closed，
   每个 worker 各留一笔 prepared 锁死分片表（`pg_blocking_pids` 给 `{0}`）。已经中招：删标记重启协调者 → `SELECT recover_prepared_transactions()`。
3. **整簇重启**（pg_partdist 在 `shared_preload_libraries`，只重启部分节点会共享内存布局错位）。
4. **重启之后**再跑 `scripts/refresh_extension_sql.sh`。反过来会报 `could not find function "…" in file pg_partdist.so`：
   重启前所有后端映射的还是旧 .so，新符号看不见；不依赖新符号的声明会先装进去，库处于"装了一半"。
5. 验证：每个节点 `SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='partdist' AND p.proname IN (<新函数>)`。
6. **pg_raft 的 SQL 不在第 4 步里**：`refresh_extension_sql.sh` 只对齐 pg_partdist 的函数面。pg_raft 新增/改动的函数要逐节点重放——
   小函数用 `scripts/apply_pg_raft_sql.sh <函数名>`；**函数体含空行的大 plpgsql（如 `pg_raft_promote_prepare_ex`）** 该脚本只抽到首个空行，
   须按 `CREATE OR REPLACE FUNCTION <名>(` 到 `$<tag>$;` 抽全块，`SET search_path=partdist; SET citus.enable_ddl_propagation=off;` 后逐节点执行，
   新函数再 `ALTER EXTENSION pg_raft ADD FUNCTION …` 入籍。

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
