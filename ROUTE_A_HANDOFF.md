# 路线 A 交接文档（2026-10-17）

> 给下一个会话：先读这份，再读本机记忆 `shardpg-route-a.md`、`lab-access-and-rules.md`。
> 本文只写**现状、做法、下一步**；推导过程见提交 f502735 的说明与代码注释。

## 1. 目标与约束

- 目标：在保住 **多分片、高可用、全局一致性（TSO MVCC）、跨节点提交原子性（DTX 2PC）、跨节点读隔离** 的前提下，
  让 ShardPG 的 TPC-C 性能尽量接近 Citus 三副本（64 迷你仓：t128 105.7k / t192 118.6k / t256 128.9k / t384 125.3k / t512 124.7k tpmC）。
- 对照必须公平：同一套事务函数、READ COMMITTED、shared_buffers 8GB、同样的副本结构（每节点一主两备，ANY 1）。

## 2. 路线 A 架构（现状）

- 每个 worker（及可选的协调者）= 1 主 + 2 个 PG 物理流复制备库（另两台机器上），`synchronous_standby_names = ANY 1 (...)`。
- 数据组单副本：`pg_partdist.single_replica_fast_commit=on`、`pg_raft.single_member_fast_commit`，不再走 Raft 数据复制。
- Raft 只做控制面（group 0）：节点地址、备库登记、切主编排。
- TSO 发 start_ts / commit_ts；DTX 2PC 负责跨节点提交。
  - 原来 TSO 只在协调者上（`pg_partdist.tso_master`），协调者一挂全簇不能读写。
  - **2026-10-19 起可选 `pg_partdist.tso_raft = on`**：发号者 = 控制面 leader（平时偏向协调者），协调者宕机后由新 leader 接着发号。设计见 `pg-partdist-src/docs/TSO_RAFT_DESIGN.md`，lab 部署用 `tpcc/lab/tsoraft_conf.sh on`。

### 关键代码

| 位置 | 作用 |
|---|---|
| `pg-partdist-src/src/shard_wal.c`、`include/shard_wal.h` | 自定义 WAL rmgr 147：2PC PREPARED / 判决 / TSO 上界记 WAL，备库回放重建分片 clog 与未决登记 |
| `pg-partdist-src/src/tso.c` | TSO 上界记 WAL、恢复中拒发号、首次服务重置 GlobalSafeTs 冻结期 |
| `pg-partdist-src/src/shard_visibility.c` | 可见性；PREPARED 且"未决"时严格读等决议（任务 #13 修复）；`pg_partdist.diag_visibility` 诊断 |
| `pg-partdist-src/src/dtx_pending.c` | 判决落账；前台不等同步备库，后台清扫等持久后注销登记（`PendingCtl->vlsn`） |
| `pg-partdist-src/src/wal/partwal_sync.c` | WAL 插入钩子的映射查询本地缓存（`relmap_gen`） |
| `pg-partdist-src/patches/0016-elr-syncrep.patch` | 提前放锁与同步复制共存（已进构建） |
| `pg-partdist-src/patches/0017-diag-shard-chain.patch` | 顺链放弃诊断（留档，**不进构建**） |
| `pg-raft-src/src/raft_failover.c` | 自动切主控制器、TSO 连接串对齐、旧主自我隔离、`pg_raft_propose_node_standby` |
| `pg-raft-src/src/raft_consensus.c` | 节点地址覆盖、hardstate 记 WAL（rmgr 148）、DTX 恢复对 Citus 目录事务的闭合 |
| `pg-raft-src/src/topology_monitor.c` | 维护进程：地址同步、TSO 串对齐、隔离检查、切主 tick |
| `pg-raft-src/sql/pg_raft--1.0.sql` | `node_address(tli)`、`node_standby` 表与提议函数 |
| `pg-partdist-src/src/tso.c`、`tso_client.c` | 2026-10-19：`tso_raft` 发号状态（任期 / 租约 / 已 apply 上界）、多地址客户端与换届重试 |
| `pg-raft-src/src/raft_consensus.c` 末尾 | 2026-10-19：TSO 管理进程 `pg_raft_tso_main`（租约确认、接任、续批、发号者回协调者）、`OP_TSO_HWM`、快照带 `tso_hwm` |
| `pg-partdist-src/src/shard_xid.c`、`shard_wal.c` | 2026-10-19：分片 xid 水位推进记 WAL（`XLOG_SHARD_XID_WM`），备库回放抬高水位文件；恢复期读不挂槽 |

GUC：`pg_raft.auto_failover`（默认 off，`shardpg_rep.sh register` 打开）、`pg_raft.failover_probe_ms`=2000、`pg_raft.failover_fail_threshold`=3、`pg_partdist.diag_visibility`（默认 off）。

## 3. 已验证（lab 三机，64 迷你仓）

| 场景 | 结果 | 脚本 |
|---|---|---|
| 停 worker 主库（无人工） | 5.7 s 路由、18 s 全部可读写；快照一致、一致性 0 | `routea_autofailover_round.sh` |
| 停协调者 | 15 s 恢复；worker 侧协调者位置同步、无悬挂 2PC；TSO 单调 | `routea_coordfailover_round.sh`（COORD_HA=1） |
| 负载中 docker kill 主库 | 6.5 s 切主；无悬挂 2PC / 未决登记；一致性 0 | `routea_killload_round.sh` |
| 旧主拉起 | ~10 s 自行停机（时间线低于接替者） | 同上 |
| 补回副本 | `shardpg_rep.sh rejoin n3` 后两备库 quorum；一致性 0 | `commits_rejoin_check.sh` |
| 快照隔离修复 | 4 轮 128–256 终端零违例零中止；46 次"旧逻辑会判错"被纠正 | `diag_round.sh` |
| 协调者宕机（`tso_raft`，10-19） | 负载从 n2 进，kill 协调者：0 失败 0 中止，约 8 s 无吞吐后由 n2 接着发号；一致性 0；协调者回来 28 s 后发号者回到它。旧版同场景 65 个客户端全部中止 | `tsoraft_coordkill.sh` |
| 4 个 worker 全面检查（10-20） | 入口 / TPC-C / 读隔离 / kill worker / kill 协调者 七项全过；切主后 8 s 各 worker 路由元数据更新 | `redeploy5w.sh`、`health5w_check.sh` |
| 384+ 终端（节点内 TSO 代理，10-20） | 4 台机器 384 / 512 终端 82.3k / 75.0k（修复前协调者连接耗尽、结果无效）；协调者上取号连接固定 9 条，`too many clients` 0 次；kill 协调者 0 失败、9 s 空窗 | `validate_proxy.sh` |
| 切主后分片 xid（10-19） | 未提交事务 + kill 主库：新版不重发、0 行复活；旧版重发 S 且 20 行复活 | `tsoraft_sxid.sh` |

## 4. 性能现状（热身后，零违例）

| 终端 | ShardPG | Citus | 比例 |
|---|---|---|---|
| 128 | 67–71k | 105.7k | ~65% |
| 192 | 80–84k | 118.6k | ~70% |
| 256 | 84–95k | 128.9k | ~70% |
| 384 | 84–86k | 125.3k | ~68% |

256 终端对照剖析（`compare_profile.sh`）：

- 两边主机 CPU 都 ~20%（不是资源瓶颈），ShardPG 每笔事务 CPU 约 1.4×。
- Citus worker 会话主要等行锁（已顶到 64 仓天花板）；ShardPG "无等待事件"会话是 Citus 的 7–8 倍，行锁等待只有 1/3。
- gdb：约 1/5 停在 `shard_xid_state_ex` 的 `pg_usleep(1 ms)` 轮询（读者等"提交中"写者落判决）。
- 协调者每笔事务 3.4 次 TSO RPC（Citus 1.0）；WAL 每笔 1.2–1.6 KB（Citus 0.8–1.2 KB）；148 上 PartWAL 全局锁等待 ~8%。
- 轮次间波动 ±5–10%；每轮恢复后的第一次跑是冷启动（偏低），对比前先热身。

2026-10-19 同口径重测（全新部署，每档两遍，`TSO_RAFT_REPORT_20261019.md` §3）：ShardPG 新版（`tso_raft`）8 轮平均 77.2k、旧版 75.7k、Citus 105.4k、单机三副本 PG 72.0k；新版约为 Citus 的 73%。

2026-10-20：一主两备原生 PG（`pgsync3.sh`，ANY 1 + `synchronous_commit = on`）对照（`TSO_RAFT_REPORT_20261019.md` §6、§7.3）：
- 64 迷你仓：PG 76.3k，ShardPG 3 worker 76.5k，基本持平；
- 标准规模 30 仓（每轮 3 min × 2）：PG 28.2k（100 终端起锁死在约 1,000 tps，行锁要一直拿到刷盘和备库确认之后），ShardPG（4 台机器）45.8k，约 1.6 倍。主要原因是提前放锁（补丁 0014 + 0016）。ShardPG 两遍之间差 12–28%，要看检查点落在哪一轮。
- 100 标准仓、限内存在 lab 做不了：数据盘是机械盘，会测成磁盘的成绩。

## 5. 下一步（按预期收益）

1. **可见性轮询改事件唤醒**：`shard_xid_state_ex` 里"提交中"与 PREPARED 等决议的 `pg_usleep` 循环，改为判决公布时唤醒（条件变量 / 闩锁）。纯工程，风险低。
2. **TSO 往返**：节点内合批代理 10-20 已做（`pg_partdist.tso_proxy`，主要解决连接数，吞吐不变）；还可以把 commit_ts 取号移出持锁区（需论证提交记录写法）。
3. **待用户决定的语义开关**：同步复制下严格读"不读未被备库确认的提交"（比原生 PG/Citus 更强）。放宽到 PG 语义可显著缩小第 1 项的等待窗口。用户尚未表态——**先做 1、2，量化后再问**。
4. **2PC 路径**：PartWAL 全局排它锁内的标记写入（`dtx_participant.c` 的 AppendDtxRecord + PREPARE 标记、`dtx_replicate_verdict`、fin_marker）。路线 A 单成员组无读者，但单机崩溃恢复（`dtx_close_indoubt` 扫流）间接依赖，评估后加开关。
5. WAL 体积、每事务 CPU。
6. 标准规模 10-20 已测 30 仓（见第 4 节）。100 仓在 lab 的机械盘上受写盘限制；更大规模要有 SSD 才有意义。

## 6. 已知风险 / 未做

- **【10-20 已修】worker 切主后各 worker 上的 Citus 路由元数据不更新**：`citus_update_node` 只改协调者，把全部 worker 标成未同步后 Citus 后台逐个补同步互相等、永远补不上 → 从 worker 进来的查询路由到死掉的旧主、2PC 恢复问不到发起节点、prepared 悬挂锁行。修法：切主后立即 `start_metadata_sync_to_all_nodes()` + 控制面 leader 每 10 s 兜底；2PC 恢复按控制面现任地址问发起节点（`TSO_RAFT_REPORT_20261019.md` §5）。
- **【10-19 已修】切主后分片 xid 被重复发号**（水位推进记 WAL `XLOG_SHARD_XID_WM`，备库回放抬高水位文件；`TSO_RAFT_DESIGN.md` §3，lab 确定性复现与验证见 `TSO_RAFT_REPORT_20261019.md`）。原记录：分片 xid 的持久发号水位是普通文件、不进 WAL，备库上是旧值；物理复制提升后发号起点只从回放的提交 / 判决记录学到，旧主"写了数据没提交"用过的号会被重发，那些元组在新主上随新事务提交而部分"复活"（lab 实测：仓 54 订单 1409 与仓 14 的 Delivery 共用分片 xid 269327）。Raft 模式的 `ShardXidClaimOnPromote` 有跳批保护，路线 A 没走。修法：水位推进记 WAL（每批一次）由备库回放推进，或提升后逐分片跳批 + 判中止。详见 `TPCC_CLOUD_REPORT_20261018.md` 9.3。
- **【10-18 晚已修】跨节点原子性**：发起方崩溃可致一半提交（云上 12 次 kill 3 次）。已按方案 B 修（判决与发起方本地提交绑定，`DTX_2PC_DESIGN.md` §14），lab 验证通过。
- 快速模式下未决登记日志不 fsync；早于最近检查点的 PREPARED 记录不在回放区间 → 操作系统级崩溃时可能缺登记（只靠协调组问询收敛）。
- 旧主不会自动重建为备库（`shardpg_rep.sh rejoin nX` 是手动步骤）。
- 协调者备库（COORD_HA=1）会拖慢提交，与 Citus（协调者无备库）比性能时不要开。
- 客户端只连固定协调者地址，切协调者后需改连新地址（无 VIP / 多主机连接串）。`tso_raft` 下 worker 也能当入口（函数下推照常），但要客户端自己列多个地址。
- `tso_raft` 下协调者宕机的发号空窗 ≈ 控制面选举（lab `election_timeout_ms = 6000`）+ 1.25 × `pg_raft.tso_lease_ms` + 200 ms，实测约 8 s；Citus 元数据（DDL、加删节点、改路由）仍只在协调者上。

## 7. 常用操作（lab，145 上 `R=/data1/yyt/shardpg-fix-20261014`）

1. 本地打包：`tar czf fix-src.tar.gz pg-partdist-src pg-raft-src tpcc`，传到 145 的 `/tmp/fix-src.tar.gz`。
2. `bash restore_routea.sh`：三机重建 `shardpg:prof` 镜像 → 全新部署 → 装载 → 6 个同步备库。约 4.5 min。
3. 跑验证 / 性能脚本（第 3 节，或 `perf_round.sh`、`compare_profile.sh`）。

- 只做编译检查：把源码 tar 拷进 `shardpg-fix-client` 容器，`make PG_CONFIG=/work/pg-install/bin/pg_config`。
- 内核补丁列表在 lab 的 `build/prof/Dockerfile` 与仓库 `tpcc/lab/Dockerfile.prof`（0012–0016）。
- **lab 现状（10-20 晚）**：
  - 部署：4 台机器的 ShardPG 标准规模 30 仓（套件 `$R/tpcc-std`，`redeploy4m.sh` 加 `KIT=tpcc-std`），镜像 `shardpg:tsoraft`，带 TSO 代理；
  - PG 一主两备（`pgsync-*`）已停，数据还在，30 仓；
  - 迷你仓套件 `$R/tpcc` 的 `cluster.env` 是 4 台机器版本，3 worker 原件是 `cluster.env.4node`；
  - lab 数据盘都是机械盘，不要用限内存的办法模拟小服务器。
