# Demo 套件已知问题（待改）

> 登记日期：2026-09-17。来源：09-16 在 `pg-test-container`（1c+3w、`pg_raft.max_groups=64`）上整套复跑 2.0 时代 demo 10 项，
> 以及 09-17 核对 demo 脚本改动史。**本文件只登记、不修**，按用户裁定"demo 相关问题记下来后面再改"。
> 原版 demo（`pg-citus-tx2-container` / 分支 shardpg-2.0）从未改动过；以下说的都是 `shardpg-test` 线上的副本。

## 0. 现状一览

| # | 套件 | 09-16 复跑 | 问题归类 |
|---|---|---|---|
| 1 | `tests/verify_continuity_and_crash.sh` | 20✓ / 11✗ | 期望值过时（9 条）+ 重启后多一条记录未定因（2 条），见 §1 |
| 2 | `tests/test_shard_auto_init.sh` | 5/0 | 期望文件随 SQL 函数增加会过时，见 §4.3 |
| 3 | `tests/test_multi_table_isolation.sh` | 146/0 | 断言已被放宽，见 §2 |
| 4 | `tests/test_crash_recovery.sh` | PASS | 断言已被放宽，见 §2 |
| 5 | `tests/test_bulk_insert_recovery.sh` | 8/1 | 性能阈值（开销 26.5% vs <20%），见 §3 |
| 6 | `tests/test_segment_boundary_lsn.sh` | 6/0 | — |
| 7 | `tests/test_corrupt_segment_recovery.sh` | 44/0 | — |
| 8 | `tests/test_demux_backlog_recovery.sh` | 29/0 | 断言已被放宽，见 §2 |
| 9 | `tests/perf_latency.sh` | ✗ p99 489 ms / avg 23.7 ms | 性能阈值 + 测量环境不干净，见 §3 |
| 10 | `tests/test_enospc_recovery.sh` | 12/0 | — |

## 1. #1 `verify_continuity_and_crash.sh` 的 11 个红

**9 条是"1 行 = 1 条 parwal 记录"的 2.0 老假设。** 现在一行插入会产生堆记录 + 索引记录，事务尾部还有提交 MARKER，
索引/TOAST 与主堆共用一条捕获链是设计（FRD §5.2）。实测：

    1A-count        expected=3  got=10      1C-count (3+3)  expected=6   got=20
    2A-pre-crash    expected=5  got=16      2C-count (5+5)  expected=10  got=32

**另 2 条不是比例问题，要单独定因**：

    1B-count unchanged before new inserts     expected=10  got=11   （重启后、未插入前多了 1 条）
    2B-no duplicate records (redo idempotent)  expected=16  got=17   （崩溃恢复后多了 1 条）

候选解释是后台发射器在重启/恢复后落了一条非数据记录（D2 冻结账目同步、分叉自愈基线、SHARD MARKER 之类——
`reproduce-env.sh` V6 指纹竞态就是同一类现象），但**尚未取证**。若多出的是 DATA 记录，就是 redo 不幂等的真缺陷。
修的时候先把这条记录的 `rmid/info/flags` 打出来再下结论。

**改法建议**：计数期望改成"按记录类型分别计数"（DATA 行数、INDEX、MARKER 各自精确），不要只放宽成"≥1"（见 §2 的教训）。

## 2. 8 个 demo 脚本的断言在 09-10/11 被放宽过

提交 `a69d04b`、`9e6c81e`（T7.13"OPS 套件拓扑无关化"）改了 #2–#8、#10 共 8 个脚本（+606/−343 行）：

- 端口、节点数、数据目录改为从 `pg_dist_node` 动态读取 —— 合理，保留。
- **把"插 N 行 = N 条记录"的精确计数断言改成了"≥ 1 条"**（`check_true ... -ge 1`），理由同 §1。
  "≥1"只能证明"有记录"，证明不了"条数对、没重复、没丢"。#1 没被改，所以它还红着；其余 8 个的绿里有一部分是这么来的。

**改法建议**：与 §1 一起，按记录类型恢复精确期望；恢复后再复跑确认没有被放宽掩盖的问题。

## 3. 性能类断言（#5 第 4 项、#9）

| 项 | 阈值 | 09-16 实测 | 早先 |
|---|---|---|---|
| #5 批量 INSERT hook 开销 | < 20% | 26.5% | 9/0 时通过 |
| #9 端到端延迟 | p99 < 10 ms、avg < 5 ms | p99 489 ms、avg 23.7 ms | p99 749 ms |

两个问题叠在一起，要分开处理：

1. **测量环境不干净**：09-16 全程 `pg-citus-tx2-container`（9 节点，~1.3 GB 内存、~20% CPU）与 test 集群并跑，
   宿主机只有 2 vCPU / 3.8 GB。这期间的性能数字（含"约 23 条 raft 记录/秒"）都不能当基线。tx2 已于 09-17 停止，需重测。
2. **阈值是 2.0 单机口径**：#9 的 10 ms / 5 ms 定于没有 Raft 同步复制的时代，现在每条记录要同步等多数派，
   阈值需要按"有复制"的架构和目标硬件重新定。

## 4. 驱动脚本与执行方式

1. **宿主机侧 / 容器侧没写清**：`tests/*.sh` 大多用 `psql -h localhost`，必须在容器内跑，在宿主机跑会整片报
   "协调者 :5432 连不上"；`tests/perf_latency.sh` 与 `sim/run_*_sim.sh` 是宿主机侧（内部 `docker exec`）。
2. **默认容器名过时**：`perf_latency.sh`、`sim/run_noload_sim.sh` 默认 `CONTAINER=pg-partdist-raft4-container`
   （旧 raft4 环境，已停）。`d3ae9d3` 已改成可用环境变量覆盖，但默认值仍是旧名。
3. **`sim/run_noload_sim.sh` 的 `ensure_cluster` 写死 3 节点**：端口只看 `5432 5433 5434`，协调者数据目录写成
   `master`，而 `reproduce-env.sh` 建出来的是 `coordinator`、`worker1..N`。在 4 节点 / 9 节点环境上它的"自动拉起"是坏的。
   本次复跑没用它，是逐个套件单跑的。
4. **`perf_latency.sh` 每跑一次覆写 `docs/LATENCY_P99_REPORT.md`**，会在 git 里产生无意义改动；建议输出到 scratch 目录。
5. **期望文件会随 SQL 函数增加而过时**：`test_shard_auto_init.sh` 的 `08_schema_existence` 期望文件
   曾在 09-12 → 09-14 之间红着没人发现（新增 13 个函数未回填）。每次加 SQL 函数要同步重建期望文件。
6. **容器内副本滞后**：`pg-test-container` 里的 `perf_latency.sh`、`run_noload_sim.sh` 来自 `b1ecaa9`，比工作区旧；
   以后在容器内跑 demo 前先同步。
