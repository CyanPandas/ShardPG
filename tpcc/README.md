# ShardPG TPC-C 套件（`shardpg-tpcc` 分支）

目标实验：**三台服务器、10 仓库、16 终端、6 小时**。本目录是这个实验的完整工具链；
本分支对产品代码做的配套改动见文末「本分支的产品改动」。

```
tpcc/
├── cluster.env.example     拓扑与规模参数（复制成 cluster.env 再改）
├── conf/node.conf.tmpl     每个节点 postgresql.conf 的追加段模板
├── sql/
│   ├── 01_schema.sql       模式：主键 + 二级索引一次建全，无外键
│   ├── 02_gen_funcs.sql    随机数 / NURand / 字符串生成
│   ├── 03_distribute.sql   item 转引用表 → 共置分布 → 建 Raft 组 + 供副本 → 打标
│   ├── 04_load_funcs.sql   按块装载的函数
│   ├── 05_txn_funcs.sql    五类事务（plpgsql，协调者上执行）
│   └── 90_consistency.sql  TPC-C §3.3.2 一致性条件
├── pgbench/*.sql           五类事务的 pgbench 脚本
└── scripts/
    ├── gen_node_conf.sh    生成各节点配置
    ├── wire_cluster.sh     装扩展 + node_map + Citus 接线 + 核对 TSO
    ├── build_db.sh         建库（调用 load.sh）+ 行数与一致性核对
    ├── run.sh              预热 + 正式压测 + 计算 tpmC + 收尾一致性检查
    └── monitor.sh          长跑监控（run.sh 自动起）
```

## 1. 拓扑

| 服务器 | 进程 | 端口 | node_id |
|---|---|---|---|
| A | coordinator（兼 TSO master） | 5432 | 1 |
| A | worker | 5433 | 2 |
| B | worker | 5433 | 3 |
| C | worker | 5433 | 4 |

- 每张表 6 个分片（`SHARD_COUNT=6`，每台 worker 2 个主分片），每个分片一个 Raft 组：
  单副本（`REPLICAS=0`）= 只有主；三副本（`REPLICAS=2`）= 1 主 + 2 副本，三台各一份。
- 压测客户端（pgbench）最好放第四台机器；放 A 上会和 coordinator 抢 CPU。

### 单副本模式（`REPLICAS=0`，`cluster.env.example` 的默认值）

每个分片仍建一个 Raft 组，但组里只有 placement 一个成员（多数派 = 1），不供副本。

- **组不能省**：跨仓库事务（约 10% 的 NewOrder、15% 的 Payment）走 Citus 2PC；打标分片在
  `COMMIT PREPARED` 时不写终局判决（补丁 0009），判决与 commit_ts 靠协调者分片组的决议广播
  落到参与方。完全不建组时，跨分片事务提交后参与方的行会一直停在 PREPARED、对读者不可见。
- **与 3 副本的差异**：没有副本回放、没有物理基线、没有分叉修复；监控里 `ring_overwrites`、
  `diverged_pending` 失去意义。任何一台 worker 宕机，它上面的分片直接不可用（没有可升主的
  副本）——6 h 长跑期间不要做故障注入。
- 写路径少了副本复制的等待，吞吐会高于 3 副本；两者的数字不能直接对比。
- 主机名用 IP。**不要**把节点叫 `coordinator` / `master` / `worker1..3`：pg_raft 原本
  会把这几个名字回环映射到 127.0.0.1（单机测试拓扑用的）。本分支加了
  `pg_raft.loopback_node_names`，模板里已设为 `off`。

## 2. 部署

每台服务器装同一份构建（内核 = PG 16.14 + `pg-partdist-src/patches/` 下 0001–0011
全部补丁，扩展 = `pg-partdist-src` + `pg-raft-src` + Citus）。

```bash
cp cluster.env.example cluster.env      # 改 IP / 端口 / 目录
bash scripts/gen_node_conf.sh           # 生成 conf/out/node<id>.conf

# 每个节点（在它所在的服务器上）：
initdb -D $PGDATA -U postgres --no-locale
cat conf/out/node<id>.conf >> $PGDATA/postgresql.conf
echo "host all postgres 10.0.0.0/24 trust" >> $PGDATA/pg_hba.conf   # 集群内 + 客户端
pg_ctl -D $PGDATA -l $PGDATA/server.log start

# 在客户端上：
bash scripts/wire_cluster.sh            # 装扩展、接线、等 group0 leader、核对 TSO
bash scripts/build_db.sh                # 建库 + 装载 + 核对（数十分钟）
bash scripts/run.sh 6h-10w-16t          # 正式跑（预热 15 min + 6 h）
```

### 必须知道的几条

1. **TSO 必须在灌任何数据之前生效。** 模板里 coordinator 设了 `tso_master = on`，所有
   节点的 `tso_conninfo` 都指向它。先写数据后配 TSO，已提交数据的 commit_ts 处在另一个
   时间戳体系里，分片 clog 将永远截断不了（P7-G4）。`wire_cluster.sh` 会核对每个节点都能
   取到号、GlobalSafeTs 非 0。
2. **coordinator 重启 = 全集群停发号。** TSO 首次服务后会留 `$PGDATA/pg_tso_boot`，重启
   后检测到它就拒绝发号（防止时间戳倒流）。测试集群里的处置是删掉这个文件再起；生产语义是
   整簇重建。**6 小时之内不要重启 coordinator。**
3. **建库顺序不能换**：索引在建表时建（打标后禁 CREATE INDEX）；先打标后写数据（打标前
   写入的行在分片可见性规则下不可见）；item 先本地灌好再转引用表（引用表运行期禁写）。
   `build_db.sh` 按这个顺序做，并在每一步失败时停下。
4. **只跑 ANALYZE，不要跑 VACUUM**（分片表的 VACUUM / 整库 VACUUM 一律被拒）。空间回收
   由分片 vacuum 自动启动器负责（见 §4）。
5. TPC-C 必须建在 `postgres` 库、以 `postgres` 用户连接：节点间连接串写死了这两项。
6. `pg_raft.max_groups` 是启动参数：8 张表 × 6 分片 = 48 个数据组，模板给的 64 留了余量。
   改 `SHARD_COUNT` 时同步调它。

## 3. 事务实现与 ShardPG 的语义差异

- **隔离级别是快照隔离（SI）**，不管会话设的是什么级别（SERIALIZABLE 被拒）。写写冲突
  按 first-committer-wins 处理：后到的事务先等前一个结束，前一个提交后报
  `40001 could not serialize access`。**没有 READ COMMITTED 式的重读**。
- 因此重试是常态：`run.sh` 用 `pgbench --max-tries=$MAX_TRIES` 自动重试 40001 / 40P01，
  `summary.txt` 里给出各类事务的重试次数，报告里要单独列出。10 仓 16 终端时
  `warehouse.w_ytd`（Payment 每笔都改）是全局热点，重试率会显著。
- 行锁（`SELECT ... FOR UPDATE / FOR SHARE`、`NOWAIT`、`SKIP LOCKED`）由本分支的补丁 0011
  提供。分片表上**只存单个持锁者**：兼容的锁模式（两个 FOR SHARE、KEY SHARE 对
  NO KEY UPDATE）也会排队，不会合并成 multixact。并发度比原生 PG 略低，语义更严。
- **每个事务都要先加入 ShardPG 全局事务**（`tpcc.begin_global()`，五类事务函数和装载函数的
  第一句）：取 gxid + TSO start_ts，`SET LOCAL pg_partdist.join_info` 并让 Citus 把它传播到
  每条分片连接。不加入的话，跨分片写在 PREPARE 时被拒（"未加入全局事务的分片写不允许
  PREPARE TRANSACTION"）；只读事务加入后各 worker 共用同一个快照。必须是 `SET LOCAL` 语句
  （Citus 拦截语句来传播，`set_config()` 它看不到），且要在第一次访问分布表之前。
  自己写别的负载时同样要遵守。
- 访问分布表的函数一律写 plpgsql：Citus 不支持 `LANGUAGE sql` 函数里带参数访问分布表。
- 事务函数里**不能有 EXCEPTION 块**（plpgsql 的异常块是子事务，分片表禁子事务写）。所以
  NewOrder 的 1% 无效商品回滚改成"先查完全部商品、有无效的就零写入返回"，净效果与回滚
  相同。
- 没有键入时间 / 思考时间（吞吐模式）。结果是"该配置下的吞吐"，不是可审计的 TPC-C 成绩。
- 终端按 `client_id % 仓库数 + 1` 绑定主仓库：16 终端 / 10 仓时，1–6 号仓各有 2 个终端。

## 4. 6 小时长跑的风险与监控

| 风险 | 现象 | 看哪里 |
|---|---|---|
| 表膨胀 | 分片表没有页内剪枝；回收只靠分片 vacuum | `tables.tsv` / `shards.tsv` 的大小曲线 |
| 回收推不动 | 热点页拿不到 cleanup lock ⇒ 本趟不干净 ⇒ clog 截断点不前进 ⇒ 龄只涨不降 | `shards.tsv` 的 `xid_age`；节点日志里的 `分片 vacuum 自动启动器` 行 |
| 捕获环溢出 / 副本分叉 | `ring_overwrites` > 0、`diverged_pending` = t、`divergence` 非空 | `nodes.tsv`、`shards.tsv`；心跳会自动重做基线 |
| 2PC 悬挂 | `dtx_pending` 持续不降 | `nodes.tsv` |
| 不可重试错误 | 终端中途中止，吞吐断崖 | `pgbench.err` 里的 `client N aborted`；`summary.txt` 的中止终端数 |
| 连接打满 | worker 取 commit_ts 失败（`TSO 不可达或拒绝服务`） | `nodes.tsv` 的 `connections`；调 `max_connections` |

空间回收的配置（模板已设）：

```
pg_partdist.shard_vacuum_trigger_age = 20000         # 分片 xid 龄满 2 万就清一次
pg_partdist.shard_vacuum_trigger_interval_s = 300    # 同一分片两次之间至少 5 分钟
```

`shard_vacuum_max_age`（回卷护栏，默认 2 亿）保持不动——它同时管 WARNING，调低会刷日志。

**正式跑 6 小时之前，先跑一次 30 分钟**（`DURATION_S=1800 RAMPUP_S=300`），确认：
`summary.txt` 中止终端数 = 0、一致性违反 = 0、`shards.tsv` 的 `xid_age` 会周期性回落、
`ring_overwrites` 不增长。再用 30 分钟的表大小增速外推 6 小时的磁盘占用。

## 5. 结果

`results/<时间>_<标签>/`：

- `summary.txt`：tpmC（测量窗口两端 `sum(d_next_o_id)` 之差 ÷ 分钟数，恰好是提交成功的
  NewOrder 数）、pgbench 各脚本的事务数 / 延迟 / 重试 / 失败、中止终端数、一致性违反数；
- `pgbench_agg.*`：每分钟一行的吞吐与延迟（画随时间变化的曲线用）；
- `progress.tsv` / `nodes.tsv` / `shards.tsv` / `tables.tsv`：监控；
- `consistency_before.txt` / `consistency_after.txt`；
- `settings_node*.tsv`：本次运行时各节点的实际参数。

报告建议包含：tpmC、各类事务 p90/p99 延迟与重试率、tpmC 与表大小随时间的曲线、
一致性检查结果、监控里出现过的异常（分叉、环溢出、终端中止）及其时间点。

## 本分支的产品改动

| 改动 | 位置 | 作用 |
|---|---|---|
| 补丁 0011：分片表行锁 | `pg-partdist-src/patches/0011-shard-row-locks.patch`（`heapam.c` / `heapam_handler.c`） | `heap_lock_tuple` 对分片表走单持锁者分支（lock-only xmax = 分片 xid，WAL / redo 原样）；`heapam_tuple_lock` 对分片表拒绝追链重读、报 40001 |
| lock-only xmax 的可见性 | `src/shard_visibility.c` | 读侧把加锁当作没有 xmax；更新判定：外人持锁在跑 ⇒ 等待，自己持锁 / 持锁者已结束 ⇒ TM_Ok，锁不参与 first-committer-wins |
| 分片 vacuum 的 LP_REDIRECT | `src/shard_vacuum.c` | 带索引的表按 HOT 链取死前缀：后面还有活成员 ⇒ 根做成重定向，整链死 ⇒ 根 LP_DEAD 进索引两阶段。取代原来"带 HOT 链的死根 ⇒ ERROR" |
| 按回收触发自动 vacuum | `src/shard_xid.c`、`src/shard_vacuum.c` | `shard_vacuum_trigger_age` / `shard_vacuum_trigger_interval_s`，默认 0 = 关 |
| 多机部署 | `pg-raft-src/src/pg_raft.c`、`raft_consensus.c` | `pg_raft.loopback_node_names`（默认 on）、`pg_raft.peer_connect_timeout_s`（默认 1） |
| 心跳自连异步化 | `src/shard_vacuum.c`、`src/tso_client.c` | vacuum 自动启动器 / 分叉自动修复不再阻塞 TSO 续租（原先一趟大表 sweep 就能让本节点全部分片快照被栅栏作废） |
| 等待路径的瞬时窗 | `src/shard_visibility.c` | `sv_xmax_wait` 无条件等持有者的 xid 锁，不再误报"无主 RUNNING" |
| **SI 提交中窗口（既有缺陷）** | `src/shard_visibility.c`、`src/shard_xid.c`、`src/pg_partdist.c` | 持有者已取 commit_ts、判决未落时读者不再误判 RUNNING（会导致同一行两版同时可见或都不可见）；语句开始前提前取 start_ts |

所有新 GUC 的默认值都等于改动前的行为。后三项是沙箱压测暴露的既有缺陷修复，详见
`TX_TSO_MVCC_DEV_PLAN.md` §6 的 C5–C7 与验证记录。
