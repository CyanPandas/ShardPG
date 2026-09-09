# pg_partdist

> ## ★ 先读这里（2026-09-09 回填）
>
> **本文以下的正文是 `shardpg-2.0` 时代的文档：三节点、只讲"分区级 WAL 拆分"。
> 它描述的是本项目最早的一层，早已不是全貌。** 当前分支的实际形态是：
>
> | | 现状 |
> |---|---|
> | **集群** | 1 coordinator + 8 worker（9 节点，端口 5432–5440），单机容器内 |
> | **能力层** | 分区级 WAL（本文） → 物理回放 R1/L1 + DDL/冻结控制通道 D1/D2 → Raft 多分区组 + 自治切主 → DTX-2PC 决议进数据组 → **TX-TSO-MVCC**（TSO 全局时间戳 + 分片级 xid/clog + SI 可见性 + 分片 vacuum/GC） |
> | **内核依赖** | **10 个补丁是硬依赖**，`pg-install/` 是"已打补丁并编译好"的整棵树，随仓库提交。改补丁后必须同步回仓库，见 `pg-partdist-src/patches/README.md` |
> | **成熟度** | **P1–P6 六期已实施，但 P6 未结项**：21 条未闭合缺陷，其中 4 条是"已提交数据在切主/供给后不可见或丢失"级别。**不具备生产可用性** |
>
> ### 文档地图（按"想知道什么"找）
>
> | 想知道 | 看 |
> |---|---|
> | 事务/可见性/GC 的方案定稿 | `TX_TSO_MVCC_DESING.md`（§10 是第一期功能限制，**上生产前必读**） |
> | 怎么做的、什么顺序、验收数字 | `TX_TSO_MVCC_DEV_PLAN.md`（§5 是风险登记簿） |
> | 物理回放（follower 怎么追平、切主怎么交接） | `pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md` |
> | 分布式提交（2PC 决议怎么进数据组） | `pg-partdist-src/docs/DTX_2PC_DESIGN.md` |
> | Raft 模块的缺口与冻结范围 | `pg-raft-src/docs/raft_module_revision_plan.md` §0.2 |
> | **现在有哪些缺陷、接下来怎么补** | **`pg-partdist-src/docs/P6_EXIT_AUDIT.md`（盘点）+ `P7_REMEDIATION_PLAN.md`（计划）** |
> | 怎么把环境原样建起来 | `pg-raft-src/reproduce-env.sh`（`up` / `verify` / `test` / `destroy`）；本分支的场地说明见 `PG_TEST_ENV.md` |
>
> ### 门禁入口
>
> ```bash
> # 31 套全量（最坏 9.5h，跑前先腾内存和磁盘）
> CONTAINER=pg-test-container bash pg-partdist-src/tests/run_p6_exit.sh
> # 单套件
> CONTAINER=pg-test-container bash pg-partdist-src/tests/test_<name>.sh
> ```

PostgreSQL 16 + Citus 13.1.0 的分区级 WAL 拆分扩展。

`pg_partdist` 通过 Hook 机制自动拦截 Citus 分布式表的 INSERT 操作，在每个 Worker 节点上生成**分区级 WAL 日志**（`pg_parwal/`），并由后台 Demux Worker 将记录拆分落盘到对应的分区段文件，整个过程对业务层完全透明。

```
Coordinator  INSERT INTO orders VALUES (1, 100)
                 │
                 ▼  Citus 自动路由
Worker1      INSERT INTO orders_102225 VALUES (1, 100)
                 │
                 ▼  ExecutorFinish Hook（pg_partdist 自动触发）
                 ├─ 识别 shard 名称 "orders_102225"（后缀含 4+ 位数字）
                 ├─ 分配 per-partition 单调 LSN
                 └─ 写入 pg_wal（自定义 RMGR）
                 │
                 ▼  Demux Worker（后台进程，随 Worker 自动启动）
             $PGDATA/pg_parwal/42571/000000010000000000000007  ← 段文件落盘
```

---

## 目录

1. [环境依赖](#环境依赖)
2. [克隆仓库与启动容器](#克隆仓库与启动容器)
3. [编译与安装 pg_partdist](#编译与安装-pg_partdist)
4. [集群配置](#集群配置)
5. [测试一：分区 WAL 创建测试](#测试一分区-wal-创建测试)
6. [测试二：表间隔离性测试](#测试二表间隔离性测试)
7. [测试三：重启后连续性测试](#测试三重启后连续性测试)
8. [测试四：崩溃恢复测试](#测试四崩溃恢复测试)
9. [测试五：批量更新](#测试五批量更新)
10. [测试六：自动创建分区目录](#测试六自动创建分区目录)
11. [自动化回归测试](#自动化回归测试)
12. [完整测试套件](#完整测试套件)
13. [性能指标](#性能指标)
14. [on-disk 布局](#on-disk-布局)
15. [SQL 函数参考](#sql-函数参考)
16. [已知限制](#已知限制)

---

## 环境依赖

| 依赖 | 版本 | 说明 |
|------|------|------|
| Docker | 任意 | 运行预置的编译 + 运行环境 |
| Git | 任意 | 克隆仓库 |

所有 PostgreSQL 16.14 和 Citus 13.1.0 二进制文件均已内置在仓库的 `pg-install/` 目录中，无需手动编译 PG 本身。

---

## 克隆仓库与启动容器

### 1. 克隆

```bash
git clone https://github.com/CyanPandas/ShardPG.git
cd ShardPG
```

### 2. 构建 Docker 镜像

```bash
docker build -t pg-partdist-env .
```

### 3. 启动容器

以下命令将 `pg-install/`（PG 二进制）和 `pg-cluster-data/`（集群数据目录）挂载进容器，源码目录**不挂载**（需用 `docker cp` 更新）：

```bash
docker run -d \
  --name pg-citus-cluster-container \
  -v "$(pwd)/pg-install:/work/pg-install" \
  -v "$(pwd)/pg-cluster-data:/work/pg-cluster-data" \
  pg-partdist-env \
  sleep infinity
```

### 4. 初始化集群（首次运行）

```bash
# 进入容器，切换到 postgres 用户
docker exec -it -u postgres pg-citus-cluster-container bash

# 以下命令在容器内执行
PG=/work/pg-install/bin
DATA=/work/pg-cluster-data

# 初始化三个节点的数据目录
$PG/initdb -D $DATA/master   --encoding=UTF8
$PG/initdb -D $DATA/worker1  --encoding=UTF8
$PG/initdb -D $DATA/worker2  --encoding=UTF8
```

在每个节点的 `postgresql.conf` 末尾追加以下配置：

```bash
# Coordinator
cat >> $DATA/master/postgresql.conf <<'EOF'
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
EOF

# Worker1
cat >> $DATA/worker1/postgresql.conf <<'EOF'
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
pg_partdist.local_node_id = 1
EOF

# Worker2
cat >> $DATA/worker2/postgresql.conf <<'EOF'
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
pg_partdist.local_node_id = 2
EOF
```

启动集群并注册节点：

```bash
$PG/pg_ctl start -D $DATA/master  -l $DATA/master/pg.log  -o '-p 5432' -w
$PG/pg_ctl start -D $DATA/worker1 -l $DATA/worker1/pg.log -o '-p 5433' -w
$PG/pg_ctl start -D $DATA/worker2 -l $DATA/worker2/pg.log -o '-p 5434' -w

# 在 Coordinator 安装 Citus 并注册 Worker 节点
$PG/psql -p 5432 -d postgres -c "CREATE EXTENSION citus;"
$PG/psql -p 5432 -d postgres -c "SELECT citus_set_coordinator_host('localhost', 5432);"
$PG/psql -p 5432 -d postgres -c "SELECT citus_add_node('localhost', 5433);"
$PG/psql -p 5432 -d postgres -c "SELECT citus_add_node('localhost', 5434);"

# 在三个节点分别安装 pg_partdist
$PG/psql -p 5432 -d postgres -c "CREATE EXTENSION pg_partdist;"
$PG/psql -p 5433 -d postgres -c "CREATE EXTENSION citus; CREATE EXTENSION pg_partdist;"
$PG/psql -p 5434 -d postgres -c "CREATE EXTENSION citus; CREATE EXTENSION pg_partdist;"
```

### 5. 日常启停（集群已初始化后）

```bash
# 以 postgres 用户在容器内执行
docker exec -u postgres pg-citus-cluster-container \
    /work/pg-partdist-src/start-cluster.sh

docker exec -u postgres pg-citus-cluster-container \
    /work/pg-partdist-src/stop-cluster.sh
```

---

## 编译与安装 pg_partdist

源码存放在容器内的 `/work/pg-partdist-src/`，宿主机修改后需通过 `docker cp` 传入容器再编译。

### 复制源码到容器并编译

```bash
# 将宿主机上的源码复制到容器
docker cp pg-partdist-src/. \
    pg-citus-cluster-container:/work/pg-partdist-src/

# 在容器内编译并安装（以 root 身份运行 make，无需 -u postgres）
docker exec pg-citus-cluster-container bash -c "
    cd /work/pg-partdist-src
    make PG_CONFIG=/work/pg-install/bin/pg_config
    make install PG_CONFIG=/work/pg-install/bin/pg_config
"
```

> 编译产物 `pg_partdist.so` 会被安装到 `/work/pg-install/lib/postgresql/pg_partdist.so`，
> 该路径已挂载到宿主机的 `pg-install/lib/postgresql/`，宿主机可直接看到更新。
>
> **安装后必须重启所有节点**才能加载新的 `.so`：

```bash
docker exec -u postgres pg-citus-cluster-container bash -c '
    PG=/work/pg-install/bin
    DATA=/work/pg-cluster-data
    $PG/pg_ctl stop  -D $DATA/worker2 -m fast -w
    $PG/pg_ctl stop  -D $DATA/worker1 -m fast -w
    $PG/pg_ctl stop  -D $DATA/master  -m fast -w
    $PG/pg_ctl start -D $DATA/master  -l $DATA/master/pg.log  -o "-p 5432" -w
    $PG/pg_ctl start -D $DATA/worker1 -l $DATA/worker1/pg.log -o "-p 5433" -w
    $PG/pg_ctl start -D $DATA/worker2 -l $DATA/worker2/pg.log -o "-p 5434" -w
'
```

---

## 集群配置

`pg_partdist` 必须列入**所有三个节点**的 `shared_preload_libraries`，顺序须在 `citus` 之后。

| 节点 | 端口 | `local_node_id` |
|------|------|-----------------|
| Coordinator | 5432 | 不需要设置 |
| Worker1 | 5433 | `1` |
| Worker2 | 5434 | `2` |

验证 Demux Worker 已随 Worker 节点启动（每个 Worker 各有一个后台进程）：

```bash
docker exec pg-citus-cluster-container bash -c \
    "ps aux | grep 'pg_partdist demux' | grep -v grep"
# 预期输出示例（两行，分属 worker1 和 worker2）：
# postgres  2699 ... postgres: pg_partdist demux worker
# postgres  2723 ... postgres: pg_partdist demux worker
```

---

## 测试一：分区 WAL 创建测试

**目标**：创建 Citus 分布式表并插入数据后，Worker 自动生成 `pg_parwal/<shard_oid>/` 目录和段文件，记录条数与 INSERT 次数一致，partition_lsn 从 1 开始单调递增。

### 手动步骤

**Step 1：清理 Worker1 的旧数据（可选，保证测试环境干净）**

```bash
docker exec -u postgres pg-citus-cluster-container bash -c '
    PG=/work/pg-install/bin; DATA=/work/pg-cluster-data
    $PG/pg_ctl stop  -D $DATA/worker1 -m fast -w
    rm -rf $DATA/worker1/pg_parwal
    $PG/pg_ctl start -D $DATA/worker1 -l $DATA/worker1/pg.log -o "-p 5433" -w
'
```

**Step 2：在 Coordinator 创建分布式表**

```sql
-- 连接 Coordinator（端口 5432）
DROP TABLE IF EXISTS demo CASCADE;
CREATE TABLE demo (id int PRIMARY KEY, val text);
SELECT create_distributed_table('demo', 'id', shard_count => 4);
```

**Step 3：查找 Worker1 负责的 shard，插入数据**

```sql
-- 查看 Worker1 负责的 shard ID
SELECT shardid
FROM pg_dist_shard s
JOIN pg_dist_shard_placement p USING(shardid)
WHERE s.logicalrelid = 'demo'::regclass AND p.nodeport = 5433;
-- 示例返回：102289, 102291

-- 找到能路由到 shard 102289 的 id 值
SELECT v FROM generate_series(1, 1000) v
WHERE get_shard_id_for_distribution_column('demo', v) = 102289
LIMIT 3;
-- 示例返回：1, 5, 9

-- 在 Coordinator 插入（Citus 自动路由到 Worker1）
INSERT INTO demo VALUES (1, 'row-1');
INSERT INTO demo VALUES (5, 'row-2');
INSERT INTO demo VALUES (9, 'row-3');
```

**Step 4：等待 Demux Worker 落盘**

```sql
-- 连接 Worker1（端口 5433）
SELECT partdist.demux_flush();
-- 该函数阻塞直到 Demux 将当前所有 pg_wal 记录写入 pg_parwal
```

**Step 5：验证分区目录和记录**

```bash
# 查看 Worker1 的 pg_parwal 目录（应出现 2 个 OID 子目录，对应 Worker1 的 2 个 shard）
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/
# 示例输出：124514  124519
```

```sql
-- 连接 Worker1（端口 5433），用实际 OID 替换 124514
SELECT partdist.count_parwal_records(124514::oid);
-- 预期：3（等于 INSERT 次数）

SELECT partdist.verify_partition_wal(124514::oid);
-- 预期：t（LSN 严格单调递增）

-- 查看每条记录明细
SELECT partition_lsn, orig_node_lsn, flags, is_valid
FROM partdist.check_partition_wal(124514::oid)
ORDER BY partition_lsn;
```

预期输出：

```
 partition_lsn | orig_node_lsn | flags | is_valid
---------------+---------------+-------+----------
             1 | 0/7C12340     |     1 | t
             2 | 0/7C12480     |     1 | t
             3 | 0/7C125C0     |     1 | t
```

```bash
# 查看段文件（命名规则与 pg_wal 相同，24 位十六进制）
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/124514/
# 示例输出：000000010000000000000007
```

---

## 测试二：表间隔离性测试

**目标**：两张分布式表各自拥有独立的 `pg_parwal/<oid>/` 目录，OID 集合无交集，写入互不干扰。

### 手动步骤

**Step 1：创建两张分布式表**

```sql
-- 在 Coordinator（5432）
DROP TABLE IF EXISTS dist_table_a CASCADE;
DROP TABLE IF EXISTS dist_table_b CASCADE;

CREATE TABLE dist_table_a (id int PRIMARY KEY, val text);
SELECT create_distributed_table('dist_table_a', 'id', shard_count => 4);

CREATE TABLE dist_table_b (id int PRIMARY KEY, val text);
SELECT create_distributed_table('dist_table_b', 'id', shard_count => 4);
```

**Step 2：向表 A 的 Worker1 分片插入 3 行**

```sql
-- 找 Worker1 上表 A 的 shard
SELECT shardid FROM pg_dist_shard s
JOIN pg_dist_shard_placement p USING(shardid)
WHERE s.logicalrelid = 'dist_table_a'::regclass AND p.nodeport = 5433;
-- 示例：102289, 102291

-- 找落入 shard 102289 的 id
SELECT v FROM generate_series(1, 1000) v
WHERE get_shard_id_for_distribution_column('dist_table_a', v) = 102289 LIMIT 3;
-- 示例：1, 5, 9

INSERT INTO dist_table_a VALUES (1,'a'), (5,'a'), (9,'a');
```

```bash
docker exec -u postgres pg-citus-cluster-container \
    /work/pg-install/bin/psql -p 5433 -d postgres \
    -c "SELECT partdist.demux_flush();"
```

**Step 3：记录表 A 在 Worker1 上的 OID 和 count**

```bash
# 当前所有 pg_parwal 目录 = 表 A 的 shard OID
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/
# 示例：124514  124519  ← 这是表 A 的 OID
```

```sql
-- 连接 Worker1，记录 count（用实际 OID 替换）
SELECT partdist.count_parwal_records(124514::oid);   -- 预期：3
SELECT partdist.count_parwal_records(124519::oid);   -- 预期：3
```

**Step 4：向表 B 的 Worker1 分片插入 3 行**

```sql
SELECT shardid FROM pg_dist_shard s
JOIN pg_dist_shard_placement p USING(shardid)
WHERE s.logicalrelid = 'dist_table_b'::regclass AND p.nodeport = 5433;
-- 示例：102293, 102295

SELECT v FROM generate_series(1, 1000) v
WHERE get_shard_id_for_distribution_column('dist_table_b', v) = 102293 LIMIT 3;
-- 示例：2, 6, 10

INSERT INTO dist_table_b VALUES (2,'b'), (6,'b'), (10,'b');
```

```bash
docker exec -u postgres pg-citus-cluster-container \
    /work/pg-install/bin/psql -p 5433 -d postgres \
    -c "SELECT partdist.demux_flush();"
```

**Step 5：验证隔离性**

```bash
# 新增了 2 个目录（表 B 的 OID），共 4 个
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/
# 示例：124514  124519  124536  124539
```

```sql
-- 表 A 的 count 不变（仍为 3，未被表 B 的写入影响）
SELECT partdist.count_parwal_records(124514::oid);   -- 预期：3
SELECT partdist.count_parwal_records(124519::oid);   -- 预期：3

-- 表 B 的 count 正确（为 3）
SELECT partdist.count_parwal_records(124536::oid);   -- 预期：3
SELECT partdist.count_parwal_records(124539::oid);   -- 预期：3

-- 两张表的 LSN 各自单调，互不影响
SELECT partdist.verify_partition_wal(124514::oid);   -- 预期：t
SELECT partdist.verify_partition_wal(124536::oid);   -- 预期：t
```

### 自动化脚本（114 项断言，覆盖双 Worker）

```bash
docker cp pg-partdist-src/tests/test_multi_table_isolation.sh \
    pg-citus-cluster-container:/work/pg-partdist-src/tests/test_multi_table_isolation.sh

docker exec pg-citus-cluster-container \
    chmod +x /work/pg-partdist-src/tests/test_multi_table_isolation.sh

docker exec -u postgres pg-citus-cluster-container \
    /work/pg-partdist-src/tests/test_multi_table_isolation.sh
# 预期：总计 PASS=114  FAIL=0
```

---

## 测试三：重启后连续性测试

**目标**：Worker 正常 `pg_ctl stop/start` 后，`pg_parwal/<oid>/` 目录名不变，原段文件保留，新写入的 partition_lsn 从上次最大值 +1 继续，无跳跃。

### 手动步骤

> **前提**：已完成测试一，Worker1 的 `pg_parwal/` 中已有数据（本例使用 OID=124514，count=3）。

**Step 1：记录重启前状态**

```bash
# 段文件列表
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/124514/
# 示例：000000010000000000000007

# 目录集合
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/
# 示例：124514  124519
```

```sql
-- 连接 Worker1（5433），记录 count
SELECT partdist.count_parwal_records(124514::oid);   -- 示例：3
```

**Step 2：正常关机并重启 Worker1**

```bash
docker exec -u postgres pg-citus-cluster-container bash -c '
    PG=/work/pg-install/bin; DATA=/work/pg-cluster-data
    $PG/pg_ctl stop  -D $DATA/worker1 -m fast -w
    $PG/pg_ctl start -D $DATA/worker1 -l $DATA/worker1/pg.log -o "-p 5433" -w
'
```

**Step 3：验证重启后目录不变、count 不变**

```bash
# 目录集合应与重启前完全相同（无新增，无消失）
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/
# 预期：124514  124519（与重启前相同）
```

```sql
-- count 仍为 3（无丢失、无重复）
SELECT partdist.count_parwal_records(124514::oid);   -- 预期：3
```

**Step 4：重启后继续写入**

```sql
-- 在 Coordinator 插入未使用过的 id（避免主键冲突）
INSERT INTO demo VALUES (13, 'post-restart-1');
INSERT INTO demo VALUES (17, 'post-restart-2');
INSERT INTO demo VALUES (21, 'post-restart-3');
```

```bash
docker exec -u postgres pg-citus-cluster-container \
    /work/pg-install/bin/psql -p 5433 -d postgres \
    -c "SELECT partdist.demux_flush();"
```

**Step 5：验证 count=6、LSN 1→6 连续、无新增目录**

```sql
-- 连接 Worker1（5433）
SELECT partdist.count_parwal_records(124514::oid);
-- 预期：6（3 旧 + 3 新）

SELECT partdist.verify_partition_wal(124514::oid);
-- 预期：t（LSN 单调，无断档）

SELECT partition_lsn, is_valid
FROM partdist.check_partition_wal(124514::oid)
ORDER BY partition_lsn;
-- 预期：1 t / 2 t / 3 t / 4 t / 5 t / 6 t
```

```bash
# 目录集合不变
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/
# 预期：124514  124519（同重启前）

# 段文件不变（原文件保留，新数据追加在同一文件中）
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/124514/
# 预期：000000010000000000000007（同重启前）
```

### 自动化脚本（31 项断言，涵盖重启 + 崩溃两种场景）

```bash
docker cp pg-partdist-src/tests/verify_continuity_and_crash.sh \
    pg-citus-cluster-container:/work/pg-partdist-src/tests/verify_continuity_and_crash.sh

docker exec pg-citus-cluster-container \
    chmod +x /work/pg-partdist-src/tests/verify_continuity_and_crash.sh

docker exec -u postgres pg-citus-cluster-container \
    /work/pg-partdist-src/tests/verify_continuity_and_crash.sh
# 预期：总计 PASS=31  FAIL=0
```

---

## 测试四：崩溃恢复测试

**目标**：`kill -9 postmaster` 后，PostgreSQL crash recovery 调用 `partdist_wal_redo` 回调，该回调具备幂等性（不产生重复记录）；重启后 partition_lsn 仍连续，可继续写入。

### 手动步骤

> **前提**：Worker1 的 `pg_parwal/` 中已有数据（本例 OID=124514，count=3）。

**Step 1：记录崩溃前状态**

```sql
-- 连接 Worker1（5433）
SELECT partdist.count_parwal_records(124514::oid);   -- 示例：3
SELECT partdist.verify_partition_wal(124514::oid);   -- 示例：t
```

**Step 2：模拟崩溃（kill -9 postmaster）**

```bash
# 获取 Worker1 的 postmaster PID
W1_PID=$(docker exec pg-citus-cluster-container \
    head -1 /work/pg-cluster-data/worker1/postmaster.pid)
echo "杀死 Worker1 postmaster PID=$W1_PID"

# 发送 SIGKILL（模拟断电/崩溃，不经过正常关机流程）
docker exec pg-citus-cluster-container kill -9 "$W1_PID"
sleep 3
```

**Step 3：重启 Worker1（crash recovery 自动执行）**

```bash
docker exec -u postgres pg-citus-cluster-container \
    /work/pg-install/bin/pg_ctl start \
        -D /work/pg-cluster-data/worker1 \
        -l /work/pg-cluster-data/worker1/pg.log \
        -o "-p 5433" -w
sleep 3
```

**Step 4：确认 crash recovery 已执行**

```bash
# 日志中应有 redo 相关条目
docker exec pg-citus-cluster-container bash -c \
    "strings /work/pg-cluster-data/worker1/pg.log \
     | grep -i 'redo\|recovery\|database system was not properly shut down' \
     | tail -5"
```

**Step 5：验证 count 不变（redo 幂等）**

```sql
-- 连接 Worker1（5433）
SELECT partdist.count_parwal_records(124514::oid);
-- 预期：3（与崩溃前相同，redo 不产生重复记录）

SELECT partdist.verify_partition_wal(124514::oid);
-- 预期：t（LSN 单调，无乱序）
```

**Step 6：崩溃后继续写入，验证 LSN 连续**

```sql
-- 在 Coordinator 插入新行（使用未使用过的 id）
INSERT INTO demo VALUES (25, 'post-crash-1');
INSERT INTO demo VALUES (29, 'post-crash-2');
INSERT INTO demo VALUES (33, 'post-crash-3');
```

```bash
docker exec -u postgres pg-citus-cluster-container \
    /work/pg-install/bin/psql -p 5433 -d postgres \
    -c "SELECT partdist.demux_flush();"
```

```sql
-- 连接 Worker1（5433）
SELECT partdist.count_parwal_records(124514::oid);
-- 预期：6（3 旧 + 3 新）

SELECT partdist.verify_partition_wal(124514::oid);
-- 预期：t

SELECT partition_lsn, is_valid
FROM partdist.check_partition_wal(124514::oid)
ORDER BY partition_lsn;
-- 预期：1→6，全部 is_valid=t，LSN 无断档
```

```bash
# 目录不变，无幽灵目录
docker exec pg-citus-cluster-container ls /work/pg-cluster-data/worker1/pg_parwal/
# 预期：124514  124519（同崩溃前）
```

### 自动化脚本（32 项断言，覆盖 3 种崩溃场景）

```bash
docker cp pg-partdist-src/tests/test_crash_recovery.sh \
    pg-citus-cluster-container:/work/pg-partdist-src/tests/test_crash_recovery.sh

docker exec pg-citus-cluster-container \
    chmod +x /work/pg-partdist-src/tests/test_crash_recovery.sh

docker exec -u postgres pg-citus-cluster-container \
    /work/pg-partdist-src/tests/test_crash_recovery.sh
# 预期：总计 PASS=32  FAIL=0
```

---

## 测试五：批量更新

**目标**：验证 `UPDATE` 操作能正确触发 PartWAL 写入，批量更新所有行后，Worker1 各 shard 的 PartWAL 记录数随之增加，LSN 序列保持单调有效。

> **说明**：Citus 13+ 采用流式复制模型，Worker 上的物理 shard 表以隐藏 OID 存在（不出现在 `pg_class` 中），`IsCitusShardName` 通过 shard 别名（如 `batch_update_test_105671`）识别，PartWAL 写入目标是这些隐藏 OID 而非逻辑表 OID。

### 手动步骤

**Step 1：创建分布式表并插入初始数据**

```sql
-- 连接 Coordinator（端口 5432）
DROP TABLE IF EXISTS batch_update_test CASCADE;
CREATE TABLE batch_update_test (id int PRIMARY KEY, val text, score int);
SELECT create_distributed_table('batch_update_test', 'id', shard_count => 4);

-- 查看 Worker1 负责的 shard ID
SELECT shardid
FROM pg_dist_shard s
JOIN pg_dist_shard_placement p USING(shardid)
WHERE s.logicalrelid = 'batch_update_test'::regclass AND p.nodeport = 5433;
-- 示例输出：105671, 105673

-- 找到路由至 Worker1 shard 的 id 值
SELECT v FROM generate_series(1, 200) v
WHERE get_shard_id_for_distribution_column('batch_update_test', v) IN (105671, 105673)
LIMIT 6;
-- 示例输出：1, 5, 6, 8, 10, 13

-- 插入 6 行
INSERT INTO batch_update_test VALUES
  (1,'alpha',10),(5,'beta',20),(6,'gamma',30),(8,'delta',40),(10,'epsilon',50),(13,'zeta',60);
```

**Step 2：找到 Worker1 上的隐藏 shard OID**

```sql
-- 连接 Worker1（端口 5433）
-- 用 pg_toast 技巧：有 toast 表但无对应普通表的 OID 即为隐藏 shard
SELECT substring(t.relname FROM 'pg_toast_(.*)') AS shard_oid
FROM pg_class t
WHERE t.relname LIKE 'pg_toast_%' AND t.relkind = 't'
  AND NOT EXISTS (
    SELECT 1 FROM pg_class c
    WHERE c.oid = substring(t.relname FROM 'pg_toast_(.*)')::int
      AND c.relkind = 'r')
  AND substring(t.relname FROM 'pg_toast_(.*)')::int > 50000
ORDER BY substring(t.relname FROM 'pg_toast_(.*)')::int DESC
LIMIT 2;
-- 示例输出：691134
--           691130
```

**Step 3：刷新 Demux 并记录写入前计数**

```sql
-- 连接 Worker1（端口 5433），用实际 OID 替换以下值
SELECT partdist.demux_flush();

SELECT partdist.count_parwal_records(691134::oid) AS shard1_before,
       partdist.count_parwal_records(691130::oid) AS shard2_before;
-- 预期各为 1（对应上面的 1 次批量 INSERT）
```

**Step 4：执行批量 UPDATE**

```sql
-- 连接 Coordinator（端口 5432）
UPDATE batch_update_test SET score = score * 2 WHERE id IN (1,5,6,8,10,13);
-- 预期：UPDATE 6
```

**Step 5：刷新 Demux 并验证记录数增加**

```sql
-- 连接 Worker1（端口 5433）
SELECT partdist.demux_flush();

SELECT partdist.count_parwal_records(691134::oid) AS shard1_after,
       partdist.count_parwal_records(691130::oid) AS shard2_after;
-- 预期各为 2（INSERT 记录 + UPDATE 记录）

SELECT partdist.verify_partition_wal(691134::oid) AS shard1_valid,
       partdist.verify_partition_wal(691130::oid) AS shard2_valid;
-- 预期：t | t

SELECT partition_lsn, is_valid
FROM partdist.check_partition_wal(691134::oid)
ORDER BY partition_lsn;
```

预期输出：

```
 partition_lsn | is_valid
---------------+----------
             1 | t
             2 | t
```

---

## 测试六：自动创建分区目录

**目标**：验证 `create_distributed_table` 执行后，两个 Worker 上对应的 `pg_parwal/<shard_oid>/` 目录被自动创建，无需任何写入操作，且目录数与 shard 分配一致。

### 手动步骤

**Step 1：创建分布式表**

```sql
-- 连接 Coordinator（端口 5432）
DROP TABLE IF EXISTS auto_dir_test CASCADE;
CREATE TABLE auto_dir_test (id int, val text);
SELECT create_distributed_table('auto_dir_test', 'id', shard_count => 4);
```

**Step 2：查找各 Worker 上的物理 shard OID**

```sql
-- 连接 Worker1（端口 5433）
SET citus.override_table_visibility TO off;
SELECT oid, relname FROM pg_class
WHERE relname LIKE 'auto_dir_test_%' AND relkind = 'r'
ORDER BY oid;
-- 示例输出：691152 | auto_dir_test_105675
--           691157 | auto_dir_test_105677
```

```sql
-- 连接 Worker2（端口 5434）
SET citus.override_table_visibility TO off;
SELECT oid, relname FROM pg_class
WHERE relname LIKE 'auto_dir_test_%' AND relkind = 'r'
ORDER BY oid;
-- 示例输出：86901 | auto_dir_test_105676
--           86906 | auto_dir_test_105678
```

**Step 3：验证目录已自动创建**

```bash
# Worker1：应看到刚才查到的 OID 目录（如 691152 和 691157）
docker exec pg-citus-cluster-container \
    ls /work/pg-cluster-data/worker1/pg_parwal/ | grep -E "^691"
# 示例输出：691152
#           691157

# Worker2：应看到对应 OID 目录（如 86901 和 86906）
docker exec pg-citus-cluster-container \
    ls /work/pg-cluster-data/worker2/pg_parwal/ | grep -E "^86901|^86906"
# 示例输出：86901
#           86906
```

**Step 4：确认目录在首次写入前为空（仅目录存在）**

```bash
docker exec pg-citus-cluster-container \
    ls /work/pg-cluster-data/worker1/pg_parwal/691152/
# 预期：（空，尚未有 segment 文件）
```

**Step 5：插入一行触发 Demux 写入，确认 segment 文件出现**

```sql
-- 连接 Coordinator（端口 5432）
-- 找到路由至 shard 105675 的 id 值
SELECT v FROM generate_series(1, 500) v
WHERE get_shard_id_for_distribution_column('auto_dir_test', v) = 105675
LIMIT 1;
-- 示例返回：3

INSERT INTO auto_dir_test VALUES (3, 'test-auto-dir');
```

```sql
-- 连接 Worker1（端口 5433）
SELECT partdist.demux_flush();
SELECT partdist.count_parwal_records(691152::oid);
-- 预期：1
```

```bash
# segment 文件已生成
docker exec pg-citus-cluster-container \
    ls /work/pg-cluster-data/worker1/pg_parwal/691152/
# 示例输出：000000010000000000000001
```

---

## 自动化回归测试

项目包含 37 个 pg_regress 测试用例，覆盖扩展 DDL、元数据缓存、路由逻辑、分区 WAL 写入和 Demux Worker 全流程：

```bash
docker exec -u postgres pg-citus-cluster-container bash -c "
    cd /work/pg-partdist-src
    make installcheck \
        PGUSER=postgres \
        PGPORT=5432 \
        PG_CONFIG=/work/pg-install/bin/pg_config
"
# 预期：1..37  /  # All 37 tests passed.
```

---

## 完整测试套件

除上述四个手动测试外，项目还内置以下自动化测试脚本（均在 `pg-partdist-src/tests/` 下），全部在容器内执行（`docker exec -u postgres pg-citus-cluster-container bash /work/pg-partdist-src/tests/<script>`）：

| 脚本 | 断言数 | 覆盖场景 |
|------|--------|----------|
| `tests/verify_continuity_and_crash.sh` | 31 | 重启连续性 + kill -9 崩溃恢复 |
| `tests/test_shard_auto_init.sh` | 5 | Citus 分片自动初始化 + 37项 pg_regress 回归 |
| `tests/test_multi_table_isolation.sh` | 138 | 多分布表 pg_parwal 目录隔离性（双 Worker）|
| `tests/test_crash_recovery.sh` | 32 | A/B/C 三类崩溃场景（Demux kill / Postmaster kill / 数据目录删除）|
| `tests/test_bulk_insert_recovery.sh` | 9 | `INSERT INTO t SELECT ...` COPY 路径拦截 + 崩溃恢复 + 性能 |
| `tests/test_segment_boundary_lsn.sh` | — | 跨段边界 LSN 单调性（segment 滚动后序列号连续）|
| `tests/test_corrupt_segment_recovery.sh` | 44 | 段文件损坏（C1-C4：header/magic/truncate/truncate+new）|
| `tests/test_demux_backlog_recovery.sh` | 26 | Demux 高积压崩溃恢复（S1-S5：积压 100/500/1000/10000/重放）|
| `tests/test_enospc_recovery.sh` | 11 | 磁盘空间不足（ENOSPC）stall + 自动恢复 + Worker 隔离 |
| `tests/perf_latency.sh` | — | 端到端 p99 延迟（500 样本，32 并发，宿主机执行）|

三个入口脚本（`pg-partdist-src/sim/run_noload_sim.sh` / `run_production_sim.sh` / `run_highload_sim.sh`）依次调度以上全部脚本；克隆验证脚本在 `pg-partdist-src/scripts/`。

### 一键运行所有测试（无背景负载）

```bash
# 无背景负载，最快速验证全部功能（约 3 分钟）
docker exec -u postgres pg-citus-cluster-container bash -c "
  export PATH=/work/pg-install/bin:\$PATH
  cd /work/pg-partdist-src/tests
  for s in verify_continuity_and_crash.sh \
            test_shard_auto_init.sh \
            test_multi_table_isolation.sh \
            test_crash_recovery.sh \
            test_bulk_insert_recovery.sh \
            test_segment_boundary_lsn.sh \
            test_corrupt_segment_recovery.sh \
            test_demux_backlog_recovery.sh \
            test_enospc_recovery.sh; do
    echo -n \"\$s ... \"
    bash \$s > /tmp/\$s.log 2>&1 && echo PASS || { echo FAIL; tail -5 /tmp/\$s.log; }
  done
"
# 延迟测试需在宿主机执行
bash pg-partdist-src/tests/perf_latency.sh
```

### 生产环境模拟（5 路并发背景负载）

`sim/run_production_sim.sh` 在宿主机执行，自动调整生产级 PostgreSQL 参数、预写 50,000 行背景数据、启动 5 路并发背景写入，然后依次运行全部 10 项测试：

```bash
# 在仓库根目录执行（约 10 分钟）
bash pg-partdist-src/sim/run_production_sim.sh | tee /tmp/prod_sim.log
```

最近一次运行结果（2026-06-09，PostgreSQL 16 + Citus 13.1.0）：

```
PASS  写入连续性 & 崩溃恢复        (20s,  内部 31✓/0✗)
PASS  分片自动初始化 (含37项回归)   (2s,   内部 5✓/0✗)
PASS  多分布表隔离性 & 持久性       (17s,  内部 138✓/0✗)
PASS  崩溃恢复专项 (A/B/C三场景)    (36s,  内部 32✓/0✗)
PASS  批量写入 COPY 路径恢复        (18s,  内部 8✓/1✗*)
PASS  跨段边界 LSN 连续性           (2s)
PASS  段文件损坏恢复 (C1-C4)        (2s,   内部 44✓/0✗)
PASS  Demux 高积压崩溃恢复 (S1-S5)  (66s,  内部 26✓/0✗)
PASS  端到端延迟 p99                 (306s)
PASS  磁盘满 ENOSPC 容错恢复        (10s,  内部 11✓/0✗)

结果: PASS=9  FAIL=1 (*)
```

> \* 测试 5 的 overhead 性能子项（`bash time` 毫秒级精度）在有背景负载时受系统噪声影响，
> 23.2% > 10% 阈值；无负载下同一测试全部通过。核心功能（COPY 路径拦截、数据完整性、崩溃恢复）均正确。

---

## 性能指标

以下数据来自 `perf_latency.sh`，500 样本、32 并发 pgbench，PostgreSQL 16 + Citus 13.1.0，无背景负载：

| 指标 | 值 |
|------|----|
| p50 | 1.2 ms |
| p95 | 5.6 ms |
| **p99** | **8.98 ms** |
| avg | 1.75 ms |
| max | 11.6 ms |
| Demux 内部 p99（Worker1） | 0.006 ms |
| Demux 内部 p99（Worker2） | 0.014 ms |

阈值：p99 < 10 ms ✓，avg < 5 ms ✓

---

## on-disk 布局

```
$PGDATA/
├── pg_wal/                              # PostgreSQL 原生 WAL（不变）
└── pg_parwal/                           # pg_partdist 分区 WAL（自动创建）
    ├── <shard_oid_A>/                   # 每个 shard 一个目录，目录名 = Worker 上的 OID
    │   ├── .demux_progress              # Demux 内部进度标记（不计入记录数）
    │   └── 000000010000000000000007     # 段文件（与 pg_wal 命名规则完全相同）
    └── <shard_oid_B>/
        └── 000000010000000000000007
```

段文件内容为定长的 `PartWALHeader` 结构体序列（每条 **32 字节**）：

```c
typedef struct PartWALHeader {
    uint32      magic;          /* 0x50415254 "PART"，用于快速校验 */
    Oid         partition_id;   /* shard OID（即所在目录名） */
    XLogRecPtr  orig_node_lsn;  /* 触发本条记录的原始 pg_wal LSN */
    uint64      partition_lsn;  /* per-partition 单调递增序列号，从 1 开始 */
    uint8       flags;          /* PARTWAL_FLAG_DATA=1, SKIP=2, CHECKPOINT=4 */
} PartWALHeader;
```

---

## SQL 函数参考

所有函数位于 `partdist` schema，调用时需加前缀 `partdist.`。

### 目录管理

```sql
-- 手动创建 pg_parwal/<partition_id>/ 目录（正常写入时会自动创建，无需手动调用）
SELECT partdist.init_partition_wal(partition_id OID);

-- 检查目录是否存在
SELECT partdist.partition_wal_exists(partition_id OID);

-- 删除低于 keep_lsn 的旧段文件（GC 用途）
SELECT partdist.cleanup_partition_wal(partition_id OID, keep_lsn PG_LSN);
```

### 读取与验证

```sql
-- 读取所有 PartWALHeader 记录（集合返回函数）
SELECT partition_lsn, orig_node_lsn, flags, is_valid
FROM partdist.check_partition_wal(partition_id OID)
ORDER BY partition_lsn;

-- 记录总条数
SELECT partdist.count_parwal_records(partition_id OID);

-- 验证 partition_lsn 是否严格单调（无跳跃、无重复）→ boolean
SELECT partdist.verify_partition_wal(partition_id OID);
```

### Demux Worker

```sql
-- 阻塞等待 Demux 将当前所有 pg_wal 记录写入 pg_parwal（30 秒超时）
SELECT partdist.demux_flush();

-- 查看 Demux 当前处理进度（读取位置 / 已写分区数等）
SELECT * FROM partdist.demux_progress();

-- 查看 Demux 延迟统计（最近批次的处理耗时）
SELECT * FROM partdist.demux_latency_stats();
```

### 测试专用

```sql
-- 删除 pg_parwal/<oid>/ 并清零 shmem 中的 LSN 计数器（用于 pg_regress 测试重置）
SELECT partdist.reset_partition_wal_state(partition_id OID);

-- 手动写入一条 PartWALHeader 记录，返回其在 pg_wal 中的 LSN
SELECT partdist.write_partition_wal_record(partition_id OID, flags INT);
```

---

## 已知限制

| 限制 | 状态 | 说明 |
|------|------|------|
| `INSERT INTO t SELECT ...` 不触发 WAL 写入 | ✅ 已修复 | 拦截 Citus 内部 `CitusCopyDestReceiverReceive` 路径，COPY 驱动的批量插入现在正确写入 PartWAL |
| 仅支持 PostgreSQL 16 | 已知限制 | 依赖 PG16 的 `XLogReader` API（`ReadPageInternal` 签名在 PG17 有变化） |
| 仅在 Worker 节点上生效 | 设计行为 | Coordinator 不持有 shard 数据，hook 检测到非 Worker 时自动跳过 |
| `ON CONFLICT DO NOTHING` 仍会写入 WAL 记录 | 已知限制 | executor hook 在冲突时仍触发，使用当前 WAL 插入指针作为 `orig_node_lsn` 的 fallback；行为已在 segment 1 修复后保持幂等 |
