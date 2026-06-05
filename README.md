# pg_partdist

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
9. [自动化回归测试](#自动化回归测试)
10. [on-disk 布局](#on-disk-布局)
11. [SQL 函数参考](#sql-函数参考)
12. [已知限制](#已知限制)

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
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/
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
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/124514/
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
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/
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
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/
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
docker cp pg-partdist-src/test_multi_table_isolation.sh \
    pg-citus-cluster-container:/work/pg-partdist-src/test_multi_table_isolation.sh

docker exec pg-citus-cluster-container \
    chmod +x /work/pg-partdist-src/test_multi_table_isolation.sh

docker exec -u postgres pg-citus-cluster-container \
    /work/pg-partdist-src/test_multi_table_isolation.sh
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
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/124514/
# 示例：000000010000000000000007

# 目录集合
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/
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
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/
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
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/
# 预期：124514  124519（同重启前）

# 段文件不变（原文件保留，新数据追加在同一文件中）
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/124514/
# 预期：000000010000000000000007（同重启前）
```

### 自动化脚本（31 项断言，涵盖重启 + 崩溃两种场景）

```bash
docker cp pg-partdist-src/verify_continuity_and_crash.sh \
    pg-citus-cluster-container:/work/pg-partdist-src/verify_continuity_and_crash.sh

docker exec pg-citus-cluster-container \
    chmod +x /work/pg-partdist-src/verify_continuity_and_crash.sh

docker exec -u postgres pg-citus-cluster-container \
    /work/pg-partdist-src/verify_continuity_and_crash.sh
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
ls /home/zhanhao/pg-citus-cluster/pg-cluster-data/worker1/pg_parwal/
# 预期：124514  124519（同崩溃前）
```

### 自动化脚本（32 项断言，覆盖 3 种崩溃场景）

```bash
docker cp pg-partdist-src/test_crash_recovery.sh \
    pg-citus-cluster-container:/work/pg-partdist-src/test_crash_recovery.sh

docker exec pg-citus-cluster-container \
    chmod +x /work/pg-partdist-src/test_crash_recovery.sh

docker exec -u postgres pg-citus-cluster-container \
    /work/pg-partdist-src/test_crash_recovery.sh
# 预期：总计 PASS=32  FAIL=0
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

| 限制 | 原因 |
|------|------|
| `INSERT INTO t SELECT ...` 不触发 WAL 写入 | Citus 对 SELECT 驱动的 INSERT 使用内部 COPY 路径，绕过 `ExecutorFinish` hook |
| 仅支持 PostgreSQL 16 | 依赖 PG16 的 `XLogReader` API（`ReadPageInternal` 签名在 PG17 有变化） |
| 仅在 Worker 节点上生效 | Coordinator 不持有 shard 数据，hook 检测到非 Worker 时自动跳过 |
| `ON CONFLICT DO NOTHING` 仍会写入 WAL 记录 | executor hook 在冲突时仍触发，使用当前 WAL 插入指针作为 `orig_node_lsn` 的 fallback |
