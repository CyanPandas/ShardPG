# pg_partdist — Citus 分布式分区 WAL 插件

pg_partdist 是一个 PostgreSQL 扩展，通过 Hook 机制自动拦截 Citus 分布式表的 INSERT 操作，在每个 Worker 节点上生成**分区级 WAL 日志**（`pg_parwal/`），并由后台 Demux Worker 将记录拆分落盘到对应的分区段文件。

**整个过程对应用层透明**：使用标准 Citus SQL（`CREATE TABLE` + `create_distributed_table` + `INSERT`），无需任何 pg_partdist 专属语句，Worker 节点即自动生成分区目录和段文件。

```
Coordinator  INSERT INTO orders VALUES (1, 100)
                 │
                 ▼  Citus 路由
Worker1      INSERT INTO orders_102225 VALUES (1, 100)
                 │
                 ▼  ExecutorFinish Hook（pg_partdist 自动触发）
                 ├─ 识别 shard 名称 "orders_102225"
                 ├─ 自动创建 pg_parwal/42571/
                 └─ 写入 pg_wal（custom RMGR）
                 │
                 ▼  Demux Worker（后台进程，自动运行）
             pg_parwal/42571/000000010000000000000007  ← 段文件落盘
```

---

## 依赖

| 依赖 | 版本 | 说明 |
|------|------|------|
| Docker | 任意 | 运行预置环境 |
| Git | 任意 | 克隆仓库 |
| PostgreSQL | 16.x | 仓库已内置预编译二进制 |
| Citus | 13.1 | 仓库已内置预编译二进制 |

本机**不需要**单独安装 PostgreSQL 或 Citus，仓库 `pg-install/` 目录已包含完整的预编译二进制。

---

## 快速开始

### 1. 克隆仓库

```bash
git clone https://github.com/CyanPandas/ShardPG.git
cd ShardPG
```

### 2. 启动 Docker 容器

```bash
# 构建镜像（含 gcc/make 等编译依赖）
docker build -t shardpg .

# 启动容器，将仓库目录挂载到 /work
docker run -it \
  --name shardpg-dev \
  -v "$(pwd):/work" \
  shardpg bash
```

后续所有命令均在容器内执行。

---

### 3. 初始化三节点集群

容器内执行：

```bash
PG=/work/pg-install/bin
DATA=/work/pg-cluster-data

# 初始化三个数据目录
$PG/initdb -D $DATA/master  -U postgres --no-instructions
$PG/initdb -D $DATA/worker1 -U postgres --no-instructions
$PG/initdb -D $DATA/worker2 -U postgres --no-instructions

# 写入 postgresql.conf（三节点分别配置端口和插件）
for node_conf in \
  "$DATA/master/postgresql.conf|5432" \
  "$DATA/worker1/postgresql.conf|5433" \
  "$DATA/worker2/postgresql.conf|5434"; do

  conf="${node_conf%%|*}"
  port="${node_conf##*|}"
  cat >> "$conf" <<EOF

port = $port
listen_addresses = '*'
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
EOF
done

# 放开本地连接认证
for node in master worker1 worker2; do
  cat > $DATA/$node/pg_hba.conf <<'EOF'
local   all  all             trust
host    all  all  127.0.0.1/32  trust
host    all  all  ::1/128       trust
EOF
done
```

> **关键配置说明**
> - `shared_preload_libraries = 'citus,pg_partdist'`：citus 必须在前，pg_partdist 在后
> - `max_prepared_transactions = 200`：Citus 分布式事务要求
> - 三个节点都需要此配置，缺一不可

---

### 4. 编译并安装 pg_partdist

```bash
cd /work/pg-partdist-src

make PG_CONFIG=/work/pg-install/bin/pg_config
make install PG_CONFIG=/work/pg-install/bin/pg_config
```

---

### 5. 启动集群

```bash
PG=/work/pg-install/bin
DATA=/work/pg-cluster-data

$PG/pg_ctl start -D $DATA/master  -l $DATA/master/pg.log  -w
$PG/pg_ctl start -D $DATA/worker1 -l $DATA/worker1/pg.log -w
$PG/pg_ctl start -D $DATA/worker2 -l $DATA/worker2/pg.log -w

# 验证三节点运行正常
$PG/pg_isready -p 5432 && $PG/pg_isready -p 5433 && $PG/pg_isready -p 5434
```

---

### 6. 安装扩展并注册 Worker

```bash
PSQL=/work/pg-install/bin/psql

# 三个节点各自安装 citus 和 pg_partdist
for port in 5432 5433 5434; do
  $PSQL -p $port -U postgres -c "CREATE EXTENSION citus;"
  $PSQL -p $port -U postgres -c "CREATE EXTENSION pg_partdist;"
done

# Coordinator 注册两个 Worker
$PSQL -p 5432 -U postgres -c "
  SELECT citus_add_node('127.0.0.1', 5433);
  SELECT citus_add_node('127.0.0.1', 5434);
  SELECT * FROM citus_get_active_worker_nodes();
"
```

---

## 端到端验证

以下**全程只用标准 Citus SQL**，pg_partdist 在后台自动工作。

```bash
PSQL=/work/pg-install/bin/psql
DATA=/work/pg-cluster-data

# 清理旧的 parwal 文件，确保从空白开始
rm -rf $DATA/worker1/pg_parwal/* $DATA/worker2/pg_parwal/*

# ── 标准 Citus 建表 ──────────────────────────────────────────
$PSQL -p 5432 -U postgres -c "
  CREATE TABLE orders (id int, amount numeric, note text);
  SELECT create_distributed_table('orders', 'id', shard_count => 4);
"

# ── 普通 INSERT，应用层无需感知 pg_partdist ──────────────────
$PSQL -p 5432 -U postgres -c "
  INSERT INTO orders VALUES (1,  100.0, 'alpha');
  INSERT INTO orders VALUES (2,  200.0, 'beta');
  INSERT INTO orders VALUES (10, 150.0, 'gamma');
  INSERT INTO orders VALUES (20, 300.0, 'delta');
  INSERT INTO orders VALUES (30, 250.0, 'epsilon');
"

# ── 稍等片刻（Demux Worker 异步落盘）──────────────────────────
sleep 2

# ── 查看 Worker 上自动生成的分区目录和段文件 ─────────────────
echo "Worker1 pg_parwal:"
ls $DATA/worker1/pg_parwal/

echo "Worker2 pg_parwal:"
ls $DATA/worker2/pg_parwal/

echo "段文件（Worker1）:"
ls $DATA/worker1/pg_parwal/*/
```

期望输出示例（OID 数字因环境而异）：

```
Worker1 pg_parwal:
42123   42127

Worker2 pg_parwal:
42124   42128

段文件（Worker1）:
/work/pg-cluster-data/worker1/pg_parwal/42123/:
  000000010000000000000003
```

**验证记录数和 LSN 完整性（在 Worker1 执行）：**

```bash
$PSQL -p 5433 -U postgres -c "
SELECT
    s.shardid,
    ('orders_' || s.shardid)::regclass::oid          AS partition_id,
    partdist.count_parwal_records(
      ('orders_' || s.shardid)::regclass::oid)        AS parwal_records,
    partdist.verify_partition_wal(
      ('orders_' || s.shardid)::regclass::oid)        AS lsn_ok
FROM pg_dist_shard s
JOIN pg_dist_placement p ON s.shardid = p.shardid
WHERE s.logicalrelid = 'orders'::regclass
  AND p.groupid = 1
ORDER BY s.shardid;
"
```

期望：`parwal_records > 0`，`lsn_ok = t`。

---

## 回归测试

```bash
cd /work/pg-partdist-src

make installcheck \
    PGUSER=postgres \
    PGPORT=5432 \
    PG_CONFIG=/work/pg-install/bin/pg_config
```

37 个测试全部通过：

```
ok 1   - 01_extension_create
ok 2   - 02_partition_map_basic
...
ok 37  - 37_demux_full_cycle
All 37 tests passed.
```

---

## 架构说明

### 核心组件

| 组件 | 位置 | 作用 |
|------|------|------|
| ExecutorFinish Hook | `src/wal/partition_wal.c` | 拦截 Worker 上的 shard INSERT，识别分区 ID，写入自定义 WAL |
| Custom WAL RMGR | `src/wal/partition_wal.c` | 注册 `RM_EXPERIMENTAL_ID`，crash recovery 时重建 parwal 文件 |
| Demux Worker | `src/worker/demux_worker.c` | 后台进程，读取 `pg_wal` 中的 PartWALHeader 记录，写入 `pg_parwal/<partition_id>/` |
| PartitionWALWriter | `src/wal/partition_wal_writer.c` | 缓冲式段文件写入器，按 WAL 段边界切换文件 |
| Metadata Cache | `src/metadata_cache.c` | shared memory 中的分区-节点映射缓存（LWLock 保护） |
| Write Router | `src/router/write_router.c` | 写路由决策（local / remote / not_found / node_down） |

### Citus shard 自动识别原理

Coordinator 路由 INSERT 时，向 Worker 发送的 SQL 形如：

```sql
INSERT INTO public.orders_102225 (id, amount, note) VALUES ($1, $2, $3)
```

pg_partdist 的 ExecutorFinish Hook 从查询计划的 `rte->eref->aliasname` 字段读取表名 `"orders_102225"`，检测到后缀 `_102225` 符合 Citus shard 命名规则（`_[4位以上数字]`），自动触发 WAL 写入，无需预先在 `partdist.partition_map` 注册。

### on-disk 布局

```
$PGDATA/
├── pg_wal/                        # PostgreSQL 原生 WAL
│   └── ...
└── pg_parwal/                     # pg_partdist 分区 WAL
    ├── <shard_oid_A>/             # 每个 shard 一个目录（自动创建）
    │   ├── 000000010000000000000003   # 段文件（与 pg_wal 命名规则相同）
    │   └── 000000010000000000000004
    └── <shard_oid_B>/
        └── 000000010000000000000003
```

段文件内容为定长的 `PartWALHeader` 结构体序列：

```c
typedef struct PartWALHeader {
    uint32      magic;          /* 0x50415254 "PART" */
    Oid         partition_id;   /* shard OID */
    XLogRecPtr  orig_node_lsn;  /* 对应的 pg_wal LSN */
    uint64      partition_lsn;  /* per-partition 单调递增序列号 */
    uint8       flags;          /* PARTWAL_FLAG_DATA 等 */
} PartWALHeader;
```

---

## pg_partdist SQL 函数参考

```sql
-- 目录管理
SELECT partdist.init_partition_wal(partition_id oid);       -- 创建 pg_parwal/<oid>/
SELECT partdist.partition_wal_exists(partition_id oid);     -- 目录是否存在
SELECT partdist.cleanup_partition_wal(partition_id oid, keep_lsn pg_lsn);

-- 读取 / 验证
SELECT * FROM partdist.check_partition_wal(partition_id oid);   -- 读所有记录
SELECT * FROM partdist.read_all_headers(partition_id oid);      -- 读 lsn + orig_lsn
SELECT partdist.count_parwal_records(partition_id oid);         -- 记录条数
SELECT partdist.verify_partition_wal(partition_id oid);         -- LSN 单调性验证

-- Demux Worker
SELECT partdist.demux_flush();          -- 等待 Demux 处理完当前 WAL（30s 超时）
SELECT * FROM partdist.demux_progress();
SELECT * FROM partdist.demux_latency_stats();

-- 缓存 / 路由
SELECT partdist.pg_partdist_route_write(partition_id oid);  -- 路由决策
SELECT * FROM partdist.pg_partdist_cache_stats();

-- 测试专用
SELECT partdist.reset_partition_wal_state(partition_id oid);    -- 清空并重置 LSN
SELECT partdist.write_partition_wal_record(partition_id oid, flags int);
```

---

## GUC 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `pg_partdist.local_node_id` | `-1` | 本节点 ID，`-1` 表示未配置（路由层不生效） |

```ini
# postgresql.conf
pg_partdist.local_node_id = 1
```

---

## 已知限制

| 限制 | 原因 |
|------|------|
| `INSERT INTO t SELECT ...` 不触发 Hook | Citus 对 SELECT 驱动的 INSERT 使用 COPY 协议，绕过 ExecutorFinish |
| 仅支持 PostgreSQL 16 | 依赖 PG16 的 XLogReader API 语义 |

---

## 快速重置

```bash
PG=/work/pg-install/bin
DATA=/work/pg-cluster-data

$PG/pg_ctl stop -D $DATA/master  -m fast
$PG/pg_ctl stop -D $DATA/worker1 -m fast
$PG/pg_ctl stop -D $DATA/worker2 -m fast

rm -rf $DATA/master $DATA/worker1 $DATA/worker2
# 从 Step 3 重新开始
```
