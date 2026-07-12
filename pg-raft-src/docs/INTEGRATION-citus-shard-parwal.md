# 师兄分支集成说明：`feat/citus-shard-integration`

来源：<https://github.com/CyanPandas/ShardPG/tree/feat/citus-shard-integration>

## 集成的内容

已将 `pg-partdist-src/` 替换为师兄分支，新增：

| 模块 | 路径 | 作用 |
|------|------|------|
| 分区 WAL Hook | `src/wal/partition_wal.c` | Citus shard INSERT → 自定义 WAL RMGR |
| 段文件写入 | `src/wal/partition_wal_writer.c` | `pg_parwal/<oid>/` 落盘 |
| Demux Worker | `src/worker/demux_worker.c` | 后台从 pg_wal 拆分到分区目录 |
| SQL 测试 | `test/sql/21-37_*.sql` | 目录 / 头格式 / Demux / LSN 校验 |

## 与现有 pg_raft 的关系

- **pg_partdist**：数据面 worker 日志分区（师兄）
- **pg_raft**：控制面元数据 / failover（你）
- `shared_preload_libraries` 顺序：`citus,pg_partdist[,pg_raft]`
- 三节点均需 `pg_partdist` preload；`pg_raft` 扩展仍主要在协调节点创建

## 使用

```bash
cd ~/pg-citus-cluster
./setup.sh --build      # 若集群未建
./setup.sh --partdist   # 编译 + 三节点配置 + 冒烟
./setup.sh --raft       # 在 partdist 之后
./run-tests.sh
bash test-parwal-worker-check.sh   # 查看 worker pg_parwal
```

## 测试端口约定

| 测试集 | 端口 | 原因 |
|--------|------|------|
| 01-20 路由/元数据 | 5432 | 协调节点 |
| 21-37 分区 WAL | 5433 | worker1 上 Demux + parwal |

## 上游同步

```bash
git clone --branch feat/citus-shard-integration \
  https://github.com/CyanPandas/ShardPG.git /tmp/ShardPG-upstream
rsync -a /tmp/ShardPG-upstream/pg-partdist-src/ ~/pg-citus-cluster/pg-partdist-src/
```
