# Raft 模块

## 当前主线

当前主线是 **纯 C `pg_raft` 控制面**，不再继续推进 pgElephant/pgraft 的 Go runtime 路线。后续工作集中在三件事：

- 补齐纯 C Raft 的安全语义：RequestVote 日志新旧检查、HardState 持久化、catch-up / snapshot。
- 将控制面 failover 与 PartWAL 复制进度绑定，避免把未追平日志的副本提升为 primary。
- 逐步接入 PartWAL 多副本同步和 2PC 决议备份，使 prepare / commit 满足 quorum ACK 语义。

## 当前进度（控制面共识已跑通）

| 组件 | 状态 |
|------|------|
| `pg_raft` 扩展 | 三节点 preload，`raft_enabled=on` |
| Raft 选举 | RequestVote / 多数派 / 随机超时 / 心跳 |
| 日志复制 | AppendEntries → commit → 持久化 `raft_log` → apply 到 `node_map`/`partition_map` |
| 快照 | apply 后写入 `raft_snapshot`（`node_map` + `partition_map` JSONB） |
| TopologyMonitor | BGWorker + libpq 自探测，仅 Leader 执行 |
| Failover | 停 worker → 探测 → 仅受影响分区 primary 切换（三节点元数据一致） |
| 重选回归 | leader 宕机重选、旧 leader 恢复后降级为 follower、非 leader propose 被拒绝 |
| 数据面复制 | `pg_partdist` 已有 PartWALHeader / Demux Worker / `pg_parwal/N`，但 quorum ACK 链路尚未接入 |

## 推荐入口

```bash
cd ~/pg-citus-cluster
docker start pg-citus-cluster-container
./setup.sh --raft              # 编译 + 三节点 Raft 配置
./run-tests.sh                 # 回归验证：pg_partdist + pg_raft + PartWAL 冒烟
```

## 常用命令

```bash
# 三节点状态
for p in 5432 5433 5434; do
  docker exec -u postgres pg-citus-cluster-container \
    /work/pg-install/bin/psql -p $p -U postgres -tAc \
    "SELECT * FROM partdist.pg_raft_get_cluster_status();"
done

# 在 Leader 上 propose（函数在 partdist schema）
docker exec -u postgres pg-citus-cluster-container \
  /work/pg-install/bin/psql -p 5432 -U postgres -c \
  "SELECT partdist.pg_raft_propose_node_status(3, 'down');"

# 多分区测试布局
# P1=9101 primary=node1 secondaries=node2,node3
# P2=9102 primary=node3 secondaries=node1,node2
# P3=9103 primary=node2 secondaries=node1,node3
```

## 关键 GUC

```ini
pg_raft.node_id = 1|2|3          # 每节点不同
pg_raft.raft_enabled = on
pg_raft.peers = '1@127.0.0.1:5432,2@127.0.0.1:5433,3@127.0.0.1:5434'
```

## 目录

- `pg-raft-src/` — 源码（`raft_consensus.c` 选举+复制，`topology_monitor.c` 探测）
- `setup-raft.sh` — 容器内安装脚本
- `run-tests.sh` — 项目回归入口
- `test/sql/raft_*.sql` — 回归用例

## 注意

1. SQL 函数在 **`partdist`** schema：`partdist.pg_raft_*`
2. 当前是 **一个控制面 Raft 集群 + 分区级元数据日志**，不是每个 partition 一个独立 Raft group
3. **Leader 可能在任意节点**，测试脚本会自动查找 Leader 端口
4. 当前 failover 仍以控制面元数据切换为主；下一阶段要继续把真实 follower replay / ACK 与 `applied_part_lsn` 联动
5. Raft 日志已同步落盘到 `partdist.raft_log`，apply 后生成 `partdist.raft_snapshot`
6. 修改源码后：`./setup.sh --raft` 或容器内 `make install` + 重启节点

## 下一步

- [x] RequestVote 增加 `last_log_index` / `last_log_term`
- [x] HardState 持久化：`current_term` / `voted_for` / `commit_index`
- [x] failover 绑定 `switch_partition_lsn` / `switch_orig_lsn`
- [x] 少于多数派拒绝提交
- [x] 未追平 `applied_part_lsn` 的副本不能晋升
- [x] leader 宕机重选，旧 leader 恢复后降级为 follower
- [ ] HardState 崩溃恢复专项回归
- [ ] follower replay / ACK 与 `applied_part_lsn` 真实联动
- [ ] partwalmgr 接口联调与 quorum ACK 复制语义
