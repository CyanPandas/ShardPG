# raft2.0 交接文档

本文档用于把当前 `/home/pxr/pg-citus-cluster` 中的 Raft 工作迁移到另一台服务器，并集成到师兄 `CyanPandas/ShardPG` 的 `shardpg-3.0` 工作之上。

## 1. 当前方向

最终目标是实现分区级高可用：每个分区都有 primary/secondary 副本，primary 故障时不能只改 `partition_map`，必须通过 Raft 多数派决议完成切主，并且新 primary 必须证明已经追平切换点之前的分区日志。

当前短期主线不是“每个分区一个 Raft group”，而是：

- 控制面：纯 C `pg_raft`，负责 leader election、AppendEntries、控制面日志、节点状态、分区 primary 切换决议。
- 数据面：复用师兄 `shardpg-3.0` 的 `pg_partdist`、PartWAL、`follower_partition_map.applied_part_lsn`、follower replay 设计。
- 安全线：`OP_PARTITION_PRIMARY` 必须携带 `old_primary_node`、`primary_node`、`switch_partition_lsn`、`switch_orig_lsn`，未追平 `switch_partition_lsn` 的 secondary 不能晋升。
- 长期演进：每个 partition replica set 独立 Raft group，partition primary 等同该分区 Raft leader。

## 2. 不再维护的旧路线

以下内容已经从当前主线清理，不要在新服务器上恢复：

- pgElephant/pgraft Go bridge。
- `pg_raft.use_pgraft` GUC。
- 读取 `pgraft_cluster` 共享内存的桥接代码。
- `setup-pgraft-spike.sh`、pgraft spike 文档。
- 组会 demo、raft10 demo、`setup-raft10.sh` 等展示入口。

当前唯一推荐入口是：

```bash
cd ~/pg-citus-cluster
./setup.sh --raft
./run-tests.sh
```

## 3. 当前已完成能力

- RequestVote 已携带 `last_log_index` / `last_log_term`，并拒绝日志落后的候选者。
- HardState 已有本地持久化路径：`current_term`、`voted_for`、`commit_index`。
- AppendEntries 已用于控制面日志复制和多数派提交。
- 多数派不可用时 propose 失败，不留下可被后续偷偷提交的未提交尾日志。
- leader 宕机后可重新选举，旧 leader 恢复后降级为 follower。
- `OP_PARTITION_PRIMARY` 已携带切换进度字段。
- failover 已有最小 `applied_part_lsn` 过滤，未追平副本不能晋升。
- apply `OP_PARTITION_PRIMARY` 后已接入 `partwal_notify_primary_switch(...)` 边界函数。
- 当前测试基线：`run-tests.sh` 最近通过结果为 71 通过、0 失败。

## 4. 与师兄 ShardPG 的集成方式

拉取当前 Raft 发布分支：

```bash
git clone -b raft2.0 https://github.com/xinrongpeng14-web/raft.git ~/raft2.0
```

3. 把 `~/raft2.0/pg-raft-src`、`setup-raft.sh`、`run-tests.sh`、`test/sql/raft_*.sql`、`docs/interfaces/partwalmgr_raft.idl` 接入新的 ShardPG 工作区。

4. `pg_partdist` 侧不要盲目覆盖师兄最新文件。先阅读：

```bash
~/raft2.0/pg_partdist_sync_change_log.md
~/raft2.0/pg-partdist-adapter/README.md
```

然后只把必要接口合入师兄最新版：

- `partdist.get_partition_flush_lsn(partition_id)`
- `partdist.get_follower_applied_part_lsn(partition_id)`
- `partdist.partwal_notify_primary_switch(partition_id, old_primary_node, new_primary_node, switch_orig_lsn)`

5. 若师兄新版已经提供等价正式接口，优先适配正式接口，不要重复设计一套 Sender/Receiver/Apply。

## 5. pg-partdist-src 修改约束

后续如果修改 `/home/pxr/pg-citus-cluster/pg-partdist-src` 或新服务器对应 ShardPG 的 `pg-partdist-src`，必须同步更新 `pg_partdist_sync_change_log.md`，写明：

- 修改时间。
- 修改文件。
- 修改目的。
- 对 `pg_raft`、failover、PartWAL、follower replay 的影响。
- 验证方式和结果。

新增注释、计划和交接文档尽量使用中文。

## 6. 下一步开发顺序

1. 补 HardState 崩溃恢复专项回归，覆盖 `current_term / voted_for / commit_index` 重启后的连续性。
2. 把 `applied_part_lsn` 从最小占位过滤推进到真实 follower replay / ACK 更新。
3. 让 `partwal_notify_primary_switch(...)` 在 `pg_partdist` 内完成真实角色切换、缓存刷新和 replay 状态处理。
4. 设计 `OP_PREPARE_DECISION`、`OP_COMMIT_DECISION`，把 2PC prepare/commit 决议写入控制面 Raft。
5. 在短期方案稳定后，再评估每分区独立 Raft group。

## 7. 新会话提示词

```text
你现在接手我的 ShardPG+/pg-citus-cluster 项目。当前要在师兄 CyanPandas/ShardPG 的 shardpg-3.0 工作之上集成我的纯 C pg_raft 插件。主线目标是分区级主从切换：副本 primary 故障时必须通过 Raft 多数派决议切主，新 primary 必须追平 switch_partition_lsn，不能只更新 partition_map。

请先阅读 README.md、RAFT2_HANDOFF.md、raft_module_revision_plan.md、pg_partdist_sync_change_log.md。不要恢复 pgraft Go bridge、pg_raft.use_pgraft、setup-pgraft-spike、组会 demo、raft10 demo 或 setup-raft10。

重要约束：
1. 新增注释、计划和文档说明尽量使用中文。
2. 如果修改 pg-partdist-src 下任何内容，必须同步记录到 pg_partdist_sync_change_log.md，说明改了什么、为什么改、影响范围和验证方式。
3. 不要删除 pg-cluster-data、pg-install、.o/.so 等运行态或构建产物，除非我明确要求。
4. 优先复用师兄 shardpg-2.0 已有的 PartWAL、follower_partition_map.applied_part_lsn 和 follower replay 设计，不要在 pg_raft 内另起一套数据面复制框架。
5. 当前已完成：RequestVote 日志新旧检查、HardState 基础持久化、无多数派拒绝提交、陈旧候选者拒票、未追平 PartWAL 副本不能晋升、leader 宕机重选与旧 leader 降级回归。
6. 下一步优先做 HardState 崩溃恢复专项回归，然后推进真实 follower replay/ACK 与 applied_part_lsn 的联动。

开始前请先快速审查 git status、README.md、RAFT2_HANDOFF.md 和 raft_module_revision_plan.md，确认没有被带偏。
```
