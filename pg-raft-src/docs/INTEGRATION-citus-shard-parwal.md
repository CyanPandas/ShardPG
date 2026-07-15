# pg_raft ↔ pg_partdist 集成说明(shardpg-3.0)

> 2026-07-15 重写。旧版描述的 `feat/citus-shard-integration` 三节点拓扑、
> Demux 流式拆分、`setup.sh --partdist/--raft`、test 21-37 端口约定均已过时,
> 历史内容见 Git 历史。当前以 `CyanPandas/ShardPG` 分支 `shardpg-3.0` 为准。

## 仓库与环境

| 模块 | 路径 | 角色 |
|------|------|------|
| pg_partdist | `pg-partdist-src/` | 数据面:分区 WAL 捕获与同步落盘(师兄) |
| pg_raft | `pg-raft-src/` | 控制面:元数据共识 / 分区级 failover(彭欣荣) |

运行环境为临时 raft4 容器 `pg-partdist-raft4-container` 的**四节点**拓扑:
master:5432 + worker1:5433 + worker2:5434 + worker3:5435,多数派 3/4。
四个节点全部 `shared_preload_libraries = 'citus,pg_partdist,pg_raft'`,
且每个节点都创建 `pg_partdist` / `pg_raft` 扩展(Raft leader 可能在任一节点)。

## 数据面现状(parwal-2.0 同步写路径)

与旧版描述的"Demux Worker 流式解复用"不同,3.0 实际路径为:

- `wal_insert_hook` 在 XLogInsert 处捕获 Citus 分片表 WAL 记录;
- `XACT_EVENT_PRE_COMMIT` / `PRE_PREPARE` 时 `PartWALFlush()` 同步落盘到
  `pg_parwal/<分片本地OID>/` 段文件(不变式:parwal fsync 先于事务提交 WAL fsync);
- `partition_lsn` 为每分区 1 起单调序号;
- Demux worker 退化为一次性崩溃恢复(`BGW_NEVER_RESTART`),`demux_flush()` 为空操作;
- follower 物理回放仍是设计稿(`pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md`),
  `follower_partition_map` 有表无 C 侧写入方。

## 控制面 ↔ 数据面边界(唯一接口)

pg_raft 不直接触碰 pg_parwal 内部,只通过 `pg-partdist-src/src/raft_boundary.c`
落地的三个 `partdist` schema SQL 函数:

| 函数 | 用途 |
|------|------|
| `get_partition_flush_lsn(oid)` | 本节点该分区最新已落盘 `partition_lsn`(切换点来源) |
| `get_follower_applied_part_lsn(oid)` | 本节点 follower 回放进度(切主候选过滤依据) |
| `partwal_notify_primary_switch(oid,int,int,pg_lsn)` | `OP_PARTITION_PRIMARY` apply 后的切主通知(回放落地前为日志占位) |

failover 语义(2026-07-15 起):切换点优先远程读**旧 primary 节点**上的
`get_partition_flush_lsn`(真实写路径下 parwal 在承载分片的节点上),不可达时
回退 leader 本地;候选人在追平 `switch_partition_lsn` 的副本中选
`applied_part_lsn` 最大者;无任何进度源时记 WARNING 并按最大 applied 兜底。

## 使用

```bash
# 容器内:编译安装 pg_partdist + pg_raft,四节点配置并收敛 leader
docker exec -u postgres pg-partdist-raft4-container bash /work/pg-raft-src/setup-raft.sh

# 宿主机:四节点 Raft 控制面回归(raft_01–raft_11,leader 端口自动探测)
bash pg-raft-src/run-raft-tests.sh

# 宿主机:数据面无负载回归(10 项)
bash pg-partdist-src/sim/run_noload_sim.sh
```

注意:容器内 `/work/pg-raft-src`、`/work/pg-partdist-src` 是独立拷贝,宿主机改完
需 `docker cp` 同步并 `docker exec -u 0 ... chown -R postgres:postgres`;改 C 代码后
需容器内重新 `make install` 并重启四节点(.so 是 preload 的)。

## 上游同步

pg_raft 上游为 <https://github.com/xinrongpeng14-web/raft> 分支 `raft2.0`;
同步进 `pg-raft-src/` 时需保持四节点适配(`setup-raft.sh` / `run-raft-tests.sh` /
raft_02、raft_04、raft_07 的节点 4 断言)。凡改动 `pg-partdist-src/`,必须在
`docs/pg_partdist_sync_change_log.md` 记录改了什么、为什么、影响范围与验证方式。
