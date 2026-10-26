# 2026-10-23 lab 展示预演

本轮以实际 lab 九张表讲稿和当前 `tpcc_demo.sql`、`tpcc_ha_demo.sql` 的操作为依据。九张表中 `customer`、`orders` 为共置分布式表，各 4 片、每片 1 主 2 从，其余 7 张为 master 本地表。lab 有三台 worker，实际仓库 1、2 初始同在 worker1，仓库 3、4 同在 worker2；跨机器事务检查使用仓库 1 和 3，不能照搬本地四 worker 环境的路由说明。

范围为 148/master:32432、145/worker1:32433、147/worker2:32434、148/worker3:32435，以及本轮创建并移除的 147/worker4:32436。未操作 TPCC 项目或其容器。新 worker4 无挂载，安装和数据都在自己的容器文件系统中；未更新宿主机的安装目录或 nodes.conf。

## 已核对的展示内容

| 环节 | 实测检查 |
|---|---|
| 九张表建表与显示 | 9 张表、2 张分布式表、8 个分片、每片两个从；`\dt`、`\dt+`、`\d`、`\d+` 正常，没有建表遗留临时表 |
| 分布式事务 | 跨机器提交后余额守恒；显式回滚、重复订单出错后的整笔回滚均恢复两边余额 |
| 两个客户端会话 | 未提交余额对另一会话不可见；长事务内余额和行数保持快照；提交后新快照看到新值；同一行的第二个写者等待后收到 `40001`，先提交者胜 |
| 持续写入与增删改查 | 第一轮写入脚本完成 25 轮、0 次失败；单分片查询计划为 1 个任务、聚合为 4 个；共置连接、更新、删除结果正确；后台订单均有对应客户 |
| Raft 展示 | 三成员日志追齐；能解码 Heap INSERT、分布式 PREPARE 和 COMMIT；停机节点显示 unreachable |
| 自动切主与恢复 | worker1 使用 `pg_ctl -m immediate` 停机；最终完整轮约 28 秒登记切主、34 秒恢复读取；已确认提交的数据摘要一致；恢复、追平、切回后停机期间的写入仍在 |
| 新节点和在线副本迁移 | 全新 worker4 入簇约 31 秒；五节点 peers/组 0 成员均为 5；新节点只重放当前 8 个分区登记，没有历史 205 条残留；迁移源副本的物理表和组被清理 |
| 迁移期间持续写入 | 最终轮迁移约 14 秒；写入脚本完成 19 轮、0 次失败；旧客户余额不变，后台订单没有孤儿引用 |
| 新节点当主 | 两个共置分片一起切到 worker4，全部客户数据摘要一致；查询实际在 worker4 执行，更新和共置连接均正确 |
| 删表和移除新节点 | 五节点 prepared、SQL 日志空洞均为 0；删表后数据组、分区登记、hardstate 均为 0；worker4 及专用镜像已删除，原四节点均 running/active；TSO 1000146 → 1100001 |

故障窗口不承诺每条客户端请求成功：第一轮断电后的首次读取出现旧连接断开错误。交互式 psql 会继续运行，路由和连接恢复后重试；验收要求恢复后数据一致。重复键 `23505` 和写冲突 `40001` 是事务展示的预期错误。

## 本轮发现并修复的两处问题

1. 原宿主机 `shardpg_ctl add-node` 挂载旧 `pg-install`，会把新节点接到旧二进制上。新增容器配置模式，从当前 worker2 的容器安装复制新节点；新增节点仅允许本演示的 worker4。创建、移除及控制配置均在自己的容器内完成。移除时直接更新 postgres 所有的配置文件，避免 `sed -i` 对安装目录写权限的要求；该收尾已在最终完整轮验证。
2. 新节点重放 N112 之前的历史时出现 205 条已删分片登记（102008–102212）。原 `partition_map_unregister` 将它们塞进一条超过 768 字节的 Raft 记录，且 master 本来没有这些行时可能误报成功。N117 改为每批最多 32 个 bigint、提交失败明确报错。已通过 Raft 日志补写历史删除，重启旧 worker4、再创建全新 worker4 均未重现。新增本地专项 21/0，原删表/hardstate 回归 24/0。

## 后续展示的控制入口

**2026-10-26 入口更新**：按用户要求，在 145 的 DDL 专属目录安装了启动脚本，现在直接 `cd` 后执行 `./shardpg_ctl`：

```bash
cd /data1/yyt/shardpg-ddl-demo/pg-install/bin
./shardpg_ctl status
./shardpg_ctl shards
```

对应展示命令为 `./shardpg_ctl add-node worker4 10.77.110.147 32436`、`./shardpg_ctl move-replica <实际分片名> <实际源节点> worker4`、`./shardpg_ctl move-primary <实际分片名> worker4`。删表后执行 `./shardpg_ctl remove-node worker4`，恢复 master + 3 worker；移除时会按现有 Raft peers 约束重启这套 DDL 数据库。

宿主机入口来自 `demo/lab/shardpg_ctl_host`，通过 Docker 读取当前 `shardpg-ddl-worker1` 内的实现并启用容器配置模式，因此加节点仍复制已升级的容器安装。原入口备份为同目录的 `shardpg_ctl.pre-cd-20261026`；此次仅更新 145 的这个 DDL 入口，没有改宿主机数据库二进制、其他项目或服务。直接使用 `docker exec shardpg-ddl-…` 调用 psql、pg_ctl 的方式仍可用。

10 月 26 日只读验证通过：`./shardpg_ctl status` 显示四节点 running/active，`./shardpg_ctl shards` 列出现场已有的 customer/orders 共 8 个分片，原生 `docker exec … pg_ctl status` 正常。本次未停启数据库、未修改表数据。

收尾只读核对：三台主机的其他 424 个容器 ID、名称和状态均未变化；四个 DDL 节点在本次预演时段未出现 PANIC 或 signal 6/11 崩溃记录。

原始 SQL、两个会话记录、写入统计及完整日志保存在本机 `/tmp/shardpg-ddl-demo-20261023/`。初次失败的日志也保留，最终结果以 `transactions.log`、`high_availability-final.log`、`migration-final.log`、`historical-residue-repair-final.log`、`n117-local-final.log` 和 `drop-regression-final.log` 为准。
