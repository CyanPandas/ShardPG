new1 实验室演示控制工具

在 145 服务器运行，入口：
  cd /data1/yyt/new1-demo-17c10c0-20261110
  ./shardpg_ctl status
  ./shardpg_ctl shards customer

此入口通过同目录 demo-control.json 绑定当前部署的提交、容器 ID、IP 和端口。
每次操作先验证容器身份和 release 标签；不会通过旧的 shardpg-ddl-* 容器执行。
需要 145 可以免密 SSH 到 147、148。工具使用 Python 3.6+，不依赖第三方库。
口令由容器内 /work/secrets/pgpass 提供。

当前端口：master=145:33432，worker1=145:33433，worker2=147:33434，
worker3=148:33435；新增 worker4=147:33436。
数据库目录统一为 /work/node/pgdata；socket 为 /work/node 和 /tmp。

模拟断电、检查及恢复：
  ./shardpg_ctl shards customer
  ./shardpg_ctl stop worker1 immediate
  ./shardpg_ctl status
  ./shardpg_ctl shards customer
  ./shardpg_ctl start worker1
  ./shardpg_ctl files worker1

只有 worker1 当前担任主的业务分片才会因停 worker1 而切主。
用 shards 查询新的 primary 和 term；status 的 running/stopped 表示数据库进程状态。
启动恢复时沿用原数据目录，不重新初始化，也不清理已有数据。

手动切主：从 shards 输出复制当前分片名，替换下面的 <当前分片名>：
  ./shardpg_ctl move-primary <当前分片名> worker1
  ./shardpg_ctl shards customer

共置的 customer 和 orders 同一哈希段会一起切主。目标节点必须在线且已有副本。
删表重建后分片号会递增，不能沿用报告中的历史分片号。

新增节点与副本迁移：
  ./shardpg_ctl add-node worker4 10.77.110.147 33436
  ./shardpg_ctl status
  ./shardpg_ctl move-replica <当前分片名> worker3 worker4
  ./shardpg_ctl shards customer

worker4 需事先用同版产物完成容器、initdb、扩展、地址和口令配置，
并在 demo-control.json 中登记实际容器 ID。add-node 执行数据库入组。
迁移的源节点须属于该分片；目标须尚未持有该副本。验收完成后 worker4 会留在集群。

其他操作：
  ./shardpg_ctl files worker2
  printf 'SELECT * FROM shardpg.nodes;\n' | ./shardpg_ctl sql
  printf 'SELECT * FROM partdist.pg_raft_group_status();\n' | ./shardpg_ctl sql worker1

原始报告同时混用了旧集群的 324xx、354xx 端口和旧容器名；这些命令须改用本工具。
验收状态见对应结果文件，不能由工具安装成功推断故障切换或事务测试通过。

验收完成后查看：
  cat CURRENT_STATUS.json
  cat ACCEPTANCE_RESULT.txt
  printf "SELECT * FROM ha.raft_log('customer', 1, 5);\n" | ./shardpg_ctl sql

ha.raft_log 仅安装在入口节点，从各 worker 的 Raft 视图读取数据。
重装观察函数可执行：./shardpg_ctl sql < install_raft_log.sql
安装脚本仅在创建辅助函数的事务中关闭 Citus DDL 传播；业务事务协议保持开启。
原始产物清单保留为 release.json，追加 SQL 修复后的清单为 release-with-dispatch-fix.json。
当前状态和通过范围以 CURRENT_STATUS.json、ACCEPTANCE_RESULT.txt 及对应验收日志为准。
