# 多机 TPC-C 实验脚本（lab）

2026-10-14 ~ 10-15 在内网 145 / 147 / 148 上跑 ShardPG 与单机 PostgreSQL 对比时用到的部署、压测与诊断脚本。
脚本里写死了主机 IP 与目录（145：`/data1/yyt/shardpg-fix-20261014`，147/148：`/home/yyt/shardpg-fix-20261014`），
换环境前先改这些路径。

## 部署

| 文件 | 作用 |
|---|---|
| `Dockerfile.prof` | 在基础镜像 `shardpg:tpcc-01e5635-20261014` 上打内核补丁 0012–0015、Citus 补丁，重编 PG / Citus / pg_partdist / pg_raft，产出 `shardpg:prof` |
| `node.sh <id> <port> <confdir>` | 在本机起一个节点容器（host 网络，initdb + 追加 `node<id>.conf`） |
| `redeploy.sh` | 两机布局（145 两个节点 + 147 一个节点）：构建（任一机失败即中止）→ 全新部署 → 接线 → 后台建库 |
| `redeploy4.sh` | 三机 4 节点布局：148 = coordinator + worker，145 / 147 各一个 worker |
| `small.conf` | 各节点追加的覆盖配置（`NODE_CONF_EXTRA`）：shared_buffers 8GB、vacuum 触发参数、`rc_update_recheck`、`early_lock_release` 等 |

## 压测

| 文件 | 作用 |
|---|---|
| `scale.sh <shardpg\|native> <终端数...>` | 同一库上按不同终端数各跑 1 min，打印 tpmC / tps / 延迟 / 一致性违例 |
| `seqruns.sh` / `nseqruns.sh` | 同一库连跑多轮（看随轮次是否衰退） |
| `localonly.sh on\|off` | 两个套件同时切换"无远程仓"（函数第二参传 1）⇒ 不产生跨节点 2PC |

对比纪律：单机 PG 与 ShardPG 必须同隔离级别（read committed）、同 shared_buffers（8GB）、同一份事务函数、都在新建库上测。

## 诊断（diag/）

| 文件 | 作用 |
|---|---|
| `waitev.sh` | 压测中对 worker 采样 `pg_stat_activity` 等待事件 |
| `walstat.sh` / `iostat.sql` | 一轮内 WAL 字节 / 刷盘次数 / FPI、`pg_stat_io` 写入 |
| `hoststat.sh` / `wbytes.sh` / `cpuattr.sh` | 主机 CPU / 磁盘、按进程类别的写盘字节与 CPU |
| `perfrec.sh` / `perfcg.sh` / `perfcallers.sh` / `perfincl.sh` | 容器内 perf 扁平剖析、调用图、调用者归并、包含占比 |
| `lk.sql` | 行锁等待者与阻塞者 |
| `idxcheck.sql` | 索引条目与堆扫描行数逐分片核对 |
| `rctest.sh` / `hot.sh` | READ COMMITTED 重评语义测试、单热行并发 +1 丢更新测试 |
| `pendmon.sh` | 压测中采样未决 2PC 登记表大小 |
