# pg_test 测试环境 ↔ GitHub 分支 `shardpg-test` 强绑定说明

> 建立日期：2026-09-09。目的：给实验性改动一块独立场地，
> **不干扰 `shardpg-TX2`（tx2 线）与 `~/shardpg-demo`（演示材料）**。

## 1. 绑定关系（三处一一对应）

| 角色 | 位置 | 说明 |
|---|---|---|
| GitHub 分支 | `shardpg-test` | 从 `shardpg-TX2` 尖端 `efc6a2e` 原样分出，建时零差异 |
| 开发工作区 | `/home/zhanhao/shardpg-test-work` | **唯一**写代码/文档的地方；origin 带 token，已装推送守卫 |
| 容器环境 | `pg-test-container`（镜像 `pg-partdist-raft4-env`） | 1 coordinator + 8 workers，仅作构建/运行沙箱 |
| 一次性克隆 | `/home/zhanhao/pg_test/ShardPG` | `reproduce-env.sh` 建环境用的克隆，**不要当工作区** |

节点与端口：`coordinator = node1:5432`，`worker1..worker8 = node2..node9:5433..5440`。

## 2. 隔离保证（怎么保证 tx2 / demo 不受影响）

1. **分支侧**：`~/shardpg-test-work/.git/hooks/pre-push` 拒绝一切目标不是
   `refs/heads/shardpg-test` 的推送；`remote.origin.push` 也钉死成
   `refs/heads/shardpg-test:refs/heads/shardpg-test`，裸 `git push` 只会推本分支。
2. **目录侧**：本线的一切产出只落 `~/shardpg-test-work`；
   `~/shardpg-tx2-work`（tx2 工作区）与 `~/shardpg-demo`（演示文档）一律不碰。
3. **容器侧**：`pg-test-container` 与 `pg-citus-tx2-container` 是两个独立容器，
   数据目录互不可见。

## 3. 宿主机只放得下一套 9 节点环境（硬约束）

宿主机 2 核 / 3.8GB。现有 9 节点环境共四套，**必须二选一**：
`pg-citus-replay-container` / `pg-citus-tx-container` / `pg-citus-tx2-container` / `pg-test-container`。

切换方式（`stop` 保留数据，`start` 即恢复）：

```bash
# 切到 pg_test
docker stop pg-citus-tx2-container && docker start pg-test-container
# 切回 tx2
docker stop pg-test-container && docker start pg-citus-tx2-container
```

并存的后果在 tx2 那边实测过：OOM + 连环改选 + 数据组丢主，症状会伪装成回放缺陷。

## 3.1 容器进程名额（2026-09-14：僵尸进程会把容器 fork 名额耗尽）

**现象**：旧 `reproduce-env.sh` 用 `docker run -d … sleep infinity` 起容器，PID 1 是 `sleep`，
不回收孤儿。`pg_ctl` 起的 postmaster 在 `docker exec` 返回后就挂到 PID 1 名下，节点每停一次、
每被 kill -9 一次（连同它遗留的 backend）都变成一个永不回收的僵尸。僵尸不占 CPU/内存，但**照占
容器 pids cgroup 的名额**：systemd 给 docker scope 的默认 `TasksMax` = 内核 threads-max 的 15%，
本机（4 GB）是 **4621**。

**实测**（pg-test，建于 09-10）：4 天攒到 **2486** 个僵尸（全是 uid 999 的 postgres），按创建日
196 / 705 / 680 / 483 / 422，名额已用 2749/4621。照这个速度 3~4 天用满 —— 之后容器里一切 fork 失败：
postgres 起不了 backend / bgworker，`docker exec` 也可能失败，套件会整片以"连不上"的形态变红，
极易误判成产品缺陷。

**处置**：
1. `reproduce-env.sh` 已改为 `docker run -d --init …`（PID 1 = docker-init/tini，负责回收），新建环境不再有这个问题。
2. **已存在的容器**加不了 `--init`，要重建。数据全在可写层（无 volume，`/work` 约 0.8 GB），所以走
   "快照 → 带 --init 重起"，不丢数据：
   ```bash
   # 1) 干净停 9 个节点（-m fast，确认 postmaster.pid 全部消失）
   # 2) 快照 + 旧容器改名留作回滚
   docker stop -t 5 pg-test-container
   docker commit pg-test-container pg-test-env-snap:<日期>-preinit
   docker rename pg-test-container pg-test-container-preinit
   # 3) 同名同 hostname 带 --init 重起（集群元数据只用 127.0.0.1/localhost，与 hostname 无关）
   docker run -d --init --hostname <旧 hostname> --name pg-test-container pg-test-env-snap:<日期>-preinit sleep infinity
   # 4) 起 9 个节点，rm coordinator/pg_tso_boot，核对 PID 1 = /sbin/docker-init、僵尸 0
   # 回滚：docker rm -f pg-test-container && docker rename pg-test-container-preinit pg-test-container && docker start pg-test-container
   ```
3. 门禁 `run_p6_exit.sh` 起跑前打印名额 / 僵尸数 / PID 1：用量过 50% 警告，过 80% FATAL（退出码 3）。

其余三套 9 节点容器（replay / tx / tx2）同样是 `sleep infinity`，**本线不动**；它们启动后长期跑测试也会遇到同一问题。

## 4. 常用命令

```bash
# 重建/校验/销毁（脚本在本工作区里，用本分支那份）
ENV_NAME=pg_test BRANCH=shardpg-test \
  bash ~/shardpg-test-work/pg-raft-src/reproduce-env.sh {up|verify|test|destroy}

# 连库
docker exec -it -u postgres pg-test-container \
  /work/pg-install/bin/psql -p 5432 -U postgres -d postgres

# 把工作区代码同步进容器、重编、安装扩展（两个扩展都做；可只给一个目标名）
bash ~/shardpg-test-work/pg-partdist-src/scripts/sync_build.sh [pg-partdist-src|pg-raft-src]
# 只核对"容器源码 == 工作区"与".so 导出符号齐全"，不构建：
bash ~/shardpg-test-work/pg-partdist-src/scripts/sync_build.sh --check
```

> **不要再用 `docker cp` + `make -s` 手工同步**（2026-09-13，P7-W5）。那条路在"看起来成功"
> 的状态下装进去过旧 .so 两次：`docker cp` 漏带 `include/`；`grep error` 把编译错误滤掉后
> `make install` 拿 stale `.o` 重链；`docker cp` 保留宿主机 mtime，源文件比容器 `.o` 旧时
> make 直接判"已是最新"；PGXS 未开 autodepend，头文件变了不触发重编。`sync_build.sh`
> 按内容同步并刷新 mtime、同步后逐文件核对、头文件变了自动 `make clean`、编译输出不过滤
> （implicit declaration 也判失败）、核对已装 .so 与构建产物一致、逐个核对应导出符号。
> 它报"头文件有变"时，节点必须**整簇重启**才算生效。

清理（`verify_cleanup.sh` 不覆盖本环境）：

```bash
docker rm -f pg-test-container && rm -rf /home/zhanhao/pg_test
# 工作区另删：rm -rf /home/zhanhao/shardpg-test-work
```

## 4.1 两条实测出来的操作纪律（2026-09-09，T7.1 期踩出来）

**① 重启 coordinator 之后必须删 TSO boot 标记，否则全簇停发号。**
设计 §2.4 的防呆：TSO 首次服务时落 `$PGDATA/pg_tso_boot`，重启后检测到"曾服务过"
即拒绝发号（把"静默重发号 = 数据损坏"变成"响亮停摆"）。测试集群里每次
`pg_ctl restart` 完 coordinator 都会撞上，症状是后面每一步莫名其妙地空/红：

```
ERROR:  TSO 检测到上个纪元的 boot 标记（pg_tso_boot），拒绝发号
```

处置（**仅限可重建的测试集群**；生产语义是整簇重建）：

```bash
docker exec -i -u postgres pg-test-container bash -c \
  'rm -f /work/pg-cluster-data/coordinator/pg_tso_boot && \
   /work/pg-install/bin/pg_ctl restart -D /work/pg-cluster-data/coordinator \
     -l /work/pg-cluster-data/coordinator.log -w -t 60'
```

**② `ALTER SYSTEM` 必须自己占一条 `-c`。**
写成 `-c "ALTER SYSTEM RESET ...; SELECT pg_reload_conf();"` 会被 psql 当成隐式
事务块 ⇒ `ERROR: ALTER SYSTEM cannot run inside a transaction block` ⇒ **白名单
根本没撤掉**，而调用方通常把输出丢进 `/dev/null`，于是下一轮验收在"以为清干净了"
的污染现场上跑，红得毫无道理（本期实测浪费了两轮）。

## 4.2 再三条（2026-09-10，T7.2/T7.8 期踩出来）

**③ 停节点的套件必须自带复原（EXIT trap），而且要在杀之前先登记。**
`test_promote_handover_p7.sh` [5] 杀掉旧主之后从不复原，跑完那个节点一直躺着。
后果不是"这个套件红"，而是**后面每个套件都被毒化**：紧接着的
`test_slot_reclaim_p7` 第一条断言死在"节点 :5433 可连"，和它要验的槽位回收
毫无关系 —— 极容易被误判成"槽位回收回归了"。已修（登记为 P7-E6）。
同一条纪律适用于 T7.13 的 OPS 8 套改造。

**④ 验收套件不能复用固定表名 —— 半删的分片会把那张分布表永久锁死。**
在 worker 上直删分片表之后（绕过 §10 的常规手法），协调者侧那张分布表
**两条路都删不掉**：走 Citus 2PC 被"含分片打标表 DROP 禁 PREPARE"拦；
绕到各节点本地删，那个分区组往往已经因为主副本的表先没了而凑不齐多数派，
`propose` 直接失败。于是第二轮必撞 `relation already exists`。
`test_baseline_clog_p7.sh` 已改成每轮 `TBASE="t72_b_$(date +%H%M%S)"`。
底层缺陷登记为 **P7-D3**，未修。

**⑤ 改了代码又要干净集群时，用 `reproduce-env.sh reset`，别用 `up`。**
`up` 会从 GitHub 重新克隆，**工作区里尚未提交的改动会被丢掉**。`reset` 只重建
9 个数据目录，容器与已装好的扩展原样保留（先自己 `make install`，再 reset，
最后节点是新起的，装载的就是新 `.so`）。改了 shmem 结构的话必须整簇重起 ——
`reset` 天然满足。

## 4.3 再三条（2026-09-15，P7-W6/W7 期踩出来）

1. **给 pg_raft 加 SQL 函数要手工滚到 9 节点**：`sync_build.sh` 只装 .so（守卫⑥核对符号在
   .so 里），门禁的 `refresh_extension_sql.sh` 只对齐 pg_partdist 的函数面，pg_raft 的没人管。
   用 `bash pg-partdist-src/scripts/apply_pg_raft_sql.sh 函数名…`：从 `pg_raft--1.0.sql` 抽块、
   替换 MODULE_PATHNAME、带 `citus.enable_ddl_propagation=off` 逐节点执行（Citus 否则拦 worker
   上的 CREATE FUNCTION："operation is not allowed on this node"）、`ALTER EXTENSION pg_raft ADD`
   入籍，最后核对 SQL 文件里全部函数在 9 节点点得到名。
2. **高并发取证用 `tests/test_highload_w7.sh`**（已入门禁，默认 16 客户端 40 s，`CLIENTS=/DUR=`
   可调）。32 客户端会把协调者 `max_connections=100` 打满（pgbench 32 + Citus 每客户端到各
   worker 的连接），worker 侧 TSO 取 commit_ts 的自连失败 ⇒ 事务报 `TSO 不可达或拒绝服务`
   中止 —— 环境配置，不是产品缺陷。这台 2 vCPU 机器 tps 只有 30–40，8 客户端 idle 就已 0%，
   延迟 p99 秒级是饱和，不要拿它判写路径性能。
3. `pkill -f 模式` 会匹配到**发起它的那个 shell 自己**（命令行里就含模式串）把自己杀掉，
   后面的步骤一个都不跑（2026-09-15 实测）。要杀后台链，把命令写进脚本文件再执行，或按
   `pgrep` 结果排除 `$$`。

## 5. 当前在这块场地上做什么

**批次 #12 起：P6 缺陷收口（P7）。** 盘点见
`pg-partdist-src/docs/P6_EXIT_AUDIT.md`，计划见
`pg-partdist-src/docs/P7_REMEDIATION_PLAN.md`（6 个批次 + 6 条待裁 + 出口标准）。
第一批四条都是"切主/供给后已提交数据不可见或丢失"级别，且都在 pg-partdist 侧、
不需要解冻 pg_raft。

## 6. 建环境时的验收结果

见本文件末尾「验收记录」一节。

### 验收记录

| 时间 | 动作 | 结果 |
|---|---|---|
| 2026-09-09 | `reproduce-env.sh up` | 9 节点全起，group0 **1s 收敛，leader = node1**，`/work/REPRODUCED_FROM_COMMIT` = `efc6a2e` |
| 2026-09-09 | `reproduce-env.sh verify`（V1–V6） | **PASS=25 FAIL=0**，与 pg-citus-tx2 建环境时的基线一字不差 |

全量套件（`reproduce-env.sh test`，十套件、数十分钟）**建环境时没跑**；
本分支代码与 `shardpg-TX2` 尖端零差异，回归数字沿用 TX2 的记录，需要时再跑。
