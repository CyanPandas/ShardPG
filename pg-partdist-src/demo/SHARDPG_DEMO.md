# ShardPG 演示教程：5 个节点上的一张业务表（只输入 SQL）

> 本教程每一步都在 **2026-10-03** 的 `pg-test-container`（1 master + 4 worker，共 5 个节点）上**完整实跑过一遍**，下面每段 SQL 之后的输出就是那次实跑的原样输出（灌数据到宕机归队共用时约 2 分 58 秒）。你的分片号、时间戳、事务号会不同，其余应当一致。

## 一、你的要求（梳理与润色）

在 Windows 11 PowerShell 终端里做一场 ShardPG 的现场演示，按**真实业务的顺序**走，只看功能体现、不铺细节：

1. **规模**：5 个节点 —— 1 个 master（只做路由 + TSO）+ 4 个 worker。
2. **前面全部提前做好**：建表（4 个分片，一台 worker 一片）、每个分片建 Raft 组（**副本因子 3**：1 主 + 2 副本，不是每台都存一份）、选主、供副本、打标接入事务系统 —— 这些都放进 `start`，**不在台前展示**；`start` 成功后只显示“准备好了”。
3. **演示从灌数据开始**：业务数据放在文件里，一条 `\i` 读进去；灌完查一次各分片的数据，再看各分区的副本信息。
4. **并发事务**：两个窗口同时做跨分片的读写 —— 快照隔离、写写冲突、总额守恒。
5. **切主**：手动把某个分片的主切走，看 master 的路由自动跟过去、数据照读。
6. **宕机**：停掉一台（等同断电），看 Raft 自发选出新主、期间照常读写，再把它拉回来自动归队。
7. **全自动版**：另有一个脚本自己“敲” SQL —— 每条 SQL 逐字打出来、真的执行、结果实时显示，一条命令跑完整场（见第三节末）。

> 要看更细的东西（流控、分片 xid 分配器与 clog、页面多版本、惰性回放、副本逐字节比对），
> 用上一版的 14 步脚本 `shardpg_demo_steps_full.sql`。

## 二、文件（都在 `~/shardpg-test-work/pg-partdist-src/demo/`，只用于演示）

| 文件 | 作用 |
|---|---|
| `shardpg_demo_steps.sql` | **演示脚本**：只有 SQL 和事务，按步骤排好，`-- @A` / `-- @B` 标明在哪个窗口输入 |
| `shardpg_demo_functions.sql` | 封装好的演示函数库（`demo.*`），只装在 master 上；演示结束整体删除 |
| `shardpg_demo.sh` | 服务器上的启动器：`start` 准备环境（含提前建表接入）/ `sql A`、`sql B` 打开会话 / `stop` 恢复环境 / `auto` 全自动演示 |
| `shardpg_demo_auto.py` | **全自动演示**：自己一条条“敲” SQL（逐字打出来，A> / B> 两个窗口）、真执行、实时显示结果；`bash shardpg_demo.sh auto` |
| `shardpg_demo.ps1` | Windows 端的一键入口（可选）：`.\shardpg_demo.ps1 start` / `sql A` / `sql B` / `stop` / `auto` |
| `demo_data.sql` | 业务数据（500 个账户，分 10 批，每批一个全局事务），演示里 `\i /tmp/demo_data.sql` 读它 |
| `gen_demo_data.py` | 重新生成上面这份数据：`python3 gen_demo_data.py <行数> demo_data.sql` |
| `shardpg_demo_steps_full.sql` | 上一版的 14 步详细脚本（流控、分片 xid/clog、惰性回放、逐字节比对），要看细节时用 |
| `SHARDPG_DEMO.md` | 本教程 |
| `shardpg_demo128.sh` 等 | **演示二**：128 个分区组上的分布式事务，见 `SHARDPG_DEMO128.md`；与本演示共用集群，**两套互斥，换演示前先 stop** |

`start` 对集群做的临时改动只有：在 master 上建 `demo` 模式（函数库 + dblink）、打开 TSO（会重启一次 master）、把 4 台 worker 的 Raft 选举超时从 6 s 放宽到 15 s、TSO 租约从 60 s 放宽到 5 分钟（见“已知问题”第 6、7 条），把 `demo_data.sql` 拷进容器的 /tmp，然后**提前建好 `account` 表并接入系统**（建组 → 选主 → 供副本 → 打标；建组走产品函数 `raft_replicate_table_shards_bulk`，副本因子 3，成员按数据所在节点轮转）。`stop` 把它们全部撤掉，并按演示前各节点 `postgresql.auto.conf` 的原样还原参数；实测还原后与演示前的环境快照逐项一致。

> **术语（按本项目的设计口径，见 `docs/DTX_2PC_DESIGN.md` §2.1/§2.3）**：
> **master** 就是 :5432 那个节点 —— 只做**路由**和 **TSO**，不放分片数据、不领导任何数据组；所有读写都经它路由。
> **协调者**是**事务**的概念，不是节点：每个全局事务在自己的**写集**里挑一个分片组当**协调组**（`participants[hash(dtxid) mod n]`），决议写进那个组的 Raft 日志才算全局提交点；协调组的 leader 挂了，组内选出新 leader 自动接着当协调者。master 不领导任何数据组，所以永远不会被选成协调组。

## 三、准备：从 Windows 11 PowerShell 进入演示

> 中文要正常显示：用 Windows Terminal 打开 PowerShell（默认 UTF-8）；老式控制台先执行 `chcp 65001`。

**窗口 1**（之后作为窗口 A）—— 准备演示环境：

```powershell
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh start"
```

```text
演示一（4 个分片） 正在准备：建表（4 个分片）→ 建组（副本因子 3）→ 选主 → 供副本 → 打标（约 2–5 分钟）…
准备好了
```

`start` 先打一行“演示一（4 个分片） 正在准备：建表（4 个分片）→ 建组（副本因子 3）→ 选主 → 供副本 → 打标（约 2–5 分钟）…”，成功后再打“准备好了”。这几分钟它在替你把**台前不展示的部分**做完：
建 `account` 表（4 个分片，一台 worker 一片）、每个分片建一个 Raft 组（**副本因子 3**：数据所在那台 + 按端口轮转的另外两台，
第四台不在这个组里）、等数据所在那台当选、把副本供到组内另外两台、打标接入事务系统。

**出错时不会是“准备好了”**，而是一条写明原因、带下一步怎么做的提示，例如：

```text
启动失败：上一次演示还没收尾（demo 模式还在）
先执行：bash shardpg_demo.sh stop
```

想看它背后做了什么（含选主的 NOTICE 时间线），加 `VERBOSE=1`：
`ssh -t zhanhao@34.31.210.7 "VERBOSE=1 bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh start"`


然后在窗口 1 打开会话 A，**再开一个 PowerShell 窗口**打开会话 B：

```powershell
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh sql A"   # 窗口 A
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh sql B"   # 窗口 B
```

（把 `shardpg_demo.ps1` 拷到 Windows 上，也可以用 `.\shardpg_demo.ps1 start`、`.\shardpg_demo.ps1 sql A`、`.\shardpg_demo.ps1 sql B`。）
看到提示符 `A>` / `B>` 就可以开始输入下面的 SQL 了。`SELECT * FROM demo.help();` 列出全部演示函数。

### 全自动演示（不想自己敲的时候）

`shardpg_demo_auto.py` 会自己做完“准备 → 全部步骤 → 恢复环境”，你只看屏幕。每条 SQL 像有人在敲键盘一样**逐字打出来**（`A>` 是窗口 A，`B>` 是窗口 B，`--` 开头的是旁白），回车之后真的发给数据库执行，结果和 NOTICE 时间线实时显示；事务跨多条输入、B 被 A 的行锁挡住时“先别等它，回到 A 继续”，都和手工一样。

```powershell
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh auto"
```

| 选项 | 作用 |
|---|---|
| `--no-prepare` | 环境已经 `start` 过，直接播放 |
| `--keep` | 播完不 `stop`，留着环境给你继续手敲 |
| `--speed fast` / `normal` / `slow` | 打字速度（默认 normal） |
| `--pause 3` | 每条语句之间停 3 秒，给观众看结果 |
| `--from 4` | 从第 4 步开始播（前面几步的结果已经在环境里） |

PowerShell 里也可以：`.\shardpg_demo.ps1 auto`，传选项用 `-AutoArgs '--no-prepare --keep --speed slow'`。

## 四、演示步骤（全部是 SQL）

带 `NOTICE:` 的行是函数执行过程中**实时**打出来的时间线（毫秒是从本步开始计时），最后的表格是结果。

### 第 1 步：灌业务数据

数据在 demo_data.sql 里（500 个账户，分 10 批，每批一个全局事务），一条命令读进来。

慢是设计使然：打标表的每条记录都要 propose 给该分片的 Raft 组、等多数派落盘（约 2 行/秒）。

**窗口 A**（耗时 81.9 s）：

```sql
\i /tmp/demo_data.sql
```

```text
Timing is on.
BEGIN
Time: 1.765 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311297，start_ts = 18（本事务在所有分片上共用这一个快照）
(1 row)

Time: 74.617 ms
INSERT 0 50
Time: 448.734 ms
COMMIT
Time: 9298.797 ms (00:09.299)
BEGIN
Time: 0.155 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311298，start_ts = 24（本事务在所有分片上共用这一个快照）
(1 row)

Time: 3.466 ms
INSERT 0 50
Time: 22.263 ms
COMMIT
Time: 7007.509 ms (00:07.008)
BEGIN
Time: 0.161 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311299，start_ts = 30（本事务在所有分片上共用这一个快照）
(1 row)

Time: 2.926 ms
INSERT 0 50
Time: 175.348 ms
COMMIT
Time: 6776.823 ms (00:06.777)
BEGIN
Time: 0.141 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311300，start_ts = 36（本事务在所有分片上共用这一个快照）
(1 row)

Time: 2.404 ms
INSERT 0 50
Time: 32.815 ms
COMMIT
Time: 6648.478 ms (00:06.648)
BEGIN
Time: 0.119 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311301，start_ts = 42（本事务在所有分片上共用这一个快照）
(1 row)

Time: 0.534 ms
INSERT 0 50
Time: 7.825 ms
COMMIT
Time: 5560.538 ms (00:05.561)
BEGIN
Time: 0.242 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311302，start_ts = 48（本事务在所有分片上共用这一个快照）
(1 row)

Time: 0.691 ms
INSERT 0 50
Time: 26.948 ms
COMMIT
Time: 6753.183 ms (00:06.753)
BEGIN
Time: 0.082 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311303，start_ts = 54（本事务在所有分片上共用这一个快照）
(1 row)

Time: 0.483 ms
INSERT 0 50
Time: 166.682 ms
COMMIT
Time: 14505.251 ms (00:14.505)
BEGIN
Time: 0.099 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311304，start_ts = 60（本事务在所有分片上共用这一个快照）
(1 row)

Time: 5.755 ms
INSERT 0 50
Time: 14.598 ms
COMMIT
Time: 6295.428 ms (00:06.295)
BEGIN
Time: 0.140 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311305，start_ts = 66（本事务在所有分片上共用这一个快照）
(1 row)

Time: 6.031 ms
INSERT 0 50
Time: 176.688 ms
COMMIT
Time: 9237.148 ms (00:09.237)
BEGIN
Time: 0.097 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311306，start_ts = 72（本事务在所有分片上共用这一个快照）
(1 row)

Time: 0.532 ms
INSERT 0 50
Time: 11.745 ms
COMMIT
Time: 6809.092 ms (00:06.809)
Timing is off.
```

### 第 2 步：数据被自动分到了哪几台

建表时就是 4 个分片，Citus 按哈希把每个键落到其中一个；下面直接读各 worker 上的分片表。

**窗口 A**（耗时 5.7 s）：

```sql
SELECT * FROM demo.distribution('account');
```

```text
 分片 | 节点 | 端口 | 行数 | 余额合计 | 占比 
--------+--------+--------+--------+--------------+--------
 S1     | w1     |   5433 |    119 |       570219 | 23.8%
 S2     | w2     |   5434 |    138 |       717417 | 27.6%
 S3     | w3     |   5435 |    118 |       637989 | 23.6%
 S4     | w4     |   5436 |    125 |       604192 | 25.0%
(4 rows)
```

**窗口 A**：

```sql
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
 账户数 | 余额总和 
-----------+--------------
       500 |      2529817
(1 row)
```

### 第 3 步：各分区的副本信息

副本因子 3：每个分片 1 个主 + 2 个副本，放在按数据所在节点轮转的另外两台上（第四台不在这个组里，

每台 worker 只存 3/4 的分片，不是每台都存全部）。副本经 Raft 把主的分区流收下落盘，

但不马上 redo（惰性回放）：“已收到”是收下的日志位点，“已回放”是真正 redo 到哪，差值就是待回放。

**窗口 A**：

```sql
SELECT * FROM demo.replicas('account');
```

```text
 分片 | 主 | 副本 | 已收到 | 已回放 | 待回放 |    状态    
--------+-----+--------+-----------+-----------+-----------+--------------
 S1     | w1  | w2     |       302 |         0 |       302 | armed，idle
 S1     | w1  | w3     |       302 |         0 |       302 | armed，idle
 S2     | w2  | w3     |       339 |         0 |       339 | armed，idle
 S2     | w2  | w4     |       339 |         0 |       339 | armed，idle
 S3     | w3  | w1     |       301 |         0 |       301 | armed，idle
 S3     | w3  | w4     |       301 |         0 |       301 | armed，idle
 S4     | w4  | w1     |       314 |         0 |       314 | armed，idle
 S4     | w4  | w2     |       314 |         0 |       314 | armed，idle
(8 rows)
```

### 第 4 步：并发事务

幕 1：A、B 各开一个全局事务，各看一眼总额。

**窗口 A**（耗时 2.5 s）：

```sql
BEGIN;
SELECT demo.global_txn();
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311307，start_ts = 90（本事务在所有分片上共用这一个快照）
(1 row)

 账户数 | 余额总和 
-----------+--------------
       500 |      2529817
(1 row)
```

**窗口 B**（耗时 2.8 s）：

```sql
BEGIN;
SELECT demo.global_txn();
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311308，start_ts = 91（本事务在所有分片上共用这一个快照）
(1 row)

 账户数 | 余额总和 
-----------+--------------
       500 |      2529817
(1 row)
```

幕 2：A 做一笔跨分片转账（两个账户在不同节点上），钱从一边到另一边，总额不变。

**窗口 A**（耗时 2.9 s）：

```sql
UPDATE account SET balance = balance - 300 WHERE id = 1;
UPDATE account SET balance = balance + 300 WHERE id = 2;
SELECT id, owner, balance FROM account WHERE id IN (1, 2) ORDER BY id;
```

```text
UPDATE 1
UPDATE 1
 id |   owner    | balance 
----+------------+---------
  1 | 成都0001 |    3840
  2 | 杭州0002 |    6465
(2 rows)
```

幕 3：B 看不见 A 没提交的转账（快照隔离）。

**窗口 B**：

```sql
SELECT id, owner, balance FROM account WHERE id IN (1, 2) ORDER BY id;
```

```text
 id |   owner    | balance 
----+------------+---------
  1 | 成都0001 |    4140
  2 | 杭州0002 |    6165
(2 rows)
```

幕 4：A 提交；B 在自己的事务里仍然看不见（B 的 start_ts 早于 A 的 commit_ts）。

**窗口 A**：

```sql
COMMIT;
```

```text
COMMIT
```

**窗口 B**：

```sql
SELECT id, owner, balance FROM account WHERE id IN (1, 2) ORDER BY id;
```

```text
 id |   owner    | balance 
----+------------+---------
  1 | 成都0001 |    4140
  2 | 杭州0002 |    6165
(2 rows)
```

**窗口 B**：

```sql
COMMIT;
```

```text
COMMIT
```

幕 5：写写冲突 —— A 改 id=3 不提交，B 也去改它，B 被挡住。

**窗口 A**（耗时 2.2 s）：

```sql
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 7 WHERE id = 3;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311309，start_ts = 95（本事务在所有分片上共用这一个快照）
(1 row)

UPDATE 1
```

**窗口 B**（这条会被挡住，终端停在这里 —— 先别等它，切回窗口 A）：

```sql
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance - 7 WHERE id = 3;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311310，start_ts = 96（本事务在所有分片上共用这一个快照）
(1 row)
```

A 提交后 B 的等待结束，但它被中止：先提交者胜，B 只能重试。

**窗口 A**：

```sql
COMMIT;
```

```text
COMMIT
```

**窗口 B**（A 提交之后，B 刚才被挡住的那条语句返回了）：

```text
ERROR:  could not serialize access due to concurrent update
DETAIL:  分片 395412 目标行的并发删改 commit_ts=97 ≥ 本事务 start_ts=96（first-committer-wins，设计 §4.4）。
HINT:  重试事务。
CONTEXT:  while executing command on localhost:5434
```

**窗口 B**：

```sql
ROLLBACK;
```

```text
ROLLBACK
```

幕 6：总额守恒（跨分片转账 + 一笔被中止的写，钱一分没多一分没少）。

**窗口 A**：

```sql
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
 账户数 | 余额总和 
-----------+--------------
       500 |      2529824
(1 row)
```

### 第 5 步：手动切换 leader

把 S1 的主切到它组内的另一台：选主过程实时打出来，切完 master 的路由自动跟过去，

数据照读、总额不变。

**窗口 A**（耗时 10.7 s）：

```sql
SELECT * FROM demo.switch_leader('account', 'S1');
```

```text
NOTICE:  把 S1 的主从 w1 切到 w2（同组成员：w1 w2 w3）
NOTICE:  切换前：S1 的主 = w1，下一个分片 xid = 14
NOTICE:       7 ms  副本都已确认最新提交（各从的 Raft 提交位点 = 主的日志末尾）
NOTICE:      13 ms  S1  切换前：w1 leader/任期1，w2 follower/任期1，w3 follower/任期1，w4 无组；控制面登记 w1；master 把读写路由到 w1
NOTICE:      28 ms  S1  w2  pg_raft_group_campaign：对 S1 的组发起竞选
NOTICE:    1063 ms  S1  w2  发起竞选 → candidate（任期 2，向其余成员要票）
NOTICE:    1132 ms  S1  w1  退位 → follower，跟随 w2（任期 2）
NOTICE:    1132 ms  S1  w2  ★ 当选 leader（任期 2）
NOTICE:    1132 ms  S1  w3  follower：认 w2 为 leader（任期 2）
NOTICE:    3868 ms  S1  master  控制面登记（0 号组 partition_map）：主 = w2（登记任期 2）
NOTICE:    3868 ms  S1  master  Citus 路由表：master 把读写发往 w2 :5434
NOTICE:    4262 ms  master  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
NOTICE:    9896 ms  S1  w1  旧主被新主自动重新供给成副本（回放槽位 armed）
 分片 | 原来的主 | 新主 | 新任期 | 切换耗时_ms |                             旧主                             
--------+--------------+--------+-----------+-----------------+----------------------------------------------------------------
 S1     | w1           | w2     | 2         |            3932 | 是：切换后 9896 ms 被新主自动重新供给（armed）
(1 row)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |            S1             |            S2             |            S3             |            S4             |            小结             
----------+---------------------------+---------------------------+---------------------------+---------------------------+-------------------------------
 w1 :5433 | follower（回放到 0） | ·（不在组内）      | follower（回放到 0） | follower（回放到 0） | 0 个 leader + 3 个 follower
 w2 :5434 | ★ leader（任期2）   | ★ leader（任期1）   | ·（不在组内）      | follower（回放到 0） | 2 个 leader + 1 个 follower
 w3 :5435 | follower（回放到 0） | follower（回放到 0） | ★ leader（任期1）   | ·（不在组内）      | 1 个 leader + 2 个 follower
 w4 :5436 | ·（不在组内）      | follower（回放到 0） | follower（回放到 0） | ★ leader（任期1）   | 1 个 leader + 2 个 follower
(4 rows)
```

**窗口 A**：

```sql
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
 账户数 | 余额总和 
-----------+--------------
       500 |      2529824
(1 row)
```

### 第 6 步：不可抗力宕机

直接停掉 S1 当前的主（immediate：不做 checkpoint，等同断电）。

该分片的组超时、自发选主；没有这个分片数据的成员即使当选也会主动让位。

**窗口 A**（耗时 22.1 s）：

```sql
SELECT * FROM demo.crash('account');
```

```text
NOTICE:  停掉 w2（它是 S1 的主；它同时还是另外几个分片的从）
NOTICE:  宕机前：w2 是 S1、S2 的主；其余分片它只是从
NOTICE:      46 ms  副本都已确认最新提交（各从的 Raft 提交位点 = 主的日志末尾）
NOTICE:      62 ms  S1  宕机前：w1 follower/任期2，w2 leader/任期2，w3 follower/任期2，w4 无组；控制面登记 w2；master 把读写路由到 w2
NOTICE:      69 ms  S2  宕机前：w1 无组，w2 leader/任期1，w3 follower/任期1，w4 follower/任期1；控制面登记 w2；master 把读写路由到 w2
NOTICE:      74 ms  S3  宕机前：w1 follower/任期1，w2 无组，w3 leader/任期1，w4 follower/任期1；控制面登记 w3；master 把读写路由到 w3
NOTICE:      78 ms  S4  宕机前：w1 follower/任期1，w2 follower/任期1，w3 无组，w4 leader/任期1；控制面登记 w4；master 把读写路由到 w4
NOTICE:     779 ms  w2  pg_ctl -m immediate stop：进程直接退出，不做 checkpoint（模拟断电）
NOTICE:     779 ms  S1  w2  ✗ 连不上（节点宕机）
NOTICE:     779 ms  S2  w2  ✗ 连不上（节点宕机）
NOTICE:     779 ms  S3  w2  ✗ 连不上（节点宕机）
NOTICE:     779 ms  S4  w2  ✗ 连不上（节点宕机）
NOTICE:   17485 ms  S2  w3  发起竞选 → candidate（任期 2，向其余成员要票）
NOTICE:   17485 ms  S2  w4  follower（任期 2，还没认出 leader）
NOTICE:   17557 ms  S2  w3  ★ 当选 leader（任期 2）
NOTICE:   17557 ms  S2  w4  follower：认 w3 为 leader（任期 2）
NOTICE:   17804 ms  S1  w1  ★ 当选 leader（任期 3）
NOTICE:   17804 ms  S1  w3  follower：认 w1 为 leader（任期 3）
NOTICE:   20044 ms  S2  master  控制面登记（0 号组 partition_map）：主 = w3（登记任期 2）
NOTICE:   20044 ms  S2  master  Citus 路由表：master 把读写发往 w3 :5435
NOTICE:   20735 ms  S1  master  控制面登记（0 号组 partition_map）：主 = w1（登记任期 3）
NOTICE:   20735 ms  S1  master  Citus 路由表：master 把读写发往 w1 :5433
NOTICE:   21029 ms  master  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
 分片 | 宕机前的主 | 宕机后的主 | 任期 | 不可用时长_ms 
--------+-----------------+-----------------+--------+--------------------
 S1     | w2              | w1              | 3      |              20806
 S2     | w2              | w3              | 2      |              20806
 S3     | w3              | w3              | 1      |                  0
 S4     | w4              | w4              | 1      |                  0
(4 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |            S1             |            S2             |            S3             |            S4             |            小结             
----------+---------------------------+---------------------------+---------------------------+---------------------------+-------------------------------
 w1 :5433 | ★ leader（任期3）   | ·（不在组内）      | follower（回放到 0） | follower（回放到 0） | 1 个 leader + 2 个 follower
 w2 :5434 | ✗ 宕机                | ✗ 宕机                | ✗ 宕机                | ✗ 宕机                | 宕机
 w3 :5435 | follower（回放到 0） | ★ leader（任期2）   | ★ leader（任期1）   | ·（不在组内）      | 2 个 leader + 1 个 follower
 w4 :5436 | ·（不在组内）      | follower（回放到 0） | follower（回放到 0） | ★ leader（任期1）   | 1 个 leader + 2 个 follower
(4 rows)
```

宕机期间照常读写。

**窗口 B**：

```sql
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
 账户数 | 余额总和 
-----------+--------------
       500 |      2529824
(1 row)
```

**窗口 B**（耗时 4.0 s）：

```sql
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 1 WHERE id IN (1, 2, 3);
COMMIT;
```

```text
BEGIN
                                                  global_txn                                                  
--------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 311311，start_ts = 111（本事务在所有分片上共用这一个快照）
(1 row)

UPDATE 3
COMMIT
```

把节点拉起来：它以 follower 身份归队，原来当主的分片被新主自动重新供给成副本。

**窗口 A**（耗时 18.5 s）：

```sql
SELECT * FROM demo.recover('account');
```

```text
NOTICE:  把 w2 拉起来
NOTICE:       6 ms  S1  拉起前：w1 leader/任期3，w2 ✗宕机，w3 follower/任期3，w4 无组；控制面登记 w1；master 把读写路由到 w1
NOTICE:      10 ms  S2  拉起前：w1 无组，w2 ✗宕机，w3 leader/任期2，w4 follower/任期2；控制面登记 w3；master 把读写路由到 w3
NOTICE:      15 ms  S3  拉起前：w1 follower/任期1，w2 ✗宕机，w3 leader/任期1，w4 follower/任期1；控制面登记 w3；master 把读写路由到 w3
NOTICE:      20 ms  S4  拉起前：w1 follower/任期1，w2 ✗宕机，w3 无组，w4 leader/任期1；控制面登记 w4；master 把读写路由到 w4
NOTICE:     961 ms  w2  pg_ctl start：进程起来了，开始重新加入各个组
NOTICE:     962 ms  S1  w2  follower（任期 2，还没认出 leader）
NOTICE:     962 ms  S2  w2  follower（任期 0，还没认出 leader）
NOTICE:     962 ms  S3  w2  （本节点上还没有这个组）
NOTICE:     962 ms  S4  w2  follower（任期 0，还没认出 leader）
NOTICE:    1456 ms  S2  w2  follower（任期 1，还没认出 leader）
NOTICE:    1533 ms  S2  w2  follower：认 w3 为 leader（任期 2）
NOTICE:    1533 ms  S4  w2  follower（任期 1，还没认出 leader）
NOTICE:    1706 ms  S4  w2  follower：认 w4 为 leader（任期 1）
NOTICE:    1779 ms  S1  w2  follower：认 w1 为 leader（任期 3）
NOTICE:    2326 ms  master  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
NOTICE:    2487 ms  S1  w2  回放槽位 armed —— 已是 w1 的合格副本
NOTICE:    2506 ms  S4  w2  回放槽位 armed —— 已是 w4 的合格副本
NOTICE:   17561 ms  S2  w2  回放槽位 armed —— 已是 w3 的合格副本
 分片 | 当前的主 | 节点 | 在组里的角色 |    回放槽位     
--------+--------------+--------+--------------------+---------------------
 S1     | w1           | w2     | follower           | armed，回放到 0
 S2     | w3           | w2     | follower           | armed，回放到 0
 S3     | w3           | w2     | 不是成员       | —
 S4     | w4           | w2     | follower           | armed，回放到 0
(4 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |            S1             |            S2             |            S3             |            S4             |            小结             
----------+---------------------------+---------------------------+---------------------------+---------------------------+-------------------------------
 w1 :5433 | ★ leader（任期3）   | ·（不在组内）      | follower（回放到 0） | follower（回放到 0） | 1 个 leader + 2 个 follower
 w2 :5434 | follower（回放到 0） | follower（回放到 0） | ·（不在组内）      | follower（回放到 0） | 0 个 leader + 3 个 follower
 w3 :5435 | follower（回放到 0） | ★ leader（任期2）   | ★ leader（任期1）   | ·（不在组内）      | 2 个 leader + 1 个 follower
 w4 :5436 | ·（不在组内）      | follower（回放到 0） | follower（回放到 0） | ★ leader（任期1）   | 1 个 leader + 2 个 follower
(4 rows)
```

**窗口 A**（耗时 2.0 s）：

```sql
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
 账户数 | 余额总和 
-----------+--------------
       500 |      2529827
(1 row)
```

## 五、收尾：恢复演示前的环境

两个窗口里都输入 `\q` 退出 psql，然后在任一 PowerShell 窗口：

```powershell
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh stop"
```

```text
恢复演示前的环境
  删表 account
  删控制面登记（partition_map）里演示分片的行：4 个分片
  20 项参数已按演示前的 postgresql.auto.conf 还原（TSO、选举超时）
  demo 函数库已删除（master 与各 worker）
  清掉 5 个残留打标登记（pg_shard_xid）
完成：已净场（0 个数据 Raft 组、0 张演示表、0 张分片壳表）
```

`stop` 做的事：回滚残留的 prepared 事务 → 删演示表与各 worker 上的分片 / 副本壳表 → 拆掉数据 Raft 组 → 删控制面登记（partition_map）里演示分片的行 → 参数按演示前各节点 `postgresql.auto.conf` 原样还原（TSO、选举超时）→ 删 `demo` 函数库。本次实跑之后对 5 个节点做了环境快照比对（参数文件、扩展、模式、public 表与函数、数据组、控制面登记行数、prepared、容器 /tmp），与演示前**完全一致**。

## 六、演示时要如实说明的几点

1. **灌数据慢是设计使然，不是卡住。** 打标表的每条记录（堆记录 + 索引记录各一条）都要单独 propose 给该分片的
   Raft 组、等多数派落盘，一条一个同步往返 —— 实测约 **1.8–2.5 行/秒**。500 行 ≈ 5 分钟。
   要快就少灌点：`python3 gen_demo_data.py 200 demo_data.sql` 重新生成数据文件（约 2 分钟）。

2. **必须“先打标、再灌数据”。** 打标要求分片是空的（“已有 N 个堆块，不能登记为打标表”）：
   打标之前写进去的行带的是节点原生 xid，打标后按分片可见性规则读不到，等于让数据凭空消失。
   所以 `start` 在建表之后、灌数据之前就把打标做完了 —— 演示里灌数据的那一步不用再关心它。

3. **分片数在建表时定死，之后不能自己分裂。** `citus_split_shard_by_split_points`、`alter_distributed_table`、
   `rebalance_table_shards`、`citus_move_shard_placement` 都在产品的集群级禁用清单里（它们会亲手搬分片数据，
   绕过物理回放与 raft 放置）。所以演示是"建表时就 4 分片，数据按哈希自动落到 4 台"，这也是真实 Citus 部署的做法。

4. **`start` 里的选主要等几十秒。** 组刚建好时数据只在一台上：建组顺序是"数据所在那台先建组当选、另外两台再入组"，
   另外两台在入组之前不会主动竞选；已知自己没有数据的成员也不再竞选（P7-N57）。`start` 的“约 2–5 分钟”主要花在选主等待与供副本上。

5. **副本因子 3，不等于 worker 数。** 建组走产品函数 `raft_replicate_table_shards_bulk(表, 2)`（2 个副本）：
   数据所在那台先当选、副本再入组、从**当前**组主逐个供副本（组主中途换了也跟着走）。
   早先演示让每台 worker 都进组，是因为自写的建组流程在"组建好、副本还没供"那一刻凑不齐多数派；产品函数没有这个问题。

6. **TSO 租约演示期间放宽到 5 分钟**（`TSO_LEASE_MS`）。默认 60 s 时，几十秒的灌数批次会撞上
   "分片快照被栅栏作废：本节点已 45000 ms 未能向 TSO 续租"，整批白做。

7. **选举超时演示期间放宽到 15 s**（`ELECTION_MS=6000` 可改回产品默认）。这台机器只有 2 个 vCPU，
   共识 tick 偶尔一轮要卡 5–9 s，6 s 的超时会误判主死、冒出多余的选举。代价是宕机后要二三十秒才切完。

8. **别在演示中途重启 master。** TSO 计数器在 master 内存里，重启后会留下 boot 标记并拒绝发号
   （fail-closed 防呆），要按 `start` 的步骤重开。

9. **加节点不是一条命令。** 新 worker 除了 `citus_add_node`，还要：把它写进所有节点的 `pg_raft.peers`
   （PGC_POSTMASTER，要整簇重启）、在它上面建好扩展、并手工往 `partdist.node_map` 补一行真实地址
   （`OP_NODE_STATUS` 的 apply 只会写 `node<id>`/5432 这样的占位地址）。

11. **演示窗口别留着没结束的事务再去 `stop`。** 窗口里 `BEGIN` 之后没有 `COMMIT` / `ROLLBACK`，会一直持着演示表的锁，
    `DROP TABLE` 就卡到超时。`stop` 现在会先清掉持锁的会话和闲置超过 30 秒的 `idle in transaction` 会话，
    但演示结束前养成先 `\q`（或 `ROLLBACK;`）的习惯更稳。全自动演示不存在这个问题（它自己会关会话）。


## 七、演示函数一览

```text
函数              |                                           作用                                           
---------------------------------+--------------------------------------------------------------------------------------------
 demo.nodes()                    | 各节点：端口、类型、pg_raft 节点号、在线状态、控制面 0 号组角色
 demo.shards('表')              | 每个分片落在哪个节点、哈希范围、行数
 demo.locate('表', 键)         | 某个键落在哪个分片、当前由哪个节点服务
 demo.raft_elect('表')          | 每个分片建 Raft 组并选主（实时打出选主过程）
 demo.raft_replicas('表')       | 在各组的主上把副本供到其余两台 → 最终主从
 demo.raft_groups('表')         | 每台 worker 在各组里的角色、任期、日志位点
 demo.roles('表')               | 同一节点上的混合角色（节点 × 分片）
 demo.routing('表')             | 路由三层：master 的 Citus 路由表 / 控制面登记 / 节点本地角色
 demo.flow('表')                | 流控：Raft 日志环 + 分区流捕获环
 demo.global_txn()               | 在 BEGIN 之后调用：加入全局事务（跨分片写必须）
 demo.xid('表')                 | 分片级 xid 分配器：每个分片的下一个号、水位、与原生 xid 对比
 demo.clog('表', 'S1')          | 分片级 clog：每个分片 xid 的判决、start_ts、commit_ts、写了哪些行
 demo.versions('表', 'S1')      | 页面上的多版本：每个元组版本的 xmin/xmax 与判决
 demo.replay('表')              | 惰性回放：从已收到 vs 已回放
 demo.replicas('表')            | 各分区的副本信息：主、副本、已收到/已回放/待回放
 demo.distribution('表')        | 各分片的数据量（行数、余额、占比），直接读各 worker 上的分片表
 demo.setup('表')               | 一条命令：建组（副本因子 3）→ 选主 → 供副本 → 打标（start 已替你做好）
 demo.catchup('表')             | 触发回放，让每个从追平
 demo.compare('表')             | 副本与主逐字节比对
 demo.switch_leader('表', 'S1') | 手动切换 leader（同组内挑一台；实时打出切主过程）
 demo.crash('表')               | 模拟宕机（immediate stop），看 Raft 自动选主
 demo.recover('表')             | 拉起宕机节点，看它归队、被自动重新供给
(22 rows)
```

