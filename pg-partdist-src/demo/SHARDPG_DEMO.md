# ShardPG 演示教程：5 个节点上的一张业务表（只输入 SQL）

> 本教程每一步都在 **2026-09-23** 的 `pg-test-container`（1 master + 4 worker，共 5 个节点）上**完整实跑过一遍**，下面每段 SQL 之后的输出就是那次实跑的原样输出（从建表到恢复环境共用时约 9 分 56 秒）。你的分片号、时间戳、事务号会不同，其余应当一致。

## 一、你的要求（梳理与润色）

在 Windows 11 PowerShell 终端里做一场 ShardPG 的现场演示，按**真实业务的顺序**走，只看功能体现、不铺细节：

1. **规模**：5 个节点 —— 1 个 master（只做路由 + TSO）+ 4 个 worker。
2. **建表**：建一张业务表，系统自动把它分成 4 个分片、一台 worker 一片；应用始终只面对一张 `account` 表。
3. **接入**：每个分片自动建一个 Raft 组并**自发选主**、供副本、接入事务系统（分片级 xid + 分片级 clog + TSO）。
4. **灌数据**：业务数据放在文件里，演示时一条 `\i` 读进去；灌完看系统自己把数据分到了哪几台。
5. **并发事务**：两个窗口同时做跨分片的读写 —— 快照隔离、写写冲突、总额守恒。
6. **切主**：手动把某个分片的主切走，看 master 的路由自动跟过去、数据照读。
7. **宕机**：停掉一台（等同断电），看 Raft 自发选出新主、期间照常读写，再把它拉回来自动归队。

> 要看更细的东西（流控、分片 xid 分配器与 clog、页面多版本、惰性回放、副本逐字节比对），
> 用上一版的 14 步脚本 `shardpg_demo_steps_full.sql`。

## 二、文件（都在 `~/shardpg-test-work/pg-partdist-src/demo/`，只用于演示）

| 文件 | 作用 |
|---|---|
| `shardpg_demo_steps.sql` | **演示脚本**：只有 SQL 和事务，按步骤排好，`-- @A` / `-- @B` 标明在哪个窗口输入 |
| `shardpg_demo_functions.sql` | 封装好的演示函数库（`demo.*`），只装在 master 上；演示结束整体删除 |
| `shardpg_demo.sh` | 服务器上的启动器：`start` 准备环境 / `sql A`、`sql B` 打开会话 / `stop` 恢复环境 |
| `shardpg_demo.ps1` | Windows 端的一键入口（可选）：`.\shardpg_demo.ps1 start` / `sql A` / `sql B` / `stop` |
| `demo_data.sql` | 业务数据（500 个账户，分 10 批，每批一个全局事务），演示里 `\i /tmp/demo_data.sql` 读它 |
| `gen_demo_data.py` | 重新生成上面这份数据：`python3 gen_demo_data.py <行数> demo_data.sql` |
| `shardpg_demo_steps_full.sql` | 上一版的 14 步详细脚本（流控、分片 xid/clog、惰性回放、逐字节比对），要看细节时用 |
| `SHARDPG_DEMO.md` | 本教程 |

`start` 对集群做的临时改动只有：在 master 上建 `demo` 模式（函数库 + dblink）、打开 TSO（会重启一次 master）、把 4 台 worker 的 Raft 选举超时从 6 s 放宽到 15 s、TSO 租约从 60 s 放宽到 5 分钟（见“已知问题”第 6、7 条），并把 `demo_data.sql` 拷进容器的 /tmp。选主本身**不做任何干预**：建完组就让它们自发竞选。`stop` 把它们全部撤掉，并按演示前各节点 `postgresql.auto.conf` 的原样还原参数；实测还原后与演示前的环境快照逐项一致。

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
准备好了
```

成功就只有这一行（约十几秒）。**出错时不会是这一行**，而是一条写明原因、带下一步怎么做的提示，例如：

```text
启动失败：上一次演示还没收尾（demo 模式还在）
先执行：bash shardpg_demo.sh stop
```

想看它在背后做了什么（装函数库、记参数原样、开 TSO、放宽选举超时），加 `VERBOSE=1`：
`ssh -t zhanhao@34.31.210.7 "VERBOSE=1 bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh start"`

然后在窗口 1 打开会话 A，**再开一个 PowerShell 窗口**打开会话 B：

```powershell
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh sql A"   # 窗口 A
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh sql B"   # 窗口 B
```

（把 `shardpg_demo.ps1` 拷到 Windows 上，也可以用 `.\shardpg_demo.ps1 start`、`.\shardpg_demo.ps1 sql A`、`.\shardpg_demo.ps1 sql B`。）
看到提示符 `A>` / `B>` 就可以开始输入下面的 SQL 了。`SELECT * FROM demo.help();` 列出全部演示函数。

## 四、演示步骤（全部是 SQL）

带 `NOTICE:` 的行是函数执行过程中**实时**打出来的时间线（毫秒是从本步开始计时），最后的表格是结果。

### 第 1 步：集群

1 个 master + 4 个 worker。控制面是 0 号 Raft 组（5 个节点都在里面），

它登记"每个分片的主是谁"，master 的路由就按这份登记走。

**窗口 A**：

```sql
SELECT * FROM demo.nodes();
```

```text
 节点 | 端口 | 类型 | raft节点号 | 状态 |     控制面0号组     
--------+--------+--------+---------------+--------+--------------------------
 master |   5432 | master |             1 | 在线 | follower（任期 696）
 w1     |   5433 | worker |             2 | 在线 | follower（任期 696）
 w2     |   5434 | worker |             3 | 在线 | leader（任期 696）
 w3     |   5435 | worker |             4 | 在线 | follower（任期 696）
 w4     |   5436 | worker |             5 | 在线 | follower（任期 696）
(5 rows)
```

### 第 2 步：建一张业务表，并把它接入系统

建表就是 4 个分片，Citus 按哈希把它们分到 4 台 worker —— 应用只面对一张 account 表。

然后一条命令完成三件事：每个分片建一个 Raft 组（成员 = 全部 4 台 worker）→ 自发选主 → 供副本 →

打标接入事务系统（分片级 xid + 分片级 clog + TSO 时间戳）。

打标必须在写入之前做：分片得是空的。

**窗口 A**：

```sql
SET citus.shard_count = 4;
CREATE TABLE account (id int PRIMARY KEY, owner text NOT NULL, balance int NOT NULL DEFAULT 0)
    WITH (autovacuum_enabled = off);
SELECT create_distributed_table('account', 'id', colocate_with => 'none');
```

```text
SET
CREATE TABLE
 create_distributed_table 
--------------------------
 
(1 row)
```

**窗口 A**（耗时 198.0 s）：

```sql
SELECT * FROM demo.setup('account');
```

```text
NOTICE:  ① 每个分片建一个 Raft 组（成员 = 4 台 worker），然后**不干预**，等它们自发选主
NOTICE:     选主完成（78 s）：S1→w1（任期 3），S2→w2（任期 3），S3→w3（任期 1），S4→w4（任期 2）
NOTICE:  ② 在每个组的主上把副本供到组内其余成员（物理基线进分区流）
NOTICE:  S1 在 w2 上的回放槽位没 armed，从当前主 w1 补供
NOTICE:  S2 在 w1 上的回放槽位没 armed，从当前主 w2 补供
NOTICE:  S3 在 w1 上的回放槽位没 armed，从当前主 w3 补供
NOTICE:  S4 在 w1 上的回放槽位没 armed，从当前主 w4 补供
NOTICE:  ③ 打标：接入分片级 xid + 分片级 clog + TSO 时间戳（必须在写入之前做，分片得是空的）
NOTICE:     全部就位，用时 197 s
 分片 | 数据在 | leader |   副本   | 选举轮次 | 事务系统 
--------+-----------+--------+------------+--------------+--------------
 S1     | w1        | w1     | w2, w3, w4 |            3 | 已接入
 S2     | w2        | w2     | w1, w3, w4 |            3 | 已接入
 S3     | w3        | w3     | w1, w2, w4 |            1 | 已接入
 S4     | w4        | w4     | w1, w2, w3 |            2 | 已接入
(4 rows)
```

### 第 3 步：灌业务数据，看系统自己把它分到哪

数据在 demo_data.sql 里（500 个账户，分 10 批，每批一个全局事务）。

慢是设计使然：打标表的每条记录都要 propose 给该分片的 Raft 组、等多数派落盘。

**窗口 A**（耗时 290.2 s）：

```sql
\i /tmp/demo_data.sql
```

```text
Timing is on.
BEGIN
Time: 0.603 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274433，start_ts = 22（本事务在所有分片上共用这一个快照）
(1 row)

Time: 48.775 ms
INSERT 0 50
Time: 245.876 ms
COMMIT
Time: 27717.286 ms (00:27.717)
BEGIN
Time: 0.257 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274434，start_ts = 28（本事务在所有分片上共用这一个快照）
(1 row)

Time: 0.913 ms
INSERT 0 50
Time: 18.012 ms
COMMIT
Time: 23264.222 ms (00:23.264)
BEGIN
Time: 0.222 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274435，start_ts = 34（本事务在所有分片上共用这一个快照）
(1 row)

Time: 0.905 ms
INSERT 0 50
Time: 10.193 ms
COMMIT
Time: 25693.328 ms (00:25.693)
BEGIN
Time: 0.252 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274436，start_ts = 40（本事务在所有分片上共用这一个快照）
(1 row)

Time: 1.964 ms
INSERT 0 50
Time: 17.949 ms
COMMIT
Time: 33997.751 ms (00:33.998)
BEGIN
Time: 0.242 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274437，start_ts = 46（本事务在所有分片上共用这一个快照）
(1 row)

Time: 1.202 ms
INSERT 0 50
Time: 11.125 ms
COMMIT
Time: 26931.812 ms (00:26.932)
BEGIN
Time: 0.173 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274438，start_ts = 52（本事务在所有分片上共用这一个快照）
(1 row)

Time: 1.496 ms
INSERT 0 50
Time: 19.967 ms
COMMIT
Time: 24213.617 ms (00:24.214)
BEGIN
Time: 0.221 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274439，start_ts = 58（本事务在所有分片上共用这一个快照）
(1 row)

Time: 0.722 ms
INSERT 0 50
Time: 8.648 ms
COMMIT
Time: 26557.762 ms (00:26.558)
BEGIN
Time: 0.510 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274440，start_ts = 64（本事务在所有分片上共用这一个快照）
(1 row)

Time: 1.409 ms
INSERT 0 50
Time: 12.632 ms
COMMIT
Time: 35443.660 ms (00:35.444)
BEGIN
Time: 0.166 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274441，start_ts = 70（本事务在所有分片上共用这一个快照）
(1 row)

Time: 10.829 ms
INSERT 0 50
Time: 31.544 ms
COMMIT
Time: 35969.692 ms (00:35.970)
BEGIN
Time: 0.412 ms
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274442，start_ts = 76（本事务在所有分片上共用这一个快照）
(1 row)

Time: 1.278 ms
INSERT 0 50
Time: 21.821 ms
COMMIT
Time: 29785.621 ms (00:29.786)
Timing is off.
```

**窗口 A**（耗时 6.3 s）：

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

### 第 4 步：并发事务

幕 1：A、B 各开一个全局事务，各看一眼总额。

**窗口 A**：

```sql
BEGIN;
SELECT demo.global_txn();
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274443，start_ts = 94（本事务在所有分片上共用这一个快照）
(1 row)

 账户数 | 余额总和 
-----------+--------------
       500 |      2529817
(1 row)
```

**窗口 B**（耗时 2.1 s）：

```sql
BEGIN;
SELECT demo.global_txn();
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274444，start_ts = 95（本事务在所有分片上共用这一个快照）
(1 row)

 账户数 | 余额总和 
-----------+--------------
       500 |      2529817
(1 row)
```

幕 2：A 做一笔跨分片转账（两个账户在不同节点上），钱从一边到另一边，总额不变。

**窗口 A**：

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

**窗口 A**（耗时 4.9 s）：

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

**窗口 A**：

```sql
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 7 WHERE id = 3;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274445，start_ts = 99（本事务在所有分片上共用这一个快照）
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
--------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 274446，start_ts = 100（本事务在所有分片上共用这一个快照）
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
DETAIL:  分片 293403 目标行的并发删改 commit_ts=101 ≥ 本事务 start_ts=100（first-committer-wins，设计 §4.4）。
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

**窗口 A**（耗时 25.7 s）：

```sql
SELECT * FROM demo.switch_leader('account', 'S1');
```

```text
NOTICE:  把 S1 的主从 w1 切到 w2（同组成员：w1 w2 w3 w4）
NOTICE:  切换前：S1 的主 = w1，下一个分片 xid = 14
NOTICE:      21 ms  副本都已确认最新提交（各从的 Raft 提交位点 = 主的日志末尾）
NOTICE:      58 ms  S1  切换前：w1 leader/任期3，w2 follower/任期3，w3 follower/任期3，w4 follower/任期3；控制面登记 w1；master 把读写路由到 w1
NOTICE:     220 ms  S1  w2  pg_raft_group_campaign：对 S1 的组发起竞选
NOTICE:    2476 ms  S1  w1  退位 → follower，跟随 w2（任期 4）
NOTICE:    2476 ms  S1  w2  ★ 当选 leader（任期 4）
NOTICE:    2476 ms  S1  w3  follower：认 w2 为 leader（任期 4）
NOTICE:    2476 ms  S1  w4  follower：认 w2 为 leader（任期 4）
NOTICE:   15516 ms  S1  master  控制面登记（0 号组 partition_map）：主 = w2（登记任期 4）
NOTICE:   15516 ms  S1  master  Citus 路由表：master 把读写发往 w2 :5434
NOTICE:   17224 ms  master  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
NOTICE:   25460 ms  S1  w1  旧主被新主自动重新供给成副本（回放槽位 armed）
 分片 | 原来的主 | 新主 | 新任期 | 切换耗时_ms |                             旧主                              
--------+--------------+--------+-----------+-----------------+-----------------------------------------------------------------
 S1     | w1           | w2     | 4         |           15711 | 是：切换后 25460 ms 被新主自动重新供给（armed）
(1 row)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |            S1             |            S2             |            S3             |            小结             
----------+---------------------------+---------------------------+---------------------------+-------------------------------
 w1 :5433 | follower（回放到 0） | follower（回放到 0） | follower（回放到 0） | 0 个 leader + 4 个 follower
 w2 :5434 | ★ leader（任期4）   | ★ leader（任期3）   | follower（回放到 0） | 2 个 leader + 2 个 follower
 w3 :5435 | follower（回放到 0） | follower（回放到 0） | ★ leader（任期1）   | 1 个 leader + 3 个 follower
 w4 :5436 | follower（回放到 0） | follower（回放到 0） | follower（回放到 0） | 1 个 leader + 3 个 follower
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

**窗口 A**（耗时 34.7 s）：

```sql
SELECT * FROM demo.crash('account');
```

```text
NOTICE:  停掉 w2（它是 S1 的主；它同时还是另外几个分片的从）
NOTICE:  宕机前：w2 是 S1、S2 的主；其余分片它只是从
NOTICE:    2251 ms  副本都已确认最新提交（各从的 Raft 提交位点 = 主的日志末尾）
NOTICE:    2262 ms  S1  宕机前：w1 follower/任期4，w2 leader/任期4，w3 follower/任期4，w4 follower/任期4；控制面登记 w2；master 把读写路由到 w2
NOTICE:    2272 ms  S2  宕机前：w1 follower/任期3，w2 leader/任期3，w3 follower/任期3，w4 follower/任期3；控制面登记 w2；master 把读写路由到 w2
NOTICE:    2280 ms  S3  宕机前：w1 follower/任期1，w2 follower/任期1，w3 leader/任期1，w4 follower/任期1；控制面登记 w3；master 把读写路由到 w3
NOTICE:    2287 ms  S4  宕机前：w1 follower/任期2，w2 follower/任期2，w3 follower/任期2，w4 leader/任期2；控制面登记 w4；master 把读写路由到 w4
NOTICE:    3554 ms  w2  pg_ctl -m immediate stop：进程直接退出，不做 checkpoint（模拟断电）
NOTICE:    3554 ms  S1  w2  ✗ 连不上（节点宕机）
NOTICE:    3554 ms  S2  w2  ✗ 连不上（节点宕机）
NOTICE:    3554 ms  S3  w2  ✗ 连不上（节点宕机）
NOTICE:    3554 ms  S4  w2  ✗ 连不上（节点宕机）
NOTICE:   20308 ms  S1  w1  follower（任期 5，还没认出 leader）
NOTICE:   20308 ms  S1  w3  follower（任期 5，还没认出 leader）
NOTICE:   20308 ms  S1  w4  ★ 当选 leader（任期 5）
NOTICE:   20568 ms  S1  w1  follower：认 w4 为 leader（任期 5）
NOTICE:   20568 ms  S1  w3  follower：认 w4 为 leader（任期 5）
NOTICE:   23995 ms  S2  w1  follower：认 w3 为 leader（任期 4）
NOTICE:   23995 ms  S2  w3  ★ 当选 leader（任期 4）
NOTICE:   23995 ms  S2  w4  follower：认 w3 为 leader（任期 4）
NOTICE:   30576 ms  S1  master  控制面登记（0 号组 partition_map）：主 = w4（登记任期 5）
NOTICE:   30576 ms  S1  master  Citus 路由表：master 把读写发往 w4 :5436
NOTICE:   32745 ms  S2  master  控制面登记（0 号组 partition_map）：主 = w3（登记任期 4）
NOTICE:   32745 ms  S2  master  Citus 路由表：master 把读写发往 w3 :5435
NOTICE:   34309 ms  master  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
 分片 | 宕机前的主 | 宕机后的主 | 任期 | 不可用时长_ms 
--------+-----------------+-----------------+--------+--------------------
 S1     | w2              | w4              | 5      |              32812
 S2     | w2              | w3              | 4      |              32812
 S3     | w3              | w3              | 1      |                  0
 S4     | w4              | w4              | 2      |                  0
(4 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |            S1             |            S2             |            S3             |            小结             
----------+---------------------------+---------------------------+---------------------------+-------------------------------
 w1 :5433 | follower（回放到 0） | follower（回放到 0） | follower（回放到 0） | 0 个 leader + 4 个 follower
 w2 :5434 | ✗ 宕机                | ✗ 宕机                | ✗ 宕机                | 宕机
 w3 :5435 | follower（回放到 0） | ★ leader（任期4）   | ★ leader（任期1）   | 2 个 leader + 2 个 follower
 w4 :5436 | ★ leader（任期5）   | follower（回放到 0） | follower（回放到 0） | 2 个 leader + 2 个 follower
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

**窗口 B**（耗时 4.6 s）：

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
 已加入全局事务：gxid = 274447，start_ts = 115（本事务在所有分片上共用这一个快照）
(1 row)

UPDATE 3
COMMIT
```

把节点拉起来：它以 follower 身份归队，原来当主的分片被新主自动重新供给成副本。

**窗口 A**（耗时 16.5 s）：

```sql
SELECT * FROM demo.recover('account');
```

```text
NOTICE:  把 w2 拉起来
NOTICE:      11 ms  S1  拉起前：w1 follower/任期5，w2 ✗宕机，w3 follower/任期5，w4 leader/任期5；控制面登记 w4；master 把读写路由到 w4
NOTICE:      16 ms  S2  拉起前：w1 follower/任期4，w2 ✗宕机，w3 leader/任期4，w4 follower/任期4；控制面登记 w3；master 把读写路由到 w3
NOTICE:      20 ms  S3  拉起前：w1 follower/任期1，w2 ✗宕机，w3 leader/任期1，w4 follower/任期1；控制面登记 w3；master 把读写路由到 w3
NOTICE:      23 ms  S4  拉起前：w1 follower/任期2，w2 ✗宕机，w3 follower/任期2，w4 leader/任期2；控制面登记 w4；master 把读写路由到 w4
NOTICE:    1005 ms  w2  pg_ctl start：进程起来了，开始重新加入各个组
NOTICE:    1005 ms  S1  w2  follower（任期 0，还没认出 leader）
NOTICE:    1005 ms  S2  w2  follower（任期 3，还没认出 leader）
NOTICE:    1005 ms  S3  w2  follower（任期 0，还没认出 leader）
NOTICE:    1005 ms  S4  w2  follower（任期 0，还没认出 leader）
NOTICE:    1487 ms  S1  w2  follower：认 w4 为 leader（任期 5）
NOTICE:    1487 ms  S2  w2  follower：认 w3 为 leader（任期 4）
NOTICE:    1487 ms  S3  w2  follower（任期 1，还没认出 leader）
NOTICE:    1698 ms  S3  w2  follower：认 w3 为 leader（任期 1）
NOTICE:    1698 ms  S4  w2  follower：认 w4 为 leader（任期 2）
NOTICE:    2514 ms  master  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
NOTICE:    2664 ms  S1  w2  回放槽位 armed —— 已是 w4 的合格副本
NOTICE:    2687 ms  S3  w2  回放槽位 armed —— 已是 w3 的合格副本
NOTICE:    2698 ms  S4  w2  回放槽位 armed —— 已是 w4 的合格副本
NOTICE:   16417 ms  S2  w2  回放槽位 armed —— 已是 w3 的合格副本
 分片 | 当前的主 | 节点 | 在组里的角色 |    回放槽位     
--------+--------------+--------+--------------------+---------------------
 S1     | w4           | w2     | follower           | armed，回放到 0
 S2     | w3           | w2     | follower           | armed，回放到 0
 S3     | w3           | w2     | follower           | armed，回放到 0
 S4     | w4           | w2     | follower           | armed，回放到 0
(4 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |            S1             |            S2             |            S3             |            小结             
----------+---------------------------+---------------------------+---------------------------+-------------------------------
 w1 :5433 | follower（回放到 0） | follower（回放到 0） | follower（回放到 0） | 0 个 leader + 4 个 follower
 w2 :5434 | follower（回放到 0） | follower（回放到 0） | follower（回放到 0） | 0 个 leader + 4 个 follower
 w3 :5435 | follower（回放到 0） | ★ leader（任期4）   | ★ leader（任期1）   | 2 个 leader + 2 个 follower
 w4 :5436 | ★ leader（任期5）   | follower（回放到 0） | follower（回放到 0） | 2 个 leader + 2 个 follower
(4 rows)
```

**窗口 A**（耗时 2.3 s）：

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
  删表 account（分片 103039 103040 103041 103042）
  删控制面登记（partition_map）里演示分片的行：103039,103040,103041,103042
  20 项参数已按演示前的 postgresql.auto.conf 还原（TSO、选举超时）
  demo 函数库已删除（master 与各 worker）
完成：残留数据 Raft 组 0 个，残留演示表 0 张
```

`stop` 做的事：回滚残留的 prepared 事务 → 删演示表与各 worker 上的分片 / 副本壳表 → 拆掉数据 Raft 组 → 删控制面登记（partition_map）里演示分片的行 → 参数按演示前各节点 `postgresql.auto.conf` 原样还原（TSO、选举超时）→ 删 `demo` 函数库。本次实跑之后对 5 个节点做了环境快照比对（参数文件、扩展、模式、public 表与函数、数据组、控制面登记行数、prepared、容器 /tmp），与演示前**完全一致**。

## 六、演示时要如实说明的几点

1. **灌数据慢是设计使然，不是卡住。** 打标表的每条记录（堆记录 + 索引记录各一条）都要单独 propose 给该分片的
   Raft 组、等多数派落盘，一条一个同步往返 —— 实测约 **1.8–2.5 行/秒**。500 行 ≈ 5 分钟。
   要快就少灌点：`python3 gen_demo_data.py 200 demo_data.sql` 重新生成数据文件（约 2 分钟）。

2. **必须"先打标、再灌数据"。** 打标要求分片是空的（"已有 N 个堆块，不能登记为打标表"）：
   打标之前写进去的行带的是节点原生 xid，打标后按分片可见性规则读不到，等于让数据凭空消失。

3. **分片数在建表时定死，之后不能自己分裂。** `citus_split_shard_by_split_points`、`alter_distributed_table`、
   `rebalance_table_shards`、`citus_move_shard_placement` 都在产品的集群级禁用清单里（它们会亲手搬分片数据，
   绕过物理回放与 raft 放置）。所以演示是"建表时就 4 分片，数据按哈希自动落到 4 台"，这也是真实 Citus 部署的做法。

4. **自发选主要等几十秒到两三分钟。** 组刚建好时数据只在一台上，另外几台是空的；Raft 不知道谁有数据，
   空的那台照样可能先当选 —— 它做升主前置时发现自己没有这个分片的副本，于是拒绝升主、主动让位并退避
   5 个选举周期，直到有数据的那台当选。结果表里的"选举轮次"就是看这个的（1 = 一次选中）。

5. **组成员 = 全部 worker（副本因子等于节点数）。** 试过把副本因子调成 3，会在供副本时踩
   `provision_shard_replica: 建壳表失败: record 1 未达多数派` —— 组建好、副本还没供的那一刻，
   组内没有任何一台能落盘流里最早那条记录。演示因此让每台 worker 都进组。

6. **TSO 租约演示期间放宽到 5 分钟**（`TSO_LEASE_MS`）。默认 60 s 时，几十秒的灌数批次会撞上
   "分片快照被栅栏作废：本节点已 45000 ms 未能向 TSO 续租"，整批白做。

7. **选举超时演示期间放宽到 15 s**（`ELECTION_MS=6000` 可改回产品默认）。这台机器只有 2 个 vCPU，
   共识 tick 偶尔一轮要卡 5–9 s，6 s 的超时会误判主死、冒出多余的选举。代价是宕机后要二三十秒才切完。

8. **别在演示中途重启 master。** TSO 计数器在 master 内存里，重启后会留下 boot 标记并拒绝发号
   （fail-closed 防呆），要按 `start` 的步骤重开。

9. **加节点不是一条命令。** 新 worker 除了 `citus_add_node`，还要：把它写进所有节点的 `pg_raft.peers`
   （PGC_POSTMASTER，要整簇重启）、在它上面建好扩展、并手工往 `partdist.node_map` 补一行真实地址
   （`OP_NODE_STATUS` 的 apply 只会写 `node<id>`/5432 这样的占位地址）。


## 七、演示函数一览

```text
函数                 |                                          作用                                           
---------------------------------------+-------------------------------------------------------------------------------------------
 demo.nodes()                          | 各节点：端口、类型、pg_raft 节点号、在线状态、控制面 0 号组角色
 demo.shards('表')                    | 每个分片落在哪个节点、哈希范围、行数
 demo.locate('表', 键)               | 某个键落在哪个分片、当前由哪个节点服务
 demo.raft_elect('表')                | 每个分片建 Raft 组并选主（实时打出选主过程）
 demo.raft_replicas('表')             | 在各组的主上把副本供到其余两台 → 最终主从
 demo.raft_groups('表')               | 每台 worker 在各组里的角色、任期、日志位点
 demo.roles('表')                     | 同一节点上的混合角色（节点 × 分片）
 demo.routing('表')                   | 路由三层：master 的 Citus 路由表 / 控制面登记 / 节点本地角色
 demo.flow('表')                      | 流控：Raft 日志环 + 分区流捕获环
 demo.global_txn()                     | 在 BEGIN 之后调用：加入全局事务（跨分片写必须）
 demo.xid('表')                       | 分片级 xid 分配器：每个分片的下一个号、水位、与原生 xid 对比
 demo.clog('表', 'S1')                | 分片级 clog：每个分片 xid 的判决、start_ts、commit_ts、写了哪些行
 demo.versions('表', 'S1')            | 页面上的多版本：每个元组版本的 xmin/xmax 与判决
 demo.replay('表')                    | 惰性回放：从已收到 vs 已回放
 demo.catchup('表')                   | 触发回放，让每个从追平
 demo.compare('表')                   | 副本与主逐字节比对
 demo.switch_leader('表', 'S1', 'w2') | 手动切换 leader（实时打出切主过程）
 demo.crash('表')                     | 模拟宕机（immediate stop），看 Raft 自动选主
 demo.recover('表')                   | 拉起宕机节点，看它归队、被自动重新供给
(19 rows)
```

