# ShardPG 演示教程：4 个节点上的分片、Raft 与事务（只输入 SQL）

> 本教程每一步都在 **2026-09-19** 的 `pg-test-container`（1 协调者 + 3 worker，共 4 个节点）上**完整实跑过一遍**，下面每段 SQL 之后的输出就是那次实跑的原样输出（从建表到恢复环境共用时约 2 分 38 秒）。你的分片号、时间戳、事务号会不同，其余应当一致。

## 一、你的要求（梳理与润色）

在 Windows 11 PowerShell 终端里做一场 ShardPG 的现场演示：

1. **运行形态**：在 PowerShell 里输入命令，结果直接显示在终端上。
2. **规模**：整场只用 4 个节点 —— 1 个协调者 + 3 个 worker。
3. **数据分布**：按设计文档建一张跨节点的分布表，展示每个分片落在哪个节点、每个键落在哪个分片。
4. **Raft**：每个分片自动建一个 Raft 组；把选主的**动态过程**（谁发起竞选、任期、谁当选、谁认它为 leader）实时展示出来；标明最终的主（leader）和从（follower）分别是哪些节点；并演示**同一个节点上既有 leader 又有 follower**。
5. **路由与流控**：展示路由信息（经协调者的读写发往哪、控制面登记的主从、各节点本地角色）和流控信息（Raft 日志环、分区流捕获环）。
6. **SQL**：增删改查；两个会话**并发**执行事务，包括**跨分片插入**、**跨分片删除**，体现快照隔离与写写冲突。
7. **高可用**：**手动切换 leader** 的过程；**模拟不可抗力宕机**时 Raft 自动选主的过程；节点恢复后自动归队。
8. **事务内核**：**分片级 xid 分配器**、**分片级 clog**，以及**惰性回放**。
9. **形式约束**：演示脚本里只有 SQL 和事务，过程性的东西全部预先封装；先实跑通、把过程记录成本教程；演示完把环境恢复成演示前的样子；这些文件只用于演示，**不改动项目的任何其他文件和代码**。

对照：

| 要求 | 在哪一步 | 用到的函数 / 语句 |
|---|---|---|
| 4 个节点 | 第 1 步 | `demo.nodes()` |
| 跨节点的表、分片在哪 | 第 2 步 | `CREATE TABLE` + `create_distributed_table`、`demo.shards()`、`demo.locate()` |
| 自动建 Raft 组、选主动态过程 | 第 3 步 | `demo.raft_elect()`（NOTICE 实时时间线） |
| 最终主从 | 第 4 步 | `demo.raft_replicas()`、`demo.raft_groups()` |
| 一个节点既有 leader 又有 follower | 第 5、13、14 步 | `demo.roles()` |
| 路由信息 | 第 7 步 | `demo.routing()` |
| 增删改查 | 第 8 步 | `INSERT / UPDATE / DELETE / SELECT`，跨分片写用 `demo.global_txn()` |
| 分片级 xid 分配器、分片级 clog | 第 9、12、13 步 | `demo.xid()`、`demo.clog()`、`demo.versions()` |
| 流控信息 | 第 10 步 | `demo.flow()` |
| 惰性回放 | 第 11、14 步 | `demo.replay()`、`demo.catchup()`、`demo.compare()` |
| 并发事务（跨分片插入 / 删除） | 第 12 步 | 窗口 A、B 各自 `BEGIN … COMMIT` |
| 手动切换 leader | 第 13 步 | `demo.switch_leader()` |
| 模拟宕机、恢复 | 第 14 步 | `demo.crash()`、`demo.recover()` |

## 二、文件（都在 `~/shardpg-test-work/pg-partdist-src/demo/`，只用于演示）

| 文件 | 作用 |
|---|---|
| `shardpg_demo_steps.sql` | **演示脚本**：只有 SQL 和事务，按步骤排好，`-- @A` / `-- @B` 标明在哪个窗口输入 |
| `shardpg_demo_functions.sql` | 封装好的演示函数库（`demo.*`），只装在协调者上；演示结束整体删除 |
| `shardpg_demo.sh` | 服务器上的启动器：`start` 准备环境 / `sql A`、`sql B` 打开会话 / `stop` 恢复环境 |
| `shardpg_demo.ps1` | Windows 端的一键入口（可选）：`.\shardpg_demo.ps1 start` / `sql A` / `sql B` / `stop` |
| `SHARDPG_DEMO.md` | 本教程 |

`start` 对集群做的临时改动只有：在协调者上建 `demo` 模式（函数库 + dblink）、打开 TSO（会重启一次协调者）、把 3 台 worker 的 Raft 选举超时从 6 s 放宽到 15 s（见“已知问题”第 7 条），建组供副本期间再临时冻结到 30 s（函数内复位）。`stop` 把它们全部撤掉，并按演示前各节点 `postgresql.auto.conf` 的原样还原参数；实测还原后与演示前的环境快照逐项一致。

## 三、准备：从 Windows 11 PowerShell 进入演示

> 中文要正常显示：用 Windows Terminal 打开 PowerShell（默认 UTF-8）；老式控制台先执行 `chcp 65001`。

**窗口 1**（之后作为窗口 A）—— 准备演示环境：

```powershell
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh start"
```

```text
① 安装 demo 函数库（只装在协调者上）
② 记下演示会改动的参数在各节点 postgresql.auto.conf 里的原样（stop 时原样还原）
③ 打开 TSO（全局时间戳服务，跑在协调者上）：删 boot 标记 → 重启协调者 → tso_master=on → 各节点指向它
④ 演示期间把 3 台 worker 的 Raft 选举超时放宽到 15 s（2 vCPU 上避免负载抖动误选主；stop 时还原）
准备好了。打开会话：bash shardpg_demo.sh sql A（第二个窗口：bash shardpg_demo.sh sql B）；演示函数一览：SELECT * FROM demo.help();
```

然后在窗口 1 打开会话 A，**再开一个 PowerShell 窗口**打开会话 B：

```powershell
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh sql A"   # 窗口 A
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh sql B"   # 窗口 B
```

（把 `shardpg_demo.ps1` 拷到 Windows 上，也可以用 `.\shardpg_demo.ps1 start`、`.\shardpg_demo.ps1 sql A`、`.\shardpg_demo.ps1 sql B`。）
看到提示符 `A>` / `B>` 就可以开始输入下面的 SQL 了。`SELECT * FROM demo.help();` 列出全部演示函数。

## 四、演示步骤（全部是 SQL）

带 `NOTICE:` 的行是函数执行过程中**实时**打出来的时间线（毫秒是从本步开始计时），最后的表格是结果。

### 第 1 步：集群里的 4 个节点

1 个协调者（cn）+ 3 个 worker（w1 w2 w3）。每个节点有一个 pg_raft 节点号；

控制面是 0 号 Raft 组（4 个节点都在里面），它登记"每个分片的主是谁"。

**窗口 A**：

```sql
SELECT * FROM demo.nodes();
```

```text
 节点 | 端口 |  类型   | raft节点号 | 状态 |     控制面0号组     
--------+--------+-----------+---------------+--------+--------------------------
 cn     |   5432 | 协调者 |             1 | 在线 | follower（任期 475）
 w1     |   5433 | worker    |             2 | 在线 | follower（任期 475）
 w2     |   5434 | worker    |             3 | 在线 | leader（任期 475）
 w3     |   5435 | worker    |             4 | 在线 | follower（任期 475）
(4 rows)
```

### 第 2 步：建一张跨节点的表，看每个分片在哪个节点

3 个分片，Citus 按哈希把它们分别放到 3 台 worker 上；每个键落在哪个分片由哈希决定。

**窗口 A**：

```sql
SET citus.shard_count = 3;
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

**窗口 A**：

```sql
SELECT * FROM demo.shards('account');
```

```text
 分片 | 分片号 | 所在节点 | 端口 |       哈希范围        | 行数 | worker上的表名 
--------+-----------+--------------+--------+---------------------------+--------+--------------------
 S1     |    102897 | w1           |   5433 | [-2147483648, -715827884] |      0 | account_102897
 S2     |    102898 | w2           |   5434 | [-715827883, 715827881]   |      0 | account_102898
 S3     |    102899 | w3           |   5435 | [715827882, 2147483647]   |      0 | account_102899
(3 rows)
```

**窗口 A**：

```sql
SELECT k AS 键, l.分片, l.当前主节点 FROM generate_series(1, 9) k, demo.locate('account', k) l;
```

```text
 键 | 分片 | 当前主节点 
-----+--------+-----------------
   1 | S1     | w1
   2 | S3     | w3
   3 | S2     | w2
   4 | S1     | w1
   5 | S1     | w1
   6 | S2     | w2
   7 | S1     | w1
   8 | S1     | w1
   9 | S3     | w3
(9 rows)
```

### 第 3 步：每个分片自动建一个 Raft 组，并选出 leader（选主过程实时打出来）

每个分片一个 Raft 组，成员 = 3 台 worker。数据此刻只在分片的 placement 节点上，所以由它发起竞选：

任期 0 → 它变成 candidate（任期 1）向另外两台要票 → 拿到多数票当选 leader → 另外两台认它为 leader；

当选后它先做升主前置，再到控制面登记"我是这个分片的主"，最后 Citus 路由指向它。

**窗口 A**（耗时 29.7 s）：

```sql
SELECT * FROM demo.raft_elect('account');
```

```text
NOTICE:  准备：3 台 worker 的选举超时临时冻结在 30 s —— 没有节点会自发竞选，只有被点名的节点发起（建组供副本结束后复位）
NOTICE:  ────── S1（分片 102897）：数据在 w1 上，建组 [成员 w1 w2 w3]，由 w1 发起竞选 ──────
NOTICE:     426 ms  S1  建好组：w1 follower/任期0，w2 follower/任期0，w3 follower/任期0；控制面登记 还没有；Citus 路由 → w1
NOTICE:     444 ms  S1  w1  pg_raft_group_campaign：点名它发起竞选
NOTICE:     750 ms  S1  w1  ★ 当选 leader（任期 1）
NOTICE:     750 ms  S1  w2  follower（任期 1，还没认出 leader）
NOTICE:     750 ms  S1  w3  follower（任期 1，还没认出 leader）
NOTICE:     891 ms  S1  w2  follower：认 w1 为 leader（任期 1）
NOTICE:     891 ms  S1  w3  follower：认 w1 为 leader（任期 1）
NOTICE:    7450 ms  S1  cn  控制面登记（0 号组 partition_map）：主 = w1（登记任期 1）
NOTICE:          （当选之后、登记之前的这段时间，新 leader 在做升主前置：追平日志、认领无主 xid，然后才上报控制面）
NOTICE:  ────── S2（分片 102898）：数据在 w2 上，建组 [成员 w1 w2 w3]，由 w2 发起竞选 ──────
NOTICE:     249 ms  S2  建好组：w1 follower/任期0，w2 follower/任期0，w3 follower/任期0；控制面登记 还没有；Citus 路由 → w2
NOTICE:     266 ms  S2  w2  pg_raft_group_campaign：点名它发起竞选
NOTICE:     900 ms  S2  w1  follower：认 w2 为 leader（任期 1）
NOTICE:     900 ms  S2  w2  ★ 当选 leader（任期 1）
NOTICE:     900 ms  S2  w3  follower（任期 1，还没认出 leader）
NOTICE:     953 ms  S2  w3  follower：认 w2 为 leader（任期 1）
NOTICE:   13103 ms  S2  cn  控制面登记（0 号组 partition_map）：主 = w2（登记任期 1）
NOTICE:          （当选之后、登记之前的这段时间，新 leader 在做升主前置：追平日志、认领无主 xid，然后才上报控制面）
NOTICE:  ────── S3（分片 102899）：数据在 w3 上，建组 [成员 w1 w2 w3]，由 w3 发起竞选 ──────
NOTICE:     128 ms  S3  建好组：w1 follower/任期0，w2 follower/任期0，w3 follower/任期0；控制面登记 还没有；Citus 路由 → w3
NOTICE:     146 ms  S3  w3  pg_raft_group_campaign：点名它发起竞选
NOTICE:     435 ms  S3  w1  follower（任期 1，还没认出 leader）
NOTICE:     435 ms  S3  w2  follower（任期 1，还没认出 leader）
NOTICE:     435 ms  S3  w3  ★ 当选 leader（任期 1）
NOTICE:     489 ms  S3  w1  follower：认 w3 为 leader（任期 1）
NOTICE:     489 ms  S3  w2  follower：认 w3 为 leader（任期 1）
NOTICE:    8027 ms  S3  cn  控制面登记（0 号组 partition_map）：主 = w3（登记任期 1）
NOTICE:          （当选之后、登记之前的这段时间，新 leader 在做升主前置：追平日志、认领无主 xid，然后才上报控制面）
 分片 | 分片号 | leader | followers | 任期 | 选主耗时_ms | 控制面登记的主 | citus路由 
--------+-----------+--------+-----------+--------+-----------------+-----------------------+-------------
 S1     |    102897 | w1     | w2, w3    | 1      |            7506 | w1                    | w1 :5433
 S2     |    102898 | w2     | w1, w3    | 1      |           13162 | w2                    | w2 :5434
 S3     |    102899 | w3     | w1, w2    | 1      |            8081 | w3                    | w3 :5435
(3 rows)
```

### 第 4 步：供副本 → 最终的主和从

在每个组的 leader 上把副本供到另外两台：发一份物理基线进分区流（经 Raft 复制），

目标节点按基线配对文件号、arm 回放槽位。之后每个分片 = 1 主（leader）+ 2 从（follower）。

**窗口 A**（耗时 12.3 s）：

```sql
SELECT * FROM demo.raft_replicas('account');
```

```text
NOTICE:  选举超时已复位到演示期间的 15 s
 分片 | 主 | 副本供到 | 基线游标 |                                   结果                                    
--------+-----+--------------+--------------+-----------------------------------------------------------------------------
 S1     | w1  | w2           | 1            | 已供：物理基线进分区流 → w2 配对文件号、arm 回放槽位
 S1     | w1  | w3           | 6            | 已供：物理基线进分区流 → w3 配对文件号、arm 回放槽位
 S2     | w2  | w1           | 1            | 已供：物理基线进分区流 → w1 配对文件号、arm 回放槽位
 S2     | w2  | w3           | 6            | 已供：物理基线进分区流 → w3 配对文件号、arm 回放槽位
 S3     | w3  | w1           | 1            | 已供：物理基线进分区流 → w1 配对文件号、arm 回放槽位
 S3     | w3  | w2           | 6            | 已供：物理基线进分区流 → w2 配对文件号、arm 回放槽位
(6 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.raft_groups('account');
```

```text
 分片 | 节点 |   角色   | 任期 | 认定的leader | 日志末尾 | 已提交 | 已应用 
--------+--------+------------+--------+-----------------+--------------+-----------+-----------
 S1     | w1     | ★ leader |      1 | w1              |           10 |        10 |        10
 S1     | w2     | follower   |      1 | w1              |           10 |        10 |        10
 S1     | w3     | follower   |      1 | w1              |           10 |        10 |        10
 S2     | w1     | follower   |      1 | w2              |           10 |        10 |        10
 S2     | w2     | ★ leader |      1 | w2              |           10 |        10 |        10
 S2     | w3     | follower   |      1 | w2              |           10 |        10 |        10
 S3     | w1     | follower   |      1 | w3              |           10 |        10 |        10
 S3     | w2     | follower   |      1 | w3              |           10 |        10 |         9
 S3     | w3     | ★ leader |      1 | w3              |           10 |        10 |        10
(9 rows)
```

### 第 5 步：同一个节点上既有 leader 又有 follower

行是节点、列是分片：每台 worker 都是一个分片的 leader、另外两个分片的 follower。

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |            S1             |            S2             |            S3             |            小结             
----------+---------------------------+---------------------------+---------------------------+-------------------------------
 w1 :5433 | ★ leader（任期1）   | follower（回放到 0） | follower（回放到 0） | 1 个 leader + 2 个 follower
 w2 :5434 | follower（回放到 0） | ★ leader（任期1）   | follower（回放到 0） | 1 个 leader + 2 个 follower
 w3 :5435 | follower（回放到 0） | follower（回放到 0） | ★ leader（任期1）   | 1 个 leader + 2 个 follower
(3 rows)
```

### 第 6 步：接入事务系统（打标）

打标之后，这张表的分片才走"分片级 xid + 分片级 clog + TSO 时间戳"的事务机制。

**窗口 A**（耗时 2.3 s）：

```sql
SELECT * FROM partdist.set_table_shard_mvcc('account');
```

```text
 shardid | node_port |                  status                   
---------+-----------+-------------------------------------------
  102897 |      5433 | registered oid=227626 replica_notice=sent
  102898 |      5434 | registered oid=169545 replica_notice=sent
  102899 |      5435 | registered oid=71841 replica_notice=sent
(3 rows)
```

### 第 7 步：路由信息

三层：① Citus 路由表决定经协调者的读写发往哪个节点；② 控制面登记（0 号 Raft 组）记录每个分片的主从；

③ 每个节点本地知道自己对这个分片是主（写入被捕获进分区流）还是从（只收流）。

**窗口 A**：

```sql
SELECT * FROM demo.routing('account');
```

```text
 分片 |                       层                       | 节点 |                                              内容                                              
--------+-------------------------------------------------+--------+--------------------------------------------------------------------------------------------------
 S1     | ① Citus 路由（pg_dist_placement）         | w1     | 经协调者的读写都发往 w1 :5433
 S1     | ② 控制面登记（0 号组 partition_map） | w1     | 主 = w1，从 = w2,w3，登记任期 1
 S1     | ③ 节点本地（route_status）              | w1     | 主：写入被捕获进分区流；role=replica_or_plain captured=yes members=4 xid_watermark=0
 S1     | ③ 节点本地（route_status）              | w2     | 从：只收流、不接受写；role=replica_or_plain captured=no members=4 xid_watermark=0
 S1     | ③ 节点本地（route_status）              | w3     | 从：只收流、不接受写；role=replica_or_plain captured=no members=4 xid_watermark=0
 S2     | ① Citus 路由（pg_dist_placement）         | w2     | 经协调者的读写都发往 w2 :5434
 S2     | ② 控制面登记（0 号组 partition_map） | w2     | 主 = w2，从 = w1,w3，登记任期 1
 S2     | ③ 节点本地（route_status）              | w1     | 从：只收流、不接受写；role=replica_or_plain captured=no members=4 xid_watermark=0
 S2     | ③ 节点本地（route_status）              | w2     | 主：写入被捕获进分区流；role=replica_or_plain captured=yes members=4 xid_watermark=0
 S2     | ③ 节点本地（route_status）              | w3     | 从：只收流、不接受写；role=replica_or_plain captured=no members=4 xid_watermark=0
 S3     | ① Citus 路由（pg_dist_placement）         | w3     | 经协调者的读写都发往 w3 :5435
 S3     | ② 控制面登记（0 号组 partition_map） | w3     | 主 = w3，从 = w1,w2，登记任期 1
 S3     | ③ 节点本地（route_status）              | w1     | 从：只收流、不接受写；role=replica_or_plain captured=no members=4 xid_watermark=0
 S3     | ③ 节点本地（route_status）              | w2     | 从：只收流、不接受写；role=replica_or_plain captured=no members=4 xid_watermark=0
 S3     | ③ 节点本地（route_status）              | w3     | 主：写入被捕获进分区流；role=replica_or_plain captured=yes members=4 xid_watermark=0
(15 rows)
```

### 第 8 步：增删改查

只落一个分片的写走 1PC，直接写；跨分片的写要走 2PC，必须先加入全局事务（取全局事务号 + TSO 快照时间戳）。

**窗口 A**：

```sql
INSERT INTO account VALUES (1, 'alice', 1000);
```

```text
INSERT 0 1
```

不加入全局事务就跨分片写：被拒（2PC 的 PREPARE 不放行）。

**窗口 A**：

```sql
INSERT INTO account VALUES (2, 'bob', 1000), (3, 'carol', 1000);
```

```text
WARNING:  未加入全局事务的分片写不允许 PREPARE TRANSACTION
HINT:  经连接加入协议（partdist_join_global_txn / join_info GUC）携带 gxid 后放行。
WARNING:  connection to the remote node postgres@localhost:5435 failed with the following error: ERROR:  未加入全局事务的分片写不允许 PREPARE TRANSACTION
HINT:  经连接加入协议（partdist_join_global_txn / join_info GUC）携带 gxid 后放行。
another command is already in progress
ERROR:  未加入全局事务的分片写不允许 PREPARE TRANSACTION
HINT:  经连接加入协议（partdist_join_global_txn / join_info GUC）携带 gxid 后放行。
CONTEXT:  while executing command on localhost:5434
```

加入全局事务后跨分片插入：一条语句写 3 个分片。

**窗口 A**（耗时 18.8 s）：

```sql
BEGIN;
SELECT demo.global_txn();
INSERT INTO account VALUES (2, 'bob', 1000), (3, 'carol', 1000), (4, 'dave', 1000),
                           (5, 'erin', 1000), (6, 'frank', 1000), (9, 'ivy', 1000);
COMMIT;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 204801，start_ts = 18（本事务在所有分片上共用这一个快照）
(1 row)

INSERT 0 6
COMMIT
```

**窗口 A**：

```sql
SELECT * FROM account ORDER BY id;
```

```text
 id | owner | balance 
----+-------+---------
  1 | alice |    1000
  2 | bob   |    1000
  3 | carol |    1000
  4 | dave  |    1000
  5 | erin  |    1000
  6 | frank |    1000
  9 | ivy   |    1000
(7 rows)
```

单分片的改和删；再做一次跨分片的改和删。

**窗口 A**：

```sql
UPDATE account SET balance = balance - 100 WHERE id = 1;
DELETE FROM account WHERE id = 5;
```

```text
UPDATE 1
DELETE 1
```

**窗口 A**（耗时 2.8 s）：

```sql
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 100 WHERE id IN (2, 3);
DELETE FROM account WHERE id IN (4, 9);
COMMIT;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 204802，start_ts = 30（本事务在所有分片上共用这一个快照）
(1 row)

UPDATE 2
DELETE 2
COMMIT
```

**窗口 A**：

```sql
SELECT * FROM account ORDER BY id;
```

```text
 id | owner | balance 
----+-------+---------
  1 | alice |     900
  2 | bob   |    1100
  3 | carol |    1100
  6 | frank |    1000
(4 rows)
```

### 第 9 步：分片级 xid 分配器与分片级 clog

每个分片有自己的 xid 分配器（从 3 起发），元组头里的 xmin/xmax 存的是分片 xid，与节点原生 xid 无关；

每个分片有自己的一本 clog：分片 xid → 判决 + start_ts + commit_ts。

**窗口 A**：

```sql
SELECT * FROM demo.xid('account');
```

```text
 分片 | 主节点 | 下一个分片xid |                      主的持久化水位                       | 主节点的原生xid | 各从学到的水位 
--------+-----------+--------------------+------------------------------------------------------------------+-----------------------+-----------------------
 S1     | w1        |                  8 | 4099（按 4096 一批落盘，崩溃后从批次上界续发） |                 99068 | w2=8，w3=8
 S2     | w2        |                  6 | 4099（按 4096 一批落盘，崩溃后从批次上界续发） |                100096 | w1=6，w3=6
 S3     | w3        |                  6 | 4099（按 4096 一批落盘，崩溃后从批次上界续发） |                102325 | w1=6，w2=6
(3 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.clog('account', 'S1');
```

```text
NOTICE:  S1 当前的主是 w1，下面是它那本分片 clog（st：0 空/运行中 1 PREPARED 2 COMMITTED 3 ABORTED）
 分片xid |   判决    | start_ts | commit_ts | 写入的行 | 删改的行 
-----------+-------------+----------+-----------+--------------+--------------
         3 | 2 COMMITTED |       14 |        15 | 1            | 
         4 | 2 COMMITTED |       18 |        19 | 4,5          | 
         5 | 2 COMMITTED |       26 |        27 | 1            | 1
         6 | 2 COMMITTED |       28 |        29 |              | 5
         7 | 2 COMMITTED |       30 |        31 |              | 4
(5 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.versions('account', 'S1');
```

```text
NOTICE:  S1 当前的主是 w1；元组头里的 xmin/xmax 是**分片 xid**，查的是这个分片自己的 clog
 位置 | id | xmin |     xmin判决     | xmax |     xmax判决     |      现在可见      
--------+----+------+--------------------+------+--------------------+------------------------
 (0,1)  |  1 |    3 | st=2 sts=14 cts=15 |    5 | st=2 sts=26 cts=27 | 否（已被删/改）
 (0,2)  |  4 |    4 | st=2 sts=18 cts=19 |    7 | st=2 sts=30 cts=31 | 否（已被删/改）
 (0,3)  |  5 |    4 | st=2 sts=18 cts=19 |    6 | st=2 sts=28 cts=29 | 否（已被删/改）
 (0,4)  |  1 |    5 | st=2 sts=26 cts=27 |    0 |                    | 是
(4 rows)
```

### 第 10 步：流控信息

Raft 日志环：每组 128 条，环深度 = 已追加未应用；环满先背压等待、超时才丢弃。

分区流捕获环：主上写入先进捕获环再进分区流；覆盖次数 > 0 才说明有记录没进流。

**窗口 A**：

```sql
SELECT * FROM demo.flow('account');
```

```text
 分片 | 节点 |  角色  | 日志环深度 | 环容量 | 环满背压等待 | 环满丢弃 | 多数派不足丢弃 | 捕获环未消费 | 捕获环覆盖 | 写路径背压排空 
--------+--------+----------+-----------------+-----------+--------------------+--------------+-----------------------+--------------------+-----------------+-----------------------
 S1     | w1     | leader   |               0 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
 S1     | w2     | follower |               1 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
 S1     | w3     | follower |               1 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
 S2     | w1     | follower |               1 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
 S2     | w2     | leader   |               0 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
 S2     | w3     | follower |               1 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
 S3     | w1     | follower |               1 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
 S3     | w2     | follower |               1 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
 S3     | w3     | leader   |               0 |       128 |                  0 |            0 |                     0 |                  0 |               0 |                     0
(9 rows)
```

### 第 11 步：惰性回放

从节点经 Raft 把主的分区流收下落盘，但不马上 redo（armed ≠ 正在回放）；有人触发才追平。

**窗口 A**：

```sql
SELECT * FROM demo.replay('account');
```

```text
 分片 | 主 | 主的流位点 | 从 | 从已收到 | 从已回放 | 待回放 |  回放槽   
--------+-----+-----------------+-----+--------------+--------------+-----------+--------------
 S1     | w1  |              35 | w2  |           34 |            0 |        34 | armed，idle
 S1     | w1  |              35 | w3  |           34 |            0 |        34 | armed，idle
 S2     | w2  |              31 | w1  |           30 |            0 |        30 | armed，idle
 S2     | w2  |              31 | w3  |           30 |            0 |        30 | armed，idle
 S3     | w3  |              32 | w1  |           31 |            0 |        31 | armed，idle
 S3     | w3  |              32 | w2  |           31 |            0 |        31 | armed，idle
(6 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.catchup('account');
```

```text
 分片 | 从 | 回放前 | 目标 | 回放后 | 耗时_ms 
--------+-----+-----------+--------+-----------+-----------
 S1     | w2  |         0 |     34 |        34 |       243
 S1     | w3  |         0 |     35 |        35 |       162
 S2     | w1  |         0 |     30 |        30 |       328
 S2     | w3  |         0 |     31 |        31 |       249
 S3     | w1  |         0 |     32 |        32 |       244
 S3     | w2  |         0 |     32 |        32 |       237
(6 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.replay('account');
```

```text
 分片 | 主 | 主的流位点 | 从 | 从已收到 | 从已回放 | 待回放 |  回放槽   
--------+-----+-----------------+-----+--------------+--------------+-----------+--------------
 S1     | w1  |              35 | w2  |           35 |           34 |         1 | armed，idle
 S1     | w1  |              35 | w3  |           35 |           35 |         0 | armed，idle
 S2     | w2  |              31 | w1  |           31 |           30 |         1 | armed，idle
 S2     | w2  |              31 | w3  |           31 |           31 |         0 | armed，idle
 S3     | w3  |              32 | w1  |           32 |           32 |         0 | armed，idle
 S3     | w3  |              32 | w2  |           32 |           32 |         0 | armed，idle
(6 rows)
```

追平之后，副本页面与主逐字节一致（按内核 heap_mask 口径）。

**窗口 A**（耗时 2.9 s）：

```sql
SELECT * FROM demo.compare('account');
```

```text
 分片 | 主 | 从 |       主堆        |    主键索引     
--------+-----+-----+---------------------+---------------------
 S1     | w1  | w2  | ✓ 逐字节一致 | ✓ 逐字节一致
 S1     | w1  | w3  | ✓ 逐字节一致 | ✓ 逐字节一致
 S2     | w2  | w1  | ✓ 逐字节一致 | ✓ 逐字节一致
 S2     | w2  | w3  | ✓ 逐字节一致 | ✓ 逐字节一致
 S3     | w3  | w1  | ✓ 逐字节一致 | ✓ 逐字节一致
 S3     | w3  | w2  | ✓ 逐字节一致 | ✓ 逐字节一致
(6 rows)
```

### 第 12 步：两个窗口并发做事务（跨分片插入、跨分片删除、快照隔离、写写冲突）

幕 1：A、B 各开一个全局事务，各看一眼（两边都有一个 start_ts）。

**窗口 A**：

```sql
BEGIN;
SELECT demo.global_txn();
SELECT * FROM account ORDER BY id;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 204803，start_ts = 38（本事务在所有分片上共用这一个快照）
(1 row)

 id | owner | balance 
----+-------+---------
  1 | alice |     900
  2 | bob   |    1100
  3 | carol |    1100
  6 | frank |    1000
(4 rows)
```

**窗口 B**：

```sql
BEGIN;
SELECT demo.global_txn();
SELECT * FROM account ORDER BY id;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 204804，start_ts = 39（本事务在所有分片上共用这一个快照）
(1 row)

 id | owner | balance 
----+-------+---------
  1 | alice |     900
  2 | bob   |    1100
  3 | carol |    1100
  6 | frank |    1000
(4 rows)
```

幕 2：A 跨分片插入 3 行（落在 3 个分片），再跨分片改 2 行；A 自己看得见。

**窗口 A**：

```sql
INSERT INTO account VALUES (7, 'grace', 500), (19, 'kate', 500), (11, 'judy', 500);
UPDATE account SET balance = balance - 50 WHERE id IN (2, 3);
SELECT * FROM account ORDER BY id;
```

```text
INSERT 0 3
UPDATE 2
 id | owner | balance 
----+-------+---------
  1 | alice |     900
  2 | bob   |    1050
  3 | carol |    1050
  6 | frank |    1000
  7 | grace |     500
 11 | judy  |     500
 19 | kate  |     500
(7 rows)
```

幕 3：B 看不见 A 没提交的改动。

**窗口 B**：

```sql
SELECT * FROM account ORDER BY id;
```

```text
 id | owner | balance 
----+-------+---------
  1 | alice |     900
  2 | bob   |    1100
  3 | carol |    1100
  6 | frank |    1000
(4 rows)
```

幕 4：A 提交；B 在自己的事务里仍然看不见（快照隔离：B 的 start_ts 早于 A 的 commit_ts）。

**窗口 A**（耗时 4.5 s）：

```sql
COMMIT;
```

```text
COMMIT
```

**窗口 B**：

```sql
SELECT * FROM account ORDER BY id;
```

```text
 id | owner | balance 
----+-------+---------
  1 | alice |     900
  2 | bob   |    1100
  3 | carol |    1100
  6 | frank |    1000
(4 rows)
```

幕 5：B 跨分片删除 2 行并提交。

**窗口 B**（耗时 5.3 s）：

```sql
DELETE FROM account WHERE id IN (1, 6);
COMMIT;
```

```text
DELETE 2
COMMIT
```

幕 6：A 新开的查询是新快照：A 的插入/修改、B 的删除全部可见。

**窗口 A**：

```sql
SELECT * FROM account ORDER BY id;
```

```text
 id | owner | balance 
----+-------+---------
  2 | bob   |    1050
  3 | carol |    1050
  7 | grace |     500
 11 | judy  |     500
 19 | kate  |     500
(5 rows)
```

幕 7：写写冲突。A 改 id=2（不提交）；B 也去删 id=2 —— B 被行锁挡住。

**窗口 A**：

```sql
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 7 WHERE id IN (2, 11);
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 204805，start_ts = 50（本事务在所有分片上共用这一个快照）
(1 row)

UPDATE 2
```

**窗口 B**（这条会被挡住，终端停在这里 —— 先别等它，切回窗口 A）：

```sql
BEGIN;
SELECT demo.global_txn();
DELETE FROM account WHERE id = 2;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 204806，start_ts = 51（本事务在所有分片上共用这一个快照）
(1 row)
```

A 提交后，B 的等待结束，但它被中止：先提交者胜（first-committer-wins），B 只能重试。

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
DETAIL:  分片 71841 目标行的并发删改 commit_ts=52 ≥ 本事务 start_ts=51（first-committer-wins，设计 §4.4）。
HINT:  重试事务。
CONTEXT:  while executing command on localhost:5435
```

**窗口 B**：

```sql
ROLLBACK;
```

```text
ROLLBACK
```

幕 8：分片 clog 上的整段故事（A 的号已提交、B 被中止的号是 ABORTED）。

**窗口 A**：

```sql
SELECT * FROM demo.clog('account', 'S3');
```

```text
NOTICE:  S3 当前的主是 w3，下面是它那本分片 clog（st：0 空/运行中 1 PREPARED 2 COMMITTED 3 ABORTED）
 分片xid |   判决    | start_ts | commit_ts | 写入的行 | 删改的行 
-----------+-------------+----------+-----------+--------------+--------------
         3 | 3 ABORTED   |       17 |         0 | 2            | 
         4 | 2 COMMITTED |       18 |        19 | 2,9          | 
         5 | 2 COMMITTED |       30 |        31 | 2            | 2,9
         6 | 2 COMMITTED |       38 |        40 | 2,11         | 2
         7 | 2 COMMITTED |       50 |        52 | 2,11         | 2,11
         8 | 3 ABORTED   |       51 |         0 |              | 
(6 rows)
```

### 第 13 步：手动切换 leader

把 S1 的主从 w1 切到 w2（w2 本来就是 S2 的主）：在 w2 上对 S1 的组发起竞选，过程实时打出来。

切完 w2 同时当两个分片的主，旧主 w1 被新主自动重新供给成副本；新主的分片 xid 接着旧主的号往下发。

**窗口 A**（耗时 16.2 s）：

```sql
SELECT * FROM demo.switch_leader('account', 'S1', 'w2');
```

```text
NOTICE:  切换前：S1 的主 = w1，下一个分片 xid = 10
NOTICE:    1220 ms  副本都已确认最新提交（各从的 Raft 提交位点 = 主的日志末尾）
NOTICE:    1227 ms  S1  切换前：w1 leader/任期1，w2 follower/任期1，w3 follower/任期1；控制面登记 w1；Citus 路由 → w1
NOTICE:    1390 ms  S1  w2  pg_raft_group_campaign：对 S1 的组发起竞选
NOTICE:    1721 ms  S1  w1  退位 → follower，跟随 ?（任期 2）
NOTICE:    1721 ms  S1  w2  发起竞选 → candidate（任期 2，向其余成员要票）
NOTICE:    1721 ms  S1  w3  follower（任期 2，还没认出 leader）
NOTICE:    1858 ms  S1  w1  follower：认 w2 为 leader（任期 2）
NOTICE:    1858 ms  S1  w2  ★ 当选 leader（任期 2）
NOTICE:    1858 ms  S1  w3  follower：认 w2 为 leader（任期 2）
NOTICE:    9206 ms  S1  cn  控制面登记（0 号组 partition_map）：主 = w2（登记任期 2）
NOTICE:    9206 ms  S1  cn  Citus 路由：读写发往 w2 :5434
NOTICE:   10118 ms  cn  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
NOTICE:   16162 ms  S1  w1  旧主被新主自动重新供给成副本（回放槽位 armed）
 分片 | 原来的主 | 新主 | 新任期 | 切换耗时_ms | 原主换下时的下一个分片xid | 新主接着发的下一个分片xid |                    原主重新成为副本                     
--------+--------------+--------+-----------+-----------------+--------------------------------------+--------------------------------------+-----------------------------------------------------------------
 S1     | w1           | w2     | 2         |            9358 | 10                                   | 10                                   | 是：切换后 16162 ms 被新主自动重新供给（armed）
(1 row)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |             S1             |             S2             |             S3             |            小结             
----------+----------------------------+----------------------------+----------------------------+-------------------------------
 w1 :5433 | follower（回放到 0）  | follower（回放到 30） | follower（回放到 32） | 0 个 leader + 3 个 follower
 w2 :5434 | ★ leader（任期2）    | ★ leader（任期1）    | follower（回放到 32） | 2 个 leader + 1 个 follower
 w3 :5435 | follower（回放到 35） | follower（回放到 31） | ★ leader（任期1）    | 1 个 leader + 2 个 follower
(3 rows)
```

**窗口 A**：

```sql
INSERT INTO account VALUES (8, 'heidi', 800);
SELECT * FROM demo.xid('account');
```

```text
INSERT 0 1
 分片 | 主节点 | 下一个分片xid |                      主的持久化水位                       | 主节点的原生xid | 各从学到的水位 
--------+-----------+--------------------+------------------------------------------------------------------+-----------------------+-----------------------
 S1     | w2        |                 11 | 4106（按 4096 一批落盘，崩溃后从批次上界续发） |                100164 | w1=4099，w3=11
 S2     | w2        |                  8 | 4099（按 4096 一批落盘，崩溃后从批次上界续发） |                100164 | w1=8，w3=8
 S3     | w3        |                  9 | 4099（按 4096 一批落盘，崩溃后从批次上界续发） |                102408 | w1=9，w2=9
(3 rows)
```

### 第 14 步：模拟不可抗力宕机

直接把 w2 停掉（immediate：不做 checkpoint，等同断电）。它此刻是 S1、S2 两个分片的主。

两个组各自超时、各自选主；w2 只当从的 S3 不受影响。

（停机前函数会先等副本确认最新一笔提交，原因见教程"已知问题"第 1 条。）

**窗口 A**（耗时 32.1 s）：

```sql
SELECT * FROM demo.crash('account', 'w2');
```

```text
NOTICE:  宕机前：w2 是 S1、S2 的主；其余分片它只是从
NOTICE:     567 ms  副本都已确认最新提交（各从的 Raft 提交位点 = 主的日志末尾）
NOTICE:     570 ms  S1  宕机前：w1 follower/任期2，w2 leader/任期2，w3 follower/任期2；控制面登记 w2；Citus 路由 → w2
NOTICE:     572 ms  S2  宕机前：w1 follower/任期1，w2 leader/任期1，w3 follower/任期1；控制面登记 w2；Citus 路由 → w2
NOTICE:     575 ms  S3  宕机前：w1 follower/任期1，w2 follower/任期1，w3 leader/任期1；控制面登记 w3；Citus 路由 → w3
NOTICE:     854 ms  w2  pg_ctl -m immediate stop：进程直接退出，不做 checkpoint（模拟断电）
NOTICE:     855 ms  S1  w2  ✗ 连不上（节点宕机）
NOTICE:     855 ms  S2  w2  ✗ 连不上（节点宕机）
NOTICE:     855 ms  S3  w2  ✗ 连不上（节点宕机）
NOTICE:   23303 ms  S1  w1  发起竞选 → candidate（任期 3，向其余成员要票）
NOTICE:   23491 ms  S1  w1  ★ 当选 leader（任期 3）
NOTICE:   23491 ms  S1  w3  follower：认 w1 为 leader（任期 3）
NOTICE:   25441 ms  S2  w1  follower：认 w3 为 leader（任期 2）
NOTICE:   25441 ms  S2  w3  ★ 当选 leader（任期 2）
NOTICE:   30367 ms  S1  cn  控制面登记（0 号组 partition_map）：主 = w1（登记任期 3）
NOTICE:   30367 ms  S1  cn  Citus 路由：读写发往 w1 :5433
NOTICE:   31800 ms  S2  cn  控制面登记（0 号组 partition_map）：主 = w3（登记任期 2）
NOTICE:   31800 ms  S2  cn  Citus 路由：读写发往 w3 :5435
NOTICE:   32074 ms  cn  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
 分片 | 宕机前的主 | 宕机后的主 | 任期 | 不可用时长_ms 
--------+-----------------+-----------------+--------+--------------------
 S1     | w2              | w1              | 3      |              31858
 S2     | w2              | w3              | 2      |              31858
 S3     | w3              | w3              | 1      |                  0
(3 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |             S1             |             S2             |             S3             |            小结             
----------+----------------------------+----------------------------+----------------------------+-------------------------------
 w1 :5433 | ★ leader（任期3）    | follower（回放到 30） | follower（回放到 32） | 1 个 leader + 2 个 follower
 w2 :5434 | ✗ 宕机                 | ✗ 宕机                 | ✗ 宕机                 | 宕机
 w3 :5435 | follower（回放到 35） | ★ leader（任期2）    | ★ leader（任期1）    | 2 个 leader + 1 个 follower
(3 rows)
```

宕机期间照常读写（在 B 窗口）。

**窗口 B**（耗时 2.0 s）：

```sql
SELECT * FROM account ORDER BY id;
INSERT INTO account VALUES (10, 'leo', 300);
INSERT INTO account VALUES (12, 'mia', 300);
```

```text
 id | owner | balance 
----+-------+---------
  2 | bob   |    1057
  3 | carol |    1050
  3 | carol |    1000
  7 | grace |     500
  8 | heidi |     800
 11 | judy  |     507
 19 | kate  |     500
(7 rows)

INSERT 0 1
INSERT 0 1
```

**窗口 B**：

```sql
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 1 WHERE id IN (3, 8, 10);
COMMIT;
```

```text
BEGIN
                                                 global_txn                                                  
-------------------------------------------------------------------------------------------------------------
 已加入全局事务：gxid = 204807，start_ts = 62（本事务在所有分片上共用这一个快照）
(1 row)

UPDATE 3
COMMIT
```

把 w2 拉起来：它以 follower 身份回到 3 个组，原来当主的分片被新主自动重新供给成副本。

**窗口 A**（耗时 5.8 s）：

```sql
SELECT * FROM demo.recover('account', 'w2');
```

```text
NOTICE:       4 ms  S1  拉起前：w1 leader/任期3，w2 ✗宕机，w3 follower/任期3；控制面登记 w1；Citus 路由 → w1
NOTICE:       7 ms  S2  拉起前：w1 follower/任期2，w2 ✗宕机，w3 leader/任期2；控制面登记 w3；Citus 路由 → w3
NOTICE:       9 ms  S3  拉起前：w1 follower/任期1，w2 ✗宕机，w3 leader/任期1；控制面登记 w3；Citus 路由 → w3
NOTICE:     728 ms  w2  pg_ctl start：进程起来了，开始重新加入各个组
NOTICE:     728 ms  S1  w2  follower（任期 0，还没认出 leader）
NOTICE:     728 ms  S2  w2  重新连上：follower，跟随 w3（任期 2）
NOTICE:     728 ms  S3  w2  follower（任期 0，还没认出 leader）
NOTICE:     823 ms  S1  w2  follower：认 w1 为 leader（任期 3）
NOTICE:     823 ms  S3  w2  follower：认 w3 为 leader（任期 1）
NOTICE:    1214 ms  cn  控制面（0 号组）在各节点都已应用完 —— 可以接着读写了
NOTICE:    1219 ms  S1  w2  回放槽位 armed —— 已是 w1 的合格副本
NOTICE:    1228 ms  S3  w2  回放槽位 armed —— 已是 w3 的合格副本
NOTICE:    5744 ms  S2  w2  回放槽位 armed —— 已是 w3 的合格副本
 分片 | 当前的主 | 节点 | 在组里的角色 |    回放槽位     
--------+--------------+--------+--------------------+---------------------
 S1     | w1           | w2     | follower           | armed，回放到 0
 S2     | w3           | w2     | follower           | armed，回放到 0
 S3     | w3           | w2     | follower           | armed，回放到 0
(3 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.roles('account');
```

```text
  节点  |             S1             |             S2             |             S3             |            小结             
----------+----------------------------+----------------------------+----------------------------+-------------------------------
 w1 :5433 | ★ leader（任期3）    | follower（回放到 30） | follower（回放到 32） | 1 个 leader + 2 个 follower
 w2 :5434 | follower（回放到 0）  | follower（回放到 0）  | follower（回放到 0）  | 0 个 leader + 3 个 follower
 w3 :5435 | follower（回放到 35） | ★ leader（任期2）    | ★ leader（任期1）    | 2 个 leader + 1 个 follower
(3 rows)
```

**窗口 A**：

```sql
SELECT * FROM demo.catchup('account');
```

```text
 分片 | 从 | 回放前 | 目标 | 回放后 | 耗时_ms 
--------+-----+-----------+--------+-----------+-----------
 S1     | w2  |         0 |     73 |        73 |       265
 S1     | w3  |        35 |     73 |        73 |       175
 S2     | w1  |        30 |     59 |        59 |       210
 S2     | w2  |         0 |     60 |        60 |       261
 S3     | w1  |        32 |     47 |        47 |       273
 S3     | w2  |         0 |     47 |        47 |       293
(6 rows)
```

**窗口 A**（耗时 3.7 s）：

```sql
SELECT * FROM demo.compare('account');
```

```text
 分片 | 主 | 从 |       主堆        |    主键索引     
--------+-----+-----+---------------------+---------------------
 S1     | w1  | w2  | ✓ 逐字节一致 | ✓ 逐字节一致
 S1     | w1  | w3  | ✓ 逐字节一致 | ✓ 逐字节一致
 S2     | w3  | w1  | ✓ 逐字节一致 | ✓ 逐字节一致
 S2     | w3  | w2  | ✓ 逐字节一致 | ✓ 逐字节一致
 S3     | w3  | w1  | ✓ 逐字节一致 | ✓ 逐字节一致
 S3     | w3  | w2  | ✓ 逐字节一致 | ✓ 逐字节一致
(6 rows)
```

**窗口 A**：

```sql
SELECT * FROM account ORDER BY id;
```

```text
 id | owner | balance 
----+-------+---------
  2 | bob   |    1057
  3 | carol |    1051
  7 | grace |     500
  8 | heidi |     801
 10 | leo   |     301
 11 | judy  |     507
 12 | mia   |     300
 19 | kate  |     500
(8 rows)
```

## 五、收尾：恢复演示前的环境

两个窗口里都输入 `\q` 退出 psql，然后在任一 PowerShell 窗口：

```powershell
ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh stop"
```

```text
恢复演示前的环境
  删表 account（分片 102897 102898 102899）
  删控制面登记（partition_map）里演示分片的行：102897,102898,102899
  16 项参数已按演示前的 postgresql.auto.conf 还原（TSO、选举超时）
  demo 函数库已删除
完成：残留数据 Raft 组 0 个，残留演示表 0 张
```

`stop` 做的事：回滚残留的 prepared 事务 → 删演示表与各 worker 上的分片 / 副本壳表 → 拆掉数据 Raft 组 → 删控制面登记（partition_map）里演示分片的行 → 参数按演示前各节点 `postgresql.auto.conf` 原样还原（TSO、选举超时）→ 删 `demo` 函数库。本次实跑之后对 4 个节点做了环境快照比对（参数文件、扩展、模式、public 表与函数、数据组、控制面登记行数、prepared、容器 /tmp），与演示前**完全一致**。

## 六、演示时要如实说明的几点（已知问题）

1. **（P0，这次实跑新发现，产品未修）刚提交的一笔若在下一次心跳之前遇上切主 / 宕机，新主会把它判成中止。** 主上一笔单分片提交：数据与提交标记已复制到多数派、客户端已收到成功；但从节点要等主的下一次心跳才知道“这条已提交”。恰在这一拍里主宕机，新主升主时只回放到它**已知**的提交位点，随后把这笔的分片 xid 认领改判为 ABORTED —— 而那条提交标记其实就在新主自己的日志里。在做本教程的第 1 轮实跑里（当时还没有下面这道等待）就撞上了：第 13 步刚插入的 heidi，在紧接着的宕机后消失（新主 clog 里它的分片 xid 10 为 ABORTED，而新主分区流 plsn 64 的提交标记正带着分片 xid 10、commit_ts=55；新主升主时只回放到 63）。因此 `demo.crash()` / `demo.switch_leader()` 在动手前先等各副本的 Raft 提交位点追上主的日志末尾（时间线里“副本都已确认最新提交”那一行），避开这一拍。修法方向：升主追平的上界取新主本地日志末尾，而不是已知提交位点。
2. **宕机切主后，新主上的第一笔写入可能卡住。** 新主要在自己这里应用一条“我是主”的控制面登记，这条应用的事务提交时要拿该分片的复制认领位；若此刻恰有写入先拿到认领位，两者会互等（这类等待 PostgreSQL 的死锁检测看不见），只能靠 60 s 超时逐轮解开，实测一次持续了约 6 分钟。演示函数在切主 / 宕机后先等各节点把控制面日志应用完（时间线里“控制面在各节点都已应用完”那一行）才把控制权交还给你。
3. **切主的一瞬，经协调者的读可能被拒 1–2 s。** 同一条控制面登记由各节点各自应用，先后差 0.2–1.9 s：旧主先应用就先把读闸门合上，而协调者的路由还指向它，这 1–2 s 里的读会报“不允许在本节点上对副本壳表…执行查询”。
4. **第 10 步的“多数派不足丢弃”偶尔会是非零（比如 4），那不是分叉。** 建组后、供副本前，从节点上还没有分片表，主发的“冻结账目”提案凑不齐多数派被丢（流位点 1）；之后供副本的物理基线把它完整覆盖，并清掉分叉标记。供副本来得快就是 0（本教程这一轮就是 0）。
5. **（这次实跑新发现，产品未修）节点重启时，同一个 Raft 组可能在它上面被建成 2 个槽位。** 重启后多个后端同时“按注册表恢复 / 按通告建组”，而建组的“先查有没有、再找空槽插入”不在同一把锁里；多出来的那个槽位会反复发起竞选，把该组的主一次次逼下台（实测一个组的任期从 2 被抬到 12）。槽位只在共享内存里，再重启一次该节点即可消除——`demo.recover()` 拉起节点后会自己检查，发现就再重启（时间线里有“⚠ … 被建成了 2 个槽位”那一行）。实测 3 次重启里 2 次出现。
6. **当选到登记之间有 5–10 s。** 那是新 leader 在做升主前置（追平日志、闭合 in-doubt、认领无主 xid），做完才上报控制面。
7. **演示期间选举超时放宽到 15 s，所以宕机后大约 20 多秒才切完。** 默认是 6 s；但这台演示机只有 2 个 vCPU，两个会话并发跑跨分片 2PC 再加上逐字节比对的刷盘时，每个节点上那一个串行的共识 tick 偶尔一轮要卡 5–9 s（日志告警“共识 tick 耗时 9021 ms（选举超时 6000 ms）”），心跳断档超过 6 s 就会误选主。
8. **自动归队只等约 5 分钟。** 新主拉起的“自动归队工作者”每 15 s 检查一次、约 5 分钟后退出；宕机节点停得更久，`demo.recover()` 会在等 45 s 没结果后手动触发一次重新供给（`partdist.reprovision_demoted`）。
9. **别在演示中途重启协调者。** TSO 计数器在协调者内存里，重启后会拒绝发号（boot 防呆），要按 `start` 里的步骤重开。
10. **中止事务留下的记录要等下一笔提交才复制到从。** 所以刚发生过中止（例如第 8 步被拒的跨分片写）的分片，在下一次提交之前做逐字节比对会不一致，`demo.compare()` 会注明原因。

## 七、演示函数一览

```text
函数                 |                                           作用                                            
---------------------------------------+---------------------------------------------------------------------------------------------
 demo.nodes()                          | 4 个节点：端口、类型、pg_raft 节点号、在线状态、控制面 0 号组角色
 demo.shards('表')                    | 每个分片落在哪个节点、哈希范围、行数
 demo.locate('表', 键)               | 某个键落在哪个分片、当前由哪个节点服务
 demo.raft_elect('表')                | 每个分片建 Raft 组并选主（实时打出选主过程）
 demo.raft_replicas('表')             | 在各组的主上把副本供到其余两台 → 最终主从
 demo.raft_groups('表')               | 每台 worker 在各组里的角色、任期、日志位点
 demo.roles('表')                     | 同一节点上的混合角色（节点 × 分片）
 demo.routing('表')                   | 路由三层：Citus 路由 / 控制面登记 / 节点本地
 demo.flow('表')                      | 流控：Raft 日志环 + 分区流捕获环
 demo.global_txn()                     | 在 BEGIN 之后调用：加入全局事务（跨分片写必须）
 demo.xid('表')                       | 分片级 xid 分配器：每个分片的下一个号、水位、与原生 xid 对比
 demo.clog('表', 'S1')                | 分片级 clog：每个分片 xid 的判决、start_ts、commit_ts、写了哪些行
 demo.versions('表', 'S1')            | 页面上的多版本：每个元组版本的 xmin/xmax 与判决
 demo.replay('表')                    | 惰性回放：从已收到 vs 已回放
 demo.catchup('表')                   | 触发回放，让每个从追平
 demo.compare('表')                   | 副本与主逐字节比对
 demo.switch_leader('表', 'S1', 'w2') | 手动切换 leader（实时打出切主过程）
 demo.crash('表', 'w2')               | 模拟宕机（immediate stop），看 Raft 自动选主
 demo.recover('表', 'w2')             | 拉起宕机节点，看它归队、被自动重新供给
(19 rows)
```

