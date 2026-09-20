-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示脚本：只有 SQL 和事务
--
-- 两个窗口都连到 master（:5432）：窗口 A（提示符 A>）、窗口 B（提示符 B>）。
-- 术语：master 只做路由 + TSO，不放数据、不领导任何数据组；
--       「协调者」是事务的概念 —— 每个全局事务在自己的写集里选一个分片组当协调组。
-- 每一段前面的 "-- @A" / "-- @B" 表示在哪个窗口里输入；"-- @B &" 表示这条会被挡住、先别等它，
-- "-- @B wait" 表示回到 B 窗口看它刚才被挡住的那条语句的结果。
-- 过程性的东西（跨节点查询、建组、切主、宕机……）都已封装在 demo.* 函数里（shardpg_demo_functions.sql）。
-- ════════════════════════════════════════════════════════════════════════════

-- ════ 第 1 步：集群里的 4 个节点 ════
-- 1 个 master + 3 个 worker（w1 w2 w3）。每个节点有一个 pg_raft 节点号；
-- 控制面是 0 号 Raft 组（4 个节点都在里面），它登记"每个分片的主是谁"。
-- @A
SELECT * FROM demo.nodes();

-- ════ 第 2 步：建一张跨节点的表，看每个分片在哪个节点 ════
-- 3 个分片，Citus 按哈希把它们分别放到 3 台 worker 上；每个键落在哪个分片由哈希决定。
-- @A
SET citus.shard_count = 3;
CREATE TABLE account (id int PRIMARY KEY, owner text NOT NULL, balance int NOT NULL DEFAULT 0)
    WITH (autovacuum_enabled = off);
SELECT create_distributed_table('account', 'id', colocate_with => 'none');
-- @A
SELECT * FROM demo.shards('account');
-- @A
SELECT k AS 键, l.分片, l.当前主节点 FROM generate_series(1, 9) k, demo.locate('account', k) l;

-- ════ 第 3 步：每个分片自动建一个 Raft 组，自发选主（全过程实时打出来） ════
-- 每个分片一个 Raft 组，成员 = 3 台 worker。**不做任何人工干预**：建完组三台各自倒计时，
-- 谁的选举超时先到点谁竞选（任期 +1、向另外两台要票、拿到多数票当选），和真实环境一样。
-- 此刻数据只在分片原来那台上，另外两台是空的；空的那台也可能先当选 —— 它做升主前置时
-- 会发现自己没有这个分片的副本，于是拒绝升主、主动让位并退避 5 个选举周期，把机会让给别人。
-- 直到有数据的那台当选，才会到控制面登记"我是这个分片的主"，Citus 路由随之指向它。
-- （结果表里的"选举轮次"= 任期：1 = 一次选中，>1 = 中间有人当选后被拒、让了位。）
-- @A
SELECT * FROM demo.raft_elect('account');

-- ════ 第 4 步：供副本 → 最终的主和从 ════
-- 在每个组的 leader 上把副本供到另外两台：发一份物理基线进分区流（经 Raft 复制），
-- 目标节点按基线配对文件号、arm 回放槽位。之后每个分片 = 1 主（leader）+ 2 从（follower）。
-- @A
SELECT * FROM demo.raft_replicas('account');
-- @A
SELECT * FROM demo.raft_groups('account');

-- ════ 第 5 步：同一个节点上既有 leader 又有 follower ════
-- 行是节点、列是分片：每台 worker 都是一个分片的 leader、另外两个分片的 follower。
-- @A
SELECT * FROM demo.roles('account');

-- ════ 第 6 步：接入事务系统（打标） ════
-- 打标之后，这张表的分片才走"分片级 xid + 分片级 clog + TSO 时间戳"的事务机制。
-- @A
SELECT * FROM partdist.set_table_shard_mvcc('account');

-- ════ 第 7 步：路由信息 ════
-- 三层：① Citus 路由表决定经 master 的读写发往哪个节点；② 控制面登记（0 号 Raft 组）记录每个分片的主从；
-- ③ 每个节点本地知道自己对这个分片是主（写入被捕获进分区流）还是从（只收流）。
-- @A
SELECT * FROM demo.routing('account');

-- ════ 第 8 步：增删改查 ════
-- 只落一个分片的写走 1PC，直接写；跨分片的写要走 2PC，必须先加入全局事务（取全局事务号 + TSO 快照时间戳）。
-- @A
INSERT INTO account VALUES (1, 'alice', 1000);
-- 不加入全局事务就跨分片写：被拒（2PC 的 PREPARE 不放行）。
-- @A
INSERT INTO account VALUES (2, 'bob', 1000), (3, 'carol', 1000);
-- 加入全局事务后跨分片插入：一条语句写 3 个分片。
-- @A
BEGIN;
SELECT demo.global_txn();
INSERT INTO account VALUES (2, 'bob', 1000), (3, 'carol', 1000), (4, 'dave', 1000),
                           (5, 'erin', 1000), (6, 'frank', 1000), (9, 'ivy', 1000);
COMMIT;
-- @A
SELECT * FROM account ORDER BY id;
-- 单分片的改和删；再做一次跨分片的改和删。
-- @A
UPDATE account SET balance = balance - 100 WHERE id = 1;
DELETE FROM account WHERE id = 5;
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 100 WHERE id IN (2, 3);
DELETE FROM account WHERE id IN (4, 9);
COMMIT;
-- @A
SELECT * FROM account ORDER BY id;

-- ════ 第 9 步：分片级 xid 分配器与分片级 clog ════
-- 每个分片有自己的 xid 分配器（从 3 起发），元组头里的 xmin/xmax 存的是分片 xid，与节点原生 xid 无关；
-- 每个分片有自己的一本 clog：分片 xid → 判决 + start_ts + commit_ts。
-- @A
SELECT * FROM demo.xid('account');
-- @A
SELECT * FROM demo.clog('account', 'S1');
-- @A
SELECT * FROM demo.versions('account', 'S1');

-- ════ 第 10 步：流控信息 ════
-- Raft 日志环：每组 128 条，环深度 = 已追加未应用；环满先背压等待、超时才丢弃。
-- 分区流捕获环：主上写入先进捕获环再进分区流；覆盖次数 > 0 才说明有记录没进流。
-- @A
SELECT * FROM demo.flow('account');

-- ════ 第 11 步：惰性回放 ════
-- 从节点经 Raft 把主的分区流收下落盘，但不马上 redo（armed ≠ 正在回放）；有人触发才追平。
-- @A
SELECT * FROM demo.replay('account');
-- @A
SELECT * FROM demo.catchup('account');
-- @A
SELECT * FROM demo.replay('account');
-- 追平之后，副本页面与主逐字节一致（按内核 heap_mask 口径）。
-- @A
SELECT * FROM demo.compare('account');

-- ════ 第 12 步：两个窗口并发做事务（跨分片插入、跨分片删除、快照隔离、写写冲突） ════
-- 幕 1：A、B 各开一个全局事务，各看一眼（两边都有一个 start_ts）。
-- @A
BEGIN;
SELECT demo.global_txn();
SELECT * FROM account ORDER BY id;
-- @B
BEGIN;
SELECT demo.global_txn();
SELECT * FROM account ORDER BY id;
-- 幕 2：A 跨分片插入 3 行（落在 3 个分片），再跨分片改 2 行；A 自己看得见。
-- @A
INSERT INTO account VALUES (7, 'grace', 500), (19, 'kate', 500), (11, 'judy', 500);
UPDATE account SET balance = balance - 50 WHERE id IN (2, 3);
SELECT * FROM account ORDER BY id;
-- 幕 3：B 看不见 A 没提交的改动。
-- @B
SELECT * FROM account ORDER BY id;
-- 幕 4：A 提交；B 在自己的事务里仍然看不见（快照隔离：B 的 start_ts 早于 A 的 commit_ts）。
-- @A
COMMIT;
-- @B
SELECT * FROM account ORDER BY id;
-- 幕 5：B 跨分片删除 2 行并提交。
-- @B
DELETE FROM account WHERE id IN (1, 6);
COMMIT;
-- 幕 6：A 新开的查询是新快照：A 的插入/修改、B 的删除全部可见。
-- @A
SELECT * FROM account ORDER BY id;
-- 幕 7：写写冲突。A 改 id=2（不提交）；B 也去删 id=2 —— B 被行锁挡住。
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 7 WHERE id IN (2, 11);
-- @B &
BEGIN;
SELECT demo.global_txn();
DELETE FROM account WHERE id = 2;
-- A 提交后，B 的等待结束，但它被中止：先提交者胜（first-committer-wins），B 只能重试。
-- @A
COMMIT;
-- @B wait
-- @B
ROLLBACK;
-- 幕 8：分片 clog 上的整段故事（A 的号已提交、B 被中止的号是 ABORTED）。
-- @A
SELECT * FROM demo.clog('account', 'S3');

-- ════ 第 13 步：手动切换 leader ════
-- 把 S1 的主从 w1 切到 w2（w2 本来就是 S2 的主）：在 w2 上对 S1 的组发起竞选，过程实时打出来。
-- 切完 w2 同时当两个分片的主，旧主 w1 被新主自动重新供给成副本；新主的分片 xid 接着旧主的号往下发。
-- @A
SELECT * FROM demo.switch_leader('account', 'S1', 'w2');
-- @A
SELECT * FROM demo.roles('account');
-- @A
INSERT INTO account VALUES (8, 'heidi', 800);
SELECT * FROM demo.xid('account');

-- ════ 第 14 步：模拟不可抗力宕机 ════
-- 直接把 w2 停掉（immediate：不做 checkpoint，等同断电）。它此刻是 S1、S2 两个分片的主。
-- 两个组各自超时、各自选主；w2 只当从的 S3 不受影响。
-- （停机前函数会先等副本确认最新一笔提交，原因见教程"已知问题"第 1 条。）
-- @A
SELECT * FROM demo.crash('account', 'w2');
-- @A
SELECT * FROM demo.roles('account');
-- 宕机期间照常读写（在 B 窗口）。
-- @B
SELECT * FROM account ORDER BY id;
INSERT INTO account VALUES (10, 'leo', 300);
INSERT INTO account VALUES (12, 'mia', 300);
-- @B
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 1 WHERE id IN (3, 8, 10);
COMMIT;
-- 把 w2 拉起来：它以 follower 身份回到 3 个组，原来当主的分片被新主自动重新供给成副本。
-- @A
SELECT * FROM demo.recover('account', 'w2');
-- @A
SELECT * FROM demo.roles('account');
-- @A
SELECT * FROM demo.catchup('account');
-- @A
SELECT * FROM demo.compare('account');
-- @A
SELECT * FROM account ORDER BY id;
