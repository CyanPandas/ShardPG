-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示：5 个节点上的一张业务表（只输入 SQL）
--
-- 两个窗口都连到 master（:5432）：窗口 A（提示符 A>）、窗口 B（提示符 B>）。
-- 每段前面的 "-- @A" / "-- @B" 表示在哪个窗口输入；"-- @B &" 表示这条会被挡住、
-- 先别等它；"-- @B wait" 表示回到 B 窗口看它刚才被挡住的那条语句的结果。
--
-- 术语：master 只做路由 + TSO，不放数据、不领导任何数据组；
--       「协调者」是事务的概念 —— 每个全局事务在自己的写集里挑一个分片组当协调组。
-- ════════════════════════════════════════════════════════════════════════════

-- ════ 第 1 步：集群 ════
-- 1 个 master + 4 个 worker。控制面是 0 号 Raft 组（5 个节点都在里面），
-- 它登记"每个分片的主是谁"，master 的路由就按这份登记走。
-- @A
SELECT * FROM demo.nodes();

-- ════ 第 2 步：建一张业务表，并把它接入系统 ════
-- 建表就是 4 个分片，Citus 按哈希把它们分到 4 台 worker —— 应用只面对一张 account 表。
-- 然后一条命令完成三件事：每个分片建一个 Raft 组（成员 = 全部 4 台 worker）→ 自发选主 → 供副本 →
-- 打标接入事务系统（分片级 xid + 分片级 clog + TSO 时间戳）。
-- 打标必须在写入之前做：分片得是空的。
-- @A
SET citus.shard_count = 4;
CREATE TABLE account (id int PRIMARY KEY, owner text NOT NULL, balance int NOT NULL DEFAULT 0)
    WITH (autovacuum_enabled = off);
SELECT create_distributed_table('account', 'id', colocate_with => 'none');
-- @A
SELECT * FROM demo.setup('account');

-- ════ 第 3 步：灌业务数据，看系统自己把它分到哪 ════
-- 数据在 demo_data.sql 里（500 个账户，分 10 批，每批一个全局事务）。
-- 慢是设计使然：打标表的每条记录都要 propose 给该分片的 Raft 组、等多数派落盘。
-- @A
\i /tmp/demo_data.sql
-- @A
SELECT * FROM demo.distribution('account');
-- @A
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;

-- ════ 第 4 步：并发事务 ════
-- 幕 1：A、B 各开一个全局事务，各看一眼总额。
-- @A
BEGIN;
SELECT demo.global_txn();
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
-- @B
BEGIN;
SELECT demo.global_txn();
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
-- 幕 2：A 做一笔跨分片转账（两个账户在不同节点上），钱从一边到另一边，总额不变。
-- @A
UPDATE account SET balance = balance - 300 WHERE id = 1;
UPDATE account SET balance = balance + 300 WHERE id = 2;
SELECT id, owner, balance FROM account WHERE id IN (1, 2) ORDER BY id;
-- 幕 3：B 看不见 A 没提交的转账（快照隔离）。
-- @B
SELECT id, owner, balance FROM account WHERE id IN (1, 2) ORDER BY id;
-- 幕 4：A 提交；B 在自己的事务里仍然看不见（B 的 start_ts 早于 A 的 commit_ts）。
-- @A
COMMIT;
-- @B
SELECT id, owner, balance FROM account WHERE id IN (1, 2) ORDER BY id;
-- @B
COMMIT;
-- 幕 5：写写冲突 —— A 改 id=3 不提交，B 也去改它，B 被挡住。
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 7 WHERE id = 3;
-- @B &
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance - 7 WHERE id = 3;
-- A 提交后 B 的等待结束，但它被中止：先提交者胜，B 只能重试。
-- @A
COMMIT;
-- @B wait
-- @B
ROLLBACK;
-- 幕 6：总额守恒（跨分片转账 + 一笔被中止的写，钱一分没多一分没少）。
-- @A
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;

-- ════ 第 5 步：手动切换 leader ════
-- 把 S1 的主切到它组内的另一台：选主过程实时打出来，切完 master 的路由自动跟过去，
-- 数据照读、总额不变。
-- @A
SELECT * FROM demo.switch_leader('account', 'S1');
-- @A
SELECT * FROM demo.roles('account');
-- @A
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;

-- ════ 第 6 步：不可抗力宕机 ════
-- 直接停掉 S1 当前的主（immediate：不做 checkpoint，等同断电）。
-- 该分片的组超时、自发选主；没有这个分片数据的成员即使当选也会主动让位。
-- @A
SELECT * FROM demo.crash('account');
-- @A
SELECT * FROM demo.roles('account');
-- 宕机期间照常读写。
-- @B
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
-- @B
BEGIN;
SELECT demo.global_txn();
UPDATE account SET balance = balance + 1 WHERE id IN (1, 2, 3);
COMMIT;
-- 把节点拉起来：它以 follower 身份归队，原来当主的分片被新主自动重新供给成副本。
-- @A
SELECT * FROM demo.recover('account');
-- @A
SELECT * FROM demo.roles('account');
-- @A
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;
