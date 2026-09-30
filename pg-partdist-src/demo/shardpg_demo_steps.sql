-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示：5 个节点上的一张业务表（只输入 SQL）
--
-- 前提：start 已经把 account 表建好（4 个分片，一台 worker 一片），并完成了建组、自发选主、
-- 供副本、打标接入事务系统 —— 这些都不在台前，演示从"灌业务数据"开始。
--
-- 两个窗口都连到 master（:5432）：窗口 A（提示符 A>）、窗口 B（提示符 B>）。
-- 每段前面的 "-- @A" / "-- @B" 表示在哪个窗口输入；"-- @B &" 表示这条会被挡住、
-- 先别等它；"-- @B wait" 表示回到 B 窗口看它刚才被挡住的那条语句的结果。
--
-- 术语：master 只做路由 + TSO，不放数据、不领导任何数据组；
--       「协调者」是事务的概念 —— 每个全局事务在自己的写集里挑一个分片组当协调组。
-- ════════════════════════════════════════════════════════════════════════════

-- ════ 第 1 步：灌业务数据 ════
-- 数据在 demo_data.sql 里（500 个账户，分 10 批，每批一个全局事务），一条命令读进来。
-- 慢是设计使然：打标表的每条记录都要 propose 给该分片的 Raft 组、等多数派落盘（约 2 行/秒）。
-- @A
\i /tmp/demo_data.sql

-- ════ 第 2 步：数据被自动分到了哪几台 ════
-- 建表时就是 4 个分片，Citus 按哈希把每个键落到其中一个；下面直接读各 worker 上的分片表。
-- @A
SELECT * FROM demo.distribution('account');
-- @A
SELECT count(*) AS 账户数, sum(balance) AS 余额总和 FROM account;

-- ════ 第 3 步：各分区的副本信息 ════
-- 每个分片 1 个主 + 3 个副本（分布在其余三台）。副本经 Raft 把主的分区流收下落盘，
-- 但不马上 redo（惰性回放）：“已收到”是收下的日志位点，“已回放”是真正 redo 到哪，差值就是待回放。
-- @A
SELECT * FROM demo.replicas('account');

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
