-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示二：128 个分区组上的分布式事务（只输入 SQL）
--
-- 场景：一张用户钱包表 wallet，按用户 id 哈希成 128 个分区；每个分区一个 Raft 组，
-- 副本因子 3（1 主 + 2 副本，按数据所在节点轮转放在另外两台 worker 上）——
-- 每台 worker 大约当 32 个分区的主、再给 64 个分区当副本，而不是每台都存全部 128 份。
--
-- 前提：bash shardpg_demo128.sh start 已经把 wallet 表建好、128 个组选好主、副本供好、
-- 打标接入事务系统 —— 这些都不在台前，演示从"看集群全貌"开始。
--
-- 两个窗口都连到 master（:5432）：窗口 A（提示符 A>）、窗口 B（提示符 B>）。
-- 每段前面的 "-- @A" / "-- @B" 表示在哪个窗口输入；"-- @B &" 表示这条会被挡住、
-- 先别等它；"-- @B wait" 表示回到 B 窗口看它刚才被挡住的那条语句的结果。
--
-- 术语：master 只做路由 + TSO，不放数据、不领导任何数据组；
--       「协调者」是事务的概念 —— 每个分布式事务在自己写过的分区组里挑一个当协调组，
--       提交决议写进协调组的 Raft 日志，达多数派即全局提交点。
-- ════════════════════════════════════════════════════════════════════════════

-- ════ 第 1 步：集群全貌 ════
-- 4 台 worker、128 个分区组。每台当约 32 个分区的主，另给约 64 个分区当副本 ——
-- 每个分区只有 3 份（副本因子 3），不是每台都存一份。
-- @A
SELECT * FROM demo.cluster('wallet');
-- 128 个分区组逐个列出：分区标签 P001…P128 ↔ Citus 分片号一一对应；主、两个副本、Raft 任期。
-- @A
SELECT * FROM demo.groups('wallet');

-- ════ 第 2 步：灌数据 —— 一笔事务写到一百多个分区 ════
-- 数据在 demo128_data.sql 里：256 个钱包（最后一个是平台红包账户），一个全局事务一次写入。
-- 256 个键哈希到 128 个分区，落到其中一百多个；提交时这些分区组一起做两阶段提交：
-- 各组把自己那份日志复制到多数派，再由协调组写下提交决议（这台 2 vCPU 机器上约 1 分钟）。
-- @A
\i /tmp/demo128_data.sql
-- @A
SELECT * FROM demo.last_txn('wallet');
-- @A
SELECT * FROM demo.ledger('wallet');
-- @A
SELECT * FROM demo.cluster('wallet');

-- ════ 第 3 步：跨分区转账 ════
-- 用户 7 给用户 200 转 100 元。两个钱包在不同分区、主在不同的 worker 上。
-- @A
SELECT * FROM demo.locate('wallet', 7, 200);
-- 转账前的余额：
-- @A
SELECT id, owner, balance FROM wallet WHERE id IN (7, 200) ORDER BY id;
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE wallet SET balance = balance - 100 WHERE id = 7;
UPDATE wallet SET balance = balance + 100 WHERE id = 200;
COMMIT;
-- 这笔事务的两阶段提交账本：两个参与分区组，其中一个当协调组。
-- @A
SELECT * FROM demo.last_txn('wallet');
-- 转账后的余额：用户 7 少 100，用户 200 多 100。
-- @A
SELECT id, owner, balance FROM wallet WHERE id IN (7, 200) ORDER BY id;

-- ════ 第 4 步：一笔事务涉及 16 个分区 ════
-- 平台红包账户（256 号）给 16 个用户各发 10 元：一笔事务，17 个钱包，分布在十几个分区组上。
-- 要么全发出去，要么一个都不发；总额守恒。
-- 发红包前：红包账户和这 16 个用户的余额。
-- @A
SELECT id, owner, balance FROM wallet WHERE id = 256 OR id BETWEEN 1 AND 16 ORDER BY id;
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE wallet SET balance = balance - 160 WHERE id = 256;
UPDATE wallet SET balance = balance + 10 WHERE id BETWEEN 1 AND 16;
COMMIT;
-- @A
SELECT * FROM demo.last_txn('wallet');
-- 发红包后：红包账户少 160，16 个用户各多 10。
-- @A
SELECT id, owner, balance FROM wallet WHERE id = 256 OR id BETWEEN 1 AND 16 ORDER BY id;
-- @A
SELECT * FROM demo.ledger('wallet');

-- ════ 第 5 步：并发事务（两个窗口） ════
-- 幕 1：A 开一笔转账（用户 7 → 用户 150，50 元），先不提交。
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE wallet SET balance = balance - 50 WHERE id = 7;
UPDATE wallet SET balance = balance + 50 WHERE id = 150;
-- 幕 2：B 同时开一笔事务：看不见 A 没提交的转账（快照隔离）。
-- @B
BEGIN;
SELECT demo.global_txn();
SELECT id, owner, balance FROM wallet WHERE id IN (7, 150) ORDER BY id;
-- 幕 3：B 也要动用户 150 的钱包 —— 和 A 写同一行，B 被挡住。
-- @B &
UPDATE wallet SET balance = balance - 20 WHERE id = 150;
-- 幕 4：A 提交。B 的等待结束，但它被中止：先提交者胜，B 只能重试。
-- @A
COMMIT;
-- @B wait
-- @B
ROLLBACK;
-- 幕 5：总额守恒。
-- @A
SELECT * FROM demo.ledger('wallet');

-- ════ 第 6 步：手动切主 ════
-- 运维主动把用户 7 所在分区组（P035）的主切到同组另一台副本上（比如要下线原来那台做维护）。
-- 过程实时打出来：目标副本发起竞选 → 当选 → 控制面登记 → master 路由跟过去 → 旧主被新主重新供成副本。
-- @A
SELECT * FROM demo.locate('wallet', 7);
-- @A
SELECT * FROM demo.switch_group('wallet', 'P035');
-- 切完以后：该组的主换了台、任期 +1。
-- @A
SELECT * FROM demo.groups('wallet') WHERE 分区 = 'P035';
-- 业务照常：用户 7 再给用户 200 转 30 元，写入落到新主上；余额与总额都对得上。
-- @A
SELECT id, owner, balance FROM wallet WHERE id IN (7, 200) ORDER BY id;
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE wallet SET balance = balance - 30 WHERE id = 7;
UPDATE wallet SET balance = balance + 30 WHERE id = 200;
COMMIT;
-- @A
SELECT id, owner, balance FROM wallet WHERE id IN (7, 200) ORDER BY id;
-- @A
SELECT * FROM demo.ledger('wallet');

-- ════ 第 7 步：一台 worker 断电 ════
-- 直接停掉 w2（immediate：不做 checkpoint，等同断电）。它当主的那三十来个分区组，
-- 各自在剩下的两个成员里重新选主、登记到 master 的路由；其余分区不受影响。
-- @A
SELECT * FROM demo.crash_worker('wallet', 'w2');
-- 断电期间照常做跨分区事务（再发一轮红包，这次 32 个用户）。
-- @B
BEGIN;
SELECT demo.global_txn();
UPDATE wallet SET balance = balance - 320 WHERE id = 256;
UPDATE wallet SET balance = balance + 10 WHERE id BETWEEN 101 AND 132;
COMMIT;
-- @B
SELECT * FROM demo.last_txn('wallet');
-- @B
SELECT * FROM demo.ledger('wallet');
-- 把 w2 拉起来：它以 follower 身份回到各个组（主不抢回来）；它原来当主的那些分区，
-- 新主会在后台把它重新供成副本。
-- @A
SELECT * FROM demo.recover_worker('wallet');
-- @A
SELECT * FROM demo.cluster('wallet');
-- @A
SELECT * FROM demo.rejoin('wallet');
