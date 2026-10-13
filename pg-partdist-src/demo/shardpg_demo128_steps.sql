-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示二：128 个分区组上的分布式事务（只输入 SQL）
--
-- 场景：一张用户钱包表 wallet，按用户 id 哈希成 128 个分区；每个分区一个 Raft 组，
-- 副本因子 3（1 主 + 2 副本，按数据所在节点轮转放在另外两台 worker 上）——
-- 每个分区只存 3 份，而不是每台 worker 都存全部 128 份。
-- 谁当主：建组时由数据所在的那台（Citus 放分片的节点）先发起竞选当主 —— 别的成员此时还是空壳，
-- 没有数据的成员不能当主（候选人必须是完整的副本）。Citus 把 128 个分片轮流放到 4 台 worker 上，
-- 所以开局恰好每台 32 个主、64 个副本；之后切主、宕机都会让这个分布变化（第 6、7 步）。
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

-- ════ 第 1 步：灌数据 —— 钱包写进 128 个分区 ════
-- 台前现场灌 256 个钱包（1–256 号；256 号是平台红包账户，后面发红包就从它出）：
-- 一个全局事务，落到一百多个分区上；各组把自己那份日志复制到多数派，再由协调组写下提交决议（约 1 分钟）。
-- @A
\i /tmp/demo128_data.sql
-- 灌完的结果：台前现场灌的 + 准备阶段预灌的（以实际查询结果为准），以及余额总和（后面每一步都拿它核对）。
-- @A
SELECT count(*) FILTER (WHERE id <= 256) AS 台前现场灌的, count(*) FILTER (WHERE id > 256) AS 准备阶段预灌的,
       count(*) AS 合计, sum(balance) AS 余额总和 FROM wallet;

-- ════ 第 2 步：集群全貌与数据分布 ════
-- 4 台 worker、128 个分区组，每个分区 3 份（副本因子 3），不是每台都存一份。
-- 开局每台当 32 个分区的主：建组时由数据所在的那台先发起竞选当主（Citus 把 128 个分片轮流放到 4 台上），
-- 不是自由选举的结果 —— 没数据的成员是空壳，不能当主。"主分区行数"是这台当主的分区里一共有多少行。
-- @A
SELECT * FROM demo.cluster('wallet');
-- 128 个分区组逐个列出：分区标签 P001…P128 ↔ Citus 分片号一一对应；主、两个副本、Raft 任期。
-- @A
SELECT * FROM demo.groups('wallet');
-- 数据落在哪：按当主的节点汇总（分区数、行数、余额）。
-- @A
SELECT 节点, count(*) AS 主分区数, sum(行数) AS 行数, sum(余额合计) AS 余额合计
  FROM demo.distribution('wallet') GROUP BY 节点 ORDER BY 节点;

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
-- 发红包后：红包账户少 160，16 个用户各多 10。
-- @A
SELECT id, owner, balance FROM wallet WHERE id = 256 OR id BETWEEN 1 AND 16 ORDER BY id;
-- @A
SELECT * FROM demo.ledger('wallet');

-- ════ 第 5 步：并发事务与快照隔离（两个窗口） ════
-- 每个全局事务在 BEGIN 后由 demo.global_txn() 向 TSO 取一个 start_ts，整个事务的所有读都用这一个快照；
-- 提交时再取 commit_ts。一笔提交对某个事务可见，当且仅当 commit_ts < 该事务的 start_ts。
-- 幕 1：A 开一笔事务：用户 7 给新开户的用户 900001 转 50 元（UPDATE + INSERT），先不提交。
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE wallet SET balance = balance - 50 WHERE id = 7;
INSERT INTO wallet VALUES (900001, '新用户900001', 50);
-- 自见性：A 在自己的事务里看得见自己刚写的东西（7 号少了 50，900001 号已经在了）。
-- @A
SELECT id, owner, balance FROM wallet WHERE id IN (7, 255, 900001) ORDER BY id;
-- 幕 2：B 开一笔事务，拿到自己的 start_ts。A 还没提交：B 看到的 7 号是原来的余额，也没有 900001 号。
-- @B
BEGIN;
SELECT demo.global_txn();
SELECT id, owner, balance FROM wallet WHERE id IN (7, 255, 900001) ORDER BY id;
-- 幕 3：A 提交。用户 7 所在分区（P035）的分片 clog 里最新一条就是 A 这笔：它的 commit_ts 晚于 B 的 start_ts。
-- （提交判决是异步落进参与分区 clog 的，后台约半分钟清扫一次；这个查询会先触发一次清扫，等判决落定再列。）
-- @A
COMMIT;
-- @A
SELECT * FROM demo.part_clog('wallet', 'P035', 1);
-- 幕 4：A 已经提交了，B 再查一次 —— 仍然看不见：B 还在用自己的 start_ts，A 的 commit_ts 比它晚。
-- @B
SELECT id, owner, balance FROM wallet WHERE id IN (7, 255, 900001) ORDER BY id;
-- 幕 5：B 把 255 号销户（DELETE）并提交。255 号 A 没碰过，没有写冲突，B 正常提交。
-- @B
DELETE FROM wallet WHERE id = 255;
COMMIT;
-- 幕 6：A 开一个新查询 —— 新事务拿新的快照，A 的转账、新开户和 B 的销户全都看得见。
-- @A
SELECT id, owner, balance FROM wallet WHERE id IN (7, 255, 900001) ORDER BY id;
-- 总账：账户数 +1（开户）−1（销户）；余额总和 = 第 4 步之后的总和 − 255 号销户时的余额（转账本身不改变总额）。
-- @A
SELECT * FROM demo.ledger('wallet');

-- ════ 第 6 步：手动切主 —— 分片 xid 跟着主走，不回退、不重号 ════
-- 每个分区有自己的事务号（分片 xid），只由这个分区组的主发号；主按批（4096 个一批）预留号段，
-- 并把预留上界写进 Raft 日志。切主后新主不知道旧主在预留范围内实际发到了哪儿，所以从旧主预留上界
-- 之后的下一批接着发 —— 号只会往前跳，绝不会把旧主发过的号再发一遍。
-- 运维主动把用户 7 所在分区组（P035）的主切到同组另一台副本上（比如要下线原来那台做维护）。
-- @A
SELECT * FROM demo.locate('wallet', 7);
-- 切主前：P035 三个成员各自的角色、任期；主的"下一个分片 xid"和已预留上界。
-- @A
SELECT * FROM demo.part_xid('wallet', 'P035');
-- 切主：过程实时打出来（竞选 → 当选 → 控制面登记 → master 路由跟过去 → 旧主被新主重新供成副本）。
-- @A
SELECT * FROM demo.switch_group('wallet', 'P035');
-- 切主后：主换了台、任期 +1；新主的"下一个分片 xid"越过了旧主的已预留上界。
-- @A
SELECT * FROM demo.part_xid('wallet', 'P035');
-- 业务照常：用户 7 再给用户 200 转 30 元，写入落到新主上。
-- @A
BEGIN;
SELECT demo.global_txn();
UPDATE wallet SET balance = balance - 30 WHERE id = 7;
UPDATE wallet SET balance = balance + 30 WHERE id = 200;
COMMIT;
-- P035 分片 clog 的最近几笔（只列真正写过行的号）：最新一笔是新主发的号，跳过了旧主的整个预留范围，
-- 比切主前的每一笔都大；切主前那几笔的判决原样都在（新主的 clog 是从 Raft 日志复制来的）。
-- @A
SELECT * FROM demo.part_clog('wallet', 'P035', 4);
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
SELECT * FROM demo.ledger('wallet');
-- 把 w2 拉起来：它以 follower 身份回到各个组（主不抢回来）；它原来当主的那些分区，
-- 新主会在后台把它重新供成副本。
-- @A
SELECT * FROM demo.recover_worker('wallet');
-- @A
SELECT * FROM demo.cluster('wallet');
-- @A
SELECT * FROM demo.rejoin('wallet');
