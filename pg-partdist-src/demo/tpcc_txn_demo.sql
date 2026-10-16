-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 事务逻辑演示：沿用四步演示第 1 步建的 customer / orders（12 个客户，订单为空）
--
-- 开两个窗口，都连 master（:5432）：
--   docker exec -it -e PGCLIENTENCODING=UTF8 -u postgres pg-test-container \
--       /work/pg-install/bin/psql -h /tmp -p 5432 -U postgres
-- 每段前面的【窗口 A】/【窗口 B】表示这段在哪个窗口输入。
--
-- 一共三部分：1 边写边读 → 2 分布式事务 → 3 增删改查
-- ════════════════════════════════════════════════════════════════════════════

-- ════ 第 1 部分：不停地写，写的过程中读行数 ════
-- 【窗口 A】一个循环不停地下订单，每张订单单独提交（4 个仓库轮流写，每条只写一个分片）；
--           要跑几十秒，输入后不用等它，直接去窗口 B
DO $$
BEGIN
  FOR i IN 1..40 LOOP
    INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt)
    VALUES (1 + i % 4, 1, 100 + i, 1 + i % 2, 5);
    COMMIT;
  END LOOP;
END $$;
-- 【窗口 B】A 还在写的时候，每秒查一次订单数，查 15 次（只显示数据行）
\pset tuples_only on
SELECT now()::time(0) AS t, count(*) AS orders FROM orders \watch i=1 c=15
\pset tuples_only off
-- 【窗口 B】等窗口 A 显示 DO（写完了）再查：一共 40 张订单，4 个仓库各 10 张
SELECT o_w_id, count(*) AS orders FROM orders GROUP BY 1 ORDER BY 1;

-- ════ 第 2 部分：分布式事务（一个事务写多个分片） ════
-- 规则：一个事务只写一个分片时直接写；要写多个分片，BEGIN 之后先执行 SELECT shardpg.global_txn();
-- 2.1 准备：每个客户 1000 元。不带分片键的 UPDATE 会改到全部 4 个分片，所以放在全局事务里
-- 【窗口 A】
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = 1000;
COMMIT;
SELECT count(*) AS customers, sum(c_balance) AS total FROM customer;
-- 2.2 提交：仓库 1 的 Tom 转 300 元给仓库 2 的 Lucy，同时给 Tom 下一张订单（三个写操作，跨两个分片）
-- 【窗口 A】
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = c_balance - 300 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 300 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 1;
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 1, 1, 5);
COMMIT;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 ORDER BY 1;
SELECT sum(c_balance) AS total FROM customer;
-- 2.3 回滚：仓库 3 转 500 给仓库 4，事务里看得到修改，ROLLBACK 之后两边都恢复
-- 【窗口 A】
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = c_balance - 500 WHERE c_w_id = 3 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 500 WHERE c_w_id = 4 AND c_d_id = 1 AND c_id = 1;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (3, 4) ORDER BY 1;
ROLLBACK;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (3, 4) ORDER BY 1;
-- 2.4 中途出错：两笔余额已改，第三条插入的订单号重复 → 整个事务作废，COMMIT 实际执行的是 ROLLBACK
-- 【窗口 A】
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = c_balance - 200 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 2;
UPDATE customer SET c_balance = c_balance + 200 WHERE c_w_id = 3 AND c_d_id = 1 AND c_id = 2;
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 1, 2, 3);
COMMIT;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 2 AND c_w_id IN (1, 3) ORDER BY 1;
SELECT sum(c_balance) AS total FROM customer;
-- 2.5 隔离：A 改了还没提交，B 看不见；B 开着的事务看到的是开始那一刻的快照，A 提交后也不变
-- 【窗口 A】
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = c_balance - 100 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 100 WHERE c_w_id = 4 AND c_d_id = 1 AND c_id = 1;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
-- 【窗口 B】
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
BEGIN;
SELECT shardpg.global_txn();
SELECT sum(c_balance) AS total FROM customer;
-- 【窗口 A】
COMMIT;
-- 【窗口 B】B 的事务还开着：仍然看到旧值；B 提交后再查，看到 A 的修改
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
COMMIT;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
-- 2.6 写写冲突：A、B 同时改同一个客户，B 被挡住；A 提交后 B 报错（先提交者胜），B 只能重试
-- 【窗口 A】
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = c_balance + 7 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;
-- 【窗口 B】最后这条 UPDATE 会卡住（在等 A），不用等它，回到窗口 A
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = c_balance - 7 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;
-- 【窗口 A】提交
COMMIT;
-- 【窗口 B】卡住的 UPDATE 返回报错 could not serialize access due to concurrent update，回滚后看结果
ROLLBACK;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;
SELECT sum(c_balance) AS total FROM customer;

-- ════ 第 3 部分：增删改查 ════
-- 3.1 增：仓库 1 新来一个客户，给他下两张订单（客户和订单在同一仓库 → 同一个分片）
-- 【窗口 A】
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance) VALUES (1, 3, 1, 'Nina', 'He', 500);
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 3, 1, 1, 4), (1, 3, 2, 1, 6);
-- 3.2 查：按主键查一个客户；EXPLAIN 看它只发给一个节点（Task Count: 1）
SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
-- 查一个仓库的全部客户（同一个分片内）
SELECT c_d_id, c_id, c_first, c_last, c_balance FROM customer WHERE c_w_id = 1 ORDER BY 1, 2;
-- 跨仓库统计：4 个分片并行算，master 汇总（Task Count: 4）
SELECT c_w_id, count(*) AS customers, sum(c_balance) AS balance FROM customer GROUP BY 1 ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT c_w_id, count(*) FROM customer GROUP BY 1;
-- 客户和订单连接：两表按仓库共置，连接在各 worker 本地完成
SELECT c.c_w_id, c.c_d_id, c.c_id, c.c_first, count(o.o_id) AS orders
  FROM customer c
  JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3;
-- 3.3 改：改一个客户（单分片，直接改）
UPDATE customer SET c_balance = c_balance + 50, c_last = 'Huang' WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
SELECT c_w_id, c_d_id, c_id, c_first, c_last, c_balance FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
-- 一次改多个仓库：要放进全局事务
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_last = upper(c_last) WHERE c_d_id = 2;
COMMIT;
SELECT c_w_id, c_d_id, c_id, c_first, c_last FROM customer WHERE c_d_id = 2 ORDER BY 1;
-- 3.4 删：删一张订单，再删这个客户的其余订单和客户本人（都在仓库 1 的分片里）
DELETE FROM orders WHERE o_w_id = 1 AND o_d_id = 3 AND o_id = 2;
DELETE FROM orders WHERE o_w_id = 1 AND o_d_id = 3 AND o_c_id = 1;
DELETE FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
SELECT count(*) AS customers FROM customer;
SELECT count(*) AS orders FROM orders WHERE o_d_id = 3;
