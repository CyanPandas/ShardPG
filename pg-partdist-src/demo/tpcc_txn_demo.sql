-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 事务逻辑演示：在四步演示第 1 步建好的 customer / orders 上直接做，不清空、不重建
--
-- 开三个窗口，都连 master（:5432）：
--   docker exec -it -e PGCLIENTENCODING=UTF8 -u postgres pg-test-container \
--       /work/pg-install/bin/psql -h /tmp -p 5432 -U postgres
--   窗口 W：持续写入，从第 1 部分开始一直写到演示结束
--   窗口 A、窗口 B：其余操作
-- 每段前面的【窗口 W】/【窗口 A】/【窗口 B】表示这段在哪个窗口输入。
--
-- 一共三部分：1 开始持续写入、边写边读 → 2 分布式事务 → 3 增删改查（2、3 进行时写入一直不停）
-- ════════════════════════════════════════════════════════════════════════════

-- ════ 第 1 部分：开始持续写入，边写边读 ════
-- 【窗口 W】不停地下订单，每张订单单独提交（4 个仓库轮流写，每条只写一个分片），订单号接着表里已有的往下编。
--           启动后它会一直写，不用等它；演示结束时在这个窗口按 Ctrl+C 停掉
DO $$
DECLARE
  base int;
BEGIN
  SELECT coalesce(max(o_id), 0) INTO base FROM orders WHERE o_d_id = 9;
  FOR i IN 1..1000000 LOOP
    INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt)
    VALUES (1 + i % 4, 9, base + i, 1 + i % 2, 5);
    COMMIT;
    PERFORM pg_sleep(0.2);
  END LOOP;
END $$;
-- 【窗口 B】写入进行中，每秒查一次订单总数，查 10 次（只显示数据行）
\pset tuples_only on
SELECT now()::time(0) AS t, count(*) AS orders FROM orders \watch i=1 c=10
\pset tuples_only off

-- ════ 第 2 部分：分布式事务（一个事务写多个分片），窗口 W 仍在写入 ════
-- 一个事务写多个分片时，系统自动把它作为一个全局事务（统一的事务号和快照时间戳），写法和普通 PostgreSQL 一样
-- 【窗口 B】写入还在继续
SELECT count(*) AS orders FROM orders;
-- 2.1 准备：每个客户 1000 元。不带分片键的 UPDATE 会改到全部 4 个分片，一条语句就是一个全局事务
-- 【窗口 A】
UPDATE customer SET c_balance = 1000;
SELECT count(*) AS customers, sum(c_balance) AS total FROM customer;
-- 2.2 提交：仓库 1 的 Tom 转 300 元给仓库 2 的 Lucy，同时给 Tom 下一张订单（三个写操作，跨两个分片）
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 300 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 300 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 1;
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 1, 1, 5);
COMMIT;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 ORDER BY 1;
SELECT sum(c_balance) AS total FROM customer;
-- 2.3 回滚：仓库 3 转 500 给仓库 4，事务里看得到修改，ROLLBACK 之后两边都恢复
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 500 WHERE c_w_id = 3 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 500 WHERE c_w_id = 4 AND c_d_id = 1 AND c_id = 1;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (3, 4) ORDER BY 1;
ROLLBACK;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (3, 4) ORDER BY 1;
-- 2.4 中途出错：两笔余额已改，第三条插入的订单号重复 → 整个事务作废，COMMIT 实际执行的是 ROLLBACK
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 200 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 2;
UPDATE customer SET c_balance = c_balance + 200 WHERE c_w_id = 3 AND c_d_id = 1 AND c_id = 2;
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 1, 2, 3);
COMMIT;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 2 AND c_w_id IN (1, 3) ORDER BY 1;
SELECT sum(c_balance) AS total FROM customer;
-- 2.5 隔离：A 改了还没提交，B 看不见；B 开着的事务看到的是开始那一刻的快照，A 提交后也不变
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 100 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 100 WHERE c_w_id = 4 AND c_d_id = 1 AND c_id = 1;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
-- 【窗口 B】
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
BEGIN;
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
UPDATE customer SET c_balance = c_balance + 7 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;
-- 【窗口 B】最后这条 UPDATE 会卡住（在等 A），不用等它，回到窗口 A
BEGIN;
UPDATE customer SET c_balance = c_balance - 7 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;
-- 【窗口 A】提交
COMMIT;
-- 【窗口 B】卡住的 UPDATE 返回报错 could not serialize access due to concurrent update，回滚后看结果
ROLLBACK;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;
SELECT sum(c_balance) AS total FROM customer;

-- ════ 第 3 部分：增删改查，窗口 W 仍在写入 ════
-- 【窗口 B】写入还在继续
SELECT count(*) AS orders FROM orders;
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
-- 一次改多个仓库（4 个分片）：直接改
UPDATE customer SET c_last = upper(c_last) WHERE c_d_id = 2;
SELECT c_w_id, c_d_id, c_id, c_first, c_last FROM customer WHERE c_d_id = 2 ORDER BY 1;
-- 3.4 删：删一张订单，再删这个客户的其余订单和客户本人（都在仓库 1 的分片里）
DELETE FROM orders WHERE o_w_id = 1 AND o_d_id = 3 AND o_id = 2;
DELETE FROM orders WHERE o_w_id = 1 AND o_d_id = 3 AND o_c_id = 1;
DELETE FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
SELECT count(*) AS customers FROM customer;
SELECT count(*) AS orders FROM orders WHERE o_d_id = 3;

-- ════ 结束：停掉持续写入 ════
-- 【窗口 W】按 Ctrl+C 停止循环（提示 canceling statement due to user request；已经提交的订单都保留）
-- 【窗口 B】整场演示期间写入的订单，按仓库统计；以及订单总数
SELECT o_w_id, count(*) AS orders, min(o_id), max(o_id) FROM orders WHERE o_d_id = 9 GROUP BY 1 ORDER BY 1;
SELECT count(*) AS orders FROM orders;
