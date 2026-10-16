-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 事务逻辑演示：在四步演示第 1 步建好的 customer / orders 上直接做，不清空、不重建
--
-- 一个终端 + 两个窗口：
--   终端 W：宿主机上的 shell，跑持续写入脚本 txn_demo_writer.sh —— 演示开始前启动，演示全部结束后按 Ctrl+C 停
--   窗口 A、窗口 B：都连 master（:5432）
--       docker exec -it -e PGCLIENTENCODING=UTF8 -u postgres pg-test-container \
--           /work/pg-install/bin/psql -h /tmp -p 5432 -U postgres
-- 每段前面的【终端 W】/【窗口 A】/【窗口 B】表示这段在哪里做。
--
-- 写入脚本轮流做三种写入，每条都是不开事务的单条 INSERT，每条数据都不同：
--   ① 只插客户表  ② 只插订单表（给它建过的客户下单）  ③ 新客户 + 他的订单（两条 INSERT，同一仓库）
-- 它只写 9 区（c_d_id / o_d_id = 9）；演示里操作的是 1、2 区的 12 个客户（总额 12000），两边互不干扰。
--
-- 一共三部分：1 边写边读 → 2 分布式事务 → 3 增删改查（写入脚本一直在跑）
-- ════════════════════════════════════════════════════════════════════════════

-- ════ 开始之前：启动持续写入 ════
-- 【终端 W】启动写入脚本，每完成一条打一行；让它一直跑，不用等它
--           bash ~/shardpg-ddl-work/pg-partdist-src/demo/txn_demo_writer.sh

-- ════ 第 1 部分：边写边读 ════
-- 【窗口 B】写入进行中，每秒查一次客户数和订单数，查 10 次（只显示数据行）：两个数一直在涨
\pset tuples_only on
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o \watch i=1 c=10
\pset tuples_only off

-- ════ 第 2 部分：分布式事务（一个事务写多个分片），写入脚本仍在跑 ════
-- 一个事务写多个分片时，系统自动把它作为一个全局事务（统一的事务号和快照时间戳），写法和普通 PostgreSQL 一样
-- 【窗口 B】写入还在继续
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o;
-- 2.1 准备：12 个演示客户每人 1000 元。不带分片键的 UPDATE 会改到全部 4 个分片，一条语句就是一个全局事务
-- 【窗口 A】
UPDATE customer SET c_balance = 1000 WHERE c_d_id IN (1, 2);
SELECT count(*) AS customers, sum(c_balance) AS total FROM customer WHERE c_d_id IN (1, 2);
-- 2.2 提交：仓库 1 的 Tom 转 300 元给仓库 2 的 Lucy，同时给 Tom 下一张订单（三个写操作，跨两个分片）
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 300 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 300 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 1;
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 1, 1, 5);
COMMIT;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 ORDER BY 1;
SELECT sum(c_balance) AS total FROM customer WHERE c_d_id IN (1, 2);
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
SELECT sum(c_balance) AS total FROM customer WHERE c_d_id IN (1, 2);
-- 2.5 隔离：A 改了还没提交，B 看不见；B 开着的事务只看开始那一刻的快照 ——
--     A 提交了、写入脚本也一直在插，B 的事务里查到的都不变
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 100 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 100 WHERE c_w_id = 4 AND c_d_id = 1 AND c_id = 1;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
-- 【窗口 B】A 还没提交：看到的是旧值；然后开一个事务，记下此刻的客户数和订单数
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
BEGIN;
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o;
-- 【窗口 A】
COMMIT;
-- 【窗口 B】等几秒（写入脚本又插了几条）再查：B 的事务还开着，A 的转账看不到，新插的客户和订单也看不到
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o;
COMMIT;
-- 【窗口 B】B 提交后再查：看到 A 的修改，行数也跟上了
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY 1;
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o;
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
SELECT sum(c_balance) AS total FROM customer WHERE c_d_id IN (1, 2);

-- ════ 第 3 部分：增删改查，写入脚本仍在跑 ════
-- 【窗口 B】写入还在继续
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o;
-- 3.1 增：仓库 1 新来一个客户，给他下两张订单（客户和订单在同一仓库 → 同一个分片）
-- 【窗口 A】
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance) VALUES (1, 3, 1, 'Nina', 'He', 500);
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 3, 1, 1, 4), (1, 3, 2, 1, 6);
-- 3.2 查：按主键查一个客户；EXPLAIN 看它只发给一个节点（Task Count: 1）
SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
-- 查仓库 1 的演示客户（同一个分片内；9 区是写入脚本插的，不列）
SELECT c_d_id, c_id, c_first, c_last, c_balance FROM customer WHERE c_w_id = 1 AND c_d_id <> 9 ORDER BY 1, 2;
-- 跨仓库统计：4 个分片并行算，master 汇总（Task Count: 4）
SELECT c_w_id, count(*) AS customers, sum(c_balance) AS balance FROM customer WHERE c_d_id <> 9 GROUP BY 1 ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT c_w_id, count(*) FROM customer GROUP BY 1;
-- 客户和订单连接：两表按仓库共置，连接在各 worker 本地完成
SELECT c.c_w_id, c.c_d_id, c.c_id, c.c_first, count(o.o_id) AS orders
  FROM customer c
  JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 WHERE c.c_d_id <> 9
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
SELECT count(*) AS customers FROM customer WHERE c_d_id <> 9;
SELECT count(*) AS orders FROM orders WHERE o_d_id = 3;

-- ════ 结束：停掉持续写入 ════
-- 【终端 W】按 Ctrl+C：正在执行的那一条做完再停，最后打印三种写入各成功多少次、库里 9 区的客户数和订单数
-- 【窗口 B】写入脚本插进来的数据（9 区）按仓库统计：每个仓库的客户、以及这些客户的订单（共置连接）
SELECT c.c_w_id AS warehouse, count(DISTINCT c.c_id) AS customers, count(o.o_id) AS orders
  FROM customer c
  LEFT JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 WHERE c.c_d_id = 9
 GROUP BY 1 ORDER BY 1;
-- 合计：和终端 W 最后打印的一致；订单都挂在已有客户名下（上面按客户连出来的订单数加起来等于这里）
SELECT c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer WHERE c_d_id = 9) c, (SELECT count(*) AS n FROM orders WHERE o_d_id = 9) o;
