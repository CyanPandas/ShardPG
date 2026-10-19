-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示：TPC-C 客户表 customer + 订单表 orders，从建表到删表一共 7 步
--   1 建表 → 2 分片信息与主从 → 3 表和分片的对应（含共置、分区日志在磁盘上）
--   → 4 边写边读 → 5 分布式事务 → 6 增删改查（4–6 步期间写入脚本一直在跑）
--   → 7 删表
--
-- 一个终端 + 两个窗口：
--   窗口 A、窗口 B：都连 master（:5432）
--       docker exec -it -e PGCLIENTENCODING=UTF8 -u postgres pg-test-container \
--           /work/pg-install/bin/psql -h /tmp -p 5432 -U postgres
--   终端 W：宿主机上的 shell，跑持续写入脚本 txn_demo_writer.sh —— 第 3 步做完再启动，第 6 步做完按 Ctrl+C 停
-- 每段前面的【窗口 A】/【窗口 B】/【终端 W】表示这段在哪里做。
-- ⚠ 反斜杠开头的命令（\pset、\dt 等）要单独一行输入、回车，不要和下面的 SQL 一起粘贴：
--   多行一起粘贴时终端会把它们当成一整行，反斜杠命令会把后面的 SQL 都当成自己的参数（报 extra argument ... ignored）。
--
-- 写入脚本轮流做三种写入，每条都是不开事务的单条 INSERT，每条数据都不同：
--   ① 只插客户表  ② 只插订单表（给它建过的客户下单）  ③ 新客户 + 他的订单（两条 INSERT，同一仓库）
-- 它写的行区域号都是 9（c_d_id / o_d_id = 9）；演示里操作的是区域号 1、2 的 12 个客户（总额 12000）。
-- 两批数据在同一张表、同样的分片里，只是主键不重叠、演示查询按区域号过滤，所以互不干扰。
--
-- worker 端口：:5433 = worker1，:5434 = worker2，:5435 = worker3，:5436 = worker4
-- ════════════════════════════════════════════════════════════════════════════

-- ════ 第 1 步：建表 ════
-- 建表时直接指定：分片键、分片数、副本数（1 主 + n 从）、共置。
-- 一条语句完成建表、分布、建 Raft 组（选主 + 供副本）、接入分布式事务（打标），每张表约 1 分钟。
-- 【窗口 A】
CREATE TABLE customer (
    c_w_id    int           NOT NULL,          -- 仓库号：分片键
    c_d_id    int           NOT NULL,          -- 区域号
    c_id      int           NOT NULL,          -- 客户号
    c_first   varchar(16)   NOT NULL,
    c_last    varchar(16)   NOT NULL,
    c_balance numeric(12,2) NOT NULL DEFAULT 0,
    c_since   timestamp     NOT NULL DEFAULT now(),
    PRIMARY KEY (c_w_id, c_d_id, c_id),        -- 主键必须包含分片键
    SHARD KEY (c_w_id) SHARDS 4 REPLICAS 2     -- 按仓库号哈希切 4 片，每片 1 主 2 从
);

CREATE TABLE orders (
    o_w_id    int       NOT NULL,              -- 仓库号：分片键
    o_d_id    int       NOT NULL,
    o_id      int       NOT NULL,
    o_c_id    int       NOT NULL,              -- 下单客户
    o_ol_cnt  int       NOT NULL,
    o_entry_d timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    SHARD KEY (o_w_id) COLOCATE WITH customer  -- 与 customer 共置：同一仓库的客户和订单在同一位置
);

-- 写入一些客户：每条 INSERT 只写一个仓库（同一个分片）
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last) VALUES
    (1, 1, 1, 'Tom',  'Li'),   (1, 1, 2, 'Amy',  'Wang'), (1, 2, 1, 'Bob',  'Zhang');
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last) VALUES
    (2, 1, 1, 'Lucy', 'Chen'), (2, 1, 2, 'Jack', 'Liu'),  (2, 2, 1, 'Mary', 'Zhao');
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last) VALUES
    (3, 1, 1, 'Ken',  'Sun'),  (3, 1, 2, 'Ann',  'Zhou'), (3, 2, 1, 'Leo',  'Wu');
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last) VALUES
    (4, 1, 1, 'Ivy',  'Xu'),   (4, 1, 2, 'Sam',  'Hu'),   (4, 2, 1, 'Eva',  'Guo');
SELECT c_w_id, count(*) AS customers FROM customer GROUP BY 1 ORDER BY 1;

-- ════ 第 2 步：分片信息与主从 ════
-- 每个分片的物理表名、分片键、哈希范围、主、从、任期
-- 【窗口 A】
SELECT * FROM shardpg.shards WHERE table_name IN ('customer', 'orders');
-- 按 worker 看：每台 worker 上放着哪些分片（主 / 从）
SELECT worker, string_agg(shard_name || '（' || role || '）', ', ' ORDER BY shard_name) AS shards
  FROM (SELECT shard_name, "primary" AS worker, '主' AS role FROM shardpg.shards
         WHERE table_name IN ('customer', 'orders')
        UNION ALL
        SELECT shard_name, unnest(string_to_array(replicas, ',')), '从' FROM shardpg.shards
         WHERE table_name IN ('customer', 'orders')) x
 GROUP BY 1 ORDER BY 1;

-- ════ 第 3 步：表和分片的对应 ════
-- \dt 表名：父表一行（分片键、分片数、副本数、共置），下面逐行列出子表（分片）及其主从
-- 【窗口 A】
\dt customer
-- \dt+ 表名：再加每个子表的行数、大小（父表是各子表之和）
\dt+ customer
-- 按模式匹配：匹配到的分布式表连同子表一起展开
\dt customer*
\dt orders
-- 直接按子表名查：子表只在 worker 上，master 上查不到，提示它属于哪张表
\dt customer_1*
-- 每一行在哪个分片、哪台 worker：查询里加两列（后面各步都用这两列看"改的是哪个分片上的表"）
--   tableoid::regclass::text AS shard          这一行所在的物理分片表（名字在 worker 上取）
--   ':' || current_setting('port') AS worker   实际执行这一行的 worker（就是该分片的主）
-- 12 个客户：同一仓库的客户在同一个分片；分片按仓库号的哈希分，仓库 3、4 落在同一个分片
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, count(*) AS rows,
       string_agg('仓库' || c_w_id || ' ' || c_first, ', ' ORDER BY c_w_id, c_d_id, c_id) AS customers
  FROM customer GROUP BY 1, 2 ORDER BY 1;

-- ── 3.1 共置：建 orders 时为什么写 COLOCATE WITH customer ──
-- 一句话：两张表按同一把尺子切（同样的分片键类型、分片数、哈希范围），切出来的同一段放在同一批节点上 ——
-- 同一个仓库的客户和他的订单，永远在同一台 worker 上。
-- 写法：被共置的表先建；共置的表只写 SHARD KEY (列) COLOCATE WITH 表，分片数跟着走、不能再写 SHARDS。
-- 注意副本数目前不会跟着走（不写 REPLICAS 取默认 2，customer 正好也是 2），要和被共置的表写成一样（P7-N106）
-- 【窗口 A】
-- ① 元数据：两张表登记在同一个共置组（colocationid 相同）；共置是元数据里的约定，Citus 只认这个号
SELECT logicalrelid::text AS table_name, column_to_column_name(logicalrelid, partkey) AS shard_key, colocationid,
       (SELECT count(*) FROM pg_dist_shard s WHERE s.logicalrelid = p.logicalrelid) AS shards
  FROM pg_dist_partition p WHERE logicalrelid IN ('customer'::regclass, 'orders'::regclass) ORDER BY 1;
-- ② 同一段哈希范围的两个分片并排看：范围一样、主一样、从一样（same_nodes 全是 t）
SELECT c.hash_range, c.shard_name AS customer_shard, o.shard_name AS orders_shard, c."primary", c.replicas,
       (c."primary", c.replicas) = (o."primary", o.replicas) AS same_nodes
  FROM shardpg.shards c
  JOIN shardpg.shards o ON o.table_name = 'orders'::regclass AND o.hash_range = c.hash_range
 WHERE c.table_name = 'customer'::regclass
 ORDER BY c.shard_name;
-- ③ 效果：客户 JOIN 订单拆成 4 个任务（每段一个），每个任务在一台 worker 上把同一段的
--    orders_X 和 customer_Y 本地连接（看每个 Task 的 Node 和两个 Seq Scan 的分片名），节点之间不搬数据
SET citus.explain_all_tasks = on;
EXPLAIN (COSTS OFF)
SELECT c.c_w_id, c.c_first, count(o.o_id) AS orders
  FROM customer c JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 GROUP BY 1, 2;
RESET citus.explain_all_tasks;
-- ④ 对照（可选，建表约 50 s）：同样 4 片、只是不写 COLOCATE WITH 的订单表 orders_nc。
--    它的分片碰巧也放在同样的节点上，但共置号不同 —— 同样的连接直接报错
--    the query contains a join that requires repartitioning；打开重分布才能跑，要 20 个任务、
--    先把两张表按连接键在节点之间重新分发一遍（MapMergeJob）。
--    不共置还有一个后果：切主、迁移时两张表各走各的，现在碰巧在一起的以后也会分开（共置对齐只管同一共置组）
CREATE TABLE orders_nc (o_w_id int NOT NULL, o_d_id int NOT NULL, o_id int NOT NULL, o_c_id int NOT NULL,
    PRIMARY KEY (o_w_id, o_d_id, o_id), SHARD KEY (o_w_id) SHARDS 4 REPLICAS 2);
SELECT logicalrelid::text AS table_name, colocationid FROM pg_dist_partition
 WHERE logicalrelid IN ('customer'::regclass, 'orders'::regclass, 'orders_nc'::regclass) ORDER BY 1;
SELECT c.c_w_id, count(o.o_id) FROM customer c JOIN orders_nc o ON o.o_w_id = c.c_w_id AND o.o_c_id = c.c_id GROUP BY 1;
SET citus.enable_repartition_joins = on;
EXPLAIN (COSTS OFF)
SELECT c.c_w_id, count(o.o_id) FROM customer c JOIN orders_nc o ON o.o_w_id = c.c_w_id AND o.o_c_id = c.c_id GROUP BY 1;
RESET citus.enable_repartition_joins;
DROP TABLE orders_nc;

-- ── 3.2 分区日志（partwal）在磁盘上 ──
-- 每个分片是一个 Raft 组（组号 = 分片号），组里每个成员节点上都有：一张分片表 + 一个分区日志目录
--   $PGDATA/pg_parwal/<这个分片在该节点上本地表的 oid>/       （目录名是本地 oid，各节点不同，不是分片号）
-- 共置的两个分片是两个独立的 Raft 组、两份独立的日志，只是成员节点相同。
-- 【窗口 A】
-- ① 仓库 1 这一段的两个分片在各 worker 上的目录：三台有（1 主 2 从），一台没有
SELECT get_shard_id_for_distribution_column('customer', 1) AS cid,
       get_shard_id_for_distribution_column('orders', 1)   AS osid \gset
SELECT nodeport AS worker, result AS parwal_dirs
  FROM run_command_on_workers(format($$
    SELECT string_agg(rpad(shard_name(t, s), 16) || coalesce(current_setting('data_directory') || '/pg_parwal/'
                      || partdist.local_partition_for_shard(s)::oid, '（不在这台）'), E'\n' ORDER BY s)
      FROM (VALUES ('customer'::regclass, %s::bigint), ('orders'::regclass, %s::bigint)) v(t, s)$$, :cid, :osid))
 ORDER BY 1;
-- ② 在主上 ls。\! 不认 psql 变量，所以先把两个目录取进 psql 变量、再用 \setenv 交给 shell（下面每行单独回车）
SELECT ltrim("primary", ':')::int AS pport FROM shardpg.shards WHERE shard_name = shard_name('customer', :cid) \gset
SELECT split_part(result, E'\n', 1) AS cdir, split_part(result, E'\n', 2) AS odir
  FROM run_command_on_workers(format($$
    SELECT current_setting('data_directory') || '/pg_parwal/' || partdist.local_partition_for_shard(%s)::oid || E'\n'
        || current_setting('data_directory') || '/pg_parwal/' || partdist.local_partition_for_shard(%s)::oid$$, :cid, :osid))
 WHERE nodeport = :pport \gset
\setenv CDIR :cdir
\setenv ODIR :odir
-- 主上 pg_parwal 下每个分片一个子目录（这台上有主也有从，都在这里）
\! ls -C $(dirname $CDIR)
-- 仓库 1 这一段：customer、orders 各一个目录。00000001… 是日志记录本身（沿用 pg_wal 的段文件命名）；
-- 主上还有 checkpoint（处理到哪）、fileset（这个分片的表和索引文件清单）、freeze（冻结位点）、promoted（本节点这份是升过主的）
\! ls -l $CDIR $ODIR
-- ③ customer 这个分片在主和第一个从上的目录并排看：日志段文件一样大（记录一条不差）；
--    从上没有 promoted，多了 locmap（主上的表文件号 → 本机表文件号的对照：从收到的是主的原始 WAL，
--    升主回放时靠它落到本机的表上）和 replay_enabled（回放槽位已就绪）
SELECT ltrim(split_part(replicas, ',', 1), ':')::int AS rport FROM shardpg.shards WHERE shard_name = shard_name('customer', :cid) \gset
SELECT result AS rdir
  FROM run_command_on_workers(format($$
    SELECT current_setting('data_directory') || '/pg_parwal/' || partdist.local_partition_for_shard(%s)::oid$$, :cid))
 WHERE nodeport = :rport \gset
\setenv RDIR :rdir
\! ls -l $CDIR $RDIR

-- ════ 第 4 步：开始持续写入，边写边读 ════
-- 表已经建好（第 1 步完成之后才能启动写入脚本：表还在建的时候往里写会让建表失败）
-- 【终端 W】启动写入脚本，每完成一条打一行；让它一直跑，不用等它
--           bash ~/shardpg-ddl-work/pg-partdist-src/demo/txn_demo_writer.sh
-- 【窗口 B】写入进行中，每秒查一次客户数和订单数，查 10 次（只显示数据行）：两个数一直在涨
--           分三次输入：\pset tuples_only on 回车；SELECT 两行（以 \watch 结尾）回车；\pset tuples_only off 回车
\pset tuples_only on
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o \watch i=1 c=10
\pset tuples_only off

-- ════ 第 5 步：分布式事务（一个事务写多个分片），写入脚本仍在跑 ════
-- 一个事务写多个分片时，系统自动把它作为一个全局事务（统一的事务号和快照时间戳），写法和普通 PostgreSQL 一样
-- 增删改语句本身带不出分片和 worker（Citus 不许 RETURNING 里有非 IMMUTABLE 函数），所以改完、提交前查一次
-- 5.0 先认人：演示操作的 12 个客户（区域号 9 是写入脚本插的，不列）。后面每条增删改的注释都写明改的是谁
-- 【窗口 A】
SELECT c_first || ' ' || c_last AS name, c_w_id, c_d_id, c_id,
       tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker
  FROM customer WHERE c_d_id <> 9 ORDER BY c_w_id, c_d_id, c_id;
-- 【窗口 B】写入还在继续
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o;
-- 5.1 准备：5.0 列出的 12 个客户每人 1000 元。不带分片键的 UPDATE 发给全部 4 个分片，一条语句就是一个全局事务
-- 【窗口 A】
UPDATE customer SET c_balance = 1000 WHERE c_d_id IN (1, 2);      -- 12 人全改
SELECT count(*) AS customers, sum(c_balance) AS total FROM customer WHERE c_d_id IN (1, 2);
-- 这条 UPDATE 改到了哪：按分片汇总
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, count(*) AS rows, sum(c_balance) AS balance
  FROM customer WHERE c_d_id IN (1, 2) GROUP BY 1, 2 ORDER BY 1;
-- 5.2 提交：仓库 1 的 Tom 转 300 元给仓库 2 的 Lucy，同时给 Tom 下一张订单（三个写操作，跨两个分片、两台 worker）
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 300 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;   -- Tom  −300
UPDATE customer SET c_balance = c_balance + 300 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 1;   -- Lucy +300
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 1, 1, 5);   -- Tom 的订单：订单号 (1, 1, 1)，客户号 1，5 行
-- 提交前看这个事务改到了哪：两个客户在两个分片、两台 worker；订单和 Tom 在同一台 worker（共置）
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_first, c_balance
  FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 2) ORDER BY c_w_id;
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, o_w_id, o_id, o_c_id, o_ol_cnt
  FROM orders WHERE o_w_id = 1 AND o_d_id = 1 AND o_id = 1;
COMMIT;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 ORDER BY 1;
SELECT sum(c_balance) AS total FROM customer WHERE c_d_id IN (1, 2);
-- 5.3 回滚：仓库 3 的 Ken 转 500 给仓库 2 的 Lucy（跨两个分片），事务里看得到修改，ROLLBACK 之后两边都恢复
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 500 WHERE c_w_id = 3 AND c_d_id = 1 AND c_id = 1;   -- Ken  −500
UPDATE customer SET c_balance = c_balance + 500 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 1;   -- Lucy +500
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_first, c_balance
  FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (2, 3) ORDER BY c_w_id;
ROLLBACK;
SELECT c_w_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (2, 3) ORDER BY 1;
-- 5.4 中途出错：仓库 1 的 Amy 转 200 元给仓库 3 的 Ann，同时给 Amy 下一张订单；
--     两笔余额已改，第三条插入的订单号和 5.2 Tom 那张重复 → 整个事务作废，COMMIT 实际执行的是 ROLLBACK
--     报错里的 localhost:端口 就是订单所在分片的主（下面查询的 worker 列）
-- 【窗口 A】先看订单表里已有的订单（区域 9 是写入脚本的，不看）：(1, 1, 1) 已经有了 —— 5.2 给 Tom 下的那张，
--           所以下面事务里第三条 INSERT 的订单 (1, 1, 1) 会和它重复
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt
  FROM orders WHERE o_d_id <> 9 ORDER BY o_w_id, o_d_id, o_id;
BEGIN;
UPDATE customer SET c_balance = c_balance - 200 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 2;   -- Amy −200
UPDATE customer SET c_balance = c_balance + 200 WHERE c_w_id = 3 AND c_d_id = 1 AND c_id = 2;   -- Ann +200
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 1, 2, 3);   -- Amy 的订单：订单号 (1, 1, 1) 已被 Tom 那张占用 → 报错
COMMIT;
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_first, c_balance
  FROM customer WHERE c_d_id = 1 AND c_id = 2 AND c_w_id IN (1, 3) ORDER BY c_w_id;
SELECT sum(c_balance) AS total FROM customer WHERE c_d_id IN (1, 2);
-- 5.5 隔离：A 改了还没提交，B 看不见；B 开着的事务只看开始那一刻的快照 ——
--     A 提交了、写入脚本也一直在插，B 的事务里查到的都不变
--     A 做的是：仓库 1 的 Tom 转 100 元给仓库 4 的 Ivy
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance - 100 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;   -- Tom −100
UPDATE customer SET c_balance = c_balance + 100 WHERE c_w_id = 4 AND c_d_id = 1 AND c_id = 1;   -- Ivy +100
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_first, c_balance
  FROM customer WHERE c_d_id = 1 AND c_id = 1 AND c_w_id IN (1, 4) ORDER BY c_w_id;
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
-- 5.6 写写冲突：A、B 同时改仓库 2 的 Jack（同一个分片上的同一行），B 被挡住；A 提交后 B 报错（先提交者胜），B 只能重试
-- 【窗口 A】
BEGIN;
UPDATE customer SET c_balance = c_balance + 7 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;   -- A：Jack +7
-- 【窗口 B】最后这条 UPDATE 会卡住（在等 A），不用等它，回到窗口 A
BEGIN;
UPDATE customer SET c_balance = c_balance - 7 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;   -- B：Jack −7
-- 【窗口 A】提交
COMMIT;
-- 【窗口 B】卡住的 UPDATE 返回报错 could not serialize access due to concurrent update，回滚后看结果
ROLLBACK;
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_first, c_balance
  FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 2;
SELECT sum(c_balance) AS total FROM customer WHERE c_d_id IN (1, 2);

-- ════ 第 6 步：增删改查，写入脚本仍在跑 ════
-- 【窗口 B】写入还在继续
SELECT clock_timestamp()::time(0) AS t, c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer) c, (SELECT count(*) AS n FROM orders) o;
-- 6.1 增：仓库 1 新来一个客户 Nina He（区域 3、客户号 1），给 Nina 下两张订单（客户和订单在同一仓库 → 同一台 worker）
-- 【窗口 A】
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance) VALUES (1, 3, 1, 'Nina', 'He', 500);   -- 新客户 Nina
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 3, 1, 1, 4), (1, 3, 2, 1, 6);   -- Nina 的订单 1、2
-- 增到了哪：客户进了 customer 的分片、订单进了 orders 的分片，两个分片在同一台 worker
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_d_id, c_id, c_first
  FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, o_w_id, o_d_id, o_id, o_ol_cnt
  FROM orders WHERE o_w_id = 1 AND o_d_id = 3 ORDER BY o_id;
-- 6.2 查：按主键查一个客户（Nina）；EXPLAIN 看它只发给一个节点（Task Count: 1，Node 就是上面那台 worker）
SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
EXPLAIN (COSTS OFF) SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
-- 查仓库 1 的演示客户：都在同一个分片、同一台 worker（区域号 9 是写入脚本插的，不列）
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_d_id, c_id, c_first, c_last, c_balance
  FROM customer WHERE c_w_id = 1 AND c_d_id <> 9 ORDER BY c_d_id, c_id;
-- 跨仓库统计：4 个分片并行算，master 汇总（Task Count: 4）
SELECT c_w_id, count(*) AS customers, sum(c_balance) AS balance FROM customer WHERE c_d_id <> 9 GROUP BY 1 ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT c_w_id, count(*) FROM customer GROUP BY 1;
-- 客户和订单连接：两表按仓库共置，连接在各 worker 本地完成
SELECT c.c_w_id, c.c_d_id, c.c_id, c.c_first, count(o.o_id) AS orders
  FROM customer c
  JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 WHERE c.c_d_id <> 9
 GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3;
-- 6.3 改：改一个客户 Nina：余额 +50、姓改成 Huang（单分片，直接改），改完看改的是哪个分片
UPDATE customer SET c_balance = c_balance + 50, c_last = 'Huang' WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;   -- Nina
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_d_id, c_id, c_first, c_last, c_balance
  FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
-- 一次改多个仓库：区域 2 的 Bob、Mary、Leo、Eva 姓改成大写，一条语句改到 3 个分片、3 台 worker
UPDATE customer SET c_last = upper(c_last) WHERE c_d_id = 2;   -- Bob、Mary、Leo、Eva
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_d_id, c_id, c_first, c_last
  FROM customer WHERE c_d_id = 2 ORDER BY c_w_id;
-- 6.4 删：先看要删的在哪（都在仓库 1 的分片里），再删 Nina 的订单 2、Nina 的其余订单和 Nina 本人
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, o_w_id, o_d_id, o_id, o_c_id
  FROM orders WHERE o_w_id = 1 AND o_d_id = 3 ORDER BY o_id;
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, c_w_id, c_d_id, c_id, c_first
  FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
DELETE FROM orders WHERE o_w_id = 1 AND o_d_id = 3 AND o_id = 2;     -- Nina 的订单 2
DELETE FROM orders WHERE o_w_id = 1 AND o_d_id = 3 AND o_c_id = 1;   -- Nina 的其余订单（订单 1）
DELETE FROM customer WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;   -- Nina 本人
SELECT count(*) AS customers FROM customer WHERE c_d_id <> 9;
SELECT count(*) AS orders FROM orders WHERE o_d_id = 3;
-- 停掉持续写入（删表之前必须停）
-- 【终端 W】按 Ctrl+C：正在执行的那一条做完再停，最后打印三种写入各成功多少次、库里区域号 9 的客户数和订单数
-- 【窗口 B】写入脚本插进来的数据（区域号 9）按仓库统计：每个仓库的客户、以及这些客户的订单（共置连接）
SELECT c.c_w_id AS warehouse, count(DISTINCT c.c_id) AS customers, count(o.o_id) AS orders
  FROM customer c
  LEFT JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 WHERE c.c_d_id = 9
 GROUP BY 1 ORDER BY 1;
-- 同一批数据按分片看：写入脚本的数据落在哪些分片、哪台 worker
SELECT tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker, count(*) AS customers
  FROM customer WHERE c_d_id = 9 GROUP BY 1, 2 ORDER BY 1;
-- 合计：和终端 W 最后打印的一致；订单都挂在已有客户名下（上面按客户连出来的订单数加起来等于这里）
SELECT c.n AS customers, o.n AS orders
  FROM (SELECT count(*) AS n FROM customer WHERE c_d_id = 9) c, (SELECT count(*) AS n FROM orders WHERE o_d_id = 9) o;

-- ════ 第 7 步：删表 ════
-- 一条 DROP：删表的同时自动拆 Raft 组、删各节点副本表、删主从登记（下面三条分三次输入，\dt 那行单独回车）
-- 【窗口 A】
DROP TABLE orders, customer;
\dt customer
SELECT * FROM shardpg.shards WHERE table_name::text IN ('customer', 'orders');
