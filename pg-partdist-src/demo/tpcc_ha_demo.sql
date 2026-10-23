-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示（二）：高可用（实验室三机集群：master + worker1 / worker2 / worker3，第 6 步加 worker4）
--   0 准备数据 → 1 观察函数 → 2 一次写入在 Raft 里的样子 → 3 停掉一个 worker：切主、读写
--   → 4 把它拉起来：追上日志 → 5 手动把主切回它、在它上面读写 → 6 加节点、迁移副本（迁移前后对比）→ 7 删表
--   → 8 收尾：去掉加的节点
--
-- 前提：TPC-C 演示的第二步做完（9 张表已建好、还没有数据）。
-- 两个终端，都先从自己的电脑 ssh yyt@10.77.110.145：
--   【终端 A】psql，连 148 上的 master（下面的 SQL 都在这里敲）：
--       docker exec -it -e PGOPTIONS='-c client_min_messages=warning' shardpg-ddl-worker1 \
--           /work/pg-install/bin/psql -h 10.77.110.148 -p 32432 -U postgres
--   【终端 B】145 的 shell：
--       · 停 / 起 worker1 用 PostgreSQL 原生的 pg_ctl（worker1 的数据库跑在 145 的容器 shardpg-ddl-worker1 里）：
--           docker exec -u postgres shardpg-ddl-worker1 /work/pg-install/bin/pg_ctl status -D /work/pg-cluster-data/worker1
--       · 加节点、迁移副本、手动切主、看分片文件用 shardpg_ctl（和 pg_ctl 放在一起）：
--           cd /data1/yyt/shardpg-ddl-demo/pg-install/bin
--           ./shardpg_ctl status
-- 节点：master（148:32432）、worker1（145:32433）、worker2（147:32434）、worker3（148:32435）；第 6 步加 worker4（147:32436）。
-- ⚠ 反斜杠命令（\gset、\watch 跟在查询末尾一起粘贴没问题；\c 这类要单独一行）。
--
-- 背景：
--   · 每个分片是一个 Raft 组，组号就是分片号，3 个成员 = 1 主 2 从，三台 worker 各一份。
--     master 不存数据，只做路由和发时间戳（TSO）；控制面组（组 0，所有节点都在）登记每个分片现在的主是谁。
--   · 主上每条本地 WAL 记录同时写进这个分片的分区日志，作为一条 Raft 日志复制给两个从，多数派落盘才算提交。
--   · 从只把日志收下落盘、不马上回放进表（惰性回放），升主的时候才回放。
-- ════════════════════════════════════════════════════════════════════════════


-- ════ 第 0 步：准备数据【终端 A】 ════
-- 4 个仓库各 2 个客户、每人余额 1000，再给几个客户下订单
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance) VALUES
    (1, 1, 1, 'Tom',  'Li',   1000), (1, 1, 2, 'Amy',  'Wang', 1000),
    (2, 1, 1, 'Lucy', 'Chen', 1000), (2, 1, 2, 'Jack', 'Liu',  1000),
    (3, 1, 1, 'Ken',  'Sun',  1000), (3, 1, 2, 'Ann',  'Zhou', 1000),
    (4, 1, 1, 'Ivy',  'Xu',   1000), (4, 1, 2, 'Sam',  'Hu',   1000);
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES
    (1, 1, 1, 1, 5), (1, 1, 2, 2, 3), (2, 1, 1, 1, 4), (3, 1, 1, 1, 2), (4, 1, 1, 2, 6);
SELECT c_w_id, c_id, c_first, c_balance, tableoid::regclass::text AS shard, shardpg.node_name() AS worker
  FROM customer ORDER BY c_w_id, c_id;


-- ════ 第 1 步：观察函数【终端 A】 ════
-- 只有一个函数：某个分片键值所在分片的 Raft 日志 —— 主和每个从上最新的 n 条（默认 5 条）。
-- Raft 日志在各 worker 本地（partdist.raft_log），用 run_command_on_workers 到每台去读、在 master 上拼起来。
--   role：primary / replica（Raft 里的 leader / follower）；unreachable = 那台 worker 停了
--   log_index / term：日志序号、写这条时的任期；三台对得上就是复制到了
--   record：这条日志是什么 —— DATA 记录是主节点本地 WAL 的原样字节（Heap / Btree …，与 pg_waldump 叫法一致），
--           TXN 是事务标记（提交 / 预备），DTX 是两阶段提交的决议，CTRL 是控制记录
--   committed：本节点是否已知这条提交了（从上最新一条要等主下一次心跳，约 1 秒）
CREATE SCHEMA IF NOT EXISTS ha;
CREATE OR REPLACE FUNCTION ha.raft_log(tbl regclass, key int, n int DEFAULT 5)
RETURNS TABLE (worker text, role text, log_index bigint, term bigint, record text, committed boolean)
LANGUAGE sql AS $f$
WITH w AS (
  SELECT nodeport, success, result
    FROM run_command_on_workers(format($q$
      SELECT coalesce(json_agg(json_build_object(
               'state', (SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = l.group_id),
               'idx', l.log_index, 'term', l.term, 'op', l.op_type, 'p', l.payload, 'committed', l.committed)), '[]')
        FROM (SELECT * FROM partdist.raft_log WHERE group_id = %s ORDER BY log_index DESC LIMIT %s) l$q$,
      get_shard_id_for_distribution_column(tbl, key), n))
), x AS (
  SELECT nm.name AS worker,
         CASE r.state WHEN 'leader' THEN 'primary' WHEN 'follower' THEN 'replica' ELSE r.state END AS role,
         r.idx AS log_index, r.term,
         CASE WHEN r.op <> 'OP_PARWAL' THEN r.op
              WHEN f & 8 = 8 THEN 'DTX '  || coalesce((ARRAY['PREPARE','DECISION','COMMIT','ABORT','FORGET'])[i], i::text)
              WHEN f & 4 = 4 THEN 'CTRL ' || coalesce((ARRAY['FILESET_UPDATE','FREEZE_UPDATE','SHARD_CLOG','SHARD_DROP',
                                                             'DDL_HINT','SHARD_MVCC','BASELINE_END'])[i], i::text)
              WHEN f & 2 = 2 THEN 'TXN '  || coalesce((ARRAY['COMMIT','PREPARE','ABORT','COMMIT_PREPARED','ABORT_PREPARED'])[((i & 112) >> 4) + 1], i::text)
              ELSE coalesce((SELECT rm_name FROM pg_get_wal_resource_managers() WHERE rm_id = rm), 'rmid ' || rm) || ' ' ||
                   coalesce(CASE rm
                     WHEN 0  THEN (ARRAY['CHECKPOINT_SHUTDOWN','CHECKPOINT_ONLINE','NOOP','NEXTOID','SWITCH','BACKUP_END',
                                         'PARAMETER_CHANGE','RESTORE_POINT','FPW_CHANGE','END_OF_RECOVERY','FPI_FOR_HINT','FPI'])[(i >> 4) + 1]
                     WHEN 9  THEN (ARRAY['REWRITE','PRUNE','VACUUM','FREEZE_PAGE','VISIBLE','MULTI_INSERT','LOCK_UPDATED','NEW_CID'])[((i & 112) >> 4) + 1]
                     WHEN 10 THEN (ARRAY['INSERT','DELETE','UPDATE','TRUNCATE','HOT_UPDATE','CONFIRM','LOCK','INPLACE'])[((i & 112) >> 4) + 1]
                     WHEN 11 THEN (ARRAY['INSERT_LEAF','INSERT_UPPER','INSERT_META','SPLIT_L','SPLIT_R','INSERT_POST','DEDUP','DELETE',
                                         'UNLINK_PAGE','UNLINK_PAGE_META','MARK_PAGE_HALFDEAD','VACUUM','REUSE_PAGE','META_CLEANUP'])[(i >> 4) + 1]
                   END, 'info ' || i)
         END AS record,
         r.committed
    FROM w
    JOIN shardpg.nodes nm ON nm.port = w.nodeport
    CROSS JOIN LATERAL json_to_recordset(CASE WHEN w.success THEN w.result::json ELSE '[]' END)
          AS r(state text, idx bigint, term bigint, op text, p jsonb, committed boolean)
    CROSS JOIN LATERAL (SELECT (r.p->>'rmid')::int AS rm, (r.p->>'info')::int AS i, (r.p->>'flags')::int AS f) k
  UNION ALL
  SELECT nm.name, 'unreachable', NULL, NULL, NULL, NULL
    FROM w JOIN shardpg.nodes nm ON nm.port = w.nodeport WHERE NOT w.success
)
SELECT * FROM x ORDER BY CASE role WHEN 'primary' THEN 0 WHEN 'replica' THEN 1 ELSE 2 END, worker, log_index
$f$;


-- ════ 第 2 步：一次写入在 Raft 里的样子【终端 A】 ════
-- 2.1 仓库 1 所在分片：主和两个从上最新的 5 条日志 —— 三台的 log_index、term、record 一模一样
SELECT * FROM ha.raft_log('customer', 1);

-- 2.2 写一行（仓库 1），再看：三台都多了 3 条
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance) VALUES (1, 2, 1, 'Raft', 'One', 100);
SELECT * FROM ha.raft_log('customer', 1);
-- 一条 INSERT = 3 条日志：Heap INSERT（堆元组）、Btree INSERT_LEAF（主键索引）、TXN COMMIT（提交标记）。
-- 主把这 3 条复制给两个从、多数派落盘之后 INSERT 才返回。从上最后一条的 committed 可能还是 f，过一秒再查就是 t。

-- 2.3 跨机器的事务：两阶段提交的每一步也是 Raft 日志（仓库 1 的分片主在 worker1、仓库 3 的在 worker2，见第 0 步的 worker 列）
--     两个分片的主在同一台上时（比如仓库 1 和 2 都在 worker1），Citus 在那一台上一次提交，用不着两阶段
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance) VALUES (3, 2, 1, 'Raft', 'Two', 100);
BEGIN;
UPDATE customer SET c_balance = c_balance - 30 WHERE c_w_id = 1 AND c_d_id = 2 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 30 WHERE c_w_id = 3 AND c_d_id = 2 AND c_id = 1;
COMMIT;
SELECT * FROM ha.raft_log('customer', 1, 8) WHERE role = 'primary';
SELECT * FROM ha.raft_log('customer', 3, 8) WHERE role = 'primary';
-- 两个分片各有：Heap HOT_UPDATE（改余额）→ DTX PREPARE + TXN PREPARE（第一阶段）；
-- 协调组写 DTX DECISION（全局决议），参与组写 DTX COMMIT，各自 TXN COMMIT（第二阶段）；过几秒协调组再写 DTX FORGET 收尾。
-- 决议本身也复制到了多数派：协调组的主这时挂了，新主照样知道这笔事务该提交。


-- ════ 第 3 步：停掉一个 worker —— 切主、读写 ════
-- 3.1【终端 A】记下仓库 1 的两个分片（customer / orders 共置，同一仓库落在同一位置），看它们的主
SELECT shard_name('customer', get_shard_id_for_distribution_column('customer', 1)) AS c1,
       shard_name('orders',   get_shard_id_for_distribution_column('orders', 1))   AS o1 \gset
SELECT shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1');
-- 下面把主 worker1（在 145 上）停掉。primary 列不是 worker1 的话，先在终端 B 把它切过来再往下做：
--     ./shardpg_ctl move-primary <c1 的值> worker1

-- 3.2【终端 B】用 PostgreSQL 原生的 pg_ctl 直接停掉 worker1 的数据库（-m immediate = 不做 checkpoint，等同断电；容器还在）
--     docker exec -u postgres shardpg-ddl-worker1 /work/pg-install/bin/pg_ctl stop -D /work/pg-cluster-data/worker1 -m immediate
--     docker exec -u postgres shardpg-ddl-worker1 /work/pg-install/bin/pg_ctl status -D /work/pg-cluster-data/worker1

-- 3.3【终端 A】马上读：仓库 1 的主刚停，连不上；仓库 3 的分片主在 worker2 上，不受影响
--     （仓库 2 所在分片的主也在 worker1 上，同样要等切主，见第 0 步输出的 worker 列）
SELECT c_w_id, c_id, c_first, c_balance FROM customer WHERE c_w_id = 1 AND c_d_id = 1;
SELECT c_w_id, c_id, c_first, c_balance FROM customer WHERE c_w_id = 3 AND c_d_id = 1;

-- 3.4【终端 A】等它切主（每 3 秒查一次，最多 2 分钟；两行的 primary 都变了、term 加 1、不再变就可以 Ctrl+C）：
--     ① 从收不到心跳，等满选举超时（15 s）后发起选举，拿到多数票当选；
--     ② 新主先把还没回放的日志回放进表（升主前置）—— 惰性回放的账在这里付；
--     ③ 向控制面登记新主，master 的路由随之切换。实测从停机到能读写 30–45 s。
--     两个分片是两个独立的 Raft 组，新主可能先落在不同 worker 上；master 发现后会把 orders 的主转到 customer 的主那台
--     （term 再加 1），所以等两行 primary 相同再停
SELECT clock_timestamp()::time(0) AS t, shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1') \watch i=3 c=40

-- 3.5【终端 A】Raft 视角：worker1 是 unreachable；剩下两个成员里一个当了主（primary），term 加 1
SELECT * FROM ha.raft_log('customer', 1, 3);

-- 3.6【终端 A】新主上照常读写（worker 一列是实际执行的节点）
SELECT c_w_id, c_d_id, c_id, c_first, c_balance, tableoid::regclass::text AS shard, shardpg.node_name() AS worker
  FROM customer WHERE c_w_id = 1 ORDER BY c_d_id, c_id;
UPDATE customer SET c_balance = c_balance + 5 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
SELECT c_w_id, c_d_id, c_id, c_first, c_balance, shardpg.node_name() AS worker
  FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
-- Tom 的余额从 1000 变成 1005，执行的是新主
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 3, 1, 7);
SELECT o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt, tableoid::regclass::text AS shard, shardpg.node_name() AS worker
  FROM orders WHERE o_w_id = 1 ORDER BY o_id;
-- 跨分片事务也照常提交（仓库 1 的新主 + 仓库 2）
BEGIN;
UPDATE customer SET c_balance = c_balance - 10 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 10 WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 1;
COMMIT;
SELECT c_w_id, c_id, c_first, c_balance FROM customer WHERE c_d_id = 1 AND c_id = 1 ORDER BY c_w_id;

-- 3.7【终端 A】共置连接：同一仓库的客户和订单在同一台上连接
SELECT c.c_first, c.c_last, o.o_id, o.o_ol_cnt, shardpg.node_name() AS worker
  FROM customer c JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 WHERE c.c_w_id = 1 ORDER BY o.o_id;


-- ════ 第 4 步：把 worker1 拉起来 —— 追上日志 ════
-- 4.1【终端 B】用 pg_ctl 把 worker1 的数据库起来（日志文件和平时一样，写在数据目录里的 pg.log）
--     docker exec -u postgres shardpg-ddl-worker1 /work/pg-install/bin/pg_ctl start -D /work/pg-cluster-data/worker1 -l /work/pg-cluster-data/worker1/pg.log
--     docker exec -u postgres shardpg-ddl-worker1 /work/pg-install/bin/pg_ctl status -D /work/pg-cluster-data/worker1

-- 4.2【终端 A】它以从的身份回来，认了新主的 term、不会回来抢主；停机期间缺的日志由新主补发。
--     每 10 秒看一次三台各自最新的一条日志（n = 1）：worker1 的 log_index 追上主、role 是 replica 就可以 Ctrl+C
--     （如果它这份数据从没当过从，新主还会给它重做一次物理基线，约 1 分钟）
SELECT clock_timestamp()::time(0) AS t, * FROM ha.raft_log('customer', 1, 1) \watch i=10 c=30


-- ════ 第 5 步：手动把主切回 worker1，在它上面读写（计划内切换，比如维护前把主挪走、维护完再挪回来） ════
-- 拉回来的 worker1 在这两个分片里是从（replica）；把主手动切回它。
-- 5.1【终端 B】把仓库 1 的两个分片（customer 和共置的 orders 一起）切回 worker1。
--     分片名用 3.1 输出里的 c1（例如 customer_102xxx）：
--     ./shardpg_ctl move-primary customer_102xxx worker1
--   做的事（领导权转交）：目标先把副本回放追平 → 现任主挡住新提交、把目标追到日志末尾 → 请目标竞选（term 加 1）
--   → 新主回放、向控制面登记，路由切过来。排队的那几笔提交报"不是主"、重试即可

-- 5.2【终端 A】两个分片的主都回到 worker1，term 再加 1；Raft 视角 worker1 是 primary
SELECT shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1');
SELECT * FROM ha.raft_log('customer', 1, 1);

-- 5.3【终端 A】在 worker1 上照常读写（worker 一列是实际执行的节点）
--     先读：worker1 停机期间在新主上写的那几笔（Tom 的余额 995、订单 3）都在 —— 它回来后追上的日志切主前已回放进表
SELECT c_w_id, c_d_id, c_id, c_first, c_balance, tableoid::regclass::text AS shard, shardpg.node_name() AS worker
  FROM customer WHERE c_w_id = 1 ORDER BY c_d_id, c_id;
SELECT o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt, shardpg.node_name() AS worker
  FROM orders WHERE o_w_id = 1 ORDER BY o_id;
--     再写：改余额、下一张新订单，再读回来
UPDATE customer SET c_balance = c_balance + 5 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 1, 4, 1, 2);
SELECT c_first, c_balance, shardpg.node_name() AS worker
  FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
SELECT o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt, shardpg.node_name() AS worker
  FROM orders WHERE o_w_id = 1 AND o_d_id = 1 AND o_id = 4;
--     Tom 的余额 995 → 1000，订单 4 写进去了，执行的都是 worker1


-- ════ 第 6 步：加节点、迁移副本 ════
-- 6.1【终端 B】在 147 上加一台 worker4（端口 32436，约 1 分钟，不重启任何节点）：
--     ./shardpg_ctl add-node worker4 10.77.110.147 32436
--     ./shardpg_ctl status
--   (1) start a container on 147, initdb, start PostgreSQL, create the extensions and the node table;
--   (2) append the new node to pg_raft.peers on every node (hot reload) and register it in the control plane;
--   (3) citus_add_node: the distributed tables' shells are created on the new node; it holds no shards yet

-- 6.2【终端 A】迁移前：仓库 1 这一段的主从、各成员的日志
SELECT shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1');
SELECT * FROM ha.raft_log('customer', 1, 2);

-- 6.3【终端 B】迁移前：源节点（这一段的第二个从，一般是 worker3）和新节点 worker4 上实际有哪些分片
--     ./shardpg_ctl files worker3
--     ./shardpg_ctl files worker4

-- 6.4【终端 B】把这一段的一份从副本从 worker3 迁到 worker4（customer 和共置的 orders 一起搬，1–3 分钟，全程可读写）：
--     ./shardpg_ctl move-replica customer_102xxx worker3 worker4
--   (1) add member: the new node joins each Raft group (single-node membership change, 3 -> 4 members);
--   (2) provision: create the replica tables, ship a physical baseline, the new node catches up with the log;
--   (3) remove member: the source node leaves (4 -> 3) and the control plane updates the replica set;
--   (4) clean up the source node: these two groups, replica tables, replay registrations and hardstate files

-- 6.5【终端 A】迁移后：replicas 里 worker3 换成了 worker4；worker4 的日志和主一致，worker3 不在里面了
SELECT shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1');
SELECT * FROM ha.raft_log('customer', 1, 2);

-- 6.6【终端 B】迁移后：worker3 上没有这两个分片了，worker4 上有了
--     ./shardpg_ctl files worker3
--     ./shardpg_ctl files worker4

-- 6.7（可选）让新节点当主，在它上面读写
--     【终端 B】./shardpg_ctl move-primary customer_102xxx worker4
--     【终端 A】
UPDATE customer SET c_balance = c_balance + 1 WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;
SELECT c_first, c_balance, tableoid::regclass::text AS shard, shardpg.node_name() AS worker
  FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1;


-- ════ 第 7 步：删表【终端 A】 ════
DROP TABLE warehouse, district, customer, history, new_order, orders, order_line, item, stock;
SELECT * FROM shardpg.shards;
DROP SCHEMA ha CASCADE;
-- 【终端 B】每台 worker 上都没有分片表、也没有分片的 Raft 组状态文件了：
--     ./shardpg_ctl files worker1
--     ./shardpg_ctl files worker2
--     ./shardpg_ctl files worker3
--     ./shardpg_ctl files worker4


-- ════ 第 8 步：收尾 —— 去掉第 6 步加的 worker4【终端 B】 ════
-- pg_raft.peers 只许热加载追加、不许热删，去掉节点要整簇重启一次（约 1 分钟；终端 A 的 psql 会断开，之后重连）：
--     ./shardpg_ctl remove-node worker4
-- 之后集群回到 master + 3 个 worker，可以从 TPC-C 演示的第二步重新建表再演。
