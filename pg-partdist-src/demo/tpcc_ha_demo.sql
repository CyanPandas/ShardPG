-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 演示（二）：高可用与内部机制 —— 全部在 psql 里敲
--   1 定义观察函数 → 2 一次写入在 Raft 里的样子 → 3 停掉一个 worker：切主、读写
--   → 4 把它拉起来：重新加入 Raft 组 → 5 手动切主 → 6 加一台 worker、把副本迁过去 → 7 删表：内部清理
--   文末：重复演示前怎么把加的节点去掉
--
-- 前提：tpcc_demo.sql 第 1–3 步做完（customer / orders 已建好、有数据）。
-- 一个 psql 连 master（:5432），下面所有命令都在这一个窗口里敲：
--     docker exec -it -e PGCLIENTENCODING=UTF8 -u postgres pg-test-container \
--         /work/pg-install/bin/psql -h /tmp -p 5432 -U postgres
-- 节点：master :5432；worker :5433 / :5434 / :5435 / :5436，
--       数据目录 /work/pg-cluster-data/worker1 … worker4（端口减 5432 就是编号）；第 6 步新加 :5437（worker5）。
-- ⚠ \c 这类反斜杠命令要单独一行回车，不要和 SQL 一起粘贴；\watch、\gset 跟在查询末尾、和查询一起粘贴没问题。
--
-- 背景（这次要看的东西）：
--   · 每个分片是一个 Raft 组，组号就是分片号（pg_dist_shard.shardid），3 个成员 = 1 主 2 从。
--     master 不存数据，只做路由和发时间戳（TSO）；另有一个控制面组（组 0，5 个节点都在）登记每个分片现在的主是谁。
--   · 主上每条本地 WAL 记录同时写进这个分片的分区日志（pg_parwal），作为一条 Raft 日志条目复制给两个从，
--     多数派落盘才算提交、才返回给客户端。
--   · 从只把日志收下落盘，不马上回放进表（从上的表文件是 0 字节的"壳表"，也不允许直接查询）；
--     升主的时候才回放 —— 惰性回放。
-- ════════════════════════════════════════════════════════════════════════════


-- ════ 第 1 步：定义观察函数 ════
-- Raft 状态和日志都在各 worker 本地（partdist.pg_raft_group_status()、partdist.raft_log），
-- 用 Citus 的 run_command_on_workers 到每台去读，再在 master 上拼起来。三个函数放在 ha 模式里。
CREATE SCHEMA IF NOT EXISTS ha;

-- 分区日志一条记录的类别：flags 低 4 位 = DATA 1 / MARKER 2 / CTRL 4 / DTX 8；
-- DATA 记录就是主节点本地 WAL 的原样字节，rmid / info 与 pg_waldump 的叫法一致
CREATE OR REPLACE FUNCTION ha.parwal_kind(rmid int, info int, flags int) RETURNS text
LANGUAGE sql STABLE AS $$
SELECT CASE
  WHEN flags & 8 = 8 THEN 'DTX '  || coalesce((ARRAY['PREPARE','DECISION','COMMIT','ABORT','FORGET'])[info], info::text)
  WHEN flags & 4 = 4 THEN 'CTRL ' || coalesce((ARRAY['FILESET_UPDATE','FREEZE_UPDATE','SHARD_CLOG','SHARD_DROP',
                                                     'DDL_HINT','SHARD_MVCC','BASELINE_END'])[info], info::text)
  WHEN flags & 2 = 2 THEN 'TXN '  || coalesce((ARRAY['COMMIT','PREPARE','ABORT','COMMIT_PREPARED','ABORT_PREPARED'])[((info & 112) >> 4) + 1], info::text)
  ELSE coalesce((SELECT rm_name FROM pg_get_wal_resource_managers() WHERE rm_id = rmid), 'rmid ' || rmid) || ' ' ||
       coalesce(CASE rmid
         WHEN 0  THEN (ARRAY['CHECKPOINT_SHUTDOWN','CHECKPOINT_ONLINE','NOOP','NEXTOID','SWITCH','BACKUP_END',
                             'PARAMETER_CHANGE','RESTORE_POINT','FPW_CHANGE','END_OF_RECOVERY','FPI_FOR_HINT','FPI'])[(info >> 4) + 1]
         WHEN 9  THEN (ARRAY['REWRITE','PRUNE','VACUUM','FREEZE_PAGE','VISIBLE','MULTI_INSERT','LOCK_UPDATED','NEW_CID'])[((info & 112) >> 4) + 1]
         WHEN 10 THEN (ARRAY['INSERT','DELETE','UPDATE','TRUNCATE','HOT_UPDATE','CONFIRM','LOCK','INPLACE'])[((info & 112) >> 4) + 1]
         WHEN 11 THEN (ARRAY['INSERT_LEAF','INSERT_UPPER','INSERT_META','SPLIT_L','SPLIT_R','INSERT_POST','DEDUP','DELETE',
                             'UNLINK_PAGE','UNLINK_PAGE_META','MARK_PAGE_HALFDEAD','VACUUM','REUSE_PAGE','META_CLEANUP'])[(info >> 4) + 1]
       END, 'info ' || info)
END
$$;

-- 一张表每个分片在每台 worker 上的 Raft 状态（key 给了就只看这个分片键值所在的分片）
--   last_index / commit_index / applied：本节点 Raft 日志的末尾、已提交、已应用位置
--   parwal：本节点分区日志落盘到的位置；table_bytes：本节点这份分片表文件的大小
--   replica_ok：这个从能不能顶上（回放槽位就绪、没有分叉标记）；主这一列为空
CREATE OR REPLACE FUNCTION ha.raft_status(tbl regclass, key int DEFAULT NULL)
RETURNS TABLE (shard text, worker text, role text, term bigint, leader text,
               last_index bigint, commit_index bigint, applied bigint, parwal bigint, table_bytes bigint,
               replica_ok boolean)
LANGUAGE sql AS $f$
WITH s AS (
  SELECT shardid, shard_name(logicalrelid, shardid) AS shard_name
    FROM pg_dist_shard
   WHERE logicalrelid = tbl
     AND (key IS NULL OR shardid = get_shard_id_for_distribution_column(tbl, key))
), w AS (
  SELECT nodeport, success, result
    FROM run_command_on_workers(format($q$
      SELECT coalesce(json_agg(json_build_object(
               'gid', g.group_id, 'state', g.state, 'term', g.current_term, 'leader', g.leader_node_id,
               'last', g.last_log_index, 'commit', g.commit_index, 'applied', g.last_applied,
               'parwal', partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(g.group_id)),
               'bytes',  pg_relation_size(partdist.local_partition_for_shard(g.group_id)),
               'ok', CASE WHEN g.state <> 'leader' THEN coalesce(
                       (SELECT armed AND state <> 'failed' AND partdist.shard_divergence(shard) IS NULL
                          FROM partdist.replay_status()
                         WHERE shard = partdist.local_partition_for_shard(g.group_id)), false) END)), '[]')
        FROM partdist.pg_raft_group_status() g
       WHERE g.group_id = ANY (%L::bigint[])$q$, (SELECT array_agg(shardid) FROM s)))
)
SELECT s.shard_name, ':' || w.nodeport, r.state, r.term, ':' || nm.port,
       r.last, r.commit, r.applied, r.parwal, r.bytes, r.ok
  FROM w
  CROSS JOIN LATERAL json_to_recordset(CASE WHEN w.success THEN w.result::json ELSE '[]' END)
        AS r(gid bigint, state text, term bigint, leader int, last bigint, commit bigint,
             applied bigint, parwal bigint, bytes bigint, ok boolean)
  JOIN s ON s.shardid = r.gid
  LEFT JOIN partdist.node_map nm ON nm.node_id = r.leader
UNION ALL
SELECT '-', ':' || nodeport, '连不上', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
  FROM w WHERE NOT success
ORDER BY 1, 2
$f$;

-- 某个分片键值所在分片的 Raft 日志：每台 worker 上最后 n 条，解出每条是什么记录
CREATE OR REPLACE FUNCTION ha.raft_log(tbl regclass, key int, n int DEFAULT 5)
RETURNS TABLE (worker text, role text, log_index bigint, term bigint, record text,
               gxid bigint, bytes int, orig_lsn pg_lsn, committed boolean)
LANGUAGE sql AS $f$
WITH w AS (
  SELECT nodeport, success, result
    FROM run_command_on_workers(format($q$
      SELECT coalesce(json_agg(json_build_object(
               'state', (SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = l.group_id),
               'idx', l.log_index, 'term', l.term, 'op', l.op_type, 'p', l.payload, 'committed', l.committed)), '[]')
        FROM (SELECT * FROM partdist.raft_log WHERE group_id = %s ORDER BY log_index DESC LIMIT %s) l$q$,
      get_shard_id_for_distribution_column(tbl, key), n))
)
SELECT ':' || w.nodeport, r.state, r.idx, r.term,
       CASE WHEN r.op = 'OP_PARWAL'
            THEN ha.parwal_kind((r.p->>'rmid')::int, (r.p->>'info')::int, (r.p->>'flags')::int)
            ELSE r.op END,
       (r.p->>'gxid')::bigint, (r.p->>'nbytes')::int, (r.p->>'orig_lsn')::pg_lsn, r.committed
  FROM w
  CROSS JOIN LATERAL json_to_recordset(CASE WHEN w.success THEN w.result::json ELSE '[]' END)
        AS r(state text, idx bigint, term bigint, op text, p jsonb, committed boolean)
UNION ALL
SELECT ':' || nodeport, '连不上', NULL, NULL, NULL, NULL, NULL, NULL, NULL FROM w WHERE NOT success
ORDER BY 1, 3
$f$;


-- ════ 第 2 步：一次写入在 Raft 里的样子 ════
-- 2.1 每个分片在每台 worker 上的状态：每个分片 3 行（1 leader 2 follower），三台的 last_index = commit_index = parwal；
--     follower 的 table_bytes = 0 —— 数据只在日志里，没有回放进表文件（惰性回放）
SELECT * FROM ha.raft_status('customer');

-- 2.2 写一行（仓库 1），看它在 Raft 日志里变成了什么
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance) VALUES (1, 3, 1, 'Raft', 'One', 100);
SELECT * FROM ha.raft_log('customer', 1, 3);
-- 一条 INSERT = 3 条日志：Heap INSERT（堆元组）、Btree INSERT_LEAF（主键索引）、TXN COMMIT（提交标记）。
-- 三台 worker 上 log_index、term、orig_lsn 都一样：主把这 3 条复制给两个从、落盘之后 INSERT 才返回。
-- 从上最后一条的 committed 可能还是 f：提交位置随主的下一次心跳（1 s）才传到从，再查一次就是 t。
-- orig_lsn 是这条记录在主节点本地 WAL 里的位置 —— 日志条目里装的就是主的 WAL 记录原样；
-- Heap / Btree 那两条字节数大，是 checkpoint 之后第一次改这一页带的整页镜像（FPI），和原生 WAL 一样。

-- 2.3 跨分片事务：两阶段提交的每一步也是 Raft 日志
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last, c_balance) VALUES (2, 3, 1, 'Raft', 'Two', 100);
BEGIN;
UPDATE customer SET c_balance = c_balance - 30 WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 30 WHERE c_w_id = 2 AND c_d_id = 3 AND c_id = 1;
COMMIT;
SELECT * FROM ha.raft_log('customer', 1, 7) WHERE role = 'leader';
SELECT * FROM ha.raft_log('customer', 2, 7) WHERE role = 'leader';
-- 两个分片各有：Heap HOT_UPDATE（改余额）→ DTX PREPARE + TXN PREPARE（第一阶段，各自落盘）；
-- 然后协调组（这笔事务里是仓库 1 的分片）写 DTX DECISION（全局决议），参与组写 DTX COMMIT，各自 TXN COMMIT（第二阶段）；
-- 过几秒协调组再写一条 DTX FORGET 收尾（n 调大能看到）。
-- 决议本身也是一条 Raft 日志，已经复制到多数派：协调组的主这时挂了，新主照样知道这笔事务该提交。

-- 2.4 再看仓库 1 这个分片：三台的日志位置一起往前走了
SELECT * FROM ha.raft_status('customer', 1);


-- ════ 第 3 步：停掉一个 worker —— 切主、读写 ════
-- 3.1 记下仓库 1 的两个分片（customer / orders 共置，同一仓库落在同一位置），看它们的主
SELECT shard_name('customer', get_shard_id_for_distribution_column('customer', 1)) AS c1,
       shard_name('orders',   get_shard_id_for_distribution_column('orders', 1))   AS o1 \gset
SELECT shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1');
-- 主在 :5433（worker1）。下面把它停掉；主不在 :5433 的话，把命令里的 worker1 换成对应目录。

-- （可选）另开一个终端跑持续写入脚本，停机期间能看到写到仓库 1 的那些失败约半分钟、其余照常：
--     bash pg-partdist-src/demo/txn_demo_writer.sh
--     第 7 步删表之前一定要 Ctrl+C 停掉它

-- 3.2 直接停掉 :5433。-m immediate 不做 checkpoint，等同断电。
--     COPY … TO PROGRAM 在 master 所在机器上以 postgres 用户执行命令（要超级用户），worker 都在同一个容器里
COPY (SELECT 1) TO PROGRAM '/work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/worker1 -m immediate stop';

-- 3.3 马上读：仓库 1 的主刚停，报 Connection refused；仓库 2 不受影响
SELECT c_w_id, c_id, c_first, c_balance FROM customer WHERE c_w_id = 1 AND c_d_id = 1;
SELECT c_w_id, c_id, c_first, c_balance FROM customer WHERE c_w_id = 2 AND c_d_id = 1;

-- 3.4 等它切主（每 3 秒查一次，最多 75 秒；两行的 primary 都变了、term 加 1 就可以 Ctrl+C）：
--     ① 从收不到心跳，等满选举超时（pg_raft.election_timeout_ms = 15 s）后发起选举，拿到多数票当选；
--     ② 新主先把还没回放的日志回放进表（升主前置）—— 惰性回放的账在这里付，积压一千多条约 5–20 s；
--     ③ 向控制面（组 0）登记新主，master 的路由（partition_map、pg_dist_placement）随之切换。
--     实测从停机到能读写 30–45 s。两个分片是两个独立的 Raft 组，各自选举，新主可能落在不同 worker 上；
--     master 每 5 s 检查一次共置关系，登记稳定 15 s 后，请 orders 的现任主把领导权转交给 customer 的主所在节点
--     （先挡住新写入、把那台追平，再让它竞选），所以有时会看到 orders 的 term 再加 1、primary 变成和 customer 一样。
--     两行 primary 相同、term 不再变就可以 Ctrl+C（最多 2 分钟）
SELECT clock_timestamp()::time(0) AS t, shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1') \watch i=3 c=40

-- 3.5 Raft 视角：:5433 连不上；剩下两个成员里一个当选 leader，term 加 1；
--     新主的 table_bytes 不再是 0（升主时回放进了表文件）
SELECT * FROM ha.raft_status('customer', 1);
SELECT * FROM ha.raft_status('orders', 1);

-- 3.6 新主上照常读写（worker 一列是实际执行的节点）
SELECT c_w_id, c_d_id, c_id, c_first, c_balance,
       tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker
  FROM customer WHERE c_w_id = 1 AND c_d_id IN (1, 3) ORDER BY c_d_id, c_id;
UPDATE customer SET c_balance = c_balance + 5 WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt) VALUES (1, 3, 1, 1, 5);
SELECT o_w_id, o_d_id, o_id, o_c_id, tableoid::regclass::text AS shard, ':' || current_setting('port') AS worker
  FROM orders WHERE o_w_id = 1 AND o_d_id = 3;
-- 跨分片事务也照常提交（仓库 1 的新主 + 仓库 2）
BEGIN;
UPDATE customer SET c_balance = c_balance - 10 WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
UPDATE customer SET c_balance = c_balance + 10 WHERE c_w_id = 2 AND c_d_id = 3 AND c_id = 1;
COMMIT;
SELECT c_w_id, c_first, c_last, c_balance FROM customer WHERE c_d_id = 3 ORDER BY c_w_id;

-- 3.7 共置连接：同一仓库的客户和订单在同一个节点上连接（Citus 把整条连接发给一台 worker，两张表的主必须在同一台）
SELECT c.c_first, c.c_last, o.o_id, o.o_ol_cnt, ':' || current_setting('port') AS worker
  FROM customer c JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 WHERE c.c_w_id = 1 AND c.c_d_id = 3;
-- 对齐之前（3.4 里两行 primary 还不一样的那十几秒）做这条连接会报"不允许在本节点上对副本壳表执行查询"
-- —— 另一张表在那台上只是副本壳表；单表读写、跨分片事务不受影响。对齐的记录在 master 上：
SELECT * FROM partdist.colocation_align_attempt ORDER BY attempted_at DESC LIMIT 4;


-- ════ 第 4 步：把 :5433 拉起来 —— 重新加入 Raft 组 ════
COPY (SELECT 1) TO PROGRAM '/work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/worker1 -l /work/pg-cluster-data/worker1/pg.log start';

-- 4.1 控制面：:5433 从 down 变回 active
SELECT node_id, ':' || port AS worker, status FROM partdist.node_map ORDER BY node_id;

-- 4.2 它回来时以 follower 身份进组，认了新主的 term，不会回来抢主（PreVote）；
--     它停机期间缺的日志由新主补发（只补缺的那段：新主记着它停机前确认到哪），:5433 的 last_index 追上 leader；
--     如果它这份数据从没当过从（本地没有回放槽位，replica_ok = f），新主还会给它重做一次物理基线，之后变 t。
--     每 15 秒查一次，最多 7 分半；:5433 两行的 last_index 和 leader 一样、replica_ok 是 t 就可以 Ctrl+C。
--     补发是后台通道一批批做的，有写入在排队时它每批之间让路，所以追平期间写入照常（开着写入脚本可以看）
SELECT clock_timestamp()::time(0) AS t, shard, worker, role, term, last_index, commit_index, replica_ok
  FROM ha.raft_status('customer', 1)
UNION ALL
SELECT clock_timestamp()::time(0), shard, worker, role, term, last_index, commit_index, replica_ok
  FROM ha.raft_status('orders', 1) \watch i=15 c=30

-- 4.3 其他分片里 :5433 本来就是从，停机期间落下的日志也补齐了
SELECT * FROM ha.raft_status('customer');


-- ════ 第 5 步：手动切主（计划内切换，比如维护前把主挪走） ════
-- 把仓库 1 的两个分片（customer 和共置的 orders 一起）切回 :5433（第 4 步里 :5433 追平之后再做）。
-- shardpg.move_primary 对每个分片做一次 Raft 领导权转交：
--   ① 先在目标节点上把副本预热回放到已提交的位置（基线收尾、能顶上才转）；
--   ② 现任主拿住这个组的复制认领位 —— 新的提交在门口排队，日志不再增长；
--   ③ 现任主把目标追平到自己的日志末尾，请目标竞选：目标的日志和主一样新，必然当选（term 加 1）；
--   ④ 新主把还没回放的日志回放进表、向控制面登记，路由切过来。排队的那几笔提交报"不是主"、重试即可。
-- 不直接在目标上强制竞选（pg_raft_group_campaign）：持续写入时目标的日志总比主短一点，选不上反而把主拉下来、整组重选。
SELECT shardpg.move_primary(:'c1', 5433);
SELECT shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1');
-- term 再加 1，两个分片的主都回到 :5433；共置连接正常
SELECT c.c_first, c.c_last, o.o_id, o.o_ol_cnt,
       c.tableoid::regclass::text AS c_shard, o.tableoid::regclass::text AS o_shard, ':' || current_setting('port') AS worker
  FROM customer c JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 WHERE c.c_w_id = 1 AND c.c_d_id = 3;


-- ════ 第 6 步：加一台 worker，把一份副本迁过去 ════
-- 6.1 操作系统层准备新节点：initdb → 照 worker4 复制配置（改端口、节点号，pg_raft.peers = 现有 5 个 + 它自己）
--     → 启动 → 建 citus / pg_partdist / pg_raft 扩展 → node_map 种子。新节点号 6、端口 5437、数据目录 worker5。
--     \! 在 psql 所在的机器上执行 shell 命令（这里 psql 就在容器里、以 postgres 用户运行），约 20 s
\! bash /work/pg-partdist-src/scripts/new_worker_node.sh 6 5437

-- 6.2 入簇，不重启任何节点（约 20 s）：
--   ① 各节点的 pg_raft.peers 末尾追加新节点：这个参数可以热加载（ALTER SYSTEM + pg_reload_conf），只许一次追加一个。
--      控制面组 0 的成员就是这张表，5 → 6 个，多数派 3 → 4；一次只加一个，新旧两个多数派必然相交，各节点先后 reload 的几秒里不会脑裂
--   ② 经控制面日志登记新节点地址（各节点 node_map 按日志序同时学到），等新节点追上控制面日志（落后太多直接装快照）
--   ③ Citus citus_add_node：同步元数据，各分布表的外壳建到新节点上（这时它上面还没有分片）
SELECT * FROM shardpg.add_node('127.0.0.1', 5437);

-- 6.3 六台都认了新成员：pg_raft.peers 6 项，控制面组 0 成员 6、提交位置一致
SELECT nodeport, result FROM run_command_on_workers($$
  SELECT 'peers ' || array_length(string_to_array(current_setting('pg_raft.peers'), ','), 1)
      || '，组 0 成员 ' || cluster_size || '，commit ' || commit_index
    FROM partdist.pg_raft_group_status() WHERE group_id = 0$$) ORDER BY 1;

-- 6.4 把仓库 1 这一段的一份从副本迁到新节点：customer 和共置的 orders 一起搬（同一段的组成员必须相同，主才对得齐）。
--     源节点取这一段的第二个从副本
SELECT ltrim(split_part(replicas, ',', 2), ':')::int AS from_port FROM shardpg.shards WHERE shard_name = :'c1' \gset
SELECT * FROM shardpg.move_replica(:'c1', :from_port, 5437);
--   ① 新节点入组：先在新节点上建组，再由 leader 走 Raft 日志加成员（单节点成员变更，3 → 4 个成员）
--   ② 供副本：leader 到新节点建副本表，发物理基线（经 Raft 复制），配回放位置；新节点追上日志
--   ③ 源节点出组（4 → 3），控制面改登记的从副本集
--   ④ 清理源节点上这两个组、副本表、回放登记
--   源节点要是主，会先把主切走（共置分片一起切）。实测 1–5 分钟（前面几步刚切过主、后台还在重供基线时偏慢）；全程可读写

-- 6.5 新节点上有了这两个分片的副本：日志位置与 leader 一致、replica_ok = t；源节点的行没了
SELECT * FROM ha.raft_status('customer', 1);
SELECT * FROM ha.raft_status('orders', 1);

-- 6.6 让新节点当主（和第 5 步一样的领导权转交，共置分片一起切），在它上面读写、做共置连接
SELECT shardpg.move_primary(:'c1', 5437);
SELECT shard_name, "primary", replicas, term FROM shardpg.shards WHERE shard_name IN (:'c1', :'o1');
UPDATE customer SET c_balance = c_balance + 1 WHERE c_w_id = 1 AND c_d_id = 3 AND c_id = 1;
SELECT c.c_first, c.c_balance, o.o_id, c.tableoid::regclass::text AS c_shard, o.tableoid::regclass::text AS o_shard,
       ':' || current_setting('port') AS worker
  FROM customer c JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
 WHERE c.c_w_id = 1 AND c.c_d_id = 3;


-- ════ 第 7 步：删表 —— 看内部清理 ════
-- （可选的写入脚本此时必须已经停掉）
-- 7.1 先记下两张表的全部分片号（= Raft 组号）
SELECT array_agg(shardid ORDER BY shardid) AS sids FROM pg_dist_shard WHERE logicalrelid IN ('customer'::regclass, 'orders'::regclass) \gset
SELECT nodeport, result AS before_drop FROM run_command_on_workers(format($$
  SELECT 'Raft 组 ' || (SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id = ANY (%1$L::bigint[]))
      || '，分片表 ' || (SELECT count(*) FROM unnest(%1$L::bigint[]) s, LATERAL (VALUES ('customer_' || s), ('orders_' || s)) t(name)
                         WHERE to_regclass(t.name) IS NOT NULL)$$, :'sids'));

-- 7.2 删表：一条 DROP，拆 Raft 组、删各节点上的分片表和副本壳表、删主从登记
DROP TABLE orders, customer;

-- 7.3 核对：master 上的主从登记、各 worker（含新加的 :5437）上的 Raft 组和分片表（含副本壳表）都没了
SELECT count(*) AS partition_map_rows FROM partdist.partition_map WHERE partition_id::bigint = ANY (:'sids'::bigint[]);
SELECT nodeport, result AS after_drop FROM run_command_on_workers(format($$
  SELECT 'Raft 组 ' || (SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id = ANY (%1$L::bigint[]))
      || '，分片表 ' || (SELECT count(*) FROM unnest(%1$L::bigint[]) s, LATERAL (VALUES ('customer_' || s), ('orders_' || s)) t(name)
                         WHERE to_regclass(t.name) IS NOT NULL)$$, :'sids'));

-- 7.4 收尾：删掉观察函数（会同步删各 worker 上的 ha 模式）
DROP SCHEMA ha CASCADE;


-- ════ 文末：重复演示前去掉第 6 步加的节点 ════
-- pg_raft.peers 只许热加载追加、不许热删除（各节点的复制游标按它的槽位存）；去掉节点要整簇重启一次 ——
-- master 的 TSO 发号上限现在落盘（$PGDATA/pg_tso_hwm），有数据也能重启。宿主机上执行（第 7 步删表之后）：
--     bash pg-partdist-src/demo/ha_demo_reset.sh          # 去掉 :5437：citus_remove_node → 各节点 peers 还原 → 整簇重启 → 删数据目录
-- 之后从 tpcc_demo.sql 第 1 步重新建表即可。
