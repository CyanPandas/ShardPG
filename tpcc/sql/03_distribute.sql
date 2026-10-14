-- 把 TPC-C 表接入 ShardPG：分布 → 建 Raft 组 + 供副本 → 打标。在协调者上执行：
--   psql -v shard_count=6 -v replicas=2 -f 03_distribute.sql
--
-- 顺序是硬约束，照 demo/shardpg_demo_functions.sql 的 demo.setup() 实跑过的流程：
--   ① item 在**本地表**状态下灌好，再 create_reference_table —— 打标集群上引用表
--      运行期禁写（T7.10），只能借 Citus 建引用表时的那次拷贝把数据带过去；
--   ② 其余 8 张表按仓库号共置分布，此时全部为空；
--   ③ 每个分片建 Raft 组、选主、供副本（raft_replicate_table_shards_bulk，分批）；
--   ④ 打标（set_table_shard_mvcc）—— 必须在写入任何数据之前，分片得是空的：
--      打标前写入的行带原生 xid，打标后按分片可见性规则不可见。
-- 失败都当失败处理（RAISE），不带着半接入的表往下走。

\set ON_ERROR_STOP on
SET citus.shard_replication_factor = 1;   -- 复制由 Raft 管，Citus 只放一份
SET citus.shard_count = :shard_count;

-- ① item：本地灌 10 万行，再转引用表
INSERT INTO item (i_id, i_name, i_price, i_data, i_im_id)
SELECT i, tpcc.astring(14, 24), (tpcc.rand_int(100, 10000) / 100.0)::numeric(5,2),
       tpcc.data_original(), tpcc.rand_int(1, 10000)
  FROM generate_series(1, 100000) i;
SELECT create_reference_table('item');

-- ② 共置分布（全部按仓库号）
SELECT create_distributed_table('warehouse',  'w_id');
SELECT create_distributed_table('district',   'd_w_id',  colocate_with => 'warehouse');
SELECT create_distributed_table('customer',   'c_w_id',  colocate_with => 'warehouse');
SELECT create_distributed_table('history',    'h_w_id',  colocate_with => 'warehouse');
SELECT create_distributed_table('oorder',     'o_w_id',  colocate_with => 'warehouse');
SELECT create_distributed_table('new_order',  'no_w_id', colocate_with => 'warehouse');
SELECT create_distributed_table('order_line', 'ol_w_id', colocate_with => 'warehouse');
SELECT create_distributed_table('stock',      's_w_id',  colocate_with => 'warehouse');

-- ③④ 接入函数：建组 + 供副本（分批 32、失败的单独重跑，最多 4 轮）→ 打标（重试到 90 s）
CREATE OR REPLACE FUNCTION tpcc.onboard(p_tbl regclass, p_replicas int)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    sids   bigint[];
    todo   bigint[];
    failed bigint[];
    r      record;
    i      int;
    bad    int := 0;
    detail text;
BEGIN
    SELECT array_agg(shardid ORDER BY shardid) INTO sids
      FROM pg_dist_shard WHERE logicalrelid = p_tbl;
    todo := sids;
    FOR i IN 1..4 LOOP
        bad := 0; detail := ''; failed := '{}';
        FOR r IN SELECT b.*
                   FROM generate_series(0, (coalesce(array_length(todo, 1), 0) - 1) / 32) wv(k),
                        LATERAL partdist.raft_replicate_table_shards_bulk(
                            p_tbl, p_replicas, 180, 3, (todo)[wv.k * 32 + 1 : wv.k * 32 + 32]) b
        LOOP
            IF r.status <> 'ok' THEN
                bad := bad + 1;
                failed := failed || r.shardid;
                detail := detail || format(E'  分片 %s：%s\n', r.shardid, left(r.status, 200));
            END IF;
        END LOOP;
        EXIT WHEN bad = 0;
        RAISE NOTICE '%：% 个分片建组/供副本没过，第 % 轮重跑', p_tbl, bad, i;
        todo := failed;
    END LOOP;
    IF bad > 0 THEN
        RAISE EXCEPTION E'%：建组 / 供副本失败 % 个分片：\n%', p_tbl, bad, detail;
    END IF;

    FOR i IN 1..30 LOOP
        bad := 0; detail := '';
        PERFORM partdist.reconcile_routing();
        FOR r IN SELECT * FROM partdist.set_table_shard_mvcc(p_tbl) LOOP
            IF r.status !~ '^(registered|already)' THEN
                bad := bad + 1;
                detail := detail || format(E'  分片 %s（:%s）：%s\n', r.shardid, r.node_port, r.status);
            END IF;
        END LOOP;
        EXIT WHEN bad = 0;
        PERFORM pg_sleep(3);
    END LOOP;
    IF bad > 0 THEN
        RAISE EXCEPTION E'%：打标失败 % 个分片：\n%', p_tbl, bad, detail;
    END IF;
    RETURN format('%s：%s 个分片已接入（1 主 + %s 副本）', p_tbl, array_length(sids, 1), p_replicas);
END $$;

SELECT tpcc.onboard('warehouse',  :replicas);
SELECT tpcc.onboard('district',   :replicas);
SELECT tpcc.onboard('customer',   :replicas);
SELECT tpcc.onboard('history',    :replicas);
SELECT tpcc.onboard('oorder',     :replicas);
SELECT tpcc.onboard('new_order',  :replicas);
SELECT tpcc.onboard('order_line', :replicas);
SELECT tpcc.onboard('stock',      :replicas);
