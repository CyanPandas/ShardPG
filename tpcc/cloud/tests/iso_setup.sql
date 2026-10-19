-- 跨节点读隔离测试的准备：建一张 72 行的"账户"表，按 w 与 warehouse 共置分布（行散在所有 worker 上），每行 100000，总和 7,200,000。
-- psql -v tbl=<表名> -v shardpg=<1|0> -f iso_setup.sql。ShardPG 上打标过的表删不掉，所以每次用新表名。
\set ON_ERROR_STOP on
CREATE TABLE :"tbl" (w int PRIMARY KEY, bal bigint NOT NULL);
SELECT create_distributed_table(:'tbl', 'w', colocate_with => 'warehouse');
\if :shardpg
-- ShardPG：每个分片建单成员 Raft 组并打标（必须在写数据之前），与 03_distribute.sql 对 TPC-C 表的做法相同
SELECT tpcc.onboard(:'tbl', 0);
\endif
BEGIN;
SELECT tpcc.begin_global();
INSERT INTO :"tbl" SELECT w, 100000 FROM generate_series(1, 72) w;
COMMIT;
-- 转账：两行在不同分片上（多半在不同节点上），按 w 从小到大更新，减少死锁
CREATE OR REPLACE FUNCTION tpcc.iso_xfer(t text, a int, b int, x int) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM tpcc.begin_global();
    IF a < b THEN
        EXECUTE format('UPDATE %I SET bal = bal - $1 WHERE w = $2', t) USING x, a;
        EXECUTE format('UPDATE %I SET bal = bal + $1 WHERE w = $2', t) USING x, b;
    ELSE
        EXECUTE format('UPDATE %I SET bal = bal + $1 WHERE w = $2', t) USING x, b;
        EXECUTE format('UPDATE %I SET bal = bal - $1 WHERE w = $2', t) USING x, a;
    END IF;
END $$;
