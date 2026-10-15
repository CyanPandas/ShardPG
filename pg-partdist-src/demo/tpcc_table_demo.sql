-- ════════════════════════════════════════════════════════════════════════════
-- ShardPG 建表逻辑演示：TPC-C 客户表 customer + 订单表 orders
--
-- 在 master（:5432）的 psql 里执行（P7-N87–N90：内核补丁 0016 / psql 补丁 0017 / shardpg_ddl.c）：
--   docker exec -it -u postgres pg-test-container /work/pg-install/bin/psql -h /tmp -p 5432 -U postgres
--
-- 一共四步：1 建表 → 2 分区信息（主从）→ 3 表与分区的对应 → 4 删表
-- ════════════════════════════════════════════════════════════════════════════
\pset footer off

\echo '════════════════ 第 1 步：建表 ════════════════'
-- 建表时直接指定：分片键、分片数、副本数（1 主 + n 从）、共置。
-- 一条语句完成建表、分布、建 Raft 组（选主 + 供副本）、接入分布式事务（打标），约 45 秒。
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

\echo '════════════════ 第 2 步：分区信息与主从 ════════════════'
-- 每个分片的物理表名、分片键、哈希范围、主、从、任期
SELECT * FROM shardpg.shards WHERE table_name IN ('customer', 'orders');

\echo '════════════════ 第 3 步：表和分区的对应 ════════════════'
-- \dt 表名：父表一行（分片键、分片数、副本数、共置），下面逐行列出子表（分片）及其主从
\dt customer
\dt orders

\echo '════════════════ 第 4 步：删表 ════════════════'
-- 一条 DROP：删表的同时自动拆 Raft 组、删各节点副本表、删主从登记
DROP TABLE orders, customer;
\dt customer
SELECT * FROM shardpg.shards WHERE table_name::text IN ('customer', 'orders');
