-- TPC-C 模式（ShardPG 版）。在协调者上执行。
--
-- 与通用 TPC-C 套件的三处不同，都是 ShardPG 的硬约束：
--   1. **不建外键**：外键检查的语义（快照之外的读）与分片表的 SI 可见性不一致，
--      本分支未验证这条路径；
--   2. **索引在建表时一次建全**：分片打标之后禁 CREATE INDEX（设计 §10），
--      通用套件"灌完数据再建索引"的顺序在这里会直接报错；
--   3. **热点小表压低 fillfactor**：分片表没有页内剪枝（on-access prune 被屏蔽），
--      HOT 更新只能用页上现成的空闲空间；warehouse/district 每行每秒被更新多次，
--      留足空间让它们尽量走 HOT，把空间回收留给分片 vacuum 的链处置。

SET citus.enable_ddl_propagation TO on;

DROP TABLE IF EXISTS order_line, new_order, oorder, history, customer, stock,
                     district, warehouse, item CASCADE;

CREATE TABLE warehouse (
    w_id        int           NOT NULL,
    w_ytd       numeric(12,2) NOT NULL,
    w_tax       numeric(4,4)  NOT NULL,
    w_name      varchar(10)   NOT NULL,
    w_street_1  varchar(20)   NOT NULL,
    w_street_2  varchar(20)   NOT NULL,
    w_city      varchar(20)   NOT NULL,
    w_state     char(2)       NOT NULL,
    w_zip       char(9)       NOT NULL,
    PRIMARY KEY (w_id)
) WITH (fillfactor = 10, autovacuum_enabled = off);

CREATE TABLE district (
    d_w_id       int           NOT NULL,
    d_id         int           NOT NULL,
    d_ytd        numeric(12,2) NOT NULL,
    d_tax        numeric(4,4)  NOT NULL,
    d_next_o_id  int           NOT NULL,
    d_name       varchar(10)   NOT NULL,
    d_street_1   varchar(20)   NOT NULL,
    d_street_2   varchar(20)   NOT NULL,
    d_city       varchar(20)   NOT NULL,
    d_state      char(2)       NOT NULL,
    d_zip        char(9)       NOT NULL,
    PRIMARY KEY (d_w_id, d_id)
) WITH (fillfactor = 10, autovacuum_enabled = off);

CREATE TABLE customer (
    c_w_id         int           NOT NULL,
    c_d_id         int           NOT NULL,
    c_id           int           NOT NULL,
    c_discount     numeric(4,4)  NOT NULL,
    c_credit       char(2)       NOT NULL,
    c_last         varchar(16)   NOT NULL,
    c_first        varchar(16)   NOT NULL,
    c_credit_lim   numeric(12,2) NOT NULL,
    c_balance      numeric(12,2) NOT NULL,
    c_ytd_payment  numeric(12,2) NOT NULL,
    c_payment_cnt  int           NOT NULL,
    c_delivery_cnt int           NOT NULL,
    c_street_1     varchar(20)   NOT NULL,
    c_street_2     varchar(20)   NOT NULL,
    c_city         varchar(20)   NOT NULL,
    c_state        char(2)       NOT NULL,
    c_zip          char(9)       NOT NULL,
    c_phone        char(16)      NOT NULL,
    c_since        timestamp     NOT NULL,
    c_middle       char(2)       NOT NULL,
    c_data         varchar(500)  NOT NULL,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
) WITH (fillfactor = 80, autovacuum_enabled = off);
CREATE INDEX customer_name_idx ON customer (c_w_id, c_d_id, c_last, c_first);

CREATE TABLE history (
    h_c_id    int           NOT NULL,
    h_c_d_id  int           NOT NULL,
    h_c_w_id  int           NOT NULL,
    h_d_id    int           NOT NULL,
    h_w_id    int           NOT NULL,
    h_date    timestamp     NOT NULL,
    h_amount  numeric(6,2)  NOT NULL,
    h_data    varchar(24)   NOT NULL
) WITH (autovacuum_enabled = off);

CREATE TABLE oorder (
    o_w_id        int       NOT NULL,
    o_d_id        int       NOT NULL,
    o_id          int       NOT NULL,
    o_c_id        int       NOT NULL,
    o_carrier_id  int,
    o_ol_cnt      int       NOT NULL,
    o_all_local   int       NOT NULL,
    o_entry_d     timestamp NOT NULL,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
) WITH (fillfactor = 90, autovacuum_enabled = off);
CREATE INDEX oorder_cust_idx ON oorder (o_w_id, o_d_id, o_c_id, o_id);

CREATE TABLE new_order (
    no_w_id  int NOT NULL,
    no_d_id  int NOT NULL,
    no_o_id  int NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
) WITH (autovacuum_enabled = off);

CREATE TABLE order_line (
    ol_w_id         int           NOT NULL,
    ol_d_id         int           NOT NULL,
    ol_o_id         int           NOT NULL,
    ol_number       int           NOT NULL,
    ol_i_id         int           NOT NULL,
    ol_delivery_d   timestamp,
    ol_amount       numeric(6,2)  NOT NULL,
    ol_supply_w_id  int           NOT NULL,
    ol_quantity     int           NOT NULL,
    ol_dist_info    char(24)      NOT NULL,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
) WITH (fillfactor = 90, autovacuum_enabled = off);

CREATE TABLE stock (
    s_w_id        int          NOT NULL,
    s_i_id        int          NOT NULL,
    s_quantity    int          NOT NULL,
    s_ytd         int          NOT NULL,
    s_order_cnt   int          NOT NULL,
    s_remote_cnt  int          NOT NULL,
    s_data        varchar(50)  NOT NULL,
    s_dist_01     char(24)     NOT NULL,
    s_dist_02     char(24)     NOT NULL,
    s_dist_03     char(24)     NOT NULL,
    s_dist_04     char(24)     NOT NULL,
    s_dist_05     char(24)     NOT NULL,
    s_dist_06     char(24)     NOT NULL,
    s_dist_07     char(24)     NOT NULL,
    s_dist_08     char(24)     NOT NULL,
    s_dist_09     char(24)     NOT NULL,
    s_dist_10     char(24)     NOT NULL,
    PRIMARY KEY (s_w_id, s_i_id)
) WITH (fillfactor = 80, autovacuum_enabled = off);

-- item：只读，做成 Citus 引用表。引用表在打标集群上运行期禁写（T7.10），
-- 所以先当普通本地表灌好，再由 create_reference_table 把现有数据拷到各节点
-- （见 03_distribute.sql）。
CREATE TABLE item (
    i_id     int           NOT NULL,
    i_name   varchar(24)   NOT NULL,
    i_price  numeric(5,2)  NOT NULL,
    i_data   varchar(50)   NOT NULL,
    i_im_id  int           NOT NULL,
    PRIMARY KEY (i_id)
);
