-- 根据系统功能演示报告第1.3节；只替换表名前缀，保留全部字段和约束。

CREATE TABLE {{prefix}}customer (
    c_w_id         int           NOT NULL,
    c_d_id         int           NOT NULL,
    c_id           int           NOT NULL,
    c_first        varchar(16),
    c_middle       char(2),
    c_last         varchar(16),
    c_street_1     varchar(20),
    c_street_2     varchar(20),
    c_city         varchar(20),
    c_state        char(2),
    c_zip          char(9),
    c_phone        char(16),
    c_since        timestamp,
    c_credit       char(2),
    c_credit_lim   numeric(12,2),
    c_discount     numeric(4,4),
    c_balance      numeric(12,2),
    c_ytd_payment  numeric(12,2),
    c_payment_cnt  int,
    c_delivery_cnt int,
    c_data         varchar(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id),
    SHARD KEY (c_w_id) SHARDS 4 REPLICAS 2
);

CREATE TABLE {{prefix}}warehouse (
    w_id       int           NOT NULL,
    w_name     varchar(10),
    w_street_1 varchar(20),
    w_street_2 varchar(20),
    w_city     varchar(20),
    w_state    char(2),
    w_zip      char(9),
    w_tax      numeric(4,4),
    w_ytd      numeric(12,2),
    PRIMARY KEY (w_id)
);

CREATE TABLE {{prefix}}district (
    d_w_id      int           NOT NULL,
    d_id        int           NOT NULL,
    d_name      varchar(10),
    d_street_1  varchar(20),
    d_street_2  varchar(20),
    d_city      varchar(20),
    d_state     char(2),
    d_zip       char(9),
    d_tax       numeric(4,4),
    d_ytd       numeric(12,2),
    d_next_o_id int,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE {{prefix}}history (
    h_c_id   int,
    h_c_d_id int,
    h_c_w_id int,
    h_d_id   int,
    h_w_id   int,
    h_date   timestamp,
    h_amount numeric(6,2),
    h_data   varchar(24)
);

CREATE TABLE {{prefix}}new_order (
    no_w_id int NOT NULL,
    no_d_id int NOT NULL,
    no_o_id int NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE {{prefix}}orders (
    o_w_id       int       NOT NULL,
    o_d_id       int       NOT NULL,
    o_id         int       NOT NULL,
    o_c_id       int,
    o_entry_d    timestamp,
    o_carrier_id int,
    o_ol_cnt     int,
    o_all_local  int,
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    SHARD KEY (o_w_id) COLOCATE WITH {{prefix}}customer
);

CREATE TABLE {{prefix}}order_line (
    ol_w_id        int          NOT NULL,
    ol_d_id        int          NOT NULL,
    ol_o_id        int          NOT NULL,
    ol_number      int          NOT NULL,
    ol_i_id        int          NOT NULL,
    ol_supply_w_id int,
    ol_delivery_d  timestamp,
    ol_quantity    int,
    ol_amount      numeric(6,2),
    ol_dist_info   char(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE TABLE {{prefix}}item (
    i_id    int          NOT NULL,
    i_im_id int,
    i_name  varchar(24),
    i_price numeric(5,2),
    i_data  varchar(50),
    PRIMARY KEY (i_id)
);

CREATE TABLE {{prefix}}stock (
    s_w_id       int         NOT NULL,
    s_i_id       int         NOT NULL,
    s_quantity   int,
    s_dist_01    char(24),
    s_dist_02    char(24),
    s_dist_03    char(24),
    s_dist_04    char(24),
    s_dist_05    char(24),
    s_dist_06    char(24),
    s_dist_07    char(24),
    s_dist_08    char(24),
    s_dist_09    char(24),
    s_dist_10    char(24),
    s_ytd        int,
    s_order_cnt  int,
    s_remote_cnt int,
    s_data       varchar(50),
    PRIMARY KEY (s_w_id, s_i_id)
);
