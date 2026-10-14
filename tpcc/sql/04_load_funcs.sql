-- 装载函数（TPC-C §4.3.3.1）。每个函数一次只装一小块、各自一个事务（由
-- scripts/load.sh 逐块调用）：大事务会让分区 WAL 捕获环长时间背压，
-- 且一次失败要整块重来。所有块都按仓库号落在同一组共置分片上。

CREATE OR REPLACE FUNCTION tpcc.load_warehouse(w int) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO warehouse
    SELECT w, 300000.00, (tpcc.rand_int(0, 2000) / 10000.0)::numeric(4,4),
           tpcc.astring(6, 10), tpcc.astring(10, 20), tpcc.astring(10, 20),
           tpcc.astring(10, 20), tpcc.astring(2, 2), tpcc.zip();
    INSERT INTO district
    SELECT w, d, 30000.00, (tpcc.rand_int(0, 2000) / 10000.0)::numeric(4,4), 3001,
           tpcc.astring(6, 10), tpcc.astring(10, 20), tpcc.astring(10, 20),
           tpcc.astring(10, 20), tpcc.astring(2, 2), tpcc.zip()
      FROM generate_series(1, 10) d;
$$;

-- stock：每仓 10 万行，按 i_lo..i_hi 分块
CREATE OR REPLACE FUNCTION tpcc.load_stock(w int, i_lo int, i_hi int) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO stock
    SELECT w, i, tpcc.rand_int(10, 100), 0, 0, 0, tpcc.data_original(),
           tpcc.astring(24, 24), tpcc.astring(24, 24), tpcc.astring(24, 24),
           tpcc.astring(24, 24), tpcc.astring(24, 24), tpcc.astring(24, 24),
           tpcc.astring(24, 24), tpcc.astring(24, 24), tpcc.astring(24, 24),
           tpcc.astring(24, 24)
      FROM generate_series(i_lo, i_hi) i;
$$;

-- customer + history：每区 3000 个客户，每个客户一条 history
CREATE OR REPLACE FUNCTION tpcc.load_customer(w int, d int) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO customer
    SELECT w, d, c,
           (tpcc.rand_int(0, 5000) / 10000.0)::numeric(4,4),
           CASE WHEN random() < 0.10 THEN 'BC' ELSE 'GC' END,
           CASE WHEN c <= 1000 THEN tpcc.lastname(c - 1)
                ELSE tpcc.lastname(tpcc.nurand(255, 0, 999, tpcc.c_last_load())) END,
           tpcc.astring(8, 16), 50000.00, -10.00, 10.00, 1, 0,
           tpcc.astring(10, 20), tpcc.astring(10, 20), tpcc.astring(10, 20),
           tpcc.astring(2, 2), tpcc.zip(), tpcc.nstring(16, 16), now(), 'OE',
           tpcc.astring(300, 500)
      FROM generate_series(1, 3000) c;
    INSERT INTO history
    SELECT c, d, w, d, w, now(), 10.00, tpcc.astring(12, 24)
      FROM generate_series(1, 3000) c;
$$;

-- oorder + order_line + new_order：每区 3000 单，o_id > 2100 的未发货、进 new_order
CREATE OR REPLACE FUNCTION tpcc.load_orders(w int, d int) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO oorder
    SELECT w, d, o, tpcc.load_o_c_id(w, d, o),
           CASE WHEN o <= 2100 THEN tpcc.rand_int(1, 10) END,
           tpcc.load_ol_cnt(w, d, o), 1, now()
      FROM generate_series(1, 3000) o;
    INSERT INTO order_line
    SELECT w, d, o, ol, tpcc.rand_int(1, 100000),
           CASE WHEN o <= 2100 THEN now() END,
           CASE WHEN o <= 2100 THEN 0.00 ELSE (tpcc.rand_int(1, 999999) / 100.0)::numeric(6,2) END,
           w, 5, tpcc.astring(24, 24)
      FROM generate_series(1, 3000) o,
           LATERAL generate_series(1, tpcc.load_ol_cnt(w, d, o)) ol;
    INSERT INTO new_order
    SELECT w, d, o FROM generate_series(2101, 3000) o;
$$;
