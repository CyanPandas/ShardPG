-- 跨节点提交原子性的不变式（TPC-C 套件自带的 90_consistency.sql 只核对同一仓库内、共置在同一节点上的数据）。
-- 按仓库号逐个核对；每项 violations 都必须是 0。只在没有负载、2PC 都已收尾时执行。
--   远程 Payment：客户行在客户所在仓的节点上改 c_balance，history 行写在付款仓的节点上（h_c_w_id = 客户所在仓）
--   远程 NewOrder：stock 行在供货仓的节点上改 s_ytd / s_order_cnt / s_remote_cnt，order_line 写在下单仓的节点上
-- 装载时的初值：c_balance = -10、每个客户一条 h_amount = 10 的 history、已送达订单行 ol_amount = 0；
--   stock 的 s_ytd / s_order_cnt / s_remote_cnt = 0；初始订单（o_id <= 3000）的订单行都是本仓供货、ol_quantity = 5。
\set ON_ERROR_STOP on

\echo '[X1] Payment：每仓 Σc_balance = Σ已送达 ol_amount − Σh_amount（history 按客户所在仓 h_c_w_id 归集）（TPC-C §3.3.2.10 的按仓汇总形式）'
WITH c AS (SELECT c_w_id AS w, sum(c_balance) AS bal FROM customer GROUP BY 1),
     o AS (SELECT ol_w_id AS w, sum(ol_amount) AS amt FROM order_line WHERE ol_delivery_d IS NOT NULL GROUP BY 1),
     h AS (SELECT h_c_w_id AS w, sum(h_amount) AS amt FROM history GROUP BY 1)
SELECT count(*) FILTER (WHERE c.bal <> o.amt - h.amt) AS violations, count(*) AS warehouses
  FROM c JOIN o USING (w) JOIN h USING (w);

\echo '[X2] NewOrder：每个供货仓 Σs_remote_cnt = 由它供货的远程订单行数；Σs_order_cnt = 由它供货的新订单行数；Σs_ytd = Σol_quantity − 5 × 初始订单行数'
WITH s AS (SELECT s_w_id AS w, sum(s_remote_cnt) AS rc, sum(s_order_cnt) AS oc, sum(s_ytd) AS ytd FROM stock GROUP BY 1),
     l AS (SELECT ol_supply_w_id AS w,
                  count(*) FILTER (WHERE ol_w_id <> ol_supply_w_id) AS rc,
                  count(*) FILTER (WHERE ol_o_id > 3000) AS oc,
                  sum(ol_quantity) AS qty,
                  count(*) FILTER (WHERE ol_o_id <= 3000) AS n0
             FROM order_line GROUP BY 1)
SELECT count(*) FILTER (WHERE s.rc <> l.rc) + count(*) FILTER (WHERE s.oc <> l.oc)
       + count(*) FILTER (WHERE s.ytd <> l.qty - 5 * l.n0) AS violations,
       count(*) AS warehouses, sum(l.rc) AS remote_order_lines, sum(l.oc) AS new_order_lines
  FROM s JOIN l USING (w);
