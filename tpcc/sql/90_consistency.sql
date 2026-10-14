-- TPC-C 一致性条件（§3.3.2.1–§3.3.2.4），压测结束后在协调者上执行。
-- 每条输出违反的行数，全部为 0 才算通过。
-- 写成"按分布列 join 聚合子查询"的形式：Citus 能把它整个下推到各分片，
-- 相关子查询 + OR 的写法则可能被拒绝。

\echo '[1] W_YTD = sum(D_YTD)（§3.3.2.1）'
SELECT count(*) AS violations
  FROM warehouse w
  JOIN (SELECT d_w_id, sum(d_ytd) AS s FROM district GROUP BY d_w_id) d ON d.d_w_id = w.w_id
 WHERE w.w_ytd <> d.s;

\echo '[2] D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)（§3.3.2.2；该区 new_order 为空时只比前一半）'
SELECT count(*) AS violations
  FROM district d
  JOIN (SELECT o_w_id, o_d_id, max(o_id) AS m FROM oorder GROUP BY o_w_id, o_d_id) o
    ON o.o_w_id = d.d_w_id AND o.o_d_id = d.d_id
  LEFT JOIN (SELECT no_w_id, no_d_id, max(no_o_id) AS m FROM new_order GROUP BY no_w_id, no_d_id) n
    ON n.no_w_id = d.d_w_id AND n.no_d_id = d.d_id
 WHERE d.d_next_o_id - 1 <> o.m
    OR (n.m IS NOT NULL AND d.d_next_o_id - 1 <> n.m);

\echo '[3] max(NO_O_ID) - min(NO_O_ID) + 1 = count(new_order)（§3.3.2.3）'
SELECT count(*) AS violations FROM (
    SELECT no_w_id, no_d_id, max(no_o_id) - min(no_o_id) + 1 AS span, count(*) AS n
      FROM new_order GROUP BY no_w_id, no_d_id) s
 WHERE span <> n;

\echo '[4] sum(O_OL_CNT) = count(order_line)（§3.3.2.4）'
SELECT count(*) AS violations
  FROM (SELECT o_w_id, o_d_id, sum(o_ol_cnt) AS want FROM oorder GROUP BY o_w_id, o_d_id) o
  JOIN (SELECT ol_w_id, ol_d_id, count(*) AS got FROM order_line GROUP BY ol_w_id, ol_d_id) l
    ON l.ol_w_id = o.o_w_id AND l.ol_d_id = o.o_d_id
 WHERE o.want <> l.got;

\echo '[5] 每单订单行数 = o_ol_cnt（逐单）'
SELECT count(*) AS violations
  FROM oorder o
  LEFT JOIN (SELECT ol_w_id, ol_d_id, ol_o_id, count(*) AS got FROM order_line
              GROUP BY ol_w_id, ol_d_id, ol_o_id) l
    ON l.ol_w_id = o.o_w_id AND l.ol_d_id = o.o_d_id AND l.ol_o_id = o.o_id
 WHERE l.got IS DISTINCT FROM o.o_ol_cnt;
