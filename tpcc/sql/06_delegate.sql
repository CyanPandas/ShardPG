-- 事务函数下推（Citus function call delegation）：按仓库号 p_w 与 warehouse 共置，
-- `SELECT tpcc.new_order(w, nw)` 整体发到持有 w 的 worker 上执行 —— 本仓库分片全部
-- 本地访问，一笔事务一次网络往返，而不是每条语句一次。
--
-- 约 90% 的事务只碰本仓库：在 worker 上就是单节点事务，不走 2PC。
-- 剩下的（NewOrder 1% 远程供货行、Payment 15% 远程客户）由 worker 作为发起方对另一台
-- worker 做嵌套分布式访问，需要 citus.allow_nested_distributed_execution = on
-- （conf/node.conf.tmpl 已设）。
\set ON_ERROR_STOP on
SELECT create_distributed_function('tpcc.new_order(int,int)',  'p_w', colocate_with => 'warehouse');
SELECT create_distributed_function('tpcc.payment(int,int)',    'p_w', colocate_with => 'warehouse');
SELECT create_distributed_function('tpcc.order_status(int)',   'p_w', colocate_with => 'warehouse');
SELECT create_distributed_function('tpcc.delivery(int)',       'p_w', colocate_with => 'warehouse');
SELECT create_distributed_function('tpcc.stock_level(int)',    'p_w', colocate_with => 'warehouse');

-- 装载函数也按仓库号下推（2026-10-15）：不下推时 Citus 在协调者上为一块数据向同一台
-- worker 开多条 COPY 连接，于是每块都走 2PC。多副本下 2PC 的 PREPARE 要先把整块记录
-- 复制给副本，期间占着分片组的复制认领位，且 raft_log 行被 prepared 事务锁到
-- COMMIT PREPARED；与未决 2PC 清扫 / 判决复制交叉等待时只能靠 60 s 超时解开，
-- 随后的复制失败又触发整片重发基线（三副本 64 仓装载实测卡死在最后 5 块）。
-- 下推后每块是 worker 上的单机本地事务，与 TPC-C 事务函数走同一条路径。
SELECT create_distributed_function('tpcc.load_warehouse(int)',         'w', colocate_with => 'warehouse');
SELECT create_distributed_function('tpcc.load_stock(int,int,int)',     'w', colocate_with => 'warehouse');
SELECT create_distributed_function('tpcc.load_customer(int,int)',      'w', colocate_with => 'warehouse');
SELECT create_distributed_function('tpcc.load_orders(int,int)',        'w', colocate_with => 'warehouse');
