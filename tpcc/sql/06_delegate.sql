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
