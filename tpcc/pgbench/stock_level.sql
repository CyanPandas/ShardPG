-- TPC-C stock_level：终端绑定主仓库 = client_id % nw + 1（16 终端 / 10 仓时，1..6 号仓各 2 个终端）
\set w :client_id % :nw + 1
SELECT tpcc.stock_level(:w);
