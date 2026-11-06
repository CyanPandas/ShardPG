# 热行压力：N 个会话对同一 stock 行做 +1，结束核对总增量（丢更新/报错即暴露）
P="/work/pg-install/bin/psql -h 10.77.110.145 -p 31080 -U postgres -XAtq"
q() { docker exec shardpg-fix-client $P -c "$1"; }
v0=$(q "select s_order_cnt from stock where s_w_id=${2:-3} and s_i_id=7")
cat > /tmp/hot.sql <<SQL
UPDATE stock SET s_order_cnt = s_order_cnt + 1 WHERE s_w_id=${2:-3} AND s_i_id=7;
SQL
docker cp /tmp/hot.sql shardpg-fix-client:/tmp/hot.sql
docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h 10.77.110.145 -p 31080 -U postgres -n -c ${1:-32} -j 8 -T 15 -f /tmp/hot.sql postgres 2>&1 | grep -E "processed|failed|tps|error" | head -6
v1=$(q "select s_order_cnt from stock where s_w_id=${2:-3} and s_i_id=7")
echo "before=$v0 after=$v1 delta=$((v1-v0))"
