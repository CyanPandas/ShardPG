#!/bin/bash
# 路线 A 切主正确性检查：负载 → 等备库追平 → 记行数/校验和 → 切 n3 → 再记 → 比对 + 一致性检查
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
PSQL="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -h 10.77.110.148 -p 31080"
snap() {
  $PSQL -F' ' -c "select 'warehouse', count(*), sum(w_ytd)::numeric(20,2) from warehouse union all
    select 'district', count(*), sum(d_ytd)::numeric(20,2) from district union all
    select 'customer', count(*), sum(c_balance)::numeric(20,2) from customer union all
    select 'history', count(*), sum(h_amount)::numeric(20,2) from history union all
    select 'oorder', count(*), sum(o_ol_cnt) from oorder union all
    select 'new_order', count(*), sum(no_o_id) from new_order union all
    select 'order_line', count(*), sum(ol_amount)::numeric(20,2) from order_line union all
    select 'stock', count(*), sum(s_ytd) from stock union all
    select 'stock_minw', min(c), max(c) from (select count(*) c from stock group by s_w_id) z"
}
log "负载（64 终端 60 s）"; bash scale.sh shardpg 64 2>&1 | tail -1
bash shardpg_rep.sh wait
log "切主前快照"; snap > /tmp/fo_before.txt; cat /tmp/fo_before.txt
log "切主"; bash failover_n3.sh > /tmp/fo_new.txt 2>&1; grep -E "提升|备库收到|Citus|NEW=" /tmp/fo_new.txt
log "等新主的分片组选出主"
for i in $(seq 120); do
  l=$(docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -h $(grep -oE "NEW=[0-9.]+" /tmp/fo_new.txt | cut -d= -f2) -p 31095 -c "select count(*) filter (where state='leader') from partdist.pg_raft_group_status() where group_id<>0" 2>/dev/null)
  [ "$l" = 16 ] && $PSQL -c "select count(*) from warehouse" >/dev/null 2>&1 && break; sleep 1
done
log "切主后快照"; snap > /tmp/fo_after.txt; cat /tmp/fo_after.txt
if diff /tmp/fo_before.txt /tmp/fo_after.txt >/dev/null; then log "切主前后完全一致"; else log "切主前后不一致："; diff /tmp/fo_before.txt /tmp/fo_after.txt; fi
log "一致性检查"; $PSQL -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "各节点的节点 3 地址"; for hp in 10.77.110.148:31080 10.77.110.145:31082 10.77.110.148:31083 10.77.110.145:31095; do h=${hp%:*}; p=${hp#*:}; echo "$hp: $(docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -h $h -p $p -c "select host||':'||port from partdist.node_address where node_id=3")"; done
log "切主后 TPC-C（16 终端）"; bash scale.sh shardpg 16 2>&1 | tail -1
R2=$(ls -td tpcc/results/*/ | head -1); grep -E "ERROR" $R2/pgbench.err | cut -c1-200 | sort | uniq -c | head -4
log "切主后一致性"; $PSQL -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "完成"
