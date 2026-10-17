#!/bin/bash
# 路线 A 自动切主检查：负载 → 等备库追平 → 快照 → 只停 n3 主库（不做任何人工切换）
# → 等控制面 leader 的切主控制器提升备库、改地址与 Citus 路由 → 快照比对 + 一致性 + 切主后 TPC-C
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
PSQL="$CLI -h 10.77.110.148 -p 31080"
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
log "登记备库、打开自动切主"; bash shardpg_rep.sh register
log "负载（64 终端 60 s）"; bash scale.sh shardpg 64 2>&1 | tail -1
bash shardpg_rep.sh wait
log "切主前快照"; snap > /tmp/afo_before.txt; cat /tmp/afo_before.txt
log "停 n3 主库（147:31081），之后不做任何人工操作"
T0=$(date +%s.%N); ssh -n 10.77.110.147 "docker stop -t 1 shardpg-fix-n3" >/dev/null
NEW=""
for i in $(seq 240); do
  NEW=$($PSQL -c "select nodename||':'||nodeport from pg_dist_node where nodeport=31095 limit 1" 2>/dev/null)
  [ -n "$NEW" ] && break; sleep 0.5
done
T1=$(date +%s.%N)
[ -n "$NEW" ] || { log "240 s 内控制器没有切主"; ssh -n 10.77.110.148 "docker logs --since 5m shardpg-fix-n1 2>&1 | grep -E 'pg_raft: 节点 3|切主' | tail -20"; exit 1; }
log "Citus 路由已改到 $NEW（停机后 $(echo "$T1 - $T0" | bc) s）"
NH=${NEW%:*}
for i in $(seq 120); do
  l=$($CLI -h $NH -p 31095 -c "select count(*) filter (where state='leader') from partdist.pg_raft_group_status() where group_id<>0" 2>/dev/null)
  [ "$l" = 16 ] && $PSQL -c "select count(*) from warehouse" >/dev/null 2>&1 && break; sleep 0.5
done
T2=$(date +%s.%N); log "新主的分片组全部选出主、协调者可读（停机后 $(echo "$T2 - $T0" | bc) s）"
log "控制器日志"; ssh -n 10.77.110.148 "docker logs --since 5m shardpg-fix-n1 2>&1 | grep -E 'pg_raft: 节点 3' | cut -c1-220 | tail -12"
log "新主的复制：$($CLI -h $NH -p 31095 -F' ' -c 'select application_name, state, sync_state from pg_stat_replication') / $($CLI -h $NH -p 31095 -c 'show synchronous_standby_names')"
log "切主后快照"; snap > /tmp/afo_after.txt; cat /tmp/afo_after.txt
if diff /tmp/afo_before.txt /tmp/afo_after.txt >/dev/null; then log "切主前后完全一致"; else log "切主前后不一致："; diff /tmp/afo_before.txt /tmp/afo_after.txt; fi
log "一致性检查"; $PSQL -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "各节点的节点 3 地址 / 备库登记"
for hp in 10.77.110.148:31080 10.77.110.145:31082 10.77.110.148:31083 $NH:31095; do
  h=${hp%:*}; p=${hp#*:}
  echo "$hp: $($CLI -h $h -p $p -c "select host||':'||port from partdist.node_address where node_id=3") | $($CLI -h $h -p $p -c "select string_agg(name, ',' order by name) from partdist.node_standby where node_id=3")"
done
log "切主后 TPC-C（16 终端）"; bash scale.sh shardpg 16 2>&1 | tail -1
R2=$(ls -td tpcc/results/*/ | head -1); grep -E "ERROR" $R2/pgbench.err | cut -c1-200 | sort | uniq -c | head -4
log "切主后一致性"; $PSQL -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "完成"
