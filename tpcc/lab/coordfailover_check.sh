#!/bin/bash
# 路线 A 协调者自动切主检查（需 COORD_HA=1 建好协调者备库）：负载 → 追平 → 快照 + TSO 水位
# → 只停协调者 n1（148:31080），不做任何人工操作 → 控制面新 leader 提升协调者备库、改 Citus 协调者位置、
# 各节点 TSO 连接串自动对齐 → 经新协调者快照比对 + 一致性 + TSO 单调 + TPC-C
R=/data1/yyt/shardpg-fix-20261014; cd $R
export COORD_HA=1
log() { echo "[$(date +%T)] $*"; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
snap() {
  $CLI -h $1 -p $2 -F' ' -c "select 'warehouse', count(*), sum(w_ytd)::numeric(20,2) from warehouse union all
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
log "切主前快照"; snap 10.77.110.148 31080 > /tmp/cfo_before.txt; cat /tmp/cfo_before.txt
log "旧协调者 TSO：$($CLI -h 10.77.110.148 -p 31080 -c 'select partdist.partdist_tso_status()')"
log "停协调者 n1（148:31080），之后不做任何人工操作"
T0=$(date +%s.%N); ssh -n 10.77.110.148 "docker stop -t 1 shardpg-fix-n1" >/dev/null
NEW=""
for i in $(seq 360); do
  for h in 10.77.110.145 10.77.110.147; do
    r=$($CLI -h $h -p 31097 -c "select pg_is_in_recovery()::text || ' ' || (select nodename||':'||nodeport from pg_dist_node where groupid = 0)" 2>/dev/null)
    [ "$r" = "false $h:31097" ] && { NEW=$h; break; }
  done
  [ -n "$NEW" ] && break; sleep 0.5
done
T1=$(date +%s.%N)
if [ -z "$NEW" ]; then
  log "180 s 内控制器没有切协调者"
  for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do
    $CLI -h ${hp%:*} -p ${hp#*:} -c "select '$hp', * from partdist.pg_raft_group_status() where group_id=0" 2>&1 | head -2
  done
  exit 1
fi
log "新协调者 $NEW:31097 已提升、Citus 协调者位置已改（停机后 $(echo "$T1 - $T0" | bc) s）"
WANT="host=$NEW port=31097 dbname=postgres user=postgres"
for i in $(seq 120); do
  ok=0
  for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083 $NEW:31097; do
    [ "$($CLI -h ${hp%:*} -p ${hp#*:} -c 'show pg_partdist.tso_conninfo' 2>/dev/null)" = "$WANT" ] && ok=$((ok+1))
  done
  [ $ok = 4 ] && $CLI -h $NEW -p 31097 -c "select count(*) from warehouse" >/dev/null 2>&1 && break; sleep 0.5
done
T2=$(date +%s.%N); log "各节点 TSO 连接串已对齐 $ok/4、新协调者可读（停机后 $(echo "$T2 - $T0" | bc) s）"
log "新协调者 TSO：$($CLI -h $NEW -p 31097 -c 'select partdist.partdist_tso_status()')"
log "新协调者的复制：$($CLI -h $NEW -p 31097 -F' ' -c 'select application_name, state, sync_state from pg_stat_replication') / $($CLI -h $NEW -p 31097 -c 'show synchronous_standby_names')"
log "Citus 节点表（新协调者）"; $CLI -h $NEW -p 31097 -F' ' -c "select nodeid, groupid, nodename, nodeport from pg_dist_node order by 1"
for i in $(seq 30); do
  w=""; for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do w="$w $($CLI -h ${hp%:*} -p ${hp#*:} -c 'select nodename||chr(58)||nodeport from pg_dist_node where groupid=0')"; done
  [ "$(echo $w | tr ' ' '\n' | sort -u)" = "$NEW:31097" ] && break; sleep 1
done
log "worker 上的协调者位置：$w"
log "悬挂 2PC：$(for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083 $NEW:31097; do echo -n "$hp=$($CLI -h ${hp%:*} -p ${hp#*:} -c 'select count(*) from pg_prepared_xacts') "; done)"
log "切主后快照"; snap $NEW 31097 > /tmp/cfo_after.txt; cat /tmp/cfo_after.txt
if diff /tmp/cfo_before.txt /tmp/cfo_after.txt >/dev/null; then log "切主前后完全一致"; else log "切主前后不一致："; diff /tmp/cfo_before.txt /tmp/cfo_after.txt; fi
log "一致性检查"; $CLI -h $NEW -p 31097 -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "切主后 TPC-C（16 终端，经新协调者）"
cp tpcc/cluster.env /tmp/cluster.env.cfo
sed -i "s/^COORD_HOST=.*/COORD_HOST=$NEW/; s/^COORD_PORT=.*/COORD_PORT=31097/" tpcc/cluster.env
bash scale.sh shardpg 16 2>&1 | tail -1
R2=$(ls -td tpcc/results/*/ | head -1); grep -E "ERROR" $R2/pgbench.err | cut -c1-200 | sort | uniq -c | head -4
cp /tmp/cluster.env.cfo tpcc/cluster.env
log "切主后一致性"; $CLI -h $NEW -p 31097 -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "新协调者 TSO：$($CLI -h $NEW -p 31097 -c 'select partdist.partdist_tso_status()')"
log "完成"
