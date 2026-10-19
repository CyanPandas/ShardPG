#!/bin/bash
# 跨节点原子性 + 崩溃恢复（云上 cloud/tests/test_atomicity.sh 的实验室版）：64 终端 TPC-C 跑着时对一个 worker
# docker kill（SIGKILL），停 DOWN 秒再拉起（不切主）；等 2PC 全部收尾后跑标准一致性与跨节点不变式。
# 三台 worker 轮流杀，共 ROUNDS 轮。用法（145 上）：fixb_killrounds.sh   可选 ROUNDS= T= KILL_AT= DOWN= CLIENTS=
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
ROUNDS=${ROUNDS:-3}; T=${T:-90}; KILL_AT=${KILL_AT:-30}; DOWN=${DOWN:-15}; CLIENTS=${CLIENTS:-64}
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
C1="$CLI -h 10.77.110.148 -p 31080"
WK="10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083"
OUT=$R/fixb_logs/kill-$(date +%H%M%S); mkdir -p $OUT
on() { if [ "$1" = 10.77.110.145 ]; then bash -c "$2"; else ssh -n $1 "$2"; fi; }
settled() { local tot=0 c hp; for hp in $WK; do
  c=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null)
  tot=$((tot + ${c:-99})); done; echo $tot; }
serving() { $C1 -c "select count(*) from warehouse" -c "select sum(d_next_o_id) from district" >/dev/null 2>&1; }
check() { local i s x
  for i in 1 2 3 4 5; do
    s=$($C1 -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$' | tr '\n' ' ')
    x=$($C1 -f /t/xnode_consistency_mini.sql 2>&1 | grep -E '^[0-9]+\|' | tr '\n' ' ')
    [ $(echo $s | wc -w) = 5 ] && [ $(echo $x | wc -w) = 2 ] && break; sleep 10
  done
  vs=$(echo $s | awk '{t = 0; for (i = 1; i <= NF; i++) t += $i; print t}')
  vx=$(echo $x | tr ' ' '\n' | cut -d'|' -f1 | awk '{t += $1} END {print t + 0}')
  [ $(echo $s | wc -w) = 5 ] && [ $(echo $x | wc -w) = 2 ] && echo "违例：标准 $vs、跨节点 $vx（标准 [$s] 跨节点 [$x]）" \
    || echo "！！检查没跑成（标准 [$s] 跨节点 [$x]）"; }
log "基线：$(check)"
seg=0; bad=0
for r in $(seq $ROUNDS); do for x in 10.77.110.145:31082:2 10.77.110.147:31081:3 10.77.110.148:31083:4; do
  IFS=: read h p id <<< "$x"; seg=$((seg + 1))
  s1=$($C1 -c "select sum(d_next_o_id) from district")
  docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h 10.77.110.148 -p 31080 -U postgres -n -c $CLIENTS -j $CLIENTS -T $T \
    --max-tries=20 --failures-detailed -D nw=64 -f /t/pgbench/new_order.sql@45 -f /t/pgbench/payment.sql@43 \
    -f /t/pgbench/order_status.sql@4 -f /t/pgbench/delivery.sql@4 -f /t/pgbench/stock_level.sql@4 postgres \
    > $OUT/seg$seg.out 2> $OUT/seg$seg.err &
  pg=$!
  sleep $KILL_AT; on $h "docker kill shardpg-fix-n$id >/dev/null"; t0=$(date +%s)
  sleep $DOWN; on $h "docker start shardpg-fix-n$id >/dev/null"
  for i in $(seq 120); do on $h "docker exec shardpg-fix-n$id /work/pg-install/bin/pg_isready -h 127.0.0.1 -p $p -q" && break; sleep 1; done
  for i in $(seq 300); do serving && break; sleep 1; done
  sv=$(( $(date +%s) - t0 ))
  wait $pg
  ab=$(grep -cE 'client [0-9]+ (script [0-9]+ )?aborted' $OUT/seg$seg.err)
  for i in $(seq 150); do [ "$(settled)" = 0 ] && break; sleep 2; done
  st=$(settled); s2=$($C1 -c "select sum(d_next_o_id) from district")
  c=$(check); echo "$c" | grep -q "违例：标准 0、跨节点 0" || bad=$((bad + 1))
  log "第 $seg 段 kill n$id：恢复服务 ${sv}s，NewOrder $((s2 - s1))，中止客户端 $ab，2PC 剩余 $st；$c"
done; done
log "共 $seg 次 kill，一致性 / 不变式不为 0 的 $bad 次；明细 $OUT"
