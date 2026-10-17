#!/bin/bash
# 路线 A 故障注入：TPC-C 64 终端跑着时 docker kill（SIGKILL）n3 主库 → 控制器自动切主
# → 负载结束后等 2PC 全部收尾 → 一致性检查 → 把旧主原样拉起来，看它能否认出自己已被取代并自行停机
# → 切主后 TPC-C + 一致性
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
PSQL="$CLI -h 10.77.110.148 -p 31080"
log "登记备库、打开自动切主"; bash shardpg_rep.sh register >/dev/null
log "TPC-C 64 终端 60 s（后台），20 s 后硬杀 n3 主库"
( bash scale.sh shardpg 64 > /tmp/kl_run.txt 2>&1 ) &
RUN=$!
sleep 20
T0=$(date +%s.%N); ssh -n 10.77.110.147 "docker kill shardpg-fix-n3" >/dev/null; log "已 kill n3"
NEW=""
for i in $(seq 240); do
  NEW=$($PSQL -c "select nodename||':'||nodeport from pg_dist_node where nodeport=31095 limit 1" 2>/dev/null)
  [ -n "$NEW" ] && break; sleep 0.5
done
T1=$(date +%s.%N)
[ -n "$NEW" ] || { log "120 s 内控制器没有切主"; wait $RUN; exit 1; }
log "Citus 路由已改到 $NEW（kill 后 $(echo "$T1 - $T0" | bc) s）"
ssh -n 10.77.110.148 "docker exec shardpg-fix-n1 bash -c 'grep -h \"pg_raft: 节点 3\" \$(ls -t /d/pgdata/log/* | head -2)' | cut -c1-220 | tail -8"
NH=${NEW%:*}
wait $RUN; log "负载结束：$(tail -1 /tmp/kl_run.txt)"
R2=$(ls -td tpcc/results/*/ | head -1); grep -E "ERROR|FATAL|abort" $R2/pgbench.err | sed -E 's/[0-9]+/N/g' | cut -c1-160 | sort | uniq -c | sort -rn | head -6
NODES="10.77.110.148:31080 10.77.110.145:31082 $NH:31095 10.77.110.148:31083"
log "等 2PC 收尾（各节点 pg_prepared_xacts 归零）"
for i in $(seq 120); do
  tot=0; s=""
  for hp in $NODES; do c=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select count(*) from pg_prepared_xacts" 2>/dev/null); s="$s $hp=$c"; tot=$((tot + ${c:-99})); done
  [ $tot = 0 ] && break; sleep 1
done
log "悬挂 2PC：$s"
log "未决登记：$(for hp in $NODES; do echo -n "$hp=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select partdist.dtx_pending_count()" 2>/dev/null) "; done)"
log "一致性检查"; $PSQL -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "把旧主原样拉起来（模拟宕机恢复），看它是否自行停机"
ssh -n 10.77.110.147 "docker start shardpg-fix-n3" >/dev/null
for i in $(seq 60); do
  st=$(ssh -n 10.77.110.147 "docker inspect -f '{{.State.Running}}' shardpg-fix-n3")
  [ "$st" = false ] && break; sleep 1
done
log "旧主容器 running=$st（$i s）"
ssh -n 10.77.110.147 "docker run --rm -v /home/yyt/shardpg-fix-20261014/data-n3:/d shardpg:prof bash -c 'grep -h -E \"被取代|接替\" \$(ls -t /d/pgdata/log/* | head -2)' | cut -c1-220 | tail -3"
log "旧主回来期间的 TSO 登记 / 控制面"; $PSQL -c "select partdist.partdist_tso_status()"
log "切主后 TPC-C（16 终端）"; bash scale.sh shardpg 16 2>&1 | tail -1
R2=$(ls -td tpcc/results/*/ | head -1); grep -E "ERROR" $R2/pgbench.err | cut -c1-200 | sort | uniq -c | head -4
log "切主后一致性"; $PSQL -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "完成"
