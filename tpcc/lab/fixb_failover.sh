#!/bin/bash
# 切主专项（在 killload_check.sh 基础上加核对与留证）：登记备库、开自动切主 → 64 终端 TPC-C 中途 kill n3 主库
# → 2PC 收尾 → 标准一致性 + 跨节点不变式 → 旧主拉起看是否自行停机 → 切主后 16 终端 TPC-C 60 s
# → 再核对一次 → 把各节点（含被提升的备库）日志拷到 fixb_logs/fo-<标签>/。
# 用法（145 上，集群由 redeploy_img.sh 全新部署）：fixb_failover.sh <标签>
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
TAG=${1:?标签}; OUT=$R/fixb_logs/fo-$TAG; rm -rf $OUT; mkdir -p $OUT
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
PSQL="$CLI -h 10.77.110.148 -p 31080"
chk() { echo "标准 [$($PSQL -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($PSQL -f /t/xnode_consistency_mini.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"; }
log "[$TAG] 登记备库、打开自动切主"; bash shardpg_rep.sh register >/dev/null
log "[$TAG] 切主前：$(chk)"
( bash scale.sh shardpg 64 > $OUT/load.txt 2>&1 ) &
RUN=$!
sleep 20; ssh -n 10.77.110.147 "docker kill shardpg-fix-n3" >/dev/null; T0=$(date +%s); log "[$TAG] 已 kill n3 主库"
NEW=""
for i in $(seq 240); do NEW=$($PSQL -c "select nodename||':'||nodeport from pg_dist_node where nodeport=31095 limit 1" 2>/dev/null); [ -n "$NEW" ] && break; sleep 0.5; done
log "[$TAG] Citus 路由改到 ${NEW:-（120 s 内没切）}（kill 后 $(( $(date +%s) - T0 )) s）"
wait $RUN; R2=$(ls -td tpcc/results/*/ | head -1); cp -r $R2 $OUT/load_result
log "[$TAG] 负载：$(tail -1 $OUT/load.txt)"
grep -E "ERROR|abort" $R2/pgbench.err | sed -E 's/[0-9]+/N/g' | cut -c1-200 | sort | uniq -c | sort -rn | head -5
NODES="10.77.110.148:31080 10.77.110.145:31082 ${NEW:-x:0} 10.77.110.148:31083"
for i in $(seq 120); do
  tot=0; for hp in $NODES; do c=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null); tot=$((tot + ${c:-99})); done
  [ $tot = 0 ] && break; sleep 1
done
log "[$TAG] 2PC 收尾：剩余 $tot（kill 后 $(( $(date +%s) - T0 )) s）"
log "[$TAG] 切主后核对：$(chk)"
ssh -n 10.77.110.147 "docker start shardpg-fix-n3" >/dev/null
for i in $(seq 60); do st=$(ssh -n 10.77.110.147 "docker inspect -f '{{.State.Running}}' shardpg-fix-n3"); [ "$st" = false ] && break; sleep 1; done
log "[$TAG] 旧主拉起后 running=$st（$i s）"
log "[$TAG] 切主后 TPC-C 16 终端：$(bash scale.sh shardpg 16 2>&1 | tail -1)"
R3=$(ls -td tpcc/results/*/ | head -1); cp -r $R3 $OUT/post_result
grep -E "ERROR|abort" $R3/pgbench.err | sed -E 's/[0-9]+/N/g' | cut -c1-200 | sort | uniq -c | sort -rn | head -5
log "[$TAG] 切主后压测再核对：$(chk)"
# 留证：各节点日志（被提升的备库在 145 或 148 上，容器名 shardpg-fix-n3-s<机器>）
for h in 10.77.110.145 10.77.110.148 10.77.110.147; do
  if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi
  for ct in $($c "docker ps -a --format '{{.Names}}' | grep -E '^shardpg-fix-n[1-4](-s[0-9]+)?\$'"); do
    $c "docker cp $ct:/d/pgdata/log - 2>/dev/null" > $OUT/log-$ct.tar 2>/dev/null
  done
done
ls $OUT | tr '\n' ' '; echo
