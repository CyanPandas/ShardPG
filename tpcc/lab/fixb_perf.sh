#!/bin/bash
# 方案 B 修复前后的性能对照（同一集群形态：单副本快速提交 + 每 worker 两个同步备库 ANY 1，64 迷你仓）：
# 热身 128 终端 1 轮（不计）→ TERMS 里各档各 1 min → 2PC 收尾 → 一致性 + 跨节点不变式 → 决议表统计。
# 用法（145 上，集群已由 redeploy_img.sh 部署）：fixb_perf.sh <标签>   可选 TERMS="128 128 256 256"
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
TAG=${1:?标签}
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
WK="10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083"
log "[$TAG] 热身（128 终端，不计）：$(bash scale.sh shardpg 128 2>&1 | tail -1)"
for t in ${TERMS:-128 128 256 256}; do
  log "[$TAG] $(bash scale.sh shardpg $t 2>&1 | tail -1)"
  R2=$(ls -td tpcc/results/*/ | head -1)
  grep -E "ERROR|abort" $R2/pgbench.err | sed -E 's/[0-9]+/N/g' | cut -c1-160 | sort | uniq -c | sort -rn | head -3
done
for i in $(seq 120); do
  tot=0; for hp in $WK; do c=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null); tot=$((tot + ${c:-99})); done
  [ $tot = 0 ] && break; sleep 2
done
log "[$TAG] 2PC 收尾：剩余 $tot（$((i * 2)) s 内）"
log "[$TAG] 一致性：$($CLI -h 10.77.110.148 -p 31080 -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')"
log "[$TAG] 跨节点不变式：$($CLI -h 10.77.110.148 -p 31080 -f /t/xnode_consistency_mini.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')"
for hp in $WK; do
  d=$($CLI -h ${hp%:*} -p ${hp#*:} -F/ -c "select verdict, partdist.dtx_verdict_effective(verdict, anchor_xid), count(*) from partdist.dtx_decision group by 1, 2 order by 1, 2" 2>/dev/null \
      || $CLI -h ${hp%:*} -p ${hp#*:} -F/ -c "select verdict, count(*) from partdist.dtx_decision group by 1 order by 1" 2>/dev/null)
  echo "    $hp 决议表（verdict[/有效值]/行数）：$(echo $d)"
done
