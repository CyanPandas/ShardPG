#!/bin/bash
# 路线 A 性能扫描：恢复干净集群 → 热身一轮（不计）→ 各终端数各 1 min → 一致性
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
[ "${SKIP_RESTORE:-0}" = 1 ] || bash restore_routea.sh
log "热身（128 终端，不计）"; bash scale.sh shardpg 128 2>&1 | tail -1
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
NODES="10.77.110.145:31082:n2 10.77.110.147:31081:n3 10.77.110.148:31083:n4"
for t in ${TERMS:-128 192 256 384}; do
  if [ $t = 256 ]; then
    ( for i in $(seq 25); do for x in $NODES; do IFS=: read h p n <<< "$x"; $CLI -h $h -p $p -c "select '$n', coalesce(wait_event,'CPU') from pg_stat_activity where backend_type='client backend' and state='active' and pid<>pg_backend_pid()" 2>/dev/null; done; sleep 2; done ) > /tmp/perf_waits.txt &
    SP=$!
  fi
  bash scale.sh shardpg $t 2>&1 | tail -1
  [ $t = 256 ] && { wait $SP; echo "256 终端等待事件采样（每节点前 6）："; for n in n2 n3 n4; do grep "^$n|" /tmp/perf_waits.txt | cut -d'|' -f2 | sort | uniq -c | sort -rn | head -6 | tr '\n' ' ' | sed "s/^/$n: /"; echo; done; }
  R2=$(ls -td tpcc/results/*/ | head -1)
  grep -E "ERROR|abort" $R2/pgbench.err | sed -E 's/[0-9]+/N/g' | cut -c1-180 | sort | uniq -c | sort -rn | head -4
done
log "一致性"; docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -h 10.77.110.148 -p 31080 -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "完成"
