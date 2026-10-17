#!/bin/bash
# 任务 #13 诊断：重建（含内核诊断补丁 0017）→ 路线 A 集群 → 打开 pg_partdist.diag_visibility
# → 高并发 TPC-C → 收集 pgbench 报错与各节点 [diag] 日志
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
NODES="10.77.110.148:31080:n1 10.77.110.145:31082:n2 10.77.110.147:31081:n3 10.77.110.148:31083:n4"
[ "${SKIP_RESTORE:-0}" = 1 ] || bash restore_routea.sh
for x in $NODES; do IFS=: read h p n <<< "$x"; $CLI -h $h -p $p -c "alter system set pg_partdist.diag_visibility = on" -c "select pg_reload_conf()" >/dev/null; done
log "设置：$($CLI -h 10.77.110.145 -p 31082 -F' ' -c "select name||'='||setting from pg_settings where name in ('pg_partdist.rc_update_recheck','pg_partdist.early_lock_release','pg_partdist.diag_visibility','synchronous_standby_names','default_transaction_isolation') order by 1" | tr '\n' ' ')"
for t in ${TERMS:-256 128}; do
  log "TPC-C $t 终端"; bash scale.sh shardpg $t 2>&1 | tail -1
  R2=$(ls -td tpcc/results/*/ | head -1)
  grep -E "ERROR|abort" $R2/pgbench.err | sed -E 's/[0-9]+/N/g' | cut -c1-200 | sort | uniq -c | sort -rn | head -8
done
log "一致性"; $CLI -h 10.77.110.148 -p 31080 -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
onhost() { local h=$1; shift; if [ "$h" = 10.77.110.145 ]; then bash -c "$*"; else ssh -n $h "$*"; fi; }
for x in $NODES; do
  IFS=: read h p n <<< "$x"
  log "== $n [diag] 日志"
  onhost $h "docker exec shardpg-fix-$n bash -c 'grep -h \"\\[diag\\]\" /d/pgdata/log/*.log 2>/dev/null'" > /tmp/diag_$n.txt
  echo "共 $(wc -l < /tmp/diag_$n.txt) 行；其中快照读等到 PREPARED 决议（旧逻辑会判错）$(grep -c 快照读等到 /tmp/diag_$n.txt) 行"
  grep -v -e 'xmax 是自己' -e 快照读等到 /tmp/diag_$n.txt | sed -E 's/rel=([0-9]+).*/rel=\1/; s/^.*\[diag\] //' | sort | uniq -c | sort -rn | head -12
  grep 快照读等到 /tmp/diag_$n.txt | head -3 | cut -c1-300
  rels=$(grep -oE 'rel=[0-9]+' /tmp/diag_$n.txt | cut -d= -f2 | sort -u | tr '\n' ',' | sed 's/,$//')
  [ -n "$rels" ] && $CLI -h $h -p $p -F' ' -c "set citus.override_table_visibility = off; select oid, relname from pg_class where oid in ($rels)"
  head -6 /tmp/diag_$n.txt | cut -c1-420
done
log "完成"
