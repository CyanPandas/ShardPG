#!/bin/bash
# Citus 三副本 vs ShardPG 路线 A：256 终端下的等待事件、每笔事务的 WAL / 提交、主机 CPU
R=/data1/yyt/shardpg-fix-20261014; cd $R
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
onhost() { local h=$1; shift; if [ "$h" = 10.77.110.145 ]; then bash -c "$*"; else ssh -n $h "$*"; fi; }
for SYS in citus3r shardpg; do
  if [ $SYS = citus3r ]; then W="10.77.110.145:31182:w145 10.77.110.147:31181:w147 10.77.110.148:31183:w148"; CO=10.77.110.148:31180
  else W="10.77.110.145:31082:w145 10.77.110.147:31081:w147 10.77.110.148:31083:w148"; CO=10.77.110.148:31080; fi
  echo "===== $SYS ====="
  bash scale.sh $SYS 128 >/dev/null 2>&1   # 热身
  st() { for x in $W co:${CO}; do IFS=: read h p n <<< "$x"; [ $h = co ] && { h=${CO%:*}; p=${CO#*:}; n=co; }; echo "$n $($CLI -h $h -p $p -F' ' -c "select (select sum(xact_commit) from pg_stat_database), wal_records, wal_bytes, wal_fpi, wal_sync from pg_stat_wal")"; done; }
  st | sort > /tmp/cp_b.txt
  ( for i in $(seq 25); do for x in $W; do IFS=: read h p n <<< "$x"; $CLI -h $h -p $p -c "select '$n', coalesce(wait_event,'CPU') from pg_stat_activity where backend_type='client backend' and state='active' and pid<>pg_backend_pid()" 2>/dev/null; done; sleep 2; done ) > /tmp/cp_w.txt &
  SP=$!
  for h in 10.77.110.145 10.77.110.147 10.77.110.148; do onhost $h "vmstat 10 6 | tail -4 | awk '{us+=\$13; sy+=\$14; id+=\$15; wa+=\$16} END {printf \"%s us=%d sy=%d id=%d wa=%d\\n\", \"$h\", us/4, sy/4, id/4, wa/4}'" > /tmp/cp_cpu_$h.txt & done
  bash scale.sh $SYS 256 2>&1 | tail -1
  wait
  st | sort > /tmp/cp_a.txt
  TPS=$(grep -E '^tps' $(ls -td $( [ $SYS = citus3r ] && echo tpcc-citus3r || echo tpcc )/results/*/ | head -1)/pgbench.out | grep -oE '[0-9.]+' | head -1)
  echo "tps=$TPS；每笔业务事务：提交数 / WAL 记录 / WAL 字节 / FPI / fsync"
  join /tmp/cp_b.txt /tmp/cp_a.txt | awk -v tps=$TPS '{n=tps*60; printf "  %s commits=%.2f walrec=%.1f walbytes=%.0f fpi=%.2f walsync=%.3f\n", $1, ($7-$2)/n, ($8-$3)/n, ($9-$4)/n, ($10-$5)/n, ($11-$6)/n}'
  echo "主机 CPU（vmstat 均值，%）："; cat /tmp/cp_cpu_*.txt | sed 's/^/  /'
  echo "worker 活跃会话等待事件（25 次采样合计）："
  for n in w145 w147 w148; do echo "  $n: $(grep "^$n|" /tmp/cp_w.txt | cut -d'|' -f2 | sort | uniq -c | sort -rn | head -8 | awk '{printf "%s=%s ", $2, $1}')"; done
done
