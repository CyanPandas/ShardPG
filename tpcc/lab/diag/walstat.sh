# 一轮压测前后取 pg_stat_wal / pg_stat_database 差值：每秒提交数、WAL 字节/提交、fsync/提交
cd /data1/yyt/shardpg-fix-20261014/tpcc
Q="select (select xact_commit from pg_stat_database where datname='postgres')||' '||wal_records||' '||wal_bytes||' '||wal_write||' '||wal_sync||' '||wal_fpi from pg_stat_wal"
snap() { docker exec shardpg-fix-client /work/pg-install/bin/psql -h $1 -p $2 -U postgres -XAtq -c "$Q"; }
declare -A A
for hp in "10.77.110.145:31082" "10.77.110.147:31081" "10.77.110.145:31080"; do A[$hp]=$(snap ${hp%:*} ${hp#*:}); done
docker exec shardpg-fix-client bash -c "cd /t && bash scripts/run.sh ${1:-ws} >/dev/null 2>&1"
for hp in "10.77.110.145:31082" "10.77.110.147:31081" "10.77.110.145:31080"; do
  B=$(snap ${hp%:*} ${hp#*:})
  echo "$hp ${A[$hp]} $B" | awk '{c=$8-$2; printf "%s commits=%d rec/c=%.1f bytes/c=%.0f write/c=%.2f sync/c=%.2f fpi/c=%.2f\n", $1, c, ($9-$3)/c, ($10-$4)/c, ($11-$5)/c, ($12-$6)/c, ($13-$7)/c}'
done
D=$(ls -td results/*/ | head -1); sed -n 3p $D/summary.txt; grep -E '^tps' $D/pgbench.out
