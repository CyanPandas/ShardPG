# 原生 PG：同一库连跑 n 轮 1 min
cd /data1/yyt/shardpg-fix-20261014/tpcc-native
docker exec pg-native-client /work/pg-install/bin/psql -h 10.77.110.145 -p 31090 -U postgres -XAtq -c "select count(*) from pg_proc where prosrc like '%TXN_REORDER_V1%'" 2>/dev/null || docker exec pg-native-client psql -h 10.77.110.145 -p 31090 -U postgres -XAtq -c "select count(*) from pg_proc where prosrc like '%TXN_REORDER_V1%'"
for k in $(seq $1); do
  docker exec pg-native-client bash -c "cd /t && bash scripts/run.sh $2-$k >/dev/null 2>&1"
  D=$(ls -td results/*/ | head -1)
  echo "run$k $(sed -n 3p $D/summary.txt | grep -oE '= [0-9.-]+') $(grep -E '^tps' $D/pgbench.out | cut -c1-16) aborted=$(grep -c aborted $D/pgbench.err) viol$(sed -n 5p $D/summary.txt | grep -oE '= [0-9]+')"
done
