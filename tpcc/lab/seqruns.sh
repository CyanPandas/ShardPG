# 用法：seqruns.sh <轮数> <标签>  在同一库上连跑多轮 1 min，逐轮打印 tpmC/tps 与热点表页数
cd /data1/yyt/shardpg-fix-20261014/tpcc
for k in $(seq $1); do
  docker exec shardpg-fix-client bash -c "cd /t && bash scripts/run.sh $2-$k >/dev/null 2>&1"
  D=$(ls -td results/*/ | head -1)
  pages=$(docker exec shardpg-fix-client /work/pg-install/bin/psql -h 10.77.110.145 -p 31082 -U postgres -XAtq -c "set citus.override_table_visibility=off" -c "select string_agg(relname||'='||(pg_relation_size(oid)/8192), ' ' order by relname) from pg_class where relname ~ '^(warehouse|district)_[0-9]+\$'")
  echo "run$k $(sed -n 3p $D/summary.txt | grep -oE '= [0-9.-]+') $(grep -E '^tps' $D/pgbench.out | cut -c1-16) aborted=$(grep -c aborted $D/pgbench.err) viol$(sed -n 5p $D/summary.txt | grep -oE '= [0-9]+') | $pages"
done
