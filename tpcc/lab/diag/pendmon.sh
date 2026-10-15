# 压测同时每 5 s 采一次各节点未决登记数与活跃后端数
cd /data1/yyt/shardpg-fix-20261014/tpcc
docker exec shardpg-fix-client bash -c "cd /t && bash scripts/run.sh ${1:-mon} >/dev/null 2>&1" &
P="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
for i in $(seq 1 14); do
  sleep 5
  a=$($P -h 10.77.110.145 -p 31082 -c "select partdist.dtx_pending_count()" 2>&1 | head -1)
  b=$($P -h 10.77.110.147 -p 31081 -c "select partdist.dtx_pending_count()" 2>&1 | head -1)
  c=$($P -h 10.77.110.145 -p 31082 -c "select count(*) filter (where state='active') || '/' || count(*) from pg_stat_activity" 2>&1 | head -1)
  echo "t=$((i*5)) n2=$a n3=$b n2act=$c"
done
wait
D=$(ls -td results/*/ | head -1); sed -n 3p $D/summary.txt
