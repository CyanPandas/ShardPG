# 压测中段对 n2/n3 采 pg_stat_activity 等待事件（每 0.2 s，40 次）
cd /data1/yyt/shardpg-fix-20261014/tpcc
docker exec shardpg-fix-client bash -c "cd /t && bash scripts/run.sh ${1:-we} >/dev/null 2>&1" &
sleep 20
for hp in "10.77.110.145 31082" "10.77.110.147 31081"; do set -- $hp
  echo "== $2"
  docker exec shardpg-fix-client bash -c "for i in \$(seq 40); do /work/pg-install/bin/psql -h $1 -p $2 -U postgres -XAtq -c \"select coalesce(wait_event_type,'CPU')||':'||coalesce(wait_event,'-')||' '||case when query like 'SELECT tpcc.%' then 'txn' when query like '%partdist%' then 'partdist' when query like 'PREPARE%' or query like 'COMMIT PREPARED%' then '2pc' else left(regexp_replace(query,'[0-9]+','N','g'),30) end from pg_stat_activity where state='active' and pid<>pg_backend_pid() and backend_type='client backend'\"; sleep 0.2; done" | sort | uniq -c | sort -rn | head -25
done
wait
D=$(ls -td results/*/ | head -1); sed -n 3p $D/summary.txt
