# 容器内：10 s 窗口按进程类别归并 CPU（utime+stime 差值，单位核）
P="/work/pg-install/bin/psql -h 127.0.0.1 -p ${1:-31081} -U postgres -XAtq"
declare -A T0
for f in /proc/[0-9]*/stat; do p=${f#/proc/}; p=${p%/stat}; read -r -a a < $f 2>/dev/null || continue; T0[$p]=$(( ${a[13]} + ${a[14]} )); done
$P -c "select pid, coalesce(nullif(backend_type,'client backend'),'client:'||case when query like 'SELECT tpcc.%' then 'txn' when query ~* 'shard_vacuum' then 'vacuum' when query ~* 'dtx|partdist' then 'partdist' when query ~* 'PREPARE|COMMIT' then '2pc' else left(regexp_replace(query,'[0-9]+','N','g'),25) end) from pg_stat_activity" > /tmp/cls0
sleep 10
$P -c "select pid, coalesce(nullif(backend_type,'client backend'),'client:'||case when query like 'SELECT tpcc.%' then 'txn' when query ~* 'shard_vacuum' then 'vacuum' when query ~* 'dtx|partdist' then 'partdist' when query ~* 'PREPARE|COMMIT' then '2pc' else left(regexp_replace(query,'[0-9]+','N','g'),25) end) from pg_stat_activity" > /tmp/cls1
declare -A C
while IFS='|' read -r p c; do C[$p]=$c; done < <(cat /tmp/cls0 /tmp/cls1)
hz=$(getconf CLK_TCK)
for f in /proc/[0-9]*/stat; do p=${f#/proc/}; p=${p%/stat}; read -r -a a < $f 2>/dev/null || continue
  t=$(( ${a[13]} + ${a[14]} )); d=$(( t - ${T0[$p]:-$t} )); [ $d -gt 0 ] || continue
  cls=${C[$p]:-"proc:$(cat /proc/$p/comm 2>/dev/null)"}; echo "$cls $d"
done | awk -v hz=$hz '{n=$NF; $NF=""; s[$0]+=n} END {for (k in s) printf "%7.2f cores  %s\n", s[k]/hz/10, k}' | sort -rn | head -20
