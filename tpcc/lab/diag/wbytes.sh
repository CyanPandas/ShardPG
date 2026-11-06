# 容器内：按进程类别统计 10 s 内写盘字节（/proc/<pid>/io write_bytes）
P="/work/pg-install/bin/psql -h 127.0.0.1 -p $1 -U postgres -XAtq"
declare -A W0
for d in /proc/[0-9]*; do p=${d#/proc/}; w=$(awk '/^write_bytes/{print $2}' $d/io 2>/dev/null); [ -n "$w" ] && W0[$p]=$w; done
sleep 10
$P -c "select pid, coalesce(nullif(backend_type,'client backend'),'client') from pg_stat_activity" > /tmp/cls
declare -A C; while IFS='|' read -r p c; do C[$p]=$c; done < /tmp/cls
for d in /proc/[0-9]*; do p=${d#/proc/}; w=$(awk '/^write_bytes/{print $2}' $d/io 2>/dev/null); [ -n "$w" ] || continue
  dw=$(( w - ${W0[$p]:-$w} )); [ $dw -gt 0 ] || continue; echo "${C[$p]:-other:$(cat $d/comm)} $dw"; done |
  awk '{n=$NF; $NF=""; s[$0]+=n} END {for (k in s) printf "%8.1f MB/s  %s\n", s[k]/10/1048576, k}' | sort -rn | head -12
du -sm /d/pgdata/pg_wal /d/pgdata/pg_parwal* /d/pgdata/base /d/pgdata/pg_shard_clog 2>/dev/null
