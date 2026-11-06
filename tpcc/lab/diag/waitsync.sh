#!/bin/bash
# 用法（145 上）：waitsync.sh [最长秒数] —— 等每个数据组两个副本的 last_log_index 追上 leader
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -F ,"
Q="select group_id, state, last_log_index from partdist.pg_raft_group_status() where group_id<>0"
T=${1:-900}; t0=$(date +%s)
while true; do
  lag=$(for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do h=${hp%:*}; p=${hp#*:}; $CLI -h $h -p $p -c "$Q"; done | awk -F, '{ if ($2=="leader") L[$1]=$3; else { F[$1]=F[$1]" "$3 } } END { tot=0; n=0; for (g in L) { split(F[g],a," "); for (i in a) { d=L[g]-a[i]; if (d>0) { tot+=d; n++ } } } print n" "tot }')
  echo "$(date +%T) 落后副本数/总落后条数: $lag"
  [ "${lag%% *}" = "0" ] && exit 0
  [ $(( $(date +%s) - t0 )) -ge $T ] && exit 1
  sleep 15
done
