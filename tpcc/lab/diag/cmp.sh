#!/bin/bash
# 用法（145 上）：cmp.sh —— 每组 leader 与两个副本的 last_log / commit / applied / 分区流落盘上界对比（负载停下后跑）
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -F ,"
Q="select group_id, state, current_term, last_log_index, commit_index, last_applied, partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(group_id)::oid) from partdist.pg_raft_group_status() where group_id<>0"
for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do
  h=${hp%:*}; p=${hp#*:}
  $CLI -h $h -p $p -c "$Q" | sed "s/^/$p,/"
done | sort -t, -k2,2n -k3,3 | awk -F, '
{ g=$2; n[g]++; if ($3=="leader") { L[g]=$0; lf[g]=$8; lll[g]=$5 } else { F[g]=F[g]" "$1":"$5"/"$6"/"$7"/"$8 }; fl[g]=fl[g]" "$8; ll[g]=ll[g]" "$5 }
END { bad=0; tot=0; for (g in n) { tot++; split(fl[g],a," "); split(ll[g],b," "); same=(a[1]==a[2] && a[2]==a[3] && b[1]==b[2] && b[2]==b[3]); if (!same || n[g]!=3) { bad++; if (bad<=8) print "DIFF g="g" leader="L[g]" followers="F[g] } } print "groups="tot" mismatched="bad }'
