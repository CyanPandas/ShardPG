#!/bin/bash
# (1) 提交账：256 终端 60 s，各节点 xact_commit / WAL 刷盘次数 / 同步复制等待采样，对比业务 tps
# (2) rejoin：登记 → 停 n3 主库 → 等自动切主 → shardpg_rep.sh rejoin n3 → 三副本复原 → TPC-C + 一致性
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
NODES="10.77.110.148:31080:n1 10.77.110.145:31082:n2 10.77.110.147:31081:n3 10.77.110.148:31083:n4"
stat() { for x in $NODES; do IFS=: read h p n <<< "$x"; echo "$n $($CLI -h $h -p $p -F' ' -c "select (select sum(xact_commit) from pg_stat_database), wal_records, wal_sync, wal_write from pg_stat_wal")"; done; }
log "提交账：256 终端 60 s"
stat > /tmp/cr_before.txt
( for i in $(seq 25); do for x in $NODES; do IFS=: read h p n <<< "$x"; $CLI -h $h -p $p -c "select '$n', coalesce(wait_event,'CPU'), left(regexp_replace(query, '\s+', ' ', 'g'), 60) from pg_stat_activity where backend_type='client backend' and state='active' and pid<>pg_backend_pid()" 2>/dev/null; done; sleep 2; done ) > /tmp/cr_waits.txt &
SP=$!
bash scale.sh shardpg 256 2>&1 | tail -1
wait $SP
stat > /tmp/cr_after.txt
TPS=$(grep -E '^tps' $(ls -td tpcc/results/*/ | head -1)/pgbench.out | grep -oE '[0-9.]+' | head -1)
echo "业务 tps=$TPS（60 s）；每节点每笔业务事务的：提交数 / WAL 记录 / WAL fsync"
join /tmp/cr_before.txt /tmp/cr_after.txt | awk -v tps=$TPS '{n=tps*60; printf "%s commits/txn=%.2f walrec/txn=%.1f walsync/txn=%.2f\n", $1, ($6-$2)/n, ($7-$3)/n, ($8-$4)/n}'
echo "等待事件采样（前 12）："; awk -F'|' '{print $1, $2}' /tmp/cr_waits.txt | sort | uniq -c | sort -rn | head -12
echo "SyncRep 等待时在跑的语句（前 8）："; awk -F'|' '$2=="SyncRep"{print $1, $3}' /tmp/cr_waits.txt | sort | uniq -c | sort -rn | head -8

log "rejoin：登记 → 停 n3 主库 → 等自动切主"
bash shardpg_rep.sh register >/dev/null
ssh -n 10.77.110.147 "docker stop -t 1 shardpg-fix-n3" >/dev/null
NEW=""; for i in $(seq 240); do NEW=$($CLI -h 10.77.110.148 -p 31080 -c "select nodename||':'||nodeport from pg_dist_node where nodeport=31095 limit 1" 2>/dev/null); [ -n "$NEW" ] && break; sleep 0.5; done
log "已切到 $NEW"
for i in $(seq 60); do l=$($CLI -h ${NEW%:*} -p 31095 -c "select count(*) filter (where state='leader') from partdist.pg_raft_group_status() where group_id<>0" 2>/dev/null); [ "$l" = 16 ] && break; sleep 1; done
log "rejoin n3"; bash shardpg_rep.sh rejoin n3
log "登记：$($CLI -h 10.77.110.148 -p 31080 -c "select string_agg(name||'@'||host, ' ' order by name) from partdist.node_standby where node_id=3")"
for i in $(seq 120); do r=$($CLI -h ${NEW%:*} -p 31095 -c "select count(*) from pg_stat_replication where state='streaming' and sync_state='quorum'"); [ "$r" = 2 ] && break; sleep 1; done
log "新主 $NEW 的同步备库：$($CLI -h ${NEW%:*} -p 31095 -F' ' -c "select string_agg(application_name||':'||state||':'||sync_state, ' ') from pg_stat_replication") / $($CLI -h ${NEW%:*} -p 31095 -c 'show synchronous_standby_names')"
log "TPC-C 64 终端"; bash scale.sh shardpg 64 2>&1 | tail -1
log "一致性"; $CLI -h 10.77.110.148 -p 31080 -f /t/sql/90_consistency.sql 2>&1 | grep -E "^ *[0-9]+ *$|ERROR" | tr "\n" " "; echo
log "完成"
