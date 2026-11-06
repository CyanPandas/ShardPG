#!/bin/bash
# 手工切主实验：worker n3（147:31081）主库停机 → 提升 LSN 最大的备库 → 另一个备库改跟新主 → 改 Citus / partdist 元数据
R=/data1/yyt/shardpg-fix-20261014; cd $R
PSQL="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
log() { echo "[$(date +%T)] $*"; }
log "切主前：$($PSQL -h 10.77.110.147 -p 31081 -c 'select pg_current_wal_lsn()')"
bash shardpg_rep.sh status | grep '^n3'
log "停 n3 主库（模拟宕机）"; ssh -n 10.77.110.147 "docker stop -t 1 shardpg-fix-n3" >/dev/null
sleep 2
a=$($PSQL -h 10.77.110.145 -p 31095 -c "select pg_last_wal_receive_lsn()"); b=$($PSQL -h 10.77.110.148 -p 31095 -c "select pg_last_wal_receive_lsn()")
log "备库收到的 WAL：n3s145=$a n3s148=$b"
cmp=$($PSQL -h 10.77.110.145 -p 31095 -c "select pg_wal_lsn_diff('$a','$b') >= 0")
if [ "$cmp" = t ]; then NH=10.77.110.145; OH=10.77.110.148; NS=n3s145; OS=n3s148; else NH=10.77.110.148; OH=10.77.110.145; NS=n3s148; OS=n3s145; fi
log "提升 $NS（$NH:31095）"
$PSQL -h $NH -p 31095 -c "select pg_promote(true, 60)"
for i in $(seq 60); do [ "$($PSQL -h $NH -p 31095 -c 'select pg_is_in_recovery()')" = f ] && break; sleep 1; done
log "新主 in_recovery=$($PSQL -h $NH -p 31095 -c 'select pg_is_in_recovery()') timeline=$($PSQL -h $NH -p 31095 -c 'select timeline_id from pg_control_checkpoint()')"
$PSQL -h $NH -p 31095 -c "select pg_create_physical_replication_slot('$OS', true)" >/dev/null
$PSQL -h $OH -p 31095 -c "alter system set primary_conninfo = 'host=$NH port=31095 user=postgres application_name=$OS'" -c "alter system set primary_slot_name = '$OS'" -c "select pg_reload_conf()" >/dev/null
$PSQL -h $NH -p 31095 -c "alter system set synchronous_standby_names = 'ANY 1 ($OS)'" -c "select pg_reload_conf()" >/dev/null
sleep 3
log "新主的复制：$($PSQL -h $NH -p 31095 -F' ' -c 'select application_name, state, sync_state from pg_stat_replication')"
nid=$($PSQL -h 10.77.110.148 -p 31080 -c "select nodeid from pg_dist_node where nodeport=31081")
log "Citus 元数据：node $nid → $NH:31095"
$PSQL -h 10.77.110.148 -p 31080 -c "select citus_update_node($nid, '$NH', 31095)"
# 经控制面日志改节点 3 的地址（各节点 apply：node_address / node_map / pg_raft 地址覆盖）
for hp in 10.77.110.148:31080 10.77.110.145:31082 10.77.110.148:31083 $NH:31095; do
  h=${hp%:*}; p=${hp#*:}
  if [ "$($PSQL -h $h -p $p -c "select count(*) from partdist.pg_raft_group_status() where group_id=0 and state='leader'" 2>/dev/null)" = 1 ]; then
    log "控制面 leader $hp 提议节点 3 新地址"; $PSQL -h $h -p $p -c "select partdist.pg_raft_propose_node_address(3, '$NH', 31095)"; break
  fi
done
$PSQL -h 10.77.110.148 -p 31080 -F' ' -c "select nodeid, nodename, nodeport, isactive from pg_dist_node order by 1"
echo "NEW=$NH:31095 OTHER=$OH:31095"
