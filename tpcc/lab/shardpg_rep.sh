#!/bin/bash
# ShardPG 每个 worker 一主两备（PG 物理流复制，145 上跑）——路线 A 性能探底用，结构与 citus_rep.sh 相同。
#   shardpg_rep.sh standbys      重建 6 个备库（pg_basebackup + 物理复制槽），先异步接上
#   shardpg_rep.sh sync '<模式>'  每个 worker 设 synchronous_standby_names（quorum = ANY 1 (两个备库)；'' = 异步）
#   shardpg_rep.sh register      经控制面登记备库（OP_NODE_STANDBY）并在各节点打开 pg_raft.auto_failover
#   shardpg_rep.sh rejoin nX     切主后把缺的那份副本补回来：在没有该节点副本的那台机器上按现任主建备库、登记、改 ANY 1
#   shardpg_rep.sh status|wait|stop
# 备库连接参数写进 postgresql.auto.conf（不用命令行 -c），切主后可 ALTER SYSTEM 在线改指向。
# 只动 shardpg-fix-nX-sHOST 备库容器与 shardpg-rep 数据目录；主库只改 pg_hba（加 replication）与 synchronous_standby_names。
set -u
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014
IMG=shardpg:prof
# worker 名 主机 端口 容器 备库端口
WORKERS="n2:10.77.110.145:31082:shardpg-fix-n2:31094 n3:10.77.110.147:31081:shardpg-fix-n3:31095 n4:10.77.110.148:31083:shardpg-fix-n4:31096"
HOSTS="10.77.110.145 10.77.110.147 10.77.110.148"
# 2026-10-20：额外的 worker（4 个 worker 部署用），格式同上，如 EXTRA_WORKERS="n5:10.77.110.147:31084:shardpg-fix-n5:31098"
[ -n "${EXTRA_WORKERS:-}" ] && WORKERS="$WORKERS $EXTRA_WORKERS"
# COORD_HA=1：协调者 n1（148:31080）也配两个同步备库（145 / 147 上的 31097），协调者切主实验用
[ "${COORD_HA:-0}" = 1 ] && WORKERS="n1:10.77.110.148:31080:shardpg-fix-n1:31097 $WORKERS"
PSQL="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
# 2026-10-20：MEM_LIMIT（如 8g）给备库容器设内存上限并关 swap（与 node.sh 一致，模拟小内存服务器）
MEMOPT=""; [ -n "${MEM_LIMIT:-}" ] && MEMOPT="--memory $MEM_LIMIT --memory-swap $MEM_LIMIT"
onhost() { local h=$1; shift; if [ "$h" = 10.77.110.145 ]; then bash -c "$*"; else ssh -n $h "$*"; fi; }
base() { [ "$1" = 10.77.110.145 ] && echo $R || echo $T; }
# mk_standby worker名 主库host 主库port 备库host 备库port：在备库 host 上按主库做 pg_basebackup 建备库（同步执行）
mk_standby() {
  local wn=$1 ph=$2 pp=$3 sh=$4 sp=$5
  local sn=${wn}s${sh##*.} name=shardpg-fix-$wn-s${sh##*.} dir=$(base $sh)/shardpg-rep/data-$wn-s${sh##*.}
  $PSQL -h $ph -p $pp -c "select pg_drop_replication_slot('$sn') from pg_replication_slots where slot_name='$sn'" >/dev/null
  $PSQL -h $ph -p $pp -c "select pg_create_physical_replication_slot('$sn', true)" >/dev/null
  onhost $sh "set -e; docker rm -f $name >/dev/null 2>&1 || true; mkdir -p $dir; chmod 777 $dir
    docker run --rm -v $dir:/d $IMG bash -c 'rm -rf /d/pgdata'
    docker run --rm --network host -v $dir:/d $IMG /work/pg-install/bin/pg_basebackup -h $ph -p $pp -U postgres -D /d/pgdata -X stream -S $sn -c fast
    docker run --rm -v $dir:/d $IMG bash -c \"rm -f /d/pgdata/postgresql.auto.conf.tmp; sed -i '/^primary_conninfo\|^primary_slot_name\|^synchronous_standby_names/d' /d/pgdata/postgresql.auto.conf; touch /d/pgdata/standby.signal; printf \\\"port = $sp\\nhot_standby = on\\nprimary_conninfo = 'host=$ph port=$pp user=postgres application_name=$sn'\\nprimary_slot_name = '$sn'\\nwal_receiver_timeout = 10s\\n\\\" >> /d/pgdata/postgresql.auto.conf\"
    docker run -d --name $name --network host --shm-size 2g $MEMOPT -v $dir:/d $IMG /work/pg-install/bin/postgres -D /d/pgdata >/dev/null
    for i in \$(seq 120); do docker exec $name /work/pg-install/bin/pg_isready -h 127.0.0.1 -p $sp -q && { echo $name up; exit 0; }; sleep 1; done
    echo $name FAILED; docker logs --tail 30 $name; exit 1"
}
cp_leader() {
  for hp in $(${PSQL} -h 10.77.110.145 -p 31082 -c "select hostname||':'||port from partdist.node_map order by node_id" 2>/dev/null); do
    [ "$($PSQL -h ${hp%:*} -p ${hp#*:} -c "select count(*) from partdist.pg_raft_group_status() where group_id=0 and state='leader'" 2>/dev/null)" = 1 ] && { echo $hp; return; }
  done
}

case "${1:-}" in
standbys)
  for w in $WORKERS; do
    IFS=: read wn ph pp pc sp <<< "$w"
    onhost $ph "docker exec $pc bash -c \"grep -q '^host replication all 10.77.110.0/24 trust' /d/pgdata/pg_hba.conf || echo 'host replication all 10.77.110.0/24 trust' >> /d/pgdata/pg_hba.conf\""
    $PSQL -h $ph -p $pp -c "alter system set synchronous_standby_names = ''" -c "select pg_reload_conf()" >/dev/null
    for sh in $HOSTS; do
      [ "$sh" = "$ph" ] && continue
      sn=${wn}s${sh##*.}; name=shardpg-fix-$wn-s${sh##*.}; dir=$(base $sh)/shardpg-rep/data-$wn-s${sh##*.}
      $PSQL -h $ph -p $pp -c "select pg_drop_replication_slot('$sn') from pg_replication_slots where slot_name='$sn'" >/dev/null
      $PSQL -h $ph -p $pp -c "select pg_create_physical_replication_slot('$sn', true)" >/dev/null
      onhost $sh "set -e; docker rm -f $name >/dev/null 2>&1 || true; mkdir -p $dir; chmod 777 $dir
        docker run --rm -v $dir:/d $IMG bash -c 'rm -rf /d/pgdata'
        docker run --rm --network host -v $dir:/d $IMG /work/pg-install/bin/pg_basebackup -h $ph -p $pp -U postgres -D /d/pgdata -X stream -S $sn -c fast
        docker run --rm -v $dir:/d $IMG bash -c \"touch /d/pgdata/standby.signal; printf \\\"port = $sp\\nhot_standby = on\\nprimary_conninfo = 'host=$ph port=$pp user=postgres application_name=$sn'\\nprimary_slot_name = '$sn'\\nwal_receiver_timeout = 10s\\n\\\" >> /d/pgdata/postgresql.auto.conf\"
        docker run -d --name $name --network host --shm-size 2g $MEMOPT -v $dir:/d $IMG /work/pg-install/bin/postgres -D /d/pgdata >/dev/null
        for i in \$(seq 120); do docker exec $name /work/pg-install/bin/pg_isready -h 127.0.0.1 -p $sp -q && { echo $name up; exit 0; }; sleep 1; done
        echo $name FAILED; docker logs --tail 30 $name; exit 1" &
    done
  done
  wait
  sleep 3; bash "$0" status
  ;;
sync)
  for w in $WORKERS; do
    IFS=: read wn ph pp pc sp <<< "$w"
    s=""; for sh in $HOSTS; do [ "$sh" = "$ph" ] || s="$s${s:+, }${wn}s${sh##*.}"; done
    mode="$2"; [ "$mode" = quorum ] && mode="ANY 1 ($s)"
    $PSQL -h $ph -p $pp -c "alter system set synchronous_standby_names = '$mode'" -c "select pg_reload_conf()" >/dev/null
    echo "$wn: $($PSQL -h $ph -p $pp -c 'show synchronous_standby_names')"
  done
  bash "$0" status
  ;;
register)
  # 控制面 leader（group 0）上提议；各节点 apply 后 partdist.node_standby 一致
  NODES="${NODES:-10.77.110.148:31080 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083}"
  L=""
  for hp in $NODES; do
    h=${hp%:*}; p=${hp#*:}
    [ "$($PSQL -h $h -p $p -c "select count(*) from partdist.pg_raft_group_status() where group_id=0 and state='leader'" 2>/dev/null)" = 1 ] && { L=$hp; break; }
  done
  [ -n "$L" ] || { echo "找不到控制面 leader"; exit 1; }
  for w in $WORKERS; do
    IFS=: read wn ph pp pc sp <<< "$w"
    for sh in $HOSTS; do
      [ "$sh" = "$ph" ] && continue
      $PSQL -h ${L%:*} -p ${L#*:} -c "select partdist.pg_raft_propose_node_standby(${wn#n}, '${wn}s${sh##*.}', '$sh', $sp, false)" >/dev/null
    done
  done
  for hp in $NODES; do
    h=${hp%:*}; p=${hp#*:}
    $PSQL -h $h -p $p -c "alter system set pg_raft.auto_failover = on" -c "select pg_reload_conf()" >/dev/null
  done
  sleep 2
  echo "控制面 leader $L；登记："
  $PSQL -h ${L%:*} -p ${L#*:} -F' ' -c "select node_id, name, host, port from partdist.node_standby order by 1, 2"
  ;;
status)
  for w in $WORKERS; do
    IFS=: read wn ph pp pc sp <<< "$w"
    $PSQL -h $ph -p $pp -F' ' -c "select '$wn', application_name, state, sync_state, pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) lag_bytes from pg_stat_replication order by 2"
  done
  ;;
wait)
  for i in $(seq 600); do
    ok=0
    for w in $WORKERS; do
      IFS=: read wn ph pp pc sp <<< "$w"
      r=$($PSQL -h $ph -p $pp -c "select coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)),-1)::bigint||' '||count(*) from pg_stat_replication where state='streaming'")
      [ "$r" = "0 2" ] && ok=$((ok+1))
    done
    [ $ok = $(echo $WORKERS | wc -w) ] && { echo "all standbys caught up"; exit 0; }
    sleep 2
  done
  echo "standbys NOT caught up"; exit 1
  ;;
stop)
  for w in $WORKERS; do
    IFS=: read wn ph pp pc sp <<< "$w"
    $PSQL -h $ph -p $pp -c "alter system set synchronous_standby_names = ''" -c "select pg_reload_conf()" >/dev/null 2>&1
  done
  for sh in $HOSTS; do onhost $sh "docker ps -a --format '{{.Names}}' | grep -E '^shardpg-fix-n[0-9]-s[0-9]+$' | xargs -r docker rm -f"; done
  ;;
rejoin)
  wn=${2:?worker 名，如 n3}; nid=${wn#n}
  sp=""; for w in $WORKERS n1:10.77.110.148:31080:shardpg-fix-n1:31097; do IFS=: read n0 a b c d <<< "$w"; [ "$n0" = "$wn" ] && { sp=$d; break; }; done
  L=$(cp_leader); [ -n "$L" ] || { echo "找不到控制面 leader"; exit 1; }
  cur=$($PSQL -h ${L%:*} -p ${L#*:} -c "select hostname||':'||port from partdist.node_map where node_id=$nid"); ph=${cur%:*}; pp=${cur#*:}
  have=$($PSQL -h ${L%:*} -p ${L#*:} -c "select string_agg(host, ' ') from partdist.node_standby where node_id=$nid")
  sh=""; for h in $HOSTS; do [ "$h" = "$ph" ] && continue; case " $have " in *" $h "*) ;; *) sh=$h;; esac; done
  [ -n "$sh" ] || { echo "$wn 的副本已齐（主 $cur，备库在 $have）"; exit 0; }
  echo "$wn：现任主 $cur，在 $sh 上补建备库（端口 $sp）"
  # 那台机器上若还躺着被取代的旧主容器（已自行停机），先移走，免得误启
  for w in $WORKERS n1:10.77.110.148:31080:shardpg-fix-n1:31097; do IFS=: read n0 oh op oc d <<< "$w"
    [ "$n0" = "$wn" ] && [ "$oh" = "$sh" ] && [ "$oh:$op" != "$cur" ] && onhost $sh "docker rm -f $oc >/dev/null 2>&1; true" && echo "已移走旧主容器 $oc"; done
  mk_standby $wn $ph $pp $sh $sp || exit 1
  $PSQL -h ${L%:*} -p ${L#*:} -c "select partdist.pg_raft_propose_node_standby($nid, '${wn}s${sh##*.}', '$sh', $sp, false)" >/dev/null
  names=$($PSQL -h ${L%:*} -p ${L#*:} -c "select string_agg(name, ', ' order by name) from partdist.node_standby where node_id=$nid")
  $PSQL -h $ph -p $pp -c "alter system set synchronous_standby_names = 'ANY 1 ($names)'" -c "select pg_reload_conf()" >/dev/null
  sleep 3
  $PSQL -h $ph -p $pp -F' ' -c "select '$wn', application_name, state, sync_state from pg_stat_replication order by 2"
  ;;
*) echo "usage: $0 standbys|sync <mode>|register|rejoin nX|status|wait|stop"; exit 2;;
esac
