#!/usr/bin/env bash
# 路线 A 高可用（从 lab/shardpg_rep.sh 移植到云上布局）：每个 worker 一主两备 PG 物理流复制，备库放在另外两台数据库机器上，
# 同步 quorum ANY 1；经控制面登记备库（partdist.pg_raft_propose_node_standby）并在各节点打开 pg_raft.auto_failover。
# 主库只改 pg_hba（加 replication）与 synchronous_standby_names；备库容器 $PREFIX-n<id>-s<机器 IP 末段>，数据在 $BASE/rep/。
# 用法（客户端机上，ShardPG 已部署）：bash cloud/ha.sh standbys|sync|register|status|wait|stop
#   STANDBY_SHARED_BUFFERS（默认 512MB）：每台机器上要同时跑 1 个主库 + 2 个备库，备库沿用主库的 2GB 会撑爆 7.5 GB 内存
source "$(dirname "$0")/lib.sh"
SB=${STANDBY_SHARED_BUFFERS:-512MB}
WK=(); for k in "${!NODE_ID_LIST[@]}"; do [[ "${NODE_ROLE_LIST[$k]}" == worker ]] && WK+=("$k"); done
sport() { echo $((WORKER_PORT + 8 + $1)); }            # n<id> 的备库端口：n2→5443、n3→5444、n4→5445
sname() { echo "n$1s${2##*.}"; }                       # 备库名（也是 application_name / 复制槽名）
others() { local h; for h in "${HOSTS[@]}"; do [[ "$h" == "$1" ]] || echo "$h"; done; }
leader() { local k; for k in "${!NODE_ID_LIST[@]}"; do
  [[ "$(cpsql "${NODE_HOST_LIST[$k]}" "${NODE_PORT_LIST[$k]}" -qAt -c "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id = 0 AND state = 'leader'" 2>/dev/null)" == 1 ]] \
    && { echo "${NODE_HOST_LIST[$k]}:${NODE_PORT_LIST[$k]}"; return; }; done; }

case "${1:-}" in
standbys)
  pids=()
  for k in "${WK[@]}"; do
    id=${NODE_ID_LIST[$k]}; ph=${NODE_HOST_LIST[$k]}; pp=${NODE_PORT_LIST[$k]}; sp=$(sport "$id")
    on "$ph" "docker exec $PREFIX-n$id bash -c \"grep -q '^host replication all $HBA_CIDR trust' /d/pgdata/pg_hba.conf || echo 'host replication all $HBA_CIDR trust' >> /d/pgdata/pg_hba.conf\"" \
      || die "改 n$id 的 pg_hba 失败"
    cpsql "$ph" "$pp" -qAt -c "ALTER SYSTEM SET synchronous_standby_names = ''" -c "SELECT pg_reload_conf()" >/dev/null || die "n$id reload 失败"
    for sh in $(others "$ph"); do
      sn=$(sname "$id" "$sh"); name=$PREFIX-n$id-s${sh##*.}; dir=$BASE/rep/data-n$id-s${sh##*.}
      cpsql "$ph" "$pp" -qAt -c "SELECT pg_drop_replication_slot('$sn') FROM pg_replication_slots WHERE slot_name = '$sn'" \
        -c "SELECT pg_create_physical_replication_slot('$sn', true)" >/dev/null || die "n$id 建复制槽 $sn 失败"
      on "$sh" "set -e; docker rm -f $name >/dev/null 2>&1 || true; mkdir -p $dir; chmod 777 $dir
        docker run --rm -v $dir:/d $IMG bash -c 'rm -rf /d/pgdata'
        docker run --rm --network host -v $dir:/d $IMG /work/pg-install/bin/pg_basebackup -h $ph -p $pp -U postgres -D /d/pgdata -X stream -S $sn -c fast
        docker run --rm -v $dir:/d $IMG bash -c \"touch /d/pgdata/standby.signal; printf \\\"port = $sp\\nhot_standby = on\\nshared_buffers = $SB\\nprimary_conninfo = 'host=$ph port=$pp user=postgres application_name=$sn'\\nprimary_slot_name = '$sn'\\nwal_receiver_timeout = 10s\\n\\\" >> /d/pgdata/postgresql.auto.conf\"
        docker run -d --name $name --network host --shm-size 2g -v $dir:/d $IMG /work/pg-install/bin/postgres -D /d/pgdata >/dev/null
        for i in \$(seq 120); do docker exec $name /work/pg-install/bin/pg_isready -h 127.0.0.1 -p $sp -q && { echo \"$name up :$sp（$sh）\"; exit 0; }; sleep 1; done
        echo $name FAILED; docker logs --tail 30 $name; exit 1" &
      pids+=($!)
    done
  done
  fail=0; for p in "${pids[@]}"; do wait "$p" || fail=1; done
  [[ $fail -eq 0 ]] || die "有备库没建起来"
  sleep 3; bash "$0" status
  ;;
sync)
  for k in "${WK[@]}"; do
    id=${NODE_ID_LIST[$k]}; ph=${NODE_HOST_LIST[$k]}; pp=${NODE_PORT_LIST[$k]}
    s=""; for sh in $(others "$ph"); do s+="${s:+, }$(sname "$id" "$sh")"; done
    cpsql "$ph" "$pp" -qAt -c "ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 ($s)'" -c "SELECT pg_reload_conf()" >/dev/null
    echo "n$id：synchronous_standby_names = $(cpsql "$ph" "$pp" -qAt -c 'SHOW synchronous_standby_names')"
  done
  ;;
register)
  L=$(leader); [[ -n "$L" ]] || die "找不到控制面（group 0）leader"
  for k in "${WK[@]}"; do
    id=${NODE_ID_LIST[$k]}; ph=${NODE_HOST_LIST[$k]}
    for sh in $(others "$ph"); do
      cpsql "${L%:*}" "${L#*:}" -qAt -c "SELECT partdist.pg_raft_propose_node_standby($id, '$(sname "$id" "$sh")', '$sh', $(sport "$id"), false)" >/dev/null \
        || die "登记 n$id 的备库（$sh）失败"
    done
  done
  for k in "${!NODE_ID_LIST[@]}"; do
    cpsql "${NODE_HOST_LIST[$k]}" "${NODE_PORT_LIST[$k]}" -qAt -c "ALTER SYSTEM SET pg_raft.auto_failover = on" -c "SELECT pg_reload_conf()" >/dev/null \
      || die "n${NODE_ID_LIST[$k]} 打开 auto_failover 失败"
  done
  sleep 2
  echo "控制面 leader $L；已登记的备库："
  cpsql "${L%:*}" "${L#*:}" -qAt -F ' ' -c "SELECT node_id, name, host, port FROM partdist.node_standby ORDER BY 1, 2"
  ;;
status)
  for k in "${WK[@]}"; do
    id=${NODE_ID_LIST[$k]}
    cpsql "${NODE_HOST_LIST[$k]}" "${NODE_PORT_LIST[$k]}" -qAt -F ' ' -c "SELECT 'n$id', application_name, state, sync_state,
      pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) AS lag_bytes FROM pg_stat_replication ORDER BY 2"
  done
  ;;
wait)
  for _ in $(seq 300); do
    ok=0
    for k in "${WK[@]}"; do
      r=$(cpsql "${NODE_HOST_LIST[$k]}" "${NODE_PORT_LIST[$k]}" -qAt -c "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)), -1)::bigint || ' ' || count(*) FROM pg_stat_replication WHERE state = 'streaming'")
      [[ "$r" == "0 2" ]] && ok=$((ok + 1))
    done
    [[ $ok == "${#WK[@]}" ]] && { echo "全部备库已追平"; exit 0; }
    sleep 2
  done
  die "备库 10 分钟内没追平"
  ;;
stop)
  for k in "${WK[@]}"; do
    cpsql "${NODE_HOST_LIST[$k]}" "${NODE_PORT_LIST[$k]}" -qAt -c "ALTER SYSTEM SET synchronous_standby_names = ''" -c "SELECT pg_reload_conf()" >/dev/null 2>&1
  done
  for h in "${HOSTS[@]}"; do on "$h" "docker ps -a --format '{{.Names}}' | grep -E '^$PREFIX-n[0-9]+-s[0-9]+\$' | xargs -r docker rm -f"; done
  ;;
*) echo "用法：$0 standbys|sync|register|status|wait|stop"; exit 2 ;;
esac
