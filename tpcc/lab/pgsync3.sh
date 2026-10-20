#!/bin/bash
# 三机一主两备 PostgreSQL 16.14，原生流复制 + 同步复制（2026-10-20）。
# 参考：博客园《Postgresql - 主从同步》（搭建步骤）+ 官方文档 19.6 Replication（参数语义）。
#
#   primary  = 145:31300（容器 pgsync-primary）
#   standby1 = 147:31300（容器 pgsync-standby1）
#   standby2 = 148:31300（容器 pgsync-standby2）
#
# 同步复制全部由配置参数控制（主库 postgresql.conf，可在线 reload）：
#   synchronous_standby_names = 'ANY 1 (standby1, standby2)'   法定数：任意 1 个备库确认即可
#   synchronous_commit = on                                     确认 = 备库已把 WAL 写到磁盘（remote_flush）
# ⇒ 每笔事务提交返回之前，至少 1 个备库已持久化它的 WAL。
# 备库名靠 primary_conninfo 里的 application_name 对上（pg_basebackup -d 里带上，-R 会写进去）。
#
# 数据目录：145 /data1/yyt/pgsync3/data，147 / 148 /home/yyt/pgsync3/data。口令只存在 145 的 /data1/yyt/pgsync3/.secrets。
# 用法（145 上）：pgsync3.sh deploy | status | test | stop | start | destroy
set -u
IMG=yyt-pg:16.14
PORT=31300
PRIMARY=10.77.110.145
STANDBYS="standby1:10.77.110.147 standby2:10.77.110.148"
BASE145=/data1/yyt/pgsync3
BASEREMOTE=/home/yyt/pgsync3
PGD=/var/lib/postgresql/data          # 容器内挂载点；PGDATA = $PGD/pgdata
# 2026-10-20：可配 shared_buffers / effective_cache_size；MEM_LIMIT（如 8g）给三个容器设内存上限并关 swap
SB=${SB:-8GB}; EFFC=${EFFC:-32GB}
MEMOPT=""; [ -n "${MEM_LIMIT:-}" ] && MEMOPT="--memory $MEM_LIMIT --memory-swap $MEM_LIMIT"
log() { echo "[$(date +%T)] $*"; }
onhost() { local h=$1; shift; if [ "$h" = $PRIMARY ]; then bash -c "$*"; else ssh -n $h "$*"; fi; }
base() { [ "$1" = $PRIMARY ] && echo $BASE145 || echo $BASEREMOTE; }
P() { docker exec -i pgsync-primary psql -U postgres -p $PORT -XAtq "$@"; }
S() { local h=$1 n=$2; shift 2; onhost $h "docker exec -i pgsync-$n psql -U postgres -p $PORT -XAtq $*"; }

secrets() {
  mkdir -p $BASE145; chmod 755 $BASE145     # 容器里的 postgres 用户要能进到 data/；口令文件自己 600
  if [ ! -f $BASE145/.secrets ]; then
    ( umask 077
      echo "SUPW=$(head -c 18 /dev/urandom | base64 | tr -dc A-Za-z0-9)" > $BASE145/.secrets
      echo "REPL_PASS=$(head -c 18 /dev/urandom | base64 | tr -dc A-Za-z0-9)" >> $BASE145/.secrets )
  fi
  . $BASE145/.secrets
}

wait_ready() {   # 主机 容器
  for i in $(seq 90); do onhost $1 "docker exec $2 pg_isready -h 127.0.0.1 -p $PORT -q" && return 0; sleep 1; done
  log "$2 没起来"; onhost $1 "docker logs --tail 20 $2"; exit 1
}

deploy() {
  secrets
  log "清理旧的 pgsync 容器与数据目录"
  for sb in $STANDBYS; do h=${sb#*:}; n=${sb%%:*}
    onhost $h "docker rm -f pgsync-$n >/dev/null 2>&1; mkdir -p $BASEREMOTE/data; chmod 777 $BASEREMOTE/data; docker run --rm -v $BASEREMOTE/data:$PGD $IMG bash -c 'rm -rf $PGD/pgdata'"
  done
  # lab 的 Docker 里容器 root 在宿主机上没有特权（与 node.sh / shardpg_rep.sh 一样，数据目录给 777）
  docker rm -f pgsync-primary >/dev/null 2>&1; mkdir -p $BASE145/data; chmod 777 $BASE145/data
  docker run --rm -v $BASE145/data:$PGD $IMG bash -c "rm -rf $PGD/pgdata"

  log "主库：initdb 并启动（$PRIMARY:$PORT）"
  docker run -d --name pgsync-primary --network host --shm-size 1g $MEMOPT -e POSTGRES_PASSWORD="$SUPW" -e PGDATA=$PGD/pgdata \
    -v $BASE145/data:$PGD $IMG -c port=$PORT >/dev/null
  wait_ready $PRIMARY pgsync-primary
  # 博客"主库配置"：postgresql.conf + pg_hba.conf + 复制用户（同步复制参数此时先不开，见下）
  docker exec -i pgsync-primary bash -c "cat >> $PGD/pgdata/postgresql.conf" <<CONF

# ---- pgsync3：一主两备流复制（博客园《Postgresql - 主从同步》+ 官方文档 19.6）----
listen_addresses = '*'
wal_level = replica                 # 流复制所需
max_wal_senders = 10                # 最多 10 个复制连接
max_replication_slots = 10
wal_keep_size = 1024MB              # 另外再保留 1 GB WAL（备库也用复制槽）
hot_standby = on                    # 备库可只读查询（备库从主库拷来同一份配置）
# 与 TPC-C 对照组同参数（ShardPG / Citus / 单机 PG 都是这一套）
max_connections = 1000
shared_buffers = $SB
effective_cache_size = $EFFC
work_mem = 16MB
maintenance_work_mem = 512MB
max_wal_size = 4GB
checkpoint_timeout = 15min
default_transaction_isolation = 'read committed'
CONF
  docker exec -i pgsync-primary bash -c "cat >> $PGD/pgdata/pg_hba.conf" <<HBA
# pgsync3：只允许两台备库机器以 repl_user 建复制连接
host replication repl_user 10.77.110.147/32 scram-sha-256
host replication repl_user 10.77.110.148/32 scram-sha-256
HBA
  P -c "CREATE ROLE repl_user WITH REPLICATION LOGIN PASSWORD '$REPL_PASS'" >/dev/null
  docker restart pgsync-primary >/dev/null; wait_ready $PRIMARY pgsync-primary

  for sb in $STANDBYS; do h=${sb#*:}; n=${sb%%:*}
    log "备库 $n（$h:$PORT）：pg_basebackup -R -X stream -C -S ${n}_slot，application_name=$n"
    P -c "SELECT pg_drop_replication_slot('${n}_slot') FROM pg_replication_slots WHERE slot_name = '${n}_slot'" >/dev/null
    onhost $h "set -e
      docker run --rm -v $BASEREMOTE/data:$PGD $IMG bash -c 'mkdir -p $PGD/pgdata && chown -R postgres:postgres $PGD && chmod 700 $PGD/pgdata'
      docker run --rm --network host -v $BASEREMOTE/data:$PGD $IMG gosu postgres pg_basebackup \
        -d 'host=$PRIMARY port=$PORT user=repl_user password=$REPL_PASS application_name=$n' \
        -D $PGD/pgdata -R -X stream -C -S ${n}_slot -c fast
      docker run -d --name pgsync-$n --network host --shm-size 1g $MEMOPT -e PGDATA=$PGD/pgdata -v $BASEREMOTE/data:$PGD $IMG -c port=$PORT >/dev/null" \
      || { log "$n 建库失败"; exit 1; }
    wait_ready $h pgsync-$n
  done
  for i in $(seq 60); do [ "$(P -c "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")" = 2 ] && break; sleep 1; done

  log "主库：打开同步复制（写进 postgresql.conf，reload 生效）"
  docker exec -i pgsync-primary bash -c "cat >> $PGD/pgdata/postgresql.conf" <<'CONF'
# 同步复制：提交前至少 1 个备库把 WAL 写到磁盘
synchronous_commit = on
synchronous_standby_names = 'ANY 1 (standby1, standby2)'
CONF
  P -c "SELECT pg_reload_conf()" >/dev/null; sleep 1
  status
}

status() {
  echo "主库 $PRIMARY:$PORT  synchronous_standby_names = $(P -c 'SHOW synchronous_standby_names')  synchronous_commit = $(P -c 'SHOW synchronous_commit')"
  P -F' ' -c "SELECT '  ' || application_name, client_addr, state, 'sync_state=' || sync_state, 'flush_lag=' || coalesce(flush_lag::text, '0'), 'replay_lag_bytes=' || pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) FROM pg_stat_replication ORDER BY 1"
  P -F' ' -c "SELECT '  复制槽 ' || slot_name, 'active=' || active FROM pg_replication_slots ORDER BY 1"
  for sb in $STANDBYS; do h=${sb#*:}; n=${sb%%:*}
    echo "  $n $h:$PORT：在恢复中 = $(S $h $n "-c 'SELECT pg_is_in_recovery()'" 2>&1)，已回放到 $(S $h $n "-c 'SELECT pg_last_wal_replay_lsn()'" 2>&1)"
  done
}

ins() { local t0 t1; t0=$(date +%s%N); P -c "INSERT INTO sync_demo(note) VALUES ('$1')" >/dev/null; t1=$(date +%s%N); echo $(( (t1 - t0) / 1000000 )); }
rows() { S $1 $2 "-c 'SELECT count(*) FROM sync_demo'" 2>/dev/null; }

test_sync() {
  P -c "CREATE TABLE IF NOT EXISTS sync_demo (id serial PRIMARY KEY, note text, at timestamptz DEFAULT now())" >/dev/null
  log "测试 1：两个备库都在线 —— 提交用时 $(ins 'both up') ms"
  sleep 1; log "  主库 $(P -c 'SELECT count(*) FROM sync_demo') 行；standby1 $(rows 10.77.110.147 standby1) 行；standby2 $(rows 10.77.110.148 standby2) 行"

  ssh -n 10.77.110.147 "docker stop pgsync-standby1" >/dev/null; sleep 2
  log "测试 2：停掉 standby1 —— 提交用时 $(ins 'standby1 down') ms（ANY 1 由 standby2 满足，不受影响）"
  log "  此刻同步状态：$(P -F' ' -c "SELECT string_agg(application_name || '=' || sync_state, ' ') FROM pg_stat_replication")"

  ssh -n 10.77.110.148 "docker stop pgsync-standby2" >/dev/null; sleep 2
  log "测试 3：两个备库都停 —— 提交应卡在等待同步备库确认"
  ( t0=$(date +%s); P -c "INSERT INTO sync_demo(note) VALUES ('both down')" > /tmp/pgsync_wait.out 2>&1; echo "rc=$? 用时 $(( $(date +%s) - t0 )) s" >> /tmp/pgsync_wait.out ) &
  W=$!; sleep 5
  log "  5 s 后这条提交的状态：$(P -c "SELECT coalesce(wait_event, '-') || '，已等 ' || round(extract(epoch FROM now() - query_start)) || ' s' FROM pg_stat_activity WHERE query LIKE 'INSERT INTO sync_demo%both down%' AND pid <> pg_backend_pid()")"
  t0=$(date +%s%N); P -c "SET synchronous_commit = local" -c "INSERT INTO sync_demo(note) VALUES ('local, both down')" >/dev/null; t1=$(date +%s%N)
  log "  对比：本事务 SET synchronous_commit = local 后提交用时 $(( (t1 - t0) / 1000000 )) ms（不等备库，官方文档：参数可按事务放宽）"
  ssh -n 10.77.110.148 "docker start pgsync-standby2" >/dev/null; T=$(date +%s)
  wait $W; log "  拉起 standby2 后，卡住的提交完成：$(tail -1 /tmp/pgsync_wait.out)（standby2 启动后 $(( $(date +%s) - T )) s）"

  ssh -n 10.77.110.147 "docker start pgsync-standby1" >/dev/null
  for i in $(seq 60); do [ "$(P -c "SELECT count(*) FROM pg_stat_replication WHERE state = 'streaming'")" = 2 ] && break; sleep 1; done
  sleep 2
  log "测试 4：全部恢复 —— 主库 $(P -c 'SELECT count(*) FROM sync_demo') 行；standby1 $(rows 10.77.110.147 standby1) 行；standby2 $(rows 10.77.110.148 standby2) 行"
  P -F' | ' -c "SELECT id, note FROM sync_demo ORDER BY id" | sed 's/^/    /'
  status
}

case "${1:-}" in
  deploy) deploy ;;
  status) status ;;
  test) test_sync ;;
  stop) for sb in $STANDBYS; do onhost ${sb#*:} "docker stop pgsync-${sb%%:*}" >/dev/null; done; docker stop pgsync-primary >/dev/null; log "已停" ;;
  start) docker start pgsync-primary >/dev/null; for sb in $STANDBYS; do onhost ${sb#*:} "docker start pgsync-${sb%%:*}" >/dev/null; done; log "已起" ;;
  destroy) for sb in $STANDBYS; do onhost ${sb#*:} "docker rm -f pgsync-${sb%%:*} >/dev/null 2>&1; docker run --rm -v $BASEREMOTE/data:$PGD $IMG bash -c 'rm -rf $PGD/pgdata'"; done
           docker rm -f pgsync-primary >/dev/null 2>&1; docker run --rm -v $BASE145/data:$PGD $IMG bash -c "rm -rf $PGD/pgdata"; log "已删除" ;;
  *) sed -n 2,19p "$0"; exit 1 ;;
esac
