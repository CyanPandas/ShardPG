#!/bin/bash
# ============================================================================
# ShardPG 演示集群（shardpg-ddl 构建）—— 在本机管理一个节点容器
#
#   bash demo_node.sh up      <名字>    起容器 → initdb → 写配置 → 启动 → 设口令（容器已存在就拒绝）
#   bash demo_node.sh start   <名字>    启动节点里的 PostgreSQL（容器一直开着，只起停数据库）
#   bash demo_node.sh stop    <名字> [fast|immediate]
#   bash demo_node.sh status  <名字>
#   bash demo_node.sh psql    <名字> [psql 参数…]   交互式连这个节点（容器内 unix socket）
#   bash demo_node.sh sql     <名字> [psql 参数…]   从标准输入执行 SQL
#   bash demo_node.sh destroy <名字>    删容器 + 删这个节点的数据目录（只删本部署目录里的）
#
# 节点表 nodes.conf（四台节点同一份）：名字 节点号 主机IP 端口
# 安全：只监听 127.0.0.1 和本机 IP；TCP 一律 scram 口令认证、只放行三台实验机；
#       口令只在本目录 secret 文件（600），容器内经 PGPASSFILE 读；不做端口映射以外的任何改动。
# ============================================================================
set -euo pipefail
D=$(cd "$(dirname "$0")" && pwd)
IMG=shardpg-ddl-demo:base
BIN=/work/pg-install/bin
cmd=${1:?用法见文件头}; NAME=${2:?缺节点名}
row=$(awk -v n="$NAME" '$1 == n' "$D/nodes.conf")
[[ -n "$row" ]] || { echo "nodes.conf 里没有 $NAME" >&2; exit 1; }
read -r _ ID IP PORT <<<"$row"
C=shardpg-ddl-$NAME
P=/work/pg-cluster-data/$NAME
MASTER_IP=$(awk '$2 == 1 {print $3}' "$D/nodes.conf"); MASTER_PORT=$(awk '$2 == 1 {print $4}' "$D/nodes.conf")
PEERS=$(awk '!/^#/ && NF == 4 {printf "%s%s@%s:%s", (n++ ? "," : ""), $2, $3, $4}' "$D/nodes.conf")
dex() { docker exec -i "$C" "$@"; }

case $cmd in
up)
    docker ps -a --format '{{.Names}}' | grep -qx "$C" && { echo "容器 $C 已存在" >&2; exit 1; }
    [[ -e "$D/data/$NAME" ]] && { echo "$D/data/$NAME 已存在" >&2; exit 1; }
    mkdir -p "$D/data"; chmod 777 "$D/data"            # 容器里 postgres 是 uid 999；上层目录是 700，别人进不来
    docker run -d --init --name "$C" --network host --shm-size 1g \
        -e PGPASSFILE=/work/pg-cluster-data/.pgpass -e LANG=C.UTF-8 -e PGCLIENTENCODING=UTF8 \
        -v "$D/pg-install:/work/pg-install:ro" -v "$D/data:/work/pg-cluster-data" \
        "$IMG" sleep infinity >/dev/null
    # psql 退出时要写 ~/.psql_history；镜像里 postgres 的家目录不存在，不建的话 \q 会报 could not save history
    docker exec -u root "$C" sh -c 'mkdir -p /home/postgres && chown postgres:postgres /home/postgres && chmod 700 /home/postgres'
    # 口令文件：容器里 postgres 所有、600（libpq 要求）
    awk '{print "*:*:*:postgres:" $0}' "$D/secret" | dex bash -c 'umask 077; cat > /work/pg-cluster-data/.pgpass'
    if [[ $ID == 1 ]]; then ET=6000; TSO="pg_partdist.tso_conninfo = 'host=/tmp port=$PORT dbname=postgres user=postgres'"
    else ET=15000; TSO="pg_partdist.tso_conninfo = 'host=$MASTER_IP port=$MASTER_PORT dbname=postgres user=postgres'"; fi
    dex bash -s <<EOF
set -e
$BIN/initdb -D $P -U postgres -E UTF8 --no-locale > $P.initdb.log 2>&1
cat >> $P/postgresql.conf <<'CONF'

# ---- ShardPG 演示集群（demo_node.sh）----
listen_addresses = '127.0.0.1,$IP'
port = $PORT
shared_preload_libraries = 'citus,pg_partdist,pg_raft'
max_prepared_transactions = 200
shared_buffers = 128MB
max_wal_size = 512MB
max_worker_processes = 16
logging_collector = off
password_encryption = scram-sha-256
pg_raft.node_id = $ID
pg_raft.raft_enabled = on
pg_raft.peers = '$PEERS'
pg_raft.heartbeat_ms = 1000
pg_raft.election_timeout_ms = $ET
pg_raft.probe_interval_ms = 3000
pg_raft.probe_fail_threshold = 1
pg_raft.coordinator_node_id = 1
pg_raft.max_groups = 192
citus.recover_2pc_interval = -1
pg_partdist.debug_sigusr2_backtrace = on
pg_partdist.debug_segv_backtrace = on
pg_partdist.tso_lease_ms = 300000
$TSO
CONF
cat > $P/pg_hba.conf <<'HBA'
# 本容器内 unix socket（只能 docker exec 进来）免密；TCP 一律口令，只放行三台实验机
local all all trust
host  all all 127.0.0.1/32     scram-sha-256
host  all all 10.77.110.145/32 scram-sha-256
host  all all 10.77.110.147/32 scram-sha-256
host  all all 10.77.110.148/32 scram-sha-256
HBA
$BIN/pg_ctl start -D $P -l $P/pg.log -w -t 60 >/dev/null
EOF
    # 设口令走 stdin，不出现在命令行
    awk '{printf "ALTER USER postgres PASSWORD %c%s%c;\n", 39, $0, 39}' "$D/secret" \
        | dex $BIN/psql -h /tmp -p "$PORT" -U postgres -X -q -v ON_ERROR_STOP=1 >/dev/null
    echo "$NAME (node id $ID, $IP:$PORT) started in container $C"
    ;;
start)
    dex $BIN/pg_ctl start -D "$P" -l "$P/pg.log" -w -t 90 ;;
stop)
    dex $BIN/pg_ctl stop -D "$P" -m "${3:-fast}" -w -t 90 ;;
status)
    docker ps -a --filter "name=^${C}\$" --format '{{.Names}} {{.Status}}'
    dex $BIN/pg_ctl status -D "$P" | head -1 ;;
psql)
    shift 2; docker exec -it "$C" $BIN/psql -h /tmp -p "$PORT" -U postgres "$@" ;;
sql)    # 从标准输入读 SQL（脚本用），出错即停
    shift 2; dex $BIN/psql -h /tmp -p "$PORT" -U postgres -X -v ON_ERROR_STOP=1 "$@" ;;
destroy)
    docker rm -f "$C" >/dev/null 2>&1 || true
    # 数据文件属容器里的 uid 999，用一次性容器删；只删本部署目录里这个节点的
    docker run --rm -v "$D/data:/x" "$IMG" bash -c "rm -rf /x/$NAME /x/$NAME.initdb.log"
    echo "$NAME 已删除" ;;
*) echo "未知命令 $cmd" >&2; exit 1 ;;
esac
