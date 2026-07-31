#!/usr/bin/env bash
# [宿主机] 一键完整复现 shardpg-replay 测试环境：1 coordinator + N workers（默认 8）。
#
# 本分支（shardpg-replay）是惰性回放模块的开发分支，从 shardpg-3.0 拉出。默认值已
# 绑定到本分支的标准开发环境 pg-citus-replay（9 节点），因此在本分支上直接
# `./reproduce-env.sh all` 即可原样重建该环境；改分支/规模请用环境变量覆盖。
#
# 复现内容与 raft4 环境同构：同一镜像、容器内 /work 布局、同一套 postgresql.conf
# 模板（pg_raft 参数与 setup-raft.sh 一致）、Citus 接线（coordinator 注册 + N 个
# citus_add_node）、partdist.node_map 种子、pg_partdist/pg_raft 现场编译安装。
# 不同点仅两处，均为参数：节点数 N_WORKERS、内存参数 SHARED_BUFFERS（小内存宿主
# 机上 17 实例跑不动 128MB x 17，默认降为 32MB）。
#
# 用法:
#   ./reproduce-env.sh up        # 克隆 + 起容器 + 编扩展 + initdb + 接线 + 等收敛
#   ./reproduce-env.sh verify    # 六项一致性/功能校验（V1..V6）
#   ./reproduce-env.sh destroy   # 删容器 + 删克隆目录
#   ./reproduce-env.sh all       # up + verify
#
# 可覆盖的环境变量:
#   ENV_NAME=pg-citus-replay          环境名（容器名/目录名前缀）
#   N_WORKERS=8                       worker 数（group0 成员 = N+1，须 <= RAFT_MAX_PEERS）
#   SRC_REPO=<url|path>               克隆源，默认 GitHub CyanPandas/ShardPG
#   BRANCH=shardpg-replay
#   IMAGE=pg-partdist-raft4-env       容器镜像；不存在则用克隆里的 Dockerfile 构建
#   SHARED_BUFFERS=32MB               每实例 shared_buffers
set -euo pipefail

ENV_NAME="${ENV_NAME:-pg-citus-replay}"
N_WORKERS="${N_WORKERS:-8}"
SRC_REPO="${SRC_REPO:-https://github.com/CyanPandas/ShardPG.git}"
BRANCH="${BRANCH:-shardpg-replay}"
IMAGE="${IMAGE:-pg-partdist-raft4-env}"
SHARED_BUFFERS="${SHARED_BUFFERS:-32MB}"

CLONE_ROOT="/tmp/${ENV_NAME}"
CLONE_DIR="${CLONE_ROOT}/ShardPG"
CONTAINER="${ENV_NAME//_/-}-container"
COORD_PORT=5432                       # coordinator=node1; worker i => 端口 5432+i, node_id i+1
N_NODES=$((N_WORKERS + 1))

DEX() { docker exec -i -u postgres "$CONTAINER" "$@"; }
DEX0() { docker exec -i -u 0 "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

node_dir()  { local i=$1; [[ $i -eq 1 ]] && echo coordinator || echo "worker$((i-1))"; }
node_port() { local i=$1; echo $((5431 + i)); }

build_peers() {
  local s="" i
  for i in $(seq 1 "$N_NODES"); do
    s+="${s:+,}${i}@127.0.0.1:$(node_port "$i")"
  done
  echo "$s"
}

die() { echo "FATAL: $*" >&2; exit 1; }

# ============================== up ==============================
do_up() {
  echo "========== [0/6] 前置检查 =========="
  [[ -e "$CLONE_ROOT" ]] && die "目录 $CLONE_ROOT 已存在，先 destroy 或换 ENV_NAME"
  docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER" && die "容器 $CONTAINER 已存在"

  echo "========== [1/6] 克隆 ${BRANCH} =========="
  mkdir -p "$CLONE_ROOT"
  git clone --branch "$BRANCH" --single-branch "$SRC_REPO" "$CLONE_DIR"
  local head max_peers
  head=$(git -C "$CLONE_DIR" rev-parse --short HEAD)
  echo "HEAD = $head"
  max_peers=$(grep -oP '#define RAFT_MAX_PEERS\s+\K[0-9]+' "$CLONE_DIR/pg-raft-src/src/raft_consensus.c")
  [[ "$N_NODES" -le "$max_peers" ]] || die "节点数 ${N_NODES} 超过 RAFT_MAX_PEERS=${max_peers}"

  echo "========== [2/6] 起容器（镜像 ${IMAGE}）=========="
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "镜像不存在，从克隆构建..."
    docker build -t "$IMAGE" "$CLONE_DIR"
  fi
  docker run -d --name "$CONTAINER" "$IMAGE" sleep infinity >/dev/null
  DEX0 mkdir -p /work
  for d in pg-install pg-partdist-src pg-raft-src; do
    docker cp "$CLONE_DIR/$d" "$CONTAINER:/work/$d"
  done
  DEX0 chown -R postgres:postgres /work
  DEX0 bash -c "echo '$head' > /work/REPRODUCED_FROM_COMMIT"

  echo "========== [3/6] 编译安装 pg_partdist + pg_raft =========="
  DEX bash -c 'cd /work/pg-partdist-src && make clean >/dev/null 2>&1; make -s PG_CONFIG=/work/pg-install/bin/pg_config && make -s install PG_CONFIG=/work/pg-install/bin/pg_config' >/dev/null
  DEX bash -c 'cd /work/pg-raft-src && make clean >/dev/null 2>&1; make -s PG_CONFIG=/work/pg-install/bin/pg_config && make -s install PG_CONFIG=/work/pg-install/bin/pg_config' >/dev/null
  echo "两个扩展编译安装完成"

  echo "========== [4/6] initdb + 配置 ${N_NODES} 个节点 =========="
  local PEERS; PEERS=$(build_peers)
  local i dir port
  for i in $(seq 1 "$N_NODES"); do
    dir=$(node_dir "$i"); port=$(node_port "$i")
    DEX /work/pg-install/bin/initdb -D "/work/pg-cluster-data/$dir" -U postgres --no-locale >/dev/null
    DEX bash -c "cat >> /work/pg-cluster-data/$dir/postgresql.conf" <<EOF

# reproduce-env.sh（与 raft4 环境 setup-raft.sh 同一模板，仅节点数/内存参数化）
port = ${port}
shared_preload_libraries = 'citus,pg_partdist,pg_raft'
max_prepared_transactions = 200
shared_buffers = ${SHARED_BUFFERS}
max_wal_size = 512MB
logging_collector = off
pg_raft.node_id = ${i}
pg_raft.raft_enabled = on
pg_raft.peers = '${PEERS}'
pg_raft.heartbeat_ms = 400
pg_raft.election_timeout_ms = 1500
pg_raft.probe_interval_ms = 3000
pg_raft.probe_fail_threshold = 1
# 协调节点(coordinator)：group 0 leader 优先落于此，且不得作为数据组成员
pg_raft.coordinator_node_id = 1
EOF
    DEX /work/pg-install/bin/pg_ctl start -D "/work/pg-cluster-data/$dir" \
        -l "/work/pg-cluster-data/$dir.log" -w -t 60 >/dev/null
    echo "  node$i ($dir:$port) 已启动"
  done

  echo "========== [5/6] 装扩展 + Citus 接线 + node_map 种子 =========="
  for i in $(seq 1 "$N_NODES"); do
    port=$(node_port "$i")
    PSQL "$port" -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE EXTENSION IF NOT EXISTS citus;
SET citus.enable_ddl_propagation = off;
CREATE EXTENSION IF NOT EXISTS pg_partdist;
CREATE EXTENSION IF NOT EXISTS pg_raft;
SQL
    # 与 setup-raft.sh 相同的幂等补齐（全新库应为 no-op，留作保险）
    PSQL "$port" -v ON_ERROR_STOP=0 -q <<'SQL' >/dev/null 2>&1 || true
SET citus.enable_ddl_propagation = off;
ALTER TABLE partdist.raft_log ADD COLUMN IF NOT EXISTS group_id BIGINT NOT NULL DEFAULT 0;
CREATE UNIQUE INDEX IF NOT EXISTS idx_raft_log_group_index ON partdist.raft_log(group_id, log_index);
ALTER TABLE partdist.partition_map ADD COLUMN IF NOT EXISTS primary_term BIGINT NOT NULL DEFAULT 0;
SQL
    # node_map 种子（每个节点一份全量拓扑，与 raft4 环境一致：127.0.0.1 + 端口）
    local j vals=""
    for j in $(seq 1 "$N_NODES"); do
      vals+="${vals:+,}(${j}, '127.0.0.1', $(node_port "$j"), 'active', now())"
    done
    PSQL "$port" -v ON_ERROR_STOP=1 -q -c \
      "INSERT INTO partdist.node_map (node_id, hostname, port, status, last_heartbeat)
       VALUES ${vals}
       ON CONFLICT (node_id) DO UPDATE SET hostname=EXCLUDED.hostname,
         port=EXCLUDED.port, status=EXCLUDED.status, last_heartbeat=now();"
  done
  PSQL "$COORD_PORT" -v ON_ERROR_STOP=1 -q -c \
    "SELECT citus_set_coordinator_host('localhost', ${COORD_PORT});" >/dev/null
  for i in $(seq 2 "$N_NODES"); do
    PSQL "$COORD_PORT" -v ON_ERROR_STOP=1 -q -c \
      "SELECT citus_add_node('localhost', $(node_port "$i"));" >/dev/null
    echo "  citus_add_node(localhost:$(node_port "$i")) done"
  done

  echo "========== [6/6] 等待 group0 收敛 =========="
  local t leader=""
  for t in $(seq 1 60); do
    leader=$(PSQL "$COORD_PORT" -Atc \
      "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" 2>/dev/null || echo "")
    [[ -n "$leader" && "$leader" != "0" ]] && break
    sleep 1
  done
  [[ -n "$leader" && "$leader" != "0" ]] || die "group0 在 60s 内未选出 leader"
  echo "group0 leader = node ${leader}（${t}s 收敛）"
  echo "环境 ${ENV_NAME} 就绪：容器 ${CONTAINER}，${N_NODES} 节点（coordinator:5432 + worker1..${N_WORKERS}:5433..$(node_port "$N_NODES")）"
}

# ============================ verify ============================
V_FAIL=0
check() {  # check <名字> <实际> <期望>
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; else echo "  FAIL  $1（实际='$2' 期望='$3'）"; V_FAIL=1; fi
}

do_verify() {
  local i port
  echo "========== V1 全节点可连 + 三扩展就位 =========="
  for i in $(seq 1 "$N_NODES"); do
    port=$(node_port "$i")
    check "node$i 扩展 citus,pg_partdist,pg_raft" \
      "$(PSQL "$port" -Atc "SELECT count(*) FROM pg_extension WHERE extname IN ('citus','pg_partdist','pg_raft')" 2>/dev/null || echo ERR)" "3"
  done

  echo "========== V2 Citus 拓扑（coordinator + ${N_WORKERS} workers，元数据同步到 worker）=========="
  check "pg_dist_node 行数(coordinator)" \
    "$(PSQL "$COORD_PORT" -Atc 'SELECT count(*) FROM pg_dist_node')" "$N_NODES"
  check "pg_dist_node 行数(worker1 元数据同步)" \
    "$(PSQL 5433 -Atc 'SELECT count(*) FROM pg_dist_node')" "$N_NODES"

  echo "========== V3 group0：${N_NODES} 成员、全员同 leader 同任期 =========="
  check "cluster_size(coordinator 视角)" \
    "$(PSQL "$COORD_PORT" -Atc 'SELECT cluster_size FROM partdist.pg_raft_group_status() WHERE group_id=0')" "$N_NODES"
  local pairs
  pairs=$(for i in $(seq 1 "$N_NODES"); do
    PSQL "$(node_port "$i")" -Atc \
      "SELECT leader_node_id || '/' || current_term FROM partdist.pg_raft_get_cluster_status()" 2>/dev/null
  done | sort -u)
  check "全 ${N_NODES} 节点 leader/term 一致" "$(echo "$pairs" | wc -l)" "1"
  echo "        （leader/term = ${pairs}）"

  echo "========== V4 node_map 全量种子 =========="
  for port in "$COORD_PORT" 5433 "$(node_port "$N_NODES")"; do
    check "node_map@${port} active 行数" \
      "$(PSQL "$port" -Atc "SELECT count(*) FROM partdist.node_map WHERE status='active'")" "$N_NODES"
  done

  echo "========== V5 Citus 分布表冒烟（${N_WORKERS} 分片撒满 ${N_WORKERS} worker）=========="
  PSQL "$COORD_PORT" -v ON_ERROR_STOP=1 -q -c \
    "DROP TABLE IF EXISTS reply_v5; SET citus.shard_count=${N_WORKERS}; SET citus.shard_replication_factor=1;
     CREATE TABLE reply_v5(id int primary key, v text); SELECT create_distributed_table('reply_v5','id');" >/dev/null
  check "分片分布的 worker 数" \
    "$(PSQL "$COORD_PORT" -Atc "SELECT count(DISTINCT p.groupid) FROM pg_dist_placement p JOIN pg_dist_shard s ON s.shardid=p.shardid WHERE s.logicalrelid='reply_v5'::regclass")" "$N_WORKERS"
  PSQL "$COORD_PORT" -v ON_ERROR_STOP=1 -q -c \
    "INSERT INTO reply_v5 SELECT g, 'v'||g FROM generate_series(1,160) g;" >/dev/null
  check "插入 160 行回读" "$(PSQL "$COORD_PORT" -Atc 'SELECT count(*) FROM reply_v5')" "160"

  echo "========== V6 数据面复制冒烟（单分片组：leader→2 followers 备份逐字节一致）=========="
  PSQL "$COORD_PORT" -v ON_ERROR_STOP=1 -q -c \
    "DROP TABLE IF EXISTS reply_v6; SET citus.shard_count=1; SET citus.shard_replication_factor=1;
     CREATE TABLE reply_v6(id int primary key, v text); SELECT create_distributed_table('reply_v6','id');" >/dev/null
  local gid pport pid f1 f2
  gid=$(PSQL "$COORD_PORT" -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='reply_v6'::regclass")
  pport=$(PSQL "$COORD_PORT" -Atc \
    "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
  pid=$((pport - 5431))
  f1=""; f2=""
  for i in $(seq 2 "$N_NODES"); do
    port=$(node_port "$i")
    [[ "$port" == "$pport" ]] && continue
    if [[ -z "$f1" ]]; then f1=$port; elif [[ -z "$f2" ]]; then f2=$port; else break; fi
  done
  for port in "$f1" "$f2"; do
    PSQL "$port" -v ON_ERROR_STOP=1 -q -c \
      "SET citus.enable_ddl_propagation=off; CREATE TABLE IF NOT EXISTS reply_v6_${gid} (LIKE reply_v6 INCLUDING ALL);" >/dev/null
  done
  for port in "$pport" "$f1" "$f2"; do
    PSQL "$port" -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
  done
  local members="ARRAY[${pid}, $((f1 - 5431)), $((f2 - 5431))]"
  PSQL "$pport" -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
  local t st=""
  for t in $(seq 1 20); do
    st=$(PSQL "$pport" -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null || echo "")
    [[ "$st" == "leader" ]] && break; sleep 1
  done
  check "分区组 ${gid} leader 落在 placement 节点(:${pport})" "$st" "leader"
  for port in "$f1" "$f2"; do
    PSQL "$port" -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
  done
  PSQL "$COORD_PORT" -v ON_ERROR_STOP=1 -q -c \
    "INSERT INTO reply_v6 SELECT g, 'r'||g FROM generate_series(1,3) g;" >/dev/null
  sleep 2
  local fp_sql="SELECT partdist.get_partition_flush_lsn(oidv) || ':' || COALESCE(md5(string_agg(sub.h, ',' ORDER BY sub.plsn)),'') FROM (SELECT partdist.local_partition_for_shard(${gid}) AS oidv) o, LATERAL (SELECT g AS plsn, md5(r.data) AS h FROM generate_series(1, partdist.get_partition_flush_lsn(o.oidv)) g, LATERAL partdist.partwal_read_record(o.oidv, g) r) sub GROUP BY oidv"
  local lead_fp f1_fp f2_fp
  lead_fp=$(PSQL "$pport" -Atc "$fp_sql")
  f1_fp=$(PSQL "$f1" -Atc "$fp_sql")
  f2_fp=$(PSQL "$f2" -Atc "$fp_sql")
  check "leader 侧有 parwal 记录(防两侧皆空的假阳性)" \
    "$([[ -n "$lead_fp" && "$lead_fp" != 0:* ]] && echo ok || echo empty)" "ok"
  check "follower1(:${f1}) parwal 指纹 == leader" "$f1_fp" "$lead_fp"
  check "follower2(:${f2}) parwal 指纹 == leader" "$f2_fp" "$lead_fp"
  check "follower 壳表 0 行(只备份不回放)" \
    "$(PSQL "$f1" -Atc "SELECT count(*) FROM reply_v6_${gid}")" "0"
  check "coordinator 收到自治登记(primary=${pid}, term>=1)" \
    "$(PSQL "$COORD_PORT" -Atc "SELECT count(*) FROM partdist.partition_map WHERE partition_id=${gid} AND primary_node=${pid} AND primary_term>=1")" "1"

  # 冒烟夹具清理
  for port in "$pport" "$f1" "$f2"; do
    PSQL "$port" -q -c "SELECT partdist.pg_raft_group_reset();" >/dev/null 2>&1 || true
  done
  for port in "$f1" "$f2"; do
    PSQL "$port" -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS reply_v6_${gid};" >/dev/null 2>&1 || true
  done
  PSQL "$COORD_PORT" -q -c "DROP TABLE IF EXISTS reply_v6; DROP TABLE IF EXISTS reply_v5;" >/dev/null 2>&1 || true
  for i in $(seq 1 "$N_NODES"); do
    PSQL "$(node_port "$i")" -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" >/dev/null 2>&1 || true
  done

  echo
  if [[ "$V_FAIL" -eq 0 ]]; then
    echo "VERIFY: 全部通过（${N_NODES} 节点环境与 ${BRANCH} 预期一致）"
  else
    echo "VERIFY: 存在 FAIL 项"; exit 1
  fi
}

# =========================== destroy ============================
do_destroy() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 && echo "容器 ${CONTAINER} 已删" || echo "容器 ${CONTAINER} 不存在"
  rm -rf "$CLONE_ROOT" && echo "目录 ${CLONE_ROOT} 已删"
}

case "${1:-all}" in
  up)      do_up ;;
  verify)  do_verify ;;
  destroy) do_destroy ;;
  all)     do_up; do_verify ;;
  *)       die "用法: $0 {up|verify|destroy|all}" ;;
esac
