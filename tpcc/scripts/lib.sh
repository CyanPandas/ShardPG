#!/usr/bin/env bash
# TPC-C 脚本公共部分：读 cluster.env、psql 封装。由其它脚本 source。
set -uo pipefail

TPCC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$TPCC_DIR/cluster.env}"
[[ -f "$ENV_FILE" ]] || { echo "缺 $ENV_FILE（从 cluster.env.example 复制一份改）" >&2; exit 2; }
# shellcheck disable=SC1090
source "$ENV_FILE"

PSQL_BIN="${PSQL_BIN:-${PG_BIN}/psql}"
PGBENCH_BIN="${PGBENCH_BIN:-${PG_BIN}/pgbench}"
[[ -x "$PSQL_BIN" ]] || PSQL_BIN=psql
[[ -x "$PGBENCH_BIN" ]] || PGBENCH_BIN=pgbench

# 集群元数据在 postgres 库：pg_raft / pg_partdist 的节点间连接串都写死了
# dbname=postgres user=postgres，TPC-C 也必须建在这个库里。
DB=postgres
DBUSER=postgres

# Q <host> <port> <psql 参数...>
Q()  { local h=$1 p=$2; shift 2; "$PSQL_BIN" -h "$h" -p "$p" -U "$DBUSER" -d "$DB" -X -v ON_ERROR_STOP=1 "$@"; }
QC() { Q "$COORD_HOST" "$COORD_PORT" "$@"; }
# 只取值
QV() { local h=$1 p=$2; shift 2; Q "$h" "$p" -qAt -F $'\t' -c "$*"; }

die() { echo "FATAL: $*" >&2; exit 1; }
log() { echo "[$(date '+%F %T')] $*"; }

node_count() { echo "${#NODE_IDS[@]}"; }

# Raft peers 串：id@host:port,...
peers_string() {
  local s="" i
  for i in "${!NODE_IDS[@]}"; do
    s+="${s:+,}${NODE_IDS[$i]}@${NODE_HOSTS[$i]}:${NODE_PORTS[$i]}"
  done
  echo "$s"
}

# worker 下标列表
worker_indexes() {
  local i
  for i in "${!NODE_IDS[@]}"; do
    [[ "${NODE_ROLES[$i]}" == worker ]] && echo "$i"
  done
}
