#!/usr/bin/env bash
# [容器内，postgres 用户] 准备一台新 worker（P7-N102）：initdb → 照现有 worker 复制配置 →
# pg_raft.peers = 现有列表 + 它自己 → 启动 → 建扩展 → node_map 种子。
# 做完之后在 master 上 SELECT * FROM shardpg.add_node('<host>', <port>); 让它入簇（不重启任何节点）。
#
# 用法：new_worker_node.sh <节点号> <端口> [数据目录] [模板数据目录] [master 端口] [对外地址]
#   缺省：数据目录 /work/pg-cluster-data/worker<节点号-1>，模板 = 最后一台 worker 的数据目录，
#         master 端口 5432，对外地址 127.0.0.1（与现有 pg_raft.peers / node_map 一致）
# 也可以在 master 的 psql 里经 COPY … TO PROGRAM 调用（以 postgres 用户在 master 所在机器上执行）：
#   COPY (SELECT 1) TO PROGRAM 'bash /work/pg-partdist-src/scripts/new_worker_node.sh 6 5437';
set -euo pipefail

NODE_ID="${1:?节点号}"
PORT="${2:?端口}"
BIN=/work/pg-install/bin
ROOT=/work/pg-cluster-data
DATA="${3:-$ROOT/worker$((NODE_ID - 1))}"
TEMPLATE="${4:-$(ls -d $ROOT/worker* | sort -V | tail -1)}"
MASTER_PORT="${5:-5432}"
HOST="${6:-127.0.0.1}"
PSQL() { "$BIN/psql" -h /tmp -U postgres -X -At -v ON_ERROR_STOP=1 "$@"; }

[[ -d "$TEMPLATE" && -f "$TEMPLATE/postgresql.conf" ]] || { echo "模板目录 $TEMPLATE 不是数据目录"; exit 1; }
if [[ -e "$DATA" && -n "$(ls -A "$DATA" 2>/dev/null)" ]]; then
  echo "数据目录 $DATA 已存在且非空，不覆盖"; exit 1
fi
PEERS=$(PSQL -p "$MASTER_PORT" -c "SHOW pg_raft.peers")
case ",$PEERS," in
  *",$NODE_ID@"*) echo "节点号 $NODE_ID 已在 pg_raft.peers 里：$PEERS"; exit 1 ;;
  *":$PORT,"*)    echo "端口 $PORT 已在 pg_raft.peers 里：$PEERS"; exit 1 ;;
esac
NEW_PEERS="$PEERS,$NODE_ID@$HOST:$PORT"

echo "[1/5] initdb $DATA"
"$BIN/initdb" -D "$DATA" -U postgres --no-locale >/dev/null

echo "[2/5] 配置：照 $TEMPLATE 复制，改 port / pg_raft.node_id / pg_raft.peers"
cp "$TEMPLATE/postgresql.conf" "$TEMPLATE/pg_hba.conf" "$DATA/"
[[ -f "$TEMPLATE/postgresql.auto.conf" ]] && cp "$TEMPLATE/postgresql.auto.conf" "$DATA/"
# Citus 建扩展时开了 ssl，证书跟着配置走
for f in server.crt server.key; do [[ -f "$TEMPLATE/$f" ]] && cp -p "$TEMPLATE/$f" "$DATA/"; done
sed -i -E "s/^port = .*/port = $PORT/; s/^pg_raft\.node_id = .*/pg_raft.node_id = $NODE_ID/; s/^pg_raft\.peers = .*/pg_raft.peers = '$NEW_PEERS'/" "$DATA/postgresql.conf"
sed -i -E '/^pg_raft\.peers *=/d' "$DATA/postgresql.auto.conf" 2>/dev/null || true
grep -qE "^port = $PORT\$" "$DATA/postgresql.conf" || { echo "模板里没有 'port = …' 一行，改不了端口"; exit 1; }
grep -qE "^pg_raft\.node_id = $NODE_ID\$" "$DATA/postgresql.conf" || { echo "模板里没有 'pg_raft.node_id = …' 一行"; exit 1; }

echo "[3/5] 启动（日志 $DATA/pg.log）"
"$BIN/pg_ctl" -D "$DATA" -l "$DATA/pg.log" -w -t 60 start >/dev/null

echo "[4/5] 建扩展"
PSQL -p "$PORT" -q <<'SQL'
CREATE EXTENSION IF NOT EXISTS citus;
SET citus.enable_ddl_propagation = off;
CREATE EXTENSION IF NOT EXISTS pg_partdist;
CREATE EXTENSION IF NOT EXISTS pg_raft;
CREATE EXTENSION IF NOT EXISTS pageinspect;
SQL

echo "[5/5] node_map 种子（照抄 master 的，再加上自己）"
PSQL -p "$MASTER_PORT" -c "COPY (SELECT node_id, hostname, port, status, last_heartbeat FROM partdist.node_map) TO STDOUT" \
  | PSQL -p "$PORT" -c "COPY partdist.node_map (node_id, hostname, port, status, last_heartbeat) FROM STDIN"
PSQL -p "$PORT" -c "INSERT INTO partdist.node_map (node_id, hostname, port, status, last_heartbeat)
                    VALUES ($NODE_ID, '$HOST', $PORT, 'active', now()) ON CONFLICT (node_id) DO NOTHING" >/dev/null

echo "新节点 $NODE_ID 就绪：$HOST:$PORT，数据目录 $DATA"
echo "pg_raft.peers = $NEW_PEERS"
echo "下一步（master 上）：SELECT * FROM shardpg.add_node('$HOST', $PORT);"
