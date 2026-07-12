#!/usr/bin/env bash
# [容器内] 编译安装 pg_partdist(含 raft 边界函数)+ pg_raft,
# 四节点(master:5432 + worker1:5433 + worker2:5434 + worker3:5435)
# preload + 纯 C Raft 共识配置。适配 shardpg-3.0 的 raft4 环境。
set -euo pipefail

PG_PARTDIST_SRC="${PG_PARTDIST_SRC:-/work/pg-partdist-src}"
PG_RAFT_SRC="${PG_RAFT_SRC:-/work/pg-raft-src}"
PG_CONFIG="${PG_CONFIG:-/work/pg-install/bin/pg_config}"
PEERS="1@127.0.0.1:5432,2@127.0.0.1:5433,3@127.0.0.1:5434,4@127.0.0.1:5435"
PSQL="/work/pg-install/bin/psql"
NODE_DIRS=(master worker1 worker2 worker3)
NODE_PORTS=(5432 5433 5434 5435)

cleanup_raft_loose_objects() {
  local port=$1
  $PSQL -p "$port" -U postgres -v ON_ERROR_STOP=0 <<'SQL' >/dev/null || true
SET citus.enable_ddl_propagation = off;
DROP EXTENSION IF EXISTS pg_raft CASCADE;
DROP FUNCTION IF EXISTS partdist.pg_raft_apply_committed();
DROP FUNCTION IF EXISTS partdist.pg_raft_append_entries(BIGINT, INTEGER, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TEXT, TEXT);
DROP FUNCTION IF EXISTS partdist.pg_raft_rpc(TEXT);
DROP FUNCTION IF EXISTS partdist.pg_raft_force_probe();
DROP FUNCTION IF EXISTS partdist.pg_raft_apply_payload(TEXT, JSONB);
DROP FUNCTION IF EXISTS partdist.pg_raft_propose_partition_primary(OID, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS partdist.pg_raft_propose_node_status(INTEGER, TEXT);
DROP FUNCTION IF EXISTS partdist.pg_raft_get_cluster_status();
DROP FUNCTION IF EXISTS partdist.pg_raft_get_leader();
DROP FUNCTION IF EXISTS partdist.pg_raft_is_leader();
DROP FUNCTION IF EXISTS partdist.pg_raft_version();
DROP FUNCTION IF EXISTS partdist.raft_propose_partition_primary(OID, INTEGER, INTEGER[]);
DROP FUNCTION IF EXISTS partdist.raft_propose_node_status(INTEGER, TEXT);
DROP TABLE IF EXISTS partdist.raft_snapshot CASCADE;
DROP INDEX IF EXISTS partdist.idx_raft_log_log_index;
DROP TABLE IF EXISTS partdist.raft_log CASCADE;
DROP TABLE IF EXISTS partdist.raft_state CASCADE;
SQL
}

echo "========== 编译安装 pg_partdist(含 raft 边界函数)=========="
cd "$PG_PARTDIST_SRC"
make clean 2>/dev/null || true
make PG_CONFIG="$PG_CONFIG"
make install PG_CONFIG="$PG_CONFIG"

echo "========== 编译安装 pg_raft =========="
cd "$PG_RAFT_SRC"
make clean 2>/dev/null || true
make PG_CONFIG="$PG_CONFIG"
make install PG_CONFIG="$PG_CONFIG"

configure_raft() {
  local dir=$1 node_id=$2
  local conf="/work/pg-cluster-data/${dir}/postgresql.conf"

  # 去掉旧版 pg_raft 单行配置,统一用块追加(后者生效)
  sed -i '/^pg_raft\./d' "$conf" 2>/dev/null || true
  sed -i '/^# pg_raft control plane$/d' "$conf" 2>/dev/null || true
  sed -i '/^# pg_raft 纯 C Raft 共识/d' "$conf" 2>/dev/null || true

  if grep -q "shared_preload_libraries" "$conf"; then
    sed -i "s/shared_preload_libraries = '[^']*'/shared_preload_libraries = 'citus,pg_partdist,pg_raft'/" "$conf"
  else
    echo "shared_preload_libraries = 'citus,pg_partdist,pg_raft'" >> "$conf"
  fi

  cat >> "$conf" <<EOF

# pg_raft 纯 C Raft 共识(setup-raft.sh,四节点)
pg_raft.node_id = ${node_id}
pg_raft.raft_enabled = on
pg_raft.peers = '${PEERS}'
pg_raft.heartbeat_ms = 400
pg_raft.election_timeout_ms = 1500
pg_raft.probe_interval_ms = 3000
pg_raft.probe_fail_threshold = 1
EOF
}

# pg_partdist 扩展若已按旧目录(无边界函数)安装,补齐三个 raft 边界函数。
ensure_boundary_functions() {
  local port=$1
  $PSQL -p "$port" -U postgres -v ON_ERROR_STOP=0 <<'SQL' >/dev/null || true
SET citus.enable_ddl_propagation = off;
CREATE OR REPLACE FUNCTION partdist.get_partition_flush_lsn(partition_id OID)
    RETURNS BIGINT LANGUAGE c STRICT STABLE
    AS 'pg_partdist', 'pg_partdist_get_partition_flush_lsn';
CREATE OR REPLACE FUNCTION partdist.get_follower_applied_part_lsn(partition_id OID)
    RETURNS BIGINT LANGUAGE c STRICT STABLE
    AS 'pg_partdist', 'pg_partdist_get_follower_applied_part_lsn';
CREATE OR REPLACE FUNCTION partdist.partwal_notify_primary_switch(
    partition_id OID, old_primary_node INTEGER,
    new_primary_node INTEGER, switch_orig_lsn PG_LSN)
    RETURNS void LANGUAGE c STRICT VOLATILE
    AS 'pg_partdist', 'pg_partdist_partwal_notify_primary_switch';
SQL
}

install_raft_sql_on_node() {
  local port=$1
  cleanup_raft_loose_objects "$port"
  $PSQL -p "$port" -U postgres -v ON_ERROR_STOP=0 <<'SQL' || true
SET citus.enable_ddl_propagation = off;
CREATE EXTENSION IF NOT EXISTS pg_partdist;
CREATE EXTENSION IF NOT EXISTS pg_raft;
ALTER TABLE partdist.raft_log ADD COLUMN IF NOT EXISTS log_index BIGINT;
UPDATE partdist.raft_log SET log_index = log_id WHERE log_index IS NULL;
ALTER TABLE partdist.raft_log ALTER COLUMN log_index SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_raft_log_log_index ON partdist.raft_log(log_index);
SQL
  ensure_boundary_functions "$port"
}

echo "========== 配置四节点 pg_raft =========="
for i in "${!NODE_DIRS[@]}"; do
  configure_raft "${NODE_DIRS[$i]}" "$((i + 1))"
done

for i in "${!NODE_DIRS[@]}"; do
  /work/pg-install/bin/pg_ctl restart -D "/work/pg-cluster-data/${NODE_DIRS[$i]}" \
    -o "-p ${NODE_PORTS[$i]}" -w
done

sleep 4

echo "========== 各节点安装扩展与 Raft RPC 函数 =========="
for port in "${NODE_PORTS[@]}"; do
  install_raft_sql_on_node "$port"
done

echo "========== 集群状态 =========="
for port in "${NODE_PORTS[@]}"; do
  echo "--- port ${port} ---"
  $PSQL -p "$port" -U postgres -tAc \
    "SELECT leader_node_id, current_term, local_node_id, is_leader, backend FROM partdist.pg_raft_get_cluster_status();" \
    2>/dev/null || echo "(未就绪)"
done

echo "pg_raft(Raft 选举 + 日志复制,四节点)安装完成。"
