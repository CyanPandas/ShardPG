#!/usr/bin/env bash
# [容器内] 编译安装 pg_partdist(含 raft 边界函数)+ pg_raft,
# 四节点(master:5432 + worker1:5433 + worker2:5434 + worker3:5435)
# preload + 纯 C Raft 共识配置。适配 shardpg-3.0 的 raft4 环境。
set -euo pipefail

PG_PARTDIST_SRC="${PG_PARTDIST_SRC:-/work/pg-partdist-src}"
PG_RAFT_SRC="${PG_RAFT_SRC:-/work/pg-raft-src}"
PG_CONFIG="${PG_CONFIG:-/work/pg-install/bin/pg_config}"
PSQL="/work/pg-install/bin/psql"
# 拓扑自适应（2026-08-03，与 run-raft-tests.sh 同款）：按 pg-cluster-data 下的
# 实际目录探测协调节点目录名与 worker 数，从而同时支持
#   raft4        : master      + worker1-3 (5432-5435)
#   pg_citus_raft: coordinator + worker1-8 (5432-5440)
# 可用 COORD_DIR / N_WORKERS / BASE_PORT 强制。
PG_DATA_ROOT="${PG_DATA_ROOT:-/work/pg-cluster-data}"
BASE_PORT="${BASE_PORT:-5432}"
if [[ -z "${COORD_DIR:-}" ]]; then
  if [[ -d "${PG_DATA_ROOT}/master" ]]; then COORD_DIR=master; else COORD_DIR=coordinator; fi
fi
if [[ -z "${N_WORKERS:-}" ]]; then
  N_WORKERS=$(find "$PG_DATA_ROOT" -maxdepth 1 -type d -name 'worker*' 2>/dev/null | wc -l)
  N_WORKERS=${N_WORKERS//[^0-9]/}
  [[ -n "$N_WORKERS" && "$N_WORKERS" -gt 0 ]] || N_WORKERS=3
fi
NODE_DIRS=("$COORD_DIR")
NODE_PORTS=("$BASE_PORT")
for ((_i = 1; _i <= N_WORKERS; _i++)); do
  NODE_DIRS+=("worker${_i}")
  NODE_PORTS+=($((BASE_PORT + _i)))
done
PEERS=""
for ((_i = 0; _i < ${#NODE_PORTS[@]}; _i++)); do
  [[ -n "$PEERS" ]] && PEERS+=","
  PEERS+="$((_i + 1))@127.0.0.1:${NODE_PORTS[$_i]}"
done

cleanup_raft_loose_objects() {
  local port=$1
  $PSQL -p "$port" -U postgres -v ON_ERROR_STOP=0 <<'SQL' >/dev/null || true
SET citus.enable_ddl_propagation = off;
DROP EXTENSION IF EXISTS pg_raft CASCADE;
DROP FUNCTION IF EXISTS partdist.pg_raft_apply_committed();
DROP FUNCTION IF EXISTS partdist.pg_raft_append_entries(BIGINT, INTEGER, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TEXT, TEXT);
DROP FUNCTION IF EXISTS partdist.pg_raft_append_entries(BIGINT, INTEGER, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TEXT, TEXT, BIGINT);
DROP FUNCTION IF EXISTS partdist.pg_raft_append_entries(BIGINT, INTEGER, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, TEXT, TEXT, BIGINT, BYTEA);
DROP FUNCTION IF EXISTS partdist.pg_raft_data_propose(BIGINT, BIGINT);
-- DTX-2PC 记录格式：follower_append 增加 p_flags、read_record 增加 OUT flags。
--
-- 两处坑（2026-08-03 实测，都会静默失败）：
--   ① OUT 参数变了就必须先 DROP —— CREATE OR REPLACE 改不了返回类型
--      （"cannot change return type of existing function"）；
--   ② 这两个函数是 **pg_partdist 扩展成员**，直接 DROP 会被
--      "cannot drop function ... because extension pg_partdist requires it" 拒绝，
--      必须先 ALTER EXTENSION ... DROP FUNCTION 解除归属。
--      本段整体是 ON_ERROR_STOP=0 且输出重定向的，失败**无声无息**，
--      只有全新库 CREATE EXTENSION 或函数签名比对才看得出来。
DO $mig$
DECLARE
  sig TEXT;
BEGIN
  FOREACH sig IN ARRAY ARRAY[
    'partdist.partwal_read_record(OID, BIGINT)',
    'partdist.partwal_follower_append(OID, BIGINT, PG_LSN, INTEGER, INTEGER, BIGINT, BYTEA)',
    'partdist.partwal_follower_append(OID, BIGINT, PG_LSN, INTEGER, INTEGER, BIGINT, BYTEA, INTEGER)',
    'partdist.partwal_follower_append(OID, PG_LSN, INTEGER, INTEGER, BIGINT, BYTEA)',
    'partdist.partwal_truncate_to(OID, BIGINT)'
  ] LOOP
    BEGIN
      EXECUTE format('ALTER EXTENSION pg_partdist DROP FUNCTION %s', sig);
    EXCEPTION WHEN OTHERS THEN NULL;   -- 不是扩展成员/函数不存在：忽略
    END;
    BEGIN
      EXECUTE format('DROP FUNCTION IF EXISTS %s', sig);
    EXCEPTION WHEN OTHERS THEN
      RAISE WARNING 'setup-raft: 无法 DROP %：%', sig, SQLERRM;
    END;
  END LOOP;
END
$mig$;
DROP FUNCTION IF EXISTS partdist.pg_raft_group_reset();
DROP FUNCTION IF EXISTS partdist.pg_raft_group_reset_internal();
DROP FUNCTION IF EXISTS partdist.pg_raft_group_status();
DROP FUNCTION IF EXISTS partdist.pg_raft_group_propose(BIGINT, TEXT, TEXT);
DROP FUNCTION IF EXISTS partdist.pg_raft_group_create(BIGINT, INTEGER[]);
DROP FUNCTION IF EXISTS partdist.pg_raft_group_drop(BIGINT);
DROP FUNCTION IF EXISTS partdist.pg_raft_group_create_internal(BIGINT, INTEGER[]);
DROP FUNCTION IF EXISTS partdist.pg_raft_group_drop_internal(BIGINT);
DROP FUNCTION IF EXISTS partdist.pg_raft_rpc(TEXT);
DROP FUNCTION IF EXISTS partdist.pg_raft_force_probe();
DROP FUNCTION IF EXISTS partdist.pg_raft_report_data_leader(BIGINT, INTEGER, BIGINT, INTEGER[]);
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
DROP INDEX IF EXISTS partdist.idx_raft_log_group_index;
DROP TABLE IF EXISTS partdist.raft_group CASCADE;
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
# 协调节点(master)：group 0 leader 优先落于此，且不得作为数据组成员
pg_raft.coordinator_node_id = 1
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
-- P2 数据面 Raft 组的 parwal 边界函数(已安装的 pg_partdist 扩展不会重跑安装脚本)
-- DTX-2PC：read_record 增加 OUT flags，follower_append 增加 p_flags —— 复制通道
-- 丢了 flags，DTX/标记记录在副本上会退化成 DATA 记录（DTX_2PC_DESIGN.md §5.5）。
CREATE OR REPLACE FUNCTION partdist.partwal_read_record(
    p_partition_id OID, p_partition_lsn BIGINT,
    OUT orig_lsn PG_LSN, OUT rmid INTEGER, OUT info INTEGER,
    OUT xid BIGINT, OUT flags INTEGER, OUT data BYTEA)
    RETURNS record LANGUAGE c STRICT STABLE
    AS 'pg_partdist', 'pg_partdist_partwal_read_record';
-- 运输层加固：follower 按 leader 指定的 partition_lsn 落盘（多了一个参数）
CREATE OR REPLACE FUNCTION partdist.partwal_follower_append(
    p_partition_id OID, p_partition_lsn BIGINT, p_orig_lsn PG_LSN,
    p_rmid INTEGER, p_info INTEGER, p_xid BIGINT, p_data BYTEA,
    p_flags INTEGER)
    RETURNS BIGINT LANGUAGE c STRICT VOLATILE
    AS 'pg_partdist', 'pg_partdist_partwal_follower_append';
CREATE OR REPLACE FUNCTION partdist.partwal_append_dtx_record(
    p_partition_id OID, p_kind INTEGER, p_dtxid BIGINT, p_coord_gsid BIGINT,
    p_commit_ts BIGINT DEFAULT 0, p_verdict INTEGER DEFAULT 0,
    p_participants BIGINT[] DEFAULT NULL)
    RETURNS BIGINT LANGUAGE c VOLATILE
    AS 'pg_partdist', 'pg_partdist_partwal_append_dtx_record';
CREATE OR REPLACE FUNCTION partdist.partwal_read_dtx_record(
    p_partition_id OID, p_partition_lsn BIGINT,
    OUT kind INTEGER, OUT dtxid BIGINT, OUT coord_gsid BIGINT,
    OUT commit_ts BIGINT, OUT verdict INTEGER, OUT participants BIGINT[])
    RETURNS record LANGUAGE c STRICT STABLE
    AS 'pg_partdist', 'pg_partdist_partwal_read_dtx_record';
-- DTX-2PC 决议索引表（已装 pg_partdist 不会重跑安装脚本，这里补建）
CREATE TABLE IF NOT EXISTS partdist.dtx_decision (
    dtxid         BIGINT      PRIMARY KEY,
    coord_gsid    BIGINT      NOT NULL,
    verdict       SMALLINT    NOT NULL,
    commit_ts     BIGINT      NOT NULL DEFAULT 0,
    participants  BIGINT[]    NOT NULL DEFAULT '{}',
    decided_plsn  BIGINT      NOT NULL,
    acked         BIGINT[]    NOT NULL DEFAULT '{}',
    decided_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION partdist.partwal_truncate_to(
    p_partition_id OID, p_keep_upto_part_lsn BIGINT)
    RETURNS BOOLEAN LANGUAGE c STRICT VOLATILE
    AS 'pg_partdist', 'pg_partdist_partwal_truncate_to';
CREATE OR REPLACE FUNCTION partdist.follower_set_applied_part_lsn(
    p_partition_id OID, p_applied_part_lsn BIGINT)
    RETURNS BOOLEAN LANGUAGE c STRICT VOLATILE
    AS 'pg_partdist', 'pg_partdist_follower_set_applied_part_lsn';
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
-- P1：日志按 Raft 组分命名空间，唯一性是 (group_id, log_index) 而非 log_index。
ALTER TABLE partdist.raft_log ADD COLUMN IF NOT EXISTS group_id BIGINT NOT NULL DEFAULT 0;
DROP INDEX IF EXISTS partdist.idx_raft_log_log_index;
CREATE UNIQUE INDEX IF NOT EXISTS idx_raft_log_group_index
    ON partdist.raft_log(group_id, log_index);
-- 切主重构：数据组自治选举的主副本任期（任期栅栏）。pg_partdist 已安装时不会
-- 重跑安装脚本，这里补列（与 pg_partdist--1.0.sql 中的定义保持一致）。
ALTER TABLE partdist.partition_map ADD COLUMN IF NOT EXISTS primary_term BIGINT NOT NULL DEFAULT 0;
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
