#!/usr/bin/env bash
# 各节点已 initdb、已追加 conf/out/node<id>.conf、已启动之后，在压测客户端上执行：
# 装扩展 → node_map 全量拓扑 → Citus 接线 → 等控制组选出 leader → 核对 TSO 取号。
source "$(dirname "$0")/lib.sh"

log "装扩展 + node_map 种子（${#NODE_IDS[@]} 个节点）"
vals=""
for i in "${!NODE_IDS[@]}"; do
  vals+="${vals:+,}(${NODE_IDS[$i]}, '${NODE_HOSTS[$i]}', ${NODE_PORTS[$i]}, 'active', now())"
done
for i in "${!NODE_IDS[@]}"; do
  h=${NODE_HOSTS[$i]}; p=${NODE_PORTS[$i]}
  Q "$h" "$p" -q <<SQL || die "节点 $h:$p 装扩展失败"
CREATE EXTENSION IF NOT EXISTS citus;
SET citus.enable_ddl_propagation = off;
CREATE EXTENSION IF NOT EXISTS pg_partdist;
CREATE EXTENSION IF NOT EXISTS pg_raft;
INSERT INTO partdist.node_map (node_id, hostname, port, status, last_heartbeat)
VALUES ${vals}
ON CONFLICT (node_id) DO UPDATE SET hostname = EXCLUDED.hostname, port = EXCLUDED.port,
  status = EXCLUDED.status, last_heartbeat = now();
SQL
  echo "  $h:$p ok"
done

log "Citus 接线"
QC -q -c "SELECT citus_set_coordinator_host('$COORD_HOST', $COORD_PORT);" >/dev/null || die "citus_set_coordinator_host 失败"
for i in $(worker_indexes); do
  QC -q -c "SELECT citus_add_node('${NODE_HOSTS[$i]}', ${NODE_PORTS[$i]});" >/dev/null \
    || die "citus_add_node ${NODE_HOSTS[$i]}:${NODE_PORTS[$i]} 失败"
  echo "  citus_add_node ${NODE_HOSTS[$i]}:${NODE_PORTS[$i]}"
done

log "等控制组（group 0）选出 leader"
leader=""
for _ in $(seq 1 60); do
  leader=$(QV "$COORD_HOST" "$COORD_PORT" "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" 2>/dev/null)
  [[ -n "$leader" && "$leader" != "0" && "$leader" != "-1" ]] && break
  sleep 2
done
[[ -n "$leader" && "$leader" != "0" && "$leader" != "-1" ]] || die "group0 60 轮内没有 leader"
echo "  group0 leader = node$leader"

log "核对 TSO：每个节点都能取到 start_ts，GlobalSafeTs 非 0"
for i in "${!NODE_IDS[@]}"; do
  ts=$(QV "${NODE_HOSTS[$i]}" "${NODE_PORTS[$i]}" "SELECT partdist.partdist_tso_client_start_ts()" 2>&1)
  [[ "$ts" =~ ^[0-9]+$ ]] || die "${NODE_HOSTS[$i]}:${NODE_PORTS[$i]} 取 TSO 失败：$ts"
done
sts=""
for _ in $(seq 1 30); do
  sts=$(QV "$COORD_HOST" "$COORD_PORT" "SELECT partdist.partdist_global_safe_ts()" 2>/dev/null)
  [[ -n "$sts" && "$sts" != "0" ]] && break
  sleep 1
done
[[ -n "$sts" && "$sts" != "0" ]] || die "GlobalSafeTs 一直是 0（TSO 心跳/租约没起来），分片 vacuum 将无法截断"
log "集群就绪"
