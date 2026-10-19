#!/usr/bin/env bash
# 原版 Citus 接线（make_citus.sh 生成的对照套件用）：各节点建扩展 → 登记协调者 → 加 worker。
# 与 lab 三副本 Citus 对照（tpcc-citus3r/scripts/wire_citus.sh）相同。
source "$(dirname "$0")/lib.sh"
for i in "${!NODE_IDS[@]}"; do
  for _ in $(seq 60); do QV "${NODE_HOSTS[$i]}" "${NODE_PORTS[$i]}" "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
  QV "${NODE_HOSTS[$i]}" "${NODE_PORTS[$i]}" "CREATE EXTENSION IF NOT EXISTS citus" || die "${NODE_HOSTS[$i]}:${NODE_PORTS[$i]} 建扩展失败"
done
QV "$COORD_HOST" "$COORD_PORT" "SELECT citus_set_coordinator_host('$COORD_HOST', $COORD_PORT)" >/dev/null || die "登记协调者失败"
for i in $(worker_indexes); do
  QV "$COORD_HOST" "$COORD_PORT" "SELECT citus_add_node('${NODE_HOSTS[$i]}', ${NODE_PORTS[$i]})" >/dev/null \
    || die "加 worker ${NODE_HOSTS[$i]}:${NODE_PORTS[$i]} 失败"
done
QC -c "SELECT nodeid, nodename, nodeport, noderole, isactive, shouldhaveshards FROM pg_dist_node ORDER BY nodeid"
QC -c "SELECT citus_version()"
log "Citus 集群就绪"
