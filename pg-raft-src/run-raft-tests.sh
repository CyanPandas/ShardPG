#!/usr/bin/env bash
# [宿主机] 四节点 pg_raft 控制面回归(master:5432 + worker1-3:5433-5435)
# 适配 shardpg-3.0 的 raft4 环境;pg_partdist 数据面回归请用 pg-partdist-src/sim/。
set -euo pipefail

CONTAINER="${CONTAINER:-pg-partdist-raft4-container}"
PSQL="docker exec -u postgres ${CONTAINER} /work/pg-install/bin/psql"
PG_CTL="docker exec -u postgres ${CONTAINER} /work/pg-install/bin/pg_ctl"
RAFT_TEST_DIR="/work/pg-raft-src/test/sql"
NODE_PORTS=(5432 5433 5434 5435)
PASS=0
FAIL=0

ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
bad()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
section() { echo ""; echo "========== $* =========="; }

raft_port_for_node() {
  case "$1" in
    1) echo 5432 ;;
    2) echo 5433 ;;
    3) echo 5434 ;;
    4) echo 5435 ;;
    *) return 1 ;;
  esac
}

raft_node_name_for_port() {
  case "$1" in
    5432) echo master ;;
    5433) echo worker1 ;;
    5434) echo worker2 ;;
    5435) echo worker3 ;;
    *) return 1 ;;
  esac
}

raft_node_id_for_port() {
  case "$1" in
    5432) echo 1 ;;
    5433) echo 2 ;;
    5434) echo 3 ;;
    5435) echo 4 ;;
    *) return 1 ;;
  esac
}

node_start() {
  local port=$1
  local dir
  dir=$(raft_node_name_for_port "$port")
  # 已在运行则跳过,避免重复 pg_ctl start 的无害 FATAL 噪声
  if docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_isready -q -h localhost -p "$port" 2>/dev/null; then
    return 0
  fi
  $PG_CTL start -D "/work/pg-cluster-data/${dir}" -o "-p ${port}" -w 2>/dev/null || true
}

node_stop() {
  local port=$1
  local dir
  dir=$(raft_node_name_for_port "$port")
  $PG_CTL stop -D "/work/pg-cluster-data/${dir}" -m fast 2>/dev/null || true
}

raft_current_leader_node() {
  local port node
  for port in "${NODE_PORTS[@]}"; do
    node=$($PSQL -p "$port" -U postgres -tAc \
      "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status();" 2>/dev/null || true)
    if [[ "$node" =~ ^[0-9]+$ ]] && [[ "$node" -ge 1 ]]; then
      echo "$node"
      return 0
    fi
  done
  return 1
}

raft_wait_leader_port() {
  local attempt leader_node leader_port is_leader
  for attempt in $(seq 1 20); do
    leader_node=$(raft_current_leader_node || true)
    leader_port=$(raft_port_for_node "$leader_node" 2>/dev/null || true)
    if [[ -n "${leader_port:-}" ]]; then
      is_leader=$($PSQL -p "$leader_port" -U postgres -tAc \
        "SELECT partdist.pg_raft_is_leader();" 2>/dev/null || true)
      if [[ "$is_leader" == "t" ]]; then
        echo "$leader_port"
        return 0
      fi
    fi
    sleep 0.5
  done
  return 1
}

raft_wait_new_leader_port_excluding() {
  local excluded_port=$1
  local attempt leader_port
  for attempt in $(seq 1 30); do
    leader_port=$(raft_wait_leader_port || true)
    if [[ -n "${leader_port:-}" ]] && [[ "$leader_port" != "$excluded_port" ]]; then
      echo "$leader_port"
      return 0
    fi
    sleep 1
  done
  return 1
}

start_all_nodes() {
  local port
  for port in "${NODE_PORTS[@]}"; do
    node_start "$port"
  done
}

# ------------------------------------------------------------------
section "1. Docker 与容器"
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  ok "容器 ${CONTAINER} 运行中"
else
  bad "容器未运行 → docker start ${CONTAINER}"
  echo "请先启动容器后再测试"; exit 1
fi

# ------------------------------------------------------------------
section "2. 四节点 PostgreSQL 进程"
for port in "${NODE_PORTS[@]}"; do
  node=$(raft_node_name_for_port "$port")
  if $PG_CTL -D "/work/pg-cluster-data/${node}" status &>/dev/null; then
    ok "PostgreSQL ${node} 运行中"
  else
    echo "  [INFO] PostgreSQL ${node} 未运行,自动尝试拉起"
    node_start "$port"
    if $PG_CTL -D "/work/pg-cluster-data/${node}" status &>/dev/null; then
      ok "PostgreSQL ${node} 已自动拉起"
    else
      bad "PostgreSQL ${node} 未运行"
    fi
  fi
done

# ------------------------------------------------------------------
section "3. 四端口连通"
for port in "${NODE_PORTS[@]}"; do
  if $PSQL -p "$port" -U postgres -tAc "SELECT 1" &>/dev/null; then
    ok "端口 ${port} 可连接"
  else
    bad "端口 ${port} 不可连接"
  fi
done

# ------------------------------------------------------------------
section "4. pg_raft 控制面就绪"
raft_ver=$($PSQL -p 5432 -U postgres -tAc "SELECT partdist.pg_raft_version();" 2>/dev/null || echo "")
if [[ "$raft_ver" == *"1.0"* ]]; then
  ok "pg_raft 版本: ${raft_ver}"
else
  echo "  [INFO] pg_raft 缺失,自动执行 setup-raft.sh 恢复测试环境"
  if docker exec -u postgres "$CONTAINER" bash /work/pg-raft-src/setup-raft.sh &>/dev/null; then
    raft_ver=$($PSQL -p 5432 -U postgres -tAc "SELECT partdist.pg_raft_version();" 2>/dev/null || echo "")
    if [[ "$raft_ver" == *"1.0"* ]]; then
      ok "pg_raft 版本: ${raft_ver}(自动恢复)"
    else
      bad "pg_raft 自动恢复后仍未安装"
    fi
  else
    bad "pg_raft 未安装,且自动恢复失败"
  fi
fi

# 边界函数在位(pg_partdist ↔ pg_raft 集成面)
for port in "${NODE_PORTS[@]}"; do
  fn_cnt=$($PSQL -p "$port" -U postgres -tAc \
    "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
     WHERE n.nspname = 'partdist' AND p.proname IN \
     ('get_partition_flush_lsn','get_follower_applied_part_lsn','partwal_notify_primary_switch');" \
    2>/dev/null || echo 0)
  if [[ "$fn_cnt" == "3" ]]; then
    ok "端口 ${port} 三个 raft 边界函数在位"
  else
    bad "端口 ${port} raft 边界函数缺失(${fn_cnt}/3)"
  fi
done

RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  ok "Raft 当前 leader 端口: ${RAFT_LEADER_PORT}"
else
  echo "  [INFO] 尚未确认 Raft leader,自动执行 setup-raft.sh 重新收敛四节点配置"
  if docker exec -u postgres "$CONTAINER" bash /work/pg-raft-src/setup-raft.sh &>/dev/null; then
    RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
  fi
  if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
    ok "Raft 当前 leader 端口: ${RAFT_LEADER_PORT}(自动恢复)"
  else
    bad "未能确认 Raft leader"
    RAFT_LEADER_PORT=5432
  fi
fi

# ------------------------------------------------------------------
section "5. Raft 回归用例(11 项)"

for rf in raft_01_leader_election.sql raft_03_split_brain_guard.sql; do
  if $PSQL -p 5432 -U postgres -v ON_ERROR_STOP=1 -f "${RAFT_TEST_DIR}/${rf}" &>/dev/null; then
    ok "${rf}"
  else
    bad "${rf}"
  fi
done

if $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
     -f "${RAFT_TEST_DIR}/raft_02_failover_partition.sql" &>/dev/null; then
  ok "raft_02_failover_partition.sql"
else
  bad "raft_02_failover_partition.sql"
fi

# raft_04 需先停 worker2(node 3);4 节点停 1 个仍有多数派 3/4
node_stop 5434
sleep 2
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]] && \
   $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
     -f "${RAFT_TEST_DIR}/raft_04_topology_monitor.sql" &>/dev/null; then
  ok "raft_04_topology_monitor.sql"
else
  bad "raft_04_topology_monitor.sql"
fi
node_start 5434

# raft_05: 停掉 leader 之外全部节点(剩 1/4 < 多数派 3),propose 必须失败
start_all_nodes
sleep 2
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  STOPPED_PORTS=()
  for port in "${NODE_PORTS[@]}"; do
    if [[ "$port" != "$RAFT_LEADER_PORT" ]]; then
      node_stop "$port"
      STOPPED_PORTS+=("$port")
    fi
  done
  sleep 2
  if $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
       -f "${RAFT_TEST_DIR}/raft_05_no_majority_reject_commit.sql" &>/dev/null; then
    ok "raft_05_no_majority_reject_commit.sql"
  else
    bad "raft_05_no_majority_reject_commit.sql"
  fi
  for port in "${STOPPED_PORTS[@]}"; do
    node_start "$port"
  done
else
  bad "raft_05_no_majority_reject_commit.sql(无法确认 leader)"
fi

# raft_06: leader 先提交一条日志,再在 follower 上验证陈旧 RequestVote 被拒
start_all_nodes
sleep 2
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  if $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
       -c "SELECT partdist.pg_raft_propose_node_status(98, 'active')" &>/dev/null; then
    FOLLOWER_TEST_PORT=5432
    for port in "${NODE_PORTS[@]}"; do
      if [[ "$port" != "$RAFT_LEADER_PORT" ]]; then
        FOLLOWER_TEST_PORT="$port"
        break
      fi
    done
    if $PSQL -p "$FOLLOWER_TEST_PORT" -U postgres -v ON_ERROR_STOP=1 \
         -f "${RAFT_TEST_DIR}/raft_06_stale_requestvote_rejected.sql" &>/dev/null; then
      ok "raft_06_stale_requestvote_rejected.sql"
    else
      bad "raft_06_stale_requestvote_rejected.sql"
    fi
  else
    bad "raft_06_stale_requestvote_rejected.sql(leader 预热日志失败)"
  fi
else
  bad "raft_06_stale_requestvote_rejected.sql(无法确认 leader)"
fi

# raft_07: 未追平 PartWAL 的副本不能被提升
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]] && \
   $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
     -f "${RAFT_TEST_DIR}/raft_07_uncaught_up_secondary_not_promoted.sql" &>/dev/null; then
  ok "raft_07_uncaught_up_secondary_not_promoted.sql"
else
  bad "raft_07_uncaught_up_secondary_not_promoted.sql"
fi

# raft_08: 停旧 leader → 剩 3/4 仍可选出新 leader → 旧 leader 回归为 follower
start_all_nodes
sleep 2
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  OLD_LEADER_PORT="$RAFT_LEADER_PORT"
  node_stop "$OLD_LEADER_PORT"
  sleep 2

  NEW_LEADER_PORT=$(raft_wait_new_leader_port_excluding "$OLD_LEADER_PORT" || true)
  if [[ -n "${NEW_LEADER_PORT:-}" ]]; then
    node_start "$OLD_LEADER_PORT"

    RAFT_08_OK=0
    for attempt in $(seq 1 30); do
      if $PSQL -p "$OLD_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
           -f "${RAFT_TEST_DIR}/raft_08_old_leader_rejoins_as_follower.sql" &>/dev/null; then
        RAFT_08_OK=1
        break
      fi
      sleep 1
    done

    if [[ "$RAFT_08_OK" == "1" ]]; then
      ok "raft_08_old_leader_rejoins_as_follower.sql"
    else
      bad "raft_08_old_leader_rejoins_as_follower.sql"
    fi
  else
    node_start "$OLD_LEADER_PORT"
    bad "raft_08_old_leader_rejoins_as_follower.sql(旧 leader 停机后未能选出新 leader)"
  fi
else
  bad "raft_08_old_leader_rejoins_as_follower.sql(无法确认初始 leader)"
fi

# raft_09: HardState 崩溃恢复 — follower immediate 停机重启后 term/日志/复制必须连续
start_all_nodes
sleep 2
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  if $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
       -c "SELECT partdist.pg_raft_propose_node_status(99, 'active')" &>/dev/null; then
    sleep 1
    CRASH_PORT=""
    for port in "${NODE_PORTS[@]}"; do
      if [[ "$port" != "$RAFT_LEADER_PORT" ]]; then
        CRASH_PORT="$port"
        break
      fi
    done
    TERM_BEFORE=$($PSQL -p "$CRASH_PORT" -U postgres -tAc \
      "SELECT current_term FROM partdist.pg_raft_get_cluster_status();" 2>/dev/null || echo 0)
    IDX_BEFORE=$($PSQL -p "$CRASH_PORT" -U postgres -tAc \
      "SELECT COALESCE(max(log_index), 0) FROM partdist.raft_log;" 2>/dev/null || echo 0)
    CRASH_DIR=$(raft_node_name_for_port "$CRASH_PORT")

    $PG_CTL stop -D "/work/pg-cluster-data/${CRASH_DIR}" -m immediate 2>/dev/null || true
    sleep 1

    if docker exec -u postgres "$CONTAINER" test -f "/work/pg-cluster-data/${CRASH_DIR}/pg_raft_hardstate"; then
      ok "raft_09 前置: ${CRASH_DIR} 崩溃后 pg_raft_hardstate 文件在盘"
    else
      bad "raft_09 前置: ${CRASH_DIR} 缺少 pg_raft_hardstate 持久化文件"
    fi

    node_start "$CRASH_PORT"
    sleep 2

    RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
    if [[ -n "${RAFT_LEADER_PORT:-}" ]] && \
       $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
         -c "SELECT partdist.pg_raft_propose_node_status(99, 'down')" &>/dev/null; then
      if $PSQL -p "$CRASH_PORT" -U postgres -v ON_ERROR_STOP=1 \
           -v term_before="$TERM_BEFORE" -v idx_before="$IDX_BEFORE" \
           -f "${RAFT_TEST_DIR}/raft_09_hardstate_crash_recovery.sql" &>/dev/null; then
        ok "raft_09_hardstate_crash_recovery.sql"
      else
        bad "raft_09_hardstate_crash_recovery.sql"
      fi
    else
      bad "raft_09_hardstate_crash_recovery.sql(重启后 leader 追加决议失败)"
    fi
  else
    bad "raft_09_hardstate_crash_recovery.sql(崩溃前预热决议失败)"
  fi
else
  bad "raft_09_hardstate_crash_recovery.sql(无法确认 leader)"
fi

# raft_10: 追平副本中必须提升 applied_part_lsn 最大者,switch 点来自真实写入路径
start_all_nodes
sleep 2
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  CAND_PORTS=()
  for port in "${NODE_PORTS[@]}"; do
    if [[ "$port" != "$RAFT_LEADER_PORT" ]]; then
      CAND_PORTS+=("$port")
    fi
  done
  SEED_APPLIED=(2 5 7)   # lo=落后 / mid=恰好追平 / hi=最追平
  SEED_OK=1
  for i in 0 1 2; do
    if ! $PSQL -p "${CAND_PORTS[$i]}" -U postgres -v ON_ERROR_STOP=1 -c \
      "INSERT INTO partdist.follower_partition_map (partition_id, local_relname, applied_part_lsn) \
       VALUES (9108, 'raft10_seed', ${SEED_APPLIED[$i]}) \
       ON CONFLICT (partition_id) DO UPDATE SET applied_part_lsn = EXCLUDED.applied_part_lsn;" &>/dev/null; then
      SEED_OK=0
    fi
  done
  CAND_LO=$(raft_node_id_for_port "${CAND_PORTS[0]}")
  CAND_MID=$(raft_node_id_for_port "${CAND_PORTS[1]}")
  CAND_HI=$(raft_node_id_for_port "${CAND_PORTS[2]}")

  if [[ "$SEED_OK" == "1" ]] && \
     $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
       -v cand_lo="$CAND_LO" -v cand_mid="$CAND_MID" -v cand_hi="$CAND_HI" \
       -f "${RAFT_TEST_DIR}/raft_10_most_caught_up_secondary_promoted.sql" &>/dev/null; then
    ok "raft_10_most_caught_up_secondary_promoted.sql"
  else
    bad "raft_10_most_caught_up_secondary_promoted.sql"
  fi

  for port in "${CAND_PORTS[@]}"; do
    $PSQL -p "$port" -U postgres -c \
      "DELETE FROM partdist.follower_partition_map WHERE partition_id = 9108;" &>/dev/null || true
  done
else
  bad "raft_10_most_caught_up_secondary_promoted.sql(无法确认 leader)"
fi

# raft_11: 旧 leader 停机期间新 leader 提交多条决议,旧 leader 回归后必须追平并保持 follower
start_all_nodes
sleep 2
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  if $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
       -c "SELECT partdist.pg_raft_propose_node_status(99, 'active')" &>/dev/null; then
    OLD_LEADER_PORT="$RAFT_LEADER_PORT"
    node_stop "$OLD_LEADER_PORT"
    sleep 2

    NEW_LEADER_PORT=$(raft_wait_new_leader_port_excluding "$OLD_LEADER_PORT" || true)
    if [[ -n "${NEW_LEADER_PORT:-}" ]]; then
      # 旧 leader 缺席期间提交 3 条决议,最终 node 99 = down
      PROPOSE_OK=1
      for st in down active down; do
        if ! $PSQL -p "$NEW_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
             -c "SELECT partdist.pg_raft_propose_node_status(99, '${st}')" &>/dev/null; then
          PROPOSE_OK=0
        fi
      done
      IDX_TARGET=$($PSQL -p "$NEW_LEADER_PORT" -U postgres -tAc \
        "SELECT COALESCE(max(log_index), 0) FROM partdist.raft_log;" 2>/dev/null || echo 0)

      node_start "$OLD_LEADER_PORT"
      sleep 2

      RAFT_11_OK=0
      if [[ "$PROPOSE_OK" == "1" ]] && [[ "$IDX_TARGET" =~ ^[0-9]+$ ]] && [[ "$IDX_TARGET" -gt 0 ]]; then
        for attempt in $(seq 1 3); do
          if $PSQL -p "$OLD_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
               -v idx_target="$IDX_TARGET" -v expect_status=down \
               -f "${RAFT_TEST_DIR}/raft_11_old_leader_log_catchup.sql" &>/dev/null; then
            RAFT_11_OK=1
            break
          fi
          sleep 2
        done
      fi

      if [[ "$RAFT_11_OK" == "1" ]]; then
        ok "raft_11_old_leader_log_catchup.sql"
      else
        bad "raft_11_old_leader_log_catchup.sql"
      fi
    else
      node_start "$OLD_LEADER_PORT"
      bad "raft_11_old_leader_log_catchup.sql(旧 leader 停机后未能选出新 leader)"
    fi
  else
    bad "raft_11_old_leader_log_catchup.sql(停机前预热决议失败)"
  fi
else
  bad "raft_11_old_leader_log_catchup.sql(无法确认 leader)"
fi

# 清理 Raft 回归制造的临时节点/分区,避免后台 probe 与后续测试并发打架。
RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
    -c "DELETE FROM partdist.node_map WHERE node_id IN (77, 98, 99, 100);" &>/dev/null || true
  $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
    -c "DELETE FROM partdist.partition_map WHERE partition_id IN (9107::oid, 9108::oid);" &>/dev/null || true
  for port in "${NODE_PORTS[@]}"; do
    $PSQL -p "$port" -U postgres -c \
      "DELETE FROM partdist.follower_partition_map WHERE partition_id = 9108;" &>/dev/null || true
  done
  sleep 1
fi

# ------------------------------------------------------------------
section "汇总"
echo ""
echo "通过: ${PASS}  失败: ${FAIL}"
if [[ $FAIL -eq 0 ]]; then
  echo ">>> Raft 四节点回归全部通过 <<<"
  exit 0
else
  echo ">>> 存在 ${FAIL} 项失败,请根据上方 [FAIL] 排查 <<<"
  exit 1
fi
