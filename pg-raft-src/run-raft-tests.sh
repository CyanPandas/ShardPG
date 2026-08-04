#!/usr/bin/env bash
# [宿主机] pg_raft 控制面回归。
#
# 拓扑自适应(2026-08-03)：此前硬编码四节点(master:5432 + worker1-3:5433-5435)，
# 只能在 shardpg-3.0 的 raft4 环境跑；现按容器里 pg-cluster-data/ 的实际目录
# 探测协调节点目录名与 worker 数量，从而同时支持
#   raft4      : master      + worker1-3  (5432-5435)
#   pg_citus_raft: coordinator + worker1-8  (5432-5440)
# 也可用环境变量强制：CONTAINER / COORD_DIR / N_WORKERS / BASE_PORT。
# 节点 id ↔ 端口的约定不变：node N ↔ BASE_PORT + N - 1，node 1 = 协调节点。
#
# quorum 相关的编排本身与节点数无关（raft_05 停掉"除 leader 外全部节点"；
# 数据组用例用显式 3 成员组），所以只需要把映射函数改成算术即可。
#
# pg_partdist 数据面回归请用 pg-partdist-src/sim/。
set -euo pipefail

CONTAINER="${CONTAINER:-pg-partdist-raft4-container}"
PSQL="docker exec -u postgres ${CONTAINER} /work/pg-install/bin/psql"
PG_CTL="docker exec -u postgres ${CONTAINER} /work/pg-install/bin/pg_ctl"
RAFT_TEST_DIR="/work/pg-raft-src/test/sql"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_PORT="${BASE_PORT:-5432}"

# 协调节点的数据目录名：raft4 叫 master，pg_citus_raft 叫 coordinator
if [[ -z "${COORD_DIR:-}" ]]; then
  if docker exec "$CONTAINER" test -d /work/pg-cluster-data/master 2>/dev/null; then
    COORD_DIR=master
  else
    COORD_DIR=coordinator
  fi
fi

# worker 数量：数 pg-cluster-data 下的 worker 目录（排除同名 .log 文件）
if [[ -z "${N_WORKERS:-}" ]]; then
  N_WORKERS=$(docker exec "$CONTAINER" bash -lc \
    "find /work/pg-cluster-data -maxdepth 1 -type d -name 'worker*' | wc -l" 2>/dev/null || echo 3)
  N_WORKERS=${N_WORKERS//[^0-9]/}
  [[ -n "$N_WORKERS" && "$N_WORKERS" -gt 0 ]] || N_WORKERS=3
fi

N_NODES=$((N_WORKERS + 1))
NODE_PORTS=()
for ((_i = 0; _i < N_NODES; _i++)); do
  NODE_PORTS+=($((BASE_PORT + _i)))
done

echo "拓扑：容器=${CONTAINER} 协调节点目录=${COORD_DIR} 节点数=${N_NODES}(1c+${N_WORKERS}w) 端口=${NODE_PORTS[0]}-${NODE_PORTS[$((N_NODES - 1))]}"

PASS=0
FAIL=0

ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
bad()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
section() { echo ""; echo "========== $* =========="; }

raft_port_for_node() {
  local n=$1
  (( n >= 1 && n <= N_NODES )) || return 1
  echo $((BASE_PORT + n - 1))
}

raft_node_name_for_port() {
  local n=$(( $1 - BASE_PORT + 1 ))
  (( n >= 1 && n <= N_NODES )) || return 1
  if (( n == 1 )); then echo "$COORD_DIR"; else echo "worker$((n - 1))"; fi
}

raft_node_id_for_port() {
  local n=$(( $1 - BASE_PORT + 1 ))
  (( n >= 1 && n <= N_NODES )) || return 1
  echo "$n"
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
# 有界重试（同 raft_15 先例，2026-08-04）：这是功能接线测试且 SQL 幂等（开头
# 重置 node_map/partition_map），propose 单发撞上 2 核 9 节点的负载抖动会
# 偶发 "raft propose failed"（与 raft_09 早前一次同类的环境瞬态，非时序判据）。
node_stop 5434
sleep 2
RAFT_04_OK=0
for RAFT_04_TRY in 1 2 3; do
  RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
  if [[ -n "${RAFT_LEADER_PORT:-}" ]] && \
     $PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
       -f "${RAFT_TEST_DIR}/raft_04_topology_monitor.sql" &>/dev/null; then
    RAFT_04_OK=1
    break
  fi
  sleep 5
done
if [[ "$RAFT_04_OK" == "1" ]]; then
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
      "SELECT COALESCE(max(log_index), 0) FROM partdist.raft_log WHERE group_id = 0;" 2>/dev/null || echo 0)
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

  # 失败时保留 SQL 错误尾行——此前 &>/dev/null 吞掉一切，偶发失败无从诊断
  RAFT_10_OUT=""
  if [[ "$SEED_OK" == "1" ]] && \
     RAFT_10_OUT=$($PSQL -p "$RAFT_LEADER_PORT" -U postgres -v ON_ERROR_STOP=1 \
       -v cand_lo="$CAND_LO" -v cand_mid="$CAND_MID" -v cand_hi="$CAND_HI" \
       -f "${RAFT_TEST_DIR}/raft_10_most_caught_up_secondary_promoted.sql" 2>&1); then
    ok "raft_10_most_caught_up_secondary_promoted.sql"
  else
    bad "raft_10_most_caught_up_secondary_promoted.sql($(echo "$RAFT_10_OUT" | grep -E "ERROR|EXCEPTION" | tail -1))"
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
        "SELECT COALESCE(max(log_index), 0) FROM partdist.raft_log WHERE group_id = 0;" 2>/dev/null || echo 0)

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

# raft_12: 分区级 Raft 组隔离性(P1)。在两个不同节点各建一个数据组,验证两组
# 各自独立选举、日志按 group_id 分命名空间、且控制面 group 0 不受影响。
# 每轮用一对**全新的** group id:组的 drop 只作用于本节点,对端会把旧组连同它
# 记住的 term 传播回来,复用同一个 id 重建会得到起始 term 落后于对端的新组,
# 选举被反复压制(表现为偶发的选不出 leader / 复制不出去)。
# 不依赖任何节点在线(此刻可能正处于 raft_11 的停机窗口),否则回落到固定 id
# 就又变成"复用旧 group id",踩上面说的 term 落后问题。
RAFT_12_BASE=$(( 9000000 + ($(date +%s) % 900000) * 2 ))
RAFT_12_GID_A=$RAFT_12_BASE
RAFT_12_GID_B=$((RAFT_12_BASE + 1))

raft12_cleanup_groups() {
  for port in "${NODE_PORTS[@]}"; do
    $PSQL -p "$port" -U postgres -c \
      "SELECT partdist.pg_raft_group_reset();" &>/dev/null || true
  done
}

# 返回某个组当前 leader 所在节点的端口
raft12_leader_port_for_group() {
  local gid=$1 port st
  for port in "${NODE_PORTS[@]}"; do
    st=$($PSQL -p "$port" -U postgres -tAc \
      "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = ${gid};" \
      2>/dev/null || echo "")
    if [[ "$st" == "leader" ]]; then
      echo "$port"
      return 0
    fi
  done
  return 1
}

start_all_nodes
sleep 2
# 组是 shmem 状态,且会经 RPC 在对端自动重建;先在全节点清一遍残留
raft12_cleanup_groups

RAFT_LEADER_PORT=$(raft_wait_leader_port || true)
if [[ -n "${RAFT_LEADER_PORT:-}" ]]; then
  # 在两个不同的非控制面节点各建一个组,让它们各自成为该组的 leader
  RAFT_12_NODE_A=5433
  RAFT_12_NODE_B=5434
  # 成员集必须**显式**给出（2026-08-03，DTX_2PC_DESIGN.md §9.2）：数据组的空
  # 成员集不再表示"全体节点"而是"未知"，未知即不竞选，建组入口也会直接报错。
  # 这两个组是**合成 group id**（9000000+），partition_map 里没有对应登记，
  # 因此导不出成员集，只能显式指定。取三个 worker（协调节点不作数据副本）。
  RAFT_12_MEMBERS="ARRAY[2,3,4]::int[]"
  $PSQL -p "$RAFT_12_NODE_A" -U postgres -c \
    "SELECT partdist.pg_raft_group_create(${RAFT_12_GID_A}, ${RAFT_12_MEMBERS});" &>/dev/null || true
  $PSQL -p "$RAFT_12_NODE_B" -U postgres -c \
    "SELECT partdist.pg_raft_group_create(${RAFT_12_GID_B}, ${RAFT_12_MEMBERS});" &>/dev/null || true

  # 等两组各自选出 leader(组信息经 RV/AE 自动传播到其余节点)
  RAFT_12_LEADER_A=""
  RAFT_12_LEADER_B=""
  for attempt in $(seq 1 15); do
    RAFT_12_LEADER_A=$(raft12_leader_port_for_group "$RAFT_12_GID_A" || true)
    RAFT_12_LEADER_B=$(raft12_leader_port_for_group "$RAFT_12_GID_B" || true)
    [[ -n "$RAFT_12_LEADER_A" && -n "$RAFT_12_LEADER_B" ]] && break
    sleep 1
  done

  if [[ -n "$RAFT_12_LEADER_A" && -n "$RAFT_12_LEADER_B" ]]; then
    # 只往 A 组写日志:B 组必须保持空,证明两组日志互不串扰
    RAFT_12_PROPOSE_OK=1
    for v in 1 2 3; do
      if ! $PSQL -p "$RAFT_12_LEADER_A" -U postgres -v ON_ERROR_STOP=1 -tAc \
           "SELECT partdist.pg_raft_group_propose(${RAFT_12_GID_A}, 'OP_TEST', '{\"v\":${v}}');" \
           &>/dev/null; then
        RAFT_12_PROPOSE_OK=0
      fi
    done
    # 非 leader 节点提交必须被拒(返回 0)。
    # 必须挑一个**本组成员**：挑到组外节点（例如协调节点）时该组在那里根本不存在，
    # propose 同样返回 0，用例就变成"对的结果、错的原因"——测不到"非 leader 被拒"。
    RAFT_12_NONLEADER_PORT=""
    for port in 5433 5434 5435; do
      if [[ "$port" != "$RAFT_12_LEADER_A" ]]; then
        RAFT_12_NONLEADER_PORT="$port"
        break
      fi
    done
    RAFT_12_NONLEADER_RC=$($PSQL -p "$RAFT_12_NONLEADER_PORT" -U postgres -tAc \
      "SELECT partdist.pg_raft_group_propose(${RAFT_12_GID_A}, 'OP_TEST', '{\"v\":99}');" \
      2>/dev/null || echo -1)
    sleep 2

    # 断言必须在**数据组成员**节点上跑，不能在控制面 leader 上跑：控制面 leader
    # 常态是协调节点（master），而 master 永不作数据副本、也就永远看不到数据组
    # ——旧写法能过是因为空成员集会让组经 hearsay 撒到全集群，那正是 §9.2 修掉的
    # 不安全行为。node A 是 gid_a 的建组节点、也是 gid_b 的成员，两组都可见。
    RAFT_12_OBSERVER="$RAFT_12_NODE_A"
    RAFT_12_SQL_OUT=""
    if [[ "$RAFT_12_PROPOSE_OK" == "1" ]] && [[ "$RAFT_12_NONLEADER_RC" == "0" ]] && \
       RAFT_12_SQL_OUT=$($PSQL -p "$RAFT_12_OBSERVER" -U postgres -v ON_ERROR_STOP=1 \
         -v gid_a="$RAFT_12_GID_A" -v gid_b="$RAFT_12_GID_B" \
         -f "${RAFT_TEST_DIR}/raft_12_multi_group_isolation.sql" 2>&1); then
      ok "raft_12_multi_group_isolation.sql"
    else
      bad "raft_12_multi_group_isolation.sql(propose_ok=${RAFT_12_PROPOSE_OK} nonleader_rc=${RAFT_12_NONLEADER_RC} $(echo "$RAFT_12_SQL_OUT" | grep -E "ERROR|EXCEPTION" | tail -1))"
    fi
  else
    bad "raft_12_multi_group_isolation.sql(数据组未能各自选出 leader)"
  fi

  raft12_cleanup_groups
else
  bad "raft_12_multi_group_isolation.sql(无法确认控制面 leader)"
fi

# raft_13: 数据面 Raft 组复制 + 平凡 apply(P2)。以一个 Citus reference 表的分片
# 为例(同一 shardid 复制在三个 worker 上,天然就是一个分区组的成员集),把 leader
# pg_parwal 里的真实记录当作 Raft entry 复制,验证:多数派提交、字节逐字节落到
# follower 自己的 pg_parwal、applied_part_lsn 由真实 C 写入方推进、失去多数派时
# 写入必须失败。
RAFT_13_TABLE=raft13_demo
RAFT_13_MEMBER_PORTS=(5433 5434 5435)
RAFT_13_MEMBER_IDS="ARRAY[2,3,4]"

raft13_cleanup() {
  local port
  # 用 reset 而非 drop:drop 只作用本节点,对端会把组连同旧 term 传回来,
  # 残留的数据组会一直参与 tick,拖慢后续依赖时序的控制面用例。
  for port in "${NODE_PORTS[@]}"; do
    $PSQL -p "$port" -U postgres -c \
      "SELECT partdist.pg_raft_group_reset();" &>/dev/null || true
  done
  # 数据组 leader 当选时会向控制面登记(切主重构),清掉本用例夹具的登记行
  if [[ -n "${RAFT_13_GID:-}" ]]; then
    for port in "${NODE_PORTS[@]}"; do
      $PSQL -p "$port" -U postgres -c \
        "DELETE FROM partdist.partition_map WHERE partition_id = ${RAFT_13_GID}::oid;" &>/dev/null || true
    done
  fi
  $PSQL -p 5432 -U postgres -c \
    "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${RAFT_13_TABLE};" &>/dev/null || true
}

start_all_nodes
sleep 2

RAFT_13_OK=0
RAFT_13_WHY="setup"
if $PSQL -p 5432 -U postgres -v ON_ERROR_STOP=1 -c \
     "SET citus.enable_ddl_propagation=on;
      DROP TABLE IF EXISTS ${RAFT_13_TABLE};
      CREATE TABLE ${RAFT_13_TABLE}(id int primary key, v text);
      SELECT create_reference_table('${RAFT_13_TABLE}');
      INSERT INTO ${RAFT_13_TABLE} SELECT g, repeat('x', 50) FROM generate_series(1, 20) g;" &>/dev/null; then

  for port in "${NODE_PORTS[@]}"; do
    $PSQL -p "$port" -U postgres -c "SELECT partdist.rebuild_shard_identity();" &>/dev/null || true
  done

  RAFT_13_GID=$($PSQL -p 5432 -U postgres -tAc \
    "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='${RAFT_13_TABLE}'::regclass;" 2>/dev/null || echo "")

  if [[ -n "$RAFT_13_GID" ]]; then
    # 组成员 = 承载该分片副本的三个 worker
    for port in "${RAFT_13_MEMBER_PORTS[@]}"; do
      $PSQL -p "$port" -U postgres -c \
        "SELECT partdist.pg_raft_group_create(${RAFT_13_GID}, ${RAFT_13_MEMBER_IDS});" &>/dev/null || true
    done

    RAFT_13_LEADER=""
    for attempt in $(seq 1 15); do
      for port in "${RAFT_13_MEMBER_PORTS[@]}"; do
        st=$($PSQL -p "$port" -U postgres -tAc \
          "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = ${RAFT_13_GID};" 2>/dev/null || echo "")
        [[ "$st" == "leader" ]] && RAFT_13_LEADER="$port" && break
      done
      [[ -n "$RAFT_13_LEADER" ]] && break
      sleep 1
    done

    if [[ -n "$RAFT_13_LEADER" ]]; then
      # 取一个 follower,记录复制前的 parwal 进度
      RAFT_13_FOLLOWER=""
      for port in "${RAFT_13_MEMBER_PORTS[@]}"; do
        [[ "$port" != "$RAFT_13_LEADER" ]] && RAFT_13_FOLLOWER="$port" && break
      done
      # 夹具校正：reference 表在**每个**节点都是本地主写，follower 的
      # pg_parwal/<oid>/ 里已有它自己 demux 产出的 plsn 1..N。而真实架构下
      # 一个节点对某分区要么是 primary 要么是 secondary，secondary 不会本地
      # 产出该分区的 WAL。这里把 follower 该分片的本地记录清空，模拟"纯
      # secondary"，否则 leader 的 plsn=1 会撞上 follower 自己的 plsn=1。
      # （顺带验证新增的 partwal_truncate_to。）
      $PSQL -p "$RAFT_13_FOLLOWER" -U postgres -tAc \
        "SELECT partdist.partwal_truncate_to(partdist.local_partition_for_shard(${RAFT_13_GID}), 0);" &>/dev/null || true
      RAFT_13_BEFORE=$($PSQL -p "$RAFT_13_FOLLOWER" -U postgres -tAc \
        "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(${RAFT_13_GID}));" 2>/dev/null || echo 0)
      RAFT_13_MD5=$($PSQL -p "$RAFT_13_LEADER" -U postgres -tAc \
        "SELECT md5(data) FROM partdist.partwal_read_record(partdist.local_partition_for_shard(${RAFT_13_GID}), 1);" 2>/dev/null || echo "")
      RAFT_13_LEN=$($PSQL -p "$RAFT_13_LEADER" -U postgres -tAc \
        "SELECT length(data) FROM partdist.partwal_read_record(partdist.local_partition_for_shard(${RAFT_13_GID}), 1);" 2>/dev/null || echo 0)

      # 把 leader pg_parwal 的第 1 条记录作为 Raft entry 提交到该组
      RAFT_13_IDX=$($PSQL -p "$RAFT_13_LEADER" -U postgres -tAc \
        "SELECT partdist.pg_raft_data_propose(${RAFT_13_GID}, 1);" 2>/dev/null || echo 0)
      sleep 2

      if [[ "$RAFT_13_IDX" =~ ^[0-9]+$ ]] && [[ "$RAFT_13_IDX" -gt 0 ]] && [[ -n "$RAFT_13_MD5" ]]; then
        if $PSQL -p "$RAFT_13_FOLLOWER" -U postgres -v ON_ERROR_STOP=1 \
             -v gid="$RAFT_13_GID" -v before_flush="$RAFT_13_BEFORE" \
             -v leader_md5="$RAFT_13_MD5" -v leader_len="$RAFT_13_LEN" \
             -f "${RAFT_TEST_DIR}/raft_13_data_group_replication.sql" &>/dev/null; then
          # 失去多数派(3 成员停掉 2)后,数据写入必须失败
          RAFT_13_STOPPED=()
          for port in "${RAFT_13_MEMBER_PORTS[@]}"; do
            if [[ "$port" != "$RAFT_13_LEADER" ]]; then
              node_stop "$port"
              RAFT_13_STOPPED+=("$port")
            fi
          done
          sleep 2
          RAFT_13_NOQUORUM=$($PSQL -p "$RAFT_13_LEADER" -U postgres -tAc \
            "SELECT partdist.pg_raft_data_propose(${RAFT_13_GID}, 1);" 2>/dev/null || echo -1)
          for port in "${RAFT_13_STOPPED[@]}"; do
            node_start "$port"
          done
          sleep 2

          if [[ "$RAFT_13_NOQUORUM" == "0" ]]; then
            RAFT_13_OK=1
          else
            RAFT_13_WHY="失去多数派时 data_propose 返回 ${RAFT_13_NOQUORUM},应为 0"
          fi
        else
          RAFT_13_WHY="follower 断言失败"
        fi
      else
        RAFT_13_WHY="data_propose 返回 ${RAFT_13_IDX}"
      fi
    else
      RAFT_13_WHY="数据组未选出 leader"
    fi
  else
    RAFT_13_WHY="拿不到 reference 表 shardid"
  fi
fi

if [[ "$RAFT_13_OK" == "1" ]]; then
  ok "raft_13_data_group_replication.sql"
else
  bad "raft_13_data_group_replication.sql(${RAFT_13_WHY})"
fi
raft13_cleanup

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

# raft_14 + raft_15: 切主重构验收。真实哈希分布分片,(a) 形态从副本(同构壳表+
# shard_identity),组成员=全部 worker {2,3,4}(master 不作数据副本)。
# raft_14 验复制半程:经 master 路由写入,逐条 propose,follower parwal 逐字节
# 一致、连续无洞、不回放;数据组 leader 初次当选即向控制面登记(partition_map +
# pg_dist_placement 每节点各一份)。
# raft_15 验切主全程:停掉主副本节点 → 组内自治选举 → 新 leader 上报 master →
# group 0 登记(任期栅栏)→ 每节点路由层落新主;旧主重启后以 follower 归队。
RAFT_14_TABLE=raft14_demo
RAFT_14_MEMBER_IDS="ARRAY[2,3,4]"
RAFT_14_GID=""

raft14_cleanup() {
  local port
  for port in "${NODE_PORTS[@]}"; do
    $PSQL -p "$port" -U postgres -c \
      "SELECT partdist.pg_raft_group_reset();" &>/dev/null || true
  done
  if [[ -n "$RAFT_14_GID" ]]; then
    for port in 5433 5434 5435; do
      $PSQL -p "$port" -U postgres -c \
        "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${RAFT_14_TABLE}_${RAFT_14_GID};" &>/dev/null || true
    done
    for port in "${NODE_PORTS[@]}"; do
      $PSQL -p "$port" -U postgres -c \
        "DELETE FROM partdist.partition_map WHERE partition_id = ${RAFT_14_GID}::oid;
         DELETE FROM partdist.follower_partition_map WHERE global_shard_id = ${RAFT_14_GID};" &>/dev/null || true
    done
  fi
  $PSQL -p 5432 -U postgres -c \
    "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${RAFT_14_TABLE};" &>/dev/null || true
}

start_all_nodes
sleep 2

RAFT_14_OK=0
RAFT_14_WHY="setup"
RAFT_14_LEADER=""
RAFT_14_PRIMARY_ID=""
RAFT_14_TERM=""
RAFT_14_FOLLOWER_PORTS=()

if $PSQL -p 5432 -U postgres -v ON_ERROR_STOP=1 -c \
     "SET citus.enable_ddl_propagation=on;
      DROP TABLE IF EXISTS ${RAFT_14_TABLE};
      SET citus.shard_count = 1;
      SET citus.shard_replication_factor = 1;
      CREATE TABLE ${RAFT_14_TABLE}(id int primary key, v text);
      SELECT create_distributed_table('${RAFT_14_TABLE}', 'id');" &>/dev/null; then

  RAFT_14_GID=$($PSQL -p 5432 -U postgres -tAc \
    "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='${RAFT_14_TABLE}'::regclass;" 2>/dev/null || echo "")
  RAFT_14_PRIMARY_PORT=$($PSQL -p 5432 -U postgres -tAc \
    "SELECT n.nodeport FROM pg_dist_placement p
       JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
      WHERE p.shardid = ${RAFT_14_GID:-0};" 2>/dev/null || echo "")

  if [[ -n "$RAFT_14_GID" && -n "$RAFT_14_PRIMARY_PORT" ]]; then
    RAFT_14_PRIMARY_ID=$(( RAFT_14_PRIMARY_PORT - 5431 ))

    # (a) 形态:其余 worker 建同构壳表(表名=分片表名),rebuild 注册 shard_identity
    for port in 5433 5434 5435; do
      if [[ "$port" != "$RAFT_14_PRIMARY_PORT" ]]; then
        $PSQL -p "$port" -U postgres -v ON_ERROR_STOP=1 -c \
          "SET citus.enable_ddl_propagation=off;
           CREATE TABLE IF NOT EXISTS ${RAFT_14_TABLE}_${RAFT_14_GID}
             (LIKE ${RAFT_14_TABLE} INCLUDING ALL);" &>/dev/null \
          && RAFT_14_FOLLOWER_PORTS+=("$port")
      fi
      $PSQL -p "$port" -U postgres -c "SELECT partdist.rebuild_shard_identity();" &>/dev/null || true
    done

    # 只在 placement 节点先建组:它先发起选举,leader 落在有数据的节点;
    # 其他成员靠 hearsay 自动建组,选出 leader 后再补 group_create 固化成员集
    $PSQL -p "$RAFT_14_PRIMARY_PORT" -U postgres -c \
      "SELECT partdist.pg_raft_group_create(${RAFT_14_GID}, ${RAFT_14_MEMBER_IDS});" &>/dev/null || true
    for attempt in $(seq 1 15); do
      st=$($PSQL -p "$RAFT_14_PRIMARY_PORT" -U postgres -tAc \
        "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = ${RAFT_14_GID};" 2>/dev/null || echo "")
      [[ "$st" == "leader" ]] && RAFT_14_LEADER="$RAFT_14_PRIMARY_PORT" && break
      sleep 1
    done
    for port in "${RAFT_14_FOLLOWER_PORTS[@]}"; do
      $PSQL -p "$port" -U postgres -c \
        "SELECT partdist.pg_raft_group_create(${RAFT_14_GID}, ${RAFT_14_MEMBER_IDS});" &>/dev/null || true
    done

    if [[ -n "$RAFT_14_LEADER" ]]; then
      # 经 master 路由写入(顺带验证 Citus 平面路由);hook 在 placement worker 捕获
      $PSQL -p 5432 -U postgres -v ON_ERROR_STOP=1 -c \
        "INSERT INTO ${RAFT_14_TABLE} SELECT g, 'r' || g FROM generate_series(1, 5) g;" &>/dev/null
      sleep 1
      RAFT_14_NREC=$($PSQL -p "$RAFT_14_LEADER" -U postgres -tAc \
        "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(${RAFT_14_GID}));" 2>/dev/null || echo 0)

      if [[ "$RAFT_14_NREC" =~ ^[0-9]+$ ]] && [[ "$RAFT_14_NREC" -gt 0 ]]; then
        # 一条 record 一次备份:逐条 propose,保序
        RAFT_14_PROPOSED=1
        for lsn in $(seq 1 "$RAFT_14_NREC"); do
          idx=$($PSQL -p "$RAFT_14_LEADER" -U postgres -tAc \
            "SELECT partdist.pg_raft_data_propose(${RAFT_14_GID}, ${lsn});" 2>/dev/null || echo 0)
          if ! [[ "$idx" =~ ^[0-9]+$ ]] || [[ "$idx" -le 0 ]]; then
            RAFT_14_PROPOSED=0
            RAFT_14_WHY="record ${lsn} propose 返回 ${idx}"
            break
          fi
        done
        sleep 2

        RAFT_14_MD5=$($PSQL -p "$RAFT_14_LEADER" -U postgres -tAc \
          "SELECT md5(string_agg(sub.h, ',' ORDER BY sub.plsn)) FROM (
             SELECT g AS plsn, md5(r.data) AS h
             FROM generate_series(1, ${RAFT_14_NREC}) g,
                  LATERAL partdist.partwal_read_record(partdist.local_partition_for_shard(${RAFT_14_GID}), g) r
           ) sub;" 2>/dev/null || echo "")

        if [[ "$RAFT_14_PROPOSED" == "1" && -n "$RAFT_14_MD5" ]]; then
          # 初次登记(become-leader 上报)应已随 group 0 落到 master
          RAFT_14_TERM=""
          for attempt in $(seq 1 10); do
            RAFT_14_TERM=$($PSQL -p 5432 -U postgres -tAc \
              "SELECT primary_term FROM partdist.partition_map
                WHERE partition_id = ${RAFT_14_GID}::oid AND primary_term > 0
                  AND primary_node = ${RAFT_14_PRIMARY_ID};" 2>/dev/null || echo "")
            [[ -n "$RAFT_14_TERM" ]] && break
            sleep 1
          done

          if [[ -n "$RAFT_14_TERM" ]]; then
            RAFT_14_OK=1
            for port in "${RAFT_14_FOLLOWER_PORTS[@]}"; do
              # 保留 psql 的错误正文：只报"断言失败"不可诊断（2026-08-04 教训）
              if ! RAFT_14_ERR=$($PSQL -p "$port" -U postgres -v ON_ERROR_STOP=1 \
                     -v gid="$RAFT_14_GID" -v nrec="$RAFT_14_NREC" \
                     -v leader_md5="$RAFT_14_MD5" -v primary_id="$RAFT_14_PRIMARY_ID" \
                     -v shard_table="${RAFT_14_TABLE}_${RAFT_14_GID}" \
                     -f "${RAFT_TEST_DIR}/raft_14_hash_shard_secondary_backup.sql" 2>&1); then
                RAFT_14_OK=0
                RAFT_14_WHY="follower ${port} 断言失败: $(echo "$RAFT_14_ERR" | grep -m1 -i 'ERROR\|raft_14:' | head -c 300)"
                break
              fi
            done
            # master 不作数据副本:全程不得有该分片的身份/parwal
            if [[ "$RAFT_14_OK" == "1" ]]; then
              RAFT_14_MASTER_ID=$($PSQL -p 5432 -U postgres -tAc \
                "SELECT COALESCE(partdist.local_partition_for_shard(${RAFT_14_GID})::text, 'none');" 2>/dev/null || echo "err")
              if [[ "$RAFT_14_MASTER_ID" != "none" ]]; then
                RAFT_14_OK=0
                RAFT_14_WHY="master 意外持有分片身份(${RAFT_14_MASTER_ID})"
              fi
            fi
          else
            RAFT_14_WHY="初次登记未到达 master(partition_map 无 term>0 行)"
          fi
        elif [[ "$RAFT_14_PROPOSED" == "1" ]]; then
          RAFT_14_WHY="取 leader 全量指纹失败"
        fi
      else
        RAFT_14_WHY="leader parwal 无记录(flush=${RAFT_14_NREC})"
      fi
    else
      RAFT_14_WHY="数据组 leader 未落在 placement 节点"
    fi
  else
    RAFT_14_WHY="拿不到 shardid/placement(gid=${RAFT_14_GID:-空})"
  fi
fi

if [[ "$RAFT_14_OK" == "1" ]]; then
  ok "raft_14_hash_shard_secondary_backup.sql"
else
  bad "raft_14_hash_shard_secondary_backup.sql(${RAFT_14_WHY})"
fi

# raft_15: 停掉主副本节点,验证 自治选举→上报→登记→落路由层 全链路
RAFT_15_OK=0
RAFT_15_WHY="依赖 raft_14 的组与登记"
if [[ "$RAFT_14_OK" == "1" ]]; then
  node_stop "$RAFT_14_PRIMARY_PORT"

  # 等新主登记:选举超时(1.5~3s) + 上报 tick + group 0 复制,给足余量
  RAFT_15_NEWP=""
  for attempt in $(seq 1 30); do
    sleep 1
    RAFT_15_NEWP=$($PSQL -p 5432 -U postgres -tAc \
      "SELECT primary_node FROM partdist.partition_map
        WHERE partition_id = ${RAFT_14_GID}::oid
          AND primary_term > ${RAFT_14_TERM}
          AND primary_node <> ${RAFT_14_PRIMARY_ID};" 2>/dev/null || echo "")
    [[ -n "$RAFT_15_NEWP" ]] && break
  done

  if [[ -n "$RAFT_15_NEWP" ]]; then
    RAFT_15_OK=1
    # master + 两个存活 worker 各自断言(partition_map / pg_dist_placement 每节点一份)。
    #
    # 按节点带有界重试:控制面语义是"多数派提交 + 全员**最终** apply"(§13.2)——
    # master 上出现登记只说明多数派已提交,单个 follower 的 apply 由它自己的
    # tick 推进,可以滞后。9 节点 group0 多数派 5/9,某 follower 不在提交时的
    # ack 集里时滞后窗口明显大于 4 节点(3/4),一次性断言在 9 节点环境高概率
    # 误报(2026-08-03 实测两次"节点 5434 断言失败"皆因此)。断言内容不放宽,
    # 只允许每个节点在超时窗口内追平;窗口耗尽仍不满足才是真失败。
    for port in 5432 "${RAFT_14_FOLLOWER_PORTS[@]}"; do
      RAFT_15_NODE_OK=0
      for attempt in $(seq 1 20); do
        if $PSQL -p "$port" -U postgres -v ON_ERROR_STOP=1 \
             -v gid="$RAFT_14_GID" -v old_primary_id="$RAFT_14_PRIMARY_ID" \
             -v old_term="$RAFT_14_TERM" \
             -f "${RAFT_TEST_DIR}/raft_15_self_election_failover.sql" &>/dev/null; then
          RAFT_15_NODE_OK=1
          break
        fi
        sleep 1
      done
      if [[ "$RAFT_15_NODE_OK" != "1" ]]; then
        RAFT_15_OK=0
        RAFT_15_WHY="节点 ${port} 断言失败(等待 20s 追平后仍不满足)"
        break
      fi
    done

    # 旧主重启后应以 follower 归队,且登记不回退(任期栅栏)
    node_start "$RAFT_14_PRIMARY_PORT"
    if [[ "$RAFT_15_OK" == "1" ]]; then
      RAFT_15_REJOIN=""
      for attempt in $(seq 1 20); do
        sleep 1
        st=$($PSQL -p "$RAFT_14_PRIMARY_PORT" -U postgres -tAc \
          "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = ${RAFT_14_GID};" 2>/dev/null || echo "")
        [[ "$st" == "follower" ]] && RAFT_15_REJOIN=1 && break
      done
      RAFT_15_STILL=$($PSQL -p 5432 -U postgres -tAc \
        "SELECT primary_node FROM partdist.partition_map WHERE partition_id = ${RAFT_14_GID}::oid;" 2>/dev/null || echo "")
      if [[ -z "$RAFT_15_REJOIN" ]]; then
        RAFT_15_OK=0
        RAFT_15_WHY="旧主重启后未以 follower 归队(state=${st:-无})"
      elif [[ "$RAFT_15_STILL" != "$RAFT_15_NEWP" ]]; then
        RAFT_15_OK=0
        RAFT_15_WHY="旧主归队后登记被改写(primary=${RAFT_15_STILL},期望 ${RAFT_15_NEWP})"
      fi
    fi
  else
    RAFT_15_WHY="30s 内未见新主登记(自治选举或上报链路未走通)"
    node_start "$RAFT_14_PRIMARY_PORT"
    sleep 2
  fi
fi

if [[ "$RAFT_15_OK" == "1" ]]; then
  ok "raft_15_self_election_failover.sql"
else
  bad "raft_15_self_election_failover.sql(${RAFT_15_WHY})"
fi
raft14_cleanup

# raft_16: 事务 prepare 阶段接线验收(四步设计,计划文档 §4 阶段 3)。
# 不做任何手工 propose:INSERT 经 master 路由提交时,PartWALFlush 在 [A](本地
# parwal fsync)后、[B](pg_wal 提交 fsync)前经 rendezvous 挂钩自动把新记录逐条
# propose 给分区组;多数派持久化才算 prepared。验证:
#   1) 正向:仅 INSERT,follower parwal 自动出现逐字节一致、连续无洞的备份;
#   2) 失多数派:停掉两个 follower 后 INSERT 必须失败(prepare 中止),行数不变;
#   3) 恢复:follower 回归后再 INSERT,连同中断期间的增量自动追平,终态一致。
RAFT_16_TABLE=raft16_demo
RAFT_16_MEMBER_IDS="ARRAY[2,3,4]"
RAFT_16_GID=""

raft16_cleanup() {
  local port
  for port in "${NODE_PORTS[@]}"; do
    $PSQL -p "$port" -U postgres -c \
      "SELECT partdist.pg_raft_group_reset();" &>/dev/null || true
  done
  if [[ -n "$RAFT_16_GID" ]]; then
    for port in 5433 5434 5435; do
      $PSQL -p "$port" -U postgres -c \
        "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${RAFT_16_TABLE}_${RAFT_16_GID};" &>/dev/null || true
    done
    for port in "${NODE_PORTS[@]}"; do
      $PSQL -p "$port" -U postgres -c \
        "DELETE FROM partdist.partition_map WHERE partition_id = ${RAFT_16_GID}::oid;
         DELETE FROM partdist.follower_partition_map WHERE global_shard_id = ${RAFT_16_GID};" &>/dev/null || true
    done
  fi
  $PSQL -p 5432 -U postgres -c \
    "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${RAFT_16_TABLE};" &>/dev/null || true
}

# raft_16 的 follower 断言是**终态收敛**判据，必须给有界的等待窗口而不是
# 一次性快照：Raft 只保证多数派，一笔事务现在要跑三轮复制（DATA / PREPARE 标记 /
# COMMIT 标记），每轮各自凑多数派，完全可能有一个 follower 连续两轮都不在多数派里
# 而短暂落后；leader 的下一次心跳会按 next_index 把它补齐。
# 一次性断言在 2PC 接线后变得对时序敏感（2026-08-04 实测：终态三节点完全一致，
# 只是到达得比 sleep 2 晚）。窗口内收敛即通过，超时才判失败并报最后一次的错。
raft16_follower_converged() {   # $1=port $2=nrec $3=leader_md5
  local port=$1 nrec=$2 md5=$3 i
  for i in $(seq 1 30); do
    if RAFT_16_ERR=$($PSQL -p "$port" -U postgres -v ON_ERROR_STOP=1 \
           -v gid="$RAFT_16_GID" -v nrec="$nrec" \
           -v leader_md5="$md5" -v primary_id="$RAFT_16_PRIMARY_ID" \
           -v shard_table="${RAFT_16_TABLE}_${RAFT_16_GID}" \
           -f "${RAFT_TEST_DIR}/raft_14_hash_shard_secondary_backup.sql" 2>&1); then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_all_nodes
sleep 2

RAFT_16_OK=0
RAFT_16_WHY="setup"
RAFT_16_LEADER=""
RAFT_16_FOLLOWER_PORTS=()

if $PSQL -p 5432 -U postgres -v ON_ERROR_STOP=1 -c \
     "SET citus.enable_ddl_propagation=on;
      DROP TABLE IF EXISTS ${RAFT_16_TABLE};
      SET citus.shard_count = 1;
      SET citus.shard_replication_factor = 1;
      CREATE TABLE ${RAFT_16_TABLE}(id int primary key, v text);
      SELECT create_distributed_table('${RAFT_16_TABLE}', 'id');" &>/dev/null; then

  RAFT_16_GID=$($PSQL -p 5432 -U postgres -tAc \
    "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='${RAFT_16_TABLE}'::regclass;" 2>/dev/null || echo "")
  RAFT_16_PRIMARY_PORT=$($PSQL -p 5432 -U postgres -tAc \
    "SELECT n.nodeport FROM pg_dist_placement p
       JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
      WHERE p.shardid = ${RAFT_16_GID:-0};" 2>/dev/null || echo "")

  if [[ -n "$RAFT_16_GID" && -n "$RAFT_16_PRIMARY_PORT" ]]; then
    RAFT_16_PRIMARY_ID=$(( RAFT_16_PRIMARY_PORT - 5431 ))

    for port in 5433 5434 5435; do
      if [[ "$port" != "$RAFT_16_PRIMARY_PORT" ]]; then
        $PSQL -p "$port" -U postgres -v ON_ERROR_STOP=1 -c \
          "SET citus.enable_ddl_propagation=off;
           CREATE TABLE IF NOT EXISTS ${RAFT_16_TABLE}_${RAFT_16_GID}
             (LIKE ${RAFT_16_TABLE} INCLUDING ALL);" &>/dev/null \
          && RAFT_16_FOLLOWER_PORTS+=("$port")
      fi
      $PSQL -p "$port" -U postgres -c "SELECT partdist.rebuild_shard_identity();" &>/dev/null || true
    done

    $PSQL -p "$RAFT_16_PRIMARY_PORT" -U postgres -c \
      "SELECT partdist.pg_raft_group_create(${RAFT_16_GID}, ${RAFT_16_MEMBER_IDS});" &>/dev/null || true
    for attempt in $(seq 1 15); do
      st=$($PSQL -p "$RAFT_16_PRIMARY_PORT" -U postgres -tAc \
        "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = ${RAFT_16_GID};" 2>/dev/null || echo "")
      [[ "$st" == "leader" ]] && RAFT_16_LEADER="$RAFT_16_PRIMARY_PORT" && break
      sleep 1
    done
    for port in "${RAFT_16_FOLLOWER_PORTS[@]}"; do
      $PSQL -p "$port" -U postgres -c \
        "SELECT partdist.pg_raft_group_create(${RAFT_16_GID}, ${RAFT_16_MEMBER_IDS});" &>/dev/null || true
    done

    if [[ -n "$RAFT_16_LEADER" ]]; then
      # 1) 正向:只 INSERT,不做任何手工 propose
      if $PSQL -p 5432 -U postgres -v ON_ERROR_STOP=1 -c \
           "INSERT INTO ${RAFT_16_TABLE} SELECT g, 'p' || g FROM generate_series(1, 4) g;" &>/dev/null; then
        sleep 2
        RAFT_16_NREC=$($PSQL -p "$RAFT_16_LEADER" -U postgres -tAc \
          "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(${RAFT_16_GID}));" 2>/dev/null || echo 0)
        RAFT_16_MD5=$($PSQL -p "$RAFT_16_LEADER" -U postgres -tAc \
          "SELECT md5(string_agg(sub.h, ',' ORDER BY sub.plsn)) FROM (
             SELECT g AS plsn, md5(r.data) AS h
             FROM generate_series(1, ${RAFT_16_NREC:-0}) g,
                  LATERAL partdist.partwal_read_record(partdist.local_partition_for_shard(${RAFT_16_GID}), g) r
           ) sub;" 2>/dev/null || echo "")

        if [[ "$RAFT_16_NREC" =~ ^[0-9]+$ ]] && [[ "$RAFT_16_NREC" -gt 0 && -n "$RAFT_16_MD5" ]]; then
          RAFT_16_OK=1
          for port in "${RAFT_16_FOLLOWER_PORTS[@]}"; do
            if ! raft16_follower_converged "$port" "$RAFT_16_NREC" "$RAFT_16_MD5"; then
              RAFT_16_OK=0
              RAFT_16_WHY="自动复制后 follower ${port} 断言失败: $(echo "$RAFT_16_ERR" | grep -m1 -i 'ERROR\|raft_14:' | head -c 300)"
              break
            fi
          done
        else
          RAFT_16_WHY="自动复制后 leader 侧无记录(flush=${RAFT_16_NREC})"
        fi

        # 2) 失多数派:停两个 follower,INSERT 必须失败(prepare 中止),行数不变
        if [[ "$RAFT_16_OK" == "1" ]]; then
          for port in "${RAFT_16_FOLLOWER_PORTS[@]}"; do
            node_stop "$port"
          done
          sleep 2
          if $PSQL -p 5432 -U postgres -v ON_ERROR_STOP=1 -c \
               "INSERT INTO ${RAFT_16_TABLE} VALUES (100, 'must-fail');" &>/dev/null; then
            RAFT_16_OK=0
            RAFT_16_WHY="失多数派时 INSERT 竟然成功(prepare 未被拒)"
          else
            RAFT_16_COUNT=$($PSQL -p 5432 -U postgres -tAc \
              "SELECT count(*) FROM ${RAFT_16_TABLE};" 2>/dev/null || echo -1)
            if [[ "$RAFT_16_COUNT" != "4" ]]; then
              RAFT_16_OK=0
              RAFT_16_WHY="失多数派中止后行数=${RAFT_16_COUNT},期望 4"
            fi
          fi
          for port in "${RAFT_16_FOLLOWER_PORTS[@]}"; do
            node_start "$port"
          done
          sleep 3
        fi

        # 3) 恢复:再 INSERT,增量自动复制,终态逐字节一致
        if [[ "$RAFT_16_OK" == "1" ]]; then
          if $PSQL -p 5432 -U postgres -v ON_ERROR_STOP=1 -c \
               "INSERT INTO ${RAFT_16_TABLE} SELECT g, 'q' || g FROM generate_series(5, 6) g;" &>/dev/null; then
            sleep 2
            RAFT_16_NREC2=$($PSQL -p "$RAFT_16_LEADER" -U postgres -tAc \
              "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard(${RAFT_16_GID}));" 2>/dev/null || echo 0)
            RAFT_16_MD52=$($PSQL -p "$RAFT_16_LEADER" -U postgres -tAc \
              "SELECT md5(string_agg(sub.h, ',' ORDER BY sub.plsn)) FROM (
                 SELECT g AS plsn, md5(r.data) AS h
                 FROM generate_series(1, ${RAFT_16_NREC2:-0}) g,
                      LATERAL partdist.partwal_read_record(partdist.local_partition_for_shard(${RAFT_16_GID}), g) r
               ) sub;" 2>/dev/null || echo "")
            if [[ "$RAFT_16_NREC2" =~ ^[0-9]+$ ]] && [[ "$RAFT_16_NREC2" -gt "$RAFT_16_NREC" && -n "$RAFT_16_MD52" ]]; then
              for port in "${RAFT_16_FOLLOWER_PORTS[@]}"; do
                if ! raft16_follower_converged "$port" "$RAFT_16_NREC2" "$RAFT_16_MD52"; then
                  RAFT_16_OK=0
                  RAFT_16_WHY="恢复后追平断言失败(follower ${port}): $(echo "$RAFT_16_ERR" | grep -m1 -i 'ERROR\|raft_14:' | head -c 300)"
                  break
                fi
              done
            else
              RAFT_16_OK=0
              RAFT_16_WHY="恢复后 INSERT 未产生新记录(flush ${RAFT_16_NREC} -> ${RAFT_16_NREC2})"
            fi
          else
            RAFT_16_OK=0
            RAFT_16_WHY="多数派恢复后 INSERT 仍失败"
          fi
        fi
      else
        RAFT_16_WHY="正向 INSERT 失败(prepare 挂钩误拒?)"
      fi
    else
      RAFT_16_WHY="数据组 leader 未落在 placement 节点"
    fi
  else
    RAFT_16_WHY="拿不到 shardid/placement"
  fi
fi

if [[ "$RAFT_16_OK" == "1" ]]; then
  ok "raft_16_prepare_auto_replicate"
else
  bad "raft_16_prepare_auto_replicate(${RAFT_16_WHY})"
fi
raft16_cleanup

# ------------------------------------------------------------------
# raft_17: 并发 prepare 的多数派保证(group commit 让路窗口)
# 用例本体在 test/raft_17_concurrent_prepare_quorum.sh —— 它自带夹具与清理,
# 也可独立跑(迭代时不必等整套跑完)。判据是"持有完整前缀的成员数 >= 多数派",
# 不是"全体 follower 追平" —— Raft 只保证 quorum,且本项目尚无后台追平通道。
# ------------------------------------------------------------------
section "raft_17 并发 prepare 的多数派保证"

start_all_nodes
sleep 2
RAFT_17_OUT=$(CONTAINER="$CONTAINER" BASE_PORT="$BASE_PORT" N_WORKERS="$N_WORKERS" \
                bash "${SCRIPT_DIR}/test/raft_17_concurrent_prepare_quorum.sh" 2>&1) && RAFT_17_RC=0 || RAFT_17_RC=$?
echo "$RAFT_17_OUT" | sed 's/^/    /'
if [[ "$RAFT_17_RC" -eq 0 ]]; then
  ok "raft_17_concurrent_prepare_quorum"
else
  bad "raft_17_concurrent_prepare_quorum($(echo "$RAFT_17_OUT" | tail -1))"
fi


# ------------------------------------------------------------------
# raft_18: 数据组成员集必须显式/可导出，多数派按真实副本集计算
# 用例本体在 test/raft_18_membership_explicit.sh（自带夹具与清理，可独立跑）。
# ------------------------------------------------------------------
section "raft_18 成员集显式化与真实多数派"

start_all_nodes
sleep 2
RAFT_18_OUT=$(CONTAINER="$CONTAINER" BASE_PORT="$BASE_PORT" N_WORKERS="$N_WORKERS" \
                bash "${SCRIPT_DIR}/test/raft_18_membership_explicit.sh" 2>&1) && RAFT_18_RC=0 || RAFT_18_RC=$?
echo "$RAFT_18_OUT" | sed 's/^/    /'
if [[ "$RAFT_18_RC" -eq 0 ]]; then
  ok "raft_18_membership_explicit"
else
  bad "raft_18_membership_explicit($(echo "$RAFT_18_OUT" | tail -1))"
fi

# ------------------------------------------------------------------
# raft_19: DTX-2PC 记录格式与 flags 端到端保真（含全新库 CREATE EXTENSION 冒烟）
# 用例本体在 test/raft_19_dtx_record_format.sh（自带夹具与清理，可独立跑）。
# ------------------------------------------------------------------
section "raft_19 DTX 记录格式与 flags 保真"

start_all_nodes
sleep 2
RAFT_19_OUT=$(CONTAINER="$CONTAINER" BASE_PORT="$BASE_PORT" N_WORKERS="$N_WORKERS" \
                bash "${SCRIPT_DIR}/test/raft_19_dtx_record_format.sh" 2>&1) && RAFT_19_RC=0 || RAFT_19_RC=$?
echo "$RAFT_19_OUT" | sed 's/^/    /'
if [[ "$RAFT_19_RC" -eq 0 ]]; then
  ok "raft_19_dtx_record_format"
else
  bad "raft_19_dtx_record_format($(echo "$RAFT_19_OUT" | tail -1))"
fi

# ------------------------------------------------------------------
# raft_20: DTX-2PC 决议层（决议在协调组达多数派即为全局提交点）
# 用例本体在 test/raft_20_dtx_decision.sh（自带夹具与清理，可独立跑）。
# ------------------------------------------------------------------
section "raft_20 DTX 决议层"

start_all_nodes
sleep 2
RAFT_20_OUT=$(CONTAINER="$CONTAINER" BASE_PORT="$BASE_PORT" N_WORKERS="$N_WORKERS" \
                bash "${SCRIPT_DIR}/test/raft_20_dtx_decision.sh" 2>&1) && RAFT_20_RC=0 || RAFT_20_RC=$?
echo "$RAFT_20_OUT" | sed 's/^/    /'
if [[ "$RAFT_20_RC" -eq 0 ]]; then
  ok "raft_20_dtx_decision"
else
  bad "raft_20_dtx_decision($(echo "$RAFT_20_OUT" | tail -1))"
fi

# ------------------------------------------------------------------
# raft_21: DTX-2PC 参与者侧恢复守护（推定中止 + 按决议闭合）
# 用例本体在 test/raft_21_dtx_recovery.sh（自带夹具与清理，可独立跑）。
# ------------------------------------------------------------------
section "raft_21 DTX 恢复守护"

start_all_nodes
sleep 2
RAFT_21_OUT=$(CONTAINER="$CONTAINER" BASE_PORT="$BASE_PORT" N_WORKERS="$N_WORKERS" \
                bash "${SCRIPT_DIR}/test/raft_21_dtx_recovery.sh" 2>&1) && RAFT_21_RC=0 || RAFT_21_RC=$?
echo "$RAFT_21_OUT" | sed 's/^/    /'
if [[ "$RAFT_21_RC" -eq 0 ]]; then
  ok "raft_21_dtx_recovery"
else
  bad "raft_21_dtx_recovery($(echo "$RAFT_21_OUT" | tail -1))"
fi

# ------------------------------------------------------------------
# raft_22: DTX-2PC 端到端（真实跨分区事务经内核补丁 0004 的挂点走完三阶段）
# 用例本体在 test/raft_22_dtx_end_to_end.sh（自带夹具与清理，可独立跑）。
# ------------------------------------------------------------------
section "raft_22 DTX 端到端"

start_all_nodes
sleep 2
RAFT_22_OUT=$(CONTAINER="$CONTAINER" BASE_PORT="$BASE_PORT" N_WORKERS="$N_WORKERS" \
                bash "${SCRIPT_DIR}/test/raft_22_dtx_end_to_end.sh" 2>&1) && RAFT_22_RC=0 || RAFT_22_RC=$?
echo "$RAFT_22_OUT" | sed 's/^/    /'
if [[ "$RAFT_22_RC" -eq 0 ]]; then
  ok "raft_22_dtx_end_to_end"
else
  bad "raft_22_dtx_end_to_end($(echo "$RAFT_22_OUT" | tail -1))"
fi

# ------------------------------------------------------------------
section "汇总"
echo ""
echo "通过: ${PASS}  失败: ${FAIL}"
if [[ $FAIL -eq 0 ]]; then
  echo ">>> Raft 回归全部通过（拓扑 1c+${N_WORKERS}w）<<<"
  exit 0
else
  echo ">>> 存在 ${FAIL} 项失败,请根据上方 [FAIL] 排查 <<<"
  exit 1
fi
