#!/usr/bin/env bash
# raft_18: 数据组成员集必须显式/可导出，多数派按真实副本集计算
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_18_membership_explicit.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# ── 复现的缺陷（DTX_2PC_DESIGN.md §9.2）────────────────────────────────
# 旧代码把 `n_members == 0` 重载为"全体节点"。这对控制面组（组 0）成立，
# 对数据组**永远不成立**——分片副本集必然是全体节点的真子集。实测后果
# （9 节点环境，以 SQL 默认的 NULL 成员集建组）：
#   · cluster_size 算成 9、多数派算成 5，而只有 3 个节点持有该分片数据；
#   · 该组向全集群广播 RequestVote/AppendEntries，非副本节点收到后照样
#     hearsay 建组（同样空成员集）并参与投票；
#   · 真正持有数据的 worker 反被挤成 term=0 的 follower，分片彻底不可用；
#   · 本质危险：同一组在不同节点上有两套不相交的多数派定义（2/3 vs 5/9），
#     Leader Completeness 失去交集保证。
#
# ── 判据（四条，全部是确定性的，无并发/无概率）────────────────────────
#   A. NULL 成员集 + partition_map 无登记 ⇒ 建组必须**报错拒绝**
#      （旧行为：建出 cluster_size=9 的组）
#   B. partition_map 有登记 ⇒ NULL 建组自动导出成员集，cluster_size == 3
#      （控制面是成员集的权威来源，本地可读，无需新增 RPC）
#   C. quorum 按真实成员数：3 成员全在可写；停 1 个（2/3）仍可写；
#      停 2 个（1/3）必败。旧行为下多数派=5，三个成员全在也写不进去。
#   D. 非副本节点不被拖入该组（旧行为：5436-5440 会 hearsay 建组）
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }
node_ctl() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/$1" "${@:2}" >/dev/null 2>&1; }

if [[ -z "${N_WORKERS:-}" ]]; then
  N_WORKERS=$(docker exec "$CONTAINER" bash -lc \
    "find /work/pg-cluster-data -maxdepth 1 -type d -name 'worker*' | wc -l" 2>/dev/null || echo 3)
  N_WORKERS=${N_WORKERS//[^0-9]/}
  [[ -n "$N_WORKERS" && "$N_WORKERS" -gt 0 ]] || N_WORKERS=3
fi
ALL_PORTS=()
for ((i = 0; i <= N_WORKERS; i++)); do ALL_PORTS+=($((BASE_PORT + i))); done

TBL=raft18_mem
GID=""
# 组成员固定取 node 2/3/4（worker1/2/3）——与 raft_14/16/17 的夹具一致
LEADER_PORT=$((BASE_PORT + 1))   # worker1 = node 2
F1_PORT=$((BASE_PORT + 2))       # worker2 = node 3
F2_PORT=$((BASE_PORT + 3))       # worker3 = node 4
MEMBER_PORTS=("$LEADER_PORT" "$F1_PORT" "$F2_PORT")

cleanup() {
  for d in worker1 worker2 worker3; do
    node_ctl "$d" -l "/work/pg-cluster-data/${d}.log" start -w -t 30
  done
  if [[ -n "$GID" ]]; then
    for port in "${ALL_PORTS[@]}"; do
      q "$port" "SELECT partdist.pg_raft_group_drop(${GID});" >/dev/null
      q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;" >/dev/null
      q "$port" "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}_${GID};" >/dev/null
    done
  fi
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TBL};" >/dev/null
}
fail() { cleanup; echo "raft_18 FAIL: $1"; exit 1; }

cleanup

# ── 夹具：一张分布式表，取一个落在 worker1 上的分片 ──
# 有界重试：套件里紧挨着 raft_17（第三阶段停/启两个 worker），刚重启的节点
# 可能还没就绪，建表打到它会失败——功能夹具且幂等，重试不弱化任何判据。
FIX_OK=0
for _ in 1 2 3; do
  if psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
    "SET citus.enable_ddl_propagation=on;
     SET citus.shard_count = $((N_WORKERS * 2));
     SET citus.shard_replication_factor = 1;
     DROP TABLE IF EXISTS ${TBL};
     CREATE TABLE ${TBL}(id int primary key, v text);
     SELECT create_distributed_table('${TBL}', 'id');" >/dev/null 2>&1; then
    FIX_OK=1; break
  fi
  sleep 5
done
[[ "$FIX_OK" == "1" ]] || fail "建分布式表失败（重试 3 次）"

GID=$(q "$BASE_PORT" \
  "SELECT p.shardid FROM pg_dist_placement p
     JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole='primary'
     JOIN pg_dist_shard s ON s.shardid = p.shardid
    WHERE s.logicalrelid = '${TBL}'::regclass AND n.nodeport = ${LEADER_PORT}
    ORDER BY p.shardid LIMIT 1;")
[[ -n "$GID" ]] || fail "worker1 上没有该表的分片"
q "$LEADER_PORT" "SELECT partdist.rebuild_shard_identity();" >/dev/null

# ── A. NULL 成员集 + 无登记 ⇒ 必须报错拒绝建组 ──
A_OUT=$(psql_at "$LEADER_PORT" -tAc "SELECT partdist.pg_raft_group_create(${GID});" 2>&1)
if ! grep -q "成员集未知" <<<"$A_OUT"; then
  fail "A: NULL 成员集建组本应报错拒绝，实际输出：$(tr '\n' ' ' <<<"$A_OUT" | cut -c1-160)"
fi
A_CS=$(q "$LEADER_PORT" "SELECT cluster_size FROM partdist.pg_raft_group_status() WHERE group_id=${GID};")
[[ -z "$A_CS" ]] || fail "A: 建组被拒后不应留下组，实际 cluster_size=${A_CS}"
echo "raft_18 A: NULL 成员集 + 无登记 ⇒ 建组被拒且不留残组 ✓"

# ── B. 控制面登记后，NULL 建组自动导出成员集 ──
for port in "${MEMBER_PORTS[@]}"; do
  q "$port" "INSERT INTO partdist.partition_map(partition_id, primary_node, secondary_nodes, primary_term)
             VALUES (${GID}::oid, 2, ARRAY[3,4], 1)
             ON CONFLICT (partition_id) DO UPDATE
               SET primary_node = EXCLUDED.primary_node,
                   secondary_nodes = EXCLUDED.secondary_nodes,
                   primary_term = EXCLUDED.primary_term;" >/dev/null
done
q "$LEADER_PORT" "SELECT partdist.pg_raft_group_create(${GID});" >/dev/null
B_CS=$(q "$LEADER_PORT" "SELECT cluster_size FROM partdist.pg_raft_group_status() WHERE group_id=${GID};")
[[ "$B_CS" == "3" ]] \
  || fail "B: 成员集应从 partition_map 导出为 3 个成员，实际 cluster_size=${B_CS:-<无组>}（=${N_WORKERS}+1 说明仍在按全体节点算多数派）"
echo "raft_18 B: partition_map 登记 ⇒ 成员集自动导出，cluster_size=3 ✓"

# ── C. quorum 按真实成员数（3 成员 ⇒ 多数派 2）──
for port in "$F1_PORT" "$F2_PORT"; do
  q "$port" "SET citus.enable_ddl_propagation=off;
             CREATE TABLE IF NOT EXISTS ${TBL}_${GID} (LIKE ${TBL} INCLUDING ALL);" >/dev/null
  q "$port" "SELECT partdist.rebuild_shard_identity();" >/dev/null
  q "$port" "SELECT partdist.pg_raft_group_create(${GID});" >/dev/null
done
# ★ 不能断言"某个特定节点当选"——Raft 不保证哪个成员赢，三个成员都可能。
#   （初版就是这么写的，实测 worker3 先超时先当选而挂掉，属于面向结果的错误断言。）
#   正确做法：等**任一成员**当选，动态确定 leader 与要停的 follower。
LEADER_ACTUAL=""
for _ in $(seq 1 30); do
  for port in "${MEMBER_PORTS[@]}"; do
    if [[ "$(q "$port" "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID};")" == leader ]]; then
      LEADER_ACTUAL="$port"; break
    fi
  done
  [[ -n "$LEADER_ACTUAL" ]] && break
  sleep 1
done
[[ -n "$LEADER_ACTUAL" ]] || fail "C: 30s 内三个成员都没选出 leader"

# 写入要经 Citus 路由到 leader 所在节点，否则会被写栅栏（非本组 leader）拒绝。
# 切主重构（计划 §13）会把 pg_dist_placement 切到新主，这里等它跟上。
LEADER_NODE=$((LEADER_ACTUAL - BASE_PORT + 1))
routed=0
for _ in $(seq 1 30); do
  cur=$(q "$BASE_PORT" "SELECT m.node_id FROM pg_dist_placement p
                          JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole='primary'
                          JOIN partdist.node_map m ON m.port = n.nodeport
                         WHERE p.shardid = ${GID};")
  [[ "$cur" == "$LEADER_NODE" ]] && { routed=1; break; }
  sleep 1
done
(( routed == 1 )) || fail "C: 30s 内 pg_dist_placement 未切到 leader 所在节点(node ${LEADER_NODE})"

FOLLOWER_PORTS=()
for port in "${MEMBER_PORTS[@]}"; do
  [[ "$port" == "$LEADER_ACTUAL" ]] || FOLLOWER_PORTS+=("$port")
done
port_dir() { echo "worker$(( $1 - BASE_PORT ))"; }

read -r -a IDS <<<"$(q "$BASE_PORT" "SELECT string_agg(id::text,' ') FROM (
    SELECT g AS id FROM generate_series(1,60000) g
     WHERE get_shard_id_for_distribution_column('${TBL}', g) = ${GID} LIMIT 4) t;")"
(( ${#IDS[@]} >= 3 )) || fail "C: 取不到落在该分片的 id"

psql_at "$BASE_PORT" -q -c "INSERT INTO ${TBL} VALUES (${IDS[0]},'a');" >/dev/null 2>&1 \
  || fail "C: 3 个成员全在时 INSERT 竟然失败（多数派可能仍按全体节点算）"

node_ctl "$(port_dir "${FOLLOWER_PORTS[0]}")" stop -m fast; sleep 2
psql_at "$BASE_PORT" -q -c "INSERT INTO ${TBL} VALUES (${IDS[1]},'b');" >/dev/null 2>&1 \
  || fail "C: 停 1 个 follower（2/3 仍是多数派）时 INSERT 竟然失败"

node_ctl "$(port_dir "${FOLLOWER_PORTS[1]}")" stop -m fast; sleep 2
if psql_at "$BASE_PORT" -q -c "INSERT INTO ${TBL} VALUES (${IDS[2]},'c');" >/dev/null 2>&1; then
  fail "C: 停 2 个 follower（1/3，已失多数派）时 INSERT 竟然成功"
fi
for port in "${FOLLOWER_PORTS[@]}"; do
  d=$(port_dir "$port")
  node_ctl "$d" -l "/work/pg-cluster-data/${d}.log" start -w -t 30
done
sleep 3
echo "raft_18 C: leader=port ${LEADER_ACTUAL}，quorum 按真实成员数（3 全在✓ / 停1可写✓ / 停2必败✓）✓"

# ── D. 非副本节点不被拖入该组 ──
INTRUDERS=""
for port in "${ALL_PORTS[@]}"; do
  case " ${MEMBER_PORTS[*]} " in *" $port "*) continue ;; esac
  n=$(q "$port" "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=${GID};")
  [[ "$n" == "0" ]] || INTRUDERS+=" $port"
done
[[ -z "$INTRUDERS" ]] \
  || fail "D: 非副本节点${INTRUDERS} 被拖入组 ${GID}（hearsay 建组未被拦下）"
echo "raft_18 D: 非成员节点未被拖入该组 ✓"

cleanup
echo "raft_18 PASS"
exit 0
