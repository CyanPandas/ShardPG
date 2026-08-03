#!/usr/bin/env bash
# raft_21: DTX-2PC 参与者侧恢复守护（推定中止 + 按决议闭合）
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_21_dtx_recovery.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# 依据：DTX_2PC_DESIGN.md §7（恢复）、§2.2（推定中止）、§10 第 5 步。
#
# ── 夹具说明（诚实边界）────────────────────────────────────────────
# 自动接线（内核补丁 0004 的 master 挂点，§9.3）尚未落地，所以**没有任何
# 代码会自动产生 shardpg_dtx_* 的 prepared 事务**。本用例用 `PREPARE
# TRANSACTION 'shardpg_dtx_<dtxid>_<coord_gsid>'` 手工造出参与者侧的
# in-doubt 事务——这测的是恢复守护本身（解析 gid → 按 partition_map 寻址
# 协调组 → 问决议 → 闭合 + 补标记），与谁产生的 prepared 事务无关。
#
# ── 判据 ────────────────────────────────────────────────────────────
#   A. 协调组已有 COMMIT 决议 ⇒ 恢复守护把 prepared 事务 **COMMIT**，
#      数据可见，并在该分区 parwal 补一条 DTX_COMMIT(kind=3) 标记。
#   B. 从未决议 ⇒ 恢复守护经 dtx_status 触发**推定中止**，
#      prepared 事务被 **ROLLBACK**、数据不可见、补 DTX_ABORT(kind=4) 标记，
#      且协调组的决议索引里留下 verdict=2（推定中止是**写下来**的，不是隐含的）。
#   C. 超时未到的 prepared 事务**不被动**（守护只处理超时的，避免与正常路径抢答）。
#   D. 协调组不可达时**保持 prepared 不动**——绝不擅自决定，
#      推定中止的权力只在协调组手里。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }
node_ctl() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/$1" "${@:2}" >/dev/null 2>&1; }
port_dir() { echo "worker$(( $1 - BASE_PORT ))"; }

TBL=raft21_dtx
GID=""
LEADER_PORT=""
MEMBER_PORTS=($((BASE_PORT + 1)) $((BASE_PORT + 2)) $((BASE_PORT + 3)))
DTX_C=910001   # 有 COMMIT 决议
DTX_A=910002   # 从未决议 → 推定中止
DTX_F=910003   # 未超时，不该被动
DTX_U=910004   # 协调组不可达

cleanup() {
  local port gid
  for port in "${MEMBER_PORTS[@]}"; do
    d=$(port_dir "$port")
    node_ctl "$d" -l "/work/pg-cluster-data/${d}.log" start -w -t 30
  done
  sleep 1
  for port in "${MEMBER_PORTS[@]}"; do
    for g in $(q "$port" "SELECT gid FROM pg_prepared_xacts WHERE strpos(gid,'shardpg_dtx_')=1;"); do
      q "$port" "ROLLBACK PREPARED '${g}';" >/dev/null
    done
    q "$port" "DELETE FROM partdist.dtx_decision WHERE dtxid IN (${DTX_C},${DTX_A},${DTX_F},${DTX_U});" >/dev/null
    if [[ -n "$GID" ]]; then
      q "$port" "SELECT partdist.pg_raft_group_drop(${GID});" >/dev/null
      q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;" >/dev/null
      q "$port" "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}_${GID};" >/dev/null
    fi
  done
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TBL};" >/dev/null
}
fail() { cleanup; echo "raft_21 FAIL: $1"; exit 1; }

cleanup

# ── 夹具：3 成员数据组，同时充当协调组与参与组 ──
psql_at "$BASE_PORT" -v ON_ERROR_STOP=1 -q -c \
  "SET citus.enable_ddl_propagation=on;
   SET citus.shard_count = 16;
   SET citus.shard_replication_factor = 1;
   CREATE TABLE ${TBL}(id int primary key, v text);
   SELECT create_distributed_table('${TBL}', 'id');" >/dev/null 2>&1 \
  || fail "建分布式表失败"

GID=$(q "$BASE_PORT" \
  "SELECT p.shardid FROM pg_dist_placement p
     JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
     JOIN pg_dist_shard s ON s.shardid=p.shardid
    WHERE s.logicalrelid='${TBL}'::regclass AND n.nodeport=${MEMBER_PORTS[0]}
    ORDER BY p.shardid LIMIT 1;")
[[ -n "$GID" ]] || fail "worker1 上没有该表的分片"

for port in "${MEMBER_PORTS[@]:1}"; do
  q "$port" "SET citus.enable_ddl_propagation=off;
             CREATE TABLE IF NOT EXISTS ${TBL}_${GID} (LIKE ${TBL} INCLUDING ALL);" >/dev/null
done
for port in "${MEMBER_PORTS[@]}"; do
  q "$port" "SELECT partdist.rebuild_shard_identity();" >/dev/null
  q "$port" "INSERT INTO partdist.partition_map(partition_id, primary_node, secondary_nodes, primary_term)
             VALUES (${GID}::oid, 2, ARRAY[3,4], 1)
             ON CONFLICT (partition_id) DO UPDATE
               SET primary_node=EXCLUDED.primary_node,
                   secondary_nodes=EXCLUDED.secondary_nodes;" >/dev/null
  q "$port" "SELECT partdist.pg_raft_group_create(${GID});" >/dev/null
done
for _ in $(seq 1 30); do
  for port in "${MEMBER_PORTS[@]}"; do
    [[ "$(q "$port" "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID};")" == leader ]] \
      && { LEADER_PORT="$port"; break; }
  done
  [[ -n "$LEADER_PORT" ]] && break
  sleep 1
done
[[ -n "$LEADER_PORT" ]] || fail "30s 内协调组没选出 leader"

# partition_map 的 primary_node 必须指向真实 leader，恢复守护才寻址得到
LEADER_NODE=$((LEADER_PORT - BASE_PORT + 1))
for port in "${MEMBER_PORTS[@]}"; do
  q "$port" "UPDATE partdist.partition_map SET primary_node=${LEADER_NODE}
              WHERE partition_id=${GID}::oid;" >/dev/null
done

SHARD_TBL="${TBL}_${GID}"
LOID=$(q "$LEADER_PORT" "SELECT partdist.local_partition_for_shard(${GID});")
[[ -n "$LOID" && "$LOID" != "0" ]] || fail "leader 上解析不到本地分片 OID"

# 造一个 in-doubt 的 prepared 事务（gid 编码 dtxid 与 coord_gsid）
make_prepared() {   # $1=dtxid $2=行 id
  psql_at "$LEADER_PORT" -q -c \
    "BEGIN; INSERT INTO ${SHARD_TBL} VALUES ($2, 'p$1');
     PREPARE TRANSACTION 'shardpg_dtx_$1_${GID}';" >/dev/null 2>&1
}
row_visible() { q "$LEADER_PORT" "SELECT count(*) FROM ${SHARD_TBL} WHERE id=$1;"; }
marker_kind() {  # 该分区最后一条记录的 DTX kind（不是 DTX 记录则空）
  local n
  n=$(q "$LEADER_PORT" "SELECT partdist.get_partition_flush_lsn(${LOID}::oid);")
  q "$LEADER_PORT" "SELECT kind FROM partdist.partwal_read_dtx_record(${LOID}::oid, ${n});"
}

# ── A. 已有 COMMIT 决议 ⇒ 恢复守护提交 ──
make_prepared "$DTX_C" 1001 || fail "A: 造 prepared 事务失败"
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='shardpg_dtx_${DTX_C}_${GID}';")" == "1" ]] \
  || fail "A: prepared 事务没造出来"
DEC=$(q "$LEADER_PORT" "SELECT partdist.dtx_decide(${GID}, ${DTX_C}, 1, ARRAY[${GID}]::bigint[]);")
[[ "$DEC" == "1" ]] || fail "A: 预置 COMMIT 决议失败（返回 '${DEC}'）"
N=$(q "$LEADER_PORT" "SELECT partdist.dtx_recover_prepared(0);")
[[ "$N" =~ ^[0-9]+$ ]] && (( N >= 1 )) || fail "A: 恢复守护应处理至少 1 笔，实际 '${N}'"
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='shardpg_dtx_${DTX_C}_${GID}';")" == "0" ]] \
  || fail "A: prepared 事务未被闭合"
[[ "$(row_visible 1001)" == "1" ]] \
  || fail "A: 决议是 COMMIT，数据应可见"
[[ "$(marker_kind)" == "3" ]] \
  || fail "A: 应补写 DTX_COMMIT(kind=3) 标记，实际 '$(marker_kind)'"
echo "raft_21 A: 有 COMMIT 决议 ⇒ 恢复守护提交、数据可见、补 DTX_COMMIT 标记 ✓"

# ── B. 从未决议 ⇒ 推定中止 ──
make_prepared "$DTX_A" 1002 || fail "B: 造 prepared 事务失败"
N=$(q "$LEADER_PORT" "SELECT partdist.dtx_recover_prepared(0);")
[[ "$N" =~ ^[0-9]+$ ]] && (( N >= 1 )) || fail "B: 恢复守护应处理至少 1 笔，实际 '${N}'"
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='shardpg_dtx_${DTX_A}_${GID}';")" == "0" ]] \
  || fail "B: prepared 事务未被闭合"
[[ "$(row_visible 1002)" == "0" ]] \
  || fail "B: 推定中止，数据不应可见"
[[ "$(marker_kind)" == "4" ]] \
  || fail "B: 应补写 DTX_ABORT(kind=4) 标记，实际 '$(marker_kind)'"
BV=$(q "$LEADER_PORT" "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX_A};")
[[ "$BV" == "2" ]] \
  || fail "B: 推定中止必须**把 ABORT 决议写下来**（否则后到的 COMMIT 能翻盘），索引表实际 '${BV:-无}'"
echo "raft_21 B: 从未决议 ⇒ 推定中止、数据不可见、补 DTX_ABORT 标记且决议已落库 ✓"

# ── C. 未超时的不该被动 ──
make_prepared "$DTX_F" 1003 || fail "C: 造 prepared 事务失败"
q "$LEADER_PORT" "SELECT partdist.dtx_recover_prepared(600000);" >/dev/null
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='shardpg_dtx_${DTX_F}_${GID}';")" == "1" ]] \
  || fail "C: 超时窗口内的 prepared 事务不该被恢复守护动"
[[ -z "$(q "$LEADER_PORT" "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX_F};")" ]] \
  || fail "C: 未超时的事务不该被写决议"
echo "raft_21 C: 未超时的 prepared 事务不被触碰（不与正常路径抢答）✓"

# ── D. 协调组不可达 ⇒ 保持 prepared 不动 ──
# 把 partition_map 指向一个不存在的节点，模拟协调者不可达
for port in "${MEMBER_PORTS[@]}"; do
  q "$port" "UPDATE partdist.partition_map SET primary_node=97
              WHERE partition_id=${GID}::oid;" >/dev/null
done
make_prepared "$DTX_U" 1004 || fail "D: 造 prepared 事务失败"
q "$LEADER_PORT" "SELECT partdist.dtx_recover_prepared(0);" >/dev/null
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='shardpg_dtx_${DTX_U}_${GID}';")" == "1" ]] \
  || fail "D: 协调组不可达时必须保持 prepared 不动（绝不擅自决定），实际已被闭合"
[[ -z "$(q "$LEADER_PORT" "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX_U};")" ]] \
  || fail "D: 协调组不可达时不该产生决议"
echo "raft_21 D: 协调组不可达 ⇒ 保持 prepared 不动，推定中止的权力只在协调组手里 ✓"

cleanup
echo "raft_21 PASS"
exit 0
