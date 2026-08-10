#!/usr/bin/env bash
# raft_20: DTX-2PC 决议层 —— 决议在协调组达多数派持久化即为全局提交点
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_20_dtx_decision.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# 依据：DTX_2PC_DESIGN.md §6（决议层）、§2.2（推定中止）、§10 第 4 步。
#
# ── 判据 ────────────────────────────────────────────────────────────
#   A. 非协调组 leader 上调 dtx_decide 返回 NULL（调用方据此按 partition_map
#      重新寻址），且**不留下任何决议**。
#   B. leader 上 COMMIT 决议返回后，DECISION 记录与索引表在**协调组的每个成员**
#      上都在 —— 返回即"已在多数派持久化"，这就是全局提交点。
#   C. 决议槽一次性：对同一 dtxid 再决议 ABORT 仍返回 1，不覆盖。
#   D. 推定中止：对从未决议的 dtxid 调 dtx_status，**先写 ABORT 达多数派再答 2**；
#      此后再想决议成 COMMIT 必须仍得 2。这一步不能省 —— 否则"问的时候没有、
#      答完之后原提交路径又把 COMMIT 写进去"会让同一事务出现两个矛盾结论。
#   E. ★ 协调组切主后决议仍可查：停掉协调组 leader，等新 leader 当选，
#      在新 leader 上 dtx_status 必须仍返回原决议 —— 这就是"协调权随 Raft
#      选举自动转移"，无需任何状态搬迁（索引表由各成员 apply 时各自维护）。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }
node_ctl() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/$1" "${@:2}" >/dev/null 2>&1; }
port_dir() { echo "worker$(( $1 - BASE_PORT ))"; }

TBL=raft20_dtx
GID=""
LEADER_PORT=""
FOLLOWER_PORTS=()
MEMBER_PORTS=($((BASE_PORT + 1)) $((BASE_PORT + 2)) $((BASE_PORT + 3)))

# ── dtxid 必须每轮唯一（2026-08-09 审查）────────────────────────────────
# 旧版是写死的 900101/900102/900103，而清理走的是吞错的 q()（节点不可达、
# 表不存在、语法错都静默成功）。上一轮遗留的 (dtxid=900102, verdict=2) 会让
# D 段的三条判据**全部成立**：dtx_status 直接读到旧行就返回 2、索引表里本来
# 就有 verdict=2、再决议 COMMIT 也因为决议槽一次性而返回 2 —— 于是"查无决议时
# **先写 ABORT 达多数派再答复**"这条真正的写路径一次都没被执行过。
# 照 raft_12 的做法给 dtxid 加运行时唯一前缀，并在夹具阶段断言这些 id 在
# **每个成员**上都不存在，否则 fail-fast（残留意味着判据已被污染）。
DTX_BASE=$(( 900000000 + ($(date +%s) % 9000000) * 16 ))
DTX_OK=$((       DTX_BASE + 1 ))   # 走正常提交路径的事务
DTX_PRESUMED=$(( DTX_BASE + 2 ))   # 从未决议、靠推定中止收敛的事务
DTX_FGT=$((      DTX_BASE + 3 ))   # F 段：回执收齐后走 FORGET 回收

cleanup() {
  for port in "${MEMBER_PORTS[@]}"; do
    d=$(port_dir "$port")
    node_ctl "$d" -l "/work/pg-cluster-data/${d}.log" start -w -t 30
  done
  for port in "${MEMBER_PORTS[@]}" "$BASE_PORT"; do
    q "$port" "DELETE FROM partdist.dtx_decision WHERE dtxid IN (${DTX_OK}, ${DTX_PRESUMED}, ${DTX_FGT});" >/dev/null
    if [[ -n "$GID" ]]; then
      q "$port" "SELECT partdist.pg_raft_group_drop(${GID});" >/dev/null
      q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;" >/dev/null
      q "$port" "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}_${GID};" >/dev/null
    fi
  done
  q "$BASE_PORT" "SET citus.enable_ddl_propagation=on; DROP TABLE IF EXISTS ${TBL};" >/dev/null
}
fail() { cleanup; echo "raft_20 FAIL: $1"; exit 1; }

cleanup

# ── 夹具：一个 3 成员数据组当协调组 ──
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

find_leader() {   # 在成员里找 leader；$1=排除的端口（可空）
  local port
  for port in "${MEMBER_PORTS[@]}"; do
    [[ "$port" == "${1:-}" ]] && continue
    [[ "$(q "$port" "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID};")" == leader ]] \
      && { echo "$port"; return 0; }
  done
  return 1
}
for _ in $(seq 1 30); do
  LEADER_PORT=$(find_leader || true)
  [[ -n "$LEADER_PORT" ]] && break
  sleep 1
done
[[ -n "$LEADER_PORT" ]] || fail "30s 内协调组没选出 leader"
for port in "${MEMBER_PORTS[@]}"; do
  [[ "$port" == "$LEADER_PORT" ]] || FOLLOWER_PORTS+=("$port")
done

# ── 夹具前提：三个 dtxid 在**每个成员**上都必须查无此行 ──
# 只要有一行残留，D 段（推定中止）就会退化成"读到旧决议"，写路径测不到。
PRE_CHECKED=0
for port in "${MEMBER_PORTS[@]}" "$BASE_PORT"; do
  for d in "$DTX_OK" "$DTX_PRESUMED" "$DTX_FGT"; do
    n=$(q "$port" "SELECT count(*) FROM partdist.dtx_decision WHERE dtxid=${d};")
    [[ "$n" =~ ^[0-9]+$ ]] \
      || fail "夹具：节点 ${port} 上读 dtx_decision 失败（返回 '${n}'），无法确认 dtxid 干净"
    [[ "$n" == "0" ]] \
      || fail "夹具：节点 ${port} 上 dtxid=${d} 已有 ${n} 行残留 —— 判据会被上一轮结果污染"
    PRE_CHECKED=$((PRE_CHECKED + 1))
  done
done
(( PRE_CHECKED == (${#MEMBER_PORTS[@]} + 1) * 3 )) \
  || fail "夹具：dtxid 干净性只检查了 ${PRE_CHECKED} 项，覆盖不全"
echo "raft_20 夹具: dtxid ${DTX_OK}/${DTX_PRESUMED}/${DTX_FGT} 在全部成员上均无残留 ✓"

# ── A. 非 leader 上决议必须返回 NULL 且不留痕 ──
A=$(q "${FOLLOWER_PORTS[0]}" \
     "SELECT coalesce(partdist.dtx_decide(${GID}, ${DTX_OK}, 1, ARRAY[${GID}]::bigint[])::text, 'NULL');")
[[ "$A" == "NULL" ]] \
  || fail "A: 非协调组 leader 上 dtx_decide 应返回 NULL，实际 '${A}'"
LEFT=$(q "${FOLLOWER_PORTS[0]}" "SELECT count(*) FROM partdist.dtx_decision WHERE dtxid=${DTX_OK};")
[[ "$LEFT" == "0" ]] || fail "A: 非 leader 上被拒的决议不应留痕，实际留下 ${LEFT} 行"
echo "raft_20 A: 非协调组 leader 上 dtx_decide 返回 NULL 且不留痕 ✓"

# ── B. COMMIT 决议返回即已达多数派 ──
B=$(q "$LEADER_PORT" \
     "SELECT partdist.dtx_decide(${GID}, ${DTX_OK}, 1, ARRAY[${GID}, 777]::bigint[]);")
[[ "$B" == "1" ]] || fail "B: dtx_decide(COMMIT) 应返回 1，实际 '${B}'"

# 决议返回时 leader 上必然已有；follower 的 apply 可能稍滞后，给有界时间
for port in "$LEADER_PORT" "${FOLLOWER_PORTS[@]}"; do
  OID=$(q "$port" "SELECT partdist.local_partition_for_shard(${GID});")
  [[ -n "$OID" && "$OID" != "0" ]] || fail "B: 节点 ${port} 解析不到本地分片 OID"
  ok=0
  for _ in $(seq 1 20); do
    REC=$(q "$port" "SELECT kind||'|'||dtxid||'|'||verdict||'|'||participants::text
            FROM partdist.partwal_read_dtx_record(${OID}::oid,
                 (SELECT decided_plsn FROM partdist.dtx_decision WHERE dtxid=${DTX_OK}));")
    IDX=$(q "$port" "SELECT verdict||'|'||participants::text
            FROM partdist.dtx_decision WHERE dtxid=${DTX_OK};")
    if [[ "$REC" == "2|${DTX_OK}|1|{${GID},777}" && "$IDX" == "1|{${GID},777}" ]]; then ok=1; break; fi
    sleep 1
  done
  (( ok == 1 )) \
    || fail "B: 节点 ${port} 上决议未落到位（DTX 记录='${REC:-无}' 索引='${IDX:-无}'）——决议未在多数派持久化"
done
echo "raft_20 B: COMMIT 决议返回后，DECISION 记录与索引在协调组全部 ${#MEMBER_PORTS[@]} 个成员上均在 ✓"

# ── C. 决议槽一次性 ──
C=$(q "$LEADER_PORT" "SELECT partdist.dtx_decide(${GID}, ${DTX_OK}, 2, NULL);")
[[ "$C" == "1" ]] \
  || fail "C: 已决议 COMMIT 的事务再决议 ABORT 应仍返回 1（决议槽一次性），实际 '${C}'"
echo "raft_20 C: 决议槽一次性，已有决议不被覆盖 ✓"

# ── D. 推定中止 ──
D=$(q "$LEADER_PORT" "SELECT partdist.dtx_status(${GID}, ${DTX_PRESUMED});")
[[ "$D" == "2" ]] || fail "D: 查无决议时 dtx_status 应推定中止返回 2，实际 '${D}'"
DROW=$(q "$LEADER_PORT" "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX_PRESUMED};")
[[ "$DROW" == "2" ]] \
  || fail "D: 推定中止必须**先写 ABORT 决议**再答复，索引表里却没有（实际 '${DROW:-无}'）"
D2=$(q "$LEADER_PORT" "SELECT partdist.dtx_decide(${GID}, ${DTX_PRESUMED}, 1, NULL);")
[[ "$D2" == "2" ]] \
  || fail "D: 推定中止之后再决议 COMMIT 必须仍得 2（否则同一事务两个矛盾结论），实际 '${D2}'"
echo "raft_20 D: 推定中止先写 ABORT 再答复，此后 COMMIT 无法翻盘 ✓"

# ── E. 协调组切主后决议仍可查 ──
OLD_LEADER="$LEADER_PORT"
node_ctl "$(port_dir "$OLD_LEADER")" stop -m fast
NEW_LEADER=""
for _ in $(seq 1 40); do
  NEW_LEADER=$(find_leader "$OLD_LEADER" || true)
  [[ -n "$NEW_LEADER" ]] && break
  sleep 1
done
if [[ -z "$NEW_LEADER" ]]; then
  node_ctl "$(port_dir "$OLD_LEADER")" -l "/work/pg-cluster-data/$(port_dir "$OLD_LEADER").log" start -w -t 30
  fail "E: 停掉协调组 leader 后 40s 内没有新 leader 当选"
fi
E=$(q "$NEW_LEADER" "SELECT partdist.dtx_status(${GID}, ${DTX_OK});")
E2=$(q "$NEW_LEADER" "SELECT partdist.dtx_status(${GID}, ${DTX_PRESUMED});")
node_ctl "$(port_dir "$OLD_LEADER")" -l "/work/pg-cluster-data/$(port_dir "$OLD_LEADER").log" start -w -t 30
sleep 2
[[ "$E" == "1" ]] \
  || fail "E: 协调组切主后（旧 leader ${OLD_LEADER} → 新 ${NEW_LEADER}）原 COMMIT 决议应仍可查得 1，实际 '${E}'——协调权未随选举转移"
[[ "$E2" == "2" ]] \
  || fail "E: 协调组切主后原 ABORT 决议应仍可查得 2，实际 '${E2}'"
echo "raft_20 E: 协调组切主（${OLD_LEADER} → ${NEW_LEADER}）后两笔决议均可查，协调权随 Raft 选举自动转移 ✓"

# ── F. 回执与 FORGET（§9.7 决议 GC）──
# acked 收齐（⊇ participants）后 leader 写 FORGET 记录复制到多数派，
# 每个成员 apply 它时**同步**删除本地决议行 —— 删除走与写入相同的复制路径。
LEADER_PORT=""
for _ in $(seq 1 30); do
  LEADER_PORT=$(find_leader || true)
  [[ -n "$LEADER_PORT" ]] && break
  sleep 1
done
[[ -n "$LEADER_PORT" ]] || fail "F: 协调组没有 leader"
FD=$(q "$LEADER_PORT" "SELECT partdist.dtx_decide(${GID}, ${DTX_FGT}, 1, ARRAY[${GID}, 777002]::bigint[]);")
[[ "$FD" == "1" ]] || fail "F: 预置决议失败（'${FD}'）"

# 部分回执：行还在，acked 记下已回执的组
A1=$(q "$LEADER_PORT" "SELECT partdist.dtx_ack(${GID}, ${DTX_FGT}, ARRAY[${GID}]::bigint[]);")
[[ "$A1" == "t" ]] || fail "F: 第一笔回执应被接受，实际 '${A1}'"
ACKED=$(q "$LEADER_PORT" "SELECT acked::text FROM partdist.dtx_decision WHERE dtxid=${DTX_FGT};")
[[ "$ACKED" == "{${GID}}" ]] \
  || fail "F: 部分回执后 acked 应为 {${GID}}，实际 '${ACKED}'（行不应被删——777002 还没回执）"

# 收齐：FORGET 复制出去，行在**全部成员**上消失
A2=$(q "$LEADER_PORT" "SELECT partdist.dtx_ack(${GID}, ${DTX_FGT}, ARRAY[777002]::bigint[]);")
[[ "$A2" == "t" ]] || fail "F: 第二笔回执应被接受，实际 '${A2}'"
for port in "${MEMBER_PORTS[@]}"; do
  ok=0
  for _ in $(seq 1 20); do
    [[ "$(q "$port" "SELECT count(*) FROM partdist.dtx_decision WHERE dtxid=${DTX_FGT};")" == "0" ]] \
      && { ok=1; break; }
    sleep 1
  done
  [[ $ok -eq 1 ]] \
    || fail "F: 回执收齐后成员 ${port} 上的决议行应随 FORGET 的 apply 删除，20s 未删"
done

# 行已不存在时回执返回 true（幂等闭环），且不复活任何行
A3=$(q "$LEADER_PORT" "SELECT partdist.dtx_ack(${GID}, ${DTX_FGT}, ARRAY[${GID}]::bigint[]);")
[[ "$A3" == "t" ]] || fail "F: 已 FORGET 的决议再回执应返回 true，实际 '${A3}'"
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM partdist.dtx_decision WHERE dtxid=${DTX_FGT};")" == "0" ]] \
  || fail "F: 迟到回执不得复活决议行"
echo "raft_20 F: 回执部分收齐不删、收齐即 FORGET、全体成员同步回收、迟到回执幂等 ✓"

cleanup
echo "raft_20 PASS"
exit 0
