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
#   E. 登记缺失的 citus gid（接线关闭期产生等）⇒ 按 Citus 规则闭合，
#      不允许永久滞留（Citus 自己的恢复已被 §9.4 关掉，没有别人收尾）。
#   F. Citus 规则的提交侧：master 的 pg_dist_transaction 里有该 gid ⇒ 提交。
#   G. initiator 存活栅栏：发起 backend 还活着 ⇒ 绝不推定中止
#      （防"慢 master"被误回滚后 master 又本地提交 ⇒ 分叉）。
#   H. 自动接线：TopologyMonitor 守护按 dtx_recover_interval_ms 自行闭合，
#      无需任何手工 SQL —— master 崩溃后 in-doubt 的兜底方。
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

# ── dtxid 必须每轮唯一（2026-08-09 审查，同 raft_20）────────────────────
# 旧版写死 910001..910005，清理走吞错的 q()。上一轮残留的决议行会让 B 段
# （从未决议 ⇒ 推定中止）读到旧的 verdict=2 而"通过"，真正的写路径不被执行；
# C/D 段"不该产生决议"的断言也会被残留行直接判失败或误判。
# 加运行时唯一前缀，并在夹具阶段逐成员断言这些 dtxid 干净。
DTX_BASE=$(( 900000000 + ($(date +%s) % 9000000) * 16 + 4 ))
DTX_C=$(( DTX_BASE + 0 ))   # 有 COMMIT 决议
DTX_A=$(( DTX_BASE + 1 ))   # 从未决议 → 推定中止
DTX_F=$(( DTX_BASE + 2 ))   # 未超时，不该被动
DTX_U=$(( DTX_BASE + 3 ))   # 协调组不可达
DTX_H=$(( DTX_BASE + 4 ))   # H 段：BGW 守护自动闭合

cleanup() {
  local port gid
  for port in "${MEMBER_PORTS[@]}"; do
    d=$(port_dir "$port")
    node_ctl "$d" -l "/work/pg-cluster-data/${d}.log" start -w -t 30
  done
  sleep 1
  for port in "${MEMBER_PORTS[@]}"; do
    for g in $(q "$port" "SELECT gid FROM pg_prepared_xacts
                           WHERE strpos(gid,'shardpg_dtx_')=1
                              OR gid ~ '^citus_0_[0-9]+_(77700|88800)[0-9]_0$';"); do
      q "$port" "ROLLBACK PREPARED '${g}';" >/dev/null
    done
    q "$port" "DELETE FROM partdist.dtx_decision WHERE dtxid IN (${DTX_C},${DTX_A},${DTX_F},${DTX_U},${DTX_H});" >/dev/null
    q "$port" "ALTER SYSTEM RESET pg_raft.dtx_recover_timeout_ms;" >/dev/null
    q "$port" "SELECT pg_reload_conf();" >/dev/null
    if [[ -n "$GID" ]]; then
      q "$port" "SELECT partdist.pg_raft_group_drop(${GID});" >/dev/null
      q "$port" "DELETE FROM partdist.partition_map WHERE partition_id = ${GID}::oid;" >/dev/null
      q "$port" "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}_${GID};" >/dev/null
    fi
  done
  q "$BASE_PORT" "DELETE FROM pg_dist_transaction WHERE gid ~ '^citus_0_[0-9]+_(77700|88800)[0-9]_0$';" >/dev/null
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

# ── 夹具前提：五个 dtxid 在**每个成员**上都必须查无此行 ──
PRE_CHECKED=0
for port in "${MEMBER_PORTS[@]}"; do
  for d in "$DTX_C" "$DTX_A" "$DTX_F" "$DTX_U" "$DTX_H"; do
    n=$(q "$port" "SELECT count(*) FROM partdist.dtx_decision WHERE dtxid=${d};")
    [[ "$n" =~ ^[0-9]+$ ]] \
      || fail "夹具：节点 ${port} 上读 dtx_decision 失败（返回 '${n}'），无法确认 dtxid 干净"
    [[ "$n" == "0" ]] \
      || fail "夹具：节点 ${port} 上 dtxid=${d} 已有 ${n} 行残留 —— 判据会被上一轮结果污染"
    PRE_CHECKED=$((PRE_CHECKED + 1))
  done
done
(( PRE_CHECKED == ${#MEMBER_PORTS[@]} * 5 )) \
  || fail "夹具：dtxid 干净性只检查了 ${PRE_CHECKED} 项，覆盖不全"
echo "raft_21 夹具: dtxid ${DTX_C}..${DTX_H} 在全部成员上均无残留 ✓"

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

# ── E. 登记缺失的 citus gid：发起者已死、master 无提交记录 ⇒ 按 Citus 规则回滚 ──
# 覆盖"prepared 事务产生时接线是关的（dtx_2pc_enabled=off）"等无登记形态。
# 修复前这类 gid 在恢复循环里被 continue 跳过 —— Citus 自己的恢复已被 §9.4
# 关掉，从此**没有任何人**收尾，prepared 事务连同行锁永久滞留。
DEAD_PID=$(q "$BASE_PORT" "SELECT coalesce(max(pid),1)+100000 FROM pg_stat_activity;")
GID_E="citus_0_${DEAD_PID}_777001_0"
psql_at "$LEADER_PORT" -q -c \
  "BEGIN; INSERT INTO ${SHARD_TBL} VALUES (1005, 'e');
   PREPARE TRANSACTION '${GID_E}';" >/dev/null 2>&1 \
  || fail "E: 造 citus-gid prepared 事务失败"
# ★ 手工 PREPARE 也会被参与者接线自动登记（分片表触达非空）——夹具必须显式
# 删掉登记行，"登记缺失"的前提才真的成立。没有这一步，本段在修复前的构建上
# 也能通过（走的是"有登记、coord 为 NULL"的常规 Citus 规则路径），测不到
# "无登记 ⇒ continue ⇒ 永久滞留"那个缺陷（2026-08-04 真对照实验抓出）。
q "$LEADER_PORT" "DELETE FROM partdist.dtx_participant WHERE gid='${GID_E}';" >/dev/null
q "$BASE_PORT" "DELETE FROM pg_dist_transaction WHERE gid='${GID_E}';" >/dev/null
q "$LEADER_PORT" "SELECT partdist.dtx_recover_prepared(0);" >/dev/null
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='${GID_E}';")" == "0" ]] \
  || fail "E: 登记缺失的 citus prepared 事务未被闭合（修复前的永久滞留形态）"
[[ "$(row_visible 1005)" == "0" ]] \
  || fail "E: master 无提交记录，应按 Citus 规则回滚"
echo "raft_21 E: 登记缺失的 citus gid ⇒ 按 Citus 规则推定中止，不再永久滞留 ✓"

# ── F. Citus 规则的提交侧：master 的 pg_dist_transaction 里有该 gid ⇒ 提交 ──
GID_F="citus_0_${DEAD_PID}_777002_0"
psql_at "$LEADER_PORT" -q -c \
  "BEGIN; INSERT INTO ${SHARD_TBL} VALUES (1006, 'f');
   PREPARE TRANSACTION '${GID_F}';" >/dev/null 2>&1 \
  || fail "F: 造 prepared 事务失败"
q "$BASE_PORT" "INSERT INTO pg_dist_transaction (groupid, gid) VALUES (0, '${GID_F}');" >/dev/null
q "$LEADER_PORT" "SELECT partdist.dtx_recover_prepared(0);" >/dev/null
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='${GID_F}';")" == "0" ]] \
  || fail "F: 未闭合"
[[ "$(row_visible 1006)" == "1" ]] \
  || fail "F: master 已提交（pg_dist_transaction 有行），参与者应提交"
q "$BASE_PORT" "DELETE FROM pg_dist_transaction WHERE gid='${GID_F}';" >/dev/null
echo "raft_21 F: master 有提交记录 ⇒ 参与者提交、数据可见 ✓"

# ── G. initiator 存活栅栏：发起 backend 还活着 ⇒ 绝不推定中止 ──
# pg_dist_transaction 的行在 master 本地提交前不可见；master 只是**慢**时，
# 凭"行不可见"回滚参与者 = 与 master 随后的本地提交分叉。Citus 自己的恢复靠
# 共享内存里的活跃分布式事务号拦这个窗口；我们关了它就得自己拦（用 gid 里编的
# 发起者 pid 查 master 的 pg_stat_activity）。用 master 的 checkpointer pid
# 当"永远活着的发起者"——确定性、无竞态。
LIVE_PID=$(q "$BASE_PORT" "SELECT pid FROM pg_stat_activity WHERE backend_type='checkpointer' LIMIT 1;")
[[ -n "$LIVE_PID" ]] || fail "G: 取不到 master 的 checkpointer pid"
GID_G="citus_0_${LIVE_PID}_777003_0"
psql_at "$LEADER_PORT" -q -c \
  "BEGIN; INSERT INTO ${SHARD_TBL} VALUES (1007, 'g');
   PREPARE TRANSACTION '${GID_G}';" >/dev/null 2>&1 \
  || fail "G: 造 prepared 事务失败"
q "$LEADER_PORT" "SELECT partdist.dtx_recover_prepared(0);" >/dev/null
[[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='${GID_G}';")" == "1" ]] \
  || fail "G: 发起 backend 还活着（pid=${LIVE_PID}），恢复守护不得动它 —— 慢 master 会被误推定中止"
q "$LEADER_PORT" "ROLLBACK PREPARED '${GID_G}';" >/dev/null
echo "raft_21 G: 发起者存活 ⇒ 保持 prepared 不动（防慢 master 分叉提交）✓"

# ── H. 自动接线：不手工调用，TopologyMonitor 的守护自行闭合 ──
# 恢复 partition_map（D 把它指向了不存在的节点 97）
for port in "${MEMBER_PORTS[@]}"; do
  q "$port" "UPDATE partdist.partition_map SET primary_node=${LEADER_NODE}
              WHERE partition_id=${GID}::oid;" >/dev/null
done
q "$LEADER_PORT" "ALTER SYSTEM SET pg_raft.dtx_recover_timeout_ms = 1000;" >/dev/null
q "$LEADER_PORT" "SELECT pg_reload_conf();" >/dev/null
make_prepared "$DTX_H" 1008 || fail "H: 造 prepared 事务失败"
DEC=$(q "$LEADER_PORT" "SELECT partdist.dtx_decide(${GID}, ${DTX_H}, 1, ARRAY[${GID}]::bigint[]);")
[[ "$DEC" == "1" ]] || fail "H: 预置 COMMIT 决议失败（返回 '${DEC}'）"
H_OK=0
for _ in $(seq 1 45); do
  [[ "$(q "$LEADER_PORT" "SELECT count(*) FROM pg_prepared_xacts WHERE gid='shardpg_dtx_${DTX_H}_${GID}';")" == "0" ]] \
    && { H_OK=1; break; }
  sleep 1
done
q "$LEADER_PORT" "ALTER SYSTEM RESET pg_raft.dtx_recover_timeout_ms;" >/dev/null
q "$LEADER_PORT" "SELECT pg_reload_conf();" >/dev/null
[[ "$H_OK" == "1" ]] \
  || fail "H: 45s 内 BGW 守护没有自动闭合（自动接线未生效 —— master 挂掉后 in-doubt 将永久滞留）"
[[ "$(row_visible 1008)" == "1" ]] || fail "H: 自动闭合应按决议提交"
echo "raft_21 H: 守护自动闭合，无需任何手工调用 ✓"

# ── I. pg_dist_transaction 的 GC（§9.7）──
# Citus 的恢复兼任这张表的 GC，被 §9.4 关掉后由本项目接管。删除条件缺一不可：
# 发起 backend 已死 && 所有节点确认无该 gid 的 prepared 事务；任一节点不可达
# ⇒ 整轮放弃。它是 citus 规则的真相源，删错一行 = 把等收尾的事务错判成 ABORT。
GID_I1="citus_0_${DEAD_PID}_888001_0"    # 发起者已死、无 prepared ⇒ 应删
GID_I2="citus_0_${LIVE_PID}_888002_0"    # 发起者活着 ⇒ 必须保留
GID_I3="citus_0_${DEAD_PID}_888003_0"    # 有 prepared 在 ⇒ 必须保留
q "$BASE_PORT" "INSERT INTO pg_dist_transaction (groupid, gid) VALUES
                (0,'${GID_I1}'), (0,'${GID_I2}'), (0,'${GID_I3}');" >/dev/null
psql_at "$LEADER_PORT" -q -c \
  "BEGIN; INSERT INTO ${SHARD_TBL} VALUES (1009, 'i');
   PREPARE TRANSACTION '${GID_I3}';" >/dev/null 2>&1 \
  || fail "I: 造 prepared 事务失败"

# GC 每轮最多处理 128 个候选（守护是分批周期制）；表里可能有历史积压，
# 有界循环直到夹具行被处理到或无法推进。
for _ in $(seq 1 6); do
  N=$(q "$BASE_PORT" "SELECT partdist.dtx_gc_dist_transaction();")
  [[ "$N" =~ ^[0-9]+$ ]] || fail "I: GC 返回 '${N}'（全节点在线时不应放弃）"
  [[ "$(q "$BASE_PORT" "SELECT count(*) FROM pg_dist_transaction WHERE gid='${GID_I1}';")" == "0" ]] && break
  [[ "$N" == "0" ]] && break
done
R1=$(q "$BASE_PORT" "SELECT count(*) FROM pg_dist_transaction WHERE gid='${GID_I1}';")
R2=$(q "$BASE_PORT" "SELECT count(*) FROM pg_dist_transaction WHERE gid='${GID_I2}';")
R3=$(q "$BASE_PORT" "SELECT count(*) FROM pg_dist_transaction WHERE gid='${GID_I3}';")
[[ "$R1" == "0" ]] || fail "I: 发起者已死且全网无 prepared 的行应被删除"
[[ "$R2" == "1" ]] || fail "I: 发起者还活着的行绝不能删（master 可能还没提交完）"
[[ "$R3" == "1" ]] || fail "I: 仍有 prepared 事务的行绝不能删（那是它收尾的真相源）"

# 节点不可达 ⇒ 整轮放弃（保守不删）
GID_I4="citus_0_${DEAD_PID}_888004_0"
q "$BASE_PORT" "INSERT INTO pg_dist_transaction (groupid, gid) VALUES (0,'${GID_I4}');" >/dev/null
node_ctl worker8 stop -m fast
sleep 1
N2=$(q "$BASE_PORT" "SELECT partdist.dtx_gc_dist_transaction();")
node_ctl worker8 -l "/work/pg-cluster-data/worker8.log" start -w -t 30
[[ "$N2" == "-1" ]] \
  || fail "I: 有节点不可达时 GC 必须整轮放弃（返回 -1），实际 '${N2}'"
[[ "$(q "$BASE_PORT" "SELECT count(*) FROM pg_dist_transaction WHERE gid='${GID_I4}';")" == "1" ]] \
  || fail "I: 节点不可达时不得删除任何行（无法确认 prepared 是否存在）"
q "$LEADER_PORT" "ROLLBACK PREPARED '${GID_I3}';" >/dev/null
q "$BASE_PORT" "DELETE FROM pg_dist_transaction WHERE gid IN ('${GID_I2}','${GID_I3}','${GID_I4}');" >/dev/null
echo "raft_21 I: dist_transaction GC——已死+全网无 prepared 才删；发起者活着/有 prepared/节点不可达都保守不删 ✓"

cleanup
echo "raft_21 PASS"
exit 0
