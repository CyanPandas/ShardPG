#!/usr/bin/env bash
# raft_25: 控制面日志压缩 + InstallSnapshot（计划文档 §4 阶段 1 的两个 ❌ / §12.4 #7）
#
# 独立可跑：CONTAINER=pg-citus-raft-container bash .../raft_25_snapshot_compaction.sh
# 也被 run-raft-tests.sh 调用（退出码 0=PASS，非 0=FAIL，原因在末行）。
#
# ── 被验收的缺口 ─────────────────────────────────────────────────────
# partdist.raft_log 此前只增不删（一轮回归就 240+ 行，重启时还要全表读回），
# 而 partdist.raft_snapshot 虽然每次 apply 都在写，却**没有任何消费者** ——
# 既没有 InstallSnapshot RPC，也没有按 last_included_index 删行。
# 即"有快照内容、无快照机制"。这两件是一件事的两半：压缩之后被删掉的那一段
# 只能靠快照传输，快照也只有在会压缩之后才谈得上必要。
#
# ── 判据 ─────────────────────────────────────────────────────────────
#   A. 压缩真的发生：基点 base_index/base_term 推进、raft_log 里 <= 基点的行
#      确实没了、快照行的 (last_included_index, last_included_term) 与基点**成对**
#      一致（配错对的话接收方的 prev 一致性检查会永久错位）；压缩后新提案照常提交。
#   B. **落后到压缩点之前的成员靠快照追平**：停一个 follower → 在 leader 上推进到
#      它缺的那一段已被删除 → 重启它 → 窗口内追平，且它的 base_index 也跟着推进
#      （> 它停机时的 last_log_index）。这一条**只可能由快照达成**：那些条目在
#      leader 上已经不存在，逐条补是补不出来的。
#   C. 装完快照状态机一致：node_map / partition_map 与 leader 逐行相同
#      （快照 apply 侧是整表替换，不是 upsert）。
#   D. 基点跨重启不丢（hardstate v3）：重启该成员后 base_index 不回退，
#      且新提案仍能正常复制过去。
#   E. 压缩之后**重启不丢日志尾巴**：全节点重启后新条目不得落在已存在的 index 上。
#      这一条有 in-build 对照——日志恢复若按"last_log_index > 0 就早退"（压缩前的
#      写法），重启后 last_log_index 会停在基点，新条目直接覆盖已提交位置。
#
# 真对照（离线执行，不在套件里）：把 send_install_snapshot() 改成直接返回 false
# 重编 .so，同一用例 A 段照过、B 段确定性失败（follower 永远停在停机时的进度）。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-citus-raft-container}"
BASE_PORT="${BASE_PORT:-5432}"
N_WORKERS="${N_WORKERS:-8}"
LAST_PORT=$(( BASE_PORT + N_WORKERS ))
PGCTL="docker exec -u postgres $CONTAINER /work/pg-install/bin/pg_ctl"
psql_at() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$1" -U postgres "${@:2}"; }
q() { psql_at "$1" -tAc "$2" 2>/dev/null || true; }

SCRATCH_NODE=91          # 只用于制造控制面日志条目，不在 node_map 里，不影响拓扑
# node_map.status 有 CHECK 约束（active/down/syncing）——提案的 payload 必须过约束，
# 否则 apply 会抛错。那条路径本身也是缺陷（抛错会冻住 apply 游标），已单独修掉，
# 但用例不该拿非法值去制造日志。

port_dir() { if [[ "$1" == "$BASE_PORT" ]]; then echo coordinator; else echo "worker$(( $1 - BASE_PORT ))"; fi; }
node_stop() { $PGCTL stop -D "/work/pg-cluster-data/$(port_dir "$1")" -m fast >/dev/null 2>&1 || true; }
node_start() {
  docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_isready -q -h localhost -p "$1" 2>/dev/null && return 0
  $PGCTL start -D "/work/pg-cluster-data/$(port_dir "$1")" -o "-p $1" -w >/dev/null 2>&1 || true
}
alive() { docker exec -u postgres "$CONTAINER" /work/pg-install/bin/pg_isready -q -h localhost -p "$1" 2>/dev/null; }

set_guc() {   # $1=GUC 名 $2=值，全存活节点 reload 生效
  local port
  for port in $(seq "$BASE_PORT" "$LAST_PORT"); do
    alive "$port" || continue
    psql_at "$port" -v ON_ERROR_STOP=1 -q -c "ALTER SYSTEM SET $1 = $2" >/dev/null 2>&1 \
      || fail "set_guc：节点 ${port} 设置 $1 失败"
    q "$port" "SELECT pg_reload_conf();" >/dev/null
  done
}

cleanup() {
  local port
  for port in $(seq "$BASE_PORT" "$LAST_PORT"); do node_start "$port"; done
  for port in $(seq "$BASE_PORT" "$LAST_PORT"); do
    psql_at "$port" -q -c "ALTER SYSTEM RESET pg_raft.compact_threshold" >/dev/null 2>&1 || true
    psql_at "$port" -q -c "ALTER SYSTEM RESET pg_raft.catchup_interval_ms" >/dev/null 2>&1 || true
    q "$port" "SELECT pg_reload_conf();" >/dev/null
    q "$port" "DELETE FROM partdist.node_map WHERE node_id = ${SCRATCH_NODE};" >/dev/null
  done
}
fail() { cleanup; echo "raft_25 FAIL: $1"; exit 1; }

g0() {   # $1=port $2=列名
  q "$1" "SELECT $2 FROM partdist.pg_raft_group_status() WHERE group_id = 0;"
}
# 判"删干净"要数 <= 基点的行，不能拿 min(log_index) 比：全部条目都已 apply 并压缩
# 时表里一行不剩，min 返回 NULL（COALESCE 成 0），反而会被误判成"没删干净"。
stale_rows() { q "$1" "SELECT count(*) FROM partdist.raft_log WHERE group_id = 0 AND log_index <= $2;"; }
snap_of() { q "$1" "SELECT last_included_index||'/'||last_included_term FROM partdist.raft_snapshot;"; }
state_fp() {  # 状态机指纹
  # 只取**由日志决定**的列。node_map.last_heartbeat / partition_map.updated_at 是
  # 各节点 apply 时的本地 now()，天生逐节点不同（TopologyMonitor 也会单独写它），
  # 拿它们比对是在测一个设计上从未承诺的东西。
  q "$1" "SELECT md5(
            COALESCE((SELECT string_agg(n.node_id||'|'||n.hostname||'|'||n.port||'|'||n.status, ',' ORDER BY n.node_id)
                        FROM partdist.node_map n), '')
         || COALESCE((SELECT string_agg(p.partition_id||'|'||p.primary_node||'|'||p.secondary_nodes::text||'|'||p.primary_term, ',' ORDER BY p.partition_id)
                        FROM partdist.partition_map p), ''));"
}

find_leader() {   # 控制面 leader 的端口；$1=最长等待秒数（缺省 45）
  # 必须带等待窗口：停/起节点之后控制面要重新选举，几秒内本来就没有 leader。
  local port i secs=${1:-45}
  for i in $(seq 1 "$secs"); do
    for port in $(seq "$BASE_PORT" "$LAST_PORT"); do
      alive "$port" || continue
      [[ "$(g0 "$port" state)" == "leader" ]] && { echo "$port"; return 0; }
    done
    sleep 1
  done
  return 1
}

# 提交 $1 条控制面日志。**领导权会漂移**（本实现无 PreVote，且协调节点有选举偏置），
# 而 set_guc 要在 9 个节点上跑 ALTER SYSTEM+reload，中间足够改选一次；提案打到非
# leader 只会返回 0。所以失败就重新发现 leader 再试，全局 LEADER 随之更新 ——
# 这不是放宽判据：本用例测的是压缩与快照，不是领导权稳定性。
propose_n() {  # $1=条数
  # 分批发（每批 20 条一条 SQL）：逐条发 200 次 docker exec 太慢，而一次性发 200 条
  # 又会把压缩产生的快照写/删行关在一个长事务里 —— shmem 里的基点已经推进、表里的
  # 快照行却还没提交，此刻若有成员来装快照就会读到旧行而白跑一趟。
  local want=$1 got=0 tries=0 max=20 out lp n
  while (( got < want && tries < max )); do
    tries=$(( tries + 1 ))
    n=$(( want - got )); (( n > 20 )) && n=20
    out=$(q "$LEADER" "SELECT min(x) FROM (SELECT partdist.pg_raft_propose_node_status(${SCRATCH_NODE},
              CASE WHEN g % 2 = 0 THEN 'active' ELSE 'down' END) AS x
            FROM generate_series(1, ${n}) g) t;")
    if [[ "$out" =~ ^[0-9]+$ && "$out" -gt 0 ]]; then got=$(( got + n )); continue; fi
    lp=$(find_leader 10) && LEADER="$lp" || sleep 1
  done
  (( got >= want ))
}

cleanup

LEADER=$(find_leader) || fail "控制面 30s 内没有 leader"
# 追平通道调快一点，快照安装挂在它上面
set_guc pg_raft.catchup_interval_ms 1000

# ── A：压缩真的发生，且快照的 (index, term) 与基点成对 ───────────────────
set_guc pg_raft.compact_threshold 5
BASE0=$(g0 "$LEADER" base_index)
propose_n 12 || fail "A：控制面提案失败（leader=${LEADER}）"
sleep 2
BASE1=$(g0 "$LEADER" base_index)
BTERM1=$(g0 "$LEADER" base_term)
[[ "$BASE1" =~ ^[0-9]+$ && "$BASE1" -gt "${BASE0:-0}" ]] \
  || fail "A：压缩没有发生（base_index ${BASE0} → ${BASE1}）"
[[ "$BTERM1" =~ ^[0-9]+$ && "$BTERM1" -gt 0 ]] || fail "A：base_term 未记录（${BTERM1}）"

STALE=$(stale_rows "$LEADER" "$BASE1")
[[ "$STALE" == "0" ]] \
  || fail "A：raft_log 里仍有 ${STALE} 行 <= 基点 ${BASE1} —— 压缩没删干净"

SNAP=$(snap_of "$LEADER")
[[ "$SNAP" == "${BASE1}/${BTERM1}" ]] \
  || fail "A：快照的 (index,term)=${SNAP} 与基点 (${BASE1},${BTERM1}) 不成对"

# 压缩之后照常工作
LLI_A=$(g0 "$LEADER" last_log_index)
propose_n 1 || fail "A：压缩后新提案失败"
LLI_B=$(g0 "$LEADER" last_log_index)
[[ "$LLI_B" -gt "$LLI_A" ]] || fail "A：压缩后 last_log_index 不再推进（${LLI_A} → ${LLI_B}）"

# ── B：落后到压缩点之前的成员靠快照追平 ────────────────────────────────
VICTIM=""
for p in $(seq $((BASE_PORT + 1)) "$LAST_PORT"); do
  [[ "$p" == "$LEADER" ]] && continue
  alive "$p" && { VICTIM="$p"; break; }
done
[[ -n "$VICTIM" ]] || fail "B：挑不出非 leader 的存活节点"

V_BEFORE=$(g0 "$VICTIM" last_log_index)
[[ "$V_BEFORE" =~ ^[0-9]+$ ]] || fail "B：取不到 ${VICTIM} 的 last_log_index"
node_stop "$VICTIM"
sleep 2

LEADER=$(find_leader) || fail "B：停掉 ${VICTIM} 后控制面无 leader"
# 必须推进 **超过环容量（RAFT_LOG_CAPACITY=128）**：压缩只删 partdist.raft_log 的行，
# shmem 环里最近 128 条还在，leader 照样能逐条补 —— 首版只推进 30 条，结果把
# send_install_snapshot() 摘掉的对照构建也照样通过（假测试，2026-08-05 实测抓到）。
# 环绕过去之后，那些条目才真的在 leader 上不复存在，只剩快照一条路。
propose_n 200 || fail "B：停掉一个 follower 后提案失败（多数派应仍在）"
sleep 2
BASE2=$(g0 "$LEADER" base_index)
LLI2=$(g0 "$LEADER" last_log_index)
[[ "$BASE2" -gt "$V_BEFORE" ]] \
  || fail "B：基点 ${BASE2} 未越过 ${VICTIM} 停机时的进度 ${V_BEFORE}，构造无效（行还在，逐条也能补）"
[[ $(( LLI2 - V_BEFORE )) -gt 128 ]] \
  || fail "B：只推进了 $(( LLI2 - V_BEFORE )) 条，未超过环容量 128，构造无效（环里还留着，逐条也能补）"

node_start "$VICTIM"
CONVERGED=0
for i in $(seq 1 90); do
  LLI_L=$(g0 "$LEADER" last_log_index)
  LLI_V=$(g0 "$VICTIM" last_log_index)
  BASE_V=$(g0 "$VICTIM" base_index)
  if [[ "$LLI_V" == "$LLI_L" && "${BASE_V:-0}" -gt "$V_BEFORE" ]]; then CONVERGED=1; break; fi
  sleep 1
done
[[ "$CONVERGED" == "1" ]] \
  || fail "B：${VICTIM} 未在 90s 内经快照追平（last_log_index ${LLI_V:-?}/${LLI_L:-?} base ${BASE_V:-?} 停机时 ${V_BEFORE}）"

# ── C：装完快照状态机一致 ───────────────────────────────────────────────
FP_L=$(state_fp "$LEADER"); FP_V=""
for i in $(seq 1 30); do
  FP_V=$(state_fp "$VICTIM")
  [[ -n "$FP_V" && "$FP_V" == "$FP_L" ]] && break
  sleep 1
  FP_L=$(state_fp "$LEADER")
done
[[ -n "$FP_V" && "$FP_V" == "$FP_L" ]] \
  || fail "C：${VICTIM} 的状态机与 leader 不一致（快照 apply 侧未整表替换？）"

# ── D：基点跨重启不丢，且之后仍能正常复制 ──────────────────────────────
BASE_V1=$(g0 "$VICTIM" base_index)
node_stop "$VICTIM"; sleep 1; node_start "$VICTIM"; sleep 3
BASE_V2=$(g0 "$VICTIM" base_index)
[[ "$BASE_V2" =~ ^[0-9]+$ && "$BASE_V2" -ge "$BASE_V1" ]] \
  || fail "D：重启后基点回退（${BASE_V1} → ${BASE_V2}）—— hardstate 没把基点持久化"

LEADER=$(find_leader) || fail "D：重启后控制面无 leader"
propose_n 3 || fail "D：重启后提案失败"
OK=0
for i in $(seq 1 60); do
  [[ "$(g0 "$VICTIM" last_log_index)" == "$(g0 "$LEADER" last_log_index)" ]] && { OK=1; break; }
  sleep 1
done
[[ "$OK" == "1" ]] || fail "D：重启后 ${VICTIM} 不再跟上新条目"

# ── E：压缩之后重启，日志尾巴不能丢（Leader Completeness 前置）──────────
# 压缩把 <= 基点的行删了，重启时 hardstate 会先把 last_log_index 顶到基点。
# 如果日志恢复还按"last_log_index > 0 就说明已经有日志了"早退，表里 base+1..N
# 那段尾巴就永远灌不回来 —— 重新当选的 leader 会以为自己的日志止于基点，
# 拿新内容去覆盖**已经提交**的 base+1.. 位置。
LEADER=$(find_leader) || fail "E：无 leader"
# 先把阈值顶高再造尾巴：阈值还是 5 的话，这 3 条 apply 完就又触发一次压缩，
# 基点直接追到日志末端 —— 尾巴长度变成 0，用例就成了空转（集群安静时必然如此，
# 只有恰好有后台探测补上新条目才侥幸成立）。基点在 A 段已经推进过，这里冻住它。
set_guc pg_raft.compact_threshold 100000
propose_n 3 || fail "E：提案失败"
sleep 2
LLI_E=$(g0 "$LEADER" last_log_index)
BASE_E=$(g0 "$LEADER" base_index)
[[ "$LLI_E" -gt "$BASE_E" ]] \
  || fail "E：基点 ${BASE_E} 已等于日志末端 ${LLI_E}，没有尾巴可丢，构造无效"

for p in $(seq "$BASE_PORT" "$LAST_PORT"); do node_stop "$p"; done
for p in $(seq "$BASE_PORT" "$LAST_PORT"); do node_start "$p"; done
LEADER=$(find_leader 60) || fail "E：全节点重启后 60s 内没有 leader"

# 判据是"新条目不得复用已存在的 index"。不写死成 LLI_E+1：重启后 TopologyMonitor
# 的探测本来就可能自己提几条（节点状态由 down 变回 active）。
NEXT=$(q "$LEADER" "SELECT partdist.pg_raft_propose_node_status(${SCRATCH_NODE}, 'down');")
[[ "$NEXT" =~ ^[0-9]+$ && "$NEXT" -gt "$LLI_E" ]] \
  || fail "E：重启后新条目落在 index=${NEXT}，而重启前日志已到 ${LLI_E}（基点 ${BASE_E}）—— 日志尾巴丢了，新内容会覆盖已提交条目"

cleanup
echo "raft_25 PASS: 控制面压缩+快照（A 压缩生效且 index/term 成对 / B 越过压缩点靠快照追平 / C 状态机一致 / D 基点跨重启不丢 / E 压缩后重启不丢日志尾巴）"
exit 0
