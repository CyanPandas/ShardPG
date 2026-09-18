#!/usr/bin/env bash
# [宿主机] P7-N23 回归：未交出主权的前任主再次当选，升主前置不得空转 60 s。
#
# 场景（2026-09-18，扩展验收 [9] 第 2 轮 FAIL 定因之一）：A 是在册主（本地"已升主"）；B 赢得选举
#   但还没登记就丢了领导权 ⇒ A 从没收到"主权已交给 B"，本地仍是已升主；A 再次当选时
#   promote_prepare_ex 见 bound > app 去 replay_catchup，而已升主的槽位一律拒
#   （"已升主，不再回放别人的流"）⇒ 每轮 RETURN 0，空转满 promote_catchup_deadline_ms（60 s）才兜底。
#
# 修法：槽位 promoted_clean（升主置真；降级或 follower append 真正落下他人写的 DATA/MARKER 置假）+
#   partdist.shard_promoted_selfheld(oid)；promote_prepare_ex 在 selfheld 为真时跳过那条注定失败的追平。
#
# 构造：[1] 末尾经 A 写一批并把 A 的 raft 已应用游标推过回放游标（合成 [9] 的 bound>app 前提，理由见该处）；
#   再 replay_disable B 的槽位、受控切到 B —— B 当选后升主前置 -1 主动让位、从不登记；随即受控切回 A。
#   必须亲眼看到 B 当选过、A 夺回时 partition_map 未变、selfheld=t、bound>app，才算构造成；3 次都没构造成只报不判。
# 断言：A 以新任期（primary_term 增加）重新登记用时 < REG_LIMIT_S（默认 30 s，修前 ≥ 60 s）；
#   A 日志出现"跳过追平"、没有"已升主，不再回放别人的流"；行数不变、可继续写。
# 负向对照：把 promote_prepare_ex 换回修前版本后本用例必红（修复验收时实测一次）。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ROWS="${ROWS:-40}"; ROWS2="${ROWS2:-30}"; REG_LIMIT_S="${REG_LIMIT_S:-30}"; TRIES="${TRIES:-3}"
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
Q()    { PSQL "$1" -Atc "$2" </dev/null 2>/dev/null | tail -1; }
check() {
  if [[ -z "$2" ]]; then echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置 =========="
WORKERS=($(PSQL $COORD -Atc "SELECT nodeport FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive ORDER BY nodeport" </dev/null))
NW=${#WORKERS[@]}
check "至少 3 个 worker" "$([[ $NW -ge 3 ]] && echo ok)" "ok"
declare -A NID DDIR LOGF
for p in "${WORKERS[@]}"; do
  NID[$p]=$(Q $p "SHOW pg_raft.node_id"); DDIR[$p]=$(Q $p "SHOW data_directory")
  LOGF[$p]=$(DEX bash -c "ls -t '${DDIR[$p]}/pg.log' '${DDIR[$p]}.log' 2>/dev/null | head -1" </dev/null)
done
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
ng=0; for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ng=$((ng+${n:-0})); done
check "净场：无残留数据组" "$ng" "0"
SID=""; X=""; A=""; B=""
cleanup() {
  local p r
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn23_${SID}'::regclass)" >/dev/null 2>&1; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn23" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn23_${SID}" >/dev/null 2>&1; done
  done
  echo "  [复原] 组已拆、表已删"
}
trap cleanup EXIT
build_group_on() {
  local gid=$1 want=$2 mem=$3 t p st=""
  for p in "${WORKERS[@]}"; do if [[ "$p" == "$want" ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null; else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 9000" >/dev/null; fi; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.pg_raft_group_create($gid, $mem)" >/dev/null; done
  for t in $(seq 1 40); do st=$(Q $want "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid"); [[ "$st" == leader ]] && break; sleep 1; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  echo "$st"
}
state_of() { Q $1 "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$SID"; }
lo_of() { Q $1 "SELECT partdist.local_partition_for_shard($SID)"; }
pm() { Q $COORD "SELECT primary_node||' '||primary_term FROM partdist.partition_map WHERE partition_id=$SID"; }

echo "========== [1] 夹具：rn23 单分片、3 成员组、A 在册主 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn23;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE rn23(id int primary key, pad text);
SELECT create_distributed_table('rn23', 'id', colocate_with => 'none');
SQL
# ★ A 必须是"由副本升上来的"在册主：N23 只在有回放槽位的节点上发生（原始 placement 主没有槽位，
#   promote_prepare_ex 走"无槽位"分支，根本不去追平）。实测 [9] 里夺回的 :5434 就是先当副本、第 1 轮升的主。
read SID X < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn23'::regclass" </dev/null | tr '|' ' ')
for p in "${WORKERS[@]}"; do [[ $p != $X ]] && { if [[ -z "$A" ]]; then A=$p; elif [[ -z "$B" ]]; then B=$p; fi; }; done
echo "  分片 $SID：原始主 X=:$X，由副本升上来的在册主 A=:$A，中途当选者 B=:$B"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 X=:$X" "$(build_group_on $SID $X "$ALLMEM")" "leader"
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for p in "${WORKERS[@]}"; do [[ $p == $X ]] && continue
  r=$(PSQL $X -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:100}")" "ok"; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
reg=""; for t in $(seq 1 90); do [[ "$(pm)" == "${NID[$X]} "* ]] && { reg=ok; break; }; sleep 2; done
check "登记 X 为主" "$reg" "ok"
Q $COORD "INSERT INTO rn23 SELECT g, repeat('a', 120) FROM generate_series(1, $ROWS) g" >/dev/null
check "写入 $ROWS 行" "$(Q $COORD "SELECT count(*) FROM rn23")" "$ROWS"
st=""; for t in $(seq 1 120); do st=$(state_of $A); [[ "$st" == leader ]] && break; (( t % 5 == 1 )) && Q $A "SELECT partdist.pg_raft_group_campaign($SID)" >/dev/null; sleep 1; done
check "受控切到 A" "$st" "leader"
reg=""; for t in $(seq 1 90); do [[ "$(pm)" == "${NID[$A]} "* ]] && { reg=ok; break; }; sleep 2; done
check "登记 A 为主（由副本升上来）" "$reg" "ok"
check "A 已升主且干净（shard_promoted_selfheld）" "$(Q $A "SELECT partdist.shard_promoted_selfheld($(lo_of $A)::oid)")" "t"
Q $COORD "INSERT INTO rn23 SELECT g, repeat('b', 120) FROM generate_series($((ROWS+1)), $((ROWS+ROWS2))) g" >/dev/null
check "经 A 再写 $ROWS2 行（A 自写的尾巴）" "$(Q $COORD "SELECT count(*) FROM rn23")" "$((ROWS+ROWS2))"
# ★ 合成 [9] 现场的前提：A 的 raft 已应用游标（bound = follower_applied_part_lsn）越过回放游标（app），
#   且越过的那一截**全是 A 自己当主时写的**。[9] 里 :5434 正是这个状态（连续 16 次"已升主，不再回放别人的流"），
#   但单组用例里两种自然构造都推不动它：leader 自写记录走 in_txn apply、不写这一列；中途当选者追加的 DTX
#   也没推动（09-18 实测）。这里用产品函数 follower_set_applied_part_lsn（单调推进）把它推到 A 的 flush ——
#   等价于"这些自写记录后来在 follower 身份下被 apply 了一遍"，正是 [9] 的成因推断。
loA=$(lo_of $A); fA=$(Q $A "SELECT partdist.get_partition_flush_lsn($loA)")
Q $A "SELECT partdist.follower_set_applied_part_lsn($loA, $fA)" >/dev/null
bnd0=$(Q $A "SELECT partdist.get_follower_applied_part_lsn($loA)"); app0=$(Q $A "SELECT applied FROM partdist.replay_status() WHERE shard=$loA")
echo "  合成后 A：bound=$bnd0 app=$app0 flush=$fA"
check "合成前提：A 的 bound > app" "$([[ -n "$bnd0" && -n "$app0" && "$bnd0" -gt "$app0" ]] && echo ok)" "ok"
check "合成前提：A 仍 selfheld（自写记录不经 follower append）" "$(Q $A "SELECT partdist.shard_promoted_selfheld($loA::oid)")" "t"

echo "========== [2] 构造：B 当选但登记不了（槽位 disarm ⇒ 升主前置 -1 让位），随即切回 A =========="
# ★ 确定性构造"中途当选、从未登记"的 B：先 replay_disable B 的槽位，B 赢选举后升主前置走
#   "收到分区 WAL 却无 armed 槽位"分支返回 -1、主动让位（N14），**不会登记** —— 与 [9] 实测里
#   :5435 的升主前置排队排不到号、没登记就丢主，对 A 而言是同一个局面：A 从没被告知交出主权。
#   首版靠"抢在 B 登记之前切回"碰时机，B 的 campaign 被 A 的心跳抹掉、根本没当选，却被误算成
#   "构造成功"（A 本来就是 leader）—— 所以这里**必须亲眼看到 B 当选过**才算数。
built=""; secs=""
for try in $(seq 1 $TRIES); do
  before=$(pm); term0=${before#* }
  amark=$(DEX bash -c "wc -l < '${LOGF[$A]}'" </dev/null)
  Q $B "SELECT partdist.replay_disable('rn23_${SID}'::regclass)" >/dev/null
  barmed=$(Q $B "SELECT armed FROM partdist.replay_status() WHERE shard=$(lo_of $B)")
  bwon=""
  for t in $(seq 1 120); do [[ "$(state_of $B)" == leader ]] && { bwon=1; break; }
    (( t % 12 == 1 )) && Q $B "SELECT partdist.pg_raft_group_campaign($SID)" >/dev/null; sleep 0.25; done
  # 模拟 [9] 里中途当选者在任期内往流里追加的非数据记录（converge() 每秒在各 worker 上跑 dtx_pending_sweep）：
  # 一条对不存在 dtxid 的 DTX FORGET，无害；A 作为 follower apply 它才可能把 bound 推过 app。
  [[ -n "$bwon" ]] && Q $B "SELECT partdist.partwal_append_dtx_record($(lo_of $B), 5, 424242424242, 1)" >/dev/null
  aled=""
  for t in $(seq 1 90); do [[ "$(state_of $A)" == leader ]] && { aled=1; break; }
    (( t % 6 == 1 )) && Q $A "SELECT partdist.pg_raft_group_campaign($SID)" >/dev/null; sleep 0.5; done
  t0=$(date +%s)
  held=$(Q $A "SELECT partdist.shard_promoted_selfheld($(lo_of $A)::oid)")
  mid=$(pm)
  echo "  第 $try 次：B 槽位 armed=$barmed、B 当选过=${bwon:-否}、A 夺回=${aled:-否}；partition_map 切前=($before) 夺回时=($mid)；A selfheld=$held"
  # ★ 还要 A 真有"要追的尾巴"（bound = follower_applied > app = 回放游标）——否则 promote_prepare_ex 根本不进
  #   追平分支，修前修后一样快，"用时 < 30 s"什么也证明不了（首跑 4 s PASS 就是这种空证）。
  loA=$(lo_of $A); bnd=$(Q $A "SELECT partdist.get_follower_applied_part_lsn($loA)"); app=$(Q $A "SELECT applied FROM partdist.replay_status() WHERE shard=$loA")
  echo "  第 $try 次：A 的 bound(follower_applied)=$bnd app(回放游标)=$app"
  if [[ -n "$bwon" && -n "$aled" && "$held" == t && "${mid%% *}" == "${NID[$A]}" && "${mid#* }" == "$term0" && -n "$bnd" && -n "$app" && "$bnd" -gt "$app" ]]; then built=ok; break; fi
  echo "  第 $try 次没构造成，等组稳定后重试"
  for t in $(seq 1 60); do [[ "$(pm)" == "${NID[$A]} "* && "$(state_of $A)" == leader ]] && break
    Q $A "SELECT partdist.pg_raft_group_campaign($SID)" >/dev/null; sleep 2; done
done
if [[ "$built" != ok ]]; then
  echo "  [只报] $TRIES 次都没构造出 N23 的完整前提（B 当选过且未登记、A 未降级夺回、selfheld=t、且 A 的 bound > app），本轮不判 N23"
  echo "         （前提由 [1] 末尾合成；这里没构造成通常是 B 当选/A 夺回的时机问题，看上面每次的明细）"
else
  echo "========== [3] A 以新任期重新登记：不得空转到 60 s 兜底 =========="
  newreg=""; for t in $(seq 1 120); do c=$(pm); [[ "${c%% *}" == "${NID[$A]}" && "${c#* }" -gt "$term0" ]] && { newreg=ok; break; }; sleep 1; done
  secs=$(( $(date +%s) - t0 ))
  check "A 以新任期重新登记" "$newreg" "ok"
  check "重新登记用时 ${secs}s < ${REG_LIMIT_S}s（修前 ≥ 60 s）" "$([[ -n "$newreg" && $secs -lt $REG_LIMIT_S ]] && echo ok)" "ok"
  skipped=$(DEX bash -c "tail -n +$((amark+1)) '${LOGF[$A]}' | grep -c '跳过追平'" </dev/null)
  refused=$(DEX bash -c "tail -n +$((amark+1)) '${LOGF[$A]}' | grep -c '已升主，不再回放别人的流'" </dev/null)
  check "A 日志出现'跳过追平'" "$([[ "${skipped:-0}" -gt 0 ]] && echo ok)" "ok"
  check "A 日志没有'已升主，不再回放别人的流'" "$refused" "0"
fi

echo "========== [4] 数据完整、可继续写 =========="
check "行数不变" "$(Q $COORD "SELECT count(*) FROM rn23")" "$((ROWS+ROWS2))"
Q $COORD "INSERT INTO rn23 VALUES ($((ROWS+ROWS2+1)), 'z')" >/dev/null
check "经协调者再写一行" "$(Q $COORD "SELECT count(*) FROM rn23")" "$((ROWS+ROWS2+1))"

echo "========== [5] 健康 =========="
health_check_no_crash

echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
if [[ $FAIL -gt 0 ]]; then echo "P7-N23 回归：存在 FAIL"
elif [[ "$built" != ok ]]; then echo "P7-N23 回归：**未判定**（前提没构造出来，上面的 PASS 只覆盖数据完整与健康，不说明 N23 修复有效）"
else echo "P7-N23 回归：通过"; fi
exit $(( FAIL > 0 ))
