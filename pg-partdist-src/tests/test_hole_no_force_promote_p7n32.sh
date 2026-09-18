#!/usr/bin/env bash
# [宿主机] P7-N32 回归（故障注入）：回放流有空洞的节点当选后，兜底也不得把它登记成主。
#
# 缺陷：promote_prepare_ex 对"追平失败"一律当成"还没追完"——RETURN 0，满 60 s 转兜底 force，
#   照做其余升主步骤后返回 1、登记成主。流内空洞是**永久**性的（等多久都补不上），force 登记的
#   就是明知残缺的数据。实测（N25 回归，2026-09-18）：原始主 A 重供后空洞追不上，被切回，60 s 后
#   force 登记，协调者只读到 120 行（应为 180）；随后在册主 A 的心跳修复把残缺基线推给健康的 B/C。
# 修法：追平报"流内空洞"即 RETURN -1（兜底也不放行）⇒ BGW 主动让位，由追平了的成员当选。
#
# 构造（自然空洞是偶发的，这里注入）：X=placement 原始主，A=由副本升上来的主（有回放槽位），B=新主。
#   A 当主写一批 → 受控切到 B（A 降级、槽位 armed）→ 经 B 写第 1 批 → B 上 pg_switch_wal()
#   → 经 B 写第 2 批（落进新的分区流段）→ A 追平到第 2 批之前后，删掉 A 上装着第 1 批的那个
#   分区流段文件 —— 第 2 批仍在，A 的回放必报"流内空洞"。
# 断言：反复受控切向 A（每 5 s 一次、共 CAMPAIGN_S 秒，足够跨过 60 s 兜底期限）后，A **始终没登记**；
#   A 日志有"回放流有空洞…拒绝升主"；最终在册主不是 A，经协调者读到**全部**行（不丢数）。
# 负向对照：换回修前 promote_prepare_ex ⇒ A 60 s 后被 force 登记、行数变少，本用例必红。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ROWS="${ROWS:-40}"; CAMPAIGN_S="${CAMPAIGN_S:-150}"
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
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_partdist.auto_reprovision_demoted" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn32_${SID}'::regclass)" >/dev/null 2>&1; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn32" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn32_${SID}" >/dev/null 2>&1; done
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

pm() { Q $COORD "SELECT primary_node||' '||primary_term FROM partdist.partition_map WHERE partition_id=$SID"; }
state_of() { Q $1 "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$SID"; }
lo_of() { Q $1 "SELECT partdist.local_partition_for_shard($SID)"; }
regto() { local t; for t in $(seq 1 90); do [[ "$(pm)" == "${NID[$1]} "* ]] && { echo ok; return; }; sleep 2; done; echo no; }
sw() { local t; for t in $(seq 1 120); do [[ "$(state_of $2)" == leader ]] && { echo leader; return; }; (( t % 5 == 1 )) && Q $2 "SELECT partdist.pg_raft_group_campaign($1)" >/dev/null; sleep 1; done; echo "$(state_of $2)"; }

echo "========== [1] 夹具：rn32 单分片、3 成员组；X 原始主 → A（由副本升上来）→ B =========="
# 关掉 P7-N25 自动归队：否则 A 让位后别的成员一登记，就会把 failed 的 A 重供治好、A 随后合法登记，
# 分不清"兜底被挡住"与"被治好了"。本用例只验 N32 这一道闸。
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.auto_reprovision_demoted = off" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn32;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE rn32(id int primary key, pad text);
SELECT create_distributed_table('rn32', 'id', colocate_with => 'none');
SQL
read SID X < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn32'::regclass" </dev/null | tr '|' ' ')
for p in "${WORKERS[@]}"; do [[ $p != $X ]] && { if [[ -z "$A" ]]; then A=$p; elif [[ -z "$B" ]]; then B=$p; fi; }; done
echo "  分片 $SID：X=:$X  A=:$A（将被注入空洞）  B=:$B"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 X" "$(build_group_on $SID $X "$ALLMEM")" "leader"
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for p in "${WORKERS[@]}"; do [[ $p == $X ]] && continue
  r=$(PSQL $X -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:100}")" "ok"; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "登记 X 为主" "$(regto $X)" "ok"
Q $COORD "INSERT INTO rn32 SELECT g, repeat('x', 120) FROM generate_series(1, $ROWS) g" >/dev/null
check "受控切到 A" "$(sw $SID $A)" "leader"; check "登记 A 为主" "$(regto $A)" "ok"
Q $COORD "INSERT INTO rn32 SELECT g, repeat('a', 120) FROM generate_series($((ROWS+1)), $((ROWS*2))) g" >/dev/null
check "受控切到 B（A 降级）" "$(sw $SID $B)" "leader"; check "登记 B 为主" "$(regto $B)" "ok"
loA=$(lo_of $A); sleep 3
check "A 降级后持有 armed 回放槽位" "$(Q $A "SELECT armed FROM partdist.replay_status() WHERE shard=$loA")" "t"

echo "========== [2] 经 B 写两批（中间 pg_switch_wal），在 A 上删掉第 1 批所在分区流段 ⇒ 注入空洞 =========="
# 分区流段按记录 orig_lsn 所在 pg_wal 段命名：B 上先切一次 WAL，第 1 批才会落进一个**新**段
# （首跑没切，第 1 批追加进了已有段，没有可删的新段，注入落空）。
Q $B "SELECT pg_switch_wal()" >/dev/null; Q $B "CHECKPOINT" >/dev/null
segs0=$(DEX bash -c "ls '${DDIR[$A]}/pg_parwal/$loA/' | grep -E '^[0-9A-F]{24}$' | sort" </dev/null)
Q $COORD "INSERT INTO rn32 SELECT g, repeat('b', 120) FROM generate_series($((ROWS*2+1)), $((ROWS*3))) g" >/dev/null
sleep 2; segs1=$(DEX bash -c "ls '${DDIR[$A]}/pg_parwal/$loA/' | grep -E '^[0-9A-F]{24}$' | sort" </dev/null)
Q $B "SELECT pg_switch_wal()" >/dev/null; Q $B "CHECKPOINT" >/dev/null
Q $COORD "INSERT INTO rn32 SELECT g, repeat('c', 120) FROM generate_series($((ROWS*3+1)), $((ROWS*4))) g" >/dev/null
sleep 2; segs2=$(DEX bash -c "ls '${DDIR[$A]}/pg_parwal/$loA/' | grep -E '^[0-9A-F]{24}$' | sort" </dev/null)
new1=$(comm -13 <(echo "$segs0") <(echo "$segs1")); new2=$(comm -13 <(echo "$segs1") <(echo "$segs2"))
echo "  第 1 批新段：${new1:-无}   第 2 批新段：${new2:-无}"
check "第 1 / 第 2 批各落在不同的新段里（能注入出'后面还有记录'的空洞）" "$([[ -n "$new1" && -n "$new2" ]] && echo ok)" "ok"
TOTAL=$((ROWS*4))
check "注入前经协调者读到全部 $TOTAL 行" "$(Q $COORD "SELECT count(*) FROM rn32")" "$TOTAL"
for f in $new1; do DEX rm -f "${DDIR[$A]}/pg_parwal/$loA/$f" </dev/null; done
fB=$(Q $B "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($SID))")
Q $A "SELECT partdist.replay_catchup('rn32_${SID}'::regclass, $fB)" >/dev/null 2>&1
hole=$(PSQL $A -Atc "SELECT partdist.replay_catchup('rn32_${SID}'::regclass, $fB)" </dev/null 2>&1 | grep -c '流内空洞')
check "A 的回放报流内空洞（注入生效）" "$([[ "${hole:-0}" -gt 0 ]] && echo ok)" "ok"
if [[ "${hole:-0}" -eq 0 ]]; then
  echo "  [夹具] 注入没生效，后面的断言没有意义，跳过（上面已记 FAIL）"
else

echo "========== [3] 反复受控切向 A（${CAMPAIGN_S}s，跨过 60 s 兜底期限）：A 不得登记 =========="
amark=$(DEX bash -c "wc -l < '${LOGF[$A]}'" </dev/null)
everA=""; for t in $(seq 1 $CAMPAIGN_S); do
  [[ "$(pm)" == "${NID[$A]} "* ]] && { everA=1; break; }
  (( t % 5 == 1 )) && Q $A "SELECT partdist.pg_raft_group_campaign($SID)" >/dev/null; sleep 1; done
check "[3] ★ A 始终没被登记为主（兜底也不放行）" "$([[ -z "$everA" ]] && echo ok || echo registered)" "ok"
refused=$(DEX bash -c "tail -n +$((amark+1)) '${LOGF[$A]}' | grep -c '回放流有空洞'" </dev/null)
check "[3] A 日志有'回放流有空洞…拒绝升主'" "$([[ "${refused:-0}" -gt 0 ]] && echo ok)" "ok"

echo "========== [4] 数据不丢：最终在册主不是 A，经协调者读到全部行 =========="
fin=""; for t in $(seq 1 90); do c=$(pm); p0=${c%% *}; if [[ -n "$p0" && "$p0" != "${NID[$A]}" ]]; then
  for p in "${WORKERS[@]}"; do [[ "${NID[$p]}" == "$p0" && "$(state_of $p)" == leader ]] && fin=$p; done; fi
  [[ -n "$fin" ]] && break; sleep 2; done
check "[4] 有非 A 的在册主且它是 raft leader" "$([[ -n "$fin" ]] && echo ok)" "ok"
cnt=""; for t in $(seq 1 15); do cnt=$(Q $COORD "SELECT count(*) FROM rn32"); [[ -n "$cnt" ]] && break; sleep 1; done
check "[4] ★ 经协调者读到全部 $TOTAL 行（不丢数）" "$cnt" "$TOTAL"
fi

echo "========== [5] 健康 =========="
health_check_no_crash

echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N32 回归：通过" || echo "P7-N32 回归：存在 FAIL"
exit $(( FAIL > 0 ))
