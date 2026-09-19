#!/usr/bin/env bash
# [宿主机] P7-N31 回归：切主登记生效的一瞬，协调者路由已翻到新主、新主自己那份登记还没 apply，
#   读闸门不得拒读（旧行为：`不允许在本节点上对副本壳表…执行查询`，亚秒级读失败）。
#
# 修法：升主前置返回 1 时标记"升主在途"15 s（partdist.shard_mark_promotion_pending）；读闸门
#   见到在途标记就等本地登记 apply 把"已升主"置上（有上限、可中断），等到即放行。
#
# 构造（确定性地把窗口撑大）：在新主 T 上开一个事务 `SELECT … FROM partdist.partition_map
#   WHERE partition_id=<分片> FOR UPDATE` 并持锁 HOLD_S 秒 —— T 的 group0 apply 要 UPSERT 这一行，
#   被挡住；协调者照常 apply、把路由翻到 T。这 HOLD_S 秒里经协调者读，查询全落在 T、闸门还关着。
# 断言：路由翻到 T 的那一刻 T 本地登记确实还没生效（窗口构造成功）；此后每 200 ms 读一次、
#   共 HOLD_S+6 秒：**0 次被拒**、最终读到全部行。
# 负向对照：NEG=1 时只在 T 上换一版不打 N31 标记的 promote_prepare_ex ⇒ 断言窗口内确有拒读（清理时换回）。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ROWS1="${ROWS1:-50}"; HOLD_S="${HOLD_S:-5}"; NEG="${NEG:-0}"; NEG_APPLIED=0; PPX_SQL="$(cd "$(dirname "$0")" && pwd)/.n31_ppx_$$.sql"
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
Q()    { PSQL "$1" -Atc "$2" </dev/null 2>/dev/null | tail -1; }
check() {
  if [[ -z "$2" ]]; then echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置 =========="
WORKERS=($(PSQL $COORD -Atc "SELECT nodeport FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive ORDER BY nodeport" </dev/null))
NW=${#WORKERS[@]}
check "至少 3 个 worker" "$([[ $NW -ge 3 ]] && echo ok)" "ok"
declare -A NID DDIR LOGF LOGMARK
for p in "${WORKERS[@]}"; do
  NID[$p]=$(Q $p "SHOW pg_raft.node_id"); DDIR[$p]=$(Q $p "SHOW data_directory")
  LOGF[$p]=$(DEX bash -c "ls -t '${DDIR[$p]}/pg.log' '${DDIR[$p]}.log' 2>/dev/null | head -1" </dev/null)
  LOGMARK[$p]=$(DEX bash -c "wc -l < '${LOGF[$p]}'" </dev/null)
done
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
ng=0; for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ng=$((ng+${n:-0})); done
check "净场：无残留数据组" "$ng" "0"
SID=""; A=""; B=""; C=""
cleanup() {
  local p r
  Q ${T:-0} "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'n31lock'" >/dev/null 2>&1
  if [[ "$NEG_APPLIED" == 1 ]]; then
    { echo "SET citus.enable_ddl_propagation=off;"; cat "$PPX_SQL"; echo ";"; } | PSQL $B -q -v ON_ERROR_STOP=1 >/dev/null && echo "  [复原] T 上的 promote_prepare_ex 已换回带 N31 标记的版本"; rm -f "$PPX_SQL"
  fi
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  # 先表后组；每条 DROP 套 statement_timeout（P7-N21）。副本残壳被路由守卫拒时，拆组后再删一遍。
  [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn31_${SID}'::regclass)" >/dev/null 2>&1; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn31" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn31_${SID}" >/dev/null 2>&1; done
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
# 受控切主，只动这一个组（pg_raft_group_campaign）
switch_to() { local gid=$1 target=$2 t st=""
  for t in $(seq 1 120); do
    st=$(Q $target "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid"); [[ "$st" == leader ]] && break
    (( t % 5 == 1 )) && Q $target "SELECT partdist.pg_raft_group_campaign($gid)" >/dev/null
    sleep 1
  done; echo "$st"; }
registered() { local gid=$1 target=$2 t
  for t in $(seq 1 90); do [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$gid")" == "${NID[$target]}" ]] && { echo ok; return; }; sleep 2; done; echo "no"; }
lo_of() { Q $1 "SELECT partdist.local_partition_for_shard($SID)"; }
flush_of() { Q $1 "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($SID))"; }
# 让副本 rp 回放追到主 lp 的 flush；回显最终 applied（取不到回显空串）
catch_up() { local lp=$1 rp=$2 tp lo a="" t
  tp=$(flush_of $lp); lo=$(lo_of $rp)
  for t in $(seq 1 90); do
    a=$(Q $rp "SELECT applied FROM partdist.replay_status() WHERE shard=$lo")
    [[ -n "$a" && -n "$tp" && "$a" -ge "$tp" ]] && break
    Q $rp "SELECT partdist.replay_catchup('rn31_${SID}'::regclass, $tp)" >/dev/null 2>&1; sleep 1
  done
  echo "${a}/${tp}"
}
eof_errors() { local p=$1; DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'unexpected data beyond EOF'" </dev/null; }
# 主 lp 与副本 rp 的某关系（主堆或主键索引）掩码外逐字节比对
rel_cmp() { local lp=$1 rp=$2 kind=$3 sql lpath rpath
  if [[ $kind == heap ]]; then sql="SELECT pg_relation_filepath('rn31_${SID}')"
  else sql="SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid='rn31_${SID}'::regclass AND indisprimary"; fi
  PSQL $lp -q -c "CHECKPOINT;" </dev/null >/dev/null 2>&1; PSQL $rp -q -c "CHECKPOINT;" </dev/null >/dev/null 2>&1; sleep 1
  lpath=$(Q $lp "SET citus.override_table_visibility=false; $sql")
  rpath=$(Q $rp "SET citus.enable_ddl_propagation=off; $sql")
  [[ -z "$lpath" || -z "$rpath" ]] && { echo ""; return; }
  DEX python3 /tmp/pagecmp.py --kind=$kind "${DDIR[$lp]}/$lpath" "${DDIR[$rp]}/$rpath" </dev/null 2>/dev/null | tail -1
}
verify_replica() { local tag=$1 lp=$2 rp=$3 cu a tp
  cu=$(catch_up $lp $rp); a=${cu%/*}; tp=${cu#*/}
  check "$tag：:$rp 回放追到 :$lp 的 flush（$cu）" "$([[ -n "$a" && -n "$tp" && "$a" -ge "$tp" ]] && echo ok)" "ok"
  check "$tag：:$rp 日志无 unexpected data beyond EOF" "$(eof_errors $rp)" "0"
  check "$tag：主堆掩码外逐字节一致" "$(rel_cmp $lp $rp heap)" "IDENTICAL_OUTSIDE_HOLE"
  check "$tag：主键索引掩码外逐字节一致" "$(rel_cmp $lp $rp btree)" "IDENTICAL_OUTSIDE_HOLE"
}

echo "========== [1] 夹具：rn31 单分片、3 成员组 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn31;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE rn31(id int primary key, pad text);
SELECT create_distributed_table('rn31', 'id', colocate_with => 'none');
SQL
read SID A < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn31'::regclass" </dev/null | tr '|' ' ')
for p in "${WORKERS[@]}"; do [[ $p != $A ]] && { if [[ -z "$B" ]]; then B=$p; elif [[ -z "$C" ]]; then C=$p; fi; }; done
echo "  分片 $SID：原始主 A=:$A，新主 T=:$B"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 A" "$(build_group_on $SID $A "$ALLMEM")" "leader"
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for p in "${WORKERS[@]}"; do [[ $p == $A ]] && continue
  r=$(PSQL $A -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:100}")" "ok"; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "登记 A 为主" "$(registered $SID $A)" "ok"
Q $COORD "INSERT INTO rn31 SELECT g, repeat('a', 100) FROM generate_series(1, $ROWS1) g" >/dev/null
check "写入 $ROWS1 行" "$(Q $COORD "SELECT count(*) FROM rn31")" "$ROWS1"
T=$B; sleep 2; catch_up $A $T >/dev/null

echo "========== [2] 在 T 上持 partition_map 该行的行锁（${HOLD_S}s 墙钟），挡住 T 自己那份登记的 apply；受控切到 T =========="
if [[ "$NEG" == 1 ]]; then
  # 负向对照：只在 T 上换一版**不打"升主在途"标记**的 promote_prepare_ex（清理时换回）
  PSQL $B -Atc "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='partdist' AND p.proname='pg_raft_promote_prepare_ex'" </dev/null > "$PPX_SQL" 2>/dev/null
  if ! grep -q shard_mark_promotion_pending "$PPX_SQL"; then echo "  取 T 上 promote_prepare_ex 定义失败 / 里面没有 N31 标记"; exit 2; fi
  { echo "SET citus.enable_ddl_propagation=off;"; grep -v shard_mark_promotion_pending "$PPX_SQL"; echo ";"; } | PSQL $B -q -v ON_ERROR_STOP=1 >/dev/null
  check "负向对照：T 上的定义已去掉 N31 标记" "$(Q $B "SELECT count(*) FROM pg_proc WHERE proname='pg_raft_promote_prepare_ex' AND prosrc LIKE '%shard_mark_promotion_pending%'")" "0"
  NEG_APPLIED=1
  echo "  [负向对照] T 上的 promote_prepare_ex 已去掉 N31 标记"
fi
PSQL $T -q </dev/null >/dev/null 2>&1 <<SQL &
SET application_name = 'n31lock';
BEGIN;
SELECT 1 FROM partdist.partition_map WHERE partition_id = $SID FOR UPDATE;
SELECT pg_sleep(120);
COMMIT;
SQL
LOCKPID=$!
sleep 1
check "T 上的行锁已持有" "$(Q $T "SELECT count(*) FROM pg_stat_activity WHERE application_name='n31lock' AND state='active'")" "1"
Q $T "SELECT partdist.pg_raft_group_campaign($SID)" >/dev/null
flip=""; for t in $(seq 1 300); do [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$SID")" == "${NID[$T]}" ]] && { flip=1; break; }
  (( t % 25 == 0 )) && Q $T "SELECT partdist.pg_raft_group_campaign($SID)" >/dev/null; sleep 0.2; done
check "协调者已把路由翻到 T" "$([[ -n "$flip" ]] && echo ok)" "ok"
tloc=$(Q $T "SELECT coalesce((SELECT primary_node::text FROM partdist.partition_map WHERE partition_id=$SID),'?')")
check "此刻 T 本地登记尚未生效（窗口构造成功：T 本地仍记 ${tloc}）" "$([[ "$tloc" != "${NID[$T]}" ]] && echo ok)" "ok"
# 行锁按墙钟在路由翻转后 HOLD_S 秒放掉（与读的快慢无关）
( sleep $HOLD_S; Q $T "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'n31lock'" >/dev/null ) &
RELPID=$!

echo "========== [3] 窗口内持续经协调者读（每 200 ms，墙钟 $((HOLD_S+6)) s），不得被拒 =========="
ok=0; refused=0; other=0; t0=$(date +%s); maxlat=0
while (( $(date +%s) - t0 < HOLD_S + 6 )); do
  a=$(date +%s%N)
  out=$(PSQL $COORD -Atc "SELECT count(*) FROM rn31" </dev/null 2>&1)
  lat=$(( ($(date +%s%N) - a) / 1000000 )); (( lat > maxlat )) && maxlat=$lat
  if [[ "$(echo "$out" | tail -1)" == "$ROWS1" ]]; then ok=$((ok+1))
  elif [[ "$out" == *副本壳表* ]]; then refused=$((refused+1)); echo "    [$(( $(date +%s) - t0 ))s] 被拒：$(echo "$out" | grep -m1 ERROR | cut -c1-120)"
  else other=$((other+1)); echo "    [$(( $(date +%s) - t0 ))s] 其它：$(echo "$out" | grep -m1 -E 'ERROR|FATAL' | cut -c1-160)"; fi
  sleep 0.2
done
wait $RELPID 2>/dev/null; kill $LOCKPID 2>/dev/null
echo "  读：成功 $ok、被拒 $refused、其它错误 $other；单次读最长 ${maxlat} ms（行锁在路由翻转后 ${HOLD_S} s 放掉）"
if [[ "$NEG" == 1 ]]; then
  check "[3] 负向对照：不打标记 ⇒ 窗口内确有拒读（旧行为复现）" "$([[ $refused -gt 0 ]] && echo ok)" "ok"
else
  check "[3] ★ 切主窗口内 0 次拒读" "$refused" "0"
  check "[3] 无其它错误" "$other" "0"
  check "[3] 窗口内的读被挡住等待（最长读 ≥ 1000 ms，证明确实走了等待路径）" "$([[ $maxlat -ge 1000 ]] && echo ok)" "ok"
fi
check "[3] 登记最终在 T 生效" "$(registered $SID $T)" "ok"
check "[3] 读到全部行" "$(Q $COORD "SELECT count(*) FROM rn31")" "$ROWS1"

echo "========== [4] 健康 =========="
health_check_no_crash

echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N31 回归：通过" || echo "P7-N31 回归：存在 FAIL"
exit $(( FAIL > 0 ))
