#!/usr/bin/env bash
# [宿主机] P7-N21 回归：显式拆掉的组，不许被在途 RPC 按 hearsay 再建出来。
#
# 病灶（09-18 实测）：拆组是节点本地动作；leader 的心跳/RV 一到，已拆的节点按通告把组再建成
#   空壳（日志空、last_data_plsn=0）。清场时该节点正在 DROP 这组分片表的壳，提交路径上的 DROP
#   通知经"僵尸组"复制 ⇒ 从 plsn 1 起把整条分区流历史重提，一条 DROP 卡 13～56 分钟。
# 修法：pg_raft_group_drop / group_reset 记墓碑（10 分钟）；hearsay 建组见墓碑即拒（回 "0 0"）；
#   显式 pg_raft_group_create 清墓碑。
#
# 断言：
#   [2] 只在 C 上拆组、leader A 照常发心跳 WAIT_S 秒 ⇒ C 上不复活，C 日志没有"创建 Raft 组 SID"。
#   [3] 同一条合成心跳（pg_raft_append_entries）的对照：从没存在过的组 G ⇒ hearsay **会**建出来
#       （证明这条路径是通的、本用例不空转）；拆掉 G 后同一调用 ⇒ 回 "0 0"、不建。对 SID 同样不建。
#   [4] 显式 pg_raft_group_create 在 C 上把组合法重建，追平 leader 日志、认 A 为 leader。
#   [5] 清场：拆组后 DROP TABLE 在 30 s 内返回（N21 的外在症状）。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
WAIT_S="${WAIT_S:-15}"; G=""
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
  [[ -n "$G" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.pg_raft_group_drop($G)" >/dev/null; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  # 先表后组；每条 DROP 套 statement_timeout（P7-N21）。副本残壳被路由守卫拒时，拆组后再删一遍。
  [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn21_${SID}'::regclass)" >/dev/null 2>&1; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn21" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn21_${SID}" >/dev/null 2>&1; done
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
    Q $rp "SELECT partdist.replay_catchup('rn21_${SID}'::regclass, $tp)" >/dev/null 2>&1; sleep 1
  done
  echo "${a}/${tp}"
}
eof_errors() { local p=$1; DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'unexpected data beyond EOF'" </dev/null; }
# 主 lp 与副本 rp 的某关系（主堆或主键索引）掩码外逐字节比对
rel_cmp() { local lp=$1 rp=$2 kind=$3 sql lpath rpath
  if [[ $kind == heap ]]; then sql="SELECT pg_relation_filepath('rn21_${SID}')"
  else sql="SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid='rn21_${SID}'::regclass AND indisprimary"; fi
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

echo "========== [1] 夹具：rn21 单分片、3 成员组，主在 A =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn21;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE rn21(id int primary key, pad text);
SELECT create_distributed_table('rn21', 'id', colocate_with => 'none');
SQL
read SID A < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn21'::regclass" </dev/null | tr '|' ' ')
for p in "${WORKERS[@]}"; do [[ $p != $A ]] && { if [[ -z "$B" ]]; then B=$p; elif [[ -z "$C" ]]; then C=$p; fi; }; done
echo "  分片 $SID：A=:$A（leader）、B=:$B、C=:$C（只在它上面拆组）"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 A" "$(build_group_on $SID $A "$ALLMEM")" "leader"
Q $COORD "INSERT INTO rn21 SELECT g, 'x' FROM generate_series(1, 20) g" >/dev/null
for t in $(seq 1 20); do [[ "$(Q $C "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=$SID")" == 1 ]] && break; sleep 1; done
check "C 上组 $SID 在" "$(Q $C "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=$SID")" "1"

echo "========== [2] 只在 C 上拆组，A 照常心跳 ${WAIT_S}s =========="
cmark=$(DEX bash -c "wc -l < '${LOGF[$C]}'" </dev/null)
check "C 上拆组返回 t" "$(Q $C "SELECT partdist.pg_raft_group_drop($SID)")" "t"
sleep $WAIT_S
check "[2] A 仍是组 $SID 的 leader（心跳一直在发）" "$(Q $A "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$SID")" "leader"
check "[2] ★ C 上组 $SID 没被 hearsay 复活" "$(Q $C "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=$SID")" "0"
check "[2] C 日志里没有再建组 $SID" "$(DEX bash -c "tail -n +$((cmark+1)) '${LOGF[$C]}' | grep -c '创建 Raft 组 $SID（'" </dev/null)" "0"

echo "========== [3] 合成心跳对照：从没存在过的组会被 hearsay 建出来，拆过的不会 =========="
G=$(( 990000000 + SID % 1000000 ))
check "[3] 对照组 $G 事先不存在" "$(Q $C "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=$G")" "0"
Q $C "SELECT partdist.pg_raft_append_entries(1, ${NID[$A]}, 0, 0, 0, p_group_id => $G)" >/dev/null
check "[3] 对照：同一条合成心跳把没拆过的组 $G 建出来了（hearsay 路径是通的）" "$(Q $C "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=$G")" "1"
Q $C "SELECT partdist.pg_raft_group_drop($G)" >/dev/null
r=$(Q $C "SELECT partdist.pg_raft_append_entries(1, ${NID[$A]}, 0, 0, 0, p_group_id => $G)")
check "[3] ★ 拆掉 $G 后同一调用被拒（回 '0 0'）" "$r" "0 0"
check "[3] ★ 且 $G 没被建出来" "$(Q $C "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=$G")" "0"
term=$(Q $A "SELECT current_term FROM partdist.pg_raft_group_status() WHERE group_id=$SID")
r=$(Q $C "SELECT partdist.pg_raft_append_entries($term, ${NID[$A]}, 0, 0, 0, p_group_id => $SID)")
check "[3] ★ 对拆过的 $SID 发合成心跳同样被拒" "$r" "0 0"
check "[3] $SID 在 C 上仍不存在" "$(Q $C "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=$SID")" "0"

echo "========== [4] 显式建组清墓碑：C 合法重建、追平 leader =========="
check "[4] C 上显式建组返回 t" "$(Q $C "SELECT partdist.pg_raft_group_create($SID, $ALLMEM)")" "t"
Q $COORD "INSERT INTO rn21 SELECT g, 'y' FROM generate_series(21, 30) g" >/dev/null
st=""; for t in $(seq 1 60); do
  la=$(Q $A "SELECT last_log_index FROM partdist.pg_raft_group_status() WHERE group_id=$SID")
  st=$(Q $C "SELECT state||'/'||leader_node_id||'/'||(last_log_index >= ${la:-0})::text FROM partdist.pg_raft_group_status() WHERE group_id=$SID")
  [[ "$st" == "follower/${NID[$A]}/true" ]] && break; sleep 1; done
check "[4] C 以 follower 身份认 A 为 leader、日志追平（state/leader/追平）" "$st" "follower/${NID[$A]}/true"

echo "========== [5] 清场时 DROP TABLE 不再卡住 =========="
for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done
sleep 2
ts=$(date +%s)
Q $COORD "SET statement_timeout='120s'; DROP TABLE IF EXISTS rn21" >/dev/null 2>&1
dt=$(( $(date +%s) - ts ))
echo "  DROP TABLE 用时 ${dt}s"
check "[5] DROP TABLE 30 s 内返回" "$([[ $dt -lt 30 ]] && echo ok)" "ok"
check "[5] 表已删" "$(Q $COORD "SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::text='rn21'")" "0"
ng=0; for p in "${WORKERS[@]}"; do sleep 0; n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ng=$((ng+${n:-0})); done
sleep 5
ng2=0; for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ng2=$((ng2+${n:-0})); done
check "[5] 拆组 + 删表后 5 s 内没有组被复活" "$ng/$ng2" "0/0"

echo "========== [6] 健康 =========="
health_check_no_crash

echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N21 回归：通过" || echo "P7-N21 回归：存在 FAIL"
exit $(( FAIL > 0 ))
