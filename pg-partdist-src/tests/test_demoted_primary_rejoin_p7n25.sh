#!/usr/bin/env bash
# [宿主机] P7-N25 回归：被降级的原始主自动归队。
#
# 缺陷：原始 placement 主从没当过副本，没有回放槽位 / locmap。被新主登记降级后，它照收新主的流却
#   无从回放，按 R-P4-15 升主前置对它恒返回 -1 并主动让位 —— 没有任何路径替它重做基线，3 成员组
#   第一次切主后只剩一个合格候选（N22 回归首跑实测：切回原始主 ⇒ 拒绝升主、-1 让位）。
# 修法：新主登记那次 apply 拉起一次性工作者 → partdist.reprovision_demoted(gsid, 前任主)：
#   前任主没有 armed 槽位就 provision_shard_replica 给它重供基线；有就什么都不做。
#   GUC pg_partdist.auto_reprovision_demoted（默认 on）。
#
# 本用例（单分片原生表 rn25，3 成员组；A=原始 placement 主，B=新主）：
#   [1] A 为主、供 B/C 副本，写一批
#   [2] 受控切到 B（登记）—— A 被降级
#   [3] 不做任何人工供给，等 A 自动得到 armed 回放槽位、基线收尾、追平到 B 的 flush；
#       A 的堆与主键索引与 B 掩码外逐字节一致；A 日志有自动归队记录
#   [4] 经 B 再写一批，A 照常追平、仍逐字节一致
#   [5] 受控切回 A：必须当选并登记（修前 -1 让位，永远登记不上）；行数完整、可继续写
# 负向对照：AUTO=off（全节点 auto_reprovision_demoted=off）⇒ [3]/[5] 必红。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ROWS1="${ROWS1:-120}"; ROWS2="${ROWS2:-60}"; AUTO="${AUTO:-on}"; REJOIN_S="${REJOIN_S:-150}"
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
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_partdist.auto_reprovision_demoted" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  # 先表后组；每条 DROP 套 statement_timeout（P7-N21）。副本残壳被路由守卫拒时，拆组后再删一遍。
  [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn25_${SID}'::regclass)" >/dev/null 2>&1; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn25" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn25_${SID}" >/dev/null 2>&1; done
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
    Q $rp "SELECT partdist.replay_catchup('rn25_${SID}'::regclass, $tp)" >/dev/null 2>&1; sleep 1
  done
  echo "${a}/${tp}"
}
eof_errors() { local p=$1; DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'unexpected data beyond EOF'" </dev/null; }
# 主 lp 与副本 rp 的某关系（主堆或主键索引）掩码外逐字节比对
rel_cmp() { local lp=$1 rp=$2 kind=$3 sql lpath rpath
  if [[ $kind == heap ]]; then sql="SELECT pg_relation_filepath('rn25_${SID}')"
  else sql="SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid='rn25_${SID}'::regclass AND indisprimary"; fi
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

echo "========== [1] 夹具：rn25 单分片、3 成员组，A 为原始主（auto_reprovision_demoted=$AUTO）=========="
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.auto_reprovision_demoted = $AUTO" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 1
check "GUC 已按 AUTO=$AUTO 生效" "$(for p in "${WORKERS[@]}"; do Q $p "SHOW pg_partdist.auto_reprovision_demoted"; done | sort -u)" "$AUTO"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn25;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE rn25(id int primary key, pad text);
SELECT create_distributed_table('rn25', 'id', colocate_with => 'none');
ALTER TABLE rn25 SET (autovacuum_enabled = off);
SQL
read SID A < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn25'::regclass" </dev/null | tr '|' ' ')
for p in "${WORKERS[@]}"; do [[ $p != $A ]] && { if [[ -z "$B" ]]; then B=$p; elif [[ -z "$C" ]]; then C=$p; fi; }; done
echo "  分片 $SID：原始主 A=:$A，新主 B=:$B，另一副本 C=:$C"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 A=:$A" "$(build_group_on $SID $A "$ALLMEM")" "leader"
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for p in "${WORKERS[@]}"; do [[ $p == $A ]] && continue
  r=$(PSQL $A -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:100}")" "ok"; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "登记 A 为主" "$(registered $SID $A)" "ok"
Q $COORD "INSERT INTO rn25 SELECT g, repeat('a', 120) FROM generate_series(1, $ROWS1) g" >/dev/null
check "写入 $ROWS1 行" "$(Q $COORD "SELECT count(*) FROM rn25")" "$ROWS1"
check "A 是原始主：本分片没有回放槽位" "$(Q $A "SELECT count(*) FROM partdist.replay_status() WHERE shard=$(lo_of $A)")" "0"

echo "========== [2] 受控切到 B（A 被降级）=========="
amark=$(DEX bash -c "wc -l < '${LOGF[$A]}'" </dev/null); bmark=$(DEX bash -c "wc -l < '${LOGF[$B]}'" </dev/null)
check "B 当选组 $SID" "$(switch_to $SID $B)" "leader"
check "登记 B 为主" "$(registered $SID $B)" "ok"

echo "========== [3] 不做人工供给：A 自动得到回放槽位、追平、逐字节一致 =========="
armed=""; t0=$(date +%s)
for t in $(seq 1 $REJOIN_S); do armed=$(Q $A "SELECT armed FROM partdist.replay_status() WHERE shard=$(lo_of $A)"); [[ "$armed" == t ]] && break; sleep 1; done
echo "  A 得到 armed 回放槽位用时 $(( $(date +%s) - t0 )) s（armed=${armed:-无槽位}）"
check "[3] A 在 ${REJOIN_S}s 内自动得到 armed 回放槽位" "$armed" "t"
bp=""; for t in $(seq 1 60); do bp=$(Q $A "SELECT partdist.shard_baseline_pending($(lo_of $A)::oid)"); [[ "$bp" == f ]] && break; Q $A "SELECT partdist.replay_catchup('rn25_${SID}'::regclass, $(flush_of $B))" >/dev/null 2>&1; sleep 1; done
check "[3] A 基线已收尾（baseline_pending=f）" "$bp" "f"
nlog=$(DEX bash -c "tail -n +$((bmark+1)) '${LOGF[$B]}' | grep -c '自动归队'" </dev/null)
check "[3] 新主 B 日志有自动归队记录" "$([[ "${nlog:-0}" -gt 0 ]] && echo ok)" "ok"
verify_replica "[3] 自动归队后" $B $A

echo "========== [4] 经 B 再写 $ROWS2 行，A 照常追平 =========="
Q $COORD "INSERT INTO rn25 SELECT g, repeat('b', 120) FROM generate_series($((ROWS1+1)), $((ROWS1+ROWS2))) g" >/dev/null
check "经 B 写入后总行数" "$(Q $COORD "SELECT count(*) FROM rn25")" "$((ROWS1+ROWS2))"
verify_replica "[4] 再写后" $B $A

echo "========== [5] 受控切回 A：必须当选并登记（修前 -1 让位）=========="
check "A 当选组 $SID" "$(switch_to $SID $A)" "leader"
check "[5] ★ 登记 A 为主" "$(registered $SID $A)" "ok"
# 登记刚生效的一瞬，协调者路由已翻到 A、A 自己那份登记还没 apply（读闸门还当它是副本）⇒ 读被拒
# （P7-N31，亚秒级）。这里验的是数据完整，给 10 s 有界重试；立即可读的承诺由全方位套件 [11] 严格断言。
cnt5=""; for t in $(seq 1 10); do cnt5=$(Q $COORD "SELECT count(*) FROM rn25"); [[ -n "$cnt5" ]] && break; sleep 1; done
check "[5] 行数完整" "$cnt5" "$((ROWS1+ROWS2))"
Q $COORD "INSERT INTO rn25 VALUES ($((ROWS1+ROWS2+1)), 'z')" >/dev/null
check "[5] 经协调者再写一行（路由到 A）" "$(Q $COORD "SELECT count(*) FROM rn25")" "$((ROWS1+ROWS2+1))"

echo "========== [6] 健康 =========="
health_check_no_crash

echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N25 回归（AUTO=$AUTO）：通过" || echo "P7-N25 回归（AUTO=$AUTO）：存在 FAIL"
exit $(( FAIL > 0 ))
