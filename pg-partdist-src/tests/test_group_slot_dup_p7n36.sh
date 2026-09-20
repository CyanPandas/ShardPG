#!/usr/bin/env bash
# [宿主机] P7-N36 回归（重启后同一个 Raft 组被建出两个槽位）。
#
# 病灶：raft_group_ensure 先在锁外扫一遍"这个组有没有"，没找到就进锁分配槽位 —— 两个 backend
#   同时进来就各分到一个槽位，同一个 group_id 出现两份状态。实测重启后日志里同一毫秒两行
#   「创建 Raft 组 102895（槽位 2）」「（槽位 4）」，其中一份是没有日志的空壳，它发起竞选把
#   全组任期从 2 顶到 12（3 次重启复现 2 次）。
# 修法：进锁之后**再复查一遍**，已经有了就直接返回那一份。
#
# 断言：连续 ROUNDS 轮"整簇 worker 重启 + 并发把组唤醒"，每轮每个 worker 上
#   ① 同一 group_id 不出现两行；② 日志里同一 group_id 不被"创建"两次；③ 任期不暴涨。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
SHARDS="${SHARDS:-3}"; ROUNDS="${ROUNDS:-3}"; PAR="${PAR:-8}"
VICT=""; HB_SET=""; TSO_LEASE_MS="${TSO_LEASE_MS:-60000}"
SIDS=(); SID=""
LOOPOUT="${TMPDIR:-/tmp}/.n36_$$.txt"
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
  Q $COORD "ALTER SYSTEM RESET lock_timeout" >/dev/null 2>&1; Q $COORD "SELECT pg_reload_conf()" >/dev/null 2>&1
  Q $COORD "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND query LIKE '%n36-hold-apply%'" >/dev/null 2>&1
  if [[ -n "$VICT" ]]; then
    DEX bash -c "/work/pg-install/bin/pg_ctl -D '${DDIR[$VICT]}' status >/dev/null 2>&1 || /work/pg-install/bin/pg_ctl start -D '${DDIR[$VICT]}' -l '${DDIR[$VICT]}/pg.log' -o '-p $VICT' -w -t 60 >/dev/null 2>&1" </dev/null || true
    for t in $(seq 1 40); do [[ "$(Q $VICT "SELECT 1")" == 1 ]] && break; sleep 1; done
  fi
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; done
  for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_partdist.tso_master" >/dev/null
    Q $p "ALTER SYSTEM RESET pg_partdist.tso_conninfo" >/dev/null; Q $p "ALTER SYSTEM RESET pg_partdist.tso_lease_ms" >/dev/null
    for g in $(Q $p "SELECT string_agg(gid, ' ') FROM pg_prepared_xacts"); do Q $p "ROLLBACK PREPARED '$g'" >/dev/null; done
    Q $p "SELECT pg_reload_conf()" >/dev/null; done
  rm -f "$LOOPOUT"
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  # 先表后组；每条 DROP 套 statement_timeout（P7-N21）。副本残壳被路由守卫拒时，拆组后再删一遍。
  for sid in "${SIDS[@]:-}"; do [[ -z "$sid" ]] && continue; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn36_${sid}'::regclass)" >/dev/null 2>&1; done; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn36" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    for sid in "${SIDS[@]:-}"; do [[ -z "$sid" ]] && continue; for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn36_${sid}" >/dev/null 2>&1; done; done
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
cur_leader_of() { local gid=$1 p; for p in "${WORKERS[@]}"; do [[ "$(Q $p "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid")" == leader ]] && { echo $p; return; }; done; echo ""; }
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
    Q $rp "SELECT partdist.replay_catchup('rn36_${SID}'::regclass, $tp)" >/dev/null 2>&1; sleep 1
  done
  echo "${a}/${tp}"
}
eof_errors() { local p=$1; DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'unexpected data beyond EOF'" </dev/null; }
# 主 lp 与副本 rp 的某关系（主堆或主键索引）掩码外逐字节比对
rel_cmp() { local lp=$1 rp=$2 kind=$3 sql lpath rpath
  if [[ $kind == heap ]]; then sql="SELECT pg_relation_filepath('rn36_${SID}')"
  else sql="SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid='rn36_${SID}'::regclass AND indisprimary"; fi
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

CDIR=$(Q $COORD "SHOW data_directory")

echo "========== [1] 夹具：$SHARDS 分片的表，每个分片一个 3 成员 Raft 组 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn36;
SET citus.shard_count = $SHARDS; SET citus.shard_replication_factor = 1;
CREATE TABLE rn36(id int primary key, pad text);
SELECT create_distributed_table('rn36', 'id', colocate_with => 'none');
SQL
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
declare -A OWNER
while read -r sid port; do SIDS+=("$sid"); OWNER[$sid]=$port; done < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn36'::regclass ORDER BY s.shardid" </dev/null | tr '|' ' ')
check "拿到 $SHARDS 个分片" "${#SIDS[@]}" "$SHARDS"
for sid in "${SIDS[@]}"; do
  echo "  分片 $sid：主 :${OWNER[$sid]}"
  check "  组 $sid 主落在 :${OWNER[$sid]}" "$(build_group_on $sid ${OWNER[$sid]} "$ALLMEM")" "leader"
done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do [[ $p == ${OWNER[$sid]} ]] && continue
  r=$(PSQL ${OWNER[$sid]} -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  分片 $sid 副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:80}")" "ok"; done; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for sid in "${SIDS[@]}"; do check "  分片 $sid 登记完成" "$(registered $sid ${OWNER[$sid]})" "ok"; done
Q $COORD "INSERT INTO rn36 VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),(6,'f'),(7,'g'),(8,'h'),(9,'i')" >/dev/null
check "有数据" "$(Q $COORD "SELECT count(*) FROM rn36")" "9"

declare -A T0
for p in "${WORKERS[@]}"; do T0[$p]=$(Q $p "SELECT coalesce(max(current_term),0) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); done

dup_rows=0; dup_logs=0; term_jump=0
# ★ 复现条件（照演示里的实况）：**只重启一台**，另外两台还活着 ——
#   它们会对这台原本当主的那些组发起投票（RV 风暴）、同时给自己当主的组发心跳（AE），
#   每个 RPC 落在这台的一个**新后端**里、各自 hearsay 建组；同时本地还有多个后端在
#   restore_groups_if_needed（每后端一份静态标记）。三股并发同时对同一个 group_id
#   调 raft_group_ensure —— 缺"锁内复查"就会各插一个槽位。
#   （三台一起重启反而撞不上：没有活着的对端往它身上打 RPC。实测 3 轮 0 复现。）
for round in $(seq 1 $ROUNDS); do
  VIC=${WORKERS[$(( (round - 1) % ${#WORKERS[@]} ))]}
  echo "========== [2.$round] 只重启 :$VIC（另外两台在线，持续 RV/AE）+ $PAR 个本地会话在它刚起来时猛打 =========="
  for p in "${WORKERS[@]}"; do LOGMARK[$p]=$(DEX bash -c "wc -l < '${LOGF[$p]}'" </dev/null | tr -dc '0-9'); done
  # 把存活两台的心跳压到 50 ms：每个 AE RPC 落在这台的一个新后端里、各自 hearsay 建组，
  # 3 个组 × 2 台 × 20 次/s ⇒ 每秒上百次 raft_group_ensure 撞同一个 group_id
  for p in "${WORKERS[@]}"; do [[ $p == $VIC ]] && continue
    Q $p "ALTER SYSTEM SET pg_raft.heartbeat_ms = 50" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  # 先把打手挂上：它们会一直重试连接，正好卡在这台刚接受连接的那一瞬
  for i in $(seq 1 $PAR); do
    DEX bash -c "for k in \$(seq 1 60); do /work/pg-install/bin/psql -h /tmp -p $VIC -U postgres -d postgres -X -q -Atc 'SELECT count(*) FROM partdist.pg_raft_group_status()' >/dev/null 2>&1; done" </dev/null >/dev/null 2>&1 &
  done
  DEX bash -c "/work/pg-install/bin/pg_ctl -D '${DDIR[$VIC]}' -m fast -l '${DDIR[$VIC]}/pg.log' -o '-p $VIC' restart -w -t 90" </dev/null >/dev/null 2>&1
  for t in $(seq 1 60); do [[ "$(Q $VIC "SELECT 1")" == 1 ]] && break; sleep 1; done
  wait
  for p in "${WORKERS[@]}"; do [[ $p == $VIC ]] && continue
    Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  sleep 6
  for p in "${WORKERS[@]}"; do
    d=$(Q $p "SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM partdist.pg_raft_group_status() GROUP BY group_id) x WHERE n > 1")
    l=$(DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -oE '创建 Raft 组 [0-9]+' | sort | uniq -d | wc -l" </dev/null | tr -dc '0-9')
    t=$(Q $p "SELECT coalesce(max(current_term),0) FROM partdist.pg_raft_group_status() WHERE group_id<>0")
    jump=$(( ${t:-0} - ${T0[$p]:-0} ))
    echo "    :$p 重复槽位 ${d:-?} 个，日志里被建两次的组 ${l:-?} 个，最大任期 ${T0[$p]}→${t}"
    [[ "${d:-0}" != 0 ]] && dup_rows=$((dup_rows + d))
    [[ "${l:-0}" != 0 ]] && { dup_logs=$((dup_logs + l)); DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -E '创建 Raft 组' | tail -4" </dev/null; }
    [[ $jump -gt $((4 * round)) ]] && term_jump=$((term_jump + 1))
  done
  check "[2.$round] 表还能读（重启后组恢复正常）" "$(Q $COORD "SELECT count(*) FROM rn36")" "9"
done

echo "========== [3] 汇总 =========="
check "[3] ★ 同一个 group_id 的重复槽位" "$dup_rows" "0"
check "[3] ★ 日志里同一个组被创建两次的次数" "$dup_logs" "0"
check "[3] ★ 任期暴涨的节点数（每轮最多允许 +4）" "$term_jump" "0"

echo "========== [4] 健康 =========="
health_check_no_crash
echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N36 回归：通过" || echo "P7-N36 回归：存在 FAIL"
exit $(( FAIL > 0 ))
