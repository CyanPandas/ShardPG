#!/usr/bin/env bash
# [宿主机] P7-N33 回归（切主窗口里旧主拒读）。
#
# 病灶：切主时"谁是主"这条登记要在每个节点各自 apply 一遍，各节点先后差 0.2–1.9 s（实测）。
#   旧主一旦 apply 到自己头上就立刻关门（副本壳表访问守卫），而协调者那份路由可能还指着它 ——
#   这 1–2 s 里所有落到这个分片的读全被判死：「不允许在本节点上对副本壳表（OID …）执行 SELECT」。
#   演示第 13 步手工切主时实测到（旧主关门 0.9 s 后协调者仍在往它身上路由）。
# 修法：刚被降级、且本地还没回放过新主的流时放行读 —— 这一格的数据就是它交出主权那一刻的
#   已提交状态。一旦回放推进（applied 变了）或宽限（pg_partdist.demoted_read_grace_ms，默认
#   10 s）到期，立即恢复拒读。
#
# 断言：[3] 连续 ROUNDS 次手工切主，切主窗口内被拒的读 = 0，且确实读到了（不是一次没读）。
# 窗口取 WINDOW_S（默认 1.5 s）—— 与实测到的"各节点 apply 同一条登记先后差 0.2–1.9 s"同量级。
# 宽限只保这一段：一旦新主把旧主重新供成副本、本地回放推进，宽限按设计立即失效。
# 附带覆盖 P7-N37：构造窗口靠的就是"协调者的控制面 apply 反复失败"，修前失败一次即**永久跳过**
#   这条登记（放锁后也追不上来），修后保留游标重试、放锁即追上。
# 负向对照 NEG=1：把 3 台 worker 的 demoted_read_grace_ms 设为 0（= 修前行为）⇒ 出现拒读。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ROWS1="${ROWS1:-20}"; ROUNDS="${ROUNDS:-3}"
NEG="${NEG:-0}"          # NEG=1：把宽限设为 0（修前行为）做负向对照
VICT=""; HB_SET=""; TSO_LEASE_MS="${TSO_LEASE_MS:-60000}"
LOOPOUT="${TMPDIR:-/tmp}/.n33_reads_$$.txt"
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
  Q $COORD "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND query LIKE '%n33-hold-apply%'" >/dev/null 2>&1
  if [[ -n "$VICT" ]]; then
    DEX bash -c "/work/pg-install/bin/pg_ctl -D '${DDIR[$VICT]}' status >/dev/null 2>&1 || /work/pg-install/bin/pg_ctl start -D '${DDIR[$VICT]}' -l '${DDIR[$VICT]}/pg.log' -o '-p $VICT' -w -t 60 >/dev/null 2>&1" </dev/null || true
    for t in $(seq 1 40); do [[ "$(Q $VICT "SELECT 1")" == 1 ]] && break; sleep 1; done
  fi
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; done
  for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_partdist.tso_master" >/dev/null
    Q $p "ALTER SYSTEM RESET pg_partdist.tso_conninfo" >/dev/null; Q $p "ALTER SYSTEM RESET pg_partdist.tso_lease_ms" >/dev/null
    for g in $(Q $p "SELECT string_agg(gid, ' ') FROM pg_prepared_xacts"); do Q $p "ROLLBACK PREPARED '$g'" >/dev/null; done
    Q $p "SELECT pg_reload_conf()" >/dev/null; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_partdist.demoted_read_grace_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  rm -f "$LOOPOUT"
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  # 先表后组；每条 DROP 套 statement_timeout（P7-N21）。副本残壳被路由守卫拒时，拆组后再删一遍。
  [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn33_${SID}'::regclass)" >/dev/null 2>&1; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn33" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn33_${SID}" >/dev/null 2>&1; done
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
    Q $rp "SELECT partdist.replay_catchup('rn33_${SID}'::regclass, $tp)" >/dev/null 2>&1; sleep 1
  done
  echo "${a}/${tp}"
}
eof_errors() { local p=$1; DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'unexpected data beyond EOF'" </dev/null; }
# 主 lp 与副本 rp 的某关系（主堆或主键索引）掩码外逐字节比对
rel_cmp() { local lp=$1 rp=$2 kind=$3 sql lpath rpath
  if [[ $kind == heap ]]; then sql="SELECT pg_relation_filepath('rn33_${SID}')"
  else sql="SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid='rn33_${SID}'::regclass AND indisprimary"; fi
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

echo "========== [1] 夹具：TSO + rn33 单分片打标表、3 成员组 =========="
DEX rm -f "$CDIR/pg_tso_boot" </dev/null
DEX /work/pg-install/bin/pg_ctl -D "$CDIR" -m fast -l "$CDIR/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 40); do up=$(Q $COORD "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done
check "协调者重启就绪（TSO boot）" "$up" "1"
Q $COORD "ALTER SYSTEM SET pg_partdist.tso_master=on" >/dev/null
for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo='host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null; Q $p "ALTER SYSTEM SET pg_partdist.tso_lease_ms=60000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 2; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done; sleep 3
check "TSO 取号可用" "$([[ "$(Q $COORD "SELECT partdist.partdist_tso_client_start_ts()")" =~ ^[0-9]+$ ]] && echo ok)" "ok"

PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn33;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE rn33(id int primary key, pad text);
SELECT create_distributed_table('rn33', 'id', colocate_with => 'none');
SQL
read SID A < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn33'::regclass" </dev/null | tr '|' ' ')
echo "  分片 $SID：初始主 :$A"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 :$A" "$(build_group_on $SID $A "$ALLMEM")" "leader"
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for p in "${WORKERS[@]}"; do [[ $p == $A ]] && continue
  r=$(PSQL $A -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:90}")" "ok"; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "登记 :$A 为主" "$(registered $SID $A)" "ok"
check "rn33 打标登记" "$(PSQL $COORD -Atc "SELECT count(*) FILTER (WHERE status LIKE 'registered%') FROM partdist.set_table_shard_mvcc('rn33')" </dev/null 2>&1 | tail -1)" "1"
vals=$(for i in $(seq 1 $ROWS1); do printf "(%s,'seed')," $i; done | sed 's/,$//')
Q $COORD "INSERT INTO rn33 VALUES $vals" >/dev/null
check "写入 $ROWS1 行" "$(Q $COORD "SELECT count(*) FROM rn33")" "$ROWS1"

if [[ "$NEG" == 1 ]]; then
  echo "========== [负向对照] 各 worker 把降级读宽限设为 0（= 修前行为） =========="
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.demoted_read_grace_ms = 0" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  n=0; for p in "${WORKERS[@]}"; do n=$((n + $(Q $p "SELECT count(*) FROM pg_settings WHERE name='pg_partdist.demoted_read_grace_ms' AND setting='0'"))); done
  check "负向对照：3 台 worker 的宽限都是 0" "$n" "3"
else
  n=0; for p in "${WORKERS[@]}"; do n=$((n + $(Q $p "SELECT count(*) FROM pg_settings WHERE name='pg_partdist.demoted_read_grace_ms' AND setting::int > 0"))); done
  check "降级读宽限已生效（默认 10 s）" "$n" "3"
fi

# ── 确定性构造：把协调者那份登记的 apply 锁住 → 它的路由停在旧主身上；
#    与此同时新主在 Raft 里已经选出来、旧主自己那份登记已经 apply、门已经关上。
#    这就是演示里实测到的那 1–2 s（各节点 apply 同一条登记先后差 0.2–1.9 s）。
#    协调者上设 lock_timeout=300ms：它的控制面 apply 每次撞锁就抛错回滚（路由层的更新
#    也随之回滚，路由因此留在旧主身上）。顺带覆盖 P7-N37 —— 修前这种抛错等于**永久跳过**
#    这条登记，放锁之后协调者也再不会知道新主是谁（实测：登记再没追上来）。
refused=0; total=0; other=0; hit=0; retried=0
Q $COORD "ALTER SYSTEM SET lock_timeout='300ms'" >/dev/null; Q $COORD "SELECT pg_reload_conf()" >/dev/null
CLOG=$(Q $COORD "SHOW data_directory")/pg.log
CMARK=$(DEX bash -c "wc -l < '$CLOG'" </dev/null | tr -dc '0-9')
for round in $(seq 1 $ROUNDS); do
  cur=$(cur_leader_of $SID); tgt=""
  for p in "${WORKERS[@]}"; do [[ $p != $cur ]] && { tgt=$p; break; }; done
  echo "========== [2.$round] 切主 :$cur → :$tgt；协调者的登记 apply 被锁住，路由仍指着旧主 :$cur =========="
  PSQL $COORD -q -c "BEGIN; SELECT 1 AS \"n33-hold-apply\" FROM partdist.partition_map WHERE partition_id=$SID FOR UPDATE; SELECT pg_sleep(60);" </dev/null >/dev/null 2>&1 &
  LOCKSH=$!
  held=""; for t in $(seq 1 20); do
    held=$(Q $COORD "SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%n33-hold-apply%' AND pid<>pg_backend_pid()")
    [[ "$held" == 1 ]] && break; sleep 0.5; done
  check "[2.$round] 协调者的 partition_map 行已被锁住" "$held" "1"

  { for i in $(seq 1 400); do echo "SELECT count(*) FROM rn33;"; echo "SELECT pg_sleep(0.05);"; done; } > /tmp/.n33_loop_$$.sql
  docker cp /tmp/.n33_loop_$$.sql "$CONTAINER":/tmp/n33_loop.sql >/dev/null; rm -f /tmp/.n33_loop_$$.sql
  DEX bash -c "exec /work/pg-install/bin/psql -h /tmp -p $COORD -U postgres -d postgres -X -q -At -f /tmp/n33_loop.sql 2>&1" </dev/null > "$LOOPOUT" &
  LOOPPID=$!
  sleep 1
  st=$(switch_to $SID $tgt)
  check "[2.$round] :$tgt 已成为 leader" "$st" "leader"
  # 等旧主自己 apply 到"我不再是主"（它此刻关门），而协调者还被锁着
  oldsaw=""; for t in $(seq 1 60); do
    oldsaw=$(Q $cur "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$SID")
    [[ "$oldsaw" == "${NID[$tgt]}" ]] && break; sleep 0.5; done
  coordsaw=$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$SID")
  check "[2.$round] 旧主 :$cur 已 apply 登记（自认已降级、门已关）" "$oldsaw" "${NID[$tgt]}"
  echo "    此刻协调者那份登记仍是 :$(for p in "${WORKERS[@]}"; do [[ "${NID[$p]}" == "$coordsaw" ]] && echo $p; done)（被锁住推不动）"
  [[ "$oldsaw" == "${NID[$tgt]}" && "$coordsaw" == "${NID[$cur]}" ]] && hit=$((hit + 1))
  # 只撑到真实量级的窗口（各节点 apply 同一条登记先后差 0.2–1.9 s）。
  # 再长就不公平了：新主一升主就把旧主重新供成副本，副本流一到、本地回放一推进，
  # 宽限**按设计**立即失效 —— 那之后旧主的页正在被新基线覆盖，本来就不能读。
  sleep ${WINDOW_S:-1.5}
  kill $LOCKSH 2>/dev/null; wait $LOCKSH 2>/dev/null
  Q $COORD "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND query LIKE '%n33-hold-apply%'" >/dev/null
  r7=$(DEX bash -c "tail -n +$((CMARK+1)) '$CLOG' | grep -c 'P7-N37'" </dev/null | tr -dc '0-9')
  retried=$((retried + r7))
  echo "    协调者控制面 apply 重试告警：$r7 条；样例：$(DEX bash -c "tail -n +$((CMARK+1)) '$CLOG' | grep -m1 'P7-N37'" </dev/null | cut -c1-160)"
  check "[2.$round] ★ 放锁后控制面登记追上（:$tgt）—— 条目没被丢掉（P7-N37）" "$(registered $SID $tgt)" "ok"
  sleep 2
  kill $LOOPPID 2>/dev/null; wait $LOOPPID 2>/dev/null
  DEX bash -c "pkill -f 'n33_loop.sql'" </dev/null >/dev/null 2>&1
  r=$(grep -c "副本壳表" "$LOOPOUT"); o=$(grep -c "^ERROR\|^错误" "$LOOPOUT"); t=$(grep -cE "^[0-9]+$" "$LOOPOUT")
  echo "    这一轮：成功读 $t 次，拒读（副本壳表）$r 次，其它错误 $((o > r ? o - r : 0)) 次"
  [[ $r -gt 0 ]] && echo "    拒读样例：$(grep -m1 "副本壳表" "$LOOPOUT")"
  refused=$((refused + r)); total=$((total + t)); other=$((other + (o > r ? o - r : 0)))
  check "[2.$round] 这一轮确实读到了（不是一次都没读）" "$([[ $t -gt 10 ]] && echo ok)" "ok"
done
Q $COORD "ALTER SYSTEM RESET lock_timeout" >/dev/null; Q $COORD "SELECT pg_reload_conf()" >/dev/null

echo "========== [3] 汇总 =========="
echo "  $ROUNDS 次切主，累计成功读 $total 次"
check "[3] 窗口确实构造出来了（旧主已关门 & 协调者路由还指着它）" "$hit" "$ROUNDS"
if [[ "$NEG" == 1 ]]; then
  check "[3] ★ 负向对照（宽限=0）：切主窗口内出现拒读" "$([[ $refused -gt 0 ]] && echo ok)" "ok"
else
  check "[3] ★ 切主窗口内被拒的读" "$refused" "0"
fi
check "[3] 其它错误" "$other" "0"
check "[3] ★ 控制面 apply 确实是先失败再重试成功的（P7-N37 的告警条数）" "$([[ $retried -gt 0 ]] && echo ok)" "ok"


echo "========== [4] 健康 =========="
health_check_no_crash

echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N33 回归：通过" || echo "P7-N33 回归：存在 FAIL"
exit $(( FAIL > 0 ))
