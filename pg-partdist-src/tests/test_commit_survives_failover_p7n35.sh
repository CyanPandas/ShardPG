#!/usr/bin/env bash
# [宿主机] P7-N35 回归（P0：切主丢已确认的提交）。
#
# 病灶：旧主提交最后一笔时，数据与提交标记已达多数派、客户端已收到成功；但从节点要等旧主的
#   **下一次心跳**才知道"这条已提交"。恰在这一拍里旧主宕机 ⇒ 新主日志里有这条（Raft 领导人
#   完备性），却因为升主追平的上界取的是"本节点已知已提交位点"而不回放它；随后
#   ShardXidClaimOnPromote 按"流里没有提交标记 ⇒ 从未提交"把它的分片 xid 改判 ABORTED，
#   已确认的提交就此消失（首次实测：切主后该行先可见、新主第一次写触发认领后消失）。
# 修法：升主前置里先提交一条**本任期**的空条目（Raft 标准动作），上一任期的尾巴随之提交、
#   被 apply，上界自然推到尾巴末端；凑不齐多数派就不升主、下个 tick 重试。
#
# 确定性构造：把主的 heartbeat_ms 临时调到 60 s —— 写入提交后主不再发心跳，从节点就停在
#   "拿到了这条日志、但不知道它已提交"的那一拍；此时 immediate 停主。
# 断言：[3] 前提成立（从的 commit_index < last_log_index，即确实不知道已提交）；
#   [5] 切主后那一行还在、分片 clog 上它的 xid 是 COMMITTED、行数不少。
# 负向对照 NEG=1：把两个从上的 promote_prepare_ex 换成不带 N35 的旧版 ⇒ 该行丢失、xid 被判 ABORTED。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ROWS1="${ROWS1:-10}"; ROUNDS="${ROUNDS:-2}"; NEG="${NEG:-0}"; VICT=""; HB_SET=""; TSO_LEASE_MS="${TSO_LEASE_MS:-60000}"
REPO="${REPO:-$HOME/shardpg-test-work}"
OLDPPX="${TMPDIR:-/tmp}/.n35_oldppx_$$.sql"; SRC_PPX="$REPO/pg-raft-src/sql/pg_raft--1.0.sql"
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
release_lock() {   # 放掉卡 apply 的那把行锁（后台 psql + 可能已阻塞的 AE 后端）
  [[ -z "${LOCKPORT:-}" ]] && return 0
  Q $LOCKPORT "ALTER SYSTEM RESET lock_timeout" >/dev/null; Q $LOCKPORT "SELECT pg_reload_conf()" >/dev/null
  Q $LOCKPORT "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND query LIKE '%n35-hold-apply%'" >/dev/null
  [[ -n "${LOCKSH:-}" ]] && kill $LOCKSH 2>/dev/null; wait $LOCKSH 2>/dev/null
  LOCKPORT=""; LOCKSH=""
}
cleanup() {
  release_lock
  local p r
  if [[ -n "$VICT" ]]; then
    DEX bash -c "/work/pg-install/bin/pg_ctl -D '${DDIR[$VICT]}' status >/dev/null 2>&1 || /work/pg-install/bin/pg_ctl start -D '${DDIR[$VICT]}' -l '${DDIR[$VICT]}/pg.log' -o '-p $VICT' -w -t 60 >/dev/null 2>&1" </dev/null || true
    for t in $(seq 1 40); do [[ "$(Q $VICT "SELECT 1")" == 1 ]] && break; sleep 1; done
  fi
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; done
  for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_partdist.tso_master" >/dev/null
    Q $p "ALTER SYSTEM RESET pg_partdist.tso_conninfo" >/dev/null; Q $p "ALTER SYSTEM RESET pg_partdist.tso_lease_ms" >/dev/null
    for g in $(Q $p "SELECT string_agg(gid, ' ') FROM pg_prepared_xacts"); do Q $p "ROLLBACK PREPARED '$g'" >/dev/null; done
    Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "$NEG" == 1 && -s "$OLDPPX" ]]; then
    for p in "${WORKERS[@]}"; do { echo "SET search_path=partdist; SET citus.enable_ddl_propagation=off;"; awk "/CREATE OR REPLACE FUNCTION pg_raft_promote_prepare_ex\\(/,/^\\\$promox\\\$;/" "$SRC_PPX"; } | PSQL $p -q -v ON_ERROR_STOP=1 >/dev/null 2>&1; done
    echo "  [复原] 各节点的 promote_prepare_ex 已换回带 N35 的版本"
  fi
  rm -f "$OLDPPX"
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  # 先表后组；每条 DROP 套 statement_timeout（P7-N21）。副本残壳被路由守卫拒时，拆组后再删一遍。
  [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn35_${SID}'::regclass)" >/dev/null 2>&1; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn35" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    [[ -n "$SID" ]] && for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn35_${SID}" >/dev/null 2>&1; done
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
    Q $rp "SELECT partdist.replay_catchup('rn35_${SID}'::regclass, $tp)" >/dev/null 2>&1; sleep 1
  done
  echo "${a}/${tp}"
}
eof_errors() { local p=$1; DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'unexpected data beyond EOF'" </dev/null; }
# 主 lp 与副本 rp 的某关系（主堆或主键索引）掩码外逐字节比对
rel_cmp() { local lp=$1 rp=$2 kind=$3 sql lpath rpath
  if [[ $kind == heap ]]; then sql="SELECT pg_relation_filepath('rn35_${SID}')"
  else sql="SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid='rn35_${SID}'::regclass AND indisprimary"; fi
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

echo "========== [1] 夹具：TSO + rn35 单分片打标表、3 成员组 =========="
DEX rm -f "$CDIR/pg_tso_boot" </dev/null
DEX /work/pg-install/bin/pg_ctl -D "$CDIR" -m fast -l "$CDIR/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 40); do up=$(Q $COORD "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done
check "协调者重启就绪（TSO boot）" "$up" "1"
Q $COORD "ALTER SYSTEM SET pg_partdist.tso_master=on" >/dev/null
for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo='host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null; Q $p "ALTER SYSTEM SET pg_partdist.tso_lease_ms=60000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 2; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done; sleep 3
check "TSO 取号可用" "$([[ "$(Q $COORD "SELECT partdist.partdist_tso_client_start_ts()")" =~ ^[0-9]+$ ]] && echo ok)" "ok"

PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn35;
SET citus.shard_count = 1; SET citus.shard_replication_factor = 1;
CREATE TABLE rn35(id int primary key, pad text);
SELECT create_distributed_table('rn35', 'id', colocate_with => 'none');
SQL
read SID A < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn35'::regclass" </dev/null | tr '|' ' ')
echo "  分片 $SID：初始主 :$A"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
check "组 $SID 主落在 :$A" "$(build_group_on $SID $A "$ALLMEM")" "leader"
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for p in "${WORKERS[@]}"; do [[ $p == $A ]] && continue
  r=$(PSQL $A -Atc "SELECT partdist.provision_shard_replica(${SID}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:90}")" "ok"; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "登记 :$A 为主" "$(registered $SID $A)" "ok"
check "rn35 打标登记" "$(PSQL $COORD -Atc "SELECT count(*) FILTER (WHERE status LIKE 'registered%') FROM partdist.set_table_shard_mvcc('rn35')" </dev/null 2>&1 | tail -1)" "1"
vals=$(for i in $(seq 1 $ROWS1); do printf "(%s,'seed')," $i; done | sed 's/,$//')
Q $COORD "INSERT INTO rn35 VALUES $vals" >/dev/null
check "写入 $ROWS1 行（打标表用 VALUES）" "$(Q $COORD "SELECT count(*) FROM rn35")" "$ROWS1"

if [[ "$NEG" == 1 ]]; then
  echo "========== [负向对照] 各节点换成不带 N35 的旧版 promote_prepare_ex =========="
  git -C "${REPO:-$HOME/shardpg-test-work}" show HEAD:pg-raft-src/sql/pg_raft--1.0.sql > "$OLDPPX" 2>/dev/null || true
  if [[ ! -s "$OLDPPX" ]] || grep -q 'P7-N35' "$OLDPPX"; then echo "  取不到修前那一版（HEAD 已含 N35？），跳过负向对照"; NEG=0; fi
fi
if [[ "$NEG" == 1 ]]; then
  for p in "${WORKERS[@]}"; do
    { echo "SET search_path=partdist; SET citus.enable_ddl_propagation=off;"; awk '/CREATE OR REPLACE FUNCTION pg_raft_promote_prepare_ex\(/,/^\$promox\$;/' "$OLDPPX"; } | PSQL $p -q -v ON_ERROR_STOP=1 >/dev/null
  done
  n=0; for p in "${WORKERS[@]}"; do n=$((n + $(Q $p "SELECT count(*) FROM pg_proc WHERE proname='pg_raft_promote_prepare_ex' AND prosrc LIKE '%P7-N35%'"))); done
  check "负向对照：各节点已换成旧版（不含 N35）" "$n" "0"
fi

# ── [2.00] 直测"本任期空条目"这一支：确定性构造里 last_log == commit，走不到它；
#    而它正是"新主不知道尾巴已提交"那种情形的修法，必须单独验证 OP_NOOP 可提、可提交、无害。
A0=$(cur_leader_of $SID)
echo "========== [2.00] 在主 :$A0 上直接提一条 OP_NOOP（升主前置用的就是它） =========="
st0=$(Q $A0 "SELECT last_log_index||'/'||commit_index FROM partdist.pg_raft_group_status() WHERE group_id=$SID")
noop=$(PSQL $A0 -Atc "SELECT partdist.pg_raft_group_propose($SID,'OP_NOOP','{}')" </dev/null 2>&1 | tail -1)
check "[2.00] OP_NOOP 提案拿到多数派（返回 idx>0）" "$([[ "$noop" =~ ^[0-9]+$ ]] && [[ $noop -gt 0 ]] && echo ok || echo "$noop")" "ok"
sleep 2
nw=0
for p in "${WORKERS[@]}"; do
  ci=$(Q $p "SELECT commit_index FROM partdist.pg_raft_group_status() WHERE group_id=$SID")
  [[ ${ci:-0} -ge ${noop:-0} ]] || nw=$((nw+1))
  w=$(DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'OP_NOOP.*apply 抛错\|OP_NOOP.*应用失败'" </dev/null | tr -dc '0-9')
  echo "    :$p commit_index=$ci（空条目 idx=$noop），OP_NOOP 相关告警 ${w:-0} 条"
  [[ ${w:-0} != 0 ]] && nw=$((nw+1))
done
check "[2.00] 三个节点都提交了这条空条目、且没有 apply 告警" "$nw" "0"
check "[2.00] 空条目之后照常可写" "$(Q $COORD "INSERT INTO rn35 VALUES (99009,'after-noop') RETURNING 1" >/dev/null; Q $COORD "SELECT count(*) FROM rn35 WHERE id=99009")" "1"
echo "    提空条目前 $st0 → 现在 $(Q $A0 "SELECT last_log_index||'/'||commit_index FROM partdist.pg_raft_group_status() WHERE group_id=$SID")"

# ════════════════════════════════════════════════════════════════════════════
# [2.0] 确定性构造：从节点 B 的 apply 进度行被锁住 → 条目照进环、commit_index 照推进、
#       applied 卡住。这就是事故里"新主升主时上界取小了"的那一刻。
# ════════════════════════════════════════════════════════════════════════════
A=$(cur_leader_of $SID); B=""; C=""
for p in "${WORKERS[@]}"; do [[ $p == $A ]] && continue; [[ -z "$B" ]] && B=$p || C=$p; done
echo "========== [2.0] 主 :$A，目标新主 :$B，另一从 :$C =========="
loB=$(lo_of $B)
Q $C "ALTER SYSTEM SET pg_raft.election_timeout_ms = 60000" >/dev/null; Q $C "SELECT pg_reload_conf()" >/dev/null
Q $B "ALTER SYSTEM SET pg_raft.election_timeout_ms = 3000"  >/dev/null; Q $B "SELECT pg_reload_conf()" >/dev/null
# apply 撞上这把锁时 300 ms 即失败（而不是挂住）：条目已进环、commit_index 已推进、
# 只有 applied 推不动；主等 300 ms 拿不到 :$B 的 ack，转向 :$C 照样凑齐多数派。
Q $B "ALTER SYSTEM SET lock_timeout = '300ms'" >/dev/null; Q $B "SELECT pg_reload_conf()" >/dev/null
LOCKPORT=$B
PSQL $B -q -c "BEGIN; SELECT 1 AS \"n35-hold-apply\" FROM partdist.follower_partition_map WHERE partition_id=$loB FOR UPDATE; SELECT pg_sleep(300);" </dev/null >/dev/null 2>&1 &
LOCKSH=$!
held=""; for t in $(seq 1 20); do
  held=$(Q $B "SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%n35-hold-apply%' AND pid<>pg_backend_pid()")
  [[ "$held" == 1 ]] && break; sleep 0.5; done
check "[2.0] 从节点 :$B 的 apply 进度行已被锁住" "$held" "1"
app0=$(Q $B "SELECT partdist.get_follower_applied_part_lsn($loB)")

ins=$(PSQL $COORD -Atc "SET lock_timeout=0; INSERT INTO rn35 VALUES (99100, 'committed-but-not-applied')" </dev/null 2>&1 | tr '\n' ' ')
[[ -n "${ins// /}" ]] && echo "    写入返回：$ins"
check "[2.0] 这一笔已提交（多数派 = :$A + :$C，客户端已收到成功）" "$(Q $COORD "SELECT count(*) FROM rn35 WHERE id=99100")" "1"
xid0=$(Q $A "SELECT xmin::text::bigint FROM rn35_${SID} WHERE id=99100")
appB=$(Q $B "SELECT partdist.get_follower_applied_part_lsn($loB)")
cplB=$(Q $B "SELECT partdist.pg_raft_group_committed_plsn($SID)")
sttB=$(Q $B "SELECT last_log_index||'/'||commit_index FROM partdist.pg_raft_group_status() WHERE group_id=$SID")
echo "    :$B 锁前已应用=$app0 现已应用=$appB 已提交(字节位点)=$cplB 日志末尾/已知提交=$sttB"
check "[2.0] ★ 窗口成立：:$B 上 已提交($cplB) > 已应用($appB)" "$([[ ${cplB:-0} -gt ${appB:-0} ]] && echo ok)" "ok"

echo "--- 杀掉主 :$A（immediate，等同断电），:$B 带着这个窗口去升主 ---"
VICT=$A
DEX bash -c "/work/pg-install/bin/pg_ctl -D '${DDIR[$A]}' -m immediate stop -w -t 60" </dev/null >/dev/null 2>&1
newp=""; for t in $(seq 1 150); do
  r=$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$SID")
  [[ -n "$r" && "$r" != "${NID[$A]}" ]] && { for p in "${WORKERS[@]}"; do [[ "${NID[$p]}" == "$r" ]] && newp=$p; done; break; }
  sleep 1; done
check "[2.0] 选出并登记了新主（:${newp:-?}）" "$newp" "$B"
echo "    新主升主日志：$(DEX bash -c "tail -n +$((LOGMARK[$B]+1)) '${LOGF[$B]}' | grep -E '上界|追平至|P7-N35' | tail -3 | tr '\n' ' | '" </dev/null)"
cnt=""; for t in $(seq 1 60); do cnt=$(Q $COORD "SELECT count(*) FROM rn35 WHERE id=99100"); [[ "$cnt" == 1 ]] && break; sleep 1; done
check "[2.0] ★ 切主后这一笔已确认的提交还在" "$cnt" "1"
if [[ -n "$xid0" ]]; then
  st=$(Q $B "SELECT partdist.shard_clog_status_full(partdist.local_partition_for_shard($SID)::oid, $xid0)")
  echo "    新主 :$B 上该分片 xid $xid0 的判决：$st"
  check "[2.0] ★ 新主的分片 clog 判决是 COMMITTED" "$(echo "$st" | grep -oE 'st=[0-9]+')" "st=2"
fi
release_lock
Q $B "ALTER SYSTEM RESET lock_timeout" >/dev/null; Q $B "SELECT pg_reload_conf()" >/dev/null
Q $COORD "INSERT INTO rn35 VALUES (99101, 'after-failover')" >/dev/null
check "[2.0] 新主可写" "$(Q $COORD "SELECT count(*) FROM rn35 WHERE id=99101")" "1"
check "[2.0] ★ 认领跑过之后那一笔仍在" "$(Q $COORD "SELECT count(*) FROM rn35 WHERE id=99100")" "1"
DEX bash -c "/work/pg-install/bin/pg_ctl start -D '${DDIR[$A]}' -l '${DDIR[$A]}/pg.log' -o '-p $A' -w -t 60" </dev/null >/dev/null 2>&1
for t in $(seq 1 40); do [[ "$(Q $A "SELECT 1")" == 1 ]] && break; sleep 1; done
VICT=""
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 5

# ── 一轮：把主的心跳调长（提交后不再有心跳推送"已提交"）→ 写一行 → 立刻 immediate 停主 → 看那一行还在不在
lost=0
for round in $(seq 1 $ROUNDS); do
  cur=$(cur_leader_of $SID)
  if [[ -z "$cur" ]]; then check "[第 $round 轮] 找得到当前主" "" "ok"; break; fi
  id=$((99000 + round))
  echo "========== [2.$round] 当前主 :$cur —— 心跳调长 → 写 id=$id → 立刻宕机 =========="
  Q $cur "ALTER SYSTEM SET pg_raft.heartbeat_ms = 60000" >/dev/null; Q $cur "SELECT pg_reload_conf()" >/dev/null; HB_SET=1
  sleep 1
  Q $COORD "INSERT INTO rn35 VALUES ($id, 'committed-then-crash')" >/dev/null
  ok1=$(Q $COORD "SELECT count(*) FROM rn35 WHERE id=$id")
  check "[2.$round] 这一笔已提交（客户端已收到成功）" "$ok1" "1"
  xid=$(Q $cur "SELECT xmin::text::bigint FROM rn35_${SID} WHERE id=$id")
  for p in "${WORKERS[@]}"; do [[ $p == $cur ]] && continue
    lo=$(Q $p "SELECT partdist.local_partition_for_shard($SID)")
    echo "    :$p 已应用=$(Q $p "SELECT partdist.get_follower_applied_part_lsn($lo)") 已提交=$(Q $p "SELECT partdist.pg_raft_group_committed_plsn($SID)") 日志末尾/已知提交=$(Q $p "SELECT last_log_index||'/'||commit_index FROM partdist.pg_raft_group_status() WHERE group_id=$SID")"
  done
  VICT=$cur
  DEX bash -c "/work/pg-install/bin/pg_ctl -D '${DDIR[$cur]}' -m immediate stop -w -t 60" </dev/null >/dev/null 2>&1
  newp=""; for t in $(seq 1 150); do
    r=$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$SID")
    [[ -n "$r" && "$r" != "${NID[$cur]}" ]] && { for p in "${WORKERS[@]}"; do [[ "${NID[$p]}" == "$r" ]] && newp=$p; done; break; }
    sleep 1; done
  check "[2.$round] 选出并登记了新主（:${newp:-?}）" "$([[ -n "$newp" ]] && echo ok)" "ok"
  cnt=""; for t in $(seq 1 60); do cnt=$(Q $COORD "SELECT count(*) FROM rn35 WHERE id=$id"); [[ "$cnt" == 1 ]] && break; sleep 1; done
  check "[2.$round] ★ 切主后这一笔已确认的提交还在" "$cnt" "1"
  [[ "$cnt" == 1 ]] || lost=$((lost+1))
  if [[ -n "$newp" && -n "$xid" ]]; then
    st=$(Q $newp "SELECT partdist.shard_clog_status_full(partdist.local_partition_for_shard($SID)::oid, $xid)")
    echo "    新主 :$newp 上该分片 xid $xid 的判决：$st"
    check "[2.$round] ★ 新主的分片 clog 判决是 COMMITTED" "$(echo "$st" | grep -oE 'st=[0-9]+')" "st=2"
  fi
  # 触发一次写：认领在第一次写时才跑（首次实测正是这时那行才消失）
  Q $COORD "INSERT INTO rn35 VALUES ($((id + 500)), 'after-failover')" >/dev/null
  check "[2.$round] 新主可写" "$(Q $COORD "SELECT count(*) FROM rn35 WHERE id=$((id + 500))")" "1"
  check "[2.$round] ★ 认领跑过之后那一笔仍在" "$(Q $COORD "SELECT count(*) FROM rn35 WHERE id=$id")" "1"
  # 把宕机的节点拉回来，供下一轮（它要重新成为副本）
  DEX bash -c "/work/pg-install/bin/pg_ctl start -D '${DDIR[$cur]}' -l '${DDIR[$cur]}/pg.log' -o '-p $cur' -w -t 60" </dev/null >/dev/null 2>&1
  for t in $(seq 1 40); do [[ "$(Q $cur "SELECT 1")" == 1 ]] && break; sleep 1; done
  Q $cur "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $cur "SELECT pg_reload_conf()" >/dev/null
  VICT=""
  sleep 5
done

echo "========== [3] 汇总 =========="
check "[3] ★ $ROUNDS 轮里丢失的已确认提交数" "$lost" "0"
check "[3] 总行数（$ROWS1 + 1 + 2 + $((ROUNDS * 2))）" "$(Q $COORD "SELECT count(*) FROM rn35")" "$((ROWS1 + 3 + ROUNDS * 2))"

echo "========== [4] 健康 =========="
health_check_no_crash

echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N35 回归：通过" || echo "P7-N35 回归：存在 FAIL"
exit $(( FAIL > 0 ))
