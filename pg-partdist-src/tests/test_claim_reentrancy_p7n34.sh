#!/usr/bin/env bash
# [宿主机] P7-N34 回归（宕机切主后新主被"自己等自己"卡成分钟级不可写）。
#
# 病灶：控制面 apply 主权交接那一段**握着 partition_map 的行锁**去等该分片的复制认领位；
#   认领位是本扩展自己在 shmem 里的位，PG 的死锁检测看不见，只能干等 60 s 超时。更糟的是
#   认领位不可重入：同一个 backend（升主时的 repair_diverged_shards → 控制面 apply）再申请
#   一次就是在等自己。实测宕机切主后新主上的写入连撞三轮 60 s 超时，约 6 分钟不可写。
# 修法：① 认领位可重入（同 backend 记深度，不等自己）；② 持有者就是自己时直接回收；
#   ③ 控制面 apply 里的等待上限压到 3 s，等不到就放手回滚，由下一次 AE 重做
#   （这条"下一次 AE 重做"要靠 P7-N37 的重试才真正成立）。
#
# 断言：[3] 崩溃切主前后持续写入 —— ① 全集群日志里 0 条"复制认领位超过 60000 ms"；
#   ② 从宕机到恢复可写 < MAXWAIT_S（默认 90 s，修前实测约 360 s）；③ 事后数据可读。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
SHARDS="${SHARDS:-3}"; NWRITES="${NWRITES:-600}"; MAXWAIT_S="${MAXWAIT_S:-90}"
VICT=""; HB_SET=""; VICTIM=""; TSO_LEASE_MS="${TSO_LEASE_MS:-60000}"
SIDS=(); SID=""
LOOPOUT="${TMPDIR:-/tmp}/.n34_$$.txt"
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
  for sid in "${SIDS[@]:-}"; do [[ -z "$sid" ]] && continue; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('rn34_${sid}'::regclass)" >/dev/null 2>&1; done; done
  Q $COORD "SET statement_timeout='60s'; DROP TABLE IF EXISTS rn34" >/dev/null 2>&1
  Q $COORD "SELECT recover_prepared_transactions()" >/dev/null 2>&1
  # 拆组后在途 RPC 会按 hearsay 把组再建出来（实测），副本残壳又被路由守卫拒 —— 拆组/删壳交替两轮
  for r in 1 2; do
    for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 2
    for sid in "${SIDS[@]:-}"; do [[ -z "$sid" ]] && continue; for p in "${WORKERS[@]}"; do Q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS rn34_${sid}" >/dev/null 2>&1; done; done
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
    Q $rp "SELECT partdist.replay_catchup('rn34_${SID}'::regclass, $tp)" >/dev/null 2>&1; sleep 1
  done
  echo "${a}/${tp}"
}
eof_errors() { local p=$1; DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c 'unexpected data beyond EOF'" </dev/null; }
# 主 lp 与副本 rp 的某关系（主堆或主键索引）掩码外逐字节比对
rel_cmp() { local lp=$1 rp=$2 kind=$3 sql lpath rpath
  if [[ $kind == heap ]]; then sql="SELECT pg_relation_filepath('rn34_${SID}')"
  else sql="SELECT pg_relation_filepath(indexrelid) FROM pg_index WHERE indrelid='rn34_${SID}'::regclass AND indisprimary"; fi
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

echo "========== [1] 夹具：TSO + $SHARDS 分片打标表，每个分片一个 3 成员组 =========="
DEX rm -f "$CDIR/pg_tso_boot" </dev/null
DEX /work/pg-install/bin/pg_ctl -D "$CDIR" -m fast -l "$CDIR/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 40); do up=$(Q $COORD "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done
check "协调者重启就绪（TSO boot）" "$up" "1"
Q $COORD "ALTER SYSTEM SET pg_partdist.tso_master=on" >/dev/null
for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo='host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null; Q $p "ALTER SYSTEM SET pg_partdist.tso_lease_ms=$TSO_LEASE_MS" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 2; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done; sleep 3
check "TSO 取号可用" "$([[ "$(Q $COORD "SELECT partdist.partdist_tso_client_start_ts()")" =~ ^[0-9]+$ ]] && echo ok)" "ok"

PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS rn34;
SET citus.shard_count = $SHARDS; SET citus.shard_replication_factor = 1;
CREATE TABLE rn34(id int primary key, pad text);
SELECT create_distributed_table('rn34', 'id', colocate_with => 'none');
SQL
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
declare -A OWNER
while read -r sid port; do SIDS+=("$sid"); OWNER[$sid]=$port; done < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='rn34'::regclass ORDER BY s.shardid" </dev/null | tr '|' ' ')
check "拿到 $SHARDS 个分片" "${#SIDS[@]}" "$SHARDS"
for sid in "${SIDS[@]}"; do echo "  分片 $sid：主 :${OWNER[$sid]}"; check "  组 $sid 主落在 :${OWNER[$sid]}" "$(build_group_on $sid ${OWNER[$sid]} "$ALLMEM")" "leader"; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do [[ $p == ${OWNER[$sid]} ]] && continue
  r=$(PSQL ${OWNER[$sid]} -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
  check "  分片 $sid 副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:80}")" "ok"; done; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for sid in "${SIDS[@]}"; do check "  分片 $sid 登记完成" "$(registered $sid ${OWNER[$sid]})" "ok"; done
check "rn34 打标登记" "$(PSQL $COORD -Atc "SELECT count(*) FILTER (WHERE status LIKE 'registered%') FROM partdist.set_table_shard_mvcc('rn34')" </dev/null 2>&1 | tail -1)" "$SHARDS"

# 选一台当受害者：它是某个分片的主
VICTIM=${OWNER[${SIDS[0]}]}
echo "========== [2] 持续写入 + 宕机 :$VICTIM（它是分片 ${SIDS[0]} 的主） =========="
for p in "${WORKERS[@]}"; do LOGMARK[$p]=$(DEX bash -c "wc -l < '${LOGF[$p]}'" </dev/null | tr -dc '0-9'); done
CMARK=$(DEX bash -c "wc -l < '$CDIR/pg.log'" </dev/null | tr -dc '0-9')
# 容器内一个循环：每笔写都打上时间戳（写落哪个分片由哈希决定，三个组都会被写到）
DEX bash -c "cat > /tmp/n34_load.sh <<'EOS'
i=1000
while [ \$i -lt $((1000 + NWRITES)) ]; do
  r=\$(/work/pg-install/bin/psql -h /tmp -p 5432 -U postgres -d postgres -X -Atc \"SET statement_timeout='20s'; INSERT INTO rn34 VALUES (\$i,'x')\" 2>&1 | tr '\n' ' ')
  echo \"\$(date +%s.%N) \$r\"
  i=\$((i+1))
done
EOS
exec bash /tmp/n34_load.sh" </dev/null > "$LOOPOUT" 2>&1 &
LOADPID=$!
sleep 8
TCRASH=$(date +%s.%N)
DEX bash -c "/work/pg-install/bin/pg_ctl -D '${DDIR[$VICTIM]}' -m immediate stop -w -t 60" </dev/null >/dev/null 2>&1
echo "  已在 $TCRASH 停掉 :$VICTIM"
newp=""; for t in $(seq 1 180); do
  r=$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${SIDS[0]}")
  [[ -n "$r" && "$r" != "${NID[$VICTIM]}" ]] && { for p in "${WORKERS[@]}"; do [[ "${NID[$p]}" == "$r" ]] && newp=$p; done; break; }
  sleep 1; done
check "[2] 分片 ${SIDS[0]} 选出并登记了新主（:${newp:-?}）" "$([[ -n "$newp" ]] && echo ok)" "ok"
# 等写入循环跑完或恢复可写
for t in $(seq 1 $((MAXWAIT_S + 60))); do
  kill -0 $LOADPID 2>/dev/null || break
  awk -v tc="$TCRASH" '$1 > tc && /INSERT 0 1/ {found=1; exit} END{exit !found}' "$LOOPOUT" && break
  sleep 1
done
kill $LOADPID 2>/dev/null; wait $LOADPID 2>/dev/null
DEX bash -c "pkill -f n34_load.sh" </dev/null >/dev/null 2>&1

first_ok=$(awk -v tc="$TCRASH" '$1 > tc && /INSERT 0 1/ {printf "%.1f", $1 - tc; exit}' "$LOOPOUT")
nfail=$(awk -v tc="$TCRASH" '$1 > tc && !/INSERT 0 1/ {n++} END{print n+0}' "$LOOPOUT")
nok=$(grep -c "INSERT 0 1" "$LOOPOUT")
echo "  宕机后第一笔写成功用时：${first_ok:-未恢复} s；宕机后失败的写 $nfail 笔；全程成功 $nok 笔"
echo "  失败样例：$(awk -v tc="$TCRASH" '$1 > tc && !/INSERT 0 1/ {print; exit}' "$LOOPOUT" | cut -c1-150)"
check "[2] ★ 宕机后恢复可写用时 < $MAXWAIT_S s" "$([[ -n "$first_ok" ]] && awk -v v="$first_ok" -v m="$MAXWAIT_S" 'BEGIN{exit !(v < m)}' && echo ok)" "ok"

echo "========== [3] 认领位超时统计 =========="
t60=0; t3=0
for p in "${WORKERS[@]}"; do
  a=$(DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c '复制认领位超过 60000 ms'" </dev/null | tr -dc '0-9')
  b=$(DEX bash -c "tail -n +$((LOGMARK[$p]+1)) '${LOGF[$p]}' | grep -c '复制认领位超过 3000 ms'" </dev/null | tr -dc '0-9')
  echo "    :$p 60 s 超时 ${a:-0} 条，3 s 让路（控制面 apply 放手重做）${b:-0} 条"
  t60=$((t60 + ${a:-0})); t3=$((t3 + ${b:-0}))
done
a=$(DEX bash -c "tail -n +$((CMARK+1)) '$CDIR/pg.log' | grep -c '复制认领位超过 60000 ms'" </dev/null | tr -dc '0-9'); t60=$((t60 + ${a:-0}))
check "[3] ★ 全集群 60 s 认领超时的条数" "$t60" "0"
check "[3] 数据可读" "$([[ "$(Q $COORD "SELECT count(*) FROM rn34")" -gt 0 ]] && echo ok)" "ok"

echo "========== [4] 拉回 :$VICTIM 并健康检查 =========="
DEX bash -c "/work/pg-install/bin/pg_ctl start -D '${DDIR[$VICTIM]}' -l '${DDIR[$VICTIM]}/pg.log' -o '-p $VICTIM' -w -t 60" </dev/null >/dev/null 2>&1
for t in $(seq 1 40); do [[ "$(Q $VICTIM "SELECT 1")" == 1 ]] && break; sleep 1; done
VICT=""
check "[4] :$VICTIM 已回到集群" "$(Q $VICTIM "SELECT 1")" "1"
health_check_no_crash
echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N34 回归：通过" || echo "P7-N34 回归：存在 FAIL"
exit $(( FAIL > 0 ))
