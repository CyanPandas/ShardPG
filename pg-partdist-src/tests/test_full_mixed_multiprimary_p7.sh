#!/usr/bin/env bash
# [宿主机] 全方位功能验收：混合主从 + 多主多从，跑通打标分片的分布式事务与受控切主。
#
# 与既有 test_mixed_role_p7 / test_multi_role_p7 互补：那两套验的是**不打标**分片的角色
# 并存 / 物理隔离 / 副本逐字节；本套把同一套「每台既当主又当从」的多主多从拓扑压上
# **TX2 全栈**（打标分片 MVCC + TSO + join_info + 2PC）与**受控切主**，端到端验正确性。
#
# 拓扑（NW=worker 数；两张分布表各 NW 分片、每分片一 Raft 组、成员=全部 worker）：
#   fm_mvcc：打标（分片 MVCC）——每分片主分散到不同 worker（多主），其余成员当从（多从）
#   fm_nat ：不打标（原生）——同法分散，证明「打标 / 原生」两类分片在同一簇/同一节点并存
#   ⇒ 每台 worker 同时是若干分片的主、若干分片的从（混合主从），各按自己的本地 OID 编址
#
# 断言：
#   [2] 多主多从：fm_mvcc 的 NW 个主落在 NW 台不同 worker；每台既是主又是从；本地 OID 各异
#   [3] 读写路由：往各主写单分片、经协调者读回；打标表走分片 MVCC、原生表走原生路径并存
#   [4] 物理隔离 + 副本逐字节：狂写一个组不动另一组从副本字节；各从追平且与主逐字节一致
#   [5] ★ 跨组分布式事务：打标表上 TSO+join 跨组转账 commit 守恒；显式 ROLLBACK 不改账；
#       中途失败（写不存在的目标分片行）原子性——借记一侧不得单独生效
#   [6] ★ 受控切主：对一个 fm_mvcc 组切主，登记后总额守恒、无 reissue 撞号、无 PREPARED 残留、
#       旧主降级后副本重新追平并与新主逐字节一致
#   [7] 健康：无节点崩溃
#
# 夹具纪律（见 project_pg_test_fixture_rules）：建组顺序；供副本期/切主抬 election_timeout；
#   打标经协调者 set_table_shard_mvcc；TSO boot 删 pg_tso_boot 重启协调者；单分片写用 VALUES/IN；
#   收尾三台一起拆组、拆在 DROP 之前；跑着的脚本不改。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ACCT="${ACCT:-6}"; TSO_LEASE_MS="${TSO_LEASE_MS:-60000}"
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
declare -A NID DDIR
for p in "${WORKERS[@]}"; do NID[$p]=$(Q $p "SHOW pg_raft.node_id"); DDIR[$p]=$(Q $p "SHOW data_directory"); done
CDIR=$(Q $COORD "SHOW data_directory")
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
ng=0; np0=0
for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ng=$((ng+${n:-0})); done
for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); np0=$((np0+${n:-0})); done
check "净场：无残留数据组" "$ng" "0"; check "净场：无残留 prepared" "$np0" "0"

SIDS_M=(); SIDS_N=(); declare -A LEADER SP_D
BG_PIDS=""
cleanup() {
  local p g s
  for x in $BG_PIDS; do kill "$x" 2>/dev/null; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表/TSO 未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  for p in $COORD "${WORKERS[@]}"; do for g in $(PSQL $p -Atc "SELECT gid FROM pg_prepared_xacts" </dev/null 2>/dev/null); do Q $p "ROLLBACK PREPARED '$g'" >/dev/null; done; done
  for r in 1 2; do for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 1; done
  for s in "${SIDS_M[@]}" "${SIDS_N[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('fmx_${s}'::regclass)" >/dev/null; done; done
  Q $COORD "DROP TABLE IF EXISTS fm_mvcc" >/dev/null; Q $COORD "DROP TABLE IF EXISTS fm_nat" >/dev/null
  for s in "${SIDS_M[@]}" "${SIDS_N[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS fmx_${s}" >/dev/null; done; done
  for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_partdist.tso_conninfo" >/dev/null; Q $p "ALTER SYSTEM RESET pg_partdist.tso_lease_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  Q $COORD "ALTER SYSTEM RESET pg_partdist.tso_master" >/dev/null; Q $COORD "SELECT pg_reload_conf()" >/dev/null
  echo "  [复原] prepared 已收、组已拆、表已删、TSO 已 RESET"
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
cur_leader() { local sid=$1 p; for p in "${WORKERS[@]}"; do [[ "$(Q $p "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$sid")" == leader ]] && { echo $p; return; }; done; echo ""; }
shard_ids() { Q $COORD "SELECT string_agg(g::text, ',') FROM (SELECT g FROM generate_series($3, $3+300000) g WHERE get_shard_id_for_distribution_column('$1', g) = $2 LIMIT $4) t"; }
# TX2 全栈跨组事务（join_info = gxid,start_ts,coord_gsid）
gtx() {
  PSQL $COORD -At -v ON_ERROR_STOP=1 </dev/null 2>&1 <<SQL | grep -vE '^(BEGIN|SET|COMMIT|ROLLBACK|INSERT [0-9 ]+|UPDATE [0-9]+|DELETE [0-9]+)$' | awk '{a[NR]=$0} END{ if (NR>0 && a[NR]=="txn_done") print "txn_done"; else { s=""; for(i=1;i<=NR;i++) s=s (i>1?" | ":"") a[i]; print s } }'
SELECT partdist.partdist_gxid_next()||','||partdist.partdist_tso_client_start_ts()||','||$1 AS ji \\gset
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = :'ji';
$2
COMMIT;
SELECT 'txn_done';
SQL
}
converge() { local port=$1 sql=$2 want=$3 max=${4:-60} t v="" p; for t in $(seq 1 $max); do v=$(Q $port "$sql"); [[ "$v" == "$want" ]] && { echo "$v"; return; }; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.dtx_pending_sweep()" >/dev/null; done; sleep 1; done; echo "$v"; }
main_heap_md5() { local port=$1 rel=$2; DEX bash -c "P=\$(/work/pg-install/bin/psql -h /tmp -p $port -U postgres -d postgres -X -Atc \"SELECT pg_relation_filepath('$rel')\" </dev/null); md5sum '${DDIR[$port]}/'\$P 2>/dev/null | cut -d' ' -f1"; }
# ★ 先把从副本同步到位再比：2 vCPU 建组期抢跑偏置会让某组副本基线半途而废（0 块，
#   baseline_pending），或旧主降级后作为从尚未回追。0 块/pending 就从当前主重供一遍，再长追平。
ensure_replica_synced() { local lp=$1 rp=$2 sid=$3 tbl=$4 lo tp bp sz a t
  lo=$(Q $rp "SELECT partdist.local_partition_for_shard($sid)")
  sz=$(DEX bash -c "P=\$(/work/pg-install/bin/psql -h /tmp -p $rp -U postgres -d postgres -X -Atc \"SELECT pg_relation_filepath('${tbl}_${sid}')\" </dev/null); wc -c < '${DDIR[$rp]}/'\$P 2>/dev/null" </dev/null)
  bp=$(Q $rp "SELECT partdist.shard_baseline_pending($lo::oid)")
  if [[ "${sz:-0}" == 0 || "$bp" == t ]]; then
    for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms=30000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
    PSQL $lp -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$rp]})" </dev/null >/dev/null 2>&1
    for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  fi
  tp=$(Q $lp "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($sid))")
  for t in $(seq 1 45); do a=$(Q $rp "SELECT applied_part_lsn FROM partdist.follower_partition_map WHERE partition_id=$lo"); [[ -n "$a" && -n "$tp" && "$a" -ge "$tp" ]] && break; Q $rp "SELECT partdist.replay_catchup(('${tbl}_'||$sid)::regclass,$tp)" >/dev/null 2>&1; sleep 1; done
}
# 掩码外逐字节比对（主 vs 从）：hint bit / 空闲区会不同，走 pagecmp.py（同 test_mixed_role_p7）
heap_masked_cmp() { local lp=$1 rp=$2 rel=$3 lpath rpath
  DEX bash -c "test -f /tmp/pagecmp.py || true" </dev/null
  PSQL $lp -q -c "CHECKPOINT;" </dev/null >/dev/null 2>&1; PSQL $rp -q -c "CHECKPOINT;" </dev/null >/dev/null 2>&1; sleep 1
  lpath=$(Q $lp "SET citus.override_table_visibility=false; SELECT pg_relation_filepath('$rel')")
  rpath=$(Q $rp "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('$rel')")
  DEX python3 /tmp/pagecmp.py --kind=heap "${DDIR[$lp]}/$lpath" "${DDIR[$rp]}/$rpath" </dev/null 2>/dev/null
}

echo "========== [1] 拓扑：fm_mvcc(打标) + fm_nat(原生) 各 $NW 分片、每分片一组、主分散多台 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS fm_mvcc; DROP TABLE IF EXISTS fm_nat;
SET citus.shard_count = $NW; SET citus.shard_replication_factor = 1;
CREATE TABLE fm_mvcc(id int primary key, v text, n int NOT NULL DEFAULT 0);
SELECT create_distributed_table('fm_mvcc','id', colocate_with=>'none');
ALTER TABLE fm_mvcc SET (autovacuum_enabled=off);
CREATE TABLE fm_nat(id int primary key, v text, n int NOT NULL DEFAULT 0);
SELECT create_distributed_table('fm_nat','id', colocate_with=>'none');
ALTER TABLE fm_nat SET (autovacuum_enabled=off);
SQL
# shard_identity 必须在 build/provision **之前**建（local_partition_for_shard 靠它；N12 同序）
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null
  PSQL $p -q </dev/null >/dev/null 2>&1 <<'S2'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION public.sclog_full(oid,bigint) RETURNS text AS '$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;
S2
done

# 每张表各分片：主分散到不同 worker（round-robin），建组 + 供副本
assign_and_build() {   # $1=表名 $2=数组名  → 填 SIDS_$suf / LEADER；leader = 该分片 Citus placement 所在节点
  local tbl=$1 arrname=$2 sid lp
  while read sid lp; do
    eval "$arrname+=($sid)"; LEADER[$sid]=$lp
    st=$(build_group_on $sid $lp "$ALLMEM"); check "  组 $sid（$tbl）主落在 :$lp" "$st" "leader"
    for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms=15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
    for p in "${WORKERS[@]}"; do [[ $p == $lp ]] && continue; r=$(PSQL $lp -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' '); check "    副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:70}")" "ok"; done
    for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  done < <(PSQL $COORD -Atc "SELECT s.shardid, n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='$tbl'::regclass ORDER BY s.shardid" </dev/null | tr '|' ' ')
}
assign_and_build fm_mvcc SIDS_M
assign_and_build fm_nat  SIDS_N
# ★ 稳定化：顺序建 6 个组时先建的组会被后建组的抢跑偏置夺走主权（N15），首个组的副本
#   基线因此半途而废（实测 102699 两副本 0 块）。建完统一抬全体选举超时 30 s（压住漂移），
#   逐组重认当前主、把未 armed 的副本从**当前主**重新供一遍，再 RESET。
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms=30000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
for sid in "${SIDS_M[@]}" "${SIDS_N[@]}"; do cur=$(cur_leader $sid); [[ -z "$cur" ]] && { sleep 3; cur=$(cur_leader $sid); }; [[ -n "$cur" ]] && LEADER[$sid]=$cur
  for p in "${WORKERS[@]}"; do [[ $p == $cur || -z "$cur" ]] && continue
    armed=$(Q $p "SELECT count(*) FROM partdist.replay_status() s WHERE s.shard=partdist.local_partition_for_shard($sid) AND s.armed")
    [[ "$armed" == 1 ]] && continue
    r=$(PSQL $cur -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
    check "  稳定化：组 $sid 从 :$p 从当前主 :$cur 补供" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:80}")" "ok"
  done
done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
# partition_map 两处收敛（各组当前主 = 协调者视图）
for sid in "${SIDS_M[@]}" "${SIDS_N[@]}"; do ok=""; for t in $(seq 1 60); do lp=$(cur_leader $sid); LEADER[$sid]=${lp:-${LEADER[$sid]}}; [[ -n "$lp" && "$(Q $lp "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" && "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" ]] && { ok=ok; break; }; sleep 1; done; check "  组 $sid partition_map 两处收敛（当前主 :${LEADER[$sid]}）" "$ok" "ok"; done
# 打标 fm_mvcc + TSO 全栈（fm_nat 不打标）
mk=$(PSQL $COORD -Atc "SELECT count(*) FILTER (WHERE status LIKE 'registered%') FROM partdist.set_table_shard_mvcc('fm_mvcc')" </dev/null 2>&1 | tail -1)
check "fm_mvcc 打标登记 $NW 片" "$mk" "$NW"
DEX rm -f "$CDIR/pg_tso_boot" </dev/null; DEX /work/pg-install/bin/pg_ctl -D "$CDIR" -m fast -l "$CDIR/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 40); do up=$(Q $COORD "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done; check "协调者重启就绪" "$up" "1"
Q $COORD "ALTER SYSTEM SET pg_partdist.tso_master=on" >/dev/null
for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo='host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null; Q $p "ALTER SYSTEM SET pg_partdist.tso_lease_ms=$TSO_LEASE_MS" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 2; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done; sleep 4
check "TSO 取号可用" "$([[ "$(Q $COORD "SELECT partdist.partdist_tso_client_start_ts()")" =~ ^[0-9]+$ ]] && echo ok)" "ok"

echo "========== [2] 多主多从 + 混合角色 =========="
declare -A ISLEAD ISREP
for sid in "${SIDS_M[@]}" "${SIDS_N[@]}"; do lp=$(cur_leader $sid); ISLEAD[$lp]=$(( ${ISLEAD[$lp]:-0} + 1 )); for p in "${WORKERS[@]}"; do [[ $p != $lp ]] && ISREP[$p]=$(( ${ISREP[$p]:-0} + 1 )); done; done
nmain_hosts=0; for p in "${WORKERS[@]}"; do [[ ${ISLEAD[$p]:-0} -ge 1 ]] && nmain_hosts=$((nmain_hosts+1)); done
check "多主：主分布在 ≥2 台不同 worker（多主成立）" "$([[ $nmain_hosts -ge 2 ]] && echo ok)" "ok"
nmix=0; for p in "${WORKERS[@]}"; do [[ ${ISLEAD[$p]:-0} -ge 1 && ${ISREP[$p]:-0} -ge 1 ]] && nmix=$((nmix+1)); done
check "混合角色：≥2 台 worker 同时既当主又当从" "$([[ $nmix -ge 2 ]] && echo ok)" "ok"
# 同一台上打标分片与原生分片的本地 OID 各异
p0=${WORKERS[0]}; om=$(Q $p0 "SELECT partdist.local_partition_for_shard(${SIDS_M[0]})"); on=$(Q $p0 "SELECT partdist.local_partition_for_shard(${SIDS_N[0]})")
check "同节点打标/原生分片本地 OID 各异" "$([[ -n "$om" && -n "$on" && "$om" != "$on" ]] && echo ok)" "ok"

echo "========== [3] 读写路由：打标表走分片 MVCC、原生表走原生，并存 =========="
for sid in "${SIDS_M[@]}"; do lp=${LEADER[$sid]}; ids=$(shard_ids fm_mvcc $sid 800001 $ACCT); vals=$(for a in ${ids//,/ }; do printf "(%s,'m',1000)," "$a"; done | sed 's/,$//'); out=$(gtx $sid "INSERT INTO fm_mvcc(id,v,n) VALUES $vals;"); check "  往打标组 $sid 写 $ACCT 行（TSO+join）" "$out" "txn_done"; done
for sid in "${SIDS_N[@]}"; do ids=$(shard_ids fm_nat $sid 900001 $ACCT); vals=$(for a in ${ids//,/ }; do printf "(%s,'n',1000)," "$a"; done | sed 's/,$//'); out=$(PSQL $COORD -Atc "INSERT INTO fm_nat(id,v,n) VALUES $vals; SELECT 'ok'" </dev/null 2>&1 | tail -1); check "  往原生组 $sid 写 $ACCT 行（原生路径）" "$out" "ok"; done
check "打标表经协调者读回" "$(converge $COORD "SELECT count(*) FROM fm_mvcc" $((ACCT*NW)))" "$((ACCT*NW))"
check "原生表经协调者读回" "$(Q $COORD "SELECT count(*) FROM fm_nat")" "$((ACCT*NW))"

echo "========== [4] 物理隔离 + 副本逐字节一致 =========="
# 狂写 fm_mvcc 的第一个组，另一组（fm_nat 第一个）从副本字节不变
sidM=${SIDS_M[0]}; lpM=$(cur_leader $sidM); sidN=${SIDS_N[0]}; lpN=$(cur_leader $sidN)
repN=""; for p in "${WORKERS[@]}"; do [[ $p != $lpN ]] && { repN=$p; break; }; done
before=$(main_heap_md5 $repN fm_nat_${sidN})
manyids=$(shard_ids fm_mvcc $sidM 810001 60)
gtx $sidM "UPDATE fm_mvcc SET n=n+1 WHERE id IN ($manyids);" >/dev/null 2>&1 || true
gtx $sidM "INSERT INTO fm_mvcc(id,v,n) SELECT g,'x',1 FROM unnest(ARRAY[$manyids]) g ON CONFLICT DO NOTHING;" >/dev/null 2>&1 || true
after=$(main_heap_md5 $repN fm_nat_${sidN})
check "狂写打标组不改另一（原生）组从副本主堆字节" "$([[ -n "$before" && "$before" == "$after" ]] && echo ok)" "ok"
# 各从追平并与主逐字节一致（抽 fm_mvcc 第一组 + fm_nat 第一组）
for pair in "fm_mvcc:$sidM" "fm_nat:$sidN"; do IFS=: read tbl sid <<<"$pair"; lp=$(cur_leader $sid)
  for rp in "${WORKERS[@]}"; do [[ $rp == $lp ]] && continue
    ensure_replica_synced $lp $rp $sid $tbl
    cmp=$(heap_masked_cmp $lp $rp ${tbl}_${sid})
    check "  组 $sid 从 :$rp 与主 :$lp 主堆掩码外逐字节一致" "$cmp" "IDENTICAL_OUTSIDE_HOLE"
  done
done

echo "========== [5] 跨组分布式事务：commit 守恒 / ROLLBACK 不改账 / 中途失败原子 =========="
# 取两个不同组的账户做跨组转账
sA=${SIDS_M[0]}; sB=${SIDS_M[1]}
a1=$(shard_ids fm_mvcc $sA 800001 1); b1=$(shard_ids fm_mvcc $sB 800001 1)
tot0=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$(Q $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')")")
gtx $sA "UPDATE fm_mvcc SET n=n-5 WHERE id=$a1; UPDATE fm_mvcc SET n=n+5 WHERE id=$b1;" >/dev/null
tot1=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$tot0")
check "[5] 跨组 commit 守恒" "$tot1" "$tot0"
# ROLLBACK：显式回滚不得改账
naa=$(Q $COORD "SELECT n FROM fm_mvcc WHERE id=$a1")
PSQL $COORD -At -v ON_ERROR_STOP=1 </dev/null >/dev/null 2>&1 <<SQL
SELECT partdist.partdist_gxid_next()||','||partdist.partdist_tso_client_start_ts()||','||$sA AS ji \\gset
BEGIN; SET LOCAL citus.propagate_set_commands='local'; SET LOCAL pg_partdist.join_info=:'ji';
UPDATE fm_mvcc SET n=n-7 WHERE id=$a1; UPDATE fm_mvcc SET n=n+7 WHERE id=$b1;
ROLLBACK;
SQL
check "[5] 跨组 ROLLBACK 后 a 账不变" "$(converge $COORD "SELECT n FROM fm_mvcc WHERE id=$a1" "$naa")" "$naa"
# 中途失败原子性：贷记写一个不存在的行（0 行受影响），借记不得单独生效——用真正会报错的：往目标组写违反主键
dupid=$b1
before5=$(Q $COORD "SELECT n FROM fm_mvcc WHERE id=$a1")
out5=$(gtx $sA "UPDATE fm_mvcc SET n=n-9 WHERE id=$a1; INSERT INTO fm_mvcc(id,v,n) VALUES ($dupid,'m',1);")
check "[5] 中途失败（主键冲突）事务未提交" "$([[ "$out5" != txn_done ]] && echo ok)" "ok"
check "[5] 中途失败后借记一侧未单独生效" "$(converge $COORD "SELECT n FROM fm_mvcc WHERE id=$a1" "$before5")" "$before5"

echo "========== [6] 受控切主：守恒 + 无 reissue 撞号 + 无 PREPARED 残留 + 副本重追平 =========="
FS=${SIDS_M[0]}; OLD=$(cur_leader $FS); NEWL=""; for p in "${WORKERS[@]}"; do [[ $p != $OLD ]] && { NEWL=$p; break; }; done
totb=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$(Q $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')")")
for p in "${WORKERS[@]}"; do if [[ $p == $NEWL ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms=300" >/dev/null; elif [[ $p == $OLD ]]; then Q $p "ALTER SYSTEM SET pg_raft.heartbeat_ms=60000" >/dev/null; else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms=60000" >/dev/null; fi; Q $p "SELECT pg_reload_conf()" >/dev/null; done
st=""; for t in $(seq 1 120); do st=$(Q $NEWL "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$FS"); [[ "$st" == leader ]] && break; sleep 1; done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check ":$NEWL 当选组 $FS 新主" "$st" "leader"
reg=""; for t in $(seq 1 90); do [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$FS")" == "${NID[$NEWL]}" ]] && { reg=ok; break; }; sleep 2; done
check "新主 180s 内登记" "$reg" "ok"; LEADER[$FS]=$NEWL
tota=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fm_mvcc WHERE v IN ('m','x')" "$totb" 90)
check "[6] ★ 切主后总额守恒" "$tota" "$totb"
# 无 PREPARED 残留（当前主）
lo=$(Q $NEWL "SELECT partdist.local_partition_for_shard($FS)"); xmax=$(( $(Q $NEWL "SELECT partdist.shard_xid_next($lo::oid)") + 1 )); res=0
for x in $(seq 1 $xmax); do st2=$(Q $NEWL "SELECT partdist.shard_clog_status_full($lo::oid,$x)" | grep -oE 'st=[0-9]+'); [[ "$st2" == "st=1" ]] && res=$((res+1)); done
check "[6] ★ 新主无 PREPARED 残留" "$res" "0"
# 无 reissue 撞号：跨节点对读同一分片 xid 的 sts
coll=0; declare -A seen
for p in "${WORKERS[@]}"; do lo2=$(Q $p "SELECT partdist.local_partition_for_shard($FS)"); [[ -z "$lo2" || "$lo2" == 0 ]] && continue
  while IFS='|' read x full; do sts=$(echo "$full" | grep -oE 'sts=[0-9]+' | cut -d= -f2); [[ -z "$sts" || "$sts" == 0 ]] && continue
    key="$x"; if [[ -n "${seen[$key]:-}" && "${seen[$key]}" != "$sts" ]]; then coll=$((coll+1)); fi; seen[$key]=$sts
  done < <(PSQL $p -Atc "SELECT x, partdist.shard_clog_status_full($lo2::oid,x) FROM generate_series(1,$xmax) x" </dev/null 2>/dev/null)
done
check "[6] ★ 无 reissue 撞号（同分片 xid 跨节点 start_ts 一致）" "$coll" "0"
# 副本（含旧主）重新追平并与新主逐字节一致
lpN=$(cur_leader $FS); lmd=$(main_heap_md5 $lpN fm_mvcc_${FS})
for rp in "${WORKERS[@]}"; do [[ $rp == $lpN ]] && continue
  ensure_replica_synced $lpN $rp $FS fm_mvcc
  cmp=$(heap_masked_cmp $lpN $rp fm_mvcc_${FS}); check "  切主后从 :$rp 与新主掩码外逐字节一致" "$cmp" "IDENTICAL_OUTSIDE_HOLE"
done

echo "========== [7] 健康 =========="
health_check_no_crash
echo
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "全方位（混合主从 + 多主多从）功能验收：全绿" || echo "全方位功能验收：存在 FAIL"
