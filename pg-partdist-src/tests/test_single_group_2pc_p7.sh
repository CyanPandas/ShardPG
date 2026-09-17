#!/usr/bin/env bash
# [宿主机] P7-N10 复现 / 回归：写集只有一个组、却走了 2PC 的全局事务，提交后必须可见。
#
# 缺陷：协调者的决议挂钩把"写集 ≤ 1 组"一律当 §3.4 快路径直接返回，不下发协调组、不做决议。
# 但 §3.4 说的快路径是 router 单分片 **1PC**（没有 PREPARE，在探针处就退出了）；能走到那里的
# 单组事务都是 2PC 在用（协调者侧 INSERT…SELECT 只命中一个分片 / 多分片 UPDATE 只有一个分片真改到行），
# 参与者已 PREPARE、分片 xid 记成 PREPARED、COMMIT PREPARED 按 T4.5 不写终局 ⇒ 判决永远不来 ⇒
# 打标表上提交成功的行永久不可见，且无任何告警。修法：写集 ≥ 1 组就照常做决议（单组 = 多组退化）。
#
#   [A] 单分片 INSERT…SELECT（全局事务）：提交 ⇒ 判决收敛后可见；该组决议数 +1；副本分片 clog 也得到 COMMITTED
#   [B] 2 分片表上的广播 UPDATE 只改到一个分片的行：提交 ⇒ 可见
#   [C] 对照：router 单分片 1PC 写不产生决议（快路径本身保住）
#   [D] 副本追平并逐字节一致
#
# 夹具规则：建组偏置选举超时（25）、副本 provision_shard_replica（24）、partition_map 两处收敛（32）、
#   ALTER SYSTEM 独占一条 -c（2）、重启协调者前删 pg_tso_boot（1）、收尾拆组两轮（16）。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
CONVERGE_S="${CONVERGE_S:-60}"
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
declare -A NID DDIR
for p in "${WORKERS[@]}"; do NID[$p]=$(Q $p "SHOW pg_raft.node_id"); DDIR[$p]=$(Q $p "SHOW data_directory"); done
CDIR=$(Q $COORD "SHOW data_directory")
ALLMEM="ARRAY[$(for p in "${WORKERS[@]}"; do printf '%s,' "${NID[$p]}"; done | sed 's/,$//')]"
ng=0; np0=0
for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ng=$((ng+${n:-0})); done
for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); np0=$((np0+${n:-0})); done
check "净场：无残留数据组" "$ng" "0"
check "净场：无残留 prepared 事务" "$np0" "0"

declare -A LEADER; SIDS=()
cleanup() {
  local r p sid g
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-0}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表/TSO 未动"; return; fi
  for p in $COORD "${WORKERS[@]}"; do for g in $(PSQL $p -Atc "SELECT gid FROM pg_prepared_xacts" </dev/null 2>/dev/null); do Q $p "ROLLBACK PREPARED '$g'" >/dev/null; done; done
  for r in 1 2; do for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 1; done
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('sg_${sid}'::regclass)" >/dev/null; done; done
  Q $COORD "DROP TABLE IF EXISTS sg" >/dev/null
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS sg_${sid}" >/dev/null; done; done
  for p in "${WORKERS[@]}"; do PSQL $p -q -c "SET citus.enable_ddl_propagation=off" -c "DROP FUNCTION IF EXISTS public.sclog_full(oid,bigint)" </dev/null >/dev/null 2>&1; done
  for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_partdist.tso_conninfo" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  Q $COORD "ALTER SYSTEM RESET pg_partdist.tso_master" >/dev/null; Q $COORD "SELECT pg_reload_conf()" >/dev/null
  echo "  [复原] prepared 已收、组已拆、表已删、TSO 已 RESET"
}
trap cleanup EXIT
build_group_on() {
  local gid=$1 want=$2 mem=$3 t p st=""
  for p in "${WORKERS[@]}"; do
    if [[ "$p" == "$want" ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null; else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 9000" >/dev/null; fi
    Q $p "SELECT pg_reload_conf()" >/dev/null
  done
  for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.pg_raft_group_create($gid, $mem)" >/dev/null; done
  for t in $(seq 1 40); do st=$(Q $want "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid"); [[ "$st" == leader ]] && break; sleep 1; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  echo "$st"
}
shard_ids() { Q $COORD "SELECT string_agg(g::text, ',') FROM (SELECT g FROM generate_series($2, $2+200000) g WHERE get_shard_id_for_distribution_column('sg', g) = $1 LIMIT $3) t"; }
gtx() {  # <gsid> <dml>：加入全局事务的事务；输出 txn_done 或全部输出拼一行
  PSQL $COORD -At -v ON_ERROR_STOP=1 </dev/null 2>&1 <<SQL | grep -vE '^(BEGIN|SET|COMMIT|INSERT [0-9 ]+|UPDATE [0-9]+|DELETE [0-9]+)$' \
    | awk '{a[NR]=$0} END{ if (NR>0 && a[NR]=="txn_done") print "txn_done"; else { s=""; for(i=1;i<=NR;i++) s=s (i>1?" | ":"") a[i]; print s } }'
SELECT partdist.partdist_gxid_next()||','||partdist.partdist_tso_client_start_ts()||','||$1 AS ji \\gset
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = :'ji';
$2
COMMIT;
SELECT 'txn_done';
SQL
}
converge() {  # <port> <sql> <期望> [秒]
  local port=$1 sql=$2 want=$3 max=${4:-$CONVERGE_S} t v="" p
  for t in $(seq 1 $max); do v=$(Q $port "$sql"); [[ "$v" == "$want" ]] && { echo "$v"; return; }
    for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.dtx_pending_sweep()" >/dev/null; done; sleep 1; done
  echo "$v"
}
LOCAL() { echo "SET citus.override_table_visibility=false; $1"; }
ndec() { Q $1 "SELECT count(*) FROM partdist.dtx_decision"; }

echo "========== [1] 夹具：sg 表 2 分片、每片 $NW 成员组、主=落位、副本供到其余 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS sg;
SET citus.shard_count = 2; SET citus.shard_replication_factor = 1;
CREATE TABLE sg(id int primary key, v text, k int NOT NULL DEFAULT 0);
SELECT create_distributed_table('sg', 'id', colocate_with => 'none');
ALTER TABLE sg SET (autovacuum_enabled = off);
SQL
while read sid port; do SIDS+=("$sid"); LEADER[$sid]=$port; done < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='sg'::regclass ORDER BY s.shardid" </dev/null | tr '|' ' ')
S1=${SIDS[0]}; S2=${SIDS[1]}; L1=${LEADER[$S1]}; L2=${LEADER[$S2]}
check "两片分落两台（$S1@:$L1 / $S2@:$L2）" "$([[ "$L1" != "$L2" ]] && echo ok)" "ok"
for p in "${WORKERS[@]}"; do
  Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null
  # 临时读 clog 的包装（与 dtx_* 套件同法），收尾删
  PSQL $p -q </dev/null >/dev/null 2>&1 <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION public.sclog_full(oid, bigint) RETURNS text AS '$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;
SQL
done
for sid in "${SIDS[@]}"; do
  lp=${LEADER[$sid]}
  check "组 $sid 主落在 :$lp" "$(build_group_on $sid $lp "$ALLMEM")" "leader"
  for p in "${WORKERS[@]}"; do [[ $p == $lp ]] && continue
    r=$(PSQL $lp -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
    check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok)" "ok"; done
done
for sid in "${SIDS[@]}"; do lp=${LEADER[$sid]}; ok=""
  for t in $(seq 1 60); do [[ "$(Q $lp "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" && "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" ]] && { ok=ok; break; }; sleep 1; done
  check "组 $sid partition_map 两处收敛" "$ok" "ok"
done
mk=$(PSQL $COORD -Atc "SELECT string_agg(split_part(status,' ',1), ',' ORDER BY shardid) FROM partdist.set_table_shard_mvcc('sg')" </dev/null 2>&1 | tail -1)
check "打标登记" "$mk" "registered,registered"
DEX rm -f "$CDIR/pg_tso_boot" </dev/null
DEX /work/pg-install/bin/pg_ctl -D "$CDIR" -m fast -l "$CDIR/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 40); do up=$(Q $COORD "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done
check "协调者删 pg_tso_boot 后重启就绪" "$up" "1"
Q $COORD "ALTER SYSTEM SET pg_partdist.tso_master = on" >/dev/null
for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 2; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done; sleep 4
tso=$(Q $COORD "SELECT partdist.partdist_tso_client_start_ts()")
check "TSO 取号可用（start_ts=$tso）" "$([[ "$tso" =~ ^[0-9]+$ ]] && echo ok)" "ok"
g0=""; for t in $(seq 1 30); do g0=$(Q $COORD "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()"); [[ "$g0" =~ ^[1-9] ]] && break; sleep 1; done
check "group0 有 leader" "$([[ "$g0" =~ ^[1-9] ]] && echo ok)" "ok"

echo "========== [A] ★ 单分片 INSERT…SELECT（全局事务，写集 1 组、走 2PC） =========="
ids=$(shard_ids $S1 1 5)
d0=$(ndec $L1)
out=$(gtx $S1 "INSERT INTO sg(id, v) SELECT x, 'a' FROM unnest(ARRAY[$ids]) x;")
check "事务报告提交成功" "$out" "txn_done"
check "★ 判决收敛后协调者可见 5 行" "$(converge $COORD "SELECT count(*) FROM sg WHERE v='a'" 5)" "5"
check "★ 主 :$L1 本地可见 5 行" "$(converge $L1 "$(LOCAL "SELECT count(*) FROM sg_$S1 WHERE v='a'")" 5 20)" "5"
check "★ 该组决议数 +1（单组 2PC 也做决议）" "$(( $(ndec $L1) - d0 ))" "1"
# 副本上的分片 clog：判决标记随流复制 ⇒ 副本 clog 也是 COMMITTED（st=2）
sx=$(Q $L1 "$(LOCAL "SELECT min(xmin::text::bigint) FROM sg_$S1 WHERE v='a'")")
for p in "${WORKERS[@]}"; do [[ $p == $L1 ]] && continue
  lo=$(Q $p "SELECT partdist.local_partition_for_shard($S1)")
  Q $p "SELECT partdist.replay_catchup('sg_$S1'::regclass, $(Q $L1 "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($S1))"), 10000)" >/dev/null
  st=$(converge $p "SELECT split_part(public.sclog_full($lo::oid, $sx::bigint),' ',1)" "st=2" 30)
  check "  副本 :$p 分片 clog 对 xid $sx = COMMITTED（$st）" "${st%% *}" "st=2"
done

echo "========== [B] ★ 2 分片表广播 UPDATE，只改到一个分片的行 =========="
d0=$(ndec $L1)
out=$(gtx $S1 "UPDATE sg SET k = k + 7 WHERE v = 'a';")
check "广播 UPDATE 事务提交成功" "$out" "txn_done"
check "★ 判决收敛后 5 行 k=7 可见" "$(converge $COORD "SELECT count(*) FROM sg WHERE v='a' AND k=7" 5)" "5"
check "★ 决议数 +1" "$(( $(ndec $L1) - d0 ))" "1"

echo "========== [C] 对照：router 单分片 1PC 不产生决议（快路径保住） =========="
d0=$(ndec $L1); d0b=$(ndec $L2)
one=$(shard_ids $S1 900001 1)
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO sg(id, v) VALUES ($one, 'c')" </dev/null
check "1PC 写可见" "$(converge $COORD "SELECT count(*) FROM sg WHERE v='c'" 1 20)" "1"
check "★ 1PC 写决议数不变" "$(( $(ndec $L1) - d0 + $(ndec $L2) - d0b ))" "0"

echo "========== [D] 副本追平并逐字节一致 =========="
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
sid=$S1; lp=$L1
bound=$(Q $lp "SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard($sid))")
PSQL $lp -q -c "CHECKPOINT" </dev/null >/dev/null
lpath="${DDIR[$lp]}/$(Q $lp "$(LOCAL "SELECT pg_relation_filepath('sg_$sid')")")"
for p in "${WORKERS[@]}"; do [[ $p == $lp ]] && continue
  got=""; for t in $(seq 1 30); do got=$(Q $p "SELECT partdist.replay_catchup('sg_$sid', $bound, 10000)"); [[ "$got" =~ ^[0-9]+$ && "$got" -ge "$bound" ]] && break; sleep 1; done
  PSQL $p -q -c "CHECKPOINT" </dev/null >/dev/null
  rpath="${DDIR[$p]}/$(Q $p "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('sg_$sid')")"
  same=$(DEX python3 /tmp/pagecmp.py --kind=heap "$lpath" "$rpath" </dev/null 2>/dev/null)
  check "  副本 :$p 追平($got/$bound) 且主堆逐字节一致" "$([[ "$got" =~ ^[0-9]+$ && "$got" -ge "$bound" ]] && echo "$same")" "IDENTICAL_OUTSIDE_HOLE"
done
np=0; for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); np=$((np+${n:-0})); done
check "无残留 prepared 事务" "$np" "0"
health_check_no_crash
echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]] && echo "P7-N10 单组 2PC 可见性：全部通过" || echo "P7-N10 单组 2PC 可见性：存在 FAIL"
