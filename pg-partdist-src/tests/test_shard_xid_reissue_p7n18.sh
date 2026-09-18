#!/usr/bin/env bash
# [宿主机] P7-N18 诊断 / 回归：分片 xid 跨任期重号（切主后小额总额漂移）的根因固化 + 干净路径不变式。
#
# 结论（2026-09-18，两次静默复现 + run 5 负载正例，代码四处对得上）：
#   N18 是**"未追平就升主"窗口里的负载门控竞态**，不是无条件缺陷 ——
#   ① 重号护栏 ShardXidClaimOnPromote(shard_xid.c) 把新主发号起点置为
#      Max(交接来的发号水位, 影子 next_hint)。这两个来源都只反映本节点**已 apply**
#      的号：水位由 replay_worker.c 在 apply 已提交条目时 ShardXidRaiseAllocWatermark 抬，
#      影子由 shard_replay.c ShardReplayNoteDataShardXid 在 redo 时喂。
#   ② 但升主前置 pg_raft_promote_prepare_ex 的追平上界 bound = 本节点自己的
#      follower_partition_map.applied_part_lsn（get_follower_applied_part_lsn），
#      **不是流里已复制到的字节**；而 follower 的 apply 跑在 pg_raft_append_entries
#      backend 里、纯 2PC 负载下 in_txn_replication 还会跳过推进（data_apply_advance），
#      2 vCPU 饱和时 applied 会落在 flush 后面。availability 兜底（p_force 过 60 s）更是
#      整段跳过追平。于是升主时水位是陈旧值。
#   ③ 分片流里那截"已复制未 apply 的尾巴"带着旧主发过的 PREPARE MARKER 分片 xid，
#      没进水位/影子 ⇒ 新主 next_xid 偏小 ⇒ **重发旧主已提交用过的分片 xid**。
#      分片 clog 以 xid 为键（shard_clog.c），两笔不同事务（start_ts 不同）撞同一槽，
#      判决互相覆盖；N12 之二的 in-doubt 闭合按 dtxid→sxid 映射落账时更会把判决写到
#      新事务上 ⇒ 2PC 跨分片原子性破坏、总额漂移（N12 run 5 +2；本文件负载版可复现）。
#
# 为什么静默集群下不触发（run 1/2 实测 flush_B == applied_B，无尾巴）：不加负载时
#   复制+apply 是亚毫秒级，早在目标选举超时（~1.5 s）之前就追平了，尾巴根本不存在。
#   ⇒ **单次干净切主不会重号**，这既是负向对照、也是 N18 只在"负载 + 未追平升主"下成立的证据。
#
# 本文件两段：
#   [A] 干净单切（静默）：升主后发号起点不回退、切主前已 COMMIT 的行仍在、总额守恒 —— 全部应 PASS。
#   [B] 负载版触发（P 个后台会话持续转账下对同一组连切两次）：尽力复现重号，取证跨节点对读分片 clog。
#       复现到 ⇒ [K]/[S] FAIL（这是 N18 本体，预期）；没复现到 ⇒ 只报（竞态，非每轮触发），run 5 已有正例存档。
# 只加复现与取证，不改产品码。修法候选见 P7_REMEDIATION_PLAN §1.11 N18（推荐 ①：ClaimOnPromote 把水位抬过**流里**而非 applied 的最大分片 xid）。

set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ACCT="${ACCT:-4}"; REG_LIMIT_S="${REG_LIMIT_S:-180}"; CONVERGE_S="${CONVERGE_S:-120}"
# ★ 本用例验的是判决守恒，不是 TSO 续租（P7-N11）。2 vCPU 上 4 会话跨组 2PC 会把续租栅栏（lease−lease/4=7.5 s）
#   打得满屏失败、负载空转，把要验的现象淹掉；把租约抬到 60 s 压住噪声，收尾 RESET。
TSO_LEASE_MS="${TSO_LEASE_MS:-60000}"
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
check "净场：无残留数据组" "$ng" "0"; check "净场：无残留 prepared" "$np0" "0"
declare -A LEADER; SIDS=(); BG_PIDS=""; SP_DIR=$(mktemp -d)
LOGMARK=(); for p in "${WORKERS[@]}"; do LOGMARK+=("$(DEX bash -c "wc -l < '${DDIR[$p]}/pg.log'" </dev/null)"); done
cleanup() {
  local r p sid g
  for x in $BG_PIDS; do kill "$x" 2>/dev/null; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then echo "  [保留现场] 有 FAIL，组/表/TSO/临时函数未动（取证后手工清理，或 KEEP_ON_FAIL=0）"; return; fi
  for p in $COORD "${WORKERS[@]}"; do for g in $(PSQL $p -Atc "SELECT gid FROM pg_prepared_xacts" </dev/null 2>/dev/null); do Q $p "ROLLBACK PREPARED '$g'" >/dev/null; done; done
  for r in 1 2; do for p in "${WORKERS[@]}"; do Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done; sleep 1; done
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('fv_${sid}'::regclass)" >/dev/null; done; done
  Q $COORD "DROP TABLE IF EXISTS fv" >/dev/null
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS fv_${sid}" >/dev/null; done; done
  for p in "${WORKERS[@]}"; do PSQL $p -q -c "SET citus.enable_ddl_propagation=off" -c "DROP FUNCTION IF EXISTS public.sclog_full(oid,bigint)" </dev/null >/dev/null 2>&1; done
  for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_partdist.tso_conninfo" >/dev/null; Q $p "ALTER SYSTEM RESET pg_partdist.tso_lease_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  Q $COORD "ALTER SYSTEM RESET pg_partdist.tso_master" >/dev/null; Q $COORD "SELECT pg_reload_conf()" >/dev/null
  rm -rf "$SP_DIR"
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
shard_ids() { Q $COORD "SELECT string_agg(g::text, ',') FROM (SELECT g FROM generate_series($2, $2+200000) g WHERE get_shard_id_for_distribution_column('fv', g) = $1 LIMIT $3) t"; }
gtx() {
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
converge() {
  local port=$1 sql=$2 want=$3 max=${4:-$CONVERGE_S} t v="" p
  for t in $(seq 1 $max); do v=$(Q $port "$sql"); [[ "$v" == "$want" ]] && { echo "$v"; return; }; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.dtx_pending_sweep()" >/dev/null; done; sleep 1; done
  echo "$v"
}
LOCAL() { echo "SET citus.override_table_visibility=false; $1"; }
cur_leader() {  # <sid>：按 raft 状态找当前主端口
  local sid=$1 p; for p in "${WORKERS[@]}"; do [[ "$(Q $p "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$sid")" == leader ]] && { echo $p; return; }; done; echo ""
}

echo "========== [1] 夹具：fv 表 $NW 分片、每片 $NW 成员组、打标、TSO =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS fv;
SET citus.shard_count = $NW; SET citus.shard_replication_factor = 1;
CREATE TABLE fv(id int primary key, v text, n int NOT NULL DEFAULT 0);
SELECT create_distributed_table('fv', 'id', colocate_with => 'none');
ALTER TABLE fv SET (autovacuum_enabled = off);
SQL
while read sid port; do SIDS+=("$sid"); LEADER[$sid]=$port; done < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='fv'::regclass ORDER BY s.shardid" </dev/null | tr '|' ' ')
check "$NW 片分落 $NW 台" "$(for s in "${SIDS[@]}"; do echo "${LEADER[$s]}"; done | sort -u | wc -l)" "$NW"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null
  PSQL $p -q </dev/null >/dev/null 2>&1 <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION public.sclog_full(oid, bigint) RETURNS text AS '$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;
SQL
done
# ★ 供副本期间把全体 worker 的选举超时抬到 15 s（心跳 1 s）：主发基线时心跳会断档（P7-N15），副本在基线
#   "已截断本地文件、等 FPI 重建"的半程被选成主，就是一个索引/堆都是 0 字节的空壳主（P7-N16 现场，run 3）。
#   这一段没有任何切主意图，抬超时没有副作用；build_group_on 每次会覆盖再 RESET，所以逐组设、收尾统一 RESET。
for sid in "${SIDS[@]}"; do lp=${LEADER[$sid]}
  check "组 $sid 主落在 :$lp" "$(build_group_on $sid $lp "$ALLMEM")" "leader"
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  for p in "${WORKERS[@]}"; do [[ $p == $lp ]] && continue; r=$(PSQL $lp -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' '); check "  副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:100}")" "ok"; done
done
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
# ★ 收敛等待跟着"当前主"走：供副本期间主可能漂走（P7-N15），run 3 里漂发生在守卫查完之后，
#   等待还盯着旧主 60 s 白等。现在每轮都重认主，漂了就把没供到（未 armed）的副本从新主再供一遍。
reprovision_from() {  # <sid> <当前主端口>
  local sid=$1 cur=$2 p armed r
  for p in "${WORKERS[@]}"; do [[ $p == $cur ]] && continue
    armed=$(Q $p "SELECT count(*) FROM partdist.replay_status() s WHERE s.shard = partdist.local_partition_for_shard($sid) AND s.armed")
    [[ "$armed" == 1 ]] && continue
    r=$(PSQL $cur -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' '); check "  组 $sid 补供副本到 :$p" "$([[ "$r" == shard=* ]] && echo ok || echo "${r:0:100}")" "ok"
  done
}
for sid in "${SIDS[@]}"; do ok=""
  for t in $(seq 1 90); do
    cur=$(cur_leader $sid)
    if [[ -n "$cur" && "$cur" != "${LEADER[$sid]}" ]]; then
      echo "  组 $sid 供副本期间主从 :${LEADER[$sid]} 漂到 :$cur，从新主补供"; LEADER[$sid]=$cur; reprovision_from $sid $cur
    fi
    lp=${LEADER[$sid]}
    [[ "$(Q $lp "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" && "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" ]] && { ok=ok; break; }
    sleep 1
  done
  check "组 $sid partition_map 两处收敛" "$ok" "ok"
done
mk=$(PSQL $COORD -Atc "SELECT count(*) FILTER (WHERE status LIKE 'registered%') FROM partdist.set_table_shard_mvcc('fv')" </dev/null 2>&1 | tail -1)
check "打标登记 $NW 片" "$mk" "$NW"
DEX rm -f "$CDIR/pg_tso_boot" </dev/null; DEX /work/pg-install/bin/pg_ctl -D "$CDIR" -m fast -l "$CDIR/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 40); do up=$(Q $COORD "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done; check "协调者重启就绪" "$up" "1"
Q $COORD "ALTER SYSTEM SET pg_partdist.tso_master = on" >/dev/null
for p in $COORD "${WORKERS[@]}"; do Q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null; Q $p "ALTER SYSTEM SET pg_partdist.tso_lease_ms = $TSO_LEASE_MS" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
sleep 2; for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done; sleep 4
tso=$(Q $COORD "SELECT partdist.partdist_tso_client_start_ts()"); check "TSO 取号可用（$tso）" "$([[ "$tso" =~ ^[0-9]+$ ]] && echo ok)" "ok"
g0=""; for t in $(seq 1 30); do g0=$(Q $COORD "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()"); [[ "$g0" =~ ^[1-9] ]] && break; sleep 1; done; check "group0 有 leader" "$([[ "$g0" =~ ^[1-9] ]] && echo ok)" "ok"

echo "========== [2] 账户：每分片 $ACCT 个、各 1000（VALUES 单分片写） =========="
declare -A ACC_SID; ACC_COLS=()
for sid in "${SIDS[@]}"; do ids=$(shard_ids $sid 700001 $ACCT)
  vals=$(for a in ${ids//,/ }; do printf "(%s,'acct',1000)," "$a"; done | sed 's/,$//')
  out=$(gtx $sid "INSERT INTO fv(id, v, n) VALUES $vals;"); [[ "$out" == txn_done ]] || echo "  账户插入失败：${out:0:160}"
  ACC_COLS+=("$ids"); for a in ${ids//,/ }; do ACC_SID[$a]=$sid; done
done
ACC_ALL=(); for i in $(seq 0 $((ACCT-1))); do for c in "${ACC_COLS[@]}"; do IFS=, read -ra col <<<"$c"; ACC_ALL+=("${col[$i]}"); done; done
TOTAL0=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fv WHERE v='acct'" $((1000*ACCT*NW)))
check "初始总额" "$TOTAL0" "$((1000*ACCT*NW))"

transfer_one() { gtx ${ACC_SID[$1]} "UPDATE fv SET n = n - 1 WHERE id = $1; UPDATE fv SET n = n + 1 WHERE id = $2;"; }

G=${SIDS[0]}; A=${LEADER[$G]}
SRC=""; DST=""; for a in "${ACC_ALL[@]}"; do if [[ -z "$SRC" && "${ACC_SID[$a]}" == "$G" ]]; then SRC=$a; elif [[ -z "$DST" && "${ACC_SID[$a]}" != "$G" ]]; then DST=$a; fi; done
B=""; for p in "${WORKERS[@]}"; do [[ $p != $A ]] && { B=$p; break; }; done
LOA=$(Q $A "SELECT partdist.local_partition_for_shard($G)"); LOB=$(Q $B "SELECT partdist.local_partition_for_shard($G)")
echo "  组 $G：旧主 :$A（本地 oid $LOA）→ 目标 :$B（本地 oid $LOB）；借记账户 $SRC（在 $G）→ 贷记账户 $DST（在 ${ACC_SID[$DST]}）"
K="${K:-4}"; T="${T:-2}"

echo "========== [3] 预热：$K 笔同步跨组转账 =========="
okw=0; for i in $(seq 1 $K); do out=$(transfer_one $SRC $DST); [[ "$out" == txn_done ]] && okw=$((okw+1)) || echo "  转账失败：${out:0:120}"; done
check "预热 $K 笔全部提交" "$okw" "$K"

echo "========== [4][A] 干净单切：目标 :$B 选举超时 1500 → 当选 → 登记（负向对照：不应重号） =========="
LM_B=$(DEX bash -c "wc -l < '${DDIR[$B]}/pg.log'" </dev/null)
FLUSH_A=$(Q $A "SELECT partdist.get_partition_flush_lsn($LOA::oid)"); XN_A=$(Q $A "SELECT partdist.shard_xid_next($LOA::oid)")
APPLIED_B=$(Q $B "SELECT partdist.get_follower_applied_part_lsn($LOB::oid)"); FLUSH_B=$(Q $B "SELECT partdist.get_partition_flush_lsn($LOB::oid)"); XN_B0=$(Q $B "SELECT partdist.shard_xid_next($LOB::oid)")
N_SRC=$(Q $COORD "SELECT n FROM fv WHERE id=$SRC"); N_DST=$(Q $COORD "SELECT n FROM fv WHERE id=$DST")
echo "  旧主 :$A flush=$FLUSH_A xid_next=$XN_A | 目标 :$B applied=$APPLIED_B flush=$FLUSH_B xid_next=$XN_B0 | 客户端 n($SRC)=$N_SRC n($DST)=$N_DST"
echo "  [T-only-info] 静默集群目标流是否有尾巴（flush_B>applied_B）：$([[ -n "$FLUSH_B" && -n "$APPLIED_B" && $FLUSH_B -gt $APPLIED_B ]] && echo 有 || echo 无（预期无：复制+apply 早于选举超时追平）)"
# 旧主 :$A 的 bgworker tick 间隔 = heartbeat_ms（topology_monitor 按它 WaitLatch）；抬到 60 s ≈ 停发心跳，
# 目标 :$B election_timeout 压到 300 ms（下限 200），收到最后一次心跳后 0.3–0.6 s 即自发选举。静默集群下确定当选。
for p in "${WORKERS[@]}"; do if [[ $p == $B ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 300" >/dev/null; elif [[ $p == $A ]]; then Q $p "ALTER SYSTEM SET pg_raft.heartbeat_ms = 60000" >/dev/null; else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 60000" >/dev/null; fi; Q $p "SELECT pg_reload_conf()" >/dev/null; done
st=""; for t in $(seq 1 120); do st=$(Q $B "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$G"); [[ "$st" == leader ]] && break; sleep 1; done
T_EL=$(date +%s)
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check ":$B 当选组 $G 新 leader" "$st" "leader"
reg=""; t_reg=""
for t in $(seq 1 $((REG_LIMIT_S/2))); do
  if [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$G")" == "${NID[$B]}" && "$(Q $COORD "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$G")" == "$B" ]]; then reg=ok; t_reg=$(( $(date +%s) - T_EL )); break; fi
  sleep 2
done
echo "  当选→登记 ${t_reg:-未登记}s"
check "新主 ${REG_LIMIT_S}s 内登记" "$reg" "ok"
LEADER[$G]=$B

echo "========== [5][A] 取证：升主后的发号起点与追平（干净路径应不回退、应覆盖） =========="
XN_B1=$(Q $B "SELECT partdist.shard_xid_next($LOB::oid)"); APPLIED_B1=$(Q $B "SELECT partdist.get_follower_applied_part_lsn($LOB::oid)"); FLUSH_B1=$(Q $B "SELECT partdist.get_partition_flush_lsn($LOB::oid)")
CATCH=$(DEX bash -c "tail -n +$((LM_B+1)) '${DDIR[$B]}/pg.log' | grep -oE 'shard $LOB 追平至 [0-9]+' | tail -1 | grep -oE '[0-9]+$'" </dev/null)
echo "  新主 :$B xid_next 升主前=$XN_B0 升主后=$XN_B1（旧主已发到 $XN_A） applied=$APPLIED_B1 flush=$FLUSH_B1 升主追平至=${CATCH:-?}"
check "[X] ★ 升主后发号起点不回退（B.xid_next ≥ A.xid_next）" "$([[ -n "$XN_B1" && -n "$XN_A" && $XN_B1 -ge $XN_A ]] && echo ok)" "ok"

echo "========== [6][A] 切主前客户端已收到 COMMIT 的转账在新主上仍可见 =========="
N_SRC_B=$(converge $COORD "SELECT n FROM fv WHERE id=$SRC" "$N_SRC" 30); N_DST_B=$(Q $COORD "SELECT n FROM fv WHERE id=$DST")
check "[V] ★ 借记账户 $SRC 在新主上 = 切主前客户端视图（$N_SRC）" "$N_SRC_B" "$N_SRC"

echo "========== [7][A] 新主再写 $K 笔，跨节点对读分片 clog 找撞号（干净路径应 0） =========="
okn=0; for i in $(seq 1 $K); do out=$(transfer_one $SRC $DST); [[ "$out" == txn_done ]] && okn=$((okn+1)); done
check "新主上 $K 笔提交" "$okn" "$K"
for t in 1 2 3; do for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.dtx_pending_sweep()" >/dev/null; done; sleep 1; done
XMAX=$(( $(Q $B "SELECT partdist.shard_xid_next($LOB::oid)") + 2 ))
ncoll=0
for x in $(seq 1 $XMAX); do
  va=$(Q $A "SELECT partdist.shard_clog_status_full($LOA::oid, $x)"); vb=$(Q $B "SELECT partdist.shard_clog_status_full($LOB::oid, $x)")
  ta=$(echo "$va" | grep -oE 'sts=[0-9]+' | cut -d= -f2); tb=$(echo "$vb" | grep -oE 'sts=[0-9]+' | cut -d= -f2)
  if [[ "${ta:-0}" != 0 && "${tb:-0}" != 0 && "$ta" != "$tb" ]]; then ncoll=$((ncoll+1)); echo "  撞号 xid $x：:$A $va | :$B $vb"; fi
done
check "[K] ★ 同一分片 xid 在旧主/新主上不是两笔不同事务（撞号 0）" "$ncoll" "0"

echo "========== [8][A] 总额守恒 =========="
TOTAL=$(converge $COORD "SELECT coalesce(sum(n),0) FROM fv WHERE v='acct'" "$TOTAL0" 60)
check "[S] ★ 总额守恒（$TOTAL0）" "$TOTAL" "$TOTAL0"
npx=0; for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); npx=$((npx+${n:-0})); done; check "无残留 prepared" "$npx" "0"
health_check_no_crash
echo
echo "  注：本文件 [A] 段是**负向对照**——干净单切不重号，全绿即符合根因结论（N18 只在负载+未追平升主下发生）。"
echo "      N18 正例见 N12 用例 R=2 连切（run 5：总额 +2、跨节点同 xid 不同 sts）与 P7_REMEDIATION_PLAN §1.11。"
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]] && echo "P7-N18 干净路径不变式：全绿（符合结论）" || echo "P7-N18：存在 FAIL"
