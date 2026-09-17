#!/usr/bin/env bash
# [宿主机] P7-N12 复现 / 回归：跨组事务负载下切主，已提交的判决不许丢，账户总额必须守恒。
#
# 机理（09-17 定因）：参与者学到判决后把它落进本地分片 clog，再把判决标记复制给副本；
# 复制失败（典型：它刚丢了主，写栅栏拒绝）时原本只告警、照样注销未决登记 ⇒ 回执 ⇒ 协调组收齐回执
# FORGET 决议 ⇒ 接任的新主手里只有 PREPARED、升主序列的 in-doubt 闭合四级全查不到 ⇒
# 这笔已提交的行在新主上永久不可见（转账的一侧丢了，总额漂移）。修法：复制失败保留登记，
# 挡住回执与 FORGET；主权登记转走后再注销。
#
# 本用例：TX2 全栈（打标 + TSO + join），NW 分片每片一组、每台一主两从；账户按分片交错；
# P 个会话持续跨组转账（各会话账户不重叠、每笔跨两组），期间对同一个组做 R 次确定性切主
# （旧主停心跳 → 指定 follower 当选 → 登记 → 负载继续）。最后停负载、等判决收敛，断言：
#   [F] 每次切主：新主在上界内登记
#   [G] ★ 总额守恒；每个分片当前主本地内容 = 协调者读到的该分片内容
#   [H] ★ 各主上没有"PREPARED 却已有 COMMIT 决议"的分片 xid（判决没丢）
#   [I] 复制失败告警若出现，登记必须保留到主权转走（日志里成对出现"保留"与"视为已交接"）—— 只报
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
ACCT="${ACCT:-8}"; P="${P:-4}"; R="${R:-2}"; REG_LIMIT_S="${REG_LIMIT_S:-180}"; CONVERGE_S="${CONVERGE_S:-120}"
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
STOP_FILE="$SP_DIR/stop"
bg_worker() {
  local s=$1 f=$2 i=0 ok=0 bad=0 out; local mine=() j
  for j in "${!ACC_ALL[@]}"; do [[ $(( j % P )) -eq $(( s - 1 )) ]] && mine+=("${ACC_ALL[$j]}"); done
  local nm=${#mine[@]}
  while [[ ! -f "$STOP_FILE" ]]; do
    out=$(transfer_one "${mine[$(( i % nm ))]}" "${mine[$(( (i+1) % nm ))]}"); i=$((i+1))
    if [[ "$out" == txn_done ]]; then ok=$((ok+1)); else bad=$((bad+1)); echo "$out" >> "$f.err"; sleep 3; fi   # 失败退避 3 s：别把 2 vCPU 空转打满
    echo "$ok $bad" > "$f"
  done
}
echo "========== [3] 负载 $P 会话持续跨组转账；对组 ${SIDS[0]} 做 $R 次切主 =========="
for s in $(seq 1 $P); do bg_worker $s "$SP_DIR/b$s" & BG_PIDS+="$! "; done
sleep 15
FS=${SIDS[0]}
# ★ 目标只挑**从未当过该组主**的节点：丢主的旧主是"带陈旧数据回归"的情形，升主前置按设计返回 -1
#   拒绝（R-P4-15，须先重做基线），拿它当目标验的是别的东西（P7-N14）。3 台 ⇒ 最多 2 轮。
BEEN_LEADER=" $(cur_leader $FS) "
for round in $(seq 1 $R); do
  OLD=$(cur_leader $FS); NEWL=""
  for p in "${WORKERS[@]}"; do [[ $p != $OLD && "$BEEN_LEADER" != *" $p "* ]] && { NEWL=$p; break; }; done
  [[ -z "$NEWL" ]] && { echo "  第 $round 轮：没有从未当过主的节点可选，停止切主"; break; }
  BEEN_LEADER+="$NEWL "
  [[ -z "$OLD" || -z "$NEWL" ]] && { echo "  第 $round 轮：找不到当前主，跳过"; continue; }
  for p in "${WORKERS[@]}"; do
    if [[ $p == $NEWL ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null
    elif [[ $p == $OLD ]]; then Q $p "ALTER SYSTEM SET pg_raft.heartbeat_ms = 10000" >/dev/null
    else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 60000" >/dev/null; fi
    Q $p "SELECT pg_reload_conf()" >/dev/null
  done
  st=""; for t in $(seq 1 180); do st=$(Q $NEWL "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$FS"); [[ "$st" == leader ]] && break; sleep 1; done
  T_EL=$(date +%s)
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  check "第 $round 轮：:$NEWL 当选组 $FS 新 leader（旧主 :$OLD）" "$st" "leader"
  reg=""; t_reg=""
  for t in $(seq 1 $((REG_LIMIT_S/2))); do
    if [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$FS")" == "${NID[$NEWL]}" && "$(Q $COORD "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$FS")" == "$NEWL" ]]; then reg=ok; t_reg=$(( $(date +%s) - T_EL )); break; fi; sleep 2
  done
  echo "  第 $round 轮：当选→登记 ${t_reg:-未登记}s"
  check "[F] 第 $round 轮：新主 ${REG_LIMIT_S}s 内登记" "$reg" "ok"
  LEADER[$FS]=$NEWL
  sleep 25
done
touch "$STOP_FILE"; for x in $BG_PIDS; do wait "$x" 2>/dev/null; done; BG_PIDS=""
okb=0; badb=0; for s in $(seq 1 $P); do read o b < "$SP_DIR/b$s"; okb=$((okb+o)); badb=$((badb+b)); done
echo "  负载期间转账：成功 $okb、失败 $badb"
[[ $badb -gt 0 ]] && cat "$SP_DIR"/b*.err 2>/dev/null | sed 's/[0-9]\{3,\}/N/g' | cut -c1-140 | sort | uniq -c | sort -rn | head -6 | sed 's/^/    /'

echo "========== [G] ★ 判决收敛后总额守恒；各分片当前主本地内容 = 协调者视图 =========="
check "★ 总额守恒" "$(converge $COORD "SELECT coalesce(sum(n),0) FROM fv WHERE v='acct'" "$TOTAL0")" "$TOTAL0"
for sid in "${SIDS[@]}"; do lp=$(cur_leader $sid); [[ -z "$lp" ]] && lp=${LEADER[$sid]}
  CS=$(Q $COORD "WITH x AS MATERIALIZED (SELECT id, v, n FROM fv) SELECT count(*)||'/'||coalesce(sum(n),0)||'/'||coalesce(md5(string_agg(id||':'||v||':'||n, ',' ORDER BY id)),'') FROM x WHERE get_shard_id_for_distribution_column('fv', x.id) = $sid")
  LS=$(converge $lp "$(LOCAL "SELECT count(*)||'/'||coalesce(sum(n),0)||'/'||coalesce(md5(string_agg(id||':'||v||':'||n, ',' ORDER BY id)),'') FROM fv_$sid")" "$CS" 60)
  check "★ 组 $sid 当前主 :$lp 本地内容 = 协调者视图" "$LS" "$CS"
done

echo "========== [H] ★ 各主没有'PREPARED 却已有 COMMIT 决议'的分片 xid（判决没丢） =========="
lost=0
for sid in "${SIDS[@]}"; do lp=$(cur_leader $sid); [[ -z "$lp" ]] && continue
  lo=$(Q $lp "SELECT partdist.local_partition_for_shard($sid)")
  nx=$(Q $lp "SELECT partdist.shard_xid_next($lo)")
  prep=$(PSQL $lp -At </dev/null 2>/dev/null <<SQL | grep -v '^SET'
SELECT string_agg(x::text, ',') FROM generate_series(3, ${nx:-3}-1) x WHERE public.sclog_full(${lo}::oid, x) LIKE 'st=1 %';
SQL
)
  [[ -z "$prep" ]] && { echo "  组 $sid 主 :$lp：无 PREPARED 残留"; continue; }
  echo "  组 $sid 主 :$lp：PREPARED 残留 xid = $prep"
  for x in ${prep//,/ }; do echo "      xid $x: $(Q $lp "SELECT public.sclog_full(${lo}::oid, $x)")  各节点: $(for q in "${WORKERS[@]}"; do printf ':%s=%s ' $q "$(Q $q "SELECT split_part(public.sclog_full(partdist.local_partition_for_shard($sid)::oid, $x),' ',1)")"; done)"; done
  # 这些 xid 若在任一节点的 dtx_decision 里有 COMMIT 决议（按分片 clog 记的 gxid 关联不便，这里按"任一残留 = 报"处理）
  lost=$((lost + $(tr ',' '\n' <<<"$prep" | wc -l)))
done
check "★ 各主 PREPARED 残留（判决没送到的笔数）" "$lost" "0"

echo "========== [I] 日志：复制失败告警与登记保留/交接（只报） =========="
i=0; for p in "${WORKERS[@]}"; do
  n1=$(DEX bash -c "tail -n +$((${LOGMARK[$i]}+1)) '${DDIR[$p]}/pg.log' | grep -c '判决标记未能复制给副本'" </dev/null)
  n2=$(DEX bash -c "tail -n +$((${LOGMARK[$i]}+1)) '${DDIR[$p]}/pg.log' | grep -c '视为已交接，注销'" </dev/null)
  echo "  :$p 复制失败告警 $n1 条；'视为已交接' $n2 条"; i=$((i+1))
done
np=0; for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); np=$((np+${n:-0})); done
check "无残留 prepared" "$np" "0"
health_check_no_crash
echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]] && echo "P7-N12 切主判决守恒：全部通过" || echo "P7-N12 切主判决守恒：存在 FAIL"
