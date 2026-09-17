#!/usr/bin/env bash
# [宿主机] 跨组分布式事务验收（1c+3w 起，拓扑自适应；TX2 全栈：打标 + TSO + join 传播）。
#
# 布局：一张 (worker 数) 分片的表，Citus 轮转后每台正好 1 片；每片一个"全体 worker"成员的
# Raft 组，主 = 落位节点，副本供到其余每台 ⇒ **每个节点都是一个组的主 + 其余组的从**。
# 一条事务写多个分片 = 一次横跨多个 Raft 组的分布式事务（Citus 2PC + pg_raft 每组同步复制 +
# 协调组判决）。
#
# ★ 为什么必须是 TX2 全栈（09-17 两次失败换来的）：
#   · 表**必须打标**（set_table_shard_mvcc）。未打标的分片副本属"遗留宇宙"，升主后元组带旧主原生
#     xid、新主按本地 clog 判可见性（FOLLOWER_REPLAY_DESIGN §14.2，R3 未实装）—— 首跑未打标，
#     [9] 切主后账户总额 24000→26000、新主本地读与协调者读不一致（P7-N9）。
#   · 打标分片的 2PC 写**必须加入全局事务**：`SET LOCAL pg_partdist.join_info='gxid,start_ts,gsid'`
#     + `SET LOCAL citus.propagate_set_commands='local'`，Citus 把这条 SET 传播到每条任务连接。
#     不带就在 PREPARE 被拒（"未加入全局事务的分片写不允许 PREPARE TRANSACTION"，二跑实测）。
#     gxid / start_ts 取自协调者上的 TSO，所以 TSO 必须配好（tso_master + 各节点 tso_conninfo）。
#   · 打标行**不是提交即可见**，要等协调组判决收敛；断言前轮询 dtx_pending_sweep()。
#
# 断言：
#   [2] 跨组提交：一条全局事务给每个分片各插 K 行，各主上各 K 行、协调者总数对
#   [3] 同一全局事务里跨组 UPDATE + DELETE，全部生效
#   [4] 显式 ROLLBACK：跨组写入与修改一条都不留
#   [5] 中途失败（重复主键）：整条回滚，一行不落
#   [6] 并发跨组转账：P 个会话同时转账（各会话账户不重叠、每笔跨两个组），总额守恒
#   [7] 每个组的每个副本追平到主的位点，且主堆与副本主堆掩码外逐字节一致
#   [8] 各节点无残留 prepared 事务
#   [9] ★ 跨组负载下切主（P7-N4 回归）：后台持续跨组转账时让一个组换主，新主在上界内登记，
#       负载恢复成功，总额守恒，新主本地内容与协调者读到的一致（P7-N5 守卫）
#   [10] 节点健康；日志环无丢弃；捕获环无覆盖
#
# 夹具规则：建组偏置选举超时（25）、副本 provision_shard_replica（24）、partition_map 两处收敛（32）、
#   ALTER SYSTEM 独占一条 -c（2）、重启协调者前删 pg_tso_boot（1）、catchup 给上界（7）、
#   收尾三台拆组两轮（16）、跑着的 .sh 不许改、pkill 不许匹配到自己。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
K="${K:-40}"
ACCT="${ACCT:-8}"
P="${P:-4}"
T="${T:-15}"
REG_LIMIT_S="${REG_LIMIT_S:-120}"
CONVERGE_S="${CONVERGE_S:-90}"
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
ngroups=0; np0=0
for p in "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"); ngroups=$((ngroups + ${n:-0})); done
for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); np0=$((np0 + ${n:-0})); done
check "起跑时没有残留数据组（净场）" "$ngroups" "0"
check "起跑时没有残留 prepared 事务（净场，规则 10）" "$np0" "0"

declare -A LEADER
SIDS=()
BG_PIDS=""
tso_reset() {
  local p
  for p in $COORD "${WORKERS[@]}"; do
    Q $p "ALTER SYSTEM RESET pg_partdist.tso_conninfo" >/dev/null
    Q $p "SELECT pg_reload_conf()" >/dev/null
  done
  Q $COORD "ALTER SYSTEM RESET pg_partdist.tso_master" >/dev/null
  Q $COORD "SELECT pg_reload_conf()" >/dev/null
}
cleanup() {
  local r p sid g
  for x in $BG_PIDS; do kill "$x" 2>/dev/null; done
  for p in "${WORKERS[@]}"; do
    Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null
    Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null
    Q $p "SELECT pg_reload_conf()" >/dev/null
  done
  if [[ "${KEEP_ON_FAIL:-1}" == 1 && "$FAIL" -gt 0 ]]; then
    echo "  [保留现场] 有 FAIL：组、表 cg、TSO 配置都未动（选举 GUC 已复位）。取证后手工清理，或 KEEP_ON_FAIL=0 重跑"
    return
  fi
  # 残留 prepared 会堵住之后所有 DDL（规则 10）：先收掉再删表
  for p in $COORD "${WORKERS[@]}"; do
    for g in $(PSQL $p -Atc "SELECT gid FROM pg_prepared_xacts" </dev/null 2>/dev/null); do Q $p "ROLLBACK PREPARED '$g'" >/dev/null; done
  done
  for r in 1 2; do for p in "${WORKERS[@]}"; do
    Q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null
  done; sleep 1; done
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.replay_disable('cg_${sid}'::regclass)" >/dev/null; done; done
  Q $COORD "DROP TABLE IF EXISTS cg" >/dev/null
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do Q $p "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS cg_${sid}" >/dev/null; done; done
  tso_reset
  echo "  [复原] prepared 已收、组已拆、表已删、选举与 TSO 配置已 RESET"
}
trap cleanup EXIT

build_group_on() {   # <gid> <leader端口> <成员>（规则 25）
  local gid=$1 want=$2 mem=$3 t p st=""
  for p in "${WORKERS[@]}"; do
    if [[ "$p" == "$want" ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null
    else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 9000" >/dev/null; fi
    Q $p "SELECT pg_reload_conf()" >/dev/null
  done
  for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.pg_raft_group_create($gid, $mem)" >/dev/null; done
  for t in $(seq 1 40); do st=$(Q $want "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$gid"); [[ "$st" == leader ]] && break; sleep 1; done
  for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
  echo "$st"
}
shard_ids() {   # <sid> <起始id> <个数> [偏移]：只落该分片的 id，逗号分隔
  Q $COORD "SELECT string_agg(g::text, ',') FROM (SELECT g FROM generate_series($2, $2 + 200000) g WHERE get_shard_id_for_distribution_column('cg', g) = $1 OFFSET ${4:-0} LIMIT $3) t"
}
shard_vals() {  # <sid> <起始id> <个数> <v>：多行 VALUES（Citus 逐行剪枝 ⇒ 单分片任务）
  Q $COORD "SELECT string_agg('('||g||',''$4'')', ',') FROM (SELECT g FROM generate_series($2, $2 + 200000) g WHERE get_shard_id_for_distribution_column('cg', g) = $1 LIMIT $3) t"
}
# 在一条"加入全局事务"的事务里执行 DML。<gsid> 取写集里的一个组。<结尾> 默认 COMMIT。
# 输出一行：成功 = txn_done；失败 = 全部输出拼成一行（错误原文在里面）。
gtx() {  # <gsid> <dml> [结尾]
  PSQL $COORD -At -v ON_ERROR_STOP=1 </dev/null 2>&1 <<SQL | grep -vE '^(BEGIN|SET|COMMIT|ROLLBACK|INSERT [0-9 ]+|UPDATE [0-9]+|DELETE [0-9]+)$' \
    | awk '{a[NR]=$0} END{ if (NR>0 && a[NR]=="txn_done") print "txn_done"; else { s=""; for(i=1;i<=NR;i++) s=s (i>1?" | ":"") a[i]; print s } }'
SELECT partdist.partdist_gxid_next()||','||partdist.partdist_tso_client_start_ts()||','||$1 AS ji \gset
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = :'ji';
$2
${3:-COMMIT};
SELECT 'txn_done';
SQL
}
# 等判决收敛：轮询 <check_sql> 直到等于 <期望>，其间在全部 worker 上推 dtx_pending_sweep()
converge() {  # <port> <check_sql> <期望> [秒]
  local port=$1 sql=$2 want=$3 max=${4:-$CONVERGE_S} t v="" p
  for t in $(seq 1 $max); do
    v=$(Q $port "$sql")
    [[ "$v" == "$want" ]] && { echo "$v"; return; }
    for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.dtx_pending_sweep()" >/dev/null; done
    sleep 1
  done
  echo "$v"
}
LOCAL() { echo "SET citus.override_table_visibility=false; $1"; }

echo "========== [1] 夹具：cg 表 $NW 分片、每片一个 $NW 成员组、每台一主 $((NW-1)) 从 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS cg;
SET citus.shard_count = $NW; SET citus.shard_replication_factor = 1;
CREATE TABLE cg(id int primary key, v text, n int NOT NULL DEFAULT 0);
SELECT create_distributed_table('cg', 'id', colocate_with => 'none');
ALTER TABLE cg SET (autovacuum_enabled = off);
SQL
while read sid port; do SIDS+=("$sid"); LEADER[$sid]=$port; done < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='cg'::regclass ORDER BY s.shardid" </dev/null | tr '|' ' ')
check "$NW 个分片分落 $NW 台（每台一个主）" "$(for s in "${SIDS[@]}"; do echo "${LEADER[$s]}"; done | sort -u | wc -l)" "$NW"
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.rebuild_shard_identity()" >/dev/null; done
for sid in "${SIDS[@]}"; do
  lp=${LEADER[$sid]}
  check "组 $sid 主落在 :$lp" "$(build_group_on $sid $lp "$ALLMEM")" "leader"
  for p in "${WORKERS[@]}"; do
    [[ $p == $lp ]] && continue
    r=$(PSQL $lp -Atc "SELECT partdist.provision_shard_replica(${sid}::bigint, ${NID[$p]})" </dev/null 2>&1 | tr '\n' ' ')
    check "  组 $sid 副本供到 :$p" "$([[ "$r" == shard=* ]] && echo ok)" "ok"
  done
done
for sid in "${SIDS[@]}"; do
  lp=${LEADER[$sid]}; ok=""
  for t in $(seq 1 60); do
    [[ "$(Q $lp "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" &&
       "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$sid")" == "${NID[$lp]}" ]] && { ok=ok; break; }
    sleep 1
  done
  check "组 $sid partition_map 在主节点与协调者两处收敛" "$ok" "ok"
done

echo "========== [1b] 打标：协调者 set_table_shard_mvcc('cg')（表还空着） =========="
mk=$(PSQL $COORD -Atc "SELECT string_agg(shardid||':'||split_part(status,' ',1), ',' ORDER BY shardid) FROM partdist.set_table_shard_mvcc('cg')" </dev/null 2>&1 | tail -1)
echo "  $mk"
check "全部 $NW 个分片登记为打标" "$(tr ',' '\n' <<<"$mk" | grep -c ':registered$')" "$NW"
for sid in "${SIDS[@]}"; do
  lp=${LEADER[$sid]}
  check "  组 $sid 主 :$lp 打标状态" "$(Q $lp "SELECT partdist.shard_mvcc_status(partdist.local_partition_for_shard($sid))")" "registered=yes evidence=yes replica=no"
done

echo "========== [1c] TSO：协调者当 TSO 源，全部节点配 tso_conninfo，取号探针 =========="
DEX rm -f "$CDIR/pg_tso_boot" </dev/null
DEX /work/pg-install/bin/pg_ctl -D "$CDIR" -m fast -l "$CDIR/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 40); do up=$(Q $COORD "SELECT 1"); [[ "$up" == 1 ]] && break; sleep 1; done
check "协调者删 pg_tso_boot 后重启就绪（规则 1）" "$up" "1"
TSO_CI="host=/tmp port=5432 dbname=postgres user=postgres"
Q $COORD "ALTER SYSTEM SET pg_partdist.tso_master = on" >/dev/null
for p in $COORD "${WORKERS[@]}"; do
  Q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo = '$TSO_CI'" >/dev/null
  Q $p "SELECT pg_reload_conf()" >/dev/null
done
sleep 2
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done   # 引流：心跳工作者靠首个取号 backend 缓存 node_id
sleep 4
tso_probe=$(Q $COORD "SELECT partdist.partdist_tso_client_start_ts()")
check "TSO 取号可用（start_ts=$tso_probe）" "$([[ "$tso_probe" =~ ^[0-9]+$ ]] && echo ok)" "ok"
g0=""; for t in $(seq 1 30); do g0=$(Q $COORD "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()"); [[ "$g0" =~ ^[1-9] ]] && break; sleep 1; done
check "协调者重启后 group0 有 leader" "$([[ "$g0" =~ ^[1-9] ]] && echo ok)" "ok"
if [[ ! "$tso_probe" =~ ^[0-9]+$ ]]; then
  echo "FATAL: TSO 取号不可用，后续全局事务无从进行"; echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="; exit 1
fi

echo "========== [2] ★ 跨组提交：一条全局事务给每个分片各插 $K 行 =========="
DML=""; for sid in "${SIDS[@]}"; do DML+="INSERT INTO cg(id, v) VALUES $(shard_vals $sid 1 $K c2);"$'\n'; done
out=$(gtx ${SIDS[0]} "$DML")
check "跨 $NW 组全局事务提交成功" "$out" "txn_done"
check "判决收敛后协调者总行数" "$(converge $COORD "SELECT count(*) FROM cg" $((K*NW)))" "$((K*NW))"
for sid in "${SIDS[@]}"; do check "  组 $sid 主(:${LEADER[$sid]})本地 $K 行" "$(converge ${LEADER[$sid]} "$(LOCAL "SELECT count(*) FROM cg_$sid")" $K 30)" "$K"; done

echo "========== [3] ★ 同一全局事务里跨组 UPDATE + DELETE =========="
DML=""; for sid in "${SIDS[@]}"; do
  DML+="UPDATE cg SET v='c3' WHERE id IN ($(shard_ids $sid 1 10)); DELETE FROM cg WHERE id IN ($(shard_ids $sid 1 5 10));"$'\n'
done
out=$(gtx ${SIDS[1]} "$DML")
check "跨组 UPDATE+DELETE 全局事务提交成功" "$out" "txn_done"
check "协调者：改成 c3 的行" "$(converge $COORD "SELECT count(*) FROM cg WHERE v='c3'" $((10*NW)))" "$((10*NW))"
check "协调者：删后总行数" "$(converge $COORD "SELECT count(*) FROM cg" $(( (K-5)*NW )))" "$(( (K-5)*NW ))"
for sid in "${SIDS[@]}"; do check "  组 $sid 主本地 c3=10 / 总=$((K-5))" "$(converge ${LEADER[$sid]} "$(LOCAL "SELECT count(*) FILTER (WHERE v='c3')||'/'||count(*) FROM cg_$sid")" "10/$((K-5))" 30)" "10/$((K-5))"; done

echo "========== [4] ★ 显式 ROLLBACK：跨组写入与修改一条都不留 =========="
before_cnt=$(Q $COORD 'SELECT count(*) FROM cg'); before_c3=$(Q $COORD "SELECT count(*) FROM cg WHERE v='c3'")
DML=""; for sid in "${SIDS[@]}"; do DML+="INSERT INTO cg(id, v) VALUES $(shard_vals $sid 300001 5 rb); UPDATE cg SET v='rb' WHERE id IN ($(shard_ids $sid 1 10));"$'\n'; done
out=$(gtx ${SIDS[0]} "$DML" ROLLBACK)
check "ROLLBACK 语句执行完" "$out" "txn_done"
sleep 3
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.dtx_pending_sweep()" >/dev/null; done
check "ROLLBACK 后总行数不变" "$(Q $COORD 'SELECT count(*) FROM cg')" "$before_cnt"
check "ROLLBACK 后 c3 行不变" "$(Q $COORD "SELECT count(*) FROM cg WHERE v='c3'")" "$before_c3"
check "ROLLBACK 后没有 rb 行" "$(Q $COORD "SELECT count(*) FROM cg WHERE v='rb'")" "0"
for sid in "${SIDS[@]}"; do check "  组 $sid 主本地没有 rb 行" "$(Q ${LEADER[$sid]} "$(LOCAL "SELECT count(*) FROM cg_$sid WHERE v='rb'")")" "0"; done

echo "========== [5] ★ 中途失败（重复主键）：整条回滚 =========="
DML=""; for sid in "${SIDS[@]}"; do DML+="INSERT INTO cg(id, v) VALUES $(shard_vals $sid 500001 5 bad);"$'\n'; done
DML+="INSERT INTO cg(id, v) VALUES ($(shard_ids ${SIDS[0]} 1 1), 'dup');"
out=$(gtx ${SIDS[0]} "$DML")
check "含重复主键的全局事务报错" "$([[ "$out" == *duplicate* ]] && echo ok || echo "${out:0:120}")" "ok"
sleep 3
for p in "${WORKERS[@]}"; do Q $p "SELECT partdist.dtx_pending_sweep()" >/dev/null; done
check "失败后总行数不变" "$(Q $COORD 'SELECT count(*) FROM cg')" "$before_cnt"
check "失败后没有 bad 行" "$(Q $COORD "SELECT count(*) FROM cg WHERE v='bad'")" "0"
for sid in "${SIDS[@]}"; do check "  组 $sid 主本地没有 bad 行" "$(Q ${LEADER[$sid]} "$(LOCAL "SELECT count(*) FROM cg_$sid WHERE v='bad'")")" "0"; done

echo "========== [6] ★ 并发跨组转账：$P 会话 × $T 笔，总额守恒 =========="
# 账户按分片**交错**排成一列（s0,s1,s2,s0,…），会话 s 取下标 ≡ s-1 (mod P) 的那几个：
# 会话之间账户不重叠（不互相写冲突），同一会话相邻两个账户必落不同分片（每笔都跨两个组）。
declare -A ACC_SID
ACC_COLS=()
for sid in "${SIDS[@]}"; do
  ids=$(shard_ids $sid 700001 $ACCT)
  # ★ 用多行 VALUES，不用 INSERT…SELECT：后者在全局事务里写单个分片会走 2PC，协调组判决走快路径
  #   不产生，分片 clog 永远停在 RUNNING、行永久不可见（P7-N10，三跑实测；见 [9b]）
  vals=$(for a in ${ids//,/ }; do printf "(%s,'acct',1000)," "$a"; done | sed 's/,$//')
  out=$(gtx $sid "INSERT INTO cg(id, v, n) VALUES $vals;")
  [[ "$out" == txn_done ]] || echo "  账户插入失败（分片 $sid）：${out:0:160}"
  ACC_COLS+=("$ids")
  for a in ${ids//,/ }; do ACC_SID[$a]=$sid; done
done
ACC_ALL=()
for i in $(seq 0 $((ACCT-1))); do for c in "${ACC_COLS[@]}"; do IFS=, read -ra col <<<"$c"; ACC_ALL+=("${col[$i]}"); done; done
TOTAL0=$(converge $COORD "SELECT coalesce(sum(n),0) FROM cg WHERE v='acct'" $((1000*ACCT*NW)))
check "账户初始总额（判决收敛后）" "$TOTAL0" "$((1000*ACCT*NW))"
SP_DIR=$(mktemp -d)
transfer_one() {   # <a> <b>：一笔跨组转账，成功输出 txn_done
  gtx ${ACC_SID[$1]} "UPDATE cg SET n = n - 1 WHERE id = $1; UPDATE cg SET n = n + 1 WHERE id = $2;"
}
transfer_worker() {   # <会话号> <笔数> <结果文件>
  local s=$1 cnt=$2 f=$3 i ok=0 bad=0 out
  local mine=() j
  for j in "${!ACC_ALL[@]}"; do [[ $(( j % P )) -eq $(( s - 1 )) ]] && mine+=("${ACC_ALL[$j]}"); done
  local nm=${#mine[@]}
  for i in $(seq 0 $((cnt-1))); do
    out=$(transfer_one "${mine[$(( i % nm ))]}" "${mine[$(( (i+1) % nm ))]}")
    if [[ "$out" == txn_done ]]; then ok=$((ok+1)); else bad=$((bad+1)); echo "$out" >> "$f.err"; fi
  done
  echo "$ok $bad" > "$f"
}
t0=$(date +%s)
for s in $(seq 1 $P); do transfer_worker $s $T "$SP_DIR/w$s" & done; wait
T6=$(( $(date +%s) - t0 ))
okt=0; badt=0; for s in $(seq 1 $P); do read o b < "$SP_DIR/w$s"; okt=$((okt+o)); badt=$((badt+b)); done
echo "  $((P*T)) 笔用时 ${T6}s：成功 $okt、失败 $badt"
[[ $badt -gt 0 ]] && cat "$SP_DIR"/w*.err 2>/dev/null | sed 's/[0-9]\{3,\}/N/g' | sort | uniq -c | head -5 | sed 's/^/    /'
check "★ 并发转账全部成功（会话间账户不重叠，不该有冲突）" "$badt" "0"
check "★ 判决收敛后总额守恒" "$(converge $COORD "SELECT coalesce(sum(n),0) FROM cg WHERE v='acct'" "$TOTAL0")" "$TOTAL0"
check "  余额偏移总量与成功笔数相容" "$(Q $COORD "SELECT sum(abs(n-1000)) FROM cg WHERE v='acct'" | awk -v ok=$okt '{print ($1<=2*ok && ($1%2)==0)?"ok":"偏移="$1" 成功="ok}')" "ok"

echo "========== [7] ★ 每个组的每个副本追平并与主逐字节一致 =========="
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
for sid in "${SIDS[@]}"; do
  lp=${LEADER[$sid]}
  llo=$(Q $lp "SELECT partdist.local_partition_for_shard($sid)")
  bound=$(Q $lp "SELECT partdist.get_partition_flush_lsn($llo)")
  PSQL $lp -q -c "CHECKPOINT" </dev/null >/dev/null
  lpath="${DDIR[$lp]}/$(Q $lp "$(LOCAL "SELECT pg_relation_filepath('cg_$sid')")")"
  for p in "${WORKERS[@]}"; do
    [[ $p == $lp ]] && continue
    got=""; for t in $(seq 1 40); do
      got=$(Q $p "SELECT partdist.replay_catchup('cg_$sid', $bound, 10000)")
      [[ "$got" =~ ^[0-9]+$ && "$got" -ge "$bound" ]] && break; sleep 1
    done
    PSQL $p -q -c "CHECKPOINT" </dev/null >/dev/null
    rpath="${DDIR[$p]}/$(Q $p "SET citus.enable_ddl_propagation=off; SELECT pg_relation_filepath('cg_$sid')")"
    same=$(DEX python3 /tmp/pagecmp.py --kind=heap "$lpath" "$rpath" </dev/null 2>/dev/null)
    check "  组 $sid 副本 :$p 追平($got/$bound) 且主堆逐字节一致" "$([[ "$got" =~ ^[0-9]+$ && "$got" -ge "$bound" ]] && echo "$same")" "IDENTICAL_OUTSIDE_HOLE"
  done
done

echo "========== [8] ★ 各节点无残留 prepared 事务 =========="
np=0; for p in $COORD "${WORKERS[@]}"; do n=$(Q $p "SELECT count(*) FROM pg_prepared_xacts"); np=$((np + ${n:-0})); done
check "全部节点 pg_prepared_xacts 为空" "$np" "0"

echo "========== [9] ★ 跨组负载下切主（P7-N4 回归 / P7-N5 守卫） =========="
FS=${SIDS[0]}; OLD=${LEADER[$FS]}
NEWL=""; for p in "${WORKERS[@]}"; do [[ $p != $OLD ]] && { NEWL=$p; break; }; done
echo "  组 $FS：旧主 :$OLD → 指定新主 :$NEWL；切主期间 $P 个会话持续跨组转账"
TOTAL_BEFORE=$(Q $COORD "SELECT sum(n) FROM cg WHERE v='acct'")
STOP_FILE="$SP_DIR/stop"
bg_worker() {   # <会话号> <结果文件>
  local s=$1 f=$2 i=0 ok=0 bad=0 last_ok=0 out
  local mine=() j
  for j in "${!ACC_ALL[@]}"; do [[ $(( j % P )) -eq $(( s - 1 )) ]] && mine+=("${ACC_ALL[$j]}"); done
  local nm=${#mine[@]}
  while [[ ! -f "$STOP_FILE" ]]; do
    out=$(transfer_one "${mine[$(( i % nm ))]}" "${mine[$(( (i+1) % nm ))]}")
    i=$((i+1))
    if [[ "$out" == txn_done ]]; then ok=$((ok+1)); last_ok=$(date +%s); else bad=$((bad+1)); echo "$out" >> "$f.err"; sleep 1; fi
    echo "$ok $bad $last_ok" > "$f"
  done
}
for s in $(seq 1 $P); do bg_worker $s "$SP_DIR/b$s" & BG_PIDS+="$! "; done
sleep 10
LOGF=$(DEX bash -c "ls -t '${DDIR[$NEWL]}/pg.log' '${DDIR[$NEWL]}.log' 2>/dev/null | head -1" </dev/null); LOGL=$(DEX bash -c "wc -l < '$LOGF'" </dev/null)
for p in "${WORKERS[@]}"; do
  if [[ $p == $NEWL ]]; then Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 1500" >/dev/null
  elif [[ $p == $OLD ]]; then Q $p "ALTER SYSTEM SET pg_raft.heartbeat_ms = 10000" >/dev/null
  else Q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 60000" >/dev/null; fi
  Q $p "SELECT pg_reload_conf()" >/dev/null
done
st=""; for t in $(seq 1 180); do st=$(Q $NEWL "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=$FS"); [[ "$st" == leader ]] && break; sleep 1; done
T_EL=$(date +%s)
for p in "${WORKERS[@]}"; do Q $p "ALTER SYSTEM RESET pg_raft.heartbeat_ms" >/dev/null; Q $p "ALTER SYSTEM RESET pg_raft.election_timeout_ms" >/dev/null; Q $p "SELECT pg_reload_conf()" >/dev/null; done
check "  :$NEWL 在负载下当选组 $FS 新 leader" "$st" "leader"
reg=""; t_reg=""; maxs=0
for t in $(seq 1 $((REG_LIMIT_S/2))); do
  n=$(Q $NEWL "SELECT count(*) FROM pg_stat_activity WHERE state='active' AND query LIKE '%pg_raft_promote_prepare%' AND pid<>pg_backend_pid()")
  [[ "${n:-0}" =~ ^[0-9]+$ && $n -gt $maxs ]] && maxs=$n
  if [[ "$(Q $COORD "SELECT primary_node FROM partdist.partition_map WHERE partition_id=$FS")" == "${NID[$NEWL]}" &&
        "$(Q $COORD "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$FS")" == "$NEWL" ]]; then
    reg=ok; t_reg=$(( $(date +%s) - T_EL )); break
  fi
  sleep 2
done
echo "  当选→登记 ${t_reg:-未登记}s；并发升主前置峰值 $maxs"
check "★ 负载下新主 ${REG_LIMIT_S}s 内登记（partition_map + placement 都指向 :$NEWL）" "$reg" "ok"
check "★ 同组升主前置不堆积（峰值 $maxs ≤ 1）" "$([[ $maxs -le 1 ]] && echo ok)" "ok"
[[ -n "$reg" ]] && LEADER[$FS]=$NEWL
sleep 10
T_RESUME=$(date +%s)
sleep 30
touch "$STOP_FILE"; for x in $BG_PIDS; do wait "$x" 2>/dev/null; done; BG_PIDS=""
okb=0; badb=0; resumed=0
for s in $(seq 1 $P); do read o b l < "$SP_DIR/b$s"; okb=$((okb+o)); badb=$((badb+b)); [[ ${l:-0} -ge $T_RESUME ]] && resumed=$((resumed+1)); done
echo "  切主窗口内后台转账：成功 $okb、失败 $badb；登记后 30 s 窗口里有成功提交的会话 $resumed/$P"
[[ $badb -gt 0 ]] && cat "$SP_DIR"/b*.err 2>/dev/null | sed 's/[0-9]\{3,\}/N/g' | sort | uniq -c | sort -rn | head -6 | sed 's/^/    /'
check "★ 登记后负载恢复（全部会话都有新的成功提交）" "$resumed" "$P"
check "★ 判决收敛后切主前后总额守恒" "$(converge $COORD "SELECT coalesce(sum(n),0) FROM cg WHERE v='acct'" "$TOTAL_BEFORE" 120)" "$TOTAL_BEFORE"
CSUM=$(Q $COORD "WITH x AS MATERIALIZED (SELECT id, v, n FROM cg) SELECT count(*)||'/'||coalesce(sum(n),0)||'/'||coalesce(md5(string_agg(id||':'||v||':'||n, ',' ORDER BY id)),'') FROM x WHERE get_shard_id_for_distribution_column('cg', x.id) = $FS")
LSUM=$(converge $NEWL "$(LOCAL "SELECT count(*)||'/'||coalesce(sum(n),0)||'/'||coalesce(md5(string_agg(id||':'||v||':'||n, ',' ORDER BY id)),'') FROM cg_$FS")" "$CSUM" 60)
check "★ 新主 :$NEWL 本地内容（行数/余额和/指纹）= 协调者读到的该分片（P7-N5 守卫）" "$LSUM" "$CSUM"
NTO=$(DEX bash -c "tail -n +$((LOGL+1)) '$LOGF' | grep -c '升主前置超过 .* 未返回或连接失效'" </dev/null)
check "★ 新主日志无'升主前置…未返回或连接失效'（实得 $NTO）" "$NTO" "0"

echo "========== [9b] ★ P7-N10：全局事务里写集只有一个分片、却走 2PC（INSERT…SELECT）的提交必须可见 =========="
# 已知缺陷，修好之前这一条会红。放在最后、且只打在最后一个分片上，不干扰前面各段。
LS=${SIDS[$((NW-1))]}
nids=$(shard_ids $LS 1500001 3)
before_n10=$(Q $COORD "SELECT count(*) FROM cg WHERE v='n10'")
out=$(gtx $LS "INSERT INTO cg(id, v, n) SELECT x, 'n10', 0 FROM unnest(ARRAY[$nids]) x;")
check "  全局事务 INSERT…SELECT 报告提交成功" "$out" "txn_done"
check "★ P7-N10：提交成功的 3 行在判决收敛后可见（已知缺陷：分片 clog 停在 RUNNING）" "$(converge $COORD "SELECT count(*) - $before_n10 FROM cg WHERE v='n10'" 3 30)" "3"

if [[ "$FAIL" -gt 0 ]]; then
  echo "  ---- [取证] 各节点逐分片 行数 / 账户数 / 余额和（本地读） ----"
  for sid in "${SIDS[@]}"; do for p in "${WORKERS[@]}"; do
    echo "    组 $sid :$p $(Q $p "SET citus.override_table_visibility=false; SET pg_partdist.allow_replica_access=on; SELECT count(*)||' 行 / acct '||count(*) FILTER (WHERE v='acct')||' / sum='||coalesce(sum(n),0) FROM cg_$sid")  $(Q $p "SELECT partdist.shard_mvcc_status(partdist.local_partition_for_shard($sid))")"
  done; done
  echo "    协调者 重复 id：$(Q $COORD "SELECT coalesce(string_agg(id||'x'||c, ','),'无') FROM (SELECT id, count(*) c FROM cg GROUP BY id HAVING count(*)>1) t")；账户行 $(Q $COORD "SELECT count(*) FROM cg WHERE v='acct'")（期望 $((ACCT*NW))）"
  for p in $COORD "${WORKERS[@]}"; do echo "    :$p prepared=$(Q $p "SELECT coalesce(string_agg(gid, ' '),'无') FROM pg_prepared_xacts")"; done
fi

echo "========== [10] 健康与环计数 =========="
health_check_no_crash
drops=0; ow=0
for p in "${WORKERS[@]}"; do
  d=$(Q $p "SELECT coalesce(sum(ring_full_drops + quorum_drops),0) FROM partdist.pg_raft_group_flow_stats() WHERE group_id<>0"); drops=$((drops + ${d:-0}))
  o=$(Q $p "SELECT overwrites FROM partdist.partwal_ring_stats()"); ow=$((ow + ${o:-0}))
done
echo "  日志环丢弃合计=$drops（切主窗口里旧主失去多数派时可能有少量 quorum_drops，只报不判）"
check "捕获环覆盖合计" "$ow" "0"
rm -rf "$SP_DIR"

echo ""
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]] && echo "跨组分布式事务验收：全部通过" || echo "跨组分布式事务验收：存在 FAIL"
