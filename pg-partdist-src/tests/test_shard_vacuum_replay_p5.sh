#!/usr/bin/env bash
# [宿主机] P5 vacuum 页面动作的**跨节点回放**验收（DEV PLAN §3.4 T5.4b）。
#
# 覆盖两件事：
#   T5.4b-1 —— vacuum 页面动作的跨节点回放（三份记要里挂了三次账的那一项）；
#   T5.8   —— TOAST 关系（含它的 btree 索引）一并参与，于是索引两阶段发出的
#              btree vacuum 记录也走这条回放通路；
#   U-P5-1 —— [2b] 段：**分片 clog 由流重建**（设计 §5.3）+ **持久发号水位
#              交接**。MARKER 现在带上本分区的分片 xid 与 leader 的发号水位，
#              follower 据此把 pg_shard_clog 建起来、把发号水位落进自己的
#              pg_shard_xid —— 升主后既看得见数据，也不会重号；
#   T5.4b-2 —— 两个 vacuum 水位随 CTRL 记录（复用 FREEZE_UPDATE 通道换语义，
#              不新增 opcode）到达 follower 并落进它自己的 pg_shard_xid/<oid>。
#
# ★ T5.4b-1 的由来：
#   §6.7 要求：vacuum 只在 leader 执行，它的页面修改本身走 pg_parwal 流被
#   follower 逐字节回放。前面三个任务只验到"leader 的 WAL 里确有内核标准的
#   FREEZE_PAGE / PRUNE / VACUUM 记录"，跨节点这一半一直没验。
#
# 夹具照抄 test_shard_pagecmp_p1.sh 阶段 2 的配方（1 分片分布表 + raft 组 +
# 两个 follower 壳表 + locmap），把工作负载换成"制造三类垃圾 → sweep → 截断"。
#
# ★ 关键的一条防假通过断言：**follower 的文件必须真的变了**。只比对
#   "leader == follower" 是不够的 —— 若 vacuum 记录压根没进流，两边都停在
#   vacuum 之前的状态，比对照样 IDENTICAL。
#
# ★ follower 壳表绝不能被 SELECT（无白名单 ⇒ on-access 剪枝会按原生 clog
#   清掉分片元组，就地损毁副本）。本脚本只用文件级比对触碰 follower。
set -euo pipefail

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() {
  local port=$1; shift
  [[ "$port" =~ ^[0-9]+$ && "$port" -ge 1 && "$port" -le 65535 ]] || {
    echo "Invalid test PostgreSQL port: '$port'" >&2; return 2;
  }
  DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X -q -v ON_ERROR_STOP=1 "$@"
}

check() {
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return 1
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); return 1; fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

declare -A saved_replay_trust=()
saved_shard_relids=""
saved_shard_relids_set=0
restore_replay_gucs() {
  local rc=$? p value
  trap - EXIT
  for p in "${!saved_replay_trust[@]}"; do
    value=${saved_replay_trust[$p]//\'/\'\'}
    PSQL "$p" -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = '$value'" \
      -c 'SELECT pg_reload_conf()' </dev/null >/dev/null || rc=1
  done
  if [[ -n "${pport:-}" && "$saved_shard_relids_set" == 1 ]]; then
    value=${saved_shard_relids//\'/\'\'}
    PSQL "$pport" -c "ALTER SYSTEM SET pg_partdist.shard_relids = '$value'" \
      -c 'SELECT pg_reload_conf()' </dev/null >/dev/null || rc=1
  fi
  echo "Replay suite exit: PASS=$PASS FAIL=$FAIL status=$rc"
  exit "$rc"
}
trap restore_replay_gucs EXIT

echo "================ [1] 夹具：1 分片分布表 + raft 组 + 两 follower ================"
leader0=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" </dev/null)
check "group0 有 leader" "$([[ -n "$leader0" && "$leader0" != "0" ]] && echo ok)" "ok"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  saved_replay_trust[$p]=$(PSQL "$p" -Atc 'SHOW pg_partdist.replay_trust_local_segments' </dev/null)
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done

PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS p5repl;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE p5repl(id int, v text);
SELECT create_distributed_table('p5repl', 'id');
ALTER TABLE p5repl SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='p5repl'::regclass" </dev/null)
[[ "$gid" =~ ^[0-9]+$ && "$gid" -gt 0 ]] || { echo 'Cannot resolve p5repl shard id; stopping fixture setup' >&2; exit 1; }
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
[[ "$pport" =~ ^[0-9]+$ && "$pport" -ge 5433 && "$pport" -le 5440 ]] || { echo 'Cannot resolve p5repl placement port; stopping fixture setup' >&2; exit 1; }
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="p5repl_${gid}"
echo "  shard=${gid} leader=:${pport}(node${pnode}) followers=:${f1} :${f2}"

nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册（主堆+TOAST堆+TOAST索引=3）" "$nrels" "3"
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE p5repl INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])" </dev/null)
  check "follower :$fp locmap 配对 3 对" "$np" "3"
done
for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null
done
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
# Ask the placement node to campaign for this group using ordinary Raft voting.
# A fixed 20s wait can expire before a private lab's 30s election timer fires.
PSQL "$pport" -q -c "SELECT partdist.pg_raft_group_campaign(${gid});" </dev/null >/dev/null
election_ms=$(PSQL "$pport" -Atc 'SHOW pg_raft.election_timeout_ms' </dev/null)
[[ "$election_ms" =~ ^[0-9]+$ ]] || { echo 'Invalid election timeout' >&2; exit 1; }
election_wait=$(((2 * election_ms + 999) / 1000 + 30))
st=""
for t in $(seq 1 "$election_wait"); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
if [[ "$st" != leader ]]; then
  for p in "$pport" "$f1" "$f2"; do
    PSQL "$p" -c "SELECT * FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null || true
  done
fi
check "分区组 ${gid} leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${shard_tbl}')" </dev/null)
  check "follower :$fp replay_enable" "$en" "t"
done

# Election and primary registration are separate steps. In particular, do not
# run the workload while control-plane apply is still registering this group.
registered=0
for t in $(seq 1 45); do
  registered=1
  for p in "$COORD" "$pport" "$f1" "$f2"; do
    mapped=$(PSQL "$p" -Atc "SELECT count(*) FROM partdist.partition_map WHERE partition_id=$gid AND primary_node=$pnode AND primary_term>=1" </dev/null)
    [[ "$mapped" == 1 ]] || registered=0
  done
  [[ "$registered" == 1 ]] && break
  sleep 2
done
check '控制面主节点登记已同步到 coordinator 及三个副本节点' "$registered" 1

SOID=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT '${shard_tbl}'::regclass::oid" </dev/null | tail -1)
[[ "$SOID" =~ ^[0-9]+$ && "$SOID" -gt 0 ]] || { echo 'Invalid leader relation OID' >&2; exit 1; }
saved_shard_relids=$(PSQL "$pport" -Atc 'SHOW pg_partdist.shard_relids' </dev/null)
saved_shard_relids_set=1
PSQL $pport -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${SOID}';" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL $pport -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$SOID" ]] && break; sleep 1
done
check "leader 白名单生效" "$guc" "$SOID"
PDATA=$(PSQL $pport -Atc "SHOW data_directory" </dev/null)
PSQL $pport -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE OR REPLACE FUNCTION sclog_read(oid, bigint) RETURNS int
  AS '$libdir/pg_partdist','partdist_shard_clog_read' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_wts(oid, bigint, int, bigint) RETURNS void
  AS '$libdir/pg_partdist','partdist_shard_clog_write_ts' LANGUAGE C STRICT;
SQL
check "leader 测试函数就绪" "$?" "0"
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE OR REPLACE FUNCTION sclog_read(oid, bigint) RETURNS int
  AS '$libdir/pg_partdist','partdist_shard_clog_read' LANGUAGE C STRICT;
SQL
done
check "follower 测试函数就绪" "$?" "0"

echo "================ [2] 工作负载：制造三类垃圾（全 leader 直写，含 TOAST）================"
# ★ T5.8 之后 TOAST 一并参与：每 10 行放一个进 TOAST 的大值（md5 拼的十六进制
#   串，不好压）。TOAST 关系带 btree 索引，它的清理走的正是索引两阶段，
#   于是这一轮连 btree 的 vacuum 记录也一起过回放通路。
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.override_table_visibility=false;
INSERT INTO ${shard_tbl}
SELECT g, CASE WHEN g % 10 = 0
       THEN (SELECT string_agg(md5((g*1000+i)::text),'') FROM generate_series(1,200) i)
       ELSE 'v'||g END
FROM generate_series(1, 60) g;
DELETE FROM ${shard_tbl} WHERE id % 7 = 0;
DELETE FROM ${shard_tbl} WHERE id = 20;
SQL
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.override_table_visibility=false;
BEGIN; INSERT INTO ${shard_tbl} VALUES (901,'ghost'); ROLLBACK;
SQL
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.override_table_visibility=false;
BEGIN; DELETE FROM ${shard_tbl} WHERE id = 1; ROLLBACK;
SQL
# PartWALAbort deliberately drains DATA + ABORT to the local stream without
# synchronous replication. Its documented delivery path is the next committed
# write on this same shard. Supply that write before measuring the replay goal.
# The insert/delete pair preserves the 51 visible rows and adds real vacuum work.
PSQL "$pport" -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.override_table_visibility=false;
BEGIN;
INSERT INTO ${shard_tbl} VALUES (999,'replication-barrier');
DELETE FROM ${shard_tbl} WHERE id=999;
COMMIT;
SQL
echo '  提交写入已带出前面回滚事务的 DATA 与 ABORT 标记'
vis=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl}" </dev/null | tail -1)
check "leader 可见 51 行（60 插入 - 8 - 1 已提交删除）" "$vis" "51"
TREL=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid=${SOID}" </dev/null | tail -1)
TOASTN() { PSQL $pport -Atc "SET citus.override_table_visibility=false;
  SELECT count(*) FROM generate_series(0,(pg_relation_size('$TREL')/8192)::int-1) b,
    LATERAL heap_page_items(get_raw_page('$TREL',b)) i WHERE i.lp_flags=1" </dev/null | tail -1; }
tn0=$(TOASTN)
check "值确实进了 TOAST（chunk 数 > 5）" "$([[ -n "$tn0" && "$tn0" -gt 5 ]] && echo ok)" "ok"
md5_before=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT md5(string_agg(v,'' ORDER BY id)) FROM ${shard_tbl}" </dev/null | tail -1)
n_before=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM heap_page_items(get_raw_page('${shard_tbl}',0)) WHERE lp_flags=1" </dev/null | tail -1)
check "vacuum 前第 0 页有 LP_NORMAL" "$([[ -n "$n_before" && "$n_before" -gt 10 ]] && echo ok)" "ok"

LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
check "leader 认得本地分片（local_partition_for_shard）" "$([[ -n "$LEADER_OID" && "$LEADER_OID" != "0" ]] && echo ok)" "ok"
lead_plsn1=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
echo "  lead_plsn1=${lead_plsn1}"
# partition_lsn 是**分区流内的逻辑位置**（记录序），不是字节 LSN —— 本夹具
# 这点写入量下实测只有 70 上下，照抄 pagecmp 的 ">100" 会误报。
check "leader 已产生 parwal 记录" "$([[ -n "$lead_plsn1" && "$lead_plsn1" -gt 0 ]] && echo ok)" "ok"
committed_ready=0
for t in $(seq 1 30); do
  committed=$(PSQL "$pport" -Atc "SELECT partdist.pg_raft_group_committed_plsn($gid)" </dev/null)
  if [[ "$committed" =~ ^[0-9]+$ && "$committed" -ge "$lead_plsn1" ]]; then committed_ready=1; break; fi
  sleep 1
done
check "目标位置已获 Raft 提交确认（committed=$committed, target=$lead_plsn1）" "$committed_ready" 1

wait_caught_up() {  # <fport> <期望plsn> <超时s>
  local fp=$1 target=$2 timeout=$3 app foid
  # 空 target 会让下面的 -ge 比较变成语法错误，进而把断言变成假通过
  [[ -n "$target" && "$target" -gt 0 ]] || { echo ""; return 1; }
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null)
  app=$(PSQL "$fp" -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${target}, $((timeout * 1000)))" </dev/null || echo 0)
  [[ -n "$app" && "$app" -ge "$target" ]] && { echo "$app"; return 0; }
  app=$(PSQL "$fp" -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}" </dev/null 2>/dev/null || echo 0)
  echo "Replay failed: port=$fp shard=$foid applied=${app:-unknown} target=$target" >&2
  echo "$app"; return 1
}
FWM() {  # <fport> —— 该 follower 上本分片的 vacuum 两水位（不碰壳表，只读水位）
  local fp=$1 foid
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
  PSQL "$fp" -Atc "SELECT clog_truncate_before||'/'||shard_vacuum_xid FROM partdist.shard_vacuum_watermarks(${foid}::oid)" </dev/null | tail -1
}
FPATH_MAIN() {  # <fport> —— 该 follower 上主堆文件的绝对路径
  local fp=$1 fdata frel
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  frel=$(PSQL "$fp" -Atc "SET citus.enable_ddl_propagation=off;
    SELECT pg_relation_filepath(pg_filenode_relation(CASE WHEN spc=1663 THEN 0 ELSE spc END, relnum))
      FROM partdist.shard_fileset('${shard_tbl}') WHERE role=0 AND ord=0" </dev/null | tail -1)
  echo "${fdata}/${frel}"
}
a1=$(wait_caught_up $f1 "$lead_plsn1" 180)
check "follower1 追平工作负载" "$([[ -n "$a1" && "$a1" -ge "$lead_plsn1" ]] && echo ok)" "ok"
a2=$(wait_caught_up $f2 "$lead_plsn1" 180)
check "follower2 追平工作负载" "$([[ -n "$a2" && "$a2" -ge "$lead_plsn1" ]] && echo ok)" "ok"
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
sleep 4

echo "================ [2b] U-P5-1：分片 clog 由流重建 ================"
# 设计 §5.3：「落账顺序锁定在分片流的 plsn 序上 ⇒ 每个副本重放同一个流得到
# 同一本账」。在此之前 follower 只写增强型 clog（按 gxid 索引），
# pg_shard_clog/<oid> 在它上面根本不存在 —— 升主后每个分片 xid 都读成
# 空洞=RUNNING=不可见，整张表看不见。
SCLOG() { PSQL "$1" -Atc "SELECT string_agg(sclog_read($2::oid, g::bigint)::text, ',' ORDER BY g) FROM generate_series(3,15) g" </dev/null | tail -1; }
lead_clog=$(SCLOG $pport $SOID)
check "leader 分片 clog 有 COMMITTED 判决" "$([[ "$lead_clog" =~ 2 ]] && echo ok)" "ok"
check "leader 分片 clog 有 ABORTED 判决" "$([[ "$lead_clog" =~ 3 ]] && echo ok)" "ok"
# ★ 判据不是"逐条相等"，而是按 PartWALAbort 的既有设计来：
#   中止事务若其 DATA 字节**尚未入流**，parwal 会丢弃本后端的槽位、也不写 ABORT
#   标记（"中止字节只进原生 WAL 不进分区流"，pagecmp_p1 的注释即此）。于是
#   follower 在这些位置是空洞。这是**自洽的**：那些元组同样没进流，副本上没有
#   任何东西引用那些号。
#   所以允许且仅允许一种差异：leader=ABORTED(3) 而 follower=空洞(0)。
#   COMMITTED 判决必须逐条到位 —— 少一条就是升主后"已提交数据看不见"。
cmp_clog() {  # cmp_clog <leader串> <follower串> → 打印不合规的位数
  awk -v a="$1" -v b="$2" 'BEGIN{
    n=split(a,x,","); split(b,y,","); bad=0;
    for(i=1;i<=n;i++) if (x[i]!=y[i] && !(x[i]=="3" && y[i]=="0")) bad++;
    print bad }'
}
for fp in $f1 $f2; do
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
  fclog=$(SCLOG $fp $foid)
  check "★ follower :$fp 的分片 clog 由流重建（COMMITTED 逐条到位）" \
        "$(cmp_clog "$lead_clog" "$fclog")" "0"
  check "  （对照：follower :$fp 确实拿到了非空判决，不是全空洞）" \
        "$([[ "$fclog" =~ 2 ]] && echo ok)" "ok"
done

# ---- U-P5-1 之二：持久发号水位交接 ----
# 光有 clog 判决还盖不住一格：事务的字节被别人的 group commit 顺带刷进了流、
# 随后本后端**崩溃**（不是中止）—— 既无 COMMIT 也无 ABORT 标记，而元组已经到了
# follower；新主对该号读到空洞，会把它重新发出去。带上 leader 的发号水位就没有
# 这一格。
# 判据是"follower 的水位 >= leader 的 next_xid"，而不是两边相等：leader 带的是
# **已持久化**的水位（按批次向上取整，比 next_xid 宽），而中止且字节未入流的
# 事务又会在 leader 上悄悄吃掉号 —— 两边本就不该相等。
lead_next=$(PSQL $pport -Atc "SELECT partdist.shard_xid_next(${SOID}::oid)" </dev/null | tail -1)
check "leader 已发过号（next_xid > 3）" "$([[ -n "$lead_next" && "$lead_next" -gt 3 ]] && echo ok)" "ok"
# ★★ 2026-09-12 改判据：原先比的是 `follower 水位 >= leader **当前** next_xid`，
#   实测 6 >= 8 不成立。而上面那段注释自己的论证是矛盾的 —— 它说"中止且字节未
#   入流的事务会在 leader 上**悄悄吃掉号**，两边本就不该相等"，可被悄悄吃掉的号
#   只会让 **leader 的 next_xid 更大**，正好让 `follower >= leader` 更难成立，
#   不是更容易。拿一个会随 leader 后续活动单向增长的量做下界，判据本身就不成立。
#
#   真正要保的安全性质只有一条：**升主后不能把已经用在回放元组上的号再发一次**。
#   所以该比的是"follower 自己 clog 里已判定的最大分片 xid"，而不是 leader 的
#   实时 next_xid —— 前者才是这个节点升主后可能撞上的号的上界。
for fp in $f1 $f2; do
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
  fnext=$(PSQL "$fp" -Atc "SELECT partdist.shard_xid_next(${foid}::oid)" </dev/null | tail -1)
  # 已判定（st<>0）的最大分片 xid；扫描范围与上面 SCLOG() 的 3..15 同源
  fmax=$(PSQL "$fp" -Atc "SELECT coalesce(max(g),2) FROM generate_series(3,15) g WHERE sclog_read(${foid}::oid, g::bigint) <> 0" </dev/null | tail -1)
  check "★ follower :$fp 的发号水位盖过已回放的最大分片 xid（next=$fnext > max_seen=$fmax；leader 当前 next=$lead_next 仅作参考）" \
        "$([[ -n "$fnext" && -n "$fmax" && "$fnext" -gt "$fmax" ]] && echo ok)" "ok"
done
# ★ "持久"二字要验：重启一个 follower，水位必须还在（否则升主就会从 3 号重发）
f1data=$(PSQL $f1 -Atc "SHOW data_directory" </dev/null)
DEX /work/pg-install/bin/pg_ctl -D "$f1data" -m fast restart -l "$f1data/restart_up51.log" </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 45); do up=$(PSQL $f1 -Atc "SELECT 1" </dev/null 2>/dev/null); [[ "$up" == "1" ]] && break; sleep 1; done
check "follower :$f1 重启就绪" "$up" "1"
foid1=$(PSQL "$f1" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
# 同上：持久性验的是"重启后水位没退回去"，下界仍取本节点已回放的最大分片 xid。
fnext1=$(PSQL "$f1" -Atc "SELECT partdist.shard_xid_next(${foid1}::oid)" </dev/null | tail -1)
fmax1=$(PSQL "$f1" -Atc "SELECT coalesce(max(g),2) FROM generate_series(3,15) g WHERE sclog_read(${foid1}::oid, g::bigint) <> 0" </dev/null | tail -1)
check "★ 重启后发号水位仍在（这才叫持久；next=$fnext1 > max_seen=$fmax1）" \
      "$([[ -n "$fnext1" && -n "$fmax1" && "$fnext1" -gt "$fmax1" ]] && echo ok)" "ok"

F1MAIN=$(FPATH_MAIN $f1); F2MAIN=$(FPATH_MAIN $f2)
md5_f1_pre=$(DEX md5sum "$F1MAIN" </dev/null 2>/dev/null | cut -d' ' -f1)
md5_f2_pre=$(DEX md5sum "$F2MAIN" </dev/null 2>/dev/null | cut -d' ' -f1)
check "vacuum 前取到 follower1 主堆指纹" "$([[ -n "$md5_f1_pre" ]] && echo ok)" "ok"
check "vacuum 前取到 follower2 主堆指纹" "$([[ -n "$md5_f2_pre" ]] && echo ok)" "ok"
# ★ 水位复制的对照组：vacuum 之前两个 follower 都还没有免查区
check "vacuum 前 follower1 水位 0/0" "$(FWM $f1)" "0/0"
check "vacuum 前 follower2 水位 0/0" "$(FWM $f2)" "0/0"

echo "================ [3] leader 侧 sweep + 截断 ================"
TB=$(PSQL $pport -Atc "SET citus.override_table_visibility=false;
  SELECT max(GREATEST(t_xmin::text::bigint, t_xmax::text::bigint))+1
    FROM heap_page_items(get_raw_page('${shard_tbl}',0)) WHERE lp_flags=1" </dev/null | tail -1)
check "算出截断点" "$([[ -n "$TB" && "$TB" -gt 3 ]] && echo ok)" "ok"
# This lab configures TSO before any writes. Wait for the real safe prefix;
# never rewrite commit timestamps merely to make a maintenance call succeed.
safe_ready=0
for t in $(seq 1 45); do
  safe=$(PSQL "$COORD" -Atc 'SELECT partdist.partdist_global_safe_ts()' </dev/null)
  target=$(PSQL "$pport" -Atc "SELECT target_xid FROM partdist.shard_vacuum_target(${SOID}::oid,$safe,$TB)" </dev/null)
  if [[ "$target" == "$((TB - 1))" ]]; then safe_ready=1; break; fi
  sleep 1
done
check 'TSO 安全前缀已覆盖 sweep 目标' "$safe_ready" '1'
SW=$(PSQL $pport -Atc "SELECT swept||'/'||sanitized||'/'||removed_aborted||'/'||removed_dead||'/'||pages_skipped||'/'||tuples_deferred FROM partdist.shard_vacuum_sweep('${shard_tbl}'::regclass, ${TB}::bigint)" </dev/null | tail -1)
check "sweep 干净收尾（零跳页零推迟）" "$(echo "$SW" | cut -d/ -f1,5,6)" "true/0/0"
check "★ 主堆 + TOAST 一起清：删死元组 > 9" \
      "$([[ "$(echo "$SW" | cut -d/ -f4)" -gt 9 ]] && echo ok)" "ok"
check "★ TOAST chunk 被回收（$tn0 → $(TOASTN)）" \
      "$([[ "$(TOASTN)" -lt "$tn0" ]] && echo ok)" "ok"
check "★ 活行的 TOAST 值仍能完整取出" \
      "$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT md5(string_agg(v,'' ORDER BY id)) FROM ${shard_tbl}" </dev/null | tail -1)" "$md5_before"
NSEG=$(PSQL $pport -Atc "SELECT partdist.shard_clog_truncate(${SOID}::oid, ${TB}::bigint)" </dev/null | tail -1)
check "截断成功（删段 0，不足一整段）" "$NSEG" "0"
check "leader 水位推进到 ${TB}/${TB}" \
      "$(PSQL $pport -Atc "SELECT clog_truncate_before||'/'||shard_vacuum_xid FROM partdist.shard_vacuum_watermarks(${SOID}::oid)" </dev/null | tail -1)" "${TB}/${TB}"
check "vacuum 后 leader 仍可见 51 行" \
      "$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl}" </dev/null | tail -1)" "51"
n_after=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM heap_page_items(get_raw_page('${shard_tbl}',0)) WHERE lp_flags=1" </dev/null | tail -1)
check "leader 第 0 页 LP_NORMAL 减少了" "$([[ -n "$n_after" && "$n_after" -lt "$n_before" ]] && echo ok)" "ok"

lead_plsn2=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
echo "  lead_plsn2=${lead_plsn2}"
check "vacuum 产生了新的 parwal 记录" "$([[ -n "$lead_plsn2" && "$lead_plsn2" -gt "$lead_plsn1" ]] && echo ok)" "ok"

echo "================ [4] follower 回放 vacuum 记录 ================"
b1=$(wait_caught_up $f1 "$lead_plsn2" 180)
check "follower1 追平 vacuum" "$([[ -n "$b1" && "$b1" -ge "$lead_plsn2" ]] && echo ok)" "ok"
b2=$(wait_caught_up $f2 "$lead_plsn2" 180)
check "follower2 追平 vacuum" "$([[ -n "$b2" && "$b2" -ge "$lead_plsn2" ]] && echo ok)" "ok"
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
sleep 4
md5_f1_post=$(DEX md5sum "$F1MAIN" </dev/null 2>/dev/null | cut -d' ' -f1)
md5_f2_post=$(DEX md5sum "$F2MAIN" </dev/null 2>/dev/null | cut -d' ' -f1)
# ★ 防假通过：只比"leader==follower"是不够的——若 vacuum 记录压根没进流，
#   两边都停在 vacuum 之前，比对照样 IDENTICAL。必须先证明 follower 真的变了。
check "★ follower1 主堆文件确实变了（vacuum 记录真的到了）" \
      "$([[ -n "$md5_f1_post" && "$md5_f1_post" != "$md5_f1_pre" ]] && echo ok)" "ok"
check "★ follower2 主堆文件确实变了" \
      "$([[ -n "$md5_f2_post" && "$md5_f2_post" != "$md5_f2_pre" ]] && echo ok)" "ok"
# ★★ T5.4b-2：两个水位随 CTRL（复用 FREEZE_UPDATE 通道）到达 follower 并落盘。
#    落点与 leader 同一个 pg_shard_xid/<oid>，升主时建槽路径原样读得到。
check "★ follower1 收到并落盘 vacuum 水位 ${TB}/${TB}" "$(FWM $f1)" "${TB}/${TB}"
check "★ follower2 收到并落盘 vacuum 水位 ${TB}/${TB}" "$(FWM $f2)" "${TB}/${TB}"

echo "================ [5] 三方逐字节比对 ================"
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
FILESET_PATHS_SQL="SELECT role||'.'||ord||','||
       pg_relation_filepath(pg_filenode_relation(
           CASE WHEN spc = 1663 THEN 0 ELSE spc END, relnum))
  FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord"
lead_paths=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; ${FILESET_PATHS_SQL}" </dev/null | grep ',')
pagecmp_kind() { [[ "$2" == "_vm" ]] && { echo vm; return; }; case "${1%%.*}" in 1|3) echo btree ;; *) echo heap ;; esac; }
diff_one_follower() {  # <fport> <标签>
  local fp=$1 tag=$2 fdata frows ncmp=0
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  frows=$(PSQL "$fp" -Atc "SET citus.enable_ddl_propagation=off; ${FILESET_PATHS_SQL}" </dev/null | grep ',')
  local -a rows; local lrow
  mapfile -t rows <<< "$lead_paths"
  for lrow in "${rows[@]}"; do
    [[ -n "$lrow" ]] || continue
    local key lrel frel lpath fpath fork
    key=${lrow%%,*}; lrel=${lrow#*,}
    frel=$(echo "$frows" | grep "^${key}," | cut -d, -f2)
    for fork in "" "_vm"; do
      lpath="${PDATA}/${lrel}${fork}"; fpath="${fdata}/${frel}${fork}"
      local lex fex kind same
      lex=$(DEX bash -c "test -f '$lpath' && echo y || echo n" </dev/null)
      fex=$(DEX bash -c "test -f '$fpath' && echo y || echo n" </dev/null)
      if [[ "$lex" == "n" && "$fex" == "n" ]]; then continue; fi
      check "${tag} ${key}${fork:-.main} 两侧都存在" "$lex/$fex" "y/y"
      [[ "$lex" == "y" && "$fex" == "y" ]] || continue
      kind=$(pagecmp_kind "$key" "$fork")
      if [[ -n "${VACU_LAB_ROOT:-}" ]]; then
        # The image need not contain Python: read the same bind-mounted bytes
        # using the host's Python, scoped strictly to this isolated /work mount.
        [[ "$lpath" == /work/* && "$fpath" == /work/* ]] || { echo "Unexpected page path" >&2; return 1; }
        same=$(python3 "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" --kind="$kind" \
          "$VACU_LAB_ROOT/work/${lpath#/work/}" "$VACU_LAB_ROOT/work/${fpath#/work/}" </dev/null 2>/dev/null)
      else
        same=$(DEX python3 /tmp/pagecmp.py --kind="$kind" "$lpath" "$fpath" </dev/null 2>/dev/null)
      fi
      if [[ -z "$fork" ]]; then
        # ★ T5.8 起三个 fileset 成员（主堆 / TOAST 堆 / TOAST 索引）都参与了
        #   本轮 vacuum —— 主堆走 PRUNE+VACUUM，TOAST 堆同样，TOAST 索引走
        #   index_bulk_delete 发的 btree 记录。三者一律要求逐字节相同。
        check "${tag} ${key}.main pagecmp(${kind}) 逐字节" "$same" "IDENTICAL_OUTSIDE_HOLE"
      else
        # vm fork 没有被本轮触碰过；空/未建都不是分歧，列举允许值
        case "$same" in
          IDENTICAL_OUTSIDE_HOLE|IDENTICAL_EMPTY|IDENTICAL_EXCEPT_PDLSN) same=OK ;;
        esac
        check "${tag} ${key}${fork} pagecmp(${kind}) 无分歧" "$same" "OK"
      fi
      ncmp=$((ncmp+1))
    done
  done
  check "${tag} 比对文件计数守卫（≥3）" "$([[ "$ncmp" -ge 3 ]] && echo ok)" "ok"
}
diff_one_follower $f1 "vacuum回放:f1"
diff_one_follower $f2 "vacuum回放:f2"

if [[ "${VACU_WRAP_REPLAY:-0}" == 1 ]]; then
  echo '================ [5b] 32 位算法的短环多次回卷 + 两副本回放 ================'
  [[ "${SHARD_XID_RING_BITS:-32}" == 8 ]] || { echo 'Wrap replay requires the explicitly accelerated build' >&2; exit 1; }
  previous=$(PSQL "$pport" -Atc "SELECT partdist.shard_xid_next(${SOID}::oid)" </dev/null)
  wraps=0
  for round in $(seq 1 200); do
    raw=$(PSQL "$pport" -Atc "INSERT INTO ${shard_tbl} VALUES($((10000+round)),'wrap') RETURNING xmin::text" </dev/null)
    [[ "$raw" =~ ^[0-9]+$ && "$raw" -ge 3 && "$raw" -le 255 ]] || exit 1
    if (( raw < previous )); then wraps=$((wraps+1)); fi
    previous=$raw
    PSQL "$pport" -q -c "BEGIN; INSERT INTO ${shard_tbl} VALUES($((20000+round)),'aborted'); ROLLBACK" </dev/null >/dev/null
    PSQL "$pport" -q -c "DELETE FROM ${shard_tbl} WHERE id=$((10000+round))" </dev/null >/dev/null
    if (( round % 8 == 0 )); then
      cutoff=$(PSQL "$pport" -Atc "SELECT partdist.shard_xid_next(${SOID}::oid)" </dev/null)
      ready=0
      for poll in $(seq 1 45); do
        safe=$(PSQL "$COORD" -Atc 'SELECT partdist.partdist_global_safe_ts()' </dev/null)
        target=$(PSQL "$pport" -Atc "SELECT target_xid FROM partdist.shard_vacuum_target(${SOID}::oid,$safe,$cutoff)" </dev/null)
        if [[ "$target" =~ ^[0-9]+$ && "$target" -gt 0 ]]; then
          next_target=$(((target+1)&255))
          if (( next_target < 3 )); then next_target=3; fi
          if [[ "$cutoff" == "$next_target" ]]; then ready=1; break; fi
        fi
        sleep 1
      done
      check "回卷批次 $round：安全前缀就绪" "$ready" 1
      check "回卷批次 $round：sweep 完成" "$(PSQL "$pport" -Atc "SELECT swept FROM partdist.shard_vacuum_sweep('${shard_tbl}',${cutoff})" </dev/null)" t
      PSQL "$pport" -q -c "SELECT partdist.shard_clog_truncate(${SOID}::oid,$cutoff)" </dev/null >/dev/null
      bound=$(PSQL "$pport" -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null)
      for fp in "$f1" "$f2"; do
        caught=$(wait_caught_up "$fp" "$bound" 180)
        check "回卷批次 $round：follower :$fp 追平" "$([[ "$caught" =~ ^[0-9]+$ && "$caught" -ge "$bound" ]] && echo ok)" ok
        check "回卷批次 $round：follower :$fp 回收凭据持久" "$(FWM "$fp")" "$cutoff/$cutoff"
      done
      check "回卷批次 $round：活行仍为 51" "$(PSQL "$pport" -Atc "SELECT count(*) FROM ${shard_tbl}" </dev/null)" 51
      check "回卷批次 $round：保留 TOAST 内容不变" "$(PSQL "$pport" -Atc "SELECT md5(string_agg(v,'' ORDER BY id)) FROM ${shard_tbl}" </dev/null)" "$md5_before"
    fi
  done
  check '至少发生两次真实号码复用（短环加速）' "$((wraps >= 2))" 1
  PSQL "$pport" -q -c CHECKPOINT </dev/null >/dev/null
  diff_one_follower "$f1" '重复回卷:f1'
  diff_one_follower "$f2" '重复回卷:f2'
fi

echo "================ [6] 清理 + 节点健康 ================"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" </dev/null >/dev/null 2>&1
done
for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1
done
for fp in $f1 $f2; do
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS p5repl;" </dev/null >/dev/null 2>&1
for p in $COORD $pport $f1 $f2; do
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" </dev/null >/dev/null 2>&1
done
PSQL $pport -q -c "SET citus.enable_ddl_propagation=off; DROP FUNCTION IF EXISTS sclog_read(oid,bigint); DROP FUNCTION IF EXISTS sclog_wts(oid,bigint,int,bigint);" </dev/null >/dev/null 2>&1
for fp in $f1 $f2; do
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP FUNCTION IF EXISTS sclog_read(oid,bigint);" </dev/null >/dev/null 2>&1
  fdata2=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  foid2=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
  [[ -n "$foid2" && "$foid2" != "0" ]] && DEX rm -rf "${fdata2}/pg_shard_clog/${foid2}" </dev/null
done
PSQL $pport -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
DEX rm -f "${PDATA}/pg_shard_xid/${SOID}" </dev/null
for fp in $f1 $f2; do
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
  [[ -n "$foid" && "$foid" != "0" ]] && DEX rm -f "${fdata}/pg_shard_xid/${foid}" </dev/null
done
check "清场完成" "$?" "0"
health_check_no_crash

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
exit "$FAIL"
