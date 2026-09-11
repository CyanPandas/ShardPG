#!/usr/bin/env bash
# [宿主机] T7.2 + T7.8 验收：物理基线搬分片 clog（R-P6-17）+ leader DROP 的副本侧回收（P7-D1）。
#
# 缺陷：`shard_baseline_emit` 只灌页面 + 抬发号水位，不搬 `pg_shard_clog/<oid>`；
# 而基线游标之前的 MARKER 不再回放 ⇒ 在**已有数据之后**才供给的副本，对基线
# 之前提交的每个分片 xid 都**没有判决** ⇒ 那些行读成 RUNNING（不可见）；
# 该副本一旦升主，`shard_claim_on_promote` 还会把它们改判 ABORTED ⇒ **丢行**。
# 此前只能靠"先供给、后写数据"的运维纪律绕过去，而那条纪律没人强制。
#
# 判据（**先写数据、后供给**，正是缺陷的触发姿势）：
#   [3] 先在 leader 上写够行并确认判决落进 leader 的分片 clog（st=2）
#   [4] 之后才建副本壳表/locmap/组，发物理基线
#   [5] ★★ 副本回放后，**基线之前**那些分片 xid 在副本的 clog 里也是 st=2
#       —— 修复前恒为 st=0（空洞 = RUNNING = 不可见 = 升主即丢行）
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
PASS=0; FAIL=0; NCHECK=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
DEXV() { docker exec -i -u postgres -e PGOPTIONS="-c citus.override_table_visibility=false" "$CONTAINER" "$@"; }
PSQL()  { local port=$1; shift; DEX  /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
PSQLV() { local port=$1; shift; DEXV /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
check() {
  NCHECK=$((NCHECK+1))
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

exec 9>/tmp/t72_baseclog.lock
if ! flock -n 9; then echo "FATAL: 另一个 T7.2 验收正在运行"; exit 99; fi

cleanup() {
  local p
  for p in $(seq 5432 5440); do
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
}
trap cleanup EXIT

echo "========== [0] 前置 =========="
cleanup
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" </dev/null)
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_master = on;" </dev/null >/dev/null
# ★ 协调者**自己也要配 tso_conninfo**：本套件的 join 三元组是在协调者上取的，
#   而 `partdist_tso_client_start_ts()` 走的是**客户端**通路。不配就返回 0，
#   join_info 变成 "gx,0,gsid" 非法值、事务当场中止 —— 而此前之所以能跑，
#   是**靠上一轮 ALTER SYSTEM 的残留**；环境一重建就现原形。
#   （与 R-P6-20 同一类："此前能跑只因现场有垫片"。）
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
tso=$(PSQL $COORD -Atc "SELECT partdist.partdist_tso_client_start_ts()" </dev/null 2>&1 | tail -1)
check "TSO 可发号（boot 防呆未挡）" "$([[ "$tso" =~ ^[0-9]+$ ]] && echo ok)" "ok"

echo "========== [1] 单分片分布表 =========="
# ★★ 每轮用**新表名**，不复用 t72_b（2026-09-10 实测教训）。
#   本套件按设计会在 worker 上直删分片表、把协调者侧的分布表留成半删状态，
#   而那张表**再也删不掉**：DROP 走 Citus 2PC，会被 §10「含分片打标表 DROP
#   禁 PREPARE」拦下；就算绕到各节点本地删，那个分区组这时往往已经因为主副本
#   的表先没了而凑不齐多数派，propose 直接失败。
#   于是复用同一个表名的话，第二轮必撞 "relation already exists"。
#   顺手尽力清一次历史残留，失败就算了 —— 清不掉正是上面那个原因。
TBASE="t72_b_$(date +%H%M%S)"
PSQL $COORD -q -c "DROP TABLE IF EXISTS ${TBASE};" </dev/null >/dev/null 2>&1
PSQL $COORD -v ON_ERROR_STOP=1 -q </dev/null <<SQL
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE ${TBASE}(id int, v text);
SELECT create_distributed_table('${TBASE}','id');
ALTER TABLE ${TBASE} SET (autovacuum_enabled = off);
SQL
GA=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='${TBASE}'::regclass" </dev/null)
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$GA" </dev/null)
TBL="${TBASE}_${GA}"
check "单分片就绪（shard=$GA @:$PA）" "$([[ -n "$GA" && -n "$PA" ]] && echo ok)" "ok"
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438; do
  [[ "$p" == "$PA" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; break; fi
done
check "取到两个副本（:$f1 :$f2）" "$([[ -n "$f1" && -n "$f2" ]] && echo ok)" "ok"

echo "========== [2] 打标 leader（副本此刻还不存在——这正是缺陷的姿势）=========="
OID_A=$(PSQLV $PA -Atc "SELECT '${TBL}'::regclass::oid" </dev/null | tail -1)
for pp in $PA $f1 $f2; do
  PSQL $pp -v ON_ERROR_STOP=1 -q </dev/null <<SQL
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION sclog_full(oid, bigint) RETURNS text
  AS '\$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;
SQL
  PSQL $pp -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
done
PSQL $PA -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OID_A}';" </dev/null >/dev/null
PSQL $PA -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 2
guc=$(PSQL $PA -Atc "SHOW pg_partdist.shard_relids" </dev/null)
check "leader 打标生效（oid=$OID_A）" "$guc" "$OID_A"

echo "========== [3] ★ 先写数据（判决落进 leader 的分片 clog）=========="
GX=$(PSQL $COORD -Atc "SELECT partdist.partdist_gxid_next()" </dev/null | tail -1)
STS=$(PSQL $COORD -Atc "SELECT partdist.partdist_tso_client_start_ts()" </dev/null | tail -1)
# ★ 先断言三元组本身合法，别把"取号取到 0"留给后面的断言去表现成"行取不到"
check "join 三元组合法（gxid=$GX start_ts=$STS，二者都须 >0）" \
      "$([[ "$GX" =~ ^[0-9]+$ && "$GX" -gt 0 && "$STS" =~ ^[0-9]+$ && "$STS" -gt 0 ]] && echo ok)" "ok"
w=$(PSQLV $PA -At </dev/null 2>&1 <<SQL
BEGIN;
SET LOCAL pg_partdist.join_info = '${GX},${STS},${GA}';
INSERT INTO ${TBL} SELECT g, 'pre-baseline-'||g FROM generate_series(1,20) g;
COMMIT;
SELECT 'w_done';
SQL
)
check "leader 上写入 20 行（基线之前）" "$([[ "$w" == *"w_done"* ]] && echo ok)" "ok"
SX=$(PSQLV $PA -Atc "SELECT min(xmin::text::bigint) FROM ${TBL}" </dev/null | tail -1)
check "取到基线前的分片 xid（sxid=$SX）" "$([[ -n "$SX" && "$SX" -ge 3 ]] && echo ok)" "ok"
lf=$(PSQL $PA -Atc "SELECT sclog_full(${OID_A}::oid, ${SX}::bigint)" </dev/null)
check "leader 的分片 clog 已判 st=2（$lf）" "$([[ "$lf" == *"st=2"* ]] && echo ok)" "ok"
LCTS=$(sed -E 's/.*cts=([0-9]+).*/\1/' <<< "$lf")

echo "========== [4] 之后才供给副本 + 发物理基线 =========="
nrels=$(PSQLV $PA -Atc "SELECT partdist.register_shard_fileset('${TBL}')" </dev/null | tail -1)
check "fileset 注册（成员数 ≥1）" "$([[ -n "$nrels" && "$nrels" -ge 1 ]] && echo ok)" "ok"
rows=$(PSQLV $PA -Atc "SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${TBL}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$rows"|cut -d, -f1|paste -sd,); ords=$(echo "$rows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$rows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$rows"|cut -d, -f4|paste -sd,); rels=$(echo "$rows"|cut -d, -f5|paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q </dev/null <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${TBL};
CREATE TABLE ${TBL} (LIKE ${TBASE} INCLUDING ALL);
ALTER TABLE ${TBL} SET (autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${TBL}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])" </dev/null)
  check "副本 :$fp locmap 配对" "$([[ -n "$np" && "$np" -ge 1 ]] && echo ok)" "ok"
done
pn=$((PA-5431)); f1n=$((f1-5431)); f2n=$((f2-5431))
for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
PSQL $PA -q -c "SELECT partdist.pg_raft_group_create(${GA}, ARRAY[${pn},${f1n},${f2n}]);" </dev/null >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $PA -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GA}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位（:$PA）" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${GA}, ARRAY[${pn},${f1n},${f2n}]);" </dev/null >/dev/null
  PSQL $fp -q -c "SELECT partdist.replay_enable('${TBL}')" </dev/null >/dev/null
done
sleep 3
base=$(PSQLV $PA -Atc "SELECT partdist.shard_baseline_emit('${TBL}'::regclass)" </dev/null 2>&1 | tail -1)
check "物理基线已发射（base_plsn=$base）" "$([[ "$base" =~ ^[0-9]+$ ]] && echo ok)" "ok"

echo "========== [5] ★★ 副本回放后，基线之前的分片 xid 也有判决 =========="
# ★ 主可能在建站→写数据→发基线这段时间里漂走（本宿主机 2 核 9 节点，实测 30 秒内
#   就漂过；partition_map 与 pg_dist_placement 都正确跟随 —— 路由层是好的，是夹具
#   假设了"主不动"）。这里按**当前** placement 重算主与副本，别认开局那个。
NEWPA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$GA" </dev/null)
if [[ -n "$NEWPA" && "$NEWPA" != "$PA" ]]; then
  echo "        [注意] 分片主已从 :$PA 漂到 :$NEWPA，按当前 placement 重算副本"
  PA=$NEWPA
  f1=""; f2=""
  for p in 5433 5434 5435 5436 5437 5438; do
    [[ "$p" == "$PA" ]] && continue
    if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; break; fi
  done
fi
ok5=""; det=""
for fp in $f1 $f2; do
  foid=$(PSQLV $fp -Atc "SELECT '${TBL}'::regclass::oid" </dev/null | tail -1)
  [[ "$foid" =~ ^[0-9]+$ ]] || { det+="[:$fp 无本地表] "; continue; }
  ff=""
  for t in $(seq 1 40); do
    ftip=$(PSQL $fp -Atc "SELECT partdist.get_partition_flush_lsn(${foid})" </dev/null)
    if [[ -n "$ftip" && "$ftip" != "0" ]]; then
      PSQL $fp -q -c "SELECT partdist.replay_catchup(${foid}::regclass, ${ftip}, 60000)" </dev/null >/dev/null 2>&1
    fi
    ff=$(PSQL $fp -Atc "SELECT sclog_full(${foid}::oid, ${SX}::bigint)" </dev/null)
    [[ "$ff" == *"st=2"* ]] && break
    sleep 1
  done
  det+="[:$fp oid=$foid $ff] "
  if [[ "$ff" == *"st=2"* ]]; then
    fcts=$(sed -E 's/.*cts=([0-9]+).*/\1/' <<< "$ff")
    [[ "$fcts" == "$LCTS" ]] && { ok5=ok; break; }
  fi
done
check "★★ 至少一个副本对基线**之前**的 sxid=$SX 判 st=2 且 cts 与 leader 相同（修复前恒 st=0）" "$ok5" "ok"
echo "        取证：$det"

echo "========== [6] T7.8（P7-D1）：leader DROP ⇒ 副本停流、摘槽位 =========="
# 缺陷：leader DROP TABLE 之后副本侧**完全静默** —— 壳表、回放槽位、
# pg_parwal/<oid> 全都留着永不回收；回收判据是"OID 不在本地 pg_class"，
# 而副本的壳表恰恰是本地真表，判据天然不成立。
armed_before=""
for fp in $f1 $f2; do
  foid=$(PSQLV $fp -Atc "SELECT '${TBL}'::regclass::oid" </dev/null | tail -1)
  [[ "$foid" =~ ^[0-9]+$ ]] || continue
  a=$(PSQL $fp -Atc "SELECT armed FROM partdist.replay_status() WHERE shard=${foid}" </dev/null 2>/dev/null | tail -1)
  armed_before+="[:$fp armed=$a] "
done
check "DROP 之前副本是 armed 的（$armed_before）" \
      "$([[ "$armed_before" == *"armed=t"* ]] && echo ok)" "ok"

# ★★ 为什么不走 `DROP TABLE ${TBASE}`（协调者侧）：
#   ① 带白名单删分布表会被 §10「含分片打标表 DROP 禁 PREPARE」拦下（Citus DDL 走
#      2PC），表静默存活 —— 于是"DROP 了却没通知副本"看起来像修复没生效；
#   ② **撤白名单也不再管用**（T7.4 之后）：升主/建组会把 OID 加进 shmem 打标集，
#      `ShardGatingActive()` 因此保持为真，禁令照拦。那个行为本身是**对的**
#      （这个分片确实是打标的），要改的是夹具。
#   所以这里在**主节点上直接删分片表**（`enable_ddl_propagation=off`），
#   这正是 leader 侧"fileset 里有、catalog 里没了"的形态 —— T7.8 要验的就是它。
LEADPORT=$PA
PSQLV $LEADPORT -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE ${TBL};" </dev/null >/dev/null 2>&1
gone=$(PSQLV $LEADPORT -Atc "SELECT count(*) FROM pg_class WHERE relname='${TBL}'" </dev/null | tail -1)
check "主节点 :$LEADPORT 上分片表已删除" "$gone" "0"
# ★★ 通知**不是**在 DROP 语句执行完那一刻发的，也不在 PRE_COMMIT 上
#   （2026-09-10 修正：我上一版注释写的"没有 2PC 就走 PRE_COMMIT 那条常规
#     路径"是错的 —— PRE_COMMIT 上只有 fileset 发射器，压根没有 DROP 扫描，
#     所以普通 DROP 一条通知都发不出来，实测 leader 日志里零命中）。
#   现在是**提交后惰性补发**：提交回调只推一次 shmem 代次，扫描推迟到
#   本节点**下一条语句**开头。所以这里必须在主节点上再发一条语句去触发它。
PSQLV $LEADPORT -Atc "SELECT 1" </dev/null >/dev/null 2>&1
sleep 2
ok6=""; det6=""
for fp in $f1 $f2; do
  foid=$(PSQLV $fp -Atc "SELECT '${TBL}'::regclass::oid" </dev/null 2>/dev/null | tail -1)
  [[ "$foid" =~ ^[0-9]+$ ]] || { det6+="[:$fp 壳表已不在] "; continue; }
  for t in $(seq 1 30); do
    ftip=$(PSQL $fp -Atc "SELECT partdist.get_partition_flush_lsn(${foid})" </dev/null 2>/dev/null)
    [[ -n "$ftip" && "$ftip" != "0" ]] && \
      PSQL $fp -q -c "SELECT partdist.replay_catchup(${foid}::regclass, ${ftip}, 3000)" </dev/null >/dev/null 2>&1
    a=$(PSQL $fp -Atc "SELECT armed FROM partdist.replay_status() WHERE shard=${foid}" </dev/null 2>/dev/null | tail -1)
    [[ "$a" == "f" ]] && { ok6=ok; det6+="[:$fp armed=f（${t}s）] "; break; }
    sleep 1
  done
  [[ "$ok6" == "ok" ]] && break
  det6+="[:$fp 仍 armed] "
done
check "★★ leader DROP 后副本停流（armed=f；修复前完全静默）" "$ok6" "ok"
echo "        取证：$det6"
echo "        注：壳表与 pg_parwal 目录**刻意保留** —— 删表是 DDL，回放侧不代替"
echo "            运维做删除；摘掉 armed 之后它就是一张普通本地表，可审计地回收。"

echo
echo "结果：PASS=${PASS} FAIL=${FAIL}"
if [[ "$NCHECK" -lt 16 ]]; then
  echo "FATAL: 只跑了 ${NCHECK} 条断言（应 >=16）——夹具中途退出，结果不可信"; exit 98
fi
exit $FAIL
