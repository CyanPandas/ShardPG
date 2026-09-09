#!/usr/bin/env bash
# [宿主机] T7.3 + T7.4 验收：升主时的**文件号交接**与**打标身份继承**。
#
# 两条缺陷都来自同一个断层：交接只做了"角色 + 捕获"，没把**身份**带过去。
#
#   R-P6-16（T7.3）：升主不广播 FILESET_UPDATE ⇒ 新主写入带的是它自己的
#     relfilenumber，其余副本的 locmap 仍对着旧主 ⇒ `replay_catchup` 报
#     「未知 relfilelocator（fileset 漏登记）」⇒ 那些副本从此**放不了**新主的流，
#     也就失去再次当选资格，直到有人从新主重新供给。
#     批次 #10 的 p7 [3b] 只断言了副本**收到**新主的记录，没断言**放得了** ——
#     所以这条当时没被测出来。本套件补的正是那一格。
#
#   R-P6-21（T7.4）：打标身份只由白名单 GUC 或重启扫目录装载，供给/升主都不加
#     ⇒ 新主若没人工加白名单又没重启，**写入不打标、读走原生路径**。
#
# 验收标准：
#   [5] 杀主后有新主当选，且 partition_map / pg_dist_placement 跟随
#   [6] ★ 新主的流里出现 PRIMARY_HANDOVER 的 FILESET_UPDATE（flags 位 0x4）
#   [7] ★★ **另一个副本**能把新主的写入回放进去（修复前恒报"未知 relfilelocator"）
#   [8] ★★ 新主的写入**带分片 xid**（打标身份继承；修复前走原生路径）
set -u
# ★ 先把脚本目录取成绝对路径再 cd：cd 之后 $0 还是相对路径，
#   再 dirname 就会指到新 cwd 下的同名子目录（第一版实测 source 不到 lib）
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

exec 9>/tmp/t73_handover.lock
if ! flock -n 9; then echo "FATAL: 另一个 T7.3 验收正在运行"; exit 99; fi

source "$HERE/lib_node_health.sh"; health_mark_start

cleanup() {
  local p
  for p in $(seq 5432 5440); do
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
}
trap cleanup EXIT

echo "========== [0] 前置：group0 收敛 + 撤残留白名单 =========="
cleanup
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" </dev/null)
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

# ★ TSO 前置冒烟：master 一旦重启过，boot 防呆（设计 §2.4）会**拒绝发号**，
#   症状是本套件后面每一步都莫名其妙地空/红。这里先响亮地失败，并给出动作。
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_master = on;" </dev/null >/dev/null 2>&1
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
tso=$(PSQL $COORD -Atc "SELECT partdist.partdist_tso_client_start_ts()" </dev/null 2>&1 | tail -1)
check "TSO 可发号（boot 防呆未挡）" "$([[ "$tso" =~ ^[0-9]+$ ]] && echo ok)" "ok"
if [[ ! "$tso" =~ ^[0-9]+$ ]]; then
  echo "        ↑ 若报「检测到上个纪元的 boot 标记」：master 重启过。测试集群的处置是"
  echo "          rm \$PGDATA/pg_tso_boot 后重启 coordinator（生产语义见设计 §2.4：整簇重建）。"
fi

echo "========== [1] 夹具：2 分片分布表 =========="
PSQL $COORD -q -c "DROP TABLE IF EXISTS t73_mk;" </dev/null >/dev/null 2>&1
PSQL $COORD -v ON_ERROR_STOP=1 -q </dev/null <<'SQL'
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;
CREATE TABLE t73_mk(id int, v text);
SELECT create_distributed_table('t73_mk','id');
ALTER TABLE t73_mk SET (autovacuum_enabled = off);
SQL
GA=$(PSQL $COORD -Atc "SELECT min(shardid) FROM pg_dist_shard WHERE logicalrelid='t73_mk'::regclass" </dev/null)
GB=$(PSQL $COORD -Atc "SELECT max(shardid) FROM pg_dist_shard WHERE logicalrelid='t73_mk'::regclass" </dev/null)
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$GA" </dev/null)
PB=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$GB" </dev/null)
check "两分片落在不同 worker（A@:$PA B@:$PB）" "$([[ -n "$PA" && -n "$PB" && "$PA" != "$PB" ]] && echo ok)" "ok"

KA=""; KB=""
for k in $(seq 1 200); do
  s=$(PSQL $COORD -Atc "SELECT get_shard_id_for_distribution_column('t73_mk', $k)" </dev/null)
  [[ "$s" == "$GA" && -z "$KA" ]] && KA=$k
  [[ "$s" == "$GB" && -z "$KB" ]] && KB=$k
  [[ -n "$KA" && -n "$KB" ]] && break
done
check "取到命中两分片的分布键（KA=$KA KB=$KB）" "$([[ -n "$KA" && -n "$KB" ]] && echo ok)" "ok"

TBL="t73_mk_${GA}"
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$PA" || "$p" == "$PB" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; break; fi
done
check "取到两个 follower（:$f1 :$f2）" "$([[ -n "$f1" && -n "$f2" ]] && echo ok)" "ok"

echo "========== [2] TSO 服务与客户端 =========="
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_master = on;" </dev/null >/dev/null
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
for pp in $PA $PB $f1 $f2; do
  PSQL $pp -v ON_ERROR_STOP=1 -q </dev/null <<SQL
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION sclog_full(oid, bigint) RETURNS text
  AS '\$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;
SQL
  PSQL $pp -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
  PSQL $pp -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
tso2=$(PSQL $PA -Atc "SELECT partdist.partdist_tso_client_start_ts()" </dev/null 2>&1 | tail -1)
check "worker 侧 TSO 客户端可取号（$tso2）" "$([[ "$tso2" =~ ^[0-9]+$ ]] && echo ok)" "ok"

echo "========== [3] 两个分片对称建站：fileset + 壳表/locmap + 分区组 =========="
pnode=$((PA - 5431)); f1node=$((f1 - 5431)); f2node=$((f2 - 5431)); pbnode=$((PB - 5431))
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
members_b="ARRAY[${pbnode}, ${f1node}, ${f2node}]"

# ★ 两个分片都要**完整**建站，不能只给协调分片建。三处约束，每处都实测踩过：
#
#   ① 身份（rebuild_shard_identity）四个节点都要重建。漏了 B 的后果很隐蔽：
#      `dtx_local_participant` 靠 shard_identity 把本地 oid 翻成 gsid，翻不出来就
#      登记空写集 ⇒ master 算出的写集只剩 1 个组 ⇒ 撞 §3.4 **快路径**
#      （`nparts <= 1` 直接 return，不做决议）⇒ 判决永远不产生 ⇒ 行永久不可见。
#   ② follower 必须有**壳表 + locmap**，B 也不例外。缺了就报
#      `pg_raft: group … 在本节点没有对应分片，无法落盘` ⇒ 数据条目凑不到多数派
#      ⇒ 写入直接失败（`record 1 未达多数派`）。
#   ③ 建组顺序：**placement 节点先建组并当选**（谁先建谁容易赢选举；placement
#      不是 leader 就写不了自己的分片），**再让 follower 入组并等就绪**
#      （否则同样凑不到多数派）。两个方向都实测红过。
provision_shard() {   # <leader_port> <gid> <tbl> <members_expr> <f1> <f2>
  local lp=$1 gid=$2 tbl=$3 mem=$4 fa=$5 fb=$6
  local n rows roles ords spcs dbs rels fp np
  n=$(PSQLV $lp -Atc "SELECT partdist.register_shard_fileset('${tbl}')" </dev/null | tail -1)
  check "分片 ${gid} leader:${lp} fileset 注册（成员数 ≥1）" \
        "$([[ -n "$n" && "$n" -ge 1 ]] && echo ok)" "ok"
  rows=$(PSQLV $lp -Atc "SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${tbl}') ORDER BY role, ord" </dev/null | grep ',')
  roles=$(echo "$rows" | cut -d, -f1 | paste -sd,); ords=$(echo "$rows" | cut -d, -f2 | paste -sd,)
  spcs=$(echo "$rows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$rows" | cut -d, -f4 | paste -sd,)
  rels=$(echo "$rows" | cut -d, -f5 | paste -sd,)
  for fp in $fa $fb; do
    PSQL $fp -v ON_ERROR_STOP=1 -q </dev/null <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${tbl};
CREATE TABLE ${tbl} (LIKE t73_mk INCLUDING ALL);
ALTER TABLE ${tbl} SET (autovacuum_enabled = off);
SQL
    np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])" </dev/null)
    check "分片 ${gid} follower :$fp locmap 配对" "$([[ -n "$np" && "$np" -ge 1 ]] && echo ok)" "ok"
  done
}

group_ready() {   # <gid> <follower...>：等 follower 都报出该组
  local gid=$1; shift
  local fp t seen
  for t in $(seq 1 15); do
    seen=1
    for fp in "$@"; do
      [[ -z "$(PSQL $fp -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)" ]] && seen=0
    done
    if [[ "$seen" == "1" ]]; then sleep 2; echo ok; return; fi
    sleep 1
  done
  echo timeout
}

bring_up_group() {  # <leader_port> <gid> <members_expr> <f1> <f2>
  local lp=$1 gid=$2 mem=$3 fa=$4 fb=$5 st fp t
  PSQL $lp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${mem});" </dev/null >/dev/null
  st=""
  for t in $(seq 1 20); do
    st=$(PSQL $lp -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
    [[ "$st" == "leader" ]] && break; sleep 1
  done
  check "分区组 ${gid} leader 就位（在 placement 节点 :${lp}）" "$st" "leader"
  for fp in $fa $fb; do
    PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${mem});" </dev/null >/dev/null
  done
  check "分区组 ${gid} 的 follower 已入组" "$(group_ready $gid $fa $fb)" "ok"

  # ★ 还要等 **partition_map 登记**，这一步最容易漏。
  #   自治选举胜出只是本地状态；DTX 决议路径读的是
  #   `SELECT primary_node FROM partdist.partition_map WHERE partition_id = coord_gsid`
  #   （raft_consensus.c），读不到就 `dtx_master_try_write_abort()` + ERROR（fail-closed）。
  #   中间隔着"自治选举 → 上报 → group0 apply → 落 partition_map"一整条异步链。
  #   只等 `pg_raft_group_status().state == 'leader'` 就往下走，是本项目记过案的
  #   夹具缺口（TX1 [3] 同款）。
  local pm=""
  for t in $(seq 1 30); do
    pm=$(PSQL $COORD -Atc "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${gid}" </dev/null 2>/dev/null)
    [[ -n "$pm" && "$pm" != "0" ]] && break
    sleep 1
  done
  check "分区组 ${gid} 已登记进 partition_map（primary_node=$pm）" \
        "$([[ -n "$pm" && "$pm" != "0" ]] && echo ok)" "ok"
}

provision_shard $PA $GA "$TBL" "$members"   $f1 $f2
provision_shard $PB $GB "t73_mk_${GB}" "$members_b" $f1 $f2
for p in $PA $PB $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
bring_up_group $PA $GA "$members"   $f1 $f2
bring_up_group $PB $GB "$members_b" $f1 $f2
# 只有分片 A 需要真回放（第 [7] 步要在它的副本上验判决）
for fp in $f1 $f2; do
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${TBL}')" </dev/null)
  check "follower :$fp replay_enable（分片 A）" "$en" "t"
done

echo "========== [3b] 打标：组内**全体成员**都要配白名单 =========="
# ★ 为什么不是只给 placement 节点配（第一版就是那么写的，红了两轮）：
#   ① 判定一张表是否打标只看本节点的白名单/shmem 集（`shard_oid_is_mvcc`），
#      而 **分区组的主随时可能自治切换**（本环境 2 核 9 节点，实测 30 秒内
#      102041 的主就从 :5433 漂到 :5435，`partition_map` 与 `pg_dist_placement`
#      都正确跟随了 —— 路由层是好的，是夹具假设了"主不动"）；
#   ② 新主若没有白名单，写入就不打标、读走原生路径 —— 这正是 R-P6-21
#      （供给/升主不携带打标身份）。给全体成员配白名单是**当前的运维绕法**，
#      演示文档 6c 同款；T7.4 修好 R-P6-21 之后这一段可以删掉。
mark_all() {
  local pp oid list
  for pp in $PA $PB $f1 $f2; do
    list=$(PSQLV $pp -Atc "SELECT string_agg(oid::text, ',') FROM pg_class WHERE relname IN ('t73_mk_${GA}','t73_mk_${GB}') AND relkind='r'" </dev/null | tail -1)
    [[ -z "$list" ]] && continue
    PSQL $pp -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${list}';" </dev/null >/dev/null
    PSQL $pp -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
  done
  sleep 2
}
mark_all
OID_A=$(PSQLV $PA -Atc "SELECT '${TBL}'::regclass::oid" </dev/null | tail -1)
guc=$(PSQL $PA -Atc "SHOW pg_partdist.shard_relids" </dev/null)
check "打标生效（:$PA 白名单=$guc，含分片A oid=$OID_A）" \
      "$([[ "$guc" == *"$OID_A"* ]] && echo ok)" "ok"

LOID=$(PSQL $PA -Atc "SELECT partdist.local_partition_for_shard(${GA})" </dev/null)
check "leader 侧本地分区 oid" "$([[ -n "$LOID" && "$LOID" != "0" ]] && echo ok)" "ok"
# ★ 不再用"事务前的 flush_lsn"当扫描下界：分区主可能在中途漂走，
#   开局那个 before 是**另一个节点**流上的位置，拿来当下界就全错了。
#   本夹具的流很短，[5] 直接从 1 扫到当前 flush_lsn。

echo "========== [4] 跨分片 2PC 提交 + 判决收敛 =========="
# ★ 打标分片的 Citus 路由写必须走 join 传播协议：驱动端在同一事务里
#   `SET LOCAL pg_partdist.join_info='gxid,start_ts,coord_gsid'`，
#   由 citus.propagate_set_commands=local 带到各 worker 连接上。
#   不带就撞 T4.3 的禁令："未加入全局事务的分片写不允许 PREPARE TRANSACTION"
#   （第一版用例正是这么红的，如实留痕）。
#   三个值都取自**产品接口**（partdist 模式里的真函数），不建任何 public 垫片。
# ★ 写入要能容忍**分区主的自治切换**：切主后 placement 会跟着走（路由层已实证），
#   但正在飞的那条语句会撞 `本节点不是该分区组的 leader / 请经路由层重试`。
#   夹具照它自己的 DETAIL 办：重试，并在每次重试前重取 join 三元组、补白名单。
txn_ok=""
for try in 1 2 3; do
  GX=$(PSQL $COORD -Atc "SELECT partdist.partdist_gxid_next()" </dev/null | tail -1)
  STS=$(PSQL $COORD -Atc "SELECT partdist.partdist_tso_client_start_ts()" </dev/null | tail -1)
  out=$(PSQL $COORD -v ON_ERROR_STOP=1 -At </dev/null 2>&1 <<SQL
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = '${GX},${STS},${GA}';
  INSERT INTO t73_mk VALUES (${KA}, 'a-commit');
  INSERT INTO t73_mk VALUES (${KB}, 'b-commit');
COMMIT;
SELECT 'txn_done';
SQL
)
  [[ "$out" == *"txn_done"* ]] && { txn_ok=ok; break; }
  echo "        第 ${try} 次写入未成功：$(tail -2 <<< "$out" | tr '\n' ' ')"
  mark_all
  sleep 3
done
check "跨分片 2PC 事务提交成功（第 ${try} 次，gxid=$GX coord_gsid=$GA）" "$txn_ok" "ok"

# ★ 主可能已经漂走：后面的断言一律按**当前** placement 取节点，不认开局那个。
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$GA" </dev/null)
OID_A=$(PSQLV $PA -Atc "SELECT '${TBL}'::regclass::oid" </dev/null | tail -1)
LOID=$(PSQL $PA -Atc "SELECT partdist.local_partition_for_shard(${GA})" </dev/null)
check "分片 A 当前 primary=:$PA（oid=$OID_A loid=$LOID）" "$([[ -n "$PA" && -n "$LOID" && "$LOID" != "0" ]] && echo ok)" "ok"
# ★ 打标分片的行**不是提交即可见**：判决走协调组决议的异步广播/清扫收敛，
#   在判决落进分片 clog 之前，读者按 §4.2 三态处置一律判不可见（不阻塞、不安装）。
#   所以这里断言的是"COMMIT 语句成功"，可见性放到收敛之后断言 ——
#   第一版把两者写成一条，实测红在"实际=0 期望=2"，那是**用例的错**，不是缺陷。
vis=""
for t in $(seq 1 60); do
  vis=$(PSQLV $PA -Atc "SELECT count(*) FROM ${TBL} WHERE v='a-commit'" </dev/null | tail -1)
  [[ "$vis" == "1" ]] && break
  PSQL $PA -q -c "SELECT partdist.dtx_pending_sweep();" </dev/null >/dev/null 2>&1
  sleep 1
done
check "判决收敛后 leader 上该行可见（${t}s）" "$vis" "1"

SX=$(PSQLV $PA -Atc "SELECT xmin::text::bigint FROM ${TBL} WHERE v='a-commit'" </dev/null | tail -1)
check "leader 上该行的分片 xid（xmin=$SX）" "$([[ -n "$SX" && "$SX" -ge 3 ]] && echo ok)" "ok"

lead_f=""; CTS=""
if [[ -n "$SX" && "$SX" =~ ^[0-9]+$ ]]; then
  for t in $(seq 1 40); do
    lead_f=$(PSQL $PA -Atc "SELECT sclog_full(${OID_A}::oid, ${SX}::bigint)" </dev/null)
    if [[ "$lead_f" == *"st=2"* ]]; then
      CTS=$(sed -E 's/.*cts=([0-9]+).*/\1/' <<< "$lead_f"); break
    fi
    PSQL $PA -q -c "SELECT partdist.dtx_pending_sweep();" </dev/null >/dev/null 2>&1
    sleep 1
  done
fi
check "leader 分片 clog 判决落账 st=2（$lead_f）" \
  "$([[ "$lead_f" == *"st=2"* ]] && echo ok)" "ok"
check "leader 判决带非零 commit_ts（cts=$CTS）" \
  "$([[ -n "$CTS" && "$CTS" != "0" ]] && echo ok)" "ok"

echo "========== [5] 杀主 → 新主当选 → 路由跟随 =========="
OLD_PRIMARY=$PA
NEWP=""
DEX bash -c "/work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/worker$((OLD_PRIMARY-5432)) -m immediate stop" </dev/null >/dev/null 2>&1
for t in $(seq 1 90); do
  NEWP=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$GA" </dev/null 2>/dev/null)
  [[ -n "$NEWP" && "$NEWP" != "$OLD_PRIMARY" ]] && break
  sleep 1
done
check "杀掉旧主 :$OLD_PRIMARY 后，路由切到新主 :$NEWP（${t}s）" \
      "$([[ -n "$NEWP" && "$NEWP" != "$OLD_PRIMARY" ]] && echo ok)" "ok"
pm=$(PSQL $COORD -Atc "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${GA}" </dev/null)
check "partition_map 也跟随（primary_node=$pm ⇒ 端口 $((5431+pm))）" \
      "$([[ -n "$pm" && "$((5431+pm))" == "$NEWP" ]] && echo ok)" "ok"

echo "========== [6] ★ 新主广播了 PRIMARY_HANDOVER 的 FILESET_UPDATE =========="
NOID=$(PSQLV $NEWP -Atc "SELECT '${TBL}'::regclass::oid" </dev/null | tail -1)
NLOID=$(PSQL $NEWP -Atc "SELECT partdist.local_partition_for_shard(${GA})" </dev/null)
ntip=$(PSQL $NEWP -Atc "SELECT partdist.get_partition_flush_lsn(${NLOID})" </dev/null)
hv=$(PSQL $NEWP -Atc "
  SELECT g FROM generate_series(1, ${ntip}) g,
       LATERAL partdist.partwal_read_record(${NLOID}::oid, g) r
   WHERE r.flags = 4 AND r.info = 1
     AND ((get_byte(r.data,5)::int<<8)|get_byte(r.data,4)::int) & 4 = 4
   ORDER BY g DESC LIMIT 1" </dev/null)
check "★ 新主流里有 PRIMARY_HANDOVER 的 FILESET_UPDATE（plsn=$hv）" \
      "$([[ -n "$hv" ]] && echo ok)" "ok"

echo "========== [7][8] ★★ 另一个副本放得了新主的流 + 新主写入带分片 xid =========="
# 新主之外、仍活着的那个成员
OTHER=""
for fp in $f1 $f2; do [[ "$fp" != "$NEWP" ]] && OTHER=$fp; done
check "取到另一个副本 :$OTHER" "$([[ -n "$OTHER" ]] && echo ok)" "ok"

# 新主上写一行（走 leader 直写，避开 Citus 路由缓存）
NGX=$(PSQL $COORD -Atc "SELECT partdist.partdist_gxid_next()" </dev/null | tail -1)
NSTS=$(PSQL $COORD -Atc "SELECT partdist.partdist_tso_client_start_ts()" </dev/null | tail -1)
wout=$(PSQLV $NEWP -At </dev/null 2>&1 <<SQL
BEGIN;
SET LOCAL pg_partdist.join_info = '${NGX},${NSTS},${GA}';
INSERT INTO ${TBL} VALUES (${KA}, 'after-promote');
COMMIT;
SELECT 'w_done';
SQL
)
check "新主上写入成功" "$([[ "$wout" == *"w_done"* ]] && echo ok)" "ok"

# ★★ 打标身份：新写入那行的 xmin 必须是**分片 xid**（小整数），不是原生 xid
nxmin=$(PSQLV $NEWP -Atc "SELECT xmin::text::bigint FROM ${TBL} WHERE v='after-promote'" </dev/null | tail -1)
check "★★ 新主写入带分片 xid（xmin=$nxmin，应 < 1000；未继承打标时是原生大 xid）" \
      "$([[ -n "$nxmin" && "$nxmin" -lt 1000 ]] && echo ok)" "ok"

# ★★ 另一个副本能回放新主的流（修复前恒报"未知 relfilelocator"）
oerr=""; ook=""
if [[ -n "$OTHER" ]]; then
  ooid=$(PSQLV $OTHER -Atc "SELECT '${TBL}'::regclass::oid" </dev/null | tail -1)
  for t in $(seq 1 40); do
    otip=$(PSQL $OTHER -Atc "SELECT partdist.get_partition_flush_lsn(${ooid})" </dev/null)
    if [[ -n "$otip" && "$otip" != "0" ]]; then
      oerr=$(PSQL $OTHER -Atc "SELECT partdist.replay_catchup(${ooid}::regclass, ${otip}, 60000)" </dev/null 2>&1 | tail -1)
      [[ "$oerr" =~ ^[0-9]+$ ]] && { ook=ok; break; }
    fi
    sleep 1
  done
fi
check "★★ 另一副本 :$OTHER 回放新主的流成功（applied=$oerr；修复前报'未知 relfilelocator'）" \
      "$ook" "ok"
echo "        取证：$oerr"

echo
echo "结果：PASS=${PASS} FAIL=${FAIL}"
# ★ 计数守卫：零个检查会静默通过（feedback: test-harness-silent-pass）
if [[ "$NCHECK" -lt 24 ]]; then
  echo "FATAL: 只跑了 ${NCHECK} 条断言（应 >=24）——夹具中途退出，结果不可信"
  exit 98
fi
health_check_no_drops || true
exit $FAIL
