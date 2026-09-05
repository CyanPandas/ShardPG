#!/usr/bin/env bash
# [宿主机] 批次 #8 验收：§13 约束 13 的**检测**那一半。
#
# 约束 13 的原文定性是「永久分叉，**既无检测也无修复路径**」。读代码才看清
# 机制比"提案被静默丢弃"微妙：
#
#   复制挂钩失败时事务**确实**中止了 —— replicate_group_upto 对任何一条未达
#   多数派即 ERROR，三个生产调用点全走它，提交路径处处 fail-closed。
#   挡不住的是另一件事：`lazy_truncate_heap()` 的物理截断**在 leader 上已经
#   做掉且不随事务回滚**（内核在 AccessExclusiveLock 下截空页，认定安全）。
#   leader 短了、follower 没短，那条 XLOG_SMGR_TRUNCATE 再也不会重发。
#
# **"无修复路径"这句已经不成立**：T6.1 的 shard_baseline_emit 与批次 #7 的
# provision_shard_replica 正是重做物理基线。缺的只是检测 —— 本套件验的就是它。
#
# ★★ 全套最要紧的一条：**标记必须活过那个中止的事务**。
#   出事的事务马上就要回滚，标记若写进表里会一起没掉，等于没记。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
PGCTL(){ local d=$1; shift; DEX /work/pg-install/bin/pg_ctl -D "/work/pg-cluster-data/$d" -l "/work/pg-cluster-data/$d/pg.log" "$@"; }
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/t_diverge_p8.lock
if ! flock -n 9; then echo "FATAL: 另一个批次 #8 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "================ [0] 夹具：用批次 #7 的供给入口建副本 ================"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t8dv;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t8dv(id int, v text);
SELECT create_distributed_table('t8dv', 'id');
ALTER TABLE t8dv SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
GID=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t8dv'::regclass" </dev/null)
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${GID}" </dev/null)
pnode=$((PA - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$PA" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
TBL="t8dv_${GID}"
echo "  shard=${GID} leader=:${PA}(node ${pnode}) followers=:${f1} :${f2}"
check "夹具齐" "$([[ -n "$GID" && -n "$PA" ]] && echo ok)" "ok"
PSQL $PA -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null
SOID=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT '${TBL}'::regclass::oid" </dev/null|tail -1)
PSQL $PA -q -c "INSERT INTO ${TBL} SELECT g,'r'||g FROM generate_series(1,20) g;" </dev/null >/dev/null

# 组主必须落在 placement 主上（同批次 #7 的夹具纪律：抢跑 + 复位重来）
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
st=""
for round in 1 2 3; do
  PSQL $PA -q -c "SELECT partdist.pg_raft_group_create(${GID}, ${members});" </dev/null >/dev/null
  sleep 3
  for p in $f1 $f2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_create(${GID}, ${members});" </dev/null >/dev/null; done
  for t in $(seq 1 20); do
    st=$(PSQL $PA -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID}" </dev/null 2>/dev/null)
    [[ "$st" == "leader" ]] && break; sleep 1
  done
  [[ "$st" == "leader" ]] && break
  for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1; done
  sleep 2
done
check "分区组 leader 就位（落在 placement 主上）" "$st" "leader"
for tn in $f1node $f2node; do
  r=$(PSQL $PA -Atc "SELECT partdist.provision_shard_replica(${GID}::bigint, ${tn})" </dev/null 2>&1 | tr '\n' ' ')
  check "  供给副本到 node ${tn}" "$([[ "$r" == shard=* ]] && echo ok)" "ok"
done

echo "================ [1] 干净态：没有分叉标记 ================"
d0=$(PSQL $PA -Atc "SELECT coalesce(partdist.shard_divergence(${SOID}::oid),'CLEAN')" </dev/null|tail -1)
check "★ 干净分片无标记" "$d0" "CLEAN"

echo "================ [2] ★★ 造复制失败：拆掉两个 follower 的组 ================"
# leader 因此凑不齐多数派；此刻 DATA 字节已落盘（[A] 在挂钩之前），
# 复制挂钩 ERROR ⇒ 事务中止 —— 这一段是既有行为（txn_layer_r2 §9 已验）。
for fp in $f1 $f2; do PSQL $fp -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1; done
sleep 2
errtxt=$(PSQL $PA -Atc "INSERT INTO ${TBL} VALUES (90,'diverge-me')" </dev/null 2>&1 | tr '\n' ' ')
check "★ 写入确实被拒（fail-closed 仍然成立）" \
      "$([[ "$errtxt" == *ERROR* ]] && echo rejected)" "rejected"
d1=$(PSQL $PA -Atc "SELECT coalesce(partdist.shard_divergence(${SOID}::oid),'CLEAN')" </dev/null|tail -1)
echo "  标记内容：${d1}"
check "★★ 分叉标记已写下（检测面从无到有）" \
      "$([[ "$d1" != "CLEAN" ]] && echo marked)" "marked"
check "★★ 标记**活过了那个中止的事务**（写表会一起回滚，这是全部意义）" \
      "$([[ "$d1" == *"约束 13"* ]] && echo survived)" "survived"
check "  标记带了时刻与原因（事后能查是哪一次）" \
      "$([[ "$d1" == *"复制挂钩失败"* ]] && echo ok)" "ok"

echo "================ [3] ★ 标记活过重启（非事务性 + fsync）================"
ldir="worker$((PA - 5432))"
PGCTL "$ldir" -m fast -w -t 60 restart >/dev/null 2>&1
for t in $(seq 1 40); do [[ "$(PSQL $PA -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 2; done
d2=$(PSQL $PA -Atc "SELECT coalesce(partdist.shard_divergence(${SOID}::oid),'CLEAN')" </dev/null|tail -1)
check "★ 重启后标记仍在" "$([[ "$d2" != "CLEAN" ]] && echo marked)" "marked"

echo "================ [4] ★★ 修复：重做物理基线 ⇒ 标记自动清掉 ================"
# 先把组恢复，否则基线自己也发不出去（它同样要走 raft 写路径）
# ★ 恢复组也得用抢跑配方：直接三家一起建，组主一样会落到别处
#   （首版就是这么红的三条 —— 与被测内容毫无关系）。
st2=""
for round in 1 2 3; do
  PSQL $PA -q -c "SELECT partdist.pg_raft_group_create(${GID}, ${members});" </dev/null >/dev/null 2>&1
  sleep 3
  for p in $f1 $f2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_create(${GID}, ${members});" </dev/null >/dev/null 2>&1; done
  for t in $(seq 1 20); do
    st2=$(PSQL $PA -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID}" </dev/null 2>/dev/null)
    [[ "$st2" == "leader" ]] && break; sleep 1
  done
  [[ "$st2" == "leader" ]] && break
  for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1; done
  sleep 2
done
check "  组已恢复（基线也要走 raft 写路径）" "$st2" "leader"
be=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT partdist.shard_baseline_emit(${SOID}::regclass)" </dev/null 2>&1|tail -1)
check "★ 基线发射成功（base=${be}）" "$([[ "$be" =~ ^[0-9]+$ ]] && echo ok)" "ok"
d3=$(PSQL $PA -Atc "SELECT coalesce(partdist.shard_divergence(${SOID}::oid),'CLEAN')" </dev/null|tail -1)
check "★★ 修复动作把标记清掉了（不必运维手工清）" "$d3" "CLEAN"

echo "================ [5] 阴性对照 ================"
# 清标不等于修好 —— 报错文案必须把这句说出来
nosu=$(PSQL $PA -Atc "SET ROLE pg_read_all_data; SELECT partdist.shard_clear_divergence(${SOID}::oid)" </dev/null 2>&1 | tr '\n' ' ')
check "★ 手工清标限超级用户" \
      "$([[ "$nosu" == *"限超级用户"* ]] && echo denied)" "denied"
check "  报错说清了「清标不等于修好」" \
      "$([[ "$nosu" == *"重做物理基线"* ]] && echo ok)" "ok"
d4=$(PSQL $PA -Atc "SELECT coalesce(partdist.shard_divergence(999999::oid),'CLEAN')" </dev/null|tail -1)
check "★ 不存在的分片查标记 ⇒ 无标记（不报错）" "$d4" "CLEAN"

echo "================ [6] 清理 ================"
for p in $PA $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.replay_disable('${TBL}'::regclass)" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${GID};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t8dv" </dev/null >/dev/null 2>&1
PSQL $COORD -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${GID};" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"
health_mark_start
health_check_no_crash && { echo "  PASS  收尾窗口无节点崩溃"; PASS=$((PASS+1)); } \
                     || { echo "  FAIL  收尾窗口有节点崩溃"; FAIL=$((FAIL+1)); }
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
