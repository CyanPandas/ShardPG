#!/usr/bin/env bash
# [宿主机] T6.8-2 验收：回放上界（惰性回放的核心不变式）。
#
# 设计里反复写着一句话：**"绝不碰未提交条目"**，做法是"上界由调用方给定，
# 升主路径传该组的 Raft 已提交位点"。查下来这条链**是通的**：
# follower_partition_map.applied_part_lsn 只在 raft apply 里按 [idx, commit_index]
# 合并推进（raft_consensus.c），天然不超过 commit_index；升主路径
# pg_raft_promote_prepare 取的正是它。
#
# ★★ 但在本套件之前，**这条不变式一条断言都没有**：
#   ① 全部 8 处验收传给 replay_catchup 的上界都是 get_partition_flush_lsn ——
#      那是 **leader 侧已刷盘的字节**，不是 follower 的已提交游标。也就是说
#      我们一直在按"leader 写了多少就放多少"回放，**上界形同虚设**；
#   ② `bound == 0`（"追到本地段末尾"）原本是**无条件**回退，而本该门控它的
#      pg_partdist.replay_trust_local_segments **定义了却没有一行代码读它**
#      —— 12 个套件在开它，开与不开毫无分别。
#
# 本套件把两件事都钉住：
#   - 给了上界就**不许越界**，哪怕越界的那些字节已经躺在本地段里唾手可得；
#   - 不给上界就**必须报错**（fail-closed），除非显式打开测试模式。
#
# ★ 几何怎么造：**不去制造"未达多数派"**。那条路不稳 —— 组一 reset，follower
#   连字节都收不到，"上界之外还有字节"这个前提本身就没了，断言会变成空的。
#   等价且确定的形态是：让字节**已经在 follower 本地段里**，却给一个更小的上界。
#   要验的性质完全相同 —— 回放会不会跑到本地段末尾去。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/t68_replay_bound.lock
if ! flock -n 9; then echo "FATAL: 另一个回放上界验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "================ [0] 夹具：1 分片 + raft 组 + 2 follower ================"
# ★ 本套件**必须**以 trust=off 起跑：它要验的正是这个 GUC 的效力。
#   别的套件在净场里被开成 on，不显式关掉就等于把被测对象先关掉了。
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = off;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t68b;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t68b(id int, v text);
SELECT create_distributed_table('t68b', 'id');
ALTER TABLE t68b SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t68b'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="t68b_${gid}"
echo "  shard=${gid} leader=:${pport} 有界追平=:${f1} 越界判别=:${f2}"
check "夹具齐" "$([[ -n "$gid" && -n "$pport" && -n "$f1" && -n "$f2" ]] && echo ok)" "ok"
check "trust_local_segments 已关（被测对象没被先关掉）" \
      "$(PSQL $f1 -Atc 'SHOW pg_partdist.replay_trust_local_segments' </dev/null)" "off"

nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册" "$nrels" "3"
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows"|cut -d, -f1|paste -sd,); ords=$(echo "$fsrows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$fsrows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$fsrows"|cut -d, -f4|paste -sd,)
rels=$(echo "$fsrows"|cut -d, -f5|paste -sd,)
SETLOC() { PSQL "$1" -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[], ${2}::bigint)" </dev/null 2>/dev/null | tail -1; }
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE t68b INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
  n=$(SETLOC $fp 0)
  check "follower :$fp 空表 base=0 配对" "$n" "3"
done
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null
done
PLSN()  { PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1; }
FOID()  { PSQL "$1" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1; }
RAFTB() { PSQL "$1" -Atc "SELECT partdist.get_follower_applied_part_lsn($(FOID $1))" </dev/null|tail -1; }
CATCH() { PSQL "$1" -Atc "SELECT partdist.replay_catchup($(FOID $1)::regclass, $2, 60000)" </dev/null 2>&1 | tail -1; }
APPLIED(){ PSQL "$1" -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=$(FOID $1)" </dev/null|tail -1; }

echo "================ [1] 第一批 20 行 ⇒ P1 ================"
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'a'||g FROM generate_series(1,20) g;" </dev/null >/dev/null
P1=$(PLSN)
a=$(CATCH $f1 "$P1")
check "follower1 追平到 P1=${P1}（applied=$a）" "$([[ -n "$a" && "$a" -ge "$P1" ]] && echo ok)" "ok"

echo "================ [2] 第二批 20 行 ⇒ P2，**不追** ================"
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'b'||g FROM generate_series(21,40) g;" </dev/null >/dev/null
P2=$(PLSN)
echo "  P1=${P1}  P2=${P2}"
# ★ 没有这条，后面全是空断言：上界之外必须**真的**还有字节。
check "★ 几何前提：P2 > P1（上界之外确实还有字节）" \
      "$([[ -n "$P1" && -n "$P2" && "$P2" -gt "$P1" ]] && echo ok)" "ok"
# f2 的本地段里此刻已经有 P2 那批字节（raft 已复制并提交），只是没人放。
b2=$(RAFTB $f2)
check "★ follower2 已**收到**到 P2（字节就在本地段里，唾手可得）" \
      "$([[ -n "$b2" && "$b2" -ge "$P2" ]] && echo ok)" "ok"

echo "================ [3] ★★ 给 P1 为上界 ⇒ 不许越界到 P2 ================"
a2=$(CATCH $f2 "$P1")
ap2=$(APPLIED $f2)
echo "  follower2 以 P1 为上界追平：返回=$a2  applied=$ap2"
check "★★ 达到了给定上界（applied >= P1）" \
      "$([[ -n "$ap2" && "$ap2" -ge "$P1" ]] && echo ok)" "ok"
check "★★ **没有**越界到本地段末尾（applied < P2）" \
      "$([[ -n "$ap2" && "$ap2" -lt "$P2" ]] && echo ok)" "ok"
# 数据层佐证：此刻副本内容应当与 leader **不**一致（少了第二批）
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
FPATH() {
  local fp=$1 fdata foid frel
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  foid=$(FOID "$fp")
  frel=$(PSQL "$fp" -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath(${foid}::regclass)" </dev/null|tail -1)
  echo "${fdata}/${frel}"
}
LMAIN=$(FPATH $pport)
cmp_mid=$(DEX python3 /tmp/pagecmp.py --kind=heap "$LMAIN" "$(FPATH $f2)" </dev/null 2>/dev/null)
check "★ 数据层佐证：此刻副本与 leader **不**一致（第二批确实没进去）" \
      "$([[ "$cmp_mid" != "IDENTICAL_OUTSIDE_HOLE" ]] && echo differs)" "differs"

echo "================ [4] ★ 判别：换成 P2 ⇒ 立刻放得进去 ================"
# 没有这条，第 3 条可能是因为"本地段里根本没数据"而通过 —— 那是假通过。
a3=$(CATCH $f2 "$P2")
ap3=$(APPLIED $f2)
echo "  follower2 以 P2 为上界追平：返回=$a3  applied=$ap3"
check "★★ 换上界即达到 P2（证明那些字节一直都在、能放）" \
      "$([[ -n "$ap3" && "$ap3" -ge "$P2" ]] && echo ok)" "ok"
cmp_end=$(DEX python3 /tmp/pagecmp.py --kind=heap "$LMAIN" "$(FPATH $f2)" </dev/null 2>/dev/null)
check "★★ 追满之后与 leader 逐字节一致" "$cmp_end" "IDENTICAL_OUTSIDE_HOLE"

echo "================ [5] ★★ 不给上界必须报错（fail-closed）================"
# 原本这里是**无条件**回退到"本地段末尾"，而门控它的 GUC 没有任何代码读。
noarg=$(PSQL $f1 -Atc "SELECT partdist.replay_catchup($(FOID $f1)::regclass)" </dev/null 2>&1 | tr '\n' ' ')
check "★★ trust=off 时不给上界 ⇒ ERROR" \
      "$([[ "$noarg" == *"必须显式给出回放上界"* ]] && echo closed)" "closed"
check "  报错说清了理由（不替调用方选最宽的上界）" \
      "$([[ "$noarg" == *"未达多数派"* ]] && echo ok)" "ok"
check "  提示指向 get_follower_applied_part_lsn（可执行的正解）" \
      "$([[ "$noarg" == *"get_follower_applied_part_lsn"* ]] && echo ok)" "ok"

PSQL $f1 -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
PSQL $f1 -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL $f1 -Atc 'SHOW pg_partdist.replay_trust_local_segments' </dev/null); [[ "$guc" == "on" ]] && break; sleep 1
done
check "  测试模式已打开" "$guc" "on"
ok1=$(PSQL $f1 -Atc "SELECT partdist.replay_catchup($(FOID $f1)::regclass)" </dev/null 2>&1 | tail -1)
check "★★ trust=on 时同一调用放行（GUC 确实在起作用，不是恒拦）" \
      "$([[ "$ok1" =~ ^[0-9]+$ ]] && echo ok)" "ok"
check "  放行后确实追到了本地段末尾（>= P2）" \
      "$([[ "$ok1" =~ ^[0-9]+$ && "$ok1" -ge "$P2" ]] && echo ok)" "ok"

echo "================ [6] 清理 ================"
for fp in $f1 $f2; do
  PSQL $fp -q -c "ALTER SYSTEM RESET pg_partdist.replay_trust_local_segments;" </dev/null >/dev/null 2>&1
  PSQL $fp -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}'::regclass)" </dev/null >/dev/null 2>&1
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl}" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t68b" </dev/null >/dev/null 2>&1
for p in $COORD $pport $f1 $f2; do
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" </dev/null >/dev/null 2>&1
done
check "清理完成" "ok" "ok"
health_check_no_crash && { echo "  PASS  本轮无节点崩溃（signal 11/6 或 PANIC）"; PASS=$((PASS+1)); } \
                     || { echo "  FAIL  本轮有节点崩溃"; FAIL=$((FAIL+1)); }
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
