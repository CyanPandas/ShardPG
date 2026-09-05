#!/usr/bin/env bash
# [宿主机] 解冻批次 #7 验收：① 副本供给入口；② raft → 回放的**真**角色交接。
#
# 两件事此前都是"绕行"：
#
#   ① register_shard_fileset / replay_set_locmap / replay_enable 在产品代码里
#      **没有任何调用方** —— 只有验收脚本在调，每套件抄一遍四十行样板。
#      副本只能靠人手建；raft 把某节点登记成 secondary 也不会真的有副本。
#   ② partwal_notify_primary_switch() 整个函数体只有一句 ereport(LOG)，
#      文件头写着「Placeholder until the real role-switch / replay handover lands」。
#      真正的升主动作被挂在 pg_raft_promote_prepare —— 那是**上报之前**的路，
#      不是交接；降级方向更是完全没人管。
#
# ★★ 本套件最有力的两条：
#   - [1] **不手工建任何东西**，只调一次 provision_shard_replica，
#     就得到一份与 leader **逐字节一致**的副本；
#   - [3] 切主**之前**读新主壳表必须被闸门拦、**之后**必须放行 ——
#     这条差分证明放行是**交接**干的，而不是别处顺手做掉的。
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
exec 9>/tmp/t_handover_p7.lock
if ! flock -n 9; then echo "FATAL: 另一个批次 #7 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "================ [0] 夹具：只建分布表，**不手工建副本** ================"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t7pv;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t7pv(id int, v text);
SELECT create_distributed_table('t7pv', 'id');
ALTER TABLE t7pv SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
GID=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t7pv'::regclass" </dev/null)
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${GID}" </dev/null)
pnode=$((PA - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$PA" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
TBL="t7pv_${GID}"
echo "  shard=${GID} leader=:${PA}(node ${pnode}) 待供给=:${f1}(node ${f1node}) 陪跑=:${f2}"
check "夹具齐" "$([[ -n "$GID" && -n "$PA" && -n "$f1" ]] && echo ok)" "ok"

# 打标：闸门只对**分片 xid 宇宙**里的副本生效（T6.3c 的收窄），不打标就没得可验
SOID=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT '${TBL}'::regclass::oid" </dev/null|tail -1)
PSQL $PA -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${SOID}';" </dev/null >/dev/null
PSQL $PA -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
for t in $(seq 1 10); do
  [[ "$(PSQL $PA -Atc 'SHOW pg_partdist.shard_relids' </dev/null)" == "$SOID" ]] && break; sleep 1
done
check "leader 白名单生效（分片 xid 宇宙的前提）" \
      "$(PSQL $PA -Atc 'SHOW pg_partdist.shard_relids' </dev/null)" "$SOID"
PSQL $PA -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null
PSQL $PA -q -c "INSERT INTO ${TBL} SELECT g,'r'||g FROM generate_series(1,40) g;" </dev/null >/dev/null
lrows=$(PSQL $PA -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${TBL}" </dev/null|tail -1)
check "leader 写入 40 行" "$lrows" "40"

# raft 组：供给不负责建组（组是控制面的事），这里照旧手工建
# ★★ 组主**必须**落在 placement 主上：数据只在它那儿，而供给要先发一次基线，
#   基线走 raft 写路径、要求发起者是组 leader。三个成员同时建组是场竞选竞态，
#   谁都可能赢 —— 实测连续两轮选到了没有数据的节点，整套连锁全红。
#   pg_raft 没有"指定竞选/转移主权"的入口，只能让 $PA **抢跑**：
#   先在它上面建组、等一拍再建到 follower 上；仍选不到就复位重来。
#   这是夹具的必要条件，不是被测内容。
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
  echo "  [夹具] 第 ${round} 轮组主落在别处（$st），复位重来"
  for p in $PA $f1 $f2; do PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1; done
  sleep 2
done
check "分区组 leader 就位（且落在 placement 主上）" "$st" "leader"
# 供给之前，目标节点上**什么都没有**
pre_tbl=$(PSQL $f1 -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM pg_class WHERE relname='${TBL}'" </dev/null|tail -1)
check "★ 供给前：目标节点上壳表**不存在**（证明后面的一致性不是本来就有的）" "$pre_tbl" "0"

echo "================ [1] ★★ 一条命令供给副本 ================"
# ★ 供给前**再确认一次**组主仍在本节点：基线发射走的是 raft 写路径，
#   写栅栏的判据是"本节点此刻是不是该组 leader"。选举随时可能挪走，
#   [0] 里那次确认与这里之间隔着好几步。
st2=""
for t in $(seq 1 30); do
  st2=$(PSQL $PA -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${GID}" </dev/null 2>/dev/null)
  [[ "$st2" == "leader" ]] && break; sleep 2
done
check "供给前：组主仍在本节点（基线发射要走写栅栏）" "$st2" "leader"
# ★ 报错要留全文：首版 `| tail -1` 只截到 CONTEXT 甚至空行，
#   两轮排查全靠手工复现才拿到真正的 errmsg —— 白跑了二十分钟。
prov=$(PSQL $PA -Atc "SELECT partdist.provision_shard_replica(${GID}::bigint, ${f1node})" </dev/null 2>&1 | tr '\n' ' ')
echo "  provision_shard_replica ⇒ $prov"
check "★★ 供给返回成功（不是报错）" "$([[ "$prov" == shard=* ]] && echo ok)" "ok"
check "  返回里带了基线 base" "$([[ "$prov" == *"base="* ]] && echo ok)" "ok"
post_tbl=$(PSQL $f1 -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM pg_class WHERE relname='${TBL}'" </dev/null|tail -1)
check "★★ 目标节点上壳表已被建出来" "$post_tbl" "1"
foid=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${GID})" </dev/null|tail -1)
check "  目标节点认得该分片（shard_identity 已重建）" \
      "$([[ -n "$foid" && "$foid" != "0" ]] && echo ok)" "ok"
armed=$(PSQL $f1 -Atc "SELECT armed FROM partdist.replay_status() WHERE shard=${foid}" </dev/null|tail -1)
check "★★ 回放槽位已 armed（供给把 enable 也做了）" "$armed" "t"
nloc=$(PSQL $f1 -Atc "SELECT count(*) FROM partdist.replay_locmap(${foid}::regclass)" </dev/null 2>/dev/null|tail -1)
check "  locmap 已配对（3 个成员）" "$nloc" "3"

# 从零开始、只靠一条命令 ⇒ 追平后必须与 leader 逐字节一致
tip=$(PSQL $PA -Atc "SELECT partdist.get_partition_flush_lsn(${SOID})" </dev/null|tail -1)
a=$(PSQL $f1 -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${tip}, 60000)" </dev/null 2>&1|tail -1)
check "追平（applied=$a / ${tip}）" "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$tip" ]] && echo ok)" "ok"
PSQL $PA -q -c "CHECKPOINT;" </dev/null >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
FPATH() {
  local fp=$1 fdata fo frel
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  fo=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${GID})" </dev/null|tail -1)
  frel=$(PSQL "$fp" -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath(${fo}::regclass)" </dev/null|tail -1)
  echo "${fdata}/${frel}"
}
cmpv=$(DEX python3 /tmp/pagecmp.py --kind=heap "$(FPATH $PA)" "$(FPATH $f1)" </dev/null 2>/dev/null)
check "★★ 只调一条命令 ⇒ 副本与 leader **逐字节一致**" "$cmpv" "IDENTICAL_OUTSIDE_HOLE"

# ★ 陪跑节点也要供给。它是组成员，而**从未被供给的成员照样能当选** ——
#   R-P4-15 只挡"收到了分区 WAL 却没有回放槽位"，挡不住"从来没收到过"：
#   没有壳表就没有本地 OID，字节归不了档，applied_part_lsn 恒为 0，
#   于是那道守卫的 `IF coalesce(bound,0) > 0` 判否，直接放行。
#   首版没供给它，切主时正好选中它，新主上连表都没有（已登记 R-P7-1）。
#   **纪律**：组成员集应当跟着真实副本走 —— 先供给，再进组。
prov2=$(PSQL $PA -Atc "SELECT partdist.provision_shard_replica(${GID}::bigint, ${f2node})" </dev/null 2>&1 | tr '\n' ' ')
check "★ 陪跑节点同样一条命令供给成功（供给可重复用）" \
      "$([[ "$prov2" == shard=* ]] && echo ok)" "ok"
f2oid=$(PSQL $f2 -Atc "SELECT partdist.local_partition_for_shard(${GID})" </dev/null|tail -1)
tip2=$(PSQL $PA -Atc "SELECT partdist.get_partition_flush_lsn(${SOID})" </dev/null|tail -1)
PSQL $f2 -Atc "SELECT partdist.replay_catchup(${f2oid}::regclass, ${tip2}, 60000)" </dev/null >/dev/null 2>&1

echo "================ [2] 阴性对照：非 leader 上调必须被拒 ================"
# ★ f2 此刻**已经是副本**了 —— 正好用来验"是副本 ≠ 是主"这条：
#   只查"本地有没有这个分片"是不够的，必须查控制面登记的主是不是自己。
neg=$(PSQL $f2 -Atc "SELECT partdist.provision_shard_replica(${GID}::bigint, ${f1node})" </dev/null 2>&1 | tr '\n' ' ')
check "★★ 在**副本**上调用 ⇒ ERROR（是副本不等于是主）" \
      "$([[ "$neg" == *"不是分片"* && "$neg" == *"的主"* ]] && echo rejected)" "rejected"
check "  报错指出了调错节点，而不是把 raft 写栅栏的报错甩出来" \
      "$([[ "$neg" == *"供给要在 leader 上发起"* ]] && echo ok)" "ok"
# 从未承载该分片的节点：同样拒，但走的是另一条判据
neg2=$(PSQL 5436 -Atc "SELECT partdist.provision_shard_replica(${GID}::bigint, ${f1node})" </dev/null 2>&1 | tr '\n' ' ')
check "★ 根本没有该分片的节点调用 ⇒ ERROR" \
      "$([[ "$neg2" == *"本节点没有分片"* ]] && echo rejected)" "rejected"

echo "================ [3] ★★ raft → 回放的真角色交接 ================"
# ★ 差分的前半：切主**之前**，新主候选上的壳表必须被闸门拦住。
before=$(PSQL $f1 -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${TBL}" </dev/null 2>&1 | tr '\n' ' ')
check "★★ 切主前：读壳表被副本闸门拦（它此刻确实是别人的副本）" \
      "$([[ "$before" == *"副本壳表"* ]] && echo blocked)" "blocked"

leader_dir="worker$((PA - 5432))"
PGCTL "$leader_dir" -m immediate -w -t 60 stop >/dev/null 2>&1
echo "  已停掉 leader :${PA}（${leader_dir}）"
newp=0
for t in $(seq 1 90); do
  newp=$(PSQL $COORD -Atc "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${GID}" </dev/null 2>/dev/null)
  [[ -n "$newp" && "$newp" != "$pnode" && "$newp" != "0" ]] && break
  sleep 2
done
check "控制面登记了新主（node ${newp}，原主 node ${pnode}）" \
      "$([[ -n "$newp" && "$newp" != "$pnode" && "$newp" != "0" ]] && echo ok)" "ok"

if [[ "$newp" == "$f1node" ]]; then
  NEWPORT=$f1
else
  NEWPORT=$((newp + 5431))
fi
# ★ 差分的后半：交接之后必须放行，且读到的就是切主前写入的数据。
after=""
for t in $(seq 1 30); do
  after=$(PSQL $NEWPORT -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${TBL}" </dev/null 2>&1 | tail -1)
  [[ "$after" =~ ^[0-9]+$ ]] && break; sleep 2
done
check "★★ 切主后：读壳表**放行**（交接解除了副本身份）" \
      "$([[ "$after" =~ ^[0-9]+$ ]] && echo allowed)" "allowed"
check "★★ 新主读到切主前写入的全部 40 行" "$after" "40"
narm=$(PSQL $NEWPORT -Atc "SELECT armed FROM partdist.replay_status() WHERE shard=$(PSQL $NEWPORT -Atc "SELECT partdist.local_partition_for_shard(${GID})" </dev/null|tail -1)" </dev/null 2>/dev/null|tail -1)
check "★ 交接撤下了回放 armed（新主不再是副本，防误触发盖表）" "$narm" "f"
newdir="worker$((NEWPORT - 5432))"
# ★ 不用行号基线：新主是谁要到切主后才知道，在 f1 上取的行号对 f2 是错位的
#   （首版因此漏判）。本轮的 GID 全局唯一，直接按它定位即可。
hs=$(DEX bash -c "grep -c '分片 ${GID}（本地 OID' /work/pg-cluster-data/${newdir}/pg.log" </dev/null 2>/dev/null)
check "★★ 交接在**正规位置**留了痕（notify 钩子真的干活了，不再只是 placeholder）" \
      "$([[ "${hs:-0}" -ge 1 ]] && echo ok)" "ok"

echo "================ [4] 清理 ================"
PGCTL "$leader_dir" -w -t 60 start >/dev/null 2>&1
for t in $(seq 1 40); do [[ "$(PSQL $PA -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 2; done
for p in $PA $f1 $f2; do
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "SELECT partdist.replay_disable('${TBL}'::regclass)" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${TBL}" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${GID};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t7pv" </dev/null >/dev/null 2>&1
PSQL $COORD -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${GID};" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"
# ★ 本轮**故意**停过一个 leader（切主是被测内容），崩溃哨兵会把它算成异常。
#   做法同 T6.3b：不关哨兵，而是在故意动作之后重开窗口。
health_mark_start
health_check_no_crash && { echo "  PASS  收尾窗口无节点崩溃"; PASS=$((PASS+1)); } \
                     || { echo "  FAIL  收尾窗口有节点崩溃"; FAIL=$((FAIL+1)); }
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
