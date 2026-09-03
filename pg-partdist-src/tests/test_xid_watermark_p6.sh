#!/usr/bin/env bash
# [宿主机] T6.5 验收：§5.5 水位新语义（DEV PLAN §3.8 T6.5）。
#
# FRD §7.5 的原规则："follower 回放推进**原生 nextXid** 越过流内一切 xid"。
# 理由是 R1/R2 时代副本元组身上带的就是 **leader 的原生 xid** —— 本地若把同一
# 个号再发一次，两笔互不相干的事务会共用一个查账键。
#
# TX-TSO-MVCC 之后这个理由**在打标分片上不再成立**：元组 xmin/xmax 里写的是
# 分片 xid（每分片独立宇宙），本地原生 nextXid 发到哪儿与它们毫无关系。
# 真正需要"永不重号"的是分片分配器，而它的水位已由 U-P5-1 之二经 MARKER 交接。
#
# 设计 §5.5 因此把规则改成"推进本分片分配器"，并点明副产品：
# **#40（§13 约束 4）的原生 clog 逐页补齐不再被回放触发**。
#
# ★ 本套件要证明三件事：
#   ① 打标流：follower 的原生 nextXid **不再被 leader 拖着走**；
#   ② 同时分片分配器水位**确实在动**（否则就是"两边都没做"的假通过）；
#   ③ 遗留流（未打标）**原样保留**老行为 —— 那条规则对它们仍然必需。
set -u
CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
PSQLO() { local port=$1 opts=$2; shift 2
  docker exec -i -u postgres -e HOME=/var/lib/postgresql -e PGOPTIONS="$opts" \
    "$CONTAINER" /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
NOPROP='-c citus.enable_ddl_propagation=off'
VISIBLE='-c citus.override_table_visibility=false'
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/t65_xidwm.lock
if ! flock -n 9; then echo "FATAL: 另一个 T6.5 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

# ★ 读 nextXid 而**不消耗**一个 xid：txid_current() 会分配号，用它当探针会
#   把被测量本身推着走。快照的 xmax 就是 nextXid，只读不分配。
NEXTXID() { PSQL "$1" -Atc "SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint" </dev/null | tail -1; }

echo "================ [0] 夹具：1 分片 + raft 组 + 2 follower ================"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t65wm;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t65wm(id int, v text);
SELECT create_distributed_table('t65wm', 'id');
ALTER TABLE t65wm SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t65wm'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431)); f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431)); shard_tbl="t65wm_${gid}"
echo "  shard=${gid} leader=:${pport} follower=:${f1} 陪跑=:${f2}"
check "夹具齐" "$([[ -n "$gid" && -n "$pport" && -n "$f1" ]] && echo ok)" "ok"
nrels=$(PSQLO $pport "$VISIBLE" -Atc "SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册" "$nrels" "3"
fsrows=$(PSQLO $pport "$VISIBLE" -Atc "SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows"|cut -d, -f1|paste -sd,); ords=$(echo "$fsrows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$fsrows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$fsrows"|cut -d, -f4|paste -sd,)
rels=$(echo "$fsrows"|cut -d, -f5|paste -sd,)
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE t65wm INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
  n=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[], 0::bigint)" </dev/null 2>/dev/null | tail -1)
  check "follower :$fp 配对" "$n" "3"
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
foid=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1)
PLSN() { PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1; }
CATCH() { local t=$1; PSQL $f1 -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${t}, 60000)" </dev/null 2>/dev/null|tail -1; }

# ★★ 判据改用"分支是否被走到"，不用 nextXid 的量级。
#   首版拿 leader/follower 的 nextXid 大小关系做判据，实测**根本分不出来**：
#   这台 follower 被历轮测试烧掉大量原生号，nextXid（177994）本来就远高于
#   leader（165577），于是 PartDistAdvanceNextXidPastXid 无论走不走都是 no-op，
#   两种行为在计数器上**完全同形**。连"遗留流"那条 PASS 也是为了错误的理由
#   通过的 —— 这是本套件最值得记的一条：**测量量必须能区分被测的两种行为**。
PSQL $f1 -q -c "ALTER SYSTEM SET pg_partdist.replay_debug_trace = on;" </dev/null >/dev/null
PSQL $f1 -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
f1data=$(PSQL $f1 -Atc "SHOW data_directory" </dev/null)
TRACE_N() { DEX bash -c "grep -ac '在分片 xid 宇宙，跳过原生 nextXid 推进' '$f1data/pg.log' 2>/dev/null || true" </dev/null; }

echo "================ [1] ★ 遗留流：未打标 ⇒ 老规则原样保留 ================"
# 此刻 leader 还没打标，元组带的是**原生 xid**，那条规则对它仍然必需。
t_before=$(TRACE_N)
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'legacy'||g FROM generate_series(1,50) g;" </dev/null >/dev/null
p1=$(PLSN); a=$(CATCH "$p1")
check "遗留流追平（applied=$a）" "$([[ -n "$a" && "$a" -ge "$p1" ]] && echo ok)" "ok"
t_after_legacy=$(TRACE_N)
echo "  跳过标记计数：$t_before → $t_after_legacy"
check "★ 遗留流：**没有**走跳过分支（老规则仍生效）" "$t_after_legacy" "$t_before"
wm0=$(PSQL $f1 -Atc "SELECT partdist.shard_xid_next(${foid}::oid)" </dev/null 2>/dev/null|tail -1)
check "  此时分片分配器还没水位（未打标）" "$wm0" "0"

echo "================ [2] 打标 ⇒ 进入分片 xid 宇宙 ================"
SOID=$(PSQLO $pport "$VISIBLE" -Atc "SELECT '${shard_tbl}'::regclass::oid" </dev/null|tail -1)
PSQL $pport -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${SOID}';" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL $pport -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$SOID" ]] && break; sleep 1
done
check "leader 白名单生效" "$guc" "$SOID"
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'marked'||g FROM generate_series(100,140) g;" </dev/null >/dev/null
p2=$(PLSN); a=$(CATCH "$p2")
check "打标后追平（applied=$a）" "$([[ -n "$a" && "$a" -ge "$p2" ]] && echo ok)" "ok"
wm1=$(PSQL $f1 -Atc "SELECT partdist.shard_xid_next(${foid}::oid)" </dev/null 2>/dev/null|tail -1)
echo "  follower 分片发号水位：$wm0 → $wm1"
check "★ 分片分配器水位确实在动（否则就是两边都没做的假通过）" \
      "$([[ -n "$wm1" && "$wm1" -gt 0 ]] && echo ok)" "ok"

echo "================ [3] ★★ 新宇宙：走的是跳过分支 ================"
t_base=$(TRACE_N)
for k in 1 2 3; do
  PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'burn${k}-'||g FROM generate_series($((200+k*50)),$((240+k*50))) g;" </dev/null >/dev/null
done
p3=$(PLSN); a=$(CATCH "$p3")
check "新宇宙追平（applied=$a）" "$([[ -n "$a" && "$a" -ge "$p3" ]] && echo ok)" "ok"
t_after=$(TRACE_N)
echo "  跳过标记计数：$t_base → $t_after"
check "★★ 新宇宙：确实走了跳过分支（§5.5 生效）" \
      "$([[ -n "$t_after" && -n "$t_base" && "$t_after" -gt "$t_base" ]] && echo ok)" "ok"
PSQL $f1 -q -c "ALTER SYSTEM RESET pg_partdist.replay_debug_trace;" </dev/null >/dev/null
PSQL $f1 -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null

echo "================ [4] 副产品：不再触发原生 clog 逐页补齐 ================"
# §5.5 原文："副产品：#40（§13 约束 4）的原生 clog 逐页补齐不再被回放触发"。
# 判据取"follower 的 pg_xact 没有因回放而暴涨"——补页正是它变大的原因。
nseg=$(DEX bash -c "ls '$f1data/pg_xact' 2>/dev/null | wc -l" </dev/null)
echo "  follower pg_xact 段数=$nseg"
check "follower 的 pg_xact 未被回放撑大（段数 <= 4）" \
      "$([[ -n "$nseg" && "$nseg" -le 4 ]] && echo ok)" "ok"

echo "================ [5] 数据仍然正确（迁移不能以正确性换开销） ================"
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
lrel=$(PSQLO $pport "$VISIBLE" -Atc "SELECT pg_relation_filepath(${LEADER_OID}::regclass)" </dev/null|tail -1)
ldata=$(PSQL $pport -Atc "SHOW data_directory" </dev/null)
frel=$(PSQLO $f1 "$NOPROP $VISIBLE" -Atc "SELECT pg_relation_filepath(${foid}::regclass)" </dev/null|tail -1)
cmpv=$(DEX python3 /tmp/pagecmp.py --kind=heap "${ldata}/${lrel}" "${f1data}/${frel}" </dev/null 2>/dev/null)
check "★ 副本与 leader 仍逐字节一致" "$cmpv" "IDENTICAL_OUTSIDE_HOLE"

echo "================ [6] 清理 ================"
PSQL $pport -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}'::regclass);" </dev/null >/dev/null 2>&1
  PSQLO $fp "$NOPROP" -q -c "DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t65wm;" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"
health_check_no_crash || true
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]]
