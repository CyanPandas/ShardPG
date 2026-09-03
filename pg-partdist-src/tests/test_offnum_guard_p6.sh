#!/usr/bin/env bash
# [宿主机] T6.3a 验收：offnum 前置检查（TX_TSO_MVCC_DEV_PLAN.md §3.8 T6.3a）。
#
# R-P4-20 的收口处是内核 heapam.c 的三个
#     if (PageGetMaxOffsetNumber(page) + 1 < offnum) elog(PANIC, ...)
# —— **页存在、块号在界内、但页太短**。PANIC 不可捕获：回放本体的 PG_TRY
# 拦不住，postmaster 会整节点重置，随后再选举、再认领、再 PANIC，反复自噬
# （实测单轮 16–18 次）。
#
# 本套件的价值不在"修好了 R-P4-20"（那是 T6.1/T6.2 的事），而在证明：
#   ① 这种几何**真的能被造出来**（否则前面所有守卫都是在防一个想象中的东西）；
#   ② 造出来之后**节点不死**，只是该 shard 停摆；
#   ③ 现场被完整记下来（offnum / maxoff / rmid / blk / 游标 / 基线）；
#   ④ 提示指向的修复路径**确实能修好**（发基线 + 带 base 重配对 ⇒ 逐字节一致）。
#
# ★ 怎么造：给 follower 一份**旧的页 0**。
#   leader 写 10 行 → follower 追平 → 存下 follower 的页 0；
#   leader 再写到 60 行 → follower 追平；
#   把那份 10 行的页 0 盖回去 + 重启 follower（清掉缓冲区）；
#   leader 再写一行（offnum 61）→ 追平。
#   此刻 follower 的页里只有 10 个行指针，而记录要落在 61 —— 正是那三处判据。
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
exec 9>/tmp/t63a_offnum.lock
if ! flock -n 9; then echo "FATAL: 另一个 T6.3a 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "================ [0] 夹具：1 分片 + raft 组 + 1 follower ================"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t63a;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t63a(id int, v text);
SELECT create_distributed_table('t63a', 'id');
ALTER TABLE t63a SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t63a'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="t63a_${gid}"
echo "  shard=${gid} leader=:${pport} follower=:${f1}（将被喂旧页） 陪跑=:${f2}"
check "夹具齐" "$([[ -n "$gid" && -n "$pport" && -n "$f1" ]] && echo ok)" "ok"

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
CREATE TABLE ${shard_tbl} (LIKE t63a INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
  n=$(SETLOC $fp 0)
  check "follower :$fp 空表 base=0 配对" "$n" "3"
done
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
check "leader 认得本地分片" "$([[ -n "$LEADER_OID" && "$LEADER_OID" != "0" ]] && echo ok)" "ok"
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

FPATH() {
  local fp=$1 fdata foid frel
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1)
  frel=$(PSQL "$fp" -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath(${foid}::regclass)" </dev/null|tail -1)
  echo "${fdata}/${frel}"
}
PLSN() { PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1; }
CATCH() {
  local fp=$1 target=$2 foid
  [[ -n "$target" && "$target" -gt 0 ]] || { echo ""; return 1; }
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1)
  [[ -n "$foid" && "$foid" != "0" ]] || { echo ""; return 1; }
  PSQL "$fp" -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${target}, 60000)" </dev/null 2>/dev/null | tail -1
}

echo "================ [1] 写 10 行，存下 follower 的旧页 0 ================"
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'r'||g FROM generate_series(1,10) g;" </dev/null >/dev/null
p1=$(PLSN); a=$(CATCH $f1 "$p1")
check "follower 追平 10 行（applied=$a）" "$([[ -n "$a" && "$a" -ge "$p1" ]] && echo ok)" "ok"
F1MAIN=$(FPATH $f1)
DEX bash -c "dd if='$F1MAIN' of=/tmp/t63a_oldpage bs=8192 count=1 2>/dev/null" </dev/null
oldsz=$(DEX stat -c %s /tmp/t63a_oldpage </dev/null 2>/dev/null)
check "旧页 0 已存档（$oldsz 字节）" "$oldsz" "8192"

echo "================ [2] 写到 60 行，follower 追平 ================"
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g,'r'||g FROM generate_series(11,60) g;" </dev/null >/dev/null
p2=$(PLSN); a=$(CATCH $f1 "$p2")
check "follower 追平 60 行（applied=$a）" "$([[ -n "$a" && "$a" -ge "$p2" ]] && echo ok)" "ok"

echo "================ [2b] 先把 page0 的 FPI 用掉（复现的确定性前提） ================"
# ★ 为什么必须有这一步：checkpoint 之后**第一次**修改某页的记录会带整页 FPI，
#   而 FPI 是无条件 RestoreBlockImage —— 它会把 follower 那页整个修好，几何
#   就此消失，前置检查自然不跳闸。首版没管这件事，单跑时恰好没撞上 checkpoint
#   所以过了；排在别的套件后面跑时中间夹了一次 checkpoint，当场复现不出来
#   （19/5，五条红全是"没跳闸"）。
#   做法：显式 CHECKPOINT，再写一行把 page0 的 FPI 名额用掉，让 follower 追平；
#   此后对 page0 的记录都是普通记录，盖回旧页才盖得住。
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
PSQL $pport -q -c "INSERT INTO ${shard_tbl} VALUES (900,'fpi-absorber');" </dev/null >/dev/null
p2b=$(PLSN); a=$(CATCH $f1 "$p2b")
check "FPI 名额已用掉且 follower 追平（applied=$a）" \
      "$([[ -n "$a" && "$a" -ge "$p2b" ]] && echo ok)" "ok"

echo "================ [3] ★ 把旧页 0 盖回去 + 重启（造出 R-P4-20 的几何） ================"
DEX bash -c "dd if=/tmp/t63a_oldpage of='$F1MAIN' bs=8192 count=1 conv=notrunc 2>/dev/null" </dev/null
f1data=$(PSQL $f1 -Atc "SHOW data_directory" </dev/null)
DEX /work/pg-install/bin/pg_ctl -D "$f1data" -m immediate -l "$f1data/pg.log" restart -w -t 40 </dev/null >/dev/null 2>&1
for t in $(seq 1 40); do [[ "$(PSQL $f1 -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 1; done
check "follower 已重启（缓冲区已清）" "$(PSQL $f1 -Atc 'SELECT 1' </dev/null 2>/dev/null)" "1"
MARK=$(DEX bash -c "wc -l < '$f1data/pg.log'" </dev/null 2>/dev/null)   # 记下日志位置，只看之后的

echo "================ [4] 再写一行 ⇒ 记录要落 offnum 61，而页里只有 10 个 ================"
PSQL $f1 -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null 2>&1
PSQL $pport -q -c "INSERT INTO ${shard_tbl} VALUES (61,'trip');" </dev/null >/dev/null
p3=$(PLSN)
(CATCH $f1 "$p3") >/dev/null 2>&1
sleep 3

alive=$(PSQL $f1 -Atc "SELECT 1" </dev/null 2>/dev/null)
check "★★ 节点还活着（没有被 PANIC 带走）" "$alive" "1"
crash=$(DEX bash -c "tail -n +${MARK:-1} '$f1data/pg.log' 2>/dev/null | grep -ac 'PANIC:  invalid max offset number'" </dev/null 2>/dev/null)
check "★★ 日志里没有 invalid max offset number 的 PANIC" "${crash:-0}" "0"
reset=$(DEX bash -c "tail -n +${MARK:-1} '$f1data/pg.log' 2>/dev/null | grep -ac 'all server processes terminated'" </dev/null 2>/dev/null)
check "★★ 没有发生整节点重置" "${reset:-0}" "0"

trip=$(DEX bash -c "tail -n +${MARK:-1} '$f1data/pg.log' 2>/dev/null | grep -a 'R-P4-20\]: 页太短' | tail -1" </dev/null 2>/dev/null)
check "★ 前置检查跳闸并留痕" "$([[ -n "$trip" ]] && echo ok)" "ok"
[[ -n "$trip" ]] && echo "    ${trip##*: }"
# ★ 动作必须是**停摆**而不是跳过：跳过会让页面残缺、制造下一条
#   `PANIC: invalid lp`（实测正是这么崩的）。停摆 = 槽位 FAILED + 游标不推进。
f1oid=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1)
stt=$(PSQL $f1 -Atc "SELECT state FROM partdist.replay_status() WHERE shard=${f1oid}" </dev/null 2>/dev/null|tail -1)
check "★ 该分片停摆（state=failed，不是继续跑）" "$stt" "failed"
check "  现场带出 offnum 与 maxoff" \
      "$([[ "$trip" == *"offnum="* && "$trip" == *"maxoff="* ]] && echo ok)" "ok"
det=$(DEX bash -c "tail -n +${MARK:-1} '$f1data/pg.log' 2>/dev/null | grep -a '几何：rmid=' | tail -1" </dev/null 2>/dev/null)
check "  几何形状完整（rmid/blk/游标/基线）" \
      "$([[ "$det" == *"rmid="* && "$det" == *"游标="* && "$det" == *"基线="* ]] && echo ok)" "ok"
hint=$(DEX bash -c "tail -n +${MARK:-1} '$f1data/pg.log' 2>/dev/null | grep -ac 'shard_baseline_emit'" </dev/null 2>/dev/null)
check "  提示指向 shard_baseline_emit（可执行的修复路径）" \
      "$([[ "${hint:-0}" -ge 1 ]] && echo ok)" "ok"

echo "================ [5] ★ 按提示修复：发基线 + 带 base 重配对 ================"
base=$(PSQL $pport -Atc "SELECT partdist.shard_baseline_emit('${shard_tbl}'::regclass)" </dev/null 2>&1 | tail -1)
check "基线发射（base=$base）" "$([[ "$base" =~ ^[0-9]+$ && "$base" -gt 0 ]] && echo ok)" "ok"
PSQL $f1 -q -c "SELECT partdist.replay_disable('${shard_tbl}'::regclass);" </dev/null >/dev/null 2>&1
n=$(SETLOC $f1 "$base")
check "带 base=$base 重新配对" "$n" "3"
PSQL $f1 -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null 2>&1
p4=$(PLSN); a=$(CATCH $f1 "$p4")
check "修复后追平（applied=$a）" "$([[ -n "$a" && "$a" -ge "$p4" ]] && echo ok)" "ok"

PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
LMAIN=$(FPATH $pport)
cmpv=$(DEX python3 /tmp/pagecmp.py --kind=heap "$LMAIN" "$(FPATH $f1)" </dev/null 2>/dev/null)
check "★★ 修复后与 leader 逐字节一致" "$cmpv" "IDENTICAL_OUTSIDE_HOLE"

echo "================ [6] 清理 ================"
DEX rm -f /tmp/t63a_oldpage </dev/null
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}'::regclass);" </dev/null >/dev/null 2>&1
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t63a;" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"
health_check_no_crash || true
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]]
