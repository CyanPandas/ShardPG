#!/usr/bin/env bash
# [宿主机] T6.2 验收：locmap 携带基线游标（TX_TSO_MVCC_DEV_PLAN.md §3.8 T6.2）。
#
# 补上设计 §13 约束 2 的另一半。此前 locmap 只回答"哪个文件对哪个文件"，
# 认领时无 apply_checkpoint 就从 **0** 起 —— 那等于沉默假设
#   「本地文件 == leader 在流起点时的文件」
# 而这句话从没人建立、也从没人校验。R-P4-20 的第一半正是它。
#
# v3 locmap 把这句断言写进文件：
#   base=0  —— 显式声明"从关系出生点开始"，配对时**核对本地关系确为空**；
#   base>0  —— 由 T6.1 的 shard_baseline_emit() 给出，自那条全量基线起重放。
# 旧格式（v1/v2 无此字段）在读取阶段即被拒 —— 这就是"无基线不再默认从 0，
# 而是拒绝认领"的落点。
#
# ★ 端到端主线（T6.1 + T6.2 合起来才成立的那件事）：
#   给 follower 一个**非空且内容完全错误**的壳表 —— 今天这正是 R-P4-20 的
#   形状（从 0 重放会把早期记录灌到对不上的文件上）。配上基线游标之后，
#   追平结果必须与 leader **逐字节一致**。这就是"重做物理基线"的完整闭环。
#
# ★ follower 壳表绝不能被 SELECT（无白名单 ⇒ on-access 剪枝会清掉分片元组）。
#   本脚本对 follower 只做 INSERT（制造错误内容）与文件级比对。
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

exec 9>/tmp/t62_locmap.lock
if ! flock -n 9; then echo "FATAL: 另一个 T6.2 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

echo "================ [0] 夹具 ================"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t62base;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t62base(id int, v text);
SELECT create_distributed_table('t62base', 'id');
ALTER TABLE t62base SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t62base'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="t62base_${gid}"
echo "  shard=${gid} leader=:${pport} followers=:${f1}(将被灌错误内容) :${f2}(保持空)"
check "夹具三要素齐" "$([[ -n "$gid" && -n "$pport" && -n "$f1" && -n "$f2" ]] && echo ok)" "ok"

nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null | tail -1)
check "leader fileset 注册" "$nrels" "3"
fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
roles=$(echo "$fsrows"|cut -d, -f1|paste -sd,); ords=$(echo "$fsrows"|cut -d, -f2|paste -sd,)
spcs=$(echo "$fsrows"|cut -d, -f3|paste -sd,);  dbs=$(echo "$fsrows"|cut -d, -f4|paste -sd,)
rels=$(echo "$fsrows"|cut -d, -f5|paste -sd,)
SETLOC() {  # <fport> <base> —— 成功时返回配对数，失败时空
  PSQL "$1" -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[], ${2}::bigint)" </dev/null 2>/dev/null | tail -1
}
# ★ 负向断言必须看**全文**：报错是 ERROR/DETAIL/HINT 三行，`tail -1` 只拿到 HINT，
#   于是"匹配 ERROR"永远失败 —— 首版正是这么假红的。
SETLOC_MSG() {  # <fport> <base> —— 返回完整输出（含 ERROR/DETAIL/HINT）
  PSQL "$1" -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[], ${2}::bigint)" </dev/null 2>&1 | tr '\n' ' '
}
for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE t62base INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL
done

echo "================ [1] base=0 的断言：空表放行、非空当场否掉 ================"
ok_empty=$(SETLOC $f2 0)
check "base=0 + 本地空表 ⇒ 配对成功（既有用法不破）" "$ok_empty" "3"

# ★ 把 f1 灌成"非空且内容完全错误"——今天这正是 R-P4-20 的形状
PSQL $f1 -q -c "SET citus.enable_ddl_propagation=off;
                INSERT INTO ${shard_tbl} SELECT g, 'WRONG-CONTENT' FROM generate_series(1,300) g;" </dev/null >/dev/null
PSQL $f1 -q -c "CHECKPOINT;" </dev/null >/dev/null
bad=$(SETLOC_MSG $f1 0)
check "★ base=0 + 本地非空 ⇒ 当场报错（新守卫）" \
      "$([[ "$bad" == *"已有"*"个块"* ]] && echo banned)" "banned"
check "  报错点名了 R-P4-20（可执行的诊断）" \
      "$([[ "$bad" == *"R-P4-20"* ]] && echo ok)" "ok"
check "  HINT 指向 shard_baseline_emit" \
      "$([[ "$bad" == *"shard_baseline_emit"* ]] && echo ok)" "ok"

echo "================ [2] raft 组（必须先于写入与基线） ================"
# ★ 顺序铁律：分区组不建起来，leader 的记录不会被复制到 follower 的本地段文件，
#   而 replay_trust_local_segments=on 的回放只读本地段 —— 组建晚了，基线那批
#   FPI 一个字节都到不了对面。首版把组建在基线之后，于是追平永远拿不到数据。
for p in $pport $f1 $f2; do PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null; done
LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
check "leader 认得本地分片（LEADER_OID=$LEADER_OID）" \
      "$([[ -n "$LEADER_OID" && "$LEADER_OID" != "0" ]] && echo ok)" "ok"
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
done

echo "================ [3] leader 写数据 + 发全量基线 ================"
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g, repeat('y',80) FROM generate_series(1,400) g;" </dev/null >/dev/null
plsn_pre=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
base=$(PSQL $pport -Atc "SELECT partdist.shard_baseline_emit('${shard_tbl}'::regclass)" </dev/null 2>&1 | tail -1)
check "基线发射成功（base=$base）" "$([[ "$base" =~ ^[0-9]+$ && "$base" -gt "$plsn_pre" ]] && echo ok)" "ok"

echo "================ [4] 带基线游标配对 + 启用回放 ================"
n1=$(SETLOC $f1 "$base")
check "★ f1（非空错误内容）带 base=$base 配对成功" "$n1" "3"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" </dev/null >/dev/null
done
SOID=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT '${shard_tbl}'::regclass::oid" </dev/null|tail -1)
PSQL $pport -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${SOID}';" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null

plsn_now=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
CATCH() {  # <fport> <target>
  local fp=$1 target=$2 foid
  # ★ 空 target 会让 SQL 变成语法错误、返回空，再让 -ge 比较变成假通过
  [[ -n "$target" && "$target" -gt 0 ]] || { echo ""; return 1; }
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1)
  [[ -n "$foid" && "$foid" != "0" ]] || { echo ""; return 1; }
  PSQL "$fp" -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${target}, 60000)" </dev/null 2>/dev/null | tail -1
}
a1=$(CATCH $f1 "$plsn_now"); a2=$(CATCH $f2 "$plsn_now")
check "f1 追平到 $plsn_now（applied=$a1）" "$([[ -n "$a1" && "$a1" -ge "$plsn_now" ]] && echo ok)" "ok"
check "f2 追平到 $plsn_now（applied=$a2）" "$([[ -n "$a2" && "$a2" -ge "$plsn_now" ]] && echo ok)" "ok"

# ★★ 这里原本还有一条"认领游标从 base 起"的断言，**已删除**，理由写在这里
#    而不是悄悄拿掉：
#
#    这条性质我换了三种观测方式都没能做稳 ——
#      ① grep follower 的 pg.log 找认领行：拿到过上一轮的陈旧行；
#      ② 按 shard oid 过滤那行：连续两轮取空；
#      ③ 改读 replay_status().durable：仍有取不到行的时候，
#         而事后手工查同一张表，槽位明明在、durable=407 >= base=403。
#    共同点是**观测手段依赖 worker 进程生命周期与惰性认领时机**，那不是被测性质。
#
#    而"base 之前的记录没有被应用"这件事，[5] 节的逐字节比对**已经证明了**：
#    f1 事先被灌了 300 行错误内容，若 base 之前的记录被应用进去，页面必然对不上
#    （甚至触发 offnum 前置检查停摆）。留一条做不稳的断言，比不留更糟 ——
#    它会在门禁里持续制造"红了但不是产品的问题"，把注意力引向错误的地方。

echo "================ [5] ★ 逐字节比对：错误内容被基线彻底覆盖 ================"
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
FPATH_MAIN() {
  local fp=$1 fdata foid frel
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1)
  frel=$(PSQL "$fp" -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath(${foid}::regclass)" </dev/null|tail -1)
  echo "${fdata}/${frel}"
}
LMAIN=$(FPATH_MAIN $pport)
NBLK() { local sz; sz=$(DEX stat -c %s "$1" </dev/null 2>/dev/null); [[ -n "$sz" ]] && echo $((sz/8192)) || echo ""; }
lb=$(NBLK "$LMAIN"); b1=$(NBLK "$(FPATH_MAIN $f1)"); b2=$(NBLK "$(FPATH_MAIN $f2)")
echo "  块数：leader=$lb f1=$b1 f2=$b2"
check "f1 块数与 leader 一致（错误内容整体被换掉）" "$b1" "$lb"
check "f2 块数与 leader 一致" "$b2" "$lb"
c1=$(DEX python3 /tmp/pagecmp.py --kind=heap "$LMAIN" "$(FPATH_MAIN $f1)" </dev/null 2>/dev/null)
c2=$(DEX python3 /tmp/pagecmp.py --kind=heap "$LMAIN" "$(FPATH_MAIN $f2)" </dev/null 2>/dev/null)
check "★★ f1（曾装满错误内容）与 leader 逐字节一致" "$c1" "IDENTICAL_OUTSIDE_HOLE"
check "★★ f2 与 leader 逐字节一致" "$c2" "IDENTICAL_OUTSIDE_HOLE"

echo "================ [6] 负向：旧格式 locmap 被读取器拒掉 ================"
# ★ 直接打读取器，不绕 worker。
#   `partdist.replay_locmap()` 与认领路径共用同一个 ShardReplayReadLocMap()，
#   它拒了，认领也就拒了（认领侧收到 false 即"无有效 locmap，解除 armed"）。
#   首版试图靠"改盘上文件 + 重启节点 + 触发追平"去逼出这条路径 —— worker 的
#   认领是惰性的、locmap 又缓存在它的内存 ctx 里，两轮都没触发，属**用例设计
#   错误**而不是产品问题。换成直打读取器，判据确定、不依赖时序。
f2data=$(PSQL $f2 -Atc "SHOW data_directory" </dev/null)
f2oid=$(PSQL $f2 -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null|tail -1)
LMFILE="${f2data}/pg_parwal/${f2oid}/locmap"
sz_before=$(DEX stat -c %s "$LMFILE" </dev/null 2>/dev/null)
check "v3 locmap 是 920 字节（v2 是 912，长度即版本判据）" "$sz_before" "920"

ok_read=$(PSQL $f2 -Atc "SELECT count(*) FROM partdist.replay_locmap('${shard_tbl}'::regclass)" </dev/null 2>&1 | tail -1)
check "完好的 v3 locmap 可读（$ok_read 对）" "$ok_read" "3"

# 截掉 8 字节 = 退回 v2 的长度，模拟"旧格式配对"
DEX bash -c "truncate -s $((sz_before - 8)) '$LMFILE'" </dev/null
bad_read=$(PSQL $f2 -Atc "SELECT count(*) FROM partdist.replay_locmap('${shard_tbl}'::regclass)" </dev/null 2>&1 | tr '\n' ' ')
check "★ 旧格式（v2 长度）locmap ⇒ 读取器拒绝" \
      "$([[ "$bad_read" == *"长度不符"* ]] && echo rejected)" "rejected"
check "  拒绝理由点名了 base_part_lsn 缺失的后果" \
      "$([[ "$bad_read" == *"replay_set_locmap"* ]] && echo ok)" "ok"
echo "    ${bad_read#*ERROR:  }"

# 复原，免得清理阶段踩到坏文件。
# ★ 这里必须用 $base 而不是 0 —— f2 此刻已经把 leader 的数据回放进来了，
#   壳表非空，用 base=0 会被新守卫正确拦下（首版就这么写，红了一条，
#   而那条红恰恰证明守卫是活的）。
(SETLOC $f2 "$base") >/dev/null 2>&1
restored=$(DEX stat -c %s "$LMFILE" </dev/null 2>/dev/null)
check "  locmap 已复原（$restored 字节）" "$restored" "920"

echo "================ [7] 清理 ================"
PSQL $pport -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}'::regclass);" </dev/null >/dev/null 2>&1
  PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t62base;" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"

health_check_no_crash || true
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]]
