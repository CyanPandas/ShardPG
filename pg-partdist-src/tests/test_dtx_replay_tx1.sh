#!/usr/bin/env bash
# [宿主机] TX1 跨线联测：DTX-2PC 记录流经物理回放器（合并交接 §15.4 第 5 项）。
#
# 两条线交汇处唯一没有现成用例覆盖的地方：shardpg-4.0 的 DTX 记录
# （flags=PARTWAL_FLAG_DTX(8)，载荷 DtxRecord 而非 XLogRecord）会经复制通道
# 进入 follower 的段流，而 shardpg-replay 的回放器此前从未见过这一类记录。
# 机械合并的失效模式（§15.2 #1）：DTX 被判成 DATA 喂进 rm_redo → PANIC 或
# 静默写坏页面。
#
# 验收标准：
#   1. 跨分区事务从 coordinator 提交成功（两分片落在不同 worker，走 Citus 2PC），
#      两侧 leader 段流里的 DATA/MARKER 记录正常。
#   2. DTX 记录在 leader 段流里 flags=8、orig_lsn=0；经 Raft 复制到 follower 后
#      flags **原样保留**（§5.5：复制通道丢 flags 即必炸的前置）。
#   3. follower replay_catchup 能**跨过** DTX 记录追平到段流尖端 —— 游标推进、
#      不进 rm_redo、节点零崩溃。
#   4. 回放完成后 follower 副本页面与 leader 洞外逐字节一致（pagecmp.py，
#      堆 heap_mask 口径 + pd_lsn 不掩）。
#   5. DTX 记录对 gclog 无副作用：可见性判决仍只由 MARKER 驱动。
#
# 夹具照抄 test_txn_layer_r2.sh（register_shard_fileset → follower 壳表 +
# replay_set_locmap → 数据组 → replay_enable），扩成两分片各一组。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

check() {  # check <名字> <实际> <期望>
  if [[ -n "$2" && "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置：group0 收敛 =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

echo "========== [1] 夹具：2 分片分布表（两分片必须在不同 worker）=========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS tx1_dtx;
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;
CREATE TABLE tx1_dtx(id int primary key, v text);
SELECT create_distributed_table('tx1_dtx', 'id');
ALTER TABLE tx1_dtx SET (autovacuum_enabled = off);
SQL

mapfile -t SHARDS < <(PSQL $COORD -Atc \
  "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s
     JOIN pg_dist_placement p ON p.shardid=s.shardid
     JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
    WHERE s.logicalrelid='tx1_dtx'::regclass ORDER BY s.shardid")
check "分布表有 2 个分片" "${#SHARDS[@]}" "2"
gid_a=$(echo "${SHARDS[0]}" | cut -d' ' -f1); pport_a=$(echo "${SHARDS[0]}" | cut -d' ' -f2)
gid_b=$(echo "${SHARDS[1]}" | cut -d' ' -f1); pport_b=$(echo "${SHARDS[1]}" | cut -d' ' -f2)
check "两分片 leader 在不同 worker" "$([[ "$pport_a" != "$pport_b" ]] && echo ok)" "ok"

# 找两个分片键，分别命中 shard A 与 shard B（跨分区事务的原料）
key_a=""; key_b=""
for k in $(seq 1 64); do
  s=$(PSQL $COORD -Atc "SELECT get_shard_id_for_distribution_column('tx1_dtx', $k)")
  if [[ "$s" == "$gid_a" && -z "$key_a" ]]; then key_a=$k; fi
  if [[ "$s" == "$gid_b" && -z "$key_b" ]]; then key_b=$k; fi
  [[ -n "$key_a" && -n "$key_b" ]] && break
done
check "找到命中两分片的分布键（a=${key_a} b=${key_b}）" \
      "$([[ -n "$key_a" && -n "$key_b" ]] && echo ok)" "ok"

# 每个分片：leader + 两个 follower（避开两个 leader 端口）
setup_shard() {  # setup_shard <gid> <pport>  → 输出 "f1 f2"
  local gid=$1 pport=$2 f1="" f2="" p
  for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
    [[ "$p" == "$pport_a" || "$p" == "$pport_b" ]] && continue
    if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; break; fi
  done
  echo "$f1 $f2"
}
read -r f1_a f2_a <<< "$(setup_shard "$gid_a" "$pport_a")"
# shard B 的 follower 与 A 错开，免得同一节点上两份夹具相互干扰
f1_b=""; f2_b=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport_a" || "$p" == "$pport_b" || "$p" == "$f1_a" || "$p" == "$f2_a" ]] && continue
  if [[ -z "$f1_b" ]]; then f1_b=$p; elif [[ -z "$f2_b" ]]; then f2_b=$p; break; fi
done
echo "  shardA=${gid_a} leader=:${pport_a} followers=:${f1_a} :${f2_a}"
echo "  shardB=${gid_b} leader=:${pport_b} followers=:${f1_b} :${f2_b}"

echo "========== [2] fileset 注册 + follower 壳表/locmap + 数据组 + 回放 arm =========="
setup_replication() {  # setup_replication <gid> <pport> <f1> <f2>
  local gid=$1 pport=$2 f1=$3 f2=$4
  local shard_tbl="tx1_dtx_${gid}"
  local pnode=$((pport - 5431)) f1node=$((f1 - 5431)) f2node=$((f2 - 5431))

  local nrels
  nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" | tail -1)
  check "shard ${gid}: fileset 注册（主堆+PK=2，无 TOAST 索引才 2；实际 ${nrels}）" \
        "$([[ -n "$nrels" && "$nrels" -ge 2 ]] && echo ok)" "ok"

  local fsrows roles ords spcs dbs rels
  fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')
  roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
  spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
  rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)

  local fp np
  for fp in $f1 $f2; do
    PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE tx1_dtx INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
    np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])")
    check "shard ${gid}: follower :$fp locmap 配对" "$([[ -n "$np" && "$np" -ge 2 ]] && echo ok)" "ok"
  done

  local p
  for p in $pport $f1 $f2; do
    PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
  done

  local members="ARRAY[${pnode}, ${f1node}, ${f2node}]" st t
  PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
  st=""
  for t in $(seq 1 20); do
    st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null)
    [[ "$st" == "leader" ]] && break; sleep 1
  done
  check "shard ${gid}: 分区组 leader 就位" "$st" "leader"
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
    local en
    en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${shard_tbl}')")
    check "shard ${gid}: follower :$fp replay_enable" "$en" "t"
  done
}
setup_replication "$gid_a" "$pport_a" "$f1_a" "$f2_a"
setup_replication "$gid_b" "$pport_b" "$f1_b" "$f2_b"

loid_a=$(PSQL $pport_a -Atc "SELECT partdist.local_partition_for_shard(${gid_a})")
loid_b=$(PSQL $pport_b -Atc "SELECT partdist.local_partition_for_shard(${gid_b})")
flush_lsn() { PSQL "$1" -Atc "SELECT partdist.get_partition_flush_lsn($2)"; }

echo "========== [3] 跨分区事务提交（两分片同事务，Citus 走 2PC）=========="
b4_a=$(flush_lsn $pport_a $loid_a); b4_b=$(flush_lsn $pport_b $loid_b)
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL
BEGIN;
INSERT INTO tx1_dtx VALUES (${key_a}, 'cross-a');
INSERT INTO tx1_dtx VALUES (${key_b}, 'cross-b');
COMMIT;
SQL
check "跨分区事务提交成功、两行可读" \
      "$(PSQL $COORD -Atc "SELECT count(*) FROM tx1_dtx WHERE id IN (${key_a},${key_b})")" "2"
af_a=$(flush_lsn $pport_a $loid_a); af_b=$(flush_lsn $pport_b $loid_b)
check "shard A 段流有新记录（${b4_a}→${af_a}）" "$([[ "$af_a" -gt "$b4_a" ]] && echo ok)" "ok"
check "shard B 段流有新记录（${b4_b}→${af_b}）" "$([[ "$af_b" -gt "$b4_b" ]] && echo ok)" "ok"

# Citus 对该事务是否走了 2PC 挂点、DTX 接线是否认得 gid —— 以段流为准报告
ndtx_a=$(PSQL $pport_a -Atc "
  SELECT count(*) FROM generate_series($((b4_a+1)), ${af_a}) g,
       LATERAL partdist.partwal_read_record(${loid_a}::oid, g) r WHERE r.flags = 8")
ndtx_b=$(PSQL $pport_b -Atc "
  SELECT count(*) FROM generate_series($((b4_b+1)), ${af_b}) g,
       LATERAL partdist.partwal_read_record(${loid_b}::oid, g) r WHERE r.flags = 8")
# ★ 必须断言，不能只 echo。这两个数是 TX1 的立身之本 —— 若 DTX 接线整个回归
# （GUC 总开关关掉、或 PRE_PREPARE / 阶段 3 的挂点丢失），真实 2PC 路径一条 DTX
# 记录都不产生，而下面 [4] 起全部基于**手工注入**的记录，照样全绿。
check "真实 2PC 在 shard A 段流留下 DTX 记录（${ndtx_a} 条）" \
      "$([[ -n "$ndtx_a" && "$ndtx_a" -ge 1 ]] && echo ok)" "ok"
check "真实 2PC 在 shard B 段流留下 DTX 记录（${ndtx_b} 条）" \
      "$([[ -n "$ndtx_b" && "$ndtx_b" -ge 1 ]] && echo ok)" "ok"

echo "========== [4] 显式 DTX 记录：flags 端到端保真 =========="
# 不依赖 Citus 触发条件，直接在两侧 leader 各落一条 DTX_PREPARE(kind=1) ——
# 这是把"回放器遇到 DTX 记录"变成确定性事实的最短路径。dtxid 取一个
# 不会与真实事务撞车的常数。
DTXID=888777001
plsn_dtx_a=$(PSQL $pport_a -Atc "SELECT partdist.partwal_append_dtx_record(${loid_a}::oid, 1, ${DTXID}, ${gid_a})" | tail -1)
plsn_dtx_b=$(PSQL $pport_b -Atc "SELECT partdist.partwal_append_dtx_record(${loid_b}::oid, 1, ${DTXID}, ${gid_b})" | tail -1)
check "shard A: DTX 记录落盘（plsn=${plsn_dtx_a}）" "$([[ -n "$plsn_dtx_a" && "$plsn_dtx_a" -gt 0 ]] && echo ok)" "ok"
check "shard B: DTX 记录落盘（plsn=${plsn_dtx_b}）" "$([[ -n "$plsn_dtx_b" && "$plsn_dtx_b" -gt 0 ]] && echo ok)" "ok"

hdr_a=$(PSQL $pport_a -Atc "SELECT flags||'|'||orig_lsn FROM partdist.partwal_read_record(${loid_a}::oid, ${plsn_dtx_a})")
check "shard A: leader 侧 DTX 头（flags=8 orig_lsn=0/0）" "$hdr_a" "8|0/0"

dtx_body=$(PSQL $pport_a -Atc "SELECT kind||'|'||dtxid||'|'||coord_gsid FROM partdist.partwal_read_dtx_record(${loid_a}::oid, ${plsn_dtx_a})")
check "shard A: partwal_read_dtx_record 解析（kind=1 dtxid coord）" "$dtx_body" "1|${DTXID}|${gid_a}"
# 非 DTX 位置取 plsn=1（夹具首条 INSERT 的 DATA 记录）。不能用 plsn_dtx-1：
# 阶段 [3] 的真实 2PC 已经在流里写过 DTX_PREPARE/DECISION，紧邻位置未必是 DATA。
notdtx=$(PSQL $pport_a -Atc "SELECT coalesce(kind::text,'NULL') FROM partdist.partwal_read_dtx_record(${loid_a}::oid, 1)" | tail -1)
check "shard A: 非 DTX 位置(plsn=1) read_dtx 返回 NULL" "$notdtx" "NULL"

# 再写一批 DATA：把 DTX 记录裹进正常复制增量里（复制挂钩按区间取增量，
# 直接追加的记录随下一次 flush 携带）
PSQL $COORD -v ON_ERROR_STOP=1 -q -c \
  "INSERT INTO tx1_dtx SELECT g, 'bulk'||g FROM generate_series(100, 160) g;"
tip_a=$(flush_lsn $pport_a $loid_a); tip_b=$(flush_lsn $pport_b $loid_b)
check "DTX 之后有新 DATA（A: ${tip_a} > ${plsn_dtx_a}）" "$([[ "$tip_a" -gt "$plsn_dtx_a" ]] && echo ok)" "ok"
check "DTX 之后有新 DATA（B: ${tip_b} > ${plsn_dtx_b}）" "$([[ "$tip_b" -gt "$plsn_dtx_b" ]] && echo ok)" "ok"

echo "========== [5] follower 侧：flags 保真 + 回放跨过 DTX =========="
verify_follower() {  # verify_follower <gid> <pport> <loid> <fp> <plsn_dtx> <tip>
  local gid=$1 pport=$2 loid=$3 fp=$4 plsn_dtx=$5 tip=$6
  local shard_tbl="tx1_dtx_${gid}"
  local foid fl t
  foid=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${gid})")

  # 等字节到齐再 catchup（catchup 的 upto 用 leader 尖端作显式 bound）
  fl=""
  for t in $(seq 1 90); do
    fl=$(flush_lsn $fp "$foid")
    [[ -n "$fl" && "$fl" -ge "$tip" ]] && break; sleep 2
  done
  check "shard ${gid}: follower :$fp 收齐 ${tip} 条（本地 ${fl}）" \
        "$([[ -n "$fl" && "$fl" -ge "$tip" ]] && echo ok)" "ok"

  local fhdr
  fhdr=$(PSQL $fp -Atc "SELECT flags||'|'||orig_lsn FROM partdist.partwal_read_record(${foid}::oid, ${plsn_dtx})")
  check "shard ${gid}: follower :$fp DTX 头原样（flags=8 orig_lsn=0/0）" "$fhdr" "8|0/0"

  local app
  app=$(PSQL $fp -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${tip}, 180000)" 2>&1 | tail -1)
  check "shard ${gid}: follower :$fp 回放跨过 DTX 追平（applied=${app}/${tip}）" \
        "$([[ "$app" == "$tip" ]] && echo ok)" "ok"

  # DTX 不写 gclog：这个 dtxid 对应的"事务号"在 gclog 里必须仍是 running
  local citus_gid st
  citus_gid=$(PSQL $pport -Atc "SELECT groupid FROM pg_dist_local_group")
  st=$(PSQL $fp -Atc "SELECT status FROM partdist.gclog_status(${citus_gid}, ${DTXID} & 2147483647)" 2>/dev/null)
  check "shard ${gid}: follower :$fp DTX 对 gclog 无副作用（running）" "$st" "running"
}
verify_follower "$gid_a" "$pport_a" "$loid_a" "$f1_a" "$plsn_dtx_a" "$tip_a"
verify_follower "$gid_a" "$pport_a" "$loid_a" "$f2_a" "$plsn_dtx_a" "$tip_a"
verify_follower "$gid_b" "$pport_b" "$loid_b" "$f1_b" "$plsn_dtx_b" "$tip_b"
verify_follower "$gid_b" "$pport_b" "$loid_b" "$f2_b" "$plsn_dtx_b" "$tip_b"

echo "========== [6] 页面洞外逐字节一致（pagecmp）=========="
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1

page_compare() {  # page_compare <gid> <pport> <fp>
  local gid=$1 pport=$2 fp=$3
  local shard_tbl="tx1_dtx_${gid}"
  PSQL $pport -q -c "CHECKPOINT;" >/dev/null
  sleep 2
  local pdata fdata
  pdata=$(PSQL $pport -Atc "SHOW data_directory")
  fdata=$(PSQL $fp -Atc "SHOW data_directory")

  local fs_sql="SELECT role||'.'||ord||','||
       pg_relation_filepath(pg_filenode_relation(
           CASE WHEN spc = 1663 THEN 0 ELSE spc END, relnum))
  FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord"

  local lrows frows ncmp=0
  lrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; ${fs_sql}" | grep ',')
  frows=$(PSQL $fp   -Atc "SET citus.override_table_visibility=false; ${fs_sql}" | grep ',')

  local lkv fkv key lrel frel same errf
  while IFS= read -r lkv; do
    key=${lkv%%,*}; lrel=${lkv#*,}
    fkv=$(echo "$frows" | grep "^${key}," || true)
    [[ -z "$fkv" ]] && { check "shard ${gid}:${fp} ${key} follower 侧有映射" "missing" "present"; continue; }
    frel=${fkv#*,}
    # 主堆才有权威掩码口径；索引/TOAST 只做主堆比对之外的存在性确认
    [[ "$key" != 0.* ]] && continue
    errf=$(mktemp)
    same=$(DEX python3 /tmp/pagecmp.py --kind=heap "${pdata}/${lrel}" "${fdata}/${frel}" </dev/null 2>"$errf")
    ncmp=$((ncmp+1))
    check "shard ${gid}: follower :$fp ${key}(主堆) 洞外逐字节一致" "$same" "IDENTICAL_OUTSIDE_HOLE"
    [[ "$same" != "IDENTICAL_OUTSIDE_HOLE" ]] && sed 's/^/        /' "$errf"
    rm -f "$errf"
  done <<< "$lrows"
  check "shard ${gid}: follower :$fp 至少比对了 1 个主堆文件" "$([[ "$ncmp" -ge 1 ]] && echo ok)" "ok"
}
page_compare "$gid_a" "$pport_a" "$f1_a"
page_compare "$gid_a" "$pport_a" "$f2_a"
page_compare "$gid_b" "$pport_b" "$f1_b"
page_compare "$gid_b" "$pport_b" "$f2_b"

echo ""
health_check_no_crash
health_check_no_drops

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "TX1 跨线联测：全部通过"; else echo "TX1 跨线联测：存在 FAIL"; fi

# ---- 清理（探针三件套：回放禁用 → 组清 → 壳表/分布表/partition_map）----
if [[ "${KEEP_FIXTURE:-0}" != "1" ]]; then
  for spec in "${gid_a}:${f1_a}:${f2_a}" "${gid_b}:${f1_b}:${f2_b}"; do
    gid=${spec%%:*}; rest=${spec#*:}; fx1=${rest%%:*}; fx2=${rest#*:}
    for fp in $fx1 $fx2; do
      PSQL $fp -q -c "SELECT partdist.replay_disable('tx1_dtx_${gid}');" >/dev/null 2>&1
    done
  done
  for p in $pport_a $pport_b $f1_a $f2_a $f1_b $f2_b; do
    PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" >/dev/null 2>&1
  done
  for spec in "${gid_a}:${f1_a}:${f2_a}" "${gid_b}:${f1_b}:${f2_b}"; do
    gid=${spec%%:*}; rest=${spec#*:}; fx1=${rest%%:*}; fx2=${rest#*:}
    for fp in $fx1 $fx2; do
      PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS tx1_dtx_${gid};" >/dev/null 2>&1
    done
  done
  PSQL $COORD -q -c "DROP TABLE IF EXISTS tx1_dtx;" >/dev/null 2>&1
  for p in $COORD $pport_a $pport_b $f1_a $f2_a $f1_b $f2_b; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id IN (${gid_a},${gid_b});" >/dev/null 2>&1
  done
fi

exit $FAIL
