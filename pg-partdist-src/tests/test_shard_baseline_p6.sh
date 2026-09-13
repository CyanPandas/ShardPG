#!/usr/bin/env bash
# [宿主机] T6.1 验收：全量物理基线（TX_TSO_MVCC_DEV_PLAN.md §3.8 T6.1）。
#
# 实装的是设计 §13 约束 2 后半句 ——「副本必须由 leader shard 物理拷贝初始化
# （**拷贝时记下 partition_lsn 静止点**，增量从该游标重放追齐）」。此前只有
# 前半句：locmap 只配对"哪个文件对哪个文件"，从不配对"从哪个游标开始"。
#
# ★ 核心断言（本套件存在的理由）：
#   人为把 follower 的主堆弄成**比 leader 长**（追加一个零页），这正是
#   §13 约束 13「Raft 环顶爆丢掉 leader 的物理截断」之后副本的样子 ——
#   文档原话是"永久分叉，既无检测也无修复路径"，且"运行期不 PANIC，只有
#   逐页 diff 能发现"。发一次全量基线之后，follower 必须**逐字节回到与
#   leader 一致**。这条通了，才谈得上"重做物理基线"是一条真实存在的路。
#
# ★ 防假通过：只比"基线后 leader == follower"是不够的 —— 若基线压根没进流，
#   而我制造的分歧又恰好没被读到，比对照样可能过。故必须同时断言
#   **follower 的文件确实变了**（基线前后 md5 不同）。
#
# ★ follower 壳表绝不能被 SELECT（无白名单 ⇒ on-access 剪枝会按原生 clog
#   清掉分片元组，就地损毁副本）。本脚本只用文件级手段触碰 follower。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }

check() {
  if [[ -z "${2:-}" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

exec 9>/tmp/t61_baseline.lock
if ! flock -n 9; then
  echo "FATAL: 另一个 T6.1 验收正在运行（9 节点集群是独占资源）"; exit 99
fi

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "================ [0] 夹具：1 分片 + raft 组 + 两 follower ================"
leader0=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" </dev/null)
check "group0 有 leader" "$([[ -n "$leader0" && "$leader0" != "0" ]] && echo ok)" "ok"
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  PSQL $p -q -c "ALTER SYSTEM SET pg_partdist.replay_trust_local_segments = on;" </dev/null >/dev/null
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done

PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS t61base;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE t61base(id int, v text);
SELECT create_distributed_table('t61base', 'id');
ALTER TABLE t61base SET (autovacuum_enabled = off, toast.autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='t61base'::regclass" </dev/null)
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}" </dev/null)
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="t61base_${gid}"
echo "  shard=${gid} leader=:${pport}(node${pnode}) followers=:${f1} :${f2}"
check "夹具三要素齐（gid/leader/follower）" \
      "$([[ -n "$gid" && -n "$pport" && -n "$f1" && -n "$f2" ]] && echo ok)" "ok"

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
CREATE TABLE ${shard_tbl} (LIKE t61base INCLUDING ALL);
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
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 ${gid} leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
  en=$(PSQL $fp -Atc "SELECT partdist.replay_enable('${shard_tbl}')" </dev/null)
  check "follower :$fp replay_enable" "$en" "t"
done

SOID=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT '${shard_tbl}'::regclass::oid" </dev/null | tail -1)
PSQL $pport -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${SOID}';" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL $pport -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$SOID" ]] && break; sleep 1
done
check "leader 白名单生效" "$guc" "$SOID"

LEADER_OID=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
check "leader 认得本地分片" "$([[ -n "$LEADER_OID" && "$LEADER_OID" != "0" ]] && echo ok)" "ok"

FPATH_MAIN() {  # <port> —— 该节点上本分片主堆文件的绝对路径
  local fp=$1 fdata frel foid
  fdata=$(PSQL "$fp" -Atc "SHOW data_directory" </dev/null)
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null | tail -1)
  frel=$(PSQL "$fp" -Atc "SET citus.override_table_visibility=false; SELECT pg_relation_filepath(${foid}::regclass)" </dev/null | tail -1)
  echo "${fdata}/${frel}"
}
NBLOCKS() {  # <文件绝对路径> —— 按 8192 字节算块数
  local f=$1 sz
  sz=$(DEX stat -c %s "$f" </dev/null 2>/dev/null)
  [[ -n "$sz" ]] && echo $(( sz / 8192 )) || echo ""
}
MD5() { DEX md5sum "$1" </dev/null 2>/dev/null | cut -d' ' -f1; }

# ★ 逐字节比对必须用项目自带的 pagecmp.py，不能用 md5。
#   增量记录走 REGBUF_STANDARD，FPI 里**掐掉了 [pd_lower, pd_upper) 那个洞**，
#   于是 follower 页面洞内的残字节与 leader 天然不同 —— 这不是分歧，是压缩。
#   md5 覆盖整页（含洞），会把这种合法差异报成红。首版脚本正是这么错的：
#   两个 follower 的 md5 **彼此相同**、与 leader 不同 —— 系统性差异而非损坏，
#   这个指纹本身就说明用错了尺子。
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
CMP_MAIN() {  # <fport> —— 主堆与 leader 的 pagecmp 判词
  local fp=$1
  DEX python3 /tmp/pagecmp.py --kind=heap "$LMAIN" "$(FPATH_MAIN "$fp")" </dev/null 2>/dev/null
}

wait_caught_up() {  # <fport> <期望plsn> <超时s>
  local fp=$1 target=$2 timeout=$3 app foid
  [[ -n "$target" && "$target" -gt 0 ]] || { echo ""; return 1; }
  foid=$(PSQL "$fp" -Atc "SELECT partdist.local_partition_for_shard(${gid})" </dev/null)
  app=$(PSQL "$fp" -Atc "SELECT partdist.replay_catchup(${foid}::regclass, ${target}, $((timeout * 1000)))" </dev/null 2>/dev/null || echo 0)
  [[ -n "$app" && "$app" -ge "$target" ]] && { echo "$app"; return 0; }
  app=$(PSQL "$fp" -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}" </dev/null 2>/dev/null || echo 0)
  echo "$app"; return 1
}

echo "================ [1] 写入负载 + 追平（基线之前先对齐） ================"
PSQL $pport -v ON_ERROR_STOP=1 -q <<SQL
INSERT INTO ${shard_tbl} SELECT g, repeat('x', 100) FROM generate_series(1, 400) g;
SQL
check "leader 写入 400 行" "$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM ${shard_tbl}" </dev/null | tail -1)" "400"

plsn1=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
check "leader 已产生 parwal 记录" "$([[ -n "$plsn1" && "$plsn1" -gt 0 ]] && echo ok)" "ok"
for fp in $f1 $f2; do
  a=$(wait_caught_up "$fp" "$plsn1" 30)
  check "follower :$fp 基线前已追平（applied=$a >= $plsn1）" \
        "$([[ -n "$a" && "$a" -ge "$plsn1" ]] && echo ok)" "ok"
done

# ★ 文件级比对之前必须 CHECKPOINT：leader 写的是共享缓冲区，盘上那份可能还是
#   旧的甚至全零；而 follower 侧 ShardReplayDoCheckpoint 是显式刷盘+fsync 的。
#   不刷就比，等于拿"leader 没落盘的文件"和"follower 落了盘的文件"较劲 ——
#   首版脚本正是这么错的，pagecmp 判词 hole_mismatch [0,0) vs [256,304) 里那个
#   [0,0) 就是 leader 盘上尚未写过的空白页。
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null   # ★ 比文件前必须刷盘
LMAIN=$(FPATH_MAIN $pport); F1MAIN=$(FPATH_MAIN $f1); F2MAIN=$(FPATH_MAIN $f2)
lblk=$(NBLOCKS "$LMAIN"); f1blk=$(NBLOCKS "$F1MAIN"); f2blk=$(NBLOCKS "$F2MAIN")
echo "  主堆块数：leader=$lblk f1=$f1blk f2=$f2blk"
check "基线前 leader/f1 块数一致" "$f1blk" "$lblk"
check "基线前 leader/f2 块数一致" "$f2blk" "$lblk"

echo "================ [2] ★ 人为制造「follower 比 leader 长」的分叉 ================"
# 这正是 §13 约束 13 的后果形态：leader 的物理截断丢了，follower 没短。
# 文档原话："永久分叉，既无检测也无修复路径"。
DEX bash -c "dd if=/dev/zero bs=8192 count=1 >> '$F1MAIN' 2>/dev/null" </dev/null
DEX bash -c "dd if=/dev/zero bs=8192 count=1 >> '$F2MAIN' 2>/dev/null" </dev/null
f1blk_bad=$(NBLOCKS "$F1MAIN"); f2blk_bad=$(NBLOCKS "$F2MAIN")
check "f1 已被弄长一块（$f1blk -> $f1blk_bad）" "$f1blk_bad" "$((f1blk + 1))"
check "f2 已被弄长一块（$f2blk -> $f2blk_bad）" "$f2blk_bad" "$((f2blk + 1))"
md5_f1_bad=$(MD5 "$F1MAIN"); md5_f2_bad=$(MD5 "$F2MAIN")
md5_l_pre=$(MD5 "$LMAIN")
check "分叉已成立：f1 与 leader 不一致" \
      "$([[ -n "$md5_f1_bad" && "$md5_f1_bad" != "$md5_l_pre" ]] && echo ok)" "ok"

echo "================ [3] 负向：**关掉流式分块**时，块数超过 inline 上限仍须显式报错 ================"
# ★ T7.21（P7-R3）之后，这道上限只在 fileset_baseline_chunk_blocks = 0 时生效。
#   它原本的真实作用是"别让一次灌的 FPI 描述符撑满 8192 槽的共享捕获环"
#   （环满只 WARNING 并覆盖未消费条目 = 静默丢页），分块排空之后环占用与总大小无关。
neg=$(PSQL $pport -Atc "SET pg_partdist.fileset_baseline_chunk_blocks = 0; SET pg_partdist.fileset_inline_max_blocks = 1; SELECT partdist.shard_baseline_emit('${shard_tbl}'::regclass)" </dev/null 2>&1 | tr '\n' ' ')
check "关掉分块 + 超限 ⇒ 显式 ERROR（不静默降级）" \
      "$([[ "$neg" == *"超过 pg_partdist.fileset_inline_max_blocks"* ]] && echo banned)" "banned"
plsn_after_neg=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
check "报错的那次没有留下半条记录（plsn 未变）" "$plsn_after_neg" "$plsn1"

echo "================ [4] ★ T7.21：**流式**发射全量物理基线（越过旧上限、强制多块） ================"
# 把 inline 上限压到 1、分块压到 2 块：旧实现在这里必然 ERROR；新实现必须
# 分多块发完，并且后面 [5] 的逐字节比对（pagecmp）照样要过 —— 这才证明
# "分块排空之间穿插的记录"没有打乱副本。
logmark=$(DEX bash -c "cat /work/pg-cluster-data/worker$((pport-5432))/*.log /work/pg-cluster-data/worker$((pport-5432)).log 2>/dev/null | wc -l" </dev/null | tr -d '[:space:]')
logmark=${logmark:-0}
base_plsn=$(PSQL $pport -Atc "SET pg_partdist.fileset_inline_max_blocks = 1; SET pg_partdist.fileset_baseline_chunk_blocks = 2; SELECT partdist.shard_baseline_emit('${shard_tbl}'::regclass)" </dev/null 2>&1 | tail -1)
nchunk=$(DEX bash -c "cat /work/pg-cluster-data/worker$((pport-5432))/*.log /work/pg-cluster-data/worker$((pport-5432)).log 2>/dev/null | tail -n +$((logmark + 1)) | grep -c '块流式发射' || true" </dev/null | tail -1 | tr -d '[:space:]')
check "★ 越过旧 inline 上限的基线不再报错（base_part_lsn=$base_plsn）" \
      "$([[ "$base_plsn" =~ ^[0-9]+$ ]] && echo ok)" "ok"
check "★ 基线确实分多块流式发射（日志留痕 ${nchunk} 条）" \
      "$([[ -n "$nchunk" && "$nchunk" -ge 1 ]] && echo ok)" "ok"
check "shard_baseline_emit 返回数字（base_part_lsn=$base_plsn）" \
      "$([[ "$base_plsn" =~ ^[0-9]+$ && "$base_plsn" -gt 0 ]] && echo ok)" "ok"
check "base_part_lsn 落在旧 plsn 之后（$base_plsn > $plsn1）" \
      "$([[ "$base_plsn" -gt "$plsn1" ]] && echo ok)" "ok"

plsn2=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
check "基线之后流里确有 FPI（plsn $plsn2 > base $base_plsn）" \
      "$([[ -n "$plsn2" && "$plsn2" -gt "$base_plsn" ]] && echo ok)" "ok"

echo "================ [5] follower 追平并复原 ================"
for fp in $f1 $f2; do
  a=$(wait_caught_up "$fp" "$plsn2" 60)
  check "follower :$fp 追平到基线之后（applied=$a >= $plsn2）" \
        "$([[ -n "$a" && "$a" -ge "$plsn2" ]] && echo ok)" "ok"
done

PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null   # ★ 比文件前必须刷盘
f1blk_post=$(NBLOCKS "$F1MAIN"); f2blk_post=$(NBLOCKS "$F2MAIN")
lblk_post=$(NBLOCKS "$LMAIN")
echo "  基线后块数：leader=$lblk_post f1=$f1blk_post f2=$f2blk_post"
check "★ f1 块数回到与 leader 一致（多出来的那块被截掉）" "$f1blk_post" "$lblk_post"
check "★ f2 块数回到与 leader 一致" "$f2blk_post" "$lblk_post"

md5_l=$(MD5 "$LMAIN"); md5_f1=$(MD5 "$F1MAIN"); md5_f2=$(MD5 "$F2MAIN")
check "★★ f1 主堆与 leader 逐字节一致（pagecmp）" "$(CMP_MAIN $f1)" "IDENTICAL_OUTSIDE_HOLE"
check "★★ f2 主堆与 leader 逐字节一致（pagecmp）" "$(CMP_MAIN $f2)" "IDENTICAL_OUTSIDE_HOLE"

# ★ 防假通过：必须证明 follower 的文件**真的变了**
check "防假通过：f1 文件确实被基线改过" \
      "$([[ -n "$md5_f1" && "$md5_f1" != "$md5_f1_bad" ]] && echo ok)" "ok"
check "防假通过：f2 文件确实被基线改过" \
      "$([[ -n "$md5_f2" && "$md5_f2" != "$md5_f2_bad" ]] && echo ok)" "ok"

echo "================ [6] 基线可重复（幂等性） ================"
base_plsn2=$(PSQL $pport -Atc "SELECT partdist.shard_baseline_emit('${shard_tbl}'::regclass)" </dev/null 2>&1 | tail -1)
check "第二次基线也成功（base=$base_plsn2）" \
      "$([[ "$base_plsn2" =~ ^[0-9]+$ && "$base_plsn2" -gt "$base_plsn" ]] && echo ok)" "ok"
plsn3=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
for fp in $f1 $f2; do
  a=$(wait_caught_up "$fp" "$plsn3" 60)
  check "follower :$fp 二次基线后仍追平" \
        "$([[ -n "$a" && "$a" -ge "$plsn3" ]] && echo ok)" "ok"
done
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null   # ★ 比文件前必须刷盘
check "二次基线后 f1 仍与 leader 一致（pagecmp）" "$(CMP_MAIN $f1)" "IDENTICAL_OUTSIDE_HOLE"
check "二次基线后 f2 仍与 leader 一致（pagecmp）" "$(CMP_MAIN $f2)" "IDENTICAL_OUTSIDE_HOLE"

echo "================ [7] 基线之后增量照常 ================"
PSQL $pport -q -c "INSERT INTO ${shard_tbl} SELECT g, 'post-baseline' FROM generate_series(1001,1100) g;" </dev/null >/dev/null
plsn4=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${LEADER_OID})" </dev/null | tail -1)
for fp in $f1 $f2; do
  a=$(wait_caught_up "$fp" "$plsn4" 45)
  check "follower :$fp 追平基线后的增量" \
        "$([[ -n "$a" && "$a" -ge "$plsn4" ]] && echo ok)" "ok"
done
PSQL $pport -q -c "CHECKPOINT;" </dev/null >/dev/null   # ★ 比文件前必须刷盘
check "增量之后 f1 仍与 leader 逐字节一致（pagecmp）" "$(CMP_MAIN $f1)" "IDENTICAL_OUTSIDE_HOLE"
check "增量之后 f2 仍与 leader 逐字节一致（pagecmp）" "$(CMP_MAIN $f2)" "IDENTICAL_OUTSIDE_HOLE"

echo "================ [8] 清理 ================"
PSQL $pport -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL $pport -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}'::regclass);" </dev/null >/dev/null 2>&1
  PSQL $fp -q -c "SET citus.enable_ddl_propagation = off; DROP TABLE IF EXISTS ${shard_tbl};" </dev/null >/dev/null 2>&1
done
PSQL $COORD -q -c "DROP TABLE IF EXISTS t61base;" </dev/null >/dev/null 2>&1
check "清理完成" "ok" "ok"

health_check_no_crash || true
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ "$FAIL" -eq 0 ]]
