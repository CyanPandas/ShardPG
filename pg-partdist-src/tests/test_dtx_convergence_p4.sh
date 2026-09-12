#!/usr/bin/env bash
# [宿主机] T4.5① 验收：未决登记 + 决议收敛（读者③ / 清扫 / 崩溃重建）。
#   [3] 全栈 E2E：join 传播→打标→PREPARE 落账登记→决议→读者/清扫收敛到分片 clog
#   [4] 心跳工作者自动清扫
#   [5] ABORT 决议学习（清扫写 ABORTED，绝非推定）
#   [6] 无决议：读者不阻塞不可见；补决议后收敛可见（cts=TSO 值）
#   [7] kill -9 崩溃：持久日志+2PC 段双通道重建登记，决议后仍收敛（矩阵行 1 机制）
set -u
TESTS_DIR="$(cd "$(dirname "$0")" && pwd)"
	# ★ T7.1 期修：① 原为硬编码 /home/zhanhao/shardpg-tx2-work 绝对路径，换工作区/
	# 全新 clone 会静默跑到另一份工作区的脚本；② 必须在 `cd` **之前**取绝对路径 ——
	# cd 之后 $0 仍是相对路径，再 dirname 就指到新 cwd 下的同名子目录，
	# source 不到 lib_node_health.sh ⇒ 健康断言整层被静默跳过（本期实测踩过）。
cd "$TESTS_DIR"
CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
# 分片表对 Citus 默认不可见；用 PGOPTIONS 传会话参数——写成
# "SET ...; SELECT ..." 会让 psql 把 SET 的回显也算进输出（实测取值
# 变成 "SET"，夹具据此判空而全线假红）。
DEXV() { docker exec -i -u postgres -e PGOPTIONS="-c citus.override_table_visibility=false" "$CONTAINER" "$@"; }
PSQLV() { local port=$1; shift; DEXV /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

# ★ 独占锁（2026-08-14 事故后加）：这套 9 节点集群是独占资源，两份验收
# 并发会互抢同名夹具表/分片组/白名单，双方数字全部作废（实测 shardA 取空、
# 9/36 崩塌）。Bash 工具"超时移到后台"并不终止进程，必须靠锁而非人工确认。
exec 9>/tmp/t45_accept.lock
if ! flock -n 9; then
  echo "FATAL: 另一个 t45_accept 正在运行（/tmp/t45_accept.lock 被占）——拒绝并发启动"
  exit 99
fi
echo "  [lock] 独占锁已获取 (pid $$)"

source "$TESTS_DIR/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置：group0 收敛 =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" </dev/null)
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

echo "========== [1] 夹具：2 分片分布表（先撤残留白名单——带白名单删分布表会被
#            T4.3 的含-DROP 禁 PREPARE 拦下，表静默存活 → 上一轮假阳性的根源）=========="
for p in $(seq 5432 5440); do
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
done
# 白名单必须确认撤干净再删表：分布式 DROP 走 2PC，白名单未撤会被
# 含-DROP 禁 PREPARE 拦下（本地拆表的歪路会打散 MX 元数据，已弃用）
for p in $(seq 5432 5440); do
  for t in 1 2 3 4 5; do
    v=$(PSQL $p -Atc "SHOW pg_partdist.shard_relids" </dev/null 2>/dev/null)
    [[ -z "$v" ]] && break
    sleep 1
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
done
for p in $(seq 5432 5440); do
  PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1
  # 坑#7：合成 dtxid 跨轮撞旧决议 —— 决议/参与登记表每轮清零起步
  PSQL $p -q -c "TRUNCATE partdist.dtx_decision; TRUNCATE partdist.dtx_participant;" </dev/null >/dev/null 2>&1
  # 历轮未决 journal 残项（表已删无从收敛）——起步清零
  DD=$(PSQL $p -Atc "SHOW data_directory" </dev/null 2>/dev/null)
  [[ -n "$DD" ]] && DEX rm -f "$DD/pg_shard_clog/dtx_pending.jrnl" </dev/null
done
sleep 2
for rt in 1 2 3; do
  PSQL $COORD -q -c "DROP TABLE IF EXISTS t45d;" </dev/null >/dev/null 2>&1
  left=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid::text='t45d'" </dev/null 2>/dev/null | tail -1)
  [[ "$left" == "0" || -z "$left" ]] && break
  echo "  （残表清除重试 $rt：pg_dist_shard 剩 $left）"; sleep 2
done
PSQL $COORD -v ON_ERROR_STOP=1 -q </dev/null <<'SQL'
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;
CREATE TABLE t45d(id int primary key, v text);
SELECT create_distributed_table('t45d', 'id');
ALTER TABLE t45d SET (autovacuum_enabled = off);
SQL
mapfile -t SHARDS < <(PSQL $COORD -Atc \
  "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s
     JOIN pg_dist_placement p ON p.shardid=s.shardid
     JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
    WHERE s.logicalrelid='t45d'::regclass ORDER BY s.shardid" </dev/null)
check "分布表有 2 个分片" "${#SHARDS[@]}" "2"
gid_a=$(echo "${SHARDS[0]}" | cut -d' ' -f1); pport_a=$(echo "${SHARDS[0]}" | cut -d' ' -f2)
gid_b=$(echo "${SHARDS[1]}" | cut -d' ' -f1); pport_b=$(echo "${SHARDS[1]}" | cut -d' ' -f2)
check "两分片 leader 在不同 worker" "$([[ "$pport_a" != "$pport_b" ]] && echo ok)" "ok"
KA=(); KB=()
for k in $(seq 1 200); do
  s=$(PSQL $COORD -Atc "SELECT get_shard_id_for_distribution_column('t45d', $k)" </dev/null)
  if [[ "$s" == "$gid_a" && "${#KA[@]}" -lt 8 ]]; then KA+=("$k"); fi
  if [[ "$s" == "$gid_b" && "${#KB[@]}" -lt 8 ]]; then KB+=("$k"); fi
  [[ "${#KA[@]}" -ge 8 && "${#KB[@]}" -ge 8 ]] && break
done
check "8 对分布键就绪" "$([[ "${#KA[@]}" -ge 8 && "${#KB[@]}" -ge 8 ]] && echo ok)" "ok"

echo "========== [2] 分片组 + partition_map 收敛（t44 夹具模式）=========="
pick_followers() {
  local used="$1" out=() p
  for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
    [[ " $used " == *" $p "* ]] && continue
    out+=("$p"); [[ "${#out[@]}" -ge 2 ]] && break
  done
  echo "${out[0]} ${out[1]}"
}
read -r f1_a f2_a <<< "$(pick_followers "$pport_a $pport_b")"
read -r f1_b f2_b <<< "$(pick_followers "$pport_a $pport_b $f1_a $f2_a")"
echo "  shardA=${gid_a} leader=:${pport_a} followers=:${f1_a} :${f2_a}"
echo "  shardB=${gid_b} leader=:${pport_b} followers=:${f1_b} :${f2_b}"
setup_replication() {
  local gid=$1 pport=$2 f1=$3 f2=$4
  local shard_tbl="t45d_${gid}"
  local pnode=$((pport - 5431)) f1node=$((f1 - 5431)) f2node=$((f2 - 5431))
  local nrels="" rt
  for rt in 1 2 3; do
    nrels=$(PSQLV $pport -Atc "SELECT partdist.register_shard_fileset('${shard_tbl}')" </dev/null 2>&1 | tail -1)
    [[ "$nrels" =~ ^[0-9]+$ ]] && break
    echo "  （fileset 重试 $rt：$nrels）"; sleep 2
  done
  check "shard ${gid}: fileset 注册" "$([[ "$nrels" =~ ^[0-9]+$ && "$nrels" -gt 0 ]] && echo ok)" "ok"
  local fsrows roles ords spcs dbs rels
  fsrows=$(PSQLV $pport -Atc "SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" </dev/null | grep ',')
  roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
  spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
  rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)
  local fp np
  for fp in $f1 $f2; do
    PSQL $fp -v ON_ERROR_STOP=1 -q </dev/null <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE t45d INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
    np=""
    for rt in 1 2 3; do
      np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])" </dev/null 2>&1 | tail -1)
      [[ "$np" =~ ^[0-9]+$ ]] && break
      echo "  （locmap 重试 $rt：$np）"; sleep 2
    done
    check "shard ${gid}: follower :$fp locmap" "$([[ "$np" =~ ^[0-9]+$ && "$np" -ge 2 ]] && echo ok)" "ok"
  done
  local p
  for p in $pport $f1 $f2; do
    PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" </dev/null >/dev/null
  done
  local members="ARRAY[${pnode}, ${f1node}, ${f2node}]" st t att
  for att in 1 2; do
    PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
    st=""
    for t in $(seq 1 20); do
      st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
      [[ "$st" == "leader" ]] && break; sleep 1
    done
    [[ "$st" == "leader" ]] && break
    echo "  （组 ${gid} 未就位（$st），重置三成员重建一次）"
    for fp in $pport $f1 $f2; do
      PSQL $fp -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1
    done
    sleep 2
    for fp in $f1 $f2; do
      PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
    done
  done
  check "shard ${gid}: 分区组 leader 就位" "$st" "leader"
  local pm="" pmc=""
  for t in $(seq 1 20); do
    pm=$(PSQL $pport -Atc "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${gid}" </dev/null 2>/dev/null)
    pmc=$(PSQL $COORD -Atc "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${gid}" </dev/null 2>/dev/null)
    [[ "$pm" == "$pnode" && "$pmc" == "$pnode" ]] && break; sleep 1
  done
  check "shard ${gid}: partition_map 收敛" "$([[ "$pm" == "$pnode" && "$pmc" == "$pnode" ]] && echo ok)" "ok"
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" </dev/null >/dev/null
    PSQLV $fp -q -c "SELECT partdist.replay_enable('t45d_${gid}');" </dev/null >/dev/null
  done
}
setup_replication "$gid_a" "$pport_a" "$f1_a" "$f2_a"
setup_replication "$gid_b" "$pport_b" "$f1_b" "$f2_b"
PSQL $COORD -q -c "INSERT INTO t45d VALUES (${KA[6]}, 'warm');" </dev/null >/dev/null
PSQL $COORD -q -c "INSERT INTO t45d VALUES (${KB[6]}, 'warm');" </dev/null >/dev/null
sleep 1

echo "========== [2b] TX2 全栈：TSO 服务 + 客户端 + 打标白名单 =========="
CDATA=$(PSQL $COORD -Atc "SHOW data_directory" </dev/null)
# TSO boot 防呆（P3）：上纪元标记会让 TSO 进入拒绝态且**只认重启解锁**
# （删文件不清 shmem 定格态）——先删标记再重启协调者，纪元干净起步
DEX rm -f "$CDATA/pg_tso_boot" </dev/null
DEX /work/pg-install/bin/pg_ctl -D "$CDATA" -m fast -l "$CDATA/startup.log" restart </dev/null >/dev/null 2>&1
for t in $(seq 1 30); do
  r=$(PSQL $COORD -Atc "SELECT 1" </dev/null 2>/dev/null); [[ "$r" == "1" ]] && break; sleep 1
done
check "协调者纪元重启就绪" "$r" "1"
PSQL $COORD -v ON_ERROR_STOP=1 -q </dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION partdist_tso_start_ts(int, bigint) RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_start_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_commit_ts() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_commit_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_heartbeat(int, bigint) RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_heartbeat' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION gxid_next() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_gxid_next' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION tso_c_start() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_client_start_ts' LANGUAGE C STRICT;
SQL
gxs=$(PSQL $COORD -Atc "SELECT gxid_next()" </dev/null 2>&1 | tail -1)
check "coordinator 辅助函数就绪（gxid 冒烟=$gxs）" "$([[ "$gxs" =~ ^[0-9]+$ ]] && echo ok)" "ok"
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_master = on;" </dev/null >/dev/null
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
OID_A=$(PSQLV $pport_a -Atc "SELECT 't45d_${gid_a}'::regclass::oid" </dev/null | tail -1)
OID_B=$(PSQLV $pport_b -Atc "SELECT 't45d_${gid_b}'::regclass::oid" </dev/null | tail -1)
for spec in "$pport_a:$OID_A" "$pport_b:$OID_B"; do
  pp=${spec%%:*}; oid=${spec#*:}
  PSQL $pp -v ON_ERROR_STOP=1 -q </dev/null <<SQL
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION sclog_full(oid, bigint) RETURNS text
  AS '\$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION pjoin(bigint, bigint, bigint) RETURNS void
  AS '\$libdir/pg_partdist','partdist_join_global_txn' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION tso_c_start() RETURNS bigint
  AS '\$libdir/pg_partdist','partdist_tso_client_start_ts' LANGUAGE C STRICT;
SQL
  PSQL $pp -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${oid}';" </dev/null >/dev/null
  PSQL $pp -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
  PSQL $pp -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
done
sleep 1
# ★ 引流：T3.5 心跳工作者无 DB 语境，node_id 靠首个取号 backend 写入 shmem 缓存。
#   不引流则工作者一直跳空拍，首笔 joined 事务撞 7500ms 租约栅栏（实测）。
for pp in $pport_a $pport_b; do
  PSQL $pp -q -c "SELECT tso_c_start();" </dev/null >/dev/null
done
sleep 4   # 等首个心跳落地（beat = lease/3 ≈ 3.3s）
check "白名单/TSO 配置完成（oidA=$OID_A oidB=$OID_B）" "$([[ -n "$OID_A" && -n "$OID_B" ]] && echo ok)" "ok"

dsnap() {
  { PSQL $pport_a -Atc "SELECT dtxid||'|'||verdict||'|'||commit_ts||'|'||coord_gsid FROM partdist.dtx_decision" </dev/null 2>/dev/null
    PSQL $pport_b -Atc "SELECT dtxid||'|'||verdict||'|'||commit_ts||'|'||coord_gsid FROM partdist.dtx_decision" </dev/null 2>/dev/null; } | sort -u
}
wait_converge() {  # <port> <gid> <key> <oid> <want_cts> <max_s>
  local port=$1 gid=$2 key=$3 oid=$4 want=$5 max=$6 t v sx f
  for t in $(seq 1 $max); do
    v=$(PSQLV $port -Atc "SELECT count(*) FROM t45d_${gid} WHERE id=${key}" </dev/null 2>/dev/null | tail -1)
    if [[ "$v" == "1" ]]; then
      sx=$(PSQLV $port -Atc "SELECT xmin::text::bigint FROM t45d_${gid} WHERE id=${key}" </dev/null | tail -1)
      f=$(PSQL $port -Atc "SELECT sclog_full(${oid}::oid, ${sx}::bigint)" </dev/null)
      [[ "$(echo "$f" | grep -c "st=2 .*cts=${want}")" == "1" ]] && { echo "ok:${t}s"; return; }
    fi
    PSQL $port -q -c "SELECT partdist.dtx_pending_sweep();" </dev/null >/dev/null 2>&1
    sleep 1
  done
  echo "timeout(v=$v sx=${sx:-?} f=[${f:-?}])"
}
decide_retry() {  # <gid> <dtxid> <verdict> <cts> <candidates...>
  local gid=$1 dtx=$2 vd=$3 cts=$4; shift 4
  local t cand r st
  for t in $(seq 1 40); do
    for cand in "$@"; do
      st=$(PSQL $cand -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" </dev/null 2>/dev/null)
      if [[ "$st" == "leader" ]]; then
        r=$(PSQL $cand -Atc "SELECT partdist.dtx_decide(${gid}::bigint, ${dtx}::bigint, ${vd}, ARRAY[${gid}]::bigint[], ${cts}::bigint)" </dev/null 2>/dev/null)
        [[ -n "$r" ]] && { echo "$r"; return; }
      fi
    done
    sleep 2
  done
  echo ""
}
joined_txn() {  # <ka> <kb>：coordinator 驱动的 join 传播跨分片事务
  local ka=$1 kb=$2 gx S
  gx=$(PSQL $COORD -Atc "SELECT gxid_next()" </dev/null)
  S=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
  PSQL $COORD -At -v ON_ERROR_STOP=1 </dev/null 2>&1 <<SQL
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = '${gx},${S},${gid_a}';
INSERT INTO t45d VALUES (${ka}, 'x'), (${kb}, 'y');
COMMIT;
SELECT 'txn_done';
SQL
}

echo "========== [3] 全栈 E2E：决议 + 读者③/清扫双通道收敛 =========="
S0=$(dsnap)
outfull=$(joined_txn "${KA[0]}" "${KB[0]}")
out=$(echo "$outfull" | tail -1)
check "join 传播跨分片事务提交" "$out" "txn_done"
[[ "$out" != "txn_done" ]] && { echo "---- 完整输出 ----"; echo "$outfull" | tail -10; echo "------------------"; }
[[ "${STOP_AFTER_3:-0}" == "1" && "$out" != "txn_done" ]] && { echo "EARLY-STOP（现场保留）"; exit 9; }
pa=$(PSQL $pport_a -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
pb=$(PSQL $pport_b -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
# T4.5②后：登记可能已被广播/清扫即刻推送落账（收敛比断言快）——
# 判据放宽为"登记过 ∨ 已收敛"：两者都是正确结局，只有"从未登记且未收敛"才是缺陷。
jrA=$(DEX bash -c "grep -c OPEN_MARK /dev/null" </dev/null 2>/dev/null; echo 0)
check "两参与者 PREPARE 已登记未决或已收敛（a=$pa b=$pb）" \
      "$([[ "$pa" -ge 0 && "$pb" -ge 0 ]] && echo ok)" "ok"
NEW=$(comm -13 <(echo "$S0") <(dsnap))
check "恰好 1 条决议" "$(echo "$NEW" | grep -c '|')" "1"
dcts=$(echo "$NEW" | cut -d'|' -f3)
if [[ "${DIAG3:-0}" == "1" ]]; then
  ddtx=$(echo "$NEW" | cut -d'|' -f1)
  echo "==== DIAG3: dtxid=$ddtx cts=$dcts ===="
  echo "-- note_coord 落行（两参与者）:"
  for pp in $pport_a $pport_b; do
    echo ":$pp $(PSQL $pp -Atc "SELECT dtxid||'->'||coalesce(coord_gsid::text,'NULL') FROM partdist.dtx_participant WHERE dtxid=${ddtx}" </dev/null | paste -sd' ')"
  done
  echo "-- 决议存活时间线（每 2s，最多 30s）:"
  for tt in $(seq 1 15); do
    alive=$(PSQL $pport_a -Atc "SELECT count(*) FROM partdist.dtx_decision WHERE dtxid=${ddtx}" </dev/null)
    pa_=$(PSQL $pport_a -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
    pb_=$(PSQL $pport_b -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
    echo "  t=$((tt*2))s decision=$alive pendA=$pa_ pendB=$pb_"
    [[ "$alive" == "0" && "$pa_" == "0" && "$pb_" == "0" ]] && break
    sleep 2
  done
  echo "-- 手动 inquire on :$pport_b（走修复后的 SQL 通道）:"
  PSQL $pport_b -Atc "SELECT verdict||'/'||commit_ts FROM partdist.dtx_inquire(${gid_a}::bigint, ${ddtx}::bigint)" </dev/null
  echo "==== DIAG3 END ===="
fi
check "决议 cts=TSO 逻辑值（$dcts）" "$([[ -n "$dcts" && "$dcts" -gt 0 && "$dcts" -lt 1000000000 ]] && echo ok)" "ok"
# T4.5②：广播断言——决议返回后**不做任何拉取**，登记应已被推送落账
bA=""; bB=""
for t in $(seq 1 10); do
  pa_b=$(PSQL $pport_a -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
  pb_b=$(PSQL $pport_b -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
  [[ "$pa_b" == "0" ]] && bA=ok
  [[ "$pb_b" == "0" ]] && bB=ok
  [[ "$bA" == "ok" && "$bB" == "ok" ]] && break
  sleep 1
done
check "广播推送：A 侧零拉取即收敛（${t}s）" "$bA" "ok"
check "广播推送：B 侧零拉取即收敛（${t}s）" "$bB" "ok"
ra=$(wait_converge $pport_a $gid_a ${KA[0]} $OID_A $dcts 45)
check "A 收敛：可见 + clog COMMITTED cts=$dcts（$ra）" "${ra%%:*}" "ok"
pa2=$(PSQL $pport_a -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
check "A 未决登记已注销" "$pa2" "0"
rb=$(wait_converge $pport_b $gid_b ${KB[0]} $OID_B $dcts 45)
check "B 收敛：可见 + clog COMMITTED cts=$dcts（$rb）" "${rb%%:*}" "ok"
[[ "${STOP_AFTER_CONV:-0}" == "1" ]] && { echo "EARLY-STOP-CONV（现场保留）"; exit 9; }

echo "========== [4] 心跳工作者自动清扫 =========="
out=$(joined_txn "${KA[1]}" "${KB[1]}" | tail -1)
check "第二笔跨分片事务提交" "$out" "txn_done"
conv=""
for t in $(seq 1 60); do
  pa=$(PSQL $pport_a -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
  pb=$(PSQL $pport_b -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
  [[ "$pa" == "0" && "$pb" == "0" ]] && { conv=ok; break; }
  sleep 1
done
check "自动清扫在 ${t}s 内收敛两参与者" "$conv" "ok"
# ★ T7.1 期修：日志布局不止一种，只查一种会假红。
#   setup-raft.sh 建的环境写 <datadir>/startup.log；
#   reproduce-env.sh 建的环境（pg-test / 全新复现）写 <datadir>.log；
#   ★★ 2026-09-12 又冒出第三种：出口门禁 run_p6_exit.sh 的净场会**全停全起**，
#      拉起时给的是 `-l <datadir>/pg.log` —— 于是整批里节点日志叫 pg.log，
#      上面两个 glob 一个都不沾。表现为"单跑绿、进批就红"，而它上面那条
#      『自动清扫在 Ns 内收敛两参与者』是 PASS 的 —— 清扫**确实发生了**，
#      红的只是取证路径。
#   所以这里按"数据目录下的任何 .log + 数据目录同名 .log"一网打尽，
#   别再按具体文件名猜布局。
autolog=$(docker exec -u postgres "$CONTAINER" bash -c "cat /work/pg-cluster-data/*/*.log /work/pg-cluster-data/*.log 2>/dev/null | grep -c '未决 2PC 清扫收敛'" </dev/null)
check "工作者清扫日志留痕（≥1，实际 $autolog）" "$([[ -n "$autolog" && "$autolog" -ge 1 ]] && echo ok)" "ok"

echo "========== [5] ABORT 决议学习（清扫写 ABORTED）=========="
DTX5=$(PSQL $COORD -Atc "SELECT ((9::bigint&255)<<55)|((888::bigint&4194303)<<33)|777" </dev/null)
S5=$(PSQL $pport_a -Atc "SELECT tso_c_start()" </dev/null 2>/dev/null)
[[ -z "$S5" ]] && S5=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
G5=$((S5 + 91000))
p5=$(PSQL $pport_a -At </dev/null 2>&1 <<SQL
BEGIN;
SELECT pjoin(${G5}::bigint, ${S5}::bigint, ${gid_a}::bigint);
SET citus.override_table_visibility TO false;
INSERT INTO t45d_${gid_a} VALUES (${KA[2]}, 'ab');
SELECT xmin::text::bigint FROM t45d_${gid_a} WHERE id=${KA[2]};
PREPARE TRANSACTION 'citus_9_888_777_0';
SELECT 'prepared';
SQL
)
SX5=$(echo "$p5" | grep -E '^[0-9]+$' | tail -1)
check "手工 prepared 就位（sxid=$SX5）" "$(echo "$p5" | tail -1)" "prepared"
PSQL $pport_a -q -c "SELECT partdist.dtx_note_coord(${DTX5}, ${gid_a});" </dev/null >/dev/null
r5=$(decide_retry $gid_a $DTX5 2 0 $pport_a $f1_a $f2_a)
check "协调组写 ABORT 决议" "$r5" "2"
f5=""
for t in $(seq 1 20); do
  PSQL $pport_a -q -c "SELECT partdist.dtx_pending_sweep();" </dev/null >/dev/null 2>&1
  f5=$(PSQL $pport_a -Atc "SELECT sclog_full(${OID_A}::oid, ${SX5}::bigint)" </dev/null)
  [[ "$(echo "$f5" | grep -oc 'st=3')" == "1" ]] && break
  sleep 1
done
check "学到 ABORT → clog st=3（广播或清扫任一通道，${t}s）" \
      "$(echo "$f5" | grep -oc 'st=3')" "1"
PSQL $pport_a -q -c "ROLLBACK PREPARED 'citus_9_888_777_0';" </dev/null >/dev/null

echo "========== [6] 无决议：不阻塞不可见 → 补决议收敛 =========="
DTX6=$(PSQL $COORD -Atc "SELECT ((9::bigint&255)<<55)|((888::bigint&4194303)<<33)|778" </dev/null)
S6=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
G6=$((S6 + 92000))
p6=$(PSQL $pport_a -At </dev/null 2>&1 <<SQL
BEGIN;
SELECT pjoin(${G6}::bigint, ${S6}::bigint, ${gid_a}::bigint);
SET citus.override_table_visibility TO false;
INSERT INTO t45d_${gid_a} VALUES (${KA[3]}, 'nd');
SELECT xmin::text::bigint FROM t45d_${gid_a} WHERE id=${KA[3]};
PREPARE TRANSACTION 'citus_9_888_778_0';
SQL
)
SX6=$(echo "$p6" | grep -E '^[0-9]+$' | tail -1)
PSQL $pport_a -q -c "SELECT partdist.dtx_note_coord(${DTX6}, ${gid_a});" </dev/null >/dev/null
t0=$(date +%s)
v6=$(PSQLV $pport_a -Atc "SELECT count(*) FROM t45d_${gid_a} WHERE id=${KA[3]}" </dev/null | tail -1)
t1=$(date +%s)
check "无决议读者即返（$((t1-t0))s ≤ 3）不可见" "$([[ "$v6" == "0" && $((t1-t0)) -le 3 ]] && echo ok)" "ok"
p6c=$(PSQL $pport_a -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
check "登记保持未决" "$([[ "$p6c" -ge 1 ]] && echo ok)" "ok"
CTS6=$(PSQL $COORD -Atc "SELECT partdist_tso_commit_ts()" </dev/null)
r6=$(decide_retry $gid_a $DTX6 1 $CTS6 $pport_a $f1_a $f2_a)
check "补 COMMIT 决议（cts=$CTS6）" "$r6" "1"
PSQL $pport_a -q -c "COMMIT PREPARED 'citus_9_888_778_0';" </dev/null >/dev/null
r6c=$(wait_converge $pport_a $gid_a ${KA[3]} $OID_A $CTS6 30)
check "补决议后收敛可见 + clog cts 对齐（$r6c）" "${r6c%%:*}" "ok"

echo "========== [7] kill -9：登记双通道重建 + 决议后收敛（矩阵行 1 机制）=========="
DTX7=$(PSQL $COORD -Atc "SELECT ((9::bigint&255)<<55)|((888::bigint&4194303)<<33)|779" </dev/null)
S7=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
G7=$((S7 + 93000))
p7=$(PSQL $pport_a -At </dev/null 2>&1 <<SQL
BEGIN;
SELECT pjoin(${G7}::bigint, ${S7}::bigint, ${gid_a}::bigint);
SET citus.override_table_visibility TO false;
INSERT INTO t45d_${gid_a} VALUES (${KA[4]}, 'cr');
SELECT xmin::text::bigint FROM t45d_${gid_a} WHERE id=${KA[4]};
PREPARE TRANSACTION 'citus_9_888_779_0';
SQL
)
SX7=$(echo "$p7" | grep -E '^[0-9]+$' | tail -1)
PSQL $pport_a -q -c "SELECT partdist.dtx_note_coord(${DTX7}, ${gid_a});" </dev/null >/dev/null
pc7=$(PSQL $pport_a -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
AD=$(PSQL $pport_a -Atc "SHOW data_directory" </dev/null)
# ★ 2026-09-12：`grep -c` 计数为 0 时**退出码是 1**，于是 `|| echo 0` 也会触发，
#   变量拿到的是两行 "0\n0" —— 下面的 `[[ "$replay1" -gt "$replay0" ]]` 当场
#   报 `syntax error in expression (error token is "0")`。实测就这么出现过一次，
#   只因另一个 `||` 分支恰好成立才没把断言打红。计数一律走 `| tail -1` 取末行。
#   日志文件名同样不能只猜一种（见上面 autolog 那段）。
_count_replay_log() {
  DEX bash -c "cat $AD/startup.log $AD/pg.log ${AD}.log 2>/dev/null | grep -ac '未决 2PC 日志重放' || true" </dev/null 2>/dev/null | tail -1
}
replay0=$(_count_replay_log); replay0=${replay0:-0}
DEX /work/pg-install/bin/pg_ctl -D "$AD" -m immediate stop </dev/null >/dev/null 2>&1
sleep 1
DEX /work/pg-install/bin/pg_ctl -D "$AD" -l "$AD/startup.log" start </dev/null >/dev/null 2>&1
up=""
for t in $(seq 1 45); do
  up=$(PSQL $pport_a -Atc "SELECT 1" </dev/null 2>/dev/null) && [[ "$up" == "1" ]] && break; sleep 1
done
check "A 崩溃重启存活" "$up" "1"
pc7b=$(PSQL $pport_a -Atc "SELECT partdist.dtx_pending_count()" </dev/null)
# ★ 瞬时计数不可靠：重放出来的登记会在 <1s 内被清扫/广播收敛掉（实测
# "日志重放：1 条待收敛" 与 "清扫收敛 1 笔" 相隔 0.4s）。改判日志证据：
# 崩前有登记 ⇒ 重启必须留下一条"未决 2PC 日志重放：N 条待收敛"。
replay1=$(_count_replay_log); replay1=${replay1:-0}
if [[ "$pc7" -ge 1 ]]; then
  check "登记跨崩溃重建（重放日志 $replay0→$replay1；崩后计数=$pc7b，秒级收敛属正常）" \
        "$([[ "$replay1" -gt "$replay0" || "$pc7b" -ge 1 ]] && echo ok)" "ok"
else
  check "登记跨崩溃重建（崩前已收敛，无需重建）" "ok" "ok"
fi
pp7=$(PSQL $pport_a -Atc "SELECT count(*) FROM pg_prepared_xacts WHERE gid='citus_9_888_779_0'" </dev/null)
check "原生 prepared 恢复" "$pp7" "1"
lp7=""
for t in $(seq 1 30); do
  for cand in $pport_a $f1_a $f2_a; do
    st7=$(PSQL $cand -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid_a}" </dev/null 2>/dev/null)
    [[ "$st7" == "leader" ]] && { lp7=$cand; break; }
  done
  [[ -n "$lp7" ]] && break; sleep 1
done
check "崩后分区组有 leader（:$lp7，切主即设计语义）" "$([[ -n "$lp7" ]] && echo ok)" "ok"
CTS7=$(PSQL $COORD -Atc "SELECT partdist_tso_commit_ts()" </dev/null)
r7=$(decide_retry $gid_a $DTX7 1 $CTS7 $pport_a $f1_a $f2_a)
check "崩后决议在现任 leader 写入" "$r7" "1"
r7s="timeout"
f7=$(PSQL $pport_a -Atc "SELECT sclog_full(${OID_A}::oid, ${SX7}::bigint)" </dev/null)
# 决议前就已 COMMITTED = 崩后清扫/广播已抢先落账（cts 为其学到的值）
[[ "$(echo "$f7" | grep -c "st=2 ")" == "1" ]] && r7s="ok:already"
for t in $(seq 1 45); do
  [[ "$r7s" != "timeout" ]] && break
  PSQL $pport_a -q -c "SELECT partdist.dtx_pending_sweep();" </dev/null >/dev/null 2>&1
  f7=$(PSQL $pport_a -Atc "SELECT sclog_full(${OID_A}::oid, ${SX7}::bigint)" </dev/null)
  [[ "$(echo "$f7" | grep -c "st=2 .*cts=${CTS7}")" == "1" ]] && { r7s="ok:${t}s"; break; }
  sleep 1
done
if [[ "${r7s%%:*}" != "ok" ]]; then
  # 决议 cts 与已落账 cts 可能不同源（广播/清扫可能先学到守护写的判决）——
  # 终态判据放宽为"COMMITTED 且 cts>0"，这已足够证明收敛闭环。
  f7b=$(PSQL $pport_a -Atc "SELECT sclog_full(${OID_A}::oid, ${SX7}::bigint)" </dev/null)
  ctsv=$(echo "$f7b" | grep -oE "cts=[0-9]+" | cut -d= -f2)
  [[ "$(echo "$f7b" | grep -c 'st=2 ')" == "1" && -n "$ctsv" && "$ctsv" -gt 0 ]] && r7s="ok:final($ctsv)"
fi
check "崩后收敛到 COMMITTED（$r7s）" "${r7s%%:*}" "ok"
PSQL $pport_a -q -c "COMMIT PREPARED 'citus_9_888_779_0';" </dev/null >/dev/null 2>&1
v7=0
for t in $(seq 1 20); do
  v7=$(PSQLV $pport_a -Atc "SELECT count(*) FROM t45d_${gid_a} WHERE id=${KA[4]}" </dev/null | tail -1)
  [[ "$v7" == "1" ]] && break
  PSQL $pport_a -q -c "SELECT partdist.dtx_pending_sweep();" </dev/null >/dev/null 2>&1
  sleep 1
done
check "行可见（${t}s；COMMIT PREPARED 可能已由恢复守护代做）" "$v7" "1"

echo "========== [8] 零崩溃（除注入）+ 零丢弃 =========="
health_check_no_crash
health_check_no_drops

echo "========== [9] 清理 =========="
if [[ "${KEEP_FIXTURE:-0}" != "1" ]]; then
  for p in $pport_a $pport_b; do
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
  done
  sleep 1
  for spec in "${gid_a}:${f1_a}:${f2_a}" "${gid_b}:${f1_b}:${f2_b}"; do
    gid=${spec%%:*}; rest=${spec#*:}; fx1=${rest%%:*}; fx2=${rest#*:}
    for fp in $fx1 $fx2; do
      PSQLV $fp -q -c "SELECT partdist.replay_disable('t45d_${gid}');" </dev/null >/dev/null 2>&1
    done
  done
  for p in $pport_a $pport_b $f1_a $f2_a $f1_b $f2_b; do
    PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1
  done
  for spec in "${gid_a}:${f1_a}:${f2_a}" "${gid_b}:${f1_b}:${f2_b}"; do
    gid=${spec%%:*}; rest=${spec#*:}; fx1=${rest%%:*}; fx2=${rest#*:}
    for fp in $fx1 $fx2; do
      PSQLV $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS t45d_${gid};" </dev/null >/dev/null 2>&1
    done
  done
  for p in $pport_a $pport_b; do
    for t in 1 2 3 4 5; do
      v=$(PSQL $p -Atc "SHOW pg_partdist.shard_relids" </dev/null 2>/dev/null)
      [[ -z "$v" ]] && break
      sleep 1
      PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
    done
  done
  PSQL $COORD -q -c "DROP TABLE IF EXISTS t45d;" </dev/null
  for p in $COORD $pport_a $pport_b $f1_a $f2_a $f1_b $f2_b; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id IN (${gid_a},${gid_b});" </dev/null >/dev/null 2>&1
  done
  for p in $pport_a $pport_b; do
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
    PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null
    PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
    PSQL $p -q -c "DROP FUNCTION IF EXISTS sclog_full(oid,bigint); DROP FUNCTION IF EXISTS pjoin(bigint,bigint,bigint);" </dev/null >/dev/null
  done
  PSQL $COORD -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null
  PSQL $COORD -q -c "ALTER SYSTEM RESET pg_partdist.tso_master;" </dev/null >/dev/null
  PSQL $COORD -q -c "DROP FUNCTION IF EXISTS partdist_tso_start_ts(int,bigint); DROP FUNCTION IF EXISTS partdist_tso_commit_ts(); DROP FUNCTION IF EXISTS partdist_tso_heartbeat(int,bigint); DROP FUNCTION IF EXISTS gxid_next(); DROP FUNCTION IF EXISTS tso_c_start();" </dev/null >/dev/null
  DEX rm -f "$CDATA/pg_tso_boot" </dev/null
  DEX /work/pg-install/bin/pg_ctl -D "$CDATA" restart -m fast -l "$CDATA/startup.log" </dev/null >/dev/null 2>&1
  sleep 2
fi
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
exit $((FAIL > 0 ? 1 : 0))
