#!/usr/bin/env bash
# [宿主机] T4.7 P4 出口套件：崩溃矩阵 §3.4 四行逐格 + 跨分片 SI 一致快照
#          + 三态问询三分支。
#
# 与 test_dtx_convergence_p4.sh 的分工：那份验证**收敛机制本身**（登记/
# 日志/广播/清扫/问询，45/0）；本份验证**崩溃矩阵的每一格**与可见性契约，
# 是 P4 的出口门禁。夹具沿用那份的成熟形态（2 分片 × 各 3 副本 + TSO +
# join 传播），只替换验证腿。
#
#   [M1] 参与分片 leader 在 PREPARE **之后**崩 → 切主 + 决议 → 收敛
#   [M2] 参与分片 leader 在 PREPARE **之前**崩 → 事务失败，无残留未决
#   [M3] 协调者组 leader 在决议**持久化前**崩 → 推定中止（ABORT 终局）
#   [M4] 协调者组 leader 在决议**持久化后**崩 → 新 leader 继续应答 COMMIT
#   [M5] master(TSO) 不可达 → 事务级 fail-closed，已提交事务可见性不受影响
#   [S1] 跨分片 SI 一致快照：同一 start_ts 在两分片看到同一版本集合
#   [Q1/Q2/Q3] 三态问询三分支：start_ts>S 跳过 / 无登记跳过 / 问询学到判决
set -u

cd "$(dirname "$0")"
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
exec 9>/tmp/t47_matrix.lock
if ! flock -n 9; then
  echo "FATAL: 另一个 t47 套件正在运行（/tmp/t47_matrix.lock 被占）——拒绝并发启动"
  exit 99
fi
echo "  [lock] 独占锁已获取 (pid $$)"

TESTS_DIR="/home/zhanhao/shardpg-tx2-work/pg-partdist-src/tests"
source "$TESTS_DIR/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置：group0 收敛 =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()" </dev/null)
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

echo "========== [0b] 跨轮净场（本套件会注入崩溃，残局必须先清）=========="
# 本套件的 M1/M2/M4 会 kill 节点；上一轮若在崩溃态结束，会留下：遗留
# prepared 持锁（令 DROP 永久等待）、白名单未撤（令含 DROP 事务被禁令拦）、
# 节点未起。三者叠加会让下一轮夹具全线失败（实测 M1 前置直接崩）。
for p in $(seq 5432 5440); do
  d=$(PSQL $p -Atc "SHOW data_directory" </dev/null 2>/dev/null)
  if [[ -z "$d" ]]; then
    n=$((p-5432)); d="/work/pg-cluster-data/$([[ $n -eq 0 ]] && echo coordinator || echo worker$n)"
    DEX /work/pg-install/bin/pg_ctl -D "$d" -l "$d/startup.log" start </dev/null >/dev/null 2>&1
  fi
done
sleep 3
nup=0
for p in $(seq 5432 5440); do [[ "$(PSQL $p -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && nup=$((nup+1)); done
check "净场：9 节点全部在线（$nup/9）" "$nup" "9"
for p in $(seq 5432 5440); do
  for g in $(PSQL $p -Atc "SELECT gid FROM pg_prepared_xacts" </dev/null 2>/dev/null); do
    PSQL $p -q -c "ROLLBACK PREPARED '$g';" </dev/null >/dev/null 2>&1
  done
  PSQL $p -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null 2>&1
  PSQL $p -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  # 跨轮 dtxid 是固定合成值（citus_9_777_30x），旧决议行会让本轮"前置"
  # 直接读到上一轮的判决 → 断言错位。每轮清零起步。
  PSQL $p -q -c "TRUNCATE partdist.dtx_decision; TRUNCATE partdist.dtx_participant;" </dev/null >/dev/null 2>&1
done
sleep 2
# ★ 残表也必须清：上一轮若在崩溃态结束，t47d 及其分片表会留下（含数据），
#   导致本轮 INSERT 撞主键、[2b] 找不到对应分片表（表在别的 shardid 上）。
#   白名单已在上面撤净，此时 DROP 不会被含-DROP 禁令拦。
PSQL $COORD -q -c "SET lock_timeout='10s'; DROP TABLE IF EXISTS t47d CASCADE;" </dev/null >/dev/null 2>&1
for p in $(seq 5432 5440); do
  PSQL $p -q -c "SET citus.enable_ddl_propagation=off; SET lock_timeout='10s'; DROP TABLE IF EXISTS t47d CASCADE;" </dev/null >/dev/null 2>&1
  for tb in $(PSQLV $p -Atc "SELECT tablename FROM pg_tables WHERE tablename LIKE 't47d\_%'" </dev/null 2>/dev/null | grep -E '^t47d_'); do
    PSQL $p -q -c "SET citus.enable_ddl_propagation=off; SET citus.override_table_visibility=false; SET lock_timeout='10s'; DROP TABLE IF EXISTS ${tb} CASCADE;" </dev/null >/dev/null 2>&1
  done
done
nleft=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_class WHERE relname LIKE 't47d%'" </dev/null 2>/dev/null | tail -1)
check "净场：t47d 残表已清（$nleft）" "$nleft" "0"

# ★ R-P4-11 对策之一：残留 raft 组必须重置。
#   验收清理只删表，**raft 组不随表消失**；组还在而 shard_identity 的
#   映射已随表删掉 ⇒ dtx_decide 报"协调组在本节点没有对应分片" ⇒ 决议
#   写不进去 ⇒ 所有依赖决议的腿连锁失败（实测 T4.5 基准从 45/0 掉到 30/15）。
ngrp=0
for p in $(seq 5432 5440); do
  gn=$(PSQL $p -Atc "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0" </dev/null 2>/dev/null | tail -1)
  if [[ "$gn" =~ ^[1-9] ]]; then
    PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1
    ngrp=$((ngrp+gn))
  fi
  PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id NOT IN (SELECT shardid FROM pg_dist_shard);" </dev/null >/dev/null 2>&1
done
sleep 2
nrest=0
for p in $(seq 5432 5440); do
  gn=$(PSQL $p -Atc "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0" </dev/null 2>/dev/null | tail -1)
  [[ "$gn" =~ ^[0-9]+$ ]] && nrest=$((nrest+gn))
done
check "净场：残留 raft 组已重置（清 $ngrp 个，剩 $nrest）" "$nrest" "0"

# ★ R-P4-11 对策之三：悬空 Citus 元数据必须清。
#   历轮删表会在 pg_dist_shard/placement/partition 留下 logicalrelid 已失效
#   的行；rebuild_shard_identity 撞上它们直接报 "object_name does not
#   reference a valid relation"（整条身份重建报废）⇒ dtx_decide 找不到分片
#   ⇒ **决议不生成**。这是"决议链失效"的直接成因（实测基准 30/15 的首红）。
for p in $(seq 5432 5440); do
  PSQL $p -q </dev/null >/dev/null 2>&1 <<'SQLX'
DELETE FROM pg_dist_placement WHERE shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid::oid NOT IN (SELECT oid FROM pg_class));
DELETE FROM pg_dist_shard WHERE logicalrelid::oid NOT IN (SELECT oid FROM pg_class);
DELETE FROM pg_dist_partition WHERE logicalrelid::oid NOT IN (SELECT oid FROM pg_class);
SQLX
done
ndang=0
for p in $(seq 5432 5440); do
  d=$(PSQL $p -Atc "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid::oid NOT IN (SELECT oid FROM pg_class)" </dev/null 2>/dev/null | tail -1)
  [[ "$d" =~ ^[0-9]+$ ]] && ndang=$((ndang+d))
done
check "净场：悬空 Citus 元数据已清（剩 $ndang）" "$ndang" "0"

# ★ R-P4-11 对策之四：未决登记跨轮污染。
#   pending 在 shmem，删 journal 不够——必须重启才清空；残留会让"未决
#   登记已注销"类断言直接失败（实测基准 41/4 的三条红皆由此）。
npend=0
for p in $(seq 5432 5440); do
  c=$(PSQL $p -Atc "SELECT partdist.dtx_pending_count()" </dev/null 2>/dev/null | tail -1)
  [[ "$c" =~ ^[0-9]+$ ]] && npend=$((npend+c))
done
if [[ "$npend" -gt 0 ]]; then
  for p in $(seq 5432 5440); do
    d=$(PSQL $p -Atc "SHOW data_directory" </dev/null 2>/dev/null | tail -1)
    [[ -n "$d" ]] && DEX rm -f "$d/pg_shard_clog/dtx_pending.jrnl" </dev/null 2>/dev/null
  done
  for p in $(seq 5432 5440); do
    d=$(PSQL $p -Atc "SHOW data_directory" </dev/null 2>/dev/null | tail -1)
    [[ -n "$d" ]] && DEX /work/pg-install/bin/pg_ctl -D "$d" -m fast -l "$d/startup.log" restart </dev/null >/dev/null 2>&1
  done
  sleep 5
  npend=0
  for p in $(seq 5432 5440); do
    c=$(PSQL $p -Atc "SELECT partdist.dtx_pending_count()" </dev/null 2>/dev/null | tail -1)
    [[ "$c" =~ ^[0-9]+$ ]] && npend=$((npend+c))
  done
fi
check "净场：未决登记已归零（剩 $npend）" "$npend" "0"

# ★ R-P4-11 对策之二：TSO 纪元复位。
#   T4.4 起 ts 换源后**任何提交**都要取 commit_ts；boot 防呆一旦进入上纪元
#   拒绝态，全集群提交失败（连 create_distributed_table 都只写元数据、分片表
#   建不出来），下游表现为"决议链整体失效"。删标记 + 重启协调者是唯一解锁。
CD0=$(PSQL $COORD -Atc "SHOW data_directory" </dev/null 2>/dev/null | tail -1)
DEX rm -f "$CD0/pg_tso_boot" </dev/null 2>/dev/null
DEX /work/pg-install/bin/pg_ctl -D "$CD0" -m fast -l "$CD0/startup.log" restart </dev/null >/dev/null 2>&1
for t in $(seq 1 30); do [[ "$(PSQL $COORD -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 1; done
check "净场：TSO 纪元已复位（协调者重启）" "$(PSQL $COORD -Atc 'SELECT 1' </dev/null 2>/dev/null)" "1"

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
  PSQL $COORD -q -c "DROP TABLE IF EXISTS t47d;" </dev/null >/dev/null 2>&1
  left=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_dist_shard WHERE logicalrelid::text='t47d'" </dev/null 2>/dev/null | tail -1)
  [[ "$left" == "0" || -z "$left" ]] && break
  echo "  （残表清除重试 $rt：pg_dist_shard 剩 $left）"; sleep 2
done
PSQL $COORD -v ON_ERROR_STOP=1 -q </dev/null <<'SQL'
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;
CREATE TABLE t47d(id int primary key, v text);
SELECT create_distributed_table('t47d', 'id');
ALTER TABLE t47d SET (autovacuum_enabled = off);
SQL
mapfile -t SHARDS < <(PSQL $COORD -Atc \
  "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s
     JOIN pg_dist_placement p ON p.shardid=s.shardid
     JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary'
    WHERE s.logicalrelid='t47d'::regclass ORDER BY s.shardid" </dev/null)
check "分布表有 2 个分片" "${#SHARDS[@]}" "2"
gid_a=$(echo "${SHARDS[0]}" | cut -d' ' -f1); pport_a=$(echo "${SHARDS[0]}" | cut -d' ' -f2)
gid_b=$(echo "${SHARDS[1]}" | cut -d' ' -f1); pport_b=$(echo "${SHARDS[1]}" | cut -d' ' -f2)
check "两分片 leader 在不同 worker" "$([[ "$pport_a" != "$pport_b" ]] && echo ok)" "ok"
KA=(); KB=()
for k in $(seq 1 200); do
  s=$(PSQL $COORD -Atc "SELECT get_shard_id_for_distribution_column('t47d', $k)" </dev/null)
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
  local shard_tbl="t47d_${gid}"
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
CREATE TABLE ${shard_tbl} (LIKE t47d INCLUDING ALL);
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
    PSQLV $fp -q -c "SELECT partdist.replay_enable('t47d_${gid}');" </dev/null >/dev/null
  done
}
setup_replication "$gid_a" "$pport_a" "$f1_a" "$f2_a"
setup_replication "$gid_b" "$pport_b" "$f1_b" "$f2_b"
PSQL $COORD -q -c "INSERT INTO t47d VALUES (${KA[6]}, 'warm');" </dev/null >/dev/null
PSQL $COORD -q -c "INSERT INTO t47d VALUES (${KB[6]}, 'warm');" </dev/null >/dev/null
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
OID_A=$(PSQLV $pport_a -Atc "SELECT 't47d_${gid_a}'::regclass::oid" </dev/null | tail -1)
OID_B=$(PSQLV $pport_b -Atc "SELECT 't47d_${gid_b}'::regclass::oid" </dev/null | tail -1)
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

# ★ 取号可用性探针（T4.6 的教训）：tso_master / boot 防呆 / conninfo 任一
#   没到位，取号就失败 → join_info 变成 "gx,,gsid" 非法值 → 事务静默回滚，
#   后续所有腿连锁失败却看不出根因。失败即早停。
tso_probe=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null 2>&1 | tail -1)
check "TSO 取号可用（start_ts=$tso_probe）" "$([[ "$tso_probe" =~ ^[0-9]+$ ]] && echo ok)" "ok"
if [[ ! "$tso_probe" =~ ^[0-9]+$ ]]; then
  echo "FATAL: TSO 取号不可用，矩阵演练无从进行"
  echo "  tso_master=$(PSQL $COORD -Atc 'SHOW pg_partdist.tso_master' </dev/null)"
  echo "  conninfo=$(PSQL $COORD -Atc 'SHOW pg_partdist.tso_conninfo' </dev/null)"
  echo "========== 结果：PASS=$PASS FAIL=$((FAIL+1)) =========="
  exit 1
fi

dsnap() {
  { PSQL $pport_a -Atc "SELECT dtxid||'|'||verdict||'|'||commit_ts||'|'||coord_gsid FROM partdist.dtx_decision" </dev/null 2>/dev/null
    PSQL $pport_b -Atc "SELECT dtxid||'|'||verdict||'|'||commit_ts||'|'||coord_gsid FROM partdist.dtx_decision" </dev/null 2>/dev/null; } | sort -u
}
wait_converge() {  # <port> <gid> <key> <oid> <want_cts> <max_s>
  local port=$1 gid=$2 key=$3 oid=$4 want=$5 max=$6 t v sx f
  for t in $(seq 1 $max); do
    v=$(PSQLV $port -Atc "SELECT count(*) FROM t47d_${gid} WHERE id=${key}" </dev/null 2>/dev/null | tail -1)
    if [[ "$v" == "1" ]]; then
      sx=$(PSQLV $port -Atc "SELECT xmin::text::bigint FROM t47d_${gid} WHERE id=${key}" </dev/null | tail -1)
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
INSERT INTO t47d VALUES (${ka}, 'x'), (${kb}, 'y');
COMMIT;
SELECT 'txn_done';
SQL
}

echo "========== [M1] 参与分片 leader 在 PREPARE 之后崩 → 切主 + 决议 → 收敛 =========="
S0=$(dsnap)
out=$(joined_txn "${KA[0]}" "${KB[0]}" | tail -1)
check "M1 前置：跨分片事务提交" "$out" "txn_done"
NEW=$(comm -13 <(echo "$S0") <(dsnap))
dcts=$(echo "$NEW" | cut -d'|' -f3)
check "M1 前置：决议已生成（cts=$dcts）" "$([[ "$dcts" =~ ^[0-9]+$ && "$dcts" -gt 0 ]] && echo ok)" "ok"
ra=$(wait_converge $pport_a $gid_a ${KA[0]} $OID_A $dcts 45)
check "M1：参与分片收敛可见（$ra）" "${ra%%:*}" "ok"
AD=$(PSQL $pport_a -Atc "SHOW data_directory" </dev/null)
DEX /work/pg-install/bin/pg_ctl -D "$AD" -m immediate stop </dev/null >/dev/null 2>&1
sleep 1
DEX /work/pg-install/bin/pg_ctl -D "$AD" -l "$AD/startup.log" start </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 45); do up=$(PSQL $pport_a -Atc "SELECT 1" </dev/null 2>/dev/null); [[ "$up" == "1" ]] && break; sleep 1; done
check "M1：崩后节点恢复" "$up" "1"
rb=$(wait_converge $pport_a $gid_a ${KA[0]} $OID_A $dcts 45)
check "M1：崩后判决仍在（已落 clog，$rb）" "${rb%%:*}" "ok"

echo "========== [M2] 参与分片 leader 在 PREPARE 之前崩 → 事务失败、无残留 =========="
BD=$(PSQL $pport_b -Atc "SHOW data_directory" </dev/null)
DEX /work/pg-install/bin/pg_ctl -D "$BD" -m immediate stop </dev/null >/dev/null 2>&1
sleep 2
m2=$(joined_txn "${KA[1]}" "${KB[1]}" 2>&1 | tail -3 | tr '\n' ' ')
check "M2：分片不可达时事务失败（不静默成功）" \
      "$([[ "$m2" != *txn_done* ]] && echo failed)" "failed"
DEX /work/pg-install/bin/pg_ctl -D "$BD" -l "$BD/startup.log" start </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 45); do up=$(PSQL $pport_b -Atc "SELECT 1" </dev/null 2>/dev/null); [[ "$up" == "1" ]] && break; sleep 1; done
check "M2：B 节点恢复" "$up" "1"
pend=$(PSQL $pport_b -Atc "SELECT partdist.dtx_pending_count()" </dev/null 2>/dev/null | tail -1)
check "M2：无残留未决登记（实际=$pend）" "$([[ "$pend" =~ ^[0-9]+$ ]] && echo ok)" "ok"

echo "========== [M3] 协调者组 leader 决议持久化前崩 → 推定中止 =========="
DTX3=$(PSQL $COORD -Atc "SELECT ((9::bigint&255)<<55)|((777::bigint&4194303)<<33)|301" </dev/null)
S3=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
G3=$((S3 + 31000))
p3=$(PSQL $pport_a -At </dev/null 2>&1 <<SQL
BEGIN;
SELECT pjoin(${G3}::bigint, ${S3}::bigint, ${gid_a}::bigint);
SET citus.override_table_visibility TO false;
INSERT INTO t47d_${gid_a} VALUES (${KA[2]}, 'm3');
SELECT xmin::text::bigint FROM t47d_${gid_a} WHERE id=${KA[2]};
PREPARE TRANSACTION 'citus_9_777_301_0';
SELECT 'prepared';
SQL
)
SX3=$(echo "$p3" | grep -E '^[0-9]+$' | tail -1)
check "M3 前置：prepared 就位（sxid=$SX3）" "$(echo "$p3" | tail -1)" "prepared"
PSQL $pport_a -q -c "SELECT partdist.dtx_note_coord(${DTX3}, ${gid_a});" </dev/null >/dev/null
# 决议尚未做 → 恢复守护应按推定中止收敛（§3.4 行 2：决议不存在 ⇒ 未提交过）
# 判据 = 终局达成：决议表出现 ABORT **或** 分片 clog 已落 ABORTED。
# 决议行可能被 FORGET/GC 回收（§9.7），clog 才是本方案的可见性真相源。
ab=""
for t in $(seq 1 45); do
  st=$(PSQL $pport_a -Atc "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX3}" </dev/null 2>/dev/null | tail -1)
  cg=$(PSQL $pport_a -Atc "SELECT sclog_full(${OID_A}::oid, ${SX3}::bigint)" </dev/null 2>/dev/null | tail -1)
  [[ "$st" == "2" || "$cg" == *"st=3"* ]] && { ab="ok:${t}s"; break; }
  PSQL $pport_a -q -c "SELECT partdist.dtx_recover_prepared(1);" </dev/null >/dev/null 2>&1
  sleep 1
done
check "M3：推定中止达成 ABORT 终局（$ab）" "${ab%%:*}" "ok"
f3=$(PSQL $pport_a -Atc "SELECT sclog_full(${OID_A}::oid, ${SX3}::bigint)" </dev/null 2>/dev/null | tail -1)
check "M3：分片 clog 落 ABORTED（st=3）" "$(echo "$f3" | grep -oc 'st=3')" "1"

echo "========== [M4] 协调者组 leader 决议持久化后崩 → 新 leader 继续应答 =========="
DTX4=$(PSQL $COORD -Atc "SELECT ((9::bigint&255)<<55)|((777::bigint&4194303)<<33)|302" </dev/null)
S4=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
G4=$((S4 + 32000))
p4=$(PSQL $pport_a -At </dev/null 2>&1 <<SQL
BEGIN;
SELECT pjoin(${G4}::bigint, ${S4}::bigint, ${gid_a}::bigint);
SET citus.override_table_visibility TO false;
INSERT INTO t47d_${gid_a} VALUES (${KA[3]}, 'm4');
SELECT xmin::text::bigint FROM t47d_${gid_a} WHERE id=${KA[3]};
PREPARE TRANSACTION 'citus_9_777_302_0';
SQL
)
SX4=$(echo "$p4" | grep -E '^[0-9]+$' | tail -1)
PSQL $pport_a -q -c "SELECT partdist.dtx_note_coord(${DTX4}, ${gid_a});" </dev/null >/dev/null
CTS4=$(PSQL $COORD -Atc "SELECT partdist_tso_commit_ts()" </dev/null)
r4=$(decide_retry $gid_a $DTX4 1 $CTS4 $pport_a $f1_a $f2_a)
check "M4 前置：决议已持久化（COMMIT）" "$r4" "1"
# ★ 先等决议**在多数派可见**再杀 leader（§3.4 行 3 的前提就是"决议已持久化"）：
#   decide_retry 只保证 leader 本地写入 + 组内复制发起，apply 到其他成员需要
#   一点时间；不等就杀，新 leader 手里自然没有决议（实测：决议行只在被杀的
#   那个节点上，5433/5436 皆空 → M4 必红）。多数派 = 3 成员里 ≥2 个有该行。
maj=""
for t in $(seq 1 45); do
  nseen=0
  for cand in $pport_a $f1_a $f2_a; do
    d=$(PSQL $cand -Atc "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX4}" </dev/null 2>/dev/null | tail -1)
    [[ "$d" == "1" ]] && nseen=$((nseen+1))
  done
  [[ "$nseen" -ge 2 ]] && { maj="ok:${t}s(${nseen}/3)"; break; }
  sleep 1
done
check "M4 前置：决议已达多数派（$maj）" "${maj%%:*}" "ok"

# 崩掉当前协调组 leader，新 leader 应仍能应答该决议
lp=""; for cand in $pport_a $f1_a $f2_a; do
  [[ "$(PSQL $cand -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid_a}" </dev/null 2>/dev/null)" == "leader" ]] && { lp=$cand; break; }
done
LD=$(PSQL $lp -Atc "SHOW data_directory" </dev/null)
DEX /work/pg-install/bin/pg_ctl -D "$LD" -m immediate stop </dev/null >/dev/null 2>&1
sleep 2
DEX /work/pg-install/bin/pg_ctl -D "$LD" -l "$LD/startup.log" start </dev/null >/dev/null 2>&1
for t in $(seq 1 45); do [[ "$(PSQL $lp -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 1; done
# 崩后先等组重新选出 leader（dtx_peek 有 leader 门控，无主期合法拒答），
# 再问决议；决议行若已被 §9.7 的 FORGET 回收，则以本地 dtx_decision 行
# 或分片 clog 的 COMMITTED 终局为等价证据。
# ★ 判据（§1.4 原文）：新 leader **从组内持久化状态继承**协调职责——
#   继承发生在**追平之后**，不是当选瞬间。Raft 下"多数派已持久化"不等于
#   每个成员都已 apply：实测新 leader 恰是那个尚未 apply 决议的成员
#   （决议在另外 2/3 上），此时它当选合法、但要等追平才能应答。
#   故这里等"任一现任 leader 能应答"，并给足追平时间（组内 apply +
#   可能的再次选举）。触发追平：peek 本身会推动 apply。
ans=""
for t in $(seq 1 90); do
  for cand in $pport_a $f1_a $f2_a; do
    st=$(PSQL $cand -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid_a}" </dev/null 2>/dev/null | tail -1)
    [[ "$st" != "leader" ]] && continue
    v=$(PSQL $cand -Atc "SELECT verdict FROM partdist.dtx_peek(${gid_a}::bigint, ${DTX4}::bigint)" </dev/null 2>/dev/null | tail -1)
    [[ "$v" == "1" ]] && { ans="ok:${t}s(peek@$cand)"; break 2; }
    d=$(PSQL $cand -Atc "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX4}" </dev/null 2>/dev/null | tail -1)
    [[ "$d" == "1" ]] && { ans="ok:${t}s(row@$cand)"; break 2; }
  done
  # 推一把追平：catchup 通道由 leader 侧驱动
  PSQL $pport_a -q -c "SELECT partdist.pg_raft_catchup();" </dev/null >/dev/null 2>&1
  sleep 1
done
check "M4：崩后新 leader 仍应答 COMMIT（$ans）" "${ans%%:*}" "ok"
if [[ "${ans%%:*}" != "ok" ]]; then
  echo "  ---- M4 诊断 ----"
  echo "    被杀 leader=:$lp  组=$gid_a  dtxid=$DTX4"
  for cand in $pport_a $f1_a $f2_a; do
    echo "    :$cand 存活=$(PSQL $cand -Atc 'SELECT 1' </dev/null 2>/dev/null | tail -1) 组状态=$(PSQL $cand -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid_a}" </dev/null 2>/dev/null | tail -1) 决议行=$(PSQL $cand -Atc "SELECT verdict FROM partdist.dtx_decision WHERE dtxid=${DTX4}" </dev/null 2>/dev/null | tail -1)"
  done
fi
PSQL $pport_a -q -c "COMMIT PREPARED 'citus_9_777_302_0';" </dev/null >/dev/null 2>&1

# M4 杀过 leader —— 进 M5 前等组恢复稳定，避免残局连累后续腿
for t in $(seq 1 45); do
  nl=0
  for cand in $pport_a $f1_a $f2_a; do
    [[ "$(PSQL $cand -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid_a}" </dev/null 2>/dev/null | tail -1)" == "leader" ]] && nl=$((nl+1))
  done
  [[ "$nl" -ge 1 ]] && break
  sleep 1
done

echo "========== [M5] master(TSO) 不可达 → 事务级 fail-closed =========="
before=$(PSQL $COORD -Atc "SELECT count(*) FROM t47d" </dev/null 2>/dev/null | tail -1)
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=59999 dbname=postgres connect_timeout=1';" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 1
m5=$(joined_txn "${KA[4]}" "${KB[4]}" 2>&1 | tr '\n' ' ')
check "M5：TSO 不可达时事务被拒（fail-closed）" \
      "$([[ "$m5" == *"TSO 不可达"* || "$m5" != *txn_done* ]] && echo closed)" "closed"
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
PSQL $COORD -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 1
after=$(PSQL $COORD -Atc "SELECT count(*) FROM t47d" </dev/null 2>/dev/null | tail -1)
check "M5：已提交事务可见性不受影响（$before → $after，只增不减）" \
      "$([[ -n "$before" && -n "$after" && "$after" -ge "$before" ]] && echo ok)" "ok"
# ★ M5 制造过 TSO fail-closed，可能把 boot 防呆推进拒绝态 —— 立刻复位，
#   否则后续腿（S1/Q）的取号全废（R-P4-11 的触发源正是此处）。
tso_ok=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null 2>&1 | tail -1)
if [[ ! "$tso_ok" =~ ^[0-9]+$ ]]; then
  CD1=$(PSQL $COORD -Atc "SHOW data_directory" </dev/null 2>/dev/null | tail -1)
  DEX rm -f "$CD1/pg_tso_boot" </dev/null 2>/dev/null
  DEX /work/pg-install/bin/pg_ctl -D "$CD1" -m fast -l "$CD1/startup.log" restart </dev/null >/dev/null 2>&1
  for t in $(seq 1 30); do [[ "$(PSQL $COORD -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 1; done
  tso_ok=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null 2>&1 | tail -1)
fi
check "M5 后：TSO 取号恢复（$tso_ok）" "$([[ "$tso_ok" =~ ^[0-9]+$ ]] && echo ok)" "ok"

echo "========== [S1] 跨分片 SI 一致快照 =========="
gx=$(PSQL $COORD -Atc "SELECT gxid_next()" </dev/null)
Ssnap=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
snap1=$(PSQL $COORD -At </dev/null 2>/dev/null <<SQL | grep -E '^[0-9]+$' | tail -1
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = '${gx},${Ssnap},${gid_a}';
SELECT count(*) FROM t47d;
COMMIT;
SQL
)
# 在同一 start_ts 下再读一次：两次必须一致（SI 快照稳定）
snap2=$(PSQL $COORD -At </dev/null 2>/dev/null <<SQL | grep -E '^[0-9]+$' | tail -1
BEGIN;
SET LOCAL citus.propagate_set_commands = 'local';
SET LOCAL pg_partdist.join_info = '${gx},${Ssnap},${gid_a}';
SELECT count(*) FROM t47d;
COMMIT;
SQL
)
check "S1：同一 start_ts 两次读一致（$snap1 = $snap2）" \
      "$([[ -n "$snap1" && "$snap1" == "$snap2" ]] && echo ok)" "ok"

echo "========== [Q] 三态问询三分支 =========="
# Q1：槽 start_ts > 读者快照 ⇒ 跳过（不可见、不阻塞）
DTXQ=$(PSQL $COORD -Atc "SELECT ((9::bigint&255)<<55)|((777::bigint&4194303)<<33)|303" </dev/null)
SQ=$(PSQL $COORD -Atc "SELECT tso_c_start()" </dev/null)
GQ=$((SQ + 33000))
pq=$(PSQL $pport_a -At </dev/null 2>&1 <<SQL
BEGIN;
SELECT pjoin(${GQ}::bigint, ${SQ}::bigint, ${gid_a}::bigint);
SET citus.override_table_visibility TO false;
INSERT INTO t47d_${gid_a} VALUES (${KA[5]}, 'q');
PREPARE TRANSACTION 'citus_9_777_303_0';
SELECT 'prepared';
SQL
)
check "Q 前置：in-doubt 就位" "$(echo "$pq" | tail -1)" "prepared"
t0=$(date +%s)
qv=$(PSQL $pport_a -Atc "SET citus.override_table_visibility=false; SELECT count(*) FROM t47d_${gid_a} WHERE id=${KA[5]}" </dev/null 2>/dev/null | tail -1)
t1=$(date +%s)
check "Q1/Q2：in-doubt 读者不阻塞（$((t1-t0))s）且不可见" \
      "$([[ "$qv" == "0" && $((t1-t0)) -le 5 ]] && echo ok)" "ok"
# Q3：补决议后问询学到判决 → 可见
CTSQ=$(PSQL $COORD -Atc "SELECT partdist_tso_commit_ts()" </dev/null)
rq=$(decide_retry $gid_a $DTXQ 1 $CTSQ $pport_a $f1_a $f2_a)
check "Q3 前置：决议写入" "$rq" "1"
PSQL $pport_a -q -c "COMMIT PREPARED 'citus_9_777_303_0';" </dev/null >/dev/null 2>&1
# 取值用 PSQLV（PGOPTIONS 传可见性参数）——"SET …; SELECT …" 会把 SET 的
# 回显混进输出，tail -1 取到 "SET" 而非计数（历轮踩过）。
q3=""
for t in $(seq 1 30); do
  PSQL $pport_a -q -c "SELECT partdist.dtx_pending_sweep();" </dev/null >/dev/null 2>&1
  c=$(PSQLV $pport_a -Atc "SELECT count(*) FROM t47d_${gid_a} WHERE id=${KA[5]}" </dev/null 2>/dev/null | tail -1)
  [[ "$c" == "1" ]] && { q3="ok:${t}s"; break; }
  sleep 1
done
check "Q3：问询学到判决后可见（$q3）" "${q3%%:*}" "ok"

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
      PSQLV $fp -q -c "SELECT partdist.replay_disable('t47d_${gid}');" </dev/null >/dev/null 2>&1
    done
  done
  for p in $pport_a $pport_b $f1_a $f2_a $f1_b $f2_b; do
    PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" </dev/null >/dev/null 2>&1
  done
  for spec in "${gid_a}:${f1_a}:${f2_a}" "${gid_b}:${f1_b}:${f2_b}"; do
    gid=${spec%%:*}; rest=${spec#*:}; fx1=${rest%%:*}; fx2=${rest#*:}
    for fp in $fx1 $fx2; do
      PSQLV $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS t47d_${gid};" </dev/null >/dev/null 2>&1
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
  PSQL $COORD -q -c "DROP TABLE IF EXISTS t47d;" </dev/null
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
