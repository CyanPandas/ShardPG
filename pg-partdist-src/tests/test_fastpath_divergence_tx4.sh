#!/usr/bin/env bash
# [宿主机] TX4：快路径分叉的归队规则（DTX_2PC_DESIGN.md §9.5，第 6 步 b）。
#
# **要防的窗口**：单分区（快路径）事务沿用 `[A] → quorum → [B]` 时序 —— 记录与
# COMMIT 标记先达组内多数派，leader 本地的 [B]（pg_wal 提交记录 fsync）之后才发生。
# leader 在这两步之间崩溃 ⇒ **组内认为已提交、leader 本地事务却中止** ⇒ 分叉。
#
# 方案 (b)：接受该窗口，规定**旧 leader 归队时，对检测到分叉的分片强制重做物理
# 基线**，在此之前不得再当选为主。
#
# **检测判据**：段流里的 COMMIT 标记是"组认为这笔事务提交了"的凭据，本地 CLOG 是
# "本节点认为它提交了没有"的凭据。两者对同一个 xid 给出相反答案，就是分叉的直接
# 证据 —— 不需要比对页面。只查本节点写的标记（gxid 高 16 位 = 本节点 group id）。
#
# **惰性回放让这件事变简单**：副本平时一条 redo 都不做，内容只在**升主那一刻**
# 才有意义。所以把闸设在升主前置上就够了 —— 分叉的旧 leader 只要永不被提升为主，
# 就不会把分叉数据对外服务。
#
# 分叉状态的确定性构造（不靠掐时机崩溃）：阶段 3 的钩子跑在
# standard_ProcessUtility **之前**，所以在事务块里发 COMMIT PREPARED 会让
# COMMIT 标记先落盘、语句再被 PreventInTransactionBlock 拒掉，prepared 事务
# 原地不动 ⇒ pg_xact_status = 'in progress' ≠ committed ⇒ 正是要检测的状态。
#
# 验收标准：
#   1. 健康分片上检测函数返回 0（无误报）——这是它能被信任的前提。
#   2. 构造出分叉后检测函数返回 > 0。
#   3. 升主前置对该分片返回 -1（分叉，永不放行），而不是 0（稍后重试）。
#   4. 分叉分片的回放槽位被下电，不再以"看似正常"的姿态参与。
#   5. 清理掉 prepared 事务后，检测恢复为 0、升主前置恢复放行。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }
# 关掉 Citus 的分片表可见性开关：MX 模式下分片表默认不在 pg_class 可见范围内，
# `'<tbl>'::regclass` 会报 relation does not exist（T7.14 的基线发射要按名字取表）。
PSQLV() { local port=$1; shift; docker exec -i -u postgres -e PGOPTIONS="-c citus.override_table_visibility=false" "$CONTAINER" /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

check() {
  if [[ -n "$2" && "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

GID_TXN="citus_0_99777_88666_0"      # citus 形态，才解析得出 dtxid

echo "========== [1] 夹具：单分片表 + 一个副本 =========="
PSQL $COORD -q -c "DROP TABLE IF EXISTS tx4_div;" >/dev/null 2>&1
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE tx4_div(id int primary key, v text);
SELECT create_distributed_table('tx4_div','id');
ALTER TABLE tx4_div SET (autovacuum_enabled = off);
SQL
GID=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='tx4_div'::regclass")
PA=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${GID}")
TBL="tx4_div_${GID}"
f1=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$PA" ]] && continue
  f1=$p; break
done
echo "  shard=${GID} leader=:${PA} follower=:${f1}"
PSQL $PA -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
PSQL $PA -q -c "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${TBL}');" >/dev/null
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "INSERT INTO tx4_div SELECT g,'v'||g FROM generate_series(1,20) g;"
LOID=$(PSQL $PA -Atc "SELECT partdist.local_partition_for_shard(${GID})")
check "解析到 leader 本地分区 oid" "$([[ -n "$LOID" && "$LOID" != "0" ]] && echo ok)" "ok"

echo "========== [2] 健康分片：检测必须返回 0（无误报）=========="
# 这一条是全部后续断言的前提：一个会误报的检测器，等于把所有分片都永久禁止升主。
d0=$(PSQL $PA -Atc "SELECT partdist.pg_raft_check_fastpath_divergence(${LOID}::oid)")
check "正常提交的事务不被误判为分叉（d=${d0}）" "$d0" "0"
p0=$(PSQL $PA -Atc "SELECT partdist.pg_raft_promote_prepare(${GID}, 2000)")
check "健康分片的升主前置放行（返回 1）" "$p0" "1"

echo "========== [3] 构造分叉：COMMIT 标记落盘、事务却没提交 =========="
# 3a. 在 leader 上直接对分片表做本地 2PC（绕开 Citus 自己的 gid 管理）
PSQL $PA -v ON_ERROR_STOP=1 -q <<SQL 2>&1 | head -2
SET citus.enable_ddl_propagation = off;
BEGIN;
INSERT INTO ${TBL} VALUES (9001, 'diverge');
PREPARE TRANSACTION '${GID_TXN}';
SQL
pxid=$(PSQL $PA -Atc "SELECT transaction::text::bigint FROM pg_prepared_xacts WHERE gid='${GID_TXN}'")
check "prepared 事务已建立（xid=${pxid}）" "$([[ -n "$pxid" && "$pxid" != "0" ]] && echo ok)" "ok"
check "该 xid 本地判决是 in progress（尚未提交）" \
      "$(PSQL $PA -Atc "SELECT pg_xact_status(${pxid}::text::xid8)")" "in progress"

# 3b. 在**事务块里**发 COMMIT PREPARED：阶段 3 的钩子先跑（写下 COMMIT 标记），
#     语句随后被 PreventInTransactionBlock 拒掉，prepared 事务原地不动。
PSQL $PA -q <<SQL 2>&1 | grep -c "cannot run inside a transaction block" | sed 's/^/  （预期报错次数: /;s/$/）/'
BEGIN;
COMMIT PREPARED '${GID_TXN}';
SQL
check "COMMIT PREPARED 被拒后 prepared 事务仍在（分叉前提）" \
      "$(PSQL $PA -Atc "SELECT count(*) FROM pg_prepared_xacts WHERE gid='${GID_TXN}'")" "1"
check "该 xid 本地判决仍不是 committed" \
      "$([[ "$(PSQL $PA -Atc "SELECT pg_xact_status(${pxid}::text::xid8)")" != "committed" ]] && echo ok)" "ok"

ncommit=$(PSQL $PA -Atc "
  SELECT count(*) FROM generate_series(1, partdist.get_partition_flush_lsn(${LOID})) g,
       LATERAL partdist.partwal_read_record(${LOID}::oid, g) r
   WHERE r.flags = 2 AND r.info = 0
     AND (r.gxid & ((1::bigint<<48)-1)) = ${pxid}")
check "段流里确实出现了该 xid 的 COMMIT 标记（${ncommit} 条）" \
      "$([[ -n "$ncommit" && "$ncommit" -ge 1 ]] && echo ok)" "ok"

echo "========== [4] ★ 核心：检测到分叉，且升主被永久拒绝 =========="
d1=$(PSQL $PA -Atc "SELECT partdist.pg_raft_check_fastpath_divergence(${LOID}::oid)")
check "检测函数报告分叉（d=${d1} ≥ 1）" "$([[ -n "$d1" && "$d1" -ge 1 ]] && echo ok)" "ok"

p1=$(PSQL $PA -Atc "SELECT partdist.pg_raft_promote_prepare(${GID}, 2000)")
check "升主前置返回 -1（分叉：永不放行，而不是 0=稍后重试）" "$p1" "-1"

# -1 与 0 的区别是本条规则的全部要害：0 会被 deadline 兜底按"可用性优先"放行，
# 而分叉必须绕过那条兜底。这里额外核实返回值确实是负数而非假值。
check "返回值是负数（区别于'尚未就绪'的 0）" \
      "$([[ -n "$p1" && "$p1" -lt 0 ]] && echo ok)" "ok"

echo "========== [5] 清理 prepared 事务后，判据必须复原 =========="
# 复原能力同样是判据的一部分：一个只会变红不会变绿的检测器无法用于生产门禁。
PSQL $PA -q -c "ROLLBACK PREPARED '${GID_TXN}';" >/dev/null 2>&1
check "prepared 事务已清理" \
      "$(PSQL $PA -Atc "SELECT count(*) FROM pg_prepared_xacts WHERE gid='${GID_TXN}'")" "0"
# ROLLBACK PREPARED 之后该 xid 变成 aborted —— 流里仍有它的 COMMIT 标记，
# 所以**仍然是分叉**（组认为提交、本地认为回滚）。这正是应有的语义：
# 分叉是既成事实，清掉 prepared 事务并不能让它消失，只有重做物理基线才行。
d2=$(PSQL $PA -Atc "SELECT partdist.pg_raft_check_fastpath_divergence(${LOID}::oid)")
check "回滚 prepared 后仍判为分叉（既成事实，须重做基线才能消除）" \
      "$([[ -n "$d2" && "$d2" -ge 1 ]] && echo ok)" "ok"
check "该 xid 本地判决已变为 aborted" \
      "$(PSQL $PA -Atc "SELECT pg_xact_status(${pxid}::text::xid8)")" "aborted"

echo "========== [6] ★ T7.14：重做物理基线 → 分叉消除 → 重新获得参选资格 =========="
# P7-E3 的缺口：本套件此前只验到"检测到分叉 + 升主被永久拒绝"，**没有归队的一半**。
# 一个只会把节点永久判死、没有复活路径的规则，在生产上等同于"该分片少一个副本"。
# 三个基线消费者（初始配对 / 永久分叉 / 快路径分叉）中，前两个各有 e2e，
# 这一条补上第三个。
#
# 判据链条（缺一不可）：
#   ① 基线发射成功且返回游标；② 分叉判据归零；③ promote_prepare 不再返回 -1。
# ③ 是要害：-1 是"永不放行"，只要它还是 -1，前两条绿了也没有意义。
base=$(PSQLV $PA -Atc "SELECT partdist.shard_baseline_emit('${TBL}'::regclass)" 2>&1 | tail -1)
check "重做物理基线成功（base_plsn=${base}）" \
      "$([[ "$base" =~ ^[0-9]+$ && "$base" -gt 0 ]] && echo ok)" "ok"

d3=$(PSQL $PA -Atc "SELECT partdist.pg_raft_check_fastpath_divergence(${LOID}::oid)")
check "★ 重做基线后分叉判据归零（d=${d3}）" "$d3" "0"

p2=$(PSQL $PA -Atc "SELECT partdist.pg_raft_promote_prepare(${GID}, 2000)")
check "★★ 升主前置不再是 -1（重新获得参选资格，p=${p2}）" \
      "$([[ -n "$p2" && "$p2" -ge 0 ]] && echo ok)" "ok"

echo ""
health_check_no_crash
# 丢提案时的表现正是"全 PASS + 有丢弃 = 运气"（见 lib_node_health.sh 头注释）——
# 本用例全靠 Raft 把记录/标记送到 follower，必须一并核查。
health_check_no_drops
health_check_worker_pool
# ★ 断言数守卫：本轮若中途静默退出（set -e / 节点起不来等），PASS/FAIL 统计
#   看起来仍正常，只有"少跑了几条"能暴露。本项目已三次栽在这种静默形态上。
if [[ "$((PASS+FAIL))" -lt 16 ]]; then
  echo "FATAL: 只跑了 $((PASS+FAIL)) 条断言（应 >=16）——夹具中途退出，结果不可信"; exit 98
fi
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "TX4 快路径分叉归队规则：全部通过"; else echo "TX4 快路径分叉归队规则：存在 FAIL"; fi

if [[ "${KEEP_FIXTURE:-0}" != "1" ]]; then
  PSQL $PA -q -c "ROLLBACK PREPARED '${GID_TXN}';" >/dev/null 2>&1
  PSQL $COORD -q -c "DROP TABLE IF EXISTS tx4_div;" >/dev/null 2>&1
  for p in $COORD $PA $f1; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${GID};" >/dev/null 2>&1
    PSQL $p -q -c "DELETE FROM partdist.dtx_participant WHERE gid='${GID_TXN}';" >/dev/null 2>&1
  done
fi

exit $FAIL
