#!/usr/bin/env bash
# ============================================================================
# [宿主机] P7-N106 验收（2026-10-19）：COLOCATE WITH 时副本数随共置表
#
# 原来只继承分片数；不写 REPLICAS 取 shardpg.replicas（2），实测 1 从的表和 COLOCATE WITH 它的表
# 每片 2 从 ⇒ 同一段的两个 Raft 组成员不同，共置对齐 / move_replica 都靠不住。
#
#   [1] 被共置的表 t106c：4 片、每片 1 主 1 从（故意和默认的 2 不同）
#   [2] COLOCATE WITH t106c 写 REPLICAS 2：立即报错、表没建出来
#   [3] COLOCATE WITH t106c 不写 REPLICAS：继承成 1 从，4 段的主、从与 t106c 完全相同
#   [4] COLOCATE WITH t106c 写 REPLICAS 1（与共置表相同）：照常建
#   [5] 共置目标是没有 Raft 副本登记的老表：报错
#   [6] 写数据、共置连接；删表零残留
#
# 前提：pg-test 1c+4w。约 2–3 分钟。用法：bash test_colocate_replicas_p7n106.sh
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
PASS=0; FAIL=0
q()   { docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p "${2:-5432}" -U postgres -X -At -c "$1" </dev/null 2>&1; }
run() { docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -q -v ON_ERROR_STOP=1 -c "$1" </dev/null 2>&1; }
ok()  { echo "  ✓ $1"; PASS=$((PASS + 1)); }
no()  { echo "  ✗ $1"; FAIL=$((FAIL + 1)); }
check() { if [[ "$2" == "$3" ]]; then ok "$1（$2）"; else no "$1：实际 [$2]，期望 [$3]"; fi; }
has()   { grep -qF -- "$3" <<<"$2" && ok "$1" || no "$1：输出里没有 [$3]：$(tail -3 <<<"$2")"; }
# 一张表各分片的 主|从，按分片号排
layout() { q "SELECT string_agg(\"primary\" || '|' || replicas, ' ' ORDER BY shard_name) FROM shardpg.shards WHERE table_name = '$1'::regclass"; }
cleanup() { run "DROP TABLE IF EXISTS t106o2, t106o, t106c, t106bad" >/dev/null; }
trap cleanup EXIT

echo "================ [0] 前提 ================"
ts=$(q "SELECT partdist.partdist_tso_client_start_ts()")
[[ "$ts" =~ ^[0-9]+$ ]] || { echo "TSO 不可用，停止测试"; exit 1; }
cleanup
check "Citus worker 数" "$(q "SELECT count(*) FROM pg_dist_node WHERE groupid <> 0")" "4"

echo "================ [1] 被共置的表：每片 1 主 1 从 ================"
out=$(run "CREATE TABLE t106c (w int NOT NULL, id int NOT NULL, name text, PRIMARY KEY (w, id), SHARD KEY (w) SHARDS 4 REPLICAS 1)")
has "t106c 建组 1 主 1 从" "$out" "建组：每个分片 1 主 1 从"
check "t106c 每片 1 从" "$(q "SELECT string_agg(DISTINCT cardinality(secondary_nodes)::text, ',') FROM partdist.partition_map m JOIN pg_dist_shard s ON s.shardid = m.partition_id::bigint WHERE s.logicalrelid = 't106c'::regclass")" "1"

echo "================ [2] 共置时写了不同的副本数 ================"
out=$(run "CREATE TABLE t106o (w int NOT NULL, id int NOT NULL, cid int NOT NULL, PRIMARY KEY (w, id), SHARD KEY (w) REPLICAS 2 COLOCATE WITH t106c)")
has "报错" "$out" "COLOCATE WITH 时副本数随共置表：t106c 是每片 1 从，这里写的是 REPLICAS 2"
has "提示改法" "$out" "去掉 REPLICAS，或者写成 REPLICAS 1"
check "表没建出来" "$(q "SELECT count(*) FROM pg_class WHERE relname = 't106o'")" "0"

echo "================ [3] 共置时不写 REPLICAS：继承 ================"
out=$(run "CREATE TABLE t106o (w int NOT NULL, id int NOT NULL, cid int NOT NULL, PRIMARY KEY (w, id), SHARD KEY (w) COLOCATE WITH t106c)")
has "与 t106c 共置" "$out" "与 t106c 共置"
has "建组 1 主 1 从" "$out" "建组：每个分片 1 主 1 从"
check "同一个共置组" "$(q "SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('t106c'::regclass, 't106o'::regclass)")" "1"
check "4 段的主、从与 t106c 相同" "$(layout t106o)" "$(layout t106c)"

echo "================ [4] 共置时写了相同的副本数 ================"
out=$(run "CREATE TABLE t106o2 (w int NOT NULL, id int NOT NULL, PRIMARY KEY (w, id), SHARD KEY (w) REPLICAS 1 COLOCATE WITH t106c)")
has "照常建，1 主 1 从" "$out" "建组：每个分片 1 主 1 从"
check "4 段的主、从与 t106c 相同" "$(layout t106o2)" "$(layout t106c)"

echo "================ [5] 共置目标没有 Raft 副本登记 ================"
legacy=$(q "SELECT p.logicalrelid::text FROM pg_dist_partition p WHERE p.partmethod = 'h' AND NOT EXISTS (SELECT 1 FROM pg_dist_shard s JOIN partdist.partition_map m ON m.partition_id::bigint = s.shardid WHERE s.logicalrelid = p.logicalrelid) ORDER BY 1 LIMIT 1")
if [[ -n "$legacy" ]]; then
  out=$(run "CREATE TABLE t106bad (k int PRIMARY KEY, SHARD KEY (k) COLOCATE WITH $legacy)")
  has "共置老表 $legacy 报错" "$out" "没有登记从副本，不能共置"
  check "表没建出来" "$(q "SELECT count(*) FROM pg_class WHERE relname = 't106bad'")" "0"
else
  echo "  - 环境里没有不带 Raft 副本的哈希分布表，跳过"
fi

echo "================ [6] 写数据、共置连接、删表 ================"
run "INSERT INTO t106c SELECT w, i, 'c' || w || '_' || i FROM generate_series(1, 4) w, generate_series(1, 3) i" >/dev/null || exit 1
run "INSERT INTO t106o SELECT w, i, 1 + i % 3 FROM generate_series(1, 4) w, generate_series(1, 5) i" >/dev/null || exit 1
check "共置连接：12 个客户、20 张订单" "$(q "SELECT count(DISTINCT (c.w, c.id)) || '/' || count(*) FROM t106c c JOIN t106o o ON o.w = c.w AND o.cid = c.id")" "12/20"
ids=$(q "SELECT string_agg(shardid::text, ',') FROM pg_dist_shard WHERE logicalrelid IN ('t106c'::regclass, 't106o'::regclass, 't106o2'::regclass)")
out=$(run "DROP TABLE t106o2, t106o, t106c")
has "删表清理" "$out" "清理：已删除主从登记"
check "控制面登记清掉" "$(q "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN ($ids)")" "0"
bad=0
for p in 5433 5434 5435 5436; do
  n=$(q "SET citus.override_table_visibility = off; SELECT count(*) FROM pg_class WHERE relname ~ '^t106'" "$p" | tail -1)
  [[ "$n" == 0 ]] || { bad=1; echo "      :$p 还有 $n 张 t106 表"; }
done
check "4 台 worker 上零残留" "$bad" "0"

echo
echo "================ 结果：$PASS 通过 / $FAIL 失败 ================"
[[ $FAIL -eq 0 ]]
