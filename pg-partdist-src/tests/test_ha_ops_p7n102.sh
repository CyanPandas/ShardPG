#!/usr/bin/env bash
# ============================================================================
# [宿主机] P7-N94 / N95 / N100 / N101 / N102 验收（2026-10-17）
#
#   [1] 建两张共置的 SHARD KEY 表（4 片、1 主 2 从），写数据
#   [2] N95  主分片和从分片都是 autovacuum_enabled=off
#   [3] N100 不在事务块里的多分片 SELECT：每个分支都带 BEGIN + 同一份 partdist_join_global_txn
#   [4] N94  人为把一个分片的主转交走（与共置分片分开），等共置对齐把它转回来；共置连接正常
#   [5] N102 准备新节点 + shardpg.add_node：6 个节点的 pg_raft.peers / 组 0 成员一致
#   [6] N102 shardpg.move_replica：共置段的一份从副本搬到新节点，成员、登记、源节点清理都对
#   [7]      shardpg.move_primary 到新节点（领导权转交）：读写、共置连接在新节点上，数据不少
#   [8]      删表：6 个节点上零残留
#   [9] N101 ha_demo_reset.sh 去掉新节点（整簇重启，有数据）：回到 5 节点、TSO 接着发号
#
# 前提：pg-test 5 节点（:5432 master + :5433–:5436），没有 :5437；会建 / 删表 t102c、t102o。
# 约 10–15 分钟。用法：bash test_ha_ops_p7n102.sh
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
HERE=$(cd "$(dirname "$0")" && pwd)
DEMO="${DEMO:-$HERE/../demo}"
PASS=0; FAIL=0
q()  { docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p "${2:-5432}" -U postgres -X -At -c "$1" </dev/null 2>&1; }
ok() { echo "  ✓ $1"; PASS=$((PASS + 1)); }
no() { echo "  ✗ $1"; FAIL=$((FAIL + 1)); }
check() { if [[ "$2" == "$3" ]]; then ok "$1（$2）"; else no "$1：实际 [$2]，期望 [$3]"; fi; }
wait_for() {   # wait_for 秒 描述 SQL 期望
  local t; for t in $(seq 1 "$1"); do [[ "$(q "$3")" == "$4" ]] && { ok "$2（${t} s 内）"; return 0; }; sleep 1; done
  no "$2：$1 s 内没等到 [$4]，最后是 [$(q "$3")]"; return 1
}
cleanup() {
  q "DROP TABLE IF EXISTS t102o, t102c" >/dev/null
  [[ "$(q "SELECT count(*) FROM partdist.node_map WHERE port = 5437")" != 0 ]] && bash "$DEMO/ha_demo_reset.sh" 5437 >/dev/null 2>&1
}

echo "================ [0] 前提 ================"
check "5 个节点" "$(q "SELECT count(*) FROM partdist.node_map")" "5"
check "没有 :5437" "$(q "SELECT count(*) FROM pg_dist_node WHERE nodeport = 5437")" "0"
q "DROP TABLE IF EXISTS t102o, t102c" >/dev/null

echo "================ [1] 建两张共置表、写数据 ================"
out=$(docker exec -i -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -q -v ON_ERROR_STOP=1 2>&1 <<'SQL'
CREATE TABLE t102c (w int, id int, v int NOT NULL DEFAULT 0, PRIMARY KEY (w, id), SHARD KEY (w) SHARDS 4 REPLICAS 2);
CREATE TABLE t102o (w int, id int, c int, PRIMARY KEY (w, id), SHARD KEY (w) COLOCATE WITH t102c);
INSERT INTO t102c SELECT w, i, 10 FROM generate_series(1, 8) w, generate_series(1, 5) i;
INSERT INTO t102o SELECT w, i, i FROM generate_series(1, 8) w, generate_series(1, 5) i;
SQL
)
[[ $? -eq 0 ]] && ok "建表、写 40 + 40 行" || no "建表失败：$(tail -3 <<<"$out")"
check "t102c 行数" "$(q "SELECT count(*) FROM t102c")" "40"

echo "================ [2] N95：主、从分片都关了 autovacuum ================"
nbad=0
for p in 5433 5434 5435 5436; do
  n=$(q "SET citus.override_table_visibility = off; SELECT count(*) FROM pg_class WHERE relname ~ '^t102[co]_[0-9]+\$' AND relkind = 'r' AND NOT coalesce('autovacuum_enabled=off' = ANY (reloptions), false)" "$p" | tail -1)
  nbad=$((nbad + ${n:-99}))
done
check "没关 autovacuum 的分片表" "$nbad" "0"

echo "================ [3] N100：自动提交的多分片 SELECT 用同一份快照 ================"
log=$(docker exec -u postgres "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -At \
      -c "SET citus.log_remote_commands = on" -c "SELECT count(*) FROM t102c" </dev/null 2>&1)
nb=$(grep -c "issuing BEGIN TRANSACTION" <<<"$log")
nj=$(grep -o "partdist_join_global_txn([0-9]*, [0-9]*" <<<"$log" | sort -u | wc -l)
nc=$(grep -c "issuing COMMIT" <<<"$log")
[[ "$nb" -ge 2 ]] && ok "每个分支都开远程事务块（BEGIN × $nb）" || no "BEGIN 只有 $nb 个"
check "各分支的 (gxid, start_ts) 只有一种" "$nj" "1"
[[ "$nc" -ge 2 ]] && ok "结束时普通 COMMIT × $nc" || no "COMMIT 只有 $nc 个"
nb1=$(docker exec -u postgres "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -At \
      -c "SET citus.log_remote_commands = on" -c "SELECT count(*) FROM t102c WHERE w = 1" </dev/null 2>&1 | grep -c "issuing BEGIN")
check "单分片 SELECT 仍是裸语句" "$nb1" "0"

echo "================ [4] N94：把一个分片的主单独转走，共置对齐把它转回来 ================"
read -r SC SO <<<"$(q "SELECT (SELECT get_shard_id_for_distribution_column('t102c', 1)) || ' ' || (SELECT get_shard_id_for_distribution_column('t102o', 1))")"
wait_for 120 "仓库 1 段两个分片的主先对齐" \
  "SELECT count(DISTINCT primary_node) FROM partdist.partition_map WHERE partition_id IN ($SC, $SO)" "1"
P0=$(q "SELECT primary_node FROM partdist.partition_map WHERE partition_id = $SC")
OTHER=$(q "SELECT min(x) FROM partdist.partition_map, unnest(secondary_nodes) x WHERE partition_id = $SO")
r=$(q "SELECT partdist.raft_transfer_leader($SO, $OTHER)")
[[ "$r" == transferred* ]] && ok "把 t102o 的主单独转交给节点 $OTHER（$r）" || no "转交失败：$r"
wait_for 60 "登记上 t102o 的主变成节点 $OTHER" \
  "SELECT primary_node FROM partdist.partition_map WHERE partition_id = $SO" "$OTHER"
wait_for 150 "共置对齐把它转回节点 $P0" \
  "SELECT primary_node FROM partdist.partition_map WHERE partition_id = $SO" "$P0"
check "共置连接" "$(q "SELECT count(*) FROM t102c c JOIN t102o o USING (w, id) WHERE c.w = 1")" "5"

echo "================ [5] N102：加节点 ================"
out=$(docker exec -u postgres "$C" bash /work/pg-partdist-src/scripts/new_worker_node.sh 6 5437 2>&1)
grep -q "新节点 6 就绪" <<<"$out" && ok "新节点准备好（initdb / 配置 / 建扩展）" || no "new_worker_node.sh 失败：$(tail -3 <<<"$out")"
out=$(q "SELECT string_agg(step, '/') FROM shardpg.add_node('127.0.0.1', 5437)")
check "shardpg.add_node" "$out" "new node ready/append pg_raft.peers/join control plane/shardpg add worker/done"
bad=0
for p in 5432 5433 5434 5435 5436 5437; do
  v=$(q "SELECT array_length(string_to_array(current_setting('pg_raft.peers'), ','), 1) || '/' || cluster_size FROM partdist.pg_raft_group_status() WHERE group_id = 0" "$p")
  [[ "$v" == "6/6" ]] || { bad=1; echo "      :$p peers/组 0 成员 = $v"; }
done
check "6 个节点 peers 6 项、组 0 成员 6" "$bad" "0"
check "Citus worker 数" "$(q "SELECT count(*) FROM pg_dist_node WHERE groupid <> 0")" "5"

echo "================ [6] N102：迁移副本（共置段一起搬） ================"
CN=$(q "SELECT shard_name('t102c', $SC)")
FROM=$(q "SELECT nm.port FROM partdist.partition_map m JOIN partdist.node_map nm ON nm.node_id = m.secondary_nodes[1] WHERE m.partition_id = $SC")
out=$(q "SELECT string_agg(step, '/') FROM shardpg.move_replica('$CN', $FROM, 5437)")
[[ "$out" == *"remove member/clean up source/done" ]] && ok "shardpg.move_replica :$FROM → :5437（$out）" || no "move_replica：$out"
for g in $SC $SO; do
  check "组 $g 登记里有节点 6、没有源节点" \
    "$(q "SELECT (6 = ANY (ARRAY[primary_node] || secondary_nodes))::text || '/' || ((SELECT node_id FROM partdist.node_map WHERE port = $FROM) = ANY (ARRAY[primary_node] || secondary_nodes))::text FROM partdist.partition_map WHERE partition_id = $g")" "true/false"
  check "组 $g 在 :5437 上日志追上主" \
    "$(q "SELECT (SELECT last_log_index FROM partdist.pg_raft_group_status() WHERE group_id = $g) >= $(q "SELECT last_log_index FROM partdist.pg_raft_group_status() WHERE group_id = $g" "$(q "SELECT port FROM partdist.node_map nm JOIN partdist.partition_map m ON m.primary_node = nm.node_id WHERE m.partition_id = $g")") - 2" 5437)" "t"
  check "组 $g 在源节点 :$FROM 上已拆" "$(q "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id = $g" "$FROM")" "0"
done
check "源节点上副本表已删" "$(q "SET citus.override_table_visibility = off; SELECT count(*) FROM pg_class WHERE relname IN ('$CN', '$(q "SELECT shard_name('t102o', $SO)")')" "$FROM" | tail -1)" "0"
check "迁移期间的暂停重供登记已撤销" "$(q "SELECT count(*) FROM partdist.reprovision_hold" 5437)" "0"

echo "================ [7] 领导权转交到新节点、在新节点上读写 ================"
out=$(q "SELECT shardpg.move_primary('$CN', 5437)")
[[ "$out" == *"now on worker5"* ]] && ok "shardpg.move_primary → :5437（$out）" || no "move_primary：$out"
check "t102c 仓库 1 在 :5437 上读" "$(q "SELECT count(*) || ':' || min(current_setting('port')) FROM t102c WHERE w = 1")" "5:5437"
q "UPDATE t102c SET v = v + 1 WHERE w = 1 AND id = 1" >/dev/null
check "在新主上写" "$(q "SELECT v FROM t102c WHERE w = 1 AND id = 1")" "11"
check "共置连接在新主上" "$(q "SELECT count(*) FROM t102c c JOIN t102o o USING (w, id) WHERE c.w = 1")" "5"
check "全表行数" "$(q "SELECT count(*) || '/' || sum(v) FROM t102c")" "40/401"

echo "================ [8] 删表：6 个节点零残留 ================"
SIDS=$(q "SELECT string_agg(shardid::text, ',') FROM pg_dist_shard WHERE logicalrelid IN ('t102c'::regclass, 't102o'::regclass)")
q "DROP TABLE t102o, t102c" >/dev/null
bad=0
for p in 5433 5434 5435 5436 5437; do
  n=$(q "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN ($SIDS)" "$p")
  [[ "$n" == 0 ]] || { bad=1; echo "      :$p 还有 $n 个组"; }
done
check "各节点上没有这些组" "$bad" "0"
check "partition_map 没有这些分片" "$(q "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN ($SIDS)")" "0"

echo "================ [9] N101：去掉新节点（整簇重启，有数据），TSO 接着发号 ================"
c0=$(q "SELECT split_part(split_part(partdist.partdist_tso_status(), ' ', 1), '=', 2)")
out=$(bash "$DEMO/ha_demo_reset.sh" 5437 2>&1)
grep -q "集群回到 5 个节点" <<<"$out" && ok "ha_demo_reset.sh" || no "ha_demo_reset.sh：$(tail -5 <<<"$out")"
check "节点数" "$(q "SELECT count(*) FROM partdist.node_map")" "5"
c1=$(q "SELECT split_part(split_part(partdist.partdist_tso_status(), ' ', 1), '=', 2)")
[[ "$c1" =~ ^[0-9]+$ && "$c0" =~ ^[0-9]+$ && "$c1" -gt "$c0" ]] && ok "TSO 重启后接着发号（$c0 → $c1）" || no "TSO：重启前 $c0、重启后 $c1"
check "TSO 没被 boot 标记拦" "$(q "SELECT partdist.partdist_tso_status() LIKE '%blocked=f%'")" "t"

echo "================ 结果：PASS=$PASS FAIL=$FAIL ================"
[[ $FAIL -eq 0 ]] || { echo "（失败后清场：删表、去掉 :5437）"; cleanup; }
exit $FAIL
