#!/usr/bin/env bash
# ============================================================================
# [宿主机] P7-N114 / N115 / N116 验收（2026-10-21，lab 实测的两个故障）
#
#   [1] 建表 t114（4 片、1 主 2 从，带一个延迟检查的唯一约束，给 [3] 造 PREPARE 失败用）
#   [2] N114 一个从慢（在它上面锁住 partdist.raft_log 15 s），且它在 peers 里排在另一个从前面：
#            往这一组写一行要成功、不等满超时（原来：慢的把共用时限耗光，另一个早到的确认被当成超时 ⇒ 未达多数派）；
#            锁放开后慢的那个追平
#   [3] N115 跨两个分片的事务，一个分片 PREPARE 失败（延迟唯一约束）⇒ master 回滚另一个分片已 PREPARE 的分支：
#            那个分片的主的 SQL 日志不能留洞（原来：ROLLBACK PREPARED 把主写的 raft_log 行一起撤掉，没人补）
#   [4] N116 主的日志真有洞、环里也没了（直接删 4 行、再写 150 多条把它挤出环）：
#            shardpg.move_replica 把一份副本迁到空闲节点要能做完（原来：新副本追到洞前就停，10 分钟超时）；
#            洞从其他成员补回来，新节点日志与主一致
#   [5] 删表：各节点零残留
#
# 前提：pg-test 5 节点（:5432 master + :5433–:5436），没有 t114。约 5 分钟。
# 用法：bash test_quorum_log_holes_p7n114.sh
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
PASS=0; FAIL=0
q()  { docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p "${2:-5432}" -U postgres -X -At -c "$1" </dev/null 2>&1; }
ok() { echo "  ✓ $1"; PASS=$((PASS + 1)); }
no() { echo "  ✗ $1"; FAIL=$((FAIL + 1)); }
check() { if [[ "$2" == "$3" ]]; then ok "$1（$2）"; else no "$1：实际 [$2]，期望 [$3]"; fi; }
wait_for() {   # wait_for 秒 描述 SQL 期望 [端口]
  local t; for t in $(seq 1 "$1"); do [[ "$(q "$3" "${5:-5432}")" == "$4" ]] && { ok "$2（${t} s 内）"; return 0; }; sleep 1; done
  no "$2：$1 s 内没等到 [$4]，最后是 [$(q "$3" "${5:-5432}")]"; return 1
}
port_of() { q "SELECT port FROM partdist.node_map WHERE node_id = $1"; }
# 组 $1 在端口 $2 上 SQL 日志里缺的 index（1..max）
gaps() { q "SELECT coalesce(string_agg(i::text, ',' ORDER BY i), '') FROM generate_series(1, (SELECT max(log_index) FROM partdist.raft_log WHERE group_id = $1)) i WHERE NOT EXISTS (SELECT 1 FROM partdist.raft_log r WHERE r.group_id = $1 AND r.log_index = i)" "$2"; }
last_idx() { q "SELECT last_log_index FROM partdist.pg_raft_group_status() WHERE group_id = $1" "$2"; }

echo "================ [0] 前提 ================"
check "5 个节点" "$(q "SELECT count(*) FROM partdist.node_map")" "5"
q "DROP TABLE IF EXISTS t114" >/dev/null

echo "================ [1] 建表 ================"
out=$(docker exec -i -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -q -v ON_ERROR_STOP=1 2>&1 <<'SQL'
CREATE TABLE t114 (k int, id int, u int, PRIMARY KEY (k, id), UNIQUE (k, u) DEFERRABLE INITIALLY DEFERRED,
                   SHARD KEY (k) SHARDS 4 REPLICAS 2);
INSERT INTO t114 SELECT k, i, i FROM generate_series(1, 8) k, generate_series(1, 3) i;
SQL
)
[[ $? -eq 0 ]] && ok "建表、写 24 行" || no "建表失败：$(tail -3 <<<"$out")"
SA=$(q "SELECT get_shard_id_for_distribution_column('t114', 1)")
LA=$(q "SELECT primary_node FROM partdist.partition_map WHERE partition_id = $SA")
read -r FA1 FA2 <<<"$(q "SELECT min(x) || ' ' || max(x) FROM partdist.partition_map, unnest(secondary_nodes) x WHERE partition_id = $SA")"
PLA=$(port_of "$LA"); PF1=$(port_of "$FA1"); PF2=$(port_of "$FA2")
FREE=$(q "SELECT min(node_id) FROM partdist.node_map WHERE node_id NOT IN (1, $LA, $FA1, $FA2)"); PFREE=$(port_of "$FREE")
echo "  分片 $SA（k = 1）：主 :$PLA，从 :$PF1、:$PF2；不在组里的节点 :$PFREE"
# 另一个分片：主不是 LA 的，给 [3] 造 PREPARE 失败
read -r KB SB <<<"$(q "SELECT k || ' ' || s FROM (SELECT k, get_shard_id_for_distribution_column('t114', k) s FROM generate_series(2, 8) k) x JOIN partdist.partition_map m ON m.partition_id = x.s WHERE m.primary_node <> $LA ORDER BY k LIMIT 1")"
echo "  [3] 用的另一个分片：$SB（k = $KB）"

echo "================ [2] N114：排在前面的从慢，写入照样成功 ================"
docker exec -u postgres "$C" $BIN/psql -h /tmp -p "$PF1" -U postgres -X -q \
  -c "BEGIN; LOCK TABLE partdist.raft_log IN SHARE MODE; SELECT pg_sleep(15); COMMIT;" </dev/null >/dev/null 2>&1 &
LOCKER=$!
wait_for 10 ":$PF1 上 raft_log 已锁住" \
  "SELECT count(*) FROM pg_locks WHERE relation = 'partdist.raft_log'::regclass AND mode = 'ShareLock' AND granted" "1" "$PF1"
t0=$(date +%s%N)
out=$(q "INSERT INTO t114 VALUES (1, 200, 200)")
ms=$(( ($(date +%s%N) - t0) / 1000000 ))
check "慢从锁着时往这一组写一行" "$out" "INSERT 0 1"
[[ $ms -lt 4500 ]] && ok "没等满超时（${ms} ms，上限约 5600 ms）" || no "用了 ${ms} ms（等满了超时）"
wait "$LOCKER" 2>/dev/null
wait_for 60 "锁放开后 :$PF1 追平主" \
  "SELECT (SELECT last_log_index FROM partdist.pg_raft_group_status() WHERE group_id = $SA) >= $(last_idx "$SA" "$PLA")" "t" "$PF1"
check "这一行读得到" "$(q "SELECT u FROM t114 WHERE k = 1 AND id = 200")" "200"

echo "================ [3] N115：2PC 一边 PREPARE 失败，另一边主的 SQL 日志不留洞 ================"
check "开始前主上没有洞" "$(gaps "$SA" "$PLA")" ""
out=$(docker exec -i -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -q 2>&1 <<SQL
BEGIN;
INSERT INTO t114 VALUES (1, 300, 300);
INSERT INTO t114 VALUES ($KB, 300, 777), ($KB, 301, 777);
COMMIT;
SQL
)
[[ "$out" == *"duplicate key value violates unique constraint"* ]] && ok "事务在 PREPARE 时失败（延迟唯一约束）" || no "事务没按预期失败：$out"
check "两边都没写进去" "$(q "SELECT count(*) FROM t114 WHERE id IN (300, 301)")" "0"
wait_for 20 "主 :$PLA 的 SQL 日志没有洞（回滚的 prepared 事务那几行按环补回）" \
  "SELECT coalesce(string_agg(i::text, ','), '') FROM generate_series(1, (SELECT max(log_index) FROM partdist.raft_log WHERE group_id = $SA)) i WHERE NOT EXISTS (SELECT 1 FROM partdist.raft_log r WHERE r.group_id = $SA AND r.log_index = i)" \
  "" "$PLA"
check "主上没有残留的 prepared 事务" "$(q "SELECT count(*) FROM pg_prepared_xacts" "$PLA")" "0"

echo "================ [4] N116：主的日志有洞、环里也没了，迁副本照样做完 ================"
L0=$(last_idx "$SA" "$PLA")
HLO=$((L0 - 6)); HHI=$((L0 - 3))         # 删中间的 4 行（末尾的删了 max 跟着变小，看不出洞）
q "DELETE FROM partdist.raft_log WHERE group_id = $SA AND log_index BETWEEN $HLO AND $HHI" "$PLA" >/dev/null
check "主上删出洞 $HLO..$HHI" "$(gaps "$SA" "$PLA")" "$(seq -s, $HLO $HHI)"
# 再写 50 行（每行堆 + 两个索引 ≈ 3 条），把洞挤出 128 条的环
q "DO \$\$ BEGIN FOR i IN 1000..1049 LOOP INSERT INTO t114 VALUES (1, i, i); END LOOP; END \$\$" >/dev/null
L1=$(last_idx "$SA" "$PLA")
[[ $((L1 - 128)) -gt $HHI ]] && ok "洞已出环（主到 $L1，环下界 > $HHI）" || no "洞还在环里（主到 $L1）"
check "主上洞还在（没人补）" "$(gaps "$SA" "$PLA")" "$(seq -s, $HLO $HHI)"
t0=$(date +%s)
out=$(docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -At -F'|' \
      -c "SET statement_timeout = '300s'" -c "SELECT step, detail FROM shardpg.move_replica('t114_$SA', $PF2, $PFREE)" </dev/null 2>&1)
secs=$(( $(date +%s) - t0 ))
[[ "$(tail -1 <<<"$out" | cut -d'|' -f1)" == done ]] && ok "shardpg.move_replica :$PF2 → :$PFREE 做完（${secs} s）" \
  || no "move_replica 没做完（${secs} s）：$(tail -2 <<<"$out")"
wait_for 30 "新节点 :$PFREE 日志与主一致" \
  "SELECT (SELECT last_log_index FROM partdist.pg_raft_group_status() WHERE group_id = $SA) >= $(last_idx "$SA" "$PLA")" "t" "$PFREE"
check "主上的洞已补回" "$(gaps "$SA" "$PLA")" ""
check "登记里有新节点、没有源节点" \
  "$(q "SELECT $FREE = ANY (secondary_nodes) AND NOT $FA2 = ANY (secondary_nodes) FROM partdist.partition_map WHERE partition_id = $SA")" "t"
check "k = 1 的行数" "$(q "SELECT count(*) FROM t114 WHERE k = 1")" "54"

echo "================ [5] 删表：各节点零残留 ================"
q "DROP TABLE t114" >/dev/null
SIDS=$(q "SELECT string_agg(x::text, ',') FROM unnest(ARRAY[$SA, $SB]) x")
nleft=0
for p in 5433 5434 5435 5436; do
  n=$(q "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN ($SIDS)" "$p"); nleft=$((nleft + ${n:-99}))
done
check "各节点上没有这些组" "$nleft" "0"
check "partition_map 没有这些分片" "$(q "SELECT count(*) FROM partdist.partition_map WHERE partition_id IN ($SIDS)")" "0"

echo "================ 结果：PASS=$PASS FAIL=$FAIL ================"
[[ $FAIL -eq 0 ]]
