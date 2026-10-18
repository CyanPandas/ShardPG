#!/usr/bin/env bash
# ============================================================================
# [宿主机] P7-N105 验收（2026-10-18）
#
# 现场：用户同一个 psql 会话跑高可用演示，第 4 步停过 :5433；第 6 步 shardpg.add_node 的前三步做完、
# 第④步 citus_add_node 提交时报 "DTX 驱动在 localhost:5433 上执行失败 / SSL SYSCALL error: EOF"，
# 整个函数回滚；前三步（各节点 ALTER SYSTEM、控制面登记）撤不掉，重跑报"节点号 6 已经在集群里"。
# 两处缺陷：DTX 驱动的缓存连接复用前不探活；add_node 不能续做。
#
#   [1] 缓存连接被对端掐掉之后，同一会话的下一次两阶段提交照常成功（master 日志记一行重连）
#   [2] 新节点 + add_node 在第④步失败（另一会话占着 pg_dist_node 的锁、lock_timeout），留下半程状态
#   [3] 直接重跑 add_node：前面做过的跳过，④ 做完；6 个节点 peers / 组 0 / Citus worker 都对
#   [4] 再跑一次：只返回一行"已经在集群里"
#   [5] 同一会话跨过新节点的一次重启，再做两阶段提交照常成功（现场同款路径）
#   [6] ha_demo_reset.sh 去掉新节点，回到 5 节点
#
# 前提：pg-test 5 节点（:5432 master + :5433–:5436），没有 :5437。约 3–5 分钟。
# 用法：bash test_add_node_resume_p7n105.sh
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
HERE=$(cd "$(dirname "$0")" && pwd)
DEMO="${DEMO:-$HERE/../demo}"
MLOG=/work/pg-cluster-data/coordinator/pg.log
PASS=0; FAIL=0
q()  { docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p "${2:-5432}" -U postgres -X -At -c "$1" </dev/null 2>&1; }
ok() { echo "  ✓ $1"; PASS=$((PASS + 1)); }
no() { echo "  ✗ $1"; FAIL=$((FAIL + 1)); }
check() { if [[ "$2" == "$3" ]]; then ok "$1（$2）"; else no "$1：实际 [$2]，期望 [$3]"; fi; }
reconnects() { docker exec "$C" grep -c "的缓存连接已断开" "$MLOG"; }
# 一个会话里先做一次两阶段提交（建 schema），中间执行 $1（shell，在容器里以 postgres 跑），再做一次（删 schema）
same_session_2pc() {
  docker exec -i -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -q 2>&1 <<SQL
SELECT backend_start AS t0 FROM pg_stat_activity WHERE pid = pg_backend_pid() \gset
\setenv T0 :t0
CREATE SCHEMA n105_probe;
\! $1
DROP SCHEMA n105_probe;
SELECT 'left=' || count(*) FROM pg_namespace WHERE nspname = 'n105_probe';
SQL
}
cleanup() {
  q "DROP SCHEMA IF EXISTS n105_probe" >/dev/null
  q "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'n105_lock_holder'" >/dev/null
}
trap cleanup EXIT

echo "================ [0] 前提 ================"
check "5 个节点" "$(q "SELECT count(*) FROM partdist.node_map")" "5"
check "没有 :5437" "$(q "SELECT count(*) FROM pg_dist_node WHERE nodeport = 5437")" "0"
q "DROP SCHEMA IF EXISTS n105_probe" >/dev/null

echo "================ [1] 缓存连接被对端掐掉之后，同一会话的两阶段提交 ================"
r0=$(reconnects)
out=$(same_session_2pc "$BIN/psql -h /tmp -p 5434 -U postgres -XAtc \"SELECT 'terminated=' || count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = '' AND query LIKE 'SELECT partdist.dtx_%' AND backend_start >= '\$T0'\"")
n=$(grep -o "terminated=[0-9]*" <<<"$out" | cut -d= -f2)
[[ "${n:-0}" -ge 1 ]] && ok "在 :5434 上掐掉这个会话的 DTX 缓存连接（$n 条）" || no "没找到要掐的 DTX 连接：$out"
grep -q "ERROR" <<<"$out" && no "同一会话的 DROP SCHEMA 报错：$(grep -m2 -A1 ERROR <<<"$out")" || ok "同一会话的 DROP SCHEMA 两阶段提交成功"
check "schema 已删" "$(grep -o "left=[0-9]*" <<<"$out")" "left=0"
[[ "$(reconnects)" -gt "$r0" ]] && ok "master 日志记了重连" || no "master 日志里没有\"缓存连接已断开\""

echo "================ [2] add_node 在第④步失败，留下半程状态 ================"
out=$(docker exec -u postgres "$C" bash /work/pg-partdist-src/scripts/new_worker_node.sh 6 5437 2>&1)
grep -q "新节点 6 就绪" <<<"$out" && ok "新节点准备好" || no "new_worker_node.sh 失败：$(tail -3 <<<"$out")"
# 另一会话占着 pg_dist_node 的 ROW EXCLUSIVE 锁：①–③ 只读 pg_dist_node（不冲突），④ citus_add_node 要 EXCLUSIVE ⇒ 等锁超时
docker exec -u postgres -e PGAPPNAME=n105_lock_holder "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -q \
  -c "BEGIN; LOCK TABLE pg_catalog.pg_dist_node IN ROW EXCLUSIVE MODE; SELECT pg_sleep(300); COMMIT;" </dev/null >/dev/null 2>&1 &
holder=$!
sleep 2
out=$(docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -At \
      -c "SET lock_timeout = '5s'" -c "SELECT * FROM shardpg.add_node('127.0.0.1', 5437)" </dev/null 2>&1)
grep -q "lock timeout" <<<"$out" && ok "add_node 在 citus_add_node 等锁超时" || no "add_node 没按预期失败：$(tail -3 <<<"$out")"
q "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'n105_lock_holder'" >/dev/null
wait "$holder" 2>/dev/null
check "半程：master 的 peers 已含节点 6" "$(q "SELECT current_setting('pg_raft.peers') LIKE '%,6@127.0.0.1:5437'")" "t"
check "半程：node_map 已有节点 6" "$(q "SELECT count(*) FROM partdist.node_map WHERE node_id = 6 AND port = 5437")" "1"
check "半程：Citus 还没有 :5437" "$(q "SELECT count(*) FROM pg_dist_node WHERE nodeport = 5437")" "0"

echo "================ [3] 直接重跑 add_node：续做 ================"
out=$(q "SELECT step || ' | ' || detail FROM shardpg.add_node('127.0.0.1', 5437)")
check "四步都走到" "$(cut -d'|' -f1 <<<"$out" | sed 's/ *$//' | paste -sd/)" "新节点就绪/各节点追加 pg_raft.peers/加入控制面/Citus 加入 worker/完成"
grep -q "其中 5 个上次已经追加过" <<<"$out" && ok "② 认出 5 个节点上次已经追加过、跳过" || no "② 没认出做过：$(grep "pg_raft.peers" <<<"$out")"
bad=0
for p in 5432 5433 5434 5435 5436 5437; do
  v=$(q "SELECT array_length(string_to_array(current_setting('pg_raft.peers'), ','), 1) || '/' || cluster_size FROM partdist.pg_raft_group_status() WHERE group_id = 0" "$p")
  [[ "$v" == "6/6" ]] || { bad=1; echo "      :$p peers/组 0 成员 = $v"; }
done
check "6 个节点 peers 6 项、组 0 成员 6" "$bad" "0"
check "Citus worker 数" "$(q "SELECT count(*) FROM pg_dist_node WHERE groupid <> 0")" "5"
check "各节点 node_map 都是 6 行" "$(for p in 5432 5433 5434 5435 5436 5437; do q "SELECT count(*) FROM partdist.node_map" "$p"; done | sort -u | paste -sd,)" "6"

echo "================ [4] 再跑一次 ================"
check "已入簇时只返回完成" "$(q "SELECT step || ' | ' || detail FROM shardpg.add_node('127.0.0.1', 5437)")" "完成 | 节点 6（127.0.0.1:5437）已经在集群里，不用再加"

echo "================ [5] 同一会话跨过新节点的一次重启，再做两阶段提交 ================"
r0=$(reconnects)
out=$(same_session_2pc "$BIN/pg_ctl -D /work/pg-cluster-data/worker5 -m fast restart -w -t 60 -l /work/pg-cluster-data/worker5/pg.log >/dev/null && echo restarted")
grep -q "restarted" <<<"$out" && ok ":5437 重启" || no ":5437 重启失败：$out"
grep -q "ERROR" <<<"$out" && no "重启之后同一会话的 DROP SCHEMA 报错：$(grep -m2 -A1 ERROR <<<"$out")" || ok "重启之后同一会话的 DROP SCHEMA 两阶段提交成功"
check "schema 已删" "$(grep -o "left=[0-9]*" <<<"$out")" "left=0"
[[ "$(reconnects)" -gt "$r0" ]] && ok "master 日志记了重连" || no "master 日志里没有\"缓存连接已断开\""

echo "================ [6] 去掉新节点 ================"
out=$(bash "$DEMO/ha_demo_reset.sh" 5437 2>&1)
grep -q "集群回到 5 个节点" <<<"$out" && ok "ha_demo_reset.sh" || no "ha_demo_reset.sh：$(tail -5 <<<"$out")"
check "5 个节点" "$(q "SELECT count(*) FROM partdist.node_map")" "5"

echo
echo "================ 结果：$PASS 通过 / $FAIL 失败 ================"
[[ $FAIL -eq 0 ]]
