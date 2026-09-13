#!/usr/bin/env bash
# [宿主机] T7.27（P7-W2）捕获环：背压 + 溢出记账 + 分叉自动修复 验收。
#
# 缺陷：捕获环是**全节点共享**的 8192 槽，排空只发生在事务提交（和基线分块）时，
# 满了 PartWALInsert 只能打一条 WARNING 就覆盖未消费的条目 —— 那条记录永远进不了
# 分区流，副本静默缺一条。**单个事务**只要产生 8192 条以上被捕获的记录（一条
# INSERT 灌几千行、每行堆 + 索引各一条）就会把环写爆，不需要任何并发。
#
# 修法三层：
#   ① 背压：补丁 0005 的写路径钩子（heap 插入/更新/删除开头，不在临界区）在环占用
#      过 pg_partdist.partwal_ring_high_water（默认 50%）时就地排空；
#   ② 记账：真的覆盖了（不经写钩子的批量写入，如大表 VACUUM）就记下分区，下一次
#      排空时给它打分叉标记；
#   ③ 自愈：心跳工作者按 pg_partdist.auto_repair_interval_s 自动调
#      repair_diverged_shards() 重做物理基线。
#
# ★ 夹具**只捕获、不建 Raft 组**：数据组复制目前是一条记录一次同步提案（≈4 条/秒，
#   见 P7 总账），12000 条记录走复制要跑近一个小时。环的行为与复制无关，本套件只验环。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
PASS=0; FAIL=0
WN=5440

DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }

check() {
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

setguc() {  # setguc <name> <value|RESET>
  if [[ "$2" == "RESET" ]]; then
    PSQL $WN -q -c "ALTER SYSTEM RESET $1" </dev/null >/dev/null
  else
    PSQL $WN -q -c "ALTER SYSTEM SET $1 = $2" </dev/null >/dev/null
  fi
  PSQL $WN -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null
  sleep 1
}
cleanup() {
  setguc pg_partdist.partwal_ring_high_water RESET
  setguc pg_partdist.auto_repair_interval_s RESET
  setguc pg_partdist.auto_repair_diverged RESET
  PSQL $WN -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS p7w2_ring" </dev/null >/dev/null 2>&1 || true
}
trap cleanup EXIT

stat_of() {  # stat_of <列名>
  PSQL $WN -Atc "SELECT $1 FROM partdist.partwal_ring_stats()" </dev/null | tail -1
}
nodelog_hits() {
  DEX bash -c "cat /work/pg-cluster-data/worker$(( WN - 5432 ))/*.log 2>/dev/null | grep -cE '$1' || true" </dev/null | tail -1 | tr -d '[:space:]'
}

echo "========== [1] 夹具：本地表 + 登记捕获（不建 Raft 组） =========="
PSQL $WN -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS p7w2_ring;
CREATE TABLE p7w2_ring(id int primary key, v text);
ALTER TABLE p7w2_ring SET (autovacuum_enabled = off);
SQL
OID=$(PSQL $WN -Atc "SELECT 'p7w2_ring'::regclass::oid" </dev/null | tail -1)
nrel=$(PSQL $WN -Atc "SELECT partdist.register_shard_fileset('p7w2_ring')" </dev/null | tail -1)
# 主堆 + 主键索引 + TOAST 堆 + TOAST 索引 = 4（v 是 text 列，带 TOAST）
check "登记捕获（主堆 + 主键 + TOAST 堆 + TOAST 索引 = 4 个成员）" "$nrel" "4"
PSQL $WN -q -c "SELECT partdist.shard_clear_divergence($OID)" </dev/null >/dev/null 2>&1
check "环容量 8192" "$(stat_of capacity)" "8192"

echo "========== [2] ★ 关掉背压：单事务 6000 行（≈12000 条记录）把环写爆 =========="
setguc pg_partdist.partwal_ring_high_water 0
# 先关掉自动修复：心跳每 5 秒醒一次，标记可能在下面检查之前就被修掉，断言就成了赛跑
setguc pg_partdist.auto_repair_diverged off
ow0=$(stat_of overwrites)
p0=$(PSQL $WN -Atc "SELECT partdist.get_partition_flush_lsn($OID)" </dev/null | tail -1)
PSQL $WN -v ON_ERROR_STOP=1 -q -c "INSERT INTO p7w2_ring SELECT g, 'x'||g FROM generate_series(1, 6000) g" </dev/null >/dev/null 2>&1
ow1=$(stat_of overwrites)
p1=$(PSQL $WN -Atc "SELECT partdist.get_partition_flush_lsn($OID)" </dev/null | tail -1)
echo "  覆盖计数 ${ow0} → ${ow1}；分区流 plsn ${p0} → ${p1}（写进流 $(( p1 - p0 )) 条）"
check "★ 关掉背压时确实发生覆盖（证明夹具真能把环写爆）" \
      "$([[ "$ow1" =~ ^[0-9]+$ && "$ow1" -gt "${ow0:-0}" ]] && echo ok)" "ok"
div=$(PSQL $WN -Atc "SELECT partdist.shard_divergence($OID)" </dev/null | tail -1)
check "★ 覆盖被记账并给该分区打上分叉标记（${div:0:60}…）" \
      "$([[ "$div" == *"捕获环溢出"* ]] && echo ok)" "ok"
check "告警留痕（捕获环溢出，…副本缺记录）" \
      "$([[ "$(nodelog_hits '捕获环溢出，[0-9]+ 个分区的副本缺记录')" -ge 1 ]] && echo ok)" "ok"

echo "========== [3] ★ 自愈：心跳自动重做物理基线并清掉标记 =========="
rep0=$(nodelog_hits '分叉标记自动修复：repaired=[1-9]')
setguc pg_partdist.auto_repair_interval_s 5
setguc pg_partdist.auto_repair_diverged RESET
cleared=""
for t in $(seq 1 40); do
  d=$(PSQL $WN -Atc "SELECT coalesce(partdist.shard_divergence($OID), 'NONE')" </dev/null | tail -1)
  [[ "$d" == "NONE" ]] && { cleared=ok; break; }
  sleep 2
done
check "★ 80 秒内标记被自动清掉（无人工调用）" "$cleared" "ok"
check "★ 心跳留下自动修复日志" \
      "$([[ "$(nodelog_hits '分叉标记自动修复：repaired=[1-9]')" -gt "${rep0:-0}" ]] && echo ok)" "ok"
setguc pg_partdist.auto_repair_interval_s RESET

echo "========== [4] ★ 打开背压（默认 50%）：同样的写入不再覆盖 =========="
setguc pg_partdist.partwal_ring_high_water RESET
check "背压高水位回到默认 50" "$(PSQL $WN -Atc 'SHOW pg_partdist.partwal_ring_high_water' </dev/null | tail -1)" "50"
ow2=$(stat_of overwrites)
bp2=$(stat_of backpressure_flushes)
p2=$(PSQL $WN -Atc "SELECT partdist.get_partition_flush_lsn($OID)" </dev/null | tail -1)
PSQL $WN -v ON_ERROR_STOP=1 -q -c "INSERT INTO p7w2_ring SELECT g, 'y'||g FROM generate_series(10001, 16000) g" </dev/null >/dev/null 2>&1
ow3=$(stat_of overwrites)
bp3=$(stat_of backpressure_flushes)
p3=$(PSQL $WN -Atc "SELECT partdist.get_partition_flush_lsn($OID)" </dev/null | tail -1)
echo "  覆盖计数 ${ow2} → ${ow3}；背压排空 ${bp2} → ${bp3}；分区流 plsn ${p2} → ${p3}（写进流 $(( p3 - p2 )) 条）"
check "★ 同样 6000 行，覆盖计数不变" "$ow3" "$ow2"
check "★ 背压排空确实发生过（≥ 1 次）" \
      "$([[ "$bp3" =~ ^[0-9]+$ && "$bp3" -gt "${bp2:-0}" ]] && echo ok)" "ok"
check "★ 没有新的分叉标记" "$(PSQL $WN -Atc "SELECT coalesce(partdist.shard_divergence($OID), 'NONE')" </dev/null | tail -1)" "NONE"
# 背压没有丢记录：开背压这一轮进流的条数不应少于关背压那一轮（后者有被覆盖掉的）
check "★ 进流条数（$(( p3 - p2 ))）≥ 被覆盖那一轮（$(( p1 - p0 ))）" \
      "$([[ $(( p3 - p2 )) -ge $(( p1 - p0 )) ]] && echo ok)" "ok"
check "表里 12000 行都在" "$(PSQL $WN -Atc 'SELECT count(*) FROM p7w2_ring' </dev/null | tail -1)" "12000"

echo "========== [5] 节点健康 =========="
health_check_no_crash

echo ""
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "T7.27 捕获环背压 + 自愈：全部通过"; else echo "T7.27 捕获环背压 + 自愈：存在 FAIL"; fi
