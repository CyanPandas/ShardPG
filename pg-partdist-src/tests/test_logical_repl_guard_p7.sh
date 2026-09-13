#!/usr/bin/env bash
# [宿主机] T7.22（R-P6-14）逻辑复制**协议入口**禁令验收。
#
# §10「分片表逻辑解码：禁」—— 解码器按原生 xid 组事务，对分片打标表是错误的分组。
# （R-P6-7 当年那次整节点崩溃已查明是构建产物 ABI 撕裂，不是禁令的理由。）
# T6.6 在 SQL 面禁了建槽与取变更，但那道禁令挂在 ExecutorStart 上，
# 而复制协议（replication=database 连接上发 CREATE_REPLICATION_SLOT / START_REPLICATION）
# **根本不走执行器** —— 这就是 R-P6-14。
#
# 修法不动内核：①连接认证钩子拒绝逻辑复制连接；②登记打标那一刻终止已连着的
# 逻辑 walsender；③最后一张打标表删掉之后立"逻辑解码围栏"：确认位点早于围栏的
# 旧逻辑槽（跨越了打标期）仍被拒 —— 否则"打标 → 写 → 删表 → 旧槽重连"照样解码
# 到那段 WAL（TX_TSO_MVCC_DEV_PLAN 第 24 条记的"更窄的绕法"）。
# 本套件把三条都走一遍，外加物理复制不受牵连的对照。
#
# ★ 前提说明：本环境 wal_level=replica —— 逻辑解码本来就起不来，崩溃场景不可达。
#   ①③只需要"能不能连上"，在 replica 下即可验；②需要一个**真的活着的逻辑槽**，
#   所以 [4] 会把一个 worker 临时切到 wal_level=logical，跑完 EXIT 复原。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
# 复制协议连接：mode = database（逻辑）| true（物理）
REPL() { local port=$1 mode=$2; shift 2; DEX /work/pg-install/bin/psql "host=/tmp port=$port user=postgres dbname=postgres replication=$mode" -X "$@"; }

check() {
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

# 逻辑复制连接是否被本守卫拒绝：ok = 连上了；blocked = 被守卫拒；other:<首行> = 别的原因
logical_probe() {
  local out
  out=$(REPL "$1" database -Atc "IDENTIFY_SYSTEM" </dev/null 2>&1)
  if grep -q "拒绝逻辑复制连接" <<<"$out"; then echo blocked
  elif grep -qE "^[0-9]{10,}\|" <<<"$out"; then echo ok
  else echo "other:$(head -1 <<<"$out")"; fi
}

TABLES_TO_DROP=""
MAP_ROWS=""
WNODE=""
WAL_CHANGED=0
cleanup() {
  local t
  for t in $TABLES_TO_DROP; do
    PSQL "${t%%:*}" -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${t##*:}" </dev/null >/dev/null 2>&1 || true
  done
  # 为打标登记临时塞进 partition_map 的行一并清掉，别给后面的套件留一个指向已删 oid 的登记
  for t in $MAP_ROWS; do
    PSQL "${t%%:*}" -q -c "DELETE FROM partdist.partition_map WHERE partition_id = ${t##*:}" </dev/null >/dev/null 2>&1 || true
  done
  if [[ -n "$WNODE" ]]; then
    PSQL "$WNODE" -q -c "SELECT pg_drop_replication_slot('r614_slot')" </dev/null >/dev/null 2>&1 || true
    PSQL "$WNODE" -q -c "SELECT pg_drop_replication_slot('r614_new')" </dev/null >/dev/null 2>&1 || true
    DEX bash -c "pkill -f 'pg_recvlogical.*r614_slot' || true" </dev/null >/dev/null 2>&1
  fi
  if [[ "$WAL_CHANGED" == "1" ]]; then
    PSQL "$WNODE" -q -c "ALTER SYSTEM RESET wal_level" </dev/null >/dev/null 2>&1 || true
    local d="/work/pg-cluster-data/worker$((WNODE-5432))"
    DEX /work/pg-install/bin/pg_ctl -D "$d" -m fast -w -t 60 restart -l "$d/pg.log" </dev/null >/dev/null 2>&1 || true
    echo "  [复原] :$WNODE wal_level 已 RESET 并重启"
  fi
}
trap cleanup EXIT

echo "========== [0] 前置：找一个当前**没有打标表**的 worker =========="
for p in 5440 5439 5438 5437 5436 5435 5434 5433; do
  [[ "$(logical_probe $p)" == "ok" ]] && { WNODE=$p; break; }
done
check "找到对照节点（逻辑复制连接此刻放行：:${WNODE:-∅}）" "$([[ -n "$WNODE" ]] && echo ok)" "ok"

echo "========== [1] ★ 登记打标之后，新的逻辑复制连接被拒 =========="
PSQL "$WNODE" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
DROP TABLE IF EXISTS r614_t;
CREATE TABLE r614_t(id int);
SQL
OT=$(PSQL "$WNODE" -Atc "SELECT oid FROM pg_class WHERE relname='r614_t'" </dev/null | tail -1)
TABLES_TO_DROP+="$WNODE:r614_t "
MAP_ROWS+="$WNODE:$OT "
PSQL "$WNODE" -q -c "INSERT INTO partdist.partition_map (partition_id, primary_node) VALUES ($OT, 1) ON CONFLICT DO NOTHING" </dev/null >/dev/null
reg=$(PSQL "$WNODE" -Atc "SELECT partdist.partdist_set_shard_mvcc('r614_t'::regclass); SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "打标登记成功" "$reg" "ok"
check "★ 逻辑复制连接（replication=database）被守卫拒绝" "$(logical_probe $WNODE)" "blocked"

echo "========== [2] 物理复制不受牵连 =========="
phys=$(REPL "$WNODE" true -Atc "IDENTIFY_SYSTEM" </dev/null 2>&1 | head -1)
check "物理复制连接（replication=true）照常放行（${phys:0:24}…）" \
      "$(grep -qE '^[0-9]{10,}\|' <<<"$phys" && echo ok)" "ok"
check "普通 SQL 连接照常放行" "$(PSQL "$WNODE" -Atc "SELECT 1" </dev/null | tail -1)" "1"

echo "========== [3] 负向计数的对照：SQL 面禁令仍在 =========="
sqlban=$(PSQL "$WNODE" -Atc "SELECT pg_create_logical_replication_slot('r614_sql', 'test_decoding')" </dev/null 2>&1 | head -1)
# 必须匹配**守卫自己的**报错文本：wal_level=replica 下建逻辑槽本来就会以
# "logical decoding requires wal_level >= logical" 失败，只查 ERROR 会让这条
# 在守卫退化时照样通过。
check "SQL 面建逻辑槽仍被**守卫**拦（T6.6 禁令未退化）" \
      "$([[ "$sqlban" == *"在分片打标集群上被禁用"* ]] && echo ok)" "ok"

echo "========== [4] ★ 已连着的逻辑 walsender 在登记打标时被终止 =========="
# 需要一个**活着的**逻辑槽 ⇒ 临时 wal_level=logical，EXIT 复原
PSQL "$WNODE" -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE r614_t" </dev/null >/dev/null 2>&1
TABLES_TO_DROP=""
PSQL "$WNODE" -q -c "ALTER SYSTEM SET wal_level = logical" </dev/null >/dev/null
WAL_CHANGED=1
D="/work/pg-cluster-data/worker$((WNODE-5432))"
DEX /work/pg-install/bin/pg_ctl -D "$D" -m fast -w -t 60 restart -l "$D/pg.log" </dev/null >/dev/null 2>&1
for t in $(seq 1 30); do [[ "$(PSQL "$WNODE" -Atc "SELECT 1" </dev/null 2>/dev/null | tail -1)" == "1" ]] && break; sleep 1; done
check ":$WNODE 已切到 wal_level=logical" "$(PSQL "$WNODE" -Atc "SHOW wal_level" </dev/null | tail -1)" "logical"
gate_now=$(logical_probe $WNODE)
check "重启后本节点无打标表（逻辑复制放行，前提成立）" "$gate_now" "ok"

# 留痕计数取差值：单跑时数据目录里可能还躺着上一轮的同名日志行
hit0=$(DEX bash -c "cat $D/*.log $D.log 2>/dev/null | grep -c '已终止逻辑复制进程'" </dev/null | tail -1 | tr -d '[:space:]')
REPL "$WNODE" database -Atc "CREATE_REPLICATION_SLOT r614_slot LOGICAL test_decoding" </dev/null >/dev/null 2>&1
slot=$(PSQL "$WNODE" -Atc "SELECT count(*) FROM pg_replication_slots WHERE slot_name='r614_slot'" </dev/null | tail -1)
check "经复制协议建出逻辑槽（证明协议入口在无打标时是通的）" "$slot" "1"
DEX bash -c "nohup /work/pg-install/bin/pg_recvlogical -h /tmp -p $WNODE -U postgres -d postgres --slot r614_slot --start -f /tmp/r614.out >/tmp/r614.err 2>&1 &" </dev/null
active=""
for t in $(seq 1 20); do
  active=$(PSQL "$WNODE" -Atc "SELECT active_pid FROM pg_replication_slots WHERE slot_name='r614_slot'" </dev/null | tail -1)
  [[ -n "$active" && "$active" != "0" ]] && break
  sleep 1
done
check "逻辑 walsender 已连上并持有槽（pid=${active:-∅}）" \
      "$([[ "$active" =~ ^[0-9]+$ && "$active" -gt 0 ]] && echo ok)" "ok"

PSQL "$WNODE" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE TABLE r614_t2(id int);
SQL
OT2=$(PSQL "$WNODE" -Atc "SELECT oid FROM pg_class WHERE relname='r614_t2'" </dev/null | tail -1)
TABLES_TO_DROP+="$WNODE:r614_t2 "
MAP_ROWS+="$WNODE:$OT2 "
PSQL "$WNODE" -q -c "INSERT INTO partdist.partition_map (partition_id, primary_node) VALUES ($OT2, 1) ON CONFLICT DO NOTHING" </dev/null >/dev/null
PSQL "$WNODE" -Atc "SELECT partdist.partdist_set_shard_mvcc('r614_t2'::regclass)" </dev/null >/dev/null 2>&1
gone=""
for t in $(seq 1 15); do
  gone=$(PSQL "$WNODE" -Atc "SELECT coalesce(active_pid,0) FROM pg_replication_slots WHERE slot_name='r614_slot'" </dev/null | tail -1)
  [[ "$gone" == "0" || -z "$gone" ]] && break
  sleep 1
done
check "★ 登记打标那一刻，已连着的逻辑 walsender 被终止（槽 active_pid=${gone:-∅}）" \
      "$([[ "$gone" == "0" ]] && echo ok)" "ok"
check "★ 它重连也会被拒（认证钩子接管）" "$(logical_probe $WNODE)" "blocked"
hit1=$(DEX bash -c "cat $D/*.log $D.log 2>/dev/null | grep -c '已终止逻辑复制进程'" </dev/null | tail -1 | tr -d '[:space:]')
check "节点日志留痕（本轮新增 ≥1，实际 $(( ${hit1:-0} - ${hit0:-0} ))）" \
      "$([[ -n "$hit1" && -n "$hit0" && $((hit1 - hit0)) -ge 1 ]] && echo ok)" "ok"

echo "========== [5] ★ 删掉最后一张打标表之后：跨越打标期的旧槽仍被拒，新槽放行 =========="
PSQL "$WNODE" -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE r614_t2" </dev/null >/dev/null 2>&1
TABLES_TO_DROP=""
fence_log=$(DEX bash -c "cat $D/*.log 2>/dev/null | grep -c '逻辑解码围栏立在'" </dev/null | tail -1 | tr -d '[:space:]')
check "最后一张打标表 DROP 后立起了逻辑解码围栏（日志 ${fence_log} 条）" \
      "$([[ -n "$fence_log" && "$fence_log" -ge 1 ]] && echo ok)" "ok"
out5=$(REPL "$WNODE" database -Atc "IDENTIFY_SYSTEM" </dev/null 2>&1)
check "★ 本节点已无打标表，但旧槽 r614_slot 跨越打标期 ⇒ 逻辑复制连接仍被拒" \
      "$(grep -q '跨越了分片打标期' <<<"$out5" && echo ok)" "ok"
peek=$(PSQL "$WNODE" -Atc "SELECT count(*) FROM pg_logical_slot_peek_changes('r614_slot', NULL, NULL)" </dev/null 2>&1 | head -1)
check "★ SQL 面对旧槽取变更同样被拒" "$([[ "$peek" == *"被禁用"* ]] && echo ok)" "ok"

# 围栏要活过重启（它是持久化的；只存在 shmem 里的话，重启就是一条绕法）
DEX bash -c "pkill -f 'pg_recvlogical.*r614_slot' || true" </dev/null >/dev/null 2>&1
DEX /work/pg-install/bin/pg_ctl -D "$D" -m fast -w -t 60 restart -l "$D/pg.log" </dev/null >/dev/null 2>&1
for t in $(seq 1 30); do [[ "$(PSQL "$WNODE" -Atc "SELECT 1" </dev/null 2>/dev/null | tail -1)" == "1" ]] && break; sleep 1; done
out6=$(REPL "$WNODE" database -Atc "IDENTIFY_SYSTEM" </dev/null 2>&1)
check "★ 重启之后旧槽仍被拒（围栏已持久化）" \
      "$(grep -q '跨越了分片打标期' <<<"$out6" && echo ok)" "ok"

PSQL "$WNODE" -q -c "SELECT pg_drop_replication_slot('r614_slot')" </dev/null >/dev/null 2>&1
check "删掉旧槽后逻辑复制连接放行" "$(logical_probe $WNODE)" "ok"
REPL "$WNODE" database -Atc "CREATE_REPLICATION_SLOT r614_new LOGICAL test_decoding" </dev/null >/dev/null 2>&1
newpeek=$(PSQL "$WNODE" -Atc "SELECT count(*) >= 0 FROM pg_logical_slot_peek_changes('r614_new', NULL, NULL)" </dev/null 2>&1 | tail -1)
check "新建的槽（从围栏之后起）可以正常解码" "$newpeek" "t"
PSQL "$WNODE" -q -c "SELECT pg_drop_replication_slot('r614_new')" </dev/null >/dev/null 2>&1

echo "========== [6] 节点健康 =========="
health_check_no_crash

echo ""
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "T7.22 逻辑复制协议入口禁令：全部通过"; else echo "T7.22 逻辑复制协议入口禁令：存在 FAIL"; fi
