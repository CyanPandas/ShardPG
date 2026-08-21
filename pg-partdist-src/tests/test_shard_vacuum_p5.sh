#!/usr/bin/env bash
# [宿主机] P5 分片 vacuum 页面动作验收（TX_TSO_MVCC_DEV_PLAN.md §3.4 T5.3）。
#
# 本轮覆盖 T5.3a —— 设计 §6.4 ③ **xmax 消毒**：把截断点以下的 ABORTED 与
# lock-only xmax 清成 InvalidTransactionId(0)。不做则截断后"中止了的删除"
# 被免查隐式冻结区读成"早已提交的删除"，**活行被判死**。
#
# 判据边界（三条都要验，少一条就等于没验）：
#   - ABORTED 且在截断点以下 ⇒ 清；
#   - COMMITTED ⇒ 不动（真删除，元组留给动作 ② 回收）；
#   - 截断点以上 ⇒ 不动（clog 还查得到）。
#
# 工程纪律：容器调用一律 </dev/null；断言空值即 FAIL；负向配计数守卫；
#       结尾节点健康检查（lib_node_health.sh）。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
WPORT="${WPORT:-5433}"
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }

check() {
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

NEG_RUN=0
neg() {  # neg <名字> <期望片段> <SQL>
  local hit
  hit=$(PSQL "$WPORT" -Atc "$3" </dev/null 2>&1 | grep -c "$2")
  check "负向:$1" "$hit" "1"
  NEG_RUN=$((NEG_RUN+1))
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

set_whitelist() {  # set_whitelist <oid 串（可空）>
  PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '$1';" </dev/null >/dev/null
  PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
  local guc="" i
  for i in $(seq 1 10); do
    guc=$(PSQL "$WPORT" -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$guc" == "$1" ]] && break; sleep 1
  done
  [[ "$guc" == "$1" ]]
}
crash_restart() {  # immediate 崩溃 + 拉起（等待就绪）
  DEX /work/pg-install/bin/pg_ctl -D "$DATADIR" -m immediate stop </dev/null >/dev/null 2>&1
  sleep 1
  DEX /work/pg-install/bin/pg_ctl -D "$DATADIR" -l "$DATADIR/startup.log" start </dev/null >/dev/null 2>&1
  local up="" i
  for i in $(seq 1 45); do
    up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null 2>/dev/null) && [[ "$up" == "1" ]] && break; sleep 1
  done
  [[ "$up" == "1" ]]
}
# 取某行的原始 xmax（pageinspect 直读页面，绕开一切可见性解释）。
# 按行指针定位：四行按 id 顺序一次插入、DELETE 不产生新版本（P1 禁 HOT 剪枝
# 且删除不写新元组），故 lp1..lp4 恒等于 r1..r4。
XMAX() { PSQL "$WPORT" -Atc "SELECT t_xmax FROM heap_page_items(get_raw_page('p5a',0)) WHERE lp=$1" </dev/null; }

echo "========== [0] 前置：节点就绪 + 符号 + 测试函数 =========="
up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null)
check "worker :$WPORT 可用" "$up" "1"
sym=$(DEX bash -c "nm -D /work/pg-install/lib/postgresql/pg_partdist.so | grep -c 'T partdist_shard_sanitize_xmax$'" </dev/null)
check "扩展符号 partdist_shard_sanitize_xmax 存在" "$sym" "1"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE EXTENSION IF NOT EXISTS pageinspect;
CREATE OR REPLACE FUNCTION sclog_read(oid, bigint) RETURNS int
  AS '$libdir/pg_partdist','partdist_shard_clog_read' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_write(oid, bigint, int) RETURNS void
  AS '$libdir/pg_partdist','partdist_shard_clog_write' LANGUAGE C STRICT;
SQL
check "测试函数就绪" "$?" "0"
DATADIR=$(PSQL "$WPORT" -Atc "SHOW data_directory" </dev/null)

echo "========== [1] 构造四种 xmax 形态 =========="
PSQL "$WPORT" -q -c "DROP TABLE IF EXISTS p5a;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; CREATE TABLE p5a(id int, v text) WITH (autovacuum_enabled=off);" </dev/null >/dev/null
OA=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p5a'" </dev/null)
set_whitelist "$OA"; check "p5a 白名单生效" "$?" "0"

# r1 中止删除（ABORTED xmax）/ r2 已提交删除（COMMITTED xmax）
# r3 从未删除（xmax=0）  / r4 中止删除但号更大（留在截断点以上作对照）
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL' >/dev/null
INSERT INTO p5a VALUES (1,'r1'),(2,'r2'),(3,'r3'),(4,'r4');
BEGIN; DELETE FROM p5a WHERE id=1; ROLLBACK;
BEGIN; DELETE FROM p5a WHERE id=2; COMMIT;
BEGIN; DELETE FROM p5a WHERE id=4; ROLLBACK;
SQL
X1=$(XMAX 1); X2=$(XMAX 2); X3=$(XMAX 3); X4=$(XMAX 4)
check "r1 带 ABORTED xmax（非 0）"   "$([[ -n "$X1" && "$X1" != "0" ]] && echo y)" "y"
check "r2 带 COMMITTED xmax（非 0）" "$([[ -n "$X2" && "$X2" != "0" ]] && echo y)" "y"
check "r3 无 xmax"                   "$X3" "0"
check "r4 带 ABORTED xmax（非 0）"   "$([[ -n "$X4" && "$X4" != "0" ]] && echo y)" "y"
check "r1 的 clog = ABORTED(3)"      "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OA::oid,$X1::bigint)" </dev/null)" "3"
check "r2 的 clog = COMMITTED(2)"    "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OA::oid,$X2::bigint)" </dev/null)" "2"
check "r4 的 clog = ABORTED(3)"      "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OA::oid,$X4::bigint)" </dev/null)" "3"
check "发号递增：X4 最大" "$([[ "$X4" -gt "$X1" && "$X4" -gt "$X2" ]] && echo y)" "y"
vis0=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5a" </dev/null)
check "消毒前可见 3 行（r1/r3/r4）" "$vis0" "3"

echo "========== [2] 消毒：trunc_before = X4（覆盖 X1/X2，不覆盖 X4）=========="
L0=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
RES=$(PSQL "$WPORT" -Atc "SELECT pages_scanned||'/'||pages_dirtied||'/'||tuples_sanitized FROM partdist.shard_sanitize_xmax('p5a'::regclass, $X4::bigint)" </dev/null)
# 先切段再取上界：pg_current_wal_lsn() 返回**最后一条记录的末端**，消毒记录若
# 正好是流水线末尾，-e 上界会把它自己排除掉（实测：同一脚本两次运行一过一挂）。
PSQL "$WPORT" -Atc "SELECT pg_switch_wal()" </dev/null >/dev/null
L1=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
check "返回 1 页扫描/1 页改写/1 条消毒" "$RES" "1/1/1"
check "r1 的 xmax 已清 0"        "$(XMAX 1)" "0"
check "r1 带 HEAP_XMAX_INVALID"  "$(PSQL "$WPORT" -Atc "SELECT (t_infomask & 2048) FROM heap_page_items(get_raw_page('p5a',0)) WHERE lp=1" </dev/null)" "2048"
check "r2 的 COMMITTED xmax 不动" "$(XMAX 2)" "$X2"
check "r4 的 xmax 在截断点以上不动" "$(XMAX 4)" "$X4"
check "消毒后仍可见 3 行"        "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5a" </dev/null)" "3"

echo "========== [3] WAL 取证：走的是内核 XLOG_HEAP2_FREEZE_PAGE =========="
nfrz=$(DEX /work/pg-install/bin/pg_waldump -p "$DATADIR/pg_wal" -s "$L0" -e "$L1" </dev/null 2>/dev/null | grep -c "FREEZE_PAGE")
check "区间内有 FREEZE_PAGE 记录" "$nfrz" "1"

echo "========== [4] 幂等：再跑一次零动作 =========="
RES2=$(PSQL "$WPORT" -Atc "SELECT tuples_sanitized FROM partdist.shard_sanitize_xmax('p5a'::regclass, $X4::bigint)" </dev/null)
check "第二趟 0 条" "$RES2" "0"

echo "========== [5] 负向三条 =========="
# ① 区间内仍未决 —— 把 r4 的 clog 改回 RUNNING(0) 再把它划进截断范围
PSQL "$WPORT" -Atc "SELECT sclog_write($OA::oid,$X4::bigint,0)" </dev/null >/dev/null
neg "区间内仍未决即拒" "仍未决" \
    "SELECT tuples_sanitized FROM partdist.shard_sanitize_xmax('p5a'::regclass, $((X4+1))::bigint)"
check "被拒后 r4 的 xmax 未被污染" "$(XMAX 4)" "$X4"
PSQL "$WPORT" -Atc "SELECT sclog_write($OA::oid,$X4::bigint,3)" </dev/null >/dev/null
# ② 截断点后退
PSQL "$WPORT" -Atc "SELECT partdist.shard_vacuum_set_watermarks($OA::oid, $X4::bigint, $X4::bigint)" </dev/null >/dev/null
check "水位已置 $X4" "$(PSQL "$WPORT" -Atc "SELECT clog_truncate_before FROM partdist.shard_vacuum_watermarks($OA::oid)" </dev/null)" "$X4"
neg "trunc_before 后退即拒" "小于当前截断点" \
    "SELECT tuples_sanitized FROM partdist.shard_sanitize_xmax('p5a'::regclass, $((X4-1))::bigint)"
# ③ 非分片表
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; DROP TABLE IF EXISTS p5plain; CREATE TABLE p5plain(id int);" </dev/null >/dev/null
neg "非分片表即拒" "不是分片打标表" \
    "SELECT tuples_sanitized FROM partdist.shard_sanitize_xmax('p5plain'::regclass, 100::bigint)"
check "负向计数守卫（应跑 3 条）" "$NEG_RUN" "3"

echo "========== [6] 崩溃持久性：消毒经 WAL 落盘，不是只改了内存 =========="
crash_restart; check "immediate 崩溃后重启就绪" "$?" "0"
check "重启后 r1 的 xmax 仍为 0" "$(XMAX 1)" "0"
check "重启后 r2 的 xmax 仍在"   "$(XMAX 2)" "$X2"
check "重启后仍可见 3 行"        "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5a" </dev/null)" "3"

echo "========== [7] 清场 =========="
set_whitelist ""
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; DROP TABLE IF EXISTS p5a; DROP TABLE IF EXISTS p5plain; DROP FUNCTION IF EXISTS sclog_read(oid,bigint); DROP FUNCTION IF EXISTS sclog_write(oid,bigint,int);" </dev/null >/dev/null
check "清场完成" "$?" "0"

health_check_no_crash
echo "================================================================"
echo "  PASS=$PASS  FAIL=$FAIL"
echo "================================================================"
[[ $FAIL -eq 0 ]]
