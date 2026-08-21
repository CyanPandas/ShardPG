#!/usr/bin/env bash
# [宿主机] P5 分片 vacuum 页面动作验收（TX_TSO_MVCC_DEV_PLAN.md §3.4 T5.3）。
#
# 覆盖 T5.3a + T5.3b：
#   [0]-[7] T5.3a —— §6.4 ③ **xmax 消毒**：把截断点以下的 ABORTED 与
#           lock-only xmax 清成 InvalidTransactionId(0)。不做则截断后"中止了
#           的删除"被免查隐式冻结区读成"早已提交的删除"，**活行被判死**。
#   [8]-[13] T5.3b —— §6.4 ① **删中止 xmin 的元组**。不做则截断后该 xmin
#           落进免查区被解释成"已提交全可见"，**中止事务的幽灵行复活**。
#           同时验"③ 必须先于 ①"这条顺序依赖：中止事务"插入后又更新"留下的
#           HOT 链，其 HEAP_HOT_UPDATED 位要靠 ③ 清 xmax 才失效。
#   [14]-[18] T5.3c —— §6.4 ② **删已提交删除的死元组**，判据**看 xmax 不看
#           xmin**：没被删过的老行是活的、零页面动作，中止的删除更是一点
#           不许碰。同时验 ② 与 ① 的刻意不对称（② 不推迟 HOT 链上的元组）。
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
-- T5.2 的验收辅助：sclog_write 只写 status、commit_ts 恒 0，构造不出"带时间戳的已提交槽"
CREATE OR REPLACE FUNCTION sclog_wts(oid, bigint, int, bigint) RETURNS void
  AS '$libdir/pg_partdist','partdist_shard_clog_write_ts' LANGUAGE C STRICT;
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

echo "========== [7] T5.3a 清场 =========="
set_whitelist ""
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; DROP TABLE IF EXISTS p5a; DROP TABLE IF EXISTS p5plain;" </dev/null >/dev/null
check "T5.3a 清场完成" "$?" "0"

# ================================================================
#  T5.3b —— 设计 §6.4 ① 删中止 xmin 的元组
#  不做则截断后该 xmin 落进免查隐式冻结区、被解释成"已提交全可见"，
#  **中止事务的幽灵行复活**。
# ================================================================
# NORMAL <表名> / LPDEAD <表名>：第 0 页上 LP_NORMAL(1) / LP_DEAD(3) 的行指针数
NORMAL() { PSQL "$WPORT" -Atc "SELECT count(*) FROM heap_page_items(get_raw_page('$1',0)) WHERE lp_flags=1" </dev/null; }
LPDEAD() { PSQL "$WPORT" -Atc "SELECT count(*) FROM heap_page_items(get_raw_page('$1',0)) WHERE lp_flags=3" </dev/null; }

echo "========== [8] T5.3b 构造：两活行 + 三种中止形态 =========="
PSQL "$WPORT" -q -c "DROP TABLE IF EXISTS p5b;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; CREATE TABLE p5b(id int, v text) WITH (autovacuum_enabled=off);" </dev/null >/dev/null
OB=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p5b'" </dev/null)
set_whitelist "$OB"; check "p5b 白名单生效" "$?" "0"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL' >/dev/null
INSERT INTO p5b VALUES (1,'k1'),(2,'k2');
BEGIN; INSERT INTO p5b VALUES (3,'g1'); ROLLBACK;                       -- 中止插入：根元组
BEGIN; UPDATE p5b SET v='u1' WHERE id=1; ROLLBACK;                      -- 中止更新：根 xmax + heap-only 后继
-- 中止事务内插入 + 连更两次：留下 根 → heap-only(自己也被 HOT 更新) → heap-only
-- 中间那条是"死且仍挂在 HOT 链上"的唯一形态，正是顺序依赖的检验点
BEGIN; INSERT INTO p5b VALUES (4,'g2'); UPDATE p5b SET v='g2b' WHERE id=4; UPDATE p5b SET v='g2c' WHERE id=4; ROLLBACK;
SQL
n0=$(NORMAL p5b)
check "构造后页上 7 个 LP_NORMAL" "$n0" "7"
check "构造后可见 2 行"           "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5b" </dev/null)" "2"
# 本轮截断点取"页上最大 xid + 1"，把全部中止号都划进范围
# xid 类型没有大小比较运算符，GREATEST 直接用会报"No function matches"——先转 bigint
TB=$(PSQL "$WPORT" -Atc "SELECT max(GREATEST(t_xmin::text::bigint, t_xmax::text::bigint))+1 FROM heap_page_items(get_raw_page('p5b',0)) WHERE lp_flags=1" </dev/null)
check "截断点取到值" "$([[ -n "$TB" && "$TB" -gt 3 ]] && echo y)" "y"

echo "========== [9] 顺序依赖：不先跑 ③，链上元组只能推迟 =========="
# WAL 取证窗口取这一趟：它同时产生根元组（⇒LP_DEAD⇒VACUUM 记录）与已脱链的
# heap-only 元组（⇒LP_UNUSED，只进 PRUNE 记录），两条记录一次看全。
L0=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
D1=$(PSQL "$WPORT" -Atc "SELECT tuples_removed||'/'||tuples_deferred||'/'||pages_skipped FROM partdist.shard_remove_aborted('p5b'::regclass, $TB::bigint)" </dev/null)
PSQL "$WPORT" -Atc "SELECT pg_switch_wal()" </dev/null >/dev/null
L1=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
check "先跑①：删 4 条、推迟 1 条、无跳页" "$D1" "4/1/0"
check "推迟的那条还在页上（3 个 LP_NORMAL）" "$(NORMAL p5b)" "3"
check "推迟期间可见行数不变" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5b" </dev/null)" "2"

echo "========== [10] 先 ③ 后 ①：全部清干净 =========="
S2=$(PSQL "$WPORT" -Atc "SELECT tuples_sanitized FROM partdist.shard_sanitize_xmax('p5b'::regclass, $TB::bigint)" </dev/null)
check "③ 消毒 2 条（活行 k1 的中止 xmax + 链中元组的 xmax）" "$S2" "2"
D2=$(PSQL "$WPORT" -Atc "SELECT tuples_removed||'/'||tuples_deferred||'/'||pages_skipped FROM partdist.shard_remove_aborted('p5b'::regclass, $TB::bigint)" </dev/null)
check "再跑①：删 1 条、零推迟" "$D2" "1/0/0"
check "页上只剩 2 个 LP_NORMAL" "$(NORMAL p5b)" "2"
check "页上无 LP_DEAD 残留"     "$(LPDEAD p5b)" "0"
check "两活行仍可见"            "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5b" </dev/null)" "2"
check "活行内容未被误删"        "$(PSQL "$WPORT" -Atc "SELECT string_agg(v,',' ORDER BY id) FROM p5b" </dev/null)" "k1,k2"
check "幂等：第三趟零动作"      "$(PSQL "$WPORT" -Atc "SELECT tuples_removed||'/'||tuples_deferred FROM partdist.shard_remove_aborted('p5b'::regclass, $TB::bigint)" </dev/null)" "0/0"

echo "========== [11] WAL 取证：PRUNE + VACUUM 两条内核标准记录（取 [9] 那一趟）=========="
WD=$(DEX /work/pg-install/bin/pg_waldump -p "$DATADIR/pg_wal" -s "$L0" -e "$L1" </dev/null 2>/dev/null)
check "区间内有 PRUNE 记录"  "$(echo "$WD" | grep -c 'desc: PRUNE')"  "1"
check "区间内有 VACUUM 记录" "$(echo "$WD" | grep -c 'desc: VACUUM')" "1"

echo "========== [12] 崩溃持久性 + 负向 =========="
crash_restart; check "immediate 崩溃后重启就绪" "$?" "0"
check "重启后页上仍只 2 个 LP_NORMAL" "$(NORMAL p5b)" "2"
check "重启后两活行仍在"              "$(PSQL "$WPORT" -Atc "SELECT string_agg(v,',' ORDER BY id) FROM p5b" </dev/null)" "k1,k2"
# 带索引即拒（索引两阶段是 T5.3c）。索引必须在打标之前建 —— 打标后 CREATE INDEX 被拦。
set_whitelist ""
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; DROP TABLE IF EXISTS p5idx; CREATE TABLE p5idx(id int) WITH (autovacuum_enabled=off); CREATE INDEX p5idx_i ON p5idx(id);" </dev/null >/dev/null
OI=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p5idx'" </dev/null)
set_whitelist "$OI"; check "p5idx 白名单生效" "$?" "0"
neg "带索引即拒（索引两阶段属 T5.3c）" "需要索引两阶段" \
    "SELECT tuples_removed FROM partdist.shard_remove_aborted('p5idx'::regclass, 100::bigint)"
check "负向计数守卫（累计应跑 4 条）" "$NEG_RUN" "4"

# ================================================================
#  T5.3c —— 设计 §6.4 ② 删已提交删除的死元组
#  判据**看 xmax 不看 xmin**：没被删过的老行是活的，零页面动作。
# ================================================================
echo "========== [14] T5.3c 构造：三活行 + 四种死法 =========="
set_whitelist ""
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; DROP TABLE IF EXISTS p5c; CREATE TABLE p5c(id int, v text) WITH (autovacuum_enabled=off);" </dev/null >/dev/null
OC=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p5c'" </dev/null)
set_whitelist "$OC"; check "p5c 白名单生效" "$?" "0"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL' >/dev/null
INSERT INTO p5c VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d');
DELETE FROM p5c WHERE id=1;                          -- 已提交删除：根元组死
UPDATE p5c SET v='b2' WHERE id=2;                    -- 已提交更新：根死，heap-only 后继活
BEGIN; DELETE FROM p5c WHERE id=3; ROLLBACK;         -- 中止删除：行仍是活的，② 不许碰
UPDATE p5c SET v='d2' WHERE id=4;                    -- 连更两次：中间那条是
UPDATE p5c SET v='d3' WHERE id=4;                    -- "死 + 仍挂 HOT 链"的 heap-only
SQL
check "构造后页上 7 个 LP_NORMAL" "$(NORMAL p5c)" "7"
check "构造后可见 3 行"           "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5c" </dev/null)" "3"
TC=$(PSQL "$WPORT" -Atc "SELECT max(GREATEST(t_xmin::text::bigint, t_xmax::text::bigint))+1 FROM heap_page_items(get_raw_page('p5c',0)) WHERE lp_flags=1" </dev/null)
check "截断点取到值" "$([[ -n "$TC" && "$TC" -gt 3 ]] && echo y)" "y"
# 中止删除那一行的 xmax，留作 [16] 的负向构造与"② 不碰它"的断言
XAB=$(PSQL "$WPORT" -Atc "SELECT t_xmax FROM heap_page_items(get_raw_page('p5c',0)) WHERE lp=3" </dev/null)
check "中止删除行的 xmax 非 0" "$([[ -n "$XAB" && "$XAB" != "0" ]] && echo y)" "y"
# ★ 本环境 TSO 未配置 ⇒ 遗留模式，每次提交写下的 commit_ts 都是 0，而 ② 的判据
#   是"commit_ts < GlobalSafeTs"，0 会被守卫 fail-closed 拦住（实测确认）。
#   照 T5.2 的办法用 sclog_wts 给已提交条目补上真时间戳，只动 COMMITTED 的，
#   中止那条原样留着。
PSQL "$WPORT" -Atc "SELECT sclog_wts($OC::oid, g::bigint, 2, 1000::bigint) FROM generate_series(3, $((TC-1))) g WHERE sclog_read($OC::oid, g::bigint)=2" </dev/null >/dev/null
check "已提交条目已补 commit_ts（中止那条仍是 ABORTED）" \
      "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OC::oid,$XAB::bigint)" </dev/null)" "3"

echo "========== [15] 跑 ②：只删已提交删除的，活行一条不动 =========="
L0=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
D3=$(PSQL "$WPORT" -Atc "SELECT tuples_removed||'/'||tuples_deferred||'/'||pages_skipped FROM partdist.shard_remove_dead('p5c'::regclass, $TC::bigint)" </dev/null)
PSQL "$WPORT" -Atc "SELECT pg_switch_wal()" </dev/null >/dev/null
L1=$(PSQL "$WPORT" -Atc "SELECT pg_current_wal_lsn()" </dev/null)
check "删 4 条、零推迟、无跳页" "$D3" "4/0/0"
check "页上只剩 3 个 LP_NORMAL"  "$(NORMAL p5c)" "3"
check "页上无 LP_DEAD 残留"      "$(LPDEAD p5c)" "0"
check "三活行仍可见"             "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5c" </dev/null)" "3"
check "活行内容未被误删"         "$(PSQL "$WPORT" -Atc "SELECT string_agg(v,',' ORDER BY id) FROM p5c" </dev/null)" "b2,c,d3"
check "★ 中止删除的行原样保留（xmax 未被碰）" \
      "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM heap_page_items(get_raw_page('p5c',0)) WHERE lp_flags=1 AND t_xmax::text='$XAB'" </dev/null)" "1"
check "幂等：第二趟零动作" \
      "$(PSQL "$WPORT" -Atc "SELECT tuples_removed||'/'||tuples_deferred FROM partdist.shard_remove_dead('p5c'::regclass, $TC::bigint)" </dev/null)" "0/0"
check "区间内有 PRUNE 记录"  "$(DEX /work/pg-install/bin/pg_waldump -p "$DATADIR/pg_wal" -s "$L0" -e "$L1" </dev/null 2>/dev/null | grep -c 'desc: PRUNE')"  "1"
check "区间内有 VACUUM 记录" "$(DEX /work/pg-install/bin/pg_waldump -p "$DATADIR/pg_wal" -s "$L0" -e "$L1" </dev/null 2>/dev/null | grep -c 'desc: VACUUM')" "1"
# 三个动作的分工：② 留下的中止 xmax 归 ③ 消毒
check "③ 接手消毒 1 条（②留下的中止 xmax）" \
      "$(PSQL "$WPORT" -Atc "SELECT tuples_sanitized FROM partdist.shard_sanitize_xmax('p5c'::regclass, $TC::bigint)" </dev/null)" "1"
check "消毒后仍是 3 活行"        "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p5c" </dev/null)" "3"

echo "========== [16] 负向：commit_ts=0 的已提交条目落在截断点以下 =========="
# sclog_write 只写 status、commit_ts 恒 0 —— 正好构造出 T5.2 会挡住前缀的那种槽。
# 把某条活行的 xmax 改判成这种 COMMITTED，② 必须 fail-closed 而不是照删。
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; DROP TABLE IF EXISTS p5d; CREATE TABLE p5d(id int) WITH (autovacuum_enabled=off);" </dev/null >/dev/null
OD=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p5d'" </dev/null)
set_whitelist "$OD"; check "p5d 白名单生效" "$?" "0"
PSQL "$WPORT" -q -c "INSERT INTO p5d VALUES (1); DELETE FROM p5d WHERE id=1;" </dev/null >/dev/null
XD=$(PSQL "$WPORT" -Atc "SELECT t_xmax FROM heap_page_items(get_raw_page('p5d',0)) WHERE lp=1" </dev/null)
TD=$((XD+1))
PSQL "$WPORT" -Atc "SELECT sclog_write($OD::oid,$XD::bigint,2)" </dev/null >/dev/null
check "该 xmax 已被改成 commit_ts=0 的 COMMITTED" \
      "$(PSQL "$WPORT" -Atc "SELECT sclog_read($OD::oid,$XD::bigint)" </dev/null)" "2"
neg "commit_ts 为 0 即拒" "commit_ts 为 0" \
    "SELECT tuples_removed FROM partdist.shard_remove_dead('p5d'::regclass, $TD::bigint)"
check "被拒后元组未被删" "$(NORMAL p5d)" "1"
set_whitelist "$OI"; check "p5idx 重新打标" "$?" "0"
neg "② 带索引即拒" "需要索引两阶段" \
    "SELECT tuples_removed FROM partdist.shard_remove_dead('p5idx'::regclass, 100::bigint)"
check "负向计数守卫（累计应跑 6 条）" "$NEG_RUN" "6"

echo "========== [17] 崩溃持久性 =========="
crash_restart; check "immediate 崩溃后重启就绪" "$?" "0"
check "重启后 p5c 页上仍 3 个 LP_NORMAL" "$(NORMAL p5c)" "3"
check "重启后 p5c 三活行仍在" "$(PSQL "$WPORT" -Atc "SELECT string_agg(v,',' ORDER BY id) FROM p5c" </dev/null)" "b2,c,d3"

echo "========== [18] 清场 =========="
set_whitelist ""
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation TO off; DROP TABLE IF EXISTS p5b; DROP TABLE IF EXISTS p5c; DROP TABLE IF EXISTS p5d; DROP TABLE IF EXISTS p5idx; DROP FUNCTION IF EXISTS sclog_read(oid,bigint); DROP FUNCTION IF EXISTS sclog_write(oid,bigint,int); DROP FUNCTION IF EXISTS sclog_wts(oid,bigint,int,bigint);" </dev/null >/dev/null
check "清场完成" "$?" "0"

health_check_no_crash
echo "================================================================"
echo "  PASS=$PASS  FAIL=$FAIL"
echo "================================================================"
[[ $FAIL -eq 0 ]]
