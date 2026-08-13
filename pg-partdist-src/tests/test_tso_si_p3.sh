#!/usr/bin/env bash
# [宿主机] P3 TSO/真 SI 正式验收（TX_TSO_MVCC_DEV_PLAN.md §3.2 T3.7）。
#
# 覆盖：T3.1 TSO 服务（单调/发号即登记/并发唯一/boot 防呆）；T3.2 取号通路
#       （懒取缓存/遗留模式/fail-closed）；T3.3 ts 落账+真 SI（不可重复读
#       消失/删除对称）；T3.4 first-committer-wins 40001；T3.5 GlobalSafeTs
#       （活跃跟踪/单调/租约剔除）；T3.6 strict 收紧；★竞态注入（时机定理
#       断言：C<S ⇒ 必可见，用"只增工作负载下读者计数随快照单调不减"表达）
#       与 ★boot 防呆——两项 P3 里程碑出口门禁。
# 工程纪律：容器调用一律 </dev/null；空值即 FAIL；负向计数守卫；后台会话取
#       中间值走 stdin 逐语句（-c 多语句是单个 PQexec 整体返回）；健康收尾。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
CPORT="${CPORT:-5432}"
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
neg() {  # neg <名字> <期望片段> <SQL@端口>
  local hit
  hit=$(PSQL "$3" -Atc "$4" </dev/null 2>&1 | grep -c "$2")
  check "负向:$1" "$hit" "1"
  NEG_RUN=$((NEG_RUN+1))
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

TMPD="${TMPDIR:-/tmp}/tso_si_p3.$$"
mkdir -p "$TMPD"
trap 'rm -rf "$TMPD"' EXIT

restart_coord() {
  DEX /work/pg-install/bin/pg_ctl -D "$CDATA" restart -m fast -l "$CDATA/startup.log" </dev/null >/dev/null 2>&1
  local up="" t
  for t in $(seq 1 45); do
    up=$(PSQL "$CPORT" -Atc "SELECT 1" </dev/null 2>/dev/null) && [[ "$up" == "1" ]] && break; sleep 1
  done
  [[ "$up" == "1" ]]
}

echo "========== [0] 前置：节点/符号/函数/TSO 配置 =========="
up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null)
check "worker :$WPORT 可用" "$up" "1"
CDATA=$(PSQL "$CPORT" -Atc "SHOW data_directory" </dev/null)
DEX rm -f "$CDATA/pg_tso_boot" </dev/null
PSQL "$CPORT" -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION partdist_tso_start_ts(int, bigint) RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_start_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_commit_ts() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_commit_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_heartbeat(int, bigint) RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_heartbeat' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_global_safe_ts() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_global_safe_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_status() RETURNS text
  AS '$libdir/pg_partdist','partdist_tso_status' LANGUAGE C STRICT;
SQL
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL'
SET citus.enable_ddl_propagation TO off;
CREATE OR REPLACE FUNCTION partdist_tso_start_ts(int, bigint) RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_start_ts' LANGUAGE C STRICT;
DROP TABLE IF EXISTS p3si;
CREATE TABLE p3si(id int, v text) WITH (autovacuum_enabled=off);
CREATE OR REPLACE FUNCTION tso_c_start() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_client_start_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION tso_c_commit() RETURNS bigint
  AS '$libdir/pg_partdist','partdist_tso_client_commit_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION sclog_full(oid, bigint) RETURNS text
  AS '$libdir/pg_partdist','partdist_shard_clog_read_full' LANGUAGE C STRICT;
SQL
OID=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p3si'" </dev/null)
check "夹具建表" "$([[ -n "$OID" ]] && echo ok)" "ok"
PSQL "$CPORT" -q -c "ALTER SYSTEM SET pg_partdist.tso_master = on;" </dev/null >/dev/null
PSQL "$CPORT" -q -c "ALTER SYSTEM SET pg_partdist.tso_lease_ms = 3000;" </dev/null >/dev/null
PSQL "$CPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OID}';" </dev/null >/dev/null
PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 2
mk=$(PSQL "$CPORT" -Atc "SHOW pg_partdist.tso_master" </dev/null)
check "TSO 配置生效" "$mk" "on"

echo "========== [1] T3.1 TSO 服务 =========="
neg "非 master 拒服务" "不是 TSO master" "$WPORT" "SELECT partdist_tso_start_ts(2, 0)"
t1=$(PSQL "$CPORT" -Atc "SELECT partdist_tso_start_ts(2, 0)" </dev/null)
check "首号 = 1" "$t1" "1"
check "boot 标记落盘" "$(DEX ls "$CDATA/pg_tso_boot" </dev/null 2>/dev/null | wc -l)" "1"
t4=$(PSQL "$CPORT" -Atc "SELECT partdist_tso_start_ts(3, 2)" </dev/null)
st=$(PSQL "$CPORT" -Atc "SELECT partdist_tso_status()" </dev/null)
check "发号即登记 min(2,新号)" "$(echo "$st" | grep -oc 'node3=2')" "1"
PSQL "$CPORT" -Atc "SELECT partdist_tso_start_ts(8, 0) FROM generate_series(1,200)" </dev/null > "$TMPD/ca.txt" &
PA=$!
PSQL "$CPORT" -Atc "SELECT partdist_tso_start_ts(9, 0) FROM generate_series(1,200)" </dev/null > "$TMPD/cb.txt" &
PB=$!
wait $PA $PB 2>/dev/null
uniqn=$(cat "$TMPD/ca.txt" "$TMPD/cb.txt" | grep -E '^[0-9]+$' | sort -n | uniq | wc -l)
check "并发取号 2×200 全唯一" "$uniqn" "400"
check "会话内单调" "$(sort -nC "$TMPD/ca.txt" && echo ok)" "ok"

echo "========== [2] T3.2 取号通路 =========="
pair=$(PSQL "$WPORT" -Atc "BEGIN; SELECT tso_c_start(); SELECT tso_c_start(); COMMIT;" </dev/null | grep -E '^[0-9]+$' | tr '\n' ',')
check "同事务两次取值相同（缓存）" "${pair%%,*}" "$(echo "$pair" | cut -d, -f2)"
tc=$(PSQL "$WPORT" -Atc "BEGIN; SELECT tso_c_start(); SELECT tso_c_commit(); COMMIT;" </dev/null | grep -E '^[0-9]+$' | tr '\n' ' ')
c_s=$(echo "$tc" | awk '{print $1}'); c_c=$(echo "$tc" | awk '{print $2}')
check "commit_ts > start_ts" "$([[ -n "$c_c" && "$c_c" -gt "$c_s" ]] && echo ok)" "ok"

echo "========== [3] T3.3 ts 落账 + 真 SI =========="
PSQL "$WPORT" -q -c "INSERT INTO p3si VALUES (1,'a'),(2,'b');" </dev/null >/dev/null
S1=$(PSQL "$WPORT" -Atc "SELECT xmin::text::bigint FROM p3si LIMIT 1" </dev/null)
f=$(PSQL "$WPORT" -Atc "SELECT sclog_full($OID::oid,${S1}::bigint)" </dev/null)
sts=$(echo "$f" | grep -oE "sts=[0-9]+" | cut -d= -f2)
cts=$(echo "$f" | grep -oE "cts=[0-9]+" | cut -d= -f2)
check "三列落账（COMMITTED 且 cts>sts>0）" "$([[ "$(echo "$f" | grep -oc 'st=2')" == 1 && -n "$sts" && "$sts" -gt 0 && -n "$cts" && "$cts" -gt "$sts" ]] && echo ok)" "ok"
PSQL "$WPORT" -At > "$TMPD/nr.txt" 2>&1 <<'BGSQL' &
BEGIN;
SELECT count(*) FROM p3si;
SELECT pg_sleep(5);
SELECT count(*) FROM p3si;
COMMIT;
BGSQL
BGA=$!
sleep 2
PSQL "$WPORT" -q -c "INSERT INTO p3si VALUES (10,'concurrent');" </dev/null >/dev/null
wait $BGA 2>/dev/null
n1=$(grep -E '^[0-9]+$' "$TMPD/nr.txt" | head -1); n2=$(grep -E '^[0-9]+$' "$TMPD/nr.txt" | tail -1)
check "★ 不可重复读消失（快照内 $n1=$n2）" "$n1,$n2" "2,2"
check "快照结束后可见（count=3）" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p3si" </dev/null)" "3"
PSQL "$WPORT" -At > "$TMPD/ds.txt" 2>&1 <<'BGSQL' &
BEGIN;
SELECT count(*) FROM p3si;
SELECT pg_sleep(5);
SELECT count(*) FROM p3si;
COMMIT;
BGSQL
BGB=$!
sleep 2
PSQL "$WPORT" -q -c "DELETE FROM p3si WHERE id=10;" </dev/null >/dev/null
wait $BGB 2>/dev/null
d1=$(grep -E '^[0-9]+$' "$TMPD/ds.txt" | head -1); d2=$(grep -E '^[0-9]+$' "$TMPD/ds.txt" | tail -1)
check "★ 删除对称（快照内 $d1=$d2 仍见被删行）" "$d1,$d2" "3,3"

echo "========== [4] T3.4 first-committer-wins 40001 =========="
PSQL "$WPORT" -At > "$TMPD/fw.txt" 2>&1 <<'BGSQL' &
BEGIN;
UPDATE p3si SET v='A' WHERE id=1;
SELECT pg_sleep(4);
COMMIT;
BGSQL
BGW=$!
sleep 2
out=$(PSQL "$WPORT" -Atc "UPDATE p3si SET v='B' WHERE id=1" </dev/null 2>&1)
wait $BGW 2>/dev/null
check "双写冲突后提交者 40001" "$(echo "$out" | grep -c 'could not serialize')" "1"
check "胜者值保留" "$(PSQL "$WPORT" -Atc "SELECT v FROM p3si WHERE id=1" </dev/null)" "A"
PSQL "$WPORT" -At > "$TMPD/rb.txt" 2>&1 <<'BGSQL' &
BEGIN;
UPDATE p3si SET v='X' WHERE id=2;
SELECT pg_sleep(4);
ROLLBACK;
BGSQL
BGR=$!
sleep 2
out=$(PSQL "$WPORT" -Atc "UPDATE p3si SET v='B2' WHERE id=2; SELECT 'ok'" </dev/null 2>&1 | tail -1)
wait $BGR 2>/dev/null
check "对方回滚不误报（本方成功）" "$out" "ok"

echo "========== [5] T3.5 GlobalSafeTs =========="
PSQL "$WPORT" -At > "$TMPD/sa.txt" 2>&1 <<'BGSQL' &
BEGIN;
SELECT tso_c_start();
SELECT pg_sleep(5);
COMMIT;
BGSQL
BGS=$!
sleep 2
TSA=$(grep -E '^[0-9]+$' "$TMPD/sa.txt" | head -1)
SAFE1=$(PSQL "$CPORT" -Atc "SELECT partdist_global_safe_ts()" </dev/null)
check "safe == 最老活跃 start_ts（$TSA）" "$SAFE1" "$TSA"
wait $BGS 2>/dev/null
sleep 2
SAFE2=$(PSQL "$CPORT" -Atc "SELECT partdist_global_safe_ts()" </dev/null)
check "活跃清空后 safe 单调推进" "$([[ -n "$SAFE2" && "$SAFE2" -gt "$TSA" ]] && echo ok)" "ok"
FX=$(PSQL "$CPORT" -Atc "SELECT partdist_tso_start_ts(77, 0)" </dev/null)
sleep 4
SAFE3=$(PSQL "$CPORT" -Atc "SELECT partdist_global_safe_ts()" </dev/null)
check "无人续租的伪节点被租约剔除（safe > $FX）" "$([[ -n "$SAFE3" && "$SAFE3" -gt "$FX" ]] && echo ok)" "ok"

echo "========== [6] ★ 竞态注入（出口门禁：时机定理断言） =========="
# 只增负载：writer 40 笔单事务插入；checker 40 轮独立快照计数。
# 时机定理 ⇒ C < S 的提交必可见 ⇒ 计数序列必须随快照序单调不减且终值=全量。
BASE=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p3si" </dev/null)
( for i in $(seq 1 40); do
    PSQL "$WPORT" -q -c "INSERT INTO p3si VALUES (100+$i,'race');" </dev/null >/dev/null
  done ) &
RW=$!
( for i in $(seq 1 40); do
    PSQL "$WPORT" -Atc "SELECT count(*) FROM p3si" </dev/null
  done ) > "$TMPD/race.txt" &
RC=$!
wait $RW $RC 2>/dev/null
mono=$(awk 'NR>1 && $1 < prev {bad++} {prev=$1} END {print bad+0}' "$TMPD/race.txt")
check "读者计数序列单调不减（零倒退 = 零时机异常）" "$mono" "0"
check "checker 覆盖 40 轮" "$(grep -cE '^[0-9]+$' "$TMPD/race.txt")" "40"
check "终态计数 = 基线+40" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p3si" </dev/null)" "$((BASE + 40))"
SAFE4=$(PSQL "$CPORT" -Atc "SELECT partdist_global_safe_ts()" </dev/null)
SAFE5=$(PSQL "$CPORT" -Atc "SELECT partdist_global_safe_ts()" </dev/null)
check "竞态后 GlobalSafeTs 不变式（单调）" "$([[ "$SAFE5" -ge "$SAFE4" ]] && echo ok)" "ok"

echo "========== [7] T3.6 strict 收紧 + 遗留模式 =========="
r=$(PSQL "$WPORT" -Atc "SET pg_partdist.shard_safety_mode=strict; SELECT count(*) FROM p3si" </dev/null | tail -1)
check "strict + TSO：读自动取号放行" "$r" "$((BASE + 40))"
w=$(PSQL "$WPORT" -Atc "SET pg_partdist.shard_safety_mode=strict; INSERT INTO p3si VALUES (999,'s'); DELETE FROM p3si WHERE id=999; SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "strict + TSO：写放行" "$w" "ok"
PSQL "$WPORT" -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 1
neg "遗留 strict 读被拦（无 ts）" "无 ts" "$WPORT" "SET pg_partdist.shard_safety_mode=strict; SELECT count(*) FROM p3si"
check "遗留 permissive 读正常（P2 语义）" "$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p3si" </dev/null)" "$((BASE + 40))"
check "遗留模式取号=0" "$(PSQL "$WPORT" -Atc "SELECT tso_c_start()" </dev/null)" "0"
PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres';" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 1

echo "========== [8] ★ boot 防呆（出口门禁：重启拒发号） =========="
restart_coord
check "coordinator 重启存活" "$?" "0"
neg "重启后发号被拒（响亮停摆）" "boot 标记" "$CPORT" "SELECT partdist_tso_start_ts(2, 0)"
st=$(PSQL "$CPORT" -Atc "SELECT partdist_tso_status()" </dev/null)
check "status 报 blocked=t" "$(echo "$st" | grep -oc 'blocked=t')" "1"
# errdetail 会内嵌远端 "ERROR:" 串，按特征文案计数而非 ERROR 行数
fcw=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p3si" </dev/null 2>&1 | grep -c "TSO 不可达")
check "worker 侧读经 RPC 传导 fail-closed" "$fcw" "1"
DEX rm -f "$CDATA/pg_tso_boot" </dev/null
restart_coord
check "删标记重启 = 新纪元" "$?" "0"
tn=$(PSQL "$CPORT" -Atc "SELECT partdist_tso_start_ts(2, 0)" </dev/null)
check "新纪元从 1 起发" "$tn" "1"

echo "========== [9] 负向守卫 + 清理 + 节点健康 =========="
check "负向用例计数守卫（应跑 3 条）" "$NEG_RUN" "3"
PSQL "$WPORT" -q -c "DROP TABLE p3si;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
PSQL "$WPORT" -q -c "DROP FUNCTION IF EXISTS tso_c_start(); DROP FUNCTION IF EXISTS tso_c_commit(); DROP FUNCTION IF EXISTS sclog_full(oid,bigint); DROP FUNCTION IF EXISTS partdist_tso_start_ts(int,bigint);" </dev/null >/dev/null
PSQL "$CPORT" -q -c "ALTER SYSTEM RESET pg_partdist.tso_master;" </dev/null >/dev/null
PSQL "$CPORT" -q -c "ALTER SYSTEM RESET pg_partdist.tso_lease_ms;" </dev/null >/dev/null
PSQL "$CPORT" -q -c "DROP FUNCTION IF EXISTS partdist_tso_start_ts(int,bigint); DROP FUNCTION IF EXISTS partdist_tso_commit_ts(); DROP FUNCTION IF EXISTS partdist_tso_heartbeat(int,bigint); DROP FUNCTION IF EXISTS partdist_global_safe_ts(); DROP FUNCTION IF EXISTS partdist_tso_status();" </dev/null >/dev/null
DEX rm -f "$CDATA/pg_tso_boot" </dev/null
restart_coord >/dev/null 2>&1
health_check_no_crash

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
exit "$FAIL"
