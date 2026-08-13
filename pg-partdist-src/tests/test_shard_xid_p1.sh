#!/usr/bin/env bash
# [宿主机] P1 分片 xid 打标验收（TX_TSO_MVCC_DEV_PLAN.md T1.9 之一）。
#
# 覆盖：T1.1 谓词/TOAST 归属、T1.2 发号+水位崩溃安全、T1.3 事务绑定、
#       T1.4 打标（xmin/xmax 序列）、T1.6 自见性/跨会话可见性、T1.7 冲突等待
#       两向（首提交者胜=行锁禁令报错；前者回滚后者继续）、T1.8 严格模式、
#       负向全集（SAVEPOINT/行锁/隔离级别/维护命令/PREPARE/COPY FREEZE）、
#       SELECT 不触发剪枝（P1_PRECHECK 结论 D）、普通表对照。
# 三方 pagecmp 与崩溃点扫描在 test_shard_pagecmp_p1.sh。
#
# 期望值全部动态计算：S = 本表首个分片 xid（新表新水位文件时为 3，重复跑
# 或水位残留时为文件水位值），之后的序列断言全部相对 S。
set -u

CONTAINER="${CONTAINER:-pg-citus-tx2-container}"
WPORT="${WPORT:-5433}"
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }

check() {  # check <名字> <实际> <期望>（空实际值一律 FAIL，见 R1 脚本头部说明）
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

echo "========== [0] 前置：节点就绪 + 夹具 =========="
up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null)
check "worker :$WPORT 可用" "$up" "1"

PSQL "$WPORT" -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS p1_shard;
DROP TABLE IF EXISTS p1_native;
CREATE TABLE p1_shard(id int, v int, pad text)
  WITH (autovacuum_enabled=off, toast.autovacuum_enabled=off);
ALTER TABLE p1_shard ALTER COLUMN pad SET STORAGE EXTERNAL;
CREATE TABLE p1_native(id int) WITH (autovacuum_enabled=off);
SET citus.enable_ddl_propagation TO off;
CREATE EXTENSION IF NOT EXISTS pageinspect;
SQL
OID=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='p1_shard'" </dev/null)
check "夹具建表" "$([[ -n "$OID" ]] && echo ok)" "ok"

PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OID}';" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
guc=""
for t in $(seq 1 10); do
  guc=$(PSQL "$WPORT" -Atc "SHOW pg_partdist.shard_relids" </dev/null)
  [[ "$guc" == "$OID" ]] && break; sleep 1
done
check "白名单 GUC 生效" "$guc" "$OID"

DATADIR=$(PSQL "$WPORT" -Atc "SHOW data_directory" </dev/null)

echo "========== [1] 打标序列（T1.1–T1.4） =========="
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q -c "BEGIN; INSERT INTO p1_shard VALUES (1,0,'a'),(2,0,'b'),(3,0,'c'); COMMIT;" </dev/null
S=$(PSQL "$WPORT" -Atc "SELECT t_xmin FROM heap_page_items(get_raw_page('p1_shard',0)) WHERE lp=1" </dev/null)
check "首事务已领号（S=${S}≥3）" "$([[ -n "$S" && "$S" -ge 3 ]] && echo ok)" "ok"
n_same=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM heap_page_items(get_raw_page('p1_shard',0)) WHERE t_xmin=${S}" </dev/null)
check "单事务 3 行同号" "$n_same" "3"

PSQL "$WPORT" -q -c "INSERT INTO p1_shard VALUES (4,0,'d');" </dev/null
PSQL "$WPORT" -q -c "BEGIN; SELECT count(*) FROM p1_shard; COMMIT;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "INSERT INTO p1_shard VALUES (5,0,'e');" </dev/null
x4=$(PSQL "$WPORT" -Atc "SELECT t_xmin FROM heap_page_items(get_raw_page('p1_shard',0)) WHERE lp=4" </dev/null)
x5=$(PSQL "$WPORT" -Atc "SELECT t_xmin FROM heap_page_items(get_raw_page('p1_shard',0)) WHERE lp=5" </dev/null)
check "第二事务 = S+1" "$x4" "$((S+1))"
check "只读事务不领号（下一事务 = S+2）" "$x5" "$((S+2))"

PSQL "$WPORT" -q -c "INSERT INTO p1_shard VALUES (6,0,repeat('T',9000));" </dev/null
toast_rel=$(PSQL "$WPORT" -Atc "SELECT reltoastrelid::regclass::text FROM pg_class WHERE oid=${OID}" </dev/null)
tdistinct=$(PSQL "$WPORT" -Atc "SELECT string_agg(DISTINCT t_xmin::text,',') FROM heap_page_items(get_raw_page('${toast_rel}',0))" </dev/null)
check "TOAST 随属主同号（= S+3）" "$tdistinct" "$((S+3))"

PSQL "$WPORT" -q -c "DELETE FROM p1_shard WHERE id=1;" </dev/null
xmax1=$(PSQL "$WPORT" -Atc "SELECT t_xmax FROM heap_page_items(get_raw_page('p1_shard',0)) WHERE lp=1" </dev/null)
check "DELETE 盖分片 xmax（= S+4）" "$xmax1" "$((S+4))"

PSQL "$WPORT" -q -c "INSERT INTO p1_native VALUES (1);" </dev/null
nx=$(PSQL "$WPORT" -Atc "SELECT xmin::text::bigint FROM p1_native WHERE id=1" </dev/null)
check "对照表 xmin 为原生号（远大于分片序列 S+100=$((S+100))）" \
      "$([[ -n "$nx" && "$nx" -gt $((S+100)) ]] && echo ok)" "ok"

echo "========== [2] 可见性（T1.6） =========="
r=$(PSQL "$WPORT" -Atc "BEGIN; INSERT INTO p1_shard VALUES (10,0,'x'),(11,0,'x'); SELECT count(*) FROM p1_shard; ROLLBACK;" </dev/null | grep -E '^[0-9]+$')
check "自见性：事务内可见自己未提交行" "$r" "7"
r=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p1_shard" </dev/null)
check "回滚后不可见" "$r" "5"

PSQL "$WPORT" -q -c "BEGIN; INSERT INTO p1_shard VALUES (20,0,'y'); SELECT pg_sleep(4); COMMIT;" </dev/null >/dev/null &
BG=$!
sleep 1.5
r=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p1_shard" </dev/null)
check "跨会话：未提交不可见" "$r" "5"
wait "$BG"
r=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p1_shard" </dev/null)
check "跨会话：提交后可见" "$r" "6"

r=$(PSQL "$WPORT" -Atc "BEGIN; DELETE FROM p1_shard WHERE id=20; SELECT count(*) FROM p1_shard; ROLLBACK;" </dev/null | grep -E '^[0-9]+$')
check "DELETE 自见（事务内立即生效）" "$r" "5"
r=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p1_shard" </dev/null)
check "DELETE 回滚后行恢复" "$r" "6"

echo "========== [3] 冲突等待（T1.7） =========="
PSQL "$WPORT" -q -c "BEGIN; UPDATE p1_shard SET v=1 WHERE id=2; SELECT pg_sleep(4); COMMIT;" </dev/null >/dev/null &
BG=$!
sleep 1
t0=$(date +%s%N)
w1err=$(PSQL "$WPORT" -c "UPDATE p1_shard SET v=2 WHERE id=2;" </dev/null 2>&1 | grep -c "row-level locking is not supported")
t1=$(date +%s%N); w1ms=$(( (t1-t0)/1000000 ))
wait "$BG"
check "前者提交：后者按首提交者胜报行锁禁令" "$w1err" "1"
check "后者确实等待了（${w1ms}ms > 2000）" "$([[ "$w1ms" -gt 2000 ]] && echo ok)" "ok"
r=$(PSQL "$WPORT" -Atc "SELECT v FROM p1_shard WHERE id=2" </dev/null)
check "终值 = 前者的写入" "$r" "1"

PSQL "$WPORT" -q -c "BEGIN; UPDATE p1_shard SET v=7 WHERE id=3; SELECT pg_sleep(4); ROLLBACK;" </dev/null >/dev/null &
BG=$!
sleep 1
t0=$(date +%s%N)
w2ok=$(PSQL "$WPORT" -c "UPDATE p1_shard SET v=9 WHERE id=3;" </dev/null 2>&1 | grep -c "^UPDATE 1")
t1=$(date +%s%N); w2ms=$(( (t1-t0)/1000000 ))
wait "$BG"
check "前者回滚：后者等待后正常继续" "$w2ok" "1"
check "后者确实等待了（${w2ms}ms > 2000）" "$([[ "$w2ms" -gt 2000 ]] && echo ok)" "ok"
r=$(PSQL "$WPORT" -Atc "SELECT v FROM p1_shard WHERE id=3" </dev/null)
check "终值 = 后者的写入" "$r" "9"

echo "========== [4] 发号崩溃安全（T1.2/T1.5） =========="
WM=$(DEX od -An -tu4 "${DATADIR}/pg_shard_xid/${OID}" </dev/null | tr -d ' ')
check "水位文件存在且为 4096 批量（$WM）" "$([[ -n "$WM" && $((WM % 4096)) -eq 3 ]] && echo ok)" "ok"
pre_md5=$(PSQL "$WPORT" -Atc "SELECT md5(string_agg(encode(get_raw_page('p1_shard',b),'hex'),'' ORDER BY b)) FROM generate_series(0,(pg_relation_size('p1_shard')/8192)::int-1) b" </dev/null)

DEX /work/pg-install/bin/pg_ctl -D "$DATADIR" -m immediate stop </dev/null >/dev/null 2>&1
sleep 1
DEX /work/pg-install/bin/pg_ctl -D "$DATADIR" -l "$DATADIR/startup.log" start </dev/null >/dev/null 2>&1
up=""
for t in $(seq 1 30); do
  up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null 2>/dev/null) && [[ "$up" == "1" ]] && break
  sleep 1
done
check "immediate 崩溃后节点恢复" "$up" "1"

post_md5=$(PSQL "$WPORT" -Atc "SELECT md5(string_agg(encode(get_raw_page('p1_shard',b),'hex'),'' ORDER BY b)) FROM generate_series(0,(pg_relation_size('p1_shard')/8192)::int-1) b" </dev/null)
check "崩溃恢复 redo 页面与崩溃前逐字节一致" "$post_md5" "$pre_md5"

PSQL "$WPORT" -q -c "INSERT INTO p1_shard VALUES (30,0,'z');" </dev/null
xr=$(PSQL "$WPORT" -Atc "SELECT max(t_xmin::text::bigint) FROM heap_page_items(get_raw_page('p1_shard',0)) WHERE t_xmin IS NOT NULL" </dev/null)
check "重启后从水位续发不重号（= ${WM}）" "$xr" "$WM"
WM2=$(DEX od -An -tu4 "${DATADIR}/pg_shard_xid/${OID}" </dev/null | tr -d ' ')
check "水位推进一批（= ${WM}+4096）" "$WM2" "$((WM+4096))"

echo "========== [5] 负向全集（该报错的都报错） =========="
# 基线行数动态取：[4] 的崩溃清空了临时提交表，历史中止事务按"缺席=已提交"
# 复活为可见（P1 桩已知限制），所以不能写死崩溃前的行数。
base_rows=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p1_shard" </dev/null)
neg() {  # neg <名字> <期望片段> <SQL...>
  local name=$1 frag=$2; shift 2
  local hit
  hit=$(PSQL "$WPORT" "$@" </dev/null 2>&1 | grep -c "$frag")
  check "负向:$name" "$hit" "1"
  NEG_RUN=$((NEG_RUN+1))
}
NEG_RUN=0
neg "SAVEPOINT 内写"      "不支持在子事务"                  -c "BEGIN; SAVEPOINT s; INSERT INTO p1_shard VALUES (90,0,'x'); ROLLBACK;"
neg "FOR UPDATE"          "row-level locking is not supported" -c "SELECT id FROM p1_shard WHERE id=2 FOR UPDATE;"
neg "FOR SHARE"           "row-level locking is not supported" -c "SELECT id FROM p1_shard WHERE id=2 FOR SHARE;"
neg "SERIALIZABLE 读"     "SERIALIZABLE 隔离级别"           -c "BEGIN ISOLATION LEVEL SERIALIZABLE; SELECT count(*) FROM p1_shard; ROLLBACK;"
neg "VACUUM 点名"         "VACUUM 不允许作用于分片打标表"    -c "VACUUM p1_shard;"
neg "ANALYZE 点名"        "ANALYZE 不允许作用于分片打标表"   -c "ANALYZE p1_shard;"
neg "整库 VACUUM"         "不允许整库 VACUUM"               -c "VACUUM;"
neg "CLUSTER 点名"        "CLUSTER 不允许作用于分片打标表"   -c "CLUSTER p1_shard;"
neg "CREATE INDEX 点名"   "CREATE INDEX 不允许作用于分片打标表" -c "CREATE INDEX ON p1_shard(id);"
neg "PREPARE 含分片写"    "PREPARE TRANSACTION"             -c "BEGIN; INSERT INTO p1_shard VALUES (91,0,'x'); PREPARE TRANSACTION 'p1t';"
neg "COPY FREEZE"         "COPY FREEZE"                     -c "BEGIN; TRUNCATE p1_shard; COPY p1_shard FROM PROGRAM 'echo 93,0,x' WITH (FORMAT csv, FREEZE);"
neg "严格模式读"          "读被拦截"                        -c "SET pg_partdist.shard_safety_mode=strict; SELECT count(*) FROM p1_shard;"
neg "严格模式写"          "写入被拦截"                      -c "SET pg_partdist.shard_safety_mode=strict; INSERT INTO p1_shard VALUES (92,0,'x');"
check "负向用例计数守卫（应跑 13 条）" "$NEG_RUN" "13"
r=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p1_shard" </dev/null)
check "负向用例未破坏数据（行数不变）" "$r" "$base_rows"

echo "========== [6] SELECT 不触发剪枝（结论 D） =========="
lp_before=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM heap_page_items(get_raw_page('p1_shard',0))" </dev/null)
PSQL "$WPORT" -q -c "SELECT count(*) FROM p1_shard;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT count(*) FROM p1_shard;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT count(*) FROM p1_shard;" </dev/null >/dev/null
lp_after=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM heap_page_items(get_raw_page('p1_shard',0))" </dev/null)
check "反复 SELECT 后行指针数不变（死元组未被剪掉）" "$lp_after" "$lp_before"

echo "========== [7] 普通表回归对照 =========="
PSQL "$WPORT" -q -c "CREATE INDEX ON p1_native(id);" -c "VACUUM p1_native;" -c "ANALYZE p1_native;" </dev/null >/dev/null
ok=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM p1_native" </dev/null)
check "普通表 CREATE INDEX/VACUUM/ANALYZE 正常" "$ok" "1"

echo "========== [8] 清理 + 节点健康 =========="
PSQL "$WPORT" -q -c "DROP TABLE p1_shard, p1_native;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
PSQL "$WPORT" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
DEX rm -f "${DATADIR}/pg_shard_xid/${OID}" </dev/null
health_check_no_crash

echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
exit "$FAIL"
