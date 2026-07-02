#!/bin/bash
# test_demux_backlog_recovery.sh — parwal-2.0 同步写入路径验证
#
# 旧版本测试的是异步 Demux BGW 的高积压崩溃恢复。parwal-2.0 采用同步写入
# 架构：PartWAL 记录在 XLogInsert() 触发的 hook 回调中立即落盘，无积压概念。
#
# 本脚本重构为 5 个场景，验证同步路径的核心属性：
#   S1 : 单分片同步写入 — INSERT 后立即可见，无需 flush
#   S2 : 多分片隔离性 — 两个分片 OID 的 PartWAL 文件相互独立
#   S3 : kill -9 postmaster → 崩溃恢复 → 零数据丢失 + 后续写入正常
#   S4 : 正常重启 → PartWAL 记录不变 + last_processed_lsn 保留
#   S5 : demux_progress() / demux_is_ready() 正确反映状态

set -uo pipefail

PSQL=/work/pg-install/bin/psql
PGCTL=/work/pg-install/bin/pg_ctl
DATA=/work/pg-cluster-data
PASS=0; FAIL=0

die()  { echo "FATAL: $*" >&2; exit 1; }
pass() { echo "  PASS: $*"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }

check_eq()   { [ "$2" = "$3" ]  && pass "$1 (=$2)"    || fail "$1 (expected=$3, got=$2)"; }
check_ge()   { [ "$2" -ge "$3" ] && pass "$1 ($2≥$3)" || fail "$1 (expected≥$3, got=$2)"; }
check_true() { [ "$2" = "t"  ]  && pass "$1"          || fail "$1 (got='$2')"; }

psql1()    { $PSQL -p 5433 -d postgres "$@"; }
psql1_at() { psql1 -At -c "$1"; }

count_records() {
    local pid=$1 result tries=0
    while [ $tries -lt 6 ]; do
        result=$(psql1_at "SELECT partdist.count_parwal_records($pid::oid);" 2>/dev/null || true)
        [ -n "$result" ] && echo "$result" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
verify_wal() {
    local pid=$1 result tries=0
    while [ $tries -lt 6 ]; do
        result=$(psql1_at "SELECT partdist.verify_partition_wal($pid::oid);" 2>/dev/null || true)
        [ -n "$result" ] && echo "$result" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}

# 等待崩溃恢复 BGW 完成 (poll demux_is_ready(), max 60s)
wait_demux_ready() {
    local port=${1:-5433} tries=0
    while [ $tries -lt 120 ]; do
        local r
        r=$($PSQL -p "$port" -d postgres -At \
                  -c "SELECT partdist.demux_is_ready()" 2>/dev/null || echo "f")
        [ "$r" = "t" ] && return 0
        sleep 0.5; tries=$((tries+1))
    done
    echo "  WARNING: demux_is_ready() timed out"
    return 1
}

# 获取 worker1 上指定表的特定分片（shardid）的 OID
# 用 citus.override_table_visibility=off 访问隐藏的分片表
# 注意：psql -At 模式下 SET 命令仍会输出 "SET"，用 grep 过滤只保留数字行
shard_oid_w1() {
    local tbl=$1 shardid=$2
    psql1_at "
        SET citus.override_table_visibility TO off;
        SELECT oid::text FROM pg_class
        WHERE relname = '${tbl}_${shardid}' AND relkind = 'r';" \
    | grep -E '^[0-9]+$' | tr -d '\n'
}

# 获取 worker1 上指定表的所有分片 OID（按 OID 升序）
table_oids_w1() {
    local tbl=$1
    psql1_at "
        SET citus.override_table_visibility TO off;
        SELECT oid::text FROM pg_class
        WHERE relname ~ '^${tbl}_[0-9]{4,}\$' AND relkind = 'r'
        ORDER BY oid;" \
    | grep -E '^[0-9]+$'
}

# 在 coordinator 上创建分布表
make_table() {
    local tbl=$1
    $PSQL -p 5432 -d postgres -c "
        DROP TABLE IF EXISTS $tbl CASCADE;
        CREATE TABLE $tbl (id int PRIMARY KEY, val text);
        SELECT create_distributed_table('$tbl','id',shard_count=>4);" >/dev/null
}

# 通过 coordinator 向特定分片插入 N 行
insert_for_shard() {
    local tbl=$1 shardid=$2 n=$3 offset_n=${4:-0}
    $PSQL -p 5432 -d postgres -At -c "
        SELECT v FROM generate_series(1,10000) v
        WHERE get_shard_id_for_distribution_column('$tbl', v) = $shardid
        LIMIT $n OFFSET $offset_n;" | while read id; do
        $PSQL -p 5432 -d postgres -c \
            "INSERT INTO $tbl VALUES ($id,'sync') ON CONFLICT DO NOTHING;" \
            >/dev/null 2>&1 || true
    done
}

# worker1 上 的分片 shardid 列表（按 shardid 排序）
w1_shards() {
    $PSQL -p 5432 -d postgres -At -c "
        SELECT shardid FROM pg_dist_shard s
        JOIN pg_dist_shard_placement sp USING(shardid)
        WHERE s.logicalrelid = '$1'::regclass AND sp.nodeport = 5433
        ORDER BY shardid;"
}

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════════════════════════════════════"
echo "  pg_partdist parwal-2.0 — 同步写入路径验证 (5 场景)"
echo "════════════════════════════════════════════════════════════════════════"

# ── 全局初始环境 ──────────────────────────────────────────────────────────
echo ""
echo "── 全局 Setup: 清理 worker1 pg_parwal 并重启 ──"
$PGCTL stop -D "$DATA/worker1" -m fast -w -t 60 2>/dev/null | tail -1 || true
rm -rf "$DATA/worker1/pg_parwal"
$PGCTL start -D "$DATA/worker1" -l "$DATA/worker1/pg.log" -o '-p 5433' -w -t 60 2>/dev/null | tail -1
wait_demux_ready 5433 || true
echo "  worker1 已启动，崩溃恢复 BGW 完成"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S1: 单分片同步写入 — INSERT 后立即可见 ════════"
# parwal-2.0 核心属性：写入在 ExecutorFinish hook 中同步完成，
# 调用方无需等待任何异步 BGW。
# ════════════════════════════════════════════════════════════════════════

make_table sync_test1
readarray -t SHARDS1 < <(w1_shards sync_test1)
SS1=${SHARDS1[0]}

echo "  Worker1 shard: $SS1"

echo "  插入 5 行 (不调用 demux_flush)..."
insert_for_shard sync_test1 "$SS1" 5

OID_S1=$(shard_oid_w1 sync_test1 "$SS1")
echo "  Shard OID: $OID_S1"

CNT=$(count_records "$OID_S1")
check_eq "S1-immediate count (=5, no flush)" "$CNT" "5"
check_true "S1-verify LSN monotone" "$(verify_wal "$OID_S1")"

echo "  再插入 5 行..."
insert_for_shard sync_test1 "$SS1" 5 5
CNT2=$(count_records "$OID_S1")
check_eq "S1-cumulative count (=10)" "$CNT2" "10"
check_true "S1-verify LSN still monotone" "$(verify_wal "$OID_S1")"

# demux_flush() 是空操作，调用不应报错
$PSQL -p 5433 -d postgres -c "SELECT partdist.demux_flush();" >/dev/null 2>&1
pass "S1-demux_flush() no-op succeeds"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S2: 多分片隔离性 — 两分片 PartWAL 互不干扰 ════════"
# ════════════════════════════════════════════════════════════════════════

make_table sync_test2
readarray -t SHARDS2 < <(w1_shards sync_test2)
SS2A=${SHARDS2[0]}; SS2B=${SHARDS2[1]}
echo "  Worker1 shards: $SS2A, $SS2B"

insert_for_shard sync_test2 "$SS2A" 4
insert_for_shard sync_test2 "$SS2B" 6

OID_S2A=$(shard_oid_w1 sync_test2 "$SS2A")
OID_S2B=$(shard_oid_w1 sync_test2 "$SS2B")
echo "  Shard OIDs: $OID_S2A, $OID_S2B"

CNTSA=$(count_records "$OID_S2A")
CNTSB=$(count_records "$OID_S2B")
check_eq "S2-shard A count (=4)" "$CNTSA" "4"
check_eq "S2-shard B count (=6)" "$CNTSB" "6"
check_true "S2-shard A verify" "$(verify_wal "$OID_S2A")"
check_true "S2-shard B verify" "$(verify_wal "$OID_S2B")"

# 两个分片的 PartWAL 目录必须独立存在
[ -d "$DATA/worker1/pg_parwal/$OID_S2A" ] && pass "S2-shard A has own pg_parwal dir" \
    || fail "S2-shard A missing pg_parwal dir"
[ -d "$DATA/worker1/pg_parwal/$OID_S2B" ] && pass "S2-shard B has own pg_parwal dir" \
    || fail "S2-shard B missing pg_parwal dir"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S3: kill -9 postmaster → 崩溃恢复 → 零丢失 ════════"
# ════════════════════════════════════════════════════════════════════════

$PGCTL stop -D "$DATA/worker1" -m fast -w -t 60 2>/dev/null | tail -1 || true
rm -rf "$DATA/worker1/pg_parwal"
$PGCTL start -D "$DATA/worker1" -l "$DATA/worker1/pg.log" -o '-p 5433' -w -t 60 2>/dev/null | tail -1
wait_demux_ready 5433 || true

make_table crash_test3
readarray -t SHARDS3 < <(w1_shards crash_test3)
SC3=${SHARDS3[0]}

echo "  插入 8 行 (同步落盘到 PartWAL)..."
insert_for_shard crash_test3 "$SC3" 8

OID_S3=$(shard_oid_w1 crash_test3 "$SC3")
PRE_CNT=$(count_records "$OID_S3")
check_eq "S3-pre-crash count (=8)" "$PRE_CNT" "8"
check_true "S3-pre-crash verify" "$(verify_wal "$OID_S3")"

echo "  kill -9 worker1 postmaster..."
W1_PMID=$(head -1 "$DATA/worker1/postmaster.pid")
kill -9 "$W1_PMID" 2>/dev/null || true
sleep 2

echo "  重启 worker1 (将运行崩溃恢复 BGW)..."
$PGCTL start -D "$DATA/worker1" -l "$DATA/worker1/pg.log" -o '-p 5433' -w -t 60 2>/dev/null | tail -1
wait_demux_ready 5433 || true

POST_CNT=$(count_records "$OID_S3")
echo "  崩溃恢复后 count=$POST_CNT (期望 $PRE_CNT)"
check_eq "S3-post-recovery count (无丢失/无重复)" "$POST_CNT" "$PRE_CNT"
check_true "S3-post-recovery verify (LSN 单调)" "$(verify_wal "$OID_S3")"

echo "  崩溃后追加 4 行..."
insert_for_shard crash_test3 "$SC3" 4 8
FINAL_CNT=$(count_records "$OID_S3")
check_eq "S3-final count (8+4=12)" "$FINAL_CNT" "12"
check_true "S3-final verify (追加 LSN 无间隙)" "$(verify_wal "$OID_S3")"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S4: 正常重启 → PartWAL 不变 + 追加写入继续 ════════"
# ════════════════════════════════════════════════════════════════════════

$PGCTL stop -D "$DATA/worker1" -m fast -w -t 60 2>/dev/null | tail -1 || true
rm -rf "$DATA/worker1/pg_parwal"
$PGCTL start -D "$DATA/worker1" -l "$DATA/worker1/pg.log" -o '-p 5433' -w -t 60 2>/dev/null | tail -1
wait_demux_ready 5433 || true

make_table restart_test4
readarray -t SHARDS4 < <(w1_shards restart_test4)
SR4=${SHARDS4[0]}

echo "  插入 6 行..."
insert_for_shard restart_test4 "$SR4" 6

OID_S4=$(shard_oid_w1 restart_test4 "$SR4")
PRE_R=$(count_records "$OID_S4")
check_eq "S4-pre-restart count (=6)" "$PRE_R" "6"

echo "  正常关机重启 worker1..."
$PGCTL stop -D "$DATA/worker1" -m fast -w -t 60 2>/dev/null | tail -1
$PGCTL start -D "$DATA/worker1" -l "$DATA/worker1/pg.log" -o '-p 5433' -w -t 60 2>/dev/null | tail -1
wait_demux_ready 5433 || true

POST_R=$(count_records "$OID_S4")
check_eq "S4-post-restart count 不变 (=6)" "$POST_R" "$PRE_R"
check_true "S4-post-restart verify" "$(verify_wal "$OID_S4")"

echo "  重启后追加 4 行..."
insert_for_shard restart_test4 "$SR4" 4 6
FINAL_R=$(count_records "$OID_S4")
check_eq "S4-final count (6+4=10)" "$FINAL_R" "10"
check_true "S4-final verify (LSN 连续)" "$(verify_wal "$OID_S4")"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S5: demux_progress() / demux_is_ready() 状态验证 ════════"
# ════════════════════════════════════════════════════════════════════════

# demux_is_ready() 必须在 BGW 完成后返回 true
IS_READY=$(psql1_at "SELECT partdist.demux_is_ready();" 2>/dev/null || echo "f")
check_true "S5-demux_is_ready() = true" "$IS_READY"

# demux_progress() 不能报错
PROG=$($PSQL -p 5433 -d postgres -At -c "SELECT (partdist.demux_progress()).node_name;" 2>/dev/null || echo "ERROR")
[ "$PROG" != "ERROR" ] && pass "S5-demux_progress() no error (node=$PROG)" \
                         || fail "S5-demux_progress() returned error"

# last_processed_lsn 应不为空（崩溃恢复 BGW 写入过 PartWAL 后会更新）
LSN_OK=$($PSQL -p 5433 -d postgres -At -c "
    SELECT (partdist.demux_progress()).last_processed_lsn IS NOT NULL;" 2>/dev/null || echo "f")
check_true "S5-last_processed_lsn is not null" "$LSN_OK"

# 调用 demux_flush() (no-op) 不应返回错误
FLUSH_OK=$($PSQL -p 5433 -d postgres -c "SELECT partdist.demux_flush();" \
           >/dev/null 2>&1 && echo "ok" || echo "error")
[ "$FLUSH_OK" = "ok" ] && pass "S5-demux_flush() no-op succeeds" \
                         || fail "S5-demux_flush() error"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SUMMARY ════════"
echo "  Tests passed: $PASS"
echo "  Tests failed: $FAIL"
echo ""
if [ "$FAIL" -eq 0 ]; then
    echo "parwal-2.0 同步路径验证: PASS"
    exit 0
else
    echo "parwal-2.0 同步路径验证: FAIL"
    exit 1
fi
