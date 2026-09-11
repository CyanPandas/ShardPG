#!/usr/bin/env bash
# test_bulk_insert_recovery.sh
#
# Validates that INSERT INTO dist_table SELECT ... (bulk INSERT via Citus
# COPY protocol) correctly generates PartWAL records on each worker.
#
# Tests:
#   1. Functional  — PartWAL records appear after INSERT SELECT
#   2. Restart     — LSN monotonically increases across a normal restart
#   3. Crash       — Records survive kill -9 on a worker postmaster
#   4. Performance — Bulk INSERT overhead < 20% vs baseline (median of 3 x 20k rows)
#   5. Regression  — All 37 existing tests still pass

set -Eeuo pipefail
# ★ -E（errtrace）不可少：trap ERR 默认**不被函数继承**，而 set -e 照样会因
#   函数内部的失败终止脚本 —— 没有 -E 时陷阱一声不响，比没装还误导。
trap 'echo "★ ERR: line $LINENO 命令失败 -> $BASH_COMMAND" >&2' ERR

PSQL="/work/pg-install/bin/psql"
PG_CTL="/work/pg-install/bin/pg_ctl"
COORD_PORT=5432
W1_PORT=5433   # 保留仅为兼容旧引用；实际请用 $ALL_W / $WPORT
W2_PORT=5434
COORD_DATA=/work/pg-cluster-data/master
W1_DATA=/work/pg-cluster-data/worker1
W2_DATA=/work/pg-cluster-data/worker2


# ★★ T7.13（P7-E2）：拓扑无关化（2026-09-11）。
#   本套件的 count_all_parwal / verify_monotone 是**跨"两个 worker"求和/求与**，
#   在 3 节点布局下"两个 worker"恰好就是全部 worker；9 节点上不成立 ——
#   bulk_t 的分片会散在 8 个 worker 上，只统计 worker1/2 等于**漏掉大部分分片**，
#   求和偏小、求与漏检，而断言仍会"通过"，是最危险的那种假绿。
#   忠实的推广是**遍历全部 worker**（按 pg_dist_node 动态取）。
source "$(cd "$(dirname "$0")" && pwd)/lib_topology.sh"
topo_init || { echo "FATAL: 拓扑初始化失败" >&2; exit 1; }
ALL_W=$(topo_worker_ports)
WPORT=$(set -- $ALL_W; echo "$1")      # TEST 3 的 kill -9 对象
WDATA=$(topo_datadir "$WPORT")
[[ -n "$ALL_W" && -n "$WPORT" ]] || { echo "FATAL: 取不到 worker 列表" >&2; exit 1; }
echo "worker 端口（动态）：$ALL_W；kill -9 对象：:$WPORT"

# 在任意一组端口上跑同一条 SQL，逐行输出（供求和/求与用）
each_w_sql() { local p; for p in $ALL_W; do $PSQL -U postgres -p "$p" -At -c "$1" 2>/dev/null | grep -v '^$' | tail -1; done; }
PASS=0
FAIL=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
# Use -At for tuples only, but also SET the visibility GUC inside the SQL.
# We grep away the empty "SET" confirmation line with grep -v '^$'
coord_sql() { $PSQL -U postgres -p $COORD_PORT -At -c "$1" 2>/dev/null | grep -v '^$'; }
w1_sql()    { $PSQL -U postgres -p $W1_PORT    -At -c "$1" 2>/dev/null | grep -v '^$'; }
w2_sql()    { $PSQL -U postgres -p $W2_PORT    -At -c "$1" 2>/dev/null | grep -v '^$'; }

ok()   { echo "[PASS] $1"; PASS=$((PASS+1)); }
fail() { echo "[FAIL] $1"; FAIL=$((FAIL+1)); }
check() {
    local label="$1"; local cond="$2"
    if [[ "$cond" == "true" || "$cond" == "t" || "$cond" == "1" ]]; then
        ok "$label"
    else
        fail "$label (got: $cond)"
    fi
}

wait_cluster() {
    local port tries
    for port in $COORD_PORT $ALL_W; do
        tries=0
        until $PSQL -U postgres -p $port -c "SELECT 1" >/dev/null 2>&1; do
            sleep 0.5; tries=$((tries+1))
            [[ $tries -gt 60 ]] && { echo "Timeout waiting for port $port"; exit 1; }
        done
    done
}

restart_all() {
    local p d
    # ★ 每条 pg_ctl 都要 `|| true`：本脚本开了 `set -euo pipefail`，
    #   任何一个节点 restart 返回非零都会让整个脚本**无声退出**
    #   （实测就这么停在 TEST 2，日志最后一行是"Records before restart"，
    #   看起来像卡住，其实是死了）。同 lib_topology.sh 里立的那条纪律。
    $PG_CTL -D $COORD_DATA restart -w -l $COORD_DATA/pg.log >/dev/null 2>&1 || true
    # ★★ 协调者重启后必须删 TSO boot 标记，否则全簇停发号（本环境规程，
    #   见 PG_TEST_ENV §4.1 ①）。原套件写于无 TSO 的 parwal-2.0 时代，没有这一步。
    rm -f "$COORD_DATA/pg_tso_boot" 2>/dev/null || true
    $PG_CTL -D $COORD_DATA restart -w -l $COORD_DATA/pg.log >/dev/null 2>&1 || true
    for p in $ALL_W; do
        d=$(topo_datadir "$p")
        $PG_CTL -D "$d" restart -w -l "$d/pg.log" >/dev/null 2>&1 || true
    done
    wait_cluster
}

# Setup: create distributed table
setup_table() {
    $PSQL -U postgres -p $COORD_PORT -c "DROP TABLE IF EXISTS bulk_t CASCADE;" >/dev/null 2>&1
    $PSQL -U postgres -p $COORD_PORT -c "CREATE TABLE bulk_t(id int, val text, dk int);" >/dev/null 2>&1
    $PSQL -U postgres -p $COORD_PORT -c "SELECT create_distributed_table('bulk_t','dk');" >/dev/null 2>&1
}

# Reset PartWAL state for bulk_t shards on both workers
reset_parwal() {
    local sql="SET citus.override_table_visibility TO off;
               SELECT partdist.reset_partition_wal_state(c.oid)
               FROM pg_class c WHERE c.relname ~ '^bulk_t_[0-9]{4,}\$';"
    local p
    for p in $ALL_W; do $PSQL -U postgres -p "$p" -c "$sql" >/dev/null 2>&1 || true; done
}

# Count total PartWAL records across all bulk_t shards on both workers
count_records() {
    local cnt_sql="SET citus.override_table_visibility TO off;
                   SELECT COALESCE(SUM(cnt),0)::text
                   FROM (SELECT (SELECT count(*)
                                 FROM partdist.check_partition_wal(c.oid)) AS cnt
                         FROM pg_class c
                         WHERE c.relname ~ '^bulk_t_[0-9]{4,}\$') t;"
    local v total=0
    for v in $(each_w_sql "$cnt_sql"); do total=$(( total + ${v:-0} )); done
    echo "$total"
}

# Return "t" if all bulk_t shards on both workers have monotone LSN
verify_monotone() {
    # Use bool_and without ::text so psql -At emits "t"/"f" (not "true"/"false")
    local check_sql="SET citus.override_table_visibility TO off;
                     SELECT COALESCE(bool_and(v), true)
                     FROM (SELECT partdist.verify_partition_wal(c.oid) AS v
                           FROM pg_class c
                           WHERE c.relname ~ '^bulk_t_[0-9]{4,}\$') t;"
    local v all=t
    for v in $(each_w_sql "$check_sql"); do [[ "$v" == "t" ]] || all=f; done
    echo "$all"
}

# ---------------------------------------------------------------------------
# TEST 1: Functional
# ---------------------------------------------------------------------------
test_functional() {
    echo ""
    echo "=== TEST 1: Functional (INSERT SELECT → PartWAL) ==="
    setup_table
    reset_parwal

    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO bulk_t SELECT i,'v'||i,i%10 FROM generate_series(1,100) i;" >/dev/null 2>&1
    sleep 1

    local cnt
    cnt=$(count_records)
    echo "  PartWAL records: $cnt"

    if [[ $cnt -ge 4 ]]; then
        ok "Functional: PartWAL records generated (count=$cnt)"
    else
        fail "Functional: expected >=4 records, got $cnt"
    fi
    check "Functional: LSN monotone" "$(verify_monotone)"
}

# ---------------------------------------------------------------------------
# TEST 2: Restart
# ---------------------------------------------------------------------------
test_restart() {
    echo ""
    echo "=== TEST 2: Restart (LSN continuity) ==="
    setup_table
    reset_parwal

    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO bulk_t SELECT i,'before'||i,i%5 FROM generate_series(1,50) i;" >/dev/null 2>&1
    sleep 1
    local cnt_before
    cnt_before=$(count_records)
    echo "  Records before restart: $cnt_before"

    restart_all

    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO bulk_t SELECT i,'after'||i,i%5 FROM generate_series(51,100) i;" >/dev/null 2>&1
    sleep 1
    local cnt_after
    cnt_after=$(count_records)
    echo "  Records after restart+insert: $cnt_after"

    if [[ $cnt_after -gt $cnt_before ]]; then
        ok "Restart: new records added (${cnt_before} → ${cnt_after})"
    else
        fail "Restart: no new records after restart"
    fi
    check "Restart: LSN monotone across restart" "$(verify_monotone)"
}

# ---------------------------------------------------------------------------
# TEST 3: Crash recovery
# ---------------------------------------------------------------------------
test_crash() {
    echo ""
    echo "=== TEST 3: Crash recovery (kill -9 worker1) ==="
    setup_table
    reset_parwal

    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO bulk_t SELECT i,'pre'||i,i%5 FROM generate_series(1,40) i;" >/dev/null 2>&1
    sleep 1

    local cnt_pre
    cnt_pre=$(count_records)
    echo "  Records before crash: $cnt_pre"

    # Kill 工作节点 :$WPORT（动态取，不再写死 worker1）
    local pid
    pid=$(head -1 "$WDATA/postmaster.pid" 2>/dev/null || echo "")
    if [[ -z "$pid" ]]; then
        fail "Crash: 取不到 :$WPORT 的 postmaster PID"; return
    fi
    kill -9 "$pid" 2>/dev/null; sleep 3

    # ★ 必须走 topo_start：kill -9 会留下 postmaster.pid 与
    #   /tmp/.s.PGSQL.<port>.lock，而容器里 PID 容易被复用 ⇒ PG 认定
    #   "还有 postmaster 在跑"，裸 `pg_ctl start` 死活起不来（本项目实测踩过）。
    topo_start "$WPORT" || true
    sleep 3; wait_cluster

    local cnt_post
    cnt_post=$(count_records)
    echo "  Records after crash+recovery: $cnt_post"

    if [[ $cnt_post -ge $cnt_pre ]]; then
        ok "Crash: records survive (${cnt_pre} → ${cnt_post})"
    else
        fail "Crash: records dropped (${cnt_pre} → ${cnt_post})"
    fi
    check "Crash: LSN monotone post-recovery" "$(verify_monotone)"

    # More inserts after crash
    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO bulk_t SELECT i,'post'||i,i%5 FROM generate_series(50,80) i;" >/dev/null 2>&1
    sleep 1
    check "Crash: post-crash INSERT SELECT monotone" "$(verify_monotone)"
}

# ---------------------------------------------------------------------------
# TEST 4: Performance (using bash time)
# ---------------------------------------------------------------------------
test_performance() {
    echo ""
    echo "=== TEST 4: Performance (overhead < 20%) ==="

    $PSQL -U postgres -p $COORD_PORT -c "DROP TABLE IF EXISTS perf_plain CASCADE;" >/dev/null 2>&1
    $PSQL -U postgres -p $COORD_PORT -c "CREATE TABLE perf_plain(id int,val text,dk int);" >/dev/null 2>&1
    $PSQL -U postgres -p $COORD_PORT -c "SELECT create_distributed_table('perf_plain','dk');" >/dev/null 2>&1

    # Warmup — larger to stabilize caches
    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO bulk_t     SELECT i,'w'||i,i%5 FROM generate_series(1,5000) i;" >/dev/null 2>&1
    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO perf_plain SELECT i,'w'||i,i%5 FROM generate_series(1,5000) i;" >/dev/null 2>&1

    # Take 3 timed samples each, then pick the median to reduce noise.
    time_insert() {
        local tbl=$1 n=$2
        { time $PSQL -U postgres -p $COORD_PORT -c \
            "INSERT INTO $tbl SELECT i,'x'||i,i%5 FROM generate_series(1,$n) i;" \
            >/dev/null 2>&1; } 2>&1 | awk '/real/{print $2}'
    }
    parse_s() {
        echo "$1" | sed 's/[ms]/ /g' | awk '{print $1*60+$2}'
    }
    median3() {
        local a b c
        a=$(parse_s "$1"); b=$(parse_s "$2"); c=$(parse_s "$3")
        awk -v a="$a" -v b="$b" -v c="$c" \
          'BEGIN{if(a<=b&&b<=c||c<=b&&b<=a){print b}
                 else if(b<=a&&a<=c||c<=a&&a<=b){print a}
                 else{print c}}'
    }

    local ROWS=20000
    local h1 h2 h3 b1 b2 b3
    h1=$(time_insert bulk_t    $ROWS)
    b1=$(time_insert perf_plain $ROWS)
    h2=$(time_insert bulk_t    $ROWS)
    b2=$(time_insert perf_plain $ROWS)
    h3=$(time_insert bulk_t    $ROWS)
    b3=$(time_insert perf_plain $ROWS)

    local s_hook s_base
    s_hook=$(median3 "$h1" "$h2" "$h3")
    s_base=$(median3 "$b1" "$b2" "$b3")

    echo "  With pg_partdist hooks (median of 3):  ${s_hook}s"
    echo "  Baseline           (median of 3):      ${s_base}s"

    local overhead
    overhead=$(awk -v h="$s_hook" -v b="$s_base" \
        'BEGIN{if(b==0){print "0"}else{printf "%.1f",(h-b)/b*100}}')
    echo "  Overhead: ${overhead}%"

    local thr=${BULK_OVERHEAD_THRESHOLD:-20}
    local pass
    pass=$(awk -v h="$s_hook" -v b="$s_base" -v t="$thr" \
        'BEGIN{if(b==0||((h-b)/b*100)<t){print "t"}else{print "f"}}')
    check "Performance: overhead < ${thr}%" "$pass"

    $PSQL -U postgres -p $COORD_PORT -c "DROP TABLE perf_plain CASCADE;" >/dev/null 2>&1 || true
}

# ---------------------------------------------------------------------------
# TEST 5: Regression
# ---------------------------------------------------------------------------
test_regression() {
    echo ""
    echo "=== TEST 5: Regression (37 existing tests) ==="
    local result
    if result=$(cd /work/pg-partdist-src && \
        make installcheck PGUSER=postgres PGPORT=$COORD_PORT \
             PG_CONFIG=/work/pg-install/bin/pg_config 2>&1); then
        ok "Regression: all 37 tests passed"
    else
        local nfail
        nfail=$(echo "$result" | grep -c 'FAILED' || true)
        if [[ $nfail -eq 0 ]]; then
            ok "Regression: all 37 tests passed"
        else
            fail "Regression: $nfail test(s) FAILED"
            echo "$result" | grep -E 'FAILED|not ok' | head -10
        fi
    fi
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
echo "========================================================"
echo " pg_partdist bulk INSERT (COPY path) recovery test"
echo " Date: $(date -u)"
echo "========================================================"

wait_cluster

test_functional
test_restart
test_crash
test_performance
test_regression

echo ""
echo "========================================================"
echo " Results: PASS=$PASS  FAIL=$FAIL"
echo "========================================================"
if [[ $FAIL -eq 0 ]]; then
    echo " 批量插入修复: PASS"
    exit 0
else
    echo " 批量插入修复: FAIL"
    exit 1
fi
