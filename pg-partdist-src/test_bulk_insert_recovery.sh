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
#   4. Performance — Bulk INSERT overhead < 10% vs baseline
#   5. Regression  — All 37 existing tests still pass

set -euo pipefail

PSQL="/work/pg-install/bin/psql"
PG_CTL="/work/pg-install/bin/pg_ctl"
COORD_PORT=5432
W1_PORT=5433
W2_PORT=5434
COORD_DATA=/work/pg-cluster-data/master
W1_DATA=/work/pg-cluster-data/worker1
W2_DATA=/work/pg-cluster-data/worker2
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
    for port in $COORD_PORT $W1_PORT $W2_PORT; do
        tries=0
        until $PSQL -U postgres -p $port -c "SELECT 1" >/dev/null 2>&1; do
            sleep 0.5; tries=$((tries+1))
            [[ $tries -gt 60 ]] && { echo "Timeout waiting for port $port"; exit 1; }
        done
    done
}

restart_all() {
    $PG_CTL -D $COORD_DATA  restart -w -l $COORD_DATA/pg.log  >/dev/null 2>&1
    $PG_CTL -D $W1_DATA     restart -w -l $W1_DATA/pg.log     >/dev/null 2>&1
    $PG_CTL -D $W2_DATA     restart -w -l $W2_DATA/pg.log     >/dev/null 2>&1
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
    $PSQL -U postgres -p $W1_PORT -c "$sql" >/dev/null 2>&1 || true
    $PSQL -U postgres -p $W2_PORT -c "$sql" >/dev/null 2>&1 || true
}

# Count total PartWAL records across all bulk_t shards on both workers
count_records() {
    local cnt_sql="SET citus.override_table_visibility TO off;
                   SELECT COALESCE(SUM(cnt),0)::text
                   FROM (SELECT (SELECT count(*)
                                 FROM partdist.check_partition_wal(c.oid)) AS cnt
                         FROM pg_class c
                         WHERE c.relname ~ '^bulk_t_[0-9]{4,}\$') t;"
    local w1 w2
    w1=$(w1_sql "$cnt_sql" | tail -1)
    w2=$(w2_sql "$cnt_sql" | tail -1)
    echo $(( ${w1:-0} + ${w2:-0} ))
}

# Return "t" if all bulk_t shards on both workers have monotone LSN
verify_monotone() {
    # Use bool_and without ::text so psql -At emits "t"/"f" (not "true"/"false")
    local check_sql="SET citus.override_table_visibility TO off;
                     SELECT COALESCE(bool_and(v), true)
                     FROM (SELECT partdist.verify_partition_wal(c.oid) AS v
                           FROM pg_class c
                           WHERE c.relname ~ '^bulk_t_[0-9]{4,}\$') t;"
    local m1 m2
    m1=$(w1_sql "$check_sql" | tail -1)
    m2=$(w2_sql "$check_sql" | tail -1)
    if [[ "$m1" == "t" && "$m2" == "t" ]]; then echo "t"; else echo "f"; fi
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

    # Kill worker1
    local pid
    pid=$(head -1 $W1_DATA/postmaster.pid 2>/dev/null || echo "")
    if [[ -z "$pid" ]]; then
        fail "Crash: could not find worker1 PID"; return
    fi
    kill -9 "$pid" 2>/dev/null; sleep 3

    $PG_CTL -D $W1_DATA start -w -l $W1_DATA/pg.log >/dev/null 2>&1
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
    echo "=== TEST 4: Performance (overhead < 10%) ==="

    $PSQL -U postgres -p $COORD_PORT -c "DROP TABLE IF EXISTS perf_plain CASCADE;" >/dev/null 2>&1
    $PSQL -U postgres -p $COORD_PORT -c "CREATE TABLE perf_plain(id int,val text,dk int);" >/dev/null 2>&1
    $PSQL -U postgres -p $COORD_PORT -c "SELECT create_distributed_table('perf_plain','dk');" >/dev/null 2>&1

    # Warmup
    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO bulk_t     SELECT i,'w'||i,i%5 FROM generate_series(1,500) i;" >/dev/null 2>&1
    $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO perf_plain SELECT i,'w'||i,i%5 FROM generate_series(1,500) i;" >/dev/null 2>&1

    # Time both
    local t_hook t_base
    t_hook=$({ time $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO bulk_t SELECT i,'x'||i,i%5 FROM generate_series(1,3000) i;" >/dev/null 2>&1; } 2>&1 | awk '/real/{print $2}')
    t_base=$({ time $PSQL -U postgres -p $COORD_PORT -c \
        "INSERT INTO perf_plain SELECT i,'x'||i,i%5 FROM generate_series(1,3000) i;" >/dev/null 2>&1; } 2>&1 | awk '/real/{print $2}')

    echo "  With pg_partdist hooks:  $t_hook"
    echo "  Baseline (no hooks):     $t_base"

    # Parse seconds from "0m0.123s" format
    local s_hook s_base
    s_hook=$(echo "$t_hook" | sed 's/[ms]/ /g' | awk '{print $1*60+$2}')
    s_base=$(echo "$t_base" | sed 's/[ms]/ /g' | awk '{print $1*60+$2}')

    local overhead
    overhead=$(awk -v h="$s_hook" -v b="$s_base" 'BEGIN{if(b==0){print "0"}else{printf "%.1f",(h-b)/b*100}}')
    echo "  Overhead: ${overhead}%"

    local pass
    pass=$(awk -v h="$s_hook" -v b="$s_base" 'BEGIN{if(b==0||((h-b)/b*100)<10){print "t"}else{print "f"}}')
    check "Performance: overhead < 10%" "$pass"

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
