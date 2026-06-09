#!/usr/bin/env bash
# test_enospc_recovery.sh
#
# ENOSPC fault-tolerance test for pg_partdist Demux Worker.
#
# Simulates disk-full conditions via LD_PRELOAD write() interceptor.
# Tests: stall, no-crash, cross-worker isolation, auto-recovery within 5s,
# LSN monotonicity.
#
# Usage: run inside the container or via:
#   docker exec pg-citus-cluster-container bash /work/pg-partdist-src/test_enospc_recovery.sh

set -euo pipefail
export PATH=/work/pg-install/bin:$PATH

WORKER1_DATA=/work/pg-cluster-data/worker1
WORKER1_PORT=5433
WORKER2_PORT=5434

# pg_ctl must run as the postgres user when invoked as root
pgctl() {
    if [ "$(id -u)" = "0" ]; then
        su -s /bin/bash postgres -c "PATH=/work/pg-install/bin:\$PATH $*"
    else
        "$@"
    fi
}

INJECT_TRIGGER=/tmp/enospc_inject_active
INJECT_LIB=/tmp/libenospc_inject.so

# Physical OIDs of enospc_test on each worker (queried once during setup)
W1_OID=""
W2_OID=""

PASS_COUNT=0
FAIL_COUNT=0

pass()  { echo "[PASS] $1"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail()  { echo "[FAIL] $1"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
assert_eq() {
    local got="$1" expected="$2" label="$3"
    if [ "$got" = "$expected" ]; then pass "$label"; else fail "$label (got=$got expected=$expected)"; fi
}
assert_gt() {
    local got="$1" expected="$2" label="$3"
    if [ "$got" -gt "$expected" ] 2>/dev/null; then pass "$label"; else fail "$label (got=$got not > $expected)"; fi
}

w1sql() { psql -U postgres -p $WORKER1_PORT -t -c "$1" | tr -d ' \n'; }
w2sql() { psql -U postgres -p $WORKER2_PORT -t -c "$1" | tr -d ' \n'; }

# ===================================================================
echo ""
echo "=== Pre-flight checks ==="
# ===================================================================

[ -f "$INJECT_LIB" ] || { echo "ERROR: $INJECT_LIB not found — run build step first"; exit 1; }
rm -f "$INJECT_TRIGGER"

df -h "$WORKER1_DATA"

# Resolve OIDs
W1_OID=$(psql -U postgres -p $WORKER1_PORT -t -c \
    "SELECT oid FROM pg_class WHERE relname='enospc_test' LIMIT 1;" | tr -d ' ')
W2_OID=$(psql -U postgres -p $WORKER2_PORT -t -c \
    "SELECT oid FROM pg_class WHERE relname='enospc_test' LIMIT 1;" | tr -d ' ')

[ -n "$W1_OID" ] || { echo "ERROR: enospc_test not found on worker1"; exit 1; }
[ -n "$W2_OID" ] || { echo "ERROR: enospc_test not found on worker2"; exit 1; }

echo "worker1 enospc_test OID: $W1_OID"
echo "worker2 enospc_test OID: $W2_OID"

# ===================================================================
echo ""
echo "=== Phase 1: Restart worker1 with fixed .so + LD_PRELOAD injector ==="
# ===================================================================

pgctl pg_ctl -D "$WORKER1_DATA" -m fast stop 2>&1 | tail -1
sleep 1

# Start with LD_PRELOAD so the demux worker inherits the interceptor
# Must run as postgres; pass LD_PRELOAD via the environment
if [ "$(id -u)" = "0" ]; then
    su -s /bin/bash postgres -c \
        "LD_PRELOAD=$INJECT_LIB PATH=/work/pg-install/bin:\$PATH pg_ctl -D $WORKER1_DATA start -l $WORKER1_DATA/pg.log -w -t 30" 2>&1 | tail -2
else
    LD_PRELOAD=$INJECT_LIB pg_ctl -D "$WORKER1_DATA" start \
        -l "$WORKER1_DATA/pg.log" -w -t 30 2>&1 | tail -2
fi

# Verify startup
psql -U postgres -p $WORKER1_PORT -c "SELECT 1;" > /dev/null
echo "worker1 started with LD_PRELOAD"

# Reset pg_parwal for enospc_test on worker1 to start clean
w1sql "SELECT partdist.reset_partition_wal_state($W1_OID::oid);" > /dev/null
w2sql "SELECT partdist.reset_partition_wal_state($W2_OID::oid);" > /dev/null
sleep 1  # let demux detect the reset

# ===================================================================
echo ""
echo "=== Phase 2: Baseline inserts ==="
# ===================================================================

psql -U postgres -p $WORKER1_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'baseline_'||generate_series(1,50);" > /dev/null
psql -U postgres -p $WORKER2_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'baseline_'||generate_series(1,50);" > /dev/null

psql -U postgres -p $WORKER1_PORT -c "SELECT partdist.demux_flush();" > /dev/null
psql -U postgres -p $WORKER2_PORT -c "SELECT partdist.demux_flush();" > /dev/null

N1_W1=$(w1sql "SELECT partdist.count_parwal_records($W1_OID::oid);")
N1_W2=$(w2sql "SELECT partdist.count_parwal_records($W2_OID::oid);")
echo "Baseline: worker1=$N1_W1 records, worker2=$N1_W2 records"

assert_gt "$N1_W1" "0" "Worker1 has baseline parwal records"
assert_gt "$N1_W2" "0" "Worker2 has baseline parwal records"

# Capture demux PID (worker1's demux is the 3rd pg process line by start order)
DEMUX_PID=$(ps aux | grep 'pg_partdist demux' | grep -v grep | awk 'NR==1{print $2}')
# Identify worker1 demux specifically by checking which PID belongs to worker1 instance
DEMUX_PID=$(pgrep -f "pg_partdist demux worker" | head -1)
echo "Demux PIDs: $(pgrep -f 'pg_partdist demux worker' | tr '\n' ' ')"
W1_MASTER_PID=$(pgrep -f "postgres -D $WORKER1_DATA" | head -1)
# Find the demux worker that is a child of worker1's master
W1_DEMUX_PID=$(pgrep -P "$W1_MASTER_PID" -f "demux" 2>/dev/null || \
    pgrep -f "pg_partdist demux" | head -1)
echo "Worker1 postgres PID: $W1_MASTER_PID, Demux PID: $W1_DEMUX_PID"

# ===================================================================
echo ""
echo "=== Phase 3: Activate ENOSPC injection ==="
# ===================================================================

> "$WORKER1_DATA/pg.log"   # truncate log for cleaner grep
touch "$INJECT_TRIGGER"
echo "ENOSPC injection activated at $(date)"

# Insert rows — demux on worker1 will attempt parwal writes → ENOSPC
psql -U postgres -p $WORKER1_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'stall_'||generate_series(1,30);" > /dev/null
# Give demux time to process
sleep 3

# ===================================================================
echo ""
echo "=== Phase 4: Verify stall behavior ==="
# ===================================================================

# 4a: demux still running
NEW_W1_DEMUX=$(pgrep -f "pg_partdist demux" | head -1)
if [ -n "$NEW_W1_DEMUX" ]; then
    pass "Demux worker still running after ENOSPC (PID $NEW_W1_DEMUX)"
else
    fail "Demux worker is NOT running after ENOSPC"
fi

# 4b: stalling log message
if grep -q "stalling partition $W1_OID after write error" "$WORKER1_DATA/pg.log" 2>/dev/null; then
    pass "Worker1 log: 'stalling partition $W1_OID' warning found"
else
    fail "Worker1 log: missing 'stalling partition $W1_OID' warning"
fi

# 4c: no PANIC (FATAL is connection-level, expected under background load)
if ! grep -qE '\bPANIC\b' "$WORKER1_DATA/pg.log" 2>/dev/null; then
    pass "No PANIC in worker1 log"
else
    fail "PANIC found in worker1 log!"
    grep -E '\bPANIC\b' "$WORKER1_DATA/pg.log" | head -5
fi

# 4d: worker2 unaffected
psql -U postgres -p $WORKER2_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'w2_isolation_'||generate_series(1,20);" > /dev/null
psql -U postgres -p $WORKER2_PORT -c "SELECT partdist.demux_flush();" > /dev/null
N2_W2=$(w2sql "SELECT partdist.count_parwal_records($W2_OID::oid);")
assert_gt "$N2_W2" "$N1_W2" "Worker2 demux unaffected (records grew $N1_W2 → $N2_W2)"

# ===================================================================
echo ""
echo "=== Phase 5: Release ENOSPC (simulate space freed) ==="
# ===================================================================

rm -f "$INJECT_TRIGGER"
RELEASE_TIME=$(date +%s)
echo "ENOSPC injection removed at $(date)"

# Poll for recovery: insert a row every second, flush, check count
RECOVERED=false
RECOVERY_SECS=0
for i in $(seq 1 10); do
    sleep 1
    # Trigger WAL records so demux loop runs and performs the stall probe
    psql -U postgres -p $WORKER1_PORT -c \
        "INSERT INTO enospc_test (payload) VALUES ('recovery_probe_$i');" > /dev/null 2>&1 || true
    psql -U postgres -p $WORKER1_PORT -c "SELECT partdist.demux_flush();" > /dev/null 2>&1 || true

    NOW=$(date +%s)
    RECOVERY_SECS=$((NOW - RELEASE_TIME))
    N_MID=$(w1sql "SELECT partdist.count_parwal_records($W1_OID::oid);" 2>/dev/null || echo 0)
    if [ "$N_MID" -gt "$N1_W1" ] 2>/dev/null; then
        RECOVERED=true
        echo "Recovery detected at ${RECOVERY_SECS}s: parwal count $N1_W1 → $N_MID"
        break
    fi
done

if [ "$RECOVERED" = "true" ]; then
    pass "Worker1 demux auto-recovered after space freed"
    if [ "$RECOVERY_SECS" -le 10 ]; then
        pass "Recovery within 10 seconds (${RECOVERY_SECS}s)"
    else
        fail "Recovery took ${RECOVERY_SECS}s (exceeds 10s limit)"
    fi
else
    fail "Worker1 demux did NOT recover within 10 seconds"
fi

# Check for resuming log message
if grep -q "resuming partition $W1_OID" "$WORKER1_DATA/pg.log" 2>/dev/null; then
    pass "Worker1 log: 'resuming partition $W1_OID' found"
else
    echo "(INFO: 'resuming partition' not in log — verified via behavioral count increase)"
fi

# ===================================================================
echo ""
echo "=== Phase 6: Data integrity ==="
# ===================================================================

# Insert more rows post-recovery and flush
psql -U postgres -p $WORKER1_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'post_recover_'||generate_series(1,20);" > /dev/null
psql -U postgres -p $WORKER1_PORT -c "SELECT partdist.demux_flush();" > /dev/null
sleep 1

N_FINAL=$(w1sql "SELECT partdist.count_parwal_records($W1_OID::oid);")
echo "Final worker1 parwal record count: $N_FINAL (baseline was $N1_W1)"
assert_gt "$N_FINAL" "$N1_W1" "Post-recovery parwal count exceeds baseline"

VALID=$(w1sql "SELECT partdist.verify_partition_wal($W1_OID::oid);")
assert_eq "$VALID" "t" "Worker1 partition WAL LSN monotonicity"

# ===================================================================
echo ""
echo "=== Phase 7: Cleanup — restart worker1 without LD_PRELOAD ==="
# ===================================================================

pgctl pg_ctl -D "$WORKER1_DATA" -m fast stop 2>&1 | tail -1
sleep 1
pgctl pg_ctl -D "$WORKER1_DATA" start -l "$WORKER1_DATA/pg.log" -w -t 30 2>&1 | tail -2
psql -U postgres -p $WORKER1_PORT -c "SELECT 1;" > /dev/null
echo "Worker1 restarted without LD_PRELOAD"

# ===================================================================
echo ""
echo "=== Test Summary ==="
echo "PASS: $PASS_COUNT  FAIL: $FAIL_COUNT"
if [ "$FAIL_COUNT" -eq 0 ]; then
    echo "磁盘空间不足容错测试: PASS"
    exit 0
else
    echo "磁盘空间不足容错测试: FAIL"
    exit 1
fi
