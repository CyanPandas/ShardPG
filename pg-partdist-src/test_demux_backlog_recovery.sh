#!/bin/bash
# test_demux_backlog_recovery.sh — Demux Worker high-backlog crash recovery
#
# Verifies that when the Demux Worker is killed (crash or clean restart) while
# holding a deep backlog of unprocessed WAL records, it correctly recovers
# all records.
#
# Key design insight:
#   • kill -9 (SIGKILL) on a BGW with BGWORKER_SHMEM_ACCESS → PostgreSQL
#     crash recovery runs the partdist_wal_redo handler, which re-writes
#     backlog records into pg_parwal before the new demux starts.
#   • pg_ctl stop -m fast (clean shutdown) → demux saves progress file →
#     pg_ctl start → new demux must call LoadDemuxProgress() to resume from
#     the saved position.  Without that fix, the new demux starts from
#     GetFlushRecPtr() and loses all unprocessed backlog records.
#
# Scenarios:
#   S1  : kill -9 under deep backlog (5000 records) — crash recovery path
#   S2  : clean restart (pg_ctl stop -m fast + start) under backlog
#          → tests LoadDemuxProgress progress-file fix
#   S3  : three consecutive kill -9 cycles (1500 records each)
#   S4  : ultra-deep kill -9 backlog (10 000 records)
#   S5  : concurrent new writes written immediately after crash recovery;
#          verifies the new demux and the redo handler do not conflict

PSQL=/work/pg-install/bin/psql
PGCTL=/work/pg-install/bin/pg_ctl
DATA=/work/pg-cluster-data
PASS=0; FAIL=0

die()  { echo "FATAL: $*" >&2; exit 1; }
pass() { echo "  PASS: $*"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }

check_eq() {
    local label=$1 actual=$2 expected=$3
    if [ "$actual" = "$expected" ]; then
        pass "$label (got $actual)"
    else
        fail "$label (expected $expected, got $actual)"
    fi
}
check_true() {
    local label=$1 val=$2
    if [ "$val" = "t" ]; then pass "$label"; else fail "$label (got '$val')"; fi
}

psql1()    { $PSQL -p 5433 -d postgres "$@"; }
psql1_at() { psql1 -At -c "$1"; }

count_records() {
    local pid=$1 result tries=0
    while [ $tries -lt 4 ]; do
        result=$(psql1_at "SELECT partdist.count_parwal_records($pid::oid);" 2>/dev/null || true)
        [ -n "$result" ] && echo "$result" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
verify_wal() {
    local pid=$1 result tries=0
    while [ $tries -lt 4 ]; do
        result=$(psql1_at "SELECT partdist.verify_partition_wal($pid::oid);" 2>/dev/null || true)
        [ -n "$result" ] && echo "$result" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
flush_w1()      { psql1 -c 'SELECT partdist.demux_flush();' -o /dev/null; }
reset_state()   { psql1 -c "SELECT partdist.reset_partition_wal_state($1::oid);" -o /dev/null; }

# Return the PID of the pg_partdist demux worker that is a direct child of
# worker1's postmaster.  Greps for "demux" in process args to avoid returning
# a checkpointer or autovacuum child.
get_w1_demux_pid() {
    local w1_pm
    w1_pm=$(head -1 "$DATA/worker1/postmaster.pid" 2>/dev/null) || { echo ""; return; }
    ps --ppid "$w1_pm" -o pid=,args= 2>/dev/null \
        | grep -i 'demux' \
        | awk '{print $1}' \
        | head -1 \
        | tr -d ' '
}

# Wait for crash recovery to finish (server accepting connections) AND for a
# new demux PID to appear.  Used after kill -9.
wait_for_crash_recovery_and_demux() {
    local old_pid=$1 max_wait=${2:-30} elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        if psql1 -c 'SELECT 1' > /dev/null 2>&1; then
            local new_pid
            new_pid=$(get_w1_demux_pid 2>/dev/null) || new_pid=""
            if [ -n "$new_pid" ] && [ "$new_pid" != "$old_pid" ]; then
                echo "  Recovery done + Demux restarted: PID $new_pid (waited ${elapsed}s)"
                return 0
            fi
        fi
        sleep 1; elapsed=$((elapsed+1))
    done
    echo "  ERROR: server/demux did not recover within ${max_wait}s"
    return 1
}

# Wait for a new demux PID after a graceful (SIGTERM) exit — no crash recovery,
# so connections remain available throughout.
wait_for_graceful_restart() {
    local old_pid=$1 max_wait=${2:-15} elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        local new_pid
        new_pid=$(get_w1_demux_pid 2>/dev/null) || new_pid=""
        if [ -n "$new_pid" ] && [ "$new_pid" != "$old_pid" ]; then
            echo "  Demux gracefully restarted: PID $new_pid (waited ${elapsed}s)"
            return 0
        fi
        sleep 1; elapsed=$((elapsed+1))
    done
    echo "  ERROR: Demux did not restart within ${max_wait}s"
    return 1
}

# Stop worker1, wipe pg_parwal (deletes progress file too), restart cleanly.
restart_w1_clean() {
    # Manual checkpoint first so fast stop doesn't block on a large dirty-page flush
    $PSQL -p 5433 -d postgres -c 'CHECKPOINT' >/dev/null 2>&1 || true
    # fast stop = clean checkpoint → no crash recovery needed on restart
    if ! $PGCTL stop -D "$DATA/worker1" -m fast -w -t 90 2>/dev/null; then
        $PGCTL stop -D "$DATA/worker1" -m immediate -w -t 30 2>/dev/null || true
    fi
    rm -rf "$DATA/worker1/pg_parwal"
    $PGCTL start -D "$DATA/worker1" -l "$DATA/worker1/pg.log" -o '-p 5433' \
        -w -t 120 2>/dev/null || true
    # Wait until accepting connections
    local tries=0
    while [ $tries -lt 120 ]; do
        $PSQL -p 5433 -d postgres -c 'SELECT 1' >/dev/null 2>&1 && break
        sleep 1; tries=$((tries+1))
    done
    # Wait for demux worker to appear as child of new postmaster
    local pm_pid
    pm_pid=$(head -1 "$DATA/worker1/postmaster.pid" 2>/dev/null || echo "")
    tries=0
    while [ $tries -lt 60 ]; do
        local cnt
        cnt=$(ps --ppid "$pm_pid" -o args= 2>/dev/null | grep 'demux' | wc -l)
        [ "$cnt" -ge 1 ] && break
        sleep 0.5; tries=$((tries+1))
    done
}

# Write N PartWAL records via the SQL helper on worker1 (WAL-only path;
# the Demux Worker is solely responsible for routing to pg_parwal).
write_records() {
    local pid=$1 n=$2
    psql1 -o /dev/null -c "
DO \$\$
DECLARE i int;
BEGIN
  FOR i IN 1..$n LOOP
    PERFORM partdist.write_partition_wal_record($pid::oid);
  END LOOP;
END \$\$;"
}

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  pg_partdist — Demux High-Backlog Crash Recovery Test"
echo "════════════════════════════════════════════════════════════════"

# ── Global setup ──────────────────────────────────────────────────────
echo ""
echo "── Setup ──"
$PSQL -p 5432 -d postgres -o /dev/null -c "
    DROP TABLE IF EXISTS backlog_test CASCADE;
    CREATE TABLE backlog_test (id int PRIMARY KEY, val text);
    SELECT create_distributed_table('backlog_test','id',shard_count=>4);"

P_OID=$(psql1_at "SELECT oid FROM pg_class WHERE relname='backlog_test' AND relkind='r' LIMIT 1;")
[ -n "$P_OID" ] || die "Could not find backlog_test OID on worker1"
echo "  Partition OID on worker1: $P_OID"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S1: kill -9 under 5000-record backlog ════════"
echo "  Method: SIGSTOP → write 5000 → SIGKILL"
echo "  Crash recovery (redo handler) writes backlog to pg_parwal."
echo "  Expected: 5400 records (200 baseline + 5000 redo + 200 new)"
# ════════════════════════════════════════════════════════════════════════

restart_w1_clean
reset_state "$P_OID"

echo "  Writing 200 baseline records (demux running)..."
write_records "$P_OID" 200
flush_w1
check_eq "S1-baseline count" "$(count_records "$P_OID")" "200"
check_true "S1-baseline verify" "$(verify_wal "$P_OID")"

OLD_PID=$(get_w1_demux_pid)
[ -n "$OLD_PID" ] || die "S1: could not find demux PID"
echo "  Demux PID: $OLD_PID"

echo "  SIGSTOP demux → writing 5000-record WAL backlog..."
kill -STOP "$OLD_PID"
write_records "$P_OID" 5000
echo "  5000 records in WAL (demux stopped, cannot process)"

echo "  CHECKPOINT before kill to bound crash recovery WAL range..."
$PSQL -p 5433 -d postgres -c 'CHECKPOINT' >/dev/null 2>&1 || true
echo "  SIGKILL demux → triggers PostgreSQL crash recovery..."
kill -9 "$OLD_PID"

if wait_for_crash_recovery_and_demux "$OLD_PID" 90; then
    pass "S1: auto-restarted after kill -9 (with crash recovery)"
else
    fail "S1: did not recover within 90s"
fi

echo "  Writing 200 new records after recovery..."
write_records "$P_OID" 200
flush_w1

FINAL_S1=$(count_records "$P_OID")
echo "  Final count: $FINAL_S1"
check_eq "S1: total (200+5000_redo+200)" "$FINAL_S1" "5400"
check_true "S1: verify_partition_wal" "$(verify_wal "$P_OID")"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S2: clean pg_ctl restart under 5000-record backlog ════════"
echo "  Method: SIGSTOP → write 5000 → SIGCONT + pg_ctl stop -m fast → start"
echo "  No crash recovery (clean shutdown); the new demux must resume from"
echo "  the saved progress file (LoadDemuxProgress fix)."
echo "  Expected WITH fix : 5400 (200 baseline + 5000 backlog + 200 new)"
echo "  Expected WITHOUT fix: ~400 (baseline + new only; backlog LOST)"
# ════════════════════════════════════════════════════════════════════════

restart_w1_clean
reset_state "$P_OID"

echo "  Writing 200 baseline records (demux running)..."
write_records "$P_OID" 200
flush_w1
check_eq "S2-baseline count" "$(count_records "$P_OID")" "200"
check_true "S2-baseline verify" "$(verify_wal "$P_OID")"

OLD_PID=$(get_w1_demux_pid)
[ -n "$OLD_PID" ] || die "S2: could not find demux PID"
echo "  Demux PID: $OLD_PID"

echo "  SIGSTOP demux → writing 5000-record WAL backlog (demux cannot process)..."
kill -STOP "$OLD_PID"
write_records "$P_OID" 5000
echo "  5000 records in WAL (unprocessed)"

# SIGCONT the demux so the postmaster can send SIGTERM to it cleanly, then
# immediately initiate a fast shutdown.  The postmaster signals SIGTERM to the
# demux which handles it in its main loop (saves progress, proc_exit(0)) before
# the server has time to process any of the backlog.  This is a CLEAN stop:
# no crash recovery will run on the subsequent pg_ctl start.
echo "  SIGCONT + pg_ctl stop -m fast → clean shutdown (demux saves progress)..."
kill -CONT "$OLD_PID"
$PGCTL stop -D "$DATA/worker1" -m fast -w 2>&1 | tail -1

echo "  pg_ctl start → fresh start, no crash recovery..."
$PGCTL start -D "$DATA/worker1" -l "$DATA/worker1/pg.log" -o '-p 5433' \
    -w -t 30 2>&1 | tail -1
sleep 5  # allow new demux to start and begin draining the backlog

# Wait up to 30s for the new demux PID to appear
MAX_WAIT=30; elapsed=0; NEW_PID=""
while [ $elapsed -lt $MAX_WAIT ]; do
    NEW_PID=$(get_w1_demux_pid)
    [ -n "$NEW_PID" ] && break
    sleep 1; elapsed=$((elapsed+1))
done

if [ -n "$NEW_PID" ]; then
    pass "S2: new demux started after clean restart: PID $NEW_PID"
else
    fail "S2: no demux found after clean restart"
fi

# CRITICAL: drain the 5000-record backlog BEFORE writing new records.
# The new demux starts from the saved progress file (~P200) and must read all
# 5000 backlog WAL records.  If we write new records while the drain is still
# in progress, AllocPartitionLSN reads a mid-drain on-disk high-water mark and
# assigns partition_lsn values that overlap with the backlog — those new records
# are then silently dropped by the dedup guard once the backlog finishes.
# Draining first ensures AllocPartitionLSN sees the final high-water mark (5200)
# and starts new allocations from 5201.
echo "  Waiting for 5000-record backlog to drain (flush_w1)..."
flush_w1
BACKLOG_COUNT=$(count_records "$P_OID")
echo "  After drain count: $BACKLOG_COUNT (expected 5200)"
check_eq "S2: backlog fully drained" "$BACKLOG_COUNT" "5200"

echo "  Writing 200 new records after backlog drained..."
write_records "$P_OID" 200
flush_w1

FINAL_S2=$(count_records "$P_OID")
echo "  Final count: $FINAL_S2  (expected 5400 with LoadDemuxProgress fix)"
check_eq "S2: total (200+5000_backlog+200 via progress file)" "$FINAL_S2" "5400"
check_true "S2: verify_partition_wal" "$(verify_wal "$P_OID")"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S3: Three consecutive kill -9 cycles (1500 records each) ════════"
echo "  Expected: 100 + 3×(1500 backlog + 100 new) = 4900 records"
# ════════════════════════════════════════════════════════════════════════

restart_w1_clean
reset_state "$P_OID"

echo "  Writing 100 baseline records..."
write_records "$P_OID" 100
flush_w1
check_eq "S3-baseline count" "$(count_records "$P_OID")" "100"

RUNNING=100
for CYCLE in 1 2 3; do
    BACKLOG=1500
    echo ""
    echo "  ── Cycle $CYCLE: ${BACKLOG}-record backlog (kill -9) ──"

    OLD_PID=$(get_w1_demux_pid)
    [ -n "$OLD_PID" ] || { fail "S3-c$CYCLE: no demux PID"; continue; }
    echo "  Demux PID: $OLD_PID"

    $PSQL -p 5433 -d postgres -c 'CHECKPOINT' >/dev/null 2>&1 || true
    kill -STOP "$OLD_PID"
    write_records "$P_OID" $BACKLOG
    kill -9 "$OLD_PID"
    echo "  $BACKLOG records in WAL → killed"

    if wait_for_crash_recovery_and_demux "$OLD_PID" 60; then
        pass "S3-cycle$CYCLE: auto-restarted"
    else
        fail "S3-cycle$CYCLE: did not recover"
        continue
    fi

    write_records "$P_OID" 100
    RUNNING=$((RUNNING + BACKLOG + 100))
done

echo ""
echo "  Flushing after 3 cycles..."
flush_w1
FINAL_S3=$(count_records "$P_OID")
echo "  Final count: $FINAL_S3 (expected $RUNNING)"
check_eq "S3: total after 3 crash cycles" "$FINAL_S3" "$RUNNING"
check_true "S3: verify_partition_wal" "$(verify_wal "$P_OID")"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S4: Ultra-deep kill -9 backlog (10 000 records) ════════"
echo "  Expected: 50 + 10000 + 100 = 10150 records"
# ════════════════════════════════════════════════════════════════════════

restart_w1_clean
reset_state "$P_OID"

echo "  Writing 50 baseline records..."
write_records "$P_OID" 50
flush_w1
check_eq "S4-baseline count" "$(count_records "$P_OID")" "50"

OLD_PID=$(get_w1_demux_pid)
[ -n "$OLD_PID" ] || die "S4: no demux PID"
echo "  Demux PID: $OLD_PID"

echo "  CHECKPOINT before kill to bound crash recovery WAL range..."
$PSQL -p 5433 -d postgres -c 'CHECKPOINT' >/dev/null 2>&1 || true
echo "  SIGSTOP → 10000 records → SIGKILL..."
kill -STOP "$OLD_PID"
write_records "$P_OID" 10000
kill -9 "$OLD_PID"

if wait_for_crash_recovery_and_demux "$OLD_PID" 120; then
    pass "S4: auto-restarted"
else
    fail "S4: did not recover within 120s"
fi

write_records "$P_OID" 100
flush_w1

FINAL_S4=$(count_records "$P_OID")
echo "  Final count: $FINAL_S4 (expected 10150)"
check_eq "S4: total (50+10000+100)" "$FINAL_S4" "10150"
check_true "S4: verify_partition_wal" "$(verify_wal "$P_OID")"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ S5: concurrent new writes immediately after crash recovery ════════"
echo "  Method: SIGSTOP → write 2000 backlog → kill -9 → crash recovery"
echo "  → immediately write 200 new records alongside new demux"
echo "  Verifies: redo handler (101..2100) and new demux (2101..2300) do not"
echo "  conflict; partition_lsn remains monotone; total = 100+2000+200 = 2300"
# ════════════════════════════════════════════════════════════════════════

restart_w1_clean
reset_state "$P_OID"

echo "  Writing 100 baseline records..."
write_records "$P_OID" 100
flush_w1
check_eq "S5-baseline count" "$(count_records "$P_OID")" "100"

LSN_BEFORE=$(psql1_at "SELECT (partdist.demux_progress()).last_processed_lsn;" 2>/dev/null || echo "0/0")
echo "  last_processed_lsn before kill: $LSN_BEFORE"

OLD_PID=$(get_w1_demux_pid)
[ -n "$OLD_PID" ] || die "S5: no demux PID"
echo "  Demux PID: $OLD_PID"

echo "  CHECKPOINT before kill to bound crash recovery WAL range..."
$PSQL -p 5433 -d postgres -c 'CHECKPOINT' >/dev/null 2>&1 || true
echo "  SIGSTOP → 2000 backlog → kill -9..."
kill -STOP "$OLD_PID"
write_records "$P_OID" 2000
kill -9 "$OLD_PID"

if wait_for_crash_recovery_and_demux "$OLD_PID" 90; then
    pass "S5: crash recovery complete + new demux started"
else
    fail "S5: server/demux did not recover within 90s"
fi

# Write new records concurrently while the new demux processes any remaining work.
# By the time wait_for_crash_recovery_and_demux returns, the redo handler has
# already written records 101..2100 into pg_parwal (crash recovery ran before
# connections were accepted).  New records here get LSNs 2101..2300.
echo "  Writing 200 new records immediately after crash recovery..."
write_records "$P_OID" 200
flush_w1

LSN_AFTER=$(psql1_at "SELECT (partdist.demux_progress()).last_processed_lsn;" 2>/dev/null || echo "0/0")
echo "  last_processed_lsn after recovery: $LSN_AFTER"

if [ "$LSN_BEFORE" != "$LSN_AFTER" ] && [ "$LSN_AFTER" != "0/0" ]; then
    pass "S5: last_processed_lsn advanced from $LSN_BEFORE to $LSN_AFTER"
else
    fail "S5: last_processed_lsn did not advance"
fi

FINAL_S5=$(count_records "$P_OID")
echo "  Final count: $FINAL_S5 (expected 2300)"
check_eq "S5: total (100+2000_redo+200)" "$FINAL_S5" "2300"
check_true "S5: verify_partition_wal (no conflicts, monotone LSN)" "$(verify_wal "$P_OID")"

# ════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SUMMARY ════════"
echo "  Tests passed: $PASS"
echo "  Tests failed: $FAIL"
echo ""
if [ "$FAIL" -eq 0 ]; then
    echo "Demux 高积压崩溃恢复测试: PASS"
    exit 0
else
    echo "Demux 高积压崩溃恢复测试: FAIL"
    exit 1
fi
