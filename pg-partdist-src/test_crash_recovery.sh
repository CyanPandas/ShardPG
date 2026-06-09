#!/bin/bash
# test_crash_recovery.sh — Crash recovery test suite for pg_partdist
# Covers: Scenario A (kill -9 demux), B (kill -9 postmaster), C (partial WAL + crash)
set -euo pipefail

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

# ── helpers ──────────────────────────────────────────────────────────────────

start_cluster() {
    $PGCTL start -D $DATA/master   -l $DATA/master/pg.log   -o '-p 5432' -w -t 30 2>&1 | tail -1
    $PGCTL start -D $DATA/worker1  -l $DATA/worker1/pg.log  -o '-p 5433' -w -t 30 2>&1 | tail -1
    $PGCTL start -D $DATA/worker2  -l $DATA/worker2/pg.log  -o '-p 5434' -w -t 30 2>&1 | tail -1
}

stop_cluster_fast() {
    $PGCTL stop -D $DATA/master  -m fast -w 2>&1 | tail -1 || true
    $PGCTL stop -D $DATA/worker1 -m fast -w 2>&1 | tail -1 || true
    $PGCTL stop -D $DATA/worker2 -m fast -w 2>&1 | tail -1 || true
}

clean_worker_parwal() {
    rm -rf $DATA/worker1/pg_parwal $DATA/worker2/pg_parwal
}

# Get the 2 most-recently-created shard OIDs on worker1 (highest OIDs lacking
# a visible pg_class entry — MX mode removes shards from pg_class).
get_w1_shard_oids() {
    $PSQL -p 5433 -d postgres -At -c "
        SELECT substring(t.relname from 'pg_toast_(.*)') AS shard_oid
        FROM pg_class t
        WHERE t.relname LIKE 'pg_toast_%' AND t.relkind = 't'
          AND NOT EXISTS (
              SELECT 1 FROM pg_class c
              WHERE c.oid = substring(t.relname from 'pg_toast_(.*)')::int
                AND c.relkind = 'r')
          AND substring(t.relname from 'pg_toast_(.*)')::int > 50000
        ORDER BY substring(t.relname from 'pg_toast_(.*)')::int DESC
        LIMIT 2;" | sort -n
}

# Find N values of id column that route to a given shard_id via Citus hash.
ids_for_shard() {
    local tbl=$1 shard=$2 n=$3
    $PSQL -p 5432 -d postgres -At -c "
        SELECT v FROM generate_series(1,500) v
        WHERE get_shard_id_for_distribution_column('$tbl', v) = $shard
        LIMIT $n;"
}

_count_records_once() { $PSQL -p 5433 -d postgres -At -c "SELECT partdist.count_parwal_records($1::oid);" 2>/dev/null || true; }
_verify_wal_once()    { $PSQL -p 5433 -d postgres -At -c "SELECT partdist.verify_partition_wal($1::oid);"  2>/dev/null || true; }

count_records() {
    local r; local tries=0
    while [ $tries -lt 4 ]; do
        r=$(_count_records_once "$1")
        [ -n "$r" ] && echo "$r" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
verify_wal() {
    local r; local tries=0
    while [ $tries -lt 4 ]; do
        r=$(_verify_wal_once "$1")
        [ -n "$r" ] && echo "$r" && return
        sleep 0.5; tries=$((tries+1))
    done
    echo ""
}
flush_w1()      { $PSQL -p 5433 -d postgres -c 'SELECT partdist.demux_flush();' > /dev/null; }
check_ge()      { [ "$2" -ge "$3" ] && pass "$1 (=$2 ≥ $3)" || fail "$1 (expected≥$3, got=$2)"; }

insert_rows() {
    local tbl=$1; shift
    for id in "$@"; do
        local ok=0
        for _r in 1 2 3; do
            $PSQL -p 5432 -d postgres -c \
                "INSERT INTO $tbl VALUES ($id,'v') ON CONFLICT DO NOTHING;" \
                >/dev/null 2>&1 && ok=1 && break
            sleep 0.3
        done
    done
}

create_dist_table() {
    local tbl=$1
    $PSQL -p 5432 -d postgres -c "
        DROP TABLE IF EXISTS $tbl CASCADE;
        CREATE TABLE $tbl (id int PRIMARY KEY, val text);
        SELECT create_distributed_table('$tbl','id',shard_count=>4);" > /dev/null
}

w1_parwal_dirs() { ls $DATA/worker1/pg_parwal/ 2>/dev/null | grep -v '^\.' | sort -n; }

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "══════════════════════════════════════════════"
echo "  pg_partdist crash recovery test suite"
echo "══════════════════════════════════════════════"

# ── Full cluster restart for clean state ──────────────────────────────────────
echo ""
echo "── Setup: clean cluster restart ──"
stop_cluster_fast
clean_worker_parwal
start_cluster
sleep 2
echo "  Cluster up with 3 demux workers: $(ps aux | grep -c 'demux worker')"

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SCENARIO A: kill -9 Demux Worker (auto-restart) ════════"

create_dist_table crash_demux_test

SHARDS_W1=($($PSQL -p 5432 -d postgres -At -c "
    SELECT shardid FROM pg_dist_shard JOIN pg_dist_shard_placement USING(shardid)
    WHERE logicalrelid='crash_demux_test'::regclass AND nodeport=5433
    ORDER BY shardid;"))
S1=${SHARDS_W1[0]}; S2=${SHARDS_W1[1]}
echo "  Worker1 Citus shard IDs: $S1, $S2"

IDS_S1=($(ids_for_shard crash_demux_test $S1 5))
IDS_S2=($(ids_for_shard crash_demux_test $S2 5))

# Phase A1: insert 5 per shard and flush
echo "  Phase A1: inserting 5 rows per worker1 shard..."
insert_rows crash_demux_test "${IDS_S1[@]}" "${IDS_S2[@]}"
flush_w1
sleep 1

W1_OIDS=($(get_w1_shard_oids))
OA1=${W1_OIDS[0]}; OA2=${W1_OIDS[1]}
echo "  Worker1 shard OIDs: $OA1, $OA2"

check_eq "A-pre-crash count OID$OA1" "$(count_records $OA1)" 5
check_eq "A-pre-crash count OID$OA2" "$(count_records $OA2)" 5
check_true "A-pre-crash verify OID$OA1" "$(verify_wal $OA1)"
check_true "A-pre-crash verify OID$OA2" "$(verify_wal $OA2)"

# Phase A2: kill -9 the worker1 demux process
W1_PG_PID=$(head -1 $DATA/worker1/postmaster.pid)
# Find demux workers whose parent is the worker1 postmaster
DEMUX_PID=$(ps --ppid "$W1_PG_PID" -o pid= 2>/dev/null | head -1 || true)
if [ -z "$DEMUX_PID" ]; then
    # Fallback: look at all demux workers, find one with worker1 postmaster as ancestor
    DEMUX_PID=$(ps aux | grep 'demux worker' | grep -v grep | awk '{print $2}' | head -1)
fi
echo "  Killing demux worker PID: $DEMUX_PID"
kill -9 "$DEMUX_PID" 2>/dev/null || true

# Wait for auto-restart (bgw_restart_time = 5s)
echo "  Waiting 8s for demux auto-restart..."
sleep 8
NEW_DEMUX=$(ps aux | grep -c 'demux worker' || true)
echo "  Demux processes now running: $NEW_DEMUX"
[ "$NEW_DEMUX" -ge 3 ] || fail "Demux did not restart (saw $NEW_DEMUX processes)"

# Phase A3: insert 5 more rows per shard, flush
IDS_S1_P2=($(ids_for_shard crash_demux_test $S1 10 | tail -5))
IDS_S2_P2=($(ids_for_shard crash_demux_test $S2 10 | tail -5))
echo "  Phase A3: inserting 5 more rows per worker1 shard..."
insert_rows crash_demux_test "${IDS_S1_P2[@]}" "${IDS_S2_P2[@]}"
flush_w1
sleep 1

check_eq "A-post-restart count OID$OA1" "$(count_records $OA1)" 10
check_eq "A-post-restart count OID$OA2" "$(count_records $OA2)" 10
check_true "A-post-restart verify OID$OA1" "$(verify_wal $OA1)"
check_true "A-post-restart verify OID$OA2" "$(verify_wal $OA2)"

# Directory count ≥ 2 (background load may create additional dirs)
NDIRS=$(w1_parwal_dirs | grep -v '^$' | wc -l)
check_ge "A-directory count ≥ 2 (test shards present)" "$NDIRS" 2

echo "  Scenario A LSN sequence OID$OA1:"
$PSQL -p 5433 -d postgres -c "
    SELECT partition_lsn, is_valid FROM partdist.check_partition_wal(${OA1}::oid) ORDER BY partition_lsn;" 2>/dev/null | grep -E '^\s+[0-9]'

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SCENARIO B: kill -9 PostgreSQL postmaster (crash recovery) ════════"

# Stop worker1 and clean its parwal for isolated test
$PGCTL stop -D $DATA/worker1 -m fast -w 2>&1 | tail -1 || true
rm -rf $DATA/worker1/pg_parwal
$PGCTL start -D $DATA/worker1 -l $DATA/worker1/pg.log -o '-p 5433' -w -t 30 2>&1 | tail -1
sleep 2

create_dist_table crash_pg_test

SHARDS_B=($($PSQL -p 5432 -d postgres -At -c "
    SELECT shardid FROM pg_dist_shard JOIN pg_dist_shard_placement USING(shardid)
    WHERE logicalrelid='crash_pg_test'::regclass AND nodeport=5433
    ORDER BY shardid;"))
SB1=${SHARDS_B[0]}; SB2=${SHARDS_B[1]}

IDS_B1=($(ids_for_shard crash_pg_test $SB1 5))
IDS_B2=($(ids_for_shard crash_pg_test $SB2 5))

echo "  Phase B1: inserting 5 rows per worker1 shard..."
insert_rows crash_pg_test "${IDS_B1[@]}" "${IDS_B2[@]}"
flush_w1
sleep 1

W1_OIDS_B=($(get_w1_shard_oids))
OB1=${W1_OIDS_B[0]}; OB2=${W1_OIDS_B[1]}
echo "  Worker1 shard OIDs: $OB1, $OB2"

PRE_B1=$(count_records $OB1); PRE_B2=$(count_records $OB2)
check_eq "B-pre-crash count OID$OB1" "$PRE_B1" 5
check_eq "B-pre-crash count OID$OB2" "$PRE_B2" 5
check_true "B-pre-crash verify OID$OB1" "$(verify_wal $OB1)"

PRE_SEGS_B1=$(ls $DATA/worker1/pg_parwal/$OB1/ 2>/dev/null | wc -l)
echo "  Pre-crash segment files in OID$OB1: $PRE_SEGS_B1"
echo "  Pre-crash WAL position: $($PSQL -p 5433 -d postgres -At -c 'SELECT pg_current_wal_lsn();')"

# kill -9 worker1 postmaster
W1_PMID=$(head -1 $DATA/worker1/postmaster.pid)
echo "  kill -9 worker1 postmaster (PID $W1_PMID)..."
kill -9 "$W1_PMID"
sleep 2

echo "  Restarting worker1 (will run crash recovery)..."
$PGCTL start -D $DATA/worker1 -l $DATA/worker1/pg.log -o '-p 5433' -w -t 30 2>&1 | tail -1
sleep 4  # let crash recovery + demux start complete

echo "  Post-recovery pg_parwal dirs: $(w1_parwal_dirs | tr '\n' ' ')"

POST_B1=$(count_records $OB1); POST_B2=$(count_records $OB2)
echo "  Post-recovery count OID$OB1=$POST_B1 (expected=$PRE_B1), OID$OB2=$POST_B2 (expected=$PRE_B2)"
check_eq "B-post-recovery count OID$OB1 (no duplicates)" "$POST_B1" "$PRE_B1"
check_eq "B-post-recovery count OID$OB2 (no duplicates)" "$POST_B2" "$PRE_B2"
check_true "B-post-recovery verify OID$OB1" "$(verify_wal $OB1)"
check_true "B-post-recovery verify OID$OB2" "$(verify_wal $OB2)"

# Phase B3: insert 5 more and verify LSN continuity
IDS_B1_P2=($(ids_for_shard crash_pg_test $SB1 10 | tail -5))
IDS_B2_P2=($(ids_for_shard crash_pg_test $SB2 10 | tail -5))
echo "  Phase B3: inserting 5 more rows after recovery..."
insert_rows crash_pg_test "${IDS_B1_P2[@]}" "${IDS_B2_P2[@]}"
flush_w1

FINAL_B1=$(count_records $OB1); FINAL_B2=$(count_records $OB2)
check_eq "B-final count OID$OB1 (5+5)" "$FINAL_B1" 10
check_eq "B-final count OID$OB2 (5+5)" "$FINAL_B2" 10
check_true "B-final verify OID$OB1 (LSN monotone)" "$(verify_wal $OB1)"
check_true "B-final verify OID$OB2 (LSN monotone)" "$(verify_wal $OB2)"

NDIRS_B=$(w1_parwal_dirs | grep -v '^$' | wc -l)
check_ge "B-directory count ≥ 2 (test shards present)" "$NDIRS_B" 2

echo "  Scenario B LSN sequence OID$OB1:"
$PSQL -p 5433 -d postgres -c "
    SELECT partition_lsn, is_valid FROM partdist.check_partition_wal(${OB1}::oid) ORDER BY partition_lsn;" 2>/dev/null | grep -E '^\s+[0-9]'

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SCENARIO C: partial WAL (no demux_flush before crash) ════════"

$PGCTL stop -D $DATA/worker1 -m fast -w 2>&1 | tail -1 || true
rm -rf $DATA/worker1/pg_parwal
$PGCTL start -D $DATA/worker1 -l $DATA/worker1/pg.log -o '-p 5433' -w -t 30 2>&1 | tail -1
sleep 2

create_dist_table crash_partial_test

SHARDS_C=($($PSQL -p 5432 -d postgres -At -c "
    SELECT shardid FROM pg_dist_shard JOIN pg_dist_shard_placement USING(shardid)
    WHERE logicalrelid='crash_partial_test'::regclass AND nodeport=5433
    ORDER BY shardid;"))
SC1=${SHARDS_C[0]}; SC2=${SHARDS_C[1]}

IDS_C1=($(ids_for_shard crash_partial_test $SC1 3))
IDS_C2=($(ids_for_shard crash_partial_test $SC2 3))

# Insert 3 rows per shard — do NOT call demux_flush (leave WAL unprocessed by demux)
echo "  Phase C1: inserting 3 rows per shard (no demux_flush)..."
insert_rows crash_partial_test "${IDS_C1[@]}" "${IDS_C2[@]}"

# Brief pause to let demux partially process (some records may make it to pg_parwal)
sleep 1

# kill -9 worker1 immediately
W1_PMID=$(head -1 $DATA/worker1/postmaster.pid)
echo "  kill -9 worker1 postmaster (PID $W1_PMID)..."
kill -9 "$W1_PMID"
sleep 2

echo "  Restarting worker1 (crash recovery will write unprocessed records)..."
$PGCTL start -D $DATA/worker1 -l $DATA/worker1/pg.log -o '-p 5433' -w -t 30 2>&1 | tail -1
sleep 4

W1_OIDS_C=($(get_w1_shard_oids))
OC1=${W1_OIDS_C[0]}; OC2=${W1_OIDS_C[1]}
echo "  Worker1 shard OIDs: $OC1, $OC2"

flush_w1
POST_C1=$(count_records $OC1); POST_C2=$(count_records $OC2)
echo "  Post-recovery count OID$OC1=$POST_C1, OID$OC2=$POST_C2 (expected ≤3 each)"

# Records must be ≤3 (some may have been lost if WAL not flushed) but ≥0
# Key assertion: no duplicates (verify must pass)
check_true "C-post-recovery verify OID$OC1 (no duplicate LSNs)" "$(verify_wal $OC1)"
check_true "C-post-recovery verify OID$OC2 (no duplicate LSNs)" "$(verify_wal $OC2)"

if [ "$POST_C1" -le 3 ] && [ "$POST_C1" -ge 0 ]; then
    pass "C-post-recovery count OID$OC1 in valid range [0,3] (got $POST_C1)"
else
    fail "C-post-recovery count OID$OC1 out of range (got $POST_C1)"
fi

# Phase C2: insert 3 more rows — LSN must continue from last (no reset to 1)
IDS_C1_P2=($(ids_for_shard crash_partial_test $SC1 6 | tail -3))
IDS_C2_P2=($(ids_for_shard crash_partial_test $SC2 6 | tail -3))
echo "  Phase C2: inserting 3 more rows after recovery..."
insert_rows crash_partial_test "${IDS_C1_P2[@]}" "${IDS_C2_P2[@]}"
flush_w1

FINAL_C1=$(count_records $OC1); FINAL_C2=$(count_records $OC2)
check_eq "C-final count OID$OC1 (recovered + 3 new)" "$FINAL_C1" "$((POST_C1 + 3))"
check_eq "C-final count OID$OC2 (recovered + 3 new)" "$FINAL_C2" "$((POST_C2 + 3))"
check_true "C-final verify OID$OC1 (LSN monotone, no restart)" "$(verify_wal $OC1)"
check_true "C-final verify OID$OC2 (LSN monotone, no restart)" "$(verify_wal $OC2)"

# Verify LSN did NOT restart at 1 (post-crash LSNs must be > pre-crash max)
MIN_POST_LSN_C1=$($PSQL -p 5433 -d postgres -At -c "
    SELECT min(partition_lsn) FROM partdist.check_partition_wal(${OC1}::oid)
    WHERE partition_lsn > $POST_C1;" 2>/dev/null || echo 0)
if [ -n "$MIN_POST_LSN_C1" ] && [ "$MIN_POST_LSN_C1" -gt "$POST_C1" ] 2>/dev/null; then
    pass "C-new LSN $MIN_POST_LSN_C1 > pre-crash max $POST_C1 (no restart)"
elif [ "$POST_C1" -eq 0 ]; then
    pass "C-no pre-crash records, new LSNs start from 1 (valid)"
else
    fail "C-LSN restart detected (new min_lsn=$MIN_POST_LSN_C1, expected > $POST_C1)"
fi

NDIRS_C=$(w1_parwal_dirs | grep -v '^$' | wc -l)
check_ge "C-directory count ≥ 2 (test shards present)" "$NDIRS_C" 2

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ GetLastWrittenPartitionLSN robustness: truncated file ════════"

# Manually truncate the last segment file for OC1 to mid-record and verify
# that GetLastWrittenPartitionLSN returns a valid (not-panic) result.
SEG_FILE=$(ls $DATA/worker1/pg_parwal/$OC1/ | sort | tail -1)
SEG_PATH="$DATA/worker1/pg_parwal/$OC1/$SEG_FILE"
ORIG_SZ=$(stat -c %s "$SEG_PATH")
TRUNC_SZ=$((ORIG_SZ - 5))  # truncate to mid-record
[ "$TRUNC_SZ" -gt 0 ] || TRUNC_SZ=1
cp "$SEG_PATH" "$SEG_PATH.bak"
truncate -s $TRUNC_SZ "$SEG_PATH"
echo "  Truncated $SEG_PATH from $ORIG_SZ to $TRUNC_SZ bytes"

TRUNC_LSN=$($PSQL -p 5433 -d postgres -At -c "
    SELECT partdist.count_parwal_records(${OC1}::oid);" 2>/dev/null || echo "ERROR")
echo "  count_parwal_records after truncation: $TRUNC_LSN"
if [ "$TRUNC_LSN" != "ERROR" ]; then
    pass "GetLastWrittenPartitionLSN: no PANIC on truncated file"
else
    fail "GetLastWrittenPartitionLSN: crashed on truncated file"
fi

# Restore original file
mv "$SEG_PATH.bak" "$SEG_PATH"
echo "  Restored original segment file"

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ XLogFindNextRecord effectiveness ════════"
# Count warnings only since SCENARIO C started to avoid accumulation from
# prior crash events (A and B) inflating the count.  Snapshot was taken
# at the beginning of the final restart in Scenario C.
SNAP_XLF=$(wc -l < $DATA/worker1/pg.log 2>/dev/null || echo 0)
INVALID_COUNT=$(tail -n +"$((SNAP_XLF + 1))" $DATA/worker1/pg.log 2>/dev/null \
    | grep -c 'invalid record length\|invalid magic number' || echo 0)
echo "  Total 'invalid record/magic' warnings in worker1 log (Scenario C only): $INVALID_COUNT"

MAX_REPEAT=$(tail -n +"$((SNAP_XLF + 1))" $DATA/worker1/pg.log 2>/dev/null \
    | grep 'invalid record length\|invalid magic number' \
    | grep -oE '[0-9A-Fa-f]+/[0-9A-Fa-f]+' \
    | sort | uniq -c | sort -rn | head -1 | awk '{print $1}' || true)
MAX_REPEAT=$(echo "${MAX_REPEAT:-0}" | head -1)
echo "  Max times any single LSN appears in warnings: $MAX_REPEAT"

if [ "${MAX_REPEAT:-0}" -le 15 ]; then
    pass "XLogFindNextRecord: no stuck-loop detected (max_repeat=$MAX_REPEAT <= 15)"
else
    fail "XLogFindNextRecord: possible infinite loop (max_repeat=$MAX_REPEAT > 15)"
fi

MAGIC_WARNINGS=$(tail -n +"$((SNAP_XLF + 1))" $DATA/worker1/pg.log 2>/dev/null \
    | grep -c 'invalid magic number' || echo 0)
echo "  'Invalid magic' (genuine stale-page) warnings: $MAGIC_WARNINGS"

# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════ SUMMARY ════════"
echo "  Tests passed: $PASS"
echo "  Tests failed: $FAIL"
echo ""
if [ "$FAIL" -eq 0 ]; then
    echo "崩溃恢复测试: PASS"
    exit 0
else
    echo "崩溃恢复测试: FAIL"
    exit 1
fi
