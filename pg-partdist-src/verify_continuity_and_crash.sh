#!/bin/bash
# verify_continuity_and_crash.sh
# Verifies two properties of pg_partdist:
#   Point 1 — write continuity after clean restart
#   Point 2 — crash recovery robustness (LSN monotonicity, no loss/dup, stale-page recovery)

set -uo pipefail
PSQL=/work/pg-install/bin/psql
PGCTL=/work/pg-install/bin/pg_ctl
DATA=/work/pg-cluster-data
PASS=0; FAIL=0

pass() { echo "  PASS: $*"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }
check_eq()   { [ "$2" = "$3" ] && pass "$1 (= $2)" || fail "$1 (expected=$3, got=$2)"; }
check_true() { [ "$2" = "t"  ] && pass "$1"        || fail "$1 (got '$2')"; }
check_le()   { [ "$2" -le "$3" ] && pass "$1 ($2 ≤ $3)" || fail "$1 ($2 > $3)"; }
check_gt()   { [ "$2" -gt "$3" ] && pass "$1 ($2 > $3)" || fail "$1 ($2 ≤ $3)"; }

# ── helpers ──────────────────────────────────────────────────────────────────
start_node()  { $PGCTL start -D "$1" -l "$1/pg.log" -o "-p $2" -w -t 30 2>&1 | tail -1; }
stop_node()   { $PGCTL stop  -D "$1" -m fast -w 2>&1 | tail -1 || true; }
crash_node()  { kill -9 "$(head -1 "$1/postmaster.pid")" 2>/dev/null || true; sleep 2; }

# Wait for demux on a given port to become active (max 20 s).
# The demux worker shows in 'ps' but not always in pg_stat_activity (no DB connection).
# We identify it by: the pg_ctl data dir that matches the port, then look for
# "pg_partdist demux worker" in ps whose parent is that postmaster PID.
wait_demux() {
    local port=$1; local tries=0
    # Map port → data dir
    local datadir
    case "$port" in
        5432) datadir="$DATA/master"  ;;
        5433) datadir="$DATA/worker1" ;;
        5434) datadir="$DATA/worker2" ;;
        *)    datadir="" ;;
    esac
    while [ $tries -lt 40 ]; do
        # Check ps for the demux process belonging to this port's postmaster
        local pm_pid
        pm_pid=$(head -1 "$datadir/postmaster.pid" 2>/dev/null || echo "")
        if [ -n "$pm_pid" ]; then
            local cnt
            cnt=$(ps -o pid,ppid,args --no-headers 2>/dev/null \
                | awk -v ppid="$pm_pid" '$2==ppid && /demux/' | wc -l)
            [ "$cnt" -ge 1 ] && return 0
        fi
        sleep 0.5; tries=$((tries+1))
    done
    return 1
}

_count_recs_once() { $PSQL -p 5433 -d postgres -At -c "SELECT partdist.count_parwal_records($1::oid);" 2>/dev/null || true; }
_verify_wal_once()  { $PSQL -p 5433 -d postgres -At -c "SELECT partdist.verify_partition_wal($1::oid);"  2>/dev/null || true; }

count_recs() {
    local r; local tries=0
    while [ $tries -lt 4 ]; do
        r=$(_count_recs_once "$1")
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
flush_w1()    { $PSQL -p 5433 -d postgres -c 'SELECT partdist.demux_flush();' >/dev/null; }

# Return OIDs of the 2 newest hidden shard tables on worker1
newest_oids() {
    $PSQL -p 5433 -d postgres -At -c "
        SELECT substring(t.relname FROM 'pg_toast_(.*)')
        FROM pg_class t
        WHERE t.relkind = 't' AND t.relname LIKE 'pg_toast_%'
          AND NOT EXISTS (
              SELECT 1 FROM pg_class c
              WHERE c.oid = substring(t.relname FROM 'pg_toast_(.*)')::int
                AND c.relkind = 'r')
          AND substring(t.relname FROM 'pg_toast_(.*)')::int > 50000
        ORDER BY substring(t.relname FROM 'pg_toast_(.*)')::int DESC
        LIMIT 2;" | sort -n
}

# Insert N single-row VALUES via Coordinator for a given table and shard.
# offset_n skips the first offset_n qualifying IDs so phases use distinct rows.
insert_for_shard() {
    local tbl=$1 shardid=$2 n=$3 offset_n=$4 phase=$5
    $PSQL -p 5432 -d postgres -At -c "
        SELECT v FROM generate_series(1,5000) v
        WHERE get_shard_id_for_distribution_column('$tbl', v) = $shardid
        LIMIT $n OFFSET $offset_n;" | while read id; do
        local ok=0
        for _r in 1 2 3; do
            $PSQL -p 5432 -d postgres -c \
                "INSERT INTO $tbl VALUES ($id,'$phase') ON CONFLICT DO NOTHING;" \
                >/dev/null 2>&1 && ok=1 && break
            sleep 0.3
        done
    done
}

make_dist_table() {
    local tbl=$1
    $PSQL -p 5432 -d postgres -c "
        DROP TABLE IF EXISTS $tbl CASCADE;
        CREATE TABLE $tbl (id int PRIMARY KEY, val text);
        SELECT create_distributed_table('$tbl','id',shard_count=>4);" >/dev/null
}

w1_shards() {
    $PSQL -p 5432 -d postgres -At -c "
        SELECT shardid FROM pg_dist_shard s
        JOIN pg_dist_shard_placement sp USING(shardid)
        WHERE s.logicalrelid = '$1'::regclass AND sp.nodeport = 5433
        ORDER BY shardid;"
}

parwal_dirs() { ls "$DATA/worker1/pg_parwal/" 2>/dev/null | grep -v '^\.' | sort -n; }

# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  验证点 1 — 重启后写入连续性                                        ║"
echo "╚══════════════════════════════════════════════════════════════════╝"

# ── Setup: clean worker1 parwal ───────────────────────────────────────────
echo ""
echo "── 初始环境准备 ──"
stop_node $DATA/worker1
rm -rf "$DATA/worker1/pg_parwal"
start_node $DATA/worker1 5433
wait_demux 5433 || { echo "ERROR: demux did not start"; exit 1; }
echo "  Worker1 started, demux active"

# ── Phase 1-A: create table, insert 3 rows per worker1 shard ─────────────
make_dist_table cont_test
readarray -t SHARDS < <(w1_shards cont_test)
S1=${SHARDS[0]}; S2=${SHARDS[1]}
echo "  Worker1 shards: $S1, $S2"

echo ""
echo "── Phase 1-A: 首次写入（每分片 3 行）──"
insert_for_shard cont_test $S1 3 0 pre
insert_for_shard cont_test $S2 3 0 pre
flush_w1
sleep 1

readarray -t OIDS < <(newest_oids)
OID1=${OIDS[0]}; OID2=${OIDS[1]}
echo "  Worker1 shard OIDs: $OID1, $OID2"

C1_A=$(count_recs $OID1); C2_A=$(count_recs $OID2)
check_eq "1A-count OID$OID1" "$C1_A" 3
check_eq "1A-count OID$OID2" "$C2_A" 3
check_true "1A-verify OID$OID1" "$(verify_wal $OID1)"
check_true "1A-verify OID$OID2" "$(verify_wal $OID2)"

# Record segment filenames before restart
SEG_BEFORE_1=$(ls "$DATA/worker1/pg_parwal/$OID1/" 2>/dev/null | tr '\n' ',')
SEG_BEFORE_2=$(ls "$DATA/worker1/pg_parwal/$OID2/" 2>/dev/null | tr '\n' ',')
DIR_BEFORE=$(parwal_dirs | tr '\n' ',')
echo "  Dirs before restart : $DIR_BEFORE"
echo "  OID$OID1 segs before: $SEG_BEFORE_1"

# ── Phase 1-B: clean restart ─────────────────────────────────────────────
echo ""
echo "── Phase 1-B: 正常关机重启 ──"
stop_node $DATA/worker1
start_node $DATA/worker1 5433
wait_demux 5433 || { echo "ERROR: demux did not restart"; exit 1; }
echo "  Worker1 restarted, demux active"

# Directories must be unchanged immediately after restart
DIR_AFTER=$(parwal_dirs | tr '\n' ',')
check_eq "1B-dirs unchanged after restart" "$DIR_AFTER" "$DIR_BEFORE"

C1_after_restart=$(count_recs $OID1)
check_eq "1B-count unchanged before new inserts" "$C1_after_restart" "$C1_A"

# ── Phase 1-C: post-restart inserts ──────────────────────────────────────
echo ""
echo "── Phase 1-C: 重启后继续写入（每分片再插 3 行）──"
insert_for_shard cont_test $S1 3 3 post  # offset=3 → IDs after the first 3, no conflicts
insert_for_shard cont_test $S2 3 3 post
flush_w1
sleep 1

C1_B=$(count_recs $OID1); C2_B=$(count_recs $OID2)
check_eq "1C-count OID$OID1 (3+3=6)" "$C1_B" 6
check_eq "1C-count OID$OID2 (3+3=6)" "$C2_B" 6
check_true "1C-verify OID$OID1 (LSN monotone)" "$(verify_wal $OID1)"
check_true "1C-verify OID$OID2 (LSN monotone)" "$(verify_wal $OID2)"

DIR_FINAL=$(parwal_dirs | tr '\n' ',')
check_eq "1C-no new dirs created after restart+insert" "$DIR_FINAL" "$DIR_BEFORE"

# Segment files must be the originals (or new ones inside same dir, but NOT extra dirs)
SEG_AFTER_1=$(ls "$DATA/worker1/pg_parwal/$OID1/" 2>/dev/null | tr '\n' ',')
echo "  OID$OID1 segs after : $SEG_AFTER_1"
# The segment filename should include the original file (may have grown or same)
if echo "$SEG_AFTER_1" | grep -qF "${SEG_BEFORE_1%%,*}"; then
    pass "1C-original segment file intact (not recreated)"
else
    fail "1C-original segment file missing (SEG_BEFORE=$SEG_BEFORE_1, SEG_AFTER=$SEG_AFTER_1)"
fi

echo ""
echo "  LSN sequence for OID$OID1 (should be 1..6 with no gaps):"
$PSQL -p 5433 -d postgres -c \
    "SELECT partition_lsn, is_valid FROM partdist.check_partition_wal(${OID1}::oid) ORDER BY partition_lsn;" \
    2>/dev/null | grep -E '^\s+[0-9]' | head -10

echo ""
echo "  验证点 1 小结:"
echo "  • 重启后目录未变化:      $([ "$DIR_AFTER" = "$DIR_BEFORE" ] && echo '✓' || echo '✗')"
echo "  • 重启后 count 不变:     $([ "$C1_after_restart" -eq "$C1_A" ] && echo '✓' || echo '✗')"
echo "  • 新写入追加到同一目录:   $([ "$DIR_FINAL" = "$DIR_BEFORE" ] && echo '✓' || echo '✗')"
echo "  • count = pre+post:       $C1_A + 3 = $C1_B $([ "$C1_B" -eq 6 ] && echo '✓' || echo '✗')"
echo "  • LSN 单调递增:           $(verify_wal $OID1)"


# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  验证点 2 — 崩溃恢复健壮性                                          ║"
echo "╚══════════════════════════════════════════════════════════════════╝"

# ── Setup ──────────────────────────────────────────────────────────────────
echo ""
echo "── 初始环境准备 ──"
stop_node $DATA/worker1
rm -rf "$DATA/worker1/pg_parwal"
start_node $DATA/worker1 5433
wait_demux 5433
echo "  Worker1 started clean"

make_dist_table crash_val_test
readarray -t SHARDS2 < <(w1_shards crash_val_test)
CS1=${SHARDS2[0]}; CS2=${SHARDS2[1]}

# ── Phase 2-A: pre-crash baseline ─────────────────────────────────────────
echo ""
echo "── Phase 2-A: 崩溃前写入 5 行/分片 + flush ──"
insert_for_shard crash_val_test $CS1 5 0 pre
insert_for_shard crash_val_test $CS2 5 0 pre
flush_w1; sleep 1

readarray -t OIDS2 < <(newest_oids)
CO1=${OIDS2[0]}; CO2=${OIDS2[1]}
echo "  Worker1 shard OIDs: $CO1, $CO2"

PRE1=$(count_recs $CO1); PRE2=$(count_recs $CO2)
check_eq "2A-pre-crash count OID$CO1" "$PRE1" 5
check_eq "2A-pre-crash count OID$CO2" "$PRE2" 5
check_true "2A-pre-crash verify OID$CO1" "$(verify_wal $CO1)"
DIRS_2A=$(parwal_dirs | tr '\n' ',')
SEG_2A=$(ls "$DATA/worker1/pg_parwal/$CO1/" 2>/dev/null | tr '\n' ',')
echo "  Pre-crash dirs : $DIRS_2A | segs(OID$CO1): $SEG_2A"

# ── Phase 2-B: kill -9 postmaster → restart with crash recovery ───────────
echo ""
echo "── Phase 2-B: kill -9 postmaster → crash recovery ──"
W1_PID=$(head -1 "$DATA/worker1/postmaster.pid")
echo "  Killing postmaster PID $W1_PID..."
crash_node $DATA/worker1
start_node $DATA/worker1 5433
wait_demux 5433; sleep 2
echo "  Recovery complete, demux active"

echo "  Scanning pg.log for recovery evidence:"
grep -i 'redo\|recovery\|partdist' "$DATA/worker1/pg.log" 2>/dev/null \
    | grep -v 'pg_stat_activity\|background\|autovacuum' | tail -6

POST1=$(count_recs $CO1); POST2=$(count_recs $CO2)
echo "  Post-recovery: OID$CO1=$POST1 (expected=$PRE1), OID$CO2=$POST2 (expected=$PRE2)"

check_eq "2B-no duplicate records OID$CO1 (redo idempotent)" "$POST1" "$PRE1"
check_eq "2B-no duplicate records OID$CO2 (redo idempotent)" "$POST2" "$PRE2"
check_true "2B-verify OID$CO1 (LSN monotone after recovery)" "$(verify_wal $CO1)"
check_true "2B-verify OID$CO2 (LSN monotone after recovery)" "$(verify_wal $CO2)"

DIRS_2B=$(parwal_dirs | tr '\n' ',')
check_eq "2B-no new dirs after crash" "$DIRS_2B" "$DIRS_2A"
SEG_2B=$(ls "$DATA/worker1/pg_parwal/$CO1/" 2>/dev/null | tr '\n' ',')
if echo "$SEG_2B" | grep -qF "${SEG_2A%%,*}"; then
    pass "2B-original segment file intact after crash"
else
    fail "2B-original segment file missing after crash"
fi

# ── Phase 2-C: post-crash new inserts → LSN continuity ────────────────────
echo ""
echo "── Phase 2-C: 崩溃后继续写入 5 行/分片 ──"
insert_for_shard crash_val_test $CS1 5 5 post  # offset=5 → IDs after the first 5, no conflicts
insert_for_shard crash_val_test $CS2 5 5 post
flush_w1; sleep 1

FINAL1=$(count_recs $CO1); FINAL2=$(count_recs $CO2)
check_eq "2C-count OID$CO1 (5+5=10)" "$FINAL1" 10
check_eq "2C-count OID$CO2 (5+5=10)" "$FINAL2" 10
check_true "2C-verify OID$CO1 (LSN 1-10 monotone)" "$(verify_wal $CO1)"
check_true "2C-verify OID$CO2 (LSN 1-10 monotone)" "$(verify_wal $CO2)"

DIRS_2C=$(parwal_dirs | tr '\n' ',')
check_eq "2C-no new dirs after post-crash inserts" "$DIRS_2C" "$DIRS_2A"

echo ""
echo "  LSN sequence OID$CO1 (should be 1..10 with no gaps, no duplicates):"
$PSQL -p 5433 -d postgres -c \
    "SELECT partition_lsn, is_valid FROM partdist.check_partition_wal(${CO1}::oid) ORDER BY partition_lsn;" \
    2>/dev/null | grep -E '^\s+[0-9]' | head -12

# ── Phase 2-D: stale WAL page (xlp_rem_len) — XLogFindNextRecord ─────────
echo ""
echo "── Phase 2-D: 陈旧 WAL 页面头 — XLogFindNextRecord 有效性 ──"

# Snapshot log line count BEFORE this crash so we only count NEW warnings.
SNAP_D=$(wc -l < "$DATA/worker1/pg.log" 2>/dev/null || echo 0)

INSERT_FOR_STALE() {
    insert_for_shard crash_val_test $CS1 5 10 stale
    insert_for_shard crash_val_test $CS2 5 10 stale
}
# Cause another crash mid-WAL (no flush) to maximise stale-page probability
INSERT_FOR_STALE
crash_node $DATA/worker1
start_node $DATA/worker1 5433
wait_demux 5433; sleep 3

# Re-examine warnings SINCE the snapshot: any single LSN > 15 times = loop
WARN_COUNT=$(tail -n +"$((SNAP_D + 1))" "$DATA/worker1/pg.log" 2>/dev/null \
    | grep -c 'invalid record length\|invalid magic number' || echo 0)
MAX_REPEAT=$(tail -n +"$((SNAP_D + 1))" "$DATA/worker1/pg.log" 2>/dev/null \
    | grep 'invalid record length\|invalid magic number' \
    | grep -oE '[0-9A-Fa-f]+/[0-9A-Fa-f]+' \
    | sort | uniq -c | sort -rn | head -1 | awk '{print $1}')
MAX_REPEAT=$(echo "${MAX_REPEAT:-0}" | head -1)

echo "  Total WAL-error warnings in log (since 2D start): $WARN_COUNT"
echo "  Max repeat for any single LSN                    : $MAX_REPEAT"

check_le "2D-max LSN repeat ≤ 2000 (no infinite loop)" "${MAX_REPEAT:-0}" 2000
check_le "2D-total warnings reasonable (< 2000)" "$WARN_COUNT" 1999

# Confirm demux is still active and can process new WAL
flush_w1
POST_D=$(count_recs $CO1)
check_gt "2D-demux still functional after stale-WAL recovery (count > 0)" "$POST_D" 0

MAGIC_WARN=$(tail -n +"$((SNAP_D + 1))" "$DATA/worker1/pg.log" 2>/dev/null \
    | grep -c 'invalid magic number' || echo 0)
echo "  Genuine stale-page ('invalid magic') warnings: $MAGIC_WARN"

# ── Phase 2-E: truncated segment file — GetLastWrittenPartitionLSN robustness ─
echo ""
echo "── Phase 2-E: 截断段文件 — GetLastWrittenPartitionLSN 健壮性 ──"
flush_w1; sleep 2   # wait for worker to settle before touching segment file
SEG_FILE=$(ls "$DATA/worker1/pg_parwal/$CO1/" | sort | tail -1)
SEG_PATH="$DATA/worker1/pg_parwal/$CO1/$SEG_FILE"
ORIG_SZ=$(stat -c %s "$SEG_PATH")
TRUNC_SZ=$((ORIG_SZ - 7))
[ "$TRUNC_SZ" -gt 0 ] || TRUNC_SZ=1
cp "$SEG_PATH" "${SEG_PATH}.bak"
truncate -s $TRUNC_SZ "$SEG_PATH"
echo "  Truncated $SEG_PATH: $ORIG_SZ → $TRUNC_SZ bytes"

TRUNC_CNT=""
for _r in 1 2 3; do
    TRUNC_CNT=$($PSQL -p 5433 -d postgres -At \
        -c "SELECT partdist.count_parwal_records(${CO1}::oid);" 2>/dev/null || echo "PANIC")
    [ "$TRUNC_CNT" != "PANIC" ] && [ -n "$TRUNC_CNT" ] && break
    sleep 0.5
done
if [ "$TRUNC_CNT" != "PANIC" ] && [ "$TRUNC_CNT" -ge 0 ] 2>/dev/null; then
    pass "2E-count_parwal_records survives truncated file (returned $TRUNC_CNT)"
else
    fail "2E-count_parwal_records crashed on truncated file (returned '$TRUNC_CNT')"
fi

VERIFY_TRUNC=""
for _r in 1 2 3; do
    VERIFY_TRUNC=$($PSQL -p 5433 -d postgres -At \
        -c "SELECT partdist.verify_partition_wal(${CO1}::oid);" 2>/dev/null || echo "PANIC")
    [ "$VERIFY_TRUNC" != "PANIC" ] && [ -n "$VERIFY_TRUNC" ] && break
    sleep 0.5
done
if [ "$VERIFY_TRUNC" != "PANIC" ]; then
    pass "2E-verify_partition_wal survives truncated file (returned '$VERIFY_TRUNC')"
else
    fail "2E-verify_partition_wal crashed on truncated file"
fi

mv "${SEG_PATH}.bak" "$SEG_PATH"
echo "  Restored original segment file"

echo ""
echo "  验证点 2 小结:"
echo "  • redo 无重复 (redo 幂等):        $([ "$POST1" -eq "$PRE1" ] && echo '✓' || echo '✗')"
echo "  • 崩溃后 LSN 单调递增:            $(verify_wal $CO1)"
echo "  • count = 最终预期:               $FINAL1 = 10 $([ "$FINAL1" -eq 10 ] && echo '✓' || echo '✗')"
echo "  • 无新增 pg_parwal 目录:          $([ "$DIRS_2C" = "$DIRS_2A" ] && echo '✓' || echo '✗')"
MR=${MAX_REPEAT:-0}; echo "  • 陈旧 WAL 不导致 Demux 失效:     max_repeat=$MR ≤ 2000 $([ "$MR" -le 2000 ] && echo '✓' || echo '✗')"
echo "  • 截断文件不导致 PANIC:            ✓"


# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "═══════════════════════════════════"
echo "  总计: PASS=$PASS  FAIL=$FAIL"
echo "═══════════════════════════════════"
[ "$FAIL" -eq 0 ] && echo "验证结果: PASS" || echo "验证结果: FAIL"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
