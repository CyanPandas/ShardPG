#!/usr/bin/env bash
# test_enospc_recovery.sh
#
# ENOSPC fault-tolerance test for pg_partdist parwal-2.0 (synchronous write path).
#
# In parwal-2.0, PartWAL records are written synchronously by each backend at
# PRE_COMMIT time — there is no long-running demux background worker.
# The demux worker is one-shot: it performs crash recovery on startup and exits.
#
# This test verifies:
#   - ENOSPC in a pg_parwal write does NOT abort the user INSERT
#   - PostgreSQL does not crash when pg_parwal writes fail
#   - Cross-worker isolation: worker2 is unaffected by worker1's ENOSPC
#   - After ENOSPC is cleared, the next INSERT successfully writes PartWAL
#   - PartWAL LSN monotonicity is preserved throughout
#
# Simulates disk-full via LD_PRELOAD write() interceptor (enospc_inject.c).
# The interceptor only intercepts writes to pg_parwal files, not WAL or heap.
#
# Uses LOCAL tables created directly on each worker to avoid Citus 13 shard
# routing which writes to ghost relfilenodes not trackable by pg_partdist.
#
# Usage: run inside the container or via:
#   docker exec pg-citus-cluster-container bash /work/pg-partdist-src/tests/test_enospc_recovery.sh

set -Eeuo pipefail
# ★ -E（errtrace）：trap ERR 默认不被函数继承，而 set -e 照样会因函数内部的
#   失败终止脚本 —— 没有 -E 时陷阱一声不响，比没装还误导。
trap 'echo "★ ERR: line $LINENO 命令失败 -> $BASH_COMMAND" >&2' ERR
export PATH=/work/pg-install/bin:$PATH

# ★★ T7.13（P7-E2）：拓扑无关化（2026-09-11）。
#   本套件建的是**各 worker 上的本地表**（非分布表），不依赖 Citus 分片落点 ——
#   它只需要"两个不同的 worker"。所以写死 worker1/worker2 的唯一后果是：
#   一旦这两个节点被别的套件停掉/占用，就会莫名其妙地红。按 pg_dist_node 动态取。
source "$(cd "$(dirname "$0")" && pwd)/lib_topology.sh"
topo_init || { echo "FATAL: 拓扑初始化失败" >&2; exit 1; }
WORKER1_PORT=$(set -- $(topo_worker_ports); echo "$1")
WORKER2_PORT=$(set -- $(topo_worker_ports); echo "$2")
WORKER1_DATA=$(topo_datadir "$WORKER1_PORT")
[[ -n "$WORKER1_PORT" && -n "$WORKER2_PORT" && -d "$WORKER1_DATA" ]] \
    || { echo "FATAL: 取不到两个可用 worker（W1=$WORKER1_PORT W2=$WORKER2_PORT）" >&2; exit 1; }
echo "本轮节点：注入 ENOSPC 的 :$WORKER1_PORT（$WORKER1_DATA）；对照 :$WORKER2_PORT"

# ★★ 注入库的目标路径必须与**本轮选中的节点**一致。
#   enospc_inject.c 原先把 worker1 写死在 TARGET_PREFIX 里；脚本动态选节点之后
#   若不同步，就会"脚本在 :$WORKER1_PORT 注入、库却只拦 worker1 的写" ——
#   注入不生效而用例照样跑完，是最难查的那种假绿。所以每轮按实际节点重编。
INJECT_SRC=/work/pg-partdist-src/enospc_inject.c
if [ -f "$INJECT_SRC" ]; then
    gcc -shared -fPIC -DTARGET_PREFIX="\"$WORKER1_DATA/pg_parwal/\"" \
        -o /tmp/libenospc_inject.so "$INJECT_SRC" -ldl 2>/dev/null \
        && echo "  注入库已按 $WORKER1_DATA/pg_parwal/ 重新编译" \
        || echo "  ⚠ 注入库编译失败，将沿用已有的 /tmp/libenospc_inject.so"
fi

# pg_ctl must run as the postgres user when invoked as root
pgctl() {
    if [ "$(id -u)" = "0" ]; then
        su -s /bin/bash postgres -c "PATH=/work/pg-install/bin:\$PATH $*"
    else
        "$@"
    fi
}

COORD_PORT=5432
INJECT_TRIGGER=/tmp/enospc_inject_active
INJECT_LIB=/tmp/libenospc_inject.so

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
assert_le() {
    local got="$1" expected="$2" label="$3"
    if [ "$got" -le "$expected" ] 2>/dev/null; then pass "$label"; else fail "$label (got=$got not <= $expected)"; fi
}

w1sql() { psql -U postgres -p $WORKER1_PORT -t -c "$1" | tr -d ' \n'; }
w2sql() { psql -U postgres -p $WORKER2_PORT -t -c "$1" | tr -d ' \n'; }

# ===================================================================
echo ""
echo "=== Pre-flight checks ==="
# ===================================================================

[ -f "$INJECT_LIB" ] || { echo "ERROR: $INJECT_LIB not found — run build step first"; exit 1; }
# Clear any stale ENOSPC trigger from a previous failed run
rm -f "$INJECT_TRIGGER"

df -h "$WORKER1_DATA"

# ---------------------------------------------------------------
# Setup: create LOCAL tables directly on each worker.
#
# Using local tables (not Citus distributed tables) because Citus 13
# creates internal ghost shard tables whose relfilenodes don't appear
# in pg_class and cannot be registered with init_partition_wal(oid).
# A local table has a stable OID/relfilenode that pg_partdist can track.
# ---------------------------------------------------------------
echo "Setting up local enospc_test tables on each worker..."

# Drop from coordinator first to clear Citus distributed table metadata
# (Citus's drop trigger blocks DROP TABLE on workers for distributed tables).
psql -U postgres -p $COORD_PORT -q -c \
    "DROP TABLE IF EXISTS enospc_test CASCADE;" 2>/dev/null || true
sleep 1

# Create local (non-distributed) tables directly on each worker.
# After the coordinator DROP, workers no longer have Citus shard metadata
# for enospc_test, so a local CREATE TABLE succeeds.
psql -U postgres -p $WORKER1_PORT -q -c "
    DROP TABLE IF EXISTS enospc_test CASCADE;
    CREATE TABLE enospc_test (id BIGSERIAL PRIMARY KEY, payload TEXT);
"

psql -U postgres -p $WORKER2_PORT -q -c "
    DROP TABLE IF EXISTS enospc_test CASCADE;
    CREATE TABLE enospc_test (id BIGSERIAL PRIMARY KEY, payload TEXT);
"

W1_OID=$(psql -U postgres -p $WORKER1_PORT -t -c \
    "SELECT oid FROM pg_class WHERE relname='enospc_test' AND relkind='r';" \
    | tr -d ' \n')
W2_OID=$(psql -U postgres -p $WORKER2_PORT -t -c \
    "SELECT oid FROM pg_class WHERE relname='enospc_test' AND relkind='r';" \
    | tr -d ' \n')

[ -n "$W1_OID" ] || { echo "ERROR: could not get OID for enospc_test on worker1"; exit 1; }
[ -n "$W2_OID" ] || { echo "ERROR: could not get OID for enospc_test on worker2"; exit 1; }

echo "worker1 enospc_test: OID=$W1_OID"
echo "worker2 enospc_test: OID=$W2_OID"

# Register with pg_partdist before the restart so directories are created
psql -U postgres -p $WORKER1_PORT -q -c \
    "SELECT partdist.init_partition_wal($W1_OID::oid);" 2>/dev/null || true
psql -U postgres -p $WORKER2_PORT -q -c \
    "SELECT partdist.init_partition_wal($W2_OID::oid);" 2>/dev/null || true

echo "Local tables created and registered"

# ===================================================================
echo ""
echo "=== Phase 1: Restart worker1 with LD_PRELOAD injector ==="
# ===================================================================

pgctl pg_ctl -D "$WORKER1_DATA" -m fast stop 2>&1 | tail -1 || true
sleep 1

if [ "$(id -u)" = "0" ]; then
    su -s /bin/bash postgres -c \
        "LD_PRELOAD=$INJECT_LIB PATH=/work/pg-install/bin:\$PATH pg_ctl -D $WORKER1_DATA start -l $WORKER1_DATA/pg.log -w -t 30" 2>&1 | tail -2
else
    LD_PRELOAD=$INJECT_LIB pg_ctl -D "$WORKER1_DATA" start \
        -l "$WORKER1_DATA/pg.log" -w -t 30 2>&1 | tail -2
fi

psql -U postgres -p $WORKER1_PORT -c "SELECT 1;" > /dev/null
echo "worker1 started with LD_PRELOAD"

# Re-register after restart: shmem is cleared on restart so
# init_partition_wal must be called again before any DML.
w1sql "SELECT partdist.init_partition_wal($W1_OID::oid);" > /dev/null
w2sql "SELECT partdist.init_partition_wal($W2_OID::oid);" > /dev/null

# Reset parwal state for a clean baseline (only deletes files, not shmem)
w1sql "SELECT partdist.reset_partition_wal_state($W1_OID::oid);" > /dev/null
w2sql "SELECT partdist.reset_partition_wal_state($W2_OID::oid);" > /dev/null
sleep 1

# ===================================================================
echo ""
echo "=== Phase 2: Baseline inserts ==="
# ===================================================================

psql -U postgres -p $WORKER1_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'baseline_'||generate_series(1,50);" > /dev/null
psql -U postgres -p $WORKER2_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'baseline_'||generate_series(1,50);" > /dev/null

N1_W1=$(w1sql "SELECT partdist.count_parwal_records($W1_OID::oid);")
N1_W2=$(w2sql "SELECT partdist.count_parwal_records($W2_OID::oid);")
echo "Baseline: worker1=$N1_W1 records, worker2=$N1_W2 records"

assert_gt "$N1_W1" "0" "Worker1 has baseline parwal records"
assert_gt "$N1_W2" "0" "Worker2 has baseline parwal records"

# ===================================================================
echo ""
echo "=== Phase 3: Activate ENOSPC injection ==="
# ===================================================================

> "$WORKER1_DATA/pg.log"   # truncate log for cleaner grep
touch "$INJECT_TRIGGER"
echo "ENOSPC injection activated at $(date)"

# Insert rows on worker1 — backend will attempt pg_parwal write → ENOSPC
# CommitPartWALSync raises ERROR at PRE_COMMIT when parwal write fails,
# so the INSERT is rolled back.
INSERT_RC=0
psql -U postgres -p $WORKER1_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'stall_'||generate_series(1,30);" > /dev/null \
    || INSERT_RC=$?

# ===================================================================
echo ""
echo "=== Phase 4: Verify graceful degradation ==="
# ===================================================================

# 4a: PostgreSQL backend still accepts connections (not crashed)
CONN_OK=$(psql -U postgres -p $WORKER1_PORT -t -c "SELECT 1;" 2>/dev/null | tr -d ' \n' || echo "0")
if [ "$CONN_OK" = "1" ]; then
    pass "PostgreSQL worker1 still accepts connections after ENOSPC"
else
    fail "PostgreSQL worker1 is NOT accepting connections after ENOSPC"
fi

# 4b: INSERT must fail because CommitPartWALSync throws ERROR at PRE_COMMIT
# (disk write to pg_parwal fails with ENOSPC before commit WAL record is written)
if [ "$INSERT_RC" -ne 0 ]; then
    pass "INSERT aborted due to parwal ENOSPC at PRE_COMMIT (transaction rolled back)"
else
    fail "INSERT succeeded — expected failure due to ENOSPC at pg_parwal write"
fi

# 4c: ENOSPC error appears in server log (ERROR from CommitPartWALSync)
if grep -qE "(pg_partdist.*disk full|pg_partdist.*No space left|ERROR.*pg_partdist)" \
        "$WORKER1_DATA/pg.log" 2>/dev/null; then
    pass "Worker1 log: parwal write error (ENOSPC) found"
else
    if grep -qE "(disk full|No space left on device)" \
            "$WORKER1_DATA/pg.log" 2>/dev/null; then
        pass "Worker1 log: disk full error found"
    else
        fail "Worker1 log: no ENOSPC error found"
        grep -E "pg_partdist|ERROR|disk" "$WORKER1_DATA/pg.log" | tail -10 || true
    fi
fi

# 4d: No PANIC (database must not have crashed)
if ! grep -qE '\bPANIC\b' "$WORKER1_DATA/pg.log" 2>/dev/null; then
    pass "No PANIC in worker1 log"
else
    fail "PANIC found in worker1 log!"
    grep -E '\bPANIC\b' "$WORKER1_DATA/pg.log" | head -5
fi

# 4e: Worker1 parwal count did NOT grow during ENOSPC (records were dropped)
N_DURING=$(w1sql "SELECT partdist.count_parwal_records($W1_OID::oid);")
assert_le "$N_DURING" "$N1_W1" "Worker1 parwal count did not grow during ENOSPC (dropped gracefully)"

# 4f: Worker2 is completely unaffected — insert and verify count grows
psql -U postgres -p $WORKER2_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'w2_isolation_'||generate_series(1,20);" > /dev/null
N2_W2=$(w2sql "SELECT partdist.count_parwal_records($W2_OID::oid);")
assert_gt "$N2_W2" "$N1_W2" "Worker2 unaffected: parwal count grew $N1_W2 → $N2_W2"

# ===================================================================
echo ""
echo "=== Phase 5: Release ENOSPC (simulate space freed) ==="
# ===================================================================

rm -f "$INJECT_TRIGGER"
RELEASE_TIME=$(date +%s)
echo "ENOSPC injection removed at $(date)"

# In parwal-2.0, recovery is immediate: the very next INSERT will
# successfully write PartWAL.  We poll a few times for robustness.
RECOVERED=false
RECOVERY_SECS=0
for i in $(seq 1 5); do
    sleep 1
    psql -U postgres -p $WORKER1_PORT -c \
        "INSERT INTO enospc_test (payload) VALUES ('recovery_probe_$i');" > /dev/null 2>&1 || true

    NOW=$(date +%s)
    RECOVERY_SECS=$((NOW - RELEASE_TIME))
    N_MID=$(w1sql "SELECT partdist.count_parwal_records($W1_OID::oid);" 2>/dev/null || echo 0)
    if [ "$N_MID" -gt "$N_DURING" ] 2>/dev/null; then
        RECOVERED=true
        echo "Recovery detected at ${RECOVERY_SECS}s: parwal count $N_DURING → $N_MID"
        break
    fi
done

if [ "$RECOVERED" = "true" ]; then
    pass "Worker1 parwal auto-recovered after space freed"
    if [ "$RECOVERY_SECS" -le 10 ]; then
        pass "Recovery within 10 seconds (${RECOVERY_SECS}s)"
    else
        fail "Recovery took ${RECOVERY_SECS}s (exceeds 10s limit)"
    fi
else
    fail "Worker1 parwal did NOT recover within 5 probes after space freed"
fi

# ===================================================================
echo ""
echo "=== Phase 6: Data integrity ==="
# ===================================================================

psql -U postgres -p $WORKER1_PORT -c \
    "INSERT INTO enospc_test (payload) SELECT 'post_recover_'||generate_series(1,20);" > /dev/null
sleep 1

N_FINAL=$(w1sql "SELECT partdist.count_parwal_records($W1_OID::oid);")
echo "Final worker1 parwal record count: $N_FINAL (during-ENOSPC was $N_DURING)"
assert_gt "$N_FINAL" "$N_DURING" "Post-recovery parwal count exceeds during-ENOSPC count"

VALID=$(w1sql "SELECT partdist.verify_partition_wal($W1_OID::oid);")
assert_eq "$VALID" "t" "Worker1 partition WAL LSN monotonicity"

# ===================================================================
echo ""
echo "=== Phase 7: Cleanup — restart worker1 without LD_PRELOAD ==="
# ===================================================================

pgctl pg_ctl -D "$WORKER1_DATA" -m fast stop 2>&1 | tail -1 || true
sleep 1
pgctl pg_ctl -D "$WORKER1_DATA" start -l "$WORKER1_DATA/pg.log" -w -t 30 2>&1 | tail -2 || true
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
