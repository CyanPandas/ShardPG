#!/usr/bin/env python3
"""
Diagnostic: measure the gap between pg_current_wal_flush_lsn() and
last_processed_lsn from demux_progress() over time, under pgbench load.

Also measures gap_at_t1: the gap *at the moment* we start the latency timer
after an UPDATE commit.  This isolates whether:
  (A) the measurement captures a large gap from commit-time WAL distance, or
  (B) the demux is genuinely falling behind the frontier.
"""
import psycopg2
import time
import statistics

def lsn_to_int(s):
    hi, lo = s.split('/')
    return (int(hi, 16) << 32) | int(lo, 16)

W1_PORT = 5433
W1_DSN  = f"host=127.0.0.1 port={W1_PORT} user=postgres dbname=postgres"

# ----- probe connections -----
c_obs = psycopg2.connect(W1_DSN)   # observer: reads flush_lsn & demux LSN
c_obs.autocommit = True

c_dml = psycopg2.connect(W1_DSN)   # DML connection (autocommit off)
c_dml.autocommit = False

# find a shard on worker1 that has rows
with c_obs.cursor() as cur:
    cur.execute("""
        SELECT schemaname||'.'||tablename
        FROM pg_tables
        WHERE tablename LIKE 'perf_latency_dist%'
        ORDER BY tablename
        LIMIT 1
    """)
    row = cur.fetchone()
    if row:
        shard = row[0]
        print(f"[diag] Using shard: {shard}")
    else:
        print("[diag] No perf_latency_dist shard found; create table and seed first")
        exit(1)

# get probe IDs
with c_obs.cursor() as cur:
    cur.execute(f"SELECT id FROM {shard} LIMIT 50")
    probe_ids = [r[0] for r in cur.fetchall()]

if not probe_ids:
    print("[diag] Shard has no rows")
    exit(1)

probe_idx = 0

# --------------------------------------------------------
# Phase 1: observe gap / rates for 10 seconds (baseline)
# --------------------------------------------------------
print("\n=== Phase 1: gap vs flush_lsn over 10 seconds (no DML) ===")
print(f"{'t':>6}  {'flush_lsn':>16}  {'demux_lsn':>16}  {'gap_bytes':>10}  {'wal_rate KB/s':>14}  {'dmx_rate KB/s':>14}")

prev_flush = prev_demux = None
prev_t = None
t0 = time.perf_counter()

for i in range(20):
    t = time.perf_counter() - t0
    with c_obs.cursor() as cur:
        cur.execute("SELECT pg_current_wal_flush_lsn()::text, last_processed_lsn::text FROM partdist.demux_progress()")
        row = cur.fetchone()
    flush = lsn_to_int(row[0])
    demux = lsn_to_int(row[1])
    gap   = flush - demux

    wal_rate_str  = ""
    dmx_rate_str  = ""
    if prev_flush is not None:
        dt = t - prev_t
        wal_rate_str  = f"{(flush - prev_flush) / dt / 1024:14.1f}"
        dmx_rate_str  = f"{(demux - prev_demux) / dt / 1024:14.1f}"

    print(f"{t:6.1f}  {row[0]:>16}  {row[1]:>16}  {gap:10d}  {wal_rate_str}  {dmx_rate_str}")
    prev_flush, prev_demux, prev_t = flush, demux, t
    time.sleep(0.5)

# --------------------------------------------------------
# Phase 2: measure gap_at_t1 for each UPDATE commit
# --------------------------------------------------------
print("\n=== Phase 2: gap_at_t1 (30 samples) ===")
print(f"{'#':>4}  {'commit_ms':>10}  {'gap_bytes':>10}  {'gap_ms':>8}  {'e2e_ms':>8}  {'polls':>6}")

TIMEOUT_S   = 5.0
POLL_S      = 0.001
DEMUX_RATE  = 228.0   # KB/s assumption for gap→time estimate

gaps_bytes = []
e2e_samples = []

for s in range(30):
    pid = probe_ids[probe_idx % len(probe_ids)]
    probe_idx += 1

    # commit UPDATE and time it
    t_commit_start = time.perf_counter()
    with c_dml.cursor() as cur:
        cur.execute(f"UPDATE {shard} SET val=val+1 WHERE id=%s", (pid,))
    c_dml.commit()
    commit_ms = (time.perf_counter() - t_commit_start) * 1000

    # t1 = now; query target_lsn on same connection
    t1 = time.perf_counter()
    with c_dml.cursor() as cur:
        cur.execute("SELECT pg_current_wal_flush_lsn()::text")
        target_lsn = lsn_to_int(cur.fetchone()[0])
    c_dml.commit()

    # observe gap at t1
    with c_obs.cursor() as cur:
        cur.execute("SELECT last_processed_lsn::text FROM partdist.demux_progress()")
        demux_at_t1 = lsn_to_int(cur.fetchone()[0])

    gap_at_t1 = target_lsn - demux_at_t1
    gap_ms_est = gap_at_t1 / (DEMUX_RATE * 1024) * 1000

    # poll until demux catches up
    polls = 0
    e2e_ms = None
    while True:
        with c_obs.cursor() as cur:
            cur.execute("SELECT last_processed_lsn::text FROM partdist.demux_progress()")
            cur_demux = lsn_to_int(cur.fetchone()[0])
        polls += 1
        if cur_demux >= target_lsn:
            e2e_ms = (time.perf_counter() - t1) * 1000
            break
        if (time.perf_counter() - t1) > TIMEOUT_S:
            e2e_ms = TIMEOUT_S * 1000
            break
        time.sleep(POLL_S)

    gaps_bytes.append(gap_at_t1)
    e2e_samples.append(e2e_ms)

    print(f"{s+1:4d}  {commit_ms:10.2f}  {gap_at_t1:10d}  {gap_ms_est:8.1f}  {e2e_ms:8.2f}  {polls:6d}")

# --------------------------------------------------------
# Summary
# --------------------------------------------------------
print("\n=== Summary ===")
gaps_bytes.sort()
e2e_samples.sort()
n = len(gaps_bytes)
print(f"gap_at_t1  avg={statistics.mean(gaps_bytes):.0f} bytes  "
      f"p50={gaps_bytes[n//2]:.0f}  p95={gaps_bytes[int(n*0.95)]:.0f}  "
      f"p99={gaps_bytes[min(n-1, int(n*0.99))]:.0f}")
print(f"e2e_ms     avg={statistics.mean(e2e_samples):.1f} ms  "
      f"p50={e2e_samples[n//2]:.1f}  p95={e2e_samples[int(n*0.95)]:.1f}  "
      f"p99={e2e_samples[min(n-1, int(n*0.99))]:.1f}")
