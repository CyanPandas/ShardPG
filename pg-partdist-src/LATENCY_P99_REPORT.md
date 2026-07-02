# pg_partdist End-to-End PartWAL Latency Report

**Date:** Thu Jul  2 03:41:16 UTC 2026
**Verdict:** 延迟 p99 测试: **FAIL**

---

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Cluster | Coordinator(5432), Worker1(5433), Worker2(5434) |
| Distributed table | `perf_latency_dist` |
| Shards | 12 (6 per worker) |
| pgbench clients | 32 |
| Load duration | 60 s |
| Transaction mix | 50% INSERT / 30% UPDATE / 20% SELECT |
| Latency samples | 500 (0 errors) |

## Measurement Method

Each latency sample covers the pure demux pipeline on a specific worker:

```
      UPDATE <shard_table> SET val=... WHERE id=...  [direct worker connection]
      commit()            <- WAL flushed (synchronous_commit=on)
t1  = time.perf_counter()  <- clock starts here (XLogInsert done)
      target = pg_current_wal_flush_lsn()  <- Worker flush frontier
      poll demux_progress() every 1ms until last_processed_lsn >= target
t2  = time.perf_counter()  <- demux confirmed past flush frontier
latency = (t2 - t1) * 1000  [ms]  <- queue_wait + record_processing only
```

SQL execution time is excluded (t1 starts after commit).  Using
`pg_current_wal_flush_lsn()` (not the INSERT pointer) ensures target_lsn
is always reachable by the demux.  The demux sleep was reduced from 100 ms
to 1 ms so queue_wait ≈ 0-1 ms under any load.

## E2E Latency Distribution

| Metric | Value |
|--------|-------|
| Samples (n) | 500 |
| min | 0.3091 ms |
| p50 | 1.0564 ms |
| p95 | 14.592 ms |
| **p99** | **148.2306 ms** |
| avg | 6.2023 ms |
| max | 248.2459 ms |

## Supplementary: Demux Internal Processing Latency

From Demux Worker shared-memory circular buffer (measures XLogReadRecord → FlushPartitionWALWriter):

| Node | p99 (ms) | avg (ms) |
|------|----------|----------|
| Worker1 | None | N/A |
| Worker2 | None | N/A |

## Verdict

| Criterion | Threshold | Actual | Result |
|-----------|-----------|--------|--------|
| p99 latency | < 10 ms | 148.2306 ms | FAIL |
| avg latency | < 5 ms | 6.2023 ms | FAIL |

**延迟 p99 测试: FAIL**
