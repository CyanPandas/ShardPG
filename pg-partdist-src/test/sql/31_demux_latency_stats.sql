-- T2.2: demux_latency_stats returns NULLs when no samples or valid stats

-- Function must not error regardless of sample count
SELECT
    (partdist.demux_latency_stats()).p50_ms IS NULL
        OR (partdist.demux_latency_stats()).p50_ms >= 0 AS p50_ok,
    (partdist.demux_latency_stats()).p99_ms IS NULL
        OR (partdist.demux_latency_stats()).p99_ms >= 0 AS p99_ok,
    (partdist.demux_latency_stats()).avg_ms IS NULL
        OR (partdist.demux_latency_stats()).avg_ms >= 0 AS avg_ok;

-- p99 must be >= p50 when stats are available
SELECT
    CASE
        WHEN (partdist.demux_latency_stats()).p99_ms IS NULL THEN true
        ELSE (partdist.demux_latency_stats()).p99_ms >= (partdist.demux_latency_stats()).p50_ms
    END AS p99_gte_p50;
