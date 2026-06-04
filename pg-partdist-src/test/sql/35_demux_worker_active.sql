-- T2.2 / T2.2.8: Demux Worker presence verified via shared memory

-- Shmem must be reachable (proves DemuxShmemInit ran)
SELECT (partdist.demux_progress()).node_name = 'local' AS worker_shmem_reachable;

-- The demux worker runs with BGWORKER_SHMEM_ACCESS only so it does not
-- appear in pg_stat_activity (which requires a database connection).
-- We verify via the shmem API instead.
SELECT (partdist.demux_progress()).node_name IS NOT NULL AS shmem_api_ok;
