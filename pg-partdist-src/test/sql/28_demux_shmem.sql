-- T2.2: Test demux shared memory initialization

-- Demux state must be accessible (shmem initialized)
SELECT (partdist.demux_progress()).node_name = 'local' AS node_name_ok;

-- Worker shmem reachable regardless of timing
SELECT (partdist.demux_progress()).node_name IS NOT NULL AS progress_reachable;
