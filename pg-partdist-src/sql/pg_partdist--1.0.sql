-- pg_partdist 1.0 installation script
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_partdist" to load this file. \quit

-- Note: PostgreSQL sets search_path to the extension schema ('partdist') for
-- the duration of this script when 'schema = partdist' is in the control file.
-- All unqualified CREATE statements therefore land in the partdist schema.

-- ----------------------------------------------------------------
-- Core metadata tables
-- ----------------------------------------------------------------

-- partition_map: maps each partition OID to its primary node and
-- an array of secondary node IDs.
CREATE TABLE partition_map (
    partition_id    OID         NOT NULL,
    primary_node    INTEGER     NOT NULL,
    secondary_nodes INTEGER[]   NOT NULL DEFAULT '{}',
    version         BIGINT      NOT NULL DEFAULT 1,
    updated_at      TIMESTAMPTZ          DEFAULT now(),
    CONSTRAINT pk_partition_map PRIMARY KEY (partition_id)
);

COMMENT ON TABLE partition_map IS
    'Maps partition OIDs to their primary/secondary node assignments.';

-- node_map: tracks every node in the pg_partdist cluster.
CREATE TABLE node_map (
    node_id        INTEGER     NOT NULL,
    hostname       TEXT        NOT NULL,
    port           INTEGER     NOT NULL CHECK (port BETWEEN 1 AND 65535),
    status         TEXT        NOT NULL DEFAULT 'active'
                               CHECK (status IN ('active', 'down', 'syncing')),
    last_heartbeat TIMESTAMPTZ,
    CONSTRAINT pk_node_map PRIMARY KEY (node_id)
);

COMMENT ON TABLE node_map IS
    'Tracks every node participating in the pg_partdist cluster.';

-- ----------------------------------------------------------------
-- Trigger: auto-bump version on partition_map changes
-- ----------------------------------------------------------------

CREATE OR REPLACE FUNCTION partition_map_version_bump()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.version    := OLD.version + 1;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_partition_map_version
    BEFORE UPDATE ON partition_map
    FOR EACH ROW
    EXECUTE FUNCTION partition_map_version_bump();

-- ----------------------------------------------------------------
-- C-backed functions (MODULE_PATHNAME = pg_partdist.so)
-- ----------------------------------------------------------------

CREATE OR REPLACE FUNCTION pg_partdist_version()
    RETURNS text
    LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_version';

COMMENT ON FUNCTION pg_partdist_version() IS
    'Returns the pg_partdist extension version string.';

-- Look up the primary node for a partition (hits the shared-memory cache).
CREATE OR REPLACE FUNCTION pg_partdist_get_primary(partition_id OID)
    RETURNS integer
    LANGUAGE c CALLED ON NULL INPUT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_get_primary';

COMMENT ON FUNCTION pg_partdist_get_primary(OID) IS
    'Returns the primary node ID for the given partition OID, or NULL if not registered.';

-- Invalidate the entire shared-memory cache by bumping the generation counter.
CREATE OR REPLACE FUNCTION pg_partdist_cache_invalidate()
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_cache_invalidate';

COMMENT ON FUNCTION pg_partdist_cache_invalidate() IS
    'Invalidates the pg_partdist shared-memory metadata cache.';

-- Manually bump the metadata generation (useful in tests or after bulk loads).
CREATE OR REPLACE FUNCTION pg_partdist_bump_metadata_version()
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_bump_metadata_version';

COMMENT ON FUNCTION pg_partdist_bump_metadata_version() IS
    'Increments the global metadata generation counter, forcing cache re-population.';

-- Return cache statistics from shared memory.
CREATE OR REPLACE FUNCTION pg_partdist_cache_stats(
    OUT partition_hits      BIGINT,
    OUT partition_misses    BIGINT,
    OUT node_hits           BIGINT,
    OUT node_misses         BIGINT,
    OUT metadata_generation BIGINT
)
    RETURNS record
    LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_cache_stats';

COMMENT ON FUNCTION pg_partdist_cache_stats() IS
    'Returns cache hit/miss counters and the current metadata generation.';

-- Evaluate the routing decision for a write to partition_id.
CREATE OR REPLACE FUNCTION pg_partdist_route_write(partition_id OID)
    RETURNS text
    LANGUAGE c CALLED ON NULL INPUT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_route_write';

COMMENT ON FUNCTION pg_partdist_route_write(OID) IS
    'Returns the routing decision for a write to the given partition: '
    'local | remote | not_found | node_down.';

-- ----------------------------------------------------------------
-- Milestone 2.1 — partition WAL directory management & record format
-- ----------------------------------------------------------------

-- Create pg_parwal/<partition_id>/ directory under DataDir.
CREATE OR REPLACE FUNCTION init_partition_wal(partition_id OID)
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_init_partition_wal';

COMMENT ON FUNCTION init_partition_wal(OID) IS
    'Create pg_parwal/<partition_id>/ under DataDir (idempotent).';

-- Return true if pg_parwal/<partition_id>/ exists and is a directory.
CREATE OR REPLACE FUNCTION partition_wal_exists(partition_id OID)
    RETURNS boolean
    LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_partition_wal_exists';

COMMENT ON FUNCTION partition_wal_exists(OID) IS
    'Return true if the pg_parwal directory for partition_id exists.';

-- Return the relative path of the WAL segment file for a given segment number.
CREATE OR REPLACE FUNCTION partition_wal_path(partition_id OID, segno BIGINT)
    RETURNS text
    LANGUAGE c STRICT IMMUTABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_partition_wal_path';

COMMENT ON FUNCTION partition_wal_path(OID, BIGINT) IS
    'Return pg_parwal/<partition_id>/<segname> for the given segment number.';

-- Remove segment files in pg_parwal/<partition_id>/ below keep_lsn.
CREATE OR REPLACE FUNCTION cleanup_partition_wal(partition_id OID, keep_lsn PG_LSN)
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_cleanup_partition_wal';

COMMENT ON FUNCTION cleanup_partition_wal(OID, PG_LSN) IS
    'Remove pg_parwal segment files whose end-LSN is at or below keep_lsn.';

-- Allocate the next per-partition WAL sequence number (for testing/inspection).
CREATE OR REPLACE FUNCTION alloc_partition_lsn(partition_id OID)
    RETURNS bigint
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_alloc_partition_lsn';

COMMENT ON FUNCTION alloc_partition_lsn(OID) IS
    'Return and increment the per-partition WAL LSN counter (1-based).';

-- Write a PartWALHeader record to the WAL and to pg_parwal/<partition_id>/.
-- Returns the global WAL LSN of the written record.
CREATE OR REPLACE FUNCTION write_partition_wal_record(
    partition_id OID,
    flags        INTEGER DEFAULT 1
)
    RETURNS pg_lsn
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_write_partition_wal_record';

COMMENT ON FUNCTION write_partition_wal_record(OID, INTEGER) IS
    'Write a PartWALHeader WAL record for partition_id; return the assigned LSN.';

-- Scan pg_parwal/<partition_id>/ and return all PartWALHeader records.
CREATE OR REPLACE FUNCTION check_partition_wal(partition_id OID)
    RETURNS TABLE(
        partition_lsn   BIGINT,
        orig_node_lsn   PG_LSN,
        flags           INTEGER,
        is_valid        BOOLEAN
    )
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_check_partition_wal';

COMMENT ON FUNCTION check_partition_wal(OID) IS
    'Return all PartWALHeader records from pg_parwal/<partition_id>/.';

-- Verify that partition_lsn is strictly monotone and all records have valid magic.
CREATE OR REPLACE FUNCTION verify_partition_wal(partition_id OID)
    RETURNS boolean
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_verify_partition_wal';

COMMENT ON FUNCTION verify_partition_wal(OID) IS
    'Return true if all pg_parwal records for partition_id are internally consistent.';

-- Reset pg_parwal files + shmem LSN counter for one partition (regression tests only).
CREATE OR REPLACE FUNCTION reset_partition_wal_state(partition_id OID)
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_reset_partition_wal_state';

COMMENT ON FUNCTION reset_partition_wal_state(OID) IS
    'Delete all pg_parwal segment files for partition_id and reset its LSN counter to 0.';

-- ----------------------------------------------------------------
-- Milestone 2.2 — Demux Worker and auxiliary functions
-- ----------------------------------------------------------------

-- Count all PartWALHeader records stored in pg_parwal/<partition_id>/.
CREATE OR REPLACE FUNCTION count_parwal_records(partition_id OID)
    RETURNS bigint
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_count_parwal_records';

COMMENT ON FUNCTION count_parwal_records(OID) IS
    'Return the total number of PartWALHeader records on disk for the given partition.';

-- Returns true once the one-shot crash-recovery BGW has finished.
-- Test scripts poll this instead of checking ps (BGW exits after recovery).
CREATE OR REPLACE FUNCTION demux_is_ready()
    RETURNS boolean
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_demux_is_ready';

COMMENT ON FUNCTION demux_is_ready() IS
    'Returns true once the crash-recovery background worker has completed and the synchronous write path is active.';

-- Read Demux Worker progress from shared memory.
CREATE OR REPLACE FUNCTION demux_progress(
    OUT node_name          TEXT,
    OUT last_processed_lsn PG_LSN
)
    RETURNS record
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_demux_progress';

COMMENT ON FUNCTION demux_progress() IS
    'Return the last WAL LSN processed by the Demux Worker on this node.';

-- Read rolling processing-latency statistics from the Demux Worker.
CREATE OR REPLACE FUNCTION demux_latency_stats(
    OUT p50_ms FLOAT,
    OUT p99_ms FLOAT,
    OUT avg_ms FLOAT
)
    RETURNS record
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_demux_latency_stats';

COMMENT ON FUNCTION demux_latency_stats() IS
    'Return p50/p99/avg Demux processing latency in milliseconds (NULL if no samples).';

-- Scan all pg_parwal/<partition_id>/ records and return partition_lsn + orig_node_lsn.
CREATE OR REPLACE FUNCTION read_all_headers(partition_id OID)
    RETURNS TABLE(
        partition_lsn   BIGINT,
        orig_node_lsn   PG_LSN
    )
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_read_all_headers';

COMMENT ON FUNCTION read_all_headers(OID) IS
    'Return (partition_lsn, orig_node_lsn) for every record in pg_parwal/<partition_id>/.';

-- Block until the Demux Worker has processed all WAL to the current position.
-- Regression tests that write records and immediately read back pg_parwal content
-- must call this to ensure the Demux Worker has had time to process.
CREATE OR REPLACE FUNCTION demux_flush()
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_demux_flush';

COMMENT ON FUNCTION demux_flush() IS
    'Block until the Demux Worker has processed WAL to the current flush LSN (30 s timeout).';

-- ----------------------------------------------------------------
-- Follower replay support
-- ----------------------------------------------------------------

-- follower_partition_map: tracks the mapping from logical partition_id
-- (the primary's OID, used as directory name in pg_parwal/) to the
-- follower's local table name, plus replay progress.
--
-- applied_part_lsn is updated atomically with each SPI apply batch so
-- that a follower crash during replay is safe to resume from this offset.
CREATE TABLE IF NOT EXISTS follower_partition_map (
    partition_id      OID     NOT NULL,
    local_relname     TEXT    NOT NULL,
    applied_part_lsn  BIGINT  NOT NULL DEFAULT 0,
    CONSTRAINT pk_follower_partition_map PRIMARY KEY (partition_id)
);

COMMENT ON TABLE follower_partition_map IS
    'Maps primary partition OIDs to local table names for follower lazy replay. '
    'applied_part_lsn records the last successfully committed PartWAL record.';
