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
