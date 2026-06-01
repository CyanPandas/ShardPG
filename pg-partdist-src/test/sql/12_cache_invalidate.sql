-- Test 12: cache invalidation

-- Populate data
INSERT INTO partdist.node_map (node_id, hostname, port) VALUES (1, 'coord', 5432);
INSERT INTO partdist.partition_map (partition_id, primary_node) VALUES (600::oid, 1);

-- Prime the cache
SELECT partdist.pg_partdist_get_primary(600::oid);

-- Invalidate
SELECT partdist.pg_partdist_cache_invalidate();

-- After invalidation, re-reading should still return correct data
-- (cache miss forces SPI re-fetch)
SELECT partdist.pg_partdist_get_primary(600::oid);

-- Change the primary and invalidate again
UPDATE partdist.node_map SET node_id = 1 WHERE node_id = 1; -- no-op to keep FK valid
INSERT INTO partdist.node_map (node_id, hostname, port) VALUES (5, 'newcoord', 5432) ON CONFLICT DO NOTHING;
UPDATE partdist.partition_map SET primary_node = 5 WHERE partition_id = 600::oid;
SELECT partdist.pg_partdist_cache_invalidate();
SELECT partdist.pg_partdist_get_primary(600::oid);

DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;
