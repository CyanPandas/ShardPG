-- Test 11: cache API — get_primary falls back to SPI when cache is cold

-- Setup
INSERT INTO partdist.node_map (node_id, hostname, port)
VALUES (1, 'coordinator', 5432), (2, 'worker1', 5433);
INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes)
VALUES (500::oid, 1, ARRAY[2]);

-- get_primary returns correct node
SELECT partdist.pg_partdist_get_primary(500::oid);

-- unknown partition returns NULL
SELECT partdist.pg_partdist_get_primary(99999::oid) IS NULL AS missing_is_null;

-- NULL input returns NULL (CALLED ON NULL INPUT but NULL oid → not_found → returns NULL)
SELECT partdist.pg_partdist_get_primary(NULL::oid) IS NULL AS null_oid_is_null;

DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;
