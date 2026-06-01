-- Test 02: partition_map basic CRUD
INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (1, 'coordinator', 5432, 'active'),
       (2, 'worker1',     5433, 'active'),
       (3, 'worker2',     5434, 'active');

INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes)
VALUES (100::oid, 1, ARRAY[2,3]);

SELECT partition_id, primary_node, secondary_nodes, version
FROM   partdist.partition_map
WHERE  partition_id = 100::oid;

-- update primary_node
UPDATE partdist.partition_map SET primary_node = 2
WHERE  partition_id = 100::oid;

SELECT partition_id, primary_node FROM partdist.partition_map
WHERE  partition_id = 100::oid;

-- delete
DELETE FROM partdist.partition_map WHERE partition_id = 100::oid;
SELECT count(*) FROM partdist.partition_map WHERE partition_id = 100::oid;

-- cleanup
DELETE FROM partdist.node_map;
