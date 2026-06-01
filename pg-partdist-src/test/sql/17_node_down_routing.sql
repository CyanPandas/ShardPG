-- Test 17: node_down routing — primary node is 'down'
INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (9, 'down-node', 5435, 'down');
INSERT INTO partdist.partition_map (partition_id, primary_node)
VALUES (900::oid, 9);

-- route_write should return 'node_down'
SELECT partdist.pg_partdist_route_write(900::oid) AS route;

-- restore status and check routing changes
UPDATE partdist.node_map SET status = 'active' WHERE node_id = 9;
SELECT partdist.pg_partdist_cache_invalidate();
SELECT partdist.pg_partdist_route_write(900::oid) AS route;

DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;
