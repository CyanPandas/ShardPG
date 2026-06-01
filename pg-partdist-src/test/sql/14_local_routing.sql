-- Test 14: local routing decision
-- When primary_node matches local_node_id, route must be 'local'.
-- In the test cluster, local_node_id is set via GUC to 1 (coordinator).

-- Setup
INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (1, 'coordinator', 5432, 'active');
INSERT INTO partdist.partition_map (partition_id, primary_node)
VALUES (700::oid, 1);

-- Force local_node_id = 1 for this session
SET pg_partdist.local_node_id = 1;

SELECT partdist.pg_partdist_route_write(700::oid) AS route;

RESET pg_partdist.local_node_id;
DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;
