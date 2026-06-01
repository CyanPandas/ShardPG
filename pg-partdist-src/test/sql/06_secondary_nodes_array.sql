-- Test 06: secondary_nodes INTEGER[] column

-- Setup nodes
INSERT INTO partdist.node_map (node_id, hostname, port)
VALUES (1, 'coord', 5432), (2, 'w1', 5433), (3, 'w2', 5434);

-- Empty secondaries (default)
INSERT INTO partdist.partition_map (partition_id, primary_node)
VALUES (300::oid, 1);
SELECT array_length(secondary_nodes, 1) IS NULL AS empty_default
FROM   partdist.partition_map WHERE partition_id = 300::oid;

-- Single secondary
UPDATE partdist.partition_map SET secondary_nodes = ARRAY[2]
WHERE  partition_id = 300::oid;
SELECT secondary_nodes FROM partdist.partition_map WHERE partition_id = 300::oid;

-- Multiple secondaries
UPDATE partdist.partition_map SET secondary_nodes = ARRAY[2, 3]
WHERE  partition_id = 300::oid;
SELECT secondary_nodes, array_length(secondary_nodes, 1) AS cnt
FROM   partdist.partition_map WHERE partition_id = 300::oid;

-- Array element access
SELECT secondary_nodes[1], secondary_nodes[2]
FROM   partdist.partition_map WHERE partition_id = 300::oid;

DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;
