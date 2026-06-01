-- Test 18: metadata version field and trigger

INSERT INTO partdist.node_map (node_id, hostname, port) VALUES (1, 'h', 5432);

-- version starts at 1 on INSERT
INSERT INTO partdist.partition_map (partition_id, primary_node)
VALUES (1000::oid, 1);
SELECT version FROM partdist.partition_map WHERE partition_id = 1000::oid;

-- Each UPDATE increments version by 1
UPDATE partdist.partition_map SET secondary_nodes = ARRAY[2]
WHERE  partition_id = 1000::oid;
SELECT version FROM partdist.partition_map WHERE partition_id = 1000::oid;

UPDATE partdist.partition_map SET secondary_nodes = ARRAY[2, 3]
WHERE  partition_id = 1000::oid;
SELECT version FROM partdist.partition_map WHERE partition_id = 1000::oid;

-- Setting version explicitly on INSERT is allowed (not enforced to be 1)
INSERT INTO partdist.partition_map (partition_id, primary_node, version)
VALUES (1001::oid, 1, 42);
SELECT version FROM partdist.partition_map WHERE partition_id = 1001::oid;

DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;
