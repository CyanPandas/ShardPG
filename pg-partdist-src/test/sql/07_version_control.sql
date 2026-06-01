-- Test 07: version field auto-increment trigger

INSERT INTO partdist.node_map (node_id, hostname, port) VALUES (1, 'h', 5432);
INSERT INTO partdist.partition_map (partition_id, primary_node)
VALUES (400::oid, 1);

-- initial version is 1
SELECT version FROM partdist.partition_map WHERE partition_id = 400::oid;

-- UPDATE bumps version
UPDATE partdist.partition_map SET secondary_nodes = ARRAY[2]
WHERE  partition_id = 400::oid;
SELECT version FROM partdist.partition_map WHERE partition_id = 400::oid;

-- second UPDATE bumps again
UPDATE partdist.partition_map SET primary_node = 1
WHERE  partition_id = 400::oid;
SELECT version FROM partdist.partition_map WHERE partition_id = 400::oid;

-- updated_at is set
SELECT updated_at IS NOT NULL FROM partdist.partition_map WHERE partition_id = 400::oid;

DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;
