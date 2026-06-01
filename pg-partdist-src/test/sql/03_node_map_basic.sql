-- Test 03: node_map basic CRUD
INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (10, 'host-a', 5432, 'active');

SELECT node_id, hostname, port, status FROM partdist.node_map WHERE node_id = 10;

UPDATE partdist.node_map SET status = 'syncing' WHERE node_id = 10;
SELECT node_id, status FROM partdist.node_map WHERE node_id = 10;

UPDATE partdist.node_map SET status = 'down' WHERE node_id = 10;
SELECT node_id, status FROM partdist.node_map WHERE node_id = 10;

DELETE FROM partdist.node_map WHERE node_id = 10;
SELECT count(*) FROM partdist.node_map WHERE node_id = 10;
