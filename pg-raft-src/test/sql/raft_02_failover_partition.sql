-- Raft: 注册元数据并通过 propose 模拟 failover(4 节点拓扑)
SET citus.enable_ddl_propagation = off;
CREATE EXTENSION IF NOT EXISTS pg_partdist;
CREATE EXTENSION IF NOT EXISTS pg_raft;

DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;

INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (1, 'coordinator', 5432, 'active'),
       (2, 'worker1', 5433, 'active'),
       (3, 'worker2', 5434, 'active'),
       (4, 'worker3', 5435, 'active');

INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes)
VALUES (9101::oid, 1, ARRAY[2,3]),
       (9102::oid, 3, ARRAY[1,2]),
       (9103::oid, 2, ARRAY[1,3]),
       (9104::oid, 4, ARRAY[1,2]);

SELECT partdist.pg_raft_propose_node_status(3, 'down');
SELECT partdist.pg_raft_propose_partition_primary(9102::oid, 1, ARRAY[2]);

SELECT partdist.pg_partdist_cache_invalidate();
SET pg_partdist.local_node_id = 1;
-- 多分区元数据 Raft：只切换受 node3 影响的 P2，P1/P3 不应变化。
SELECT primary_node = 1 AS p1_still_on_node1
FROM partdist.partition_map WHERE partition_id = 9101::oid;
SELECT primary_node = 1 AS p2_failover_to_node1
FROM partdist.partition_map WHERE partition_id = 9102::oid;
SELECT primary_node = 2 AS p3_still_on_node2
FROM partdist.partition_map WHERE partition_id = 9103::oid;
SELECT primary_node = 4 AS p4_still_on_node4
FROM partdist.partition_map WHERE partition_id = 9104::oid;

DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9101::oid AND primary_node = 1) THEN
    RAISE EXCEPTION 'raft_02: partition 9101 primary changed unexpectedly';
  END IF;
  IF NOT EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9102::oid AND primary_node = 1) THEN
    RAISE EXCEPTION 'raft_02: partition 9102 did not fail over to node 1';
  END IF;
  IF NOT EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9103::oid AND primary_node = 2) THEN
    RAISE EXCEPTION 'raft_02: partition 9103 primary changed unexpectedly';
  END IF;
  IF NOT EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9104::oid AND primary_node = 4) THEN
    RAISE EXCEPTION 'raft_02: partition 9104 primary changed unexpectedly';
  END IF;
END
$$;
