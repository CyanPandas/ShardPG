-- TopologyMonitor 手动触发：模拟 worker2 宕机后的自动 failover
-- 前置：pg_partdist + pg_raft 已安装；worker2 已停止
--   pg_ctl stop -D /work/pg-cluster-data/worker2

SET citus.enable_ddl_propagation = off;

DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;

INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (1, 'coordinator', 5432, 'active'),
       (2, 'worker1', 5433, 'active'),
       (3, 'worker2', 5434, 'active'),
       (4, 'worker3', 5435, 'active');

INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes)
VALUES (9101::oid, 1, ARRAY[2, 3]),
       (9102::oid, 3, ARRAY[1, 2]),
       (9103::oid, 2, ARRAY[1, 3]),
       (9104::oid, 4, ARRAY[1, 2]);

SET pg_raft.probe_fail_threshold = 1;
SELECT partdist.pg_raft_force_probe();

DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 1..20 LOOP
    PERFORM partdist.pg_raft_apply_committed();
    IF EXISTS (
        SELECT 1 FROM partdist.node_map
         WHERE node_id = 3 AND status = 'down')
       AND EXISTS (
        SELECT 1 FROM partdist.partition_map
         WHERE partition_id = 9102::oid AND primary_node <> 3) THEN
      RETURN;
    END IF;
    PERFORM pg_sleep(0.2);
  END LOOP;
END
$$;

SELECT status FROM partdist.node_map WHERE node_id = 3;
SELECT status = 'down' AS node3_down FROM partdist.node_map WHERE node_id = 3;
SELECT primary_node = 1 AS p1_unchanged
FROM partdist.partition_map WHERE partition_id = 9101::oid;
SELECT primary_node <> 3 AS p2_failed_over_from_node3
FROM partdist.partition_map WHERE partition_id = 9102::oid;
SELECT primary_node = 2 AS p3_unchanged
FROM partdist.partition_map WHERE partition_id = 9103::oid;
SELECT primary_node = 4 AS p4_unchanged
FROM partdist.partition_map WHERE partition_id = 9104::oid;

DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM partdist.node_map
       WHERE node_id = 3 AND status = 'down') THEN
    RAISE EXCEPTION 'raft_04: node3 was not marked down';
  END IF;
  IF NOT EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9101::oid AND primary_node = 1) THEN
    RAISE EXCEPTION 'raft_04: partition 9101 primary changed unexpectedly';
  END IF;
  IF EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9102::oid AND primary_node = 3) THEN
    RAISE EXCEPTION 'raft_04: partition 9102 still points to failed node3';
  END IF;
  IF NOT EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9103::oid AND primary_node = 2) THEN
    RAISE EXCEPTION 'raft_04: partition 9103 primary changed unexpectedly';
  END IF;
  IF NOT EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9104::oid AND primary_node = 4) THEN
    RAISE EXCEPTION 'raft_04: partition 9104 primary changed unexpectedly';
  END IF;
END
$$;
