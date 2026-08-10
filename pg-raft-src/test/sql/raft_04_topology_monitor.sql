-- TopologyMonitor 手动触发：模拟 worker2 宕机后的自动 failover
-- 前置：pg_partdist + pg_raft 已安装；worker2 已停止
--   pg_ctl stop -D /work/pg-cluster-data/worker2
--
-- ── 原判据的缺陷（2026-08-09 审查）────────────────────────────────────
-- 分区 9102 的夹具是 primary=3、secondary_nodes=ARRAY[1,2]，规格要求新主
-- **必须从该分区自己的副本集里挑**（pg_raft_failover_partitions_for_node 只在
-- secondary_nodes 里选、且要求 applied_part_lsn 追平 switch_partition_lsn）。
-- 旧判据只写 `primary_node <> 3`：failover 若从 node_map 里随便挑一个 active
-- 节点（例如 node 4，它根本不是 9102 的副本）也照样满足 —— 而"提升一个没有
-- 数据的节点"正是 raft_07/raft_10 花整篇在防的错。
-- 本版改成 `primary_node = ANY(<夹具声明的 secondary 集合>)`。
-- 注意不能写成 `= ANY(secondary_nodes)`：failover 成功后新主会被
-- pg_raft_build_secondary_json 从 secondary_nodes 里剔除（与 raft_02 里
-- propose(9102, 1, ARRAY[2]) 的形态一致），读到的是**改写后**的副本集，
-- 那样断言恒假。所以按夹具里写死的原始副本集 {1,2} 比对。

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
         WHERE partition_id = 9102::oid
           AND primary_node <> 3
           AND primary_node = ANY (ARRAY[1, 2])) THEN
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
SELECT primary_node <> 3 AND primary_node = ANY (ARRAY[1, 2])
         AS p2_failed_over_to_own_secondary
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
  -- 新主必须来自 9102 自己声明的副本集 {1,2}（见文件头）
  IF NOT EXISTS (
      SELECT 1 FROM partdist.partition_map
       WHERE partition_id = 9102::oid
         AND primary_node = ANY (ARRAY[1, 2])) THEN
    RAISE EXCEPTION
      'raft_04: partition 9102 被提升到 node %,它不在该分区声明的副本集 {1,2} 里 —— '
      'failover 没有按 secondary_nodes 选主',
      (SELECT primary_node FROM partdist.partition_map WHERE partition_id = 9102::oid);
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
