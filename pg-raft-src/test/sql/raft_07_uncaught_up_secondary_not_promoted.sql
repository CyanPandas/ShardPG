-- 未追平 PartWAL 的副本不能被 Raft failover 自动提升为新 primary。
-- 该用例使用一个不可连接的虚拟旧 primary，避免破坏四节点 Raft 多数派。
--
-- ── 原判据的缺陷（2026-08-09 审查）────────────────────────────────────
-- 夹具明明写了 **5** 条 PartWAL 记录再 demux_flush，switch_partition_lsn 因此
-- 必须恰好是 5（raft_10 用同一夹具、断言的就是 `<> 5` 即失败）。
-- 旧版这里只写 `switch_lsn <= 0` ⇒ 只要 flush 进度是任意正数就过：
-- 少 flush 了几条、把 5 条合成 1 条、甚至读到上一轮残留的进度，都照样绿。
-- 改成精确值 5。

SET citus.enable_ddl_propagation = off;

SELECT partdist.reset_partition_wal_state(9107::oid);

DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 1..5 LOOP
    PERFORM partdist.write_partition_wal_record(9107::oid, 1);
  END LOOP;
END
$$;

SELECT partdist.demux_flush();

DO $$
DECLARE
  switch_lsn bigint;
BEGIN
  SELECT partdist.get_partition_flush_lsn(9107::oid)
    INTO switch_lsn;

  -- 夹具写了 5 条记录 ⇒ switch_partition_lsn 必须恰好 5（与 raft_10 同判据）
  IF switch_lsn <> 5 THEN
    RAISE EXCEPTION
      'raft_07: expected switch_partition_lsn=5 from real write path, got %', switch_lsn;
  END IF;
END
$$;

DELETE FROM partdist.partition_map WHERE partition_id = 9107::oid;
DELETE FROM partdist.node_map WHERE node_id IN (1, 2, 3, 4, 77, 98, 99);

INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (1, '127.0.0.1', 5432, 'active'),
       (2, '127.0.0.1', 5433, 'active'),
       (3, '127.0.0.1', 5434, 'active'),
       (4, '127.0.0.1', 5435, 'active'),
       (77, '127.0.0.1', 5999, 'active');

INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes)
VALUES (9107::oid, 77, ARRAY[1, 2]);

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
         WHERE node_id = 77 AND status = 'down') THEN
      EXIT;
    END IF;
    PERFORM pg_sleep(0.2);
  END LOOP;
END
$$;

DO $$
DECLARE
  primary_after integer;
BEGIN
  SELECT primary_node
    INTO primary_after
  FROM partdist.partition_map
  WHERE partition_id = 9107::oid;

  IF primary_after IS DISTINCT FROM 77 THEN
    RAISE EXCEPTION
      'raft_07: uncaught-up secondary was promoted unexpectedly, primary_after=%',
      primary_after;
  END IF;

  IF NOT EXISTS (
      SELECT 1 FROM partdist.node_map
       WHERE node_id = 77 AND status = 'down') THEN
    RAISE EXCEPTION 'raft_07: virtual failed primary was not marked down';
  END IF;
END
$$;

SELECT true AS uncaught_up_secondary_rejected_ok;

DELETE FROM partdist.partition_map WHERE partition_id = 9107::oid;
DELETE FROM partdist.node_map WHERE node_id = 77;

SELECT partdist.reset_partition_wal_state(9107::oid);
