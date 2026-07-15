-- failover 候选选择:在追平 switch_partition_lsn 的副本中,必须提升
-- applied_part_lsn 最大者,而不是 secondary 列表中先出现者;
-- 且决议 payload 里的 switch_partition_lsn 必须来自真实分区写入路径
-- (get_partition_flush_lsn),不是写死的值。
--
-- harness 预置(在本用例执行前):
--   -v cand_lo=<node_id>  该节点 follower_partition_map 中 applied_part_lsn=2(落后)
--   -v cand_mid=<node_id> 该节点 applied_part_lsn=5(恰好追平)
--   -v cand_hi=<node_id>  该节点 applied_part_lsn=7(最追平)
-- 本用例在 leader 上写 5 条真实 parwal 记录 → switch_partition_lsn=5;
-- secondary 顺序故意为 [lo, mid, hi]:旧的"取第一个追平者"会选 mid,
-- 正确行为必须选 hi。

SET citus.enable_ddl_propagation = off;

SELECT set_config('raft10.cand_lo',  :'cand_lo',  false);
SELECT set_config('raft10.cand_mid', :'cand_mid', false);
SELECT set_config('raft10.cand_hi',  :'cand_hi',  false);

SELECT partdist.reset_partition_wal_state(9108::oid);

DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 1..5 LOOP
    PERFORM partdist.write_partition_wal_record(9108::oid, 1);
  END LOOP;
END
$$;

SELECT partdist.demux_flush();

DO $$
DECLARE
  switch_lsn bigint;
BEGIN
  SELECT partdist.get_partition_flush_lsn(9108::oid)
    INTO switch_lsn;

  IF switch_lsn <> 5 THEN
    RAISE EXCEPTION 'raft_10: expected switch_partition_lsn=5 from real write path, got %', switch_lsn;
  END IF;
END
$$;

DELETE FROM partdist.partition_map WHERE partition_id = 9108::oid;
DELETE FROM partdist.node_map WHERE node_id IN (1, 2, 3, 4, 77, 98, 99);

INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (1, '127.0.0.1', 5432, 'active'),
       (2, '127.0.0.1', 5433, 'active'),
       (3, '127.0.0.1', 5434, 'active'),
       (4, '127.0.0.1', 5435, 'active'),
       (77, '127.0.0.1', 5999, 'active');

DO $$
DECLARE
  lo  integer := current_setting('raft10.cand_lo')::integer;
  mid integer := current_setting('raft10.cand_mid')::integer;
  hi  integer := current_setting('raft10.cand_hi')::integer;
BEGIN
  INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes)
  VALUES (9108::oid, 77, ARRAY[lo, mid, hi]);
END
$$;

SET pg_raft.probe_fail_threshold = 1;
SELECT partdist.pg_raft_force_probe();

DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 1..20 LOOP
    PERFORM partdist.pg_raft_apply_committed();
    IF EXISTS (
        SELECT 1 FROM partdist.partition_map
         WHERE partition_id = 9108::oid AND primary_node <> 77) THEN
      EXIT;
    END IF;
    PERFORM pg_sleep(0.2);
  END LOOP;
END
$$;

DO $$
DECLARE
  hi  integer := current_setting('raft10.cand_hi')::integer;
  primary_after integer;
  payload_switch_lsn bigint;
BEGIN
  SELECT primary_node
    INTO primary_after
  FROM partdist.partition_map
  WHERE partition_id = 9108::oid;

  -- 必须提升最追平者(hi),而不是列表里先出现的追平者(mid)
  IF primary_after IS DISTINCT FROM hi THEN
    RAISE EXCEPTION
      'raft_10: expected most caught-up node % promoted, got primary_after=%',
      hi, primary_after;
  END IF;

  -- 决议 payload 的 switch_partition_lsn 必须等于真实写入路径的 flush 进度
  SELECT (payload->>'switch_partition_lsn')::bigint
    INTO payload_switch_lsn
  FROM partdist.raft_log
  WHERE op_type = 'OP_PARTITION_PRIMARY'
    AND (payload->>'partition_id')::oid = 9108::oid
  ORDER BY log_index DESC
  LIMIT 1;

  IF payload_switch_lsn IS DISTINCT FROM 5 THEN
    RAISE EXCEPTION
      'raft_10: decision payload switch_partition_lsn=% does not match real flush progress 5',
      payload_switch_lsn;
  END IF;
END
$$;

SELECT true AS most_caught_up_secondary_promoted_ok;

DELETE FROM partdist.partition_map WHERE partition_id = 9108::oid;
DELETE FROM partdist.node_map WHERE node_id = 77;

SELECT partdist.reset_partition_wal_state(9108::oid);
