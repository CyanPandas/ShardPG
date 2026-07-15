-- 旧 leader 追平回归:旧 leader 停机期间,新 leader 提交了若干新决议;
-- 旧 leader 重启回归后,必须通过 AppendEntries 追平这些已提交日志并 apply,
-- 且只能以 follower 身份存在。
-- 该用例在刚重启的旧 leader 上执行,由 harness 传入:
--   -v idx_target=<新 leader 停机期间提交后的 max(log_index)>
--   -v expect_status=<node 99 在这些决议后的最终状态>

SET citus.enable_ddl_propagation = off;

SELECT set_config('raft11.idx_target', :'idx_target', false);
SELECT set_config('raft11.expect_status', :'expect_status', false);

DO $$
DECLARE
  idx_target    bigint := current_setting('raft11.idx_target')::bigint;
  expect_status text   := current_setting('raft11.expect_status');
  idx_now       bigint;
  caught_up     boolean := false;
  i             integer;
BEGIN
  -- 1) 日志追平 + apply 追平:轮询等待旧 leader 复制并应用停机期间的决议
  FOR i IN 1..40 LOOP
    PERFORM partdist.pg_raft_apply_committed();

    SELECT max(log_index) INTO idx_now FROM partdist.raft_log;

    IF idx_now IS NOT NULL AND idx_now >= idx_target
       AND EXISTS (
           SELECT 1 FROM partdist.node_map
            WHERE node_id = 99 AND status = expect_status) THEN
      caught_up := true;
      EXIT;
    END IF;
    PERFORM pg_sleep(0.5);
  END LOOP;

  IF NOT caught_up THEN
    RAISE EXCEPTION
      'raft_11: old leader did not catch up committed log after rejoin (max_log_index=%, target=%)',
      idx_now, idx_target;
  END IF;
END
$$;

-- 2) 追平后旧 leader 必须仍是 follower(新 leader 已在更高 term 上)
DO $$
DECLARE
  is_leader_now boolean;
BEGIN
  SELECT s.is_leader
    INTO is_leader_now
  FROM partdist.pg_raft_get_cluster_status() AS s;

  IF is_leader_now THEN
    RAISE EXCEPTION 'raft_11: old leader reclaimed leadership unexpectedly after catch-up';
  END IF;
END
$$;

SELECT true AS old_leader_log_catchup_ok;
