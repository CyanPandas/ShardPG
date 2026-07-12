-- HardState 崩溃恢复:follower 被 immediate 停机(模拟崩溃)重启后,
-- current_term / 已提交日志 / 复制链路必须保持连续。
-- 该用例在刚崩溃重启的 follower 上执行,由 harness 传入崩溃前基线:
--   -v term_before=<崩溃前 current_term>
--   -v idx_before=<崩溃前 max(log_index)>
-- harness 并在重启后由 leader 追加一条 node 99 status='down' 的新决议,
-- 本用例验证 follower 能追上并 apply(崩溃后复制/回放恢复)。

SET citus.enable_ddl_propagation = off;

SELECT set_config('raft09.term_before', :'term_before', false);
SELECT set_config('raft09.idx_before', :'idx_before', false);

DO $$
DECLARE
  term_before bigint := current_setting('raft09.term_before')::bigint;
  idx_before  bigint := current_setting('raft09.idx_before')::bigint;
  term_now    bigint;
  idx_now     bigint;
  is_leader_now boolean;
BEGIN
  SELECT s.current_term, s.is_leader
    INTO term_now, is_leader_now
  FROM partdist.pg_raft_get_cluster_status() AS s;

  -- 1) term 不能回退:hardstate(current_term/voted_for)必须先落盘再响应
  IF term_now IS NULL OR term_now < term_before THEN
    RAISE EXCEPTION
      'raft_09: current_term regressed after crash restart (before=%, now=%)',
      term_before, term_now;
  END IF;

  -- 2) 已提交日志不能丢
  SELECT max(log_index) INTO idx_now FROM partdist.raft_log;
  IF idx_now IS NULL OR idx_now < idx_before THEN
    RAISE EXCEPTION
      'raft_09: committed raft_log lost after crash restart (before=%, now=%)',
      idx_before, idx_now;
  END IF;

  -- 3) 崩溃重启的 follower 不得自立为 leader
  IF is_leader_now THEN
    RAISE EXCEPTION 'raft_09: crashed follower restarted as leader unexpectedly';
  END IF;
END
$$;

-- 4) 复制链路恢复:等待 harness 在重启后由 leader 追加的
--    node 99 status='down' 决议在本 follower 上被 apply
DO $$
DECLARE
  i integer;
BEGIN
  FOR i IN 1..30 LOOP
    PERFORM partdist.pg_raft_apply_committed();
    IF EXISTS (
        SELECT 1 FROM partdist.node_map
         WHERE node_id = 99 AND status = 'down') THEN
      RETURN;
    END IF;
    PERFORM pg_sleep(0.2);
  END LOOP;
  RAISE EXCEPTION
    'raft_09: post-restart proposal not applied on follower (replication did not resume)';
END
$$;

SELECT true AS hardstate_crash_recovery_ok;
