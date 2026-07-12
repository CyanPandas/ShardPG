-- 少于多数派时，控制面 propose 必须失败，且不能把变更 apply 到元数据表

SET citus.enable_ddl_propagation = off;

DELETE FROM partdist.node_map WHERE node_id = 99;

DO $$
DECLARE
  err_msg text;
BEGIN
  BEGIN
    PERFORM partdist.pg_raft_propose_node_status(99, 'down');
    RAISE EXCEPTION 'raft_05: expected no-quorum failure';
  EXCEPTION WHEN OTHERS THEN
    err_msg := SQLERRM;
    IF err_msg NOT LIKE '%raft propose failed%' THEN
      RAISE;
    END IF;
  END;

  PERFORM partdist.pg_raft_apply_committed();

  IF EXISTS (
      SELECT 1
      FROM partdist.node_map
      WHERE node_id = 99) THEN
    RAISE EXCEPTION 'raft_05: proposal was applied despite no quorum';
  END IF;
END
$$;

SELECT true AS no_quorum_reject_ok;
