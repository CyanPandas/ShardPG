-- 控制面 Leader 固定在 node 1；node 2 上 propose 应失败
SET citus.enable_ddl_propagation = off;
SELECT partdist.pg_raft_is_leader() AS leader_on_coord;
-- 在协调节点执行；worker 上需单独测
DO $$
BEGIN
  IF current_setting('pg_raft.node_id', true) IS NOT NULL
     AND current_setting('pg_raft.node_id')::int <> 1 THEN
    PERFORM partdist.pg_raft_propose_node_status(1, 'down');
    RAISE EXCEPTION 'expected failure on non-leader';
  END IF;
EXCEPTION WHEN OTHERS THEN
  IF SQLERRM NOT LIKE '%only leader%' THEN
    RAISE;
  END IF;
END $$;
SELECT true AS split_brain_guard_ok;
