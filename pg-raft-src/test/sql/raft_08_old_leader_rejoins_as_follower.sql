-- 旧 leader 恢复后必须作为 follower 回归，且不能继续以 leader 身份对外 propose。

SET citus.enable_ddl_propagation = off;

DO $$
DECLARE
  leader_node_id integer;
  current_term bigint;
  local_node_id integer;
  is_leader boolean;
BEGIN
  SELECT s.leader_node_id, s.current_term, s.local_node_id, s.is_leader
    INTO leader_node_id, current_term, local_node_id, is_leader
  FROM partdist.pg_raft_get_cluster_status() AS s;

  IF is_leader THEN
    RAISE EXCEPTION
      'raft_08: restarted old leader unexpectedly became leader again (local_node_id=%)',
      local_node_id;
  END IF;

  IF leader_node_id = local_node_id OR leader_node_id <= 0 THEN
    RAISE EXCEPTION
      'raft_08: expected another node to be leader after re-election, leader_node_id=%, local_node_id=%',
      leader_node_id, local_node_id;
  END IF;

  IF current_term <= 0 THEN
    RAISE EXCEPTION 'raft_08: invalid current_term=%', current_term;
  END IF;

END
$$;

DO $$
DECLARE
  err_msg text;
BEGIN
  BEGIN
    PERFORM partdist.pg_raft_propose_node_status(100, 'active');
    RAISE EXCEPTION 'raft_08: restarted old leader unexpectedly accepted propose';
  EXCEPTION WHEN OTHERS THEN
    err_msg := SQLERRM;
    IF err_msg NOT LIKE '%only leader%' THEN
      RAISE;
    END IF;
  END;
END
$$;

SELECT true AS old_leader_rejoins_as_follower_ok;
