-- 已有更长日志时，陈旧候选者的 RequestVote 必须被拒绝
-- 该用例在非 leader 节点执行，由 harness 先确保 leader 已提交至少一条新日志。

SET citus.enable_ddl_propagation = off;

DO $$
DECLARE
  curr_term bigint;
  last_idx bigint;
  last_term bigint;
  stale_idx bigint;
  stale_term bigint;
  resp text;
  vote_granted int;
BEGIN
  SELECT current_term
    INTO curr_term
  FROM partdist.pg_raft_get_cluster_status();

  SELECT log_index, term
    INTO last_idx, last_term
  FROM partdist.raft_log
  ORDER BY log_index DESC
  LIMIT 1;

  IF last_idx IS NULL OR last_idx <= 0 THEN
    RAISE EXCEPTION 'raft_06: expected at least one committed log entry';
  END IF;

  stale_idx := GREATEST(last_idx - 1, 0);
  stale_term := CASE
    WHEN stale_idx = 0 THEN 0
    WHEN last_term > 0 THEN last_term - 1
    ELSE 0
  END;

  resp := partdist.pg_raft_rpc(
    format('RV %s %s %s %s', curr_term, 99, stale_idx, stale_term)
  );
  vote_granted := split_part(resp, ' ', 2)::int;

  IF vote_granted <> 0 THEN
    RAISE EXCEPTION
      'raft_06: stale candidate unexpectedly received a vote (resp=%)',
      resp;
  END IF;
END
$$;

SELECT true AS stale_requestvote_rejected_ok;

DELETE FROM partdist.node_map WHERE node_id = 98;
