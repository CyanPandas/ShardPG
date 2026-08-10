-- 已有更长日志时，陈旧候选者的 RequestVote 必须被拒绝；
-- 日志足够新的候选者必须拿到票（正向对照）。
-- 该用例在非 leader 节点执行，由 harness 先确保 leader 已提交至少一条新日志。
--
-- ── 原判据的缺陷（2026-08-09 审查）────────────────────────────────────
-- 旧版用 `RV curr_term 99 <stale_idx> <stale_term>` —— 与本节点**同一个** term。
-- 授票条件是三个 && 串联（raft_consensus.c pg_raft_rpc 的 RV 分支）：
--     in_term >= my_term
--  && (voted_for == 0 || voted_for == in_node)
--  && candidate_log_is_up_to_date_locked(...)
-- 而一个已经参与过本 term 选举的 follower，voted_for 必然是它投过的那个节点
-- （不是 99）⇒ **第二个条件就短路**，日志新旧检查一次都没被调用。
-- 后果：把 candidate_log_is_up_to_date_locked 整个删掉（永远返回 true）
-- 本用例照样 PASS。而且旧版只有"必须被拒"一侧，没有正向对照 ——
-- 一个恒拒的实现（Leader Completeness 直接没了）同样能过。
--
-- ── 本版判据 ─────────────────────────────────────────────────────────
-- 用 **curr_term + 1** 发：in_term > my_term 会先把 term 顶上去并**清空
-- voted_for**（那段代码在 RV 分支之前），于是第二个条件恒真、短路消失，
-- 日志新旧检查必然被执行。两侧都要：
--   ① 日志陈旧（last_idx-1 / last_term-1）的候选人 ⇒ 必须被拒（返回 0）；
--   ② 再用 curr_term + 2、日志取本节点**当前**的 last_idx/last_term
--      （相等即"至少一样新"）⇒ 必须拿到票（返回 1）。
--
-- 副作用（有意为之，harness 已配合）：本节点的 term 会比 leader 高 2，
-- leader 收到应答后 step_down_if_higher ⇒ 触发一轮改选。所以驱动里
-- raft_03（同样要跑在非 leader 上）已被排到本用例**之前**。

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

  IF curr_term IS NULL OR curr_term <= 0 THEN
    RAISE EXCEPTION 'raft_06: 取不到本节点 current_term（%），判据无从建立', curr_term;
  END IF;

  SELECT log_index, term
    INTO last_idx, last_term
  FROM partdist.raft_log
  WHERE group_id = 0
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

  -- ① 反向：日志陈旧的候选人必须被拒。
  --    term 用 curr_term+1 ⇒ voted_for 被清空 ⇒ 唯一能拒的理由只剩日志新旧。
  resp := partdist.pg_raft_rpc(
    format('RV %s %s %s %s', curr_term + 1, 99, stale_idx, stale_term)
  );
  vote_granted := split_part(resp, ' ', 2)::int;

  IF vote_granted <> 0 THEN
    RAISE EXCEPTION
      'raft_06: 日志陈旧(idx=%,term=%)的候选人竟然拿到了票(resp=%) —— '
      '候选人日志新旧检查失效,Leader Completeness 无保障',
      stale_idx, stale_term, resp;
  END IF;

  -- ② 正向对照：日志足够新的候选人必须拿到票。
  --    没有这一侧的话,"恒拒"的实现同样能过 ①。
  --    重新取一次 last_idx/last_term:上一发 RV 不会改日志,但 leader 可能在
  --    这期间又追加了条目,用旧值会因为"没那么新"被合法拒绝、造成假失败。
  SELECT log_index, term
    INTO last_idx, last_term
  FROM partdist.raft_log
  WHERE group_id = 0
  ORDER BY log_index DESC
  LIMIT 1;

  resp := partdist.pg_raft_rpc(
    format('RV %s %s %s %s', curr_term + 2, 98, last_idx, last_term)
  );
  vote_granted := split_part(resp, ' ', 2)::int;

  IF vote_granted <> 1 THEN
    RAISE EXCEPTION
      'raft_06: 日志与本节点一样新(idx=%,term=%)的候选人在更高 term(%)上被拒(resp=%) '
      '—— 授票条件过严/恒拒,集群将永远选不出 leader',
      last_idx, last_term, curr_term + 2, resp;
  END IF;
END
$$;

SELECT true AS stale_requestvote_rejected_ok;

DELETE FROM partdist.node_map WHERE node_id = 98;
