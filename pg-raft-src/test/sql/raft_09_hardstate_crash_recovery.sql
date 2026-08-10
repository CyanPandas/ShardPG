-- HardState 崩溃恢复:follower 被 immediate 停机(模拟崩溃)重启后,
-- current_term / voted_for / 已提交日志 / 复制链路必须保持连续。
--
-- ── 原判据的缺陷（2026-08-09 审查）────────────────────────────────────
-- 旧版只断言 "term 不回退"（term_now >= term_before）。但 follower 收到任何
-- 更高 term 的 AppendEntries 就会把 term 顶回来（raft_consensus.c 的
--   if (in_term > ctx.cons->current_term) { current_term = in_term; ... }），
-- 而崩溃重启后 leader 的心跳几乎立刻就到 —— 于是**把整个 pg_raft_hardstate
-- 文件删掉**（term 归零、voted_for 归零）也照样满足 term_now >= term_before。
-- 驱动那边也只 `test -f .../pg_raft_hardstate` 验文件在不在、不验内容。
--
-- 真正的风险是 **voted_for 丢失**：同一 term 内可以重复投票 ⇒ 一个 term 出现
-- 两个 leader ⇒ Election Safety 失效。旧判据对此零覆盖。
--
-- ── 本版判据 ─────────────────────────────────────────────────────────
-- pg_raft_get_cluster_status() / pg_raft_group_status() 都不导出 voted_for，
-- 没有只读入口，所以由 harness 在**节点还停着**的时候直接解码盘上的
-- pg_raft_hardstate（结构见 raft_consensus.c 的 RaftHardStateFile：
--   magic u32 / version u32 / current_term i64 / voted_for i32 / …，共 56 字节），
-- 并在重启后**第一时间**（leader 心跳把 term 顶回来之前）发一发 RequestVote
-- 做行为侧对照，两组测量值经 -v 传进来由本用例判定：
--   -v term_before=<崩溃前 current_term>   -v idx_before=<崩溃前 max(log_index)>
--   -v hs_term=<盘上 hardstate 的 current_term>
--   -v hs_voted=<盘上 hardstate 的 voted_for>
--   -v rv_term=<重启后 RequestVote 应答里的 term>
--   -v rv_flag=<重启后 RequestVote 是否授票:0=拒 1=授>
-- 其中 RequestVote 用**崩溃前的 term** + 一个**全新候选人 id** + 一份刻意造得
-- 足够新的日志发出：三个授票条件里日志新旧那条必然成立、term 那条也成立，
-- 唯一还能拒绝的理由就是 voted_for —— 拒绝即证明 voted_for 真的落了盘。
-- harness 在崩溃前会先把 voted_for 顶成非零（对一个虚拟候选人发一次 RV），
-- 所以 "hs_voted <> 0" 是确定性的，不依赖这一轮恰好投过票。

SET citus.enable_ddl_propagation = off;

SELECT set_config('raft09.term_before', :'term_before', false);
SELECT set_config('raft09.idx_before', :'idx_before', false);
SELECT set_config('raft09.hs_term',    :'hs_term',    false);
SELECT set_config('raft09.hs_voted',   :'hs_voted',   false);
SELECT set_config('raft09.rv_term',    :'rv_term',    false);
SELECT set_config('raft09.rv_flag',    :'rv_flag',    false);

DO $$
DECLARE
  term_before bigint := current_setting('raft09.term_before')::bigint;
  idx_before  bigint := current_setting('raft09.idx_before')::bigint;
  hs_term     bigint := current_setting('raft09.hs_term')::bigint;
  hs_voted    bigint := current_setting('raft09.hs_voted')::bigint;
  rv_term     bigint := current_setting('raft09.rv_term')::bigint;
  rv_flag     int    := current_setting('raft09.rv_flag')::int;
  term_now    bigint;
  idx_now     bigint;
  is_leader_now boolean;
BEGIN
  -- 0) 基线必须真的取到（harness 侧已 fail-fast,这里再兜一层:
  --    term_before/idx_before 若退化成 0,下面的判据全部变成恒真）
  IF term_before <= 0 THEN
    RAISE EXCEPTION
      'raft_09: 崩溃前 term 基线为 %,基线没取到 —— 判据会退化成恒真', term_before;
  END IF;

  -- 1) HardState **内容**必须完好:term 落了盘
  IF hs_term <> term_before THEN
    RAISE EXCEPTION
      'raft_09: 盘上 hardstate 的 current_term=% 与崩溃前进程内的 % 不一致 —— term 未在响应前落盘',
      hs_term, term_before;
  END IF;

  -- 2) HardState **内容**必须完好:voted_for 落了盘（丢它 = 同 term 可重复投票）
  IF hs_voted = 0 THEN
    RAISE EXCEPTION
      'raft_09: 盘上 hardstate 的 voted_for=0 —— 崩溃前刚投出的票没有落盘,'
      '同一 term 内可重复投票,Election Safety 失效';
  END IF;

  -- 3) 行为侧对照:重启后用崩溃前的 term + 新候选人 + 足够新的日志发 RequestVote,
  --    必须被拒（唯一能拒的理由就是 voted_for 已被恢复）
  IF rv_term <> term_before THEN
    RAISE EXCEPTION
      'raft_09: 重启后 RequestVote 应答的 term=% ≠ 崩溃前的 %（窗口内发生了改选）,'
      '本轮 voted_for 判据失去甄别力',
      rv_term, term_before;
  END IF;
  IF rv_flag <> 0 THEN
    RAISE EXCEPTION
      'raft_09: 崩溃重启后同一 term 内又把票投给了新候选人（RequestVote 应答=%）'
      ' —— voted_for 未从 hardstate 恢复',
      rv_flag;
  END IF;

  -- 4) 进程内 term 不能回退（保留原判据,但它只是辅助:见文件头说明,
  --    单靠它丢掉整个 hardstate 也能过）
  SELECT s.current_term, s.is_leader
    INTO term_now, is_leader_now
  FROM partdist.pg_raft_get_cluster_status() AS s;

  IF term_now IS NULL OR term_now < term_before THEN
    RAISE EXCEPTION
      'raft_09: current_term regressed after crash restart (before=%, now=%)',
      term_before, term_now;
  END IF;

  -- 5) 已提交日志不能丢
  SELECT max(log_index) INTO idx_now FROM partdist.raft_log WHERE group_id = 0;
  IF idx_now IS NULL OR idx_now < idx_before THEN
    RAISE EXCEPTION
      'raft_09: committed raft_log lost after crash restart (before=%, now=%)',
      idx_before, idx_now;
  END IF;

  -- 6) 崩溃重启的 follower 不得自立为 leader
  IF is_leader_now THEN
    RAISE EXCEPTION 'raft_09: crashed follower restarted as leader unexpectedly';
  END IF;
END
$$;

-- 7) 复制链路恢复:等待 harness 在重启后由 leader 追加的
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
