-- raft_12: 分区级 Raft 组隔离性（P1 验收）
--
-- 前提（由 run-raft-tests.sh 布置）：已在两个不同节点各建一个数据组
--   :gid_a 与 :gid_b，且两组各自完成选举；本脚本在**控制面 leader 节点**执行。
--
-- 断言：
--   1. 控制面组 0 恒存在，且仍是本节点视角的唯一控制面组；
--   2. 两个数据组在本节点均可见（组信息经 RPC 自动传播），且各自有 leader；
--   3. 两组的 leader 不是同一个节点 —— 组间领导权独立，不是"一个全局 leader"；
--   4. 数据组的日志与控制面日志互不串扰：raft_log 按 group_id 分命名空间，
--      :gid_a 的条目数 == 该组 last_log_index，且组 0 的日志未被数据组条目污染；
--   5. 数据组的已提交条目在本节点（follower）上已 apply（last_applied == commit_index）。

\set ON_ERROR_STOP on

SELECT set_config('raft12.gid_a', :'gid_a', false);
SELECT set_config('raft12.gid_b', :'gid_b', false);

DO $$
DECLARE
  gid_a        BIGINT := current_setting('raft12.gid_a')::bigint;
  gid_b        BIGINT := current_setting('raft12.gid_b')::bigint;
  n_control    INT;
  leader_a     INT;
  leader_b     INT;
  state_a      TEXT;
  state_b      TEXT;
  lastidx_a    BIGINT;
  commit_a     BIGINT;
  applied_a    BIGINT;
  rows_a       BIGINT;
  rows_ctrl    BIGINT;
  ctrl_lastidx BIGINT;
BEGIN
  -- 1. 控制面组
  SELECT count(*) INTO n_control
  FROM partdist.pg_raft_group_status() WHERE group_id = 0;
  IF n_control <> 1 THEN
    RAISE EXCEPTION 'raft_12: 控制面组 0 应恰好存在 1 个，实际 %', n_control;
  END IF;

  -- 2. 两个数据组均可见且各有 leader
  SELECT state, leader_node_id INTO state_a, leader_a
  FROM partdist.pg_raft_group_status() WHERE group_id = gid_a;
  SELECT state, leader_node_id INTO state_b, leader_b
  FROM partdist.pg_raft_group_status() WHERE group_id = gid_b;

  IF state_a IS NULL THEN
    RAISE EXCEPTION 'raft_12: 数据组 % 未传播到本节点', gid_a;
  END IF;
  IF state_b IS NULL THEN
    RAISE EXCEPTION 'raft_12: 数据组 % 未传播到本节点', gid_b;
  END IF;
  IF leader_a IS NULL OR leader_a <= 0 THEN
    RAISE EXCEPTION 'raft_12: 数据组 % 没有 leader', gid_a;
  END IF;
  IF leader_b IS NULL OR leader_b <= 0 THEN
    RAISE EXCEPTION 'raft_12: 数据组 % 没有 leader', gid_b;
  END IF;

  -- 3. 组间领导权独立
  IF leader_a = leader_b THEN
    RAISE EXCEPTION
      'raft_12: 两个数据组的 leader 都是节点 %，组间领导权未独立', leader_a;
  END IF;

  -- 4. 日志按组分命名空间
  SELECT last_log_index, commit_index, last_applied
    INTO lastidx_a, commit_a, applied_a
  FROM partdist.pg_raft_group_status() WHERE group_id = gid_a;

  SELECT count(*) INTO rows_a
  FROM partdist.raft_log WHERE group_id = gid_a;

  IF lastidx_a <= 0 THEN
    RAISE EXCEPTION 'raft_12: 数据组 % 未收到任何日志条目', gid_a;
  END IF;
  IF rows_a <> lastidx_a THEN
    RAISE EXCEPTION
      'raft_12: 数据组 % 的 raft_log 行数 % 与 last_log_index % 不一致',
      gid_a, rows_a, lastidx_a;
  END IF;

  SELECT count(*), coalesce(max(log_index), 0) INTO rows_ctrl, ctrl_lastidx
  FROM partdist.raft_log WHERE group_id = 0;
  IF EXISTS (SELECT 1 FROM partdist.raft_log
              WHERE group_id = 0 AND op_type = 'OP_TEST') THEN
    RAISE EXCEPTION 'raft_12: 数据组条目串入了控制面日志（group_id = 0）';
  END IF;
  IF rows_ctrl = 0 THEN
    RAISE EXCEPTION 'raft_12: 控制面日志为空，基线不成立';
  END IF;

  -- 5. 数据组已提交条目已 apply
  IF commit_a <> lastidx_a THEN
    RAISE EXCEPTION
      'raft_12: 数据组 % 的 commit_index % 未追上 last_log_index %',
      gid_a, commit_a, lastidx_a;
  END IF;
  IF applied_a <> commit_a THEN
    RAISE EXCEPTION
      'raft_12: 数据组 % 的 last_applied % 未追上 commit_index %',
      gid_a, applied_a, commit_a;
  END IF;

  RAISE NOTICE
    'raft_12 OK: group % leader=node% (log=%), group % leader=node%, 控制面 group0 log=%',
    gid_a, leader_a, lastidx_a, gid_b, leader_b, ctrl_lastidx;
END;
$$;
