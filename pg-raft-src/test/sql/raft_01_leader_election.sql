-- Raft 控制面选举：必须**真的**选出了 leader，且持久化状态表在位。
--
-- ★ 判据必须写成 RAISE EXCEPTION，不能用裸 SELECT。
-- psql 的 ON_ERROR_STOP=1 只对**错误**生效，`SELECT false` 的退出码依然是 0 ——
-- 原版四条裸 `SELECT <expr> AS <name>` 在"集群根本没选出 leader"
-- （pg_raft_get_leader() 返回 0）时同样报 PASS，对被测代码零覆盖。
SET citus.enable_ddl_propagation = off;

DO $$
DECLARE
  ldr int;
BEGIN
  ldr := partdist.pg_raft_get_leader();
  IF ldr IS NULL OR ldr < 1 THEN
    RAISE EXCEPTION 'raft_01: 控制面没有 leader（pg_raft_get_leader() = %）', ldr;
  END IF;

  IF partdist.pg_raft_is_leader() IS NULL THEN
    RAISE EXCEPTION 'raft_01: pg_raft_is_leader() 返回 NULL';
  END IF;

  -- 本文件在协调节点上跑，而 coordinator_node_id=1 只是**偏好不是保证**
  -- （raft_consensus.c 给协调节点更短的选举窗口，worker 接任后不会自动交还）。
  -- 所以这里只断言"is_leader 与 get_leader 自洽"，不断言 leader 一定是本节点。
  IF partdist.pg_raft_is_leader() <> (ldr = current_setting('pg_raft.node_id')::int) THEN
    RAISE EXCEPTION 'raft_01: is_leader(%) 与 get_leader(%)/node_id(%) 不自洽',
                    partdist.pg_raft_is_leader(), ldr,
                    current_setting('pg_raft.node_id');
  END IF;

  IF to_regclass('partdist.raft_state') IS NULL THEN
    RAISE EXCEPTION 'raft_01: partdist.raft_state 不存在';
  END IF;
  IF to_regclass('partdist.raft_log') IS NULL THEN
    RAISE EXCEPTION 'raft_01: partdist.raft_log 不存在';
  END IF;
END $$;
