-- Raft: leader 函数可用
SET citus.enable_ddl_propagation = off;
SELECT partdist.pg_raft_version();
SELECT partdist.pg_raft_get_leader() >= 1 AS has_leader;
SELECT partdist.pg_raft_is_leader() IS NOT NULL AS is_leader_fn_ok;

-- Raft: 持久化状态表存在，便于后续验证 term / vote / commit 持久化
SELECT to_regclass('partdist.raft_state') IS NOT NULL AS raft_state_table_exists;
SELECT to_regclass('partdist.raft_log') IS NOT NULL AS raft_log_table_exists;
