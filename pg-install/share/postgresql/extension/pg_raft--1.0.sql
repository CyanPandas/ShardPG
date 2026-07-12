-- pg_raft 1.0 — 控制面 Raft（元数据 / failover）
-- 扩展安装在 partdist schema（见 pg_raft.control）

CREATE TABLE IF NOT EXISTS raft_state (
    singleton        INT PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
    leader_node_id   INT NOT NULL DEFAULT 1,
    current_term     BIGINT NOT NULL DEFAULT 1,
    lease_until      TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO raft_state (singleton, leader_node_id, current_term)
VALUES (1, 1, 1)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS raft_log (
    log_id      BIGSERIAL PRIMARY KEY,
    log_index   BIGINT,
    term        BIGINT NOT NULL,
    op_type     TEXT NOT NULL,
    payload     JSONB NOT NULL,
    committed   BOOLEAN NOT NULL DEFAULT false,
    created_at  TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE raft_log ADD COLUMN IF NOT EXISTS log_index BIGINT;
UPDATE raft_log SET log_index = log_id WHERE log_index IS NULL;
ALTER TABLE raft_log ALTER COLUMN log_index SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_raft_log_log_index ON raft_log(log_index);

CREATE TABLE IF NOT EXISTS raft_snapshot (
    singleton           INT PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
    last_included_index BIGINT NOT NULL DEFAULT 0,
    node_map            JSONB NOT NULL DEFAULT '[]'::jsonb,
    partition_map       JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raft_state IS '控制面 Raft 持久化状态：leader 标识、term 与租约信息。';
COMMENT ON TABLE raft_log IS '控制面已复制日志：元数据变更、failover 决议等。';
COMMENT ON TABLE raft_snapshot IS '当前控制面元数据快照，用于恢复与后续快照安装。';

CREATE OR REPLACE FUNCTION pg_raft_version()
    RETURNS text LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_raft_version';

CREATE OR REPLACE FUNCTION pg_raft_is_leader()
    RETURNS boolean LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_raft_is_leader';

CREATE OR REPLACE FUNCTION pg_raft_get_leader()
    RETURNS integer LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_raft_get_leader';

CREATE OR REPLACE FUNCTION pg_raft_get_cluster_status()
    RETURNS TABLE(
        leader_node_id integer,
        current_term bigint,
        local_node_id integer,
        is_leader boolean,
        backend text
    ) LANGUAGE c STABLE
    AS 'MODULE_PATHNAME', 'pg_raft_get_cluster_status';

CREATE OR REPLACE FUNCTION pg_raft_propose_node_status(p_node_id integer, p_status text)
    RETURNS bigint LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_propose_node_status';

CREATE OR REPLACE FUNCTION pg_raft_propose_partition_primary(
    p_partition_id oid,
    p_primary_node integer,
    p_secondary_nodes integer[]
) RETURNS bigint LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_propose_partition_primary';

CREATE OR REPLACE FUNCTION pg_raft_apply_payload(p_op_type text, p_payload jsonb)
    RETURNS boolean LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_apply_payload';

CREATE OR REPLACE FUNCTION raft_propose_node_status(p_node_id integer, p_status text)
    RETURNS bigint LANGUAGE sql STRICT VOLATILE
    AS $$ SELECT pg_raft_propose_node_status($1, $2) $$;

CREATE OR REPLACE FUNCTION raft_propose_partition_primary(
    p_partition_id oid,
    p_primary_node integer,
    p_secondary_nodes integer[]
) RETURNS bigint LANGUAGE sql STRICT VOLATILE
    AS $$ SELECT pg_raft_propose_partition_primary($1, $2, $3) $$;

CREATE OR REPLACE FUNCTION pg_raft_force_probe()
    RETURNS boolean LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_force_probe';

COMMENT ON FUNCTION pg_raft_force_probe() IS
    '手动触发一次 TopologyMonitor 循环（仅 Leader 执行）：探测节点并生成 failover 提议。';

COMMENT ON FUNCTION pg_raft_propose_partition_primary(oid, integer, integer[]) IS
    '提交 OP_PARTITION_PRIMARY 控制面决议；当前接口对外仍接收 partition_id / new_primary / secondary_nodes，内部日志已保留 old_primary 与切换点字段。';

-- 纯 C Raft 节点间 RPC 端点（RequestVote / AppendEntries）。
-- 由对端 BGWorker 经 libpq 调用，在普通 backend 中执行，仅读写共享内存。
CREATE OR REPLACE FUNCTION pg_raft_rpc(p_msg text)
    RETURNS text LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_rpc';

COMMENT ON FUNCTION pg_raft_rpc(text) IS
    'Raft RPC 端点：处理 "RV <term> <cand> <last_idx> <last_term>" 和 "AE <term> <leader>" 消息。';

CREATE OR REPLACE FUNCTION pg_raft_append_entries(
    p_term bigint,
    p_leader_id integer,
    p_prev_log_index bigint,
    p_prev_log_term bigint,
    p_leader_commit bigint,
    p_entry_index bigint DEFAULT NULL,
    p_entry_term bigint DEFAULT NULL,
    p_entry_op text DEFAULT NULL,
    p_entry_payload text DEFAULT NULL
) RETURNS text LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_append_entries';

COMMENT ON FUNCTION pg_raft_append_entries IS
    'Raft AppendEntries RPC：向 follower 复制日志条目或发送心跳。';

CREATE OR REPLACE FUNCTION pg_raft_apply_committed()
    RETURNS boolean LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_apply_committed';

COMMENT ON FUNCTION pg_raft_apply_committed() IS
    '将所有已提交但尚未 apply 的 Raft 日志应用到 partdist 元数据表。';
