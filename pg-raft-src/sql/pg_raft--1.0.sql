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

-- P1 分区级 Raft 组：日志按组分命名空间。group_id = 0 为控制面组，
-- 其余为数据面分区组（group_id == partdist.shard_identity.global_shard_id）。
ALTER TABLE raft_log ADD COLUMN IF NOT EXISTS group_id BIGINT NOT NULL DEFAULT 0;
DROP INDEX IF EXISTS idx_raft_log_log_index;
CREATE UNIQUE INDEX IF NOT EXISTS idx_raft_log_group_index
    ON raft_log(group_id, log_index);

-- 组注册表：重启后 shmem 只剩控制面组，数据组由此恢复。
CREATE TABLE IF NOT EXISTS raft_group (
    group_id   BIGINT PRIMARY KEY,
    members    INTEGER[] NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raft_group IS
    '数据面 Raft 组注册表：group_id 取 partdist.shard_identity.global_shard_id（Citus shardid），members 为该分片副本所在节点。';

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

CREATE OR REPLACE FUNCTION pg_raft_report_data_leader(
    p_group_id bigint,
    p_leader_node integer,
    p_term bigint,
    p_secondary_nodes integer[]
) RETURNS bigint LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_report_data_leader';

-- ------------------------------------------------------------------
-- DTX-2PC 决议层（DTX_2PC_DESIGN.md §6）
-- ------------------------------------------------------------------
-- 决议记录写在**协调组**（写集内按 hash(dtxid) 选出的那个分区组）的日志里；
-- **该记录在协调组达到多数派持久化即为全局提交点**，dtx_decide 只有在那之后
-- 才返回成功。索引表 partdist.dtx_decision 由各成员的 apply 路径维护，
-- 因此协调组切主后新 leader 立刻可答（协调权随 Raft 选举自动转移）。
CREATE OR REPLACE FUNCTION dtx_decide(
    p_coord_gsid bigint,
    p_dtxid bigint,
    p_verdict integer,
    p_participants bigint[] DEFAULT NULL
) RETURNS integer LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_dtx_decide';

COMMENT ON FUNCTION dtx_decide(bigint, bigint, integer, bigint[]) IS
    '在协调组 leader 上写入全局决议并等多数派持久化（=提交点）。返回最终生效的 verdict（1=COMMIT 2=ABORT）；本节点不是协调组 leader 时返回 NULL，调用方按 partition_map 重新寻址。决议槽一次性：已有决议则原样返回，不覆盖。';

CREATE OR REPLACE FUNCTION dtx_status(
    p_coord_gsid bigint,
    p_dtxid bigint
) RETURNS integer LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_dtx_status';

CREATE OR REPLACE FUNCTION dtx_recover_prepared(
    p_timeout_ms integer DEFAULT 30000
) RETURNS integer LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_dtx_recover_prepared';

COMMENT ON FUNCTION dtx_recover_prepared(integer) IS
    '参与者侧恢复守护：扫描本节点超时未闭合的 shardpg_dtx_* prepared 事务，向协调组问决议并 COMMIT/ROLLBACK PREPARED，同时补写 DTX_COMMIT/ABORT 标记。问不到决议时保持 prepared 不动（推定中止的权力只在协调组手里）。返回本轮处理数。';

COMMENT ON FUNCTION dtx_status(bigint, bigint) IS
    '参与者恢复时查询决议（推定中止）：查无决议时**先写一条 ABORT 决议并达多数派**再返回 2，防止"问的时候没有、答完又被写成 COMMIT"。本节点不是协调组 leader 时返回 NULL。';

COMMENT ON FUNCTION pg_raft_report_data_leader(bigint, integer, bigint, integer[]) IS
    '数据组新任 leader 的登记入口（须在 group 0 leader 上执行）：过任期栅栏后把 '
    'OP_PARTITION_PRIMARY 提进 group 0，apply 时各节点更新 partition_map 并把真实 '
    'Citus 分片的 pg_dist_placement 指向新主。返回 >0=已提名 / -1=无需登记 / 0=非 group 0 leader。';

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
    p_entry_payload text DEFAULT NULL,
    p_group_id bigint DEFAULT 0,
    p_entry_data bytea DEFAULT NULL
) RETURNS text LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_append_entries';

COMMENT ON FUNCTION pg_raft_append_entries IS
    'Raft AppendEntries RPC：向 follower 复制日志条目或发送心跳。';

CREATE OR REPLACE FUNCTION pg_raft_apply_committed()
    RETURNS boolean LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_apply_committed';

COMMENT ON FUNCTION pg_raft_apply_committed() IS
    '将所有已提交但尚未 apply 的 Raft 日志应用到 partdist 元数据表。';


-- ---- P1：分区级 Raft 组管理接口 ----

CREATE OR REPLACE FUNCTION pg_raft_group_create_internal(
    p_group_id bigint, p_members integer[]
) RETURNS boolean LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_group_create';

CREATE OR REPLACE FUNCTION pg_raft_group_drop_internal(p_group_id bigint)
    RETURNS boolean LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_group_drop';

CREATE OR REPLACE FUNCTION pg_raft_group_propose(
    p_group_id bigint, p_op_type text, p_payload text
) RETURNS bigint LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_group_propose';

CREATE OR REPLACE FUNCTION pg_raft_group_status()
    RETURNS TABLE(
        group_id       bigint,
        state          text,
        current_term   bigint,
        leader_node_id integer,
        last_log_index bigint,
        commit_index   bigint,
        last_applied   bigint,
        cluster_size   integer
    ) LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_group_status';

-- 建组：先落注册表（重启可恢复），再建 shmem 状态机。
CREATE OR REPLACE FUNCTION pg_raft_group_create(
    p_group_id bigint, p_members integer[] DEFAULT NULL
) RETURNS boolean LANGUAGE plpgsql VOLATILE
    SET search_path = partdist, pg_catalog
AS $fn$
BEGIN
    INSERT INTO partdist.raft_group (group_id, members)
    VALUES (p_group_id, coalesce(p_members, '{}'::integer[]))
    ON CONFLICT (group_id) DO UPDATE SET members = EXCLUDED.members;
    RETURN partdist.pg_raft_group_create_internal(p_group_id, p_members);
END;
$fn$;

CREATE OR REPLACE FUNCTION pg_raft_group_drop(p_group_id bigint)
    RETURNS boolean LANGUAGE plpgsql VOLATILE
    SET search_path = partdist, pg_catalog
AS $fn$
BEGIN
    DELETE FROM partdist.raft_group WHERE group_id = p_group_id;
    DELETE FROM partdist.raft_log   WHERE group_id = p_group_id;
    RETURN partdist.pg_raft_group_drop_internal(p_group_id);
END;
$fn$;

COMMENT ON FUNCTION pg_raft_group_create(bigint, integer[]) IS
    '创建一个数据面 Raft 组（group_id 建议取 Citus shardid）；members 为空表示全体 peers。';
COMMENT ON FUNCTION pg_raft_group_status() IS
    '列出本节点全部活跃 Raft 组的角色/term/日志游标；group_id=0 为控制面组。';

-- ---- P2：数据组以 parwal 记录为 Raft entry ----

CREATE OR REPLACE FUNCTION pg_raft_data_propose(
    p_group_id bigint, p_partition_lsn bigint
) RETURNS bigint LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_data_propose';

COMMENT ON FUNCTION pg_raft_data_propose(bigint, bigint) IS
    '在数据组 leader 上把本节点 pg_parwal 的第 partition_lsn 条记录作为 Raft entry 提交；'
    '返回 Raft log index（0=失败）。提交成功即多数派已 fsync 落盘且 applied_part_lsn 已推进。';

CREATE OR REPLACE FUNCTION pg_raft_group_reset_internal()
    RETURNS integer LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_group_reset';

-- 丢弃本节点全部数据组(含注册表与日志);group 0 保留。
CREATE OR REPLACE FUNCTION pg_raft_group_reset()
    RETURNS integer LANGUAGE plpgsql VOLATILE
    SET search_path = partdist, pg_catalog
AS $fn$
DECLARE
    n integer;
BEGIN
    DELETE FROM partdist.raft_group;
    DELETE FROM partdist.raft_log WHERE group_id <> 0;
    n := partdist.pg_raft_group_reset_internal();
    RETURN n;
END;
$fn$;

COMMENT ON FUNCTION pg_raft_group_reset() IS
    '清空本节点全部数据面 Raft 组(shmem 状态 + HardState 文件 + 注册表 + 日志);控制面 group 0 不受影响。';
