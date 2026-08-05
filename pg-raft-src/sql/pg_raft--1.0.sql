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

-- 数据组日志外部化（计划文档 §11.10 E1）：段式边界表。
--
-- 数据组的每条 Raft 条目都恰好对应一条 parwal 记录，条目载荷那串描述符与
-- PartWALRecord 头部字段逐个对应 —— 也就是说日志内容**已经在段文件里**。
-- 缺的只有 term 与 index↔plsn 的对应关系，这张表补的正是这两样。
--
-- 一行 = 一个 run："从 start_index 起连续若干条，plsn 自 start_plsn 起同步 +1，
-- term 恒为 term"，延伸到下一个 run 的 start_index 为止。于是
--   条目 i 的 term = 满足 start_index <= i 的最大那行的 term
--   条目 i 的 plsn = start_plsn + (i - start_index)
-- 只在 term 变了（换届）或 plsn 跳号（失败路径）时才追加一行，稳态下一届一行。
CREATE TABLE IF NOT EXISTS raft_log_runs (
    group_id    BIGINT NOT NULL,
    start_index BIGINT NOT NULL,
    start_plsn  BIGINT NOT NULL,
    term        BIGINT NOT NULL,
    PRIMARY KEY (group_id, start_index)
);

COMMENT ON TABLE raft_log_runs IS
    '数据组日志的段式边界表（§11.10）：把 (index → plsn, term) 压成按任期/连续性分段的 run，条目载荷从 pg_parwal 记录头部重建。';

CREATE TABLE IF NOT EXISTS raft_snapshot (
    singleton           INT PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
    last_included_index BIGINT NOT NULL DEFAULT 0,
    node_map            JSONB NOT NULL DEFAULT '[]'::jsonb,
    partition_map       JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 日志压缩：基点那一条的 term。压缩之后条目本身没了，而 prev 一致性检查与
-- 选举的日志新旧比较还要用它，所以必须随快照一起持久化。
ALTER TABLE raft_snapshot ADD COLUMN IF NOT EXISTS last_included_term BIGINT NOT NULL DEFAULT 0;

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
    '参与者侧恢复守护：扫描本节点超时未闭合的 shardpg_dtx_*/citus_* prepared 事务，向协调组（或按 Citus 规则向 master）问决议并 COMMIT/ROLLBACK PREPARED，同时补写 DTX_COMMIT/ABORT 标记。问不到决议时保持 prepared 不动。顺带做回执清扫、FORGET 重试与登记 GC。返回本轮处理数。';

-- ------------------------------------------------------------------
-- DTX-2PC 决议 GC：回执 + FORGET（DTX_2PC_DESIGN.md §9.7）
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dtx_ack(
    p_coord_gsid bigint,
    p_dtxid bigint,
    p_gsids bigint[] DEFAULT NULL
) RETURNS boolean LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_dtx_ack';

COMMENT ON FUNCTION dtx_ack(bigint, bigint, bigint[]) IS
    '参与者回执：在协调组 leader 上把 p_gsids 并进该决议的 acked。收齐（acked ⊇ participants）即追加 FORGET 记录复制到多数派，各成员 apply 时同步删除决议行。非 leader 返回 NULL；行已不存在（已 FORGET）返回 true。空数组调用 = 只做收齐检查与 FORGET 重试。';

CREATE OR REPLACE FUNCTION dtx_gc_dist_transaction()
    RETURNS integer LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_dtx_gc_dist_transaction';

COMMENT ON FUNCTION dtx_gc_dist_transaction() IS
    'pg_dist_transaction 的 GC（Citus 2PC 恢复被关闭后由本项目接管）：仅删除"发起 backend 已死且所有节点均确认无该 gid 的 prepared 事务"的行；任一节点不可达返回 -1 且整轮不删。';

-- ------------------------------------------------------------------
-- DTX-2PC 升主 in-doubt 闭合（DTX_2PC_DESIGN.md §9.6，机制先行）
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dtx_close_indoubt(
    p_partition_id oid
) RETURNS integer LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_dtx_close_indoubt';

COMMENT ON FUNCTION dtx_close_indoubt(oid) IS
    '对本节点该分区的 parwal 流做 in-doubt 闭合：找出有 DTX_PREPARE 而无闭合记录的 dtxid，按 登记协调组 → 本地决议索引 → 广播 peers → citus 前缀规则 的顺序全网求决议，找到即补写 DTX_COMMIT/ABORT 标记。找不到的保持 in-doubt（NOTICE 报数）。返回闭合数。升主序列在追平之后、对外服务之前调用。';

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
        cluster_size   integer,
        base_index     bigint,
        base_term      bigint
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
    DELETE FROM partdist.raft_group     WHERE group_id = p_group_id;
    DELETE FROM partdist.raft_log       WHERE group_id = p_group_id;
    DELETE FROM partdist.raft_log_runs  WHERE group_id = p_group_id;
    RETURN partdist.pg_raft_group_drop_internal(p_group_id);
END;
$fn$;

COMMENT ON FUNCTION pg_raft_group_create(bigint, integer[]) IS
    '创建一个数据面 Raft 组（group_id 建议取 Citus shardid）；members 为空表示全体 peers。';
COMMENT ON FUNCTION pg_raft_group_status() IS
    '列出本节点全部活跃 Raft 组的角色/term/日志游标；group_id=0 为控制面组。'
    'base_index/base_term 是日志压缩基点（快照 last_included_*），0 表示从未压缩过；'
    'index <= base_index 的条目已被快照取代并从 raft_log 删除。';

-- ---- P2：数据组以 parwal 记录为 Raft entry ----

CREATE OR REPLACE FUNCTION pg_raft_data_propose(
    p_group_id bigint, p_partition_lsn bigint
) RETURNS bigint LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_data_propose';

COMMENT ON FUNCTION pg_raft_data_propose(bigint, bigint) IS
    '在数据组 leader 上把本节点 pg_parwal 的第 partition_lsn 条记录作为 Raft entry 提交；'
    '返回 Raft log index（0=失败）。提交成功即多数派已 fsync 落盘且 applied_part_lsn 已推进。';

-- 数据组日志外部化（§11.10 E1）：按段式边界表把第 p_index 条条目**重建**出来。
--
-- term 取自覆盖该 index 的 run；plsn = start_plsn + (index - start_index)；
-- 载荷从 pg_parwal 记录头部拼回来 —— 字段与格式必须与 data_propose_one()
-- 写进 raft_log.payload 的那串**完全一致**，E1 的用例逐条比对二者。
--
-- 查不到 run、或该 plsn 在本节点段文件里不存在时返回 0 行（partwal_read_record
-- 读不到记录时返回的是一行全 NULL，不是零行，所以要显式过滤 orig_lsn IS NULL）。
CREATE OR REPLACE FUNCTION pg_raft_entry_from_parwal(
    p_group_id  bigint,
    p_index     bigint,
    OUT term    bigint,
    OUT payload text
) RETURNS record LANGUAGE sql STABLE AS $$
    WITH run AS (
        SELECT r.start_index, r.start_plsn, r.term
          FROM partdist.raft_log_runs r
         WHERE r.group_id = p_group_id
           AND r.start_index <= p_index
         ORDER BY r.start_index DESC
         LIMIT 1
    )
    SELECT run.term,
           format('{"partition_lsn":%s,"orig_lsn":"%s","rmid":%s,"info":%s,'
                  '"xid":%s,"nbytes":%s,"flags":%s}',
                  run.start_plsn + (p_index - run.start_index),
                  w.orig_lsn::text, w.rmid, w.info, w.xid,
                  length(w.data), w.flags)
      FROM run,
           LATERAL partdist.partwal_read_record(
                       partdist.local_partition_for_shard(p_group_id),
                       run.start_plsn + (p_index - run.start_index)) w
     WHERE w.orig_lsn IS NOT NULL;
$$;

COMMENT ON FUNCTION pg_raft_entry_from_parwal(bigint, bigint) IS
    '数据组日志外部化（§11.10）：按 raft_log_runs 反查 (term, plsn) 并从 pg_parwal 记录头部重建该条目的载荷。';

CREATE OR REPLACE FUNCTION pg_raft_install_snapshot(
    p_term bigint, p_leader_id integer,
    p_last_included_index bigint, p_last_included_term bigint,
    p_node_map text, p_partition_map text
) RETURNS text LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_install_snapshot';

COMMENT ON FUNCTION pg_raft_install_snapshot(bigint, integer, bigint, bigint, text, text) IS
    'InstallSnapshot RPC（仅控制面/组 0）：用快照整体替换 node_map/partition_map、同步路由层、'
    '把日志压缩基点推进到 last_included_index。返回 "term flag"（flag=1 表示已安装）。'
    '日志被压缩掉的那一段只能靠它传输——落后成员的 nextIndex 落到基点及更早时由 leader 自动调用。';

CREATE OR REPLACE FUNCTION pg_raft_catchup()
    RETURNS bigint LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_catchup';

COMMENT ON FUNCTION pg_raft_catchup() IS
    '后台追平通道：对本节点为 leader 的每个组，把落后成员按 nextIndex 逐条补齐，返回补发条数。'
    '只补发已存在的条目，不产生新提案，提交点仍按多数派推进。必须在 client backend 里跑'
    '（要 SPI 读 parwal 字节与环外条目），由 TopologyMonitor 按 pg_raft.catchup_interval_ms 自连触发。';

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
    DELETE FROM partdist.raft_log_runs;
    n := partdist.pg_raft_group_reset_internal();
    RETURN n;
END;
$fn$;

COMMENT ON FUNCTION pg_raft_group_reset() IS
    '清空本节点全部数据面 Raft 组(shmem 状态 + HardState 文件 + 注册表 + 日志);控制面 group 0 不受影响。';
