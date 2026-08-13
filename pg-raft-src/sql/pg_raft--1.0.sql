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
    p_participants bigint[] DEFAULT NULL,
    p_commit_ts bigint DEFAULT 0
) RETURNS integer LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_dtx_decide';

COMMENT ON FUNCTION dtx_decide(bigint, bigint, integer, bigint[], bigint) IS
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
    DELETE FROM partdist.raft_group WHERE group_id = p_group_id;
    DELETE FROM partdist.raft_log   WHERE group_id = p_group_id;
    RETURN partdist.pg_raft_group_drop_internal(p_group_id);
END;
$fn$;

COMMENT ON FUNCTION pg_raft_group_create(bigint, integer[]) IS
    '创建一个数据面 Raft 组（group_id 建议取 Citus shardid）；members 为空表示全体 peers。';
CREATE OR REPLACE FUNCTION pg_raft_group_flow_stats()
    RETURNS TABLE(
        group_id        bigint,
        ring_depth      bigint,   -- last_log_index - last_applied
        ring_capacity   integer,
        ring_full_waits bigint,   -- 因环满而背压等待的次数（>0 正常）
        ring_full_drops bigint,   -- 背压超时后被丢弃的提案数（>0 = 可能已分叉）
        quorum_drops    bigint,   -- 多数派不足被丢弃的条目数（>0 = 可能已分叉）
        last_drop_plsn  bigint    -- 最近一次被丢弃的数据条目 partition_lsn
    ) LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_raft_group_flow_stats';

COMMENT ON FUNCTION pg_raft_group_flow_stats() IS
'每个 Raft 组的背压/丢弃计数（FRD §13 约束 13）。ring_full_drops 或 quorum_drops '
'非零意味着有提案被丢弃 —— 数据组上这等于副本可能与 leader 永久分叉（leader 的'
'物理变更如 VACUUM 尾部截断不随事务回滚），需要重做物理基线。';

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
    n := partdist.pg_raft_group_reset_internal();
    RETURN n;
END;
$fn$;

COMMENT ON FUNCTION pg_raft_group_reset() IS
    '清空本节点全部数据面 Raft 组(shmem 状态 + HardState 文件 + 注册表 + 日志);控制面 group 0 不受影响。';

-- ------------------------------------------------------------------
-- 升主前置：与惰性回放 promotion 路径合流（DTX_2PC_DESIGN.md §0.0 第 6 步 a）
-- ------------------------------------------------------------------
--
-- 数据组自治选举胜出之后、**向控制面上报之前**调用（调用点在
-- raft_consensus.c 的 data_group_try_report）。返回 false 表示"还不能上报"，
-- 调用方保留 report_pending、下个 tick 重试。
--
-- 为什么放在上报之前而不是 group 0 的 apply 里：
--   a) apply 里做追平会**阻塞整个控制面** —— 惰性回放平时一条 redo 都不做，
--      升主时的积压可能很大，group 0 是串行 apply 的，一卡全卡；
--   b) 放在上报前，"追不平就不上报"天然等价于"追不平就不翻 pg_dist_placement"，
--      而路由正是在 apply 里紧跟着登记翻的（raft_apply.c）。顺序天生正确。
--
-- 每次调用只推进最多 p_timeout_ms 毫秒（BGW tick 还要发心跳，不能久占），
-- 没追平就返回 false 等下一 tick 继续 —— 分片切成小片，心跳不受影响。
CREATE OR REPLACE FUNCTION pg_raft_promote_prepare(
    p_group_id BIGINT,
    p_timeout_ms INTEGER DEFAULT 2000)
    RETURNS INTEGER
    LANGUAGE plpgsql VOLATILE
AS $promo$
DECLARE
    loid    OID;
    is_armed BOOLEAN;
    app     BIGINT;
    bound   BIGINT;
    got     BIGINT;
    ndiv    BIGINT;
BEGIN
    -- 返回值三态：1 = 可以上报；0 = 还没好，稍后重试；**-1 = 分叉，永不上报**。
    -- 三态而不是布尔，是因为"还没追平"可以被 deadline 兜底放行（可用性优先），
    -- 而"检测到分叉"绝不能放行 —— 放行等于让一个已知与组分叉的副本当上主库。
    loid := partdist.local_partition_for_shard(p_group_id);
    IF loid IS NULL OR loid = 0 THEN
        RETURN 1;               -- 本节点没有该分片，无事可做
    END IF;

    -- ★ 第 6 步 b（§9.5）：快路径分叉的归队规则。
    --
    -- 必须排在 armed 判断**之前**：分叉的旧 leader 上，该分片是真表而不是
    -- 副本壳表，多半压根没有回放槽位 —— 若先判 armed 就会直接 RETURN 1 放行，
    -- 分叉检查形同虚设。
    ndiv := partdist.pg_raft_check_fastpath_divergence(loid);
    IF ndiv > 0 THEN
        RAISE WARNING 'pg_raft: 分片 % (组 %) 检测到快路径分叉：段流里有 % 条本节点写的 '
                      'COMMIT 标记，其事务在本地 CLOG 却不是 committed。'
                      '本节点拒绝升主，该分片须重做物理基线后才能重新参选。',
                      loid, p_group_id, ndiv;
        -- 顺手把回放槽位下电（若有），避免它以"看似正常"的姿态继续参与。
        BEGIN
            PERFORM partdist.replay_disable(loid::regclass);
        EXCEPTION WHEN OTHERS THEN
            NULL;               -- 没有槽位/已下电：无所谓
        END;
        RETURN -1;
    END IF;

    SELECT s.armed, s.applied INTO is_armed, app
      FROM partdist.replay_status() s WHERE s.shard = loid;
    IF NOT FOUND OR is_armed IS NOT TRUE THEN
        -- 本节点没有该分片的副本回放配置（没 replay_set_locmap / 没 arm）。
        -- 这不是错误：它可能一直就是该组的 leader，或副本回放尚未启用。
        -- 分叉检查已在上面做过，这里放行是安全的。
        RETURN 1;
    END IF;

    -- 追平上界取 Raft 已提交位点，绝不碰未提交条目（惰性回放的核心不变式）。
    bound := partdist.get_follower_applied_part_lsn(loid);
    IF bound IS NULL THEN bound := 0; END IF;

    IF bound > app THEN
        BEGIN
            got := partdist.replay_catchup(loid::regclass, bound, p_timeout_ms);
        EXCEPTION WHEN OTHERS THEN
            RAISE WARNING 'pg_raft: 升主追平 shard % (组 %) 失败: %',
                          loid, p_group_id, SQLERRM;
            RETURN 0;
        END;
        IF got IS NULL OR got < bound THEN
            RETURN 0;           -- 这一片没追完，下个 tick 接着追
        END IF;
    END IF;

    -- 追平之后才闭合 in-doubt：判决要按已回放到位的流来求，顺序不能反。
    -- 尽力而为 —— 闭合失败不该把一个已经追平的副本挡在升主之外，
    -- 恢复守护（dtx_recover_prepared）随后仍会周期性重试。
    BEGIN
        PERFORM partdist.dtx_close_indoubt(loid::oid);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'pg_raft: 升主闭合 in-doubt shard % (组 %) 失败: %',
                      loid, p_group_id, SQLERRM;
    END;

    RETURN 1;
END
$promo$;

COMMENT ON FUNCTION pg_raft_promote_prepare(BIGINT, INTEGER) IS
    '升主前置：先查快路径分叉，再把本节点该分片的物理回放追平到 Raft 已提交位点，最后闭合 in-doubt 分布式事务。返回 1=可上报，0=尚未就绪（可重试，超时后按可用性优先放行），-1=检测到分叉（永不放行，须重做物理基线）。';

-- ------------------------------------------------------------------
-- 快路径分叉检测（DTX_2PC_DESIGN.md §9.5，第 6 步 b）
-- ------------------------------------------------------------------
--
-- **要检测的窗口**：单分区（快路径）事务沿用 `[A] → quorum → [B]` 时序 ——
-- 记录与 COMMIT 标记先达组内多数派，leader 本地的 [B]（pg_wal 提交记录 fsync）
-- 之后才发生。leader 在这两步之间崩溃 ⇒ **组内认为已提交、leader 本地事务却
-- 中止** ⇒ 旧 leader 与自己的组分叉。
--
-- **怎么检测**：段流里的 COMMIT 标记是"本组认为这笔事务提交了"的凭据，
-- 而本地 CLOG 是"本节点认为它提交了没有"的凭据。两者对同一个 xid 给出相反
-- 答案，就是分叉的**直接证据**，不需要比对页面。
--
-- 只查**本节点写的**标记（gxid 高 16 位 = 本节点 Citus group id）：别的节点
-- 写的标记，其 xid 在本地 CLOG 里根本没有意义。
--
-- **从流尾往回扫，遇到第一个"本地确实提交了"的标记就停**：那笔事务的 [B]
-- 已经完成，比它更早的事务不可能停在 [A]-[B] 之间。所以扫描量与分叉深度同阶，
-- 正常情况下第一条就停。p_max_scan 只是防御性上限。
CREATE OR REPLACE FUNCTION pg_raft_check_fastpath_divergence(
    p_loid OID,
    p_max_scan INTEGER DEFAULT 200)
    RETURNS BIGINT
    LANGUAGE plpgsql STABLE
AS $div$
DECLARE
    tip      BIGINT;
    self_gid INTEGER;
    i        BIGINT;
    scanned  INTEGER := 0;
    ndiv     BIGINT  := 0;
    rec      RECORD;
    nid      INTEGER;
    lo       BIGINT;
    st       TEXT;
    cur_full BIGINT;
    full_xid BIGINT;
BEGIN
    SELECT groupid INTO self_gid FROM pg_dist_local_group;
    IF self_gid IS NULL THEN RETURN 0; END IF;

    -- pg_xact_status 接的是 **xid8**（64 位 full xid），而 gxid 低位存的是
    -- 32 位 TransactionId。要按当前 epoch 还原，不能硬转：epoch 边界上
    -- 硬转会查到隔了一整个 epoch 的另一笔事务的判决。
    cur_full := pg_current_xact_id()::text::bigint;

    tip := partdist.get_partition_flush_lsn(p_loid);
    IF tip IS NULL OR tip <= 0 THEN RETURN 0; END IF;

    i := tip;
    WHILE i >= 1 AND scanned < p_max_scan LOOP
        scanned := scanned + 1;
        BEGIN
            SELECT r.flags, r.info, r.gxid INTO rec
              FROM partdist.partwal_read_record(p_loid, i) r;
        EXCEPTION WHEN OTHERS THEN
            EXIT;                       -- 读不出来（截断/损坏）：不再往前
        END;

        -- flags=2 MARKER, info=0 XLOG_XACT_COMMIT
        IF rec.flags = 2 AND rec.info = 0 THEN
            nid := (rec.gxid >> 48)::int;
            lo  := rec.gxid & ((1::bigint << 48) - 1);
            IF nid = self_gid AND lo > 0 THEN
                full_xid := cur_full - (cur_full % 4294967296) + lo;
                IF full_xid > cur_full THEN
                    full_xid := full_xid - 4294967296;   -- 属上一个 epoch
                END IF;
                BEGIN
                    st := pg_xact_status(full_xid::text::xid8);
                EXCEPTION WHEN OTHERS THEN
                    st := NULL;         -- xid 已过 CLOG 截断点：无从判定，跳过
                END;
                IF st IS NULL THEN
                    NULL;
                ELSIF st = 'committed' THEN
                    EXIT;               -- [B] 已完成，更早的不可能分叉
                ELSE
                    ndiv := ndiv + 1;   -- 流说提交、本地说没有 ⇒ 分叉
                END IF;
            END IF;
        END IF;
        i := i - 1;
    END LOOP;

    RETURN ndiv;
END
$div$;

COMMENT ON FUNCTION pg_raft_check_fastpath_divergence(OID, INTEGER) IS
    '检测快路径分叉：段流里本节点写的 COMMIT 标记，其 xid 在本地 CLOG 却不是 committed。返回分叉条数，0 表示无分叉。';
