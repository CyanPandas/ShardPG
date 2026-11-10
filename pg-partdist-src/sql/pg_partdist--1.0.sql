-- pg_partdist 1.0 installation script
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_partdist" to load this file. \quit

-- Note: PostgreSQL sets search_path to the extension schema ('partdist') for
-- the duration of this script when 'schema = partdist' is in the control file.
-- All unqualified CREATE statements therefore land in the partdist schema.

-- ----------------------------------------------------------------
-- Core metadata tables
-- ----------------------------------------------------------------

-- partition_map: maps each partition OID to its primary node and
-- an array of secondary node IDs.
CREATE TABLE partition_map (
    partition_id    OID         NOT NULL,
    primary_node    INTEGER     NOT NULL,
    secondary_nodes INTEGER[]   NOT NULL DEFAULT '{}',
    version         BIGINT      NOT NULL DEFAULT 1,
    updated_at      TIMESTAMPTZ          DEFAULT now(),
    -- 数据组自治选举出的主副本任期（任期栅栏：apply 只接受不回退的更新）。
    -- 0 = 尚无数据组管理（历史/合成分区，仍走旧"控制面指定"通道）。
    primary_term    BIGINT      NOT NULL DEFAULT 0,
    -- TX-TSO-MVCC（T2.7）：本分区是否启用分片级 xid 打标（P2 起）。
    -- 由 partdist_set_shard_mvcc() 翻转；既有行默认 false 保 438 基线。
    shard_mvcc      BOOLEAN     NOT NULL DEFAULT false,
    CONSTRAINT pk_partition_map PRIMARY KEY (partition_id)
);

COMMENT ON TABLE partition_map IS
    'Maps partition OIDs to their primary/secondary node assignments.';
COMMENT ON COLUMN partition_map.primary_term IS
    '分区 raft 组自治选举的主副本任期；0 表示尚未由数据组接管。迟到/重复的登记被 apply 的任期栅栏拦下。';

-- node_map: tracks every node in the pg_partdist cluster.
CREATE TABLE node_map (
    node_id        INTEGER     NOT NULL,
    hostname       TEXT        NOT NULL,
    port           INTEGER     NOT NULL CHECK (port BETWEEN 1 AND 65535),
    status         TEXT        NOT NULL DEFAULT 'active'
                               CHECK (status IN ('active', 'down', 'syncing')),
    last_heartbeat TIMESTAMPTZ,
    CONSTRAINT pk_node_map PRIMARY KEY (node_id)
);

COMMENT ON TABLE node_map IS
    'Tracks every node participating in the pg_partdist cluster.';

-- ----------------------------------------------------------------
-- Trigger: auto-bump version on partition_map changes
-- ----------------------------------------------------------------

CREATE OR REPLACE FUNCTION partition_map_version_bump()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.version    := OLD.version + 1;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_partition_map_version
    BEFORE UPDATE ON partition_map
    FOR EACH ROW
    EXECUTE FUNCTION partition_map_version_bump();

-- ----------------------------------------------------------------
-- C-backed functions (MODULE_PATHNAME = pg_partdist.so)
-- ----------------------------------------------------------------

CREATE OR REPLACE FUNCTION pg_partdist_version()
    RETURNS text
    LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_version';

COMMENT ON FUNCTION pg_partdist_version() IS
    'Returns the pg_partdist extension version string.';

-- Look up the primary node for a partition (hits the shared-memory cache).
CREATE OR REPLACE FUNCTION pg_partdist_get_primary(partition_id OID)
    RETURNS integer
    LANGUAGE c CALLED ON NULL INPUT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_get_primary';

COMMENT ON FUNCTION pg_partdist_get_primary(OID) IS
    'Returns the primary node ID for the given partition OID, or NULL if not registered.';

-- Invalidate the entire shared-memory cache by bumping the generation counter.
CREATE OR REPLACE FUNCTION pg_partdist_cache_invalidate()
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_cache_invalidate';

COMMENT ON FUNCTION pg_partdist_cache_invalidate() IS
    'Invalidates the pg_partdist shared-memory metadata cache.';

-- Manually bump the metadata generation (useful in tests or after bulk loads).
CREATE OR REPLACE FUNCTION pg_partdist_bump_metadata_version()
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_bump_metadata_version';

COMMENT ON FUNCTION pg_partdist_bump_metadata_version() IS
    'Increments the global metadata generation counter, forcing cache re-population.';

-- Return cache statistics from shared memory.
CREATE OR REPLACE FUNCTION pg_partdist_cache_stats(
    OUT partition_hits      BIGINT,
    OUT partition_misses    BIGINT,
    OUT node_hits           BIGINT,
    OUT node_misses         BIGINT,
    OUT metadata_generation BIGINT
)
    RETURNS record
    LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_cache_stats';

COMMENT ON FUNCTION pg_partdist_cache_stats() IS
    'Returns cache hit/miss counters and the current metadata generation.';

-- Evaluate the routing decision for a write to partition_id.
CREATE OR REPLACE FUNCTION pg_partdist_route_write(partition_id OID)
    RETURNS text
    LANGUAGE c CALLED ON NULL INPUT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_route_write';

COMMENT ON FUNCTION pg_partdist_route_write(OID) IS
    'Returns the routing decision for a write to the given partition: '
    'local | remote | not_found | node_down.';

-- ----------------------------------------------------------------
-- Milestone 2.1 — partition WAL directory management & record format
-- ----------------------------------------------------------------

-- Create pg_parwal/<partition_id>/ directory under DataDir.
CREATE OR REPLACE FUNCTION init_partition_wal(partition_id OID)
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_init_partition_wal';

COMMENT ON FUNCTION init_partition_wal(OID) IS
    'Create pg_parwal/<partition_id>/ under DataDir (idempotent).';

-- Return true if pg_parwal/<partition_id>/ exists and is a directory.
CREATE OR REPLACE FUNCTION partition_wal_exists(partition_id OID)
    RETURNS boolean
    LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_partition_wal_exists';

COMMENT ON FUNCTION partition_wal_exists(OID) IS
    'Return true if the pg_parwal directory for partition_id exists.';

-- Return the relative path of the WAL segment file for a given segment number.
CREATE OR REPLACE FUNCTION partition_wal_path(partition_id OID, segno BIGINT)
    RETURNS text
    LANGUAGE c STRICT IMMUTABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_partition_wal_path';

COMMENT ON FUNCTION partition_wal_path(OID, BIGINT) IS
    'Return pg_parwal/<partition_id>/<segname> for the given segment number.';

-- Remove segment files in pg_parwal/<partition_id>/ below keep_lsn.
CREATE OR REPLACE FUNCTION cleanup_partition_wal(partition_id OID, keep_lsn PG_LSN)
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_cleanup_partition_wal';

COMMENT ON FUNCTION cleanup_partition_wal(OID, PG_LSN) IS
    'Remove pg_parwal segment files whose end-LSN is at or below keep_lsn.';

-- Allocate the next per-partition WAL sequence number (for testing/inspection).
CREATE OR REPLACE FUNCTION alloc_partition_lsn(partition_id OID)
    RETURNS bigint
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_alloc_partition_lsn';

COMMENT ON FUNCTION alloc_partition_lsn(OID) IS
    'Return and increment the per-partition WAL LSN counter (1-based).';

-- Write a PartWALHeader record to the WAL and to pg_parwal/<partition_id>/.
-- Returns the global WAL LSN of the written record.
CREATE OR REPLACE FUNCTION write_partition_wal_record(
    partition_id OID,
    flags        INTEGER DEFAULT 1
)
    RETURNS pg_lsn
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_write_partition_wal_record';

COMMENT ON FUNCTION write_partition_wal_record(OID, INTEGER) IS
    'Write a PartWALHeader WAL record for partition_id; return the assigned LSN.';

-- Scan pg_parwal/<partition_id>/ and return all PartWALHeader records.
CREATE OR REPLACE FUNCTION check_partition_wal(partition_id OID)
    RETURNS TABLE(
        partition_lsn   BIGINT,
        orig_node_lsn   PG_LSN,
        flags           INTEGER,
        is_valid        BOOLEAN
    )
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_check_partition_wal';

COMMENT ON FUNCTION check_partition_wal(OID) IS
    'Return all PartWALHeader records from pg_parwal/<partition_id>/.';

-- Verify that partition_lsn is strictly monotone and all records have valid magic.
CREATE OR REPLACE FUNCTION verify_partition_wal(partition_id OID)
    RETURNS boolean
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_verify_partition_wal';

COMMENT ON FUNCTION verify_partition_wal(OID) IS
    'Return true if all pg_parwal records for partition_id are internally consistent.';

-- Reset pg_parwal files + shmem LSN counter for one partition (regression tests only).
CREATE OR REPLACE FUNCTION reset_partition_wal_state(partition_id OID)
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_reset_partition_wal_state';

COMMENT ON FUNCTION reset_partition_wal_state(OID) IS
    'Delete all pg_parwal segment files for partition_id and reset its LSN counter to 0.';

-- ----------------------------------------------------------------
-- Milestone 2.2 — Demux Worker and auxiliary functions
-- ----------------------------------------------------------------

-- Count all PartWALHeader records stored in pg_parwal/<partition_id>/.
CREATE OR REPLACE FUNCTION count_parwal_records(partition_id OID)
    RETURNS bigint
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_count_parwal_records';

COMMENT ON FUNCTION count_parwal_records(OID) IS
    'Return the total number of PartWALHeader records on disk for the given partition.';

-- Returns true once the one-shot crash-recovery BGW has finished.
-- Test scripts poll this instead of checking ps (BGW exits after recovery).
CREATE OR REPLACE FUNCTION demux_is_ready()
    RETURNS boolean
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_demux_is_ready';

COMMENT ON FUNCTION demux_is_ready() IS
    'Returns true once the crash-recovery background worker has completed and the synchronous write path is active.';

-- Read Demux Worker progress from shared memory.
CREATE OR REPLACE FUNCTION demux_progress(
    OUT node_name          TEXT,
    OUT last_processed_lsn PG_LSN
)
    RETURNS record
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_demux_progress';

COMMENT ON FUNCTION demux_progress() IS
    'Return the last WAL LSN processed by the Demux Worker on this node.';

-- Read rolling processing-latency statistics from the Demux Worker.
CREATE OR REPLACE FUNCTION demux_latency_stats(
    OUT p50_ms FLOAT,
    OUT p99_ms FLOAT,
    OUT avg_ms FLOAT
)
    RETURNS record
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_demux_latency_stats';

COMMENT ON FUNCTION demux_latency_stats() IS
    'Return p50/p99/avg Demux processing latency in milliseconds (NULL if no samples).';

-- Scan all pg_parwal/<partition_id>/ records and return partition_lsn + orig_node_lsn.
CREATE OR REPLACE FUNCTION read_all_headers(partition_id OID)
    RETURNS TABLE(
        partition_lsn   BIGINT,
        orig_node_lsn   PG_LSN
    )
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_read_all_headers';

COMMENT ON FUNCTION read_all_headers(OID) IS
    'Return (partition_lsn, orig_node_lsn) for every record in pg_parwal/<partition_id>/.';

-- Block until the Demux Worker has processed all WAL to the current position.
-- Regression tests that write records and immediately read back pg_parwal content
-- must call this to ensure the Demux Worker has had time to process.
CREATE OR REPLACE FUNCTION demux_flush()
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_demux_flush';

COMMENT ON FUNCTION demux_flush() IS
    'Block until the Demux Worker has processed WAL to the current flush LSN (30 s timeout).';

-- ----------------------------------------------------------------
-- Follower replay support
-- ----------------------------------------------------------------

-- follower_partition_map: tracks the mapping from logical partition_id
-- (the primary's OID, used as directory name in pg_parwal/) to the
-- follower's local table name, plus replay progress.
--
-- applied_part_lsn is updated atomically with each SPI apply batch so
-- that a follower crash during replay is safe to resume from this offset.
CREATE TABLE IF NOT EXISTS follower_partition_map (
    partition_id      OID     NOT NULL,
    local_relname     TEXT    NOT NULL,
    applied_part_lsn  BIGINT  NOT NULL DEFAULT 0,
    CONSTRAINT pk_follower_partition_map PRIMARY KEY (partition_id)
);

COMMENT ON TABLE follower_partition_map IS
    'Maps primary partition OIDs to local table names for follower lazy replay. '
    'applied_part_lsn records the last successfully committed PartWAL record.';

-- ----------------------------------------------------------------
-- P0: Global shard identity (阶段 4 / §11 前置)
--
-- 全局分片身份:以 Citus 的 shardid 作为跨节点一致的 global_shard_id
-- (分片表命名为 <rel>_<shardid>,pg_dist_shard 在所有节点同步),建立
-- global_shard_id <-> 本节点 (local_oid, relfilenode, relname) 的映射。
-- 目的:让"一个分区 = 一个 Raft 组"的成员寻址,以及跨节点
-- applied_part_lsn 比较,拥有一个良定义、各节点一致的键。
-- global_shard_id 在承载同一逻辑分片副本的每个节点上都相同;
-- local_oid 是每节点独立分配的,即 pg_parwal/<oid>/ 的目录名 (== partition_id)。
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shard_identity (
    global_shard_id  BIGINT      NOT NULL,   -- Citus shardid, 集群全局唯一
    local_oid        OID         NOT NULL,   -- 本节点分片表 OID (== partition_id)
    relfilenode      OID         NOT NULL,   -- 本节点分片表 relfilenode
    local_relname    TEXT        NOT NULL,   -- 本节点分片表名 <rel>_<shardid>
    logical_relid    OID,                    -- 本节点分布表(逻辑表)OID
    registered_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_shard_identity PRIMARY KEY (global_shard_id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_shard_identity_local_oid
    ON shard_identity(local_oid);

COMMENT ON TABLE shard_identity IS
    'Maps the cluster-global Citus shardid (global_shard_id) to this node''s local '
    'shard-table identity (local_oid/relfilenode/relname). global_shard_id is identical '
    'on every node hosting a replica of the same logical shard; local_oid is node-specific. '
    'P0 prerequisite for per-partition Raft groups and cross-node applied_part_lsn comparison.';

-- follower_partition_map 增加 global_shard_id 维度(可空,由 rebuild_shard_identity 回填)。
ALTER TABLE follower_partition_map
    ADD COLUMN IF NOT EXISTS global_shard_id BIGINT;

-- 从系统目录权威解析某分片表 OID 的全局 shardid(非本节点 Citus 分片返回 NULL)。
-- 不解析表名后缀,直接用 Citus shard_name() 反查,避免命名边界问题。
-- 用 plpgsql:LANGUAGE sql 会在 CREATE 时对函数体做计划分析,而 Citus 在 worker 上
-- 会对涉及 pg_dist_shard/shard_name 的计划报 "operation is not allowed on this node";
-- 该查询在 worker 上运行时是允许的,plpgsql 体不在建函数时被计划,故可安全创建。
CREATE OR REPLACE FUNCTION shard_global_id(p_local_oid OID)
    RETURNS BIGINT
    LANGUAGE plpgsql STABLE
    SET search_path = partdist, pg_catalog, public
AS $$
DECLARE
    gid BIGINT;
BEGIN
    SELECT s.shardid INTO gid
    FROM pg_catalog.pg_dist_shard s
    WHERE pg_catalog.to_regclass(pg_catalog.shard_name(s.logicalrelid, s.shardid))::oid
          = p_local_oid
    LIMIT 1;
    RETURN gid;
END;
$$;

COMMENT ON FUNCTION shard_global_id(OID) IS
    'Resolve the cluster-global Citus shardid for a local shard-table OID, or NULL.';

-- 扫描本节点所有物理存在的 Citus 分片表,(重新)填充 shard_identity,
-- 并回填 follower_partition_map.global_shard_id。返回写入/更新的行数。
-- 幂等:可反复调用。to_regclass(shard_name(...)) 仅对本节点物理存在的分片非空,
-- 因此协调节点(无分片副本)执行时通常写入 0 行。
-- override_table_visibility=false:Citus 默认对普通连接的 pg_class 扫描隐藏分片表,
-- 隐藏后 JOIN pg_class 取不到分片行(to_regclass 不受影响但拿不到 relfilenode/relname)。
CREATE OR REPLACE FUNCTION rebuild_shard_identity()
    RETURNS INTEGER
    LANGUAGE plpgsql VOLATILE
    SET search_path = partdist, pg_catalog, public
    SET citus.override_table_visibility = 'false'
AS $$
DECLARE
    n INTEGER := 0;
BEGIN
    INSERT INTO shard_identity
        (global_shard_id, local_oid, relfilenode, local_relname, logical_relid)
    SELECT s.shardid,
           cls.oid,
           cls.relfilenode,
           cls.relname,
           s.logicalrelid
    -- T4.5 加固 v2：悬空的 pg_dist_shard 行（logicalrelid 已被删）会让
    -- shard_name() 直接抛错，整个身份重建报废。过滤必须**先于** JOIN 的
    -- shard_name 求值——SQL 不保证 WHERE 先于 JOIN ON，故用 OFFSET 0
    -- 优化栅栏把过滤钉死在子查询里（v1 只加 WHERE，实测 planner 仍先
    -- 求值 shard_name 而炸，worker 上身份重建整体报废）。
    FROM (SELECT * FROM pg_catalog.pg_dist_shard ds
           WHERE ds.logicalrelid::oid IN (SELECT oid FROM pg_catalog.pg_class)
           OFFSET 0) s
    JOIN pg_catalog.pg_class cls
      ON cls.oid = pg_catalog.to_regclass(
                       pg_catalog.shard_name(s.logicalrelid, s.shardid))::oid
    ON CONFLICT (global_shard_id) DO UPDATE
       SET local_oid     = EXCLUDED.local_oid,
           relfilenode   = EXCLUDED.relfilenode,
           local_relname = EXCLUDED.local_relname,
           logical_relid = EXCLUDED.logical_relid,
           registered_at = now();
    GET DIAGNOSTICS n = ROW_COUNT;

    -- 清理已不在本节点的分片(表被删/迁走);to_regclass 不受分片隐藏影响。
    --
    -- ★ 2026-08-18：本条**漏了 INSERT 分支同款的悬空过滤**，实测因此报废。
    -- 悬空的 pg_dist_shard 行（logicalrelid 指向的逻辑表已被删）会让
    -- shard_name() 抛 "object_name does not reference a valid relation"，
    -- 而它在 NOT EXISTS 子查询里对**每一行** pg_dist_shard 求值 ⇒ 整个
    -- rebuild_shard_identity() 报废 ⇒ shard_identity 建不出来 ⇒
    -- local_partition_for_shard() 返回空 ⇒ 下游连锁（实测 pagecmp_p1 7 条、
    -- dtx_convergence_p4 11 条同源失败）。
    -- 修法与 INSERT 分支一致：先用 OFFSET 0 优化栅栏把悬空行滤掉，再让
    -- shard_name 求值。**只加 WHERE 不够** —— planner 仍可能先求值 shard_name
    -- （INSERT 分支的 v1 就栽在这里，注释已记）。
    DELETE FROM shard_identity si
     WHERE NOT EXISTS (
         SELECT 1
           FROM (SELECT * FROM pg_catalog.pg_dist_shard ds
                  WHERE ds.logicalrelid::oid IN (SELECT oid FROM pg_catalog.pg_class)
                  OFFSET 0) s
          WHERE pg_catalog.to_regclass(
                    pg_catalog.shard_name(s.logicalrelid, s.shardid))::oid = si.local_oid);

    -- 回填 follower 进度表的全局键
    UPDATE follower_partition_map f
       SET global_shard_id = si.global_shard_id
      FROM shard_identity si
     WHERE si.local_oid = f.partition_id
       AND f.global_shard_id IS DISTINCT FROM si.global_shard_id;

    RETURN n;
END;
$$;

COMMENT ON FUNCTION rebuild_shard_identity() IS
    'Scan locally-present Citus shard tables and (re)populate shard_identity; '
    'backfill follower_partition_map.global_shard_id. Idempotent. Returns rows written.';

-- 注册单个分片(供分片创建路径 / 按需调用);非本节点 Citus 分片返回 NULL。
CREATE OR REPLACE FUNCTION register_shard_identity(p_local_oid OID)
    RETURNS BIGINT
    LANGUAGE plpgsql VOLATILE
    SET search_path = partdist, pg_catalog, public
    SET citus.override_table_visibility = 'false'
AS $$
DECLARE
    gid BIGINT;
BEGIN
    gid := partdist.shard_global_id(p_local_oid);
    IF gid IS NULL THEN
        RETURN NULL;
    END IF;

    INSERT INTO shard_identity
        (global_shard_id, local_oid, relfilenode, local_relname, logical_relid)
    SELECT gid, cls.oid, cls.relfilenode, cls.relname,
           (SELECT s.logicalrelid FROM pg_catalog.pg_dist_shard s
             WHERE s.shardid = gid LIMIT 1)
    FROM pg_catalog.pg_class cls
    WHERE cls.oid = p_local_oid
    ON CONFLICT (global_shard_id) DO UPDATE
       SET local_oid     = EXCLUDED.local_oid,
           relfilenode   = EXCLUDED.relfilenode,
           local_relname = EXCLUDED.local_relname,
           logical_relid = EXCLUDED.logical_relid,
           registered_at = now();
    RETURN gid;
END;
$$;

COMMENT ON FUNCTION register_shard_identity(OID) IS
    'Register/refresh one local shard in shard_identity; returns its global shardid or NULL.';

-- Raft 控制面用:全局分片 id <-> 本节点 partition_id(OID)互查。
-- 本节点不承载该分片时返回 NULL/0,调用方据此判断"本地是否有此分区副本"。
CREATE OR REPLACE FUNCTION local_partition_for_shard(p_global_shard_id BIGINT)
    RETURNS OID
    LANGUAGE sql STABLE
AS $$
    SELECT local_oid FROM partdist.shard_identity WHERE global_shard_id = p_global_shard_id
$$;

COMMENT ON FUNCTION local_partition_for_shard(BIGINT) IS
    'This node''s local partition_id (OID) for a global shardid, or NULL if not hosted here.';

CREATE OR REPLACE FUNCTION global_id_for_partition(p_local_oid OID)
    RETURNS BIGINT
    LANGUAGE sql STABLE
AS $$
    SELECT global_shard_id FROM partdist.shard_identity WHERE local_oid = p_local_oid
$$;

COMMENT ON FUNCTION global_id_for_partition(OID) IS
    'The global Citus shardid for a local partition_id (OID) from shard_identity, or NULL.';

-- ----------------------------------------------------------------
-- Raft control-plane boundary functions (consumed by pg_raft)
-- ----------------------------------------------------------------

-- T5.1（设计 §6.1）：分片级 vacuum 两水位的读写。观测/维护用。
CREATE OR REPLACE FUNCTION shard_vacuum_watermarks(
    p_shard OID,
    OUT clog_truncate_before BIGINT,
    OUT shard_vacuum_xid BIGINT
) RETURNS record LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'partdist_shard_vacuum_watermarks';

COMMENT ON FUNCTION shard_vacuum_watermarks(OID) IS
    'T5.1：读本分片的 vacuum 两水位。clog_truncate_before = clog 实际截断点（隐式 freeze 点、回卷龄基点）；shard_vacuum_xid = 两态恢复标记（== 前者表示无未完成的趟，> 前者表示页面趟已完成、截断待补）。';

CREATE OR REPLACE FUNCTION shard_vacuum_set_watermarks(
    p_shard OID, p_trunc_before BIGINT, p_vacuum_xid BIGINT)
RETURNS void LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_vacuum_set_watermarks';

COMMENT ON FUNCTION shard_vacuum_set_watermarks(OID, BIGINT, BIGINT) IS
    'T5.1：写本分片的 vacuum 两水位并落盘。不变式 clog_truncate_before <= shard_vacuum_xid 在落盘出口强制，违反即 ERROR。';

-- T5.2（设计 §6.3）：前缀扫描算 VacuumTargetXid。从本分片当前的
-- clog_truncate_before 起顺扫至 p_ceiling（不含），返回可安全截断到的**前一条**；
-- 0 = 一条都不能清。stop_reason 回填停因供排障。
CREATE OR REPLACE FUNCTION shard_vacuum_target(
    p_shard OID, p_safe_ts BIGINT, p_ceiling BIGINT,
    OUT target_xid BIGINT,
    OUT stop_reason TEXT
) RETURNS record LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'partdist_shard_vacuum_target';

COMMENT ON FUNCTION shard_vacuum_target(OID, BIGINT, BIGINT) IS
    'T5.2：前缀扫描算 VacuumTargetXid（设计 §6.3）。COMMITTED 且 commit_ts < p_safe_ts 放行，**ABORTED 也放行**（挡住它会让一个中止事务永久钉死截断）；遇 RUNNING / PREPARED / 空洞 / commit_ts 太新即停。停因：scanned-to-ceiling / no-safe-ts / hole-running / commit-ts-too-new / prepared / running。';

-- T5.3a（设计 §6.4 ③）：xmax 消毒。把 xmax < p_trunc_before 且 ABORTED 或
-- lock-only 的 xmax 清成 0，其余原样。**只清不推进水位** —— 顺序铁律要求
-- 页面全部清完才许动 clog，推进由 T5.4 的截断路径统一落。
CREATE OR REPLACE FUNCTION shard_sanitize_xmax(
    p_rel REGCLASS, p_trunc_before BIGINT,
    OUT pages_scanned BIGINT,
    OUT pages_dirtied BIGINT,
    OUT tuples_sanitized BIGINT
) RETURNS record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_sanitize_xmax';

COMMENT ON FUNCTION shard_sanitize_xmax(REGCLASS, BIGINT) IS
    'T5.3a：分片表 xmax 消毒（设计 §6.4 ③）。p_trunc_before = 本轮打算推进到的截断点（开区间上界，= ShardVacuumComputeTarget 结果 + 1）。截断点以下的 ABORTED / lock-only xmax 清成 0，COMMITTED 不动（留给动作②）；区间内仍未决即 ERROR。页面修改经内核 heap_freeze_execute_prepared 发 XLOG_HEAP2_FREEZE_PAGE，follower 逐字节回放。';

-- T5.3b（设计 §6.4 ①）：删中止 xmin 的元组。不做则截断后该 xmin 落进免查
-- 隐式冻结区、被解释成"已提交全可见"，**中止事务的幽灵行复活**。
-- ★ 必须先跑 shard_sanitize_xmax（③）：中止事务"插入后又更新"留下的 HOT 链，
--   其 HEAP_HOT_UPDATED 位要靠 ③ 清 xmax 才失效；③ 没跑过的那些元组会计进
--   tuples_deferred 留到下一趟。
-- ★ 只对无索引的分片表可用（索引两阶段是 T5.3c）；带索引即 ERROR。
-- pages_skipped > 0 或 tuples_deferred > 0 ⇒ 本趟不完整，不得据此截断。
CREATE OR REPLACE FUNCTION shard_remove_aborted(
    p_rel REGCLASS, p_trunc_before BIGINT,
    OUT pages_scanned BIGINT,
    OUT pages_dirtied BIGINT,
    OUT pages_skipped BIGINT,
    OUT tuples_removed BIGINT,
    OUT tuples_deferred BIGINT
) RETURNS record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_remove_aborted';

COMMENT ON FUNCTION shard_remove_aborted(REGCLASS, BIGINT) IS
    'T5.3b：删中止 xmin 的元组（设计 §6.4 ①）。判据只看 xmin：clog ABORTED 且 xmin < p_trunc_before 即删。走原生无索引表通路——XLOG_HEAP2_PRUNE 置 LP_DEAD/LP_UNUSED，紧接 XLOG_HEAP2_VACUUM 置 LP_UNUSED 并截行指针数组，页面变换全交内核 heap_page_prune_execute。需先跑 shard_sanitize_xmax；带索引即 ERROR。';

-- T5.3c（设计 §6.4 ②）：删已提交删除的死元组。**判据看 xmax 不看 xmin** ——
-- 没被删过的老行是活的，零页面动作。commit_ts < GlobalSafeTs 由 p_trunc_before
-- 的构造保证（§6.3 只让满足该条件的 COMMITTED 过关）；反命题做成守卫：
-- commit_ts=0 的已提交条目落在截断点以下即 ERROR。
-- ★ 与 ① 不对称：本动作**不推迟**仍挂 HOT 链的 heap-only 元组——已提交的 xmax
--   没有后续动作会清它的 HEAP_HOT_UPDATED，推迟就是永远推迟，会把截断钉死。
-- ★ 索引两阶段尚未实现（带索引即 ERROR），见 DEV PLAN T5.3c 实施记要。
CREATE OR REPLACE FUNCTION shard_remove_dead(
    p_rel REGCLASS, p_trunc_before BIGINT,
    OUT pages_scanned BIGINT,
    OUT pages_dirtied BIGINT,
    OUT pages_skipped BIGINT,
    OUT tuples_removed BIGINT,
    OUT tuples_deferred BIGINT
) RETURNS record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_remove_dead';

COMMENT ON FUNCTION shard_remove_dead(REGCLASS, BIGINT) IS
    'T5.3c：删已提交删除的死元组（设计 §6.4 ②）。判据只看 xmax：clog COMMITTED 且 xmax < p_trunc_before 即删；xmax 为空/中止/lock-only 一律不动。与 shard_remove_aborted 共用同一条页面通路（PRUNE + VACUUM 两条内核记录），定义域互不相交。带索引即 ERROR（索引两阶段未实现）。';

-- VACU：先验证全局安全前缀，再按 ③→①→②→显式冻结 扫主堆及 TOAST。
-- **只有整趟干净**（pages_skipped=0 且 tuples_deferred=0）才落"趟完"标记
-- shard_vacuum_xid = p_trunc_before —— 这是 shard_clog_truncate 唯一认的凭据。
CREATE OR REPLACE FUNCTION shard_vacuum_sweep(
    p_rel REGCLASS, p_trunc_before BIGINT,
    OUT swept BOOLEAN,
    OUT sanitized BIGINT,
    OUT removed_aborted BIGINT,
    OUT removed_dead BIGINT,
    OUT pages_skipped BIGINT,
    OUT tuples_deferred BIGINT
) RETURNS record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_vacuum_sweep';

COMMENT ON FUNCTION shard_vacuum_sweep(REGCLASS, BIGINT) IS
    'VACU：GlobalSafeTs 校验通过后，主堆/TOAST 依次执行 xmax 消毒、删中止插入、删死元组、PG 显式 xmin 冻结。冻结保留原始 xmin，读取通过冻结标记跳过旧事务状态。页面 WAL 持久化及复制完成、无跳页/推迟后才落完成标记。';

-- T7.17（P7-V1）：分片 vacuum 自动启动器。
-- 把"到龄了该跑 vacuum"从一条 WARNING 变成真的会跑：逐个到龄分片走
-- 「两态恢复 → 算目标 → 趟页面 → 截断」，单个分片失败只让那一个跳过。
-- 判据与阶段 1 护栏同源（age >= shard_vacuum_max_age），所以不会出现
-- "警告了却不动手"或"没警告却在动手"的错位。
-- p_max_shards <= 0 表示不限（上限仍是 64 个槽位）。
CREATE OR REPLACE FUNCTION shard_vacuum_auto(
    p_max_shards INTEGER DEFAULT 0,
    OUT shards_considered INTEGER,
    OUT shards_swept INTEGER,
    OUT shards_truncated INTEGER,
    OUT detail TEXT
) RETURNS record LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_vacuum_auto';

COMMENT ON FUNCTION shard_vacuum_auto(INTEGER) IS
    'T7.17：分片 vacuum 自动启动器。心跳工作者按 pg_partdist.shard_vacuum_auto 自连触发，也可手工调用。detail 逐分片回填结果：<oid>:<旧截断点>-><新截断点> / recover / gone / not-swept(...) / <停因> / error。取不到 GlobalSafeTs 时一律停在 no-safe-ts —— 少清一轮无害，拿不可信的安全线截断 clog 不可逆。';

-- T5.4：截断本分片 clog。门禁 = 顺序铁律；内部次序 = 先推水位后删文件。
CREATE OR REPLACE FUNCTION shard_clog_truncate(p_shard OID, p_trunc_before BIGINT)
RETURNS INTEGER LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_clog_truncate';

COMMENT ON FUNCTION shard_clog_truncate(OID, BIGINT) IS
    'VACU：先同步复制水位 CTRL，再持久化本地回收边界，最后截断分片 clog 到 p_trunc_before（开区间上界；此后 xid < 该值进入免查隐式冻结区）。要求 p_trunc_before <= shard_vacuum_xid（顺序铁律：页没清完不许动 clog），否则 ERROR。先推水位后删文件——反过来一旦中途崩溃，clog 没了而水位还说要查 clog，已提交数据当场消失。按段整删，返回删掉的段文件数。';

-- T5.5（设计 §6.5）：两态恢复。
--   clog_truncate_before == shard_vacuum_xid ⇒ 无未完成的趟，返回 'nothing'
--       （趟中崩溃落在这一格：整趟重来即可，三类动作各自幂等）；
--   clog_truncate_before <  shard_vacuum_xid ⇒ 趟完未截断，**只补做截断**，
--       返回 'truncated'，绝不重跑页面趟。
CREATE OR REPLACE FUNCTION shard_vacuum_recover(p_shard OID)
RETURNS TEXT LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_vacuum_recover';

COMMENT ON FUNCTION shard_vacuum_recover(OID) IS
    'T5.5：分片 vacuum 的两态恢复（设计 §6.5）。两个水位相等=无未完成的趟（什么都不做）；shard_vacuum_xid 跑在前面=趟完未截断（只补做截断）。幂等：连做两次第二次必回 nothing。';

-- T5.6（设计 §7）：分片级回卷护栏的观测点。
-- 龄 = next_xid - clog_truncate_before。★ 基点是 clog_truncate_before 而**不是**
-- shard_vacuum_xid：歧义边界挂在免查区的解释规则上，而"趟完未截断"窗口里
-- shard_vacuum_xid 跑在前面，拿它算龄会把紧迫度算小，方向不安全。
-- phase 0=正常 / 1=到龄（须尽快 vacuum）/ 2=已停发（该分片进只读）。
CREATE OR REPLACE FUNCTION shard_xid_age(
    p_shard OID,
    OUT age BIGINT,
    OUT phase INTEGER,
    OUT max_age BIGINT,
    OUT stop_age BIGINT
) RETURNS record LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'partdist_shard_xid_age';

COMMENT ON FUNCTION shard_xid_age(OID) IS
    'T5.6：分片 xid 龄与回卷护栏相位（设计 §7）。阈值由 pg_partdist.shard_vacuum_max_age（阶段1，默认 2e8）与 pg_partdist.shard_xid_stop_age（阶段2，默认 2^31-1e6）控制；阶段 2 触发后该分片拒发新号、进只读，护栏是分片粒度不殃及节点与集群。';

-- U-P5-1：本分片下一个待发号。follower 上它由 MARKER 捎来的 leader 发号水位
-- 落盘而来（升主后发号器取 Max(文件 alloc_wm, 影子) 起步，不会重号）。
CREATE OR REPLACE FUNCTION shard_xid_next(p_shard OID)
RETURNS BIGINT LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'partdist_shard_xid_next';

COMMENT ON FUNCTION shard_xid_next(OID) IS
    'U-P5-1：本分片下一个待发号（0 = 取不到）。所有已发号都小于它。';

CREATE OR REPLACE FUNCTION get_partition_flush_lsn(partition_id OID)
    RETURNS BIGINT
    LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_get_partition_flush_lsn';

COMMENT ON FUNCTION get_partition_flush_lsn(OID) IS
    'Returns the latest partition_lsn durably written in local pg_parwal/<partition_id>/.';

CREATE OR REPLACE FUNCTION get_follower_applied_part_lsn(partition_id OID)
    RETURNS BIGINT
    LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_get_follower_applied_part_lsn';

COMMENT ON FUNCTION get_follower_applied_part_lsn(OID) IS
    'Returns the local follower applied_part_lsn for partition_id from follower_partition_map, or 0 if absent.';

CREATE OR REPLACE FUNCTION partwal_notify_primary_switch(
    partition_id OID,
    old_primary_node INTEGER,
    new_primary_node INTEGER,
    switch_orig_lsn PG_LSN
)
    RETURNS void
    LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_partwal_notify_primary_switch';

COMMENT ON FUNCTION partwal_notify_primary_switch(OID, INTEGER, INTEGER, PG_LSN) IS
    'Notifies pg_partdist that Raft has committed and applied a partition primary switch.';

-- ------------------------------------------------------------------
-- P2 — 数据面 Raft 组的 parwal 边界函数
-- ------------------------------------------------------------------
-- 分区 Raft 组以 parwal 记录为 Raft entry：leader 用 partwal_read_record 取出
-- 记录随 AppendEntries 下发，follower 用 partwal_follower_append 原样落盘后才
-- ack（故"多数派提交"== 多数派字节已 fsync），再由 follower_set_applied_part_lsn
-- 推进进度游标。此阶段不做 redo —— 物理回放是 P3。
--
-- 注意：同一逻辑分片在各节点的本地 OID 不同，调用方必须先用 P0 的
-- local_partition_for_shard(global_shard_id) 把组 id 解析成本节点 partition_id。

-- parwal-3.0：OUT 列表里 xid 换成 gxid（64 位全局事务标识，FRD §9.1）并新增
-- flags（记录类别 DATA/MARKER/CTRL）。改 OUT 参数不能靠 CREATE OR REPLACE ——
-- 输出参数参与函数签名，必须先 DROP，否则升级时留下一个仍按 2.0 列布局解析
-- 的旧入口，而它指向的却是新 .so 的符号。
DROP FUNCTION IF EXISTS partwal_read_record(OID, BIGINT);

CREATE OR REPLACE FUNCTION partwal_read_record(
    p_partition_id OID,
    p_partition_lsn BIGINT,
    OUT orig_lsn PG_LSN,
    OUT rmid INTEGER,
    OUT info INTEGER,
    OUT flags INTEGER,
    OUT gxid BIGINT,
    OUT data BYTEA
) RETURNS record LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_partwal_read_record';

COMMENT ON FUNCTION partwal_read_record(OID, BIGINT) IS
    'Leader 侧：按 partition_lsn 从本节点 pg_parwal 读出一条完整 parwal 记录（头部字段 + 原始 WAL 字节）。gxid 高 16 位为来源节点号；读到 2.0 老记录时归一为节点号 0。';

-- p_partition_lsn 是 **leader 指定的**编号，follower 必须按它落盘而非本地自增：
-- 本节点同时是若干分区的 primary、又是另一些分区的 secondary，同一 pg_parwal
-- 目录树下既有本地 demux 写入也有复制流，用本地计数器会让两个编号空间永久错位。
-- 重传时（该编号已落过盘）幂等 no-op；出现空洞则 ERROR，由 leader 回退补齐。
-- 同样必须先 DROP 2.0 的 7 参签名：新增参数会形成**重载**而不是替换，7 参调用
-- 仍会命中旧入口，但它的 C 实现已经按 8 个参数取值 —— 读到的第 8 个参数是越界。
DROP FUNCTION IF EXISTS partwal_follower_append(OID, BIGINT, PG_LSN, INTEGER, INTEGER, BIGINT, BYTEA);

CREATE OR REPLACE FUNCTION partwal_follower_append(
    p_partition_id OID,
    p_partition_lsn BIGINT,
    p_orig_lsn PG_LSN,
    p_rmid INTEGER,
    p_info INTEGER,
    p_flags INTEGER,
    p_gxid BIGINT,
    p_data BYTEA
) RETURNS BIGINT LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_partwal_follower_append';

-- T7.24（R-P4-13 根因）：同参数的"新追加"入口。编号已被本地孤儿记录占住时，
-- 只有新追加（条目落在本节点 raft 日志末尾之后）才许截断重写；重传时不一致
-- 则标记分叉、不截断。pg_raft 在新追加时调它，重传时调原入口。
CREATE OR REPLACE FUNCTION partwal_follower_append_fresh(
    p_partition_id OID,
    p_partition_lsn BIGINT,
    p_orig_lsn PG_LSN,
    p_rmid INTEGER,
    p_info INTEGER,
    p_flags INTEGER,
    p_gxid BIGINT,
    p_data BYTEA
) RETURNS BIGINT LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_partwal_follower_append_fresh';

-- P7-N41（2026-10-03）：group commit —— follower 收一批 AppendEntries 时批内只写不 fsync，
-- 批尾 end 一次 fsync 碰过的段并持久化 checkpoint（失败报错 ⇒ 不 ack）。
CREATE OR REPLACE FUNCTION partwal_defer_sync_begin() RETURNS boolean
    LANGUAGE c VOLATILE AS 'MODULE_PATHNAME', 'pg_partdist_partwal_defer_sync_begin';
CREATE OR REPLACE FUNCTION partwal_defer_sync_end() RETURNS integer
    LANGUAGE c VOLATILE AS 'MODULE_PATHNAME', 'pg_partdist_partwal_defer_sync_end';

-- Raft 日志截断时同步截断 parwal：被截断条目的字节若滞留，切主后新 leader
-- 会把不同记录写到同一个 partition_lsn 上，幂等去重反而会保留错误内容。
CREATE OR REPLACE FUNCTION partwal_truncate_to(
    p_partition_id OID,
    p_keep_upto_part_lsn BIGINT
) RETURNS BOOLEAN LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_partwal_truncate_to';

COMMENT ON FUNCTION partwal_follower_append(OID, BIGINT, PG_LSN, INTEGER, INTEGER, INTEGER, BIGINT, BYTEA) IS
    'Follower 侧平凡 apply：按 leader 指定的 partition_lsn 把 parwal 记录原样落盘并 fsync，返回该 partition_lsn。重传幂等，不做 redo。gxid/flags 原样透传，不在此处重新合成 —— 记录的来源节点是 leader，不是本节点。';

-- ------------------------------------------------------------------
-- DTX-2PC 记录（DTX_2PC_DESIGN.md §5）
-- ------------------------------------------------------------------
-- DTX 记录与 DATA 记录共用同一个 partition_lsn 序号空间和同一条复制通道，
-- 靠头部 flags 里的 PARTWAL_FLAG_DTX(8) 区分；子类型放在 info 字段
-- （1=PREPARE 2=DECISION 3=COMMIT 4=ABORT 5=FORGET）。orig_lsn 恒为 0 —— 它不是 WAL
-- 记录，回放侧按 flags 在分派处即被路由走，永不进 rm_redo。
CREATE OR REPLACE FUNCTION partwal_append_dtx_record(
    p_partition_id OID,
    p_kind INTEGER,
    p_dtxid BIGINT,
    p_coord_gsid BIGINT,
    p_commit_ts BIGINT DEFAULT 0,
    p_verdict INTEGER DEFAULT 0,
    p_participants BIGINT[] DEFAULT NULL
) RETURNS BIGINT LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_partwal_append_dtx_record';

-- P7-N12：分片 clog 状态的正式 SQL 入口（此前只有测试临时建的包装）。0=RUNNING 1=PREPARED 2=COMMITTED 3=ABORTED
-- P7-N16：该分片是否处在全量物理基线半程（收到 FULL_BASELINE 截空、未收到 BASELINE_END）。
-- 升主前置据此拒绝放行"空壳主"（索引 0 字节、整片不可读）。
-- P7-N18（之二）：升主前把 (from_plsn, upto_plsn] 这截已复制、可能未 apply 的流里出现过的
-- 分片 xid 全部吸收进发号水位（DATA 记录的 xmin/xmax + MARKER 尾块），挡住新主重发旧号。
CREATE OR REPLACE FUNCTION shard_absorb_tail_xids(p_shard OID, p_from BIGINT, p_upto BIGINT)
    RETURNS integer LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_absorb_tail_xids';

COMMENT ON FUNCTION shard_absorb_tail_xids(OID, BIGINT, BIGINT) IS
    '扫 (p_from, p_upto] 的分区流，把 DATA/MARKER 里出现过的分片 xid 抬进发号水位；升主前置调用以防重号。';

CREATE OR REPLACE FUNCTION shard_baseline_pending(p_shard OID)
    RETURNS boolean LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_baseline_pending';

-- ★ P7-N64（2026-10-05）：本地这份分片副本是否残缺（不可升主、需要重供）。
-- "基线半程"标记只在共享内存里，节点重启就丢 —— 实测供副本中途被断电的成员重启后
-- 标记没了，升主前置放行，堆和主键都是 0 字节的空壳当上了主。这里补一条**磁盘事实**：
-- 分片任何一个索引文件是 0 字节（btree / hash / gin 都至少有一个元页）⇒ 基线没灌完。
CREATE OR REPLACE FUNCTION shard_local_copy_broken(p_shard OID)
    RETURNS boolean LANGUAGE sql STABLE
AS $fn$
    SELECT partdist.shard_baseline_pending(p_shard)
        OR EXISTS (SELECT 1 FROM pg_catalog.pg_index i
                    WHERE i.indrelid = p_shard
                      AND pg_catalog.pg_relation_size(i.indexrelid) = 0)
$fn$;

COMMENT ON FUNCTION shard_baseline_pending(OID) IS
    '收到 FULL_BASELINE（截空本地文件、等 FPI 重建）到收到匹配 BASELINE_END 之间返回真；升主前置见真即不放行。';

CREATE OR REPLACE FUNCTION shard_mark_promotion_pending(p_shard OID)
    RETURNS void LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_mark_promotion_pending';

COMMENT ON FUNCTION shard_mark_promotion_pending(OID) IS
    'P7-N31：升主前置返回 1 时调，标记该分片"即将登记为主"15 s；读闸门见此标记会等本地登记 apply 跟上而不是立即拒读。';

CREATE OR REPLACE FUNCTION shard_promoted_selfheld(p_shard OID)
    RETURNS boolean LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_promoted_selfheld';

COMMENT ON FUNCTION shard_promoted_selfheld(OID) IS
    'P7-N23：本节点对该分片仍持「已升主」身份，且（本进程生命期内）升主以来没收到过他人写的 DATA/MARKER；为真时本地流未回放的尾巴全是自己当主时写的，再次升主可跳过追平。';

CREATE OR REPLACE FUNCTION shard_clog_status(p_shard OID, p_xid BIGINT)
    RETURNS integer LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'partdist_shard_clog_read';

COMMENT ON FUNCTION shard_clog_status(OID, BIGINT) IS
    '读本节点分片 clog 里一个分片 xid 的状态：0=IN_PROGRESS（含空洞及 prepared 事务）2=COMMITTED 3=ABORTED；2PC 状态独立保存。';

CREATE OR REPLACE FUNCTION shard_clog_status_full(p_shard OID, p_xid BIGINT)
    RETURNS text LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'partdist_shard_clog_read_full';

COMMENT ON FUNCTION shard_clog_status_full(OID, BIGINT) IS
    '读本节点分片 clog 槽的全文："st=<状态> sts=<start_ts> cts=<commit_ts>"。';

-- P7-N12（之二）：升主节点把 in-doubt 决议映射回分片 xid 并落账（见 raft_boundary.c）
CREATE OR REPLACE FUNCTION partwal_prepare_sxid(
    p_partition_id OID,
    p_dtxid BIGINT,
    p_upto BIGINT,
    OUT sxid BIGINT,
    OUT hdr_gxid BIGINT
) RETURNS record LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_partwal_prepare_sxid';

COMMENT ON FUNCTION partwal_prepare_sxid(OID, BIGINT, BIGINT) IS
    '按 dtxid 在本分区流（1..p_upto）里找 DTX_PREPARE 记录的头部 gxid，再取紧随其后同 gxid 的 PREPARE MARKER 尾块里的分片 xid。找不到返回 NULL。';

CREATE OR REPLACE FUNCTION shard_verdict_apply(
    p_partition_id OID,
    p_sxid BIGINT,
    p_hdr_gxid BIGINT,
    p_committed BOOLEAN,
    p_commit_ts BIGINT
) RETURNS boolean LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_verdict_apply';

-- DTX identity is separate from the stream's native node/local transaction ID.
CREATE OR REPLACE FUNCTION shard_verdict_apply(
    p_partition_id OID, p_sxid BIGINT, p_hdr_gxid BIGINT,
    p_committed BOOLEAN, p_commit_ts BIGINT, p_dtxid BIGINT
) RETURNS boolean LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_verdict_apply';

COMMENT ON FUNCTION shard_verdict_apply(OID, BIGINT, BIGINT, BOOLEAN, BIGINT) IS
    '把一笔 in-doubt 判决落进本地分片 clog（start_ts 取 PREPARED 槽），并追加带分片 xid 的判决 MARKER 复制给副本（与 leader 侧 dtx_replicate_verdict 同构）。本地已落账即返回，复制失败返回 false。';

COMMENT ON FUNCTION partwal_append_dtx_record(OID, INTEGER, BIGINT, BIGINT, BIGINT, INTEGER, BIGINT[]) IS
    '在本节点该分区的 parwal 流追加一条 DTX 记录并 fsync，返回分配到的 partition_lsn。participants 仅 DECISION(kind=2) 记录携带。';

-- ------------------------------------------------------------------
-- DTX-2PC 决议索引表（DTX_2PC_DESIGN.md §6.2）
-- ------------------------------------------------------------------
-- DECISION 记录本身是权威（它在协调组的 Raft 日志里，达多数派即为全局提交点），
-- 但按 dtxid 顺序扫段文件太慢。协调组的 **apply 路径**在每个组成员上维护这张
-- 索引表，因此切主后新 leader 手里天然就有全表 —— 这正是"协调权随 Raft 选举
-- 自动转移"的落地形态。
--
-- 决议槽一次性：INSERT ... ON CONFLICT (dtxid) DO NOTHING。第一条进入协调组
-- 日志的决议获胜，后到的同 dtxid 决议被忽略（推定中止与正常提交路径可能并发，
-- 见 §2.2）。
CREATE TABLE IF NOT EXISTS dtx_decision (
    dtxid         BIGINT      PRIMARY KEY,
    coord_gsid    BIGINT      NOT NULL,
    verdict       SMALLINT    NOT NULL,   -- 1=COMMIT, 2=ABORT
    commit_ts     BIGINT      NOT NULL DEFAULT 0,
    participants  BIGINT[]    NOT NULL DEFAULT '{}',
    decided_plsn  BIGINT      NOT NULL,   -- 该 DECISION 记录的 partition_lsn
    acked         BIGINT[]    NOT NULL DEFAULT '{}',  -- 已回执的参与组（GC 用）
    decided_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE dtx_decision IS
    'DTX-2PC 决议索引：由协调组的 apply 路径在每个成员上维护，供 dtx_status 快速应答。权威仍是协调组日志里的 DECISION 记录。';

-- ------------------------------------------------------------------
-- dtx_participant：本节点在某笔分布式事务里的参与登记（§9.3 的落地形态）
--
-- 由 worker 自己在 PREPARE TRANSACTION 的接线里、经 libpq **独立事务**写入
-- （写在本事务里就会随事务一起进 prepared，谁也看不见）。两个消费方：
--
--   1) master 侧驱动：收齐 PREPARE 应答后逐节点读 gsids 合并出写集，
--      据此算协调组 coord_gsid、再回写下来；
--   2) 参与者恢复守护：本节点崩溃重启后，pg_prepared_xacts 里的 gid 在这里
--      查到 coord_gsid，才知道该向哪个组要决议。
--
-- ★ coord_gsid 为 NULL 的含义是**推定中止安全**的关键：master 严格先把
--   coord_gsid 写到全部参与者、再调 dtx_decide。因此"本行 coord_gsid 仍为
--   NULL" ⇒ 决议**必然还没做过** ⇒ 超时后回滚是安全的。顺序反过来就会出现
--   "全局已 COMMIT、参与者却查不到协调组"的不可解状态。
--
-- 一笔分布式事务在同一节点上可能有多个 gid（Citus 对同一节点可能开多条
-- 连接，gid 末段的 <conn> 不同），所以主键是 (dtxid, gid)。
CREATE TABLE IF NOT EXISTS dtx_participant (
    dtxid       BIGINT      NOT NULL,
    gid         TEXT        NOT NULL,
    gsids       BIGINT[]    NOT NULL,   -- 本节点在该事务里真正写过的分区组
    coord_gsid  BIGINT,                 -- master 下发；NULL = 决议尚未可能发生
    noted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_dtx_participant PRIMARY KEY (dtxid, gid)
);
CREATE INDEX IF NOT EXISTS idx_dtx_participant_dtxid ON dtx_participant(dtxid);
CREATE INDEX IF NOT EXISTS idx_dtx_participant_noted ON dtx_participant(noted_at);

COMMENT ON TABLE dtx_participant IS
    'DTX-2PC 参与登记：本节点在某笔分布式事务里写过的分区组，以及 master 下发的协调组。'
    'coord_gsid IS NULL 蕴含"决议尚未做过"（master 保证先下发再决议），推定中止据此成立。';

-- worker 自治登记（由 prepare 接线经 libpq 独立事务调用）。
CREATE OR REPLACE FUNCTION dtx_note_participant(
    p_dtxid BIGINT,
    p_gid TEXT,
    p_gsids BIGINT[]
) RETURNS BOOLEAN
    LANGUAGE sql VOLATILE
AS $$
    INSERT INTO partdist.dtx_participant (dtxid, gid, gsids)
    VALUES (p_dtxid, p_gid, p_gsids)
    ON CONFLICT (dtxid, gid) DO UPDATE
       SET gsids = EXCLUDED.gsids, noted_at = now()
    RETURNING true
$$;

COMMENT ON FUNCTION dtx_note_participant(BIGINT, TEXT, BIGINT[]) IS
    '登记本节点在某笔分布式事务里写过的分区组。必须在独立事务里调用。';

-- master 侧第一轮：本节点在该事务里写过的全部分区组（多个 gid 取并集）。
CREATE OR REPLACE FUNCTION dtx_local_participant(p_dtxid BIGINT)
    RETURNS BIGINT[]
    LANGUAGE sql STABLE
AS $$
    SELECT COALESCE(
        (SELECT array_agg(DISTINCT g ORDER BY g)
           FROM partdist.dtx_participant p, unnest(p.gsids) AS g
          WHERE p.dtxid = p_dtxid),
        '{}'::bigint[])
$$;

COMMENT ON FUNCTION dtx_local_participant(BIGINT) IS
    '本节点在该分布式事务里真正写过的分区组（global_shard_id 升序去重）。空数组 = 只读参与者。';

-- master 侧第二轮：下发协调组。必须在 dtx_decide **之前**完成（见表注释）。
CREATE OR REPLACE FUNCTION dtx_note_coord(p_dtxid BIGINT, p_coord_gsid BIGINT)
    RETURNS INTEGER
    LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    n INTEGER;
BEGIN
    UPDATE partdist.dtx_participant
       SET coord_gsid = p_coord_gsid
     WHERE dtxid = p_dtxid
       AND coord_gsid IS DISTINCT FROM p_coord_gsid;
    GET DIAGNOSTICS n = ROW_COUNT;

    -- 已经是同一个值时 ROW_COUNT=0，但登记确实在，也算成功
    IF n = 0 AND EXISTS (SELECT 1 FROM partdist.dtx_participant WHERE dtxid = p_dtxid) THEN
        n := 1;
    END IF;
    RETURN n;
END;
$$;

COMMENT ON FUNCTION dtx_note_coord(BIGINT, BIGINT) IS
    '把 master 算出的协调组下发到本节点的参与登记。返回受影响行数，0 = 本节点没有该事务的登记。';

-- 参与者恢复守护用：某个 prepared 事务的 (dtxid, coord_gsid, 本节点写过的组)。
CREATE OR REPLACE FUNCTION dtx_participant_of(
    p_gid TEXT,
    OUT dtxid BIGINT,
    OUT coord_gsid BIGINT,
    OUT gsids BIGINT[]
) RETURNS record
    LANGUAGE sql STABLE
AS $$
    SELECT dtxid, coord_gsid, gsids FROM partdist.dtx_participant WHERE gid = p_gid
$$;

COMMENT ON FUNCTION dtx_participant_of(TEXT) IS
    '按 prepared 事务的 gid 取本节点的参与登记。coord_gsid 为 NULL 表示 master 未下发过协调组（⇒ 决议必然未做过）。';

-- GC：登记行的生命周期比 prepared 事务长一点点（prepare 失败会留下孤儿行）。
-- 只删"已无对应 prepared 事务且超过 age"的行；有 prepared 事务在就绝不删，
-- 那正是恢复守护要用的线索。
CREATE OR REPLACE FUNCTION dtx_gc_participant(p_age_seconds INTEGER DEFAULT 3600)
    RETURNS INTEGER
    LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    n INTEGER;
BEGIN
    DELETE FROM partdist.dtx_participant p
     WHERE p.noted_at < now() - make_interval(secs => p_age_seconds)
       AND NOT EXISTS (SELECT 1 FROM pg_prepared_xacts x WHERE x.gid = p.gid);
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END;
$$;

COMMENT ON FUNCTION dtx_gc_participant(INTEGER) IS
    '清理已无对应 prepared 事务的陈旧参与登记（prepare 失败会留下孤儿行）。有 prepared 事务在的行绝不删。';

CREATE OR REPLACE FUNCTION partwal_read_dtx_record(
    p_partition_id OID,
    p_partition_lsn BIGINT,
    OUT kind INTEGER,
    OUT dtxid BIGINT,
    OUT coord_gsid BIGINT,
    OUT commit_ts BIGINT,
    OUT verdict INTEGER,
    OUT participants BIGINT[]
) RETURNS record LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_partwal_read_dtx_record';

COMMENT ON FUNCTION partwal_read_dtx_record(OID, BIGINT) IS
    '解析该 partition_lsn 上的 DTX 记录；该位置不是 DTX 记录（flags 无 PARTWAL_FLAG_DTX）时返回 NULL。';

COMMENT ON FUNCTION partwal_truncate_to(OID, BIGINT) IS
    'Raft 日志截断时同步截断本节点 pg_parwal：丢弃 partition_lsn > p_keep_upto_part_lsn 的记录并重写 checkpoint。';

CREATE OR REPLACE FUNCTION follower_set_applied_part_lsn(
    p_partition_id OID,
    p_applied_part_lsn BIGINT
) RETURNS BOOLEAN LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_follower_set_applied_part_lsn';

COMMENT ON FUNCTION follower_set_applied_part_lsn(OID, BIGINT) IS
    '推进 follower_partition_map.applied_part_lsn（单调不回退）。数据面 Raft 组是该列的第一个真实写入方。';

-- ==================================================================
-- R1 惰性回放（follower 物理重放）边界函数（FRD §5/§7/§10）
-- ==================================================================

-- leader 侧：构建 + 注册 + 持久化 shard 的物理文件集合（主堆/索引/TOAST）。
-- DDL 变更 fileset 后必须重新调用（FRD §12）。返回成员数。
CREATE OR REPLACE FUNCTION register_shard_fileset(
    p_shard REGCLASS
) RETURNS INTEGER LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_register_shard_fileset';

COMMENT ON FUNCTION register_shard_fileset(REGCLASS) IS
    'Leader 侧：把 shard 的全部物理文件（主堆/索引/TOAST 堆及其索引）注册进捕获反向映射并持久化，索引与 TOAST 的 WAL 记录随主堆进入同一 parwal 流。';

-- ------------------------------------------------------------------
-- T6.1（P6）：全量物理基线
-- ------------------------------------------------------------------
--
-- 把该 shard 的**全部字节**重新灌进它自己的分区流（整页 FPI，走的是 §12
-- DDL 变更那条成熟通路），返回这次基线的起点 partition_lsn。
--
-- 用法：在 **leader** 上对分片表调用，把返回值交给 follower 当
-- base_part_lsn —— 从这一条开始重放，之前的记录一律不看。
--
-- 实装的是设计 §13 约束 2 后半句（"拷贝时记下 partition_lsn 静止点"）。
-- 此前只有前半句：locmap 只配对"哪个文件对哪个文件"，从不配对"从哪个游标
-- 开始"，缺省游标 0 等于沉默地断言"本地文件 == leader 在流起点时的文件"。
DROP FUNCTION IF EXISTS shard_baseline_emit(REGCLASS);

CREATE OR REPLACE FUNCTION shard_baseline_emit(p_shard REGCLASS)
    RETURNS BIGINT
    LANGUAGE c STRICT
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_baseline_emit';

-- ★ P7-N69：定向基线 —— 只有 p_target_node 截断重建，其余 follower 照常回放 FPI、不截断
CREATE OR REPLACE FUNCTION shard_baseline_emit(p_shard REGCLASS, p_target_node INTEGER)
    RETURNS BIGINT
    LANGUAGE c STRICT
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_baseline_emit';

COMMENT ON FUNCTION shard_baseline_emit(REGCLASS) IS
    '发射该 shard 的全量物理基线（整页 FPI 进本分区流），返回基线起点 partition_lsn。三个消费者：副本初始配对、快路径分叉修复、永久分叉修复。只在 leader 上调用；块数超过 pg_partdist.fileset_inline_max_blocks 时显式报错而不静默降级。';

-- fileset 导出：follower 侧 replay_set_locmap 的输入。
CREATE OR REPLACE FUNCTION shard_fileset(
    p_shard REGCLASS,
    OUT role INTEGER,
    OUT ord INTEGER,
    OUT spc OID,
    OUT db OID,
    OUT relnum OID
) RETURNS SETOF record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_shard_fileset';

COMMENT ON FUNCTION shard_fileset(REGCLASS) IS
    '导出 shard 的物理文件集合描述（role: 0=主堆 1=索引 2=TOAST堆 3=TOAST索引；ord=同 role 内定义序）。';

-- follower 侧：按 (role, ord) 把 leader fileset 与本地 shell 表配对成 loc_map，
-- 持久化到 pg_parwal/<oid>/locmap，并登记补丁 0002 的刷脏豁免。
--
-- ★ T6.2 新增第 7 参 p_base_part_lsn —— **本配对的起效游标**，实装设计
--   §13 约束 2 的后半句（"拷贝时记下 partition_lsn 静止点"）。
--     >0 = 由 partdist.shard_baseline_emit() 给出，自那条全量基线起重放；
--      0 = 显式声明"这条流从关系的出生点开始"。这是**断言**不是缺省值，
--          函数会核对本地关系确为空，不为空即报错。
--
-- ★ 必须先 DROP 旧的 6 参签名再建 7 参版本：只 CREATE OR REPLACE 会留下一个
--   **重载**，而那个 6 参声明仍指向同一个 C 符号 —— 它会去读并不存在的第 7 个
--   参数。带 DEFAULT 的 7 参版本对既有 6 参调用点完全兼容。
DROP FUNCTION IF EXISTS replay_set_locmap(REGCLASS, INTEGER[], INTEGER[], OID[], OID[], OID[]);

CREATE OR REPLACE FUNCTION replay_set_locmap(
    p_local_shard REGCLASS,
    p_roles INTEGER[],
    p_ords INTEGER[],
    p_spcs OID[],
    p_dbs OID[],
    p_relnums OID[],
    p_base_part_lsn BIGINT DEFAULT 0
) RETURNS INTEGER LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_replay_set_locmap';

COMMENT ON FUNCTION replay_set_locmap(REGCLASS, INTEGER[], INTEGER[], OID[], OID[], OID[], BIGINT) IS
    'Follower 侧：建立 leader→本地 文件号映射（loc_map）+ 本配对的起效游标。两侧索引/TOAST 结构必须一致（同源物理基线，FRD §13.2）。p_base_part_lsn>0 取自 shard_baseline_emit()；=0 是"从流起点开始"的断言，函数会核对本地关系为空。';

-- follower 侧当前生效的 loc_map。leader 一次 VACUUM FULL/REINDEX/TRUNCATE
-- 就会经 CTRL:FILESET_UPDATE 把 leader_relnum 整体换掉（FRD §12），
-- 「控制记录应用了没有」要能直接查，而不是从页面比对通没通去反推。
CREATE OR REPLACE FUNCTION replay_locmap(
    p_local_shard REGCLASS,
    OUT role INTEGER,
    OUT ord INTEGER,
    OUT leader_spc OID,
    OUT leader_db OID,
    OUT leader_relnum OID,
    OUT local_relnum OID
) RETURNS SETOF record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_replay_locmap';

COMMENT ON FUNCTION replay_locmap(REGCLASS) IS
    'Follower 侧当前生效的 leader→本地 文件号映射（含 role/ord 配对键）。';

-- 启停该 shard 的物理回放（启用标记持久化，节点重启后自动恢复）。
CREATE OR REPLACE FUNCTION replay_enable(
    p_local_shard REGCLASS
) RETURNS BOOLEAN LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_replay_enable';

CREATE OR REPLACE FUNCTION replay_disable(
    p_local_shard REGCLASS
) RETURNS BOOLEAN LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_replay_disable';

COMMENT ON FUNCTION replay_enable(REGCLASS) IS
    '允许该本地 shard 副本被触发回放（前提：已 replay_set_locmap）。注意惰性语义：本函数只是 arm，不会开始回放，真正的回放由 replay_catchup 触发。';
COMMENT ON FUNCTION replay_disable(REGCLASS) IS
    '解除该本地 shard 副本的 armed 状态，此后 replay_catchup 会被拒绝。';

-- 回放状态观测。
CREATE OR REPLACE FUNCTION replay_status(
    OUT shard OID,
    OUT armed BOOLEAN,
    OUT state TEXT,
    OUT claimed_by INTEGER,
    OUT applied BIGINT,
    OUT target BIGINT,
    OUT durable BIGINT,
    OUT max_orig PG_LSN
) RETURNS SETOF record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_replay_status';

COMMENT ON FUNCTION replay_status() IS
    '各回放槽位状态：armed=是否允许被触发（惰性：armed 不等于在回放），state=idle/catching_up/failed，applied=已回放到的 partition_lsn，target=当前触发目标，durable=apply_checkpoint 落盘游标。';

-- ==================================================================
-- 惰性回放触发入口（L1）
-- ==================================================================

-- 平时副本一条 redo 都不做（只由 partwal_follower_append 落字节）；
-- 本函数是唯一让回放真正发生的入口，同步等待追平完成后返回。
--
-- p_upto = NULL：追到本地已落盘的全部字节（运维/测试便利）。
-- 生产升主路径**必须显式传该 Raft 组的 commit_index** —— 回放上界由调用方
-- 在确切知道提交位置的时刻给定，因此不存在"误放未提交条目"的问题
-- （物理 redo 不可逆，这是持续回放形态才要操心的风险）。
CREATE OR REPLACE FUNCTION replay_catchup(
    p_local_shard REGCLASS,
    p_upto BIGINT DEFAULT NULL,
    p_timeout_ms INTEGER DEFAULT 300000
) RETURNS BIGINT LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_replay_catchup';

COMMENT ON FUNCTION replay_catchup(REGCLASS, BIGINT, INTEGER) IS
    '惰性回放触发入口：把该副本追平到 p_upto（NULL=本地全部字节；升主时传 Raft commit_index），同步等待完成，返回追平后的 applied_part_lsn。';

-- ------------------------------------------------------------------
-- T6.3b：升主推进本地 WAL 插入位点（FRD §11 步骤 4，内核补丁 0010）
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION advance_wal_past_shard(
    p_local_shard REGCLASS
) RETURNS PG_LSN LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_advance_wal_past_shard';

CREATE OR REPLACE FUNCTION advance_wal_to(
    p_target PG_LSN
) RETURNS PG_LSN LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_advance_wal_to';

COMMENT ON FUNCTION advance_wal_to(PG_LSN) IS
    'FRD §11 步骤 4 的原语（内核补丁 0010）：把本地 WAL 插入位点在线推进到 p_target 之后的段边界。跳跃发生在一次强制检查点持有全部插入锁的临界区内，因此不存在"跳了却没有检查点"的丢数据窗口。跳过的 WAL 段号成为永久空洞：本地崩溃恢复不受影响，原生归档/PITR 在此断链。限超级用户。';

CREATE OR REPLACE FUNCTION shard_xid_raise_watermark(
    p_shard OID,
    p_watermark BIGINT
) RETURNS void LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_xid_raise_watermark';

COMMENT ON FUNCTION shard_xid_raise_watermark(OID, BIGINT) IS
    '把本分片的发号水位抬到 p_watermark（只升不降）。供给副本时由 leader 把自己的水位带过去：纯靠物理基线建出来的副本，元组里带的是分片 xid，却没有任何 MARKER 教它水位——水位为 0 会让 T6.3c 的读闸门把它误当成遗留副本放行，也会让 T6.4 的切主认领区间为空。';

CREATE OR REPLACE FUNCTION shard_divergence(p_shard OID)
    RETURNS TEXT LANGUAGE c STABLE
    AS 'MODULE_PATHNAME', 'partdist_shard_divergence';

COMMENT ON FUNCTION shard_divergence(OID) IS
    '§13 约束 13 的检测面：该分片有没有被标记为「副本可能已分叉」，返回标记时刻与原因，无标记返回 NULL。标记由复制挂钩失败时就地写下（非事务性——出事的事务马上要中止，写表会一起回滚）。修复是重做物理基线：shard_baseline_emit / provision_shard_replica 成功后会自己清。';

-- R3 读路径取证（FOLLOWER_REPLAY_DESIGN.md §9.4）：把"两跳"摊开。
--   role       本关系在本节点的角色：native_leader / replica / promoted
--   watermark  升主水位 W（promoted 才有意义）
--   nxidmap    本后端缓存的 xid_map 条目数
--   gxid       该 xid 翻出来的全局事务号；NULL = 不属于回放宇宙
--   status     gclog 里的判决：running/prepared/committed/aborted，
--              或 not_replayed（gxid 解不出来）
CREATE OR REPLACE FUNCTION route_resolve(
    p_rel OID,
    p_xid BIGINT,
    OUT role TEXT,
    OUT watermark BIGINT,
    OUT nxidmap INTEGER,
    OUT gxid BIGINT,
    OUT status TEXT
) RETURNS record LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'partdist_route_resolve';

COMMENT ON FUNCTION route_resolve(OID, BIGINT) IS
    'R3 读路径取证：本关系的路由角色/升主水位/xid_map 规模，以及给定 xid 翻成的 gxid 与 gclog 判决。';

CREATE OR REPLACE FUNCTION route_status(p_shard OID)
    RETURNS TEXT LANGUAGE c STABLE
    AS 'MODULE_PATHNAME', 'partdist_route_status';

COMMENT ON FUNCTION route_status(OID) IS
    'FRD §11 步骤 5 的观测面：该分片在**本节点**上的角色（promoted / replica_or_plain）、写入会不会被 wal_insert_hook 捕获、fileset 成员数、分配器水位。此前这三件事分散在 replay_status() / 文件系统 / shard_xid_next()，交接出问题时最需要的恰恰是这一句。';

-- T7.27（P7-W2）：捕获环观测面
CREATE OR REPLACE FUNCTION partwal_ring_stats(
    OUT capacity INTEGER,
    OUT unconsumed INTEGER,
    OUT overwrites BIGINT,
    OUT backpressure_flushes BIGINT,
    OUT diverged_pending BOOLEAN
) RETURNS record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_partwal_ring_stats';

COMMENT ON FUNCTION partwal_ring_stats() IS
    'T7.27/P7-W2：全节点共享捕获环的容量、未消费槽位数、累计覆盖次数（>0 即有记录没进分区流、副本缺记录）、写路径背压排空次数、是否有待自动修复的分叉标记。';

CREATE OR REPLACE FUNCTION repair_diverged_shards()
    RETURNS TEXT LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_repair_diverged_shards';

-- ★ P7-N66：只修指定分片（升主前置用，避免一个组的升主替别的组发基线、串成跨组等待链）
CREATE OR REPLACE FUNCTION repair_diverged_shards(p_only OID)
    RETURNS TEXT LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_repair_diverged_shards';

COMMENT ON FUNCTION repair_diverged_shards() IS
    '把「见到分叉标记就重做基线」从逐个分片手工变成一次调用：扫 pg_parwal 下所有带标记的分片，对本节点能发基线的那些重发（基线成功自动清标），发不出去的（多半不是组 leader / 多数派没恢复）保留标记并计入 skipped。限超级用户。升主路径也会顺带跑一次。';

CREATE OR REPLACE FUNCTION shard_clear_divergence(p_shard OID)
    RETURNS void LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_clear_divergence';

COMMENT ON FUNCTION shard_clear_divergence(OID) IS
    '手工清除分叉标记（限超级用户）。清标不等于修好了——真正的修复是重做物理基线，那两个入口成功后会自己清。本函数只给"取证后确认无事"的场景留。';

-- ------------------------------------------------------------------
-- 批次 #7：副本供给（在 leader 上调用，向目标节点推）
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION provision_shard_replica(
    p_global_shard_id BIGINT,
    p_target_node     INTEGER
) RETURNS TEXT LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_provision_shard_replica';

-- P7-N25：新主登记后由一次性工作者调用；也可人工调。在本节点（须是该组 leader）上判定前任主 p_target
-- 有无 armed 回放槽位，没有就 provision_shard_replica(p_gsid, p_target)。返回 not_leader / not_needed / done: … / retry: …
CREATE OR REPLACE FUNCTION reprovision_demoted(p_gsid BIGINT, p_target INTEGER)
    RETURNS TEXT LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_reprovision_demoted';

-- 2026-10-12：自动归队的兜底扫尾。pg_raft maintenance 进程每 60 s 经常驻连接调一次。
CREATE OR REPLACE FUNCTION reprovision_sweep()
    RETURNS INTEGER LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_reprovision_sweep';

COMMENT ON FUNCTION reprovision_sweep() IS
    '对本节点此刻是 leader 且持已升主记号的数据组，逐对端批量核对成员副本是否健康（armed、未 failed、不残缺、无分叉），连续两轮不健康的排自动重供。补的是归队请求撞 not_leader 被丢、此后再没人替它重供的洞。返回本轮排上的重供数。';

COMMENT ON FUNCTION provision_shard_replica(BIGINT, INTEGER) IS
    '把「给分片 X 在节点 N 上建一个副本」做成一个入口：本地登记 fileset → 发物理基线（§13 约束 2 的静止点）→ 到目标节点建壳表、按该基线的 partition_lsn 配 locmap、arm 回放。此前 register_shard_fileset/replay_set_locmap/replay_enable 在产品代码里没有任何调用方，副本只能靠人手建。须在该分片的 leader 上、由超级用户调用。';

-- ------------------------------------------------------------------
-- T6.4：§6.6 第三支，切主认领
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION shard_claim_on_promote(
    p_shard OID
) RETURNS INTEGER LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_claim_on_promote';

COMMENT ON FUNCTION shard_claim_on_promote(OID) IS
    '升主收尾：把该分片 [claim_wm, watermark) 区间内仍是 RUNNING（含空洞）的分片 xid 一律改判 ABORTED——新主追平后流里没有提交标记即从未提交过。PREPARED 不动（§6.6 第二支），调用方须先 dtx_close_indoubt。与 T2.4 的挂槽认领不同：升主的 follower 上槽位一定已存在（回放推水位时就挂了），那条路径一条都不认领，所以切主必须有自己的入口。返回改判条数。';

COMMENT ON FUNCTION advance_wal_past_shard(REGCLASS) IS
    '升主前置：把本地 WAL 插入位点推进到该分片 max_orig_lsn（已应用的最大 leader 坐标 LSN）之后。不做的后果是升主后写入的数据在一次本地崩溃后消失——新记录 LSN 小于页面携带的 leader LSN，恢复时被 lsn <= PageGetLSN 当作"已更新过"跳过。返回推进后的插入位点；本节点没有该分片回放游标时返回 NULL。';

-- ==================================================================
-- 增强型 CLOG（pg_gclog）核账入口（R2-d）
-- ==================================================================

-- 回放引入的 xid 是**别的节点**分配的，本地原生 clog 对它们一无所知
-- （nextXid 被推进但从未 ExtendCLOG）。判决落在 pg_gclog/<node_id>/ 里，
-- 键是 gxid = (node_id << 48) | local_xid。
--
-- 从未写过的槽返回 running —— 稀疏文件空洞语义，等价"未决 = 不可见"，
-- 是安全的默认值。
CREATE OR REPLACE FUNCTION gclog_status(
    p_node_id INTEGER,
    p_local_xid BIGINT,
    OUT status TEXT,
    OUT start_ts BIGINT,
    OUT commit_ts BIGINT
) RETURNS record LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'pg_partdist_gclog_status';

COMMENT ON FUNCTION gclog_status(INTEGER, BIGINT) IS
    '查增强型 CLOG 里某个全局事务的判决：status = running/prepared/committed/aborted，commit_ts 在 aborted 时为 0。running 也表示"从未记过账"。';

-- ==================================================================
-- 副本壳表的冻结账目暴露面（R2-e，FRD §13 约束 5）
-- ==================================================================

-- 副本壳表的 relfrozenxid **不由本地 vacuum 维护** —— 冻结是靠回放 leader 的
-- freeze 记录实现的，元组物理上确实被冻了，但 pg_class.relfrozenxid 这个
-- **目录字段**没人更新，于是它的 age() 会一直涨。
--
-- 光靠建表时的 autovacuum_enabled=off **挡不住**：内核 autovacuum.c 里写着
--     if (!av_enabled && !force_vacuum)   /* But ignore if at risk */
-- 一旦 relfrozenxid 落后超过 autovacuum_freeze_max_age，force_vacuum 为真，
-- autovacuum_enabled=off 就被忽略，副本壳表会被强制 anti-wraparound vacuum
-- 扫到 —— 而它的元组带的是**外来节点的 xid**，本地 clog 要么没有对应页
-- （报 "could not access status of transaction"），要么给出张冠李戴的答案。
--
-- 同时这些表还会把库级 datfrozenxid 压住，阻塞 clog 截断。
--
-- 本函数只做**观测**：把每个副本壳表离强制阈值还有多远摆出来。
-- 真正的处置（同步 leader 的 relfrozenxid，或显式推进本地值）需要 CTRL
-- 记录通道（§12），属后续工作。
CREATE OR REPLACE FUNCTION replay_freeze_status(
    OUT shard REGCLASS,
    OUT relfrozenxid_age BIGINT,
    OUT force_threshold BIGINT,
    OUT pct_to_force NUMERIC,
    OUT autovacuum_off BOOLEAN
) RETURNS SETOF record LANGUAGE sql STABLE AS $$
    SELECT c.oid::regclass,
           age(c.relfrozenxid)::bigint,
           current_setting('autovacuum_freeze_max_age')::bigint,
           round(100.0 * age(c.relfrozenxid)
                 / current_setting('autovacuum_freeze_max_age')::numeric, 2),
           -- 布尔 reloption 存的是 off/false/no/0 等多种写法，别只匹配一种
           coalesce(array_to_string(c.reloptions, ',')
                    ~* 'autovacuum_enabled=(off|false|no|0)(,|$)', false)
    FROM pg_class c
    WHERE c.relkind = 'r'
      AND EXISTS (SELECT 1 FROM partdist.replay_status() s WHERE s.shard = c.oid)
    ORDER BY age(c.relfrozenxid) DESC
$$;

COMMENT ON FUNCTION replay_freeze_status() IS
    '副本壳表的冻结账目暴露面（FRD §13 约束 5）：pct_to_force 达到 100% 时内核会忽略 autovacuum_enabled=off 强制回卷 vacuum，而副本元组带的是外来 xid，本地无法解释。';

-- ------------------------------------------------------------------
-- 陈旧回放槽位与 pg_parwal 目录的回收
-- ------------------------------------------------------------------
--
-- 槽位上限 REPLAY_MAX_SHARDS(64)，而壳表 DROP 之后槽位不会自动释放、
-- pg_parwal/<oid>/ 也不会删 —— launcher 重启还会从这些目录把槽位重建出来。
-- 反复建/删副本的环境会先攒满目录（实测一天密集测试后每节点 143–148 个），
-- 再撞上"回放槽位已满"，此后新副本一个都建不了。
--
-- 判据只有一条：**目录名那个 OID 在 pg_class 里已经不存在**。目录名就是本地
-- shard（或副本壳表）的 OID，关系还在就说明这份流仍有主，一律不碰；关系没了，
-- 流就是垃圾 —— 无论本节点对该分片是 primary 还是 secondary，判据都一样。
--
-- 被活着的 worker 认领的槽位一律不动（它可能正在其上回放）。
-- replay_set_locmap() 会在建槽之前自动调一次，所以正常路径无需手工执行；
-- 本函数供运维在"目录攒太多但暂时不建新副本"时主动清理。
CREATE OR REPLACE FUNCTION replay_reclaim_stale(
    p_grace_seconds INTEGER DEFAULT 300,
    OUT slots_freed INTEGER,
    OUT dirs_removed INTEGER)
    RETURNS record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_replay_reclaim_stale';

COMMENT ON FUNCTION replay_reclaim_stale(INTEGER) IS
    '回收关系已不存在的回放槽位与 pg_parwal 目录。判据=OID 不在 pg_class；被活着的 worker 认领的槽位不动。replay_set_locmap 建槽前会自动调用。';

-- TX-TSO-MVCC（T2.7）：把既有 partition_map 分区登记为分片打标表。
-- P2 只支持登记不支持撤销（DROP TABLE 即全清）；需超级用户、建议自动提交。
CREATE OR REPLACE FUNCTION partdist_set_shard_mvcc(rel regclass,
                                                   enable boolean DEFAULT true)
RETURNS void
AS 'MODULE_PATHNAME', 'partdist_set_shard_mvcc'
LANGUAGE C STRICT;

-- T7.30（P7-V4）：按全局分片号在**本节点**登记打标（placement 上调用；副本壳表、非空表拒绝）。
-- 分布表请用协调者上的 set_table_shard_mvcc(regclass)，它逐 placement 下发本函数。
-- ★ P7-N40：清掉本节点的孤儿打标登记（pg_shard_xid/<oid> 还在、OID 已无对应表）。
-- 返回清掉几个。孤儿的来路见 shard_xid.c 里的注释；DROP 判据已修，这个函数是给存量用的。
CREATE OR REPLACE FUNCTION shard_mvcc_gc_orphans()
    RETURNS integer LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_shard_mvcc_gc_orphans';

COMMENT ON FUNCTION shard_mvcc_gc_orphans() IS
    'P7-N40：清掉本节点 pg_shard_xid/ 里 OID 已无对应表的孤儿登记（连同 pg_shard_clog 目录、共享内存登记、分配器槽位）。';

CREATE OR REPLACE FUNCTION shard_mvcc_register(p_shardid bigint)
RETURNS text
AS 'MODULE_PATHNAME', 'partdist_shard_mvcc_register'
LANGUAGE C STRICT VOLATILE;

-- T7.30 观测点："registered=yes|no evidence=yes|no replica=yes|no"
CREATE OR REPLACE FUNCTION shard_mvcc_status(p_rel oid)
RETURNS text
AS 'MODULE_PATHNAME', 'partdist_shard_mvcc_status'
LANGUAGE C STRICT STABLE;

-- T7.30（P7-V4）：**协调者上一次调用**，把一张分布表的全部分片登记为打标表。
-- 取代"组内每个成员各自 ALTER SYSTEM SET pg_partdist.shard_relids = '<本节点 OID 列表>'"
-- 的手工做法。时机：建表（含索引 —— 打标后禁 CREATE INDEX）之后、写入数据之前。
-- 副本不登记：身份随 CTRL SHARD_MVCC / 物理基线落成证据目录，升主时继承（T7.4）。
-- 每行一个 placement；status 以 registered/already 开头为成功，其余为失败原因。幂等，可重跑。
CREATE OR REPLACE FUNCTION set_table_shard_mvcc(p_table regclass)
RETURNS TABLE(shardid bigint, node_port integer, status text)
LANGUAGE plpgsql VOLATILE
SET search_path = partdist, pg_catalog
AS $fn$
DECLARE
    v       record;
    v_ok    boolean;
    v_res   text;
    v_n     integer := 0;
    v_bad   integer := 0;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_dist_partition d
                    WHERE d.logicalrelid = p_table AND d.partmethod <> 'n') THEN
        RAISE EXCEPTION 'set_table_shard_mvcc: % 不是（非引用表的）分布表', p_table;
    END IF;

    -- ⓪ 分片身份（shard_identity）：建表之后没人自动填，登记靠它把 shardid 翻成本地 OID。
    --   每个涉及的节点跑一次，幂等（与 raft_replicate_table_shards 同款前置）。
    FOR v IN
        SELECT DISTINCT n.nodename, n.nodeport
          FROM pg_catalog.pg_dist_shard s
          JOIN pg_catalog.pg_dist_placement p ON p.shardid = s.shardid
          JOIN pg_catalog.pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
         WHERE s.logicalrelid = p_table
    LOOP
        SELECT r.success, r.result INTO v_ok, v_res
          FROM pg_catalog.master_run_on_worker(ARRAY[v.nodename], ARRAY[v.nodeport],
               ARRAY['SELECT partdist.rebuild_shard_identity()'], false) r;
        IF NOT v_ok THEN
            RAISE EXCEPTION 'set_table_shard_mvcc: %:% 上重建分片身份失败（%）',
                v.nodename, v.nodeport, v_res;
        END IF;
    END LOOP;

    FOR v IN
        SELECT s.shardid AS sid, n.nodename, n.nodeport
          FROM pg_catalog.pg_dist_shard s
          JOIN pg_catalog.pg_dist_placement p ON p.shardid = s.shardid
          JOIN pg_catalog.pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
         WHERE s.logicalrelid = p_table
         ORDER BY s.shardid, n.nodeport
    LOOP
        v_n := v_n + 1;
        SELECT r.success, r.result INTO v_ok, v_res
          FROM pg_catalog.master_run_on_worker(ARRAY[v.nodename], ARRAY[v.nodeport],
               ARRAY[format('SELECT partdist.shard_mvcc_register(%s)', v.sid)], false) r;
        shardid   := v.sid;
        node_port := v.nodeport;
        status    := CASE WHEN v_ok THEN v_res ELSE 'FAILED: ' || v_res END;
        IF status !~ '^(registered|already) ' THEN
            v_bad := v_bad + 1;
        END IF;
        RETURN NEXT;
    END LOOP;

    IF v_n = 0 THEN
        RAISE EXCEPTION 'set_table_shard_mvcc: % 没有任何 placement', p_table;
    END IF;
    IF v_bad > 0 THEN
        RAISE WARNING 'set_table_shard_mvcc: % 个 placement 中 % 个登记失败，见 status 列；修正后可重跑（幂等）',
            v_n, v_bad;
    END IF;
END
$fn$;

-- TX-TSO-MVCC（T3.1）：TSO 服务入口（v1 = master 内存计数器，设计 §2.4）。
-- 只有 pg_partdist.tso_master=on 的节点服务；worker 经 libpq 调用（T3.2）。
CREATE OR REPLACE FUNCTION partdist_tso_start_ts(node int, oldest bigint)
RETURNS bigint AS 'MODULE_PATHNAME', 'partdist_tso_start_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_commit_ts()
RETURNS bigint AS 'MODULE_PATHNAME', 'partdist_tso_commit_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_status()
RETURNS text AS 'MODULE_PATHNAME', 'partdist_tso_status' LANGUAGE C STRICT;

-- TX-TSO-MVCC（T3.2）：worker 取号通路的调试/验收入口（内部 C API 为正道）。
CREATE OR REPLACE FUNCTION partdist_tso_client_start_ts()
RETURNS bigint AS 'MODULE_PATHNAME', 'partdist_tso_client_start_ts' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_tso_client_commit_ts()
RETURNS bigint AS 'MODULE_PATHNAME', 'partdist_tso_client_commit_ts' LANGUAGE C STRICT;

-- TX-TSO-MVCC（T3.5）：GlobalSafeTs——心跳续租（worker bgworker 调用）与读出。
CREATE OR REPLACE FUNCTION partdist_tso_heartbeat(node int, oldest bigint)
RETURNS bigint AS 'MODULE_PATHNAME', 'partdist_tso_heartbeat' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_global_safe_ts()
RETURNS bigint AS 'MODULE_PATHNAME', 'partdist_global_safe_ts' LANGUAGE C STRICT;

-- TX-TSO-MVCC（T4.1）：连接加入协议与 globalXID 分配器。
CREATE OR REPLACE FUNCTION partdist_join_global_txn(gxid bigint, start_ts bigint,
                                                    coord_gsid bigint DEFAULT 0)
RETURNS void AS 'MODULE_PATHNAME', 'partdist_join_global_txn' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION partdist_gxid_next()
RETURNS bigint AS 'MODULE_PATHNAME', 'partdist_gxid_next' LANGUAGE C STRICT;

-- ------------------------------------------------------------------
-- T4.5：未决 2PC 决议收敛（TX_TSO_MVCC_DESING.md §3.3/§4.2）
-- ------------------------------------------------------------------

-- 只读决议窥视：在协调组 leader 上答该 dtxid 的判决；非 leader 或无决议
-- 返回 0 行。与 dtx_status 的本质区别：**绝不写推定中止**——这是读者与
-- 清扫的问询口，"首决胜出"的 ABORT 安装只属于恢复守护（dtx_status）。
CREATE OR REPLACE FUNCTION dtx_peek(
    p_coord_gsid bigint,
    p_dtxid bigint
) RETURNS TABLE(verdict integer, commit_ts bigint)
LANGUAGE plpgsql VOLATILE AS $fn$
-- 改 plpgsql 的**唯一理由**见下面 R-P4-16：补读必须另起一条语句。
DECLARE
    v_is_leader boolean;
    v_verdict   integer;
    v_cts       bigint;
BEGIN
    -- R-P4-12：应答前先把**自己**的 apply 积压排空。新当选的 leader 可能
    -- 正是尚未 apply 该决议的成员（决议在多数派上、选举合法），不追平就
    -- 读本地表 ⇒ 谁都问不到判决（实测 90s 不收敛）。drain 幂等、非 leader
    -- 或无该组时返回 -1，不影响下面的门控语义。
    PERFORM partdist.pg_raft_group_drain_apply(p_coord_gsid);

    SELECT EXISTS (SELECT 1 FROM partdist.pg_raft_group_status() s
                    WHERE s.group_id = p_coord_gsid AND s.state = 'leader')
      INTO v_is_leader;

    -- ① 本地答案：仍受 leader 门控（follower 有 apply 滞后会答错）。
    IF v_is_leader THEN
        SELECT d.verdict::integer, d.commit_ts::bigint
          INTO v_verdict, v_cts
          FROM partdist.dtx_decision d WHERE d.dtxid = p_dtxid;
        IF FOUND THEN
            verdict := v_verdict; commit_ts := v_cts;
            RETURN NEXT; RETURN;
        END IF;
    END IF;

    -- ② 本地无答案 ⇒ 转问组内成员（R-P4-13 绕行 + R-P4-14 去门控）。
    -- 决议是一次性的正向事实（写下不再改），从谁那里读到都等价；本步只读，
    -- 不写推定中止。拉到即幂等补进本地表，下次无需再远程问。
    v_verdict := partdist.pg_raft_group_peer_decision(p_coord_gsid, p_dtxid);
    IF v_verdict IS NULL OR v_verdict NOT IN (1, 2) THEN
        -- ★ P7-N52（之三）：leader 排空 apply 后本地与组内都查无决议 ⇒ 这是**权威的"未决"**，
        --   返回一行 verdict=0；非 leader 仍返回 0 行（"不知道"）。问询方据此区分
        --   "确实还没决议"（读者按未提交）与"没问到"（读者报可重试错误）。
        -- ★ P7-N65（2026-10-05）：leader 还没提交过本任期的条目时，前任期留下的决议
        --   可能还在它日志的未提交尾巴里、谁都没 apply —— 这时说"没有"不算数，答"不知道"。
        IF v_is_leader AND partdist.pg_raft_group_leader_settled(p_coord_gsid) THEN
            verdict := 0; commit_ts := 0;
            RETURN NEXT;
        END IF;
        RETURN;
    END IF;

    -- ★ R-P4-16（2026-08-17，修的是 R-P4-13 绕行自身的缺陷）：
    -- 上一步已把决议幂等补进本地表，但**必须另起一条语句**再读才拿得到
    -- commit_ts —— 首版把补读写成同一条 SQL 里的 LEFT JOIN，那个 JOIN 用的
    -- 是语句开始时的快照，看不到函数刚插进去的行，于是答出
    -- "verdict=1 而 commit_ts=0"。实测取证：5433 答 `1@0`，而组内两个成员
    -- 都是 `1@22`。
    SELECT d.commit_ts::bigint INTO v_cts
      FROM partdist.dtx_decision d WHERE d.dtxid = p_dtxid;

    -- ★ 守卫：COMMIT 判决必须带有效 commit_ts，否则宁可答"无从判定"让调用方
    -- 重试，**绝不能把 cts=0 发出去**。参与方拿到 `1@0` 只有两种下场，实测
    -- 两种都出现过：拒收 ⇒ 落 ABORTED 并注销登记（清扫再无东西可扫，Q3 必然
    -- 60s 超时，取证见 st=3 sts=20 cts=0）；或收下 ⇒ 落一个时间戳错误的
    -- COMMITTED，破坏 SI（M1 取证见 st=2 sts=4 cts=0）。
    -- ABORT 判决与 commit_ts 无关，0 是正常值。
    IF v_verdict = 1 AND coalesce(v_cts, 0) <= 0 THEN
        RETURN;                 -- 答不全就不答
    END IF;

    verdict := v_verdict; commit_ts := coalesce(v_cts, 0);
    RETURN NEXT;
END
$fn$;

COMMENT ON FUNCTION dtx_peek(bigint, bigint) IS
    '只读决议窥视（T4.5）：leader 查无决议时返回一行 verdict=0（权威未决，P7-N52），非 leader 问不到返回 0 行；本地答案受 leader 门控（follower 有 apply 滞后会答错）；本地无答案时转问组内成员（R-P4-13/14）并另起语句补读 commit_ts（R-P4-16）；COMMIT 判决缺有效 commit_ts 时宁可返回 0 行，绝不写推定中止、绝不答半个判决。';

-- 问询核心：SPI 解析协调组 leader 地址（partition_map→node_map）+ 远程
-- dtx_peek。verdict 0=无从判定 1=COMMIT 2=ABORT。
-- 注意：单行复合返回（OUT + RETURNS record，proretset=false）。写成
-- RETURNS TABLE 会被标记为集合返回函数，与 C 侧的单行返回协议不符，
-- SQL 调用即报 "set-valued function called in context ..."（读者③首轮
-- 静默失败的根因——清扫通道直调 C 核心不经 SQL，故只有读者路径中招）。
CREATE OR REPLACE FUNCTION dtx_inquire(
    p_coord_gsid bigint,
    p_dtxid bigint,
    OUT verdict integer,
    OUT commit_ts bigint
) RETURNS record
LANGUAGE c VOLATILE AS 'MODULE_PATHNAME', 'partdist_dtx_inquire';

-- 清扫一轮本节点未决 2PC 登记（心跳工作者自连周期触发；测试可手动调），
-- 返回收敛笔数。
CREATE OR REPLACE FUNCTION dtx_pending_sweep()
    RETURNS integer LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_dtx_pending_sweep';

CREATE OR REPLACE FUNCTION dtx_pending_count()
    RETURNS integer LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_dtx_pending_count';

-- T4.5②：决议广播的接收端。按 dtxid 找本节点未决登记并幂等落分片 clog，
-- 返回落账笔数（0 = 本节点没参与 / 已收敛）。推送是优化，拉取（dtx_inquire
-- / dtx_pending_sweep）仍是兜底真相源。
CREATE OR REPLACE FUNCTION dtx_apply_decision(
    p_dtxid bigint,
    p_verdict integer,
    p_commit_ts bigint
) RETURNS integer LANGUAGE c VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_dtx_apply_decision';

-- ------------------------------------------------------------------
-- T7.23（P7-R2）：给一张分布表的**全部分片**建组 + 供副本（在协调者上调用）
-- ------------------------------------------------------------------
-- 在此之前，每个分片都要人手走一遍：在 placement 节点建组 → 等它当选 →
-- 在各副本节点建组 → 回 leader 上 provision_shard_replica。一张 32 分片的表
-- 就是上百条命令，而这几步的**顺序**是踩出来的（见 test_clog_hole_c4 的
-- P7-T1：先铺副本再建组、或者建完发现主不对就推倒重来，都会让选举把 Citus
-- placement 迁到一个没有数据的节点上）。本函数把那条已验证的顺序固化下来：
--
--   ① placement 节点先建组、领先几秒（有数据的节点先当主）；
--   ② 再让副本节点入组，等 placement 当选 —— 落到副本上就只拆这个组重来；
--   ③ 再确认组主仍在 placement，由它逐个 provision_shard_replica（它自己会
--      发物理基线、建壳表、配 locmap、arm 回放）。
--
-- 单个分片失败不拖累其余：逐分片返回一行状态，调用方据此重试失败的那几个。
-- 副本节点按 pg_dist_node 里 worker 的顺序、从 placement 之后轮转挑选，
-- 让副本在 worker 之间摊开，而不是全挤到头几个节点上。
CREATE OR REPLACE FUNCTION raft_replicate_table_shards(
    p_table    regclass,
    p_replicas integer DEFAULT 1,
    p_leader_timeout_s integer DEFAULT 60
) RETURNS TABLE(shardid bigint, leader_port integer, replica_ports integer[], status text)
LANGUAGE plpgsql VOLATILE
SET search_path = partdist, pg_catalog
AS $fn$
DECLARE
    v_workers   record;
    v_hosts     text[]    := '{}';
    v_ports     integer[] := '{}';
    v_nodeids   integer[] := '{}';
    v_n         integer;
    v_shard     record;
    v_pidx      integer;
    v_members   integer[];
    v_rports    integer[];
    v_rhosts    text[];
    v_i         integer;
    v_k         integer;
    v_ok        boolean;
    v_res       text;
    v_state     text;
    v_t0        timestamptz;
    v_fail      text;
    v_round     integer;
BEGIN
    IF p_replicas < 1 THEN
        RAISE EXCEPTION 'raft_replicate_table_shards: p_replicas 必须 >= 1';
    END IF;

    -- 全部 primary worker，以及它们各自的 raft 节点号（逐个问，不猜映射）
    FOR v_workers IN
        SELECT n.nodename, n.nodeport
          FROM pg_catalog.pg_dist_node n
         WHERE n.noderole = 'primary' AND n.groupid <> 0 AND n.isactive
         ORDER BY n.groupid
    LOOP
        SELECT r.success, r.result INTO v_ok, v_res
          FROM pg_catalog.master_run_on_worker(ARRAY[v_workers.nodename],
                                               ARRAY[v_workers.nodeport],
                                               ARRAY['SHOW pg_raft.node_id'], false) r;
        IF NOT v_ok OR v_res !~ '^[0-9]+$' THEN
            RAISE EXCEPTION 'raft_replicate_table_shards: 取不到 %:% 的 pg_raft.node_id（%）',
                v_workers.nodename, v_workers.nodeport, v_res;
        END IF;
        v_hosts   := v_hosts   || v_workers.nodename;
        v_ports   := v_ports   || v_workers.nodeport;
        v_nodeids := v_nodeids || v_res::integer;

        -- ⓪ 分片身份登记（shard_identity）：provision_shard_replica 第一步就按它
        --   认"本节点是不是这个分片的主"，建表之后没人自动填（P7-V4 同源的手工缺口）。
        --   首版漏了这步，4 个分片全报「本节点没有分片」。幂等，每个 worker 跑一次。
        SELECT r.success, r.result INTO v_ok, v_res
          FROM pg_catalog.master_run_on_worker(ARRAY[v_workers.nodename],
                                               ARRAY[v_workers.nodeport],
                                               ARRAY['SELECT partdist.rebuild_shard_identity()'], false) r;
        IF NOT v_ok THEN
            RAISE EXCEPTION 'raft_replicate_table_shards: %:% 上重建分片身份失败（%）',
                v_workers.nodename, v_workers.nodeport, v_res;
        END IF;
    END LOOP;
    v_n := coalesce(array_length(v_ports, 1), 0);

    IF p_replicas > v_n - 1 THEN
        RAISE EXCEPTION 'raft_replicate_table_shards: 要 % 个副本，但除 placement 外只有 % 个 worker',
            p_replicas, v_n - 1;
    END IF;

    FOR v_shard IN
        SELECT s.shardid AS sid, n.nodename AS phost, n.nodeport AS pport
          FROM pg_catalog.pg_dist_shard s
          JOIN pg_catalog.pg_dist_placement p ON p.shardid = s.shardid
          JOIN pg_catalog.pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
         WHERE s.logicalrelid = p_table
         ORDER BY s.shardid
    LOOP
        shardid := v_shard.sid;
        leader_port := v_shard.pport;
        v_fail := NULL;

        v_pidx := array_position(v_ports, v_shard.pport);
        IF v_pidx IS NULL THEN
            replica_ports := NULL; status := 'placement 节点不在 worker 列表里';
            RETURN NEXT; CONTINUE;
        END IF;

        -- 副本：从 placement 之后轮转挑 p_replicas 个
        v_rports := '{}'; v_rhosts := '{}'; v_members := ARRAY[v_nodeids[v_pidx]];
        FOR v_k IN 1..p_replicas LOOP
            v_i := ((v_pidx - 1 + v_k) % v_n) + 1;
            v_rports  := v_rports  || v_ports[v_i];
            v_rhosts  := v_rhosts  || v_hosts[v_i];
            v_members := v_members || v_nodeids[v_i];
        END LOOP;
        replica_ports := v_rports;

        -- ①② placement 先建组、领先几秒，再让副本入组，然后等 placement 当选。
        --   不能"等 placement 当选之后再让副本入组"：配置里已经有副本，单独一个
        --   placement 凑不够多数派，永远选不上（首版就这么写的，逐分片超时）。
        --   选举落到副本上（副本还没有数据）⇒ 只拆**这个组**重来，不能用
        --   pg_raft_group_reset（它清的是节点上的全部数据组，会把前面已经
        --   供好的分片一起拆掉）。
        v_state := 'none';
        FOR v_round IN 1..3 LOOP
            SELECT r.success, r.result INTO v_ok, v_res
              FROM pg_catalog.master_run_on_worker(ARRAY[v_shard.phost], ARRAY[v_shard.pport],
                   ARRAY[format('SELECT partdist.pg_raft_group_create(%s, %L::integer[])',
                                v_shard.sid, v_members)], false) r;
            IF NOT v_ok OR v_res <> 't' THEN
                v_fail := '在 placement 上建组失败：' || v_res; EXIT;
            END IF;
            PERFORM pg_catalog.pg_sleep(3);

            FOR v_k IN 1..p_replicas LOOP
                SELECT r.success, r.result INTO v_ok, v_res
                  FROM pg_catalog.master_run_on_worker(ARRAY[v_rhosts[v_k]], ARRAY[v_rports[v_k]],
                       ARRAY[format('SELECT partdist.pg_raft_group_create(%s, %L::integer[])',
                                    v_shard.sid, v_members)], false) r;
                IF NOT v_ok OR v_res <> 't' THEN
                    v_fail := format('副本 :%s 入组失败：%s', v_rports[v_k], v_res); EXIT;
                END IF;
            END LOOP;
            EXIT WHEN v_fail IS NOT NULL;

            v_t0 := clock_timestamp();
            LOOP
                SELECT r.result INTO v_state
                  FROM pg_catalog.master_run_on_worker(ARRAY[v_shard.phost], ARRAY[v_shard.pport],
                       ARRAY[format('SELECT coalesce((SELECT state FROM partdist.pg_raft_group_status() '
                                    'WHERE group_id = %s), %L)', v_shard.sid, 'none')], false) r;
                EXIT WHEN v_state = 'leader'
                       OR clock_timestamp() - v_t0 > make_interval(secs => greatest(p_leader_timeout_s / 3, 10));
                PERFORM pg_catalog.pg_sleep(1);
            END LOOP;
            EXIT WHEN v_state = 'leader';

            RAISE NOTICE 'raft_replicate_table_shards: 分片 % 第 % 轮组主没落在 placement（state=%），拆组重来',
                v_shard.sid, v_round, v_state;
            PERFORM pg_catalog.master_run_on_worker(v_rhosts || v_shard.phost, v_rports || v_shard.pport,
                    array_fill(format('SELECT partdist.pg_raft_group_drop(%s)', v_shard.sid),
                               ARRAY[p_replicas + 1]), true);
            PERFORM pg_catalog.pg_sleep(2);
        END LOOP;
        IF v_fail IS NULL AND v_state <> 'leader' THEN
            v_fail := format('三轮都没让 placement 当选（最后 state=%s）', v_state);
        END IF;
        IF v_fail IS NOT NULL THEN
            status := v_fail; RETURN NEXT; CONTINUE;
        END IF;

        -- ③ 由 leader 逐个供给。每次供给前**再确认一次**组主仍在 placement：
        --   基线发射走 raft 写路径，写栅栏看的是"此刻是不是 leader"。
        FOR v_k IN 1..p_replicas LOOP
            v_t0 := clock_timestamp();
            LOOP
                SELECT r.result INTO v_state
                  FROM pg_catalog.master_run_on_worker(ARRAY[v_shard.phost], ARRAY[v_shard.pport],
                       ARRAY[format('SELECT coalesce((SELECT state FROM partdist.pg_raft_group_status() '
                                    'WHERE group_id = %s), %L)', v_shard.sid, 'none')], false) r;
                EXIT WHEN v_state = 'leader'
                       OR clock_timestamp() - v_t0 > make_interval(secs => p_leader_timeout_s);
                PERFORM pg_catalog.pg_sleep(1);
            END LOOP;
            IF v_state <> 'leader' THEN
                v_fail := format('供给 :%s 前组主已离开 placement（state=%s）', v_rports[v_k], v_state); EXIT;
            END IF;
            SELECT r.success, r.result INTO v_ok, v_res
              FROM pg_catalog.master_run_on_worker(ARRAY[v_shard.phost], ARRAY[v_shard.pport],
                   ARRAY[format('SELECT partdist.provision_shard_replica(%s, %s)',
                                v_shard.sid, v_nodeids[array_position(v_ports, v_rports[v_k])])], false) r;
            IF NOT v_ok OR v_res NOT LIKE 'shard=%' THEN
                v_fail := format('向 :%s 供给失败：%s', v_rports[v_k], v_res); EXIT;
            END IF;
        END LOOP;

        status := coalesce(v_fail, 'ok');
        RETURN NEXT;
    END LOOP;
END;
$fn$;
-- ------------------------------------------------------------------
-- ★ 2026-10-03：raft_replicate_table_shards 的并行版（上百个分片组用）
-- ------------------------------------------------------------------
-- raft_replicate_table_shards 逐分片串行：每片"建组 → 领先 3 s → 副本入组 → 等当选 →
-- 逐个供副本"，实测约 28 s/片，128 个分片就是一个小时。本函数沿用同一套已验证的顺序与
-- 同一条成员轮转规则（placement 之后按 groupid 轮转挑 p_replicas 个），但**所有分片一起推进**：
--
--   ⓪ 各 worker 重建分片身份（并行）；
--   ① 每台 worker 一条命令，把它当 placement 的那些组一次建完（4 台并行）；
--   ② 等 3 s（placement 领先），再每台一条命令让它当副本的那些组入组（并行）；
--   ③ 轮询各 worker 的组状态，等每个分片的 placement 当选；超时没落到 placement 的
--      分片拆组重来（最多 p_rounds 轮）—— 与原函数同一处置；
--   ④ 供副本：每台 leader 一轮发一个 provision_shard_replica，4 台并行（同一台上仍串行，
--      避免同一 leader 上多个基线互相抢复制认领位）；失败的最后再补供一遍。
--
-- 每个 provision 仍是独立事务（与原函数一致）。返回每个分片一行状态。
DROP FUNCTION IF EXISTS raft_replicate_table_shards_bulk(regclass, integer, integer, integer);
-- p_shards：只处理这些分片（NULL = 全部）。2026-10-04：上百个分片一次全上会把控制面打满
-- （登记风暴，P7-N58），调用方可以按批（如每批 32 个）分几次调。
CREATE OR REPLACE FUNCTION raft_replicate_table_shards_bulk(
    p_table    regclass,
    p_replicas integer DEFAULT 2,
    p_elect_timeout_s integer DEFAULT 180,
    p_rounds   integer DEFAULT 3,
    p_shards   bigint[] DEFAULT NULL
) RETURNS TABLE(shardid bigint, leader_port integer, replica_ports integer[], status text)
LANGUAGE plpgsql VOLATILE
SET search_path = partdist, pg_catalog
AS $fn$
DECLARE
    w           record;
    v_hosts     text[]    := '{}';
    v_ports     integer[] := '{}';
    v_nids      integer[] := '{}';
    v_n         integer;
    v_cmds      text[];
    v_h         text[];
    v_p         integer[];
    v_i         integer;
    v_k         integer;
    v_round     integer;
    v_t0        timestamptz;
    v_last_note timestamptz;
    v_total     integer;
    v_done      integer;
    v_pending   integer;
    v_res_cmd   text;
    v_armed     integer;
    r           record;
BEGIN
    IF p_replicas < 1 THEN
        RAISE EXCEPTION 'raft_replicate_table_shards_bulk: p_replicas 必须 >= 1';
    END IF;

    -- ⓪ worker 清单与 raft 节点号
    FOR w IN SELECT n.nodename, n.nodeport FROM pg_catalog.pg_dist_node n
              WHERE n.noderole = 'primary' AND n.groupid <> 0 AND n.isactive ORDER BY n.groupid
    LOOP
        v_hosts := v_hosts || w.nodename::text;
        v_ports := v_ports || w.nodeport;
    END LOOP;
    v_n := coalesce(array_length(v_ports, 1), 0);
    IF p_replicas > v_n - 1 THEN
        RAISE EXCEPTION 'raft_replicate_table_shards_bulk: 要 % 个副本，但除 placement 外只有 % 个 worker',
            p_replicas, v_n - 1;
    END IF;
    v_cmds := array_fill('SHOW pg_raft.node_id'::text, ARRAY[v_n]);
    FOR r IN SELECT * FROM pg_catalog.master_run_on_worker(v_hosts, v_ports, v_cmds, true) LOOP
        IF NOT r.success OR r.result !~ '^[0-9]+$' THEN
            RAISE EXCEPTION 'raft_replicate_table_shards_bulk: 取不到 %:% 的 pg_raft.node_id（%）',
                r.node_name, r.node_port, r.result;
        END IF;
        v_nids[array_position(v_ports, r.node_port)] := r.result::integer;
    END LOOP;
    v_cmds := array_fill('SELECT partdist.rebuild_shard_identity()::text'::text, ARRAY[v_n]);
    FOR r IN SELECT * FROM pg_catalog.master_run_on_worker(v_hosts, v_ports, v_cmds, true) LOOP
        IF NOT r.success THEN
            RAISE EXCEPTION 'raft_replicate_table_shards_bulk: %:% 上重建分片身份失败（%）',
                r.node_name, r.node_port, r.result;
        END IF;
    END LOOP;

    -- 每个分片：placement 下标、成员（raft 节点号）、副本下标
    DROP TABLE IF EXISTS pg_temp._rtsb;
    CREATE TEMP TABLE _rtsb (sid bigint PRIMARY KEY, pidx int, members int[], ridx int[],
                             elected bool DEFAULT false, prov_ok int DEFAULT 0, st text);
    INSERT INTO _rtsb (sid, pidx, members, ridx)
    SELECT s.shardid, x.pidx,
           ARRAY[v_nids[x.pidx]] || ARRAY(SELECT v_nids[((x.pidx - 1 + k) % v_n) + 1] FROM generate_series(1, p_replicas) k ORDER BY k),
           ARRAY(SELECT ((x.pidx - 1 + k) % v_n) + 1 FROM generate_series(1, p_replicas) k ORDER BY k)
      FROM pg_catalog.pg_dist_shard s
      JOIN pg_catalog.pg_dist_placement p ON p.shardid = s.shardid
      JOIN pg_catalog.pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
      CROSS JOIN LATERAL (SELECT array_position(v_ports, n.nodeport) AS pidx) x
     WHERE s.logicalrelid = p_table
       AND (p_shards IS NULL OR s.shardid = ANY (p_shards));
    SELECT count(*) INTO v_total FROM _rtsb;
    IF v_total = 0 THEN
        RAISE EXCEPTION 'raft_replicate_table_shards_bulk: % 没有任何分片', p_table;
    END IF;
    UPDATE _rtsb SET st = 'placement 节点不在 worker 列表里' WHERE pidx IS NULL;

    FOR v_round IN 1..p_rounds LOOP
        -- ① placement 建组（每台一条批量命令，并行）
        v_cmds := '{}'; v_h := '{}'; v_p := '{}';
        FOR v_i IN 1..v_n LOOP
            SELECT 'SELECT count(*) FILTER (WHERE partdist.pg_raft_group_create(g, m))::text FROM (VALUES '
                   || string_agg(format('(%s::bigint, %L::integer[])', sid, members), ',') || ') v(g, m)'
              INTO v_res_cmd FROM _rtsb WHERE pidx = v_i AND NOT elected AND st IS NULL;
            IF v_res_cmd IS NOT NULL THEN
                v_cmds := v_cmds || v_res_cmd; v_h := v_h || v_hosts[v_i]; v_p := v_p || v_ports[v_i];
            END IF;
        END LOOP;
        EXIT WHEN coalesce(array_length(v_cmds, 1), 0) = 0;
        PERFORM * FROM pg_catalog.master_run_on_worker(v_h, v_p, v_cmds, true);
        PERFORM pg_catalog.pg_sleep(1);

        -- ③ 等 placement 当选（轮询各 worker 的 leader 组清单）
        v_t0 := clock_timestamp(); v_last_note := v_t0;
        LOOP
            v_cmds := array_fill('SELECT coalesce(string_agg(group_id::text, '',''), '''') FROM partdist.pg_raft_group_status() WHERE state = ''leader'' AND group_id <> 0'::text, ARRAY[v_n]);
            FOR r IN SELECT * FROM pg_catalog.master_run_on_worker(v_hosts, v_ports, v_cmds, true) LOOP
                IF r.success AND r.result <> '' THEN
                    UPDATE _rtsb SET elected = true
                     WHERE NOT elected AND pidx = array_position(v_ports, r.node_port)
                       AND sid = ANY (string_to_array(r.result, ',')::bigint[]);
                END IF;
            END LOOP;
            SELECT count(*) FILTER (WHERE elected) INTO v_done FROM _rtsb;
            EXIT WHEN v_done + (SELECT count(*) FROM _rtsb WHERE st IS NOT NULL AND NOT elected) >= v_total;
            EXIT WHEN clock_timestamp() - v_t0 > make_interval(secs => p_elect_timeout_s);
            IF clock_timestamp() - v_last_note > interval '10 seconds' THEN
                RAISE NOTICE 'raft_replicate_table_shards_bulk: 第 % 轮，已有 %/% 个分片由 placement 当选', v_round, v_done, v_total;
                v_last_note := clock_timestamp();
            END IF;
            PERFORM pg_catalog.pg_sleep(2);
        END LOOP;
        SELECT count(*) INTO v_pending FROM _rtsb WHERE NOT elected AND st IS NULL;
        EXIT WHEN v_pending = 0;
        RAISE NOTICE 'raft_replicate_table_shards_bulk: 第 % 轮结束仍有 % 个分片的主没落在 placement，拆组重来', v_round, v_pending;
        -- 拆这些组（在全部成员上），下一轮重建
        v_cmds := '{}'; v_h := '{}'; v_p := '{}';
        FOR v_i IN 1..v_n LOOP
            SELECT 'SELECT count(partdist.pg_raft_group_drop(g))::text FROM unnest(ARRAY['
                   || string_agg(sid::text, ',') || ']::bigint[]) g'
              INTO v_res_cmd FROM _rtsb WHERE NOT elected AND st IS NULL AND (pidx = v_i OR v_i = ANY (ridx));
            IF v_res_cmd IS NOT NULL THEN
                v_cmds := v_cmds || v_res_cmd; v_h := v_h || v_hosts[v_i]; v_p := v_p || v_ports[v_i];
            END IF;
        END LOOP;
        IF coalesce(array_length(v_cmds, 1), 0) > 0 THEN
            PERFORM * FROM pg_catalog.master_run_on_worker(v_h, v_p, v_cmds, true);
        END IF;
        PERFORM pg_catalog.pg_sleep(2);
    END LOOP;
    UPDATE _rtsb SET st = format('%s 轮后组主仍未落在 placement', p_rounds) WHERE NOT elected AND st IS NULL;

    -- ② 副本入组（每台一条批量命令，并行）—— **在 placement 当选之后**才做。
    --    2026-10-03：原来建完 placement 组 sleep 3 s 就入组；选举超时 15 s 时 placement 还没来得及竞选，
    --    已知成员集的副本可能先超时、先当选（它没有数据），之后要靠升主前置 -1 让位 —— 每节点的升主前置
    --    是串行的，几十个组排队一百多秒，供副本那一步正撞上"主在没数据的成员上"（实测 32 个分片失败）。
    --    入组之前副本那边只有 hearsay 建出的组：照常投票、但成员集未知不会主动竞选。
    v_cmds := '{}'; v_h := '{}'; v_p := '{}';
    FOR v_i IN 1..v_n LOOP
        SELECT 'SELECT count(*) FILTER (WHERE partdist.pg_raft_group_create(g, m))::text FROM (VALUES '
               || string_agg(format('(%s::bigint, %L::integer[])', sid, members), ',') || ') v(g, m)'
          INTO v_res_cmd FROM _rtsb WHERE v_i = ANY (ridx) AND elected AND st IS NULL;
        IF v_res_cmd IS NOT NULL THEN
            v_cmds := v_cmds || v_res_cmd; v_h := v_h || v_hosts[v_i]; v_p := v_p || v_ports[v_i];
        END IF;
    END LOOP;
    IF coalesce(array_length(v_cmds, 1), 0) > 0 THEN
        PERFORM * FROM pg_catalog.master_run_on_worker(v_h, v_p, v_cmds, true);
    END IF;


    -- ④ 供副本。每一遍先**重新读**每个组当前的主、以及哪些成员已经有 armed 的回放槽位，
    --    只给"不是当前主、又还没 armed"的成员供，从**当前主**发起：
    --    CPU 饱和时组主可能在建组之后漂到别的成员（2026-10-03 实测 128 组时 44 个分片因此失败，
    --    "provision_shard_replica: 本节点不是分片的主"）。当选的成员一定持有数据（没有数据的成员
    --    当选会被升主前置拒绝并让位），所以从它供是对的；被替下来的原主同样要重新供成副本。
    --    第 k 轮每台 leader 发它的第 k 个（各台并行，同台串行），最多 p_rounds 遍。
    DROP TABLE IF EXISTS pg_temp._rtsb_prov;
    CREATE TEMP TABLE _rtsb_prov (sid bigint, lidx int, target int, seq int, result text, ok bool DEFAULT false);
    ALTER TABLE _rtsb ADD COLUMN lidx int;
    ALTER TABLE _rtsb ADD COLUMN armed int[] DEFAULT '{}';
    ALTER TABLE _rtsb ADD COLUMN prov_err text;
    -- 时间预算按"有没有进展"算：p_elect_timeout_s * 5 秒内没有任何一个副本新 armed 才放弃
    -- （组主刚换时要等它向控制面登记完才能供；128 组的登记风暴里登记本身可能排十几分钟，见 P7-N58）
    -- （128 组在 2 vCPU 上供 256 个副本要十来分钟，按总时长算会在进行中途收手）。
    v_t0 := clock_timestamp();
    v_armed := -1;
    FOR v_round IN 1..1000 LOOP
        EXIT WHEN clock_timestamp() - v_t0 > make_interval(secs => p_elect_timeout_s * 5);
        -- 当前主（下标）
        UPDATE _rtsb SET lidx = NULL;
        v_cmds := array_fill('SELECT coalesce(string_agg(group_id::text, '',''), '''') FROM partdist.pg_raft_group_status() WHERE state = ''leader'' AND group_id <> 0'::text, ARRAY[v_n]);
        FOR r IN SELECT * FROM pg_catalog.master_run_on_worker(v_hosts, v_ports, v_cmds, true) LOOP
            IF r.success AND r.result <> '' THEN
                UPDATE _rtsb SET lidx = array_position(v_ports, r.node_port)
                 WHERE sid = ANY (string_to_array(r.result, ',')::bigint[]);
            END IF;
        END LOOP;
        -- 已 armed 的副本（raft 节点号）；有新增就重置进展计时
        UPDATE _rtsb SET armed = '{}';
        v_cmds := array_fill('SELECT coalesce(string_agg(partdist.global_id_for_partition(shard)::text, '',''), '''') FROM partdist.replay_status() WHERE armed'::text, ARRAY[v_n]);
        FOR r IN SELECT * FROM pg_catalog.master_run_on_worker(v_hosts, v_ports, v_cmds, true) LOOP
            IF r.success AND r.result <> '' THEN
                UPDATE _rtsb SET armed = armed || v_nids[array_position(v_ports, r.node_port)]
                 WHERE sid = ANY (string_to_array(r.result, ',')::bigint[]);
            END IF;
        END LOOP;

        IF (SELECT coalesce(sum(cardinality(armed)), 0) FROM _rtsb) > v_armed THEN
            v_armed := (SELECT coalesce(sum(cardinality(armed)), 0) FROM _rtsb);
            v_t0 := clock_timestamp();
        END IF;
        TRUNCATE _rtsb_prov;
        -- 只从**有数据**的主发起：placement 本身，或回放槽位已 armed 的成员。没数据的成员当选（入组那一刻
        -- 它的"最近一次收到 leader 心跳"被清零，预选拦不住）会在升主前置里让位，这一遍先跳过、等它让位。
        INSERT INTO _rtsb_prov (sid, lidx, target)
        SELECT b.sid, b.lidx, m
          FROM _rtsb b CROSS JOIN LATERAL unnest(b.members) m
         WHERE b.elected AND b.st IS NULL AND b.lidx IS NOT NULL
           AND (b.lidx = b.pidx OR v_nids[b.lidx] = ANY (b.armed)
                OR EXISTS (SELECT 1 FROM partdist.partition_map pm      -- 已登记 = 升主前置验过有数据
                            WHERE pm.partition_id = b.sid AND pm.primary_node = v_nids[b.lidx]))
           AND m <> v_nids[b.lidx] AND NOT (m = ANY (b.armed));
        IF NOT EXISTS (SELECT 1 FROM _rtsb_prov) THEN
            -- 没有可发的：要么全部供完，要么剩下的都在等"没数据的主"让位
            EXIT WHEN NOT EXISTS (
                SELECT 1 FROM _rtsb b CROSS JOIN LATERAL unnest(b.members) m
                 WHERE b.elected AND b.st IS NULL
                   AND (b.lidx IS NULL OR m <> v_nids[b.lidx]) AND NOT (m = ANY (b.armed)));
            PERFORM pg_catalog.pg_sleep(5);
            CONTINUE;
        END IF;
        IF v_round > 1 THEN
            RAISE NOTICE 'raft_replicate_table_shards_bulk: 第 % 遍补供 % 个副本（含组主漂移后从新主重供）',
                v_round, (SELECT count(*) FROM _rtsb_prov);
        END IF;
        UPDATE _rtsb_prov q SET seq = x.rn
          FROM (SELECT sid, target, row_number() OVER (PARTITION BY lidx ORDER BY sid, target) rn FROM _rtsb_prov) x
         WHERE q.sid = x.sid AND q.target = x.target;

        FOR v_k IN 1..coalesce((SELECT max(seq) FROM _rtsb_prov), 0) LOOP
            v_cmds := '{}'; v_h := '{}'; v_p := '{}';
            FOR r IN SELECT sid, lidx, target FROM _rtsb_prov WHERE seq = v_k LOOP
                v_cmds := v_cmds || format('SELECT partdist.provision_shard_replica(%s::bigint, %s)', r.sid, r.target);
                v_h := v_h || v_hosts[r.lidx]; v_p := v_p || v_ports[r.lidx];
            END LOOP;
            CONTINUE WHEN coalesce(array_length(v_cmds, 1), 0) = 0;
            FOR r IN SELECT * FROM pg_catalog.master_run_on_worker(v_h, v_p, v_cmds, true) LOOP
                -- 同一台一轮只有一条，用端口对回去
                UPDATE _rtsb_prov SET result = r.result, ok = (r.success AND r.result LIKE 'shard=%')
                 WHERE seq = v_k AND lidx = array_position(v_ports, r.node_port);
            END LOOP;
        END LOOP;
        UPDATE _rtsb b SET prov_err = (SELECT string_agg(format(':%s→节点%s %s', v_ports[q.lidx], q.target, left(q.result, 120)), '；')
                                         FROM _rtsb_prov q WHERE q.sid = b.sid AND NOT q.ok);
        EXIT WHEN NOT EXISTS (SELECT 1 FROM _rtsb_prov WHERE NOT ok);
        PERFORM pg_catalog.pg_sleep(2);
    END LOOP;

    FOR r IN SELECT b.* FROM _rtsb b ORDER BY b.sid LOOP
        shardid := r.sid;
        leader_port := v_ports[coalesce(r.lidx, r.pidx)];
        replica_ports := ARRAY(SELECT v_ports[array_position(v_nids, m)] FROM unnest(r.members) m
                                WHERE m <> v_nids[coalesce(r.lidx, r.pidx)]);
        status := coalesce(r.st,
                           CASE WHEN r.lidx IS NULL THEN '组当前无主'
                                WHEN EXISTS (SELECT 1 FROM unnest(r.members) m
                                              WHERE m <> v_nids[r.lidx] AND NOT (m = ANY (r.armed)))
                                     AND NOT (r.lidx = r.pidx OR v_nids[r.lidx] = ANY (r.armed)
                                              OR EXISTS (SELECT 1 FROM partdist.partition_map pm
                                                          WHERE pm.partition_id = r.sid AND pm.primary_node = v_nids[r.lidx]))
                                     THEN '组主落在没有数据的成员上，等它让位超时'
                                WHEN EXISTS (SELECT 1 FROM _rtsb_prov q WHERE q.sid = r.sid AND NOT q.ok)
                                     THEN '供副本失败：' || coalesce(r.prov_err, '?')
                                ELSE 'ok' END);
        RETURN NEXT;
    END LOOP;

    -- ★ P7-N109：两张临时表用完就删。不删的话它们一直留在调用者的会话里
    -- （SHARD KEY 建表就是在用户自己的会话里调本函数），删完业务表再 \dt 会列出 pg_temp_N._rtsb / _rtsb_prov。
    -- 返回的行已经由 RETURN NEXT 收进结果集，这里删表不影响调用方。
    DROP TABLE IF EXISTS pg_temp._rtsb_prov, pg_temp._rtsb;
END
$fn$;

-- 2026-10-03（P7-N53）：路由对账。把 Citus 路由（pg_dist_placement）拨到控制面登记（partition_map）的主上。
-- 正常情况下每条主权登记 apply 时就拨了；但控制面快照安装那一刻偶有漏拨（实测 128 组建组时 2 个分片
-- 漏拨、之后再无人纠正：打标打到了已经是副本壳表的旧节点上）。由 topology monitor 每轮追平时顺带调用。
CREATE OR REPLACE FUNCTION reconcile_routing() RETURNS integer
LANGUAGE plpgsql VOLATILE
SET search_path = partdist, pg_catalog
AS $fn$
DECLARE n integer := 0;
BEGIN
    IF to_regclass('pg_catalog.pg_dist_placement') IS NULL THEN
        RETURN 0;
    END IF;
    WITH fix AS (
        UPDATE pg_catalog.pg_dist_placement p SET groupid = dn.groupid
          FROM partdist.partition_map pm, partdist.node_map m, pg_catalog.pg_dist_node dn
         WHERE pm.partition_id = p.shardid AND m.node_id = pm.primary_node
           AND dn.nodeport = m.port AND dn.noderole = 'primary'
           AND p.groupid <> dn.groupid
           AND (SELECT count(*) FROM pg_catalog.pg_dist_placement q WHERE q.shardid = p.shardid) = 1
        RETURNING p.shardid, dn.nodeport)
    SELECT count(*) INTO n FROM fix;
    IF n > 0 THEN
        RAISE LOG 'pg_partdist: 路由对账把 % 个分片的 pg_dist_placement 拨到了控制面登记的主上', n;
    END IF;
    RETURN n;
END
$fn$;

COMMENT ON FUNCTION raft_replicate_table_shards_bulk(regclass, integer, integer, integer, bigint[]) IS
    '2026-10-03：raft_replicate_table_shards 的并行版 —— 同样的顺序与成员轮转（副本因子 = 1 + p_replicas），'
    '所有分片一起推进：建组/入组每台一条批量命令、4 台并行；供副本每台 leader 一轮一个、4 台并行。'
    '上百个分片组时用它（串行版约 28 s/片）。';


COMMENT ON FUNCTION raft_replicate_table_shards(regclass, integer, integer) IS
    'T7.23/P7-R2：在协调者上一条命令给分布表的全部分片建 Raft 组并供副本。顺序固化为「placement 先建组并当选 → 副本入组 → leader 逐个 provision_shard_replica」；逐分片返回状态，单个失败不拖累其余。';

-- ------------------------------------------------------------------
-- T7.25（P7-R4）：DDL 自动跟随（副本侧）
-- ------------------------------------------------------------------
CREATE OR REPLACE FUNCTION replay_pending_follow(
    p_shard OID,
    OUT ddl_hint TEXT,
    OUT roles INTEGER[], OUT ords INTEGER[],
    OUT spcs OID[], OUT dbs OID[], OUT relnums OID[]
) RETURNS record LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_replay_pending_follow';

CREATE OR REPLACE FUNCTION replay_pending_follow_clear(p_shard OID)
    RETURNS INTEGER LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_replay_pending_follow_clear';

CREATE OR REPLACE FUNCTION replay_pending_base(p_shard OID)
    RETURNS BIGINT LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'partdist_replay_pending_base';

COMMENT ON FUNCTION replay_pending_base(OID) IS
    'T7.25：副本撞结构栅栏时记下的重配起效游标 = max(配对起效游标, 已落 checkpoint 游标)。补齐结构后重跑 replay_set_locmap() 须把它作第 7 参传入 —— 缺省的 0 是"从流起点开始"的断言，本地非空即被拒。无待办时为 NULL。';

CREATE OR REPLACE FUNCTION indexdef_string(p_index OID)
    RETURNS TEXT LANGUAGE c STRICT STABLE
    AS 'MODULE_PATHNAME', 'partdist_indexdef_string';

-- 对账：让本地壳表的索引集合与 leader 的定义清单一致（按去掉索引名之后的
-- 定义文本比较，多重集语义），再按 leader 的新 fileset 重配 locmap。
--
-- ★ 顺序必须是"先删后建、按清单顺序建"：fileset 的 ord 是 RelationGetIndexList
--   的 OID 升序，新建的索引拿到的 OID 天然大于所有幸存者，于是按清单顺序建出来的
--   索引与 leader 一侧落在同样的 ord 上 —— 这是配对能按 (role, ord) 对上的前提。
-- ★ 删掉的若是约束背后的索引（主键/唯一约束），必须 DROP CONSTRAINT，直接
--   DROP INDEX 会被拒。
CREATE OR REPLACE FUNCTION replay_auto_follow(p_shard OID)
RETURNS TEXT LANGUAGE plpgsql VOLATILE
SET search_path = partdist, pg_catalog
AS $fn$
DECLARE
    v        record;
    v_tbl    regclass;
    v_want   text[];
    v_norm   text[];
    v_used   boolean[];
    v_have   record;
    v_n      text;
    v_pos    integer;
    v_i      integer;
    v_drop   integer := 0;
    v_make   integer := 0;
    v_pairs  integer;
    v_have_norm text[];
    v_base   bigint;
BEGIN
    SELECT * INTO v FROM partdist.replay_pending_follow(p_shard);
    IF v.roles IS NULL THEN
        RETURN 'nothing';
    END IF;
    IF v.ddl_hint IS NULL THEN
        RETURN 'no-hint：栅栏不是由带结构提示的 DDL 引起的，需人工补结构';
    END IF;

    v_tbl  := p_shard::regclass;
    v_want := CASE WHEN v.ddl_hint = '' THEN '{}'::text[]
                   ELSE string_to_array(v.ddl_hint, E'\n') END;
    v_norm := '{}'; v_used := '{}';
    FOR v_i IN 1..coalesce(array_length(v_want, 1), 0) LOOP
        v_norm := v_norm || regexp_replace(v_want[v_i], '^(CREATE (UNIQUE )?INDEX )\S+ ON ', '\1ON ');
        v_used := v_used || false;
    END LOOP;

    PERFORM set_config('citus.enable_ddl_propagation', 'off', true);

    -- ① 删：本地有、清单里没有（多重集：每条清单只抵消一个本地索引）
    FOR v_have IN
        SELECT i.indexrelid,
               partdist.indexdef_string(i.indexrelid) AS def,
               c.conname
          FROM pg_catalog.pg_index i
          LEFT JOIN pg_catalog.pg_constraint c
                 ON c.conindid = i.indexrelid AND c.conrelid = i.indrelid
         WHERE i.indrelid = p_shard
         ORDER BY i.indexrelid
    LOOP
        v_n := regexp_replace(v_have.def, '^(CREATE (UNIQUE )?INDEX )\S+ ON ', '\1ON ');
        v_pos := NULL;
        FOR v_i IN 1..coalesce(array_length(v_norm, 1), 0) LOOP
            IF NOT v_used[v_i] AND v_norm[v_i] = v_n THEN
                v_pos := v_i; EXIT;
            END IF;
        END LOOP;
        IF v_pos IS NULL THEN
            IF v_have.conname IS NOT NULL THEN
                EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', v_tbl, v_have.conname);
            ELSE
                EXECUTE format('DROP INDEX %s', v_have.indexrelid::regclass);
            END IF;
            v_drop := v_drop + 1;
        ELSE
            v_used[v_pos] := true;
        END IF;
    END LOOP;

    -- ② 建：清单里有、本地没有，按清单顺序
    FOR v_i IN 1..coalesce(array_length(v_want, 1), 0) LOOP
        IF NOT v_used[v_i] THEN
            EXECUTE v_want[v_i];
            v_make := v_make + 1;
        END IF;
    END LOOP;

    -- ③ 核对：locmap 按 (role, ord) 配对，ord 是 RelationGetIndexList 的 OID 顺序。
    --   对账只保证"集合相等"，**顺序**相等靠的是"新建的 OID 一定比留下的大、
    --   且 leader 那边同样如此"。OID 回卷、或者 leader 与本地的历史不对称时，
    --   这个前提会破 —— 那时按 ord 配对就是把 A 索引的页回放进 B 索引的文件，
    --   **静默物理损坏**。所以配对之前按本地真实顺序再比一次，不一致宁可
    --   整个回滚、留在栅栏里等人工，也不去猜。
    SELECT coalesce(array_agg(regexp_replace(partdist.indexdef_string(i.indexrelid),
                                             '^(CREATE (UNIQUE )?INDEX )\S+ ON ', '\1ON ')
                              ORDER BY i.indexrelid), '{}')
      INTO v_have_norm
      FROM pg_catalog.pg_index i
     WHERE i.indrelid = p_shard;
    IF v_have_norm IS DISTINCT FROM v_norm THEN
        RAISE EXCEPTION 'replay_auto_follow: 对账后本地索引顺序与 leader 不一致，拒绝按 ord 配对（本地 %，leader %）',
            v_have_norm, v_norm;
    END IF;

    -- ④ 按 leader 的新 fileset 重配。起效游标必须带上（见 replay_pending_base 的注释）：
    --   缺省 0 是"从流起点开始"的断言，副本上一有数据就被拒 —— 首版就栽在这。
    v_base := partdist.replay_pending_base(p_shard);
    IF v_base IS NULL THEN
        RAISE EXCEPTION 'replay_auto_follow: shard % 缺重配起效游标（pending_base），不猜，留在栅栏', p_shard;
    END IF;
    v_pairs := partdist.replay_set_locmap(v_tbl, v.roles, v.ords, v.spcs, v.dbs, v.relnums, v_base);

    PERFORM partdist.replay_pending_follow_clear(p_shard);
    RETURN format('followed：删 %s 个索引、建 %s 个，locmap 重配 %s 对', v_drop, v_make, v_pairs);
END;
$fn$;

COMMENT ON FUNCTION replay_auto_follow(OID) IS
    'T7.25/P7-R4：副本撞上 DDL 结构栅栏后的自动跟随 —— 按 leader 随流发来的索引定义清单对账本地壳表（缺的建、多的删），再按 leader 新 fileset 重配 locmap。回放启动器会自动调它；也可手工调用。';

-- ============================================================================
-- P7-N87 / N88 / N89：ShardPG 建表 / 删表一条龙 + shardpg 模式
--
--   CREATE TABLE t (..., SHARD KEY (列) [SHARDS n] [REPLICAS n] [COLOCATE WITH 表]);
--       内核补丁 0016 解析，src/shardpg_ddl.c 执行：建表 + 分布 → 提交 →
--       partdist.shardpg_onboard()（建 Raft 组、选主、供副本、打标）
--   DROP TABLE t;
--       src/shardpg_ddl.c 把分片记进 partdist.shardpg_drop_queue 与 DROP 同一事务提交，
--       之后 partdist.shardpg_cleanup_dropped() 拆组、删副本表、删登记
--   SELECT * FROM shardpg.shards;           每个分片的主、从、哈希范围、任期（WHERE table_name IN ('t1', 't2')）
--   SELECT shardpg.cleanup_dropped();       事务块里删的表，提交后手工清
--   一个事务写多个分片（分布式事务）：自动加入全局事务，不需要额外操作（P7-N91，Citus 补丁 citus-0002）
-- ============================================================================

CREATE TABLE IF NOT EXISTS partdist.shardpg_drop_queue (
    shardid     bigint PRIMARY KEY,
    shard_name  text NOT NULL,          -- shard_name(表, 分片号)：worker 上的物理表名（已按需加引号/模式）
    rel_name    text,
    dropped_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE partdist.shardpg_drop_queue IS
    'P7-N88：已 DROP、待清理 Raft 组 / 副本表 / 登记的分片。与 DROP 同一事务写入，清理干净后删行。';

-- 路由对齐控制面登记；出错不影响调用方（DROP 前的预备动作）
CREATE OR REPLACE FUNCTION partdist.shardpg_reconcile_quiet()
RETURNS integer LANGUAGE plpgsql AS $fn$
BEGIN
    RETURN partdist.reconcile_routing();
EXCEPTION WHEN OTHERS THEN
    RAISE LOG 'shardpg_reconcile_quiet: %', SQLERRM;
    RETURN -1;
END $fn$;

-- 建组、选主、供副本、打标。不抛错：成功返回 'ok'，失败返回原因（调用方据此回退建表）
CREATE OR REPLACE FUNCTION partdist.shardpg_onboard(p_tbl regclass, p_replicas integer)
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    t0      timestamptz := clock_timestamp();
    sids    bigint[];
    todo    bigint[];
    failed  bigint[];
    r       record;
    i       int;
    bad     int := 0;
    detail  text := '';
    v_cmm   text;
BEGIN
    SELECT array_agg(shardid ORDER BY shardid) INTO sids
      FROM pg_catalog.pg_dist_shard WHERE logicalrelid = p_tbl;
    IF sids IS NULL THEN
        RETURN format('%s 没有分片', p_tbl);
    END IF;

    -- P7-N95：主分片关 autovacuum（副本由 provision_shard_replica 关）。打标表的硬盾不许 autovacuum 碰它，
    -- 不关的话 autovacuum 每分钟撞一次盾、日志刷 ERROR。Citus 把 ALTER 传到每个分片；此时还没建组、没打标
    -- SHARD KEY CREATE now applies these options in its catalog transaction.
    -- Avoid opening another distributed catalog branch in the provisioning
    -- transaction, whose local temporary scratch tables cannot be PREPAREd.
    IF NOT (SELECT coalesce(c.reloptions @> ARRAY['autovacuum_enabled=off'],false)
                   AND (c.reltoastrelid=0 OR coalesce(t.reloptions @> ARRAY['autovacuum_enabled=off'],false))
              FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_class t ON t.oid=c.reltoastrelid
             WHERE c.oid=p_tbl) THEN
        EXECUTE format('ALTER TABLE %s SET (autovacuum_enabled = off, toast.autovacuum_enabled = off)', p_tbl);
    END IF;

    -- 建组 + 选主 + 供副本：placement 先当选、副本再入组、逐个供；每批 32 个，没过的单独重跑（最多 4 轮）
    -- 内部过程的 NOTICE（第几轮、临时表）对建表的人没用，先压到 WARNING，出函数前还原
    v_cmm := current_setting('client_min_messages');
    PERFORM set_config('client_min_messages', 'warning', true);
    todo := sids;
    FOR i IN 1..4 LOOP
        bad := 0; detail := ''; failed := '{}';
        FOR r IN SELECT b.*
                   FROM generate_series(0, (coalesce(array_length(todo, 1), 0) - 1) / 32) wv(k),
                        LATERAL partdist.raft_replicate_table_shards_bulk(
                            p_tbl, p_replicas, 180, 3, (todo)[wv.k * 32 + 1 : wv.k * 32 + 32]) b
        LOOP
            IF r.status <> 'ok' THEN
                bad := bad + 1;
                failed := failed || r.shardid;
                detail := detail || format('分片 %s：%s；', r.shardid, left(r.status, 200));
            END IF;
        END LOOP;
        EXIT WHEN bad = 0;
        RAISE WARNING '建组：% 个分片没就绪，单独再跑一轮（第 % 轮）', bad, i;
        todo := failed;
    END LOOP;
    PERFORM set_config('client_min_messages', v_cmm, true);
    IF bad > 0 THEN
        RETURN format('建组 / 供副本失败 %s 个分片：%s', bad, detail);
    END IF;
    RAISE NOTICE '建组：每个分片 1 主 % 从，选主、供副本完成（% s）',
        p_replicas, round(extract(epoch FROM clock_timestamp() - t0));

    -- 打标：分片级 xid / clog / TSO 时间戳。组主刚漂移、路由还没跟上时 placement 会拒，重试到 90 s
    PERFORM set_config('client_min_messages', 'warning', true);
    FOR i IN 1..30 LOOP
        bad := 0; detail := '';
        PERFORM partdist.shardpg_reconcile_quiet();
        FOR r IN SELECT * FROM partdist.set_table_shard_mvcc(p_tbl) LOOP
            IF r.status !~ '^(registered|already)' THEN
                bad := bad + 1;
                detail := detail || format('分片 %s（:%s）：%s；', r.shardid, r.node_port, r.status);
            END IF;
        END LOOP;
        EXIT WHEN bad = 0;
        PERFORM pg_sleep(3);
    END LOOP;
    PERFORM set_config('client_min_messages', v_cmm, true);
    IF bad > 0 THEN
        RETURN format('打标失败 %s 个分片：%s', bad, detail);
    END IF;
    RAISE NOTICE '打标：% 个分片已接入分布式事务', array_length(sids, 1);

    -- ★ P7-N108：等 master 的 partition_map 登记好每个分片的主和 n 个从再返回。组主是后台向控制面（组 0）登记的，
    --   单机多端口时赶在建表返回之前，三台实验机上晚 2–8 s ⇒ 紧接着 COLOCATE WITH 这张表报「没有登记从副本」，
    --   \dt / shardpg.shards 也只有主没有从。最多等 5 分钟（128 组登记风暴见 P7-N58）；超时只警告：组和数据都已就绪
    FOR i IN 1..150 LOOP
        SELECT count(*) INTO bad FROM unnest(sids) s
         WHERE NOT EXISTS (SELECT 1 FROM partdist.partition_map pm
                            WHERE pm.partition_id = s::oid AND pm.primary_node > 0
                              AND cardinality(pm.secondary_nodes) >= p_replicas);
        EXIT WHEN bad = 0;
        PERFORM pg_sleep(2);
    END LOOP;
    IF bad > 0 THEN
        RAISE WARNING '登记：还有 % 个分片的主从没写进控制面（已等 5 分钟），稍后用 shardpg.shards 核对后再 COLOCATE WITH 这张表', bad;
    END IF;
    RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
    IF v_cmm IS NOT NULL THEN
        PERFORM set_config('client_min_messages', v_cmm, true);
    END IF;
    RETURN 'error: ' || SQLERRM;
END $fn$;

-- P7-N98：删掉本节点 follower_partition_map 里本地表已经不在的行（删表、重供基线换了表之后留下的）。
-- 按 OID 查 pg_class，并在函数里关掉 Citus 的分片表隐藏（N86 就是被它藏起分片表、误删活账）。返回删除行数。
CREATE OR REPLACE FUNCTION partdist.follower_map_gc_orphans()
RETURNS integer LANGUAGE sql
SET citus.override_table_visibility = off
AS $fn$
    WITH d AS (
        DELETE FROM partdist.follower_partition_map f
         WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c WHERE c.oid = f.partition_id)
        RETURNING 1)
    SELECT count(*)::integer FROM d
$fn$;

-- 清理已删表留下的 Raft 组、副本表、主从登记、副本回放登记。不抛错，返回摘要。
-- 只处理已经不在 pg_dist_shard 里的分片 —— 重复执行、在删表提交前执行都不会误伤。
-- ★ P7-N112（2026-10-20）：删分区登记走控制面日志（OP_PARTITION_DROP），各节点按日志序删本地的行。
-- 登记是经组 0 日志写进各节点 partition_map 的；原来删表清理在 master 和各 worker 上直接 DELETE，删除没进日志，
-- 之后有节点装快照、重放日志时这些行会被写回来（实测 pg-test master 40 行、worker1 37 行残留）。
-- 提给组 0 的 leader，再等本节点执行到这条（最多 30 s）。返回本节点上还剩的行数，0 = 删干净了。
CREATE OR REPLACE FUNCTION partdist.partition_map_unregister(p_ids bigint[])
RETURNS integer LANGUAGE plpgsql AS $fn$
DECLARE
    v_g0   integer;
    v_idx  bigint := 0;
    v_left integer := 0;
    v_batch bigint[];
    v_first integer;
    i      integer;
BEGIN
    IF p_ids IS NULL OR cardinality(p_ids) = 0 THEN
        RETURN 0;
    END IF;
    -- P7-N117: RaftLogEntry.payload has 768 bytes, including the terminator.
    -- Even 32 signed bigint values fit (< 768 bytes); one unbounded array does not.
    -- A newly joined node can replay hundreds of pre-N112 historical registrations.
    FOR v_first IN SELECT generate_series(array_lower(p_ids, 1), array_upper(p_ids, 1), 32) LOOP
        v_batch := p_ids[v_first:v_first + 31];
        v_idx := 0;
        FOR i IN 1..20 LOOP
            SELECT leader_node_id INTO v_g0 FROM partdist.pg_raft_group_status() WHERE group_id = 0;
            BEGIN
                v_idx := partdist.node_exec(v_g0, format(
                    'SELECT partdist.pg_raft_group_propose(0, ''OP_PARTITION_DROP'', %L)',
                    json_build_object('partition_ids', v_batch)::text))::bigint;
            EXCEPTION WHEN OTHERS THEN
                v_idx := 0;
            END;
            EXIT WHEN v_idx > 0;
            PERFORM pg_catalog.pg_sleep(1);
        END LOOP;
        IF v_idx <= 0 THEN
            -- Local absence is not proof of success: only another node may hold the rows.
            RAISE EXCEPTION 'could not commit partition unregister batch (% ids)', cardinality(v_batch);
        END IF;
    END LOOP;
    FOR i IN 1..30 LOOP
        SELECT count(*) INTO v_left FROM partdist.partition_map WHERE partition_id::bigint = ANY (p_ids);
        EXIT WHEN v_left = 0;
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
    RETURN v_left;
END $fn$;
COMMENT ON FUNCTION partdist.partition_map_unregister(bigint[]) IS
    'P7-N112：经控制面日志（OP_PARTITION_DROP）删这些分区的主从登记，各节点按日志序删本地的行；返回本节点上还剩的行数（0 = 干净）。';

-- ★ P7-N110 / N112（2026-10-20）：清旧残留 —— 已删分片（组号不在 pg_dist_shard 里）的主从登记和 hardstate 文件。
-- 清的是修复之前删表留下的（修复后正常删表不会再留）。在 master 上执行；主从登记经控制面日志删（各节点一致，
-- 快照重放也不会回来），hardstate 文件逐台按组号删（组还在内存里的跳过）。返回每类每处删掉的个数
CREATE OR REPLACE FUNCTION partdist.residue_gc()
RETURNS TABLE (item text, node text, removed integer) LANGUAGE plpgsql AS $fn$
DECLARE
    v_live bigint[];
    v_ids  bigint[];
    r      record;
BEGIN
    SELECT coalesce(array_agg(shardid), '{}') INTO v_live FROM pg_catalog.pg_dist_shard;
    SELECT array_agg(DISTINCT t.x ORDER BY t.x) INTO v_ids FROM (
        SELECT m.partition_id::bigint AS x FROM partdist.partition_map m
        UNION
        SELECT unnest(string_to_array(w.result, ','))::bigint
          FROM pg_catalog.run_command_on_workers(
                 $q$SELECT coalesce(string_agg(partition_id::bigint::text, ','), '') FROM partdist.partition_map$q$) w
         WHERE w.success AND w.result <> ''
    ) t WHERE NOT t.x = ANY (v_live);
    item := '主从登记（partition_map）';
    node := '全部节点，经控制面日志';
    removed := coalesce(cardinality(v_ids), 0);
    IF removed > 0 THEN
        PERFORM partdist.partition_map_unregister(v_ids);
    END IF;
    RETURN NEXT;
    FOR r IN SELECT n.name, w.success, w.result FROM pg_catalog.run_command_on_workers(format($q$
        SELECT partdist.pg_raft_hardstate_purge(coalesce(array_agg(substr(f, 19)::bigint), '{}'))
          FROM pg_catalog.pg_ls_dir('.') f
         WHERE f ~ '^pg_raft_hardstate\.[0-9]+$' AND NOT substr(f, 19)::bigint = ANY (%L::bigint[])$q$, v_live)) w
        JOIN shardpg.nodes n ON n.port = w.nodeport ORDER BY n.node_id
    LOOP
        item := 'hardstate 文件';
        node := r.name;
        removed := CASE WHEN r.success AND r.result ~ '^[0-9]+$' THEN r.result::integer END;
        RETURN NEXT;
    END LOOP;
END $fn$;
COMMENT ON FUNCTION partdist.residue_gc() IS
    'P7-N110/N112：清修复之前删表留下的残留 —— 已删分片的主从登记（经控制面日志）和各 worker 上的 hardstate 文件。在 master 上执行。';

CREATE OR REPLACE FUNCTION partdist.shardpg_cleanup_dropped()
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    v_ids    bigint[];
    v_names  text[];
    v_list   text;
    v_n      int;
    v_groups int := 0;
    v_round  int;
    v_left   text;
    r        record;
BEGIN
    SELECT array_agg(q.shardid ORDER BY q.shardid), array_agg(q.shard_name ORDER BY q.shardid), count(*)
      INTO v_ids, v_names, v_n
      FROM partdist.shardpg_drop_queue q
     WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_dist_shard s WHERE s.shardid = q.shardid);
    IF v_n = 0 THEN
        RETURN '没有待清理的分片';
    END IF;
    v_list := array_to_string(v_ids, ',');

    FOR v_round IN 1..4 LOOP
        -- ① 关回放槽，再拆 Raft 组；拆组时在途的消息会把组建回来，隔 3 秒再拆一遍
        PERFORM pg_catalog.run_command_on_workers(format(
            $q$SELECT count(partdist.replay_disable(pg_catalog.to_regclass(t)))
                 FROM unnest(%L::text[]) t WHERE pg_catalog.to_regclass(t) IS NOT NULL$q$, v_names));
        FOR r IN SELECT * FROM pg_catalog.run_command_on_workers(format(
            'SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id IN (%s)',
            v_list)) LOOP
            IF r.success AND v_round = 1 THEN v_groups := v_groups + r.result::int; END IF;
        END LOOP;
        PERFORM pg_sleep(3);
        PERFORM pg_catalog.run_command_on_workers(format(
            'SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id IN (%s)',
            v_list));
        -- ★ P7-N110：再按组号删 hardstate 文件（不要求组在内存里：兜住拆组时被写回来的、这台当时不在线的）
        PERFORM pg_catalog.run_command_on_workers(format(
            'SELECT partdist.pg_raft_hardstate_purge(ARRAY[%s]::bigint[])', v_list));
        IF v_round = 1 THEN
            IF v_groups > 0 THEN
                RAISE NOTICE '清理：已拆除 % 个分片的 Raft 组', v_n;
            ELSE
                RAISE NOTICE '清理：% 个分片没有残留的 Raft 组', v_n;
            END IF;
        END IF;

        -- ② 删各节点上的副本表（副本是本地表，Citus 不会跟着删）；逐张删，删不掉的下一轮再试
        PERFORM pg_catalog.run_command_on_workers(format(
            $o$DO $$ DECLARE t text; BEGIN
                 FOREACH t IN ARRAY %L::text[] LOOP
                   BEGIN
                     IF pg_catalog.to_regclass(t) IS NOT NULL THEN
                       EXECUTE 'DROP TABLE IF EXISTS ' || t;
                     END IF;
                   EXCEPTION WHEN OTHERS THEN NULL;
                   END;
                 END LOOP; END $$$o$, v_names));
        IF v_round = 1 THEN
            RAISE NOTICE '清理：已删除各节点上的副本表';
        END IF;

        -- ③ 删主从登记（★ P7-N112：经控制面日志，master 与各 worker 按日志序删；原来各节点直接 DELETE，
        --    装快照重放后会被写回来）、副本回放登记（P7-N98），回收打标登记与回放槽位
        PERFORM partdist.partition_map_unregister(v_ids);
        PERFORM pg_catalog.run_command_on_workers(format(
            'DELETE FROM partdist.follower_partition_map WHERE global_shard_id IN (%s)', v_list));
        PERFORM pg_catalog.run_command_on_workers('SELECT partdist.follower_map_gc_orphans()');
        PERFORM pg_catalog.run_command_on_workers('SELECT partdist.shard_mvcc_gc_orphans()');
        PERFORM pg_catalog.run_command_on_workers('SELECT slots_freed FROM partdist.replay_reclaim_stale(0)');
        IF v_round = 1 THEN
            RAISE NOTICE '清理：已删除主从登记，回收残留';
        END IF;

        -- ④ 核对：每台 worker 上 Raft 组 / 物理表 / 登记 / 副本回放登记都应为 0
        v_left := '';
        FOR r IN SELECT nodeport, success, result FROM pg_catalog.run_command_on_workers(format(
            $q$SELECT (SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN (%1$s)) || '|' ||
                      (SELECT count(*) FROM unnest(%2$L::text[]) t WHERE pg_catalog.to_regclass(t) IS NOT NULL) || '|' ||
                      (SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN (%1$s)) || '|' ||
                      (SELECT count(*) FROM partdist.follower_partition_map WHERE global_shard_id IN (%1$s)) || '|' ||
                      (SELECT count(*) FROM pg_catalog.pg_ls_dir('.') f
                        WHERE f IN (SELECT 'pg_raft_hardstate.' || x FROM unnest(ARRAY[%1$s]) x))$q$,
            v_list, v_names)) ORDER BY 1
        LOOP
            IF NOT r.success OR r.result <> '0|0|0|0|0' THEN
                v_left := v_left || format(' :%s(%s)', r.nodeport,
                                           CASE WHEN r.success THEN '组|表|登记|副本登记|hardstate=' || r.result ELSE '连不上' END);
            END IF;
        END LOOP;
        EXIT WHEN v_left = '';
        PERFORM pg_sleep(3);
    END LOOP;

    IF v_left = '' THEN
        DELETE FROM partdist.shardpg_drop_queue WHERE shardid = ANY (v_ids);
        RETURN format('已清理 %s 个分片', v_n);
    END IF;
    RAISE WARNING '清理：还有残留%（稍后再执行 SELECT shardpg.cleanup_dropped();）', v_left;
    RETURN 'residue:' || v_left;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '清理中断：%（稍后再执行 SELECT shardpg.cleanup_dropped();）', SQLERRM;
    RETURN 'error: ' || SQLERRM;
END $fn$;

-- ---- 面向用户的 shardpg 模式 ----
CREATE SCHEMA IF NOT EXISTS shardpg;

-- ★ P7-N107：节点名。节点号 1 是 master，其余是 worker<节点号-1> —— 与数据目录名（worker1、worker2…）一致，
--   scripts/new_worker_node.sh 按同一规则建目录。\dt（psql 补丁 0017 里同一条规则）、shardpg.shards、shardpg.nodes
--   与 shardpg.add_node / move_replica / move_primary 的输出都用它，不再显示 :5433 这种端口
CREATE OR REPLACE FUNCTION shardpg.node_name(p_node_id integer)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$$ SELECT CASE WHEN p_node_id = 1 THEN 'master' ELSE 'worker' || (p_node_id - 1) END $$;
COMMENT ON FUNCTION shardpg.node_name(integer) IS
    'P7-N107：节点号 → 节点名（1 = master，其余 worker<节点号-1>，与数据目录名一致）。';

-- 本机的节点名。放进分布式查询的 SELECT 列表里，求值的是实际执行这一行的 worker
CREATE OR REPLACE FUNCTION shardpg.node_name()
RETURNS text LANGUAGE sql STABLE PARALLEL SAFE AS
$$ SELECT shardpg.node_name(pg_catalog.current_setting('pg_raft.node_id', true)::integer) $$;
COMMENT ON FUNCTION shardpg.node_name() IS
    'P7-N107：本机的节点名；SELECT shardpg.node_name() AS worker FROM 分布表 … 显示每一行由哪台 worker 返回。';

CREATE OR REPLACE VIEW shardpg.nodes AS
SELECT shardpg.node_name(nm.node_id) AS name, nm.node_id, nm.hostname AS host, nm.port, nm.status
  FROM partdist.node_map nm
 ORDER BY nm.node_id;
COMMENT ON VIEW shardpg.nodes IS
    'P7-N107：节点名与地址的对照 —— \dt、shardpg.shards 里的 worker1… 是哪个 host:port（连库、停节点用）。';

CREATE OR REPLACE VIEW shardpg.shards AS
SELECT s.logicalrelid AS table_name,
       pg_catalog.shard_name(s.logicalrelid, s.shardid) AS shard_name,
       pg_catalog.column_to_column_name(p.logicalrelid, p.partkey) AS shard_key,
       '[' || s.shardminvalue || ', ' || s.shardmaxvalue || ']' AS hash_range,
       COALESCE((SELECT shardpg.node_name(nm.node_id) FROM partdist.node_map nm WHERE nm.node_id = m.primary_node),
                -- 没有主从登记的分片：Citus 放置节点，按地址到 node_map 里找名字，找不到才显示地址
                (SELECT COALESCE((SELECT shardpg.node_name(nm.node_id) FROM partdist.node_map nm
                                   WHERE nm.port = nd.nodeport
                                     AND (nm.hostname = nd.nodename
                                          OR (nm.hostname IN ('localhost', '127.0.0.1') AND nd.nodename IN ('localhost', '127.0.0.1')))
                                   LIMIT 1),
                                 nd.nodename || ':' || nd.nodeport)
                   FROM pg_catalog.pg_dist_placement pl
                   JOIN pg_catalog.pg_dist_node nd ON nd.groupid = pl.groupid AND nd.noderole = 'primary'
                  WHERE pl.shardid = s.shardid ORDER BY nd.nodeport LIMIT 1)) AS "primary",
       (SELECT string_agg(shardpg.node_name(nm.node_id), ',' ORDER BY nm.node_id)
          FROM partdist.node_map nm WHERE nm.node_id = ANY (m.secondary_nodes)) AS replicas,
       m.primary_term AS term
  FROM pg_catalog.pg_dist_shard s
  JOIN pg_catalog.pg_dist_partition p ON p.logicalrelid = s.logicalrelid AND p.partmethod = 'h'
  LEFT JOIN partdist.partition_map m ON m.partition_id::bigint = s.shardid
 ORDER BY s.logicalrelid::text, s.shardminvalue::int;
COMMENT ON VIEW shardpg.shards IS
    'P7-N89：每个分片的物理表名、分片键、哈希范围、主、从、任期（主从取控制面登记，没登记的取 Citus 放置节点）。';

CREATE OR REPLACE FUNCTION shardpg.cleanup_dropped()
RETURNS text LANGUAGE sql AS $fn$
    SELECT partdist.shardpg_cleanup_dropped()
$fn$;
COMMENT ON FUNCTION shardpg.cleanup_dropped() IS
    'P7-N88：清理已删分片表留下的 Raft 组、副本表、主从登记（在事务块里 DROP 的表提交后执行；自动清理没清干净时再执行）。';

-- ============================================================================
-- P7-N94 / P7-N102（2026-10-17）：共置分片对齐主、在线加节点、副本迁移、计划内切主
--
--   SELECT * FROM shardpg.add_node('127.0.0.1', 5437);
--       新 worker 入簇：各节点 pg_raft.peers 热加载追加它（不重启任何节点）→ 控制面登记地址 →
--       等它追上控制面日志 → Citus citus_add_node。新节点的操作系统层准备（initdb、配置、
--       建扩展、启动）见 scripts/new_worker_node.sh
--   SELECT * FROM shardpg.move_replica('customer_111208', 5434, 5437);
--       把分片的一份副本从 :5434 搬到 :5437；同一哈希段的共置分片（orders_111212 …）一起搬
--   SELECT shardpg.move_primary('customer_111208', 5435);
--       计划内切主，共置分片一起切
--   partdist.align_colocated_leaders()
--       master 的 pg_raft maintenance 每 5 s 调一次：共置分片的主不在同一台时，
--       把领导权转交到锚分片（该哈希段分片号最小的那个）的主上（P7-N94）
-- ============================================================================

-- 在指定地址的节点上执行一条 SQL：经 Citus 的 master_run_on_worker 单开连接、自动提交
-- （ALTER SYSTEM 这类不能进事务块的命令也能执行）。返回结果文本；失败时 p_strict 抛错，否则返回 NULL
CREATE OR REPLACE FUNCTION partdist.addr_exec(p_host text, p_port integer, p_cmd text, p_strict boolean DEFAULT true)
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    r record;
BEGIN
    SELECT * INTO r FROM pg_catalog.master_run_on_worker(ARRAY[p_host], ARRAY[p_port], ARRAY[p_cmd], false);
    IF r.success THEN
        RETURN r.result;
    END IF;
    IF p_strict THEN
        RAISE EXCEPTION '%:% 上执行失败：%', p_host, p_port, r.result;
    END IF;
    RETURN NULL;
END $fn$;

-- 同上，按 Raft 节点号（地址取 partdist.node_map）
CREATE OR REPLACE FUNCTION partdist.node_exec(p_node integer, p_cmd text, p_strict boolean DEFAULT true)
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    h text;
    p integer;
BEGIN
    SELECT hostname, port INTO h, p FROM partdist.node_map WHERE node_id = p_node;
    IF h IS NULL THEN
        IF p_strict THEN
            RAISE EXCEPTION 'node_map 里没有节点 %', p_node;
        END IF;
        RETURN NULL;
    END IF;
    RETURN partdist.addr_exec(h, p, p_cmd, p_strict);
END $fn$;

CREATE TABLE IF NOT EXISTS partdist.reprovision_hold (
    group_id bigint PRIMARY KEY,
    until    timestamptz NOT NULL,
    reason   text
);
COMMENT ON TABLE partdist.reprovision_hold IS
    'P7-N102：登记在这里（until 之前）的组暂停自动重供 —— 归队扫尾与新主登记后的 P7-N25 归队都跳过。shardpg.move_replica 迁移期间在各节点登记、结束删除；until 是函数中途死掉时的兜底。';

-- 【在 worker 上执行】惰性回放的"预热"：把本节点组 p_gid 的副本回放到本节点已提交的数据位点，
-- 再看它能不能顶上（基线收尾、索引不是空壳）。返回 ready 或没就绪的原因。
-- 转交领导权前、迁移完成前都要过这一关：基线半途而废的副本能赢选举，却在升主前置里一直报
-- "全量物理基线未收尾"，占着 leader、整组写不进去（2026-10-17 预演实测）。
CREATE OR REPLACE FUNCTION partdist.replica_warmup(p_gid bigint, p_timeout_ms integer DEFAULT 5000)
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    loc   oid;
    ok    boolean;
    app   bigint;
    bound bigint;
BEGIN
    loc := partdist.local_partition_for_shard(p_gid);
    IF loc IS NULL THEN
        RETURN 'no_local_shard';
    END IF;
    SELECT armed AND state <> 'failed', applied INTO ok, app FROM partdist.replay_status() WHERE shard = loc;
    IF NOT coalesce(ok, false) THEN
        RETURN 'replica_not_armed';
    END IF;
    IF partdist.shard_divergence(loc) IS NOT NULL THEN
        RETURN 'diverged';
    END IF;
    bound := partdist.pg_raft_group_committed_plsn(p_gid);
    IF coalesce(bound, 0) > coalesce(app, 0) THEN
        BEGIN
            PERFORM partdist.replay_catchup(loc::regclass, bound, p_timeout_ms);
        EXCEPTION WHEN OTHERS THEN
            RETURN 'replaying: ' || left(SQLERRM, 160);
        END;
    END IF;
    IF partdist.shard_baseline_pending(loc) THEN
        RETURN 'baseline_pending';
    END IF;
    IF partdist.shard_local_copy_broken(loc) THEN
        RETURN 'copy_broken';
    END IF;
    RETURN 'ready';
END $fn$;

-- 节点 p_node 上组 p_gid 的一列状态（pg_raft_group_status 的列名）；没有这个组 / 连不上返回 NULL
CREATE OR REPLACE FUNCTION partdist.node_group_status(p_node integer, p_gid bigint, p_col text)
RETURNS text LANGUAGE sql AS $fn$
    SELECT nullif(partdist.node_exec(p_node, format(
        'SELECT %I::text FROM partdist.pg_raft_group_status() WHERE group_id = %s', p_col, p_gid), false), '')
$fn$;

-- 组 p_gid 现在的 Raft leader（节点号）：先问 partition_map 登记的主，再问其余成员；都不是返回 NULL
CREATE OR REPLACE FUNCTION partdist.group_leader_now(p_gid bigint, p_extra integer[] DEFAULT '{}')
RETURNS integer LANGUAGE plpgsql AS $fn$
DECLARE
    m record;
    n integer;
BEGIN
    SELECT primary_node, secondary_nodes INTO m FROM partdist.partition_map WHERE partition_id = p_gid::oid;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    FOREACH n IN ARRAY (ARRAY[m.primary_node] || m.secondary_nodes || p_extra) LOOP
        IF partdist.node_group_status(n, p_gid, 'state') = 'leader' THEN
            RETURN n;
        END IF;
    END LOOP;
    RETURN NULL;
END $fn$;

-- 一个分片（名字或分片号）所在的共置哈希段：共置组号、哈希下界、段内全部分片号与分片表名
CREATE OR REPLACE FUNCTION partdist.colocated_bucket(p_shard text,
    OUT colocationid integer, OUT minv integer, OUT sids bigint[], OUT names text[])
LANGUAGE sql STABLE AS $fn$
    WITH me AS (
        SELECT s.shardid, p.colocationid, s.shardminvalue
          FROM pg_catalog.pg_dist_shard s
          JOIN pg_catalog.pg_dist_partition p ON p.logicalrelid = s.logicalrelid AND p.partmethod = 'h'
         WHERE s.shardid::text = p_shard
            OR pg_catalog.shard_name(s.logicalrelid, s.shardid) = p_shard
         LIMIT 1)
    SELECT me.colocationid::integer, me.shardminvalue::integer,
           array_agg(s.shardid ORDER BY s.shardid),
           array_agg(pg_catalog.shard_name(s.logicalrelid, s.shardid) ORDER BY s.shardid)
      FROM me
      JOIN pg_catalog.pg_dist_partition p ON p.colocationid = me.colocationid AND p.partmethod = 'h'
      JOIN pg_catalog.pg_dist_shard s ON s.logicalrelid = p.logicalrelid AND s.shardminvalue = me.shardminvalue
     GROUP BY me.colocationid, me.shardminvalue
$fn$;

-- 在组 p_gid 的 leader 上做一次成员变更（加 / 减一个节点）。leader 刚换、或上一个变更还没提交时重试，最多 p_timeout_s 秒
CREATE OR REPLACE FUNCTION partdist.group_change_member(p_gid bigint, p_node integer, p_add boolean,
                                                        p_extra integer[] DEFAULT '{}', p_timeout_s integer DEFAULT 60)
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    t0       timestamptz := clock_timestamp();
    v_leader integer;
    v_res    text;
    v_err    text := '';
BEGIN
    LOOP
        v_leader := partdist.group_leader_now(p_gid, p_extra);
        IF v_leader IS NOT NULL THEN
            BEGIN
                v_res := partdist.node_exec(v_leader, format(
                    'SELECT partdist.pg_raft_group_change_member(%s, %s, %s)', p_gid, p_node,
                    CASE WHEN p_add THEN 'true' ELSE 'false' END));
                IF v_res LIKE 'group %' THEN
                    RETURN v_res;
                END IF;
                v_err := v_res;
            EXCEPTION WHEN OTHERS THEN
                v_err := SQLERRM;
            END;
        END IF;
        IF clock_timestamp() - t0 > make_interval(secs => p_timeout_s) THEN
            RAISE EXCEPTION '组 % %节点 % 失败：%', p_gid, CASE WHEN p_add THEN '加入' ELSE '移出' END, p_node, v_err;
        END IF;
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
END $fn$;

-- 本节点（须是该组 leader）到节点 p_node 的组 p_gid 是否就绪：日志不短于 p_min_index、回放槽位 armed、未 failed、无分叉
CREATE OR REPLACE FUNCTION partdist.node_replica_ready(p_node integer, p_gid bigint, p_min_index bigint)
RETURNS boolean LANGUAGE sql AS $fn$
    SELECT coalesce(partdist.node_exec(p_node, format($q$
        SELECT coalesce((SELECT last_log_index >= %2$s FROM partdist.pg_raft_group_status() WHERE group_id = %1$s), false)
           AND coalesce((SELECT armed AND state <> 'failed' AND partdist.shard_divergence(shard) IS NULL
                           FROM partdist.replay_status()
                          WHERE shard = partdist.local_partition_for_shard(%1$s)), false)$q$,
        p_gid, p_min_index), false), 'f') = 't'
$fn$;

-- 节点 p_node 上组 p_gid 的副本预热回放后能不能顶上（partdist.replica_warmup 返回 ready）
CREATE OR REPLACE FUNCTION partdist.node_replica_warm(p_node integer, p_gid bigint)
RETURNS text LANGUAGE sql AS $fn$
    SELECT coalesce(partdist.node_exec(p_node, format('SELECT partdist.replica_warmup(%s, 5000)', p_gid), false), '连不上')
$fn$;

-- 计划内把组 p_gid 的主交给节点 p_target：先在目标上预热回放（基线收尾、能顶上才行），再请现任主
-- 转交领导权（pg_raft_group_transfer_leader：挡住新提交、把目标追平、请它竞选、等自己被替下）。
-- 不用"在目标上强制竞选"：持续写入时目标的日志总比主短一点，选不上还会把主拉下来、整组重选。
-- 返回 transferred… / already_leader / 没转成的原因
CREATE OR REPLACE FUNCTION partdist.raft_transfer_leader(p_gid bigint, p_target integer)
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    v        text;
    v_leader integer;
BEGIN
    v_leader := partdist.group_leader_now(p_gid, ARRAY[p_target]);
    IF v_leader IS NULL THEN
        RETURN 'no_leader';
    END IF;
    IF v_leader = p_target THEN
        RETURN 'already_leader';
    END IF;
    v := partdist.node_replica_warm(p_target, p_gid);
    IF v <> 'ready' THEN
        RETURN v;
    END IF;
    RETURN coalesce(partdist.node_exec(v_leader, format(
        'SELECT partdist.pg_raft_group_transfer_leader(%s, %s, 10000)', p_gid, p_target), false), 'leader_unreachable');
END $fn$;

-- 在各 worker 上登记 / 撤销这些组的"暂停自动重供"（partdist.reprovision_hold，30 分钟兜底过期）
CREATE OR REPLACE FUNCTION partdist.reprovision_hold_set(p_sids bigint[], p_on boolean, p_reason text DEFAULT NULL)
RETURNS void LANGUAGE plpgsql AS $fn$
DECLARE
    r record;
BEGIN
    FOR r IN SELECT node_id FROM partdist.node_map
              WHERE node_id <> current_setting('pg_raft.coordinator_node_id')::integer LOOP
        IF p_on THEN
            PERFORM partdist.node_exec(r.node_id, format(
                'INSERT INTO partdist.reprovision_hold (group_id, until, reason) '
                'SELECT g, now() + interval ''30 minutes'', %L FROM unnest(%L::bigint[]) g '
                'ON CONFLICT (group_id) DO UPDATE SET until = EXCLUDED.until, reason = EXCLUDED.reason',
                p_reason, p_sids), false);
        ELSE
            PERFORM partdist.node_exec(r.node_id, format(
                'DELETE FROM partdist.reprovision_hold WHERE group_id = ANY (%L::bigint[])', p_sids), false);
        END IF;
    END LOOP;
END $fn$;

-- 把一组共置分片的主切到节点 p_target：逐个请现任主转交领导权（partdist.raft_transfer_leader），
-- 再等控制面登记全部指向目标（新主先把积压回放进表再登记）
CREATE OR REPLACE FUNCTION partdist.move_bucket_primary(p_sids bigint[], p_target integer, p_timeout_s integer DEFAULT 180)
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    t0       timestamptz := clock_timestamp();
    sid      bigint;
    v_left   integer;
    v_state  text;
    v_res    text;
    v_note   text := '';
BEGIN
    LOOP
        v_left := 0;
        FOREACH sid IN ARRAY p_sids LOOP
            v_state := partdist.node_group_status(p_target, sid, 'state');
            CONTINUE WHEN v_state = 'leader' AND
                (SELECT primary_node FROM partdist.partition_map WHERE partition_id = sid::oid) = p_target;
            v_left := v_left + 1;
            CONTINUE WHEN v_state = 'leader';           -- 已当选，等它回放完、登记
            v_res := partdist.raft_transfer_leader(sid, p_target);
            v_note := format('分片 %s：%s', sid, v_res);
        END LOOP;
        EXIT WHEN v_left = 0;
        IF clock_timestamp() - t0 > make_interval(secs => p_timeout_s) THEN
            RAISE EXCEPTION '切主超时（% s）：还有 % 个分片的主没到 %（最近一次：%）', p_timeout_s, v_left,
                shardpg.node_name(p_target), v_note;
        END IF;
        PERFORM pg_catalog.pg_sleep(2);
    END LOOP;
    RETURN format('primary of %s shard(s) now on %s (%s s)', cardinality(p_sids), shardpg.node_name(p_target),
                  round(extract(epoch FROM clock_timestamp() - t0)));
END $fn$;

-- 组 p_gid 当前（已提交）的 Raft 成员集：leader 本地 raft_group.members（成员变更提交时落盘）；问不到就用 partition_map 登记
CREATE OR REPLACE FUNCTION partdist.group_raft_members(p_gid bigint, p_extra integer[] DEFAULT '{}')
RETURNS integer[] LANGUAGE plpgsql AS $fn$
DECLARE
    v_leader integer := partdist.group_leader_now(p_gid, p_extra);
    v        text;
BEGIN
    IF v_leader IS NOT NULL THEN
        v := partdist.node_exec(v_leader, format(
            'SELECT members::text FROM partdist.raft_group WHERE group_id = %s AND cardinality(members) > 0', p_gid), false);
        IF coalesce(v, '') <> '' THEN
            RETURN v::integer[];
        END IF;
    END IF;
    RETURN (SELECT ARRAY[primary_node] || secondary_nodes FROM partdist.partition_map WHERE partition_id = p_gid::oid);
END $fn$;

-- 删掉节点 p_node 上这些分片的副本：关回放槽、拆 Raft 组（拆两遍，在途消息会把组建回来）、删副本表、
-- 删副本回放登记 / 分片身份 / 打标登记、回收回放槽位。调用前该节点须已移出这些组（成员变更已提交）。
-- 返回残留（组|表|副本登记|hardstate 文件），'0|0|0|0' = 干净。shardpg.move_replica 的最后一步；中途失败时可单独重跑
CREATE OR REPLACE FUNCTION partdist.drop_local_replicas(p_node integer, p_sids bigint[], p_names text[])
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    v_list text := array_to_string(p_sids, ',');
    v_left text;
    i      integer;
BEGIN
    FOR i IN 1..5 LOOP
        PERFORM partdist.node_exec(p_node, format(
            $q$SELECT count(partdist.replay_disable(pg_catalog.to_regclass(t)))
                 FROM unnest(%L::text[]) t WHERE pg_catalog.to_regclass(t) IS NOT NULL$q$, p_names), false);
        PERFORM partdist.node_exec(p_node, format(
            'SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id IN (%s)', v_list), false);
        PERFORM pg_catalog.pg_sleep(3);
        PERFORM partdist.node_exec(p_node, format(
            'SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id IN (%s)', v_list), false);
        -- ★ P7-N110：按组号删 hardstate 文件（拆组时被写回来的也删掉）
        PERFORM partdist.node_exec(p_node, format(
            'SELECT partdist.pg_raft_hardstate_purge(ARRAY[%s]::bigint[])', v_list), false);
        -- 副本表名是 Citus 的分片名，worker 上 Citus 把它当分片、不许直接删：本语句内放开
        PERFORM partdist.node_exec(p_node, format(
            $o$DO $$ DECLARE t text; BEGIN
                 PERFORM set_config('citus.enable_ddl_propagation', 'off', true);
                 PERFORM set_config('citus.enable_manual_changes_to_shards', 'on', true);
                 FOREACH t IN ARRAY %L::text[] LOOP
                   BEGIN
                     IF pg_catalog.to_regclass(t) IS NOT NULL THEN
                       EXECUTE 'DROP TABLE IF EXISTS ' || t;
                     END IF;
                   EXCEPTION WHEN OTHERS THEN
                     RAISE WARNING 'drop_local_replicas：删 %% 失败：%%', t, SQLERRM;
                   END;
                 END LOOP; END $$$o$, p_names), false);
        PERFORM partdist.node_exec(p_node, format(
            'DELETE FROM partdist.follower_partition_map WHERE global_shard_id IN (%s)', v_list), false);
        PERFORM partdist.node_exec(p_node, format(
            'DELETE FROM partdist.shard_identity WHERE global_shard_id IN (%s)', v_list), false);
        PERFORM partdist.node_exec(p_node, 'SELECT partdist.follower_map_gc_orphans()', false);
        PERFORM partdist.node_exec(p_node, 'SELECT partdist.shard_mvcc_gc_orphans()', false);
        PERFORM partdist.node_exec(p_node, 'SELECT slots_freed FROM partdist.replay_reclaim_stale(0)', false);
        v_left := partdist.node_exec(p_node, format(
            $q$SELECT (SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN (%1$s)) || '|' ||
                      (SELECT count(*) FROM unnest(%2$L::text[]) t WHERE pg_catalog.to_regclass(t) IS NOT NULL) || '|' ||
                      (SELECT count(*) FROM partdist.follower_partition_map WHERE global_shard_id IN (%1$s)) || '|' ||
                      (SELECT count(*) FROM pg_catalog.pg_ls_dir('.') f
                        WHERE f IN (SELECT 'pg_raft_hardstate.' || x FROM unnest(ARRAY[%1$s]) x))$q$,
            v_list, p_names), false);
        EXIT WHEN v_left = '0|0|0|0';
        PERFORM pg_catalog.pg_sleep(3);
    END LOOP;
    RETURN coalesce(v_left, '连不上');
END $fn$;

-- ---- P7-N94：共置分片对齐主 ----
CREATE TABLE IF NOT EXISTS partdist.colocation_align_attempt (
    shardid      bigint PRIMARY KEY,
    target       integer,
    attempted_at timestamptz,
    result       text
);
COMMENT ON TABLE partdist.colocation_align_attempt IS
    'P7-N94：共置对齐最近一次对某分片的尝试（限流：转交成功的 60 s 内不再试，没转成的 10 s 后再试）。只在 master 上有内容。';

CREATE OR REPLACE FUNCTION partdist.align_colocated_leaders()
RETURNS integer LANGUAGE plpgsql AS $fn$
DECLARE
    r      record;
    v_res  text;
    n      integer := 0;
BEGIN
    FOR r IN
        WITH sh AS (
            SELECT s.shardid, p.colocationid, s.shardminvalue::integer AS minv,
                   m.primary_node, m.secondary_nodes, m.updated_at
              FROM pg_catalog.pg_dist_shard s
              JOIN pg_catalog.pg_dist_partition p ON p.logicalrelid = s.logicalrelid AND p.partmethod = 'h'
              JOIN partdist.partition_map m ON m.partition_id::bigint = s.shardid
             WHERE m.primary_term > 0
        ), b AS (
            SELECT colocationid, minv,
                   (array_agg(shardid ORDER BY shardid))[1] AS anchor,
                   (array_agg(primary_node ORDER BY shardid))[1] AS anchor_primary,
                   count(DISTINCT primary_node) AS nprim,
                   max(updated_at) AS last_change
              FROM sh GROUP BY colocationid, minv HAVING count(*) > 1
        )
        SELECT sh.shardid, sh.primary_node, b.anchor, b.anchor_primary, b.colocationid, b.minv
          FROM b JOIN sh USING (colocationid, minv)
         WHERE b.nprim > 1
           AND sh.primary_node <> b.anchor_primary
           AND b.anchor_primary = ANY (sh.secondary_nodes)          -- 锚的主也是它的成员（共置组成员相同）
           AND b.last_change < now() - interval '15 seconds'         -- 这一段的登记已稳定 15 s（刚切主的别急着动）
           AND NOT EXISTS (SELECT 1 FROM partdist.colocation_align_attempt a
                            WHERE a.shardid = sh.shardid
                              AND a.attempted_at > now() - CASE WHEN a.result LIKE 'transferred%'
                                                                THEN interval '60 seconds'
                                                                ELSE interval '10 seconds' END)
         ORDER BY sh.shardid
    LOOP
        -- 迁移 / 手动切主正在处理这一段（持有段锁）就不插手
        CONTINUE WHEN NOT pg_catalog.pg_try_advisory_xact_lock(r.colocationid, r.minv);
        v_res := partdist.raft_transfer_leader(r.shardid, r.anchor_primary);
        INSERT INTO partdist.colocation_align_attempt AS a (shardid, target, attempted_at, result)
        VALUES (r.shardid, r.anchor_primary, now(), v_res)
        ON CONFLICT (shardid) DO UPDATE SET target = EXCLUDED.target, attempted_at = EXCLUDED.attempted_at,
                                            result = EXCLUDED.result;
        IF v_res LIKE 'transferred%' THEN
            n := n + 1;
            RAISE LOG 'shardpg: 共置对齐 —— 分片 % 的主由节点 % 交给节点 %（与锚分片 % 同机，P7-N94）',
                r.shardid, r.primary_node, r.anchor_primary, r.anchor;
        END IF;
    END LOOP;
    RETURN n;
END $fn$;
COMMENT ON FUNCTION partdist.align_colocated_leaders() IS
    'P7-N94：共置分片的主不在同一台时（各组各自选主的结果），请其余分片的现任主把领导权转交给锚分片（同一哈希段里分片号最小的）的主所在节点；目标先预热回放、基线收尾才转。master 的 pg_raft maintenance 每 5 s 调一次。返回本轮转交成功的个数。';

-- ---- 面向用户的入口 ----

CREATE OR REPLACE FUNCTION shardpg.move_primary(p_shard text, p_to_port integer)
RETURNS text LANGUAGE plpgsql AS $fn$
DECLARE
    b    record;
    v_to integer;
    sid  bigint;
BEGIN
    SELECT * INTO b FROM partdist.colocated_bucket(p_shard);
    IF b.sids IS NULL THEN
        RAISE EXCEPTION '找不到分片 %', p_shard;
    END IF;
    SELECT node_id INTO v_to FROM partdist.node_map WHERE port = p_to_port;
    IF v_to IS NULL THEN
        RAISE EXCEPTION 'node_map 里没有端口 % 的节点', p_to_port;
    END IF;
    FOREACH sid IN ARRAY b.sids LOOP
        IF NOT EXISTS (SELECT 1 FROM partdist.partition_map
                        WHERE partition_id = sid::oid AND v_to = ANY (ARRAY[primary_node] || secondary_nodes)) THEN
            RAISE EXCEPTION '%（:%）上没有分片 % 的副本，不能当它的主', shardpg.node_name(v_to), p_to_port, sid;
        END IF;
    END LOOP;
    PERFORM pg_catalog.pg_advisory_xact_lock(b.colocationid, b.minv);
    RETURN array_to_string(b.names, ', ') || ': ' || partdist.move_bucket_primary(b.sids, v_to, 180);
END $fn$;
COMMENT ON FUNCTION shardpg.move_primary(text, integer) IS
    'P7-N94/N102：计划内切主 —— 把分片（及同一哈希段的共置分片）的主切到指定端口的 worker（它必须已有副本）。';

CREATE OR REPLACE FUNCTION shardpg.add_node(p_host text, p_port integer)
RETURNS TABLE (step text, detail text, secs integer) LANGUAGE plpgsql AS $fn$
DECLARE
    t0          timestamptz := clock_timestamp();
    ts          timestamptz;
    v_id        integer;
    v_me        text;                                   -- 新节点在 pg_raft.peers 里的写法：<id>@<host>:<port>
    v_peers     text := current_setting('pg_raft.peers');
    v_new_peers text;
    v_list      text[];
    v_old_n     integer;                                -- 新节点之外的现有节点数
    v_todo      integer := 0;                           -- ② 里这次真正追加的节点数
    v_v         text;
    r           record;
    v_n         integer;
    v_g0        integer;
    v_idx       bigint := 0;
    i           integer;
BEGIN
    -- ① 新节点：连得上、三个扩展装好了、pg_raft.peers = 现有列表 + 它自己
    -- ★ P7-N105：可以续做。①–③ 是在各节点上单独执行的（ALTER SYSTEM、控制面提案），函数中途出错回滚时
    --   撤不掉（现场：④ citus_add_node 提交时出错，重跑就报"节点号已经在集群里"）⇒ 按现状判断哪几步已经做过，
    --   做过的跳过，从没做完的那步接着做
    ts := clock_timestamp();
    v_v := partdist.addr_exec(p_host, p_port, 'SHOW pg_raft.node_id');
    v_id := v_v::integer;
    v_me := v_id || '@' || p_host || ':' || p_port;
    SELECT format('%s:%s', hostname, port) INTO v_v FROM partdist.node_map
     WHERE node_id = v_id AND (hostname <> p_host OR port <> p_port);
    IF FOUND THEN
        RAISE EXCEPTION '节点号 % 已被别的节点占用（node_map 里登记的是 %）', v_id, v_v;
    END IF;
    SELECT node_id::text INTO v_v FROM partdist.node_map
     WHERE node_id <> v_id AND hostname = p_host AND port = p_port;
    IF FOUND THEN
        RAISE EXCEPTION '%:% 在 node_map 里登记的是节点 %，新节点报的是 %', p_host, p_port, v_v, v_id;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_dist_node WHERE nodeport = p_port) THEN
        IF EXISTS (SELECT 1 FROM partdist.node_map WHERE node_id = v_id) THEN
            step := 'done'; secs := 0;
            detail := format('%s (%s:%s) is already in the cluster, nothing to do', shardpg.node_name(v_id), p_host, p_port);
            RETURN NEXT;
            RETURN;
        END IF;
        RAISE EXCEPTION '端口 % 已经是 ShardPG worker，但 node_map 里没有节点 %', p_port, v_id;
    END IF;
    v_list := string_to_array(v_peers, ',');
    IF v_list[array_length(v_list, 1)] = v_me THEN
        -- 上次已经做过 ②（master 自己的 peers 以新节点结尾）：v_peers 还原成追加前的列表
        v_new_peers := v_peers;
        v_peers := array_to_string(v_list[1:array_length(v_list, 1) - 1], ',');
    ELSIF EXISTS (SELECT 1 FROM unnest(v_list) e
                   WHERE split_part(e, '@', 1) = v_id::text OR split_part(e, ':', 2) = p_port::text) THEN
        RAISE EXCEPTION '节点号 % 或端口 % 已在 pg_raft.peers 里，但不是最后一个：%', v_id, p_port, v_peers;
    ELSE
        v_new_peers := v_peers || ',' || v_me;
    END IF;
    SELECT count(*) INTO v_old_n FROM partdist.node_map WHERE node_id <> v_id;
    v_v := partdist.addr_exec(p_host, p_port,
        'SELECT string_agg(extname, '','' ORDER BY extname) FROM pg_extension WHERE extname IN (''citus'', ''pg_partdist'', ''pg_raft'')');
    IF v_v IS DISTINCT FROM 'citus,pg_partdist,pg_raft' THEN
        RAISE EXCEPTION '新节点 %:% 的扩展不全（有：%），先在它上面 CREATE EXTENSION citus / pg_partdist / pg_raft', p_host, p_port, v_v;
    END IF;
    v_v := partdist.addr_exec(p_host, p_port, 'SHOW pg_raft.peers');
    IF v_v <> v_new_peers THEN
        RAISE EXCEPTION '新节点的 pg_raft.peers 应为 %，现为 %', v_new_peers, v_v;
    END IF;
    step := 'new node ready'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('%s (node id %s, %s:%s): extensions installed, pg_raft.peers = the %s existing nodes + itself',
                     shardpg.node_name(v_id), v_id, p_host, p_port, v_old_n);
    RETURN NEXT;

    -- ② 各节点 pg_raft.peers 末尾追加新节点：ALTER SYSTEM + reload，不重启（只许追加一个，见 pg_raft_peers_check_hook）
    --   已经是新列表的节点（上次做过）跳过
    ts := clock_timestamp();
    FOR r IN SELECT node_id, hostname, port FROM partdist.node_map WHERE node_id <> v_id ORDER BY node_id LOOP
        v_v := partdist.addr_exec(r.hostname, r.port, 'SHOW pg_raft.peers');
        IF v_v <> v_peers AND v_v <> v_new_peers THEN
            RAISE EXCEPTION '节点 %（%:%）的 pg_raft.peers 与 master 不一致：%', r.node_id, r.hostname, r.port, v_v;
        END IF;
    END LOOP;
    FOR r IN SELECT node_id, hostname, port FROM partdist.node_map WHERE node_id <> v_id ORDER BY node_id LOOP
        IF partdist.addr_exec(r.hostname, r.port, 'SHOW pg_raft.peers') <> v_new_peers THEN
            PERFORM partdist.addr_exec(r.hostname, r.port, format('ALTER SYSTEM SET pg_raft.peers = %L', v_new_peers));
            PERFORM partdist.addr_exec(r.hostname, r.port, 'SELECT pg_reload_conf()');
            v_todo := v_todo + 1;
        END IF;
    END LOOP;
    FOR i IN 1..30 LOOP
        SELECT count(*) INTO v_n FROM partdist.node_map nm
         WHERE nm.node_id <> v_id
           AND partdist.addr_exec(nm.hostname, nm.port, 'SHOW pg_raft.peers', false) IS DISTINCT FROM v_new_peers;
        EXIT WHEN v_n = 0;
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
    IF v_n > 0 THEN
        RAISE EXCEPTION '% 个节点 reload 之后 pg_raft.peers 仍是旧值（看各节点日志里 pg_raft.peers 的报错）', v_n;
    END IF;
    IF v_todo > 0 THEN
        PERFORM pg_catalog.pg_sleep(2);     -- 等各节点的 Raft 进程在下一次 tick 换上新表
    END IF;
    step := 'append pg_raft.peers'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('reloaded on %s nodes%s; control-plane group 0 members %s -> %s, quorum %s',
                     v_old_n,
                     CASE WHEN v_todo < v_old_n THEN format(' (%s of them already appended by an earlier run)', v_old_n - v_todo) ELSE '' END,
                     v_old_n, v_old_n + 1, (v_old_n + 1) / 2 + 1);
    RETURN NEXT;

    -- ③ 控制面登记新节点地址（各节点的 node_map 按日志序同时学到），等新节点追上控制面日志
    --   登记是 upsert，续做时再提一次也无妨（顺带拿到一个要等新节点追到的 index）
    ts := clock_timestamp();
    FOR i IN 1..30 LOOP
        SELECT leader_node_id INTO v_g0 FROM partdist.pg_raft_group_status() WHERE group_id = 0;
        IF v_g0 IS NOT NULL AND v_g0 > 0 THEN
            BEGIN
                v_idx := partdist.node_exec(v_g0, format(
                    'SELECT partdist.pg_raft_group_propose(0, ''OP_NODE_STATUS'', %L)',
                    json_build_object('node_id', v_id, 'status', 'active', 'hostname', p_host, 'port', p_port)::text))::bigint;
            EXCEPTION WHEN OTHERS THEN
                v_idx := 0;
            END;
        END IF;
        EXIT WHEN v_idx > 0;
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
    IF v_idx <= 0 THEN
        RAISE EXCEPTION '控制面登记新节点失败（组 0 leader = %）', v_g0;
    END IF;
    FOR i IN 1..180 LOOP
        v_v := partdist.addr_exec(p_host, p_port,
            'SELECT last_applied FROM partdist.pg_raft_group_status() WHERE group_id = 0', false);
        EXIT WHEN coalesce(nullif(v_v, '')::bigint, 0) >= v_idx
              AND EXISTS (SELECT 1 FROM partdist.node_map WHERE node_id = v_id AND port = p_port);
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
    IF coalesce(nullif(v_v, '')::bigint, 0) < v_idx THEN
        RAISE EXCEPTION '新节点 3 分钟内没追上控制面日志（它在 %，要到 %）', v_v, v_idx;
    END IF;
    -- 新节点的 node_map 以 master 为准（控制面快照里的旧占位地址一并纠正）
    PERFORM partdist.addr_exec(p_host, p_port, format(
        'INSERT INTO partdist.node_map (node_id, hostname, port, status, last_heartbeat) '
        'SELECT * FROM jsonb_populate_recordset(NULL::partdist.node_map, %L::jsonb) '
        'ON CONFLICT (node_id) DO UPDATE SET hostname = EXCLUDED.hostname, port = EXCLUDED.port',
        (SELECT json_agg(nm)::text FROM partdist.node_map nm)));
    step := 'join control plane'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('%s:%s registered at group 0 log index %s, the new node has caught up', p_host, p_port, v_idx);
    RETURN NEXT;

    -- ④ ShardPG 加 worker：同步元数据，各分布表的外壳建到新节点上
    ts := clock_timestamp();
    -- Node registration only changes Citus catalogs; it has no business shard
    -- to enlist in first-shard coordination. Use a separate metadata-only
    -- transaction, as the peer/address steps above already do. Never change
    -- the caller's protocol setting or include caller business writes here.
    PERFORM partdist.dtx_self(format(
        'SET LOCAL shardpg.first_shard_coordinator=off; '
        'SELECT pg_catalog.citus_add_node(%L,%s)', p_host, p_port));
    PERFORM partdist.addr_exec(p_host, p_port, 'SELECT partdist.rebuild_shard_identity()');
    step := 'shardpg add worker'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('%s:%s added as a ShardPG worker, table metadata synced; no shards on the new node yet, use shardpg.move_replica to move replicas over', p_host, p_port);
    RETURN NEXT;

    step := 'done'; secs := round(extract(epoch FROM clock_timestamp() - t0));
    detail := format('%s (%s:%s) joined the cluster, no node was restarted', shardpg.node_name(v_id), p_host, p_port);
    RETURN NEXT;
END $fn$;
COMMENT ON FUNCTION shardpg.add_node(text, integer) IS
    'P7-N102：新 worker 入簇（不重启）。前提：新节点已按 scripts/new_worker_node.sh 准备好（配置里 pg_raft.peers = 现有列表 + 它自己、建好 citus / pg_partdist / pg_raft 扩展、已启动）。P7-N105：中途失败后直接重跑即可，做过的步骤自动跳过。';

CREATE OR REPLACE FUNCTION shardpg.move_replica(p_shard text, p_from_port integer, p_to_port integer)
RETURNS TABLE (step text, detail text, secs integer) LANGUAGE plpgsql AS $fn$
DECLARE
    t0        timestamptz := clock_timestamp();
    ts        timestamptz;
    b         record;
    v_from    integer;
    v_to      integer;
    v_coord   integer := current_setting('pg_raft.coordinator_node_id')::integer;
    sid       bigint;
    k         integer;
    i         integer;
    v_members integer[];
    v_common  integer[];
    v_leader  integer;
    v_res     text;
    v_need    bigint[] := '{}';
    v_pending integer;
    v_newp    integer;
    v_g0      integer;
    v_idx     bigint;
    v_note    timestamptz;
    v_left    text;
    v_raft    integer[];
    v_done    boolean := true;
BEGIN
    SELECT * INTO b FROM partdist.colocated_bucket(p_shard);
    IF b.sids IS NULL THEN
        RAISE EXCEPTION '找不到分片 %（用 shardpg.shards 里的 shard_name）', p_shard;
    END IF;
    SELECT node_id INTO v_from FROM partdist.node_map WHERE port = p_from_port;
    SELECT node_id INTO v_to FROM partdist.node_map WHERE port = p_to_port;
    IF v_from IS NULL OR v_to IS NULL THEN
        RAISE EXCEPTION 'node_map 里找不到端口 % 或 %（新节点先 shardpg.add_node）', p_from_port, p_to_port;
    END IF;
    IF v_to = v_coord OR v_from = v_coord THEN
        RAISE EXCEPTION 'master 不存数据';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_dist_node WHERE nodeport = p_to_port AND noderole = 'primary' AND isactive) THEN
        RAISE EXCEPTION ':% 还不是 ShardPG worker（先 shardpg.add_node）', p_to_port;
    END IF;
    FOREACH sid IN ARRAY b.sids LOOP
        SELECT ARRAY[primary_node] || secondary_nodes INTO v_members FROM partdist.partition_map WHERE partition_id = sid::oid;
        IF v_members IS NULL THEN
            RAISE EXCEPTION '分片 % 没有主从登记（不是 Raft 管理的表）', sid;
        END IF;
        v_raft := partdist.group_raft_members(sid, ARRAY[v_to]);
        IF v_from = ANY (v_members) OR v_from = ANY (v_raft) THEN
            v_done := false;            -- 还没搬完（重跑时接着做）
        ELSIF NOT v_to = ANY (v_raft) THEN
            RAISE EXCEPTION '%（:%）上没有分片 % 的副本', shardpg.node_name(v_from), p_from_port, sid;
        END IF;
        IF v_to = ANY (v_members) AND v_from = ANY (v_members) THEN
            RAISE EXCEPTION '%（:%）上已经有分片 % 的副本', shardpg.node_name(v_to), p_to_port, sid;
        END IF;
        v_common := CASE WHEN v_common IS NULL THEN v_members
                         ELSE ARRAY(SELECT unnest(v_common) INTERSECT SELECT unnest(v_members)) END;
    END LOOP;
    -- 挡住共置对齐（P7-N94）：迁移期间这一段的主由本函数安排
    PERFORM pg_catalog.pg_advisory_xact_lock(b.colocationid, b.minv);
    step := 'check'; secs := 0;
    detail := format('%s colocated shards of one hash range (%s): move a replica from %s (:%s) to %s (:%s)%s',
                     cardinality(b.sids), array_to_string(b.names, ', '),
                     shardpg.node_name(v_from), p_from_port, shardpg.node_name(v_to), p_to_port,
                     CASE WHEN v_done THEN ' -- already moved, only the cleanup is left' ELSE '' END);
    RETURN NEXT;

    -- 迁移期间这些组暂停自动重供（归队扫尾 / P7-N25）：新成员由本函数供副本，别人再插一脚就是重复基线，
    -- 而移出源节点时那条重复基线会半途失败、在新节点上留下收不了尾的"基线半程"。结束（或出错）时撤销
    PERFORM partdist.reprovision_hold_set(b.sids, true, format('move_replica %s→%s', v_from, v_to));
    BEGIN

    IF NOT v_done THEN
    -- ① 新节点入组：先在新节点上建组（成员 = 现有成员 + 它），再由 leader 走 Raft 日志加成员（一次一个）
    ts := clock_timestamp();
    FOREACH sid IN ARRAY b.sids LOOP
        v_raft := partdist.group_raft_members(sid, ARRAY[v_to]);
        CONTINUE WHEN v_to = ANY (v_raft);          -- 上次已经加进去了
        PERFORM partdist.node_exec(v_to, format('SELECT partdist.pg_raft_group_create(%s, %L::integer[])', sid, v_raft || v_to));
        PERFORM partdist.group_change_member(sid, v_to, true);
    END LOOP;
    step := 'add member'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('%s Raft groups each add member %s (membership changes go through the Raft log, one at a time; now 4 members per group, quorum 3)',
                     cardinality(b.sids), shardpg.node_name(v_to));
    RETURN NEXT;

    -- ② 供副本：leader 先到新节点建副本表，再发物理基线（经 Raft 复制），配回放位置、arm
    ts := clock_timestamp();
    FOREACH sid IN ARRAY b.sids LOOP
        v_leader := partdist.group_leader_now(sid, ARRAY[v_to]);
        IF v_leader IS NULL THEN
            RAISE EXCEPTION '分片 % 现在没有主（正在选举？），稍后重试', sid;
        END IF;
        IF NOT partdist.node_replica_ready(v_to, sid, 0) THEN
            v_note := clock_timestamp();
            v_res := partdist.node_exec(v_leader, format('SELECT partdist.provision_shard_replica(%s, %s)', sid, v_to));
            IF v_res NOT LIKE 'shard=%' THEN
                RAISE EXCEPTION '分片 % 供副本失败：%', sid, v_res;
            END IF;
            RAISE LOG 'move_replica: 分片 % 在节点 % 上供副本用了 % s（leader 节点 %）', sid, v_to,
                round(extract(epoch FROM clock_timestamp() - v_note), 1), v_leader;
        END IF;
        v_need := v_need || partdist.node_group_status(v_leader, sid, 'last_log_index')::bigint;
    END LOOP;
    -- ③ 等新副本收齐：日志追到基线之后、回放槽位 armed（主的追平通道每 5 s 补发一批），
    --    再在新节点上预热回放到已提交位点：基线收尾、索引不是空壳，这份副本才真能顶上
    v_note := clock_timestamp();
    FOR i IN 1..300 LOOP
        v_pending := 0;
        v_left := NULL;                 -- 还没好的分片各自卡在哪（P7-N116：原来只报最后一个分片的状态）
        FOR k IN 1..cardinality(b.sids) LOOP
            IF NOT partdist.node_replica_ready(v_to, b.sids[k], v_need[k]) THEN
                v_pending := v_pending + 1;
                v_left := concat_ws('; ', v_left, format('%s: log not caught up (%s at %s, needs %s)', b.names[k],
                    shardpg.node_name(v_to),
                    coalesce(nullif(partdist.node_group_status(v_to, b.sids[k], 'last_log_index'), ''), '?'),
                    v_need[k]));
            ELSE
                v_res := partdist.node_replica_warm(v_to, b.sids[k]);
                IF v_res <> 'ready' THEN
                    v_pending := v_pending + 1;
                    v_left := concat_ws('; ', v_left, format('%s: %s', b.names[k], v_res));
                END IF;
            END IF;
        END LOOP;
        EXIT WHEN v_pending = 0;
        IF clock_timestamp() - v_note > interval '15 seconds' THEN
            RAISE NOTICE 'waiting for the new replica to catch up: % shard(s) left (% s so far; %)', v_pending,
                round(extract(epoch FROM clock_timestamp() - ts)), v_left;
            v_note := clock_timestamp();
        END IF;
        PERFORM pg_catalog.pg_sleep(2);
    END LOOP;
    IF v_pending > 0 THEN
        RAISE EXCEPTION '新副本 10 分钟内没追平（% 个分片：%）；新节点已在组里，查明原因后重跑本函数会接着做', v_pending, v_left;
    END IF;
    step := 'provision, catch up'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := 'the new replica got a physical baseline plus the log after it, and was warmed up by replay: ready to take over';
    RETURN NEXT;

    -- ④ 源节点若是主，先把这一段的主切到另一个老成员（共置分片一起切）
    IF EXISTS (SELECT 1 FROM partdist.partition_map WHERE partition_id::bigint = ANY (b.sids) AND primary_node = v_from) THEN
        ts := clock_timestamp();
        SELECT min(x) INTO v_newp FROM unnest(v_common) x WHERE x NOT IN (v_from, v_to, v_coord);
        IF v_newp IS NULL THEN
            v_newp := v_to;
        END IF;
        -- 被拉下来的旧主本来会被新主自动重供成副本（P7-N25）；迁移期间暂停自动重供，它马上就被移出
        v_res := partdist.move_bucket_primary(b.sids, v_newp, 180);
        step := 'move primary'; secs := round(extract(epoch FROM clock_timestamp() - ts));
        detail := format('the source node was the primary, moved the primary to %s first: %s', shardpg.node_name(v_newp), v_res);
        RETURN NEXT;
    END IF;

    -- ⑤ 源副本出组（成员变更走日志）
    ts := clock_timestamp();
    FOREACH sid IN ARRAY b.sids LOOP
        CONTINUE WHEN NOT v_from = ANY (partdist.group_raft_members(sid, ARRAY[v_to]));
        PERFORM partdist.group_change_member(sid, v_from, false, ARRAY[v_to]);
    END LOOP;
    -- ⑥ 控制面改登记的从副本集（主、任期不变）
    FOREACH sid IN ARRAY b.sids LOOP
        FOR i IN 1..20 LOOP
            SELECT leader_node_id INTO v_g0 FROM partdist.pg_raft_group_status() WHERE group_id = 0;
            v_idx := 0;
            BEGIN
                v_idx := partdist.node_exec(v_g0, format(
                    'SELECT partdist.pg_raft_group_propose(0, ''OP_PARTITION_SECONDARIES'', %L)',
                    (SELECT json_build_object('partition_id', sid, 'primary_term', m.primary_term,
                                              'secondary_nodes', to_json(ARRAY(
                                                  SELECT DISTINCT x FROM unnest(m.secondary_nodes || v_to) x
                                                   WHERE x NOT IN (v_from, m.primary_node) ORDER BY x)))::text
                       FROM partdist.partition_map m WHERE m.partition_id = sid::oid)))::bigint;
            EXCEPTION WHEN OTHERS THEN
                v_idx := 0;
            END;
            EXIT WHEN v_idx > 0;
            PERFORM pg_catalog.pg_sleep(1);
        END LOOP;
    END LOOP;
    FOR i IN 1..30 LOOP
        EXIT WHEN NOT EXISTS (SELECT 1 FROM partdist.partition_map
                               WHERE partition_id::bigint = ANY (b.sids)
                                 AND (v_from = ANY (ARRAY[primary_node] || secondary_nodes)
                                      OR NOT v_to = ANY (ARRAY[primary_node] || secondary_nodes)));
        PERFORM pg_catalog.pg_sleep(1);
    END LOOP;
    step := 'remove member'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := format('%s Raft groups remove member %s; the control plane now lists the replicas as %s', cardinality(b.sids), shardpg.node_name(v_from),
                     (SELECT string_agg(shardpg.node_name(nm.node_id), ',' ORDER BY nm.node_id) FROM partdist.partition_map m, partdist.node_map nm
                       WHERE m.partition_id = b.sids[1]::oid AND nm.node_id = ANY (m.secondary_nodes)));
    RETURN NEXT;

    END IF;     -- NOT v_done

    -- ⑦ 清理源节点：关回放槽、拆组（拆两遍，在途消息会把组建回来）、删副本表与各项登记
    ts := clock_timestamp();
    v_left := partdist.drop_local_replicas(v_from, b.sids, b.names);

    EXCEPTION WHEN OTHERS THEN
        PERFORM partdist.reprovision_hold_set(b.sids, false);
        RAISE;
    END;
    PERFORM partdist.reprovision_hold_set(b.sids, false);
    step := 'clean up source'; secs := round(extract(epoch FROM clock_timestamp() - ts));
    detail := CASE WHEN v_left = '0|0|0|0'
                   THEN format('Raft groups, replica tables and replay registrations of these %s shards removed from %s', cardinality(b.sids), shardpg.node_name(v_from))
                   ELSE format('leftovers on %s (groups|tables|replica entries|hardstate files = %s): run this function again later, it will only clean up', shardpg.node_name(v_from), v_left) END;
    RETURN NEXT;

    step := 'done'; secs := round(extract(epoch FROM clock_timestamp() - t0));
    detail := (SELECT string_agg(format('%s primary %s, replicas %s', sh.shard_name, sh."primary", sh.replicas), '; ' ORDER BY sh.shard_name)
                 FROM shardpg.shards sh WHERE sh.shard_name = ANY (b.names));
    RETURN NEXT;
END $fn$;
COMMENT ON FUNCTION shardpg.move_replica(text, integer, integer) IS
    'P7-N102：在线把分片的一份副本从一个 worker 搬到另一个（同一哈希段的共置分片一起搬）：新节点入组 → 供副本、追平 → 源节点是主就先切主 → 源节点出组 → 改控制面登记 → 清理源节点。全程可读写。';

-- BEGIN first_shard_coordinator.sql (keep in sync with standalone upgrade)
-- Install on every node after rebuilding Citus with citus-0004 and pg_partdist.
-- The route table lives on the TSO/router. It records only a write-once route;
-- participants, votes and the prepare state machine live on the chosen shard.
CREATE TABLE IF NOT EXISTS partdist.dtx_route (
    gxid bigint PRIMARY KEY CHECK (gxid > 0),
    coord_gsid bigint NOT NULL CHECK (coord_gsid > 0)
);
-- Raft kind 6 is an irrevocable COMMIT without a timestamp. This index is
-- rebuilt on every coordinator replica; it is never a visibility decision.
CREATE TABLE IF NOT EXISTS partdist.dtx_commit_decided (
    dtxid bigint PRIMARY KEY,
    coord_gsid bigint NOT NULL,
    participants bigint[] NOT NULL CHECK(cardinality(participants)>0),
    decided_plsn bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS partdist.dtx_decision_replay (
    coord_gsid bigint PRIMARY KEY,
    plsn bigint NOT NULL DEFAULT 0
);

-- SQL indexes and this cursor commit together. The shared Raft apply cursor
-- alone is insufficient: its advance survives a rollback of the SQL indexes.
-- Called under the coordinator's replication claim, with a committed bound.
CREATE OR REPLACE FUNCTION partdist.dtx_reindex_committed(
    p_coord bigint,p_oid oid,p_upto bigint) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE from_plsn bigint; r record;
BEGIN
    INSERT INTO partdist.dtx_decision_replay(coord_gsid) VALUES(p_coord) ON CONFLICT DO NOTHING;
    SELECT plsn INTO from_plsn FROM partdist.dtx_decision_replay
      WHERE coord_gsid=p_coord FOR UPDATE;
    IF p_upto <= from_plsn THEN RETURN; END IF;
    FOR r IN SELECT g AS plsn,d.* FROM generate_series(from_plsn+1,p_upto) g
             CROSS JOIN LATERAL partdist.partwal_read_dtx_record(p_oid,g) d
             WHERE d.kind IN (2,5,6,7,8,9,10) ORDER BY g
    LOOP
        IF r.coord_gsid<>p_coord THEN RAISE EXCEPTION 'decision in wrong coordinator stream'; END IF;
        IF r.kind IN (7,8,9,10) THEN
            PERFORM partdist.dtx_meta_apply(p_coord,r.dtxid,r.kind,r.participants);
        ELSIF r.kind=6 THEN
            IF r.verdict<>1 OR r.commit_ts<>0 THEN RAISE EXCEPTION 'invalid COMMIT decision'; END IF;
            INSERT INTO partdist.dtx_commit_decided(dtxid,coord_gsid,participants,decided_plsn)
              VALUES(r.dtxid,p_coord,r.participants,r.plsn) ON CONFLICT DO NOTHING;
        ELSIF r.kind=2 THEN
            INSERT INTO partdist.dtx_decision(dtxid,coord_gsid,verdict,commit_ts,participants,decided_plsn)
              VALUES(r.dtxid,p_coord,r.verdict,r.commit_ts,r.participants,r.plsn) ON CONFLICT DO NOTHING;
            IF EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=p_coord) THEN
                INSERT INTO partdist.dtx_ddl_outcome(dtxid,coord_gsid,verdict,commit_ts)
                  VALUES(r.dtxid,p_coord,r.verdict,r.commit_ts) ON CONFLICT DO NOTHING;
            END IF;
        ELSE
            DELETE FROM partdist.dtx_decision WHERE dtxid=r.dtxid;
            DELETE FROM partdist.dtx_commit_decided WHERE dtxid=r.dtxid;
            IF EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=p_coord) THEN
                DELETE FROM partdist.dtx_coord_branch WHERE dtxid=r.dtxid;
                DELETE FROM partdist.dtx_coord_tx WHERE dtxid=r.dtxid;
            END IF;
        END IF;
    END LOOP;
    UPDATE partdist.dtx_decision_replay SET plsn=greatest(plsn,p_upto) WHERE coord_gsid=p_coord;
END $$;
CREATE TABLE IF NOT EXISTS partdist.dtx_coord_tx (
    dtxid bigint PRIMARY KEY,
    coord_gsid bigint NOT NULL,
    phase text NOT NULL DEFAULT 'OPEN' CHECK (phase IN ('OPEN','PREPARE','ABORT')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS partdist.dtx_coord_branch (
    dtxid bigint NOT NULL REFERENCES partdist.dtx_coord_tx(dtxid),
    gid text NOT NULL,
    hostname text NOT NULL,
    port integer NOT NULL,
    gsids bigint[] NOT NULL CHECK (cardinality(gsids) > 0),
    vote boolean,
    PRIMARY KEY(dtxid,gid)
);
CREATE TABLE IF NOT EXISTS partdist.dtx_dispatch_cursor (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    last_id bigint NOT NULL DEFAULT 0
);
INSERT INTO partdist.dtx_dispatch_cursor(singleton) VALUES(true) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION partdist.dtx_rpc(text,integer,text) RETURNS text
AS '$libdir/pg_partdist', 'partdist_dtx_rpc' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_tso_rpc(text) RETURNS text
AS '$libdir/pg_partdist', 'partdist_dtx_tso_rpc' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_self(text) RETURNS text
AS '$libdir/pg_partdist', 'partdist_dtx_self' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_commit_ts() RETURNS bigint
AS '$libdir/pg_partdist', 'partdist_dtx_commit_ts' LANGUAGE c VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_pending_exists(bigint) RETURNS boolean
AS '$libdir/pg_partdist', 'partdist_dtx_pending_exists' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.shard_is_prepared(oid,bigint) RETURNS boolean
AS '$libdir/pg_partdist', 'partdist_shard_is_prepared' LANGUAGE c STRICT VOLATILE;
COMMENT ON FUNCTION partdist.shard_clog_status(oid,bigint) IS
    'Shard-local clog: 0=IN_PROGRESS (including prepared transactions), 2=COMMITTED, 3=ABORTED. Two-phase membership is independent; numeric codes retain the existing disk format.';
CREATE OR REPLACE FUNCTION partdist.dtx_branch_begin(text) RETURNS void
AS '$libdir/pg_partdist', 'partdist_dtx_branch_begin' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_branch_ready() RETURNS boolean
AS '$libdir/pg_partdist', 'partdist_dtx_branch_ready' LANGUAGE c VOLATILE;
CREATE OR REPLACE PROCEDURE partdist.dtx_handoff() LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'dtx_handoff requires the pg_partdist utility hook';
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_route_claim(p_xid bigint, p_shard bigint)
RETURNS bigint LANGUAGE sql VOLATILE SET search_path=pg_catalog,partdist AS $$
    INSERT INTO partdist.dtx_route AS r(gxid,coord_gsid) VALUES(p_xid,p_shard)
    ON CONFLICT(gxid) DO UPDATE SET coord_gsid=r.coord_gsid
    RETURNING coord_gsid
$$;

CREATE OR REPLACE FUNCTION partdist.dtx_enlist(p_xid bigint, p_coord bigint,
    p_gid text, p_host text, p_port integer, p_shards bigint[])
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE t partdist.dtx_coord_tx; old partdist.dtx_coord_branch;
BEGIN
    IF EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=p_coord) THEN
        RETURN partdist.dtx_enlist_native(p_xid,p_coord,p_gid,p_host,p_port,p_shards,NULL);
    END IF;
    IF NOT EXISTS (SELECT FROM partdist.pg_raft_group_status()
                   WHERE group_id=p_coord AND state='leader') THEN
        RAISE EXCEPTION 'this node is not the coordinator shard leader';
    END IF;
    INSERT INTO partdist.dtx_coord_tx(dtxid,coord_gsid) VALUES(p_xid,p_coord)
    ON CONFLICT DO NOTHING;
    SELECT * INTO STRICT t FROM partdist.dtx_coord_tx WHERE dtxid=p_xid FOR UPDATE;
    IF t.coord_gsid <> p_coord OR t.phase <> 'OPEN' THEN
        RAISE EXCEPTION 'coordinator changed or participant set already sealed';
    END IF;
    SELECT * INTO old FROM partdist.dtx_coord_branch WHERE dtxid=p_xid AND gid=p_gid;
    IF FOUND AND (old.hostname <> p_host OR old.port <> p_port OR
                  NOT (old.gsids <@ p_shards)) THEN
        RAISE EXCEPTION 'branch identity changed or shard set shrank';
    END IF;
    INSERT INTO partdist.dtx_coord_branch(dtxid,gid,hostname,port,gsids)
    VALUES(p_xid,p_gid,p_host,p_port,p_shards)
    ON CONFLICT(dtxid,gid) DO UPDATE SET gsids=EXCLUDED.gsids;
    RETURN true;
END $$;

-- Runs in an autonomous transaction. The row lock serializes enlistment and
-- sealing: after this commits, no new participant may silently join.
CREATE OR REPLACE FUNCTION partdist.dtx_seal(p_xid bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE t partdist.dtx_coord_tx; shards bigint[];
BEGIN
    SELECT * INTO STRICT t FROM partdist.dtx_coord_tx WHERE dtxid=p_xid;
    IF EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=t.coord_gsid) THEN
        PERFORM partdist.dtx_meta_event(t.coord_gsid,p_xid,9,NULL);
    END IF;
    SELECT * INTO STRICT t FROM partdist.dtx_coord_tx WHERE dtxid=p_xid FOR UPDATE;
    IF t.phase='ABORT' THEN RAISE EXCEPTION 'transaction already aborted'; END IF;
    IF NOT EXISTS (SELECT FROM partdist.pg_raft_group_status()
                   WHERE group_id=t.coord_gsid AND state='leader') THEN
        RAISE EXCEPTION 'coordinator lost leadership before prepare';
    END IF;
    SELECT array_agg(DISTINCT s ORDER BY s) INTO shards
    FROM partdist.dtx_coord_branch b CROSS JOIN LATERAL unnest(b.gsids) s
    WHERE b.dtxid=p_xid;
    IF coalesce(cardinality(shards),0)=0 OR NOT t.coord_gsid=ANY(shards) THEN
        RAISE EXCEPTION 'coordinator has no complete participant set';
    END IF;
    UPDATE partdist.dtx_coord_tx SET phase='PREPARE' WHERE dtxid=p_xid;
    RETURN jsonb_build_object('coord',t.coord_gsid,'shards',to_jsonb(shards));
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_branch_command(p_xid bigint,p_gid text)
RETURNS text LANGUAGE sql VOLATILE SET search_path=pg_catalog,partdist AS $$
    SELECT t.phase FROM partdist.dtx_coord_tx t JOIN partdist.dtx_coord_branch b USING(dtxid)
    WHERE t.dtxid=p_xid AND b.gid=p_gid
$$;

CREATE OR REPLACE FUNCTION partdist.dtx_vote(p_xid bigint,p_gid text,p_ok boolean)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE phase_now text;
BEGIN
    IF EXISTS(SELECT FROM partdist.dtx_coord_tx t JOIN partdist.dtx_ddl_shard d ON d.group_id=t.coord_gsid WHERE t.dtxid=p_xid) THEN
        PERFORM partdist.dtx_meta_event((SELECT coord_gsid FROM partdist.dtx_coord_tx WHERE dtxid=p_xid),p_xid,8,
            ARRAY[split_part(p_gid,'_',5)::bigint,CASE WHEN p_ok THEN 1::bigint ELSE 0::bigint END]);
        RETURN true;
    END IF;
    SELECT phase INTO STRICT phase_now FROM partdist.dtx_coord_tx WHERE dtxid=p_xid FOR UPDATE;
    IF phase_now='ABORT' THEN RETURN false; END IF;
    IF p_ok AND phase_now <> 'PREPARE' THEN RAISE EXCEPTION 'unexpected prepare vote'; END IF;
    UPDATE partdist.dtx_coord_branch SET vote=p_ok
      WHERE dtxid=p_xid AND gid=p_gid AND (vote IS NULL OR vote=p_ok);
    IF NOT FOUND THEN RAISE EXCEPTION 'unknown branch or conflicting vote'; END IF;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_votes(p_xid bigint)
RETURNS text LANGUAGE sql VOLATILE SET search_path=pg_catalog,partdist AS $$
    SELECT CASE WHEN count(*)=0 THEN 'WAIT'
                WHEN bool_or(vote IS FALSE) THEN 'ABORT'
                WHEN count(*)=count(vote) AND bool_and(vote) THEN 'READY'
                ELSE 'WAIT' END
    FROM partdist.dtx_coord_branch WHERE dtxid=p_xid
$$;

-- The only normal-path prepare driver. The router forwards one client COMMIT
-- here; it neither passes a participant list nor collects the prepare votes.
CREATE OR REPLACE FUNCTION partdist.dtx_record_commit(p_coord bigint,p_xid bigint,
    p_shards bigint[],p_cts bigint) RETURNS bigint
LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE v integer; result bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(p_xid);
    IF p_cts <= 0 THEN RAISE EXCEPTION 'invalid commit timestamp'; END IF;
    IF NOT EXISTS(SELECT FROM partdist.dtx_commit_decided
                  WHERE dtxid=p_xid AND coord_gsid=p_coord AND participants=p_shards) THEN
        RAISE EXCEPTION 'COMMIT decision must precede its timestamp';
    END IF;
    v:=partdist.dtx_decide(p_coord,p_xid,1,p_shards,p_cts);
    IF v IS DISTINCT FROM 1 THEN RAISE EXCEPTION 'coordinator rejected commit'; END IF;
    SELECT commit_ts INTO STRICT result FROM partdist.dtx_decision WHERE dtxid=p_xid;
    RETURN result;
END $$;

-- Also used after a crash between durable COMMIT and timestamp attachment.
-- No failure here can change the already chosen COMMIT into ABORT.
CREATE OR REPLACE FUNCTION partdist.dtx_complete_commit(p_xid bigint)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE d partdist.dtx_commit_decided; cts bigint;
BEGIN
    SELECT commit_ts INTO cts FROM partdist.dtx_decision WHERE dtxid=p_xid AND verdict=1;
    IF FOUND THEN RETURN cts; END IF;
    SELECT * INTO STRICT d FROM partdist.dtx_commit_decided WHERE dtxid=p_xid;
    IF NOT EXISTS(SELECT FROM partdist.pg_raft_group_status()
                  WHERE group_id=d.coord_gsid AND state='leader') THEN
        RAISE EXCEPTION 'coordinator lost leadership before timestamp attachment';
    END IF;
    cts:=partdist.dtx_commit_ts();
    RETURN partdist.dtx_self(format(
        'SELECT partdist.dtx_record_commit(%s,%s,%L::bigint[],%s)',
        d.coord_gsid,p_xid,d.participants,cts))::bigint;
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_coordinate(p_xid bigint)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE plan jsonb; coord bigint; shards bigint[]; votes text; cts bigint;
        started timestamptz:=clock_timestamp(); timeout_ms integer;
BEGIN
    -- A retry must return the same timestamp, never allocate a second one.
    SELECT commit_ts INTO cts FROM partdist.dtx_decision WHERE dtxid=p_xid AND verdict=1;
    IF FOUND THEN RETURN cts; END IF;
    IF EXISTS(SELECT FROM partdist.dtx_commit_decided WHERE dtxid=p_xid) THEN
        RETURN partdist.dtx_complete_commit(p_xid);
    END IF;
    plan:=partdist.dtx_self(format('SELECT partdist.dtx_seal(%s)',p_xid))::jsonb;
    coord:=(plan->>'coord')::bigint;
    SELECT array_agg(x::bigint) INTO shards FROM jsonb_array_elements_text(plan->'shards') x;
    SELECT coalesce((SELECT setting::integer FROM pg_settings
                     WHERE name='shardpg.prepare_timeout_ms'),30000) INTO timeout_ms;
    LOOP
        votes:=partdist.dtx_self(format('SELECT partdist.dtx_votes(%s)',p_xid));
        EXIT WHEN votes='READY';
        IF votes='ABORT' OR clock_timestamp()-started > timeout_ms*interval '1 millisecond' THEN
            PERFORM partdist.dtx_self(format('SELECT partdist.dtx_decide(%s,%s,2,%L::bigint[],0)',
                                            coord,p_xid,shards));
            RAISE EXCEPTION 'participant prepare failed or timed out';
        END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;
    IF partdist.dtx_self(format('SELECT partdist.dtx_choose_commit(%s,%s,%L::bigint[])',
                                coord,p_xid,shards))::integer IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'coordinator rejected COMMIT decision';
    END IF;
    -- The autonomous request above has committed kind 6 to a quorum BEFORE
    -- asking TSO. Only the subsequent timestamped record may be broadcast.
    RETURN partdist.dtx_complete_commit(p_xid);
END $$;

-- PREPARE registration must include the coordinator before the native prepare
-- WAL is durable, including the window before its vote is delivered.
CREATE OR REPLACE FUNCTION partdist.dtx_note_participant(p_dtxid bigint,p_gid text,p_gsids bigint[])
RETURNS boolean LANGUAGE sql VOLATILE SET search_path=pg_catalog,partdist AS $$
    INSERT INTO partdist.dtx_participant(dtxid,gid,gsids,coord_gsid)
    VALUES(p_dtxid,p_gid,p_gsids,
           CASE WHEN left(p_gid,11)='shardpg_fc_' THEN split_part(p_gid,'_',4)::bigint END)
    ON CONFLICT(dtxid,gid) DO UPDATE SET gsids=EXCLUDED.gsids,
      coord_gsid=coalesce(EXCLUDED.coord_gsid,partdist.dtx_participant.coord_gsid),noted_at=now()
    RETURNING true
$$;

CREATE OR REPLACE FUNCTION partdist.dtx_finish_decision(p_xid bigint,p_coord bigint,
    p_verdict integer,p_cts bigint)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE b record;
BEGIN
    IF p_verdict NOT IN (1,2) OR (p_verdict=1 AND p_cts<=0) THEN
        RAISE EXCEPTION 'invalid terminal decision';
    END IF;
    IF EXISTS(SELECT FROM partdist.dtx_participant WHERE dtxid=p_xid
              AND coord_gsid IS DISTINCT FROM p_coord) THEN
        RAISE EXCEPTION 'decision came from a different coordinator';
    END IF;
    PERFORM partdist.dtx_apply_decision(p_xid,p_verdict,p_cts);
    IF partdist.dtx_pending_exists(p_xid) THEN RETURN false; END IF;
    FOR b IN SELECT p.gid FROM partdist.dtx_participant p
             JOIN pg_prepared_xacts x USING(gid)
             WHERE p.dtxid=p_xid AND x.database=current_database()
    LOOP
        BEGIN
            PERFORM partdist.dtx_self(format('%s PREPARED %L',
                        CASE WHEN p_verdict=1 THEN 'COMMIT' ELSE 'ROLLBACK' END,b.gid));
        EXCEPTION WHEN OTHERS THEN
            -- Another recovery worker may already have finished it. An actual
            -- failure remains queued; never acknowledge on connection failure.
            IF EXISTS(SELECT FROM pg_prepared_xacts WHERE gid=b.gid) THEN RETURN false; END IF;
        END;
    END LOOP;
    RETURN true;
END $$;

-- Run on coordinator leaders, outside the client request. The Raft-replicated
-- decision is the outbox: leadership changes need no router-local queue.
CREATE OR REPLACE FUNCTION partdist.dtx_dispatch()
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE d record; n record; delivered integer:=0; result text; complete boolean;
        cursor_id bigint;
BEGIN
    IF NOT pg_try_advisory_xact_lock(1936745540,1) THEN RETURN 0; END IF;
    -- A client ROLLBACK never invokes dtx_coordinate. Once EVERY enrolled
    -- backend has reported its top-level abort, no branch can vote PREPARE.
    -- Record ABORT at the current shard leader through the existing quorum
    -- decision gate; it refuses to overwrite any irrevocable COMMIT. This also
    -- reaps registrations left on an old leader, without inventing a timeout
    -- verdict or deleting an unresolved participant.
    FOR d IN SELECT t.dtxid,t.coord_gsid,a.hostname,a.port,
                    array_agg(DISTINCT g.gsid ORDER BY g.gsid) AS shards
             FROM partdist.dtx_coord_tx t
             JOIN partdist.dtx_coord_branch b USING(dtxid)
             CROSS JOIN LATERAL unnest(b.gsids) AS g(gsid)
             JOIN partdist.partition_map m ON m.partition_id::bigint=t.coord_gsid
             JOIN partdist.node_map a ON a.node_id=m.primary_node
             GROUP BY t.dtxid,t.coord_gsid,a.hostname,a.port
             HAVING bool_and(b.vote IS FALSE)
             ORDER BY t.dtxid LIMIT 32
    LOOP
        BEGIN
            result:=partdist.dtx_rpc(d.hostname,d.port,
                format('SELECT partdist.dtx_decide(%s,%s,2,%L::bigint[],0)',
                       d.coord_gsid,d.dtxid,d.shards));
            IF result IS DISTINCT FROM '2' THEN CONTINUE; END IF;
            complete:=true;
            FOR n IN SELECT DISTINCT hostname,port FROM partdist.node_map
            LOOP
                BEGIN
                    result:=partdist.dtx_rpc(n.hostname,n.port,
                        format('SELECT partdist.dtx_finish_decision(%s,%s,2,0)',
                               d.dtxid,d.coord_gsid));
                    IF result IS DISTINCT FROM 't' THEN complete:=false; END IF;
                EXCEPTION WHEN OTHERS THEN complete:=false;
                END;
            END LOOP;
            IF complete THEN
                IF EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=d.coord_gsid) THEN
                    PERFORM partdist.dtx_rpc(d.hostname,d.port,format('SELECT partdist.dtx_refresh(%s)',d.coord_gsid));
                    PERFORM partdist.dtx_rpc(d.hostname,d.port,format('SELECT partdist.dtx_meta_event(%s,%s,5,NULL)',d.coord_gsid,d.dtxid));
                END IF;
                DELETE FROM partdist.dtx_coord_branch WHERE dtxid=d.dtxid;
                DELETE FROM partdist.dtx_coord_tx WHERE dtxid=d.dtxid;
                PERFORM partdist.dtx_tso_rpc(format(
                    'SELECT partdist.dtx_route_release(%s,%s)',
                    d.dtxid,d.coord_gsid));
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- Keep registration and durable decision for the next dispatch.
            NULL;
        END;
    END LOOP;
    PERFORM partdist.dtx_resolve_first_attempts();
    -- Autonomous refresh releases its index locks before timestamp completion
    -- makes another local RPC. It also repairs indexes lost after quorum commit.
    FOR d IN SELECT group_id FROM partdist.pg_raft_group_status()
             WHERE state='leader' AND group_id>0
    LOOP
        BEGIN
            PERFORM partdist.dtx_self(format('SELECT partdist.dtx_refresh(%s)',d.group_id));
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
    PERFORM partdist.dtx_recover_preparing();
    SELECT last_id INTO cursor_id FROM partdist.dtx_dispatch_cursor WHERE singleton;
    FOR d IN SELECT dd.dtxid FROM partdist.dtx_commit_decided dd
             JOIN partdist.pg_raft_group_status() g ON g.group_id=dd.coord_gsid
             WHERE g.state='leader' AND NOT EXISTS
               (SELECT FROM partdist.dtx_decision f WHERE f.dtxid=dd.dtxid)
             ORDER BY (dd.dtxid <= cursor_id),dd.dtxid LIMIT 32
    LOOP
        BEGIN
            PERFORM partdist.dtx_complete_commit(d.dtxid);
        EXCEPTION WHEN OTHERS THEN
            -- TSO unavailable: retain COMMIT and retry; never broadcast ts=0.
            NULL;
        END;
    END LOOP;
    FOR d IN SELECT dd.* FROM partdist.dtx_decision dd
             JOIN partdist.pg_raft_group_status() g ON g.group_id=dd.coord_gsid
             WHERE g.state='leader'
             ORDER BY (dd.dtxid <= cursor_id),dd.dtxid LIMIT 32
    LOOP
        complete:=true;
        FOR n IN SELECT DISTINCT hostname,port FROM partdist.node_map
        LOOP
            BEGIN
                result:=partdist.dtx_rpc(n.hostname,n.port,
                    format('SELECT partdist.dtx_finish_decision(%s,%s,%s,%s)',
                           d.dtxid,d.coord_gsid,d.verdict,d.commit_ts));
                IF result='t' THEN delivered:=delivered+1; ELSE complete:=false; END IF;
            EXCEPTION WHEN OTHERS THEN
                -- Keep DECISION until the existing per-shard acknowledgement
                -- gates confirm native completion AND quorum verdict delivery.
                complete:=false;
            END;
        END LOOP;
        IF complete THEN
            IF EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=d.coord_gsid) THEN
                PERFORM partdist.dtx_self(format('SELECT partdist.dtx_meta_event(%s,%s,5,NULL)',d.coord_gsid,d.dtxid));
            END IF;
            DELETE FROM partdist.dtx_coord_branch WHERE dtxid=d.dtxid;
            DELETE FROM partdist.dtx_coord_tx WHERE dtxid=d.dtxid;
            BEGIN
                PERFORM partdist.dtx_tso_rpc(format(
                    'SELECT partdist.dtx_route_release(%s,%s)',
                    d.dtxid,d.coord_gsid));
            EXCEPTION WHEN OTHERS THEN
                -- Failure to remove a route must not change a committed outcome.
                NULL;
            END;
        END IF;
        UPDATE partdist.dtx_dispatch_cursor SET last_id=d.dtxid WHERE singleton;
    END LOOP;
    RETURN delivered;
END $$;

REVOKE ALL ON partdist.dtx_route,partdist.dtx_coord_tx,partdist.dtx_coord_branch,
    partdist.dtx_dispatch_cursor,partdist.dtx_commit_decided,partdist.dtx_decision_replay FROM PUBLIC;
REVOKE ALL ON FUNCTION partdist.dtx_rpc(text,integer,text),partdist.dtx_tso_rpc(text),
    partdist.dtx_self(text),partdist.dtx_route_claim(bigint,bigint),
    partdist.dtx_enlist(bigint,bigint,text,text,integer,bigint[]),
    partdist.dtx_seal(bigint),partdist.dtx_vote(bigint,text,boolean),
    partdist.dtx_branch_command(bigint,text),partdist.dtx_votes(bigint),
    partdist.dtx_coordinate(bigint),partdist.dtx_dispatch(),
    partdist.dtx_record_commit(bigint,bigint,bigint[],bigint),
    partdist.dtx_complete_commit(bigint),
    partdist.dtx_reindex_committed(bigint,oid,bigint),
    partdist.dtx_finish_decision(bigint,bigint,integer,bigint) FROM PUBLIC;

-- BEGIN ddl_metadata.sql (keep in sync with standalone upgrade)
-- Persistent, decision-only shards for DDL. Business shard identity and MVCC
-- remain separate; Citus shard IDs are reserved from its global sequence.
CREATE TABLE IF NOT EXISTS partdist.dtx_ddl_shard (
    owner_node integer PRIMARY KEY CHECK(owner_node>0),
    group_id bigint UNIQUE NOT NULL CHECK(group_id>0),
    anchor_oid oid
);
CREATE TABLE IF NOT EXISTS partdist.dtx_ddl_outcome (
    dtxid bigint PRIMARY KEY, coord_gsid bigint NOT NULL,
    verdict integer NOT NULL CHECK(verdict IN (1,2)), commit_ts bigint NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON partdist.dtx_ddl_outcome FROM PUBLIC;
CREATE TABLE IF NOT EXISTS partdist.dtx_ddl_rejected (
 coord_gsid bigint NOT NULL,dtxid bigint NOT NULL,
 PRIMARY KEY(coord_gsid,dtxid)
);
REVOKE ALL ON partdist.dtx_ddl_rejected FROM PUBLIC;

REVOKE ALL ON partdist.dtx_ddl_shard FROM PUBLIC;

CREATE OR REPLACE FUNCTION partdist.local_partition_for_shard(p_global_shard_id bigint)
RETURNS oid LANGUAGE sql STABLE AS $$
    SELECT local_oid FROM partdist.shard_identity WHERE global_shard_id=p_global_shard_id
    UNION ALL
    SELECT anchor_oid FROM partdist.dtx_ddl_shard
      WHERE group_id=p_global_shard_id AND anchor_oid IS NOT NULL
$$;
CREATE OR REPLACE FUNCTION partdist.global_id_for_partition(p_local_oid oid)
RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT global_shard_id FROM partdist.shard_identity WHERE local_oid=p_local_oid
    UNION ALL
    SELECT group_id FROM partdist.dtx_ddl_shard WHERE anchor_oid=p_local_oid
$$;

CREATE OR REPLACE FUNCTION partdist.dtx_branch_set_ddl() RETURNS void
AS '$libdir/pg_partdist', 'partdist_dtx_branch_set_ddl' LANGUAGE c VOLATILE;

CREATE OR REPLACE FUNCTION partdist.dtx_ddl_install(
    p_owner integer,p_group bigint,p_members integer[])
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist
SET citus.enable_ddl_propagation=off AS $$
DECLARE old partdist.dtx_ddl_shard; anchor oid; name text;
BEGIN
    IF NOT current_setting('pg_raft.node_id')::integer=ANY(p_members) THEN
        RAISE EXCEPTION 'DDL shard replica must be a worker member';
    END IF;
    SELECT * INTO old FROM partdist.dtx_ddl_shard WHERE owner_node=p_owner;
    IF FOUND AND old.group_id<>p_group THEN RAISE EXCEPTION 'DDL shard identity changed'; END IF;
    name:=format('dtx_ddl_anchor_%s',p_owner);
    EXECUTE format('CREATE TABLE IF NOT EXISTS partdist.%I (reserved boolean)',name);
    anchor:=to_regclass(format('partdist.%I',name))::oid;
    INSERT INTO partdist.dtx_ddl_shard(owner_node,group_id,anchor_oid)
      VALUES(p_owner,p_group,anchor)
      ON CONFLICT(owner_node) DO UPDATE SET anchor_oid=EXCLUDED.anchor_oid;
    PERFORM partdist.pg_raft_group_create(p_group,p_members);
    RETURN true;
END $$;

-- Allocation commits independently before any worker receives its identity.
-- Retrying a partial initialization therefore reuses the same global IDs.
CREATE OR REPLACE FUNCTION partdist.dtx_ddl_allocate(p_owner integer)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE gid bigint;
BEGIN
    INSERT INTO partdist.dtx_ddl_shard AS d(owner_node,group_id)
      VALUES(p_owner,nextval('pg_catalog.pg_dist_shardid_seq'))
      ON CONFLICT(owner_node) DO UPDATE SET group_id=d.group_id
      RETURNING group_id INTO gid;
    RETURN gid;
END $$;

-- Called through an independent router connection before opening DDL task
-- sockets. Never change first_shard_coordinator or reuse a business table.
CREATE OR REPLACE FUNCTION partdist.dtx_ddl_ensure()
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE workers integer[]; owner integer; gid bigint; n record; answer text;
        started timestamptz:=clock_timestamp(); ready boolean;
BEGIN
    IF current_setting('pg_raft.node_id')::integer <>
       current_setting('pg_raft.coordinator_node_id')::integer THEN
        RAISE EXCEPTION 'DDL metadata initialization must run on the router';
    END IF;
    PERFORM pg_advisory_xact_lock(1936745540,2);
    SELECT array_agg(node_id ORDER BY node_id) INTO workers FROM partdist.node_map
      WHERE status='active' AND node_id<>current_setting('pg_raft.coordinator_node_id')::integer;
    IF cardinality(workers)<1 OR workers IS NULL THEN RAISE EXCEPTION 'no DDL worker members'; END IF;
    FOREACH owner IN ARRAY workers LOOP
        SELECT group_id INTO gid FROM partdist.dtx_ddl_shard WHERE owner_node=owner;
        IF gid IS NULL THEN
            gid:=partdist.dtx_self(format('SELECT partdist.dtx_ddl_allocate(%s)',owner))::bigint;
        END IF;
        FOR n IN SELECT hostname,port FROM partdist.node_map WHERE node_id=ANY(workers) LOOP
            answer:=partdist.dtx_rpc(n.hostname,n.port,format(
                'SELECT partdist.dtx_ddl_install(%s,%s,%L::integer[])',owner,gid,workers));
            IF answer IS DISTINCT FROM 't' THEN RAISE EXCEPTION 'DDL shard installation failed'; END IF;
        END LOOP;
    END LOOP;
    LOOP
        SELECT bool_and(EXISTS(SELECT FROM partdist.partition_map m
                  WHERE m.partition_id::bigint=d.group_id AND m.primary_node=ANY(workers)))
          INTO ready FROM partdist.dtx_ddl_shard d WHERE d.owner_node=ANY(workers);
        IF ready THEN RETURN true; END IF;
        IF clock_timestamp()-started>interval '25 seconds' THEN
            RAISE EXCEPTION 'DDL metadata shard election or route publication timed out';
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END $$;

ALTER TABLE partdist.dtx_coord_branch ADD COLUMN IF NOT EXISTS native_fullxid bigint;

CREATE OR REPLACE FUNCTION partdist.dtx_branch_probe(text,bigint) RETURNS text
AS '$libdir/pg_partdist', 'partdist_dtx_branch_probe' LANGUAGE c STRICT VOLATILE;

-- Control entries 7/8/9 are restricted to permanent metadata groups.
CREATE OR REPLACE FUNCTION partdist.dtx_meta_event(bigint,bigint,integer,bigint[])
RETURNS boolean AS '$libdir/pg_raft','pg_raft_dtx_metadata_event' LANGUAGE c VOLATILE;

CREATE OR REPLACE FUNCTION partdist.dtx_meta_validate(c bigint,x bigint,k integer,v bigint[])
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE t partdist.dtx_coord_tx; b partdist.dtx_coord_branch; branch_key text; a record; shards bigint[];
BEGIN
 IF NOT EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=c) THEN RAISE EXCEPTION 'not a DDL metadata group'; END IF;
 IF k=0 THEN
   SELECT * INTO b FROM partdist.dtx_coord_branch b0 WHERE b0.dtxid=x AND b0.gid=format('shardpg_fc_%s_%s_%s',x,c,v[1]);
   IF FOUND THEN
     IF coalesce(v[2],0)<>0 AND b.native_fullxid IS DISTINCT FROM v[2] THEN RAISE EXCEPTION 'DDL admission probe identity changed'; END IF;
     RETURN true;
   END IF;
   RETURN false;
 END IF;
 IF k=10 THEN RETURN NOT EXISTS(SELECT FROM partdist.dtx_ddl_rejected WHERE coord_gsid=c AND dtxid=x); END IF;
 IF k=5 THEN
   IF NOT EXISTS(SELECT FROM partdist.dtx_ddl_outcome WHERE dtxid=x AND coord_gsid=c) THEN
     RAISE EXCEPTION 'DDL completion has no durable outcome'; END IF;
   RETURN EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=x);
 END IF;
 SELECT * INTO t FROM partdist.dtx_coord_tx WHERE dtxid=x;
 IF FOUND AND t.coord_gsid<>c THEN RAISE EXCEPTION 'DDL coordinator identity changed'; END IF;
 IF k=7 THEN
   IF EXISTS(SELECT FROM partdist.dtx_ddl_rejected WHERE coord_gsid=c AND dtxid=x) OR EXISTS(SELECT FROM partdist.dtx_ddl_outcome WHERE dtxid=x) THEN
     RAISE EXCEPTION 'DDL admission was rejected or transaction already completed'; END IF;
   IF cardinality(v)<5 OR v[1]<=0 OR v[2]<0 OR v[3]<0 OR v[4]<1 OR cardinality(v)<>v[4]+4 THEN
     RAISE EXCEPTION 'invalid DDL admission payload'; END IF;
   IF t.dtxid IS NOT NULL AND t.phase<>'OPEN' THEN RAISE EXCEPTION 'DDL participant set is sealed'; END IF;
   SELECT hostname,port INTO STRICT a FROM partdist.node_map WHERE node_id=v[1];
   branch_key:=format('shardpg_fc_%s_%s_%s',x,c,v[2]);shards:=v[5:cardinality(v)];
   SELECT * INTO b FROM partdist.dtx_coord_branch b0 WHERE b0.dtxid=x AND b0.gid=branch_key;
   IF FOUND THEN
     IF b.hostname<>a.hostname OR b.port<>a.port OR NOT b.gsids<@shards OR
        (b.native_fullxid IS NOT NULL AND v[3]<>0 AND b.native_fullxid<>v[3]) THEN
       RAISE EXCEPTION 'DDL branch identity changed'; END IF;
     RETURN b.gsids<>shards OR (b.native_fullxid IS NULL AND v[3]<>0);
   END IF;
   RETURN true;
 ELSIF k=8 THEN
   IF cardinality(v)<>2 OR v[2] NOT IN (0,1) THEN RAISE EXCEPTION 'invalid DDL vote'; END IF;
   branch_key:=format('shardpg_fc_%s_%s_%s',x,c,v[1]);
   SELECT * INTO STRICT b FROM partdist.dtx_coord_branch b0 WHERE b0.dtxid=x AND b0.gid=branch_key;
   IF v[2]=1 AND t.phase<>'PREPARE' THEN RAISE EXCEPTION 'unexpected DDL prepare vote'; END IF;
   IF b.vote IS NOT NULL AND b.vote<>(v[2]=1) THEN RAISE EXCEPTION 'conflicting DDL vote'; END IF;
   RETURN b.vote IS NULL;
 ELSIF k=9 THEN
   IF t.dtxid IS NULL OR t.phase='ABORT' THEN RAISE EXCEPTION 'DDL transaction cannot seal'; END IF;
   SELECT array_agg(DISTINCT s ORDER BY s) INTO shards FROM partdist.dtx_coord_branch b0 CROSS JOIN LATERAL unnest(b0.gsids)s WHERE b0.dtxid=x;
   IF cardinality(shards)<1 OR NOT c=ANY(shards) THEN RAISE EXCEPTION 'DDL coordinator has no participant'; END IF;
   RETURN t.phase<>'PREPARE';
 END IF;
 RAISE EXCEPTION 'unknown DDL control event';
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_meta_apply(c bigint,x bigint,k integer,v bigint[])
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE a record; branch_key text;
BEGIN
 IF k=10 THEN
   INSERT INTO partdist.dtx_ddl_rejected(coord_gsid,dtxid) VALUES(c,x) ON CONFLICT DO NOTHING;
 ELSIF k=7 THEN
   SELECT hostname,port INTO STRICT a FROM partdist.node_map WHERE node_id=v[1];
   branch_key:=format('shardpg_fc_%s_%s_%s',x,c,v[2]);
   INSERT INTO partdist.dtx_coord_tx(dtxid,coord_gsid) VALUES(x,c) ON CONFLICT DO NOTHING;
   INSERT INTO partdist.dtx_coord_branch AS b(dtxid,gid,hostname,port,gsids,native_fullxid)
     VALUES(x,branch_key,a.hostname,a.port,v[5:cardinality(v)],nullif(v[3],0))
     ON CONFLICT(dtxid,gid) DO UPDATE SET gsids=EXCLUDED.gsids,native_fullxid=coalesce(b.native_fullxid,EXCLUDED.native_fullxid);
 ELSIF k=8 THEN
   branch_key:=format('shardpg_fc_%s_%s_%s',x,c,v[1]);
   UPDATE partdist.dtx_coord_branch b SET vote=(v[2]=1) WHERE b.dtxid=x AND b.gid=branch_key;
 ELSIF k=9 THEN
   UPDATE partdist.dtx_coord_tx SET phase='PREPARE' WHERE dtxid=x;
 END IF;
END $$;
REVOKE ALL ON FUNCTION partdist.dtx_meta_event(bigint,bigint,integer,bigint[]),
 partdist.dtx_meta_validate(bigint,bigint,integer,bigint[]),
 partdist.dtx_meta_apply(bigint,bigint,integer,bigint[]) FROM PUBLIC;

CREATE OR REPLACE FUNCTION partdist.dtx_enlist_native(
    p_xid bigint,p_coord bigint,p_gid text,p_host text,p_port integer,
    p_shards bigint[],p_native bigint)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE origin integer;
BEGIN
    IF EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=p_coord) THEN
      SELECT node_id INTO STRICT origin FROM partdist.node_map WHERE hostname=p_host AND port=p_port;
      IF p_gid<>format('shardpg_fc_%s_%s_%s',p_xid,p_coord,split_part(p_gid,'_',5)) THEN
        RAISE EXCEPTION 'invalid DDL branch identifier'; END IF;
      PERFORM partdist.dtx_meta_event(p_coord,p_xid,7,
        ARRAY[origin::bigint,split_part(p_gid,'_',5)::bigint,coalesce(p_native,0),cardinality(p_shards)::bigint]||p_shards);
      RETURN true;
    END IF;
    PERFORM partdist.dtx_enlist(p_xid,p_coord,p_gid,p_host,p_port,p_shards);
    UPDATE partdist.dtx_coord_branch SET native_fullxid=coalesce(native_fullxid,p_native)
      WHERE dtxid=p_xid AND gid=p_gid
        AND (native_fullxid IS NULL OR p_native IS NULL OR native_fullxid=p_native);
    IF NOT FOUND THEN RAISE EXCEPTION 'native branch identity changed'; END IF;
    RETURN true;
END $$;

-- The intent is durable BEFORE contacting a candidate. A lost admission reply
-- pins the candidate until a quorum resolves admission or records rejection.
CREATE TABLE IF NOT EXISTS partdist.dtx_ddl_first_attempt (
 gxid bigint PRIMARY KEY,candidate bigint NOT NULL,connection_id bigint NOT NULL,
 origin_host text NOT NULL,origin_port integer NOT NULL,shards bigint[] NOT NULL,
 native_fullxid bigint,state text NOT NULL CHECK(state IN ('PENDING','ACTIVE','REJECTED','DONE')),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON partdist.dtx_ddl_first_attempt FROM PUBLIC;

CREATE OR REPLACE FUNCTION partdist.dtx_first_reserve(x bigint,c bigint,conn bigint,h text,p integer,s bigint[],native bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE a partdist.dtx_ddl_first_attempt;
BEGIN
 INSERT INTO partdist.dtx_ddl_first_attempt AS old(gxid,candidate,connection_id,origin_host,origin_port,shards,native_fullxid,state)
 VALUES(x,c,conn,h,p,s,native,'PENDING')
 ON CONFLICT(gxid) DO UPDATE SET
 candidate=CASE WHEN old.state='REJECTED' THEN EXCLUDED.candidate ELSE old.candidate END,
 connection_id=CASE WHEN old.state='REJECTED' THEN EXCLUDED.connection_id ELSE old.connection_id END,
 origin_host=CASE WHEN old.state='REJECTED' THEN EXCLUDED.origin_host ELSE old.origin_host END,
 origin_port=CASE WHEN old.state='REJECTED' THEN EXCLUDED.origin_port ELSE old.origin_port END,
 shards=CASE WHEN old.state='REJECTED' THEN EXCLUDED.shards ELSE old.shards END,
 native_fullxid=CASE WHEN old.state='REJECTED' THEN EXCLUDED.native_fullxid ELSE old.native_fullxid END,
 state=CASE WHEN old.state='REJECTED' THEN 'PENDING' ELSE old.state END,updated_at=clock_timestamp()
 RETURNING * INTO a;
 RETURN to_jsonb(a);
END $$;
CREATE OR REPLACE FUNCTION partdist.dtx_first_publish(x bigint,c bigint)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE chosen bigint;
BEGIN
 IF NOT EXISTS(SELECT FROM partdist.dtx_ddl_first_attempt WHERE gxid=x AND candidate=c AND state IN ('PENDING','ACTIVE')) THEN
   RAISE EXCEPTION 'DDL first registration intent is not publishable'; END IF;
 INSERT INTO partdist.dtx_route AS r(gxid,coord_gsid) VALUES(x,c)
 ON CONFLICT(gxid) DO UPDATE SET coord_gsid=r.coord_gsid RETURNING coord_gsid INTO chosen;
 IF chosen<>c THEN RAISE EXCEPTION 'DDL first coordinator changed'; END IF;
 UPDATE partdist.dtx_ddl_first_attempt SET state='ACTIVE',updated_at=clock_timestamp() WHERE gxid=x;
 RETURN chosen;
END $$;
CREATE OR REPLACE FUNCTION partdist.dtx_first_reject(x bigint,c bigint)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
BEGIN
 UPDATE partdist.dtx_ddl_first_attempt SET state='REJECTED',updated_at=clock_timestamp()
 WHERE gxid=x AND candidate=c AND state='PENDING';
 RETURN true;
END $$;
CREATE OR REPLACE FUNCTION partdist.dtx_route_release(x bigint,c bigint)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
BEGIN
 DELETE FROM partdist.dtx_route WHERE gxid=x AND coord_gsid=c;
 UPDATE partdist.dtx_ddl_first_attempt SET state='DONE',updated_at=clock_timestamp()
 WHERE gxid=x AND candidate=c AND state='ACTIVE';
 RETURN true;
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_register_first_native(
 p_xid bigint,p_candidate bigint,p_connection bigint,p_host text,p_port integer,p_shards bigint[],p_native bigint)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE coord bigint;n record;gid text;answer text;a jsonb;old_error text;
BEGIN
 IF p_xid<=0 OR p_candidate<=0 OR p_connection<0 OR cardinality(p_shards)<1 OR NOT p_candidate=ANY(p_shards) THEN
   RAISE EXCEPTION 'invalid first registration'; END IF;
 PERFORM pg_advisory_xact_lock(p_xid);
 SELECT coord_gsid INTO coord FROM partdist.dtx_route WHERE gxid=p_xid;
 IF coord IS NULL THEN
   a:=partdist.dtx_self(format('SELECT partdist.dtx_first_reserve(%s,%s,%s,%L,%s,%L::bigint[],%s)',
     p_xid,p_candidate,p_connection,p_host,p_port,p_shards,coalesce(p_native::text,'NULL')))::jsonb;
   IF a->>'state'='DONE' THEN RAISE EXCEPTION 'DDL transaction registration already completed'; END IF;
   coord:=(a->>'candidate')::bigint;
   SELECT m.hostname,m.port INTO STRICT n FROM partdist.partition_map pm JOIN partdist.node_map m ON m.node_id=pm.primary_node WHERE pm.partition_id::bigint=coord;
   gid:=format('shardpg_fc_%s_%s_%s',p_xid,coord,a->>'connection_id');
   BEGIN
     answer:=partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_enlist_native(%s,%s,%L,%L,%s,%L::bigint[],%s)',
       p_xid,coord,gid,a->>'origin_host',a->>'origin_port',ARRAY(SELECT jsonb_array_elements_text(a->'shards')::bigint),coalesce(a->>'native_fullxid','NULL')));
     IF answer IS DISTINCT FROM 't' THEN RAISE EXCEPTION 'participant admission failed'; END IF;
   EXCEPTION WHEN OTHERS THEN
     GET STACKED DIAGNOSTICS old_error=MESSAGE_TEXT;
     BEGIN
       answer:=partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_meta_event(%s,%s,0,ARRAY[%s,%s]::bigint[])',
         coord,p_xid,a->>'connection_id',coalesce(a->>'native_fullxid','0')));
     EXCEPTION WHEN OTHERS THEN
       RAISE EXCEPTION 'DDL admission outcome is unknown; candidate remains pinned: %',old_error;
     END;
     IF answer IS DISTINCT FROM 't' THEN
       PERFORM partdist.dtx_self(format('SELECT partdist.dtx_first_reject(%s,%s)',p_xid,coord));
       RAISE EXCEPTION 'DDL candidate rejected its first registration: %',old_error;
     END IF;
   END;
   coord:=partdist.dtx_self(format('SELECT partdist.dtx_first_publish(%s,%s)',p_xid,coord))::bigint;
 END IF;
 SELECT m.hostname,m.port INTO STRICT n FROM partdist.partition_map pm JOIN partdist.node_map m ON m.node_id=pm.primary_node WHERE pm.partition_id::bigint=coord;
 gid:=format('shardpg_fc_%s_%s_%s',p_xid,coord,p_connection);
 answer:=partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_enlist_native(%s,%s,%L,%L,%s,%L::bigint[],%s)',
   p_xid,coord,gid,p_host,p_port,p_shards,coalesce(p_native::text,'NULL')));
 IF answer IS DISTINCT FROM 't' THEN RAISE EXCEPTION 'participant admission failed'; END IF;
 RETURN coord;
END $$;

-- Only the TSO/router resolves abandoned first-attempt intents. A quorum
-- rejection fence is required before trying a different candidate.
CREATE OR REPLACE FUNCTION partdist.dtx_resolve_first_attempts()
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE a partdist.dtx_ddl_first_attempt;n record;answer text;status text;count integer:=0;
BEGIN
 IF current_setting('pg_raft.node_id')::integer<>current_setting('pg_raft.coordinator_node_id')::integer THEN RETURN 0; END IF;
 FOR a IN SELECT * FROM partdist.dtx_ddl_first_attempt WHERE state IN ('PENDING','ACTIVE')
   AND updated_at<clock_timestamp()-interval '5 seconds' ORDER BY updated_at LIMIT 16 LOOP
  IF NOT pg_try_advisory_xact_lock(a.gxid) THEN CONTINUE; END IF;
  BEGIN
   SELECT m.hostname,m.port INTO STRICT n FROM partdist.partition_map pm JOIN partdist.node_map m ON m.node_id=pm.primary_node WHERE pm.partition_id::bigint=a.candidate;
   IF a.state='PENDING' THEN
    answer:=partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_meta_event(%s,%s,0,ARRAY[%s,%s]::bigint[])',a.candidate,a.gxid,a.connection_id,coalesce(a.native_fullxid,0)));
    IF answer='t' THEN
      PERFORM partdist.dtx_self(format('SELECT partdist.dtx_first_publish(%s,%s)',a.gxid,a.candidate));
    ELSIF answer='f' THEN
      PERFORM partdist.dtx_self(format('SELECT partdist.dtx_first_reject(%s,%s)',a.gxid,a.candidate));
      CONTINUE;
    ELSE CONTINUE;
    END IF;
   END IF;
   IF a.native_fullxid IS NOT NULL THEN
    answer:=format('shardpg_fc_%s_%s_%s',a.gxid,a.candidate,a.connection_id);
    status:=partdist.dtx_rpc(a.origin_host,a.origin_port,format('SELECT partdist.dtx_branch_probe(%L,%s)',answer,a.native_fullxid));
    IF status='ABORTED' THEN
      PERFORM partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_vote(%s,%L,false)',a.gxid,answer));
    END IF;
   END IF;
   count:=count+1;
  EXCEPTION WHEN OTHERS THEN NULL;
  END;
 END LOOP;
 RETURN count;
END $$;
REVOKE ALL ON FUNCTION partdist.dtx_first_reserve(bigint,bigint,bigint,text,integer,bigint[],bigint),
 partdist.dtx_first_publish(bigint,bigint),partdist.dtx_first_reject(bigint,bigint),
 partdist.dtx_route_release(bigint,bigint),partdist.dtx_resolve_first_attempts() FROM PUBLIC;

CREATE OR REPLACE FUNCTION partdist.dtx_register_first(
    p_xid bigint,p_candidate bigint,p_connection bigint,
    p_host text,p_port integer,p_shards bigint[])
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
BEGIN
    RETURN partdist.dtx_register_first_native(p_xid,p_candidate,p_connection,p_host,p_port,p_shards,NULL);
END $$;

-- Recover missing votes after an original router or task backend exits.
-- Native PREPARE is the durable vote witness; active or unprovable xids are
-- retained. A native abort permits ABORT, never an override of durable COMMIT.
CREATE OR REPLACE FUNCTION partdist.dtx_recover_preparing()
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE d record; b record; answer text; votes text; count integer:=0;
BEGIN
    FOR d IN SELECT t.dtxid FROM partdist.dtx_coord_tx t
      JOIN partdist.pg_raft_group_status() g ON g.group_id=t.coord_gsid
      WHERE t.phase='PREPARE' AND g.state='leader'
        AND NOT EXISTS(SELECT FROM partdist.dtx_decision x WHERE x.dtxid=t.dtxid)
        AND NOT EXISTS(SELECT FROM partdist.dtx_commit_decided x WHERE x.dtxid=t.dtxid)
        AND NOT EXISTS(SELECT FROM partdist.dtx_ddl_outcome x WHERE x.dtxid=t.dtxid)
      ORDER BY t.dtxid LIMIT 32
    LOOP
      FOR b IN SELECT * FROM partdist.dtx_coord_branch WHERE dtxid=d.dtxid
                 AND vote IS NULL AND native_fullxid IS NOT NULL
      LOOP
        BEGIN
          answer:=partdist.dtx_rpc(b.hostname,b.port,format(
            'SELECT partdist.dtx_branch_probe(%L,%s)',b.gid,b.native_fullxid));
          IF answer IN ('PREPARED','ABORTED') THEN
            PERFORM partdist.dtx_self(format('SELECT partdist.dtx_vote(%s,%L,%s)',
              d.dtxid,b.gid,CASE WHEN answer='PREPARED' THEN 'true' ELSE 'false' END));
          END IF;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
      END LOOP;
      votes:=partdist.dtx_self(format('SELECT partdist.dtx_votes(%s)',d.dtxid));
      IF votes IN ('READY','ABORT') THEN
        BEGIN
          PERFORM partdist.dtx_self(format('SELECT partdist.dtx_coordinate(%s)',d.dtxid));
          count:=count+1;
        EXCEPTION WHEN OTHERS THEN NULL; -- durable ABORT also raises to its caller
        END;
      END IF;
    END LOOP;
    RETURN count;
END $$;

-- Retained DDL receipts are rebuilt from quorum records and survive FORGET.
-- A router resuming after native PREPARE can obtain the same terminal answer
-- even when background recovery completed phase two before it reconnects.
CREATE OR REPLACE FUNCTION partdist.dtx_ddl_finish(p_coord bigint,p_xid bigint)
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE v integer;
BEGIN
    -- Refresh takes replay-index row locks. Release them before coordinate
    -- opens another connection to seal, vote, or record a decision.
    PERFORM partdist.dtx_self(format('SELECT partdist.dtx_refresh(%s)',p_coord));
    SELECT verdict INTO v FROM partdist.dtx_ddl_outcome WHERE dtxid=p_xid AND coord_gsid=p_coord;
    IF v IS NOT NULL THEN RETURN v; END IF;
    BEGIN
      PERFORM partdist.dtx_coordinate(p_xid);
    EXCEPTION WHEN OTHERS THEN
      PERFORM partdist.dtx_self(format('SELECT partdist.dtx_refresh(%s)',p_coord));
      SELECT verdict INTO v FROM partdist.dtx_ddl_outcome WHERE dtxid=p_xid AND coord_gsid=p_coord;
      IF v IS NULL THEN RAISE; END IF;
      RETURN v;
    END;
    PERFORM partdist.dtx_self(format('SELECT partdist.dtx_refresh(%s)',p_coord));
    SELECT verdict INTO STRICT v FROM partdist.dtx_ddl_outcome WHERE dtxid=p_xid AND coord_gsid=p_coord;
    RETURN v;
END $$;

REVOKE ALL ON FUNCTION partdist.dtx_branch_set_ddl(),
 partdist.dtx_ddl_install(integer,bigint,integer[]),partdist.dtx_ddl_ensure(),
 partdist.dtx_ddl_allocate(integer),
 partdist.dtx_register_first(bigint,bigint,bigint,text,integer,bigint[]),
 partdist.dtx_register_first_native(bigint,bigint,bigint,text,integer,bigint[],bigint),
 partdist.dtx_enlist_native(bigint,bigint,text,text,integer,bigint[],bigint),
 partdist.dtx_branch_probe(text,bigint),partdist.dtx_recover_preparing(),
 partdist.dtx_ddl_finish(bigint,bigint) FROM PUBLIC;
