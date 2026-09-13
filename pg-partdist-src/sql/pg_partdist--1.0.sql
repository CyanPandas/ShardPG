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

-- T5.4（设计 §6.4 顺序铁律）：一整趟页面动作 ③→①→②。
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
    'T5.4：一整趟页面动作（③ xmax 消毒 → ① 删中止 xmin → ② 删已提交删除的死元组）。③ 必须最先——① 对"仍挂 HOT 链的 heap-only 元组"的推迟要靠 ③ 清 xmax 才解除。整趟干净才落 shard_vacuum_xid 标记，否则返回 swept=false 且不动水位，截断随之被拦。';

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
    'T5.4：截断分片 clog 到 p_trunc_before（开区间上界；此后 xid < 该值进入免查隐式冻结区）。要求 p_trunc_before <= shard_vacuum_xid（顺序铁律：页没清完不许动 clog），否则 ERROR。先推水位后删文件——反过来一旦中途崩溃，clog 没了而水位还说要查 clog，已提交数据当场消失。按段整删，返回删掉的段文件数。';

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

CREATE OR REPLACE FUNCTION repair_diverged_shards()
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
        RETURN;                 -- 无从判定：返回 0 行
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
    '只读决议窥视（T4.5）：本地答案受 leader 门控（follower 有 apply 滞后会答错）；本地无答案时转问组内成员（R-P4-13/14）并另起语句补读 commit_ts（R-P4-16）；COMMIT 判决缺有效 commit_ts 时宁可返回 0 行，绝不写推定中止、绝不答半个判决。';

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
