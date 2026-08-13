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
    FROM pg_catalog.pg_dist_shard s
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
    DELETE FROM shard_identity si
     WHERE NOT EXISTS (
         SELECT 1 FROM pg_catalog.pg_dist_shard s
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
CREATE OR REPLACE FUNCTION replay_set_locmap(
    p_local_shard REGCLASS,
    p_roles INTEGER[],
    p_ords INTEGER[],
    p_spcs OID[],
    p_dbs OID[],
    p_relnums OID[]
) RETURNS INTEGER LANGUAGE c STRICT VOLATILE
    AS 'MODULE_PATHNAME', 'pg_partdist_replay_set_locmap';

COMMENT ON FUNCTION replay_set_locmap(REGCLASS, INTEGER[], INTEGER[], OID[], OID[], OID[]) IS
    'Follower 侧：建立 leader→本地 文件号映射（loc_map）。两侧索引/TOAST 结构必须一致（同源物理基线，FRD §13.2）。';

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
