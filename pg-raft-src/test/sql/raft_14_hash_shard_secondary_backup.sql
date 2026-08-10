-- raft_14: 真实哈希分布分片的 (a) 形态从副本备份（切主重构验收·复制半程）
--
-- 架构前提：分区主副本（数据组 leader）在 placement worker 上；其余 worker 以
-- "同构壳表 + shard_identity 注册"充当纯 secondary（P2 只备份持久化、不回放）；
-- master(node 1) 不作任何分区的数据副本。
--
-- 在**该组的每个 follower 节点**上执行，由 harness 传入：
--   -v gid=<数据组 id，即 Citus shardid>
--   -v nrec=<leader 侧该分片的 parwal 记录数（逐条 propose 后应全量到达）>
--   -v leader_md5=<leader 侧全量记录逐条 md5 保序串接后的 md5 指纹>
--   -v primary_id=<主副本所在节点 id（partdist.node_map.node_id）>
--   -v shard_table=<本节点同构壳表名>
--
-- 断言：
--   1. 壳表已注册 shard_identity（global_shard_id -> 本地 OID 可解析）；
--   2. 组可见、本节点是 follower、nrec 条全部多数派提交且 apply 追平；
--   3. 一条 record 一次备份：本节点 pg_parwal 恰有 nrec 条、flush lsn == nrec，
--      partition_lsn 严格单调（verify_partition_wal），即 1..nrec 连续无洞；
--   4. 全量字节逐字节一致：本地指纹 == leader 指纹（保序）；
--   5. 不回放：壳表行数为 0（备份持久化 != 数据可见）；
--   6. applied_part_lsn 追平到 nrec；
--   7. 控制面登记已随 group 0 复制到本节点：partition_map 里该分区
--      primary_node == 主副本节点、primary_term >= 1、secondary_nodes 含本节点
--      且不含 master(node 1)；本节点路由层 pg_dist_placement 指向主副本所在
--      Citus group。

\set ON_ERROR_STOP on

SELECT set_config('raft14.gid',         :'gid',         false);
SELECT set_config('raft14.nrec',        :'nrec',        false);
SELECT set_config('raft14.leader_md5',  :'leader_md5',  false);
SELECT set_config('raft14.primary_id',  :'primary_id',  false);
SELECT set_config('raft14.shard_table', :'shard_table', false);

DO $$
DECLARE
  gid          BIGINT := current_setting('raft14.gid')::bigint;
  nrec         BIGINT := current_setting('raft14.nrec')::bigint;
  leader_md5   TEXT   := current_setting('raft14.leader_md5');
  primary_id   INT    := current_setting('raft14.primary_id')::int;
  shard_table  TEXT   := current_setting('raft14.shard_table');
  self_id      INT    := current_setting('pg_raft.node_id')::int;
  local_oid    OID;
  st           TEXT;
  commit_idx   BIGINT;
  applied_idx  BIGINT;
  n_disk       BIGINT;
  flush_lsn    BIGINT;
  got_md5      TEXT;
  applied_plsn BIGINT;
  n_rows       BIGINT;
  pm           RECORD;
  route_gid    INT;
  primary_gid  INT;
BEGIN
  -- 1. (a) 形态：壳表 + shard_identity
  local_oid := partdist.local_partition_for_shard(gid);
  IF local_oid IS NULL THEN
    RAISE EXCEPTION 'raft_14: 本节点没有分片 % 的 shard_identity（壳表未注册）', gid;
  END IF;

  -- 2. 组状态：follower、全量提交、apply 追平
  SELECT state, commit_index, last_applied
    INTO st, commit_idx, applied_idx
  FROM partdist.pg_raft_group_status() WHERE group_id = gid;
  IF st IS NULL THEN
    RAISE EXCEPTION 'raft_14: 数据组 % 未传播到本节点', gid;
  END IF;
  IF st = 'leader' THEN
    RAISE EXCEPTION 'raft_14: 本用例应在 follower 上执行，当前节点是 leader';
  END IF;
  IF commit_idx < nrec THEN
    RAISE EXCEPTION 'raft_14: commit_index=% < nrec=%，存在未达多数派的记录',
      commit_idx, nrec;
  END IF;
  IF applied_idx <> commit_idx THEN
    RAISE EXCEPTION 'raft_14: last_applied=% 未追上 commit_index=%',
      applied_idx, commit_idx;
  END IF;

  -- 3. 一条 record 一次备份：条数、进度、连续性
  --
  -- ★ 判据不能写成 `n_disk = nrec`（2026-08-10 修）。
  --
  -- 流**不是静止的**：D2 的冻结账目发射器按 pg_partdist.freeze_sync_interval_ms
  -- （默认 60s）的节奏，在**任意事务**的 PRE_COMMIT 上给**每个已注册分片**补一条
  -- CTRL:FREEZE_UPDATE(flags=4 info=2)，哪怕该分片本身毫无活动。实测：一个静止
  -- 分片 60s 后 flush_lsn 自己从 7 长到 8。
  --
  -- 于是"leader 侧读到 nrec、follower 侧断言恰好 nrec"这种写法是**时序相关**的 ——
  -- 用例跨过一次 60s 边界就会多出一条，表现为"多了 1 条记录"的假失败
  -- （本轮 raft_14/22/24 三处同形失败皆此因）。
  --
  -- 改法比原判据**更强**而不是更弱：显式刻画"允许多出什么"。
  --   a) 1..nrec 必须一条不少（下面的 verify + 指纹比对负责"一次且仅一次"）；
  --   b) 超出 nrec 的部分**只允许是 CTRL 类**——多出任何一条 DATA/MARKER 都说明
  --      有重复备份或不该来的记录，仍然立即失败。
  n_disk := partdist.count_parwal_records(local_oid);
  IF n_disk < nrec THEN
    RAISE EXCEPTION 'raft_14: 本节点 parwal 只有 % 条记录，少于应备份的 %（有记录丢失）',
      n_disk, nrec;
  END IF;
  flush_lsn := partdist.get_partition_flush_lsn(local_oid);
  IF flush_lsn < nrec THEN
    RAISE EXCEPTION 'raft_14: flush lsn=% 小于 %（备份未追平）', flush_lsn, nrec;
  END IF;
  IF flush_lsn > nrec THEN
    DECLARE
      n_noncrtl BIGINT;
    BEGIN
      SELECT count(*) INTO n_noncrtl
        FROM generate_series(nrec + 1, flush_lsn) g,
             LATERAL partdist.partwal_read_record(local_oid, g) r
       WHERE r.flags <> 4;          -- 4 = PARTWAL_FLAG_CTRL
      IF n_noncrtl > 0 THEN
        RAISE EXCEPTION
          'raft_14: 超出 nrec=% 的 % 条记录里有 % 条不是 CTRL 类 —— 存在重复备份或多余记录',
          nrec, flush_lsn - nrec, n_noncrtl;
      END IF;
    END;
  END IF;
  IF NOT partdist.verify_partition_wal(local_oid) THEN
    RAISE EXCEPTION 'raft_14: partition_lsn 非严格单调或记录损坏（1..% 应连续无洞）', nrec;
  END IF;

  -- 4. 全量字节一致（保序指纹）
  SELECT md5(string_agg(sub.h, ',' ORDER BY sub.plsn)) INTO got_md5
  FROM (
    SELECT g AS plsn, md5(r.data) AS h
    FROM generate_series(1, nrec) g,
         LATERAL partdist.partwal_read_record(local_oid, g) r
  ) sub;
  IF got_md5 IS DISTINCT FROM leader_md5 THEN
    RAISE EXCEPTION 'raft_14: 从副本字节指纹 % != leader 指纹 %', got_md5, leader_md5;
  END IF;

  -- 5. 不回放：壳表必须仍是空表
  EXECUTE format('SELECT count(*) FROM %I', shard_table) INTO n_rows;
  IF n_rows <> 0 THEN
    RAISE EXCEPTION 'raft_14: 壳表 % 有 % 行——P2 只备份持久化，不允许回放',
      shard_table, n_rows;
  END IF;

  -- 6. applied_part_lsn 追平
  applied_plsn := partdist.get_follower_applied_part_lsn(local_oid);
  IF applied_plsn <> nrec THEN
    RAISE EXCEPTION 'raft_14: applied_part_lsn=% 期望 %', applied_plsn, nrec;
  END IF;

  -- 7. 控制面登记已复制到本节点 + 本节点路由层指向主副本
  SELECT primary_node, secondary_nodes, primary_term INTO pm
  FROM partdist.partition_map WHERE partition_id = gid::oid;
  IF pm IS NULL THEN
    RAISE EXCEPTION 'raft_14: partition_map 没有分区 % 的登记（上报链路未走通）', gid;
  END IF;
  IF pm.primary_node <> primary_id THEN
    RAISE EXCEPTION 'raft_14: partition_map.primary_node=% 期望 %（数据组 leader 所在节点）',
      pm.primary_node, primary_id;
  END IF;
  IF pm.primary_term < 1 THEN
    RAISE EXCEPTION 'raft_14: primary_term=% ，登记应带自治选举任期(>=1)', pm.primary_term;
  END IF;
  IF 1 = ANY (pm.secondary_nodes) THEN
    RAISE EXCEPTION 'raft_14: secondary_nodes=% 包含 master(node 1)——master 不作数据副本',
      pm.secondary_nodes;
  END IF;
  IF NOT (self_id = ANY (pm.secondary_nodes)) THEN
    RAISE EXCEPTION 'raft_14: 本节点(%)不在 secondary_nodes=% 中', self_id, pm.secondary_nodes;
  END IF;

  SELECT p.groupid INTO route_gid FROM pg_dist_placement p WHERE p.shardid = gid;
  SELECT n.groupid INTO primary_gid
  FROM pg_dist_node n JOIN partdist.node_map m ON m.port = n.nodeport
  WHERE m.node_id = primary_id AND n.noderole = 'primary';
  IF route_gid IS DISTINCT FROM primary_gid THEN
    RAISE EXCEPTION 'raft_14: 本节点 pg_dist_placement.groupid=% 未指向主副本所在 group %',
      route_gid, primary_gid;
  END IF;

  RAISE NOTICE
    'raft_14 OK: group % follower node % ：% 条记录逐字节一致，1..% 连续，applied=%，壳表 0 行，登记 primary=% term=%',
    gid, self_id, nrec, nrec, applied_plsn, pm.primary_node, pm.primary_term;
END;
$$;
