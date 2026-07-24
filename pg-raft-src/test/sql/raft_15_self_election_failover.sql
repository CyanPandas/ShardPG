-- raft_15: 切主重构全流程验收（自治选举 → 上报登记 → 落路由层）
--
-- 前置：raft_14 已建好数据组（成员 {2,3,4}，leader=placement worker）并完成
-- 初次登记；harness 随后停掉主副本所在 worker。期望链路：
--   分区组内 follower 在选举超时后自治选出新 leader（无需控制面指定）
--   → 新 leader 经 libpq 向 group 0 leader（master）上报 (gid, node, term)
--   → group 0 多数派提交 OP_PARTITION_PRIMARY（带任期栅栏）
--   → 每个节点 apply：更新本地 partition_map + 本地 pg_dist_placement 指向新主。
--
-- 本文件在 master 与每个存活 worker 上各跑一遍（表都是每节点一份），传入：
--   -v gid=<数据组 id/Citus shardid>
--   -v old_primary_id=<被停掉的旧主节点 id>
--   -v old_term=<raft_14 初次登记的 primary_term>
--
-- 断言：
--   1. partition_map：新主 != 旧主、新主是 worker(不是 master)、任期严格递增、
--      旧主仍留在 secondary_nodes（成员集静态，宕机不改成员）、master 不在其中；
--   2. 本节点路由层 pg_dist_placement 已指向新主所在 Citus group；
--   3. 控制面旧"指定式切主"通道未插手：primary_term > 0 的分区只接受组内
--      自治选举的登记（任期栅栏拦下 term=0 的 legacy 提案）。

\set ON_ERROR_STOP on

SELECT set_config('raft15.gid',            :'gid',            false);
SELECT set_config('raft15.old_primary_id', :'old_primary_id', false);
SELECT set_config('raft15.old_term',       :'old_term',       false);

DO $$
DECLARE
  gid            BIGINT := current_setting('raft15.gid')::bigint;
  old_primary_id INT    := current_setting('raft15.old_primary_id')::int;
  old_term       BIGINT := current_setting('raft15.old_term')::bigint;
  pm             RECORD;
  route_gid      INT;
  primary_gid    INT;
BEGIN
  SELECT primary_node, secondary_nodes, primary_term INTO pm
  FROM partdist.partition_map WHERE partition_id = gid::oid;
  IF pm IS NULL THEN
    RAISE EXCEPTION 'raft_15: partition_map 没有分区 % 的登记', gid;
  END IF;

  -- 1. 自治选举结果已登记
  IF pm.primary_node = old_primary_id THEN
    RAISE EXCEPTION 'raft_15: 主副本仍是宕机节点 %（自治选举/上报未走通）', old_primary_id;
  END IF;
  IF pm.primary_node = 1 THEN
    RAISE EXCEPTION 'raft_15: 新主登记成了 master——master 不作数据副本';
  END IF;
  IF pm.primary_node NOT IN (2, 3, 4) THEN
    RAISE EXCEPTION 'raft_15: 新主 % 不是 worker 节点', pm.primary_node;
  END IF;
  IF pm.primary_term <= old_term THEN
    RAISE EXCEPTION 'raft_15: primary_term=% 未超过旧任期 %（任期应随改选严格递增）',
      pm.primary_term, old_term;
  END IF;
  IF NOT (old_primary_id = ANY (pm.secondary_nodes)) THEN
    RAISE EXCEPTION 'raft_15: 旧主 % 应仍留在 secondary_nodes=%（成员集静态）',
      old_primary_id, pm.secondary_nodes;
  END IF;
  IF 1 = ANY (pm.secondary_nodes) THEN
    RAISE EXCEPTION 'raft_15: secondary_nodes=% 包含 master(node 1)', pm.secondary_nodes;
  END IF;

  -- 2. 本节点路由层已切到新主
  SELECT p.groupid INTO route_gid FROM pg_dist_placement p WHERE p.shardid = gid;
  SELECT n.groupid INTO primary_gid
  FROM pg_dist_node n JOIN partdist.node_map m ON m.port = n.nodeport
  WHERE m.node_id = pm.primary_node AND n.noderole = 'primary';
  IF route_gid IS DISTINCT FROM primary_gid THEN
    RAISE EXCEPTION 'raft_15: 本节点 pg_dist_placement.groupid=% 未切到新主所在 group %',
      route_gid, primary_gid;
  END IF;

  RAISE NOTICE
    'raft_15 OK: 分区 % 旧主 % -> 新主 % (term %->%)，本节点路由已指向 group %',
    gid, old_primary_id, pm.primary_node, old_term, pm.primary_term, primary_gid;
END;
$$;
