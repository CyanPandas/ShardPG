-- 收尾复核：propose 出去的决议必须在**每个存活节点**上都被 apply。
--
-- ── 为什么需要这一遍（2026-08-09 审查）────────────────────────────────
-- raft_02 与 raft_04 都是"在同一个 leader 连接里 propose、再在同一个连接里
-- 读回本地表"。leader 的 propose 路径自己就会 apply 本地，所以**follower 侧的
-- apply 路径整个是死代码**（或者集群处于"leader 认为已切主、其余节点仍指向旧主"
-- 的分叉路由）时，那两个用例照样全绿 —— 而分叉路由正是控制面最该被抓的故障。
-- 于是驱动在两个用例收尾处对每个存活节点各跑一遍本脚本（照 raft_15 的逐节点
-- 有界重试写法：控制面语义是"多数派提交 + 全员**最终** apply"，单个 follower
-- 的 apply 由它自己的 tick 推进、允许滞后，但窗口耗尽仍不满足就是真失败）。
--
-- 判据只取"决议本身写下的事实"（分区的新 primary），不取 node_map.status ——
-- 后者会被 TopologyMonitor 在被停节点恢复后改回 active，属于正常行为。
--
-- 参数：
--   -v tag=<用例名，仅用于报错文本>
--   -v part_id=<分区 id>
--   -v expect_primary=<期望的新 primary node_id>

SET citus.enable_ddl_propagation = off;

SELECT set_config('raftchk.tag',            :'tag',            false);
SELECT set_config('raftchk.part_id',        :'part_id',        false);
SELECT set_config('raftchk.expect_primary', :'expect_primary', false);

DO $$
DECLARE
  tag       text   := current_setting('raftchk.tag');
  part_id   oid    := current_setting('raftchk.part_id')::oid;
  expect    int    := current_setting('raftchk.expect_primary')::int;
  got       int;
  i         int;
BEGIN
  IF expect IS NULL OR expect <= 0 THEN
    RAISE EXCEPTION '%: expect_primary=% 不是合法节点 id，判据无从建立', tag, expect;
  END IF;

  FOR i IN 1..40 LOOP
    PERFORM partdist.pg_raft_apply_committed();
    SELECT primary_node INTO got
      FROM partdist.partition_map WHERE partition_id = part_id;
    IF got = expect THEN
      RETURN;
    END IF;
    PERFORM pg_sleep(0.25);
  END LOOP;

  RAISE EXCEPTION
    '%: 本节点 10s 内未 apply 到分区 % 的新主(期望 %,实际 %) —— '
    'follower 侧 apply 未生效或路由分叉',
    tag, part_id, expect, coalesce(got::text, '<无该分区行>');
END
$$;
