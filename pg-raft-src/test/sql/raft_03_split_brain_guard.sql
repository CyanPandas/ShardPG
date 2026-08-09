-- 脑裂守卫：**非 leader 节点**上 propose 必须被拒。
--
-- ★ 本文件必须在非 leader 端口上跑（驱动用 RAFT_LEADER_PORT 之外的端口，
-- 与 raft_06 同法）。
--
-- 原版把断言体裹在 `IF current_setting('pg_raft.node_id')::int <> 1` 里，
-- 而驱动固定用 -p 5432 跑它、协调节点的 node_id 恒为 1 ⇒ 条件恒假 ⇒
-- 一行都不执行，直接落到 `SELECT true AS split_brain_guard_ok`。
-- 把 leader 门禁整个删掉（任何 follower 都能自行 propose 并本地 apply，
-- 即真正的脑裂写入）本用例仍会 PASS。改为无条件断言。
SET citus.enable_ddl_propagation = off;

DO $$
DECLARE
  is_ldr boolean;
BEGIN
  is_ldr := partdist.pg_raft_is_leader();

  -- 前提断言：本用例只有跑在非 leader 上才有意义，跑错了地方要**失败**而不是跳过。
  IF is_ldr IS NOT FALSE THEN
    RAISE EXCEPTION 'raft_03: 夹具前提不成立 —— 本用例必须在非 leader 节点上跑（is_leader=%）', is_ldr;
  END IF;

  BEGIN
    PERFORM partdist.pg_raft_propose_node_status(1, 'down');
    RAISE EXCEPTION 'raft_03: 非 leader 上的 propose 竟然成功了（脑裂守卫失效）';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM LIKE '%脑裂守卫失效%' THEN
      RAISE;            -- 上面那条是我们自己抛的，原样透出
    END IF;
    IF SQLERRM NOT LIKE '%only leader%' THEN
      RAISE EXCEPTION 'raft_03: propose 被拒了，但原因不是 leader 门禁：%', SQLERRM;
    END IF;
  END;
END $$;
