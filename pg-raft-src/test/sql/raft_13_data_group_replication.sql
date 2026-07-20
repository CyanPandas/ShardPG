-- raft_13: 数据面 Raft 组复制 + 平凡 apply（P2 验收）
--
-- 在**该组的一个 follower 节点**上执行，由 harness 传入：
--   -v gid=<数据组 id，即 Citus shardid>
--   -v before_flush=<提交前本节点 pg_parwal 的 flush lsn>
--   -v leader_md5=<leader 侧该条 parwal 记录原始字节的 md5>
--   -v leader_len=<该条记录的字节长度>
--
-- 断言：
--   1. 该数据组在本 follower 上可见，且条目已多数派提交（commit_index >= 1）；
--   2. 平凡 apply 已推进：last_applied == commit_index；
--   3. **字节真的落到了本节点自己的 pg_parwal**：flush lsn 比提交前增长；
--   4. 落盘字节与 leader 逐字节一致（md5 + 长度），即复制的是真实 WAL 内容
--      而非描述符；
--   5. **applied_part_lsn 由真实 C 写入方推进**（此前该列恒为占位 0），
--      且等于被复制记录的 partition_lsn —— 这正是切主安全线可比真实进度的前提。

\set ON_ERROR_STOP on

SELECT set_config('raft13.gid',          :'gid',          false);
SELECT set_config('raft13.before_flush', :'before_flush', false);
SELECT set_config('raft13.leader_md5',   :'leader_md5',   false);
SELECT set_config('raft13.leader_len',   :'leader_len',   false);

DO $$
DECLARE
  gid           BIGINT := current_setting('raft13.gid')::bigint;
  before_flush  BIGINT := current_setting('raft13.before_flush')::bigint;
  leader_md5    TEXT   := current_setting('raft13.leader_md5');
  leader_len    BIGINT := current_setting('raft13.leader_len')::bigint;
  local_oid     OID;
  st            TEXT;
  commit_idx    BIGINT;
  applied_idx   BIGINT;
  after_flush   BIGINT;
  applied_plsn  BIGINT;
  got_md5       TEXT;
  got_len       BIGINT;
BEGIN
  local_oid := partdist.local_partition_for_shard(gid);
  IF local_oid IS NULL THEN
    RAISE EXCEPTION 'raft_13: 本节点没有承载分片 %（P0 映射缺失）', gid;
  END IF;

  -- 1. 组可见且已多数派提交
  SELECT state, commit_index, last_applied
    INTO st, commit_idx, applied_idx
  FROM partdist.pg_raft_group_status() WHERE group_id = gid;

  IF st IS NULL THEN
    RAISE EXCEPTION 'raft_13: 数据组 % 未传播到本节点', gid;
  END IF;
  IF st = 'leader' THEN
    RAISE EXCEPTION 'raft_13: 本用例应在 follower 上执行，当前节点是 leader';
  END IF;
  IF commit_idx < 1 THEN
    RAISE EXCEPTION 'raft_13: 数据组 % 的 commit_index=% ，条目未达多数派',
      gid, commit_idx;
  END IF;

  -- 2. 平凡 apply 已追平
  IF applied_idx <> commit_idx THEN
    RAISE EXCEPTION 'raft_13: last_applied=% 未追上 commit_index=%',
      applied_idx, commit_idx;
  END IF;

  -- 3. 字节确实落到本节点 pg_parwal
  after_flush := partdist.get_partition_flush_lsn(local_oid);
  IF after_flush <= before_flush THEN
    RAISE EXCEPTION
      'raft_13: follower pg_parwal 未增长（before=% after=%），字节没有真正落盘',
      before_flush, after_flush;
  END IF;

  -- 4. 落盘字节与 leader 逐字节一致
  SELECT md5(data), length(data) INTO got_md5, got_len
  FROM partdist.partwal_read_record(local_oid, after_flush);

  IF got_md5 IS NULL THEN
    RAISE EXCEPTION 'raft_13: 读不到本节点 partition_lsn=% 的 parwal 记录',
      after_flush;
  END IF;
  IF got_md5 <> leader_md5 OR got_len <> leader_len THEN
    RAISE EXCEPTION
      'raft_13: 复制到 follower 的字节与 leader 不一致（leader %/% vs follower %/%）',
      leader_md5, leader_len, got_md5, got_len;
  END IF;

  -- 5. applied_part_lsn 由真实写入方推进
  SELECT applied_part_lsn INTO applied_plsn
  FROM partdist.follower_partition_map WHERE partition_id = local_oid;

  IF applied_plsn IS NULL THEN
    RAISE EXCEPTION
      'raft_13: follower_partition_map 没有分片 % 的行，applied_part_lsn 仍无写入方',
      local_oid;
  END IF;
  IF applied_plsn <> 1 THEN
    RAISE EXCEPTION
      'raft_13: applied_part_lsn=% ，期望等于被复制记录的 partition_lsn=1',
      applied_plsn;
  END IF;
  IF partdist.get_follower_applied_part_lsn(local_oid) <> applied_plsn THEN
    RAISE EXCEPTION 'raft_13: 边界函数读到的进度与表不一致';
  END IF;

  RAISE NOTICE
    'raft_13 OK: group % commit=% applied=% ，parwal %→% ，字节 md5=% len=% ，applied_part_lsn=%',
    gid, commit_idx, applied_idx, before_flush, after_flush,
    got_md5, got_len, applied_plsn;
END;
$$;
