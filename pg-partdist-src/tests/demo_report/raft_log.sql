-- Router-only observer: workers expose Raft views, not this helper function.
BEGIN;
SET LOCAL citus.enable_ddl_propagation = off;
-- 报告3.5节；独立函数名；按host+port映射节点，防止不同机器同端口串台。
CREATE OR REPLACE FUNCTION public.{{prefix}}raft_log(tbl regclass, key int, n int DEFAULT 5)
RETURNS TABLE (worker text, role text, log_index bigint, term bigint, record text, committed boolean)
LANGUAGE sql AS $f$
WITH w AS (
  SELECT nodename, nodeport, success, result
    FROM run_command_on_workers(format($q$
      SELECT coalesce(json_agg(json_build_object(
               'state', (SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = l.group_id),
               'idx', l.log_index, 'term', l.term, 'op', l.op_type, 'p', l.payload, 'committed', l.committed)), '[]')
        FROM (SELECT * FROM partdist.raft_log WHERE group_id = %s ORDER BY log_index DESC LIMIT %s) l$q$,
      get_shard_id_for_distribution_column(tbl, key), n))
), x AS (
  SELECT nm.name AS worker,
         CASE r.state WHEN 'leader' THEN 'primary' WHEN 'follower' THEN 'replica' ELSE r.state END AS role,
         r.idx AS log_index, r.term,
         CASE WHEN r.op <> 'OP_PARWAL' THEN r.op
              WHEN f & 8 = 8 THEN 'DTX '  || coalesce((ARRAY['PREPARE','DECISION','COMMIT','ABORT','FORGET'])[i], i::text)
              WHEN f & 4 = 4 THEN 'CTRL ' || coalesce((ARRAY['FILESET_UPDATE','FREEZE_UPDATE','SHARD_CLOG','SHARD_DROP',
                                                             'DDL_HINT','SHARD_MVCC','BASELINE_END'])[i], i::text)
              WHEN f & 2 = 2 THEN 'TXN '  || coalesce((ARRAY['COMMIT','PREPARE','ABORT','COMMIT_PREPARED','ABORT_PREPARED'])[((i & 112) >> 4) + 1], i::text)
              ELSE coalesce((SELECT rm_name FROM pg_get_wal_resource_managers() WHERE rm_id = rm), 'rmid ' || rm) || ' ' ||
                   coalesce(CASE rm
                     WHEN 0  THEN (ARRAY['CHECKPOINT_SHUTDOWN','CHECKPOINT_ONLINE','NOOP','NEXTOID','SWITCH','BACKUP_END',
                                         'PARAMETER_CHANGE','RESTORE_POINT','FPW_CHANGE','END_OF_RECOVERY','FPI_FOR_HINT','FPI'])[(i >> 4) + 1]
                     WHEN 9  THEN (ARRAY['REWRITE','PRUNE','VACUUM','FREEZE_PAGE','VISIBLE','MULTI_INSERT','LOCK_UPDATED','NEW_CID'])[((i & 112) >> 4) + 1]
                     WHEN 10 THEN (ARRAY['INSERT','DELETE','UPDATE','TRUNCATE','HOT_UPDATE','CONFIRM','LOCK','INPLACE'])[((i & 112) >> 4) + 1]
                     WHEN 11 THEN (ARRAY['INSERT_LEAF','INSERT_UPPER','INSERT_META','SPLIT_L','SPLIT_R','INSERT_POST','DEDUP','DELETE',
                                         'UNLINK_PAGE','UNLINK_PAGE_META','MARK_PAGE_HALFDEAD','VACUUM','REUSE_PAGE','META_CLEANUP'])[(i >> 4) + 1]
                   END, 'info ' || i)
         END AS record,
         r.committed
    FROM w
    JOIN shardpg.nodes nm ON nm.port = w.nodeport AND nm.host = w.nodename
    CROSS JOIN LATERAL json_to_recordset(CASE WHEN w.success THEN w.result::json ELSE '[]' END)
          AS r(state text, idx bigint, term bigint, op text, p jsonb, committed boolean)
    CROSS JOIN LATERAL (SELECT (r.p->>'rmid')::int AS rm, (r.p->>'info')::int AS i, (r.p->>'flags')::int AS f) k
  UNION ALL
  SELECT nm.name, 'unreachable', NULL, NULL, NULL, NULL
    FROM w JOIN shardpg.nodes nm ON nm.port = w.nodeport AND nm.host = w.nodename WHERE NOT w.success
)
SELECT * FROM x ORDER BY CASE role WHEN 'primary' THEN 0 WHEN 'replica' THEN 1 ELSE 2 END, worker, log_index
$f$;
COMMIT;
