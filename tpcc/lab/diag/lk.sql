set citus.override_table_visibility = off;
-- 等待中的行锁：等的人在跑哪个事务函数、被谁挡（挡人者在干什么）、tuple 锁在哪张表
select coalesce(regexp_replace(c.relname,'_[0-9]+$',''),'-') as rel,
       left(regexp_replace(w.query,'[0-9]+','N','g'),40) as waiter,
       left(regexp_replace(b.query,'[0-9]+','N','g'),40) as blocker,
       coalesce(b.wait_event,'cpu') as blocker_wait, b.state as blocker_state
  from pg_locks l
  join pg_stat_activity w on w.pid = l.pid
  left join pg_class c on c.oid = l.relation
  left join lateral (select unnest(pg_blocking_pids(l.pid)) bp limit 1) x on true
  left join pg_stat_activity b on b.pid = x.bp
 where not l.granted and l.locktype in ('transactionid','tuple');
