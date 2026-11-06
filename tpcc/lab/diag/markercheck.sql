-- 本节点当主的每个分区，流尾 :n 条：本节点写的 COMMIT 标记按本地 CLOG 状态分类（负载停下之后跑）
--   committed / 状态不可判(NULL，非本地 xid) / 本地中止且其后有同 gxid 的 ABORT 标记 / 本地中止且其后没有 ABORT（危险，须为 0）
\set n 30000
with g as (
  select group_id, partdist.local_partition_for_shard(group_id)::oid loid
  from partdist.pg_raft_group_status() where group_id <> 0 and state = 'leader'),
f as (select group_id, loid, partdist.get_partition_flush_lsn(loid) fl from g),
c as (select pg_current_xact_id()::text::bigint cur, (select groupid from pg_dist_local_group) sg),
m as (
  select f.group_id, p plsn, r.gxid, r.info
  from f cross join lateral generate_series(greatest(1, f.fl - :n), f.fl) p
       cross join lateral partdist.partwal_read_record(f.loid, p) r
  where r.flags = 2),
cm as (
  select m.*, pg_xact_status((case when c.cur - (c.cur % 4294967296) + (m.gxid & ((1::bigint << 48) - 1)) > c.cur
                                   then c.cur - (c.cur % 4294967296) + (m.gxid & ((1::bigint << 48) - 1)) - 4294967296
                                   else c.cur - (c.cur % 4294967296) + (m.gxid & ((1::bigint << 48) - 1)) end)::text::xid8) st
  from m, c where m.info = 0 and (m.gxid >> 48)::int = c.sg)
select count(*) filter (where st = 'committed') committed,
       count(*) filter (where st is null) undeterminable,
       count(*) filter (where st is not null and st <> 'committed' and exists
             (select 1 from m a where a.gxid = cm.gxid and a.group_id = cm.group_id and a.info = 32 and a.plsn > cm.plsn)) aborted_with_abort,
       count(*) filter (where st is not null and st <> 'committed' and not exists
             (select 1 from m a where a.gxid = cm.gxid and a.group_id = cm.group_id and a.info = 32 and a.plsn > cm.plsn)) dangerous
from cm;
