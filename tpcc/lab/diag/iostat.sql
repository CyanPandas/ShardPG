create temp table if not exists _io0 as select backend_type, object, context, coalesce(writes,0) w, coalesce(extends,0) e, coalesce(fsyncs,0) f, op_bytes from pg_stat_io;
create temp table if not exists _wal0 as select wal_bytes, wal_write, wal_sync, wal_fpi from pg_stat_wal;
select pg_sleep(20);
select i.backend_type, i.object, i.context,
       round((coalesce(i.writes,0)-o.w)*i.op_bytes/20.0/1048576,2) as write_mb_s,
       round((coalesce(i.extends,0)-o.e)*i.op_bytes/20.0/1048576,2) as extend_mb_s,
       (coalesce(i.fsyncs,0)-o.f) as fsyncs
  from pg_stat_io i join _io0 o using (backend_type, object, context)
 where (coalesce(i.writes,0)-o.w) + (coalesce(i.extends,0)-o.e) > 0
 order by 4 desc;
select round((w.wal_bytes-o.wal_bytes)/20.0/1048576,2) wal_mb_s, (w.wal_write-o.wal_write)/20 wal_writes_s,
       (w.wal_sync-o.wal_sync)/20 wal_syncs_s, (w.wal_fpi-o.wal_fpi)/20 fpi_s
  from pg_stat_wal w, _wal0 o;
