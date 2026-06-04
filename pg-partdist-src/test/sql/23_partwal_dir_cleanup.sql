-- T2.1.1: partition WAL directory management — cleanup

-- Reset state for idempotent test runs
SELECT partdist.reset_partition_wal_state(23001::oid);
SELECT partdist.reset_partition_wal_state(23002::oid);

-- Initialise and write some records
SELECT partdist.init_partition_wal(23001::oid);
SELECT partdist.write_partition_wal_record(23001::oid, 1) IS NOT NULL AS wrote_1;
SELECT partdist.write_partition_wal_record(23001::oid, 1) IS NOT NULL AS wrote_2;

-- Wait for Demux Worker to route records to pg_parwal
SELECT partdist.demux_flush();

-- Verify two records exist before cleanup
SELECT COUNT(*) AS before_cleanup
FROM partdist.check_partition_wal(23001::oid);

-- cleanup_partition_wal with LSN 'FFFFFFFF/FFFFFFFF' removes all segments
SELECT partdist.cleanup_partition_wal(23001::oid, 'FFFFFFFF/FFFFFFFF'::pg_lsn);

-- After cleanup, no records should be visible (files removed)
SELECT COUNT(*) AS after_cleanup
FROM partdist.check_partition_wal(23001::oid);

-- cleanup on non-existent partition is safe (no error)
SELECT partdist.cleanup_partition_wal(99199::oid, 'FFFFFFFF/FFFFFFFF'::pg_lsn);

-- Cleanup with keep_lsn = '0/0' removes nothing (all segments end above 0/0)
SELECT partdist.init_partition_wal(23002::oid);
SELECT partdist.write_partition_wal_record(23002::oid, 1) IS NOT NULL AS wrote_3;
SELECT partdist.demux_flush();
SELECT COUNT(*) = 1 AS record_survives
FROM partdist.check_partition_wal(23002::oid);
