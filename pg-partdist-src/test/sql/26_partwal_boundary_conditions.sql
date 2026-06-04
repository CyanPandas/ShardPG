-- T2.1.2: WAL format validation — boundary conditions

-- Reset state for a clean, deterministic run
SELECT partdist.reset_partition_wal_state(26001::oid);
SELECT partdist.reset_partition_wal_state(26002::oid);
SELECT partdist.reset_partition_wal_state(26099::oid);

-- Empty partition: verify returns vacuously valid
SELECT partdist.init_partition_wal(26001::oid);
SELECT partdist.verify_partition_wal(26001::oid) AS empty_partition_valid;

-- Empty partition: check returns zero rows
SELECT COUNT(*) AS empty_count
FROM partdist.check_partition_wal(26001::oid);

-- Non-existent partition: both functions are safe
SELECT partdist.verify_partition_wal(96099::oid) AS nonexistent_valid;
SELECT COUNT(*) AS nonexistent_count
FROM partdist.check_partition_wal(96099::oid);

-- Single record: partition_lsn starts at 1 (after reset)
SELECT partdist.init_partition_wal(26002::oid);
SELECT partdist.write_partition_wal_record(26002::oid, 1) IS NOT NULL AS wrote;
SELECT partdist.demux_flush();
SELECT partition_lsn AS first_lsn_is_one
FROM partdist.check_partition_wal(26002::oid);

-- alloc_partition_lsn for fresh (reset) partition returns 1
SELECT partdist.alloc_partition_lsn(26099::oid) AS lsn_starts_at_one;

-- verify_partition_wal on single-record partition returns true (demux_flush already called)
SELECT partdist.verify_partition_wal(26002::oid) AS single_record_valid;
