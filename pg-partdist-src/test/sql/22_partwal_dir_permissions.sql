-- T2.1.1: partition WAL directory management — permissions

-- Reset state for idempotent test runs
SELECT partdist.reset_partition_wal_state(22001::oid);
SELECT partdist.reset_partition_wal_state(22002::oid);

-- Create directories for permission tests
SELECT partdist.init_partition_wal(22001::oid);
SELECT partdist.init_partition_wal(22002::oid);

-- Both directories must exist
SELECT partdist.partition_wal_exists(22001::oid) AS dir22001_exists,
       partdist.partition_wal_exists(22002::oid) AS dir22002_exists;

-- pg_parwal root must exist (check via partition_wal_exists on any created partition)
SELECT partdist.partition_wal_exists(22001::oid) AS root_accessible;

-- Write a record to verify the directory is writable by postgres
SELECT partdist.write_partition_wal_record(22001::oid, 1) IS NOT NULL AS writable;

-- Wait for Demux Worker to route the record to pg_parwal
SELECT partdist.demux_flush();

-- Record is readable back
SELECT COUNT(*) = 1 AS one_record
FROM partdist.check_partition_wal(22001::oid);

-- Cleanup partition LSN state is irrelevant for permissions test
