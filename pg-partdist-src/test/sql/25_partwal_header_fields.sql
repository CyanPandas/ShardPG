-- T2.1.2: WAL format validation — PartWALHeader field consistency

-- Reset state for a clean, deterministic run
SELECT partdist.reset_partition_wal_state(25001::oid);
SELECT partdist.reset_partition_wal_state(25099::oid);
SELECT partdist.init_partition_wal(25001::oid);

-- Write three records
SELECT partdist.write_partition_wal_record(25001::oid, 1) IS NOT NULL AS r1;
SELECT partdist.write_partition_wal_record(25001::oid, 1) IS NOT NULL AS r2;
SELECT partdist.write_partition_wal_record(25001::oid, 1) IS NOT NULL AS r3;

-- Wait for Demux Worker to route records to pg_parwal
SELECT partdist.demux_flush();

-- partition_lsn must start at 1 and be strictly increasing
SELECT partition_lsn
FROM partdist.check_partition_wal(25001::oid)
ORDER BY partition_lsn;

-- All records must have the correct partition_id (is_valid includes this check)
SELECT bool_and(is_valid) AS fields_valid
FROM partdist.check_partition_wal(25001::oid);

-- alloc_partition_lsn for a fresh partition starts at 1
SELECT partdist.alloc_partition_lsn(25099::oid) AS first_lsn;
SELECT partdist.alloc_partition_lsn(25099::oid) AS second_lsn;
SELECT partdist.alloc_partition_lsn(25099::oid) AS third_lsn;
