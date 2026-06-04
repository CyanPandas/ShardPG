-- T2.1.2: WAL format validation — magic and structure

-- Reset state for a clean, deterministic run
SELECT partdist.reset_partition_wal_state(24001::oid);
SELECT partdist.init_partition_wal(24001::oid);

-- Write a DATA record
SELECT partdist.write_partition_wal_record(24001::oid, 1) IS NOT NULL AS lsn_assigned;

-- Wait for Demux Worker to route the record to pg_parwal
SELECT partdist.demux_flush();

-- All scanned records must report is_valid = true (magic OK, partition_id matches, flags valid)
SELECT bool_and(is_valid) AS all_records_valid
FROM partdist.check_partition_wal(24001::oid);

-- flags value must equal 1 (PARTWAL_FLAG_DATA)
SELECT flags = 1 AS flag_is_data
FROM partdist.check_partition_wal(24001::oid);

-- orig_node_lsn must be > '0/0' (non-zero LSN was captured)
SELECT orig_node_lsn > '0/0'::pg_lsn AS orig_lsn_nonzero
FROM partdist.check_partition_wal(24001::oid);

-- Write a CHECKPOINT record (flags = 4)
SELECT partdist.write_partition_wal_record(24001::oid, 4) IS NOT NULL AS checkpoint_written;
SELECT partdist.demux_flush();

-- Now two records: partition_lsn 1 (DATA) and 2 (CHECKPOINT)
SELECT partition_lsn, flags, is_valid
FROM partdist.check_partition_wal(24001::oid)
ORDER BY partition_lsn;
