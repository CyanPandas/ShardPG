-- T2.1.3: crash recovery — file durability and LSN continuity

-- Reset state for a clean, deterministic run
SELECT partdist.reset_partition_wal_state(27001::oid);

-- Write records to a dedicated partition
SELECT partdist.init_partition_wal(27001::oid);
SELECT partdist.write_partition_wal_record(27001::oid, 1) IS NOT NULL AS r1;
SELECT partdist.write_partition_wal_record(27001::oid, 1) IS NOT NULL AS r2;
SELECT partdist.write_partition_wal_record(27001::oid, 1) IS NOT NULL AS r3;

-- Wait for Demux Worker to route all records to pg_parwal
SELECT partdist.demux_flush();

-- verify_partition_wal confirms continuity (partition_lsn 1,2,3 with no gaps)
SELECT partdist.verify_partition_wal(27001::oid) AS lsn_continuous;

-- Files survive a CHECKPOINT (which flushes WAL, simulating crash-safe durability)
CHECKPOINT;

-- After checkpoint files must still be present and valid
SELECT partdist.partition_wal_exists(27001::oid) AS survives_checkpoint;
SELECT COUNT(*) = 3 AS three_records_intact
FROM partdist.check_partition_wal(27001::oid);
SELECT partdist.verify_partition_wal(27001::oid) AS still_continuous;

-- orig_node_lsn → partition_lsn mapping is consistent: all orig_node_lsn values
-- must be positive (non-zero), confirming the primary-generated LSN was captured
SELECT bool_and(orig_node_lsn > '0/0'::pg_lsn) AS all_orig_lsn_nonzero
FROM partdist.check_partition_wal(27001::oid);

-- partition_lsn values are 1, 2, 3 in order
SELECT string_agg(partition_lsn::text, ',' ORDER BY partition_lsn) AS lsn_sequence
FROM partdist.check_partition_wal(27001::oid);
