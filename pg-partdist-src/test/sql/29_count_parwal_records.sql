-- T2.2: count_parwal_records function

-- Start clean for partition 9901
SELECT partdist.reset_partition_wal_state(9901::oid);

-- No records yet → 0
SELECT partdist.count_parwal_records(9901::oid) = 0 AS empty_ok;

-- Write 5 records (WAL only; Demux Worker routes to pg_parwal)
SELECT partdist.write_partition_wal_record(9901::oid, 1) IS NOT NULL AS w1;
SELECT partdist.write_partition_wal_record(9901::oid, 1) IS NOT NULL AS w2;
SELECT partdist.write_partition_wal_record(9901::oid, 1) IS NOT NULL AS w3;
SELECT partdist.write_partition_wal_record(9901::oid, 1) IS NOT NULL AS w4;
SELECT partdist.write_partition_wal_record(9901::oid, 1) IS NOT NULL AS w5;

-- Wait for Demux Worker to route all 5 records to disk
SELECT partdist.demux_flush();

-- Should now be 5
SELECT partdist.count_parwal_records(9901::oid) = 5 AS five_records;

-- Clean up
SELECT partdist.reset_partition_wal_state(9901::oid);
