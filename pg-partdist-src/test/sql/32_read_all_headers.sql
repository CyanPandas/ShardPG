-- T2.2: read_all_headers returns correct columns and data

SELECT partdist.reset_partition_wal_state(9902::oid);

-- Write 3 records
SELECT partdist.write_partition_wal_record(9902::oid, 1) IS NOT NULL AS w1;
SELECT partdist.write_partition_wal_record(9902::oid, 1) IS NOT NULL AS w2;
SELECT partdist.write_partition_wal_record(9902::oid, 1) IS NOT NULL AS w3;

-- Wait for Demux Worker
SELECT partdist.demux_flush();

-- read_all_headers should return 3 rows
SELECT count(*) = 3 AS row_count_ok FROM partdist.read_all_headers(9902::oid);

-- All orig_node_lsn must be non-zero
SELECT bool_and(orig_node_lsn > '0/0'::pg_lsn) AS all_lsn_nonzero
  FROM partdist.read_all_headers(9902::oid);

SELECT partdist.reset_partition_wal_state(9902::oid);
