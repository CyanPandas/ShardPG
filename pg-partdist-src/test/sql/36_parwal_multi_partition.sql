-- T2.2 / T2.2.1: Multiple partitions — records routed to correct directories

SELECT partdist.reset_partition_wal_state(9911::oid);
SELECT partdist.reset_partition_wal_state(9912::oid);
SELECT partdist.reset_partition_wal_state(9913::oid);

-- Write 30 records spread across 3 partitions (10 each)
DO $$
DECLARE i INT;
BEGIN
    FOR i IN 1..10 LOOP
        PERFORM partdist.write_partition_wal_record(9911::oid, 1);
        PERFORM partdist.write_partition_wal_record(9912::oid, 1);
        PERFORM partdist.write_partition_wal_record(9913::oid, 1);
    END LOOP;
END;
$$;

-- Wait for Demux Worker to route all records to their partition directories
SELECT partdist.demux_flush();

-- Each partition must have exactly 10 records
SELECT partdist.count_parwal_records(9911::oid) = 10 AS p1_count_ok;
SELECT partdist.count_parwal_records(9912::oid) = 10 AS p2_count_ok;
SELECT partdist.count_parwal_records(9913::oid) = 10 AS p3_count_ok;

-- Each partition's LSN sequence must be valid
SELECT partdist.verify_partition_wal(9911::oid) AS p1_valid;
SELECT partdist.verify_partition_wal(9912::oid) AS p2_valid;
SELECT partdist.verify_partition_wal(9913::oid) AS p3_valid;

-- Partition LSNs must be self-consistent (no cross-contamination)
SELECT bool_and(h.orig_node_lsn > '0/0'::pg_lsn) AS p1_lsn_ok
  FROM partdist.read_all_headers(9911::oid) h;
SELECT bool_and(h.orig_node_lsn > '0/0'::pg_lsn) AS p2_lsn_ok
  FROM partdist.read_all_headers(9912::oid) h;
SELECT bool_and(h.orig_node_lsn > '0/0'::pg_lsn) AS p3_lsn_ok
  FROM partdist.read_all_headers(9913::oid) h;

SELECT partdist.reset_partition_wal_state(9911::oid);
SELECT partdist.reset_partition_wal_state(9912::oid);
SELECT partdist.reset_partition_wal_state(9913::oid);
