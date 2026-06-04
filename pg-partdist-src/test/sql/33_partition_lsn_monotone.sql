-- T2.2 / T2.2.3: partition_lsn must be strictly monotone

SELECT partdist.reset_partition_wal_state(9903::oid);

-- Write 20 records
DO $$
DECLARE i INT;
BEGIN
    FOR i IN 1..20 LOOP
        PERFORM partdist.write_partition_wal_record(9903::oid, 1);
    END LOOP;
END;
$$;

-- Wait for Demux Worker to route all records to pg_parwal
SELECT partdist.demux_flush();

-- Verify count == 20
SELECT partdist.count_parwal_records(9903::oid) = 20 AS count_ok;

-- Verify strictly monotone: each partition_lsn must equal its row number
WITH ordered AS (
    SELECT partition_lsn,
           row_number() OVER (ORDER BY orig_node_lsn, partition_lsn) AS rn
    FROM partdist.read_all_headers(9903::oid)
)
SELECT bool_and(partition_lsn = rn) AS monotone_ok FROM ordered;

-- Verify starts at 1
SELECT min(partition_lsn) = 1 AS starts_at_one
  FROM partdist.read_all_headers(9903::oid);

-- Verify ends at 20
SELECT max(partition_lsn) = 20 AS ends_at_twenty
  FROM partdist.read_all_headers(9903::oid);

SELECT partdist.reset_partition_wal_state(9903::oid);
