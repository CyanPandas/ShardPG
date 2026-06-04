-- T2.2 / T2.2.4: No duplicates — Demux Worker is the sole writer to pg_parwal/N/

SELECT partdist.reset_partition_wal_state(9904::oid);

-- Write 10 records (WAL only)
DO $$
DECLARE i INT;
BEGIN
    FOR i IN 1..10 LOOP
        PERFORM partdist.write_partition_wal_record(9904::oid, 1);
    END LOOP;
END;
$$;

-- Wait for Demux Worker to route all records
SELECT partdist.demux_flush();

-- Count must be exactly 10 — Demux Worker writes each record exactly once
SELECT partdist.count_parwal_records(9904::oid) = 10 AS no_duplicates;

-- Verify monotonicity
SELECT partdist.verify_partition_wal(9904::oid) AS wal_valid;

SELECT partdist.reset_partition_wal_state(9904::oid);
