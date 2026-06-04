-- T2.2 / Full cycle: write 50 records, wait for Demux Worker, confirm LSN integrity

SELECT partdist.reset_partition_wal_state(9920::oid);

-- Write 50 records (WAL only)
DO $$
DECLARE i INT;
BEGIN
    FOR i IN 1..50 LOOP
        PERFORM partdist.write_partition_wal_record(9920::oid, 1);
    END LOOP;
END;
$$;

-- Wait for Demux Worker to route all records to pg_parwal/9920/
SELECT partdist.demux_flush();

-- Count must be 50
SELECT partdist.count_parwal_records(9920::oid) = 50 AS total_count_ok;

-- Read back and verify strict monotonicity (1..50 contiguous)
WITH headers AS (
    SELECT partition_lsn
    FROM partdist.read_all_headers(9920::oid)
    ORDER BY partition_lsn
),
indexed AS (
    SELECT partition_lsn, row_number() OVER () AS rn FROM headers
)
SELECT
    count(*) = 50                       AS row_count_ok,
    min(partition_lsn) = 1              AS starts_at_1,
    max(partition_lsn) = 50             AS ends_at_50,
    bool_and(partition_lsn = rn)        AS is_contiguous
FROM indexed;

-- verify_partition_wal must pass
SELECT partdist.verify_partition_wal(9920::oid) AS integrity_ok;

SELECT partdist.reset_partition_wal_state(9920::oid);
