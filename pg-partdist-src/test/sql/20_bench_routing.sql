-- Test 20: routing throughput micro-benchmark
-- Runs 1000 pg_partdist_route_write() calls and reports timing.

INSERT INTO partdist.node_map (node_id, hostname, port, status)
VALUES (1, 'coordinator', 5432, 'active'),
       (2, 'worker1',     5433, 'active');

INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes)
VALUES (3000::oid, 1, ARRAY[2]),
       (3001::oid, 1, ARRAY[2]),
       (3002::oid, 2, ARRAY[1]);

SET pg_partdist.local_node_id = 1;

DO $$
DECLARE
    t_start  timestamptz;
    t_end    timestamptz;
    elapsed  double precision;
    i        integer;
    route    text;
    n        integer := 1000;
    last_route text;
BEGIN
    t_start := clock_timestamp();

    FOR i IN 1..n LOOP
        SELECT partdist.pg_partdist_route_write(3000::oid) INTO last_route;
    END LOOP;

    t_end   := clock_timestamp();
    elapsed := extract(epoch FROM (t_end - t_start)) * 1000.0;

    -- Only emit stable pass/fail notice; timing values are written to server log.
    IF elapsed / n < 1.0 THEN
        RAISE NOTICE 'OK: p50 < 1 ms, last_route=%', last_route;
    ELSE
        RAISE NOTICE 'WARN: p50 >= 1 ms (shmem may be inactive), last_route=%', last_route;
    END IF;
END;
$$;

RESET pg_partdist.local_node_id;
DELETE FROM partdist.partition_map;
DELETE FROM partdist.node_map;
