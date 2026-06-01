-- Test 16: routing for unknown partition → not_found
SELECT partdist.pg_partdist_route_write(99999::oid) AS route;

-- get_primary for unknown partition returns NULL
SELECT partdist.pg_partdist_get_primary(99999::oid) IS NULL AS is_null;
