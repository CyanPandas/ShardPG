-- Test 10: Citus and pg_partdist coexist
SELECT extname FROM pg_extension WHERE extname IN ('citus', 'pg_partdist') ORDER BY extname;

-- Create a Citus distributed table while pg_partdist is loaded
CREATE TABLE partdist_citus_test (id serial PRIMARY KEY, val text);
SELECT create_distributed_table('partdist_citus_test', 'id');
SELECT count(*) > 0 AS has_shards
FROM   pg_dist_shard
WHERE  logicalrelid = 'partdist_citus_test'::regclass;
DROP TABLE partdist_citus_test;
