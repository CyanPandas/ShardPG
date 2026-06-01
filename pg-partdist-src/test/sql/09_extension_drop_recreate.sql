-- Test 09: drop and recreate the extension
DROP EXTENSION pg_partdist CASCADE;
SELECT count(*) FROM pg_namespace WHERE nspname = 'partdist';
CREATE EXTENSION pg_partdist;
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_partdist';
SELECT nspname FROM pg_namespace WHERE nspname = 'partdist';
