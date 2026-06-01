-- Test 01: extension creation and schema setup
DROP EXTENSION IF EXISTS pg_partdist CASCADE;
CREATE EXTENSION pg_partdist;

-- schema must exist
SELECT nspname FROM pg_namespace WHERE nspname = 'partdist';

-- version function
SELECT partdist.pg_partdist_version();
