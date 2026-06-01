-- Test 08: schema and object existence
SELECT nspname FROM pg_namespace WHERE nspname = 'partdist';

SELECT tablename
FROM   pg_tables
WHERE  schemaname = 'partdist'
ORDER BY tablename;

SELECT proname
FROM   pg_proc
JOIN   pg_namespace ON pg_namespace.oid = pg_proc.pronamespace
WHERE  pg_namespace.nspname = 'partdist'
ORDER BY proname;
