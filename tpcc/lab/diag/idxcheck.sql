-- 每个分片：强制索引扫描 vs 顺序扫描的行数对比（主键索引），不一致即索引指向已回收元组/漏行
SET citus.override_table_visibility = off;
SET statement_timeout = '120s';
DO $$
DECLARE r record; ci bigint; cs bigint; bad int := 0; n int := 0; pk text;
BEGIN
  FOR r IN SELECT c.oid::regclass AS rel, c.relname FROM pg_class c
           WHERE c.relkind='r' AND c.relname ~ '^(warehouse|district|customer|stock|oorder|new_order|order_line)_[0-9]+$' LOOP
    SELECT string_agg(quote_ident(a.attname), ',') INTO pk
      FROM pg_index i JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum = ANY(i.indkey)
     WHERE i.indrelid = r.rel AND i.indisprimary;
    SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off; SET LOCAL enable_indexonlyscan = off;
    EXECUTE format('SELECT count(*) FROM (SELECT %s FROM %s ORDER BY %s) s', pk, r.rel, pk) INTO ci;
    SET LOCAL enable_seqscan = on; SET LOCAL enable_indexscan = off;
    EXECUTE format('SELECT count(*) FROM %s', r.rel) INTO cs;
    SET LOCAL enable_indexscan = on;
    n := n + 1;
    IF ci <> cs THEN bad := bad + 1; RAISE NOTICE 'MISMATCH % idx=% seq=%', r.relname, ci, cs; END IF;
  END LOOP;
  RAISE NOTICE 'checked % shards, mismatches %', n, bad;
END $$;
