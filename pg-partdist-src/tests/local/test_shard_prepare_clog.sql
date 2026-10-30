-- Run with psql -v ON_ERROR_STOP=1 in an ISOLATED test database, after rebuilding
-- and loading pg_partdist/pg_raft and applying both first-shard SQL migrations.
-- Exercises real C storage functions; does not substitute a SQL state model.
\set ON_ERROR_STOP on
SET shardpg.first_shard_coordinator=off;
SET citus.enable_ddl_propagation=off;
CREATE TABLE shard_prepare_clog_fixture(id integer);
CREATE TABLE shard_prepare_clog_other(id integer);
CREATE TEMP TABLE prepare_clog_test_context(id integer);
CREATE FUNCTION pg_temp.clog_write(oid,bigint,integer,bigint) RETURNS void
AS '$libdir/pg_partdist','partdist_shard_clog_write_ts' LANGUAGE c STRICT;
CREATE FUNCTION pg_temp.prepare_slot(oid,bigint) RETURNS void
AS '$libdir/pg_partdist','partdist_shard_clog_set_prepared' LANGUAGE c STRICT;
CREATE FUNCTION pg_temp.raise_watermark(oid,bigint) RETURNS void
AS '$libdir/pg_partdist','partdist_shard_xid_raise_watermark' LANGUAGE c STRICT;
CREATE FUNCTION pg_temp.claim(oid) RETURNS integer
AS '$libdir/pg_partdist','partdist_shard_claim' LANGUAGE c STRICT;

DO $$
DECLARE s oid:='shard_prepare_clog_fixture'::regclass;
        other_shard oid:='shard_prepare_clog_other'::regclass;
        wm bigint; running bigint; prepared bigint; raw bytea;
BEGIN
    PERFORM pg_temp.claim(s);
    wm:=partdist.shard_xid_next(s)+4096;
    PERFORM pg_temp.raise_watermark(s,wm);
    wm:=partdist.shard_xid_next(s);
    running:=wm-3; prepared:=wm-2;
    PERFORM pg_temp.clog_write(s,running,0,10);
    PERFORM pg_temp.prepare_slot(s,prepared);
    PERFORM pg_temp.clog_write(other_shard,prepared,3,0);
    IF partdist.shard_clog_status(s,prepared)<>0 OR NOT partdist.shard_is_prepared(s,prepared) THEN
        RAISE EXCEPTION 'prepare must retain IN_PROGRESS with separate 2PC membership';
    END IF;
    raw:=pg_read_binary_file(format('pg_shard_clog/%s/%s',s,
                              upper(lpad(to_hex(prepared/1048576),8,'0'))),
                              (prepared%1048576)*32+24,4);
    IF raw<>decode('00000000','hex') THEN
        RAISE EXCEPTION 'on-disk clog contains a prepared state';
    END IF;
    PERFORM partdist.shard_claim_on_promote(s);
    IF partdist.shard_clog_status(s,running)<>3 OR partdist.shard_clog_status(s,prepared)<>0
       OR NOT partdist.shard_is_prepared(s,prepared) THEN
        RAISE EXCEPTION 'recovery failed to distinguish orphan RUNNING from prepared';
    END IF;
    PERFORM pg_temp.clog_write(s,prepared,2,100);
    IF partdist.shard_clog_status(s,prepared)<>2 OR partdist.shard_is_prepared(s,prepared)
       OR partdist.shard_clog_status(other_shard,prepared)<>3 THEN
        RAISE EXCEPTION 'terminal cleanup or shard isolation failed';
    END IF;
    BEGIN
        PERFORM pg_temp.clog_write(s,prepared,1,0);
        RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='PREPARED clog status accepted';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
END $$;
DROP TABLE shard_prepare_clog_fixture,shard_prepare_clog_other;
SELECT partdist.shard_mvcc_gc_orphans();
SELECT 'SHARD_PREPARE_CLOG_OK';
