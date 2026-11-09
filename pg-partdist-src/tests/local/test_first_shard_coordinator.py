#!/usr/bin/env python3
"""Exercise the actual PL/pgSQL state machine in an isolated PG16 backend.

No running server, TCP socket, Docker, or compiler is needed. C/Raft/TSO/RPC
boundaries are explicit SQL doubles. This is NOT a replication or C-hook test.
The temporary database is removed after the run; existing clusters are untouched.
"""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
SQL = ROOT / "pg-partdist-src/sql/first_shard_coordinator.sql"

STUBS = r"""
CREATE SCHEMA partdist;
CREATE FUNCTION partdist.shard_clog_status(oid,bigint) RETURNS integer LANGUAGE sql AS $$ SELECT 0 $$;
CREATE TABLE partdist.events(seq bigserial, event text);
CREATE TABLE partdist.rpc_gate(fail boolean);
INSERT INTO partdist.rpc_gate VALUES(false);
CREATE TABLE partdist.tso_gate(fail boolean);
INSERT INTO partdist.tso_gate VALUES(false);
CREATE SEQUENCE partdist.timestamps START 100;
CREATE TABLE partdist.dtx_participant(dtxid bigint,gid text,gsids bigint[],coord_gsid bigint,noted_at timestamptz DEFAULT now(),PRIMARY KEY(dtxid,gid));
CREATE TABLE partdist.dtx_decision(dtxid bigint PRIMARY KEY,coord_gsid bigint,verdict integer,commit_ts bigint,participants bigint[],decided_plsn bigserial);
CREATE TABLE partdist.node_map(hostname text,port integer,node_id serial);
CREATE TABLE partdist.partition_map(partition_id oid,primary_node integer);
INSERT INTO partdist.node_map(hostname,port) VALUES('worker1',5433),('worker2',5434);
CREATE TABLE partdist.mock_parwal(oid oid,plsn bigint,kind integer,dtxid bigint,coord_gsid bigint,commit_ts bigint,verdict integer,participants bigint[]);
CREATE FUNCTION partdist.partwal_read_dtx_record(p_oid oid,p_plsn bigint) RETURNS TABLE(kind integer,dtxid bigint,coord_gsid bigint,commit_ts bigint,verdict integer,participants bigint[]) LANGUAGE sql AS $$ SELECT kind,dtxid,coord_gsid,commit_ts,verdict,participants FROM partdist.mock_parwal WHERE oid=p_oid AND plsn=p_plsn $$;
CREATE FUNCTION partdist.dtx_refresh(bigint) RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;
CREATE FUNCTION partdist.pg_raft_group_status() RETURNS TABLE(group_id bigint,state text) LANGUAGE sql AS $$ SELECT 10::bigint,'leader'::text UNION ALL SELECT 20,'leader' UNION ALL SELECT 30,'follower' $$;
CREATE FUNCTION partdist.dtx_rpc(text,integer,text) RETURNS text LANGUAGE plpgsql AS $$ BEGIN IF (SELECT fail FROM partdist.rpc_gate) THEN RAISE EXCEPTION 'injected unavailable participant'; END IF; INSERT INTO partdist.events(event) VALUES('rpc'); RETURN 't'; END $$;
CREATE FUNCTION partdist.dtx_tso_rpc(text) RETURNS text LANGUAGE sql AS $$ SELECT '10'::text $$;
CREATE FUNCTION partdist.dtx_self(q text) RETURNS text LANGUAGE plpgsql AS $$ DECLARE r text; BEGIN EXECUTE q INTO r; RETURN r; END $$;
CREATE FUNCTION partdist.dtx_commit_ts() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN IF (SELECT fail FROM partdist.tso_gate) THEN RAISE EXCEPTION 'injected unavailable TSO'; END IF; INSERT INTO partdist.events(event) VALUES('tso'); RETURN nextval('partdist.timestamps'); END $$;
CREATE FUNCTION partdist.dtx_pending_exists(bigint) RETURNS boolean LANGUAGE sql AS $$ SELECT false $$;
CREATE FUNCTION partdist.dtx_apply_decision(bigint,integer,bigint) RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$;
CREATE FUNCTION partdist.dtx_choose_commit(c bigint,x bigint,ps bigint[]) RETURNS integer LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT FROM partdist.dtx_decision WHERE dtxid=x) THEN RETURN (SELECT verdict FROM partdist.dtx_decision WHERE dtxid=x); END IF; INSERT INTO partdist.events(event) VALUES('decision'); INSERT INTO partdist.dtx_commit_decided(dtxid,coord_gsid,participants,decided_plsn) VALUES(x,c,ps,1) ON CONFLICT DO NOTHING; RETURN 1; END $$;
CREATE FUNCTION partdist.dtx_decide(c bigint,x bigint,v integer,ps bigint[],ts bigint) RETURNS integer LANGUAGE plpgsql AS $$ BEGIN IF v=2 AND EXISTS(SELECT FROM partdist.dtx_commit_decided WHERE dtxid=x) THEN RETURN 0; END IF; INSERT INTO partdist.events(event) VALUES('timestamp'); INSERT INTO partdist.dtx_decision(dtxid,coord_gsid,verdict,commit_ts,participants) VALUES(x,c,v,ts,ps) ON CONFLICT DO NOTHING; RETURN (SELECT verdict FROM partdist.dtx_decision WHERE dtxid=x); END $$;
"""

CASES = r"""
DO $$ BEGIN
  IF partdist.dtx_route_claim(1,20) <> 20 OR partdist.dtx_route_claim(1,10) <> 20 THEN
    RAISE EXCEPTION 'first arrival was replaced by a smaller shard';
  END IF;
END $$;
SELECT partdist.dtx_enlist(1,20,'a','worker1',5433,ARRAY[20]::bigint[]);
SELECT partdist.dtx_enlist(1,20,'b','worker2',5434,ARRAY[10]::bigint[]);
DO $$ BEGIN
  IF partdist.dtx_votes(1) <> 'WAIT' THEN RAISE EXCEPTION 'missing votes accepted'; END IF;
  BEGIN
    PERFORM partdist.dtx_enlist(1,20,'a','worker2',5434,ARRAY[20]::bigint[]);
    RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='branch identity mutation accepted';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
END $$;
SELECT partdist.dtx_seal(1);
DO $$ BEGIN
  BEGIN
    PERFORM partdist.dtx_enlist(1,20,'late','worker1',5433,ARRAY[10]::bigint[]);
    RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='late enlistment accepted';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
END $$;
SELECT partdist.dtx_vote(1,'a',true);
DO $$ BEGIN
  IF partdist.dtx_votes(1) <> 'WAIT' THEN RAISE EXCEPTION 'partial votes accepted'; END IF;
  BEGIN
    PERFORM partdist.dtx_vote(1,'unknown',true);
    RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='unknown vote accepted';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
  BEGIN
    PERFORM partdist.dtx_vote(1,'a',false);
    RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='conflicting vote accepted';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
END $$;
SELECT partdist.dtx_vote(1,'b',true);
SELECT partdist.dtx_vote(1,'b',true);
DO $$ DECLARE ts bigint; again bigint; BEGIN
  IF partdist.dtx_votes(1) <> 'READY' THEN RAISE EXCEPTION 'complete votes rejected'; END IF;
  ts:=partdist.dtx_coordinate(1);
  again:=partdist.dtx_coordinate(1);
  IF ts<>100 OR again<>ts THEN RAISE EXCEPTION 'commit timestamp changed on retry'; END IF;
  IF (SELECT array_agg(event ORDER BY seq) FROM partdist.events) <> ARRAY['decision','tso','timestamp'] THEN
    RAISE EXCEPTION 'commit sent RPCs synchronously or timestamp ordering wrong';
  END IF;
  IF (SELECT participants FROM partdist.dtx_decision WHERE dtxid=1) <> ARRAY[10,20]::bigint[] THEN
    RAISE EXCEPTION 'coordinator lost a participant';
  END IF;
  IF partdist.dtx_record_commit(20,1,ARRAY[10,20]::bigint[],999)<>ts THEN
    RAISE EXCEPTION 'racing commit retry returned a different timestamp';
  END IF;
  UPDATE partdist.rpc_gate SET fail=true;
  IF partdist.dtx_dispatch()<>0 OR NOT EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=1)
     OR NOT EXISTS(SELECT FROM partdist.dtx_decision WHERE dtxid=1) THEN
    RAISE EXCEPTION 'failed dispatch lost durable work';
  END IF;
  UPDATE partdist.rpc_gate SET fail=false;
  IF partdist.dtx_dispatch()<>2 THEN RAISE EXCEPTION 'asynchronous outbox not dispatched'; END IF;
  IF EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=1) THEN
    RAISE EXCEPTION 'completed coordinator registration not reclaimed';
  END IF;
END $$;
SELECT partdist.dtx_enlist(2,10,'bad','worker1',5433,ARRAY[10]::bigint[]);
SELECT partdist.dtx_vote(2,'bad',false);
DO $$ BEGIN
  IF partdist.dtx_votes(2)<>'ABORT' THEN RAISE EXCEPTION 'failed branch accepted'; END IF;
  BEGIN
    PERFORM partdist.dtx_enlist(3,30,'notleader','worker1',5433,ARRAY[30]::bigint[]);
    RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='follower became coordinator';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
  BEGIN
    PERFORM partdist.dtx_finish_decision(1,20,1,0);
    RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='zero timestamp commit accepted';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
END $$;
SELECT partdist.dtx_note_participant(5,'shardpg_fc_5_20_1',ARRAY[10]::bigint[]);
DO $$ BEGIN
  IF (SELECT coord_gsid FROM partdist.dtx_participant WHERE dtxid=5)<>20 THEN
    RAISE EXCEPTION 'prepare registration lost coordinator';
  END IF;
END $$;

-- A crash or unavailable TSO after the autonomous decision cannot undo COMMIT.
TRUNCATE partdist.events;
SELECT partdist.dtx_choose_commit(20,6,ARRAY[10,20]::bigint[]);
UPDATE partdist.tso_gate SET fail=true;
DO $$ BEGIN
  BEGIN
    PERFORM partdist.dtx_coordinate(6);
    RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='unavailable TSO accepted';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
  IF NOT EXISTS(SELECT FROM partdist.dtx_commit_decided WHERE dtxid=6)
     OR EXISTS(SELECT FROM partdist.dtx_decision WHERE dtxid=6) THEN
    RAISE EXCEPTION 'pending COMMIT lost or timestamp invented';
  END IF;
  IF partdist.dtx_decide(20,6,2,ARRAY[10,20]::bigint[],0)<>0 THEN
    RAISE EXCEPTION 'durable COMMIT could be aborted';
  END IF;
END $$;
-- No volatile coordinator registration is available after failover.
UPDATE partdist.rpc_gate SET fail=true;
SELECT partdist.dtx_dispatch();
DO $$ BEGIN
  IF EXISTS(SELECT FROM partdist.dtx_decision WHERE dtxid=6)
     OR EXISTS(SELECT FROM partdist.events WHERE event IN ('tso','timestamp','rpc')) THEN
    RAISE EXCEPTION 'timestamp-less COMMIT dispatched';
  END IF;
END $$;
UPDATE partdist.tso_gate SET fail=false;
SELECT partdist.dtx_dispatch();
DO $$ DECLARE ts bigint; BEGIN
  SELECT commit_ts INTO STRICT ts FROM partdist.dtx_decision WHERE dtxid=6;
  IF ts<=0 OR partdist.dtx_coordinate(6)<>ts THEN
    RAISE EXCEPTION 'recovered timestamp changed';
  END IF;
  IF (SELECT array_agg(event ORDER BY seq) FROM partdist.events)
       <> ARRAY['decision','tso','timestamp'] THEN
    RAISE EXCEPTION 'recovery violated decision/timestamp ordering';
  END IF;
END $$;
UPDATE partdist.rpc_gate SET fail=false;
SELECT partdist.dtx_dispatch();
DO $$ BEGIN
  IF NOT EXISTS(SELECT FROM partdist.events WHERE event='rpc') THEN
    RAISE EXCEPTION 'recovered COMMIT was not dispatched';
  END IF;
  BEGIN
    PERFORM partdist.dtx_record_commit(20,7,ARRAY[20]::bigint[],200);
    RAISE EXCEPTION USING ERRCODE='XX999',MESSAGE='timestamp before decision accepted';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
END $$;

-- Quorum bytes survive a rollback of their SQL indexes; the durable replay
-- cursor must roll back with the indexes and never include an uncommitted tail.
INSERT INTO partdist.mock_parwal VALUES
 (900,1,6,80,90,0,1,ARRAY[90]::bigint[]),
 (900,2,2,80,90,500,1,ARRAY[90]::bigint[]),
 (900,3,5,80,90,0,0,NULL),
 (900,4,6,81,90,0,1,ARRAY[90]::bigint[]);
DO $$ BEGIN
  BEGIN
    PERFORM partdist.dtx_reindex_committed(90,900::oid,1);
    RAISE EXCEPTION 'injected transaction rollback after indexing';
  EXCEPTION WHEN raise_exception THEN NULL;
  END;
  IF EXISTS(SELECT FROM partdist.dtx_commit_decided WHERE dtxid=80)
     OR EXISTS(SELECT FROM partdist.dtx_decision_replay WHERE coord_gsid=90) THEN
    RAISE EXCEPTION 'index and replay cursor were not rolled back together';
  END IF;
  PERFORM partdist.dtx_reindex_committed(90,900::oid,1);
  IF NOT EXISTS(SELECT FROM partdist.dtx_commit_decided WHERE dtxid=80)
     OR EXISTS(SELECT FROM partdist.dtx_decision WHERE dtxid=80) THEN
    RAISE EXCEPTION 'pending decision replayed as a timestamped decision';
  END IF;
  PERFORM partdist.dtx_reindex_committed(90,900::oid,2);
  IF (SELECT commit_ts FROM partdist.dtx_decision WHERE dtxid=80) IS DISTINCT FROM 500 THEN
    RAISE EXCEPTION 'durable timestamp not recovered';
  END IF;
  PERFORM partdist.dtx_reindex_committed(90,900::oid,3);
  PERFORM partdist.dtx_reindex_committed(90,900::oid,3);
  IF EXISTS(SELECT FROM partdist.dtx_decision WHERE dtxid=80)
     OR EXISTS(SELECT FROM partdist.dtx_commit_decided WHERE dtxid IN (80,81)) THEN
    RAISE EXCEPTION 'FORGET resurrected COMMIT or uncommitted tail was applied';
  END IF;
END $$;
SELECT 'FIRST_SHARD_STATE_MACHINE_OK';
"""


RECOVERY_CASES = r"""
INSERT INTO partdist.partition_map VALUES(10,1),(20,2);
CREATE TABLE partdist.recovery_gate(verdict text,ack boolean);
INSERT INTO partdist.recovery_gate VALUES('2',true);
CREATE OR REPLACE FUNCTION partdist.dtx_rpc(h text,p integer,q text)
RETURNS text LANGUAGE plpgsql AS $$
BEGIN
  IF (SELECT fail FROM partdist.rpc_gate) THEN RAISE EXCEPTION 'unavailable'; END IF;
  INSERT INTO partdist.events(event) VALUES(q);
  IF q LIKE '%dtx_decide(%' THEN RETURN (SELECT verdict FROM partdist.recovery_gate); END IF;
  RETURN (SELECT CASE WHEN ack THEN 't' ELSE 'f' END FROM partdist.recovery_gate);
END $$;
SELECT partdist.dtx_enlist(100,10,'aborted','worker1',5433,ARRAY[10]::bigint[]);
SELECT partdist.dtx_vote(100,'aborted',false);
SELECT partdist.dtx_route_claim(100,10);
UPDATE partdist.rpc_gate SET fail=true;
SELECT partdist.dtx_dispatch();
DO $$ BEGIN
  IF NOT EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=100) THEN
    RAISE EXCEPTION 'unavailable decision service lost registration'; END IF;
END $$;
UPDATE partdist.rpc_gate SET fail=false;
UPDATE partdist.recovery_gate SET verdict='1';
SELECT partdist.dtx_dispatch();
DO $$ BEGIN
  IF NOT EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=100)
     OR EXISTS(SELECT FROM partdist.events WHERE event LIKE '%dtx_finish_decision(100%') THEN
    RAISE EXCEPTION 'existing COMMIT was replaced or registration lost'; END IF;
END $$;
UPDATE partdist.recovery_gate SET verdict='0';
SELECT partdist.dtx_dispatch();
DO $$ BEGIN
  IF NOT EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=100) THEN
    RAISE EXCEPTION 'timestamp-pending COMMIT was discarded'; END IF;
END $$;
UPDATE partdist.recovery_gate SET verdict='2',ack=false;
SELECT partdist.dtx_dispatch();
DO $$ BEGIN
  IF NOT EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=100) THEN
    RAISE EXCEPTION 'registration lost before participant acknowledgement'; END IF;
END $$;
UPDATE partdist.recovery_gate SET ack=true;
SELECT partdist.dtx_dispatch();
DO $$ BEGIN
  IF EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=100)
     OR EXISTS(SELECT FROM partdist.dtx_coord_branch WHERE dtxid=100) THEN
    RAISE EXCEPTION 'fully acknowledged rollback was not reclaimed'; END IF;
END $$;
TRUNCATE partdist.events;
SELECT partdist.dtx_enlist(101,10,'ended','worker1',5433,ARRAY[10]::bigint[]);
SELECT partdist.dtx_enlist(101,10,'active','worker2',5434,ARRAY[20]::bigint[]);
SELECT partdist.dtx_vote(101,'ended',false);
SELECT partdist.dtx_dispatch();
DO $$ BEGIN
  IF NOT EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=101)
     OR EXISTS(SELECT FROM partdist.events WHERE event LIKE '%dtx_decide(%') THEN
    RAISE EXCEPTION 'active branch was presumed aborted'; END IF;
END $$;
SELECT 'FIRST_SHARD_STATE_MACHINE_OK';
"""


class CoordinatorSQLTests(unittest.TestCase):
    def run_sql_case(self, case_sql):
        env = dict(os.environ)
        env["LD_LIBRARY_PATH"] = str(ROOT / "pg-install/lib")
        script = SQL.read_text()
        # Replace only C ABI declarations, not the PL/pgSQL under test.
        script = re.sub(r"CREATE OR REPLACE FUNCTION[^;]+?LANGUAGE c[^;]*;", "", script,
                        flags=re.S)
        # Install function definitions on one line each for single-user mode.
        # Strip SQL line comments before joining: preserve dollar-quoted bodies.
        script = " ".join(re.sub(r"--[^\n]*", "", script).splitlines())
        setup = " ".join(STUBS.splitlines())
        cases = " ".join(re.sub(r"--[^\n]*", "", case_sql).splitlines())
        with tempfile.TemporaryDirectory(prefix="shardpg-first-coord-sql-") as td:
            init = subprocess.run([str(ROOT / "pg-install/bin/initdb"), "-D", td,
                                   "--no-locale", "-A", "trust"], env=env,
                                  capture_output=True, text=True, timeout=60)
            self.assertEqual(init.returncode, 0, init.stdout + init.stderr)
            run = subprocess.run([str(ROOT / "pg-install/bin/postgres"), "--single",
                                  "-D", td, "postgres"], input=setup+"\n"+script+"\n"+cases+"\n",
                                 env=env, capture_output=True, text=True, timeout=60)
            self.assertEqual(run.returncode, 0, run.stdout + run.stderr)
            self.assertNotRegex(run.stderr, r"\b(ERROR|FATAL|PANIC):", run.stdout + run.stderr)
            self.assertIn('= "FIRST_SHARD_STATE_MACHINE_OK"', run.stdout)

    def test_actual_sql_state_machine(self):
        self.run_sql_case(CASES)

    def test_aborted_registration_recovery(self):
        self.run_sql_case(RECOVERY_CASES)

    def test_install_script_matches_upgrade(self):
        install = (ROOT / "pg-partdist-src/sql/pg_partdist--1.0.sql").read_text()
        marker = "-- BEGIN first_shard_coordinator.sql (keep in sync with standalone upgrade)\n"
        self.assertEqual(install.split(marker)[1], SQL.read_text())
        raft = ROOT / "pg-raft-src/sql"
        marker = "-- BEGIN first_shard_decision.sql (keep in sync with standalone upgrade)\n"
        self.assertEqual((raft / "pg_raft--1.0.sql").read_text().split(marker)[1],
                         (raft / "first_shard_decision.sql").read_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
