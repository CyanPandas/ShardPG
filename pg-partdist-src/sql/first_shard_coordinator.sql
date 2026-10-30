-- Install on every node after rebuilding Citus with citus-0004 and pg_partdist.
-- The route table lives on the TSO/router. It records only a write-once route;
-- participants, votes and the prepare state machine live on the chosen shard.
CREATE TABLE IF NOT EXISTS partdist.dtx_route (
    gxid bigint PRIMARY KEY CHECK (gxid > 0),
    coord_gsid bigint NOT NULL CHECK (coord_gsid > 0)
);
-- Raft kind 6 is an irrevocable COMMIT without a timestamp. This index is
-- rebuilt on every coordinator replica; it is never a visibility decision.
CREATE TABLE IF NOT EXISTS partdist.dtx_commit_decided (
    dtxid bigint PRIMARY KEY,
    coord_gsid bigint NOT NULL,
    participants bigint[] NOT NULL CHECK(cardinality(participants)>0),
    decided_plsn bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS partdist.dtx_decision_replay (
    coord_gsid bigint PRIMARY KEY,
    plsn bigint NOT NULL DEFAULT 0
);

-- SQL indexes and this cursor commit together. The shared Raft apply cursor
-- alone is insufficient: its advance survives a rollback of the SQL indexes.
-- Called under the coordinator's replication claim, with a committed bound.
CREATE OR REPLACE FUNCTION partdist.dtx_reindex_committed(
    p_coord bigint,p_oid oid,p_upto bigint) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE from_plsn bigint; r record;
BEGIN
    INSERT INTO partdist.dtx_decision_replay(coord_gsid) VALUES(p_coord) ON CONFLICT DO NOTHING;
    SELECT plsn INTO from_plsn FROM partdist.dtx_decision_replay
      WHERE coord_gsid=p_coord FOR UPDATE;
    IF p_upto <= from_plsn THEN RETURN; END IF;
    FOR r IN SELECT g AS plsn,d.* FROM generate_series(from_plsn+1,p_upto) g
             CROSS JOIN LATERAL partdist.partwal_read_dtx_record(p_oid,g) d
             WHERE d.kind IN (2,5,6) ORDER BY g
    LOOP
        IF r.coord_gsid<>p_coord THEN RAISE EXCEPTION 'decision in wrong coordinator stream'; END IF;
        IF r.kind=6 THEN
            IF r.verdict<>1 OR r.commit_ts<>0 THEN RAISE EXCEPTION 'invalid COMMIT decision'; END IF;
            INSERT INTO partdist.dtx_commit_decided(dtxid,coord_gsid,participants,decided_plsn)
              VALUES(r.dtxid,p_coord,r.participants,r.plsn) ON CONFLICT DO NOTHING;
        ELSIF r.kind=2 THEN
            INSERT INTO partdist.dtx_decision(dtxid,coord_gsid,verdict,commit_ts,participants,decided_plsn)
              VALUES(r.dtxid,p_coord,r.verdict,r.commit_ts,r.participants,r.plsn) ON CONFLICT DO NOTHING;
        ELSE
            DELETE FROM partdist.dtx_decision WHERE dtxid=r.dtxid;
            DELETE FROM partdist.dtx_commit_decided WHERE dtxid=r.dtxid;
        END IF;
    END LOOP;
    UPDATE partdist.dtx_decision_replay SET plsn=greatest(plsn,p_upto) WHERE coord_gsid=p_coord;
END $$;
CREATE TABLE IF NOT EXISTS partdist.dtx_coord_tx (
    dtxid bigint PRIMARY KEY,
    coord_gsid bigint NOT NULL,
    phase text NOT NULL DEFAULT 'OPEN' CHECK (phase IN ('OPEN','PREPARE','ABORT')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS partdist.dtx_coord_branch (
    dtxid bigint NOT NULL REFERENCES partdist.dtx_coord_tx(dtxid),
    gid text NOT NULL,
    hostname text NOT NULL,
    port integer NOT NULL,
    gsids bigint[] NOT NULL CHECK (cardinality(gsids) > 0),
    vote boolean,
    PRIMARY KEY(dtxid,gid)
);
CREATE TABLE IF NOT EXISTS partdist.dtx_dispatch_cursor (
    singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
    last_id bigint NOT NULL DEFAULT 0
);
INSERT INTO partdist.dtx_dispatch_cursor(singleton) VALUES(true) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION partdist.dtx_rpc(text,integer,text) RETURNS text
AS '$libdir/pg_partdist', 'partdist_dtx_rpc' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_tso_rpc(text) RETURNS text
AS '$libdir/pg_partdist', 'partdist_dtx_tso_rpc' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_self(text) RETURNS text
AS '$libdir/pg_partdist', 'partdist_dtx_self' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_commit_ts() RETURNS bigint
AS '$libdir/pg_partdist', 'partdist_dtx_commit_ts' LANGUAGE c VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_pending_exists(bigint) RETURNS boolean
AS '$libdir/pg_partdist', 'partdist_dtx_pending_exists' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.shard_is_prepared(oid,bigint) RETURNS boolean
AS '$libdir/pg_partdist', 'partdist_shard_is_prepared' LANGUAGE c STRICT VOLATILE;
COMMENT ON FUNCTION partdist.shard_clog_status(oid,bigint) IS
    'Shard-local clog: 0=IN_PROGRESS (including prepared transactions), 2=COMMITTED, 3=ABORTED. Two-phase membership is independent; numeric codes retain the existing disk format.';
CREATE OR REPLACE FUNCTION partdist.dtx_branch_begin(text) RETURNS void
AS '$libdir/pg_partdist', 'partdist_dtx_branch_begin' LANGUAGE c STRICT VOLATILE;
CREATE OR REPLACE FUNCTION partdist.dtx_branch_ready() RETURNS boolean
AS '$libdir/pg_partdist', 'partdist_dtx_branch_ready' LANGUAGE c VOLATILE;
CREATE OR REPLACE PROCEDURE partdist.dtx_handoff() LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'dtx_handoff requires the pg_partdist utility hook';
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_route_claim(p_xid bigint, p_shard bigint)
RETURNS bigint LANGUAGE sql VOLATILE SET search_path=pg_catalog,partdist AS $$
    INSERT INTO partdist.dtx_route AS r(gxid,coord_gsid) VALUES(p_xid,p_shard)
    ON CONFLICT(gxid) DO UPDATE SET coord_gsid=r.coord_gsid
    RETURNING coord_gsid
$$;

CREATE OR REPLACE FUNCTION partdist.dtx_enlist(p_xid bigint, p_coord bigint,
    p_gid text, p_host text, p_port integer, p_shards bigint[])
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE t partdist.dtx_coord_tx; old partdist.dtx_coord_branch;
BEGIN
    IF NOT EXISTS (SELECT FROM partdist.pg_raft_group_status()
                   WHERE group_id=p_coord AND state='leader') THEN
        RAISE EXCEPTION 'this node is not the coordinator shard leader';
    END IF;
    INSERT INTO partdist.dtx_coord_tx(dtxid,coord_gsid) VALUES(p_xid,p_coord)
    ON CONFLICT DO NOTHING;
    SELECT * INTO STRICT t FROM partdist.dtx_coord_tx WHERE dtxid=p_xid FOR UPDATE;
    IF t.coord_gsid <> p_coord OR t.phase <> 'OPEN' THEN
        RAISE EXCEPTION 'coordinator changed or participant set already sealed';
    END IF;
    SELECT * INTO old FROM partdist.dtx_coord_branch WHERE dtxid=p_xid AND gid=p_gid;
    IF FOUND AND (old.hostname <> p_host OR old.port <> p_port OR
                  NOT (old.gsids <@ p_shards)) THEN
        RAISE EXCEPTION 'branch identity changed or shard set shrank';
    END IF;
    INSERT INTO partdist.dtx_coord_branch(dtxid,gid,hostname,port,gsids)
    VALUES(p_xid,p_gid,p_host,p_port,p_shards)
    ON CONFLICT(dtxid,gid) DO UPDATE SET gsids=EXCLUDED.gsids;
    RETURN true;
END $$;

-- Runs in an autonomous transaction. The row lock serializes enlistment and
-- sealing: after this commits, no new participant may silently join.
CREATE OR REPLACE FUNCTION partdist.dtx_seal(p_xid bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE t partdist.dtx_coord_tx; shards bigint[];
BEGIN
    SELECT * INTO STRICT t FROM partdist.dtx_coord_tx WHERE dtxid=p_xid FOR UPDATE;
    IF t.phase='ABORT' THEN RAISE EXCEPTION 'transaction already aborted'; END IF;
    IF NOT EXISTS (SELECT FROM partdist.pg_raft_group_status()
                   WHERE group_id=t.coord_gsid AND state='leader') THEN
        RAISE EXCEPTION 'coordinator lost leadership before prepare';
    END IF;
    SELECT array_agg(DISTINCT s ORDER BY s) INTO shards
    FROM partdist.dtx_coord_branch b CROSS JOIN LATERAL unnest(b.gsids) s
    WHERE b.dtxid=p_xid;
    IF coalesce(cardinality(shards),0)=0 OR NOT t.coord_gsid=ANY(shards) THEN
        RAISE EXCEPTION 'coordinator has no complete participant set';
    END IF;
    UPDATE partdist.dtx_coord_tx SET phase='PREPARE' WHERE dtxid=p_xid;
    RETURN jsonb_build_object('coord',t.coord_gsid,'shards',to_jsonb(shards));
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_branch_command(p_xid bigint,p_gid text)
RETURNS text LANGUAGE sql VOLATILE SET search_path=pg_catalog,partdist AS $$
    SELECT t.phase FROM partdist.dtx_coord_tx t JOIN partdist.dtx_coord_branch b USING(dtxid)
    WHERE t.dtxid=p_xid AND b.gid=p_gid
$$;

CREATE OR REPLACE FUNCTION partdist.dtx_vote(p_xid bigint,p_gid text,p_ok boolean)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE phase_now text;
BEGIN
    SELECT phase INTO STRICT phase_now FROM partdist.dtx_coord_tx WHERE dtxid=p_xid FOR UPDATE;
    IF phase_now='ABORT' THEN RETURN false; END IF;
    IF p_ok AND phase_now <> 'PREPARE' THEN RAISE EXCEPTION 'unexpected prepare vote'; END IF;
    UPDATE partdist.dtx_coord_branch SET vote=p_ok
      WHERE dtxid=p_xid AND gid=p_gid AND (vote IS NULL OR vote=p_ok);
    IF NOT FOUND THEN RAISE EXCEPTION 'unknown branch or conflicting vote'; END IF;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_votes(p_xid bigint)
RETURNS text LANGUAGE sql VOLATILE SET search_path=pg_catalog,partdist AS $$
    SELECT CASE WHEN count(*)=0 THEN 'WAIT'
                WHEN bool_or(vote IS FALSE) THEN 'ABORT'
                WHEN count(*)=count(vote) AND bool_and(vote) THEN 'READY'
                ELSE 'WAIT' END
    FROM partdist.dtx_coord_branch WHERE dtxid=p_xid
$$;

-- The only normal-path prepare driver. The router forwards one client COMMIT
-- here; it neither passes a participant list nor collects the prepare votes.
CREATE OR REPLACE FUNCTION partdist.dtx_record_commit(p_coord bigint,p_xid bigint,
    p_shards bigint[],p_cts bigint) RETURNS bigint
LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE v integer; result bigint;
BEGIN
    PERFORM pg_advisory_xact_lock(p_xid);
    IF p_cts <= 0 THEN RAISE EXCEPTION 'invalid commit timestamp'; END IF;
    IF NOT EXISTS(SELECT FROM partdist.dtx_commit_decided
                  WHERE dtxid=p_xid AND coord_gsid=p_coord AND participants=p_shards) THEN
        RAISE EXCEPTION 'COMMIT decision must precede its timestamp';
    END IF;
    v:=partdist.dtx_decide(p_coord,p_xid,1,p_shards,p_cts);
    IF v IS DISTINCT FROM 1 THEN RAISE EXCEPTION 'coordinator rejected commit'; END IF;
    SELECT commit_ts INTO STRICT result FROM partdist.dtx_decision WHERE dtxid=p_xid;
    RETURN result;
END $$;

-- Also used after a crash between durable COMMIT and timestamp attachment.
-- No failure here can change the already chosen COMMIT into ABORT.
CREATE OR REPLACE FUNCTION partdist.dtx_complete_commit(p_xid bigint)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE d partdist.dtx_commit_decided; cts bigint;
BEGIN
    SELECT commit_ts INTO cts FROM partdist.dtx_decision WHERE dtxid=p_xid AND verdict=1;
    IF FOUND THEN RETURN cts; END IF;
    SELECT * INTO STRICT d FROM partdist.dtx_commit_decided WHERE dtxid=p_xid;
    IF NOT EXISTS(SELECT FROM partdist.pg_raft_group_status()
                  WHERE group_id=d.coord_gsid AND state='leader') THEN
        RAISE EXCEPTION 'coordinator lost leadership before timestamp attachment';
    END IF;
    cts:=partdist.dtx_commit_ts();
    RETURN partdist.dtx_self(format(
        'SELECT partdist.dtx_record_commit(%s,%s,%L::bigint[],%s)',
        d.coord_gsid,p_xid,d.participants,cts))::bigint;
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_coordinate(p_xid bigint)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE plan jsonb; coord bigint; shards bigint[]; votes text; cts bigint;
        started timestamptz:=clock_timestamp(); timeout_ms integer;
BEGIN
    -- A retry must return the same timestamp, never allocate a second one.
    SELECT commit_ts INTO cts FROM partdist.dtx_decision WHERE dtxid=p_xid AND verdict=1;
    IF FOUND THEN RETURN cts; END IF;
    IF EXISTS(SELECT FROM partdist.dtx_commit_decided WHERE dtxid=p_xid) THEN
        RETURN partdist.dtx_complete_commit(p_xid);
    END IF;
    plan:=partdist.dtx_self(format('SELECT partdist.dtx_seal(%s)',p_xid))::jsonb;
    coord:=(plan->>'coord')::bigint;
    SELECT array_agg(x::bigint) INTO shards FROM jsonb_array_elements_text(plan->'shards') x;
    SELECT coalesce((SELECT setting::integer FROM pg_settings
                     WHERE name='shardpg.prepare_timeout_ms'),30000) INTO timeout_ms;
    LOOP
        votes:=partdist.dtx_self(format('SELECT partdist.dtx_votes(%s)',p_xid));
        EXIT WHEN votes='READY';
        IF votes='ABORT' OR clock_timestamp()-started > timeout_ms*interval '1 millisecond' THEN
            PERFORM partdist.dtx_self(format('SELECT partdist.dtx_decide(%s,%s,2,%L::bigint[],0)',
                                            coord,p_xid,shards));
            RAISE EXCEPTION 'participant prepare failed or timed out';
        END IF;
        PERFORM pg_sleep(0.01);
    END LOOP;
    IF partdist.dtx_self(format('SELECT partdist.dtx_choose_commit(%s,%s,%L::bigint[])',
                                coord,p_xid,shards))::integer IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION 'coordinator rejected COMMIT decision';
    END IF;
    -- The autonomous request above has committed kind 6 to a quorum BEFORE
    -- asking TSO. Only the subsequent timestamped record may be broadcast.
    RETURN partdist.dtx_complete_commit(p_xid);
END $$;

-- PREPARE registration must include the coordinator before the native prepare
-- WAL is durable, including the window before its vote is delivered.
CREATE OR REPLACE FUNCTION partdist.dtx_note_participant(p_dtxid bigint,p_gid text,p_gsids bigint[])
RETURNS boolean LANGUAGE sql VOLATILE SET search_path=pg_catalog,partdist AS $$
    INSERT INTO partdist.dtx_participant(dtxid,gid,gsids,coord_gsid)
    VALUES(p_dtxid,p_gid,p_gsids,
           CASE WHEN left(p_gid,11)='shardpg_fc_' THEN split_part(p_gid,'_',4)::bigint END)
    ON CONFLICT(dtxid,gid) DO UPDATE SET gsids=EXCLUDED.gsids,
      coord_gsid=coalesce(EXCLUDED.coord_gsid,partdist.dtx_participant.coord_gsid),noted_at=now()
    RETURNING true
$$;

CREATE OR REPLACE FUNCTION partdist.dtx_finish_decision(p_xid bigint,p_coord bigint,
    p_verdict integer,p_cts bigint)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE b record;
BEGIN
    IF p_verdict NOT IN (1,2) OR (p_verdict=1 AND p_cts<=0) THEN
        RAISE EXCEPTION 'invalid terminal decision';
    END IF;
    IF EXISTS(SELECT FROM partdist.dtx_participant WHERE dtxid=p_xid
              AND coord_gsid IS DISTINCT FROM p_coord) THEN
        RAISE EXCEPTION 'decision came from a different coordinator';
    END IF;
    PERFORM partdist.dtx_apply_decision(p_xid,p_verdict,p_cts);
    IF partdist.dtx_pending_exists(p_xid) THEN RETURN false; END IF;
    FOR b IN SELECT p.gid FROM partdist.dtx_participant p
             JOIN pg_prepared_xacts x USING(gid)
             WHERE p.dtxid=p_xid AND x.database=current_database()
    LOOP
        BEGIN
            PERFORM partdist.dtx_self(format('%s PREPARED %L',
                        CASE WHEN p_verdict=1 THEN 'COMMIT' ELSE 'ROLLBACK' END,b.gid));
        EXCEPTION WHEN OTHERS THEN
            -- Another recovery worker may already have finished it. An actual
            -- failure remains queued; never acknowledge on connection failure.
            IF EXISTS(SELECT FROM pg_prepared_xacts WHERE gid=b.gid) THEN RETURN false; END IF;
        END;
    END LOOP;
    RETURN true;
END $$;

-- Run on coordinator leaders, outside the client request. The Raft-replicated
-- decision is the outbox: leadership changes need no router-local queue.
CREATE OR REPLACE FUNCTION partdist.dtx_dispatch()
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE d record; n record; delivered integer:=0; result text; complete boolean;
        cursor_id bigint;
BEGIN
    IF NOT pg_try_advisory_xact_lock(1936745540,1) THEN RETURN 0; END IF;
    -- Autonomous refresh releases its index locks before timestamp completion
    -- makes another local RPC. It also repairs indexes lost after quorum commit.
    FOR d IN SELECT group_id FROM partdist.pg_raft_group_status()
             WHERE state='leader' AND group_id>0
    LOOP
        BEGIN
            PERFORM partdist.dtx_self(format('SELECT partdist.dtx_refresh(%s)',d.group_id));
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
    SELECT last_id INTO cursor_id FROM partdist.dtx_dispatch_cursor WHERE singleton;
    FOR d IN SELECT dd.dtxid FROM partdist.dtx_commit_decided dd
             JOIN partdist.pg_raft_group_status() g ON g.group_id=dd.coord_gsid
             WHERE g.state='leader' AND NOT EXISTS
               (SELECT FROM partdist.dtx_decision f WHERE f.dtxid=dd.dtxid)
             ORDER BY (dd.dtxid <= cursor_id),dd.dtxid LIMIT 32
    LOOP
        BEGIN
            PERFORM partdist.dtx_complete_commit(d.dtxid);
        EXCEPTION WHEN OTHERS THEN
            -- TSO unavailable: retain COMMIT and retry; never broadcast ts=0.
            NULL;
        END;
    END LOOP;
    FOR d IN SELECT dd.* FROM partdist.dtx_decision dd
             JOIN partdist.pg_raft_group_status() g ON g.group_id=dd.coord_gsid
             WHERE g.state='leader'
             ORDER BY (dd.dtxid <= cursor_id),dd.dtxid LIMIT 32
    LOOP
        complete:=true;
        FOR n IN SELECT DISTINCT hostname,port FROM partdist.node_map
        LOOP
            BEGIN
                result:=partdist.dtx_rpc(n.hostname,n.port,
                    format('SELECT partdist.dtx_finish_decision(%s,%s,%s,%s)',
                           d.dtxid,d.coord_gsid,d.verdict,d.commit_ts));
                IF result='t' THEN delivered:=delivered+1; ELSE complete:=false; END IF;
            EXCEPTION WHEN OTHERS THEN
                -- Keep DECISION until the existing per-shard acknowledgement
                -- gates confirm native completion AND quorum verdict delivery.
                complete:=false;
            END;
        END LOOP;
        IF complete THEN
            DELETE FROM partdist.dtx_coord_branch WHERE dtxid=d.dtxid;
            DELETE FROM partdist.dtx_coord_tx WHERE dtxid=d.dtxid;
            BEGIN
                PERFORM partdist.dtx_tso_rpc(format(
                    'WITH removed AS (DELETE FROM partdist.dtx_route WHERE gxid=%s AND coord_gsid=%s RETURNING 1) SELECT true',
                    d.dtxid,d.coord_gsid));
            EXCEPTION WHEN OTHERS THEN
                -- Failure to remove a route must not change a committed outcome.
                NULL;
            END;
        END IF;
        UPDATE partdist.dtx_dispatch_cursor SET last_id=d.dtxid WHERE singleton;
    END LOOP;
    RETURN delivered;
END $$;

REVOKE ALL ON partdist.dtx_route,partdist.dtx_coord_tx,partdist.dtx_coord_branch,
    partdist.dtx_dispatch_cursor,partdist.dtx_commit_decided,partdist.dtx_decision_replay FROM PUBLIC;
REVOKE ALL ON FUNCTION partdist.dtx_rpc(text,integer,text),partdist.dtx_tso_rpc(text),
    partdist.dtx_self(text),partdist.dtx_route_claim(bigint,bigint),
    partdist.dtx_enlist(bigint,bigint,text,text,integer,bigint[]),
    partdist.dtx_seal(bigint),partdist.dtx_vote(bigint,text,boolean),
    partdist.dtx_branch_command(bigint,text),partdist.dtx_votes(bigint),
    partdist.dtx_coordinate(bigint),partdist.dtx_dispatch(),
    partdist.dtx_record_commit(bigint,bigint,bigint[],bigint),
    partdist.dtx_complete_commit(bigint),
    partdist.dtx_reindex_committed(bigint,oid,bigint),
    partdist.dtx_finish_decision(bigint,bigint,integer,bigint) FROM PUBLIC;
