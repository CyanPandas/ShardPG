-- Persistent, decision-only shards for DDL. Business shard identity and MVCC
-- remain separate; Citus shard IDs are reserved from its global sequence.
CREATE TABLE IF NOT EXISTS partdist.dtx_ddl_shard (
    owner_node integer PRIMARY KEY CHECK(owner_node>0),
    group_id bigint UNIQUE NOT NULL CHECK(group_id>0),
    anchor_oid oid
);
CREATE TABLE IF NOT EXISTS partdist.dtx_ddl_outcome (
    dtxid bigint PRIMARY KEY, coord_gsid bigint NOT NULL,
    verdict integer NOT NULL CHECK(verdict IN (1,2)), commit_ts bigint NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON partdist.dtx_ddl_outcome FROM PUBLIC;
CREATE TABLE IF NOT EXISTS partdist.dtx_ddl_rejected (
 coord_gsid bigint NOT NULL,dtxid bigint NOT NULL,
 PRIMARY KEY(coord_gsid,dtxid)
);
REVOKE ALL ON partdist.dtx_ddl_rejected FROM PUBLIC;

REVOKE ALL ON partdist.dtx_ddl_shard FROM PUBLIC;

CREATE OR REPLACE FUNCTION partdist.local_partition_for_shard(p_global_shard_id bigint)
RETURNS oid LANGUAGE sql STABLE AS $$
    SELECT local_oid FROM partdist.shard_identity WHERE global_shard_id=p_global_shard_id
    UNION ALL
    SELECT anchor_oid FROM partdist.dtx_ddl_shard
      WHERE group_id=p_global_shard_id AND anchor_oid IS NOT NULL
$$;
CREATE OR REPLACE FUNCTION partdist.global_id_for_partition(p_local_oid oid)
RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT global_shard_id FROM partdist.shard_identity WHERE local_oid=p_local_oid
    UNION ALL
    SELECT group_id FROM partdist.dtx_ddl_shard WHERE anchor_oid=p_local_oid
$$;

CREATE OR REPLACE FUNCTION partdist.dtx_branch_set_ddl() RETURNS void
AS '$libdir/pg_partdist', 'partdist_dtx_branch_set_ddl' LANGUAGE c VOLATILE;

CREATE OR REPLACE FUNCTION partdist.dtx_ddl_install(
    p_owner integer,p_group bigint,p_members integer[])
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist
SET citus.enable_ddl_propagation=off AS $$
DECLARE old partdist.dtx_ddl_shard; anchor oid; name text;
BEGIN
    IF NOT current_setting('pg_raft.node_id')::integer=ANY(p_members) THEN
        RAISE EXCEPTION 'DDL shard replica must be a worker member';
    END IF;
    SELECT * INTO old FROM partdist.dtx_ddl_shard WHERE owner_node=p_owner;
    IF FOUND AND old.group_id<>p_group THEN RAISE EXCEPTION 'DDL shard identity changed'; END IF;
    name:=format('dtx_ddl_anchor_%s',p_owner);
    EXECUTE format('CREATE TABLE IF NOT EXISTS partdist.%I (reserved boolean)',name);
    anchor:=to_regclass(format('partdist.%I',name))::oid;
    INSERT INTO partdist.dtx_ddl_shard(owner_node,group_id,anchor_oid)
      VALUES(p_owner,p_group,anchor)
      ON CONFLICT(owner_node) DO UPDATE SET anchor_oid=EXCLUDED.anchor_oid;
    PERFORM partdist.pg_raft_group_create(p_group,p_members);
    RETURN true;
END $$;

-- Allocation commits independently before any worker receives its identity.
-- Retrying a partial initialization therefore reuses the same global IDs.
CREATE OR REPLACE FUNCTION partdist.dtx_ddl_allocate(p_owner integer)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE gid bigint;
BEGIN
    INSERT INTO partdist.dtx_ddl_shard AS d(owner_node,group_id)
      VALUES(p_owner,nextval('pg_catalog.pg_dist_shardid_seq'))
      ON CONFLICT(owner_node) DO UPDATE SET group_id=d.group_id
      RETURNING group_id INTO gid;
    RETURN gid;
END $$;

-- Called through an independent router connection before opening DDL task
-- sockets. Never change first_shard_coordinator or reuse a business table.
CREATE OR REPLACE FUNCTION partdist.dtx_ddl_ensure()
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE workers integer[]; owner integer; gid bigint; n record; answer text;
        started timestamptz:=clock_timestamp(); ready boolean;
BEGIN
    IF current_setting('pg_raft.node_id')::integer <>
       current_setting('pg_raft.coordinator_node_id')::integer THEN
        RAISE EXCEPTION 'DDL metadata initialization must run on the router';
    END IF;
    PERFORM pg_advisory_xact_lock(1936745540,2);
    SELECT array_agg(node_id ORDER BY node_id) INTO workers FROM partdist.node_map
      WHERE status='active' AND node_id<>current_setting('pg_raft.coordinator_node_id')::integer;
    IF cardinality(workers)<1 OR workers IS NULL THEN RAISE EXCEPTION 'no DDL worker members'; END IF;
    FOREACH owner IN ARRAY workers LOOP
        SELECT group_id INTO gid FROM partdist.dtx_ddl_shard WHERE owner_node=owner;
        IF gid IS NULL THEN
            gid:=partdist.dtx_self(format('SELECT partdist.dtx_ddl_allocate(%s)',owner))::bigint;
        END IF;
        FOR n IN SELECT hostname,port FROM partdist.node_map WHERE node_id=ANY(workers) LOOP
            answer:=partdist.dtx_rpc(n.hostname,n.port,format(
                'SELECT partdist.dtx_ddl_install(%s,%s,%L::integer[])',owner,gid,workers));
            IF answer IS DISTINCT FROM 't' THEN RAISE EXCEPTION 'DDL shard installation failed'; END IF;
        END LOOP;
    END LOOP;
    LOOP
        SELECT bool_and(EXISTS(SELECT FROM partdist.partition_map m
                  WHERE m.partition_id::bigint=d.group_id AND m.primary_node=ANY(workers)))
          INTO ready FROM partdist.dtx_ddl_shard d WHERE d.owner_node=ANY(workers);
        IF ready THEN RETURN true; END IF;
        IF clock_timestamp()-started>interval '25 seconds' THEN
            RAISE EXCEPTION 'DDL metadata shard election or route publication timed out';
        END IF;
        PERFORM pg_sleep(0.1);
    END LOOP;
END $$;

ALTER TABLE partdist.dtx_coord_branch ADD COLUMN IF NOT EXISTS native_fullxid bigint;

CREATE OR REPLACE FUNCTION partdist.dtx_branch_probe(text,bigint) RETURNS text
AS '$libdir/pg_partdist', 'partdist_dtx_branch_probe' LANGUAGE c STRICT VOLATILE;

-- Control entries 7/8/9 are restricted to permanent metadata groups.
CREATE OR REPLACE FUNCTION partdist.dtx_meta_event(bigint,bigint,integer,bigint[])
RETURNS boolean AS '$libdir/pg_raft','pg_raft_dtx_metadata_event' LANGUAGE c VOLATILE;

CREATE OR REPLACE FUNCTION partdist.dtx_meta_validate(c bigint,x bigint,k integer,v bigint[])
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE t partdist.dtx_coord_tx; b partdist.dtx_coord_branch; branch_key text; a record; shards bigint[];
BEGIN
 IF NOT EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=c) THEN RAISE EXCEPTION 'not a DDL metadata group'; END IF;
 IF k=0 THEN
   SELECT * INTO b FROM partdist.dtx_coord_branch b0 WHERE b0.dtxid=x AND b0.gid=format('shardpg_fc_%s_%s_%s',x,c,v[1]);
   IF FOUND THEN
     IF coalesce(v[2],0)<>0 AND b.native_fullxid IS DISTINCT FROM v[2] THEN RAISE EXCEPTION 'DDL admission probe identity changed'; END IF;
     RETURN true;
   END IF;
   RETURN false;
 END IF;
 IF k=10 THEN RETURN NOT EXISTS(SELECT FROM partdist.dtx_ddl_rejected WHERE coord_gsid=c AND dtxid=x); END IF;
 IF k=5 THEN
   IF NOT EXISTS(SELECT FROM partdist.dtx_ddl_outcome WHERE dtxid=x AND coord_gsid=c) THEN
     RAISE EXCEPTION 'DDL completion has no durable outcome'; END IF;
   RETURN EXISTS(SELECT FROM partdist.dtx_coord_tx WHERE dtxid=x);
 END IF;
 SELECT * INTO t FROM partdist.dtx_coord_tx WHERE dtxid=x;
 IF FOUND AND t.coord_gsid<>c THEN RAISE EXCEPTION 'DDL coordinator identity changed'; END IF;
 IF k=7 THEN
   IF EXISTS(SELECT FROM partdist.dtx_ddl_rejected WHERE coord_gsid=c AND dtxid=x) OR EXISTS(SELECT FROM partdist.dtx_ddl_outcome WHERE dtxid=x) THEN
     RAISE EXCEPTION 'DDL admission was rejected or transaction already completed'; END IF;
   IF cardinality(v)<5 OR v[1]<=0 OR v[2]<0 OR v[3]<0 OR v[4]<1 OR cardinality(v)<>v[4]+4 THEN
     RAISE EXCEPTION 'invalid DDL admission payload'; END IF;
   IF t.dtxid IS NOT NULL AND t.phase<>'OPEN' THEN RAISE EXCEPTION 'DDL participant set is sealed'; END IF;
   SELECT hostname,port INTO STRICT a FROM partdist.node_map WHERE node_id=v[1];
   branch_key:=format('shardpg_fc_%s_%s_%s',x,c,v[2]);shards:=v[5:cardinality(v)];
   SELECT * INTO b FROM partdist.dtx_coord_branch b0 WHERE b0.dtxid=x AND b0.gid=branch_key;
   IF FOUND THEN
     IF b.hostname<>a.hostname OR b.port<>a.port OR NOT b.gsids<@shards OR
        (b.native_fullxid IS NOT NULL AND v[3]<>0 AND b.native_fullxid<>v[3]) THEN
       RAISE EXCEPTION 'DDL branch identity changed'; END IF;
     RETURN b.gsids<>shards OR (b.native_fullxid IS NULL AND v[3]<>0);
   END IF;
   RETURN true;
 ELSIF k=8 THEN
   IF cardinality(v)<>2 OR v[2] NOT IN (0,1) THEN RAISE EXCEPTION 'invalid DDL vote'; END IF;
   branch_key:=format('shardpg_fc_%s_%s_%s',x,c,v[1]);
   SELECT * INTO STRICT b FROM partdist.dtx_coord_branch b0 WHERE b0.dtxid=x AND b0.gid=branch_key;
   IF v[2]=1 AND t.phase<>'PREPARE' THEN RAISE EXCEPTION 'unexpected DDL prepare vote'; END IF;
   IF b.vote IS NOT NULL AND b.vote<>(v[2]=1) THEN RAISE EXCEPTION 'conflicting DDL vote'; END IF;
   RETURN b.vote IS NULL;
 ELSIF k=9 THEN
   IF t.dtxid IS NULL OR t.phase='ABORT' THEN RAISE EXCEPTION 'DDL transaction cannot seal'; END IF;
   SELECT array_agg(DISTINCT s ORDER BY s) INTO shards FROM partdist.dtx_coord_branch b0 CROSS JOIN LATERAL unnest(b0.gsids)s WHERE b0.dtxid=x;
   IF cardinality(shards)<1 OR NOT c=ANY(shards) THEN RAISE EXCEPTION 'DDL coordinator has no participant'; END IF;
   RETURN t.phase<>'PREPARE';
 END IF;
 RAISE EXCEPTION 'unknown DDL control event';
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_meta_apply(c bigint,x bigint,k integer,v bigint[])
RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE a record; branch_key text;
BEGIN
 IF k=10 THEN
   INSERT INTO partdist.dtx_ddl_rejected(coord_gsid,dtxid) VALUES(c,x) ON CONFLICT DO NOTHING;
 ELSIF k=7 THEN
   SELECT hostname,port INTO STRICT a FROM partdist.node_map WHERE node_id=v[1];
   branch_key:=format('shardpg_fc_%s_%s_%s',x,c,v[2]);
   INSERT INTO partdist.dtx_coord_tx(dtxid,coord_gsid) VALUES(x,c) ON CONFLICT DO NOTHING;
   INSERT INTO partdist.dtx_coord_branch AS b(dtxid,gid,hostname,port,gsids,native_fullxid)
     VALUES(x,branch_key,a.hostname,a.port,v[5:cardinality(v)],nullif(v[3],0))
     ON CONFLICT(dtxid,gid) DO UPDATE SET gsids=EXCLUDED.gsids,native_fullxid=coalesce(b.native_fullxid,EXCLUDED.native_fullxid);
 ELSIF k=8 THEN
   branch_key:=format('shardpg_fc_%s_%s_%s',x,c,v[1]);
   UPDATE partdist.dtx_coord_branch b SET vote=(v[2]=1) WHERE b.dtxid=x AND b.gid=branch_key;
 ELSIF k=9 THEN
   UPDATE partdist.dtx_coord_tx SET phase='PREPARE' WHERE dtxid=x;
 END IF;
END $$;
REVOKE ALL ON FUNCTION partdist.dtx_meta_event(bigint,bigint,integer,bigint[]),
 partdist.dtx_meta_validate(bigint,bigint,integer,bigint[]),
 partdist.dtx_meta_apply(bigint,bigint,integer,bigint[]) FROM PUBLIC;

CREATE OR REPLACE FUNCTION partdist.dtx_enlist_native(
    p_xid bigint,p_coord bigint,p_gid text,p_host text,p_port integer,
    p_shards bigint[],p_native bigint)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE origin integer;
BEGIN
    IF EXISTS(SELECT FROM partdist.dtx_ddl_shard WHERE group_id=p_coord) THEN
      SELECT node_id INTO STRICT origin FROM partdist.node_map WHERE hostname=p_host AND port=p_port;
      IF p_gid<>format('shardpg_fc_%s_%s_%s',p_xid,p_coord,split_part(p_gid,'_',5)) THEN
        RAISE EXCEPTION 'invalid DDL branch identifier'; END IF;
      PERFORM partdist.dtx_meta_event(p_coord,p_xid,7,
        ARRAY[origin::bigint,split_part(p_gid,'_',5)::bigint,coalesce(p_native,0),cardinality(p_shards)::bigint]||p_shards);
      RETURN true;
    END IF;
    PERFORM partdist.dtx_enlist(p_xid,p_coord,p_gid,p_host,p_port,p_shards);
    UPDATE partdist.dtx_coord_branch SET native_fullxid=coalesce(native_fullxid,p_native)
      WHERE dtxid=p_xid AND gid=p_gid
        AND (native_fullxid IS NULL OR p_native IS NULL OR native_fullxid=p_native);
    IF NOT FOUND THEN RAISE EXCEPTION 'native branch identity changed'; END IF;
    RETURN true;
END $$;

-- The intent is durable BEFORE contacting a candidate. A lost admission reply
-- pins the candidate until a quorum resolves admission or records rejection.
CREATE TABLE IF NOT EXISTS partdist.dtx_ddl_first_attempt (
 gxid bigint PRIMARY KEY,candidate bigint NOT NULL,connection_id bigint NOT NULL,
 origin_host text NOT NULL,origin_port integer NOT NULL,shards bigint[] NOT NULL,
 native_fullxid bigint,state text NOT NULL CHECK(state IN ('PENDING','ACTIVE','REJECTED','DONE')),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
REVOKE ALL ON partdist.dtx_ddl_first_attempt FROM PUBLIC;

CREATE OR REPLACE FUNCTION partdist.dtx_first_reserve(x bigint,c bigint,conn bigint,h text,p integer,s bigint[],native bigint)
RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE a partdist.dtx_ddl_first_attempt;
BEGIN
 INSERT INTO partdist.dtx_ddl_first_attempt AS old(gxid,candidate,connection_id,origin_host,origin_port,shards,native_fullxid,state)
 VALUES(x,c,conn,h,p,s,native,'PENDING')
 ON CONFLICT(gxid) DO UPDATE SET
 candidate=CASE WHEN old.state='REJECTED' THEN EXCLUDED.candidate ELSE old.candidate END,
 connection_id=CASE WHEN old.state='REJECTED' THEN EXCLUDED.connection_id ELSE old.connection_id END,
 origin_host=CASE WHEN old.state='REJECTED' THEN EXCLUDED.origin_host ELSE old.origin_host END,
 origin_port=CASE WHEN old.state='REJECTED' THEN EXCLUDED.origin_port ELSE old.origin_port END,
 shards=CASE WHEN old.state='REJECTED' THEN EXCLUDED.shards ELSE old.shards END,
 native_fullxid=CASE WHEN old.state='REJECTED' THEN EXCLUDED.native_fullxid ELSE old.native_fullxid END,
 state=CASE WHEN old.state='REJECTED' THEN 'PENDING' ELSE old.state END,updated_at=clock_timestamp()
 RETURNING * INTO a;
 RETURN to_jsonb(a);
END $$;
CREATE OR REPLACE FUNCTION partdist.dtx_first_publish(x bigint,c bigint)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE chosen bigint;
BEGIN
 IF NOT EXISTS(SELECT FROM partdist.dtx_ddl_first_attempt WHERE gxid=x AND candidate=c AND state IN ('PENDING','ACTIVE')) THEN
   RAISE EXCEPTION 'DDL first registration intent is not publishable'; END IF;
 INSERT INTO partdist.dtx_route AS r(gxid,coord_gsid) VALUES(x,c)
 ON CONFLICT(gxid) DO UPDATE SET coord_gsid=r.coord_gsid RETURNING coord_gsid INTO chosen;
 IF chosen<>c THEN RAISE EXCEPTION 'DDL first coordinator changed'; END IF;
 UPDATE partdist.dtx_ddl_first_attempt SET state='ACTIVE',updated_at=clock_timestamp() WHERE gxid=x;
 RETURN chosen;
END $$;
CREATE OR REPLACE FUNCTION partdist.dtx_first_reject(x bigint,c bigint)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
BEGIN
 UPDATE partdist.dtx_ddl_first_attempt SET state='REJECTED',updated_at=clock_timestamp()
 WHERE gxid=x AND candidate=c AND state='PENDING';
 RETURN true;
END $$;
CREATE OR REPLACE FUNCTION partdist.dtx_route_release(x bigint,c bigint)
RETURNS boolean LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
BEGIN
 DELETE FROM partdist.dtx_route WHERE gxid=x AND coord_gsid=c;
 UPDATE partdist.dtx_ddl_first_attempt SET state='DONE',updated_at=clock_timestamp()
 WHERE gxid=x AND candidate=c AND state='ACTIVE';
 RETURN true;
END $$;

CREATE OR REPLACE FUNCTION partdist.dtx_register_first_native(
 p_xid bigint,p_candidate bigint,p_connection bigint,p_host text,p_port integer,p_shards bigint[],p_native bigint)
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE coord bigint;n record;gid text;answer text;a jsonb;old_error text;
BEGIN
 IF p_xid<=0 OR p_candidate<=0 OR p_connection<0 OR cardinality(p_shards)<1 OR NOT p_candidate=ANY(p_shards) THEN
   RAISE EXCEPTION 'invalid first registration'; END IF;
 PERFORM pg_advisory_xact_lock(p_xid);
 SELECT coord_gsid INTO coord FROM partdist.dtx_route WHERE gxid=p_xid;
 IF coord IS NULL THEN
   a:=partdist.dtx_self(format('SELECT partdist.dtx_first_reserve(%s,%s,%s,%L,%s,%L::bigint[],%s)',
     p_xid,p_candidate,p_connection,p_host,p_port,p_shards,coalesce(p_native::text,'NULL')))::jsonb;
   IF a->>'state'='DONE' THEN RAISE EXCEPTION 'DDL transaction registration already completed'; END IF;
   coord:=(a->>'candidate')::bigint;
   SELECT m.hostname,m.port INTO STRICT n FROM partdist.partition_map pm JOIN partdist.node_map m ON m.node_id=pm.primary_node WHERE pm.partition_id::bigint=coord;
   gid:=format('shardpg_fc_%s_%s_%s',p_xid,coord,a->>'connection_id');
   BEGIN
     answer:=partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_enlist_native(%s,%s,%L,%L,%s,%L::bigint[],%s)',
       p_xid,coord,gid,a->>'origin_host',a->>'origin_port',ARRAY(SELECT jsonb_array_elements_text(a->'shards')::bigint),coalesce(a->>'native_fullxid','NULL')));
     IF answer IS DISTINCT FROM 't' THEN RAISE EXCEPTION 'participant admission failed'; END IF;
   EXCEPTION WHEN OTHERS THEN
     GET STACKED DIAGNOSTICS old_error=MESSAGE_TEXT;
     BEGIN
       answer:=partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_meta_event(%s,%s,0,ARRAY[%s,%s]::bigint[])',
         coord,p_xid,a->>'connection_id',coalesce(a->>'native_fullxid','0')));
     EXCEPTION WHEN OTHERS THEN
       RAISE EXCEPTION 'DDL admission outcome is unknown; candidate remains pinned: %',old_error;
     END;
     IF answer IS DISTINCT FROM 't' THEN
       PERFORM partdist.dtx_self(format('SELECT partdist.dtx_first_reject(%s,%s)',p_xid,coord));
       RAISE EXCEPTION 'DDL candidate rejected its first registration: %',old_error;
     END IF;
   END;
   coord:=partdist.dtx_self(format('SELECT partdist.dtx_first_publish(%s,%s)',p_xid,coord))::bigint;
 END IF;
 SELECT m.hostname,m.port INTO STRICT n FROM partdist.partition_map pm JOIN partdist.node_map m ON m.node_id=pm.primary_node WHERE pm.partition_id::bigint=coord;
 gid:=format('shardpg_fc_%s_%s_%s',p_xid,coord,p_connection);
 answer:=partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_enlist_native(%s,%s,%L,%L,%s,%L::bigint[],%s)',
   p_xid,coord,gid,p_host,p_port,p_shards,coalesce(p_native::text,'NULL')));
 IF answer IS DISTINCT FROM 't' THEN RAISE EXCEPTION 'participant admission failed'; END IF;
 RETURN coord;
END $$;

-- Only the TSO/router resolves abandoned first-attempt intents. A quorum
-- rejection fence is required before trying a different candidate.
CREATE OR REPLACE FUNCTION partdist.dtx_resolve_first_attempts()
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE a partdist.dtx_ddl_first_attempt;n record;answer text;status text;count integer:=0;
BEGIN
 IF current_setting('pg_raft.node_id')::integer<>current_setting('pg_raft.coordinator_node_id')::integer THEN RETURN 0; END IF;
 FOR a IN SELECT * FROM partdist.dtx_ddl_first_attempt WHERE state IN ('PENDING','ACTIVE')
   AND updated_at<clock_timestamp()-interval '5 seconds' ORDER BY updated_at LIMIT 16 LOOP
  IF NOT pg_try_advisory_xact_lock(a.gxid) THEN CONTINUE; END IF;
  BEGIN
   SELECT m.hostname,m.port INTO STRICT n FROM partdist.partition_map pm JOIN partdist.node_map m ON m.node_id=pm.primary_node WHERE pm.partition_id::bigint=a.candidate;
   IF a.state='PENDING' THEN
    answer:=partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_meta_event(%s,%s,0,ARRAY[%s,%s]::bigint[])',a.candidate,a.gxid,a.connection_id,coalesce(a.native_fullxid,0)));
    IF answer='t' THEN
      PERFORM partdist.dtx_self(format('SELECT partdist.dtx_first_publish(%s,%s)',a.gxid,a.candidate));
    ELSIF answer='f' THEN
      PERFORM partdist.dtx_self(format('SELECT partdist.dtx_first_reject(%s,%s)',a.gxid,a.candidate));
      CONTINUE;
    ELSE CONTINUE;
    END IF;
   END IF;
   IF a.native_fullxid IS NOT NULL THEN
    answer:=format('shardpg_fc_%s_%s_%s',a.gxid,a.candidate,a.connection_id);
    status:=partdist.dtx_rpc(a.origin_host,a.origin_port,format('SELECT partdist.dtx_branch_probe(%L,%s)',answer,a.native_fullxid));
    IF status='ABORTED' THEN
      PERFORM partdist.dtx_rpc(n.hostname,n.port,format('SELECT partdist.dtx_vote(%s,%L,false)',a.gxid,answer));
    END IF;
   END IF;
   count:=count+1;
  EXCEPTION WHEN OTHERS THEN NULL;
  END;
 END LOOP;
 RETURN count;
END $$;
REVOKE ALL ON FUNCTION partdist.dtx_first_reserve(bigint,bigint,bigint,text,integer,bigint[],bigint),
 partdist.dtx_first_publish(bigint,bigint),partdist.dtx_first_reject(bigint,bigint),
 partdist.dtx_route_release(bigint,bigint),partdist.dtx_resolve_first_attempts() FROM PUBLIC;

CREATE OR REPLACE FUNCTION partdist.dtx_register_first(
    p_xid bigint,p_candidate bigint,p_connection bigint,
    p_host text,p_port integer,p_shards bigint[])
RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
BEGIN
    RETURN partdist.dtx_register_first_native(p_xid,p_candidate,p_connection,p_host,p_port,p_shards,NULL);
END $$;

-- Recover missing votes after an original router or task backend exits.
-- Native PREPARE is the durable vote witness; active or unprovable xids are
-- retained. A native abort permits ABORT, never an override of durable COMMIT.
CREATE OR REPLACE FUNCTION partdist.dtx_recover_preparing()
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE d record; b record; answer text; votes text; count integer:=0;
BEGIN
    FOR d IN SELECT t.dtxid FROM partdist.dtx_coord_tx t
      JOIN partdist.pg_raft_group_status() g ON g.group_id=t.coord_gsid
      WHERE t.phase='PREPARE' AND g.state='leader'
        AND NOT EXISTS(SELECT FROM partdist.dtx_decision x WHERE x.dtxid=t.dtxid)
        AND NOT EXISTS(SELECT FROM partdist.dtx_commit_decided x WHERE x.dtxid=t.dtxid)
        AND NOT EXISTS(SELECT FROM partdist.dtx_ddl_outcome x WHERE x.dtxid=t.dtxid)
      ORDER BY t.dtxid LIMIT 32
    LOOP
      FOR b IN SELECT * FROM partdist.dtx_coord_branch WHERE dtxid=d.dtxid
                 AND vote IS NULL AND native_fullxid IS NOT NULL
      LOOP
        BEGIN
          answer:=partdist.dtx_rpc(b.hostname,b.port,format(
            'SELECT partdist.dtx_branch_probe(%L,%s)',b.gid,b.native_fullxid));
          IF answer IN ('PREPARED','ABORTED') THEN
            PERFORM partdist.dtx_self(format('SELECT partdist.dtx_vote(%s,%L,%s)',
              d.dtxid,b.gid,CASE WHEN answer='PREPARED' THEN 'true' ELSE 'false' END));
          END IF;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
      END LOOP;
      votes:=partdist.dtx_self(format('SELECT partdist.dtx_votes(%s)',d.dtxid));
      IF votes IN ('READY','ABORT') THEN
        BEGIN
          PERFORM partdist.dtx_self(format('SELECT partdist.dtx_coordinate(%s)',d.dtxid));
          count:=count+1;
        EXCEPTION WHEN OTHERS THEN NULL; -- durable ABORT also raises to its caller
        END;
      END IF;
    END LOOP;
    RETURN count;
END $$;

-- Retained DDL receipts are rebuilt from quorum records and survive FORGET.
-- A router resuming after native PREPARE can obtain the same terminal answer
-- even when background recovery completed phase two before it reconnects.
CREATE OR REPLACE FUNCTION partdist.dtx_ddl_finish(p_coord bigint,p_xid bigint)
RETURNS integer LANGUAGE plpgsql SET search_path=pg_catalog,partdist AS $$
DECLARE v integer;
BEGIN
    -- Refresh takes replay-index row locks. Release them before coordinate
    -- opens another connection to seal, vote, or record a decision.
    PERFORM partdist.dtx_self(format('SELECT partdist.dtx_refresh(%s)',p_coord));
    SELECT verdict INTO v FROM partdist.dtx_ddl_outcome WHERE dtxid=p_xid AND coord_gsid=p_coord;
    IF v IS NOT NULL THEN RETURN v; END IF;
    BEGIN
      PERFORM partdist.dtx_coordinate(p_xid);
    EXCEPTION WHEN OTHERS THEN
      PERFORM partdist.dtx_self(format('SELECT partdist.dtx_refresh(%s)',p_coord));
      SELECT verdict INTO v FROM partdist.dtx_ddl_outcome WHERE dtxid=p_xid AND coord_gsid=p_coord;
      IF v IS NULL THEN RAISE; END IF;
      RETURN v;
    END;
    PERFORM partdist.dtx_self(format('SELECT partdist.dtx_refresh(%s)',p_coord));
    SELECT verdict INTO STRICT v FROM partdist.dtx_ddl_outcome WHERE dtxid=p_xid AND coord_gsid=p_coord;
    RETURN v;
END $$;

REVOKE ALL ON FUNCTION partdist.dtx_branch_set_ddl(),
 partdist.dtx_ddl_install(integer,bigint,integer[]),partdist.dtx_ddl_ensure(),
 partdist.dtx_ddl_allocate(integer),
 partdist.dtx_register_first(bigint,bigint,bigint,text,integer,bigint[]),
 partdist.dtx_register_first_native(bigint,bigint,bigint,text,integer,bigint[],bigint),
 partdist.dtx_enlist_native(bigint,bigint,text,text,integer,bigint[],bigint),
 partdist.dtx_branch_probe(text,bigint),partdist.dtx_recover_preparing(),
 partdist.dtx_ddl_finish(bigint,bigint) FROM PUBLIC;
