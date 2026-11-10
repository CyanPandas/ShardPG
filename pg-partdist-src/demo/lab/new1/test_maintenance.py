#!/usr/bin/env python3
"""Explicit maintenance regression: create/drop, refuse occupied retirement, remove/rejoin.
Runs on the control host against an already configured demo; no load testing.
"""
import argparse
import json
from pathlib import Path
import runpy
import subprocess
import sys
import time


def check_retirement_replay(c):
    # Apply replay semantics in a rolled-back transaction: a late probe must not
    # resurrect a removed node, while explicit registration must permit rejoin.
    for n in c.nodes:
        c.sql("""BEGIN; SET LOCAL citus.enable_ddl_propagation=off;
-- Bind the C entrypoint's text ABI explicitly; the legacy public SQL wrapper
-- declares jsonb, while the C entrypoint consumes serialized JSON text.
CREATE FUNCTION pg_temp.apply_control_entry(text,text) RETURNS boolean
LANGUAGE c AS '$libdir/pg_raft','pg_raft_apply_payload';
SELECT pg_temp.apply_control_entry('OP_NODE_STATUS','{"node_id":90001,"hostname":"retirement-test","port":59999,"status":"active"}');
SELECT pg_temp.apply_control_entry('OP_NODE_REMOVE','{"node_id":90001}');
SELECT pg_temp.apply_control_entry('OP_NODE_REMOVE','{"node_id":90001}');
SELECT pg_temp.apply_control_entry('OP_NODE_STATUS','{"node_id":90001,"status":"down"}');
DO $$ BEGIN IF EXISTS(SELECT FROM partdist.node_map WHERE node_id=90001) THEN RAISE EXCEPTION 'late probe resurrected retired node'; END IF; END $$;
SELECT pg_temp.apply_control_entry('OP_NODE_STATUS','{"node_id":90001,"hostname":"retirement-test","port":59999,"status":"active"}');
DO $$ BEGIN IF NOT EXISTS(SELECT FROM partdist.node_map WHERE node_id=90001) THEN RAISE EXCEPTION 'explicit rejoin failed'; END IF; END $$;
ROLLBACK;""",name=n)



def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',required=True)
    p.add_argument('--output',required=True)
    p.add_argument('--run',action='store_true')
    p.add_argument('--resume-after-retirement',action='store_true',help='resume only after a completed journaled retirement')
    a=p.parse_args()
    if not a.run:
        print('Use --run during a maintenance window; all databases restart for remove-node.')
        return
    ctl=Path(__file__).with_name('shardpg_ctl')
    Control=runpy.run_path(str(ctl))['Control']
    c=Control(a.config)
    results=json.loads(Path(a.output).read_text(encoding='utf-8')) if a.resume_after_retirement else []
    def passed(case):
        results.append(dict(case=case,status='PASS'))
        Path(a.output).write_text(json.dumps(results,indent=2)+'\n',encoding='utf-8')
        print('PASS '+case,flush=True)
    def command(*args):
        subprocess.run([sys.executable,str(ctl),'--config',a.config]+list(args),check=True)
    table='new1_maintenance_probe'
    assert c.scalar("SELECT to_regclass('public.{}') IS NULL;".format(table))=='t','test table exists'
    if a.resume_after_retirement:
        assert c.nodes['worker4'].get('retired'),'finish journaled retirement before resuming'
    else:
        assert not c.nodes['worker4'].get('retired'),'expected joined worker4'
        # Also exercises cleanup when an older release has already emptied the drop queue.
        c.sql('SELECT shardpg.cleanup_dropped();')
        for n in c.nodes:
            assert c.scalar('SELECT count(*) FROM partdist.shard_identity WHERE NOT EXISTS(SELECT FROM pg_class WHERE oid=local_oid);',name=n)=='0'
        before=c.scalar('SELECT count(*) FROM partdist.partition_map m JOIN partdist.dtx_ddl_shard d ON d.group_id=m.partition_id::bigint;')
        c.sql('SELECT * FROM partdist.residue_gc();')
        assert c.scalar('SELECT count(*) FROM partdist.partition_map m JOIN partdist.dtx_ddl_shard d ON d.group_id=m.partition_id::bigint;')==before
        passed('historical absent identities cleaned; DDL groups protected from residue GC')
    def probe(active,refusal=False):
        c.sql('CREATE TABLE '+table+'(k int PRIMARY KEY,v int NOT NULL,SHARD KEY(k) SHARDS 4 REPLICAS 2);')
        sids=json.loads(c.scalar("SELECT json_agg(shardid) FROM pg_dist_shard WHERE logicalrelid='{}'::regclass;".format(table)))
        ids=','.join(str(x) for x in sids)
        c.sql('INSERT INTO '+table+' SELECT i,0 FROM generate_series(1,32) i;')
        c.sql('BEGIN; UPDATE '+table+' SET v=10; ROLLBACK;')
        assert c.scalar('SELECT count(*)=32 AND sum(v)=0 FROM '+table+';')=='t'
        c.sql('BEGIN; UPDATE '+table+' SET v=1; COMMIT;')
        assert c.scalar('SELECT count(*)=32 AND sum(v)=32 FROM '+table+';')=='t'
        if refusal:
            r=subprocess.run([sys.executable,str(ctl),'--config',a.config,'remove-node','worker4'],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            assert r.returncode!=0 and b'still owns business replicas' in r.stderr,r.stderr.decode()
            passed('occupied worker retirement refused')
        c.sql('DROP TABLE '+table+';')
        for n in active:
            assert c.scalar('SELECT count(*) FROM partdist.shard_identity WHERE global_shard_id IN ('+ids+');',name=n)=='0'
            assert c.scalar('SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN ('+ids+');',name=n)=='0'
            assert c.scalar("SELECT count(*) FROM pg_ls_dir('.') f WHERE f IN (SELECT 'pg_raft_hardstate.'||s FROM unnest(ARRAY["+ids+"]) s);",name=n)=='0'
            c.wait_for('probe transactions drain '+n,lambda n=n:c.scalar('SELECT (SELECT count(*) FROM pg_prepared_xacts)+partdist.dtx_pending_count()+(SELECT count(*) FROM partdist.dtx_coord_tx)+(SELECT count(*) FROM partdist.dtx_coord_branch)+(SELECT count(*) FROM partdist.dtx_route);',name=n)=='0')
    if not a.resume_after_retirement:
        probe(list(c.nodes),True)
        passed('five-node create cross-shard commit rollback drop and identity cleanup')
        command('remove-node','worker4')
    command('remove-node','worker4')
    c=Control(a.config)
    active=[n for n in c.nodes if not c.nodes[n].get('retired')]
    assert len(active)==4
    for n in active:
        assert c.scalar('SELECT count(*) FROM partdist.node_map WHERE node_id=5;',name=n)=='0'
        assert c.scalar('SELECT count(*) FROM partdist.raft_group WHERE group_id>0 AND 5=ANY(members);',name=n)=='0'
    passed('worker4 removed with DDL groups retained and repeat removal idempotent')
    probe(active)
    passed('four-node create commit rollback drop after retirement')
    # Prove native catalog PREPARE and first-shard RPC do not require loopback TCP.
    command('start','worker4')
    original_listen=c.scalar('SHOW listen_addresses;',name='worker4')
    c.sql("ALTER SYSTEM SET listen_addresses=:'host';",{'host':c.nodes['worker4']['host']},name='worker4')
    command('stop','worker4','fast'); command('start','worker4')
    assert c.scalar('SHOW listen_addresses;',name='worker4')==c.nodes['worker4']['host']
    command('add-node','worker4',c.nodes['worker4']['host'],str(c.nodes['worker4']['port']))
    c=Control(a.config)
    probe(list(c.nodes))
    passed('worker4 rejoined and five-node create commit rollback drop')
    assert c.scalar("SELECT partdist.dtx_self($q$SELECT current_setting('pg_raft.node_id')$q$);",name='worker4')=='5'
    passed('local RPC and native catalog PREPARE work without loopback TCP')
    c.sql("ALTER SYSTEM SET listen_addresses=:'addresses';",{'addresses':original_listen},name='worker4')
    command('stop','worker4','fast'); command('start','worker4')
    c.wait_for('worker4 local RPC after restore',lambda:c.scalar("SELECT partdist.dtx_self($q$SELECT current_setting('pg_raft.node_id')$q$);",name='worker4')=='5')
    for n in c.nodes:
        assert c.scalar('SELECT count(*) FROM partdist.node_map;',name=n)=='5'
        assert c.scalar('SELECT count(*) FROM partdist.raft_group r JOIN partdist.dtx_ddl_shard d USING(group_id) WHERE NOT 5=ANY(r.members);',name=n)=='0'
    passed('five-node topology and DDL memberships restored')
    check_retirement_replay(c)
    passed('retirement replay idempotent; late probes ignored; explicit rejoin permitted')


if __name__=='__main__':
    main()
