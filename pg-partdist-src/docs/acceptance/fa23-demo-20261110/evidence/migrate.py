from fixture import *
assert state()['added'];assert not state().get('migrated')
b=state()['business'];C=b['customer'];O=b['orders'];sid=b['shard'];shard=C+'_'+str(sid)
source=int(sql(1,'pick_source_replica','SELECT secondary_nodes[1] FROM partdist.partition_map WHERE partition_id=%d'%sid));assert source in [2,3,4]
b['source']=source;checkpoint('business',b)
bucket=json.loads(sql(1,'bucket_before',"SELECT row_to_json(x) FROM partdist.colocated_bucket('%s') x"%shard));checkpoint('bucket',bucket)
with (ROOT/'writer.log').open('xb') as f:
 writer=subprocess.Popen(['python3','-u',str(ROOT/'writer.py')],cwd=str(ROOT),stdout=f,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)
checkpoint('writer_pid',writer.pid)
for _ in range(90):
 p=ROOT/'writer-result.json'
 if p.exists() and json.loads(p.read_text())['commits']>=2:break
 if writer.poll() is not None:raise RuntimeError('writer ended before migration')
 time.sleep(1)
else:raise RuntimeError('writer not ready')
try:
 sql(1,'demo_move_replica',"SET statement_timeout='900s'; SELECT * FROM shardpg.move_replica('%s',%d,%d)"%(shard,PORTS[source-1],PORTS[4]),timeout=930)
 checkpoint('migration_returned',True)
finally:
 (ROOT/'writer.stop').write_text('stop after current transaction\n')
 try:writer.wait(timeout=130)
 except subprocess.TimeoutExpired:raise RuntimeError('writer still running; inspect before proceeding')
wr=json.loads((ROOT/'writer-result.json').read_text());assert wr['passed'] and writer.returncode==0,wr
for s in bucket['sids']:
 assert sql(1,'migration_membership_%s'%s,"SELECT NOT %d=ANY(ARRAY[primary_node]||secondary_nodes) AND 5=ANY(ARRAY[primary_node]||secondary_nodes) FROM partdist.partition_map WHERE partition_id=%d"%(source,s))=='t'
 assert sql(source,'source_group_removed_%s'%s,'SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=%d'%s)=='0'
 name=next(n for n in bucket['names'] if n.endswith('_'+str(s)))
 assert sql(source,'source_table_removed_%s'%s,"SELECT to_regclass('public.%s') IS NULL"%name)=='t'
assert sql(1,'balance_after_migration','SELECT count(*),sum(balance) FROM '+C)=='16|16000'
assert sql(1,'orders_after_migration','SELECT count(*),sum(amount) FROM '+O)=='%d|%d'%(16+wr['commits']*2,1600+wr['commits']*2)
sql(1,'demo_move_primary',"SET statement_timeout='240s'; SELECT * FROM shardpg.move_primary('%s',%d)"%(shard,PORTS[4]),timeout=270)
for s in bucket['sids']:assert sql(1,'new_primary_%s'%s,'SELECT primary_node FROM partdist.partition_map WHERE partition_id=%d'%s)=='5'
assert sql(1,'join_on_new_primary',"SELECT count(*) FROM %s c JOIN %s o ON c.w=o.w WHERE c.w=%d"%(C,O,b['key_a']))==str(1+wr['commits'])
sql(1,'commit_on_new_primary',"BEGIN; UPDATE %s SET balance=balance-7 WHERE w=%d; UPDATE %s SET balance=balance+7 WHERE w=%d; COMMIT;"%(C,b['key_a'],C,b['key_b']))
sql(1,'rollback_on_new_primary',"BEGIN; UPDATE %s SET balance=balance+500 WHERE w=%d; DELETE FROM %s WHERE w=%d; ROLLBACK;"%(C,b['key_a'],O,b['key_b']))
assert sql(1,'post_primary_balances','SELECT count(*),sum(balance) FROM '+C)=='16|16000'
checkpoint('migrated',True);print('FA23_DEMO_MIGRATION_WRITES_AND_TRANSFER_PASS',flush=True)
