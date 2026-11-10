from fixture import *
import re
assert state()['moved_back'] and not state().get('shrunk')
b=state()['business'];C=b['customer'];O=b['orders'];snapshot=state()['before_shrink_data']
assert sql(1,'resume_worker_still_registered','SELECT count(*) FROM pg_dist_node WHERE nodeport=%d'%PORTS[4])=='1'
assert sql(1,'resume_worker_still_empty','SELECT count(*) FROM partdist.partition_map WHERE 5=ANY(ARRAY[primary_node]||secondary_nodes)')=='0'
for i in range(1,6):assert sql(i,'resume_no_prepared','SELECT count(*) FROM pg_prepared_xacts')=='0'
sql(1,'demo_citus_remove_node_metadata',"SELECT partdist.dtx_self(format('SET LOCAL shardpg.first_shard_coordinator=off; SELECT pg_catalog.citus_remove_node(%%L,%%s)',nodename,nodeport)) FROM pg_dist_node WHERE nodeport=%d"%PORTS[4])
assert sql(1,'protocol_after_remove','SHOW shardpg.first_shard_coordinator')=='on'
for i in range(1,5):sql(i,'demo_remove_address','DELETE FROM partdist.node_map WHERE node_id=5')
time.sleep(4)
for i in [5,4,3,2,1]:dex([BIN+'/pg_ctl','-D','/work/test/n%d/pgdata'%i,'-m','fast','-w','-t','90','stop'],timeout=100)
checkpoint('shrink_stopped',True)
for i in range(1,5):
 p=ROOT/('data/n%d/pgdata/postgresql.auto.conf'%i);old=p.read_text();(ROOT/('node%d.auto.conf.before-shrink'%i)).write_text(old)
 p.write_text(re.sub(r'^pg_raft\.peers\s*=.*\n?','',old,flags=re.M))
for i in range(1,5):dex([BIN+'/pg_ctl','-D','/work/test/n%d/pgdata'%i,'-l','/work/test/n%d/startup.log'%i,'-w','-t','60','start'],timeout=70)
for _ in range(30):
 v=sql(1,'wait_control_after_shrink','SELECT leader_node_id>0 FROM partdist.pg_raft_group_status() WHERE group_id=0')
 if v=='t':break
 time.sleep(2)
else:raise RuntimeError('control leader not elected after shrink')
for i in range(1,5):
 sql(i,'demo_remove_address_after_restart','DELETE FROM partdist.node_map WHERE node_id=5')
 assert sql(i,'four_peers_after_shrink','SHOW pg_raft.peers')==','.join(str(j)+'@127.0.0.1:'+str(p) for j,p in enumerate(PORTS[:4],1))
assert sql(1,'removed_worker_absent','SELECT count(*) FROM pg_dist_node WHERE nodeport=%d'%PORTS[4])=='0'
assert sql(1,'after_shrink_data',"SELECT md5(string_agg(w||':'||id||':'||balance,',' ORDER BY w,id)) FROM %s; SELECT md5(string_agg(w||':'||id||':'||amount,',' ORDER BY w,id)) FROM %s"%(C,O),timeout=300)==snapshot
sql(1,'commit_after_shrink',"SET statement_timeout='180s'; BEGIN; UPDATE %s SET balance=balance-3 WHERE w=%d; UPDATE %s SET balance=balance+3 WHERE w=%d; COMMIT;"%(C,b['key_a'],C,b['key_b']))
sql(1,'rollback_after_shrink',"BEGIN; UPDATE %s SET balance=balance+900 WHERE w=%d; DELETE FROM %s WHERE w=%d; ROLLBACK;"%(C,b['key_a'],O,b['key_b']))
assert sql(1,'final_balance','SELECT count(*),sum(balance) FROM '+C)=='16|16000'
for i in range(1,5):assert sql(i,'final_no_prepared','SELECT count(*) FROM pg_prepared_xacts')=='0'
checkpoint('shrunk',True);print('FA23_DEMO_SHRINK_AND_BUSINESS_PASS',flush=True)
