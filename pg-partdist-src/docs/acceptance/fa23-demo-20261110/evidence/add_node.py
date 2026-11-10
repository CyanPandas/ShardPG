from fixture import *
assert state()['seeded'];assert 5 not in state()['initialized']
i=5;port=PORTS[4];directory='/work/test/n5'
dex(['mkdir','-m','700',directory]);dex([BIN+'/initdb','-D',directory+'/pgdata','-U','postgres','--no-locale','--encoding=UTF8','--auth-local=trust','--auth-host=scram-sha-256','--pwfile=/work/secrets/password'])
# Copy only the private fixture's node configuration, then set the new identity.
dex(['cp','/work/test/n4/pgdata/postgresql.conf',directory+'/pgdata/postgresql.conf'])
dex(['cp','/work/test/n4/pgdata/pg_hba.conf',directory+'/pgdata/pg_hba.conf'])
peers=','.join(str(j)+'@127.0.0.1:'+str(p) for j,p in enumerate(PORTS,1))
conf="\nport=%d\npg_raft.node_id=5\nunix_socket_directories='/work/test/n5, /tmp'\npg_raft.peers='%s'\n"%(port,peers)
dex(['bash','-c','cat >> '+directory+'/pgdata/postgresql.conf'],conf.encode())
dex([BIN+'/pg_ctl','-D',directory+'/pgdata','-l',directory+'/startup.log','-w','-t','45','start'])
s=state();s['initialized'].append(5);save(s)
for ext in ['citus','pg_partdist','pg_raft']:sql(5,'new_extension_'+ext,'CREATE EXTENSION '+ext)
values=','.join("(%d,'127.0.0.1',%d,'active',now())"%(j,p) for j,p in enumerate(PORTS,1))
sql(5,'new_node_address_seed','INSERT INTO partdist.node_map(node_id,hostname,port,status,last_heartbeat) VALUES '+values+' ON CONFLICT(node_id) DO NOTHING')
assert sql(5,'new_node_protocol','SHOW shardpg.first_shard_coordinator')=='on'
sql(1,'demo_add_node',"SET statement_timeout='300s'; SELECT * FROM shardpg.add_node('127.0.0.1',%d)"%port,timeout=330)
for i in range(1,6):
 assert sql(i,'five_peers','SHOW pg_raft.peers')==peers
 sql(i,'control_status','SELECT group_id,state,leader_node_id,current_term,commit_index,last_applied FROM partdist.pg_raft_group_status() WHERE group_id=0')
assert sql(1,'new_worker_registered',"SELECT count(*) FROM pg_dist_node WHERE nodename='127.0.0.1' AND nodeport=%d AND isactive"%port)=='1'
sql(1,'repeat_add_node',"SELECT * FROM shardpg.add_node('127.0.0.1',%d)"%port)
checkpoint('added',True);print('FA23_DEMO_ADD_NODE_PASS',flush=True)
