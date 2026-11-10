from fixture import *
import socket
assert state()['initialized']==[] and not state()['wired']
for port in PORTS:
 s=socket.socket();s.bind(('127.0.0.1',port));s.close()
assert not (ROOT/'data/n1/pgdata/postmaster.pid').exists()
(ROOT/'data/n1').rename(ROOT/'data/n1-port-conflict-preserved')
for i in range(1,5):
 port=PORTS[i-1];directory='/work/test/n'+str(i)
 dex(['mkdir','-m','700',directory]);dex([BIN+'/initdb','-D',directory+'/pgdata','-U','postgres','--no-locale','--encoding=UTF8','--auth-local=trust','--auth-host=scram-sha-256','--pwfile=/work/secrets/password'])
 peers=','.join(str(j)+'@127.0.0.1:'+str(p) for j,p in enumerate(PORTS[:4],1))
 conf={'listen_addresses':"'127.0.0.1'",'port':str(port),'unix_socket_directories':"'/work/test/n%d, /tmp'"%i,'unix_socket_permissions':"'0700'",'shared_preload_libraries':"'citus,pg_partdist,pg_raft'",'shared_buffers':"'32MB'",'max_connections':'100','max_worker_processes':'64','max_prepared_transactions':'100','max_wal_size':"'512MB'",'fsync':'on','synchronous_commit':'on','logging_collector':'on','log_directory':"'log'",'pg_raft.node_id':str(i),'pg_raft.coordinator_node_id':'1','pg_raft.raft_enabled':'on','pg_raft.peers':"'"+peers+"'",'pg_raft.heartbeat_ms':'1000','pg_raft.election_timeout_ms':'6000','pg_partdist.tso_master':'on' if i==1 else 'off','pg_partdist.tso_conninfo':"'host=127.0.0.1 port=%d dbname=postgres user=postgres passfile=/work/secrets/pgpass'"%PORTS[0],'citus.recover_2pc_interval':'-1','citus.node_conninfo':"'sslmode=disable'"}
 dex(['bash','-c','cat >> '+directory+'/pgdata/postgresql.conf'],('\n'+''.join(k+' = '+v+'\n' for k,v in conf.items())).encode())
 dex(['bash','-c','cat > '+directory+'/pgdata/pg_hba.conf'],b'local all all trust\nhost all postgres 127.0.0.1/32 scram-sha-256\n')
 dex([BIN+'/pg_ctl','-D',directory+'/pgdata','-l',directory+'/startup.log','-w','-t','45','start'])
 s=state();s['initialized'].append(i);save(s)
 for ext in ['citus','pg_partdist','pg_raft']:sql(i,'extension_'+ext,'CREATE EXTENSION '+ext)
 values=','.join("(%d,'127.0.0.1',%d,'active',now())"%(j,p) for j,p in enumerate(PORTS[:4],1))
 sql(i,'bootstrap_node_map','INSERT INTO partdist.node_map(node_id,hostname,port,status,last_heartbeat) VALUES '+values)
sql(1,'coordinator_address',"SELECT citus_set_coordinator_host('127.0.0.1',%d)"%PORTS[0])
sql(1,'ddl_metadata','SELECT partdist.dtx_ddl_ensure()')
for p in PORTS[1:4]:sql(1,'bootstrap_worker',"SELECT citus_add_node('127.0.0.1',%d)"%p,bootstrap=True)
checkpoint('wired',True)
for i in range(1,5):assert sql(i,'default_protocol','SHOW shardpg.first_shard_coordinator')=='on'
print('FA23_DEMO_FOUR_NODE_READY',flush=True)
