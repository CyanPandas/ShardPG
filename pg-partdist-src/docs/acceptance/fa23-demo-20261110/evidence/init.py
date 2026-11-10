from fixture import *
import socket,shutil
BASE=Path('/home/yyt/yyt/shardpg-ddl-first-20261109T024500Z')
assert (BASE/'.deployment-owner').read_text().strip()==BASE.name
proof=json.loads((BASE/'build-r4-result.json').read_text());assert proof['complete'] and proof['baseline']=='8e471bb'
assert not (ROOT/'state.json').exists()
assert not command(['docker','ps','-aq','--filter','name=^'+NAME+'$']).strip()
for p in PORTS:
 so=socket.socket();so.bind(('127.0.0.1',p));so.close()
for d in ['data','secrets']:(ROOT/d).mkdir(mode=0o700)
command(['cp','-a','--reflink=auto',str(BASE/'pg-install'),str(ROOT/'pg-install')],timeout=300)
for f in ['password','pgpass']:
 shutil.copyfile(str(BASE/'secrets'/f),str(ROOT/'secrets'/f));(ROOT/'secrets'/f).chmod(0o600)
sha={f:hashlib.sha256((ROOT/'pg-install/lib/postgresql'/f).read_bytes()).hexdigest() for f in proof['sha256']}
assert sha==proof['sha256'];(ROOT/'build-proof.json').write_text(json.dumps(proof,indent=2)+'\n')
uid=os.getuid();gid=os.getgid()
args=['docker','create','--init','--network','host','--cpus','4','--memory','5g','--shm-size','512m','--ulimit','core=0','--name',NAME,'--user','0','--label',LABEL+'='+ROOT.name,'-e','LD_LIBRARY_PATH=/work/pg-install/lib','-e','PGPASSFILE=/work/secrets/pgpass','--mount','type=bind,src='+str(ROOT/'pg-install')+',dst=/work/pg-install,readonly','--mount','type=bind,src='+str(ROOT/'data')+',dst=/work/test','--mount','type=bind,src='+str(ROOT/'secrets')+',dst=/work/secrets,readonly','--entrypoint','/bin/bash','sha256:46cc50ca101783d3f3f27babe9d3f99a8caa908c42a0c94fd3fc753fb99e4d29','-ceu','getent group "$2" >/dev/null || groupadd -g "$2" demotest; usermod -o -u "$1" -g "$2" postgres; exec sleep infinity','bash',str(uid),str(gid)]
cid=command(args).decode().strip();save(dict(id=cid,name=NAME,uid=uid,gid=gid,initialized=[],wired=False,baseline='8e471bb+DDL-R4',sha256=sha));command(['docker','start',cid])
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
