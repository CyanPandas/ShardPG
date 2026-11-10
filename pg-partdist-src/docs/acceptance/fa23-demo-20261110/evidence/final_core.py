from fixture import *
assert state()['shrunk'];assert not state().get('core_flow_pass')
residual_routes={}
b=state()['business'];C=b['customer'];O=b['orders']
business_shards=sql(1,'all_business_shards',"SELECT shardid FROM pg_dist_shard WHERE logicalrelid IN ('%s'::regclass,'%s'::regclass) ORDER BY shardid"%(C,O))
ids=[int(x) for x in business_shards.splitlines()];assert len(ids)==8;idlist=','.join(map(str,ids));checkpoint('all_business_shards',ids)
for i in range(1,5):
 assert sql(i,'final_protocol','SHOW shardpg.first_shard_coordinator')=='on'
 assert sql(i,'final_control_size','SELECT cluster_size FROM partdist.pg_raft_group_status() WHERE group_id=0')=='4'
 assert sql(i,'final_removed_node_absent','SELECT count(*) FROM partdist.node_map WHERE node_id=5')=='0'
 for attempt in range(15):
  health=sql(i,'final_transaction_health',"SELECT (SELECT count(*) FROM pg_prepared_xacts),partdist.dtx_pending_count(),(SELECT count(*) FROM partdist.dtx_coord_tx),(SELECT count(*) FROM partdist.dtx_coord_branch),(SELECT count(*) FROM partdist.dtx_route)")
  parts=health.split('|')
  if parts[:4]==['0','0','0','0']:
   residual_routes[str(i)]=int(parts[4]);break
  time.sleep(2)
 else:raise RuntimeError('transaction state not drained at node%d'%i)
# The legacy demo ends by dropping its own tables; keep all failure/test logs.
sql(1,'demo_drop_tables',"SET statement_timeout='180s'; DROP TABLE %s,%s"%(O,C),timeout=210)
for i in range(1,5):
 for attempt in range(20):
  count=sql(i,'drop_business_group_cleanup','SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN (%s)'%idlist)
  if count=='0':break
  time.sleep(2)
 else:raise RuntimeError('business groups remain on node%d'%i)
 assert sql(i,'drop_partition_map_cleanup','SELECT count(*) FROM partdist.partition_map WHERE partition_id IN (%s)'%idlist)=='0'
 assert sql(i,'drop_physical_tables_cleanup',"SELECT count(*) FROM pg_class WHERE relname ~ '^fa23_demo_(customer|orders)_[0-9]+$'")=='0'
 assert sql(i,'drop_no_prepared','SELECT count(*) FROM pg_prepared_xacts')=='0'
# Preserve rather than remove the stopped node5 test data.
assert dex([BIN+'/pg_isready','-h','127.0.0.1','-p',str(PORTS[0])]).decode().strip().endswith('accepting connections')
checkpoint('core_flow_pass',True);checkpoint('complete_cleanup_pass',not any(residual_routes.values()));checkpoint('retained_route_counts',residual_routes);checkpoint('completed_utc',time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
print('FA23_NORMAL_DEMO_CORE_FLOW_PASS',flush=True)
print('RETAINED_ROUTE_COUNTS',json.dumps(residual_routes),flush=True)
