from fixture import *
peers=','.join(str(j)+'@127.0.0.1:'+str(p) for j,p in enumerate(PORTS,1))
port=PORTS[4]
for i in range(1,6):
 assert sql(i,'five_peers','SHOW pg_raft.peers')==peers
 sql(i,'control_status','SELECT group_id,state,leader_node_id,current_term,commit_index,last_applied FROM partdist.pg_raft_group_status() WHERE group_id=0')
assert sql(1,'new_worker_registered',"SELECT count(*) FROM pg_dist_node WHERE nodename='127.0.0.1' AND nodeport=%d AND isactive"%port)=='1'
sql(1,'repeat_add_node',"SELECT * FROM shardpg.add_node('127.0.0.1',%d)"%port)
checkpoint('added',True);print('FA23_DEMO_ADD_NODE_PASS',flush=True)
