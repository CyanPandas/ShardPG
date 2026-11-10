from fixture import *
assert 5 in state()['initialized'] and not state().get('added')
for i in range(1,6):assert sql(i,'resume_no_prepared','SELECT count(*) FROM pg_prepared_xacts')=='0'
assert sql(1,'resume_not_in_citus','SELECT count(*) FROM pg_dist_node WHERE nodeport=%d'%PORTS[4])=='0'
sql(1,'install_add_node_metadata_fix','SET citus.enable_ddl_propagation=off;\n'+(ROOT/'demo_node_registration.sql').read_text())
assert sql(1,'protocol_before_add','SHOW shardpg.first_shard_coordinator')=='on'
sql(1,'demo_add_node_resumed',"SET statement_timeout='300s'; SELECT * FROM shardpg.add_node('127.0.0.1',%d); SHOW shardpg.first_shard_coordinator;"%PORTS[4],timeout=330)
exec((ROOT/'add_check.py').read_text())
