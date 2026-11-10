from fixture import *
assert state()['wired'];assert not state().get('seeded')
C='fa23_demo_customer';O='fa23_demo_orders'
assert sql(1,'test_names_unused',"SELECT to_regclass('%s') IS NULL AND to_regclass('%s') IS NULL"%(C,O))=='t'
sql(1,'create_customer',"SET statement_timeout='180s'; CREATE TABLE %s(w integer NOT NULL,id integer NOT NULL,balance integer NOT NULL,PRIMARY KEY(w,id),SHARD KEY(w) SHARDS 4 REPLICAS 2)"%C)
sql(1,'create_orders',"SET statement_timeout='180s'; CREATE TABLE %s(w integer NOT NULL,id integer NOT NULL,amount integer NOT NULL,PRIMARY KEY(w,id),SHARD KEY(w) COLOCATE WITH %s)"%(O,C))
sql(1,'seed_customer',"INSERT INTO %s SELECT w,1,1000 FROM generate_series(1,16) w"%C)
sql(1,'seed_orders',"INSERT INTO %s SELECT w,1,100 FROM generate_series(1,16) w"%O)
assert sql(1,'initial_join',"SELECT count(*),sum(c.balance),sum(o.amount) FROM %s c JOIN %s o USING(w,id)"%(C,O))=='16|16000|1600'
routes=sql(1,'routing',"SELECT w,get_shard_id_for_distribution_column('%s',w),m.primary_node FROM generate_series(1,16) w JOIN partdist.partition_map m ON m.partition_id=get_shard_id_for_distribution_column('%s',w) ORDER BY w"%(C,C))
rows=[list(map(int,x.split('|'))) for x in routes.splitlines()];a=rows[0];b=next(x for x in rows if x[2]!=a[2])
checkpoint('business',dict(customer=C,orders=O,key_a=a[0],key_b=b[0],shard=a[1],source=None,routes=rows))
sql(1,'cross_worker_commit',"BEGIN; UPDATE %s SET balance=balance-10 WHERE w=%d; UPDATE %s SET balance=balance+10 WHERE w=%d; COMMIT;"%(C,a[0],C,b[0]))
assert sql(1,'commit_visible',"SELECT sum(balance) FROM %s"%C)=='16000'
sql(1,'cross_worker_rollback',"BEGIN; UPDATE %s SET balance=balance+200 WHERE w=%d; DELETE FROM %s WHERE w=%d; ROLLBACK;"%(C,a[0],O,b[0]))
assert sql(1,'rollback_visible',"SELECT count(*),sum(c.balance),sum(o.amount) FROM %s c JOIN %s o USING(w,id)"%(C,O))=='16|16000|1600'
checkpoint('seeded',True);print('FA23_DEMO_SEED_AND_TRANSACTIONS_PASS',flush=True)
