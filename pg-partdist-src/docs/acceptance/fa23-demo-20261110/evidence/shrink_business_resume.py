from fixture import *
assert state()['shrink_stopped'] and not state().get('shrunk')
b=state()['business'];C=b['customer'];O=b['orders'];snapshot=state()['before_shrink_data']
assert sql(1,'after_shrink_data',"SELECT md5(string_agg(w||':'||id||':'||balance,',' ORDER BY w,id)) FROM %s; SELECT md5(string_agg(w||':'||id||':'||amount,',' ORDER BY w,id)) FROM %s"%(C,O),timeout=300)==snapshot
sql(1,'commit_after_shrink',"SET statement_timeout='180s'; BEGIN; UPDATE %s SET balance=balance-3 WHERE w=%d; UPDATE %s SET balance=balance+3 WHERE w=%d; COMMIT;"%(C,b['key_a'],C,b['key_b']))
sql(1,'rollback_after_shrink',"BEGIN; UPDATE %s SET balance=balance+900 WHERE w=%d; DELETE FROM %s WHERE w=%d; ROLLBACK;"%(C,b['key_a'],O,b['key_b']))
assert sql(1,'final_balance','SELECT count(*),sum(balance) FROM '+C)=='16|16000'
for i in range(1,5):assert sql(i,'final_no_prepared','SELECT count(*) FROM pg_prepared_xacts')=='0'
checkpoint('shrunk',True);print('FA23_DEMO_SHRINK_AND_BUSINESS_PASS',flush=True)
