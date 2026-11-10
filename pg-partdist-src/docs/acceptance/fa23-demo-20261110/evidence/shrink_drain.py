from fixture import *
events=[json.loads(x) for x in (ROOT/'events.jsonl').read_text().splitlines()]
for label in ['after_shrink_data','commit_after_shrink','rollback_after_shrink','final_balance']:
 rows=[x for x in events if x.get('event')=='sql_result' and x.get('label')==label]
 assert rows and rows[-1]['returncode']==0,label
for i in range(1,5):
 for _ in range(30):
  n=sql(i,'wait_async_phase_two','SELECT count(*) FROM pg_prepared_xacts')
  if n=='0':break
  time.sleep(2)
 else:raise RuntimeError('unresolved prepared transactions on node%d'%i)
checkpoint('shrunk',True);print('FA23_DEMO_SHRINK_AND_BUSINESS_PASS',flush=True)
exec((ROOT/'final_check.py').read_text())
