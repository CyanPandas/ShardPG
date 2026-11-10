from fixture import *
assert state()['core_flow_pass']
result={'time':time.time(),'nodes':{},'crash_markers':{}}
for i in range(1,5):
 result['nodes'][str(i)]=sql(i,'post_drop_final_health',"SELECT (SELECT count(*) FROM pg_prepared_xacts),partdist.dtx_pending_count(),(SELECT count(*) FROM partdist.dtx_coord_tx),(SELECT count(*) FROM partdist.dtx_coord_branch),(SELECT count(*) FROM partdist.dtx_route); SELECT gxid,coord_gsid FROM partdist.dtx_route ORDER BY gxid;")
 hits=0
 for p in (ROOT/('data/n%d/pgdata/log'%i)).glob('*.log'):
  for line in p.open(errors='replace'):
   if ' PANIC:' in line or 'terminated by signal 11' in line or 'terminated by signal 6' in line:hits+=1
 result['crash_markers'][str(i)]=hits
assert not any(result['crash_markers'].values()),result['crash_markers']
(ROOT/'postcheck-result.json').write_text(json.dumps(result,indent=2)+'\n')
print('POSTCHECK_COMPLETE',flush=True)
