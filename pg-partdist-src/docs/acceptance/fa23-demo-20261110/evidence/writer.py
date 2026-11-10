from fixture import *
b=state()['business'];C=b['customer'];O=b['orders'];a=b['key_a'];z=b['key_b'];done=0
result={'started':time.time(),'commits':0,'passed':False}
try:
 for j in range(1,121):
  if (ROOT/'writer.stop').exists():break
  sql(1,'writer_commit_%d'%j,"SET statement_timeout='90s'; BEGIN; UPDATE %s SET balance=balance-1 WHERE w=%d AND id=1; UPDATE %s SET balance=balance+1 WHERE w=%d AND id=1; INSERT INTO %s VALUES(%d,%d,1),(%d,%d,1); COMMIT;"%(C,a,C,z,O,a,1000+j,z,1000+j),timeout=120)
  done=j;result['commits']=done;(ROOT/'writer-result.json').write_text(json.dumps(result,indent=2))
  time.sleep(1)
 result['passed']=done>0
except BaseException as e:result['error']=repr(e);raise
finally:
 result['finished']=time.time();(ROOT/'writer-result.json').write_text(json.dumps(result,indent=2))
