from pathlib import Path
import os,json,subprocess,time,hashlib
ROOT=Path('/home/yyt/yyt/fa23-demo-validation-20261110')
BIN='/work/pg-install/bin'
NAME='yyt-fa23-demo-validation-20261110'
LABEL='yyt.fa23.demo.validation'
PORTS=[28442+i for i in range(5)]
def command(args,data=None,timeout=180):
 r=subprocess.run(args,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
 if r.returncode:raise RuntimeError('command failed '+str(args[:5])+': '+r.stderr.decode(errors='replace')[-4000:])
 return r.stdout

def state():return json.loads((ROOT/'state.json').read_text())
def save(s):
 p=ROOT/'state.tmp';p.write_text(json.dumps(s,indent=2)+'\n');p.replace(ROOT/'state.json')
def inspect():
 s=state();o=json.loads(command(['docker','inspect',s['id']]))[0]
 assert o['Config']['Labels'][LABEL]==ROOT.name
 assert all(not m['RW'] or m['Source']==str(ROOT/'data') for m in o['Mounts'])
 return s

def dex(args,data=None,timeout=180):
 s=inspect();return command(['docker','exec','-i','--user',str(s['uid'])+':'+str(s['gid']),s['id']]+args,data,timeout)
def sql(n,label,q,bootstrap=False,timeout=240,allow_error=False):
 s=inspect();args=['docker','exec','-i','--user',str(s['uid'])+':'+str(s['gid'])]
 if bootstrap:args+=['-e','PGOPTIONS=-c shardpg.first_shard_coordinator=off']
 args += [s['id'],BIN+'/psql','-XqAt','-v','ON_ERROR_STOP=1','-h','/work/test/n'+str(n),'-p',str(PORTS[n-1]),'-U','postgres','-d','postgres']
 start=time.time()
 with (ROOT/'events.jsonl').open('a') as f:f.write(json.dumps(dict(time=start,event='sql_sent',node=n,label=label,sql=q,bootstrap=bootstrap))+'\n')
 r=subprocess.run(args,input=q.encode(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
 e=dict(time=time.time(),event='sql_result',node=n,label=label,returncode=r.returncode,stdout=r.stdout.decode(errors='replace'),stderr=r.stderr.decode(errors='replace'),seconds=round(time.time()-start,3))
 with (ROOT/'events.jsonl').open('a') as f:f.write(json.dumps(e)+'\n')
 print(label,n,r.returncode,e['stdout'][-1600:],e['stderr'][-1600:],flush=True)
 if r.returncode and not allow_error:raise RuntimeError(label+' failed; inspect events.jsonl; do not blindly repeat writes')
 return e if allow_error else e['stdout'].strip()
def checkpoint(key,value):
 s=state();s[key]=value;save(s)
