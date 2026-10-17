#!/bin/bash
# 路线 A 探底一键流程（145 上 nohup 跑）：单副本 ShardPG + 每 worker 两个同步流复制备库，ELR 开/关各扫一遍
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
PSQL="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
log "停备库"; bash shardpg_rep.sh stop
log "重建镜像 + 全新部署"; rm -f tpcc/build_db.log; bash redeploy4.sh > /tmp/redeploy4.log 2>&1
tail -3 /tmp/redeploy4.log
for i in $(seq 1 120); do grep -q EXIT= tpcc/build_db.log 2>/dev/null && break; sleep 10; done
grep -E "^\[|EXIT" tpcc/build_db.log | sed -n "/④/,\$p"; sed -n "/一致性基线/,\$p" tpcc/build_db.log | grep -E "^ +[0-9]+$" | tr "\n" " "; echo
grep -q "EXIT=0" tpcc/build_db.log || { log "建库失败，停"; exit 1; }
log "建备库"; bash shardpg_rep.sh standbys 2>&1 | grep -E "up|FAILED"
bash shardpg_rep.sh sync quorum; bash shardpg_rep.sh wait
for elr in on off; do
  for hp in 10.77.110.148:31080 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do
    h=${hp%:*}; p=${hp#*:}; $PSQL -h $h -p $p -c "alter system set pg_partdist.early_lock_release = $elr" -c "select pg_reload_conf()" >/dev/null
  done
  sleep 2; log "ELR=$elr（$($PSQL -h 10.77.110.145 -p 31082 -c 'show pg_partdist.early_lock_release')）扫描"
  bash scale.sh shardpg 128 192 256 384 512 2>&1 | tail -5
  for d in $(ls -td tpcc/results/*/ | head -5); do grep -h "ERROR" $d/pgbench.err 2>/dev/null | cut -c1-160 | head -1; done
  bash shardpg_rep.sh status
done
log "完成"
