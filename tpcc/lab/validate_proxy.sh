#!/bin/bash
# TSO 代理（2026-10-20）验证：4 台机器部署（协调者 144）+ 64 迷你仓，不限内存
#   1 全新部署（新镜像）2 128/256/384/512 终端各 1 min，压测中采样协调者连接数 3 负载中 kill 协调者（负载从 n2 进）
R=/data1/yyt/shardpg-fix-20261014; cd $R; L=$R/tsoraft_logs
log() { echo "[$(date +%T)] $*"; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
log "== 1 全新部署（4 台机器，64 迷你仓，TSO 代理）"
KIT=tpcc bash redeploy4m.sh shardpg:tsoraft > $L/deploy-vp.log 2>&1; tail -1 $L/deploy-vp.log
grep -q "部署完成" $L/deploy-vp.log || { log "部署失败"; exit 1; }
for i in $(seq 120); do $CLI -h 10.77.110.144 -p 31080 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && break; sleep 1; done
# 代理进程不连本地库（只 BGWORKER_SHMEM_ACCESS），pg_stat_activity 里看不到，按进程名数
for hn in 10.77.110.144:n1 10.77.110.145:n2 10.77.110.147:n3 10.77.110.148:n4; do
  h=${hn%:*}; if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi
  log "  $h TSO 代理进程：$($c "docker top shardpg-fix-${hn#*:} | grep -c 'pg_partdist tso proxy'")"
done
log "== 2 压测（每档 1 min），压测中第 40 s 采样协调者上的连接"
for t in 128 256 384 512; do
  ( sleep 40; $CLI -h 10.77.110.144 -p 31080 -c "select count(*) || ' 个连接（发号连接 ' || count(*) filter (where query like '%partdist_tso_%') || '，客户端 ' || count(*) filter (where client_addr = '10.77.110.145' and application_name = 'pgbench') || '）' from pg_stat_activity where backend_type = 'client backend'" > $L/vp-conn-$t.txt 2>&1 ) &
  r=$(bash scale.sh shardpg $t 2>&1 | tail -1); wait
  log "  $r；协调者上 $(cat $L/vp-conn-$t.txt)"
done
log "  协调者日志里 too many clients：$(ssh -n 10.77.110.144 "docker exec shardpg-fix-n1 bash -c \"cat /d/pgdata/log/*.log\"" | grep -c 'too many clients')"
log "== 3 负载中 kill 协调者（负载从 n2 进）"
COORD_HOST=10.77.110.144 DUR=90 bash tsoraft_coordkill.sh vp > $L/ck-vp.out 2>&1
grep -E '^\[' $L/ck-vp.out
log "VP_DONE"
