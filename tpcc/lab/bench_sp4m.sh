#!/bin/bash
# 4 台机器（协调者单独在 144 + 3 个 worker，每个 worker 一主两备 ANY 1）的 ShardPG TPC-C，口径同 perf3_run.sh。
# 先等 bench_pg_vs_sp.sh 跑完（ALL_DONE），不与它同时压测。用法（145 上，nohup 后台）：bench_sp4m.sh
R=/data1/yyt/shardpg-fix-20261014; cd $R; L=$R/tsoraft_logs
log() { echo "[$(date +%T)] $*"; }
until grep -q ALL_DONE $L/bench_pg_vs_sp.out 2>/dev/null; do sleep 20; done
log "== ShardPG 一主两备（4 台机器：协调者 144 + 3 worker）：全新部署"
bash redeploy4m.sh shardpg:tsoraft > $L/deploy-sp4m.log 2>&1; tail -1 $L/deploy-sp4m.log
grep -q "部署完成" $L/deploy-sp4m.log || { log "部署失败，见 $L/deploy-sp4m.log"; exit 1; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
for i in $(seq 120); do $CLI -h 10.77.110.144 -p 31080 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && break; sleep 1; done
NODES="10.77.110.144:31080 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083" bash tsoraft_status.sh
log "== ShardPG 4 台机器性能"; bash perf3_run.sh shardpg sp4m
log "== SP4M_DONE"
