#!/bin/bash
# 2026-10-19 全套实验（145 上，nohup 后台跑；日志在 tsoraft_logs/）：
#   1. 新版镜像（TSO 随控制面 leader + 分片 xid 水位进 WAL）→ 全新部署 → 性能
#   2. 旧版 shardpg:fixb（协调者单点发号）→ 全新部署 → 性能 → 协调者宕机对照 → 分片 xid 对照
#   3. 三副本 Citus 全新部署装载 → 性能；4. 三副本单机 PG 重新装载 → 性能
# 性能口径见 perf3_run.sh（64 迷你仓、热身 + 128/192/256/384 终端各 1 min × 2 遍，每个 worker / 单机一主两备 ANY 1）。
R=/data1/yyt/shardpg-fix-20261014; cd $R; L=$R/tsoraft_logs; mkdir -p $L
log() { echo "[$(date +%T)] $*"; }
step() { log "== $*"; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
wait_tso_on_coord() { for i in $(seq 120); do $CLI -h 10.77.110.148 -p 31080 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && return 0; sleep 1; done; return 1; }

step "新版镜像"; bash tsoraft_image.sh 2>&1 | tail -3
step "新版全新部署"; bash tsoraft_conf.sh on | head -1
bash redeploy_img.sh shardpg:tsoraft > $L/deploy-new.log 2>&1; tail -1 $L/deploy-new.log
wait_tso_on_coord && log "发号者在协调者上" || log "120 s 内发号者没落到协调者"; bash tsoraft_status.sh
step "新版性能"; bash perf3_run.sh shardpg new

step "旧版全新部署"; bash tsoraft_conf.sh off | head -1
bash redeploy_img.sh shardpg:fixb > $L/deploy-old.log 2>&1; tail -1 $L/deploy-old.log
step "旧版性能"; bash perf3_run.sh shardpg old
step "旧版协调者宕机对照"; DUR=90 bash tsoraft_coordkill.sh old1 > $L/ck-old1.out 2>&1; grep -E '^\[' $L/ck-old1.out
step "旧版分片 xid 对照"; bash tsoraft_sxid.sh old1 > $L/sx-old1.out 2>&1; grep -E '^\[' $L/sx-old1.out

step "三副本 Citus：全新部署 + 装载"
bash citus_rep.sh stop >/dev/null 2>&1; bash deploy_citus3r.sh 2>&1 | tail -2
docker exec citus3r-client bash -c 'cd /t && bash scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
log "Citus 装载 $(tail -1 tpcc-citus3r/build_db.log)"
bash citus_rep.sh standbys 2>&1 | grep -c " up" | sed 's/^/备库起来 /'; bash citus_rep.sh sync quorum >/dev/null; bash citus_rep.sh wait; bash citus_rep.sh status 2>&1 | tail -6
step "Citus 性能"; bash perf3_run.sh citus3r citus

step "三副本单机 PG：重新装载"
bash natrep.sh sync '' >/dev/null
docker exec pg-native-client bash -c 'cd /t && bash scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
log "单机装载 $(tail -1 tpcc-native/build_db.log)"
bash natrep.sh standbys 2>&1 | tail -3; bash natrep.sh wait; bash natrep.sh sync 'ANY 1 (s147, s148)' 2>&1 | tail -3
step "单机 PG 性能"; bash perf3_run.sh native native
step "ALL_DONE"
