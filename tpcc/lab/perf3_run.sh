#!/bin/bash
# 三套系统同一口径的 TPC-C（64 迷你仓、同一套事务函数、RC、shared_buffers 8GB、每个 worker / 单机一主两备 ANY 1）：
#   热身 128 终端 1 min（不计）→ 128 / 192 / 256 / 384 终端各 1 min，跑两遍。每轮前后都做一致性检查（run.sh 默认）。
# 用法（145 上，对应集群已部署、已装载、备库已追平）：perf3_run.sh <shardpg|citus3r|native> <标签>
R=/data1/yyt/shardpg-fix-20261014; cd $R
SYS=${1:?系统}; TAG=${2:?标签}; OUT=$R/tsoraft_logs/perf-$TAG.txt
log() { echo "[$(date +%T)] $*" | tee -a $OUT; }
log "[$TAG] 热身：$(bash scale.sh $SYS 128 2>&1 | tail -1)"
for rnd in 1 2; do
  for t in 128 192 256 384; do log "[$TAG] 第 $rnd 遍 $(bash scale.sh $SYS $t 2>&1 | tail -1)"; done
done
log "[$TAG] 完成"
