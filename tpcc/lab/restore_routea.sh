#!/bin/bash
# 恢复干净的路线 A 探底集群：停备库 → 全新部署（单副本快速提交）→ 装载 → 建 6 个同步备库（ANY 1）
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
bash shardpg_rep.sh stop; ssh -n 10.77.110.147 "docker start shardpg-fix-n3 >/dev/null 2>&1; true"
rm -f tpcc/build_db.log; bash redeploy4.sh > /tmp/redeploy4.log 2>&1
for i in $(seq 1 120); do grep -q EXIT= tpcc/build_db.log 2>/dev/null && break; sleep 10; done
grep -E "EXIT" tpcc/build_db.log; sed -n "/一致性基线/,\$p" tpcc/build_db.log | grep -E "^ +[0-9]+$" | tr "\n" " "; echo
bash shardpg_rep.sh standbys 2>&1 | grep -E "up|FAILED"; bash shardpg_rep.sh sync quorum >/dev/null; bash shardpg_rep.sh wait; bash shardpg_rep.sh status
log "恢复完成"
