#!/bin/bash
# 用指定镜像全新部署路线 A 实验室集群（不从源码重编，其余同 restore_routea.sh）：
#   三台机器的 shardpg:prof 改指向该镜像（shardpg_rep.sh 等脚本写死了这个名字）→ 停备库 → 4 节点全新 initdb →
#   接线 → 装载 64 迷你仓 → 6 个同步备库（ANY 1）。
# 用法（145 上）：redeploy_img.sh shardpg:fixb | shardpg:prof-pre-fixb
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
IMG=${1:?用法：redeploy_img.sh <镜像>}
for h in localhost 10.77.110.147 10.77.110.148; do
  if [ $h = localhost ]; then c="bash -c"; else c="ssh -n $h"; fi
  $c "docker image inspect $IMG >/dev/null && docker tag $IMG shardpg:prof" || { echo "$h 上没有 $IMG"; exit 1; }
done
log "三台的 shardpg:prof → $IMG（pg_raft.so $(docker run --rm shardpg:prof md5sum /work/pg-install/lib/postgresql/pg_raft.so | cut -c1-12)）"
bash shardpg_rep.sh stop >/dev/null 2>&1; ssh -n 10.77.110.147 "docker start shardpg-fix-n3 >/dev/null 2>&1; true"
docker rm -f shardpg-fix-client shardpg-fix-n1 >/dev/null 2>&1; rm -f tpcc/build_db.log
ssh -n 10.77.110.147 "docker rm -f shardpg-fix-n1 >/dev/null 2>&1; true"
IMG=shardpg:prof ./node.sh 2 31082 tpcc/conf/out &
ssh -n 10.77.110.147 "cd $T && IMG=shardpg:prof ./node.sh 3 31081 conf" &
ssh -n 10.77.110.148 "cd $T && (IMG=shardpg:prof ./node.sh 1 31080 conf & IMG=shardpg:prof ./node.sh 4 31083 conf & wait)" &
wait
docker run -d --name shardpg-fix-client --network host -v $PWD/tpcc:/t shardpg:prof sleep infinity >/dev/null
docker exec shardpg-fix-client bash /t/scripts/wire_cluster.sh 2>&1 | tail -2
log "装载（64 迷你仓）"
docker exec shardpg-fix-client bash -c 'bash /t/scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
grep -E "EXIT" tpcc/build_db.log; sed -n "/一致性基线/,\$p" tpcc/build_db.log | grep -E "^ +[0-9]+$" | tr "\n" " "; echo
grep -q "EXIT=0" tpcc/build_db.log || { log "装载失败"; tail -20 tpcc/build_db.log; exit 1; }
bash shardpg_rep.sh standbys 2>&1 | grep -E "up|FAILED"; bash shardpg_rep.sh sync quorum >/dev/null; bash shardpg_rep.sh wait; bash shardpg_rep.sh status
log "部署完成（$IMG）"
