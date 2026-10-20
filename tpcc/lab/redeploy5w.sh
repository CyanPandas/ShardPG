#!/bin/bash
# 4 个 worker 的路线 A 实验室集群（2026-10-20）：协调者 n1（148:31080）+ worker n2（145:31082）/ n3（147:31081）/
#   n4（148:31083）/ n5（147:31084）；每个 worker 一主两备 ANY 1（n5 的备库在 145 / 148 的 31098）。
# 64 迷你仓，8 个分片（每个 worker 2 个），TSO 随控制面 leader（TSO_RAFT=1）。
# 用法（145 上）：redeploy5w.sh [镜像=shardpg:tsoraft]
# 4 节点的 cluster.env / conf/out 先备份为 cluster.env.4node / conf/out.4node_tsoraft（只备份一次），恢复见文件末尾注释。
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
IMG=${1:-shardpg:tsoraft}
export EXTRA_WORKERS="n5:10.77.110.147:31084:shardpg-fix-n5:31098"
[ -f tpcc/cluster.env.4node ] || cp tpcc/cluster.env tpcc/cluster.env.4node
[ -d tpcc/conf/out.4node_tsoraft ] || cp -r tpcc/conf/out tpcc/conf/out.4node_tsoraft
sed -e 's/^NODE_IDS=.*/NODE_IDS=(1 2 3 4 5)/' \
    -e 's/^NODE_HOSTS=.*/NODE_HOSTS=(10.77.110.148 10.77.110.145 10.77.110.147 10.77.110.148 10.77.110.147)/' \
    -e 's/^NODE_PORTS=.*/NODE_PORTS=(31080 31082 31081 31083 31084)/' \
    -e 's/^NODE_ROLES=.*/NODE_ROLES=(coordinator worker worker worker worker)/' \
    -e 's/^SHARD_COUNT=.*/SHARD_COUNT=8/' tpcc/cluster.env.4node > tpcc/cluster.env
TSO_RAFT=1 bash tpcc/scripts/gen_node_conf.sh > /dev/null || { log "生成配置失败"; exit 1; }
scp -q tpcc/conf/out/node3.conf tpcc/conf/out/node5.conf 10.77.110.147:$T/conf/
scp -q tpcc/conf/out/node1.conf tpcc/conf/out/node4.conf 10.77.110.148:$T/conf/
log "配置：5 个节点、8 个分片；$(grep -c '^pg_partdist.tso_raft = on' tpcc/conf/out/node*.conf | tr '\n' ' ')"
for h in localhost 10.77.110.147 10.77.110.148; do
  if [ $h = localhost ]; then c="bash -c"; else c="ssh -n $h"; fi
  $c "docker image inspect $IMG >/dev/null && docker tag $IMG shardpg:prof" || { echo "$h 上没有 $IMG"; exit 1; }
done
log "三台的 shardpg:prof → $IMG"
bash shardpg_rep.sh stop > /dev/null 2>&1
docker rm -f shardpg-fix-client shardpg-fix-n1 > /dev/null 2>&1; rm -f tpcc/build_db.log
ssh -n 10.77.110.147 "docker rm -f shardpg-fix-n1 shardpg-fix-n5 > /dev/null 2>&1; true"
IMG=shardpg:prof ./node.sh 2 31082 tpcc/conf/out &
ssh -n 10.77.110.147 "cd $T && (IMG=shardpg:prof ./node.sh 3 31081 conf & IMG=shardpg:prof ./node.sh 5 31084 conf & wait)" &
ssh -n 10.77.110.148 "cd $T && (IMG=shardpg:prof ./node.sh 1 31080 conf & IMG=shardpg:prof ./node.sh 4 31083 conf & wait)" &
wait
docker run -d --name shardpg-fix-client --network host -v $PWD/tpcc:/t shardpg:prof sleep infinity > /dev/null
docker exec shardpg-fix-client bash /t/scripts/wire_cluster.sh 2>&1 | tail -2
log "装载（64 迷你仓，8 个分片）"
docker exec shardpg-fix-client bash -c 'bash /t/scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
grep -E "EXIT" tpcc/build_db.log
grep -q "EXIT=0" tpcc/build_db.log || { log "装载失败"; tail -20 tpcc/build_db.log; exit 1; }
bash shardpg_rep.sh standbys 2>&1 | grep -E " up|FAILED"; bash shardpg_rep.sh sync quorum > /dev/null; bash shardpg_rep.sh wait; bash shardpg_rep.sh status
log "部署完成（4 worker，$IMG）"
# 恢复 4 节点：cp tpcc/cluster.env.4node tpcc/cluster.env; rm -rf tpcc/conf/out; cp -r tpcc/conf/out.4node_tsoraft tpcc/conf/out；
#   再把 node3 / node1 / node4.conf 拷回 147 / 148 的 $T/conf，跑 redeploy_img.sh
