#!/bin/bash
# 4 台机器的路线 A 实验室集群（2026-10-20）：协调者 n1 单独放在 144（10.77.110.144:31080），
# worker n2（145:31082）/ n3（147:31081）/ n4（148:31083），每个 worker 一主两备 ANY 1（备库在 145/147/148 之间交叉放）。
# TSO 随控制面 leader（TSO_RAFT=1）。压测客户端在 145。
# 用法（145 上）：[KIT=tpcc] [MEM_LIMIT=8g] redeploy4m.sh [镜像=shardpg:tsoraft]
#   KIT       套件目录（默认 tpcc = 64 迷你仓；tpcc-std = 标准规模，cluster.env 由调用方写好）
#   MEM_LIMIT 每个数据库容器（协调者、worker、备库）的内存上限，并关 swap —— 模拟内存这么大的服务器
# 144 是共享机器：只新增 shardpg-fix-n1 容器与 /home/yyt/shardpg-fix-20261014 目录，不碰别的。
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
IMG=${1:-shardpg:tsoraft}
KIT=${KIT:-tpcc}
export MEM_LIMIT=${MEM_LIMIT:-}
C4=10.77.110.144
if [ -f $KIT/cluster.env.4node ]; then     # 迷你套件：由 3 worker 原件改出 4 台机器版本
  sed -e "s/^NODE_HOSTS=.*/NODE_HOSTS=($C4 10.77.110.145 10.77.110.147 10.77.110.148)/" \
      -e "s/^COORD_HOST=.*/COORD_HOST=$C4/" $KIT/cluster.env.4node > $KIT/cluster.env
fi
grep -q "^COORD_HOST=$C4" $KIT/cluster.env || { log "$KIT/cluster.env 的协调者不是 $C4"; exit 1; }
TSO_RAFT=1 bash $KIT/scripts/gen_node_conf.sh > /dev/null || { log "生成配置失败"; exit 1; }
for h in 10.77.110.147 10.77.110.148 $C4; do ssh -n $h "mkdir -p $T/conf"; scp -q node.sh $h:$T/; done
scp -q $KIT/conf/out/node1.conf $C4:$T/conf/
scp -q $KIT/conf/out/node3.conf 10.77.110.147:$T/conf/; scp -q $KIT/conf/out/node4.conf 10.77.110.148:$T/conf/
log "配置：套件 $KIT，协调者在 $C4，内存上限 ${MEM_LIMIT:-不限}，shared_buffers $(grep -h '^shared_buffers' $KIT/conf/out/node2.conf | tail -1 | cut -d= -f2)"
# 144 上的镜像按 ID 比：同名但不是这一版就重新传
want=$(docker image inspect $IMG --format '{{.Id}}')
have=$(ssh -n $C4 "docker image inspect $IMG --format '{{.Id}}' 2>/dev/null")
if [ "$want" != "$have" ]; then
  log "把 $IMG 传到 $C4（1.4 GB）"
  docker save $IMG | ssh $C4 "docker load" | tail -1
fi
for h in localhost 10.77.110.147 10.77.110.148 $C4; do
  if [ $h = localhost ]; then c="bash -c"; else c="ssh -n $h"; fi
  $c "docker image inspect $IMG > /dev/null && docker tag $IMG shardpg:prof" || { echo "$h 上没有 $IMG"; exit 1; }
done
log "四台的 shardpg:prof → $IMG（$(docker run --rm shardpg:prof md5sum /work/pg-install/lib/postgresql/pg_partdist.so | cut -c1-12)）"
bash shardpg_rep.sh stop > /dev/null 2>&1
docker rm -f shardpg-fix-client shardpg-fix-n1 > /dev/null 2>&1; rm -f $KIT/build_db.log
ssh -n 10.77.110.147 "docker rm -f shardpg-fix-n1 > /dev/null 2>&1; true"
ssh -n 10.77.110.148 "docker rm -f shardpg-fix-n1 > /dev/null 2>&1; true"      # 3 台机器部署时的协调者
IMG=shardpg:prof ./node.sh 2 31082 $KIT/conf/out &
ssh -n 10.77.110.147 "cd $T && MEM_LIMIT=$MEM_LIMIT IMG=shardpg:prof ./node.sh 3 31081 conf" &
ssh -n 10.77.110.148 "cd $T && MEM_LIMIT=$MEM_LIMIT IMG=shardpg:prof ./node.sh 4 31083 conf" &
ssh -n $C4 "cd $T && MEM_LIMIT=$MEM_LIMIT IMG=shardpg:prof ./node.sh 1 31080 conf" &
wait
docker run -d --name shardpg-fix-client --network host -v $PWD/$KIT:/t shardpg:prof sleep infinity > /dev/null
docker exec shardpg-fix-client bash /t/scripts/wire_cluster.sh 2>&1 | tail -2
log "装载（$(grep '^WAREHOUSES=' $KIT/cluster.env | cut -d= -f2) 仓）"
docker exec shardpg-fix-client bash -c 'bash /t/scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
grep -E "EXIT" $KIT/build_db.log
grep -q "EXIT=0" $KIT/build_db.log || { log "装载失败"; tail -20 $KIT/build_db.log; exit 1; }
bash shardpg_rep.sh standbys 2>&1 | grep -E " up|FAILED"; bash shardpg_rep.sh sync quorum > /dev/null; bash shardpg_rep.sh wait; bash shardpg_rep.sh status
log "部署完成（4 台机器：协调者 $C4 + 3 worker，$IMG，套件 $KIT）"
