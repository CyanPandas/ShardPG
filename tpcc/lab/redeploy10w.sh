#!/bin/bash
# 10 个 worker 的路线 A 实验室集群（2026-10-21，功能验证）：协调者 n1（144:31080）+ 10 个 worker，每个一主两备 ANY 1，
# 布局见 w10.env。TSO 随控制面 leader（TSO_RAFT=1），控制面 11 个成员。
# 套件 tpcc-stdw10：40 仓、20 个分片（每个 worker 2 个），shared_buffers 1GB（w10.conf）；套件不在就由 tpcc-std 生成。
# 数据目录与 4 台机器部署分开（<根>/w10/、<根>/shardpg-rep-w10/）：4 台机器那套（300 仓）的容器被换掉，数据留在盘上。
# 144 是共享机器：只动 shardpg-fix-n1 容器与 /home/yyt/shardpg-fix-20261014 目录。
# 用法（145 上）：redeploy10w.sh [镜像=shardpg:noconflict]
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
IMG=${1:-shardpg:noconflict}
source $R/w10.env
C4=10.77.110.144
onhost() { local h=$1; shift; if [ "$h" = 10.77.110.145 ]; then bash -c "$*"; else ssh -n $h "$*"; fi; }
base() { case "$1" in 10.77.110.145) echo $R ;; 10.77.110.148) echo $BASE148 ;; *) echo $T ;; esac; }

if [ ! -d $KIT ]; then
  mkdir $KIT && tar -C tpcc-std --exclude=./results --exclude=./build_db.log --exclude=./conf/out -cf - . | tar -C $KIT -xf -
  cp w10.conf $KIT/conf/w10.conf
  ids=""; hosts=""; ports=""; roles=""
  for x in $W10_NODES; do IFS=: read id h p <<< "$x"
    ids="$ids $id"; hosts="$hosts $h"; ports="$ports $p"; roles="$roles $([ $id = 1 ] && echo coordinator || echo worker)"; done
  cat > $KIT/cluster.env <<EOF
# 2026-10-21 10 个 worker 功能验证（redeploy10w.sh 生成）：协调者 144 + 10 个 worker，每个一主两备 ANY 1，布局见 lab/w10.env。
# 40 仓、20 个分片（每个 worker 2 个）；shared_buffers 1GB（conf/w10.conf）。
NODE_IDS=(${ids# })
NODE_HOSTS=(${hosts# })
NODE_PORTS=(${ports# })
NODE_ROLES=(${roles# })
COORD_HOST=$C4
COORD_PORT=31080
PG_BIN=/work/pg-install/bin
PGDATA_ROOT=/d/pgdata
WAREHOUSES=40
TERMINALS=64
RAMPUP_S=60
DURATION_S=120
SHARD_COUNT=20
REPLICAS=0
LOAD_PARALLEL=20
MAX_TRIES=20
RESULT_ROOT=/t/results
NODE_CONF_EXTRA=\$TPCC_DIR/conf/w10.conf
CONSISTENCY_EACH=0
MONITOR=0
EOF
  mkdir -p $KIT/results; chmod -R a+rwX $KIT
  log "生成套件 $KIT"
fi
TSO_RAFT=1 bash $KIT/scripts/gen_node_conf.sh > /dev/null || { log "生成配置失败"; exit 1; }
for h in 10.77.110.147 10.77.110.148 $C4; do ssh -n $h "mkdir -p $T/conf-w10"; scp -q node.sh $h:$T/; done
for x in $W10_NODES; do IFS=: read id h p <<< "$x"
  [ $h = 10.77.110.145 ] || scp -q $KIT/conf/out/node$id.conf $h:$T/conf-w10/; done
log "配置：套件 $KIT，$(echo $W10_NODES | wc -w) 个节点，shared_buffers $(grep -h '^shared_buffers' $KIT/conf/out/node2.conf | tail -1 | cut -d= -f2)，peers $(grep -h '^pg_raft.peers' $KIT/conf/out/node2.conf | tr -cd ',' | wc -c | awk '{print $1+1}') 个"
# 144 上的镜像按 ID 比：同名但不是这一版就重新传
want=$(docker image inspect $IMG --format '{{.Id}}')
have=$(ssh -n $C4 "docker image inspect $IMG --format '{{.Id}}' 2>/dev/null")
if [ "$want" != "$have" ]; then
  log "把 $IMG 传到 $C4（1.4 GB）"
  docker save $IMG | ssh $C4 "docker load" | tail -1
fi
for h in 10.77.110.145 10.77.110.147 10.77.110.148 $C4; do
  onhost $h "docker image inspect $IMG > /dev/null && docker tag $IMG shardpg:prof" || { echo "$h 上没有 $IMG"; exit 1; }
done
log "四台的 shardpg:prof → $IMG（$(docker run --rm shardpg:prof md5sum /work/pg-install/lib/postgresql/pg_partdist.so | cut -c1-12)）"
# 换掉现有的 ShardPG 容器（4 台机器那套的主库、备库；只删容器，数据目录不动）
bash shardpg_rep.sh stop > /dev/null 2>&1
docker rm -f shardpg-fix-client > /dev/null 2>&1; rm -f $KIT/build_db.log
for h in 10.77.110.145 10.77.110.147 10.77.110.148 $C4; do
  onhost $h "docker ps -a --format '{{.Names}}' | grep -E '^shardpg-fix-n[0-9]+\$' | xargs -r docker rm -f > /dev/null"
done
for h in 10.77.110.145 10.77.110.147 10.77.110.148 $C4; do
  cmd=""
  for x in $W10_NODES; do IFS=: read id hh p <<< "$x"; [ $hh = $h ] || continue
    if [ $h = 10.77.110.145 ]; then cf=$KIT/conf/out; else cf=conf-w10; fi
    cmd="$cmd DATA_DIR=$(base $h)/w10/data-n$id IMG=shardpg:prof ./node.sh $id $p $cf &"
  done
  if [ $h = 10.77.110.145 ]; then (cd $R && eval "$cmd wait") & else ssh -n $h "cd $T && ($cmd wait)" & fi
done
wait
docker run -d --name shardpg-fix-client --network host -v $PWD/$KIT:/t shardpg:prof sleep infinity > /dev/null
docker exec shardpg-fix-client bash /t/scripts/wire_cluster.sh 2>&1 | tail -2
log "装载（$(grep '^WAREHOUSES=' $KIT/cluster.env | cut -d= -f2) 仓，$(grep '^SHARD_COUNT=' $KIT/cluster.env | cut -d= -f2) 个分片）"
docker exec shardpg-fix-client bash -c 'bash /t/scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
grep -E "EXIT" $KIT/build_db.log
grep -q "EXIT=0" $KIT/build_db.log || { log "装载失败"; tail -20 $KIT/build_db.log; exit 1; }
bash shardpg_rep.sh standbys 2>&1 | grep -E " up|FAILED"; bash shardpg_rep.sh sync quorum > /dev/null; bash shardpg_rep.sh wait; bash shardpg_rep.sh status
log "部署完成（协调者 $C4 + 10 个 worker，$IMG，套件 $KIT）"
