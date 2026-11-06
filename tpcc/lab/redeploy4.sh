# 三机 4 节点：148 = coordinator(n1) + worker(n4)，145 = worker(n2)，147 = worker(n3)
# 三机并行构建 shardpg:prof（任一失败即中止）→ 全新部署 → 接线 → 后台建库
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014
cp /tmp/fix-src.tar.gz $R/build/prof/fix-src.tar.gz
for h in 10.77.110.147 10.77.110.148; do scp -q /tmp/fix-src.tar.gz $R/build/prof/Dockerfile $h:$T/build/prof/; done
rm -f /tmp/rc_*.log
(cd $R/build/prof && DOCKER_BUILDKIT=0 docker build --network=none --memory=4g -t shardpg:prof . > build.log 2>&1; echo "145 build rc=$?" > /tmp/rc_145.log) &
for h in 10.77.110.147 10.77.110.148; do
  (ssh -n $h "cd $T/build/prof && DOCKER_BUILDKIT=0 docker build --network=none --memory=4g -t shardpg:prof . > build.log 2>&1; echo $h build rc=\$?" > /tmp/rc_$h.log) &
done
wait
cat /tmp/rc_*.log
if grep -L "rc=0" /tmp/rc_*.log | grep -q .; then echo BUILD FAILED; grep -E "error" $R/build/prof/build.log | head -5; exit 1; fi
cd $R; docker rm -f shardpg-fix-client shardpg-fix-n1 >/dev/null 2>&1; rm -f tpcc/build_db.log
ssh -n 10.77.110.147 "docker rm -f shardpg-fix-n1 >/dev/null 2>&1; true"
IMG=shardpg:prof ./node.sh 2 31082 tpcc/conf/out &
ssh -n 10.77.110.147 "cd $T && IMG=shardpg:prof ./node.sh 3 31081 conf" &
ssh -n 10.77.110.148 "cd $T && (IMG=shardpg:prof ./node.sh 1 31080 conf & IMG=shardpg:prof ./node.sh 4 31083 conf & wait)" &
wait
docker run -d --name shardpg-fix-client --network host -v $PWD/tpcc:/t shardpg:prof sleep infinity >/dev/null
docker exec shardpg-fix-client bash /t/scripts/wire_cluster.sh 2>&1 | tail -2
docker exec -d shardpg-fix-client bash -c 'bash /t/scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
