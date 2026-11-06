# 两机构建 shardpg:prof → 全新部署 3 节点 → 接线 → 后台建库
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014
cp /tmp/fix-src.tar.gz $R/build/prof/fix-src.tar.gz; scp -q /tmp/fix-src.tar.gz 10.77.110.147:$T/build/prof/fix-src.tar.gz
(cd $R/build/prof && DOCKER_BUILDKIT=0 docker build --network=none --memory=4g -t shardpg:prof . > build.log 2>&1; rc=$?; echo $rc > /tmp/rc145; echo 145 build rc=$rc; grep -E "\.c:[0-9]+:[0-9]+: (warning|error)" build.log | head -3) &
(ssh -n 10.77.110.147 "cd $T/build/prof && DOCKER_BUILDKIT=0 docker build --network=none --memory=4g -t shardpg:prof . > build.log 2>&1; echo 147 build rc=\$?" > /tmp/rc147.log; cat /tmp/rc147.log) &
wait
if [ "$(cat /tmp/rc145)" != 0 ] || ! grep -q "rc=0" /tmp/rc147.log; then echo BUILD FAILED; grep -E "error" $R/build/prof/build.log | head -5; exit 1; fi
cd $R; docker rm -f shardpg-fix-client >/dev/null 2>&1; rm -f tpcc/build_db.log
docker rm -f shardpg-fix-n1 >/dev/null 2>&1 || true   # 协调者已移到 147
IMG=shardpg:prof ./node.sh 2 31082 tpcc/conf/out & ssh -n 10.77.110.147 "cd $T && (IMG=shardpg:prof ./node.sh 1 31080 conf & IMG=shardpg:prof ./node.sh 3 31081 conf & wait)" & wait
docker run -d --name shardpg-fix-client --network host -v $PWD/tpcc:/t shardpg:prof sleep infinity >/dev/null
docker exec shardpg-fix-client bash /t/scripts/wire_cluster.sh 2>&1 | tail -1
docker exec -d shardpg-fix-client bash -c 'bash /t/scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
