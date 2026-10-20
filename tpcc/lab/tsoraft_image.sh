#!/bin/bash
# TSO 随控制面 leader + 切主后分片 xid 不重发（2026-10-19）的实验室镜像：三台机器各做一个
#   shardpg:tsoraft = shardpg:prof-pre-fixb + build/tsoraft/out 下新编的两个扩展库与两份扩展 SQL
# 前置：build/tsoraft/out 已由 build_tsoraft.sh 编好（补丁 = git diff 2fedde3，含方案 B）。
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014
set -e
cd $R/build/tsoraft
ls out/pg_partdist.so out/pg_raft.so out/pg_partdist--1.0.sql out/pg_raft--1.0.sql >/dev/null
cat > Dockerfile <<'DF'
FROM shardpg:prof-pre-fixb
COPY out/pg_partdist.so out/pg_raft.so /work/pg-install/lib/postgresql/
COPY out/pg_partdist--1.0.sql out/pg_raft--1.0.sql /work/pg-install/share/postgresql/extension/
LABEL tpcc.fix="route-a + atomicity-fix-b + tso-follows-raft-leader + shard-xid-wm-wal"
DF
BUILD='DOCKER_BUILDKIT=0 docker build --network=none -t shardpg:tsoraft . > build.log 2>&1 && echo "$(hostname) shardpg:tsoraft 就绪" || { tail -5 build.log; exit 1; }'
bash -c "$BUILD" &
for h in 10.77.110.147 10.77.110.148; do
  ssh -n $h "mkdir -p $T/build/tsoraft/out"
  scp -q Dockerfile $h:$T/build/tsoraft/ && scp -q out/* $h:$T/build/tsoraft/out/
  ssh -n $h "cd $T/build/tsoraft && $BUILD" &
done
wait
for h in localhost 10.77.110.147 10.77.110.148; do
  if [ $h = localhost ]; then c="bash -c"; else c="ssh -n $h"; fi
  $c "docker run --rm shardpg:tsoraft md5sum /work/pg-install/lib/postgresql/pg_raft.so /work/pg-install/lib/postgresql/pg_partdist.so | cut -c1-12 | tr '\n' ' '; echo"
done
