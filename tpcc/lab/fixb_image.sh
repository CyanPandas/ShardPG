#!/bin/bash
# 方案 B（跨节点原子性修复，2026-10-18）的实验室镜像：在三台机器上各做一个
#   shardpg:fixb = shardpg:prof-pre-fixb + build/fixb/out 下新编的 pg_partdist.so / pg_raft.so 与两份扩展 SQL
# 首次运行时把当前的 shardpg:prof 备份成 shardpg:prof-pre-fixb（修复前版本，A/B 对照用）。
# 前置：build/fixb/out 里已有编好的 4 个文件（在 shardpg:prof 容器里对 /work 源码打补丁后 make 得到）。
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014
set -e
cd $R/build/fixb
ls out/pg_partdist.so out/pg_raft.so out/pg_partdist--1.0.sql out/pg_raft--1.0.sql >/dev/null
cat > Dockerfile <<'EOF'
FROM shardpg:prof-pre-fixb
COPY out/pg_partdist.so out/pg_raft.so /work/pg-install/lib/postgresql/
COPY out/pg_partdist--1.0.sql out/pg_raft--1.0.sql /work/pg-install/share/postgresql/extension/
LABEL tpcc.fix="route-a + atomicity-fix-b (tentative decision anchored on driver commit)"
EOF
BUILD='docker image inspect shardpg:prof-pre-fixb >/dev/null 2>&1 || docker tag shardpg:prof shardpg:prof-pre-fixb;
  DOCKER_BUILDKIT=0 docker build --network=none -t shardpg:fixb . > build.log 2>&1 && echo "$(hostname) shardpg:fixb 就绪" || { tail -5 build.log; exit 1; }'
bash -c "$BUILD" &
for h in 10.77.110.147 10.77.110.148; do
  ssh -n $h "mkdir -p $T/build/fixb/out"
  scp -q Dockerfile $h:$T/build/fixb/ && scp -q out/* $h:$T/build/fixb/out/
  ssh -n $h "cd $T/build/fixb && $BUILD" &
done
wait
for h in localhost 10.77.110.147 10.77.110.148; do
  if [ $h = localhost ]; then c="bash -c"; else c="ssh -n $h"; fi
  $c "docker run --rm shardpg:fixb md5sum /work/pg-install/lib/postgresql/pg_raft.so /work/pg-install/lib/postgresql/pg_partdist.so | cut -c1-12 | tr '\n' ' '; echo"
done
