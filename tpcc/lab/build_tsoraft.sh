#!/bin/bash
# 在 shardpg:prof-pre-fixb（/work 源码 = 2fedde3）里打补丁、编译，产物放 out/
R=/data1/yyt/shardpg-fix-20261014
B=$R/build/tsoraft
cd $B
docker run --rm -u root -v $B:/b shardpg:prof-pre-fixb bash -c '
 set -e; mkdir -p /tmp/b; cp -r /work/pg-partdist-src /work/pg-raft-src /tmp/b/; cd /tmp/b
 zcat /b/tsoraft.patch.gz | patch -p1 > /b/patch.log 2>&1 || { cat /b/patch.log; exit 1; }
 for d in pg-partdist-src pg-raft-src; do
   (cd $d && make clean >/dev/null 2>&1; make PG_CONFIG=/work/pg-install/bin/pg_config) > /b/build-$d.log 2>&1 || { echo "FAIL $d"; grep -E "error:|warning:" /b/build-$d.log | head -40; exit 1; }
 done
 mkdir -p /b/out; cp pg-partdist-src/pg_partdist.so pg-raft-src/pg_raft.so pg-partdist-src/sql/pg_partdist--1.0.sql pg-raft-src/sql/pg_raft--1.0.sql /b/out/
 chmod -R a+rwX /b
 echo BUILD_OK'
echo "--- warnings:"; grep -h -E "warning:|error:" $B/build-*.log | grep -v "^make" | head -40
