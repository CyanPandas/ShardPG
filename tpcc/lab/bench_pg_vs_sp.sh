#!/bin/bash
# 2026-10-20：三机一主两备 PostgreSQL（pgsync3.sh：原生流复制，ANY 1 (standby1, standby2) + synchronous_commit = on）
# 与三机一主两备 ShardPG（每个 worker 一主两备 ANY 1；4 个 worker 与 3 个 worker 各一组）的 TPC-C。
# 口径同 perf3_run.sh：64 迷你仓、同一套事务函数、RC、shared_buffers 8GB；热身 + 128/192/256/384 终端各 1 min × 2 遍。
# 跑完后 ShardPG 恢复成原来的 3 worker 部署。用法（145 上，nohup 后台）：bench_pg_vs_sp.sh
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014; cd $R; L=$R/tsoraft_logs; mkdir -p $L
log() { echo "[$(date +%T)] $*"; }
step() { log "== $*"; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
wait_tso_on_coord() { for i in $(seq 120); do $CLI -h 10.77.110.148 -p 31080 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && return 0; sleep 1; done; return 1; }

if [ "${SKIP_PG_LOAD:-0}" != 1 ]; then
step "PG 一主两备：TPC-C 套件（复制单机套件、指向 31300）+ 客户端 + 装载"
rm -rf tpcc-pgsync; cp -a tpcc-native tpcc-pgsync; rm -rf tpcc-pgsync/results/* tpcc-pgsync/build_db.log
chmod 777 tpcc-pgsync/results      # 客户端容器里的用户要能写结果
sed -i -e 's/^NODE_PORTS=.*/NODE_PORTS=(31300)/' -e 's/^COORD_PORT=.*/COORD_PORT=31300/' tpcc-pgsync/cluster.env
. /data1/yyt/pgsync3/.secrets
docker rm -f pgsync-client > /dev/null 2>&1
docker run -d --name pgsync-client --network host -e PGPASSWORD="$SUPW" -v $R/tpcc-pgsync:/t shardpg:fix-20261014 sleep infinity > /dev/null
docker exec pgsync-client bash -c 'cd /t && bash scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
log "PG 装载 $(tail -1 tpcc-pgsync/build_db.log)"
fi
chmod 777 tpcc-pgsync/results
bash pgsync3.sh status
step "PG 一主两备性能"; rm -f $L/perf-pgsync.txt; bash perf3_run.sh pgsync pgsync

step "ShardPG 一主两备（4 个 worker）：全新部署"
bash redeploy5w.sh shardpg:tsoraft > $L/deploy-sp4w.log 2>&1; tail -1 $L/deploy-sp4w.log
wait_tso_on_coord && log "发号者在协调者上" || log "120 s 内发号者没落到协调者"
step "ShardPG 4 worker 性能"; bash perf3_run.sh shardpg sp4w

step "ShardPG 一主两备（3 个 worker）：恢复 4 节点配置 + 全新部署"
EXTRA_WORKERS="n5:10.77.110.147:31084:shardpg-fix-n5:31098" bash shardpg_rep.sh stop > /dev/null 2>&1
docker rm -f shardpg-fix-n5-s145 > /dev/null 2>&1
ssh -n 10.77.110.147 "docker rm -f shardpg-fix-n5 > /dev/null 2>&1; true"
ssh -n 10.77.110.148 "docker rm -f shardpg-fix-n5-s148 > /dev/null 2>&1; true"
cp tpcc/cluster.env.4node tpcc/cluster.env; rm -rf tpcc/conf/out; cp -r tpcc/conf/out.4node_tsoraft tpcc/conf/out
scp -q tpcc/conf/out/node3.conf 10.77.110.147:$T/conf/; scp -q tpcc/conf/out/node1.conf tpcc/conf/out/node4.conf 10.77.110.148:$T/conf/
bash redeploy_img.sh shardpg:tsoraft > $L/deploy-sp3w.log 2>&1; tail -1 $L/deploy-sp3w.log
wait_tso_on_coord && log "发号者在协调者上" || log "120 s 内发号者没落到协调者"
step "ShardPG 3 worker 性能"; bash perf3_run.sh shardpg sp3w
step "ALL_DONE"
