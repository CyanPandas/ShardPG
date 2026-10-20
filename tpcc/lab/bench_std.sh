#!/bin/bash
# 2026-10-20：标准规模 TPC-C（30 仓），不限 CPU、不限内存：
#   PG       三机一主两备（pgsync3.sh：原生流复制，synchronous_standby_names = 'ANY 1 (standby1, standby2)'
#            + synchronous_commit = on ⇒ 每笔提交返回前至少 1 个备库已把 WAL 刷盘）
#   ShardPG  4 台机器：协调者 144 + 3 个 worker（145 / 147 / 148），每个 worker 一主两备 ANY 1；
#            TSO 随控制面 leader + 节点内 TSO 代理（pg_partdist.tso_proxy，默认开）
#   两边 shared_buffers 8GB / effective_cache_size 32GB（ShardPG 见 std.conf），容器不设 CPU / 内存上限；
#   同一套事务函数、RC、同一个 pgbench；压测见 perf5_run.sh（热身 + 100/200/300/400 终端各一轮 × 2 遍，每轮预热 60 s + 测量 3 min）。
#   （同日先试过每个容器限 8g 内存、shared_buffers 2GB：PG 的 12 GB 库装不进内存，145 的数据盘是单块机械盘，
#    100% 繁忙、每次读等 120 ms，PG 只有 1–2.5k tpmC，测的是这块盘而不是数据库，数据作废。
#    随后不限资源跑 100 仓：12 GB 的库随机写回 + WAL 刷盘把 145 / 147 的机械盘写满，PG 只有约 9k tpmC、
#    活跃会话几乎全在等 WAL 写盘，用户叫停，改 30 仓、每轮 3 min。）
# 套件：tpcc-std = 仓库里的标准 tpcc/ + conf/std.conf（= lab/std.conf）+ xnode_consistency.sql + 手写的 cluster.env（WAREHOUSES=30、RAMPUP_S=60、DURATION_S=180、CONSISTENCY_EACH=0、MONITOR=0）；
#       tpcc-pgsync-std = tpcc-std 经 scripts/make_native.sh 生成，cluster.env 指向 145:31300。两个目录都要 chmod 777。
# 先测 PG（ShardPG 容器停掉），再全新部署 ShardPG 测；跑完 lab 留在 ShardPG 标准规模部署上。
# 用法（145 上，nohup 后台）：bench_std.sh   可选 SKIP_PG=1 / SKIP_SP=1
R=/data1/yyt/shardpg-fix-20261014; cd $R; L=$R/tsoraft_logs; mkdir -p $L
log() { echo "[$(date +%T)] $*"; }
step() { log "== $*"; }
limits() { for h in 10.77.110.144 10.77.110.145 10.77.110.147 10.77.110.148; do
    if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi
    $c "docker ps --format '{{.Names}}' | grep -E '$1' | xargs -r docker inspect -f '{{.Name}} 内存上限={{.HostConfig.Memory}} 含swap={{.HostConfig.MemorySwap}}'" | sed "s/^/  $h /"; done; }

if [ "${SKIP_PG:-0}" != 1 ]; then
step "停 ShardPG（PG 测试期间不留别的数据库负载）"
bash shardpg_rep.sh stop > /dev/null 2>&1
docker rm -f shardpg-fix-client > /dev/null 2>&1; docker stop shardpg-fix-n2 > /dev/null 2>&1
ssh -n 10.77.110.147 "docker stop shardpg-fix-n3 > /dev/null 2>&1; true"
ssh -n 10.77.110.148 "docker stop shardpg-fix-n4 > /dev/null 2>&1; true"
ssh -n 10.77.110.144 "docker stop shardpg-fix-n1 > /dev/null 2>&1; true"

step "PG 一主两备：全新部署（shared_buffers 8GB，不限资源）"
bash pgsync3.sh destroy > $L/std-pg-deploy.log 2>&1
SB=8GB EFFC=32GB bash pgsync3.sh deploy >> $L/std-pg-deploy.log 2>&1
grep -E "^\[" $L/std-pg-deploy.log | tail -3
bash pgsync3.sh status 2>&1 | sed 's/password=[^ ]*/password=***/g'
limits '^pgsync-(primary|standby)'
. /data1/yyt/pgsync3/.secrets
docker rm -f pgsync-client > /dev/null 2>&1
docker run -d --name pgsync-client --network host -e PGPASSWORD="$SUPW" -v $R/tpcc-pgsync-std:/t shardpg:prof sleep infinity > /dev/null
unset SUPW REPL_PASS
step "PG 装载 $(grep '^WAREHOUSES=' tpcc-pgsync-std/cluster.env | cut -d= -f2) 仓（同步复制开着）"
rm -f tpcc-pgsync-std/build_db.log
docker exec pgsync-client bash -c 'cd /t && bash scripts/build_db.sh > /t/build_db.log 2>&1; echo EXIT=$? >> /t/build_db.log'
log "PG 装载 $(grep -E '^\[|FAIL' tpcc-pgsync-std/build_db.log | grep -E '④|⑤|FAIL' | tr '\n' ' ') $(tail -1 tpcc-pgsync-std/build_db.log)"
grep -q "EXIT=0" tpcc-pgsync-std/build_db.log || { log "PG 装载失败"; tail -20 tpcc-pgsync-std/build_db.log; exit 1; }
docker exec pgsync-client /work/pg-install/bin/psql -h 10.77.110.145 -p 31300 -U postgres -XAtq -c "checkpoint" > /dev/null
log "PG 库大小 $(docker exec pgsync-client /work/pg-install/bin/psql -h 10.77.110.145 -p 31300 -U postgres -XAtq -c "select pg_size_pretty(pg_database_size('postgres'))")"
bash pgsync3.sh status 2>&1 | sed 's/password=[^ ]*/password=***/g'
step "PG 一主两备性能"; rm -f $L/perf5-pg.txt; bash perf5_run.sh pgsync-std pg
bash pgsync3.sh status 2>&1 | sed 's/password=[^ ]*/password=***/g'
step "停 PG"
docker rm -f pgsync-client > /dev/null 2>&1
bash pgsync3.sh stop > /dev/null 2>&1
fi

if [ "${SKIP_SP:-0}" != 1 ]; then
step "ShardPG 4 台机器（协调者 144 + 3 worker，一主两备）：全新部署 + 装载 $(grep '^WAREHOUSES=' tpcc-std/cluster.env | cut -d= -f2) 仓（shared_buffers 8GB，不限资源）"
KIT=tpcc-std bash redeploy4m.sh shardpg:tsoraft > $L/std-sp-deploy.log 2>&1
grep -E "^\[|EXIT|FAIL" $L/std-sp-deploy.log | tail -6
grep -q "部署完成" $L/std-sp-deploy.log || { log "ShardPG 部署失败"; tail -30 $L/std-sp-deploy.log; exit 1; }
log "ShardPG 装载 $(grep -E '④|⑤' tpcc-std/build_db.log | tr '\n' ' ')"
limits '^shardpg-fix-n[0-9]'
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
for i in $(seq 120); do $CLI -h 10.77.110.144 -p 31080 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && break; sleep 1; done
log "发号者：$($CLI -h 10.77.110.144 -p 31080 -c "select partdist.partdist_tso_status()" | grep -oE 'served=[tf]') （协调者）"
for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do
  log "  $hp 库大小 $($CLI -h ${hp%:*} -p ${hp#*:} -c "select pg_size_pretty(pg_database_size('postgres'))")"
done
step "ShardPG 性能"; rm -f $L/perf5-sp.txt; bash perf5_run.sh shardpg-std sp
bash shardpg_rep.sh status
fi
step "STD_DONE"
