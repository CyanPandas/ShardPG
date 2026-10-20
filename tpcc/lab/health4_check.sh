#!/bin/bash
# 4 节点 ShardPG（协调者 n1 + worker n2/n3/n4，每个 worker 一主两备 ANY 1）全面健康检查。
# 用法（145 上）：health4_check.sh [镜像=shardpg:tsoraft]；日志 tsoraft_logs/health4-<时刻>/
#   0 全新部署 + 登记备库（打开自动切主）   1 拓扑 / 控制面 / TSO / 复制状态
#   2 从 4 个节点分别当入口跑 TPC-C 事务      3 经协调者 128 终端 TPC-C 1 min + 标准一致性 + 跨节点不变式 + 无悬挂 2PC
#   4 跨节点读隔离（并发转账 + 读总额）       5 负载中 kill worker n4 主库（自动切主）
#   6 负载中 kill 协调者 n1（TSO 由新 leader 接着发）   7 收尾核对
R=/data1/yyt/shardpg-fix-20261014; cd $R
IMG=${1:-shardpg:tsoraft}
OUT=$R/tsoraft_logs/health4-$(date +%m%d-%H%M); mkdir -p $OUT
log() { echo "[$(date +%T)] $*" | tee -a $OUT/run.log; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
N1="$CLI -h 10.77.110.148 -p 31080"
chk() { local P="$1"; echo "标准 [$($P -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($P -f /t/xnode_consistency_mini.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"; }
pend() { local t=0 v; for hp in "$@"; do v=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null); t=$((t + ${v:-99})); done; echo $t; }
PB="-f /t/pgbench/new_order.sql@45 -f /t/pgbench/payment.sql@43 -f /t/pgbench/order_status.sql@4 -f /t/pgbench/delivery.sql@4 -f /t/pgbench/stock_level.sql@4"

log "== 0 全新部署（$IMG，TSO 随控制面 leader）"
bash tsoraft_conf.sh on > /dev/null
bash redeploy_img.sh $IMG > $OUT/deploy.log 2>&1; log "$(tail -1 $OUT/deploy.log)"
bash shardpg_rep.sh register > /dev/null; log "已登记 6 个备库、打开自动切主"
for i in $(seq 120); do $N1 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && break; sleep 1; done

log "== 1 拓扑与状态"
$N1 -F' ' -c "select '  Citus 节点 ' || nodeid || ' 组 ' || groupid || ' ' || nodename || ':' || nodeport || ' ' || noderole || ' active=' || isactive || ' 元数据同步=' || metadatasynced || ' 放分片=' || shouldhaveshards from pg_dist_node order by groupid" | tee -a $OUT/run.log
$N1 -c "select '  分片放置：' || string_agg(nodename || ':' || nodeport || ' ' || n || ' 个', '；') from (select nodename, nodeport, count(*) n from pg_dist_shard_placement group by 1, 2 order by 2) s" | tee -a $OUT/run.log
bash tsoraft_status.sh | sed 's/^/  /' | tee -a $OUT/run.log
bash shardpg_rep.sh status 2>&1 | sed 's/^/  复制 /' | tee -a $OUT/run.log

log "== 2 每个节点都能当入口"
for hp in 10.77.110.148:31080 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do
  docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h ${hp%:*} -p ${hp#*:} -U postgres -n -c 4 -j 4 -t 50 --max-tries=20 -D nw=64 $PB postgres > $OUT/entry-${hp#*:}.out 2>&1
  log "  入口 $hp：$(grep -E '^number of transactions actually|^number of failed' $OUT/entry-${hp#*:}.out | tr '\n' ' ')客户端中止 $(grep -c aborted $OUT/entry-${hp#*:}.out)"
done

log "== 3 经协调者 TPC-C 128 终端 1 min"
log "  $(bash scale.sh shardpg 128 2>&1 | tail -1)"
log "  2PC 残留：$(pend 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083)；核对：$(chk "$N1")"

log "== 4 跨节点读隔离（24 并发，转账 80% + 一条语句读全表总额 20%，60 s）"
tbl=iso_$(date +%H%M%S); rm -f $R/tpcc/iso_bad.log
$N1 -q -v tbl=$tbl -v shardpg=1 -f /t/iso_setup.sql > $OUT/iso_setup.log 2>&1 || log "  建表失败：$(tail -2 $OUT/iso_setup.log)"
printf '%s\n' '\set a random(1, 72)' '\set d random(1, 71)' '\set b 1 + (:a - 1 + :d) % 72' '\set x random(1, 100)' "SELECT tpcc.iso_xfer('$tbl', :a, :b, :x);" > $R/tpcc/iso_xfer.sql
printf '%s\n' 'BEGIN;' 'SELECT tpcc.begin_global();' "SELECT sum(bal) AS s FROM $tbl \\gset" 'COMMIT;' '\if :s != 7200000' '\shell echo :s >> /t/iso_bad.log' '\endif' > $R/tpcc/iso_reader.sql
docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h 10.77.110.148 -p 31080 -U postgres -n -c 24 -j 24 -T 60 --max-tries=20 -f /t/iso_xfer.sql@8 -f /t/iso_reader.sql@2 postgres > $OUT/iso.out 2>&1
reads=$(awk '/SQL script 2:/{f=1} f && /- [0-9]+ transactions/{print $2; exit}' $OUT/iso.out); xfers=$(awk '/SQL script 1:/{f=1} f && /- [0-9]+ transactions/{print $2; exit}' $OUT/iso.out)
bad=0; [ -f $R/tpcc/iso_bad.log ] && bad=$(wc -l < $R/tpcc/iso_bad.log)
final=$($N1 -c "BEGIN" -c "SELECT tpcc.begin_global()" -c "SELECT sum(bal) FROM $tbl" -c "COMMIT" | grep -E '^[0-9]+$' | tail -1)
log "  转账 ${xfers:-?} 笔、读总额 ${reads:-?} 次，读到不一致 $bad 次；静止后总额 $final（应为 7200000）；客户端中止 $(grep -c aborted $OUT/iso.out)"

log "== 5 负载中 kill worker n4 主库（148:31083）"
( bash scale.sh shardpg 64 > $OUT/fo_load.txt 2>&1 ) & RUN=$!
sleep 20; ssh -n 10.77.110.148 "docker kill shardpg-fix-n4" > /dev/null; T0=$(date +%s); log "  已 kill n4"
NEW=""; for i in $(seq 240); do NEW=$($N1 -c "select nodename||':'||nodeport from pg_dist_node where nodeport=31096 limit 1" 2>/dev/null); [ -n "$NEW" ] && break; sleep 0.5; done
log "  Citus 路由改到 ${NEW:-（120 s 内没切）}（kill 后 $(( $(date +%s) - T0 )) s）"
wait $RUN; R2=$(ls -td tpcc/results/*/ | head -1)
log "  负载：$(tail -1 $OUT/fo_load.txt)；客户端中止 $(grep -cE 'aborted' $R2/pgbench.err)"
for i in $(seq 90); do p=$(pend 10.77.110.145:31082 10.77.110.147:31081 ${NEW:-x:0}); [ "$p" = 0 ] && break; sleep 1; done
log "  2PC 残留 $p；核对：$(chk "$N1")"
ssh -n 10.77.110.148 "docker start shardpg-fix-n4" > /dev/null
for i in $(seq 60); do st=$(ssh -n 10.77.110.148 "docker inspect -f '{{.State.Running}}' shardpg-fix-n4"); [ "$st" = false ] && break; sleep 1; done
log "  旧主 n4 拉起后 running=$st（$i s，false = 已自行停机）"

log "== 6 负载中 kill 协调者 n1（负载从 n2 进）"
DUR=90 bash tsoraft_coordkill.sh h4 > $OUT/coordkill.out 2>&1
grep -E '^\[' $OUT/coordkill.out | sed 's/^/  /' | tee -a $OUT/run.log

log "== 7 收尾"
log "  核对：$(chk "$N1")"
bash tsoraft_status.sh | sed 's/^/  /' | tee -a $OUT/run.log
log "HEALTH4_DONE"
