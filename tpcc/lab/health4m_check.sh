#!/bin/bash
# 2026-10-21：4 台机器布局（协调者 n1 144:31080 + worker n2 145:31082 / n3 147:31081 / n4 148:31083，每个 worker 一主两备 ANY 1）
# 的全面健康检查。用来验证"ShardPG 不用 INSERT ... ON CONFLICT"的改动：控制面 raft 日志、快照、节点与分区登记、
# 备库与切主登记、分片身份、参与登记、2PC 决议都换了写法，所以部署、入口、读隔离、竞态、切主、协调者宕机全过一遍。
# 套件 tpcc-std（30 标准仓）；148 的数据放 /data3/yyt（BASE148，不写 148 的 /home）。
#   0 全新部署 + 登记备库（打开自动切主）     1 拓扑 / 控制面 / TSO / 复制状态
#   2 4 个节点分别当入口跑 TPC-C 事务          3 经协调者 128 终端 TPC-C（60 s + 1 min）+ 核对 + 无悬挂 2PC
#   4 跨节点读隔离（并发转账 + 读总额）         5 2PC 决议竞态的确定性验证（默认 3 轮 + SOLO 3 轮）
#   6 分片表拒绝 INSERT ... ON CONFLICT，普通表照常
#   7 负载中 kill worker n4 主库（自动切主）    8 负载中 kill 协调者 n1（TSO 由新 leader 接着发）   9 收尾核对
# 用法（145 上）：health4m_check.sh [镜像=shardpg:noconflict]；日志 tsoraft_logs/health4m-<时刻>/
R=/data1/yyt/shardpg-fix-20261014; cd $R
IMG=${1:-shardpg:noconflict}; KIT=tpcc-std; NW=$(grep '^WAREHOUSES=' $KIT/cluster.env | cut -d= -f2)
export BASE148=/data3/yyt/shardpg-fix-20261014
OUT=$R/tsoraft_logs/health4m-$(date +%m%d-%H%M); mkdir -p $OUT
log() { echo "[$(date +%T)] $*" | tee -a $OUT/run.log; }
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
N1="$CLI -h 10.77.110.144 -p 31080"
ALL="10.77.110.144:31080 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083"
chk() { local P="$1"; echo "标准 [$($P -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($P -f /t/xnode_consistency.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"; }
pend() { local t=0 v; for hp in "$@"; do v=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null); t=$((t + ${v:-99})); done; echo $t; }
PB="-f /t/pgbench/new_order.sql@45 -f /t/pgbench/payment.sql@43 -f /t/pgbench/order_status.sql@4 -f /t/pgbench/delivery.sql@4 -f /t/pgbench/stock_level.sql@4"

log "== 0 全新部署（$IMG，$KIT，$NW 仓；148 的数据在 $BASE148）"
KIT=$KIT bash redeploy4m.sh $IMG > $OUT/deploy.log 2>&1
grep -q "部署完成" $OUT/deploy.log || { log "部署失败：$(tail -5 $OUT/deploy.log | tr '\n' ' ')"; log "HEALTH4M_DONE"; exit 1; }
log "$(grep -E '部署完成|shardpg:prof →' $OUT/deploy.log | tr '\n' ' ')"
NODES="$ALL" bash shardpg_rep.sh register > $OUT/register.log 2>&1
log "登记 6 个备库、打开自动切主：$(tail -2 $OUT/register.log | tr '\n' ' ')"
for i in $(seq 120); do $N1 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && break; sleep 1; done

log "== 1 拓扑与状态"
$N1 -F' ' -c "select '  Citus 节点 ' || nodeid || ' 组 ' || groupid || ' ' || nodename || ':' || nodeport || ' ' || noderole || ' active=' || isactive || ' 元数据同步=' || metadatasynced from pg_dist_node order by groupid" | tee -a $OUT/run.log
NODES="$ALL" bash tsoraft_status.sh | sed 's/^/  /' | tee -a $OUT/run.log
bash shardpg_rep.sh status 2>&1 | sed 's/^/  复制 /' | tee -a $OUT/run.log
log "  备库登记（控制面 apply 后各节点一致）：$(for hp in $ALL; do echo -n "${hp%:*}=$($CLI -h ${hp%:*} -p ${hp#*:} -c 'select count(*) from partdist.node_standby') "; done)"

log "== 2 每个节点都能当入口"
for hp in $ALL; do
  docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h ${hp%:*} -p ${hp#*:} -U postgres -n -c 4 -j 4 -t 50 --max-tries=20 -D nw=$NW $PB postgres > $OUT/entry-${hp#*:}.out 2>&1
  log "  入口 $hp：$(grep -E '^number of transactions actually|^number of failed' $OUT/entry-${hp#*:}.out | tr '\n' ' ')客户端中止 $(grep -c aborted $OUT/entry-${hp#*:}.out)"
done

log "== 3 经协调者 TPC-C 128 终端（预热 60 s + 测量 1 min）"
log "  $(SCALE_DUR=60 bash scale.sh shardpg-std 128 2>&1 | tail -1)"
for i in $(seq 60); do p=$(pend 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083); [ "$p" = 0 ] && break; sleep 1; done
log "  2PC 残留：$p；核对：$(chk "$N1")"

log "== 4 跨节点读隔离（24 并发，转账 80% + 一条语句读全表总额 20%，60 s）"
tbl=iso_$(date +%H%M%S); rm -f $R/$KIT/iso_bad.log
$N1 -q -v tbl=$tbl -v shardpg=1 -f /t/iso_setup.sql > $OUT/iso_setup.log 2>&1 || log "  建表失败：$(tail -2 $OUT/iso_setup.log)"
printf '%s\n' '\set a random(1, 72)' '\set d random(1, 71)' '\set b 1 + (:a - 1 + :d) % 72' '\set x random(1, 100)' "SELECT tpcc.iso_xfer('$tbl', :a, :b, :x);" > $R/$KIT/iso_xfer.sql
printf '%s\n' 'BEGIN;' 'SELECT tpcc.begin_global();' "SELECT sum(bal) AS s FROM $tbl \\gset" 'COMMIT;' '\if :s != 7200000' '\shell echo :s >> /t/iso_bad.log' '\endif' > $R/$KIT/iso_reader.sql
chmod 666 $R/$KIT/iso_xfer.sql $R/$KIT/iso_reader.sql
docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h 10.77.110.144 -p 31080 -U postgres -n -c 24 -j 24 -T 60 --max-tries=20 -f /t/iso_xfer.sql@8 -f /t/iso_reader.sql@2 postgres > $OUT/iso.out 2>&1
reads=$(awk '/SQL script 2:/{f=1} f && /- [0-9]+ transactions/{print $2; exit}' $OUT/iso.out); xfers=$(awk '/SQL script 1:/{f=1} f && /- [0-9]+ transactions/{print $2; exit}' $OUT/iso.out)
bad=0; [ -f $R/$KIT/iso_bad.log ] && bad=$(wc -l < $R/$KIT/iso_bad.log)
final=$($N1 -c "BEGIN" -c "SELECT tpcc.begin_global()" -c "SELECT sum(bal) FROM $tbl" -c "COMMIT" | grep -E '^[0-9]+$' | tail -1)
log "  转账 ${xfers:-?} 笔、读总额 ${reads:-?} 次，读到不一致 $bad 次；静止后总额 $final（应为 7200000）；客户端中止 $(grep -c aborted $OUT/iso.out)"

log "== 5 2PC 决议竞态的确定性验证"
KIT=$KIT bash dtx_race_check.sh 3 > $OUT/race.out 2>&1
log "  默认组：$(grep -E '共 3 轮|RACE_' $OUT/race.out | tr '\n' ' ')"
KIT=$KIT SOLO=1 bash dtx_race_check.sh 3 > $OUT/race_solo.out 2>&1
log "  SOLO 组：$(grep -E '共 3 轮|RACE_' $OUT/race_solo.out | tr '\n' ' ')"

log "== 6 分片表拒绝 INSERT ... ON CONFLICT，普通表照常"
r1=$($N1 -c "INSERT INTO warehouse SELECT * FROM warehouse WHERE w_id = 1 ON CONFLICT DO NOTHING" 2>&1 | head -1)
r2=$($N1 -c "INSERT INTO district SELECT * FROM district WHERE d_w_id = 2 AND d_id = 1 ON CONFLICT (d_w_id, d_id) DO UPDATE SET d_tax = EXCLUDED.d_tax" 2>&1 | head -1)
r3=$($N1 -c "CREATE TEMP TABLE oc_t (k int PRIMARY KEY, v int)" -c "INSERT INTO oc_t VALUES (1, 1)" -c "INSERT INTO oc_t VALUES (1, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v" -c "SELECT 'oc_t.v=' || v FROM oc_t" 2>&1 | tail -1)
log "  分片表 DO NOTHING：$r1"
log "  分片表 DO UPDATE：$r2"
log "  普通临时表（不是分片表）：$r3"

log "== 7 负载中 kill worker n4 主库（148:31083）"
( SCALE_DUR=60 bash scale.sh shardpg-std 64 > $OUT/fo_load.txt 2>&1 ) & RUN=$!
sleep 20; ssh -n 10.77.110.148 "docker kill shardpg-fix-n4" > /dev/null; T0=$(date +%s); log "  已 kill n4"
NEW=""; for i in $(seq 240); do NEW=$($N1 -c "select nodename||':'||nodeport from pg_dist_node where nodeport=31096 limit 1" 2>/dev/null); [ -n "$NEW" ] && break; sleep 0.5; done
log "  Citus 路由改到 ${NEW:-（120 s 内没切）}（kill 后 $(( $(date +%s) - T0 )) s）"
wait $RUN; R2=$(ls -td $KIT/results/*/ | head -1)
log "  负载：$(tail -1 $OUT/fo_load.txt)；客户端中止 $(grep -cE 'aborted' $R2/pgbench.err)"
for i in $(seq 90); do p=$(pend 10.77.110.145:31082 10.77.110.147:31081 ${NEW:-x:0}); [ "$p" = 0 ] && break; sleep 1; done
log "  2PC 残留 $p；核对：$(chk "$N1")"
log "  各节点 partition_map / node_address 一致性：$(for hp in 10.77.110.144:31080 10.77.110.145:31082 10.77.110.147:31081 ${NEW:-x:0}; do echo -n "${hp}=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select md5(string_agg(partition_id || ':' || primary_node || ':' || primary_term, ',' order by partition_id)) from partdist.partition_map" 2>/dev/null | cut -c1-8)/$($CLI -h ${hp%:*} -p ${hp#*:} -c "select string_agg(node_id || '@' || host || ':' || port, ',' order by node_id) from partdist.node_address" 2>/dev/null) "; done)"
ssh -n 10.77.110.148 "docker start shardpg-fix-n4" > /dev/null
for i in $(seq 60); do st=$(ssh -n 10.77.110.148 "docker inspect -f '{{.State.Running}}' shardpg-fix-n4"); [ "$st" = false ] && break; sleep 1; done
log "  旧主 n4 拉起后 running=$st（$i s，false = 已自行停机）"

log "== 8 负载中 kill 协调者 n1（负载从 n2 进）"
NODES="10.77.110.144:31080 10.77.110.145:31082 10.77.110.147:31081 ${NEW:-10.77.110.148:31083}" \
WNODES="10.77.110.145:31082 10.77.110.147:31081 ${NEW:-10.77.110.148:31083}" \
COORD_HOST=10.77.110.144 NW=$NW XN=/t/xnode_consistency.sql SYS=shardpg-std DUR=90 \
  bash tsoraft_coordkill.sh h4m > $OUT/coordkill.out 2>&1
grep -E '^\[' $OUT/coordkill.out | sed 's/^/  /' | tee -a $OUT/run.log

log "== 9 收尾"
log "  核对：$(chk "$N1")"
NODES="10.77.110.144:31080 10.77.110.145:31082 10.77.110.147:31081 ${NEW:-10.77.110.148:31083}" bash tsoraft_status.sh | sed 's/^/  /' | tee -a $OUT/run.log
log "  各节点日志里的决议锁等待：$(for hn in 10.77.110.145:n2 10.77.110.147:n3; do h=${hn%:*}; if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi; echo -n "${hn#*:}=$($c "docker exec shardpg-fix-${hn#*:} bash -c 'cat /d/pgdata/log/*.log'" | grep -c '决议锁被并发写入者持有') "; done)"
log "  各节点日志里的 raft_log / 元数据写入报错：$(for hn in 10.77.110.144:n1 10.77.110.145:n2 10.77.110.147:n3; do h=${hn%:*}; if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi; echo -n "${hn#*:}=$($c "docker exec shardpg-fix-${hn#*:} bash -c 'cat /d/pgdata/log/*.log'" | grep -cE 'UPDATE 失败|INSERT 失败|INSERT 应插入|duplicate key value|批量持久化 raft_log 失败|写 leader 日志行失败') "; done)"
log "HEALTH4M_DONE"
