#!/bin/bash
# 2026-10-21：10 个 worker（协调者 144 + 10 个 worker，每个一主两备 ANY 1，布局见 w10.env）能否正常工作的检查。
# 套件 tpcc-stdw10（40 仓、20 个分片）；148 的数据放 /data3/yyt。在 health4m_check.sh 的基础上加了"一笔事务改 10 个 worker"。
#   0 全新部署 + 登记 20 个备库（打开自动切主）
#   1 拓扑 / 控制面 / TSO / 复制 / 每个 worker 上的分片与仓
#   2 11 个节点分别当入口跑 TPC-C 事务
#   3 经协调者 TPC-C 64 终端（预热 60 s + 测量 2 min）+ 核对 + 无悬挂 2PC；负载中采各 worker 连接数与各机可用内存
#   4 跨节点读隔离（并发转账 + 读总额）
#   5 一笔事务同时改 10 个 worker（10 个参与节点的 2PC）：提交、回滚、8 并发 60 s 后总额不变
#   6 2PC 决议竞态的确定性验证（n2 发起，n3 / n4 参与；默认 3 轮 + SOLO 3 轮）
#   7 分片表拒绝 INSERT ... ON CONFLICT，普通表照常
#   8 负载中 kill worker n7 主库（148:31103，自动切主）   9 负载中 kill 协调者 n1   10 收尾核对、各节点日志里的写入报错
# 用法（145 上，nohup 后台）：health10w_check.sh [镜像=shardpg:noconflict]；日志 tsoraft_logs/health10w-<时刻>/
R=/data1/yyt/shardpg-fix-20261014; cd $R
IMG=${1:-shardpg:noconflict}
source $R/w10.env
NW=$(grep '^WAREHOUSES=' $KIT/cluster.env 2>/dev/null | cut -d= -f2); NW=${NW:-40}
OUT=$R/tsoraft_logs/health10w-$(date +%m%d-%H%M); mkdir -p $OUT
log() { echo "[$(date +%T)] $*" | tee -a $OUT/run.log; }
trap 'echo STD_DONE >> $OUT/run.log' EXIT
(nohup bash disk_guard.sh $OUT/run.log > /dev/null 2>&1 < /dev/null &)
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
N1="$CLI -h 10.77.110.144 -p 31080"
ALL=""; for x in $W10_NODES; do ALL="$ALL ${x#*:}"; done; ALL=${ALL# }          # 11 个节点 host:port（协调者在第一个）
WK=${ALL#* }                                                                     # 10 个 worker
onhost() { local h=$1; shift; if [ "$h" = 10.77.110.145 ]; then bash -c "$*"; else ssh -n $h "$*"; fi; }
chk() { local P="$1"; echo "标准 [$($P -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($P -f /t/xnode_consistency.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"; }
pend() { local t=0 v; for hp in "$@"; do v=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null); t=$((t + ${v:-99})); done; echo $t; }
PB="-f /t/pgbench/new_order.sql@45 -f /t/pgbench/payment.sql@43 -f /t/pgbench/order_status.sql@4 -f /t/pgbench/delivery.sql@4 -f /t/pgbench/stock_level.sql@4"

log "== 0 全新部署（$IMG，$KIT；148 的数据在 $BASE148）"
bash redeploy10w.sh $IMG > $OUT/deploy.log 2>&1
grep -q "部署完成" $OUT/deploy.log || { log "部署失败：$(tail -5 $OUT/deploy.log | tr '\n' ' ')"; log "HEALTH10W_DONE"; exit 1; }
log "$(grep -E '部署完成|shardpg:prof →|配置：' $OUT/deploy.log | tr '\n' ' ')"
log "  装载：$(grep -E '^\[.*(④|⑥|建库完成)' $KIT/build_db.log | sed -E 's/^\[[0-9-]+ //' | tr '\n' ' ')"
NW=$(grep '^WAREHOUSES=' $KIT/cluster.env | cut -d= -f2)
NODES="$ALL" bash shardpg_rep.sh register > $OUT/register.log 2>&1
log "登记 20 个备库、打开自动切主：$(head -1 $OUT/register.log)，登记 $(grep -cE '^[0-9]+ n[0-9]+s' $OUT/register.log) 条"
for i in $(seq 120); do $N1 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && break; sleep 1; done

log "== 1 拓扑与状态"
$N1 -F' ' -c "select '  Citus 节点 ' || nodeid || ' 组 ' || groupid || ' ' || nodename || ':' || nodeport || ' ' || noderole || ' active=' || isactive || ' 元数据同步=' || metadatasynced from pg_dist_node order by groupid" | tee -a $OUT/run.log
log "  每个 worker 的 warehouse 分片数 / 仓数：$($N1 -c "select string_agg(nodeport || '=' || ns || '/' || nw, ' ' order by nodeport) from (select p.nodeport, count(distinct p.shardid) ns, count(w) nw from pg_dist_shard_placement p join pg_dist_shard s using (shardid) left join generate_series(1, $NW) w on get_shard_id_for_distribution_column('warehouse', w) = p.shardid where s.logicalrelid = 'warehouse'::regclass group by 1) x")"
NODES="$ALL" bash tsoraft_status.sh | sed 's/^/  /' | tee -a $OUT/run.log
log "  控制面（组 0）：$(for hp in $ALL; do $CLI -h ${hp%:*} -p ${hp#*:} -c "select state || '/任期' || current_term from partdist.pg_raft_group_status() where group_id = 0" 2>/dev/null | tr '\n' ' '; done | tr ' ' '\n' | sort | uniq -c | awk '{printf "%s×%s ", $2, $1}')"
log "  每个节点的 Raft 组数：$(for hp in $ALL; do echo -n "${hp#*:}=$($CLI -h ${hp%:*} -p ${hp#*:} -c 'select count(*) from partdist.pg_raft_group_status()' 2>/dev/null) "; done)"
bash shardpg_rep.sh status 2>&1 | sed 's/^/  复制 /' | tee -a $OUT/run.log
log "  备库登记（控制面 apply 后各节点一致）：$(for hp in $ALL; do echo -n "${hp#*:}=$($CLI -h ${hp%:*} -p ${hp#*:} -c 'select count(*) from partdist.node_standby') "; done)"

log "== 2 每个节点都能当入口"
for hp in $ALL; do
  docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h ${hp%:*} -p ${hp#*:} -U postgres -n -c 4 -j 4 -t 50 --max-tries=20 -D nw=$NW $PB postgres > $OUT/entry-${hp#*:}.out 2>&1
  log "  入口 $hp：$(grep -E '^number of transactions actually|^number of failed' $OUT/entry-${hp#*:}.out | tr '\n' ' ')客户端中止 $(grep -c aborted $OUT/entry-${hp#*:}.out)"
done

log "== 3 经协调者 TPC-C 64 终端（预热 60 s + 测量 2 min）"
( for i in $(seq 12); do sleep 15
    echo "$(date +%T) 连接 $(for hp in $WK; do echo -n "${hp#*:}=$($CLI -h ${hp%:*} -p ${hp#*:} -c 'select count(*) from pg_stat_activity' 2>/dev/null) "; done)" \
         "可用内存GB $(for h in 10.77.110.145 10.77.110.147 10.77.110.148; do echo -n "${h##*.}=$(onhost $h "free -g | awk '/Mem/{print \$7}'") "; done)"
  done > $OUT/load_sample.txt ) & SMP=$!
log "  $(bash scale.sh shardpg-stdw10 64 2>&1 | tail -1)"
wait $SMP
log "  负载中每个 worker 的最大连接数：$(awk '{for (i = 3; i <= NF; i++) if ($i ~ /^[0-9]+=[0-9]+$/ && $i !~ /^14[578]=/) {split($i, a, "="); if (a[2] > m[a[1]]) m[a[1]] = a[2]}} END {for (k in m) printf "%s=%s ", k, m[k]}' $OUT/load_sample.txt)"
log "  负载中各机最少可用内存（GB）：$(grep -oE '1(45|47|48)=[0-9]+' $OUT/load_sample.txt | awk -F= '{if (!($1 in m) || $2 < m[$1]) m[$1] = $2} END {for (k in m) printf "%s=%s ", k, m[k]}')"
for i in $(seq 60); do p=$(pend $WK); [ "$p" = 0 ] && break; sleep 1; done
log "  2PC 残留：$p；核对：$(chk "$N1")"

log "== 4 跨节点读隔离（24 并发，转账 80% + 一条语句读全表总额 20%，60 s）"
tbl=iso_$(date +%H%M%S); rm -f $R/$KIT/iso_bad.log
$N1 -q -v tbl=$tbl -v shardpg=1 -f /t/iso_setup.sql > $OUT/iso_setup.log 2>&1 || log "  建表失败：$(tail -2 $OUT/iso_setup.log)"
printf '%s\n' '\set a random(1, 72)' '\set d random(1, 71)' '\set b 1 + (:a - 1 + :d) % 72' '\set x random(1, 100)' "SELECT tpcc.iso_xfer('$tbl', :a, :b, :x);" > $R/$KIT/iso_xfer.sql
printf '%s\n' 'BEGIN;' 'SELECT tpcc.begin_global();' "SELECT sum(bal) AS s FROM $tbl \\gset" 'COMMIT;' '\if :s != 7200000' '\shell echo :s >> /t/iso_bad.log' '\endif' > $R/$KIT/iso_reader.sql
chmod 666 $R/$KIT/iso_xfer.sql $R/$KIT/iso_reader.sql
log "  表 $tbl 的 72 行分在 $($N1 -c "select count(distinct nodeport) from pg_dist_shard_placement p join pg_dist_shard s using (shardid) where s.logicalrelid = '$tbl'::regclass") 个 worker 上"
docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h 10.77.110.144 -p 31080 -U postgres -n -c 24 -j 24 -T 60 --max-tries=20 -f /t/iso_xfer.sql@8 -f /t/iso_reader.sql@2 postgres > $OUT/iso.out 2>&1
reads=$(awk '/SQL script 2:/{f=1} f && /- [0-9]+ transactions/{print $2; exit}' $OUT/iso.out); xfers=$(awk '/SQL script 1:/{f=1} f && /- [0-9]+ transactions/{print $2; exit}' $OUT/iso.out)
bad=0; [ -f $R/$KIT/iso_bad.log ] && bad=$(wc -l < $R/$KIT/iso_bad.log)
final=$($N1 -c "BEGIN" -c "SELECT tpcc.begin_global()" -c "SELECT sum(bal) FROM $tbl" -c "COMMIT" | grep -E '^[0-9]+$' | tail -1)
log "  转账 ${xfers:-?} 笔、读总额 ${reads:-?} 次，读到不一致 $bad 次；静止后总额 $final（应为 7200000）；客户端中止 $(grep -c aborted $OUT/iso.out)"

log "== 5 一笔事务同时改 10 个 worker（每个 worker 取一个仓：收款仓 w_ytd / d_ytd(d_id=1) 加 9x，其余 9 个仓各减 x）"
WS=""; for hp in $WK; do WS="$WS $($N1 -c "select min(w) from generate_series(1, $NW) w join pg_dist_shard_placement p on p.shardid = get_shard_id_for_distribution_column('warehouse', w) where p.nodeport = ${hp#*:}")"; done
WS=$(echo $WS | tr ' ' '\n' | grep -E '^[0-9]+$' | sort -n | tr '\n' ' '); WS=${WS% }; WL=$(echo $WS | tr ' ' ',')
log "  仓 $WS（$(echo $WS | wc -w) 个，分在 $($N1 -c "select count(distinct p.nodeport) from unnest('{$WL}'::int[]) w join pg_dist_shard_placement p on p.shardid = get_shard_id_for_distribution_column('warehouse', w)") 个 worker 上）"
ytd10() { $N1 -c "BEGIN" -c "SELECT tpcc.begin_global()" -c "select string_agg(w_ytd::text, ' ' order by w_id) from warehouse where w_id in ($WL)" -c "COMMIT" | grep -E '^[0-9. -]+$' | tail -1; }
# Citus 不能下推"聚合之后的目标列子查询"，两个和用 CTE 分开算
sum10() { $N1 -c "BEGIN" -c "SELECT tpcc.begin_global()" -c "with w as (select sum(w_ytd) s from warehouse where w_id in ($WL)), d as (select sum(d_ytd) s from district where d_w_id in ($WL)) select w.s || '/' || d.s from w, d" -c "COMMIT" | grep -E '^[0-9.]+/[0-9.]+$' | tail -1; }
# 一笔事务的 SQL：$1 = 收款仓在 WS 里的序号（1 起），$2 = x
tx10() { local k=0 w s="BEGIN; SELECT tpcc.begin_global();"; for w in $WS; do k=$((k+1))
    if [ $k = $1 ]; then a="+ $((9 * $2))"; else a="- $2"; fi
    s="$s UPDATE warehouse SET w_ytd = w_ytd $a WHERE w_id = $w; UPDATE district SET d_ytd = d_ytd $a WHERE d_w_id = $w AND d_id = 1;"; done
  echo "$s"; }
Y0=$(ytd10); S0=$(sum10)
o=$(docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -h 10.77.110.144 -p 31080 -c "$(tx10 3 7) COMMIT;" 2>&1 | grep -oE '(ERROR|WARNING):.*' | tr '\n' ' ')
Y1=$(ytd10)
d=$(paste -d' ' <(echo $Y0 | tr ' ' '\n') <(echo $Y1 | tr ' ' '\n') | awk '{printf "%s ", $2 - $1}')
log "  提交：会话报错 [${o:-无}]，各仓 w_ytd 变化 [$d]（应为第 3 个 +63、其余 -7）"
o=$(docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -h 10.77.110.144 -p 31080 -c "$(tx10 5 11) SELECT 1 / 0; COMMIT;" 2>&1 | grep -oE '(ERROR|WARNING):.*' | head -2 | tr '\n' ' ')
o2=$(docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq -h 10.77.110.144 -p 31080 -c "$(tx10 6 13) ROLLBACK;" 2>&1 | grep -oE '(ERROR|WARNING):.*' | tr '\n' ' ')
Y2=$(ytd10)
log "  出错回滚（报错 [$o]）、显式回滚（报错 [${o2:-无}]）：w_ytd $([ "$Y2" = "$Y1" ] && echo 都没变 || echo "！！变了：$Y1 → $Y2")"
{ echo '\set r random(1, 10)'; echo '\set x random(1, 100)'; echo 'BEGIN;'; echo 'SELECT tpcc.begin_global();'
  k=0; for w in $WS; do k=$((k+1))
    echo "UPDATE warehouse SET w_ytd = w_ytd + CASE WHEN :r = $k THEN 9 * :x ELSE - :x END WHERE w_id = $w;"
    echo "UPDATE district SET d_ytd = d_ytd + CASE WHEN :r = $k THEN 9 * :x ELSE - :x END WHERE d_w_id = $w AND d_id = 1;"; done
  echo 'COMMIT;'; } > $R/$KIT/tx10.sql; chmod 666 $R/$KIT/tx10.sql
docker exec shardpg-fix-client /work/pg-install/bin/pgbench -h 10.77.110.144 -p 31080 -U postgres -n -c 8 -j 8 -T 60 --max-tries=20 --failures-detailed -f /t/tx10.sql postgres > $OUT/tx10.out 2>&1
for i in $(seq 60); do p=$(pend $WK); [ "$p" = 0 ] && break; sleep 1; done
S1=$(sum10)
log "  8 并发 60 s：$(grep -E '^number of transactions actually|^number of failed|^number of transactions retried|^tps' $OUT/tx10.out | sed 's/ (without initial connection time)//' | tr '\n' ' ')客户端中止 $(grep -c aborted $OUT/tx10.out)"
log "  10 个仓 Σw_ytd / Σd_ytd：开始 $S0，静止后 $S1（应相同）；2PC 残留 $p；核对：$(chk "$N1")"

log "== 6 2PC 决议竞态的确定性验证（n2 发起，n3 / n4 参与）"
KIT=$KIT bash dtx_race_check.sh 3 > $OUT/race.out 2>&1
log "  默认组：$(grep -E '共 3 轮|RACE_' $OUT/race.out | tr '\n' ' ')"
KIT=$KIT SOLO=1 bash dtx_race_check.sh 3 > $OUT/race_solo.out 2>&1
log "  SOLO 组：$(grep -E '共 3 轮|RACE_' $OUT/race_solo.out | tr '\n' ' ')"

log "== 7 分片表拒绝 INSERT ... ON CONFLICT，普通表照常"
r1=$($N1 -c "INSERT INTO warehouse SELECT * FROM warehouse WHERE w_id = 1 ON CONFLICT DO NOTHING" 2>&1 | head -1)
r2=$($N1 -c "INSERT INTO district SELECT * FROM district WHERE d_w_id = 2 AND d_id = 1 ON CONFLICT (d_w_id, d_id) DO UPDATE SET d_tax = EXCLUDED.d_tax" 2>&1 | head -1)
r3=$($N1 -c "CREATE TEMP TABLE oc_t (k int PRIMARY KEY, v int)" -c "INSERT INTO oc_t VALUES (1, 1)" -c "INSERT INTO oc_t VALUES (1, 2) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v" -c "SELECT 'oc_t.v=' || v FROM oc_t" 2>&1 | tail -1)
log "  分片表 DO NOTHING：$r1"
log "  分片表 DO UPDATE：$r2"
log "  普通临时表（不是分片表）：$r3"

log "== 8 负载中 kill worker n7 主库（148:31103，备库在 145 / 147 的 31113）"
( SCALE_DUR=60 bash scale.sh shardpg-stdw10 64 > $OUT/fo_load.txt 2>&1 ) & RUN=$!
sleep 20; ssh -n 10.77.110.148 "docker kill shardpg-fix-n7" > /dev/null; T0=$(date +%s); log "  已 kill n7"
NEW=""; for i in $(seq 240); do NEW=$($N1 -c "select nodename||':'||nodeport from pg_dist_node where nodeport=31113 limit 1" 2>/dev/null); [ -n "$NEW" ] && break; sleep 0.5; done
log "  Citus 路由改到 ${NEW:-（120 s 内没切）}（kill 后 $(( $(date +%s) - T0 )) s）"
wait $RUN; R2=$(ls -td $KIT/results/*/ | head -1)
log "  负载：$(tail -1 $OUT/fo_load.txt)；客户端中止 $(grep -cE 'aborted' $R2/pgbench.err)"
ALL=${ALL/10.77.110.148:31103/${NEW:-x:0}}; WK=${WK/10.77.110.148:31103/${NEW:-x:0}}
for i in $(seq 90); do p=$(pend $WK); [ "$p" = 0 ] && break; sleep 1; done
log "  2PC 残留 $p；核对：$(chk "$N1")"
log "  各节点 partition_map 摘要 / node_address 里 n7 的地址：$(for hp in $ALL; do echo -n "${hp#*:}=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select md5(string_agg(partition_id || ':' || primary_node || ':' || primary_term, ',' order by partition_id)) from partdist.partition_map" 2>/dev/null | cut -c1-8)/$($CLI -h ${hp%:*} -p ${hp#*:} -c "select host || ':' || port from partdist.node_address where node_id = 7" 2>/dev/null) "; done)"
ssh -n 10.77.110.148 "docker start shardpg-fix-n7" > /dev/null
for i in $(seq 60); do st=$(ssh -n 10.77.110.148 "docker inspect -f '{{.State.Running}}' shardpg-fix-n7"); [ "$st" = false ] && break; sleep 1; done
log "  旧主 n7 拉起后 running=$st（$i s，false = 已自行停机）"

log "== 9 负载中 kill 协调者 n1（负载从 n2 进）"
NODES="$ALL" WNODES="$WK" COORD_HOST=10.77.110.144 NW=$NW XN=/t/xnode_consistency.sql SYS=shardpg-stdw10 DUR=90 \
  bash tsoraft_coordkill.sh h10w > $OUT/coordkill.out 2>&1
grep -E '^\[' $OUT/coordkill.out | sed 's/^/  /' | tee -a $OUT/run.log

log "== 10 收尾"
log "  核对：$(chk "$N1")"
NODES="$ALL" bash tsoraft_status.sh | sed 's/^/  /' | tee -a $OUT/run.log
log "  控制面（组 0）：$(for hp in $ALL; do $CLI -h ${hp%:*} -p ${hp#*:} -c "select state || '/任期' || current_term from partdist.pg_raft_group_status() where group_id = 0" 2>/dev/null | tr '\n' ' '; done | tr ' ' '\n' | sort | uniq -c | awk '{printf "%s×%s ", $2, $1}')"
# 现任主库容器（只看在跑的，不拉起任何停着的容器）：n7 切主后是 shardpg-fix-n7-s145 / -s147
cts=""; for x in $W10_NODES; do IFS=: read id h p <<< "$x"; hp=$h:$p; [ $id = 7 ] && hp=${NEW:-$hp}
  h=${hp%:*}; if [ $id = 7 ] && [ -n "$NEW" ]; then c=shardpg-fix-n7-s${h##*.}; else c=shardpg-fix-n$id; fi; cts="$cts $h/$c"; done
log "  各节点日志里的决议锁等待：$(for hc in $cts; do h=${hc%/*}; c=${hc#*/}; echo -n "${c#shardpg-fix-}=$(onhost $h "docker exec $c bash -c 'cat /d/pgdata/log/*.log'" 2>/dev/null | grep -c '决议锁被并发写入者持有') "; done)"
log "  各节点日志里的 raft_log / 元数据写入报错：$(for hc in $cts; do h=${hc%/*}; c=${hc#*/}; echo -n "${c#shardpg-fix-}=$(onhost $h "docker exec $c bash -c 'cat /d/pgdata/log/*.log'" 2>/dev/null | grep -cE 'UPDATE 失败|INSERT 失败|INSERT 应插入|duplicate key value|批量持久化 raft_log 失败|写 leader 日志行失败') "; done)"
log "  各节点日志里的 PANIC / 崩溃重启：$(for hc in $cts; do h=${hc%/*}; c=${hc#*/}; echo -n "${c#shardpg-fix-}=$(onhost $h "docker exec $c bash -c 'cat /d/pgdata/log/*.log'" 2>/dev/null | grep -cE 'PANIC|terminated by signal') "; done)"
log "HEALTH10W_DONE"
