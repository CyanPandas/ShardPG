#!/bin/bash
# 协调者宕机实验（TSO 随控制面 leader，2026-10-19）：
#   负载从 worker n2 进（不经协调者，函数下推照常）→ 压测途中 docker kill 协调者 n1
#   → 看 TSO 换届的空窗、有没有客户端中止、新发号者从哪里起发 → 标准一致性 + 跨节点不变式
#   → 拉起 n1，看发号者是否回到协调者、经协调者的负载是否正常 → 再核对一次。
# 用法（145 上，集群由 redeploy_img.sh shardpg:tsoraft 全新部署）：tsoraft_coordkill.sh <标签>
R=/data1/yyt/shardpg-fix-20261014; cd $R
TAG=${1:?标签}; OUT=$R/tsoraft_logs/ck-$TAG; rm -rf $OUT; mkdir -p $OUT
log() { echo "[$(date +%T)] $*" | tee -a $OUT/run.log; }
TERMS=${TERMS:-64}; DUR=${DUR:-150}; KILL_AT=${KILL_AT:-40}
CH=${COORD_HOST:-10.77.110.148}      # 协调者所在机器（4 台机器部署时是 144）
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
N2="$CLI -h 10.77.110.145 -p 31082"; N1="$CLI -h $CH -p 31080"
chk() { local P="$1"; echo "标准 [$($P -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($P -f /t/xnode_consistency_mini.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"; }
# NODES = 全部节点（协调者在第一个），WNODES = 除协调者外的节点；4 个 worker 部署时由调用方覆盖
NODES=${NODES:-$CH:31080 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083}
WNODES=${WNODES:-10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083}
serving() { for hp in $NODES; do
  s=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -oE 'counter=[0-9]+ hwm=[0-9]+|raft_term=[0-9]+ lease_left_ms=[0-9]+' | tr '\n' ' ')
  case "$s" in *"raft_term=0 "*|"") ;; *) echo "$hp $s";; esac; done; }
log "[$TAG] 起点：发号者 $(serving | tr '\n' ';')"
log "[$TAG] 起点核对（经 n2）：$(chk "$N2")"
D0=$($N2 -c "select sum(d_next_o_id) from district")
docker exec shardpg-fix-client bash -c "cd /t && /work/pg-install/bin/pgbench -h 10.77.110.145 -p 31082 -U postgres -n \
  -c $TERMS -j $TERMS -T $DUR -P 1 --max-tries=20 --failures-detailed -D nw=64 \
  -f pgbench/new_order.sql@45 -f pgbench/payment.sql@43 -f pgbench/order_status.sql@4 \
  -f pgbench/delivery.sql@4 -f pgbench/stock_level.sql@4 postgres" > $OUT/pgbench.out 2> $OUT/pgbench.err &
PGB=$!
sleep $KILL_AT
ssh -n $CH "docker kill shardpg-fix-n1" >/dev/null; T0=$(date +%s.%N); log "[$TAG] 已 kill 协调者 n1（压测第 ${KILL_AT} s）"
NEWS=""
for i in $(seq 300); do NEWS=$(serving); [ -n "$NEWS" ] && break; sleep 0.1; done
log "[$TAG] 新发号者：${NEWS:-（30 s 内没有）}（kill 后 $(echo "$(date +%s.%N) - $T0" | bc | cut -c1-5) s 观测到）"
wait $PGB
D1=$($N2 -c "select sum(d_next_o_id) from district")
log "[$TAG] 负载：$(grep -E '^tps|^number of failed|^number of transactions actually' $OUT/pgbench.out | tr '\n' ' ')；NewOrder $((D1 - D0)) 笔"
log "[$TAG] 客户端中止：$(grep -c 'aborted' $OUT/pgbench.err)；错误：$(grep -E 'ERROR' $OUT/pgbench.err | sed -E 's/[0-9]+/N/g' | cut -c1-160 | sort | uniq -c | sort -rn | head -3 | tr '\n' ';')"
# 进度里吞吐跌到 kill 前均值 10% 以下的秒数 = 发号空窗
grep -E '^progress' $OUT/pgbench.err | awk -v k=$KILL_AT '{t=$2+0; v=$4+0; if (t<=k && t>k-20) {s+=v; n++} if (t>k) {if (v < 0.1*s/n) z++; if (t<=k+30) printf "%d:%d ", t, v}} END {printf "\nkill 前 20 s 均值 %.0f tps；kill 后吞吐 < 10%% 的秒数 %d\n", s/n, z}' > $OUT/gap.txt
log "[$TAG] $(tail -1 $OUT/gap.txt)"; log "[$TAG] kill 后 30 s 逐秒 tps：$(head -1 $OUT/gap.txt)"
for i in $(seq 60); do c=0; for hp in $WNODES; do v=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null); c=$((c + ${v:-99})); done; [ $c = 0 ] && break; sleep 1; done
log "[$TAG] 2PC 收尾：剩余 $c"
log "[$TAG] 协调者宕机期间核对（经 n2）：$(chk "$N2")"
ssh -n $CH "docker start shardpg-fix-n1" >/dev/null; T1=$(date +%s); log "[$TAG] 拉起 n1"
for i in $(seq 120); do s=$(serving); case "$s" in $CH:31080*) break;; esac; sleep 1; done
log "[$TAG] 拉起后 $(( $(date +%s) - T1 )) s 发号者：$(serving | tr '\n' ';')"
log "[$TAG] 经协调者 TPC-C 16 终端：$(bash scale.sh shardpg 16 2>&1 | tail -1)"
log "[$TAG] 收尾核对（经协调者）：$(chk "$N1")"
for h in 10.77.110.145 10.77.110.147 10.77.110.148; do
  if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi
  for ct in $($c "docker ps -a --format '{{.Names}}' | grep -E '^shardpg-fix-n[1-4]\$'"); do $c "docker cp $ct:/d/pgdata/log - 2>/dev/null" > $OUT/log-$ct.tar 2>/dev/null; done
done
echo CK_DONE
