#!/bin/bash
# 2026-10-21：推定中止与迟到判决的竞态（决议槽修复）的确定性验证。4 台机器部署（协调者 144），镜像 shardpg:dtxfix。
#   发起方 = n2（145:31082）上直接开的会话：本地仓 A 的 w_ytd + 2D，n3 上的仓 B、n4 上的仓 C 各 − D
#   （两个参与节点 ⇒ 常规路径：先通知协调组、再写判决）。
#   发起方停在暂停点 3（协调组已通知、判决未写）PAUSE 秒；三个 worker 的推定中止超时调到 RTO 毫秒；
#   dtx_status 写下推定中止的 ABORT、放掉认领位之后再停 HOLD 秒不提交 ⇒ 发起方醒来写判决时，正落在
#   "ABORT 已写、未提交"的窗口里 —— 2026-10-21 lab 300 仓一半提交的现场。
#   期望：发起方拿到"全局决议是 ABORT"（40001）整笔中止，三个仓的 w_ytd 都不变；协调组节点日志里有
#   "推定中止已写、事务未提交" 与 "决议锁被并发写入者持有"（发起方在决议锁上等守护的事务提交，再读到 ABORT）。
#   修复前的代码在同一窗口里会让发起方提交、参与方被回滚（一半提交）。
#   若守护没赶在发起方醒来前推定中止，这一轮整笔 COMMIT（没进窗口，也算一致），脚本会把 w_ytd 改回原值。
#   SOLO=1：只把 n3 的推定中止超时调低（n2 / n4 调到 10 min）。两个参与节点的守护都去问协调组时，后来的那个会
#     拿着认领位卡在主键冲突上，发起方排在它后面、等到 ABORT 提交后用新快照直接看到；只留一个守护去问，认领位在
#     "ABORT 已写未提交"期间是空的，发起方会直接在决议锁上等守护的事务提交、再读到 ABORT（日志"决议锁被并发写入者持有"）。
# 用法（145 上）：[KIT=tpcc-std] [PAUSE=25] [HOLD=20] [RTO=8000] [SOLO=1] dtx_race_check.sh [轮数=3]
R=/data1/yyt/shardpg-fix-20261014; cd $R
log() { echo "[$(date +%T)] $*"; }
N=${1:-3}; PAUSE=${PAUSE:-25}; HOLD=${HOLD:-20}; RTO=${RTO:-8000}; KIT=${KIT:-tpcc-std}
W=$(grep '^WAREHOUSES=' $KIT/cluster.env | cut -d= -f2)
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
C1="$CLI -h 10.77.110.144 -p 31080"
WK="10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083"
wh_on() { $C1 -c "select w from generate_series(1, $W) w join pg_dist_shard_placement p
  on p.shardid = get_shard_id_for_distribution_column('warehouse', w) where p.nodeport = $1 order by w limit 1"; }
A=$(wh_on 31082); B=$(wh_on 31081); C=$(wh_on 31083)
ytd() { $C1 -c "select string_agg(w_ytd::text, ' ' order by w_id) from warehouse where w_id in ($A, $B, $C)"; }
settled() { local tot=0 c hp; for hp in $WK; do
  c=$($CLI -h ${hp%:*} -p ${hp#*:} -c "select (select count(*) from pg_prepared_xacts) + partdist.dtx_pending_count()" 2>/dev/null)
  tot=$((tot + ${c:-99})); done; echo $tot; }
setguc() { local hp; for hp in $WK; do $CLI -h ${hp%:*} -p ${hp#*:} -c "$1" -c "$2" -c "select pg_reload_conf()" > /dev/null; done; }
# SOLO=1：n2 / n4 的推定中止超时调到 10 min，只留 n3 的守护去推定中止
solo() { local hp; for hp in 10.77.110.145:31082 10.77.110.148:31083; do
  $CLI -h ${hp%:*} -p ${hp#*:} -c "alter system set pg_raft.dtx_recover_timeout_ms = 600000" -c "select pg_reload_conf()" > /dev/null; done; }
nodelog() {   # 节点 名字 → 该容器全部日志（UTC）
  local h=$1 n=$2; if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi
  $c "docker exec shardpg-fix-$n bash -c 'cat /d/pgdata/log/*.log'"; }
trap 'setguc "alter system reset pg_raft.dtx_recover_timeout_ms" "alter system reset pg_raft.dtx_test_status_hold_ms"' EXIT
setguc "alter system set pg_raft.dtx_recover_timeout_ms = $RTO" "alter system set pg_raft.dtx_test_status_hold_ms = $((HOLD * 1000))"
[ "${SOLO:-0}" = 1 ] && solo
log "仓 A=$A（n2，发起方）B=$B（n3）C=$C（n4）；暂停 ${PAUSE}s，推定中止超时 ${RTO}ms$([ "${SOLO:-0}" = 1 ] && echo "（只 n3；n2 / n4 为 10 min）")，推定中止后停 ${HOLD}s"
pass=0; hit=0
for r in $(seq $N); do
  for i in $(seq 60); do [ "$(settled)" = 0 ] && break; sleep 2; done
  [ "$(settled)" = 0 ] || { log "！！第 $r 轮开测前还有未收尾的 2PC（$(settled)），停止"; exit 1; }
  D=$((RANDOM % 900 + 100)); Y0=$(ytd)
  SQL="SET pg_raft.dtx_test_pause_point = 3; SET pg_raft.dtx_test_pause_ms = $((PAUSE * 1000));
BEGIN; SELECT tpcc.begin_global();
UPDATE warehouse SET w_ytd = w_ytd + $((2 * D)) WHERE w_id = $A;
UPDATE warehouse SET w_ytd = w_ytd - $D WHERE w_id = $B;
UPDATE warehouse SET w_ytd = w_ytd - $D WHERE w_id = $C;
COMMIT;"
  T0=$(date +%s)
  out=$(docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -Xq -h 10.77.110.145 -p 31082 -c "$SQL" 2>&1 | tr '\n' ' ')
  T1=$(date +%s)
  for i in $(seq 90); do [ "$(settled)" = 0 ] && break; sleep 2; done
  Y1=$(ytd); set -- $Y0; a0=$1 b0=$2 c0=$3; set -- $Y1; a1=$1 b1=$2 c1=$3
  da=$(awk -v x=$a0 -v y=$a1 'BEGIN{print y-x}'); db=$(awk -v x=$b0 -v y=$b1 'BEGIN{print y-x}'); dc=$(awk -v x=$c0 -v y=$c1 'BEGIN{print y-x}')
  dtx=$(nodelog 10.77.110.145 n2 | grep "测试暂停点 3" | tail -1 | grep -oE "dtx [0-9]+" | cut -d' ' -f2)
  ev=""; for hn in 10.77.110.147:n3 10.77.110.148:n4; do
    l=$(nodelog ${hn%:*} ${hn#*:} | grep -E "dtx ${dtx:-x} (推定中止已写|的决议锁被并发写入者持有|写决议前发现已有决议)" | sed -E 's/.*pg_raft: //' | cut -c1-80 | tr '\n' ';')
    [ -n "$l" ] && ev="$ev ${hn#*:}: $l"; done
  if echo "$out" | grep -q "全局决议是 ABORT" && [ "$da$db$dc" = "000" ]; then
    res="整笔 ABORT（发起方读回推定中止的判决）"; pass=$((pass+1)); echo "$ev" | grep -q "决议锁被并发写入者持有" && hit=$((hit+1))
  elif [ "$da" = "$((2 * D))" ] && [ "$db" = "-$D" ] && [ "$dc" = "-$D" ]; then
    res="整笔 COMMIT（没进窗口）"; pass=$((pass+1))
    $CLI -h 10.77.110.145 -p 31082 -c "BEGIN" -c "SELECT tpcc.begin_global()" \
      -c "UPDATE warehouse SET w_ytd = w_ytd - $((2 * D)) WHERE w_id = $A" -c "UPDATE warehouse SET w_ytd = w_ytd + $D WHERE w_id = $B" \
      -c "UPDATE warehouse SET w_ytd = w_ytd + $D WHERE w_id = $C" -c "COMMIT" > /dev/null
  else
    res="！！不一致：w_ytd 变化 A $da / B $db / C $dc（D=$D）"
  fi
  log "第 $r 轮 dtx=${dtx:-?}（会话 $((T1 - T0)) s）：$res"
  log "    会话输出：$(echo "$out" | grep -oE "ERROR:[^。]*|COMMIT" | head -2 | tr '\n' ' ')"
  log "    协调组节点日志：${ev:- 无}"
done
log "共 $N 轮，一致 $pass 轮，其中命中竞态窗口（读回推定中止的判决）$hit 轮；收尾核对：标准 [$($C1 -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($C1 -f /t/xnode_consistency.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"
[ $pass = $N ] && log "RACE_PASS" || log "RACE_FAIL"
