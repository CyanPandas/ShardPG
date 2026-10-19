#!/usr/bin/env bash
# 跨节点提交原子性 + 崩溃恢复（ShardPG 单副本）：TPC-C 压测中途对一个 worker docker kill（SIGKILL），过 DOWN 秒再拉起；
# 等全部 2PC 收尾（各节点 pg_prepared_xacts 与 DTX 未决登记都归零）后，跑跨节点不变式（xnode_consistency.sql）
# 和 TPC-C 标准一致性检查（90_consistency.sql）。三台 worker 轮流各杀一次。
# 用法（客户端机上，ShardPG 已部署好）：bash cloud/tests/test_atomicity.sh   可选：CLIENTS=并发 T=每段秒数 KILL_AT=第几秒杀 DOWN=停几秒
source "$(dirname "$0")/../lib.sh"
CLIENTS=${CLIENTS:-64}; T=${T:-100}; KILL_AT=${KILL_AT:-35}; DOWN=${DOWN:-20}   # 不能叫 C：cloud.env 里 C 是第三台机器的 IP
H=$COORD_NODE_HOST; P=$COORD_PORT; M=/t
ensure_client
OUT="$TPCC_DIR/results/$(date +%Y%m%d-%H%M%S)_shardpg-atomicity"; mkdir -p "$OUT"
QN() { cpsql "$1" "$2" -qAt -c "$3" 2>/dev/null; }
neworders() { QN "$H" "$P" "SELECT sum(d_next_o_id) FROM district"; }
# unsettled：各节点 pg_prepared_xacts 行数与 DTX 未决登记数之和
unsettled() { local k n=0 a b
  for k in "${!NODE_ID_LIST[@]}"; do
    a=$(QN "${NODE_HOST_LIST[$k]}" "${NODE_PORT_LIST[$k]}" "SELECT count(*) FROM pg_prepared_xacts")
    b=$(QN "${NODE_HOST_LIST[$k]}" "${NODE_PORT_LIST[$k]}" "SELECT partdist.dtx_pending_count()")
    [[ "$a" =~ ^[0-9]+$ && "$b" =~ ^[0-9]+$ ]] || { echo "?"; return; }
    n=$((n + a + b))
  done; echo $n; }
wait_settled() { local t0=$(date +%s) u
  while :; do u=$(unsettled); [[ "$u" == 0 ]] && { echo $(( $(date +%s) - t0 )); return 0; }
    (( $(date +%s) - t0 > 300 )) && { echo "超时（仍有 $u）"; return 1; }; sleep 3; done; }
# 重启过的 worker 要等它的分片组重新当选、路由登记好才能服务（在此之前 ShardPG 拒绝在它上面查询，防读到旧数据）
serving() { cpsql "$H" "$P" -qAt -c "SELECT count(*) FROM warehouse" -c "SELECT sum(d_next_o_id) FROM district" >/dev/null 2>&1; }
check() { local tag=$1 vs vx ns nx i
  for i in 1 2 3 4 5; do   # 查询失败（如分片暂不可服务）就重试；5 项 + 2 项都出结果才算跑成
    cx "source $M/scripts/lib.sh && QC -f $M/sql/90_consistency.sql" > "$OUT/$tag.std.txt" 2>&1
    cpsql "$H" "$P" -f "$M/cloud/tests/xnode_consistency.sql" > "$OUT/$tag.xnode.txt" 2>&1
    ns=$(grep -c violations "$OUT/$tag.std.txt"); nx=$(grep -c violations "$OUT/$tag.xnode.txt")
    [[ $ns == 5 && $nx == 2 ]] && break; sleep 10
  done
  [[ $ns == 5 && $nx == 2 ]] || { echo "！！一致性检查没跑成（见 $OUT/$tag.*.txt）"; return; }
  vs=$(grep -A2 'violations' "$OUT/$tag.std.txt" | grep -E '^ *[0-9]+ *$' | awk '{s+=$1} END{print s+0}')
  vx=$(awk '/violations/{getline; getline; split($0, a, "|"); gsub(/ /, "", a[1]); s += a[1]} END{print s+0}' "$OUT/$tag.xnode.txt")
  echo "标准一致性 $ns 项违例 $vs；跨节点不变式 $nx 项违例 $vx"; }

log "基线（压测前）：$(wait_settled >/dev/null; check baseline)"
grep -E "remote_order_lines|^ +[0-9]+ \|" "$OUT/baseline.xnode.txt" | tail -1 | sed 's/^/    跨节点订单行（远程供货 \/ 新订单行）：/'
ROUNDS=${ROUNDS:-1}   # 每台 worker 各杀几次
seg=0
for r in $(seq 1 "$ROUNDS"); do for k in 1 2 3; do
  seg=$((seg + 1))
  id=${NODE_ID_LIST[$k]}; h=${NODE_HOST_LIST[$k]}; p=${NODE_PORT_LIST[$k]}
  s1=$(neworders)
  log "第 $seg 段：$CLIENTS 终端 TPC-C ${T}s，第 ${KILL_AT}s 对 worker n$id（$h:$p）docker kill，停 ${DOWN}s 后拉起"
  cx "/work/pg-install/bin/pgbench -h $H -p $P -U postgres -n -c $CLIENTS -j $CLIENTS -T $T --max-tries=20 --failures-detailed \
      -D nw=$WAREHOUSES -f $M/pgbench/new_order.sql@45 -f $M/pgbench/payment.sql@43 -f $M/pgbench/order_status.sql@4 \
      -f $M/pgbench/delivery.sql@4 -f $M/pgbench/stock_level.sql@4 postgres" > "$OUT/phase$seg.out" 2> "$OUT/phase$seg.err" &
  pg=$!
  sleep "$KILL_AT"
  on "$h" "docker kill $PREFIX-n$id >/dev/null" && log "  已 kill n$id"
  sleep "$DOWN"
  on "$h" "docker start $PREFIX-n$id >/dev/null"
  tr0=$(date +%s)
  for _ in $(seq 1 120); do on "$h" "docker exec $PREFIX-n$id /work/pg-install/bin/pg_isready -h 127.0.0.1 -p $p -q" && break; sleep 1; done
  log "  n$id 已拉起（可连接，$(( $(date +%s) - tr0 )) s）"
  for _ in $(seq 1 300); do serving && break; sleep 1; done
  serving && log "  集群恢复服务（所有分片可查询）：拉起后 $(( $(date +%s) - tr0 )) s" || log "  ！！拉起后 300 s 仍不能服务"
  wait $pg
  ab=$(grep -cE 'client [0-9]+ (script [0-9]+ )?aborted' "$OUT/phase$seg.err")
  st=$(wait_settled); for _ in $(seq 1 30); do s2=$(neworders); [[ "$s2" =~ ^[0-9]+$ ]] && break; sleep 2; done
  log "  压测结束：提交 NewOrder $((s2 - s1)) 笔，中止的客户端 $ab（连接被断的客户端会中止，属预期）；2PC 全部收尾用时 ${st} s"
  log "  核对：$(check phase$seg)"
  grep -m3 -oE 'ERROR: .{0,120}|FATAL: .{0,120}' "$OUT/phase$seg.err" | sort | uniq -c | sort -rn | head -3 | sed 's/^/    错误样例：/'
done; done
log "全部结束，明细在 $OUT"
