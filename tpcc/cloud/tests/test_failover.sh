#!/usr/bin/env bash
# 高可用：TPC-C 压测中途对一个 worker 主库 docker kill（SIGKILL），不人工干预，看自动切主：
#   ① 控制面把该节点的路由（partdist.node_map）改到哪个备库、用了多久；② 该节点上的仓库多久能写入；
#   ③ 已确认的提交是否丢失（kill 前读到的 sum(d_next_o_id) ≤ 恢复后的值）；④ 2PC 全部收尾、跨节点不变式与标准一致性检查；
#   ⑤ 把旧主拉起来，看它是否自我隔离（时间线落后于接替者 → 自行停机）。
# 用法（客户端机上，已跑过 ha.sh standbys/sync/register）：bash cloud/tests/test_failover.sh   可选：VICTIM=节点号 CLIENTS= T= KILL_AT=
source "$(dirname "$0")/../lib.sh"
VICTIM=${VICTIM:-3}; CLIENTS=${CLIENTS:-64}; T=${T:-180}; KILL_AT=${KILL_AT:-60}   # 不能叫 C：cloud.env 里 C 是第三台机器的 IP
H=$COORD_NODE_HOST; P=$COORD_PORT; M=/t
ensure_client
for k in "${!NODE_ID_LIST[@]}"; do [[ "${NODE_ID_LIST[$k]}" == "$VICTIM" ]] && { vh=${NODE_HOST_LIST[$k]}; vp=${NODE_PORT_LIST[$k]}; }; done
[[ -n "${vh:-}" ]] || die "没有节点 n$VICTIM"
OUT="$TPCC_DIR/results/$(date +%Y%m%d-%H%M%S)_shardpg-failover"; mkdir -p "$OUT"
QN() { cpsql "$1" "$2" -qAt -c "$3" 2>/dev/null; }
route() { QN "$H" "$P" "SELECT hostname || ':' || port FROM partdist.node_map WHERE node_id = $VICTIM"; }
W=$(QN "$H" "$P" "SELECT w FROM generate_series(1, $WAREHOUSES) w JOIN pg_dist_shard_placement p
     ON p.shardid = get_shard_id_for_distribution_column('warehouse', w) WHERE p.nodename = '$vh' AND p.nodeport = $vp LIMIT 1")
[[ -n "$W" ]] || die "找不到放在 n$VICTIM（$vh:$vp）上的仓库"
# 写探针：对该节点上的一个仓做一次空更新（w_ytd 不变，一致性条件不受影响），2 s 超时
probe() { cpsql "$H" "$P" -qAt -c "SET statement_timeout = '2s'" -c "BEGIN" -c "SELECT tpcc.begin_global()" \
            -c "UPDATE warehouse SET w_ytd = w_ytd WHERE w_id = $W" -c "COMMIT" >/dev/null 2>&1; }
probe || die "压测前写探针就失败了"
log "受害节点 n$VICTIM = $vh:$vp（探针仓库 $W），路由 $(route)"
bash "$CLOUD_DIR/ha.sh" status | sed 's/^/    /'

cx "/work/pg-install/bin/pgbench -h $H -p $P -U postgres -n -c $CLIENTS -j $CLIENTS -T $T --max-tries=20 --failures-detailed -P 10 \
    -D nw=$WAREHOUSES -f $M/pgbench/new_order.sql@45 -f $M/pgbench/payment.sql@43 -f $M/pgbench/order_status.sql@4 \
    -f $M/pgbench/delivery.sql@4 -f $M/pgbench/stock_level.sql@4 postgres" > "$OUT/pgbench.out" 2> "$OUT/pgbench.err" &
pg=$!
sleep "$KILL_AT"
s_before=$(QN "$H" "$P" "SELECT sum(d_next_o_id) FROM district")
on "$vh" "docker kill $PREFIX-n$VICTIM >/dev/null"; t0=$(date +%s.%N)
log "已 kill n$VICTIM 主库（kill 前已确认的 sum(d_next_o_id) = $s_before）"
t_route=""; t_write=""
for _ in $(seq 1 120); do
  now=$(date +%s.%N)
  r=$(route); [[ -z "$t_route" && -n "$r" && "$r" != "$vh:$vp" ]] && { t_route=$(echo "$now - $t0" | bc); log "  路由改到 $r（kill 后 ${t_route}s）"; }
  if [[ -z "$t_write" ]] && probe; then t_write=$(echo "$(date +%s.%N) - $t0" | bc); log "  仓库 $W 可以写入（kill 后 ${t_write}s）"; fi
  [[ -n "$t_route" && -n "$t_write" ]] && break
  sleep 1
done
wait $pg
ab=$(grep -cE 'client [0-9]+ (script [0-9]+ )?aborted' "$OUT/pgbench.err")
log "压测结束：中止的客户端 $ab（kill 时连在受害节点上的事务会出错、客户端中止，属预期）"
grep -E '^progress' "$OUT/pgbench.err" | sed -E 's/progress: ([0-9.]+) s, ([0-9.]+) tps.*/\1s:\2/' | tr '\n' ' ' | sed 's/^/    每 10 s 的 tps：/'; echo

# 等 2PC 收尾：按控制面当前的节点地址逐个查 pg_prepared_xacts 与 DTX 未决登记
t1=$(date +%s); left="?"
for _ in $(seq 1 100); do
  left=0
  while IFS='|' read -r nh np; do
    a=$(QN "$nh" "$np" "SELECT count(*) FROM pg_prepared_xacts"); b=$(QN "$nh" "$np" "SELECT partdist.dtx_pending_count()")
    [[ "$a" =~ ^[0-9]+$ && "$b" =~ ^[0-9]+$ ]] || { left="?"; break; }
    left=$((left + a + b))
  done < <(QN "$H" "$P" "SELECT hostname || '|' || port FROM partdist.node_map ORDER BY node_id")
  [[ "$left" == 0 ]] && break; sleep 3
done
log "2PC 收尾：剩余 $left（用时 $(( $(date +%s) - t1 )) s）"
for _ in $(seq 1 60); do s_after=$(QN "$H" "$P" "SELECT sum(d_next_o_id) FROM district"); [[ "$s_after" =~ ^[0-9]+$ ]] && break; sleep 2; done
[[ "$s_after" -ge "$s_before" ]] && lost="无丢失" || lost="！！丢失 $((s_before - s_after)) 笔"
log "已确认提交：kill 前 $s_before，恢复后 $s_after → $lost"
for _ in 1 2 3 4 5; do   # 查询失败（分片暂不可服务）就重试；5 项 + 2 项都出结果才算跑成
  cx "source $M/scripts/lib.sh && QC -f $M/sql/90_consistency.sql" > "$OUT/std.txt" 2>&1
  cpsql "$H" "$P" -f "$M/cloud/tests/xnode_consistency.sql" > "$OUT/xnode.txt" 2>&1
  [[ $(grep -c violations "$OUT/std.txt") == 5 && $(grep -c violations "$OUT/xnode.txt") == 2 ]] && break; sleep 10
done
vs=$(grep -A2 'violations' "$OUT/std.txt" | grep -E '^ *[0-9]+ *$' | awk '{s+=$1} END{print s+0}')
vx=$(awk '/violations/{getline; getline; split($0, a, "|"); gsub(/ /, "", a[1]); s += a[1]} END{print s+0}' "$OUT/xnode.txt")
log "一致性：标准 $(grep -c violations "$OUT/std.txt") 项违例 $vs；跨节点不变式 $(grep -c violations "$OUT/xnode.txt") 项违例 $vx"

log "旧主隔离：把被 kill 的旧主容器拉起来，观察 40 s"
on "$vh" "docker start $PREFIX-n$VICTIM >/dev/null"
sleep 40
st=$(on "$vh" "docker inspect -f '{{.State.Status}}' $PREFIX-n$VICTIM")
w_old=$(on "$vh" "docker exec $PREFIX-n$VICTIM /work/pg-install/bin/psql -h 127.0.0.1 -p $vp -U postgres -qAt -c 'SELECT pg_is_in_recovery()'" 2>&1 | tail -1)
log "  旧主容器状态 = $st；能否连接：${w_old:-不能连接}；当前路由 $(route)"
on "$vh" "docker logs --tail 5 $PREFIX-n$VICTIM 2>&1; docker exec $PREFIX-n$VICTIM bash -c 'grep -hE \"被取代|接替|时间线|timeline\" \$(ls -t /d/pgdata/log/* | head -2) | tail -3' 2>/dev/null" | cut -c1-200 | sed 's/^/    /'
{
  echo "== ShardPG 自动切主（kill n$VICTIM 主库 @ ${KILL_AT}s，$CLIENTS 终端）"
  echo "路由切换 ${t_route:-超时} s；该节点上的仓可写 ${t_write:-超时} s；已确认提交 $lost；2PC 剩余 $left"
  echo "一致性违例：标准 $vs、跨节点 $vx；旧主拉起后状态 $st"
} | tee "$OUT/summary.txt"
