#!/usr/bin/env bash
# 正式压测：预热 RAMPUP_S + 测量 DURATION_S，TERMINALS 个终端，标准事务配比
# （NewOrder 45 / Payment 43 / OrderStatus 4 / Delivery 4 / StockLevel 4），无键入/思考时间。
#
# tpmC 不从 pgbench 的事务数推：NewOrder 有 1% 是零写入的无效商品回滚，而
# 重试过的事务在 pgbench 里只算一次 —— 直接取测量窗口两端 sum(d_next_o_id) 的差，
# 恰好等于窗口内提交成功的 NewOrder 数。
#
# 用法：bash run.sh [标签]   结果写到 $RESULT_ROOT/<时间>_<标签>/
source "$(dirname "$0")/lib.sh"
TAG=${1:-tpcc}
OUT="$RESULT_ROOT/$(date '+%Y%m%d-%H%M%S')_${TAG}"
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
PB="$TPCC_DIR/pgbench"

log "结果目录 $OUT"
cp "$ENV_FILE" "$OUT/cluster.env"
for i in "${!NODE_IDS[@]}"; do
  QV "${NODE_HOSTS[$i]}" "${NODE_PORTS[$i]}" \
     "SELECT name, setting FROM pg_settings WHERE name ~ '^(pg_raft|pg_partdist|citus)\.' OR name IN
        ('max_connections','max_prepared_transactions','shared_buffers','synchronous_commit','work_mem')
      ORDER BY 1" > "$OUT/settings_node${NODE_IDS[$i]}.tsv" 2>&1
done
QC -f "$TPCC_DIR/sql/90_consistency.sql" > "$OUT/consistency_before.txt" 2>&1

log "起监控（每 60 s）"
bash "$TPCC_DIR/scripts/monitor.sh" "$OUT" 60 &
MON=$!
trap 'kill $MON 2>/dev/null' EXIT

TOTAL=$((RAMPUP_S + DURATION_S))
log "起 pgbench：$TERMINALS 终端、$WAREHOUSES 仓、总时长 ${TOTAL}s（预热 ${RAMPUP_S}s）"
"$PGBENCH_BIN" -h "$COORD_HOST" -p "$COORD_PORT" -U "$DBUSER" -n \
  -c "$TERMINALS" -j "$TERMINALS" -T "$TOTAL" -P 60 \
  --max-tries="$MAX_TRIES" --failures-detailed \
  -D nw="$WAREHOUSES" \
  -f "$PB/new_order.sql@45" -f "$PB/payment.sql@43" -f "$PB/order_status.sql@4" \
  -f "$PB/delivery.sql@4" -f "$PB/stock_level.sql@4" \
  -l --aggregate-interval=60 --log-prefix="$OUT/pgbench_agg" \
  "$DB" > "$OUT/pgbench.out" 2> "$OUT/pgbench.err" &
PGB=$!

# 取数失败（如可重试的串行化错误）时重试，不能拿空值去算 tpmC
snap() { local v i; for i in 1 2 3 4 5 6 7 8 9 10; do
  v=$(QV "$COORD_HOST" "$COORD_PORT" "SELECT sum(d_next_o_id) FROM district" 2>/dev/null)
  [[ "$v" =~ ^[0-9]+$ ]] && { echo "$v"; return 0; }; sleep 1; done
  echo "FATAL: sum(d_next_o_id) 连续 10 次取不到" >&2; echo 0; }
sleep "$RAMPUP_S"
S1=$(snap); T1=$(date +%s)
log "预热结束：sum(d_next_o_id)=$S1，开始计成绩"
sleep "$DURATION_S"
S2=$(snap); T2=$(date +%s)
log "测量窗口结束：sum(d_next_o_id)=$S2"
wait "$PGB"; PGB_RC=$?
kill "$MON" 2>/dev/null

MIN=$(awk -v a="$T1" -v b="$T2" 'BEGIN{printf "%.2f", (b-a)/60}')
TPMC=$(awk -v a="$S1" -v b="$S2" -v m="$MIN" 'BEGIN{printf "%.1f", (b-a)/m}')
QC -f "$TPCC_DIR/sql/90_consistency.sql" > "$OUT/consistency_after.txt" 2>&1
VIOL=$(grep -A2 'violations' "$OUT/consistency_after.txt" | grep -E '^ *[0-9]+ *$' | awk '{s+=$1} END{print s+0}')
ABORTED=$(grep -c 'client [0-9]* aborted' "$OUT/pgbench.err" || true)

{
  echo "== ShardPG TPC-C 结果 =="
  echo "仓库 $WAREHOUSES  终端 $TERMINALS  预热 ${RAMPUP_S}s  测量 ${DURATION_S}s（实测 ${MIN} min）"
  echo "tpmC（测量窗口内提交成功的 NewOrder / 分钟）= $TPMC"
  echo "pgbench 退出码 = $PGB_RC   中途中止的终端数 = $ABORTED（>0 说明出现了不可重试的错误，见 pgbench.err）"
  echo "一致性条件违反总数 = $VIOL（应为 0，见 consistency_after.txt）"
  echo
  echo "== pgbench 汇总（各脚本的事务数、延迟、重试与失败）=="
  sed -n '/^transaction type/,$p' "$OUT/pgbench.out"
} | tee "$OUT/summary.txt"
