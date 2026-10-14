#!/usr/bin/env bash
# 建库：模式 → 生成函数 → 分布 + 建组 + 打标 → 装载 → ANALYZE → 核对行数。
# 顺序不能换（见 sql/03_distribute.sql 文件头）。整个过程约数十分钟，取决于机器。
source "$(dirname "$0")/lib.sh"
S="$TPCC_DIR/sql"

log "① 模式"
QC -q -f "$S/01_schema.sql" || die "建模式失败"
log "② 生成函数 / 装载函数 / 事务函数"
for f in 02_gen_funcs.sql 04_load_funcs.sql 05_txn_funcs.sql; do
  QC -q -f "$S/$f" || die "$f 失败"
done
log "③ 分布 + 建 Raft 组 + 供副本 + 打标（shard_count=$SHARD_COUNT replicas=$REPLICAS）"
QC -v shard_count="$SHARD_COUNT" -v replicas="$REPLICAS" -f "$S/03_distribute.sql" || die "接入失败"
if [[ "${DELEGATE:-1}" == 1 && -f "$S/06_delegate.sql" ]]; then
  log "③b 事务函数下推到 worker（create_distributed_function，按仓库号共置）"
  QC -q -f "$S/06_delegate.sql" || die "函数下推失败"
fi

log "④ 装载 $WAREHOUSES 个仓库（并行 $LOAD_PARALLEL）"
bash "$TPCC_DIR/scripts/load.sh" || die "装载失败"

log "⑤ ANALYZE（分片表 T2.6 起允许 ANALYZE；VACUUM 一律被拒，不要跑）"
QC -q -c "ANALYZE warehouse, district, customer, history, oorder, new_order, order_line, stock, item;" \
  || die "ANALYZE 失败"

log "⑥ 行数核对"
W=$WAREHOUSES
check() { local name=$1 sql=$2 want=$3 got
  got=$(QV "$COORD_HOST" "$COORD_PORT" "$sql")
  if [[ "$got" == "$want" ]]; then echo "  ok   $name = $got"; else echo "  FAIL $name = $got（应为 $want）"; return 1; fi; }
bad=0
check warehouse  "SELECT count(*) FROM warehouse"  "$W"              || bad=1
check district   "SELECT count(*) FROM district"   "$((W*10))"       || bad=1
check customer   "SELECT count(*) FROM customer"   "$((W*30000))"    || bad=1
check history    "SELECT count(*) FROM history"    "$((W*30000))"    || bad=1
check oorder     "SELECT count(*) FROM oorder"     "$((W*30000))"    || bad=1
check new_order  "SELECT count(*) FROM new_order"  "$((W*9000))"     || bad=1
check stock      "SELECT count(*) FROM stock"      "$((W*100000))"   || bad=1
check item       "SELECT count(*) FROM item"       "100000"          || bad=1
check order_line "SELECT (SELECT count(*) FROM order_line) = (SELECT sum(o_ol_cnt) FROM oorder)" "t" || bad=1
[[ $bad -eq 0 ]] || die "装载后行数不对"
log "一致性基线（装载后必须全 0）"
QC -f "$S/90_consistency.sql"
log "建库完成"
