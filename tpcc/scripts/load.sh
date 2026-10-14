#!/usr/bin/env bash
# 装载：把每个仓库拆成小块（仓库+区、stock 每 2 万行、每区客户、每区订单），
# 每块一个事务，xargs 并行。失败的块打印出来并整体返回非零。
source "$(dirname "$0")/lib.sh"

tasks=$(mktemp)
trap 'rm -f "$tasks"' EXIT
for w in $(seq 1 "$WAREHOUSES"); do
  echo "SELECT tpcc.load_warehouse($w)"
  for lo in 1 20001 40001 60001 80001; do
    echo "SELECT tpcc.load_stock($w, $lo, $((lo+19999)))"
  done
  for d in $(seq 1 10); do
    echo "SELECT tpcc.load_customer($w, $d)"
    echo "SELECT tpcc.load_orders($w, $d)"
  done
done > "$tasks"

n=$(wc -l < "$tasks")
log "$n 个装载块"
export PSQL_BIN COORD_HOST COORD_PORT DBUSER DB
fails=$(xargs -P "$LOAD_PARALLEL" -I{} bash -c \
  '"$PSQL_BIN" -h "$COORD_HOST" -p "$COORD_PORT" -U "$DBUSER" -d "$DB" -X -q -v ON_ERROR_STOP=1 -c "{}" >/dev/null 2>&1 || echo "FAILED: {}"' \
  < "$tasks")
if [[ -n "$fails" ]]; then
  echo "$fails"
  die "有装载块失败（见上）。整库重建：重新跑 build_db.sh"
fi
log "装载完成"
