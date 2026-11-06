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
# 打乱顺序：按仓库顺序排时，xargs 同时取到的几块几乎都属于同一个仓库 ⇒ 同一组分片。
# 多副本下每块提交都要占该分片组的复制认领位，同组的块只能一个个过，并行度退化成 1。
# 块之间没有依赖（无外键，区的 d_next_o_id 由 load_warehouse 直接写定），打乱是安全的。
shuf -o "$tasks" "$tasks"

n=$(wc -l < "$tasks")
log "$n 个装载块"
export PSQL_BIN COORD_HOST COORD_PORT DBUSER DB
errs=$(mktemp -d)
trap 'rm -f "$tasks"; rm -rf "$errs"' EXIT
export errs
fails=$(xargs -P "$LOAD_PARALLEL" -I{} bash -c \
  'f="$errs/$(echo "{}" | md5sum | cut -c1-12)";
   "$PSQL_BIN" -h "$COORD_HOST" -p "$COORD_PORT" -U "$DBUSER" -d "$DB" -X -q -v ON_ERROR_STOP=1 -c "{}" >/dev/null 2>"$f" \
     || echo "FAILED: {} —— $(grep -m1 ERROR "$f")"' \
  < "$tasks")
# 并行块撞同一分片时，大 COPY 全程占着该组复制认领位，后到者等满 60 s 报
# "等待组 N 的复制认领位超过 60000 ms"。每块是一个事务、失败即整块回滚，
# 所以失败的块串行重试（不再与别的块并发）即可。
for attempt in 1 2 3; do
  [[ -z "$fails" ]] && break
  log "第 $attempt 轮串行重试 $(echo "$fails" | wc -l) 个失败块"
  retry=$(echo "$fails" | sed -E 's/^FAILED: (.*) —— .*$/\1/')
  fails=""
  while IFS= read -r t; do
    [[ -z "$t" ]] && continue
    f="$errs/retry"
    "$PSQL_BIN" -h "$COORD_HOST" -p "$COORD_PORT" -U "$DBUSER" -d "$DB" -X -q -v ON_ERROR_STOP=1 -c "$t" >/dev/null 2>"$f" \
      || fails+="FAILED: $t —— $(grep -m1 ERROR "$f")"$'\n'
  done <<< "$retry"
  fails=${fails%$'\n'}
done
if [[ -n "$fails" ]]; then
  echo "$fails" | head -5
  echo "（共 $(echo "$fails" | wc -l) 块失败，上面只列前 5 条）"
  die "有装载块失败。整库重建：重新跑 build_db.sh"
fi
log "装载完成"
