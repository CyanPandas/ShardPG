#!/usr/bin/env bash
# 跨节点读隔离：并发跨节点转账（80%）+ 一条语句读全表余额总和（20%）。转账不改总额，
# 所以每次读到的总和都应是 7,200,000；读到别的值 = 读到了"转了一半"的状态（某个节点上已提交、另一个节点上还没有）。
# 用法（客户端机上）：bash cloud/tests/test_isolation.sh <shardpg|citus> [并发数=24] [秒数=120]
source "$(dirname "$0")/../lib.sh"
SYS=${1:?shardpg|citus}; CLIENTS=${2:-24}; T=${3:-120}   # 不能叫 C：cloud.env 里 C 是第三台机器的 IP
case $SYS in
  shardpg) DIR=$TPCC_DIR;  MNT=/t;  FLAG=1 ;;
  citus)   DIR=$CITUS_DIR; MNT=/tc; FLAG=0 ;;
  *) die "第一个参数是 shardpg 或 citus" ;;
esac
H=$COORD_NODE_HOST; P=$COORD_PORT
ensure_client
cpsql "$H" "$P" -qAt -c "SELECT 1" >/dev/null || die "连不上 $SYS 协调者 $H:$P"
tbl=iso_$(date +%H%M%S)
OUT="$DIR/results/$(date +%Y%m%d-%H%M%S)_${SYS}-isolation"; mkdir -p "$OUT"; M=${OUT/#$DIR/$MNT}
cp "$CLOUD_DIR/tests/iso_setup.sql" "$OUT/"
sed -e "s/@TBL@/$tbl/g" "$CLOUD_DIR/tests/iso_xfer.sql" > "$OUT/xfer.sql"
sed -e "s/@TBL@/$tbl/g" -e "s#@BADLOG@#$M/bad.log#g" "$CLOUD_DIR/tests/iso_reader.sql" > "$OUT/reader.sql"

log "$SYS：建测试表 $tbl（72 行、总额 7,200,000）"
cpsql "$H" "$P" -q -v tbl="$tbl" -v shardpg=$FLAG -f "$M/iso_setup.sql" > "$OUT/setup.log" 2>&1 || { tail -5 "$OUT/setup.log"; die "建表失败"; }
log "$SYS：$CLIENTS 个并发、$T 秒（转账 80% + 读总和 20%）"
cx "/work/pg-install/bin/pgbench -h $H -p $P -U postgres -n -c $CLIENTS -j $CLIENTS -T $T --max-tries=20 --failures-detailed \
    -f $M/xfer.sql@8 -f $M/reader.sql@2 postgres" > "$OUT/pgbench.out" 2> "$OUT/pgbench.err"
rc=$?
reads=$(awk '/SQL script 2:/{f=1} f && /- [0-9]+ transactions/{print $2; exit}' "$OUT/pgbench.out")
xfers=$(awk '/SQL script 1:/{f=1} f && /- [0-9]+ transactions/{print $2; exit}' "$OUT/pgbench.out")
bad=0; [[ -f "$OUT/bad.log" ]] && bad=$(wc -l < "$OUT/bad.log")
final=$(cpsql "$H" "$P" -qAt -c "BEGIN" -c "SELECT tpcc.begin_global()" -c "SELECT sum(bal) FROM $tbl" -c "COMMIT" | grep -E '^[0-9]+$' | tail -1)
aborted=$(grep -cE 'client [0-9]+ (script [0-9]+ )?aborted' "$OUT/pgbench.err")
{
  echo "== $SYS 跨节点读隔离（表 $tbl，$CLIENTS 并发，$T 秒）"
  echo "pgbench 退出码 = $rc   中止的客户端 = $aborted"
  echo "转账提交 = ${xfers:-?}   读总和 = ${reads:-?}   读到总和 ≠ 7,200,000 的次数 = $bad"
  [[ "$bad" -gt 0 ]] && echo "读到的异常总和（前 10 个）：$(head -10 "$OUT/bad.log" | tr '\n' ' ')"
  echo "结束后静止状态的总额 = ${final:-?}（应为 7200000）"
  sed -n '/^transaction type/,/^latency average/p' "$OUT/pgbench.out" | grep -E "processed|failed|retried|latency average"
} | tee "$OUT/summary.txt"
