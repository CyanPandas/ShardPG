#!/usr/bin/env bash
# 长跑监控：每 INTERVAL 秒采一轮，写进结果目录的四个 TSV。
#   progress.tsv  时刻、累计 NewOrder 数（sum(d_next_o_id)）、history 行数
#   nodes.tsv     每节点：捕获环（未消费/累计覆盖/背压次数/待修复分叉）、未决 DTX 数、连接数
#   shards.tsv    每 worker 上每个分片表：大小、是否打标、分片 xid 龄、分叉原因
#   tables.tsv    每张分布表的总大小（含索引、TOAST）
# 用法：bash monitor.sh <结果目录> [INTERVAL]；run.sh 会自动在后台起它。
source "$(dirname "$0")/lib.sh"
OUT=${1:?结果目录}; INTERVAL=${2:-60}
mkdir -p "$OUT"
[[ -f "$OUT/progress.tsv" ]] || echo -e "ts\tneworders\thistory" > "$OUT/progress.tsv"
[[ -f "$OUT/nodes.tsv" ]]    || echo -e "ts\tnode\tring_unconsumed\tring_overwrites\tbackpressure\tdiverged_pending\tdtx_pending\tconnections" > "$OUT/nodes.tsv"
[[ -f "$OUT/shards.tsv" ]]   || echo -e "ts\tnode\tshard\tbytes\tmvcc_status\txid_age\tdivergence" > "$OUT/shards.tsv"
[[ -f "$OUT/tables.tsv" ]]   || echo -e "ts\ttable\tbytes" > "$OUT/tables.tsv"

SHARD_RE='^(warehouse|district|customer|history|oorder|new_order|order_line|stock)_[0-9]+$'

while :; do
  ts=$(date '+%F %T')
  QV "$COORD_HOST" "$COORD_PORT" \
     "SELECT '$ts', (SELECT sum(d_next_o_id) FROM district), (SELECT count(*) FROM history)" >> "$OUT/progress.tsv" 2>>"$OUT/monitor.err"
  QV "$COORD_HOST" "$COORD_PORT" \
     "SELECT '$ts', logicalrelid::text, citus_total_relation_size(logicalrelid)
        FROM pg_dist_partition WHERE partmethod <> 'n' ORDER BY 2" >> "$OUT/tables.tsv" 2>>"$OUT/monitor.err"
  for i in "${!NODE_IDS[@]}"; do
    h=${NODE_HOSTS[$i]}; p=${NODE_PORTS[$i]}; n="node${NODE_IDS[$i]}"
    QV "$h" "$p" \
       "SELECT '$ts', '$n', r.unconsumed, r.overwrites, r.backpressure_flushes, r.diverged_pending,
               partdist.dtx_pending_count(), (SELECT count(*) FROM pg_stat_activity)
          FROM partdist.partwal_ring_stats() r" >> "$OUT/nodes.tsv" 2>>"$OUT/monitor.err" || echo -e "$ts\t$n\tUNREACHABLE" >> "$OUT/nodes.tsv"
    [[ "${NODE_ROLES[$i]}" == worker ]] || continue
    # worker 上 Citus 默认对非 Citus 会话隐藏分片表，这里关掉
    "$PSQL_BIN" -h "$h" -p "$p" -U "$DBUSER" -d "$DB" -X -qAt -F $'\t' \
      -c "SET citus.override_table_visibility = off" \
      -c "SELECT '$ts', '$n', c.relname, pg_total_relation_size(c.oid),
                 partdist.shard_mvcc_status(c.oid), (partdist.shard_xid_age(c.oid)).age,
                 coalesce(partdist.shard_divergence(c.oid), '')
            FROM pg_class c
           WHERE c.relkind = 'r' AND c.relname ~ '$SHARD_RE'
           ORDER BY c.relname" >> "$OUT/shards.tsv" 2>>"$OUT/monitor.err"
  done
  sleep "$INTERVAL"
done
