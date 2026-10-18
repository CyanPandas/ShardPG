#!/usr/bin/env bash
# ============================================================================
# [宿主机] ha_demo_reset.sh —— 高可用演示（tpcc_ha_demo.sql）收尾：去掉第 6 步加的 worker，集群回到原来的节点数，
# 便于重复演示。在第 7 步删表之后执行。
#
#   bash pg-partdist-src/demo/ha_demo_reset.sh            # 去掉 :5437
#   bash pg-partdist-src/demo/ha_demo_reset.sh 5438       # 去掉别的端口
#
# 步骤：核对它上面已经没有任何分片副本（没有就继续）→ citus_remove_node → 停掉它 → 各节点 node_map 删行
#       → 整簇停机 → 各节点 postgresql.auto.conf 里去掉第 6 步写进去的 pg_raft.peers（回到 postgresql.conf 里的原值）
#       → 整簇启动 → 核对 → 删它的数据目录。
# 为什么要整簇重启：pg_raft.peers 只许热加载**追加**（复制游标按槽位存，删一个后面的全错位），删节点只能在启动时换表。
# master 的 TSO 发号上限现在落盘（pg_tso_hwm，P7-N101），有数据也能重启，不会再从 1 发号。
# ============================================================================
set -euo pipefail
C="${CONTAINER:-pg-test-container}"
PORT="${1:-5437}"
BIN=/work/pg-install/bin
R=/work/pg-cluster-data
q()  { docker exec -u postgres "$C" $BIN/psql -h /tmp -p "$1" -U postgres -X -At -v ON_ERROR_STOP=1 -c "$2" </dev/null; }
say() { printf '\n== %s\n' "$*"; }

NODE=$(q 5432 "SELECT node_id FROM partdist.node_map WHERE port = $PORT")
[[ -n "$NODE" ]] || { echo "node_map 里没有端口 $PORT 的节点，不用还原"; exit 0; }
say "去掉节点 $NODE（:$PORT）"

n=$(q 5432 "SELECT count(*) FROM partdist.partition_map WHERE $NODE = ANY (ARRAY[primary_node] || secondary_nodes)")
if [[ "$n" != 0 ]]; then
  echo "还有 $n 个分片的副本在 :$PORT 上，先删表（第 7 步），或用 shardpg.move_replica 把副本迁走"; exit 1
fi
DATA=$(q "$PORT" "SHOW data_directory" 2>/dev/null || true)
[[ -n "$DATA" ]] || DATA="$R/worker$((NODE - 1))"

say "Citus 移除 worker"
if [[ "$(q 5432 "SELECT count(*) FROM pg_dist_node WHERE nodeport = $PORT")" != 0 ]]; then
  host=$(q 5432 "SELECT nodename FROM pg_dist_node WHERE nodeport = $PORT")
  q 5432 "SELECT citus_remove_node('$host', $PORT)" >/dev/null
fi
echo "ok"

# 先删 node_map 行、再停它：控制面探测按 node_map 逐台探活，先停的话它会登记一条"节点 N down"，
# apply 时把刚删的行（以占位地址）又插回来。重启之后再删一遍兜底
say "各节点 node_map 删掉节点 $NODE，停掉 :$PORT"
PORTS=$(q 5432 "SELECT string_agg(port::text, ' ' ORDER BY node_id) FROM partdist.node_map WHERE node_id <> $NODE")
for p in $PORTS; do q "$p" "DELETE FROM partdist.node_map WHERE node_id = $NODE" >/dev/null; done
sleep 4
docker exec -u postgres "$C" $BIN/pg_ctl -D "$DATA" -m fast stop -w -t 60 >/dev/null 2>&1 || true
echo "ok（剩下 :${PORTS// /, :}）"

say "整簇停机 → 各节点去掉 postgresql.auto.conf 里的 pg_raft.peers → 整簇启动"
DIRS=""
for p in $PORTS; do DIRS+=" $(q "$p" "SHOW data_directory")"; done
for d in $DIRS; do [[ "$d" == "$R/coordinator" ]] || docker exec -u postgres "$C" $BIN/pg_ctl -D "$d" -m fast stop -w -t 90 >/dev/null & done; wait
docker exec -u postgres "$C" $BIN/pg_ctl -D "$R/coordinator" -m fast stop -w -t 90 >/dev/null
for d in $DIRS; do docker exec -u postgres "$C" sed -i -E '/^pg_raft\.peers *=/d' "$d/postgresql.auto.conf"; done
docker exec -u postgres "$C" $BIN/pg_ctl -D "$R/coordinator" -l "$R/coordinator/pg.log" start -w -t 90 >/dev/null
for d in $DIRS; do [[ "$d" == "$R/coordinator" ]] || docker exec -u postgres "$C" $BIN/pg_ctl -D "$d" -l "$d/pg.log" start -w -t 90 >/dev/null; done

say "核对"
want=$(q 5432 "SHOW pg_raft.peers")
bad=0
for p in $PORTS; do
  for t in $(seq 1 30); do
    line=$(q "$p" "SELECT current_setting('pg_raft.peers') || ' | 组 0 成员 ' || cluster_size || ' ' || state FROM partdist.pg_raft_group_status() WHERE group_id = 0" 2>/dev/null || true)
    [[ "$line" == "$want | 组 0 成员 "* ]] && break
    sleep 2
  done
  echo ":$p  $line"
  [[ "$line" == "$want | 组 0 成员 "* ]] || bad=1
done
[[ $bad == 0 ]] || { echo "有节点没回到原来的 pg_raft.peers，查各节点日志"; exit 1; }
for p in $PORTS; do q "$p" "DELETE FROM partdist.node_map WHERE node_id = $NODE" >/dev/null; done
echo "node_map：$(q 5432 "SELECT count(*) FROM partdist.node_map") 行"
echo "TSO：$(q 5432 "SELECT partdist.partdist_tso_status()" | cut -d' ' -f1-5)"

say "删掉 :$PORT 的数据目录 $DATA"
docker exec -u postgres "$C" rm -rf "$DATA"
echo "完成：集群回到 $(wc -w <<<"$PORTS") 个节点；从 tpcc_demo.sql 第 1 步重新建表即可再演"
