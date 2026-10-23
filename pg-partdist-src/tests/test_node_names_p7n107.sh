#!/usr/bin/env bash
# ============================================================================
# [宿主机] P7-N107 验收（2026-10-19）：\dt / shardpg.shards 显示节点名，不再显示 :5433 这种端口
#
# 规则：节点号 1 是 master，其余 worker<节点号-1>，与 worker 的数据目录名一致（new_worker_node.sh 同一规则）。
#
#   [1] shardpg.nodes：名字、地址对照；worker 名字 = 该节点数据目录名
#   [2] shardpg.node_name()：每台节点上取到的就是自己的名字
#   [3] SHARD KEY 建表后 \dt：List of shards 每行的 Primary / Replicas 是节点名及可选 (down) 状态，与控制面登记一致
#   [4] 分布式查询里 shardpg.node_name() 在实际执行的 worker 上求值，等于该分片的主
#   [5] 没有主从登记的老 Citus 表：分片的 Primary 也按地址换成节点名
#   [6] 删表零残留
#
# 前提：pg-test 1c+4w，psql 补丁 0017 与 pg_partdist SQL 已部署。约 1–2 分钟。
# 用法：bash test_node_names_p7n107.sh
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
PASS=0; FAIL=0
q()   { docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p "${2:-5432}" -U postgres -X -At -c "$1" </dev/null 2>&1; }
run() { docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -q -c "$1" </dev/null 2>&1; }
dt()  { docker exec -u postgres -e PGCLIENTENCODING=UTF8 "$C" $BIN/psql -h /tmp -p 5432 -U postgres -X -A -F'|' -c "\\dt $1" </dev/null 2>&1; }
ok()  { echo "  ✓ $1"; PASS=$((PASS + 1)); }
no()  { echo "  ✗ $1"; FAIL=$((FAIL + 1)); }
check() { if [[ "$2" == "$3" ]]; then ok "$1（$2）"; else no "$1：实际 [$2]，期望 [$3]"; fi; }
cleanup() { run "DROP TABLE IF EXISTS t107" >/dev/null; }
trap cleanup EXIT

echo "================ [0] 前提 ================"
cleanup
check "Citus worker 数" "$(q "SELECT count(*) FROM pg_dist_node WHERE groupid <> 0")" "4"

echo "================ [1] shardpg.nodes ================"
check "名字对照" "$(q "SELECT string_agg(name || '=' || port, ' ' ORDER BY node_id) FROM shardpg.nodes")" \
      "master=5432 worker1=5433 worker2=5434 worker3=5435 worker4=5436"
bad=0
for p in 5433 5434 5435 5436; do
  dir=$(q "SHOW data_directory" "$p"); name=$(q "SELECT name FROM shardpg.nodes WHERE port = $p")
  [[ "${dir##*/}" == "$name" ]] || { bad=1; echo "      :$p 数据目录 $dir，名字 $name"; }
done
check "worker 名字 = 数据目录名" "$bad" "0"

echo "================ [2] shardpg.node_name() ================"
got=""
for p in 5432 5433 5434 5435 5436; do got="$got $(q "SELECT shardpg.node_name()" "$p")"; done
check "各节点取到自己的名字" "${got# }" "master worker1 worker2 worker3 worker4"

echo "================ [3] 建表后 \\dt 显示节点名 ================"
out=$(run "CREATE TABLE t107 (w int NOT NULL, id int NOT NULL, PRIMARY KEY (w, id), SHARD KEY (w) SHARDS 4 REPLICAS 2)")
check "t107 建好" "$(q "SELECT count(*) FROM shardpg.shards WHERE table_name = 't107'::regclass AND \"primary\" IS NOT NULL")" "4"
out=$(dt t107)
# List of shards 一行：Schema|Name|Table|Hash range|Primary|Replicas（2026-10-19 起分片单独一张表）
rows=$(grep -E '^public\|t107_[0-9]+\|t107\|' <<<"$out")
check "4 个分片行" "$(grep -c . <<<"$rows")" "4"
check "主、从都是节点名（没有 :端口）" \
      "$(awk -F'|' '$5 ~ /^worker[0-9]+( \(down\))?$/ && $6 ~ /^worker[0-9]+( \(down\))?,worker[0-9]+( \(down\))?$/' <<<"$rows" | grep -c .)" "4"
# \dt 每行的 分片名|主|从 与控制面登记逐行比对
want=$(q "SELECT string_agg(shard_name(s.logicalrelid, s.shardid) || '|' || (SELECT shardpg.node_name(n.node_id) || CASE WHEN n.status <> 'active' THEN ' (down)' ELSE '' END FROM partdist.node_map n WHERE n.node_id=m.primary_node) || '|' ||
                            (SELECT string_agg(shardpg.node_name(n.node_id) || CASE WHEN n.status <> 'active' THEN ' (down)' ELSE '' END, ',' ORDER BY n.node_id) FROM partdist.node_map n WHERE n.node_id=ANY(m.secondary_nodes)), ' ' ORDER BY s.shardid)
            FROM pg_dist_shard s JOIN partdist.partition_map m ON m.partition_id::bigint = s.shardid
           WHERE s.logicalrelid = 't107'::regclass")
have=$(awk -F'|' '{ printf "%s%s|%s|%s", (NR > 1 ? " " : ""), $2, $5, $6 }' <<<"$rows")
check "\\dt 与控制面登记一致" "$have" "$want"
check "shardpg.shards 与 \\dt 一致" \
      "$(q "SELECT string_agg(shard_name || '|' || \"primary\" || '|' || replicas, ' ' ORDER BY shard_name) FROM shardpg.shards WHERE table_name = 't107'::regclass")" "${have// (down)/}"

echo "================ [4] 分布式查询里的 shardpg.node_name() ================"
run "INSERT INTO t107 SELECT w, i FROM generate_series(1, 8) w, generate_series(1, 2) i" >/dev/null
res=$(q "SELECT count(*) FILTER (WHERE x.worker = sh.\"primary\") || '/' || count(*)
           FROM (SELECT tableoid::regclass::text AS shard, shardpg.node_name() AS worker FROM t107 GROUP BY 1, 2) x
           JOIN shardpg.shards sh ON sh.shard_name = x.shard")
[[ "${res%/*}" == "${res#*/}" && "${res#*/}" -ge 3 ]] 2>/dev/null \
  && ok "每个分片：实际执行的 worker = 登记的主（$res 个分片）" || no "实际执行的 worker 与登记的主不符：$res"

echo "================ [5] 没有主从登记的老 Citus 表 ================"
legacy=$(q "SELECT p.logicalrelid::text FROM pg_dist_partition p WHERE p.partmethod = 'h' AND NOT EXISTS (SELECT 1 FROM pg_dist_shard s JOIN partdist.partition_map m ON m.partition_id::bigint = s.shardid WHERE s.logicalrelid = p.logicalrelid) ORDER BY 1 LIMIT 1")
if [[ -n "$legacy" ]]; then
  lrows=$(dt "$legacy" | grep -E "^public\\|${legacy}_[0-9]+\\|${legacy}\\|")
  check "$legacy 有分片行" "$([[ -n "$lrows" ]] && echo yes)" "yes"
  check "$legacy 的分片主都是节点名" "$(awk -F'|' '$5 !~ /^worker[0-9]+$/' <<<"$lrows" | grep -c .)" "0"
else
  echo "  - 环境里没有不带 Raft 登记的哈希分布表，跳过"
fi

echo "================ [6] 删表 ================"
ids=$(q "SELECT string_agg(shardid::text, ',') FROM pg_dist_shard WHERE logicalrelid = 't107'::regclass")
run "DROP TABLE t107" >/dev/null
check "控制面登记清掉" "$(q "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN ($ids)")" "0"
bad=0
for p in 5433 5434 5435 5436; do
  n=$(q "SET citus.override_table_visibility = off; SELECT count(*) FROM pg_class WHERE relname ~ '^t107'" "$p" | tail -1)
  [[ "$n" == 0 ]] || { bad=1; echo "      :$p 还有 $n 张 t107 表"; }
done
check "4 台 worker 上零残留" "$bad" "0"

echo
echo "================ 结果：$PASS 通过 / $FAIL 失败 ================"
[[ $FAIL -eq 0 ]]
