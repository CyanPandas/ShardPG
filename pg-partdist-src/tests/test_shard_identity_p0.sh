#!/usr/bin/env bash
# test_shard_identity_p0.sh
#
# P0 全局分片身份回归(阶段 4 / §11 前置)。
#
# 验证 partdist.shard_identity 及其解析函数:以 Citus shardid 作为跨节点一致的
# global_shard_id,把它映射到每个节点各自的本地分片表 (local_oid/relfilenode/relname),
# 使"一个分区 = 一个 Raft 组"的成员寻址与跨节点 applied_part_lsn 比较有良定义的键。
#
# 断言:
#   A. 覆盖:每个 worker 上 shard_identity 行数 == 该节点物理存在的 Citus 分片数。
#   B. 往返:对每个已注册分片,shard_global_id(local_oid)==global_shard_id 且
#            local_partition_for_shard(global_shard_id)==local_oid。
#   C. 跨节点:同一 reference-table 分片的 shardid 在所有 worker 相同,而各节点
#            local_partition_for_shard 返回本地非空且互不相同的 OID;协调节点/未承载
#            节点返回 NULL。
#   D. 剪枝:删除该 reference 表并 rebuild 后,其行从所有节点的 shard_identity 消失。
#
# 前提:pg-partdist-raft4-container 运行中,四节点均已加载 pg_partdist 且创建扩展
#       (扩展 SQL 已含 P0 段);Citus 分片已分布。
#
# 用法: bash test_shard_identity_p0.sh

set -uo pipefail

CONTAINER="${CONTAINER:-pg-partdist-raft4-container}"
PG_INSTALL="/work/pg-install"
COORD=5432
WORKERS=(5433 5434 5435)
ALL=(5432 5433 5434 5435)

P() { docker exec -u postgres "$CONTAINER" "${PG_INSTALL}/bin/psql" -tA -U postgres -d postgres -p "$1" "${@:2}"; }

PASS=0; FAIL=0
ok()   { echo "  PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

echo "== P0 shard identity regression =="

# 预置:确保各节点映射为最新
for p in "${ALL[@]}"; do P "$p" -c "SELECT partdist.rebuild_shard_identity();" >/dev/null; done

# ---- A. 覆盖 ----
echo "[A] coverage: shard_identity rows == locally-present shards"
for p in "${WORKERS[@]}"; do
  reg=$(P "$p" -c "SELECT count(*) FROM partdist.shard_identity;")
  # 独立计算本节点物理存在的分片数(to_regclass 不受分片隐藏影响,无需 visibility override)
  phys=$(P "$p" -c "SELECT count(*) FROM pg_catalog.pg_dist_shard s
     WHERE pg_catalog.to_regclass(pg_catalog.shard_name(s.logicalrelid,s.shardid)) IS NOT NULL;")
  if [ "$reg" = "$phys" ] && [ "$reg" -gt 0 ]; then ok "worker $p: $reg registered == $phys present";
  else bad "worker $p: registered=$reg present=$phys"; fi
done

# ---- B. 往返 ----
echo "[B] round-trip: shard_global_id / local_partition_for_shard invert"
for p in "${WORKERS[@]}"; do
  badn=$(P "$p" -c "SELECT count(*) FROM partdist.shard_identity si
      WHERE partdist.shard_global_id(si.local_oid) IS DISTINCT FROM si.global_shard_id
         OR partdist.local_partition_for_shard(si.global_shard_id) IS DISTINCT FROM si.local_oid;")
  if [ "$badn" = "0" ]; then ok "worker $p: all rows round-trip"; else bad "worker $p: $badn rows fail round-trip"; fi
done

# ---- C+D. 跨节点 + 剪枝(用 reference table:同一 shardid 复制到所有 worker)----
echo "[C] cross-node: reference-table shardid identical across workers, local OIDs differ"
P "$COORD" -c "SET citus.enable_ddl_propagation=on;
  DROP TABLE IF EXISTS partdist_p0_refdemo;
  CREATE TABLE partdist_p0_refdemo(id int primary key);
  SELECT create_reference_table('partdist_p0_refdemo');" >/dev/null 2>&1
REF=$(P "$COORD" -c "SELECT shardid FROM pg_catalog.pg_dist_shard WHERE logicalrelid='partdist_p0_refdemo'::regclass;")

if [ -z "$REF" ]; then
  bad "could not create reference table for cross-node check"
else
  for p in "${WORKERS[@]}"; do P "$p" -c "SELECT partdist.rebuild_shard_identity();" >/dev/null; done
  oids=""; distinct_ok=1; present_ok=1
  for p in "${WORKERS[@]}"; do
    o=$(P "$p" -c "SELECT partdist.local_partition_for_shard($REF);")
    [ -z "$o" ] && present_ok=0
    case " $oids " in *" $o "*) distinct_ok=0;; esac
    oids="$oids $o"
  done
  coord_null=$(P "$COORD" -c "SELECT coalesce(partdist.local_partition_for_shard($REF)::text,'NULL');")
  [ "$present_ok" = "1" ] && ok "shardid $REF present on all workers (local OIDs:$oids)" || bad "shardid $REF missing on some worker (oids:$oids)"
  [ "$distinct_ok" = "1" ] && ok "per-node local OIDs are distinct (node-specific identity)" || bad "local OIDs not distinct:$oids"
  [ "$coord_null" = "NULL" ] && ok "coordinator does not host the shard (NULL)" || bad "coordinator unexpectedly returned $coord_null"

  echo "[D] prune: after dropping the table + rebuild, its rows vanish"
  P "$COORD" -c "SET citus.enable_ddl_propagation=on; DROP TABLE partdist_p0_refdemo;" >/dev/null 2>&1
  pruned_ok=1
  for p in "${WORKERS[@]}"; do
    P "$p" -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
    left=$(P "$p" -c "SELECT count(*) FROM partdist.shard_identity WHERE global_shard_id=$REF;")
    [ "$left" = "0" ] || pruned_ok=0
  done
  [ "$pruned_ok" = "1" ] && ok "demo shard pruned from all workers" || bad "demo shard rows survived prune"
fi

echo
echo "== P0 结果: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" = "0" ] && { echo "P0 shard identity 回归: PASS"; exit 0; } || { echo "P0 shard identity 回归: FAIL"; exit 1; }
