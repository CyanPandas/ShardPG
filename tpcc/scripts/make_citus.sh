#!/usr/bin/env bash
# 由本套件生成原版 Citus 对照套件：模式 / 生成函数 / 装载函数 / 事务函数 / 函数下推（06_delegate.sql）/
# pgbench 脚本与本套件逐字节相同，只把 03_distribute.sql 换成"纯 Citus 共置分布 + begin_global 空操作"
# （无 Raft 组 / 打标），副本数固定 1（shard_replication_factor = 1）。item 的灌法与各表的
# create_distributed_table 从本套件的 03_distribute.sql 原样摘出，迷你规模也跟着对。
# 用法：bash scripts/make_citus.sh <目标目录>；目标目录里的 cluster.env 另写（拓扑同 ShardPG，DELEGATE=1）。
set -euo pipefail
SRC="$(cd "$(dirname "$0")/.." && pwd)"
DST=${1:?目标目录}
mkdir -p "$DST"
DST=$(cd "$DST" && pwd)
[[ "$DST" != "$SRC" ]] || { echo "目标目录不能是套件本身" >&2; exit 2; }

rm -rf "$DST/sql" "$DST/scripts" "$DST/pgbench"
cp -r "$SRC/sql" "$SRC/scripts" "$SRC/pgbench" "$DST/"

item=$(sed -n '/^INSERT INTO item/,/;[[:space:]]*$/p' "$SRC/sql/03_distribute.sql")
dist=$(grep -E "^SELECT (create_reference_table|create_distributed_table)\(" "$SRC/sql/03_distribute.sql")
[[ -n "$item" ]] || { echo "没在 $SRC/sql/03_distribute.sql 里找到 item 的 INSERT" >&2; exit 1; }
[[ $(grep -c create_distributed_table <<< "$dist") -eq 8 && $(grep -c create_reference_table <<< "$dist") -eq 1 ]] \
  || { echo "03_distribute.sql 里的 create_*_table 不是 1 + 8 条：" >&2; echo "$dist" >&2; exit 1; }
cat > "$DST/sql/03_distribute.sql" <<SQL
-- 原版 Citus 对照（scripts/make_citus.sh 生成）：按仓库号共置分布，单副本；无 Raft 组 / 打标。在协调者上执行：
--   psql -v shard_count=6 -f 03_distribute.sql
\set ON_ERROR_STOP on
SET citus.shard_replication_factor = 1;
SET citus.shard_count = :shard_count;
-- ShardPG 的"加入全局事务"在原版 Citus 上是空操作：Citus 自己的分布式事务 / 2PC 负责跨节点原子性
CREATE OR REPLACE FUNCTION tpcc.begin_global() RETURNS void LANGUAGE plpgsql AS \$\$ BEGIN END \$\$;
-- item 先本地灌好再转引用表（与 ShardPG 同一份数据生成）
$item
$dist
SQL
echo "原版 Citus 对照套件已生成：$DST（item $(grep -o 'generate_series(1, [0-9]*)' "$DST/sql/03_distribute.sql")，$(grep -c create_distributed_table "$DST/sql/03_distribute.sql") 张分布表）"
