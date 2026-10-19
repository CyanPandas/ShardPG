#!/usr/bin/env bash
# 由本套件生成单机原生 PostgreSQL 对照套件：模式 / 生成函数 / 装载函数 / 事务函数 / pgbench 脚本
# 与本套件逐字节相同，只把 03_distribute.sql 换成"只灌 item + begin_global 空操作"，并去掉
# 函数下推（06_delegate.sql）。item 的灌法从本套件的 03_distribute.sql 原样摘出，所以迷你规模
# （make_mini.sh 改过的套件）也跟着对。
# 用法：bash scripts/make_native.sh <目标目录>；目标目录里的 cluster.env 另写（单节点、role=coordinator）。
set -euo pipefail
SRC="$(cd "$(dirname "$0")/.." && pwd)"
DST=${1:?目标目录}
mkdir -p "$DST"
DST=$(cd "$DST" && pwd)
[[ "$DST" != "$SRC" ]] || { echo "目标目录不能是套件本身" >&2; exit 2; }

rm -rf "$DST/sql" "$DST/scripts" "$DST/pgbench"
cp -r "$SRC/sql" "$SRC/scripts" "$SRC/pgbench" "$DST/"
rm -f "$DST/sql/06_delegate.sql"

item=$(sed -n '/^INSERT INTO item/,/;[[:space:]]*$/p' "$SRC/sql/03_distribute.sql")
[[ -n "$item" ]] || { echo "没在 $SRC/sql/03_distribute.sql 里找到 item 的 INSERT" >&2; exit 1; }
cat > "$DST/sql/03_distribute.sql" <<SQL
-- 原生 PG 对照（scripts/make_native.sh 生成）：无 Citus / Raft / 打标，只灌 item
\set ON_ERROR_STOP on
$item
-- 全局事务加入在原生 PG 上是空操作
CREATE OR REPLACE FUNCTION tpcc.begin_global() RETURNS void LANGUAGE plpgsql AS \$\$ BEGIN END \$\$;
SQL
echo "原生对照套件已生成：$DST（item $(grep -o 'generate_series(1, [0-9]*)' "$DST/sql/03_distribute.sql")）"
