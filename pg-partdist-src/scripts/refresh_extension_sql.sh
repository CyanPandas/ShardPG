#!/usr/bin/env bash
# 把 sql/pg_partdist--1.0.sql 里的 **C 函数声明** 重放到运行中的集群（R-P6-12）。
#
# ★ 为什么需要它：扩展 SQL **只在 CREATE EXTENSION 时执行一次**。之后往文件里
#   新增的声明，谁也不会自动补到已有的库上。tx2 是增量搭起来的环境，实测
#   **声明了 82 个、库里少 10 个**（partdist_tso_* / partdist_gxid_next /
#   partdist_join_global_txn / partdist_global_safe_ts / partdist_set_shard_mvcc …），
#   而且 partdist_tso_start_ts 只在协调者上、还落在 public 而不是 partdist 模式里。
#
#   交付物本身没问题（一次干净 clone 的 CREATE EXTENSION 会得到完整 82 个），
#   但**任何"SQL 里声明了就一定能调"的假设都会在本环境翻车** ——
#   T6.8 的门禁判据一开始就踩了这个坑（拿 partdist_tso_status() 当判据，
#   而库里根本没有这个函数）。
#
# 只重放 `AS 'MODULE_PATHNAME', '...'` 这类 C 函数：它们是 CREATE OR REPLACE、
# 幂等、且不依赖任何表数据。plpgsql/sql 函数与建表语句一概不碰 —— 那些可能
# 依赖执行顺序或已有数据，重放的风险大于收益。
set -u
C="${CONTAINER:-pg-citus-tx2-container}"
SQLFILE="$(cd "$(dirname "$0")/.." && pwd)/sql/pg_partdist--1.0.sql"
PORTS="${PORTS:-$(seq 5432 5440)}"
SCHEMA="${SCHEMA:-partdist}"

[[ -f "$SQLFILE" ]] || { echo "FATAL: 找不到 $SQLFILE"; exit 1; }

# 抽出 C 函数声明：从 CREATE OR REPLACE FUNCTION 到以 ';' 结尾的那一句，
# 且句中含 MODULE_PATHNAME。用 awk 攒块，遇到分号收尾。
tmp=$(mktemp)
awk '
  /^CREATE OR REPLACE FUNCTION/ { buf=$0"\n"; inblk=1; next }
  inblk { buf=buf $0"\n" }
  inblk && /;[[:space:]]*$/ {
      if (buf ~ /MODULE_PATHNAME/) printf "%s", buf
      inblk=0; buf=""
  }
' "$SQLFILE" | sed "s/MODULE_PATHNAME/\$libdir\/pg_partdist/g" > "$tmp"

n=$(grep -c "^CREATE OR REPLACE FUNCTION" "$tmp")
echo "  抽出 C 函数声明 $n 条"
[[ "$n" -gt 0 ]] || { echo "FATAL: 一条都没抽到，抽取逻辑坏了（静默通过是最糟的失败模式）"; rm -f "$tmp"; exit 1; }

# ★ 权限必须在 **docker cp 之前** 改：mktemp 造的是 0600、属主是宿主 UID，
#   docker cp 原样保留模式，容器里的 postgres 读不到 —— 症状是
#   `/tmp/refresh_ext.sql: Permission denied`。
#   cp 之后再 docker exec chmod 也不行：exec 的默认用户同样不是属主。
chmod 0644 "$tmp"
docker cp "$tmp" "$C":/tmp/refresh_ext.sql >/dev/null
rm -f "$tmp"

for p in $PORTS; do
  out=$(docker exec -i -u postgres -e HOME=/var/lib/postgresql \
          -e PGOPTIONS="-c citus.enable_ddl_propagation=off" "$C" \
          /work/pg-install/bin/psql -h /tmp -p "$p" -U postgres -d postgres -X \
          -v ON_ERROR_STOP=0 -c "SET search_path=${SCHEMA},public" -f /tmp/refresh_ext.sql </dev/null 2>&1)
  # ★ 不能只 grep '^ERROR'：psql **自己**的错误（打不开文件、连不上）前缀是
  #   `psql: error:`，漏掉它就会把"整个文件根本没执行"报成"报错 0 条" ——
  #   首版正是这样静默通过的，9 个节点全报成功，实际一条都没跑。
  nerr=$(printf '%s' "$out" | grep -cE "^ERROR|^psql: error" || true)
  have=$(docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" \
          /work/pg-install/bin/psql -h /tmp -p "$p" -U postgres -d postgres -X -Atc \
          "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='${SCHEMA}'" </dev/null 2>/dev/null)
  printf "  :%s 重放完成（报错 %s 条，%s 现有函数 %s 个）\n" "$p" "${nerr:-?}" "$SCHEMA" "${have:-?}"
  [[ "${nerr:-0}" -gt 0 ]] && printf '%s\n' "$out" | grep -E "^ERROR|^psql: error" | head -3 | sed 's/^/      /'
done

# ★ 自检：抽出来的**每一个函数名**都要在库里点到名。
#   首版自检是"C 函数总数 >= 抽出条数"——那是个数量对比，96 >= 73 轻松通过，
#   而实际有 9 个函数一个都没建上。**数总量抓不住"少了哪几个"**。
vport=$(echo $PORTS | awk '{print $2}')
names=$(grep -oE "^CREATE OR REPLACE FUNCTION [a-z_]+" /tmp/_refresh_names.txt 2>/dev/null | awk '{print $NF}')
docker exec -i "$C" cat /tmp/refresh_ext.sql 2>/dev/null > /tmp/_refresh_names.txt || true
names=$(grep -oE "^CREATE OR REPLACE FUNCTION [a-z_]+" /tmp/_refresh_names.txt | awk '{print $NF}' | sort -u)
nname=$(printf '%s\n' "$names" | grep -c . || true)
inlist=$(printf "'%s'," $names | sed 's/,$//')
gone=$(docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" \
  /work/pg-install/bin/psql -h /tmp -p "$vport" -U postgres -d postgres -X -Atc \
  "SELECT string_agg(x,' ') FROM unnest(ARRAY[${inlist}]) x
    WHERE NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
                      WHERE n.nspname='${SCHEMA}' AND p.proname=x)" </dev/null 2>/dev/null)
rm -f /tmp/_refresh_names.txt
if [[ -n "${gone:-}" ]]; then
  echo "  ⚠ 自检不过：:${vport} 上仍缺 ${gone}"
  exit 1
fi
echo "  自检通过：抽出的 ${nname} 个函数在 :${vport} 上全部点到名"
