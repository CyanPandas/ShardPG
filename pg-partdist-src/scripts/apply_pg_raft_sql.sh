#!/usr/bin/env bash
# [宿主机] 把 pg-raft-src/sql/pg_raft--1.0.sql 里**新增/改签名**的函数滚到已建好的 9 个节点。
#
# 为什么需要它：CREATE EXTENSION 只在建环境时跑一次；之后往 pg_raft--1.0.sql 加函数，
# sync_build.sh 只装 .so（守卫⑥会确认符号在 .so 里），库里的声明不会自己长出来。
# 门禁的 refresh_extension_sql.sh 只对齐 pg_partdist 的函数面，pg_raft 的没人管 ——
# 2026-09-15 P7-W6 加 pg_raft_group_quorum_alive() 时补的这个口子。
#
# 做法：把 SQL 文件里指定名字的 CREATE OR REPLACE FUNCTION … ; 块（含紧随的 COMMENT）
# 原样抽出，在每个节点上以 search_path=partdist 执行，并 ALTER EXTENSION pg_raft ADD
# （已属于扩展则忽略）。不给名字 = 只做核对（每个名字在 9 节点上都点得到）。
#
# 用法：bash apply_pg_raft_sql.sh 函数名 [函数名...]
#       CONTAINER=… 覆盖容器（默认 pg-test-container）
set -u
C="${CONTAINER:-pg-test-container}"
T="$(cd "$(dirname "$0")" && pwd)"
SQLF="$T/../../pg-raft-src/sql/pg_raft--1.0.sql"
[[ -f "$SQLF" ]] || { echo "找不到 $SQLF" >&2; exit 2; }
# Citus 拦 worker 上的 CREATE FUNCTION（"operation is not allowed on this node"）：
# 与 refresh_extension_sql.sh 同法，关掉 DDL 传播再执行。MODULE_PATHNAME 是
# CREATE EXTENSION 才会替换的占位，这里按 control 文件替成 $libdir/pg_raft。
PS() { local p=$1; shift; docker exec -i -u postgres -e PGOPTIONS="-c citus.enable_ddl_propagation=off" "$C" /work/pg-install/bin/psql -h /tmp -p "$p" -U postgres -d postgres -X -v ON_ERROR_STOP=1 "$@"; }
MODPATH=$(sed -n "s/^module_pathname *= *'\(.*\)'.*/\1/p" "$T/../../pg-raft-src/pg_raft.control")
[[ -n "$MODPATH" ]] || MODPATH='$libdir/pg_raft'

extract_block() {   # 抽出 "CREATE OR REPLACE FUNCTION <name>(" 起、到下一个空行为止的块（含 COMMENT）
  awk -v n="$1" '
    $0 ~ ("^CREATE OR REPLACE FUNCTION " n "\\(") { inb=1 }
    inb { print }
    inb && /^$/ { exit }
  ' "$SQLF" | sed "s|'MODULE_PATHNAME'|'${MODPATH}'|"
}
sig_of() {   # $1=函数名；从**整个 SQL 文件**取 "name(argtypes)" 给 ALTER EXTENSION 用。
  # 不能从抽出的块里取：extract_block 到第一个空行为止，而 CREATE 的 `;` 与 COMMENT
  # 之间正好隔着一个空行 —— 块里没有 COMMENT，sig 会是空、ALTER 被静默跳过（member=0）。
  grep -o "COMMENT ON FUNCTION $1([^)]*)" "$SQLF" | head -1 | sed 's/COMMENT ON FUNCTION //'
}

rc=0
for fn in "$@"; do
  blk=$(extract_block "$fn")
  [[ -n "$blk" ]] || { echo "✗ SQL 文件里没有 $fn 的 CREATE OR REPLACE FUNCTION 块" >&2; rc=1; continue; }
  sig=$(sig_of "$fn")
  [[ -n "$sig" ]] || echo "  ⚠ $fn 无 COMMENT ON FUNCTION 行，跳过 ALTER EXTENSION（函数仍会创建，只是不入籍）" >&2
  for p in $(seq 5432 5440); do
    if printf 'SET search_path = partdist, pg_catalog;\n%s\n' "$blk" | PS "$p" -q >/dev/null 2>"/tmp/apply_pg_raft_sql.$p.err"; then
      mtag=""
      if [[ -n "$sig" ]]; then
        # 已是成员时 ADD 报 "already member"，按成功待之；其余错误显式报出
        aerr=$(PS "$p" -q -c "ALTER EXTENSION pg_raft ADD FUNCTION partdist.${sig};" </dev/null 2>&1)
        if [[ $? -ne 0 ]] && ! grep -qi "already a member\|已经是" <<<"$aerr"; then
          mtag=" (入籍失败: $(head -1 <<<"$aerr" | cut -c1-70))"; rc=1
        fi
      fi
      echo "  :$p ✓ $fn${mtag}"
    else
      echo "  :$p ✗ $fn：$(head -2 /tmp/apply_pg_raft_sql.$p.err | tr '\n' ' ')"; rc=1
    fi
  done
done

# 核对：SQL 文件里每个 C 函数在 9 节点上都点得到名
missing=0
for fn in $(grep -o "^CREATE OR REPLACE FUNCTION [a-z_0-9]*" "$SQLF" | awk '{print $NF}' | sort -u); do
  for p in $(seq 5432 5440); do
    n=$(PS "$p" -Atc "SELECT count(*) FROM pg_proc p JOIN pg_namespace s ON s.oid=p.pronamespace WHERE s.nspname='partdist' AND p.proname='$fn'" </dev/null 2>/dev/null)
    [[ "$n" =~ ^[1-9] ]] || { echo "  ✗ :$p 缺 partdist.$fn"; missing=$((missing+1)); }
  done
done
[[ $missing -eq 0 ]] && echo "核对：pg_raft--1.0.sql 全部函数在 9 节点上都点得到名" || rc=1
exit $rc
