#!/usr/bin/env bash
# 检查一棵 pg-install 树是不是打过 pg_partdist/pg_raft 所需的四个内核补丁。
#
# ★ 为什么需要它：`reproduce-env.sh` **不打补丁、也不重编 PostgreSQL**，它直接
# 把仓库里的 `pg-install/` 整棵树拷进容器。所以"从零 clone 能不能复原"完全取决于
# 仓库里这份构建是不是打过补丁的。2026-08-04 实测踩过：仓库里那份只含 0001，
# 缺 0001v2 和 0002，`destroy` 之后 `up` 出来的环境编不过 pg_partdist——而这时
# 旧容器已经删了，只能手工在容器里重编 PostgreSQL 抢救。
#
# 本脚本让这个问题在 **clone 之后、编扩展之前**就暴露，而不是等到
# `error: 'buffer_flush_lsn_exempt_hook' undeclared`。
#
# 用法：check_pg_install_patched.sh [pg-install 路径]   默认取脚本所在仓库的 pg-install/
set -u

ROOT="${1:-$(cd "$(dirname "$0")/../.." && pwd)/pg-install}"
fail=0

say() { printf '  %-58s %s\n' "$1" "$2"; }

# ★ 注意 `grep -c` 的返回值：无匹配时它**既打印 0、又以 1 退出**。写成
# `n=$(grep -c ... || echo 0)` 会叠出 "0\n0"，后面 `[[ "$n" -gt 0 ]]` 直接
# 语法错误 —— 而那条错误信息混在正常输出里很容易被忽略，检查就成了半瞎的。
# 用 `|| true` 保住那个 0。
need_grep() {  # need_grep <说明> <文件> <符号>
  local n
  if [[ ! -f "$2" ]]; then say "$1" "缺文件 $2"; fail=1; return; fi
  n=$(grep -c "$3" "$2" 2>/dev/null || true)
  n=${n:-0}
  if [[ "$n" -gt 0 ]]; then say "$1" "OK（$n 处）"; else say "$1" "缺失"; fail=1; fi
}

need_nm() {    # need_nm <说明> <符号>
  local n
  if [[ ! -x "$ROOT/bin/postgres" ]]; then say "$1" "缺 bin/postgres"; fail=1; return; fi
  n=$(nm -D "$ROOT/bin/postgres" 2>/dev/null | grep -c "$2" || true)
  n=${n:-0}
  if [[ "$n" -gt 0 ]]; then say "$1" "OK"; else say "$1" "缺失（二进制未重编？）"; fail=1; fi
}

echo "检查 pg-install 内核补丁：$ROOT"
need_grep "0001   wal_insert_hook 声明（xloginsert.h）" \
          "$ROOT/include/postgresql/server/access/xloginsert.h" "wal_insert_hook"
need_nm   "0001v2 wal_insert_hook 符号（bin/postgres，RM_SMGR 无块引用也回调）" \
          "wal_insert_hook"
need_grep "0002   buffer_flush_lsn_exempt_hook 声明（bufmgr.h）" \
          "$ROOT/include/postgresql/server/storage/bufmgr.h" "buffer_flush_lsn_exempt_hook"
need_nm   "0002   buffer_flush_lsn_exempt 符号（bin/postgres）" \
          "buffer_flush_lsn_exempt"
need_grep "0004   pre_record_commit_hook 声明（xact.h）" \
          "$ROOT/include/postgresql/server/access/xact.h" "pre_record_commit_hook"
need_nm   "0004   pre_record_commit_hook 符号（bin/postgres，DTX-2PC 决议挂点）" \
          "pre_record_commit_hook"

if [[ "$fail" -ne 0 ]]; then
  cat <<'EOF'

！这份 pg-install 不是打过补丁的构建，pg_partdist 编不过。
  修法见 pg-partdist-src/patches/README.md 的「补丁与仓库里 pg-install/ 的关系」：
  在容器里用 postgres-src 基线 + 四个补丁重编，再把 bin/postgres 与受影响的
  头文件同步回仓库并提交。
EOF
  exit 1
fi

echo "  → 四个补丁齐全"
exit 0
