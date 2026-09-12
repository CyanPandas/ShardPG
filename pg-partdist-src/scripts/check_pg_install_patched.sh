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

# ★ static 函数不进动态符号表，`nm -D` 查不到 —— 对这类只能按 strings 找。
#   （实测教训：给 0010 的 XLogAdvancePendingInsertPosition 先用了 need_nm，
#    立刻报"缺失"，而仓库与容器的 bin/postgres **md5 完全相同**且都含该字符串 ——
#    是判据选错了，不是补丁缺失。守卫产生假警报比漏检更糟：它会让人去重编一个
#    本来就好的二进制。）
need_str() {   # need_str <说明> <字符串>
  need_str_in "$1" "bin/postgres" "$2"
}

# ★★ 2026-09-12 补：判据要落到**具体哪个二进制**上。
#   PostgreSQL 的 rmgrdesc/*.c 会被编译**两次** —— 一次进 bin/postgres，
#   一次进 bin/pg_waldump（后者用符号链接把 desc 源拉进 src/bin/pg_waldump
#   单独编）。所以"只重编后端并同步 bin/postgres"会留下一个**能跑但解不出
#   新记录体的 pg_waldump**：它不报错，只是静静地少打一段注解。
need_str_in() {  # need_str_in <说明> <相对二进制路径> <字符串>
  local n
  if [[ ! -x "$ROOT/$2" ]]; then say "$1" "缺 $2"; fail=1; return; fi
  n=$(strings "$ROOT/$2" 2>/dev/null | grep -cF "$3" || true)
  n=${n:-0}
  if [[ "$n" -gt 0 ]]; then say "$1" "OK"; else say "$1" "缺失（$2 未重编？）"; fail=1; fi
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

# ★★ 2026-09-11 补：0005–0010 六个补丁**此前完全没有覆盖**，脚本却输出
#   "四个补丁齐全" —— 一个只验 3/10 的守卫，却给出"齐全"的结论，正是
#   §6.2 那类"看起来验过了"。而它是 reproduce-env.sh 在**建容器之前**用来
#   拦截"pg-install 没打补丁"的唯一关口：漏检的补丁一旦缺失，环境会以
#   各种难查的方式坏掉（TX-TSO-MVCC 的分片 xid、可见性、2PC rmgr 全在这几个里）。
#
#   触发这次复查的是 T7.12-P1 的 shard_clog_p2：它靠 `pg_waldump | grep
#   "shard xids:"` 取证而红。第一轮只查了 bin/postgres，十个补丁的特征符号
#   全都在，于是"方向被排除"—— **这个结论是错的**：0007 改的是 xactdesc.c，
#   而解码那份注解的是 **bin/pg_waldump**，它停留在打补丁之前的版本
#   （2026-09-12 实测：bin/postgres 含 "shard xids:"，bin/pg_waldump 不含）。
#   产品侧一直是对的，红的是取证工具。教训：**一个补丁"在不在"要按它实际
#   影响的每个产物分别验，不能只挑主二进制。**
need_nm   "0005   shard_relation_xid_hook 符号（分片 xid 打标）" \
          "shard_relation_xid_hook"
need_nm   "0006   shard_visibility_hook 符号（分片可见性）" \
          "shard_visibility_hook"
need_nm   "0007   shard_xact_wal_list_hook 符号（xact 记录带分片 xid）" \
          "shard_xact_wal_list_hook"
need_nm   "0008   shard_vacuum_read_hook 符号（vacuum 读钩子）" \
          "shard_vacuum_read_hook"
need_nm   "0009   shard_at_prepare_hook 符号（2PC rmgr）" \
          "shard_at_prepare_hook"
need_str  "0010   XLogAdvancePendingInsertPosition（升主推进写入位置；static，按 strings 验）" \
          "XLogAdvancePendingInsertPosition"

# 前端产物：pg_waldump 自带一份 xactdesc.o，必须同样带 0007 的注解代码，
# 否则 commit/abort 记录体里的分片 xid 块解不出来（取证类用例会无声地红）。
need_str_in "0007   pg_waldump 能解分片 xid 注解（前端另编一份 xactdesc.o）" \
            "bin/pg_waldump" "shard xids:"

if [[ "$fail" -ne 0 ]]; then
  cat <<'EOF'

！这份 pg-install 不是打过补丁的构建，pg_partdist 编不过。
  修法见 pg-partdist-src/patches/README.md 的「补丁与仓库里 pg-install/ 的关系」：
  在容器里用 postgres-src 基线 + **全部十个补丁**重编，再把 bin/postgres 与
  受影响的头文件同步回仓库并提交。
EOF
  exit 1
fi

echo "  → 十个补丁齐全（0001/0001v2/0002/0004 声明+符号，0005–0010 符号，0007 另验 pg_waldump）"
exit 0
