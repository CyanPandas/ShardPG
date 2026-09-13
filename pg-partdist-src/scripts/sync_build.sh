#!/usr/bin/env bash
# [宿主机] 把工作区的扩展源码同步进容器、构建、安装 —— 带守卫（P7-W5）。
#
# 为什么要有它：R-P6-7 / T7.27 两次栽在同一个坑上 —— 改了共享结构体或头文件后，
# 手工 `docker cp` + 增量 make 在"看起来成功"的状态下装进去一个旧 .so：
#   ① `docker cp` 没带上 include/（或与被 SIGKILL 打断的 build 抢时间），容器里是旧头；
#   ② `make -s ... | grep error` 把编译错误滤掉了，紧接着的 make install 拿**上一次的
#      stale .o** 重新链接成功、报 installed，.so 里却没有新符号；
#   ③ 本脚本写的时候又查出第三条：docker cp / tar 都**保留宿主机的 mtime**。宿主机
#      源文件若比容器里的 .o 旧，make 判定"已是最新"，改动根本不参与编译。
#   ④ PGXS 在本环境没开 autodepend，**头文件变了不会触发任何 .c 重编**。
# 现象全都伪装成"回放缺陷 / 接口不存在"，每次都要绕半天才现形。
#
# 守卫（任何一条不过即非零退出，不装半成品）：
#   1. 按**内容**比对（md5 清单），只同步真变了的文件，落盘时刷新 mtime（tar -m）
#   2. 同步后再比一次清单：容器 == 工作区，否则 FAIL（堵 ①）
#   3. 头文件清单与上次成功构建不同 ⇒ make clean 全量重编（堵 ④；共享结构体变动的前提）
#   4. 编译输出**不过滤**落日志；rc≠0 或出现 error / implicit declaration / undefined
#      reference 即 FAIL（堵 ②；implicit declaration 就是 P7-W3 的形态）
#   5. 装上去的 .so 与刚编出来的逐字节相同
#   6. 源码里每个 PG_FUNCTION_INFO_V1(f) 的 f 与 pg_finfo_f、SQL 里每个
#      AS 'MODULE_PATHNAME', 'sym' 的 sym，都必须在 .so 的**动态导出符号表**里（堵 ②③）
#
# 用法：
#   bash sync_build.sh                    # 两个扩展都做（pg-partdist-src、pg-raft-src）
#   bash sync_build.sh pg-partdist-src    # 只做一个
#   bash sync_build.sh --check [目标...]  # 只做守卫 2 与 6，不同步不构建（取证用）
# 环境变量：
#   CONTAINER   默认 pg-test-container
#   WS_ROOT     工作区根（默认本脚本所在仓库根）
#   BUILD_ROOT  容器内源码根（默认 /work）
#   NO_INSTALL=1  只构建不安装（守卫 5 跳过，守卫 6 查构建目录里的 .so）
#   FORCE_CLEAN=1 无论头文件变没变都 make clean
#
# ★ 构建装好 ≠ 节点已加载。头文件变了（守卫 3 触发）时共享内存布局可能变了，
#   必须**整簇重启**（门禁净场或 reproduce-env.sh reset），只重启部分节点会栈踩踏。
set -uo pipefail

CONTAINER="${CONTAINER:-pg-test-container}"
WS_ROOT="${WS_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}"
BUILD_ROOT="${BUILD_ROOT:-/work}"
PG_CONFIG=/work/pg-install/bin/pg_config
MODE=build
if [[ "${1:-}" == "--check" ]]; then MODE=check; shift; fi
TARGETS=("$@"); [[ ${#TARGETS[@]} -eq 0 ]] && TARGETS=(pg-partdist-src pg-raft-src)

DEX() { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
fail() { echo "  ✗ FAIL  $*"; RC=1; TRC=1; }
ok()   { echo "  ✓ $*"; }

# 参与构建的文件：src/ include/ sql/ 下的 .c .h .sql + Makefile + *.control
MANIFEST_CMD='find src include sql -type f \( -name "*.c" -o -name "*.h" -o -name "*.sql" \) 2>/dev/null; ls Makefile *.control 2>/dev/null'
manifest_host() { (cd "$1" && eval "$MANIFEST_CMD" | LC_ALL=C sort | xargs -d '\n' md5sum); }
manifest_ctr()  { DEX bash -c "cd '$1' && { $MANIFEST_CMD; } | LC_ALL=C sort | xargs -d '\n' md5sum" </dev/null; }

RC=0
for T in "${TARGETS[@]}"; do
  H="$WS_ROOT/$T"; C="$BUILD_ROOT/$T"; TRC=0; HDR_CHANGED=0
  echo "================ $T  （$MODE；容器 $CONTAINER:$C）================"
  [[ -d "$H" ]] || { fail "工作区没有 $H"; continue; }
  DEX test -d "$C" </dev/null || { fail "容器里没有 $C"; continue; }
  SO_NAME=$(sed -nE 's/^MODULE_big *= *([A-Za-z0-9_]+).*/\1/p' "$H/Makefile" | head -1)
  [[ -n "$SO_NAME" ]] || { fail "Makefile 里取不到 MODULE_big"; continue; }

  if [[ "$MODE" == build ]]; then
    # ── 守卫 1：按内容挑出要同步的文件 ──
    changed=$(LC_ALL=C comm -23 <(manifest_host "$H" | LC_ALL=C sort) <(manifest_ctr "$C" | LC_ALL=C sort) | awk '{print $2}')
    nchg=$(printf '%s' "$changed" | grep -c . || true)
    if [[ "$nchg" -gt 0 ]]; then
      printf '%s\n' "$changed" | sed 's/^/      同步 /'
      # -m：落盘时间 = 现在，保证比容器里任何旧 .o 新（堵 ③）
      (cd "$H" && printf '%s\n' "$changed" | tar -cf - -T -) | DEX tar -C "$C" -xmf -
      ok "同步 $nchg 个文件（按内容比对）"
    else
      ok "源码无变更"
    fi
  fi

  # ── 守卫 2：同步后清单必须一致 ──
  diffout=$(LC_ALL=C diff <(manifest_host "$H") <(manifest_ctr "$C"))
  if [[ -n "$diffout" ]]; then
    fail "容器源码与工作区不一致（头文件没带上就是这个形态）："
    printf '%s\n' "$diffout" | grep -E '^[<>]' | head -20 | sed 's/^/        /'
    continue
  fi
  ok "容器源码 == 工作区（$(manifest_host "$H" | wc -l) 个文件逐个 md5 相同）"

  SO_BUILT="$C/$SO_NAME.so"
  if [[ "$MODE" == build ]]; then
    # ── 守卫 3：头文件变了就全量重编 ──
    hdr_now=$(DEX bash -c "cd '$C' && find include src -name '*.h' 2>/dev/null | LC_ALL=C sort | xargs -d '\n' md5sum | md5sum | cut -d' ' -f1" </dev/null)
    hdr_last=$(DEX bash -c "cat '$C/.sync_build.hdr' 2>/dev/null" </dev/null)
    if [[ -z "$hdr_last" ]]; then
      ok "首次运行（容器里没有构建基线 .sync_build.hdr）⇒ make clean 全量重编"
      DEX bash -c "cd '$C' && make clean PG_CONFIG=$PG_CONFIG >/dev/null 2>&1" </dev/null
      HDR_CHANGED=0   # 无从比较，不断言布局变了
    elif [[ "${FORCE_CLEAN:-0}" == 1 || "$hdr_now" != "$hdr_last" ]]; then
      ok "头文件与上次成功构建不同（或 FORCE_CLEAN）⇒ make clean 全量重编"
      DEX bash -c "cd '$C' && make clean PG_CONFIG=$PG_CONFIG >/dev/null 2>&1" </dev/null
      [[ "$hdr_now" != "$hdr_last" ]] && HDR_CHANGED=1
    else
      HDR_CHANGED=0
    fi

    # ── 守卫 4：编译输出不过滤 ──
    log="/tmp/sync_build.$T.log"
    DEX bash -c "cd '$C' && make PG_CONFIG=$PG_CONFIG > '$log' 2>&1; echo \$? > '$log.rc'" </dev/null
    brc=$(DEX cat "$log.rc" </dev/null)
    bad=$(DEX bash -c "grep -nE 'error:|implicit declaration of function|undefined reference' '$log'" </dev/null)
    nwarn=$(DEX bash -c "grep -c 'warning:' '$log'" </dev/null)
    if [[ "$brc" != 0 || -n "$bad" ]]; then
      fail "编译不合格（make rc=$brc；rc=0 也可能是隐式声明这类只报 warning 的致命问题）—— 容器内完整日志 $log："
      printf '%s\n' "$bad" | head -15 | sed 's/^/        /'
      continue
    fi
    ok "编译通过（rc=0，warning ${nwarn:-0} 条，完整日志 $log）"

    if [[ "${NO_INSTALL:-0}" != 1 ]]; then
      DEX bash -c "cd '$C' && make install PG_CONFIG=$PG_CONFIG > '$log.install' 2>&1" </dev/null \
        || { fail "make install 失败（$log.install）"; continue; }
      # ── 守卫 5：装上去的就是刚编出来的 ──
      SO_INST="$(DEX $PG_CONFIG --pkglibdir </dev/null)/$SO_NAME.so"
      if DEX cmp -s "$SO_BUILT" "$SO_INST" </dev/null; then ok "已安装 $SO_INST（与构建产物逐字节相同）"
      else fail "已安装的 $SO_INST 与构建产物不同"; continue; fi
    fi
  fi

  # ── 守卫 6：该导出的符号都在 .so 里 ──
  SO_CHECK="$SO_BUILT"
  if [[ "$MODE" == check || "${NO_INSTALL:-0}" != 1 ]]; then
    SO_CHECK="$(DEX $PG_CONFIG --pkglibdir </dev/null)/$SO_NAME.so"
    [[ "$MODE" == check && "$C" != /work/* ]] && SO_CHECK="$SO_BUILT"
  fi
  exported=$(DEX nm -D --defined-only "$SO_CHECK" </dev/null 2>/dev/null | awk '{print $NF}' | LC_ALL=C sort -u)
  [[ -n "$exported" ]] || { fail "读不到 $SO_CHECK 的动态符号表"; continue; }
  want_finfo=$(cd "$H" && grep -rhoE 'PG_FUNCTION_INFO_V1\([A-Za-z0-9_]+\)' src 2>/dev/null \
               | sed -E 's/.*\(([A-Za-z0-9_]+)\)/\1/' | LC_ALL=C sort -u)
  want_sql=$(cd "$H" && cat sql/*.sql 2>/dev/null | tr '\n' ' ' \
             | grep -oE "MODULE_PATHNAME'[[:space:]]*,[[:space:]]*'[A-Za-z0-9_]+'" \
             | sed -E "s/.*,[[:space:]]*'([A-Za-z0-9_]+)'/\1/" | LC_ALL=C sort -u)
  want=$( { printf '%s\n' "$want_finfo"; printf '%s\n' "$want_finfo" | sed 's/^/pg_finfo_/'; printf '%s\n' "$want_sql"; } \
          | grep . | LC_ALL=C sort -u)
  nwant=$(printf '%s\n' "$want" | grep -c .)
  missing=$(LC_ALL=C comm -23 <(printf '%s\n' "$want") <(printf '%s\n' "$exported"))
  if [[ "$nwant" -eq 0 ]]; then
    fail "一个应导出的符号都没抽到（抽取失效 = 空检查）"
  elif [[ -n "$missing" ]]; then
    fail "$SO_CHECK 缺 $(printf '%s\n' "$missing" | grep -c .) 个应导出符号（stale .o 重链就是这个形态）："
    printf '%s\n' "$missing" | head -20 | sed 's/^/        /'
  else
    ok "导出符号齐全：$nwant 个（PG_FUNCTION_INFO_V1 $(printf '%s\n' "$want_finfo" | grep -c .) 个 × 2 + SQL 引用去重）"
  fi

  if [[ "$MODE" == build && $TRC -eq 0 ]]; then
    DEX bash -c "echo '$hdr_now' > '$C/.sync_build.hdr'" </dev/null
    [[ "$HDR_CHANGED" == 1 ]] && echo "  ★ 本次头文件有变：共享内存布局可能变了，节点须**整簇重启**后才算生效"
  fi
done

echo "================ 结果：$([[ $RC -eq 0 ]] && echo 通过 || echo 失败) ================"
exit $RC
