#!/usr/bin/env bash
# 拓扑无关化助手 —— 被**容器内**执行的 OPS 套件 source（T7.13 / P7-E2）。
#
# ★ 存在的理由：这 8 套都是按 3 节点布局（coordinator + worker1 + worker2）写的，
#   把「worker1 / worker2」直接当成「全部 worker」。9 节点上这个前提不成立：
#     · 它们建的表，分片可能落在 worker3..worker8 —— 套件盯着 worker1/2 什么也
#       看不到，于是报"目录缺失""找不到 OID"，看起来像产品缺陷；
#     · 更糟的是 crash_recovery / multi_table_isolation 会 `stop` 掉 worker1/2
#       并 `rm -rf` 它们的 pg_parwal —— 在 9 节点上那是对两个**无关**节点做破坏，
#       而且做完不复原，把之后每一套都变成 connection refused。
#   实测代价见 run_p6_exit.sh 里的那段注释：首版收编它们，直接毒化整批
#   （tso_si_p3 17/21、连带 7 套全红）。
#
# 用法（容器内，直接用 /work/pg-install/bin 下的 psql）：
#   source "$(dirname "$0")/lib_topology.sh"
#   topo_init                      # 校验端口<->数据目录映射；装 EXIT 复原钩子
#   topo_ports_for_table t         # 该表分片所在的 primary 端口（去重、升序、空格分隔）
#   topo_datadir <port>            # 该端口的数据目录
#   topo_stop <port> [-m mode]     # 停节点，**自动登记复原**
#   topo_start <port>              # 起节点并等它可连
#   topo_restore_all               # 复原所有被 topo_stop 停过的节点（EXIT 已自动调）
#
# ★★ 纪律之上还有一条（2026-09-10 实测补）：**在会被 SIGKILL 的环境里，
#   "做了危险操作再恢复"这个模式本身就不成立。** 本宿主机内存紧（3.9 G 总量、
#   9 节点常驻 2.2 G），后台任务会被系统直接 SIGKILL —— EXIT 钩子一行都跑不到。
#   实测：test_shard_auto_init 的权限用例第一版改成"对全部 worker chmod 555
#   再由 EXIT 恢复"，那一轮正好被杀在 chmod 前一刻，纯属侥幸；晚半秒就是
#   **全簇 pg_parwal 永久只读**，之后每一套都会以莫名其妙的方式红。
#
#   所以：危险操作要**天然局部**，把爆炸半径缩到"就算永远不恢复也无所谓"。
#   那个用例最终改成"在分片目录路径上放个普通文件让 mkdir 失败"，只波及本表
#   自己那几个 OID 的路径。
#
#   停节点是个**例外**，可以靠复原兜底 —— 因为它是**可诊断**的：连不上一目了然，
#   重启即恢复。只读目录则是**静默**的，这是两者的分水岭。
#
# ★ 纪律：**停节点的套件必须自带复原。** 本项目已经为此吃过亏 ——
#   P7-E6（`test_promote_handover_p7.sh` 杀完主不复原）让紧随其后的
#   `test_slot_reclaim_p7` 死在"节点可连"上，和它要验的槽位回收毫无关系，
#   极容易被误判成"槽位回收回归了"。所以复原挂在 EXIT 上，中途 Ctrl-C 也复原。

_TOPO_BIN=/work/pg-install/bin
_TOPO_DATA=/work/pg-cluster-data
_TOPO_COORD_PORT=5432
_TOPO_STOPPED=""          # 空格分隔的端口列表：被本套件停过、待复原

# 端口 -> 数据目录。命名约定与本环境一致（coordinator:5432，worker N:5432+N）。
# 这一条是**环境常量**，与 lib_node_health.sh 里写死端口列表同源；
# 真正不能写死的是"哪个节点承载我的分片"，那必须查 pg_dist_placement。
topo_datadir() {
  local port=$1
  if [[ "$port" == "$_TOPO_COORD_PORT" ]]; then
    echo "$_TOPO_DATA/coordinator"
  else
    echo "$_TOPO_DATA/worker$((port - _TOPO_COORD_PORT))"
  fi
}

topo_psql() { local port=$1; shift; "$_TOPO_BIN/psql" -U postgres -p "$port" -d postgres -X "$@"; }

topo_alive() {
  [[ "$(topo_psql "$1" -Atc 'SELECT 1' 2>/dev/null | tail -1)" == "1" ]]
}

# 该表的分片落在哪些 primary 上（去重、升序）。查协调者的 Citus 元数据。
topo_ports_for_table() {
  topo_psql "$_TOPO_COORD_PORT" -Atc "
    SELECT DISTINCT n.nodeport
      FROM pg_dist_shard s
      JOIN pg_dist_placement p ON p.shardid = s.shardid
      JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
     WHERE s.logicalrelid = '$1'::regclass
     ORDER BY 1" 2>/dev/null | tr '\n' ' '
}

# 全部 worker 端口（不含协调者），按 pg_dist_node 动态取。
topo_worker_ports() {
  topo_psql "$_TOPO_COORD_PORT" -Atc "
    SELECT nodeport FROM pg_dist_node
     WHERE noderole = 'primary' AND nodeport <> $_TOPO_COORD_PORT
     ORDER BY 1" 2>/dev/null | tr '\n' ' '
}

# 等旧 postmaster **彻底**退出。
# ★ 2026-09-10 实测：不等就启动，会撞上"the database system is shutting down" ——
#   pg_ctl start 失败，而 60 s 轮询看到的一直是那句 FATAL，节点就此躺着不起。
#   `pg_ctl stop -w` 并不总能保证返回时进程已消失（超时被 `|| true` 吞掉时尤其如此），
#   所以这里以 `pg_ctl status` 为准再确认一遍。
_topo_wait_down() {
  local dir=$1 t
  for t in $(seq 1 60); do
    "$_TOPO_BIN/pg_ctl" -D "$dir" status >/dev/null 2>&1 || return 0
    sleep 1
  done
  return 1
}

# kill -9 之后会留下两个锁文件，且都记着**已经不存在的**那个 PID：
#   · $PGDATA/postmaster.pid
#   · /tmp/.s.PGSQL.<port>.lock（Unix socket 锁）
# PostgreSQL 本来会识别陈旧的 pid 文件并自行清理，但前提是那个 PID 已经不存在。
# 在容器里 PID 很容易被**复用** —— 一旦复用，PG 就认定"还有 postmaster 在跑"，
# 于是死活起不来：
#   FATAL: lock file "postmaster.pid" already exists
#   HINT:  Is another postmaster (PID 141805) running in data directory ...
# 3 节点开发机上 PID 不容易复用，所以原套件从没撞上；这里必须自己处理 ——
# `kill -9` 是 crash_recovery 的**测试手段本身**，不是意外。
#
# 安全判据：PID 不存在，**或**存在但它的 cmdline 里没有本数据目录 ⇒ 陈旧，可删。
_topo_clear_stale_locks() {
  local dir=$1 port=$2 pid
  pid=$(head -1 "$dir/postmaster.pid" 2>/dev/null)
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    # 同样避开裸 AND 链：`a | b && return 1` 作为独立语句，b 失败时整条返回非零，
    # 在 set -e 下会终止调用方。写成 if，状态被显式测试，才与调用方式无关地安全。
    if tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | grep -q -- "$dir"; then
      return 1                                  # 真在跑，别碰
    fi
  fi
  rm -f "$dir/postmaster.pid" "/tmp/.s.PGSQL.${port}.lock" "/tmp/.s.PGSQL.${port}" 2>/dev/null || true
  return 0
}

topo_start() {
  local port=$1 dir t try; dir=$(topo_datadir "$port")
  for try in 1 2 3; do
    # ★ 必须写成 if，不能写 `[[ ... ]] && _topo_clear_stale_locks ...`：
    #   调用方多半开了 `set -e`，而该函数在"postmaster 真在跑"时**故意**返回 1，
    #   于是整条 && 链返回非零 ⇒ 脚本当场终止。实测就这么把 crash_recovery
    #   在 Scenario B 中途打断了（12 条 PASS 之后无声退出，看起来像套件跑完了）。
    if [[ "$try" -gt 1 ]]; then _topo_clear_stale_locks "$dir" "$port" || true; fi
    _topo_wait_down "$dir" || {
      # 还没停干净：升级为 immediate，别无限等
      "$_TOPO_BIN/pg_ctl" -D "$dir" -m immediate stop >/dev/null 2>&1 || true
      sleep 2
    }
    # ★★ 这里的 `|| true` 不是装饰（2026-09-10 实测，ERR 陷阱 + set -E 定位到本行）。
    #   调用方多半开了 `set -e`，而**函数内部的失败同样会终止整个脚本**。
    #   kill -9 之后第 1 次 start 必然失败（残留锁），裸写这条的后果是：
    #   脚本当场死在这里，下面的重试循环一次都轮不到 —— 表现为
    #   「12 条 PASS、零 FAIL、rc=1」，看起来像跑完了且全过，只有 rc 泄露真相。
    #   纪律：**供 set -e 脚本 source 的库，凡"预期会失败且已被处理"的命令都不能裸写。**
    "$_TOPO_BIN/pg_ctl" -D "$dir" -l "$dir/pg.log" -o "-p $port" start -w -t 60 >/dev/null 2>&1 || true
    for t in $(seq 1 30); do topo_alive "$port" && return 0; sleep 1; done
    echo "  ⚠ topo_start: :$port 第 ${try}/3 次未起来，重试" >&2
  done
  echo "  ⚠ topo_start: :$port 三次尝试后仍未起来" >&2
  return 1
}

# 停节点。**先登记再停** —— 停到一半被打断也要能复原。
topo_stop() {
  local port=$1 mode=${2:--m fast} dir; dir=$(topo_datadir "$port")
  case " $_TOPO_STOPPED " in *" $port "*) ;; *) _TOPO_STOPPED+="$port ";; esac
  # shellcheck disable=SC2086
  "$_TOPO_BIN/pg_ctl" -D "$dir" $mode stop -w -t 60 >/dev/null 2>&1 || true
  _topo_wait_down "$dir" || "$_TOPO_BIN/pg_ctl" -D "$dir" -m immediate stop >/dev/null 2>&1 || true
}

topo_restore_all() {
  local p rc=0
  for p in $_TOPO_STOPPED; do
    topo_alive "$p" && continue
    topo_start "$p" || rc=1
    echo "  [topo] 已复原节点 :$p"
  done
  _TOPO_STOPPED=""
  return $rc
}

# 装 EXIT 钩子；若调用方已有 EXIT trap，则**串在它前面**而不是覆盖掉。
_topo_install_trap() {
  local prev
  prev=$(trap -p EXIT | sed -E "s/^trap -- '(.*)' EXIT\$/\1/")
  if [[ -z "$prev" || "$prev" == "$(trap -p EXIT)" ]]; then
    trap 'topo_restore_all' EXIT
  else
    trap "topo_restore_all; $prev" EXIT
  fi
}

topo_init() {
  topo_alive "$_TOPO_COORD_PORT" || { echo "FATAL: 协调者 :$_TOPO_COORD_PORT 连不上" >&2; return 1; }
  _topo_install_trap
  return 0
}
