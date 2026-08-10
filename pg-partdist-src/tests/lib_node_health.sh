#!/usr/bin/env bash
# 节点健康检查助手 —— 被各验收脚本 source。
#
# ★ 存在的理由：**验收脚本对"节点崩了"完全是瞎的。**
#
# 排查 D2 时连续四轮误判都源于这一点：R1 报一堆文件 diff 不一致，看上去像回放
# 逻辑缺陷，实际是 leader 在 VACUUM 期间 segfault、节点整体重置、该分区组失去
# 多数派、follower 根本没收到后半截记录。而每次崩在哪一步是随机的，于是**每轮
# 失败的项都不一样**，更像"偶发的回放 bug"。
#
# 用法：
#   source "$(dirname "$0")/lib_node_health.sh"
#   health_mark_start          # 用例开始时打时间戳
#   ...
#   health_check_no_crash      # 收尾时断言：本轮时间窗内无 signal 11 / PANIC
#
# health_check_no_crash 调用 check()，所以必须在定义了 check() 之后 source/调用。

_HEALTH_START_TS=""
_HEALTH_DROPS_BASE=""

# 全部 9 个节点的端口（与 /work/pg-cluster-data 的节点布局一样是环境常量）
_HEALTH_PORTS="5432 5433 5434 5435 5436 5437 5438 5439 5440"

# 每端口一行："<port> <ring_full_drops+quorum_drops 总和>"；函数不存在的端口输出 "<port> MISSING"
_health_drops_snapshot() {
  local p v
  for p in $_HEALTH_PORTS; do
    v=$(docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p "$p"           -U postgres -d postgres -Atc           "SELECT COALESCE(sum(ring_full_drops+quorum_drops),0) FROM partdist.pg_raft_group_flow_stats()"           2>/dev/null </dev/null)
    [[ "$v" =~ ^[0-9]+$ ]] || v=MISSING
    echo "$p $v"
  done
}

health_mark_start() {
  _HEALTH_START_TS=$(docker exec -u postgres "$CONTAINER" date -u "+%Y-%m-%d %H:%M:%S" 2>/dev/null)
  _HEALTH_DROPS_BASE=$(_health_drops_snapshot)
}

# 输出本轮时间窗内的崩溃行，无则输出空。
#
# ★ 过滤放在**宿主机**做，容器里只负责 grep。
# 早先版本把 awk 程序嵌进 docker exec bash -c 的单引号里，靠 '"'"' 拼接，
# 结果时间窗判断整个失效 —— 日志里明明有 signal 11 却报"无崩溃"，
# 一个**静默通过**的检查比没有这个检查更糟。少一层嵌套就少一类这种坑。
# <datadir 名> → 该节点**此刻真正**的 stderr 文件（解析 /proc/<postmaster>/fd/2）。
#
# ★ 为什么不能按约定路径猜：pg_ctl -l 换过一次目标，约定路径上的那个文件就此
# 冻结，而按它取证的检查会永远读到一个不再更新的死文件 —— 静默退化成空检查。
# 实测：test_local_wal_conflict 把 worker1/worker2 重启到 <datadir>/lwc_restart.log
# 且没还回去，此后 worker1.log 的 mtime 停在那一刻不动；L1 在其上 grep 崩溃恢复
# 证据恒为 0 条，而 health_check_no_crash 的两个 glob（*/restart.log 和 *.log）
# 也都匹配不到新目标 —— **这两个节点上"本轮无节点崩溃"是空检查**。
health_node_log() {
  local d=$1
  docker exec -u postgres "$CONTAINER" bash -c "
    pm=\$(head -1 /work/pg-cluster-data/$d/postmaster.pid 2>/dev/null)
    [ -n \"\$pm\" ] && readlink /proc/\$pm/fd/2 2>/dev/null" 2>/dev/null \
  | grep '^/' || true
}

# 全部节点当前真正的日志文件（去重，每行一个绝对路径）
health_live_logs() {
  docker exec -u postgres "$CONTAINER" bash -c '
    for d in /work/pg-cluster-data/*/; do
      pf="${d}postmaster.pid"; [ -f "$pf" ] || continue
      pm=$(head -1 "$pf" 2>/dev/null); [ -n "$pm" ] || continue
      l=$(readlink /proc/$pm/fd/2 2>/dev/null)
      case "$l" in /*) echo "$l";; esac
    done' 2>/dev/null | sort -u
}

health_crash_lines() {
  [[ -z "$_HEALTH_START_TS" ]] && return 0
  # 扫描集合 = 各节点**当前真正**的日志 ∪ 历史约定路径（时间窗内的旧世代日志
  # 仍然算数）。两者可能指向同一文件，故最后 sort -u 去重，避免一次崩溃被数两遍。
  local live
  live=$(health_live_logs | tr '\n' ' ')
  docker exec -u postgres "$CONTAINER" bash -c \
    "grep -HE \"terminated by signal (11|6|4|7)|PANIC:\" $live /work/pg-cluster-data/*/restart.log /work/pg-cluster-data/*/*.log /work/pg-cluster-data/*.log 2>/dev/null" \
  2>/dev/null | sort -u \
  | awk -v ts="$_HEALTH_START_TS" '
      {
        i = index($0, ":")            # grep -H 的 "路径:" 分隔（路径本身不含冒号）
        if (i == 0) next
        rest = substr($0, i + 1)
      }
      match(rest, /^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]/) {
        if (substr(rest, RSTART, RLENGTH) >= ts) print
      }'
}

health_check_no_crash() {
  local lines n
  lines=$(health_crash_lines)
  n=$(printf '%s' "$lines" | grep -c . || true)
  check "本轮无节点崩溃（signal 11/6 或 PANIC）" "$n" "0"
  if [[ "$n" != "0" ]]; then
    printf '%s\n' "$lines" | sed 's/^/        /'
    echo "        ↑ 上面这些崩溃会让节点整体重置、分区组失去多数派，"
    echo "          由此产生的 diff 不一致**不是回放缺陷**，先修崩溃。"
    echo "        取栈回溯：ALTER SYSTEM SET pg_partdist.debug_segv_backtrace=on; 后重启节点。"
  fi
}

# 断言：回放 worker 池没有越界（每节点存活 worker 数 ≤ pg_partdist.replay_workers）。
#
# ★ 为什么必须有这条：这个泄漏**不会让任何功能断言变红**，只会让环境越来越脆。
# worker 是常驻的（主循环只在关机时退出），而 replay_disable 会让它释放认领；
# launcher 曾按"启动过几个"限池、且"无任何认领就把计数归零"，于是每经历一次
# arm → disable → arm 就永久多一个 worker。每个用例收尾都 disable，所以是
# **每轮每节点漏一个**。漏到撞满 max_worker_processes 之后，该节点再也拉不起
# 回放 worker，follower 静默停止追平 —— 症状是"每轮失败的用例都不一样"，
# 极易被当成偶发的回放 bug。本环境 08-08 已经实际撞上过。
health_check_worker_pool() {
  local lim over
  lim=$(docker exec -u postgres "$CONTAINER" /work/pg-install/bin/psql -p 5433 \
        -U postgres -d postgres -Atc "SHOW pg_partdist.replay_workers" 2>/dev/null </dev/null)
  if [[ ! "$lim" =~ ^[0-9]+$ ]]; then
    echo "  跳过  回放 worker 池检查（取不到 pg_partdist.replay_workers）"
    return 0
  fi
  over=$(docker exec -u postgres "$CONTAINER" bash -c '
    lim='"$lim"'
    for d in /work/pg-cluster-data/*/; do
      pf="${d}postmaster.pid"; [ -f "$pf" ] || continue
      pm=$(head -1 "$pf" 2>/dev/null); [ -n "$pm" ] || continue
      n=$(ps --ppid "$pm" -o args= 2>/dev/null | grep -c "replay worker")
      [ "$n" -gt "$lim" ] && echo "$(basename "$d")=$n"
    done' 2>/dev/null | tr '\n' ' ' | sed 's/ *$//')
  [[ -z "$over" ]] && over=none
  check "回放 worker 池未越界（上限 ${lim}/节点）" "$over" "none"
  [[ "$over" != "none" ]] && \
    echo "        ↑ 越界节点=存活worker数。撞满 max_worker_processes 后回放会静默停摆。"
  return 0
}

# 断言：本轮时间窗内没有任何 Raft 提案被丢弃（§13 约束 13 / #39 的验收判据）。
#
# ★ 为什么必须有这条：只查崩溃的 health_check_no_crash 抓不住"静默丢弃"。
# 实测两轮对照 —— 同一个缺陷（apply 认领在 FATAL 上泄漏），一轮丢中的全是
# 可重试的冻结 CTRL（五套件全 PASS），另一轮丢中 VACUUM 截断的 DATA（follower
# 永久分叉 + 回放 PANIC 循环）。**全 PASS + 有丢弃 = 运气，不是通过。**
#
# 节点重启会把 shmem 计数清零，所以差值只认"涨"（负数按 0 计）——重启丢掉的
# 计数由 health_check_no_crash 那侧兜住（重启本身就是崩溃）。
health_check_no_drops() {
  local total=0 missing=0 detail="" p base cur delta
  local cur_snap
  cur_snap=$(_health_drops_snapshot)
  for p in $_HEALTH_PORTS; do
    base=$(printf '%s\n' "$_HEALTH_DROPS_BASE" | awk -v p="$p" '$1==p{print $2}')
    cur=$(printf '%s\n' "$cur_snap" | awk -v p="$p" '$1==p{print $2}')
    if [[ "$cur" == "MISSING" || "$base" == "MISSING" || -z "$cur" || -z "$base" ]]; then
      missing=$((missing+1)); continue
    fi
    delta=$((cur - base)); [[ "$delta" -lt 0 ]] && delta=0
    total=$((total+delta))
    [[ "$delta" -gt 0 ]] && detail+=" :$p+$delta"
  done
  if [[ "$missing" -eq 9 ]]; then
    # 静默通过是最糟的失败模式：函数整个不存在时要喊出来，不能装作 0
    echo "  跳过  丢弃检查（partdist.pg_raft_group_flow_stats 不存在，flow-stats 版本之前的环境）"
    return
  fi
  check "本轮无 Raft 提案被丢弃（ring_full_drops + quorum_drops）" "$total" "0"
  if [[ "$total" != "0" ]]; then
    echo "        增量按节点：${detail}"
    echo "        ↑ 数据组丢提案 = leader 已 durable 的物理变更没送到 follower，"
    echo "          即使本轮文件 diff 全过也可能只是丢中了可重试的 CTRL —— 视为不通过。"
  fi
}
