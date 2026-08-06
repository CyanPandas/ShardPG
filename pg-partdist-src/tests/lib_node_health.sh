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
health_crash_lines() {
  [[ -z "$_HEALTH_START_TS" ]] && return 0
  docker exec -u postgres "$CONTAINER" bash -c \
    'grep -HE "terminated by signal (11|6|4|7)|PANIC:" /work/pg-cluster-data/*/restart.log /work/pg-cluster-data/*.log 2>/dev/null' \
  2>/dev/null \
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
