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

health_mark_start() {
  _HEALTH_START_TS=$(docker exec -u postgres "$CONTAINER" date -u "+%Y-%m-%d %H:%M:%S" 2>/dev/null)
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
