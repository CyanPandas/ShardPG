#!/bin/bash
# 大库压测的磁盘护栏（2026-10-21）：每 30 s 看一遍我们会写的几块共享盘，任何一块剩余空间低于底线，
# 就停掉压测脚本、停掉我们自己的 shardpg-fix-* / pgsync-* 容器（只 stop，不删数据），保证不把别人的盘写满。
# 只动我们自己的进程与容器；不删任何文件。用法（145 上，nohup 后台）：disk_guard.sh <盯哪个日志里的 STD_DONE>
R=/data1/yyt/shardpg-fix-20261014; WATCH=${1:-$R/tsoraft_logs/bench_std.out}; OUT=$R/tsoraft_logs/disk_guard.txt
# 主机 挂载点 底线（GB）
LIMITS="10.77.110.145:/data1:200 10.77.110.147:/home:200 10.77.110.148:/home:25 10.77.110.148:/data3:200 10.77.110.144:/home:15"
onhost() { local h=$1; shift; if [ $h = 10.77.110.145 ]; then bash -c "$*"; else ssh -n -o ConnectTimeout=10 $h "$*"; fi; }
stop_all() {
  echo "[$(date +%T)] 触发护栏：$1，停压测与我们的容器" >> $OUT
  for p in $(ps -eo pid,cmd | grep -E "[b]ash (bench_std|perf5_run|scale|health10w_check)\.sh" | awk '{print $1}'); do kill $p; done
  docker exec shardpg-fix-client pkill pgbench 2>/dev/null
  for h in 10.77.110.145 10.77.110.147 10.77.110.148 10.77.110.144; do
    onhost $h "docker ps --format '{{.Names}}' | grep -E '^(shardpg-fix-|pgsync-)' | xargs -r docker stop -t 30 > /dev/null"
  done
  echo "[$(date +%T)] 已停" >> $OUT; echo "STD_DONE（磁盘护栏中止）" >> $WATCH
}
echo "[$(date +%T)] 护栏启动：$LIMITS" >> $OUT
n=0
while ! grep -q STD_DONE $WATCH 2>/dev/null; do
  line=""
  for x in $LIMITS; do
    IFS=: read h m lim <<< "$x"
    free=$(onhost $h "df -BG --output=avail $m | tail -1 | tr -dc 0-9")
    [ -z "$free" ] && continue       # 一时连不上就跳过这一轮，不误杀
    line="$line ${h##*.}:$m=${free}G"
    [ "$free" -lt "$lim" ] && { stop_all "${h} ${m} 只剩 ${free} GB（底线 ${lim} GB）"; exit 0; }
  done
  n=$((n+1)); [ $((n % 10)) = 1 ] && echo "[$(date +%T)]$line" >> $OUT    # 每 5 min 记一次余量
  sleep 30
done
echo "[$(date +%T)] 压测结束，护栏退出" >> $OUT
