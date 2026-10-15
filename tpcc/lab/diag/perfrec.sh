# 容器内：对全部 postgres 进程 perf record 10 s，出扁平符号报告
PERF="/tmp/perflib/ld-linux-x86-64.so.2 --library-path /tmp/perflib /tmp/perf"
pids=$(pgrep -d, postgres)
cd /tmp && $PERF record -F 499 -p "$pids" -o /tmp/perf.data -- sleep ${1:-10} >/dev/null 2>&1
$PERF report -i /tmp/perf.data --no-children --sort dso,symbol --stdio 2>/dev/null | grep -v '^#' | grep -v '^$' | head -${2:-60}
