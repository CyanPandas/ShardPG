# 容器内：带调用图（dwarf）录 5 s，看 LWLockAttemptLock / hash_search 的调用者
PERF="/tmp/perflib/ld-linux-x86-64.so.2 --library-path /tmp/perflib /tmp/perf"
pids=$(pgrep -d, postgres)
cd /tmp && $PERF record -F 199 --call-graph dwarf,16384 -p "$pids" -o /tmp/perfcg.data -- sleep ${1:-5} >/dev/null 2>&1
for sym in LWLockAttemptLock hash_search_with_hash_value; do
  echo "=== callers of $sym"
  $PERF report -i /tmp/perfcg.data --no-children --stdio -S $sym -G --percent-limit 2 2>/dev/null | grep -v '^#' | grep -v '^$' | head -60
done
