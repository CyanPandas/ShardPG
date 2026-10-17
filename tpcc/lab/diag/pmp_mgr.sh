# 容器内：抓复制流管理进程（postgres: pg_raft stream）的栈
p=$(pgrep -f 'pg_raft stream' | head -1)
for i in $(seq ${1:-10}); do
  timeout 5 gdb -q -batch -p $p -ex "bt 25" 2>/dev/null | grep -E '^#' | sed -E 's/^#[0-9]+ +(0x[0-9a-f]+ in )?//; s/ \(.*//' | grep -vE '^(\?\?|ExecInterpExpr|ExecEvalFunc|ExecScan|ExecProcNode|ExecutePlan|standard_ExecutorRun|CitusExecutorRun|_SPI_execute_plan|SPI_execute|ExecResult|PortalRun|PortalRunSelect|ExecModifyTable)' | head -14 | tr '\n' '<'
  echo
  sleep 0.3
done | grep -v '^$' | sort | uniq -c | sort -rn | head -12
