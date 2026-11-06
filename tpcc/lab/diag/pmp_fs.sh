# 容器内：抓复制流接收会话（application_name=pg_raft_stream）的栈，不论状态
P=/work/pg-install/bin/psql
pids=$($P -h 127.0.0.1 -p ${2:-31082} -U postgres -XAtq -c "select pid from pg_stat_activity where application_name='pg_raft_stream'")
for i in $(seq ${1:-6}); do
  for p in $pids; do
    timeout 5 gdb -q -batch -p $p -ex "bt 40" 2>/dev/null | grep -E '^#' | sed -E 's/^#[0-9]+ +(0x[0-9a-f]+ in )?//; s/ \(.*//' | grep -vE '^(\?\?|ExecInterpExpr|ExecEvalFunc|ExecScan|ExecProcNode|ExecutePlan|standard_ExecutorRun|CitusExecutorRun|_SPI_execute_plan|SPI_execute|ExecResult|PortalRun|PortalRunSelect|ExecModifyTable|PostgresMain|ServerLoop|PostmasterMain|main|BackendRun|BackendStartup|exec_execute_message|ExecMakeTableFunctionResult|FunctionNext|ExecutorRun)' | head -12 | tr '\n' '<'
    echo
  done
done | grep -v '^$' | sort | uniq -c | sort -rn | head -14
