# 容器内：抓 TPC-C 事务后端（citus_internal）的栈
P=/work/pg-install/bin/psql
for i in $(seq ${1:-4}); do
  for p in $($P -h 127.0.0.1 -p ${2:-31082} -U postgres -XAtq -c "select pid from pg_stat_activity where state='active' and application_name like 'citus_internal%' order by random() limit 8"); do
    timeout 5 gdb -q -batch -p $p -ex "bt 40" 2>/dev/null | grep -E '^#' | sed -E 's/^#[0-9]+ +(0x[0-9a-f]+ in )?//; s/ \(.*//' | grep -vE '^(\?\?|ExecInterpExpr|ExecEvalFunc|ExecScan|ExecProcNode|ExecutePlan|standard_ExecutorRun|CitusExecutorRun|_SPI_execute_plan|SPI_execute|ExecResult|PortalRun|PortalRunSelect|ExecModifyTable|PostgresMain|ServerLoop|PostmasterMain|main|BackendRun|BackendStartup|exec_simple_query|ExecMakeTableFunctionResult|FunctionNext|ExecutorRun|fmgr_sql|postquel_getnext|ExecEvalExprSwitchContext|plpgsql_exec_function|exec_stmts|exec_stmt_block|plpgsql_call_handler|exec_stmt_execsql|exec_run_select|SPI_execute_plan_with_paramlist)$' | head -12 | tr '\n' '<'
    echo
  done
done | grep -v '^$' | sort | uniq -c | sort -rn | head -16
