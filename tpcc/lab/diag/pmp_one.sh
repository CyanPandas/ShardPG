# 容器内：抓来自某地址的复制流接收会话的栈
P=/work/pg-install/bin/psql
pid=$($P -h 127.0.0.1 -p ${2:-31082} -U postgres -XAtq -c "select pid from pg_stat_activity where application_name='pg_raft_stream' and client_addr='${3:-10.77.110.147}' limit 1")
echo "pid=$pid $($P -h 127.0.0.1 -p ${2:-31082} -U postgres -XAtq -c "select state, wait_event_type, wait_event, now()-query_start from pg_stat_activity where pid=$pid")"
for i in $(seq ${1:-5}); do
  timeout 8 gdb -q -batch -p $pid -ex "bt 30" 2>/dev/null | grep -E '^#' | sed -E 's/^#[0-9]+ +(0x[0-9a-f]+ in )?//; s/ \(.*//' | head -22 | tr '\n' '<'
  echo
  sleep 1
done | sort | uniq -c | sort -rn
