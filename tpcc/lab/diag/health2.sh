#!/bin/bash
# 用法（145 上）：health2.sh —— 领导权分布、复制流会话、日志计数
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083 10.77.110.148:31080; do
  h=${hp%:*}; p=${hp#*:}
  echo "$hp $($CLI -h $h -p $p -c "select 'leader='||(select count(*) from partdist.pg_raft_group_status() where state='leader' and group_id<>0)||' follower='||(select count(*) from partdist.pg_raft_group_status() where state<>'leader' and group_id<>0)||' stream_in='||(select count(*) from pg_stat_activity where application_name='pg_raft_stream')||' bgw='||(select count(*) from pg_stat_activity where backend_type='pg_raft stream')" 2>&1 | head -1)"
done
pat='复制流[^（(，,：:]*|未达多数派[^）)]*）?|ring full|PANIC|分叉|deadlock detected|拒绝升主|TupleDesc|snapshot.*leak|still referenced|server process.*terminated'
for c in shardpg-fix-n2; do echo "== $c"; docker exec $c bash -c "cat /d/pgdata/log/*.log 2>/dev/null | grep -oE '$pat' | sort | uniq -c | sort -rn | head -12"; done
ssh -n 10.77.110.147 "echo '== shardpg-fix-n3'; docker exec shardpg-fix-n3 bash -c \"cat /d/pgdata/log/*.log 2>/dev/null | grep -oE '$pat' | sort | uniq -c | sort -rn | head -12\""
ssh -n 10.77.110.148 "for c in shardpg-fix-n1 shardpg-fix-n4; do echo \"== \$c\"; docker exec \$c bash -c \"cat /d/pgdata/log/*.log 2>/dev/null | grep -oE '$pat' | sort | uniq -c | sort -rn | head -12\"; done"
