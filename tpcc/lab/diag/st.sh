#!/bin/bash
# 用法（145 上）：st.sh —— 每个 worker：leader 组的最大 (last-commit)/(commit-applied)、活跃会话等待事件、管理进程最近日志
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do h=${hp%:*}; p=${hp#*:}
  echo "== $hp $($CLI -h $h -p $p -c "select 'maxlag_c='||max(last_log_index-commit_index)||' maxlag_a='||max(commit_index-last_applied)||' sum_last='||sum(last_log_index) from partdist.pg_raft_group_status() where group_id<>0 and state='leader'") $($CLI -h $h -p $p -c "select string_agg(w||'x'||c, ' ') from (select coalesce(wait_event,'cpu') w, count(*) c from pg_stat_activity where state='active' and pid<>pg_backend_pid() group by 1 order by 2 desc limit 5) z")"
done
