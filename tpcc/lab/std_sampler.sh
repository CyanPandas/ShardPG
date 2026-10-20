#!/bin/bash
# 标准规模压测（bench_std.sh）期间每 60 s 采样一次，判断是否受磁盘限制：
#   各机数据盘 10 s 繁忙度；活跃会话等待事件分布（PG 阶段看主库，ShardPG 阶段看三个 worker 合计）；PG 阶段的备库 flush 落后量
#   依赖 /tmp/yyt-busy.sh（= lab/disk_busy.sh，145 上先拷过去，脚本会再分发到 147 / 148 / 144）
R=/data1/yyt/shardpg-fix-20261014; OUT=$R/tsoraft_logs/std-sampler.txt
for h in 10.77.110.147 10.77.110.148 10.77.110.144; do scp -q /tmp/yyt-busy.sh $h:/tmp/; done
W="select coalesce(wait_event_type,'CPU')||':'||coalesce(wait_event,'-') e, count(*) n from pg_stat_activity where backend_type='client backend' and state='active' and pid <> pg_backend_pid() group by 1"
while ! grep -q STD_DONE $R/tsoraft_logs/bench_std.out; do
  b=$( { echo "145 $(bash /tmp/yyt-busy.sh /data1)" & echo "147 $(ssh -n 10.77.110.147 bash /tmp/yyt-busy.sh /home)" & echo "148 $(ssh -n 10.77.110.148 bash /tmp/yyt-busy.sh /home)" & echo "144 $(ssh -n 10.77.110.144 bash /tmp/yyt-busy.sh /home)" & wait; } | sort | tr '\n' ' ')
  if docker ps --format '{{.Names}}' | grep -q '^pgsync-client$'; then
    P="docker exec pgsync-client /work/pg-install/bin/psql -h 10.77.110.145 -p 31300 -U postgres -XAtq -F="
    w=$($P -c "$W order by 2 desc limit 4" | tr '\n' ' ')
    r=$($P -c "select application_name||' 落后 '||pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn)) from pg_stat_replication order by 1" | tr '\n' ' ')
    echo "[$(date +%T)] PG  盘[$b] 等待[$w] $r" >> $OUT
  elif docker ps --format '{{.Names}}' | grep -q '^shardpg-fix-client$'; then
    w=$(for hp in 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083; do
          docker exec shardpg-fix-client /work/pg-install/bin/psql -h ${hp%:*} -p ${hp#*:} -U postgres -XAtq -F' ' -c "$W" 2>/dev/null; done |
        awk '{s[$1]+=$2} END{for (k in s) print s[k], k}' | sort -rn | head -4 | awk '{printf "%s=%s ", $2, $1}')
    echo "[$(date +%T)] SP  盘[$b] worker 等待[$w]" >> $OUT
  fi
  sleep 50
done
