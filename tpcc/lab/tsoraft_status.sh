#!/bin/bash
# 各节点：控制面 leader、本节点 TSO 发号状态（raft_term > 0 且租约未过期 = 正在发号）
P="docker exec -i shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
# NODES 可覆盖（4 个 worker 部署时加上 n5）
for hp in ${NODES:-10.77.110.148:31080 10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083}; do
  echo "$hp $($P -h ${hp%:*} -p ${hp#*:} -c "select 'leader=' || partdist.pg_raft_get_leader() || ' ' || partdist.partdist_tso_status()" 2>&1 | head -1 | sed -E 's/ node[0-9]+=[0-9]+//g')"
done
