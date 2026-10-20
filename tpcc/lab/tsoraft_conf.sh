#!/bin/bash
# 在四个节点的配置文件末尾加上 / 去掉"TSO 随控制面 leader"一段（重新部署后生效）。
# 用法（145 上）：tsoraft_conf.sh on | off
# 文件：145 的 tpcc/conf/out/node{1..4}.conf（node2 在本机用），147 的 conf/node3.conf，148 的 conf/node{1,4}.conf
R=/data1/yyt/shardpg-fix-20261014; T=/home/yyt/shardpg-fix-20261014
MODE=${1:?用法：tsoraft_conf.sh on|off}
LIST="host=10.77.110.148 port=31080 dbname=postgres user=postgres connect_timeout=2;host=10.77.110.145 port=31082 dbname=postgres user=postgres connect_timeout=2;host=10.77.110.147 port=31081 dbname=postgres user=postgres connect_timeout=2;host=10.77.110.148 port=31083 dbname=postgres user=postgres connect_timeout=2"
EDIT="sed -i '/^# ---- TSO 随控制面 leader（TSO_RAFT=1）/,\$d'"
ADD="printf '%s\n' '# ---- TSO 随控制面 leader（TSO_RAFT=1）：协调者宕机后由新 leader 接着发号 ----' 'pg_partdist.tso_raft = on' \"pg_partdist.tso_conninfo = '$LIST'\""
one() {   # $1 = 主机（localhost 或 IP），$2 = 文件
  local cmd="$EDIT $2"
  [ "$MODE" = on ] && cmd="$cmd && $ADD >> $2"
  if [ "$1" = localhost ]; then bash -c "$cmd"; else ssh -n $1 "$cmd"; fi
}
for i in 1 2 3 4; do one localhost $R/tpcc/conf/out/node$i.conf; done
one 10.77.110.147 $T/conf/node3.conf
one 10.77.110.148 $T/conf/node1.conf
one 10.77.110.148 $T/conf/node4.conf
echo "TSO 随控制面 leader：$MODE"
for f in "localhost $R/tpcc/conf/out/node2.conf" "10.77.110.147 $T/conf/node3.conf" "10.77.110.148 $T/conf/node1.conf" "10.77.110.148 $T/conf/node4.conf"; do
  set -- $f; if [ $1 = localhost ]; then c="bash -c"; else c="ssh -n $1"; fi
  echo "  $1 $(basename $2): $($c "grep -c '^pg_partdist.tso_raft = on' $2")"
done
