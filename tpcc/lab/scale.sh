# 用法：[SCALE_DUR=秒] scale.sh <shardpg|native|citus3r|pgsync|shardpg-std|pgsync-std> <终端数...>  依次按不同终端数各跑一轮（同一库，时长见套件 cluster.env），打印 tpmC/tps/延迟
SYS=$1; shift
if [ "$SYS" = native ]; then D=/data1/yyt/shardpg-fix-20261014/tpcc-native; C=pg-native-client; elif [ "$SYS" = citus3r ]; then D=/data1/yyt/shardpg-fix-20261014/tpcc-citus3r; C=citus3r-client; elif [ "$SYS" = pgsync ]; then D=/data1/yyt/shardpg-fix-20261014/tpcc-pgsync; C=pgsync-client; elif [ "$SYS" = pgsync-std ]; then D=/data1/yyt/shardpg-fix-20261014/tpcc-pgsync-std; C=pgsync-client; elif [ "$SYS" = shardpg-std ]; then D=/data1/yyt/shardpg-fix-20261014/tpcc-std; C=shardpg-fix-client; else D=/data1/yyt/shardpg-fix-20261014/tpcc; C=shardpg-fix-client; fi
cd $D; cp cluster.env /tmp/cluster.env.$SYS.bak
# SCALE_DUR=秒：临时改测量时长（如热身轮），跑完随 cluster.env 一起恢复
[ -n "${SCALE_DUR:-}" ] && sed -i "s/^DURATION_S=.*/DURATION_S=$SCALE_DUR/" cluster.env
for t in "$@"; do
  sed -i "s/^TERMINALS=.*/TERMINALS=$t/" cluster.env
  docker exec $C bash -c "cd /t && bash scripts/run.sh $SYS-t$t >/dev/null 2>&1"
  R=$(ls -td results/*/ | head -1)
  echo "$SYS t=$t $(sed -n 3p $R/summary.txt | grep -oE '= [0-9.]+') $(grep -E '^tps' $R/pgbench.out | cut -c1-16) $(grep -E '^latency average' $R/pgbench.out | head -1) viol$(sed -n 5p $R/summary.txt | grep -oE '= [0-9]+')"
done
cp /tmp/cluster.env.$SYS.bak cluster.env
