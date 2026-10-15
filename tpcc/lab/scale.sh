# 用法：scale.sh <shardpg|native> <终端数...>  依次按不同终端数各跑 1 min（同一库），打印 tpmC/tps/延迟
SYS=$1; shift
if [ "$SYS" = native ]; then D=/data1/yyt/shardpg-fix-20261014/tpcc-native; C=pg-native-client; else D=/data1/yyt/shardpg-fix-20261014/tpcc; C=shardpg-fix-client; fi
cd $D; cp cluster.env /tmp/cluster.env.$SYS.bak
for t in "$@"; do
  sed -i "s/^TERMINALS=.*/TERMINALS=$t/" cluster.env
  docker exec $C bash -c "cd /t && bash scripts/run.sh $SYS-t$t >/dev/null 2>&1"
  R=$(ls -td results/*/ | head -1)
  echo "$SYS t=$t $(sed -n 3p $R/summary.txt | grep -oE '= [0-9.]+') $(grep -E '^tps' $R/pgbench.out | cut -c1-16) $(grep -E '^latency average' $R/pgbench.out | head -1) viol$(sed -n 5p $R/summary.txt | grep -oE '= [0-9]+')"
done
cp /tmp/cluster.env.$SYS.bak cluster.env
