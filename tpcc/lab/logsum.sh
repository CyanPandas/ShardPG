# 汇总三节点日志里的 WARNING/ERROR（数字归一）
R=/data1/yyt/shardpg-fix-20261014
summ() { grep -hE "WARNING|ERROR|LOG:  pg_partdist: (shard_vacuum|未决)" | sed -E 's/^[0-9-]+ [0-9:.]+ [A-Z]+ \[[0-9]+\] //; s/[0-9]+/N/g' | sort | uniq -c | sort -rn | head -${1:-10} | cut -c1-200; }
for n in 1 2; do echo "== n$n"; docker exec shardpg-fix-n$n bash -c "cat /d/pgdata/log/*.log" | summ $1; done
echo "== n3"; ssh -n 10.77.110.147 "docker exec shardpg-fix-n3 bash -c \"cat /d/pgdata/log/*.log\"" | summ $1
