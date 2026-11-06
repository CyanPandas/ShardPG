# 压测中段采两机 CPU（vmstat）与磁盘（iostat）
cd /data1/yyt/shardpg-fix-20261014/tpcc
docker exec shardpg-fix-client bash -c "cd /t && bash scripts/run.sh ${1:-hs} >/dev/null 2>&1" &
sleep 20
echo "== 145 vmstat"; vmstat 5 3 | tail -3
echo "== 147 vmstat"; ssh -n 10.77.110.147 "vmstat 5 3 | tail -3"
echo "== 145 iostat"; iostat -x 5 2 2>/dev/null | awk '/^Device/ {h=$0} /^sd|^nvme/ {l[$1]=$0} END {print h; for (k in l) print l[k]}'
echo "== 147 iostat"; ssh -n 10.77.110.147 "iostat -x 5 2 2>/dev/null" | awk '/^Device/ {h=$0} /^sd|^nvme/ {l[$1]=$0} END {print h; for (k in l) print l[k]}'
echo "== 145 top postgres cpu"; top -b -n1 -o %CPU | head -20 | tail -13
wait
D=$(ls -td results/*/ | head -1); sed -n 3p $D/summary.txt
