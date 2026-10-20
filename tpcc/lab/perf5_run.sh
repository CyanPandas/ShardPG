#!/bin/bash
# 2026-10-20 标准规模对比的一组压测：热身 200 终端（预热 60 s + 2 min，不计）→ 100 / 200 / 300 / 400 终端
# 各一轮（预热 RAMPUP_S + 测量 DURATION_S，见套件 cluster.env；30 仓那组是 60 s + 3 min），跑两遍；每轮前后不做一致性检查（CONSISTENCY_EACH=0），全部跑完后统一核对：
# 标准 90_consistency.sql + 跨节点不变式 xnode_consistency.sql（标准规模版）。
# 另记：数据节点缓冲池命中（blks_hit / blks_read）、第 1 遍 400 终端第 4 分钟各容器内存、有没有后端被 OOM 杀掉。
# 用法（145 上，集群已部署、已装载、备库已追平）：perf5_run.sh <pgsync-std|shardpg-std> <标签>
R=/data1/yyt/shardpg-fix-20261014; cd $R
SYS=${1:?系统}; TAG=${2:?标签}; OUT=$R/tsoraft_logs/perf5-$TAG.txt
if [ $SYS = pgsync-std ]; then C=pgsync-client; COORD=10.77.110.145:31300; NODES=$COORD; PAT='^pgsync-(primary|standby)'
else C=shardpg-fix-client; COORD=10.77.110.144:31080; NODES="10.77.110.145:31082 10.77.110.147:31081 10.77.110.148:31083"; PAT='^shardpg-fix-n[0-9]'; fi
PSQL="docker exec $C /work/pg-install/bin/psql -U postgres -XAtq"
log() { echo "[$(date +%T)] $*" | tee -a $OUT; }
onhost() { local h=$1; shift; if [ $h = 10.77.110.145 ]; then bash -c "$*"; else ssh -n $h "$*"; fi; }
blks() { local h=0 r=0 hp x; for hp in $NODES; do
    x=$($PSQL -h ${hp%:*} -p ${hp#*:} -c "select sum(blks_hit) || ' ' || sum(blks_read) from pg_stat_database"); set -- $x; h=$((h+$1)); r=$((r+$2)); done
  echo "$h $r"; }
memsnap() { local h; for h in 10.77.110.144 10.77.110.145 10.77.110.147 10.77.110.148; do
    onhost $h "docker ps --format '{{.Names}}' | grep -E '$PAT' | xargs -r docker stats --no-stream --format '{{.Name}} {{.MemUsage}}'" 2>/dev/null | sed "s/^/  $h /"; done; }
oomchk() { local h n=0 x; for h in 10.77.110.144 10.77.110.145 10.77.110.147 10.77.110.148; do
    x=$(onhost $h "for c in \$(docker ps --format '{{.Names}}' | grep -E '$PAT'); do { docker logs \$c 2>&1; docker exec \$c bash -c 'cat /d/pgdata/log/*.log 2>/dev/null'; } | grep -c 'terminated by signal 9'; done" 2>/dev/null | awk '{s+=$1} END{print s+0}')
    n=$((n + ${x:-0})); done; echo $n; }
log "[$TAG] 热身（不计）：$(SCALE_DUR=120 bash scale.sh $SYS 200 2>&1 | tail -1)"
b0=$(blks)
for rnd in 1 2; do
  for t in 100 200 300 400; do
    [ $rnd = 1 ] && [ $t = 400 ] && ( sleep 240; memsnap > $R/tsoraft_logs/perf5-$TAG-mem400.txt ) &
    log "[$TAG] 第 $rnd 遍 $(bash scale.sh $SYS $t 2>&1 | tail -1)"
  done
done
wait
b1=$(blks); set -- $b0 $b1
log "[$TAG] 8 轮合计数据节点缓冲池：命中 $(( $3 - $1 )) 块、读入 $(( $4 - $2 )) 块，命中率 $(awk -v h=$(( $3 - $1 )) -v r=$(( $4 - $2 )) 'BEGIN{printf "%.2f%%", 100*h/(h+r)}')"
log "[$TAG] 第 1 遍 400 终端第 4 分钟各容器内存："; cat $R/tsoraft_logs/perf5-$TAG-mem400.txt | tee -a $OUT
log "[$TAG] 后端被 OOM 杀掉（terminated by signal 9）次数：$(oomchk)"
sleep 30    # 等 2PC 全部收尾再核对
QC="$PSQL -h ${COORD%:*} -p ${COORD#*:}"
log "[$TAG] 收尾核对：标准 [$($QC -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($QC -f /t/xnode_consistency.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"
log "[$TAG] 完成"
