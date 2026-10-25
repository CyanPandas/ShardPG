#!/bin/bash
# 2026-10-21：决议槽修复（镜像 shardpg:dtxfix）的验证。等 PG 300 仓（bench_std_pg300.out）测完之后：
#   1 4 台机器全新部署 30 标准仓（148 的数据放 /data3/yyt，不写 148 的 /home）
#   2 确定性竞态验证 dtx_race_check.sh（3 轮）
#   3 TPC-C 回归：热身 200 终端，100 / 200 / 300 / 400 终端各一轮（每轮 60 s + 3 min），收尾核对标准一致性与跨节点不变式
# 用法（145 上，nohup 后台）：verify_dtxfix.sh
R=/data1/yyt/shardpg-fix-20261014; L=$R/tsoraft_logs; OUT=$L/verify_dtxfix.out; cd $R
log() { echo "[$(date +%T)] $*" >> $OUT; }
while ! grep -q STD_DONE $L/bench_std_pg300.out 2>/dev/null; do sleep 30; done
grep -q "触发护栏" $L/disk_guard.txt && { log "PG 阶段触发过磁盘护栏，不做验证"; exit 1; }
sleep 60      # 等上一阶段的护栏与采样进程退出
(nohup bash disk_guard.sh $OUT > /dev/null 2>&1 < /dev/null &)
log "== 1 全新部署（shardpg:dtxfix，30 仓，148 的数据在 /data3）"
KIT=tpcc-std BASE148=/data3/yyt/shardpg-fix-20261014 bash redeploy4m.sh shardpg:dtxfix > $L/deploy-dtxfix.log 2>&1
grep -q "部署完成" $L/deploy-dtxfix.log || { log "部署失败：$(tail -3 $L/deploy-dtxfix.log | tr '\n' ' ')"; echo STD_DONE >> $OUT; exit 1; }
log "$(grep -E '^\[' $L/deploy-dtxfix.log | grep -E 'shardpg:prof|部署完成' | tr '\n' ' ')"
CLI="docker exec shardpg-fix-client /work/pg-install/bin/psql -U postgres -XAtq"
for i in $(seq 120); do $CLI -h 10.77.110.144 -p 31080 -c "select partdist.partdist_tso_status()" 2>/dev/null | grep -qE 'raft_term=[1-9]' && break; sleep 1; done
log "== 2 确定性竞态验证"
bash dtx_race_check.sh 3 >> $OUT 2>&1
log "== 3 TPC-C 回归（30 仓）"
log "热身（不计）：$(SCALE_DUR=120 bash scale.sh shardpg-std 200 2>&1 | tail -1)"
for t in 100 200 300 400; do log "$(bash scale.sh shardpg-std $t 2>&1 | tail -1)"; done
sleep 30
C1="$CLI -h 10.77.110.144 -p 31080"
log "收尾核对：标准 [$($C1 -f /t/sql/90_consistency.sql 2>&1 | grep -E '^ *[0-9]+ *$|ERROR' | tr '\n' ' ')] 跨节点 [$($C1 -f /t/xnode_consistency.sql 2>&1 | grep -E '^[0-9]+\||ERROR' | tr '\n' ' ')]"
log "各节点日志里决议锁等待（决议锁被并发写入者持有）：$(for hn in 10.77.110.145:n2 10.77.110.147:n3 10.77.110.148:n4; do h=${hn%:*}; if [ $h = 10.77.110.145 ]; then c="bash -c"; else c="ssh -n $h"; fi; echo -n "${hn#*:}=$($c "docker exec shardpg-fix-${hn#*:} bash -c 'cat /d/pgdata/log/*.log'" | grep -c '决议锁被并发写入者持有') "; done)"
echo "STD_DONE" >> $OUT
