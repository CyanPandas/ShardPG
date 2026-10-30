#!/bin/bash
# 2026-10-21 晚：PG 300 仓（bench300b_pg.out）测完后接着做：
#   1 新镜像（follower 收日志改回并行，撞主键在子事务里显式改做 UPDATE）在 30 仓上跑全面健康检查 health4m_check.sh
#   2 部署成功、竞态验证没有失败，就重测 ShardPG 300 仓（同口径，标签 sp300c）
# 每段都开磁盘护栏；任何一段触发护栏就停。
R=/data1/yyt/shardpg-fix-20261014; L=$R/tsoraft_logs; cd $R; OUT=$L/after_pg300b.out
log() { echo "[$(date +%T)] $*" >> $OUT; }
g0=$(grep -c "触发护栏" $L/disk_guard.txt 2>/dev/null)
guard_hit() { [ "$(grep -c "触发护栏" $L/disk_guard.txt 2>/dev/null)" != "$g0" ]; }
while ! grep -q STD_DONE $L/bench300b_pg.out 2>/dev/null; do sleep 30; done
guard_hit && { log "PG 段触发过磁盘护栏，停"; exit 1; }
sleep 60
log "== 1 健康检查（30 仓，shardpg:noconflict 新版）"
[ -f $L/health4m.out ] && mv $L/health4m.out $L/health4m.run2.out
(nohup bash disk_guard.sh $L/health4m.out > /dev/null 2>&1 < /dev/null &)
bash health4m_check.sh shardpg:noconflict > $L/health4m.out 2>&1; echo STD_DONE >> $L/health4m.out
log "健康检查结束：$(grep -cE 'RACE_PASS' $L/health4m.out) 组竞态通过，$(grep -c 'RACE_FAIL' $L/health4m.out) 组失败；$(grep '写入报错' $L/health4m.out | sed 's/^ *//')"
guard_hit && { log "健康检查触发过磁盘护栏，停"; exit 1; }
grep -q "部署完成" $L/health4m.out && ! grep -q "RACE_FAIL" $L/health4m.out || { log "健康检查没过，不重测 300 仓"; exit 1; }
sleep 60
log "== 2 重测 ShardPG 300 仓（sp300c）"
(nohup bash disk_guard.sh $L/bench300c_sp.out > /dev/null 2>&1 < /dev/null &)
(WATCH=$L/bench300c_sp.out OUT=$L/std-sampler300c-sp.txt M148=/data3 nohup bash std_sampler.sh > /dev/null 2>&1 < /dev/null &)
SKIP_PG=1 SPKIT=tpcc-std300 SPTAG=sp300c SPIMG=shardpg:noconflict BASE148=/data3/yyt/shardpg-fix-20261014 \
  bash bench_std.sh > $L/bench300c_sp.out 2>&1
grep -q STD_DONE $L/bench300c_sp.out || echo "== STD_DONE（异常结束）" >> $L/bench300c_sp.out
log "ShardPG 300 仓重测结束"
log "AFTER_DONE"
