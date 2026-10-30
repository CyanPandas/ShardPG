#!/bin/bash
# 2026-10-21：ShardPG 300 仓（bench_std300.out）跑完之后，接着按同一口径测三机一主两备 PG 300 仓。
# ShardPG 阶段触发过磁盘护栏就不接着测。PG 148 上的备库放 /data3/yyt/pgsync3（148 的 /home 不够）；
# 装载期间临时关同步复制（只为省装载时间），装完恢复 ANY 1、等两个备库追平后才开始压测。
R=/data1/yyt/shardpg-fix-20261014; L=$R/tsoraft_logs; cd $R; OUT=$L/bench_std_pg300.out
while ! grep -q STD_DONE $L/bench_std300.out; do sleep 30; done
grep -q "触发护栏" $L/disk_guard.txt && { echo "[$(date +%T)] ShardPG 阶段触发过磁盘护栏，不接着测 PG" >> $OUT; exit 1; }
sleep 60      # 等上一阶段的护栏与采样进程退出
echo "[$(date +%T)] 清掉 30 仓 PG 的旧数据目录（我们自己的 pgsync3/data，148 上在 /home）" >> $OUT
bash pgsync3.sh destroy >> $L/std-pg300-deploy.log 2>&1
ssh -n 10.77.110.148 "mkdir -p /data3/yyt/pgsync3"
(nohup bash disk_guard.sh $OUT > /dev/null 2>&1 < /dev/null &)
(WATCH=$OUT OUT=$L/std-sampler-pg300.txt M148=/data3 nohup bash std_sampler.sh > /dev/null 2>&1 < /dev/null &)
SKIP_SP=1 PGKIT=tpcc-pgsync-std300 PGTAG=pg300 PG_BASE148=/data3/yyt/pgsync3 PG_ASYNC_LOAD=1 bash bench_std.sh >> $OUT 2>&1
