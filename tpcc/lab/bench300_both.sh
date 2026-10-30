#!/bin/bash
# 2026-10-21 晚：用修复后的代码（镜像 shardpg:noconflict）按同一口径测 300 标准仓：
#   先 ShardPG（4 台机器：协调者 144 + 3 worker，每个 worker 一主两备 ANY 1），再三机一主两备 PostgreSQL。
#   每轮预热 60 s + 测量 3 min，终端 100 / 200 / 300 / 400，跑两遍（perf5_run.sh）；两边都全新部署、全新装载，
#   148 上的数据都放 /data3/yyt（148 的 /home 不够）；每段都开磁盘护栏与采样，ShardPG 段触发护栏就不接着测 PG。
# 用法（145 上，nohup 后台）：bench300_both.sh
R=/data1/yyt/shardpg-fix-20261014; L=$R/tsoraft_logs; cd $R
OUTS=$L/bench300b_sp.out; OUTP=$L/bench300b_pg.out
g0=$(grep -c "触发护栏" $L/disk_guard.txt 2>/dev/null)
guard_hit() { [ "$(grep -c "触发护栏" $L/disk_guard.txt 2>/dev/null)" != "$g0" ]; }

(nohup bash disk_guard.sh $OUTS > /dev/null 2>&1 < /dev/null &)
(WATCH=$OUTS OUT=$L/std-sampler300b-sp.txt M148=/data3 nohup bash std_sampler.sh > /dev/null 2>&1 < /dev/null &)
SKIP_PG=1 SPKIT=tpcc-std300 SPTAG=sp300b SPIMG=shardpg:noconflict BASE148=/data3/yyt/shardpg-fix-20261014 \
  bash bench_std.sh > $OUTS 2>&1
grep -q STD_DONE $OUTS || echo "== STD_DONE（异常结束）" >> $OUTS
guard_hit && { echo "[$(date +%T)] ShardPG 段触发过磁盘护栏，不接着测 PG" >> $OUTP; exit 1; }
sleep 60      # 等上一段的护栏与采样进程退出

echo "[$(date +%T)] 清掉上一次 PG 的旧数据目录（我们自己的 pgsync3/data）" >> $OUTP
bash pgsync3.sh destroy > $L/std-pg300b-destroy.log 2>&1
PG_BASE148=/data3/yyt/pgsync3 bash pgsync3.sh destroy >> $L/std-pg300b-destroy.log 2>&1
ssh -n 10.77.110.148 "mkdir -p /data3/yyt/pgsync3"
(nohup bash disk_guard.sh $OUTP > /dev/null 2>&1 < /dev/null &)
(WATCH=$OUTP OUT=$L/std-sampler300b-pg.txt M148=/data3 nohup bash std_sampler.sh > /dev/null 2>&1 < /dev/null &)
SKIP_SP=1 PGKIT=tpcc-pgsync-std300 PGTAG=pg300b PG_BASE148=/data3/yyt/pgsync3 PG_ASYNC_LOAD=1 \
  bash bench_std.sh >> $OUTP 2>&1
grep -q STD_DONE $OUTP || echo "== STD_DONE（异常结束）" >> $OUTP
