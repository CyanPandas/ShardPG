#!/bin/bash
# 两轮：协调者自动切主（协调者带备库）→ 带负载硬杀 worker 主库 + 旧主回归自我隔离
R=/data1/yyt/shardpg-fix-20261014; cd $R
echo "===== 协调者切主 ====="; COORD_HA=1 bash routea_coordfailover_round.sh
echo "===== 带负载硬杀 n3 ====="; COORD_HA=0 bash routea_killload_round.sh
