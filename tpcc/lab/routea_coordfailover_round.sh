#!/bin/bash
# 重建镜像 + 恢复干净集群（协调者也带两个同步备库）+ 协调者自动切主检查
R=/data1/yyt/shardpg-fix-20261014; cd $R
export COORD_HA=1
bash restore_routea.sh
bash coordfailover_check.sh
