#!/bin/bash
# 重建镜像 + 恢复干净集群 + 切主正确性检查
R=/data1/yyt/shardpg-fix-20261014; cd $R
bash restore_routea.sh
bash failover_check.sh
