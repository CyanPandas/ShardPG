#!/bin/bash
# 重建镜像 + 恢复干净集群 + 带负载硬杀主 / 旧主回归隔离检查
R=/data1/yyt/shardpg-fix-20261014; cd $R
bash restore_routea.sh
bash killload_check.sh
