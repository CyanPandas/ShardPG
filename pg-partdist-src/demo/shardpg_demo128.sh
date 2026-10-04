#!/usr/bin/env bash
# ============================================================================
# shardpg_demo128.sh —— ShardPG 演示二：128 个分区组上的分布式事务（只用于演示，不改动项目任何文件）
#
#   bash shardpg_demo128.sh start      准备：建 wallet 表（128 个分片）→ 每片一个 Raft 组（副本因子 3）→ 选主 → 供副本 → 打标
#   bash shardpg_demo128.sh sql A      打开会话 A（连 master 的交互式 psql，提示符 A>）
#   bash shardpg_demo128.sh sql B      打开会话 B（第二个窗口，演示并发事务）
#   bash shardpg_demo128.sh stop       演示结束：拆组、删表、删登记、参数还原，恢复原环境
#   bash shardpg_demo128.sh auto       全自动演示（步骤见 shardpg_demo128_steps.sql）
#
# 与演示一（shardpg_demo.sh，4 个分片）共用同一套集群、同一个启动器，两套互斥：
# 换演示前先 stop（哪一个的 stop 都能把两套的表清掉）。
# ============================================================================
DEMO_PROFILE=128 exec bash "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/shardpg_demo.sh" "$@"
