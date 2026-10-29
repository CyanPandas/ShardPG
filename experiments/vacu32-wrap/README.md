# 32 位分片 XID 回卷：147 验收

实现遵循冻结老 xmin、完整清理后复用、未回收区间小于半圈的方案。
分片事务号、发号/认领/清理水位和 clog 索引始终为 uint32，没有事务轮次。
详细设计见 [XID_WRAP32_IMPLEMENTATION.md](../../pg-partdist-src/docs/XID_WRAP32_IMPLEMENTATION.md)。

2026-10-29 验证已通过：生产32位本地合约9条，短环合约10条（600轮、347次号码
复用），P5 182项、P7 66项、普通三副本回放67项、短环三副本回放282项。
各套件零失败、无非预期崩溃；生产构建跳过一条仅供短环加速的多圈用例。
编译无警告，生产/短环两组C算术测试通过。

[`results.json`](results.json) 记录原始运行目录、构建输入及源码/证据校验和；
[`evidence/`](evidence) 保留成功套件的原始输出。P5/P7与副本专项分两轮运行，使用
同一C/SQL构建快照；先前未通过的引导/测试脚本问题修正后才记录对应成功结果。
完整的失败历史和数据库诊断仍保存在原 `/home/zhanhao/yyt/vacu32-wrap/logs`。

## 在已有147实验环境复现

脚本限定使用既有 `yyt-vacu-container`，要求其标签对应
`/home/yyt/vacu-bb62a57-20261029T074050Z`，以及既有镜像/编译工具。它们经历史
`2222 → 145 → 147` 隧道运行，不建立隧道、不下载镜像、不新建容器。
`VACU_SSH_CONFIG` 可指定另一份SSH配置；默认使用本目录的 `ssh-jump-145.conf`。

在包含本提交的工作树中执行：

```bash
bash experiments/vacu32-wrap/prepare-work.sh
bash experiments/vacu32-wrap/build-on-147.sh
bash experiments/vacu32-wrap/validate-on-147.sh
bash experiments/vacu32-wrap/regress-on-147.sh
```

`prepare-work.sh` 从当前提交创建独立源码快照，已有 `work` 时拒绝覆盖。
后续源码修改应先提交，再选择新的实验目录准备快照。测试入口会核对C/SQL与已编
快照一致；原yyt工作副本也可以继续使用同名脚本。

`validate-on-147.sh` 使用复制的安装和私有TSO/worker数据库验证本地合约；
`regress-on-147.sh` 使用1协调+3worker私有集群验证历史回归及三副本多圈回放。
后者支持 `--replay-only`，只执行普通及短环副本专项。所有新数据、安装副本及日志
位于专属临时子目录，154xx端口与原5432..5440分离；退出只停止本次创建的节点。

## 范围

真实32位边界由停止的空夹具注入临近最大值水位后跨越；完整多圈采用同一uint32
实现的短环测试构建。没有在真实32位空间连续烧完多整圈，也未替换原9节点安装或
业务数据库。运行期负向用例会故意触发ERROR；集群扩展安装前后台也可能短暂报告
schema不存在，因此“通过”指套件断言和健康检查通过，不代表日志中没有ERROR字样。
原实验08:33缺失LIKE源表DDL段错误未在本次任务中定因。

新水位格式和物理基线控制记录要求各副本使用兼容代码；没有验证混合版本或在线
滚动升级。生产构建默认32位；`SHARD_XID_RING_BITS=8`专供隔离测试。
