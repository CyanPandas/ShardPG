# pg_test 测试环境 ↔ GitHub 分支 `shardpg-test` 强绑定说明

> 建立日期：2026-09-09。目的：给实验性改动一块独立场地，
> **不干扰 `shardpg-TX2`（tx2 线）与 `~/shardpg-demo`（演示材料）**。

## 1. 绑定关系（三处一一对应）

| 角色 | 位置 | 说明 |
|---|---|---|
| GitHub 分支 | `shardpg-test` | 从 `shardpg-TX2` 尖端 `efc6a2e` 原样分出，建时零差异 |
| 开发工作区 | `/home/zhanhao/shardpg-test-work` | **唯一**写代码/文档的地方；origin 带 token，已装推送守卫 |
| 容器环境 | `pg-test-container`（镜像 `pg-partdist-raft4-env`） | 1 coordinator + 8 workers，仅作构建/运行沙箱 |
| 一次性克隆 | `/home/zhanhao/pg_test/ShardPG` | `reproduce-env.sh` 建环境用的克隆，**不要当工作区** |

节点与端口：`coordinator = node1:5432`，`worker1..worker8 = node2..node9:5433..5440`。

## 2. 隔离保证（怎么保证 tx2 / demo 不受影响）

1. **分支侧**：`~/shardpg-test-work/.git/hooks/pre-push` 拒绝一切目标不是
   `refs/heads/shardpg-test` 的推送；`remote.origin.push` 也钉死成
   `refs/heads/shardpg-test:refs/heads/shardpg-test`，裸 `git push` 只会推本分支。
2. **目录侧**：本线的一切产出只落 `~/shardpg-test-work`；
   `~/shardpg-tx2-work`（tx2 工作区）与 `~/shardpg-demo`（演示文档）一律不碰。
3. **容器侧**：`pg-test-container` 与 `pg-citus-tx2-container` 是两个独立容器，
   数据目录互不可见。

## 3. 宿主机只放得下一套 9 节点环境（硬约束）

宿主机 2 核 / 3.8GB。现有 9 节点环境共四套，**必须二选一**：
`pg-citus-replay-container` / `pg-citus-tx-container` / `pg-citus-tx2-container` / `pg-test-container`。

切换方式（`stop` 保留数据，`start` 即恢复）：

```bash
# 切到 pg_test
docker stop pg-citus-tx2-container && docker start pg-test-container
# 切回 tx2
docker stop pg-test-container && docker start pg-citus-tx2-container
```

并存的后果在 tx2 那边实测过：OOM + 连环改选 + 数据组丢主，症状会伪装成回放缺陷。

## 4. 常用命令

```bash
# 重建/校验/销毁（脚本在本工作区里，用本分支那份）
ENV_NAME=pg_test BRANCH=shardpg-test \
  bash ~/shardpg-test-work/pg-raft-src/reproduce-env.sh {up|verify|test|destroy}

# 连库
docker exec -it -u postgres pg-test-container \
  /work/pg-install/bin/psql -p 5432 -U postgres -d postgres

# 把工作区代码同步进容器后重编扩展
docker cp ~/shardpg-test-work/pg-partdist-src pg-test-container:/work/
docker exec -i -u postgres pg-test-container bash -c \
  'cd /work/pg-partdist-src && make -s PG_CONFIG=/work/pg-install/bin/pg_config && \
   make -s install PG_CONFIG=/work/pg-install/bin/pg_config'
```

清理（`verify_cleanup.sh` 不覆盖本环境）：

```bash
docker rm -f pg-test-container && rm -rf /home/zhanhao/pg_test
# 工作区另删：rm -rf /home/zhanhao/shardpg-test-work
```

## 5. 建环境时的验收结果

见本文件末尾「验收记录」一节。

### 验收记录

| 时间 | 动作 | 结果 |
|---|---|---|
| 2026-09-09 | `reproduce-env.sh up` | 9 节点全起，group0 **1s 收敛，leader = node1**，`/work/REPRODUCED_FROM_COMMIT` = `efc6a2e` |
| 2026-09-09 | `reproduce-env.sh verify`（V1–V6） | **PASS=25 FAIL=0**，与 pg-citus-tx2 建环境时的基线一字不差 |

全量套件（`reproduce-env.sh test`，十套件、数十分钟）**建环境时没跑**；
本分支代码与 `shardpg-TX2` 尖端零差异，回归数字沿用 TX2 的记录，需要时再跑。
