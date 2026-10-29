# yyt 独立 VACU 实验

本目录归档 `/home/zhanhao/yyt` 实验中实际使用的部署脚本、地址配置及验证报告。
代码副本的测试修正已合入仓库对应的 `pg-partdist-src/tests/` 路径。
`results.json` 保存验证结果摘要，`evidence-manifest.json` 保存原始日志的路径和 SHA256。
源码快照、数据库文件、原始日志和传输包继续保留在原实验目录，不随本目录入库。

这些脚本依赖独立实验根目录中的 `work/` 源码导出；本目录不包含该导出，不能直接
作为已初始化实验运行。现有实验仍在下面记录的 `/home/zhanhao/yyt` 及远端专用目录运行。
`continue-on-147.sh` 固定指向本次已有实验；新实验应使用独立目录和容器名。
`ssh-jump-145.conf` 是本次使用的 2222 跳板入口；`ssh-147.conf` 仅为早期 2147 配置留档。
两份文件只包含地址、端口和用户名，不包含密钥或密码。

输入版本：`shardpg-vacu`，提交 `bb62a57`。来源及完整提交号见 `source.json`。
已导出独立源码和 PostgreSQL 构建到 `work/`，不挂载、不修改其他项目目录。

## 隔离范围

- 容器：`yyt-vacu-container`，操作前核对专用所有权标签。
- 网络：`--network none`，节点之间只用容器内部回环网络，不向宿主机发布端口。
- `/work` 只绑定本目录的 `work/`，数据库文件在 `work/pg-cluster-data/`。
- `/tmp` 只绑定本目录的 `tmp/`，日志在 `logs/` 及 `work/pg-cluster-data/`。
- 不停止、删除或重启任何其他容器；不修改 Docker socket 权限和宿主机用户/组。
- 容器镜像和引擎元数据由 Docker 正常管理，其他项目目录不作为可写挂载。

## 运行

```bash
bash /home/zhanhao/yyt/lab.sh preflight
bash /home/zhanhao/yyt/lab.sh up
bash /home/zhanhao/yyt/lab.sh verify
bash /home/zhanhao/yyt/lab.sh test
```

默认 1 coordinator + 3 workers，限制 2 CPU、2 GiB 内存；使用现有构建镜像，若不存在
才在本实验名下构建镜像。扩展在容器中重新编译和安装，TSO 在测试写入前配置。
不覆盖已有实验数据库或同名容器；失败时保留现场用于检查。

历史 VACUUM 三套回归含固定 9 节点的夹具，如需运行，应在首次创建环境时选择：

```bash
N_WORKERS=8 LAB_MEMORY=3g bash /home/zhanhao/yyt/lab.sh up
bash /home/zhanhao/yyt/lab.sh regress
```

先确认宿主机有足够空闲资源；脚本不会停掉其他环境给自己腾资源。
上述历史套件含对本实验节点的崩溃和重启操作。

## 当前执行结果

147 的独立容器已创建，两份 C 扩展 `pg_partdist`、`pg_raft` 已编译安装，
9 个数据库节点已启动。补上控制面登记等待后，环境检查 V1–V6 全部通过。
契约测试 4/4 通过；页面清理、停发保护和崩溃恢复套件 PASS=180、FAIL=0。
自动清理套件修正夹具后 PASS=65、FAIL=0。
复制功能的 66 项断言已全部通过：两个 follower 回放 VACUUM 后，主堆、TOAST 堆及
TOAST 索引页面比对一致，清理水位均为 9/9，保留数据及大字段内容正确。
该轮末尾的健康检查因日志通配符处理返回 2；修正后只读补查 PASS=10、FAIL=0，
九节点均可连，本轮无新崩溃。复制日志为 `logs/remote-147-resume-20261029T090342Z.log`，
健康补查日志为 `logs/remote-147-resume-20261029T091208Z.log`。
完整结果及验证边界见 [TEST_REPORT_147.md](TEST_REPORT_147.md)。

本目录的实验脚本已修正并在远端复验以下事项：自动清理套件恢复原有 TSO 设置；
手工 sweep 前等待真实安全前缀；按 postmaster 当前日志取证；把已禁止的行锁作为
负向用例；套件失败返回非零并停止后续套件；复制夹具失败立即停止，避免空 shard/端口。
不靠修改提交时间戳或强行推进安全水位使测试通过。

2026-10-29 08:33:12 UTC 的崩溃语句为
`CREATE TABLE p5repl_ (LIKE p5repl INCLUDING ALL)`，当时源表不存在。
完整日志尾部已取回 `logs/remote-147-diagnostics-20261029T083905Z.tar.gz`。
原日志未开启栈回溯，尚不能定位到 C 函数；不能把这次崩溃当成普通 SQL 错误忽略。

只读取状态并取回日志：

```bash
bash /home/zhanhao/yyt/continue-on-147.sh --diagnose
```

定点复现已记录的错误建表语句并收集栈：

```bash
bash /home/zhanhao/yyt/continue-on-147.sh --trace-ddl
```

定点复现仅临时重启专用实验容器的 worker2，开启其已有的诊断 GUC；结束时恢复
原 GUC 及日志目标。不会操作其他容器，不运行整套测试。若本次未复现，仍记录原崩溃未解决。

本次定点复现（08:43 UTC）返回了正常的“源表不存在”错误，未复现 SIGSEGV。
原崩溃仍列为未解决事项。仅运行自动清理及复制套件，并在 worker2
开启临时栈回溯取证（测试结束后恢复）：

```bash
bash /home/zhanhao/yyt/continue-on-147.sh --remaining
```

此模式复用原数据目录，不重编译，不重复执行已通过的 4 个契约用例及 180 项页面套件。
若某套失败，停止后续套件并保存诊断日志。

最新复制夹具修正：建组后等待控制面登记；在末次回滚后提交同分片的一组插入/删除，
按 `PartWALAbort` 的既有规则带出本地 DATA/ABORT 尾巴；检查 Raft 提交上界覆盖
回放目标后才等待副本。不降低目标位置，不放宽安全断言。本次重跑中提交位置 132
及 VACUUM 后位置 147 均被两个 follower 追平。

仅重跑复制套件：

```bash
bash /home/zhanhao/yyt/continue-on-147.sh --replay
```

节点详细诊断现保存为本地 `logs/remote-147-state-*.log`，完整 PostgreSQL 日志保存为
`logs/remote-147-diagnostics-*.tar.gz`，终端只保留测试进展及简短日志尾部，无需手工复制全部输出。

只检查九节点连接及从本轮复制开始以来的崩溃日志：

```bash
bash /home/zhanhao/yyt/continue-on-147.sh --health
```

实验脚本现增加写入前的控制面登记等待（最长 90 秒）及失败后的只读诊断。
该改动只在本目录；等待条件检查全部节点的 `partition_map`，不手动填充主节点元数据。
若仍未登记，会停止测试并输出各节点状态和日志，保留现场。

在源机器的 VS Code 终端（`zhanhao@server-1`）继续：

```bash
bash /home/zhanhao/yyt/continue-on-147.sh
```

脚本通过 2222 → 145 → 147 更新少量实验脚本，复用现有实验容器和数据库，
按环境检查、VACUUM 契约测试、历史回归的顺序继续。实验目录固定为
`/home/yyt/vacu-bb62a57-20261029T074050Z`。本地的
`logs/remote-147-resume-*.log` 保存终端输出，远端 `logs/lab.log` 保存实验日志。
