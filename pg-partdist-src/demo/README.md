# ShardPG 演示：同一节点上的混合主从

`mixed_primary_demo.py` 在 `pg-test-container`（1 协调者 + 3 worker）上演示：一张 3 分片的表，每个分片一个
Raft 组、成员 = 全部 3 台 worker，三个组的主分别落在三台 worker 上 ——

```
            S1          S2          S3
  w1 :5433  ★主         从          从
  w2 :5434  从          ★主         从
  w3 :5435  从          从          ★主         ← 每台 worker = 1 个分片的主 + 2 个分片的从
```

在这个拓扑上把原版演示（`~/shardpg-demo/DEMO_WINDOWS_POWERSHELL.md`）的功能逐项跑一遍，并补上
「同一节点混合主从」特有的几项（同节点读闸门、角色在线变化、身兼多个主的节点宕机）。

## 怎么跑

从 Windows PowerShell（或任何终端）SSH 到服务器，然后：

```bash
cd ~/shardpg-test-work/pg-partdist-src/demo

python3 mixed_primary_demo.py --auto      # 全自动：执行每步建议 SQL 并逐项断言，结束时打 PASS/FAIL 并清理现场
python3 mixed_primary_demo.py             # 交互式：每步讲解 + 建议 SQL；可逐条执行，也可以自己写 SQL
python3 mixed_primary_demo.py --cleanup   # 只清理（删表、拆组、复位 TSO）
```

- `--auto --keep`：跑完保留现场（之后可以 `python3 mixed_primary_demo.py --from 12` 进交互式接着看）。
- `--from N`：交互式从第 N 步开始（前面步骤建立的状态须已在）。
- 环境变量 `CONTAINER` 可换容器（默认 `pg-test-container`）。**与其它测试 / 演示互斥**：同一集群上不要同时跑。
- 只依赖宿主机的 `python3`（标准库）和 `docker`；逐字节比对用的是 `../tests/pagecmp.py`（自动拷进容器）。

## 交互式怎么用

每一步先打印讲解，本步有前置动作（重启协调者开 TSO、建组、杀节点）时会先问你「回车执行」。然后列出**建议 SQL**：

```
建议的 SQL（回车逐条执行、\all 全部执行；也可以直接写你自己的 SQL，@会话 前缀换会话，\help 看全部命令）
 ▶ 1 [A] BEGIN; …   -- 幕1 A 开事务
    2 [B] BEGIN; …   -- 幕1 B 开事务
[第8步] coord>
```

- **回车** = 执行下一条建议；执行后会告诉你结果「✓ 符合预期」还是「✗ 与预期不符」。
- **直接输入 SQL**（以 `;` 结尾，可多行）= 在当前会话执行。`@w2 SELECT …;` = 在 w2 上执行；`\use A` 切当前会话。
- 会话是**常驻的 psql 进程**：`BEGIN` 之后事务一直开着，跨多次输入。语句被锁住时界面不会卡死，
  它返回时结果自动打出来（或 `@B \wait` 收结果）。
- 会话：`coord`（协调者，写 `mx_stock` 就行，Citus 自动路由）、`w1 w2 w3`（直连 worker，分片表
  `mx_stock_<分片号>` 可见）、`A B`（两个独立的协调者连接，演示并发事务）。
- `\next` 下一步（先跑本步验证）、`\back` 上一步、`\step N`、`\list`、`\show`、`\check`。
- 观测：`\roles`（混合主从一览）、`\cmp`（副本追平后逐字节比对）、`\where <sku>`、`\cheat`（观测函数速查）。
- 动作：`\gbegin`（开全局事务，跨分片写用）、`\bg start|stop`（后台读探针）、`\node stop|start w2`（停 / 起节点）。
- 其它 psql 元命令（`\d mx_stock`、`\x` …）原样交给当前会话。`\q` 退出（会问要不要清理）。

## 步骤一览

| 步 | 内容 | 自己写 SQL 可以试试 |
|---|---|---|
| 0 | 前置：节点编号（端口 / groupid / pg_raft 节点号）、控制面 0 号组 | `@w2 SELECT * FROM partdist.pg_raft_group_status();` |
| 1 | TSO：删 boot 标记、重启协调者、开 tso_master；worker 经 libpq 取号 | 在不同节点连取几次 `partdist.partdist_tso_client_start_ts()`，看单调递增 |
| 2 | 建 3 分片表，看分片落在哪、sku 落在哪 | `SELECT get_shard_id_for_distribution_column('mx_stock', 4242);` |
| 3 | 每分片一个 Raft 组 + `provision_shard_replica` 供副本 ⇒ 每台 1 主 2 从 | `\roles`；`@w3 SELECT * FROM partdist.replay_status();` |
| 4 | `set_table_shard_mvcc` 打标，接入事务系统 | `@w1 SELECT partdist.shard_mvcc_status(partdist.local_partition_for_shard(<分片号>));` |
| 5 | 写 24 行：跨分片写不加入全局事务 ⇒ PREPARE 被拒；单分片写走 1PC；`\gbegin` 后跨分片写成功。worker 原生 xid 几十万，分片元组 xmin 从 3 起 —— 每分片一个 xid 宇宙 | 自己插几行，再到该分片的主上看 `xmin` |
| 6 | Raft 复制 + 惰性回放（收到≠回放）+ 触发追平 + 分片 clog 同步 + 逐字节一致 | 写一行后看从的「收到位点」涨、「已回放到」不涨，再 `replay_catchup` |
| 7 | 同一台 w1：自己当主的分片可读，别人的副本壳表被读闸门拒绝（判据 = 副本上有没有回放学到的分片 xid 水位） | 换 w2 / w3 各读一遍三个分片表；看 `route_status` 的 `xid_watermark` |
| 8 | 会话 A / B：自见性、快照隔离（A 提交后 B 仍看不见）、增删改、页面 xmin/xmax、判决账本、写写冲突 | 自己编排 A / B 的交替顺序；试试 B 先 `SELECT` 再等 A 提交 |
| 9 | 受控切主：S1 的主挪到已经是 S2 主的节点；后台读探针 0 失败；发号不断档；旧主自动归队 | 切回去：`@w1 SELECT partdist.pg_raft_group_campaign(<S1>);` |
| 10 | 宕机：杀掉身兼两个主的节点；两组各自选主；它只当从的分片 0 失败；拉起后自动归队、逐字节一致 | 宕机期间自己读写各分片；`\roles` 看角色 |
| 11 | 跨分片 2PC（`\gbegin`）：守恒、ROLLBACK、中途失败原子；三本分片账同一个 commit_ts | 不用 `\gbegin` 直接 `BEGIN` 跨分片写，看会怎样 |
| 12 | 终检：角色一览、全部副本逐字节一致、无 PREPARED 残留 | |

## 演示环境的几点说明

- **别在演示中途重启协调者**：TSO 计数器在内存里，boot 防呆会拒绝发号（第 1 步那次重启就是按规矩删了标记才做的）。
- 第 3 步先把全体 `pg_raft.election_timeout_ms` 冻结在 30 s，再在 placement 节点上 `pg_raft_group_campaign`
  点名当选（新组日志全空，必然选上），供副本期间保持冻结，本步结束复位。这是 2 vCPU 演示机上防组主漂移的夹具手法，
  不是产品要求。**别对日志落后的节点反复 campaign**：它选不上，却会一次次抬高任期把现任主逼下台（组就一直没主）。
- 第 9 步的「0 次读失败」依赖 P7-N31 的修复（新主登记生效的一瞬读闸门会等本地登记，而不是拒读）；
  「旧主自动归队」依赖 P7-N25 的自动重供。
- 第 10 步当主的分片会有一段**选举 + 登记**的不可用窗口（约十几秒），这是预期的；只当从的分片不应有任何失败。
- `\cmp` 在副本没有 armed 回放槽位时会先从当前主重供基线再比（这是运维上的正规流程），并在输出里注明。
