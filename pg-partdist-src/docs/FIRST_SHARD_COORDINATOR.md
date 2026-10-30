# 首个分片协调事务（源码改动，尚未完成集群验收）

本协议取代数据事务原来的 master 驱动 prepare 路径。master 在路由前取得
`gxid` 和 `start_ts`，将同一上下文传给所有分支。首个成功登记到达的分片成为
协调分片；并发到达以 TSO/router 上 `dtx_route` 唯一键的登记顺序为准。
这张表只保存路由，不保存参与者、投票或决议。

## 正常流程

1. `TsoEnsureRouterSnapshot` 在 Citus 规划之前分配事务上下文；远程 BEGIN
   钩子同时覆盖缓存计划并传播上下文。一个事务仅使用一份 `gxid/start_ts`。
2. 每个 Worker 在开始执行分片计划时登记自身分支及分片集合。登记发送给
   首个分片的 leader，保存在 `dtx_coord_tx/dtx_coord_branch`。
3. 客户端 COMMIT 到达 router 后，router 完成任务连接的登记检查，发送
   `CALL partdist.dtx_handoff()` 停放原连接，并向协调分片转交一次
   `dtx_coordinate(gxid)` 请求。router 不发送 `PREPARE TRANSACTION`，
   不收集 prepare 投票，也不发送 `COMMIT PREPARED`。
4. 协调分片封闭登记集合，发布 PREPARE 命令。原 Worker backend 从协调者
   读取命令，然后对自己持有的事务调用 `PrepareTransactionBlock`。
   PostgreSQL 的开放事务属于原 backend，不能由新建 RPC 连接替它 prepare。
5. Worker 沿用 DATA 多数派 → PREPARE 标记多数派 → 自治登记 → 本地
   PREPARE WAL 的顺序。`XACT_EVENT_PREPARE` 才直接向协调者发送成功投票。
6. 协调者收齐成功票，先把不带时间戳的 `DTX_COMMIT_DECISION(kind=6)`
   复制到协调分片 Raft 多数派；这是不可撤销的 COMMIT decision。
7. decision 成功后才向 TSO 申请 `commit_ts`，再把该时间戳随 `kind=2` 记录
   复制到协调组，保证恢复和重试使用同一权威时间戳。尚无时间戳时不广播 commit。
8. 时间戳持久化后即可通过 router 返回客户端。独立的
   `pg_partdist asynchronous commit` worker 读取持久化决议并分发
   `dtx_finish_decision(gxid,coord,verdict,commit_ts)`；客户端路径不等分片回复。

统计 prepare 的单位是 PG 连接上的事务分支。一条分支可访问同一 Worker 的多个
分片；登记保留所有分片，数据、prepare 和终局标记仍分别进入各自的 Raft 组。
只读分片参与 prepare/完成登记，但不会为未写入的数据创建分片 xid/clog 槽。

## 持久性与恢复

- `dtx_route` 一次选定协调分片，不按分片 ID 排序选主，也不采用原来的取模选择。
- 协调者的 OPEN/PREPARE 登记在本机独立事务里持久化。决议前丢失协调者时，
  不尝试在缺少投票集合的新 leader 上继续提交；现有恢复机制在协调组形成
  权威 ABORT 决议后收尾。
- PREPARE 的 gid 为 `shardpg_fc_<gxid>_<coord_gsid>_<connection_id>`。
  自治参与登记和原生 2PC 状态中都保存协调信息；gid 也可用于恢复寻址。
- decision 后 TSO 暂不可用或进程退出时，`dtx_commit_decided` 保留不可撤销的
  COMMIT。后台补取并持久化时间戳；`dtx_status` 此时返回等待，不能推定 ABORT。
- 新 leader 先确认本任期多数派并追平日志，再判断是否存在决议。独立的
  `dtx_decision_replay` 游标与决议索引在同一 SQL 事务提交，补齐“日志已提交但
  SQL 索引事务回滚”的窗口；仅重放已提交的 plsn 前缀。FORGET 同时清理两种索引。
- 带时间戳的 COMMIT 决议本身是可恢复的异步发送队列，随协调组 Raft 日志复制。协调组换主
  后，新 leader 可以继续分发；不依赖 router 内存、客户端连接或原协调进程。
- 每个参与者按自己的 `(local_shard_oid, shard_xid)` 写终局标记并取得多数派确认，
  再发布本地分片 clog 终局。复制失败保留未决登记，不能提前发送完成回执。
- 原生 `COMMIT PREPARED` 收尾时使用权威决议的 `commit_ts` 关闭所有本地分支流，
  包括只读分片。此处不能再新取时间戳。
- 同一节点的同一全局事务可能有多个 prepared 分支；所有分支都结束后才允许
  回执，防止第一条分支结束就触发决议 FORGET。
- prepare 阶段分片 clog 保持 `IN_PROGRESS`（保留原格式的数值 0），终局才写
  COMMITTED/ABORTED（沿用 2/3，并非 PostgreSQL 原生位编码）。独立目录
  `pg_shard_twophase/<local_shard_oid>/<sxid>` 标识 prepared，带全局事务号及校验和；
  原生 2PC、分片 PREPARE marker 都可恢复该登记。它不属于 clog 状态枚举。
- 读路径、无主事务认领、切主闭合、VACUUM 阻挡、未决清扫均改查独立 2PC 登记。
  基线通过单独 `SHARD_PREPARED` 控制记录携带它；终局、分片删除及 xid 循环复用
  清理登记。旧槽值 1 仅作为读兼容识别，新写不再产生 PREPARED clog 槽。
- 分片 clog 仍为 `pg_shard_clog/<local_shard_oid>/...`。协调登记、路由、全局
  决议保存在独立控制表中，不写入其他分片的 clog。
- 客户端在决议成功但回复丢失时得到的结果可能未知；router 不得自行发送回滚
  去覆盖已经持久化的 COMMIT。

## 构建和安装

这是源码及补丁交付，仓库的 `pg-install` 二进制尚未重建，也没有改动运行集群。

1. 在 Citus 13.1.0 源码上，依次应用已有的 `citus-0002`、`citus-0003` 和新增的
   `citus-0004-first-shard-coordinator.patch`。第四个补丁新增分支 BEGIN 接口及
   整体提交委托接口，并让已交接分支不再进入 router 的原生回滚路径。
2. 按本项目构建纪律清理重编 Citus、pg_partdist、pg_raft；更新目标环境的二进制。
3. 排空事务后，在每个节点安装 `pg-raft-src/sql/first_shard_decision.sql` 和
   `pg-partdist-src/sql/first_shard_coordinator.sql`。全新创建扩展时主安装脚本
   已包含同一定义。升级期间停用事务流量及后台恢复，不能混跑两版协议；新增
   分片基线控制记录同样要求所有副本升级。
4. 重启以加载新库及异步提交 worker；每个节点需要额外一个 background-worker
   名额。当前项目的后台控制任务连接 `postgres` 数据库，此实现沿用该约定。

`shardpg.first_shard_coordinator` 默认开启。配置了新协议却缺少 Citus 0004
接口时拒绝启动数据事务，不会静默退回 master prepare。
管理员可在会话中关闭该开关，用旧路径完成建表、建组等引导操作；没有纳管分片的
元数据事务不满足“首个分片”的前提。新路径遇到没有任何纳管分片的分支会报错，
不将它悄悄排除。引导完成后重新开启。混合本地写和分片写、动态改协议的事务，
不应作为本协议的支持场景。

## 验证范围

`python3 pg-partdist-src/tests/local/test_first_shard_coordinator.py` 使用临时 PG16
单用户实例运行实际 PL/pgSQL，覆盖首到登记、登记集合封闭、投票完整性、错误票、
decision→时间戳顺序、TSO 失败后的恢复、重试时间戳不变、异步发送分离、
决议索引回滚重放与 FORGET，以及 prepare 恢复信息。测试显式替换了
C/Raft/TSO/RPC 边界，不代表 C 回调、多数派或切主验证。

仍需在具备构建环境的隔离集群运行：跨分片写/读、多连接同分片、COPY、提交后立即
断开客户端、提交回复丢失、prepare 前后断电、协调组换主、某参与组失多数派、
异步分发失败重试及各分片 clog/副本一致性。当前环境缺少编译器和 make，且不能
访问 Docker；因此不能把本次源码改动视为已完成这些验收。

`tests/local/test_shard_prepare_clog.sql` 是重建后在隔离测试库执行的 C 存储层
验收脚本，检查 prepare 时 clog 为 IN_PROGRESS、认领不误杀、终局清理 2PC，
以及分片状态相互隔离。副本基线与重启恢复仍需集群验收。当前环境不能编译新库，
该脚本尚未执行。
