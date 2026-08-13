# P4 前置核查 + §9.1 三实验（T4.0）

日期：2026-08-13。对应 `TX_TSO_MVCC_DEV_PLAN.md` §3.3 T4.0 的七个问题。
实验在 pg-citus-tx2 容器（Citus 13.1-1，9 节点）实测；pg-raft-src 盘点为
**只读**（冻结未解除）。

---

## 结论一：§9.1 三实验 —— **全部通过，MX 路线放行**

**实验 1（元数据同步现状）**：`pg_dist_node` 9/9 节点 `hasmetadata=t、
metadatasynced=t`——Citus 13.1 的 `citus_add_node`（reproduce-env.sh:186 现用
方式）默认即同步元数据，**建集群脚本零改动**，MX 前提天然成立。

**实验 2（worker 驱动 2PC 一致性，兼 V5）**：32 分片分布表全表 UPDATE，
参与节点 :5434 的 WAL 取证——

| 驱动方 | :5434 记录 | 结果读一致性 |
|---|---|---|
| master(:5432) | 1×PREPARE + 1×COMMIT_PREPARED | ✓ |
| worker1(:5433) | 3×PREPARE + 3×COMMIT_PREPARED | ✓（双端读 40\|w2 一致） |

**2PC 语义与正确性逐项一致**；唯一观测差异是**连接并行度**（worker 驱动对同一
参与节点开了 3 条连接、每连接一笔 prepared 事务；master 驱动 1 条）——语义
无差，P4 实施时对"每连接一笔 2PC"的既有假设本就成立（DTX 链路按连接捕获）。
纪律项实测：worker 上 `citus.recover_2pc_interval=-1` ✓、
`shared_preload_libraries=citus,pg_partdist,pg_raft`（Citus 居首）✓。

**实验 3（local execution）**：worker1 驱动、目标分片本地放置——
`citus.log_local_commands` 证词 "executing the command locally" 实测出现，
写入全局可见。`wal_insert_hook` 位于 XLogInsert 层，本地执行写 WAL 与普通
路径无异 ⇒ 捕获行为不变（结构性成立；DTX 参与者捕获在 T4.7 套件正式回归）。

## 结论二：V2 连接建立点 —— **每连接前置机制运行时实证，挂点确认**

worker 驱动多分片写期间，参与节点 `log_statement=all` 抓到
`assign_distributed_transaction_id` 每连接前置调用（2 条/事务连接数）——
§9.2 第 2 层"借同位置机制"的挂点**实证存在且在 worker 驱动下同样生效**。
完整枚举（COPY/repartition/中间结果路径）在 T4.1 落协议时以同法逐路取证 +
安全网兜底（漏网 = 响亮报错不 = 静默错读）。

## 结论三：V3 引用表 —— **系统现役零使用，裁定"第一期建表后只读"**

`pg_dist_partition WHERE partmethod='n'` = **0 行**（已交付系统无引用表）；
tests/ 仅 `test_shard_identity_p0.sh` 一处提及 `create_reference_table`
（P0 时代脚本，不在 634 基线 14 套件内）。裁定：第一期引用表**建表后只读**
（§9.2 第 3 层专项按此落地，写路径 ProcessUtility/安全网拦截），风险面≈0。

## 结论四：gxid 分配器 —— **§2.1 为准，独立于分片 xid；水位文件纪律复用**

两处设计文本的表述分歧裁定：§2.1（节点16b|节点内单调序号48b、批量水位持久化、
跳号无害复用绝不允许）是 globalXID 的权威定义；§9 表"低位换分片 xid"指既有
gxid 编码宏（GxidNodeId/GxidLocalXid）在**旧 txn 层账本键**上的含义迁移，
不改变新分配器的定义。落位：每节点 shmem int64 计数器 + 水位文件
`$PGDATA/pg_gxid_wm`（复用 shard_xid 的 tmp+fsync+durable_rename 纪律与
批量 4096），协调者分片 leader 所在节点发号。

## 结论五：start_ts 跨连接注入 —— **注入通道 + 本地活跃集合必须登记**

新 API `TsoInjectStartTs(ts)`（T4.1）：参与者后端 join 时注入协调者下发的
start_ts——置 cur_start_ts（不自取、不 RPC）+ **登记进本节点活跃集合**
（否则该节点心跳携带的 oldest 看不见远端读快照，GlobalSafeTs 会越过活跃
远端读 = 正确性破坏，这是本核查抓出的关键点）+ 事务结束既有回调清理。
栅栏语义沿用（远端节点自己的心跳新鲜度保护该快照）。

## 结论六：PREPARE 持久化 —— **补丁 0009 = twophase 状态文件扩列 + clog PREPARED**

- 分片 clog 写 PREPARED：挂 XACT_EVENT_PRE_PREPARE 之后的 PREPARE 路径
  （放行条件 = 已 join 全局事务，P1 全面禁令按此收口）；带 start_ts，
  parent_xid 链起用（T2.1 槽五列早已定格，零迁移）。
- 崩溃恢复：`twophase.c` 2PC 状态文件扩 (分片oid, 分片xid) 列表（§8-①，
  仿 0007 的可选块思路）——恢复 prepared 事务时重建分片绑定与 PREPARED
  槽；`RecoverPreparedTransactions` 挂扩展回调。COMMIT/ABORT PREPARED 走
  既有 0007 记录体（收集钩子读恢复后的绑定）。预计补丁 **0009**，细节
  T4.3 定稿。

## 结论七：pg_raft 解冻范围清单（★ 用户决策项，批准前 T4.4 不动工）

**触碰面（只读盘点）**：
- `raft_consensus.c` 单文件：`dtx_master_pre_record_commit`（:7019–:7258，
  约 240 行）——搬迁本体：Citus 协调节点闸门（`pg_raft_coordinator_node_id`
  判据，:7043 附近）改为"本节点是本事务协调者分片 leader"判据；决议写入的
  raft 组从 master 侧配置组改为**协调者分片组**；SQL 面（读本地
  pg_dist_transaction 取参与者集合）在 MX 下天然随节点成立（实验 2 佐证）。
  安装点 :7339（`pre_record_commit_hook = ...`）不动。
- 设计 §9 提及的 `check_fastpath_divergence`：**两仓均无同名符号**（tx4 机制
  的描述性名称）；tx4 涉改的断言在 pg-partdist/测试侧，不扩大解冻面。
- `DtxRecordPayload`/`dtx_decision` ts 换源：载体在 **pg-partdist**
  （dtx_record.h/dtx_participant.c）与 SQL schema，不在冻结面内。
- **流控 #39 存档**（`~/pg_raft_flowcontrol_39.patch.bak`，实存）：与本次
  搬迁**正交**（流控 vs DTX 决议）。选项：(a) 本次解冻只做决议搬迁，#39
  另行立项（默认推荐——改动面最小、验收面清晰）；(b) 搭车重放 #39（省一次
  解冻，但 P4 验收要背流控回归）。**待用户裁定。**

**解冻请求**：批准范围 = raft_consensus.c 的 dtx_master_pre_record_commit
函数体及其闸门/组选择逻辑（估算 ≤300 行改动面）；#39 按上述选项裁定。

---

## 风险登记（并入 DEV PLAN §5）

- **R-P4-1 连接并行度差异**：worker 驱动对同一参与节点开多条连接（实验 2
  实测 3 条），每连接一笔 prepared 事务——参与者集合/决议广播必须按
  "(事务, 连接/分片)"粒度对齐既有 DTX 捕获，不能假设一节点一 prepared。
- **R-P4-2 注入 ts 漏登记**：远端后端注入的 start_ts 若不进本地活跃集合，
  GlobalSafeTs 将越过活跃远端读（结论五）；T4.1 验收必须含"远端持快照期间
  safe 被钉住"用例。
