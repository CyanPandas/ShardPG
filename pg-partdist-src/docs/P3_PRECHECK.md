# P3 前置核查（T3.0）

日期：2026-08-13。对应 `TX_TSO_MVCC_DEV_PLAN.md` §3.2 T3.0 的六个问题。
证据以工作区当前树与 pg-citus-tx2 容器为准。

---

## 结论一：ts 载体 —— **int64 逻辑单调计数器（从 1 起），0 = 无 ts**

**既有字段盘点**（全部 uint64，装得下任何一种载体）：

| 字段 | 现值来源 | P3 处置 |
|---|---|---|
| `ShardClogSlot.start_ts/commit_ts`（分片 clog，32B 槽） | 恒 0（P2 占位） | **P3 起用**，值=TSO |
| `TxnMarkerPayload.start_ts/commit_ts`（partwal MARKER） | `GetCurrentTimestamp()` 占位（partwal_sync.c:720 注释"TSO 就位前的时间源"） | P3 不动，P4 换源 |
| `DtxRecordPayload.commit_ts` / `dtx_decision.commit_ts` | 本地时钟占位（dtx_participant.c:480） | P3 不动，P4 换源 |
| `EnhancedClogSlot.start_ts/commit_ts`（每来源节点域） | MARKER 载荷 | 基线机器，P3 不碰 |

**定案**：逻辑计数器——TSO 唯一职责是全序，物理时刻无语义；混合时戳只在
需要与墙钟粗对齐时才值得（本方案不需要）。**两套 ts 宇宙短期并存**：TSO
逻辑值只进分片 clog/分片可见性；MARKER/DTX 的本地时钟值只进旧 txn 层
（基线路径），二者在 P3 无任何比较点（audit：分片可见性只读
ShardClogSlot；旧 txn 层只读 EnhancedClogSlot）。P4 换源时统一。
**向后兼容红利**：P2 时代已提交行 commit_ts=0，新判据 `0 < 任何 start_ts`
⇒ 历史提交行对一切新快照可见——语义恰好正确，无需迁移。

---

## 结论二：TSO 通路 —— **coordinator SQL 函数 + worker 后端 libpq 缓存连接**

- 服务端：coordinator（node1:5432）pg_partdist shmem int64 计数器 +
  `partdist_tso_start_ts(node, oldest)` / `partdist_tso_commit_ts()` SQL 函数
  ——复用既有连接认证/监听设施，零新协议。
- 客户端：**pg_partdist 直接链接 libpq**（Makefile `SHLIB_LINK += $(libpq)`，
  postgres_fdw 同款做法）。实证现状：pg_partdist 自身零 libpq 引用，
  dtx_participant.h:76 注明"libpq 在本项目里由 pg_raft 持有"——而
  **pg-raft-src 冻结中不许改**，借道不可行，自链是唯一干净路径。
- 连接管理：每后端缓存一条到 coordinator 的连接（会话生存期），断线重连
  一次，再失败即 ERROR——**绝不本地时钟顶替**（§2.2 纪律 2）。
- coordinator 地址来源：node_map（metadata_cache 已缓存 hostname/port，
  node 1 即 master）；本节点号 = `partdist_node_id` GUC（gxid 来源节点号，
  自动取 Citus group id，global_mvcc.h:38）。
- 延迟成本：同宿主 docker 回环一次 SQL 往返 ~0.2–0.5ms；写事务每笔
  +2 RPC（start_ts + commit_ts）≈ +1ms——P3 测试规模可接受，批量取号
  留后续优化（§2.4 HA 段已留档）。

---

## 结论三：commit_ts 进提交记录体 —— **原位改造 0007 块（重生成补丁），且必须预取暂存**

- **关键约束（实测代码）**：0007 收集钩子在 `XactLogCommitRecord` 临界区内
  被调（`Assert(CritSectionCount > 0)`），**不可能在那里做 TSO RPC**。
  定案：commit_ts 在 `XACT_EVENT_PRE_COMMIT` 回调（临界区外、
  RecordTransactionCommit 之前）取号并暂存后端状态——这正是"合法窗口内
  尽晚取"（§2.2 纪律 3）在单分片语境的落点；收集钩子只拷暂存值。
  TSO 不可达在 PRE_COMMIT 即 ERROR（事务干净中止）；进了临界区暂存必已
  就绪，收集钩子发现缺失即 PANIC（见风险 R-P3-1）。
- **块格式**：`xl_xact_shard_xids` 头从 `{int nxids}` 扩为
  `{int nxids; uint32 commit_ts_lo; uint32 commit_ts_hi}`——用两个 uint32
  承载 int64，规避"块起点仅 4 字节对齐、解析是直接指针转型"的非对齐
  uint64 读取问题。abort 记录不带 ts（ABORTED 不参与 ts 判定，写 0）。
- **补丁载体**：**原位改造 0007**（更新 kernel_0007.py 替换表重生成，
  redo 钩子签名同步加 commit_ts 参数）。不开 0009：同一个块被两个补丁
  先后改 = 叠罗汉混乱；0007 是本项目私有格式，无既有装机兼容义务。

---

## 结论四：start_ts 懒取 —— **与时机定理相容，采纳**

设计 §2.2 写"事务开始时取"；实现取**首次触达分片表时**（读或写皆同一
入口：可见性/发号路径的后端事务态检查）。相容性论证：本事务全部分片读
共用这一个 start_ts（一事务一取，映射随 XactCallback 清理），时机定理只
依赖"S 由 TSO 发出且晚于 C ⇒ prepared/决议痕迹处处可见"——S 的取得时点
从 BEGIN 推迟到首触分片表，单调性论证逐字成立；首触之前的原生表访问本就
不归分片快照管辖。收益：纯原生事务零 TSO 成本（596 基线零扰动），只读
分片事务恰一次 RPC（§4.3 零协调原文）。

---

## 结论五：GlobalSafeTs v1 最小面 —— **登记+双通道+租约全量做，栅栏做"心跳新鲜度检查"**

- 登记表：coordinator shmem `{node → oldest_active_ts, lease_deadline}`；
  搭车通道 = start_ts 请求随行（T3.1 原子发号即登记）；心跳通道 = 每节点
  一个**新的轻量 bgworker**（实证：demux worker 是 one-shot 崩溃恢复型
  `BGW_NEVER_RESTART`、replay worker 按分区，均不可搭车——新注册一个
  周期 bgworker，逻辑 = 报"本节点最老活跃 ts 或'无'"+ 续租）。
- 租约：到期 master 清该节点登记为"无"。
- **栅栏最小面**：worker 侧 shmem 记"最近一次心跳成功时刻"；分片 ts 取用
  点检查 `now < last_beat + lease − ε`，超期即 `snapshot too old` ERROR
  ——先于 master 剔除生效，依赖仅时钟漂移速率有界（§6.2 原文）。每次
  检查一次时钟读+比较，可接受。P5 vacuum 接 `partdist_global_safe_ts()`
  读出函数时地基已全。

---

## 结论六：Proxy 粘性路由 + 会话映射 —— **推迟并入 P4**

P3 无多分片事务、无 MX 转发：粘性路由的消费方（"同一会话的事务要落到
同一协调者连接"）在 P4 的 `partdist_join_global_txn` 连接加入协议出现之前
不存在。P3 的一切分片访问是 leader 直连（P1 起的运行纪律）。拟并入 P4
与连接协议一体设计（避免先做一版无消费方的接口再返工）。§3 P3 里程碑行
的该子项随本裁定移账 P4，DEV PLAN §3 P4 行不变（其"决议搬迁 + MX"本就
涵盖）。

---

## 风险登记（并入 DEV PLAN §5）

- **R-P3-1 无 ts 判决落账**：分片写事务的 commit 记录若带 ts=0 落账，该行
  将对一切快照可见（0 < 任何 start_ts）= SI 静默破坏。防线两道：
  PRE_COMMIT 取号失败即 ERROR（事务中止，临界区外）；收集钩子（临界区内）
  发现暂存缺失即 PANIC——宁可崩溃恢复也不写 0 值判决。测试须含"停 TSO 后
  提交分片写事务"用例。
- **R-P3-2 双 ts 宇宙串线**：TSO 逻辑值与 MARKER/DTX 本地时钟值在 P3 并存，
  绝不可出现在同一比较里（当前审计无比较点）；P4 换源前重新审计一遍。
