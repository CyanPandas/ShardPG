# 事务管理设计（TX-TSO-MVCC v1）

> **定位**：本文是 shardpg 事务管理（全局快照 + 分布式提交 + 分片级 MVCC/GC）的
> **方案定稿记录**，与 `pg-partdist-src/docs/DTX_2PC_DESIGN.md`（DTX-2PC，已交付的
> 决议通道）、`pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md`（FRD，物理回放）并列。
> 本文在 DTX-2PC"决议进数据组"的骨架上加入 **TSO 全局时间戳序 + 分片级 xid/clog
> 可见性**，并把决议驱动从 master 搬迁到协调者分片。
>
> **代码基线**：`shardpg-TX2` @ `7ff2656`（2026-08-11 自 shardpg-TX 尖端分出）。
> 文中引用的现有代码事实均在该基线上复核过。
>
> **状态**：方案定稿（2026-08-12 评审对话逐条敲定）。**未实施，未动任何代码。**
> 带【待定】标注的条目集中在 §11。

---

## 0 状态速览与决策日志

| # | 决策点 | 定稿内容 |
|---|--------|----------|
| 1 | 角色划分 | 薄 master / 协调者=首写分片 / 参与分片（§1） |
| 2 | 协调者选定 | 第一条写 SQL 命中的分片；一条写语句命中多分片时取分片 id 最小者 |
| 3 | globalXID | 节点级分配器：节点号 16b + 单调序号 48b；批量水位持久化；磁盘丢失换 node_id 或嵌 incarnation（§2.1） |
| 4 | TSO 两个时机 | start_ts 事务开始时取（发号即登记）；commit_ts 在全部 PREPARE 持久确认之后、决议持久化之前取；不预取不缓存；不可达 fail-closed（§2.2） |
| 5 | 投票收集 | PREPARE 的同步成功应答即持久 YES 票，无需轮询（§3.2） |
| 6 | 提交点 | 决议（verdict+commit_ts 一条原子记录）在协调者 raft 组多数派落盘的瞬间；此后异步广播（§3.3） |
| 7 | 可见性 | COMMITTED 且 commit_ts < 快照 start_ts；PREPARED 走三态处置；读者永不阻塞、永不安装决议（§4） |
| 8 | 隔离级别 | Snapshot Isolation；写写冲突 first-committer-wins（§4.4） |
| 9 | 分片级 xid | 每分片独立 32 位分配器；xmin/xmax 直接存分片 xid；页格式不变；0/1/2 保留、从 3 起；每分片一本 clog，xid 稠密连续即行号（§5） |
| 10 | Vacuum/GC | 三变量 + GlobalSafeTs（双通道上报+租约栅栏）+ 前缀规则（ABORTED 放行）+ 页面三类动作与 xmax 消毒 + 两态恢复 + 无主 RUNNING 认领（§6） |
| 11 | 回卷 | `xid_age = (next_xid − clog_truncate_before) mod 2^32`；两阶段分片级护栏（§7） |
| 12 | 复制/恢复落地 | vacuum 仅 leader 执行；两个水位走 CTRL 记录借流序；"页在前、截断在后"由流序自动保证（§6.7） |
| 13 | 决议逻辑位置 | 现有 `dtx_master_pre_record_commit` 整块从 master 搬迁到协调者分片 leader（§9） |
| 14 | 语句转发载体 | **Citus MX**（元数据同步至 worker，由协调者分片 leader 所在 worker 驱动分布式事务）；自建 dispatcher 不进第一期，仅留作后期单分片点写快路径选项（§9.1，2026-08-12 敲定） |
| 15 | hint bits | 分片表**第一期禁用**（可见性分支不读不写四个提示位）；配套禁分片表异步提交；若剖析显示中间带查询热，演进为"vacuum 代设"而非读者设位（§4.5，2026-08-12 敲定） |
| 16 | Citus 路径审计 | 执行方案**四层**敲定：fail-closed 安全网（无 start_ts 读/无 gxid 写分片表一律 ERROR，白名单不黑名单）+ 连接加入协议 + 分类处置（rebalancer/move/undistribute 等禁用）+ 负向测试门禁；COPY 协调者=首行命中分片（§9.2，2026-08-12 敲定） |
| 17 | TSO HA 范围裁定 | **v1 不做 TSO HA**：假设 master 不宕机、TSO 不重启；TSO=内存单调计数器；运行纪律=master/TSO 重启即整簇重建 + boot 标记防呆（重启拒发号）；完整 HA（绑 group0 leader 方向，论证留档）推迟后续版本立项（§2.4，2026-08-12 裁定） |

**待定项**（不阻塞定稿，见 §11）：§9.2 两个核实点（Citus 连接建立点枚举、
引用表现状）；CIC/CLUSTER 处置。

---

## 1 角色三分

### 1.1 master 节点（薄）

类比 Citus coordinator 但职责收窄为三件，**不参与任何事务的协调、决议、持久化**：

1. **路由**：事务入口 Proxy。含粘性路由——同一事务的后续语句必须送到同一个协调者
   leader，master 需维护"会话 → 协调者"映射表（新增会话状态，见 §9）。
2. **TSO 发号**：start_ts / commit_ts 的唯一来源，单调递增；发号与最老快照登记在
   同一 RPC 内原子完成（§2.3）。
3. **GlobalSafeTs**：维护各协调者节点上报的最老快照时间，计算全局安全水位（§6.2）。

### 1.2 协调者（是分片，不是节点）

**选定规则（定稿）**：事务第一条写 SQL 命中的分片即协调者；若第一条写语句一次命中
多个分片，取其中分片 id 最小者。只读语句不触发选定——先读后写的事务在第一条写 SQL
出现的瞬间才确定协调者并分配 globalXID。COPY 同规则：首行命中的分片即协调者（§9.2）。

选定规则的由来：写集在交互式事务里逐条展开、事前不可知，而 globalXID 必须在首次写
分片时就存在（分片 clog 行、流内 gxid 都依赖它），因此协调者必须在首写瞬间可确定
——"首写即协调者"是唯一自洽解（Percolator 选 primary 的同款思路）。旧规则
"写集[globalXID % n]"因先有鸡先有蛋废弃。

协调者职责：其当前 leader 所在节点承载事务会话——分配 globalXID（调用所在节点的
节点级分配器）、向 master 申请 start_ts/commit_ts、把语句分发给其余参与分片、收
PREPARE 票、做 decision。**协调者自身的一切持久化（含决议）由它所在的 raft 组
多数派完成。**

### 1.3 参与分片

执行属于自己的写；"prepared" = 本组多数派持久化数据 + DTX_PREPARE 标记；PREPARE
的同步成功应答即持久 YES 票；事后接收异步广播的决议。**任何分片的任何持久化都只
发生在其自己的 raft 组内，全局无例外。**

### 1.4 协调者崩溃语义（本模型的关键收益）

协调职责锚定在分片上：协调者节点崩溃 = 该分片 raft 组切主，**新 leader 从组内持久化
状态继承协调职责**——未决事务由新 leader 继续决议，或按 §4.2 的 in-doubt 规则收敛。
悬决窗口从"参与者超时"缩短到"选举 + 追平"。

### 1.5 单分片快路径

协调者 = 唯一参与者，无 2PC（对应现有 `dtx_master_pre_record_commit` 的
nparts<=1 快路径语义）。commit_ts 仍从 TSO 取、仍落本分片 clog——可见性规则全局
只有一套。

---

## 2 全局标识与时间

### 2.1 globalXID 分配器

- 编码：`节点号(16b) | 节点内单调序号(48b)`，int64。48 位序号在 100k tps 下约
  89 年耗尽；32 位在 10k tps 下仅 ~5 天，故取 48。
- "在协调者上分配"的准确含义：由协调者分片**当前 leader 所在节点**的分配器发号。
  协调者切主后新事务带新节点号前缀——gxid 只是身份标识，无碍唯一性。
- 崩溃安全：批量水位持久化——先持久化高水位 H，仅在 H 以下发号，越界前先推
  H += 4096 再发（PG 序列 SEQ_LOG 同款）。跳号无害，**复用绝不允许**。
- 磁盘全失：必须换 node_id，或在编码内嵌入持久化的 incarnation/epoch。
- 该节点级计数器**只为分发 globalXID 而存在**，与分片级 xid（§5）是两码事。

### 2.2 TSO 两个取号时机（含时机定理）

- **start_ts**：事务开始时取（含只读事务）。
- **commit_ts**：**在全部参与分片的 PREPARE 持久确认之后、决议持久化之前**取。

时机定理的证明（为什么这是唯一正确窗口）：TSO 单调 ⇒ 若 commit_ts C 先于某快照
S 发出，则 S 发出时各参与分片的 prepared 标记必已处处持久存在——读者按 §4.2 三态
规则问询必能得到答案，不存在"C < S 但读者看不到任何痕迹"的窗口。配套三条纪律：

1. **不预取、不缓存**任何时间戳（读写两侧同律，§2.3）；
2. TSO 不可达时 **fail-closed**（事务失败，绝不用本地时钟顶替）；
3. 在合法窗口内**尽晚取** commit_ts——最小化读者撞见 in-doubt 的等待窗口。

decision 与 commit_ts 必须是**一条原子持久化记录**；客户端 ACK 必须在决议多数派
复制之后。

现有代码衔接：`DtxRecordPayload.commit_ts` 与 `dtx_decision.commit_ts` 字段已存在
（当前按 DTX_2PC_DESIGN.md §9.8 以本地时钟占位），只需换值来源；
`TxnMarkerPayload.start_ts/commit_ts` 的"TSO 就位前取本地 TimestampTz"占位注释
同此。

### 2.3 取号即登记（铁律）

任何快照的 start_ts 必须来自 TSO 并**随发随登记**：start_ts 请求携带"本节点当前
最老活跃快照（或'无'）"，master 先把该节点登记更新为 min(所携带值, 即将发出的新
start_ts)，**然后**才返回新号。一次 RPC，原子，无竞态窗口。禁止任何本地缓存/复用
旧 ts 的读路径。此登记是 §6.2 GlobalSafeTs 的安全地基。

### 2.4 TSO 服务形态（v1 范围裁定，2026-08-12）

**裁定**：v1 假设 master 不宕机、TSO 进程不重启，**不做 TSO 自身的 HA 与崩溃安全**
——TSO 即 master 上的内存单调计数器：不做水位持久化、不做租约发号、不绑 group0。
一切正常路径优先，HA 整体留给后续版本。

配套三条：

1. **运行纪律**：master/TSO 一旦重启，单调性公理即失守（可能重发已发出的时间戳），
   整簇数据的时间戳序不再可信——**必须整簇重建**。当前各环境均为可重建的测试集群，
   代价可接受。
2. **防呆（建议实现，代价极小）**：TSO 首次服务时落一个 boot 标记；启动时检测到
   "曾服务过"⇒ 拒绝发号（fail-closed 报错）。把"重启后静默重发号 = 数据损坏"变成
   "重启后响亮停摆"，与全方案 fail-closed 哲学一致。
3. **范围边界**：本裁定只豁免 **TSO 自身**的故障。§6.2 GlobalSafeTs 的双通道/租约/
   栅栏针对的是 **worker 失联**（vacuum 正确性依赖），不在豁免之列，仍属 v1。

**后续版本的 HA 方向（论证留档，届时立项裁定）**：绑 group0 leader——水位推进写进
group0 raft 日志（多数派持久，切主后新 leader 从状态机续发）、持租约发号（新 leader
等旧租约过期，堵被分区旧 leader 的双发号）、接管后冻结 GlobalSafeTs 一个租约周期再
推进。已识别的四条负面影响及缓解：故障半径升级为全簇写停摆（TSO 固有属性，绑定使
单点数量最小化）；TSO 高频 RPC 可能延误 raft 心跳诱发选举振荡（缓解：客户端批量
取号、心跳隔离与优先、放宽 group0 选举超时）；group0 日志由事件驱动变持续涓流
（快照压缩 + 监控基线更新）；领导权漂移代价变高（稳定策略：优先驻留 master、禁无谓
transfer）。实施需解冻 pg_raft 模块。工业先例：TiDB PD 的 TSO 结构。

---

## 3 分布式提交（2PC）

### 3.1 全流程

```
client → master(Proxy) → 粘性路由 → 协调者分片 leader
  ① 首写：确定协调者，分配 globalXID，随写下发 coord_gsid 到各参与分片
  ② 执行：语句由协调者分发；各参与分片写数据（本组 raft 多数派）
  ③ PREPARE：协调者向所有参与分片发 PREPARE TRANSACTION
       —— 各分片同步应答成功 = 持久 YES 票（§3.2）
  ④ 取 commit_ts（全部票齐之后，§2.2）
  ⑤ 决议：verdict + commit_ts 一条记录，协调者 raft 组多数派落盘 = 提交点
  ⑥ ACK 客户端
  ⑦ 异步广播决议到各参与分片（各分片幂等落本分片 clog）
```

### 3.2 投票 = PREPARE 的同步成功应答（PG 内核依据）

`twophase.c` 的 `EndPrepare`：XLOG_XACT_PREPARE 写入并 `XLogFlush` **之后**
PREPARE TRANSACTION 才向客户端返回成功——同步应答本身就是持久化的 YES 票，无需
第二轮状态轮询。崩溃后由 `RecoverPreparedTransactions` 恢复 prepared 事务。
对 `pg_prepared_xacts` 的状态重查仅属恢复路径。Citus 在参与者 PREPARE 失败时会
提前报错，配合内核补丁 0004 的 `pre_record_commit_hook`（在全部 PRE_COMMIT 回调
之后、RecordTransactionCommit 之前触发），钩子触发点即"全部 PREPARE 已成功"点。

### 3.3 提交点与异步广播

决议在协调者 raft 组多数派落盘的瞬间即全局提交点（与 DTX_2PC_DESIGN.md 的既有
结论一致）。广播是异步的、允许丢失：任何参与分片迟迟未收到决议，读者或恢复流程
按 §4.2 问询协调者组 leader 收敛；学到的判决幂等回写本分片 clog。

### 3.4 崩溃矩阵

| 崩溃者 | 时机 | 收敛方式 |
|---|---|---|
| 参与分片 leader | PREPARE 前后 | 本组 raft 切主；prepared 状态由 2PC 持久化恢复；未 prepare 即失败 → 协调者决 ABORT |
| 协调者 leader | 决议持久化前 | 本组切主，新 leader 无决议记录 → 按超时对未决事务决 ABORT（安全：决议不存在 ⇒ 未提交过） |
| 协调者 leader | 决议持久化后 | 新 leader 组内有决议 → 继续广播；in-doubt 问询照常应答 |
| master | 任意 | 事务级 fail-closed（TSO 不可达）；不影响已提交事务的可见性判定（判定只依赖分片 clog） |

---

## 4 可见性（分片级 MVCC）

### 4.1 基本规则

读快照 = start_ts。元组可见 ⇔ 其 xmin 对应的本分片 clog 行
**status = COMMITTED 且 commit_ts < start_ts**（xmax 对称：删除可见则元组不可见）。
判定路径：`元组 xmin(分片 xid) → 本分片 clog[xid] → status/ts 比较`。
一个事务内跨分片的读共用同一个 start_ts——TSO 单调性保证这天然是全局一致快照。

### 4.2 PREPARED 三态处置（读者永不阻塞、永不安装决议）

读到 PREPARED 条目时：

1. 条目 start_ts > 快照 S ⇒ 直接跳过（其未来 commit_ts 必 > S）；
2. 本地 `dtx_participant.coord_gsid` 为 NULL ⇒ 直接跳过——**NULL 不变式**：
   协调者严格先把 coord_gsid 播到全部参与者、再做决议（`pg_partdist--1.0.sql`
   dtx_participant 表注释所载），故 NULL ⇒ 决议必然未做 ⇒ 未来 C > S；
3. 否则问协调者分片组 **leader**（必须 leader——follower 有 apply 滞后会答错）：
   COMMIT(C≤S)可见 / ABORT 不可见 / 尚无决议 ⇒ 跳过。学到的判决**幂等回写**本分片
   clog；"首决胜出"的 ABORT 安装只属于恢复路径，读者绝不安装。

新模型下协调者首写即定，coord_gsid 随写入/PREPARE 一起下发——分支 2 退化为极端
窗口的兜底，安全性论证原样成立。

### 4.3 只读事务：零协调

无写集 ⇒ 无协调者、不分配 globalXID。向 master 申请 start_ts（发号即登记，§2.3），
读路由到目标分片 **leader**（follower 是惰性回放冷副本，不可服务读），可见性用与
读写事务完全相同的一套规则。一次 TSO 交互后纯本地判账，不碰 raft、不碰 2PC；唯一
可能的远程交互是撞上 PREPARED 时的问询，且该窗口已被"commit_ts 尽晚取"纪律压到
最小。

### 4.4 写写冲突（SI 语义）

本方案提供 Snapshot Isolation。写写冲突沿用原生行锁排队（行锁仍按原生 xid，§8-④），
等待结束后加一条判定：**若 xmax 持有者已提交且其 commit_ts > 本事务 start_ts ⇒
serialization failure 中止**（first-committer-wins，即 PG REPEATABLE READ 的
"could not serialize access due to concurrent update" 语义按 ts 重述）。不提供
RC 式 EPQ 重读。

### 4.5 hint bits 处置（已敲定：第一期禁用，2026-08-12）

**决策**：分片表可见性分支不读、不写 `t_infomask` 的四个提示位
（XMIN/XMAX_COMMITTED/INVALID）；剪枝/freeze 路径（§6.4）亦不产生。
配套纪律：**分片表禁异步提交**（§10）。

依据（三个先决事实改变了这道选择题的价码）：

1. **免查区已存在**——`xid < clog_truncate_before` 的隐式冻结区不需要任何查询，
   hint 位在此毫无用武之地；
2. **新鲜区 hint 帮不上**——TSO 可见性还需 commit_ts 与 start_ts 比较，而 status
   与 commit_ts 在稠密 clog 同一槽内，一次访问全拿到；hint 唯一能省的是"中间带"
   （已全局可见、尚未被截断覆盖），宽度 ≈ 一个 vacuum 周期；
3. **（2026-08-12 T1.0 核查后修正）** 原"杀手问题"（hint 破坏 pagecmp 验收）
   **不成立**：已交付验收 `tests/pagecmp.py` 照抄内核 `heap_mask()` 掩码集合，
   未冻结元组的可见性提示位本就在掩码内、掩码外的位单独严格比对，FPI_FOR_HINT
   亦已入流水线（详见 `docs/P1_PRECHECK.md` 结论 A）。禁用的额外收益收窄为
   "分片表页面零非 WAL 修改，少一份对掩码集合的依赖"。决策不变，依据 = 事实
   1、2 + 重定义的三处改动量。且每分片一本 clog 已把原生 CLogControlLock 热点
   摊薄成分片粒度，禁用后的读放大代价温和。

**演进路径（预埋，不在第一期）**：若剖析显示中间带 clog 查询成为热点，采用
"**vacuum 代设**"——提示位只由 §6.4 页面趟设置（其页面修改本来就写 WAL 进流，
位变更随流回放、副本逐字节一致；vacuum 手里天然有 GlobalSafeTs），读者只读不写。
明确**不采用**"读者设位 + 按分片 clog 落盘点判定"的重定义方案：其三处大改动
（读路径摸 GlobalSafeTs、审计所有 infomask 旁路消费者、设位的流记录/掩码难题）
均被 vacuum 代设绕开。将来实现时位语义为"已提交且 commit_ts < 设位时刻的
GlobalSafeTs"（稳定谓词，逐元组提前隐式冻结）；安全前置沿用铁律"位只许断言
全局持久事实"——决议路径天然满足（参与者 clog 写 COMMITTED 必在协调组决议
多数派持久之后），单分片快路径在同步提交下满足。

【已核实，2026-08-12】原生表 hint 位与 pagecmp 的既有处理已查明：验收按内核
`wal_consistency_checking`/`heap_mask()` 口径掩可见性提示位，并比内核更严
（pd_lsn 不掩、冻结丢失判差异）；结论与证据链见 `docs/P1_PRECHECK.md`。

---

## 5 分片级 xid 与分片级 clog

### 5.1 分配器

每分片独立 32 位 xid 分配器，**完全不使用原生节点级 xid 作为分片表元组的事务标识**。
0/1/2 保留（0=Invalid 供 xmax 消毒清空用、2=Frozen 保留页面既有语义），从 3 起发
（FirstNormalTransactionId 同款），回卷时跳回 3。稠密连续分配 ⇒ **xid 即本分片
clog 行号**，寻址一跳、无需换算表。分配器状态放共享内存，批量水位 + checkpoint
持久化，恢复时用 WAL 记录体内的分片 xid 推进（§8-①）。

### 5.2 页面与查账键

数据页格式**不变**：xmin/xmax 字段里存的就是分片 xid。查账键永远是
**(分片, xid)**——读元组时所在分片由被扫描的 relation 天然给出，跨分片同值 xid
互不相干（两本账永不相交），不存在任何按裸 xid 的全节点查找。

### 5.3 clog 行结构

每分片一本 clog（现有 EnhancedClog 从"每来源节点"改域为"每分片"，§9）：

```
clog[xid] = { globalXID, start_ts, commit_ts, status, parent_xid }
status ∈ { RUNNING=0(全零槽), PREPARED, COMMITTED, ABORTED }
```

行在事务**首写本分片**时以 RUNNING 落账；PREPARE 改 PREPARED；决议到达改
COMMITTED/ABORTED。写入幂等。parent_xid 承载子事务链（仿 pg_subtrans）。
落账顺序锁定在分片流的 plsn 序上 ⇒ 每个副本重放同一个流得到同一本账
（确定性、副本一致）。

### 5.4 结构性后果：一事务多 xid

一个本地事务写 N 个分片就要持有 N 个分片 xid（每分片一个）。PG"一事务一 xid"
的全部假设因此要动，是 §8 清单里一半条目的根源。子事务在每个所写分片各领子 xid，
父子链进该分片 clog 的 parent 列。

### 5.5 与 FRD §7.5 水位规则的关系

原规则（`shard_replay.c` `PartDistAdvanceNextXidPastXid`）：follower 回放推进
**原生 nextXid** 越过流内一切 xid。新方案下改为推进**本分片分配器**——语义更干净；
副产品：#40（§13 约束 4）的原生 clog 逐页补齐不再被回放触发。切主前新 leader 的
分片分配器已被推过本分片史上一切旧号 ⇒ 同分片内 xid 永不重号（这是查账键唯一需要
的性质）。deadline-bailout 升主仍需先扫已收未放记录头推水位（FRD 既有边界情形）。

---

## 6 Vacuum / GC

### 6.1 三变量（每分片）

| 变量 | 持久化 | 语义 |
|---|---|---|
| `clog_truncate_before` | 是（CTRL 记录+checkpoint） | 本分片 clog 实际截断到哪；**隐式 freeze 点**、回卷龄的基点 |
| `ShardVacuumXid` | 是（CTRL 记录+checkpoint） | **整趟完成水印**（不是逐 xid 游标）：本次 vacuum 在页面侧实际完成到哪 |
| `VacuumTargetXid` | 否，每次实时计算 | 本次按 GlobalSafeTs 扫 clog 得到的理论可达位置 |

**隐式 freeze**：截断之后，凡 `xid < clog_truncate_before` 一律解释为"已提交且对
一切现在与未来快照可见"。成立依据：前缀规则保证截断点以下全是 committed 且
commit_ts < GlobalSafeTs，而未来一切快照 start_ts ≥ GlobalSafeTs。**不需要改写
xmin**——页面不动，对逐字节回放验收（pagecmp）是重大利好。

### 6.2 GlobalSafeTs

定义：`GlobalSafeTs ≤ 全集群所有活跃快照 start_ts 的最小值`（TSO 单调性保证未来
发出的只会更大）。计算于 master（master 即 TSO，发号与登记原子，无竞态窗口）：

- **双通道上报**：搭车（每次 start_ts 申请随行，§2.3）+ **周期心跳**（无事务也报
  "当前最老或'无'"——只靠搭车，空闲节点会永久钉死 GlobalSafeTs）。心跳即租约续期。
- **护栏一（租约）**：租约到期 master 把该节点登记清为"无"。
- **护栏二（栅栏）**：节点必须在到期前提前量 ε 自行作废本地活跃快照（此后任何读
  一律报 snapshot too old），master 才在到期点剔除。依赖仅是时钟漂移速率有界，
  不需要对钟。
- 性质：GlobalSafeTs 单调不减；vacuum 取到旧值只损失回收量、不损失正确性。

### 6.3 前缀扫描规则（计算 VacuumTargetXid）

从 `clog_truncate_before` 起顺扫本分片 clog：

- **放行**：COMMITTED 且 commit_ts < GlobalSafeTs；**ABORTED 也放行**（其垃圾由
  §6.4 清掉——若 ABORTED 也挡，一个中止事务将永久钉死截断，回卷死亡）；
- **停止**：遇到第一个 RUNNING、PREPARED、或 COMMITTED 但 commit_ts ≥ GlobalSafeTs
  的条目；VacuumTargetXid = 其前一条。

可证性质（记录备查）：RUNNING 阻挡者之上不存在 commit_ts < GlobalSafeTs 的条目
（start_ts 先于首写、首写序即落账序、TSO 单调，三段传递）——前缀规则在 RUNNING
阻挡下几乎无损，损失的只是阻挡点之上 ABORTED 事务的垃圾。死元组清除与前缀截断的
解耦记为后续优化，第一期不做。

### 6.4 页面三类动作（截断的前置条件）

对 `xid < VacuumTargetXid` 的整分片页面趟（配"**本页已洁净**"位图跳过干净页，
PG visibility map 类似物）：

1. **删中止 xmin 的元组**——否则截断后该 xmin 落入免查区被解释为"已提交全可见"，
   中止事务的幽灵行复活；
2. **删 xmax 已提交且 commit_ts(xmax) < GlobalSafeTs 的死元组**（可回收与否看
   xmax 不看 xmin：没被删过的老行是活的，零页面动作）；含索引项清理，两阶段
   （收集死 TID → 清索引 → 回收行指针）；
3. **xmax 消毒**：把截断点以下的 ABORTED xmax 与 lock-only xmax 清成 Invalid(0)
   ——否则截断后"中止了的删除"读成"早已提交的删除"，活行被判死。
   （即 `heap_prepare_freeze_tuple` 的 xmax 处置换形态重现。）

**顺序铁律：数据页、索引、堆等全部清完，才许动 clog。**

### 6.5 中断恢复（两态）

页面趟不按 xid 推进（垃圾散布任意页），故 ShardVacuumXid 只有两个有意义取值：

- 趟中崩溃 ⇒ 整趟重来（幂等：已删的再删是空操作）；
- 趟完、截断前崩溃 ⇒ `ShardVacuumXid = target > clog_truncate_before`，只补做截断。

不变式：`clog_truncate_before ≤ ShardVacuumXid ≤ VacuumTargetXid(本次)`。

### 6.6 无主 RUNNING 行认领

clog 行以 RUNNING 落账后、事务未到提交/中止时崩溃 ⇒ 该行永无人改 ⇒ 前缀永久
被挡。认领规则：

- **普通事务**：leader 崩溃恢复结束时，扫 clog 尾部 RUNNING 行，凡不在已 PREPARE
  集合里的 → 改 ABORTED（PG"崩溃即中止"在分片 clog 上补一笔）；
- **PREPARED（2PC 未决）**：不许动，等 §4.2 的 in-doubt 决议路径来改；vacuum 只等；
- **切主场景**：新主追平后流里没有该事务的提交标记 ⇒ 从未提交过 ⇒ 改 ABORTED 安全。
  依据：提交的必要条件是提交标记已多数派入流（[A]<[B] + 多数派先于本地提交），
  raft 选举保证新主拥有全部多数派条目。

> **实装（T6.4，2026-09-04，解冻批次 #6）**：三支各自的落点如下。
>
> | 分支 | 入口 | 触发点 |
> |---|---|---|
> | 普通事务（崩溃恢复） | `ShardXidEnsureClaimed()` → `shard_xid_slot_attach()` | 挂槽即认领，范围 `[claim_wm, ceiling)` |
> | PREPARED | —— | `ShardClogClaimRange()` **只改 RUNNING**，非终局的 PREPARED 天然被跳过 |
> | 切主 | `ShardXidClaimOnPromote()` → `partdist.shard_claim_on_promote(oid)` | `pg_raft_promote_prepare` 里，追平 + `dtx_close_indoubt` 之后 |
>
> **切主必须是独立入口**，不能复用第一支：`ShardXidEnsureClaimed()` 只在
> **槽位不存在**时认领，而升主的 follower 上槽位一定已经存在 —— 回放推进分片
> 分配器水位时就把它挂上了（§5.5 / T6.5）。复用的话那条路径直接 `return 0`，
> 一条都不认领。
>
> **区间上界取发号水位 `watermark` 而不是 `next_xid`**：watermark 按批次
> （`SHARD_XID_BATCH` = 4096）向上取整，比 next_xid 宽出至多一个批次。follower
> 只能从"入了流的 MARKER"学到号，而 leader 上**中止且字节未入流**的事务会悄悄
> 吃掉号 —— 宽的那个正好把这段窗口盖住。这就是 U-P5-1 那句
> "**水位是认领的输入，认领是水位的兜底**"落成代码的样子：两件事必须同期做完，
> 单做任一件都留口子。
>
> **顺序**：`dtx_close_indoubt()` 排在认领之前。认领本身不动 PREPARED（未决的
> 2PC 分片 xid 在 follower 上由回放的 `XLOG_XACT_PREPARE` 分支写成
> `TXN_PREPARED`），所以反过来做语义上也不会误伤；先闭合只是让**有决议的**先
> 落终局，剩下的才交给认领兜底。
>
> 验收 `test_promote_p6.sh` 39/0，含一条**判别断言**（先证明第一支返回 0、
> 够不着）与三条阴性对照（PREPARED / COMMITTED 不动）。

### 6.7 复制与持久化落地

- vacuum **只在 leader 执行**；其页面修改本身就是页面变更，走 pg_parwal 流被
  follower 逐字节回放——follower 不跑自己的 vacuum。
- 两个水位作为 **CTRL 记录**写进本分片流（现成 FREEZE_UPDATE 通道换语义）。
  "页在前、截断在后"由**流序自动保证**——leader 崩溃恢复按 WAL 序、follower 按
  流序，顺序铁律在所有副本上同时成立，无需额外协议。checkpoint 收口，与现有
  apply_checkpoint 一致。

---

## 7 回卷防护（分片级）

`xid_age = (next_xid − clog_truncate_before) mod 2^32`。

- **基点必须是 clog_truncate_before 而非 ShardVacuumXid**：歧义边界挂在解释规则
  （免查隐式冻结区）上；"趟完未截断"崩溃窗口里 ShardVacuumXid 跑在前面，用它算龄
  会把紧迫度算小，方向不安全。平时两者相等。
- **阶段 1**（`autovacuum_freeze_max_age` 分片版）：到龄强制启动分片防回卷 vacuum，
  **无视常规 vacuum 开关**（§13 约束 5 的教训：防回卷本就不理会
  autovacuum_enabled=off）。
- **阶段 2**（`xidStopLimit` 分片版）：age 达 2^31 − 安全边距（如 10^6）时拒发新
  xid，**该分片进只读**——护栏是分片粒度，不殃及节点和集群。
- 实话两条：拒发只是止血，解锁必须解决前缀阻挡者——超龄 RUNNING 事务按策略强杀，
  **PREPARED 未决绝不允许单方中止**（只能走协调组决议）；GlobalSafeTs 被钉死会
  间接钉死 vacuum，告警体系须把"最老快照的龄"一并纳入监控。
- 简化红利：本方案里 xid 大小比较只剩一个用途（与截断点比、判免查区），可见性全走
  时间戳——2^31 歧义面远窄于 PG 原生，但纪律照抄不省。

---

## 8 内核改动清单（postgres-src，量级远超现有 0001/0002/0004 钩子补丁）

**① 发号与事务绑定**
- 每分片 nextXid（DSA/dshash 共享内存 + 批量水位 + checkpoint 快照 + 恢复时按
  WAL 记录体分片 xid 推进）；
- `xact.c`：首写某分片时惰性领取该分片 xid；每后端 `{分片 → 本事务分片xid}` 映射；
  子事务逐分片领子 xid；
- `twophase.c`：2PC 状态文件追加 (分片, xid) 列表，崩溃恢复据此还原未决状态与锁。

**② heapam 打标与 WAL/redo**
- `heap_insert/update/delete/lock_tuple/multi_insert` 盖 xmin/xmax 改用本分片 xid；
- `xl_heap_*` 记录体增加分片 xid 字段（**记录头 xid 保持原生**，避免破坏恢复期
  其他机制）；`heap_xlog_*` 改用记录体分片 xid 盖章——同时保住"leader 崩溃恢复
  redo 与 follower rm_redo 回放逐字节一致（pagecmp）"；
- `xl_xact_commit/abort` 追加 (分片, xid) 列表，供崩溃恢复重做分片 clog 落账。

**③ 可见性与快照（最大的一块）**
- `heapam_visibility.c` 全部 Satisfies* 按"是否分片表"分叉，分片表走 §4 规则；
- `TransactionIdIsCurrentTransactionId` 分片表路径查后端映射（自见性）；
- `TransactionIdIsInProgress`/EPQ 冲突判定：分片表按"clog 无行或 RUNNING = 在跑"，
  冲突中止规则见 §4.4；
- hint bits：分片表禁用（已敲定，§4.5）——可见性分支不读不写四个提示位；
- `GetSnapshotData`：分片表不消费原生 xmin/xmax/xip，快照 = start_ts；混合查询按
  relation 分叉；relcache（RelationData）加"是分片表"O(1) 标志位。

**④ 行锁与等待**
- `XactLockTableWait`：共享内存维护"活跃 (分片, 分片xid) → 持有者原生 xid"反查表，
  等待路径先反查再等（行锁排队仍按原生 xid）；
- MultiXact 与分片 xid 空间根本冲突：第一期禁分片表 SELECT FOR SHARE 多锁者；
- SSI（predicate.c）按原生 xid：分片表禁 SERIALIZABLE。

**⑤ vacuum / freeze / 剪枝**
- `HeapTupleSatisfiesVacuum` + `pruneheap.c` GlobalVis 判定改按 §6；
- `pg_class.relfrozenxid` 对分片表重解释；`vac_update_datfrozenxid` **排除**分片表；
  `autovacuum.c` 分片表按 §7 触发；
- CLUSTER/VACUUM FULL（rewriteheap）、CREATE INDEX CONCURRENTLY：分叉或第一期禁
  【待定】。

**⑥ clog / subtrans 分片化**：原生 clog/subtrans 对分片表停用，由 §5.3 分片 clog
（含 parent 链）接管。

（②的 WAL 记录体扩展方式、①的恢复推进细节为实现期设计点，原则已定：记录头保原生、
记录体带分片 xid。）

---

## 9 项目侧组件改动清单（pg-partdist-src / pg-raft-src / master）

| 组件 | 改动 |
|---|---|
| `partwal_sync.c` | 捕获时记录头 xid 换为该分片 xid（查后端映射）；MARKER 按 (事务, 分片) 发射并带该分片 xid |
| `shard_replay.c` | §7.5 水位改推进分片分配器（§5.5）；#40 原生 clog 补齐不再被回放触发 |
| `enhanced_clog.*` | 作用域"每来源节点"→"**每分片**"（`pg_gclog/<node>/` → `pg_gclog/<shard>/`）；槽位直接按分片 xid 索引；行结构扩 globalXID 列 |
| `shard_xidmap.*` | **整体删除**——分片 xid 稠密即行号，两跳变一跳 |
| gxid 编码 | `(node<<48)\|xid` 低位换分片 xid，含义不变 |
| `raft_consensus.c` DTX | `DtxRecordPayload.xid`、写集捕获、`check_fastpath_divergence`（现读原生 CLOG）全部改读分片 clog；**`dtx_master_pre_record_commit` 整块从 master 搬迁到协调者分片 leader**（结构性最大搬动） |
| master 侧 | 新增：Proxy 粘性路由 + 会话→协调者映射；TSO 服务（发号+原子登记+GlobalSafeTs+租约/心跳）；TSO 自身 HA：v1 不做（§2.4 裁定），后续版本按"绑 group0 leader"方向立项 |
| SQL schema | `dtx_decision`/`dtx_participant` 结构不变（值来源改 TSO）；FREEZE_UPDATE CTRL 换 §6.7 语义 |
| 新组件 | "本页已洁净"位图；无主 RUNNING 认领（恢复期+升主期）；回卷两阶段护栏；快照登记/心跳客户端 |
| 验收 | tx2 基线 438 项中凡断言 xmin/clog 的逐个重写；vacuum/GC/回卷/租约栅栏新增套件 |
| 运维纪律 | 每次内核补丁变更重编并把 `pg-install/bin/postgres` 同步提交回仓库，补丁与二进制成对更新 |

### 9.1 语句转发载体（已敲定：Citus MX，2026-08-12）

**决策**：协调者 → 参与分片的语句分发采用 **Citus MX**——元数据同步至 worker，
由协调者分片 leader 所在 worker 上的 Citus 完成语句改写、分片路由、远端连接管理
与 PREPARE TRANSACTION 下发。选择依据：

1. 已交付的整条 DTX 链（Citus 居首的 PRE_COMMIT 顺序纪律、0004 触发点 =
   "全部 PREPARE 已成功"点、本地读 `pg_dist_transaction` 取参与者集合）**在哪个
   节点驱动事务就在哪个节点成立**——§9 表中"决议逻辑搬迁"因此从重写退化为换节点跑；
2. 自建 dispatcher 的真实成本是重写分布式 planner（逻辑表名→分片表改写、多分片
   散播聚合、跨节点死锁检测），不是 libpq 管道；仓库内 `write_router.c`
   （milestone 1.2 遗留死代码，ROUTE_REMOTE 直接报错）即当年自建路由半途而废的证据；
3. MX 的最危险缺口是 fail-safe 的：切主后元数据未跟上、写发给旧 leader 时被
   pg_raft 写栅栏（非 leader ⇒ ERROR）挡下——可重试错误，非数据损坏。

**配套改动**：`raft_update_citus_placement` 的 pg_dist_placement 翻转从"只更新
master"扩展为"传播到所有持元数据节点"。

**P4 前置验证实验（三条，未通过不得进入 P4 实施）**：

1. 现集群搭建脚本加节点方式是否已启用元数据同步；Citus 13.1 从 worker 驱动
   "含 PREPARE 的多分片写事务"与 master 驱动行为逐项一致；
2. `citus.recover_2pc_interval=-1`、`shared_preload_libraries` Citus 居首等纪律
   在 worker 上同样生效（共享 conf 模板应天然满足，须实测确认）；
3. worker 驱动时本分片写走 Citus local execution（不绕 libpq 回环），
   `wal_insert_hook` 与 DTX 参与者捕获在该路径上行为不变。

**保留后门**：自建 dispatcher（libpq）不进第一期；若后期剖析显示带分片键的单分片
点写为主且 Citus 执行器开销成瓶颈，可作为窄快路径优化叠在 MX 之上。

### 9.2 Citus 内部路径审计执行方案（已敲定：四层，2026-08-12）

背景：Citus 是不打补丁的外部代码库（gitlink 原样是项目资产），其内部操作（搬分片、
COPY、维护任务）自带"亲手读写分片数据"的路径，且全部假定原生快照/可见性契约——
本方案把分片表契约整个换掉了。故审计形态只能是"**枚举 + 门禁**"而非"定义 + 证明"，
且默认拒绝：

**第 1 层：fail-closed 安全网（白名单，不是黑名单）。** 分片表可见性分支与打标路径
各加守卫：未加入全局事务（无登记 start_ts）的快照读分片表 ⇒ ERROR；未持有 gxid 的
写分片表 ⇒ ERROR。绝不静默回退原生语义。审计遗漏的代价从"静默读错数据"降为
"响亮报错"——漏网路径伤可用性、不伤正确性（PRE_COMMIT 白名单教训在读路径的复用）。

**第 2 层：连接加入协议。** 参与者侧调用
`partdist_join_global_txn(gxid, start_ts, coord_gsid)`；协调者 worker 的 Citus 每开
一条参与分片连接先发此调用（借 `assign_distributed_transaction_id` 的同位置每连接
前置机制，不改 Citus）。顺带完成两件方案内的事：start_ts 跨连接传播（跨节点读一致
性载体）、coord_gsid 下发（§4.2 NULL 不变式窗口再收紧）。审计收敛为枚举 Citus 的
连接建立点（普通查询/COPY/repartition/中间结果）逐一确认过此道【核实点 ①】。

**第 3 层：分类处置（第一期）。**

| 类别 | 路径 | 处置 |
|---|---|---|
| 禁用 | rebalancer、citus_move_shard_placement、undistribute_table、alter_distributed_table | ProcessUtility/UDF 拦截报错；"搬分片"= 后续 raft 成员变更专项 |
| 接入后放行 | 多分片 SELECT/DML、COPY | 走第 2 层协议；COPY 协调者 = 首行命中分片（§1.2） |
| 确认分叉覆盖 | ANALYZE（采样走 vacuum 可见性路径） | §8-⑤ 分叉天然覆盖，写用例确认 |
| 保持原生 | 元数据同步、分布式死锁检测、shard size 统计 | 不碰分片表元组数据，零改动 |
| 专项【核实点 ②】 | 纯引用表写事务 | 无分片写集 ⇒ 无协调者分片，且 recover_2pc_interval=-1 已关 Citus 原生恢复，决议与恢复两头落空；第一期倾向**引用表建表后只读**；已交付系统对引用表的使用现状未验证，须先查 |

**第 4 层：负向测试门禁。** 禁用项负向用例 + 放行项正向用例 + 安全网触发用例挂进
验收基线，作 P4 前置；此后 Citus 升版本，暗道变化由安全网当场拦截——审计是长期
在线的门禁，不是一次性读代码。

---

## 10 第一期功能限制

| 功能 | 处置 | 原因 |
|---|---|---|
| 分片表 SERIALIZABLE | 禁 | SSI 按原生 xid；本方案语义即 SI |
| 分片表 SELECT FOR SHARE（多锁者） | 禁 | MultiXact 实例级 |
| 分片表逻辑解码 | 禁 | 解码按原生 xid 组事务。**实装位置（T6.6 修正）**：禁令在**入口**——`pg_create_logical_replication_slot` / `pg_logical_slot_{get,peek}_{,binary_}changes` 进禁用函数表，白名单非空时连槽都不许建。此前守卫挂在可见性层（`HeapTupleSatisfiesHistoricMVCC`），够不着：实测崩溃发生在**解码阶段**（`*** stack smashing detected ***` → SIGABRT → 整节点重置，R-P6-7），根因是补丁 0005 的 4 字节分片 xid 尾缀与原生解析器的长度约定冲突，**未修，仅遏制** |
| 分片表原生流复制热备读 | 本来就不用 | 本项目用自己的回放 |
| 分片表异步提交（synchronous_commit=off） | 禁 | 可见性/提交点语义只许断言已持久事实（§4.5）；[A]<[B] 与多数派提交点均以同步提交为前提。**实装位置**：`shard_xid_for_current_xact` 写路径（T6.6 补——此前本行只有文档、**代码零守卫**）；`on`/`local`/`remote_write` 放行 |
| CIC / CLUSTER / VACUUM FULL | **禁（V4 已裁定，2026-08-13）**；P5 freeze/回收全章落地后再评估 CLUSTER/VACUUM FULL，CIC 随索引专项 | CIC 的多快照阶段与 validate 等待全按原生 xid 机制，且 P 期分片表本就禁索引；CLUSTER/VACUUM FULL 走 rewriteheap 的 freeze/裁决会拿分片 xid 查原生 clog，且换 relfilenode 需 fileset 重绑（复制面）。ANALYZE 已于 T2.6 解禁（补丁 0008 读侧分叉，只判不收） |
| Citus rebalancer / move_shard_placement / undistribute_table / alter_distributed_table | 禁 | 原生快照读分片表 = 静默错读；与 raft 管理的放置冲突；搬分片 = 后续 raft 成员变更专项（§9.2）。**实装位置**：`ShardGuardCheckPlan` **遍历计划树**取 FunctionScan/targetlist/qual（T6.6 修正——首版扫 `pstmt->rtable` 的 `rte->functions` 是死代码，setrefs.c 已把它清成 NIL，于是 `SELECT * FROM f(...)` 整类写法一直绕得过去，R-P6-8） |
| 含分片写的 PREPARE TRANSACTION | P1 禁（PRE_PREPARE 拦截，且拦截先于 PartWAL 刷流——回调 LIFO 序，见 DEV PLAN T1.9 记要） | 后端映射/临时提交表无法跨会话延续 prepared 事务；若字节先进流再中止，还会打断在途复制、让 leader plsn 跑到多数派前头（T1.9 实测） |
| 分片打标表的 Citus 路由写（coordinator 经手的 INSERT/UPDATE/DELETE/COPY） | P1 禁——验收与使用一律 leader 直写 | 本分支 Citus 分布式写一律 2PC（PREPARE TRANSACTION，DTX 链路即建于其上）→ 撞上一条禁令，事务在 PREPARE 点中止（T1.9 实测两轮）；2PC × 打标的接线 = P4 正题（决议搬迁 + MX） |
| 引用表运行期写入 | 建表后只读【需核实现状】 | 无分片写集 ⇒ 无协调者分片；Citus 原生 2PC 恢复已关（§9.2） |

---

## 11 待定项汇总

1. **Citus 内部路径审计**：执行方案已敲定（四层，§9.2），余两个核实点——
   ① Citus 连接建立点枚举的完备性（普通查询/COPY/repartition/中间结果）；
   ② 引用表现状（已交付系统是否有引用表写；第一期倾向建表后只读）。
2. ~~CIC/CLUSTER 等维护命令的分叉 vs 第一期禁用~~——**已裁定第一期禁用**
   （2026-08-13，V4，理由入 §10 行内；CLUSTER/VACUUM FULL 在 P5 出口再评估，
   CIC 随索引专项）。
3. （已记为后续优化，非待定）死元组清除与前缀截断解耦；vacuum 代设 hint
   （§4.5 演进路径）；"本页已洁净"位图之外的免趟加速。

（原"语句转发载体"已于 2026-08-12 敲定为 Citus MX，移至 §9.1；
原"hint bits"已同日敲定为第一期禁用 + vacuum 代设演进，移至 §4.5；
原"TSO 服务自身 HA"已同日裁定 v1 不做、绑 group0 方向留档后续版本，移至 §2.4。）

---

## 12 分期计划建议

| 期 | 内容 | 验收要点 |
|---|---|---|
| P1 | 内核原型：分片发号器 + heapam 打标 + WAL 记录体扩展（单分片、无 2PC） | leader 崩溃恢复 redo 与回放 rm_redo 页面逐字节一致（pagecmp）；自见性/行锁回归；hint/pagecmp 既有处理已核实（docs/P1_PRECHECK.md） |
| P2 | 分片 clog（EnhancedClog 改域）+ 可见性分叉 + 只读路径 + xid_map 摘除 | 单分片读写混合 + 崩溃恢复下可见性正确；无主 RUNNING 认领生效 |
| P3 | master 薄化：Proxy 粘性路由 + TSO（内存单调计数器，HA 按 §2.4 裁定不做）+ GlobalSafeTs 双通道/租约/栅栏 | 竞态窗口注入测试（空闲节点、租约过期读者被栅栏拒绝）；TSO boot 标记防呆生效 |
| P4 | 多分片 2PC：协调者选定/globalXID/commit_ts 时机/决议搬迁（Citus MX 驱动，§9.1）/三态问询 | **前置：§9.1 三条验证实验通过 + §9.2 审计门禁用例集就绪**；崩溃矩阵（§3.4）逐格演练；in-doubt 收敛时延受"选举+追平"界定 |
| P5 | vacuum/GC 全章 + 回卷两阶段护栏 + CTRL 水位落地 | 幽灵行/活行误删注入测试（§6.4 三类动作各一）；两态恢复；age 护栏触发 |
| P6 | 切主/恢复全链路联测 + 门禁并入 tx2 基线 | 全量套件零 FAIL；§10 限制项的负向用例（应报错的确实报错） |

---

*本文由 2026-08-12 方案评审对话整理定稿；对话中的逐条敲定记录见 §0 决策日志。*
