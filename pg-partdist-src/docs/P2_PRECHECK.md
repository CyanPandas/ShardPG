# P2 前置核查（T2.0）

日期：2026-08-13。对应 `TX_TSO_MVCC_DEV_PLAN.md` §3.1 T2.0 的五个问题。
所有行号以容器 `/work/postgres-src`（基线 e9067809 + 0001/0001v2/0002/0004/0005/0006）
与工作区 `pg-partdist-src` 当前树为准。

---

## 结论一：EnhancedClog 复用面 —— **共存新实例，不改造旧域**

**现状**（`src/replay/enhanced_clog.c`，335 行）：

- 键 = 64 位 gxid，经 `GxidNodeId`/`GxidLocalXid` 拆成 **uint16 node_id** +
  local_xid；路径 `pg_gclog/<node_id>/<8位十六进制段号>`；
- 24 字节槽 {start_ts, commit_ts, status, parent_xid}，有
  `StaticAssertDecl(sizeof == 24)` 钉死磁盘格式；
- 全零槽 = TXN_RUNNING = 未决 = 不可见（与 §5.3 需要的语义天然同向）；
- I/O 纪律：OpenTransientFile 不缓存 fd（resource owner 会关）、幂等 pwrite
  不取锁、脏段列表 + 延迟 fsync（`EnhancedClogSync`）；
- 四个消费者（dtx_participant / replay_worker / shard_replay / enhanced_clog
  自身）全部按 gxid 调用，服务 438 基线的回放/DTX 路径。

**为什么不能参数化改造**：分片域的键是 (Oid shard, uint32 sxid)——shard Oid 是
32 位，塞不进 uint16 node_id 的编码与目录命名；改 gxid 编码或路径方案 = 动基线
磁盘格式与四个消费者 = 动 438。

**定案**：新建 `src/shard_clog.c` + `include/shard_clog.h`，独立 API
（按 (shard, sxid) 键），**克隆**旧域的全部 I/O 纪律（无 fd 缓存/幂等 pwrite/
脏段延迟 fsync/稀疏洞=RUNNING）；旧域一个字节不动。

**实施偏差（对设计 §5.3/§9 的两处，理由如下）**：

1. 目录用**独立顶层 `pg_shard_clog/<oid>/`**，不挤进 `pg_gclog/`——
   与 `pg_shard_xid/<oid>`（水位）对称，DROP GC 一次删两个同构目录；
   避免碰 GCLOG_DIR 常量与旧域目录扫描逻辑。
2. 槽扩到 **32 字节** {uint64 globalXID, uint64 start_ts, uint64 commit_ts,
   uint32 status, uint32 parent_xid}——§5.3 行结构原文照收；P2 只用 status，
   globalXID 恒 0（P4 起用）、ts 两列占位（P3 起用）、parent 恒 0（P4 子事务）。

---

## 结论二：shard_xidmap —— **P2 只做路径隔离，物理删除挪 P6**

**现状**：`xid_map` 是回放侧 per-ShardReplayCtx 的哈希（record xid → gxid），
消费者 = replay_worker.c（槽位迁移/快照恢复）、replay_checkpoint.c（**apply
checkpoint 磁盘格式含 xid_map 快照，CRC 覆盖**）、shard_replay.c（水位/登记）。
这是 438 基线里 r1/tx1 等套件的现役机器，删除 = 改 checkpoint 磁盘格式 = 动基线。

**分片表路径隔离已成立**（grep 实证）：`shard_xid.c` / `shard_visibility.c` /
补丁 0005/0006 零 xid_map 引用；leader 路径不经过 replay ctx。follower 侧分片表
记录头是原生 xid（0005 设计），会像普通记录一样进 xid_map——无害噪声：follower
分片壳表在 P1/P2 纪律下**永不被查询**（R1 三方判据的既有约束），分片可见性
永不咨询 xid_map。

**定案**：P2 出口条目"xid_map 路径隔离判定"以本节为档；物理删除（连带 apply
checkpoint 格式变更）并入 P6 基线合并时统一做。

---

## 结论三：补丁 0007 机制 —— **xinfo 可选块可行，标记时序三段闭环成立**

**xinfo 位空间**（`src/include/access/xact.h:206-214`）：已用 bit 0–8
（DBINFO…DROPPED_STATS），uint32 还剩 23 位。取 **bit 9 =
`XACT_XINFO_HAS_SHARD_XIDS`**。

**记录构造挂点**：`XactLogCommitRecord`（xact.c:5663）/ `XactLogAbortRecord`
（:5835）。新增函数指针钩子，扩展侧返回本事务 (分片oid, 分片xid) 数组
（后端映射里现成）；n>0 时设 xinfo 位并把数组注册进记录体。无分片写的事务
**记录体逐字节零变化**（可选块机制天然保证）。

**解析面**：`xl_xact_parsed_commit`（xact.h:389，abort 同型别名）扩两个字段
（nshardxids / 指针）；`ParseCommitRecord`/`ParseAbortRecord`（xactdesc.c）加
一个 if 块。desc 函数顺带打印，换取验收标准"pg_waldump 可辨"。逻辑解码
（DecodeCommit）不读新字段即忽略——分片表 P1 已禁逻辑解码，无交互。

**redo 挂点**：`xact_redo_commit`（xact.c:5979）/ `xact_redo_abort`（:6133）
末尾新增钩子，把解析出的列表交扩展重做 clog 落账（写入幂等，重复 redo 安全）。

**正常路径标记时序**（xact.c CommitTransaction 实测行序）：
`RecordTransactionCommit()`(2293，插入+刷提交记录+原生 clog) →
`ProcArrayEndTransaction`(2317) → **`CallXactCallbacks(XACT_EVENT_COMMIT)`
(2335) → RESOURCE_RELEASE_BEFORE_LOCKS(2340) → RESOURCE_RELEASE_LOCKS(2360)**。
即：既有 XactCallback 里做 COMMITTED 标记，天然落在"提交记录已持久"之后、
"行锁释放"**之前**——被唤醒的等待者重评时 clog 已是终态，无活锁窗口。
ABORT 对称（XACT_EVENT_ABORT 亦先于锁释放）。

**三段闭环**：
1. RUNNING：首写取号时落账（T2.3，发号成功后返回前，fail-closed）；
2. COMMITTED/ABORTED：记录落盘后回调标记；崩在记录与标记之间 → redo 钩子补齐；
3. 崩溃隐式中止（无 abort 记录）→ 槽保持 RUNNING → T2.4 认领判 ABORTED。
   分片写事务必然已分配原生 xid（0005 记录头保原生 = 必调
   GetCurrentTransactionId）⇒ 提交/显式中止必有记录，闭环无缝。

---

## 结论四：认领触发点与扫描上下界 —— **懒触发双入口 + 启动恢复上限**

**触发点**：每分片一个本次启动内存标志"已认领"。**两个入口都检查**：
① 发号器槽位首次初始化（写路径）；② 可见性首次咨询该分片 clog（读路径）。
只挂写路径不够——UPDATE 先做旧元组可见性判定、后取号，撞无主 RUNNING xmax 时
可能尚未触发分配器。另提供显式 SQL 函数供测试与运维。

**上界（正确性关键）**：认领扫描上界 = **启动后首次触达时刻的恢复上限**
（max(水位文件, T2.5 WAL 推进值)，在发出任何新号**之前**捕获并冻结）。
不能用"当前发号水位"——懒触发可能晚于重启后新事务开跑，用动态水位会把
崩溃后新生的 RUNNING 活事务误判 ABORTED（误杀）。

**下界与持久化**：认领水位（已认领上界）持久化进水位文件：**4 字节扩 8 字节**
{alloc_wm, claim_wm}，读路径兼容旧 4 字节格式（缺列按 claim_wm=3 即全量扫）。
扫描 [max(3, claim_wm), 恢复上限) 的 RUNNING 槽（含跳号全零洞，判 ABORTED
无副作用）改写 ABORTED，完成后 claim_wm=恢复上限落盘。幂等，二次崩溃安全。

---

## 结论五：partition_map 门控 —— **加列 + 注册函数，白名单 GUC 降级为并集**

**现状**（`sql/pg_partdist--1.0.sql:15`）：partition_map 六列，pk partition_id，
UPDATE 触发器只做 version 自增。**实证**：全部 tests/sim 对 partition_map 零
位置式 INSERT（都走函数/显式列名），**尾部加列不破坏任何既有语句**。

**定案**：加列 `shard_mvcc BOOLEAN NOT NULL DEFAULT false`（尾列 + DEFAULT，
既有行自动 false = 基线表默认不打标，P1_PRECHECK 结论 C 的 438 保护延续）；
新注册函数翻转该列并 bump version；metadata cache 缓存该标志提供 O(1) 判定
（失效跟随 partition_map 既有版本机制）。GUC 白名单 `pg_partdist.shard_relids`
保留为测试便捷通道，运行时判定取**两者并集**。TOAST 归属仍走 P1 的命名规约 +
后端映射，不进 partition_map。

---

## 风险登记（并入 DEV PLAN §5）

- **R-P2-1 解析面回归**：0007 触碰 commit/abort 记录解析（xactdesc/redo/解码
  三处共用）——无分片块的记录必须逐字节零行为变化；438 全量是兜底门禁。
- **R-P2-2 认领误杀**：扫描上界一旦取成动态发号水位，会把重启后新活事务判
  ABORTED（数据静默消失）。上界必须是冻结的启动恢复上限（结论四），套件必须
  含"重启后立即开新事务再触发认领"的用例。
