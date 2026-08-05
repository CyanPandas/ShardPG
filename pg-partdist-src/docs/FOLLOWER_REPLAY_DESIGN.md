# pg_partdist Follower 物理重放设计(v3 实现版)

> **核心目标**:Follower 收到 Leader 通过 Raft 复制过来的 WAL 字节流后,把其中记录的物理修改
> 应用到本地对应的**表、索引和 TOAST 文件**上;事务层信息(gxid、时间戳)同步登记,
> 为全局 MVCC 可见性判定预留接口。
>
> **核心机制**:复用 PostgreSQL 原生 redo 引擎——`DecodeXLogRecord` → 改写 `RelFileLocator`
> → `rm_redo`。不把 WAL 反解析成 SQL,不经过执行器;redo 例程按 record 携带的
> 文件号 + 块号直接定位数据文件页面、修改字节。
>
> 代码定位基于 `postgres-src`(**PostgreSQL 16.14**,本版行号已在本仓库源码中复核)。
> follower 侧回放代码尚未落地,本文即其开发依据;leader 侧捕获链路
> (patch 0001 + demux)已存在,§5 描述其需要的改造。
>
> 本文与《问题 3 设计说明》(reply_v1)为同一方案的两份视图:reply_v1 面向评审,
> 本文面向实现;3.3 节"五阶段回放流程"与本文 §7 一一对应,§15 给出对照表。

## v3 相对 v2 的修正与决策落定

| # | v2 的状态 | v3 的落定 | 章节 |
|---|-----------|-----------|------|
| ① | xid/clog 是"最大开放决策点",推荐方案 A(集群 xid 区间批发) | **弃用方案 A**,采用已评审定稿的 gxid + 每分区 xid_map + 增强型 CLOG + ShardRouteEntry 路由方案(不改 tuple 字节、不动本地 xid 分配路径) | §9 |
| ② | COMMIT/ABORT 标记 `data_len=0`,复用原生 clog(`TransactionIdCommitTree`) | 标记记录带 `TxnMarkerPayload`(TSO 时间戳 + 子事务列表),写**增强型 CLOG**;原生 clog 不感知外来 xid | §4.3、§7.6 |
| ③ | 记录头 `TransactionId xid`(32 位) | parwal-3.0:头部改为 64 位 `GlobalTransactionId gxid`,头大小仍 40 字节 | §4.1 |
| ④ | `AdvanceNextFullTransactionIdPastXid` 直接调用 | 该函数带 `Assert(AmStartupProcess() \|\| !IsUnderPostmaster)`(varsup.c,已核实),bgworker 中触发断言;改用扩展自带加锁变体 `PartDistAdvanceNextXidPastXid()` | §7.5 |
| ⑤ | checkpoint 只存游标 | 增加 `max_replayed_fxid`(FullTransactionId,升主水位 W 的持久化来源)与 xid_map 快照,单文件原子落盘 | §8.4 |
| ⑥ | — | 明确 `orig_lsn` 的语义 = 记录在 leader 侧的 **EndRecPtr**(end LSN),而非起始 LSN | §4.2 |
| ⑦ | — | SMGR 类记录的 `RelFileLocator` 在 main data 而非 block ref,重映射需特判 | 附录 A |

## ★ 形态修正:本项目采用**触发式惰性回放**(2026-08-01,用户确认)

> **v3 稿(以及 §7 全文)描述的是"持续回放":worker 常驻轮询,已提交的字节一到
> 就 redo 进去。本项目实际要的不是这个 —— 是惰性回放。** 二者的差别不在回放
> 引擎,在**何时触发**。

| | 持续回放(v3 稿) | **惰性回放(本项目)** |
|---|---|---|
| 平时 | worker 不断消费并 redo | **一条 redo 都不做**;副本只有 P2 平凡 apply 落下的字节 |
| 触发 | 无(始终在跑) | 升主时调 `replay_catchup(shard, commit_index)` 同步追平,之后才对外服务 |
| 回放上界 | 需要 pg_raft 持续注入 commit_index | 触发那一刻由调用方给定 |
| 副本状态 | 始终"热" | 平时是"冷"字节,追平后才成为可用副本 |
| 每节点开销 | N 个分片副本 = N 条 redo 流常驻 | 零 |
| 切主延迟 | 最小 | 与积压量成正比 |

**为什么惰性更契合本架构**:每个节点同时是若干分区的 primary、又是另一些分区的
secondary(§9 前提)。持续回放意味着每个节点常驻几十条 redo 流一直烧 CPU/IO,
而这些副本绝大多数永远不会被提升。惰性把这笔开销挪到真正需要的那一刻。

**顺带消解掉一个正确性风险**:follower 收到 AppendEntries 就先 fsync 再 ack,
即**字节落盘早于条目提交**。持续回放若按"本地有什么就放什么"推进,会 redo 掉
尚未达成多数派的条目,而物理 redo **不可逆** —— 一旦该条目被 Raft 截断就无法
回滚。惰性回放的触发点(升主)恰好是确切知道提交位置的时刻,`bound` 由调用方
传入 commit_index,天然不会越界;未提交的字节还躺在盘上没动过,截断直接删即可。

**实现落点**(R1 的引擎全部复用,只改驱动层):
- 槽位状态机 `IDLE ↔ CATCHING_UP`(失败停 `FAILED` 并留 errmsg);
  平时 `target_plsn <= applied`,worker 在主循环里直接跳过 —— 这就是"惰性"的全部。
- `replay_enable` 语义改为 **arm(允许被触发)**,不再意味着开始回放;
  `armed` 标记持久化,节点重启后恢复,但**重启也不会自行回放**。
- `replay_catchup(shard, upto, timeout_ms)`:写目标 → 唤醒 worker → 同步等待
  → 返回 `applied`。`upto = NULL` 表示追到本地全部字节(运维/测试便利),
  **生产升主路径必须显式传 commit_index**。
- 触发入口经 rendezvous variable `partdist_replay_catchup_hook` 导出给 pg_raft,
  两个扩展无编译期依赖。
- 追平失败被 `PG_TRY` 捕获落到 `FAILED`,不拖垮 worker(shmem worker 崩溃会连带
  整个节点重置);游标在 `apply_checkpoint` 里,下次触发只补未完成的部分。

**§7 以下各节描述的回放管线本身(五阶段、loc_map、页级幂等、apply checkpoint)
全部照旧有效**,只是"每 shard 一个 worker 串行消费"改为"每 shard 一个 worker
串行**追平**,平时休眠"。

---

## v3.1 评审修正(2026-07-31,R1 开工前)

对照 PG 16.14 源码复核 v3 后补入。①为正确性缺陷,②③为立项/排期阻断项,④⑤⑥为落地约束。

| # | 修正 | 章节 |
|---|------|------|
| ① | **捕获侧漏掉无块引用记录**:`XLOG_SMGR_TRUNCATE/CREATE` 只有 `XLogRegisterData`,`blocks[]` 恒空,原捕获判据永不命中,而 §5.3 要求捕获它。v3 只做了 follower 侧 main-data 重映射(附录 A),**捕获侧的另一半缺失** ⇒ 副本永不截断,静默分歧 | §5.2、§5.3、§14.2 |
| ② | **R4 硬阻断于 R3**:promoted shard 的元组 xmin 是旧 leader 的 xid,读它必须走 §9.4 规则 3(R3 实装);R3 又阻塞于本文档外的全局 MVCC 文档 ⇒ R4 验收的"继续读写"在 R3 前不可达 | §14.2 |
| ③ | **per-shard 常驻 bgworker 撞 `max_worker_processes`**(默认 8,实测环境即 8),而每节点 shard 数为几十~上百 ⇒ 改为 worker 池 + 轮转认领;与 pg_raft 的 `RAFT_MAX_GROUPS=32` 同形状,应统一处置 | §7、§13.10 |
| ④ | **`EB_SKIP_EXTENSION_LOCK` 写死在 `xlogutils.c:526`,不受 `InRecovery` 控制** ⇒ "单写者"不能只写在前置条件里,必须落成排他认领锁,否则并发扩展同一文件是静默堆损坏 | §13.10 |
| ⑤ | ~~decoded 内含指向 body 的裸指针~~ **实现时复核推翻**:PG16.14 `DecodeXLogRecord` 深拷贝全部载荷进 decoded 尾部空间,`body` 在 decode 返回后即可复用。**真正的陷阱在读取侧**:`readRecordBuf` 仅对跨页记录持有原始字节,取"原始记录字节"必须自行装配(旧 demux 崩溃恢复路径曾因此写入垃圾 payload,已随 R1 修复) | §7.4 |
| ⑥ | **升主推进 WAL 位点的运维面空白**:跳过的段号成为永久空洞,影响归档连续性、`max_wal_size` 账目、级联备库 ⇒ R4 立项前须出处置结论 | §11 |
| ⑦ | **R1 验收判据"逐页 diff 一致"不可达**(实测修正):FPI 恢复会把页内空闲空洞清零,主库保留残字节,原生备库亦然 ⇒ 判据改为"两侧 pd_lower/pd_upper 相同且**洞外**逐字节一致"(实测洞外差异为 0) | §14.2 |
| ⑧ | **worker 进程初始化三要素**(实测,缺一即段错误):`BackgroundWorkerInitializeConnection(NULL)` 走 BaseInit、`CreateAuxProcessResourceOwner()` 供 buffer pin 记账、`RmgrStartup()` 建各 rmgr 的 redo 内存上下文。v3 稿"纯 `BGWORKER_SHMEM_ACCESS`、无 DB 连接"不成立 | §7 |
| ⑨ | **`pd_prune_xid` 也必须排除在一致性判据外**(L1 实测):`heap_xlog_prune()` 原文注释 "we don't worry about updating the page's prunability hints" —— **内核 redo 故意不复制该字段**,它只是"这页可能有东西可清理"的优化提示。实测 leader 侧 VACUUM 后归 0、follower 侧保留 prune 前旧值(6 字节,3 页各 2 字节),原生备库同样如此。判据 = 空洞与 pd_prune_xid 之外全等 | §14.2 |
| ⑩ | **一致性判据改为对齐内核 `heap_mask()`**(R2 实测):逐次补例外的路子在 R2 又撞墙 —— 主堆稳定 4 字节差异,全部落在**元组间 MAXALIGN 对齐填充**里。改为照抄 `wal_consistency_checking` 给堆页用的那套掩码(`heap_mask()` + `bufmask.c`):空洞 / `pd_prune_xid` / `pd_flags` 三个提示位 / 未冻结元组的 `t_infomask & HEAP_XACT_MASK` / `t_cid` / 元组对齐填充。**但 `pd_lsn` 刻意不掩**(R1 的核心主张就是页 LSN 逐字节等于 leader 的 `orig_lsn`),并额外保留"leader 已冻结而 follower 未冻结 → 判为差异"(冻结记录是写 WAL 的,丢了是真缺陷) | §14.2 |
| ⑪ | **`autovacuum_enabled=off` 挡不住防回卷**(R2-e 核实):`autovacuum.c:3196` 是 `if (!av_enabled && !force_vacuum)` —— `relfrozenxid` 落后超过 `autovacuum_freeze_max_age` 时 off 被忽略,副本壳表照样被强制 anti-wraparound vacuum 扫到,而它的元组带外来 xid,本地 clog 无法解释;同时压住库级 `datfrozenxid`、阻塞 clog 截断。§13 约束 4 拿 off 当保护是**不够的**,只是推迟。R2-e 先交付可观测(`replay_freeze_status()`),处置待 §12 CTRL 通道 | §13 |

另核实两处对本设计有利、v3 未提及的事实:`CreateFakeRelcacheEntry` 在 16.14 已不再
`Assert(InRecovery)`(§13.3);核内 `AdvanceNextFullTransactionIdPastXid` 写 `nextXid`
本就持 `XidGenLock`,不安全的仅是那次无锁读 ⇒ §7.5 的持锁读-改-写变体严格更安全。

---

## 0. 目标与非目标

**目标**

1. Follower 对其托管的每个 shard 副本,持续消费该 shard Raft 组已 commit 的 WAL 字节流,
   将物理修改应用到本地该 shard 的主堆、全部索引、TOAST 堆、TOAST 索引文件。
2. 崩溃后可从持久化游标恢复重放,不丢不重(页级幂等 + 记录级游标)。
3. 回放同时维护事务层状态:xid_map(local_xid → gxid)、增强型 CLOG(时间戳 + 状态)、
   本地 nextXid 水位——升主后可见性判定所需的全部输入在回放期间就绪。
4. Leader 宕机后,任一追平的 Follower 可提升为新 Leader 并继续读写。

**非目标(本期)**

- 不做逻辑复制/SQL 回放;不支持跨大版本异构副本(物理复制要求两侧页布局一致)。
- **MVCC 可见性读路径本期只定义接口(§10),不实现**;Follower 上的 shard 副本不服务读。
- 增强型 CLOG 的完整存储引擎设计(SLRU 化、GC)属全局 MVCC 文档范围,本文只定义
  回放模块调用的写接口与最小实现要求。

---

## 1. 术语与现状基线

- **shard**:Hash 分区的一个分片,每 shard = 一个独立 Raft 组(1 leader + N follower)。
  每个 shard 在每个节点上是一张普通堆表 `<tablename>_<shardid>`,有独立 OID / relfilenode。
- **ParWAL**:`pg_parwal/<partition_id>/` 下的段文件,即该 shard Raft 组的日志流;
  记录格式为 `PartWALRecord`(`include/partition_wal_header.h`,本版升级为 parwal-3.0)。
- **partition_lsn**:shard 内单调递增序号(1 起),= 该组 Raft log index。
- **orig_lsn**:该记录在 leader `pg_wal` 中的 **end LSN**(见 §4.2)。
- **gxid**:`GlobalTransactionId`,64 位全局事务号,`MakeGlobalXid(node_id, xid)` 构造(§9.1)。
- **xid_map**:每分区一张的 local_xid → gxid 映射表(§9.2)。
- **增强型 CLOG**:16 字节/事务的状态存储(start_ts、commit_ts、status),按 gxid 寻址(§9.3)。
- **watermark(W)**:升主水位,= 升主时刻的 max_replayed_fxid,用于切分"回放引入的 xid"
  与"本机新分配的 xid"两个命名空间(§9.4、§11)。

**仓库现有组件(leader 侧,已落地)**

| 组件 | 位置 | 作用 |
|------|------|------|
| `wal_insert_hook` | `patches/0001-add-wal-insert-hook.patch`(xloginsert.c:584 回调点) | `XLogInsert` 成功后回调:`(end_lsn, rmid, info, blocks[], record_bytes, record_len)`,record 字节为完整原始 `XLogRecord` 的连续拷贝 |
| demux worker | `src/worker/demux_worker.c` | 扫描 pg_wal,按 relfilenode 过滤记录写入 ParWAL 段 |
| ParWAL writer / queue / sync | `src/wal/` | 段文件写入、fsync、checkpoint 文件维护 |

**现状与目标的差距**:
1. demux 按**单个** relfilenode 匹配(`PartWALCheckpointFile` 只有一个 `relfilenode` 字段)
   ——只覆盖主堆,索引/TOAST 记录整类漏捕获(§5 修正)。
2. 记录头为 parwal-2.0(`PARTWAL_RECORD_VERSION_2`,32 位 xid);RM_XACT 提交/回滚记录
   以原始字节直接入流,无 TSO 时间戳(§4 升级为 parwal-3.0 标记记录)。

---

## 2. 为什么"复用 PG redo"就是"直接写数据文件"

PG 物理恢复路径(16.14):

```
GetRmgr(record->xl_rmid).rm_redo(xlogreader)              ← 按 rmid 派发
  ├─ heap_redo   / heap2_redo                              ← 表、TOAST 堆
  ├─ btree_redo                                            ← 普通索引、TOAST 索引
  └─ ...(hash/gin/gist/brin 同构)
       └─ XLogReadBufferForRedo(record, id, &buf)          xlogutils.c
            └─ XLogReadBufferExtended(rlocator, fork, blkno)
                 └─ smgropen(rlocator) → ReadBuffer / 扩展文件    ← 按物理文件号打开
```

支撑设计的四个代码事实(均已在本仓库 postgres-src 复核):

1. **redo 完全由解码后的 record 驱动,是物理块级的**。每个 block 引用携带
   `RelFileLocator{spcOid, dbOid, relNumber}` + `ForkNumber` + `BlockNumber`;
   `heap_xlog_insert` 从 record 取 tuple 字节 `PageAddItem` 进页面——不碰 SQL、不碰执行器。
2. **索引 redo 与 heap 完全同构**。`nbtxlog.c` 中 insert/split/vacuum/delete 全部走
   `XLogReadBufferForRedo`;TOAST 表本身就是普通 heap + 一个 btree 索引。
   ⇒ **表 / 索引 / TOAST 三类文件由同一条重放路径天然覆盖**,无需分别写代码。
3. **幂等靠页面 LSN**:`XLogReadBufferForRedoExtended` 开头
   `XLogRecPtr lsn = record->EndRecPtr;`(xlogutils.c:359),随后
   `if (lsn <= PageGetLSN(page)) return BLK_DONE`——已应用过的记录自动跳过;
   应用后 `PageSetLSN(page, lsn)` 盖的也是这个 EndRecPtr。这是 §4.2 orig_lsn 语义
   和 §8.4 崩溃安全的共同基础。
4. **redo 假定恢复上下文**:页面越界扩展文件时 `Assert(InRecovery)`(xlogutils.c)。
   `InRecovery` 是普通进程级全局变量,apply worker 进程置为 `true` 即可满足;
   该断言背后的真实约束是"扩展无需关系扩展锁"——每 shard 单一 apply worker 串行应用,约束成立。

**唯一需要的改写点**:record 里的 `RelFileLocator` 是 leader 的文件号,follower 上同一 shard
的文件号不同 ⇒ 在 `DecodeXLogRecord` 之后、`rm_redo` 之前,把 `decoded->blocks[i].rlocator`
改写为本地文件号(§7.4)。其余 forknum / blkno / tuple 字节 / FPI 全部原样——**前提是
follower 副本由 leader shard 的物理拷贝初始化,页布局一致**(§13 约束 2)。

---

## 3. 端到端数据流

```
 LEADER(shard S 的可写主)                        FOLLOWER(shard S 的副本)
┌─────────────────────────────────────┐         ┌──────────────────────────────────────┐
│ backend 执行 DML                     │         │                                      │
│   └─ XLogInsert(heap/btree/...)      │         │                                      │
│        └─ wal_insert_hook            │         │                                      │
│           blocks[] 反查 fileset       │         │                                      │
│           命中 shard S → 环形队列      │         │                                      │
│ PRE_COMMIT: PartWALFlush             │         │ Raft entry committed                 │
│   按 shard 分组 → PartWALRecord       │  Raft   │   └─ shard S 的 replay worker         │
│   (头携带 gxid)写段 + fsync ─────────┼────────▶│      逐条(§7 五阶段):               │
│   = Raft entry(prepare [A])         │         │      DATA   → decode→登记 xid_map     │
│ COMMIT: 标记记录(TxnMarkerPayload:  │         │               →remap→盖 orig_lsn      │
│   start_ts/commit_ts/subxids)────────┼────────▶│               →rm_redo(写页面)      │
│ ABORT:  标记记录(commit_ts=0)───────┼────────▶│      MARKER → 增强型 CLOG 置状态      │
│                                      │         │      CTRL   → fileset/loc_map 更新   │
│                                      │         │      推进 applied_part_lsn            │
└─────────────────────────────────────┘         └──────────────────────────────────────┘
```

顺序保证(§8.4 详述):Raft entry 持久化并 committed → apply → 脏页允许刷盘 → 游标推进。

---

## 4. 记录格式(parwal-3.0)

### 4.1 头部:xid → gxid

`include/partition_wal_header.h` 升级,头大小保持 40 字节(尾部 4 字节 xid + 4 字节
隐式填充,合并为 8 字节 gxid),新增 `PARTWAL_RECORD_VERSION_3`:

```c
typedef uint64 GlobalTransactionId;          /* 定义见 §9.1 / global_mvcc.h */

typedef struct PartWALRecord
{
    uint32              magic;          /* PARTWAL_MAGIC = 0x50415254 ("PART")      */
    Oid                 partition_id;   /* 所属分区 OID,与目录名交叉验证            */
    XLogRecPtr          orig_lsn;       /* leader 侧该记录的 end LSN(§4.2)         */
    uint64              partition_lsn;  /* 分区内严格单调序号(1 起)= Raft index    */
    uint8               rmid;           /* 原始 XLog resource manager ID             */
    uint8               info;           /* 原始 XLog info;标记记录见 §4.3           */
    uint8               version;        /* PARTWAL_RECORD_VERSION_3                  */
    uint8               flags;          /* PARTWAL_FLAG_*                            */
    uint32              data_len;       /* 头后载荷字节数                            */
    GlobalTransactionId gxid;           /* 顶层事务的全局事务号(§9.1)              */
} PartWALRecord;                        /* sizeof = 40,与 2.0 相同                  */

#define PARTWAL_FLAG_DATA    UINT8_C(0x01)   /* 数据修改记录(载荷 = 原始 XLogRecord)*/
#define PARTWAL_FLAG_MARKER  UINT8_C(0x02)   /* 事务标记记录(载荷 = TxnMarkerPayload)*/
#define PARTWAL_FLAG_CTRL    UINT8_C(0x04)   /* 控制记录(FILESET_UPDATE 等,§12)    */
```

**记录分类一律以 flags 判定,不以 data_len 判定**(标记记录载荷非空,见 §4.3)。

### 4.2 orig_lsn 的精确语义:end LSN

redo 对页面盖的 LSN 与幂等比较用的 LSN 都是 `record->EndRecPtr`(§2 事实 3)。
leader 侧页面被盖上的正是该记录的 end LSN;follower 要做到"页面含 LSN 域字节级一致",
`orig_lsn` 必须取同一个值。patch 0001 的 hook 回调第一个参数就是 `XLogInsert` 返回的
`EndPos`(xloginsert.c:584),现有捕获链路已满足——本节只是把这一语义**固定为格式契约**,
禁止任何一侧改存起始 LSN。

### 4.3 事务标记记录(MARKER)

由 leader 的提交/中止路径生成(不再让 demux 透传原始 RM_XACT 记录):

```c
typedef struct TxnMarkerPayload
{
    uint64  start_ts;     /* 事务启动时间戳,StartTransaction() 时向 TSO 申请      */
    uint64  commit_ts;    /* 提交时间戳,CommitTransaction() 时向 TSO 申请;
                             ABORT 标记中为 0                                      */
    uint32  nsubxacts;    /* 已提交子事务数;无 SAVEPOINT 时为 0                   */
    uint32  reserved;     /* 显式补齐,恒为 0                                      */
    /* TransactionId subxacts[nsubxacts] 紧随其后(leader 侧本地 xid) */
} TxnMarkerPayload;       /* sizeof = 24;data_len = 24 + 4 * nsubxacts */
```

- `flags` 含 `PARTWAL_FLAG_MARKER`;`rmid = RM_XACT_ID`;
  `info = XLOG_XACT_COMMIT` 或 `XLOG_XACT_ABORT`(取 `info & XLOG_XACT_OPMASK` 判别)。
- `gxid`(头部)= 顶层事务的全局事务号;子事务的 gxid 由 follower 按
  `MakeGlobalXid(leader_node_id, subxid)` 合成(§7.6)。
- 与 reply_v1 的 16 字节定长相比多出 `nsubxacts` 与子事务数组:这是为覆盖
  SAVEPOINT 语义的**有意格式增量**(§9.5),需回同步到评审文档。
- `reserved` 是**显式**补齐字段,不是编译器填充。结构体整体直写磁盘,留一个
  未初始化的 4 字节洞会让同一逻辑内容产生不同字节 —— parwal-2.0 的 `xid`
  尾部填充就踩过这个坑(见 `PartWALRecordGxid` 的兼容读注释)。因此
  `sizeof(TxnMarkerPayload)` 是 **24 而非 20**,`global_mvcc.c` 用
  `StaticAssertDecl` 把 24 与 `nsubxacts` 的偏移 16 钉在编译期。

**写入时机与顺序(实现约束,R2-b)**

COMMIT 标记在**本事务 DATA 记录复制成功之后**才落盘,由此得到一条强不变式:

> 段流里出现某个 gxid 的 COMMIT 标记 ⇒ 该事务的 DATA 记录已在多数派上持久化。

顺序若反过来(先写标记再复制),复制挂钩一旦 ERROR(凑不齐多数派、本节点
已不是该组 leader),事务中止时就要再补一条 ABORT 标记 —— 同一个 gxid 先
COMMIT 后 ABORT,回放侧只要停在两者之间就会把一个已回滚的事务判成可见。

- ABORT 标记(`commit_ts = 0`)在 `XACT_EVENT_ABORT` 补写,条件是本事务的字节
  **已经落进段文件**:group commit 下被 peer backend 顺手写掉,或本事务过了
  PRE_COMMIT 之后才失败。此时丢弃 ring buffer 槽位已无意义,follower 迟早
  拿到这批 DATA 记录,必须给它们一个终态。abort 路径不调用复制挂钩(中止中
  再 ERROR 会升级为 FATAL),标记由该分区下一次写入的区间式复制带走。
- **2PC 尚未覆盖**:`XACT_EVENT_PRE_PREPARE` 不写 COMMIT 标记 —— prepared
  事务还可能 `ROLLBACK PREPARED`,此刻写 COMMITTED 会让最终回滚的数据在
  follower 上变可见。代价是 2PC 事务在 follower 上保持"未决 = 不可见",
  语义安全。补齐 PREPARE / COMMIT PREPARED 两段式标记是后续工作。

### 4.4 兼容性

- 回放模块按 `version` 分派:`VERSION_2` 段流仅支持 R1 阶段的纯物理回放
  (RM_XACT 原始记录按附录 A 跳过,无事务层登记);`VERSION_3` 为完整语义。
- 写入侧升级为 3.0 后,存量 2.0 段不做原地转换——该副本重新做一次物理基线 + 从新流追平。

---

## 5. Leader 侧捕获:ShardFileSet 与反向映射

### 5.1 问题

索引记录携带的是**索引文件自己的 relfilenode**,TOAST 记录携带的是 toast 关系的
relfilenode。按"主堆单一 relfilenode"过滤(现状)会把这两类记录整类丢弃,
follower 的索引与 TOAST 文件永远不更新——直接违背核心目标。

### 5.2 设计:以"shard 的物理文件集合"为捕获与映射单位

```c
/* include/shard_fileset.h —— leader/follower 共用 */
typedef struct ShardFileSet
{
    Oid            shard_oid;                   /* = partition_id */
    int            nrels;
    RelFileLocator rels[FLEXIBLE_ARRAY_MEMBER]; /* [0]=主堆, 后接全部索引、
                                                   toast 堆、toast 索引 */
} ShardFileSet;
```

- **构建**:shard 建表 / 建副本 / DDL 时,级联查询 `pg_class`(主堆)→ `pg_index`
  (全部索引)→ `pg_class.reltoastrelid`(TOAST 堆及其索引),收齐全部 relfilenode。
- **leader 侧反向映射**:共享内存哈希 `relfilenode → shard_oid`,由 fileset 展开而来;
  DDL 时同步刷新(§12)。
- **捕获规则(块引用路径)**:`wal_insert_hook` 收到的 `blocks[]` 中**任一** block 的
  `rlocator.relNumber` 命中反向映射 ⇒ 整条 record 归入该 shard 的 ParWAL 流。
  heap/btree 记录的所有 block 必属同一关系,不会跨 shard;实现中加断言,
  violation 即 ERROR(说明 fileset 漏登记)。
- **★ 捕获规则(无块引用路径,必须特判)**:部分记录**不注册任何 buffer**,
  `blocks[]` 为空,上一条判据对它们恒不命中。已核实(PG 16.14 `storage.c`
  `RelationTruncate`,`XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE` 分支):

  ```c
  XLogBeginInsert();
  XLogRegisterData((char *) &xlrec, sizeof(xlrec));   /* ← 只有 data,无 XLogRegisterBuffer */
  lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_TRUNCATE | XLR_SPECIAL_REL_UPDATE);
  ```

  `XLOG_SMGR_CREATE` 同构。二者的 `RelFileLocator` 都在 **main data** 里
  (`xl_smgr_truncate.rlocator` / `xl_smgr_create.rlocator`)。因此捕获侧必须对
  `rmid == RM_SMGR_ID` 单独取 main data 首字段查反向映射,与附录 A 中 follower
  侧的 main-data 重映射特判**成对存在**——只做 follower 侧那一半,记录根本进不了流。

  > 漏掉的后果是**静默的**:leader 的 VACUUM 截断尾部空页不会在 follower 发生,
  > 副本文件长于 leader。运行期不 PANIC,只有逐页 diff 能发现;而纯 pgbench 负载
  > 未必触发截断 ⇒ R1 验收用例必须显式制造一次 VACUUM 截断(§14.2 R1 验收)。
- **FSM / VM**:与主关系同 relNumber、不同 fork,hook 的 block 引用带 forkno,
  天然命中,无需额外登记。
- **gxid 填充**:hook 上下文处于事务内,顶层 gxid 在 StartTransaction 时已构造并缓存于
  backend 局部变量,填入 `PartWALRecord.gxid` 为 O(1) 操作,无目录查询。

### 5.3 必须捕获的记录范围

物理回放要求**完整物理子流**——凡是修改这些文件字节的记录一条都不能漏,
否则页 LSN 链断裂,`heap_xlog_*` 内 `PageAddItem` 会 PANIC。按 rmgr:

| rmgr | 记录 | 说明 |
|------|------|------|
| RM_HEAP | insert / delete / update / hot_update / lock / inplace | 表与 TOAST 堆的 DML |
| RM_HEAP2 | multi_insert / prune / vacuum / freeze_page / visible | COPY、HOT 清理、vacuum、VM 位 |
| RM_BTREE | insert / split / vacuum / delete / dedup / newroot ... | 普通索引与 TOAST 索引 |
| RM_HASH/GIN/GIST/SPGIST/BRIN | (如 shard 用到) | 同构,按需纳入 |
| RM_XLOG | FPI / FPI_FOR_HINT | checksum/hint 触发的全页镜像 |
| RM_SMGR | truncate / create | VACUUM 截断尾部空页;**无块引用,捕获侧与重映射侧均须 main-data 特判**(§5.2、附录 A) |
| RM_XACT | commit / abort | **不进 DATA 流**,由提交路径转为 §4.3 标记记录 |

注意:捕获判据是 **blocks[] 命中 fileset**,不做 DML 白名单过滤——这自动涵盖上表全部
**块级**记录,包括 vacuum/prune/FPI(它们可能由 SELECT 触发的 HOT 剪枝、autovacuum、
checksum hint 写盘产生,执行器层打标签覆盖不到,这正是反向映射表存在的理由)。
**唯一的例外是无块引用的 RM_SMGR 记录**,走 §5.2 的 main-data 特判分支;
除此之外不得再增加特判——新增特判即意味着捕获判据有洞,应在 §5.2 补全而非分散处理。

---

## 6. Raft 复制与提交语义(沿用现有设计)

- PRE_COMMIT 阶段 `PartWALFlush`:按 shard 分组,赋 `partition_lsn`(= Raft log index),
  写段 + fsync,作为 Raft entry 发起复制(2PC prepare `[A]`)。
- 事务 COMMIT / ABORT:向涉及的每个 shard 的流追加标记记录,同样走 Raft。
- **Follower 只应用已 committed 的 entry**;`partition_lsn` 必须严格连续(+1),
  出现空洞即 PANIC(Raft 层保证不应发生)。
- **回放模块与 Raft 解耦**:回放循环通过注入的边界回调取"可回放上界":

```c
/* include/shard_replay.h */
typedef uint64 (*ShardReplayBoundFn)(Oid shard_oid);   /* 返回已 committed 的最大 partition_lsn */
```

  生产环境由 pg_raft 桥接提供(commit_index);单元/单机测试模式提供
  "段内全部记录可回放"的默认实现(GUC `pg_partdist.replay_trust_local_segments = on`),
  使回放模块可以脱离 Raft 独立开发验收。

---

## 7. Follower 回放管线(核心,对应 reply_v1 §3.3 五阶段)

回放的**并发单位是 shard**:每个 shard 的字节流由唯一一个 replay worker 串行消费
(bgworker;文件号均来自 loc_map,不需要 relcache)。

> **★ worker 进程初始化三要素(R1 实测,缺一即段错误;v3 稿"纯 SHMEM_ACCESS
> 无 DB 连接"的说法不成立)**:
> 1. `BGWORKER_BACKEND_DATABASE_CONNECTION` + `BackgroundWorkerInitializeConnection(NULL)`
>    ——不是为了 catalog(dbname=NULL 不连任何库),而是为了走完 BaseInit:
>    `InitBufferPoolAccess`/pgstat/fd.c 都在这里初始化;缺了它第一次 ReadBuffer
>    直接段错误,且 shmem worker 崩溃会把**整个节点**拖进 crash recovery;
> 2. `CreateAuxProcessResourceOwner()`——回放全程无事务,`CurrentResourceOwner`
>    为 NULL 时 buffer pin 记账(`ResourceOwnerEnlargeBuffers`)段错误;与 startup
>    进程同款做法,退出路径自动释放残留 pin;
> 3. `RmgrStartup()`(镜像 StartupXLOG)——btree/gin/gist 的 redo 入口第一件事
>    是 `MemoryContextSwitchTo(opCtx)`,`opCtx` 由各自的 `rm_startup` 创建;
>    不调它,第一条 NEWROOT 记录就把 `CurrentMemoryContext` 切成 NULL,下一个
>    palloc 即崩。heap 记录没有私有上下文——**"heap 能回放"不证明初始化完整**。

> **★ 不是"每 shard 常驻一个 bgworker"**。`max_worker_processes` 默认 8(本项目
> 测试环境实测即为 8),而每节点承载的 shard 数是几十到上百量级(Citus 默认
> `shard_count=32`,再叠加本节点作为其他分片副本持有的组)——per-shard 常驻必然撞上限。
> 落地形态是 **worker 池**:池大小由 GUC `pg_partdist.replay_workers` 控制
> (默认 4,上限受 `max_worker_processes` 约束),每个 worker 轮转认领一批 shard,
> **同一 shard 在任一时刻只被一个 worker 持有**(见 §13.10 的排他不变式)。
> 这与 pg_raft 侧 `RAFT_MAX_GROUPS=32` 是同一形状的问题(每节点分区数远超静态槽位),
> 两处应统一为"池 + 游标复用",不要各自扩大静态数组。

**前置条件**(静态不变式):
1. *基线一致性*:本地数据文件由 leader shard 物理基线拷贝初始化,页布局逐块对齐;
   loc_map 已建立并反映最新文件对应关系(基线拷贝时生成,DDL 经控制记录更新,§12)。
2. *上下文隔离*:回放期间 worker 对该分区全部数据文件独占访问,分区不对外提供读写。

### 7.1 回放上下文与文件号映射

```c
/* include/shard_replay.h */
typedef struct LocMapEntry
{
    RelFileLocator  leader_loc;      /* hash key:record 里携带的就是它 */
    RelFileLocator  local_loc;
} LocMapEntry;

typedef struct ShardReplayCtx
{
    Oid                shard_oid;
    HTAB              *loc_map;            /* 文件号映射:leader fileset ↔ 本地 fileset
                                              按"关系角色 + 索引定义序"逐一配对 */
    HTAB              *xid_map;            /* local_xid → gxid(§9.2;R1 阶段可为 NULL) */
    XLogReaderState   *reader;             /* 仅作 DecodeXLogRecord 容器,读回调全 NULL
                                              (XLogReaderAllocate 分配,自带 errormsg 缓冲) */
    uint64             applied_part_lsn;   /* 已应用游标(内存值) */
    uint64             durable_part_lsn;   /* 已持久化游标(checkpoint 落盘值) */
    XLogRecPtr         max_orig_lsn;       /* 已应用的最大 leader end LSN(升主收尾用) */
    FullTransactionId  max_replayed_fxid;  /* 已回放的最大 xid(64 位,升主水位 W 来源)
                                              由 xl_xid 按本地当前 epoch 折算(§7.5) */
    TimestampTz        last_ckpt_time;     /* 上次 apply checkpoint 时刻 */
} ShardReplayCtx;
```

> 命名对照:reply_v1 中的 `max_replayed_xid`(32 位)在实现中直接采用 64 位
> `max_replayed_fxid`——32 位值跨 epoch 回卷后 "xid ≤ W" 比较失效,且
> `ShardRouteEntry.watermark` 本就是 `FullTransactionId`,两处类型必须一致。

### 7.2 阶段一:初始化与边界确定

1. 读 `pg_parwal/<shard>/apply_checkpoint`(§8.4):得 `durable_part_lsn`
   (回放起点 = 该值 + 1)、`max_orig_lsn`、`max_replayed_fxid`,并加载同文件内的
   xid_map 快照重建哈希表。文件不存在 = 全新副本,游标从 0 起、xid_map 为空。
2. `PartDistAdvanceNextXidPastXid(max_replayed_fxid)`(§7.5)——**worker 启动即执行**:
   本地 nextXid 只在 PG 自身 checkpoint 时随 pg_control 持久化,崩溃可能回退,
   必须用我们自己持久化的水位重新拉齐,否则升主前的水位保证失效。
3. 初始化 `ShardReplayCtx`;`InRecovery = true`(进程级全局变量,满足 redo 扩展文件的
   `Assert(InRecovery)`,worker 生命周期内保持;合法性:worker 是该 shard 文件的唯一
   写入者,与 startup 进程独占恢复的前提等价)。
4. 取回放上界 = `ShardReplayBoundFn(shard_oid)`(§6);位于未提交位点之后的记录视为
   无效,不读取(段尾截断即天然恢复点)。

### 7.3 阶段二:逐条读取与校验

顺序读段文件,逐条解析 `PartWALRecord`:

1. *完整性校验*:`magic == 0x50415254`;`partition_id` 与目录名一致;`version` 支持;
   记录体完整——体不完整则停在边界等待下一批(或 FATAL,视 bound 是否已越过)。
2. *幂等与连续性*:`partition_lsn <= applied_part_lsn` → 重复投递,跳过;
   `partition_lsn != applied_part_lsn + 1` → 日志空洞,PANIC。

### 7.4 阶段三 A:应用 DATA 记录(decode → 登记 → remap → 盖 LSN → rm_redo)

```c
static void
ShardReplayDataRecord(ShardReplayCtx *ctx, PartWALRecord *h, char *body)
{
    XLogRecord        *record = (XLogRecord *) body;
    DecodedXLogRecord *decoded;
    char              *errormsg;

    /* 与场景无关的记录直接跳过(附录 A 白名单) */
    if (ShardReplaySkippable(h->rmid, record->xl_info))
        return;

    /* 1) 解码原始字节;lsn 实参即 EndRecPtr 来源,直接用 leader 的 orig_lsn(end LSN) */
    decoded = palloc(DecodeXLogRecordRequiredSpace(record->xl_tot_len));
    if (!DecodeXLogRecord(ctx->reader, decoded, record, h->orig_lsn, &errormsg))
        ereport(PANIC, (errmsg("parwal: shard %u 解码失败 @plsn %lu: %s",
                               ctx->shard_oid, h->partition_lsn, errormsg)));

    /* 2) 登记全局事务映射 + 推进水位(仅当记录携带有效 xid;
     *    VACUUM/HOT 剪枝/FPI_FOR_HINT 等无事务号记录跳过本步) */
    if (TransactionIdIsValid(record->xl_xid))
    {
        ShardXidMapInsert(ctx, record->xl_xid,
                          MakeGlobalXid(GxidNodeId(h->gxid), record->xl_xid));
        ShardReplayNoteXid(ctx, record->xl_xid);      /* 更新 max_replayed_fxid(§7.5) */
    }

    /* 3) 文件号重映射:leader → 本地(唯一改写点;SMGR 记录另有 main-data 特判,附录 A) */
    for (int id = 0; id <= decoded->max_block_id; id++)
    {
        DecodedBkpBlock *blk = &decoded->blocks[id];
        LocMapEntry     *e;
        bool             found;

        if (!blk->in_use)
            continue;
        e = hash_search(ctx->loc_map, &blk->rlocator, HASH_FIND, &found);
        if (!found)
            ereport(PANIC, (errmsg("parwal: shard %u 未知 relfilelocator %u/%u/%u"
                                   "(fileset 漏登记:索引或 TOAST?)", ctx->shard_oid,
                                   blk->rlocator.spcOid, blk->rlocator.dbOid,
                                   blk->rlocator.relNumber)));
        blk->rlocator = e->local_loc;   /* forknum / blkno / 数据 / FPI 原样 */
    }

    /* 4) 挂上 reader:EndRecPtr = orig_lsn ⇒ redo 内 PageSetLSN 盖 leader 的 end LSN,
     *    页面(含 LSN 域)与 leader 字节级一致;重放已应用记录时 lsn <= PageGetLSN
     *    → BLK_DONE 跳过,页级幂等(§8) */
    ctx->reader->record     = decoded;
    ctx->reader->ReadRecPtr = h->orig_lsn;      /* 仅错误报文用,无正确性依赖 */
    ctx->reader->EndRecPtr  = h->orig_lsn;

    /* 5) ★ 派发原生 redo:heap_redo→表/TOAST 堆页面,btree_redo→索引页面,
     *    内部 XLogReadBufferForRedo → smgropen(本地文件) → 按块号改页面字节 */
    GetRmgr(h->rmid).rm_redo(ctx->reader);

    pfree(decoded);       /* 注意:只释放 decoded;body 的生命周期见下方 ★ */
}
```

> **★ 缓冲区生命周期(实现复核后修正)**:PG16.14 的 `DecodeXLogRecord` 是
> **深拷贝**——FPI 镜像、block data、main data 全部 memcpy 进 `decoded` 尾部的
> 连续空间(`xlogreader.c`:"Copy the data of each fragment to contiguous
> space"),`decoded` 完全自包含。因此 `body` 缓冲在 decode 返回后即可复用,
> 主循环用单个复用缓冲区逐条读取是安全的。
>
> 真正的陷阱在**取原始字节**的一侧(leader 捕获/回读路径):`XLogReadRecord`
> 之后 `readRecordBuf` 仅对**跨页**记录持有装配好的原始字节;单页记录的
> record 指针直指页读缓冲,`readRecordBuf` 里是陈旧内容。凡需要"原始连续
> 记录字节"(写 parwal、group-commit peer 槽位回读)必须按页自行装配并以
> `xl_crc` 校验(实现见 `partwal_sync.c` 的 `AssembleRawWALRecord`)。

注:`xl_xid` 与头部 `gxid` 内嵌的 xid **不一定相同**——子事务的记录 `xl_xid` 是
subxid,而头部 gxid 是顶层事务的。因此第 2 步以 `xl_xid` 为键、以
`MakeGlobalXid(来源节点, xl_xid)` 为值登记(每个 xid 独立 gxid),提交树语义由
标记记录闭合(§7.6)。同一 local_xid 若已存在映射且 gxid 不一致 → XID 命名空间
冲突或数据损坏,PANIC。

### 7.5 事务号水位推进:PartDistAdvanceNextXidPastXid

**目的**:本地后续分配的 xid 必须严格大于所有回放引入的 xid,否则升主后
"xid ≤ W → 查 xid_map"的二分判定失效(新事务会被误路由到旧 leader 的命名空间)。

**不能直接用核内函数**:varsup.c 的 `AdvanceNextFullTransactionIdPastXid()` 带
`Assert(AmStartupProcess() || !IsUnderPostmaster)`(无锁读 nextXid 只对 startup
进程安全,已核实)。replay worker 是普通 bgworker,assert 构建下直接崩溃。

**扩展自带变体**(`src/replay/replay_worker.c`):

```c
void
PartDistAdvanceNextXidPastXid(TransactionId xid)
{
    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
    /* 复制 varsup.c 的 epoch 推断逻辑,但读-改-写全程持锁:
     * next = XidFromFullTransactionId(ShmemVariableCache->nextXid);
     * 若 xid 未达 next → 直接返回;否则 TransactionIdAdvance(xid),
     * 保持现有 epoch、检测 32 位回绕(+1 epoch),写回 nextXid。 */
    LWLockRelease(XidGenLock);
}
```

**调用时机**:逐条记录只更新 `ctx->max_replayed_fxid`(`ShardReplayNoteXid`,
按本地当前 epoch 折算为 64 位);批次结束、apply checkpoint 前、升主收尾时,
调用上述函数一次性拉齐 nextXid。worker 启动时以持久化的 `max_replayed_fxid`
先行拉齐(§7.2 第 2 步)。

**登记要在 skip 判断之前**(实现约束,R2-c):附录 A 白名单里那些记录不改页面,
但它们的 xid 一样是 leader 已经分配掉的。水位漏掉它们,本地就可能重新分配到
同一个号,升主后 "xid ≤ W → 查 xid_map" 的判定随即错位。

**只对 `version >= 3` 的流登记**:2.0 记录的 gxid 是补 0 节点号的兼容值(§4.4),
登进 xid_map 会把"协调节点(group id 恰好是 0)的事务"和"来源未知"混为一谈。

**epoch 取本地口径**:leader 的 epoch 不在段流里,而这个水位存在的意义就是和
本地 nextXid 比大小,两边口径必须一致。`ShardReplayInitXidMap` 在建表时持
`XidGenLock` 读一次本地 epoch 作为起点。

### 7.6 阶段三 B:应用 MARKER 记录(增强型 CLOG)

```c
static void
ShardReplayMarkerRecord(ShardReplayCtx *ctx, PartWALRecord *h, TxnMarkerPayload *m)
{
    uint16          origin = GxidNodeId(h->gxid);
    TransactionId  *subxids = (TransactionId *) ((char *) m + sizeof(TxnMarkerPayload));

    if (h->data_len != sizeof(TxnMarkerPayload) + m->nsubxacts * sizeof(TransactionId))
        ereport(PANIC, (errmsg("parwal: marker 载荷长度不符")));

    if ((h->info & XLOG_XACT_OPMASK) == XLOG_XACT_COMMIT)
    {
        /* 提交树:顶层 + 全部已提交子事务,等价 TransactionIdCommitTree 的全局版 */
        EnhancedClogWriteStatus(h->gxid, m->start_ts, m->commit_ts, TXN_COMMITTED);
        for (uint32 i = 0; i < m->nsubxacts; i++)
            EnhancedClogWriteStatus(MakeGlobalXid(origin, subxids[i]),
                                    m->start_ts, m->commit_ts, TXN_COMMITTED);
    }
    else
        EnhancedClogWriteStatus(h->gxid, m->start_ts, 0, TXN_ABORTED);

    ShardReplayNoteXid(ctx, GxidLocalXid(h->gxid));
}
```

- **原生 clog 不写**:回放引入的 xid 在本地原生 clog 中是空洞(nextXid 被推进但
  `ExtendCLOG` 从未为这些区间建页)。任何原生可见性例程都不得触碰这些 xid——由
  §13 约束 5(副本表屏蔽 autovacuum、不服务读)保证,升主后由路由表接管(§9.4)。
- 中止的子事务不在 COMMIT 标记的列表中 → 其 gxid 在增强型 CLOG 中无 COMMITTED
  记录 → 天然不可见,SAVEPOINT 回滚语义正确。
- 元组可见的最终条件(读路径,本期不实现):状态 = COMMITTED **且**
  `commit_ts ≤ 快照 start_ts`(§10)。

### 7.7 阶段三 C:应用 CTRL 记录(D1 已实装)

头部约定:`flags` 含 `PARTWAL_FLAG_CTRL`;`rmid = PARTWAL_CTRL_RMID`(0xFF 哨兵值,
**不是**真实 rmgr id——CTRL 压根不进 `rm_redo`,派发判据是 flags 的类别位;
设成不可能被当 rmgr 用的值,是为了万一有人误按 rmid 分派时立刻炸掉,
而不是安静地走进某个 rmgr 的 redo);`info = opcode`。

目前仅 `FILESET_UPDATE`(§12):按新 fileset 全量描述重建 loc_map,处理完毕才继续
后续 DATA 记录——控制记录之后的 DATA 才会引用新 relfilenode,apply 串行 ⇒ 无竞态。

**未知 opcode 一律 ERROR**,与 §7.3 未知 rmid 同款 fail-fast:控制记录改变的是
"后续记录怎么解释",静默跳过一条没读懂的控制记录,等于在错误的映射上继续 redo。

### 7.8 阶段四:游标推进与持久化

1. 每条记录处理完毕:`applied_part_lsn = partition_lsn`;
   `max_orig_lsn = Max(max_orig_lsn, orig_lsn)`。
2. 周期性(时间或字节阈值)执行 apply checkpoint(§8.4):
   刷 shard 脏页 → fsync 增强型 CLOG 增量 → 原子写 checkpoint 文件
   (游标 + max_orig_lsn + max_replayed_fxid + xid_map 快照)。
   `durable_part_lsn` 仅在此时推进。

### 7.9 阶段五:回放收尾(升主场景)

见 §11(依赖补丁 0003 与路由表,R4 阶段实现)。

---

## 8. 页 LSN、刷脏与崩溃一致性

### 8.1 为什么不能"重新落一遍本地 WAL"

- `XLogInsert` 只能从 registered buffers/data **重新构造**记录,没有"原样插入一条
  现成 XLogRecord"的接口;伪造 registration 等于重写 xloginsert,得不偿失。
- Raft 复制的 ParWAL 本身已是持久化日志(entry 先 commit 才 apply),
  follower 再生成一份内容不同的本地 WAL 属于双日志冗余,还会再次触发
  `wal_insert_hook` 造成回环,需额外抑制。

### 8.2 页 LSN 直接使用 leader 的 orig_lsn(end LSN)

§7.4 中 `EndRecPtr = orig_lsn`,redo 内 `PageSetLSN` 盖的即 leader 侧同一页面
将持有的 LSN。收益:

- **页面与 leader 字节级一致**(含 LSN 域)——副本可交叉校验、可直接作新副本的种子。
- **页级幂等**:重放已应用记录时 `lsn <= PageGetLSN` → `BLK_DONE` 跳过,
  且 LSN 确定(不随重放次数变化),崩溃后从游标重放安全(§8.4)。
  记录级幂等(§7.3 游标去重)与页级幂等互补,缺一不可:游标定位"从哪条重放",
  页 LSN 保证"重放到已应用页面无副作用"。

### 8.3 代价:必须绕开本地 WAL 的刷脏顺序检查(内核补丁 0002)

`FlushBuffer` 刷脏页前执行 `XLogFlush(页LSN)` 以保证"WAL 先于数据"。
follower 节点整体是正常主库身份(`XLogInsertAllowed()` 为真),不会走 standby 的
`UpdateMinRecoveryPoint` 分支,于是拿 leader 的 LSN 去 flush 本地 pg_wal——
本地没有这个位置,直接 `ERROR: xlog flush request %X/%X is not satisfied`。

**补丁 0002**:`FlushBuffer` 中加钩子:

```c
/* bufmgr.c */
extern bool (*buffer_flush_lsn_exempt_hook)(const RelFileLocator *rlocator);
/* XLogFlush(recptr) 前:命中钩子且返回 true → 跳过 XLogFlush */
```

扩展侧实现:查共享内存"副本文件集合"哈希(路由表的前身/子集,由 replay launcher
维护)。语义上正确而非 hack:"WAL 先于数据"要求**保护该页的日志**先于页落盘;
这些页的日志是 ParWAL/Raft log,apply 发生在 entry 持久化并 committed **之后**,
先行条件在写页之前已满足。

> 注意豁免的生命周期:升主后,旧页面在被本地 WAL 重新保护(首次修改产生 FPI)之前
> 仍携带 leader 坐标 LSN。补丁 0003 把本地插入位点推过 `max_orig_lsn`(§11)后,
> `XLogFlush(页LSN)` 自然可满足,豁免可随角色切换收敛为仅 FOLLOWER 角色生效。

### 8.4 应用游标与崩溃恢复

`pg_parwal/<shard>/apply_checkpoint`(与 leader 侧 demux checkpoint 分开),
**单文件原子落盘**(tmp + fsync + rename + 目录 fsync),游标与 xid_map 快照
天然一致,无双文件交叠窗口:

```c
typedef struct ShardApplyCheckpoint
{
    uint32            magic;              /* PARTWAL_CHECKPOINT_MAGIC "CHKP"        */
    uint32            version;            /* 3                                       */
    Oid               shard_oid;
    uint64            durable_part_lsn;   /* 此游标前的记录效果已全部持久化          */
    XLogRecPtr        max_orig_lsn;       /* 已应用的最大 leader end LSN(§11 用)   */
    FullTransactionId max_replayed_fxid;  /* 升主水位 W 的持久化来源(§7.2/§11)     */
    uint64            resume_segno;       /* 恢复扫描起点提示(仅优化,需校验)      */
    uint64            resume_offset;
    uint32            nxidmap;            /* 随后的 xid_map 快照条目数               */
    uint32            crc;                /* 头 + 快照全体的 CRC32C                  */
    /* XidMapEntry xidmap[nxidmap] 紧随其后 */
} ShardApplyCheckpoint;
```

**CRC 必须覆盖快照本体**(实现约束,R2-c):只校验头的话,快照被截断或写坏时
头依旧自洽,重启后 xid_map 静默残缺——页面上有元组、却查不到这些 xid 属于谁,
可见性判定就此错位,而且没有任何报错。

版本号仍是 **3**:`nxidmap == 0` 时"头 + 空快照"的 CRC 与旧的"只算头"逐字节
等价,因此 R1 时代写下的 checkpoint 继续通过校验——升级到 R2 不会把已有
follower 打回从 plsn 1 重放(那恰好是最危险的路径,见 §8.5)。

`XidMapEntry` 带一个显式的 `reserved` 补齐字段(`sizeof == 16`,`gxid` 在偏移 8,
两条 `StaticAssertDecl` 钉死):条目原样落盘并计入 CRC,留未初始化的填充洞会让
同一份逻辑内容每次写出不同字节,副本之间没法按字节比对 checkpoint——与
`TxnMarkerPayload.reserved`(§4.3)同源的教训。

**推进协议**(类比 checkpoint 的 REDO 点):

1. apply 到 `partition_lsn = N`;
2. 把 shard 副本文件的全部脏页刷盘(`FlushRelationsAllBuffers`/按 fileset 逐文件
   `FlushOneBuffer` + `smgrimmedsync`),fsync 增强型 CLOG 中本批标记涉及的页;
3. 之后才允许写出 `durable_part_lsn = N` 的 checkpoint 文件(含 xid_map 快照)。

**崩溃恢复**:worker 重启 → 读 checkpoint → 重建 xid_map、拉齐 nextXid → 从 N+1 重放。
区间 (N, applied) 内:已刷盘的页凭 `lsn <= PageGetLSN` 跳过;未刷盘的页从旧内容重放;
增强型 CLOG 的丢失更新由标记记录重放幂等重写——与 PG 自身崩溃恢复同模型,正确性同源。

**xid_map 增长控制**:条目在对应 xid 全部冻结(回放到 leader 的 freeze 记录,
tuple 置 `HEAP_XMIN_FROZEN`)后不再被可见性查询需要,可在 checkpoint 时按
"已冻结上界"截断快照;精细 GC 属全局 MVCC 文档范围,本期只留 `ShardXidMapTruncate`
接口位。

### 8.5 撕裂页(torn page)

follower 的刷盘节奏与 leader 的 FPI 节奏(leader checkpoint 周期)不同步,
不能靠流内 FPI 治愈 follower 本地的撕裂写。对策(按落地顺序):

1. **R1/R2 阶段**:依赖页 checksum 检测撕裂,检测到即废弃本副本、从 leader 重新
   物理拷贝该 shard(简单可靠,fail-safe)。
2. **R4 前落地**:驱逐前页镜像 sidecar——脏页首次刷盘前把整页镜像追加写入
   `pg_parwal/<shard>/dw/` 双写文件并 fsync;恢复时先用 sidecar 修复半写页再从游标
   重放。仅 shard 副本文件付此代价,checkpoint 后可截断。
3. 存储原子写 8KB 的部署可关闭 sidecar(GUC,不作为默认假设)。

---

## 9. 全局事务标识与可见性框架(定稿方案,替代 v2 §9 方案 A)

> 方案 A(集群 xid 区间批发)已弃用:它要求 patch 本地 xid 分配路径并引入区间租约
> 服务;定稿方案不改 tuple 字节、不动 `GetNewTransactionId`,以"文件 → 分区 → 命名
> 空间"的路由消除歧义,代价是每分区一张 xid_map 与一张节点级路由表(存储量化:
> 路由表 ≈ shards × 4~6 文件 × 64B,一万分片约 3~4MB;xid_map 条目 ~48B,
> 上界受冻结 GC 约束)。

### 9.1 gxid 编码

```c
/* include/global_mvcc.h */
typedef uint64 GlobalTransactionId;

#define GXID_NODE_BITS   16
#define MakeGlobalXid(node_id, xid) \
    (((uint64) (node_id) << 48) | (uint64) (xid))
#define GxidNodeId(gxid)    ((uint16) ((gxid) >> 48))
#define GxidLocalXid(gxid)  ((TransactionId) ((gxid) & 0xFFFFFFFF))
#define InvalidGlobalXid    UINT64_C(0)
```

高 16 位为节点号,低 48 位当前放 32 位本地 xid(预留至 48 位,便于未来携带
FullTransactionId 低位)。示例:`MakeGlobalXid(1, 100) = 0x0001000000000064`。

### 9.2 xid_map(每分区一张)

```c
/* include/shard_xidmap.h */
typedef struct XidMapEntry
{
    TransactionId       local_xid;   /* 键:XLogRecord.xl_xid(leader 分配的 32 位) */
    GlobalTransactionId gxid;        /* 值:MakeGlobalXid(来源节点, local_xid)       */
} XidMapEntry;
```

- **作用域是分区而非节点**:一个节点承载多个副本组,各 leader 的 xid 空间独立且可能
  重叠;`local_xid → gxid` 的唯一性只在单分区内成立。分区归属由持有该表的
  `ShardReplayCtx`(回放期)/`ShardRouteEntry`(查询期)表达,条目内无需分区字段。
- **持久化**:随 apply_checkpoint 快照落盘(§8.4)——它服务查询期可见性,生命周期
  远超单次回放;仅驻内存则崩溃后"页面上有元组、映射已丢失",可见性无从判定。
- **存储形态**:R1/R2 为 worker 私有 HTAB + 快照文件;R3(读路径实装)迁移为
  shmem 内 in-place DSA + dshash(PG16 无 DSM registry,须在 shmem_startup 阶段
  `dsa_create_in_place` 预建),dshash 句柄登记于路由表条目,任意 backend 可查。

### 9.3 增强型 CLOG(回放模块只用写接口)

```c
/* include/enhanced_clog.h */
typedef enum
{
    TXN_RUNNING   = 0,
    TXN_PREPARED  = 1,
    TXN_COMMITTED = 2,
    TXN_ABORTED   = 3
} TxnStatus;

typedef struct EnhancedClogRecord      /* 16 字节定长槽 */
{
    uint8   start_ts[6];
    uint8   commit_ts[6];
    uint32  status;
} EnhancedClogRecord;

extern void EnhancedClogWriteStatus(GlobalTransactionId gxid,
                                    uint64 start_ts, uint64 commit_ts,
                                    TxnStatus status);
extern bool EnhancedClogReadStatus(GlobalTransactionId gxid,
                                   TxnStatus *status,
                                   uint64 *start_ts, uint64 *commit_ts);   /* 预留 */
```

最小实现要求(满足回放模块即可):按 `GxidNodeId` 分目录
`pg_gclog/<node_id>/`,段文件内按低位 xid 直接寻址定长槽;写路径
write + 延迟 fsync,fsync 时机由 §8.4 推进协议步骤 2 保证。SLRU 化、缓冲与 GC
属全局 MVCC 文档范围。

**槽长由 16 改为 24 字节(R2-d 实测修正)。** 上面的 `uint8 start_ts[6]` /
`commit_ts[6]` 是 48 位时间戳,**放不下 TimestampTz** —— 它是"自 2000-01-01
起的微秒数",`2^48 µs ≈ 8.9 年`,**2008 年就溢出了**;实测当前值
`839,057,670,472,675`,是 `2^48 = 281,474,976,710,656` 的三倍。截断不报错,
只会把提交顺序悄悄弄乱,而 §10 的可见性判据正是 `commit_ts ≤ 快照 start_ts`。
落地布局:

```c
typedef struct EnhancedClogSlot
{
    uint64  start_ts;       /* 事务启动时间戳                   */
    uint64  commit_ts;      /* 提交时间戳;ABORTED 时为 0        */
    uint32  status;         /* TxnStatus                        */
    uint32  reserved;       /* 显式补齐,恒为 0                  */
} EnhancedClogSlot;         /* 24 字节 */
```

TSO 就位后只换取值来源,不动布局。`reserved` 与 `TxnMarkerPayload`/`XidMapEntry`
同理:槽直写磁盘,不留未初始化的填充洞。

**全零槽 = `TXN_RUNNING` = 未决 = 不可见。** 这正是稀疏文件空洞读出来的样子,
也正是想要的默认值:没有判决的事务一律当作还没提交。段文件因此**必然稀疏**,
只有真正回放过的 xid 才占实际块。

**回滚的子事务不写 `ABORTED`,靠"缺席"表达**(与 §7.6 一致):被 `ROLLBACK TO`
的子事务不在 MARKER 的提交清单里,于是它的槽从未被写过,读出来就是
`TXN_RUNNING`。实测核账:顶层与已 RELEASE 的子事务全部 `committed`,
被回滚的那个是 `running`。

**不缓存 fd**(与内核 SLRU 同款做法,`slru.c` 的读写页都是开-读写-关)。
`OpenTransientFile` 的 fd 登记在 resource owner 上、事务结束即被关闭,跨事务
缓存它轻则报 "temporary files and directories not closed at end-of-transaction",
重则 fd 已被复用、后续 pwrite 打到不相干的文件上。开销可忽略:MARKER 是
"每事务每分区一条",而每条记录本就要走一次带 fsync 的 Raft 往返。

**并发**:回放 worker 是每分区一个,而 gclog 是每来源节点一套 ⇒ 同一段文件会被
多个 worker 同时写,`ShardReplayCtx` 那套"每分区独占、无需加锁"的前提在这里
**不成立**。但实际不需要锁:段文件创建用 `O_CREAT`(不带 `O_EXCL`,并发创建都
成功、同一个文件);槽内容幂等 —— 同一 gxid 被两个 worker 写,是因为该事务跨了
两个分片,两条 MARKER 的载荷来自同一次提交,算出的 24 字节完全相同,任何交错
都得到同样的结果。

### 9.4 ShardRouteEntry 可见性路由表(R3 实装,本期建结构)

```c
/* include/shard_route.h */
typedef enum
{
    SHARD_NATIVE_LEADER,      /* 本机原生 Leader 分区 */
    SHARD_FOLLOWER_REPLAYED,  /* 同步而来,当前为 Follower */
    SHARD_PROMOTED            /* 同步而来,已升主 */
} ShardRole;

typedef struct ShardRouteEntry
{
    RelFileLocator    local_loc;   /* 键:本地文件号(含索引、TOAST 堆及其索引) */
    Oid               shard_oid;
    ShardRole         role;
    FullTransactionId watermark;   /* 升主水位 W,仅 SHARD_PROMOTED 有效 */
    dshash_table_handle xid_map_h; /* 该分区 xid_map 的 dshash 句柄(R3) */
} ShardRouteEntry;
```

与 leader 侧 fileset 反向哈希**同构同源、查询方向相反**;随副本创建、fileset 变更、
角色切换在同一临界区原子更新。补丁 0002 的豁免判断(§8.3)复用同一张表。

**路由规则**(查询期,以 `BufferGetTag(buffer)` 的 RelFileLocator 为键):

1. 未命中,或 `role = SHARD_NATIVE_LEADER` → 本机命名空间,
   `gxid = MakeGlobalXid(本机 node_id, xid)`;
2. `role = SHARD_FOLLOWER_REPLAYED` → 查该分区 xid_map 得 gxid;
3. `role = SHARD_PROMOTED` → `xid ≤ watermark(W)` 则查该分区 xid_map,
   否则归本机命名空间。

规则成立的两个前提均由回放模块保证:物理隔离(本机事务与回放事务的元组永不同文件,
§7 前置条件 2)、水位隔离(升主后新分配 xid 严格大于 W,§7.5)。

### 9.5 子事务语义(与 reply_v1 的差异点,需回同步)

`xl_xid` 是产生该记录的(子)事务号,头部 gxid 是顶层事务的。若把 subxid 直接映射到
顶层 gxid,SAVEPOINT 回滚的子事务元组会随顶层提交被误判可见。因此:

- xid_map 按 `xl_xid → MakeGlobalXid(origin, xl_xid)` 登记(每 xid 独立 gxid);
- COMMIT 标记携带已提交子事务列表,逐一写 COMMITTED(§4.3、§7.6),
  等价原生 `TransactionIdCommitTree` 的全局版;
- 中止子事务不在列表 → 无 COMMITTED 记录 → 不可见。

---

## 10. MVCC 预留接口(本期只定义,不实现)

```c
/* include/global_mvcc.h —— 读路径全部 stub:ereport(ERROR, "not implemented") */

/* 命名空间路由 + 映射:§9.4 三条规则的封装。
 * 返回 InvalidGlobalXid 表示 xid 无对应全局事务(如冻结后)。 */
extern GlobalTransactionId PartDistResolveGxid(RelFileLocator loc, TransactionId xid);

/* 全局快照(TSO start_ts)下的元组可见性总入口,未来接管
 * HeapTupleSatisfiesMVCC 对路由表命中文件的调用。 */
extern bool HeapTupleSatisfiesGlobalMVCC(HeapTupleHeader tuple,
                                         uint64 snapshot_start_ts,
                                         Buffer buffer);

/* 路由表维护(回放/建副本模块调用,R1 即可实装注册,查询侧 R3 生效) */
extern void PartDistRouteRegister(Oid shard_oid,
                                  const ShardFileSet *local_fileset,
                                  ShardRole role);
extern void PartDistRouteUpdateFileset(Oid shard_oid,
                                       const ShardFileSet *new_fileset);
extern void PartDistRoutePromote(Oid shard_oid, FullTransactionId watermark);
extern void PartDistRouteDrop(Oid shard_oid);

/* xid_map(回放侧写,查询侧读) */
extern void ShardXidMapInsert(ShardReplayCtx *ctx, TransactionId xid,
                              GlobalTransactionId gxid);
extern bool ShardXidMapLookup(Oid shard_oid, TransactionId xid,
                              GlobalTransactionId *gxid);
extern void ShardXidMapTruncate(Oid shard_oid, TransactionId frozen_bound);
```

回放模块本期**调用**:`EnhancedClogWriteStatus`、`ShardXidMapInsert`、
`PartDistRouteRegister/UpdateFileset`(注册与 fileset 维护)、
`PartDistAdvanceNextXidPastXid`。其余为读侧预留,签名冻结、实现留空。

---

## 11. Failover:Follower 提升为 Leader(R4)

1. Raft 选举胜出(日志最全者当选,Raft 保证)。
2. **追平**:调 `replay_catchup(shard, commit_index, timeout)` —— **这是惰性回放
   唯一真正干活的时刻**(见文首"形态修正")。在此之前该副本一条 redo 都没做过,
   积压可能很大,追平耗时与积压量成正比,须计入切主 SLA。
   内部即循环 §7 阶段二~四,直至 `applied_part_lsn` 达到 `commit_index`。
3. **最终 apply checkpoint**:落盘游标、`max_orig_lsn`、`max_replayed_fxid`、
   xid_map 快照;`PartDistAdvanceNextXidPastXid(max_replayed_fxid)` 拉齐 nextXid。
4. **推进本地 WAL 插入位点**越过 `max_orig_lsn`(内核补丁 0003,
   `pg_partdist_advance_wal_to(XLogRecPtr)`:语义类似 pg_resetwal 的 `-l` 但在线执行
   ——持有全部 WALInsertLock,把 insert 位置跳到目标 LSN 所在段起点并切段)。
   必要性:提升后该 shard 页面开始由本地 `XLogInsert` 保护;若本地位点低于页 LSN,
   新记录 LSN 小于页面现有 LSN,本地崩溃恢复时 `lsn <= PageGetLSN` 会**错误跳过
   新记录**,数据丢失。位点越过后,"页面 LSN 单调递增"不变式恢复,§8.3 的
   XLogFlush 豁免对该分区随之收敛。

   > **运维面(本文档此前的空白,落地前须定方案)**:一个节点同时托管多个不同 leader
   > 的副本,各 leader 的 LSN 坐标彼此无关;每为一个 shard 升主就把本地插入位点向前
   > 跳一次,**跳过的 WAL 段号成为永久空洞**。LSN 是 64 位、不存在耗尽问题,但受影响的是:
   > (a) **归档连续性**——`archive_command` 不会为跳过的段号产生文件,下游按段号连续性
   > 校验的备份工具会报缺段,须确认所用工具容忍空洞或改用 `pg_receivewal`;
   > (b) `max_wal_size` / `wal_keep_size` 的账目按 LSN 距离计算,一次大跳跃会让
   > checkpoint 触发逻辑瞬间失真;(c) 已有的物理备库(如果该节点自身还带 standby)
   > 会在位点跳跃处断流,须重建。R4 立项时必须先出这三项的处置结论。
5. **路由切换**:`PartDistRoutePromote(shard_oid, W = max_replayed_fxid)`——
   角色置 `SHARD_PROMOTED`、登记水位,同一临界区原子生效。此后写路由切到本节点,
   `wal_insert_hook` 开始为其捕获新流,`partition_lsn` 从 Raft log index 继续。
6. 旧 leader 恢复后作为 follower 归队:对齐 `durable_part_lsn`,未 committed 的尾部
   按 Raft 截断——凭 §8.4 顺序保证(committed 才 apply 才刷盘),被截断记录的效果
   不可能已持久化。

控制面(自治选举 → 上报 master → group0 登记 → 落路由层 pg_dist_placement)见
raft 模块修订计划 §13,不在本文范围。

---

## 12. DDL 与文件集合维护(D1 已实装)

改变 relfilenode 集合的操作:`CREATE/DROP INDEX`、`REINDEX`、`VACUUM FULL`、
`TRUNCATE`、`ALTER TABLE` 重写类。

> **★ D1 之前的失效模式是「静默分歧」,不是报错。** 这条要写在最前面,因为它
> 决定了本节的优先级。捕获判据是"记录的 blocks[] 命中反向哈希"
> (`partwal_sync.c` 的 `if (found)`),**没命中就直接 return**。于是 leader 上
> 一次 `CREATE INDEX` 换出的新 relfilenode 从未登记 ⇒ 该索引的 WAL 记录一条都
> 不进流 ⇒ follower 的索引永远停在旧内容,而**两侧谁都不报错**。
> (follower 侧那句 "未知 relfilelocator" PANIC 只在另一种情形下出现:有人手动
> 调了 `register_shard_fileset()` 却没同步更新 follower 的 loc_map。)

### 12.1 leader 侧:检测与发射

检测挂在 `ProcessUtility_hook` 上,**不是事件触发器** —— `VACUUM FULL` 与
`CLUSTER` 不触发 `ddl_command_end`,用事件触发器会漏掉它们。

判据故意粗:只看语句类型(`UtilityMayChangeRelfilenode()` 的白名单),不从
parse tree 精确解出受影响的关系。精确解要为每种语句写一套(`AlterTableStmt`
还得逐个子命令看),漏一种就是静默分歧;而真正决定发不发记录的是 PRE_COMMIT
那次 fileset diff,误报的代价只是多走一次 catalog 遍历。

**发射点在 `XACT_EVENT_PRE_COMMIT`,不在 ProcessUtility 里。** DDL 一执行完就
发的话,事务随后回滚,follower 已经按新结构换过表、把本地文件截过了 —— leader
回到旧结构,副本停在新结构,且新结构的内容来自一次不存在的 DDL。放到 PRE_COMMIT,
暴露窗口与既有 COMMIT 标记完全一致(见 §13 约束 11)。

`ShardFilesetMaybeEmitUpdates()` 的六步,顺序全部是硬的:

1. `PartWALFlush(Invalid, false)` —— 排空已缓冲的记录。它们引用**旧**文件号,
   必须排在 CTRL 之前:排在后面的话 follower 已经换过表,旧文件号在 loc_map 里
   查不到,当场报"未知 relfilelocator"。
2. 重建 fileset 并与持久化版本 diff(按 `(role, ord)` 比对文件号);无变化收工。
3. `RegisterShardFileSet()` —— 新文件号即刻进入捕获。**必须在第 5 步之前**,
   否则那批 FPI 一条都进不了流(而且是静默丢弃,正是上面那个失效模式)。
4. 追加 `CTRL:FILESET_UPDATE`,载荷是新 fileset 的**全量**描述。
   全量而非增量,是因为 follower 侧的应用必须幂等(崩溃后从游标重放会再走一次),
   全量描述天然幂等:应用几次结果都是"loc_map 等于这份描述"。
5. 对**换了文件号的成员**调 `log_newpage_range(..., page_std = false)`,把新文件
   内容以 FPI 形式送进流。`page_std = false`(整页搬,不掐 `[pd_lower, pd_upper)`
   的洞)有两个理由:VM/FSM 这类非标准布局的 fork 掐洞判据本就不适用;主 fork
   整页搬则让 follower 的新文件成为 leader 的逐字节副本,连洞内残字节都一致,
   正好省掉一处判据例外(对比 §14.2 的 ★ 块)。
6. `PartWALFlush(Invalid, true)` —— 排空 FPI 并写 COMMIT 标记。

CTRL 记录的 `orig_lsn` 取**当前 WAL 插入位置**(`GetXLogInsertRecPtr()`),
不是本事务数据记录的最大 LSN:紧跟其后的 FPI 的 `orig_lsn` 必然更大,这样
"CTRL 在前、FPI 在后"在 `partition_lsn` 与 `orig_lsn` 两个维度上都成立,
段号(由 orig_lsn 换算)也不会倒挂。

`PartWALAppendCtrl()` 还顺手把该分区登进本事务的 touched 表并**就地复制一次**:
纯结构变更事务(典型是 `DROP INDEX`)不产生任何 DATA 记录,`PartWALFlush` 会在
"本 backend 无插入"分支直接返回,控制记录就只落本地、永远进不了 Raft。

**超大关系的退路**:新文件总块数超过 GUC `pg_partdist.fileset_inline_max_blocks`
(默认 131072 = 1 GB)时,CTRL 带 `NEEDS_REBASELINE` 标志且**不灌内容** ——
与其让一次 `VACUUM FULL` 把几十 GB 塞进 Raft 日志,不如显式退回基线拷贝。
follower 见到该位一律停在栅栏上。

### 12.2 follower 侧:换表还是停下来

判据是**结构有没有变**,即新 fileset 的 `(role, ord)` 集合是否与当前 loc_map
相同(成员数相等 + 每个 leader 成员都能找到同 `(role, ord)` 的本地对应;
两侧 `(role, ord)` 各自唯一 ⇒ 这两条合起来即集合相等)。

| 情形 | DDL | 处置 |
|---|---|---|
| 集合不变,只换文件号 | `VACUUM FULL` / `REINDEX` / `TRUNCATE` / 重写类 `ALTER` | **全自动**:原地换 loc_map,把换了号的成员对应的本地文件**截成 0 块**,后续 FPI 填满 |
| 集合变了(增或减) | `CREATE INDEX` / `DROP INDEX` | **停在结构栅栏** `REPLAY_NEEDS_STRUCT` |

**为什么集合变了不能自动配:`ord` 是位置,不是身份。** leader 删掉 `ord=0` 的
索引之后,原来的 `ord=1` 会补位成 `ord=0`。照 `(role, ord)` 硬配,follower 会把
leader 新 0 号索引的内容灌进本地那个本该被删掉的 0 号索引文件里,而 catalog
还宣称它是另一组列上的索引 —— **静默损坏**,比停下来难查得多。

**为什么本地文件要先截 0**:leader 换号意味着那是一个全新的空文件,随后流里是它
全部页面的 FPI。本地旧文件若比新文件长,FPI 覆盖不到的尾部会留下上一代的残页,
而两侧文件长度不同这件事本身就会让页面比对直接判负。截断幂等:崩溃后从游标重放
会再截一次 0、再放同一批 FPI,结果相同。

**栅栏的语义是「游标一个字节都没推进」**:全部校验通过之前不动任何字节,
`ApplyCtrlRecord()` 返回 false 后应用循环直接跳出,`applied_part_lsn` 停在该
CTRL 记录**之前**。所以它与 `REPLAY_FAILED` 是两回事,值得单列一个状态:
前者补齐本地结构 + 重跑 `replay_set_locmap()` 就能原地继续,后者通常意味着这个
副本要重做。混成一个状态,运维分不清"补个索引就行"和"这副本废了"。

栅栏的解除靠 `replay_set_locmap()`:它写完新 locmap 会把槽位的 `locmap_gen` 加一,
worker 据此丢弃内存里那张旧 loc_map 重建 ctx —— 否则补完结构再触发一次,
worker 抱着旧表还是撞同一道栅栏。

`replay_catchup()` 遇到栅栏**立即报错返回**,不干等到超时:它要等的是人工动作,
而超时报出来的会是"追平超时",把原因盖掉。

### 12.3 opcode 清单

| opcode | 载荷 | 用途 |
|---|---|---|
| `FILESET_UPDATE` (0x01) | `PartWALCtrlFilesetUpdate` + `ShardFileSetRel[]` | 物理文件集合变更(§12.1/§12.2) |
| `FREEZE_UPDATE` (0x02) | `PartWALCtrlFreezeUpdate` + `PartWALFreezeEntry[]` | 冻结账目同步(§13 约束 5,D2) |

两者的**失败语义刻意不同**,别照抄:

- `FILESET_UPDATE` 发射失败 ⇒ **让调用方的事务中止**。结构变了却没通知副本,
  DDL 就不该提交,否则副本静默分歧。
- `FREEZE_UPDATE` 发射失败 ⇒ **只告警,不更新基线,下次重发**。它只是账目字段,
  为它中止用户的一次 VACUUM 是本末倒置 —— 而实测正会发生:VACUUM 刷出的 FPI
  洪水把 Raft 心跳饿死、领导权移走,紧接着的 `PartWALAppendCtrl` 即报错。

### 12.4 已知边界(D1 未覆盖)

- **`DROP TABLE`**:shard 整个没了。副本侧的处置(停流 / 删副本)需要另一个
  opcode,D1 保持沉默而不是发一条半吊子的 `FILESET_UPDATE`。
- **`ALTER TABLE ... SET TABLESPACE`** 改的是 `spcOid`,机制上与换 relfilenode
  同路(diff 按整个 `RelFileLocator` 比),但 follower 侧本地表空间未必存在,
  未验证。
- follower 侧的结构补齐目前是**人工**的(或由上层协调通道驱动)。让 replay
  worker 自己跑 SPI DDL 的路走不通:它得从本地堆读数据建索引,而回放元组在 R3
  之前根本不可见,建出来是空的(虽然后续 FPI 会覆盖),且要把 DDL 与
  `InRecovery = true` 混在一个进程里。

---

## 13. 约束与风险清单

1. **完整物理子流**(§5.3):凡改这些文件字节的记录一条不漏——按 fileset 命中捕获,
   漏登记在 follower 侧以"未知 relfilelocator" PANIC 暴露(fail-fast,不静默漏)。
2. **同源物理基线**:副本必须由 leader shard 物理拷贝初始化(拷贝时记下
   partition_lsn 静止点,增量从该游标重放追齐)。
3. **恢复上下文完备性**:`InRecovery = true` 覆盖已核实断言;落地时审计 redo 路径
   其余 `InRecovery` / `reachedConsistency` 分支(`log_invalid_page` 在非恢复进程中
   reachedConsistency=false,会记入进程内 invalid_page_tab 而不立即 PANIC——
   追平/升主前调用 `XLogHaveInvalidPages()` 断言为空),列入 R1 验收项。
   **已核实的反例**:`CreateFakeRelcacheEntry`(`heap_xlog_visible` 会用)在 16.14
   **已不再** `Assert(InRecovery)`,该路径不构成障碍。
4. **原生 clog 空洞**:nextXid 被推进但回放 xid 区间从未 `ExtendCLOG`,原生
   `TransactionIdDidCommit` 触碰这些 xid 会因 clog 文件缺失报错。必须保证:
   副本壳表 `autovacuum_enabled = off`(建表即设 + launcher 防御性校验),
   副本分区不服务任何本地读写(升主前)。
5. **冻结与回卷账目**:副本表的 relfrozenxid 不由本地 vacuum 维护(freeze 由 leader
   的 freeze 记录回放实现 —— 元组物理上确实被冻了,只是 `pg_class.relfrozenxid`
   这个**目录字段**没人更新)。本地 `datfrozenxid` 计算须排除副本壳表,或经控制记录
   同步 leader 的 relfrozenxid;同时 nextXid 被最活跃 leader 拉齐后,本地普通表的
   age() 相应增大,须确认 autovacuum 对本地表正常 freeze。R2 验收项。

   > **⚠ 约束 4 的 `autovacuum_enabled = off` 挡不住这条(R2-e 核实)。**
   > 内核 `autovacuum.c:3196` 原文:
   > ```c
   > /* User disabled it in pg_class.reloptions?  (But ignore if at risk) */
   > if (!av_enabled && !force_vacuum)
   > ```
   > `force_vacuum` 的判据是 `relfrozenxid < nextXid - autovacuum_freeze_max_age`。
   > 一旦越线,`autovacuum_enabled=off` **被忽略**,副本壳表照样会被强制
   > anti-wraparound vacuum 扫到 —— 而它的元组带的是外来节点的 xid:本地 clog
   > 要么没有对应页(§13 约束 4 说的 "could not access status of transaction"),
   > 要么给出张冠李戴的答案。同时这些表还会把库级 `datfrozenxid` 压住、阻塞
   > clog 截断。
   >
   > 所以"建表即设 autovacuum off"只是**推迟**问题,不是解决。真正的处置有两条路,
   > 都需要 CTRL 记录通道(§12):
   >   (a) leader 在 freeze 之后经 CTRL 记录同步自己的 relfrozenxid,follower 据此
   >       更新壳表的目录字段 —— 语义最干净;
   >   (b) 直接把壳表的 relfrozenxid 推到接近本地 nextXid。副本元组的 xid 本就是
   >       外来的、本地机制"必须永不解释"(约束 4),本地口径的 relfrozenxid 对它
   >       没有意义 —— 但这是一次**有语义后果的取舍**(升主后该字段仍然是错的),
   >       须显式决策,不能顺手做掉。
   >
   > R2-e 交付的是**可观测**:`partdist.replay_freeze_status()` 把每个副本壳表
   > 离强制阈值还有多远(`pct_to_force`)摆出来,验收用例断言 autovacuum 确实已关
   > 并打印最坏值。
   >
   > **★ 处置已定:走方案 (a),经 CTRL 同步(D2 已实装,2026-08-04 用户决策)。**
   >
   > 为什么 (a) 是**真话**而 (b) 是编造:`relfrozenxid = X` 的语义是"本关系内
   > 不存在比 X 更老的未冻结 xid"。副本堆页与 leader **逐字节一致** ⇒ 同一句话
   > 在 follower 上同样成立。搬过来不是伪造,而是把一个本来就为真、只是没人
   > 写下来的事实补上。(b) 则是明知副本元组带外来 xid 还把本地值往前推。
   >
   > 落地形态(§12.4):
   > - 新 opcode `CTRL:FREEZE_UPDATE`,载荷只含堆关系(索引的 `relfrozenxid` 恒为 0)。
   > - leader 侧检测是**时间驱动**而非 DDL 驱动:`autovacuum` 推进 relfrozenxid
   >   **不走 ProcessUtility**(它在 autovacuum worker 里直接调 `vacuum_rel`),
   >   D1 那套"DDL 后置脏标记"对它完全无效。GUC
   >   `pg_partdist.freeze_sync_interval_ms`(默认 60s;relfrozenxid 是以千万 xid
   >   为尺度变化的慢变量,分钟级滞后无影响)。去重靠与持久化基线
   >   `pg_parwal/<oid>/freeze` 的 diff,不靠间隔本身。
   >
   >   **★ 间隔水位必须在共享内存里**(`PartWALCtlData.freeze_last_check`,
   >   经 `PartWALFreezeCheckDue()` 在锁下 compare-and-set),不能是 backend
   >   本地 static —— 本地 static 的初值 0 = "从没查过",于是**每条新连接的
   >   第一次提交都会无视间隔立刻发射**。R1 开几十条短命 psql 连接,实测就把
   >   "每分钟一次"变成了"每连接一次",几十条 CTRL propose 压在 VACUUM 灌
   >   Raft 日志环的窗口上,把 128 槽的环顶爆 → 见约束 13(永久分叉)。
   >   改成节点级水位后,同一轮 R1 的发射次数从几十降到 2,环顶爆 0 次。
   > - **发射是尽力而为,失败绝不拖垮调用方的事务。** 与 FILESET_UPDATE 的区别
   >   是本质的:结构变了却没通知副本,DDL 就不该提交;而 relfrozenxid 只是账目,
   >   为它中止一次 VACUUM 是本末倒置 —— 实测正会如此:VACUUM 刷出的 FPI 洪水
   >   把 Raft 心跳饿死、领导权移走,紧接着的 `PartWALAppendCtrl` 即以"本节点
   >   不是该分区组的 leader"报错。失败时**不更新基线**,下次检查自然重发。
   > - **follower 侧的写入不在 replay worker 里做。** worker 的连接是
   >   `BackgroundWorkerInitializeConnection(NULL, NULL, 0)` —— **故意不选数据库**
   >   (那么写是为了走完 `BaseInit`,不是为了读目录)。在它里面碰 `pg_class` 会
   >   当场 `cannot read pg_class without having selected a database` FATAL,
   >   而 worker 一 FATAL 就重启、从游标重放、再撞同一条记录 —— **无限崩溃循环**
   >   (实测撞到,`applied` 永远停在 0)。所以 worker 只把值搬进共享内存槽位,
   >   真正写 `pg_class`(`heap_inplace_update`,同内核 `vac_update_relstats`)
   >   由 `replay_catchup()` 的**调用方**完成 —— 惰性形态下回放只可能由它触发,
   >   一定有这么一个有数据库、有事务的普通 backend 在。
   > - **★★★ leader 侧的发射用白名单门禁:本 backend 必须已经执行过至少一条
   >   用户语句**(`ShardFreezeNoteUserActivity()`,由 `ExecutorStart` /
   >   `ProcessUtility` 两个钩子置位),外加第二道 `MyBackendType == B_BACKEND`
   >   且 `application_name` 不以 `citus_` 开头。这不是防御性编程,是**必需条件**。
   >
   >   理由:本检查挂在**每一个**事务的 `PRE_COMMIT` 上,而发射一次 = 一次
   >   **同步 Raft 复制**,要阻塞等对端多数派 ack。于是任何"本身就在管理到
   >   对端连接"、或者根本还没初始化完的进程,一旦被挂上这个钩子就会出事。
   >   实测四例,**前三例都是先补黑名单、下一轮换个进程接着崩**:
   >
   >   | 进程 | 后果 |
   >   |---|---|
   >   | ① autovacuum launcher(不绑数据库) | 读 `pg_class` 当场 FATAL → 节点重置 |
   >   | ② Citus 维护守护进程(`B_BG_WORKER`,跑 2PC 恢复 + 分布式死锁检测) | 同节点 backend `signal 11` → 节点重置 → 该组失多数派 → follower 追不平 |
   >   | ③ Citus 内部 backend(`citus_internal gpid=` 等) | 本节点在给别的节点干活,在它的提交点反向同步等对端,同类重入 |
   >   | ④ **任何新连接的 `InitPostgres` 引导事务** | `B_BACKEND` + 有数据库 + `application_name` 未设 ⇒ 前三条黑名单全放行,而 backend 尚未初始化完 → **SIGSEGV** |
   >
   >   ④ 的栈回溯(用 `pg_partdist.debug_segv_backtrace` 拿到):
   >   ```
   >   InitPostgres → CommitTransactionCommand → PartWALXactCallback
   >     → ShardFreezeMaybeEmitUpdates → ShardFreezeEmitOne
   >     → PartWALAppendCtrl → PartWALReplicateTouched
   >     → pg_raft_partwal_replicate → data_propose_one
   >     → text_to_cstring → pg_detoast_datum_packed        ← SIGSEGV
   >   ```
   >   即**每建一条连接就赌一次**。节点被打死后该分区组失去多数派,follower
   >   收不到后半截记录,最终表现成"回放出的文件 diff 不一致"——而且每轮崩在
   >   哪一步是随机的,于是**每轮失败的项都不一样**,极像回放逻辑的偶发缺陷。
   >   R1 因此连续四轮被误判(29~47/55 之间乱跳),直到把栈打出来才定位。
   >
   >   **黑名单永远补不完,所以改成白名单。** 发射器本就是时间驱动的慢账目
   >   同步,"等这个 backend 先跑条正经语句"没有任何代价。
   >
   >   排除 autovacuum worker 看似矛盾(推进 `relfrozenxid` 的正是它),其实不然:
   >   发射器是**时间驱动 + 与持久化基线 diff**,不关心"谁改的",晚一点由任意
   >   普通 backend 的提交点捎带即可。
   >
   >   对比 §12 的 fileset 发射器:它由 `ProcessUtility` 置的脏标记护着,只在真正
   >   跑了 DDL 的事务里发射(Citus 传播 DDL 用的正是内部 backend,所以那条**不能**
   >   按 `citus_` 排除)。**时间驱动的没有这层保护,必须自己判。**
   >
   >   配套修掉的 pg_raft 缺陷:`data_propose_one()` 读记录头时没判 `isnull`,
   >   而 `partwal_read_record` 读不出记录时返回的是**一行全 NULL**而不是零行,
   >   于是 `TextDatumGetCString(0)` 直接解引用空指针。一个本该是事务级 ERROR
   >   的情况被放大成节点级崩溃 —— 上面那个 SIGSEGV 的最后一跳就是它。
   > - **写入不间断的分片上,账目可能长期同步不了**(已知代价,非缺陷):本事务
   >   只要还有未落盘的分区记录就整轮跳过(`PartWALHasPendingRecords()`),不与
   >   正在写分片流的事务交织。fileset 变更不能这么做(结构变更必须与那次 DDL
   >   同事务发出去),冻结账目可以 —— 两者的可延迟性本就不同。
   >
   > **仍未根治(D2 的边界,须显式记住)**:follower 的 `nextXid` 是被节点上
   > **最活跃的那个 leader** 拉上去的(`PartDistAdvanceNextXidPastXid` 推的是
   > 节点全局值)。同时托管来自 leader X(很忙)与 leader Y(很闲)的副本时,
   > Y 的 `relfrozenxid` 在本节点的 xid 空间里看起来依然很老,`age()` 照样会爆。
   > (a) 根治的是**单 leader 场景**——也就是现在必然出问题的那个;跨 leader 的
   > xid 偏斜要把副本壳表从 `datfrozenxid` 计算与 autovacuum 强制路径里摘出去,
   > 那需要内核补丁。与约束 12 是同一件事的两面:副本文件被本地 WAL 触碰的
   > 唯一剩余入口就是这条 anti-wraparound vacuum。
6. **unlogged / 临时表**:不产生 WAL,天然不在流内;shard 表必须是 logged。
7. **多索引 AM**:R1 只承诺 heap + btree(覆盖 TOAST);GIN 等按附录 A 矩阵逐个
   验证后放开。
8. **性能**:每 shard 单 worker 串行,吞吐上限 ≈ 单核 redo 速度(与 PG standby
   同量级),shard 粒度天然并行;瓶颈实测后再谈批量优化。
9. **撕裂页**(§8.5):sidecar 落地前,unclean shutdown 后以 checksum 校验 + 重新
   基线兜底。
10. **★ 单写者不变式必须显式加锁,不能只写在前置条件里**。§7 前置条件 2("worker 对
    该分区全部数据文件独占访问")在代码里**没有任何强制**。已核实
    (`xlogutils.c:526` 扩展文件路径):

    ```c
    Assert(InRecovery);
    buffer = ExtendBufferedRelTo(BMR_SMGR(smgr, RELPERSISTENCE_PERMANENT), forknum, NULL,
                                 EB_PERFORMING_RECOVERY | EB_SKIP_EXTENSION_LOCK, blkno + 1, ...);
    ```

    **`EB_SKIP_EXTENSION_LOCK` 是写死在这条路径里的常量,不受 `InRecovery` 开关控制**
    ——`InRecovery=true` 只是让断言通过,并不会为你加回关系扩展锁。所以一旦"同一 shard
    只有一个写者"被打破(worker 池认领冲突、将来的并行回放、误启动两个 launcher),
    两个进程会**无锁并发扩展同一个文件**,结果是静默堆损坏,不报错、不 PANIC。
    落地要求:worker 认领 shard 时取该 shard 的排他锁(shmem 槽位 CAS 或 advisory lock),
    释放在 worker 退出路径上;并在回放主循环入口断言"本进程持有该 shard 的认领权"。
    这条与 §7 的 worker 池形态是配套的,不可只做池不做锁。
11. **★ `PRE_COMMIT` 之后失败的窗口,对 fileset 变更没有"缺席即回滚"这条退路**
    (D1 记入)。DATA 与 COMMIT 标记都在 `XACT_EVENT_PRE_COMMIT` 落盘并复制,
    此后事务仍有可能失败;对 DATA 而言这不成问题——**没有 COMMIT 标记的数据在
    follower 上就是未决 = 不可见**(§4.3/§9.3),回滚语义由缺席表达。

    `CTRL:FILESET_UPDATE` 没有这个性质:它不经事务判决,在流内是无条件生效的。
    所以 PRE_COMMIT 之后 DDL 事务若失败,leader 回到旧结构、副本已换到新结构,
    且新结构的内容来自一次并未发生的 DDL;leader 侧持久化的 fileset 文件也会
    指向已不存在的 relfilenode(下一次 DDL 的 diff 会把它纠回来)。

    窗口与既有 COMMIT 标记完全同宽——发射点刻意选在 `PartWALFlush(..., true)`
    **之前**紧邻处,不新增暴露面。要真正消除它,得让 CTRL 也参与事务判决
    (follower 见到 CTRL 先挂起、等到该 gxid 的 COMMIT 标记才生效),
    代价是回放管线从"串行应用"变成"带未决队列",与 §7 的形态冲突。
    D1 显式选择记录而不是解决。
12. **★★ 副本文件被两条互不知情的 redo 流写,而 FPI 的应用是无条件的**
    (2026-08-04 受控实验证实,已修一半)。

    副本的 shard 文件有两个写者:本模块的 parwal 回放(盖 **leader 坐标**的
    `orig_lsn`),以及节点自身的**本地 pg_wal 崩溃恢复**。两者互不知情。

    致命处在于内核对 FPI 的处理**不比页 LSN**。`xlogutils.c` 的
    `XLogReadBufferForRedoExtended()`:

    ```c
    /* If it has a full-page image and it should be restored, do it. */
    if (XLogRecBlockImageApply(record, block_id))
    {
        *buf = XLogReadBufferExtended(rlocator, forknum, blkno,
                                      RBM_ZERO_AND_LOCK, prefetch_buffer);
        if (!RestoreBlockImage(record, block_id, page)) ...
    ```

    整页清零后覆盖,**前面没有任何 `lsn <= PageGetLSN(page)` 判断**——那条判据
    只存在于下面的非 FPI 分支。所以"我们盖上去的 leader LSN 比本地 LSN 大"
    这件事**保护不了**任何东西。

    触发路径:follower 的 shell 表是本地 `CREATE TABLE ... (LIKE ... INCLUDING ALL)`
    建的,建索引时每个元页都以 FPI 写进了本地 WAL(`wal_level = replica` 下
    wal_skip 优化不适用——它只在 `wal_level = minimal` 时生效)。此后节点一旦
    崩溃,且崩溃恢复的 redo 起点早于建表,那条 FPI 就把回放出来的元页**无条件
    盖回** `_bt_initmetapage` 的初值。

    **受控实测**(控制变量 = 建壳表之前先 CHECKPOINT 把 redo 点钉住):

    | | pd_lsn | btm_root |
    |---|---|---|
    | 崩溃前(回放写的) | `0/A74EFD0` | 1 |
    | immediate 崩溃重启后 | `0/81DF748` | **0** |

    建壳表的 WAL 区间是 `[0/81AECC0, 0/81E0198]`,崩溃后那一页的 LSN 正落在
    区间内,是一个**本地** LSN。前两轮探针没能复现,只因 redo 点碰巧已越过建表
    位置——这个坑的窗口是"建壳表到下一次本地 checkpoint",默认最长 5 分钟。

    **已落地的修法**:`replay_set_locmap()` 末尾强制一次
    `RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT)`,
    把 redo 点推到建壳表之后。选这个入口是因为它是建立/刷新映射的唯一通道:
    首次配对,以及 §12 结构栅栏之后的重新配对(后者紧跟运维在本地壳表上做的
    `CREATE INDEX`,同样会留下本地 WAL)。同一个受控实验验证:修复后崩溃前后
    元页逐字节一致。

    **不变式**:*副本文件在开始回放之前,写过它们的本地 WAL 必须已被 checkpoint
    甩到 redo 点之后;开始回放之后,不得再有任何本地 WAL 记录写它们。*

    **仍未根治**(与约束 5 联动):`autovacuum_enabled = off` 挡不住
    anti-wraparound vacuum,它一旦扫到副本壳表就会写本地 WAL,把这个洞重新打开。
    彻底的办法只有让副本文件永不被本地 WAL 触碰,而那需要先解决约束 5 的
    relfrozenxid 处置。**两条约束应当合并立项,不要各修各的。**

13. **★★ Raft 日志环顶爆 ⇒ leader 的物理截断在 follower 上永久缺失**
    (2026-08-05 实测,未修,记为 pg_raft 侧待办)。

    `RAFT_LOG_CAPACITY = 128`,`log_append_locked()` 的门是
    `last_log_index - last_applied >= 127`。VACUUM 的 FPI 洪水灌记录的速度
    远高于同步 apply 速率(实测 ~3–10 条/秒),环一满就
    ```
    WARNING: pg_raft: group 102042 log ring full, cannot append
    ERROR:   pg_raft: 分区 188522(组 102042) record 2607 复制未达多数派,prepare 失败,事务中止
    ```
    问题在于:**`lazy_truncate_heap()` 的物理截断在 leader 上已经做掉,而它
    不随事务回滚**(内核在 `AccessExclusiveLock` 下截空页,认定安全)。于是
    leader 短了、follower 没短,那条 `XLOG_SMGR_TRUNCATE` 再也不会重发 ——
    **永久分叉,既无检测也无修复路径**。R1 的实测表现:TOAST 主堆
    leader `155648` / follower `614400`,而索引、TOAST 索引、主堆都对得上,
    极像"某个特定关系的回放漏了"。

    相关但不同的一个坑:中止的那个事务在分区流里留下了读不出来的空洞
    (`partwal_read_record 返回空行`),复制挂钩是区间式的
    (`[last_data_plsn+1, flush_lsn]`),踩到洞就整段失败。pg_raft 的
    `data_propose_one()` 原本在这里**没判 `isnull`** —— 而
    `partwal_read_record` 读不出记录时返回的是**一行全 NULL 而非零行**,
    上游 `SPI_processed == 0` 那道门拦不住,于是 `TextDatumGetCString(0)`
    直接 SIGSEGV 打死整个节点。已修成事务级 ERROR。

    **D2 与这条的关系**:冻结账目发射器会往同一个环里插 CTRL propose。
    发射水位原本是 backend 本地 static(初值 0 = 从没查过),导致**每条新连接
    的第一次提交都无视间隔立刻发射**;R1 开几十条短命 psql 连接,恰好压在
    VACUUM 灌环的窗口上,把环顶爆。已改为节点级共享水位
    (`PartWALFreezeCheckDue`,`PartWALCtlData.freeze_last_check`)。
    **但那只是让 D2 不再去顶这个环,环本身的流控缺失没有解决。**

    真正的修法在 pg_raft:propose 侧在环接近满时**阻塞等 apply**而不是报错,
    或把环溢出到磁盘。

---

## 14. 代码落点与分阶段计划

### 14.1 新增/修改文件

```
pg-partdist-src/
  include/
    partition_wal_header.h     [改] parwal-3.0:gxid 头、MARKER/CTRL flags、TxnMarkerPayload;
                                    D1:PARTWAL_CTRL_RMID/opcode、PartWALCtrlFilesetUpdate
    partwal_sync.h             [改] PartWALFlush(upto, write_marker)、PartWALEndTxn;
                                    ABORT 标记补写(§4.3 写入时机)
    shard_fileset.h            [新] ShardFileSet + 反向映射接口(§5);
                                    D1:BuildShardFileSetEx(带 relids)、
                                    ShardFilesetNoteMaybeChanged/MaybeEmitUpdates(§12)
    shard_replay.h             [新] ShardReplayCtx、LocMapEntry、回放入口、边界回调(§6/§7)
    shard_xidmap.h             [新] XidMapEntry、xid_map 接口(§9.2)
    shard_route.h              [新] ShardRole、ShardRouteEntry、路由接口(§9.4)
    enhanced_clog.h            [新] TxnStatus、EnhancedClogRecord、读写接口(§9.3)
    global_mvcc.h              [新] GlobalTransactionId、MakeGlobalXid、预留读接口(§10)
  src/replay/
    replay_worker.c            [新] launcher + per-shard bgworker、InRecovery
    shard_replay.c             [新] 五阶段主循环、DATA/MARKER/CTRL 三路派发、skip 白名单;
                                    PartDistAdvanceNextXidPastXid 与 xid_map 的
                                    建表/恢复/登记(实现落在这里,不另开 xid_map.c ——
                                    它们全都只被回放主循环调用,拆文件只增加往返)
    replay_checkpoint.c        [新] apply_checkpoint 单文件原子读写(§8.4),
                                    含 xid_map 快照与覆盖快照的 CRC
    enhanced_clog.c            [新] pg_gclog 写路径 + 延迟 fsync;读接口供核账
                                    (R3 读路径的入口)
    shard_route.c              [新] 共享内存路由表(注册/换表/promote;读侧 stub)
  patches/
    0002-flushbuffer-lsn-exempt-hook.patch   [新] §8.3
    0003-advance-wal-insert.patch            [新] §11(R4)
  tests/
    pagecmp.py                 页面比对(内核 heap_mask() 掩码集合,**堆页专用**)
    test_follower_replay_r1.sh R1 物理回放闭环
    test_lazy_replay_l1.sh     L1 惰性触发语义
    test_txn_layer_r2.sh       R2 事务层(gclog 直接查账)
    test_ddl_fileset_d1.sh     [新] D1 DDL/fileset 控制通道(§12)
    test_local_wal_conflict.sh [新] 本地 WAL 崩溃恢复不得覆盖回放结果(§13 约束 12)
    test_freeze_sync_d2.sh     [新] 冻结账目经 CTRL 同步(§13 约束 5)
```

> `test_local_wal_conflict.sh` 必须**独立**于 R1 的 kill -9 用例:修复生效后
> redo 点永远落在建壳表之后,R1 再也走不到该场景,对这条修复没有回归能力。
> 本用例反过来刻意把 redo 点钉在建壳表**之前**,让"修复是否还在"成为唯一变量。

GUC(前缀沿用 `pg_partdist.`):`replay_workers`(worker 池大小,默认 4,§7)、
`replay_naptime_ms`、`replay_checkpoint_interval_ms`、
`replay_checkpoint_bytes`、`replay_trust_local_segments`(测试模式,§6)、
`fileset_inline_max_blocks`(D1,§12,默认 131072)、
`freeze_sync_interval_ms`(D2,§13 约束 5,默认 60000;0 = 每事务查,测试用)、
`replay_dw_enabled`(sidecar,§8.5,**未实现**)、
`debug_segv_backtrace`(诊断,`PGC_POSTMASTER`,默认 off)。

> **★ `debug_segv_backtrace` 为什么必须有。** PostgreSQL 不装 SIGSEGV 处理器,
> 崩溃进程在日志里只留一行 `server process (PID nnn) was terminated by signal 11`
> ——没有栈、没有语句;而本环境 `core_pattern` 是管道到 apport,容器里拿不到 core。
> 结果是"看得见崩、看不见在哪崩"。开启后处理器把栈打进服务器日志再交还默认处置
> (崩溃语义不变),配合
> `addr2line -f -C -e /work/pg-install/lib/postgresql/pg_partdist.so <偏移>`
> 就能还原调用链。约束 5 的第 ④ 例正是这么定位的 —— 在此之前连续四轮误判。
>
> 验收脚本侧的配套:`tests/lib_node_health.sh` 的 `health_check_no_crash`,
> 在每个套件收尾断言"本轮时间窗内无 signal 11/6 或 PANIC"。**在此之前验收
> 脚本对"节点崩了"完全是瞎的**,把节点级崩溃导致的文件不一致报成回放缺陷。

### 14.2 阶段计划

| 阶段 | 内容 | 验收 | 前置 |
|------|------|------|------|
| R1 物理回放闭环 | fileset 化捕获(含索引/TOAST,**含 §5.2 的 RM_SMGR main-data 特判**);补丁 0002;replay worker(**worker 池 + §13.10 排他认领**):decode→remap→盖 orig_lsn→rm_redo(兼容 v2 段流,XACT 原始记录跳过);apply checkpoint(无 xid_map);skip 白名单;`XLogHaveInvalidPages` 审计 | 带索引 + TOAST 的表,leader 写入后 follower 文件与 leader 在**内核 `heap_mask()` 掩码之外逐字节一致(且 `pd_lsn` 不掩,须相同)**(见下方 ★);kill -9 worker 后重启追平且仍一致;**用例须显式制造一次 VACUUM 尾部截断**(否则 §5.2 的 SMGR 洞测不出来) | — |
| R2 事务层 | parwal-3.0(gxid 头 + TSO 标记 + 子事务列表);xid_map + 快照;`max_replayed_fxid` + nextXid 拉齐;增强型 CLOG 写路径;冻结账目核查(§13 约束 5) | 提交事务 COMMITTED、中止/子事务回滚 ABORTED/缺失;崩溃后 xid_map 与 CLOG 幂等重建;`pg_gclog` 内容与 leader 事务历史一致(**★★ 见下方「R2 验收 = 账本正确，不是可见」**) | R1 |
| D1 DDL/fileset 控制通道 | CTRL 记录格式 + `PartWALAppendCtrl`;locmap v2(加 `role`/`ord`);leader 侧 `ProcessUtility_hook` 检测 → PRE_COMMIT 发射 `FILESET_UPDATE` + `log_newpage_range` 灌新文件;follower 侧换表/截断 与 `REPLAY_NEEDS_STRUCT` 结构栅栏(§12) | `VACUUM FULL`/`REINDEX`/`TRUNCATE` 全自动追平且页面比对仍一致;`CREATE INDEX` 停在栅栏(游标不推进、locmap 未换),补齐本地结构 + 重跑 `replay_set_locmap()` 后原地继续;未同步结构的另一 follower 必须仍停住 | R1 |
| D2 冻结账目同步 | `CTRL:FREEZE_UPDATE`;leader 侧时间驱动检测(autovacuum 不走 ProcessUtility)+ 持久化基线 diff + **尽力而为**发射;follower 侧 worker 发布到槽位、`replay_catchup` 调用方写 `pg_class`(§13 约束 5) | follower 壳表的 `relfrozenxid` 由建表初值变为 leader 的值、`age()` 有界;leader 再次 VACUUM 后能重新同步;账目未变时不重复发射 | D1 |
| R3 可见性接口 | 路由表 + xid_map 迁 dshash 共享化;`PartDistResolveGxid`/`HeapTupleSatisfiesGlobalMVCC` 实装(**另行立项,MVCC 文档定稿后启动**) | 两个 leader 的 shard 副本同居一 follower,交叉提交/回滚可见性正确 | R2 + **全局 MVCC 文档定稿** |
| R4 提升 | 补丁 0003(**含 §11 的归档/`max_wal_size`/级联备库三项处置结论**);§11 六步收尾;旧 leader 归队 | 杀 leader → follower 提升 → 继续读写 → 旧 leader 归队追平,全程数据一致;升主后重启,W 从 checkpoint 恢复,判定不漂移 | **R3(硬阻断,见下)** |

> **★ "逐页字节级一致"必须排除页内空闲空洞(R1 实测修正)。** v3 稿写的
> "逐页 diff 一致(含 LSN 域)"**不可达**,原因不在回放而在 FPI 机制本身:
> 带 `BKPIMAGE_HAS_HOLE` 的全页镜像只搬 `[0, pd_lower)` 与 `[pd_upper, BLCKSZ)`
> 两段,`RestoreBlockImage` 恢复时把中间空洞 **`MemSet(..., 0, hole_length)` 清零**,
> 而主库那片区域保留着被删元组的残字节 ⇒ 两侧在空洞内必然不同。
> **原生流复制备库同样如此**,与本设计无关。
>
> 实测数据(R1 第 8 轮,1 主 2 从,300 行 + TOAST + PK 索引 + 一次 VACUUM 尾部截断):
> 主堆 2 页共 4356 字节不同,**其中 4356 字节全部落在 `[pd_lower, pd_upper)` 内,
> 洞外差异为 0**;页头(含 `pd_lsn`)、行指针、元组数据、special 区逐字节一致;
> VM fork 整文件一致。
>
> **第二处例外:`pd_prune_xid`**(L1 实测补入)。`heap_xlog_prune()` 里内核自己写着:
> "Note: we don't worry about updating the page's prunability hints. At worst
> this will cause an extra prune cycle to occur soon." —— **redo 故意不复制它**。
> 它只是"这页可能有东西可清理"的优化提示,主备分歧无害。实测 leader 侧 VACUUM
> 后归 0、follower 侧保留 prune 前的旧值(6 字节差异,3 页各 2 字节)。
>
> **判据改为对齐内核自己的掩码规则**(R2 实测重订)。上面两处例外是逐次撞出来的,
> 到 R2 又撞上第三处 —— 主堆稳定报 4 字节差异,查下来全部落在**元组之间的
> MAXALIGN 对齐填充**里(实测:`lp[190]` 覆盖 `[4624,4677)`,`MAXALIGN(53)=56`,
> 填充区 `[4677,4680)`,差异字节正是 4677)。元组按 8 字节对齐存放,而 `lp_len`
> 是元组真实长度,中间那几个字节谁也不读、redo 也不写,主备残留内容不同是常态。
>
> 与其继续一处处补,不如直接采用**内核既有的权威答案**:PostgreSQL 的
> `wal_consistency_checking` 就是用来验证"redo 出来的页与主库页是否一致"的,
> 它对堆页调用 `heap_mask()`(`src/backend/access/heap/heapam.c`)配合
> `bufmask.c` 的 `mask_page_lsn_and_checksum` / `mask_page_hint_bits` /
> `mask_unused_space`,把主备之间本就不保证相同的字段统一涂掉再比。
> `tests/pagecmp.py` 照抄这份掩码集合:
>
> | 内核掩掉的 | 原因 | 本项目 |
> |---|---|---|
> | `pd_lsn` / `pd_checksum` | 备库页 LSN 与主库无关 | **不掩**,见下 |
> | `pd_prune_xid` | `heap_xlog_prune()` 原文"we don't worry about updating the page's prunability hints" | 掩 |
> | `pd_flags` 的 `PD_HAS_FREE_LINES`/`PD_PAGE_FULL`/`PD_ALL_VISIBLE` | 三个都是提示位 | 按位掩,其余位严格比 |
> | `[pd_lower, pd_upper)` 空洞 | FPI 恢复清零,主库留残字节 | 掩 |
> | 未冻结元组的 `t_infomask & HEAP_XACT_MASK`(0xFFF0) | 可见性提示位由读取者写,不产生 WAL | 掩;掩码外的位单独比 |
> | 已冻结元组的 `HEAP_XMAX_INVALID`/`HEAP_XMAX_COMMITTED` | 同上 | 掩 |
> | `t_cid` | 回放时被置成 `FirstCommandId`(见 `heap_xlog_insert`) | 掩 |
> | 每条行指针后的 `MAXALIGN(lp_len) - lp_len` 填充 | 无人读、redo 不写 | 掩 |
>
> **本项目刻意比内核更严的一条:`pd_lsn` 不掩。** R1 的核心主张就是 follower 用
> leader 的 `orig_lsn` 盖页(§4.2/§8.2),页 LSN 必须逐字节相同 —— 这正是本设计
> 区别于普通逻辑复制的地方,不能跟着内核一起放过。
>
> **另加一条内核不需要、本项目必要的检查**:leader 已冻结(`t_infomask & 0x0300
> == 0x0300`)而 follower 未冻结 → 判为差异。冻结记录 `XLOG_HEAP2_FREEZE_PAGE`
> 是**写 WAL** 的,丢了它是真缺陷,不能被"提示位豁免"一起放过。
>
> 修正后的判据:两侧 `pd_lower`/`pd_upper` 必须相同(否则"洞"的位置不可比),
> 且**上表掩码之外的全部字节相同**。判据仍覆盖 `pd_lsn`、全部行指针、
> 全部元组数据(含 xmin/xmax 本身、`t_infomask2`、`t_ctid`、`t_hoff`)与 special 区。
> 掩码规则是**堆页专用**的(要走行指针与元组头布局);索引页需另配
> `btree_mask` 等,现工具不适用。**而用例目前把它也用在索引文件上** ——
> 它会照 `HeapTupleHeaderData` 的布局去掩元组内偏移 20–22(`t_infomask`)与
> 8–12(`t_cid`),那些位置在 `IndexTupleData` 里是**真正的键数据**。
> 后果不是误报是**漏报**:每条索引元组白送 6 字节豁免。待办。

> **★ 判据本身要有"确实跑过"的守卫(D1 实测教训)。** 逐成员比对的循环
> 用 `while read ... <<< "$rows"` 驱动,而循环体里的 `docker exec -i`
> **会把循环自己的标准输入一并吞掉** —— 第一行之后的 fileset 成员被静默跳过。
> 表现是全 PASS,但实际只比了主堆,索引与 TOAST 一个字节都没验。
>
> 这类失败的共性是"零个检查会静默通过":路径解析失败、循环提前退出、
> 关系名对不上,统统表现为少几条 PASS 而不是一条 FAIL。所以逐成员比对
> **必须配一条计数守卫**(实际比对的文件数 >= fileset 成员数)。
> 修法:成员清单先 `mapfile` 进数组再 for 循环,容器调用一律 `</dev/null`。
>
> 顺带一条:路径不能用 `relnum::regclass` 反查 —— `relnum` 是 **relfilenode**,
> 与关系 OID 只在关系刚建好时碰巧相等,leader 一做 `VACUUM FULL`/`REINDEX`/
> `TRUNCATE` 就分家(这正是 §12 的常规场景),要走 `pg_filenode_relation()`。

> **★★ 已定位并修复:崩溃恢复后 btree 元页(metapage)陈旧**(2026-08-04,
> 修好上面那个守卫之后**第一次**比到索引文件就暴露出来;此前索引从未被比过)。
> 根因与修法见 §13 约束 12,下面保留当时的现象记录。
>
> 实测(R1 用例,1 主 2 从,同一份 leader 文件):
> - **f2**(全程无崩溃)6 个文件**全部**逐字节一致,含 PK 索引与 TOAST 索引。
> - **f1**(走了 §14.2 R1 行的 kill -9 → 节点重置 → 从 durable 游标续放)
>   两个索引的 **page 0 = 元页**不一致,堆/TOAST 堆/VM fork 全部一致:
>
>   | 偏移 | 字段(`BTMetaPageData`) | leader | f1 |
>   |---|---|---|---|
>   | 4–7 | `pd_lsn.xrecoff` | 非零 | 陈旧值 |
>   | 32 | `btm_root` | 3 | **0** |
>   | 36 | `btm_level` | 1 | **0** |
>   | 40 | `btm_fastroot` | 3 | **0** |
>   | 44 | `btm_fastlevel` | 1 | **0** |
>
>   全零正是 `_bt_initmetapage()` 写下的初值 ⇒ f1 的元页**停在建索引那一刻**,
>   `XLOG_BTREE_NEWROOT`(或带元页更新的 split)的效果丢了。页 LSN 也陈旧,
>   说明这一页在 f1 上**根本没被 redo 写过**。
>
> 元页 `btm_root = 0` 意味着索引在升主后不可用(`_bt_getroot` 会认为树是空的)。
> 这不是掩码问题,`btree_mask()` 不掩元页的这些字段。
>
> **根因不在 §8.4 的推进协议**(那是最初的猜测,受控实验推翻了它),
> 而是"两条 redo 流互不知情" —— 见 §13 约束 12。
>
> **这正是 R1 的 kill -9 用例本该抓住的场景** —— 抓不住的唯一原因是当时的
> 页面比对只比了主堆(见上一条 ★)。

> **★★ R2 验收 = 账本正确，不是可见。** 这条要写死,免得反复误判进度。
>
> R2 交付后,follower 上**仍然读得出被回滚的行** —— 实测 leader 5 行、follower 6 行,
> 多出来的正是被 `ROLLBACK TO SAVEPOINT` 掉的那个子事务写的。**这不是缺陷**:
> 元组的 `xmin` 是 leader 的 32 位 xid,follower 拿它去查**自己的**原生 clog,
> 查到的是它自己历史上那个碰巧同号的事务(实测 follower 本地 xid 计数器 59529,
> 而元组 xmin 才 4002~4008)。同一个 32 位数字在两个节点上是两笔毫不相干的事务 ——
> 这正是 gxid 存在的全部理由。
>
> R2 要证明的是**账记对了**,不是**有人查对了账**:
>   - `pg_gclog` 里顶层与已 RELEASE 的子事务判为 `committed`,被 `ROLLBACK TO`
>     的那个**没有记录**(空洞 = `TXN_RUNNING` = 未决 = 不可见 —— 回滚语义由
>     "缺席"表达,不写 `ABORTED`);
>   - kill -9 之后段流里每条 COMMIT 标记在 gclog 里仍是 `committed`(§8.4 推进协议)。
>
> 这两条 `tests/test_txn_layer_r2.sh` 都**直接查账**断言(`partdist.gclog_status()`),
> 不靠"多读出几行"这类间接现象。
>
> 让 `SELECT` 去查 gclog 是 **R3** 的事(`HeapTupleSatisfiesGlobalMVCC` +
> xid_map 迁 dshash 共享化)。R3 落地那天,上面那 6 行会变回 5 行 —— 那才是
> 可见性的验收信号,不属于 R2。
>
> 相应地,`tests/test_lazy_replay_l1.sh` 里那句 `VACUUM (FREEZE)` 已在 R2-f 去掉,
> 改为普通事务批次 + **普通** VACUUM:FREEZE 当初是**页面比对的拐杖**(freeze 记录
> 整体重写 `t_infomask`,把 leader 扫描期设上的提示位洗成 canonical 状态),判据
> 对齐 `heap_mask()` 之后不再需要。保留普通 VACUUM 是为了不丢 VM fork 的比对覆盖 ——
> `vacuum_freeze_min_age` 默认 5000 万,用例里的元组一个都够不着,不会被冻结。
> (`test_follower_replay_r1.sh` 仍保留 `VACUUM (FREEZE)`:它那条"壳表行数一致"
> 的内容校验**确实**依赖冻结元组可读,那是 R1 阶段的既定边界,用例注释已写明。)

> **★ R4 硬阻断于 R3,不是"先后"而是"依赖"。** 升主后该 shard 上的元组 xmin/xmax
> 是**旧 leader 的本地 xid**,读它们必须走 §9.4 路由规则 3(`xid ≤ W` 查该分区
> xid_map),而规则 3 的实装在 R3。R3 未落地时,promoted shard 上的任何读都会让原生
> `HeapTupleSatisfiesMVCC` 去查本地 clog——那里对这些 xid 是空洞(§13.4)。
> 所以 R4 验收标准里的"**继续读写**"在 R3 之前不可能达成,R4 最多做到"可写不可读"。
>
> 而 R3 自身的启动条件(全局 MVCC 文档定稿)**在本文档控制范围之外** ⇒ 整条 failover
> 路径的关键路径长度取决于那份文档。立项排期时必须把它当作外部阻塞项显式列出,
> 不要当成可与 R1/R2 并行推进的独立工作。
>
> 若需要在 R3 之前就拿到可读的 failover,唯一的绕法是**升主时冻结全部回放元组**
> (把 xmin 置 `HEAP_XMIN_FROZEN`,可见性不再依赖 xid 解析)——代价是升主时间与
> shard 大小成正比,且丢失 MVCC 历史。本文档不推荐,仅记录为可选逃生门。

---

## 15. 与 reply_v1(评审文档)条目对照

| reply_v1 | 本文 | 差异说明 |
|----------|------|----------|
| 3.2 PartWALRecord / TxnMarkerPayload | §4 | **已回同步(R2-f)**:marker 增加 `nsubxacts` + 子事务列表;`TxnMarkerPayload` 为 **24 字节**(含显式 `reserved`),非 20 —— 原文的 20 与它自己 §7.6 的 `sizeof(TxnMarkerPayload)` 长度校验自相矛盾。`PartWALRecord` 保持 40 字节、`gxid` 在偏移 32,与 2.0 逐字节等长 |
| 3.2 ShardFileSet / loc_map | §5 / §7.1 | 一致 |
| 3.2 xid_map 作用域与持久化 | §9.2 / §8.4 | 快照并入 apply_checkpoint 单文件(原子性实现方式) |
| 3.2 ShardRouteEntry / 增强型 CLOG / ShardReplayCtx / ShardApplyCheckpoint | §9.4 / §9.3 / §7.1 / §8.4 | `max_replayed_xid`(32 位)→ `max_replayed_fxid`(64 位)并持久化;checkpoint 增加 CRC 与恢复提示字段。**R2-d 回同步**:`EnhancedClogRecord` 槽长由 16 改 24 字节 —— 原文的 6 字节时间戳是 48 位,放不下 TimestampTz(2^48 µs ≈ 8.9 年,2008 年即溢出;实测当前值是 2^48 的三倍),截断会悄悄弄乱提交顺序,而 §10 的可见性判据正是 `commit_ts ≤ 快照 start_ts` |
| 3.3 前置条件 + 五阶段 | §7 前置条件 + §7.2~§7.9 | 水位推进原语:`SetTransactionIdLimit()` → `PartDistAdvanceNextXidPastXid()`(前者仅设防回卷限,不推进 nextXid,评审文档待更正) |
| 3.3 末尾路由规则 | §9.4 | 一致(三条规则原文保留) |
| 3.4 回放示例 A~D | — | 示例不重复收录,以 §7 代码骨架为准 |
| 3.5 工程审查问答 | §9.2 / §9.4 | 结论一致 |

---

## 附录 A:WAL 记录覆盖矩阵(follower 侧处理方式)

| rmid / info | 处理 | 说明 |
|-------------|------|------|
| HEAP: INSERT/DELETE/UPDATE/HOT_UPDATE/LOCK/INPLACE | `rm_redo` | 表、TOAST 堆 DML |
| HEAP: TRUNCATE | 跳过 | 仅服务逻辑解码,heap_redo 中即为 no-op;物理截断走 SMGR |
| HEAP2: MULTI_INSERT/PRUNE/VACUUM/FREEZE_PAGE/VISIBLE | `rm_redo` | 含 VM 位与 HOT 清理;多数无有效 xl_xid,不登记 xid_map |
| HEAP2: NEW_CID | 跳过 | 仅服务逻辑解码,对页面无影响 |
| HEAP2: REWRITE | 跳过 | logical rewrite 映射,物理副本无关 |
| BTREE: INSERT_*/SPLIT_*/DEDUP/VACUUM/DELETE/MARK_PAGE_HALFDEAD/UNLINK_*/NEWROOT | `rm_redo` | 普通索引、TOAST 索引 |
| BTREE: REUSE_PAGE | 跳过 | 仅用于 standby 查询冲突,不改页面 |
| XLOG: FPI / FPI_FOR_HINT | `rm_redo` | 全页镜像覆盖页面;xl_xid 无效,不登记 xid_map |
| SMGR: TRUNCATE | `rm_redo`,**main-data 特判改写** | `xl_smgr_truncate` 的 `RelFileLocator` 在记录 main data 而非 block ref,块引用重映射覆盖不到,须在 decoded main_data 副本中就地改写后再派发 |
| SMGR: CREATE | 特判 | 不直接 redo,走 §12 fileset 流程 |
| XACT: COMMIT / ABORT | 标记记录(§7.6) | parwal-3.0 不进 DATA 流;v2 存量段中的原始 XACT 记录跳过 |
| 其余(STANDBY/RELMAP/DBASE/...) | 不捕获 | 不落在 shard 文件上,fileset 过滤天然排除 |

跳过逻辑集中于 `ShardReplaySkippable(rmid, info)` 白名单函数;凡未知 rmid/info
组合一律 PANIC(宁可 fail-fast,不静默漏回放)。
