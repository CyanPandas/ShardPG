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
    /* TransactionId subxacts[nsubxacts] 紧随其后(leader 侧本地 xid) */
} TxnMarkerPayload;       /* data_len = 20 + 4 * nsubxacts */
```

- `flags` 含 `PARTWAL_FLAG_MARKER`;`rmid = RM_XACT_ID`;
  `info = XLOG_XACT_COMMIT` 或 `XLOG_XACT_ABORT`(取 `info & XLOG_XACT_OPMASK` 判别)。
- `gxid`(头部)= 顶层事务的全局事务号;子事务的 gxid 由 follower 按
  `MakeGlobalXid(leader_node_id, subxid)` 合成(§7.6)。
- 与 reply_v1 的 16 字节定长相比多出 `nsubxacts` 与子事务数组:这是为覆盖
  SAVEPOINT 语义的**有意格式增量**(§9.5),需回同步到评审文档。

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

### 7.7 阶段三 C:应用 CTRL 记录

目前仅 `FILESET_UPDATE`(§12):按新 fileset 全量描述更新 loc_map 与
(未来的)路由表条目,处理完毕才继续后续 DATA 记录——控制记录之后的 DATA 才会引用
新 relfilenode,apply 串行 ⇒ 无竞态。

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
`pg_gclog/<node_id>/`,段文件内按低位 xid 直接寻址 16 字节槽;写路径
write + 延迟 fsync,fsync 时机由 §8.4 推进协议步骤 2 保证。SLRU 化、缓冲与 GC
属全局 MVCC 文档范围。

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
2. **追平**:循环 §7 阶段二~四,直至 `applied_part_lsn` 达到组内 committed 上界。
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

## 12. DDL 与文件集合维护

改变 relfilenode 集合的操作:`CREATE/DROP INDEX`、`REINDEX`、`VACUUM FULL`、
`TRUNCATE`、`ALTER TABLE` 重写类。shard 上的 DDL 必须走集群协调通道,流程:

1. leader 执行 DDL → 刷新本地 fileset 与反向映射;
2. 向该 shard 的 ParWAL 流追加 `FILESET_UPDATE` 控制记录(新 fileset 全量描述,
   含各关系角色与索引定义序),随 Raft 复制;
3. follower apply 到该记录:执行等价物理结构变更,更新 loc_map 与路由表条目
   (`PartDistRouteUpdateFileset`,新旧文件号在同一临界区换表)。
   顺序保证:控制记录之后的 DATA 才会引用新 relfilenode,apply 串行 ⇒ 无竞态。

REINDEX/VACUUM FULL 在 leader 侧产生的新文件内容本身以 FPI/记录形式进流,数据量大;
早期阶段可将这类 DDL 降级为"触发该 shard 副本重新做物理基线拷贝",简单可靠。

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
   的 freeze 记录回放实现)。本地 `datfrozenxid` 计算须排除副本壳表,或经控制记录
   同步 leader 的 relfrozenxid;同时 nextXid 被最活跃 leader 拉齐后,本地普通表的
   age() 相应增大,须确认 autovacuum 对本地表正常 freeze。R2 验收项。
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

---

## 14. 代码落点与分阶段计划

### 14.1 新增/修改文件

```
pg-partdist-src/
  include/
    partition_wal_header.h     [改] parwal-3.0:gxid 头、MARKER/CTRL flags、TxnMarkerPayload
    shard_fileset.h            [新] ShardFileSet + 反向映射接口(§5)
    shard_replay.h             [新] ShardReplayCtx、LocMapEntry、回放入口、边界回调(§6/§7)
    shard_xidmap.h             [新] XidMapEntry、xid_map 接口(§9.2)
    shard_route.h              [新] ShardRole、ShardRouteEntry、路由接口(§9.4)
    enhanced_clog.h            [新] TxnStatus、EnhancedClogRecord、读写接口(§9.3)
    global_mvcc.h              [新] GlobalTransactionId、MakeGlobalXid、预留读接口(§10)
  src/replay/
    replay_worker.c            [新] launcher + per-shard bgworker、InRecovery、
                                    PartDistAdvanceNextXidPastXid
    shard_replay.c             [新] 五阶段主循环、DATA/MARKER/CTRL 三路派发、skip 白名单
    xid_map.c                  [新] HTAB 实现 + 快照 dump/load(R3 迁 dshash)
    replay_checkpoint.c        [新] apply_checkpoint 单文件原子读写(§8.4)
    shard_route.c              [新] 共享内存路由表(注册/换表/promote;读侧 stub)
    enhanced_clog.c            [新] 写路径最小实现;读路径 stub
  patches/
    0002-flushbuffer-lsn-exempt-hook.patch   [新] §8.3
    0003-advance-wal-insert.patch            [新] §11(R4)
```

GUC(前缀沿用 `pg_partdist.`):`replay_workers`(worker 池大小,默认 4,§7)、
`replay_naptime_ms`、`replay_checkpoint_interval_ms`、
`replay_checkpoint_bytes`、`replay_trust_local_segments`(测试模式,§6)、
`replay_dw_enabled`(sidecar,§8.5)。

### 14.2 阶段计划

| 阶段 | 内容 | 验收 | 前置 |
|------|------|------|------|
| R1 物理回放闭环 | fileset 化捕获(含索引/TOAST,**含 §5.2 的 RM_SMGR main-data 特判**);补丁 0002;replay worker(**worker 池 + §13.10 排他认领**):decode→remap→盖 orig_lsn→rm_redo(兼容 v2 段流,XACT 原始记录跳过);apply checkpoint(无 xid_map);skip 白名单;`XLogHaveInvalidPages` 审计 | 带索引 + TOAST 的 pgbench 表,leader 写入后 follower 文件与 leader **逐页 diff 一致(含 LSN 域)**;kill -9 worker 任意时刻,重启追平且 diff 一致;**用例须显式制造一次 VACUUM 尾部截断**(否则 §5.2 的 SMGR 洞测不出来) | — |
| R2 事务层 | parwal-3.0(gxid 头 + TSO 标记 + 子事务列表);xid_map + 快照;`max_replayed_fxid` + nextXid 拉齐;增强型 CLOG 写路径;冻结账目核查(§13.5) | 提交事务 COMMITTED、中止/子事务回滚 ABORTED/缺失;崩溃后 xid_map 与 CLOG 幂等重建;`pg_gclog` 内容与 leader 事务历史一致 | R1 |
| R3 可见性接口 | 路由表 + xid_map 迁 dshash 共享化;`PartDistResolveGxid`/`HeapTupleSatisfiesGlobalMVCC` 实装(**另行立项,MVCC 文档定稿后启动**) | 两个 leader 的 shard 副本同居一 follower,交叉提交/回滚可见性正确 | R2 + **全局 MVCC 文档定稿** |
| R4 提升 | 补丁 0003(**含 §11 的归档/`max_wal_size`/级联备库三项处置结论**);§11 六步收尾;旧 leader 归队 | 杀 leader → follower 提升 → 继续读写 → 旧 leader 归队追平,全程数据一致;升主后重启,W 从 checkpoint 恢复,判定不漂移 | **R3(硬阻断,见下)** |

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
| 3.2 PartWALRecord / TxnMarkerPayload | §4 | marker 增加 nsubxacts + 子事务列表(§9.5,需回同步) |
| 3.2 ShardFileSet / loc_map | §5 / §7.1 | 一致 |
| 3.2 xid_map 作用域与持久化 | §9.2 / §8.4 | 快照并入 apply_checkpoint 单文件(原子性实现方式) |
| 3.2 ShardRouteEntry / 增强型 CLOG / ShardReplayCtx / ShardApplyCheckpoint | §9.4 / §9.3 / §7.1 / §8.4 | `max_replayed_xid`(32 位)→ `max_replayed_fxid`(64 位)并持久化;checkpoint 增加 CRC 与恢复提示字段 |
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
