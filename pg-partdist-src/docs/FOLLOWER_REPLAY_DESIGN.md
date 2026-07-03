# pg_parwal 从副本物理重放设计与参考实现

> 目标:在 follower 上**复用 PostgreSQL 核心 redo 机制(`rm_redo`)** 重放 per-shard 的 ParWAL 物理日志流,
> **不把物理日志反解析成 SQL**。核心思路:`DecodeXLogRecord` → 改写 `RelFileLocator` → `rm_redo`,
> heap 应用逻辑一行不动。
>
> 代码定位基于 `postgres-src`(REL_16_STABLE)。本文件为设计与参考实现,**不代表 src/ 下已落地的代码**。

---

## 0. 背景与前提

- 采用 Hash 分区,**每个分片(shard)= 一个独立 Raft 组**;组内 1 个 leader(可写主)+ 若干 follower。
- 每个分片在每个节点上是一张普通堆表,表名形如 `<tablename>_<shardid>`,**拥有独立的 OID 和 relfilenode**。
- `pg_parwal/<partition_id>/` 段文件 = 该分片 Raft 组的日志流;`partition_lsn` = 该组的 Raft log index。
- leader 侧 `wal_insert_hook` 捕获原始 `XLogRecord` 字节存入 ParWAL;PRE_COMMIT 时按分片分组落盘并作为
  Raft entry 复制(prepare 阶段 `[A]`)。

### PG 物理 WAL 回放路径(代码定位,REL_16_STABLE)

```
PerformWalRecovery()                                  xlogrecovery.c
  └─ ApplyWalRecord(xlogreader, record)               xlogrecovery.c:1867
       └─ GetRmgr(record->xl_rmid).rm_redo(xlogreader)          :1951   ← 按 rmid 派发
            └─ heap_redo(record)                       heapam.c:10527    ← RM_HEAP
                 heap_xlog_insert / _delete / _update            heapam.c:9781/9708/10047
                      └─ XLogReadBufferForRedo(record, id, &buf)  xlogutils.c:317
                           └─ XLogReadBufferForRedoExtended       xlogutils.c:354
                                └─ XLogReadBufferExtended(rlocator, fork, blkno...) :473
                                     └─ smgropen(rlocator)        xlogutils.c:494  ← 按物理文件号打开
```

三个关键事实:

1. **redo 完全由解码后的 `XLogReaderState` 驱动**,不碰 SQL、不碰执行器。`heap_xlog_insert` 就是从 record
   取出 block tag 和 tuple 字节 `PageAddItem` 进页面(`heapam.c:9846-9869`)。这正是要复用的。
2. **redo 是物理块级的**。每条 record 的 block 引用携带 `RelFileLocator{spcOid,dbOid,relNumber}` +
   `BlockNumber`,redo 用它 `smgropen(rlocator)` 打开物理文件、定位 `blkno/offnum` 改字节。
3. **幂等性靠页面 LSN**:`if (lsn <= PageGetLSN(page)) return BLK_DONE`(`xlogutils.c:434`)。

复用注入点:**`DecodeXLogRecord` 之后、`rm_redo` 之前,`DecodedXLogRecord.blocks[i].rlocator` 可写**,
把 leader 的 relfilenode 改成 follower 本地的即可。

---

## 1. 共享:pg_parwal 段记录格式

leader 写、follower 读的统一格式。在原 32 字节 header 上,把 `_pad` 借 1 字节做 `rec_type`
(区分 DATA / COMMIT / ABORT)。

```c
/* parwal_format.h —— leader 与 follower 共用 */

#define PARTWAL_MAGIC   0x50415254      /* "PART" */

typedef enum PartWALRecType
{
    PARTWAL_REC_DATA   = 0,   /* body 是一条原始 XLogRecord(heap 字节) */
    PARTWAL_REC_COMMIT = 1,   /* 2PC 提交标记,body 是 xid 列表 */
    PARTWAL_REC_ABORT  = 2,   /* 2PC 回滚标记 */
} PartWALRecType;

typedef struct PartWALRecHeader     /* 固定 32 字节,落在每条记录最前面 */
{
    uint32      magic;          /* PARTWAL_MAGIC,完整性校验 */
    uint32      partition_id;   /* 所属 shard 的“逻辑” OID(目录名) */
    uint64      orig_lsn;       /* leader pg_wal 中的原始 LSN(交叉校验) */
    uint64      partition_lsn;  /* 分区内单调递增序号 = 该 shard Raft log index */
    uint8       rmid;           /* 原始 XLogRecord 的 rmid(冗余,便于过滤) */
    uint8       info;           /* 原始 xl_info(冗余) */
    uint8       rec_type;       /* PartWALRecType(原 _pad[0]) */
    uint8       _pad;
    uint32      data_len;       /* 紧随其后的 body 字节数 */
} PartWALRecHeader;

StaticAssertDecl(sizeof(PartWALRecHeader) == 32, "PartWALRecHeader must be 32 bytes");
```

DATA 记录的 `body` = **一条完整的原始 `XLogRecord`**(从 `xl_tot_len` 开头那 24 字节起,后跟 block headers
+ data),即 leader 侧 `wal_insert_hook` 里直接 `memcpy` 出来的字节。

---

## 2. leader 侧捕获(简版,说明 body 从哪来)

重点:**整条原始 record 字节,不做任何解析**。

```c
/* 在 wal_insert_hook 里(XLogInsert 生成 record 之后) */
static void
PartWALCapture(XLogRecPtr lsn, XLogRecData *rdata, RmgrId rmid, uint8 info)
{
    Oid     shard;
    char   *recbuf;
    uint32  reclen;

    /* 1) 由 block 引用里的 relfilenode 反查所属 shard;命不中直接返回 */
    if (!PartWALResolveShard(rdata, &shard))
        return;

    /* 2) 把这条 record 的原始字节线性化(XLogRecData 链 -> 连续 buffer) */
    recbuf = PartWALLinearizeRecord(rdata, &reclen);   /* 含 XLogRecord 头 */

    /* 3) 只写共享内存环形缓冲区的一个描述符:{shard, lsn, recbuf, reclen, rmid, info}
     *    —— 纯内存,不落盘、不复制(落盘/复制在 PRE_COMMIT 的 Flush 里做) */
    PartWALPushSlot(shard, lsn, recbuf, reclen, rmid, info);
}
```

PRE_COMMIT 的 `PartWALFlush` 把描述符按 shard 分组,每条加 `PartWALRecHeader` 写进 `pg_parwal/<shard>/`
段、fsync,并作为 Raft entry 复制(prepare 阶段 `[A]`)。

---

## 3. follower 侧:每个 shard 的 apply 上下文 + relfilenode 重映射

```c
/* parwal_apply.c */

#include "postgres.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "access/rmgr.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/storage_xlog.h"
#include "storage/bufmgr.h"
#include "utils/relmapper.h"

/*
 * 一个 shard 的重放上下文。每个 shard 是独立 Raft 组,follower 上用一个
 * apply worker 串行重放它的 ParWAL 段流。
 */
typedef struct ShardApplyCtx
{
    Oid             shard_oid;       /* 逻辑 shard OID(= partition_id) */

    /* leader 上该 shard 各物理关系的 relfilenode  ->  本地 relfilenode 的映射。
     * 至少包含:heap 主关系;若有 toast / index,各占一项。
     * 用 leader 的 RelFileLocator 做 key,因为 record 里携带的就是它。 */
    HTAB           *loc_map;         /* RelFileLocator -> RelFileLocator */

    XLogReaderState *reader;         /* 复用的解码器,见 §4 */
    XLogRecPtr      apply_lsn;       /* 本地为重放分配的单调 LSN,见 §7 注意点 */
    uint64          last_part_lsn;   /* 已重放到的 partition_lsn,gap/幂等游标 */
} ShardApplyCtx;

typedef struct LocMapEntry
{
    RelFileLocator  leader_loc;      /* hash key */
    RelFileLocator  local_loc;
} LocMapEntry;

/*
 * 把一条已解码记录里所有 block 引用的 RelFileLocator,从 leader 的物理文件号
 * 改写成本地文件号。这是整套“复用 redo”方案唯一需要的改写点。
 */
static void
RemapDecodedLocators(DecodedXLogRecord *decoded, ShardApplyCtx *ctx)
{
    for (int id = 0; id <= decoded->max_block_id; id++)
    {
        DecodedBkpBlock *blk = &decoded->blocks[id];
        LocMapEntry     *e;
        bool             found;

        if (!blk->in_use)
            continue;

        e = hash_search(ctx->loc_map, &blk->rlocator, HASH_FIND, &found);
        if (!found)
            ereport(ERROR,
                    (errmsg("parwal: shard %u 收到未知 relfilelocator %u/%u/%u",
                            ctx->shard_oid,
                            blk->rlocator.spcOid, blk->rlocator.dbOid,
                            blk->rlocator.relNumber)));

        /* 关键:覆盖文件标识。forknum / blkno / 数据 / FPI 全部保持不变,
         * 因为 follower 是该 shard 的物理拷贝,页布局与 leader 一致。 */
        blk->rlocator = e->local_loc;
    }
}
```

`loc_map` 怎么建:follower 接管/创建某 shard 副本时,对该 shard 文件做一次物理 base copy 作为基线
(见 §7 约束②),同时记录 `leader_relfilenode -> local_relfilenode` 存进 `loc_map`。

---

## 4. follower 侧:复用 PG redo 重放一条 DATA 记录(核心)

`DecodeXLogRecord` → 重映射 → `rm_redo`,**完全复用 heap_redo**。

```c
/*
 * 重放一条 ParWAL DATA 记录(body 是原始 XLogRecord 字节)。
 */
static void
PartWALRedoDataRecord(ShardApplyCtx *ctx, XLogRecord *record, uint64 part_lsn)
{
    XLogReaderState   *reader = ctx->reader;
    DecodedXLogRecord *decoded;
    size_t             sz;
    char              *errmsg;
    XLogRecPtr         lsn;

    /* 0) gap / 幂等:partition_lsn 必须严格 +1;已重放过的直接跳过 */
    if (part_lsn <= ctx->last_part_lsn)
        return;                                  /* 重复投递,幂等丢弃 */
    if (part_lsn != ctx->last_part_lsn + 1)
        ereport(PANIC,
                (errmsg("parwal: shard %u 日志空洞: 期望 %lu 实际 %lu",
                        ctx->shard_oid, ctx->last_part_lsn + 1, part_lsn)));

    /* 1) 为这条记录在本地分配一个单调 LSN 用于盖页(见 §7 注意点) */
    lsn = ++ctx->apply_lsn;

    /* 2) 解码:把原始字节解成 DecodedXLogRecord。
     *    decoded 自己分配,DecodeXLogRecord 只往这块 buffer 里写。 */
    sz = DecodeXLogRecordRequiredSpace(record->xl_tot_len);
    decoded = (DecodedXLogRecord *) palloc(sz);

    if (!DecodeXLogRecord(reader, decoded, record, lsn, &errmsg))
        ereport(PANIC, (errmsg("parwal: 解码失败: %s", errmsg)));

    /* 3) 文件号重映射:leader relfilenode -> 本地 relfilenode */
    RemapDecodedLocators(decoded, ctx);

    /* 4) 把解码结果挂到 reader 上,让 redo 宏 XLogRecGet* / XLogReadBufferForRedo
     *    能读到它。EndRecPtr 就是 redo 用来盖 PageSetLSN 的那个 lsn。 */
    decoded->next_lsn = lsn;
    reader->record    = decoded;
    reader->ReadRecPtr = lsn;
    reader->EndRecPtr  = lsn;

    /* 5) 与标准恢复一致:推进 nextXid,避免后续可见性/回卷问题 */
    AdvanceNextFullTransactionIdPastXid(record->xl_xid);

    /* 6) ★ 复用核心:按 rmid 派发到 heap_redo / heap2_redo ...
     *    heap_xlog_insert/update/delete 内部走 XLogReadBufferForRedo ->
     *    smgropen(本地 rlocator) -> 改对应 blkno/offnum 的字节。 */
    GetRmgr(record->xl_rmid).rm_redo(reader);

    ctx->last_part_lsn = part_lsn;
    pfree(decoded);
}
```

---

## 5. follower 侧:重放 COMMIT / ABORT 标记(让 tuple 可见)

光重放 heap 记录,tuple 的 `xmin` 是 leader 的 xid,但 follower 的 clog 还没标 committed → 谁也看不见。
2PC 的 COMMIT 标记到达时,必须复用 PG 标记 clog 的逻辑(等价于 `xact_redo_commit` 里的
`TransactionIdCommitTree`,`xact.c:5995`)。

```c
typedef struct PartWALCommitBody     /* PARTWAL_REC_COMMIT / ABORT 的 body */
{
    TransactionId   xid;
    int             nsubxacts;
    TransactionId   subxacts[FLEXIBLE_ARRAY_MEMBER];
} PartWALCommitBody;

static void
PartWALApplyCommit(ShardApplyCtx *ctx, PartWALCommitBody *c)
{
    /* 复用 PG 的提交树标记:把 xid(及子事务)在 clog 里置为 committed,
     * 这正是 xact_redo_commit() 在物理备库上做的事。此后本 shard 上
     * 这些 xid 写入的 tuple 立即可见。 */
    TransactionIdCommitTree(c->xid, c->nsubxacts, c->subxacts);

    AdvanceNextFullTransactionIdPastXid(c->xid);
    /* 如有需要,这里还可推进本 shard 的 apply 可见点 / 通知等待者 */
}

static void
PartWALApplyAbort(ShardApplyCtx *ctx, PartWALCommitBody *a)
{
    /* prepared 记录已写进页,但事务回滚:标记 aborted,MVCC 自然不可见,
     * 物理上靠后续 vacuum/prune 回收(与单机 abort 行为一致)。 */
    TransactionIdAbortTree(a->xid, a->nsubxacts, a->subxacts);
}
```

---

## 6. follower 侧:段扫描主循环(入口)

```c
/*
 * 顺序扫描一个 shard 段文件的字节流,逐条派发。
 * buf/len 通常来自 Raft 已 commit 的 entry,或本地段文件(crash recovery 补读)。
 */
static void
PartWALApplyStream(ShardApplyCtx *ctx, char *buf, Size len)
{
    char *p   = buf;
    char *end = buf + len;

    while (p + sizeof(PartWALRecHeader) <= end)
    {
        PartWALRecHeader *h = (PartWALRecHeader *) p;
        char             *body;

        /* 完整性:magic 不符 = 段被截断/损坏 -> 停在此处,触发恢复路径 */
        if (h->magic != PARTWAL_MAGIC)
            ereport(PANIC, (errmsg("parwal: shard %u 段损坏(magic 不符)",
                                   ctx->shard_oid)));
        if (h->partition_id != ctx->shard_oid)
            ereport(PANIC, (errmsg("parwal: 段归属错位 %u != %u",
                                   h->partition_id, ctx->shard_oid)));
        if (p + sizeof(PartWALRecHeader) + h->data_len > end)
            break;                               /* body 不完整,等下一批 */

        body = p + sizeof(PartWALRecHeader);

        switch (h->rec_type)
        {
            case PARTWAL_REC_DATA:
                /* body 就是原始 XLogRecord */
                PartWALRedoDataRecord(ctx, (XLogRecord *) body, h->partition_lsn);
                break;

            case PARTWAL_REC_COMMIT:
                PartWALApplyCommit(ctx, (PartWALCommitBody *) body);
                break;

            case PARTWAL_REC_ABORT:
                PartWALApplyAbort(ctx, (PartWALCommitBody *) body);
                break;
        }

        p = body + h->data_len;
    }
}
```

reader 的一次性分配(每个 apply worker 一个):

```c
static XLogReaderState *
PartWALMakeReader(void)
{
    /* 我们从不调用 XLogReadRecord,只用 DecodeXLogRecord + rm_redo,
     * 所以 page_read 等回调可为 NULL。 */
    return XLogReaderAllocate(wal_segment_size, NULL,
                              XL_ROUTINE(.page_read = NULL,
                                         .segment_open = NULL,
                                         .segment_close = NULL),
                              NULL);
}
```

---

## 7. 成立前提(必须正视的硬约束)

上面代码能跑通,**当且仅当**满足下列条件;否则会 PANIC 或写坏页。这些是物理 redo 的本质要求,
不是代码能绕过的。

1. **捕获必须是完整物理子流,不能只挑 DML。**
   `§2` 的 `PartWALCapture` 必须把**所有落在该 shard 文件上的 record** 都捕获:`RM_HEAP`
   (insert/update/delete/lock/inplace)+ `RM_HEAP2`(multi_insert/prune/vacuum/freeze/visible)
   + **full-page image** + 该 shard 的 FSM/VM 改动。漏任何一条,页 LSN 链和布局就断,
   `heap_xlog_*` 里的 `PageAddItem`/`"invalid max offset"` 会 PANIC。

2. **副本必须同源基线。** follower 的 shard 必须由 leader shard 的**物理拷贝**初始化(`loc_map` 同时建立),
   而不是建空表从头 replay。否则 `blkno/offnum` 对不上。

3. **页 LSN 与 WAL flush 顺序(最尖锐的一条)。**
   `§4` 里 `PageSetLSN(page, lsn)` 用的 `lsn` 是自分配的 `apply_lsn`。但 PG 的 buffer manager 在刷脏页前会
   `XLogFlush(pageLSN)`,要求该 LSN ≤ 本地已 flush 的 WAL 位置。follower 的这些 heap 字节**不在它自己的
   pg_wal 里**,直接盖任意 LSN 会让刷盘逻辑死等或破坏“WAL 先于数据”。两种正解,二选一:
   - **(a) 重新落 WAL**:follower apply 时把同一条 record 通过 `XLogInsert` 写进自己的 pg_wal,
     用返回的真实 LSN 盖页(最稳,但有二次 WAL 开销);
   - **(b) 受控恢复对象**:把 shard 副本当作受控恢复对象,由 apply worker 独占 LSN 推进并自管刷盘点
     (类似 standby 的 minRecoveryPoint),`apply_lsn` 取自该受控序列。

   推荐先用 (a) 跑通正确性。

4. **xid 命名空间 / clog。** 每个 shard 的 leader 各有独立 xid 计数器;一个 follower 节点若同时托管多个
   shard 的副本,`TransactionIdCommitTree` 写的是**本地共享 clog**,不同 leader 的 xid 会撞。要么给每 shard
   隔离 clog/xid 空间,要么 leader 侧改用全局分配的 xid。这条不解决,`§5` 的可见性是错的。

5. **TOAST / 索引。** 带大字段的 tuple 会产生 toast 关系的独立 record(另一个 relfilenode);索引也是。
   它们必须在 `loc_map` 里各有一项,且同样要被 `§2` 捕获进对应 shard 的流。

---

## 8. 小结

- `§3`–`§6` 的重放代码把 **“DecodeXLogRecord → 改 rlocator → rm_redo”** 串起来,heap 应用零改动,
  完全是“不反解析成 SQL”的物理重放。
- 真正的工作量和风险全在 `§7` 的 5 条约束上,尤其是 ①(完整捕获)和 ③(页 LSN)。
- 代价是:**每个分片副本必须被当作该分片的物理备库**——同 relfilenode 映射、同源基线、完整有序无 gap
  的物理流。换来的是完全复用 PG 成熟的 redo 引擎。
