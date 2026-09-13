/*
 * shard_fileset.c
 *
 * ShardFileSet 构建 / 注册 / 持久化 / RM_SMGR main-data 解析（FRD §5）。
 *
 * 捕获判据从"主堆单一 relfilenode"升级为"fileset 任一成员命中"后，
 * 索引与 TOAST 的 WAL 记录与主堆走同一条捕获链，物理子流才完整。
 * fileset 同时原子落盘到 pg_parwal/<shard_oid>/fileset，供无 catalog
 * 访问的 bgworker（demux 崩溃恢复、replay worker）在启动时重建注册。
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "shard_clog.h"		/* T7.4：打标身份的持久证据 */
#include "shard_fileset.h"
#include "partition_wal.h"
#include "partwal_sync.h"
#include "utils/ruleutils.h"		/* T7.25：pg_get_indexdef_string */
#include "shard_xid.h"	/* 批次 #10：路由层角色切换 */
#include "shard_replay.h"	/* 批次 #10：路由层角色切换 */

#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlogrecord.h"
#include "catalog/pg_class.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/resowner.h"
#include "storage/smgr.h"
#include "utils/elog.h"
#include "utils/guc.h"          /* application_name */
#include "utils/syscache.h"
#include "utils/rel.h"

#define SHARD_FILESET_FILENAME  "fileset"

/* ================================================================== */
/* 构建（catalog 遍历，backend 事务上下文）                            */
/* ================================================================== */

static void
FileSetAdd(ShardFileSet *fs, const RelFileLocator *loc, uint8 role, uint8 ord)
{
    if (fs->nrels >= SHARD_FILESET_MAX_RELS)
        ereport(ERROR,
                (errmsg("pg_partdist: shard %u 的物理文件数超过上限 %d",
                        fs->shard_oid, SHARD_FILESET_MAX_RELS)));

    fs->rels[fs->nrels].loc  = *loc;
    fs->rels[fs->nrels].role = role;
    fs->rels[fs->nrels].ord  = ord;
    fs->rels[fs->nrels]._pad = 0;
    fs->nrels++;
}

int
BuildShardFileSet(Oid shard_oid, ShardFileSet *fs)
{
    return BuildShardFileSetEx(shard_oid, fs, NULL);
}

int
BuildShardFileSetEx(Oid shard_oid, ShardFileSet *fs, Oid *relids)
{
    Relation    rel;
    List       *indexes;
    ListCell   *lc;
    uint8       ord;
    Oid         toastoid;

    /*
     * relids 与 fs->rels 一一对应、同序填充。要关系 OID 是因为
     * log_newpage_range() 要 Relation，而 fileset 里只有 RelFileLocator。
     * 与其事后拿文件号反查（RelidByRelfilenumber 对默认表空间的入参约定
     * 容易写错），不如在这里顺手带出来 —— 反正这些关系本来就打开着。
     */
#define FILESET_NOTE_RELID(oid) \
    do { if (relids != NULL) relids[fs->nrels] = (oid); } while (0)

    memset(fs, 0, sizeof(*fs));
    fs->magic     = SHARD_FILESET_MAGIC;
    fs->shard_oid = shard_oid;
    fs->nrels     = 0;

    rel = try_relation_open(shard_oid, AccessShareLock);
    if (rel == NULL)
        return -1;

    /* [0] 主堆 */
    FILESET_NOTE_RELID(shard_oid);
    FileSetAdd(fs, &rel->rd_locator, SHARD_REL_MAIN, 0);

    /* 普通索引：RelationGetIndexList 返回 OID 升序 = 稳定的"定义序" */
    indexes = RelationGetIndexList(rel);
    ord = 0;
    foreach(lc, indexes)
    {
        Relation idx = relation_open(lfirst_oid(lc), AccessShareLock);

        FILESET_NOTE_RELID(lfirst_oid(lc));
        FileSetAdd(fs, &idx->rd_locator, SHARD_REL_INDEX, ord++);
        relation_close(idx, AccessShareLock);
    }
    list_free(indexes);

    /* TOAST 堆及其索引 */
    toastoid = rel->rd_rel->reltoastrelid;
    if (OidIsValid(toastoid))
    {
        Relation toast = table_open(toastoid, AccessShareLock);

        FILESET_NOTE_RELID(toastoid);
        FileSetAdd(fs, &toast->rd_locator, SHARD_REL_TOAST, 0);

        indexes = RelationGetIndexList(toast);
        ord = 0;
        foreach(lc, indexes)
        {
            Relation tidx = relation_open(lfirst_oid(lc), AccessShareLock);

            FILESET_NOTE_RELID(lfirst_oid(lc));
            FileSetAdd(fs, &tidx->rd_locator, SHARD_REL_TOAST_INDEX, ord++);
            relation_close(tidx, AccessShareLock);
        }
        list_free(indexes);
        table_close(toast, AccessShareLock);
    }

    relation_close(rel, AccessShareLock);
    return fs->nrels;

#undef FILESET_NOTE_RELID
}

/* ================================================================== */
/* §12：DDL 引起的 fileset 变更 —— 检测、发射 CTRL、灌新文件内容        */
/* ================================================================== */

/*
 * 为什么检测点在 ProcessUtility、发射点却在 PRE_COMMIT：
 *
 * DDL 一执行完就发 FILESET_UPDATE 的话，事务随后回滚，follower 已经按新结构
 * 换过表、把本地文件截过了 —— leader 回到旧结构，副本停在新结构，且新结构的
 * 内容来自一次不存在的 DDL。放到 PRE_COMMIT，暴露窗口就与既有 COMMIT 标记
 * 完全一致（PRE_COMMIT 之后仍可能失败，那是 §13 已记的既有缺口，不是新增的）。
 *
 * 所以 ProcessUtility 只置一个 backend 本地脏标记，真正的重建/diff/发射
 * 全在 PRE_COMMIT 做。普通 DML 事务不会置这个标记，代价为零。
 */
static bool fileset_maybe_changed = false;

/* GUC：单次 fileset 变更最多把多少个块以 FPI 形式灌进流 */
int fileset_inline_max_blocks = 131072;     /* 1 GB */

/*
 * T7.21（P7-R3）：流式基线的分块大小（块数）。每灌完这么多块就排空一次捕获环
 * 并复制出去，再灌下一块。0 = 不分块（回到一次灌完的旧行为）。
 */
int fileset_baseline_chunk_blocks = 16384;  /* 128 MB */

void
ShardFilesetNoteMaybeChanged(void)
{
    fileset_maybe_changed = true;
}

/* 在新 fileset 里按 (role, ord) 找成员；找不到返回 NULL */
static const ShardFileSetRel *
FileSetFind(const ShardFileSet *fs, uint8 role, uint8 ord)
{
    int i;

    for (i = 0; i < fs->nrels; i++)
        if (fs->rels[i].role == role && fs->rels[i].ord == ord)
            return &fs->rels[i];
    return NULL;
}

static bool
FileSetEquals(const ShardFileSet *a, const ShardFileSet *b)
{
    int i;

    if (a->nrels != b->nrels)
        return false;

    for (i = 0; i < a->nrels; i++)
    {
        const ShardFileSetRel *m = FileSetFind(b, a->rels[i].role,
                                               a->rels[i].ord);

        if (m == NULL || !RelFileLocatorEquals(m->loc, a->rels[i].loc))
            return false;
    }
    return true;
}

/*
 * 数一个关系的全部 fork 共有多少块；do_log 为真时顺便把它们以 FPI 记录
 * 写进 pg_wal —— 此刻该文件号已经注册进反向哈希，捕获钩子会把这些 FPI
 * 收进本分区的流，follower 端按普通 DATA 记录重放即可把新文件填满。
 *
 * page_std 一律传 false（整页进 FPI，不做 [pd_lower,pd_upper) 掐洞）：
 *   - VM/FSM 这类非标准布局的 fork，掐洞判据本身就不适用；
 *   - 主 fork 虽然可以掐，但整页搬运让 follower 的新文件成为 leader 的
 *     逐字节副本，连"洞内残字节"都一致，正好省掉一处判据例外。
 * 代价只是一次性 DDL 多搬一点字节，而 DDL 之后的新文件本就是紧实的。
 */
static uint64 FileSetLogRelationPagesChunked(Oid relid, uint64 *pending);

static uint64
FileSetLogRelationPages(Oid relid, bool do_log)
{
    Relation    rel;
    ForkNumber  fork;
    uint64      total = 0;

    rel = try_relation_open(relid, AccessShareLock);
    if (rel == NULL)
        return 0;

    for (fork = 0; fork <= MAX_FORKNUM; fork++)
    {
        BlockNumber nblocks;

        if (!smgrexists(RelationGetSmgr(rel), fork))
            continue;

        nblocks = smgrnblocks(RelationGetSmgr(rel), fork);
        if (nblocks == 0)
            continue;

        total += nblocks;
        if (do_log)
            log_newpage_range(rel, fork, 0, nblocks, false /* page_std */);
    }

    relation_close(rel, AccessShareLock);
    return total;
}

/*
 * T7.21（P7-R3）：**流式**物理基线 —— 按块灌 FPI，每灌完一块就排空捕获环并复制。
 *
 * ★ 为什么非分块不可：FPI 不是直接进分区流的，而是先经 wal_insert_hook 写一条
 *   描述符进**全节点共享**的捕获环（`PARTWAL_BUFFER_SLOTS = 8192` 槽），
 *   等 PartWALFlush 排空。`log_newpage_range` 每条 XLOG_FPI 最多带 32 块，
 *   所以一次灌完的基线占 `块数/32` 个槽 —— 1 GB 就是 4096 槽，**半个环**。
 *   而环满时 `PartWALInsert` **只打一条 WARNING 就覆盖未消费的条目**
 *   （它跑在 XLogInsert 里，没法等、也没法排空），被覆盖的那几页从此不在流里：
 *   副本基线缺页，**静默物理分歧**。安静节点上 1 GB 上限还留有 2 倍余量，
 *   可一旦同时有别的写入分享同一个环，这点余量就不存在了。
 *
 *   原来的 1 GB 硬上限因此不是"保守"，而是**恰好卡在环容量的一半**、且只在
 *   无并发负载时成立的偶然安全；它的副作用是大于 1 GB 的分片既不能供给、
 *   也不能修复分叉（P7-R3）。
 *
 * ★ 分块之后，环占用被压到 `chunk/32` 槽（默认 16384 块 = 512 槽 ≈ 6% 环），
 *   与基线总大小无关；复制侧的背压由 raft 的 wait_for_log_room 承担。
 *   事务中途排空是既有做法（DDL 发 FILESET_UPDATE 前、dtx 判决补发后都这么调），
 *   `PartWALFlush` 自己会先 XLogFlush 到对应 LSN。
 *
 * ★ 分块跨越并发写入仍然物理一致：每张 FPI 是"此刻"的页面全像；之后对该页的
 *   任何修改在 WAL 里 LSN 都更大，回放侧 redo 按 orig_lsn 与页 LSN 比较，
 *   已含进全像的修改会被跳过 —— 与 PG 自己"模糊拷贝 + WAL 回放"的基础备份
 *   同一个道理，与是否分块无关。
 */
static uint64
FileSetLogRelationPagesChunked(Oid relid, uint64 *pending)
{
    Relation    rel;
    ForkNumber  fork;
    uint64      total = 0;
    int         nchunks = 0;

    if (fileset_baseline_chunk_blocks <= 0)
        return FileSetLogRelationPages(relid, true);

    rel = try_relation_open(relid, AccessShareLock);
    if (rel == NULL)
        return 0;

    for (fork = 0; fork <= MAX_FORKNUM; fork++)
    {
        BlockNumber nblocks;
        BlockNumber start;

        if (!smgrexists(RelationGetSmgr(rel), fork))
            continue;

        nblocks = smgrnblocks(RelationGetSmgr(rel), fork);
        for (start = 0; start < nblocks;)
        {
            BlockNumber end = start + (BlockNumber) fileset_baseline_chunk_blocks;

            if (end > nblocks || end < start)
                end = nblocks;

            CHECK_FOR_INTERRUPTS();
            log_newpage_range(rel, fork, start, end, false /* page_std */);
            total += (uint64) (end - start);
            start = end;

            nchunks++;

            /*
             * 攒够一块才排空。按"未排空块数"而不是"每块必排"：调用方逐个
             * 关系、逐个 fork 调进来，小关系一块都攒不满 —— 每块必排的话，
             * 一次 TRUNCATE 小表也要多做几次 XLogFlush（每次都是一次 fsync）。
             * 计数跨调用累计（由调用方持有），环里未排空的 FPI 因此恒 < 一块。
             */
            *pending += (uint64) (end - start);
            if (*pending >= (uint64) fileset_baseline_chunk_blocks)
            {
                PartWALFlush(InvalidXLogRecPtr, false);
                *pending = 0;
            }
        }
    }

    relation_close(rel, AccessShareLock);

    if (nchunks > 1)
        ereport(LOG,
                (errmsg("pg_partdist: 关系 %u 的物理基线分 %d 块流式发射，共 %llu 块",
                        relid, nchunks, (unsigned long long) total)));
    return total;
}

/*
 * EmitFilesetHandover —— T7.3（R-P6-16）：升主时把**本节点的 fileset** 广播出去，
 * 让其余副本把 locmap 重绑到新主的文件号上。
 *
 * 与 EmitFilesetUpdate 的三处差别，每一处都是必须的：
 *   ① **不发任何 FPI**：内容没变，副本手上的文件是它自己回放出来的、与新主同源；
 *   ② 置 `PARTWAL_FSUPD_PRIMARY_HANDOVER`，让 follower 走"只重绑、不截断"分支；
 *   ③ 不比较新旧 fileset —— 交接时"变了的成员"就是全部成员，比较没有意义。
 *
 * 仍然要做的一件事与 DDL 路径相同：**先把本地 fileset 登记进反向哈希**
 * （`RegisterShardFileSet`），否则新主自己后续的写入进不了流。
 * `PartDistRoutePromote` 已经做过一次，这里靠它幂等。
 *
 * 失败不上抛：交接的其余部分（角色、捕获、打标身份）已经完成，广播失败只是让
 * 其余副本晚一点才能跟上（运维动作 = 从新主重新供给）。把 ERROR 抛出去会让
 * 整个 OP_PARTITION_PRIMARY 的 apply 失败，那才是真的坏。
 */
void
PartDistEmitFilesetHandover(Oid shard_oid)
{
    ShardFileSet             fs;
    PartWALCtrlFilesetUpdate *payload;
    uint32                   payload_len;

    if (!OidIsValid(shard_oid))
        return;
    if (BuildShardFileSet(shard_oid, &fs) < 1)
        return;

    RegisterShardFileSet(&fs);

    payload_len = (uint32) PartWALCtrlFilesetUpdateSize(fs.nrels);
    payload = palloc0(payload_len);
    payload->nrels    = (uint32) fs.nrels;
    payload->flags    = PARTWAL_FSUPD_PRIMARY_HANDOVER;
    payload->reserved = 0;
    memcpy(PartWALCtrlFilesetRels(payload), fs.rels,
           (size_t) fs.nrels * sizeof(ShardFileSetRel));

    PG_TRY();
    {
        PartWALAppendCtrl(shard_oid, PARTWAL_CTRL_FILESET_UPDATE,
                          (const char *) payload, payload_len);
        elog(LOG, "pg_partdist: 分片 %u 升主后广播 fileset 交接（%d 个成员）",
             shard_oid, fs.nrels);
    }
    PG_CATCH();
    {
        ErrorData *ed;

        MemoryContextSwitchTo(TopMemoryContext);
        ed = CopyErrorData();
        FlushErrorState();
        ereport(WARNING,
                (errmsg("pg_partdist: 分片 %u 的 fileset 交接广播失败：%s",
                        shard_oid, ed->message),
                 errdetail("其余副本的 locmap 仍指向旧主的文件号，"
                           "它们放不了新主的流，也就暂时失去当选资格。"),
                 errhint("运维动作：从新主重新供给这些副本。")));
        FreeErrorData(ed);
    }
    PG_END_TRY();

    pfree(payload);
}

/*
 * ShardDropSentinelPath —— 已发过 DROP 通知的标记，落在该分片的 parwal 目录里。
 *
 * 没有它，扫描不收敛：`fileset` 文件在 DROP 之后仍然留着（那正是"这个分片曾归
 * 本节点维护"的持久证据），于是**每来一次新的 DROP，历史上所有残留分片都会被
 * 重发一遍通知**。重发本身无害（幂等），但日志会越积越长，raft 上也是白跑的
 * 提案。落一个哨兵文件把它收住；哨兵丢了最坏就是多发一次。
 */
static void
ShardDropSentinelPath(Oid shard_oid, char *buf, size_t buflen)
{
    snprintf(buf, buflen, "%s/%s/%u/dropped", DataDir, PARTITION_WAL_DIR,
             shard_oid);
}

/*
 * EmitShardDropNotice —— T7.8（P7-D1）：告诉副本"这个分片没了"。
 *
 * ★ 失败不上抛，而且是**真的**不上抛（2026-09-10 改正）。
 *
 *   原先这里只是 PG_TRY/PG_CATCH + FlushErrorState 就接着往下走。那在
 *   `COMMIT PREPARED` 之后那条路径上勉强能用（后面没别的事要做了），可现在
 *   扫描挂在**任意用户语句开头**，问题就现形了：PG 的 PG_CATCH **不会**把
 *   事务恢复成可用状态，只有子事务能。实测后果是整条用户语句被带挂 ——
 *   夹具里 `replay_set_locmap` / `replay_enable` 全线报"relation does not
 *   exist"，因为它们前面那条建壳表的语句已经被这里的报错弄脏了。
 *
 *   触发它的是最普通不过的情形：本节点上留着某个已删分片的 fileset，而本节点
 *   **不是那个分区组的 leader** —— `PartWALAppendCtrl` 于是理直气壮地报
 *   "本节点不是该分区组的 leader"。这不是异常，是常态。
 *
 * 返回 true 表示通知已发出（调用方据此落哨兵）。
 */
static bool
EmitShardDropNotice(Oid shard_oid)
{
    MemoryContext oldcxt   = CurrentMemoryContext;
    ResourceOwner oldowner = CurrentResourceOwner;
    volatile bool ok       = false;

    /*
     * 判据（"fileset 里有、catalog 里没了"）也放进子事务里一起做：
     * `LoadShardFileSet` 读文件、`BuildShardFileSetEx` 走 syscache，两者都能抛。
     * 只把发射包起来是不够的 —— 这里的每一行都跑在别人的语句上。
     */
    BeginInternalSubTransaction(NULL);
    PG_TRY();
    {
        ShardFileSet old_fs;
        ShardFileSet new_fs;
        Oid          new_relids[SHARD_FILESET_MAX_RELS];

        if (!LoadShardFileSet(shard_oid, &old_fs) ||          /* 不归本节点维护 */
            BuildShardFileSetEx(shard_oid, &new_fs, new_relids) >= 1)   /* 表还在 */
        {
            ReleaseCurrentSubTransaction();
            MemoryContextSwitchTo(oldcxt);
            CurrentResourceOwner = oldowner;
            return false;
        }

        PartWALAppendCtrl(shard_oid, PARTWAL_CTRL_SHARD_DROP, NULL, 0);
        PartWALNoteTouchedPartition(shard_oid);
        ReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcxt);
        CurrentResourceOwner = oldowner;
        ok = true;
        elog(LOG, "pg_partdist: 分片 %u 已 DROP，已通知副本停流", shard_oid);
    }
    PG_CATCH();
    {
        ErrorData *ed;

        MemoryContextSwitchTo(oldcxt);
        ed = CopyErrorData();
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcxt);
        CurrentResourceOwner = oldowner;

        /*
         * LOG 而不是 WARNING：扫描是搭在别人的语句上跑的，把它的失败推给一个
         * 毫不相干的客户端是错的 —— 何况"本节点不是该组 leader"根本不算异常。
         */
        ereport(LOG,
                (errmsg("pg_partdist: 分片 %u 的 DROP 通知未能发出：%s",
                        shard_oid, ed->message),
                 errdetail("副本侧的壳表/槽位/目录将保持残留；本节点重新成为该组 "
                           "leader 后会再试。")));
        FreeErrorData(ed);
    }
    PG_END_TRY();

    return ok;
}

/*
 * ShardFilesetEmitDropNotices —— T7.8：**COMMIT PREPARED 之后**的 DROP 扫一遍。
 *
 * ★ 为什么非要有这一支（2026-09-10 实测定位）：
 *   fileset 发射器只挂在 `XACT_EVENT_PRE_COMMIT` 上，而 **Citus 的 DDL 在 worker
 *   上一律走 2PC** —— PostgreSQL 那时触发的是 `XACT_EVENT_PRE_PREPARE`，
 *   于是"DROP 掉一张分布表"的那笔事务里，发射器**一次都没跑**。
 *   现象：日志里只见到给**残留旧 fileset** 补发的通知（那是后来某笔本地事务顺手
 *   发的），当轮刚删的分片反而没有 —— 看起来像"通知没实装"，其实是挂错了时机。
 *
 * ★ 为什么不是简单地也挂到 PRE_PREPARE 上：那时**提交还没成定局**，
 *   事务仍可能 ROLLBACK PREPARED。副本一旦按通知停了流，回滚之后就再也追不上 ——
 *   这正是 D1 当初把 fileset 发射放在 PRE_COMMIT（而不是 DDL 执行完就发）的理由，
 *   §12 的注释里写着。放到 `XACT_EVENT_COMMIT_PREPARED` 则没有这个问题：
 *   那一刻表是真的没了。
 *
 * ★ 只处理 DROP，不处理 fileset 变更：后者要与那批 FPI 保持"CTRL 先、内容后"的
 *   顺序，必须留在提交之前；DROP 没有内容要灌，事后发反而更安全。
 *
 * 代价：每次 COMMIT PREPARED 多一次分区清单遍历 + 每个分区一次 syscache 命中判定。
 * 分区清单是本节点实际维护的那几十个，不是全库扫描。
 */
void
ShardFilesetEmitDropNotices(bool force)
{
    static TimestampTz last_scan = 0;
    TimestampTz         now;
    char                dirpath[MAXPGPATH];
    DIR                *dir;
    struct dirent      *de;

    if (!IsTransactionState() || !OidIsValid(MyDatabaseId))
        return;

    /*
     * ★ 必须扫**目录**，不能扫 shmem 反向哈希（`PartWALSyncListPartitions`）。
     *   第一版就是那么写的，结果只对**残留的旧 OID** 发出通知，当轮刚删的分片
     *   反而没有 —— 因为表一 DROP，它在反向哈希里的条目就没了，事后扫描根本
     *   看不见它。持久化的 fileset 文件（`pg_parwal/<oid>/fileset`）才是"这个
     *   分片曾经归本节点维护"的持久证据，DROP 不会把它带走。
     *
     * force=true 时跳过频率守卫：调用方（提交后惰性补发）自己带了"代次变了"
     * 这个更精确的守卫 —— 每笔已提交的 DROP 最多触发一次，不需要再限流；
     * 若这里照样限流，刚删完 1 秒内的那次补发就会被吃掉，通知就丢了。
     *
     * 频率守卫：COMMIT PREPARED 不是冷路径，1 秒一次足够（DROP 通知晚一秒到达
     * 副本没有任何后果 —— 它只是让副本停流，不涉及数据正确性）。
     *   与 ShardFreezeMaybeEmitUpdates 的时间间隔守卫同一条纪律。
     */
    now = GetCurrentTimestamp();
    if (!force && last_scan != 0 &&
        !TimestampDifferenceExceeds(last_scan, now, 1000))
        return;
    last_scan = now;

    snprintf(dirpath, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);
    dir = AllocateDir(dirpath);
    if (dir == NULL)
        return;

    while ((de = ReadDir(dir, dirpath)) != NULL)
    {
        Oid           shard_oid;
        char         *endptr;
        char          sentinel[MAXPGPATH];
        struct stat   st;
        int           fd;

        if (de->d_name[0] == '.')
            continue;
        shard_oid = (Oid) strtoul(de->d_name, &endptr, 10);
        if (*endptr != '\0' || shard_oid == InvalidOid)
            continue;

        /* 已经通知过了：不重发（见 ShardDropSentinelPath 的注释） */
        ShardDropSentinelPath(shard_oid, sentinel, sizeof(sentinel));
        if (stat(sentinel, &st) == 0)
            continue;

        /* 判据 + 发射都在 EmitShardDropNotice 的子事务里做 */
        if (!EmitShardDropNotice(shard_oid))
            continue;           /* 表还在 / 不归本节点 / 本节点不是该组 leader */

        fd = OpenTransientFile(sentinel, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
        if (fd >= 0)
            CloseTransientFile(fd);
    }
    FreeDir(dir);
}

static void
EmitFilesetUpdate(Oid shard_oid, const ShardFileSet *old_fs,
                  const ShardFileSet *new_fs, const Oid *new_relids)
{
    PartWALCtrlFilesetUpdate *payload;
    uint32      payload_len;
    int         changed[SHARD_FILESET_MAX_RELS];
    int         nchanged = 0;
    uint64      total_blocks = 0;
    uint16      flags = 0;
    int         i;

    /*
     * 哪些成员需要把内容也送过去：新出现的（CREATE INDEX），或 (role,ord)
     * 还在但文件号换了的（VACUUM FULL / REINDEX / TRUNCATE / 重写类 ALTER）。
     * 没变的成员的历史记录早已在流里，一个字节都不用重发。
     */
    for (i = 0; i < new_fs->nrels; i++)
    {
        const ShardFileSetRel *o = FileSetFind(old_fs, new_fs->rels[i].role,
                                               new_fs->rels[i].ord);

        if (o == NULL || !RelFileLocatorEquals(o->loc, new_fs->rels[i].loc))
        {
            changed[nchanged++] = i;
            total_blocks += FileSetLogRelationPages(new_relids[i], false);
        }
    }

    if (total_blocks > (uint64) fileset_inline_max_blocks)
    {
        flags |= PARTWAL_FSUPD_NEEDS_REBASELINE;
        ereport(WARNING,
                (errmsg("pg_partdist: shard %u 的 fileset 变更涉及 %llu 个块，"
                        "超过 pg_partdist.fileset_inline_max_blocks = %d",
                        shard_oid, (unsigned long long) total_blocks,
                        fileset_inline_max_blocks),
                 errdetail("只发结构变更通知，新文件内容不进流。"),
                 errhint("该 shard 的副本需要重做物理基线拷贝。")));
    }

    /*
     * 注册必须在 log_newpage_range 之前：捕获钩子的判据是"文件号命中反向
     * 哈希"，没登记的话下面那批 FPI 一条都进不了流（而且是**静默**丢弃 ——
     * 这正是 D1 之前 DDL 造成副本静默分歧的机制）。
     */
    RegisterShardFileSet(new_fs);

    /*
     * T7.25（P7-R4）：成员**结构**变了（(role, ord) 集合不同）⇒ 在 FILESET_UPDATE
     * 之前先发一条结构提示，载荷是新 fileset 全部普通索引的定义清单。
     * 只换文件号的变更（VACUUM FULL / REINDEX / TRUNCATE）结构不变，副本侧本来
     * 就能全自动跟上，不发。
     */
    {
        bool structural = (old_fs->nrels != new_fs->nrels);

        for (i = 0; !structural && i < new_fs->nrels; i++)
            if (FileSetFind(old_fs, new_fs->rels[i].role, new_fs->rels[i].ord) == NULL)
                structural = true;

        if (structural)
        {
            StringInfoData hint;
            int            ord;
            int            nidx = 0;

            initStringInfo(&hint);
            /* 按 ord 升序输出，与副本侧 RelationGetIndexList 的 OID 序对齐 */
            for (ord = 0; ord < SHARD_FILESET_MAX_RELS; ord++)
                for (i = 0; i < new_fs->nrels; i++)
                    if (new_fs->rels[i].role == SHARD_REL_INDEX &&
                        new_fs->rels[i].ord == ord)
                    {
                        char *def = pg_get_indexdef_string(new_relids[i]);

                        if (nidx++ > 0)
                            appendStringInfoChar(&hint, '\n');
                        appendStringInfoString(&hint, def);
                        pfree(def);
                    }

            PartWALAppendCtrl(shard_oid, PARTWAL_CTRL_DDL_HINT,
                              hint.data, (uint32) hint.len);
            ereport(LOG,
                    (errmsg("pg_partdist: shard %u 成员结构变化，已发 DDL 结构提示"
                            "（%d 个索引定义）", shard_oid, nidx)));
            pfree(hint.data);
        }
    }

    payload_len = (uint32) PartWALCtrlFilesetUpdateSize(new_fs->nrels);
    payload = palloc0(payload_len);
    payload->nrels    = (uint32) new_fs->nrels;
    payload->flags    = flags;
    payload->reserved = 0;
    memcpy(PartWALCtrlFilesetRels(payload), new_fs->rels,
           (size_t) new_fs->nrels * sizeof(ShardFileSetRel));

    PartWALAppendCtrl(shard_oid, PARTWAL_CTRL_FILESET_UPDATE,
                      (const char *) payload, payload_len);
    pfree(payload);

    /*
     * CTRL 已在流里，现在灌内容 —— 顺序不能反（follower 要先换完表）。
     *
     * T7.21：这里同样走分块排空。内联上限 1 GB = 4096 个环槽（半个捕获环），
     * 一次 VACUUM FULL 灌满的同时若别的会话也在写同一个节点，环就会满、
     * `PartWALInsert` 覆盖未消费条目（P7-W2）—— 与基线路径是同一个风险，
     * 只是这里上限更低、所以此前没人撞见。
     */
    if ((flags & PARTWAL_FSUPD_NEEDS_REBASELINE) == 0)
    {
        uint64 pending = 0;

        /* 尾巴（不足一块）留给提交时的那次排空，与此前的时序一致 */
        for (i = 0; i < nchanged; i++)
            (void) FileSetLogRelationPagesChunked(new_relids[changed[i]], &pending);
    }

    ereport(LOG,
            (errmsg("pg_partdist: shard %u fileset 变更已发射（%d 个成员，"
                    "其中 %d 个换了文件号，共 %llu 块%s）",
                    shard_oid, new_fs->nrels, nchanged,
                    (unsigned long long) total_blocks,
                    (flags & PARTWAL_FSUPD_NEEDS_REBASELINE)
                        ? "，内容未进流" : "")));
}

/* ================================================================== */
/* T6.1（P6）：全量物理基线                                            */
/* ================================================================== */

/*
 * ShardBaselineEmit — 把一个 shard 的**全部字节**重新灌进它自己的分区流，
 * 并返回这次基线的起点 partition_lsn。
 *
 * ★ 它解决的是设计 §13 约束 2 后半句一直没实装的那件事：
 *   「副本必须由 leader shard 物理拷贝初始化（**拷贝时记下 partition_lsn
 *     静止点**，增量从该游标重放追齐）」。
 *   此前 locmap 只配对了"哪个文件对哪个文件"，从不配对"从哪个游标开始"，
 *   于是缺省游标 0 等于沉默地断言"本地文件 == leader 在流起点时的文件" ——
 *   这个断言从来没人建立过，也从来没人校验过（R-P4-20 的第一半）。
 *
 * ★ 为什么不做"文件拷贝 + 带外传输"，而是走流：
 *   项目里已经有一条把文件字节送到 follower 的成熟通路 —— §12 的 DDL 变更
 *   用 log_newpage_range() 把新文件整页灌成 FPI，捕获钩子收进分区流，
 *   follower 当普通 DATA 记录重放。**全量基线与它逐字一致，差别只在
 *   "截断哪些成员"**（DDL 只截换了文件号的，基线截全部）。走同一条路的好处：
 *     - 没有传输问题（跨机、权限、断点续传统统不存在）；
 *     - follower 侧**一行新的导入逻辑都不需要**，FPI 重放本来就会；
 *     - 与 Raft 复制、崩溃恢复、幂等去重全部天然兼容。
 *
 * ★ 为什么不需要独占锁（与 pg_basebackup 同理）：
 *   设某页在扫描中途被改。若改动发生在该页 FPI **之前**，FPI 里已经含它；
 *   若发生在**之后**，那条增量记录的 partition_lsn 大于 FPI 的，重放时
 *   "先 FPI 后增量"，结果一样正确。扫描期间新扩的块也一样 —— 它们的创建
 *   记录排在基线之后。故 AccessShareLock（FileSetLogRelationPages 已取）足够。
 *
 * ★ 顺序铁律：CTRL 必须**先于**那批 FPI 进流。follower 要先按 CTRL 换完表、
 *   截完文件，后面的 FPI 才有地方落。这一条与 §12 的 EmitFilesetUpdate 相同，
 *   那里的注释写着"顺序不能反"。
 *
 * 返回值 = 这条 CTRL 的 partition_lsn。把它交给 follower 当 base_part_lsn：
 * 从它开始重放，之前的记录一律不看，也不需要看。
 */
uint64
ShardBaselineEmit(Oid shard_oid)
{
    ShardFileSet    fs;
    Oid             relids[SHARD_FILESET_MAX_RELS];
    PartWALCtrlFilesetUpdate *payload;
    uint32          payload_len;
    uint64          total_blocks = 0;
    uint64          base_plsn;
    int             nrels;
    int             i;

    nrels = BuildShardFileSetEx(shard_oid, &fs, relids);
    if (nrels < 0)
        ereport(ERROR,
                (errmsg("pg_partdist: shard %u 不存在，无法发射物理基线",
                        shard_oid)));
    if (nrels == 0)
        ereport(ERROR,
                (errmsg("pg_partdist: shard %u 的 fileset 为空，无法发射物理基线",
                        shard_oid)));

    /* 先量一次总块数：超限就**明确报错**，不静默降级 */
    for (i = 0; i < nrels; i++)
        total_blocks += FileSetLogRelationPages(relids[i], false);

    /*
     * T7.21（P7-R3）：流式发射之后，基线**不再按总块数设硬上限**。
     * 原上限的真实作用是"别让一次灌的 FPI 描述符撑满共享捕获环"（见
     * FileSetLogRelationPagesChunked 的注释），分块排空之后环占用与总大小无关。
     * 只有显式把分块关掉（fileset_baseline_chunk_blocks = 0）时才保留旧门禁。
     */
    if (fileset_baseline_chunk_blocks <= 0 &&
        total_blocks > (uint64) fileset_inline_max_blocks)
        ereport(ERROR,
                (errmsg("pg_partdist: shard %u 的物理基线涉及 %llu 个块，"
                        "超过 pg_partdist.fileset_inline_max_blocks = %d",
                        shard_oid, (unsigned long long) total_blocks,
                        fileset_inline_max_blocks),
                 errdetail("流式基线已被关掉（pg_partdist.fileset_baseline_chunk_blocks = 0），"
                           "一次灌完会撑满共享捕获环、静默丢页。"),
                 errhint("打开流式基线，或确认体量后调高 fileset_inline_max_blocks。")));

    /*
     * 注册必须在 log_newpage_range 之前 —— 捕获钩子的判据是"文件号命中反向
     * 哈希"，没登记的话下面那批 FPI 一条都进不了流，而且是**静默**丢弃。
     * （这正是 §12 之前 DDL 造成副本静默分歧的机制，此处照抄那条纪律。）
     */
    RegisterShardFileSet(&fs);

    payload_len = (uint32) PartWALCtrlFilesetUpdateSize(nrels);
    payload = palloc0(payload_len);
    payload->nrels    = (uint32) nrels;
    payload->flags    = PARTWAL_FSUPD_FULL_BASELINE;
    payload->reserved = 0;
    memcpy(PartWALCtrlFilesetRels(payload), fs.rels,
           (size_t) nrels * sizeof(ShardFileSetRel));

    base_plsn = PartWALAppendCtrl(shard_oid, PARTWAL_CTRL_FILESET_UPDATE,
                                  (const char *) payload, payload_len);
    pfree(payload);

    /*
     * CTRL 已在流里，现在灌全部内容 —— 顺序不能反。
     * T7.21：按块流式灌，每块之后排空捕获环（见 FileSetLogRelationPagesChunked）。
     * 先排空一次：CTRL 与之前缓冲的记录必须排在第一块 FPI 之前。
     */
    PartWALFlush(InvalidXLogRecPtr, false);
    {
        uint64 pending = 0;

        for (i = 0; i < nrels; i++)
            (void) FileSetLogRelationPagesChunked(relids[i], &pending);
        if (pending > 0)
            PartWALFlush(InvalidXLogRecPtr, false);
    }

    /*
     * ★ T7.2（R-P6-17）：页面之外，**分片 clog 也要搬**。
     *
     * 只灌页面的后果很具体：基线游标之前的 MARKER 不再回放，于是"先有数据、
     * 后供给"的副本对基线前提交的每个分片 xid 都没有判决 —— 那些行读成 RUNNING
     * （不可见）；它一旦升主，`shard_claim_on_promote` 还会把这些 RUNNING 改判
     * ABORTED，**已经提交的行就此消失**。此前只能靠"先供给、后写数据"的运维
     * 纪律绕过去，而那条纪律没有任何东西强制。
     *
     * 上界取本分片**下一个待发的号**：小于它的号才可能已经落过账。
     * 非打标分片（没有 clog 目录）返回 0 块，行为与从前逐字一致。
     */
    if (ShardClogDirExists(shard_oid))
        (void) ShardClogEmitBaseline(shard_oid, ShardXidNextToIssue(shard_oid));

    ereport(LOG,
            (errmsg("pg_partdist: shard %u 物理基线已发射（%d 个成员，%llu 块，"
                    "base_part_lsn=%llu）",
                    shard_oid, nrels, (unsigned long long) total_blocks,
                    (unsigned long long) base_plsn)));

    /*
     * ★ 批次 #8：基线发完就把分叉标记清掉 —— 重做物理基线**就是**§13 约束 13
     * 的修复动作（FULL_BASELINE 会让 follower 先截断全部成员再重灌），
     * 没有必要再让运维手工去清一次。清标只发生在这里和 provision（它也发基线），
     * 手工入口 shard_clear_divergence() 是给"确认过没事"的取证场景留的。
     */
    ShardClearDiverged(shard_oid);

    return base_plsn;
}

/* ================================================================== */
/* §13 约束 5：把 leader 的冻结账目同步给副本（D2）                     */
/* ================================================================== */

/*
 * 上次已发射的值，持久化在 pg_parwal/<oid>/freeze。
 *
 * 为什么要持久化而不是每次都发：relfrozenxid 是慢变量，绝大多数检查都会发现
 * "没变"。没有这份基线就只能每次都发一条 CTRL 走一趟 Raft，纯浪费。
 */
#define SHARD_FREEZE_FILENAME   "freeze"
#define SHARD_FREEZE_MAGIC      UINT32_C(0x465A4E58)    /* "FZNX" */

typedef struct ShardFreezeFile
{
    uint32              magic;
    Oid                 shard_oid;
    int32               nrels;
    PartWALFreezeEntry  rels[SHARD_FILESET_MAX_RELS];
} ShardFreezeFile;

/* GUC：两次冻结账目检查之间的最小间隔；0 = 每个事务都查（测试用） */
int freeze_sync_interval_ms = 60000;

/*
 * 本 backend 是否执行过用户语句 —— 冻结发射器的白名单开关。
 * 详见 ShardFreezeMaybeEmitUpdates 的守卫注释（第 ④ 条：InitPostgres 引导事务）。
 */
static bool shard_freeze_user_activity = false;

void
ShardFreezeNoteUserActivity(void)
{
    shard_freeze_user_activity = true;
}

static void
FreezePath(Oid shard_oid, char *path, char *tmp)
{
    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, shard_oid, SHARD_FREEZE_FILENAME);
    if (tmp != NULL)
        snprintf(tmp, MAXPGPATH, "%s/%s/%u/%s.tmp",
                 DataDir, PARTITION_WAL_DIR, shard_oid, SHARD_FREEZE_FILENAME);
}

/*
 * 收齐本 shard 各**堆**关系的冻结账目。索引的 pg_class.relfrozenxid 恒为 0，
 * 同步它没有意义，所以只取主堆与 TOAST 堆。返回条目数。
 *
 * ★★ 只走 syscache，**绝不 relation_open**。
 *
 * 本函数是从 XACT_EVENT_PRE_COMMIT 调下来的。在提交回调里现取关系锁
 * （relation_open 会取 AccessShareLock）是危险动作：那时事务的锁集合本该
 * 已经定型，新加的锁会扰乱 Citus 追踪的等待边 —— 实测表现为它的分布式
 * 死锁检测器 dump_local_wait_edges() 段错误，把整个节点带崩，而 R1 用例
 * 恰好在 kill -9 重置对端之后撞上这个窗口。
 *
 * 判据实验很干净：仅用 GUC 关掉本发射器（其余代码一字不改），
 * R1 从 29/45 变成 **49/49 全过**。
 *
 * syscache 版本拿到的是同一份 pg_class 元组的内容，不取任何锁：
 * 主堆的 OID 就是 shard_oid，TOAST 堆经 reltoastrelid 一跳可得。
 */
static int
BuildShardFreeze(Oid shard_oid, PartWALFreezeEntry *out)
{
    HeapTuple     ctup;
    Form_pg_class form;
    Oid           toastoid;
    int           n = 0;

    ctup = SearchSysCache1(RELOID, ObjectIdGetDatum(shard_oid));
    if (!HeapTupleIsValid(ctup))
        return 0;
    form = (Form_pg_class) GETSTRUCT(ctup);

    out[n].role         = SHARD_REL_MAIN;
    out[n].ord          = 0;
    out[n].reserved     = 0;
    out[n].relfrozenxid = (uint32) form->relfrozenxid;
    out[n].relminmxid   = (uint32) form->relminmxid;
    n++;

    toastoid = form->reltoastrelid;
    ReleaseSysCache(ctup);

    if (OidIsValid(toastoid))
    {
        ctup = SearchSysCache1(RELOID, ObjectIdGetDatum(toastoid));
        if (HeapTupleIsValid(ctup))
        {
            form = (Form_pg_class) GETSTRUCT(ctup);
            out[n].role         = SHARD_REL_TOAST;
            out[n].ord          = 0;
            out[n].reserved     = 0;
            out[n].relfrozenxid = (uint32) form->relfrozenxid;
            out[n].relminmxid   = (uint32) form->relminmxid;
            n++;
            ReleaseSysCache(ctup);
        }
    }
    return n;
}

static bool
LoadShardFreeze(Oid shard_oid, ShardFreezeFile *fz)
{
    char    path[MAXPGPATH];
    int     fd;
    ssize_t nb;

    FreezePath(shard_oid, path, NULL);
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        return false;
    nb = read(fd, fz, sizeof(*fz));
    CloseTransientFile(fd);

    return (nb == (ssize_t) sizeof(*fz) &&
            fz->magic == SHARD_FREEZE_MAGIC &&
            fz->shard_oid == shard_oid &&
            fz->nrels >= 0 && fz->nrels <= SHARD_FILESET_MAX_RELS);
}

static void
StoreShardFreeze(Oid shard_oid, const PartWALFreezeEntry *ents, int n)
{
    ShardFreezeFile fz;
    char            path[MAXPGPATH];
    char            tmp[MAXPGPATH];
    int             fd;
    ssize_t         nb;

    memset(&fz, 0, sizeof(fz));
    fz.magic     = SHARD_FREEZE_MAGIC;
    fz.shard_oid = shard_oid;
    fz.nrels     = n;
    memcpy(fz.rels, ents, (size_t) n * sizeof(PartWALFreezeEntry));

    InitPartitionWALDirectory(shard_oid);
    FreezePath(shard_oid, path, tmp);

    fd = OpenTransientFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
    if (fd < 0)
    {
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法创建 freeze 临时文件 \"%s\": %m", tmp)));
        return;
    }
    do {
        nb = write(fd, &fz, sizeof(fz));
    } while (nb < 0 && errno == EINTR);

    if (nb == (ssize_t) sizeof(fz) && pg_fsync(fd) == 0)
    {
        CloseTransientFile(fd);
        if (rename(tmp, path) != 0)
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: 无法就位 freeze 文件 \"%s\": %m", path)));
    }
    else
    {
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: freeze 文件 \"%s\" 写入失败: %m", tmp)));
        CloseTransientFile(fd);
        (void) unlink(tmp);
    }
}

static bool
FreezeEntriesEqual(const PartWALFreezeEntry *a, int na,
                   const PartWALFreezeEntry *b, int nb)
{
    int i;

    if (na != nb)
        return false;
    for (i = 0; i < na; i++)
        if (a[i].role != b[i].role || a[i].ord != b[i].ord ||
            a[i].relfrozenxid != b[i].relfrozenxid ||
            a[i].relminmxid != b[i].relminmxid)
            return false;
    return true;
}

/*
 * T5.4b-2（设计 §6.7）：把本分片的两个 vacuum 水位发进分区流。
 *
 * **尽力而为**，与冻结账目同一条纪律：发不出去绝不能把调用方的 vacuum 带下水。
 * 失败的后果是 follower 的免查区落后于 leader —— 这是**安全方向**（follower
 * 只会少信一点，不会多信），且水位发的是**绝对值**不是增量，下一轮截断自然
 * 补上。
 *
 * 只对**本节点维护着 fileset 的分区**发射（`LoadShardFileSet` 探测）。这道
 * 门同时挡掉两类情形：非复制的本地打标表（发了只会凭空造出 pg_parwal 目录），
 * 以及 follower 侧（那里天然没有持久化 fileset）。
 */
void
ShardVacuumEmitWatermarkCtrl(Oid shard_oid, TransactionId trunc_before,
                             TransactionId vacuum_xid)
{
    ShardFileSet              probe;
    PartWALCtrlFreezeUpdate  *payload;
    PartWALFreezeVacuumWm    *wm;
    uint32                    payload_len;
    MemoryContext             oldcxt;

    if (!IsTransactionState() || !OidIsValid(MyDatabaseId))
        return;
    if (!LoadShardFileSet(shard_oid, &probe))
        return;                 /* 不是本节点维护的复制分区 */

    payload_len = (uint32) PartWALCtrlFreezeUpdateSizeEx(0, true);
    payload = palloc0(payload_len);
    payload->nrels = 0;         /* 纯水位记录：不带冻结账目 */
    payload->flags = PARTWAL_FREEZE_HAS_VACUUM_WM;
    wm = PartWALCtrlFreezeVacuumWm(payload);
    wm->clog_truncate_before = (uint32) trunc_before;
    wm->shard_vacuum_xid     = (uint32) vacuum_xid;

    oldcxt = CurrentMemoryContext;      /* 必须在 PG_TRY 之前存 */
    PG_TRY();
    {
        PartWALAppendCtrl(shard_oid, PARTWAL_CTRL_FREEZE_UPDATE,
                          (const char *) payload, payload_len);
        ereport(DEBUG1,
                (errmsg("pg_partdist: shard %u vacuum 水位已发射（%u/%u）",
                        shard_oid, trunc_before, vacuum_xid)));
    }
    PG_CATCH();
    {
        ErrorData *ed;

        MemoryContextSwitchTo(oldcxt);
        ed = CopyErrorData();
        FlushErrorState();
        ereport(WARNING,
                (errmsg("pg_partdist: shard %u vacuum 水位发射失败（%s）——"
                        "follower 免查区暂时落后，下一轮截断会补上",
                        shard_oid, ed->message)));
        FreeErrorData(ed);
    }
    PG_END_TRY();

    pfree(payload);
}

/*
 * 单个 shard 的冻结账目检查 + 发射。调用方保证在事务上下文里。
 * 返回 true = 发了一条 CTRL。
 */
static bool
ShardFreezeEmitOne(Oid shard_oid)
{
    PartWALFreezeEntry       cur[SHARD_FILESET_MAX_RELS];
    ShardFreezeFile          old;
    int                      n;
    PartWALCtrlFreezeUpdate *payload;
    uint32                   payload_len;
    MemoryContext            oldcxt;

    /* 只走 syscache —— 见 BuildShardFreeze 头部关于"提交回调里不许取锁"的说明 */
    n = BuildShardFreeze(shard_oid, cur);
    if (n == 0)
        return false;           /* 表没了 */

    if (LoadShardFreeze(shard_oid, &old) &&
        FreezeEntriesEqual(old.rels, old.nrels, cur, n))
        return false;           /* 没变，绝大多数检查走到这里就返回 */

    payload_len = (uint32) PartWALCtrlFreezeUpdateSize(n);
    payload = palloc0(payload_len);
    payload->nrels = (uint32) n;
    payload->flags = 0;      /* D2 的冻结账目：不带 vacuum 水位块 */
    memcpy(PartWALCtrlFreezeRels(payload), cur,
           (size_t) n * sizeof(PartWALFreezeEntry));

    /*
     * ★ 冻结账目的发射是**尽力而为**，失败绝不能把调用方的事务带下水。
     *
     * 与 FILESET_UPDATE 的区别是本质的：结构变了却没通知副本，DDL 就不该
     * 提交（否则副本静默分歧）；而 relfrozenxid 只是一个账目字段，为它中止
     * 用户的一次 VACUUM 是本末倒置 —— 而这恰恰是实测会发生的事：VACUUM
     * 刷出的 FPI 洪水会把 Raft 心跳饿死、领导权移走，紧接着的
     * PartWALAppendCtrl 就以"本节点不是该分区组的 leader"报错，整个 VACUUM
     * 随之中止。
     *
     * 失败时**不更新基线**，于是下一次检查会重发；等领导权回来自然补上。
     */
    oldcxt = CurrentMemoryContext;      /* 必须在 PG_TRY 之前存 */
    PG_TRY();
    {
        PartWALAppendCtrl(shard_oid, PARTWAL_CTRL_FREEZE_UPDATE,
                          (const char *) payload, payload_len);
    }
    PG_CATCH();
    {
        ErrorData *ed;

        /*
         * 先切回进 PG_TRY 之前的上下文再 CopyErrorData —— 它断言
         * CurrentMemoryContext != ErrorContext，且结果分配在当前上下文里。
         * 写成 MemoryContextSwitchTo(CurrentMemoryContext) 是空操作，起不到
         * 任何作用（同 replay_worker.c 里既有的 PG_CATCH 写法）。
         */
        MemoryContextSwitchTo(oldcxt);
        ed = CopyErrorData();
        FlushErrorState();

        ereport(WARNING,
                (errmsg("pg_partdist: shard %u 冻结账目发射失败，下次检查重试: %s",
                        shard_oid, ed->message)));
        FreeErrorData(ed);
        pfree(payload);
        return false;           /* 基线保持不变 ⇒ 下次会重发 */
    }
    PG_END_TRY();

    pfree(payload);

    /*
     * 基线在发射**成功之后**才更新：先更新再发射的话，发射一旦失败基线就
     * 领先了实际，这一轮的变化从此再也不会被重发。
     */
    StoreShardFreeze(shard_oid, cur, n);

    ereport(LOG,
            (errmsg("pg_partdist: shard %u 冻结账目已发射（%d 个堆关系）",
                    shard_oid, n)));
    return true;
}

void
ShardFreezeMaybeEmitUpdates(void)
{
    Oid                parts[PARTWAL_RELFHASH_SIZE];
    int                nparts;
    int                p;

    /*
     * ★★ 两条守卫缺一不可，缺后一条会**把整个节点搞崩**。
     *
     * `IsTransactionState()` 只回答"现在能不能跑 SQL"，**不回答"我有没有
     * 数据库"**。而本函数是**时间驱动**的：它在**每一个**事务的 PRE_COMMIT
     * 都会被调到，包括那些没有选数据库的辅助进程的事务 —— 典型就是
     * **autovacuum launcher**：它按设计不绑定数据库（要遍历所有库），却会
     * 开事务去读 pg_database。在它的事务里走到 BuildShardFileSetEx 读
     * pg_class，会当场
     *     FATAL: cannot read pg_class without having selected a database
     * 而 launcher 一死，postmaster 就 "terminating any other active server
     * processes; reinitializing" —— **整个节点重置**。
     *
     * 实测症状极具迷惑性：只有跑得久的用例（R1/L1，十几分钟）会挂，短用例
     * 全过 —— 因为 autovacuum_naptime 默认 60 秒，短用例撞不上。
     *
     * 对比 §12 的 fileset 发射器：它有 fileset_maybe_changed 脏标记护着，
     * 而那个标记只有 ProcessUtility 会置（辅助进程不跑 utility 语句），
     * 所以它天然避开了这条路。**时间驱动的没有这层保护，必须自己判。**
     */
    if (!IsTransactionState() || !OidIsValid(MyDatabaseId))
        return;

    /*
     * ★★★ 白名单：本 backend 必须已经执行过至少一条**用户语句**。
     *
     * 这是本模块交学费最贵的一条守卫。先后撞了四种进程，每次都以为"再补一条
     * 黑名单就完了"，下一轮换个进程接着崩：
     *
     *   ① autovacuum launcher（不绑数据库）→ 读 pg_class FATAL → 节点重置
     *   ② Citus 维护守护进程（B_BG_WORKER，跑 2PC 恢复 + 分布式死锁检测）
     *   ③ Citus 内部 backend（application_name 以 citus_ 开头）
     *   ④ **任何新连接的 InitPostgres 引导事务** —— 这个最致命：它是
     *      B_BACKEND、有数据库、application_name 还没设，前三条黑名单全部
     *      放行，而此刻 backend 根本没初始化完。实测栈回溯：
     *          InitPostgres → CommitTransactionCommand → PartWALXactCallback
     *          → ShardFreezeMaybeEmitUpdates → ShardFreezeEmitOne
     *          → PartWALAppendCtrl → PartWALReplicateTouched
     *          → pg_raft_partwal_replicate → data_propose_one
     *          → text_to_cstring → pg_detoast_datum_packed   ← SIGSEGV
     *      于是**每建一条连接就赌一次**；节点被打死后该分区组失去多数派、
     *      follower 追不平，最终表现成"回放出的文件 diff 不一致"。
     *
     * 黑名单永远补不完，改成白名单：**只有跑过用户语句的 backend 才发射**。
     * 标记由 ExecutorStart / ProcessUtility 两个钩子置位，它们只在真实语句上
     * 触发；引导事务和辅助进程的内部事务一条都走不到。发射器本就是时间驱动
     * 的慢账目同步，"等这个 backend 先干点正事"没有任何代价。
     *
     * 下面的进程类型判断保留为第二道门 —— 同一原则的另一面：发射一次 = 一次
     * **同步 Raft 复制**，要阻塞等对端多数派 ack，于是任何"本身就在管理到
     * 对端连接"的进程，一旦被挂上这个钩子就会重入自己：
     *
     *   - **Citus 维护守护进程**（B_BG_WORKER）。它跑 2PC 恢复和分布式死锁
     *     检测，后者会向对端发 dump_local_wait_edges RPC。实测日志：
     *         [174849] LOG: pg_partdist: shard 139370 冻结账目已发射（2 个堆关系）
     *         [174849] STATEMENT: SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'citus\_2\_%'
     *     ——发射器跑在它的 2PC 恢复事务里；1.7 秒后同节点一个 backend
     *         server process (PID 175085) was terminated by signal 11
     *     节点整体重置 → 该组失去多数派 → follower 追不平 → R1 大面积 FAIL。
     *
     *   - **Citus 内部 backend**（B_BACKEND，但 application_name 以 citus_ 开头：
     *     citus_internal gpid= / citus_run_command gpid= / citus_rebalancer gpid=）。
     *     它是"本节点在给别的节点干活"，在它的提交点上反向同步等对端同样重入。
     *
     *   - autovacuum worker / launcher、并行 worker、我们自己的 replay worker。
     *
     * 排除 autovacuum worker 看似矛盾（推进 relfrozenxid 的正是它），其实不然：
     * 发射器是**时间驱动 + 与持久化基线 diff**，不依赖"谁改的"，晚一点由任意
     * 普通 backend 的提交点捎带上即可。账目是慢变量，这点滞后无意义。
     */
    if (!shard_freeze_user_activity ||
        MyBackendType != B_BACKEND ||
        (application_name != NULL && strncmp(application_name, "citus_", 6) == 0))
        return;

    /*
     * ★★ 本事务只要还有未落盘的分区记录，就**整轮跳过**，等下一次检查。
     *
     * 冻结账目是尽力而为的账目同步，没有任何理由和一个正在写分片流的事务
     * 交织。硬要在那种事务里追加 CTRL，就得先把它的 DATA 排空（§12 的
     * fileset 发射器正是这么做的）—— 而实测表明那样仍会打乱流：R1 的
     * VACUUM 尾部截断（RM_SMGR）在 follower 上不生效，主堆停在 40960 而
     * leader 已截到 16384，索引/TOAST 却都对得上。
     *
     * 与其去猜排空的边界条件，不如**由构造回避**：跳过一轮的代价是账目晚
     * 60 秒同步，而 relfrozenxid 是以千万 xid 为尺度变化的慢变量。
     * fileset 发射器不能这么做（结构变更必须与那次 DDL 同事务发出去），
     * 冻结账目可以 —— 两者的可延迟性本就不同。
     */
    if (PartWALHasPendingRecords())
        return;

    /*
     * 间隔守卫。为什么需要它：**autovacuum 推进 relfrozenxid 不走
     * ProcessUtility**（它在 autovacuum worker 里直接调 vacuum_rel），
     * 所以 D1 那套"DDL 后置脏标记"的检测对它完全无效。这里改用时间驱动，
     * 让任何有事务流过的 leader 都会周期性地把账目对一遍。
     *
     * relfrozenxid 是以千万 xid 为尺度变化的慢变量，分钟级的滞后毫无影响。
     *
     * ★★★ 水位在**共享内存**里（PartWALFreezeCheckDue），不是 backend 本地
     * static。本地 static 的初值 0 = "从没查过"，于是每个新连接的第一次提交
     * 都会无视间隔立刻发射 —— R1 开几十条短命 psql 连接，实测就成了几十次
     * 发射，且恰好压在 VACUUM 灌 Raft 日志环的窗口上：128 槽的环被顶爆
     * （"log ring full, cannot append"）→ VACUUM 事务中止 → 而它的物理截断在
     * leader 上已经做掉了，follower 永远收不到那条 SMGR 截断记录 → **永久
     * 分叉**（表现为 R1 的 TOAST 文件两侧大小对不上）。
     * 换成节点级水位后，整个节点每 interval 只查一次。
     */
    if (!PartWALFreezeCheckDue(freeze_sync_interval_ms))
        return;

    nparts = PartWALSyncListPartitions(parts, lengthof(parts));

    for (p = 0; p < nparts; p++)
    {
        ShardFileSet probe;

        /* 没有持久化 fileset 的分区不归本节点维护（follower 侧天然为空） */
        if (!LoadShardFileSet(parts[p], &probe))
            continue;

        (void) ShardFreezeEmitOne(parts[p]);
    }
}

void
ShardFilesetMaybeEmitUpdates(void)
{
    Oid     parts[PARTWAL_RELFHASH_SIZE];
    int     nparts;
    int     p;
    bool    drained = false;

    if (!fileset_maybe_changed)
        return;
    fileset_maybe_changed = false;      /* 一个事务只做一遍 */

    /*
     * 同 ShardFreezeMaybeEmitUpdates 的 MyDatabaseId 守卫（见那里的长注释）。
     * 本函数靠脏标记天然避开了辅助进程（它们不跑 utility 语句），这条是
     * 纵深防御 —— 判据写在"要读 catalog"这件事本身上，比依赖调用者天然
     * 不会来更可靠。
     */
    if (!IsTransactionState() || !OidIsValid(MyDatabaseId))
        return;

    nparts = PartWALSyncListPartitions(parts, lengthof(parts));

    for (p = 0; p < nparts; p++)
    {
        ShardFileSet old_fs;
        ShardFileSet new_fs;
        Oid          new_relids[SHARD_FILESET_MAX_RELS];

        /* 没有持久化 fileset 的分区不归本节点维护（或还没注册过） */
        if (!LoadShardFileSet(parts[p], &old_fs))
            continue;

        /*
         * ★ T7.8（P7-D1，2026-09-09）：表被 DROP 掉了 —— 发 `SHARD_DROP`。
         *
         * 此处**原本是沉默的**（原注释："副本侧的处置需要另一个 opcode，属
         * 后续工作"）。沉默的代价：副本的壳表、回放槽位、`pg_parwal/<oid>`
         * 目录永不回收；而回收判据是"OID 不在本地 pg_class"，副本的壳表恰恰
         * 是本地真表，这条判据对副本天然不成立。
         *
         * 发射失败不上抛：DROP 本身是本地 DDL，已经在提交路上，不能因为
         * 通知不到副本就把它带崩。副本收不到只是回退到从前的行为（残留）。
         */
        if (BuildShardFileSetEx(parts[p], &new_fs, new_relids) < 1)
        {
            EmitShardDropNotice(parts[p]);
            continue;
        }

        if (FileSetEquals(&old_fs, &new_fs))
            continue;

        /*
         * 先把已缓冲的记录排空。它们引用的是**旧**文件号，必须排在
         * FILESET_UPDATE 之前 —— 排在后面的话，follower 已经换过表，
         * 旧文件号在 loc_map 里查不到，当场 "未知 relfilelocator" 报错。
         * 只在确实有变更时才排空，普通 DDL（比如 COMMENT ON）不受影响。
         */
        if (!drained)
        {
            PartWALFlush(InvalidXLogRecPtr, false);
            drained = true;
        }

        EmitFilesetUpdate(parts[p], &old_fs, &new_fs, new_relids);
    }
}

/* ================================================================== */
/* 注册 + 持久化                                                       */
/* ================================================================== */

static void
FileSetPath(Oid shard_oid, char *path, char *tmp)
{
    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, shard_oid, SHARD_FILESET_FILENAME);
    if (tmp != NULL)
        snprintf(tmp, MAXPGPATH, "%s/%s/%u/%s.tmp",
                 DataDir, PARTITION_WAL_DIR, shard_oid,
                 SHARD_FILESET_FILENAME);
}

void
RegisterShardFileSet(const ShardFileSet *fs)
{
    char    path[MAXPGPATH];
    char    tmp[MAXPGPATH];
    int     fd;
    ssize_t nb;
    int     i;

    /* 1) shmem 反向哈希：fileset 全体成员 relNumber → shard_oid */
    for (i = 0; i < fs->nrels; i++)
        PartWALSyncRegister(fs->shard_oid, fs->rels[i].loc.relNumber);

    /* 2) 原子持久化（tmp + fsync + rename），bgworker 启动时重建注册用 */
    InitPartitionWALDirectory(fs->shard_oid);
    FileSetPath(fs->shard_oid, path, tmp);

    /*
     * T7.8：本分片重新登记，说明它又归本节点维护了 —— 清掉上一轮的
     * "已通知过 DROP" 哨兵，否则同一个 OID 被复用后再删就不再发通知。
     */
    {
        char sentinel[MAXPGPATH];

        ShardDropSentinelPath(fs->shard_oid, sentinel, sizeof(sentinel));
        (void) unlink(sentinel);
    }

    fd = OpenTransientFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
    if (fd < 0)
    {
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法创建 fileset 临时文件 \"%s\": %m",
                        tmp)));
        return;
    }

    do {
        nb = write(fd, fs, sizeof(*fs));
    } while (nb < 0 && errno == EINTR);

    if (nb == (ssize_t) sizeof(*fs) && pg_fsync(fd) == 0)
    {
        CloseTransientFile(fd);
        if (rename(tmp, path) != 0)
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: 无法就位 fileset 文件 \"%s\": %m",
                            path)));
    }
    else
    {
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: fileset 文件 \"%s\" 写入失败: %m", tmp)));
        CloseTransientFile(fd);
        (void) unlink(tmp);
    }
}

bool
LoadShardFileSet(Oid shard_oid, ShardFileSet *fs)
{
    char    path[MAXPGPATH];
    int     fd;
    ssize_t nb;

    FileSetPath(shard_oid, path, NULL);

    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        return false;

    nb = read(fd, fs, sizeof(*fs));
    CloseTransientFile(fd);

    if (nb != (ssize_t) sizeof(*fs))
        return false;
    if (fs->magic != SHARD_FILESET_MAGIC || fs->shard_oid != shard_oid)
        return false;
    if (fs->nrels < 1 || fs->nrels > SHARD_FILESET_MAX_RELS)
        return false;

    return true;
}

void
LoadAllShardFileSets(void)
{
    char           dirpath[MAXPGPATH];
    DIR           *dir;
    struct dirent *de;

    snprintf(dirpath, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);

    dir = AllocateDir(dirpath);
    if (dir == NULL)
        return;

    while ((de = ReadDir(dir, dirpath)) != NULL)
    {
        Oid          shard_oid;
        char        *endptr;
        ShardFileSet fs;
        int          i;

        if (de->d_name[0] == '.')
            continue;

        shard_oid = (Oid) strtoul(de->d_name, &endptr, 10);
        if (*endptr != '\0' || shard_oid == 0)
            continue;

        if (!LoadShardFileSet(shard_oid, &fs))
            continue;

        for (i = 0; i < fs.nrels; i++)
            PartWALSyncRegister(shard_oid, fs.rels[i].loc.relNumber);

        ereport(DEBUG1,
                (errmsg("pg_partdist: 从持久化 fileset 重建 shard %u 的 "
                        "%d 个成员注册", shard_oid, fs.nrels)));
    }
    FreeDir(dir);
}

/* ================================================================== */
/* RM_SMGR main-data 解析（FRD §5.2 捕获侧特判）                       */
/* ================================================================== */

bool
SmgrRecordGetLocator(const char *record_data, uint32 record_len,
                     uint8 info, RelFileLocator *out)
{
    const char *p;
    const char *end;
    uint8       block_id;
    uint32      main_len;
    uint8       op = info & XLR_RMGR_INFO_MASK;

    if (record_data == NULL || record_len < SizeOfXLogRecord + 2)
        return false;

    p   = record_data + SizeOfXLogRecord;
    end = record_data + record_len;

    /*
     * 无块引用记录的布局：XLogRecord 头之后直接是 main-data 头
     * (XLR_BLOCK_ID_DATA_SHORT: 1 字节长度；_LONG: 4 字节长度)，
     * 随后即 xl_smgr_create / xl_smgr_truncate。
     */
    block_id = (uint8) *p++;
    if (block_id == XLR_BLOCK_ID_DATA_SHORT)
    {
        main_len = (uint8) *p++;
    }
    else if (block_id == XLR_BLOCK_ID_DATA_LONG)
    {
        if (p + sizeof(uint32) > end)
            return false;
        memcpy(&main_len, p, sizeof(uint32));
        p += sizeof(uint32);
    }
    else
        return false;           /* 出现块引用或未知布局：不是我们要的形态 */

    if (op == XLOG_SMGR_CREATE)
    {
        xl_smgr_create xlrec;

        if (main_len < sizeof(xlrec) || p + sizeof(xlrec) > end)
            return false;
        memcpy(&xlrec, p, sizeof(xlrec));
        *out = xlrec.rlocator;
        return true;
    }
    else if (op == XLOG_SMGR_TRUNCATE)
    {
        xl_smgr_truncate xlrec;

        if (main_len < sizeof(xlrec) || p + sizeof(xlrec) > end)
            return false;
        memcpy(&xlrec, p, sizeof(xlrec));
        *out = xlrec.rlocator;
        return true;
    }

    return false;
}

/* ================================================================== */
/* 副本供给（解冻批次 #7）                                            */
/* ================================================================== */

#include "libpq-fe.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

/* 取一列文本，取不到返回 NULL（palloc 在调用方的上下文里） */
static char *
prov_query_text(const char *sql)
{
    char *out = NULL;

    /*
     * ★ read_only 必须是 false：这些查询里带 `SET citus.override_table_visibility`
     * （不设它就看不见分片表，Citus 把分片表从 pg_class 扫描里藏了），
     * 而只读 SPI **不允许 SET**，会当场报错。首版写成 true，症状是
     * "CONTEXT: SQL statement \"SET citus.override_table_visibility=false; ...\""
     * —— 报错指着的是被拒的那条 SET，不是查询本身。
     */
    if (SPI_execute(sql, false, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);

        if (!isnull)
            out = TextDatumGetCString(d);
    }
    return out;
}

PG_FUNCTION_INFO_V1(partdist_provision_shard_replica);

/*
 * provision_shard_replica(global_shard_id bigint, target_node int) → text
 *
 * ★★ 解冻批次 #7：把「给分片 X 在节点 N 上建一个副本」从**纯手工**变成一个入口。
 *
 * 此前 register_shard_fileset / replay_set_locmap / replay_enable 在
 * pg-partdist-src/src 与 pg-raft-src/src 里**没有任何调用方** —— 只有验收脚本
 * 在调，每套件都要自己抄一遍那四十行样板。后果不只是麻烦：
 *   - 副本**只能靠人建**，raft 把某个节点登记成 secondary 也不会真的有副本；
 *   - R-P4-15 那种「旧主带着陈旧数据回归」只能靠**拒绝升主**兜底，
 *     而不是「重建副本让它归队」—— 因为根本没有重建这个动作。
 *
 * **本函数跑在 leader 上，向目标节点推**。选推不选拉，是因为 leader 手里已经
 * 有全部素材（fileset 就在本地），拉的话目标节点还得反过来读 leader 的目录。
 *
 * 步骤（顺序是要害）：
 *   ① 本地登记 fileset —— 成员集合以此刻为准；
 *   ② **先发物理基线**（ShardBaselineEmit），拿到它的 partition_lsn 作 base；
 *   ③ 再去目标节点建壳表 + 按 base 配 locmap + arm。
 * 反过来做（先配 locmap 再发基线）会让目标从 base=0 起追，正是 §13 约束 2
 * 「拷贝时记下静止点」没做时的老毛病 —— R-P4-20 那串 PANIC 的土壤。
 *
 * 壳表用 `LIKE <分布表>` 建：分布表在每个节点上都有 Citus 的空壳，而分片表
 * 只在放置节点上有，拿不到定义。
 */
Datum
partdist_provision_shard_replica(PG_FUNCTION_ARGS)
{
    int64           gsid = PG_GETARG_INT64(0);
    int32           target = PG_GETARG_INT32(1);
    StringInfoData  q;
    StringInfoData  remote;
    char           *shard_tbl = NULL;
    char           *logical = NULL;
    char           *host = NULL;
    char           *portstr = NULL;
    char           *roles = NULL, *ords = NULL, *spcs = NULL, *dbs = NULL, *rels = NULL;
    uint64          lead_wm = 0;
    Oid             loid = InvalidOid;
    uint64          base;
    char            conninfo[256];
    PGconn         *conn;
    PGresult       *res;
    text           *ret;
    MemoryContext   caller_cxt = CurrentMemoryContext;
    char           *remote_sql = NULL;
    char           *summary = NULL;

    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("provision_shard_replica() 限超级用户")));

    if (SPI_connect() != SPI_OK_CONNECT)
        ereport(ERROR, (errmsg("provision_shard_replica: SPI_connect 失败")));

    initStringInfo(&q);

    /* ① 本节点必须**就是**这个分片的主，否则无从推送 */
    appendStringInfo(&q,
                     "SELECT local_oid::text FROM partdist.shard_identity "
                     "WHERE global_shard_id = %lld", (long long) gsid);
    { char *t = prov_query_text(q.data); if (t) loid = (Oid) strtoul(t, NULL, 10); }
    if (!OidIsValid(loid))
    {
        SPI_finish();
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("provision_shard_replica: 本节点没有分片 %lld", (long long) gsid),
                 errhint("本函数须在该分片的 leader 上调用。")));
    }

    /*
     * ★ 「本地有这个分片」不等于「本节点是它的主」—— 副本上也有。
     *
     * 不拦的话，副本上调用会一路走到基线发射，被 pg_raft 的写栅栏拒掉
     * （"本节点不是该分区组的 leader"），报错指向 raft、看不出是**调错了节点**。
     * 判据取控制面的登记（partition_map.primary_node）与本节点的 raft 编号：
     * 编号是 pg_raft.node_id，**不是** Citus 的 groupid，两者差 1。
     */
    {
        char       *gstate;
        char       *prim;

        /*
         * ★ 判据取**本节点此刻的 raft 组状态**，与写栅栏查的是同一件事
         * （raft_consensus.c：`state != RAFT_LEADER` 即拒本地写）。
         *
         * 首版拿 partition_map.primary_node 比对本节点编号 —— 那是**控制面的
         * 登记**，它落后于选举：实测第一次调用时组已选出主、登记却还是"未登记"，
         * 于是自己把自己拦了。登记只留作报错时的旁证。
         */
        resetStringInfo(&q);
        appendStringInfo(&q,
                         "SELECT state FROM partdist.pg_raft_group_status() "
                         "WHERE group_id = %lld", (long long) gsid);
        gstate = prov_query_text(q.data);

        if (gstate == NULL || strcmp(gstate, "leader") != 0)
        {
            char        sbuf[64];
            char        pbuf[64];

            snprintf(sbuf, sizeof(sbuf), "%s", gstate ? gstate : "无该组");
            resetStringInfo(&q);
            appendStringInfo(&q,
                             "SELECT primary_node::text FROM partdist.partition_map "
                             "WHERE partition_id = %lld", (long long) gsid);
            prim = prov_query_text(q.data);
            snprintf(pbuf, sizeof(pbuf), "%s", prim ? prim : "未登记");
            SPI_finish();
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("provision_shard_replica: 本节点不是分片 %lld 的主"
                            "（本节点该组状态=%s，控制面登记的主=%s）",
                            (long long) gsid, sbuf, pbuf),
                     errdetail("供给要在 leader 上发起：它要先发一次物理基线，"
                               "而基线走 raft 写路径，副本上会被写栅栏拒掉。"),
                     errhint("到该分片的主节点上调用；组还没选出主时先等它收敛。")));
        }
    }

    resetStringInfo(&q);
    appendStringInfo(&q,
                     "SELECT hostname||':'||port FROM partdist.node_map "
                     "WHERE node_id = %d", target);
    host = prov_query_text(q.data);
    if (host == NULL)
    {
        SPI_finish();
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("provision_shard_replica: node_map 里没有节点 %d", target)));
    }
    portstr = strchr(host, ':');
    if (portstr != NULL)
        *portstr++ = '\0';

    resetStringInfo(&q);
    appendStringInfo(&q,
                     "SET citus.override_table_visibility=false; "
                     "SELECT %u::regclass::text", loid);
    shard_tbl = prov_query_text(q.data);

    resetStringInfo(&q);
    appendStringInfo(&q,
                     "SELECT logicalrelid::text FROM pg_dist_shard "
                     "WHERE shardid = %lld", (long long) gsid);
    logical = prov_query_text(q.data);
    if (shard_tbl == NULL || logical == NULL)
    {
        SPI_finish();
        ereport(ERROR,
                (errmsg("provision_shard_replica: 解析不出分片 %lld 的表名"
                        "（分片表=%s 分布表=%s）", (long long) gsid,
                        shard_tbl ? shard_tbl : "?", logical ? logical : "?")));
    }

    /* ② 登记 fileset（成员集以此刻为准） */
    resetStringInfo(&q);
    appendStringInfo(&q,
                     "SET citus.override_table_visibility=false; "
                     "SELECT partdist.register_shard_fileset(%u::regclass)::text", loid);
    (void) prov_query_text(q.data);

    /*
     * ★★ 阶段①：**先**去目标节点把壳表和身份建出来，之后才能发基线。
     *
     * 顺序是实测钉死的，不是偏好。目标节点没有壳表时，`shard_identity` 里就没有
     * 这个分片的本地 OID，raft 的字节**归不了档**，于是它 ack 不了 —— 三成员组里
     * 只有 leader 能 ack，基线提案卡在 1/3：
     *     WARNING: 组 N propose plsn=1 失败(state=2 last_log_index=0 ...)
     *     ERROR:   分区 X(组 N) record 1 未达多数派
     * 报错指着"未达多数派"，看着像 raft 有毛病，其实是**目标还没准备好收**。
     * 各验收脚本的手工配方里，壳表也一直是在建组之前建的 —— 那条隐含前提
     * 从没写下来过，本函数把它变成显式的两段。
     */
    /* 阶段①的远程语句：只建壳表 + 重建身份，让目标先能收字节 */
    initStringInfo(&remote);
    appendStringInfo(&remote,
                     "SET citus.enable_ddl_propagation=off; "
                     "CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING ALL); "
                     "ALTER TABLE %s SET (autovacuum_enabled=off, toast.autovacuum_enabled=off); "
                     "SELECT partdist.rebuild_shard_identity();",
                     shard_tbl, logical, shard_tbl);
    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%s dbname=postgres user=postgres "
             "connect_timeout=5 options='-c statement_timeout=60000'",
             host, portstr ? portstr : "5432");

    /*
     * ★ SPI_connect() 把 CurrentMemoryContext 切到 SPI 的过程上下文，
     * SPI_finish() 会把它整个释放 —— 上面所有 TextDatumGetCString / StringInfo
     * 的内存都在那里。首版在 SPI_finish 之后还用 shard_tbl / rels 拼返回值，
     * 那是**读已释放内存**。要跨过 SPI_finish 的串必须先搬到调用方上下文。
     */
    remote_sql = MemoryContextStrdup(caller_cxt, remote.data);
    shard_tbl  = MemoryContextStrdup(caller_cxt, shard_tbl);
    SPI_finish();

    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        char errbuf[256];

        strlcpy(errbuf, PQerrorMessage(conn), sizeof(errbuf));
        PQfinish(conn);
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_FAILURE),
                 errmsg("provision_shard_replica: 连不上目标节点 %d (%s): %s",
                        target, conninfo, errbuf)));
    }
    res = PQexec(conn, remote_sql);
    if (PQresultStatus(res) != PGRES_TUPLES_OK &&
        PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        char errbuf[512];

        strlcpy(errbuf, PQerrorMessage(conn), sizeof(errbuf));
        PQclear(res);
        PQfinish(conn);
        ereport(ERROR,
                (errmsg("provision_shard_replica: 目标节点 %d 建壳表失败: %s",
                        target, errbuf)));
    }
    PQclear(res);

    /* ── 阶段②：目标已能收字节，这才发基线；base 取基线那一刻的 plsn ── */
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        PQfinish(conn);
        ereport(ERROR, (errmsg("provision_shard_replica: SPI_connect 失败(阶段②)")));
    }
    initStringInfo(&q);
    appendStringInfo(&q,
                     "SET citus.override_table_visibility=false; "
                     "SELECT partdist.shard_baseline_emit(%u::regclass)::text", loid);
    { char *t = prov_query_text(q.data); base = t ? strtoull(t, NULL, 10) : 0; }

    /* 成员清单在基线之后读：成员集与基线同源 */
    resetStringInfo(&q);
    appendStringInfo(&q,
                     "SET citus.override_table_visibility=false; "
                     "SELECT string_agg(role::text,',' ORDER BY role,ord), "
                     "       string_agg(ord::text,',' ORDER BY role,ord), "
                     "       string_agg(spc::text,',' ORDER BY role,ord), "
                     "       string_agg(db::text,',' ORDER BY role,ord), "
                     "       string_agg(relnum::text,',' ORDER BY role,ord) "
                     "  FROM partdist.shard_fileset(%u::regclass)", loid);
    if (SPI_execute(q.data, false, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool isnull;
        roles = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
        ords  = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
        spcs  = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull));
        dbs   = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 4, &isnull));
        rels  = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 5, &isnull));
    }
    if (roles == NULL)
    {
        SPI_finish();
        PQfinish(conn);
        ereport(ERROR, (errmsg("provision_shard_replica: 分片 %lld 的 fileset 为空",
                               (long long) gsid)));
    }

    /*
     * ★★ 把 leader 的**分片发号水位**一并带过去。
     *
     * 纯靠物理基线建出来的副本，元组里带的是**分片 xid**（页镜像原样拷过来的），
     * 可它没收过任何 MARKER，本地水位是 0。两处会因此判错：
     *   - T6.3c 的读闸门判据是 `ShardXidAllocWatermark > 0`（"在不在分片 xid
     *     宇宙里"），水位 0 就被当成**遗留副本**放行 —— 而遗留副本之所以能读，
     *     前提正是"元组带原生 xid"，在这里根本不成立；
     *   - T6.4 的切主认领扫 [claim_wm, watermark)，水位 0 ⇒ 区间为空 ⇒
     *     一条无主 RUNNING 都认领不到。
     * 实测就是前者：供给完之后读副本壳表**没有被拦**。
     */
    resetStringInfo(&q);
    appendStringInfo(&q,
                     "SELECT partdist.shard_xid_next(%u::oid)::text", loid);
    { char *t = prov_query_text(q.data); lead_wm = t ? strtoull(t, NULL, 10) : 0; }

    /*
     * ★ 必须 initStringInfo 而不是 resetStringInfo：`remote` 是在**阶段①的
     * SPI 上下文**里 init 的，第一次 SPI_finish() 已经把那块内存整个释放。
     * reset 之后往里 append 就是写已释放内存 —— 实测症状是远程 SQL 被拦腰
     * 拼错：`ARRAY[5517348,SET citus.enab...`，报回来的却是目标节点的
     * "syntax error at or near citus"，看着像 SQL 拼串写错了，其实是内存问题。
     * 这是同一个上下文陷阱在本函数里的第二次，第一次是 SPI_finish 之后还用
     * shard_tbl / rels 拼返回值。
     */
    initStringInfo(&remote);
    appendStringInfo(&remote,
                     "SET citus.enable_ddl_propagation=off; "
                     "SELECT partdist.replay_set_locmap('%s', ARRAY[%s], ARRAY[%s], "
                     "ARRAY[%s]::oid[], ARRAY[%s]::oid[], ARRAY[%s]::oid[], %llu::bigint); "
                     "SELECT partdist.replay_enable('%s'); "
                     "SELECT partdist.shard_xid_raise_watermark('%s'::regclass::oid, %llu);",
                     shard_tbl, roles, ords, spcs, dbs, rels,
                     (unsigned long long) base, shard_tbl,
                     shard_tbl, (unsigned long long) lead_wm);
    resetStringInfo(&q);
    appendStringInfo(&q,
                     "shard=%lld target_node=%d table=%s base=%llu xid_wm=%llu members=%s",
                     (long long) gsid, target, shard_tbl,
                     (unsigned long long) base, (unsigned long long) lead_wm, rels);
    remote_sql = MemoryContextStrdup(caller_cxt, remote.data);
    summary    = MemoryContextStrdup(caller_cxt, q.data);
    SPI_finish();

    res = PQexec(conn, remote_sql);
    if (PQresultStatus(res) != PGRES_TUPLES_OK &&
        PQresultStatus(res) != PGRES_COMMAND_OK)
    {
        char errbuf[512];

        strlcpy(errbuf, PQerrorMessage(conn), sizeof(errbuf));
        PQclear(res);
        PQfinish(conn);
        ereport(ERROR,
                (errmsg("provision_shard_replica: 目标节点 %d 配 locmap/arm 失败: %s",
                        target, errbuf)));
    }
    PQclear(res);
    PQfinish(conn);

    ret = cstring_to_text(summary);
    PG_RETURN_TEXT_P(ret);
}

/* ================================================================== */
/* 分叉标记（§13 约束 13 的"检测"那一半，批次 #8）                    */
/* ================================================================== */

#define SHARD_DIVERGED_FILENAME "diverged"
#define SHARD_PROMOTED_FILENAME "promoted"

/*
 * §13 约束 13 的机制，读原文才看清它比"提案被静默丢弃"微妙得多：
 *
 *   复制挂钩失败时事务**确实**中止了（replicate_group_upto 对任何一条未达
 *   多数派即 ERROR，三个生产调用点全走它，所以提交路径处处 fail-closed）。
 *   问题在于 `lazy_truncate_heap()` 的物理截断**在 leader 上已经做掉，且不随
 *   事务回滚** —— 内核在 AccessExclusiveLock 下截空页，认定安全。
 *   于是 leader 短了、follower 没短，那条 XLOG_SMGR_TRUNCATE 再也不会重发。
 *
 * 原文把它定性成「永久分叉，**既无检测也无修复路径**」。**修复路径这句已经
 * 不成立了**：T6.1 的 shard_baseline_emit 与批次 #7 的 provision_shard_replica
 * 正是重做物理基线。缺的只是**检测** —— 本节补的就是它。
 *
 * 标记必须是**非事务性**的：出事的那个事务马上就要中止，写进表里会一起回滚，
 * 等于没记。所以落成 pg_parwal/<oid>/diverged 这个文件，与水位/fileset 同侧。
 */
static void
shard_diverged_path(char *path, size_t len, Oid shard_oid)
{
    snprintf(path, len, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, shard_oid, SHARD_DIVERGED_FILENAME);
}

void
ShardMarkDiverged(Oid shard_oid, const char *reason)
{
    char    path[MAXPGPATH];
    char    buf[512];
    int     fd;
    int     n;

    if (!OidIsValid(shard_oid))
        return;

    shard_diverged_path(path, MAXPGPATH, shard_oid);
    n = snprintf(buf, sizeof(buf), "%s|%s\n",
                 timestamptz_to_str(GetCurrentTimestamp()),
                 reason ? reason : "(无)");

    fd = OpenTransientFile(path, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
    if (fd < 0)
    {
        /* 目录可能还没建（分片从没写过流）—— 记不下就只告警，不再添乱 */
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法写分叉标记 \"%s\": %m", path)));
        return;
    }
    if (write(fd, buf, n) != n || pg_fsync(fd) != 0)
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 分叉标记 \"%s\" 落盘不完整: %m", path)));
    CloseTransientFile(fd);
}

/* 返回标记内容（palloc），没有标记返回 NULL */
char *
ShardDivergedReason(Oid shard_oid)
{
    char    path[MAXPGPATH];
    char    buf[512];
    int     fd;
    int     n;

    if (!OidIsValid(shard_oid))
        return NULL;

    shard_diverged_path(path, MAXPGPATH, shard_oid);
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        return NULL;
    n = read(fd, buf, sizeof(buf) - 1);
    CloseTransientFile(fd);
    if (n <= 0)
        return NULL;
    buf[n] = '\0';
    if (buf[n - 1] == '\n')
        buf[n - 1] = '\0';
    return pstrdup(buf);
}

void
ShardClearDiverged(Oid shard_oid)
{
    char    path[MAXPGPATH];

    if (!OidIsValid(shard_oid))
        return;
    shard_diverged_path(path, MAXPGPATH, shard_oid);
    if (unlink(path) != 0 && errno != ENOENT)
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 分叉标记 \"%s\" 删除失败: %m", path)));
}

PG_FUNCTION_INFO_V1(partdist_shard_divergence);
Datum
partdist_shard_divergence(PG_FUNCTION_ARGS)
{
    Oid     shard = PG_GETARG_OID(0);
    char   *r = ShardDivergedReason(shard);

    if (r == NULL)
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(cstring_to_text(r));
}

PG_FUNCTION_INFO_V1(partdist_shard_clear_divergence);
Datum
partdist_shard_clear_divergence(PG_FUNCTION_ARGS)
{
    Oid     shard = PG_GETARG_OID(0);

    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("shard_clear_divergence() 限超级用户"),
                 errdetail("清标记不等于修好了分叉：真正的修复是重做物理基线"
                           "（shard_baseline_emit / provision_shard_replica），"
                           "它们成功后会自己清。")));
    ShardClearDiverged(shard);
    PG_RETURN_VOID();
}


/*
 * ShardPromotedMark* —— "本节点已是该分片的主"这件事的**持久**记号（批次 #9）。
 *
 * ★ 批次 #7 把它只放在共享内存的槽位里，注释还写着"不持久化：重启后节点若仍是
 * leader，升主路径会再跑一遍并重新置位"。**那句话是错的** —— 重启之后没有任何
 * 东西会重跑交接（交接由 group0 apply 触发，而主权没再变过就不会再 apply 一次）。
 * 于是一个**注册在案的主**在重启后被自己的副本闸门永久拦住。
 *
 * 实测代价：dtx_tso_p4 的 M5/S1 两条长期报"实际取不到值"，把 stderr 捞出来才
 * 看见是「不允许在本节点上对副本壳表执行查询」—— 协调器的分布式读被路由到那个
 * 节点，撞在自己的闸门上。这条红在 T6.7 基线里就有，一直没人查到底。
 */
static void
shard_promoted_path(char *path, size_t len, Oid shard_oid)
{
    snprintf(path, len, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, shard_oid, SHARD_PROMOTED_FILENAME);
}

void
ShardPromotedMarkWrite(Oid shard_oid, bool promoted)
{
    char    path[MAXPGPATH];
    int     fd;

    if (!OidIsValid(shard_oid))
        return;
    shard_promoted_path(path, MAXPGPATH, shard_oid);

    if (!promoted)
    {
        if (unlink(path) != 0 && errno != ENOENT)
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: 升主记号 \"%s\" 删除失败: %m", path)));
        return;
    }

    fd = OpenTransientFile(path, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
    if (fd < 0)
    {
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法写升主记号 \"%s\": %m", path)));
        return;
    }
    if (write(fd, "1\n", 2) != 2)
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 升主记号 \"%s\" 写入不完整: %m", path)));
    (void) pg_fsync(fd);
    CloseTransientFile(fd);
}

bool
ShardPromotedMarkRead(Oid shard_oid)
{
    char        path[MAXPGPATH];
    struct stat st;

    if (!OidIsValid(shard_oid))
        return false;
    shard_promoted_path(path, MAXPGPATH, shard_oid);
    return (stat(path, &st) == 0);
}

PG_FUNCTION_INFO_V1(partdist_repair_diverged_shards);

/*
 * repair_diverged_shards() → text
 *
 * 批次 #9：把「见到分叉标记就去重做基线」从**逐个分片手工**变成一次调用。
 *
 * 扫 pg_parwal/ 下所有带 diverged 标记的分片，对**本节点是组 leader** 的那些
 * 重发物理基线（基线成功会自己清标）。不是 leader 的跳过并说明 —— 基线要走
 * raft 写路径，副本上发不出去。
 *
 * ★ 为什么不做成"自动触发"：唯一能保证 quorum 已恢复的时刻是下一次复制成功，
 * 而那在**提交路径**上；在那里放大成一次全量 FPI 洪水，代价与风险都要单独评估。
 * 现在的形态是：检测自动、修复一条命令、升主路径顺带跑一次。
 */
Datum
partdist_repair_diverged_shards(PG_FUNCTION_ARGS)
{
    char            dirpath[MAXPGPATH];
    DIR            *d;
    struct dirent  *de;
    StringInfoData  out;
    int             nrepaired = 0, nskipped = 0, nstale = 0;

    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("repair_diverged_shards() 限超级用户")));

    initStringInfo(&out);
    snprintf(dirpath, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);
    d = AllocateDir(dirpath);
    if (d == NULL)
        PG_RETURN_TEXT_P(cstring_to_text("repaired=0 skipped=0 stale_cleaned=0（没有 pg_parwal 目录）"));

    while ((de = ReadDir(d, dirpath)) != NULL)
    {
        Oid     shard;
        char   *reason;

        if (de->d_name[0] < '0' || de->d_name[0] > '9')
            continue;
        shard = (Oid) strtoul(de->d_name, NULL, 10);
        reason = ShardDivergedReason(shard);
        if (reason == NULL)
            continue;

        /*
         * ★ 表已经不在了 ⇒ 标记是孤儿，直接清掉。
         *   不清的话它会**永远**堆在报告里：实测一次调用就 skipped=7，
         *   全是早先夹具留下的、表早已 DROP 的分片。一个越用越吵的报告
         *   等于没有报告 —— 真出事的那一条会被淹掉。
         */
        if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(shard)))
        {
            ShardClearDiverged(shard);
            nstale++;
            continue;
        }

        /*
         * 只对本节点是组 leader 的分片动手。判据与写栅栏同源 —— 副本上发基线
         * 一定被拒，硬试只会把一个可读的报告变成一串 ERROR。
         */
        PG_TRY();
        {
            (void) ShardBaselineEmit(shard);   /* 成功即清标 */
            nrepaired++;
            appendStringInfo(&out, " repaired:%u", shard);
        }
        PG_CATCH();
        {
            /* 发不出去（多半不是 leader / 多数派没恢复）：留标记，报出来 */
            FlushErrorState();
            nskipped++;
            appendStringInfo(&out, " skipped:%u", shard);
        }
        PG_END_TRY();
    }
    FreeDir(d);

    {
        StringInfoData s2;

        initStringInfo(&s2);
        appendStringInfo(&s2, "repaired=%d skipped=%d stale_cleaned=%d%s",
                         nrepaired, nskipped, nstale, out.data);
        PG_RETURN_TEXT_P(cstring_to_text(s2.data));
    }
}

/* ================================================================== */
/* 路由层：升主的角色切换（FRD §11 步骤 5）                            */
/* ================================================================== */

PG_FUNCTION_INFO_V1(partdist_route_status);

/*
 * PartDistRoutePromote —— FRD §11 步骤 5 点名的入口（R4，批次 #10）。
 *
 * 原文：「角色置 SHARD_PROMOTED、登记水位，同一临界区原子生效。此后写路由切到
 * 本节点，`wal_insert_hook` 开始为其捕获新流」。它在 FRD 里只有签名、没有实现。
 *
 * 落地时把三件事写在一处：
 *   ① **角色** —— 置持久 promoted 记号（解除 T6.3c 的副本读闸门，并让
 *      replay_catchup 拒绝再拿别人的流盖自己的表）；
 *   ② **捕获** —— 把本地 fileset 登记进反向哈希，`wal_insert_hook` 才认得
 *      新主的写入、把它们送进分区流；
 *   ③ **水位** —— 分配器水位由 T6.5 的回放路径维护，这里只做观测面暴露，
 *      不重复置位（重复置位会与回放争写，且没有新信息）。
 *
 * ★ 关于②：`EnsurePartWALRegistered` 本来就会在新主**第一次 DML** 时惰性登记
 *   （`InitPartitionWALAndRegister`）。这里提前做一次是**把窗口关掉**：
 *   惰性登记发生在语句执行期，而 fileset 遍历要 catalog、要事务态；
 *   在交接点显式做掉，新主对外服务的第一条写就一定被捕获。
 *
 * ★ 关于"同一临界区原子生效"：**没有做到，如实记**。角色走 ReplayCtl->lock、
 *   捕获走 PartWALCtl->lock，两把锁跨不到一起，硬凑要新引一把全局锁。
 *   实际需要的性质是"**两件事都在对外服务之前完成**" —— 交接点在
 *   OP_PARTITION_PRIMARY apply 里，而路由登记（pg_dist_placement）在同一
 *   apply 的后半段，这个顺序已经保证了它。
 */
void
PartDistRoutePromote(Oid shard_oid)
{
    ShardFileSet fs;

    if (!OidIsValid(shard_oid))
        return;

    /* ① 角色 */
    ShardReplicaSetPromoted(shard_oid, true);

    /* ② 捕获 */
    if (BuildShardFileSet(shard_oid, &fs) > 0)
        RegisterShardFileSet(&fs);

    /*
     * ③ **打标身份**（T7.4 / R-P6-21，2026-09-09 补）。
     *
     * 缺陷现场：判定一张表是否分片打标只看本节点的白名单 GUC 或 shmem 集合
     * （`shard_oid_is_mvcc`），而该集合只由 `partdist_set_shard_mvcc()` 或
     * **重启时扫 `pg_shard_xid/` 目录**装载。副本从来不设白名单，
     * `provision_shard_replica` / 本函数也都不加 —— 于是**升主后的新主若既没有
     * 人工加白名单、又没重启，写入就不打标、读走原生路径**。
     * `handover_provision_p7` [3b] 与 `promote_catchup_tx3` [4] 都是在这个状态下
     * 通过的：通过的原因是错的。演示文档 6c 因此要求"给组内全部成员配白名单"。
     *
     * 判据用**持久证据**而不是猜：`pg_shard_clog/<oid>` 目录只由回放路径在收到
     * 带分片 xid 的 MARKER 时创建，所以它存在 ⇔ 这个分片的流里带过分片 xid
     * ⇔ 它是打标分片。补两件事，与 `partdist_set_shard_mvcc()` 的 ②③ 步同源：
     *   · `ShardMvccEnsureWatermarkFile()` —— 预创建水位文件，它同时**就是**
     *     启动装载的登记表，于是这次交接跨重启也不会丢；
     *   · `ShardMvccSetAdd()` —— 本次进程内立即生效，不必等重启。
     *
     * 不做的事：不碰 `partition_map.shard_mvcc` 真相列。那是控制面的事，
     * 由 group0 复制；这里只负责"本节点认得出自己手上这张表是打标表"。
     */
    if (ShardClogDirExists(shard_oid))
    {
        ShardMvccEnsureWatermarkFile(shard_oid);
        ShardMvccSetAdd(shard_oid);
        elog(LOG, "pg_partdist: 分片 %u 升主时继承打标身份（pg_shard_clog 存在）",
             shard_oid);
    }

    /*
     * ④ **文件号交接**（T7.3 / R-P6-16，2026-09-09 补）。
     *
     * 新主此后写进流里的记录带的是它自己的 relfilenumber，而其余副本的 locmap
     * 还对着旧主的号 —— 不广播就报「未知 relfilelocator ... (fileset 漏登记)」，
     * 那些副本从此放不了新主的流、也失去再次当选资格（R-P4-15 拦升主的那一格），
     * 直到有人从新主重新供给它们。批次 #10 的 p7 [3b] 只断言了副本**收到**新主的
     * 记录，没断言**放得了**，所以这条缺陷当时没被测出来。
     */
    PartDistEmitFilesetHandover(shard_oid);
}

/*
 * route_status(shard oid) → text
 *
 * 观测面：这个分片在**本节点**上现在是什么角色、写入会不会被捕获、分配器水位
 * 到哪了。此前这三件事分散在 replay_status() / 文件系统 / shard_xid_next()，
 * 想回答"新主到底接管好了没有"要拼三处 —— 交接出问题时最需要的恰恰是这一句。
 */
Datum
partdist_route_status(PG_FUNCTION_ARGS)
{
    Oid             shard = PG_GETARG_OID(0);
    StringInfoData  s;
    ShardFileSet    fs;
    bool            captured = false;
    int             nrel;

    initStringInfo(&s);

    nrel = BuildShardFileSet(shard, &fs);
    if (nrel > 0)
        captured = PartWALSyncIsRegistered(fs.rels[0].loc.relNumber);

    appendStringInfo(&s, "role=%s captured=%s members=%d xid_watermark=%u",
                     ShardPromotedMarkRead(shard) ? "promoted" : "replica_or_plain",
                     captured ? "yes" : "no",
                     nrel > 0 ? nrel : 0,
                     (unsigned) ShardXidAllocWatermark(shard));

    PG_RETURN_TEXT_P(cstring_to_text(s.data));
}
