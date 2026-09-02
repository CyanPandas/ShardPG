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
#include <unistd.h>

#include "shard_fileset.h"
#include "partition_wal.h"
#include "partwal_sync.h"

#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlogrecord.h"
#include "catalog/pg_class.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "storage/fd.h"
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

    /* CTRL 已在流里，现在灌内容 —— 顺序不能反（follower 要先换完表） */
    if ((flags & PARTWAL_FSUPD_NEEDS_REBASELINE) == 0)
        for (i = 0; i < nchanged; i++)
            (void) FileSetLogRelationPages(new_relids[changed[i]], true);

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

    if (total_blocks > (uint64) fileset_inline_max_blocks)
        ereport(ERROR,
                (errmsg("pg_partdist: shard %u 的物理基线涉及 %llu 个块，"
                        "超过 pg_partdist.fileset_inline_max_blocks = %d",
                        shard_oid, (unsigned long long) total_blocks,
                        fileset_inline_max_blocks),
                 errdetail("基线是**显式**操作，这里不做静默降级 —— 一次灌太多块"
                           "会把 Raft 日志环顶爆（§13 约束 13），后果是永久分叉。"),
                 errhint("确认该 shard 的体量后调高 "
                         "pg_partdist.fileset_inline_max_blocks 再重试。")));

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

    /* CTRL 已在流里，现在灌全部内容 —— 顺序不能反 */
    for (i = 0; i < nrels; i++)
        (void) FileSetLogRelationPages(relids[i], true);

    ereport(LOG,
            (errmsg("pg_partdist: shard %u 物理基线已发射（%d 个成员，%llu 块，"
                    "base_part_lsn=%llu）",
                    shard_oid, nrels, (unsigned long long) total_blocks,
                    (unsigned long long) base_plsn)));

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
         * 表被 DROP 掉了。副本侧的处置（停流 / 删副本）需要另一个 opcode，
         * 属后续工作；这里保持沉默而不是发一条半吊子的 FILESET_UPDATE。
         */
        if (BuildShardFileSetEx(parts[p], &new_fs, new_relids) < 1)
            continue;

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
