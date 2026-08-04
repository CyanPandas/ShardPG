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
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/smgr.h"
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

    if (!IsTransactionState())
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
