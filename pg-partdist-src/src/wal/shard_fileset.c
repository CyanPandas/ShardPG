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
#include "access/xlogrecord.h"
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "storage/fd.h"
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
    Relation    rel;
    List       *indexes;
    ListCell   *lc;
    uint8       ord;
    Oid         toastoid;

    memset(fs, 0, sizeof(*fs));
    fs->magic     = SHARD_FILESET_MAGIC;
    fs->shard_oid = shard_oid;
    fs->nrels     = 0;

    rel = try_relation_open(shard_oid, AccessShareLock);
    if (rel == NULL)
        return -1;

    /* [0] 主堆 */
    FileSetAdd(fs, &rel->rd_locator, SHARD_REL_MAIN, 0);

    /* 普通索引：RelationGetIndexList 返回 OID 升序 = 稳定的"定义序" */
    indexes = RelationGetIndexList(rel);
    ord = 0;
    foreach(lc, indexes)
    {
        Relation idx = relation_open(lfirst_oid(lc), AccessShareLock);

        FileSetAdd(fs, &idx->rd_locator, SHARD_REL_INDEX, ord++);
        relation_close(idx, AccessShareLock);
    }
    list_free(indexes);

    /* TOAST 堆及其索引 */
    toastoid = rel->rd_rel->reltoastrelid;
    if (OidIsValid(toastoid))
    {
        Relation toast = table_open(toastoid, AccessShareLock);

        FileSetAdd(fs, &toast->rd_locator, SHARD_REL_TOAST, 0);

        indexes = RelationGetIndexList(toast);
        ord = 0;
        foreach(lc, indexes)
        {
            Relation tidx = relation_open(lfirst_oid(lc), AccessShareLock);

            FileSetAdd(fs, &tidx->rd_locator, SHARD_REL_TOAST_INDEX, ord++);
            relation_close(tidx, AccessShareLock);
        }
        list_free(indexes);
        table_close(toast, AccessShareLock);
    }

    relation_close(rel, AccessShareLock);
    return fs->nrels;
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
