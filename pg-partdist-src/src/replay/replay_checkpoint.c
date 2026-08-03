/*
 * replay_checkpoint.c
 *
 * apply checkpoint 单文件原子读写（FRD §8.4）。
 * pg_parwal/<shard>/apply_checkpoint，与 leader 侧 demux checkpoint 分开。
 * tmp + fsync + rename + 目录 fsync；头部 CRC32C（crc 字段置 0 计算）。
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "shard_replay.h"
#include "partition_wal.h"

#include "miscadmin.h"
#include "storage/fd.h"

static void
ApplyCheckpointPath(Oid shard_oid, char *path, char *tmp)
{
    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, shard_oid,
             APPLY_CHECKPOINT_FILENAME);
    if (tmp != NULL)
        snprintf(tmp, MAXPGPATH, "%s.tmp", path);
}

/*
 * CRC 覆盖「头（crc 字段置 0）+ 全部 xid_map 条目」。
 *
 * nxidmap == 0 时结果与旧的"只算头"逐字节相同 —— R1 时代写下的 checkpoint
 * 因此继续通过校验，升级到 R2 不会把已有 follower 打回从头重放。
 */
static uint32
ApplyCheckpointCrc(const ShardApplyCheckpoint *chk, const XidMapEntry *ents)
{
    ShardApplyCheckpoint copy = *chk;
    pg_crc32c            crc;

    copy.crc = 0;
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (const char *) &copy, sizeof(copy));
    if (chk->nxidmap > 0 && ents != NULL)
        COMP_CRC32C(crc, (const char *) ents,
                    (size_t) chk->nxidmap * sizeof(XidMapEntry));
    FIN_CRC32C(crc);
    return (uint32) crc;
}

bool
ReadApplyCheckpoint(Oid shard_oid, ShardApplyCheckpoint *out,
                    XidMapEntry **entries)
{
    char         path[MAXPGPATH];
    int          fd;
    ssize_t      nb;
    XidMapEntry *ents = NULL;
    size_t       entbytes;

    if (entries != NULL)
        *entries = NULL;

    ApplyCheckpointPath(shard_oid, path, NULL);

    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        return false;

    nb = read(fd, out, sizeof(*out));
    if (nb != (ssize_t) sizeof(*out))
    {
        CloseTransientFile(fd);
        return false;
    }
    if (out->magic != APPLY_CHECKPOINT_MAGIC ||
        out->version != APPLY_CHECKPOINT_VERSION ||
        out->shard_oid != shard_oid)
    {
        CloseTransientFile(fd);
        return false;
    }
    if (out->nxidmap > SHARD_XIDMAP_MAX_ENTRIES)
    {
        CloseTransientFile(fd);
        ereport(WARNING,
                (errmsg("shard replay: shard %u apply_checkpoint 的 nxidmap=%u "
                        "越界，按全新副本处理", shard_oid, out->nxidmap)));
        return false;
    }

    if (out->nxidmap > 0)
    {
        entbytes = (size_t) out->nxidmap * sizeof(XidMapEntry);
        ents = (XidMapEntry *) palloc(entbytes);
        nb = read(fd, ents, entbytes);
        if (nb != (ssize_t) entbytes)
        {
            CloseTransientFile(fd);
            pfree(ents);
            ereport(WARNING,
                    (errmsg("shard replay: shard %u apply_checkpoint 的 xid_map "
                            "快照被截断（%zd/%zu 字节），按全新副本处理",
                            shard_oid, nb, entbytes)));
            return false;
        }
    }
    CloseTransientFile(fd);

    if (out->crc != ApplyCheckpointCrc(out, ents))
    {
        if (ents != NULL)
            pfree(ents);
        ereport(WARNING,
                (errmsg("shard replay: shard %u apply_checkpoint CRC 不符，"
                        "按全新副本处理（游标从 0 起）", shard_oid)));
        return false;
    }

    if (entries != NULL)
        *entries = ents;
    else if (ents != NULL)
        pfree(ents);

    return true;
}

void
WriteApplyCheckpoint(const ShardApplyCheckpoint *chk, const XidMapEntry *entries)
{
    char                 path[MAXPGPATH];
    char                 tmp[MAXPGPATH];
    char                 dirpath[MAXPGPATH];
    int                  fd;
    ssize_t              nb;
    ShardApplyCheckpoint copy = *chk;

    if (chk->nxidmap > 0 && entries == NULL)
        ereport(ERROR,
                (errmsg("shard replay: shard %u 声明了 %u 条 xid_map 快照却没有"
                        "给出内容", chk->shard_oid, chk->nxidmap)));

    copy.crc = ApplyCheckpointCrc(chk, entries);

    ApplyCheckpointPath(chk->shard_oid, path, tmp);

    fd = OpenTransientFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
    if (fd < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("shard replay: 无法创建 apply_checkpoint 临时文件 "
                        "\"%s\": %m", tmp)));

    do {
        nb = write(fd, &copy, sizeof(copy));
    } while (nb < 0 && errno == EINTR);

    if (nb != (ssize_t) sizeof(copy))
    {
        CloseTransientFile(fd);
        (void) unlink(tmp);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("shard replay: apply_checkpoint 短写 \"%s\"", tmp)));
    }

    /* xid_map 快照紧随头部；与头同在一个 tmp 文件里，rename 才是原子的 */
    if (chk->nxidmap > 0)
    {
        size_t      remaining = (size_t) chk->nxidmap * sizeof(XidMapEntry);
        const char *ptr = (const char *) entries;

        while (remaining > 0)
        {
            do {
                nb = write(fd, ptr, remaining);
            } while (nb < 0 && errno == EINTR);

            if (nb <= 0)
            {
                CloseTransientFile(fd);
                (void) unlink(tmp);
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("shard replay: apply_checkpoint 的 xid_map "
                                "快照写入失败 \"%s\": %m", tmp)));
            }
            ptr += nb;
            remaining -= (size_t) nb;
        }
    }

    if (pg_fsync(fd) != 0)
    {
        CloseTransientFile(fd);
        (void) unlink(tmp);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("shard replay: apply_checkpoint fsync 失败 \"%s\": %m",
                        tmp)));
    }
    CloseTransientFile(fd);

    if (rename(tmp, path) != 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("shard replay: 无法就位 apply_checkpoint \"%s\": %m",
                        path)));

    /* 目录 fsync：让 rename 本身持久化 */
    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, chk->shard_oid);
    fd = OpenTransientFile(dirpath, O_RDONLY | PG_BINARY);
    if (fd >= 0)
    {
        (void) pg_fsync(fd);
        CloseTransientFile(fd);
    }
}
