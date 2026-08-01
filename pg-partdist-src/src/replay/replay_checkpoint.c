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

static uint32
ApplyCheckpointCrc(const ShardApplyCheckpoint *chk)
{
    ShardApplyCheckpoint copy = *chk;
    pg_crc32c            crc;

    copy.crc = 0;
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (const char *) &copy, sizeof(copy));
    FIN_CRC32C(crc);
    return (uint32) crc;
}

bool
ReadApplyCheckpoint(Oid shard_oid, ShardApplyCheckpoint *out)
{
    char    path[MAXPGPATH];
    int     fd;
    ssize_t nb;

    ApplyCheckpointPath(shard_oid, path, NULL);

    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        return false;

    nb = read(fd, out, sizeof(*out));
    CloseTransientFile(fd);

    if (nb != (ssize_t) sizeof(*out))
        return false;
    if (out->magic != APPLY_CHECKPOINT_MAGIC ||
        out->version != APPLY_CHECKPOINT_VERSION ||
        out->shard_oid != shard_oid)
        return false;
    if (out->crc != ApplyCheckpointCrc(out))
    {
        ereport(WARNING,
                (errmsg("shard replay: shard %u apply_checkpoint CRC 不符，"
                        "按全新副本处理（游标从 0 起）", shard_oid)));
        return false;
    }

    return true;
}

void
WriteApplyCheckpoint(const ShardApplyCheckpoint *chk)
{
    char                 path[MAXPGPATH];
    char                 tmp[MAXPGPATH];
    char                 dirpath[MAXPGPATH];
    int                  fd;
    ssize_t              nb;
    ShardApplyCheckpoint copy = *chk;

    copy.crc = ApplyCheckpointCrc(chk);

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
