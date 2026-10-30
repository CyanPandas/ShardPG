/* Persistent 2PC membership. Never introduces a PREPARED status into clog. */
#include "postgres.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include "shard_twophase.h"
#include "shard_clog.h"
#include "shard_xid.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "port/pg_crc32c.h"

typedef struct ShardPreparedEntry
{
    uint64 gxid;
    uint32 magic;
    pg_crc32c crc;
} ShardPreparedEntry;
#define SHARD_PREPARED_MAGIC UINT32_C(0x53503231)

static void
prepared_path(char *dir, char *path, Oid shard, TransactionId sxid)
{
    snprintf(dir, MAXPGPATH, "%s/pg_shard_twophase/%u", DataDir, shard);
    snprintf(path, MAXPGPATH, "%s/%08X", dir, sxid);
}

void
ShardTwoPhaseRemember(Oid shard, TransactionId sxid, uint64 gxid)
{
    char root[MAXPGPATH], dir[MAXPGPATH], path[MAXPGPATH];
    ShardPreparedEntry e;
    ssize_t n;
    int fd;
    snprintf(root, sizeof(root), "%s/pg_shard_twophase", DataDir);
    prepared_path(dir, path, shard, sxid);
    if (MakePGDirectory(root) < 0 && errno != EEXIST)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot create shard 2PC root: %m")));
    if (MakePGDirectory(dir) < 0 && errno != EEXIST)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot create shard 2PC directory: %m")));
    fsync_fname(DataDir, true);
    fsync_fname(root, true);
    memset(&e, 0, sizeof(e));
    e.gxid = gxid;
    e.magic = SHARD_PREPARED_MAGIC;
    INIT_CRC32C(e.crc);
    COMP_CRC32C(e.crc, &e, offsetof(ShardPreparedEntry, crc));
    FIN_CRC32C(e.crc);
    fd = OpenTransientFile(path, O_CREAT | O_WRONLY | PG_BINARY);
    if (fd < 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot open shard 2PC record: %m")));
    do { n = pg_pwrite(fd, &e, sizeof(e), 0); } while (n < 0 && errno == EINTR);
    if (n != sizeof(e) || pg_fsync(fd) != 0)
    {
        CloseTransientFile(fd);
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot persist shard 2PC record: %m")));
    }
    CloseTransientFile(fd);
    fsync_fname(dir, true);
}

bool
ShardTwoPhaseIsPrepared(Oid shard, TransactionId sxid)
{
    char dir[MAXPGPATH], path[MAXPGPATH];
    ShardPreparedEntry e;
    ShardClogSlot slot;
    pg_crc32c crc;
    ssize_t n;
    int fd;
    if (!ShardClogReadSlot(shard, sxid, &slot) || slot.status != TXN_RUNNING)
        return false;
    prepared_path(dir, path, shard, sxid);
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
    {
        if (errno == ENOENT) return false;
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot read shard 2PC record: %m")));
    }
    do { n = pg_pread(fd, &e, sizeof(e), 0); } while (n < 0 && errno == EINTR);
    CloseTransientFile(fd);
    if (n != sizeof(e))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("short shard 2PC record")));
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, &e, offsetof(ShardPreparedEntry, crc));
    FIN_CRC32C(crc);
    if (e.magic != SHARD_PREPARED_MAGIC || !EQ_CRC32C(crc, e.crc))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("corrupt shard 2PC record")));
    return e.gxid == slot.global_xid;
}

void
ShardTwoPhaseForget(Oid shard, TransactionId sxid)
{
    char dir[MAXPGPATH], path[MAXPGPATH];
    prepared_path(dir, path, shard, sxid);
    if (unlink(path) == 0) fsync_fname(dir, true);
    else if (errno != ENOENT)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot remove shard 2PC record: %m")));
}

void
ShardTwoPhaseClearRange(Oid shard, TransactionId from, TransactionId to, bool all)
{
    char dir[MAXPGPATH], path[MAXPGPATH];
    DIR *d;
    struct dirent *de;
    bool changed = false;
    prepared_path(dir, path, shard, 0);
    d = AllocateDir(dir);
    if (!d)
    {
        if (errno == ENOENT) return;
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot scan shard 2PC directory: %m")));
    }
    while ((de = ReadDir(d, dir)) != NULL)
    {
        unsigned int sxid;
        char extra;
        if (strlen(de->d_name) != 8 || sscanf(de->d_name, "%8x%c", &sxid, &extra) != 1)
            continue;
        if (!all && !ShardXidInRange(sxid, from, to)) continue;
        snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
        if (unlink(path) != 0 && errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot clear shard 2PC record: %m")));
        changed = true;
    }
    FreeDir(d);
    if (changed) fsync_fname(dir, true);
}

PG_FUNCTION_INFO_V1(partdist_shard_is_prepared);
Datum
partdist_shard_is_prepared(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(ShardTwoPhaseIsPrepared(PG_GETARG_OID(0),
                                           ShardXidFromSql(PG_GETARG_INT64(1))));
}
