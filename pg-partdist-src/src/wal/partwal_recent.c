/*
 * partwal_recent.c — 最近写下的分区流记录的节点级内存缓存（2026-10-16）。
 *
 * 动机与语义见 include/partwal_recent.h。实现：
 *   · 一块共享内存字节环（pg_partdist.partwal_recent_cache_mb），按 32 字节对齐逐条
 *     追加 [项头 | PartWALRecord | 载荷]；写到环尾放不下时补一个填充项、绕回环首；
 *   · 一张共享哈希 (分区, plsn) → 环上位置 + 代号；环满（或哈希满）时从最老的一项起
 *     逐项淘汰，同时删掉仍指向它的哈希项；
 *   · 一把 LWLock：登记独占、查询共享。查询把载荷拷进调用方的内存上下文；
 *   · 代号：任何分区截断（编号回退）/ 整流重置时整体 +1，旧代号的项一律当作不存在。
 *
 * 只登记写入器刷缓冲时真正 write() 进段文件的记录（超过写入器缓冲的大记录走直写，
 * 不进缓存），所以命中的内容与文件逐字节相同；查不中由调用方退回文件读。
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

#include "partwal_recent.h"

int         partwal_recent_cache_mb = 256;

#define RECENT_ALIGN        32
#define RECENT_AVG_ITEM     160     /* 估哈希容量：平均每项至少这么多字节 */

typedef struct RecentKey
{
    Oid         partition_id;
    uint32      pad;
    uint64      plsn;
} RecentKey;

typedef struct RecentEnt
{
    RecentKey   key;
    uint64      pos;            /* 环上单调位置（取模得偏移） */
    uint64      epoch;
} RecentEnt;

typedef struct RecentItem
{
    RecentKey   key;            /* partition_id = InvalidOid：环尾填充项 */
    uint32      len;            /* 本项总长（含项头），RECENT_ALIGN 的倍数 */
    uint32      rec_len;        /* PartWALRecord + 载荷 */
} RecentItem;

typedef struct RecentCtl
{
    LWLock     *lock;
    uint64      size;           /* 环字节数 */
    uint64      head;           /* 下一项的单调位置 */
    uint64      tail;           /* 最老一项的单调位置 */
    uint64      epoch;
    int64       nent;
    int64       maxent;
} RecentCtl;

static RecentCtl *RCtl = NULL;
static char *RRing = NULL;
static HTAB *RHash = NULL;

static uint64
recent_size(void)
{
    return (uint64) partwal_recent_cache_mb * 1024 * 1024;
}

static long
recent_maxent(void)
{
    return (long) (recent_size() / RECENT_AVG_ITEM);
}

void
RequestPartWALRecentShmem(void)
{
    if (partwal_recent_cache_mb <= 0)
        return;
    RequestAddinShmemSpace(MAXALIGN(sizeof(RecentCtl)));
    RequestAddinShmemSpace(recent_size());
    RequestAddinShmemSpace(hash_estimate_size(recent_maxent(), sizeof(RecentEnt)));
    RequestNamedLWLockTranche("pg_partdist_partwal_recent", 1);
}

void
PartWALRecentShmemInit(void)
{
    bool        found;
    bool        found_ring;
    HASHCTL     info;

    if (partwal_recent_cache_mb <= 0)
        return;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    RCtl = ShmemInitStruct("pg_partdist_partwal_recent", sizeof(RecentCtl), &found);
    RRing = ShmemInitStruct("pg_partdist_partwal_recent_ring", recent_size(), &found_ring);
    if (!found)
    {
        memset(RCtl, 0, sizeof(RecentCtl));
        RCtl->lock = &(GetNamedLWLockTranche("pg_partdist_partwal_recent"))->lock;
        RCtl->size = recent_size();
        RCtl->maxent = recent_maxent();
        RCtl->epoch = 1;
    }
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(RecentKey);
    info.entrysize = sizeof(RecentEnt);
    RHash = ShmemInitHash("pg_partdist_partwal_recent_hash",
                          recent_maxent(), recent_maxent(),
                          &info, HASH_ELEM | HASH_BLOBS);
    LWLockRelease(AddinShmemInitLock);
}

/* 淘汰最老的一项（调用方持独占锁，且 tail < head） */
static void
recent_evict_one(void)
{
    RecentItem *it = (RecentItem *) (RRing + RCtl->tail % RCtl->size);

    if (it->key.partition_id != InvalidOid)
    {
        RecentEnt  *e = (RecentEnt *) hash_search(RHash, &it->key, HASH_FIND, NULL);

        if (e != NULL && e->pos == RCtl->tail)
        {
            (void) hash_search(RHash, &it->key, HASH_REMOVE, NULL);
            RCtl->nent--;
        }
    }
    RCtl->tail += it->len;
}

/* 登记一条记录（调用方持独占锁） */
static void
recent_put_locked(Oid partition_id, const PartWALRecord *rec, const char *data)
{
    uint32      rec_len = (uint32) sizeof(PartWALRecord) + rec->data_len;
    uint32      need = (uint32) TYPEALIGN(RECENT_ALIGN, sizeof(RecentItem) + rec_len);
    uint64      off;
    RecentItem *it;
    RecentEnt  *e;
    bool        found;

    if (need > RCtl->size / 16)
        return;                 /* 太大：不缓存，读的时候走文件 */

    off = RCtl->head % RCtl->size;
    if (off + need > RCtl->size)
    {
        uint32      fill = (uint32) (RCtl->size - off);

        while (RCtl->tail < RCtl->head && RCtl->head + fill - RCtl->tail > RCtl->size)
            recent_evict_one();
        it = (RecentItem *) (RRing + off);
        memset(&it->key, 0, sizeof(RecentKey));
        it->len = fill;
        it->rec_len = 0;
        RCtl->head += fill;
        off = 0;
    }
    while (RCtl->tail < RCtl->head &&
           (RCtl->head + need - RCtl->tail > RCtl->size || RCtl->nent >= RCtl->maxent))
        recent_evict_one();
    if (RCtl->nent >= RCtl->maxent)
        return;                 /* 不该发生：环空了哈希还是满的 */

    it = (RecentItem *) (RRing + off);
    memset(&it->key, 0, sizeof(RecentKey));
    it->key.partition_id = partition_id;
    it->key.plsn = rec->partition_lsn;
    it->len = need;
    it->rec_len = rec_len;
    memcpy((char *) it + sizeof(RecentItem), rec, sizeof(PartWALRecord));
    if (rec->data_len > 0)
        memcpy((char *) it + sizeof(RecentItem) + sizeof(PartWALRecord), data, rec->data_len);

    e = (RecentEnt *) hash_search(RHash, &it->key, HASH_ENTER, &found);
    if (!found)
        RCtl->nent++;
    e->pos = RCtl->head;
    e->epoch = RCtl->epoch;
    RCtl->head += need;
}

void
PartWALRecentInsertBuffer(Oid partition_id, const char *buf, int len)
{
    int         pos = 0;

    if (RCtl == NULL || len < (int) sizeof(PartWALRecord))
        return;

    LWLockAcquire(RCtl->lock, LW_EXCLUSIVE);
    while (pos + (int) sizeof(PartWALRecord) <= len)
    {
        PartWALRecord rec;

        memcpy(&rec, buf + pos, sizeof(PartWALRecord));
        if (rec.magic != PARTWAL_MAGIC ||
            (int64) pos + (int64) sizeof(PartWALRecord) + (int64) rec.data_len > (int64) len)
            break;
        if (rec.partition_id == partition_id && rec.partition_lsn > 0)
            recent_put_locked(partition_id, &rec, buf + pos + sizeof(PartWALRecord));
        pos += (int) sizeof(PartWALRecord) + (int) rec.data_len;
    }
    LWLockRelease(RCtl->lock);
}

bool
PartWALRecentLookup(Oid partition_id, uint64 plsn,
                    PartWALRecord *out_rec, char **out_data)
{
    RecentKey   key;
    RecentEnt  *e;
    bool        hit = false;

    if (RCtl == NULL)
        return false;

    memset(&key, 0, sizeof(key));
    key.partition_id = partition_id;
    key.plsn = plsn;

    LWLockAcquire(RCtl->lock, LW_SHARED);
    e = (RecentEnt *) hash_search(RHash, &key, HASH_FIND, NULL);
    if (e != NULL && e->epoch == RCtl->epoch &&
        e->pos >= RCtl->tail && e->pos < RCtl->head)
    {
        RecentItem *it = (RecentItem *) (RRing + e->pos % RCtl->size);

        if (it->key.partition_id == partition_id && it->key.plsn == plsn)
        {
            memcpy(out_rec, (char *) it + sizeof(RecentItem), sizeof(PartWALRecord));
            *out_data = NULL;
            if (out_rec->data_len > 0)
            {
                *out_data = (char *) palloc(out_rec->data_len);
                memcpy(*out_data, (char *) it + sizeof(RecentItem) + sizeof(PartWALRecord),
                       out_rec->data_len);
            }
            hit = true;
        }
    }
    LWLockRelease(RCtl->lock);
    return hit;
}

void
PartWALRecentInvalidateAll(void)
{
    if (RCtl == NULL)
        return;
    LWLockAcquire(RCtl->lock, LW_EXCLUSIVE);
    RCtl->epoch++;
    LWLockRelease(RCtl->lock);
}
