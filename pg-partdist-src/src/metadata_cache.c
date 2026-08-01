#include "pg_partdist.h"
#include "metadata_cache.h"
#include "partition_wal.h"
#include "demux_worker.h"
#include "shard_replay.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "executor/spi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "access/xact.h"

/* ---- module-level shared-memory pointers ---- */
PartdistSharedState *PartdistState    = NULL;
HTAB                *PartitionMapHash = NULL;
HTAB                *NodeMapHash      = NULL;

/* ---- internal helpers ---- */

Size
pg_partdist_shmem_size(void)
{
    Size size = 0;

    size = add_size(size, sizeof(PartdistSharedState));
    size = add_size(size, hash_estimate_size(PARTDIST_PARTITION_MAX_ENTRIES,
                                              sizeof(PartitionHashEntry)));
    size = add_size(size, hash_estimate_size(PARTDIST_NODE_MAX_ENTRIES,
                                              sizeof(NodeHashEntry)));
    return size;
}

void
pg_partdist_shmem_request_hook(void)
{
    RequestAddinShmemSpace(pg_partdist_shmem_size());
    RequestNamedLWLockTranche("pg_partdist", 2);

    /* Partition WAL LSN counter shmem */
    RequestPartitionWALShmem();

    /* Demux worker shared state */
    RequestDemuxShmem();

    /* Replay 认领槽位 + 副本豁免哈希（FRD §7/§13.10/补丁 0002） */
    RequestReplayShmem();
}

void
pg_partdist_shmem_startup_hook(void)
{
    bool        found;
    HASHCTL     ctl;
    LWLockPadded *locks;

    /* Reset in case of postmaster restart */
    PartdistState    = NULL;
    PartitionMapHash = NULL;
    NodeMapHash      = NULL;

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    /* ---- shared state struct ---- */
    PartdistState = ShmemInitStruct("pg_partdist_state",
                                    sizeof(PartdistSharedState),
                                    &found);
    if (!found)
    {
        locks = GetNamedLWLockTranche("pg_partdist");
        PartdistState->partition_lock     = &locks[0].lock;
        PartdistState->node_lock          = &locks[1].lock;
        PartdistState->metadata_generation = 0;
        PartdistState->partition_hits     = 0;
        PartdistState->partition_misses   = 0;
        PartdistState->node_hits          = 0;
        PartdistState->node_misses        = 0;
    }

    /* ---- partition_map hash ---- */
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(Oid);
    ctl.entrysize = sizeof(PartitionHashEntry);
    PartitionMapHash = ShmemInitHash("pg_partdist_partition_hash",
                                     PARTDIST_PARTITION_MAX_ENTRIES,
                                     PARTDIST_PARTITION_MAX_ENTRIES,
                                     &ctl,
                                     HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);

    /* ---- node_map hash ---- */
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(int32);
    ctl.entrysize = sizeof(NodeHashEntry);
    NodeMapHash = ShmemInitHash("pg_partdist_node_hash",
                                PARTDIST_NODE_MAX_ENTRIES,
                                PARTDIST_NODE_MAX_ENTRIES,
                                &ctl,
                                HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);

    LWLockRelease(AddinShmemInitLock);

    /* Initialise partition WAL LSN counter shmem (outside AddinShmemInitLock) */
    PartitionWALShmemInit();

    /* Initialise Demux worker shared state */
    DemuxShmemInit();

    /* Initialise replay control (claim slots + flush-exempt data) */
    ReplayShmemInit();
}

/* ---- SPI helpers ---- */

/*
 * Load one partition row from the system table into *out.
 * Returns true on success, false if not found.
 * Must be called inside an SPI context.
 */
static bool
LoadPartitionFromTable(Oid partition_id, PartitionMapEntry *out)
{
    int         ret;
    bool        isnull;
    Datum       d;
    char        sql[256];
    HeapTuple   tup;
    TupleDesc   tdesc;
    ArrayType  *arr;
    int         nelems;
    Datum      *elems;
    bool       *nulls;
    int         i;

    snprintf(sql, sizeof(sql),
             "SELECT primary_node, secondary_nodes, version"
             " FROM partdist.partition_map"
             " WHERE partition_id = %u",
             partition_id);

    ret = SPI_execute(sql, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
        return false;

    tup   = SPI_tuptable->vals[0];
    tdesc = SPI_tuptable->tupdesc;

    /* primary_node */
    d = SPI_getbinval(tup, tdesc, 1, &isnull);
    if (isnull)
        return false;
    out->primary_node = DatumGetInt32(d);

    /* secondary_nodes INTEGER[] */
    d = SPI_getbinval(tup, tdesc, 2, &isnull);
    out->num_secondaries = 0;
    if (!isnull)
    {
        arr = DatumGetArrayTypeP(d);
        deconstruct_array(arr,
                          INT4OID, sizeof(int32), true, TYPALIGN_INT,
                          &elems, &nulls, &nelems);
        for (i = 0; i < nelems && i < PARTDIST_MAX_SECONDARIES; i++)
        {
            if (!nulls[i])
                out->secondary_nodes[i] = DatumGetInt32(elems[i]);
        }
        out->num_secondaries = Min(nelems, PARTDIST_MAX_SECONDARIES);
        pfree(elems);
        pfree(nulls);
    }

    /* version */
    d = SPI_getbinval(tup, tdesc, 3, &isnull);
    out->version = isnull ? 1 : DatumGetInt64(d);

    out->partition_id = partition_id;
    return true;
}

/*
 * Load one node row from the system table into *out.
 * Returns true on success, false if not found.
 */
static bool
LoadNodeFromTable(int32 node_id, NodeMapEntry *out)
{
    int         ret;
    bool        isnull;
    Datum       d;
    char        sql[256];
    HeapTuple   tup;
    TupleDesc   tdesc;
    char       *hostname;
    char       *status_str;

    snprintf(sql, sizeof(sql),
             "SELECT hostname, port, COALESCE(status, 'active')"
             " FROM partdist.node_map"
             " WHERE node_id = %d",
             node_id);

    ret = SPI_execute(sql, true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
        return false;

    tup   = SPI_tuptable->vals[0];
    tdesc = SPI_tuptable->tupdesc;

    /* hostname */
    d = SPI_getbinval(tup, tdesc, 1, &isnull);
    if (isnull)
        return false;
    hostname = TextDatumGetCString(d);
    strlcpy(out->hostname, hostname, PARTDIST_MAX_HOSTNAME);

    /* port */
    d = SPI_getbinval(tup, tdesc, 2, &isnull);
    out->port = isnull ? 0 : DatumGetInt32(d);

    /* status */
    d = SPI_getbinval(tup, tdesc, 3, &isnull);
    out->status = NODE_ACTIVE;
    if (!isnull)
    {
        status_str = TextDatumGetCString(d);
        if (strcmp(status_str, "down") == 0)
            out->status = NODE_DOWN;
        else if (strcmp(status_str, "syncing") == 0)
            out->status = NODE_SYNCING;
    }

    out->node_id = node_id;
    return true;
}

/* ---- public cache API ---- */

/*
 * GetPartitionPrimary — return the primary node ID for partition_id.
 * Returns -1 if the partition is not found.
 *
 * When the shared-memory cache is available:
 *   1. Check the hash under a shared lock.
 *   2. On hit with a valid generation, return immediately.
 *   3. On miss or stale entry, fall through to an SPI fetch, then
 *      repopulate the hash under an exclusive lock.
 *
 * Without shared memory (extension not in shared_preload_libraries):
 *   Always falls back to a direct SPI query.
 */
int32
GetPartitionPrimary(Oid partition_id)
{
    PartitionHashEntry *hentry;
    PartitionMapEntry   loaded;
    int64               current_gen;
    int32               result;
    bool                found;
    bool                need_spi;

    need_spi = false;
    result   = -1;

    /* ---- fast path: shared-memory cache ---- */
    if (PartitionMapHash != NULL && PartdistState != NULL)
    {
        LWLockAcquire(PartdistState->partition_lock, LW_SHARED);
        current_gen = PartdistState->metadata_generation;
        hentry = (PartitionHashEntry *)
            hash_search(PartitionMapHash, &partition_id, HASH_FIND, &found);

        if (found && hentry->is_valid &&
            hentry->meta_version == current_gen)
        {
            result = hentry->primary_node;
            PartdistState->partition_hits++;
            LWLockRelease(PartdistState->partition_lock);
            return result;
        }
        PartdistState->partition_misses++;
        LWLockRelease(PartdistState->partition_lock);

        need_spi = true;
    }
    else
    {
        need_spi = true;
    }

    if (!need_spi)
        return result;

    /* ---- SPI fallback ---- */
    {
        MemoryContext oldctx = CurrentMemoryContext;
        bool connected = false;

        PG_TRY();
        {
            if (SPI_connect() != SPI_OK_CONNECT)
                ereport(ERROR,
                        (errmsg("pg_partdist: SPI_connect failed")));
            connected = true;

            if (LoadPartitionFromTable(partition_id, &loaded))
                result = loaded.primary_node;

            SPI_finish();
            connected = false;
        }
        PG_CATCH();
        {
            if (connected)
                SPI_finish();
            MemoryContextSwitchTo(oldctx);
            PG_RE_THROW();
        }
        PG_END_TRY();
    }

    /* ---- repopulate cache ---- */
    if (PartitionMapHash != NULL && PartdistState != NULL && result >= 0)
    {
        LWLockAcquire(PartdistState->partition_lock, LW_EXCLUSIVE);
        current_gen = PartdistState->metadata_generation;
        hentry = (PartitionHashEntry *)
            hash_search(PartitionMapHash, &partition_id, HASH_ENTER, &found);
        hentry->primary_node   = loaded.primary_node;
        hentry->num_secondaries = loaded.num_secondaries;
        memcpy(hentry->secondary_nodes, loaded.secondary_nodes,
               sizeof(int32) * loaded.num_secondaries);
        hentry->meta_version = current_gen;
        hentry->is_valid     = true;
        LWLockRelease(PartdistState->partition_lock);
    }

    return result;
}

/*
 * GetPartitionEntry — fill *out with partition data.
 * Returns false if not found.
 */
bool
GetPartitionEntry(Oid partition_id, PartitionMapEntry *out)
{
    int32 primary = GetPartitionPrimary(partition_id);
    if (primary < 0)
        return false;

    /*
     * Re-read from cache to get secondary_nodes too.
     * Under a shared lock, this is a second lookup but avoids duplicating
     * the SPI logic.
     */
    if (PartitionMapHash != NULL && PartdistState != NULL)
    {
        bool found;
        PartitionHashEntry *hentry;

        LWLockAcquire(PartdistState->partition_lock, LW_SHARED);
        hentry = (PartitionHashEntry *)
            hash_search(PartitionMapHash, &partition_id, HASH_FIND, &found);
        if (found && hentry->is_valid)
        {
            out->partition_id    = partition_id;
            out->primary_node    = hentry->primary_node;
            out->num_secondaries = hentry->num_secondaries;
            memcpy(out->secondary_nodes, hentry->secondary_nodes,
                   sizeof(int32) * hentry->num_secondaries);
            out->version = hentry->meta_version;
        }
        LWLockRelease(PartdistState->partition_lock);
        return found;
    }

    /* No cache: use the primary we already fetched */
    out->partition_id    = partition_id;
    out->primary_node    = primary;
    out->num_secondaries = 0;
    out->version         = 0;
    return true;
}

/*
 * GetNodeInfo — return a palloc'd NodeMapEntry for node_id.
 * Returns NULL if the node is not found.
 * The caller is responsible for pfree'ing the result.
 */
NodeMapEntry *
GetNodeInfo(int32 node_id)
{
    NodeHashEntry *hentry;
    NodeMapEntry   loaded;
    NodeMapEntry  *result;
    int64          current_gen;
    bool           found;

    /* ---- fast path: cache ---- */
    if (NodeMapHash != NULL && PartdistState != NULL)
    {
        LWLockAcquire(PartdistState->node_lock, LW_SHARED);
        current_gen = PartdistState->metadata_generation;
        hentry = (NodeHashEntry *)
            hash_search(NodeMapHash, &node_id, HASH_FIND, &found);

        if (found && hentry->is_valid &&
            hentry->meta_version == current_gen)
        {
            result = (NodeMapEntry *) palloc(sizeof(NodeMapEntry));
            result->node_id = hentry->node_id;
            strlcpy(result->hostname, hentry->hostname, PARTDIST_MAX_HOSTNAME);
            result->port   = hentry->port;
            result->status = hentry->status;
            PartdistState->node_hits++;
            LWLockRelease(PartdistState->node_lock);
            return result;
        }
        PartdistState->node_misses++;
        LWLockRelease(PartdistState->node_lock);
    }

    /* ---- SPI fallback ---- */
    {
        bool connected = false;
        bool ok        = false;

        PG_TRY();
        {
            if (SPI_connect() != SPI_OK_CONNECT)
                ereport(ERROR,
                        (errmsg("pg_partdist: SPI_connect failed")));
            connected = true;

            ok = LoadNodeFromTable(node_id, &loaded);
            SPI_finish();
            connected = false;
        }
        PG_CATCH();
        {
            if (connected)
                SPI_finish();
            PG_RE_THROW();
        }
        PG_END_TRY();

        if (!ok)
            return NULL;
    }

    /* ---- repopulate cache ---- */
    if (NodeMapHash != NULL && PartdistState != NULL)
    {
        LWLockAcquire(PartdistState->node_lock, LW_EXCLUSIVE);
        current_gen = PartdistState->metadata_generation;
        hentry = (NodeHashEntry *)
            hash_search(NodeMapHash, &node_id, HASH_ENTER, &found);
        hentry->node_id = loaded.node_id;
        strlcpy(hentry->hostname, loaded.hostname, PARTDIST_MAX_HOSTNAME);
        hentry->port        = loaded.port;
        hentry->status      = loaded.status;
        hentry->meta_version = current_gen;
        hentry->is_valid    = true;
        LWLockRelease(PartdistState->node_lock);
    }

    result = (NodeMapEntry *) palloc(sizeof(NodeMapEntry));
    *result = loaded;
    return result;
}

/*
 * InvalidatePartdistCache — mark all cache entries stale by bumping
 * the metadata generation.  On the next lookup every entry will be
 * re-fetched from the system tables.
 */
void
InvalidatePartdistCache(void)
{
    if (PartdistState == NULL)
        return;

    LWLockAcquire(PartdistState->partition_lock, LW_EXCLUSIVE);
    PartdistState->metadata_generation++;
    LWLockRelease(PartdistState->partition_lock);
}
