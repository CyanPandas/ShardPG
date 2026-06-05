/*
 * partition_wal.c
 *
 * Milestone 2.1 — pg_parwal directory management, PartWALHeader record
 * writing, custom WAL RMGR, and ExecutorFinish hook integration.
 *
 * On-disk layout: pg_parwal/<partition_id>/<segname>
 *   Each segment file is a flat sequence of fixed-size PartWALHeader
 *   structs.  The segment naming matches PostgreSQL's native WAL format
 *   so records can be correlated with the global WAL by segment number.
 *
 * WAL integration: every DML on a registered partition emits one
 *   PartWALHeader record via the custom RMGR (RM_EXPERIMENTAL_ID).
 *   During crash recovery the RMGR's redo callback re-creates the
 *   pg_parwal file from the same record.
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "partition_wal.h"
#include "partition_wal_writer.h"
#include "metadata_cache.h"

#include "access/rmgr.h"
#include "catalog/namespace.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_namespace.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/tuplestore.h"

/* ================================================================== */
/* Shared memory                                                        */
/* ================================================================== */

/* PARTWAL_LOCK_TRANCHE is defined in partition_wal.h */
#define PARTWAL_SHMEM_NAME      "pg_partdist_wal_state"
#define PARTWAL_HASH_NAME       "pg_partdist_wal_lsn"

typedef struct PartWALSharedState
{
    LWLock *lsn_lock;   /* protects PartWALLSNHash */
} PartWALSharedState;

typedef struct PartWALLSNEntry
{
    Oid     partition_id;   /* hash key — must be first */
    uint64  next_lsn;       /* next LSN to return (1-based) */
} PartWALLSNEntry;

static PartWALSharedState *partwal_shmem  = NULL;
static HTAB               *PartWALLSNHash = NULL;

void
RequestPartitionWALShmem(void)
{
    RequestAddinShmemSpace(PartitionWALShmemSize());
    RequestNamedLWLockTranche(PARTWAL_LOCK_TRANCHE, 1);
}

Size
PartitionWALShmemSize(void)
{
    Size sz = 0;
    sz = add_size(sz, sizeof(PartWALSharedState));
    sz = add_size(sz, hash_estimate_size(PARTDIST_PARTITION_MAX_ENTRIES,
                                         sizeof(PartWALLSNEntry)));
    return sz;
}

void
PartitionWALShmemInit(void)
{
    bool    found;
    HASHCTL ctl;

    partwal_shmem = (PartWALSharedState *)
        ShmemInitStruct(PARTWAL_SHMEM_NAME,
                        sizeof(PartWALSharedState), &found);
    if (!found)
        partwal_shmem->lsn_lock =
            &(GetNamedLWLockTranche(PARTWAL_LOCK_TRANCHE))[0].lock;

    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(Oid);
    ctl.entrysize = sizeof(PartWALLSNEntry);

    PartWALLSNHash = ShmemInitHash(PARTWAL_HASH_NAME,
                                    PARTDIST_PARTITION_MAX_ENTRIES,
                                    PARTDIST_PARTITION_MAX_ENTRIES,
                                    &ctl,
                                    HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
}

/* ================================================================== */
/* Per-partition LSN allocation                                         */
/* ================================================================== */

uint64
AllocPartitionLSN(Oid partition_id)
{
    PartWALLSNEntry *entry;
    bool             found;
    uint64           result;
    uint64           init_lsn = 1;

    if (partwal_shmem == NULL || PartWALLSNHash == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_partdist: partition WAL shmem not initialised — "
                        "is pg_partdist in shared_preload_libraries?")));

    /*
     * Check under a shared lock whether the entry already exists.  If it
     * doesn't, recover the on-disk high-water mark so that partition_lsn
     * remains strictly increasing across server restarts.  File I/O is done
     * outside the exclusive lock to avoid holding it during disk reads.
     */
    LWLockAcquire(partwal_shmem->lsn_lock, LW_SHARED);
    hash_search(PartWALLSNHash, &partition_id, HASH_FIND, &found);
    LWLockRelease(partwal_shmem->lsn_lock);

    if (!found)
    {
        uint64 last = GetLastWrittenPartitionLSN(partition_id);
        if (last > 0)
            init_lsn = last + 1;
    }

    LWLockAcquire(partwal_shmem->lsn_lock, LW_EXCLUSIVE);
    entry = (PartWALLSNEntry *)
        hash_search(PartWALLSNHash, &partition_id, HASH_ENTER, &found);
    if (!found)
    {
        entry->partition_id = partition_id;
        entry->next_lsn     = init_lsn;
    }
    result = entry->next_lsn++;
    LWLockRelease(partwal_shmem->lsn_lock);
    return result;
}

/* ================================================================== */
/* Directory management (D2.1.1)                                        */
/* ================================================================== */

void
InitPartitionWALDirectory(Oid partition_id)
{
    char path[MAXPGPATH];

    /* Create pg_parwal/ root (ignore EEXIST) */
    snprintf(path, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);
    if (MakePGDirectory(path) < 0 && errno != EEXIST)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not create directory \"%s\": %m", path)));

    /* Create pg_parwal/<partition_id>/ */
    snprintf(path, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);
    if (MakePGDirectory(path) < 0 && errno != EEXIST)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not create partition WAL directory \"%s\": %m",
                        path)));
}

/*
 * GetPartitionWALPath — returns a palloc'd relative path:
 *   pg_parwal/<partition_id>/<segname>
 *
 * The segment is named with timeline=1 using PG's native XLogFileName.
 */
char *
GetPartitionWALPath(Oid partition_id, XLogSegNo segno)
{
    char *result = palloc(MAXPGPATH);
    char  segname[MAXPGPATH];

    XLogFileName(segname, 1 /* timeline */, segno, wal_segment_size);
    snprintf(result, MAXPGPATH, "%s/%u/%s",
             PARTITION_WAL_DIR, partition_id, segname);
    return result;
}

void
CleanupPartitionWAL(Oid partition_id, XLogRecPtr keepPtr)
{
    char          dirpath[MAXPGPATH];
    DIR          *dir;
    struct dirent *de;

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir == NULL)
    {
        if (errno == ENOENT)
            return;     /* nothing to clean */
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open directory \"%s\": %m", dirpath)));
    }

    while ((de = readdir(dir)) != NULL)
    {
        TimeLineID  tli;
        XLogSegNo   segno;
        XLogRecPtr  seg_end_lsn;
        char        filepath[MAXPGPATH];

        if (!IsXLogFileName(de->d_name))
            continue;

        XLogFromFileName(de->d_name, &tli, &segno, wal_segment_size);

        /* Segment spans [segno*segsz, (segno+1)*segsz); end = (segno+1)*segsz */
        XLogSegNoOffsetToRecPtr(segno + 1, 0, wal_segment_size, seg_end_lsn);

        if (seg_end_lsn <= keepPtr)
        {
            snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, de->d_name);
            if (unlink(filepath) < 0)
                ereport(WARNING,
                        (errcode_for_file_access(),
                         errmsg("could not remove partition WAL file \"%s\": %m",
                                filepath)));
        }
    }

    closedir(dir);
}

/* ================================================================== */
/* Writing PartWALHeader records to pg_parwal files                     */
/* ================================================================== */

/*
 * WriteHeaderToFile — append one PartWALHeader to the segment file that
 * corresponds to header->orig_node_lsn.  Creates the file if absent.
 *
 * Uses O_APPEND for safe concurrent appends from multiple backends.
 */
static void
WriteHeaderToFile(const PartWALHeader *header)
{
    XLogSegNo segno;
    char     *relpath;
    char      abspath[MAXPGPATH];
    int       fd;
    ssize_t   written;

    /* Map orig_node_lsn → segment number; fall back to segment 1 */
    if (header->orig_node_lsn == InvalidXLogRecPtr)
        segno = 1;
    else
        XLByteToSeg(header->orig_node_lsn, segno, wal_segment_size);

    if (segno == 0)
        segno = 1;  /* segment 0 is not a valid pg_wal file */

    relpath  = GetPartitionWALPath(header->partition_id, segno);
    snprintf(abspath, MAXPGPATH, "%s/%s", DataDir, relpath);
    pfree(relpath);

    fd = open(abspath, O_WRONLY | O_CREAT | O_APPEND, 0600);
    if (fd < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not open partition WAL file \"%s\": %m",
                        abspath)));

    do {
        written = write(fd, header, sizeof(PartWALHeader));
    } while (written < 0 && errno == EINTR);

    if (written != (ssize_t) sizeof(PartWALHeader))
    {
        close(fd);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("could not write to partition WAL file \"%s\": %m",
                        abspath)));
    }

    if (pg_fsync(fd) != 0)
        ereport(WARNING,
                (errcode_for_file_access(),
                 errmsg("could not fsync partition WAL file \"%s\": %m",
                        abspath)));

    close(fd);
}

/*
 * WritePartitionWALRecord — core function for D2.1.3.
 *
 * 1. Allocates the next per-partition_lsn.
 * 2. Writes a PartWALHeader WAL record via the custom RMGR so that crash
 *    recovery can replay the pg_parwal write.
 * 3. Mirrors the record into pg_parwal/<partition_id>/<segname>.
 *
 * orig_lsn: caller-provided global LSN (e.g. XactLastRecEnd) that
 *           identifies the primary DML record this entry refers to.
 */
XLogRecPtr
WritePartitionWALRecord(Oid partition_id, uint8 flags, XLogRecPtr orig_lsn)
{
    PartWALHeader header;
    XLogRecPtr    custom_lsn;

    if (!XLogInsertAllowed())
        return InvalidXLogRecPtr;

    /* Ensure directory exists before writing */
    InitPartitionWALDirectory(partition_id);

    /* Build the header (orig_node_lsn set before WAL insert) */
    memset(&header, 0, sizeof(header));
    header.magic         = PARTWAL_MAGIC;
    header.partition_id  = partition_id;
    header.orig_node_lsn = orig_lsn;
    header.partition_lsn = AllocPartitionLSN(partition_id);
    header.flags         = flags;

    /*
     * Write to WAL only.  The Demux background worker reads from pg_wal and
     * routes records to the appropriate pg_parwal/N/ segment files.
     *
     * We flush the WAL after inserting so that GetFlushRecPtr() in
     * demux_flush() sees a target that includes this record.  Without the
     * flush, the WAL record stays in the shared WAL buffer, the demux cannot
     * read it, and demux_flush() returns a stale (too-early) target that lets
     * it return before the record reaches pg_parwal/N/.
     *
     * Crash recovery (partdist_wal_redo) still calls WriteHeaderToFile so
     * pg_parwal/N/ is rebuilt correctly after a crash.
     */
    XLogBeginInsert();
    XLogRegisterData((char *) &header, sizeof(PartWALHeader));
    custom_lsn = XLogInsert(RM_EXPERIMENTAL_ID, PARTWAL_RMGR_INFO_DATA);

    /* Flush WAL so the demux worker can read this record from pg_wal/ */
    XLogFlush(custom_lsn);

    return custom_lsn;
}

/* ================================================================== */
/* Custom WAL RMGR                                                      */
/* ================================================================== */

static void
partdist_wal_redo(XLogReaderState *record)
{
    PartWALHeader *header;
    uint32         data_len;

    data_len = XLogRecGetDataLen(record);
    if (data_len < sizeof(PartWALHeader))
    {
        ereport(WARNING,
                (errmsg("pg_partdist: WAL record too short for PartWALHeader "
                        "(%u bytes, need %zu)",
                        data_len, sizeof(PartWALHeader))));
        return;
    }

    header = (PartWALHeader *) XLogRecGetData(record);

    if (header->magic != PARTWAL_MAGIC)
    {
        ereport(WARNING,
                (errmsg("pg_partdist: invalid magic 0x%08X in WAL record",
                        header->magic)));
        return;
    }

    /*
     * Idempotent redo: if the demux already wrote this record to pg_parwal
     * before the crash, skip it to avoid duplicates.  GetLastWrittenPartitionLSN
     * scans the on-disk segment files; if partition_lsn is already present,
     * we can safely skip the write.  If the directory or files don't exist
     * yet, GetLastWrittenPartitionLSN returns 0 and we write normally.
     */
    {
        uint64 last = GetLastWrittenPartitionLSN(header->partition_id);
        if (header->partition_lsn <= last)
            return;
    }

    /* Re-create directory and write record during recovery */
    InitPartitionWALDirectory(header->partition_id);
    WriteHeaderToFile(header);
}

static void
partdist_wal_desc(StringInfo buf, XLogReaderState *record)
{
    PartWALHeader *header  = (PartWALHeader *) XLogRecGetData(record);
    uint8          info    = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

    appendStringInfo(buf,
                     "partition_id=%u partition_lsn=" UINT64_FORMAT
                     " orig_lsn=%X/%08X flags=0x%02X",
                     header->partition_id,
                     header->partition_lsn,
                     LSN_FORMAT_ARGS(header->orig_node_lsn),
                     (unsigned) header->flags);

    if (info == PARTWAL_RMGR_INFO_SKIP)
    {
        PartWALSkip *skip = (PartWALSkip *) XLogRecGetData(record);
        appendStringInfo(buf,
                         " skip_range=[%X/%08X, %X/%08X)",
                         LSN_FORMAT_ARGS(skip->lsn_start),
                         LSN_FORMAT_ARGS(skip->lsn_end));
    }
}

static const char *
partdist_wal_identify(uint8 info)
{
    switch (info & ~XLR_INFO_MASK)
    {
        case PARTWAL_RMGR_INFO_DATA:  return "PARTWAL_DATA";
        case PARTWAL_RMGR_INFO_SKIP:  return "PARTWAL_SKIP";
        default:                       return NULL;
    }
}

static const RmgrData PartdistRmgrData = {
    .rm_name     = "pg_partdist",
    .rm_redo     = partdist_wal_redo,
    .rm_desc     = partdist_wal_desc,
    .rm_identify = partdist_wal_identify,
    .rm_startup  = NULL,
    .rm_cleanup  = NULL,
    .rm_mask     = NULL,
    .rm_decode   = NULL,
};

void
RegisterPartitionWALRmgr(void)
{
    RegisterCustomRmgr(RM_EXPERIMENTAL_ID, &PartdistRmgrData);
}

/* ================================================================== */
/* Citus distributed-table detection                                    */
/* ================================================================== */

/*
 * IsCitusShardName — true if a relation name string ends with _<N> where N is
 * at least 4 decimal digits (Citus shard table naming convention).
 *
 * This works on the raw name string (e.g. from rte->eref->aliasname or from
 * get_rel_name), so it does not require a syscache lookup.
 */
static bool
IsCitusShardName(const char *name)
{
    const char *underscore;
    const char *p;

    if (name == NULL || *name == '\0')
        return false;

    underscore = strrchr(name, '_');
    if (underscore == NULL || *(underscore + 1) == '\0')
        return false;

    p = underscore + 1;
    while (*p)
    {
        if (*p < '0' || *p > '9')
            return false;
        p++;
    }

    return (p - (underscore + 1)) >= 4;
}

/*
 * IsCitusShardTable — check if relid looks like a Citus shard table by
 * examining its name via get_rel_name (syscache).
 *
 * Note: in Citus 13+ with streaming replication model ('s'), shard tables
 * are not stored as separate pg_class entries; get_rel_name may return NULL
 * for their OIDs.  In that case this function returns false and the caller
 * should use the alias-name-based check instead.
 */
bool
IsCitusShardTable(Oid relid)
{
    char *relname;

    if (!OidIsValid(relid))
        return false;

    relname = get_rel_name(relid);
    return IsCitusShardName(relname);
}

/*
 * GetLocalCitusGroupId — return this node's Citus groupid (0=coordinator,
 * >0=worker), or -1 if Citus is not installed or the lookup fails.
 *
 * Uses a direct heap scan of pg_dist_local_group instead of SPI to avoid
 * leaving the SPI stack dirty when called from within ExecutorFinish.
 * The result is cached per-backend after the first successful lookup.
 */
static int32
GetLocalCitusGroupId(void)
{
    static int32 cached_group_id = -2; /* -2 = not yet queried */
    Oid          oid;
    Relation     rel;
    SysScanDesc  scan;
    HeapTuple    tup;

    if (cached_group_id != -2)
        return cached_group_id;

    /* Check if pg_dist_local_group exists (Citus might not be installed) */
    oid = get_relname_relid("pg_dist_local_group", PG_CATALOG_NAMESPACE);
    if (!OidIsValid(oid))
    {
        cached_group_id = -1;
        return -1;
    }

    PG_TRY();
    {
        rel  = table_open(oid, AccessShareLock);
        scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
        tup  = systable_getnext(scan);
        if (HeapTupleIsValid(tup))
        {
            bool  isnull;
            Datum d = heap_getattr(tup, 1, RelationGetDescr(rel), &isnull);
            cached_group_id = isnull ? -1 : DatumGetInt32(d);
        }
        else
            cached_group_id = -1;
        systable_endscan(scan);
        table_close(rel, AccessShareLock);
    }
    PG_CATCH();
    {
        FlushErrorState();
        cached_group_id = -1;
    }
    PG_END_TRY();

    return cached_group_id;
}

/*
 * Per-backend cache entry for Citus distributed-table status.
 * Avoids repeated SPI calls for the same relation.
 */
typedef struct DistTableEntry
{
    Oid  relid;     /* hash key — must be first */
    bool is_dist;
} DistTableEntry;

static HTAB *DistTableCache = NULL;

/*
 * GetCitusDistTableStatus — true if relid appears in pg_dist_partition,
 * meaning it is a Citus distributed (logical) table.
 *
 * Uses a direct index scan of pg_dist_partition instead of SPI to avoid
 * leaving the SPI stack dirty when called from within ExecutorFinish.
 * Results are cached per-backend to avoid repeated catalog scans.
 */
static bool
GetCitusDistTableStatus(Oid relid)
{
    DistTableEntry *entry;
    bool  found;
    bool  is_dist = false;

    /* Lazy-init backend-local cache */
    if (DistTableCache == NULL)
    {
        HASHCTL ctl;
        memset(&ctl, 0, sizeof(ctl));
        ctl.keysize   = sizeof(Oid);
        ctl.entrysize = sizeof(DistTableEntry);
        DistTableCache = hash_create("pg_partdist_dist_cache", 256, &ctl,
                                      HASH_ELEM | HASH_BLOBS);
    }

    /* Check cache */
    entry = hash_search(DistTableCache, &relid, HASH_FIND, &found);
    if (found)
        return entry->is_dist;

    /* Cache miss: scan pg_dist_partition */
    {
        Oid         pgDistPartitionOid;
        Oid         pgDistPartitionIdxOid;
        Relation    rel;
        SysScanDesc scan;
        ScanKeyData key;
        HeapTuple   tup;

        pgDistPartitionOid = get_relname_relid("pg_dist_partition",
                                                PG_CATALOG_NAMESPACE);
        if (OidIsValid(pgDistPartitionOid))
        {
            pgDistPartitionIdxOid = get_relname_relid(
                "pg_dist_partition_logical_relid_index",
                PG_CATALOG_NAMESPACE);

            PG_TRY();
            {
                rel = table_open(pgDistPartitionOid, AccessShareLock);
                ScanKeyInit(&key, 1 /* logicalrelid */,
                            BTEqualStrategyNumber, F_OIDEQ,
                            ObjectIdGetDatum(relid));
                scan = systable_beginscan(rel, pgDistPartitionIdxOid,
                                          OidIsValid(pgDistPartitionIdxOid),
                                          NULL, 1, &key);
                tup = systable_getnext(scan);
                if (HeapTupleIsValid(tup))
                    is_dist = true;
                systable_endscan(scan);
                table_close(rel, AccessShareLock);
            }
            PG_CATCH();
            {
                FlushErrorState();
                is_dist = false;
            }
            PG_END_TRY();
        }
    }

    /* Cache the result */
    entry = hash_search(DistTableCache, &relid, HASH_ENTER, &found);
    entry->relid   = relid;
    entry->is_dist = is_dist;

    return is_dist;
}

/* ================================================================== */
/* ExecutorFinish hook (D2.1.3)                                         */
/* ================================================================== */

static bool in_partwal_finish = false;

/*
 * ShouldWritePartWAL — determine whether partition WAL should be written for
 * a given (relid, rel_alias) pair.
 *
 * @relid:     OID of the target relation in the executor plan.
 * @rel_alias: relation name from rte->eref->aliasname (parse tree), or NULL.
 *             This is used instead of get_rel_name() because Citus 13+ shard
 *             tables may not have visible pg_class entries; their names are
 *             only accessible from the parse tree (e.g. "e2e_test_102177").
 *
 * Priority:
 *  1. Manually registered in partdist.partition_map → always write.
 *  2. Alias name matches Citus shard pattern (<table>_<N>, N≥4 digits).
 *     This detects coordinator-routed INSERTs into virtual shard tables.
 *  3. Citus distributed table on a Worker node (pg_dist_partition lookup)
 *     → write only if groupid > 0 (not Coordinator).
 *
 * Must be called with in_partwal_finish = true so SPI queries don't re-enter.
 */
bool
ShouldWritePartWAL(Oid relid, const char *rel_alias)
{
    bool found = false;

    /* 1. partition_map cache (manually registered partitions) */
    if (PartitionMapHash != NULL && PartdistState != NULL)
    {
        LWLockAcquire(PartdistState->partition_lock, LW_SHARED);
        hash_search(PartitionMapHash, &relid, HASH_FIND, &found);
        LWLockRelease(PartdistState->partition_lock);
    }
    if (found)
        return true;

    /*
     * 2. Alias-name based Citus shard detection.
     *
     * When the Coordinator routes INSERT INTO foo TO a Worker, it sends
     * "INSERT INTO foo_102177 VALUES (...)" to that Worker.  The shard table
     * "foo_102177" may not have a visible pg_class entry (Citus 13+ MX), so
     * get_rel_name() returns NULL.  But rte->eref->aliasname contains the
     * literal "foo_102177" from the query parse tree.  Detecting the shard
     * suffix here allows pg_partdist to write WAL for the shard's relid.
     *
     * Also covers older Citus configs where shard tables ARE in pg_class
     * (IsCitusShardTable via get_rel_name would also catch those).
     */
    if (IsCitusShardName(rel_alias))
        return true;

    /* Also try syscache-based name (older Citus with pg_class shard entries) */
    if (IsCitusShardTable(relid))
        return true;

    /*
     * 3. Citus distributed table (logical relid in pg_dist_partition).
     *    Catches the outer Citus Adaptive scan on a Worker that has the
     *    logical table in its range table.
     *    Skip on Coordinator (groupid=0) — data lives on Workers.
     */
    if (GetLocalCitusGroupId() > 0 && GetCitusDistTableStatus(relid))
        return true;

    return false;
}

/*
 * pg_partdist_executor_finish — fires after every DML that touches a
 * relation registered in partdist.partition_map or a Citus distributed table.
 *
 * Writes one PartWALHeader record to the global WAL via the custom RMGR.
 * The Demux background worker reads from pg_wal and routes records into
 * per-partition pg_parwal/<partition_id>/ segment files.
 *
 * WritePartitionWALRecord auto-creates pg_parwal/<partition_id>/ on first
 * write, so no pre-registration of shard tables is required.
 *
 * orig_lsn is captured from XactLastRecEnd before our custom record is
 * inserted, so it points to the most recent DML record in this transaction.
 */
void
pg_partdist_executor_finish(QueryDesc *queryDesc)
{
    PlannedStmt *pstmt;
    ListCell    *lc;
    XLogRecPtr   orig_lsn;

    if (in_partwal_finish)
        return;

    /* Only DML */
    if (queryDesc->operation != CMD_INSERT &&
        queryDesc->operation != CMD_UPDATE &&
        queryDesc->operation != CMD_DELETE)
        return;

    /* WAL shmem must be ready */
    if (!XLogInsertAllowed())
        return;
    if (partwal_shmem == NULL || PartWALLSNHash == NULL)
        return;

    pstmt = queryDesc->plannedstmt;
    if (pstmt == NULL)
        return;

    orig_lsn = XactLastRecEnd;
    /* ON CONFLICT DO NOTHING writes no heap WAL; fall back to current insert ptr */
    if (orig_lsn == InvalidXLogRecPtr)
        orig_lsn = GetXLogInsertRecPtr();

    /*
     * Set the reentrancy flag before ShouldWritePartWAL so that any SPI
     * queries inside that function don't trigger our hook again.
     */
    in_partwal_finish = true;
    PG_TRY();
    {
        if (pstmt->resultRelations != NIL)
        {
            foreach(lc, pstmt->resultRelations)
            {
                Index          rti   = lfirst_int(lc);
                RangeTblEntry *rte   = rt_fetch(rti, pstmt->rtable);
                Oid            relid = rte->relid;
                const char    *alias = (rte->eref != NULL)
                                       ? rte->eref->aliasname : NULL;

                if (!ShouldWritePartWAL(relid, alias))
                    continue;

                WritePartitionWALRecord(relid, PARTWAL_FLAG_DATA, orig_lsn);
            }
        }
        else
        {
            /*
             * Fallback for Citus Adaptive CustomScan at coordinator level:
             * resultRelations is NIL; scan rtable for any matching relation.
             */
            foreach(lc, pstmt->rtable)
            {
                RangeTblEntry *rte   = (RangeTblEntry *) lfirst(lc);
                const char    *alias;

                if (rte->rtekind != RTE_RELATION || rte->relid == InvalidOid)
                    continue;

                alias = (rte->eref != NULL) ? rte->eref->aliasname : NULL;

                if (!ShouldWritePartWAL(rte->relid, alias))
                    continue;

                WritePartitionWALRecord(rte->relid, PARTWAL_FLAG_DATA, orig_lsn);
            }
        }
    }
    PG_FINALLY();
    {
        in_partwal_finish = false;
    }
    PG_END_TRY();
}

/* ================================================================== */
/* ProcessUtility hook (bulk INSERT / COPY interception)               */
/* ================================================================== */

static bool in_partwal_utility = false;

/*
 * pg_partdist_process_utility — write a PartWALHeader record after any
 * COPY FROM that targets a Citus shard table or manually registered
 * partition.
 *
 * Must be called AFTER the utility statement has already executed
 * successfully (i.e., no error was raised by the previous hook in the
 * chain).  This guarantees we only emit WAL for committed COPY data.
 *
 * Flow for INSERT INTO dist SELECT ... at coordinator:
 *   The coordinator uses CitusCopyDestReceiver to send each shard's rows
 *   to workers via COPY FROM STDIN over the protocol connection.  On the
 *   worker the COPY is processed as a utility statement (ProcessUtility),
 *   NOT through ExecutorFinish — so our executor hook never fires.  This
 *   function fills that gap.
 *
 * partition_id = OID of the physical shard table (e.g. bulk_test_102321).
 *   Citus hides shard tables via citus.override_table_visibility but they
 *   are real pg_class entries; RangeVarGetRelid finds them correctly.
 */
void
pg_partdist_process_utility(PlannedStmt *pstmt,
                             const char *queryString,
                             bool readOnlyTree,
                             ProcessUtilityContext context,
                             ParamListInfo params,
                             QueryEnvironment *queryEnv,
                             DestReceiver *dest,
                             QueryCompletion *qc)
{
    Node     *parsetree;
    CopyStmt *copyStmt;
    Oid       relid;
    const char *relname;
    XLogRecPtr  orig_lsn;

    if (in_partwal_utility)
        return;

    /* WAL subsystem must be ready */
    if (!XLogInsertAllowed())
        return;
    if (partwal_shmem == NULL || PartWALLSNHash == NULL)
        return;

    parsetree = pstmt->utilityStmt;
    if (!IsA(parsetree, CopyStmt))
        return;

    copyStmt = (CopyStmt *) parsetree;

    /* Only interested in COPY FROM (data ingestion), not COPY TO */
    if (!copyStmt->is_from || copyStmt->relation == NULL)
        return;

    /*
     * Resolve the target relation OID.  We use missing_ok=true because:
     *   a) the statement already succeeded (table must exist), and
     *   b) we never want a lookup failure here to mask the original result.
     */
    relid = RangeVarGetRelid(copyStmt->relation, NoLock, true /* missing_ok */);
    if (!OidIsValid(relid))
        return;

    relname = copyStmt->relation->relname;

    in_partwal_utility = true;
    PG_TRY();
    {
        if (ShouldWritePartWAL(relid, relname))
        {
            orig_lsn = XactLastRecEnd;
            if (orig_lsn == InvalidXLogRecPtr)
                orig_lsn = GetXLogInsertRecPtr();

            WritePartitionWALRecord(relid, PARTWAL_FLAG_DATA, orig_lsn);
        }
    }
    PG_FINALLY();
    {
        in_partwal_utility = false;
    }
    PG_END_TRY();
}

/* ================================================================== */
/* SQL-callable functions                                               */
/* ================================================================== */

/*
 * ResetPartitionWALState — remove all pg_parwal files for partition_id and
 * reset the shared-memory LSN counter back to 1.  Used by regression tests
 * to ensure each test run starts from a clean, deterministic state.
 */
void
ResetPartitionWALState(Oid partition_id)
{
    char          dirpath[MAXPGPATH];
    DIR          *dir;
    struct dirent *de;

    /* Reset shmem counter: remove the entry so next AllocPartitionLSN starts at 1 */
    if (partwal_shmem != NULL && PartWALLSNHash != NULL)
    {
        LWLockAcquire(partwal_shmem->lsn_lock, LW_EXCLUSIVE);
        hash_search(PartWALLSNHash, &partition_id, HASH_REMOVE, NULL);
        LWLockRelease(partwal_shmem->lsn_lock);
    }

    /* Delete all segment files in pg_parwal/<partition_id>/ */
    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir == NULL)
        return;

    while ((de = readdir(dir)) != NULL)
    {
        char filepath[MAXPGPATH];
        if (!IsXLogFileName(de->d_name))
            continue;
        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, de->d_name);
        (void) unlink(filepath);
    }
    closedir(dir);
}

PG_FUNCTION_INFO_V1(pg_partdist_reset_partition_wal_state);
Datum
pg_partdist_reset_partition_wal_state(PG_FUNCTION_ARGS)
{
    Oid partition_id = PG_GETARG_OID(0);
    ResetPartitionWALState(partition_id);
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_partdist_init_partition_wal);
Datum
pg_partdist_init_partition_wal(PG_FUNCTION_ARGS)
{
    Oid partition_id = PG_GETARG_OID(0);
    InitPartitionWALDirectory(partition_id);
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_partdist_partition_wal_exists);
Datum
pg_partdist_partition_wal_exists(PG_FUNCTION_ARGS)
{
    Oid         partition_id = PG_GETARG_OID(0);
    char        path[MAXPGPATH];
    struct stat st;

    snprintf(path, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);
    PG_RETURN_BOOL(stat(path, &st) == 0 && S_ISDIR(st.st_mode));
}

PG_FUNCTION_INFO_V1(pg_partdist_partition_wal_path);
Datum
pg_partdist_partition_wal_path(PG_FUNCTION_ARGS)
{
    Oid   partition_id = PG_GETARG_OID(0);
    int64 segno_arg    = PG_GETARG_INT64(1);
    char *rel          = GetPartitionWALPath(partition_id, (XLogSegNo) segno_arg);

    PG_RETURN_TEXT_P(cstring_to_text(rel));
}

PG_FUNCTION_INFO_V1(pg_partdist_cleanup_partition_wal);
Datum
pg_partdist_cleanup_partition_wal(PG_FUNCTION_ARGS)
{
    Oid        partition_id = PG_GETARG_OID(0);
    XLogRecPtr keep_ptr     = PG_GETARG_LSN(1);

    CleanupPartitionWAL(partition_id, keep_ptr);
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_partdist_alloc_partition_lsn);
Datum
pg_partdist_alloc_partition_lsn(PG_FUNCTION_ARGS)
{
    Oid    partition_id = PG_GETARG_OID(0);
    uint64 lsn;

    lsn = AllocPartitionLSN(partition_id);
    PG_RETURN_INT64((int64) lsn);
}

/*
 * pg_partdist_write_partition_wal_record(partition_id oid, flags int) → pg_lsn
 *
 * Directly write a PartWALHeader record for testing purposes.
 * Uses GetXLogInsertRecPtr() as the orig_lsn so the test can observe
 * a non-zero value without needing a preceding DML.
 */
PG_FUNCTION_INFO_V1(pg_partdist_write_partition_wal_record);
Datum
pg_partdist_write_partition_wal_record(PG_FUNCTION_ARGS)
{
    Oid        partition_id = PG_GETARG_OID(0);
    int32      flags_arg    = PG_GETARG_INT32(1);
    XLogRecPtr orig_lsn;
    XLogRecPtr result_lsn;

    /* Capture current WAL insert position as the "orig" LSN */
    orig_lsn = GetXLogInsertRecPtr();

    result_lsn = WritePartitionWALRecord(partition_id, (uint8) flags_arg, orig_lsn);
    if (result_lsn == InvalidXLogRecPtr)
        PG_RETURN_NULL();

    PG_RETURN_LSN(result_lsn);
}

/*
 * pg_partdist_check_partition_wal(partition_id oid)
 *   → TABLE(partition_lsn bigint, orig_node_lsn pg_lsn,
 *           flags integer, is_valid boolean)
 *
 * Scans every PartWALHeader record in pg_parwal/<partition_id>/ and
 * returns them as rows.  Files are processed in lexicographic (= segment)
 * order; within each file records appear in write order.
 */
PG_FUNCTION_INFO_V1(pg_partdist_check_partition_wal);
Datum
pg_partdist_check_partition_wal(PG_FUNCTION_ARGS)
{
    Oid             partition_id = PG_GETARG_OID(0);
    ReturnSetInfo  *rsinfo        = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc       tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext   old_cxt;
    char            dirpath[MAXPGPATH];
    DIR            *dir;
    struct dirent  *de;

    /* Validate calling context */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that "
                        "cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not allowed "
                        "in this context")));

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("return type must be a row type")));

    old_cxt   = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore  = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult  = tupstore;
    rsinfo->setDesc    = BlessTupleDesc(tupdesc);
    MemoryContextSwitchTo(old_cxt);

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir != NULL)
    {
        /* Collect segment file names */
        char   segfiles[256][MAXPGPATH];
        int    nfiles = 0;
        int    i, j;

        while ((de = readdir(dir)) != NULL && nfiles < 256)
        {
            if (IsXLogFileName(de->d_name))
            {
                strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
                nfiles++;
            }
        }
        closedir(dir);

        /* Sort lexicographically (equals segment order for same timeline) */
        for (i = 0; i < nfiles - 1; i++)
            for (j = i + 1; j < nfiles; j++)
                if (strcmp(segfiles[i], segfiles[j]) > 0)
                {
                    char tmp[MAXPGPATH];
                    strlcpy(tmp, segfiles[i], MAXPGPATH);
                    strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                    strlcpy(segfiles[j], tmp, MAXPGPATH);
                }

        for (i = 0; i < nfiles; i++)
        {
            char          filepath[MAXPGPATH];
            int           fd;
            PartWALHeader header;
            ssize_t       nbytes;

            snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
            fd = open(filepath, O_RDONLY);
            if (fd < 0)
                continue;

            while ((nbytes = read(fd, &header, sizeof(PartWALHeader)))
                   == (ssize_t) sizeof(PartWALHeader))
            {
                Datum  values[4];
                bool   nulls[4] = {false, false, false, false};
                bool   is_valid;

                is_valid = (header.magic        == PARTWAL_MAGIC         &&
                            header.partition_id == partition_id           &&
                            (header.flags == PARTWAL_FLAG_DATA      ||
                             header.flags == PARTWAL_FLAG_SKIP       ||
                             header.flags == PARTWAL_FLAG_CHECKPOINT));

                values[0] = Int64GetDatum((int64) header.partition_lsn);
                values[1] = LSNGetDatum(header.orig_node_lsn);
                values[2] = Int32GetDatum((int32) header.flags);
                values[3] = BoolGetDatum(is_valid);

                tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
            }
            close(fd);
        }
    }

    PG_RETURN_NULL();
}

/*
 * pg_partdist_verify_partition_wal(partition_id oid) → boolean
 *
 * Scan all PartWALHeader records in pg_parwal/<partition_id>/ and verify:
 *   - magic == PARTWAL_MAGIC
 *   - partition_id fields match the argument
 *   - partition_lsn is strictly monotone (no gaps, no duplicates)
 *
 * Returns true if all records pass; false on any violation.
 * An empty or non-existent directory is vacuously valid (returns true).
 */
PG_FUNCTION_INFO_V1(pg_partdist_verify_partition_wal);
Datum
pg_partdist_verify_partition_wal(PG_FUNCTION_ARGS)
{
    Oid           partition_id  = PG_GETARG_OID(0);
    char          dirpath[MAXPGPATH];
    DIR          *dir;
    struct dirent *de;
    uint64        expected_lsn  = 1;
    bool          valid         = true;
    char          segfiles[256][MAXPGPATH];
    int           nfiles        = 0;
    int           i, j;

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir == NULL)
        PG_RETURN_BOOL(true);   /* vacuously valid */

    while ((de = readdir(dir)) != NULL && nfiles < 256)
    {
        if (IsXLogFileName(de->d_name))
        {
            strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
            nfiles++;
        }
    }
    closedir(dir);

    /* Sort for stable ordering */
    for (i = 0; i < nfiles - 1; i++)
        for (j = i + 1; j < nfiles; j++)
            if (strcmp(segfiles[i], segfiles[j]) > 0)
            {
                char tmp[MAXPGPATH];
                strlcpy(tmp, segfiles[i], MAXPGPATH);
                strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                strlcpy(segfiles[j], tmp, MAXPGPATH);
            }

    for (i = 0; i < nfiles && valid; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALHeader header;
        ssize_t       nbytes;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY);
        if (fd < 0)
            continue;

        while ((nbytes = read(fd, &header, sizeof(PartWALHeader)))
               == (ssize_t) sizeof(PartWALHeader))
        {
            if (header.magic != PARTWAL_MAGIC ||
                header.partition_id != partition_id)
            {
                valid = false;
                break;
            }
            if (header.partition_lsn != expected_lsn)
            {
                valid = false;
                break;
            }
            expected_lsn++;
        }

        close(fd);
    }

    PG_RETURN_BOOL(valid);
}
