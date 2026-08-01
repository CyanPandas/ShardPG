/*
 * partition_wal.c
 *
 * parwal-2.0 — pg_parwal directory management, hook integration, and
 * SQL functions.
 *
 * Design changes from 1.x:
 *   - No custom WAL RMGR (RM_EXPERIMENTAL_ID / XLogInsert removed).
 *   - ExecutorStart hook captures start_lsn into a per-backend static.
 *   - ExecutorFinish hook enqueues (partition_id, relfilenode, start_lsn,
 *     end_lsn) into the shmem ring buffer.
 *   - ProcessUtility hook does the same for COPY FROM.
 *   - The Demux Worker dequeues entries and writes PartWALRecord structs
 *     (header + raw WAL data) to pg_parwal/<partition_id>/ files.
 *
 * On-disk format: each pg_parwal/<partition_id>/<segname> file is a
 * flat sequence of (PartWALRecord header, data_len raw bytes) records.
 * A separate "checkpoint" file tracks the last processed LSN and the
 * relfilenode for crash recovery.
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "partition_wal.h"
#include "partition_wal_header.h"
#include "partition_wal_writer.h"
#include "partwal_sync.h"
#include "metadata_cache.h"
#include "demux_worker.h"
#include "shard_fileset.h"

#include "access/table.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"

/* ================================================================== */
/* Shared memory (minimal: no LSN hash needed; queue handles it)       */
/* ================================================================== */

/*
 * In parwal-2.0, the only WAL-specific shared state is the queue
 * (in partwal_sync.c).  We keep a stub PartWALSharedState for the
 * lock tranche slot required by RequestPartitionWALShmem().
 */
#define PARTWAL_SHMEM_NAME  "pg_partdist_wal_state"

typedef struct PartWALSharedState
{
    LWLock *dummy_lock; /* placeholder — not actively used in 2.0 */
} PartWALSharedState;

static PartWALSharedState *partwal_shmem = NULL;

void
RequestPartitionWALShmem(void)
{
    RequestAddinShmemSpace(PartitionWALShmemSize());
    RequestNamedLWLockTranche(PARTWAL_LOCK_TRANCHE, 1);
    /* Request shmem for the synchronous write path (relfilenode lookup hash) */
    RequestPartWALSyncShmem();
}

Size
PartitionWALShmemSize(void)
{
    return sizeof(PartWALSharedState);
}

void
PartitionWALShmemInit(void)
{
    bool found;

    partwal_shmem = (PartWALSharedState *)
        ShmemInitStruct(PARTWAL_SHMEM_NAME,
                        sizeof(PartWALSharedState), &found);
    if (!found)
        partwal_shmem->dummy_lock =
            &(GetNamedLWLockTranche(PARTWAL_LOCK_TRANCHE))[0].lock;

    /* Initialise the synchronous write shmem (relfilenode lookup hash) */
    PartWALSyncShmemInit();
}

/* ================================================================== */
/* Directory management                                                 */
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

static RelFileNumber GetRelFileNumberSafe(Oid relid);

void
InitPartitionWALAndRegister(Oid partition_id)
{
    RelFileNumber rfn;

    InitPartitionWALDirectory(partition_id);

    rfn = GetRelFileNumberSafe(partition_id);
    if (RelFileNumberIsValid(rfn))
    {
        PartWALSyncRegister(partition_id, rfn);

        /*
         * fileset 化捕获（FRD §5）：主堆之外，索引 / TOAST 堆 / TOAST 索引
         * 也必须进反向映射，否则这几类记录整类漏捕获。本函数被
         * EnsurePartWALRegistered 按 DML 频度调用，catalog 遍历用
         * backend 本地环形缓存去重；DDL 变更后由 register_shard_fileset()
         * SQL 函数强制刷新（会绕过该缓存）。
         */
        if (IsTransactionState())
        {
#define FILESET_SEEN_MAX 16
            static Oid seen[FILESET_SEEN_MAX] = {0};
            static int seen_pos = 0;
            int        s;
            bool       done = false;

            for (s = 0; s < FILESET_SEEN_MAX; s++)
                if (seen[s] == partition_id)
                {
                    done = true;
                    break;
                }

            if (!done)
            {
                ShardFileSet fs;

                if (BuildShardFileSet(partition_id, &fs) > 0)
                {
                    RegisterShardFileSet(&fs);
                    seen[seen_pos] = partition_id;
                    seen_pos = (seen_pos + 1) % FILESET_SEEN_MAX;
                }
            }
#undef FILESET_SEEN_MAX
        }
    }
}

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
    char           dirpath[MAXPGPATH];
    DIR           *dir;
    struct dirent *de;

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir == NULL)
    {
        if (errno == ENOENT)
            return;
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
/* Citus distributed-table detection                                    */
/* ================================================================== */

bool
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

bool
IsCitusShardTable(Oid relid)
{
    char *relname;

    if (!OidIsValid(relid))
        return false;

    relname = get_rel_name(relid);
    return IsCitusShardName(relname);
}

static int32
GetLocalCitusGroupId(void)
{
    static int32 cached_group_id = -2;
    Oid          oid;
    Relation     rel;
    SysScanDesc  scan;
    HeapTuple    tup;

    if (cached_group_id != -2)
        return cached_group_id;

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

typedef struct DistTableEntry
{
    Oid  relid;
    bool is_dist;
} DistTableEntry;

static HTAB *DistTableCache = NULL;

static bool
GetCitusDistTableStatus(Oid relid)
{
    DistTableEntry *entry;
    bool  found;
    bool  is_dist = false;

    if (DistTableCache == NULL)
    {
        HASHCTL ctl;
        memset(&ctl, 0, sizeof(ctl));
        ctl.keysize   = sizeof(Oid);
        ctl.entrysize = sizeof(DistTableEntry);
        DistTableCache = hash_create("pg_partdist_dist_cache", 256, &ctl,
                                      HASH_ELEM | HASH_BLOBS);
    }

    entry = hash_search(DistTableCache, &relid, HASH_FIND, &found);
    if (found)
        return entry->is_dist;

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
                ScanKeyInit(&key, 1,
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

    entry = hash_search(DistTableCache, &relid, HASH_ENTER, &found);
    entry->relid   = relid;
    entry->is_dist = is_dist;

    return is_dist;
}

/* ================================================================== */
/* ShouldWritePartWAL                                                   */
/* ================================================================== */

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

    /* 2. Alias-name based Citus shard detection */
    if (IsCitusShardName(rel_alias))
        return true;

    if (IsCitusShardTable(relid))
        return true;

    /* 3. Citus distributed table on a Worker node */
    if (GetLocalCitusGroupId() > 0 && GetCitusDistTableStatus(relid))
        return true;

    return false;
}

/*
 * EnsurePartWALRegistered — lazy registration for Citus MX distributed tables.
 *
 * Called from partdist_executor_start() before each DML.  If the target
 * relation is a Citus distributed table on this worker but has not yet been
 * registered in the shmem relfilenode hash, registers it now so that the
 * XLogInsert() hook can identify WAL records belonging to it.
 *
 * Covers MX-mode tables whose names have no "_<shardid>" suffix and therefore
 * slip through IsCitusShardName() in the object_access hook.
 * ShouldWritePartWAL() step 3 (GetLocalCitusGroupId > 0 &&
 * GetCitusDistTableStatus) catches them.
 *
 * Idempotent: GetCitusDistTableStatus() caches results per-backend and
 * InitPartitionWALDirectory() tolerates EEXIST.
 */
void
EnsurePartWALRegistered(Oid relid)
{
    if (!OidIsValid(relid))
        return;

    if (!XLogInsertAllowed())
        return;

    if (!ShouldWritePartWAL(relid, get_rel_name(relid)))
        return;

    PG_TRY();
    {
        InitPartitionWALAndRegister(relid);
    }
    PG_CATCH();
    {
        FlushErrorState();
    }
    PG_END_TRY();
}

/*
 * GetRelFileNumberSafe — open a relation and return its relfilenode.
 * Returns InvalidRelFileNumber if the relation cannot be opened.
 * Extracted as a standalone function to avoid nested PG_TRY() blocks
 * (which trigger -Wshadow=compatible-local warnings from PostgreSQL's
 * PG_TRY macro expansion).
 */
static RelFileNumber
GetRelFileNumberSafe(Oid relid)
{
    Relation      rel;
    RelFileNumber rfn;

    PG_TRY();
    {
        rel = table_open(relid, NoLock);
        rfn = rel->rd_locator.relNumber;
        table_close(rel, NoLock);
    }
    PG_CATCH();
    {
        FlushErrorState();
        rfn = InvalidRelFileNumber;
    }
    PG_END_TRY();

    return rfn;
}

/* ================================================================== */
/* ProcessUtility hook (parwal-2.0)                                     */
/* ================================================================== */

/*
 * pg_partdist_process_utility — called from partdist_process_utility in
 * pg_partdist.c BEFORE the utility chain executes.
 *
 * For COPY FROM statements, ensures the target shard is registered in the
 * shmem relfilenode hash so the WAL insert hook can identify WAL records
 * written by the COPY.  No flush is needed: CommitPartWALSync() handles
 * writing buffered records at transaction commit.
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
    Node      *parsetree;
    CopyStmt  *copyStmt;

    if (!XLogInsertAllowed())
        return;

    parsetree = pstmt->utilityStmt;
    if (!IsA(parsetree, CopyStmt))
        return;

    copyStmt = (CopyStmt *) parsetree;
    if (!copyStmt->is_from || copyStmt->relation == NULL)
        return;

    /* Register the shard BEFORE the COPY executes so the hook sees it */
    if (copyStmt->relation != NULL)
    {
        Oid nspid = InvalidOid;
        Oid relid;

        if (copyStmt->relation->schemaname != NULL)
            nspid = get_namespace_oid(copyStmt->relation->schemaname,
                                      true /* missing_ok */);
        if (!OidIsValid(nspid))
            nspid = LookupExplicitNamespace("public", true);

        relid = get_relname_relid(copyStmt->relation->relname, nspid);
        if (OidIsValid(relid))
            EnsurePartWALRegistered(relid);
    }
}

/* ================================================================== */
/* ResetPartitionWALState                                               */
/* ================================================================== */

void
ResetPartitionWALState(Oid partition_id)
{
    char           dirpath[MAXPGPATH];
    DIR           *dir;
    struct dirent *de;

    /* Delete all segment files and the checkpoint file */
    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir == NULL)
        return;

    while ((de = readdir(dir)) != NULL)
    {
        char filepath[MAXPGPATH];

        /* Delete WAL segment files */
        if (IsXLogFileName(de->d_name))
        {
            snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, de->d_name);
            (void) unlink(filepath);
        }
        /* Delete checkpoint file */
        else if (strcmp(de->d_name, PARTWAL_CHECKPOINT_FILENAME) == 0)
        {
            snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, de->d_name);
            (void) unlink(filepath);
        }
    }
    closedir(dir);
}

/* ================================================================== */
/* SQL-callable functions                                               */
/* ================================================================== */

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
    InitPartitionWALAndRegister(partition_id);
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

/*
 * alloc_partition_lsn — kept for SQL compatibility.
 * Reads the checkpoint, increments last_part_lsn, writes it back, and
 * returns the allocated value (1-based, strictly monotone).
 */
PG_FUNCTION_INFO_V1(pg_partdist_alloc_partition_lsn);
Datum
pg_partdist_alloc_partition_lsn(PG_FUNCTION_ARGS)
{
    Oid                   partition_id = PG_GETARG_OID(0);
    PartWALCheckpointFile chk;
    uint64                next_lsn;

    if (ReadPartWALCheckpoint(partition_id, &chk))
        next_lsn = chk.last_part_lsn + 1;
    else
    {
        memset(&chk, 0, sizeof(chk));
        next_lsn = 1;
    }

    /* Ensure directory exists before writing checkpoint */
    InitPartitionWALDirectory(partition_id);

    /* Persist the incremented counter */
    WritePartWALCheckpoint(partition_id,
                           (RelFileNumber) partition_id,
                           chk.last_wal_lsn,
                           next_lsn);

    PG_RETURN_INT64((int64) next_lsn);
}

/*
 * write_partition_wal_record — test helper.
 *
 * Writes a synthetic PartWALRecord directly to pg_parwal/<partition_id>/
 * using the writer API.  This bypasses the demux WAL-scan path so that
 * unit tests with synthetic (non-existent) partition OIDs still work.
 *
 * The "flags" argument is stored in the rmid field of PartWALRecord so that
 * check_partition_wal() (which returns rmid as "flags") can verify it.
 *
 * After writing, the demux latch is set so that demux_flush() returns quickly.
 */
PG_FUNCTION_INFO_V1(pg_partdist_write_partition_wal_record);
Datum
pg_partdist_write_partition_wal_record(PG_FUNCTION_ARGS)
{
    Oid                 partition_id = PG_GETARG_OID(0);
    int32               flags        = PG_GETARG_INT32(1);
    XLogRecPtr          lsn;
    TimeLineID          tli;
    PartitionWALWriter *writer;
    static const char   dummy_data[1] = {0};

    if (!XLogInsertAllowed())
        PG_RETURN_NULL();

    /*
     * Use the WAL insert pointer (the next-record position), not the flush
     * pointer.  GetFlushRecPtr() can return the exact segment-boundary LSN
     * (e.g. 1/2E000000) right after pg_switch_wal(), which pg_walfile_name_offset
     * assigns to the *previous* segment's file.  GetXLogInsertRecPtr() lands
     * a few bytes past the page header in the new segment, so it unambiguously
     * belongs to the correct segment file — critical for T4's boundary detection.
     */
    lsn = GetXLogInsertRecPtr();
    if (XLogRecPtrIsInvalid(lsn))
        lsn = GetFlushRecPtr(&tli);
    if (XLogRecPtrIsInvalid(lsn))
        PG_RETURN_NULL();

    InitPartitionWALDirectory(partition_id);

    PG_TRY();
    {
        writer = CreatePartitionWALWriter(partition_id,
                                          (RelFileNumber) partition_id);
        if (writer == NULL)
            PG_RETURN_NULL();

        AppendPartWALRecord(writer, lsn,
                            (uint8)(flags & 0xFF),
                            0,
                            dummy_data, 0,
                            InvalidTransactionId);
        FlushPartitionWALWriter(writer, true);
        DestroyPartitionWALWriter(writer);
    }
    PG_CATCH();
    {
        FlushErrorState();
        PG_RETURN_NULL();
    }
    PG_END_TRY();

    /* Wake the demux so demux_flush() returns quickly */
    if (DemuxState != NULL && DemuxState->demux_latch != NULL)
        SetLatch(DemuxState->demux_latch);

    PG_RETURN_LSN(lsn);
}

/*
 * check_partition_wal(partition_id oid)
 *   → TABLE(partition_lsn bigint, orig_node_lsn pg_lsn, flags integer,
 *            is_valid boolean)
 *
 * Scans every PartWALRecord in pg_parwal/<partition_id>/.
 * In parwal-2.0, "flags" is mapped to rmid for backward SQL compat;
 * is_valid checks magic and partition_id.
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
    char            segfiles[256][MAXPGPATH];
    int             nfiles = 0;
    int             i, j;

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

    old_cxt  = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult  = tupstore;
    rsinfo->setDesc    = BlessTupleDesc(tupdesc);
    MemoryContextSwitchTo(old_cxt);

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir == NULL)
        PG_RETURN_NULL();

    while ((de = readdir(dir)) != NULL && nfiles < 256)
    {
        if (IsXLogFileName(de->d_name))
        {
            strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
            nfiles++;
        }
    }
    closedir(dir);

    /* Sort ascending */
    for (i = 0; i < nfiles - 1; i++)
        for (j = i + 1; j < nfiles; j++)
            if (strcmp(segfiles[i], segfiles[j]) > 0)
            {
                char tmp[MAXPGPATH];
                strlcpy(tmp,         segfiles[i], MAXPGPATH);
                strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                strlcpy(segfiles[j], tmp,         MAXPGPATH);
            }

    for (i = 0; i < nfiles; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALRecord rec;
        ssize_t       nb;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY);
        if (fd < 0)
            continue;

        while ((nb = read(fd, &rec, sizeof(PartWALRecord)))
               == (ssize_t) sizeof(PartWALRecord))
        {
            Datum  values[4];
            bool   nulls[4] = {false, false, false, false};
            bool   is_valid;

            is_valid = (rec.magic        == PARTWAL_MAGIC &&
                        rec.partition_id == partition_id);

            values[0] = Int64GetDatum((int64) rec.partition_lsn);
            values[1] = LSNGetDatum(rec.orig_lsn);
            /* "flags" column: report rmid for compatibility */
            values[2] = Int32GetDatum((int32) rec.rmid);
            values[3] = BoolGetDatum(is_valid);

            tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);

            /* Skip the variable-length data payload */
            if (rec.data_len > 0)
            {
                if (lseek(fd, (off_t) rec.data_len, SEEK_CUR) < 0)
                    break;
            }
        }
        close(fd);
    }

    PG_RETURN_NULL();
}

/*
 * verify_partition_wal(partition_id oid) → boolean
 *
 * Scan all PartWALRecord headers in pg_parwal/<partition_id>/ and verify:
 *   - magic == PARTWAL_MAGIC
 *   - partition_id fields match the argument
 *   - partition_lsn is strictly monotone (no gaps, no duplicates)
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
        PG_RETURN_BOOL(true);

    while ((de = readdir(dir)) != NULL && nfiles < 256)
    {
        if (IsXLogFileName(de->d_name))
        {
            strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
            nfiles++;
        }
    }
    closedir(dir);

    for (i = 0; i < nfiles - 1; i++)
        for (j = i + 1; j < nfiles; j++)
            if (strcmp(segfiles[i], segfiles[j]) > 0)
            {
                char tmp[MAXPGPATH];
                strlcpy(tmp,         segfiles[i], MAXPGPATH);
                strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                strlcpy(segfiles[j], tmp,         MAXPGPATH);
            }

    for (i = 0; i < nfiles && valid; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALRecord rec;
        ssize_t       nb;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY);
        if (fd < 0)
            continue;

        while ((nb = read(fd, &rec, sizeof(PartWALRecord)))
               == (ssize_t) sizeof(PartWALRecord))
        {
            if (rec.magic != PARTWAL_MAGIC ||
                rec.partition_id != partition_id)
            {
                valid = false;
                break;
            }
            if (rec.partition_lsn != expected_lsn)
            {
                valid = false;
                break;
            }
            expected_lsn++;

            /* Skip variable-length payload */
            if (rec.data_len > 0)
            {
                if (lseek(fd, (off_t) rec.data_len, SEEK_CUR) < 0)
                {
                    valid = false;
                    break;
                }
            }
        }

        close(fd);
    }

    PG_RETURN_BOOL(valid);
}
