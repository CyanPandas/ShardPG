#include "pg_partdist.h"
#include "metadata_cache.h"
#include "write_router.h"
#include "partition_wal.h"
#include "demux_worker.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "funcapi.h"
#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

/* ---- GUC ---- */
int pg_partdist_local_node_id = -1;

/* ---- hook save slots ---- */
static shmem_request_hook_type    prev_shmem_request_hook    = NULL;
static shmem_startup_hook_type    prev_shmem_startup_hook    = NULL;
static ExecutorStart_hook_type    prev_ExecutorStart_hook    = NULL;
static ExecutorFinish_hook_type   prev_ExecutorFinish_hook   = NULL;
static object_access_hook_type    prev_object_access_hook    = NULL;
static ProcessUtility_hook_type   prev_ProcessUtility_hook   = NULL;

/* ---- forward declarations ---- */
void _PG_init(void);

PG_FUNCTION_INFO_V1(pg_partdist_version);
PG_FUNCTION_INFO_V1(pg_partdist_get_primary);
PG_FUNCTION_INFO_V1(pg_partdist_cache_invalidate);
PG_FUNCTION_INFO_V1(pg_partdist_bump_metadata_version);
PG_FUNCTION_INFO_V1(pg_partdist_cache_stats);
PG_FUNCTION_INFO_V1(pg_partdist_route_write);

/* ---- chained hook wrappers ---- */

static void
partdist_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();
    pg_partdist_shmem_request_hook();
}

static void
partdist_shmem_startup(void)
{
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    pg_partdist_shmem_startup_hook();
}

static void
partdist_executor_start(QueryDesc *queryDesc, int eflags)
{
    pg_partdist_executor_start(queryDesc, eflags);

    if (prev_ExecutorStart_hook)
        prev_ExecutorStart_hook(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

static void
partdist_executor_finish(QueryDesc *queryDesc)
{
    /* Run the primary finish chain first */
    if (prev_ExecutorFinish_hook)
        prev_ExecutorFinish_hook(queryDesc);
    else
        standard_ExecutorFinish(queryDesc);

    /* Then write partition WAL records for any partition-mapped relations */
    pg_partdist_executor_finish(queryDesc);
}

/*
 * partdist_object_access — object_access_hook for Goal 1 (shard creation).
 *
 * When Citus creates a shard table on a Worker (OAT_POST_CREATE on a
 * relation), auto-create the pg_parwal/<shard_oid>/ directory so it's ready
 * before the first INSERT arrives.  This is optional since
 * WritePartitionWALRecord also calls InitPartitionWALDirectory, but it is
 * nice to have the directory immediately after shard creation.
 */
static void
partdist_object_access(ObjectAccessType access,
                        Oid classId,
                        Oid objectId,
                        int subId,
                        void *arg)
{
    if (prev_object_access_hook)
        prev_object_access_hook(access, classId, objectId, subId, arg);

    /* Only interested in newly created relations (not indexes, types, etc.) */
    if (access != OAT_POST_CREATE ||
        classId != RelationRelationId ||
        subId != 0)
        return;

    if (!OidIsValid(objectId))
        return;

    /* Auto-init WAL directory for Citus shard tables */
    if (!IsCitusShardTable(objectId))
        return;

    PG_TRY();
    {
        InitPartitionWALDirectory(objectId);
    }
    PG_CATCH();
    {
        FlushErrorState();
        ereport(WARNING,
                (errmsg("pg_partdist: could not initialize WAL directory "
                        "for shard OID %u", objectId)));
    }
    PG_END_TRY();
}

/*
 * partdist_process_utility — ProcessUtility_hook wrapper.
 *
 * Calls the previous hook (Citus + standard) first, then invokes
 * pg_partdist_process_utility to write PartWAL records for any COPY
 * FROM that targeted a shard table.  The pg_partdist call happens only
 * after the previous hook returns without error, so aborted COPY
 * operations do not generate spurious PartWAL entries.
 */
static void
partdist_process_utility(PlannedStmt *pstmt,
                          const char *queryString,
                          bool readOnlyTree,
                          ProcessUtilityContext context,
                          ParamListInfo params,
                          QueryEnvironment *queryEnv,
                          DestReceiver *dest,
                          QueryCompletion *qc)
{
    /* Execute the statement via the existing chain first */
    if (prev_ProcessUtility_hook)
        prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree,
                                 context, params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv, dest, qc);

    /* On successful return, record any shard-targeted COPY FROM */
    pg_partdist_process_utility(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv, dest, qc);
}

/* ---- module load ---- */

void
_PG_init(void)
{
    if (!process_shared_preload_libraries_in_progress)
    {
        /*
         * Not in shared_preload_libraries.  The SQL functions still work
         * (they fall back to direct SPI), but caching and routing hooks
         * are disabled.
         */
        return;
    }

    /* GUC: local node ID */
    DefineCustomIntVariable(
        "pg_partdist.local_node_id",
        "Node ID of the local PostgreSQL instance in the pg_partdist cluster.",
        NULL,
        &pg_partdist_local_node_id,
        -1,         /* boot default */
        -1,         /* min */
        INT_MAX,    /* max */
        PGC_USERSET,
        0,
        NULL, NULL, NULL
    );

    /* Chain shared-memory hooks */
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = partdist_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = partdist_shmem_startup;

    /* Chain executor hooks */
    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = partdist_executor_start;

    prev_ExecutorFinish_hook = ExecutorFinish_hook;
    ExecutorFinish_hook = partdist_executor_finish;

    /* Chain object-access hook for shard table auto-detection */
    prev_object_access_hook = object_access_hook;
    object_access_hook = partdist_object_access;

    /* Chain ProcessUtility hook to intercept COPY FROM on shard tables */
    prev_ProcessUtility_hook = ProcessUtility_hook;
    ProcessUtility_hook = partdist_process_utility;

    /* Register custom WAL RMGR for partition WAL records */
    RegisterPartitionWALRmgr();

    /* Register Demux background worker */
    RegisterDemuxWorker();
}

/* ---- SQL-callable functions ---- */

Datum
pg_partdist_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("1.0"));
}

/* pg_partdist_get_primary(partition_id OID) → INTEGER */
Datum
pg_partdist_get_primary(PG_FUNCTION_ARGS)
{
    Oid     partition_id = PG_GETARG_OID(0);
    int32   primary;

    primary = GetPartitionPrimary(partition_id);
    if (primary < 0)
        PG_RETURN_NULL();

    PG_RETURN_INT32(primary);
}

/* pg_partdist_cache_invalidate() → VOID */
Datum
pg_partdist_cache_invalidate(PG_FUNCTION_ARGS)
{
    InvalidatePartdistCache();
    PG_RETURN_VOID();
}

/* pg_partdist_bump_metadata_version() → VOID */
Datum
pg_partdist_bump_metadata_version(PG_FUNCTION_ARGS)
{
    if (PartdistState == NULL)
        PG_RETURN_VOID();     /* shmem not initialised */

    /*
     * Atomically increment the generation counter under the partition lock
     * (either lock would do; partition_lock is arbitrarily chosen here).
     */
    LWLockAcquire(PartdistState->partition_lock, LW_EXCLUSIVE);
    PartdistState->metadata_generation++;
    LWLockRelease(PartdistState->partition_lock);

    PG_RETURN_VOID();
}

/* pg_partdist_cache_stats() → TABLE(...) */
Datum
pg_partdist_cache_stats(PG_FUNCTION_ARGS)
{
    TupleDesc   tupdesc;
    Datum       values[5];
    bool        nulls[5];
    HeapTuple   tuple;

    /* Build result tuple descriptor */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    tupdesc = BlessTupleDesc(tupdesc);

    memset(nulls, false, sizeof(nulls));

    if (PartdistState == NULL)
    {
        /* shmem not initialised — return all zeros */
        values[0] = Int64GetDatum(0);
        values[1] = Int64GetDatum(0);
        values[2] = Int64GetDatum(0);
        values[3] = Int64GetDatum(0);
        values[4] = Int64GetDatum(0);
    }
    else
    {
        LWLockAcquire(PartdistState->partition_lock, LW_SHARED);
        values[0] = Int64GetDatum((int64) PartdistState->partition_hits);
        values[1] = Int64GetDatum((int64) PartdistState->partition_misses);
        values[2] = Int64GetDatum((int64) PartdistState->node_hits);
        values[3] = Int64GetDatum((int64) PartdistState->node_misses);
        values[4] = Int64GetDatum(PartdistState->metadata_generation);
        LWLockRelease(PartdistState->partition_lock);
    }

    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/* pg_partdist_route_write(partition_id OID) → TEXT */
Datum
pg_partdist_route_write(PG_FUNCTION_ARGS)
{
    Oid             partition_id = PG_GETARG_OID(0);
    WriteRequest    req;
    RouteStatus     status;
    const char     *result;

    req.partition_id  = partition_id;
    req.xid           = GetCurrentTransactionId();
    req.cid           = GetCurrentCommandId(false);
    req.query_string  = NULL;

    status = RouteWriteRequest(&req);
    result = RouteStatusString(status);

    PG_RETURN_TEXT_P(cstring_to_text(result));
}
