#include "pg_partdist.h"
#include "metadata_cache.h"
#include "write_router.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "funcapi.h"
#include "access/xact.h"

PG_MODULE_MAGIC;

/* ---- GUC ---- */
int pg_partdist_local_node_id = -1;

/* ---- hook save slots ---- */
static shmem_request_hook_type  prev_shmem_request_hook  = NULL;
static shmem_startup_hook_type  prev_shmem_startup_hook  = NULL;
static ExecutorStart_hook_type  prev_ExecutorStart_hook  = NULL;

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

    /* Chain executor hook */
    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = partdist_executor_start;
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
