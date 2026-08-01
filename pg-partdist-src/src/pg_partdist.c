#include "pg_partdist.h"
#include "metadata_cache.h"
#include "write_router.h"
#include "partition_wal.h"
#include "partwal_sync.h"
#include "demux_worker.h"
#include "shard_replay.h"

#include "storage/bufmgr.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "funcapi.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "executor/executor.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "tcop/utility.h"

PG_MODULE_MAGIC;

/* ---- GUC ---- */
int pg_partdist_local_node_id = -1;

/* ---- hook save slots ---- */
static shmem_request_hook_type    prev_shmem_request_hook    = NULL;
static shmem_startup_hook_type    prev_shmem_startup_hook    = NULL;
static ExecutorStart_hook_type    prev_ExecutorStart_hook    = NULL;
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

/*
 * PartWALXactCallback — transaction event callback.
 *
 * PRE_COMMIT / PRE_PREPARE:
 *   Drain PartWALCtl ring-buffer slots to pg_parwal and fsync via
 *   PartWALFlush() — strictly before XLogFlush() writes the commit/prepare
 *   WAL record.  This is the [A] < [B] atomicity invariant.
 *   PRE_PREPARE handles Citus 2PC: TopTransactionContext is freed at PREPARE
 *   time; calling PartWALFlush() here (before the context is torn down)
 *   prevents use-after-free on the per-backend max-LSN tracker.
 *
 * ABORT:
 *   Invalidate this backend's pending ring-buffer slots; no disk I/O.
 *
 * COMMIT:
 *   Update last_committed_lsn for demux_progress() latency tracking.
 */
static void
PartWALXactCallback(XactEvent event, void *arg)
{
    TimeLineID  tli;
    XLogRecPtr  flush_now;

    switch (event)
    {
        case XACT_EVENT_PRE_COMMIT:
        case XACT_EVENT_PRE_PREPARE:
            PartWALFlush(InvalidXLogRecPtr);
            break;

        case XACT_EVENT_ABORT:
            PartWALAbort();
            break;

        case XACT_EVENT_COMMIT:
            if (DemuxState == NULL)
                break;
            flush_now = GetFlushRecPtr(&tli);
            if (flush_now == InvalidXLogRecPtr)
                break;
            LWLockAcquire(DemuxState->lock, LW_EXCLUSIVE);
            if (DemuxState->last_committed_lsn == InvalidXLogRecPtr ||
                DemuxState->last_committed_lsn < flush_now)
                DemuxState->last_committed_lsn = flush_now;
            LWLockRelease(DemuxState->lock);
            break;

        default:
            break;
    }
}

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

/*
 * partdist_executor_start — ExecutorStart hook.
 *
 * Lazily registers each DML target relation in the shmem relfilenode hash
 * so the WAL insert hook can identify WAL records belonging to it.
 * No start_lsn capture needed — the hook now buffers records directly.
 */
static void
partdist_executor_start(QueryDesc *queryDesc, int eflags)
{
    if (queryDesc->operation == CMD_INSERT ||
        queryDesc->operation == CMD_UPDATE ||
        queryDesc->operation == CMD_DELETE)
    {
        if (queryDesc->plannedstmt != NULL &&
            queryDesc->plannedstmt->resultRelations != NIL)
        {
            ListCell *lc;

            foreach(lc, queryDesc->plannedstmt->resultRelations)
            {
                Index          rti = lfirst_int(lc);
                RangeTblEntry *rte = rt_fetch(rti,
                                              queryDesc->plannedstmt->rtable);
                if (rte != NULL && OidIsValid(rte->relid))
                    EnsurePartWALRegistered(rte->relid);
            }
        }
    }

    /* Call the routing check in write_router.c */
    pg_partdist_executor_start(queryDesc, eflags);

    if (prev_ExecutorStart_hook)
        prev_ExecutorStart_hook(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

/*
 * partdist_object_access — object_access_hook for shard creation.
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

    if (access != OAT_POST_CREATE ||
        classId != RelationRelationId ||
        subId != 0)
        return;

    if (!XLogInsertAllowed())
        return;

    if (get_rel_relkind(objectId) != RELKIND_RELATION)
        return;

    if (!IsCitusShardTable(objectId))
        return;

    PG_TRY();
    {
        InitPartitionWALAndRegister(objectId);
    }
    PG_CATCH();
    {
        FlushErrorState();
        ereport(WARNING,
                (errmsg("pg_partdist: could not initialize WAL "
                        "directory for shard OID %u", objectId)));
    }
    PG_END_TRY();
}

/*
 * partdist_process_utility — ProcessUtility_hook wrapper.
 *
 * For COPY FROM statements, calls pg_partdist_process_utility BEFORE the
 * chain so that the target shard is registered in the relfilenode hash
 * before the COPY writes any WAL records.
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
    /* Register COPY FROM target BEFORE the chain writes WAL */
    pg_partdist_process_utility(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv, dest, qc);

    /* Execute the statement via the existing chain */
    if (prev_ProcessUtility_hook)
        prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree,
                                 context, params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv, dest, qc);
}

/* ---- module load ---- */

void
_PG_init(void)
{
    if (!process_shared_preload_libraries_in_progress)
        return;

    /* GUC: local node ID */
    DefineCustomIntVariable(
        "pg_partdist.local_node_id",
        "Node ID of the local PostgreSQL instance in the pg_partdist cluster.",
        NULL,
        &pg_partdist_local_node_id,
        -1,
        -1,
        INT_MAX,
        PGC_USERSET,
        0,
        NULL, NULL, NULL
    );

    /* Chain shared-memory hooks */
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = partdist_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = partdist_shmem_startup;

    /* Chain executor start hook (lazy shard registration) */
    prev_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = partdist_executor_start;

    /* Chain object-access hook */
    prev_object_access_hook = object_access_hook;
    object_access_hook = partdist_object_access;

    /* Chain ProcessUtility hook */
    prev_ProcessUtility_hook = ProcessUtility_hook;
    ProcessUtility_hook = partdist_process_utility;

    /*
     * Install WAL insert hook (mirrors XLogInsert role in pg_wal design).
     * For each XLogInsert() touching a tracked partition's relfilenode,
     * PartWALInsert() writes one slot to the PartWALCtl shared ring buffer.
     * PartWALFlush() drains the buffer to pg_parwal at XACT_EVENT_PRE_COMMIT.
     */
    wal_insert_hook = PartWALInsert;

    /*
     * 补丁 0002 豁免钩子：物理回放副本页面携带 leader 坐标 LSN，
     * FlushBuffer 对命中副本文件集合的页跳过 XLogFlush（FRD §8.3）。
     * 在每个进程（含 checkpointer/bgwriter）的 _PG_init 都会装上。
     */
    buffer_flush_lsn_exempt_hook = PartDistFlushExemptHook;

    /* Replay GUCs + launcher（FRD §7：worker 池 + 排他认领） */
    DefineReplayGUCs();
    RegisterReplayLauncher();

    /*
     * 导出惰性回放的触发入口。pg_raft 在选举胜出、准备把某分区提升为
     * primary 时取用：catchup(shard, commit_index) 同步追平后才对外服务。
     * 经 rendezvous variable 传递，两个扩展之间无编译期依赖。
     */
    {
        void **rv = find_rendezvous_variable("partdist_replay_catchup_hook");

        *rv = (void *) ShardReplayCatchUp;
    }

    /* Register Demux background worker for crash recovery at startup */
    RegisterDemuxWorker();

    /* Transaction callback: write PartWAL at PRE_COMMIT, discard on ABORT */
    RegisterXactCallback(PartWALXactCallback, NULL);
}

/* ---- SQL-callable functions ---- */

Datum
pg_partdist_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("1.0"));
}

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

Datum
pg_partdist_cache_invalidate(PG_FUNCTION_ARGS)
{
    InvalidatePartdistCache();
    PG_RETURN_VOID();
}

Datum
pg_partdist_bump_metadata_version(PG_FUNCTION_ARGS)
{
    if (PartdistState == NULL)
        PG_RETURN_VOID();

    LWLockAcquire(PartdistState->partition_lock, LW_EXCLUSIVE);
    PartdistState->metadata_generation++;
    LWLockRelease(PartdistState->partition_lock);

    PG_RETURN_VOID();
}

Datum
pg_partdist_cache_stats(PG_FUNCTION_ARGS)
{
    TupleDesc   tupdesc;
    Datum       values[5];
    bool        nulls[5];
    HeapTuple   tuple;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));

    tupdesc = BlessTupleDesc(tupdesc);
    memset(nulls, false, sizeof(nulls));

    if (PartdistState == NULL)
    {
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
