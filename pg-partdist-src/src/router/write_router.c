#include "pg_partdist.h"
#include "write_router.h"
#include "metadata_cache.h"

#include "executor/executor.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "utils/elog.h"
#include "access/xact.h"

/* Prevent recursive entry into the hook */
static bool in_partdist_hook = false;

/*
 * RouteStatusString — stable string representation for RouteStatus values.
 */
const char *
RouteStatusString(RouteStatus status)
{
    switch (status)
    {
        case ROUTE_LOCAL:     return "local";
        case ROUTE_REMOTE:    return "remote";
        case ROUTE_NOT_FOUND: return "not_found";
        case ROUTE_NODE_DOWN: return "node_down";
        default:              return "unknown";
    }
}

/*
 * RouteWriteRequest — determine where to send a write for the given partition.
 *
 * Decision logic:
 *   1. Look up partition_id in partition_map.
 *   2. If not found → ROUTE_NOT_FOUND.
 *   3. Look up the primary node in node_map.
 *   4. If the primary node is 'down' → ROUTE_NODE_DOWN.
 *   5. If primary_node == pg_partdist.local_node_id → ROUTE_LOCAL.
 *   6. Otherwise → ROUTE_REMOTE.
 *
 * When local_node_id is -1 (unconfigured), step 5 never matches and
 * every write resolves to ROUTE_REMOTE.  This is intentional: operators
 * must set pg_partdist.local_node_id before the routing layer becomes
 * fully active.
 */
RouteStatus
RouteWriteRequest(WriteRequest *req)
{
    int32        primary_node;
    NodeMapEntry *node;
    RouteStatus   status;

    /* Step 1-2: resolve partition → primary */
    primary_node = GetPartitionPrimary(req->partition_id);
    if (primary_node < 0)
        return ROUTE_NOT_FOUND;

    /* Step 3-4: resolve primary → node info */
    node = GetNodeInfo(primary_node);
    if (node != NULL && node->status == NODE_DOWN)
    {
        pfree(node);
        return ROUTE_NODE_DOWN;
    }
    if (node != NULL)
        pfree(node);

    /* Step 5-6: local vs remote */
    if (pg_partdist_local_node_id >= 0 &&
        primary_node == pg_partdist_local_node_id)
        status = ROUTE_LOCAL;
    else
        status = ROUTE_REMOTE;

    return status;
}

/*
 * pg_partdist_executor_start — ExecutorStart hook.
 *
 * For INSERT/UPDATE/DELETE on relations that appear in partition_map,
 * evaluate the routing decision and emit a DEBUG1 log entry.
 *
 * In milestone 1.2 the hook is informational only: it logs the routing
 * decision and lets every operation proceed normally.  The hook is
 * intentionally conservative:
 *   - It skips if the cache is not initialised (no shared_preload_libraries).
 *   - It skips if local_node_id is unconfigured.
 *   - It does not intercept non-DML or utility statements.
 */
void
pg_partdist_executor_start(QueryDesc *queryDesc, int eflags)
{
    PlannedStmt *pstmt;
    ListCell    *lc;

    /* Guard against recursive entry (the SPI calls inside routing also
     * go through ExecutorStart). */
    if (in_partdist_hook)
        return;

    /* Only intercept DML */
    if (queryDesc->operation != CMD_INSERT &&
        queryDesc->operation != CMD_UPDATE &&
        queryDesc->operation != CMD_DELETE)
        return;

    /* Require local_node_id to be set */
    if (pg_partdist_local_node_id < 0)
        return;

    /* Require the cache to be initialised */
    if (PartitionMapHash == NULL)
        return;

    pstmt = queryDesc->plannedstmt;
    if (pstmt == NULL || (pstmt->resultRelations == NIL && pstmt->rtable == NIL))
        return;

    /* For Citus distributed DML, resultRelations may be NIL; skip routing
     * check in that case (Citus handles routing itself). */
    if (pstmt->resultRelations == NIL)
        return;

    in_partdist_hook = true;
    PG_TRY();
    {
        foreach(lc, pstmt->resultRelations)
        {
            Index          rti = lfirst_int(lc);
            RangeTblEntry *rte;
            Oid            relid;
            WriteRequest   req;
            RouteStatus    route;

            rte   = rt_fetch(rti, pstmt->rtable);
            relid = rte->relid;

            req.partition_id = relid;
            req.xid          = GetCurrentTransactionIdIfAny();
            req.cid          = GetCurrentCommandId(false);
            req.query_string = (char *) queryDesc->sourceText;

            route = RouteWriteRequest(&req);

            /* Log the routing decision */
            ereport(DEBUG1,
                    (errmsg("pg_partdist: relation %u → %s",
                            relid, RouteStatusString(route))));

            /*
             * In milestone 1.2, remote writes are rejected with an
             * informative error so that the routing path is verifiable
             * via tests without needing real cross-node forwarding.
             */
            if (route == ROUTE_REMOTE)
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("pg_partdist: write to partition %u must be "
                                "routed to node %d (remote)",
                                relid,
                                GetPartitionPrimary(relid)),
                         errhint("Configure cross-node forwarding or execute "
                                 "the write on the primary node.")));

            if (route == ROUTE_NODE_DOWN)
                ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                         errmsg("pg_partdist: primary node for partition %u "
                                "is currently down",
                                relid)));
        }
    }
    PG_FINALLY();
    {
        in_partdist_hook = false;
    }
    PG_END_TRY();
}
