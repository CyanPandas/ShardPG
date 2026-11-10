/*
 * First-arrival shard coordination.
 *
 * The router owns client/task sockets, never votes or sends PREPARE. It parks
 * each task socket in dtx_handoff and forwards COMMIT to the chosen shard.
 * The shard coordinator seals its own enrolment list and publishes PREPARE.
 * Each original task backend consumes that command, prepares its own native
 * transaction, and votes only from XACT_EVENT_PREPARE (after EndPrepare).
 *
 * The replicated DECISION is also the durable async delivery queue. No task
 * socket or router-local pg_dist_transaction row is required for phase two.
 */
#include "postgres.h"
#include "dtx_coordinator.h"
#include "dtx_participant.h"
#include "dtx_pending.h"
#include "dtx_record.h"
#include "partwal_sync.h"
#include "tso.h"

#include <dlfcn.h>
#include "access/xact.h"
#include "access/transam.h"
#include "storage/procarray.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "distributed/connection_management.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_management.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/varlena.h"
#include "utils/wait_event.h"

bool shardpg_first_shard_coordinator = true;
static bool hooks_installed = false;
static bool router_started = false;
static bool router_transaction_ddl = false;
static void (*prepare_local_catalog)(void) = NULL;
static uint64 branch_native_fullxid = 0;
static int router_ddl_depth = 0;
static bool branch_is_ddl = false;
static bool inside_enlist = false;
static bool handing_off = false;
static int prepare_timeout_ms = 30000;
static char branch_gid[NAMEDATALEN];
static int64 branch_xid;
static uint64 branch_connection;
static int64 coordinator;
static int64 branch_shards[DTX_PENDING_MAX_PAIRS];
static Oid branch_partitions[DTX_PENDING_MAX_PAIRS];
static int branch_nshards;
static char coord_host[NAMEDATALEN];
static int coord_port;
static char rpc_database[NAMEDATALEN];

typedef void (*BranchHook)(MultiConnection *, StringInfo);
typedef bool (*DelegateHook)(void);

PGDLLEXPORT void DtxDispatchWorkerMain(Datum arg);

static const char *
rpc_host(const char *host)
{
    /* Same single-host aliases as pg_raft_format_conninfo. */
    if (strcmp(host, "master") == 0 || strcmp(host, "coordinator") == 0 ||
        (strncmp(host, "worker", 6) == 0 && strspn(host + 6, "0123456789") == strlen(host + 6)))
        return "127.0.0.1";
    return host;
}

/* Always an independent, synchronous transaction. Never interpolate connection
 * options or credentials into SQL or error messages. Caller owns returned text.
 */
static char *
rpc(const char *host, int port, const char *base, const char *sql)
{
    char portstr[16];
    char options[96];
    const char *keys[] = {"dbname", "host", "port", "user", "application_name",
                          "connect_timeout", "options", NULL};
    const char *values[8];
    PGconn *conn;
    PGresult *res;
    char *answer;
    char *socket_config = NULL;
    List *socket_dirs = NIL;

    snprintf(portstr, sizeof(portstr), "%d", port);
    snprintf(options, sizeof(options), "-c synchronous_commit=on -c statement_timeout=%d",
             prepare_timeout_ms + 10000);
    if (!rpc_database[0] && OidIsValid(MyDatabaseId) && IsTransactionState())
        strlcpy(rpc_database, get_database_name(MyDatabaseId), sizeof(rpc_database));
    values[0] = base ? base : (rpc_database[0] ? rpc_database : "postgres");
    values[1] = base ? NULL : rpc_host(host);
    /* Match pg_raft's local participant registration: a local RPC uses the
     * configured Unix socket even if loopback TCP could not bind at startup. */
    if (!base && port == PostPortNumber &&
        (strcmp(values[1], "127.0.0.1") == 0 || strcmp(values[1], "localhost") == 0))
    {
        socket_config = pstrdup(GetConfigOption("unix_socket_directories", false, false));
        if (SplitDirectoriesString(socket_config, ',', &socket_dirs) && socket_dirs != NIL)
            values[1] = (char *) linitial(socket_dirs);
    }
    values[2] = base ? NULL : portstr;
    values[3] = base ? NULL : "postgres";
    values[4] = "shardpg_dtx_internal";
    values[5] = "5";
    values[6] = options;
    values[7] = NULL;
    conn = PQconnectdbParams(keys, values, base != NULL);
    list_free_deep(socket_dirs);
    if (socket_config) pfree(socket_config);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        PQfinish(conn);
        ereport(ERROR, (errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
                        errmsg("DTX control connection failed")));
    }
    res = PQexec(conn, sql);
    if (res == NULL ||
        (PQresultStatus(res) != PGRES_COMMAND_OK &&
         (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1 ||
          PQnfields(res) != 1 || PQgetisnull(res, 0, 0))))
    {
        char *detail = pstrdup(res ? PQresultErrorMessage(res) : "no result");
        const char *state = res ? PQresultErrorField(res, PG_DIAG_SQLSTATE) : NULL;
        int code = state && strlen(state) == 5 ?
            MAKE_SQLSTATE(state[0],state[1],state[2],state[3],state[4]) : ERRCODE_CONNECTION_FAILURE;
        if (res) PQclear(res);
        PQfinish(conn);
        ereport(ERROR, (errcode(code),
                        errmsg("DTX control request failed"), errdetail_internal("%s", detail)));
    }
    answer = pstrdup(PQresultStatus(res) == PGRES_COMMAND_OK ? "t" : PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);
    return answer;
}

static char *
tso_control(const char *sql)
{
    const char *base = GetConfigOption("pg_partdist.tso_conninfo", false, false);
    if (!base || !base[0])
        ereport(ERROR, (errmsg("first-shard coordination requires a TSO connection")));
    return rpc(NULL, 0, base, sql);
}

static void
coordinator_address(int64 gsid, char *host, int *port)
{
    char *sql = psprintf("SELECT n.hostname,n.port FROM partdist.partition_map p "
                         "JOIN partdist.node_map n ON n.node_id=p.primary_node "
                         "WHERE p.partition_id=" INT64_FORMAT "::oid", gsid);
    bool isnull;
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "DTX cannot open SPI");
    PushActiveSnapshot(GetTransactionSnapshot());
    if (SPI_execute(sql, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
        ereport(ERROR, (errmsg("DTX coordinator shard has no current leader")));
    strlcpy(host, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1), NAMEDATALEN);
    *port = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
    if (isnull || *port <= 0)
        elog(ERROR, "DTX coordinator has no port");
    PopActiveSnapshot();
    SPI_finish();
    pfree(sql);
}

static void
enlist_business(void)
{
    StringInfoData q;
    char *result;
    char self_host[NAMEDATALEN];
    int self_port;
    int i;

    if (!branch_gid[0]) return;
    if (!coordinator)
    {
        char *sql;
        if (!branch_nshards)
            sql = psprintf("SELECT coord_gsid FROM partdist.dtx_route WHERE gxid=" INT64_FORMAT,
                           branch_xid);
        else
            sql = psprintf("SELECT partdist.dtx_route_claim(" INT64_FORMAT "," INT64_FORMAT ")",
                           branch_xid, branch_shards[0]);
        result = tso_control(sql);
        coordinator = pg_strtoint64(result);
        pfree(result);
        pfree(sql);
        TsoSetCoordinator(coordinator);
        snprintf(branch_gid, sizeof(branch_gid), "shardpg_fc_" INT64_FORMAT "_" INT64_FORMAT "_" UINT64_FORMAT,
                 branch_xid, coordinator, branch_connection);
        coordinator_address(coordinator, coord_host, &coord_port);
    }
    /* A branch can cover several shards on one worker. It contributes one vote,
     * but records all its shard IDs; clog pairs remain local to each shard. */
    {
        const char *node = GetConfigOption("pg_raft.node_id", false, false);
        char *sql = psprintf("SELECT hostname,port FROM partdist.node_map WHERE node_id=%d", atoi(node));
        bool isnull;
        SPI_connect();
        PushActiveSnapshot(GetTransactionSnapshot());
        if (SPI_execute(sql, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
            elog(ERROR, "DTX participant has no node address");
        strlcpy(self_host, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1), NAMEDATALEN);
        self_port = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
        if (isnull) elog(ERROR, "DTX participant has no port");
        PopActiveSnapshot();
        SPI_finish();
        pfree(sql);
    }
    initStringInfo(&q);
    appendStringInfo(&q, "SELECT partdist.dtx_enlist(" INT64_FORMAT "," INT64_FORMAT ",%s,%s,%d,ARRAY[",
                     branch_xid, coordinator, quote_literal_cstr(branch_gid),
                     quote_literal_cstr(self_host), self_port);
    for (i = 0; i < branch_nshards; i++)
        appendStringInfo(&q, "%s" INT64_FORMAT, i ? "," : "", branch_shards[i]);
    appendStringInfoString(&q, "]::bigint[])");
    result = rpc(coord_host, coord_port, NULL, q.data);
    pfree(result);
    pfree(q.data);
}


static void
enlist(void)
{
    StringInfoData q;
    char *result;
    char self_host[NAMEDATALEN];
    int self_port;
    int i;
    const char *node;
    char *sql;
    bool isnull;

    if (!branch_is_ddl) { enlist_business(); return; }
    if (!branch_gid[0] || !branch_nshards) return;
    node = GetConfigOption("pg_raft.node_id", false, false);
    sql = psprintf("SELECT hostname,port FROM partdist.node_map WHERE node_id=%d", atoi(node));
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    if (SPI_execute(sql, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
        elog(ERROR, "DTX participant has no node address");
    strlcpy(self_host, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1), NAMEDATALEN);
    self_port = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
    if (isnull) elog(ERROR, "DTX participant has no port");
    PopActiveSnapshot();
    SPI_finish();
    pfree(sql);

    /* The route is published only AFTER the candidate has admitted this
     * participant. A failed admission must never win the first-arrival race.
     * The TSO serializes publication; the chosen shard owns participants/votes. */
    initStringInfo(&q);
    appendStringInfo(&q, "SELECT partdist.dtx_register_first_native(" INT64_FORMAT "," INT64_FORMAT "," UINT64_FORMAT ",%s,%d,ARRAY[",
                     branch_xid, branch_shards[0], branch_connection,
                     quote_literal_cstr(self_host), self_port);
    for (i = 0; i < branch_nshards; i++)
        appendStringInfo(&q, "%s" INT64_FORMAT, i ? "," : "", branch_shards[i]);
    appendStringInfo(&q, "]::bigint[]," UINT64_FORMAT ")", branch_native_fullxid);
    result = tso_control(q.data);
    coordinator = pg_strtoint64(result);
    pfree(result);
    pfree(q.data);
    if (coordinator <= 0) elog(ERROR, "DTX registration returned no coordinator");
    TsoSetCoordinator(coordinator);
    snprintf(branch_gid, sizeof(branch_gid), "shardpg_fc_" INT64_FORMAT "_" INT64_FORMAT "_" UINT64_FORMAT,
             branch_xid, coordinator, branch_connection);
    coordinator_address(coordinator, coord_host, &coord_port);
}

static void
enlist_relation(Oid oid)
{
    char *sql;
    bool isnull;
    int64 gsid = 0;
    int i;
    if (!OidIsValid(oid)) return;
    sql = psprintf("SELECT global_shard_id FROM partdist.shard_identity WHERE local_oid=%u::oid", oid);
    SPI_connect();
    if (SPI_execute(sql, true, 1) == SPI_OK_SELECT && SPI_processed == 1)
    {
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull) gsid = DatumGetInt64(d);
    }
    SPI_finish();
    pfree(sql);
    if (gsid <= 0) return;
    for (i = 0; i < branch_nshards; i++)
        if (branch_shards[i] == gsid) return;
    if (branch_nshards == DTX_PENDING_MAX_PAIRS)
        elog(ERROR, "DTX participant shard limit exceeded");
    branch_partitions[branch_nshards] = oid;
    branch_shards[branch_nshards++] = gsid;
    enlist();
}

void
DtxEnlistPlan(PlannedStmt *plan)
{
    ListCell *lc;
    if (!branch_gid[0] && router_transaction_ddl && router_ddl_depth == 0 && plan != NULL &&
        (plan->commandType == CMD_INSERT || plan->commandType == CMD_UPDATE ||
         plan->commandType == CMD_DELETE || plan->commandType == CMD_MERGE))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("business writes cannot join a distributed DDL transaction"),
                       errhint("Commit or roll back the DDL transaction before writing business rows.")));
    if (!branch_gid[0] || branch_is_ddl || inside_enlist || plan == NULL) return;
    inside_enlist = true;
    PG_TRY();
    {
        foreach(lc, plan->rtable)
        {
            RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
            if (rte->rtekind == RTE_RELATION) enlist_relation(rte->relid);
        }
    }
    PG_FINALLY();
    { inside_enlist = false; }
    PG_END_TRY();
}

int
DtxBranchPartitions(const Oid **partitions)
{
    *partitions = branch_partitions;
    /* DDL votes protect native catalog PREPARE; the anchor replicates the
     * coordinator decision, not a synthetic business-row PREPARE marker. */
    return branch_gid[0] && !branch_is_ddl ? branch_nshards : 0;
}

PG_FUNCTION_INFO_V1(partdist_dtx_branch_begin);
Datum
partdist_dtx_branch_begin(PG_FUNCTION_ARGS)
{
    char *gid = text_to_cstring(PG_GETARG_TEXT_PP(0));
    int64 id;
    unsigned long long connection;
    long long ignored_id, ignored_coord;
    if (branch_gid[0] || !DtxidFromGid(gid, &id) || id != TsoCurrentGxid() ||
        strlen(gid) >= sizeof(branch_gid) ||
        sscanf(gid, "shardpg_fc_%lld_%lld_%llu", &ignored_id, &ignored_coord, &connection) != 3)
        elog(ERROR, "invalid or duplicate DTX branch context");
    strlcpy(branch_gid, gid, sizeof(branch_gid));
    branch_xid = id;
    branch_connection = (uint64) connection;
    branch_native_fullxid = U64FromFullTransactionId(GetTopFullTransactionId());
    PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(partdist_dtx_branch_set_ddl);
Datum
partdist_dtx_branch_set_ddl(PG_FUNCTION_ARGS)
{
    const char *node = GetConfigOption("pg_raft.node_id", false, false);
    char *q;
    bool isnull;
    if (!branch_gid[0] || branch_nshards || coordinator)
        elog(ERROR, "DDL context must precede participant registration");
    q = psprintf("SELECT group_id FROM partdist.dtx_ddl_shard WHERE owner_node=%d AND anchor_oid IS NOT NULL", atoi(node));
    SPI_connect();
    if (SPI_execute(q, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
        elog(ERROR, "DDL metadata shard is not initialized on this worker");
    branch_shards[0] = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    if (isnull || branch_shards[0] <= 0) elog(ERROR, "invalid DDL metadata shard");
    SPI_finish();
    pfree(q);
    branch_nshards = 1;
    branch_is_ddl = true;
    inside_enlist = true;
    PG_TRY(); { enlist(); }
    PG_FINALLY(); { inside_enlist = false; }
    PG_END_TRY();
    PG_RETURN_VOID();
}

/* DDL context is lexical, surviving shardpg_create's internal transaction
 * boundaries without changing a session GUC or affecting unrelated sessions. */
bool
DtxUtilityEnter(Node *stmt)
{
    int node_id;
    int router_id;
    Oid ns;
    char *r;
    if (!shardpg_first_shard_coordinator || branch_gid[0] || creating_extension ||
        stmt == NULL || !(IsA(stmt, CreateStmt) || IsA(stmt, DropStmt) ||
                          IsA(stmt, CreateSchemaStmt) || IsA(stmt, IndexStmt)))
        return false;
    node_id = atoi(GetConfigOption("pg_raft.node_id", false, false));
    router_id = atoi(GetConfigOption("pg_raft.coordinator_node_id", false, false));
    if (node_id != router_id) return false;
    /* Extension upgrade scripts create their own local service tables. They
     * must not recursively bootstrap a service whose SQL is being installed. */
    if (IsA(stmt, CreateStmt))
    {
        const char *schema = ((CreateStmt *) stmt)->relation->schemaname;
        if (schema && strcmp(schema, "partdist") == 0) return false;
    }
    if (router_started && !router_transaction_ddl)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("distributed DDL cannot join an existing business transaction"),
                       errhint("Commit or roll back the business transaction before running DDL.")));
    ns = get_namespace_oid("partdist", true);
    if (!OidIsValid(ns) || !OidIsValid(get_relname_relid("dtx_ddl_shard", ns)))
        return false; /* extension installation itself has no DDL service yet */
    if (router_ddl_depth == 0)
    {
        r = rpc("127.0.0.1", PostPortNumber, NULL, "SELECT partdist.dtx_ddl_ensure()");
        if (strcmp(r, "t") != 0) elog(ERROR, "DDL metadata shards are not ready");
        pfree(r);
    }
    router_ddl_depth++;
    return true;
}

void
DtxUtilityLeave(void)
{
    Assert(router_ddl_depth > 0);
    router_ddl_depth--;
}

PG_FUNCTION_INFO_V1(partdist_dtx_branch_ready);
Datum
partdist_dtx_branch_ready(PG_FUNCTION_ARGS)
{
    int n;
    Oid *oids;
    int i;
    if (!branch_gid[0]) elog(ERROR, "missing DTX branch context");
    inside_enlist = true;
    PG_TRY();
    {
        n = branch_is_ddl ? 0 : PartWALTouchedCount();
        oids = palloc(sizeof(Oid) * Max(n, 1));
        if (!branch_is_ddl) n = PartWALCopyTouched(oids, n);
        for (i = 0; i < n; i++) enlist_relation(oids[i]);
        pfree(oids);
        if (!branch_nshards)
            elog(ERROR, "first-shard coordination requires a managed shard on every branch");
        enlist();
    }
    PG_FINALLY();
    { inside_enlist = false; }
    PG_END_TRY();
    PG_RETURN_BOOL(true);
}

/* The router only parks the socket. PREPARE is a command read from the shard
 * coordinator, and must be consumed by the backend owning the open transaction.
 */
bool
DtxHandoffUtility(Node *stmt, ProcessUtilityContext context, QueryCompletion *qc)
{
    CallStmt *call;
    TimestampTz started;
    char *sql;
    if (IsA(stmt, TransactionStmt) &&
        ((TransactionStmt *) stmt)->kind == TRANS_STMT_COMMIT &&
        router_transaction_ddl && router_started)
    {
        if (context != PROCESS_UTILITY_TOPLEVEL)
            elog(ERROR, "distributed DDL COMMIT requires a top-level command");
        if (DtxCommitDdlCatalog(CurrentMemoryContext))
        {
            if (qc) SetQueryCompletion(qc, CMDTAG_COMMIT, 0);
            return true;
        }
    }
    if (!IsA(stmt, CallStmt)) return false;
    call = (CallStmt *) stmt;
    if (!call->funcexpr ||
        get_func_namespace(call->funcexpr->funcid) != get_namespace_oid("partdist", true) ||
        strcmp(get_func_name(call->funcexpr->funcid), "dtx_handoff") != 0)
        return false;
    if (context != PROCESS_UTILITY_TOPLEVEL || !branch_gid[0] || !coordinator)
        elog(ERROR, "DTX handoff requires a registered top-level branch");
    sql = psprintf("SELECT partdist.dtx_branch_command(" INT64_FORMAT ",%s)",
                   branch_xid, quote_literal_cstr(branch_gid));
    started = GetCurrentTimestamp();
    for (;;)
    {
        char *command = rpc(coord_host, coord_port, NULL, sql);
        bool prepare = strcmp(command, "PREPARE") == 0;
        bool abort = strcmp(command, "ABORT") == 0;
        pfree(command);
        if (abort) elog(ERROR, "DTX coordinator aborted the transaction");
        if (prepare) break;
        if (TimestampDifferenceExceeds(started, GetCurrentTimestamp(), prepare_timeout_ms))
            elog(ERROR, "DTX coordinator prepare command timed out");
        CHECK_FOR_INTERRUPTS();
        pg_usleep(10000L);
    }
    pfree(sql);
    handing_off = true;
    (void) GetTopTransactionId();
    PartDistDtxNotePrepareGid(branch_gid);
    if (!PrepareTransactionBlock(branch_gid))
        elog(ERROR, "DTX branch could not enter prepare");
    if (qc) SetQueryCompletion(qc, CMDTAG_PREPARE_TRANSACTION, 0);
    return true;
}

static void
branch_callback(XactEvent event, void *arg)
{
    MemoryContext saved = CurrentMemoryContext;
    if (event != XACT_EVENT_PREPARE && event != XACT_EVENT_ABORT &&
        event != XACT_EVENT_COMMIT) return;
    if (branch_gid[0] && coordinator &&
        (event == XACT_EVENT_ABORT || (event == XACT_EVENT_PREPARE && handing_off)))
    {
        /* This callback cannot throw after EndPrepare. Lost votes cause timeout
         * and recovery, never a false positive vote at the coordinator. */
        PG_TRY();
        {
            char *q = psprintf("SELECT partdist.dtx_vote(" INT64_FORMAT ",%s,%s)",
                               branch_xid, quote_literal_cstr(branch_gid),
                               event == XACT_EVENT_PREPARE ? "true" : "false");
            char *r = rpc(coord_host, coord_port, NULL, q);
            pfree(r);
            pfree(q);
        }
        PG_CATCH();
        { MemoryContextSwitchTo(saved); FlushErrorState(); }
        PG_END_TRY();
    }
    branch_gid[0] = '\0';
    branch_xid = coordinator = 0;
    branch_nshards = 0;
    handing_off = inside_enlist = router_started = branch_is_ddl = router_transaction_ddl = false;
    branch_native_fullxid = 0;
}

void
DtxRequireRouterHooks(void)
{
    if (shardpg_first_shard_coordinator && !hooks_installed)
        ereport(ERROR, (errmsg("first-shard coordination requires Citus patch citus-0004")));
}

static void
router_begin(MultiConnection *connection, StringInfo command)
{
    int64 gxid = TsoCurrentGxid();
    if (!shardpg_first_shard_coordinator || MyBackendType != B_BACKEND) return;
    /* Citus creates shared dependencies (schemas/roles) in independent
     * transactions before starting the coordinated table transaction. Those
     * sockets are committed and closed by Citus itself. Enrolling them in the
     * table transaction would leave votes that can never arrive. */
    if (router_ddl_depth > 0 && !InCoordinatedTransaction()) return;
    if (gxid <= 0)
        elog(ERROR, "router must allocate gxid and start_ts before opening a shard branch");
    snprintf(connection->remoteTransaction.preparedName, NAMEDATALEN,
             "shardpg_fc_" INT64_FORMAT "_0_" UINT64_FORMAT, gxid, connection->connectionId);
    appendStringInfo(command, "SELECT partdist.dtx_branch_begin(%s);",
                     quote_literal_cstr(connection->remoteTransaction.preparedName));
    if (router_ddl_depth > 0)
    {
        appendStringInfoString(command, "SELECT partdist.dtx_branch_set_ddl();");
        router_transaction_ddl = true;
    }
    router_started = true;
}

static void
router_ready_branches(void)
{
    dlist_iter it;
    /* Quiesce task sockets and finish enrolment, including COPY/empty branches.
     * These are execution acknowledgements, not PREPARE votes. */
    dlist_foreach(it, &InProgressTransactions)
    {
        MultiConnection *c = dlist_container(MultiConnection, transactionNode, it.cur);
        PGresult *r;
        if (c->remoteTransaction.transactionFailed)
            elog(ERROR, "a distributed task failed before handoff");
        r = PQexec(c->pgConn, "SELECT partdist.dtx_branch_ready()");
        if (!r || PQresultStatus(r) != PGRES_TUPLES_OK)
        {
            char *detail = pstrdup(r ? PQresultErrorMessage(r) : PQerrorMessage(c->pgConn));
            if (r) PQclear(r);
            ereport(ERROR, (errmsg("a distributed branch could not register with its coordinator"),
                            errdetail_internal("%s", detail)));
        }
        PQclear(r);
    }
}

static bool
router_commit(void)
{
    dlist_iter it;
    char host[NAMEDATALEN];
    int port;
    char *sql;
    char *result;
    int64 gsid;
    if (!router_started) return false;
    router_ready_branches();
    sql = psprintf("SELECT coord_gsid FROM partdist.dtx_route WHERE gxid=" INT64_FORMAT,
                   TsoCurrentGxid());
    result = tso_control(sql);
    gsid = pg_strtoint64(result);
    pfree(result);
    pfree(sql);
    coordinator_address(gsid, host, &port);

    PG_TRY();
    {
        dlist_foreach(it, &InProgressTransactions)
        {
            MultiConnection *c = dlist_container(MultiConnection, transactionNode, it.cur);
            /* From this point only the shard coordinator may decide the branch.
             * Citus's abort path must not roll it back after an ambiguous reply. */
            c->remoteTransaction.transactionState = REMOTE_TRANS_COMMITTED;
            if (!SendRemoteCommand(c, "CALL partdist.dtx_handoff()"))
                elog(ERROR, "DTX branch handoff failed");
            /* Citus sockets are nonblocking. Flush bytes, without waiting for
             * any response (the coordinator owns the vote barrier). */
            {
                TimestampTz since = GetCurrentTimestamp();
                int status;
                while ((status = PQflush(c->pgConn)) == 1)
                {
                    CHECK_FOR_INTERRUPTS();
                    if (TimestampDifferenceExceeds(since, GetCurrentTimestamp(), prepare_timeout_ms))
                        elog(ERROR, "DTX handoff send timed out");
                    (void) WaitLatchOrSocket(MyLatch,
                        WL_LATCH_SET | WL_SOCKET_WRITEABLE | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                        PQsocket(c->pgConn), 100L, PG_WAIT_EXTENSION);
                    ResetLatch(MyLatch);
                }
                if (status < 0) elog(ERROR, "DTX handoff socket failed");
            }
        }
        sql = psprintf("SELECT partdist.dtx_coordinate(" INT64_FORMAT ")", TsoCurrentGxid());
        result = rpc(host, port, NULL, sql);
        if (pg_strtoint64(result) <= 0) elog(ERROR, "DTX coordinator returned no commit timestamp");
        pfree(result);
        pfree(sql);
    }
    PG_FINALLY();
    {
        /* Do not wait for phase-two acknowledgements or reuse parked sockets. */
        dlist_foreach(it, &InProgressTransactions)
        {
            MultiConnection *c = dlist_container(MultiConnection, transactionNode, it.cur);
            if (c->remoteTransaction.transactionState == REMOTE_TRANS_COMMITTED && c->pgConn)
                ShutdownConnection(c);
        }
    }
    PG_END_TRY();
    return true;
}

/* Commit the router catalog as a genuine native PREPARED participant.
 * This runs at shardpg_create/drop's explicit transaction boundary, or when
 * intercepting a user COMMIT for a DDL-only transaction. */
bool
DtxCommitDdlCatalog(MemoryContext back_to)
{
    int64 id, coord;
    char host[NAMEDATALEN];
    int port;
    char gid[NAMEDATALEN];
    char self_host[NAMEDATALEN];
    int self_port;
    List *connections = NIL;
    ListCell *lc;
    dlist_iter it;
    char *q, *answer;
    TimestampTz started;
    bool isnull;

    if (!router_started || !router_transaction_ddl) return false;
    if (prepare_local_catalog == NULL)
        elog(ERROR, "distributed DDL requires Citus patch citus-0005-ddl-catalog-participant");
    MemoryContextSwitchTo(back_to);
    id = TsoCurrentGxid();
    router_ready_branches();
    q = psprintf("SELECT coord_gsid FROM partdist.dtx_route WHERE gxid=" INT64_FORMAT, id);
    answer = tso_control(q);
    coord = pg_strtoint64(answer);
    pfree(q); pfree(answer);
    coordinator_address(coord, host, &port);
    SPI_connect();
    if (SPI_execute("SELECT hostname,port FROM partdist.node_map WHERE node_id=current_setting('pg_raft.node_id')::integer", true, 1) != SPI_OK_SELECT || SPI_processed != 1)
        elog(ERROR, "DDL router has no participant address");
    strlcpy(self_host, SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1), NAMEDATALEN);
    self_port = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
    if (isnull) elog(ERROR, "DDL router has no participant port");
    SPI_finish();
    MemoryContextSwitchTo(back_to);
    snprintf(gid, sizeof(gid), "shardpg_fc_" INT64_FORMAT "_" INT64_FORMAT "_" INT64_FORMAT,
             id, coord, PG_INT64_MAX);
    q = psprintf("SELECT partdist.dtx_enlist_native(" INT64_FORMAT "," INT64_FORMAT ",%s,%s,%d,ARRAY[" INT64_FORMAT "]::bigint[]," UINT64_FORMAT ")",
                 id, coord, quote_literal_cstr(gid), quote_literal_cstr(self_host), self_port, coord,
                 U64FromFullTransactionId(GetTopFullTransactionId()));
    answer = rpc(host, port, NULL, q);
    if (strcmp(answer, "t") != 0) elog(ERROR, "DDL catalog participant admission failed");
    pfree(q); pfree(answer);
    q = psprintf("SELECT partdist.dtx_seal(" INT64_FORMAT ")", id);
    answer = rpc(host, port, NULL, q); pfree(q); pfree(answer);
    dlist_foreach(it, &InProgressTransactions)
    {
        MultiConnection *c = dlist_container(MultiConnection, transactionNode, it.cur);
        int flushed;
        connections = lappend(connections, c);
        c->remoteTransaction.transactionState = REMOTE_TRANS_COMMITTED;
        if (!SendRemoteCommand(c, "CALL partdist.dtx_handoff()"))
            elog(ERROR, "DDL branch handoff failed");
        started = GetCurrentTimestamp();
        while ((flushed = PQflush(c->pgConn)) == 1)
        {
            CHECK_FOR_INTERRUPTS();
            if (TimestampDifferenceExceeds(started, GetCurrentTimestamp(), prepare_timeout_ms))
                elog(ERROR, "DDL branch handoff send timed out");
            pg_usleep(1000L);
        }
        if (flushed < 0) elog(ERROR, "DDL branch handoff socket failed");
    }
    prepare_local_catalog();
    strlcpy(branch_gid, gid, sizeof(branch_gid));
    branch_xid = id; coordinator = coord;
    strlcpy(coord_host, host, sizeof(coord_host)); coord_port = port;
    branch_connection = (uint64) PG_INT64_MAX;
    branch_native_fullxid = U64FromFullTransactionId(GetTopFullTransactionId());
    branch_is_ddl = handing_off = true;
    branch_nshards = 1; branch_shards[0] = coord;
    TsoSetCoordinator(coord);
    PartDistDtxNotePrepareGid(gid);
    while (ActiveSnapshotSet()) PopActiveSnapshot();
    if (!IsTransactionBlock())
    {
        BeginTransactionBlock();
        CommitTransactionCommand(); /* enter block, without committing work */
    }
    if (!PrepareTransactionBlock(gid)) elog(ERROR, "DDL catalog could not enter native PREPARE");
    CommitTransactionCommand();
    StartTransactionCommand();
    MemoryContextSwitchTo(back_to);
    q = psprintf("SELECT partdist.dtx_ddl_finish(" INT64_FORMAT "," INT64_FORMAT ")", coord, id);
    answer = rpc(host, port, NULL, q);
    if (strcmp(answer, "2") == 0)
        ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                       errmsg("distributed DDL was aborted by its shard coordinator")));
    if (strcmp(answer, "1") != 0) elog(ERROR, "DDL coordinator returned no durable COMMIT");
    pfree(q); pfree(answer);
    started = GetCurrentTimestamp();
    for (;;)
    {
        q = psprintf("SELECT NOT EXISTS(SELECT FROM partdist.dtx_route WHERE gxid=" INT64_FORMAT ")", id);
        answer = tso_control(q); pfree(q);
        if (strcmp(answer, "t") == 0) { pfree(answer); break; }
        pfree(answer);
        CHECK_FOR_INTERRUPTS();
        if (TimestampDifferenceExceeds(started, GetCurrentTimestamp(), prepare_timeout_ms))
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("DDL COMMIT is durable but phase two completion is not yet confirmed"),
                errdetail("Retain transaction " INT64_FORMAT " and inspect its durable decision; do not replay the DDL.", id)));
        pg_usleep(10000L);
    }
    foreach(lc, connections) CloseConnection((MultiConnection *) lfirst(lc));
    return true;
}

/* SQL control-plane helpers are restricted to the extension's administrator. */
/* A native xid stays in ProcArray while active OR prepared. Test that
 * before inspecting prepared state so the active->prepared transition can
 * never be mistaken for an abort. Full xid age fences recycled native IDs. */
PG_FUNCTION_INFO_V1(partdist_dtx_branch_probe);
Datum
partdist_dtx_branch_probe(PG_FUNCTION_ARGS)
{
    char *gid = text_to_cstring(PG_GETARG_TEXT_PP(0));
    uint64 full = (uint64) PG_GETARG_INT64(1);
    uint64 next = U64FromFullTransactionId(ReadNextFullTransactionId());
    TransactionId xid = (TransactionId) full;
    char *q;
    bool prepared = false;
    bool isnull;
    if (full == 0 || full >= next || next-full >= UINT64CONST(0x80000000))
        PG_RETURN_TEXT_P(cstring_to_text("UNKNOWN"));
    if (!TransactionIdIsInProgress(xid))
        PG_RETURN_TEXT_P(cstring_to_text(TransactionIdDidCommit(xid) ? "COMMITTED" : "ABORTED"));
    q = psprintf("SELECT EXISTS(SELECT FROM pg_prepared_xacts WHERE gid=%s AND database=current_database() AND transaction::text::bigint=%u)",
                 quote_literal_cstr(gid), xid);
    SPI_connect();
    if (SPI_execute(q, true, 1) == SPI_OK_SELECT && SPI_processed == 1)
        prepared = DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
    SPI_finish(); pfree(q);
    PG_RETURN_TEXT_P(cstring_to_text(prepared ? "PREPARED" : "ACTIVE"));
}

PG_FUNCTION_INFO_V1(partdist_dtx_rpc);
Datum partdist_dtx_rpc(PG_FUNCTION_ARGS)
{
    if (!superuser()) elog(ERROR, "DTX RPC requires superuser");
    PG_RETURN_TEXT_P(cstring_to_text(rpc(text_to_cstring(PG_GETARG_TEXT_PP(0)),
        PG_GETARG_INT32(1), NULL, text_to_cstring(PG_GETARG_TEXT_PP(2)))));
}
PG_FUNCTION_INFO_V1(partdist_dtx_tso_rpc);
Datum partdist_dtx_tso_rpc(PG_FUNCTION_ARGS)
{
    if (!superuser()) elog(ERROR, "DTX TSO RPC requires superuser");
    PG_RETURN_TEXT_P(cstring_to_text(tso_control(text_to_cstring(PG_GETARG_TEXT_PP(0)))));
}
PG_FUNCTION_INFO_V1(partdist_dtx_self);
Datum partdist_dtx_self(PG_FUNCTION_ARGS)
{
    if (!superuser()) elog(ERROR, "DTX self RPC requires superuser");
    PG_RETURN_TEXT_P(cstring_to_text(rpc("127.0.0.1", PostPortNumber, NULL,
        text_to_cstring(PG_GETARG_TEXT_PP(0)))));
}
PG_FUNCTION_INFO_V1(partdist_dtx_commit_ts);
Datum partdist_dtx_commit_ts(PG_FUNCTION_ARGS)
{
    int64 ts = PartDistTsoDtxDecisionTs();
    if (ts <= 0) elog(ERROR, "DTX requires a TSO commit timestamp");
    PG_RETURN_INT64(ts);
}
PG_FUNCTION_INFO_V1(partdist_dtx_pending_exists);
Datum partdist_dtx_pending_exists(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DtxPendingContainsDtxid(PG_GETARG_INT64(0)));
}

void
DtxScheduleDecision(int64 dtxid, int verdict, int64 commit_ts)
{
    /* DECISION is already durable in the coordinator Raft group. Polling it
     * survives process/node failure; never do participant RPCs on this path. */
}

/* Close every branch stream, including read-only participants. No new TSO
 * timestamp is allocated here. Shard clog slots were already finalized by
 * dtx_apply_decision; these markers close the native parent/subtransaction tree
 * and the DTX in-doubt records using the SAME authoritative commit timestamp. */
void
DtxFinishPrepared(const char *gid, bool committed)
{
    long long xid64, coord64;
    unsigned long long branch;
    char host[NAMEDATALEN];
    int port;
    char *q, *result, *marker;
    int64 cts;
    TransactionId native_xid;
    uint32 len;
    Oid parts[DTX_PENDING_MAX_PAIRS];
    int n, i;
    bool isnull;

    if (sscanf(gid, "shardpg_fc_%lld_%lld_%llu", &xid64, &coord64, &branch) != 3)
        elog(ERROR, "invalid first-shard prepared identifier");
    if (DtxPendingContainsDtxid(xid64))
        elog(ERROR, "DTX verdict has not reached all local shard quorums");
    coordinator_address(coord64, host, &port);
    q = psprintf("SELECT commit_ts FROM partdist.dtx_decision WHERE dtxid=%lld AND verdict=%d",
                 xid64, committed ? 1 : 2);
    result = rpc(host, port, NULL, q);
    cts = pg_strtoint64(result);
    pfree(q);
    pfree(result);
    if (committed && cts <= 0) elog(ERROR, "DTX commit has no authoritative timestamp");
    SPI_connect();
    PushActiveSnapshot(GetTransactionSnapshot());
    q = psprintf("SELECT transaction::text::bigint FROM pg_prepared_xacts WHERE gid=%s "
                 "AND database=current_database()", quote_literal_cstr(gid));
    if (SPI_execute(q, true, 1) != SPI_OK_SELECT || SPI_processed != 1)
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("prepared transaction no longer exists")));
    native_xid = (TransactionId) DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                             SPI_tuptable->tupdesc, 1, &isnull));
    pfree(q);
    q = psprintf("SELECT si.local_oid FROM partdist.dtx_participant p "
                 "CROSS JOIN LATERAL unnest(p.gsids) g "
                 "JOIN partdist.shard_identity si ON si.global_shard_id=g "
                 "JOIN partdist.partition_map pm ON pm.partition_id=g "
                 "WHERE p.gid=%s AND pm.primary_node=current_setting('pg_raft.node_id')::integer",
                 quote_literal_cstr(gid));
    /* After ownership transfer, promotion closes in-doubt records before it
     * publishes the route. The fenced former leader must release its native
     * prepared transaction without proposing more data into the moved group. */
    if (SPI_execute(q, true, 0) != SPI_OK_SELECT ||
        SPI_processed > DTX_PENDING_MAX_PAIRS)
        elog(ERROR, "invalid DTX participant partition set");
    n = (int) SPI_processed;
    for (i = 0; i < n; i++)
        parts[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
                                                SPI_tuptable->tupdesc, 1, &isnull));
    pfree(q);
    PopActiveSnapshot();
    SPI_finish();
    marker = PartWALBuildVerdictMarker(0, committed ? cts : 0, &len);
    for (i = 0; i < n; i++)
    {
        AppendDtxRecord(parts[i], committed ? DTX_COMMIT : DTX_ABORT,
                        xid64, coord64, committed ? cts : 0, 0, NULL, 0, native_xid);
        PartWALAppendMarkerFor(parts[i], native_xid,
                               committed ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT, marker, len);
        PartWALNoteTouchedPartition(parts[i]);
    }
    pfree(marker);
    PartWALFlush(InvalidXLogRecPtr, false);
}

/* Separate from the TSO lease heartbeat: an unavailable participant must never
 * delay lease renewal. Recovery continues even if the original router exits. */
void
DtxDispatchWorkerMain(Datum arg)
{
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);
    for (;;)
    {
        CHECK_FOR_INTERRUPTS();
        if (ShutdownRequestPending) proc_exit(0);
        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }
        PG_TRY();
        {
            StartTransactionCommand();
            PushActiveSnapshot(GetTransactionSnapshot());
            SPI_connect();
            (void) SPI_execute("SELECT partdist.dtx_dispatch()", false, 0);
            SPI_finish();
            PopActiveSnapshot();
            CommitTransactionCommand();
        }
        PG_CATCH();
        {
            MemoryContextSwitchTo(TopMemoryContext);
            FlushErrorState();
            AbortCurrentTransaction();
        }
        PG_END_TRY();
        (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         250L, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
    }
}

void
DtxCoordinatorInit(void)
{
    BranchHook *begin = (BranchHook *) dlsym(RTLD_DEFAULT, "remote_transaction_branch_hook");
    DelegateHook *delegate = (DelegateHook *) dlsym(RTLD_DEFAULT, "remote_transaction_delegate_hook");
    BackgroundWorker worker;
    prepare_local_catalog = (void (*)(void)) dlsym(RTLD_DEFAULT, "ShardpgPrepareLocalCatalog");
    DefineCustomBoolVariable("shardpg.first_shard_coordinator",
        "First-arrival shard drives prepare and asynchronous commit.", NULL,
        &shardpg_first_shard_coordinator, true, PGC_SUSET, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("shardpg.prepare_timeout_ms", "Coordinator prepare timeout.", NULL,
        &prepare_timeout_ms, 30000, 100, 600000, PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL, NULL);
    if (begin && delegate)
    {
        *begin = router_begin;
        *delegate = router_commit;
        hooks_installed = true;
    }
    RegisterXactCallback(branch_callback, NULL);
    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    strlcpy(worker.bgw_library_name, "pg_partdist", BGW_MAXLEN);
    strlcpy(worker.bgw_function_name, "DtxDispatchWorkerMain", BGW_MAXLEN);
    strlcpy(worker.bgw_name, "pg_partdist asynchronous commit", BGW_MAXLEN);
    strlcpy(worker.bgw_type, "pg_partdist asynchronous commit", BGW_MAXLEN);
    RegisterBackgroundWorker(&worker);
}
