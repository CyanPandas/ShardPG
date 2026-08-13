#include "pg_raft.h"

#include "executor/spi.h"
#include "utils/guc.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "access/xact.h"

#include <ctype.h>

PG_MODULE_MAGIC;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

typedef struct NodeProbeEntry {
    int node_id;
    int fail_count;
} NodeProbeEntry;

static NodeProbeEntry probe_cache[64];
static int            probe_cache_n = 0;

static bool pg_raft_lookup_node_conninfo(int node_id, char *conninfo, size_t len);
static bool pg_raft_remote_fetch_applied_part_lsn(int node_id, Oid partition_id,
                                                  uint64 *applied_part_lsn);
static bool pg_raft_get_partition_switch_point(Oid partition_id,
                                               uint64 *switch_partition_lsn,
                                               char **switch_orig_lsn_text);
static int  pg_raft_parse_int_json_array(const char *json, int *out, int max_items);
static char *pg_raft_build_secondary_json(const int *candidates, int candidate_count,
                                          int chosen_primary);
static void pg_raft_append_json_string(StringInfo buf, const char *value);

static int
pg_raft_fail_threshold_guc(void)
{
    const char *raw = GetConfigOption("pg_raft.probe_fail_threshold", false, false);

    if (raw && raw[0])
        return atoi(raw);
    return pg_raft_probe_fail_threshold;
}

static void partdist_shmem_request(void);
static void partdist_shmem_startup(void);

void _PG_init(void);

PG_FUNCTION_INFO_V1(pg_raft_version);
PG_FUNCTION_INFO_V1(pg_raft_is_leader);
PG_FUNCTION_INFO_V1(pg_raft_get_leader);
PG_FUNCTION_INFO_V1(pg_raft_get_cluster_status);
PG_FUNCTION_INFO_V1(pg_raft_propose_node_status);
PG_FUNCTION_INFO_V1(pg_raft_propose_partition_primary);
PG_FUNCTION_INFO_V1(pg_raft_apply_payload);
PG_FUNCTION_INFO_V1(pg_raft_force_probe);
PG_FUNCTION_INFO_V1(pg_raft_report_data_leader);

void
pg_raft_format_conninfo(const char *hostname, int port, char *conninfo, size_t len)
{
    /*
     * Single-machine multi-port test topology: node-directory names
     * (coordinator/master, worker1..worker3) are not resolvable hostnames,
     * so map them to loopback.  The port comes from partdist.node_map;
     * fall back to the conventional 5432..5435 layout when absent.
     */
    const char *host = hostname;

    if (strcmp(hostname, "coordinator") == 0 || strcmp(hostname, "master") == 0)
    {
        host = "127.0.0.1";
        if (port <= 0)
            port = 5432;
    }
    else if (strcmp(hostname, "worker1") == 0)
    {
        host = "127.0.0.1";
        if (port <= 0)
            port = 5433;
    }
    else if (strcmp(hostname, "worker2") == 0)
    {
        host = "127.0.0.1";
        if (port <= 0)
            port = 5434;
    }
    else if (strcmp(hostname, "worker3") == 0)
    {
        host = "127.0.0.1";
        if (port <= 0)
            port = 5435;
    }

    snprintf(conninfo, len,
             "host=%s port=%d dbname=postgres user=postgres connect_timeout=1",
             host, port);
}

static NodeProbeEntry *
get_probe(int node_id)
{
    int i;
    for (i = 0; i < probe_cache_n; i++)
        if (probe_cache[i].node_id == node_id)
            return &probe_cache[i];
    if (probe_cache_n >= 64)
        return NULL;
    probe_cache[probe_cache_n].node_id = node_id;
    probe_cache[probe_cache_n].fail_count = 0;
    return &probe_cache[probe_cache_n++];
}

static bool
pg_raft_lookup_node_conninfo(int node_id, char *conninfo, size_t len)
{
    StringInfoData sql;
    int            ret;
    bool           isnull;
    bool           found = false;
    bool           spi_owned;
    bool           pushed_snap = false;
    char          *hostname = NULL;
    int            port = 0;

    if (!pg_raft_spi_begin(&spi_owned))
        return false;

    if (!spi_owned && !ActiveSnapshotSet())
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        pushed_snap = true;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT hostname, port "
                     "FROM partdist.node_map "
                     "WHERE node_id = %d",
                     node_id);
    ret = SPI_execute(sql.data, true, 1);
    pfree(sql.data);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        hostname = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
                                                     SPI_tuptable->tupdesc, 1, &isnull));
        port = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                           SPI_tuptable->tupdesc, 2, &isnull));
        pg_raft_format_conninfo(hostname, port, conninfo, len);
        pfree(hostname);
        found = true;
    }

    if (pushed_snap)
        PopActiveSnapshot();
    if (spi_owned)
        pg_raft_spi_end(spi_owned);
    return found;
}

static bool
pg_raft_remote_fetch_applied_part_lsn(int node_id, Oid partition_id,
                                      uint64 *applied_part_lsn)
{
    char        conninfo[512];
    char        sql[256];
    PGconn     *conn;
    PGresult   *res;
    bool        ok = false;

    *applied_part_lsn = 0;

    if (!pg_raft_lookup_node_conninfo(node_id, conninfo, sizeof(conninfo)))
        return false;

    snprintf(sql, sizeof(sql),
             "SELECT partdist.get_follower_applied_part_lsn(%u)",
             partition_id);

    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        PQfinish(conn);
        return false;
    }

    res = PQexec(conn, sql);
    if (res != NULL && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
    {
        char *val = PQgetvalue(res, 0, 0);

        if (val != NULL && val[0] != '\0')
        {
            *applied_part_lsn = (uint64) strtoull(val, NULL, 10);
            ok = true;
        }
    }

    if (res != NULL)
        PQclear(res);
    PQfinish(conn);
    return ok;
}

/*
 * 真实写路径(parwal-2.0 同步写)下,分区 parwal 与其 flush 进度位于承载该分片
 * 的节点(即旧 primary),而不是 Raft leader 本地。切换点优先远程读旧 primary;
 * 旧 primary 不可达(常见:宕机触发的 failover)时由调用方回退 leader 本地值。
 */
static bool
pg_raft_remote_fetch_partition_switch_point(int node_id, Oid partition_id,
                                            uint64 *flush_lsn,
                                            char **orig_lsn_text)
{
    char        conninfo[512];
    char        sql[512];
    PGconn     *conn;
    PGresult   *res;
    bool        ok = false;

    *flush_lsn = 0;
    *orig_lsn_text = NULL;

    if (!pg_raft_lookup_node_conninfo(node_id, conninfo, sizeof(conninfo)))
        return false;

    snprintf(sql, sizeof(sql),
             "SELECT COALESCE(partdist.get_partition_flush_lsn(%u), 0), "
             "       COALESCE((SELECT orig_node_lsn::text "
             "                 FROM partdist.read_all_headers(%u) "
             "                 ORDER BY partition_lsn DESC LIMIT 1), '0/0')",
             partition_id, partition_id);

    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        PQfinish(conn);
        return false;
    }

    res = PQexec(conn, sql);
    if (res != NULL && PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
    {
        char *lsn_val = PQgetvalue(res, 0, 0);
        char *orig_val = PQgetvalue(res, 0, 1);

        if (lsn_val != NULL && lsn_val[0] != '\0')
        {
            *flush_lsn = (uint64) strtoull(lsn_val, NULL, 10);
            *orig_lsn_text = pstrdup((orig_val != NULL && orig_val[0] != '\0')
                                     ? orig_val : "0/0");
            ok = true;
        }
    }

    if (res != NULL)
        PQclear(res);
    PQfinish(conn);
    return ok;
}

static bool
pg_raft_get_partition_switch_point(Oid partition_id,
                                   uint64 *switch_partition_lsn,
                                   char **switch_orig_lsn_text)
{
    StringInfoData sql;
    int            ret;
    bool           isnull;
    bool           spi_owned;
    bool           pushed_snap = false;
    bool           ok = false;
    MemoryContext  caller_ctx = CurrentMemoryContext;

    *switch_partition_lsn = 0;
    *switch_orig_lsn_text = pstrdup("0/0");

    if (!pg_raft_spi_begin(&spi_owned))
        return false;

    if (!spi_owned && !ActiveSnapshotSet())
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        pushed_snap = true;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT COALESCE(partdist.get_partition_flush_lsn(%u), 0), "
                     "       COALESCE(h.orig_node_lsn::text, '0/0') "
                     "FROM (SELECT 1) AS dummy "
                     "LEFT JOIN LATERAL ("
                     "    SELECT orig_node_lsn "
                     "    FROM partdist.read_all_headers(%u) "
                     "    ORDER BY partition_lsn DESC "
                     "    LIMIT 1"
                     ") h ON true",
                     partition_id,
                     partition_id);
    ret = SPI_execute(sql.data, true, 1);
    pfree(sql.data);

    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        char *orig_lsn_text;

        *switch_partition_lsn = (uint64) DatumGetInt64(
            SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull));
        orig_lsn_text = TextDatumGetCString(
            SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull));
        pfree(*switch_orig_lsn_text);
        {
            MemoryContext old_ctx = MemoryContextSwitchTo(caller_ctx);
            *switch_orig_lsn_text = pstrdup(orig_lsn_text);
            MemoryContextSwitchTo(old_ctx);
        }
        pfree(orig_lsn_text);
        ok = true;
    }

    if (pushed_snap)
        PopActiveSnapshot();
    if (spi_owned)
        pg_raft_spi_end(spi_owned);
    return ok;
}

static int
pg_raft_parse_int_json_array(const char *json, int *out, int max_items)
{
    const char *p = json;
    int         count = 0;

    while (*p != '\0' && count < max_items)
    {
        if (*p == '-' || isdigit((unsigned char) *p))
        {
            char *endptr = NULL;
            long  value = strtol(p, &endptr, 10);

            out[count++] = (int) value;
            p = endptr;
            continue;
        }
        p++;
    }

    return count;
}

static char *
pg_raft_build_secondary_json(const int *candidates, int candidate_count,
                             int chosen_primary)
{
    StringInfoData json;
    int            i;
    bool           first = true;

    initStringInfo(&json);
    appendStringInfoChar(&json, '[');
    for (i = 0; i < candidate_count; i++)
    {
        if (candidates[i] == chosen_primary)
            continue;
        if (!first)
            appendStringInfoChar(&json, ',');
        appendStringInfo(&json, "%d", candidates[i]);
        first = false;
    }
    appendStringInfoChar(&json, ']');
    return json.data;
}

static void
pg_raft_append_json_string(StringInfo buf, const char *value)
{
    escape_json(buf, value ? value : "");
}

int64
pg_raft_propose_node_status_internal(int node_id, const char *status)
{
    StringInfoData payload;
    StringInfoData sql;
    int64          log_id = 0;
    int            ret;
    bool           isnull;
    bool           spi_owned;

    if (!pg_raft_is_leader_local())
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: only leader may propose")));

    initStringInfo(&payload);
    appendStringInfo(&payload, "{\"node_id\": %d, \"status\": \"%s\"}",
                     node_id, status);

    if (pg_raft_raft_enabled)
    {
        int64 idx = pg_raft_consensus_propose(PG_RAFT_OP_NODE_STATUS, payload.data);
        pfree(payload.data);
        if (idx <= 0)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: raft propose failed (not leader, no quorum, or log full)")));
        return idx;
    }

    if (!pg_raft_spi_begin(&spi_owned))
        ereport(ERROR, (errmsg("pg_raft: SPI_connect failed")));

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO partdist.raft_log (term, op_type, payload, committed) "
                     "VALUES (%ld, %s, %s::jsonb, true) RETURNING log_id",
                     RaftLeaderShmemData ? RaftLeaderShmemData->current_term : 1,
                     quote_literal_cstr(PG_RAFT_OP_NODE_STATUS),
                     quote_literal_cstr(payload.data));

    ret = SPI_execute(sql.data, false, 0);
    if (ret == SPI_OK_INSERT && SPI_processed > 0)
    {
        log_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                             SPI_tuptable->tupdesc, 1, &isnull));
    }
    pfree(sql.data);

    {
        char *payload_copy = pstrdup(payload.data);

        pfree(payload.data);
        pg_raft_spi_end(spi_owned);
        pg_raft_apply_payload_sql(PG_RAFT_OP_NODE_STATUS, payload_copy);
        pg_raft_replicate_to_peers(PG_RAFT_OP_NODE_STATUS, payload_copy);
        pfree(payload_copy);
    }
    return log_id;
}

int64
pg_raft_propose_partition_primary_internal(Oid partition_id, int primary_node,
                                           const char *secondary_nodes_json,
                                           int old_primary_node,
                                           uint64 switch_partition_lsn,
                                           const char *switch_orig_lsn,
                                           int64 primary_term)
{
    StringInfoData payload;
    StringInfoData ins;
    int64          log_id = 0;
    int            ret;
    bool           isnull;
    char          *payload_copy;
    bool           spi_owned;

    if (!pg_raft_is_leader_local())
        return 0;

    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"partition_id\": %u, \"primary_node\": %d, "
                     "\"old_primary_node\": %d, \"secondary_nodes\": %s, "
                     "\"primary_term\": %lld, "
                     "\"switch_partition_lsn\": " UINT64_FORMAT ", "
                     "\"switch_orig_lsn\": ",
                     partition_id, primary_node, old_primary_node,
                     secondary_nodes_json, (long long) primary_term,
                     switch_partition_lsn);
    pg_raft_append_json_string(&payload, switch_orig_lsn ? switch_orig_lsn : "0/0");
    appendStringInfoChar(&payload, '}');

    if (pg_raft_raft_enabled)
    {
        int64 idx = pg_raft_consensus_propose(PG_RAFT_OP_PARTITION_PRIMARY, payload.data);
        pfree(payload.data);
        if (idx <= 0)
            ereport(ERROR,
                    (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                     errmsg("pg_raft: raft propose failed (not leader, no quorum, or log full)")));
        return idx;
    }

    if (!pg_raft_spi_begin(&spi_owned))
        return 0;

    initStringInfo(&ins);
    appendStringInfo(&ins,
                     "INSERT INTO partdist.raft_log (term, op_type, payload, committed) "
                     "VALUES (%ld, %s, %s::jsonb, true) RETURNING log_id",
                     (long) (RaftLeaderShmemData ? RaftLeaderShmemData->current_term : 1),
                     quote_literal_cstr(PG_RAFT_OP_PARTITION_PRIMARY),
                     quote_literal_cstr(payload.data));
    ret = SPI_execute(ins.data, false, 0);
    if (ret == SPI_OK_INSERT && SPI_processed > 0)
        log_id = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                             SPI_tuptable->tupdesc, 1, &isnull));
    pfree(ins.data);

    payload_copy = pstrdup(payload.data);
    pfree(payload.data);
    pg_raft_spi_end(spi_owned);

    pg_raft_apply_payload_sql(PG_RAFT_OP_PARTITION_PRIMARY, payload_copy);
    pg_raft_replicate_to_peers(PG_RAFT_OP_PARTITION_PRIMARY, payload_copy);
    pfree(payload_copy);
    return log_id;
}

void
pg_raft_topology_probe_and_failover(void)
{
    StringInfoData sql;
    int            ret;
    int            i;
    bool           isnull;
    bool           spi_owned;
    bool           pushed_snap = false;
    int            down_ids[64];
    int            down_n = 0;
    int            up_ids[64];
    int            up_n = 0;
    int            thresh;

    if (!pg_raft_is_leader_local())
    {
        elog(DEBUG1, "pg_raft: topology skip (not leader, node_id=%d)", pg_raft_node_id);
        return;
    }

    elog(LOG, "pg_raft DBG: is_leader=true, entering probe body");

    if (!pg_raft_spi_begin(&spi_owned))
    {
        elog(DEBUG1, "pg_raft: topology SPI begin failed");
        return;
    }

    elog(LOG, "pg_raft DBG: SPI begun (owned=%d), about to query node_map", spi_owned);

    if (!spi_owned && !ActiveSnapshotSet())
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        pushed_snap = true;
    }

    initStringInfo(&sql);
    appendStringInfoString(&sql,
                           "SELECT node_id, hostname, port, status FROM partdist.node_map");

    ret = SPI_execute(sql.data, true, 0);
    pfree(sql.data);
    if (ret != SPI_OK_SELECT)
    {
        elog(DEBUG1, "pg_raft: topology SPI select failed ret=%d", ret);
        if (pushed_snap)
            PopActiveSnapshot();
        pg_raft_spi_end(spi_owned);
        return;
    }

    thresh = pg_raft_fail_threshold_guc();

    elog(LOG, "pg_raft DBG: probe scanned %d nodes thresh=%d", (int) SPI_processed, thresh);

    for (i = 0; i < (int) SPI_processed; i++)
    {
        HeapTuple  tup = SPI_tuptable->vals[i];
        TupleDesc  desc = SPI_tuptable->tupdesc;
        int        nid;
        char      *host;
        int        port;
        char      *status;
        char       conninfo[256];
        PGconn    *conn;
        PGresult  *res;
        NodeProbeEntry *pe;

        nid = DatumGetInt32(SPI_getbinval(tup, desc, 1, &isnull));
        host = TextDatumGetCString(SPI_getbinval(tup, desc, 2, &isnull));
        port = DatumGetInt32(SPI_getbinval(tup, desc, 3, &isnull));
        status = TextDatumGetCString(SPI_getbinval(tup, desc, 4, &isnull));

        if (nid == pg_raft_node_id)
        {
            pfree(host);
            pfree(status);
            continue;
        }

        pg_raft_format_conninfo(host, port, conninfo, sizeof(conninfo));
        conn = PQconnectdb(conninfo);
        pe = get_probe(nid);

        if (PQstatus(conn) != CONNECTION_OK)
        {
            if (pe)
                pe->fail_count++;
            elog(DEBUG1, "pg_raft: node %d connect fail count=%d thresh=%d",
                 nid, pe ? pe->fail_count : -1, thresh);
            if (pe && pe->fail_count >= thresh && strcmp(status, "down") != 0 &&
                down_n < 64)
                down_ids[down_n++] = nid;
            PQfinish(conn);
            pfree(host);
            pfree(status);
            continue;
        }

        res = PQexec(conn, "SELECT 1");
        if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            if (pe)
                pe->fail_count++;
            if (pe && pe->fail_count >= thresh && strcmp(status, "down") != 0 &&
                down_n < 64)
                down_ids[down_n++] = nid;
        }
        else
        {
            if (pe)
                pe->fail_count = 0;
            if (strcmp(status, "down") == 0 && up_n < 64)
                up_ids[up_n++] = nid;
        }
        if (res != NULL)
            PQclear(res);
        PQfinish(conn);
        pfree(host);
        pfree(status);
    }

    if (pushed_snap)
        PopActiveSnapshot();

    pg_raft_spi_end(spi_owned);

    elog(LOG, "pg_raft DBG: probe loop done down=%d up=%d", down_n, up_n);

    /*
     * 勿在 SPI 扫描 node_map 的同一连接里用 SPI_execute 调 SQL 版 propose（会嵌套
     * SPI 并可能崩溃）。探测结束后直接走 internal + failover。
     */
    for (i = 0; i < down_n; i++)
    {
        elog(LOG, "pg_raft DBG: propose down node %d + failover", down_ids[i]);
        pg_raft_propose_node_status_internal(down_ids[i], "down");
        pg_raft_failover_partitions_for_node(down_ids[i]);
    }
    for (i = 0; i < up_n; i++)
    {
        elog(LOG, "pg_raft DBG: propose up node %d + rejoin secondaries", up_ids[i]);
        pg_raft_propose_node_status_internal(up_ids[i], "active");
        pg_raft_rejoin_partitions_for_node(up_ids[i]);
    }
    elog(LOG, "pg_raft DBG: probe complete");
}

void
pg_raft_failover_partitions_for_node(int down_node_id)
{
    StringInfoData sql;
    int            ret;
    bool           spi_owned;
    int            i;
    bool           isnull;
    Oid           *parts;
    int           *old_primaries;
    char         **sec_jsons;
    int            n;
    MemoryContext  caller_ctx = CurrentMemoryContext;

    ereport(DEBUG1, errmsg("pg_raft: failover partitions for node %d", down_node_id));

    if (!pg_raft_spi_begin(&spi_owned))
        return;

    /*
     * 切主重构后，凡由数据组自治选举管理的分区（primary_term > 0，即新任 leader
     * 已上报过）不再走这条"控制面指定新主"的旧通道——组内自会选出新 leader 并
     * 上报登记。这里只对尚无数据组的历史分区（term=0，测试夹具）保留旧行为。
     */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "WITH affected AS ("
                     "  SELECT partition_id, primary_node, secondary_nodes "
                     "  FROM partdist.partition_map "
                     "  WHERE primary_node = %d AND COALESCE(primary_term, 0) = 0"
                     ") "
                     "SELECT partition_id, primary_node, secondary_nodes::text "
                     "FROM affected "
                     "ORDER BY partition_id",
                     down_node_id);

    ret = SPI_execute(sql.data, true, 0);
    pfree(sql.data);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
    {
        if (spi_owned)
            pg_raft_spi_end(spi_owned);
        return;
    }

    {
        MemoryContext old_ctx = MemoryContextSwitchTo(caller_ctx);

        n = (int) SPI_processed;
        parts = (Oid *) palloc(sizeof(Oid) * n);
        old_primaries = (int *) palloc(sizeof(int) * n);
        sec_jsons = (char **) palloc(sizeof(char *) * n);

        for (i = 0; i < n; i++)
        {
            char *sec_text;

            parts[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
                                                      SPI_tuptable->tupdesc, 1, &isnull));
            old_primaries[i] = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                                           SPI_tuptable->tupdesc, 2, &isnull));
            sec_text = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
                                                         SPI_tuptable->tupdesc, 3, &isnull));
            sec_jsons[i] = pstrdup(sec_text);
            pfree(sec_text);
        }

        MemoryContextSwitchTo(old_ctx);
    }

    if (spi_owned)
    {
        pg_raft_spi_end(spi_owned);
        spi_owned = false;
    }

    for (i = 0; i < n; i++)
    {
        int     candidates[64];
        int     candidate_count;
        int     cidx;
        int     chosen_primary = 0;
        uint64  switch_partition_lsn = 0;
        char   *switch_orig_lsn = NULL;
        char   *new_secondary_json = NULL;

        uint64  chosen_applied = 0;

        candidate_count = pg_raft_parse_int_json_array(sec_jsons[i], candidates, lengthof(candidates));
        (void) pg_raft_get_partition_switch_point(parts[i],
                                                 &switch_partition_lsn,
                                                 &switch_orig_lsn);

        /* 切换点优先取旧 primary 上的真实 flush 进度,不可达则保留 leader 本地值 */
        {
            uint64  remote_flush = 0;
            char   *remote_orig = NULL;

            if (pg_raft_remote_fetch_partition_switch_point(old_primaries[i], parts[i],
                                                            &remote_flush, &remote_orig))
            {
                if (remote_flush > switch_partition_lsn)
                {
                    switch_partition_lsn = remote_flush;
                    pfree(switch_orig_lsn);
                    switch_orig_lsn = remote_orig;
                    remote_orig = NULL;
                    elog(LOG,
                         "pg_raft: using old primary node %d flush progress as switch point for partition %u (switch_partition_lsn=%llu)",
                         old_primaries[i], parts[i],
                         (unsigned long long) switch_partition_lsn);
                }
                if (remote_orig != NULL)
                    pfree(remote_orig);
            }
        }

        if (switch_partition_lsn == 0)
            elog(WARNING,
                 "pg_raft: no partition progress source for partition %u (old primary %d unreachable and no local parwal); promotion falls back to max applied_part_lsn",
                 parts[i], old_primaries[i]);

        /* 在所有追平切换点的候选中选 applied_part_lsn 最大者(并列取先列出者) */
        for (cidx = 0; cidx < candidate_count; cidx++)
        {
            uint64 applied_part_lsn = 0;

            if (candidates[cidx] == down_node_id)
                continue;
            if (!pg_raft_remote_fetch_applied_part_lsn(candidates[cidx], parts[i],
                                                       &applied_part_lsn))
            {
                elog(LOG,
                     "pg_raft: skip candidate node %d for partition %u because applied_part_lsn is unavailable",
                     candidates[cidx], parts[i]);
                continue;
            }
            if (applied_part_lsn < switch_partition_lsn)
            {
                elog(LOG,
                     "pg_raft: skip candidate node %d for partition %u because applied_part_lsn=%llu < switch_partition_lsn=%llu",
                     candidates[cidx], parts[i],
                     (unsigned long long) applied_part_lsn,
                     (unsigned long long) switch_partition_lsn);
                continue;
            }
            if (chosen_primary <= 0 || applied_part_lsn > chosen_applied)
            {
                chosen_primary = candidates[cidx];
                chosen_applied = applied_part_lsn;
            }
        }

        if (chosen_primary > 0)
            elog(LOG,
                 "pg_raft: partition %u chose most caught-up candidate node %d (applied_part_lsn=%llu, switch_partition_lsn=%llu)",
                 parts[i], chosen_primary,
                 (unsigned long long) chosen_applied,
                 (unsigned long long) switch_partition_lsn);

        if (chosen_primary <= 0)
        {
            elog(WARNING,
                 "pg_raft: no caught-up secondary available for partition %u after node %d down",
                 parts[i], down_node_id);
            pfree(switch_orig_lsn);
            pfree(sec_jsons[i]);
            continue;
        }

        new_secondary_json = pg_raft_build_secondary_json(candidates, candidate_count,
                                                          chosen_primary);

        elog(LOG,
             "pg_raft: failover partition %u from node %d to node %d at switch_partition_lsn=%llu switch_orig_lsn=%s",
             parts[i], down_node_id, chosen_primary,
             (unsigned long long) switch_partition_lsn,
             switch_orig_lsn ? switch_orig_lsn : "0/0");
        (void) pg_raft_propose_partition_primary_internal(parts[i], chosen_primary,
                                                          new_secondary_json,
                                                          old_primaries[i],
                                                          switch_partition_lsn,
                                                          switch_orig_lsn ? switch_orig_lsn : "0/0",
                                                          0);
        pfree(new_secondary_json);
        pfree(switch_orig_lsn);
        pfree(sec_jsons[i]);
    }
    pfree(parts);
    pfree(old_primaries);
    pfree(sec_jsons);

    if (spi_owned)
        pg_raft_spi_end(spi_owned);
}

/*
 * 节点恢复上线后，将其重新加入各分区的 secondary_nodes（不抢 primary）。
 * failover 时 down 节点会从 secondaries 中剔除；恢复后应作为从副本重新挂回。
 */
void
pg_raft_rejoin_partitions_for_node(int up_node_id)
{
    StringInfoData sql;
    int            ret;
    bool           spi_owned;
    int            i;
    bool           isnull;
    Oid           *parts;
    int           *primaries;
    char         **sec_jsons;
    int            n;
    MemoryContext  caller_ctx = CurrentMemoryContext;

    ereport(DEBUG1, errmsg("pg_raft: rejoin partitions for recovered node %d", up_node_id));

    if (!pg_raft_spi_begin(&spi_owned))
        return;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partition_id, primary_node, "
                     "       COALESCE(("
                     "         SELECT jsonb_agg(x ORDER BY x)::text "
                     "         FROM ("
                     "           SELECT DISTINCT unnest(secondary_nodes || ARRAY[%d]) AS x"
                     "         ) s"
                     "       ), '[%d]') AS secondary_json "
                     "FROM partdist.partition_map "
                     "WHERE primary_node <> %d "
                     "  AND NOT (%d = ANY(secondary_nodes)) "
                     "  AND COALESCE(primary_term, 0) = 0 "
                     "ORDER BY partition_id",
                     up_node_id, up_node_id, up_node_id, up_node_id);

    ret = SPI_execute(sql.data, true, 0);
    pfree(sql.data);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
    {
        if (spi_owned)
            pg_raft_spi_end(spi_owned);
        return;
    }

    {
        MemoryContext old_ctx = MemoryContextSwitchTo(caller_ctx);

        n = (int) SPI_processed;
        parts = (Oid *) palloc(sizeof(Oid) * n);
        primaries = (int *) palloc(sizeof(int) * n);
        sec_jsons = (char **) palloc(sizeof(char *) * n);

        for (i = 0; i < n; i++)
        {
            char *sec_text;

            parts[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
                                                      SPI_tuptable->tupdesc, 1, &isnull));
            primaries[i] = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                                      SPI_tuptable->tupdesc, 2, &isnull));
            sec_text = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[i],
                                                         SPI_tuptable->tupdesc, 3, &isnull));
            sec_jsons[i] = pstrdup(sec_text);
            pfree(sec_text);
        }

        MemoryContextSwitchTo(old_ctx);
    }

    if (spi_owned)
    {
        pg_raft_spi_end(spi_owned);
        spi_owned = false;
    }

    for (i = 0; i < n; i++)
    {
        elog(LOG, "pg_raft: rejoin node %d as secondary for partition %u (primary=%d)",
             up_node_id, parts[i], primaries[i]);
        (void) pg_raft_propose_partition_primary_internal(parts[i], primaries[i],
                                                          sec_jsons[i],
                                                          primaries[i],
                                                          0,
                                                          "0/0",
                                                          0);
        pfree(sec_jsons[i]);
    }
    pfree(parts);
    pfree(primaries);
    pfree(sec_jsons);

    if (spi_owned)
        pg_raft_spi_end(spi_owned);
}

static void
partdist_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();
    pg_raft_shmem_request();
}

static void
partdist_shmem_startup(void)
{
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    pg_raft_shmem_startup();
}

static void
register_topology_worker(void)
{
    BackgroundWorker worker;

    if (!process_shared_preload_libraries_in_progress)
        return;

    /*
     * 纯 C Raft 模式下每个节点都要运行 BGWorker 推进状态机；
     * 非 Raft 开发模式下仅 node 1 运行 TopologyMonitor。
     * 注：非 Leader 节点的 is_leader_local() 返回 false，不会误触发探测。
     */
    if (!pg_raft_raft_enabled && pg_raft_node_id != 1)
        return;

    MemSet(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_raft");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "pg_raft_topology_monitor_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "pg_raft topology monitor");
    snprintf(worker.bgw_type, BGW_MAXLEN, "pg_raft");
    worker.bgw_main_arg = Int32GetDatum(pg_raft_node_id);
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

void
_PG_init(void)
{
    /* POSTMASTER GUC 只能在 shared_preload 阶段注册；CREATE EXTENSION 会再次加载 .so */
    if (process_shared_preload_libraries_in_progress)
    {
        DefineCustomIntVariable("pg_raft.node_id",
                                "Local node id for control-plane Raft.",
                                NULL, &pg_raft_node_id, 1, 1, 65534,
                                PGC_POSTMASTER, 0, NULL, NULL, NULL);

        DefineCustomBoolVariable("pg_raft.raft_enabled",
                                 "Enable built-in pure-C Raft consensus for leader election.",
                                 NULL, &pg_raft_raft_enabled, false,
                                 PGC_POSTMASTER, 0, NULL, NULL, NULL);

        DefineCustomStringVariable("pg_raft.peers",
                                   "Raft cluster members: 'id@host:port,...' (include self).",
                                   NULL, &pg_raft_peers, "",
                                   PGC_POSTMASTER, 0, NULL, NULL, NULL);
    }

    if (!process_shared_preload_libraries_in_progress)
    {
        /* 扩展二次加载时仅注册可热更新 GUC（若尚未注册） */
        return;
    }

    DefineCustomIntVariable("pg_raft.propose_wait_ms",
                            "日志环满时等待 apply 追上的上限（毫秒）；0 = 不等，直接丢提案。",
                            "数据组丢提案会让副本与 leader 永久分叉，默认应保持 > 0。",
                            &pg_raft_propose_wait_ms, 10000, 0, 600000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.probe_interval_ms",
                            "Topology probe interval in milliseconds.",
                            NULL, &pg_raft_probe_interval_ms, 3000, 500, 60000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.probe_fail_threshold",
                            "Consecutive probe failures before marking node down.",
                            NULL, &pg_raft_probe_fail_threshold, 2, 1, 20,
                            PGC_USERSET, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.leader_lease_ms",
                            "Leader lease duration in milliseconds.",
                            NULL, &pg_raft_leader_lease_ms, 10000, 1000, 120000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.election_timeout_ms",
                            "Raft election timeout base in ms (randomized up to 2x).",
                            NULL, &pg_raft_election_timeout_ms, 1500, 200, 60000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.coordinator_node_id",
                            "Coordinator node id: preferred group-0 leader; excluded from data groups (0=none).",
                            NULL, &pg_raft_coordinator_node_id, 1, 0, 65534,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.heartbeat_ms",
                            "Raft leader heartbeat interval in milliseconds.",
                            NULL, &pg_raft_heartbeat_ms, 400, 50, 10000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = partdist_shmem_request;
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = partdist_shmem_startup;

    /*
     * prepare 接线：把复制函数注入 pg_partdist 的 rendezvous 挂点。
     * PartWALFlush 在 [A] 本地 parwal fsync 后、[B] 提交 fsync 前逐分区调用。
     */
    {
        void **rv = find_rendezvous_variable("partdist_partwal_replicate_hook");

        *rv = (void *) pg_raft_partwal_replicate;
    }

    DefineCustomBoolVariable("pg_raft.dtx_2pc_enabled",
                             "启用跨分区事务的 DTX-2PC：master 侧决议驱动 + 参与者自治登记。",
                             "关闭后跨分区事务退回 Citus 原生 2PC（提交点在 master 本地），"
                             "跨分区原子性不再有多数派保证。",
                             &pg_raft_dtx_2pc_enabled, true,
                             PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.catchup_interval_ms",
                            "后台追平通道的运行间隔（0=关闭）。",
                            "本节点为 leader 的每个组，把落后成员按 nextIndex 逐条补齐。"
                            "TopologyMonitor 经 libpq 自连接调 partdist.pg_raft_catchup()，"
                            "因而跑在 client backend 里（BGW 无 SPI，取不到 parwal 字节，"
                            "也回读不了环外条目）。关闭后落后副本只能等下一次业务写入顺带补齐。",
                            &pg_raft_catchup_interval_ms, 5000, 0, 3600000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.compact_threshold",
                            "控制面日志压缩阈值：已 apply 的条目超过该数量即压缩（0=关）。",
                            "压缩会把 partdist.raft_log 里 last_applied 之前的行删掉，"
                            "并把基点记进 hardstate；被删掉的那一段之后只能靠 "
                            "partdist.pg_raft_install_snapshot() 传给落后成员。"
                            "只作用于控制面（组 0）——数据组的状态机在物理回放之前就是 "
                            "parwal 字节流本身，压缩它的正解是日志外部化。",
                            &pg_raft_compact_threshold, 500, 0, 1000000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.dtx_recover_interval_ms",
                            "DTX-2PC 参与者恢复守护的自动运行间隔（0=关闭自动运行）。",
                            "每个节点的 TopologyMonitor 按该间隔经 libpq 自连接调"
                            " partdist.dtx_recover_prepared()，收尾超时未闭合的 in-doubt 事务。",
                            &pg_raft_dtx_recover_interval_ms, 10000, 0, 3600000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.dtx_recover_timeout_ms",
                            "prepared 事务超过该年龄才会被恢复守护处理。",
                            "只影响\"多久开始问\"，不影响正确性；设保守以免与正常路径的"
                            "阶段 3 抢答（DTX_2PC_DESIGN.md §7）。",
                            &pg_raft_dtx_recover_timeout_ms, 30000, 1000, 3600000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.promote_catchup_slice_ms",
                            "升主前置每个 BGW tick 最多推进多久的物理回放追平。",
                            "切片是为了不让追平久占 tick —— 同一个 tick 还要给其余各组"
                            "发心跳，久占会触发别组的无谓改选。没追完下个 tick 接着追。",
                            &pg_raft_promote_catchup_slice_ms, 2000, 100, 60000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    DefineCustomIntVariable("pg_raft.promote_catchup_deadline_ms",
                            "升主前置累计超过该时长仍未追平时，按可用性优先放行上报。",
                            "追不平就永不上报会让分片永久无主，比读到旧数据更糟。"
                            "放行时打 WARNING，把这个取舍显式化。设 0 表示永不放行。",
                            &pg_raft_promote_catchup_deadline_ms, 60000, 0, 3600000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

    /*
     * DTX-2PC 接线（DTX_2PC_DESIGN.md §9.3）：
     *   pre_record_commit_hook   —— 内核补丁 0004 开的挂点，master 侧做决议；
     *   partdist_dtx_note_participant_hook —— pg_partdist 在 prepare 时调，
     *   经 libpq 独立事务把本节点写集登记出去（libpq 由 pg_raft 持有）。
     */
    pg_raft_dtx_install_hooks();

    register_topology_worker();
}

Datum
pg_raft_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text("1.0"));
}

Datum
pg_raft_is_leader(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(pg_raft_is_leader_local());
}

static int
pg_raft_current_leader_id(void)
{
    int leader = pg_raft_node_id;

    /* raft_enabled：以内置纯 C Raft 的共识 leader 为准（>0 表示已选出） */
    if (pg_raft_raft_enabled)
    {
        int l = pg_raft_consensus_leader_id();
        if (l > 0)
            return l;
    }

    if (RaftLeaderShmemData && RaftLeaderLock)
    {
        LWLockAcquire(RaftLeaderLock, LW_SHARED);
        leader = RaftLeaderShmemData->leader_node_id;
        LWLockRelease(RaftLeaderLock);
    }
    return leader;
}

Datum
pg_raft_get_leader(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(pg_raft_current_leader_id());
}

Datum
pg_raft_get_cluster_status(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    Datum           values[5];
    bool            nulls[5] = {false, false, false, false, false};
    HeapTuple       tuple;
    TupleDesc       tupdesc;

    if (SRF_IS_FIRSTCALL())
    {
        MemoryContext oldcontext;

        funcctx = SRF_FIRSTCALL_INIT();
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        tupdesc = CreateTemplateTupleDesc(5);
        TupleDescInitEntry(tupdesc, (AttrNumber) 1, "leader_node_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 2, "current_term", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 3, "local_node_id", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 4, "is_leader", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber) 5, "backend", TEXTOID, -1, 0);
        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr > 0)
        SRF_RETURN_DONE(funcctx);

    values[0] = Int32GetDatum(pg_raft_current_leader_id());
    values[1] = Int64GetDatum(pg_raft_raft_enabled
                              ? pg_raft_consensus_term()
                              : (RaftLeaderShmemData ? RaftLeaderShmemData->current_term : 1));
    values[2] = Int32GetDatum(pg_raft_node_id);
    values[3] = BoolGetDatum(pg_raft_is_leader_local());
    values[4] = CStringGetTextDatum(pg_raft_raft_enabled ? "raft-c" : "pg_raft");

    tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
    funcctx->call_cntr++;
    SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
}

Datum
pg_raft_propose_node_status(PG_FUNCTION_ARGS)
{
    int    node_id = PG_GETARG_INT32(0);
    char  *status = text_to_cstring(PG_GETARG_TEXT_PP(1));
    int64  log_id;

    log_id = pg_raft_propose_node_status_internal(node_id, status);
    pfree(status);
    PG_RETURN_INT64(log_id);
}

Datum
pg_raft_propose_partition_primary(PG_FUNCTION_ARGS)
{
    Oid         part = PG_GETARG_OID(0);
    int         primary = PG_GETARG_INT32(1);
    ArrayType  *arr = PG_GETARG_ARRAYTYPE_P(2);
    StringInfoData arrlit;
    Datum         *elems;
    int            nelems;
    int            i;
    int64          log_id;

    if (!pg_raft_is_leader_local())
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: only leader may propose")));

    initStringInfo(&arrlit);
    appendStringInfoChar(&arrlit, '[');
    deconstruct_array(arr, INT4OID, 4, true, 'i', &elems, NULL, &nelems);
    for (i = 0; i < nelems; i++)
    {
        if (i > 0)
            appendStringInfoChar(&arrlit, ',');
        appendStringInfo(&arrlit, "%d", DatumGetInt32(elems[i]));
    }
    appendStringInfoChar(&arrlit, ']');

    log_id = pg_raft_propose_partition_primary_internal(part, primary, arrlit.data,
                                                        0, 0, "0/0", 0);
    pfree(arrlit.data);
    PG_RETURN_INT64(log_id);
}

/*
 * 切主重构：数据组新任 leader 的登记入口（在 group 0 leader——常态是 master——上执行）。
 * 数据组自治选举出新 leader 后，由新 leader 的 BGW tick 经 libpq 调用本函数上报；
 * 这里过任期栅栏后把 OP_PARTITION_PRIMARY 提进 group 0，apply 时每个成员节点各自
 * 更新本地 partition_map，真实 Citus 分片还会把本地 pg_dist_placement 指向新主
 * （路由元数据的 4 节点同步由 group 0 日志本身完成）。
 *
 * 返回值约定（上报方据此决定是否重试）：
 *   >0  已提名（group 0 日志 index）
 *   -1  无需登记（同任期已登记过，或已有更新任期的登记）——停止重试
 *    0  本节点当前不是 group 0 leader——下个 tick 换当时的 leader 重试
 */
Datum
pg_raft_report_data_leader(PG_FUNCTION_ARGS)
{
    int64      group_id = PG_GETARG_INT64(0);
    int        leader_node = PG_GETARG_INT32(1);
    int64      term = PG_GETARG_INT64(2);
    ArrayType *sec_arr = PG_ARGISNULL(3) ? NULL : PG_GETARG_ARRAYTYPE_P(3);
    StringInfoData sql;
    StringInfoData sec_json;
    int64      stored_term = 0;
    int        stored_primary = 0;
    int        ret;
    bool       isnull;
    bool       spi_owned;
    int64      idx;

    if (group_id <= 0)
        ereport(ERROR, (errmsg("pg_raft: 数据组 id 必须 > 0")));
    if (term <= 0)
        ereport(ERROR, (errmsg("pg_raft: 上报任期必须 > 0")));
    if (pg_raft_coordinator_node_id > 0 &&
        leader_node == pg_raft_coordinator_node_id)
        ereport(ERROR,
                (errmsg("pg_raft: 协调节点 %d 不作数据副本，不能登记为分区 %lld 的主",
                        pg_raft_coordinator_node_id, (long long) group_id)));

    if (!pg_raft_is_leader_local())
        PG_RETURN_INT64(0);

    /* 任期栅栏预检：旧任期的迟到上报、同任期的重复上报，都不再进 group 0 日志 */
    if (!pg_raft_spi_begin(&spi_owned))
        PG_RETURN_INT64(0);
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT primary_node, COALESCE(primary_term, 0) "
                     "FROM partdist.partition_map WHERE partition_id = %u",
                     (Oid) group_id);
    ret = SPI_execute(sql.data, true, 1);
    pfree(sql.data);
    if (ret == SPI_OK_SELECT && SPI_processed > 0)
    {
        stored_primary = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                                     SPI_tuptable->tupdesc, 1, &isnull));
        stored_term = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                                  SPI_tuptable->tupdesc, 2, &isnull));
    }
    pg_raft_spi_end(spi_owned);

    if (stored_term > term ||
        (stored_term == term && stored_primary == leader_node))
        PG_RETURN_INT64(-1);

    /* secondaries = 上报的成员集去掉新主；协调节点若混入则剔除并告警 */
    initStringInfo(&sec_json);
    appendStringInfoChar(&sec_json, '[');
    if (sec_arr != NULL)
    {
        Datum *elems;
        bool  *nulls;
        int    nelems;
        int    i;
        int    n = 0;

        deconstruct_array(sec_arr, INT4OID, 4, true, 'i', &elems, &nulls, &nelems);
        for (i = 0; i < nelems; i++)
        {
            int v;

            if (nulls[i])
                continue;
            v = DatumGetInt32(elems[i]);
            if (v == leader_node)
                continue;
            if (pg_raft_coordinator_node_id > 0 && v == pg_raft_coordinator_node_id)
            {
                elog(WARNING,
                     "pg_raft: 分区 %lld 上报的从副本集包含协调节点 %d，已剔除",
                     (long long) group_id, v);
                continue;
            }
            appendStringInfo(&sec_json, "%s%d", n ? "," : "", v);
            n++;
        }
    }
    appendStringInfoChar(&sec_json, ']');

    idx = pg_raft_propose_partition_primary_internal((Oid) group_id, leader_node,
                                                     sec_json.data, stored_primary,
                                                     0, "0/0", term);
    pfree(sec_json.data);

    if (idx > 0)
        elog(LOG, "pg_raft: 登记数据组 %lld 新主 node=%d term=%lld (group0 idx=%lld)",
             (long long) group_id, leader_node, (long long) term, (long long) idx);
    PG_RETURN_INT64(idx);
}

Datum
pg_raft_force_probe(PG_FUNCTION_ARGS)
{
    pg_raft_topology_probe_and_failover();
    PG_RETURN_BOOL(true);
}

Datum
pg_raft_apply_payload(PG_FUNCTION_ARGS)
{
    char  *op = text_to_cstring(PG_GETARG_TEXT_PP(0));
    char  *payload = text_to_cstring(PG_GETARG_TEXT_PP(1));
    bool   ok;

    ok = pg_raft_apply_payload_sql(op, payload);
    pfree(op);
    pfree(payload);
    PG_RETURN_BOOL(ok);
}
