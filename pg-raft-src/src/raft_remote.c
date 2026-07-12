#include "pg_raft.h"

#include "executor/spi.h"
#include "libpq-fe.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

/*
 * 当前控制面日志复制由 raft_consensus.c 的 AppendEntries 路径负责。
 * 这个旧接口只保留为兼容占位，避免历史调用点误以为这里负责真正提交。
 */
bool
pg_raft_replicate_to_peers(const char *op_type, const char *payload_json)
{
    (void) op_type;
    (void) payload_json;
    return true;
}

#if 0
/*
 * 远程 apply 骨架（worker 可加载 pg_raft 后取消 #if 0）
 * 须使用 pg_raft_spi_begin / pg_raft_format_conninfo，勿在 SQL 调用栈里 SPI_finish
 * 断开外层连接。
 */
bool
pg_raft_replicate_to_peers_remote(const char *op_type, const char *payload_json)
{
    StringInfoData sql;
    StringInfoData remote_sql;
    int            ret;
    bool           isnull;
    int            i;
    bool           ok = true;
    bool           spi_owned;
    bool           pushed_snap = false;

    if (!pg_raft_spi_begin(&spi_owned))
        return false;

    if (!spi_owned && !ActiveSnapshotSet())
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        pushed_snap = true;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT node_id, hostname, port FROM partdist.node_map "
                     "WHERE node_id <> %d AND status = 'active'",
                     pg_raft_node_id);

    ret = SPI_execute(sql.data, true, 0);
    pfree(sql.data);
    if (ret != SPI_OK_SELECT)
    {
        if (pushed_snap)
            PopActiveSnapshot();
        if (spi_owned)
            pg_raft_spi_end(spi_owned);
        return false;
    }

    initStringInfo(&remote_sql);
    appendStringInfo(&remote_sql,
                     "SELECT partdist.pg_raft_apply_payload(%s, %s::jsonb)",
                     quote_literal_cstr(op_type),
                     quote_literal_cstr(payload_json));

    for (i = 0; i < (int) SPI_processed; i++)
    {
        HeapTuple   tup = SPI_tuptable->vals[i];
        TupleDesc   desc = SPI_tuptable->tupdesc;
        int         nid;
        char       *host;
        int         port;
        char        conninfo[512];
        PGconn     *conn;
        PGresult   *res;

        nid = DatumGetInt32(SPI_getbinval(tup, desc, 1, &isnull));
        host = TextDatumGetCString(SPI_getbinval(tup, desc, 2, &isnull));
        port = DatumGetInt32(SPI_getbinval(tup, desc, 3, &isnull));

        pg_raft_format_conninfo(host, port, conninfo, sizeof(conninfo));
        conn = PQconnectdb(conninfo);
        if (PQstatus(conn) != CONNECTION_OK)
        {
            elog(DEBUG1, "pg_raft: peer %d connect failed: %s", nid, PQerrorMessage(conn));
            PQfinish(conn);
            pfree(host);
            ok = false;
            continue;
        }

        res = PQexec(conn, remote_sql.data);
        if (PQresultStatus(res) != PGRES_TUPLES_OK)
        {
            elog(DEBUG1, "pg_raft: peer %d apply failed: %s", nid, PQerrorMessage(conn));
            ok = false;
        }
        PQclear(res);
        PQfinish(conn);
        pfree(host);
    }

    pfree(remote_sql.data);
    if (pushed_snap)
        PopActiveSnapshot();
    if (spi_owned)
        pg_raft_spi_end(spi_owned);
    return ok;
}
#endif
