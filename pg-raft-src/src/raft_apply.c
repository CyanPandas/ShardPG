#include "postgres.h"

#include "pg_raft.h"

#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

static bool
raft_spi_begin(bool *spi_owned)
{
    if (!pg_raft_spi_begin(spi_owned))
        return false;
    if (*spi_owned)
        PushActiveSnapshot(GetTransactionSnapshot());
    return true;
}

static void
raft_spi_end(bool spi_owned)
{
    if (spi_owned)
    {
        PopActiveSnapshot();
        pg_raft_spi_end(true);
    }
}

static void
raft_invalidate_cache(void)
{
    (void) SPI_execute("SELECT partdist.pg_partdist_cache_invalidate()", false, 0);
}

static void
raft_notify_primary_switch(Oid partition_id,
                           int old_primary_node,
                           int new_primary_node,
                           const char *switch_orig_lsn)
{
    StringInfoData sql;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT partdist.partwal_notify_primary_switch(%u, %d, %d, %s::pg_lsn)",
                     partition_id,
                     old_primary_node,
                     new_primary_node,
                     quote_literal_cstr(switch_orig_lsn ? switch_orig_lsn : "0/0"));
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);
}

static void
raft_save_metadata_snapshot(int64 last_included_index)
{
    StringInfoData sql;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO partdist.raft_snapshot "
                     "(singleton, last_included_index, node_map, partition_map, updated_at) "
                     "SELECT 1, %lld, "
                     "COALESCE((SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id) FROM partdist.node_map n), '[]'::jsonb), "
                     "COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY partition_id) FROM partdist.partition_map p), '[]'::jsonb), "
                     "now() "
                     "ON CONFLICT (singleton) DO UPDATE SET "
                     "last_included_index = GREATEST(partdist.raft_snapshot.last_included_index, "
                     "EXCLUDED.last_included_index), "
                     "node_map = EXCLUDED.node_map, "
                     "partition_map = EXCLUDED.partition_map, "
                     "updated_at = now()",
                     (long long) last_included_index);
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);
}

bool
pg_raft_apply_node_status(int node_id, const char *status)
{
    StringInfoData sql;
    int            ret;
    bool           ok = false;
    bool           spi_owned;

    if (!raft_spi_begin(&spi_owned))
        return false;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "UPDATE partdist.node_map SET status = %s, last_heartbeat = now() "
                     "WHERE node_id = %d",
                     quote_literal_cstr(status), node_id);
    ret = SPI_execute(sql.data, false, 0);
    ok = (ret == SPI_OK_UPDATE && SPI_processed > 0);
    pfree(sql.data);

    if (ok)
    {
        raft_invalidate_cache();
        raft_save_metadata_snapshot(pg_raft_consensus_last_applied());
    }
    raft_spi_end(spi_owned);
    return ok;
}

bool
pg_raft_apply_partition_primary(Oid partition_id, int primary_node,
                              const char *secondaries_array_literal,
                              int old_primary_node,
                              uint64 switch_partition_lsn,
                              const char *switch_orig_lsn)
{
    StringInfoData sql;
    int            ret;
    bool           ok = false;
    bool           spi_owned;

    if (!raft_spi_begin(&spi_owned))
        return false;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes) "
                     "VALUES (%u, %d, %s::int[]) "
                     "ON CONFLICT (partition_id) DO UPDATE SET "
                     "primary_node = EXCLUDED.primary_node, "
                     "secondary_nodes = EXCLUDED.secondary_nodes, updated_at = now()",
                     partition_id, primary_node,
                     quote_literal_cstr(secondaries_array_literal));

    ret = SPI_execute(sql.data, false, 0);
    ok = (ret == SPI_OK_INSERT || ret == SPI_OK_UPDATE);
    pfree(sql.data);

    if (ok)
    {
        elog(LOG,
             "pg_raft: apply partition primary partition=%u old_primary=%d new_primary=%d "
             "switch_partition_lsn=" UINT64_FORMAT " switch_orig_lsn=%s",
             partition_id, old_primary_node, primary_node,
             switch_partition_lsn,
             switch_orig_lsn ? switch_orig_lsn : "0/0");
        raft_notify_primary_switch(partition_id,
                                   old_primary_node,
                                   primary_node,
                                   switch_orig_lsn);
        raft_invalidate_cache();
        raft_save_metadata_snapshot(pg_raft_consensus_last_applied());
    }
    raft_spi_end(spi_owned);
    return ok;
}

bool
pg_raft_apply_payload_sql(const char *op_type, const char *payload_json)
{
    StringInfoData sql;
    int            ret;
    bool           ok = false;
    bool           spi_owned;
    bool           handled_partition_primary = false;
    MemoryContext  caller_ctx = CurrentMemoryContext;

    if (!raft_spi_begin(&spi_owned))
        return false;

    if (strcmp(op_type, PG_RAFT_OP_NODE_STATUS) == 0)
    {
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "INSERT INTO partdist.node_map (node_id, hostname, port, status, last_heartbeat) "
                         "SELECT (j->>'node_id')::int, 'node' || (j->>'node_id'), 5432, j->>'status', now() "
                         "FROM (SELECT %s::jsonb AS j) s "
                         "ON CONFLICT (node_id) DO UPDATE SET "
                         "status = EXCLUDED.status, last_heartbeat = now()",
                         quote_literal_cstr(payload_json));
        ret = SPI_execute(sql.data, false, 0);
        ok = (ret == SPI_OK_INSERT || ret == SPI_OK_UPDATE);
        pfree(sql.data);
    }
    else if (strcmp(op_type, PG_RAFT_OP_PARTITION_PRIMARY) == 0)
    {
        bool    isnull;
        Oid     partition_id;
        int     primary_node;
        int     old_primary_node;
        uint64  switch_partition_lsn;
        char   *secondary_nodes_text;
        char   *switch_orig_lsn_text;

        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "SELECT (j->>'partition_id')::oid, "
                         "       (j->>'primary_node')::int, "
                         "       COALESCE((SELECT array_agg(t::int)::text "
                         "                 FROM jsonb_array_elements_text(j->'secondary_nodes') t), '{}'), "
                         "       COALESCE((j->>'old_primary_node')::int, 0), "
                         "       COALESCE((j->>'switch_partition_lsn')::bigint, 0), "
                         "       COALESCE(j->>'switch_orig_lsn', '0/0') "
                         "FROM (SELECT %s::jsonb AS j) s",
                         quote_literal_cstr(payload_json));
        ret = SPI_execute(sql.data, true, 1);
        pfree(sql.data);

        if (ret == SPI_OK_SELECT && SPI_processed > 0)
        {
            partition_id = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[0],
                                                          SPI_tuptable->tupdesc, 1, &isnull));
            primary_node = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                                       SPI_tuptable->tupdesc, 2, &isnull));
            {
                char *tmp_secondary_nodes;
                char *tmp_switch_orig_lsn;
                MemoryContext old_ctx;

                tmp_secondary_nodes = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
                                                                        SPI_tuptable->tupdesc, 3, &isnull));
                tmp_switch_orig_lsn = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
                                                                        SPI_tuptable->tupdesc, 6, &isnull));
                old_ctx = MemoryContextSwitchTo(caller_ctx);
                secondary_nodes_text = pstrdup(tmp_secondary_nodes);
                switch_orig_lsn_text = pstrdup(tmp_switch_orig_lsn);
                MemoryContextSwitchTo(old_ctx);
                pfree(tmp_secondary_nodes);
                pfree(tmp_switch_orig_lsn);
            }
            old_primary_node = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
                                                           SPI_tuptable->tupdesc, 4, &isnull));
            switch_partition_lsn = (uint64) DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                                                        SPI_tuptable->tupdesc, 5, &isnull));

            /*
             * 解析 payload 的 SPI 查询结束后，先关闭外层 SPI，再进入真正
             * 的 apply。否则 apply 内部的 UPDATE / notify 会复用同一个
             * SPI 连接，返回外层再 SPI_finish 时容易破坏 backend 状态。
             */
            raft_spi_end(spi_owned);
            spi_owned = false;

            ok = pg_raft_apply_partition_primary(partition_id, primary_node,
                                                 secondary_nodes_text,
                                                 old_primary_node,
                                                 switch_partition_lsn,
                                                 switch_orig_lsn_text);

            pfree(secondary_nodes_text);
            pfree(switch_orig_lsn_text);
            return ok;
        }
    }

    if (ok && !handled_partition_primary)
    {
        raft_invalidate_cache();
        raft_save_metadata_snapshot(pg_raft_consensus_last_applied());
    }
    raft_spi_end(spi_owned);
    return ok;
}
