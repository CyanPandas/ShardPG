#include "postgres.h"

#include "pg_raft.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
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

/*
 * 刷新快照里的**状态机内容**（每次控制面 apply 之后都做）。
 *
 * ★ 只写内容，**不动 last_included_index/last_included_term**。这两个字段只有
 * 日志压缩（control_maybe_compact）能推进，而且必须成对写 —— 它们是"这份内容
 * 对应哪条日志"的凭据，装快照的一方要拿 term 去做 prev 一致性检查。若这里也去
 * 推进 index 而拿不到对应的 term，快照就会带着 (较新的 index, 陈旧的 term)
 * 发出去，接收方的日志匹配从此错位。
 *
 * 代价：内容可能比 index 新（apply 一直在往前走，压缩点还停在后面）。这是安全的
 * ——接收方按 index 设游标后，leader 会把 index+1.. 之后的条目再发一遍，控制面
 * apply 全是幂等 upsert（partition_map 还带任期栅栏），重放到同一个终态。
 */
static void
raft_save_metadata_snapshot(void)
{
    StringInfoData sql;

    initStringInfo(&sql);
    appendStringInfoString(&sql,
                     "INSERT INTO partdist.raft_snapshot "
                     "(singleton, last_included_index, last_included_term, node_map, partition_map, updated_at) "
                     "SELECT 1, 0, 0, "
                     "COALESCE((SELECT jsonb_agg(to_jsonb(n) ORDER BY node_id) FROM partdist.node_map n), '[]'::jsonb), "
                     "COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY partition_id) FROM partdist.partition_map p), '[]'::jsonb), "
                     "now() "
                     "ON CONFLICT (singleton) DO UPDATE SET "
                     "node_map = EXCLUDED.node_map, "
                     "partition_map = EXCLUDED.partition_map, "
                     "updated_at = now()");
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
        raft_save_metadata_snapshot();
    }
    raft_spi_end(spi_owned);
    return ok;
}

/*
 * 桥接路由层：partition_id 是真实 Citus shardid 时，把**本节点**的
 * pg_dist_placement 指向新主所在的 Citus group（node_map.port ↔ pg_dist_node.nodeport）。
 * group 0 的 apply 在每个成员节点各自执行，因此 4 节点路由元数据的同步由 raft
 * 日志本身完成；UPDATE 触发 Citus 的 dist_placement_cache_invalidate 失效缓存。
 * 合成分区（partition_id 不在 pg_dist_placement 中）自然匹配 0 行，无副作用。
 *
 * 注意：P3 物理回放落地前，从副本壳表还没有数据——此桥只保证"机制先行"，
 * 切主后立即服务读写要等回放追平（见计划文档 §12）。
 */
static void
raft_update_citus_placement(Oid partition_id, int primary_node)
{
    StringInfoData sql;
    int            ret;
    bool           isnull;
    MemoryContext  oldcontext = CurrentMemoryContext;
    ResourceOwner  oldowner = CurrentResourceOwner;

    ret = SPI_execute("SELECT to_regclass('pg_dist_placement') IS NOT NULL", true, 1);
    if (ret != SPI_OK_SELECT || SPI_processed == 0 ||
        !DatumGetBool(SPI_getbinval(SPI_tuptable->vals[0],
                                    SPI_tuptable->tupdesc, 1, &isnull)))
        return;

    /*
     * 只对**单放置**分片切路由。reference 表/rf>1 的分片在 pg_dist_placement
     * 里同一 shardid 有多行（每个节点组一行），既没有"唯一主"的语义，直接
     * UPDATE 还会撞 (shardid, groupid) 唯一键——raft_13 的 reference 夹具组
     * 上报登记时实测踩中。
     */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "UPDATE pg_dist_placement p SET groupid = n.groupid "
                     "FROM partdist.node_map m, pg_dist_node n "
                     "WHERE m.node_id = %d AND n.nodeport = m.port "
                     "  AND n.noderole = 'primary' "
                     "  AND p.shardid = %u AND p.groupid <> n.groupid "
                     "  AND (SELECT count(*) FROM pg_dist_placement q "
                     "       WHERE q.shardid = p.shardid) = 1",
                     primary_node, partition_id);

    /*
     * 子事务隔离：桥接只是"锦上添花"的路由层落盘，任何意外 SQL 错误都不允许
     * 打穿控制面 apply——否则 apply 游标会卡死在这条日志上，之后所有控制面
     * 决议（含别的分区的登记）全部停摆（首轮回归实测到的毒丸场景）。
     */
    BeginInternalSubTransaction(NULL);
    PG_TRY();
    {
        ret = SPI_execute(sql.data, false, 0);
        if (ret == SPI_OK_UPDATE && SPI_processed > 0)
            elog(LOG, "pg_raft: 路由层已切换 shard %u -> node %d (pg_dist_placement)",
                 partition_id, primary_node);
        ReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcontext);
        CurrentResourceOwner = oldowner;
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        MemoryContextSwitchTo(oldcontext);
        CurrentResourceOwner = oldowner;
        elog(WARNING,
             "pg_raft: 路由层桥接失败已跳过(shard %u -> node %d)，partition_map 登记不受影响",
             partition_id, primary_node);
    }
    PG_END_TRY();
    pfree(sql.data);
}

/*
 * 用快照整体替换控制面状态机（InstallSnapshot 的 apply 侧）。
 *
 * 状态机 = partdist.node_map + partdist.partition_map。快照载荷正是这两张表的
 * `jsonb_agg(to_jsonb(...))`，所以这里用 jsonb_populate_recordset 原样灌回。
 *
 * **必须整体替换而不是 upsert**：快照代表"截至 last_included_index 的完整状态"，
 * 本地多出来的行（来自另一条历史，或已被删除的分区）留着就是分叉。
 *
 * 替换完还要把路由层同步一遍 —— 逐条日志 apply 时每条 OP_PARTITION_PRIMARY 都会
 * 调 raft_update_citus_placement()，装快照跳过了那些日志，不补这一步的话本节点的
 * pg_dist_placement 会停留在旧主上（"装了快照但路由没跟上"是最难查的那类分叉）。
 */
bool
pg_raft_apply_snapshot_state(const char *node_map_json,
                             const char *partition_map_json)
{
    StringInfoData sql;
    bool           spi_owned;
    bool           ok = false;
    int            i;
    int            nparts = 0;
    Oid           *part_ids = NULL;
    int           *part_primaries = NULL;

    if (node_map_json == NULL || partition_map_json == NULL)
        return false;

    if (!raft_spi_begin(&spi_owned))
        return false;

    /*
     * 删与插分两条语句发：写在同一条语句里（data-modifying CTE）时，INSERT 的
     * 唯一性检查未必看得见同命令内的删除，会撞主键。分两条则 SPI 会推进命令
     * 计数器，第二条稳稳看得见第一条的效果。
     *
     * 空数组一律跳过而不是照删：宁可保留旧行也不能把 node_map 清空——那会让
     * 本节点失去全部对端地址，连"谁是 leader"都问不出来。
     */
    initStringInfo(&sql);
    if (strcmp(node_map_json, "[]") != 0)
    {
        if (SPI_execute("DELETE FROM partdist.node_map", false, 0) < 0)
            goto done;
        appendStringInfo(&sql,
                         "INSERT INTO partdist.node_map "
                         "SELECT * FROM jsonb_populate_recordset(NULL::partdist.node_map, %s::jsonb)",
                         quote_literal_cstr(node_map_json));
        if (SPI_execute(sql.data, false, 0) < 0)
            goto done;
        resetStringInfo(&sql);
    }
    else
        elog(WARNING, "pg_raft: 快照里的 node_map 为空，跳过替换（保留本地行）");

    if (SPI_execute("DELETE FROM partdist.partition_map", false, 0) < 0)
        goto done;
    if (strcmp(partition_map_json, "[]") != 0)
    {
        appendStringInfo(&sql,
                         "INSERT INTO partdist.partition_map "
                         "SELECT * FROM jsonb_populate_recordset(NULL::partdist.partition_map, %s::jsonb)",
                         quote_literal_cstr(partition_map_json));
        if (SPI_execute(sql.data, false, 0) < 0)
            goto done;
        resetStringInfo(&sql);
    }

    /* 取回分区列表，逐个把路由层拨到快照里的主副本 */
    appendStringInfoString(&sql,
                           "SELECT partition_id, primary_node FROM partdist.partition_map "
                           "WHERE primary_node IS NOT NULL ORDER BY partition_id");
    if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT && SPI_processed > 0)
    {
        MemoryContext oldctx = MemoryContextSwitchTo(TopTransactionContext);

        nparts = (int) SPI_processed;
        part_ids = (Oid *) palloc(sizeof(Oid) * nparts);
        part_primaries = (int *) palloc(sizeof(int) * nparts);
        MemoryContextSwitchTo(oldctx);

        for (i = 0; i < nparts; i++)
        {
            bool isnull;

            part_ids[i] = DatumGetObjectId(SPI_getbinval(SPI_tuptable->vals[i],
                                                         SPI_tuptable->tupdesc,
                                                         1, &isnull));
            part_primaries[i] = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[i],
                                                            SPI_tuptable->tupdesc,
                                                            2, &isnull));
        }
    }

    for (i = 0; i < nparts; i++)
        raft_update_citus_placement(part_ids[i], part_primaries[i]);

    raft_invalidate_cache();
    ok = true;

done:
    pfree(sql.data);
    raft_spi_end(spi_owned);
    return ok;
}

bool
pg_raft_apply_partition_primary(Oid partition_id, int primary_node,
                              const char *secondaries_array_literal,
                              int old_primary_node,
                              uint64 switch_partition_lsn,
                              const char *switch_orig_lsn,
                              int64 primary_term)
{
    StringInfoData sql;
    int            ret;
    bool           ok = false;
    bool           applied = false;
    bool           spi_owned;

    if (!raft_spi_begin(&spi_owned))
        return false;

    /*
     * 任期栅栏：只接受任期不回退的更新。数据组自治选举的任期单调递增，
     * 迟到的旧任期登记（或旧"控制面指定"通道的 term=0 提案）不能覆盖
     * 新任期的结果。栅栏放在 apply 里，group 0 各成员按同一日志序执行，
     * 判定结果天然一致。
     */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO partdist.partition_map (partition_id, primary_node, secondary_nodes, primary_term) "
                     "VALUES (%u, %d, %s::int[], %lld) "
                     "ON CONFLICT (partition_id) DO UPDATE SET "
                     "primary_node = EXCLUDED.primary_node, "
                     "secondary_nodes = EXCLUDED.secondary_nodes, "
                     "primary_term = EXCLUDED.primary_term, updated_at = now() "
                     "WHERE partition_map.primary_term <= EXCLUDED.primary_term",
                     partition_id, primary_node,
                     quote_literal_cstr(secondaries_array_literal),
                     (long long) primary_term);

    ret = SPI_execute(sql.data, false, 0);
    ok = (ret == SPI_OK_INSERT || ret == SPI_OK_UPDATE);
    applied = ok && SPI_processed > 0;
    pfree(sql.data);

    if (applied)
    {
        elog(LOG,
             "pg_raft: apply partition primary partition=%u old_primary=%d new_primary=%d "
             "primary_term=%lld switch_partition_lsn=" UINT64_FORMAT " switch_orig_lsn=%s",
             partition_id, old_primary_node, primary_node,
             (long long) primary_term, switch_partition_lsn,
             switch_orig_lsn ? switch_orig_lsn : "0/0");
        raft_notify_primary_switch(partition_id,
                                   old_primary_node,
                                   primary_node,
                                   switch_orig_lsn);
        raft_update_citus_placement(partition_id, primary_node);
        raft_invalidate_cache();
        raft_save_metadata_snapshot();
    }
    else if (ok)
        elog(LOG,
             "pg_raft: partition %u 主副本更新被任期栅栏拦下 (提案任期 %lld 落后于已登记任期)",
             partition_id, (long long) primary_term);
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
        int64   primary_term;
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
                         "       COALESCE(j->>'switch_orig_lsn', '0/0'), "
                         "       COALESCE((j->>'primary_term')::bigint, 0) "
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
            primary_term = DatumGetInt64(SPI_getbinval(SPI_tuptable->vals[0],
                                                       SPI_tuptable->tupdesc, 7, &isnull));

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
                                                 switch_orig_lsn_text,
                                                 primary_term);

            pfree(secondary_nodes_text);
            pfree(switch_orig_lsn_text);
            return ok;
        }
    }

    if (ok && !handled_partition_primary)
    {
        raft_invalidate_cache();
        raft_save_metadata_snapshot();
    }
    raft_spi_end(spi_owned);
    return ok;
}
