/*
 * dtx_participant.c — DTX-2PC 参与者侧接线（worker 侧）
 *
 * 设计依据：pg-partdist-src/docs/DTX_2PC_DESIGN.md §3.3 / §5 / §8.3 / §9.3。
 * 模块职责与时序见 include/dtx_participant.h 的文件头注释。
 */
#include "postgres.h"

#include "dtx_participant.h"
#include "dtx_record.h"
#include "partition_wal.h"
#include "partition_wal_writer.h"
#include "partwal_sync.h"

#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "tso.h"					/* T4.4：TsoMarkerCommitTs 换源 */

#include <string.h>

bool pg_partdist_dtx_2pc_enabled = true;

/* 本事务待处理的 prepared 事务 gid（TopMemoryContext 里，跨事务边界安全） */
static char *dtx_pending_gid = NULL;
static int64 dtx_pending_dtxid = 0;

/* PRE_PREPARE 里在 flush 之前取的触达分区快照 */
static Oid  *dtx_touched_snapshot = NULL;
static int   dtx_touched_count = 0;

static void **dtx_note_hook_rv = NULL;

/* ------------------------------------------------------------------ */
/* gid → dtxid                                                         */
/* ------------------------------------------------------------------ */

/*
 * Citus 的 gid 形如 citus_<group>_<pid>_<txnnum>_<conn>（remote_transaction.c
 * 的 Assign2PCIdentifier）。同一笔分布式事务发往不同 worker 的 gid 只有末段
 * <conn> 不同，前三段完全一致，所以各节点解析前三段就得到同一个 dtxid，
 * **不需要任何协商**。
 *
 * 打包：group(8) | pid(22) | txnnum(33) = 63 位，恒为正。
 * 为什么必须带 pid：Citus 文档明说 txnnum 只保证"自本次重启以来"唯一，
 * 协调节点重启后会重号 —— 光用 (group, txnnum) 做主键，重启前后的两笔事务
 * 会在 partdist.dtx_decision 上撞主键，把旧决议错当成新事务的决议。
 * pid 正是 Citus 为跨重启熵而放进 gid 的那一段。
 */
#define DTX_GROUP_BITS   8
#define DTX_PID_BITS    22
#define DTX_TXN_BITS    33

bool
DtxidFromGid(const char *gid, int64 *dtxid)
{
    long long group = 0, pid = 0, txn = 0, conn = 0;
    long long d = 0, c = 0;

    if (gid == NULL)
        return false;

    if (sscanf(gid, "citus_%lld_%lld_%lld_%lld",
               &group, &pid, &txn, &conn) == 4)
    {
        if (pid <= 0 || txn <= 0)
            return false;
        *dtxid = ((int64) (group & 0xFF) << (DTX_PID_BITS + DTX_TXN_BITS)) |
                 ((int64) (pid & 0x3FFFFF) << DTX_TXN_BITS) |
                 ((int64) (txn & 0x1FFFFFFFFLL));
        return (*dtxid > 0);
    }

    /* 本项目自造的 gid（机制测试用，见 §5.4） */
    if (sscanf(gid, "shardpg_dtx_%lld_%lld", &d, &c) == 2 && d > 0)
    {
        *dtxid = (int64) d;
        return true;
    }

    return false;
}

/* ------------------------------------------------------------------ */
/* 生命周期                                                            */
/* ------------------------------------------------------------------ */

void
PartDistDtxReset(void)
{
    if (dtx_pending_gid != NULL)
    {
        pfree(dtx_pending_gid);
        dtx_pending_gid = NULL;
    }
    dtx_pending_dtxid = 0;
    if (dtx_touched_snapshot != NULL)
    {
        pfree(dtx_touched_snapshot);
        dtx_touched_snapshot = NULL;
    }
    dtx_touched_count = 0;
}

void
PartDistDtxNotePrepareGid(const char *gid)
{
    int64         dtxid = 0;
    MemoryContext old;

    PartDistDtxReset();

    if (!pg_partdist_dtx_2pc_enabled)
        return;
    if (!DtxidFromGid(gid, &dtxid))
        return;                 /* 不是我们认识的 gid：完全不介入 */

    old = MemoryContextSwitchTo(TopMemoryContext);
    dtx_pending_gid = pstrdup(gid);
    MemoryContextSwitchTo(old);
    dtx_pending_dtxid = dtxid;
}

/* ------------------------------------------------------------------ */
/* PRE_PREPARE 两个挂点                                                */
/* ------------------------------------------------------------------ */

void
PartDistDtxPrePrepareCapture(void)
{
    int n;

    if (dtx_pending_gid == NULL)
        return;

    n = PartWALTouchedCount();
    if (n <= 0)
        return;                 /* 只读参与者（§8.3）：不登记、不写标记 */

    /*
     * 快照必须活到 PartWALFlush() 之后 —— flush 末尾的复制挂钩会清空触达集合。
     * 放 TopMemoryContext：PrepareTransaction() 会在回调之后拆掉
     * TopTransactionContext。
     */
    dtx_touched_snapshot = (Oid *) MemoryContextAlloc(TopMemoryContext,
                                                      sizeof(Oid) * n);
    dtx_touched_count = PartWALCopyTouched(dtx_touched_snapshot, n);
}

/*
 * 本地分区 OID → 全局 shard id。查不到（非 Citus 分片 / 尚未登记）返回 0。
 * 走 partdist.shard_identity，不用 shard_global_id()：后者要查
 * pg_dist_shard + shard_name()，在 prepare 的关键路径上没必要那么重。
 */
static int64
dtx_gsid_of(Oid partition_id)
{
    StringInfoData sql;
    int64          gsid = 0;
    bool           isnull;

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT global_shard_id FROM partdist.shard_identity "
                     " WHERE local_oid = %u::oid",
                     (unsigned) partition_id);
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        Datum d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
                                1, &isnull);

        if (!isnull)
            gsid = DatumGetInt64(d);
    }
    pfree(sql.data);
    return gsid;
}

void
PartDistDtxPrePrepareFinish(void)
{
    Oid            *touched;
    int             ntouched;
    int64          *gsids;
    int             ngsids = 0;
    int             i;
    TransactionId   xid;
    DtxNoteParticipantHook note;
    bool            ok;

    if (dtx_pending_gid == NULL)
    {
        PartDistDtxReset();
        return;
    }

    touched = dtx_touched_snapshot;
    ntouched = dtx_touched_count;
    xid = GetTopTransactionIdIfAny();

    /*
     * 1) 每个触达分区追加一条 DTX_PREPARE 标记。
     *
     * 必须在 PartWALFlush() **之后**追加：DATA 记录的 partition_lsn 是在 flush
     * 落盘时才分配的，先追加标记会让标记排在它所标记的数据**前面**，回放侧
     * （FRD §7.6 按 DATA → PREPARE → COMMIT 顺序建立可见性）就会读到倒序。
     */
    gsids = (int64 *) palloc(sizeof(int64) * (ntouched > 0 ? ntouched : 1));

    /*
     * 2PC 标记的第一段：PREPARE 标记（DTX_2PC_DESIGN.md §3.3）。
     *
     * DTX_PREPARE 记录服务的是"升主时闭合 in-doubt"，它不参与事务判决 ——
     * 回放侧对 DTX 类记录只推游标。真正让 follower 记账的只有 MARKER 类记录，
     * 所以这里必须另发一条。
     *
     * 载荷带子事务清单、**不带** commit_ts：此刻还没提交。整棵提交树在
     * follower 上落成 TXN_PREPARED（未决＝不可见），等第二段的 COMMIT/ABORT
     * 标记来定性。子事务靠槽里的 parent_xid 跟随顶层，因为第二段是在
     * COMMIT PREPARED 自己的事务里补写的，那里拿不到本事务的子事务清单。
     *
     * 必须在 SPI_connect 之前组装：SPI 会把 CurrentMemoryContext 切到它的
     * 过程上下文，SPI_finish 时连载荷一起释放。
     */
    {
    int n_unmanaged = 0;
    char   *prep_marker = NULL;
    uint32  prep_marker_len = 0;

    if (ntouched > 0 && TransactionIdIsValid(xid))
        prep_marker = PartWALBuildMarkerPayload(true, false, &prep_marker_len);

    if (ntouched > 0)
    {
        if (SPI_connect() != SPI_OK_CONNECT)
            ereport(ERROR,
                    (errmsg("pg_partdist: DTX prepare 接线无法建立 SPI 连接")));

        /*
         * ★ 必须自己压一个活动快照。XACT_EVENT_PRE_PREPARE 跑在
         * PrepareTransaction() 里、PreCommit_Portals(true) **之后** —— 那时
         * portal 已全部关闭、活动快照已弹空，直接 SPI_execute 会报
         * "cannot execute SQL without an outer snapshot or portal"
         * （2026-08-04 实测：第一次真的跑通端到端 2PC 时立刻撞上）。
         * pg_raft 的 raft_persist_spi_begin() 早就是这么做的。
         */
        PushActiveSnapshot(GetTransactionSnapshot());

        for (i = 0; i < ntouched; i++)
        {
            int64 gsid = dtx_gsid_of(touched[i]);

            /*
             * 未登记进 shard_identity 的分区不是 Citus 分片（或身份表还没
             * 重建），它没有数据组、也就没有 Raft 日志可承载标记 —— 跳过，
             * 行为退回接线前，但要计数：混合写集有分叉窗口（§9.4 残留边界）。
             */
            if (gsid <= 0)
            {
                n_unmanaged++;
                continue;
            }

            (void) AppendDtxRecord(touched[i], DTX_PREPARE,
                                   dtx_pending_dtxid,
                                   0 /* coord_gsid：prepare 时 master 还没算出来 */,
                                   0 /* commit_ts */, 0 /* verdict */,
                                   NULL, 0, xid);

            /* 紧随其后的 PREPARE 标记：让 follower 的增强型 CLOG 记上这棵树 */
            if (prep_marker != NULL)
                PartWALAppendMarkerFor(touched[i], xid, XLOG_XACT_PREPARE,
                                       prep_marker, prep_marker_len);

            PartWALNoteTouchedPartition(touched[i]);
            gsids[ngsids++] = gsid;
        }
        PopActiveSnapshot();
        SPI_finish();
    }

    /*
     * 混合写集告警（DTX_2PC_DESIGN.md §9.4 残留边界）：本事务同时写了纳管
     * 分片与非纳管的分片形名表。非纳管部分归 master 本地提交管辖、纳管部分
     * 归协调组决议管辖 —— master 在决议 COMMIT 之后、本地 commit record
     * 落盘之前崩溃时两套规则给出相反答案。检测有边界：只有触达集合里的
     * 非纳管**分片形名**表可见；普通表的写入不进触达集合，测不到。
     */
    if (n_unmanaged > 0 && ngsids > 0)
        ereport(WARNING,
                (errmsg("pg_partdist: 分布式事务 %lld 混合了纳管与非纳管写入"
                        "（%d 个非纳管分区）",
                        (long long) dtx_pending_dtxid, n_unmanaged),
                 errdetail("非纳管部分不受协调组决议保护；master 在决议后、"
                           "本地提交前崩溃存在两套规则分叉的窗口。")));

    if (prep_marker != NULL)
        pfree(prep_marker);
    }

    /* 2) 再复制一轮，把 PREPARE 标记也推到多数派（失败即 ERROR，事务中止） */
    if (ngsids > 0)
        PartWALFlush(InvalidXLogRecPtr, false);

    /*
     * 3) 自治登记。必须在 prepare 记录落盘（[B]）之前完成：master 一旦收到
     * PREPARE 的成功应答就会来读这张表算写集，读不到就等于"本节点没写过任何
     * 纳管分片"，写集会缺一块，决议据此做出就是错的。
     *
     * ★ 只读参与者（ngsids == 0）也要登记，登记一个空数组。
     * 它不进写集（§8.3 的剔除照常成立，dtx_local_participant 返回空数组），
     * 但**必须**能收到 master 回填的 coord_gsid —— 否则本节点崩溃重启后，
     * 它这笔 prepared 事务既查不到协调组、也无从判断该提交还是回滚。
     * 它可能改过非纳管的表（reference 表 / 普通表），擅自回滚就是分叉。
     */
    if (dtx_note_hook_rv == NULL)
        dtx_note_hook_rv =
            find_rendezvous_variable("partdist_dtx_note_participant_hook");
    note = (DtxNoteParticipantHook) *dtx_note_hook_rv;

    if (note == NULL)
    {
        /*
         * 没装 pg_raft：没有数据组，也就没有 2PC 可言，行为同接线前。
         * 标记记录已经写进本地 parwal 流，是无害的冗余。
         */
        PartDistDtxReset();
        return;
    }

    ok = note(dtx_pending_dtxid, dtx_pending_gid, gsids, ngsids);
    if (!ok)
        ereport(ERROR,
                (errcode(ERRCODE_CONNECTION_EXCEPTION),
                 errmsg("pg_partdist: 分布式事务 %lld 的参与登记失败，中止 prepare",
                        (long long) dtx_pending_dtxid),
                 errdetail("登记是 master 侧算写集的唯一依据；宁可中止本事务，"
                           "也不能让协调者在写集不完整的情况下做提交决议。")));

    PartDistDtxReset();
}

/* ------------------------------------------------------------------ */
/* 阶段 3：COMMIT/ROLLBACK PREPARED 时补标记                            */
/* ------------------------------------------------------------------ */

void
PartDistDtxOnFinishPrepared(const char *gid, bool committed)
{
    int64          dtxid = 0;
    int64          coord_gsid = 0;
    int64          gsids[64];
    int            ngsids = 0;
    int            i;
    StringInfoData sql;
    bool           pushed = false;
    TransactionId  prepared_xid = InvalidTransactionId;
    char          *fin_marker = NULL;
    uint32         fin_marker_len = 0;

    if (!pg_partdist_dtx_2pc_enabled)
        return;
    if (!DtxidFromGid(gid, &dtxid))
        return;

    if (SPI_connect() != SPI_OK_CONNECT)
        return;
    if (!ActiveSnapshotSet())
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        pushed = true;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT coalesce(coord_gsid, 0), gsids "
                     "  FROM partdist.dtx_participant WHERE gid = %s",
                     quote_literal_cstr(gid));
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d;

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
        if (!isnull)
            coord_gsid = DatumGetInt64(d);

        d = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);
        if (!isnull)
        {
            ArrayType *arr = DatumGetArrayTypeP(d);
            Datum     *elems;
            bool      *nulls;
            int        n = 0;

            deconstruct_array(arr, INT8OID, 8, true, 'd', &elems, &nulls, &n);
            for (i = 0; i < n && ngsids < (int) lengthof(gsids); i++)
                if (!nulls[i])
                    gsids[ngsids++] = DatumGetInt64(elems[i]);
        }
    }
    pfree(sql.data);

    /*
     * ★ 被标记事务的**本地 top-level xid**（DTX_2PC_DESIGN.md §3.3 阶段 3）。
     *
     * 标记要落在那笔 prepared 事务头上，而不是 COMMIT PREPARED 这条语句
     * 自己的事务上 —— 两者是不同的 xid，用错了 follower 就把判决记到一个
     * 无关事务上，被标记的事务反而永远未决。
     *
     * 本钩子跑在 standard_ProcessUtility **之前**，此刻 prepared 事务仍在
     * pg_prepared_xacts 里，是最直接也最不需要额外持久化状态的取法。
     * 走 ::text::bigint 是为了不依赖 xid 类型的 Datum 表示。
     */
    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "SELECT transaction::text::bigint FROM pg_prepared_xacts "
                     " WHERE gid = %s", quote_literal_cstr(gid));
    if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
    {
        bool  isnull;
        Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                SPI_tuptable->tupdesc, 1, &isnull);

        if (!isnull)
            prepared_xid = (TransactionId) DatumGetInt64(d);
    }
    pfree(sql.data);

    /*
     * 载荷不带子事务清单：这里是**另一个事务**，xactGetCommittedChildren()
     * 返回的是它自己的（空）清单。被 prepared 事务的子事务已由第一段的
     * PREPARE 标记写成 TXN_PREPARED 且带 parent_xid，读路径会把它们解析到
     * 顶层的判决上（见 enhanced_clog.h）。
     */
    if (TransactionIdIsValid(prepared_xid))
        fin_marker = PartWALBuildMarkerPayload(false, committed, &fin_marker_len);

    for (i = 0; i < ngsids; i++)
    {
        Oid   local_oid;
        int64 gsid = gsids[i];

        local_oid = InvalidOid;
        initStringInfo(&sql);
        appendStringInfo(&sql,
                         "SELECT local_oid FROM partdist.shard_identity "
                         " WHERE global_shard_id = %lld", (long long) gsid);
        if (SPI_execute(sql.data, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
        {
            bool  isnull;
            Datum d = SPI_getbinval(SPI_tuptable->vals[0],
                                    SPI_tuptable->tupdesc, 1, &isnull);

            if (!isnull)
                local_oid = DatumGetObjectId(d);
        }
        pfree(sql.data);

        if (!OidIsValid(local_oid))
            continue;

        /*
         * 协调组跳过 DTX 记录：它的 DECISION{verdict=COMMIT} 对本组而言语义上
         * 就等于 DTX_COMMIT（§5.3 "省一轮"）。ABORT 路径下协调组通常连决议都
         * 没有（推定中止），补一条 DTX_ABORT 是有意义的，所以只在 COMMIT 时跳。
         */
        if (!(committed && gsid == coord_gsid))
            (void) AppendDtxRecord(local_oid,
                                   committed ? DTX_COMMIT : DTX_ABORT,
                                   dtxid, coord_gsid,
                                   committed ? TsoMarkerCommitTs() : 0,
                                   0, NULL, 0, InvalidTransactionId);

        /*
         * ★ 事务标记则**每个参与组都要写**，协调组也不例外。
         *
         * 上面那条"省一轮"只对 DTX 记录成立：DECISION 是 DTX 类记录，回放侧
         * 对它只推游标、不记账。真正让 follower 的增强型 CLOG 得出判决的只有
         * MARKER 类记录，协调组所在分片的副本同样需要它 —— 少写一条，那个
         * 分片上这笔事务就永远停在未决。
         */
        if (fin_marker != NULL)
            PartWALAppendMarkerFor(local_oid, prepared_xid,
                                   committed ? XLOG_XACT_COMMIT
                                             : XLOG_XACT_ABORT,
                                   fin_marker, fin_marker_len);

        PartWALNoteTouchedPartition(local_oid);
    }

    if (fin_marker != NULL)
        pfree(fin_marker);

    if (pushed)
        PopActiveSnapshot();
    SPI_finish();

    /*
     * 尽力复制（§3.3 阶段 3 明确说"随下一次 flush 复制"，不在客户端返回的
     * 关键路径上）：全局结果在阶段 2 已经持久化，标记只是让升主回放少问一次
     * 协调组。复制失败**绝不能**把 COMMIT PREPARED 带崩 —— 那才是真的丢可用性。
     */
    if (ngsids > 0)
    {
        PG_TRY();
        {
            PartWALFlush(InvalidXLogRecPtr, false);
        }
        PG_CATCH();
        {
            FlushErrorState();
            elog(LOG, "pg_partdist: dtx %lld 的阶段 3 标记复制失败，留待下次 flush 携带",
                 (long long) dtxid);
        }
        PG_END_TRY();
    }
}

void
PartDistDtxDefineGUCs(void)
{
    DefineCustomBoolVariable(
        "pg_partdist.dtx_2pc_enabled",
        "启用跨分区事务的 DTX-2PC 接线（参与登记 + 决议）。",
        "关闭后 PREPARE TRANSACTION 不再登记参与者、master 也不做决议，"
        "行为退回接线前（Citus 原生 2PC）。",
        &pg_partdist_dtx_2pc_enabled,
        true,
        PGC_SIGHUP,
        0,
        NULL, NULL, NULL);
}
