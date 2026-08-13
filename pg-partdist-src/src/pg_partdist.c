#include "pg_partdist.h"
#include "metadata_cache.h"
#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif
#include <signal.h>
#include <unistd.h>
#include "write_router.h"
#include "partition_wal.h"
#include "partwal_sync.h"
#include "demux_worker.h"
#include "shard_replay.h"
#include "global_mvcc.h"
#include "dtx_participant.h"
#include "shard_xid.h"
#include "shard_visibility.h"
#include "tso.h"

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
            /*
             * 若本事务跑过会改 relfilenode 的 DDL，先把 fileset 变更发出去
             * （§12）：它内部会先排空旧文件号的记录，再追加 CTRL、灌新文件
             * 内容。放在下面这次 flush 之前，是为了让 COMMIT 标记仍然是本
             * 事务在流里的最后一条。
             */
            ShardFilesetMaybeEmitUpdates();

            /*
             * 冻结账目同步（§13 约束 5）。放在 fileset 之后：结构变更可能
             * 换出新的 TOAST 堆，先让 fileset 定下来再取它的 relfrozenxid。
             * 本函数自带时间间隔守卫，普通事务只多一次时间戳比较。
             */
            ShardFreezeMaybeEmitUpdates();

            /* 提交已成定局 → 连同 COMMIT 标记一起落盘 */
            PartWALFlush(InvalidXLogRecPtr, true);
            break;

        case XACT_EVENT_PRE_PREPARE:
            /*
             * DTX-2PC 参与者接线（DTX_2PC_DESIGN.md §3.3）：两个挂点必须
             * 夹住 PartWALFlush() —— Capture 在 flush 之前取触达集合快照，
             * Finish 在 flush 之后追加 DTX_PREPARE 标记、再复制一轮、并把
             * 本节点的写集自治登记出去。无 2PC 语境（gid 不认识 / 只读
             * 参与者）时两者都是空操作。
             *
             * write_marker=false：prepared 事务还可能 ROLLBACK PREPARED，
             * 此刻不能写 COMMITTED 标记（详见 partwal_sync.h 中 write_marker
             * 的说明）。字节照常落盘并复制 —— 物理回放本就不是事务性的，
             * 可见性由标记决定。
             */
            PartDistDtxPrePrepareCapture();
            PartWALFlush(InvalidXLogRecPtr, false);
            PartDistDtxPrePrepareFinish();
            break;

        case XACT_EVENT_ABORT:
            PartWALAbort();
            PartDistDtxReset();
            break;

        case XACT_EVENT_PREPARE:
            /* 事务真正结束：清掉"已落盘 LSN + 涉及分区"这套本地记账 */
            PartWALEndTxn();
            PartDistDtxReset();
            break;

        case XACT_EVENT_COMMIT:
            /* 事务真正结束：清掉"已落盘 LSN + 涉及分区"这套本地记账 */
            PartWALEndTxn();
            PartDistDtxReset();
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
    ShardFreezeNoteUserActivity();

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
 * UtilityMayChangeRelfilenode — 这条语句跑完之后，某个 shard 的 fileset
 * 有没有可能变了（FRD §12）。
 *
 * 判据故意是**语句类型**而不是"哪张表"：从 parse tree 精确解出受影响的
 * 关系要为每种语句写一套（AlterTableStmt 还得逐个子命令看），漏一种就是
 * 静默分歧。而真正决定要不要发记录的是 PRE_COMMIT 里那次 fileset diff ——
 * 这里只需要"可能变了"这个粗判，误报的代价不过是多走一次 catalog 遍历，
 * 而 DDL 本来就稀少。
 *
 * 名单来源：所有会走 RelationSetNewRelfilenumber() 的路径（TRUNCATE、
 * 重写类 ALTER TABLE、CLUSTER/VACUUM FULL、REINDEX）加上会新建/删除关系的
 * （CREATE/DROP INDEX、DROP TABLE）。VACUUM FULL 与 CLUSTER 不触发
 * ddl_command_end 事件触发器，这也是这里挂 ProcessUtility 而不是用事件
 * 触发器的原因。
 */
static bool
UtilityMayChangeRelfilenode(Node *parsetree)
{
    switch (nodeTag(parsetree))
    {
        case T_IndexStmt:           /* CREATE INDEX                       */
        case T_ReindexStmt:         /* REINDEX                            */
        case T_ClusterStmt:         /* CLUSTER                            */
        case T_VacuumStmt:          /* VACUUM FULL（普通 VACUUM 会 diff 掉）*/
        case T_TruncateStmt:        /* TRUNCATE                           */
        case T_AlterTableStmt:      /* 重写类 ALTER / ADD CONSTRAINT      */
        case T_DropStmt:            /* DROP INDEX / DROP TABLE            */
            return true;
        default:
            return false;
    }
}

/*
 * partdist_process_utility — ProcessUtility_hook wrapper.
 *
 * For COPY FROM statements, calls pg_partdist_process_utility BEFORE the
 * chain so that the target shard is registered in the relfilenode hash
 * before the COPY writes any WAL records.
 *
 * 语句执行**之后**再判 fileset 是否可能变化（§12）：此刻新 relfilenode
 * 已经存在，PRE_COMMIT 的 diff 才算得出来。
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
    ShardFreezeNoteUserActivity();

    /* Register COPY FROM target BEFORE the chain writes WAL */
    pg_partdist_process_utility(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv, dest, qc);

    /*
     * TX-TSO-MVCC P1：VACUUM/ANALYZE/CLUSTER 点到分片打标表一律拦下 ——
     * 原生 clog 会误判分片 xid，回收/重写路径会删活元组（P1_PRECHECK 结论 D）。
     * 白名单为空时一次指针比较即返回。
     */
    if (pstmt->utilityStmt != NULL)
        ShardXidUtilityGuard(pstmt->utilityStmt);

    /*
     * DTX-2PC：截下 PREPARE TRANSACTION '<gid>'。这是**唯一**能同时看到 gid
     * 和本事务触达集合的位置 —— PRE_PREPARE 回调里拿不到 gid（prepareGID 是
     * xact.c 的 static），而语句执行完事务就已经 prepared 了。
     */
    if (pstmt->utilityStmt != NULL && IsA(pstmt->utilityStmt, TransactionStmt))
    {
        TransactionStmt *tstmt = (TransactionStmt *) pstmt->utilityStmt;

        if (tstmt->kind == TRANS_STMT_PREPARE)
            PartDistDtxNotePrepareGid(tstmt->gid);
        /*
         * 阶段 3（§3.3）：master 收到决议后本来就要对每个参与者发
         * COMMIT/ROLLBACK PREPARED，在这条语句执行**前**顺带把
         * DTX_COMMIT/DTX_ABORT 标记补进本节点各触达组的 parwal 流 ——
         * 零额外往返，且此时事务上下文还在，SPI 可用。
         */
        else if (tstmt->kind == TRANS_STMT_COMMIT_PREPARED ||
                 tstmt->kind == TRANS_STMT_ROLLBACK_PREPARED)
            PartDistDtxOnFinishPrepared(tstmt->gid,
                                        tstmt->kind == TRANS_STMT_COMMIT_PREPARED);
    }

    /* Execute the statement via the existing chain */
    if (prev_ProcessUtility_hook)
        prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree,
                                 context, params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree,
                                context, params, queryEnv, dest, qc);

    /* §12：可能动了 relfilenode → 让 PRE_COMMIT 去 diff 一次 fileset */
    if (pstmt->utilityStmt != NULL &&
        UtilityMayChangeRelfilenode(pstmt->utilityStmt))
        ShardFilesetNoteMaybeChanged();
}

/* ---- SIGSEGV 诊断 ---- */

/*
 * PostgreSQL 不装 SIGSEGV 处理器，崩溃进程只在 postmaster 日志里留一行
 *     server process (PID nnn) was terminated by signal 11
 * ——没有栈、没有语句、没有任何指向。而本环境的 core_pattern 是管道到 apport，
 * 容器里拿不到 core。结果就是"看得见崩、看不见在哪崩"，只能靠猜；这套模块
 * 排查 D2 时因此连续四轮误判。
 *
 * 这个处理器把栈打进 stderr（即服务器日志），然后交还默认处置让 postmaster
 * 照常看到真正的 signal 11 —— 崩溃语义一点不变，只是多留一份现场。
 * backtrace_symbols_fd 是 async-signal-safe 的那个变体（不 malloc）。
 * 后端二进制以 --export-dynamic 链接，所以能出函数名。
 */
static bool debug_segv_backtrace = false;

#ifdef HAVE_EXECINFO_H
static void
partdist_segv_handler(int signum)
{
    void *frames[64];
    int   n;
    char  hdr[128];
    int   len;

    len = snprintf(hdr, sizeof(hdr),
                   "pg_partdist: 收到信号 %d，pid %d 栈回溯：\n",
                   signum, (int) getpid());
    if (len > 0)
    {
        ssize_t rc = write(STDERR_FILENO, hdr, (size_t) len);

        (void) rc;      /* 信号处理器里写不出去也没别的办法 */
    }

    n = backtrace(frames, (int) lengthof(frames));
    backtrace_symbols_fd(frames, n, STDERR_FILENO);

    /* 交还默认处置：postmaster 仍看到真正的 signal，崩溃语义不变 */
    signal(signum, SIG_DFL);
    raise(signum);
}
#endif

/* ---- module load ---- */

void
_PG_init(void)
{
    if (!process_shared_preload_libraries_in_progress)
        return;

    DefineCustomBoolVariable(
        "pg_partdist.debug_segv_backtrace",
        "崩溃（SIGSEGV/SIGBUS/SIGILL）时把栈回溯打进服务器日志。",
        "core_pattern 被 apport 接管、容器内拿不到 core 时的替代手段。",
        &debug_segv_backtrace,
        false,
        PGC_POSTMASTER,
        0,
        NULL, NULL, NULL
    );

#ifdef HAVE_EXECINFO_H
    if (debug_segv_backtrace)
    {
        /* 处理器随 fork 继承，因此在 postmaster 装一次即覆盖全部后端 */
        signal(SIGSEGV, partdist_segv_handler);
        signal(SIGBUS,  partdist_segv_handler);
        signal(SIGILL,  partdist_segv_handler);
    }
#endif

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

    /* GUC: gxid 的来源节点号（与上面的路由层 local_node_id 不是一回事） */
    DefineGlobalMVCCGUCs();

    /*
     * GUC: DDL 换了文件号之后，最多把多少个块以 FPI 形式灌进分区流（§12）。
     * 超过则只发结构变更通知，副本需重做物理基线 —— 与其让一次 VACUUM FULL
     * 把几十 GB 塞进 Raft 日志，不如显式退回基线拷贝。
     */
    DefineCustomIntVariable(
        "pg_partdist.fileset_inline_max_blocks",
        "DDL 变更 fileset 后，随控制记录灌入分区流的新文件块数上限。",
        "超过此值只发结构变更通知（NEEDS_REBASELINE），副本须重做物理基线。",
        &fileset_inline_max_blocks,
        131072,             /* 1 GB */
        0,
        INT_MAX,
        PGC_SUSET,
        0,
        NULL, NULL, NULL
    );

    /*
     * GUC: 两次冻结账目检查的最小间隔（§13 约束 5）。relfrozenxid 是以千万
     * xid 为尺度变化的慢变量，分钟级滞后毫无影响；设 0 表示每个事务都查，
     * 仅供验收用例使用。
     */
    DefineCustomIntVariable(
        "pg_partdist.freeze_sync_interval_ms",
        "两次把 leader 的 relfrozenxid 同步给副本的检查之间的最小间隔。",
        "0 = 每个事务都检查（测试用）。autovacuum 推进 relfrozenxid 不走 "
        "ProcessUtility，所以这条同步是时间驱动而非 DDL 驱动。",
        &freeze_sync_interval_ms,
        60000,
        0,
        INT_MAX,
        PGC_SUSET,
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

    /* DTX-2PC 接线总开关 */
    PartDistDtxDefineGUCs();

    /*
     * TX-TSO-MVCC P1（T1.1–T1.3）：白名单 GUC（钩子与回调的安装在下面，
     * 必须晚于 PartWALXactCallback 注册，见彼处注释）。白名单默认为空 ⇒
     * 钩子首个比较即返回，既有行为零变化。
     */
    ShardXidDefineGUCs();

    /*
     * TX-TSO-MVCC P1（T1.6–T1.8）：补丁 0006 的可见性分叉钩子 + 安全网
     * GUC。同样白名单为空即零变化（is_shard_rel 一次比较返回 false）。
     */
    ShardVisibilityDefineGUCs();
    TsoDefineGUCs();
    TsoClientDefineGUCs();
    ShardVisibilityInstallHooks();

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

    /*
     * T4.4：决议点取号出口。pg_raft 的 dtx_master_pre_record_commit 在
     * "全部 PREPARE 已成功"点取 commit_ts（§2-4 两时机；未配置 TSO 返回 0，
     * 决议侧回退本地时钟）。经 rendezvous variable 传递，无编译期依赖。
     */
    {
        void **rv = find_rendezvous_variable("partdist_tso_dtx_decision_ts_fn");

        *rv = (void *) PartDistTsoDtxDecisionTs;
    }

    /* Register Demux background worker for crash recovery at startup */
    RegisterDemuxWorker();
    TsoRegisterHeartbeatWorker();   /* T3.5：GlobalSafeTs 心跳/续租 */

    /* Transaction callback: write PartWAL at PRE_COMMIT, discard on ABORT */
    RegisterXactCallback(PartWALXactCallback, NULL);

    /*
     * TX-TSO-MVCC P1：分片 xid 打标钩子 + 事务回调。**必须在
     * PartWALXactCallback 之后注册**——回调按 LIFO 调用，后注册者先执行：
     * PRE_PREPARE 的"含分片写禁 PREPARE"禁令要在 PartWAL 把字节刷进
     * 分区流（并触发 raft 复制）之前发作，否则被判死刑的事务字节已进流，
     * 中止还会打断在途复制，让 leader 本地 plsn 跑到多数派已提交位点前头
     * （T1.9 实测：follower 追平永远差一条）。
     */
    ShardXidInstallHook();
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
