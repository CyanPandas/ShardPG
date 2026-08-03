/*
 * replay_worker.c
 *
 * 回放进程形态（FRD §7 + §13.10）与 SQL 边界函数。
 *
 *   launcher（静态 bgworker，每节点 1 个）
 *     ├─ 启动时扫描各 pg_parwal 子目录的 locmap/replay_enabled 恢复槽位
 *     ├─ 存在 enabled 槽位时拉起至多 replay_workers 个动态 worker
 *     └─ 周期回收死 worker 的陈旧认领
 *   worker（动态 bgworker，BGWORKER_SHMEM_ACCESS，无 DB 连接）
 *     ├─ InRecovery = true（进程级；扩展文件路径的 Assert 前提，FRD §2 事实4）
 *     ├─ 认领 = 槽位 CAS（claimed_by 0 → MyProcPid），退出路径释放
 *     └─ 对每个认领的 shard 循环：bound → ShardReplayRun → 更新 applied
 *
 * 补丁 0002 的豁免钩子也在本文件：判据 = 槽位里登记的本地副本文件号。
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

#include "shard_replay.h"
#include "shard_fileset.h"
#include "partition_wal.h"
#include "partition_wal_writer.h"
#include "enhanced_clog.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"       /* wal_segment_size */
#include "access/xlogutils.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/resowner.h"
#include "utils/wait_event.h"

/* ================================================================== */
/* GUC                                                                 */
/* ================================================================== */

int  replay_workers                = 1;
int  replay_naptime_ms             = 200;
int  replay_checkpoint_interval_ms = 2000;
int  replay_checkpoint_records     = 512;
bool replay_trust_local_segments   = false;
int  replay_debug_delay_ms         = 0;
bool replay_debug_trace            = false;

void
DefineReplayGUCs(void)
{
    DefineCustomIntVariable("pg_partdist.replay_workers",
                            "Replay worker 池大小（FRD §7：池 + 轮转认领）",
                            NULL, &replay_workers,
                            1, 1, 8,
                            PGC_POSTMASTER, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_partdist.replay_naptime_ms",
                            "回放循环空转间隔(ms)",
                            NULL, &replay_naptime_ms,
                            200, 10, 60000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_partdist.replay_checkpoint_interval_ms",
                            "apply checkpoint 时间阈值(ms)",
                            NULL, &replay_checkpoint_interval_ms,
                            2000, 100, 600000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_partdist.replay_checkpoint_records",
                            "apply checkpoint 记录数阈值",
                            NULL, &replay_checkpoint_records,
                            512, 1, 1000000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_partdist.replay_trust_local_segments",
                             "测试模式：本地段内容即回放上界（FRD §6）",
                             NULL, &replay_trust_local_segments,
                             false,
                             PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomIntVariable("pg_partdist.replay_debug_delay_ms",
                            "调试：worker 进入主循环前的等待(ms)，给 gdb attach 留窗口",
                            NULL, &replay_debug_delay_ms,
                            0, 0, 600000,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);
    DefineCustomBoolVariable("pg_partdist.replay_debug_trace",
                             "调试：回放路径面包屑日志",
                             NULL, &replay_debug_trace,
                             false,
                             PGC_SIGHUP, 0, NULL, NULL, NULL);
}

/* ================================================================== */
/* 共享内存                                                            */
/* ================================================================== */

#define REPLAY_CTL_SHMEM_NAME   "pg_partdist_replay_ctl"
#define REPLAY_LOCK_TRANCHE     "pg_partdist_replay"

ReplayCtlData *ReplayCtl = NULL;

void
RequestReplayShmem(void)
{
    RequestAddinShmemSpace(sizeof(ReplayCtlData));
    RequestNamedLWLockTranche(REPLAY_LOCK_TRANCHE, 1);
}

void
ReplayShmemInit(void)
{
    bool found;

    ReplayCtl = (ReplayCtlData *)
        ShmemInitStruct(REPLAY_CTL_SHMEM_NAME, sizeof(ReplayCtlData), &found);
    if (!found)
    {
        memset(ReplayCtl, 0, sizeof(ReplayCtlData));
        ReplayCtl->lock =
            &GetNamedLWLockTranche(REPLAY_LOCK_TRANCHE)[0].lock;
    }
}

/* ================================================================== */
/* 补丁 0002 豁免钩子（在**每个**会刷 shared buffer 的进程里跑）       */
/* ================================================================== */

bool
PartDistFlushExemptHook(const RelFileLocator *rlocator)
{
    int  i, j;
    bool hit = false;

    if (ReplayCtl == NULL || ReplayCtl->nreplicas == 0)
        return false;           /* 无副本：零成本快速返回 */

    LWLockAcquire(ReplayCtl->lock, LW_SHARED);
    for (i = 0; i < REPLAY_MAX_SHARDS && !hit; i++)
    {
        ReplayShardSlot *s = &ReplayCtl->slots[i];

        if (s->shard_oid == InvalidOid || s->nlocs == 0)
            continue;
        for (j = 0; j < s->nlocs; j++)
            if (s->locs[j] == rlocator->relNumber)
            {
                hit = true;
                break;
            }
    }
    LWLockRelease(ReplayCtl->lock);
    return hit;
}

/* ================================================================== */
/* 槽位管理                                                            */
/* ================================================================== */

static ReplayShardSlot *
ReplaySlotFindLocked(Oid shard_oid, bool create)
{
    int i;
    int free_idx = -1;

    for (i = 0; i < REPLAY_MAX_SHARDS; i++)
    {
        if (ReplayCtl->slots[i].shard_oid == shard_oid)
            return &ReplayCtl->slots[i];
        if (free_idx < 0 && ReplayCtl->slots[i].shard_oid == InvalidOid)
            free_idx = i;
    }
    if (!create)
        return NULL;
    if (free_idx < 0)
        ereport(ERROR,
                (errmsg("pg_partdist: 回放槽位已满(%d)", REPLAY_MAX_SHARDS)));

    memset(&ReplayCtl->slots[free_idx], 0, sizeof(ReplayShardSlot));
    ReplayCtl->slots[free_idx].shard_oid = shard_oid;
    return &ReplayCtl->slots[free_idx];
}

/* 从 locmap 文件把本地文件号灌进槽位（豁免钩子的数据源） */
static void
ReplaySlotLoadLocsLocked(ReplayShardSlot *s)
{
    char             path[MAXPGPATH];
    int              fd;
    ssize_t          nb;
    ReplayLocMapFile lm;
    int              i, j;

    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, s->shard_oid,
             REPLAY_LOCMAP_FILENAME);

    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        return;
    nb = read(fd, &lm, sizeof(lm));
    CloseTransientFile(fd);

    if (nb != (ssize_t) sizeof(lm) || lm.magic != REPLAY_LOCMAP_MAGIC ||
        lm.shard_oid != s->shard_oid ||
        lm.npairs < 1 || lm.npairs > SHARD_FILESET_MAX_RELS)
        return;

    s->nlocs = 0;
    for (i = 0; i < lm.npairs; i++)
    {
        for (j = 0; j < s->nlocs; j++)
            if (s->locs[j] == lm.pairs[i].local_loc.relNumber)
                break;
        if (j == s->nlocs && s->nlocs < SHARD_FILESET_MAX_RELS)
            s->locs[s->nlocs++] = lm.pairs[i].local_loc.relNumber;
    }
}

static void
ReplayRecountReplicasLocked(void)
{
    int i, n = 0;

    for (i = 0; i < REPLAY_MAX_SHARDS; i++)
        if (ReplayCtl->slots[i].shard_oid != InvalidOid &&
            ReplayCtl->slots[i].nlocs > 0)
            n++;
    ReplayCtl->nreplicas = n;
}

/* ================================================================== */
/* 触发式追平（惰性回放入口）                                          */
/* ================================================================== */

/*
 * ShardReplayCatchUp — 把 shard 追平到 bound，同步等待完成。
 *
 * 惰性回放的全部"何时回放"逻辑就在这里：**平时一条记录都不放**，
 * 只有被调用（升主 / 运维 / 测试）时才追。bound 由调用方给定 ——
 * 升主场景传该组的 Raft commit_index，天然不会碰到未提交条目。
 *
 * bound == 0 表示"追到本地已落盘的全部字节"（测试/运维便利；
 * 生产升主路径必须显式传 commit_index）。
 */
uint64
ShardReplayCatchUp(Oid shard_oid, uint64 bound, int timeout_ms)
{
    ReplayShardSlot *s;
    uint64           gen;
    uint64           applied = 0;
    int              waited = 0;
    char             errbuf[REPLAY_ERRMSG_LEN];

    if (ReplayCtl == NULL)
        ereport(ERROR, (errmsg("replay_catchup: 回放共享内存未初始化")));

    if (bound == 0)
        bound = GetLastWrittenPartitionLSN(shard_oid);

    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    s = ReplaySlotFindLocked(shard_oid, false);
    if (s == NULL || s->nlocs == 0)
    {
        LWLockRelease(ReplayCtl->lock);
        ereport(ERROR,
                (errmsg("replay_catchup: shard %u 尚未 replay_set_locmap",
                        shard_oid)));
    }
    if (!s->armed)
    {
        LWLockRelease(ReplayCtl->lock);
        ereport(ERROR,
                (errmsg("replay_catchup: shard %u 未 armed（replay_enable）",
                        shard_oid)));
    }

    /* 已经到位：直接返回，不惊动 worker */
    if (bound <= s->applied)
    {
        applied = s->applied;
        LWLockRelease(ReplayCtl->lock);
        return applied;
    }

    if (bound > s->target_plsn)
        s->target_plsn = bound;
    s->state = REPLAY_CATCHING_UP;
    s->errmsg[0] = '\0';
    s->generation++;
    gen = s->generation;
    LWLockRelease(ReplayCtl->lock);

    /* worker 以 replay_naptime_ms 轮询槽位，这里同步等它把 applied 抬上去 */
    for (;;)
    {
        bool done = false, failed = false;

        CHECK_FOR_INTERRUPTS();

        LWLockAcquire(ReplayCtl->lock, LW_SHARED);
        applied = s->applied;
        if (s->state == REPLAY_FAILED && s->generation >= gen)
        {
            failed = true;
            strlcpy(errbuf, s->errmsg, sizeof(errbuf));
        }
        else if (applied >= bound)
            done = true;
        LWLockRelease(ReplayCtl->lock);

        if (failed)
            ereport(ERROR,
                    (errmsg("replay_catchup: shard %u 追平失败: %s",
                            shard_oid, errbuf)));
        if (done)
            return applied;

        if (timeout_ms > 0 && waited >= timeout_ms)
            ereport(ERROR,
                    (errmsg("replay_catchup: shard %u 追平超时 "
                            "(%d ms, applied=%llu 目标=%llu)",
                            shard_oid, timeout_ms,
                            (unsigned long long) applied,
                            (unsigned long long) bound)));

        (void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         50, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        waited += 50;
    }
}

/* ================================================================== */
/* launcher                                                            */
/* ================================================================== */

void
RegisterReplayLauncher(void)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 5;
    snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_partdist");
    snprintf(worker.bgw_function_name, BGW_MAXLEN, "ReplayLauncherMain");
    snprintf(worker.bgw_name, BGW_MAXLEN, "pg_partdist replay launcher");
    snprintf(worker.bgw_type, BGW_MAXLEN, "pg_partdist replay launcher");
    RegisterBackgroundWorker(&worker);
}

/* 启动时从磁盘恢复槽位（locmap 存在 → 建槽；replay_enabled 存在 → 启用） */
static void
LauncherRecoverSlots(void)
{
    char           dirpath[MAXPGPATH];
    DIR           *dir;
    struct dirent *de;

    snprintf(dirpath, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);
    dir = AllocateDir(dirpath);
    if (dir == NULL)
        return;

    while ((de = ReadDir(dir, dirpath)) != NULL)
    {
        Oid   shard_oid;
        char *endptr;
        char  path[MAXPGPATH];
        struct stat st;
        ReplayShardSlot *s;

        if (de->d_name[0] == '.')
            continue;
        shard_oid = (Oid) strtoul(de->d_name, &endptr, 10);
        if (*endptr != '\0' || shard_oid == 0)
            continue;

        snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
                 DataDir, PARTITION_WAL_DIR, shard_oid,
                 REPLAY_LOCMAP_FILENAME);
        if (stat(path, &st) != 0)
            continue;

        LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
        s = ReplaySlotFindLocked(shard_oid, true);
        if (s->nlocs == 0)
            ReplaySlotLoadLocsLocked(s);

        snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
                 DataDir, PARTITION_WAL_DIR, shard_oid,
                 REPLAY_ENABLED_FILENAME);
        if (stat(path, &st) == 0)
            s->armed = true;    /* 只是"允许触发"，重启后不会自行回放 */

        ReplayRecountReplicasLocked();
        LWLockRelease(ReplayCtl->lock);
    }
    FreeDir(dir);
}

void
ReplayLauncherMain(Datum arg)
{
    int nworkers_started = 0;

    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    pqsignal(SIGHUP,  SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();

    LauncherRecoverSlots();

    ereport(LOG, (errmsg("pg_partdist replay launcher 启动"
                         "（池上限 %d worker）", replay_workers)));

    while (!ShutdownRequestPending)
    {
        int  i;
        bool have_enabled = false;

        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
        for (i = 0; i < REPLAY_MAX_SHARDS; i++)
        {
            ReplayShardSlot *s = &ReplayCtl->slots[i];

            if (s->shard_oid == InvalidOid)
                continue;

            /* 回收死 worker 的陈旧认领（§13.10：认领必须可回收） */
            if (s->claimed_by != 0 && kill(s->claimed_by, 0) != 0)
            {
                ereport(LOG,
                        (errmsg("pg_partdist replay: 回收 shard %u 的陈旧认领"
                                "（PID %d 已死）", s->shard_oid,
                                s->claimed_by)));
                s->claimed_by = 0;
                /* 追平中途死掉：状态置回 IDLE，等下一次触发重来
                 * （游标在 apply_checkpoint 里，重来只补未完成的部分） */
                if (s->state == REPLAY_CATCHING_UP)
                    s->state = REPLAY_IDLE;
            }
            if (s->armed)
                have_enabled = true;
        }
        LWLockRelease(ReplayCtl->lock);

        /* 有工作且池未满 → 拉起 worker（worker 自行认领并常驻） */
        while (have_enabled && nworkers_started < replay_workers)
        {
            BackgroundWorker        w;
            BackgroundWorkerHandle *h;

            memset(&w, 0, sizeof(w));
            /*
             * DATABASE_CONNECTION 不是为了访问 catalog（回放不需要），而是
             * 为了 BackgroundWorkerInitializeConnection(NULL) 走完 BaseInit：
             * 纯 SHMEM_ACCESS worker 没有 InitBufferPoolAccess/pgstat 初始化，
             * 第一次 ReadBuffer 就会段错误 —— 且 shmem worker 崩溃会把整个
             * 节点拖进 crash recovery（实测踩到）。
             */
            w.bgw_flags        = BGWORKER_SHMEM_ACCESS |
                                 BGWORKER_BACKEND_DATABASE_CONNECTION;
            w.bgw_start_time   = BgWorkerStart_RecoveryFinished;
            w.bgw_restart_time = BGW_NEVER_RESTART;    /* launcher 负责重拉 */
            snprintf(w.bgw_library_name, BGW_MAXLEN, "pg_partdist");
            snprintf(w.bgw_function_name, BGW_MAXLEN, "ReplayWorkerMain");
            snprintf(w.bgw_name, BGW_MAXLEN,
                     "pg_partdist replay worker %d", nworkers_started);
            snprintf(w.bgw_type, BGW_MAXLEN, "pg_partdist replay worker");
            w.bgw_notify_pid = MyProcPid;

            if (!RegisterDynamicBackgroundWorker(&w, &h))
            {
                ereport(WARNING,
                        (errmsg("pg_partdist replay: 无法拉起 worker"
                                "（max_worker_processes 不足？）")));
                break;
            }
            nworkers_started++;
        }

        /*
         * 粗粒度存活检查：认领全空但曾拉起过 worker → 可能全部退出，
         * 允许重拉（nworkers_started 归零的条件放宽到"无任何认领"）。
         */
        if (nworkers_started > 0)
        {
            bool any_claim = false;

            LWLockAcquire(ReplayCtl->lock, LW_SHARED);
            for (i = 0; i < REPLAY_MAX_SHARDS; i++)
                if (ReplayCtl->slots[i].claimed_by != 0)
                {
                    any_claim = true;
                    break;
                }
            LWLockRelease(ReplayCtl->lock);
            if (!any_claim)
                nworkers_started = 0;
        }

        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         1000, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
    }

    proc_exit(0);
}

/* ================================================================== */
/* worker                                                              */
/* ================================================================== */

static void
ReplayWorkerReleaseClaims(int code, Datum arg)
{
    int i;

    if (ReplayCtl == NULL)
        return;
    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    for (i = 0; i < REPLAY_MAX_SHARDS; i++)
        if (ReplayCtl->slots[i].claimed_by == MyProcPid)
            ReplayCtl->slots[i].claimed_by = 0;
    LWLockRelease(ReplayCtl->lock);
}

void
ReplayWorkerMain(Datum arg)
{
    ShardReplayCtx *ctxs[REPLAY_MAX_SHARDS] = {NULL};
    MemoryContext   work_cxt;

    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    pqsignal(SIGHUP,  SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();

    /*
     * dbname = NULL：不连任何库，但走完 InitPostgres/BaseInit ——
     * 缓冲管理（InitBufferPoolAccess）、pgstat、fd.c 都在这里初始化。
     * 回放只按 RelFileLocator 直接操作文件与共享缓冲，不需要 catalog。
     */
    BackgroundWorkerInitializeConnection(NULL, NULL, 0);

    /*
     * 回放全程不在事务里跑，CurrentResourceOwner 为 NULL —— 而 ReadBuffer
     * 的 pin 记账需要它（ResourceOwnerEnlargeBuffers 对 NULL 直接段错误，
     * 实测 ip 正落在该函数）。与 startup 进程做法一致：挂 AuxProcess
     * 资源属主，进程退出路径自动释放持有的 pin。
     */
    CreateAuxProcessResourceOwner();

    on_shmem_exit(ReplayWorkerReleaseClaims, (Datum) 0);

    /*
     * 恢复上下文（FRD §7.2 第 3 步）：满足 redo 扩展文件路径的
     * Assert(InRecovery)。合法性：本 worker 是所认领 shard 文件的唯一
     * 写入者（槽位排他认领），与 startup 进程独占恢复的前提等价。
     * 必须在 InitPostgres 之后再置位。
     */
    InRecovery = true;

    /*
     * rmgr 私有恢复状态初始化（镜像 StartupXLOG）。btree/gin/gist 的 redo
     * 入口第一件事就是 MemoryContextSwitchTo(opCtx)，而 opCtx 由各自的
     * rm_startup 创建 —— 不调它，btree_redo 会把 CurrentMemoryContext 切成
     * NULL，下一个 palloc 即段错误（实测：heap 记录正常、第一条 NEWROOT
     * 必崩，kern.log ip 落在 palloc）。
     */
    RmgrStartup();

    work_cxt = AllocSetContextCreate(TopMemoryContext,
                                     "shard replay work",
                                     ALLOCSET_DEFAULT_SIZES);

    ereport(LOG, (errmsg("pg_partdist replay worker 启动 (pid %d)",
                         MyProcPid)));

    if (replay_debug_delay_ms > 0)
    {
        ereport(LOG, (errmsg("pg_partdist replay worker 调试等待 %d ms",
                             replay_debug_delay_ms)));
        (void) WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         replay_debug_delay_ms, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
    }

    while (!ShutdownRequestPending)
    {
        int  i;
        bool did_work = false;

        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        /* 认领未被认领的 armed 槽位（CAS 语义：持锁检查 + 置位） */
        LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
        for (i = 0; i < REPLAY_MAX_SHARDS; i++)
        {
            ReplayShardSlot *s = &ReplayCtl->slots[i];

            if (s->shard_oid != InvalidOid && s->armed &&
                s->claimed_by == 0)
                s->claimed_by = MyProcPid;
        }
        LWLockRelease(ReplayCtl->lock);

        /* 处理自己认领的槽位 */
        for (i = 0; i < REPLAY_MAX_SHARDS; i++)
        {
            ReplayShardSlot *s = &ReplayCtl->slots[i];
            uint64           bound;

            if (s->shard_oid == InvalidOid ||
                s->claimed_by != MyProcPid)
                continue;

            if (!s->armed)
            {
                /* 释放已解除的认领 */
                LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
                if (s->claimed_by == MyProcPid)
                    s->claimed_by = 0;
                LWLockRelease(ReplayCtl->lock);
                continue;
            }

            /*
             * ★ 惰性核心：没有待办就什么都不做。
             * 平时 target_plsn 停在已追平的位置，worker 在这里直接跳过 ——
             * 副本上一条 redo 都不会发生，字节由 P2 的平凡 apply 负责落盘。
             * 只有 replay_catchup（升主/运维/测试）把 target 抬高才干活。
             */
            if (s->target_plsn <= s->applied && s->state != REPLAY_CATCHING_UP)
                continue;

            /* 惰性建 ctx（阶段一：checkpoint + locmap） */
            if (ctxs[i] == NULL || ctxs[i]->shard_oid != s->shard_oid)
            {
                MemoryContext        old;
                ShardReplayCtx      *ctx;
                ShardApplyCheckpoint chk;
                XidMapEntry         *xments = NULL;

                /*
                 * 槽位换了 shard：旧 ctx 的 xid_map 必须显式销毁。它建在
                 * TopMemoryContext 上，条目上限 100 万 × 48B ≈ 48MB —— 换几次
                 * shard 就把 worker 撑起来了（reader / ctx 本身也一并释放）。
                 */
                if (ctxs[i] != NULL)
                {
                    if (ctxs[i]->xid_map != NULL)
                        hash_destroy(ctxs[i]->xid_map);
                    if (ctxs[i]->reader != NULL)
                        XLogReaderFree(ctxs[i]->reader);
                    if (ctxs[i]->seg_fd >= 0)
                        CloseTransientFile(ctxs[i]->seg_fd);
                    pfree(ctxs[i]);
                    ctxs[i] = NULL;
                }

                old = MemoryContextSwitchTo(TopMemoryContext);
                ctx = palloc0(sizeof(ShardReplayCtx));
                ctx->shard_oid = s->shard_oid;
                ctx->seg_fd    = -1;

                if (!ShardReplayLoadLocMap(ctx))
                {
                    MemoryContextSwitchTo(old);
                    pfree(ctx);
                    ereport(WARNING,
                            (errmsg("pg_partdist replay: shard %u 无有效 "
                                    "locmap，解除 armed", s->shard_oid)));
                    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
                    s->armed = false;
                    s->state = REPLAY_FAILED;
                    strlcpy(s->errmsg, "无有效 locmap", REPLAY_ERRMSG_LEN);
                    if (s->claimed_by == MyProcPid)
                        s->claimed_by = 0;
                    LWLockRelease(ReplayCtl->lock);
                    continue;
                }

                ShardReplayInitXidMap(ctx);

                if (ReadApplyCheckpoint(ctx->shard_oid, &chk, &xments))
                {
                    ctx->durable_part_lsn  = chk.durable_part_lsn;
                    ctx->applied_part_lsn  = chk.durable_part_lsn;
                    ctx->max_orig_lsn      = chk.max_orig_lsn;
                    ctx->max_replayed_fxid =
                        FullTransactionIdFromU64(chk.max_replayed_fxid);

                    ShardReplayRestoreXidMap(ctx, xments, chk.nxidmap);
                    if (xments != NULL)
                        pfree(xments);
                }

                /*
                 * 启动即拉齐 nextXid（§7.2 第 2 步）：本地 nextXid 只在 PG 自身
                 * checkpoint 时随 pg_control 持久化，崩溃后可能回退到回放水位
                 * 之前 —— 那时本地新事务会分到已经被回放占用的 xid。
                 */
                ShardReplayAdvanceWatermark(ctx);

                ctx->reader = XLogReaderAllocate(wal_segment_size, NULL,
                                                 XL_ROUTINE(), NULL);
                if (ctx->reader == NULL)
                    ereport(ERROR,
                            (errmsg("pg_partdist replay: 无法分配 reader")));

                ctx->last_ckpt_time = GetCurrentTimestamp();
                ctxs[i] = ctx;
                MemoryContextSwitchTo(old);

                /* 认领时把持久化游标同步进槽位，供 catchup 的"已到位"判定 */
                LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
                s->applied = ctx->applied_part_lsn;
                LWLockRelease(ReplayCtl->lock);

                ereport(LOG,
                        (errmsg("pg_partdist replay: 认领 shard %u，"
                                "游标从 %llu 起（惰性：待触发）",
                                ctx->shard_oid,
                                (unsigned long long) ctx->applied_part_lsn)));
            }

            /* 断言认领权仍在手（FRD §13.10 单写者不变式） */
            Assert(s->claimed_by == MyProcPid);

            bound = s->target_plsn;
            if (bound > ctxs[i]->applied_part_lsn)
            {
                MemoryContext old = MemoryContextSwitchTo(work_cxt);
                bool           ok = true;

                /*
                 * 追平失败不能拖垮 worker（否则整个节点连带重置）：
                 * 捕获后落 FAILED + errmsg，等下一次触发重来。游标在
                 * apply_checkpoint 里，重来只补未完成的部分。
                 */
                PG_TRY();
                {
                    ShardReplayRun(ctxs[i], bound);
                }
                PG_CATCH();
                {
                    ErrorData *ed;

                    MemoryContextSwitchTo(old);
                    ed = CopyErrorData();
                    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
                    s->state = REPLAY_FAILED;
                    strlcpy(s->errmsg, ed->message, REPLAY_ERRMSG_LEN);
                    LWLockRelease(ReplayCtl->lock);
                    ereport(WARNING,
                            (errmsg("pg_partdist replay: shard %u 追平失败: %s",
                                    s->shard_oid, ed->message)));
                    FreeErrorData(ed);
                    FlushErrorState();
                    ok = false;
                }
                PG_END_TRY();

                MemoryContextSwitchTo(old);
                MemoryContextReset(work_cxt);
                did_work = ok;

                if (ok)
                {
                    /* 收尾 checkpoint：追平结束即持久化游标 */
                    ShardReplayDoCheckpoint(ctxs[i]);
                    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
                    s->applied = ctxs[i]->applied_part_lsn;
                    if (s->applied >= s->target_plsn)
                        s->state = REPLAY_IDLE;     /* 回到休眠 */
                    LWLockRelease(ReplayCtl->lock);

                    ereport(LOG,
                            (errmsg("pg_partdist replay: shard %u 追平至 %llu",
                                    s->shard_oid,
                                    (unsigned long long) ctxs[i]->applied_part_lsn)));
                }
                else
                {
                    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
                    s->applied = ctxs[i]->applied_part_lsn;
                    LWLockRelease(ReplayCtl->lock);
                }
            }
        }

        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         did_work ? 0 : replay_naptime_ms,
                         PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
    }

    RmgrCleanup();
    proc_exit(0);
}

/* ================================================================== */
/* SQL 边界函数                                                        */
/* ================================================================== */

PG_FUNCTION_INFO_V1(pg_partdist_register_shard_fileset);
PG_FUNCTION_INFO_V1(pg_partdist_shard_fileset);
PG_FUNCTION_INFO_V1(pg_partdist_replay_set_locmap);
PG_FUNCTION_INFO_V1(pg_partdist_replay_enable);
PG_FUNCTION_INFO_V1(pg_partdist_replay_disable);
PG_FUNCTION_INFO_V1(pg_partdist_replay_status);
PG_FUNCTION_INFO_V1(pg_partdist_replay_catchup);
PG_FUNCTION_INFO_V1(pg_partdist_gclog_status);

/*
 * gclog_status(node_id int, local_xid bigint)
 *   → (status text, start_ts bigint, commit_ts bigint)
 *
 * 增强型 CLOG 的核账入口。R3 的读路径会走 C 接口，这里只是把同一份账本
 * 暴露给验收用例 —— 「follower 上这笔事务到底判成什么了」必须能直接问出来，
 * 否则只能靠"多读出一行"这种间接现象反推。
 *
 * 从未写过的槽返回 running（稀疏文件空洞语义，= 未决 = 不可见）。
 */
Datum
pg_partdist_gclog_status(PG_FUNCTION_ARGS)
{
    int32               node_id   = PG_GETARG_INT32(0);
    int64               local_xid = PG_GETARG_INT64(1);
    GlobalTransactionId gxid;
    TxnStatus           status = TXN_RUNNING;
    uint64              start_ts = 0, commit_ts = 0;
    TupleDesc           tupdesc;
    Datum               values[3];
    bool                nulls[3] = {false, false, false};
    const char         *name;

    if (node_id < 0 || node_id > PG_UINT16_MAX)
        ereport(ERROR,
                (errmsg("pg_partdist: node_id %d 超出 gxid 的 16 位范围", node_id)));
    if (local_xid < 0 || (uint64) local_xid >= (UINT64CONST(1) << GXID_XID_BITS))
        ereport(ERROR,
                (errmsg("pg_partdist: local_xid %lld 超出 gxid 的 48 位范围",
                        (long long) local_xid)));

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR, (errmsg("pg_partdist: gclog_status 返回类型不是复合类型")));
    tupdesc = BlessTupleDesc(tupdesc);

    gxid = MakeGlobalXid((uint16) node_id, (TransactionId) local_xid);
    (void) EnhancedClogReadStatus(gxid, &status, &start_ts, &commit_ts);

    switch (status)
    {
        case TXN_RUNNING:   name = "running";   break;
        case TXN_PREPARED:  name = "prepared";  break;
        case TXN_COMMITTED: name = "committed"; break;
        case TXN_ABORTED:   name = "aborted";   break;
        default:            name = "unknown";   break;
    }

    values[0] = CStringGetTextDatum(name);
    values[1] = Int64GetDatum((int64) start_ts);
    values[2] = Int64GetDatum((int64) commit_ts);

    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * register_shard_fileset(regclass) → int
 * leader 侧：构建 + 注册 + 持久化该 shard 的 fileset（DDL 后强制刷新）。
 */
Datum
pg_partdist_register_shard_fileset(PG_FUNCTION_ARGS)
{
    Oid          relid = PG_GETARG_OID(0);
    ShardFileSet fs;
    int          n;

    n = BuildShardFileSet(relid, &fs);
    if (n < 0)
        ereport(ERROR,
                (errmsg("register_shard_fileset: 关系 %u 不存在", relid)));

    RegisterShardFileSet(&fs);
    PG_RETURN_INT32(n);
}

/*
 * shard_fileset(regclass) → SETOF (role int, ord int, spc oid, db oid, relnum oid)
 * 导出 fileset 描述（follower 侧 replay_set_locmap 的输入）。
 */
Datum
pg_partdist_shard_fileset(PG_FUNCTION_ARGS)
{
    Oid           relid = PG_GETARG_OID(0);
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    ShardFileSet  fs;
    int           n, i;

    InitMaterializedSRF(fcinfo, 0);

    n = BuildShardFileSet(relid, &fs);
    if (n < 0)
        ereport(ERROR,
                (errmsg("shard_fileset: 关系 %u 不存在", relid)));

    for (i = 0; i < n; i++)
    {
        Datum values[5];
        bool  nulls[5] = {false, false, false, false, false};

        values[0] = Int32GetDatum((int32) fs.rels[i].role);
        values[1] = Int32GetDatum((int32) fs.rels[i].ord);
        values[2] = ObjectIdGetDatum(fs.rels[i].loc.spcOid);
        values[3] = ObjectIdGetDatum(fs.rels[i].loc.dbOid);
        values[4] = ObjectIdGetDatum(fs.rels[i].loc.relNumber);

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
                             values, nulls);
    }

    PG_RETURN_NULL();
}

/*
 * replay_set_locmap(local regclass, roles int[], ords int[],
 *                   spcs oid[], dbs oid[], relnums oid[]) → int
 *
 * follower 侧：按 (role, ord) 把 leader fileset 与本地 shell 表的
 * fileset 配对，持久化 locmap，并把本地文件号登记进豁免槽位。
 */
Datum
pg_partdist_replay_set_locmap(PG_FUNCTION_ARGS)
{
    Oid        local_relid = PG_GETARG_OID(0);
    ArrayType *roles_a  = PG_GETARG_ARRAYTYPE_P(1);
    ArrayType *ords_a   = PG_GETARG_ARRAYTYPE_P(2);
    ArrayType *spcs_a   = PG_GETARG_ARRAYTYPE_P(3);
    ArrayType *dbs_a    = PG_GETARG_ARRAYTYPE_P(4);
    ArrayType *rels_a   = PG_GETARG_ARRAYTYPE_P(5);

    Datum     *roles, *ords, *spcs, *dbs, *rels;
    int        n1, n2, n3, n4, n5;
    ShardFileSet     local_fs;
    ReplayLocMapFile lm;
    int              i, j;
    char             path[MAXPGPATH];
    char             tmp[MAXPGPATH];
    int              fd;
    ssize_t          nb;

    deconstruct_array_builtin(roles_a, INT4OID, &roles, NULL, &n1);
    deconstruct_array_builtin(ords_a,  INT4OID, &ords,  NULL, &n2);
    deconstruct_array_builtin(spcs_a,  OIDOID,  &spcs,  NULL, &n3);
    deconstruct_array_builtin(dbs_a,   OIDOID,  &dbs,   NULL, &n4);
    deconstruct_array_builtin(rels_a,  OIDOID,  &rels,  NULL, &n5);

    if (n1 != n2 || n1 != n3 || n1 != n4 || n1 != n5 || n1 < 1 ||
        n1 > SHARD_FILESET_MAX_RELS)
        ereport(ERROR,
                (errmsg("replay_set_locmap: 数组长度不一致或超限")));

    if (BuildShardFileSet(local_relid, &local_fs) < 1)
        ereport(ERROR,
                (errmsg("replay_set_locmap: 本地关系 %u 不存在", local_relid)));

    /* 按 (role, ord) 配对（FRD §7.1："关系角色 + 索引定义序"） */
    memset(&lm, 0, sizeof(lm));
    lm.magic     = REPLAY_LOCMAP_MAGIC;
    lm.shard_oid = local_relid;
    lm.npairs    = 0;

    for (i = 0; i < n1; i++)
    {
        int32 role = DatumGetInt32(roles[i]);
        int32 ord  = DatumGetInt32(ords[i]);
        bool  matched = false;

        for (j = 0; j < local_fs.nrels; j++)
        {
            if ((int32) local_fs.rels[j].role == role &&
                (int32) local_fs.rels[j].ord == ord)
            {
                LocMapEntry *e = &lm.pairs[lm.npairs];

                e->leader_loc.spcOid    = DatumGetObjectId(spcs[i]);
                e->leader_loc.dbOid     = DatumGetObjectId(dbs[i]);
                e->leader_loc.relNumber = DatumGetObjectId(rels[i]);
                e->local_loc            = local_fs.rels[j].loc;
                lm.npairs++;
                matched = true;
                break;
            }
        }
        if (!matched)
            ereport(ERROR,
                    (errmsg("replay_set_locmap: leader (role=%d, ord=%d) 在"
                            "本地 shell 表 %u 上无对应关系 —— 两侧索引/TOAST "
                            "结构必须一致（FRD §13.2 同源物理基线）",
                            role, ord, local_relid)));
    }

    /* 持久化 locmap（tmp + fsync + rename） */
    InitPartitionWALDirectory(local_relid);
    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, local_relid, REPLAY_LOCMAP_FILENAME);
    snprintf(tmp, MAXPGPATH, "%s.tmp", path);

    fd = OpenTransientFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
    if (fd < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("replay_set_locmap: 无法创建 \"%s\": %m", tmp)));
    do {
        nb = write(fd, &lm, sizeof(lm));
    } while (nb < 0 && errno == EINTR);
    if (nb != (ssize_t) sizeof(lm) || pg_fsync(fd) != 0)
    {
        CloseTransientFile(fd);
        (void) unlink(tmp);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("replay_set_locmap: 写入 \"%s\" 失败: %m", tmp)));
    }
    CloseTransientFile(fd);
    if (rename(tmp, path) != 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("replay_set_locmap: 无法就位 \"%s\": %m", path)));

    /* 槽位登记（豁免钩子即刻生效；enabled 仍需 replay_enable） */
    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    {
        ReplayShardSlot *s = ReplaySlotFindLocked(local_relid, true);

        ReplaySlotLoadLocsLocked(s);
        ReplayRecountReplicasLocked();
    }
    LWLockRelease(ReplayCtl->lock);

    PG_RETURN_INT32(lm.npairs);
}

/*
 * replay_enable(regclass) / replay_disable(regclass)
 */
Datum
pg_partdist_replay_enable(PG_FUNCTION_ARGS)
{
    Oid  relid = PG_GETARG_OID(0);
    char path[MAXPGPATH];
    int  fd;
    ReplayShardSlot *s;

    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    s = ReplaySlotFindLocked(relid, false);
    if (s == NULL || s->nlocs == 0)
    {
        LWLockRelease(ReplayCtl->lock);
        ereport(ERROR,
                (errmsg("replay_enable: shard %u 尚未 replay_set_locmap",
                        relid)));
    }
    /*
     * armed 只表示"允许被触发"，**不会**自己开始回放（惰性语义）。
     * 真正的回放由 replay_catchup 触发。
     */
    s->armed = true;
    s->state = REPLAY_IDLE;
    s->errmsg[0] = '\0';
    LWLockRelease(ReplayCtl->lock);

    /* 持久化启用标记（节点重启后 launcher 自动恢复） */
    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, relid, REPLAY_ENABLED_FILENAME);
    fd = OpenTransientFile(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
    if (fd >= 0)
        CloseTransientFile(fd);

    PG_RETURN_BOOL(true);
}

Datum
pg_partdist_replay_disable(PG_FUNCTION_ARGS)
{
    Oid  relid = PG_GETARG_OID(0);
    char path[MAXPGPATH];
    ReplayShardSlot *s;

    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    s = ReplaySlotFindLocked(relid, false);
    if (s != NULL)
        s->armed = false;
    LWLockRelease(ReplayCtl->lock);

    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, relid, REPLAY_ENABLED_FILENAME);
    (void) unlink(path);

    PG_RETURN_BOOL(s != NULL);
}

/*
 * replay_status() → SETOF (shard oid, enabled bool, claimed_by int,
 *                          applied bigint, durable bigint, max_orig pg_lsn)
 */
Datum
pg_partdist_replay_status(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    int            i;
    static const char *state_name[] = {"idle", "catching_up", "failed"};

    InitMaterializedSRF(fcinfo, 0);

    LWLockAcquire(ReplayCtl->lock, LW_SHARED);
    for (i = 0; i < REPLAY_MAX_SHARDS; i++)
    {
        ReplayShardSlot     *s = &ReplayCtl->slots[i];
        ShardApplyCheckpoint chk;
        Datum values[8];
        bool  nulls[8];

        if (s->shard_oid == InvalidOid)
            continue;

        memset(nulls, 0, sizeof(nulls));
        values[0] = ObjectIdGetDatum(s->shard_oid);
        values[1] = BoolGetDatum(s->armed);
        values[2] = CStringGetTextDatum(
            (s->state >= 0 && s->state <= 2) ? state_name[s->state] : "?");
        values[3] = Int32GetDatum(s->claimed_by);
        values[4] = Int64GetDatum((int64) s->applied);
        values[5] = Int64GetDatum((int64) s->target_plsn);

        /* 只读游标，不需要 xid_map 快照 —— 传 NULL 免掉一次 palloc/pfree */
        if (ReadApplyCheckpoint(s->shard_oid, &chk, NULL))
        {
            values[6] = Int64GetDatum((int64) chk.durable_part_lsn);
            values[7] = LSNGetDatum(chk.max_orig_lsn);
        }
        else
        {
            values[6] = Int64GetDatum(0);
            nulls[7]  = true;
        }

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
                             values, nulls);
    }
    LWLockRelease(ReplayCtl->lock);

    PG_RETURN_NULL();
}

/*
 * replay_catchup(shard regclass, upto bigint DEFAULT NULL,
 *                timeout_ms int DEFAULT 300000) → bigint
 *
 * 惰性回放的触发入口。upto = NULL 表示"追到本地已落盘的全部字节"；
 * 生产升主路径必须显式传该组的 Raft commit_index。
 */
Datum
pg_partdist_replay_catchup(PG_FUNCTION_ARGS)
{
    Oid    relid = PG_GETARG_OID(0);
    uint64 bound = 0;
    int    timeout_ms = PG_ARGISNULL(2) ? 300000 : PG_GETARG_INT32(2);

    if (!PG_ARGISNULL(1))
    {
        int64 v = PG_GETARG_INT64(1);

        if (v < 0)
            ereport(ERROR, (errmsg("replay_catchup: upto 不能为负")));
        bound = (uint64) v;
    }

    PG_RETURN_INT64((int64) ShardReplayCatchUp(relid, bound, timeout_ms));
}
