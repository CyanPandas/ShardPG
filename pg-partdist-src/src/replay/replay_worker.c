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
#include "shard_xid.h"                 /* T5.4b-2：vacuum 水位落盘 */

#include "access/heapam.h"              /* heap_inplace_update */
#include "access/relation.h"
#include "access/table.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"       /* wal_segment_size */
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "common/file_utils.h"          /* — */
#include "utils/syscache.h"             /* SearchSysCacheExists1（回收用） */
#include "storage/fd.h"
#include "postmaster/bgwriter.h"        /* RequestCheckpoint */
#include "utils/syscache.h"
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
int  replay_reclaim_grace_secs     = 300;
bool replay_debug_trace            = false;

void
DefineReplayGUCs(void)
{
    DefineCustomIntVariable("pg_partdist.replay_reclaim_grace_secs",
                            "回收陈旧 pg_parwal 目录前的宽限期（秒）。",
                            "判据是\"目录名那个 OID 在 pg_class 里已不存在\"；"
                            "宽限期只为避开\"关系刚建、目录已在\"这类窄窗口。"
                            "设 0 表示不等待。",
                            &replay_reclaim_grace_secs, 300, 0, 86400,
                            PGC_SIGHUP, 0, NULL, NULL, NULL);

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
                (errmsg("pg_partdist: 回放槽位已满(%d)", REPLAY_MAX_SHARDS),
                 errhint("陈旧槽位（壳表已 DROP 但槽位未释放）可用 "
                         "partdist.replay_reclaim_stale() 回收；"
                         "replay_set_locmap() 已在报本错之前自动试过一次。")));

    memset(&ReplayCtl->slots[free_idx], 0, sizeof(ReplayShardSlot));
    ReplayCtl->slots[free_idx].shard_oid = shard_oid;
    return &ReplayCtl->slots[free_idx];
}

static void ReplaySlotLoadLocsLocked(ReplayShardSlot *s);
static ReplayShardSlot *ReplaySlotFindLocked(Oid shard_oid, bool create);

/*
 * 把 worker 发布在槽位里的冻结账目写进本地 pg_class（§13 约束 5，D2）。
 *
 * **必须在普通 backend 里调用**：replay worker 的连接没有选数据库，
 * 碰 pg_class 会 FATAL 并把它拖进崩溃重启循环。所以落点选在
 * replay_catchup() 的调用方 —— 惰性形态下回放只可能由它触发，一定有这么
 * 一个有数据库、有事务的调用方在。
 *
 * 为什么搬 leader 的原值是**真话**而不是编造：relfrozenxid = X 的语义是
 * "本关系内不存在比 X 更老的未冻结 xid"。副本堆页与 leader 逐字节一致，
 * 同一句话在本节点同样成立 —— 这正是本方案区别于"直接把本地值往前推"
 * （写一个明知为假的目录字段）的地方。
 *
 * 用 heap_inplace_update 而非普通 UPDATE，与内核 vac_update_relstats 同款：
 * pg_class 的这几个字段是非事务性的账目字段。
 */
static void
ReplayDrainAndApplyFreeze(Oid shard_oid)
{
    PartWALFreezeEntry ents[SHARD_FILESET_MAX_RELS];
    int                n = 0;
    int                i;
    bool               have_wm = false;
    TransactionId      wm_tb = InvalidTransactionId;
    TransactionId      wm_vx = InvalidTransactionId;
    Relation           classRel;
    Oid                toastoid = InvalidOid;
    Relation           shell;

    /* 取走并清空，避免下一次 catchup 重复写 */
    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    for (i = 0; i < REPLAY_MAX_SHARDS; i++)
    {
        ReplayShardSlot *s = &ReplayCtl->slots[i];

        if (s->shard_oid != shard_oid)
            continue;
        if (s->freeze_n > 0)
        {
            n = s->freeze_n;
            memcpy(ents, s->freeze, (size_t) n * sizeof(PartWALFreezeEntry));
            s->freeze_n = 0;
        }
        if (s->vacuum_wm_valid)
        {
            wm_tb = s->vacuum_trunc_before;
            wm_vx = s->vacuum_xid;
            have_wm = true;
            s->vacuum_wm_valid = false;
        }
        break;
    }
    LWLockRelease(ReplayCtl->lock);

    /*
     * T5.4b-2（设计 §6.7）：把 leader 的分片 vacuum 两水位落进本节点的
     * pg_shard_xid/<oid> —— 与 leader 同一个存储位，升主时 shard_xid 的
     * 建槽路径原样读得到，不需要额外的交接协议。
     *
     * 搬 leader 的原值同样是**真话**（与 relfrozenxid 同款论证）：
     * "xid < clog_truncate_before 的分片事务都已提交且早于 GlobalSafeTs"
     * 这句话，在页面逐字节一致的副本上同样成立。
     *
     * 放在冻结账目之前处理：水位是可见性解释规则的输入，早一步到位没有坏处。
     */
    if (have_wm)
    {
        ShardVacuumSetWatermarks(shard_oid, wm_tb, wm_vx);
        ereport(DEBUG1,
                (errmsg("pg_partdist replay: shard %u vacuum 水位已落盘（%u/%u）",
                        shard_oid, wm_tb, wm_vx)));
    }

    if (n == 0)
        return;

    shell = try_relation_open(shard_oid, AccessShareLock);
    if (shell == NULL)
        return;
    toastoid = shell->rd_rel->reltoastrelid;
    relation_close(shell, AccessShareLock);

    classRel = table_open(RelationRelationId, RowExclusiveLock);

    for (i = 0; i < n; i++)
    {
        Oid           relid;
        HeapTuple     ctup;
        Form_pg_class pgcform;

        relid = (ents[i].role == SHARD_REL_MAIN) ? shard_oid : toastoid;
        if (!OidIsValid(relid))
            continue;

        ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
        if (!HeapTupleIsValid(ctup))
            continue;           /* 关系没了；leader 下次变化时会重发 */

        pgcform = (Form_pg_class) GETSTRUCT(ctup);
        if (pgcform->relfrozenxid != (TransactionId) ents[i].relfrozenxid ||
            pgcform->relminmxid != (MultiXactId) ents[i].relminmxid)
        {
            pgcform->relfrozenxid = (TransactionId) ents[i].relfrozenxid;
            pgcform->relminmxid   = (MultiXactId) ents[i].relminmxid;
            heap_inplace_update(classRel, ctup);
        }
        heap_freetuple(ctup);
    }

    table_close(classRel, RowExclusiveLock);

    ereport(DEBUG1,
            (errmsg("pg_partdist replay: shard %u 冻结账目已写入 pg_class"
                    "（%d 条）", shard_oid, n)));
}

void
ReplaySlotRefreshLocs(Oid shard_oid)
{
    ReplayShardSlot *s;

    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    s = ReplaySlotFindLocked(shard_oid, false);
    if (s != NULL)
        ReplaySlotLoadLocsLocked(s);
    LWLockRelease(ReplayCtl->lock);
}

/* 从 locmap 文件把本地文件号灌进槽位（豁免钩子的数据源） */
static void
ReplaySlotLoadLocsLocked(ReplayShardSlot *s)
{
    ReplayLocMapFile lm;
    int              i, j;

    if (!ShardReplayReadLocMap(s->shard_oid, &lm, NULL))
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
        bool done = false, failed = false, fenced = false;

        CHECK_FOR_INTERRUPTS();

        LWLockAcquire(ReplayCtl->lock, LW_SHARED);
        applied = s->applied;
        if ((s->state == REPLAY_FAILED || s->state == REPLAY_NEEDS_STRUCT) &&
            s->generation >= gen)
        {
            failed = (s->state == REPLAY_FAILED);
            fenced = (s->state == REPLAY_NEEDS_STRUCT);
            strlcpy(errbuf, s->errmsg, sizeof(errbuf));
        }
        else if (applied >= bound)
            done = true;
        LWLockRelease(ReplayCtl->lock);

        if (failed)
            ereport(ERROR,
                    (errmsg("replay_catchup: shard %u 追平失败: %s",
                            shard_oid, errbuf)));
        /*
         * 栅栏也要立刻返回，不能干等到超时 —— 它要等的是人工动作，
         * 而超时报出来的会是"追平超时"，把原因盖掉。
         */
        if (fenced)
            ereport(ERROR,
                    (errmsg("replay_catchup: shard %u 停在结构栅栏（已追至 "
                            "%llu）: %s", shard_oid,
                            (unsigned long long) applied, errbuf),
                     errhint("本地 shell 表做等价结构变更后重跑 "
                             "replay_set_locmap()，再重新触发 replay_catchup()。")));
        if (done)
        {
            ReplayDrainAndApplyFreeze(shard_oid);
            return applied;
        }

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
    /*
     * 池上限必须按**当前活着的 worker 数**来限，不能用"本 launcher 启动过
     * 几个"的计数器。
     *
     * 旧写法是 nworkers_started 只增，再配一段"任何槽位都没被认领就把它归零"
     * 的放宽 —— 两者合起来是个泄漏：worker 是常驻的（主循环只在节点关机时
     * 退出），而 replay_disable 会让它释放认领；于是每经历一次
     * arm → disable → arm，launcher 就以为池空了，再拉一个**永久** worker。
     *
     * 实测证据：replay_workers=1 的节点上同时活着 4 个 worker，且全部叫
     * "replay worker 0"（计数器被反复清零留下的直接痕迹）；本环境 08-08 的
     * 节点日志里已经刷出过 "max_worker_processes 不足" —— 一旦撞上限，
     * RegisterDynamicBackgroundWorker 会一直失败，该节点**再也拉不起任何
     * 回放 worker**，follower 从此静默停止追平（且会连带饿死 Citus
     * 维护进程等其他动态 bgworker）。
     */
#define REPLAY_LAUNCH_MAX 64
    BackgroundWorkerHandle *handles[REPLAY_LAUNCH_MAX];
    int nhandles       = 0;   /* 存活 worker 数（句柄数） */
    int next_worker_id = 0;   /* 只增，仅用于 bgw_name，便于区分世代 */

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

        /* 回收已退出 worker 的句柄；回收完 nhandles 就是当前存活数 */
        {
            int   k = 0;
            pid_t wpid;

            for (i = 0; i < nhandles; i++)
            {
                if (GetBackgroundWorkerPid(handles[i], &wpid) == BGWH_STOPPED)
                    pfree(handles[i]);
                else
                    handles[k++] = handles[i];
            }
            nhandles = k;
        }

        /* 有工作且池未满 → 拉起 worker（worker 自行认领并常驻） */
        while (have_enabled && nhandles < replay_workers &&
               nhandles < REPLAY_LAUNCH_MAX)
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
                     "pg_partdist replay worker %d", next_worker_id);
            snprintf(w.bgw_type, BGW_MAXLEN, "pg_partdist replay worker");
            w.bgw_notify_pid = MyProcPid;

            {
                MemoryContext oldcxt;
                bool          ok;

                /* 句柄要跨轮存活，必须分配在长生命周期的上下文里 */
                oldcxt = MemoryContextSwitchTo(TopMemoryContext);
                ok = RegisterDynamicBackgroundWorker(&w, &h);
                MemoryContextSwitchTo(oldcxt);

                if (!ok)
                {
                    ereport(WARNING,
                            (errmsg("pg_partdist replay: 无法拉起 worker"
                                    "（max_worker_processes 不足？）")));
                    break;
                }
            }
            handles[nhandles++] = h;
            next_worker_id++;
        }

        /*
         * 这里原本还有一段"认领全空就把池计数归零"的放宽。它是上面那个泄漏的
         * 另一半：worker 常驻不退出，而 disable 会释放认领，于是"认领全空"根本
         * 不等于"worker 都退出了"。改成按句柄数真实统计存活后，这段放宽既无必要
         * 也有害，删掉。worker 死掉的情形由上面的句柄回收（BGWH_STOPPED）覆盖，
         * 槽位上的陈旧认领仍由本循环开头那段 kill(pid,0) 检查回收。
         */

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

            /*
             * 停在结构栅栏上的 shard 同样不再干活：它缺的是人工结构变更，
             * 每个 naptime 重试一次只会把同一条 LOG 刷满日志。重新触发
             * replay_catchup() 会把状态改回 CATCHING_UP，自然解除。
             */
            if (s->state == REPLAY_NEEDS_STRUCT)
                continue;

            /* 惰性建 ctx（阶段一：checkpoint + locmap） */
            if (ctxs[i] == NULL || ctxs[i]->shard_oid != s->shard_oid ||
                ctxs[i]->locmap_gen != s->locmap_gen)
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
                ctx->shard_oid  = s->shard_oid;
                ctx->seg_fd     = -1;
                ctx->locmap_gen = s->locmap_gen;

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
                    if (ctxs[i]->needs_struct)
                    {
                        /*
                         * 结构栅栏（§12）：游标停在那条 FILESET_UPDATE 之前，
                         * 已应用的部分是完整的。落 NEEDS_STRUCT 而不是 FAILED，
                         * 是为了让运维一眼看出"补个结构就能原地继续"。
                         */
                        s->state = REPLAY_NEEDS_STRUCT;
                        strlcpy(s->errmsg, ctxs[i]->struct_errmsg,
                                REPLAY_ERRMSG_LEN);
                    }
                    else if (s->applied >= s->target_plsn)
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
PG_FUNCTION_INFO_V1(pg_partdist_replay_locmap);
PG_FUNCTION_INFO_V1(pg_partdist_replay_enable);
PG_FUNCTION_INFO_V1(pg_partdist_replay_disable);
PG_FUNCTION_INFO_V1(pg_partdist_replay_status);

/* ================================================================== */
/* 陈旧槽位与目录的回收                                                 */
/* ================================================================== */

/*
 * ReplayReclaimStale — 回收"关系已经不存在了"的回放槽位与 pg_parwal 目录。
 *
 * **要解决的问题**：槽位与目录都只增不减。壳表 DROP 之后槽位不释放、
 * pg_parwal/<oid>/ 也不删，launcher 重启还会从这些目录把槽位重建出来。
 * REPLAY_MAX_SHARDS 是 64，占满之后 replay_set_locmap() 直接报"回放槽位已满"，
 * 新副本一个都建不了；目录则会攒到几百个（实测一天密集测试后每节点 143–148 个）。
 *
 * **判据只有一条：该 OID 在 pg_class 里已经不存在。**
 * 目录名就是本地 shard（或副本壳表）的 OID，关系还在就说明这份流仍有主；
 * 关系没了，流就是垃圾 —— 无论本节点对该分片是 primary 还是 secondary，
 * 判据都一样。
 *
 * 三条安全约束：
 *   a) **必须跑在有目录访问的 backend 里**（launcher 只有 SHMEM_ACCESS，
 *      查不了 pg_class，所以这件事做不进 launcher）；
 *   b) **被认领的槽位一律不动** —— worker 可能正在其上回放；
 *   c) **宽限期**：只删 mtime 早于 grace_secs 的目录。目录是在第一条记录落盘时
 *      创建的、彼时关系必然存在，所以窗口很窄；宽限期是廉价保险，
 *      免得撞上"关系刚建、尚未对本会话可见"这类边角。
 *
 * 返回释放的槽位数与删除的目录数。
 */
void
ReplayReclaimStale(int grace_secs, int *slots_freed, int *dirs_removed)
{
    char           dirpath[MAXPGPATH];
    DIR           *dir;
    struct dirent *de;
    time_t         now = time(NULL);
    int            nslots = 0;
    int            ndirs = 0;
    int            i;

    if (ReplayCtl == NULL)
        ereport(ERROR, (errmsg("replay_reclaim_stale: 回放共享内存未初始化")));

    /* ---- 1) 释放槽位：关系已不存在、且无人认领 ---- */
    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    for (i = 0; i < REPLAY_MAX_SHARDS; i++)
    {
        ReplayShardSlot *s = &ReplayCtl->slots[i];

        if (s->shard_oid == InvalidOid)
            continue;
        if (s->claimed_by != 0 && kill(s->claimed_by, 0) == 0)
            continue;                   /* 有活着的 worker 认领着，不动 */
        if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(s->shard_oid)))
            continue;                   /* 关系还在 */

        ereport(LOG,
                (errmsg("pg_partdist replay: 回收陈旧槽位 shard %u（关系已不存在）",
                        s->shard_oid)));
        memset(s, 0, sizeof(ReplayShardSlot));
        nslots++;
    }
    LWLockRelease(ReplayCtl->lock);

    /* ---- 2) 删目录：关系已不存在、且过了宽限期 ---- */
    snprintf(dirpath, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);
    dir = AllocateDir(dirpath);
    if (dir != NULL)
    {
        while ((de = ReadDir(dir, dirpath)) != NULL)
        {
            char        sub[MAXPGPATH];
            char       *endptr;
            Oid         oid;
            struct stat st;

            if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
                continue;

            oid = (Oid) strtoul(de->d_name, &endptr, 10);
            if (*endptr != '\0' || oid == InvalidOid)
                continue;               /* 不是 OID 命名的目录，不碰 */

            if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(oid)))
                continue;               /* ★ 关系还在 —— 这份流仍有主，绝不能删 */

            snprintf(sub, MAXPGPATH, "%s/%s", dirpath, de->d_name);
            if (stat(sub, &st) != 0 || !S_ISDIR(st.st_mode))
                continue;
            if (grace_secs > 0 && (now - st.st_mtime) < grace_secs)
                continue;               /* 还在宽限期内 */

            if (!rmtree(sub, true))
            {
                ereport(WARNING,
                        (errmsg("pg_partdist replay: 删除陈旧目录 %s 失败", sub)));
                continue;
            }
            ereport(LOG,
                    (errmsg("pg_partdist replay: 回收陈旧目录 pg_parwal/%s"
                            "（关系已不存在）", de->d_name)));
            ndirs++;
        }
        FreeDir(dir);
    }

    if (slots_freed != NULL)
        *slots_freed = nslots;
    if (dirs_removed != NULL)
        *dirs_removed = ndirs;
}

PG_FUNCTION_INFO_V1(pg_partdist_replay_reclaim_stale);

Datum
pg_partdist_replay_reclaim_stale(PG_FUNCTION_ARGS)
{
    int         grace = PG_GETARG_INT32(0);
    int         nslots = 0;
    int         ndirs = 0;
    TupleDesc   tupdesc;
    Datum       values[2];
    bool        nulls[2] = {false, false};

    ReplayReclaimStale(grace, &nslots, &ndirs);

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "replay_reclaim_stale: 返回类型不是复合类型");
    tupdesc = BlessTupleDesc(tupdesc);

    values[0] = Int32GetDatum(nslots);
    values[1] = Int32GetDatum(ndirs);
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

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
 * replay_locmap(local regclass)
 *   → (role int, ord int, leader_spc oid, leader_db oid,
 *      leader_relnum oid, local_relnum oid)
 *
 * 直接把 follower 当前的 leader→本地 文件号映射摆出来。
 *
 * 有了 §12 的 CTRL 换表之后，locmap 不再是"建一次就不变"的：leader 一次
 * VACUUM FULL 就会让 leader_relnum 整体换掉。判断"控制记录到底应用了没有"
 * 必须能直接查这张表 —— 否则只能靠页面比对通没通去反推，那是间接现象。
 */
Datum
pg_partdist_replay_locmap(PG_FUNCTION_ARGS)
{
    Oid              relid  = PG_GETARG_OID(0);
    ReturnSetInfo   *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    ReplayLocMapFile lm;
    const char      *reason = NULL;
    int              i;

    InitMaterializedSRF(fcinfo, 0);

    if (!ShardReplayReadLocMap(relid, &lm, &reason))
        ereport(ERROR,
                (errmsg("replay_locmap: shard %u 的 locmap 不可用: %s",
                        relid, reason ? reason : "未知原因")));

    for (i = 0; i < lm.npairs; i++)
    {
        Datum values[6];
        bool  nulls[6] = {false, false, false, false, false, false};

        values[0] = Int32GetDatum((int32) lm.pairs[i].role);
        values[1] = Int32GetDatum((int32) lm.pairs[i].ord);
        values[2] = ObjectIdGetDatum(lm.pairs[i].leader_loc.spcOid);
        values[3] = ObjectIdGetDatum(lm.pairs[i].leader_loc.dbOid);
        values[4] = ObjectIdGetDatum(lm.pairs[i].leader_loc.relNumber);
        values[5] = ObjectIdGetDatum(lm.pairs[i].local_loc.relNumber);

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
    lm.version   = REPLAY_LOCMAP_VERSION;
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
                e->role                 = local_fs.rels[j].role;
                e->ord                  = local_fs.rels[j].ord;
                e->reserved             = 0;
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

    /* 持久化 locmap（tmp + fsync + rename），与 CTRL 换表共用同一实现 */
    ShardReplayWriteLocMap(&lm);

    /*
     * ★ 强制一次本地 checkpoint —— 这不是可选的稳妥措施，是**正确性必需**。
     *
     * 副本的 shard 文件会被两条互不知情的 redo 流写：本模块的 parwal 回放
     * （盖 leader 坐标 LSN），以及节点自身的本地 pg_wal 崩溃恢复。而 FPI 的
     * 应用是**无条件**的 —— xlogutils.c 的 XLogReadBufferForRedoExtended 在
     * XLogRecBlockImageApply() 分支里直接 RBM_ZERO_AND_LOCK + RestoreBlockImage，
     * 前面**没有任何页 LSN 比较**（LSN 判据只存在于非 FPI 分支）。
     *
     * 于是：建壳表（CREATE TABLE ... INCLUDING ALL，wal_level=replica 下
     * wal_skip 不适用）把每个索引的元页以 FPI 写进了本地 WAL。只要之后节点
     * 崩溃、且崩溃恢复的 redo 起点早于建表，那条 FPI 就会把回放出来的元页
     * 无条件盖回 _bt_initmetapage 的初值（btm_root=0 ⇒ 升主后索引不可用）。
     *
     * 实测（2026-08-04 受控实验）：崩溃前盘面 pd_lsn=0/A74EFD0 btm_root=1，
     * immediate 崩溃重启后 pd_lsn=0/81DF748 btm_root=0 —— 后者正落在建壳表的
     * WAL 区间 [0/81AECC0, 0/81E0198] 内，是一个**本地** LSN。
     *
     * 所以不变式是：**副本文件在开始回放之前，写过它们的本地 WAL 必须已经被
     * checkpoint 甩到 redo 点之后。** 本函数是建立/刷新映射的唯一入口
     * （首次配对、以及 §12 结构栅栏之后的重新配对 —— 后者紧跟在运维于本地
     * 壳表上做的 CREATE INDEX 之后，同样会留下本地 WAL），因此把 checkpoint
     * 钉在这里。代价是一次强制 checkpoint，而本函数是稀有操作。
     *
     * 注意这**不能**根治：§13.5 已证 autovacuum_enabled=off 挡不住
     * anti-wraparound vacuum，它一旦扫到副本壳表就又会写本地 WAL，把这个洞
     * 重新打开。彻底的办法是让副本文件永不被本地 WAL 触碰（见 FRD §13 约束 12）。
     */
    RequestCheckpoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE | CHECKPOINT_WAIT);

    /*
     * 建槽之前先回收一次陈旧槽位与目录。
     *
     * 槽位上限是 REPLAY_MAX_SHARDS(64)，而壳表 DROP 之后槽位不会自动释放 ——
     * 反复建/删副本的环境（尤其测试）会把槽位占满，此后
     * "回放槽位已满" 让新副本一个都建不了。这里是**唯一**会新建槽位的入口，
     * 也就是回收的最佳时机：先扫一遍把关系已不存在的槽位与目录清掉，
     * 再去找空位。判据是"OID 在 pg_class 里已不存在"，关系还在的一律不碰。
     *
     * 必须在取锁**之前**做 —— ReplayReclaimStale 自己要取同一把锁。
     */
    {
        int freed = 0, removed = 0;

        ReplayReclaimStale(replay_reclaim_grace_secs, &freed, &removed);
        if (freed > 0 || removed > 0)
            ereport(LOG,
                    (errmsg("pg_partdist replay: 建槽前回收了 %d 个陈旧槽位、"
                            "%d 个陈旧目录", freed, removed)));
    }

    /* 槽位登记（豁免钩子即刻生效；enabled 仍需 replay_enable） */
    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    {
        ReplayShardSlot *s = ReplaySlotFindLocked(local_relid, true);

        ReplaySlotLoadLocsLocked(s);
        ReplayRecountReplicasLocked();

        /*
         * 配对换过了 → 让 worker 丢掉内存里那张 loc_map 重建（§12 栅栏恢复）。
         * 同时把 NEEDS_STRUCT 清掉：结构既已补齐，栅栏就不该继续拦着，
         * 下一次 replay_catchup 应当直接开工。
         */
        s->locmap_gen++;
        if (s->state == REPLAY_NEEDS_STRUCT)
        {
            s->state = REPLAY_IDLE;
            s->errmsg[0] = '\0';
        }
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
    static const char *state_name[] = {"idle", "catching_up", "failed",
                                       "needs_struct"};

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
            (s->state >= 0 && s->state < (int) lengthof(state_name))
                ? state_name[s->state] : "?");
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
