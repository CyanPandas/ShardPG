/*
 * demux_worker.c
 * pg_partdist Demux background worker — parwal-2.0.
 *
 * Architecture change from 1.x:
 *   • No longer reads RM_EXPERIMENTAL_ID records from pg_wal.
 *   • Instead reads (partition_id, relfilenode, start_lsn, end_lsn)
 *     entries from the shmem ring buffer filled by hooks.
 *   • For each entry: opens an XLogReader, scans pg_wal from start_lsn
 *     to end_lsn, filters records whose relfilenode matches the target,
 *     and writes PartWALRecord (header + raw data) to pg_parwal files.
 *   • On startup: performs crash recovery by reading per-partition
 *     checkpoint files (which store relfilenode + last_wal_lsn +
 *     last_part_lsn) and rescanning WAL from last_wal_lsn forward.
 *
 * Filtering WAL records by relfilenode:
 *   For each XLogRecord, iterate block references.  If any block
 *   reference has rlocator.relNumber == target relfilenode, the record
 *   is written.  Also write records for RM_XACT_ID (transaction
 *   commit/abort records do not have block refs but are included for
 *   completeness in the range scan).
 *
 * Note: bgw_flags = BGWORKER_SHMEM_ACCESS only (no DB connection).
 * We therefore cannot use RelationOpen() in the demux.  The relfilenode
 * is stored in the queue entry (captured by the hook) and also in the
 * checkpoint file (for crash recovery).
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "demux_worker.h"
#include "partition_wal.h"
#include "partition_wal_header.h"
#include "partition_wal_writer.h"
#include "partwal_sync.h"
#include "shard_fileset.h"

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

/* ================================================================== */
/* Shared memory                                                        */
/* ================================================================== */

DemuxSharedState *DemuxState = NULL;

Size
DemuxShmemSize(void)
{
    return sizeof(DemuxSharedState);
}

void
RequestDemuxShmem(void)
{
    RequestAddinShmemSpace(DemuxShmemSize());
    RequestNamedLWLockTranche(DEMUX_LOCK_TRANCHE, 1);
}

void
DemuxShmemInit(void)
{
    bool found;

    DemuxState = (DemuxSharedState *)
        ShmemInitStruct(DEMUX_SHMEM_NAME, sizeof(DemuxSharedState), &found);

    if (!found)
    {
        DemuxState->lock                = &GetNamedLWLockTranche(DEMUX_LOCK_TRANCHE)[0].lock;
        DemuxState->last_processed_lsn  = InvalidXLogRecPtr;
        DemuxState->last_committed_lsn  = InvalidXLogRecPtr;
        DemuxState->worker_active       = false;
        DemuxState->demux_latch         = NULL;
        pg_atomic_init_u32(&DemuxState->drop_notice_gen, 0);   /* T7.8 */
        pg_atomic_init_u32(&DemuxState->drop_notice_swept, 0);
        DemuxState->latency_head        = 0;
        DemuxState->latency_count       = 0;
        memset(DemuxState->latency_buf, 0, sizeof(DemuxState->latency_buf));
    }
}

/* ================================================================== */
/* Background worker registration                                       */
/* ================================================================== */

void
RegisterDemuxWorker(void)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(worker));
    strlcpy(worker.bgw_name, "pg_partdist demux worker", BGW_MAXLEN);
    strlcpy(worker.bgw_type, "pg_partdist demux", BGW_MAXLEN);

    worker.bgw_flags        = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time   = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART; /* one-shot crash recovery */

    strlcpy(worker.bgw_library_name,  "pg_partdist",    BGW_MAXLEN);
    strlcpy(worker.bgw_function_name, "DemuxWorkerMain", BGW_MAXLEN);

    worker.bgw_main_arg   = Int32GetDatum(0);
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

/* ================================================================== */
/* Progress file helpers (kept for demux_progress() compatibility)      */
/* ================================================================== */

typedef struct DemuxProgressFile
{
    uint32      magic;
    uint32      version;
    XLogRecPtr  lsn;
} DemuxProgressFile;

static XLogRecPtr
LoadDemuxProgress(void)
{
    char              path[MAXPGPATH];
    int               fd;
    DemuxProgressFile pf;
    ssize_t           nb;

    snprintf(path, MAXPGPATH, "%s/%s", DataDir, DEMUX_PROGRESS_FILE);
    fd = open(path, O_RDONLY, 0);
    if (fd < 0)
        return InvalidXLogRecPtr;

    nb = read(fd, &pf, sizeof(pf));
    close(fd);

    if (nb != (ssize_t) sizeof(pf) ||
        pf.magic   != DEMUX_PROGRESS_MAGIC ||
        pf.version != 1)
        return InvalidXLogRecPtr;

    return pf.lsn;
}

static void
SaveDemuxProgress(XLogRecPtr lsn)
{
    char              path[MAXPGPATH];
    char              tmp[MAXPGPATH];
    int               fd;
    DemuxProgressFile pf;
    ssize_t           nb;

    pf.magic   = DEMUX_PROGRESS_MAGIC;
    pf.version = 1;
    pf.lsn     = lsn;

    snprintf(path, MAXPGPATH, "%s/%s", DataDir, DEMUX_PROGRESS_FILE);
    snprintf(tmp,  MAXPGPATH, "%s.tmp", path);

    fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0)
        return;

    do {
        nb = write(fd, &pf, sizeof(pf));
    } while (nb < 0 && errno == EINTR);

    close(fd);

    if (nb == (ssize_t) sizeof(pf))
        (void) rename(tmp, path);
}

/*
 * WAL reader callbacks and ScanWALRangeForPartition have been moved to
 * partwal_sync.c where they serve both the synchronous backend write path
 * and this crash-recovery scan.
 */

/* ================================================================== */
/* Writer cache                                                         */
/* ================================================================== */

static PartitionWALWriter *active_writers[DEMUX_MAX_PARTITIONS];
static Oid                 writer_pids[DEMUX_MAX_PARTITIONS];
static int                 num_writers = 0;

static PartitionWALWriter *
GetOrCreateWriter(Oid partition_id, RelFileNumber relfilenode)
{
    int i;

    for (i = 0; i < num_writers; i++)
        if (writer_pids[i] == partition_id)
        {
            /* Update relfilenode if it changed */
            if (RelFileNumberIsValid(relfilenode) &&
                !RelFileNumberIsValid(active_writers[i]->relfilenode))
                active_writers[i]->relfilenode = relfilenode;
            return active_writers[i];
        }

    if (num_writers >= DEMUX_MAX_PARTITIONS)
    {
        ereport(WARNING,
                (errmsg("pg_partdist demux: writer cache full (%d partitions), "
                        "skipping partition %u",
                        DEMUX_MAX_PARTITIONS, partition_id)));
        return NULL;
    }

    active_writers[num_writers] = CreatePartitionWALWriter(partition_id,
                                                            relfilenode);
    writer_pids[num_writers]    = partition_id;
    num_writers++;
    return active_writers[num_writers - 1];
}

static void
FlushAllWritersWithSync(void)
{
    int i;

    for (i = 0; i < num_writers; i++)
    {
        PG_TRY();
        {
            FlushPartitionWALWriter(active_writers[i], true);
        }
        PG_CATCH();
        {
            FlushErrorState();
            active_writers[i]->enospc_stalled = true;
            active_writers[i]->last_stall_time = GetCurrentTimestamp();
        }
        PG_END_TRY();
    }
}

static void
DestroyAllWriters(void)
{
    int i;

    for (i = 0; i < num_writers; i++)
        DestroyPartitionWALWriter(active_writers[i]);
    num_writers = 0;
}

/* ================================================================== */
/* Latency recording                                                    */
/* ================================================================== */

/*
 * WAL reader callbacks and ScanWALRangeForPartition have moved to
 * partwal_sync.c (declared in partwal_sync.h).  DemuxCrashRecovery below
 * calls ScanWALRangeForPartition from there.
 */

/* ================================================================== */
/* Crash recovery on startup                                            */
/* ================================================================== */

/*
 * DemuxCrashRecovery — on startup, scan pg_parwal/ for partition directories
 * that have checkpoint files.  For each, if last_wal_lsn < current flush LSN,
 * rescan WAL from last_wal_lsn forward to recover any missing records.
 */
static void
DemuxCrashRecovery(void)
{
    char           parwal_root[MAXPGPATH];
    DIR           *dir;
    struct dirent *de;
    TimeLineID     tli = 1;
    XLogRecPtr     flush_lsn;

    /*
     * 先从持久化 fileset 文件重建 shmem 反向映射（索引/TOAST 成员），
     * 随后的 WAL 扫描过滤(RecordTouchesPartition)与运行时捕获才同判据。
     */
    LoadAllShardFileSets();

    flush_lsn = GetFlushRecPtr(&tli);
    if (flush_lsn == InvalidXLogRecPtr)
        return;

    snprintf(parwal_root, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);

    dir = opendir(parwal_root);
    if (dir == NULL)
        return;

    while ((de = readdir(dir)) != NULL)
    {
        Oid                   partition_id;
        char                 *endptr;
        PartWALCheckpointFile chk;
        PartitionWALWriter   *w;

        /* Only process numeric directory names (partition OIDs) */
        if (de->d_name[0] == '.')
            continue;

        partition_id = (Oid) strtoul(de->d_name, &endptr, 10);
        if (*endptr != '\0' || partition_id == 0)
            continue;

        /* Read checkpoint for this partition */
        if (!ReadPartWALCheckpoint(partition_id, &chk))
            continue;

        if (!RelFileNumberIsValid(chk.relfilenode))
            continue;

        /*
         * Register this partition in the shmem hash so the WAL insert hook
         * can identify future WAL records for it.  Done even if no recovery
         * scan is needed (partition is up to date).
         *
         * 注意主堆 rfn 之外的成员（索引/TOAST）由启动时的
         * LoadAllShardFileSets() 从持久化 fileset 文件补齐注册。
         */
        PartWALSyncRegister(partition_id, chk.relfilenode);

        /* If already up to date, skip WAL scan */
        if (chk.last_wal_lsn != InvalidXLogRecPtr &&
            chk.last_wal_lsn >= flush_lsn)
            continue;

        ereport(LOG,
                (errmsg("pg_partdist demux: crash recovery for partition %u "
                        "from LSN %X/%08X (relfilenode=%u)",
                        partition_id,
                        LSN_FORMAT_ARGS(chk.last_wal_lsn),
                        chk.relfilenode)));

        w = GetOrCreateWriter(partition_id, chk.relfilenode);
        if (w == NULL)
            continue;

        PG_TRY();
        {
            XLogRecPtr scan_start = chk.last_wal_lsn;

            /*
             * Do NOT use 'continue' here — jumping out of a PG_TRY block
             * without executing PG_END_TRY corrupts PG_exception_stack and
             * can cause subsequent ereport(ERROR) to siglongjmp to a stale
             * handler, producing "double free" heap corruption.
             */
            if (scan_start != InvalidXLogRecPtr)
            {
                /*
                 * Guard against stale checkpoint LSNs from a previous cluster
                 * lifetime.  WAL before (flush_lsn - wal_segment_size) may have
                 * been recycled; scanning it causes allocate_recordbuf() to trip
                 * on invalid xl_tot_len values and leave readRecordBuf dangling,
                 * which then double-frees on XLogReaderFree.
                 */
                if (flush_lsn > (XLogRecPtr) wal_segment_size &&
                    scan_start < flush_lsn - (XLogRecPtr) wal_segment_size)
                    scan_start = flush_lsn - (XLogRecPtr) wal_segment_size;

                ScanWALRangeForPartition(w, scan_start, flush_lsn);
            }
        }
        PG_CATCH();
        {
            FlushErrorState();
            ereport(WARNING,
                    (errmsg("pg_partdist demux: crash recovery failed for "
                            "partition %u", partition_id)));
        }
        PG_END_TRY();
    }

    closedir(dir);

    /* Flush and checkpoint all writers after recovery */
    FlushAllWritersWithSync();
}

/* ================================================================== */
/* Signal handling                                                      */
/* ================================================================== */

static volatile sig_atomic_t got_sigterm = false;

static void
DemuxSigterm(SIGNAL_ARGS)
{
    got_sigterm = true;
    SetLatch(MyLatch);
}

/* ================================================================== */
/* Main entry point                                                     */
/* ================================================================== */

PGDLLEXPORT void
DemuxWorkerMain(Datum arg)
{
    TimeLineID tli = 1;
    XLogRecPtr flush_lsn;

    /* Install signal handlers */
    pqsignal(SIGTERM, DemuxSigterm);
    pqsignal(SIGHUP,  SIG_IGN);
    BackgroundWorkerUnblockSignals();

    /* Mark active and publish latch */
    if (DemuxState != NULL)
    {
        LWLockAcquire(DemuxState->lock, LW_EXCLUSIVE);
        DemuxState->worker_active = true;
        DemuxState->demux_latch   = MyLatch;
        LWLockRelease(DemuxState->lock);
    }

    /* Ensure pg_parwal/ root exists */
    {
        char root[MAXPGPATH];
        snprintf(root, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);
        if (MakePGDirectory(root) < 0 && errno != EEXIST)
            ereport(WARNING,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist demux: could not create %s: %m", root)));
    }

    /*
     * Initialize last_processed_lsn and last_committed_lsn from the
     * saved progress file so that demux_progress() has a valid starting
     * value on clean restart.
     */
    {
        XLogRecPtr saved = LoadDemuxProgress();

        if (!XLogRecPtrIsInvalid(saved) && DemuxState != NULL)
        {
            LWLockAcquire(DemuxState->lock, LW_EXCLUSIVE);
            if (DemuxState->last_processed_lsn == InvalidXLogRecPtr)
                DemuxState->last_processed_lsn = saved;
            if (DemuxState->last_committed_lsn == InvalidXLogRecPtr)
                DemuxState->last_committed_lsn = saved;
            LWLockRelease(DemuxState->lock);
        }
    }

    /*
     * One-shot crash recovery: rescan WAL for any partition whose checkpoint
     * shows missing records, and populate the shmem relfilenode hash so the
     * WAL insert hook can identify future writes for all known partitions.
     *
     * After recovery this worker exits.  Normal operation uses the
     * synchronous FlushPendingPartWAL() path in each backend.
     */
    DemuxCrashRecovery();

    /* Flush writers and persist progress after recovery */
    FlushAllWritersWithSync();
    flush_lsn = GetFlushRecPtr(&tli);
    SaveDemuxProgress(flush_lsn);
    DestroyAllWriters();

    /* Advance last_processed_lsn to current WAL position */
    if (DemuxState != NULL)
    {
        LWLockAcquire(DemuxState->lock, LW_EXCLUSIVE);
        if (DemuxState->last_processed_lsn == InvalidXLogRecPtr ||
            DemuxState->last_processed_lsn < flush_lsn)
            DemuxState->last_processed_lsn = flush_lsn;
        if (DemuxState->last_committed_lsn == InvalidXLogRecPtr ||
            DemuxState->last_committed_lsn < flush_lsn)
            DemuxState->last_committed_lsn = flush_lsn;
        DemuxState->worker_active      = false;
        DemuxState->recovery_complete  = true;
        DemuxState->demux_latch        = NULL;
        LWLockRelease(DemuxState->lock);
    }

    ereport(LOG,
            (errmsg("pg_partdist: crash recovery complete, "
                    "synchronous write path now active")));

    proc_exit(0);
}

/* ================================================================== */
/* SQL-callable functions                                               */
/* ================================================================== */

/*
 * count_parwal_records(partition_id OID) → BIGINT
 *
 * Counts valid PartWALRecord records in pg_parwal/<partition_id>/.
 * Reads the fixed-size header and skips the variable-length data payload.
 */
PG_FUNCTION_INFO_V1(pg_partdist_count_parwal_records);
Datum
pg_partdist_count_parwal_records(PG_FUNCTION_ARGS)
{
    Oid           partition_id = PG_GETARG_OID(0);
    char          dirpath[MAXPGPATH];
    DIR          *dir;
    struct dirent *de;
    int64         total   = 0;
    bool          stopped = false;
    char          segfiles[256][MAXPGPATH];
    int           nfiles  = 0;
    int           i, j;

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir == NULL)
        PG_RETURN_INT64(0);

    while ((de = readdir(dir)) != NULL && nfiles < 256)
    {
        if (IsXLogFileName(de->d_name))
        {
            strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
            nfiles++;
        }
    }
    closedir(dir);

    /* Sort ascending */
    for (i = 0; i < nfiles - 1; i++)
        for (j = i + 1; j < nfiles; j++)
            if (strcmp(segfiles[i], segfiles[j]) > 0)
            {
                char tmp[MAXPGPATH];
                strlcpy(tmp,         segfiles[i], MAXPGPATH);
                strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                strlcpy(segfiles[j], tmp,         MAXPGPATH);
            }

    for (i = 0; i < nfiles && !stopped; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALRecord rec;
        ssize_t       nb;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY, 0);
        if (fd < 0)
            continue;

        while ((nb = read(fd, &rec, sizeof(PartWALRecord)))
               == (ssize_t) sizeof(PartWALRecord))
        {
            if (rec.magic        != PARTWAL_MAGIC ||
                rec.partition_id != partition_id)
            {
                stopped = true;
                break;
            }
            total++;

            /* Skip variable-length payload */
            if (rec.data_len > 0)
            {
                if (lseek(fd, (off_t) rec.data_len, SEEK_CUR) < 0)
                {
                    stopped = true;
                    break;
                }
            }
        }

        close(fd);
    }

    PG_RETURN_INT64(total);
}

/*
 * demux_is_ready() → BOOLEAN
 * Returns true once the one-shot crash-recovery BGW has completed and the
 * synchronous write path is fully active.  Test scripts poll this instead of
 * checking ps, because the BGW exits after recovery (BGW_NEVER_RESTART).
 */
PG_FUNCTION_INFO_V1(pg_partdist_demux_is_ready);
Datum
pg_partdist_demux_is_ready(PG_FUNCTION_ARGS)
{
    bool ready = false;

    if (DemuxState != NULL)
    {
        LWLockAcquire(DemuxState->lock, LW_SHARED);
        ready = DemuxState->recovery_complete;
        LWLockRelease(DemuxState->lock);
    }
    PG_RETURN_BOOL(ready);
}

/*
 * demux_progress() → TABLE(node_name TEXT, last_processed_lsn PG_LSN)
 */
PG_FUNCTION_INFO_V1(pg_partdist_demux_progress);
Datum
pg_partdist_demux_progress(PG_FUNCTION_ARGS)
{
    TupleDesc  tupdesc;
    Datum      values[2];
    bool       nulls[2] = {false, false};
    HeapTuple  tuple;
    XLogRecPtr lsn = InvalidXLogRecPtr;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));
    tupdesc = BlessTupleDesc(tupdesc);

    if (DemuxState != NULL)
    {
        LWLockAcquire(DemuxState->lock, LW_SHARED);
        lsn = DemuxState->last_committed_lsn;
        LWLockRelease(DemuxState->lock);
    }

    values[0] = CStringGetTextDatum("local");
    if (lsn == InvalidXLogRecPtr)
    {
        nulls[1]  = true;
        values[1] = (Datum) 0;
    }
    else
        values[1] = LSNGetDatum(lsn);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * demux_latency_stats() → TABLE(p50_ms FLOAT, p99_ms FLOAT, avg_ms FLOAT)
 */
PG_FUNCTION_INFO_V1(pg_partdist_demux_latency_stats);
Datum
pg_partdist_demux_latency_stats(PG_FUNCTION_ARGS)
{
    TupleDesc  tupdesc;
    Datum      values[3];
    bool       nulls[3] = {false, false, false};
    HeapTuple  tuple;
    int64      samples[DEMUX_LATENCY_SAMPLES];
    int64      count = 0;
    int64      n, i, j;
    double     sum = 0;
    int64      tmp;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));
    tupdesc = BlessTupleDesc(tupdesc);

    if (DemuxState != NULL)
    {
        LWLockAcquire(DemuxState->lock, LW_SHARED);
        count = (DemuxState->latency_count < DEMUX_LATENCY_SAMPLES)
                ? DemuxState->latency_count
                : DEMUX_LATENCY_SAMPLES;
        memcpy(samples, DemuxState->latency_buf, count * sizeof(int64));
        LWLockRelease(DemuxState->lock);
    }

    if (count == 0)
    {
        nulls[0] = nulls[1] = nulls[2] = true;
        values[0] = values[1] = values[2] = (Datum) 0;
        tuple = heap_form_tuple(tupdesc, values, nulls);
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }

    n = count;
    for (i = 1; i < n; i++)
    {
        tmp = samples[i];
        j   = i - 1;
        while (j >= 0 && samples[j] > tmp)
        {
            samples[j + 1] = samples[j];
            j--;
        }
        samples[j + 1] = tmp;
    }

    for (i = 0; i < n; i++)
        sum += (double) samples[i];

    values[0] = Float8GetDatum(samples[n / 2]                / 1000.0);
    values[1] = Float8GetDatum(samples[(int)(n * 99 / 100)]  / 1000.0);
    values[2] = Float8GetDatum(sum / n                       / 1000.0);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * read_all_headers(partition_id OID)
 *   → TABLE(partition_lsn BIGINT, orig_node_lsn PG_LSN)
 */
PG_FUNCTION_INFO_V1(pg_partdist_read_all_headers);
Datum
pg_partdist_read_all_headers(PG_FUNCTION_ARGS)
{
    Oid             partition_id = PG_GETARG_OID(0);
    ReturnSetInfo  *rsinfo        = (ReturnSetInfo *) fcinfo->resultinfo;
    TupleDesc       tupdesc;
    Tuplestorestate *tupstore;
    MemoryContext   old_cxt;
    char            dirpath[MAXPGPATH];
    DIR            *dir;
    struct dirent  *de;
    char            segfiles[256][MAXPGPATH];
    int             nfiles = 0;
    int             i, j;

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that "
                        "cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required")));
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("return type must be a row type")));

    old_cxt  = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
    tupstore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult  = tupstore;
    rsinfo->setDesc    = BlessTupleDesc(tupdesc);
    MemoryContextSwitchTo(old_cxt);

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, partition_id);

    dir = opendir(dirpath);
    if (dir == NULL)
        PG_RETURN_NULL();

    while ((de = readdir(dir)) != NULL && nfiles < 256)
    {
        if (IsXLogFileName(de->d_name))
        {
            strlcpy(segfiles[nfiles], de->d_name, MAXPGPATH);
            nfiles++;
        }
    }
    closedir(dir);

    for (i = 0; i < nfiles - 1; i++)
        for (j = i + 1; j < nfiles; j++)
            if (strcmp(segfiles[i], segfiles[j]) > 0)
            {
                char tmp[MAXPGPATH];
                strlcpy(tmp,         segfiles[i], MAXPGPATH);
                strlcpy(segfiles[i], segfiles[j], MAXPGPATH);
                strlcpy(segfiles[j], tmp,         MAXPGPATH);
            }

    for (i = 0; i < nfiles; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALRecord rec;
        ssize_t       nbytes;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY);
        if (fd < 0)
            continue;

        while ((nbytes = read(fd, &rec, sizeof(PartWALRecord)))
               == (ssize_t) sizeof(PartWALRecord))
        {
            Datum values[2];
            bool  nulls[2] = {false, false};

            if (rec.magic != PARTWAL_MAGIC || rec.partition_id != partition_id)
            {
                /* Skip variable-length payload before stopping */
                if (rec.data_len > 0)
                    (void) lseek(fd, (off_t) rec.data_len, SEEK_CUR);
                continue;
            }

            values[0] = Int64GetDatum((int64) rec.partition_lsn);
            values[1] = LSNGetDatum(rec.orig_lsn);

            tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);

            /* Skip variable-length payload */
            if (rec.data_len > 0)
            {
                if (lseek(fd, (off_t) rec.data_len, SEEK_CUR) < 0)
                    break;
            }
        }
        close(fd);
    }

    PG_RETURN_NULL();
}

/*
 * demux_flush() → void
 *
 * In parwal-2.0.1 (synchronous write path), PartWAL records are written
 * directly by each backend at ExecutorFinish time.  There is no background
 * worker queue to drain, so this function returns immediately.
 *
 * last_processed_lsn is updated by FlushPendingPartWAL after each write,
 * so callers that check demux_progress() will see consistent values.
 */
PG_FUNCTION_INFO_V1(pg_partdist_demux_flush);
Datum
pg_partdist_demux_flush(PG_FUNCTION_ARGS)
{
    PG_RETURN_VOID();
}
