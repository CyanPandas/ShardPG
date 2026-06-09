/*
 * demux_worker.c
 * pg_partdist Demux background worker — Milestone 2.2.
 *
 * Reads the local pg_wal stream and routes PartWALHeader records (written by
 * the custom RM_EXPERIMENTAL_ID RMGR) into per-partition pg_parwal/N/ segment
 * files via buffered PartitionWALWriters.
 *
 * Design notes
 * ------------
 * • Deduplication: on startup each writer queries the highest partition_lsn
 *   already on disk (written by the synchronous M2.1 path).  Records with
 *   partition_lsn ≤ that watermark are skipped without writing.
 * • Progress file: DataDir/pg_parwal/.demux_progress records the last WAL LSN
 *   fully processed so the worker resumes from the right place after restart.
 * • Memory cap: total buffer across all active writers is bounded by
 *   DEMUX_MAX_PARTITIONS × PARWAL_WRITER_BUFFER_SIZE ≤ 128 MB.
 * • Back-pressure: a partition whose writer encounters ENOSPC is stalled
 *   independently; other partitions continue writing.
 * • Latency stats: processing latency is stored in a shared-memory circular
 *   buffer for SQL queries via demux_latency_stats().
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "demux_worker.h"
#include "partition_wal.h"
#include "partition_wal_header.h"
#include "partition_wal_writer.h"

#include "access/rmgr.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
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
    worker.bgw_restart_time = 5;    /* restart 5 s after crash */

    strlcpy(worker.bgw_library_name,  "pg_partdist",    BGW_MAXLEN);
    strlcpy(worker.bgw_function_name, "DemuxWorkerMain", BGW_MAXLEN);

    worker.bgw_main_arg   = Int32GetDatum(0);
    worker.bgw_notify_pid = 0;

    RegisterBackgroundWorker(&worker);
}

/* ================================================================== */
/* Progress file helpers                                                */
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
        return;     /* best-effort */

    do {
        nb = write(fd, &pf, sizeof(pf));
    } while (nb < 0 && errno == EINTR);

    close(fd);

    if (nb == (ssize_t) sizeof(pf))
        (void) rename(tmp, path);
}

/* ================================================================== */
/* WAL reader callbacks                                                 */
/* ================================================================== */

/* Forward declarations needed by DemuxReadPage */
static void DemuxOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
                             TimeLineID *tli_p);
static void DemuxCloseSegment(XLogReaderState *state);

static int
DemuxReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr,
              int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
    off_t      offset;
    ssize_t    count;
    XLogSegNo  segno;
    TimeLineID tli = 1;

    /*
     * PG16 changed the XLogReader API: ReadPageInternal no longer calls
     * segment_open before calling page_read.  The segment_open callback is
     * now only invoked by the WALRead() helper, which we don't use.  We
     * must therefore manage segment opening inside this callback.
     */
    XLByteToSeg(targetPagePtr, segno, state->segcxt.ws_segsize);
    if (state->seg.ws_file < 0 || state->seg.ws_segno != segno)
    {
        if (state->seg.ws_file >= 0)
            DemuxCloseSegment(state);
        DemuxOpenSegment(state, segno, &tli);
    }

    if (state->seg.ws_file < 0)
        return -1;

    offset = XLogSegmentOffset(targetPagePtr, state->segcxt.ws_segsize);

    if (lseek(state->seg.ws_file, (off_t) offset, SEEK_SET) < 0)
        return -1;

    count = read(state->seg.ws_file, readBuf, XLOG_BLCKSZ);
    if (count < 0)
        return -1;

    return (int) count;
}

static void
DemuxOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
                 TimeLineID *tli_p)
{
    char fname[MAXPGPATH];
    char path[MAXPGPATH];
    int  fd;

    /*
     * The XLogReader passes *tli_p = &state->seg.ws_tli which starts at 0
     * (zero-initialized).  A TLI of 0 generates an invalid filename.
     * Use timeline 1 which is the primary server's timeline.
     */
    *tli_p = 1;

    XLogFileName(fname, *tli_p, nextSegNo, state->segcxt.ws_segsize);
    snprintf(path, MAXPGPATH, "%s/pg_wal/%s", DataDir, fname);

    fd = open(path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist demux: could not open WAL segment "
                        "\"%s\": %m", path)));

    state->seg.ws_file  = fd;
    state->seg.ws_segno = nextSegNo;
    state->seg.ws_tli   = *tli_p;
}

static void
DemuxCloseSegment(XLogReaderState *state)
{
    if (state->seg.ws_file >= 0)
    {
        close(state->seg.ws_file);
        state->seg.ws_file = -1;
    }
}

/* ================================================================== */
/* Writer cache — local to this process                                 */
/* ================================================================== */

static PartitionWALWriter *active_writers[DEMUX_MAX_PARTITIONS];
static Oid                 writer_pids[DEMUX_MAX_PARTITIONS];
static int                 num_writers = 0;

static PartitionWALWriter *
GetOrCreateWriter(Oid partition_id)
{
    int i;

    for (i = 0; i < num_writers; i++)
        if (writer_pids[i] == partition_id)
            return active_writers[i];

    if (num_writers >= DEMUX_MAX_PARTITIONS)
    {
        ereport(WARNING,
                (errmsg("pg_partdist demux: writer cache full (%d partitions), "
                        "skipping partition %u",
                        DEMUX_MAX_PARTITIONS, partition_id)));
        return NULL;
    }

    active_writers[num_writers] = CreatePartitionWALWriter(partition_id);
    writer_pids[num_writers]    = partition_id;
    num_writers++;
    return active_writers[num_writers - 1];
}

static void
FlushAllWriters(void)
{
    int i;

    for (i = 0; i < num_writers; i++)
    {
        PG_TRY();
        {
            FlushPartitionWALWriter(active_writers[i], false);
        }
        PG_CATCH();
        {
            FlushErrorState();
            if (!active_writers[i]->enospc_stalled)
                ereport(WARNING,
                        (errmsg("pg_partdist demux: stalling "
                                "partition %u after write error",
                                active_writers[i]->partition_id)));
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

static void
RecordLatencySample(int64 us)
{
    if (DemuxState == NULL)
        return;

    LWLockAcquire(DemuxState->lock, LW_EXCLUSIVE);
    DemuxState->latency_buf[DemuxState->latency_head] = us;
    DemuxState->latency_head =
        (DemuxState->latency_head + 1) % DEMUX_LATENCY_SAMPLES;
    DemuxState->latency_count++;
    LWLockRelease(DemuxState->lock);
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
    XLogReaderState *reader;
    XLogRecPtr       startLSN;
    TimeLineID       tli = 1;
    int              save_counter       = 0;
    int              null_streak        = 0;  /* consecutive NULL reads from same pos */
    int              batch_counter      = 0;  /* records processed since startup */

    /* Install signal handlers */
    pqsignal(SIGTERM, DemuxSigterm);
    pqsignal(SIGHUP,  SIG_IGN);
    BackgroundWorkerUnblockSignals();

    /*
     * Ask the EEVDF scheduler (Linux 5.15+) for the lowest scheduling
     * latency tier.  latency_nice = -20 gives this process the lowest
     * virtual deadline so it preempts other tasks immediately after
     * SetLatch fires.  No privilege is required.  Fails silently on
     * kernels that don't support the flag.
     */
    {
        /* sched_attr ABI — must match include/uapi/linux/sched/types.h */
        struct {
            uint32_t size;
            uint32_t sched_policy;
            uint64_t sched_flags;
            int32_t  sched_nice;
            uint32_t sched_priority;
            uint64_t sched_runtime;
            uint64_t sched_deadline;
            uint64_t sched_period;
            uint32_t sched_util_min;
            uint32_t sched_util_max;
            int32_t  sched_latency_nice;
        } attr;

        memset(&attr, 0, sizeof(attr));
        attr.size               = sizeof(attr);
        attr.sched_policy       = 0;  /* SCHED_NORMAL / SCHED_OTHER */
        attr.sched_flags        = UINT64_C(0x40);  /* SCHED_FLAG_LATENCY_NICE */
        attr.sched_latency_nice = -20;

        /* __NR_sched_setattr = 314 on x86-64 */
        (void) syscall(314, 0, &attr, 0);
    }

    /* Mark as active and publish our latch so backends can wake us */
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
     * Determine start LSN.
     *
     * Recovery strategy (in priority order):
     *   1. If a saved progress file exists, resume from that WAL position.
     *      This is critical for the clean-restart path (pg_ctl restart):
     *      when the demux exits gracefully before draining all WAL records,
     *      the next demux would otherwise start at GetFlushRecPtr() and
     *      silently skip every record between the saved position and the
     *      WAL end.  On a post-crash restart the redo handler has already
     *      re-written those records into pg_parwal via crash recovery, so
     *      re-reading from an older position is harmless: the partition_lsn
     *      deduplication guard in CreatePartitionWALWriter skips
     *      already-present records, and the null_streak / XLogFindNextRecord
     *      mechanism handles any stale page headers from the pre-crash WAL.
     *   2. Fall back to the current WAL flush frontier.  Used when no
     *      progress file exists (first run, or pg_parwal was wiped) or when
     *      the saved LSN is at or past the current flush position.
     */
    {
        XLogRecPtr saved_lsn = LoadDemuxProgress();
        XLogRecPtr flush_lsn = GetFlushRecPtr(&tli);

        if (flush_lsn == InvalidXLogRecPtr)
            flush_lsn = GetRedoRecPtr();

        if (!XLogRecPtrIsInvalid(saved_lsn) && saved_lsn < flush_lsn)
            startLSN = saved_lsn;
        else
            startLSN = flush_lsn;
    }

    /* Allocate WAL reader */
    reader = XLogReaderAllocate(wal_segment_size, NULL,
                                XL_ROUTINE(.page_read     = DemuxReadPage,
                                           .segment_open  = DemuxOpenSegment,
                                           .segment_close = DemuxCloseSegment),
                                NULL);
    if (reader == NULL)
        ereport(ERROR,
                (errcode(ERRCODE_OUT_OF_MEMORY),
                 errmsg("pg_partdist demux: could not allocate WAL reader")));

    XLogBeginRead(reader, startLSN);

    /* Main processing loop */
    while (!got_sigterm)
    {
        XLogRecord  *record;
        char        *errormsg = NULL;
        XLogRecPtr   flush_lsn;
        TimestampTz  t_start;

        /* Don't read past flushed frontier */
        flush_lsn = GetFlushRecPtr(&tli);
        if (reader->EndRecPtr >= flush_lsn)
        {
            /*
             * Flush pg_parwal write buffers to disk FIRST, then advance
             * last_processed_lsn.  This ordering guarantees that any caller
             * of demux_flush() who sees last_processed_lsn >= target will
             * also find the data already written to the parwal segment files.
             *
             * last_committed_lsn is updated eagerly by the commit callback
             * and is used by demux_progress() / latency measurement.
             * last_processed_lsn is the demux-only "data-on-disk" barrier.
             */
            FlushAllWriters();

            if (DemuxState != NULL)
            {
                LWLockAcquire(DemuxState->lock, LW_EXCLUSIVE);
                if (DemuxState->last_processed_lsn == InvalidXLogRecPtr ||
                    DemuxState->last_processed_lsn < flush_lsn)
                    DemuxState->last_processed_lsn = flush_lsn;
                LWLockRelease(DemuxState->lock);
            }

            WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                      DEMUX_SLEEP_MS,
                      PG_WAIT_EXTENSION);
            ResetLatch(MyLatch);
            continue;
        }

        t_start = GetCurrentTimestamp();

        PG_TRY();
        {
            record = XLogReadRecord(reader, &errormsg);
        }
        PG_CATCH();
        {
            /*
             * DemuxOpenSegment threw: the WAL segment is unavailable
             * (recycled or not yet written).  Skip to the current flush
             * frontier so we don't spin, then wait.
             */
            FlushErrorState();
            DemuxCloseSegment(reader);
            startLSN = GetFlushRecPtr(&tli);
            XLogBeginRead(reader, startLSN);
            WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                      DEMUX_SLEEP_MS,
                      PG_WAIT_EXTENSION);
            ResetLatch(MyLatch);
            continue;
        }
        PG_END_TRY();

        if (record == NULL)
        {
            if (errormsg != NULL)
                ereport(WARNING,
                        (errmsg("pg_partdist demux: WAL read error at %X/%08X: %s",
                                LSN_FORMAT_ARGS(reader->EndRecPtr),
                                errormsg)));

            /*
             * End-of-stream or decode failure.
             *
             * The common cause of repeated NULLs is a stale xlp_rem_len in a
             * WAL page header left over from a pre-crash session.  When
             * XLogBeginRead() repositions the reader inside such a page,
             * XLogReadRecord() re-enters its "fresh start" code path, which
             * tries to process the stale continuation bytes from the previous
             * page and fails the CRC check.
             *
             * Correct recovery: use XLogFindNextRecord() which scans forward
             * byte-by-byte using CRC validation, bypassing xlp_rem_len
             * entirely.  This finds the next valid record even in pages with
             * stale page headers.
             */
            null_streak++;
            if (null_streak > 3)
            {
                XLogRecPtr next_valid = InvalidXLogRecPtr;

                PG_TRY();
                {
                    next_valid = XLogFindNextRecord(reader, reader->EndRecPtr);
                }
                PG_CATCH();
                {
                    FlushErrorState();
                    next_valid = InvalidXLogRecPtr;
                }
                PG_END_TRY();

                null_streak = 0;

                /*
                 * Whether or not XLogFindNextRecord found a valid record,
                 * always sleep before retrying.  Without the sleep, we can
                 * spin at >100k iterations/second on a stale WAL page that
                 * XLogFindNextRecord keeps "finding" but XLogReadRecord keeps
                 * rejecting, flooding the server log with warnings.
                 *
                 * The 1ms sleep caps warning rate at ~4000/sec worst case and
                 * is imperceptible for real workloads (the commit-callback
                 * path keeps last_committed_lsn current regardless).
                 */
                (void) next_valid; /* reader already repositioned if valid */
            }

            WaitLatch(MyLatch,
                      WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                      DEMUX_SLEEP_MS,
                      PG_WAIT_EXTENSION);
            ResetLatch(MyLatch);
            continue;
        }

        null_streak = 0;

        /* Process only our custom RMGR records */
        if (XLogRecGetRmid(reader) == RM_EXPERIMENTAL_ID)
        {
            uint8  info     = XLogRecGetInfo(reader) & ~XLR_INFO_MASK;
            uint32 data_len = XLogRecGetDataLen(reader);

            if (info == PARTWAL_RMGR_INFO_DATA &&
                data_len >= sizeof(PartWALHeader))
            {
                PartWALHeader      *hdr = (PartWALHeader *) XLogRecGetData(reader);
                PartitionWALWriter *w   = NULL;

                if (hdr->magic == PARTWAL_MAGIC)
                {
                    PG_TRY();
                    {
                        w = GetOrCreateWriter(hdr->partition_id);
                    }
                    PG_CATCH();
                    {
                        ereport(WARNING,
                                (errmsg("pg_partdist demux: could not create writer "
                                        "for partition %u — skipping record",
                                        hdr->partition_id)));
                        FlushErrorState();
                        w = NULL;
                    }
                    PG_END_TRY();

                    if (w != NULL)
                    {
                        /*
                         * Reset detection: if partition_lsn restarted at 1
                         * but the writer has a higher watermark, check whether
                         * the on-disk files were deleted (reset_partition_wal_state).
                         * If so, reset the writer so we re-write from lsn=1.
                         */
                        if (hdr->partition_lsn == 1 && w->last_partition_lsn > 0)
                        {
                            uint64 disk_lsn =
                                GetLastWrittenPartitionLSN(hdr->partition_id);

                            if (disk_lsn == 0)
                            {
                                /* Files deleted — restart writer from scratch */
                                if (w->fd >= 0)
                                {
                                    close(w->fd);
                                    w->fd = -1;
                                }
                                w->current_segno      = 0;
                                w->buf_used           = 0;
                                w->last_partition_lsn = 0;
                                w->enospc_stalled     = false;
                            }
                        }

                        /*
                         * Un-stall check: probe disk space at most once per
                         * second.  Only clear the stall flag here; the actual
                         * recovery log is emitted after a successful write so
                         * we don't spam the log when the write still fails
                         * (e.g. during a slow recovery where statvfs shows
                         * space but the write keeps failing transiently).
                         */
                        if (w->enospc_stalled &&
                            TimestampDifferenceExceeds(w->last_stall_time,
                                                       GetCurrentTimestamp(),
                                                       1000))
                        {
                            char           parwal_dir[MAXPGPATH];
                            struct statvfs sv;

                            snprintf(parwal_dir, MAXPGPATH, "%s/%s",
                                     DataDir, PARTITION_WAL_DIR);
                            if (statvfs(parwal_dir, &sv) == 0 &&
                                (uint64) sv.f_bavail * (uint64) sv.f_bsize
                                >= sizeof(PartWALHeader))
                                w->enospc_stalled = false;
                            else
                                w->last_stall_time = GetCurrentTimestamp();
                        }

                        if (!w->enospc_stalled &&
                            hdr->partition_lsn > w->last_partition_lsn)
                        {
                            bool was_stalled = (w->last_stall_time != 0);

                            PG_TRY();
                            {
                                WritePartitionWAL(w, hdr);
                                /* fsync deferred: flushed every ~200ms in the
                                 * caught-up branch via FlushAllWriters, and on
                                 * graceful shutdown.  Per-record fsync blocked
                                 * the demux for ~74% of wall time at 380 TPS. */
                                if (was_stalled)
                                    ereport(WARNING,
                                            (errmsg("pg_partdist demux: "
                                                    "resuming partition %u "
                                                    "after disk space freed",
                                                    hdr->partition_id)));
                                w->last_stall_time = 0; /* reset stall history */
                            }
                            PG_CATCH();
                            {
                                FlushErrorState();
                                if (!was_stalled)
                                    ereport(WARNING,
                                            (errmsg("pg_partdist demux: stalling "
                                                    "partition %u after write error",
                                                    hdr->partition_id)));
                                w->enospc_stalled = true;
                                w->last_stall_time = GetCurrentTimestamp();
                            }
                            PG_END_TRY();
                        }
                    }
                }
            }
        }

        /*
         * Record processing latency only for RM_EXPERIMENTAL_ID records
         * (actual PartWAL pipeline records) to avoid high-frequency LWLock
         * acquisitions for the bulk of pass-through records.
         *
         * Update last_processed_lsn every 32 records to batch LWLock pressure.
         * The caught-up branch above also updates it, so under any load the
         * value stays within 32 records of the true processing position.
         */
        if (XLogRecGetRmid(reader) == RM_EXPERIMENTAL_ID && DemuxState != NULL)
        {
            TimestampTz t_end = GetCurrentTimestamp();
            long        secs;
            int         us;

            TimestampDifference(t_start, t_end, &secs, &us);
            RecordLatencySample((int64) secs * 1000000 + us);
        }

        /*
         * batch_counter is still incremented for potential future use,
         * but last_processed_lsn is no longer updated per-record.
         * It is updated ONLY in the caught-up branch, after FlushAllWriters(),
         * so that demux_flush() callers always find data on disk.
         */
        if (DemuxState != NULL)
            batch_counter++;

        /* Persist progress periodically (clamp to flush to stay safe) */
        if (++save_counter >= 16)
        {
            XLogRecPtr safe_lsn = reader->EndRecPtr;
            XLogRecPtr flush_now = GetFlushRecPtr(&tli);

            if (safe_lsn > flush_now)
                safe_lsn = flush_now;

            SaveDemuxProgress(safe_lsn);
            save_counter = 0;
        }
    }

    /* Graceful shutdown */
    FlushAllWriters();
    {
        XLogRecPtr safe_lsn  = reader->EndRecPtr;
        XLogRecPtr flush_now = GetFlushRecPtr(&tli);

        if (safe_lsn > flush_now)
            safe_lsn = flush_now;

        SaveDemuxProgress(safe_lsn);
    }
    DestroyAllWriters();
    XLogReaderFree(reader);

    if (DemuxState != NULL)
    {
        LWLockAcquire(DemuxState->lock, LW_EXCLUSIVE);
        DemuxState->worker_active = false;
        DemuxState->demux_latch   = NULL;
        LWLockRelease(DemuxState->lock);
    }

    proc_exit(0);
}

/* ================================================================== */
/* SQL-callable functions                                               */
/* ================================================================== */

/* pg_partdist.count_parwal_records(partition_id OID) → BIGINT
 *
 * Counts valid PartWALHeader records in pg_parwal/<partition_id>/.
 * Files are processed in lexicographic (segment) order.  Scanning stops at
 * the first record whose magic or partition_id is invalid, so the result
 * represents the number of intact records before the first corruption point.
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

    /* Sort ascending so we process segments in write order */
    for (i = 0; i < nfiles - 1; i++)
        for (j = i + 1; j < nfiles; j++)
            if (strcmp(segfiles[i], segfiles[j]) > 0)
            {
                char tmp[MAXPGPATH];
                strlcpy(tmp,          segfiles[i], MAXPGPATH);
                strlcpy(segfiles[i],  segfiles[j], MAXPGPATH);
                strlcpy(segfiles[j],  tmp,          MAXPGPATH);
            }

    for (i = 0; i < nfiles && !stopped; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALHeader header;
        ssize_t       nb;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY, 0);
        if (fd < 0)
            continue;

        while ((nb = read(fd, &header, sizeof(PartWALHeader)))
               == (ssize_t) sizeof(PartWALHeader))
        {
            if (header.magic        != PARTWAL_MAGIC ||
                header.partition_id != partition_id)
            {
                stopped = true;
                break;
            }
            total++;
        }

        close(fd);
    }

    PG_RETURN_INT64(total);
}

/*
 * pg_partdist.demux_progress()
 *   → TABLE(node_name TEXT, last_processed_lsn PG_LSN)
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
        /* Return last_committed_lsn for latency measurement: it is updated
         * eagerly by the commit callback so that pg_current_wal_flush_lsn()
         * == last_processed_lsn within the same query that committed. */
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
 * pg_partdist.demux_latency_stats()
 *   → TABLE(p50_ms FLOAT, p99_ms FLOAT, avg_ms FLOAT)
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

    /* Sort sample buffer (insertion sort, small N ≤ 1024) */
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

    values[0] = Float8GetDatum(samples[n / 2]            / 1000.0); /* p50 ms */
    values[1] = Float8GetDatum(samples[(int)(n * 99 / 100)] / 1000.0); /* p99 ms */
    values[2] = Float8GetDatum(sum / n                   / 1000.0); /* avg ms */

    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * pg_partdist.read_all_headers(partition_id OID)
 *   → TABLE(partition_lsn BIGINT, orig_node_lsn PG_LSN)
 *
 * Used by T2.2.3 (partition_lsn monotonicity).
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

    for (i = 0; i < nfiles; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALHeader header;
        ssize_t       nbytes;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, segfiles[i]);
        fd = open(filepath, O_RDONLY);
        if (fd < 0)
            continue;

        while ((nbytes = read(fd, &header, sizeof(PartWALHeader)))
               == (ssize_t) sizeof(PartWALHeader))
        {
            Datum values[2];
            bool  nulls[2] = {false, false};

            if (header.magic != PARTWAL_MAGIC || header.partition_id != partition_id)
                continue;

            values[0] = Int64GetDatum((int64) header.partition_lsn);
            values[1] = LSNGetDatum(header.orig_node_lsn);

            tuplestore_putvalues(tupstore, rsinfo->setDesc, values, nulls);
        }
        close(fd);
    }

    PG_RETURN_NULL();
}

/*
 * pg_partdist.demux_flush() → void
 *
 * Block until the Demux Worker has processed all WAL up to the current
 * flush position (or timeout after 30 s).  Used by regression tests that
 * write records via write_partition_wal_record() and then immediately read
 * back results from pg_parwal/N/.  Because the Demux Worker is now the
 * sole writer to pg_parwal/N/, callers must call this before reading.
 */
PG_FUNCTION_INFO_V1(pg_partdist_demux_flush);
Datum
pg_partdist_demux_flush(PG_FUNCTION_ARGS)
{
    XLogRecPtr  target_lsn;
    XLogRecPtr  processed_lsn;
    TimeLineID  tli;
    int         waited_ms   = 0;
    int         max_wait_ms = 30000;    /* 30 s hard timeout */

    target_lsn = GetFlushRecPtr(&tli);

    while (waited_ms < max_wait_ms)
    {
        if (DemuxState != NULL)
        {
            LWLockAcquire(DemuxState->lock, LW_SHARED);
            processed_lsn = DemuxState->last_processed_lsn;
            LWLockRelease(DemuxState->lock);

            if (processed_lsn != InvalidXLogRecPtr && processed_lsn >= target_lsn)
                break;
        }

        pg_usleep(20000);   /* 20 ms */
        waited_ms += 20;
    }

    if (waited_ms >= max_wait_ms)
        ereport(WARNING,
                (errmsg("pg_partdist.demux_flush: timed out after %d ms "
                        "waiting for Demux Worker to reach LSN %X/%08X",
                        max_wait_ms,
                        LSN_FORMAT_ARGS(target_lsn))));

    PG_RETURN_VOID();
}
