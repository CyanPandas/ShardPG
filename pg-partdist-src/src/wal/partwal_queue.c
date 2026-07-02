/*
 * partwal_queue.c
 * Shared-memory ring buffer for parwal-2.0.
 *
 * Hooks (ExecutorFinish, ProcessUtility) enqueue (partition_id, relfilenode,
 * start_lsn, end_lsn) entries.  The Demux Worker dequeues them and scans
 * the corresponding pg_wal range by relfilenode.
 *
 * On overflow, entries are silently dropped; the per-partition checkpoint
 * file enables crash recovery from the last known position.
 */
#include "postgres.h"

#include "partwal_queue.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"

/* Module-level pointer */
PartWALQueue *PartWALQueueState = NULL;

/* ================================================================== */
/* Lifecycle                                                            */
/* ================================================================== */

Size
PartWALQueueShmemSize(void)
{
    return sizeof(PartWALQueue);
}

void
RequestPartWALQueueShmem(void)
{
    RequestAddinShmemSpace(PartWALQueueShmemSize());
    RequestNamedLWLockTranche(PARTWAL_QUEUE_TRANCHE, 1);
}

void
PartWALQueueShmemInit(void)
{
    bool found;

    PartWALQueueState = (PartWALQueue *)
        ShmemInitStruct(PARTWAL_QUEUE_SHMEM_NAME,
                        sizeof(PartWALQueue), &found);

    if (!found)
    {
        PartWALQueueState->lock           =
            &(GetNamedLWLockTranche(PARTWAL_QUEUE_TRANCHE))[0].lock;
        PartWALQueueState->head           = 0;
        PartWALQueueState->tail           = 0;
        PartWALQueueState->overflow_count = 0;
        memset(PartWALQueueState->entries, 0, sizeof(PartWALQueueState->entries));
    }
}

/* ================================================================== */
/* Producer                                                             */
/* ================================================================== */

void
PartWALQueueEnqueue(Oid partition_id,
                    RelFileNumber relfilenode,
                    XLogRecPtr start_lsn,
                    XLogRecPtr end_lsn)
{
    uint32 next_tail;

    if (PartWALQueueState == NULL)
        return;

    LWLockAcquire(PartWALQueueState->lock, LW_EXCLUSIVE);

    next_tail = (PartWALQueueState->tail + 1) % PARTWAL_QUEUE_SIZE;

    if (next_tail == PartWALQueueState->head)
    {
        /* Buffer full — drop entry */
        PartWALQueueState->overflow_count++;
        LWLockRelease(PartWALQueueState->lock);
        return;
    }

    PartWALQueueState->entries[PartWALQueueState->tail].partition_id = partition_id;
    PartWALQueueState->entries[PartWALQueueState->tail].relfilenode  = relfilenode;
    PartWALQueueState->entries[PartWALQueueState->tail].start_lsn    = start_lsn;
    PartWALQueueState->entries[PartWALQueueState->tail].end_lsn      = end_lsn;

    PartWALQueueState->tail = next_tail;

    LWLockRelease(PartWALQueueState->lock);
}

/* ================================================================== */
/* Consumer                                                             */
/* ================================================================== */

bool
PartWALQueueDequeue(PartWALQueueEntry *entry)
{
    if (PartWALQueueState == NULL)
        return false;

    LWLockAcquire(PartWALQueueState->lock, LW_EXCLUSIVE);

    if (PartWALQueueState->head == PartWALQueueState->tail)
    {
        /* Empty */
        LWLockRelease(PartWALQueueState->lock);
        return false;
    }

    *entry = PartWALQueueState->entries[PartWALQueueState->head];
    PartWALQueueState->head =
        (PartWALQueueState->head + 1) % PARTWAL_QUEUE_SIZE;

    LWLockRelease(PartWALQueueState->lock);
    return true;
}

bool
PartWALQueueIsEmpty(void)
{
    bool empty;

    if (PartWALQueueState == NULL)
        return true;

    LWLockAcquire(PartWALQueueState->lock, LW_SHARED);
    empty = (PartWALQueueState->head == PartWALQueueState->tail);
    LWLockRelease(PartWALQueueState->lock);

    return empty;
}
