/*
 * partwal_queue.h
 * Shared-memory ring buffer for parwal-2.0 hook → demux communication.
 *
 * When ExecutorFinish or ProcessUtility detects a write to a tracked
 * partition, it enqueues a PartWALQueueEntry into this ring buffer.
 * The Demux Worker drains the queue, scans the corresponding pg_wal
 * range by relfilenode, and writes self-contained PartWALRecord entries
 * to pg_parwal/<partition_id>/.
 */
#ifndef PARTWAL_QUEUE_H
#define PARTWAL_QUEUE_H

#include "postgres.h"
#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"

/* ------------------------------------------------------------------ */
/* Ring buffer configuration                                            */
/* ------------------------------------------------------------------ */

#define PARTWAL_QUEUE_SIZE       1024   /* number of slots in the ring buffer */
#define PARTWAL_QUEUE_SHMEM_NAME "pg_partdist_queue"
#define PARTWAL_QUEUE_TRANCHE    "pg_partdist_queue"

/* ------------------------------------------------------------------ */
/* Queue entry                                                          */
/* ------------------------------------------------------------------ */

typedef struct PartWALQueueEntry
{
    Oid             partition_id;   /* OID of the partition (shard table)   */
    RelFileNumber   relfilenode;    /* relfilenode of partition table        */
    XLogRecPtr      start_lsn;     /* WAL insert ptr captured in ExecStart  */
    XLogRecPtr      end_lsn;       /* WAL flush ptr captured in ExecFinish  */
} PartWALQueueEntry;

/* ------------------------------------------------------------------ */
/* Ring buffer shared state                                             */
/* ------------------------------------------------------------------ */

typedef struct PartWALQueue
{
    LWLock             *lock;               /* protects head/tail/entries   */
    uint32              head;               /* next slot to read (demux)    */
    uint32              tail;               /* next slot to write (backend) */
    uint64              overflow_count;     /* entries dropped due to full  */
    PartWALQueueEntry   entries[PARTWAL_QUEUE_SIZE];
} PartWALQueue;

/* Module-level pointer set in shmem_startup_hook */
extern PartWALQueue *PartWALQueueState;

/* ------------------------------------------------------------------ */
/* Lifecycle                                                            */
/* ------------------------------------------------------------------ */

extern void   RequestPartWALQueueShmem(void);
extern Size   PartWALQueueShmemSize(void);
extern void   PartWALQueueShmemInit(void);

/* ------------------------------------------------------------------ */
/* Producer API (called from hooks)                                     */
/* ------------------------------------------------------------------ */

/*
 * PartWALQueueEnqueue — append one entry to the ring buffer.
 * If the buffer is full, the entry is silently dropped and overflow_count
 * is incremented (crash recovery via checkpoint handles gaps).
 */
extern void PartWALQueueEnqueue(Oid partition_id,
                                RelFileNumber relfilenode,
                                XLogRecPtr start_lsn,
                                XLogRecPtr end_lsn);

/* ------------------------------------------------------------------ */
/* Consumer API (called from demux worker)                              */
/* ------------------------------------------------------------------ */

/*
 * PartWALQueueDequeue — remove one entry from the ring buffer.
 * Returns true and fills *entry if a slot was available; false if empty.
 */
extern bool PartWALQueueDequeue(PartWALQueueEntry *entry);

/*
 * PartWALQueueIsEmpty — true if no entries are pending.
 * Callers must be prepared for race: a backend may enqueue immediately
 * after this returns true.
 */
extern bool PartWALQueueIsEmpty(void);

#endif /* PARTWAL_QUEUE_H */
