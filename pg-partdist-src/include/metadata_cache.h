#ifndef METADATA_CACHE_H
#define METADATA_CACHE_H

#include "pg_partdist.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"

/* ---- shared memory state ---- */

typedef struct PartdistSharedState
{
    LWLock     *partition_lock;         /* pointer into named LWLock tranche */
    LWLock     *node_lock;
    int64       metadata_generation;    /* bumped on any metadata write */
    uint64      partition_hits;
    uint64      partition_misses;
    uint64      node_hits;
    uint64      node_misses;
} PartdistSharedState;

/*
 * Partition hash entry: hash key must be the first field (Oid).
 * All data is fixed-size so entries live safely in shared memory.
 */
typedef struct PartitionHashEntry
{
    Oid     partition_id;                           /* HASH KEY — must be first */
    int32   primary_node;
    int32   secondary_nodes[PARTDIST_MAX_SECONDARIES];
    int     num_secondaries;
    int64   meta_version;                           /* generation when populated */
    bool    is_valid;
} PartitionHashEntry;

/*
 * Node hash entry: hash key must be the first field (int32).
 */
typedef struct NodeHashEntry
{
    int32       node_id;                            /* HASH KEY — must be first */
    char        hostname[PARTDIST_MAX_HOSTNAME];
    int         port;
    NodeStatus  status;
    int64       meta_version;
    bool        is_valid;
} NodeHashEntry;

/* ---- module-level pointers (set in shmem_startup_hook) ---- */
extern PartdistSharedState *PartdistState;
extern HTAB                *PartitionMapHash;
extern HTAB                *NodeMapHash;

/* ---- internal helpers exposed to pg_partdist.c ---- */
extern Size pg_partdist_shmem_size(void);
extern void pg_partdist_shmem_request_hook(void);
extern void pg_partdist_shmem_startup_hook(void);

#endif /* METADATA_CACHE_H */
