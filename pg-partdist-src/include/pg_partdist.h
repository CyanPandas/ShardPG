#ifndef PG_PARTDIST_H
#define PG_PARTDIST_H

#include "postgres.h"
#include "fmgr.h"

/* ---- compile-time limits ---- */
#define PARTDIST_MAX_SECONDARIES        32
#define PARTDIST_MAX_HOSTNAME           256
#define PARTDIST_PARTITION_MAX_ENTRIES  1024
#define PARTDIST_NODE_MAX_ENTRIES       64

/* GUC: which node ID is "this" node (-1 = not configured) */
extern int pg_partdist_local_node_id;

/* ---- core enums / structs used by both layers ---- */

typedef enum NodeStatus
{
    NODE_ACTIVE,
    NODE_DOWN,
    NODE_SYNCING
} NodeStatus;

/*
 * Fixed-size partition entry (safe in shared memory and on the stack).
 * secondary_nodes[] is stored inline; num_secondaries records how many
 * slots are in use.
 */
typedef struct PartitionMapEntry
{
    Oid     partition_id;
    int32   primary_node;
    int32   secondary_nodes[PARTDIST_MAX_SECONDARIES];
    int     num_secondaries;
    int64   version;
} PartitionMapEntry;

/*
 * Fixed-size node entry (safe in shared memory).
 */
typedef struct NodeMapEntry
{
    int32       node_id;
    char        hostname[PARTDIST_MAX_HOSTNAME];
    int         port;
    NodeStatus  status;
} NodeMapEntry;

/* ---- routing types ---- */

typedef enum RouteStatus
{
    ROUTE_LOCAL,        /* primary is this node — execute here */
    ROUTE_REMOTE,       /* primary is a remote node */
    ROUTE_NOT_FOUND,    /* partition_id not in partition_map */
    ROUTE_NODE_DOWN     /* primary node status is 'down' */
} RouteStatus;

typedef struct WriteRequest
{
    Oid             partition_id;
    TransactionId   xid;
    CommandId       cid;
    char           *query_string;
} WriteRequest;

/* ---- public API (implemented in respective .c files) ---- */

/* metadata_cache.c */
extern int32        GetPartitionPrimary(Oid partition_id);
extern bool         GetPartitionEntry(Oid partition_id, PartitionMapEntry *out);
extern NodeMapEntry *GetNodeInfo(int32 node_id);   /* palloc'd copy, caller frees */
extern void         InvalidatePartdistCache(void);

/* write_router.c */
extern RouteStatus  RouteWriteRequest(WriteRequest *req);

/* partition_wal.c — forward declaration (full API in partition_wal.h) */
struct QueryDesc;
extern void pg_partdist_executor_finish(struct QueryDesc *queryDesc);

#endif /* PG_PARTDIST_H */
