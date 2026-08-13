/*
 * shard_stamp.h  (pg_partdist patch 0005)
 *
 * Hook that lets an extension supply per-shard tuple xids for shard-managed
 * relations.  Contract:
 *   - returns false if the relation is not shard-managed (caller uses the
 *     native xid as usual);
 *   - returns true if it is; when assign==true, *sxid is set to the shard
 *     xid bound to the current top-level transaction for this relation's
 *     shard (allocated lazily on first use; the hook must ereport(ERROR)
 *     for cases it does not support, e.g. subtransactions).
 * The WAL record header xid always stays native; insert/multi_insert/update
 * records carry the shard xid as the LAST sizeof(TransactionId) bytes of
 * their main data, flagged by XLH_INSERT_SHARD_XID / XLH_UPDATE_SHARD_XID.
 * delete/lock need no extra payload: their xmax already travels in the
 * record body.
 */
#ifndef SHARD_STAMP_H
#define SHARD_STAMP_H

struct RelationData;

typedef bool (*shard_relation_xid_hook_type) (struct RelationData *relation,
											  bool assign,
											  TransactionId *sxid);
extern PGDLLIMPORT shard_relation_xid_hook_type shard_relation_xid_hook;


/*
 * patch 0006: extension-supplied tuple visibility for shard-managed
 * relations.
 *
 * Shard tuples carry per-shard xids in xmin/xmax, so every native
 * clog/procarray consultation would misjudge them; each Satisfies* entry
 * point forks on these hooks before touching native state.  Contract:
 *   - is_shard_rel must be cheap (called on visibility hot paths) and must
 *     not touch catalogs;
 *   - each satisfies_* returns true if it decided (*visible / *result
 *     filled in), false = not shard-managed, caller continues natively;
 *   - implementations must neither read nor write hint bits;
 *   - xmax_wait translates a shard xmax to its holder's native xid and
 *     blocks until that transaction ends; the caller then re-evaluates
 *     from scratch (heap_delete/heap_update goto l1/l2).
 * HeapTupleSatisfiesToast is deliberately NOT forked (it consults no
 * clog/procarray for normal tuples), but toast DELETION does reach
 * HeapTupleSatisfiesUpdate -- the extension's is_shard_rel therefore
 * must recognize toast relations of shard tables (it keeps a
 * backend-local toast->owner map fed by the stamping hook, which always
 * runs first within heap_delete).
 */
#include "access/htup.h"
#include "access/tableam.h"
#include "storage/buf.h"
#include "storage/itemptr.h"
#include "storage/lmgr.h"
#include "utils/snapshot.h"

typedef struct ShardVisibilityHooks
{
	bool		(*is_shard_rel) (Oid tableOid);
	bool		(*satisfies_mvcc) (HeapTuple htup, Snapshot snapshot,
								   Buffer buffer, bool *visible);
	bool		(*satisfies_self) (HeapTuple htup, Buffer buffer,
								   bool *visible);
	bool		(*satisfies_dirty) (HeapTuple htup, Snapshot snapshot,
									Buffer buffer, bool *visible);
	bool		(*satisfies_update) (HeapTuple htup, CommandId curcid,
									 Buffer buffer, TM_Result *result);
	void		(*xmax_wait) (struct RelationData *relation,
							  TransactionId sxid,
							  ItemPointer ctid, XLTW_Oper oper);
} ShardVisibilityHooks;

extern PGDLLIMPORT const ShardVisibilityHooks *shard_visibility_hooks;

#endif							/* SHARD_STAMP_H */
