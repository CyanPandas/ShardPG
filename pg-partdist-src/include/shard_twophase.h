#ifndef SHARD_TWOPHASE_H
#define SHARD_TWOPHASE_H

#include "postgres.h"

/* Two-phase membership is independent of the IN_PROGRESS clog state. The
 * durable per-shard index is rebuilt from native 2PC records / shard markers. */
extern void ShardTwoPhaseRemember(Oid shard, TransactionId sxid, uint64 gxid);
extern bool ShardTwoPhaseIsPrepared(Oid shard, TransactionId sxid);
extern void ShardTwoPhaseForget(Oid shard, TransactionId sxid);
extern void ShardTwoPhaseClearRange(Oid shard, TransactionId from, TransactionId to, bool all);
extern void ShardTwoPhasePrepare(Oid shard, TransactionId sxid, int64 start_ts, int64 gxid);

#endif
