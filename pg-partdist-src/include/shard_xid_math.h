/* 32-bit shard XIDs only. There is no transaction epoch or extended XID.
 * Test builds may shorten the ring to exercise exactly the same boundary code.
 */
#ifndef SHARD_XID_MATH_H
#define SHARD_XID_MATH_H

#include <stdint.h>
#include <stdbool.h>

#ifndef SHARD_XID_TEST_BITS
#define SHARD_XID_TEST_BITS 32
#endif
#if SHARD_XID_TEST_BITS < 8 || SHARD_XID_TEST_BITS > 32
#error "SHARD_XID_TEST_BITS must be between 8 and 32"
#endif

#define SHARD_XID_MASK (UINT32_MAX >> (32 - SHARD_XID_TEST_BITS))
#define SHARD_XID_HALF ((SHARD_XID_MASK >> 1) + UINT32_C(1))

static inline uint32_t
ShardXidDistance(uint32_t from, uint32_t to)
{
    return (to - from) & SHARD_XID_MASK;
}

static inline uint32_t
ShardXidNormalize(uint32_t xid)
{
    xid &= SHARD_XID_MASK;
    return xid < 3 ? 3 : xid;
}

static inline uint32_t
ShardXidAdvance(uint32_t xid, uint32_t distance)
{
    return ShardXidNormalize((xid + distance) & SHARD_XID_MASK);
}

/* Zero denotes an absent legacy watermark, never a normal transaction. */
static inline bool
ShardXidPrecedes(uint32_t a, uint32_t b)
{
    if (a == 0 || b == 0)
        return a == 0 && b != 0;
    return a != b && ShardXidDistance(a, b) < SHARD_XID_HALF;
}

static inline bool
ShardXidFollows(uint32_t a, uint32_t b)
{
    return ShardXidPrecedes(b, a);
}

static inline uint32_t
ShardXidLater(uint32_t a, uint32_t b)
{
    return ShardXidFollows(a, b) ? a : b;
}

static inline bool
ShardXidInRange(uint32_t xid, uint32_t from, uint32_t to)
{
    return ShardXidDistance(from, xid) < ShardXidDistance(from, to);
}

#endif
