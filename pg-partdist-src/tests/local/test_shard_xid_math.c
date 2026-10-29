#include "shard_xid_math.h"
#include <assert.h>
#include <stdio.h>

int main(void)
{
    uint32_t start, offset;
    assert(sizeof(ShardXidAdvance(3, 1)) == 4);
    assert(ShardXidAdvance(SHARD_XID_MASK, 1) == 3);
    assert(ShardXidAdvance(SHARD_XID_MASK - 1, 1) == SHARD_XID_MASK);
    assert(ShardXidPrecedes(SHARD_XID_MASK, 3));
    assert(!ShardXidPrecedes(3, SHARD_XID_MASK));
    assert(ShardXidDistance(SHARD_XID_MASK, 3) == 4);
    assert(!ShardXidPrecedes(7, 7));
    assert(!ShardXidPrecedes(3, ShardXidAdvance(3, SHARD_XID_HALF)));
    assert(!ShardXidPrecedes(ShardXidAdvance(3, SHARD_XID_HALF), 3));
    for (start = 3; start < 67; start++)
        for (offset = 1; offset < 64; offset++)
        {
            uint32_t a = (SHARD_XID_MASK - start) & SHARD_XID_MASK;
            uint32_t b = ShardXidAdvance(a, offset);
            assert(ShardXidPrecedes(a, b));
            assert(ShardXidFollows(b, a));
            assert(ShardXidLater(a, b) == b);
            assert(ShardXidInRange(a, a, b));
            assert(!ShardXidInRange(b, a, b));
        }
#if SHARD_XID_TEST_BITS == 8
    /* Exhaust the accelerated circle: every valid pair inside half a circle. */
    for (start = 3; start <= SHARD_XID_MASK; start++)
        for (offset = 3; offset <= SHARD_XID_MASK; offset++)
        {
            uint32_t d = ShardXidDistance(start, offset);
            assert(ShardXidPrecedes(start, offset) == (d && d < SHARD_XID_HALF));
        }
#endif
    printf("PASS: %u-bit circular xid arithmetic; identifiers remain uint32_t\n", SHARD_XID_TEST_BITS);
    return 0;
}
