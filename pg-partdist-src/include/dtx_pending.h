/*
 * dtx_pending.h — T4.5：参与者节点的未决 2PC 登记表（决议收敛的地基）
 *
 * 语义：每笔含分片写的 prepared 事务在 PREPARE 时登记
 * (gxid, coord_gsid, dtxid, start_ts, pairs[])，直到**判决落进分片 clog**
 * 才注销（COMMIT PREPARED 本身不注销——终局要等决议）。登记持久化在
 * $PGDATA/pg_shard_clog/dtx_pending.jrnl（OPEN fsync 先于 PREPARE 的 WAL
 * 刷盘 ⇒ "prepared 存在 ⇒ 登记必在"），启动时重放重建 shmem 表——
 * 这补上了"COMMIT PREPARED 之后、判决落账之前崩溃"的孤儿窗口。
 *
 * 收敛通道（§3.3/§4.2）：
 *   · 清扫：心跳工作者自连触发 partdist.dtx_pending_sweep()——逐项向协调组
 *     leader 只读问询（partdist.dtx_peek，绝不写推定中止），学到判决即幂等
 *     落分片 clog 并注销；
 *   · 读者 ③：可见性挂钩撞 PREPARED 且槽 start_ts ≤ 快照时，经缓存自连
 *     问询同一决议，学到即回写——同一 gxid 每事务只问一次（memo），
 *     保证同快照内跨分片答案一致；问询未果一律不可见、不阻塞、不安装。
 */
#ifndef DTX_PENDING_H
#define DTX_PENDING_H

#include "postgres.h"

#define DTX_PENDING_MAX			128
#define DTX_PENDING_MAX_PAIRS	16	/* = SHARD_XID_MAX_PER_XACT */

typedef struct DtxPendingEntry
{
	int64		gxid;			/* 0 = 空槽 */
	int64		coord_gsid;
	int64		dtxid;
	int64		start_ts;
	int32		nxids;
	uint32		pairs[2 * DTX_PENDING_MAX_PAIRS];	/* (shard, sxid)… */
} DtxPendingEntry;

extern void RequestDtxPendingShmem(void);
extern void DtxPendingShmemInit(void);

extern void DtxPendingRegister(int64 gxid, int64 coord_gsid, int64 dtxid,
							   int64 start_ts, int nxids, const uint32 *pairs);
extern void DtxPendingReRegister(int64 gxid, int64 coord_gsid, int64 dtxid,
								 int64 start_ts, int nxids, const uint32 *pairs);
extern void DtxPendingFinalized(int64 gxid);
extern bool DtxPendingLookup(int64 gxid, DtxPendingEntry *out);
extern int	DtxPendingCount(void);

/* 清扫核心（需要 backend 语境：SPI + libpq）；返回本轮注销数 */
extern int	DtxPendingSweep(void);

/*
 * 读者 ③（可见性挂钩内调用；绝不 ERROR、绝不阻塞式等待）。
 * 返回 0=无从判定（不可见处置） 1=COMMIT（*cts_out 有效） 2=ABORT。
 */
extern int	DtxReaderResolve(int64 gxid, int64 *cts_out);

/* 心跳工作者用：自连触发一轮清扫（无 DB 语境，纯 libpq） */
extern void DtxPendingSelfTriggerSweep(void);

/* dtx_participant.c 的 PRE_PREPARE 捕获（at-prepare 钩子取用） */
extern int64 PartDistPendingDtxid(void);

#endif							/* DTX_PENDING_H */
