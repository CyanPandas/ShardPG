/*
 * shard_wal.h —— 分片账本的 WAL 资源管理器（2026-10-17，路线 A：节点级同步流复制）
 *
 * 普通提交的分片判决随提交记录走（补丁 0007：分片 xid 对 + commit_ts，redo 钩子重建）。
 * 但有两类账本写入**不随任何提交记录**：
 *
 *   1. 2PC 参与者 PREPARE 时的 PREPARED 落账（槽位 + gxid + 未决登记）；
 *   2. 提交记录之外落的判决 —— 参与者收到协调者决议后落判决（dtx_pending_apply_verdict）、
 *      读者就地补落、ROLLBACK PREPARED 回调、升主序列的 verdict_apply。
 *
 * 它们原来只写本地文件。物理备库回放 pg_wal 拿不到，切主后这些分片 xid 在新主上是
 * 空槽（RUNNING）：2PC 改过的行不可见（实测每仓库 stock 约 0.5% 的行"消失"），
 * 升主认领还会把它们改判 ABORTED。
 *
 * 这里把这两类写入各记一条 WAL 记录，redo 时在备库上原样重建：
 *   XLOG_SHARD_PREPARED  —— 写 PREPARED 槽、补未决登记、推进发号影子水位；
 *   XLOG_SHARD_VERDICT   —— 写终局判决、注销未决登记、推进发号影子水位。
 *
 * 持久性次序：
 *   · PREPARED 记录在 PrepareTransaction 的 at_prepare 钩子里写，排在 PREPARE 记录之前，
 *     随 EndPrepare 的刷盘与同步复制等待一起落到多数派 —— 投票之前必已持久。
 *   · 决议路径的 VERDICT 记录：写记录 → 落槽位（本机可见）→ 等它到达同步备库 →
 *     才注销未决登记。登记注销之前本节点不会向协调组回执（dtx_ack_sweep 看登记），
 *     协调组收齐回执之前不会 FORGET 决议 —— 所以哪怕记录没赶上切主，新主上的
 *     PREPARED 槽 + 未决登记仍能问到决议、补落同样的判决。
 */
#ifndef SHARD_WAL_H
#define SHARD_WAL_H

#include "postgres.h"
#include "access/xlogdefs.h"

/* 自定义资源管理器号（RM_MIN_CUSTOM_ID..RM_MAX_CUSTOM_ID 区间内，集群内唯一即可） */
#define SHARD_RMGR_ID			147
#define SHARD_RMGR_NAME			"pg_partdist_shard"

#define XLOG_SHARD_PREPARED		0x00
#define XLOG_SHARD_VERDICT		0x10
#define XLOG_SHARD_TSO_HWM		0x20	/* TSO 高水位预留（协调者切主后从备库接着发号） */

typedef struct xl_shard_prepared
{
	int64		gxid;
	int64		coord_gsid;
	int64		dtxid;
	int64		start_ts;
	int32		nxids;
	int32		pad;
	uint32		pairs[FLEXIBLE_ARRAY_MEMBER];	/* 2*nxids：shard, sxid 交错 */
} xl_shard_prepared;

typedef struct xl_shard_verdict
{
	int64		gxid;			/* 0 = 不关联未决登记 */
	int64		commit_ts;		/* ABORTED 恒 0 */
	int32		committed;
	int32		nxids;
	uint32		pairs[FLEXIBLE_ARRAY_MEMBER];
} xl_shard_verdict;

typedef struct xl_tso_hwm
{
	int64		hwm;			/* 已持久预留的上界：发出的号都严格小于它 */
} xl_tso_hwm;

extern void ShardWalRegisterRmgr(void);
extern XLogRecPtr ShardWalLogTsoHwm(int64 hwm);
extern XLogRecPtr ShardWalLogPrepared(int64 gxid, int64 coord_gsid, int64 dtxid,
									  int64 start_ts, int nxids, const uint32 *pairs);
extern XLogRecPtr ShardWalLogVerdict(int64 gxid, bool committed, int64 commit_ts,
									 int nxids, const uint32 *pairs);
/* 本地刷到 lsn 并等同步备库确认；返回同步备库是否已确认（没有同步备库时恒 true） */
extern bool ShardWalWaitDurable(XLogRecPtr lsn);
/* 不等待：本地已刷且（有同步备库时）同步备库已确认到 lsn */
extern bool ShardWalIsDurable(XLogRecPtr lsn);

#endif							/* SHARD_WAL_H */
