/*
 * shard_wal.c —— 分片账本的 WAL 资源管理器（设计见 shard_wal.h 头注释）
 */
#include "postgres.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "replication/syncrep.h"
#include "replication/walsender.h"
#include "replication/walsender_private.h"
#include "storage/lwlock.h"

#include "dtx_pending.h"
#include "shard_clog.h"
#include "shard_wal.h"
#include "shard_xid.h"
#include "tso.h"

static void shard_rmgr_redo(XLogReaderState *record);
static void shard_rmgr_desc(StringInfo buf, XLogReaderState *record);
static const char *shard_rmgr_identify(uint8 info);

static const RmgrData shard_rmgr = {
	.rm_name = SHARD_RMGR_NAME,
	.rm_redo = shard_rmgr_redo,
	.rm_desc = shard_rmgr_desc,
	.rm_identify = shard_rmgr_identify,
};

void
ShardWalRegisterRmgr(void)
{
	RegisterCustomRmgr(SHARD_RMGR_ID, &shard_rmgr);
}

XLogRecPtr
ShardWalLogPrepared(int64 gxid, int64 coord_gsid, int64 dtxid, int64 start_ts,
					int nxids, const uint32 *pairs)
{
	xl_shard_prepared hdr;

	if (nxids <= 0 || !XLogInsertAllowed())
		return InvalidXLogRecPtr;
	memset(&hdr, 0, sizeof(hdr));
	hdr.gxid = gxid;
	hdr.coord_gsid = coord_gsid;
	hdr.dtxid = dtxid;
	hdr.start_ts = start_ts;
	hdr.nxids = nxids;
	XLogBeginInsert();
	XLogRegisterData((char *) &hdr, offsetof(xl_shard_prepared, pairs));
	XLogRegisterData((char *) pairs, (Size) nxids * 2 * sizeof(uint32));
	return XLogInsert(SHARD_RMGR_ID, XLOG_SHARD_PREPARED);
}

XLogRecPtr
ShardWalLogVerdict(int64 gxid, bool committed, int64 commit_ts,
				   int nxids, const uint32 *pairs)
{
	xl_shard_verdict hdr;

	if (nxids <= 0 || !XLogInsertAllowed())
		return InvalidXLogRecPtr;
	memset(&hdr, 0, sizeof(hdr));
	hdr.gxid = gxid;
	hdr.commit_ts = committed ? commit_ts : 0;
	hdr.committed = committed ? 1 : 0;
	hdr.nxids = nxids;
	XLogBeginInsert();
	XLogRegisterData((char *) &hdr, offsetof(xl_shard_verdict, pairs));
	XLogRegisterData((char *) pairs, (Size) nxids * 2 * sizeof(uint32));
	return XLogInsert(SHARD_RMGR_ID, XLOG_SHARD_VERDICT);
}

XLogRecPtr
ShardWalLogTsoHwm(int64 hwm)
{
	xl_tso_hwm	xl;

	if (!XLogInsertAllowed())
		return InvalidXLogRecPtr;
	xl.hwm = hwm;
	XLogBeginInsert();
	XLogRegisterData((char *) &xl, sizeof(xl));
	return XLogInsert(SHARD_RMGR_ID, XLOG_SHARD_TSO_HWM);
}

XLogRecPtr
ShardWalLogTsoRaftHwm(int64 hwm)
{
	xl_tso_hwm	xl;

	if (!XLogInsertAllowed())
		return InvalidXLogRecPtr;
	xl.hwm = hwm;
	XLogBeginInsert();
	XLogRegisterData((char *) &xl, sizeof(xl));
	return XLogInsert(SHARD_RMGR_ID, XLOG_SHARD_TSO_RAFT_HWM);
}

/*
 * 发号水位推进。调用方持分片 xid 全局锁、在这一批的第一个号发出之前调 —— 用到这些号的
 * 元组记录在 WAL 里必然排在它后面，备库只要见到了元组，就一定先回放过这条水位。
 * 不等刷盘：主库自己的重启靠水位文件（发号前已 fsync），这条只给备库用。
 */
XLogRecPtr
ShardWalLogXidWatermark(Oid shard, uint32 wm)
{
	xl_shard_xid_wm xl;

	if (!XLogInsertAllowed())
		return InvalidXLogRecPtr;
	xl.shard = shard;
	xl.wm = wm;
	XLogBeginInsert();
	XLogRegisterData((char *) &xl, sizeof(xl));
	return XLogInsert(SHARD_RMGR_ID, XLOG_SHARD_XID_WM);
}

bool
ShardWalWaitDurable(XLogRecPtr lsn)
{
	XLogRecPtr	flushed;
	bool		defined;
	int			save_sc = synchronous_commit;

	if (XLogRecPtrIsInvalid(lsn))
		return true;
	XLogFlush(lsn);
	if (max_wal_senders == 0 || WalSndCtl == NULL)
		return true;
	defined = (((volatile WalSndCtlData *) WalSndCtl)->sync_standbys_status &
			   SYNC_STANDBY_DEFINED) != 0;
	if (!defined)
		return true;

	/*
	 * 这里等的是账本 / TSO 记录本身到达同步备库，跟本会话的 synchronous_commit 无关：
	 * 会话设了 off / local 也必须等（否则判决回执、发号会先于备库持久）。
	 * SyncRepWaitForLSN 内部看 SyncRepRequested()，临时抬到 remote_flush。
	 * SyncRepWaitForLSN 被取消 / 终止时会提前返回，所以返回后再核对一次确认位置。
	 */
	HOLD_INTERRUPTS();
	if (synchronous_commit <= SYNCHRONOUS_COMMIT_LOCAL_FLUSH)
		synchronous_commit = SYNCHRONOUS_COMMIT_REMOTE_FLUSH;
	SyncRepWaitForLSN(lsn, false);
	synchronous_commit = save_sc;
	RESUME_INTERRUPTS();

	LWLockAcquire(SyncRepLock, LW_SHARED);
	flushed = ((volatile WalSndCtlData *) WalSndCtl)->lsn[SYNC_REP_WAIT_FLUSH];
	defined = (((volatile WalSndCtlData *) WalSndCtl)->sync_standbys_status &
			   SYNC_STANDBY_DEFINED) != 0;
	LWLockRelease(SyncRepLock);
	return !defined || flushed >= lsn;
}

bool
ShardWalIsDurable(XLogRecPtr lsn)
{
	XLogRecPtr	flushed;
	bool		defined;

	if (XLogRecPtrIsInvalid(lsn))
		return true;
	if (GetFlushRecPtr(NULL) < lsn)
		return false;
	if (max_wal_senders == 0 || WalSndCtl == NULL)
		return true;
	LWLockAcquire(SyncRepLock, LW_SHARED);
	flushed = ((volatile WalSndCtlData *) WalSndCtl)->lsn[SYNC_REP_WAIT_FLUSH];
	defined = (((volatile WalSndCtlData *) WalSndCtl)->sync_standbys_status &
			   SYNC_STANDBY_DEFINED) != 0;
	LWLockRelease(SyncRepLock);
	return !defined || flushed >= lsn;
}

static void
shard_rmgr_redo(XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	char	   *data = XLogRecGetData(record);
	int			i;

	switch (info)
	{
		case XLOG_SHARD_PREPARED:
			{
				xl_shard_prepared *xl = (xl_shard_prepared *) data;

				for (i = 0; i < xl->nxids; i++)
				{
					Oid			shard = (Oid) xl->pairs[2 * i];
					TransactionId sxid = (TransactionId) xl->pairs[2 * i + 1];
					ShardClogSlot slot;

					/* 已有终局（判决记录先到 / 崩溃恢复重放）就不退回 PREPARED */
					if (ShardClogReadSlot(shard, sxid, &slot) &&
						(slot.status == TXN_COMMITTED || slot.status == TXN_ABORTED))
						continue;
					ShardClogSetPrepared(shard, sxid, xl->start_ts, xl->gxid);
					ShardXidRedoAdvance(shard, sxid);
				}
				DtxPendingReRegister(xl->gxid, xl->coord_gsid, xl->dtxid,
									 xl->start_ts, xl->nxids, xl->pairs);
				break;
			}
		case XLOG_SHARD_VERDICT:
			{
				xl_shard_verdict *xl = (xl_shard_verdict *) data;

				for (i = 0; i < xl->nxids; i++)
				{
					Oid			shard = (Oid) xl->pairs[2 * i];
					TransactionId sxid = (TransactionId) xl->pairs[2 * i + 1];

					ShardClogSetVerdict(shard, sxid, xl->committed != 0,
										xl->committed ? xl->commit_ts : 0);
					ShardXidRedoAdvance(shard, sxid);
				}
				if (xl->gxid != 0)
					DtxPendingFinalized(xl->gxid);
				break;
			}
		case XLOG_SHARD_TSO_HWM:
			TsoRedoHwm(((xl_tso_hwm *) data)->hwm);
			break;
		case XLOG_SHARD_TSO_RAFT_HWM:
			TsoRaftRedoAppliedHwm(((xl_tso_hwm *) data)->hwm);
			break;
		case XLOG_SHARD_XID_WM:
			{
				xl_shard_xid_wm *xl = (xl_shard_xid_wm *) data;

				ShardXidRedoRaiseWatermark(xl->shard, (TransactionId) xl->wm);
				break;
			}
		default:
			elog(PANIC, "shard_rmgr_redo: unknown op code %u", info);
	}
}

static void
shard_rmgr_desc(StringInfo buf, XLogReaderState *record)
{
	uint8		info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	char	   *data = XLogRecGetData(record);

	if (info == XLOG_SHARD_PREPARED)
	{
		xl_shard_prepared *xl = (xl_shard_prepared *) data;

		appendStringInfo(buf, "gxid " INT64_FORMAT " dtxid " INT64_FORMAT " nxids %d",
						 xl->gxid, xl->dtxid, xl->nxids);
	}
	else if (info == XLOG_SHARD_VERDICT)
	{
		xl_shard_verdict *xl = (xl_shard_verdict *) data;

		appendStringInfo(buf, "gxid " INT64_FORMAT " %s cts " INT64_FORMAT " nxids %d",
						 xl->gxid, xl->committed ? "COMMIT" : "ABORT",
						 xl->commit_ts, xl->nxids);
	}
	else if (info == XLOG_SHARD_TSO_HWM || info == XLOG_SHARD_TSO_RAFT_HWM)
		appendStringInfo(buf, "hwm " INT64_FORMAT, ((xl_tso_hwm *) data)->hwm);
	else if (info == XLOG_SHARD_XID_WM)
		appendStringInfo(buf, "shard %u wm %u",
						 ((xl_shard_xid_wm *) data)->shard, ((xl_shard_xid_wm *) data)->wm);
}

static const char *
shard_rmgr_identify(uint8 info)
{
	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_SHARD_PREPARED:
			return "PREPARED";
		case XLOG_SHARD_VERDICT:
			return "VERDICT";
		case XLOG_SHARD_TSO_HWM:
			return "TSO_HWM";
		case XLOG_SHARD_TSO_RAFT_HWM:
			return "TSO_RAFT_HWM";
		case XLOG_SHARD_XID_WM:
			return "XID_WM";
	}
	return NULL;
}
