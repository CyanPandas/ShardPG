/*
 * dtx_pending.c — T4.5：未决 2PC 登记表 + 持久日志 + 决议收敛（清扫/读者问询）
 *
 * 设计脉络见 dtx_pending.h 头注释与 TX_TSO_MVCC_DESING.md §3.3/§4.2。
 * 拉取收敛架构：不依赖决议方推送（广播允许丢失的极限情形 = 从未推送），
 * 一切判决从协调组 leader 的 partdist.dtx_decision 只读问询而来。
 */
#include "postgres.h"
#include "lib/stringinfo.h"

#include "dtx_pending.h"
#include "shard_wal.h"				/* 2026-10-17：判决记 WAL */
#include "shard_clog.h"
#include "gxid.h"					/* T7.1：从 gxid 还原本地 xid */
#include "partwal_sync.h"			/* T7.1：判决标记补发 */
#include "partition_wal_header.h"	/* T7.1：XLOG_XACT_* / MARKER 载荷 */
#include "partition_wal_writer.h"	/* partwal_fast_commit */

#include "access/htup_details.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"	/* PostPortNumber */
#include "storage/fd.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/guc.h"			/* P7-N12：GetConfigOption 读 pg_raft.node_id */
#include "access/xact.h"

#include <fcntl.h>
#include <unistd.h>

/* ---- shmem ---- */

/*
 * P7-N86：最近学到的判决环。登记注销之后，本节点仍可能在登记里的分片上读到
 * PREPARED 槽（实测：灌数批次与快照隔离那一步，清扫"收敛 1 笔"之后十几秒内，
 * 快照读与就地闭合仍在同一 (分片, xid) 上看到 PREPARED）。此时本机已经没有登记，
 * 读者只能走就地 in-doubt 闭合 —— 它要从头扫整条分区流，数据一多就超过自连
 * 5 s 的语句超时（N83），读因此卡到下一次重启。判决对一个 gxid 是不可变的，
 * 学到过就记下来，读者撞上这种槽时直接按它补落本地分片 clog。
 */
#define DTX_RECENT_MAX	1024

typedef struct DtxRecentEnt
{
	int64		gxid;
	int32		verdict;		/* 1 = COMMIT, 2 = ABORT */
	int64		cts;
} DtxRecentEnt;

typedef struct DtxPendingCtl
{
	slock_t		mutex;
	int			nused;
	/*
	 * TPC-C 分支：gxid / dtxid 的紧凑镜像（与 e[] 同下标，0 = 空槽）。每项约 2 KB，
	 * 原来按 e[i].gxid 跨 2 KB 步长扫 1024 项 = 上千次缓存缺失，全程持自旋锁；
	 * 读者、广播、登记都在扫，128 终端下这把锁成了排队点。镜像扫 8 KB 连续内存。
	 */
	int64		gx[DTX_PENDING_MAX];
	int64		dx[DTX_PENDING_MAX];
	/*
	 * 2026-10-17（路线 A）：判决已落槽位、判决 WAL 的 LSN（0 = 还没落）。前台路径（决议广播、
	 * 读者问到）落完不等同步备库，登记留着；清扫看到这里非 0 就不再问协调组，只等这条 WAL
	 * 持久后注销。不进持久日志：重启后登记重放、判决由清扫重新问来。
	 */
	XLogRecPtr	vlsn[DTX_PENDING_MAX];
	DtxPendingEntry e[DTX_PENDING_MAX];
	int			recent_next;
	DtxRecentEnt recent[DTX_RECENT_MAX];
} DtxPendingCtl;

static DtxPendingCtl *PendingCtl = NULL;

/* ---- 持久日志 ---- */

#define DTX_JRNL_MAGIC	0x44545850	/* 'DTXP' */
#define DTX_JRNL_OPEN	1
#define DTX_JRNL_CLOSE	2

typedef struct DtxJrnlRec
{
	uint32		magic;
	uint32		kind;
	DtxPendingEntry ent;		/* CLOSE 只有 gxid 有意义 */
} DtxJrnlRec;

static void
dtx_jrnl_path(char *path, size_t len)
{
	snprintf(path, len, "%s/pg_shard_clog/dtx_pending.jrnl", DataDir);
}

static void
dtx_jrnl_ensure_dir(void)
{
	char		path[MAXPGPATH];

	snprintf(path, MAXPGPATH, "%s/pg_shard_clog", DataDir);
	if (MakePGDirectory(path) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建目录 \"%s\": %m", path)));
}

/* 追加一条日志记录；do_fsync=true 时落盘后才返回（OPEN 的持久化点） */
static void
dtx_jrnl_append(uint32 kind, const DtxPendingEntry *ent, bool do_fsync)
{
	char		path[MAXPGPATH];
	int			fd;
	DtxJrnlRec	rec;

	dtx_jrnl_ensure_dir();
	dtx_jrnl_path(path, sizeof(path));

	memset(&rec, 0, sizeof(rec));
	rec.magic = DTX_JRNL_MAGIC;
	rec.kind = kind;
	rec.ent = *ent;

	fd = BasicOpenFile(path, O_WRONLY | O_APPEND | O_CREAT | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 打不开未决 2PC 日志 \"%s\": %m", path)));
	if (write(fd, &rec, sizeof(rec)) != sizeof(rec))
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno ? save_errno : ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 未决 2PC 日志写入失败: %m")));
	}
	if (do_fsync && pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 未决 2PC 日志 fsync 失败: %m")));
	}
	close(fd);
}

/* ---- shmem 内部操作（调用方持 mutex） ---- */

static DtxPendingEntry *
pending_find_locked(int64 gxid)
{
	int			i;

	for (i = 0; i < DTX_PENDING_MAX; i++)
		if (PendingCtl->gx[i] == gxid)
			return &PendingCtl->e[i];
	return NULL;
}

/* pending_insert_locked 的结果（自旋锁里不能 elog，告警由调用方放锁后打） */
#define PENDING_INS_OK			0
#define PENDING_INS_FULL		1	/* 登记表满，整条没登上 */
#define PENDING_INS_PAIRS_FULL	2	/* 合并后分片数超上限，超出的没登上 */

static int
pending_insert_locked(const DtxPendingEntry *ent)
{
	DtxPendingEntry *slot = pending_find_locked(ent->gxid);
	int			i,
				j;
	int			rc = PENDING_INS_OK;

	if (slot != NULL)
	{
		/*
		 * ★ P7-N82（2026-10-13）：同一 gxid 在本节点**多次** PREPARE —— Citus 一笔事务
		 * 在同一台 worker 上可以开多条连接（每条连接各自 PREPARE 一个本地事务、带着
		 * 各自写过的分片）。原来这里整条覆盖，后到的登记把先到那条连接的分片清单
		 * 冲掉：判决只落到后者的分片上，前者的分片 clog 永远停在 PREPARED、本机又
		 * 查不到登记，快照读一直报"判决暂时无法确定"（演示二第 4 步红包实测：
		 * w4 上 citus_…_27_6 与 _27_12 两条连接，P096 那份被冲掉，读卡死半小时）。
		 * 改为并集：(shard, sxid) 去重追加。日志重放走同一函数，多条 OPEN 同样并起来。
		 */
		for (i = 0; i < ent->nxids; i++)
		{
			bool		have = false;

			for (j = 0; j < slot->nxids; j++)
				if (slot->pairs[2 * j] == ent->pairs[2 * i] &&
					slot->pairs[2 * j + 1] == ent->pairs[2 * i + 1])
				{
					have = true;
					break;
				}
			if (have)
				continue;
			if (slot->nxids >= DTX_PENDING_MAX_PAIRS)
			{
				rc = PENDING_INS_PAIRS_FULL;
				break;
			}
			slot->pairs[2 * slot->nxids] = ent->pairs[2 * i];
			slot->pairs[2 * slot->nxids + 1] = ent->pairs[2 * i + 1];
			slot->nxids++;
		}
		return rc;
	}

	slot = pending_find_locked(0);
	if (slot == NULL)
		return PENDING_INS_FULL;	/* 满了：不覆盖别人（覆盖 = 那笔事务永失收敛通道） */
	PendingCtl->nused++;
	*slot = *ent;
	PendingCtl->gx[slot - PendingCtl->e] = ent->gxid;
	PendingCtl->dx[slot - PendingCtl->e] = ent->dtxid;
	PendingCtl->vlsn[slot - PendingCtl->e] = InvalidXLogRecPtr;
	return PENDING_INS_OK;
}

static void
pending_insert_warn(int rc, int64 gxid)
{
	/* 128 槽 × 节点级在测试与常规负载下到不了；到了先解决泄漏源 */
	if (rc == PENDING_INS_FULL)
		elog(WARNING,
			 "pg_partdist: 未决 2PC 登记表已满（%d），gxid=" INT64_FORMAT
			 " 未登记——该事务只能靠恢复守护收敛", DTX_PENDING_MAX, gxid);
	else if (rc == PENDING_INS_PAIRS_FULL)
		elog(WARNING,
			 "pg_partdist: 未决 2PC 登记 gxid=" INT64_FORMAT " 合并后分片数超出上限 %d，"
			 "超出部分未登记——只能靠读者就地闭合收敛", gxid, DTX_PENDING_MAX_PAIRS);
}

static void
pending_remove_locked(int64 gxid)
{
	DtxPendingEntry *slot = pending_find_locked(gxid);

	if (slot != NULL)
	{
		PendingCtl->gx[slot - PendingCtl->e] = 0;
		PendingCtl->dx[slot - PendingCtl->e] = 0;
		/* 只清头部即可：空槽靠 gx[] 判定，pairs 下次登记整条覆盖 */
		slot->gxid = 0;
		slot->nxids = 0;
		PendingCtl->nused--;
	}
}

/* ---- 启动重放（shmem startup hook 语境，postmaster/单进程） ---- */

static void
dtx_jrnl_load_and_compact(void)
{
	char		path[MAXPGPATH];
	FILE	   *fp;
	DtxJrnlRec	rec;
	int			nopen = 0;
	int			nrec = 0;

	dtx_jrnl_path(path, sizeof(path));
	fp = AllocateFile(path, PG_BINARY_R);
	if (fp == NULL)
		return;					/* 没有日志 = 没有未决 */

	while (fread(&rec, sizeof(rec), 1, fp) == 1)
	{
		nrec++;
		if (rec.magic != DTX_JRNL_MAGIC)
			break;				/* 尾部半条/损坏：后面的不可信，止步 */
		if (rec.kind == DTX_JRNL_OPEN)
			pending_insert_warn(pending_insert_locked(&rec.ent), rec.ent.gxid);
		else if (rec.kind == DTX_JRNL_CLOSE)
			pending_remove_locked(rec.ent.gxid);
	}
	FreeFile(fp);
	nopen = PendingCtl->nused;

	/* 压实：只留 OPEN 项重写（临时文件 + durable_rename） */
	if (nrec > nopen)
	{
		char		tmp[MAXPGPATH];
		int			fd;
		int			i;
		bool		ok = true;

		snprintf(tmp, sizeof(tmp), "%s.tmp", path);
		fd = BasicOpenFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
		if (fd < 0)
			return;				/* 压不动就不压，功能不受影响 */
		for (i = 0; i < DTX_PENDING_MAX && ok; i++)
		{
			if (PendingCtl->e[i].gxid == 0)
				continue;
			memset(&rec, 0, sizeof(rec));
			rec.magic = DTX_JRNL_MAGIC;
			rec.kind = DTX_JRNL_OPEN;
			rec.ent = PendingCtl->e[i];
			if (write(fd, &rec, sizeof(rec)) != sizeof(rec))
				ok = false;
		}
		if (ok && pg_fsync(fd) != 0)
			ok = false;
		close(fd);
		if (ok)
			durable_rename(tmp, path, WARNING);
		else
			unlink(tmp);
	}

	if (nopen > 0)
		elog(LOG, "pg_partdist: 未决 2PC 日志重放：%d 条待收敛（共 %d 条记录）",
			 nopen, nrec);
}

/* ---- 对外：shmem 编排 ---- */

void
RequestDtxPendingShmem(void)
{
	RequestAddinShmemSpace(MAXALIGN(sizeof(DtxPendingCtl)));
}

void
DtxPendingShmemInit(void)
{
	bool		found;

	PendingCtl = ShmemInitStruct("pg_partdist_dtx_pending",
								 sizeof(DtxPendingCtl), &found);
	if (!found)
	{
		memset(PendingCtl, 0, sizeof(DtxPendingCtl));
		SpinLockInit(&PendingCtl->mutex);
		dtx_jrnl_load_and_compact();
	}
}

/* ---- 对外：登记 / 注销 / 查询 ---- */

void
DtxPendingRegister(int64 gxid, int64 coord_gsid, int64 dtxid,
				   int64 start_ts, int nxids, const uint32 *pairs)
{
	DtxPendingEntry ent;
	int			rc;

	if (gxid == 0 || nxids <= 0 || PendingCtl == NULL)
		return;
	if (nxids > DTX_PENDING_MAX_PAIRS)
		ereport(ERROR,
				(errmsg("pg_partdist: 单事务分片数 %d 超出未决登记上限 %d",
						nxids, DTX_PENDING_MAX_PAIRS)));

	memset(&ent, 0, sizeof(ent));
	ent.gxid = gxid;
	ent.coord_gsid = coord_gsid;
	ent.dtxid = dtxid;
	ent.start_ts = start_ts;
	ent.nxids = nxids;
	memcpy(ent.pairs, pairs, (Size) nxids * 2 * sizeof(uint32));

	/*
	 * 持久化点：OPEN fsync 先于 shmem 登记（也先于 PREPARE 的 WAL 刷盘）。
	 * 单副本快速提交下不 fsync：prepared 一旦持久，崩溃恢复时 2PC 段的 recover
	 * 回调会 DtxPendingReRegister 补登；PREPARE 之前就崩 = 事务本身不存在。
	 */
	dtx_jrnl_append(DTX_JRNL_OPEN, &ent, !partwal_fast_commit);

	SpinLockAcquire(&PendingCtl->mutex);
	rc = pending_insert_locked(&ent);
	SpinLockRelease(&PendingCtl->mutex);
	pending_insert_warn(rc, gxid);
}

/* 崩溃恢复的 2PC 段重放路径：日志里通常已有，缺了才补（幂等） */
void
DtxPendingReRegister(int64 gxid, int64 coord_gsid, int64 dtxid,
					 int64 start_ts, int nxids, const uint32 *pairs)
{
	bool		have = true;
	DtxPendingEntry *slot;
	int			i,
				j;

	if (gxid == 0 || PendingCtl == NULL)
		return;
	/*
	 * P7-N82：同一 gxid 的多个 2PC 段（多条连接各 PREPARE 一次）各带一份分片清单 ——
	 * 登记在、但没覆盖本段全部 (shard, sxid) 时也要补（Register 按并集合并）。
	 */
	SpinLockAcquire(&PendingCtl->mutex);
	slot = pending_find_locked(gxid);
	if (slot == NULL)
		have = false;
	else
		for (i = 0; i < nxids && have; i++)
		{
			have = false;
			for (j = 0; j < slot->nxids; j++)
				if (slot->pairs[2 * j] == pairs[2 * i] &&
					slot->pairs[2 * j + 1] == pairs[2 * i + 1])
				{
					have = true;
					break;
				}
		}
	SpinLockRelease(&PendingCtl->mutex);
	if (!have)
		DtxPendingRegister(gxid, coord_gsid, dtxid, start_ts, nxids, pairs);
}

void
DtxPendingFinalized(int64 gxid)
{
	DtxPendingEntry ent;
	bool		have;

	if (gxid == 0 || PendingCtl == NULL)
		return;
	SpinLockAcquire(&PendingCtl->mutex);
	have = (pending_find_locked(gxid) != NULL);
	pending_remove_locked(gxid);
	SpinLockRelease(&PendingCtl->mutex);
	if (have)
	{
		memset(&ent, 0, sizeof(ent));
		ent.gxid = gxid;
		dtx_jrnl_append(DTX_JRNL_CLOSE, &ent, false);	/* 丢了只多一次幂等重扫 */
	}
}

bool
DtxPendingLookup(int64 gxid, DtxPendingEntry *out)
{
	DtxPendingEntry *slot;
	bool		found = false;

	if (gxid == 0 || PendingCtl == NULL)
		return false;
	SpinLockAcquire(&PendingCtl->mutex);
	slot = pending_find_locked(gxid);
	if (slot != NULL)
	{
		*out = *slot;
		found = true;
	}
	SpinLockRelease(&PendingCtl->mutex);
	return found;
}

/* R-P4-5：pg_raft 回执门经 rendezvous 调用——该 dtxid 是否仍有未决登记 */
bool
DtxPendingContainsDtxid(int64 dtxid)
{
	bool		found = false;
	int			i;

	if (dtxid == 0 || PendingCtl == NULL)
		return false;
	SpinLockAcquire(&PendingCtl->mutex);
	for (i = 0; i < DTX_PENDING_MAX; i++)
		if (PendingCtl->gx[i] != 0 && PendingCtl->dx[i] == dtxid)
		{
			found = true;
			break;
		}
	SpinLockRelease(&PendingCtl->mutex);
	return found;
}

int
DtxPendingCount(void)
{
	int			n;

	if (PendingCtl == NULL)
		return 0;
	SpinLockAcquire(&PendingCtl->mutex);
	n = PendingCtl->nused;
	SpinLockRelease(&PendingCtl->mutex);
	return n;
}

/* ---- 决议问询核心（SPI 解析协调组 leader 地址 + libpq 只读 peek） ---- */

/*
 * 返回 0=无从判定 1=COMMIT 2=ABORT。任何失败都按 0 返回（调用方保持未决），
 * 绝不 ERROR —— 这条路径服务读者与清扫，抛错会把只读语句炸掉。
 */
static int
dtx_inquire_core_ex(int64 coord_gsid, int64 dtxid, int64 *cts_out, bool *answered);

static int
dtx_inquire_core(int64 coord_gsid, int64 dtxid, int64 *cts_out)
{
	bool		answered;

	return dtx_inquire_core_ex(coord_gsid, dtxid, cts_out, &answered);
}

/*
 * ★ P7-N52（之三，2026-10-04）：answered = 有没有哪个候选给出了**权威**答复。
 * dtx_peek 在协调组 leader 上（排空 apply 之后）查无决议时现在返回一行 verdict=0
 * —— 那是"确实还没决议"；所有候选都只给 0 行（连不上 / 非 leader 且转问也问不到）
 * 则是"不知道"。返回值仍是 0/1/2（清扫等旧调用方语义不变）。
 */
static int
dtx_inquire_core_ex(int64 coord_gsid, int64 dtxid, int64 *cts_out, bool *answered)
{
	char		sql[1024];
#define DTX_INQ_MAX_CAND 8
	char		hosts[DTX_INQ_MAX_CAND][NAMEDATALEN];
	int			ports[DTX_INQ_MAX_CAND];
	int			ncand = 0;
	int			ci;
	int			verdict = 0;
	bool		spi_ok = false;

	*cts_out = 0;
	*answered = false;

	if (dtxid == 0)
		return 0;

	/*
	 * 1) 协调组与其 leader 地址。**权威 = 本地 partdist.dtx_participant.
	 * coord_gsid**（§4.2 分支②：master 严格先下发再决议，NULL/无行 ⇒ 决议
	 * 必然未做 ⇒ 按无从判定返回）。登记表里 join 携带的 gsid 只作兜底
	 * （dtx_participant 行被 GC 的窗口）。
	 */
	if (SPI_connect() != SPI_OK_CONNECT)
		return 0;

	/*
	 * ★ P7-N68（2026-10-05）：本机有这笔事务的参与登记、协调组号却为空 ⇒ **权威的
	 * "还没决议"**。master 严格"先把协调组号下发到全部参与者、全部成功才取
	 * commit_ts 并决议"（任一下发失败 dtx_remote_scalar 即 ERROR、本事务中止）；决议的 commit_ts 必然晚于此刻
	 * 读者的快照，按不可见处理正确。原来这里因为一个候选都没有而算作"没问到"，读者
	 * 报可重试错误 —— master 在决议前就失败的孤儿 PREPARED（断电时很常见）要等 Citus
	 * 恢复回滚才消失，期间碰到同一行的读写全部失败（实测回归里转账连续 30 次失败）。
	 */
	if (coord_gsid <= 0)
	{
		snprintf(sql, sizeof(sql),
				 "SELECT bool_and(coord_gsid IS NULL) FROM partdist.dtx_participant "
				 " WHERE dtxid = %lld", (long long) dtxid);
		if (SPI_execute(sql, true, 0) == SPI_OK_SELECT && SPI_processed == 1)
		{
			bool		isnull;
			Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 1, &isnull);

			if (!isnull && DatumGetBool(d))
			{
				SPI_finish();
				*answered = true;
				return 0;
			}
		}
	}

	/*
	 * ★ 2026-08-17（R-P4-14）：候选是**协调组全部成员**（primary +
	 * secondary_nodes），不再只认 partition_map.primary_node。
	 *
	 * 原因：本函数按 partition_map 的"登记主"寻址，而 dtx_peek 内部按 raft
	 * 的 state='leader' 门控 —— 两个"主"的概念不同步。协调组切主后，raft 侧
	 * 立刻有了新 leader，partition_map 却要走"自选举→上报→group0→回落"才更新；
	 * 窗口期内问询打到旧主：要么连不上（旧主正是被杀那个），要么连上了但它已
	 * 不是 leader ⇒ dtx_peek 返回 0 行 ⇒ 判决永远学不到。实测表现为 Q3
	 * "决议明明写了、60s + 主动 drain 都救不回来"。
	 *
	 * primary 仍排在最前（pri=0），命中率最高；失败才依次退到 secondary。
	 * 全部返 0 时语义不变（保持未决），不引入推定中止。
	 */
	snprintf(sql, sizeof(sql),
			 "SELECT g.gsid, n.hostname, n.port FROM ("
			 "  SELECT x.gsid FROM ("
			 "    SELECT 1 AS pri, dp.coord_gsid AS gsid "
			 "      FROM partdist.dtx_participant dp "
			 "     WHERE dp.dtxid = %lld AND dp.coord_gsid IS NOT NULL "
			 "    UNION ALL SELECT 2, %lld WHERE %lld > 0"
			 "  ) x ORDER BY x.pri LIMIT 1) g "
			 "  JOIN partdist.partition_map p ON p.partition_id = g.gsid "
			 "  JOIN LATERAL (SELECT 0 AS pri, p.primary_node AS nid "
			 "                UNION ALL "
			 "                SELECT 1, s.nid FROM unnest(p.secondary_nodes) AS s(nid)"
			 "               ) c ON true "
			 "  JOIN partdist.node_map n ON n.node_id = c.nid "
			 " ORDER BY c.pri, n.node_id LIMIT %d",
			 (long long) dtxid, (long long) coord_gsid, (long long) coord_gsid,
			 DTX_INQ_MAX_CAND);
	if (SPI_execute(sql, true, DTX_INQ_MAX_CAND) == SPI_OK_SELECT)
	{
		uint64		r;

		for (r = 0; r < SPI_processed && ncand < DTX_INQ_MAX_CAND; r++)
		{
			bool		isnull;
			Datum		gd = SPI_getbinval(SPI_tuptable->vals[r],
										   SPI_tuptable->tupdesc, 1, &isnull);
			char	   *h = SPI_getvalue(SPI_tuptable->vals[r],
										 SPI_tuptable->tupdesc, 2);
			Datum		pd;
			bool		pnull;

			if (isnull || h == NULL)
				continue;
			pd = SPI_getbinval(SPI_tuptable->vals[r],
							   SPI_tuptable->tupdesc, 3, &pnull);
			if (pnull || DatumGetInt32(pd) <= 0)
				continue;
			coord_gsid = DatumGetInt64(gd);
			strlcpy(hosts[ncand], h, NAMEDATALEN);
			ports[ncand] = DatumGetInt32(pd);
			ncand++;
			spi_ok = true;
		}
	}
	SPI_finish();
	if (!spi_ok || ncand <= 0 || coord_gsid <= 0)
		return 0;

	/*
	 * 2) 只读 peek，逐个候选试到学到判决为止（0 行 = 该节点无从应答：非
	 * leader、或组内也没这条决议）。连不上就换下一个 —— 被杀的旧主正是
	 * 最常见的第一候选。
	 */
	for (ci = 0; ci < ncand && verdict != 1 && verdict != 2; ci++)
	{
		char		conninfo[256];
		PGconn	   *conn;
		PGresult   *res;

		snprintf(conninfo, sizeof(conninfo),
				 "host=%s port=%d dbname=postgres user=postgres "
				 "connect_timeout=2 options='-c statement_timeout=2000'",
				 hosts[ci], ports[ci]);
		conn = PQconnectdb(conninfo);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			PQfinish(conn);
			continue;
		}
		snprintf(sql, sizeof(sql),
				 "SELECT verdict, commit_ts FROM partdist.dtx_peek(%lld, %lld)",
				 (long long) coord_gsid, (long long) dtxid);
		res = PQexec(conn, sql);
		if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0)
		{
			verdict = atoi(PQgetvalue(res, 0, 0));
			*cts_out = strtoll(PQgetvalue(res, 0, 1), NULL, 10);
			*answered = true;
		}
		PQclear(res);
		PQfinish(conn);
	}
	if (verdict != 1 && verdict != 2)
		return 0;
	return verdict;
}

/* 学到判决：整笔（本节点全部 pairs）幂等落分片 clog + 注销 */
/*
 * dtx_replicate_verdict —— T7.1（R-P6-15）：把刚落进本地分片 clog 的判决
 * 补一条 MARKER 进分区流，让副本得到同一本账。
 *
 * ★ 为什么补在这里，而不是在 COMMIT PREPARED 那一步。
 *
 * 缺陷现场（R-P6-15，2026-09-06 多分片演示实测）：阶段 3 的终局标记由
 * `dtx_participant.c` 在 `COMMIT PREPARED` 的钩子里发，载荷走
 * `PartWALBuildMarkerPayload()` 组装 —— 而那个函数按 `ShardXidXactCount() > 0`
 * 决定带不带分片 xid 尾，`COMMIT PREPARED` 跑在**另一个没碰过分片表的事务**里，
 * 计数恒 0 ⇒ 24 字节旧格式 ⇒ 回放侧 `sxid` 非法、`ShardClogSetVerdict` 被跳过
 * （`shard_replay.c` 的 `TransactionIdIsNormal(sxid)` 分支）⇒ **副本的分片 clog
 * 对每笔跨分片事务永远停在 PREPARED**；等协调组的 DECISION 被 FORGET 回收、
 * `pg_dist_transaction` 也 GC 掉之后，`dtx_close_indoubt` 四级全落空 ⇒
 * 切主后那些**已经提交成功的行在新主上永久不可见**。
 *
 * 那为什么不是在 COMMIT PREPARED 处把分片 xid 补上就好？因为那里还缺**第二个
 * 权威值**：判决的 `commit_ts`。COMMIT PREPARED 的钩子跑在决议之后，但本节点
 * 未必已经收到决议广播；就地 `TsoMarkerCommitTs()` 取到的是一个**新号**，
 * 比决议的 commit_ts 晚。用它落副本的分片 clog，会让同一行在 leader 与
 * follower 上对同一快照可见性不同 —— 把"永久不可见"换成"偶发不一致"，
 * 不是修复。
 *
 * 判决落账这一刻则两个值都是权威的：分片 xid 来自未决登记的 pairs（PREPARE 时
 * 与 2PC 状态段一起 fsync，"prepared 存在 ⇒ 登记必在"），commit_ts 就是本函数
 * 正在往本地分片 clog 里写的那一个。**leader 写什么，副本就收到什么**，
 * 由构造保证一致。三条调用路径（广播接收 `dtx_apply_decision`、清扫工作者、
 * 恢复守护）都是普通 backend，可以安全地开 writer、取 PartWALCtl 锁、发提案。
 *
 * ★ 失败一律不上抛。本地分片 clog 已经落账（调用方在本函数之前做的），那是
 * 正确性的底线；复制是尽力而为，与 §3.3 阶段 3 "随下一次 flush 复制" 同一口径。
 * 最典型的失败是**本节点已不是该分片的主**（切主后由恢复守护补判决，R-P4-9 的
 * 场景）——写栅栏会拒，这正是它该拒的：陈旧主不得再往流里写。
 */
static int replicate_fail_streak = 0;	/* 本 backend 连续复制失败次数（成功即清零） */

static bool
dtx_replicate_verdict(const DtxPendingEntry *ent, int verdict, int64 cts)
{
	TransactionId	xid;
	char		   *payload = NULL;
	uint32			payload_len = 0;
	uint8			op;
	int				i;
	bool			appended = false;
	bool			ok = true;
	int				touched_before;

	if (ent->nxids <= 0 || (verdict != 1 && verdict != 2))
		return true;

	/* 被标记的是那笔 prepared 事务的本地 xid，不是当前语句的 */
	xid = GxidLocalXid(ent->gxid);
	if (!TransactionIdIsValid(xid))
		return true;

	op = (verdict == 1) ? XLOG_XACT_COMMIT : XLOG_XACT_ABORT;
	touched_before = PartWALTouchedCount();

	PG_TRY();
	{
		payload = PartWALBuildVerdictMarker(ent->start_ts,
											verdict == 1 ? cts : 0,
											&payload_len);

		for (i = 0; i < ent->nxids; i++)
		{
			Oid				shard = (Oid) ent->pairs[2 * i];
			TransactionId	sxid = (TransactionId) ent->pairs[2 * i + 1];

			if (!OidIsValid(shard) || !TransactionIdIsNormal(sxid))
				continue;

			PartWALAppendMarkerForShardXid(shard, xid, op,
										   payload, payload_len, sxid);

			/*
			 * ★ 必须登记"触达"，否则下面的 flush 不会提议这条记录。
			 * `PartWALAppendMarkerForShardXid` 只把字节写进**本地段**并 fsync；
			 * 送进 Raft 是 `PartWALFlush` 干的，而它只遍历本事务触达过的分区
			 * （`partwal_my_touched`）。漏了这一步的现象很具体：leader 的段流
			 * 里有这条 32 字节判决标记，**副本的段流里没有**（实测 leader 7 条 /
			 * follower 6 条），副本的分片 clog 因此停在 PREPARED ——
			 * 与没修之前一模一样，但原因完全不同。
			 * `dtx_participant.c` 的阶段 3 也是这么配对写的。
			 */
			PartWALNoteTouchedPartition(shard);
			appended = true;
		}

		/*
		 * 尽力复制一轮。不 flush 的话标记只躺在本地段里，副本要等下一笔
		 * 写入才顺带收到 —— 而"下一笔写入"可能永远不来（尤其是切主前的
		 * 最后一笔事务，恰恰是最需要它的那笔）。
		 */
		if (appended)
			PartWALFlush(InvalidXLogRecPtr, false);
		replicate_fail_streak = 0;
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		MemoryContextSwitchTo(TopMemoryContext);
		ed = CopyErrorData();
		FlushErrorState();

		/*
		 * ★ P7-N51（2026-10-03）：撤回本次登进 touched 表的分区。不撤的话，调用方（常常是
		 * 一条只读查询的可见性挂钩在闭合 in-doubt）自己的提交点 flush 会再复制一遍它、
		 * 照样被写栅栏拒 ⇒ **只读 SELECT 报错**（演示二断电后实测：w1 丢了组 104517 的主，
		 * 之后对 wallet 的每一次查询都 ERROR）。标记字节留在本地流里当孤儿，清扫时重试。
		 */
		PartWALTouchedTruncate(touched_before);

		/*
		 * ★ P7-N12（2026-09-17）：这里原先只告警、调用方照样注销未决登记。
		 * 后果不是"副本晚点拿到判决"，而是**永远拿不到**：注销 ⇒ 本节点回执
		 * ⇒ 协调组收齐回执 FORGET 决议；本节点丢主后接任的新主手里只有
		 * PREPARED，升主序列的 dtx_close_indoubt 四级全查不到 ⇒ 已提交的行
		 * 在新主上永久不可见（跨组转账验收：切主后账户总额 24000→23998→23999，
		 * 旧主日志正是 `判决标记未能复制给副本 … record 381 未达多数派`）。
		 * 现在返回 false，由调用方**保留登记**——回执门（R-P4-5）随之关着，
		 * 决议不会被遗忘，新主升主时问得到。
		 */
		/* 限流：登记保留后清扫每轮都会重试，失败时只有第 1、每第 50 次打 WARNING，其余 LOG */
		replicate_fail_streak++;
		ereport((replicate_fail_streak == 1 || replicate_fail_streak % 50 == 0) ? WARNING : LOG,
				(errmsg("pg_partdist: 判决标记未能复制给副本（gxid=%lld，连续第 %d 次）：%s",
						(long long) ent->gxid, replicate_fail_streak, ed->message),
				 errdetail("本地分片 clog 判决已落账；未决登记**保留**（挡住回执与决议 FORGET），"
						   "清扫时重试复制；本节点若已不是该分片的主，待主权登记转到新主后自动注销。"),
				 errhint("本节点若已不是该分片的主，这是写栅栏的正常拒绝。")));
		FreeErrorData(ed);
		ok = false;
	}
	PG_END_TRY();

	if (payload != NULL)
		pfree(payload);
	return ok;
}

/*
 * P7-N12：本节点对这笔登记的责任是否已经转走 —— 登记里每个分片的**当前主**
 * （partition_map.primary_node）都不是本节点。
 *
 * 为什么这个判据够用：新主的升主序列在**登记 partition_map 之前**跑
 * dtx_close_indoubt（pg_raft_promote_prepare_ex），而只要本登记还在，
 * 回执就被挡住、决议就还在 ⇒ 新主那一步一定问得到判决并把闭合记录写进它的流
 * （它是 leader，写得进）。partition_map 已经指向别人 = 那一步已经跑过。
 * 任一分片查不到映射或仍指向本节点 ⇒ 不放行（宁可多留一轮）。
 * 只在清扫（普通 backend、可开 SPI）里调用。
 */
static bool
dtx_pending_responsibility_moved(const DtxPendingEntry *ent)
{
	const char *v = GetConfigOption("pg_raft.node_id", true, false);
	int			me = (v != NULL) ? atoi(v) : 0;
	bool		moved = true;
	int			i;

	if (me <= 0 || ent->nxids <= 0)
		return false;
	if (SPI_connect() != SPI_OK_CONNECT)
		return false;
	PG_TRY();
	{
		for (i = 0; i < ent->nxids && moved; i++)
		{
			char		q[256];
			bool		isnull = true;
			int32		pn = 0;

			snprintf(q, sizeof(q),
					 "SELECT pm.primary_node FROM partdist.shard_identity si "
					 "  JOIN partdist.partition_map pm ON pm.partition_id = si.global_shard_id::oid "
					 " WHERE si.local_oid = %u LIMIT 1",
					 (unsigned) ent->pairs[2 * i]);
			if (SPI_execute(q, true, 1) == SPI_OK_SELECT && SPI_processed == 1)
				pn = DatumGetInt32(SPI_getbinval(SPI_tuptable->vals[0],
												 SPI_tuptable->tupdesc, 1, &isnull));
			if (isnull || pn <= 0 || pn == me)
				moved = false;
		}
	}
	PG_CATCH();
	{
		FlushErrorState();
		moved = false;
	}
	PG_END_TRY();
	SPI_finish();
	return moved;
}

/*
 * 学到判决：整笔（本节点全部 pairs）幂等落分片 clog；复制给副本成功才注销登记。
 * 返回是否已注销（P7-N12：复制失败 ⇒ 登记保留，调用方按各自语境处置）。
 */
static void
dtx_recent_remember(int64 gxid, int verdict, int64 cts)
{
	int			i;

	if (gxid == 0 || PendingCtl == NULL || (verdict != 1 && verdict != 2))
		return;
	SpinLockAcquire(&PendingCtl->mutex);
	for (i = 0; i < DTX_RECENT_MAX; i++)
		if (PendingCtl->recent[i].gxid == gxid)
			break;
	if (i == DTX_RECENT_MAX)
	{
		i = PendingCtl->recent_next;
		PendingCtl->recent_next = (i + 1) % DTX_RECENT_MAX;
	}
	PendingCtl->recent[i].gxid = gxid;
	PendingCtl->recent[i].verdict = verdict;
	PendingCtl->recent[i].cts = (verdict == 1) ? cts : 0;
	SpinLockRelease(&PendingCtl->mutex);
}

int
DtxRecentVerdict(int64 gxid, int64 *cts_out)
{
	int			verdict = 0;
	int			i;

	*cts_out = 0;
	if (gxid == 0 || PendingCtl == NULL)
		return 0;
	SpinLockAcquire(&PendingCtl->mutex);
	for (i = 0; i < DTX_RECENT_MAX; i++)
		if (PendingCtl->recent[i].gxid == gxid)
		{
			verdict = PendingCtl->recent[i].verdict;
			*cts_out = PendingCtl->recent[i].cts;
			break;
		}
	SpinLockRelease(&PendingCtl->mutex);
	return verdict;
}

/* 记下某登记的判决 WAL 位置（清扫据此只等持久、不再问询） */
static void
pending_note_vlsn(int64 gxid, XLogRecPtr lsn)
{
	DtxPendingEntry *slot;

	if (PendingCtl == NULL || XLogRecPtrIsInvalid(lsn))
		return;
	SpinLockAcquire(&PendingCtl->mutex);
	slot = pending_find_locked(gxid);
	if (slot != NULL)
		PendingCtl->vlsn[slot - PendingCtl->e] = lsn;
	SpinLockRelease(&PendingCtl->mutex);
}

/*
 * wait = false：前台路径（决议广播的接收端、读者问到判决）。判决照样先记 WAL、落槽位
 * （本机立即可见 —— 切主后新主凭 PREPARED 槽 + 登记问到的是同一决议，可见性一致），
 * 但不在这里等同步备库：广播在协调者的提交路径上，读者还持着缓冲区锁。没确认就不注销
 * 登记（不回执、协调组不 FORGET），由后台清扫等持久后注销。
 */
static bool
dtx_pending_apply_verdict_ex(const DtxPendingEntry *ent, int verdict, int64 cts, bool wait)
{
	int			i;
	int			nbad = 0;
	XLogRecPtr	vlsn;
	bool		durable;

	/*
	 * 2026-10-17（路线 A）：先记判决 WAL，再落槽位（本机可见），再等它到达同步备库；
	 * 没确认到就不注销未决登记 —— 登记在，本节点就不回执，协调组就不会 FORGET 决议，
	 * 切主后新主还能凭 PREPARED 槽 + 登记问到它（见 shard_wal.h）。
	 */
	vlsn = ShardWalLogVerdict(ent->gxid, verdict == 1, verdict == 1 ? cts : 0,
							  ent->nxids, ent->pairs);
	for (i = 0; i < ent->nxids; i++)
		ShardClogSetVerdict((Oid) ent->pairs[2 * i],
							(TransactionId) ent->pairs[2 * i + 1],
							verdict == 1, verdict == 1 ? cts : 0);
	dtx_recent_remember(ent->gxid, verdict, cts);	/* P7-N86 */
	durable = wait ? ShardWalWaitDurable(vlsn) : ShardWalIsDurable(vlsn);
	if (!durable)
		pending_note_vlsn(ent->gxid, vlsn);

	/* P7-N86 诊断：落完立刻回读，槽位没变成终局就说出来 */
	for (i = 0; i < ent->nxids; i++)
		if (ShardClogReadStatus((Oid) ent->pairs[2 * i],
								(TransactionId) ent->pairs[2 * i + 1])
			!= (verdict == 1 ? TXN_COMMITTED : TXN_ABORTED))
			nbad++;
	if (nbad > 0)
		elog(WARNING, "pg_partdist: gxid=%lld 判决落分片 clog 后回读，%d/%d 个槽位不是终局（P7-N86）",
			 (long long) ent->gxid, nbad, ent->nxids);

	/* ★ T7.1（R-P6-15）：本节点刚落的这本账，要复制给副本 */
	if (!dtx_replicate_verdict(ent, verdict, cts))
		return false;

	if (!durable)
	{
		if (wait)
			elog(LOG, "pg_partdist: gxid=%lld 的判决 WAL 未确认到达同步备库，暂不注销未决登记（清扫重试）",
				 (long long) ent->gxid);
		return false;
	}
	DtxPendingFinalized(ent->gxid);
	return true;
}

static bool
dtx_pending_apply_verdict(const DtxPendingEntry *ent, int verdict, int64 cts)
{
	return dtx_pending_apply_verdict_ex(ent, verdict, cts, false);
}

/* ---- 清扫（backend 语境） ---- */

int
DtxPendingSweep(void)
{
	/* 堆上：每项约 2 KB，整表拷贝放栈上会爆（容量已从 128 提到 1024） */
	DtxPendingEntry *snap = (DtxPendingEntry *) palloc(sizeof(DtxPendingEntry) * DTX_PENDING_MAX);
	XLogRecPtr *slsn = (XLogRecPtr *) palloc(sizeof(XLogRecPtr) * DTX_PENDING_MAX);
	int			nsnap = 0;
	int			ndone = 0;
	int			i;

	if (PendingCtl == NULL)
		return 0;

	SpinLockAcquire(&PendingCtl->mutex);
	for (i = 0; i < DTX_PENDING_MAX; i++)
		if (PendingCtl->gx[i] != 0)
		{
			slsn[nsnap] = PendingCtl->vlsn[i];
			snap[nsnap++] = PendingCtl->e[i];
		}
	SpinLockRelease(&PendingCtl->mutex);

	for (i = 0; i < nsnap; i++)
	{
		int64		cts = 0;
		int			verdict;

		/* 判决已由前台落过槽位、只差 WAL 持久：等它（后台，可以等），不再问协调组 */
		if (!XLogRecPtrIsInvalid(slsn[i]))
		{
			CHECK_FOR_INTERRUPTS();
			if (ShardWalWaitDurable(slsn[i]))
			{
				DtxPendingFinalized(snap[i].gxid);
				ndone++;
			}
			continue;
		}

		verdict = dtx_inquire_core(snap[i].coord_gsid, snap[i].dtxid, &cts);
		CHECK_FOR_INTERRUPTS();
		if (verdict != 0)
		{
			if (dtx_pending_apply_verdict_ex(&snap[i], verdict, cts, true))
				ndone++;
			else if (dtx_pending_responsibility_moved(&snap[i]))
			{
				/* P7-N12：本节点已不是这些分片的主，新主升主时已闭合 in-doubt */
				elog(LOG, "pg_partdist: 未决登记 gxid=%lld 的判决未能由本节点复制，"
					 "但分片主权已登记到别的节点，视为已交接，注销",
					 (long long) snap[i].gxid);
				DtxPendingFinalized(snap[i].gxid);
				ndone++;
			}
			continue;
		}

		/*
		 * 自愈一：pairs 里已无任何 PREPARED 槽 ⇒ 终局已由别的通道写过
		 * （postabort、或人为清理），登记是残渣——注销。活的 prepared
		 * 事务必然带着 PREPARED 槽（at-prepare 同刻写入），不会误伤。
		 *
		 * 自愈二（2026-08-14 实测边界）：槽还是 PREPARED，但 pairs 的分片
		 * 表已整体不存在（表被 DROP 而 clog 目录残留——白名单撤除后 DROP
		 * 不走 RememberDrop GC 的路径）⇒ 数据都没了，终局无意义，同样是
		 * 残渣。判据取"全部 pairs 的 shard oid 均查无 relation"，任一存在
		 * 即不动（绝不误杀活账）。
		 */
		{
			bool		any_prepared = false;
			int			j;

			for (j = 0; j < snap[i].nxids; j++)
			{
				TxnStatus	st = ShardClogReadStatus((Oid) snap[i].pairs[2 * j],
													 (TransactionId) snap[i].pairs[2 * j + 1]);

				/*
				 * ★ TPC-C 分支：RUNNING 也算"活账"。参与者在 PRE_PREPARE 里先自治登记、
				 * 到 at-prepare 才把槽位写成 PREPARED；清扫插在两者之间时槽还是 RUNNING，
				 * 原生 prepared 也还没出现，下面两条兜底都拦不住 —— 活登记被当残渣注销，
				 * 决议从此落不到槽位上，该分片的读永久报"判决暂时无法确定"
				 * （心跳清扫异步化后每 lease/3 一轮，32 终端 1 分钟实测两笔）。
				 * 残渣的本意是"全部槽都已终局"；崩溃遗留的 RUNNING 由 T2.4 认领改判 ABORTED。
				 */
				if (st == TXN_PREPARED || st == TXN_RUNNING)
				{
					any_prepared = true;
					break;
				}
			}
			if (any_prepared)
			{
				bool		any_rel = false;

				/*
				 * ★ TPC-C 分支：查系统缓存，不能用 SQL 查 pg_class —— Citus 的
				 * override_table_visibility（默认 on）在普通会话里把分片表从 pg_class
				 * 里藏掉，清扫的自连会话于是"一张分片表都查不到"，把活登记当成
				 * "表全没了"的残渣注销，决议从此落不到槽位上（槽位永久 PREPARED，
				 * 读报"判决暂时无法确定"）。
				 */
				for (j = 0; j < snap[i].nxids; j++)
				{
					if (SearchSysCacheExists1(RELOID,
											  ObjectIdGetDatum((Oid) snap[i].pairs[2 * j])))
					{
						any_rel = true;
						break;
					}
				}
				if (!any_rel)
					any_prepared = false;	/* 表全没了：按残渣注销 */
			}
			/*
			 * ★ 防误杀（2026-08-14 三点追踪抓获）：崩溃重启后 PREPARED 槽的
			 * 重建（2PC 段 recover）与登记重建（journal 重放）不在同一时刻，
			 * 清扫若插在"登记已回来、槽还没回来"的空窗里，上面两条自愈判据
			 * 都会把**活账**当残渣注销——随后守护补取到 commit_ts 想落账时
			 * 登记已不在，判决永久落不上（实测：16:02:14 清扫注销 →
			 * 16:02:46 守护补到 dcts=23 却落账返回 0 → 行永久不可见）。
			 *
			 * 兜底判据：原生 prepared 事务还在 ⇒ 这笔账**一定是活的**，
			 * 无论槽/表此刻看起来如何，一律不注销。gid 前缀按 §5.4 两种形态
			 * 匹配（citus_… 与 shardpg_dtx_…），dtxid 为 0 时退化为不兜底。
			 */
			if (!any_prepared && snap[i].dtxid != 0 &&
				SPI_connect() == SPI_OK_CONNECT)
			{
				char		q[192];

				snprintf(q, sizeof(q),
						 "SELECT 1 FROM pg_prepared_xacts WHERE gid LIKE 'citus\\_%%'"
						 "    OR gid LIKE 'shardpg\\_dtx\\_%%' LIMIT 1");
				if (SPI_execute(q, true, 1) == SPI_OK_SELECT && SPI_processed > 0)
					any_prepared = true;	/* 还有活的 prepared：不敢注销 */
				SPI_finish();
			}

			if (!any_prepared)
			{
				{
					StringInfoData dbg;
					int			k;

					initStringInfo(&dbg);
					for (k = 0; k < snap[i].nxids; k++)
						appendStringInfo(&dbg, " (%u,%u)=%d",
										 (unsigned) snap[i].pairs[2 * k],
										 (unsigned) snap[i].pairs[2 * k + 1],
										 (int) ShardClogReadStatus((Oid) snap[i].pairs[2 * k],
																   (TransactionId) snap[i].pairs[2 * k + 1]));
					elog(LOG, "pg_partdist: 未决登记 gxid=%lld dtxid=%lld 判决未知、槽位已无 PREPARED，按残渣注销（P7-N86 诊断）nxids=%d%s",
						 (long long) snap[i].gxid, (long long) snap[i].dtxid,
						 snap[i].nxids, dbg.data);
					pfree(dbg.data);
				}
				DtxPendingFinalized(snap[i].gxid);
				ndone++;
			}
		}
	}
	return ndone;
}

/* ---- 读者 ③（可见性挂钩内；memo 保同快照一致 + 限流） ---- */

typedef struct DtxReaderMemo
{
	int64		gxid;
	int			verdict;		/* 1/2 = 已学到（不可变）；0 = 仅失败标记 */
	int64		cts;
	uint64		fail_epoch;		/* 本事务纪元内问过且未果 → 不再重试 */
	bool		noanswer;		/* 未果的原因是"没问到答案"（连接/超时/报错），不是"协调组答：未决" */
} DtxReaderMemo;

static HTAB *reader_memo = NULL;
static uint64 xact_epoch = 1;
static bool epoch_cb_registered = false;

static void
dtx_pending_xact_cb(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_COMMIT || event == XACT_EVENT_ABORT)
		xact_epoch++;
}

static DtxReaderMemo *
reader_memo_get(int64 gxid, bool create)
{
	if (reader_memo == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(int64);
		ctl.entrysize = sizeof(DtxReaderMemo);
		ctl.hcxt = TopMemoryContext;
		reader_memo = hash_create("pg_partdist dtx reader memo", 64, &ctl,
								  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	if (!epoch_cb_registered)
	{
		RegisterXactCallback(dtx_pending_xact_cb, NULL);
		epoch_cb_registered = true;
	}
	return (DtxReaderMemo *) hash_search(reader_memo, &gxid,
										 create ? HASH_ENTER : HASH_FIND, NULL);
}

/*
 * 读者自连（本节点、短超时）。可见性挂钩里不能起 SPI，只能另开一个干净 backend。
 */
static PGconn *
dtx_reader_selfconn(void)
{
	static PGconn *selfconn = NULL;

	if (selfconn == NULL || PQstatus(selfconn) != CONNECTION_OK)
	{
		char		conninfo[256];

		if (selfconn != NULL)
		{
			PQfinish(selfconn);
			selfconn = NULL;
		}
		snprintf(conninfo, sizeof(conninfo),
				 "host=/tmp port=%d dbname=postgres user=postgres "
				 "connect_timeout=2 options='-c statement_timeout=5000'",
				 PostPortNumber);
		selfconn = PQconnectdb(conninfo);
		if (PQstatus(selfconn) != CONNECTION_OK)
		{
			PQfinish(selfconn);
			selfconn = NULL;
		}
	}
	return selfconn;
}

int
DtxReaderResolve(int64 gxid, int64 *cts_out)
{
	return DtxReaderResolveEx(gxid, cts_out, false);
}

int
DtxReaderResolveEx(int64 gxid, int64 *cts_out, bool retry_undecided)
{
	DtxReaderMemo *memo;
	DtxPendingEntry ent;
	char		sql[192];
	int			verdict = 0;
	int64		cts = 0;
	bool		noanswer = false;

	*cts_out = 0;
	if (gxid == 0)
		return 0;

	memo = reader_memo_get(gxid, false);
	if (memo != NULL && memo->verdict != 0)
	{
		*cts_out = memo->cts;
		return memo->verdict;
	}
	if (memo != NULL && memo->fail_epoch == xact_epoch &&
		!(retry_undecided && !memo->noanswer))
		return memo->noanswer ? 4 : 0;	/* 本事务已问过未果——不重试（限流） */

	/*
	 * ②：登记缺失。在执行 PREPARE 的节点上这意味着"决议必然未做"；但切主后的
	 * **新主**上从来就没有这份登记（P7-N52），同样缺失却可能早已提交。
	 * 返回 3 交调用方区分：快照读据此走就地 in-doubt 闭合，不再一律当未提交。
	 */
	if (!DtxPendingLookup(gxid, &ent))
		return 3;

	/*
	 * 经自连问询（partdist.dtx_inquire 在干净 backend 里做 SPI 寻址 +
	 * 远程 peek）。读路径可能持着 buffer 锁，连接与语句都限短超时；
	 * 失败按未果处置并 memo 本事务不再重试。
	 */
	{
		PGconn	   *conn = dtx_reader_selfconn();
		PGresult   *res;

		noanswer = true;
		if (conn != NULL)
		{
			snprintf(sql, sizeof(sql),
					 "SELECT verdict, commit_ts FROM partdist.dtx_inquire(%lld, %lld)",
					 (long long) ent.coord_gsid, (long long) ent.dtxid);
			res = PQexec(conn, sql);
			if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0)
			{
				noanswer = PQgetisnull(res, 0, 0);
				verdict = noanswer ? 0 : atoi(PQgetvalue(res, 0, 0));
				cts = strtoll(PQgetvalue(res, 0, 1), NULL, 10);
			}
			PQclear(res);
		}
	}

	memo = reader_memo_get(gxid, true);
	if (verdict == 1 || verdict == 2)
	{
		/* 学到即回写（幂等）；复制成功才注销，否则留给清扫；memo 使同快照后续元组零 RPC */
		(void) dtx_pending_apply_verdict(&ent, verdict, cts);
		memo->verdict = verdict;
		memo->cts = (verdict == 1) ? cts : 0;
		*cts_out = memo->cts;
		return verdict;
	}
	memo->verdict = 0;
	memo->fail_epoch = xact_epoch;
	memo->noanswer = noanswer;
	/*
	 * ★ P7-N52/N60：4 = 没问到答案（自连失败 / 语句超时 / 报错）。与 0（协调组答"未决"）
	 * 分开：客户端已确认提交的事务，在协调组那里一定已有判决 —— 问不到只是这一次没问到，
	 * 快照读据此报可重试错误，不能当未提交（实测：灌数提交返回后紧接着的读，问询
	 * dtx_inquire 超时，256 行读成 255 行）。
	 */
	return noanswer ? 4 : 0;
}

/*
 * ★ P7-N52：DtxReaderCloseOrphan — 快照读撞上"判决定不下来的 PREPARED"时的就地补救。
 *
 * 未决登记只在执行 PREPARE 的那台节点上；切主后新主上的 PREPARED 槽位没有登记，
 * DtxReaderResolve 无从问起。这里经自连对该分片跑一次 partdist.dtx_close_indoubt
 * （升主序列用的同一段代码：从流里映射 dtxid → 分片 xid、全网求决议、落分片 clog），
 * 调用方随后重读槽位。每个分片每个事务纪元最多跑一次（限流：同一次读不反复闭合）。
 * 返回 true = 闭合调用本身成功（不代表一定定下来了）。
 */
typedef struct DtxCloseMemo
{
	Oid			shard;
	uint64		epoch;
	bool		complete;		/* 本纪元那次闭合时全部数据节点可达（P7-N52 之四） */
} DtxCloseMemo;

static HTAB *close_memo = NULL;

bool
DtxReaderCloseOrphan(Oid shard, bool *search_complete)
{
	DtxCloseMemo *m;
	bool		found;
	PGconn	   *conn;
	PGresult   *res;
	char		sql[128];
	bool		ok = false;

	if (close_memo == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(DtxCloseMemo);
		ctl.hcxt = TopMemoryContext;
		close_memo = hash_create("pg_partdist dtx close memo", 16, &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	(void) reader_memo_get(0, false);	/* 确保纪元回调已挂 */
	*search_complete = false;
	m = (DtxCloseMemo *) hash_search(close_memo, &shard, HASH_ENTER, &found);
	if (found && m->epoch == xact_epoch)
	{
		*search_complete = m->complete;
		return false;
	}
	m->epoch = xact_epoch;
	m->complete = false;

	conn = dtx_reader_selfconn();
	if (conn == NULL)
		return false;
	/*
	 * ★ P7-N52（之四）：可达性要在闭合**之后**量 —— 闭合期间问过的节点此刻仍可达，
	 * 才能说"全网都问到了"。
	 */
	snprintf(sql, sizeof(sql),
			 "SELECT partdist.dtx_close_indoubt(%u::oid), "
			 "partdist.pg_raft_all_data_peers_reachable()",
			 (unsigned) shard);
	res = PQexec(conn, sql);
	ok = (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1);
	if (ok)
		m->complete = (strcmp(PQgetvalue(res, 0, 1), "t") == 0);
	*search_complete = m->complete;
	PQclear(res);

	/* 闭合可能刚学到判决：清掉本纪元的失败标记，让后续元组能再问一次 */
	if (ok && reader_memo != NULL)
	{
		HASH_SEQ_STATUS hs;
		DtxReaderMemo *rm;

		hash_seq_init(&hs, reader_memo);
		while ((rm = (DtxReaderMemo *) hash_seq_search(&hs)) != NULL)
			if (rm->verdict == 0)
				rm->fail_epoch = 0;
	}
	return ok;
}

/* ---- T4.5②：决议主动广播（推送通道；拉取仍是兜底真相源） ---- */

/*
 * DtxBroadcastDecision — 决议多数派落盘后，把判决推给各参与节点。
 *
 * 语义（§3.3）：广播是**纯优化**——允许丢失、允许部分失败、绝不阻塞提交
 * 路径的正确性。收不到的分片由读者问询/清扫拉取收敛（那条路已实测 1s 级）。
 * 因此这里所有错误都吞掉：宁可少推一次，绝不让广播失败影响已提交事务。
 *
 * 收件人来自本节点的 partdist.dtx_participant（参与者自治登记的权威表）
 * →节点地址经 node_map 解析；对每个节点调一次 dtx_apply_decision，
 * 对端按 dtxid 找自己的未决登记、幂等落分片 clog（与清扫同一落账函数）。
 * 由 pg_raft 决议点经 rendezvous "partdist_dtx_broadcast_fn" 调用。
 */
/*
 * TPC-C 分支：广播连接按 backend 缓存。原来每个参与节点每笔决议 PQconnectdb 一次
 * （对端 fork 新后端），128 终端下既慢又常撞 connect_timeout —— 推不到的判决堆进
 * 未决登记表，表满后读者全走慢路径。
 */
#define DTX_BCAST_CONN_MAX	16

typedef struct DtxBcastConn
{
	char		host[NAMEDATALEN];
	int			port;
	PGconn	   *conn;
} DtxBcastConn;

static DtxBcastConn bcast_conns[DTX_BCAST_CONN_MAX];
static int	bcast_nconns = 0;

static PGconn *
dtx_bcast_conn(const char *host, int port)
{
	char		conninfo[256];
	int			i;
	DtxBcastConn *c = NULL;

	for (i = 0; i < bcast_nconns; i++)
		if (bcast_conns[i].port == port && strcmp(bcast_conns[i].host, host) == 0)
		{
			c = &bcast_conns[i];
			break;
		}
	if (c != NULL && c->conn != NULL)
	{
		if (PQstatus(c->conn) == CONNECTION_OK &&
			PQtransactionStatus(c->conn) == PQTRANS_IDLE)
			return c->conn;
		PQfinish(c->conn);
		c->conn = NULL;
	}
	if (c == NULL)
	{
		if (bcast_nconns >= DTX_BCAST_CONN_MAX)
			return NULL;
		c = &bcast_conns[bcast_nconns++];
		strlcpy(c->host, host, NAMEDATALEN);
		c->port = port;
		c->conn = NULL;
	}
	snprintf(conninfo, sizeof(conninfo),
			 "host=%s port=%d dbname=postgres user=postgres "
			 "connect_timeout=2 options='-c statement_timeout=2000'",
			 host, port);
	c->conn = PQconnectdb(conninfo);
	if (PQstatus(c->conn) != CONNECTION_OK)
	{
		PQfinish(c->conn);
		c->conn = NULL;
	}
	return c->conn;
}

static void
dtx_bcast_drop(PGconn *conn)
{
	int			i;

	for (i = 0; i < bcast_nconns; i++)
		if (bcast_conns[i].conn == conn)
		{
			PQfinish(conn);
			bcast_conns[i].conn = NULL;
		}
}

void
DtxBroadcastDecision(int64 dtxid, int verdict, int64 commit_ts)
{
	char		sql[1024];		/* 原 256：17 位 dtxid 下参与者查询被截断成语法错误，广播全军覆没 */
	char		hosts[16][NAMEDATALEN];
	int			ports[16];
	int			nodeids[16];
	PGconn	   *sent[16];
	int			nnodes = 0;
	int			self_id = -1;
	int			i;

	if (dtxid == 0 || (verdict != 1 && verdict != 2))
		return;

	{
		const char *nid = GetConfigOption("pg_raft.node_id", true, false);

		if (nid != NULL)
			self_id = atoi(nid);
	}

	/*
	 * ★ SPI 必须在**任何**出口关闭：本函数跑在 dtx_write_decision 的提交
	 * 路径上，SPI 泄漏会让宿主事务收尾时报 "transaction left non-empty SPI
	 * stack"（实测目击）。SPI_execute 抛错时 SPI_finish 走不到——用
	 * PG_TRY/PG_FINALLY 兜住；广播失败本就允许（拉取通道兜底）。
	 */
	if (SPI_connect() != SPI_OK_CONNECT)
		return;
	PG_TRY();
	{
		snprintf(sql, sizeof(sql),
				 "SELECT DISTINCT n.hostname, n.port, n.node_id "
				 "  FROM partdist.dtx_participant dp "
				 "  JOIN partdist.shard_identity si "
				 "    ON si.global_shard_id = ANY (dp.gsids) "
				 "  JOIN partdist.partition_map pm "
				 "    ON pm.partition_id = si.global_shard_id "
				 "  JOIN partdist.node_map n ON n.node_id = pm.primary_node "
				 " WHERE dp.dtxid = %lld", (long long) dtxid);
		if (SPI_execute(sql, true, 0) == SPI_OK_SELECT)
		{
			int			np = (int) SPI_processed;

			for (i = 0; i < np && nnodes < 16; i++)
			{
				bool		isnull;
				bool		idnull;
				char	   *h = SPI_getvalue(SPI_tuptable->vals[i],
											 SPI_tuptable->tupdesc, 1);
				Datum		pd = SPI_getbinval(SPI_tuptable->vals[i],
											   SPI_tuptable->tupdesc, 2,
											   &isnull);
				Datum		nd = SPI_getbinval(SPI_tuptable->vals[i],
											   SPI_tuptable->tupdesc, 3,
											   &idnull);

				if (h == NULL || isnull)
					continue;
				strlcpy(hosts[nnodes], h, NAMEDATALEN);
				ports[nnodes] = DatumGetInt32(pd);
				nodeids[nnodes] = idnull ? -2 : DatumGetInt32(nd);
				nnodes++;
			}
		}
	}
	PG_FINALLY();
	{
		SPI_finish();
	}
	PG_END_TRY();

	/* 先把远端的请求全发出去（流水），本节点那份就地落账，最后统一收回执 */
	snprintf(sql, sizeof(sql),
			 "SELECT partdist.dtx_apply_decision(%lld, %d, %lld)",
			 (long long) dtxid, verdict, (long long) commit_ts);
	for (i = 0; i < nnodes; i++)
	{
		sent[i] = NULL;
		if (nodeids[i] == self_id)
			continue;
		sent[i] = dtx_bcast_conn(hosts[i], ports[i]);
		if (sent[i] != NULL && !PQsendQuery(sent[i], sql))
		{
			dtx_bcast_drop(sent[i]);
			sent[i] = NULL;		/* 推不到就算了，拉取通道兜底 */
		}
	}
	for (i = 0; i < nnodes; i++)
		if (nodeids[i] == self_id)
		{
			int			n = DtxApplyDecisionByDtxid(dtxid, verdict, commit_ts);

			if (n == 0)
				elog(DEBUG1, "pg_partdist: 广播本机落账 0 笔 dtxid=%lld pending=%d",
					 (long long) dtxid, DtxPendingCount());
		}
	if (nnodes == 0)
		elog(DEBUG1, "pg_partdist: 广播收件人为空 dtxid=%lld", (long long) dtxid);
	for (i = 0; i < nnodes; i++)
	{
		PGresult   *res;
		bool		bad = false;

		if (sent[i] == NULL)
			continue;
		while ((res = PQgetResult(sent[i])) != NULL)
		{
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
				bad = true;
			PQclear(res);
		}
		if (bad && PQtransactionStatus(sent[i]) != PQTRANS_IDLE)
			dtx_bcast_drop(sent[i]);
	}
}

/*
 * R-P4-9：按 dtxid 把判决落进本节点分片 clog（供 pg_raft 恢复守护在
 * COMMIT/ROLLBACK PREPARED **之前**调用）。
 *
 * 为什么必须先落账再闭合：闭合之后原生 prepared 消失，2PC 段随之作废，
 * 未决登记失去载体——此时若判决还没写进 clog，那些行就永久不可见
 * （切主后阶段 3 标记的本地写被写栅栏拒，正是这条路的实测形态）。
 * 反过来"先落账、后闭合"：落账失败可整轮重来（登记还在），闭合失败也
 * 只是下一轮再闭合一次（落账幂等）。
 *
 * 返回落账笔数（0 = 本节点没参与 / 已收敛）。
 */
int
DtxApplyDecisionByDtxid(int64 dtxid, int verdict, int64 commit_ts)
{
	/*
	 * 同一 dtxid 在本节点通常只有 1 条登记（多条连接的段已按 gxid 并成一条）；
	 * 原来每次 palloc 整表 2 MB 当快照，广播每笔都付。这里按需拷，最多 8 条一批。
	 */
	DtxPendingEntry snap[8];
	int			nsnap = 0;
	int			ndone = 0;
	int			i;

	if (PendingCtl == NULL || dtxid == 0 || (verdict != 1 && verdict != 2))
		return 0;

	SpinLockAcquire(&PendingCtl->mutex);
	for (i = 0; i < DTX_PENDING_MAX && nsnap < lengthof(snap); i++)
		if (PendingCtl->gx[i] != 0 && PendingCtl->dx[i] == dtxid)
			snap[nsnap++] = PendingCtl->e[i];
	SpinLockRelease(&PendingCtl->mutex);

	for (i = 0; i < nsnap; i++)
	{
		(void) dtx_pending_apply_verdict(&snap[i], verdict, commit_ts);	/* 落账即算送达；复制失败由清扫接手 */
		ndone++;
	}
	return ndone;
}

/*
 * 广播的接收端：按 dtxid 找本节点未决登记，幂等落账（与清扫同一函数）。
 * 找不到登记 = 本节点没参与 / 已收敛，返回 0；落账成功返回 1。
 */
PG_FUNCTION_INFO_V1(partdist_dtx_apply_decision);
Datum
partdist_dtx_apply_decision(PG_FUNCTION_ARGS)
{
	int64		dtxid = PG_GETARG_INT64(0);
	int32		verdict = PG_GETARG_INT32(1);
	int64		cts = PG_GETARG_INT64(2);

	PG_RETURN_INT32(DtxApplyDecisionByDtxid(dtxid, (int) verdict, cts));
}

/* ---- 心跳工作者的自连触发（无 DB 语境，纯 libpq） ---- */

void
DtxPendingSelfTriggerSweep(void)
{
	char		conninfo[256];
	PGconn	   *conn;
	PGresult   *res;

	snprintf(conninfo, sizeof(conninfo),
			 "host=/tmp port=%d dbname=postgres user=postgres connect_timeout=2",
			 PostPortNumber);
	conn = PQconnectdb(conninfo);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		PQfinish(conn);
		return;
	}
	res = PQexec(conn, "SELECT partdist.dtx_pending_sweep()");
	if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0 &&
		strcmp(PQgetvalue(res, 0, 0), "0") != 0)
		elog(LOG, "pg_partdist: 未决 2PC 清扫收敛 %s 笔", PQgetvalue(res, 0, 0));
	PQclear(res);
	PQfinish(conn);
}

/* ---- SQL 入口 ---- */

PG_FUNCTION_INFO_V1(partdist_dtx_pending_sweep);
Datum
partdist_dtx_pending_sweep(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(DtxPendingSweep());
}

PG_FUNCTION_INFO_V1(partdist_dtx_pending_count);
Datum
partdist_dtx_pending_count(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(DtxPendingCount());
}

PG_FUNCTION_INFO_V1(partdist_dtx_inquire);
Datum
partdist_dtx_inquire(PG_FUNCTION_ARGS)
{
	int64		coord_gsid = PG_GETARG_INT64(0);
	int64		dtxid = PG_GETARG_INT64(1);
	int64		cts = 0;
	bool		answered = false;
	int			verdict = dtx_inquire_core_ex(coord_gsid, dtxid, &cts, &answered);
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false, false};

	/* P7-N52：没有任何权威答复 ⇒ verdict 为 NULL（读者据此报可重试错误，而不是当未提交） */
	if (verdict == 0 && !answered)
		nulls[0] = true;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "partdist_dtx_inquire: 需要复合返回类型");
	values[0] = Int32GetDatum(verdict);
	values[1] = Int64GetDatum(cts);
	PG_RETURN_DATUM(HeapTupleGetDatum(
					heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls)));
}

/* 调试/验收辅助：登记表内容一览（每行 gxid|coord_gsid|dtxid|start_ts|nxids） */
PG_FUNCTION_INFO_V1(partdist_dtx_pending_dump);
Datum
partdist_dtx_pending_dump(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	int			i;

	initStringInfo(&buf);
	if (PendingCtl != NULL)
	{
		SpinLockAcquire(&PendingCtl->mutex);
		for (i = 0; i < DTX_PENDING_MAX; i++)
		{
			DtxPendingEntry *e = &PendingCtl->e[i];

			if (e->gxid == 0)
				continue;
			appendStringInfo(&buf, "%lld|%lld|%lld|%lld|%d\n",
							 (long long) e->gxid, (long long) e->coord_gsid,
							 (long long) e->dtxid, (long long) e->start_ts,
							 e->nxids);
		}
		SpinLockRelease(&PendingCtl->mutex);
	}
	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
