/*
 * dtx_pending.c — T4.5：未决 2PC 登记表 + 持久日志 + 决议收敛（清扫/读者问询）
 *
 * 设计脉络见 dtx_pending.h 头注释与 TX_TSO_MVCC_DESING.md §3.3/§4.2。
 * 拉取收敛架构：不依赖决议方推送（广播允许丢失的极限情形 = 从未推送），
 * 一切判决从协调组 leader 的 partdist.dtx_decision 只读问询而来。
 */
#include "postgres.h"

#include "dtx_pending.h"
#include "shard_clog.h"

#include "access/htup_details.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"	/* PostPortNumber */
#include "storage/fd.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "access/xact.h"

#include <fcntl.h>
#include <unistd.h>

/* ---- shmem ---- */

typedef struct DtxPendingCtl
{
	slock_t		mutex;
	int			nused;
	DtxPendingEntry e[DTX_PENDING_MAX];
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
		if (PendingCtl->e[i].gxid == gxid)
			return &PendingCtl->e[i];
	return NULL;
}

static void
pending_insert_locked(const DtxPendingEntry *ent)
{
	DtxPendingEntry *slot = pending_find_locked(ent->gxid);

	if (slot == NULL)
		slot = pending_find_locked(0);
	if (slot == NULL)
	{
		/* 满了：不覆盖别人（覆盖 = 那笔事务永失收敛通道），只告警放弃。
		 * 128 槽 × 节点级在测试与常规负载下到不了；到了先解决泄漏源。 */
		elog(WARNING,
			 "pg_partdist: 未决 2PC 登记表已满（%d），gxid=" INT64_FORMAT
			 " 未登记——该事务只能靠恢复守护收敛", DTX_PENDING_MAX, ent->gxid);
		return;
	}
	if (slot->gxid == 0)
		PendingCtl->nused++;
	*slot = *ent;
}

static void
pending_remove_locked(int64 gxid)
{
	DtxPendingEntry *slot = pending_find_locked(gxid);

	if (slot != NULL)
	{
		memset(slot, 0, sizeof(*slot));
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
			pending_insert_locked(&rec.ent);
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

	/* 持久化点：OPEN fsync 先于 shmem 登记（也先于 PREPARE 的 WAL 刷盘） */
	dtx_jrnl_append(DTX_JRNL_OPEN, &ent, true);

	SpinLockAcquire(&PendingCtl->mutex);
	pending_insert_locked(&ent);
	SpinLockRelease(&PendingCtl->mutex);
}

/* 崩溃恢复的 2PC 段重放路径：日志里通常已有，缺了才补（幂等） */
void
DtxPendingReRegister(int64 gxid, int64 coord_gsid, int64 dtxid,
					 int64 start_ts, int nxids, const uint32 *pairs)
{
	bool		have;

	if (gxid == 0 || PendingCtl == NULL)
		return;
	SpinLockAcquire(&PendingCtl->mutex);
	have = (pending_find_locked(gxid) != NULL);
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
		if (PendingCtl->e[i].gxid != 0 && PendingCtl->e[i].dtxid == dtxid)
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
dtx_inquire_core(int64 coord_gsid, int64 dtxid, int64 *cts_out)
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
		}
		PQclear(res);
		PQfinish(conn);
	}
	if (verdict != 1 && verdict != 2)
		return 0;
	return verdict;
}

/* 学到判决：整笔（本节点全部 pairs）幂等落分片 clog + 注销 */
static void
dtx_pending_apply_verdict(const DtxPendingEntry *ent, int verdict, int64 cts)
{
	int			i;

	for (i = 0; i < ent->nxids; i++)
		ShardClogSetVerdict((Oid) ent->pairs[2 * i],
							(TransactionId) ent->pairs[2 * i + 1],
							verdict == 1, verdict == 1 ? cts : 0);
	DtxPendingFinalized(ent->gxid);
}

/* ---- 清扫（backend 语境） ---- */

int
DtxPendingSweep(void)
{
	DtxPendingEntry snap[DTX_PENDING_MAX];
	int			nsnap = 0;
	int			ndone = 0;
	int			i;

	if (PendingCtl == NULL)
		return 0;

	SpinLockAcquire(&PendingCtl->mutex);
	for (i = 0; i < DTX_PENDING_MAX; i++)
		if (PendingCtl->e[i].gxid != 0)
			snap[nsnap++] = PendingCtl->e[i];
	SpinLockRelease(&PendingCtl->mutex);

	for (i = 0; i < nsnap; i++)
	{
		int64		cts = 0;
		int			verdict = dtx_inquire_core(snap[i].coord_gsid,
											   snap[i].dtxid, &cts);

		CHECK_FOR_INTERRUPTS();
		if (verdict != 0)
		{
			dtx_pending_apply_verdict(&snap[i], verdict, cts);
			ndone++;
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
				if (ShardClogReadStatus((Oid) snap[i].pairs[2 * j],
										(TransactionId) snap[i].pairs[2 * j + 1])
					== TXN_PREPARED)
				{
					any_prepared = true;
					break;
				}
			}
			if (any_prepared && SPI_connect() == SPI_OK_CONNECT)
			{
				bool		any_rel = false;

				for (j = 0; j < snap[i].nxids; j++)
				{
					char		q[96];

					snprintf(q, sizeof(q),
							 "SELECT 1 FROM pg_catalog.pg_class WHERE oid = %u",
							 (unsigned) snap[i].pairs[2 * j]);
					if (SPI_execute(q, true, 1) == SPI_OK_SELECT &&
						SPI_processed > 0)
					{
						any_rel = true;
						break;
					}
				}
				SPI_finish();
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

int
DtxReaderResolve(int64 gxid, int64 *cts_out)
{
	DtxReaderMemo *memo;
	DtxPendingEntry ent;
	char		sql[192];
	int			verdict = 0;
	int64		cts = 0;

	*cts_out = 0;
	if (gxid == 0)
		return 0;

	memo = reader_memo_get(gxid, false);
	if (memo != NULL && memo->verdict != 0)
	{
		*cts_out = memo->cts;
		return memo->verdict;
	}
	if (memo != NULL && memo->fail_epoch == xact_epoch)
		return 0;				/* 本事务已问过未果——不重试（限流） */

	if (!DtxPendingLookup(gxid, &ent))
		return 0;				/* ②：登记缺失 ⇒ 决议必然未做 ⇒ 跳过 */

	/*
	 * 经自连问询（partdist.dtx_inquire 在干净 backend 里做 SPI 寻址 +
	 * 远程 peek）。读路径可能持着 buffer 锁，连接与语句都限短超时；
	 * 失败按未果处置并 memo 本事务不再重试。
	 */
	{
		static PGconn *selfconn = NULL;
		PGresult   *res;

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
					 "connect_timeout=2 options='-c statement_timeout=2000'",
					 PostPortNumber);
			selfconn = PQconnectdb(conninfo);
			if (PQstatus(selfconn) != CONNECTION_OK)
			{
				PQfinish(selfconn);
				selfconn = NULL;
			}
		}
		if (selfconn != NULL)
		{
			snprintf(sql, sizeof(sql),
					 "SELECT verdict, commit_ts FROM partdist.dtx_inquire(%lld, %lld)",
					 (long long) ent.coord_gsid, (long long) ent.dtxid);
			res = PQexec(selfconn, sql);
			if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0)
			{
				verdict = atoi(PQgetvalue(res, 0, 0));
				cts = strtoll(PQgetvalue(res, 0, 1), NULL, 10);
			}
			PQclear(res);
		}
	}

	memo = reader_memo_get(gxid, true);
	if (verdict == 1 || verdict == 2)
	{
		/* 学到即回写（幂等）+ 注销；memo 使同快照后续元组零 RPC */
		dtx_pending_apply_verdict(&ent, verdict, cts);
		memo->verdict = verdict;
		memo->cts = (verdict == 1) ? cts : 0;
		*cts_out = memo->cts;
		return verdict;
	}
	memo->verdict = 0;
	memo->fail_epoch = xact_epoch;
	return 0;
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
void
DtxBroadcastDecision(int64 dtxid, int verdict, int64 commit_ts)
{
	char		sql[256];
	char		hosts[16][NAMEDATALEN];
	int			ports[16];
	int			nnodes = 0;
	int			i;

	if (dtxid == 0 || (verdict != 1 && verdict != 2))
		return;

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
				 "SELECT DISTINCT n.hostname, n.port "
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
				char	   *h = SPI_getvalue(SPI_tuptable->vals[i],
											 SPI_tuptable->tupdesc, 1);
				Datum		pd = SPI_getbinval(SPI_tuptable->vals[i],
											   SPI_tuptable->tupdesc, 2,
											   &isnull);

				if (h == NULL || isnull)
					continue;
				strlcpy(hosts[nnodes], h, NAMEDATALEN);
				ports[nnodes] = DatumGetInt32(pd);
				nnodes++;
			}
		}
	}
	PG_FINALLY();
	{
		SPI_finish();
	}
	PG_END_TRY();

	for (i = 0; i < nnodes; i++)
	{
		char		conninfo[256];
		PGconn	   *conn;
		PGresult   *res;

		snprintf(conninfo, sizeof(conninfo),
				 "host=%s port=%d dbname=postgres user=postgres "
				 "connect_timeout=2 options='-c statement_timeout=2000'",
				 hosts[i], ports[i]);
		conn = PQconnectdb(conninfo);
		if (PQstatus(conn) != CONNECTION_OK)
		{
			PQfinish(conn);
			continue;			/* 推不到就算了，拉取通道兜底 */
		}
		snprintf(sql, sizeof(sql),
				 "SELECT partdist.dtx_apply_decision(%lld, %d, %lld)",
				 (long long) dtxid, verdict, (long long) commit_ts);
		res = PQexec(conn, sql);
		PQclear(res);
		PQfinish(conn);
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
	DtxPendingEntry snap[DTX_PENDING_MAX];
	int			nsnap = 0;
	int			ndone = 0;
	int			i;

	if (PendingCtl == NULL || dtxid == 0 || (verdict != 1 && verdict != 2))
		return 0;

	SpinLockAcquire(&PendingCtl->mutex);
	for (i = 0; i < DTX_PENDING_MAX; i++)
		if (PendingCtl->e[i].gxid != 0 && PendingCtl->e[i].dtxid == dtxid)
			snap[nsnap++] = PendingCtl->e[i];
	SpinLockRelease(&PendingCtl->mutex);

	for (i = 0; i < nsnap; i++)
	{
		dtx_pending_apply_verdict(&snap[i], verdict, commit_ts);
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
	int			verdict = dtx_inquire_core(coord_gsid, dtxid, &cts);
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false, false};

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
