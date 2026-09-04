/*
 * shard_clog.c
 *
 * 分片级 clog 存储层最小实现（设计 §5.3，P2 T2.1）。
 * 语义、并发与持久化契约见 shard_clog.h 头注释。
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "shard_clog.h"
#include "shard_fileset.h"	/* T5.4b-2：水位 CTRL 发射 */
#include "shard_xid.h"

#include "access/transam.h"
#include "funcapi.h"			/* T5.2 SQL 包装：复合返回 */
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "common/relpath.h"

StaticAssertDecl(sizeof(ShardClogSlot) == 32,
				 "ShardClogSlot 必须是 32 字节（pg_shard_clog 的磁盘格式）");
StaticAssertDecl(offsetof(ShardClogSlot, status) == 24,
				 "status 必须落在偏移 24");

static void
ShardClogSegPath(char *path, size_t pathlen, Oid shard, uint32 segno)
{
	snprintf(path, pathlen, "%s/%s/%u/%08X",
			 DataDir, SHARD_CLOG_DIR, shard, segno);
}

static void
ShardClogDirPath(char *path, size_t pathlen, Oid shard)
{
	snprintf(path, pathlen, "%s/%s/%u", DataDir, SHARD_CLOG_DIR, shard);
}

/* EEXIST 是常态 —— 多后端并发建同一个目录 */
static void
ShardClogEnsureDir(Oid shard)
{
	char		path[MAXPGPATH];

	snprintf(path, MAXPGPATH, "%s/%s", DataDir, SHARD_CLOG_DIR);
	if (MakePGDirectory(path) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建目录 \"%s\": %m", path)));

	ShardClogDirPath(path, MAXPGPATH, shard);
	if (MakePGDirectory(path) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建目录 \"%s\": %m", path)));
}

/* fsync 分片目录 —— 新建段文件后目录项也要持久（durable_rename 同款纪律） */
static void
ShardClogFsyncDir(Oid shard)
{
	char		path[MAXPGPATH];
	int			fd;

	ShardClogDirPath(path, MAXPGPATH, shard);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法打开目录 \"%s\": %m", path)));
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 目录 \"%s\" fsync 失败: %m", path)));
	}
	CloseTransientFile(fd);
}

/*
 * 打开段文件。create=false 且不存在时返回 -1（读路径当作全洞）。
 * create=true 时若段是新建的，顺带 fsync 分片目录（*created 告知调用方，
 * 但目录持久化在本函数内已完成）。调用方负责 CloseTransientFile。
 */
static int
ShardClogOpenSegFile(Oid shard, uint32 segno, bool create)
{
	char		path[MAXPGPATH];
	int			fd;

	ShardClogSegPath(path, MAXPGPATH, shard, segno);

	/* 先试已存在的（多数路径），免掉目录操作 */
	fd = OpenTransientFile(path, (create ? O_RDWR : O_RDONLY) | PG_BINARY);
	if (fd >= 0)
		return fd;
	if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法打开分片 clog 段 \"%s\": %m", path)));
	if (!create)
		return -1;

	ShardClogEnsureDir(shard);
	fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建分片 clog 段 \"%s\": %m", path)));
	ShardClogFsyncDir(shard);
	return fd;
}

/* 写一个槽（幂等 pwrite，无锁 —— 论证见头文件）。durable=true 时 fsync 段。 */
static void
ShardClogWriteSlot(Oid shard, TransactionId sxid,
				   const ShardClogSlot *slot, bool durable)
{
	uint32		segno = sxid / SHARD_CLOG_XIDS_PER_SEGMENT;
	off_t		off = (off_t) (sxid % SHARD_CLOG_XIDS_PER_SEGMENT)
		* SHARD_CLOG_SLOT_SIZE;
	int			fd;
	ssize_t		nb;

	if (sxid < FirstNormalTransactionId)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist: 分片 clog 拒写保留 xid %u（shard %u）",
						sxid, shard)));

	fd = ShardClogOpenSegFile(shard, segno, true);

	do
	{
		nb = pg_pwrite(fd, slot, sizeof(ShardClogSlot), off);
	} while (nb < 0 && errno == EINTR);

	if (nb != (ssize_t) sizeof(ShardClogSlot))
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 分片 clog 写入失败 shard=%u xid=%u: %m",
						shard, sxid)));
	}

	if (durable && pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 分片 clog 段 fsync 失败 shard=%u seg=%08X: %m",
						shard, segno)));
	}
	CloseTransientFile(fd);
}

void
ShardClogSetRunning(Oid shard, TransactionId sxid, int64 start_ts)
{
	ShardClogSlot slot;

	memset(&slot, 0, sizeof(slot));	/* 全零 = RUNNING；start_ts 是附注列 */
	slot.start_ts = (uint64) start_ts;
	ShardClogWriteSlot(shard, sxid, &slot, false);
}

void
ShardClogSetVerdict(Oid shard, TransactionId sxid, bool committed,
					int64 commit_ts)
{
	ShardClogSlot slot;

	/* 读改写：保留 RUNNING 落账时写下的 start_ts（§5.3 行五列） */
	if (!ShardClogReadSlot(shard, sxid, &slot))
		memset(&slot, 0, sizeof(slot));
	slot.status = (uint32) (committed ? TXN_COMMITTED : TXN_ABORTED);
	slot.commit_ts = committed ? (uint64) commit_ts : 0;
	ShardClogWriteSlot(shard, sxid, &slot, true);
}

void
ShardClogSetPrepared(Oid shard, TransactionId sxid, int64 start_ts, int64 gxid)
{
	ShardClogSlot slot;

	/* 读改写：RUNNING 落账里的 start_ts 若已在，以入参为准（同源同值） */
	if (!ShardClogReadSlot(shard, sxid, &slot))
		memset(&slot, 0, sizeof(slot));
	slot.status = (uint32) TXN_PREPARED;
	slot.start_ts = (uint64) start_ts;
	slot.global_xid = (uint64) gxid;
	ShardClogWriteSlot(shard, sxid, &slot, true);
}

bool
ShardClogReadSlot(Oid shard, TransactionId sxid, ShardClogSlot *out)
{
	uint32		segno = sxid / SHARD_CLOG_XIDS_PER_SEGMENT;
	off_t		off = (off_t) (sxid % SHARD_CLOG_XIDS_PER_SEGMENT)
		* SHARD_CLOG_SLOT_SIZE;
	int			fd;
	ssize_t		nb;

	memset(out, 0, sizeof(*out));	/* 缺席 = 全零 = RUNNING */

	/* 0/1/2 保留号永不落账；防御性按未决处理 */
	if (sxid < FirstNormalTransactionId)
		return false;

	fd = ShardClogOpenSegFile(shard, segno, false);
	if (fd < 0)
		return false;			/* 整段没建过 = 全洞 = 未决 */

	do
	{
		nb = pg_pread(fd, out, sizeof(*out), off);
	} while (nb < 0 && errno == EINTR);

	if (nb != (ssize_t) sizeof(*out) && nb != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 分片 clog 读取不完整 shard=%u xid=%u (%zd/%u)",
						shard, sxid, nb, SHARD_CLOG_SLOT_SIZE)));
	}
	CloseTransientFile(fd);

	if (nb == 0)
	{
		memset(out, 0, sizeof(*out));	/* 文件尾之外 = 洞 */
		return false;
	}
	return true;
}

TxnStatus
ShardClogReadStatus(Oid shard, TransactionId sxid)
{
	ShardClogSlot slot;

	(void) ShardClogReadSlot(shard, sxid, &slot);
	return (TxnStatus) slot.status;
}

/*
 * T2.4 认领：[from, to) 内 RUNNING（含洞）→ ABORTED。逐段处理：段内逐槽
 * pread 判状态、RUNNING 则 pwrite ABORTED，段收尾一次 fsync。范围来自
 * "冻结的启动恢复上限"（调用方 shard_xid.c 保证，R-P2-2），本函数只管
 * 机械改判。PREPARED 保留是 §6.6 第二分支的"不许动"（P4 才有人写它）。
 */
int
ShardClogClaimRange(Oid shard, TransactionId from, TransactionId to)
{
	int			claimed = 0;
	TransactionId sxid;

	if (from < FirstNormalTransactionId)
		from = FirstNormalTransactionId;

	sxid = from;
	while (sxid < to)
	{
		uint32		segno = sxid / SHARD_CLOG_XIDS_PER_SEGMENT;
		TransactionId seg_end = (TransactionId) (segno + 1) *
			SHARD_CLOG_XIDS_PER_SEGMENT;
		TransactionId upto = Min(to, seg_end);
		int			fd = ShardClogOpenSegFile(shard, segno, true);
		bool		dirtied = false;

		for (; sxid < upto; sxid++)
		{
			off_t		off = (off_t) (sxid % SHARD_CLOG_XIDS_PER_SEGMENT)
				* SHARD_CLOG_SLOT_SIZE;
			ShardClogSlot slot;
			ssize_t		nb;

			do
			{
				nb = pg_pread(fd, &slot, sizeof(slot), off);
			} while (nb < 0 && errno == EINTR);

			if (nb != (ssize_t) sizeof(slot) && nb != 0)
			{
				CloseTransientFile(fd);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("pg_partdist: 认领读槽失败 shard=%u xid=%u (%zd)",
								shard, sxid, nb)));
			}
			if (nb == (ssize_t) sizeof(slot) &&
				(TxnStatus) slot.status != TXN_RUNNING)
				continue;		/* 终局/PREPARED 保留 */

			memset(&slot, 0, sizeof(slot));
			slot.status = (uint32) TXN_ABORTED;
			do
			{
				nb = pg_pwrite(fd, &slot, sizeof(slot), off);
			} while (nb < 0 && errno == EINTR);
			if (nb != (ssize_t) sizeof(slot))
			{
				CloseTransientFile(fd);
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("pg_partdist: 认领改判失败 shard=%u xid=%u: %m",
								shard, sxid)));
			}
			dirtied = true;
			claimed++;
		}

		if (dirtied && pg_fsync(fd) != 0)
		{
			CloseTransientFile(fd);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("pg_partdist: 认领段 fsync 失败 shard=%u seg=%08X: %m",
							shard, segno)));
		}
		CloseTransientFile(fd);
	}
	return claimed;
}

/*
 * 0007 redo 钩子：崩溃恢复重放 commit/abort 记录时重做判决。
 * 幂等（重复 redo 写同样字节）；ShardClogSetVerdict 自带 fsync，ERROR 会
 * 中止恢复——与原生 clog 写盘失败同级别，正确的失败方式。
 */
void
ShardClogXactRedo(int nxids, const uint32 *pairs, uint64 commit_ts,
				  bool committed)
{
	int			i;

	for (i = 0; i < nxids; i++)
	{
		ShardClogSetVerdict((Oid) pairs[2 * i],
							(TransactionId) pairs[2 * i + 1],
							committed, (int64) commit_ts);
		/* T2.5：顺手累计影子推进（水位文件缺失/落后时的发号起点兜底） */
		ShardXidRedoAdvance((Oid) pairs[2 * i],
							(TransactionId) pairs[2 * i + 1]);
	}
}

/* ================= DROP TABLE 提交时点 GC ================= */

#define SHARD_CLOG_PENDING_DROPS_MAX 16

static Oid	pending_drops[SHARD_CLOG_PENDING_DROPS_MAX];
static int	pending_drops_n = 0;

void
ShardClogRememberDrop(Oid shard)
{
	int			i;

	for (i = 0; i < pending_drops_n; i++)
		if (pending_drops[i] == shard)
			return;

	if (pending_drops_n >= SHARD_CLOG_PENDING_DROPS_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_partdist: 单事务最多 DROP %d 张分片打标表",
						SHARD_CLOG_PENDING_DROPS_MAX)));

	pending_drops[pending_drops_n++] = shard;
}

bool
ShardClogHasPendingDrops(void)
{
	return pending_drops_n > 0;
}

/*
 * 提交时点执行删除。提交已成事实，这里不许 ERROR（会把提交翻成 PANIC 级
 * 混乱）—— 删不掉只 WARNING，残留是无害孤儿（头文件"已知边界"）。
 * 崩在提交记录与本函数之间同理：孤儿目录，T2.7 注册前清目录兜底 OID 复用。
 */
void
ShardClogAtCommit(void)
{
	int			i;

	for (i = 0; i < pending_drops_n; i++)
	{
		char		path[MAXPGPATH];
		struct stat st;

		ShardClogDirPath(path, MAXPGPATH, pending_drops[i]);
		/* 目录可能从未建过（表没写过判决）——rmtree 会自己打 WARNING，先探 */
		if (stat(path, &st) == 0 && !rmtree(path, true))
			ereport(WARNING,
					(errmsg("pg_partdist: 分片 clog 目录 \"%s\" 删除不完整，"
							"残留为无害孤儿", path)));

		snprintf(path, MAXPGPATH, "%s/pg_shard_xid/%u",
				 DataDir, pending_drops[i]);
		if (unlink(path) != 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("pg_partdist: 水位文件 \"%s\" 删除失败: %m", path)));

		/*
		 * R-P6-9：共享内存里的打标登记也要摘掉。此前只清文件不清登记，
		 * 而这个函数上方那句"DROP TABLE 会连同水位/clog 文件一并清理"正是
		 * `partdist_set_shard_mvcc(..., false)` 拒绝撤销时给出的理由 ——
		 * 承诺与实现对不上。留着的后果不是多占一个槽位，而是 **OID 复用后
		 * 误伤无关表**：新表被判成分片打标表，若它是分布式表，DROP 走 2PC
		 * 就撞上 §10 的 PRE_PREPARE 禁令删不掉。实测表现为夹具残表逐轮累积
		 * （122→244→366），症状伪装成"回放写多了"。
		 */
		ShardMvccSetRemove(pending_drops[i]);
	}
	pending_drops_n = 0;
}

void
ShardClogAtAbort(void)
{
	pending_drops_n = 0;
}

/* ================= T5.4：clog 截断（设计 §6.4 顺序铁律）================= */

int
ShardClogTruncate(Oid shard, TransactionId trunc_before)
{
	TransactionId cur_tb;
	TransactionId vacuum_xid;
	uint32		nfull;
	uint32		segno;
	int			removed = 0;

	if (!TransactionIdIsValid(trunc_before) || trunc_before <= FIRST_SHARD_XID)
		return 0;				/* 没什么可截的 */

	ShardVacuumGetWatermarks(shard, &cur_tb, &vacuum_xid);

	if (trunc_before < cur_tb)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("分片 %u 的截断点不许后退：目标 %u < 当前 %u",
						shard, trunc_before, cur_tb)));

	/*
	 * ★ 顺序铁律的落地点（设计 §6.4 末）：页面动作没做完就不许动 clog。
	 * shard_vacuum_xid 是"整趟页面动作已完成"的唯一凭据，只由
	 * ShardVacuumSweep 在三类动作全部清完（无跳页、无推迟）后落下。
	 */
	if (trunc_before > vacuum_xid)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("分片 %u 的页面尚未清到 %u（趟标记只到 %u），不许截断 clog",
						shard, trunc_before, vacuum_xid),
				 errdetail("设计 §6.4 顺序铁律：数据页、索引、堆全部清完，才许动 clog。"
						   "此时截断会让中止事务的幽灵行复活、让活行被判死。"),
				 errhint("先跑一趟完整的 partdist.shard_vacuum_sweep()。")));

	/*
	 * ★ 先推水位、后删文件。反过来一旦在中间崩溃，clog 没了而水位还说"要查
	 * clog"，那些 xid 读成空洞=RUNNING=不可见，已提交数据当场消失。
	 */
	if (trunc_before > cur_tb)
	{
		ShardVacuumSetWatermarks(shard, trunc_before, vacuum_xid);

		/*
		 * §6.7：两个水位作为 CTRL 记录写进本分片流，follower 据此建立同一个
		 * 免查区。放在推水位之后、删文件之前 —— 与"先推水位后删文件"同一个
		 * 理由：先让语义到位，再回收空间。尽力而为，失败不影响本次截断。
		 */
		ShardVacuumEmitWatermarkCtrl(shard, trunc_before, vacuum_xid);
	}

	/* 只删完全落在 trunc_before 以下的整段；跨界那一段留着 */
	nfull = trunc_before / SHARD_CLOG_XIDS_PER_SEGMENT;
	for (segno = 0; segno < nfull; segno++)
	{
		char		path[MAXPGPATH];

		ShardClogSegPath(path, MAXPGPATH, shard, segno);
		if (unlink(path) == 0)
			removed++;
		else if (errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("pg_partdist: 删除分片 clog 段 \"%s\" 失败: %m", path)));
	}

	if (removed > 0)
		ShardClogFsyncDir(shard);

	return removed;
}

/* ================= 验收/运维用 SQL 包装 =================
 * T2.1 验收与 T2.8 套件直接 CREATE FUNCTION ... '$libdir/pg_partdist' 使用；
 * 正式并入扩展 SQL 文件随 T2.4 的显式函数一批做。
 */

PG_FUNCTION_INFO_V1(partdist_shard_clog_read_full);
Datum
partdist_shard_clog_read_full(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = (TransactionId) PG_GETARG_INT64(1);
	ShardClogSlot slot;
	char		buf[96];

	(void) ShardClogReadSlot(shard, sxid, &slot);
	snprintf(buf, sizeof(buf),
			 "st=%u sts=" UINT64_FORMAT " cts=" UINT64_FORMAT,
			 slot.status, slot.start_ts, slot.commit_ts);
	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

PG_FUNCTION_INFO_V1(partdist_shard_claim);
Datum
partdist_shard_claim(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);

	PG_RETURN_INT32((int32) ShardXidEnsureClaimed(shard));
}

PG_FUNCTION_INFO_V1(partdist_shard_clog_read);
Datum
partdist_shard_clog_read(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = (TransactionId) PG_GETARG_INT64(1);

	PG_RETURN_INT32((int32) ShardClogReadStatus(shard, sxid));
}

PG_FUNCTION_INFO_V1(partdist_shard_clog_write);
Datum
partdist_shard_clog_write(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = (TransactionId) PG_GETARG_INT64(1);
	int32		status = PG_GETARG_INT32(2);

	switch ((TxnStatus) status)
	{
		case TXN_RUNNING:
			ShardClogSetRunning(shard, sxid, 0);
			break;
		case TXN_COMMITTED:
			ShardClogSetVerdict(shard, sxid, true, 0);
			break;
		case TXN_ABORTED:
			ShardClogSetVerdict(shard, sxid, false, 0);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("status 只接受 0(RUNNING)/2(COMMITTED)/3(ABORTED)，"
							"PREPARED 是 P4 的事")));
	}
	PG_RETURN_VOID();
}

/* ================= T5.2 前缀扫描：算 VacuumTargetXid（设计 §6.3）=================
 *
 * 从 clog_truncate_before 起顺扫本分片 clog，返回可安全截断到的**前一条**。
 *
 * 放行（可越过）：
 *   · COMMITTED 且 commit_ts < GlobalSafeTs —— 没有活跃快照还需要它之前的版本；
 *   · **ABORTED 一律放行** —— 这是本规则最反直觉、也最要紧的一条。若 ABORTED
 *     也挡，**一个中止事务就能永久钉死截断，直到回卷死亡**；它留下的垃圾由
 *     §6.4 的页面三类动作清掉，不需要靠挡住前缀来保证。
 *
 * 停止（VacuumTargetXid = 其前一条）：
 *   · RUNNING —— 未决，之后的判决还没写；
 *   · PREPARED —— 2PC 未决，**绝不允许单方推定**；
 *   · COMMITTED 但 commit_ts >= GlobalSafeTs —— 仍可能被活跃快照看到。
 *
 * 可证性质（设计原文备查）：RUNNING 阻挡者之上不存在 commit_ts < GlobalSafeTs
 * 的条目（start_ts 先于首写、首写序即落账序、TSO 单调，三段传递）。故前缀规则
 * 在 RUNNING 阻挡下几乎无损，损失的只是阻挡点之上 ABORTED 事务的垃圾。
 * "死元组清除与前缀截断解耦"记为后续优化，第一期不做。
 *
 * 返回 InvalidTransactionId 表示"一条都不能清"（含 safe_ts 取不到的情形）。
 */
TransactionId
ShardVacuumComputeTarget(Oid shard, TransactionId from, int64 safe_ts,
						 TransactionId ceiling, const char **stop_reason)
{
	TransactionId xid;
	TransactionId last_ok = InvalidTransactionId;

	if (stop_reason != NULL)
		*stop_reason = "scanned-to-ceiling";

	/*
	 * safe_ts <= 0：取不到 GlobalSafeTs（未配置 TSO / RPC 失败）。**什么都不清**
	 * —— 见 TsoGetGlobalSafeTs 的注释，偏小安全、取不到就别动。
	 */
	if (safe_ts <= 0)
	{
		if (stop_reason != NULL)
			*stop_reason = "no-safe-ts";
		return InvalidTransactionId;
	}

	if (!TransactionIdIsValid(from) || from < FIRST_SHARD_XID)
		from = FIRST_SHARD_XID;

	for (xid = from; xid < ceiling; xid++)
	{
		ShardClogSlot slot;

		if (!ShardClogReadSlot(shard, xid, &slot))
		{
			/*
			 * 读不出槽 = 稀疏空洞 = 全零 = TXN_RUNNING（见 shard_clog.h 头注释）。
			 * 未决即停 —— 空洞不代表"没有这个事务"，只代表判决没写下来。
			 */
			if (stop_reason != NULL)
				*stop_reason = "hole-running";
			break;
		}

		if (slot.status == TXN_ABORTED)
		{
			last_ok = xid;		/* ★ ABORTED 放行（见上方说明） */
			continue;
		}
		if (slot.status == TXN_COMMITTED)
		{
			if ((int64) slot.commit_ts > 0 && (int64) slot.commit_ts < safe_ts)
			{
				last_ok = xid;
				continue;
			}
			if (stop_reason != NULL)
				*stop_reason = "commit-ts-too-new";
			break;
		}
		/* RUNNING / PREPARED */
		if (stop_reason != NULL)
			*stop_reason = (slot.status == TXN_PREPARED) ? "prepared" : "running";
		break;
	}

	return last_ok;
}

/* ---- T5.2 SQL 包装：验收观测点 ---- */
PG_FUNCTION_INFO_V1(partdist_shard_vacuum_target);
Datum
partdist_shard_vacuum_target(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	int64		safe_ts = PG_GETARG_INT64(1);
	TransactionId ceiling = (TransactionId) PG_GETARG_INT64(2);
	TransactionId tb,
				vx,
				target;
	const char *reason = NULL;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	/* 起点 = 当前截断点（设计 §6.3：从 clog_truncate_before 起顺扫） */
	ShardVacuumGetWatermarks(shard, &tb, &vx);
	target = ShardVacuumComputeTarget(shard, tb, safe_ts, ceiling, &reason);

	values[0] = Int64GetDatum((int64) target);
	values[1] = CStringGetTextDatum(reason ? reason : "");
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/* ---- T5.2 验收辅助：带 commit_ts 的落账（sclog_write 只写 status，ts 恒 0）---- */
PG_FUNCTION_INFO_V1(partdist_shard_clog_write_ts);
Datum
partdist_shard_clog_write_ts(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = (TransactionId) PG_GETARG_INT64(1);
	int32		status = PG_GETARG_INT32(2);
	int64		cts = PG_GETARG_INT64(3);

	switch ((TxnStatus) status)
	{
		case TXN_RUNNING:
			ShardClogSetRunning(shard, sxid, cts);
			break;
		case TXN_PREPARED:
			ShardClogSetPrepared(shard, sxid, cts, 0);
			break;
		case TXN_COMMITTED:
			ShardClogSetVerdict(shard, sxid, true, cts);
			break;
		case TXN_ABORTED:
			ShardClogSetVerdict(shard, sxid, false, cts);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("status 只接受 0/1/2/3，收到 %d", status)));
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(partdist_shard_clog_truncate);
Datum
partdist_shard_clog_truncate(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId trunc_before = (TransactionId) PG_GETARG_INT64(1);

	PG_RETURN_INT32(ShardClogTruncate(shard, trunc_before));
}

PG_FUNCTION_INFO_V1(partdist_shard_claim_on_promote);

/*
 * shard_claim_on_promote(shard oid) → int
 *
 * T6.4 / §6.6 第三支的 SQL 入口。升主路径（pg_raft_promote_prepare）在追平并
 * 闭合 in-doubt 之后调用；验收也用它单独驱动认领。返回改判条数。
 */
Datum
partdist_shard_claim_on_promote(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);

	PG_RETURN_INT32((int32) ShardXidClaimOnPromote(shard));
}

PG_FUNCTION_INFO_V1(partdist_shard_clog_set_prepared);

/*
 * 验收探针：直接把某分片 xid 写成 TXN_PREPARED。
 *
 * 不去放宽 partdist_shard_clog_write() 的 status 白名单 —— 那道"PREPARED 是
 * P4 的事"的守卫是故意的，为了一条测试把它松掉，等于用验收去磨产品的棱角。
 * T6.4 需要 PREPARED 只是为了做**阴性对照**（认领必须不动它，§6.6 第二支），
 * 单开一个探针最小且不影响任何产品路径。
 */
Datum
partdist_shard_clog_set_prepared(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = (TransactionId) PG_GETARG_INT64(1);

	ShardClogSetPrepared(shard, sxid, 1, 1);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(partdist_shard_xid_raise_watermark);

/*
 * 验收探针：直接抬高本分片的发号水位（等价于回放消费一条更高水位的 MARKER）。
 *
 * 为什么用探针而不是"真烧一整批号"：水位按 SHARD_XID_BATCH(4096) 成批推进，
 * 挂槽那一刻 claim_wm 与 watermark 相等，缺口要等 leader 烧掉**整整 4096 笔
 * 事务**才张开。实测在本环境跑 4200 笔自动提交事务要 31 秒（还只是普通表，
 * 分片表叠上 raft 复制到分钟级），而这段代价换不来任何新覆盖 ——
 * "真 MARKER 路径确实会抬水位"已由 T6.5 的 test_xid_watermark_p6.sh 证过
 * （17/0，含"水位确实在动"的正向断言）。本探针只把同一份状态确定性地摆好，
 * 让 T6.4 能专心验它自己的那件事：给定缺口，认领改判谁、不动谁。
 */
Datum
partdist_shard_xid_raise_watermark(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId wm = (TransactionId) PG_GETARG_INT64(1);

	ShardXidRaiseAllocWatermark(shard, wm);
	PG_RETURN_VOID();
}
