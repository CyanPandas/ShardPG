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
#include "shard_xid.h"

#include "access/transam.h"
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
	}
	pending_drops_n = 0;
}

void
ShardClogAtAbort(void)
{
	pending_drops_n = 0;
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
