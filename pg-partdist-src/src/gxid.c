/*
 * gxid.c — globalXID 分配器实现（P4 T4.1）。语义见 gxid.h。
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "gxid.h"
#include "global_mvcc.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

typedef struct GxidState
{
	LWLock	   *lock;
	uint64		next_seq;		/* 下一个待发序号；0 保留不发 */
	uint64		watermark;		/* 已持久化上界：所有已发序号 < watermark */
} GxidState;

static GxidState *GxidCtl = NULL;

void
RequestGxidShmem(void)
{
	RequestAddinShmemSpace(MAXALIGN(sizeof(GxidState)));
	RequestNamedLWLockTranche("pg_partdist_gxid", 1);
}

void
GxidShmemInit(void)
{
	bool		found;

	GxidCtl = NULL;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	GxidCtl = ShmemInitStruct("pg_partdist_gxid", sizeof(GxidState), &found);
	if (!found)
	{
		GxidCtl->next_seq = 0;	/* 0 = 未初始化：首用时从水位文件装载 */
		GxidCtl->watermark = 0;
		GxidCtl->lock = &GetNamedLWLockTranche("pg_partdist_gxid")[0].lock;
	}
	LWLockRelease(AddinShmemInitLock);
}

static uint64
gxid_read_wm(void)
{
	char		path[MAXPGPATH];
	int			fd;
	uint64		wm;
	int			r;

	snprintf(path, sizeof(path), "%s/%s", DataDir, GXID_WM_FILE);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return 0;			/* 本节点首次发号 */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法打开 gxid 水位文件 \"%s\": %m", path)));
	}
	r = read(fd, &wm, sizeof(wm));
	if (r != (int) sizeof(wm))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("gxid 水位文件 \"%s\" 损坏（读到 %d 字节）", path, r)));
	CloseTransientFile(fd);
	return wm;
}

static void
gxid_persist_wm(uint64 wm)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	int			fd;

	snprintf(tmppath, sizeof(tmppath), "%s/%s.tmp", DataDir, GXID_WM_FILE);
	snprintf(path, sizeof(path), "%s/%s", DataDir, GXID_WM_FILE);

	fd = OpenTransientFile(tmppath, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法创建 gxid 水位文件 \"%s\": %m", tmppath)));
	errno = 0;
	if (write(fd, &wm, sizeof(wm)) != (int) sizeof(wm))
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法写 gxid 水位文件 \"%s\": %m", tmppath)));
	}
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法 fsync gxid 水位文件 \"%s\": %m", tmppath)));
	CloseTransientFile(fd);
	durable_rename(tmppath, path, ERROR);
}

int64
GxidAllocate(void)
{
	uint64		seq;
	uint16		node;

	if (GxidCtl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist gxid 共享内存未初始化")));

	LWLockAcquire(GxidCtl->lock, LW_EXCLUSIVE);

	if (GxidCtl->next_seq == 0)
	{
		/* 首用装载：从水位续发（跳号无害，绝不重发） */
		GxidCtl->next_seq = Max(gxid_read_wm(), 1);
		GxidCtl->watermark = GxidCtl->next_seq;
	}

	if (GxidCtl->next_seq >= GxidCtl->watermark)
	{
		uint64		new_wm = GxidCtl->next_seq + GXID_BATCH;

		gxid_persist_wm(new_wm);	/* 先落盘后发号（fail-closed） */
		GxidCtl->watermark = new_wm;
	}

	seq = GxidCtl->next_seq++;
	LWLockRelease(GxidCtl->lock);

	if (seq > GXID_SEQ_MASK)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("gxid 48 位序号耗尽（seq=" UINT64_FORMAT "）", seq)));

	node = PartDistLocalNodeId();
	return (int64) (((uint64) node << 48) | seq);
}

/* ================= SQL 入口（协调者驱动/验收用） ================= */

PG_FUNCTION_INFO_V1(partdist_gxid_next);
Datum
partdist_gxid_next(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(GxidAllocate());
}
