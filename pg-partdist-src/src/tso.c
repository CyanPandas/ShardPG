/*
 * tso.c — TSO 服务实现（P3 T3.1）。语义与形态见 tso.h 头注释。
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "tso.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/* ---- GUC ---- */

static bool tso_master = false; /* 只有 master 节点置 on，其余拒服务 */
static int	tso_lease_ms = 10000;	/* 登记租约时长（T3.5 起消费） */

/* ---- 共享内存 ---- */

typedef struct TsoNodeEntry
{
	int32		node_id;		/* -1 = 空槽 */
	int64		oldest_ts;		/* 该节点最老活跃快照 start_ts；0 = "无" */
	TimestampTz lease_deadline; /* 登记的租约期限（T3.5 消费） */
} TsoNodeEntry;

typedef struct TsoState
{
	LWLock	   *lock;
	int64		counter;		/* 下一个待发；从 1 起，0 保留为"无 ts" */
	int64		safe_ts;		/* T3.5 GlobalSafeTs：单调不减（§6.2） */
	bool		served;			/* 本次启动是否已服务过（boot 标记已落盘） */
	bool		boot_blocked;	/* 启动时检测到 boot 标记且无高水位文件 ⇒ 拒发号 */
	int64		hwm;			/* 已持久预留的上界（发出的号都 < hwm）；0 = 尚未预留 */
	TimestampTz boot_time;		/* 本次启动时刻：GlobalSafeTs 在一个租约期内不前进 */
	TsoNodeEntry nodes[TSO_MAX_NODES];
} TsoState;

static TsoState *TsoCtl = NULL;

static int64 tso_read_hwm(void);

void
TsoDefineGUCs(void)
{
	DefineCustomBoolVariable(
		"pg_partdist.tso_master",
		"本节点是否为 TSO master（v1 唯一发号点，§2.4）。",
		"off 的节点收到 TSO 请求一律 ERROR——防误连错节点开出第二个时间戳纪元。",
		&tso_master,
		false,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	DefineCustomIntVariable(
		"pg_partdist.tso_lease_ms",
		"GlobalSafeTs 节点登记的租约时长（毫秒，§6.2）。",
		NULL,
		&tso_lease_ms,
		10000,
		1000,
		3600000,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);
}

void
RequestTsoShmem(void)
{
	RequestAddinShmemSpace(MAXALIGN(sizeof(TsoState)));
	RequestNamedLWLockTranche("pg_partdist_tso", 1);
}

void
TsoShmemInit(void)
{
	bool		found;

	TsoCtl = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	TsoCtl = ShmemInitStruct("pg_partdist_tso", sizeof(TsoState), &found);
	if (!found)
	{
		int			i;
		struct stat st;
		char		path[MAXPGPATH];

		TsoCtl->counter = 1;
		TsoCtl->safe_ts = 0;
		TsoCtl->served = false;
		TsoCtl->hwm = 0;
		TsoCtl->boot_time = GetCurrentTimestamp();
		for (i = 0; i < TSO_MAX_NODES; i++)
		{
			TsoCtl->nodes[i].node_id = -1;
			TsoCtl->nodes[i].oldest_ts = 0;
			TsoCtl->nodes[i].lease_deadline = 0;
		}
		TsoCtl->lock = &GetNamedLWLockTranche("pg_partdist_tso")[0].lock;

		/*
		 * boot 防呆（§2.4 配套 2）：标记在 = 上个纪元服务过 = 单调性公理
		 * 已不可信。postmaster 启动期单线程读一次即可。
		 */
		snprintf(path, sizeof(path), "%s/%s", DataDir, TSO_BOOT_MARKER);
		TsoCtl->boot_blocked = (stat(path, &st) == 0);

		/*
		 * 2026-10-16：有高水位文件 ⇒ 上个纪元发出的号全都小于它，从这里接着发，
		 * 单调性成立，不再拦。读不出合法值则维持 boot 标记的判断（fail-closed）。
		 */
		{
			int64		h = tso_read_hwm();

			if (h > 0)
			{
				TsoCtl->counter = h;
				TsoCtl->hwm = h;	/* 本纪元的第一个号就要求再预留一批 */
				TsoCtl->boot_blocked = false;
			}
		}
	}
	LWLockRelease(AddinShmemInitLock);
}

/* 读高水位文件；不存在或内容非法返回 0 */
static int64
tso_read_hwm(void)
{
	char		path[MAXPGPATH];
	char		buf[64];
	FILE	   *fp;
	long long	v = 0;

	snprintf(path, sizeof(path), "%s/%s", DataDir, TSO_HWM_FILE);
	fp = fopen(path, "r");		/* postmaster 启动期也会调，不走 fd.c 记账 */
	if (fp == NULL)
		return 0;
	if (fgets(buf, sizeof(buf), fp) == NULL || sscanf(buf, "%lld", &v) != 1 || v <= 0)
		v = 0;
	fclose(fp);
	return (int64) v;
}

/*
 * 持久预留新上界：临时文件写入 + fsync，durable_rename 换上（目录也 fsync）。
 * 须持 TSO 排它锁；失败 ERROR（宁可不发号也不越过未持久的上界）。
 */
static void
tso_persist_hwm(int64 hwm)
{
	char		path[MAXPGPATH];
	char		tmp[MAXPGPATH];
	char		buf[32];
	int			fd;
	int			len;

	snprintf(path, sizeof(path), "%s/%s", DataDir, TSO_HWM_FILE);
	snprintf(tmp, sizeof(tmp), "%s/%s.tmp", DataDir, TSO_HWM_FILE);
	len = snprintf(buf, sizeof(buf), INT64_FORMAT "\n", hwm);
	fd = OpenTransientFile(tmp, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建 TSO 高水位文件 \"%s\": %m", tmp)));
	if (write(fd, buf, len) != len || pg_fsync(fd) != 0)
	{
		int			save_errno = errno;

		CloseTransientFile(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: TSO 高水位文件写入失败: %m")));
	}
	CloseTransientFile(fd);
	(void) durable_rename(tmp, path, ERROR);
}

/*
 * 首次服务前把 boot 标记 durably 落盘（文件 + 目录都 fsync）。
 * 顺序铁律：标记持久 **先于** 第一个号发出——崩在两者之间只会多一次
 * "误拦重启"（删标记即恢复），绝不会出现"发过号却检测不到"。
 */
static void
tso_write_boot_marker(void)
{
	char		path[MAXPGPATH];
	int			fd;

	snprintf(path, sizeof(path), "%s/%s", DataDir, TSO_BOOT_MARKER);
	fd = OpenTransientFile(path, O_CREAT | O_WRONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建 TSO boot 标记 \"%s\": %m", path)));
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: TSO boot 标记 fsync 失败: %m")));
	}
	CloseTransientFile(fd);

	fd = OpenTransientFile(DataDir, O_RDONLY | PG_BINARY);
	if (fd >= 0)
	{
		(void) pg_fsync(fd);
		CloseTransientFile(fd);
	}
}

/* 服务门卫：master 身份 + boot 防呆 + shmem 就绪。须持锁前调用。 */
static void
tso_service_gate(void)
{
	if (TsoCtl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist: TSO 共享内存未初始化")));

	if (!tso_master)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("本节点不是 TSO master（pg_partdist.tso_master=off）"),
				 errhint("v1 只有 master 发号；请连接 master 节点。")));

	if (TsoCtl->boot_blocked)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("TSO 检测到上个纪元的 boot 标记（%s），拒绝发号", TSO_BOOT_MARKER),
				 errdetail("master/TSO 重启后单调性公理失守，时间戳序不再可信"
						   "（设计 §2.4 运行纪律）。"),
				 errhint("整簇重建后删除 $PGDATA/%s 再重启本节点。", TSO_BOOT_MARKER)));
}

/* 发一个号（须持排它锁；首次服务顺带落 boot 标记） */
static int64
tso_next_locked(void)
{
	if (!TsoCtl->served)
	{
		tso_write_boot_marker();
		TsoCtl->served = true;
	}
	/* 2026-10-16：要发的号够到已持久的上界 ⇒ 先持久预留下一批（约每百万个号一次） */
	if (TsoCtl->counter >= TsoCtl->hwm)
	{
		int64		next = TsoCtl->counter + TSO_HWM_BATCH;

		tso_persist_hwm(next);
		TsoCtl->hwm = next;
	}
	return TsoCtl->counter++;
}

/* ================= SQL 入口 ================= */

/*
 * partdist_tso_start_ts(node, oldest) — 取 start_ts，发号即登记（§2.3）：
 * 同一临界区内先把该节点登记更新为 min(携带值, 新号)，再返回新号。
 * oldest = 0 表示"该节点当前无活跃快照"⇒ 新号即成为其最老活跃。
 */
PG_FUNCTION_INFO_V1(partdist_tso_start_ts);
Datum
partdist_tso_start_ts(PG_FUNCTION_ARGS)
{
	int32		node = PG_GETARG_INT32(0);
	int64		oldest = PG_GETARG_INT64(1);
	int64		ts;
	int			i;
	int			free_i = -1;
	TsoNodeEntry *e = NULL;

	tso_service_gate();
	if (node < 0 || oldest < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node 须 >=0、oldest 须 >=0（0=无活跃快照）")));

	LWLockAcquire(TsoCtl->lock, LW_EXCLUSIVE);
	ts = tso_next_locked();

	for (i = 0; i < TSO_MAX_NODES; i++)
	{
		if (TsoCtl->nodes[i].node_id == node)
		{
			e = &TsoCtl->nodes[i];
			break;
		}
		if (free_i < 0 && TsoCtl->nodes[i].node_id == -1)
			free_i = i;
	}
	if (e == NULL)
	{
		if (free_i < 0)
		{
			LWLockRelease(TsoCtl->lock);
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("TSO 节点登记表已满（上限 %d）", TSO_MAX_NODES)));
		}
		e = &TsoCtl->nodes[free_i];
		e->node_id = node;
	}
	e->oldest_ts = (oldest > 0 && oldest < ts) ? oldest : ts;
	e->lease_deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
													tso_lease_ms);
	LWLockRelease(TsoCtl->lock);

	PG_RETURN_INT64(ts);
}

/* partdist_tso_commit_ts() — 取 commit_ts（非快照，不登记） */
PG_FUNCTION_INFO_V1(partdist_tso_commit_ts);
Datum
partdist_tso_commit_ts(PG_FUNCTION_ARGS)
{
	int64		ts;

	tso_service_gate();

	LWLockAcquire(TsoCtl->lock, LW_EXCLUSIVE);
	ts = tso_next_locked();
	LWLockRelease(TsoCtl->lock);

	PG_RETURN_INT64(ts);
}

/*
 * T3.5：租约过期清扫 + GlobalSafeTs 计算（须持排它锁）。
 * 候选 = min(租约内且有活跃的节点 oldest)；全无活跃 ⇒ counter（已发尽安全）。
 * 单调不减：候选 < 存量属不变式破坏（登记/栅栏漏了），WARNING 并保存量。
 */
static int64
tso_compute_safe_locked(void)
{
	TimestampTz now = GetCurrentTimestamp();
	int64		cand = 0;
	int			i;

	for (i = 0; i < TSO_MAX_NODES; i++)
	{
		TsoNodeEntry *e = &TsoCtl->nodes[i];

		if (e->node_id == -1)
			continue;
		if (e->lease_deadline < now)
		{
			e->oldest_ts = 0;	/* 护栏一：租约到期登记清为"无" */
			continue;
		}
		if (e->oldest_ts > 0 && (cand == 0 || e->oldest_ts < cand))
			cand = e->oldest_ts;
	}
	if (cand == 0)
		cand = TsoCtl->counter;

	/*
	 * 2026-10-16：重启后一个租约期内不前进。master 能重启了，而 worker 上跨重启
	 * 存活的快照要等下一次心跳才重新登记；在那之前"全无活跃 ⇒ counter"会把
	 * 安全线一下推到新纪元的起点，vacuum 就可能越过这些快照。
	 */
	if (GetCurrentTimestamp() <
		TimestampTzPlusMilliseconds(TsoCtl->boot_time, tso_lease_ms))
		return TsoCtl->safe_ts;

	if (cand >= TsoCtl->safe_ts)
		TsoCtl->safe_ts = cand;
	else
		elog(WARNING,
			 "pg_partdist: GlobalSafeTs 候选 " INT64_FORMAT
			 " 低于存量 " INT64_FORMAT "（不变式破坏？），保持存量",
			 cand, TsoCtl->safe_ts);
	return TsoCtl->safe_ts;
}

/*
 * partdist_tso_heartbeat(node, oldest) — 双通道之二：周期心跳（§6.2）。
 * 无事务也报"最老或无（0）"，即租约续期。返回 lease 毫秒数——worker 侧
 * 栅栏据此计算作废时限（先于 master 剔除生效）。
 */
PG_FUNCTION_INFO_V1(partdist_tso_heartbeat);
Datum
partdist_tso_heartbeat(PG_FUNCTION_ARGS)
{
	int32		node = PG_GETARG_INT32(0);
	int64		oldest = PG_GETARG_INT64(1);
	int			i;
	int			free_i = -1;
	TsoNodeEntry *e = NULL;

	tso_service_gate();
	if (node < 0 || oldest < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("node 须 >=0、oldest 须 >=0（0=无活跃快照）")));

	LWLockAcquire(TsoCtl->lock, LW_EXCLUSIVE);
	for (i = 0; i < TSO_MAX_NODES; i++)
	{
		if (TsoCtl->nodes[i].node_id == node)
		{
			e = &TsoCtl->nodes[i];
			break;
		}
		if (free_i < 0 && TsoCtl->nodes[i].node_id == -1)
			free_i = i;
	}
	if (e == NULL && free_i >= 0)
	{
		e = &TsoCtl->nodes[free_i];
		e->node_id = node;
	}
	if (e != NULL)
	{
		e->oldest_ts = oldest;
		e->lease_deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
														tso_lease_ms);
	}
	LWLockRelease(TsoCtl->lock);

	PG_RETURN_INT64((int64) tso_lease_ms);
}

/* partdist_global_safe_ts() — 读出（P5 vacuum 的地基 + 验收观测点） */
PG_FUNCTION_INFO_V1(partdist_global_safe_ts);
Datum
partdist_global_safe_ts(PG_FUNCTION_ARGS)
{
	int64		safe;

	if (TsoCtl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist: TSO 共享内存未初始化")));
	if (!tso_master)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("本节点不是 TSO master（pg_partdist.tso_master=off）")));

	LWLockAcquire(TsoCtl->lock, LW_EXCLUSIVE);
	safe = tso_compute_safe_locked();
	LWLockRelease(TsoCtl->lock);
	PG_RETURN_INT64(safe);
}

/* partdist_tso_status() — 观测/验收（不发号、不落标记；master 之外也可看） */
PG_FUNCTION_INFO_V1(partdist_tso_status);
Datum
partdist_tso_status(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	int			i;

	if (TsoCtl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist: TSO 共享内存未初始化")));

	initStringInfo(&buf);
	LWLockAcquire(TsoCtl->lock, LW_SHARED);
	appendStringInfo(&buf, "counter=" INT64_FORMAT " hwm=" INT64_FORMAT
					 " safe=" INT64_FORMAT " served=%c blocked=%c",
					 TsoCtl->counter, TsoCtl->hwm, TsoCtl->safe_ts,
					 TsoCtl->served ? 't' : 'f',
					 TsoCtl->boot_blocked ? 't' : 'f');
	for (i = 0; i < TSO_MAX_NODES; i++)
		if (TsoCtl->nodes[i].node_id != -1)
			appendStringInfo(&buf, " node%d=" INT64_FORMAT,
							 TsoCtl->nodes[i].node_id,
							 TsoCtl->nodes[i].oldest_ts);
	LWLockRelease(TsoCtl->lock);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
