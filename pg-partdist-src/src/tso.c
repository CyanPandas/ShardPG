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
	bool		boot_blocked;	/* 启动时检测到 boot 标记、又没有发号上限文件 ⇒ 拒发号 */
	int64		reserved;		/* P7-N101：已落盘的发号上限，发出的号都 < 它 */
	TimestampTz safe_freeze_until;	/* P7-N101：重启后一个租约周期内 GlobalSafeTs 不前进 */
	TsoNodeEntry nodes[TSO_MAX_NODES];
} TsoState;

static TsoState *TsoCtl = NULL;

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

/*
 * ★ P7-N101：发号上限落盘（高水位）。
 *
 * 原设计（§2.4 v1 裁定）TSO 只是内存计数器，master 一重启就"单调性失守"，只能整簇重建；
 * boot 标记把它变成"重启后拒发号"。实际后果是**有数据时 master 不能重启**：删标记重启
 * 就从 1 重新发号，旧提交全部不可见；不删就一直拒发号。部署、加节点都卡在这上面。
 *
 * 做法与 gxid 水位（gxid.c）同构：发号前保证 counter < reserved，reserved 先落盘
 * （写临时文件 → fsync → durable_rename）再发号；每次预留 TSO_RESERVE_STEP 个号。
 * 重启时 counter 从文件里的 reserved 接着发 —— 跳过一段号无害（时间戳只要求单调），
 * 绝不会重发。文件里同时存当时的 GlobalSafeTs（一个下界，重启后从它起步）。
 *
 * GlobalSafeTs 重启后的空窗：各节点登记的最老活跃快照只在内存里，重启后清空；若马上按
 * "无人登记 ⇒ safe = counter"算，会越过还活着的远端快照、让 vacuum 清掉它要读的版本。
 * 所以重启后冻结一个租约周期（tso_lease_ms）不前进 —— 期间各节点心跳会重新登记；
 * 租约到期还没心跳的节点，其快照本就被栅栏作废（§6.2），与没重启时同一套规则。
 */
#define TSO_RESERVE_STEP	100000

static bool
tso_read_hwm(int64 *reserved, int64 *safe)
{
	char		path[MAXPGPATH];
	char		buf[128];
	int			fd;
	int			n;
	long long	r = 0,
				sf = 0;

	snprintf(path, sizeof(path), "%s/%s", DataDir, TSO_HWM_FILE);
	fd = open(path, O_RDONLY | PG_BINARY, 0);
	if (fd < 0)
		return false;
	n = read(fd, buf, sizeof(buf) - 1);
	close(fd);
	if (n <= 0)
		return false;
	buf[n] = '\0';
	if (sscanf(buf, "%lld %lld", &r, &sf) < 1 || r <= 0)
		return false;
	*reserved = (int64) r;
	*safe = (sf > 0 && sf <= r) ? (int64) sf : 0;
	return true;
}

/* 先落盘后发号（fail-closed）：写不下去就 ERROR，这个号不发 */
static void
tso_persist_hwm(int64 reserved, int64 safe)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	char		buf[128];
	int			fd;
	int			len;

	snprintf(tmppath, sizeof(tmppath), "%s/%s.tmp", DataDir, TSO_HWM_FILE);
	snprintf(path, sizeof(path), "%s/%s", DataDir, TSO_HWM_FILE);
	len = snprintf(buf, sizeof(buf), INT64_FORMAT " " INT64_FORMAT "\n", reserved, safe);

	fd = OpenTransientFile(tmppath, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建 TSO 发号上限文件 \"%s\": %m", tmppath)));
	errno = 0;
	if (write(fd, buf, len) != len)
	{
		if (errno == 0)
			errno = ENOSPC;
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法写 TSO 发号上限文件 \"%s\": %m", tmppath)));
	}
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: TSO 发号上限文件 fsync 失败: %m")));
	}
	CloseTransientFile(fd);
	durable_rename(tmppath, path, ERROR);
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

		int64		hwm = 0;
		int64		hwm_safe = 0;

		TsoCtl->counter = 1;
		TsoCtl->safe_ts = 0;
		TsoCtl->served = false;
		TsoCtl->reserved = 0;
		TsoCtl->safe_freeze_until = 0;
		for (i = 0; i < TSO_MAX_NODES; i++)
		{
			TsoCtl->nodes[i].node_id = -1;
			TsoCtl->nodes[i].oldest_ts = 0;
			TsoCtl->nodes[i].lease_deadline = 0;
		}
		TsoCtl->lock = &GetNamedLWLockTranche("pg_partdist_tso")[0].lock;

		/*
		 * P7-N101：有发号上限文件 ⇒ 从上限接着发（上次发出的号都比它小），boot 标记不再拦。
		 * 没有上限文件、却有 boot 标记 ⇒ 上个纪元是老版本发的号（不知道发到了哪），
		 * 照旧拒发号（§2.4 配套 2）；要接续，先手工写上限文件（见 tso_status 的提示）。
		 */
		snprintf(path, sizeof(path), "%s/%s", DataDir, TSO_BOOT_MARKER);
		if (tso_read_hwm(&hwm, &hwm_safe))
		{
			TsoCtl->counter = hwm;
			TsoCtl->reserved = hwm;		/* 第一个号发出前先把上限再推一段并落盘 */
			TsoCtl->safe_ts = hwm_safe;
			TsoCtl->safe_freeze_until =
				TimestampTzPlusMilliseconds(GetCurrentTimestamp(), tso_lease_ms);
			TsoCtl->boot_blocked = false;
		}
		else
			TsoCtl->boot_blocked = (stat(path, &st) == 0);
	}
	LWLockRelease(AddinShmemInitLock);
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
				 errhint("整簇重建后删除 $PGDATA/%s 再重启本节点；要保留数据接着发号，"
						 "把一个大于上次发出的最大号的数写进 $PGDATA/%s 再重启（P7-N101）。",
						 TSO_BOOT_MARKER, TSO_HWM_FILE)));
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
	/* P7-N101：号用到上限就先把上限推一段、落盘，再发 */
	if (TsoCtl->counter >= TsoCtl->reserved)
	{
		int64		new_res = TsoCtl->counter + TSO_RESERVE_STEP;

		tso_persist_hwm(new_res, TsoCtl->safe_ts);
		TsoCtl->reserved = new_res;
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

	/* P7-N101：重启后一个租约周期内不前进（各节点的最老快照登记还没重新报上来） */
	if (TsoCtl->safe_freeze_until != 0 && now < TsoCtl->safe_freeze_until)
		return TsoCtl->safe_ts;

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
	appendStringInfo(&buf, "counter=" INT64_FORMAT " safe=" INT64_FORMAT
					 " served=%c blocked=%c reserved=" INT64_FORMAT,
					 TsoCtl->counter, TsoCtl->safe_ts,
					 TsoCtl->served ? 't' : 'f',
					 TsoCtl->boot_blocked ? 't' : 'f',
					 TsoCtl->reserved);
	for (i = 0; i < TSO_MAX_NODES; i++)
		if (TsoCtl->nodes[i].node_id != -1)
			appendStringInfo(&buf, " node%d=" INT64_FORMAT,
							 TsoCtl->nodes[i].node_id,
							 TsoCtl->nodes[i].oldest_ts);
	LWLockRelease(TsoCtl->lock);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
