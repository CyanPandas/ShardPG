/*
 * tso_client.c — worker 侧 TSO 取号通路（P3 T3.2，P3_PRECHECK 结论二/四）。
 *
 * 形态：
 *   - 每后端缓存一条到 master 的 libpq 连接（会话生存期）；RPC 失败重连
 *     重试一次，仍失败即 ERROR——**绝不本地时钟顶替**（§2.2 纪律 2）。
 *   - start_ts **懒取**（首次调用即本事务分片快照点，P3_PRECHECK 结论四）：
 *     一事务一取，后端事务态缓存，XactCallback 清理。
 *   - commit_ts 在 XACT_EVENT_PRE_COMMIT（临界区外、提交记录前 = 合法窗口
 *     内尽晚）取号暂存；0007 收集钩子在临界区内只拷暂存（T3.3 消费）。
 *   - **发号即登记的携带值**：本节点活跃快照集合放共享内存（每后端一槽），
 *     取号前算 min(其余活跃) 作 oldest 随行上报（0=无）；事务结束清槽。
 *     master 侧登记滞后只会让 GlobalSafeTs 偏小 = 安全方向（§6.2）。
 *   - **遗留模式**：GUC pg_partdist.tso_conninfo 为空 ⇒ 不 RPC、start/commit
 *     一律返回 0（"无 ts"）——P1/P2 语义原样保留，596 基线零扰动；strict
 *     模式收紧（T3.6）后无 ts 访问被安全网拦截。fail-closed 只对"已配置但
 *     不可达"生效。
 */
#include "postgres.h"

#include "tso.h"
#include "global_mvcc.h"
#include "shard_xid.h"

#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/wait_event.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

/* ---- GUC ---- */

static char *tso_conninfo = NULL;	/* 空 = 遗留模式（不取号，ts=0） */

/* ---- 本节点活跃快照集合（共享内存，每后端一槽） ---- */

#define TSO_CLIENT_MAX_ACTIVE	256

typedef struct TsoActiveEntry
{
	int32		pid;			/* 0 = 空槽 */
	int64		start_ts;
} TsoActiveEntry;

typedef struct TsoClientState
{
	LWLock	   *lock;
	int32		node_id;		/* 本节点号缓存：首个取号后端写入（-1=未知）。
								 * 心跳 bgworker 无数据库连接，不能做 Citus
								 * catalog 查询（实测 SIGSEGV 循环拖垮节点），
								 * 只读这里；未知则跳过心跳——没人取过号就
								 * 没有快照需要续租保护。 */
	TimestampTz last_beat_ok;	/* T3.5 栅栏：最近一次租约续期成功时刻
								 * （start_ts RPC 或心跳，二者都在 master
								 * 侧续租；commit_ts 不续租不算数） */
	int32		lease_ms;		/* master 返回的租约时长；0=未知 */
	TsoActiveEntry active[TSO_CLIENT_MAX_ACTIVE];
} TsoClientState;

static TsoClientState *TsoClientCtl = NULL;

/* ---- 后端状态 ---- */

static PGconn *tso_conn = NULL;
static int64 cur_start_ts = 0;	/* 本事务分片快照；0 = 未取 */
static int64 cur_commit_ts = 0; /* PRE_COMMIT 暂存；0 = 未取 */
static char tso_last_err[256];	/* 最近一次失败原因（连接被弃后仍可报） */

void
TsoClientDefineGUCs(void)
{
	DefineCustomStringVariable(
		"pg_partdist.tso_conninfo",
		"到 TSO master 的 libpq 连接串（T3.2）。",
		"空 = 遗留模式：不取号、ts 一律 0（P1/P2 语义）；配置后 TSO 不可达即"
		" ERROR（fail-closed，绝不本地时钟顶替）。",
		&tso_conninfo,
		"",
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);
}

void
RequestTsoClientShmem(void)
{
	RequestAddinShmemSpace(MAXALIGN(sizeof(TsoClientState)));
	RequestNamedLWLockTranche("pg_partdist_tso_client", 1);
}

void
TsoClientShmemInit(void)
{
	bool		found;

	TsoClientCtl = NULL;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	TsoClientCtl = ShmemInitStruct("pg_partdist_tso_client",
								   sizeof(TsoClientState), &found);
	if (!found)
	{
		memset(TsoClientCtl->active, 0, sizeof(TsoClientCtl->active));
		TsoClientCtl->node_id = -1;
		TsoClientCtl->last_beat_ok = 0;
		TsoClientCtl->lease_ms = 0;
		TsoClientCtl->lock =
			&GetNamedLWLockTranche("pg_partdist_tso_client")[0].lock;
	}
	LWLockRelease(AddinShmemInitLock);
}

static bool
tso_configured(void)
{
	return tso_conninfo != NULL && tso_conninfo[0] != '\0';
}

bool
TsoConfigured(void)
{
	return tso_configured();
}

/* ---- 活跃集合维护 ---- */

static int64
tso_active_min_locked(void)
{
	int64		m = 0;
	int			i;

	for (i = 0; i < TSO_CLIENT_MAX_ACTIVE; i++)
		if (TsoClientCtl->active[i].pid != 0 &&
			(m == 0 || TsoClientCtl->active[i].start_ts < m))
			m = TsoClientCtl->active[i].start_ts;
	return m;
}

static void
tso_active_register(int64 ts)
{
	int			i;
	int			free_i = -1;

	LWLockAcquire(TsoClientCtl->lock, LW_EXCLUSIVE);
	for (i = 0; i < TSO_CLIENT_MAX_ACTIVE; i++)
	{
		if (TsoClientCtl->active[i].pid == MyProcPid)
		{
			TsoClientCtl->active[i].start_ts = ts;
			LWLockRelease(TsoClientCtl->lock);
			return;
		}
		if (free_i < 0 && TsoClientCtl->active[i].pid == 0)
			free_i = i;
	}
	if (free_i >= 0)
	{
		TsoClientCtl->active[free_i].start_ts = ts;
		TsoClientCtl->active[free_i].pid = MyProcPid;
	}
	/* 槽满：不登记，只影响 GlobalSafeTs 精度（偏小=安全方向），不拦事务 */
	LWLockRelease(TsoClientCtl->lock);
}

void
TsoClientClearActive(void)
{
	int			i;

	cur_start_ts = 0;
	cur_commit_ts = 0;

	if (TsoClientCtl == NULL)
		return;
	LWLockAcquire(TsoClientCtl->lock, LW_EXCLUSIVE);
	for (i = 0; i < TSO_CLIENT_MAX_ACTIVE; i++)
		if (TsoClientCtl->active[i].pid == MyProcPid)
		{
			TsoClientCtl->active[i].pid = 0;
			TsoClientCtl->active[i].start_ts = 0;
			break;
		}
	LWLockRelease(TsoClientCtl->lock);
}

/* T3.5：续租成功登记（start_ts / 心跳成功后调；lease_ms<=0 只记时刻） */
static void
tso_note_lease_renewal(int32 lease_ms)
{
	if (TsoClientCtl == NULL)
		return;
	LWLockAcquire(TsoClientCtl->lock, LW_EXCLUSIVE);
	TsoClientCtl->last_beat_ok = GetCurrentTimestamp();
	if (lease_ms > 0)
		TsoClientCtl->lease_ms = lease_ms;
	LWLockRelease(TsoClientCtl->lock);
}

/*
 * T3.5 栅栏（护栏二，§6.2）：持有活跃快照期间若本节点已 lease−ε 没续上租
 * （ε = lease/4），本地先行作废快照——必然先于 master 的到期剔除生效，
 * 依赖仅时钟漂移速率有界。心跳 worker 每 lease/3 续一次，健康时远够不着。
 */
static void
tso_fence_check(void)
{
	TimestampTz beat;
	int32		lease;

	if (TsoClientCtl == NULL || !tso_configured())
		return;
	LWLockAcquire(TsoClientCtl->lock, LW_SHARED);
	beat = TsoClientCtl->last_beat_ok;
	lease = TsoClientCtl->lease_ms;
	LWLockRelease(TsoClientCtl->lock);
	if (lease <= 0 || beat == 0)
		return;					/* 尚无租约信息（本快照刚经 RPC 取得） */

	if (GetCurrentTimestamp() >
		TimestampTzPlusMilliseconds(beat, lease - lease / 4))
		ereport(ERROR,
				(errcode(ERRCODE_SNAPSHOT_TOO_OLD),
				 errmsg("分片快照被栅栏作废：本节点已 %d ms 未能向 TSO 续租",
						lease - lease / 4),
				 errdetail("GlobalSafeTs 护栏二（设计 §6.2）：本地作废必须先于"
						   " master 租约剔除，否则 vacuum 可能越过活跃快照。"),
				 errhint("检查 TSO master 连通性后重试事务。")));
}

/* ---- RPC ---- */

static void
tso_disconnect(void)
{
	if (tso_conn != NULL)
	{
		PQfinish(tso_conn);
		tso_conn = NULL;
	}
}

/* 执行一次取号 SQL；成功返回 ts，失败返回 -1（调用方决定重连重试） */
static int64
tso_rpc_once(const char *sql)
{
	PGresult   *res;
	int64		ts = -1;

	if (tso_conn == NULL)
	{
		tso_conn = PQconnectdb(tso_conninfo);
		if (PQstatus(tso_conn) != CONNECTION_OK)
		{
			snprintf(tso_last_err, sizeof(tso_last_err), "connect: %s",
					 PQerrorMessage(tso_conn));
			tso_disconnect();
			return -1;
		}
	}

	res = PQexec(tso_conn, sql);
	if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
	{
		ts = strtoll(PQgetvalue(res, 0, 0), NULL, 10);
		PQclear(res);
		return (ts > 0) ? ts : -1;
	}
	snprintf(tso_last_err, sizeof(tso_last_err), "exec: %s",
			 PQerrorMessage(tso_conn));
	PQclear(res);
	tso_disconnect();			/* 失败即弃连接，重试走全新连接 */
	return -1;
}

static int64
tso_rpc(const char *sql, const char *what)
{
	int64		ts = tso_rpc_once(sql);

	if (ts < 0)
		ts = tso_rpc_once(sql);	/* 重连重试一次 */
	if (ts < 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("TSO 不可达或拒绝服务（取 %s 失败）", what),
				 errdetail("conninfo=\"%s\"；%s", tso_conninfo, tso_last_err),
				 errhint("fail-closed：绝不以本地时钟顶替（设计 §2.2 纪律 2）。")));
	return ts;
}

/* ---- 对内 API（T3.3 起被可见性/落账消费） ---- */

/*
 * 取本事务的分片快照 start_ts（懒取 + 事务内缓存）。
 * 遗留模式（未配置）返回 0。
 */
int64
TsoGetStartTs(void)
{
	char		sql[128];
	int64		oldest;

	if (cur_start_ts != 0)
	{
		tso_fence_check();		/* T3.5：持有快照期间的每次取用都过栅栏 */
		return cur_start_ts;
	}
	if (!tso_configured())
		return 0;

	if (TsoClientCtl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist: TSO client 共享内存未初始化")));

	LWLockAcquire(TsoClientCtl->lock, LW_SHARED);
	oldest = tso_active_min_locked();
	LWLockRelease(TsoClientCtl->lock);

	/* 字面量显式转型：0 会被解析成 int4 撞不上 (int, bigint) 签名 */
	snprintf(sql, sizeof(sql),
			 "SELECT partdist_tso_start_ts(%d, CAST(" INT64_FORMAT " AS bigint))",
			 (int) PartDistLocalNodeId(), oldest);
	cur_start_ts = tso_rpc(sql, "start_ts");
	if (TsoClientCtl->node_id < 0)
		TsoClientCtl->node_id = (int32) PartDistLocalNodeId();
	tso_note_lease_renewal(0);	/* start_ts 在 master 侧即续租 */
	tso_active_register(cur_start_ts);
	return cur_start_ts;
}

/*
 * PRE_COMMIT 暂存 commit_ts（临界区外的最后落点，"尽晚取"）。
 * 已暂存或遗留模式则跳过。R-P3-1 第一道防线：这里 ERROR = 事务干净中止。
 */
void
TsoStashCommitTs(void)
{
	if (cur_commit_ts != 0 || !tso_configured())
		return;
	cur_commit_ts = tso_rpc("SELECT partdist_tso_commit_ts()", "commit_ts");
}

/* 收集钩子/落账读取暂存（临界区内安全：纯内存读） */
int64
TsoStashedCommitTs(void)
{
	return cur_commit_ts;
}

/* ================= 验收/调试 SQL 包装 ================= */

PG_FUNCTION_INFO_V1(partdist_tso_client_start_ts);
Datum
partdist_tso_client_start_ts(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(TsoGetStartTs());
}

PG_FUNCTION_INFO_V1(partdist_tso_client_commit_ts);
Datum
partdist_tso_client_commit_ts(PG_FUNCTION_ARGS)
{
	TsoStashCommitTs();
	PG_RETURN_INT64(TsoStashedCommitTs());
}

/* ================= T3.5 心跳 bgworker ================= */

/*
 * 发一次心跳（worker 进程调用；失败静默返回 false——worker 不能死，
 * 由栅栏负责把失联转化为读侧 ERROR）。
 */
static bool
TsoHeartbeatOnce(void)
{
	char		sql[128];
	int64		oldest;
	int64		lease;

	int32		node;

	if (!tso_configured() || TsoClientCtl == NULL)
		return false;

	LWLockAcquire(TsoClientCtl->lock, LW_SHARED);
	node = TsoClientCtl->node_id;
	oldest = tso_active_min_locked();
	LWLockRelease(TsoClientCtl->lock);

	if (node < 0)
	{
		if (partdist_node_id >= 0)
			node = partdist_node_id;	/* GUC 显式给了，无需 catalog */
		else
			return false;		/* 节点号未知=从没人取过号，无需续租 */
	}

	snprintf(sql, sizeof(sql),
			 "SELECT partdist_tso_heartbeat(%d, CAST(" INT64_FORMAT " AS bigint))",
			 node, oldest);
	lease = tso_rpc_once(sql);
	if (lease < 0)
	{
		lease = tso_rpc_once(sql);	/* 重连重试一次 */
		if (lease < 0)
			return false;
	}
	tso_note_lease_renewal((int32) lease);
	return true;
}

void
TsoHeartbeatWorkerMain(Datum main_arg)
{
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	for (;;)
	{
		int			wait_ms = 5000;

		CHECK_FOR_INTERRUPTS();
		if (ShutdownRequestPending)
			proc_exit(0);
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		if (tso_configured())
		{
			(void) TsoHeartbeatOnce();
			if (TsoClientCtl != NULL && TsoClientCtl->lease_ms > 0)
				wait_ms = Max(1000, TsoClientCtl->lease_ms / 3);
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 wait_ms, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
}

void
TsoRegisterHeartbeatWorker(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	strlcpy(worker.bgw_name, "pg_partdist tso heartbeat", BGW_MAXLEN);
	strlcpy(worker.bgw_type, "pg_partdist tso heartbeat", BGW_MAXLEN);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 10;	/* 常驻：异常退出 10s 重启 */
	strlcpy(worker.bgw_library_name, "pg_partdist", BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, "TsoHeartbeatWorkerMain", BGW_MAXLEN);
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&worker);
}
