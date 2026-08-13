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
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

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
		return cur_start_ts;
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
