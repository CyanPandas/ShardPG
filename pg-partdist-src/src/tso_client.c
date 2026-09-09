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
#include "dtx_pending.h"

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
static int64 cur_gxid = 0;		/* T4.1：join 注入的全局事务号；0 = 无 */
static int64 cur_coord_gsid = 0;	/* T4.1：协调者分片组；0 = 未知 */

/*
 * T4.2 自动 join 通道：驱动端 `SET LOCAL pg_partdist.join_info='gxid,ts,gsid'`
 * + `SET LOCAL citus.propagate_set_commands='local'`，Citus 把该 SET LOCAL
 * 原样传播到每条任务连接（实验实证，每连接一次）——参与端 GUC assign 只暂存
 * （assign 上下文不许 ereport），真正注入推迟到首次取用（TsoGetStartTs），
 * 冲突/未配置在那里以正常 ERROR 收口。事务结束 GUC 回卷 + 回调双重清理。
 */
static char *join_info_string = NULL;
static int64 pending_join_gxid = 0;
static int64 pending_join_ts = 0;
static int64 pending_join_gsid = 0;
static char tso_last_err[256];	/* 最近一次失败原因（连接被弃后仍可报） */

static bool
check_join_info(char **newval, void **extra, GucSource source)
{
	const char *s = (*newval != NULL) ? *newval : "";
	long long	g, ts, gs;
	char		trail;

	if (s[0] == '\0')
		return true;
	if (sscanf(s, "%lld,%lld,%lld%c", &g, &ts, &gs, &trail) != 3 ||
		g <= 0 || ts <= 0 || gs < 0)
	{
		GUC_check_errdetail("格式须为 'gxid,start_ts,coord_gsid'（gxid/ts>0，gsid>=0）。");
		return false;
	}
	return true;
}

static void
assign_join_info(const char *newval, void *extra)
{
	long long	g = 0, ts = 0, gs = 0;
	char		trail;

	if (newval != NULL && newval[0] != '\0' &&
		sscanf(newval, "%lld,%lld,%lld%c", &g, &ts, &gs, &trail) == 3)
	{
		pending_join_gxid = (int64) g;
		pending_join_ts = (int64) ts;
		pending_join_gsid = (int64) gs;
	}
	else
	{
		pending_join_gxid = 0;
		pending_join_ts = 0;
		pending_join_gsid = 0;
	}
}

void
TsoClientDefineGUCs(void)
{
	DefineCustomStringVariable(
		"pg_partdist.join_info",
		"连接加入协议的传播载体（T4.2）：'gxid,start_ts,coord_gsid'。",
		"驱动端在事务内 SET LOCAL 本参数（配合 citus.propagate_set_commands="
		"'local'），Citus 传播到每条任务连接即完成参与端自动 join。",
		&join_info_string,
		"",
		PGC_USERSET,
		0,
		check_join_info,
		assign_join_info,
		NULL);

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
	cur_gxid = 0;
	cur_coord_gsid = 0;

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
/*
 * ★ T7.5（R-P6-20）：本文件发出去的每一条 RPC 都必须写**全限定名**
 * `partdist.partdist_tso_*`。
 *
 * 现场（2026-09-09 在 pg-test 环境实测复现）：这些 C 函数由扩展装进
 * `partdist` 模式（`pg_partdist.control` 的 `schema = partdist`），而本文件
 * 此前发的是裸函数名；TSO 服务端连接用的是默认 `search_path = "$user", public`
 * ⇒ 取号、心跳、commit_ts、safe_ts **全部** `function partdist_tso_start_ts
 * (integer, bigint) does not exist` ⇒ 取号 fail-closed ⇒ **全簇分片写不进去**：
 *
 *     ERROR:  function partdist_tso_start_ts(integer, bigint) does not exist
 *     ERROR:  TSO 不可达或拒绝服务（取 start_ts 失败）
 *
 * 此前之所以"能跑"，是因为 P4 期各验收套件都在 `public` 里现建了同名垫片；
 * 演示环境则靠 `ALTER DATABASE postgres SET search_path TO ..., partdist` 绕过。
 * 两者都不是产品形态 —— 干净装出来的集群第一笔分片写就会撞上。
 */
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

	/* T4.2：GUC 通道有待注入三元组 ⇒ 走 join（优先于自取 RPC） */
	if (pending_join_ts > 0)
	{
		TsoInjectStartTs(pending_join_ts);
		if (cur_gxid != 0 && cur_gxid != pending_join_gxid)
			ereport(ERROR,
					(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
					 errmsg("join_info 的 gxid=" INT64_FORMAT
							" 与已加入的 " INT64_FORMAT " 冲突",
							pending_join_gxid, cur_gxid)));
		cur_gxid = pending_join_gxid;
		if (pending_join_gsid > 0)
			cur_coord_gsid = pending_join_gsid;
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
			 "SELECT partdist.partdist_tso_start_ts(%d, CAST(" INT64_FORMAT " AS bigint))",
			 (int) PartDistLocalNodeId(), oldest);
	cur_start_ts = tso_rpc(sql, "start_ts");
	if (TsoClientCtl->node_id < 0)
		TsoClientCtl->node_id = (int32) PartDistLocalNodeId();
	tso_note_lease_renewal(0);	/* start_ts 在 master 侧即续租 */
	tso_active_register(cur_start_ts);
	return cur_start_ts;
}

/*
 * T5.2：取 GlobalSafeTs（vacuum 前缀扫描的判据，设计 §6.2/§6.3）。
 *
 * 为什么必须走 RPC：`partdist_global_safe_ts()` 在 master 之外直接 ERROR，
 * 而 vacuum 跑在**分片 leader（worker）**上 —— 本地读不到。
 *
 * 为什么"偏小是安全方向"：GlobalSafeTs 是"没有任何活跃快照会看到更早提交"
 * 的下界。取到偏小的值 ⇒ 前缀扫描更早停下 ⇒ 少清一点垃圾，正确性不受损；
 * 取到偏大才危险（会把仍被活跃快照需要的版本判为可回收）。故 RPC 失败时
 * **返回 0 = 什么都别清**，而不是回退到某个本地估计值。
 */
int64
TsoGetGlobalSafeTs(void)
{
	if (!tso_configured())
		return 0;				/* 遗留模式：无 TSO 宇宙，不清 */
	return tso_rpc("SELECT partdist.partdist_global_safe_ts()", "global_safe_ts");
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
	cur_commit_ts = tso_rpc("SELECT partdist.partdist_tso_commit_ts()", "commit_ts");
}

/* 收集钩子/落账读取暂存（临界区内安全：纯内存读） */
int64
TsoStashedCommitTs(void)
{
	return cur_commit_ts;
}

/*
 * T4.4 换源：MARKER / DTX 记录的 commit_ts 取值源。
 *
 * 遗留模式（未配置 TSO）：本地时钟 —— 行为与换源前逐字节一致，tx1/tx2/tx4
 * 基线断言不动。
 * TSO 模式：返回本事务的 PRE_COMMIT 暂存值（懒取：首次调用即取号），保证
 * 同一事务的 MARKER、0007 尾块、DTX 记录用同一个 ts。COMMIT PREPARED 的
 * 阶段 3 跑在另一个工具事务里，取到的是该工具事务的新号 —— 晚于决议 ts，
 * 方向保守（副本迟可见、不早可见）；决议 ts 的精确回填走 T4.5 广播。
 * fail-closed 由 tso_rpc 保证（不可达即 ERROR，绝不回退本地时钟）。
 */
int64
TsoMarkerCommitTs(void)
{
	if (!tso_configured())
		return (int64) GetCurrentTimestamp();
	TsoStashCommitTs();
	return cur_commit_ts;
}

/*
 * T4.4：决议点取号（pg_raft 经 rendezvous "partdist_tso_dtx_decision_ts_fn"
 * 调用）。"全部 PREPARE 持久确认之后、决议持久化之前"的新号（§2-4 两时机，
 * 不预取不缓存）；未配置 TSO 返回 0（决议侧回退本地时钟，遗留宇宙不混）。
 *
 * ★ 取到的决议 ts **覆盖**本事务的 PRE_COMMIT 暂存：驱动事务自己的
 * PRE_COMMIT 早于 Citus 的远端 PREPARE（回调顺序，Citus 居首纪律），
 * 暂存值早于"票齐"点、不能作决议 ts；覆盖之后，本地分片 0007 尾块在
 * 提交记录里携带的就是决议 ts —— 驱动节点本地分片的可见性与全局提交点
 * 自洽（PRE_COMMIT 已组装的 MARKER 载荷仍是早值，见 R-P4-3，T4.5 收口）。
 */
int64
PartDistTsoDtxDecisionTs(void)
{
	int64		ts;

	if (!tso_configured())
		return 0;
	ts = tso_rpc("SELECT partdist.partdist_tso_commit_ts()", "dtx_decision_ts");
	cur_commit_ts = ts;
	return ts;
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
			 "SELECT partdist.partdist_tso_heartbeat(%d, CAST(" INT64_FORMAT " AS bigint))",
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

		/*
		 * T4.5：未决 2PC 清扫兜底（读者问询之外的收敛通道）。本工作者无
		 * DB 语境，经自连触发 partdist.dtx_pending_sweep() 在干净 backend
		 * 里做 SPI 寻址 + 远程 peek。登记表空则零开销。
		 */
		if (DtxPendingCount() > 0)
			DtxPendingSelfTriggerSweep();

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

/* ================= T4.1 连接加入协议（§9.2 第 2 层） ================= */

/*
 * 注入协调者下发的 start_ts（参与者后端不自取、不 RPC）。
 * 必须登记进本节点活跃集合——否则本节点心跳携带的 oldest 看不见这个远端
 * 快照，GlobalSafeTs 会越过活跃远端读（P4_PRECHECK 结论五 / R-P4-2）。
 * 栅栏语义沿用：本节点自己的心跳新鲜度保护该快照。
 */
void
TsoInjectStartTs(int64 ts)
{
	if (ts <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("注入的 start_ts 必须 > 0")));
	if (!tso_configured())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("本节点未配置 TSO（pg_partdist.tso_conninfo），拒绝加入全局事务"),
				 errdetail("未配置节点没有心跳/栅栏保护，注入快照会脱离 "
						   "GlobalSafeTs 视野（fail-closed）。")));
	if (cur_start_ts != 0 && cur_start_ts != ts)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("本事务已持有 start_ts=" INT64_FORMAT
						"，不能改注 " INT64_FORMAT, cur_start_ts, ts)));

	cur_start_ts = ts;
	tso_active_register(ts);
}

int64
TsoCurrentGxid(void)
{
	return cur_gxid;
}

int64
TsoCurrentCoordGsid(void)
{
	return cur_coord_gsid;
}

/*
 * partdist_join_global_txn(gxid, start_ts, coord_gsid) —— 参与者后端登记
 * 三元组（§9.2 第 2 层）。T4.1 交付直调形态（协调者→参与者的自动发送
 * 通道 = 发起端登记表 + 参与端回拉，随 T4.2 MX 路由一体接线）。
 * coord_gsid=0 表示暂未知（§4.2 NULL 不变式的兜底分支照常成立）。
 */
PG_FUNCTION_INFO_V1(partdist_join_global_txn);
Datum
partdist_join_global_txn(PG_FUNCTION_ARGS)
{
	int64		gxid = PG_GETARG_INT64(0);
	int64		start_ts = PG_GETARG_INT64(1);
	int64		coord_gsid = PG_GETARG_INT64(2);

	if (gxid <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gxid 必须 > 0")));
	if (cur_gxid != 0 && cur_gxid != gxid)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("本事务已加入 gxid=" INT64_FORMAT
						"，不能改投 " INT64_FORMAT, cur_gxid, gxid)));

	TsoInjectStartTs(start_ts);
	cur_gxid = gxid;
	if (coord_gsid > 0)
		cur_coord_gsid = coord_gsid;

	PG_RETURN_VOID();
}
