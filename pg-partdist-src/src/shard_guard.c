/*
 * shard_guard.c — T4.6：§9.2 第 3 层「分类处置」的禁用项拦截。
 *
 * 设计依据 TX_TSO_MVCC_DESING.md §9.2 第 3 层与 §10 第一期功能限制：
 * Citus 的搬分片/去分布/改分布路径**亲手读写分片表数据**，且全部假定原生
 * 快照与可见性契约——而本方案把分片表的可见性契约整个换掉了（xmin 是分片
 * xid、判决在分片 clog、可见性看 commit_ts vs start_ts）。这些路径若放行：
 *
 *   · rebalancer / citus_move_shard_placement / citus_copy_shard_placement
 *     用原生快照读源分片、写目标分片 —— 分片 xid 拿去查原生 clog = **静默
 *     错读**（读到的可见性与真相无关），且与 raft 管理的放置直接冲突
 *     （放置真相在 group0 的 partition_map，不是 Citus 自己搬的结果）；
 *   · undistribute_table / citus_schema_undistribute 把分片数据回收进本地
 *     表，同样走原生读；
 *   · alter_distributed_table 重建分布表 = 换 relfilenode + 搬数据，既碰
 *     可见性又要 fileset 重绑（复制面）。
 *
 * 处置：**拦下报错**（§9.2「禁用」行）。搬分片的正解是后续的 raft 成员变更
 * 专项，不是 Citus 的搬运器。
 *
 * 判据用「函数名 + 白名单非空」：
 *   · 名字取自 pg_proc.proname（不硬编 OID —— Citus 升版本 OID 会变，
 *     名字是它的公开 API，稳定得多）；
 *   · 门控与 P1 拦截同源：`shard_gating_active()` 为假（没有任何打标表）
 *     时零成本直接返回，438 基线与非本方案的库完全不受影响。
 *
 * 拦截点选 ExecutorStart：这些禁用项都是 UDF（`SELECT citus_...()`），
 * 走的是 planner/executor 而非 ProcessUtility；ExecutorStart 是它们必经的
 * 最早挂点，且此处报错能干净地翻掉整个语句。
 */
#include "postgres.h"

#include "shard_guard.h"
#include "shard_xid.h"
#include "shard_replay.h"	/* T6.3c：副本壳表闸门 */

#include "catalog/pg_proc.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "utils/lsyscache.h"

/*
 * 禁用清单（§9.2 第 3 层「禁用」行 + §10）。
 *
 * 同名多签名（citus_move_shard_placement 有 4 参与 6 参两版）一并覆盖 ——
 * 按名字匹配天然如此，正是不硬编 OID 的又一好处。
 */
static const char *const shard_banned_funcs[] = {
	"citus_move_shard_placement",
	"citus_copy_shard_placement",
	"citus_rebalance_start",
	"rebalance_table_shards",
	"undistribute_table",
	"citus_schema_undistribute",
	"alter_distributed_table",
	NULL
};

static bool
shard_guard_func_banned(Oid funcid, const char **name_out)
{
	char	   *name = get_func_name(funcid);
	int			i;

	if (name == NULL)
		return false;

	for (i = 0; shard_banned_funcs[i] != NULL; i++)
	{
		if (strcmp(name, shard_banned_funcs[i]) == 0)
		{
			*name_out = shard_banned_funcs[i];	/* 静态串，出作用域仍有效 */
			pfree(name);
			return true;
		}
	}
	pfree(name);
	return false;
}

/* 表达式树遍历：任何 FuncExpr 命中禁用清单即报错 */
static bool
shard_guard_expr_walker(Node *node, void *context)
{
	const char *name = NULL;

	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr) &&
		shard_guard_func_banned(((FuncExpr *) node)->funcid, &name))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("T4.6/§9.2：%s() 在分片打标集群上被禁用", name),
				 errdetail("该路径用**原生快照**读写分片表数据，而分片表的可见性"
						   "契约已换成分片 xid + 分片 clog（commit_ts vs "
						   "start_ts）——放行等于静默错读；搬分片还与 raft "
						   "管理的放置真相（partition_map）冲突。"),
				 errhint("搬分片走后续的 raft 成员变更专项；确需使用请先撤下"
						 "pg_partdist.shard_relids 白名单（即退出本方案语义）。")));

	return expression_tree_walker(node, shard_guard_expr_walker, context);
}

/*
 * ShardGuardCheckPlan — ExecutorStart 挂点调用。
 *
 * ★ 遍历方式（2026-08-14 首版栽过的坑）：**不能**把 plan 树（planTree->
 * targetlist / qual）直接丢给 expression_tree_walker —— plan 节点里混着
 * 执行期专用节点，walker 遇到不认识的类型直接 `unrecognized node type`
 * 抛错，于是门控一开、**每条 SQL 都炸**（实测两个 worker 连 `SELECT 1`
 * 都起不来）。
 *
 * 正解：用 planner 已经算好的 `pstmt->invalItems`? 不 —— 最稳的是扫
 * `pstmt->planTree` 的**表达式字段**时走 plan 专用的 walker
 * （planstate_tree_walker 是执行期的、这里也不合适）。本方案取更简单可靠
 * 的路子：禁用项都是**顶层 UDF 调用**（`SELECT citus_xxx(...)`），planner
 * 会把它放进 Result 节点的 targetlist，其元素是 TargetEntry→FuncExpr；
 * 直接按 Result 节点取、且只对 TargetEntry 的 expr 递归，避开一切 plan
 * 节点类型。非 Result 顶层（如 FROM 里的函数扫描）由 rtable 的 functions
 * 字段覆盖。
 */
void
ShardGuardCheckPlan(PlannedStmt *pstmt)
{
	ListCell   *lc;

	if (pstmt == NULL)
		return;

	/*
	 * ★ T6.3c：先查**副本壳表**，再查打标表禁用项。
	 *
	 * 两者的门控条件不同，不能共用 ShardGatingActive()：副本壳表所在的
	 * follower 节点**从不设** pg_partdist.shard_relids，白名单恒空，
	 * 于是那道快门一开就把副本这一支也挡在外面 —— 这正是此前"设计文档反复
	 * 强调不许 SELECT、代码里却没人拦"的直接原因。
	 * 副本这一支自己的快门是 ReplayCtl->nreplicas == 0（见 ShardReplicaIsLocal）。
	 */
	foreach(lc, pstmt->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte != NULL && rte->rtekind == RTE_RELATION)
			ShardReplicaAccessGate(rte->relid, "查询");
	}

	if (!ShardGatingActive())
		return;					/* 无打标表：下面的禁用项检查零成本返回 */

	/* ① 顶层 Result 的 targetlist（`SELECT f(...)` 的落点） */
	if (pstmt->planTree != NULL && IsA(pstmt->planTree, Result))
	{
		foreach(lc, pstmt->planTree->targetlist)
		{
			TargetEntry *te = (TargetEntry *) lfirst(lc);

			if (te != NULL && IsA(te, TargetEntry) && te->expr != NULL)
				(void) shard_guard_expr_walker((Node *) te->expr, NULL);
		}
	}

	/* ② RTE_FUNCTION 的函数表达式（`SELECT * FROM f(...)`） */
	foreach(lc, pstmt->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
		ListCell   *lc2;

		if (rte == NULL || rte->rtekind != RTE_FUNCTION)
			continue;
		foreach(lc2, rte->functions)
		{
			RangeTblFunction *rtf = (RangeTblFunction *) lfirst(lc2);

			if (rtf != NULL && rtf->funcexpr != NULL)
				(void) shard_guard_expr_walker(rtf->funcexpr, NULL);
		}
	}
}
