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
#include "nodes/plannodes.h"	/* T6.6：FunctionScan / Append 等计划节点 */
#include "nodes/primnodes.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace_d.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"		/* R-P6-22：读 pg_raft.raft_enabled */
#include "utils/hsearch.h"
#include "utils/lsyscache.h"

/*
 * 禁用清单（§9.2 第 3 层「禁用」行 + §10）。
 *
 * 同名多签名（citus_move_shard_placement 有 4 参与 6 参两版）一并覆盖 ——
 * 按名字匹配天然如此，正是不硬编 OID 的又一好处。
 */
/*
 * ★ R-P6-22 / T7.9（2026-09-10）：清单拆成两张，判据不同。
 *
 * **集群级**（下表）：Citus 的运维/搬运类 UDF。它们该禁的理由是"本集群的分片放置
 * 由 raft 管、数据由物理回放同步"，与**本节点有没有打标表**毫无关系 ——
 * 而它们恰恰只在**协调者**上调用，协调者上既没有打标表、也没人给它设白名单。
 * 旧代码把这一层压在 `ShardGatingActive()`（本节点有打标表）之下，于是
 * **生产形态下这一整层是熄火的**：`negative_p6` 之所以全绿，是因为夹具用一个
 * worker 上的表 OID 给协调者开了闸门（`test_negative_p6.sh:64-66`），
 * 那句夹具注释其实已经把真相写出来了，只是没人往下追一步。
 *
 * **打标级**（下面第二张表）：逻辑解码入口。那是针对"分片打标元组带 4 字节尾缀"
 * 这件事的，本节点没有打标表时确实无从谈起，判据保持不变。
 */
static const char *const shard_banned_cluster_funcs[] = {
	"citus_move_shard_placement",
	"citus_copy_shard_placement",
	"citus_rebalance_start",
	"rebalance_table_shards",
	"undistribute_table",
	"citus_schema_undistribute",
	"alter_distributed_table",
	/*
	 * ★ T7.9：补上审计查出的 8 个同类（Citus 13.1 里都存在，实测 pg_proc 全是
	 * C 函数）。它们同样亲手搬/读分片数据，绕过去就是静默错读或与 raft 放置冲突。
	 * `master_*` 是旧名，按名匹配天然覆盖。
	 */
	"citus_split_shard_by_split_points",
	"isolate_tenant_to_new_shard",
	"citus_drain_node",
	"master_move_shard_placement",
	"master_copy_shard_placement",
	"replicate_table_shards",
	"citus_schema_move",
	"alter_table_set_access_method",
	NULL
};

static const char *const shard_banned_marked_funcs[] = {
	/*
	 * ★ T6.6：逻辑解码入口。§10 早就写着"分片表逻辑解码：禁"，补丁 0006 也
	 * 确实实装了一道守卫 —— 但它挂在**可见性层**
	 * （HeapTupleSatisfiesHistoricMVCC），而实测崩溃发生在**解码阶段**，
	 * 根本到不了那里：
	 *     LOG:  starting logical decoding for slot "t66slot"
	 *     *** stack smashing detected ***: terminated
	 *     server process ... was terminated by signal 6: Aborted
	 *
	 * 机制：补丁 0005 在 insert/update/multi_insert 的**主数据末尾追加 4 字节
	 * 分片 xid 尾缀**，而逻辑解码走的是**原生解析器**，它按记录长度反算元组
	 * 长度时会多算那 4 字节 ⇒ 拷贝越界 ⇒ 栈保护器 abort。后端崩溃会让
	 * postmaster **整节点重置**，也就是说一次普通的 SQL 调用能打掉整个节点。
	 *
	 * 所以禁令必须**前移到入口**：白名单非空时，连槽都不许建、更不许解码。
	 * 在解码器学会那个尾缀之前，这是唯一能保证"不触碰腐坏路径"的位置。
	 * 已登记 R-P6-7。
	 */
	"pg_create_logical_replication_slot",
	"pg_logical_slot_get_changes",
	"pg_logical_slot_peek_changes",
	"pg_logical_slot_get_binary_changes",
	"pg_logical_slot_peek_binary_changes",
	NULL
};

/*
 * ShardGuardClusterManaged —— R-P6-22 的新闸门：**本集群是不是 raft 管放置的**。
 *
 * 判据取 `pg_raft.raft_enabled`。为什么是它：Citus 运维类 UDF 该禁的理由就是
 * "放置由 raft 管、数据由物理回放同步"，raft 一开这条就成立，与本节点手上有没有
 * 打标表无关。
 *
 * 每 backend 解析一次并缓存：`GetConfigOption` 是哈希查找，放在 ExecutorStart 的
 * 每条语句上不合适；而这个值在进程生命期内改变的唯一途径是 SIGHUP 改 GUC，
 * 那种场景下"新连接才生效"是可以接受的（禁令是纵深防御的第三层，不是唯一一层）。
 */
static bool
ShardGuardClusterManaged(void)
{
	static int cached = -1;		/* -1 未解析 / 0 否 / 1 是 */

	if (cached < 0)
	{
		const char *v = GetConfigOption("pg_raft.raft_enabled", true, false);

		cached = (v != NULL && (v[0] == 'o' || v[0] == 'O' ||
								v[0] == 't' || v[0] == 'T' ||
								v[0] == '1')) ? 1 : 0;
		if (cached == 1 && v != NULL && strcmp(v, "off") == 0)
			cached = 0;			/* "off" 也以 'o' 开头，单独排掉 */
	}
	return cached == 1;
}

/*
 * ShardGuardIsReferenceTable —— T7.10（2026-09-09 用户裁定"拦"）：这张表是不是
 * Citus **引用表**（`pg_dist_partition.partmethod = 'n'`）。
 *
 * 为什么要拦：DESIGN §10 一直写着"引用表建表后只读"，理由是它没有分片写集 ⇒
 * 没有协调者分片 ⇒ 不受协调组决议保护，而 Citus 原生 2PC 恢复又被 §9.2 关掉了
 * （`citus.recover_2pc_interval = -1`）—— 也就是说引用表的跨节点写在本方案里
 * **既不受我们的决议保护、也不受 Citus 的恢复保护**。但那一行此前只是纸面约定：
 * 2026-09-09 复核时 `shard_guard.c` 全文找不到任何引用表判据，测试也零断言。
 *
 * 缓存：与 `partition_wal.c` 的 `IsCitusDistributedTable` 同款 backend 本地哈希 ——
 * 写路径每条语句都要问，不能每次都扫 catalog。表被 DROP/重建时 OID 会变，
 * 旧条目自然失效（与那份缓存同一条论证）。
 */
typedef struct RefTableEntry
{
	Oid		relid;
	bool	is_ref;
} RefTableEntry;

static HTAB *RefTableCache = NULL;

static bool
ShardGuardIsReferenceTable(Oid relid)
{
	RefTableEntry *entry;
	bool		found = false;
	bool		is_ref = false;
	Oid			pgDistPartitionOid;

	if (!OidIsValid(relid))
		return false;

	if (RefTableCache == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(RefTableEntry);
		RefTableCache = hash_create("pg_partdist_reftable_cache", 64, &ctl,
									HASH_ELEM | HASH_BLOBS);
	}

	entry = hash_search(RefTableCache, &relid, HASH_FIND, &found);
	if (found)
		return entry->is_ref;

	pgDistPartitionOid = get_relname_relid("pg_dist_partition",
										   PG_CATALOG_NAMESPACE);
	if (OidIsValid(pgDistPartitionOid))
	{
		PG_TRY();
		{
			Relation	rel;
			SysScanDesc scan;
			ScanKeyData key;
			HeapTuple	tup;
			Oid			idx = get_relname_relid(
							"pg_dist_partition_logical_relid_index",
							PG_CATALOG_NAMESPACE);

			rel = table_open(pgDistPartitionOid, AccessShareLock);
			ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(relid));
			scan = systable_beginscan(rel, idx, OidIsValid(idx), NULL, 1, &key);
			tup = systable_getnext(scan);
			if (HeapTupleIsValid(tup))
			{
				bool	isnull = true;
				Datum	d = heap_getattr(tup, 2 /* partmethod */,
										 RelationGetDescr(rel), &isnull);

				if (!isnull)
					is_ref = (DatumGetChar(d) == 'n');
			}
			systable_endscan(scan);
			table_close(rel, AccessShareLock);
		}
		PG_CATCH();
		{
			FlushErrorState();
			is_ref = false;		/* 查不出来就不拦：守卫不能把普通库带崩 */
		}
		PG_END_TRY();
	}

	entry = hash_search(RefTableCache, &relid, HASH_ENTER, &found);
	entry->relid  = relid;
	entry->is_ref = is_ref;
	return is_ref;
}

/*
 * ShardGuardCheckReferenceWrite —— T7.10：引用表运行期写，拦。
 *
 * 由 `partdist_planner`（planner_hook）在**Citus 改写之前**调用 —— 挂在
 * ExecutorStart 上判 `pstmt->resultRelations` 的第一版实测完全不生效：
 * Citus 把引用表的写重写成自己的 CustomScan，顶层已经不是普通 ModifyTable。
 */
void
ShardGuardCheckReferenceWrite(Oid relid)
{
	if (!ShardGuardClusterManaged())
		return;
	if (!ShardGuardIsReferenceTable(relid))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("T7.10/§10：引用表 \"%s\" 在分片打标集群上建表后只读，"
					"运行期写入被禁用", get_rel_name(relid)),
			 errdetail("引用表没有分片写集 ⇒ 没有协调者分片 ⇒ 不受协调组决议保护；"
					   "而 Citus 原生 2PC 恢复已按 §9.2 关闭"
					   "（citus.recover_2pc_interval = -1）—— 它的跨节点写两头"
					   "都没有保护。"),
			 errhint("引用数据请在建表阶段一次性灌入。")));
}

static bool guard_check_marked = false;		/* 本次遍历要不要查打标级清单 */

static bool
shard_guard_func_banned(Oid funcid, const char **name_out)
{
	char	   *name = get_func_name(funcid);
	int			i;

	if (name == NULL)
		return false;

	for (i = 0; shard_banned_cluster_funcs[i] != NULL; i++)
	{
		if (strcmp(name, shard_banned_cluster_funcs[i]) == 0)
		{
			*name_out = shard_banned_cluster_funcs[i];	/* 静态串，安全 */
			pfree(name);
			return true;
		}
	}
	if (guard_check_marked)
		for (i = 0; shard_banned_marked_funcs[i] != NULL; i++)
		{
			if (strcmp(name, shard_banned_marked_funcs[i]) == 0)
			{
				*name_out = shard_banned_marked_funcs[i];
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
 * 计划树遍历：只取节点的**表达式字段**，节点本身不交给 expression_tree_walker
 * （交出去会撞 `unrecognized node type`，见下方 ShardGuardCheckPlan 的注释）。
 */
static void
shard_guard_walk_plan(Plan *plan)
{
	ListCell   *lc;

	if (plan == NULL)
		return;

	/* 表达式字段：targetlist 覆盖 `SELECT f(...)`，qual 覆盖 `WHERE f(...)` */
	foreach(lc, plan->targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);

		if (te != NULL && IsA(te, TargetEntry) && te->expr != NULL)
			(void) shard_guard_expr_walker((Node *) te->expr, NULL);
	}
	(void) shard_guard_expr_walker((Node *) plan->qual, NULL);

	/* ★ `SELECT * FROM f(...)`：funcexpr 在**计划节点**里，不在 rtable 里 */
	if (IsA(plan, FunctionScan))
	{
		foreach(lc, ((FunctionScan *) plan)->functions)
		{
			RangeTblFunction *rtf = (RangeTblFunction *) lfirst(lc);

			if (rtf != NULL && rtf->funcexpr != NULL)
				(void) shard_guard_expr_walker(rtf->funcexpr, NULL);
		}
	}

	/* 子节点 */
	shard_guard_walk_plan(plan->lefttree);
	shard_guard_walk_plan(plan->righttree);

	if (IsA(plan, Append))
		foreach(lc, ((Append *) plan)->appendplans)
			shard_guard_walk_plan((Plan *) lfirst(lc));
	else if (IsA(plan, MergeAppend))
		foreach(lc, ((MergeAppend *) plan)->mergeplans)
			shard_guard_walk_plan((Plan *) lfirst(lc));
	else if (IsA(plan, BitmapAnd))
		foreach(lc, ((BitmapAnd *) plan)->bitmapplans)
			shard_guard_walk_plan((Plan *) lfirst(lc));
	else if (IsA(plan, BitmapOr))
		foreach(lc, ((BitmapOr *) plan)->bitmapplans)
			shard_guard_walk_plan((Plan *) lfirst(lc));
	else if (IsA(plan, SubqueryScan))
		shard_guard_walk_plan(((SubqueryScan *) plan)->subplan);
	else if (IsA(plan, CustomScan))
		foreach(lc, ((CustomScan *) plan)->custom_plans)
			shard_guard_walk_plan((Plan *) lfirst(lc));
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
 * 正解（T6.6 修正）：**遍历计划树**，只把每个节点的**表达式字段**
 * （targetlist / qual）交给 expression_tree_walker，节点本身绝不交出去。
 *
 * ★★ 首版为什么漏：首版靠两处取表达式 —— 顶层 Result 的 targetlist，
 * 外加 `pstmt->rtable` 里 RTE_FUNCTION 的 `rte->functions`。后者是**死代码**：
 * PlannedStmt 的 rtable 是 setrefs.c 造的扁平副本，
 * `add_rte_to_flat_rtable()` 明写着 `newrte->functions = NIL;`
 * （本仓库 postgres-src/src/backend/optimizer/plan/setrefs.c:554），
 * 执行器改从 FunctionScan **计划节点**的 functions 字段取。于是循环永远
 * 空转，`SELECT * FROM f(...)` 这一整类调用从 T4.6 起就绕得过禁用清单 ——
 * 实测 `SELECT * FROM citus_rebalance_start()` 一路跑进了 Citus 重平衡器。
 * 已登记 R-P6-8。
 *
 * 覆盖边界（如实记）：targetlist、qual、FunctionScan.functions、以及
 * pstmt->subplans。连接节点专属的 joinqual / 索引 quals 未覆盖 —— 禁用项
 * 都是顶层 UDF 调用，不会长在那些位置；真要长在那儿属另一类问题。
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

	/*
	 * ★ R-P6-22：两层判据。
	 *   · 集群级（Citus 运维类）—— raft 一开就查，**不看本节点有没有打标表**；
	 *   · 打标级（逻辑解码）—— 仍按本节点是否有打标表。
	 * 两者都不成立时零成本返回，普通 PostgreSQL 用法一条指令都不多花。
	 */
	guard_check_marked = ShardGatingActive();
	if (!ShardGuardClusterManaged() && !guard_check_marked)
		return;

	shard_guard_walk_plan(pstmt->planTree);

	/* CTE / 子查询：initPlan、SubPlan 都指向这里，走一遍即全覆盖 */
	foreach(lc, pstmt->subplans)
		shard_guard_walk_plan((Plan *) lfirst(lc));
}
