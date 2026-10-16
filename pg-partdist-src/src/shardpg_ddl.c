/*-------------------------------------------------------------------------
 *
 * shardpg_ddl.c
 *	  ShardPG 建表 / 删表一条龙（P7-N87 / N88 / N89）
 *
 * 一、CREATE TABLE ... SHARD KEY (列) [SHARDS n] [REPLICAS n] [COLOCATE WITH 表]
 *
 *   内核补丁 0016 把 SHARD KEY 子句解析成命名空间 "shardpg" 的 reloption
 *   （shardpg.column / shards / replicas / colocate）。这里在 Citus 之前截下：
 *
 *     事务 1：去掉 shardpg.* 选项照常建表 → create_distributed_table
 *             （citus.shard_replication_factor=1：副本由 Raft 管，Citus 只放一份）
 *     提交   ：worker 要看得见新分片才能建组、供副本
 *     事务 2：partdist.shardpg_onboard() —— 建 Raft 组、选主、供副本、打标
 *     提交
 *
 *   语句内提交的做法与 VACUUM 相同（PreventInTransactionBlock + 自管事务）：
 *   不能在事务块里执行，所以用户在这条语句返回之前写不进数据，
 *   "打标必须先于写入"的约束自然成立。事务 2 失败时把刚建的表连同分片删掉再报错。
 *
 * 二、DROP TABLE 分片表
 *
 *   被删的分布式表只要有分片在控制面登记（partdist.partition_map）就截下：
 *
 *     事务 1：把它的全部分片记进 partdist.shardpg_drop_queue → 照常 DROP
 *             （记队列与删表同一事务：DROP 失败回滚时队列也回滚，Raft 组不会被提前拆掉）
 *     提交
 *     事务 2：partdist.shardpg_cleanup_dropped() —— 关回放槽、拆 Raft 组、删各节点副本表、
 *             删登记、回收残留，并逐节点核对
 *
 *   在事务块里删表（或嵌在函数里）时不能中途提交：只记队列，提示提交后执行
 *   SELECT shardpg.cleanup_dropped()。清理函数只动"已经不在 pg_dist_shard 里"的分片，
 *   所以重复执行、提前执行都不会误伤活着的表。
 *
 *   只在 coordinator 上截：worker 收到的 DROP（Citus 下发的分片删除、MX 元数据清理）
 *   照常执行。
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "commands/extension.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "shardpg_ddl.h"
#include "tso.h"

int			shardpg_shard_count = 0;		/* 0 = 等于 worker 数 */
int			shardpg_replicas = 2;
static bool shardpg_drop_cleanup = true;

void
ShardpgDdlDefineGucs(void)
{
	DefineCustomIntVariable("shardpg.shard_count",
							"SHARD KEY 建表时不写 SHARDS 的默认分片数（0 = 等于 worker 数）。",
							NULL,
							&shardpg_shard_count,
							0, 0, 4096,
							PGC_USERSET, 0,
							NULL, NULL, NULL);
	DefineCustomIntVariable("shardpg.replicas",
							"SHARD KEY 建表时不写 REPLICAS 的默认副本数（每个分片 1 主 + n 从）。",
							NULL,
							&shardpg_replicas,
							2, 1, 16,
							PGC_USERSET, 0,
							NULL, NULL, NULL);
	DefineCustomBoolVariable("shardpg.drop_cleanup",
							 "DROP TABLE 删分片表时自动拆 Raft 组、删副本表、删登记。",
							 "关掉后 DROP 只记清理队列，由 SELECT shardpg.cleanup_dropped() 手工清。",
							 &shardpg_drop_cleanup,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	DefineCustomBoolVariable("shardpg.auto_global_txn",
							 "跨分片事务自动加入全局事务（统一 gxid 与快照时间戳）。",
							 "Citus 开每个远程事务分支时，master 把本事务的全局事务号和 start_ts 随同一条 BEGIN "
							 "交代过去（P7-N91）。关掉后一个事务写多个分片会在 PREPARE 时被拒。",
							 &shardpg_auto_global_txn,
							 true,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);
	MarkGUCPrefixReserved("shardpg");
}

/* ---------------------------------------------------------------- 工具 */

/* 执行一条 SQL，取第一行第一列（文本，可能为 NULL），结果拷到 cxt */
static char *
spi_value(const char *sql, MemoryContext cxt)
{
	char	   *out = NULL;
	int			rc;

	SPI_connect();
	rc = SPI_execute(sql, false, 0);
	if (rc < 0)
		elog(ERROR, "shardpg: SPI 执行失败（%d）：%s", rc, sql);
	if (SPI_processed > 0 && SPI_tuptable != NULL)
	{
		char	   *v = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

		if (v != NULL)
			out = MemoryContextStrdup(cxt, v);
	}
	SPI_finish();
	return out;
}

/*
 * 语句内提交：照 VACUUM 的做法，先弹掉活动快照（门户推的那个也弹，提交时
 * AtCommit_Portals 会把活动门户的 portalSnapshot 置空，PortalRunUtility 就不再弹），
 * 提交、开新事务，切回调用方的内存上下文（门户上下文跨提交存活）。
 */
static void
commit_and_restart(MemoryContext back_to)
{
	while (ActiveSnapshotSet())
		PopActiveSnapshot();
	CommitTransactionCommand();
	StartTransactionCommand();
	MemoryContextSwitchTo(back_to);
}

static bool
is_coordinator(MemoryContext cxt)
{
	char	   *v = spi_value("SELECT pg_catalog.citus_is_coordinator()", cxt);

	return v != NULL && strcmp(v, "t") == 0;
}

/* ---------------------------------------------------------------- 建表 */

typedef struct ShardKeyOpts
{
	char	   *column;
	int			shards;			/* 0 = 未指定 */
	int			replicas;		/* 0 = 未指定 */
	char	   *colocate;
	int			location;
} ShardKeyOpts;

/* 从 options 里摘下 shardpg.*；没有就返回 false */
static bool
extract_shard_opts(CreateStmt *stmt, ShardKeyOpts *o, List **rest)
{
	ListCell   *lc;
	bool		found = false;

	memset(o, 0, sizeof(*o));
	*rest = NIL;
	foreach(lc, stmt->options)
	{
		DefElem    *d = lfirst_node(DefElem, lc);

		if (d->defnamespace == NULL || strcmp(d->defnamespace, "shardpg") != 0)
		{
			*rest = lappend(*rest, d);
			continue;
		}
		found = true;
		if (strcmp(d->defname, "column") == 0)
		{
			o->column = strVal(d->arg);
			o->location = d->location;
		}
		else if (strcmp(d->defname, "shards") == 0)
			o->shards = intVal(d->arg);
		else if (strcmp(d->defname, "replicas") == 0)
			o->replicas = intVal(d->arg);
		else if (strcmp(d->defname, "colocate") == 0)
			o->colocate = strVal(d->arg);
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("不认识的分片选项 \"%s\"", d->defname)));
	}
	return found;
}

static bool
list_has_name(List *names, const char *col)
{
	ListCell   *lc;

	foreach(lc, names)
	{
		if (strcmp(strVal(lfirst(lc)), col) == 0)
			return true;
	}
	return false;
}

/*
 * 建表前的检查：分片键列要存在；主键 / 唯一约束要包含分片键（Citus 的硬要求，
 * 提前用中文报，免得建了一半再回滚）。带 LIKE / INHERITS 的表列不全，只查约束。
 */
static void
check_shard_key(CreateStmt *stmt, const ShardKeyOpts *o)
{
	ListCell   *lc;
	bool		column_found = false;
	bool		columns_complete = (stmt->inhRelations == NIL);

	/* 先查列在不在，再查约束：列不存在时报"列不存在"，而不是"不在主键里" */
	foreach(lc, stmt->tableElts)
	{
		Node	   *elt = lfirst(lc);

		if (IsA(elt, TableLikeClause))
			columns_complete = false;
		else if (IsA(elt, ColumnDef) &&
				 strcmp(((ColumnDef *) elt)->colname, o->column) == 0)
			column_found = true;
	}
	if (columns_complete && !column_found)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("分片键列 \"%s\" 不存在", o->column)));

	foreach(lc, stmt->tableElts)
	{
		Node	   *elt = lfirst(lc);

		if (IsA(elt, ColumnDef))
		{
			ColumnDef  *cd = (ColumnDef *) elt;
			ListCell   *lc2;

			foreach(lc2, cd->constraints)
			{
				Constraint *c = lfirst_node(Constraint, lc2);

				if ((c->contype == CONSTR_PRIMARY || c->contype == CONSTR_UNIQUE) &&
					strcmp(cd->colname, o->column) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
							 errmsg("分片键 %s 必须包含在%s里", o->column,
									c->contype == CONSTR_PRIMARY ? "主键" : "唯一约束"),
							 errhint("把 %s 加进%s，例如 PRIMARY KEY (%s, %s)；或改用 SHARD KEY (%s)。",
									 o->column,
									 c->contype == CONSTR_PRIMARY ? "主键" : "唯一约束",
									 cd->colname, o->column, cd->colname)));
			}
		}
		else if (IsA(elt, Constraint))
		{
			Constraint *c = (Constraint *) elt;

			if ((c->contype == CONSTR_PRIMARY || c->contype == CONSTR_UNIQUE) &&
				!list_has_name(c->keys, o->column))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						 errmsg("分片键 %s 必须包含在%s里", o->column,
								c->contype == CONSTR_PRIMARY ? "主键" : "唯一约束"),
						 errhint("把 %s 加进%s；或把分片键换成%s里的某一列。",
								 o->column,
								 c->contype == CONSTR_PRIMARY ? "主键" : "唯一约束",
								 c->contype == CONSTR_PRIMARY ? "主键" : "唯一约束")));
		}
	}
}

static void
shardpg_create(PlannedStmt *pstmt, const char *queryString,
			   ProcessUtilityContext context, ParamListInfo params,
			   QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc,
			   ShardpgRunChainFn run_chain, CreateStmt *orig, const ShardKeyOpts *in)
{
	MemoryContext caller = CurrentMemoryContext;
	ShardKeyOpts o = *in;
	List	   *rest;
	ShardKeyOpts dummy;
	CreateStmt *stmt;
	PlannedStmt *p2;
	Oid			relid;
	char	   *relname;
	char	   *v;
	int			nworkers;
	StringInfoData sql;

	PreventInTransactionBlock(context == PROCESS_UTILITY_TOPLEVEL,
							  "CREATE TABLE ... SHARD KEY");

	if (orig->relation->relpersistence == RELPERSISTENCE_TEMP)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("临时表不能指定 SHARD KEY")));
	if (orig->partspec != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("分区表（PARTITION BY）暂不支持 SHARD KEY")));
	if (o.colocate != NULL && o.shards > 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COLOCATE WITH 时分片数随共置表，不能再写 SHARDS")));
	check_shard_key(orig, &o);

	if (!is_coordinator(caller))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("SHARD KEY 建表只能在 master（coordinator）上执行")));

	/* IF NOT EXISTS 且表已在：交给链上照常报"已存在，跳过" */
	if (orig->if_not_exists &&
		OidIsValid(RangeVarGetRelid(orig->relation, NoLock, true)))
	{
		extract_shard_opts(orig, &dummy, &rest);
		stmt = copyObject(orig);
		stmt->options = rest;
		p2 = copyObject(pstmt);
		p2->utilityStmt = (Node *) stmt;
		run_chain(p2, queryString, false, context, params, queryEnv, dest, qc);
		return;
	}

	v = spi_value("SELECT pg_catalog.count(*) FROM pg_catalog.pg_dist_node"
				  " WHERE noderole = 'primary' AND isactive AND shouldhaveshards AND groupid <> 0",
				  caller);
	nworkers = v ? atoi(v) : 0;
	if (nworkers < 1)
		ereport(ERROR,
				(errmsg("集群里没有可放分片的 worker")));
	if (o.replicas == 0)
		o.replicas = shardpg_replicas;
	if (o.replicas > nworkers - 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("副本数 %d 超出上限：%d 台 worker 最多 1 主 %d 从",
						o.replicas, nworkers, nworkers - 1)));
	if (o.colocate == NULL && o.shards == 0)
		o.shards = shardpg_shard_count > 0 ? shardpg_shard_count : nworkers;
	if (o.colocate != NULL)
	{
		initStringInfo(&sql);
		appendStringInfo(&sql,
						 "SELECT p.partmethod FROM pg_catalog.pg_dist_partition p"
						 " WHERE p.logicalrelid = pg_catalog.to_regclass(%s)",
						 quote_literal_cstr(o.colocate));
		v = spi_value(sql.data, caller);
		if (v == NULL || strcmp(v, "h") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("共置目标 %s 不存在或不是分片表", o.colocate)));
	}

	/* ---- 事务 1：建表 + 分布 ---- */
	extract_shard_opts(orig, &dummy, &rest);
	stmt = copyObject(orig);
	stmt->options = rest;
	p2 = copyObject(pstmt);
	p2->utilityStmt = (Node *) stmt;
	run_chain(p2, queryString, false, context, params, queryEnv, dest, qc);
	CommandCounterIncrement();

	relid = RangeVarGetRelid(stmt->relation, NoLock, false);
	relname = MemoryContextStrdup(caller,
								  quote_qualified_identifier(get_namespace_name(get_rel_namespace(relid)),
															 get_rel_name(relid)));

	SPI_connect();
	SPI_execute("SELECT pg_catalog.set_config('citus.shard_replication_factor', '1', true)", false, 0);
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT pg_catalog.create_distributed_table(%s::pg_catalog.regclass, %s, colocate_with => %s",
					 quote_literal_cstr(relname), quote_literal_cstr(o.column),
					 quote_literal_cstr(o.colocate ? o.colocate : "none"));
	if (o.colocate == NULL)
		appendStringInfo(&sql, ", shard_count => %d", o.shards);
	appendStringInfoChar(&sql, ')');
	if (SPI_execute(sql.data, false, 0) < 0)
		elog(ERROR, "shardpg: create_distributed_table 执行失败");
	SPI_finish();

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT pg_catalog.count(*) FROM pg_catalog.pg_dist_shard WHERE logicalrelid = %s::pg_catalog.regclass",
					 quote_literal_cstr(relname));
	v = spi_value(sql.data, caller);
	if (o.colocate != NULL)
		ereport(NOTICE,
				(errmsg("分布：按 %s 哈希切成 %s 个分片，与 %s 共置", o.column, v ? v : "?", o.colocate)));
	else
		ereport(NOTICE,
				(errmsg("分布：按 %s 哈希切成 %s 个分片", o.column, v ? v : "?")));

	commit_and_restart(caller);

	/* ---- 事务 2：建组、选主、供副本、打标 ---- */
	initStringInfo(&sql);
	appendStringInfo(&sql, "SELECT partdist.shardpg_onboard(%s::pg_catalog.regclass, %d)",
					 quote_literal_cstr(relname), o.replicas);
	v = spi_value(sql.data, caller);
	if (v != NULL && strcmp(v, "ok") == 0)
	{
		commit_and_restart(caller);
		return;
	}

	/* 失败：删掉刚建的表（连同分片、已建的组），再报错 */
	commit_and_restart(caller);
	{
		char	   *why = v ? v : "（无返回）";

		initStringInfo(&sql);
		appendStringInfo(&sql, "DROP TABLE IF EXISTS %s", relname);
		SPI_connect();
		SPI_execute(sql.data, false, 0);
		SPI_finish();
		commit_and_restart(caller);
		(void) spi_value("SELECT partdist.shardpg_cleanup_dropped()", caller);
		commit_and_restart(caller);
		ereport(ERROR,
				(errmsg("建表 %s 没有完成，已删除：%s", relname, why),
				 errhint("看日志里 raft_replicate_table_shards_bulk / set_table_shard_mvcc 的报错；"
						 "处理后重新执行 CREATE TABLE。")));
	}
}

/* ---------------------------------------------------------------- 删表 */

static bool
shardpg_drop(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
			 ProcessUtilityContext context, ParamListInfo params,
			 QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc,
			 ShardpgRunChainFn run_chain, DropStmt *drop)
{
	MemoryContext caller = CurrentMemoryContext;
	StringInfoData oids;
	StringInfoData sql;
	ListCell   *lc;
	int			nrel = 0;
	char	   *v;
	bool		standalone;

	if (creating_extension ||
		!OidIsValid(get_relname_relid("pg_dist_shard", PG_CATALOG_NAMESPACE)) ||
		!OidIsValid(RangeVarGetRelid(makeRangeVar("partdist", "shardpg_drop_queue", -1),
									 NoLock, true)))
		return false;

	initStringInfo(&oids);
	foreach(lc, drop->objects)
	{
		RangeVar   *rv = makeRangeVarFromNameList((List *) lfirst(lc));
		Oid			relid = RangeVarGetRelid(rv, NoLock, true);

		if (!OidIsValid(relid))
			continue;
		appendStringInfo(&oids, "%s%u", nrel ? "," : "", relid);
		nrel++;
	}
	if (nrel == 0)
		return false;

	/* 有分片在控制面登记过的表，才归 ShardPG 管 */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT pg_catalog.count(*) FROM pg_catalog.pg_dist_shard s"
					 " WHERE s.logicalrelid = ANY ('{%s}'::pg_catalog.oid[])"
					 "   AND EXISTS (SELECT 1 FROM pg_catalog.pg_dist_shard s2"
					 "               JOIN partdist.partition_map m ON m.partition_id = s2.shardid"
					 "               WHERE s2.logicalrelid = s.logicalrelid)",
					 oids.data);
	v = spi_value(sql.data, caller);
	if (v == NULL || strcmp(v, "0") == 0)
		return false;
	if (!is_coordinator(caller))
		return false;

	/* ---- 事务 1：记队列 + 删表（同一事务，DROP 回滚则队列一起回滚） ---- */
	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "INSERT INTO partdist.shardpg_drop_queue (shardid, shard_name, rel_name)"
					 " SELECT s.shardid, pg_catalog.shard_name(s.logicalrelid, s.shardid),"
					 "        s.logicalrelid::pg_catalog.regclass::pg_catalog.text"
					 "   FROM pg_catalog.pg_dist_shard s"
					 "  WHERE s.logicalrelid = ANY ('{%s}'::pg_catalog.oid[])"
					 "    AND EXISTS (SELECT 1 FROM pg_catalog.pg_dist_shard s2"
					 "                JOIN partdist.partition_map m ON m.partition_id = s2.shardid"
					 "                WHERE s2.logicalrelid = s.logicalrelid)"
					 " ON CONFLICT (shardid) DO NOTHING",
					 oids.data);
	SPI_connect();
	if (SPI_execute(sql.data, false, 0) < 0)
		elog(ERROR, "shardpg: 记录待清理分片失败");
	SPI_finish();

	/* 路由先对齐控制面登记：组主漂移过时，DROP 会被发到已不是主的那台 */
	(void) spi_value("SELECT partdist.shardpg_reconcile_quiet()", caller);

	run_chain(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);

	standalone = (context == PROCESS_UTILITY_TOPLEVEL && !IsTransactionBlock());
	if (!standalone || !shardpg_drop_cleanup)
	{
		ereport(NOTICE,
				(errmsg("分片表的 Raft 组、副本表、主从登记待清理"),
				 errhint("提交后执行 SELECT shardpg.cleanup_dropped();")));
		return true;
	}

	commit_and_restart(caller);

	/* ---- 事务 2：清理 ---- */
	(void) spi_value("SELECT partdist.shardpg_cleanup_dropped()", caller);
	commit_and_restart(caller);
	return true;
}

/* ---------------------------------------------------------------- 入口 */

bool
ShardpgDdlIntercept(PlannedStmt *pstmt, const char *queryString,
					bool readOnlyTree, ProcessUtilityContext context,
					ParamListInfo params, QueryEnvironment *queryEnv,
					DestReceiver *dest, QueryCompletion *qc,
					ShardpgRunChainFn run_chain)
{
	Node	   *stmt = pstmt->utilityStmt;

	if (stmt == NULL)
		return false;

	if (IsA(stmt, CreateStmt))
	{
		ShardKeyOpts o;
		List	   *rest;

		if (!extract_shard_opts((CreateStmt *) stmt, &o, &rest))
			return false;
		if (o.column == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("SHARD KEY 缺少分片键列")));
		shardpg_create(pstmt, queryString, context, params, queryEnv, dest, qc,
					   run_chain, (CreateStmt *) stmt, &o);
		return true;
	}

	if (IsA(stmt, DropStmt) && ((DropStmt *) stmt)->removeType == OBJECT_TABLE)
		return shardpg_drop(pstmt, queryString, readOnlyTree, context, params,
							queryEnv, dest, qc, run_chain, (DropStmt *) stmt);

	return false;
}
