/*
 * shard_visibility.c — TX-TSO-MVCC 可见性裁决（内核补丁 0006 的
 *                      shard_visibility_hooks 实现，access/shard_stamp.h）。
 *
 * 设计出处：TX_TSO_MVCC_DESING.md §4/§5.3、TX_TSO_MVCC_DEV_PLAN.md T1.6–T1.8
 * （分叉结构）+ T2.3（真相源换持久分片 clog）。
 *
 * 语义速查（T2.3 起）：
 *   - xid 状态四值：我自己的（后端映射命中）/ RUNNING / ABORTED / COMMITTED。
 *   - 裁决顺序：后端映射 → 共享内存**活跃表**（本次启动内 RUNNING 的事务，
 *     兼行锁反查）→ 后端**终局缓存** → **分片 clog**（真相源，pg_shard_clog）。
 *   - clog 全零/空洞 = RUNNING = 不可见 —— 崩溃后未决事务天然不可见（T2.4
 *     认领改判 ABORTED），P1 桩"缺席=已提交"的崩溃漏判在此消失。
 *   - COMMITTED/ABORTED 是终局态，后端缓存永不失效（DROP+OID 复用的极端
 *     场景由注册前清目录 + 实践上的后端换代覆盖，P2 已知边界）。
 *   - 仍无 ts：已提交即可见（近似 read-committed）；P3 原位换成
 *     "status=COMMITTED 且 commit_ts < start_ts"。
 *   - 自见性按元组原始 cid 近似：同事务内"插入后又更新/删除同一行"会把
 *     cmin 覆盖成 cmax（内核 AdjustCmax 对分片 xid 不生成 combo cid），
 *     记录为已知限制，验收用例避开。
 *   - 一律不读不写 hint 位（§4.5）。
 */
#include "pg_partdist.h"
#include "shard_xid.h"
#include "shard_clog.h"
#include "shard_visibility.h"

#include "access/htup_details.h"
#include "access/shard_stamp.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapshot.h"

/* ---- T1.8 GUC ---- */

int			shard_safety_mode = SHARD_SAFETY_PERMISSIVE;

static const struct config_enum_entry shard_safety_mode_options[] = {
	{"permissive", SHARD_SAFETY_PERMISSIVE, false},
	{"strict", SHARD_SAFETY_STRICT, false},
	{NULL, 0, false}
};

/* ---- 活跃表（本次启动内 RUNNING 的分片事务；兼 T1.7 行锁反查） ---- */

typedef struct ShardCommitKey
{
	Oid			shard;
	TransactionId sxid;
} ShardCommitKey;

typedef struct ShardCommitEntry
{
	ShardCommitKey key;
	TransactionId native_xid;	/* 持有者的原生 top xid（反查表） */
} ShardCommitEntry;

static HTAB *ShardCommitHash = NULL;
static LWLock *ShardCommitLock = NULL;

/* ---- 后端终局缓存（COMMITTED/ABORTED 不可变，读一次 clog 记住） ---- */

typedef struct ShardVerdictCacheEntry
{
	ShardCommitKey key;
	uint8		status;			/* TXN_COMMITTED / TXN_ABORTED */
} ShardVerdictCacheEntry;

static HTAB *verdict_cache = NULL;

static ShardVerdictCacheEntry *
verdict_cache_search(const ShardCommitKey *key, HASHACTION action)
{
	if (verdict_cache == NULL)
	{
		HASHCTL		ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ShardCommitKey);
		ctl.entrysize = sizeof(ShardVerdictCacheEntry);
		ctl.hcxt = TopMemoryContext;
		verdict_cache = hash_create("pg_partdist shard verdict cache",
									256, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	return (ShardVerdictCacheEntry *) hash_search(verdict_cache, key,
												  action, NULL);
}

/* ---- 内部：xid 状态判定 ---- */

typedef enum SxidState
{
	SXID_MY_OWN,
	SXID_RUNNING,
	SXID_COMMITTED,
	SXID_ABORTED
} SxidState;

static SxidState
shard_xid_state(Oid shard, TransactionId sxid, TransactionId *native_xid)
{
	ShardCommitKey key;
	ShardCommitEntry *e;
	ShardVerdictCacheEntry *ce;
	TxnStatus	st;

	if (native_xid)
		*native_xid = InvalidTransactionId;

	/* 0/1/2 保留号按原生语义恒可见（Frozen/Bootstrap）——防御，正常不出现 */
	if (sxid < FirstNormalTransactionId)
		return SXID_COMMITTED;

	if (ShardXidMineForShard(shard) == sxid)
		return SXID_MY_OWN;

	key.shard = shard;
	key.sxid = sxid;

	/* 活跃表：本次启动内 RUNNING 的都在这，命中即免文件 I/O */
	LWLockAcquire(ShardCommitLock, LW_SHARED);
	e = (ShardCommitEntry *) hash_search(ShardCommitHash, &key,
										 HASH_FIND, NULL);
	if (e != NULL)
	{
		if (native_xid)
			*native_xid = e->native_xid;
		LWLockRelease(ShardCommitLock);
		return SXID_RUNNING;
	}
	LWLockRelease(ShardCommitLock);

	/* 终局缓存 */
	ce = verdict_cache_search(&key, HASH_FIND);
	if (ce != NULL)
		return (ce->status == TXN_COMMITTED) ? SXID_COMMITTED : SXID_ABORTED;

	/* 分片 clog（真相源）。全零/空洞 = RUNNING = 未决不可见（§5.3）。 */
	st = ShardClogReadStatus(shard, sxid);
	switch (st)
	{
		case TXN_COMMITTED:
		case TXN_ABORTED:
			ce = verdict_cache_search(&key, HASH_ENTER);
			ce->status = (uint8) st;
			return (st == TXN_COMMITTED) ? SXID_COMMITTED : SXID_ABORTED;

		case TXN_PREPARED:
			/* P4 之前不该出现；按 §4.2"未决=不可见、读者不阻塞"处理 */
		case TXN_RUNNING:
		default:
			/*
			 * clog RUNNING 且不在活跃表：要么是崩溃遗留的无主事务（T2.4
			 * 认领改判 ABORTED），要么理论上的落账竞态窗——两者按未决处理
			 * 都正确（不可见）。native_xid 无从给出，等待路径自行处置。
			 */
			return SXID_RUNNING;
	}
}

/* ---- T1.8 守卫点 ---- */

void
ShardAccessGate(Oid shard, const char *what)
{
	if (shard_safety_mode == SHARD_SAFETY_STRICT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("安全网严格模式：分片打标表（OID %u）的%s被拦截",
						shard, what),
				 errdetail("P3 接入 TSO 后严格语义 = 无 start_ts 读 / 无 gxid "
						   "写才拦；当前访问两者皆无（§9.2 第 1 层）。")));

	if (IsolationIsSerializable())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("P1 不支持 SERIALIZABLE 隔离级别访问分片打标表（OID %u）",
						shard)));
}

/* ---- 补丁 0006 钩子实现 ---- */

static bool
sv_is_shard_rel(Oid tableOid)
{
	return OidIsValid(ShardXidLookupByOid(tableOid));
}

static bool
sv_satisfies_mvcc(HeapTuple htup, Snapshot snapshot, Buffer buffer,
				  bool *visible)
{
	HeapTupleHeader tuple = htup->t_data;
	Oid			shard = ShardXidLookupByOid(htup->t_tableOid);

	if (!OidIsValid(shard))
		return false;

	ShardAccessGate(shard, "MVCC 读");

	switch (shard_xid_state(shard, HeapTupleHeaderGetRawXmin(tuple), NULL))
	{
		case SXID_MY_OWN:
			/* 本命令开始之后才插入的行不可见（cid 近似，见文件头） */
			if (HeapTupleHeaderGetRawCommandId(tuple) >= snapshot->curcid)
			{
				*visible = false;
				return true;
			}
			break;
		case SXID_RUNNING:
		case SXID_ABORTED:
			*visible = false;
			return true;
		case SXID_COMMITTED:
			/* P1 无 ts：已提交即可见；P2 换 commit_ts < start_ts */
			break;
	}

	if ((tuple->t_infomask & HEAP_XMAX_INVALID) ||
		!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
	{
		*visible = true;
		return true;
	}

	switch (shard_xid_state(shard, HeapTupleHeaderGetRawXmax(tuple), NULL))
	{
		case SXID_MY_OWN:
			/* 本命令开始之后才删除 ⇒ 本命令仍看得见 */
			*visible = (HeapTupleHeaderGetRawCommandId(tuple) >=
						snapshot->curcid);
			break;
		case SXID_RUNNING:
		case SXID_ABORTED:
			*visible = true;
			break;
		case SXID_COMMITTED:
			*visible = false;
			break;
	}
	return true;
}

static bool
sv_satisfies_self(HeapTuple htup, Buffer buffer, bool *visible)
{
	HeapTupleHeader tuple = htup->t_data;
	Oid			shard = ShardXidLookupByOid(htup->t_tableOid);

	if (!OidIsValid(shard))
		return false;

	ShardAccessGate(shard, "Self 读");

	switch (shard_xid_state(shard, HeapTupleHeaderGetRawXmin(tuple), NULL))
	{
		case SXID_MY_OWN:
			break;				/* 自己的插入立即可见（不看 cid） */
		case SXID_RUNNING:
		case SXID_ABORTED:
			*visible = false;
			return true;
		case SXID_COMMITTED:
			break;
	}

	if ((tuple->t_infomask & HEAP_XMAX_INVALID) ||
		!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
	{
		*visible = true;
		return true;
	}

	switch (shard_xid_state(shard, HeapTupleHeaderGetRawXmax(tuple), NULL))
	{
		case SXID_MY_OWN:
			*visible = false;	/* 自己的删除立即生效 */
			break;
		case SXID_RUNNING:
		case SXID_ABORTED:
			*visible = true;
			break;
		case SXID_COMMITTED:
			*visible = false;
			break;
	}
	return true;
}

static bool
sv_satisfies_dirty(HeapTuple htup, Snapshot snapshot, Buffer buffer,
				   bool *visible)
{
	HeapTupleHeader tuple = htup->t_data;
	Oid			shard = ShardXidLookupByOid(htup->t_tableOid);
	TransactionId native;

	if (!OidIsValid(shard))
		return false;

	ShardAccessGate(shard, "Dirty 读");

	/* 调用方约定：入口已把 snapshot->xmin/xmax 置 Invalid（内核分叉点保证） */

	switch (shard_xid_state(shard, HeapTupleHeaderGetRawXmin(tuple), &native))
	{
		case SXID_MY_OWN:
			break;
		case SXID_RUNNING:
			/* 镜像原生语义：报告"插入进行中"，xmin 给原生持有者供调用方等待 */
			snapshot->xmin = native;
			*visible = true;
			return true;
		case SXID_ABORTED:
			*visible = false;
			return true;
		case SXID_COMMITTED:
			break;
	}

	if ((tuple->t_infomask & HEAP_XMAX_INVALID) ||
		!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
	{
		*visible = true;
		return true;
	}

	switch (shard_xid_state(shard, HeapTupleHeaderGetRawXmax(tuple), &native))
	{
		case SXID_MY_OWN:
			*visible = false;
			break;
		case SXID_RUNNING:
			snapshot->xmax = native;
			*visible = true;
			break;
		case SXID_ABORTED:
			*visible = true;
			break;
		case SXID_COMMITTED:
			*visible = false;
			break;
	}
	return true;
}

static bool
sv_satisfies_update(HeapTuple htup, CommandId curcid, Buffer buffer,
					TM_Result *result)
{
	HeapTupleHeader tuple = htup->t_data;
	Oid			shard = ShardXidLookupByOid(htup->t_tableOid);

	if (!OidIsValid(shard))
		return false;

	ShardAccessGate(shard, "更新判定");

	switch (shard_xid_state(shard, HeapTupleHeaderGetRawXmin(tuple), NULL))
	{
		case SXID_MY_OWN:
			if (HeapTupleHeaderGetRawCommandId(tuple) >= curcid)
			{
				*result = TM_Invisible; /* 本命令内才插入 */
				return true;
			}
			break;
		case SXID_RUNNING:
		case SXID_ABORTED:
			*result = TM_Invisible;
			return true;
		case SXID_COMMITTED:
			break;
	}

	if ((tuple->t_infomask & HEAP_XMAX_INVALID) ||
		!TransactionIdIsValid(HeapTupleHeaderGetRawXmax(tuple)))
	{
		*result = TM_Ok;
		return true;
	}

	switch (shard_xid_state(shard, HeapTupleHeaderGetRawXmax(tuple), NULL))
	{
		case SXID_MY_OWN:
			/* 本命令删的 = SelfModified；更早命令删的 = 不可见 */
			*result = (HeapTupleHeaderGetRawCommandId(tuple) >= curcid) ?
				TM_SelfModified : TM_Invisible;
			break;
		case SXID_RUNNING:
			*result = TM_BeingModified;
			break;
		case SXID_ABORTED:
			*result = TM_Ok;
			break;
		case SXID_COMMITTED:
			*result = !ItemPointerEquals(&htup->t_self, &tuple->t_ctid) ?
				TM_Updated : TM_Deleted;
			break;
	}
	return true;
}

/*
 * T1.7：heap_delete/heap_update 冲突路径的等待翻译。返回后内核 goto l1/l2
 * 重评（判定收敛在 sv_satisfies_update 里）。孤儿 RUNNING（clog 未决但活跃
 * 表无持有者）直接 ERROR —— 静默返回会让调用方 BeingModified→等待→重评
 * 无限自旋；T2.4 认领落地后此路径改为触发认领。
 */
static void
sv_xmax_wait(struct RelationData *relation, TransactionId sxid,
			 ItemPointer ctid, XLTW_Oper oper)
{
	Oid			shard = ShardXidRelidLookup((Relation) relation);
	TransactionId native;

	if (!OidIsValid(shard))
		return;

	if (shard_xid_state(shard, sxid, &native) != SXID_RUNNING)
		return;					/* 已结束，重评即可 */

	if (!TransactionIdIsValid(native))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("分片 %u 的 xid %u 是无主 RUNNING（clog 未决且活跃表无持有者）",
						shard, sxid),
				 errdetail("崩溃遗留的未决事务待认领改判 ABORTED（T2.4）；"
						   "报错好过忙等自旋。")));

	if (TransactionIdIsCurrentTransactionId(native))
		return;					/* 防御：自己等自己必死锁 */

	if (TransactionIdIsInProgress(native))
		XactLockTableWait(native, (Relation) relation, ctid, oper);

	if (shard_xid_state(shard, sxid, NULL) == SXID_RUNNING &&
		!TransactionIdIsInProgress(native))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("分片 %u 的 xid %u 是无主 RUNNING 条目（持有者原生 xid %u 已不在）",
						shard, sxid, native),
				 errdetail("持有者后端异常消亡；等 T2.4 认领或重启集群。")));
}

static const ShardVisibilityHooks sv_hooks = {
	sv_is_shard_rel,
	sv_satisfies_mvcc,
	sv_satisfies_self,
	sv_satisfies_dirty,
	sv_satisfies_update,
	sv_xmax_wait
};

/* ---- 登记 / 结束标记 ---- */

void
ShardCommitRegisterRunning(Oid shard, TransactionId sxid,
						   TransactionId native_xid)
{
	ShardCommitKey key;
	ShardCommitEntry *e;

	if (ShardCommitHash == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist 分片活跃表未初始化")));

	/*
	 * T2.3：先落 clog RUNNING 账（§5.3"首写落账"），再进活跃表。落账失败则
	 * 领号作废（跳号无害），fail-closed。此刻 sxid 尚未写进任何元组，读者
	 * 不可能查到它 —— 两步之间无竞态窗。
	 */
	ShardClogSetRunning(shard, sxid);

	key.shard = shard;
	key.sxid = sxid;
	LWLockAcquire(ShardCommitLock, LW_EXCLUSIVE);
	e = (ShardCommitEntry *) hash_search(ShardCommitHash, &key,
										 HASH_ENTER_NULL, NULL);
	if (e == NULL)
	{
		LWLockRelease(ShardCommitLock);
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("分片活跃表已满（上限 %d 条）",
						SHARD_COMMIT_MAX_ENTRIES),
				 errhint("活跃分片事务数不该有这个量级；查泄漏。")));
	}
	e->native_xid = native_xid;
	LWLockRelease(ShardCommitLock);
}

/*
 * 事务结束：先写 clog 终局判决（真相源，判决写自带 fsync），再摘活跃表条目。
 * 顺序不能反 —— 反过来会出现"两处都查不到"的窗口，读者会把已提交事务当
 * RUNNING 处理还好，把已中止的当 RUNNING 也还好（都不可见），但等待路径会
 * 撞无主 ERROR；正序只有"活跃表仍命中"的瞬时窗，语义无害。
 *
 * 在 COMMIT/ABORT 回调上下文执行：此刻事务结局已成事实，判决写失败没有
 * 可用的 ERROR 语义（提交后 ERROR 会引发对已提交事务的递归中止）——升
 * PANIC 借崩溃恢复走 0007 redo 重做落账，失败方式正确。
 */
void
ShardCommitMarkEnded(Oid shard, TransactionId sxid, bool committed)
{
	ShardCommitKey key;

	if (ShardCommitHash == NULL)
		return;

	PG_TRY();
	{
		ShardClogSetVerdict(shard, sxid, committed);
	}
	PG_CATCH();
	{
		ereport(PANIC,
				(errmsg("pg_partdist: 分片 clog 判决写入失败（分片 %u，xid %u，%s），"
						"崩溃恢复将由补丁 0007 redo 补齐",
						shard, sxid, committed ? "COMMITTED" : "ABORTED")));
	}
	PG_END_TRY();

	key.shard = shard;
	key.sxid = sxid;
	LWLockAcquire(ShardCommitLock, LW_EXCLUSIVE);
	if (hash_search(ShardCommitHash, &key, HASH_REMOVE, NULL) == NULL)
		elog(WARNING,
			 "pg_partdist: 事务结束时活跃表缺条目（分片 %u，xid %u）",
			 shard, sxid);
	LWLockRelease(ShardCommitLock);
}

/* ---- 接线 ---- */

void
ShardVisibilityDefineGUCs(void)
{
	DefineCustomEnumVariable(
		"pg_partdist.shard_safety_mode",
		"分片打标表访问的安全网模式（§9.2 第 1 层）。",
		"permissive = 放行（P1 默认）；strict = 分片表一切读写拦截"
		"（验证守卫点位置用；P3 接 TSO 后语义收紧为无 start_ts/gxid 才拦）。",
		&shard_safety_mode,
		SHARD_SAFETY_PERMISSIVE,
		shard_safety_mode_options,
		PGC_SUSET,
		0,
		NULL, NULL, NULL);
}

void
RequestShardCommitShmem(void)
{
	RequestAddinShmemSpace(hash_estimate_size(SHARD_COMMIT_MAX_ENTRIES,
											  sizeof(ShardCommitEntry)));
	RequestNamedLWLockTranche("pg_partdist_shard_commit", 1);
}

void
ShardCommitShmemInit(void)
{
	HASHCTL		ctl;

	ShardCommitHash = NULL;
	ShardCommitLock = NULL;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ShardCommitKey);
	ctl.entrysize = sizeof(ShardCommitEntry);
	ShardCommitHash = ShmemInitHash("pg_partdist_shard_commit",
									SHARD_COMMIT_MAX_ENTRIES,
									SHARD_COMMIT_MAX_ENTRIES,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
	ShardCommitLock = &GetNamedLWLockTranche("pg_partdist_shard_commit")[0].lock;
}

void
ShardVisibilityInstallHooks(void)
{
	shard_visibility_hooks = &sv_hooks;
}
