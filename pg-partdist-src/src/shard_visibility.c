/*
 * shard_visibility.c — TX-TSO-MVCC P1：T1.6 可见性临时桩 + T1.7 行锁反查 +
 *                      T1.8 安全网骨架，实现内核补丁 0006 的
 *                      shard_visibility_hooks（access/shard_stamp.h）。
 *
 * 设计出处：TX_TSO_MVCC_DESING.md §4、TX_TSO_MVCC_DEV_PLAN.md T1.6–T1.8。
 * 语义速查（P1 桩）：
 *   - xid 状态四值：我自己的（后端映射命中）/ RUNNING / ABORTED / 已提交
 *     （共享表缺席即提交 —— COMMIT 时删除条目，见 shard_visibility.h）。
 *   - 已提交即全局可见：P1 没有 commit_ts/start_ts，可见性没有时间点语义
 *     （近似 read-committed）；P2 在同一分叉里原位换成
 *     "status=COMMITTED 且 commit_ts < start_ts"。
 *   - 自见性按元组原始 cid 近似：同事务内"插入后又更新/删除同一行"会把
 *     cmin 覆盖成 cmax（内核 AdjustCmax 对分片 xid 不生成 combo cid），
 *     该场景 P1 记录为已知限制，验收用例避开。
 *   - 一律不读不写 hint 位（§4.5）。
 */
#include "pg_partdist.h"
#include "shard_xid.h"
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
#include "utils/snapshot.h"

/* ---- T1.8 GUC ---- */

int			shard_safety_mode = SHARD_SAFETY_PERMISSIVE;

static const struct config_enum_entry shard_safety_mode_options[] = {
	{"permissive", SHARD_SAFETY_PERMISSIVE, false},
	{"strict", SHARD_SAFETY_STRICT, false},
	{NULL, 0, false}
};

/* ---- 临时提交表（T1.6 桩 + T1.7 反查合一） ---- */

#define SHARD_ENTRY_RUNNING		1
#define SHARD_ENTRY_ABORTED		2

typedef struct ShardCommitKey
{
	Oid			shard;
	TransactionId sxid;
} ShardCommitKey;

typedef struct ShardCommitEntry
{
	ShardCommitKey key;
	uint8		status;			/* RUNNING / ABORTED */
	TransactionId native_xid;	/* RUNNING 持有者的原生 top xid（反查表） */
} ShardCommitEntry;

static HTAB *ShardCommitHash = NULL;
static LWLock *ShardCommitLock = NULL;

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
	SxidState	st;

	if (native_xid)
		*native_xid = InvalidTransactionId;

	if (ShardXidMineForShard(shard) == sxid)
		return SXID_MY_OWN;

	key.shard = shard;
	key.sxid = sxid;
	LWLockAcquire(ShardCommitLock, LW_SHARED);
	e = (ShardCommitEntry *) hash_search(ShardCommitHash, &key,
										 HASH_FIND, NULL);
	if (e == NULL)
		st = SXID_COMMITTED;	/* 缺席=已提交（P1 桩语义） */
	else if (e->status == SHARD_ENTRY_ABORTED)
		st = SXID_ABORTED;
	else
	{
		st = SXID_RUNNING;
		if (native_xid)
			*native_xid = e->native_xid;
	}
	LWLockRelease(ShardCommitLock);
	return st;
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
 * 重评（判定收敛在 sv_satisfies_update 里）。孤儿 RUNNING（持有者不在了条目
 * 还挂着）直接 ERROR —— P1 不做认领（§6 推 P2），报错好过忙等自旋。
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

	if (!TransactionIdIsValid(native) ||
		TransactionIdIsCurrentTransactionId(native))
		return;					/* 防御：自己等自己必死锁 */

	if (TransactionIdIsInProgress(native))
		XactLockTableWait(native, (Relation) relation, ctid, oper);

	if (shard_xid_state(shard, sxid, NULL) == SXID_RUNNING &&
		!TransactionIdIsInProgress(native))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("分片 %u 的 xid %u 是无主 RUNNING 条目（持有者原生 xid %u 已不在）",
						shard, sxid, native),
				 errdetail("P1 不支持无主条目认领（设计 §6.5 推 P2）。")));
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
				 errmsg("pg_partdist 临时提交表未初始化")));

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
				 errmsg("P1 临时提交表已满（上限 %d 条）",
						SHARD_COMMIT_MAX_ENTRIES),
				 errhint("重启集群清空；P2 分片 clog 落地后本表移除。")));
	}
	e->status = SHARD_ENTRY_RUNNING;
	e->native_xid = native_xid;
	LWLockRelease(ShardCommitLock);
}

void
ShardCommitMarkEnded(Oid shard, TransactionId sxid, bool committed)
{
	ShardCommitKey key;
	ShardCommitEntry *e;

	if (ShardCommitHash == NULL)
		return;

	key.shard = shard;
	key.sxid = sxid;
	LWLockAcquire(ShardCommitLock, LW_EXCLUSIVE);
	if (committed)
	{
		/* 删除即提交点（缺席=已提交，P1 桩） */
		if (hash_search(ShardCommitHash, &key, HASH_REMOVE, NULL) == NULL)
			elog(WARNING,
				 "pg_partdist: 提交时临时提交表缺 RUNNING 条目（分片 %u，xid %u）",
				 shard, sxid);
	}
	else
	{
		e = (ShardCommitEntry *) hash_search(ShardCommitHash, &key,
											 HASH_FIND, NULL);
		if (e != NULL)
			e->status = SHARD_ENTRY_ABORTED;
		else
			elog(WARNING,
				 "pg_partdist: 中止时临时提交表缺 RUNNING 条目（分片 %u，xid %u）",
				 shard, sxid);
	}
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
