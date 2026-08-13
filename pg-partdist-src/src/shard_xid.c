/*
 * shard_xid.c — TX-TSO-MVCC P1：T1.1 白名单谓词 + T1.2 每分片发号器 +
 *               T1.3 事务绑定，合起来实现内核补丁 0005 的
 *               shard_relation_xid_hook（access/shard_stamp.h）。
 *
 * 设计出处：TX_TSO_MVCC_DESING.md §5、TX_TSO_MVCC_DEV_PLAN.md T1.1–T1.3、
 * P1_PRECHECK.md 结论 C/D。内核侧零新增补丁 —— 发号与绑定全在这里，
 * 钩子里惰性领取。
 */
#include "pg_partdist.h"
#include "shard_xid.h"
#include "shard_clog.h"
#include "shard_visibility.h"

#include <unistd.h>

#include "access/shard_stamp.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"
#include "utils/rel.h"

/*
 * 回卷护栏：P1 不处理 32 位分片 xid 回卷（设计 §7 推 P5），逼近上限直接
 * 拒绝发号，绝不静默绕回 0/1/2 保留区。
 */
#define SHARD_XID_HARD_LIMIT	((TransactionId) 0xFFFF0000)

/* ---- T1.1：GUC 白名单 ---- */

/*
 * 解析结果挂在 GUC 的 extra 上（guc_malloc 分配、GUC 机制负责释放/切换），
 * assign 钩子只换指针 —— 热路径读它无锁：每后端各持一份，SIGHUP 各自换。
 */
typedef struct ShardRelidsCfg
{
	int			n;
	Oid			relids[FLEXIBLE_ARRAY_MEMBER];	/* 升序去重 */
} ShardRelidsCfg;

static char *shard_relids_string = NULL;		/* GUC 原始串 */
static ShardRelidsCfg *shard_relids_cfg = NULL; /* 当前生效的解析结果 */

/* ---- T1.2：共享内存发号器 ---- */

typedef struct ShardXidSlot
{
	Oid			shard_relid;	/* InvalidOid = 空槽 */
	TransactionId next_xid;		/* 下一个待发 */
	TransactionId watermark;	/* 已持久化上界：所有已发号 < watermark */
} ShardXidSlot;

typedef struct ShardXidState
{
	LWLock	   *lock;			/* 罩全部槽位；水位落盘也在锁内（每 4096 号
								 * 才一次 fsync，见 SHARD_XID_BATCH） */
	ShardXidSlot slots[SHARD_XID_MAX_SLOTS];
} ShardXidState;

static ShardXidState *ShardXidCtl = NULL;

/* ---- T1.3：后端本地 事务↔分片 xid 映射 ---- */

static struct
{
	Oid			shard;
	TransactionId sxid;
}			xact_map[SHARD_XID_MAX_PER_XACT];
static int	xact_map_n = 0;

/*
 * 后端本地 TOAST→属主 映射（会话生存期）。
 *
 * 为什么需要：可见性钩子只拿得到 t_tableOid，没有 Relation，TOAST 表按名字
 * 归并属主的规约在那里用不上。而 TOAST 表并非只被 SatisfiesToast 读——
 * **toast 删除**（属主行 DELETE / UPDATE 换出旧值）走 heap_delete →
 * HeapTupleSatisfiesUpdate(TOAST 元组)，不分叉就会拿分片 xid 查原生 clog
 * 并把 hint 位写上 TOAST 页（T1.9 CP2 实测：redo 页面因此分叉）。
 * 好在 heap_delete 顶部的打标钩子必然先于 SatisfiesUpdate 执行且持有
 * Relation：谓词命中 TOAST 时顺手登记，之后同后端的 by-OID 查询即可命中。
 * 映射满了只是不缓存（fail 方向 = 漏分叉写 hint，pagecmp 会抓），P1 够用。
 */
#define SHARD_TOAST_MAP_MAX		32
static struct
{
	Oid			toast;
	Oid			owner;
}			toast_map[SHARD_TOAST_MAP_MAX];
static int	toast_map_n = 0;

static void
toast_map_note(Oid toast_oid, Oid owner)
{
	int			i;

	for (i = 0; i < toast_map_n; i++)
		if (toast_map[i].toast == toast_oid)
		{
			toast_map[i].owner = owner;
			return;
		}
	if (toast_map_n < SHARD_TOAST_MAP_MAX)
	{
		toast_map[toast_map_n].toast = toast_oid;
		toast_map[toast_map_n].owner = owner;
		toast_map_n++;
	}
}

/* ================= T1.1 白名单 ================= */

static int
oid_cmp_qsort(const void *a, const void *b)
{
	Oid			oa = *(const Oid *) a;
	Oid			ob = *(const Oid *) b;

	if (oa < ob)
		return -1;
	if (oa > ob)
		return 1;
	return 0;
}

static bool
oid_whitelisted(ShardRelidsCfg *cfg, Oid relid)
{
	int			lo = 0;
	int			hi = cfg->n - 1;

	while (lo <= hi)
	{
		int			mid = (lo + hi) / 2;

		if (cfg->relids[mid] == relid)
			return true;
		if (cfg->relids[mid] < relid)
			lo = mid + 1;
		else
			hi = mid - 1;
	}
	return false;
}

static bool
check_shard_relids(char **newval, void **extra, GucSource source)
{
	const char *p = (*newval != NULL) ? *newval : "";
	const char *q;
	int			max_n = 1;
	int			n = 0;
	ShardRelidsCfg *cfg;

	for (q = p; *q; q++)
		if (*q == ',')
			max_n++;

	cfg = (ShardRelidsCfg *)
		guc_malloc(LOG, offsetof(ShardRelidsCfg, relids) + max_n * sizeof(Oid));
	if (cfg == NULL)
		return false;

	while (*p)
	{
		char	   *end;
		unsigned long v;

		while (*p == ' ' || *p == '\t' || *p == ',')
			p++;
		if (*p == '\0')
			break;

		errno = 0;
		v = strtoul(p, &end, 10);
		if (end == p || errno != 0 || v == 0 || v > 0xFFFFFFFFUL)
		{
			GUC_check_errdetail("无法解析的表 OID：\"%s\"。", p);
			guc_free(cfg);
			return false;
		}
		p = end;
		while (*p == ' ' || *p == '\t')
			p++;
		if (*p != '\0' && *p != ',')
		{
			GUC_check_errdetail("OID 之间必须用逗号分隔：\"%s\"。", p);
			guc_free(cfg);
			return false;
		}
		cfg->relids[n++] = (Oid) v;
	}

	qsort(cfg->relids, n, sizeof(Oid), oid_cmp_qsort);
	{
		int			out = 0;
		int			i;

		for (i = 0; i < n; i++)
			if (out == 0 || cfg->relids[out - 1] != cfg->relids[i])
				cfg->relids[out++] = cfg->relids[i];
		cfg->n = out;
	}

	*extra = cfg;
	return true;
}

static void
assign_shard_relids(const char *newval, void *extra)
{
	shard_relids_cfg = (ShardRelidsCfg *) extra;
}

/*
 * T1.1 谓词。热路径（heap_insert/update/delete + heap_page_prune_opt 每次
 * 页访问）零 catalog 查询：白名单为空一次比较即返回；TOAST 归属靠
 * pg_toast_<owner> 命名规约从 relcache 已有字段解出，不查 pg_class。
 */
Oid
ShardXidRelidLookup(Relation relation)
{
	ShardRelidsCfg *cfg = shard_relids_cfg;
	Oid			relid;

	if (cfg == NULL || cfg->n == 0)
		return InvalidOid;

	relid = RelationGetRelid(relation);
	if (oid_whitelisted(cfg, relid))
		return relid;

	if (relation->rd_rel->relkind == RELKIND_TOASTVALUE)
	{
		unsigned int owner;
		char		trail;

		/* %c 拒绝尾随字符，确保整名匹配 pg_toast_<oid> */
		if (sscanf(RelationGetRelationName(relation),
				   "pg_toast_%u%c", &owner, &trail) == 1 &&
			oid_whitelisted(cfg, (Oid) owner))
		{
			/* 让可见性钩子的 by-OID 查询也认得这张 TOAST（见 toast_map 注释） */
			toast_map_note(relid, (Oid) owner);
			return (Oid) owner;
		}
	}

	return InvalidOid;
}

Oid
ShardXidLookupByOid(Oid reloid)
{
	ShardRelidsCfg *cfg = shard_relids_cfg;
	int			i;

	if (cfg == NULL || cfg->n == 0)
		return InvalidOid;
	if (oid_whitelisted(cfg, reloid))
		return reloid;
	/* TOAST：查后端登记的归属映射（属主须仍在名单内） */
	for (i = 0; i < toast_map_n; i++)
		if (toast_map[i].toast == reloid &&
			oid_whitelisted(cfg, toast_map[i].owner))
			return toast_map[i].owner;
	return InvalidOid;
}

/* ================= T1.2 发号器 ================= */

/*
 * 水位文件：$PGDATA/pg_shard_xid/<oid>，4 字节小端 uint32。
 * 崩溃语义：文件里的值是"已授权发放的上界"，重启从它续发 —— 最多跳
 * SHARD_XID_BATCH 个号，绝不重发（DEV PLAN T1.2 的"跳号无害"裁定）。
 */
static TransactionId
shard_xid_read_watermark(Oid shard)
{
	char		path[MAXPGPATH];
	int			fd;
	uint32		wm;
	int			r;

	snprintf(path, sizeof(path), SHARD_XID_DIR "/%u", shard);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return 0;			/* 该分片首次发号 */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法打开分片 xid 水位文件 \"%s\": %m", path)));
	}

	r = read(fd, &wm, sizeof(wm));
	if (r != sizeof(wm))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("分片 xid 水位文件 \"%s\" 损坏（读到 %d 字节）",
						path, r)));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法关闭分片 xid 水位文件 \"%s\": %m", path)));

	return (TransactionId) wm;
}

static void
shard_xid_persist_watermark(Oid shard, TransactionId wm)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	int			fd;
	uint32		v = (uint32) wm;

	if (MakePGDirectory(SHARD_XID_DIR) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法创建目录 \"%s\": %m", SHARD_XID_DIR)));

	snprintf(tmppath, sizeof(tmppath), SHARD_XID_DIR "/%u.tmp", shard);
	snprintf(path, sizeof(path), SHARD_XID_DIR "/%u", shard);

	fd = OpenTransientFile(tmppath, O_CREAT | O_TRUNC | O_WRONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法创建分片 xid 水位文件 \"%s\": %m", tmppath)));

	errno = 0;
	if (write(fd, &v, sizeof(v)) != sizeof(v))
	{
		if (errno == 0)
			errno = ENOSPC;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法写分片 xid 水位文件 \"%s\": %m", tmppath)));
	}
	if (pg_fsync(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法 fsync 分片 xid 水位文件 \"%s\": %m", tmppath)));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法关闭分片 xid 水位文件 \"%s\": %m", tmppath)));

	/* rename + 目录 fsync 一步到位；失败即 ERROR，槽位状态不推进 */
	durable_rename(tmppath, path, ERROR);
}

/*
 * 发一个号。锁内做水位落盘（每 SHARD_XID_BATCH 次分配才一次 fsync；
 * 且分配频率是"每事务每分片"而不是每元组 —— T1.3 的映射缓存住了）。
 * 任何 ERROR 都发生在槽位状态推进之前，fail-closed。
 */
static TransactionId
shard_xid_allocate(Oid shard)
{
	ShardXidSlot *slot = NULL;
	ShardXidSlot *free_slot = NULL;
	TransactionId result;
	int			i;

	if (ShardXidCtl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist 分片 xid 共享内存未初始化")));

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);

	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
	{
		if (ShardXidCtl->slots[i].shard_relid == shard)
		{
			slot = &ShardXidCtl->slots[i];
			break;
		}
		if (free_slot == NULL &&
			ShardXidCtl->slots[i].shard_relid == InvalidOid)
			free_slot = &ShardXidCtl->slots[i];
	}

	if (slot == NULL)
	{
		TransactionId wm;

		if (free_slot == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("分片 xid 槽位用尽（上限 %d）", SHARD_XID_MAX_SLOTS)));

		wm = shard_xid_read_watermark(shard);
		free_slot->next_xid = Max(wm, FIRST_SHARD_XID);
		free_slot->watermark = free_slot->next_xid;
		/* relid 最后置：上面 ERROR 的话槽位仍是空的 */
		free_slot->shard_relid = shard;
		slot = free_slot;
	}

	if (slot->next_xid >= SHARD_XID_HARD_LIMIT)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("分片 %u 的 32 位 xid 逼近上限 %u，P1 不支持回卷",
						shard, SHARD_XID_HARD_LIMIT)));

	if (slot->next_xid >= slot->watermark)
	{
		TransactionId new_wm = slot->next_xid + SHARD_XID_BATCH;

		shard_xid_persist_watermark(shard, new_wm);
		slot->watermark = new_wm;
	}

	result = slot->next_xid++;

	LWLockRelease(ShardXidCtl->lock);
	return result;
}

/* ================= T1.3 事务绑定 ================= */

static TransactionId
shard_xid_for_current_xact(Oid shard)
{
	TransactionId sxid;
	int			i;

	/* T1.8 守卫点（写路径）：strict 模式拦截 + SERIALIZABLE 禁令 */
	ShardAccessGate(shard, "写入");

	/*
	 * 子事务全面禁写（DEV PLAN T1.3）：整个事务只有一个分片 xid，SAVEPOINT
	 * 局部回滚无法表达 —— 已领过号的复用也不行，所以嵌套检查放在查表之前。
	 */
	if (GetCurrentTransactionNestLevel() > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("P1 不支持在子事务（SAVEPOINT/EXCEPTION 块）内写分片打标表"),
				 errdetail("分片 %u 的整事务只绑定一个分片 xid，无法表达子事务局部回滚。",
						   shard)));

	for (i = 0; i < xact_map_n; i++)
		if (xact_map[i].shard == shard)
			return xact_map[i].sxid;

	if (xact_map_n >= SHARD_XID_MAX_PER_XACT)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("单事务触达的分片数超过上限 %d", SHARD_XID_MAX_PER_XACT)));

	sxid = shard_xid_allocate(shard);

	/*
	 * T2.3：先落 clog RUNNING 账再进活跃表（都在 RegisterRunning 里，含
	 * 持有者原生 xid——此刻原生 xid 必已分配，0005 让 heap_* 顶部先
	 * GetCurrentTransactionId 再换号）。任一步失败则本次领号作废（跳号
	 * 无害），映射未推进。
	 */
	ShardCommitRegisterRunning(shard, sxid, GetCurrentTransactionId());

	xact_map[xact_map_n].shard = shard;
	xact_map[xact_map_n].sxid = sxid;
	xact_map_n++;

	return sxid;
}

TransactionId
ShardXidMineForShard(Oid shard)
{
	int			i;

	for (i = 0; i < xact_map_n; i++)
		if (xact_map[i].shard == shard)
			return xact_map[i].sxid;
	return InvalidTransactionId;
}

static void
shard_xid_xact_callback(XactEvent event, void *arg)
{
	int			i;

	switch (event)
	{
		case XACT_EVENT_PRE_PREPARE:
			/*
			 * P1 禁 2PC 含分片写：PREPARE 后由别的会话 COMMIT/ROLLBACK
			 * PREPARED，本后端映射已清、临时提交表条目会永挂 RUNNING。
			 * PRE_PREPARE 是最后一个还能安全 ERROR 的点。
			 * 含分片表 DROP 的事务同禁：挂起的 GC 无法跟去别的会话结算。
			 */
			if (xact_map_n > 0 || ShardClogHasPendingDrops())
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("P1 不支持对含分片打标表写入/删除的事务执行 PREPARE TRANSACTION")));
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			/* 先翻临时提交表（删除条目=提交点，P1 桩），再清映射 */
			for (i = 0; i < xact_map_n; i++)
				ShardCommitMarkEnded(xact_map[i].shard, xact_map[i].sxid, true);
			xact_map_n = 0;
			ShardClogAtCommit();	/* DROP TABLE 的文件 GC，提交才删 */
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			for (i = 0; i < xact_map_n; i++)
				ShardCommitMarkEnded(xact_map[i].shard, xact_map[i].sxid, false);
			xact_map_n = 0;
			ShardClogAtAbort();
			break;

		case XACT_EVENT_PREPARE:
			/* 有分片写/DROP 的事务在 PRE_PREPARE 已被拦，这里只会是空的 */
			xact_map_n = 0;
			ShardClogAtAbort();
			break;

		default:
			break;
	}
}

/* ================= 钩子与接线 ================= */

static bool
shard_relation_xid_impl(struct RelationData *relation, bool assign,
						TransactionId *sxid)
{
	Oid			shard = ShardXidRelidLookup((Relation) relation);

	if (!OidIsValid(shard))
		return false;
	if (assign)
		*sxid = shard_xid_for_current_xact(shard);
	return true;
}

void
ShardXidDefineGUCs(void)
{
	DefineCustomStringVariable(
		"pg_partdist.shard_relids",
		"P1 白名单：以分片 xid 打标 xmin/xmax 的表 OID 列表（逗号分隔）。",
		"空 = 停用打标（438 基线依赖此默认值，见 P1_PRECHECK 结论 C）。"
		"TOAST 表自动随属主。仅 P1 验收用，P2 起改由 partition_map 驱动。",
		&shard_relids_string,
		"",
		PGC_SIGHUP,
		0,
		check_shard_relids,
		assign_shard_relids,
		NULL);
}

void
RequestShardXidShmem(void)
{
	RequestAddinShmemSpace(MAXALIGN(sizeof(ShardXidState)));
	RequestNamedLWLockTranche("pg_partdist_shard_xid", 1);
}

void
ShardXidShmemInit(void)
{
	bool		found;

	ShardXidCtl = NULL;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	ShardXidCtl = ShmemInitStruct("pg_partdist_shard_xid",
								  sizeof(ShardXidState),
								  &found);
	if (!found)
	{
		memset(ShardXidCtl->slots, 0, sizeof(ShardXidCtl->slots));
		ShardXidCtl->lock =
			&GetNamedLWLockTranche("pg_partdist_shard_xid")[0].lock;
	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * 0007：把本事务的 (shard, sxid) 对交给 XactLogCommitRecord/AbortRecord
 * 写进记录体。**在临界区内被调**——只拷静态缓冲，不 palloc 不 ereport。
 */
static uint32 wal_pair_buf[2 * SHARD_XID_MAX_PER_XACT];

static int
shard_xact_wal_list_impl(uint32 **pairs)
{
	int			i;

	for (i = 0; i < xact_map_n; i++)
	{
		wal_pair_buf[2 * i] = (uint32) xact_map[i].shard;
		wal_pair_buf[2 * i + 1] = (uint32) xact_map[i].sxid;
	}
	*pairs = wal_pair_buf;
	return xact_map_n;
}

void
ShardXidInstallHook(void)
{
	shard_relation_xid_hook = shard_relation_xid_impl;
	shard_xact_wal_list_hook = shard_xact_wal_list_impl;
	shard_xact_redo_hook = ShardClogXactRedo;
	RegisterXactCallback(shard_xid_xact_callback, NULL);
}

/* ================= P1 数据保护拦截 ================= */

static void
shard_xid_guard_range_var(ShardRelidsCfg *cfg, RangeVar *rv, const char *cmd)
{
	Oid			relid;
	unsigned int owner;
	char		trail;

	if (rv == NULL)
		return;

	/* 直接点名 TOAST 表（VACUUM pg_toast.pg_toast_NNN）也拦 */
	if (rv->relname != NULL &&
		sscanf(rv->relname, "pg_toast_%u%c", &owner, &trail) == 1 &&
		oid_whitelisted(cfg, (Oid) owner))
		relid = (Oid) owner;
	else
		relid = RangeVarGetRelid(rv, NoLock, true);

	if (OidIsValid(relid) && oid_whitelisted(cfg, relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("P1: %s 不允许作用于分片打标表 \"%s\"",
						cmd, rv->relname),
				 errdetail("原生 clog 会误判分片 xid（P1_PRECHECK 结论 D），"
						   "回收/重写路径会删除活元组。")));
}

/*
 * VACUUM/ANALYZE/CLUSTER 拦截。白名单为空时零成本直接返回，438 基线不受
 * 影响。整库 VACUUM 在白名单非空时一律拒绝 —— 无法逐表甄别，fail-closed
 * （P1 白名单只出现在专用验收集群，代价可接受）。
 */
void
ShardXidUtilityGuard(Node *parsetree)
{
	ShardRelidsCfg *cfg = shard_relids_cfg;

	if (cfg == NULL || cfg->n == 0 || parsetree == NULL)
		return;

	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *stmt = (VacuumStmt *) parsetree;
		const char *cmd = stmt->is_vacuumcmd ? "VACUUM" : "ANALYZE";
		ListCell   *lc;

		if (stmt->rels == NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("P1: 分片打标白名单非空时不允许整库 %s", cmd),
					 errhint("请点名不含分片打标表的目标表。")));

		foreach(lc, stmt->rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);

			if (OidIsValid(vrel->oid) && oid_whitelisted(cfg, vrel->oid))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("P1: %s 不允许作用于分片打标表（OID %u）",
								cmd, vrel->oid)));
			shard_xid_guard_range_var(cfg, vrel->relation, cmd);
		}
	}
	else if (IsA(parsetree, ClusterStmt))
	{
		ClusterStmt *stmt = (ClusterStmt *) parsetree;

		shard_xid_guard_range_var(cfg, stmt->relation, "CLUSTER");
	}
	else if (IsA(parsetree, IndexStmt))
	{
		/*
		 * 索引构建用 HeapTupleSatisfiesVacuum 判活 —— 对分片 xid 是数据
		 * 损坏级误判（同结论 D）；且 P1 无行锁，索引维护路径也走不通。
		 * 白名单表在 P1 一律无索引（建表期的约束索引靠测试纪律保证）。
		 */
		shard_xid_guard_range_var(cfg, ((IndexStmt *) parsetree)->relation,
								  "CREATE INDEX");
	}
	else if (IsA(parsetree, ReindexStmt))
	{
		ReindexStmt *stmt = (ReindexStmt *) parsetree;

		if (stmt->relation != NULL)
			shard_xid_guard_range_var(cfg, stmt->relation, "REINDEX");
	}
	else if (IsA(parsetree, DropStmt))
	{
		/*
		 * DROP TABLE 分片打标表：登记提交时点的文件 GC（pg_shard_clog/<oid>
		 * + pg_shard_xid/<oid>，T2.1）。必须在标准 ProcessUtility 之前解析
		 * 名字 —— 执行后本事务内 catalog 里已经查不到了。级联删除（DROP
		 * SCHEMA ... CASCADE）不走这个分支，孤儿文件无害（shard_clog.h
		 * 已知边界）。
		 */
		DropStmt   *stmt = (DropStmt *) parsetree;

		if (stmt->removeType == OBJECT_TABLE)
		{
			ListCell   *lc;

			foreach(lc, stmt->objects)
			{
				RangeVar   *rv = makeRangeVarFromNameList((List *) lfirst(lc));
				Oid			relid = RangeVarGetRelid(rv, NoLock, true);

				if (OidIsValid(relid) && oid_whitelisted(cfg, relid))
					ShardClogRememberDrop(relid);
			}
		}
	}
}
