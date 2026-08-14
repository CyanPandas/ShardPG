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
#include "dtx_pending.h"
#include "tso.h"
#include "shard_visibility.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/shard_stamp.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "fmgr.h"
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
	TransactionId claim_wm;		/* T2.4 认领水位：< 它的历史号已全部认领过 */
} ShardXidSlot;

/*
 * T2.4 不变式：**槽位存在 ⇒ 该分片本次启动已完成无主 RUNNING 认领**。
 * 建槽统一走 shard_xid_slot_attach()——先按"冻结的启动恢复上限"（挂槽前
 * 读到的文件 alloc_wm，此刻本分片必无任何新号已发）认领 [claim_wm, 上限)，
 * 再挂槽。发号路径与可见性读路径（ShardXidEnsureClaimed）共用，双入口
 * 由此归一（P2_PRECHECK 结论四；上限取动态水位会误杀新活事务，R-P2-2）。
 */

/*
 * T2.5 WAL 影子推进：崩溃恢复期 startup 进程从 0007 commit/abort 记录体的
 * (分片, 分片xid) 对里累计每分片"已见最大号+1"。与发号器槽位分开存——
 * 槽位有"存在=已认领"不变式（T2.4），startup 不建槽只记影子。
 */
typedef struct ShardXidShadow
{
	Oid			shard_relid;	/* InvalidOid = 空 */
	TransactionId next_hint;	/* redo 已见最大分片 xid + 1 */
} ShardXidShadow;

typedef struct ShardXidState
{
	LWLock	   *lock;			/* 罩全部槽位；水位落盘也在锁内（每 4096 号
								 * 才一次 fsync，见 SHARD_XID_BATCH） */
	ShardXidSlot slots[SHARD_XID_MAX_SLOTS];
	ShardXidShadow shadow[SHARD_XID_MAX_SLOTS];

	/*
	 * T2.7 partition_map 驱动门控：已登记打标的表 OID 集合。真相 =
	 * partition_map.shard_mvcc 列（注册函数写）；本集合是运行时判定用的
	 * 影子，重启时由 ShardXidShmemInit 扫 pg_shard_xid/ 目录重建（注册即
	 * 预创建水位文件，目录就是启动登记表——不引入第二份持久结构）。
	 * mvcc_n 作无锁快门：0 = 全库无登记表，438 基线路径只多一次整型读。
	 */
	int			mvcc_n;
	Oid			mvcc_set[SHARD_XID_MAX_SLOTS];
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

static bool oid_whitelisted(ShardRelidsCfg *cfg, Oid relid);

/* ---- T2.7：partition_map 驱动门控（mvcc 集合） ---- */

static bool
shard_mvcc_set_contains(Oid relid)
{
	int			n;
	int			i;
	bool		found = false;

	if (ShardXidCtl == NULL)
		return false;
	n = ShardXidCtl->mvcc_n;	/* 无锁快门（对齐 int 读；写侧持锁递增） */
	if (n == 0)
		return false;

	LWLockAcquire(ShardXidCtl->lock, LW_SHARED);
	for (i = 0; i < ShardXidCtl->mvcc_n; i++)
		if (ShardXidCtl->mvcc_set[i] == relid)
		{
			found = true;
			break;
		}
	LWLockRelease(ShardXidCtl->lock);
	return found;
}

/* 门控是否开着（GUC 名单非空 或 有登记表）——各入口的零成本早退条件 */
static inline bool
shard_gating_active(ShardRelidsCfg *cfg)
{
	return (cfg != NULL && cfg->n > 0) ||
		(ShardXidCtl != NULL && ShardXidCtl->mvcc_n > 0);
}

/* 统一谓词：GUC 白名单（测试通道）∪ partition_map 登记（正道），取并集 */
static bool
shard_oid_is_mvcc(ShardRelidsCfg *cfg, Oid relid)
{
	if (cfg != NULL && cfg->n > 0 && oid_whitelisted(cfg, relid))
		return true;
	return shard_mvcc_set_contains(relid);
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

	if (!shard_gating_active(cfg))
		return InvalidOid;

	relid = RelationGetRelid(relation);
	if (shard_oid_is_mvcc(cfg, relid))
		return relid;

	if (relation->rd_rel->relkind == RELKIND_TOASTVALUE)
	{
		unsigned int owner;
		char		trail;

		/* %c 拒绝尾随字符，确保整名匹配 pg_toast_<oid> */
		if (sscanf(RelationGetRelationName(relation),
				   "pg_toast_%u%c", &owner, &trail) == 1 &&
			shard_oid_is_mvcc(cfg, (Oid) owner))
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

	if (!shard_gating_active(cfg))
		return InvalidOid;
	if (shard_oid_is_mvcc(cfg, reloid))
		return reloid;
	/* TOAST：查后端登记的归属映射（属主须仍在名单内） */
	for (i = 0; i < toast_map_n; i++)
		if (toast_map[i].toast == reloid &&
			shard_oid_is_mvcc(cfg, toast_map[i].owner))
			return toast_map[i].owner;
	return InvalidOid;
}

/* ================= T1.2 发号器 ================= */

/*
 * 水位文件：$PGDATA/pg_shard_xid/<oid>。
 * T2.4 起 8 字节小端 {uint32 alloc_wm, uint32 claim_wm}；兼容读 4 字节旧
 * 格式（缺 claim_wm 按 FIRST_SHARD_XID = 全量补认领，安全方向）。
 * 崩溃语义：alloc_wm 是"已授权发放的上界"，重启从它续发 —— 最多跳
 * SHARD_XID_BATCH 个号，绝不重发（DEV PLAN T1.2 的"跳号无害"裁定）；
 * claim_wm 之下的历史号已全部认领过（RUNNING 已改判 ABORTED）。
 */
static void
shard_xid_read_wm_file(Oid shard, TransactionId *alloc_wm,
					   TransactionId *claim_wm)
{
	char		path[MAXPGPATH];
	int			fd;
	uint32		v[2];
	int			r;

	*alloc_wm = 0;
	*claim_wm = 0;

	snprintf(path, sizeof(path), SHARD_XID_DIR "/%u", shard);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
	{
		if (errno == ENOENT)
			return;				/* 该分片从未发过号 */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法打开分片 xid 水位文件 \"%s\": %m", path)));
	}

	r = read(fd, v, sizeof(v));
	if (r == (int) sizeof(v))
	{
		*alloc_wm = (TransactionId) v[0];
		*claim_wm = (TransactionId) v[1];
	}
	else if (r == (int) sizeof(uint32))
	{
		/* 旧 4 字节格式：认领水位按最低值，触发全量补认领 */
		*alloc_wm = (TransactionId) v[0];
		*claim_wm = FIRST_SHARD_XID;
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("分片 xid 水位文件 \"%s\" 损坏（读到 %d 字节）",
						path, r)));
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("无法关闭分片 xid 水位文件 \"%s\": %m", path)));
}

static void
shard_xid_persist_watermark(Oid shard, TransactionId alloc_wm,
							TransactionId claim_wm)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	int			fd;
	uint32		v[2];

	v[0] = (uint32) alloc_wm;
	v[1] = (uint32) claim_wm;

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
 * 找到或建立分片槽位（须持 ShardXidCtl->lock 排它锁调用）。建槽时完成
 * T2.4 认领：认领上限 = 此刻文件里的 alloc_wm ——"冻结的启动恢复上限"，
 * 挂槽前本分片必无任何新号已发（发号必先经本函数），新活事务永不进认领
 * 范围（R-P2-2）。认领或落盘失败即 ERROR，relid 未置、槽位仍空，fail-closed。
 * nclaimed 非 NULL 时返回本次改判条数（已有槽位 = 0）。
 */
static ShardXidSlot *
shard_xid_slot_attach(Oid shard, int *nclaimed)
{
	ShardXidSlot *free_slot = NULL;
	TransactionId alloc_wm;
	TransactionId claim_wm;
	TransactionId ceiling;
	int			i;

	if (nclaimed)
		*nclaimed = 0;

	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
	{
		if (ShardXidCtl->slots[i].shard_relid == shard)
			return &ShardXidCtl->slots[i];
		if (free_slot == NULL &&
			ShardXidCtl->slots[i].shard_relid == InvalidOid)
			free_slot = &ShardXidCtl->slots[i];
	}

	if (free_slot == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("分片 xid 槽位用尽（上限 %d）", SHARD_XID_MAX_SLOTS)));

	shard_xid_read_wm_file(shard, &alloc_wm, &claim_wm);
	ceiling = Max(alloc_wm, FIRST_SHARD_XID);
	claim_wm = Max(claim_wm, FIRST_SHARD_XID);

	/*
	 * T2.5：WAL 影子推进兜底——水位文件缺失/落后时，本次恢复窗口内见过的
	 * 最大分片 xid + 1 仍抬高起点，已完成事务的号绝不重发（窗口外的有判决
	 * 兜底：发号路径的终局槽跳过守卫）。文件健在时影子 ≤ 文件，取 Max 无扰。
	 */
	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
		if (ShardXidCtl->shadow[i].shard_relid == shard)
		{
			ceiling = Max(ceiling, ShardXidCtl->shadow[i].next_hint);
			break;
		}

	if (claim_wm < ceiling)
	{
		int			n = ShardClogClaimRange(shard, claim_wm, ceiling);

		/*
		 * 认领水位推到上限并落盘。alloc 位同步抬到 ceiling——文件缺失而
		 * 影子抬了上限时，写回 {0, ceiling} 会让下次重启退化（claim_wm >
		 * alloc_wm 的畸形档），抬高只多跳号、方向安全。
		 */
		shard_xid_persist_watermark(shard, Max(alloc_wm, ceiling), ceiling);
		if (nclaimed)
			*nclaimed = n;
	}

	free_slot->next_xid = ceiling;
	free_slot->watermark = free_slot->next_xid;
	free_slot->claim_wm = ceiling;
	/* relid 最后置：上面 ERROR 的话槽位仍是空的 */
	free_slot->shard_relid = shard;
	return free_slot;
}

/*
 * 发一个号。锁内做水位落盘（每 SHARD_XID_BATCH 次分配才一次 fsync；
 * 且分配频率是"每事务每分片"而不是每元组 —— T1.3 的映射缓存住了）。
 * 任何 ERROR 都发生在槽位状态推进之前，fail-closed。
 */
static TransactionId
shard_xid_allocate(Oid shard)
{
	ShardXidSlot *slot;
	TransactionId result;

	if (ShardXidCtl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist 分片 xid 共享内存未初始化")));

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);

	slot = shard_xid_slot_attach(shard, NULL);

	for (;;)
	{
		if (slot->next_xid >= SHARD_XID_HARD_LIMIT)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("分片 %u 的 32 位 xid 逼近上限 %u，P1 不支持回卷",
							shard, SHARD_XID_HARD_LIMIT)));

		if (slot->next_xid >= slot->watermark)
		{
			TransactionId new_wm = slot->next_xid + SHARD_XID_BATCH;

			shard_xid_persist_watermark(shard, new_wm, slot->claim_wm);
			slot->watermark = new_wm;
		}

		result = slot->next_xid++;

		/*
		 * T2.5 终局槽跳过守卫：clog 里已有判决的号 = 某段历史已经用过它
		 * （水位文件缺失/落后才会走到），跳过绝不重发——重发会让新事务的
		 * 结局"复活"同号历史元组。正常路径槽位是洞（RUNNING），一次通过；
		 * 每次分配多一次 pread、无 fsync，可接受。
		 */
		if (ShardClogReadStatus(shard, result) == TXN_RUNNING)
			break;
	}

	LWLockRelease(ShardXidCtl->lock);
	return result;
}

/*
 * T2.5：恢复期由 0007 redo 钩子喂入 (分片, 分片xid)，累计每分片影子推进。
 * startup 进程调用；只记影子不建槽（T2.4"槽位存在=已认领"不变式）。影子
 * 数组满则静默丢弃——影子是优化性兜底，缺了退化为文件水位语义，不伤正确性
 * （终局槽跳过守卫仍兜底不重号）。
 */
void
ShardXidRedoAdvance(Oid shard, TransactionId sxid)
{
	int			i;
	int			free_i = -1;

	if (ShardXidCtl == NULL)
		return;

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);
	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
	{
		if (ShardXidCtl->shadow[i].shard_relid == shard)
		{
			ShardXidCtl->shadow[i].next_hint =
				Max(ShardXidCtl->shadow[i].next_hint, sxid + 1);
			LWLockRelease(ShardXidCtl->lock);
			return;
		}
		if (free_i < 0 && ShardXidCtl->shadow[i].shard_relid == InvalidOid)
			free_i = i;
	}
	if (free_i >= 0)
	{
		ShardXidCtl->shadow[free_i].next_hint = sxid + 1;
		ShardXidCtl->shadow[free_i].shard_relid = shard;
	}
	LWLockRelease(ShardXidCtl->lock);
}

/*
 * T2.4：确保某分片本次启动已完成无主 RUNNING 认领（可见性读路径入口）。
 * 槽位存在即已认领（不变式），快路径共享锁一次扫描。返回本次改判条数。
 */
int
ShardXidEnsureClaimed(Oid shard)
{
	int			nclaimed = 0;
	bool		found = false;
	int			i;

	if (ShardXidCtl == NULL)
		return 0;				/* shmem 未起（防御） */

	LWLockAcquire(ShardXidCtl->lock, LW_SHARED);
	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
		if (ShardXidCtl->slots[i].shard_relid == shard)
		{
			found = true;
			break;
		}
	LWLockRelease(ShardXidCtl->lock);
	if (found)
		return 0;

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);
	(void) shard_xid_slot_attach(shard, &nclaimed);
	LWLockRelease(ShardXidCtl->lock);
	return nclaimed;
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
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			/*
			 * T3.2：含分片写的事务在此取 commit_ts 暂存（临界区外、提交
			 * 记录前 = 合法窗口内尽晚，P3_PRECHECK 结论三）。TSO 不可达
			 * 在这里 ERROR = 事务干净中止（R-P3-1 第一道防线）。遗留模式
			 * （tso_conninfo 空）内部自跳过。
			 */
			if (xact_map_n > 0)
				TsoStashCommitTs();
			break;

		case XACT_EVENT_PRE_PREPARE:
			/*
			 * T4.3 放行条件：已 join 全局事务（gxid 在手）的分片写允许
			 * PREPARE——PREPARED 落账与 2PC 段注册在 at_prepare 钩子里做
			 * （StartPrepare 之后）。未 join 的分片写维持 P1 禁令；含分片
			 * 表 DROP 的事务仍禁（挂起 GC 无法跨会话结算）。
			 */
			if (ShardClogHasPendingDrops())
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("不支持对含分片打标表 DROP 的事务执行 PREPARE TRANSACTION")));
			if (xact_map_n > 0 && TsoCurrentGxid() == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("未加入全局事务的分片写不允许 PREPARE TRANSACTION"),
						 errhint("经连接加入协议（partdist_join_global_txn / "
								 "join_info GUC）携带 gxid 后放行。")));
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			/* 先翻临时提交表（删除条目=提交点，P1 桩），再清映射 */
			for (i = 0; i < xact_map_n; i++)
				ShardCommitMarkEnded(xact_map[i].shard, xact_map[i].sxid, true);
			xact_map_n = 0;
			ShardClogAtCommit();	/* DROP TABLE 的文件 GC，提交才删 */
			TsoClientClearActive();
			break;

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			for (i = 0; i < xact_map_n; i++)
				ShardCommitMarkEnded(xact_map[i].shard, xact_map[i].sxid, false);
			xact_map_n = 0;
			ShardClogAtAbort();
			TsoClientClearActive();
			break;

		case XACT_EVENT_PREPARE:
			/*
			 * T4.5：joined 分片写的 PREPARE（T4.3 起合法）在此把活跃表条目
			 * **只摘不判**——此后"未决"由 clog PREPARED + 未决登记表承载，
			 * 读者走 §4.2 三态。不摘的话：持有条目的是 Citus 池化任务连接
			 * （可存活极久），读者先命中活跃表即返 RUNNING，决议收敛写进
			 * clog 的 COMMITTED 被永久遮蔽（实测：池连接一退出行立即可见，
			 * 正是本缺陷的指纹）。终局判决绝不在此预写。
			 */
			for (i = 0; i < xact_map_n; i++)
				ShardCommitRemove(xact_map[i].shard, xact_map[i].sxid);
			xact_map_n = 0;
			ShardClogAtAbort();
			TsoClientClearActive();
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
		memset(ShardXidCtl->shadow, 0, sizeof(ShardXidCtl->shadow));
		memset(ShardXidCtl->mvcc_set, 0, sizeof(ShardXidCtl->mvcc_set));
		ShardXidCtl->mvcc_n = 0;
		ShardXidCtl->lock =
			&GetNamedLWLockTranche("pg_partdist_shard_xid")[0].lock;

		/*
		 * T2.7 启动装载：pg_shard_xid/ 目录就是登记表——注册函数预创建
		 * 水位文件、DROP GC 删除之。postmaster 启动期单线程，无需持锁。
		 */
		{
			DIR		   *dir = AllocateDir(SHARD_XID_DIR);
			struct dirent *de;

			while (dir != NULL && (de = ReadDir(dir, SHARD_XID_DIR)) != NULL)
			{
				unsigned int oid;
				char		trail;

				if (sscanf(de->d_name, "%u%c", &oid, &trail) != 1 || oid == 0)
					continue;	/* "."/".."/"*.tmp" 等一律跳过 */
				if (ShardXidCtl->mvcc_n >= SHARD_XID_MAX_SLOTS)
				{
					elog(WARNING,
						 "pg_partdist: 打标登记超过 %d 张，OID %u 未装载"
						 "（该表在重启后不再打标——不可继续使用！）",
						 SHARD_XID_MAX_SLOTS, oid);
					continue;
				}
				ShardXidCtl->mvcc_set[ShardXidCtl->mvcc_n++] = (Oid) oid;
			}
			if (dir != NULL)
				FreeDir(dir);
		}
	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * T2.7：把表登进运行时 mvcc 集合（注册函数与测试用；幂等）。
 */
void
ShardMvccSetAdd(Oid relid)
{
	int			i;

	if (ShardXidCtl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist 分片 xid 共享内存未初始化")));

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);
	for (i = 0; i < ShardXidCtl->mvcc_n; i++)
		if (ShardXidCtl->mvcc_set[i] == relid)
		{
			LWLockRelease(ShardXidCtl->lock);
			return;
		}
	if (ShardXidCtl->mvcc_n >= SHARD_XID_MAX_SLOTS)
	{
		LWLockRelease(ShardXidCtl->lock);
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("打标登记表已满（上限 %d 张）", SHARD_XID_MAX_SLOTS)));
	}
	/* 先写槽位再抬 mvcc_n——无锁快门读侧永远看不到未初始化的槽 */
	ShardXidCtl->mvcc_set[ShardXidCtl->mvcc_n] = relid;
	ShardXidCtl->mvcc_n++;
	LWLockRelease(ShardXidCtl->lock);
}

/*
 * T2.7：为登记表预创建水位文件（{0,0}，8 字节）——让 pg_shard_xid/ 目录
 * 在首写之前就承担"启动登记表"职责。已存在则不动（幂等）。
 */
void
ShardMvccEnsureWatermarkFile(Oid relid)
{
	char		path[MAXPGPATH];
	struct stat st;

	snprintf(path, sizeof(path), SHARD_XID_DIR "/%u", relid);
	if (stat(path, &st) == 0)
		return;
	shard_xid_persist_watermark(relid, 0, 0);
}

/*
 * 0007：把本事务的 (shard, sxid) 对交给 XactLogCommitRecord/AbortRecord
 * 写进记录体。**在临界区内被调**——只拷静态缓冲，不 palloc 不 ereport。
 */
static uint32 wal_pair_buf[2 * SHARD_XID_MAX_PER_XACT];

static int
shard_xact_wal_list_impl(uint32 **pairs, uint64 *commit_ts)
{
	int			i;

	for (i = 0; i < xact_map_n; i++)
	{
		wal_pair_buf[2 * i] = (uint32) xact_map[i].shard;
		wal_pair_buf[2 * i + 1] = (uint32) xact_map[i].sxid;
	}
	*pairs = wal_pair_buf;

	/*
	 * commit_ts = PRE_COMMIT 暂存值（R-P3-1：暂存失败在 PRE_COMMIT 即 ERROR，
	 * 提交路径必经 CallXactCallbacks(PRE_COMMIT) ⇒ 到达这里暂存必已就绪或
	 * 本就是遗留模式 0）。abort 记录构造也走本钩子：abort 路径无 PRE_COMMIT、
	 * 暂存自然为 0，redo 侧按 committed 标志区分，0 属正常。
	 */
	*commit_ts = (uint64) TsoStashedCommitTs();
	return xact_map_n;
}

/* ---- T4.3：分片 2PC 段（补丁 0009） ---- */

typedef struct ShardTwoPhasePayload
{
	int64		gxid;
	int64		start_ts;
	int64		coord_gsid;		/* T4.5：问询寻址 */
	int64		dtxid;			/* T4.5：决议键（镜像自 Citus gid） */
	int32		nxids;
	uint32		pairs[FLEXIBLE_ARRAY_MEMBER];	/* 2*nxids 个 uint32 */
} ShardTwoPhasePayload;

/* T4.4 及之前的段格式（升级期恢复兼容） */
typedef struct ShardTwoPhasePayloadV1
{
	int64		gxid;
	int64		start_ts;
	int32		nxids;
	uint32		pairs[FLEXIBLE_ARRAY_MEMBER];
} ShardTwoPhasePayloadV1;

/*
 * 段载荷解析（v2/v1 按长度判别，尺寸差 16 字节不会歧义）。
 * 返回 false = 两版都对不上（损坏/未知），调用方按无段处置。
 * v1 无 coord_gsid/dtxid → 置 0（问询走不通，靠恢复守护收敛——升级期
 * 仅存量 prepared 事务受此限）。
 */
static bool
shard_twophase_parse(const void *recdata, uint32 len,
					 ShardTwoPhasePayload *hdr, const char **pairs_base)
{
	const char *base = (const char *) recdata;

	if (len >= offsetof(ShardTwoPhasePayload, pairs))
	{
		memcpy(hdr, base, offsetof(ShardTwoPhasePayload, pairs));
		if (hdr->nxids >= 0 &&
			len == offsetof(ShardTwoPhasePayload, pairs) +
				   (Size) hdr->nxids * 2 * sizeof(uint32))
		{
			*pairs_base = base + offsetof(ShardTwoPhasePayload, pairs);
			return true;
		}
	}
	if (len >= offsetof(ShardTwoPhasePayloadV1, pairs))
	{
		ShardTwoPhasePayloadV1 v1;

		memcpy(&v1, base, offsetof(ShardTwoPhasePayloadV1, pairs));
		if (v1.nxids >= 0 &&
			len == offsetof(ShardTwoPhasePayloadV1, pairs) +
				   (Size) v1.nxids * 2 * sizeof(uint32))
		{
			hdr->gxid = v1.gxid;
			hdr->start_ts = v1.start_ts;
			hdr->coord_gsid = 0;
			hdr->dtxid = 0;
			hdr->nxids = v1.nxids;
			*pairs_base = base + offsetof(ShardTwoPhasePayloadV1, pairs);
			return true;
		}
	}
	return false;
}

/*
 * StartPrepare 之后的注册点：PREPARED 落账（durable，投票持久前必须已
 * 持久）+ 2PC 状态段注册（崩溃恢复据此重建）。此刻 xact_map 仍在
 * （XACT_EVENT_PREPARE 的清理在 EndPrepare 之后）。
 */
static void
shard_at_prepare_impl(void)
{
	ShardTwoPhasePayload *p;
	Size		sz;
	int			i;
	int64		gxid;
	int64		sts;

	if (xact_map_n == 0)
		return;

	gxid = TsoCurrentGxid();
	sts = TsoGetStartTs();

	for (i = 0; i < xact_map_n; i++)
		ShardClogSetPrepared(xact_map[i].shard, xact_map[i].sxid, sts, gxid);

	sz = offsetof(ShardTwoPhasePayload, pairs) +
		(Size) xact_map_n * 2 * sizeof(uint32);
	p = (ShardTwoPhasePayload *) palloc(sz);
	p->gxid = gxid;
	p->start_ts = sts;
	p->coord_gsid = TsoCurrentCoordGsid();
	p->dtxid = PartDistPendingDtxid();
	p->nxids = xact_map_n;
	for (i = 0; i < xact_map_n; i++)
	{
		p->pairs[2 * i] = (uint32) xact_map[i].shard;
		p->pairs[2 * i + 1] = (uint32) xact_map[i].sxid;
	}
	RegisterTwoPhaseRecord(TWOPHASE_RM_SHARD_ID, 0, p, (uint32) sz);

	/*
	 * T4.5：未决登记（OPEN fsync 先于 EndPrepare 的 WAL 刷盘 ⇒
	 * "prepared 存在 ⇒ 登记必在"）。判决落分片 clog 时才注销——
	 * COMMIT PREPARED 不注销（终局等决议）。
	 */
	DtxPendingRegister(gxid, p->coord_gsid, p->dtxid, sts,
					   xact_map_n, p->pairs);
	pfree(p);
}

/* 崩溃恢复：重建 PREPARED 落账（幂等；startup 进程执行） */
static void
shard_twophase_recover_impl(TransactionId xid, uint16 info,
							void *recdata, uint32 len)
{
	ShardTwoPhasePayload hdr;
	const char *pairs_base;
	uint32		pairs[2 * DTX_PENDING_MAX_PAIRS];
	int			i;

	if (!shard_twophase_parse(recdata, len, &hdr, &pairs_base))
		return;
	for (i = 0; i < hdr.nxids; i++)
	{
		uint32		pv[2];

		memcpy(pv, pairs_base + (Size) i * 2 * sizeof(uint32), sizeof(pv));
		ShardClogSetPrepared((Oid) pv[0], (TransactionId) pv[1],
							 hdr.start_ts, hdr.gxid);
		if (i < DTX_PENDING_MAX_PAIRS)
		{
			pairs[2 * i] = pv[0];
			pairs[2 * i + 1] = pv[1];
		}
	}
	/* T4.5：登记表兜底重建（日志重放通常已覆盖，缺了才补） */
	if (hdr.nxids > 0 && hdr.nxids <= DTX_PENDING_MAX_PAIRS)
		DtxPendingReRegister(hdr.gxid, hdr.coord_gsid, hdr.dtxid,
							 hdr.start_ts, hdr.nxids, pairs);
}

/*
 * COMMIT PREPARED：不写终局——判决与 commit_ts 走协调者决议的异步广播/
 * 问询收敛（§3.1 步骤 ⑦，T4.5）；这里写 ts=0 的 COMMITTED 会破坏 SI
 * （0 对一切快照可见）。PREPARED 槽保持，读者按 §4.2 三态处置。
 */
static void
shard_twophase_postcommit_impl(TransactionId xid, uint16 info,
							   void *recdata, uint32 len)
{
	elog(DEBUG1, "pg_partdist: 分片 2PC 段 postcommit（终局待决议广播，T4.5）");
}

/* ABORT PREPARED：中止即终局，写 ABORTED（ts 无意义恒 0） */
static void
shard_twophase_postabort_impl(TransactionId xid, uint16 info,
							  void *recdata, uint32 len)
{
	ShardTwoPhasePayload hdr;
	const char *pairs_base;
	int			i;

	if (!shard_twophase_parse(recdata, len, &hdr, &pairs_base))
		return;
	for (i = 0; i < hdr.nxids; i++)
	{
		uint32		pv[2];

		memcpy(pv, pairs_base + (Size) i * 2 * sizeof(uint32), sizeof(pv));
		ShardClogSetVerdict((Oid) pv[0], (TransactionId) pv[1], false, 0);
	}
	/* T4.5：中止即终局，注销未决登记 */
	DtxPendingFinalized(hdr.gxid);
}

void
ShardXidInstallHook(void)
{
	shard_relation_xid_hook = shard_relation_xid_impl;
	shard_xact_wal_list_hook = shard_xact_wal_list_impl;
	shard_xact_redo_hook = ShardClogXactRedo;
	shard_at_prepare_hook = shard_at_prepare_impl;
	shard_twophase_recover_hook = shard_twophase_recover_impl;
	shard_twophase_postcommit_hook = shard_twophase_postcommit_impl;
	shard_twophase_postabort_hook = shard_twophase_postabort_impl;
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
		shard_oid_is_mvcc(cfg, (Oid) owner))
		relid = (Oid) owner;
	else
		relid = RangeVarGetRelid(rv, NoLock, true);

	if (OidIsValid(relid) && shard_oid_is_mvcc(cfg, relid))
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

	if (!shard_gating_active(cfg) || parsetree == NULL)
		return;

	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *stmt = (VacuumStmt *) parsetree;
		ListCell   *lc;

		/*
		 * T2.6：ANALYZE 放行——读侧判活走 0008 钩子（shard_visibility.c 的
		 * sv_satisfies_vacuum，只判不收）。VACUUM（含 VACUUM ANALYZE/FULL）
		 * 维持禁到 P5（页面动作/freeze 全章）。
		 */
		if (!stmt->is_vacuumcmd)
			return;

		if (stmt->rels == NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("分片打标白名单非空时不允许整库 VACUUM"),
					 errhint("请点名不含分片打标表的目标表。")));

		foreach(lc, stmt->rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);

			if (OidIsValid(vrel->oid) && shard_oid_is_mvcc(cfg, vrel->oid))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("VACUUM 不允许作用于分片打标表（OID %u，P5 前禁）",
								vrel->oid)));
			shard_xid_guard_range_var(cfg, vrel->relation, "VACUUM");
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

				if (OidIsValid(relid) && shard_oid_is_mvcc(cfg, relid))
					ShardClogRememberDrop(relid);
			}
		}
	}
}

/* ================= T2.7 注册函数（SQL 入口） ================= */

/*
 * partdist_set_shard_mvcc(regclass) —— 把既有 partition_map 分区登记为
 * 分片打标表。三步：① 真相列 UPDATE（事务性；无行即 ERROR，登记的必须是
 * 已注册分区）；② 预创建水位文件（目录=启动登记表，持久）；③ 运行时集合。
 * ②③ 不随回滚撤销——失败方向是"多打标"（表成为事实白名单成员），语义
 * 安全；反向（该打标未打标）才是数据损坏。P2 不支持撤销：DROP TABLE 即全清
 * （回滚错配面直接消灭）。建议在自动提交里调用。
 */
PG_FUNCTION_INFO_V1(partdist_set_shard_mvcc);
Datum
partdist_set_shard_mvcc(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	bool		enable = PG_GETARG_BOOL(1);
	char		sql[128];
	int			ret;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("partdist_set_shard_mvcc 需要超级用户")));

	if (!enable)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("P2 不支持撤销打标登记"),
				 errhint("DROP TABLE 会连同水位/clog 文件一并清理。")));

	if (SPI_connect() != SPI_OK_CONNECT)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SPI_connect 失败")));
	snprintf(sql, sizeof(sql),
			 "UPDATE partdist.partition_map SET shard_mvcc = true"
			 " WHERE partition_id = %u",
			 relid);
	ret = SPI_execute(sql, false, 0);
	if (ret != SPI_OK_UPDATE)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("partition_map 更新失败（SPI %d）", ret)));
	if (SPI_processed != 1)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("partition_map 里没有分区 %u 的登记行", relid),
				 errhint("先按既有流程注册分区，再登记打标。")));
	SPI_finish();

	ShardMvccEnsureWatermarkFile(relid);
	ShardMvccSetAdd(relid);

	PG_RETURN_VOID();
}
