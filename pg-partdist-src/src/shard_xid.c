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
#include "shard_replay.h"	/* T6.3c：副本壳表闸门 */
#include "shard_clog.h"
#include "dtx_pending.h"
#include "tso.h"
#include "shard_visibility.h"
#include "funcapi.h"			/* T5.1 SQL 包装：复合类型返回 */

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

/* T5.6：回卷护栏两阶段的阈值（设计 §7），GUC 可调 —— 验收要把它们调小 */
static int	shard_vacuum_max_age = 200000000;
bool		shard_vacuum_auto_enabled = true;	/* T7.17：自动启动器开关 */
static int	shard_xid_stop_age = 2146483648;

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

/*
 * 水位合理性上限。
 *
 * ★【2026-08-21 T5.6 更正，两处】
 *
 * 其一：**原注释里的举证是错的**。它说"实测 '\\x' 序列被读成 858814556，
 * 正落在该区间之上"—— 而 858814556 < 2^30 = 1073741824，这道守卫**拦不住
 * 它自己引用的那个值**。绝对阈值天生分不清"损坏值 8.6 亿"和"真跑了 8.6 亿
 * 笔事务的分片"，这是它的固有局限，不是参数没调好。
 *
 * 其二：**原来的 2^30 会抢在回卷护栏前面触发**。设计 §7 的阶段 2 停发线是
 * 2^31 − 边距；一个正常运转到那个量级的分片，重启读水位时会先撞上 2^30 的
 * "文件损坏"报错 —— 一条完全误导的错误信息。阈值因此上抬到 2^31，让
 * **T5.6 的分片级停发护栏先说话**（它给的信息才是对的：该分片进只读、
 * 去解决前缀阻挡者）。
 *
 * 保留这道检查的意义收窄为：越过停发线还能读到的水位，要么文件损坏、
 * 要么护栏本身失效 —— 两种都必须 fail-closed。
 *
 * 顺带记下一条**不能加**的检查：`vacuum_xid <= alloc_wm` 看着是个真不变式
 * （不可能清理到从未发出过的号），但 T5.4b-2 起 **follower 侧的水位文件正是
 * alloc_wm=0 而 trunc_before/vacuum_xid 非 0**（水位由 CTRL 复制过来、发号
 * 从未在本节点发生），加上去会把每个 follower 判成损坏。
 */
#define SHARD_XID_SANITY_MAX	((TransactionId) 0x80000000)

typedef struct ShardXidSlot
{
	Oid			shard_relid;	/* InvalidOid = 空槽 */
	TransactionId next_xid;		/* 下一个待发 */
	TransactionId watermark;	/* 已持久化上界：所有已发号 < watermark */
	TransactionId claim_wm;		/* T2.4 认领水位：< 它的历史号已全部认领过 */
	/*
	 * T5.1 vacuum 两水位（设计 §6.1）。放进槽位而不是每次读文件，是因为
	 * shard_xid_persist_watermark() 是**整文件覆写** —— 任何一次发号/认领
	 * 落盘若不带上它们，就会把它们抹成 0（免查区凭空消失，虽方向安全但
	 * 等于白做一次 vacuum）。槽位是内存权威副本，建槽时从文件载入。
	 */
	TransactionId trunc_before;	/* clog 实际截断点 = 隐式 freeze 点 */
	TransactionId vacuum_xid;	/* 两态恢复标记，>= trunc_before */
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

/* T4.6：门控谓词的对外包装（shard_guard.c 用） */
bool
ShardGatingActive(void)
{
	return shard_gating_active(shard_relids_cfg);
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
 *
 * 格式演进（每次都保持**向后兼容读**，缺失字段一律取"安全方向"的值）：
 *   T1.2  4 字节： {alloc_wm}
 *   T2.4  8 字节： {alloc_wm, claim_wm}
 *   T5.1 16 字节：{alloc_wm, claim_wm, clog_truncate_before, shard_vacuum_xid}
 * 全部 uint32 小端。
 *
 * 崩溃语义：alloc_wm 是"已授权发放的上界"，重启从它续发 —— 最多跳
 * SHARD_XID_BATCH 个号，绝不重发（DEV PLAN T1.2 的"跳号无害"裁定）；
 * claim_wm 之下的历史号已全部认领过（RUNNING 已改判 ABORTED）。
 *
 * T5.1 两个 vacuum 水位（设计 §6.1）：
 *   clog_truncate_before —— 本分片 clog 实际截断到哪；**隐式 freeze 点**，
 *                           也是回卷龄的基点（设计 §7 明确：基点必须是它而
 *                           不是 shard_vacuum_xid，否则"趟完未截断"崩溃窗口
 *                           里龄会被算小，方向不安全）；
 *   shard_vacuum_xid     —— 两态恢复标记（设计 §6.5）：等于
 *                           clog_truncate_before 表示"无未完成的趟"；大于它
 *                           表示"页面趟已完成、截断尚未做"，重启只补截断。
 * 缺失时两者均取 0 —— 含义是"从未截断、从未 vacuum"，**这是安全方向**：
 * 免查隐式冻结区为空，所有 xid 都要正常查 clog，不会把未决当已提交。
 */
static void
shard_xid_read_wm_file(Oid shard, TransactionId *alloc_wm,
					   TransactionId *claim_wm,
					   TransactionId *trunc_before,
					   TransactionId *vacuum_xid)
{
	char		path[MAXPGPATH];
	int			fd;
	uint32		v[4];
	int			r;

	*alloc_wm = 0;
	*claim_wm = 0;
	if (trunc_before != NULL)
		*trunc_before = 0;
	if (vacuum_xid != NULL)
		*vacuum_xid = 0;

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
		/* T5.1 起的 16 字节完整格式 */
		*alloc_wm = (TransactionId) v[0];
		*claim_wm = (TransactionId) v[1];
		if (trunc_before != NULL)
			*trunc_before = (TransactionId) v[2];
		if (vacuum_xid != NULL)
			*vacuum_xid = (TransactionId) v[3];
	}
	else if (r == (int) (2 * sizeof(uint32)))
	{
		/* T2.4 的 8 字节格式：两个 vacuum 水位缺席 ⇒ 取 0（从未截断） */
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

	/*
	 * ★ T5.1 加固：读回的水位做**合理性校验**，而不是照单全收。
	 *
	 * 实测暴露：文件字节损坏时（本轮是人为写坏），损坏值会被当作有效水位
	 * 直接用于发号 —— 新行的 xmin 直接变成那个垃圾数（实测 858814556）。
	 * 一旦发出去就污染了该分片的 xid 空间，且无声无息。
	 *
	 * 校验依据：分片 xid 是**稠密连续分配**的（设计 §5.2），alloc_wm 只可能
	 * 由本节点一次次 +SHARD_XID_BATCH 推上去。真实值远小于 2^31；一个落在
	 * 高位区间的值只可能来自损坏。这里不追求精确边界，只拦"显然不可能"的，
	 * fail-closed：宁可报错要求人工介入，也不静默发出坏号。
	 */
	if (*alloc_wm >= SHARD_XID_SANITY_MAX || *claim_wm >= SHARD_XID_SANITY_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("分片 xid 水位文件 \"%s\" 的水位值不合理"
						"（alloc_wm=%u claim_wm=%u，上限 %u）",
						path, *alloc_wm, *claim_wm,
						(uint32) SHARD_XID_SANITY_MAX),
				 errdetail("分片 xid 稠密连续分配，正常值远小于该上限；"
						   "此形态只可能来自文件损坏。"),
				 errhint("继续使用会把损坏值当分片 xid 发出去，污染该分片的 "
						 "xid 空间（实测新行 xmin 直接变成垃圾数）。")));
}

static void
shard_xid_persist_watermark(Oid shard, TransactionId alloc_wm,
							TransactionId claim_wm,
							TransactionId trunc_before,
							TransactionId vacuum_xid)
{
	char		tmppath[MAXPGPATH];
	char		path[MAXPGPATH];
	int			fd;
	uint32		v[4];

	/*
	 * T5.1 不变式（设计 §6.5）：clog_truncate_before <= shard_vacuum_xid。
	 * 破坏它意味着"截断点跑到了 vacuum 进度前面" —— 免查区会覆盖尚未清理
	 * 垃圾的 xid，中止事务的幽灵行会复活。故在**落盘这唯一出口**上守住，
	 * 而不是散在各调用点（fail-closed：宁可 ERROR 也不写下坏水位）。
	 */
	if (TransactionIdIsValid(trunc_before) && TransactionIdIsValid(vacuum_xid) &&
		trunc_before > vacuum_xid)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("分片 %u 的 vacuum 水位不变式被破坏："
						"clog_truncate_before=%u > shard_vacuum_xid=%u",
						shard, trunc_before, vacuum_xid),
				 errdetail("截断点不得越过 vacuum 进度，否则免查隐式冻结区会"
						   "覆盖尚未清理垃圾的 xid（设计 §6.4/§6.5）。")));

	v[0] = (uint32) alloc_wm;
	v[1] = (uint32) claim_wm;
	v[2] = (uint32) trunc_before;
	v[3] = (uint32) vacuum_xid;

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

/* ---- T5.1 SQL 包装：验收观测点 ---- */
PG_FUNCTION_INFO_V1(partdist_shard_vacuum_watermarks);
Datum
partdist_shard_vacuum_watermarks(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId tb,
				vx;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	ShardVacuumGetWatermarks(shard, &tb, &vx);
	values[0] = TransactionIdGetDatum(tb);
	values[1] = TransactionIdGetDatum(vx);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

PG_FUNCTION_INFO_V1(partdist_shard_vacuum_set_watermarks);
Datum
partdist_shard_vacuum_set_watermarks(PG_FUNCTION_ARGS)
{
	ShardVacuumSetWatermarks(PG_GETARG_OID(0),
							 (TransactionId) PG_GETARG_INT64(1),
							 (TransactionId) PG_GETARG_INT64(2));
	PG_RETURN_VOID();
}

/* T5.1 接口用到，定义在下方 */
static ShardXidSlot *shard_xid_slot_attach(Oid shard, int *nclaimed);

/*
 * ================= T5.1 vacuum 两水位对外接口（设计 §6.1）=================
 *
 * 读：返回本分片的 {clog_truncate_before, shard_vacuum_xid}。槽位不存在时
 *     按 0/0 返回（"从未截断、从未 vacuum"，安全方向），**不建槽** ——
 *     建槽会顺带触发 T2.4 认领，读水位这种轻动作不该有那种副作用。
 * 写：整体更新两水位并落盘。不变式由 shard_xid_persist_watermark 统一守，
 *     此处不重复判断（单一出口，见该函数注释）。
 */
/*
 * T5.4：本分片"下一个待发号"。vacuum 侧用它做 fail-closed 上界 ——
 * 清理/截断到一个**从未发出过**的号是没有意义的输入，而它的后果很重：
 * 整个已用 xid 空间落进免查区，此后每一行新写入的 xmin 都会被读成
 * "早已提交、对一切快照可见"，中止事务的行也一并复活。
 *
 * 槽位不存在（本次启动没碰过该分片）时回落读水位文件，理由同
 * ShardVacuumGetWatermarks：这是跨重启的持久事实。取不到返回 0，
 * 调用方按"无从判断、不设限"处理（此时该分片本就没发过号）。
 */
/*
 * U-P5-1 之二：本分片**已持久化**的发号水位（所有已发号 < 它）。
 *
 * 与 ShardXidNextToIssue 的区别是刻意的：那个返回 next_xid（更紧的界，
 * vacuum 的 fail-closed 上界要紧的），而这个返回 slot->watermark ——
 * 发号器每次落盘按 SHARD_XID_BATCH 向上取整，于是它比 next_xid 大出至多
 * 一个批次。**给 follower 要的正是更宽的那个**：它只能从"入了流的 MARKER"
 * 学到水位，而中止且字节未入流的事务会在 leader 上悄悄吃掉号；宽一个批次
 * 就把那段窗口盖住了。
 */
TransactionId
ShardXidAllocWatermark(Oid shard)
{
	TransactionId wm = 0;
	int			i;

	if (ShardXidCtl == NULL)
		return 0;

	LWLockAcquire(ShardXidCtl->lock, LW_SHARED);
	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
	{
		if (ShardXidCtl->slots[i].shard_relid == shard)
		{
			wm = ShardXidCtl->slots[i].watermark;
			break;
		}
	}
	LWLockRelease(ShardXidCtl->lock);

	if (wm == 0)
	{
		TransactionId a = 0,
					c = 0;

		shard_xid_read_wm_file(shard, &a, &c, NULL, NULL);
		wm = a;
	}
	return wm;
}

TransactionId
ShardXidNextToIssue(Oid shard)
{
	TransactionId next = 0;
	int			i;

	if (ShardXidCtl == NULL)
		return 0;

	LWLockAcquire(ShardXidCtl->lock, LW_SHARED);
	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
	{
		if (ShardXidCtl->slots[i].shard_relid == shard)
		{
			next = ShardXidCtl->slots[i].next_xid;
			break;
		}
	}
	LWLockRelease(ShardXidCtl->lock);

	if (next == 0)
	{
		TransactionId a = 0,
					c = 0;

		shard_xid_read_wm_file(shard, &a, &c, NULL, NULL);
		next = a;				/* 文件缺失时该函数已置 0 = 取不到 */
	}

	return next;
}

/*
 * U-P5-1 之二：把发号水位**只抬不降**地落进本分片的水位文件。
 *
 * follower 用它接住 leader 随 MARKER 捎来的水位 —— 升主后
 * `shard_xid_slot_attach()` 取 `Max(文件 alloc_wm, 影子)` 起步，就不会把副本里
 * 已有的号重新发一遍。
 *
 * 只抬不降：MARKER 是逐条到达的，乱序/重放都可能带来更小的值；而水位的语义
 * 是"所有已发号 < 它"，退回去就等于允许重号。
 *
 * ★ 不动 claim_wm / 两个 vacuum 水位 —— 落盘是整文件覆写，它们必须原样带回
 *   （T5.1 记要里那条"任何一次落盘不带上它们就会抹成 0"的教训）。
 */
void
ShardXidRaiseAllocWatermark(Oid shard, TransactionId alloc_wm)
{
	ShardXidSlot *slot;

	if (!TransactionIdIsNormal(alloc_wm) || ShardXidCtl == NULL)
		return;

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);
	slot = shard_xid_slot_attach(shard, NULL);

	if (alloc_wm > slot->watermark)
	{
		/* 与 ShardVacuumSetWatermarks 同一条纪律：先落盘、成功了才改槽位 */
		shard_xid_persist_watermark(shard, alloc_wm, slot->claim_wm,
									slot->trunc_before, slot->vacuum_xid);
		slot->watermark = alloc_wm;
		if (slot->next_xid < alloc_wm)
			slot->next_xid = alloc_wm;
	}
	LWLockRelease(ShardXidCtl->lock);
}

void
ShardVacuumGetWatermarks(Oid shard, TransactionId *trunc_before,
						 TransactionId *vacuum_xid)
{
	int			i;

	if (trunc_before != NULL)
		*trunc_before = 0;
	if (vacuum_xid != NULL)
		*vacuum_xid = 0;

	if (ShardXidCtl == NULL)
		return;

	LWLockAcquire(ShardXidCtl->lock, LW_SHARED);
	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
	{
		if (ShardXidCtl->slots[i].shard_relid == shard)
		{
			if (trunc_before != NULL)
				*trunc_before = ShardXidCtl->slots[i].trunc_before;
			if (vacuum_xid != NULL)
				*vacuum_xid = ShardXidCtl->slots[i].vacuum_xid;
			break;
		}
	}
	LWLockRelease(ShardXidCtl->lock);

	/*
	 * 槽位不存在 ⇒ 本次启动还没碰过该分片。回落读文件：vacuum 水位是
	 * **跨重启的持久事实**，不能因为"这次还没建槽"就报 0（那会让回卷龄
	 * 算出天文数字、误触阶段 2 拒发号）。
	 */
	if (trunc_before != NULL && *trunc_before == 0 &&
		vacuum_xid != NULL && *vacuum_xid == 0)
	{
		TransactionId a,
					c;

		shard_xid_read_wm_file(shard, &a, &c, trunc_before, vacuum_xid);
	}
}

void
ShardVacuumSetWatermarks(Oid shard, TransactionId trunc_before,
						 TransactionId vacuum_xid)
{
	ShardXidSlot *slot;

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);
	slot = shard_xid_slot_attach(shard, NULL);

	/*
	 * ★ 顺序要害（2026-08-18 实测踩到）：**先落盘、成功了才更新槽位**。
	 * 反过来写的话，不变式守卫在落盘处 ERROR 时槽位已被改脏 —— 而读接口
	 * 优先从槽位取值，于是"被拒绝的写"照样从读接口里看得见（实测读到
	 * 300/200 这个本该被拒的组合）。ERROR 会中止事务但**不会回滚 shmem**，
	 * 这类"内存先于持久化"的写法在共享内存上一律是错的。
	 */
	shard_xid_persist_watermark(shard, slot->watermark, slot->claim_wm,
								trunc_before, vacuum_xid);
	slot->trunc_before = trunc_before;
	slot->vacuum_xid = vacuum_xid;
	LWLockRelease(ShardXidCtl->lock);
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
	TransactionId slot_trunc_before;	/* T5.1：从文件载入，落盘时原样带回 */
	TransactionId slot_vacuum_xid;
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

	shard_xid_read_wm_file(shard, &alloc_wm, &claim_wm,
						   &slot_trunc_before, &slot_vacuum_xid);
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
		shard_xid_persist_watermark(shard, Max(alloc_wm, ceiling), ceiling,
									slot_trunc_before, slot_vacuum_xid);
		if (nclaimed)
			*nclaimed = n;
	}

	free_slot->next_xid = ceiling;
	free_slot->watermark = free_slot->next_xid;
	free_slot->claim_wm = ceiling;
	/* T5.1：两个 vacuum 水位随槽位常驻，供后续落盘原样带回（见结构体注释） */
	free_slot->trunc_before = slot_trunc_before;
	free_slot->vacuum_xid = slot_vacuum_xid;
	/* relid 最后置：上面 ERROR 的话槽位仍是空的 */
	free_slot->shard_relid = shard;
	return free_slot;
}

/*
 * 发一个号。锁内做水位落盘（每 SHARD_XID_BATCH 次分配才一次 fsync；
 * 且分配频率是"每事务每分片"而不是每元组 —— T1.3 的映射缓存住了）。
 * 任何 ERROR 都发生在槽位状态推进之前，fail-closed。
 */
/* ================= T5.6：分片级回卷护栏两阶段（设计 §7）================= */

/*
 * 分片 xid 龄。
 *
 * ★ 基点必须是 clog_truncate_before，**不是** shard_vacuum_xid（设计 §7 原文）：
 *   歧义边界挂在免查隐式冻结区的解释规则上，而那条规则读的正是
 *   clog_truncate_before；「趟完未截断」的窗口里 shard_vacuum_xid 跑在前面，
 *   拿它算龄会把紧迫度算**小** —— 方向不安全。平时两者相等。
 */
static TransactionId
shard_xid_age_locked(const ShardXidSlot *slot)
{
	if (slot->next_xid <= slot->trunc_before)
		return 0;
	return slot->next_xid - slot->trunc_before;
}

/* 阶段 1 的 WARNING 每个后端每分片只发一次（持续状态，逐次发号刷屏无意义） */
static Oid	warned_shards[SHARD_XID_MAX_SLOTS];
static int	warned_shards_n = 0;

static bool
shard_xid_warn_once(Oid shard)
{
	int			i;

	for (i = 0; i < warned_shards_n; i++)
		if (warned_shards[i] == shard)
			return false;
	if (warned_shards_n < SHARD_XID_MAX_SLOTS)
		warned_shards[warned_shards_n++] = shard;
	return true;
}

/*
 * 发号前的两阶段护栏。须持 ShardXidCtl->lock 调用。
 *
 * 阶段 2 走 ERROR：**该分片进只读**，护栏是分片粒度，不殃及节点与集群
 * （这正是分片级 clog 相对原生全局 clog 的好处）。
 *
 * 实话两条（设计 §7 原文，写进错误信息里，因为这是操作者当场要知道的）：
 *   - 拒发只是止血，**解锁必须解决前缀阻挡者** —— 超龄 RUNNING 事务按策略
 *     强杀，而 **PREPARED 未决绝不允许单方中止**，只能走协调组决议；
 *   - GlobalSafeTs 被钉死会间接钉死 vacuum，告警体系须把「最老快照的龄」
 *     一并纳入监控。
 */
static void
shard_xid_wraparound_gate(Oid shard, const ShardXidSlot *slot)
{
	TransactionId age = shard_xid_age_locked(slot);

	if (age >= (TransactionId) shard_xid_stop_age)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("分片 %u 的 xid 龄 %u 达到停发线 %d，该分片进只读",
						shard, age, shard_xid_stop_age),
				 errdetail("龄 = next_xid(%u) - clog_truncate_before(%u)。"
						   "护栏是分片粒度，本节点与集群的其它分片不受影响。",
						   slot->next_xid, slot->trunc_before),
				 errhint("拒发只是止血：解锁必须让 clog 截断点推进 —— 先用 "
						 "partdist.shard_vacuum_target() 查前缀阻挡者。"
						 "超龄 RUNNING 可按策略强杀；PREPARED 未决绝不允许"
						 "单方中止，只能走协调组决议。")));

	if (age >= (TransactionId) shard_vacuum_max_age && shard_xid_warn_once(shard))
		ereport(WARNING,
				(errmsg("分片 %u 的 xid 龄 %u 已达 shard_vacuum_max_age %d",
						shard, age, shard_vacuum_max_age),
				 errdetail("停发线是 %d。", shard_xid_stop_age),
				 errhint("尽快跑 partdist.shard_vacuum_sweep() + "
						 "partdist.shard_clog_truncate() 推进截断点。")));
}

/*
 * T7.17（P7-V1）：列出**到龄**的分片，供自动启动器取用。
 *
 * 判据与阶段 1 护栏同源（`age >= shard_vacuum_max_age`）—— 这一点是有意的：
 * 护栏发 WARNING 说"该跑 vacuum 了"，自动启动器就该在同一条线上动手，
 * 两者用不同判据会出现"警告了但不动手"或"没警告却在动手"的错位。
 *
 * 只读共享内存、不碰 catalog、不做 IO：调用方是心跳工作者（无 DB 语境），
 * 它拿到非空结果才会去自连一个真 backend 干活。零到龄分片时代价 = 一次
 * LW_SHARED + 一趟 64 槽的线性扫描。
 */
int
ShardXidOverdueShards(Oid *shards, TransactionId *ages, int max)
{
	int			i;
	int			n = 0;

	if (ShardXidCtl == NULL || max <= 0)
		return 0;

	LWLockAcquire(ShardXidCtl->lock, LW_SHARED);
	for (i = 0; i < SHARD_XID_MAX_SLOTS && n < max; i++)
	{
		ShardXidSlot *slot = &ShardXidCtl->slots[i];
		TransactionId age;

		if (slot->shard_relid == InvalidOid)
			continue;
		age = shard_xid_age_locked(slot);
		if (age < (TransactionId) shard_vacuum_max_age)
			continue;
		shards[n] = slot->shard_relid;
		if (ages != NULL)
			ages[n] = age;
		n++;
	}
	LWLockRelease(ShardXidCtl->lock);
	return n;
}

/*
 * T7.17：处于「趟完、截断没做」（设计 §6.5 两态之二）的分片。
 *
 * ★ 为什么要和"到龄"分开列：这一格与龄无关 —— 它是**上一趟在落标记与截断
 *   之间崩过一次**留下的，页面动作已经全部完成并持久，只差最后一步截断。
 *   在此之前 `ShardVacuumRecover()` **只有手工入口**（SQL 函数），也就是说
 *   这一格能一直挂到该分片到龄为止：那段时间里 clog 段文件不回收、免查区
 *   不前进，而修复它只需要一次几乎零成本的补截断。
 */
int
ShardXidTwoStateShards(Oid *shards, int max)
{
	int			i;
	int			n = 0;

	if (ShardXidCtl == NULL || max <= 0)
		return 0;

	LWLockAcquire(ShardXidCtl->lock, LW_SHARED);
	for (i = 0; i < SHARD_XID_MAX_SLOTS && n < max; i++)
	{
		ShardXidSlot *slot = &ShardXidCtl->slots[i];

		if (slot->shard_relid == InvalidOid)
			continue;
		if (!TransactionIdIsValid(slot->vacuum_xid) ||
			slot->vacuum_xid <= slot->trunc_before)
			continue;
		shards[n++] = slot->shard_relid;
	}
	LWLockRelease(ShardXidCtl->lock);
	return n;
}

/* 同上，只要个数（心跳每 5 s 一次，别为计数分配数组） */
int
ShardXidOverdueCount(void)
{
	Oid			buf[SHARD_XID_MAX_SLOTS];

	int			n = ShardXidOverdueShards(buf, NULL, SHARD_XID_MAX_SLOTS);

	/* 两态之二同样要人管（见 ShardXidTwoStateShards 的注释） */
	if (n == 0)
		n = ShardXidTwoStateShards(buf, SHARD_XID_MAX_SLOTS);
	return n;
}

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

	shard_xid_wraparound_gate(shard, slot);

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

			shard_xid_persist_watermark(shard, new_wm, slot->claim_wm,
										slot->trunc_before, slot->vacuum_xid);
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
/*
 * ShardXidClaimOnPromote —— §6.6 第三支：**切主认领**（T6.4）。
 *
 * 判据是设计原文那句，干净且可证：
 *
 *   > 新主追平后流里没有该事务的提交标记 ⇒ 从未提交过 ⇒ 改 ABORTED 安全。
 *   > 依据：提交的必要条件是提交标记已多数派入流（[A]<[B] + 多数派先于本地
 *   > 提交），raft 选举保证新主拥有全部多数派条目。
 *
 * **与 T2.4 那一支的区别，正是本函数存在的理由**：ShardXidEnsureClaimed() 只在
 * "槽位不存在"时认领（挂槽即认领，见上面的不变式）。而升主的 follower 上槽位
 * **一定已经存在** —— 回放推进分片分配器水位时就把它挂上了（T6.5）。于是那条
 * 路径直接 return 0，一条都不认领。切主必须有自己的入口，强制对
 * [claim_wm, watermark) 再扫一遍。
 *
 * **上界为什么取 watermark 而不是 next_xid**：watermark 是**持久发号水位**
 * （P5 交付），按批次向上取整，比 next_xid 宽出至多一个批次。follower 只能从
 * "入了流的 MARKER"学到号，而 leader 上中止且字节未入流的事务会悄悄吃掉号 ——
 * 宽的那个正好把这段窗口盖住。这就是"水位是认领的输入、认领是水位的兜底"
 * （U-P5-1 残留格的配套关系）：两件事必须同期做完，单做任一件都留口子。
 *
 * **PREPARED 一根汗毛都不动**：ShardClogClaimRange 只改 RUNNING（含空洞槽），
 * 这是 §6.6 第二支。未决的 2PC 分片 xid 在 follower 上由回放
 * （shard_replay.c 的 XLOG_XACT_PREPARE 分支）写成 TXN_PREPARED，因此不会被
 * 本函数误判成中止 —— 否则一笔后来被判 COMMIT 的分布式事务会在这一分片上丢掉。
 * 调用方仍应把 dtx_close_indoubt() 排在本函数**之前**，让有决议的先落终局。
 *
 * 返回本次改判的条数。
 */
int
ShardXidClaimOnPromote(Oid shard)
{
	ShardXidSlot *slot;
	TransactionId from;
	TransactionId to;
	int			n = 0;
	int			i;

	if (ShardXidCtl == NULL)
		return 0;				/* shmem 未起（防御） */

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);

	/* 槽位不在就按老路挂（挂槽本身即认领一次），在就强制再扫一遍 */
	slot = shard_xid_slot_attach(shard, &n);

	/*
	 * ★ 2026-09-06：新主的发号起点 = Max(MARKER 交接来的 next_xid, 影子)。
	 *
	 * 槽位多半在追平期就已由 ShardXidRaiseAllocWatermark 挂上（值 = 旧主最后一条
	 * 入流 MARKER 时的 next_xid），slot_attach 因此不会再看影子。而影子里还有
	 * 回放期从 DATA 记录见到的分片 xid（ShardReplayNoteDataShardXid）——那是
	 * "字节入了流、却没有提交标记"的号，正是不能重发的那一类。这里补上取 Max。
	 */
	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
	{
		if (ShardXidCtl->shadow[i].shard_relid != shard)
			continue;
		if (ShardXidCtl->shadow[i].next_hint > slot->watermark)
		{
			TransactionId hint = ShardXidCtl->shadow[i].next_hint;

			shard_xid_persist_watermark(shard, hint, slot->claim_wm,
										slot->trunc_before, slot->vacuum_xid);
			slot->watermark = hint;
			if (slot->next_xid < hint)
				slot->next_xid = hint;
		}
		break;
	}

	from = slot->claim_wm;
	to = slot->watermark;

	if (from < to)
	{
		n += ShardClogClaimRange(shard, from, to);

		/*
		 * 顺序同 ShardXidSetVacuumWatermarks 的要害注释：**先落盘、成功了才
		 * 更新槽位**。ERROR 会中止事务但不会回滚 shmem，反过来写就会让
		 * "被拒绝的写"照样从读接口里看得见。
		 */
		shard_xid_persist_watermark(shard, slot->watermark, to,
									slot->trunc_before, slot->vacuum_xid);
		slot->claim_wm = to;
	}

	LWLockRelease(ShardXidCtl->lock);

	/*
	 * ★ 批次 #7：解除副本身份的动作**已经从这里搬走**。
	 *
	 * T6.8 时它临时放在这里，是因为当时解冻批次 #6 只批准了
	 * pg_raft_promote_prepare 里的两处调用，加不了第三处 —— 那是个**局部替身**，
	 * 而且时机偏早：promote_prepare 跑在**上报之前**，此刻路由还没翻过来。
	 *
	 * 正规位置是 FRD §11 步骤 5 说的"路由切换"那一刻，即 group0 apply 了
	 * OP_PARTITION_PRIMARY 之后 —— 现在实装在
	 * partdist.partwal_notify_primary_switch()（raft_boundary.c）。
	 * 那里还能顺带处理**降级**方向，这是本函数根本够不着的。
	 */
	return n;
}

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
	 * ★ T6.6：异步提交禁令（设计 §10 的"分片表异步提交"一行）。
	 *
	 * 这一条此前**只写在文档里，代码一个字都没拦** —— §10 逐条核实时才发现，
	 * 它是那张限制表里唯一的空档（SERIALIZABLE 在 shard_visibility.c、行锁与
	 * COPY FREEZE/推测插入在补丁 0005、逻辑解码在补丁 0006、维护命令与 Citus
	 * 运维项在 ShardXidUtilityGuard/shard_guard.c，都实装了）。
	 *
	 * 为什么必须拦：本方案的可见性与提交点语义**只许断言已持久的事实**
	 * （§4.5）。synchronous_commit=off 下，事务向客户端报成功时提交记录**尚未
	 * 落盘**，于是：
	 *   - 单分片快路径的 `[A] → quorum → [B]` 时序失去意义（[B] 根本没发生），
	 *   - 2PC 的多数派提交点也建立在"本地已持久"这个前提上，
	 *   - 崩溃之后 clog 判决与页面元组会不一致 —— 而这正是 §9.5 那类
	 *     "旧 leader 与自己的组分叉"的成因。
	 * 后果不可见、不报错、只在崩溃时显形，所以必须 fail-closed 在写入那一刻。
	 *
	 * 只拦 OFF：local/remote_write/remote_flush/remote_apply 都保证本地已 flush，
	 * 前提成立。
	 */
	if (synchronous_commit == SYNCHRONOUS_COMMIT_OFF)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("分片打标表（OID %u）不支持异步提交"
						"（synchronous_commit = off）", shard),
				 errdetail("本方案的可见性与提交点语义只许断言**已持久**的事实："
						   "异步提交下事务报成功时提交记录尚未落盘，单分片快路径的"
						   " [A]<[B] 时序与 2PC 的多数派提交点都会失去前提，"
						   "崩溃后判决与页面元组不一致（§10 / §4.5）。"),
				 errhint("把 synchronous_commit 设为 on / local / remote_* "
						 "之一再写分片表。")));

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

/* U-P5-1：本事务是否碰过任何分片打标表（决定 MARKER 要不要带分片 xid 尾） */
int
ShardXidXactCount(void)
{
	return xact_map_n;
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

	/* ---- T5.6：分片级回卷护栏两阶段（设计 §7）---- */
	DefineCustomIntVariable(
		"pg_partdist.shard_vacuum_max_age",
		"阶段 1：分片 xid 龄达到此值即到龄，须尽快跑分片 vacuum。",
		"分片版的 autovacuum_freeze_max_age。龄 = next_xid - clog_truncate_before。"
		"到龄发一条 WARNING，并（在 pg_partdist.shard_vacuum_auto 打开时）"
		"由心跳工作者触发 partdist.shard_vacuum_auto()（T7.17/P7-V1）。",
		&shard_vacuum_max_age,
		200000000,
		1, INT_MAX,
		PGC_SIGHUP,
		0, NULL, NULL, NULL);

	DefineCustomBoolVariable(
		"pg_partdist.shard_vacuum_auto",
		"分片 vacuum 自动启动器（T7.17/P7-V1）。",
		"打开后，TSO 心跳工作者每轮检查是否有分片到龄，有则自连触发 "
		"partdist.shard_vacuum_auto()。关掉即退回「只发 WARNING、全靠手工」的"
		"旧行为 —— 出问题时这是第一个该关的开关。",
		&shard_vacuum_auto_enabled,
		true,
		PGC_SIGHUP,
		0, NULL, NULL, NULL);

	DefineCustomIntVariable(
		"pg_partdist.shard_xid_stop_age",
		"阶段 2：分片 xid 龄达到此值即拒发新号，该分片进只读。",
		"分片版的 xidStopLimit。默认 2^31 − 10^6（设计 §7）。护栏是分片粒度，"
		"不殃及节点与集群。拒发只是止血 —— 解锁必须解决前缀阻挡者。",
		&shard_xid_stop_age,
		2146483648,
		1, INT_MAX,
		PGC_SIGHUP,
		0, NULL, NULL, NULL);
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
 * ShardXidSlotRelease —— T7.7（R-P6-4）：DROP 提交时把分配器槽位（连同影子）
 * 还回去。
 *
 * 缺陷：`SHARD_XID_MAX_SLOTS = 64`/节点是**定长 shmem**，而此前全仓**没有任何
 * 释放路径**（grep 零命中）——建删 64 张打标表之后，该节点再也建不出第 65 张，
 * 报「分片 xid 槽位用尽」。门禁一直靠"移走水位文件 + 重启"绕过去
 * （`run_p6_exit.sh` 直接 `mv`），也就是说这条缺陷在验收里是被**隐藏**的。
 *
 * 判据与 `ShardMvccSetRemove()` 完全同源：都挂在 DROP 的提交时点
 * （`ShardClogAtCommit`），回滚不执行——失败方向是"槽位没还"，只是浪费一格，
 * 语义安全；反向（表还在、槽位被抢走）才会让发号从头开始，那是数据损坏。
 *
 * 影子（`shadow[]`）一并清：它记的是"本次启动窗口里见过的最大分片 xid"，
 * 表都没了，留着只会在 OID 复用时把新表的起点顶到一个莫名其妙的高位。
 */
void
ShardXidSlotRelease(Oid relid)
{
	int			i;

	if (ShardXidCtl == NULL || !OidIsValid(relid))
		return;

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);
	for (i = 0; i < SHARD_XID_MAX_SLOTS; i++)
	{
		if (ShardXidCtl->slots[i].shard_relid == relid)
		{
			memset(&ShardXidCtl->slots[i], 0, sizeof(ShardXidSlot));
			ShardXidCtl->slots[i].shard_relid = InvalidOid;
		}
		if (ShardXidCtl->shadow[i].shard_relid == relid)
			memset(&ShardXidCtl->shadow[i], 0, sizeof(ShardXidShadow));
	}
	LWLockRelease(ShardXidCtl->lock);
}

/*
 * ShardMvccSetRemove —— 把 OID 从共享内存的打标登记集合里摘掉（R-P6-9）。
 *
 * 集合本身是**故意只进不出**的（`partdist_set_shard_mvcc(..., false)` 明确
 * 拒绝撤销），那条禁令针对的是"运行中的表想退出打标语义"。但 **DROP 是另一
 * 回事**：那张表已经不存在了，登记再留着只会误伤后来者 —— 提示原文写的就是
 * 「DROP TABLE 会连同水位/clog 文件一并清理」，而此前**只清了文件、没清这个
 * 集合**，承诺与实现对不上。
 *
 * 后果实测过（R-P6-9）：OID 一被复用，一张毫不相干的新表就被
 * shard_oid_is_mvcc() 判成分片打标表；它若是分布式表，DROP 走 2PC 就撞上
 * §10 的 PRE_PREPARE 禁令而删不掉，夹具残表逐轮累积（122→244→366），
 * 症状伪装成"回放写多了"。
 *
 * 只在 DROP 提交时点调用（ShardClogAtCommit），与文件 GC 同一处。
 */
void
ShardMvccSetRemove(Oid relid)
{
	int			i;

	if (ShardXidCtl == NULL)
		return;					/* shmem 未起：没什么可摘 */

	LWLockAcquire(ShardXidCtl->lock, LW_EXCLUSIVE);
	for (i = 0; i < ShardXidCtl->mvcc_n; i++)
		if (ShardXidCtl->mvcc_set[i] == relid)
		{
			/* 末位填洞再降计数：读侧持锁遍历，顺序对它不可见 */
			ShardXidCtl->mvcc_set[i] =
				ShardXidCtl->mvcc_set[ShardXidCtl->mvcc_n - 1];
			ShardXidCtl->mvcc_n--;
			break;
		}
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
	shard_xid_persist_watermark(relid, 0, 0, 0, 0);
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
 * T6.3c：副本壳表的维护命令闸门。判据是"本地是副本"，与白名单无关。
 * 只拦会**动文件字节**或**触发回收判定**的那几类；DROP 不拦（清理要用）。
 */
static void
shard_replica_guard_rv(RangeVar *rv, const char *what)
{
	Oid relid;

	if (rv == NULL)
		return;
	relid = RangeVarGetRelid(rv, NoLock, true /* missing_ok */);
	ShardReplicaAccessGate(relid, what);
}

static void
shard_replica_utility_guard(Node *parsetree)
{
	if (ReplayCtl == NULL || ReplayCtl->nreplicas == 0)
		return;					/* 无副本：零成本返回 */

	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *stmt = (VacuumStmt *) parsetree;
		ListCell   *lc;
		const char *what = stmt->is_vacuumcmd ? "VACUUM" : "ANALYZE";

		/*
		 * ★ 整库形态只拦 VACUUM，**不拦 ANALYZE**。
		 *
		 * 与打标表那一支的处置逐条对齐：那边同样"白名单非空时不许整库 VACUUM"，
		 * 而 ANALYZE 早在 T2.6 就已解禁（读侧走补丁 0008 分叉，只判不收）。
		 * 整库 ANALYZE 是节点级的日常操作，因为存在一份副本就让它全库失败，
		 * 是那种"最后一定会被运维关掉"的守卫 —— 首版这么写，当场把
		 * shard_clog_p2 的"整库 ANALYZE 放行"打红。
		 */
		if (stmt->rels == NIL)
		{
			if (stmt->is_vacuumcmd)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("本节点持有副本壳表时不允许整库 VACUUM"),
						 errdetail("副本元组带外来分片 xid，原生回收路径会误判；"
								   "且回收动作写本地 WAL，会重新打开 §13 约束 12 的洞。"),
						 errhint("请点名不含副本壳表的目标表。")));
			return;				/* 整库 ANALYZE 放行 */
		}

		foreach(lc, stmt->rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);

			if (OidIsValid(vrel->oid))
				ShardReplicaAccessGate(vrel->oid, what);
			shard_replica_guard_rv(vrel->relation, what);
		}
	}
	else if (IsA(parsetree, ClusterStmt))
		shard_replica_guard_rv(((ClusterStmt *) parsetree)->relation, "CLUSTER");
	/*
	 * ★ CREATE INDEX / REINDEX **不拦** —— 它们是 §12 写明的修复路径的一部分。
	 *
	 * 结构栅栏立起来之后，文档给运维的指令原文是"请在本地 shell 表上做等价
	 * 结构变更后重跑 replay_set_locmap()" —— 那个"等价结构变更"就是在副本壳表
	 * 上 CREATE INDEX。把它拦掉等于**把文档写明的唯一修复路径堵死**
	 * （首版这么写，当场把 ddl_fileset_d1 从 78/0 打到 65/11）。
	 *
	 * 安全性论证：这一步之后紧跟着的是 CTRL:FILESET_UPDATE 与那批 FPI，
	 * 新索引文件的内容**会被流整体重建**，本地建索引时读堆判活的那点结果
	 * 一个字节都不会留下。
	 */
	else if (IsA(parsetree, TruncateStmt))
	{
		ListCell *lc;

		foreach(lc, ((TruncateStmt *) parsetree)->relations)
			shard_replica_guard_rv((RangeVar *) lfirst(lc), "TRUNCATE");
	}
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

	if (parsetree == NULL)
		return;

	/*
	 * ★ T6.3c：副本壳表这一支的门控条件与打标表**不同**，必须先查。
	 *
	 * follower 节点从不设 pg_partdist.shard_relids，白名单恒空 ⇒
	 * shard_gating_active() 恒假 ⇒ 下面整段对副本壳表统统不生效。
	 * 而副本壳表恰恰是最不能被 VACUUM / ANALYZE / CLUSTER 碰的东西：
	 * 它的元组带外来分片 xid，原生回收路径会把活元组当垃圾清掉，
	 * 且这些动作都写**本地 WAL**（§13 约束 12 的洞）。
	 */
	shard_replica_utility_guard(parsetree);

	if (!shard_gating_active(cfg))
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

/* ---- T5.6 观测点：分片 xid 龄与护栏相位 ---- */
PG_FUNCTION_INFO_V1(partdist_shard_xid_age);
Datum
partdist_shard_xid_age(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId next = ShardXidNextToIssue(shard);
	TransactionId tb;
	TransactionId age;
	int			phase;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	ShardVacuumGetWatermarks(shard, &tb, NULL);
	age = (next <= tb) ? 0 : next - tb;
	phase = (age >= (TransactionId) shard_xid_stop_age) ? 2
		: (age >= (TransactionId) shard_vacuum_max_age) ? 1 : 0;

	values[0] = Int64GetDatum((int64) age);
	values[1] = Int32GetDatum(phase);
	values[2] = Int64GetDatum((int64) shard_vacuum_max_age);
	values[3] = Int64GetDatum((int64) shard_xid_stop_age);
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/* ---- U-P5-1 观测点：本分片下一个待发号 ---- */
PG_FUNCTION_INFO_V1(partdist_shard_xid_next);
Datum
partdist_shard_xid_next(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64((int64) ShardXidNextToIssue(PG_GETARG_OID(0)));
}
