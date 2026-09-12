/*
 * shard_route.c — R3 读路径的路由与 xid 翻译（FOLLOWER_REPLAY_DESIGN.md §9.4）。
 *
 * 见 include/shard_route.h 的文件头注释。这里只记实现上的三个决定：
 *
 * ① 键用本地关系 OID，数据源用 apply_checkpoint。
 *    回放侧 `ctx->shard_oid` 就是本地壳表 OID，apply_checkpoint 也按它编址
 *    （pg_parwal/<oid>/apply_checkpoint），而可见性钩子拿到的 t_tableOid 正是
 *    同一个值 —— 于是"这张表是不是回放来的壳表"这个问题，可以只用一次
 *    open() 回答，**完全不碰 catalog**。这是硬要求：可见性判定可能在持缓冲区
 *    锁时被调用。
 *
 * ② xid_map 缓存成**按 local_xid 升序的数组**，不是 HTAB。
 *    单分区上限 100 万条 × 16 字节 = 16MB，数组比 HTAB 省一半以上内存，二分
 *    查找对这个量级足够快；而且 apply_checkpoint 本来就是数组格式，省一次
 *    重建。比较用无符号序：一次回放窗口内的 xid 不会跨回卷，真跨了也只是
 *    查不到（退回本机语义），不会给出错误判决。
 *
 * ③ 缓存**只在本后端内**，并且分两种刷新节奏：
 *    · 判定为"不是壳表"的关系（绝大多数）——每 1s 至多再探一次。代价是一个
 *      必然 ENOENT 的 open()，不缓存的话每条元组都要探一次。
 *    · 判定为壳表的关系 —— 查不到某个 xid 时才重读 checkpoint（限 200ms 一次）。
 *      副本还在追平时 xid_map 会长，这条自愈路径让读者不必等到下次连接。
 *      已升主的分片不再回放，账本是终态，重读也只是白读一次。
 */
#include "pg_partdist.h"

#include "shard_route.h"
#include "partition_wal.h"
#include "shard_fileset.h"
#include "shard_replay.h"
#include "shard_xidmap.h"

#include "enhanced_clog.h"

#include "funcapi.h"
#include "utils/builtins.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "access/xact.h"
#include "utils/timestamp.h"

/*
 * 再探节奏：**每关系每事务至多一次**。
 *
 * ★ 不能按墙钟。第一版写的是 `TimestampDifferenceExceeds(last, GetCurrentTimestamp(), …)`,
 *   而 sv_satisfies_mvcc 现在对**每一条元组**都会走到这里 —— 普通表的全表扫描
 *   会变成"每行一次 gettimeofday"。判据换成事务开始时间戳：它是事务开头算好的
 *   静态值，取用不花钱，而且"一个事务内路由结论恒定"本身就是更干净的语义
 *   （扫描中途换结论只会让同一条 SELECT 自相矛盾）。
 */
#define ROUTE_PROBE_EPOCH()     GetCurrentTransactionStartTimestamp()

typedef struct RouteCacheEntry
{
    Oid             reloid;         /* 键 */
    bool            is_shell;
    ShardRole       role;
    TransactionId   watermark;      /* 仅 SHARD_PROMOTED 有意义 */
    uint32          nxidmap;
    XidMapEntry    *xidmap;         /* 按 local_xid 升序；NULL = 空 */
    uint64          ckpt_lsn;       /* 快照来源 checkpoint 的 durable_part_lsn */
    TimestampTz     probe_epoch;    /* 上次读 checkpoint 时的事务开始时间戳 */
} RouteCacheEntry;

static HTAB         *RouteCache = NULL;
static MemoryContext RouteCacheCtx = NULL;

static int
xidmap_cmp(const void *a, const void *b)
{
    TransactionId xa = ((const XidMapEntry *) a)->local_xid;
    TransactionId xb = ((const XidMapEntry *) b)->local_xid;

    if (xa < xb) return -1;
    if (xa > xb) return 1;
    return 0;
}

static void
route_cache_init(void)
{
    HASHCTL ctl;

    if (RouteCache != NULL)
        return;

    RouteCacheCtx = AllocSetContextCreate(TopMemoryContext,
                                          "pg_partdist route cache",
                                          ALLOCSET_SMALL_SIZES);
    memset(&ctl, 0, sizeof(ctl));
    ctl.keysize   = sizeof(Oid);
    ctl.entrysize = sizeof(RouteCacheEntry);
    ctl.hcxt      = RouteCacheCtx;
    RouteCache = hash_create("pg_partdist route cache", 32, &ctl,
                             HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

void
PartDistRouteCacheReset(void)
{
    if (RouteCache == NULL)
        return;
    hash_destroy(RouteCache);
    RouteCache = NULL;
    MemoryContextDelete(RouteCacheCtx);
    RouteCacheCtx = NULL;
}

/*
 * 重读 apply_checkpoint，刷新这一条缓存。
 *
 * ReadApplyCheckpoint 在任何异常（文件缺失、magic/版本不符、CRC 不符、
 * 快照截断）下都只返回 false 或发 WARNING，**不会 ereport(ERROR)** ——
 * 这正是能把它放进可见性路径的前提：读判决的路上不该冒出事务级错误。
 */
/*
 * 只读 checkpoint 的**头部**，用来判断"账本变没变"。
 *
 * 不验 CRC —— 它覆盖头+全部条目，头部单独读没法验。这没关系：本函数只作
 * 变化探测器，真要用条目时走 route_entry_load()，那条路会完整校验。
 * 返回 false = 没有 checkpoint / 头不认识，一律按"不是壳表"处理。
 */
static bool
route_peek_header(Oid reloid, uint64 *ckpt_lsn, uint32 *nxidmap,
                  TransactionId *watermark)
{
    char                 path[MAXPGPATH];
    int                  fd;
    ssize_t              nb;
    ShardApplyCheckpoint hdr;

    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, reloid, APPLY_CHECKPOINT_FILENAME);
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        return false;
    nb = read(fd, &hdr, sizeof(hdr));
    CloseTransientFile(fd);
    if (nb != (ssize_t) sizeof(hdr) ||
        hdr.magic != APPLY_CHECKPOINT_MAGIC ||
        hdr.version != APPLY_CHECKPOINT_VERSION ||
        hdr.shard_oid != reloid ||
        hdr.nxidmap > SHARD_XIDMAP_MAX_ENTRIES)
        return false;

    *ckpt_lsn  = hdr.durable_part_lsn;
    *nxidmap   = hdr.nxidmap;
    *watermark = (TransactionId)
        XidFromFullTransactionId(FullTransactionIdFromU64(hdr.max_replayed_fxid));
    return true;
}

static void
route_entry_load(RouteCacheEntry *e)
{
    ShardApplyCheckpoint chk;
    XidMapEntry         *ents = NULL;
    MemoryContext        old;

    e->probe_epoch = ROUTE_PROBE_EPOCH();

    old = MemoryContextSwitchTo(RouteCacheCtx);

    if (!ReadApplyCheckpoint(e->reloid, &chk, &ents))
    {
        /* 没有 apply_checkpoint ⇒ 本节点没回放过它 ⇒ §9.4 规则 1 */
        MemoryContextSwitchTo(old);
        if (e->xidmap != NULL)
        {
            pfree(e->xidmap);
            e->xidmap = NULL;
        }
        e->is_shell  = false;
        e->role      = SHARD_NATIVE_LEADER;
        e->watermark = InvalidTransactionId;
        e->nxidmap   = 0;
        e->ckpt_lsn  = 0;
        return;
    }

    if (e->xidmap != NULL)
        pfree(e->xidmap);

    e->xidmap  = ents;                  /* 已在 RouteCacheCtx 里 palloc */
    e->nxidmap = chk.nxidmap;
    if (e->nxidmap > 1 && e->xidmap != NULL)
        qsort(e->xidmap, e->nxidmap, sizeof(XidMapEntry), xidmap_cmp);

    MemoryContextSwitchTo(old);

    e->is_shell  = true;
    e->role      = ShardPromotedMarkRead(e->reloid)
                   ? SHARD_PROMOTED : SHARD_FOLLOWER_REPLAYED;
    e->watermark = (TransactionId)
        XidFromFullTransactionId(FullTransactionIdFromU64(chk.max_replayed_fxid));
    e->ckpt_lsn  = chk.durable_part_lsn;
}

static RouteCacheEntry *
route_entry_get(Oid reloid)
{
    RouteCacheEntry *e;
    bool             found;

    if (!OidIsValid(reloid))
        return NULL;

    route_cache_init();
    e = (RouteCacheEntry *) hash_search(RouteCache, &reloid, HASH_ENTER, &found);
    if (!found)
    {
        e->is_shell   = false;
        e->role       = SHARD_NATIVE_LEADER;
        e->watermark  = InvalidTransactionId;
        e->nxidmap    = 0;
        e->xidmap     = NULL;
        e->ckpt_lsn   = 0;
        e->probe_epoch = 0;
        route_entry_load(e);
        return e;
    }

    if (e->probe_epoch == ROUTE_PROBE_EPOCH())
        return e;                   /* 本事务内已探过，结论恒定 */

    if (!e->is_shell)
    {
        /* 负缓存：壳表是后建的，长连接不能一直认旧结论 */
        route_entry_load(e);
        return e;
    }

    /*
     * 已认定是壳表：**角色和水位每事务都要刷**（升主会被回收 —— 实测见过
     * 一轮里两次选举，先当选后又退回副本；缓存里留着 promoted 会让
     * "xid > W 归本机"那一支判错边）。但 xid_map 可能有上百万条，不能每事务
     * 重读一遍 —— 只在 checkpoint 的 (游标, 条目数) 真的变了时才整张重载。
     */
    {
        uint64        lsn = 0;
        uint32        n = 0;
        TransactionId wm = InvalidTransactionId;

        e->probe_epoch = ROUTE_PROBE_EPOCH();
        if (!route_peek_header(e->reloid, &lsn, &n, &wm))
        {
            route_entry_load(e);    /* checkpoint 没了：退回本机语义 */
            return e;
        }
        if (lsn != e->ckpt_lsn || n != e->nxidmap)
        {
            route_entry_load(e);
            return e;
        }
        e->watermark = wm;
        e->role = ShardPromotedMarkRead(e->reloid)
                  ? SHARD_PROMOTED : SHARD_FOLLOWER_REPLAYED;
    }
    return e;
}

bool
PartDistRouteLookup(Oid local_reloid, ShardRouteInfo *out)
{
    RouteCacheEntry *e = route_entry_get(local_reloid);

    if (e == NULL || !e->is_shell)
        return false;

    if (out != NULL)
    {
        out->shard_oid = e->reloid;
        out->role      = e->role;
        out->watermark = e->watermark;
        out->nxidmap   = e->nxidmap;
    }
    return true;
}

static GlobalTransactionId
xidmap_search(RouteCacheEntry *e, TransactionId xid)
{
    uint32 lo = 0;
    uint32 hi;

    if (e->xidmap == NULL || e->nxidmap == 0)
        return InvalidGlobalXid;

    hi = e->nxidmap;
    while (lo < hi)
    {
        uint32 mid = lo + (hi - lo) / 2;
        TransactionId m = e->xidmap[mid].local_xid;

        if (m == xid)
            return e->xidmap[mid].gxid;
        if (m < xid)
            lo = mid + 1;
        else
            hi = mid;
    }
    return InvalidGlobalXid;
}

GlobalTransactionId
PartDistResolveGxid(Oid local_reloid, TransactionId xid)
{
    RouteCacheEntry *e = route_entry_get(local_reloid);

    if (e == NULL || !e->is_shell || !TransactionIdIsNormal(xid))
        return InvalidGlobalXid;

    /*
     * §9.4 规则 3 的后一半：升主之后本机自己发的号严格大于 W（§7.5 水位隔离），
     * 归本机命名空间。不先挡这一下，新主自己写的行会去查一本不属于它的账。
     */
    if (e->role == SHARD_PROMOTED &&
        TransactionIdIsValid(e->watermark) &&
        TransactionIdFollows(xid, e->watermark))
        return InvalidGlobalXid;

    /*
     * 查不到就是查不到，这里**不再重读**：`route_entry_get()` 已经在本事务的
     * 第一次访问时探过头部，账本长了就整张重载过了。事务中途新回放进来的
     * 条目本来也不该对本事务可见（快照语义），所以"一个事务内账本恒定"
     * 既省事又更对。返回 InvalidGlobalXid ⇒ 调用方退回本机语义。
     */
    return xidmap_search(e, xid);
}

/* ================================================================== */
/* 取证面                                                              */
/* ================================================================== */

/*
 * route_resolve(rel oid, xid bigint)
 *   → (role text, watermark bigint, nxidmap int, gxid bigint, status text)
 *
 * 把 §9.4 的两跳摊开给验收看：这张表被判成什么角色、xid_map 里有多少条、
 * 这个 xid 翻成哪个 gxid、gclog 给的判决是什么。
 *
 * ★ 为什么要有它：只断言"升主后读到 40 行"是**结果正确**，证不了**路径正确** ——
 *   万一哪天本机 clog 里碰巧同号的事务也是 committed，行照样读得出来，
 *   而 R3 一步都没走。取证函数让"走没走这条路"可断言。
 */
PG_FUNCTION_INFO_V1(partdist_route_resolve);

Datum
partdist_route_resolve(PG_FUNCTION_ARGS)
{
    Oid                 reloid = PG_GETARG_OID(0);
    int64               xid_in = PG_GETARG_INT64(1);
    ShardRouteInfo      info;
    GlobalTransactionId gxid;
    TxnStatus           st = TXN_RUNNING;
    uint64              sts = 0;
    uint64              cts = 0;
    Datum               values[5];
    bool                nulls[5];
    TupleDesc           tupdesc;
    HeapTuple           tuple;
    const char         *rolename;
    const char         *stname;

    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "partdist.route_resolve 必须以记录形式调用");
    tupdesc = BlessTupleDesc(tupdesc);

    memset(nulls, 0, sizeof(nulls));

    if (!PartDistRouteLookup(reloid, &info))
    {
        rolename = "native_leader";
        values[0] = CStringGetTextDatum(rolename);
        nulls[1] = nulls[2] = nulls[3] = nulls[4] = true;
        tuple = heap_form_tuple(tupdesc, values, nulls);
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }

    rolename = (info.role == SHARD_PROMOTED) ? "promoted" : "replica";
    values[0] = CStringGetTextDatum(rolename);
    values[1] = Int64GetDatum((int64) info.watermark);
    values[2] = Int32GetDatum((int32) info.nxidmap);

    gxid = PartDistResolveGxid(reloid, (TransactionId) xid_in);
    if (!GlobalXidIsValid(gxid))
    {
        nulls[3] = true;
        values[4] = CStringGetTextDatum("not_replayed");
        tuple = heap_form_tuple(tupdesc, values, nulls);
        PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
    }

    values[3] = Int64GetDatum((int64) gxid);
    (void) EnhancedClogReadStatus(gxid, &st, &sts, &cts);
    switch (st)
    {
        case TXN_COMMITTED: stname = "committed"; break;
        case TXN_ABORTED:   stname = "aborted";   break;
        case TXN_PREPARED:  stname = "prepared";  break;
        default:            stname = "running";   break;
    }
    values[4] = CStringGetTextDatum(stname);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
