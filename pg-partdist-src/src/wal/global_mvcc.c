/*
 * global_mvcc.c
 *
 * 全局事务标识（gxid）的节点号解析 —— FRD §9.1。
 *
 * 详见 include/global_mvcc.h 的头注释：gxid 高 16 位是**来源节点号**，
 * 本文件负责回答"本节点是几号"这一个问题。
 */
#include "postgres.h"

#include "global_mvcc.h"
#include "partition_wal_header.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/*
 * 记录头是**裸结构体直写磁盘**的，字段偏移即磁盘格式。3.0 相对 2.0 只把
 * 32..39（旧 xid + 尾部填充）合成一个 uint64，总长必须仍是 40 —— 否则既有
 * 段文件全部读不出来，而这个错误只会在运行时表现为"魔数对、内容全是垃圾"。
 */
StaticAssertDecl(sizeof(PartWALRecord) == 40,
                 "PartWALRecord 必须保持 40 字节（parwal-2.0/3.0 同长）");
StaticAssertDecl(offsetof(PartWALRecord, gxid) == 32,
                 "gxid 必须落在偏移 32（2.0 的 xid 字段原位）");

/*
 * MARKER 载荷同样是裸结构体直写磁盘：24 字节定长头 + 子事务数组。
 * 变长了就得同步改 §4.3 与两侧的长度校验，先在编译期钉死。
 */
StaticAssertDecl(sizeof(TxnMarkerPayload) == 24,
                 "TxnMarkerPayload 必须是 24 字节（FRD §4.3 格式契约）");
StaticAssertDecl(offsetof(TxnMarkerPayload, nsubxacts) == 16,
                 "nsubxacts 必须落在偏移 16");

int partdist_node_id = -1;      /* GUC pg_partdist.node_id */

/*
 * Citus 的 pg_dist_local_group.groupid：协调节点 0，各 worker 全局唯一且稳定
 * （节点重装不变，正是 gxid 需要的性质）。没有 Citus 目录时返回 -1。
 *
 * 走 systable 扫描而不是 SPI —— 调用方可能在 flush 路径（PRE_COMMIT）上，
 * 那里已经在一个事务里但不宜再开 SPI 连接。
 */
static int32
PartDistCitusGroupIdInternal(void)
{
    static int32 cached_group_id = -2;      /* -2 = 尚未解析 */
    Oid          oid;

    if (cached_group_id != -2)
        return cached_group_id;

    oid = get_relname_relid("pg_dist_local_group", PG_CATALOG_NAMESPACE);
    if (!OidIsValid(oid))
    {
        cached_group_id = -1;
        return -1;
    }

    PG_TRY();
    {
        Relation    rel;
        SysScanDesc scan;
        HeapTuple   tup;

        rel  = table_open(oid, AccessShareLock);
        scan = systable_beginscan(rel, InvalidOid, false, NULL, 0, NULL);
        tup  = systable_getnext(scan);
        if (HeapTupleIsValid(tup))
        {
            bool  isnull;
            Datum d = heap_getattr(tup, 1, RelationGetDescr(rel), &isnull);

            cached_group_id = isnull ? -1 : DatumGetInt32(d);
        }
        else
            cached_group_id = -1;
        systable_endscan(scan);
        table_close(rel, AccessShareLock);
    }
    PG_CATCH();
    {
        FlushErrorState();
        cached_group_id = -1;
    }
    PG_END_TRY();

    return cached_group_id;
}

int32
PartDistCitusGroupId(void)
{
    return PartDistCitusGroupIdInternal();
}

/*
 * PartDistLocalNodeId — 本节点在 gxid 高 16 位里的编号。
 *
 * 优先级：GUC pg_partdist.node_id（>= 0 时生效） > Citus group id > 0。
 *
 * 落到 0 有两种情况：本节点确实是协调节点（group id 就是 0），或者根本没有
 * Citus 目录（裸 PG 单测）。两者都不影响正确性 —— 单节点场景下本来就只有
 * 一个 xid 空间。真正需要区分来源的是"一个节点承载多个 leader 副本"，那种
 * 拓扑必然有 Citus 目录、group id 也必然 > 0。
 */
uint16
PartDistLocalNodeId(void)
{
    static int  cached = -1;
    int32       gid;

    if (partdist_node_id >= 0)
        return (uint16) partdist_node_id;

    if (cached >= 0)
        return (uint16) cached;

    gid = PartDistCitusGroupIdInternal();
    cached = (gid > 0) ? gid : 0;

    if (gid > 0 && gid > PG_UINT16_MAX)
        ereport(WARNING,
                (errmsg("pg_partdist: Citus group id %d 超出 gxid 的 16 位节点号"
                        "范围，gxid 的来源节点将发生回绕", gid),
                 errhint("用 GUC pg_partdist.node_id 显式指定一个 0..65535 的编号。")));

    return (uint16) cached;
}

void
DefineGlobalMVCCGUCs(void)
{
    /*
     * 与 pg_partdist.local_node_id 是**两回事**，不要合并：后者是路由层用的
     * "本节点在 partdist.node_map 里的编号"，PGC_USERSET，回归测试会在会话里
     * SET/RESET 它来构造 local/remote 路由分支。gxid 的节点号一旦在段流中途
     * 变化，xid_map 就会把两个来源记成同一个，所以这里必须是 PGC_POSTMASTER。
     */
    DefineCustomIntVariable("pg_partdist.node_id",
                            "gxid 高 16 位使用的本节点号；-1 = 取 Citus group id",
                            "全局事务标识（FRD §9.1）的来源节点编号。集群内必须唯一"
                            "且稳定；留空时自动取 pg_dist_local_group.groupid。",
                            &partdist_node_id,
                            -1, -1, PG_UINT16_MAX,
                            PGC_POSTMASTER, 0, NULL, NULL, NULL);
}
