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
#include "storage/fd.h"

#include <fcntl.h>
#include <unistd.h>
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
/*
 * groupid 的持久化侧影（T4.5②，R-P4-6 定谳产物）。
 *
 * 背景：demux 等**无 DB 连接**的 bgworker 在崩溃恢复扫描里逐条 append 时
 * 会经 PartDistLocalNodeId() 走到这里；无连接进程的 catcache 是 NULL，
 * get_relname_relid → SearchCatCache 直接 SIGSEGV → postmaster 整节点重置
 * （实测 worker1 十二连崩，选举乱象全是它的下游）。
 *
 * 修法：backend 首次经 catalog 解析成功后把 groupid 写进
 * $PGDATA/pg_partdist_groupid；无 DB 语境改读文件。**文件必在**的序论证：
 * WAL 里能出现分片记录 ⇒ 必有 backend 在线捕获过 ⇒ 该 backend 已解析并
 * 落盘 groupid。文件缺失 ⇒ 本节点从未有过分片写 ⇒ 恢复扫描不会命中任何
 * 分片记录，兜底 -1（→节点号 0）不会被真实使用。
 */
static void
partdist_groupid_persist(int32 gid)
{
    char        path[MAXPGPATH];
    char        buf[16];
    int         fd;

    snprintf(path, sizeof(path), "%s/pg_partdist_groupid", DataDir);
    fd = BasicOpenFile(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
    if (fd < 0)
        return;                 /* 尽力而为：写不动下次再写 */
    snprintf(buf, sizeof(buf), "%d\n", gid);
    if (write(fd, buf, strlen(buf)) > 0)
        (void) pg_fsync(fd);
    close(fd);
}

static int32
partdist_groupid_from_file(void)
{
    char        path[MAXPGPATH];
    FILE       *fp;
    int         v = -1;

    snprintf(path, sizeof(path), "%s/pg_partdist_groupid", DataDir);
    fp = AllocateFile(path, "r");
    if (fp == NULL)
        return -1;
    if (fscanf(fp, "%d", &v) != 1)
        v = -1;
    FreeFile(fp);
    return v;
}

static int32
PartDistCitusGroupIdInternal(void)
{
    static int32 cached_group_id = -2;      /* -2 = 尚未解析 */
    Oid          oid;

    if (cached_group_id != -2)
        return cached_group_id;

    /* 无 DB 连接（demux/纯 shmem worker）：绝不摸 catalog，读持久化侧影 */
    if (!OidIsValid(MyDatabaseId))
    {
        cached_group_id = partdist_groupid_from_file();
        return cached_group_id;
    }

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

    /* 解析成功即落盘侧影（含 -1："无 Citus 目录"也是稳定结论） */
    partdist_groupid_persist(cached_group_id);

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
