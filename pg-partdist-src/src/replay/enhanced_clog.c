/*
 * enhanced_clog.c
 *
 * 增强型 CLOG（pg_gclog）写路径最小实现（FRD §9.3）。
 *
 * 形态：pg_gclog/<node_id>/<8 位十六进制段号>，段内扁平定长槽数组，
 * 槽号 = 段内 xid 偏移，pwrite/pread 直接寻址。write + 延迟 fsync。
 *
 * 关于并发（这是本文件唯一的新并发面）：
 * 回放 worker 是**每分区一个**，而 gclog 是**每来源节点一套** —— 于是同一份
 * 段文件会被多个 worker 同时写。这与 ShardReplayCtx 那套"每分区独占、无需加锁"
 * 的前提不同，不能照搬。
 *
 * 但真正需要互斥的只有段文件的创建：两个 worker 同时发现段不存在会各建一次，
 * O_CREAT 不带 O_EXCL 本就允许（都成功、同一个文件），所以连这也不是竞态。
 * 槽内容本身是幂等的：
 *   - 同一 gxid 被两个 worker 写，是因为该事务跨了两个分片，两条 MARKER 的
 *     载荷来自同一次提交，算出的 24 字节完全相同 —— 任何交错都得到同样的字节；
 *   - 同一 worker 重放同一条 MARKER（崩溃恢复），同理。
 * 因此写路径不取锁。**唯一的例外**是同一 gxid 先 COMMIT 后 ABORT
 * （R2-b 已把这个窗口压到"标记自身复制失败"这一种，且同分区内按流序串行应用，
 * 末态正确）。跨分区交错出现该序列在当前设计下不可达 —— 一个事务的两条
 * MARKER 判决必然相同。
 *
 * SLRU 化、共享缓冲与 GC 属全局 MVCC 文档范围，本文件不做。
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "enhanced_clog.h"

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/memutils.h"

StaticAssertDecl(sizeof(EnhancedClogSlot) == 24,
                 "EnhancedClogSlot 必须是 24 字节（pg_gclog 的磁盘格式）");
StaticAssertDecl(offsetof(EnhancedClogSlot, status) == 16,
                 "status 必须落在偏移 16");

/*
 * 每次 I/O 都 open/close，**不缓存 fd** —— 与内核 SLRU 同款做法
 * （slru.c 的 SlruPhysicalReadPage/WritePage 都是开-读写-关）。
 *
 * 不能缓存的理由是硬的：OpenTransientFile 拿到的 fd 登记在 resource owner 上，
 * 事务结束时会被统一关掉。跨事务缓存它，轻则像 SQL 函数那样报
 * "temporary files and directories not closed at end-of-transaction"，
 * 重则 fd 已被关闭并被别处复用，后续 pwrite 打到不相干的文件上。
 *
 * 开销可以忽略：MARKER 是"每事务每分区一条"，而每条记录本来就要走一次带
 * fsync 的 Raft 往返，多一次 open/close 完全淹没在里面。
 *
 * 只记"哪些段被本进程写脏了"，供 EnhancedClogSync() 延迟 fsync。
 * 每个回放 worker 一份，互不影响 —— worker A 不需要替 B 刷盘，
 * B 自己的 apply checkpoint 会刷（§8.4 的推进协议是每分区各自推进的）。
 */
typedef struct GClogDirtySeg
{
    uint16  node_id;
    uint32  segno;
} GClogDirtySeg;

#define GCLOG_MAX_DIRTY_SEGS 16

static GClogDirtySeg gclog_dirty[GCLOG_MAX_DIRTY_SEGS];
static int           gclog_ndirty = 0;

static void
GClogSegPath(char *path, size_t pathlen, uint16 node_id, uint32 segno)
{
    snprintf(path, pathlen, "%s/%s/%u/%08X",
             DataDir, GCLOG_DIR, (unsigned) node_id, segno);
}

/*
 * 确保 pg_gclog/ 与 pg_gclog/<node_id>/ 存在。
 * 已存在（EEXIST）是正常情况 —— 多个 worker 并发建同一个目录。
 */
static void
GClogEnsureDir(uint16 node_id)
{
    char path[MAXPGPATH];

    snprintf(path, MAXPGPATH, "%s/%s", DataDir, GCLOG_DIR);
    if (MakePGDirectory(path) != 0 && errno != EEXIST)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法创建目录 \"%s\": %m", path)));

    snprintf(path, MAXPGPATH, "%s/%s/%u", DataDir, GCLOG_DIR,
             (unsigned) node_id);
    if (MakePGDirectory(path) != 0 && errno != EEXIST)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法创建目录 \"%s\": %m", path)));
}

/*
 * 打开一个段。create == false 且文件不存在时返回 -1（读路径据此当作空洞）。
 * 调用方负责 CloseTransientFile。
 */
static int
GClogOpenSegFile(uint16 node_id, uint32 segno, bool create)
{
    char path[MAXPGPATH];
    int  fd;
    int  flags;

    if (create)
        GClogEnsureDir(node_id);

    GClogSegPath(path, MAXPGPATH, node_id, segno);
    flags = (create ? (O_RDWR | O_CREAT) : O_RDONLY) | PG_BINARY;

    fd = OpenTransientFile(path, flags);
    if (fd < 0)
    {
        if (!create && errno == ENOENT)
            return -1;          /* 该段从未写过 —— 全洞 */
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法打开 gclog 段 \"%s\": %m", path)));
    }
    return fd;
}

/*
 * 记下"这个段被写脏了"。
 *
 * 脏段列表满了就地 fsync 一次全清 —— 宁可多刷一次盘，也不能丢掉某个段
 * "待 fsync"的事实：漏刷就是 checkpoint 宣称已持久化而判决其实还在 page cache。
 */
static void GClogSyncAll(void);

static void
GClogMarkDirty(uint16 node_id, uint32 segno)
{
    int i;

    for (i = 0; i < gclog_ndirty; i++)
        if (gclog_dirty[i].node_id == node_id && gclog_dirty[i].segno == segno)
            return;

    if (gclog_ndirty >= GCLOG_MAX_DIRTY_SEGS)
        GClogSyncAll();

    gclog_dirty[gclog_ndirty].node_id = node_id;
    gclog_dirty[gclog_ndirty].segno   = segno;
    gclog_ndirty++;
}

void
EnhancedClogWriteStatus(GlobalTransactionId gxid,
                        uint64 start_ts, uint64 commit_ts,
                        TxnStatus status)
{
    uint16            node_id = GxidNodeId(gxid);
    uint64            local_xid = GxidLocalXid(gxid);
    uint32            segno;
    off_t             off;
    EnhancedClogSlot  slot;
    int               fd;
    ssize_t           nb;

    if (local_xid == 0)
        return;                 /* 无 xid 的记录，无账可记 */

    segno = (uint32) (local_xid / GCLOG_XIDS_PER_SEGMENT);
    off   = (off_t) (local_xid % GCLOG_XIDS_PER_SEGMENT) * GCLOG_SLOT_SIZE;

    memset(&slot, 0, sizeof(slot));   /* reserved 与任何将来字段都归零 */
    slot.start_ts  = start_ts;
    slot.commit_ts = commit_ts;
    slot.status    = (uint32) status;

    fd = GClogOpenSegFile(node_id, segno, true);

    do {
        nb = pg_pwrite(fd, &slot, sizeof(slot), off);
    } while (nb < 0 && errno == EINTR);

    if (nb != (ssize_t) sizeof(slot))
    {
        CloseTransientFile(fd);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: gclog 写入失败 node=%u xid=%llu: %m",
                        node_id, (unsigned long long) local_xid)));
    }
    CloseTransientFile(fd);

    GClogMarkDirty(node_id, segno);
}

bool
EnhancedClogReadStatus(GlobalTransactionId gxid, TxnStatus *status,
                       uint64 *start_ts, uint64 *commit_ts)
{
    uint16            node_id = GxidNodeId(gxid);
    uint64            local_xid = GxidLocalXid(gxid);
    uint32            segno;
    off_t             off;
    EnhancedClogSlot  slot;
    int               fd;
    ssize_t           nb;

    if (status != NULL)     *status = TXN_RUNNING;
    if (start_ts != NULL)   *start_ts = 0;
    if (commit_ts != NULL)  *commit_ts = 0;

    if (local_xid == 0)
        return false;

    segno = (uint32) (local_xid / GCLOG_XIDS_PER_SEGMENT);
    off   = (off_t) (local_xid % GCLOG_XIDS_PER_SEGMENT) * GCLOG_SLOT_SIZE;

    fd = GClogOpenSegFile(node_id, segno, false);
    if (fd < 0)
        return true;            /* 整段没建过 = 全洞 = 未决 */

    do {
        nb = pg_pread(fd, &slot, sizeof(slot), off);
    } while (nb < 0 && errno == EINTR);

    if (nb != (ssize_t) sizeof(slot) && nb != 0)
    {
        CloseTransientFile(fd);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: gclog 读取不完整 node=%u xid=%llu (%zd/%u)",
                        node_id, (unsigned long long) local_xid,
                        nb, GCLOG_SLOT_SIZE)));
    }
    CloseTransientFile(fd);

    if (nb == 0)
        return true;            /* 读到文件尾之外 = 洞 */

    if (status != NULL)     *status = (TxnStatus) slot.status;
    if (start_ts != NULL)   *start_ts = slot.start_ts;
    if (commit_ts != NULL)  *commit_ts = slot.commit_ts;
    return true;
}

static void
GClogSyncAll(void)
{
    int i;

    for (i = 0; i < gclog_ndirty; i++)
    {
        int fd = GClogOpenSegFile(gclog_dirty[i].node_id,
                                  gclog_dirty[i].segno, false);

        if (fd < 0)
            continue;           /* 段被删了（GC）—— 没什么可刷的 */
        if (pg_fsync(fd) != 0)
        {
            CloseTransientFile(fd);
            ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("pg_partdist: gclog 段 node=%u seg=%08X fsync 失败: %m",
                            gclog_dirty[i].node_id, gclog_dirty[i].segno)));
        }
        CloseTransientFile(fd);
    }
    gclog_ndirty = 0;
}

void
EnhancedClogSync(void)
{
    GClogSyncAll();
}

void
EnhancedClogCloseAll(void)
{
    /* 不再缓存 fd，只需把待刷的段刷掉 */
    GClogSyncAll();
}
