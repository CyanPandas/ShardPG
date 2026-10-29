/*
 * shard_clog.c
 *
 * 分片级 clog 存储层最小实现（设计 §5.3，P2 T2.1）。
 * 语义、并发与持久化契约见 shard_clog.h 头注释。
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "shard_clog.h"
#include "partition_wal_header.h"	/* T7.2：基线 clog CTRL 载荷 */
#include "partwal_sync.h"			/* T7.2：PartWALAppendCtrl */
#include "shard_fileset.h"	/* T5.4b-2：水位 CTRL 发射 */
#include "shard_xid.h"
#include "port/pg_crc32c.h"
#include "shard_visibility.h"
#include "dtx_pending.h"
#include "access/xlogrecovery.h"

#include "access/transam.h"
#include "funcapi.h"			/* T5.2 SQL 包装：复合返回 */
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "common/relpath.h"

StaticAssertDecl(sizeof(ShardClogSlot) == 32,
				 "ShardClogSlot 必须是 32 字节（pg_shard_clog 的磁盘格式）");
StaticAssertDecl(offsetof(ShardClogSlot, status) == 24,
				 "status 必须落在偏移 24");

static void
ShardClogSegPath(char *path, size_t pathlen, Oid shard, uint32 segno)
{
	snprintf(path, pathlen, "%s/%s/%u/%08X",
			 DataDir, SHARD_CLOG_DIR, shard, segno);
}

static void
ShardClogDirPath(char *path, size_t pathlen, Oid shard)
{
	snprintf(path, pathlen, "%s/%s/%u", DataDir, SHARD_CLOG_DIR, shard);
}

/*
 * ShardClogDirExists —— 本节点有没有这个分片的分片 clog 目录。
 *
 * T7.4（R-P6-21）用它当"这张表是分片打标表"的**持久证据**：副本侧的
 * `pg_shard_clog/<oid>` 只由回放路径在收到带分片 xid 的 MARKER 时创建
 * （`shard_replay.c` 的 `ShardClogSetVerdict`/`SetPrepared` 分支），
 * 所以目录存在 ⇔ 这个分片的流里带过分片 xid ⇔ 它是打标分片。
 */
bool
ShardClogDirExists(Oid shard)
{
	char		path[MAXPGPATH];
	struct stat st;

	ShardClogDirPath(path, sizeof(path), shard);
	return (stat(path, &st) == 0 && S_ISDIR(st.st_mode));
}

static void ShardClogEnsureDir(Oid shard);

void
ShardClogEnsureEvidence(Oid shard)
{
	ShardClogEnsureDir(shard);
}

/* EEXIST 是常态 —— 多后端并发建同一个目录 */
static void
ShardClogEnsureDir(Oid shard)
{
	char		path[MAXPGPATH];

	snprintf(path, MAXPGPATH, "%s/%s", DataDir, SHARD_CLOG_DIR);
	if (MakePGDirectory(path) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建目录 \"%s\": %m", path)));

	ShardClogDirPath(path, MAXPGPATH, shard);
	if (MakePGDirectory(path) != 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建目录 \"%s\": %m", path)));
}

/* fsync 分片目录 —— 新建段文件后目录项也要持久（durable_rename 同款纪律） */
static void
ShardClogFsyncDir(Oid shard)
{
	char		path[MAXPGPATH];
	int			fd;

	ShardClogDirPath(path, MAXPGPATH, shard);
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法打开目录 \"%s\": %m", path)));
	if (pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 目录 \"%s\" fsync 失败: %m", path)));
	}
	CloseTransientFile(fd);
}

/*
 * 打开段文件。create=false 且不存在时返回 -1（读路径当作全洞）。
 * create=true 时若段是新建的，顺带 fsync 分片目录（*created 告知调用方，
 * 但目录持久化在本函数内已完成）。调用方负责 CloseTransientFile。
 */
static int
ShardClogOpenSegFile(Oid shard, uint32 segno, bool create)
{
	char		path[MAXPGPATH];
	int			fd;

	ShardClogSegPath(path, MAXPGPATH, shard, segno);

	/* 先试已存在的（多数路径），免掉目录操作 */
	fd = OpenTransientFile(path, (create ? O_RDWR : O_RDONLY) | PG_BINARY);
	if (fd >= 0)
		return fd;
	if (errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法打开分片 clog 段 \"%s\": %m", path)));
	if (!create)
		return -1;

	ShardClogEnsureDir(shard);
	fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 无法创建分片 clog 段 \"%s\": %m", path)));
	ShardClogFsyncDir(shard);
	return fd;
}

/* 写一个槽（幂等 pwrite，无锁 —— 论证见头文件）。durable=true 时 fsync 段。 */
static void
ShardClogWriteSlot(Oid shard, TransactionId sxid,
				   const ShardClogSlot *slot, bool durable)
{
	uint32		segno = sxid / SHARD_CLOG_XIDS_PER_SEGMENT;
	off_t		off = (off_t) (sxid % SHARD_CLOG_XIDS_PER_SEGMENT)
		* SHARD_CLOG_SLOT_SIZE;
	int			fd;
	ssize_t		nb;

	if (sxid < FirstNormalTransactionId)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pg_partdist: 分片 clog 拒写保留 xid %u（shard %u）",
						sxid, shard)));

	fd = ShardClogOpenSegFile(shard, segno, true);

	do
	{
		nb = pg_pwrite(fd, slot, sizeof(ShardClogSlot), off);
	} while (nb < 0 && errno == EINTR);

	if (nb != (ssize_t) sizeof(ShardClogSlot))
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 分片 clog 写入失败 shard=%u xid=%u: %m",
						shard, sxid)));
	}

	if (durable && pg_fsync(fd) != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 分片 clog 段 fsync 失败 shard=%u seg=%08X: %m",
						shard, segno)));
	}
	CloseTransientFile(fd);
}

void
ShardClogSetRunning(Oid shard, TransactionId sxid, int64 start_ts)
{
	ShardClogSlot slot;

	memset(&slot, 0, sizeof(slot));	/* 全零 = RUNNING；start_ts 是附注列 */
	slot.start_ts = (uint64) start_ts;
	ShardClogWriteSlot(shard, sxid, &slot, false);
}

void
ShardClogSetVerdict(Oid shard, TransactionId sxid, bool committed,
					int64 commit_ts)
{
	ShardClogSlot slot;

	/* 读改写：保留 RUNNING 落账时写下的 start_ts（§5.3 行五列） */
	if (!ShardClogReadSlot(shard, sxid, &slot))
		memset(&slot, 0, sizeof(slot));
	slot.status = (uint32) (committed ? TXN_COMMITTED : TXN_ABORTED);
	slot.commit_ts = committed ? (uint64) commit_ts : 0;
	ShardClogWriteSlot(shard, sxid, &slot, true);
}

void
ShardClogSetPrepared(Oid shard, TransactionId sxid, int64 start_ts, int64 gxid)
{
	ShardClogSlot slot;

	/* 读改写：RUNNING 落账里的 start_ts 若已在，以入参为准（同源同值） */
	if (!ShardClogReadSlot(shard, sxid, &slot))
		memset(&slot, 0, sizeof(slot));
	slot.status = (uint32) TXN_PREPARED;
	slot.start_ts = (uint64) start_ts;
	slot.global_xid = (uint64) gxid;
	ShardClogWriteSlot(shard, sxid, &slot, true);
}

/*
 * ShardClogEmitBaseline —— T7.2（R-P6-17）：把本分片 clog 里 [FirstNormal, upto)
 * 的槽切块塞进分区流，让副本拿到基线**之前**那些事务的判决。
 *
 * 为什么必须搬：基线只灌页面 + 抬发号水位，而基线游标之前的 MARKER 不再回放。
 * 于是"先有数据、后供给"的副本对基线前提交的每个分片 xid 都没有判决，那些行
 * 读成 RUNNING（不可见）；它一旦升主，`shard_claim_on_promote` 还会把这些
 * RUNNING 改判 ABORTED —— **已经提交的行就此消失**。此前只能靠"先供给、后写
 * 数据"的运维纪律绕过去，而那条纪律没有任何东西强制。
 *
 * 只搬**有内容**的槽块：全零块（= 全是 RUNNING 空洞）不发，省流量也省回放。
 * 幂等：同一块重放两次写同样的字节。
 *
 * 返回发出的块数。
 */
int
ShardClogEmitBaseline(Oid shard, TransactionId upto)
{
	TransactionId	x;
	int				nblocks = 0;
	char		   *buf;
	uint32			buflen;
	PartWALCtrlShardClog *hdr;
	char		   *slots;

	if (!TransactionIdIsNormal(upto))
		return 0;

	buflen = (uint32) PartWALCtrlShardClogSize(SHARD_CLOG_BASELINE_CHUNK);
	buf = palloc0(buflen);
	hdr = (PartWALCtrlShardClog *) buf;
	slots = PartWALCtrlShardClogSlots(buf);

    {
        TransactionId tb;
        ShardVacuumGetWatermarks(shard, &tb, NULL);
        x = tb ? tb : FIRST_SHARD_XID;
    }
    if (ShardXidDistance(x, upto) >= SHARD_XID_HALF)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("baseline xid interval exceeds half-circle")));
    while (x != upto)
    {
        uint32 n = Min((uint32) SHARD_CLOG_BASELINE_CHUNK, ShardXidDistance(x, upto));
        uint32 i;
        bool any = false;
        n = Min(n, SHARD_XID_MASK - x + 1);
        memset(slots, 0, (size_t) SHARD_CLOG_BASELINE_CHUNK * sizeof(ShardClogSlot));
        for (i = 0; i < n; i++)
        {
            ShardClogSlot slot;
            if (ShardClogReadSlot(shard, x + i, &slot))
            {
                memcpy(slots + (size_t) i * sizeof(slot), &slot, sizeof(slot));
                any = true;
            }
        }
        if (any)
        {
            hdr->first_sxid = x;
            hdr->nslots = n;
            PartWALAppendCtrl(shard, PARTWAL_CTRL_SHARD_CLOG, buf,
                              (uint32) PartWALCtrlShardClogSize(n));
            nblocks++;
        }
        x = ShardXidAdvance(x, n);
        CHECK_FOR_INTERRUPTS();
    }

	pfree(buf);
	if (nblocks > 0)
		ereport(LOG,
				(errmsg("pg_partdist: shard %u 的分片 clog 随基线发射 %d 块"
						"（上界 sxid=%u）", shard, nblocks, upto)));
	return nblocks;
}

/*
 * ShardClogApplyBaselineChunk —— follower 侧：把一块槽原样写进本地分片 clog。
 * 键是分片 xid，与 oid 无关，所以不需要任何翻译。幂等。
 */
static void
ShardClogSyncSegment(Oid shard, uint32 segno)
{
    int fd = ShardClogOpenSegFile(shard, segno, false);
    if (fd < 0 || pg_fsync(fd) != 0 || CloseTransientFile(fd) != 0)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot sync shard %u clog segment %u: %m", shard, segno)));
}

void
ShardClogApplyBaselineChunk(Oid shard, TransactionId first_sxid,
                            const char *slots, uint32 nslots)
{
    uint32 i, previous_seg = UINT32_MAX;
    bool dirty = false;
    if (!nslots || nslots > SHARD_CLOG_BASELINE_CHUNK ||
        first_sxid > SHARD_XID_MASK || nslots - 1 > SHARD_XID_MASK - first_sxid)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid shard clog baseline range")));
    for (i = 0; i < nslots; i++)
    {
        ShardClogSlot slot;
        uint32 segno = (first_sxid + i) / SHARD_CLOG_XIDS_PER_SEGMENT;
        memcpy(&slot, slots + (size_t) i * sizeof(slot), sizeof(slot));
        if (slot.status == 0) continue;
        if (dirty && previous_seg != segno)
        {
            ShardClogSyncSegment(shard, previous_seg);
            dirty = false;
        }
        ShardClogWriteSlot(shard, first_sxid + i, &slot, false);
        dirty = true; previous_seg = segno;
    }
    /* The last payload slot may be a hole; sync the last actual write. */
    if (dirty) ShardClogSyncSegment(shard, previous_seg);
}

bool
ShardClogReadSlot(Oid shard, TransactionId sxid, ShardClogSlot *out)
{
	uint32		segno = sxid / SHARD_CLOG_XIDS_PER_SEGMENT;
	off_t		off = (off_t) (sxid % SHARD_CLOG_XIDS_PER_SEGMENT)
		* SHARD_CLOG_SLOT_SIZE;
	int			fd;
	ssize_t		nb;

	memset(out, 0, sizeof(*out));	/* 缺席 = 全零 = RUNNING */

	/* 0/1/2 保留号永不落账；防御性按未决处理 */
	if (sxid < FirstNormalTransactionId)
		return false;

	fd = ShardClogOpenSegFile(shard, segno, false);
	if (fd < 0)
		return false;			/* 整段没建过 = 全洞 = 未决 */

	do
	{
		nb = pg_pread(fd, out, sizeof(*out), off);
	} while (nb < 0 && errno == EINTR);

	if (nb != (ssize_t) sizeof(*out) && nb != 0)
	{
		CloseTransientFile(fd);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_partdist: 分片 clog 读取不完整 shard=%u xid=%u (%zd/%u)",
						shard, sxid, nb, SHARD_CLOG_SLOT_SIZE)));
	}
	CloseTransientFile(fd);

	if (nb == 0)
	{
		memset(out, 0, sizeof(*out));	/* 文件尾之外 = 洞 */
		return false;
	}
	return true;
}

TxnStatus
ShardClogReadStatus(Oid shard, TransactionId sxid)
{
	ShardClogSlot slot;

	(void) ShardClogReadSlot(shard, sxid, &slot);
	return (TxnStatus) slot.status;
}

/*
 * T2.4 认领：[from, to) 内 RUNNING（含洞）→ ABORTED。逐段处理：段内逐槽
 * pread 判状态、RUNNING 则 pwrite ABORTED，段收尾一次 fsync。范围来自
 * "冻结的启动恢复上限"（调用方 shard_xid.c 保证，R-P2-2），本函数只管
 * 机械改判。PREPARED 保留是 §6.6 第二分支的"不许动"（P4 才有人写它）。
 */
/* A recovery log fence is indexed by the SAME raw 32-bit slot as clog. It is
 * a WAL byte address, never a transaction number or an epoch. Unlike a fence
 * for a whole numeric circle, per-slot fences also handle a transaction that
 * commits late across zero and reservations that straddle zero. */
#define SHARD_REDO_FENCE_MAGIC UINT32_C(0x53524631)
typedef struct ShardRedoFence
{
    XLogRecPtr lsn;
    pg_crc32c crc;
    uint32 magic;
} ShardRedoFence;
StaticAssertDecl(sizeof(ShardRedoFence) == 16, "redo fence disk format");

static void
shard_redo_fence_path(char *path, Oid shard, uint32 segno)
{
    snprintf(path, MAXPGPATH, "%s/pg_shard_xid/%u.redo/%08X", DataDir, shard, segno);
}

void
ShardClogSetRedoFenceRange(Oid shard, TransactionId from, TransactionId to, XLogRecPtr lsn)
{
    ShardRedoFence fences[512];
    TransactionId x = from;
    char dir[MAXPGPATH], path[MAXPGPATH];
    uint32 i;

    if (from == to) return;
    if (ShardXidDistance(from, to) >= SHARD_XID_HALF)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("redo fence range exceeds half-circle")));
    snprintf(dir, sizeof(dir), "%s/pg_shard_xid", DataDir);
    if (MakePGDirectory(dir) < 0 && errno != EEXIST)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot create redo fence parent: %m")));
    snprintf(dir, sizeof(dir), "%s/pg_shard_xid/%u.redo", DataDir, shard);
    if (MakePGDirectory(dir) < 0 && errno != EEXIST)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot create redo fence directory: %m")));
    fsync_fname("pg_shard_xid", true);
    memset(fences, 0, sizeof(fences));
    for (i = 0; i < lengthof(fences); i++)
    {
        fences[i].lsn = lsn;
        fences[i].magic = SHARD_REDO_FENCE_MAGIC;
        INIT_CRC32C(fences[i].crc);
        COMP_CRC32C(fences[i].crc, &fences[i].lsn, sizeof(lsn));
        COMP_CRC32C(fences[i].crc, &fences[i].magic, sizeof(uint32));
        FIN_CRC32C(fences[i].crc);
    }
    while (x != to)
    {
        uint32 segno = x / SHARD_CLOG_XIDS_PER_SEGMENT;
        int fd;
        shard_redo_fence_path(path, shard, segno);
        fd = OpenTransientFile(path, O_CREAT | O_RDWR | PG_BINARY);
        if (fd < 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot open redo fence %s: %m", path)));
        do
        {
            uint32 n = Min((uint32) lengthof(fences), ShardXidDistance(x, to));
            uint32 local = x % SHARD_CLOG_XIDS_PER_SEGMENT;
            size_t bytes;
            n = Min(n, SHARD_CLOG_XIDS_PER_SEGMENT - local);
            n = Min(n, SHARD_XID_MASK - x + 1);
            bytes = (size_t) n * sizeof(ShardRedoFence);
            if (pg_pwrite(fd, fences, bytes, (off_t) local * sizeof(ShardRedoFence)) != bytes)
                ereport(ERROR, (errcode_for_file_access(), errmsg("cannot write redo fence %s: %m", path)));
            x = ShardXidAdvance(x, n);
        } while (x != to && x / SHARD_CLOG_XIDS_PER_SEGMENT == segno);
        if (pg_fsync(fd) != 0 || CloseTransientFile(fd) != 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot sync redo fence %s: %m", path)));
    }
    fsync_fname(dir, true);
}

XLogRecPtr
ShardClogGetRedoFence(Oid shard, TransactionId sxid, bool required)
{
    char path[MAXPGPATH];
    ShardRedoFence fence;
    pg_crc32c crc;
    ssize_t n = 0;
    int fd;
    shard_redo_fence_path(path, shard, sxid / SHARD_CLOG_XIDS_PER_SEGMENT);
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0 && errno != ENOENT)
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot read redo fence %s: %m", path)));
    memset(&fence, 0, sizeof(fence));
    if (fd >= 0)
    {
        n = pg_pread(fd, &fence, sizeof(fence),
                     (off_t) (sxid % SHARD_CLOG_XIDS_PER_SEGMENT) * sizeof(fence));
        if (CloseTransientFile(fd) != 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot close redo fence %s: %m", path)));
    }
    if (!required && (n == 0 || (n == sizeof(fence) && fence.magic == 0 && fence.lsn == 0 && fence.crc == 0)))
        return InvalidXLogRecPtr;
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, &fence.lsn, sizeof(fence.lsn));
    COMP_CRC32C(crc, &fence.magic, sizeof(fence.magic));
    FIN_CRC32C(crc);
    if (n != sizeof(fence) || fence.magic != SHARD_REDO_FENCE_MAGIC || !EQ_CRC32C(crc, fence.crc))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("invalid/missing redo fence for shard %u xid %u", shard, sxid)));
    return fence.lsn;
}

/* Clear the recycled arc, never extending sparse files. Called only while
 * holding the allocator lock, before publishing authorization for new numbers. */
void
ShardClogClearAll(Oid shard)
{
    char dir[MAXPGPATH];
    DIR *d;
    struct dirent *de;
    ShardClogDirPath(dir, sizeof(dir), shard);
    d = AllocateDir(dir);
    if (!d)
    {
        if (errno == ENOENT) return;
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot scan shard clog %s: %m", dir)));
    }
    while ((de = ReadDir(d, dir)) != NULL)
    {
        uint32 segno;
        char extra;
        char path[MAXPGPATH];
        if (strlen(de->d_name) != 8 || sscanf(de->d_name, "%8x%c", &segno, &extra) != 1)
            continue;
        snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
        if (unlink(path) != 0 && errno != ENOENT)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot remove shard clog %s: %m", path)));
    }
    FreeDir(d);
    ShardClogFsyncDir(shard);
}

int
ShardClogClearRange(Oid shard, TransactionId from, TransactionId to, bool retired)
{
    char dir[MAXPGPATH];
    DIR *d;
    struct dirent *de;
    uint32 distance = ShardXidDistance(from, to);
    int removed = 0;
    bool changed = false;
    char zeros[32768] = {0};

    if (!distance)
        return 0;
    if (distance >= SHARD_XID_HALF)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("shard %u clear range is not within half-circle", shard)));
    ShardClogDirPath(dir, sizeof(dir), shard);
    d = AllocateDir(dir);
    if (!d)
    {
        if (errno == ENOENT) return 0;
        ereport(ERROR, (errcode_for_file_access(), errmsg("cannot scan shard clog %s: %m", dir)));
    }
    while ((de = ReadDir(d, dir)) != NULL)
    {
        uint32 segno, first, last, first_offset;
        char tail;
        char path[MAXPGPATH];
        int fd;
        struct stat st;
        uint32 offset;
        bool dirty = false;

        if (strlen(de->d_name) != 8 || sscanf(de->d_name, "%8x%c", &segno, &tail) != 1 ||
            segno > SHARD_XID_MASK / SHARD_CLOG_XIDS_PER_SEGMENT)
            continue;
        first = segno * SHARD_CLOG_XIDS_PER_SEGMENT;
        last = first | (SHARD_CLOG_XIDS_PER_SEGMENT - 1);
        last = Min(last, SHARD_XID_MASK);
        if (!ShardXidInRange(first, from, to) && !ShardXidInRange(last, from, to) &&
            !(from >= first && from <= last))
            continue;
        snprintf(path, sizeof(path), "%s/%s", dir, de->d_name);
        /* Both endpoints can be in a wrapping arc while its middle is live.
         * Require the entire linear file extent to fit before the arc ends.
         * This also handles a short test ring sharing one physical segment. */
        first_offset = ShardXidDistance(from, first);
        if ((first_offset < distance && last - first < distance - first_offset) ||
            (retired && ShardXidInRange(last, from, to) && (to < first || to > last)))
        {
            if (unlink(path) != 0 && errno != ENOENT)
                ereport(ERROR, (errcode_for_file_access(), errmsg("cannot remove shard clog %s: %m", path)));
            removed++; changed = true;
            shard_redo_fence_path(path, shard, segno);
            if (unlink(path) != 0 && errno != ENOENT)
                ereport(ERROR, (errcode_for_file_access(), errmsg("cannot remove retired redo fence %s: %m", path)));
            continue;
        }
        fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
        if (fd < 0 || fstat(fd, &st) != 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot open shard clog %s: %m", path)));
        for (offset = 0; offset <= last - first;)
        {
            uint32 xid = first + offset;
            uint32 n = Min((uint32) (sizeof(zeros) / SHARD_CLOG_SLOT_SIZE), last - xid + 1);
            off_t pos = (off_t) offset * SHARD_CLOG_SLOT_SIZE;
            size_t bytes;

            if (pos >= st.st_size) break;
            /* Split chunks at either end of the arc. */
            if (ShardXidInRange(xid, from, to))
            {
                n = Min(n, ShardXidDistance(xid, to));
                bytes = Min((off_t) n * SHARD_CLOG_SLOT_SIZE, st.st_size - pos);
                if (pg_pwrite(fd, zeros, bytes, pos) != bytes)
                    ereport(ERROR, (errcode_for_file_access(), errmsg("cannot clear shard clog %s: %m", path)));
                dirty = true;
            }
            else if (ShardXidDistance(xid, from) < n)
                n = ShardXidDistance(xid, from);
            offset += n;
        }
        if (dirty && pg_fsync(fd) != 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot fsync shard clog %s: %m", path)));
        if (CloseTransientFile(fd) != 0)
            ereport(ERROR, (errcode_for_file_access(), errmsg("cannot close shard clog %s: %m", path)));
        changed |= dirty;
    }
    FreeDir(d);
    if (changed)
    {
        char redo_dir[MAXPGPATH];
        struct stat st;
        ShardClogFsyncDir(shard);
        snprintf(redo_dir, sizeof(redo_dir), "%s/pg_shard_xid/%u.redo", DataDir, shard);
        if (stat(redo_dir, &st) == 0) fsync_fname(redo_dir, true);
    }
    return removed;
}

int
ShardClogClaimRange(Oid shard, TransactionId from, TransactionId to)
{
    TransactionId sxid;
    int claimed = 0;
    uint32 previous_seg = UINT32_MAX;
    bool dirty = false;
    if (from < FIRST_SHARD_XID) from = FIRST_SHARD_XID;
    if (ShardXidDistance(from, to) >= SHARD_XID_HALF)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("shard %u claim interval exceeds half-circle", shard)));
    for (sxid = from; sxid != to; sxid = ShardXidAdvance(sxid, 1))
    {
        ShardClogSlot slot;
        uint32 segno = sxid / SHARD_CLOG_XIDS_PER_SEGMENT;
        ShardClogReadSlot(shard, sxid, &slot);
        if (slot.status != TXN_RUNNING) continue;
        if (dirty && previous_seg != segno)
        {
            ShardClogSyncSegment(shard, previous_seg);
            dirty = false;
        }
        memset(&slot, 0, sizeof(slot));
        slot.status = TXN_ABORTED;
        ShardClogWriteSlot(shard, sxid, &slot, false);
        dirty = true; previous_seg = segno;
        claimed++;
        CHECK_FOR_INTERRUPTS();
    }
    if (dirty) ShardClogSyncSegment(shard, previous_seg);
    return claimed;
}

/*
 * 0007 redo 钩子：崩溃恢复重放 commit/abort 记录时重做判决。
 * 幂等（重复 redo 写同样字节）；ShardClogSetVerdict 自带 fsync，ERROR 会
 * 中止恢复——与原生 clog 写盘失败同级别，正确的失败方式。
 */
void
ShardClogXactRedo(int nxids, const uint32 *pairs, uint64 commit_ts,
				  bool committed)
{
	int			i;

	for (i = 0; i < nxids; i++)
	{
		if (!ShardXidNativeRedoAllowed((Oid) pairs[2*i], pairs[2*i+1],
									 GetXLogReplayRecPtr(NULL)))
			continue;
		ShardClogSetVerdict((Oid) pairs[2 * i],
							(TransactionId) pairs[2 * i + 1],
							committed, (int64) commit_ts);
		/* T2.5：顺手累计影子推进（水位文件缺失/落后时的发号起点兜底） */
		ShardXidRedoAdvance((Oid) pairs[2 * i],
							(TransactionId) pairs[2 * i + 1]);
	}
}

/* ================= DROP TABLE 提交时点 GC ================= */

/*
 * ★ 2026-10-03：原来是 `static Oid pending_drops[16]`，单事务 DROP 超过 16 张打标表就
 * ERROR「单事务最多 DROP 16 张分片打标表」。这个 16 不是任何格式上的限制 —— 2PC 记录本来
 * 就是变长的（ndrops * sizeof(Oid)）—— 只是进程内数组定长。分片一多就撞：一张 128 分片的
 * 表铺在 4 台 worker 上，Citus 在每台 worker 的**一个**事务里删掉它的 32 个分片。
 * 改成按需增长（TopMemoryContext，事务结束复位；容量保留复用）。
 */
static Oid *pending_drops = NULL;
static int	pending_drops_n = 0;
static int	pending_drops_cap = 0;

void
ShardClogRememberDrop(Oid shard)
{
	int			i;

	for (i = 0; i < pending_drops_n; i++)
		if (pending_drops[i] == shard)
			return;

	if (pending_drops_n >= pending_drops_cap)
	{
		int			newcap = pending_drops_cap > 0 ? pending_drops_cap * 2 : 32;

		pending_drops = pending_drops == NULL
			? (Oid *) MemoryContextAlloc(TopMemoryContext, newcap * sizeof(Oid))
			: (Oid *) repalloc(pending_drops, newcap * sizeof(Oid));
		pending_drops_cap = newcap;
	}
	pending_drops[pending_drops_n++] = shard;
}

bool
ShardClogHasPendingDrops(void)
{
	return pending_drops_n > 0;
}

int
ShardClogPendingDropList(const Oid **oids)
{
	*oids = pending_drops;
	return pending_drops_n;
}

/*
 * 提交时点执行删除。提交已成事实，这里不许 ERROR（会把提交翻成 PANIC 级
 * 混乱）—— 删不掉只 WARNING，残留是无害孤儿（头文件"已知边界"）。
 * 崩在提交记录与本函数之间同理：孤儿目录，T2.7 注册前清目录兜底 OID 复用。
 */
void
ShardClogAtCommit(void)
{
	ShardClogGcDropped(pending_drops, pending_drops_n);
	pending_drops_n = 0;
}

/*
 * T7.31（P7-D3）：回收主体从 ShardClogAtCommit 抽出来，两条提交路径共用 ——
 * 本后端直接提交（上面），与 COMMIT PREPARED 的 2PC 回调（shard_xid.c）。
 * 后者跑在**执行 COMMIT PREPARED 的那个后端**里，与当初 DROP 的后端无关，
 * 所以清单只能从 2PC 记录里拿，不能读本进程的 pending_drops。
 */
void
ShardClogGcDropped(const Oid *oids, int n)
{
	int			i;
	Oid			dropped[SHARD_CLOG_PENDING_DROPS_MAX];

	/*
	 * 调用方可能传的就是 pending_drops 本身（回收过程中会摘共享内存登记、可能触发
	 * 别的路径），所以分批拷进局部缓冲再动手。★ 2026-10-03：原来超过 16 个时截断
	 * （P7-N40 时改成了出声截断）；现在 pending_drops 不再有上限，这里就**分批全收**。
	 */
	if (n > SHARD_CLOG_PENDING_DROPS_MAX)
	{
		int			done;

		for (done = 0; done < n; done += SHARD_CLOG_PENDING_DROPS_MAX)
			ShardClogGcDropped(oids + done, Min(n - done, SHARD_CLOG_PENDING_DROPS_MAX));
		return;
	}
	memcpy(dropped, oids, (size_t) n * sizeof(Oid));

	for (i = 0; i < n; i++)
	{
		char		path[MAXPGPATH];
		struct stat st;

		ShardClogDirPath(path, MAXPGPATH, dropped[i]);
		/* 目录可能从未建过（表没写过判决）——rmtree 会自己打 WARNING，先探 */
		if (stat(path, &st) == 0 && !rmtree(path, true))
			ereport(WARNING,
					(errmsg("pg_partdist: 分片 clog 目录 \"%s\" 删除不完整，"
							"残留为无害孤儿", path)));

		snprintf(path, MAXPGPATH, "%s/pg_shard_xid/%u",
				 DataDir, dropped[i]);
		if (unlink(path) != 0 && errno != ENOENT)
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("pg_partdist: 水位文件 \"%s\" 删除失败: %m", path)));

		/*
		 * R-P6-9：共享内存里的打标登记也要摘掉。此前只清文件不清登记，
		 * 而这个函数上方那句"DROP TABLE 会连同水位/clog 文件一并清理"正是
		 * `partdist_set_shard_mvcc(..., false)` 拒绝撤销时给出的理由 ——
		 * 承诺与实现对不上。留着的后果不是多占一个槽位，而是 **OID 复用后
		 * 误伤无关表**：新表被判成分片打标表，若它是分布式表，DROP 走 2PC
		 * 就撞上 §10 的 PRE_PREPARE 禁令删不掉。实测表现为夹具残表逐轮累积
		 * （122→244→366），症状伪装成"回放写多了"。
		 */
		ShardMvccSetRemove(dropped[i]);

		/*
		 * T7.7（R-P6-4）：分配器槽位也要还。`SHARD_XID_MAX_SLOTS = 64` 是定长
		 * shmem，此前全仓没有任何释放路径 —— 建删 64 张打标表后该节点再也建不出
		 * 第 65 张。门禁一直靠"移走水位文件 + 重启"绕过去，等于把这条缺陷藏在
		 * 净场脚本里。判据与上面那句同源：都在 DROP 的提交时点，回滚不执行。
		 */
        snprintf(path, MAXPGPATH, "%s/pg_shard_xid/%u.redo", DataDir, dropped[i]);
        if (stat(path, &st) == 0 && !rmtree(path, true))
            ereport(WARNING, (errmsg("cannot remove obsolete redo fences %s", path)));
		ShardXidSlotRelease(dropped[i]);
	}
}

void
ShardClogAtAbort(void)
{
	pending_drops_n = 0;
}

/* ================= T5.4：clog 截断（设计 §6.4 顺序铁律）================= */

int
ShardClogTruncate(Oid shard, TransactionId trunc_before)
{
    TransactionId tb, vx;
    if (!TransactionIdIsNormal(trunc_before))
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid shard xid cutoff %u", trunc_before)));
    ShardVacuumGetWatermarks(shard, &tb, &vx);
    if (ShardXidPrecedes(trunc_before, tb) || ShardXidFollows(trunc_before, vx))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("shard %u cutoff %u is outside completed interval %u..%u", shard, trunc_before, tb, vx)));
    if (tb != trunc_before)
        ShardVacuumEmitWatermarkCtrl(shard, trunc_before, vx);
    return ShardXidFinishReclaim(shard, trunc_before, vx);
}

/* ================= 验收/运维用 SQL 包装 =================
 * T2.1 验收与 T2.8 套件直接 CREATE FUNCTION ... '$libdir/pg_partdist' 使用；
 * 正式并入扩展 SQL 文件随 T2.4 的显式函数一批做。
 */

PG_FUNCTION_INFO_V1(partdist_shard_clog_read_full);
Datum
partdist_shard_clog_read_full(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = ShardXidFromSql(PG_GETARG_INT64(1));
	ShardClogSlot slot;
	char		buf[96];

	(void) ShardClogReadSlot(shard, sxid, &slot);
	snprintf(buf, sizeof(buf),
			 "st=%u sts=" UINT64_FORMAT " cts=" UINT64_FORMAT,
			 slot.status, slot.start_ts, slot.commit_ts);
	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

PG_FUNCTION_INFO_V1(partdist_shard_claim);
Datum
partdist_shard_claim(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);

	PG_RETURN_INT32((int32) ShardXidEnsureClaimed(shard));
}

PG_FUNCTION_INFO_V1(partdist_shard_clog_read);
Datum
partdist_shard_clog_read(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = ShardXidFromSql(PG_GETARG_INT64(1));

	PG_RETURN_INT32((int32) ShardClogReadStatus(shard, sxid));
}

PG_FUNCTION_INFO_V1(partdist_shard_clog_write);
Datum
partdist_shard_clog_write(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = ShardXidFromSql(PG_GETARG_INT64(1));
	int32		status = PG_GETARG_INT32(2);

	switch ((TxnStatus) status)
	{
		case TXN_RUNNING:
			ShardClogSetRunning(shard, sxid, 0);
			break;
		case TXN_COMMITTED:
			ShardClogSetVerdict(shard, sxid, true, 0);
			break;
		case TXN_ABORTED:
			ShardClogSetVerdict(shard, sxid, false, 0);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("status 只接受 0(RUNNING)/2(COMMITTED)/3(ABORTED)，"
							"PREPARED 是 P4 的事")));
	}
	PG_RETURN_VOID();
}

/* ================= T5.2 前缀扫描：算 VacuumTargetXid（设计 §6.3）=================
 *
 * 从 clog_truncate_before 起顺扫本分片 clog，返回可安全截断到的**前一条**。
 *
 * 放行（可越过）：
 *   · COMMITTED 且 commit_ts < GlobalSafeTs —— 没有活跃快照还需要它之前的版本；
 *   · **ABORTED 一律放行** —— 这是本规则最反直觉、也最要紧的一条。若 ABORTED
 *     也挡，**一个中止事务就能永久钉死截断，直到回卷死亡**；它留下的垃圾由
 *     §6.4 的页面三类动作清掉，不需要靠挡住前缀来保证。
 *
 * 停止（VacuumTargetXid = 其前一条）：
 *   · RUNNING —— 未决，之后的判决还没写；
 *   · PREPARED —— 2PC 未决，**绝不允许单方推定**；
 *   · COMMITTED 但 commit_ts >= GlobalSafeTs —— 仍可能被活跃快照看到。
 *
 * 可证性质（设计原文备查）：RUNNING 阻挡者之上不存在 commit_ts < GlobalSafeTs
 * 的条目（start_ts 先于首写、首写序即落账序、TSO 单调，三段传递）。故前缀规则
 * 在 RUNNING 阻挡下几乎无损，损失的只是阻挡点之上 ABORTED 事务的垃圾。
 * "死元组清除与前缀截断解耦"记为后续优化，第一期不做。
 *
 * 返回 InvalidTransactionId 表示"一条都不能清"（含 safe_ts 取不到的情形）。
 */
TransactionId
ShardVacuumComputeTarget(Oid shard, TransactionId from, int64 safe_ts,
						 TransactionId ceiling, const char **stop_reason)
{
	TransactionId xid;
	TransactionId last_ok = InvalidTransactionId;

	if (stop_reason != NULL)
		*stop_reason = "scanned-to-ceiling";

	/*
	 * safe_ts <= 0：取不到 GlobalSafeTs（未配置 TSO / RPC 失败）。**什么都不清**
	 * —— 见 TsoGetGlobalSafeTs 的注释，偏小安全、取不到就别动。
	 */
	if (safe_ts <= 0)
	{
		if (stop_reason != NULL)
			*stop_reason = "no-safe-ts";
		return InvalidTransactionId;
	}

	(void) ShardXidEnsureClaimed(shard);
	if (!TransactionIdIsValid(from) || from < FIRST_SHARD_XID)
		from = FIRST_SHARD_XID;

	if (ShardXidDistance(from, ceiling) >= SHARD_XID_HALF)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vacuum scan exceeds half-circle")));
	}
	for (xid = from; xid != ceiling; xid = ShardXidAdvance(xid, 1))
	{
		ShardClogSlot slot;

		if (DtxPendingReferences(shard, xid) || ShardCommitIsActive(shard, xid))
		{
			if (stop_reason != NULL)
				*stop_reason = "prepared-not-closed";
			break;
		}
		if (!ShardClogReadSlot(shard, xid, &slot))
		{
			/*
			 * 读不出槽 = 稀疏空洞 = 全零 = TXN_RUNNING（见 shard_clog.h 头注释）。
			 * 未决即停 —— 空洞不代表"没有这个事务"，只代表判决没写下来。
			 */
			if (stop_reason != NULL)
				*stop_reason = "hole-running";
			break;
		}

		if (slot.status == TXN_ABORTED)
		{
			last_ok = xid;		/* ★ ABORTED 放行（见上方说明） */
			continue;
		}
		if (slot.status == TXN_COMMITTED)
		{
			if ((int64) slot.commit_ts > 0 && (int64) slot.commit_ts < safe_ts)
			{
				last_ok = xid;
				continue;
			}
			if (stop_reason != NULL)
				*stop_reason = "commit-ts-too-new";
			break;
		}
		/* RUNNING / PREPARED */
		if (stop_reason != NULL)
			*stop_reason = (slot.status == TXN_PREPARED) ? "prepared" : "running";
		break;
	}

	return last_ok;
}

/* ---- T5.2 SQL 包装：验收观测点 ---- */
PG_FUNCTION_INFO_V1(partdist_shard_vacuum_target);
Datum
partdist_shard_vacuum_target(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	int64		safe_ts = PG_GETARG_INT64(1);
	TransactionId ceiling = ShardXidFromSql(PG_GETARG_INT64(2));
	TransactionId tb,
				vx,
				target;
	const char *reason = NULL;
	Datum		values[2];
	bool		nulls[2] = {false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	/* 起点 = 当前截断点（设计 §6.3：从 clog_truncate_before 起顺扫） */
	ShardVacuumGetWatermarks(shard, &tb, &vx);
	target = ShardVacuumComputeTarget(shard, tb, safe_ts, ceiling, &reason);

	values[0] = Int64GetDatum((int64) target);
	values[1] = CStringGetTextDatum(reason ? reason : "");
	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/* ---- T5.2 验收辅助：带 commit_ts 的落账（sclog_write 只写 status，ts 恒 0）---- */
PG_FUNCTION_INFO_V1(partdist_shard_clog_write_ts);
Datum
partdist_shard_clog_write_ts(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = ShardXidFromSql(PG_GETARG_INT64(1));
	int32		status = PG_GETARG_INT32(2);
	int64		cts = PG_GETARG_INT64(3);

	switch ((TxnStatus) status)
	{
		case TXN_RUNNING:
			ShardClogSetRunning(shard, sxid, cts);
			break;
		case TXN_PREPARED:
			ShardClogSetPrepared(shard, sxid, cts, 0);
			break;
		case TXN_COMMITTED:
			ShardClogSetVerdict(shard, sxid, true, cts);
			break;
		case TXN_ABORTED:
			ShardClogSetVerdict(shard, sxid, false, cts);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("status 只接受 0/1/2/3，收到 %d", status)));
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(partdist_shard_clog_truncate);
Datum
partdist_shard_clog_truncate(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId trunc_before = ShardXidFromSql(PG_GETARG_INT64(1));

	PG_RETURN_INT32(ShardClogTruncate(shard, trunc_before));
}

PG_FUNCTION_INFO_V1(partdist_shard_claim_on_promote);

/*
 * shard_claim_on_promote(shard oid) → int
 *
 * T6.4 / §6.6 第三支的 SQL 入口。升主路径（pg_raft_promote_prepare）在追平并
 * 闭合 in-doubt 之后调用；验收也用它单独驱动认领。返回改判条数。
 */
Datum
partdist_shard_claim_on_promote(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);

	PG_RETURN_INT32((int32) ShardXidClaimOnPromote(shard));
}

PG_FUNCTION_INFO_V1(partdist_shard_clog_set_prepared);

/*
 * 验收探针：直接把某分片 xid 写成 TXN_PREPARED。
 *
 * 不去放宽 partdist_shard_clog_write() 的 status 白名单 —— 那道"PREPARED 是
 * P4 的事"的守卫是故意的，为了一条测试把它松掉，等于用验收去磨产品的棱角。
 * T6.4 需要 PREPARED 只是为了做**阴性对照**（认领必须不动它，§6.6 第二支），
 * 单开一个探针最小且不影响任何产品路径。
 */
Datum
partdist_shard_clog_set_prepared(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId sxid = ShardXidFromSql(PG_GETARG_INT64(1));

	ShardClogSetPrepared(shard, sxid, 1, 1);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(partdist_shard_xid_raise_watermark);

/*
 * 验收探针：直接抬高本分片的发号水位（等价于回放消费一条更高水位的 MARKER）。
 *
 * 为什么用探针而不是"真烧一整批号"：水位按 SHARD_XID_BATCH(4096) 成批推进，
 * 挂槽那一刻 claim_wm 与 watermark 相等，缺口要等 leader 烧掉**整整 4096 笔
 * 事务**才张开。实测在本环境跑 4200 笔自动提交事务要 31 秒（还只是普通表，
 * 分片表叠上 raft 复制到分钟级），而这段代价换不来任何新覆盖 ——
 * "真 MARKER 路径确实会抬水位"已由 T6.5 的 test_xid_watermark_p6.sh 证过
 * （17/0，含"水位确实在动"的正向断言）。本探针只把同一份状态确定性地摆好，
 * 让 T6.4 能专心验它自己的那件事：给定缺口，认领改判谁、不动谁。
 */
Datum
partdist_shard_xid_raise_watermark(PG_FUNCTION_ARGS)
{
	Oid			shard = PG_GETARG_OID(0);
	TransactionId wm = ShardXidFromSql(PG_GETARG_INT64(1));

	ShardXidRaiseAllocWatermark(shard, wm);
	PG_RETURN_VOID();
}
