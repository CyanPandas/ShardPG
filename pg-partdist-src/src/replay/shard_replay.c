/*
 * shard_replay.c
 *
 * Follower 物理回放核心（FRD §7，R1：纯物理闭环）。
 *
 * 五阶段（对应 FRD §7.2–§7.8，R1 无 xid_map/MARKER）：
 *   1. 初始化：apply_checkpoint → 游标；locmap 文件 → loc_map；InRecovery=true
 *   2. 逐条读取与校验：magic / partition_id / 连续性（plsn 空洞 = ERROR）
 *   3. 应用 DATA：decode → remap → 盖 orig_lsn → rm_redo
 *      （RM_SMGR 走无 XLogFlush 的克隆路径；RM_XACT/白名单跳过）
 *   4. 游标推进与 apply checkpoint（刷脏 → smgrimmedsync → 原子写游标）
 *   5. 升主收尾属 R4，本文件不涉及
 *
 * 读取策略：每轮 catch-up 先扫全部段文件建 (plsn → 文件,偏移) 索引再按
 * plsn 序应用 —— 段文件按 orig_lsn 分段，并发捕获下 plsn 与文件序可能
 * 有局部倒置，顺序读文件不保证 plsn 有序。checkpoint 的 resume_segno
 * 只是扫描起点提示（FRD §8.4）。
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "shard_replay.h"
#include "shard_fileset.h"
#include "partition_wal.h"
#include "partition_wal_header.h"
#include "partition_wal_writer.h"
#include "enhanced_clog.h"
#include "shard_clog.h"	/* U-P5-1：分片 clog 由流重建 */
#include "shard_xid.h"

#include "utils/pg_crc.h"        /* R-P4-20：redo 前校验记录 CRC */
#include "access/clog.h"            /* ExtendCLOG（§13 约束 4） */
#include "access/commit_ts.h"       /* ExtendCommitTs */
#include "access/heapam_xlog.h"
#include "access/nbtxlog.h"
#include "access/subtrans.h"        /* ExtendSUBTRANS */
#include "access/transam.h"
#include "access/rmgr.h"
#include "access/visibilitymap.h"
#include "access/xact.h"            /* XLOG_XACT_COMMIT / XLOG_XACT_OPMASK */
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "catalog/pg_control.h"     /* XLOG_FPI / XLOG_FPI_FOR_HINT */
#include "catalog/storage_xlog.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/freespace.h"
#include "storage/smgr.h"
#include "utils/memutils.h"

/* ================================================================== */
/* loc_map 装载                                                        */
/* ================================================================== */

bool
ShardReplayReadLocMap(Oid shard_oid, ReplayLocMapFile *lm, const char **reason)
{
    char    path[MAXPGPATH];
    int     fd;
    ssize_t nb;

    if (reason != NULL)
        *reason = NULL;

    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, shard_oid, REPLAY_LOCMAP_FILENAME);

    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
    {
        if (reason != NULL)
            *reason = "locmap 文件不存在（未调用 replay_set_locmap）";
        return false;
    }

    nb = read(fd, lm, sizeof(*lm));
    CloseTransientFile(fd);

    /*
     * 长度先判：v1 的 locmap 是 780 字节、v2 是 912，短读本身就是版本判据。
     * 长度对不上时连 magic 都不该信 —— 那几个字节可能落在 v1 的别的字段上。
     */
    if (nb != (ssize_t) sizeof(*lm))
    {
        if (reason != NULL)
            *reason = "locmap 长度不符（v1 旧格式或文件损坏）——"
                      "请重跑 replay_set_locmap() 重建";
        return false;
    }
    if (lm->magic != REPLAY_LOCMAP_MAGIC)
    {
        if (reason != NULL)
            *reason = "locmap magic 不符";
        return false;
    }
    if (lm->version != REPLAY_LOCMAP_VERSION)
    {
        if (reason != NULL)
            *reason = "locmap 版本不符（v1 无 role/ord，v2 无 base_part_lsn，"
                      "都无法就地升级）—— 请重跑 replay_set_locmap() 重建。"
                      "★ 旧版配对没有起效游标，等于沉默假设"
                      "「本地文件 == leader 在流起点时的文件」，"
                      "本版拒绝按这个假设认领（T6.2）";
        return false;
    }
    if (lm->shard_oid != shard_oid ||
        lm->npairs < 1 || lm->npairs > SHARD_FILESET_MAX_RELS)
    {
        if (reason != NULL)
            *reason = "locmap 内容自相矛盾（shard_oid 或 npairs 越界）";
        return false;
    }

    return true;
}

void
ShardReplayWriteLocMap(const ReplayLocMapFile *lm)
{
    char    path[MAXPGPATH];
    char    tmp[MAXPGPATH];
    int     fd;
    ssize_t nb;

    InitPartitionWALDirectory(lm->shard_oid);
    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, lm->shard_oid,
             REPLAY_LOCMAP_FILENAME);
    snprintf(tmp, MAXPGPATH, "%s.tmp", path);

    fd = OpenTransientFile(tmp, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY);
    if (fd < 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法创建 locmap 临时文件 \"%s\": %m",
                        tmp)));

    do {
        nb = write(fd, lm, sizeof(*lm));
    } while (nb < 0 && errno == EINTR);

    if (nb != (ssize_t) sizeof(*lm) || pg_fsync(fd) != 0)
    {
        CloseTransientFile(fd);
        (void) unlink(tmp);
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: locmap 写入 \"%s\" 失败: %m", tmp)));
    }
    CloseTransientFile(fd);

    if (rename(tmp, path) != 0)
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("pg_partdist: 无法就位 locmap \"%s\": %m", path)));
}

bool
ShardReplayLoadLocMap(ShardReplayCtx *ctx)
{
    ReplayLocMapFile lm;
    HASHCTL          hctl;
    int              i;

    if (!ShardReplayReadLocMap(ctx->shard_oid, &lm, NULL))
        return false;

    memset(&hctl, 0, sizeof(hctl));
    hctl.keysize   = sizeof(RelFileLocator);
    hctl.entrysize = sizeof(LocMapEntry);
    hctl.hcxt      = TopMemoryContext;

    ctx->loc_map = hash_create("shard replay loc_map",
                               SHARD_FILESET_MAX_RELS,
                               &hctl,
                               HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    ctx->nlocal = 0;
    ctx->base_part_lsn = lm.base_part_lsn;   /* T6.2：本配对的起效游标 */
    for (i = 0; i < lm.npairs; i++)
    {
        LocMapEntry *e;
        bool         found;
        int          j;

        e = (LocMapEntry *)
            hash_search(ctx->loc_map, &lm.pairs[i].leader_loc,
                        HASH_ENTER, &found);
        e->local_loc = lm.pairs[i].local_loc;
        e->role      = lm.pairs[i].role;
        e->ord       = lm.pairs[i].ord;
        e->reserved  = 0;

        /* 收集去重后的本地文件号（checkpoint 刷脏用） */
        for (j = 0; j < ctx->nlocal; j++)
            if (RelFileLocatorEquals(ctx->local_locs[j], lm.pairs[i].local_loc))
                break;
        if (j == ctx->nlocal && ctx->nlocal < SHARD_FILESET_MAX_RELS)
            ctx->local_locs[ctx->nlocal++] = lm.pairs[i].local_loc;
    }

    /*
     * ★ R-P4-8（2026-08-15 修）：本地文件必须**实际存在**才认领。
     *
     * locmap 有效不等于目标还在：表被 DROP 后 locmap 文件仍躺在
     * pg_parwal/<shard>/ 里（清理只删表，不删回放侧的配对），回放器照旧
     * 认领、从游标 0 起重放，读到已删/被截断的页面直接
     * `PANIC: invalid max offset number` —— PANIC 不可捕获，回放本体的
     * PG_TRY 拦不住，于是**整节点重置 → 再选举 → 再认领 → 再 PANIC**，
     * 反复自噬（实测 worker3/4 单轮各 16–18 次，term 21→23）。
     *
     * 判据取"主堆（role=0）的本地文件存在"：主堆没了就是表没了，索引/TOAST
     * 的缺失由后续 redo 自行处置（它们本就允许延迟建立）。用 smgrexists
     * 而非 catalog 查询 —— 回放 worker 无 DB 连接，摸 catalog 会 SIGSEGV
     * （R-P4-6 的教训）。
     */
    for (i = 0; i < lm.npairs; i++)
    {
        SMgrRelation smgr;

        if (lm.pairs[i].role != SHARD_REL_MAIN)
            continue;

        smgr = smgropen(lm.pairs[i].local_loc, InvalidBackendId);
        if (!smgrexists(smgr, MAIN_FORKNUM))
        {
            ereport(WARNING,
                    (errmsg("pg_partdist replay: shard %u 的本地主堆文件已不存在"
                            "（relNumber=%u），拒绝认领",
                            ctx->shard_oid,
                            (unsigned) lm.pairs[i].local_loc.relNumber),
                     errdetail("表多半已被 DROP 而 locmap 残留；继续回放会读到"
                               "已删页面并触发不可捕获的 PANIC（R-P4-8）。"),
                     errhint("清理该 shard 的 pg_parwal 目录，或重跑 "
                             "replay_set_locmap() 重新配对。")));
            hash_destroy(ctx->loc_map);
            ctx->loc_map = NULL;
            ctx->nlocal = 0;
            return false;       /* 调用方按"无有效 locmap"解除 armed */
        }
    }

    return true;
}

/* ================================================================== */
/* skip 白名单（FRD 附录 A）                                           */
/* ================================================================== */

/*
 * 返回 true = 该记录对物理副本无意义，跳过。
 * 未知 rmid/info 组合一律 ERROR —— 宁可 fail-fast，不静默漏回放。
 * （worker 内 ERROR → 进程退出重启，同一条记录反复报错，日志内高度可见；
 * 语义上等价 FRD 的 PANIC fail-fast，但不拖垮整个节点。）
 */
static bool
ShardReplaySkippable(uint8 rmid, uint8 info)
{
    uint8 op;

    switch (rmid)
    {
        case RM_HEAP_ID:
            op = info & XLOG_HEAP_OPMASK;
            if (op == XLOG_HEAP_TRUNCATE)
                return true;    /* 仅服务逻辑解码，heap_redo 中即 no-op */
            return false;

        case RM_HEAP2_ID:
            op = info & XLOG_HEAP_OPMASK;
            if (op == XLOG_HEAP2_NEW_CID || op == XLOG_HEAP2_REWRITE)
                return true;
            return false;

        case RM_BTREE_ID:
            op = info & ~XLR_INFO_MASK;
            if (op == XLOG_BTREE_REUSE_PAGE)
                return true;    /* 仅 standby 冲突用，不改页面 */
            return false;

        case RM_XLOG_ID:
            op = info & ~XLR_INFO_MASK;
            if (op == XLOG_FPI || op == XLOG_FPI_FOR_HINT)
                return false;
            return true;        /* checkpoint 等不落在 shard 文件上 */

        case RM_XACT_ID:
            return true;        /* v2 段流：原始 XACT 记录跳过（附录 A） */

        case RM_SMGR_ID:
            return false;       /* 专用路径处理 */

        case RM_HASH_ID:
        case RM_GIN_ID:
        case RM_GIST_ID:
        case RM_SPGIST_ID:
        case RM_BRIN_ID:
            /* R1 只承诺 heap + btree（FRD §13.7）*/
            ereport(ERROR,
                    (errmsg("shard replay: R1 不支持的索引 AM 记录 "
                            "rmid=%u info=0x%02X", rmid, info)));
            return false;

        default:
            ereport(ERROR,
                    (errmsg("shard replay: 未知 rmid=%u info=0x%02X，"
                            "拒绝静默跳过", rmid, info)));
            return false;
    }
}

/* ================================================================== */
/* RM_SMGR 应用（克隆 smgr_redo，去掉 XLogFlush，locator 走 loc_map）  */
/* ================================================================== */

static RelFileLocator
ReplayRemapLocator(ShardReplayCtx *ctx, const RelFileLocator *leader_loc,
                   uint64 plsn)
{
    LocMapEntry *e;
    bool         found;

    e = (LocMapEntry *) hash_search(ctx->loc_map, leader_loc,
                                    HASH_FIND, &found);
    if (!found)
        ereport(ERROR,
                (errmsg("shard replay: shard %u 未知 relfilelocator %u/%u/%u "
                        "@plsn %llu（fileset 漏登记：索引或 TOAST？）",
                        ctx->shard_oid,
                        leader_loc->spcOid, leader_loc->dbOid,
                        leader_loc->relNumber,
                        (unsigned long long) plsn)));
    return e->local_loc;
}

/*
 * ApplySmgrRecord — smgr_redo 的回放版。
 *
 * 与内核 smgr_redo 的差异（其余逐行同构，基于 16.14 storage.c）：
 *   1. locator 经 loc_map 改写为本地文件号；
 *   2. **不调用 XLogFlush(lsn)** —— lsn 是 leader 坐标，本地 pg_wal 没有
 *      这个位置；该页的保护日志是 parwal/Raft log，apply 前已持久化
 *      （FRD §8.3 同一论证，truncate 是"不可回退"操作也不例外：记录
 *      已 committed 落盘，崩溃后从游标重放会再次走到这里）。
 */
static void
ApplySmgrRecord(ShardReplayCtx *ctx, const PartWALRecord *hdr,
                const char *body)
{
    uint8          op = hdr->info & XLR_RMGR_INFO_MASK;
    RelFileLocator leader_loc;
    RelFileLocator local_loc;

    if (!SmgrRecordGetLocator(body, hdr->data_len, hdr->info, &leader_loc))
        ereport(ERROR,
                (errmsg("shard replay: shard %u 无法解析 SMGR 记录 @plsn %llu",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn)));

    local_loc = ReplayRemapLocator(ctx, &leader_loc, hdr->partition_lsn);

    if (op == XLOG_SMGR_CREATE)
    {
        xl_smgr_create xlrec;
        SMgrRelation   reln;

        /* main data 布局已由 SmgrRecordGetLocator 校验；此处取完整 xlrec */
        {
            const char *p = body + SizeOfXLogRecord;
            uint8       block_id = (uint8) *p++;

            if (block_id == XLR_BLOCK_ID_DATA_SHORT)
                p += 1;
            else
                p += 4;
            memcpy(&xlrec, p, sizeof(xlrec));
        }

        reln = smgropen(local_loc, InvalidBackendId);
        smgrcreate(reln, xlrec.forkNum, true);
    }
    else if (op == XLOG_SMGR_TRUNCATE)
    {
        xl_smgr_truncate xlrec;
        SMgrRelation     reln;
        Relation         rel;
        ForkNumber       forks[MAX_FORKNUM];
        BlockNumber      blocks[MAX_FORKNUM];
        BlockNumber      old_blocks[MAX_FORKNUM];
        int              nforks = 0;
        bool             need_fsm_vacuum = false;

        {
            const char *p = body + SizeOfXLogRecord;
            uint8       block_id = (uint8) *p++;

            if (block_id == XLR_BLOCK_ID_DATA_SHORT)
                p += 1;
            else
                p += 4;
            memcpy(&xlrec, p, sizeof(xlrec));
        }

        reln = smgropen(local_loc, InvalidBackendId);
        smgrcreate(reln, MAIN_FORKNUM, true);

        /* 内核在此处 XLogFlush(lsn)；回放路径刻意省略（见函数头注释） */

        if ((xlrec.flags & SMGR_TRUNCATE_HEAP) != 0)
        {
            forks[nforks]      = MAIN_FORKNUM;
            old_blocks[nforks] = smgrnblocks(reln, MAIN_FORKNUM);
            blocks[nforks]     = xlrec.blkno;
            nforks++;

            /* invalid_page_tab 记的是**本地**改写后的文件号 */
            XLogTruncateRelation(local_loc, MAIN_FORKNUM, xlrec.blkno);
        }

        rel = CreateFakeRelcacheEntry(local_loc);

        if ((xlrec.flags & SMGR_TRUNCATE_FSM) != 0 &&
            smgrexists(reln, FSM_FORKNUM))
        {
            blocks[nforks] = FreeSpaceMapPrepareTruncateRel(rel, xlrec.blkno);
            if (BlockNumberIsValid(blocks[nforks]))
            {
                forks[nforks]      = FSM_FORKNUM;
                old_blocks[nforks] = smgrnblocks(reln, FSM_FORKNUM);
                nforks++;
                need_fsm_vacuum = true;
            }
        }
        if ((xlrec.flags & SMGR_TRUNCATE_VM) != 0 &&
            smgrexists(reln, VISIBILITYMAP_FORKNUM))
        {
            blocks[nforks] = visibilitymap_prepare_truncate(rel, xlrec.blkno);
            if (BlockNumberIsValid(blocks[nforks]))
            {
                forks[nforks]      = VISIBILITYMAP_FORKNUM;
                old_blocks[nforks] = smgrnblocks(reln, VISIBILITYMAP_FORKNUM);
                nforks++;
            }
        }

        if (nforks > 0)
        {
            START_CRIT_SECTION();
            smgrtruncate2(reln, forks, nforks, old_blocks, blocks);
            END_CRIT_SECTION();
        }

        if (need_fsm_vacuum)
            FreeSpaceMapVacuumRange(rel, xlrec.blkno, InvalidBlockNumber);

        FreeFakeRelcacheEntry(rel);
    }
    else
        ereport(ERROR,
                (errmsg("shard replay: 未知 SMGR op 0x%02X @plsn %llu",
                        op, (unsigned long long) hdr->partition_lsn)));
}

/* ================================================================== */
/* DATA 记录应用（FRD §7.4）                                           */
/* ================================================================== */

/* ================================================================== */
/* T6.3a：offnum 前置检查 —— 把不可捕获的 PANIC 降级为该 shard 停摆      */
/* ================================================================== */

/*
 * 内核在三处以 `elog(PANIC, "invalid max offset number")` 收口
 * （heapam.c 的 heap_xlog_insert / heap_xlog_multi_insert / heap_xlog_update），
 * 判据都是同一条：
 *
 *     if (PageGetMaxOffsetNumber(page) + 1 < offnum) elog(PANIC, ...);
 *
 * 即**页存在、块号在界内、但页太短**（缺目标 offnum 之前的行指针）。
 *
 * ★ 现有的三道守卫够不着这一格：R-P4-8 的两道查的是"关系是否存在"
 *   （`smgrexists`），块号边界那道查的是"块号是否在界内"。加上块号那道之后的
 *   10 轮验证里，它的 WARNING **一次都没触发**，而 PANIC 照常复发 —— 那条
 *   注释就是这么写的。
 *
 * ★ PANIC 不可捕获：回放本体的 PG_TRY 拦不住，postmaster 会**整节点重置**，
 *   随后再选举、再认领、再 PANIC，反复自噬（实测单轮 16–18 次）。所以这道
 *   检查的价值不在"修好了什么"，而在**把节点级灾难降格成单个 shard 停摆**：
 *   ERROR 走外层 PG_TRY，槽位置 REPLAY_FAILED，别的 shard 照常。
 *
 * ★ 它取代了此前那个只覆盖"带 INIT_PAGE 的 INSERT"的特例陷阱 —— 那是本检查
 *   的一个子集（INIT_PAGE ⇒ 页被 PageInit 清空 ⇒ 有效 maxoff = 0）。
 *
 * ★ 诊断随检查一起打，而不是挂在"游标从 0 起"上。R-P4-20 的取证点当初写成
 *   `if (ctx->applied_part_lsn == 0)`，而实测 30 次 PANIC 里**最后 7 次的游标
 *   是 10 和 11** —— 那个精心埋的取证点一次都没为它们打过，这正是几何形状
 *   两周没被记下来的原因。挂在跳闸点上，就永远打得到。
 */

/*
 * 取该块在 redo 之前的有效 max offset 与页面 LSN；
 * 返回 false = 无需检查（带整页镜像 / 块不存在 / 越界）。
 */
static bool
shard_replay_page_maxoff(const DecodedBkpBlock *blk, OffsetNumber *maxoff_out,
						 XLogRecPtr *pagelsn_out)
{
	SMgrRelation smgr = smgropen(blk->rlocator, InvalidBackendId);
	BlockNumber  nblocks;
	Buffer       buf;
	Page         page;

	/* 带整页镜像：内核走 RestoreBlockImage，压根不做 offnum 判据 */
	if (blk->has_image)
		return false;

	/* WILL_INIT / INIT_PAGE：页会被 PageInit 清空 ⇒ 有效 maxoff = 0 */
	if ((blk->flags & BKPBLOCK_WILL_INIT) != 0)
	{
		*maxoff_out  = 0;
		*pagelsn_out = InvalidXLogRecPtr;	/* 页会被清空，LSN 闸门不适用 */
		return true;
	}

	if (!smgrexists(smgr, blk->forknum))
		return false;
	nblocks = smgrnblocks(smgr, blk->forknum);
	if (blk->blkno >= nblocks)
		return false;			/* 越界那道守卫已经处置过 */

	/*
	 * ★ 必须走缓冲区而不是 smgrread：页面很可能正脏在共享缓冲区里、盘上那份
	 * 还是旧的（更短）。从盘上读会**误报**。这次查找随后被 redo 自己命中，
	 * 代价只是一次哈希命中加一把共享锁。
	 */
	buf = ReadBufferWithoutRelcache(blk->rlocator, blk->forknum, blk->blkno,
									RBM_NORMAL, NULL, true);
	if (!BufferIsValid(buf))
		return false;

	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);
	*maxoff_out  = PageIsNew(page) ? 0 : PageGetMaxOffsetNumber(page);
	*pagelsn_out = PageIsNew(page) ? InvalidXLogRecPtr : PageGetLSN(page);
	UnlockReleaseBuffer(buf);
	return true;
}

/*
 * 返回 true = 可以派发 redo；false = 已经拦下（调用方跳过本条记录）。
 */
static bool
ShardReplayOffnumPrecheck(ShardReplayCtx *ctx, XLogReaderState *reader,
						  const XLogRecord *rec, const PartWALRecord *hdr)
{
	DecodedXLogRecord *dec = reader->record;
	uint8         info = rec->xl_info & ~XLR_INFO_MASK;
	uint8         op   = info & XLOG_HEAP_OPMASK;
	OffsetNumber  want = InvalidOffsetNumber;   /* 记录想落的最大 offnum */
	int           blkid = 0;
	const char   *what = NULL;
	OffsetNumber  maxoff;
	XLogRecPtr    pagelsn = InvalidXLogRecPtr;

	if (rec->xl_rmid == RM_HEAP_ID && op == XLOG_HEAP_INSERT)
	{
		want  = ((xl_heap_insert *) XLogRecGetData(reader))->offnum;
		what  = "INSERT";
	}
	else if (rec->xl_rmid == RM_HEAP_ID &&
			 (op == XLOG_HEAP_UPDATE || op == XLOG_HEAP_HOT_UPDATE))
	{
		/* 新元组恒在 block 0（heap_xlog_update 的 newaction 分支） */
		want  = ((xl_heap_update *) XLogRecGetData(reader))->new_offnum;
		what  = "UPDATE(new)";
	}
	else if (rec->xl_rmid == RM_HEAP2_ID && op == XLOG_HEAP2_MULTI_INSERT)
	{
		xl_heap_multi_insert *m =
			(xl_heap_multi_insert *) XLogRecGetData(reader);
		bool isinit = ((info & XLOG_HEAP_INIT_PAGE) != 0);
		int  i;

		/*
		 * ★★ 取**第一个**偏移，不是最大的那个。
		 *
		 * 内核的判据在**循环内**，而每次 PageAddItem 都会把 maxoff 加一：
		 *     for (i = 0; i < ntuples; i++) {
		 *         offnum = isinit ? FirstOffsetNumber + i : offsets[i];
		 *         if (PageGetMaxOffsetNumber(page) + 1 < offnum) PANIC;
		 *         PageAddItem(...);            ← maxoff 随之增长
		 *     }
		 * 所以约束是"**第一个**装得下"，后面的随页面一起长。
		 *
		 * 首版取最大值与初始 maxoff 比，注释还写着"最大者过了其余都过" ——
		 * 推理正好反了。实测代价：一条 10 元组的 MULTI_INSERT（offsets
		 * 121..130、页内 maxoff=120）被误判，而它在内核里完全合法。更糟的是
		 * 它一被拦下，后续引用 121+ 的 UPDATE 就真的找不到行指针，于是级联出
		 * 两百多条"误报"——**整串假象都源自这一个 off-by-max**。
		 */
		want = isinit ? (OffsetNumber) FirstOffsetNumber : m->offsets[0];
		i = 0;					/* 上面已取首个偏移，循环留给将来扩展 */
		(void) i;
		what = "MULTI_INSERT";
	}
	else
		return true;			/* 其余记录类型不走这三处判据 */

	if (dec == NULL || blkid > dec->max_block_id ||
		!dec->blocks[blkid].in_use)
		return true;

	if (!shard_replay_page_maxoff(&dec->blocks[blkid], &maxoff, &pagelsn))
		return true;			/* 无需检查 */

	/*
	 * ★★ 内核自己的那道闸门必须照抄，否则会**大面积误报**。
	 *
	 * XLogReadBufferForRedoExtended 的非 FPI 分支先判
	 *     if (lsn <= PageGetLSN(page)) return BLK_DONE;
	 * 页面已经到达或越过这条记录时 redo 直接跳过，**压根到不了 offnum 判据**。
	 * 而"越过"完全可能伴随 maxoff 变小 —— 后来的 prune/vacuum 把行指针数组
	 * 截短了，页面 LSN 却更新。此时页短是**正常状态**，不是分歧。
	 *
	 * 首版漏了这一条，在 pagecmp_p1 里一轮打出 **89 条误报**（清一色
	 * HOT_UPDATE），跳过合法记录之后页面残缺，下一条记录撞上另一处判据
	 * `elog(PANIC, "invalid lp")` —— **遏制层自己制造了它要防的那类崩溃**。
	 * 判据必须与内核逐字一致，包括它的前置分支。
	 */
	if (!XLogRecPtrIsInvalid(pagelsn) && hdr->orig_lsn <= pagelsn)
		return true;			/* redo 会 BLK_DONE 跳过，不会触发判据 */

	if ((uint32) maxoff + 1 >= (uint32) want)
		return true;			/* 正常：够得着 */

	/*
	 * 跳闸。这里把**全部几何形状**一次打全 —— 事后回溯只有这一次机会
	 * （夹具下一轮就清场，日志又会被轮转）。
	 */
	/*
	 * ★ 动作是**停摆**而不是跳过。
	 *
	 * 首版写成"跳过本条、游标照推"，这是错的：数据记录一旦被跳过，页面就此
	 * 残缺，下一条依赖它的记录立刻撞上另一处判据 elog(PANIC, "invalid lp")
	 * —— **遏制层自己制造了它要防的那类崩溃**（实测一轮 89 次跳过之后，
	 * worker2/worker3 各崩两次）。
	 *
	 * ERROR 由外层 PG_TRY 接住 ⇒ 槽位置 REPLAY_FAILED、**游标不推进**，
	 * 运维据 errhint 重做物理基线后可从原位继续，别的 shard 不受影响。
	 * 这才是任务卡上"该 shard 停摆"的意思。
	 */
	/*
	 * ★ 取证必须先以 WARNING 单独发一条，不能只挂在 ERROR 的 errdetail 上。
	 *
	 * 这条 ERROR 会被外层 PG_TRY 接住，而 PG_CATCH 只取 ed->message 重发一条
	 * WARNING —— **errdetail / errhint 连同整个几何形状一起被吞掉**，日志里
	 * 只剩一句"追平失败: 页太短"。实测就是这样：套件里"几何形状完整"与
	 * "提示指向 shard_baseline_emit"两条断言取不到值。
	 *
	 * 而"看得清"正是这道遏制层的四个目标之一（造得出/拦得住/看得清/修得好），
	 * R-P4-20 拖了两周没定案，缺的就是现场。所以先记全，再停摆。
	 */
	ereport(WARNING,
			(errmsg("pg_partdist replay [R-P4-20]: 页太短，停止该分片回放"
					"（shard %u plsn=%llu %s offnum=%u 但页内 maxoff=%u）",
					ctx->shard_oid,
					(unsigned long long) hdr->partition_lsn,
					what, (unsigned) want, (unsigned) maxoff),
			 errdetail("几何：rmid=%u info=0x%02X blk=%u/%u relNumber=%u "
					   "fork=%d image=%d will_init=%d 游标=%llu 基线=%llu "
					   "记录LSN=%X/%X 页LSN=%X/%X。"
					   "内核会在此处 elog(PANIC, \"invalid max offset number\") "
					   "——不可捕获，后果是整节点重置并反复自噬（R-P4-20）。",
					   (unsigned) rec->xl_rmid, (unsigned) info,
					   (unsigned) blkid, (unsigned) dec->blocks[blkid].blkno,
					   (unsigned) dec->blocks[blkid].rlocator.relNumber,
					   (int) dec->blocks[blkid].forknum,
					   dec->blocks[blkid].has_image ? 1 : 0,
					   (dec->blocks[blkid].flags & BKPBLOCK_WILL_INIT) ? 1 : 0,
					   (unsigned long long) ctx->applied_part_lsn,
					   (unsigned long long) ctx->base_part_lsn,
					   LSN_FORMAT_ARGS(hdr->orig_lsn),
					   LSN_FORMAT_ARGS(pagelsn)),
			 errhint("本副本与该流不同代。在 leader 上调 "
					 "partdist.shard_baseline_emit() 重做物理基线，"
					 "并以返回值作为 base_part_lsn 重跑 replay_set_locmap()。")));

	/* 现场已落盘，现在停摆：ERROR 走外层 PG_TRY ⇒ FAILED + 游标不推进 */
	ereport(ERROR,
			(errmsg("pg_partdist replay [R-P4-20]: 页太短，停止该分片回放"
					"（shard %u plsn=%llu %s offnum=%u 但页内 maxoff=%u）",
					ctx->shard_oid,
					(unsigned long long) hdr->partition_lsn,
					what, (unsigned) want, (unsigned) maxoff)));
	return false;				/* 到不了这里：上面是 ERROR */
}

static void
ApplyDataRecord(ShardReplayCtx *ctx, const PartWALRecord *hdr, char *body)
{
    XLogRecord        *record = (XLogRecord *) body;
    DecodedXLogRecord *decoded;
    char              *errormsg = NULL;
    int                id;

    if (hdr->data_len < SizeOfXLogRecord ||
        record->xl_tot_len != hdr->data_len)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu 记录体长度不符 "
                        "(data_len=%u, xl_tot_len=%u)",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->data_len, record->xl_tot_len)));

    /*
     * 登记事务号（§7.5/§9.2）。放在 skip 判断**之前**：被跳过的记录不改页面，
     * 但它的 xid 一样是 leader 已经分配掉的 —— 水位漏掉它，本地就可能重新
     * 分配到同一个号，升主后 "xid <= W" 的判定随即错位。
     *
     * 只对 version >= 3 的流有意义：2.0 记录的 gxid 是补 0 节点号的兼容值，
     * 登进 xid_map 会把"协调节点的事务"和"未知来源"混为一谈（§4.4）。
     */
    if (hdr->version >= PARTWAL_RECORD_VERSION_3)
        ShardReplayNoteXid(ctx, PartWALRecordGxid(hdr));

    if (ShardReplaySkippable(record->xl_rmid, record->xl_info))
        return;

    if (record->xl_rmid == RM_SMGR_ID)
    {
        ApplySmgrRecord(ctx, hdr, body);
        return;
    }

    REPLAY_TRACE("TRACE data: decode plsn=%llu tot_len=%u",
                 (unsigned long long) hdr->partition_lsn,
                 record->xl_tot_len);

    /* 1) 解码原始字节；lsn 实参 = leader 的 end LSN（§4.2/§8.2） */
    decoded = palloc(DecodeXLogRecordRequiredSpace(record->xl_tot_len));
    if (!DecodeXLogRecord(ctx->reader, decoded, record, hdr->orig_lsn,
                          &errormsg))
        ereport(ERROR,
                (errmsg("shard replay: shard %u 解码失败 @plsn %llu: %s",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        errormsg ? errormsg : "(no message)")));

    /* 2) 文件号重映射：leader → 本地（唯一改写点） */
    for (id = 0; id <= decoded->max_block_id; id++)
    {
        DecodedBkpBlock *blk = &decoded->blocks[id];

        if (!blk->in_use)
            continue;
        blk->rlocator = ReplayRemapLocator(ctx, &blk->rlocator,
                                           hdr->partition_lsn);
    }

    /*
     * 3) 挂上 reader：EndRecPtr = orig_lsn ⇒ redo 内 PageSetLSN 盖 leader
     *    的 end LSN，页面（含 LSN 域）与 leader 字节级一致；重放已应用记录
     *    时 lsn <= PageGetLSN → BLK_DONE，页级幂等（§8.2）。
     */
    ctx->reader->record     = decoded;
    ctx->reader->ReadRecPtr = hdr->orig_lsn;
    ctx->reader->EndRecPtr  = hdr->orig_lsn;

    REPLAY_TRACE("TRACE data: rm_redo plsn=%llu rmid=%u",
                 (unsigned long long) hdr->partition_lsn, record->xl_rmid);

    /*
     * ★ R-P4-8 运行时守卫（2026-08-15）：redo 之前确认目标文件还在。
     *
     * 认领时的 locmap 校验只查一次；**表可以在认领之后被 DROP**（验收清理
     * 就这么干），此后每条记录都会 redo 到已删文件上 →
     * `PANIC: invalid max offset number` → 不可捕获 → 整节点重置 → 再选举
     * → 再认领 → 再 PANIC，反复自噬。故必须逐条守。
     *
     * 目标不存在时**跳过该条**（返回让调用方推进游标）：数据都没了，重放
     * 无意义；继续往下反而是唯一会炸的路。smgrexists 走本地文件系统，
     * 无 catalog 依赖（回放 worker 无 DB 连接，R-P4-6 的教训）。
     */
    for (id = 0; id <= decoded->max_block_id; id++)
    {
        DecodedBkpBlock *blk = &decoded->blocks[id];
        SMgrRelation     smgr;

        if (!blk->in_use)
            continue;
        smgr = smgropen(blk->rlocator, InvalidBackendId);
        if (!smgrexists(smgr, blk->forknum))
        {
            ereport(WARNING,
                    (errmsg("pg_partdist replay: 目标文件已不存在，跳过该记录"
                            "（shard %u，plsn=%llu，relNumber=%u fork=%d）",
                            ctx->shard_oid,
                            (unsigned long long) hdr->partition_lsn,
                            (unsigned) blk->rlocator.relNumber,
                            (int) blk->forknum),
                     errdetail("表多半在认领之后被 DROP；继续 redo 会触发"
                               "不可捕获的 PANIC（R-P4-8）。")));
            return;             /* 跳过本条，游标由调用方推进 */
        }

        /*
         * 块号边界检查：记录要落的块号若已超出本地关系的实际大小就跳过。
         * 只对"不会自建页"的记录生效 —— 带整页镜像（has_image）或标了
         * WILL_INIT 的记录本就会把页建出来，拦它们反而破坏正常回放。
         *
         * ★ 务必看清它**不解决 R-P4-20**（2026-08-17 实测确认）。
         * R-P4-20 的 PANIC 抛在 heapam.c：
         *     action = XLogReadBufferForRedo(record, 0, &buffer);
         *     if (action == BLK_NEEDS_REDO)
         *         if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
         *             elog(PANIC, "invalid max offset number");
         * 即**页存在、块号在界内、但页太短**（缺目标 offnum 之前的行指针）。
         * 本检查按块号判断，够不着那一种 —— 加上它之后 10 轮验证里该
         * WARNING **一次都没触发**，PANIC 照常复发。留着它是因为"记录指向
         * 本地不存在的块"本身也该拦，不是因为它修好了什么。
         */
        /*
         * ★ R-P4-20 诊断已于 T6.3a 迁走（2026-09-02）。
         *
         * 原先它挂在 `if (ctx->applied_part_lsn == 0)` 上，理由是"只在出事形态
         * 下打，不污染正常日志"。实测把 30 次 PANIC 与前一行认领日志逐条配对
         * 之后发现：**最后 7 次的游标是 10 和 11，不是 0** —— 这个精心埋的
         * 取证点一次都没为它们打过，几何形状因此两周无从回溯。
         *
         * 现在诊断挂在 ShardReplayOffnumPrecheck 的**跳闸点**上：只在真要出事
         * 时打，且无论游标是几都打得到。既不污染日志，也不会再漏。
         */

        if (!blk->has_image && (blk->flags & BKPBLOCK_WILL_INIT) == 0)
        {
            BlockNumber nblocks = smgrnblocks(smgr, blk->forknum);

            if (blk->blkno >= nblocks)
            {
                ereport(WARNING,
                        (errmsg("pg_partdist replay: 记录越出本地关系边界，跳过该记录"
                                "（shard %u，plsn=%llu，relNumber=%u fork=%d "
                                "blkno=%u 实际块数=%u）",
                                ctx->shard_oid,
                                (unsigned long long) hdr->partition_lsn,
                                (unsigned) blk->rlocator.relNumber,
                                (int) blk->forknum,
                                (unsigned) blk->blkno,
                                (unsigned) nblocks),
                         errdetail("本地关系比记录所设想的短 —— 多为新建的空壳表"
                                   "被灌入上一代的段流。继续 redo 会触发不可捕获的"
                                   "PANIC: invalid max offset number（R-P4-20）。")));
                return;         /* 跳过本条，游标由调用方推进 */
            }
        }
    }

    /*
     * ★ R-P4-20 完整性校验（2026-08-18）
     *
     * 本模块此前把段文件里的字节直接当 XLogRecord 交给 redo：
     * DecodeXLogRecord 只校验**结构**（块引用与各段长度是否自洽），
     * 既不校验 CRC，也不校验记录主体数据 —— 而 xl_heap_insert.offnum
     * 恰恰在主体数据里。PostgreSQL 自己的回放路径是校验 CRC 的，这里绕过了。
     *
     * 这道校验有两个作用：
     *   ① 健壮性：外来字节进 redo 前先验完整性，本就该有（独立于本次崩溃）；
     *   ② 判据：CRC 通过 ⇒ 记录是主副本真实写出来的 ⇒ 崩溃属"流与本地关系
     *      不同代"；CRC 失败 ⇒ 字节损坏或读偏 ⇒ 属读取路径缺陷。
     *      两者修法完全不同，这一位就能定案。
     */
    {
        pg_crc32c   crc;

        INIT_CRC32C(crc);
        COMP_CRC32C(crc, ((char *) record) + SizeOfXLogRecord,
                    record->xl_tot_len - SizeOfXLogRecord);
        COMP_CRC32C(crc, (char *) record, offsetof(XLogRecord, xl_crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(record->xl_crc, crc))
        {
            ereport(WARNING,
                    (errmsg("pg_partdist replay [R-P4-20]: 记录 CRC 校验失败，跳过"
                            "（shard %u plsn=%llu rmid=%u info=0x%02x tot_len=%u）",
                            ctx->shard_oid,
                            (unsigned long long) hdr->partition_lsn,
                            (unsigned) record->xl_rmid,
                            (unsigned) (record->xl_info & ~XLR_INFO_MASK),
                            (unsigned) record->xl_tot_len),
                     errdetail("段流字节与其自带校验和不符：字节损坏或读取定位错误。"
                               "继续 redo 会把损坏内容写进本地关系。")));
            return;             /* 跳过本条，游标由调用方推进 */
        }
    }

    /*
     * ★ T6.3a：offnum 前置检查（2026-09-02）
     *
     * 取代了此前那个只覆盖"带 INIT_PAGE 的 INSERT"的特例陷阱 —— 那是本检查的
     * 一个子集。现在 INSERT / UPDATE / MULTI_INSERT 三类一并查，判据与内核
     * 完全一致（maxoff + 1 < offnum），跳闸即 WARNING + 跳过本条，
     * **绝不让内核 elog(PANIC) 把整节点带走**。
     */
    if (!ShardReplayOffnumPrecheck(ctx, ctx->reader, record, hdr))
        return;                 /* 跳过本条，游标由调用方推进 */

    /* 4) 派发原生 redo */
    GetRmgr(record->xl_rmid).rm_redo(ctx->reader);

    REPLAY_TRACE("TRACE data: rm_redo done plsn=%llu",
                 (unsigned long long) hdr->partition_lsn);

    ctx->reader->record = NULL;
    pfree(decoded);
}

/* ================================================================== */
/* 事务号水位与 xid_map（FRD §7.5 / §9.2）                             */
/* ================================================================== */

/*
 * PartDistAdvanceNextXidPastXid — 把本地 nextXid 拉到严格大于 xid。
 *
 * 逐行对照 varsup.c 的 AdvanceNextFullTransactionIdPastXid()，两处不同：
 *
 * 1. 核内那个版本无锁读 nextXid（带 Assert(AmStartupProcess() || !IsUnderPostmaster)
 *    声明这只对 startup 进程安全），replay worker 是普通 bgworker，assert 构建下
 *    直接崩。这里读-改-写全程持 XidGenLock。
 *
 * 2. ★★★ 被跳过的 xid 区间必须补 ExtendCLOG/ExtendCommitTs/ExtendSUBTRANS
 *    （§13 约束 4）。核内版本不补是有前提的：它只在 startup 进程里跑，standby
 *    的 clog ZEROPAGE 记录会随主库 WAL 流一起回放过来。我们的回放**没有**这条
 *    流 —— nextXid 一旦跳过 clog 页边界（32768 个 xid 一页），那一页的 ZEROPAGE
 *    既没人写、WAL 里也没有，页只以内存 SLRU 的形式碰巧存在。实测两个症状
 *    （2026-08-05，同一天两种死法）：
 *
 *      a) kill -9 后节点起不来：崩溃恢复 redo 到本地事务的 COMMIT 记录，
 *             FATAL: could not access status of transaction 57338
 *             Could not read from file "pg_xact/0000" at offset 8192
 *         pg_xact 文件只有 1 页而本地 xid 已用到 57k。只能手工 dd 补零页救。
 *
 *      b) autovacuum 无限自旋拖死整节点：xid 落在不可靠 clog 覆盖区，
 *         可见性判定自相矛盾，lazy_scan_prune 的重试循环无中断检查地
 *         攥着 buffer 锁转 54 分钟，pg_cancel 无效，唯一解法 kill -9 ——
 *         随即触发 (a)。
 *
 *    补法与 GetNewTransactionId 完全同构：对区间内每个 xid 依次调三个
 *    Extend*（它们各自只在"该 xid 是所在页第一个"时真正清零建页并写
 *    ZEROPAGE WAL，其余情况是一次取模判断的空转）。必须在 XidGenLock 内做：
 *    锁外的并发本地分配可能已在同一页上提交，Extend* 的重新清零会抹掉
 *    已提交状态。锁内则区间 (旧 nextXid, 新 nextXid) 保证无人用过，清零幂等。
 */
void
PartDistAdvanceNextXidPastXid(TransactionId xid)
{
    FullTransactionId newNextFullXid;
    TransactionId     next_xid;
    TransactionId     cur;
    uint32            epoch;
    uint32            gap;

    if (!TransactionIdIsNormal(xid))
        return;

    LWLockAcquire(XidGenLock, LW_EXCLUSIVE);

    next_xid = XidFromFullTransactionId(ShmemVariableCache->nextXid);
    epoch    = EpochFromFullTransactionId(ShmemVariableCache->nextXid);

    if (!TransactionIdFollowsOrEquals(xid, next_xid))
    {
        LWLockRelease(XidGenLock);
        return;                 /* 本地已经走在前面 */
    }

    /*
     * 给 [旧 nextXid, xid] 里的每个 xid 补齐三件套 —— 语义上等价于
     * "这些 xid 都像被本地 GetNewTransactionId 分配过一样"。
     * gap 按 32 位回绕差计算，只用于日志。
     */
    gap = (uint32) (xid - next_xid) + 1;
    if (gap > 1000000)
        ereport(WARNING,
                (errmsg("pg_partdist replay: nextXid 一次性跳进 %u 个 xid"
                        "（%u -> %u），clog/subtrans 逐页补齐会持锁较久",
                        gap, next_xid, xid)));

    cur = next_xid;
    for (;;)
    {
        ExtendCLOG(cur);
        ExtendCommitTs(cur);
        ExtendSUBTRANS(cur);
        if (TransactionIdEquals(cur, xid))
            break;
        TransactionIdAdvance(cur);
    }

    /*
     * 目标是 xid + 1。32 位加一回绕时 epoch 进位 —— 与核内同款判据：
     * 新值比旧的 next_xid 小就说明绕过去了。
     */
    TransactionIdAdvance(xid);
    if (xid < next_xid)
        epoch++;

    newNextFullXid = FullTransactionIdFromEpochAndXid(epoch, xid);
    if (FullTransactionIdFollows(newNextFullXid, ShmemVariableCache->nextXid))
        ShmemVariableCache->nextXid = newNextFullXid;

    LWLockRelease(XidGenLock);
}

/*
 * ShardReplayInitXidMap — 建空的 xid_map，并把水位的 epoch 对齐到本地。
 *
 * epoch 必须取本地的：leader 的 epoch 不在段流里，而这个水位存在的意义
 * 就是和本地 nextXid 比大小，两边口径必须一致。
 */
void
ShardReplayInitXidMap(ShardReplayCtx *ctx)
{
    HASHCTL info;

    memset(&info, 0, sizeof(info));
    info.keysize   = sizeof(TransactionId);
    info.entrysize = sizeof(XidMapEntry);
    info.hcxt      = TopMemoryContext;

    ctx->xid_map = hash_create("pg_partdist shard xid_map", 1024, &info,
                               HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    LWLockAcquire(XidGenLock, LW_SHARED);
    ctx->max_replayed_fxid =
        FullTransactionIdFromEpochAndXid(
            EpochFromFullTransactionId(ShmemVariableCache->nextXid),
            FirstNormalTransactionId);
    LWLockRelease(XidGenLock);
}

/*
 * ShardReplayRestoreXidMap — 用 checkpoint 快照重建 xid_map。
 */
void
ShardReplayRestoreXidMap(ShardReplayCtx *ctx, const XidMapEntry *ents,
                         uint32 nents)
{
    uint32 i;

    if (ctx->xid_map == NULL || ents == NULL || nents == 0)
        return;

    for (i = 0; i < nents; i++)
    {
        XidMapEntry *e;
        bool         found;

        e = (XidMapEntry *) hash_search(ctx->xid_map, &ents[i].local_xid,
                                        HASH_ENTER, &found);
        e->reserved = 0;
        e->gxid     = ents[i].gxid;
    }

    ereport(DEBUG1,
            (errmsg("shard replay: shard %u 从 checkpoint 恢复 xid_map %u 条",
                    ctx->shard_oid, nents)));
}

/*
 * ShardReplayAdvanceWatermark — 把本地 nextXid 拉到回放水位之上。
 */
void
ShardReplayAdvanceWatermark(ShardReplayCtx *ctx)
{
    TransactionId xid = XidFromFullTransactionId(ctx->max_replayed_fxid);

    if (TransactionIdIsNormal(xid))
        PartDistAdvanceNextXidPastXid(xid);
}

/*
 * ShardReplayNoteXid — 登记一个回放引入的事务号。
 *
 * 两件事：
 *   1. 写 xid_map（local_xid → gxid）。同一个 local_xid 在同一分区内只会
 *      对应一个来源节点（那是 gxid 存在的理由），重复登记是幂等的。
 *   2. 抬高 max_replayed_fxid。折算 64 位用**本地当前 epoch** —— leader 的
 *      epoch 不在流里，而这个水位只用于和本地 nextXid 比较，本地口径即可。
 */
void
ShardReplayNoteXid(ShardReplayCtx *ctx, GlobalTransactionId gxid)
{
    TransactionId     local_xid = (TransactionId) GxidLocalXid(gxid);
    XidMapEntry      *ent;
    bool              found;
    FullTransactionId fxid;

    if (!TransactionIdIsNormal(local_xid))
        return;                 /* 无 xid 的记录（如 VACUUM 的部分 freeze）*/

    if (ctx->xid_map != NULL)
    {
        if (hash_get_num_entries(ctx->xid_map) >= SHARD_XIDMAP_MAX_ENTRIES)
            ereport(ERROR,
                    (errmsg("shard replay: shard %u 的 xid_map 条目数达到上限 %d",
                            ctx->shard_oid, SHARD_XIDMAP_MAX_ENTRIES),
                     errdetail("条目要等对应 xid 全部冻结后才可回收（FRD §8.4）；"
                               "在那套账目接上之前这里硬停，不静默丢条目 —— "
                               "丢一条就是丢一份可见性信息。")));

        ent = (XidMapEntry *) hash_search(ctx->xid_map, &local_xid,
                                          HASH_ENTER, &found);
        ent->reserved = 0;      /* 条目会原样落盘并计入 CRC，补齐位必须确定 */
        ent->gxid     = gxid;
    }

    /*
     * 按本地当前 epoch 折算成 64 位（§7.1 注：32 位跨 epoch 比较会失效）。
     *
     * 已知局限：若 leader 的 xid 在一次追平内跨过 32 位回绕，回绕之后的
     * local_xid 折算出的 fxid 会小于当前水位，这里就不再抬高 —— 水位停在
     * 回绕点。要正确处理得靠冻结账目给出"当前 epoch"的可信来源，那属于
     * R2-e（§13.5）的范围；单次追平跨 40 亿个 xid 在本项目的规模下不会发生，
     * 先按停在回绕点处理，而不是假装折算正确。
     */
    fxid = FullTransactionIdFromEpochAndXid(
               EpochFromFullTransactionId(ctx->max_replayed_fxid), local_xid);
    if (FullTransactionIdFollows(fxid, ctx->max_replayed_fxid))
        ctx->max_replayed_fxid = fxid;
}

/* ================================================================== */
/* MARKER 记录应用（FRD §7.6）                                         */
/* ================================================================== */

/*
 * ApplyMarkerRecord — 事务标记登记。
 *
 * R2-b 阶段只做**校验与解析**：把载荷长度、类别、子事务清单核对清楚，并把
 * 顶层与子事务的 gxid 解出来。真正的落账（增强型 CLOG / pg_gclog 写入、
 * xid_map 登记、max_replayed_fxid 推进）分别在 R2-c / R2-d 接上 ——
 * 那两处只需要在下面标注的位置各插一次调用，解析逻辑不再改动。
 *
 * 与 DATA 的关键区别：标记记录**不碰任何页面**，因此不参与页级幂等
 * （BLK_DONE）那套机制。重复回放的幂等性由增强型 CLOG 的"写同一状态"
 * 天然满足（R2-d）。
 */
static void
ApplyMarkerRecord(ShardReplayCtx *ctx, const PartWALRecord *hdr, char *body)
{
    TxnMarkerPayload    *m = (TxnMarkerPayload *) body;
    GlobalTransactionId  gxid = PartWALRecordGxid(hdr);
    TransactionId       *subxacts;
    uint16               origin;
    uint8                op;
    uint32               i;
    bool                 has_sx = false;
    TransactionId        sxid = InvalidTransactionId;

    if (hdr->version < PARTWAL_RECORD_VERSION_3)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu MARKER 记录出现在 "
                        "version=%u 的流里（2.0 无事务层语义）",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->version)));

    if (hdr->data_len < sizeof(TxnMarkerPayload))
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu MARKER 载荷过短 (%u)",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->data_len)));

    if (m->flags & ~PARTWAL_MARKER_KNOWN_FLAGS)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu MARKER 出现未知 flags "
                        "0x%08X —— 拒绝按旧语义蒙混过去", ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn, m->flags)));

    has_sx = (m->flags & PARTWAL_MARKER_HAS_SHARD_XID) != 0;

    if (hdr->data_len != (uint32) TxnMarkerPayloadSizeEx(m->nsubxacts, m->flags))
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu MARKER 载荷长度不符"
                        "（data_len=%u，nsubxacts=%u flags=0x%08X 需要 %zu）",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->data_len, m->nsubxacts, m->flags,
                        TxnMarkerPayloadSizeEx(m->nsubxacts, m->flags))));

    if (has_sx)
        sxid = (TransactionId) *TxnMarkerShardXidPtr(m);

    /*
     * U-P5-1 之二：记下 leader 的发号水位，取 max 后随 apply checkpoint 落盘
     * （复用 T5.4b-2 那条"worker 只暂存、由 replay_catchup 的调用方写"的通路）。
     * 这里不当场落盘：每条 MARKER 一次 fsync 太重，而 follower 崩溃后从持久
     * 游标重放会重新导出同一个值，语义安全。
     */
    if (m->flags & PARTWAL_MARKER_HAS_ALLOC_WM)
    {
        TransactionId wm = (TransactionId) *TxnMarkerAllocWmPtr(m);

        if (wm > ctx->pending_alloc_wm)
            ctx->pending_alloc_wm = wm;
    }

    origin   = GxidNodeId(gxid);
    op       = hdr->info & XLOG_XACT_OPMASK;
    subxacts = TxnMarkerSubxacts(m);

    if (op != XLOG_XACT_COMMIT && op != XLOG_XACT_ABORT &&
        op != XLOG_XACT_PREPARE)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu MARKER 的 info=0x%02X "
                        "不是 COMMIT/ABORT/PREPARE",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn, hdr->info)));

    REPLAY_TRACE("TRACE marker: plsn=%llu %s node=%u xid=%llu nsub=%u "
                 "start_ts=%llu commit_ts=%llu",
                 (unsigned long long) hdr->partition_lsn,
                 (op == XLOG_XACT_COMMIT) ? "COMMIT" : "ABORT",
                 origin, (unsigned long long) GxidLocalXid(gxid),
                 m->nsubxacts,
                 (unsigned long long) m->start_ts,
                 (unsigned long long) m->commit_ts);

    /*
     * 顶层与全部已提交子事务都要进 xid_map / 抬高水位：子事务写下的元组
     * 带的是**子事务自己的** xid，升主后判定这些元组的可见性同样要能
     * 由 local_xid 反查到 gxid。
     */
    ShardReplayNoteXid(ctx, gxid);
    for (i = 0; i < m->nsubxacts; i++)
        ShardReplayNoteXid(ctx, MakeGlobalXid(origin, subxacts[i]));

    /*
     * 落账到增强型 CLOG（§7.6）。
     *
     * COMMIT 要写**整棵提交树** —— 顶层 + 全部已提交子事务，等价于
     * TransactionIdCommitTree 的全局版本。被 ROLLBACK TO 掉的子事务不在
     * m->subxacts 里，于是它的 gxid 在 gclog 中始终是全零槽 = TXN_RUNNING
     * = 未决 = 不可见：SAVEPOINT 回滚语义由"缺席"表达，不需要额外写 ABORTED。
     *
     * 只写不 fsync；落盘由 apply checkpoint 前的 EnhancedClogSync() 保证
     * （§8.4 推进协议第 2 步）。重放同一条 MARKER 会算出同样的槽内容，
     * 幂等，崩溃恢复正是靠这一点。
     */
    /*
     * ★ U-P5-1（设计 §5.3）：分片 clog 也由流重建。
     *
     * 在此之前 follower 只写增强型 clog（按 gxid 索引），
     * `pg_shard_clog/<oid>` 在它上面根本不存在 —— 升主之后每个分片 xid 都
     * 读成空洞＝RUNNING＝不可见，整张表看不见。设计 §5.3 要的正是
     * 「每个副本重放同一个流得到同一本账」。
     *
     * 分片 oid 用**本地的** ctx->shard_oid：MARKER 逐分区追加，一个分区就是
     * 一个分片，载荷里只带 xid 不带 oid，因此不需要跨节点翻译。
     *
     * 顺带推进影子发号水位（ShardXidRedoAdvance）：升主后发号器建槽时取
     * `Max(文件 alloc_wm, 影子)`，这一步让新主不至于从 3 号重发。
     * **注意这只是本次启动内的内存影子**，跨重启的持久交接仍是缺口
     * （见 DEV PLAN U-P5-1 记要"未做的一半"）。
     */
    if (TransactionIdIsNormal(sxid))
    {
        ShardXidRedoAdvance(ctx->shard_oid, sxid);

        if (op == XLOG_XACT_COMMIT)
            ShardClogSetVerdict(ctx->shard_oid, sxid, true, (int64) m->commit_ts);
        else if (op == XLOG_XACT_ABORT)
            ShardClogSetVerdict(ctx->shard_oid, sxid, false, 0);
        else                    /* XLOG_XACT_PREPARE */
            ShardClogSetPrepared(ctx->shard_oid, sxid, (int64) m->start_ts,
                                 (int64) gxid);

        REPLAY_TRACE("TRACE marker: shard clog 落账 shard=%u sxid=%u op=0x%02X",
                     ctx->shard_oid, sxid, op);
    }

    if (op == XLOG_XACT_COMMIT)
    {
        EnhancedClogWriteStatus(gxid, m->start_ts, m->commit_ts, TXN_COMMITTED);
        for (i = 0; i < m->nsubxacts; i++)
            EnhancedClogWriteStatus(MakeGlobalXid(origin, subxacts[i]),
                                    m->start_ts, m->commit_ts, TXN_COMMITTED);
    }
    else if (op == XLOG_XACT_PREPARE)
    {
        /*
         * 2PC 的第一段（DTX_2PC_DESIGN.md §3.3）：判决还没到，整棵提交树
         * 先落成 TXN_PREPARED = 未决 = 不可见 —— 与"从未写过"的空洞槽同义，
         * 因此中途崩溃、或决议永远不来，语义都是安全的。
         *
         * 子事务额外记下 parent_xid：COMMIT PREPARED 那条语句跑在**另一个
         * 事务**里，拿不到本事务的子事务清单，所以第二段的 COMMIT 标记只
         * 携带顶层 xid。读路径靠这条链把子事务解析到顶层的判决上
         * （见 enhanced_clog.h 里 parent_xid 的注释）。
         */
        EnhancedClogWriteStatus(gxid, m->start_ts, 0, TXN_PREPARED);
        for (i = 0; i < m->nsubxacts; i++)
            EnhancedClogWriteStatusWithParent(MakeGlobalXid(origin, subxacts[i]),
                                              m->start_ts, 0, TXN_PREPARED,
                                              (TransactionId) GxidLocalXid(gxid));
    }
    else
        EnhancedClogWriteStatus(gxid, m->start_ts, 0, TXN_ABORTED);
}

/* ================================================================== */
/* CTRL 记录应用（FRD §7.7/§12）                                       */
/* ================================================================== */

/*
 * 记下"停在结构栅栏上"。返回 false 让调用方跳出应用循环 **且不推进游标** ——
 * 这是 NEEDS_STRUCT 与 FAILED 的实质区别：栅栏是可原地恢复的，
 * 运维把本地结构补齐、重跑 replay_set_locmap() 之后，从同一个游标继续即可。
 */
static bool
ReplayFenceStruct(ShardReplayCtx *ctx, const char *fmt,...)
    pg_attribute_printf(2, 3);

static bool
ReplayFenceStruct(ShardReplayCtx *ctx, const char *fmt,...)
{
    va_list ap;

    va_start(ap, fmt);
    vsnprintf(ctx->struct_errmsg, sizeof(ctx->struct_errmsg), fmt, ap);
    va_end(ap);

    ctx->needs_struct = true;

    ereport(LOG,
            (errmsg("shard replay: shard %u 停在结构栅栏 @plsn %llu: %s",
                    ctx->shard_oid,
                    (unsigned long long) ctx->applied_part_lsn + 1,
                    ctx->struct_errmsg)));
    return false;
}

/* 在当前 loc_map 里按 (role, ord) 找本地文件号 */
static bool
LocalLocForRoleOrd(ShardReplayCtx *ctx, uint8 role, uint8 ord,
                   RelFileLocator *out)
{
    HASH_SEQ_STATUS seq;
    LocMapEntry    *e;

    hash_seq_init(&seq, ctx->loc_map);
    while ((e = (LocMapEntry *) hash_seq_search(&seq)) != NULL)
    {
        if (e->role == role && e->ord == ord)
        {
            *out = e->local_loc;
            hash_seq_term(&seq);
            return true;
        }
    }
    return false;
}

/*
 * 把一个本地关系的全部 fork 截成 0 块。
 *
 * leader 侧换了文件号（VACUUM FULL / REINDEX / TRUNCATE）意味着那是一个
 * **全新的空文件**，随后流里跟着的是它全部页面的 FPI。本地对应文件必须先
 * 清空：否则旧文件比新文件长时，FPI 覆盖不到的尾部会留下上一代的残页，
 * 而两侧文件长度不同这件事本身就会让页面比对直接判负。
 *
 * 幂等：崩溃后从游标重放会再截一次 0，再重放同一批 FPI，结果相同（§8.4）。
 */
static void
ReplayTruncateLocalRel(const RelFileLocator *loc)
{
    SMgrRelation reln = smgropen(*loc, InvalidBackendId);
    ForkNumber   forks[MAX_FORKNUM + 1];
    BlockNumber  old_blocks[MAX_FORKNUM + 1];
    BlockNumber  blocks[MAX_FORKNUM + 1];
    int          nforks = 0;
    ForkNumber   f;

    smgrcreate(reln, MAIN_FORKNUM, true);

    for (f = 0; f <= MAX_FORKNUM; f++)
    {
        BlockNumber n;

        if (!smgrexists(reln, f))
            continue;
        n = smgrnblocks(reln, f);
        if (n == 0)
            continue;

        forks[nforks]      = f;
        old_blocks[nforks] = n;
        blocks[nforks]     = 0;
        nforks++;

        /* invalid_page_tab 记的是本地文件号（同 ApplySmgrRecord） */
        XLogTruncateRelation(*loc, f, 0);
    }

    if (nforks > 0)
    {
        START_CRIT_SECTION();
        smgrtruncate2(reln, forks, nforks, old_blocks, blocks);
        END_CRIT_SECTION();
    }
}

/*
 * ApplyFreezeRecord — 收下 leader 的冻结账目，搁进 ctx 待写。
 *
 * **这里不碰 catalog**：写 pg_class 要开事务，而 CommitTransactionCommand
 * 会把 CurrentResourceOwner 置空，回放循环后续的 buffer pin 记账随即踩空。
 * 真正的写入在 ShardReplayDoCheckpoint 里做（写游标之前）。
 */
static bool
ApplyFreezeRecord(ShardReplayCtx *ctx, const PartWALRecord *hdr,
                  const char *body)
{
    const PartWALCtrlFreezeUpdate *upd;
    const PartWALFreezeEntry      *ents;
    uint32                         i;
    bool                           has_wm;

    if (hdr->data_len < sizeof(PartWALCtrlFreezeUpdate))
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu FREEZE_UPDATE 载荷"
                        "过短 (%u)", ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->data_len)));

    upd = (const PartWALCtrlFreezeUpdate *) body;
    has_wm = (upd->flags & PARTWAL_FREEZE_HAS_VACUUM_WM) != 0;

    if (upd->flags & ~PARTWAL_FREEZE_HAS_VACUUM_WM)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu FREEZE_UPDATE 出现"
                        "未知 flags 0x%08X —— 拒绝按旧语义蒙混过去",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn, upd->flags)));

    /*
     * T5.4b-2：nrels 允许为 0，**但仅当带了 vacuum 水位块** —— 两本账各发
     * 各的（vacuum 截断只带水位，D2 的冻结账目只带 rels），而两者皆空的
     * 记录没有意义。
     */
    if (upd->nrels > SHARD_FILESET_MAX_RELS ||
        (upd->nrels < 1 && !has_wm) ||
        hdr->data_len != (uint32) PartWALCtrlFreezeUpdateSizeEx(upd->nrels, has_wm))
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu FREEZE_UPDATE 长度"
                        "与 nrels/flags 不符 (len=%u nrels=%u flags=0x%08X)",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->data_len, upd->nrels, upd->flags)));

    ents = PartWALCtrlFreezeRels(upd);

    for (i = 0; i < upd->nrels; i++)
        if (ents[i].role != SHARD_REL_MAIN && ents[i].role != SHARD_REL_TOAST)
            ereport(ERROR,
                    (errmsg("shard replay: shard %u @plsn %llu FREEZE_UPDATE "
                            "出现非堆 role=%u —— 索引的 relfrozenxid 恒为 0，"
                            "不该出现在这里", ctx->shard_oid,
                            (unsigned long long) hdr->partition_lsn,
                            ents[i].role)));

    /* 全量覆盖：同一分区的后一条 FREEZE_UPDATE 天然作废前一条 */
    if (upd->nrels > 0)
    {
        memcpy(ctx->pending_freeze, ents,
               (size_t) upd->nrels * sizeof(PartWALFreezeEntry));
        ctx->pending_freeze_n = (int) upd->nrels;
    }

    if (has_wm)
    {
        const PartWALFreezeVacuumWm *wm = PartWALCtrlFreezeVacuumWm(upd);

        ctx->pending_trunc_before = (TransactionId) wm->clog_truncate_before;
        ctx->pending_vacuum_xid   = (TransactionId) wm->shard_vacuum_xid;
        ctx->pending_vacuum_wm    = true;

        REPLAY_TRACE("TRACE freeze: shard %u 收到 vacuum 水位 %u/%u",
                     ctx->shard_oid,
                     ctx->pending_trunc_before, ctx->pending_vacuum_xid);
    }

    return true;
}

/*
 * ApplyCtrlRecord — 应用一条控制记录。
 *
 * 返回 true = 已应用，调用方推进游标；false = 停在栅栏，游标原地不动。
 *
 * 目前只有 FILESET_UPDATE 一个 opcode。它的语义是"leader 的物理文件集合
 * 换了，这是新的全量描述"。follower 的处置分两种，判据是**结构有没有变**：
 *
 *   (role, ord) 集合不变，只是文件号变了
 *       → VACUUM FULL / REINDEX / TRUNCATE / 重写类 ALTER。本地关系一一对应
 *         得上，原地换表 + 把对应本地文件截 0，后续 FPI 自然填满。全自动。
 *
 *   (role, ord) 集合变了（增或减）
 *       → CREATE INDEX / DROP INDEX。停在栅栏。
 *
 * 为什么集合变了就不能自动配：ord 是**位置**（索引定义序），不是身份。
 * leader 删掉 ord=0 那个索引之后，原来的 ord=1 会补位成 ord=0 —— 若照
 * (role, ord) 硬配，follower 会把 leader 新的 0 号索引的内容灌进本地那个
 * 本该被删掉的 0 号索引文件里，而 catalog 还宣称它是另一组列上的索引。
 * 那是**静默损坏**，比停下来难查得多。
 */
static bool
ApplyCtrlRecord(ShardReplayCtx *ctx, const PartWALRecord *hdr,
                const char *body)
{
    const PartWALCtrlFilesetUpdate *upd;
    const ShardFileSetRel          *rels;
    ReplayLocMapFile                lm;
    HASHCTL                         hctl;
    HTAB                           *new_map;
    uint32                          i;

    if (hdr->version < PARTWAL_RECORD_VERSION_3 ||
        hdr->rmid != PARTWAL_CTRL_RMID)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu CTRL 记录头不合法"
                        "（version=%u rmid=%u）", ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->version, hdr->rmid)));

    if (hdr->info == PARTWAL_CTRL_FREEZE_UPDATE)
        return ApplyFreezeRecord(ctx, hdr, body);

    if (hdr->info != PARTWAL_CTRL_FILESET_UPDATE)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu 未知 CTRL opcode "
                        "0x%02X —— 拒绝静默跳过控制记录",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn, hdr->info)));

    if (hdr->data_len < sizeof(PartWALCtrlFilesetUpdate))
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu FILESET_UPDATE "
                        "载荷过短 (%u)", ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->data_len)));

    upd = (const PartWALCtrlFilesetUpdate *) body;

    if (upd->nrels < 1 || upd->nrels > SHARD_FILESET_MAX_RELS ||
        hdr->data_len != (uint32) PartWALCtrlFilesetUpdateSize(upd->nrels))
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu FILESET_UPDATE "
                        "长度与 nrels 不符 (len=%u nrels=%u)",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->data_len, upd->nrels)));

    rels = PartWALCtrlFilesetRels(upd);

    /*
     * ★ T6.1：未知标志位一律 ERROR，绝不静默忽略。
     *
     * 此前这里对 flags 只查 NEEDS_REBASELINE 一位，别的位视而不见 —— 那等于
     * 允许"新版 leader 发了新语义、旧版 follower 当没看见照常重放"，是最难查
     * 的一类分歧。MARKER 的标志位扩展当初就立了这条规矩，CTRL 这边补上。
     */
    if ((upd->flags & ~PARTWAL_FSUPD_KNOWN_FLAGS) != 0)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu FILESET_UPDATE "
                        "含未知标志位 0x%04X（已知 0x%04X）",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        (unsigned) upd->flags,
                        (unsigned) PARTWAL_FSUPD_KNOWN_FLAGS),
                 errdetail("leader 版本比本副本新 —— 拒绝按旧语义重放。")));

    /* 两位语义相反（内容没来 vs 内容全来了），同时置位是协议错误 */
    if ((upd->flags & PARTWAL_FSUPD_NEEDS_REBASELINE) != 0 &&
        (upd->flags & PARTWAL_FSUPD_FULL_BASELINE) != 0)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu FILESET_UPDATE "
                        "同时置位 NEEDS_REBASELINE 与 FULL_BASELINE",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn)));

    if ((upd->flags & PARTWAL_FSUPD_NEEDS_REBASELINE) != 0)
        return ReplayFenceStruct(ctx,
                                 "leader 的 fileset 变更超过 "
                                 "fileset_inline_max_blocks，新文件内容未随流"
                                 "携带 —— 本副本须重做物理基线拷贝");

    /*
     * 结构判据：新 fileset 的 (role, ord) 集合必须与当前 loc_map 完全相同。
     * 个数相等 + 每个 leader 成员都能在本地找到同 (role, ord) 的对应，
     * 因为两侧 (role, ord) 各自唯一，这两条合起来即集合相等。
     */
    if (upd->nrels != (uint32) hash_get_num_entries(ctx->loc_map))
        return ReplayFenceStruct(ctx,
                                 "leader fileset 成员数由 %ld 变为 %u"
                                 "（索引增删）—— 请在本地 shell 表上做等价"
                                 "结构变更后重跑 replay_set_locmap()",
                                 hash_get_num_entries(ctx->loc_map),
                                 upd->nrels);

    memset(&lm, 0, sizeof(lm));
    lm.magic     = REPLAY_LOCMAP_MAGIC;
    lm.version   = REPLAY_LOCMAP_VERSION;
    lm.shard_oid = ctx->shard_oid;
    lm.npairs    = 0;

    /*
     * ★ T6.2：起效游标必须跟着走，不能被这次换表抹成 0。
     *
     * 普通 fileset 变更沿用原值；**全量基线**则把它推进到本条 CTRL 自己的
     * 编号 —— 基线之后，"本配对自哪个游标起有效"的答案就是这一条。这样
     * 崩在基线中途（CTRL 已应用、FPI 还没灌完、checkpoint 也没落）时，
     * 重启会从这条 CTRL 重来：再截一次、再灌一遍，幂等且正确；若还停在旧
     * base，就要多走一大段无谓的重放。
     */
    lm.base_part_lsn =
        ((upd->flags & PARTWAL_FSUPD_FULL_BASELINE) != 0)
            ? hdr->partition_lsn
            : ctx->base_part_lsn;

    for (i = 0; i < upd->nrels; i++)
    {
        RelFileLocator local;

        if (!LocalLocForRoleOrd(ctx, rels[i].role, rels[i].ord, &local))
            return ReplayFenceStruct(ctx,
                                     "leader 的 (role=%u, ord=%u) 在本地"
                                     " shell 表上无对应关系 —— 请做等价结构"
                                     "变更后重跑 replay_set_locmap()",
                                     rels[i].role, rels[i].ord);

        lm.pairs[lm.npairs].leader_loc = rels[i].loc;
        lm.pairs[lm.npairs].local_loc  = local;
        lm.pairs[lm.npairs].role       = rels[i].role;
        lm.pairs[lm.npairs].ord        = rels[i].ord;
        lm.pairs[lm.npairs].reserved   = 0;
        lm.npairs++;
    }

    /*
     * 过了上面全部校验才动手 —— 栅栏必须在**一个字节都没改**之前立起来，
     * 否则"游标没推进"就不再等于"什么都没发生"。
     *
     * 先截断：leader 文件号变了的成员，本地文件清空等 FPI 重建。
     * 文件号没变的（哈希里查得到）内容仍然有效，一个字节都不要动。
     */
    {
        bool full_baseline = ((upd->flags & PARTWAL_FSUPD_FULL_BASELINE) != 0);

        for (i = 0; i < upd->nrels; i++)
        {
            bool found;

            /*
             * ★ T6.1 全量基线：**每一个**成员都截成 0 块，不管它的 leader
             * 文件号变没变。这正是基线与 DDL 变更的唯一区别 —— DDL 只需要
             * 重建换了号的那几个（没换号的历史记录早已在流里、内容仍然有效），
             * 而基线的语义是"忘掉之前的一切，后面这批 FPI 就是全部真相"。
             *
             * 不截干净的后果很具体：本地文件比 leader 长时，尾部那些**基线
             * 不覆盖**的块会原样留下来，成为副本里一段谁也不会再碰的幽灵数据
             * （FRD §13 里"副本文件长于 leader，运行期不 PANIC，只有逐页 diff
             * 能发现"说的就是这种）。
             */
            (void) hash_search(ctx->loc_map, &rels[i].loc, HASH_FIND, &found);
            if (full_baseline || !found)
                ReplayTruncateLocalRel(&lm.pairs[i].local_loc);
        }

        if (full_baseline)
            ereport(LOG,
                    (errmsg("pg_partdist replay: shard %u @plsn %llu 收到全量"
                            "物理基线，已截断全部 %u 个本地文件，等待 FPI 重建",
                            ctx->shard_oid,
                            (unsigned long long) hdr->partition_lsn,
                            upd->nrels)));
    }

    /* 换表：整张 loc_map 重建（新旧文件号在同一临界区换完，§12） */
    memset(&hctl, 0, sizeof(hctl));
    hctl.keysize   = sizeof(RelFileLocator);
    hctl.entrysize = sizeof(LocMapEntry);
    hctl.hcxt      = TopMemoryContext;
    new_map = hash_create("shard replay loc_map", SHARD_FILESET_MAX_RELS,
                          &hctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

    ctx->nlocal = 0;
    for (i = 0; i < (uint32) lm.npairs; i++)
    {
        LocMapEntry *e;
        bool         found;
        int          j;

        e = (LocMapEntry *) hash_search(new_map, &lm.pairs[i].leader_loc,
                                        HASH_ENTER, &found);
        e->local_loc = lm.pairs[i].local_loc;
        e->role      = lm.pairs[i].role;
        e->ord       = lm.pairs[i].ord;
        e->reserved  = 0;

        for (j = 0; j < ctx->nlocal; j++)
            if (RelFileLocatorEquals(ctx->local_locs[j], lm.pairs[i].local_loc))
                break;
        if (j == ctx->nlocal && ctx->nlocal < SHARD_FILESET_MAX_RELS)
            ctx->local_locs[ctx->nlocal++] = lm.pairs[i].local_loc;
    }

    hash_destroy(ctx->loc_map);
    ctx->loc_map = new_map;

    /* 持久化 + 刷新槽位（豁免钩子的数据源）*/
    ShardReplayWriteLocMap(&lm);
    ctx->base_part_lsn = lm.base_part_lsn;      /* T6.2：与文件保持一致 */
    ReplaySlotRefreshLocs(ctx->shard_oid);

    ereport(LOG,
            (errmsg("shard replay: shard %u @plsn %llu 已应用 FILESET_UPDATE"
                    "（%u 个成员）", ctx->shard_oid,
                    (unsigned long long) hdr->partition_lsn, upd->nrels)));
    return true;
}

/* ================================================================== */
/* 段文件索引（每轮 catch-up 重建）                                    */
/* ================================================================== */

typedef struct ParwalIndexEnt
{
    uint64  plsn;
    int     file_idx;
    off_t   off;            /* 记录头起始偏移 */
    PartWALRecord hdr;
} ParwalIndexEnt;

typedef struct ParwalIndex
{
    int             nfiles;
    char            files[256][MAXPGPATH];
    int             nents;
    int             cap;
    ParwalIndexEnt *ents;
} ParwalIndex;

static int
ParwalEntCmp(const void *a, const void *b)
{
    uint64 pa = ((const ParwalIndexEnt *) a)->plsn;
    uint64 pb = ((const ParwalIndexEnt *) b)->plsn;

    if (pa < pb) return -1;
    if (pa > pb) return 1;
    return 0;
}

/*
 * BuildParwalIndex — 扫描 pg_parwal/<shard>/ 全部段文件，
 * 收集 plsn ∈ (from_plsn, +∞) 的记录位置，按 plsn 排序。
 */
static void
BuildParwalIndex(ShardReplayCtx *ctx, uint64 from_plsn, ParwalIndex *idx)
{
    char           dirpath[MAXPGPATH];
    DIR           *dir;
    struct dirent *de;
    int            i, j;

    idx->nfiles = 0;
    idx->nents  = 0;
    idx->cap    = 1024;
    idx->ents   = palloc(idx->cap * sizeof(ParwalIndexEnt));

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, ctx->shard_oid);

    dir = AllocateDir(dirpath);
    if (dir == NULL)
        return;

    while ((de = ReadDir(dir, dirpath)) != NULL && idx->nfiles < 256)
    {
        if (IsXLogFileName(de->d_name))
            strlcpy(idx->files[idx->nfiles++], de->d_name, MAXPGPATH);
    }
    FreeDir(dir);

    /* 文件名升序 */
    for (i = 0; i < idx->nfiles - 1; i++)
        for (j = i + 1; j < idx->nfiles; j++)
            if (strcmp(idx->files[i], idx->files[j]) > 0)
            {
                char tmp[MAXPGPATH];

                strlcpy(tmp,           idx->files[i], MAXPGPATH);
                strlcpy(idx->files[i], idx->files[j], MAXPGPATH);
                strlcpy(idx->files[j], tmp,           MAXPGPATH);
            }

    for (i = 0; i < idx->nfiles; i++)
    {
        char          filepath[MAXPGPATH];
        int           fd;
        PartWALRecord rec;
        ssize_t       nb;
        off_t         off = 0;

        snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, idx->files[i]);
        fd = OpenTransientFile(filepath, O_RDONLY | PG_BINARY);
        if (fd < 0)
            continue;

        while ((nb = read(fd, &rec, sizeof(PartWALRecord)))
               == (ssize_t) sizeof(PartWALRecord))
        {
            if (rec.magic != PARTWAL_MAGIC ||
                rec.partition_id != ctx->shard_oid)
                break;          /* 尾部残记录/异物：本文件到此为止 */

            if (rec.partition_lsn > from_plsn)
            {
                if (idx->nents >= idx->cap)
                {
                    idx->cap *= 2;
                    idx->ents = repalloc(idx->ents,
                                         idx->cap * sizeof(ParwalIndexEnt));
                }
                idx->ents[idx->nents].plsn     = rec.partition_lsn;
                idx->ents[idx->nents].file_idx = i;
                idx->ents[idx->nents].off      = off;
                idx->ents[idx->nents].hdr      = rec;
                idx->nents++;
            }

            off += (off_t) (sizeof(PartWALRecord) + rec.data_len);
            if (rec.data_len > 0 &&
                lseek(fd, off, SEEK_SET) != off)
                break;
        }

        CloseTransientFile(fd);
    }

    if (idx->nents > 1)
        qsort(idx->ents, idx->nents, sizeof(ParwalIndexEnt), ParwalEntCmp);
}

/* ================================================================== */
/* apply checkpoint（FRD §8.4 推进协议）                               */
/* ================================================================== */

/*
 * 把待发布的冻结账目搬进共享内存槽位（§13 约束 5，D2）。
 *
 * 这里**只做内存搬运，绝不碰 catalog** —— replay worker 没有选数据库，
 * 读 pg_class 会当场 FATAL 并把 worker 拖进崩溃重启循环（见
 * ShardReplayCtx.pending_freeze 的说明）。真正的写入由 replay_catchup()
 * 的调用方在普通 backend 里完成。
 */
static void
ShardReplayPublishPendingFreeze(ShardReplayCtx *ctx)
{
    int i;

    if (ctx->pending_freeze_n == 0 && !ctx->pending_vacuum_wm &&
        ctx->pending_alloc_wm == 0)
        return;

    LWLockAcquire(ReplayCtl->lock, LW_EXCLUSIVE);
    for (i = 0; i < REPLAY_MAX_SHARDS; i++)
    {
        ReplayShardSlot *s = &ReplayCtl->slots[i];

        if (s->shard_oid != ctx->shard_oid)
            continue;

        if (ctx->pending_freeze_n > 0)
        {
            memcpy(s->freeze, ctx->pending_freeze,
                   (size_t) ctx->pending_freeze_n * sizeof(PartWALFreezeEntry));
            s->freeze_n = ctx->pending_freeze_n;
        }
        if (ctx->pending_vacuum_wm)
        {
            s->vacuum_trunc_before = ctx->pending_trunc_before;
            s->vacuum_xid          = ctx->pending_vacuum_xid;
            s->vacuum_wm_valid     = true;
        }
        if (ctx->pending_alloc_wm > s->alloc_wm)
            s->alloc_wm = ctx->pending_alloc_wm;
        break;
    }
    LWLockRelease(ReplayCtl->lock);

    REPLAY_TRACE("TRACE freeze: shard %u 发布 %d 条冻结账目 + vacuum 水位 %d",
                 ctx->shard_oid, ctx->pending_freeze_n,
                 ctx->pending_vacuum_wm ? 1 : 0);
    ctx->pending_freeze_n = 0;
    ctx->pending_vacuum_wm = false;
    ctx->pending_alloc_wm = 0;
}

void
ShardReplayDoCheckpoint(ShardReplayCtx *ctx)
{
    ShardApplyCheckpoint chk;
    XidMapEntry         *ents = NULL;
    uint32               nents = 0;
    int                  i;

    if (ctx->applied_part_lsn == ctx->durable_part_lsn)
        return;                 /* 没有新进展 */

    /*
     * 推进协议（§8.4）：
     *   1. 把本 shard 副本文件的全部脏页刷盘（FlushBuffer 经补丁 0002
     *      豁免 XLogFlush(leader LSN)）；
     *   2. smgrimmedsync 落到持久层；
     *   3. 之后才允许写 durable_part_lsn 的 checkpoint 文件。
     */
    REPLAY_TRACE("TRACE ckpt: begin shard %u upto %llu", ctx->shard_oid,
                 (unsigned long long) ctx->applied_part_lsn);

    for (i = 0; i < ctx->nlocal; i++)
    {
        SMgrRelation reln = smgropen(ctx->local_locs[i], InvalidBackendId);
        ForkNumber   fork;

        REPLAY_TRACE("TRACE ckpt: flush rel %u/%u/%u",
                     ctx->local_locs[i].spcOid, ctx->local_locs[i].dbOid,
                     ctx->local_locs[i].relNumber);
        FlushRelationsAllBuffers(&reln, 1);
        for (fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++)
            if (smgrexists(reln, fork))
                smgrimmedsync(reln, fork);
    }

    /*
     * §8.4 推进协议第 2 步的后半：fsync 本批标记写进增强型 CLOG 的判决。
     *
     * 顺序不能反 —— checkpoint 文件一旦落盘就宣告"该游标之前的效果都已持久化"。
     * 若判决还留在 page cache 里就先写了游标，崩溃后会重放不到那批 MARKER
     * （游标已经跨过去了），于是页面上有元组、gclog 里没有判决：一笔永久未决的
     * 事务，既不可见也不会被回收。
     */
    EnhancedClogSync();

    REPLAY_TRACE("TRACE ckpt: flush done, writing cursor");

    /*
     * 在写游标之前把本批回放引入的 xid 拉齐到本地 nextXid（§7.5）。
     * 顺序不能反：checkpoint 一旦落盘就宣告"该游标之前的效果都已持久化"，
     * 而 nextXid 落后于回放水位本身就是一种未完成的效果。
     */
    if (TransactionIdIsNormal(XidFromFullTransactionId(ctx->max_replayed_fxid)))
        PartDistAdvanceNextXidPastXid(
            XidFromFullTransactionId(ctx->max_replayed_fxid));

    /*
     * 冻结账目发布到槽位（§13 约束 5）。放在写游标之前，是为了让"游标之前
     * 的效果都已就绪"这句话对它也成立 —— 尽管真正落 pg_class 的是
     * replay_catchup 的调用方。
     *
     * 已知的**尽力而为**边界：槽位在共享内存里，节点崩溃即丢，而 leader 侧
     * 的基线已记为"已发"、不会重发。后果只是这一轮账目滞后 —— leader 下次
     * vacuum 推进 relfrozenxid 时会再发一条补上。对一个以千万 xid 为尺度
     * 变化的账目字段，这个语义够用；要精确一次得把它也放进 apply_checkpoint
     * 快照并动 checkpoint 格式版本，不值当。
     */
    ShardReplayPublishPendingFreeze(ctx);

    /* xid_map 快照：与头同在一个 tmp 文件里原子落盘（§8.4） */
    nents = (ctx->xid_map != NULL)
            ? (uint32) hash_get_num_entries(ctx->xid_map) : 0;
    if (nents > 0)
    {
        HASH_SEQ_STATUS seq;
        XidMapEntry    *e;
        uint32          k = 0;

        ents = (XidMapEntry *) palloc(nents * sizeof(XidMapEntry));
        hash_seq_init(&seq, ctx->xid_map);
        while ((e = (XidMapEntry *) hash_seq_search(&seq)) != NULL)
        {
            if (k >= nents)     /* 理论不可达：本函数期间没有并发写入 */
            {
                hash_seq_term(&seq);
                break;
            }
            ents[k++] = *e;
        }
        nents = k;
    }

    memset(&chk, 0, sizeof(chk));
    chk.magic            = APPLY_CHECKPOINT_MAGIC;
    chk.version          = APPLY_CHECKPOINT_VERSION;
    chk.shard_oid        = ctx->shard_oid;
    chk.durable_part_lsn = ctx->applied_part_lsn;
    chk.max_orig_lsn     = ctx->max_orig_lsn;
    chk.max_replayed_fxid = U64FromFullTransactionId(ctx->max_replayed_fxid);
    chk.resume_segno     = ctx->seg_segno;
    chk.resume_offset    = (uint64) ctx->seg_off;
    chk.nxidmap          = nents;

    WriteApplyCheckpoint(&chk, ents);

    if (ents != NULL)
        pfree(ents);

    REPLAY_TRACE("TRACE ckpt: shard %u 游标=%llu 水位fxid=%llu xid_map=%u 条",
                 ctx->shard_oid,
                 (unsigned long long) chk.durable_part_lsn,
                 (unsigned long long) chk.max_replayed_fxid, nents);

    ctx->durable_part_lsn   = ctx->applied_part_lsn;
    ctx->records_since_ckpt = 0;
    ctx->last_ckpt_time     = GetCurrentTimestamp();
}

/* ================================================================== */
/* 回放主循环（阶段二~四）                                             */
/* ================================================================== */

void
ShardReplayRun(ShardReplayCtx *ctx, uint64 bound)
{
    ParwalIndex idx;
    int         pos = 0;
    char       *body = NULL;
    uint32      body_cap = 0;
    int         cur_fd = -1;
    int         cur_file = -1;
    char        dirpath[MAXPGPATH];

    if (bound <= ctx->applied_part_lsn)
        return;

    /*
     * 每轮开工先清栅栏标记：这一轮可能正是运维补完结构后重新触发的，
     * 不清的话即使 CTRL 应用成功，收尾仍会把槽位落回 NEEDS_STRUCT。
     */
    ctx->needs_struct     = false;
    ctx->struct_errmsg[0] = '\0';

    REPLAY_TRACE("TRACE Run: shard %u applied=%llu bound=%llu",
                 ctx->shard_oid,
                 (unsigned long long) ctx->applied_part_lsn,
                 (unsigned long long) bound);

    snprintf(dirpath, MAXPGPATH, "%s/%s/%u",
             DataDir, PARTITION_WAL_DIR, ctx->shard_oid);

    BuildParwalIndex(ctx, ctx->applied_part_lsn, &idx);

    REPLAY_TRACE("TRACE Run: 索引 %d 条 (files=%d)", idx.nents, idx.nfiles);

    while (ctx->applied_part_lsn < bound)
    {
        uint64          expected = ctx->applied_part_lsn + 1;
        ParwalIndexEnt *ent = NULL;

        /* 有序索引上顺序推进；跳过重复 plsn（重传去重的残留） */
        while (pos < idx.nents && idx.ents[pos].plsn < expected)
            pos++;
        if (pos < idx.nents && idx.ents[pos].plsn == expected)
            ent = &idx.ents[pos];

        if (ent == NULL)
        {
            /*
             * 空洞判定（§7.3）：期望的 plsn 缺失。若其后还有更大的 plsn
             * 已落盘 → 流内空洞，物理回放不允许 → ERROR（fail-fast，
             * worker 重启后重试）。否则 = 尾部尚未到达，正常返回等待。
             */
            if (pos < idx.nents)
                ereport(ERROR,
                        (errmsg("shard replay: shard %u 流内空洞：期望 plsn "
                                "%llu，下一条已存在的是 %llu",
                                ctx->shard_oid,
                                (unsigned long long) expected,
                                (unsigned long long) idx.ents[pos].plsn)));
            break;
        }

        /* 读记录体 */
        if (cur_file != ent->file_idx)
        {
            char filepath[MAXPGPATH];

            if (cur_fd >= 0)
                CloseTransientFile(cur_fd);
            snprintf(filepath, MAXPGPATH, "%s/%s",
                     dirpath, idx.files[ent->file_idx]);
            cur_fd = OpenTransientFile(filepath, O_RDONLY | PG_BINARY);
            if (cur_fd < 0)
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("shard replay: 无法打开段文件 \"%s\": %m",
                                filepath)));
            cur_file = ent->file_idx;
        }

        if (ent->hdr.data_len > 0)
        {
            ssize_t nb;

            if (ent->hdr.data_len > body_cap)
            {
                if (body != NULL)
                    pfree(body);
                body_cap = Max(ent->hdr.data_len, (uint32) 65536);
                body     = palloc(body_cap);
            }

            nb = pg_pread(cur_fd, body, ent->hdr.data_len,
                          ent->off + (off_t) sizeof(PartWALRecord));
            if (nb != (ssize_t) ent->hdr.data_len)
                ereport(ERROR,
                        (errcode_for_file_access(),
                         errmsg("shard replay: shard %u @plsn %llu 记录体读取"
                                "不完整 (%zd/%u)", ctx->shard_oid,
                                (unsigned long long) expected,
                                nb, ent->hdr.data_len)));

            REPLAY_TRACE("TRACE apply: plsn=%llu rmid=%u info=0x%02X len=%u "
                         "flags=0x%02X",
                         (unsigned long long) expected,
                         ent->hdr.rmid, ent->hdr.info, ent->hdr.data_len,
                         ent->hdr.flags);

            /*
             * 阶段三的三路派发（FRD §7.4/§7.6/§7.7）。判据是 flags 的类别位，
             * 不是 rmid —— MARKER 的 rmid 也是 RM_XACT_ID，但载荷是
             * TxnMarkerPayload 而不是 XLogRecord，走 redo 会当场解码失败。
             * 2.0 段流的 flags 恒为 0，PartWALRecordIsData 把它们归入 DATA。
             */
            if (PartWALRecordIsMarker(&ent->hdr))
                ApplyMarkerRecord(ctx, &ent->hdr, body);
            else if (PartWALRecordIsCtrl(&ent->hdr))
            {
                /*
                 * 结构栅栏：游标停在这条 CTRL **之前**。不能推进 ——
                 * 推进了就等于宣称这条控制记录已生效，而它其实没有。
                 */
                if (!ApplyCtrlRecord(ctx, &ent->hdr, body))
                    break;
            }
            else if (PartWALRecordIsDtx(&ent->hdr))
            {
                /*
                 * DTX 记录（DTX_2PC_DESIGN.md §5.5）：载荷是 DtxRecord，不是
                 * XLogRecord，绝不能进 rm_redo。它服务于升主时的 in-doubt
                 * 闭合（恢复守护按需回读段文件），对物理回放只推进游标。
                 * 事务号水位无需在此登记：同一事务的 DATA 记录带着相同
                 * gxid，已在数据路径登记过。
                 */
                REPLAY_TRACE("TRACE dtx skip: plsn=%llu kind=%u",
                             (unsigned long long) expected, ent->hdr.info);
            }
            else
                ApplyDataRecord(ctx, &ent->hdr, body);

            REPLAY_TRACE("TRACE apply done: plsn=%llu",
                         (unsigned long long) expected);
        }
        else
        {
            /*
             * data_len == 0：旧 group-commit 缺陷产生的占位记录（捕获侧
             * 已修复，新流不再出现）。物理内容缺失，只能跳过并告警 ——
             * 这类历史流不满足 R1 验收，仅为兼容旧目录不至于卡死。
             */
            ereport(WARNING,
                    (errmsg("shard replay: shard %u @plsn %llu data_len=0，"
                            "无字节可回放（旧流占位记录），跳过",
                            ctx->shard_oid,
                            (unsigned long long) expected)));
        }

        /* 阶段四：游标推进（内存值） */
        ctx->applied_part_lsn = expected;
        if (ent->hdr.orig_lsn > ctx->max_orig_lsn)
            ctx->max_orig_lsn = ent->hdr.orig_lsn;
        ctx->seg_segno = (uint64) ent->file_idx;
        ctx->seg_off   = ent->off;
        ctx->records_since_ckpt++;
        pos++;

        if (ctx->records_since_ckpt >= (uint64) replay_checkpoint_records)
            ShardReplayDoCheckpoint(ctx);
    }

    if (cur_fd >= 0)
        CloseTransientFile(cur_fd);
    if (body != NULL)
        pfree(body);
    pfree(idx.ents);

    /* 每轮收尾：落一次 checkpoint（若有进展） */
    ShardReplayDoCheckpoint(ctx);

    /*
     * §13.3 审计：追平后不应残留 invalid page（引用了未来才创建的页等）。
     * 有残留说明流不完整或 fileset 漏登记。
     */
    if (XLogHaveInvalidPages())
        ereport(ERROR,
                (errmsg("shard replay: shard %u 追平后仍有 invalid pages，"
                        "流不完整或 fileset 漏登记", ctx->shard_oid)));
}
