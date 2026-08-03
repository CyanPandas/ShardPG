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

#include "access/heapam_xlog.h"
#include "access/nbtxlog.h"
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
ShardReplayLoadLocMap(ShardReplayCtx *ctx)
{
    char             path[MAXPGPATH];
    int              fd;
    ssize_t          nb;
    ReplayLocMapFile lm;
    HASHCTL          hctl;
    int              i;

    snprintf(path, MAXPGPATH, "%s/%s/%u/%s",
             DataDir, PARTITION_WAL_DIR, ctx->shard_oid,
             REPLAY_LOCMAP_FILENAME);

    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
        return false;

    nb = read(fd, &lm, sizeof(lm));
    CloseTransientFile(fd);

    if (nb != (ssize_t) sizeof(lm) ||
        lm.magic != REPLAY_LOCMAP_MAGIC ||
        lm.shard_oid != ctx->shard_oid ||
        lm.npairs < 1 || lm.npairs > SHARD_FILESET_MAX_RELS)
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
    for (i = 0; i < lm.npairs; i++)
    {
        LocMapEntry *e;
        bool         found;
        int          j;

        e = (LocMapEntry *)
            hash_search(ctx->loc_map, &lm.pairs[i].leader_loc,
                        HASH_ENTER, &found);
        e->local_loc = lm.pairs[i].local_loc;

        /* 收集去重后的本地文件号（checkpoint 刷脏用） */
        for (j = 0; j < ctx->nlocal; j++)
            if (RelFileLocatorEquals(ctx->local_locs[j], lm.pairs[i].local_loc))
                break;
        if (j == ctx->nlocal && ctx->nlocal < SHARD_FILESET_MAX_RELS)
            ctx->local_locs[ctx->nlocal++] = lm.pairs[i].local_loc;
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
 * 逐行对照 varsup.c 的 AdvanceNextFullTransactionIdPastXid()，只有一处不同：
 * 核内那个版本无锁读 nextXid（带 Assert(AmStartupProcess() || !IsUnderPostmaster)
 * 声明这只对 startup 进程安全），replay worker 是普通 bgworker，assert 构建下
 * 直接崩。这里读-改-写全程持 XidGenLock。
 */
void
PartDistAdvanceNextXidPastXid(TransactionId xid)
{
    FullTransactionId newNextFullXid;
    TransactionId     next_xid;
    uint32            epoch;

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

    if (hdr->version < PARTWAL_RECORD_VERSION_3)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu MARKER 记录出现在 "
                        "version=%u 的流里（2.0 无事务层语义）",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->version)));

    if (hdr->data_len < sizeof(TxnMarkerPayload) ||
        hdr->data_len != (uint32) TxnMarkerPayloadSize(m->nsubxacts))
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu MARKER 载荷长度不符"
                        "（data_len=%u，nsubxacts=%u 需要 %zu）",
                        ctx->shard_oid,
                        (unsigned long long) hdr->partition_lsn,
                        hdr->data_len, m->nsubxacts,
                        TxnMarkerPayloadSize(m->nsubxacts))));

    origin   = GxidNodeId(gxid);
    op       = hdr->info & XLOG_XACT_OPMASK;
    subxacts = TxnMarkerSubxacts(m);

    if (op != XLOG_XACT_COMMIT && op != XLOG_XACT_ABORT)
        ereport(ERROR,
                (errmsg("shard replay: shard %u @plsn %llu MARKER 的 info=0x%02X "
                        "既不是 COMMIT 也不是 ABORT",
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
    if (op == XLOG_XACT_COMMIT)
    {
        EnhancedClogWriteStatus(gxid, m->start_ts, m->commit_ts, TXN_COMMITTED);
        for (i = 0; i < m->nsubxacts; i++)
            EnhancedClogWriteStatus(MakeGlobalXid(origin, subxacts[i]),
                                    m->start_ts, m->commit_ts, TXN_COMMITTED);
    }
    else
        EnhancedClogWriteStatus(gxid, m->start_ts, 0, TXN_ABORTED);
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
                ereport(ERROR,
                        (errmsg("shard replay: shard %u @plsn %llu 收到 CTRL "
                                "记录，但控制记录尚未实现（FRD §7.7/§12）",
                                ctx->shard_oid,
                                (unsigned long long) expected)));
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
