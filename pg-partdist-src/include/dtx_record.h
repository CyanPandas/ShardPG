/*
 * dtx_record.h
 *
 * 跨分区事务（DTX-2PC）在 pg_parwal 流里的记录载荷定义。
 * 设计依据：pg-partdist-src/docs/DTX_2PC_DESIGN.md §5。
 *
 * 这类记录复用 parwal-2.0 的 40 字节 PartWALRecord 头，靠头部 flags 里的
 * PARTWAL_FLAG_DTX 与数据记录区分；子类型放在头部 info 字段里
 * （DtxRecordKind），载荷是下面的 DtxRecordPayload。
 *
 * 头字段的取值约定（与 §5.1 一致，实现必须遵守）：
 *   rmid          = RM_XACT_ID —— 只为人眼/工具可读，回放侧**不据此分派**
 *   info          = DtxRecordKind，**不是** XLog info
 *   flags         = PARTWAL_FLAG_DTX
 *   orig_lsn      = InvalidXLogRecPtr(0) —— DTX 记录不是 WAL 记录，没有
 *                   leader 侧 end LSN；回放侧**禁止**拿它盖页 LSN
 *                   （它本来就不进 rm_redo，按 flags 在分派处即被路由走）
 *   xid           = 该分区上本事务的本地 top-level xid；无则 0
 *   partition_lsn = 与 DATA 记录同一序号空间，正常递增
 *   data_len      = sizeof(DtxRecordPayload) + 8 * nparticipants
 */
#ifndef DTX_RECORD_H
#define DTX_RECORD_H

#include "postgres.h"

typedef enum DtxRecordKind
{
    DTX_PREPARE  = 1,   /* 参与组：本事务在本组已 prepared          */
    DTX_DECISION = 2,   /* 协调组：全局决议（verdict 见下）         */
    DTX_COMMIT   = 3,   /* 参与组：提交标记                         */
    DTX_ABORT    = 4    /* 参与组：中止标记                         */
} DtxRecordKind;

#define DTX_VERDICT_COMMIT  UINT32_C(1)
#define DTX_VERDICT_ABORT   UINT32_C(2)

/*
 * 定长部分 32 字节；participants[] 紧随其后，仅 DECISION 记录携带。
 * PREPARE 记录**不带**参与者清单——那正是 §2.2 用推定中止换掉"协调者续跑"
 * 所省掉的东西。
 */
typedef struct DtxRecordPayload
{
    uint64  dtxid;            /* 全局事务号                                    */
    int64   coord_gsid;       /* 协调组 global_shard_id；DECISION 中 = 自身    */
    uint64  commit_ts;        /* 提交时间戳；PREPARE/ABORT 为 0                */
    uint32  verdict;          /* 仅 DECISION 有效：DTX_VERDICT_*               */
    uint32  nparticipants;    /* 仅 DECISION 有效；其余为 0                    */
    /* int64 participants[nparticipants] 紧随其后：参与组 global_shard_id 升序 */
} DtxRecordPayload;

#define DtxPayloadSize(nparts) \
    (sizeof(DtxRecordPayload) + sizeof(int64) * (Size) (nparts))

#define DtxPayloadParticipants(p) \
    ((int64 *) ((char *) (p) + sizeof(DtxRecordPayload)))

static inline bool
DtxRecordKindIsValid(int kind)
{
    return kind >= DTX_PREPARE && kind <= DTX_ABORT;
}

#endif /* DTX_RECORD_H */
