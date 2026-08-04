/*
 * dtx_participant.h
 *
 * DTX-2PC 参与者侧接线（worker 侧）。设计依据
 * pg-partdist-src/docs/DTX_2PC_DESIGN.md §3.3 / §5 / §9.3。
 *
 * 形态：Citus 在 master 的 XACT_EVENT_PRE_COMMIT 里对每个参与节点发
 * `PREPARE TRANSACTION 'citus_<group>_<pid>_<txnnum>_<conn>'`。
 * 本模块在 worker 的 ProcessUtility 里截下这条语句（此时触达集合还在），
 * 记住 gid；随后在 XACT_EVENT_PRE_PREPARE 里：
 *
 *   1) PartWALFlush()  —— DATA 落盘 fsync 并复制到多数派（[A] + quorum）
 *   2) 对每个触达分区追加一条 DTX_PREPARE 标记（携带本地 top-level xid）
 *   3) 再 PartWALFlush() 一次，把标记也推到多数派
 *   4) 把 (dtxid, gid, 触达组的 global_shard_id[]) **自治登记**到
 *      partdist.dtx_participant —— 这是 master 侧算写集、以及本节点崩溃后
 *      恢复守护找协调组的依据
 *
 * 之后 PG 才写 prepare 记录并 fsync（[B]），时序不变式 [A] < [B] 保持。
 *
 * 触达集合为空 ⇒ 本节点在这笔事务里**没有写任何纳管分区**（只读参与者），
 * 不登记、不写标记，master 侧的写集里自然不会有它（§8.3 只读参与者剔除）。
 */
#ifndef DTX_PARTICIPANT_H
#define DTX_PARTICIPANT_H

#include "postgres.h"

/*
 * 从 prepared 事务的 gid 推出全局事务号 dtxid。
 *
 * 支持两种 gid：
 *   citus_<group>_<pid>_<txnnum>_<conn>   —— Citus 原生（正常路径）
 *   shardpg_dtx_<dtxid>_<coord_gsid>      —— 本项目自造（机制测试用）
 *
 * Citus gid 的前三段 (group, pid, txnnum) 在同一笔分布式事务的**所有**参与者
 * 上完全相同（只有末段 conn 因连接而异），因此各节点独立解析得到同一个 dtxid，
 * 无需任何协商。打包方式见 dtx_participant.c 的注释。
 *
 * 解析不出返回 false。
 */
extern bool DtxidFromGid(const char *gid, int64 *dtxid);

/* ProcessUtility 里看到 PREPARE TRANSACTION 'gid' 时调用（执行前） */
extern void PartDistDtxNotePrepareGid(const char *gid);

/*
 * XACT_EVENT_PRE_PREPARE 回调里的两个挂点，**必须夹住** PartWALFlush()：
 *   Capture 在 flush 之前取触达集合快照（flush 末尾会把集合清空）；
 *   Finish  在 flush 之后追加 DTX_PREPARE 标记、再复制一轮、并自治登记。
 * 无待处理 gid 时两者都是空操作。
 */
extern void PartDistDtxPrePrepareCapture(void);
extern void PartDistDtxPrePrepareFinish(void);

/* GUC：整条 2PC 接线的总开关（默认 on） */
extern bool pg_partdist_dtx_2pc_enabled;
extern void PartDistDtxDefineGUCs(void);

/* 事务结束（COMMIT/ABORT/PREPARE 完成）时清状态 */
extern void PartDistDtxReset(void);

/*
 * 阶段 3（§3.3）：ProcessUtility 里看到 COMMIT/ROLLBACK PREPARED '<gid>' 时调用
 * （执行前），给本节点在这笔事务里写过的每个分区组补一条 DTX_COMMIT/DTX_ABORT
 * 标记。这是**零额外往返**的挂点 —— master 的阶段 3 本来就要发这条语句。
 *
 * 协调组不补 COMMIT 标记：它的 DECISION 记录语义上已经是（§5.3）。
 */
extern void PartDistDtxOnFinishPrepared(const char *gid, bool committed);

/*
 * 参与者自治登记挂钩。由 pg_raft 经 rendezvous variable
 * "partdist_dtx_note_participant_hook" 提供 —— 登记必须写在**独立事务**里
 * （本事务马上要进入 prepared，写在里面就永远看不见），而独立事务只能经
 * libpq 自连接完成，libpq 在本项目里由 pg_raft 持有。
 *
 * 返回 false 表示登记失败：调用方 ereport(ERROR) 中止事务 —— 宁可中止，
 * 也不能让 master 在写集不完整的情况下做决议。
 */
typedef bool (*DtxNoteParticipantHook) (int64 dtxid, const char *gid,
										const int64 *gsids, int ngsids);

#endif /* DTX_PARTICIPANT_H */
