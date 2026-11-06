/*
 * shard_stamp.h  (pg_partdist patch 0005)
 *
 * Hook that lets an extension supply per-shard tuple xids for shard-managed
 * relations.  Contract:
 *   - returns false if the relation is not shard-managed (caller uses the
 *     native xid as usual);
 *   - returns true if it is; when assign==true, *sxid is set to the shard
 *     xid bound to the current top-level transaction for this relation's
 *     shard (allocated lazily on first use; the hook must ereport(ERROR)
 *     for cases it does not support, e.g. subtransactions).
 * The WAL record header xid always stays native; insert/multi_insert/update
 * records carry the shard xid as the LAST sizeof(TransactionId) bytes of
 * their main data, flagged by XLH_INSERT_SHARD_XID / XLH_UPDATE_SHARD_XID.
 * delete/lock need no extra payload: their xmax already travels in the
 * record body.
 */
#ifndef SHARD_STAMP_H
#define SHARD_STAMP_H

struct RelationData;

typedef bool (*shard_relation_xid_hook_type) (struct RelationData *relation,
											  bool assign,
											  TransactionId *sxid);
extern PGDLLIMPORT shard_relation_xid_hook_type shard_relation_xid_hook;


/*
 * patch 0006: extension-supplied tuple visibility for shard-managed
 * relations.
 *
 * Shard tuples carry per-shard xids in xmin/xmax, so every native
 * clog/procarray consultation would misjudge them; each Satisfies* entry
 * point forks on these hooks before touching native state.  Contract:
 *   - is_shard_rel must be cheap (called on visibility hot paths) and must
 *     not touch catalogs;
 *   - each satisfies_* returns true if it decided (*visible / *result
 *     filled in), false = not shard-managed, caller continues natively;
 *   - implementations must neither read nor write hint bits;
 *   - xmax_wait translates a shard xmax to its holder's native xid and
 *     blocks until that transaction ends; the caller then re-evaluates
 *     from scratch (heap_delete/heap_update goto l1/l2).
 * HeapTupleSatisfiesToast is deliberately NOT forked (it consults no
 * clog/procarray for normal tuples), but toast DELETION does reach
 * HeapTupleSatisfiesUpdate -- the extension's is_shard_rel therefore
 * must recognize toast relations of shard tables (it keeps a
 * backend-local toast->owner map fed by the stamping hook, which always
 * runs first within heap_delete).
 */
#include "access/htup.h"
#include "access/xlogdefs.h"
#include "access/tableam.h"
#include "storage/buf.h"
#include "storage/itemptr.h"
#include "storage/lmgr.h"
#include "utils/snapshot.h"

typedef struct ShardVisibilityHooks
{
	bool		(*is_shard_rel) (Oid tableOid);
	bool		(*satisfies_mvcc) (HeapTuple htup, Snapshot snapshot,
								   Buffer buffer, bool *visible);
	bool		(*satisfies_self) (HeapTuple htup, Buffer buffer,
								   bool *visible);
	bool		(*satisfies_dirty) (HeapTuple htup, Snapshot snapshot,
									Buffer buffer, bool *visible);
	bool		(*satisfies_update) (HeapTuple htup, CommandId curcid,
									 Buffer buffer, TM_Result *result);
	void		(*xmax_wait) (struct RelationData *relation,
							  TransactionId sxid,
							  ItemPointer ctid, XLTW_Oper oper);
} ShardVisibilityHooks;

extern PGDLLIMPORT const ShardVisibilityHooks *shard_visibility_hooks;

/* ---- 0007：commit/abort 记录体分片 xid 列表 ---- */

/*
 * 收集本事务的分片 xid 对写进 commit/abort 记录体。返回对数，*pairs 指向
 * 2n 个 uint32（shard,sxid 交错）的静态缓冲；*commit_ts 填 TSO 提交时间戳
 * （PRE_COMMIT 已暂存的值——**本钩子在临界区内被调**：实现不得 palloc /
 * ereport(ERROR) / 做任何 RPC，只拷内存；abort 路径暂存缺席自然为 0）。
 */
typedef int (*shard_xact_wal_list_hook_type) (uint32 **pairs,
											  uint64 *commit_ts);
extern PGDLLIMPORT shard_xact_wal_list_hook_type shard_xact_wal_list_hook;

/*
 * 崩溃恢复：把 commit/abort 记录体里的对列表交扩展重做分片 clog 落账
 * （幂等；在 startup 进程里执行）。commit_ts 来自记录体（abort 为 0）。
 */
typedef void (*shard_xact_redo_hook_type) (int nxids, const uint32 *pairs,
										   uint64 commit_ts, bool committed);
extern PGDLLIMPORT shard_xact_redo_hook_type shard_xact_redo_hook;

/* ---- 0008：vacuum 类读判定（T2.6，ANALYZE 读侧） ---- */

/*
 * 分片元组的 HeapTupleSatisfiesVacuumHorizon 裁决。返回 true 表示已
 * 裁决（*res 有效）；返回 false 走原生路径（非分片表）。约定：
 *   - 只允许产出 LIVE / INSERT_IN_PROGRESS / DELETE_IN_PROGRESS /
 *     RECENTLY_DEAD / DEAD（中止插入）——"只判不收"：RECENTLY_DEAD 由
 *     内核分叉点配新鲜原生 xid 作 dead_after，一切提升检查落保守分支；
 *   - 钩子未装而元组是分片的 ⇒ 分叉点 fail-closed ERROR（0006 语义）。
 */
typedef bool (*shard_vacuum_read_hook_type) (HeapTuple htup, Buffer buffer,
											 int *res);
extern PGDLLIMPORT shard_vacuum_read_hook_type shard_vacuum_read_hook;

/* ---- 0009：分片 2PC 段（twophase rmgr 槽位经钩子分发） ---- */

/*
 * PREPARE 状态文件里的分片段：{gxid, start_ts, (shard,sxid) 对}。
 * recover 在崩溃恢复重建 PREPARED 落账；postabort 写 ABORTED 判决；
 * postcommit 不写终局（判决与 commit_ts 走协调者决议广播，§3.1 步骤 ⑦）。
 */
typedef void (*shard_twophase_cb_type) (TransactionId xid, uint16 info,
										void *recdata, uint32 len);
extern PGDLLIMPORT shard_twophase_cb_type shard_twophase_recover_hook;
extern PGDLLIMPORT shard_twophase_cb_type shard_twophase_postcommit_hook;
extern PGDLLIMPORT shard_twophase_cb_type shard_twophase_postabort_hook;

/*
 * PrepareTransaction 的记录注册点（StartPrepare 之后、EndPrepare 之前，
 * AtPrepare_* 同位置）——PRE_PREPARE 回调先于 StartPrepare，在那里
 * RegisterTwoPhaseRecord 会被 StartPrepare 重置吞掉。
 */
typedef void (*shard_at_prepare_hook_type) (void);
extern PGDLLIMPORT shard_at_prepare_hook_type shard_at_prepare_hook;

/* ---- 0012：READ COMMITTED 下的更新链追随（EPQ 重评） ---- */

/*
 * 默认 false：分片表只提供快照隔离，目标行在快照之后被并发改过 ⇒ 40001
 * （first-committer-wins，设计 §4.4）。扩展可把它打开（GUC
 * pg_partdist.rc_update_recheck）：此时 READ COMMITTED 事务的 UPDATE/DELETE/
 * SELECT FOR UPDATE 与原生 PG 一样顺更新链锁住最新版本、经 EvalPlanQual
 * 重评条件后再改；读仍用事务快照。可见性分叉点（HeapTupleSatisfiesUpdate 钩子）
 * 与本标志同进同退。
 */
extern PGDLLIMPORT bool shard_rc_follow_updates;

/* ---- 0014：提前释放锁（early lock release） ---- */

/*
 * 打开后（扩展 GUC pg_partdist.early_lock_release），同步提交的事务在
 * RecordTransactionCommit 里只插入提交记录、不等刷盘（clog 按异步提交标记，
 * 提示位等 WAL 落盘才设），照常放锁、清 procarray；到 CommitTransaction 末尾、
 * 回复客户端之前才 XLogFlush 到提交记录 —— 客户端收到的提交仍然持久。
 * 热行上的后继写者不必等前者的 fsync：它自己的提交记录排在后面，刷盘必然连带。
 *
 * shard_elr_pending_lsn：本次提交走了提前放锁时为提交记录末尾（扩展的
 *   XACT_EVENT_COMMIT 回调据此推迟写分片判决），否则 Invalid。
 * shard_elr_post_flush_hook：刷盘完成后调用（事务已结束，不得访问目录）。
 */
extern PGDLLIMPORT bool shard_early_lock_release;
/*
 * 本次提交不得提前放锁（pre_record_commit 钩子置位，RecordTransactionCommit 用后清零）。
 * 2PC 驱动者必须先刷本地提交再让 Citus 发 COMMIT PREPARED：否则崩溃后远端已提交、
 * 本地那半却丢了。
 */
extern PGDLLIMPORT bool shard_elr_inhibit;
extern PGDLLIMPORT XLogRecPtr shard_elr_pending_lsn;
typedef void (*shard_elr_post_flush_hook_type) (void);
extern PGDLLIMPORT shard_elr_post_flush_hook_type shard_elr_post_flush_hook;

#endif							/* SHARD_STAMP_H */
