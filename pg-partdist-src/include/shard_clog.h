/*
 * shard_clog.h
 *
 * 分片级 clog（pg_shard_clog）—— TX-TSO-MVCC 的持久事务状态账本
 * （设计 §5.3，P2 T2.1）。
 *
 * 与 pg_gclog（enhanced_clog.*，"每来源节点"一套）**共存、互不相干**：
 * 那套的键是 64 位 gxid（uint16 node_id 编码进目录名），服务既有回放/DTX
 * 基线；这套的键是 (Oid shard, 分片xid)——32 位 Oid 塞不进 node_id 的编码，
 * 参数化复用等于动基线磁盘格式（P2_PRECHECK 结论一），所以是独立新实例，
 * 只克隆它的 I/O 纪律。状态枚举 TxnStatus 两家共用同一套值。
 *
 * 布局：pg_shard_clog/<shard_oid>/<8 位十六进制段号>，段内扁平定长 32 字节
 * 槽数组，槽号 = 分片 xid 段内偏移（稠密发号 ⇒ xid 即行号，直接寻址）。
 * 文件稀疏；**全零槽 = TXN_RUNNING = 未决 = 不可见** —— 空洞读出来就是这个，
 * 没有判决的事务一律当作没提交，语义安全（正是 P1 临时提交表"缺席=已提交"
 * 崩溃漏判的反面，P2 换桩的意义所在）。
 *
 * 持久化契约（与 pg_gclog 的"延迟 fsync + apply checkpoint 收口"不同）：
 *
 *   - COMMITTED/ABORTED 判决写入**立即 fsync**。本目录对内核 checkpointer
 *     不可见：一旦 checkpoint 把 redo 点推过了提交记录，而判决还睡在
 *     page cache 里，崩溃后既没有 WAL 重做（0007 的 redo 钩子够不着 redo 点
 *     之前的记录）、判决又丢了 —— 槽回到 RUNNING，认领(T2.4)会把它改判
 *     ABORTED，已提交数据静默消失。每提交一次 fsync 是 P2 接受的代价
 *     （单分片事务恰好一次），优化留到性能专项。
 *   - RUNNING 落账**不 fsync**。空洞 = RUNNING，丢了等于没丢；且判决写的
 *     fsync 顺带把同段里先前的 RUNNING 字节也刷下去了。
 *   - 新建段文件时 fsync 分片目录一次（目录项持久化，durable_rename 同理）。
 *
 * 并发：不取锁。不同 sxid 写不同偏移；同一 sxid 的重复写（提交回调 + 0007
 * redo 重做）内容逐字节相同，幂等 —— 与 enhanced_clog.c 头注释的论证同构。
 *
 * 生命周期：DROP TABLE 分片打标表时，本目录与 pg_shard_xid/<oid> 水位文件
 * 在**事务提交时点**一并删除（回滚不删）——由 ShardXidUtilityGuard 侦测
 * DropStmt 登记、shard_xid 的 XactCallback 在 COMMIT/ABORT 时结算。
 * 已知边界（P2_PRECHECK 结论一）：
 *   - 经由级联（DROP SCHEMA ... CASCADE 等）的删除不经 DropStmt 表分支，
 *     文件成为孤儿 —— 无害占盘；OID 复用场景由 T2.7 注册函数建账前清目录兜底。
 *   - 目录按 OID 不分库（与 pg_shard_xid 同款单库假设，P1 已接受）。
 */
#ifndef SHARD_CLOG_H
#define SHARD_CLOG_H

#include "postgres.h"

#include "enhanced_clog.h"		/* TxnStatus */

/*
 * 槽 = 设计 §5.3 行结构五列原文。P2 只用 status：global_xid 恒 0（P4 2PC
 * 起用）、start_ts/commit_ts 占位（P3 TSO 起用）、parent_xid 恒 0（P4 子
 * 事务链起用）。现在就把磁盘格式定到位，后期只填值不迁移。
 */
typedef struct ShardClogSlot
{
	uint64		global_xid;
	uint64		start_ts;
	uint64		commit_ts;
	uint32		status;			/* TxnStatus */
	uint32		parent_xid;
} ShardClogSlot;

#define SHARD_CLOG_SLOT_SIZE		((uint32) sizeof(ShardClogSlot))

/* 每段容纳的 xid 数：1M 槽 × 32B = 32MB/段上限（稀疏，实占远小于此） */
#define SHARD_CLOG_XIDS_PER_SEGMENT	(UINT32_C(1) << 20)

#define SHARD_CLOG_DIR				"pg_shard_clog"

/* 首写本分片时落 RUNNING 账（写全零槽，只扩文件不 fsync） */
extern void ShardClogSetRunning(Oid shard, TransactionId sxid);

/* 写终局判决（COMMITTED / ABORTED），立即 fsync。幂等。 */
extern void ShardClogSetVerdict(Oid shard, TransactionId sxid, bool committed);

/* 读状态。空洞/段不存在 = TXN_RUNNING。 */
extern TxnStatus ShardClogReadStatus(Oid shard, TransactionId sxid);

/*
 * T2.4 认领原语：把 [from, to) 里所有 RUNNING（含全零洞）改判 ABORTED，
 * 已有终局判决与 PREPARED（P4 in-doubt，不许动）原样保留。每段一次 fsync。
 * 返回改判条数。幂等。
 */
extern int ShardClogClaimRange(Oid shard, TransactionId from, TransactionId to);

/*
 * 补丁 0007 的 redo 钩子实现：把 commit/abort 记录体里的 (shard,sxid) 对
 * 列表重做成判决（幂等）。在 startup 进程里跑，签名与
 * shard_xact_redo_hook_type 一致。
 */
extern void ShardClogXactRedo(int nxids, const uint32 *pairs, bool committed);

/* ---- DROP TABLE 生命周期（提交时点 GC）---- */
extern void ShardClogRememberDrop(Oid shard);
extern bool ShardClogHasPendingDrops(void);
extern void ShardClogAtCommit(void);	/* 执行挂起的删除 */
extern void ShardClogAtAbort(void);		/* 丢弃挂起的删除 */

#endif							/* SHARD_CLOG_H */
