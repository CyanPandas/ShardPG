#ifndef SHARD_VISIBILITY_H
#define SHARD_VISIBILITY_H

#include "pg_partdist.h"

/*
 * TX-TSO-MVCC P1（T1.6–T1.8，配内核补丁 0006）：
 *   T1.6 可见性临时桩 —— 实现补丁 0006 的 shard_visibility_hooks
 *        （access/shard_stamp.h）。【分叉结构 = 永久交付物；本文件的"临时
 *        提交表"= P2 分片 clog 落地后整体移除的一次性桩】。
 *   T1.7 行锁反查 —— 临时提交表的 RUNNING 条目同时记录持有者原生 xid，
 *        heap_delete/update 冲突路径经 xmax_wait 钩子翻译后按原生 xid 等待。
 *   T1.8 安全网骨架 —— GUC pg_partdist.shard_safety_mode（§9.2 第 1 层）：
 *        permissive=放行（P1 默认）；strict=分片表一切读写拦截（P3 接 TSO
 *        后语义收紧为"无 start_ts 读/无 gxid 写才拦"）。
 *
 * 临时提交表语义（P1 桩，P2 换分片 clog + commit_ts 判定）：
 *   条目存在 RUNNING（含持有者原生 xid）｜条目存在 ABORTED｜缺席 = 已提交。
 *   COMMIT 即删除条目（自清理）；崩溃重启后哈希清空 ⇒ 历史 xid 一律视为已
 *   提交 —— 崩溃时未提交的事务会被误判可见，这是 P1 已知桩限制（页面字节
 *   正确性不受影响，pagecmp 验收不受影响）。
 */

/* 临时提交表容量（固定，占满即 ERROR；P2 移除本表） */
#define SHARD_COMMIT_MAX_ENTRIES	65536

/* T1.8 安全网模式 */
#define SHARD_SAFETY_PERMISSIVE		0
#define SHARD_SAFETY_STRICT			1

extern int	shard_safety_mode;

extern void ShardVisibilityDefineGUCs(void);
extern void RequestShardCommitShmem(void);
extern void ShardCommitShmemInit(void);
extern void ShardVisibilityInstallHooks(void);

/* 领号时登记 RUNNING（T1.2 分配路径调用）；表满即 ERROR */
extern void ShardCommitRegisterRunning(Oid shard, TransactionId sxid,
									   TransactionId native_xid);

/* 事务结束回调里调用：提交=删除条目（缺席即提交），中止=置 ABORTED。
 * 在 COMMIT/ABORT 回调上下文执行，绝不 ERROR，异常一律 WARNING。 */
extern void ShardCommitMarkEnded(Oid shard, TransactionId sxid, bool committed);

/* T1.8 守卫点：strict 模式拦截 + SERIALIZABLE 禁令；读写路径共用 */
extern void ShardAccessGate(Oid shard, const char *what);

#endif							/* SHARD_VISIBILITY_H */
