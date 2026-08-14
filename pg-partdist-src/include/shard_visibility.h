#ifndef SHARD_VISIBILITY_H
#define SHARD_VISIBILITY_H

#include "pg_partdist.h"

/*
 * TX-TSO-MVCC 可见性裁决（配内核补丁 0006；分叉结构 T1.6–T1.8 交付，
 * 真相源自 T2.3 起为持久分片 clog——shard_clog.h）：
 *   可见性 —— 实现补丁 0006 的 shard_visibility_hooks（access/shard_stamp.h）。
 *        裁决顺序：后端映射（自见）→ 活跃表 → 后端终局缓存 → 分片 clog。
 *        clog 全零/空洞 = RUNNING = 不可见；崩溃后未决事务天然不可见
 *        （T2.4 认领改判 ABORTED）。P1 桩的"缺席=已提交"崩溃漏判已消除。
 *   行锁反查（T1.7）—— 活跃表条目记录持有者原生 xid，heap_delete/update
 *        冲突路径经 xmax_wait 钩子翻译后按原生 xid 等待。
 *   安全网骨架（T1.8）—— GUC pg_partdist.shard_safety_mode（§9.2 第 1 层）：
 *        permissive=放行（默认）；strict=分片表一切读写拦截（P3 接 TSO
 *        后语义收紧为"无 start_ts 读/无 gxid 写才拦"）。
 *
 * 活跃表 = 本次启动内 RUNNING 的分片事务（事务态，不持久）：领号时进表、
 * 事务结束回调里"先写 clog 判决、后摘条目"。崩溃重启后表清空 ⇒ 遗留事务
 * 在 clog 里保持 RUNNING（不可见）等认领，语义安全。
 */

/* 活跃表容量（固定，占满即 ERROR = 同时活跃的分片事务数上限） */
#define SHARD_COMMIT_MAX_ENTRIES	65536

/* T1.8 安全网模式 */
#define SHARD_SAFETY_PERMISSIVE		0
#define SHARD_SAFETY_STRICT			1

extern int	shard_safety_mode;

extern void ShardVisibilityDefineGUCs(void);
extern void RequestShardCommitShmem(void);
extern void ShardCommitShmemInit(void);
extern void ShardVisibilityInstallHooks(void);

/* 领号时调用（T1.2 分配路径）：先落 clog RUNNING 账，再进活跃表；
 * 任一步失败领号作废（fail-closed，跳号无害） */
extern void ShardCommitRemove(Oid shard, TransactionId sxid);
extern void ShardCommitRegisterRunning(Oid shard, TransactionId sxid,
									   TransactionId native_xid);

/* 事务结束回调里调用：先写 clog 终局判决（fsync），后摘活跃表条目。
 * 判决写失败升 PANIC（借崩溃恢复走 0007 redo 补齐）；摘条目异常仅 WARNING。 */
extern void ShardCommitMarkEnded(Oid shard, TransactionId sxid, bool committed);

/* T1.8 守卫点：strict 模式拦截 + SERIALIZABLE 禁令；读写路径共用 */
extern void ShardAccessGate(Oid shard, const char *what);

#endif							/* SHARD_VISIBILITY_H */
