/*
 * tso.h — TX-TSO-MVCC P3：TSO 服务（设计 §2，v1 形态 = §2.4 裁定）。
 *
 * v1 = master（coordinator）上的共享内存 int64 单调逻辑计数器：
 *   - 从 1 起发号，0 保留为"无 ts"（P2 历史判决 ts=0 在 §4.1 判据下恒可见，
 *     兼容红利，P3_PRECHECK 结论一）；
 *   - 不做水位持久化、不做 TSO 自身 HA（§2.4：master/TSO 重启 = 单调性失守
 *     = 整簇重建）；
 *   - **boot 防呆**（§2.4 配套 2）：首次服务前把 $PGDATA/pg_tso_boot 标记
 *     durably 落盘；启动时检测到标记 ⇒ 本次启动一切发号请求 ERROR（响亮
 *     停摆），重建流程删标记后重启即恢复全新纪元；
 *   - **发号即登记**（§2.3 铁律）：start_ts 请求携带该节点最老活跃快照
 *     （0=无），同一临界区内先登记 min(携带值, 新号) 再返回新号——
 *     GlobalSafeTs（§6.2，T3.5 消费）的安全地基；
 *   - 服务身份显式化：GUC pg_partdist.tso_master=on 的节点才服务，其余
 *     节点收到调用一律 ERROR（fail-closed，防误连错节点开出第二个纪元）。
 *
 * SQL 入口（worker 经 libpq 调用，T3.2）：
 *   partdist_tso_start_ts(node int, oldest bigint) → bigint
 *   partdist_tso_commit_ts()                       → bigint
 *   partdist_tso_status()                          → text（观测/验收用）
 */
#ifndef PARTDIST_TSO_H
#define PARTDIST_TSO_H

#include "postgres.h"

#define TSO_MAX_NODES		64
#define TSO_BOOT_MARKER		"pg_tso_boot"

extern void TsoDefineGUCs(void);
extern void RequestTsoShmem(void);
extern void TsoShmemInit(void);

/* ---- T3.2 worker 取号通路（tso_client.c） ---- */
extern void TsoClientDefineGUCs(void);
extern void RequestTsoClientShmem(void);
extern void TsoClientShmemInit(void);
extern int64 TsoGetStartTs(void);		/* 懒取 + 事务内缓存；遗留模式=0 */
extern void TsoStashCommitTs(void);		/* PRE_COMMIT 暂存（临界区外） */
extern int64 TsoStashedCommitTs(void);	/* 临界区内只读暂存 */
extern void TsoClientClearActive(void);	/* 事务结束清缓存与活跃槽 */
extern bool TsoConfigured(void);		/* conninfo 非空 = TSO 模式 */

/* ---- T3.5 GlobalSafeTs（心跳 bgworker + 栅栏） ---- */
extern void TsoRegisterHeartbeatWorker(void);
extern PGDLLEXPORT void TsoHeartbeatWorkerMain(Datum main_arg);

#endif							/* PARTDIST_TSO_H */
