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
/*
 * 2026-10-16：时间戳高水位文件。内容 = 已持久预留的上界（十进制 int64），
 * 发出的每个号都严格小于它。重启后从这里接着发，单调性跨重启成立 —— 有了它，
 * boot 标记不再意味着"单调性失守"，master 可以重启（换库、宕机恢复都需要）。
 */
#define TSO_HWM_FILE		"pg_tso_hwm"
#define TSO_HWM_BATCH		INT64CONST(1000000)	/* 每次预留的号数（只影响落盘频率与重启跳号） */

extern void TsoDefineGUCs(void);
extern void TsoRedoHwm(int64 hwm);	/* 2026-10-17：备库回放高水位记录 */
extern void RequestTsoShmem(void);
extern void TsoShmemInit(void);

/* ---- T3.2 worker 取号通路（tso_client.c） ---- */
extern void TsoClientDefineGUCs(void);
extern void RequestTsoClientShmem(void);
extern void TsoClientShmemInit(void);
extern int64 TsoGetStartTs(void);		/* 懒取 + 事务内缓存；遗留模式=0 */
extern void TsoRefreshStartTs(void);	/* RC 重评后换新快照（语句边界调用） */
extern void TsoStashCommitTs(void);		/* PRE_COMMIT 暂存（临界区外） */
extern int64 TsoStashedCommitTs(void);	/* 临界区内只读暂存 */
extern int64 TsoPeekStartTs(void);	/* T7.11：只看不取，0 = 本事务没有 TSO start_ts */
extern int64 TsoMarkerCommitTs(void);	/* T4.4：MARKER/DTX 记录 ts 源（遗留=本地时钟） */
extern int64 TsoMarkerCommitTsEx(bool *is_tso);	/* T7.29：同上，并告知值属于哪个宇宙 */
extern int64 PartDistTsoDtxDecisionTs(void);	/* T4.4：决议点取号（遗留=0；rendezvous 出口） */
extern void TsoClientClearActive(void);	/* 事务结束清缓存与活跃槽 */
extern bool TsoConfigured(void);		/* conninfo 非空 = TSO 模式 */

/* ---- T4.1 连接加入协议 ---- */
extern void TsoInjectStartTs(int64 ts);	/* 注入协调者下发的快照（登记活跃集合） */
extern int64 TsoCurrentGxid(void);		/* 0 = 未加入全局事务 */
extern int64 TsoCurrentCoordGsid(void);	/* 0 = 未知 */

/* T5.2：worker 侧取 GlobalSafeTs（经 RPC；失败返回 0 = 不清，安全方向） */
extern int64 TsoGetGlobalSafeTs(void);

/* ---- T3.5 GlobalSafeTs（心跳 bgworker + 栅栏） ---- */
extern void TsoRegisterHeartbeatWorker(void);
extern PGDLLEXPORT void TsoHeartbeatWorkerMain(Datum main_arg);

#endif							/* PARTDIST_TSO_H */
