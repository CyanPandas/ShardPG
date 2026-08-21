/*
 * shard_vacuum.h
 *
 * 分片级 vacuum 的页面动作（设计 §6.4，P5 T5.3）。
 *
 * §6.4 三类动作：① 删中止 xmin 的元组；② 删已提交删除的死元组（含索引两
 * 阶段）；③ **xmax 消毒**——把截断点以下的 ABORTED 与 lock-only xmax 清成
 * `InvalidTransactionId`。本头文件当前只声明 ③（T5.3a）。
 *
 * 顺序铁律（§6.4 末）：数据页、索引、堆全部清完，才许动 clog。因此这里的
 * 每个入口都只做"清"，**绝不推进任何水位** —— 推进由 T5.4 的截断路径在
 * 确认整趟做完之后统一落。
 *
 * 复制（§6.7）：这些页面修改本身就是页面变更，走 pg_parwal 流被 follower
 * 逐字节回放，follower 不跑自己的 vacuum。故本模块的每一次页面写入都必须
 * 经由**内核自己的 WAL 发生器**产生，不自造记录格式 —— ③ 用的是
 * `heap_freeze_execute_prepared()`（发 XLOG_HEAP2_FREEZE_PAGE）。
 */
#ifndef SHARD_VACUUM_H
#define SHARD_VACUUM_H

#include "postgres.h"

#include "utils/relcache.h"

/* 一趟页面动作的计数（验收与排障用） */
typedef struct ShardVacuumPageStats
{
	int64		pages_scanned;	/* 扫过的页数 */
	int64		pages_dirtied;	/* 实际改写并写 WAL 的页数 */
	int64		tuples_touched; /* 被消毒的元组数 */
} ShardVacuumPageStats;

/*
 * §6.4 ③：xmax 消毒。
 *
 * trunc_before = 本轮**打算**推进到的截断点（开区间上界，语义同持久水位
 * `clog_truncate_before`：`xid < trunc_before` 即免查隐式冻结区）。判据：
 *
 *   - `xmax >= trunc_before`  ⇒ 不动（还在查得到 clog 的区间里）；
 *   - lock-only xmax          ⇒ 清（锁随事务结束释放，留着会被免查区读成
 *                                "已提交的删除"）；
 *   - clog ABORTED            ⇒ 清（本条就是 §6.4 ③ 的正主）；
 *   - clog COMMITTED          ⇒ 不动（真删除，元组由动作 ② 回收；留着被读成
 *                                "已提交的删除"恰好正确）；
 *   - clog RUNNING/PREPARED/空洞：
 *       · 落在 [当前截断点, trunc_before) ⇒ **ERROR**。§6.3 的前缀扫描遇
 *         未决即停，调用者不可能算出跨过未决条目的 target；到这里说明调用
 *         者给错了 trunc_before，fail-closed；
 *       · 落在当前截断点以下 ⇒ 不动。其 clog 已被上一轮截断，判不出来了；
 *         这是过去时的既成事实，本趟无从补救（只可能来自上一轮违反顺序
 *         铁律），留着是唯一安全动作。
 *
 * 调用者需持有 rel 的锁（建议 ShareUpdateExclusiveLock，与原生 vacuum 同级）。
 * 本函数不推进水位、不截断 clog。
 */
extern void ShardVacuumSanitizeXmax(Relation rel, TransactionId trunc_before,
									ShardVacuumPageStats *stats);

#endif							/* SHARD_VACUUM_H */
