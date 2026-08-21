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

/* 一趟页面动作的计数（验收与排障用；各动作只填与自己相关的字段） */
typedef struct ShardVacuumPageStats
{
	int64		pages_scanned;	/* 扫过的页数 */
	int64		pages_dirtied;	/* 实际改写并写 WAL 的页数 */
	int64		pages_skipped;	/* ①：拿不到 cleanup lock 而跳过的页数 */
	int64		tuples_touched; /* ③ 消毒条数 / ① 移除条数 */
	int64		tuples_deferred;	/* ①：死但仍挂在 HOT 链上，本趟不动 */
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

/*
 * §6.4 ①：删中止 xmin 的元组。
 *
 * 不做的后果：截断之后该 xmin 落进免查隐式冻结区、被解释成"已提交、全可见"，
 * **中止事务的幽灵行复活**。
 *
 * trunc_before 语义与 ③ 相同；未决条目的两分支守卫也相同。判据只看 xmin：
 * clog ABORTED 且 `xmin < trunc_before` ⇒ 删；COMMITTED ⇒ 留。
 *
 * ★ 必须先跑 ③ 再跑 ①（顺序不是偏好，是依赖）：中止事务"插入后又更新"会留下
 *   一条 HOT 链，链中元组既是死的、又还挂着 `HEAP_HOT_UPDATED`；而那个 bit 的
 *   有效性以 `HEAP_XMAX_INVALID == 0` 为前提，正是 ③ 清 xmax 时一并解除的。
 *   ③ 没跑过时这些元组会被计进 `tuples_deferred` 留到下一趟。
 *
 * ★ 只对**无索引**的分片表可用：本函数走的是原生"无索引表"通路
 *   （prune 记录把行指针置 LP_DEAD，紧接一条 vacuum 记录置 LP_UNUSED），
 *   不含索引两阶段（那是 T5.3c）。带索引的关系一律 ERROR，fail-closed。
 *
 * 调用者需持有 rel 的锁（建议 ShareUpdateExclusiveLock）。本函数逐页取
 * **cleanup lock**（回收行指针的硬要求）；取不到则跳过该页并计入
 * `pages_skipped` —— 跳过即本趟不完整，截断不得进行（T5.4 的门禁条件）。
 */
extern void ShardVacuumRemoveAbortedXmin(Relation rel,
										 TransactionId trunc_before,
										 ShardVacuumPageStats *stats);

#endif							/* SHARD_VACUUM_H */
