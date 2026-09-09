#ifndef SHARD_XID_H
#define SHARD_XID_H

#include "pg_partdist.h"
#include "nodes/nodes.h"
#include "utils/rel.h"

/*
 * TX-TSO-MVCC P1（T1.1–T1.3，扩展侧，无内核补丁）：
 *   T1.1 分片表判定 —— GUC 白名单 pg_partdist.shard_relids 门控（P1_PRECHECK
 *        结论 C：不能按 partition_map 门控，否则报废 438 基线）；TOAST 表按
 *        pg_toast_<owner> 命名规约映射回属主分片，热路径零 catalog 查询。
 *   T1.2 每分片 32 位 xid 分配器 —— 共享内存固定槽位；0/1/2 保留、从 3 起
 *        （FirstNormalTransactionId，设计 §5.1）；批量水位 4096 持久化到
 *        $PGDATA/pg_shard_xid/<oid>，重启从水位续发（跳号无害）。
 *   T1.3 事务↔分片 xid 绑定 —— 后端本地 {分片 → 本事务 xid} 映射，首写惰性
 *        领取，XactCallback 清理；子事务（SAVEPOINT）P1 直接报错。
 *
 * 三者共同实现内核补丁 0005 的 shard_relation_xid_hook 契约
 * （access/shard_stamp.h）：assign=false 只判定、assign=true 领号。
 */

/* 分片 xid 起点：0=Invalid、1=Bootstrap、2=Frozen 语义保留（设计 §5.1） */
#define FIRST_SHARD_XID			((TransactionId) 3)

/* 每次水位推进的批量（一次 fsync 覆盖 4096 个号，崩溃至多跳这么多） */
#define SHARD_XID_BATCH			4096

/* 共享内存固定槽位数（P1 够用；P2 换 DSA/dshash，见 DEV PLAN T1.2） */
#define SHARD_XID_MAX_SLOTS		64

/* 单事务最多触达的分片数（后端本地映射的上限） */
#define SHARD_XID_MAX_PER_XACT	16

/* T4.6：门控是否开着（GUC 白名单非空 或 有 partition_map 登记） */
extern bool ShardGatingActive(void);

/* 水位文件所在目录（$PGDATA 下） */
#define SHARD_XID_DIR			"pg_shard_xid"

extern void ShardXidDefineGUCs(void);
extern void RequestShardXidShmem(void);
extern void ShardXidShmemInit(void);
extern void ShardXidInstallHook(void);

/* T1.1 谓词：relation 属于哪个白名单分片（TOAST 归并到属主）；不属于返回 InvalidOid */
extern Oid	ShardXidRelidLookup(Relation relation);

/* 按表 OID 直查白名单（可见性热路径用）。TOAST 经后端登记的归属映射命中：
 * 纯读走 SatisfiesToast 不需要分叉，但 toast 删除会走 SatisfiesUpdate，
 * 不分叉会把 hint 位写上 TOAST 页（T1.9 CP2 实测），映射登记发生在
 * 打标钩子命中 TOAST Relation 时（同后端、先于可见性判定）。 */
extern Oid	ShardXidLookupByOid(Oid reloid);

/* 本事务在该分片已领的 xid；未领返回 InvalidTransactionId（自见性判定用） */
/* U-P5-1：本事务已绑定的分片数（0 = 没碰过分片打标表） */
extern int	ShardXidXactCount(void);

extern TransactionId ShardXidMineForShard(Oid shard);

/* T2.4：确保该分片本次启动已完成无主 RUNNING 认领（槽位存在=已认领）。
 * 发号路径自动触发；可见性读路径在咨询 clog 前调用。返回本次改判条数。 */
extern int	ShardXidEnsureClaimed(Oid shard);
/* T6.4：§6.6 第三支，切主认领（强制重扫 [claim_wm, watermark)） */
extern int	ShardXidClaimOnPromote(Oid shard);

/* T2.5：恢复期影子推进（0007 redo 钩子逐对喂入，startup 进程调用） */
extern void ShardXidRedoAdvance(Oid shard, TransactionId sxid);

/* T2.7：partition_map 驱动门控——运行时集合登记 + 水位文件预创建
 * （pg_shard_xid/ 目录 = 启动登记表，ShardXidShmemInit 扫描重建集合） */
extern void ShardMvccSetAdd(Oid relid);
/* R-P6-9：DROP 提交时把 OID 摘掉，否则 OID 复用会误伤无关表 */
extern void ShardMvccSetRemove(Oid relid);

/* T7.7（R-P6-4）：DROP 提交时归还分配器槽位与影子。与 ShardMvccSetRemove 同源。 */
extern void ShardXidSlotRelease(Oid relid);
extern void ShardMvccEnsureWatermarkFile(Oid relid);

/* P1 数据保护拦截：VACUUM/ANALYZE/CLUSTER 点名白名单表一律 ERROR
 * （P1_PRECHECK 结论 D：原生 clog 会误判分片 xid，重写/回收路径必须封死） */
extern void ShardXidUtilityGuard(Node *parsetree);

/*
 * T5.1（设计 §6.1）：分片级 vacuum 两水位。与 alloc/claim 水位同住
 * $PGDATA/pg_shard_xid/<oid>（16 字节格式，向后兼容读 8/4 字节旧格式）。
 *   trunc_before —— clog 实际截断点 = 隐式 freeze 点 = 回卷龄基点（§7）；
 *   vacuum_xid   —— 两态恢复标记：== trunc_before 表示无未完成的趟，
 *                   > trunc_before 表示"页面趟已完成、截断待补"（§6.5）。
 * 不变式 trunc_before <= vacuum_xid 在落盘出口统一强制。
 */
/* U-P5-1 之二：本分片已持久化的发号水位（比 next_xid 宽一个批次，给 follower 用） */
extern TransactionId ShardXidAllocWatermark(Oid shard);

/* U-P5-1 之二：只抬不降地落盘发号水位（follower 接住 leader 的水位用） */
extern void ShardXidRaiseAllocWatermark(Oid shard, TransactionId alloc_wm);

/* T5.4：本分片下一个待发号（vacuum 的 fail-closed 上界）。0 = 取不到 */
extern TransactionId ShardXidNextToIssue(Oid shard);

extern void ShardVacuumGetWatermarks(Oid shard, TransactionId *trunc_before,
									 TransactionId *vacuum_xid);
extern void ShardVacuumSetWatermarks(Oid shard, TransactionId trunc_before,
									 TransactionId vacuum_xid);

#endif							/* SHARD_XID_H */
