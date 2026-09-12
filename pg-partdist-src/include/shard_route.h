/*
 * shard_route.h
 *
 * R3 读路径：回放来的元组怎么判可见（FOLLOWER_REPLAY_DESIGN.md §9.4/§10）。
 *
 * 问题：回放把 leader 的元组**原样**写进本节点的数据文件，元组头里的
 * xmin/xmax 只剩 32 位本地 xid —— 那是**别人的**号。拿它查本机 clog，查到的
 * 是本机历史上碰巧同号的另一笔事务（实测：升主后整张表读成 0 行，因为那些号
 * 在本机判决是 aborted，或根本没建过 clog 页）。
 *
 * 补信息的办法是 gxid（高 16 位来源节点 + 低 48 位该节点本地 xid）。回放时
 * 每条记录的头都带 gxid，回放侧据此建了两本账：
 *   · xid_map（每分区一张，local_xid → gxid），随 apply_checkpoint 落盘；
 *   · pg_gclog（每来源节点一套，gxid → 判决），MARKER 到达时写。
 * 读路径就是把这两跳接起来：tuple.xmin → xid_map → gxid → gclog → 判决。
 *
 * §9.4 的三条路由规则：
 *   1. 本机原生 leader 分区（没有 apply_checkpoint）→ 走本机命名空间，
 *      与改动前逐字节等价；
 *   2. 副本（replica）→ 查本分区 xid_map；
 *   3. 已升主（promoted）→ xid ≤ 升主水位 W 的查 xid_map，W 之上是新主
 *      自己发的号，归本机命名空间。
 *
 * ★ 为什么键是**本地关系 OID**而不是 RelFileLocator：回放上下文里的
 *   `ctx->shard_oid` 本来就是本地壳表的 OID（分片 clog、apply_checkpoint 目录
 *   都按它编址），而可见性钩子拿到的 `htup->t_tableOid` 正是同一个值。
 *   用它做键，读侧一次目录访问都不需要 —— 这一点是硬要求：可见性判定可能
 *   在持缓冲区锁时被调用，碰 catalog 会死锁。
 *
 * ★★ 本期交付面 = **MVCC 读**，仅此一条。
 *   `satisfies_self / satisfies_dirty / satisfies_update` 仍走本机语义 ——
 *   也就是说：升主后的新主**读得到**切主前的行，但对这些行做 UPDATE/DELETE
 *   时，判可更新性走的还是本机 clog，结论不可信。这是有意划的界，不是遗漏：
 *   写路径要处理的是"谁能改一条别人宇宙里的元组"，涉及行锁、xmax 消毒与
 *   回卷，属 R4（FOLLOWER_REPLAY_DESIGN.md §11）。在那之前，promoted 分片的
 *   正确用法是**只读 + 重做基线**，与 R3 未实装时的约束一致（R4 阻断于 R3，
 *   现在 R3 这一半通了，R4 的其余部分没通）。
 *
 * ★★ 另一条**先于本模块就存在**的隐患，读通之后更容易撞上，记在这里：
 *   补丁 0005 让 `heap_page_prune_opt` 对**分片打标**的关系直接返回，但判据是
 *   `shard_relation_xid_hook`（= 在 partition_map 里登记过）。没打标的分布表
 *   （tx3 夹具就是）回放到副本上之后**不在这道豁免里** —— 升主后访问闸放行，
 *   一条普通 SELECT 触发的 on-access 剪枝会拿**本机 clog** 去判那些外来 xid
 *   的死活，把回放好的元组当垃圾清掉。只插入的页碰不到（`pd_prune_xid` 是
 *   leader 写下的，多半为 0），有过 UPDATE/DELETE 的页就说不准。
 *   本模块只改**读**，不碰剪枝，因此既没引入也没消除它；根治要么把豁免判据
 *   从"打过标"放宽到"路由表命中"（要动内核补丁 0005），要么升主时强制
 *   重做基线。**在那之前，promoted 分片仍应按只读对待。**
 *
 * ★ 为什么允许在这条路径上读文件：既有实现本来就这么做 ——
 *   `ShardClogReadSlot` 读 pg_shard_clog/<oid>，`EnhancedClogReadStatus`
 *   读 pg_gclog/<node>/<seg>，都在同一条判定路径上。本模块只是多读一个
 *   apply_checkpoint，而且**只在每后端每关系第一次**读，之后全是内存命中。
 */
#ifndef SHARD_ROUTE_H
#define SHARD_ROUTE_H

#include "postgres.h"
#include "access/transam.h"

#include "global_mvcc.h"

typedef enum ShardRole
{
    SHARD_NATIVE_LEADER = 0,    /* 本机原生分区：没有 apply_checkpoint */
    SHARD_FOLLOWER_REPLAYED,    /* 同步而来，当前为 follower           */
    SHARD_PROMOTED              /* 同步而来，已升主                    */
} ShardRole;

typedef struct ShardRouteInfo
{
    Oid           shard_oid;    /* 本地壳表 OID（= 回放侧 ctx->shard_oid） */
    ShardRole     role;
    TransactionId watermark;    /* 升主水位 W；仅 SHARD_PROMOTED 有意义    */
    uint32        nxidmap;      /* 已缓存的 xid_map 条目数                 */
} ShardRouteInfo;

/*
 * 本关系是不是"回放来的壳表"。false = §9.4 规则 1，调用方按本机语义走。
 * 只读本后端缓存 + 至多一次 apply_checkpoint 读，不碰 catalog。
 */
extern bool PartDistRouteLookup(Oid local_reloid, ShardRouteInfo *out);

/*
 * §9.4 规则 2/3 的封装：把本关系上的一个 32 位 xid 翻成 gxid。
 * 返回 InvalidGlobalXid 表示"这个号不属于回放宇宙"——调用方应退回本机语义
 * （规则 1、或规则 3 里 xid > W 的那一半）。
 */
extern GlobalTransactionId PartDistResolveGxid(Oid local_reloid,
                                               TransactionId xid);

/* 丢弃本后端的路由/xid_map 缓存（DROP、重做基线、验收用例换夹具时用）。 */
extern void PartDistRouteCacheReset(void);

#endif                          /* SHARD_ROUTE_H */
