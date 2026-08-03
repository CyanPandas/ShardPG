/*
 * global_mvcc.h
 *
 * 全局事务标识（gxid）—— FRD §9.1。
 *
 * 一个节点同时承载若干个 leader 的 shard 副本，各 leader 的 xid 空间彼此
 * 独立且必然重叠：node 3 的 xid 1000 与 node 7 的 xid 1000 是两个毫无关系
 * 的事务。物理回放把双方的元组都写进本节点的数据文件后，元组头里的
 * xmin/xmax 只剩 32 位本地 xid，**信息已经不足以区分来源**。
 *
 * gxid 就是补回这段信息：高 16 位记来源节点，低 48 位放该节点的本地 xid。
 * 它只出现在 parwal 记录头与 xid_map/pg_gclog 里，**不改动任何元组字节** ——
 * 元组仍是 leader 写下的原样，来源由"文件 → 分区 → 命名空间"的路由反查
 * （§9.4，R3 实装）。
 */
#ifndef GLOBAL_MVCC_H
#define GLOBAL_MVCC_H

#include "postgres.h"

typedef uint64 GlobalTransactionId;

#define GXID_NODE_BITS   16
#define GXID_XID_BITS    48

#define MakeGlobalXid(node_id, xid) \
    (((uint64) (uint16) (node_id) << GXID_XID_BITS) | \
     ((uint64) (xid) & ((UINT64_C(1) << GXID_XID_BITS) - 1)))

#define GxidNodeId(gxid)    ((uint16) ((gxid) >> GXID_XID_BITS))
#define GxidLocalXid(gxid)  ((TransactionId) ((gxid) & 0xFFFFFFFF))

#define InvalidGlobalXid    UINT64_C(0)
#define GlobalXidIsValid(gxid)  ((gxid) != InvalidGlobalXid)

/*
 * 本节点号。取 Citus 的 pg_dist_local_group.groupid（协调节点为 0，各 worker
 * 全局唯一），可用 GUC pg_partdist.node_id 覆盖 —— 单测/裸 PG 环境没有 Citus
 * 目录，以及将来节点号需要与 pg_raft 的 node_id 对齐时都要靠它。
 *
 * 首次调用会扫一次 pg_catalog.pg_dist_local_group，之后进程内缓存。因此
 * **不要在 wal_insert_hook 里调用**（XLogInsert 内部不允许碰目录）：捕获侧
 * 只记本地 xid，到 PRE_COMMIT 的 flush 路径再合成 gxid。
 */
extern int    partdist_node_id;         /* GUC；-1 = 自动取 Citus group id */
extern uint16 PartDistLocalNodeId(void);
extern int32  PartDistCitusGroupId(void);   /* 原始 group id，-1 = 无 Citus */
extern void   DefineGlobalMVCCGUCs(void);

#endif /* GLOBAL_MVCC_H */
