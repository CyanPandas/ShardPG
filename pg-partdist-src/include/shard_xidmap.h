/*
 * shard_xidmap.h
 *
 * 每分区一张的 local_xid → gxid 映射（FRD §9.2）。
 *
 * 为什么作用域是**分区**而不是节点：一个节点会承载多个副本组，各 leader 的
 * xid 空间彼此独立且必然重叠 —— 甲组的 xid 1000 和乙组的 xid 1000 是两个
 * 毫不相干的事务。`local_xid → gxid` 的唯一性只在单分区内成立，所以表由
 * 持有该分区的 ShardReplayCtx（回放期）/ ShardRouteEntry（查询期，R3）持有，
 * 条目里不需要再存分区字段。
 *
 * 为什么必须持久化：它服务的是**查询期可见性**，生命周期远超一次回放。
 * 只驻内存的话，崩溃后就成了"页面上有元组、但不知道这些 xid 属于谁"，
 * 可见性无从判定。因此随 apply_checkpoint 快照一起原子落盘（§8.4）。
 *
 * 形态：R1/R2 是 worker 私有 HTAB + 快照文件；R3 读路径实装时迁移为
 * shmem 内 in-place DSA + dshash（PG16 无 DSM registry，须在 shmem_startup
 * 阶段 dsa_create_in_place 预建），句柄登记在路由表条目上供任意 backend 查。
 */
#ifndef SHARD_XIDMAP_H
#define SHARD_XIDMAP_H

#include "postgres.h"
#include "access/transam.h"

#include "global_mvcc.h"

/*
 * reserved 是**显式**补齐位，不是编译器填充：条目会原样写进 apply_checkpoint
 * 并计入 CRC。留一个未初始化的 4 字节洞，同一份逻辑内容每次落盘的字节都不同 ——
 * 副本之间没法按字节比对 checkpoint，parwal-2.0 的 xid 尾部填充就是这个坑
 * （见 partition_wal_header.h 的 PartWALRecordGxid 兼容读注释）。
 */
typedef struct XidMapEntry
{
    TransactionId       local_xid;  /* 键：leader 分配的 32 位 xid（XLogRecord.xl_xid）*/
    uint32              reserved;   /* 显式补齐，恒为 0                                */
    GlobalTransactionId gxid;       /* 值：MakeGlobalXid(来源节点, local_xid)          */
} XidMapEntry;

StaticAssertDecl(sizeof(XidMapEntry) == 16,
                 "XidMapEntry 必须是 16 字节（apply_checkpoint 快照的磁盘格式）");
StaticAssertDecl(offsetof(XidMapEntry, gxid) == 8,
                 "gxid 必须落在偏移 8");

/*
 * 单分区 xid_map 的条目上限。
 *
 * 条目在对应 xid 全部冻结后才可回收（§8.4「xid_map 增长控制」），在那套账目
 * 接上之前，这里给一个硬上限并在触顶时明确报错 —— 悄悄丢条目等于悄悄丢
 * 可见性信息，比停下来更糟。48 字节/条 × 100 万 ≈ 48MB，单分区远够用。
 */
#define SHARD_XIDMAP_MAX_ENTRIES  1000000

#endif /* SHARD_XIDMAP_H */
