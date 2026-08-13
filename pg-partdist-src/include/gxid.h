/*
 * gxid.h — globalXID 分配器（P4 T4.1，设计 §2.1；P4_PRECHECK 结论四）。
 *
 * 编码：节点号(16b) | 节点内单调序号(48b)，int64。由协调者分片 leader 所在
 * 节点发号；每节点独立计数器，跳号无害、复用绝不允许。
 * 崩溃安全：批量水位持久化（$PGDATA/pg_gxid_wm，8 字节小端 uint64；
 * tmp+fsync+durable_rename，先落盘后发号——shard_xid 同款纪律）。
 * 与既有 gxid 宏（global_mvcc.h GxidNodeId/GxidLocalXid）同构，旧 txn 层
 * 账本键的语义迁移见 P4_PRECHECK 结论四。
 */
#ifndef PARTDIST_GXID_H
#define PARTDIST_GXID_H

#include "postgres.h"

#define GXID_WM_FILE		"pg_gxid_wm"
#define GXID_BATCH			4096
#define GXID_SEQ_MASK		UINT64CONST(0x0000FFFFFFFFFFFF)

extern void RequestGxidShmem(void);
extern void GxidShmemInit(void);

/* 发一个 gxid（本节点号自动编码；崩溃后从水位续发不重号） */
extern int64 GxidAllocate(void);

#endif							/* PARTDIST_GXID_H */
