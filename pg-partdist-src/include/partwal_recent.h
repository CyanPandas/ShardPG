/*
 * partwal_recent.h — 最近写下的分区流记录的节点级内存缓存（2026-10-16）。
 *
 * 复制（leader 按 plsn 把刚落盘的记录读回来发给副本）与 DTX 收尾（按 dtxid 找刚写的
 * DTX_PREPARE）都要按 plsn 回读分区流。文件回读靠段名序 = plsn 序的不变式加后端本地
 * 稀疏索引，索引冷 / 切主后段名倒挂 / 记录不在时退化成整条流逐条 read+lseek 的全扫描
 * （三副本 TPC-C 实测单次几十秒到几分钟，持着组的复制认领位）。这里把写进段文件的
 * 每条记录同时放进一个共享内存环，按 (分区, plsn) 查；查不中再走文件。
 *
 * 只缓存已 write() 进段文件的记录（写入器刷缓冲时登记），内容与文件逐字节相同。
 * 任何分区发生截断（编号回退）时整体换代，此前登记的条目一律作废 —— 截断罕见，
 * 作废的代价只是退回文件读。
 */
#ifndef PARTWAL_RECENT_H
#define PARTWAL_RECENT_H

#include "postgres.h"
#include "partition_wal_header.h"

extern int partwal_recent_cache_mb;     /* GUC pg_partdist.partwal_recent_cache_mb（0 = 关） */

extern void RequestPartWALRecentShmem(void);
extern void PartWALRecentShmemInit(void);

/* 登记写入器缓冲里的一串完整记录（头 + 载荷，首尾相接） */
extern void PartWALRecentInsertBuffer(Oid partition_id, const char *buf, int len);
/* 命中则填 *out_rec、把载荷 palloc 到 *out_data（data_len = 0 时为 NULL），返回 true */
extern bool PartWALRecentLookup(Oid partition_id, uint64 plsn,
                                PartWALRecord *out_rec, char **out_data);
/* 截断前后调用：此前的条目全部作废 */
extern void PartWALRecentInvalidateAll(void);

#endif                          /* PARTWAL_RECENT_H */
