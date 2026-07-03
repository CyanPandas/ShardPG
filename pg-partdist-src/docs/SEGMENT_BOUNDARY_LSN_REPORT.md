# 跨段切换边界 LSN 连续性验证报告

**日期：** 2026-06-07  
**环境：** pg-citus-cluster-container（PostgreSQL 16 + Citus 13.1）  
**脚本：** `test_segment_boundary_lsn.sh`

---

## 测试目标

验证 pg_partdist 的 `pg_parwal/<partition_id>/` 目录在经历多次 WAL 段切换后，`partition_lsn` 在跨段边界处保持严格连续（相邻记录差值恒为 1），不发生任何断裂。

---

## 测试方法

| 参数 | 值 |
|------|-----|
| 测试 Partition OID | 99001（虚拟 OID，直接调用 `write_partition_wal_record`）|
| 每批写入记录数 | 5 |
| WAL 段切换次数 | **6 次**（≥5 次要求） |
| 总批次 | 7（6 个中间批 + 1 个最终批） |
| 总写入记录数 | 35 |
| 等待同步方式 | `partdist.demux_flush()`（阻塞直到 Demux Worker 处理完全部 WAL）|

### 测试流程

```
for i in 1..7:
    write_partition_wal_record(99001, 1) × 5   # 写入 5 条 PartWALHeader
    if i < 7:
        pg_switch_wal()                         # 强制段切换
demux_flush()                                   # 等待 Demux Worker 路由完成
```

---

## 测试结果

所有测试项全部通过：

```
==================================================
 test_segment_boundary_lsn.sh
 测试 OID    : 99001
 段切换次数  : 6  (≥5 要求)
 每批记录数  : 5
 总批次      : 7
 预期总记录  : 35
 预期段文件  : 7
==================================================

[PASS] T1  verify_partition_wal(99001) = true
[PASS] T2  count_parwal_records = 35  (= 预期 35)
[PASS] T3  SQL gap 检查：partition_lsn 全段无断裂（gap 数 = 0）
[PASS] T4  段边界连续性：6 处边界全部 diff=1（检测到 ≥6 次切换）
[PASS] T5  二进制解析：partition_lsn 跨段全部连续
[PASS] T6  6 次段切换后 count_parwal_records = 35 = 写入数 35

 PASSED : 6
 FAILED : 0
 段文件 : 7  段切换 : 6
```

---

## LSN 序列样本（段边界处）

SQL 层（`check_partition_wal` + `pg_walfile_name_offset`）检测到 6 处 WAL 文件边界，每处 `partition_lsn` 差值均为 1：

| 边界 # | partition_lsn | diff | WAL 段编号变化 |
|--------|---------------|------|----------------|
| 1 | 5 → 6 | **1** | 00000013 → 00000014 |
| 2 | 10 → 11 | **1** | 00000014 → 00000015 |
| 3 | 15 → 16 | **1** | 00000015 → 00000016 |
| 4 | 20 → 21 | **1** | 00000016 → 00000017 |
| 5 | 25 → 26 | **1** | 00000017 → 00000018 |
| 6 | 30 → 31 | **1** | 00000018 → 00000019 |

---

## 二进制段文件解析（Perl）

直接读取 `pg_parwal/99001/` 下所有 32 字节 `PartWALHeader` 记录，
验证跨文件边界 `partition_lsn` 连续性：

```
  段文件数    : 7（期望 7）

  000000010000000000000013    5 条  plsn [  1- 5]  orig=0/13000288
  000000010000000000000014    5 条  plsn [  6-10]  orig=0/14000028
  000000010000000000000015    5 条  plsn [ 11-15]  orig=0/15000028
  000000010000000000000016    5 条  plsn [ 16-20]  orig=0/16000028
  000000010000000000000017    5 条  plsn [ 21-25]  orig=0/17000028
  000000010000000000000018    5 条  plsn [ 26-30]  orig=0/18000028
  000000010000000000000019    5 条  plsn [ 31-35]  orig=0/19000028

  OK  边界 #1: ...000013 → ...000014  plsn 5→6   diff=1
  OK  边界 #2: ...000014 → ...000015  plsn 10→11 diff=1
  OK  边界 #3: ...000015 → ...000016  plsn 15→16 diff=1
  OK  边界 #4: ...000016 → ...000017  plsn 20→21 diff=1
  OK  边界 #5: ...000017 → ...000018  plsn 25→26 diff=1
  OK  边界 #6: ...000018 → ...000019  plsn 30→31 diff=1

  总断裂数        : 0
  跨段边界(OK)    : 6
  二进制解析结论  : PASS
```

---

## 关键机制说明

- `partition_lsn` 由 `AllocPartitionLSN` 原子递增分配，与 WAL 段边界无关。
- `WritePartitionWAL` 根据 `orig_node_lsn` 决定写入哪个 pg_parwal 段文件（`XLByteToSeg`），但 `partition_lsn` 计数器不会因此重置。
- `pg_switch_wal()` 强制 worker 进入新 WAL 段；之后写入的记录 `orig_node_lsn` 落入新段 → Demux Worker 创建新的 pg_parwal 段文件。
- 跨文件 `partition_lsn` 连续性由 `verify_partition_wal()` 和二进制解析双重确认。

---

## 结论

```
跨段边界 LSN 连续性: PASS
```

经 6 次 WAL 段切换（≥5 次），7 个 pg_parwal 段文件中：
- **`verify_partition_wal`**: true（严格单调验证通过）
- **`count_parwal_records`**: 35 = 写入数（无记录丢失）
- **SQL 层 gap 检查**: 0 处断裂
- **6 处跨段边界**: 全部 `partition_lsn` diff = 1
- **Perl 二进制解析**: 无断裂，所有边界连续
