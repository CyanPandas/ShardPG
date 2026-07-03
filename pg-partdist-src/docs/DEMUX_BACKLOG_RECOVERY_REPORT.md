# Demux Worker 高积压崩溃恢复测试报告

**日期**: 2026-06-06  
**测试脚本**: `test_demux_backlog_recovery.sh`  
**被测组件**: pg_partdist Demux Background Worker  
**环境**: PostgreSQL 16 + Citus 13.1，容器 `pg-citus-cluster-container`

---

## 积压制造方法

采用 **SIGSTOP + 批量 WAL 写入 + SIGKILL/pg_ctl stop** 的三步法：

1. **SIGSTOP 暂停 Demux**：向 Demux Worker 发送 `SIGSTOP`，使其挂起，无法消费 WAL。
2. **批量写入 WAL**：通过 `partdist.write_partition_wal_record()` 的 PL/pgSQL DO 循环写入 N 条记录（每条调用 `XLogFlush` 确保落盘）。此时记录只在 `pg_wal` 中，尚未路由到 `pg_parwal`。
3. **触发崩溃或重启**：
   - **kill -9**：触发 PostgreSQL Crash Recovery，`partdist_wal_redo` 将积压记录写入 `pg_parwal`；
   - **pg_ctl stop -m fast + start**：干净重启，Demux 从 progress 文件恢复（需 LoadDemuxProgress 修复）。

---

## 发现的 Bug

### 根因：`LoadDemuxProgress()` 从未在启动时被调用

**文件**: `src/worker/demux_worker.c`，`DemuxWorkerMain()` 函数

`SaveDemuxProgress()` 在主循环中每 16 条记录保存一次，退出时也保存。但 `LoadDemuxProgress()` 虽已定义，**从未在启动时被调用**。`DemuxWorkerMain()` 始终从 `GetFlushRecPtr()`（当前 WAL 末端）开始读取。

**影响场景**：干净重启（`pg_ctl restart -m fast`）时：
- Demux 优雅退出，在某个中间 WAL 位置（如 P200）保存 progress 文件；
- 期间有 5000 条新写入（P201..P5200）在 WAL 中等待；
- 重启后，新 Demux 从 `GetFlushRecPtr() = P5200` 开始，完全跳过积压；
- **结果**：5000 条记录永久丢失（无 Crash Recovery 兜底）。

**修复**：在 `DemuxWorkerMain()` 的启动 LSN 逻辑中，优先调用 `LoadDemuxProgress()`：

```c
{
    XLogRecPtr saved_lsn = LoadDemuxProgress();
    XLogRecPtr flush_lsn = GetFlushRecPtr(&tli);

    if (flush_lsn == InvalidXLogRecPtr)
        flush_lsn = GetRedoRecPtr();

    if (!XLogRecPtrIsInvalid(saved_lsn) && saved_lsn < flush_lsn)
        startLSN = saved_lsn;
    else
        startLSN = flush_lsn;
}
```

**安全性**：post-crash 重启时，Crash Recovery 已通过 `partdist_wal_redo` 将所有积压记录写入 `pg_parwal`。新 Demux 从旧 progress 位置重读这些记录时，`partition_lsn` 去重保护（`hdr->partition_lsn > w->last_partition_lsn`）会跳过已写入的记录；WAL 页头陈旧问题由 `null_streak / XLogFindNextRecord` 机制处理。

---

## 测试结果

| 场景 | 描述 | 积压量 | 预期 | 结果 |
|------|------|--------|------|------|
| **S1** | kill -9 下 5000 条积压（Crash Recovery 路径） | 5000 | 5400 | **PASS** (5400) |
| **S2** | 干净重启（pg_ctl stop -m fast + start）下 5000 条积压（Progress File 路径） | 5000 | 5400 | **PASS** (5400) |
| **S3** | 三次连续 kill -9 循环（每次 1500 条） | 4500 | 4900 | **PASS** (4900) |
| **S4** | 超深积压 kill -9（10000 条） | 10000 | 10150 | **PASS** (10150) |
| **S5** | Crash Recovery 后立即并发写入（验证无冲突） | 2000 | 2300 | **PASS** (2300) |

**断言统计**：26 PASS / 0 FAIL

---

## 各场景关键验证

- **自动重启**：kill -9 后 Postmaster 在 `bgw_restart_time=5` 秒内重启 Demux（实测 3–9 秒）。
- **无记录丢失**：`count_parwal_records` 与实际写入数精确相符（S4: 10150 条记录）。
- **无重复记录**：多次 Crash 循环后，`verify_partition_wal` 返回 `t`（LSN 单调递增）。
- **并发写入安全**：Crash Recovery 后立即写入的新记录（S5: 2101..2300）与 redo handler 写入的积压记录（101..2100）无冲突，`partition_lsn` 全程单调。
- **干净重启恢复**：progress 文件路径正确恢复 5000 条积压（S2），`verify_partition_wal` 验证 LSN 连续。

---

## 最终结论

**Demux 高积压崩溃恢复测试: PASS**
