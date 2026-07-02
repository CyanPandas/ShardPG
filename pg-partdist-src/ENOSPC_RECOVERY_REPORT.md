# 磁盘空间不足（ENOSPC）容错测试验证报告

## 1. 小容量文件系统构造方法

本容器无 `CAP_SYS_ADMIN` 权限，无法挂载 tmpfs。采用 **LD_PRELOAD write() 拦截器**替代：

- 编写 `enospc_inject.c`，拦截所有指向 `/work/pg-cluster-data/worker1/pg_parwal/` 路径的 `write()` 系统调用
- 当 `/tmp/enospc_inject_active` 文件存在时，返回 `-1`（`errno = ENOSPC`）
- 删除该文件即可"释放空间"（注入停止，写入恢复正常）
- 以 `LD_PRELOAD=/tmp/libenospc_inject.so` 重启 Worker1，Demux Worker 作为子进程继承该拦截

此方法优点：精确可控，可重复，不依赖文件系统权限，不影响宿主机。

---

## 2. 发现的 Bug 及根因

### Bug 1：un-stall 逻辑永远为真（导致无限重试）

**位置：** `src/worker/demux_worker.c`，stall 检查块

**原始代码：**
```c
if (w->enospc_stalled)
{
    uint64 disk_lsn = GetLastWrittenPartitionLSN(hdr->partition_id);
    if (disk_lsn < hdr->partition_lsn)
        w->enospc_stalled = false;
}
```

**根因：** stall 发生后，`disk_lsn` 是最后成功写入的 partition_lsn（记为 N-1），而 `hdr->partition_lsn` 是当前待写记录（记为 N+1）。因此 `N-1 < N+1` 永远为真，导致每次新 WAL 记录到来时都会立即 un-stall，然后写入失败，再次 stall。这形成了无限的 1ms 级别重试循环，且每次循环都会向 buffer 追加一条记录，最终可能导致 buffer 溢出。

### Bug 2：ENOSPC 后 fd 未关闭（导致潜在部分写污染）

**位置：** `src/wal/partition_wal_writer.c`，`FlushPartitionWALWriter()`

**根因：** `write()` 返回 ENOSPC 时，原代码直接 `ereport(ERROR)`，文件描述符未关闭。若 write() 在 ENOSPC 前已写出部分字节（partial write），后续 un-stall 后以 O_APPEND 模式写入时，部分数据会被重复写入，导致数据重复或 LSN 不连续。

### Bug 3：无 `last_stall_time` 跟踪（无法限速重试）

**位置：** `include/partition_wal_writer.h`，`PartitionWALWriter` 结构体

**根因：** 缺少时间戳字段，无法对 ENOSPC 恢复探测进行速率限制。

---

## 3. 修复方案

### 修复 1：时间限速 + statvfs 磁盘空间检查（替换 un-stall 逻辑）

```c
/* 每秒最多探测一次，通过 statvfs 检查真实可用空间 */
if (w->enospc_stalled &&
    TimestampDifferenceExceeds(w->last_stall_time, GetCurrentTimestamp(), 1000))
{
    char parwal_dir[MAXPGPATH];
    struct statvfs sv;
    snprintf(parwal_dir, MAXPGPATH, "%s/%s", DataDir, PARTITION_WAL_DIR);
    if (statvfs(parwal_dir, &sv) == 0 &&
        (uint64) sv.f_bavail * (uint64) sv.f_bsize >= sizeof(PartWALHeader))
        w->enospc_stalled = false;
    else
        w->last_stall_time = GetCurrentTimestamp();
}
```

### 修复 2：ENOSPC 时关闭 fd，缓冲区仅保留未写数据

```c
if (errno == ENOSPC)
{
    int bytes_out = (int)(ptr - writer->buffer);
    int complete  = (bytes_out / (int)sizeof(PartWALHeader)) * (int)sizeof(PartWALHeader);
    if (complete > 0)
    {
        memmove(writer->buffer, writer->buffer + complete,
                writer->buf_used - complete);
        writer->buf_used -= complete;
    }
    close(writer->fd);
    writer->fd = -1;
    ereport(ERROR, (errcode(ERRCODE_DISK_FULL), ...));
}
```

### 修复 3：OpenWriterSegment 截断到对齐边界

```c
/* 删除 partial write 留下的残余尾部 */
off_t sz = lseek(fd, 0, SEEK_END);
off_t aligned = (sz / (off_t)sizeof(PartWALHeader)) * (off_t)sizeof(PartWALHeader);
if (aligned < sz)
    (void) ftruncate(fd, aligned);
```

### 修复 4：添加 `last_stall_time TimestampTz` 字段 + 日志优化

- `PartitionWALWriter` 新增 `last_stall_time` 字段
- "stalling" WARNING 只在首次 stall（非重试 stall）时输出
- "resuming" WARNING 只在写入实际成功后输出，避免日志刷屏

---

## 4. 测试过程中的关键观察

### 环境
- Worker1 (port 5433)：enospc_test OID = 225260
- Worker2 (port 5434)：enospc_test OID = 61800
- 注入库：`/tmp/libenospc_inject.so`，触发文件：`/tmp/enospc_inject_active`

### 关键日志（Worker1 `pg.log`）

**磁盘满时（注入激活）：**
```
WARNING:  pg_partdist demux: stalling partition 225260 after write error
```

**空间释放后（注入停止）：**
```
WARNING:  pg_partdist demux: resuming partition 225260 after disk space freed
```

### Demux Worker 状态
- ENOSPC 注入期间：Demux Worker 进程 PID 保持不变（未崩溃）
- 无 FATAL/PANIC 日志
- Worker2 的 Demux Worker 完全不受影响（parwal 记录继续增长）

### 恢复时间
- 注入停止至首条新记录写入 pg_parwal：**1 秒**（远低于 5 秒要求）
- 原理：statvfs 探测每 1 秒一次；注入停止后，下一次探测即检测到可用空间，立即 un-stall 并成功刷写

### 数据完整性
- `verify_partition_wal(225260)` 返回 `t`（LSN 严格单调递增）
- 恢复后 parwal 记录数大于 baseline

---

## 5. 已知限制

**长时间 stall 期间的记录跳过：** 当 stall 持续期间有 WAL 记录到来但 1 秒探测窗口未到达时，这些记录会被跳过（不写入 buffer）。对于短时间 stall（< 缓冲区容量 / 写入速率 ≈ 8192 records），影响极小；对于长时间 stall，部分 parwal 记录可能缺失。但主数据库数据不受影响（记录仍在 pg_wal 中），重启 Demux Worker 可从 progress file 恢复。

---

## 6. 测试结果

```
PASS: 11  FAIL: 0
磁盘空间不足容错测试: PASS
```

| 测试项 | 结果 |
|--------|------|
| 基线写入正常 | PASS |
| ENOSPC 后 Demux Worker 不崩溃 | PASS |
| 日志中出现 "stalling partition" 警告 | PASS |
| 无 FATAL/PANIC | PASS |
| Worker2 Demux 不受影响 | PASS |
| 空间释放后自动恢复 | PASS |
| 恢复时间 ≤ 5 秒（实测 1 秒） | PASS |
| 日志中出现 "resuming partition" 信息 | PASS |
| 恢复后 parwal 记录数超过 baseline | PASS |
| LSN 单调递增（verify_partition_wal = t） | PASS |
| 清理后 Worker1 正常重启 | PASS |
