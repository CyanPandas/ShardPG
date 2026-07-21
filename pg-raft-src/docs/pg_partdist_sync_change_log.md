# pg-partdist-src 变更记录

> 本文档是 `pg_raft` 侧与 `pg_partdist` 侧之间的**交接台账**：凡是为了 Raft 而改动
> `pg-partdist-src`（即 raft 之外）的内容，都必须在此追加记录，写明修改时间、修改文件、
> 修改目的，以及对 `pg_raft` / failover / PartWAL 行为的影响。
>
> **目录：** 2026-07-10（初次同步与边界函数）· [2026-07-17 P0 全局分片身份](#2026-07-17)
> · [2026-07-18 P2 数据面边界函数](#2026-07-18) · [2026-07-20 运输层加固](#2026-07-20)
> · [2026-07-21 修复全新库 CREATE EXTENSION](#2026-07-21)


## 2026-07-10

### 1. 同步来源

- 同步目的：对齐师兄  `shardpg-3.0` 最新进度，使后续 `pg_raft` 对接真实的 PartWAL 同步写入与 follower replay 机制。

### 2. 同步后引入的关键能力

- 新增 `partwal_sync` 同步写入路径：
  - `PartWALInsert`
  - `PartWALFlush`
  - `PartWALAbort`
- 新增 `follower_partition_map(partition_id, local_relname, applied_part_lsn)` 元数据表。
- 新增 follower replay 设计文档：
  - `pg-partdist-src/docs/FOLLOWER_REPLAY_DESIGN.md`
- 新增 `patches/0001-add-wal-insert-hook.patch`，用于给 PostgreSQL 增加 `wal_insert_hook` 扩展点。

### 3. 为兼容新版 pg_partdist 做的配套调整

- 将 `pg-partdist-src/patches/0001-add-wal-insert-hook.patch` 应用到当前 `postgres-src`：
  - `src/backend/access/transam/xloginsert.c`
  - `src/include/access/xloginsert.h`
- 在容器内重新编译并安装 PostgreSQL，使 `pg-install` 暴露新的 hook 头文件和符号。
- 调整 `pg-partdist-src/Makefile`：
  - 不再写死 `/work/pg-install/bin/pg_config`
  - 默认优先使用当前仓库的 `../pg-install/bin/pg_config`

### 4. 验证情况

- `pg-partdist-src` 在宿主机路径下执行 `make clean && make` 通过。
- `pg-raft-src` 在同步后的环境下执行 `make clean && make` 通过。

### 5. 对后续开发的影响

- 后续若 `pg_raft` 需要读取副本追平状态，应优先复用：
  - `follower_partition_map.applied_part_lsn`
  - `read_all_headers(...)`
  - `verify_partition_wal(...)`
- 后续若再次修改 `pg-partdist-src` 中的任何文件，必须继续在本文档中追加记录，写明：
  - 修改时间
  - 修改文件
  - 修改目的
  - 对 `pg_raft` / failover / PartWAL 行为的影响

### 6. 本地追加修改（为 pg_raft 对接新版进度接口）

- 修改时间：2026-07-10
- 修改文件：
  - `pg-partdist-src/include/pg_partdist.h`
  - `pg-partdist-src/src/pg_partdist.c`
  - `pg-partdist-src/sql/pg_partdist--1.0.sql`
- 修改目的：
  - 新增只读接口 `get_partition_flush_lsn(partition_id)`
  - 新增只读接口 `get_follower_applied_part_lsn(partition_id)`
  - 让 `pg_raft` 不再直接依赖 `follower_partition_map` 表结构和 `read_all_headers(...)` 细节，而是通过 `pg_partdist` 暴露的稳定边界读取切主需要的进度信息
  - 为新增只读接口补齐 `executor/spi.h` 依赖，确保本地编译通过
- 影响说明：
  - `pg_raft` 可通过标准 SQL 接口查询本地分区最新持久化 `partition_lsn`
  - `pg_raft` 可通过标准 SQL 接口查询某节点本地 follower 的 `applied_part_lsn`
  - 后续若 `pg_partdist` 内部实现改变，优先保持这两个接口兼容，而不是让 `pg_raft` 继续感知内部表结构

### 7. 本地追加修改（补齐切主通知边界）

- 修改时间：2026-07-10
- 修改文件：
  - `pg-partdist-src/include/pg_partdist.h`
  - `pg-partdist-src/src/pg_partdist.c`
  - `pg-partdist-src/sql/pg_partdist--1.0.sql`
  - `pg-partdist-src/test/expected/08_schema_existence.out`
- 修改目的：
  - 为 `pg_raft` 的 `OP_PARTITION_PRIMARY` apply 阶段提供正式的 `partwal_notify_primary_switch(...)` 边界函数
  - 先把切主通知收敛到 `pg_partdist` 的稳定入口，后续再在该入口内接入真实的 follower replay / sender / 角色切换逻辑
  - 更新 schema existence 预期，纳入新增函数和已新增的进度接口
- 影响说明：
  - `pg_raft` 不再只是在日志里打印切主信息，而是会在 apply 成功后显式通知 `pg_partdist`
  - 当前通知函数先记录日志，不改变现有 PartWAL 行为语义
  - 后续若 `shardpg-2.0` 提供正式切主机制，可在不改 `pg_raft` 调用方的前提下直接替换 `pg_partdist` 侧实现

---

<a id="2026-07-17"></a>

## 2026-07-17（P0：全局分片身份映射）

对应提交 `93b4867`。属于“分区级 Raft 组”工程的第 0 期，是其余各期的硬前提。

### 8. 新增 `shard_identity` 映射表与 5 个解析函数

- 修改时间：2026-07-17
- 修改文件：
  - `pg-partdist-src/sql/pg_partdist--1.0.sql`（纯新增，+171 行）
  - `pg-partdist-src/tests/test_shard_identity_p0.sh`（新增回归）
- 修改目的：
  - 建立 `global_shard_id ↔ (node_id, local_oid, relfilenode)` 的跨节点一致映射。
  - **`global_shard_id` 直接采用 Citus `shardid`**：分片表命名为 `<rel>_<shardid>`，
    而 `pg_dist_shard` 在各节点同步，是天然的跨节点一致键，无需另造 id 分配器。
  - 新增 `partdist.shard_identity(global_shard_id PK, local_oid, relfilenode,
    local_relname, logical_relid)`，并为 `follower_partition_map` 增加
    `global_shard_id` 列。
  - 新增 5 个函数：`shard_global_id(oid)`、`rebuild_shard_identity()`（幂等，自带剪枝）、
    `register_shard_identity(oid)`、`local_partition_for_shard(bigint)`、
    `global_id_for_partition(oid)`。
- 实现要点（两个非显然的坑）：
  - 用 Citus 的 `shard_name()` 反查，避免自己解析表名后缀。
  - 扫描 `pg_class` 取分片表前必须 `SET citus.override_table_visibility = false`，
    否则 Citus 默认对普通连接**隐藏分片表**，扫不到任何行。（`to_regclass()` 不受影响。）
- 影响说明：
  - **对既有 PartWAL / demux / 写路径行为零影响**，本次只做纯新增。
  - 对 `pg_raft`：这是**跨节点比较 `applied_part_lsn` 的前提**。同一逻辑分片在各节点的
    本地 OID 不同（`pg_parwal/<OID>` 就是用它命名的），此前控制面 failover 直接比较不同
    节点的进度值，隐含地假设了 partition_id 全局一致，实际并不成立。
  - 后续所有数据面调用的约定：先用 `local_partition_for_shard(global_shard_id)` 把
    Raft 组 id 解析成**本节点**的 OID，再调用任何以 `partition_id` 为参数的边界函数。
- 验证：`test_shard_identity_p0.sh` 10/10（覆盖率、往返互逆、跨节点各异 OID、剪枝）；
  `run-raft-tests.sh` 仍 27/27（当时基线）。

---

<a id="2026-07-18"></a>

## 2026-07-18（P2：数据面 Raft 组的 parwal 边界函数）

对应提交 `1d5d780`（该提交同时含 pg_raft 侧的 P1+P2，此处只记 pg-partdist-src 部分）。

### 9. 新增 3 个 parwal 数据面边界函数

- 修改时间：2026-07-18
- 修改文件：
  - `pg-partdist-src/src/raft_boundary.c`（+251 行）
  - `pg-partdist-src/sql/pg_partdist--1.0.sql`（+47 行）
- 修改目的：让分区级 Raft 组能把 **真实 parwal 记录**当作 Raft entry 复制到副本。
  - `partwal_read_record(partition_id, partition_lsn)` → `(orig_lsn, rmid, info, xid, data)`：
    leader 侧按 `partition_lsn` 读出一条完整记录（头部字段 + 原始 WAL 字节）。
  - `partwal_follower_append(...)`：follower 侧把收到的记录原样落盘并 **fsync**，
    成功才允许 ack。
  - `follower_set_applied_part_lsn(partition_id, lsn)`：单调推进
    `follower_partition_map.applied_part_lsn`。
- 影响说明：
  - **“多数派提交”自此严格等价于“多数派已 fsync 持久化”** —— 因为 follower 是先落盘
    再 ack，而不是先 ack 再异步落盘。
  - `follower_partition_map.applied_part_lsn` 自此**第一次有了真实的 C 写入方**。
    在此之前它恒为占位 0，切主安全线里的跨节点进度比较实际上比的是常量。
  - 本阶段**不做 redo**：follower 只落盘字节 + 推进游标，堆表不变。物理回放是 P3。
- 验证：`raft_13_data_group_replication.sql`（字节级 md5 一致、进度真实推进、
  失去多数派时拒写）；基线升至 29/29。

---

<a id="2026-07-20"></a>

## 2026-07-20（运输层加固：补齐物理回放的前提）

对应提交 `d06c821`。起因是一次独立审查**推翻了此前“物理回放前提已具备”的结论** ——
发现 5 项缺陷全在运输层，被“平凡 apply 恰好幂等且单调”这一性质掩盖，一旦换成 redo 就会暴露。

### 10. 写入器支持按指定编号落盘、支持截断，fsync 语义收紧

- 修改时间：2026-07-20
- 修改文件：
  - `pg-partdist-src/src/wal/partition_wal_writer.c`（+225 行）
  - `pg-partdist-src/include/partition_wal_writer.h`（+23 行）
  - `pg-partdist-src/src/raft_boundary.c`（修改，+56/-22）
  - `pg-partdist-src/sql/pg_partdist--1.0.sql`（+13 行）
- 修改目的与具体改动：
  1. **新增 `AppendPartWALRecordAt(writer, expected_partition_lsn, ...)`**，
     并把原 `AppendPartWALRecord()` 改为 `expected = 0` 的包装。语义：
     - `expected == last + 1` → 正常写入；
     - `expected <= last` → **幂等 no-op**（重传去重），返回 false；
     - `expected > last + 1` → **ERROR**，绝不留空洞，交由 leader 回退补齐。
     **必要性**：原先 follower 用本地自增计数器编号，而 Raft 侧记录的
     `applied_part_lsn` 是 **leader 的**编号。两个编号空间只在“双方目录都从空开始且全程
     同步”时才巧合相等。本架构下每个节点同时是若干分区的 primary、又是另一些分区的
     secondary，同一 `pg_parwal/` 目录树下既有本地 demux 写入也有复制流，必然错位。
  2. **新增 `TruncatePartWALTo(partition_id, relfilenode, keep_upto_plsn)`** 及 SQL 包装
     `partwal_truncate_to(OID, BIGINT) → BOOLEAN`：按 `partition_lsn` 截断段文件
     （定位到首条超出的记录后 `ftruncate`，其后的段文件 unlink，最后重写 checkpoint）。
     **必要性**：切主后新 leader 会把**不同的**记录写到同一个 `partition_lsn` 上；
     旧字节若滞留在盘上，第 1 条新增的幂等去重反而会**保留错误内容**。
  3. **fsync 失败由 `WARNING` 升为 `ERROR`** —— 失败的 fsync 绝不能产生一个 ack。
  4. **修复 >256KB 记录绕过 flush/checkpoint 的缺陷**：超过缓冲区的记录走
     `AppendPartWALRecord` 的直写分支，写完即返回，`buf_used` 仍是 0；而
     `FlushPartitionWALWriter()` 原先在 `buf_used == 0` 时提前返回，导致该记录
     **既不 fsync 也不更新 checkpoint**，陈旧的 checkpoint 会让下一个写入器
     **重新发放同一个 `partition_lsn`**。
  5. **`partwal_follower_append` 签名变更**：新增 `p_partition_lsn BIGINT` 作为**第 2 个**
     参数（其后所有参数位置顺延），并改为返回该 `partition_lsn`。
- 影响说明：
  - **这是一次破坏性签名变更。** `pg_raft` 侧 `data_entry_store()` 必须传入描述符里的
    `partition_lsn`；`setup-raft.sh` 的 `ensure_boundary_functions` 与其 cleanup 段
    也必须同步更新（已做）。
  - 对**单机既有写路径**：`AppendPartWALRecord()` 行为不变（`expected = 0` 即原自增语义），
    但 fsync 失败与大记录两项修复会改变异常路径行为 —— 原先被静默吞掉的错误现在会报出来。
  - 对 `pg_raft`：重传不再产生第二份物理副本；日志截断能真正回收 `partition_lsn` 空间。
- 验证：`run-raft-tests.sh` 29/29 **连续三轮**；`test_shard_identity_p0.sh` 10/10。

---

<a id="2026-07-21"></a>

## 2026-07-21（修复全新库上 `CREATE EXTENSION` 失败）

### 11. 修正 `partwal_follower_append` 的 `COMMENT` 签名

- 修改时间：2026-07-21
- 修改文件：
  - `pg-partdist-src/sql/pg_partdist--1.0.sql`
  - `pg-install/share/postgresql/extension/pg_partdist--1.0.sql`（已安装副本，同步）
- 问题：2026-07-20 的加固给 `partwal_follower_append` 增加了第 2 个参数，但安装脚本里的
  `COMMENT ON FUNCTION partwal_follower_append(OID, PG_LSN, INTEGER, INTEGER, BIGINT, BYTEA)`
  仍写的是旧的 6 参数形式。该签名的函数已不存在，于是在**全新数据库**上执行
  `CREATE EXTENSION pg_partdist` 直接失败：

  ```
  ERROR:  function partwal_follower_append(oid, pg_lsn, integer, integer, bigint, bytea) does not exist
  ```

- 修改内容：把 `COMMENT` 的参数列表改为实际的 7 参数签名
  `(OID, BIGINT, PG_LSN, INTEGER, INTEGER, BIGINT, BYTEA)`；
  并为此前遗漏的 `partwal_truncate_to(OID, BIGINT)` 补上 `COMMENT`。
- 影响说明：
  - **仅影响全新安装**。已安装的扩展不受影响，函数本身与运行时行为均未改变。
  - **为什么回归 29/29 全绿却没发现**：回归环境总是复用已装扩展，而 `setup-raft.sh` 的
    `ensure_boundary_functions` 只是逐个补建函数，**不会重跑安装脚本**，因此安装脚本
    内部的不一致是完全静默的。
  - **教训（针对后续所有边界函数改动）**：改任何一个边界函数的签名，必须同步四处 ——
    `CREATE FUNCTION`、`COMMENT ON FUNCTION`、`setup-raft.sh` 的补建与 cleanup 段、
    `pg-install/share/postgresql/extension/` 下的已安装副本。测试计划应补一条
    “全新库 `CREATE EXTENSION` 冒烟检查”，否则这类错误无法被现有回归捕获。
- 验证：在 worker1 上新建库执行 `CREATE EXTENSION pg_partdist` 成功，
  并确认 `partwal_follower_append` / `partwal_truncate_to` / `partwal_read_record`
  三个函数以正确签名注册。
