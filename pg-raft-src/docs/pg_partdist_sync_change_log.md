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

### 12. `partition_map` 增加 `primary_term` 列（切主重构的任期栅栏）

- 修改时间：2026-07-24
- 修改文件：
  - `pg-partdist-src/sql/pg_partdist--1.0.sql`（`CREATE TABLE partition_map` 增列 + `COMMENT ON COLUMN`）
  - `pg-install/share/postgresql/extension/pg_partdist--1.0.sql`（已安装副本，经容器 `make install` 同步）
  - `pg-raft-src/setup-raft.sh`（已安装库补列：`ALTER TABLE ... ADD COLUMN IF NOT EXISTS primary_term BIGINT NOT NULL DEFAULT 0`）
- 目的：切主机制重构（计划文档 §13）。数据组自治选举出的新 leader 上报后，
  `OP_PARTITION_PRIMARY` 携带其选举任期；apply 端以
  `WHERE partition_map.primary_term <= EXCLUDED.primary_term` 做任期栅栏，
  拦下迟到/重复登记与旧"控制面指定"通道的 term=0 提案。
- 对 Raft/failover/PartWAL 行为的影响：
  - `primary_term = 0` 语义为"尚无数据组接管"，旧 failover/rejoin 通道只对这类分区生效
    （`pg_raft.c` 两处查询加了 `AND COALESCE(primary_term,0)=0` 门）；
  - `primary_term > 0` 的分区主副本变更**只**来自组内自治选举 + 上报登记；
  - apply 在真实 Citus shardid 上还会更新本地 `pg_dist_placement`（落路由层），
    合成分区无副作用。PartWAL 层无行为变化。
- 验证：raft_14/raft_15（见 run-raft-tests.sh 新增段）+ raft_01–13 回归 + 全新库
  `CREATE EXTENSION pg_partdist` 冒烟。

### 13. `PartWALFlush` 增加复制挂钩（事务 prepare 接线）

- 修改时间：2026-07-24
- 修改文件：`pg-partdist-src/src/wal/partwal_sync.c`
- 修改内容：`PartWALFlush()` 在本事务记录落盘 fsync 完成、全部锁释放之后，
  对本 backend 本事务涉及的每个分区调用 rendezvous 挂钩
  `"partdist_partwal_replicate_hook"`（函数指针由 pg_raft 的 `_PG_init` 注入；
  未装载/未启用 raft 时为空指针，零开销）。收集只统计
  `slot->backend_id == MyBackendId` 的分区，上限 64 个/事务。
- 目的：计划文档 §14 —— prepare 四步设计的第 2 步自动挂接。复制发生在
  [A]（parwal fsync）之后、[B]（pg_wal 提交 fsync）之前；挂钩内部未达多数派
  会 ERROR，事务在 prepare 中止。
- 对 Raft/failover/PartWAL 行为的影响：
  - 无数据组的分区（含合成分区、未纳管分片、raft_log 等普通表）行为与之前
    完全一致（挂钩内查不到 global_shard_id 或组即返回）；
  - 有数据组的分区：写入提交前自动逐条复制（一条 record 一次备份）；
    本节点非组 leader 时写入被拒（写栅栏）；
  - PartWAL 本身的落盘/fsync/编号语义无任何变化。
- 边界：group commit 让路窗口内（记录被其他 backend 顺带落盘）本事务不触发挂钩，
  增量在该分区下一次写入时补齐（详见计划文档 §14.3）。
- 验证：raft_16（自动复制/失多数派拒 prepare/恢复追平）+ raft_01–15 回归 +
  全新库 CREATE EXTENSION 冒烟。

## 2026-08-03

### 修复 group commit 让路窗口（DTX-2PC 第 1 步）

- 修改时间：2026-08-03
- 修改文件：`pg-partdist-src/src/wal/partwal_sync.c`
- 依据：`pg-partdist-src/docs/DTX_2PC_DESIGN.md` §9.1
- 背景：上一条（2026-07-24）末尾把"group commit 让路窗口"记为**边界**，
  说"增量在该分区下一次写入时补齐"。**这个定性是错的，它是正确性缺陷。**
  两条独立的成因：
  1. 触达集合只统计 `slot->backend_id == MyBackendId` 的槽位 ⇒ backend X
     顺带落盘 backend Y 的槽位时，**Y 的分区不在 X 的复制范围里**；
  2. Y 随后看到 `flushed_upto` 已覆盖自己，走提前返回分支 ⇒ **复制挂钩
     根本不被调用**。
  合起来：Y 的记录落了盘、事务提交成功、却没有任何人把它复制出去。
  无 2PC 时表现为复制延后；有 2PC 时协调者会据此写 COMMIT 决议 —— 丢数据。
- 修改内容：
  1. 触达集合改为 **per-backend 在 `PartWALInsert` 时登记**
     （`PartWALNoteTouched`，出 `PartWALCtl->lock` 后调用，内部 palloc），
     不再于 flush 时从环形缓冲区反推；数组按需增长，不再有 64 个/事务的上限；
  2. `PartWALFlush` 的**提前返回路径也调用复制挂钩**
     （`PartWALReplicateTouched`）。该路径上 `flushed_upto >= upto_lsn` 是在
     持锁状态、writer 析构（含 fsync）之后才推进的，所以 [A] 对本事务同样成立，
     复制是安全且必需的；
  3. 顺带修掉提前返回路径不释放 `partwal_pending` 的问题（此前该路径下
     捕获的 WAL 字节副本会一直挂到下一次 flush/abort，会话内累积）。
- 对 Raft/failover/PartWAL 行为的影响：
  - PartWAL 的落盘/fsync/编号语义**无任何变化**；
  - 未装载 pg_raft 时行为完全一致（挂钩指针为空即返回）；
  - 有数据组的分区：并发写入下"提交返回时多数派已持久化"从"最终一致"
    收紧为**真正的 prepare 语义**；
  - 提前返回路径新增一次 SPI（挂钩内部查 global_shard_id / flush lsn），
    这是必需的开销。
- 配套（pg_raft 侧，不属本台账但一并记）：`pg_raft_partwal_replicate()` 增加
  本组复制的串行化认领位，避免并发 backend 抢同一段 `partition_lsn` 重复提案
  烧掉 `RAFT_LOG_CAPACITY=128` 的环槽位；持有者 backend 消失时由等待者按
  `BackendPidGetProc()` 回收。
- 验证现状（**如实记录，验收未完成**）：raft_01–16 在修复版上无回退（46/1，
  唯一的 raft_15 失败在同构建重跑即通过，判为 flake）。新增
  `raft_17_concurrent_prepare_quorum`，但**尚未拿到可信的对照实验**：
  1. 最初的判据（burst 后断言 follower 逐字节追平）**测不出这个缺陷** ——
     漏掉的记录会被增量下界在下一次非提前返回的 flush 里补齐，终态几乎总是
     收敛，实测**修复前的构建照样通过**。判据已改为"失多数派 + 并发写 ⇒
     提交成功行数必须为 0"（见 `DTX_2PC_DESIGN.md` §9.1 的方框）。
  2. 新判据的验证被一个**新发现的 segfault** 阻断：6 路并发单行 INSERT 打同一
     Citus 分片必崩（3/3），**不需要 raft 组**、**修复前后都崩**。崩溃会重置
     shmem 组表，之后的 INSERT 因"查不到数据组"跳过挂钩并提交成功 ——
     与让路窗口的症状无法区分。详见 `DTX_2PC_DESIGN.md` §9.0。
  结论：本次改动的**正确性由代码路径本身确定**（提前返回分支不调挂钩、
  触达集合看不到被 peer 消费的槽位，两点在代码里都是无歧义的），
  但**端到端验收要等 segfault 修掉之后补做**。

### 并发写入路径三缺陷修复（同日，DTX_2PC_DESIGN.md §9.0）

- 修改时间：2026-08-03（同日第二批）
- 修改文件（pg-partdist-src 侧）：
  1. `src/wal/partwal_sync.c` —— `ReadRawWALRecordAt` 的 static reader 及
     `XLogReadRecord` 全程切 `TopMemoryContext`。旧代码在 PRE_COMMIT 回调里
     懒分配（事务级 context），事务结束即释放而 static 指针仍在 —— 第二次
     进入即 use-after-free，症状为 `pfree called with invalid pointer`
     （每次同一地址）或 SIGSEGV 拖垮整节点；无 raft 组的 6 路并发单行 INSERT
     即 3/3 复现，修复后 3/3 干净。顺带：`PartWALReadPage` 短读（count <
     reqLen）改按失败返回（原当成功，页缓冲尾部是未初始化栈内存）；
     group-commit 提前返回路径补 `FreePartWALPendingContent()`（原路径不释放，
     捕获的 WAL 字节副本会挂到下一次 flush/abort）。
  2. `src/raft_boundary.c` —— `partwal_truncate_to` 增加 `PartWALCtl->lock`
     互斥。原来完全无锁，截断段文件/改写 checkpoint 与持锁的 PartWALFlush
     追加者并发，实测出现 checkpoint.tmp/fileset.tmp rename ENOENT 竞争。
- 配套（pg_raft 侧，一并记录）：`data_propose_one` 对 `partwal_read_record`
  的结果逐列判 `isnull`（gdb backtrace 实锤的空指针解引用：函数查不到记录时
  返回的是**一行全 NULL** 而非零行，`SPI_processed==0` 挡不住）；`orig_lsn`
  字符串在 `SPI_finish` 前拷出（潜伏 UAF）；`discard_uncommitted_entry`
  不再调用 `partwal_truncate_to`（leader 侧失败回滚只撤日志条目，字节留作
  孤儿重新 propose —— 原截断会删掉并发事务已落盘的记录，致其静默漏复制提交，
  raft_17 阶段二实测 13/240 行丢数据）。
- 验证：无组并发 burst 修复前 3/3 崩、修复后 3/3 干净；raft_17 三阶段 PASS
  （① 并发终态多数派 3/3；② 确定性让路——被让路的长事务记录仍达多数派；
  ③ 失多数派提交行数=0），全程无崩溃。**真对照实验**（同用例仅换 .so，
  构建身份经 nm 符号验证）：让路窗口未修的构建在阶段二确定性失败——
  P 提交成功而其 2 条记录只在 leader（leader=43, follower=41,41），
  修复版同位置 43/43/43。终态判据测不出该缺陷（阶段一两个构建都过，
  被增量下界自愈掩盖），详见 DTX_2PC_DESIGN.md §9.1 方框。

### ⚠️ 本轮定位踩的工作流坑（务必留意）

**docker cp 保留宿主机源文件 mtime**：cp 进容器的 .c 若比容器里的 .o 旧，
make 会静默跳过重编，install 装的还是旧 .so —— A/B 对照实验跑的根本不是
你以为的构建。本轮曾因此把修复版当对照版跑了两轮、写下过错误结论后返工。
规程：cp 后必须 touch 再 make；构建诊断过滤用 `grep -E ": (error|warning):"`
（避免匹配 gcc 命令行里的 -Werror=vla）；换构建后必须用 nm 特征符号验证
产物身份，不能只信脚本 echo。

### DTX-2PC 第 2 步：成员集显式化（同日第三批）

- 修改时间：2026-08-03
- 修改文件：**仅 pg-raft-src 侧**（`src/raft_consensus.c`），pg-partdist-src 无改动。
  本条记录在台账里是因为它改变了 `partdist.partition_map` 的**用途**：
  该表从此不只是"切主登记结果"，还是**数据组成员集的权威来源**——
  pg_raft 会在所有 SPI 可用的路径上读它来导出成员集。
  改动 partition_map 的 schema / 语义时必须一并考虑 pg_raft 的这个依赖。
- 依据：`pg-partdist-src/docs/DTX_2PC_DESIGN.md` §9.2
- 缺陷：`n_members == 0` 被重载为"全体节点"。对控制面组（组 0）成立，对数据组
  永远不成立（分片副本集必然是全体节点的真子集）。实测（9 节点，SQL 默认的
  `p_members = NULL` 建组）：cluster_size 算成 9、多数派算成 5；该组向全集群广播
  RequestVote，非副本节点 hearsay 建组并参与投票；真正持有数据的 worker 反被挤成
  term=0 的 follower，分片彻底不可用；三个副本全在也写不进去。本质危险是同一组
  在不同节点上有两套不相交的多数派定义（2/3 vs 5/9），Leader Completeness 失去
  交集保证，非副本节点还能赢得它没有数据的分片的领导权。
- 修复：① 数据组空成员集语义改为"**未知**"并 fail-stop，但**只剥夺主动参与**
  （不竞选/不当选/不提案），被动应答（投票、落盘+ack）照旧；
  `cluster_majority()` 在规模未知时返回不可能达到的票数作 fail-closed 兜底；② 成员集从 `partdist.partition_map` 本地导出
  （`{primary_node} ∪ secondary_nodes` 剔除协调节点），控制面已把它复制到每个
  节点，因此无需新增 RPC —— 这正是"成员集必须来自控制面下发、不能从报文里推断"；
  ③ `pg_raft_group_create` 的 NULL 成员集不再静默建组，导不出来即报错并给 hint；
  ④ prepare 路径成员集未知时 ERROR 中止事务，而不是跳过复制照常提交。
- 对 Raft/failover/PartWAL 行为的影响：
  - 控制面组（组 0）语义**逐字节不变**（空成员集仍表示全体节点）；
  - 已显式建组的数据组行为不变（raft_13/14/15/16/17 全绿）；
  - 新增的失败模式是 fail-stop 的：成员集导不出来时宁可该组不可用，
    也不允许两套多数派定义共存。
- 残留边界：全新分片的**首次**选举发生在 partition_map 有登记之前（登记由当选
  leader 上报产生，鸡生蛋），首次建组仍须显式给成员集；成员**变更**（扩缩副本）
  仍缺 joint consensus，正解仍是 `OP_CONFIG_CHANGE` + 配置作为日志条目复制。
- **踩过的坑（务必留意）**：初版把 `pg_raft_rpc` / `pg_raft_append_entries` 也
  一并拒绝，raft_14/15/16/17 **全挂**。原因是数据组的引导流程（计划 §11.5.2）
  依赖 hearsay：只在 placement 节点先建组，其余成员靠收到 RV 自动建组并投票，
  之后才补 create 固化——而首次选举时 partition_map 尚无登记（登记正是当选
  leader 上报产生的）。正确边界是"主动 vs 被动"：候选人/leader 只向自己成员集
  里的节点发报文，多数派算术是**对方**做的，被动应答不会让谁算错；危险的只有
  成员集未知的节点**主动竞选**。所以只堵 `group_tick`，RPC 路径改为机会性补齐、
  补不上照常应答。
- **既有用例的连带调整**：raft_12 的夹具依赖的正是被修掉的不安全行为——
  ① 以空成员集建组（现已报错拒绝）⇒ 改为显式 `ARRAY[2,3,4]`；
  ② 在**控制面 leader（常态是 master）**上断言"两个数据组均可见"，而 master
  永不作数据副本，旧写法能过只因空成员集会让组经 hearsay 撒到全集群
  ⇒ 断言改到数据组成员节点上跑。顺带修掉"非 leader 提交被拒"挑到组外节点的
  问题（组在那里根本不存在，propose 同样返回 0，属"对的结果、错的原因"）。
- 验证：9 节点全量 **49/49 全绿**（raft_01–18）。新增 `test/raft_18_membership_explicit.sh`（四条确定性判据 A/B/C/D）。
  真对照（同用例仅换 .so，nm 验证构建身份）：修复前 A 处 `group_create` 返回 `t`
  而非报错。**测试方法论**：C 用例最初硬断言"worker1 当选"，实测 worker3 先超时
  先当选而误报——Raft 不保证哪个成员赢，已改为动态发现 leader 与待停 follower。

### DTX-2PC 第 3 步：记录格式与 flags 端到端保真（同日第四批）

- 修改时间：2026-08-03
- 修改文件（pg-partdist-src 侧）：
  - `include/partition_wal_header.h` —— 显式化记录分类位：`PARTWAL_FLAG_DATA(0x01)`
    / `MARKER(0x02)` / `CTRL(0x04)`（FRD 预留）/ **`DTX(0x08)`**，并加
    `PartWALRecordIsData()`。**兼容性：flags==0 视同 DATA**（存量段文件都是 0），
    新写入的数据记录显式带 DATA 位。
  - `include/dtx_record.h`（新增）—— `DtxRecordKind` 与 `DtxRecordPayload`
    （dtxid / coord_gsid / commit_ts / verdict / participants[]），
    以及头字段取值约定：`rmid=RM_XACT_ID`（仅可读性）、`info=kind`（**不是**
    XLog info）、**`orig_lsn` 恒为 0**（不是 WAL 记录，回放侧禁止拿它盖页 LSN）。
  - `include/partition_wal_writer.h` + `src/wal/partition_wal_writer.c` ——
    `AppendPartWALRecord{,At}` 增加 `flags` 参数（此前头部 flags 恒写 0）。
  - `src/raft_boundary.c` —— `partwal_read_record` 增加 `OUT flags`；
    `partwal_follower_append` 增加 `p_flags`；新增
    `partwal_append_dtx_record` / `partwal_read_dtx_record`。
  - `sql/pg_partdist--1.0.sql` + `pg-install/share/postgresql/extension/` 副本
    + `pg-raft-src/setup-raft.sh` 的 ensure/cleanup 段（四处同步点全覆盖）。
- 目的：DTX 记录与 DATA 记录共用同一个 `partition_lsn` 序号空间和同一条复制通道，
  只靠头部 flags 区分。**复制通道丢了 flags，DTX/标记记录到 follower 就退化成
  DATA 记录，升主回放时被当作原始 WAL 字节喂给 `rm_redo`** —— PANIC 或静默堆损坏。
- 对 Raft/failover/PartWAL 行为的影响：
  - PartWAL 的落盘/fsync/编号语义**无变化**；DATA 记录只是头部多了个 flags 位；
  - pg_raft 侧：`data_propose_one` 的描述符 JSON 增加 `"flags"`，
    `data_entry_store` 解析后透传给 `partwal_follower_append`。
    pg_raft **有意不依赖** pg_partdist 头文件，所以它只镜像了一个
    `PARTWAL_FLAG_DATA=1` 常量作"描述符缺 flags 时的兜底"，其余**不透明透传**。
- **踩过的坑（都会静默失败，务必留意）**：
  1. `CREATE OR REPLACE` **改不了返回类型**（`partwal_read_record` 加了 OUT 列），
     必须先 DROP；
  2. 这两个函数是 **pg_partdist 扩展成员**，直接 `DROP FUNCTION` 被
     `cannot drop function ... because extension pg_partdist requires it` 拒绝，
     必须先 `ALTER EXTENSION pg_partdist DROP FUNCTION ...` 解除归属；
  3. 上述两个错误发生在 `setup-raft.sh` 的 `ON_ERROR_STOP=0` + 输出重定向段里，
     **无声无息**——结果是既有库留下新旧两个 `partwal_follower_append` 重载、
     `partwal_read_record` 根本没更新。cleanup 段已改写为 DO 块（先 ALTER
     EXTENSION 再 DROP，真失败时 `RAISE WARNING`），顺带折叠了几条一直在静默
     失败的历史 DROP 行；
  4. `pg_partdist` 在 `shared_preload_libraries` 里，`make install` 后**必须重启
     节点**，否则 `CREATE EXTENSION` 报 `could not find function ... in file`
     （看着像符号没导出，其实是 postmaster 还映射着旧 `.so`）。
  5. `cleanup_raft_loose_objects` 第一句是 `DROP EXTENSION pg_raft CASCADE`，
     **连带删掉 `partdist.raft_log`**——单独调它而不跟着跑
     `install_raft_sql_on_node` 会把控制面日志清空。后果比想象中重：
     已重启的节点从空表恢复成 index=0，未重启的节点 shmem 环仍停在旧 index，
     而项目**没有 InstallSnapshot**，落后超 `RAFT_LOG_CAPACITY=128` 的节点
     **永远追不上** —— 本次表现为只有 coordinator 的 group0 是 0/0/0、其余
     节点 888，登记到不了 master、`pg_dist_placement` 不切，
     raft_06/09/10/14/15/18/19 集体失败（**与本步代码无关**，是环境损坏）。
     要刷新边界函数请调 `install_raft_sql_on_node <port>`（内部先 cleanup 再重建）。
     控制面重置的正确顺序见 raft 计划 §12.3.B.6 旁注与环境记忆；关键是
     **先 `ALTER SYSTEM SET pg_raft.raft_enabled=off` 再清表**——直接 TRUNCATE
     无效，TopologyMonitor 每秒就把新条目写回去了。
- 附带：`setup-raft.sh` 也改为**拓扑自适应**（与 `run-raft-tests.sh` 同款探测），
  不再写死四节点，`PEERS` 按实际节点数生成。
- 验证：9 节点全量 **50/50 全绿**（raft_01–19）。新增
  `test/raft_19_dtx_record_format.sh`（A 全新库冒烟+签名 / B leader 侧格式与载荷
  往返 / C DATA 不被误判为 DTX / D **follower 侧 flags 保真**）。

### DTX-2PC 第 4a 步：决议层本体（2026-08-03）

- 修改文件（pg-partdist-src 侧）：`sql/pg_partdist--1.0.sql` 新增
  `partdist.dtx_decision` 决议索引表（+ `pg-install` 已安装副本、
  `setup-raft.sh` 的 `CREATE TABLE IF NOT EXISTS` 补建段）。
- 决议逻辑本体在 pg_raft 侧（`src/raft_consensus.c`：`dtx_decide` /
  `dtx_status` / `dtx_write_decision` / apply 路径的索引维护），
  但**它写的是 pg_partdist 的表**，因此记在本台账：
  改 `partdist.dtx_decision` 的 schema 必须同时看 pg_raft 的 apply 路径。
- 语义要点（`DTX_2PC_DESIGN.md` §6）：
  - **决议记录在协调组达多数派持久化 = 全局提交点**。`dtx_decide` 内部
    `replicate_group_upto()` 返回才算数（follower 先 fsync 再 ack，
    故"多数派提交"严格等价于"多数派已持久化"）。
  - 决议**必须连同它之前的增量一起复制**，不能只 propose 自己那一条 ——
    follower 的 `AppendPartWALRecordAt` 遇到 plsn 空洞会 ERROR、拿不到 ack。
    为此把原 `pg_raft_partwal_replicate` 里的增量循环抽成
    `replicate_group_upto()`，prepare 路径与决议路径共用。
  - **索引表由每个组成员的 apply 路径各自维护**（`data_entry_apply` 里按
    `flags & DTX` 且 `info == DECISION` 判定，用 `partwal_read_dtx_record`
    从本节点刚落盘的字节解析）。这正是"协调权随 Raft 选举自动转移"的落地点：
    协调组切主后新 leader 手里天然有全表，`dtx_status` 立刻可答。
  - **推定中止**：`dtx_status` 查无决议时**先写一条 ABORT 决议并达多数派再答复**，
    否则"问的时候没有、答完之后原提交路径又把 COMMIT 写进去"会让同一事务出现
    两个矛盾结论。写完之后决议槽被占，后到的 COMMIT 被一次性检查挡下。
- **本步未做的部分（4b）**：内核补丁 0004 的 master 挂点、关 Citus 2PC 恢复。
  暂缓理由见 `DTX_2PC_DESIGN.md` §9.3 的落地评估（要重编整个 PG 并连
  `pg-install` 一起提交，风险与代价都高，而决议正确性与挂点无关）。
  **在自动接线落地之前，跨分区事务并不会真的走 2PC** —— 别把 4a 的绿灯
  读成"2PC 已上线"。
- 验证：9 节点全量 **51/51 全绿**（raft_01–20）。新增 `test/raft_20_dtx_decision.sh`
  （A 非 leader 拒绝且不留痕 / B 决议在全部成员上均在 / C 决议槽一次性 /
  D 推定中止 / E 协调组切主后仍可查）。
