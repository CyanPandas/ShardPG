# pg-partdist-src 变更记录


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
