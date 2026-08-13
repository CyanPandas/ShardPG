# PostgreSQL 内核补丁

`postgres-src` 是指向官方 `github.com/postgres/postgres.git` 的 submodule，无法
向上游推送自定义改动。本项目依赖的几个内核扩展点都是对 PostgreSQL 的直接修改，
因此以 patch 文件的形式保存在这里，作为**可复现的构建依赖**。

**基线 commit**：`e9067809ee722ed18152baed2191eaf92d770b29`
（`postgres-src` 的 `.gitlink` 指向的同一个 commit，`REL_16_STABLE`，PostgreSQL 16.14）

## 补丁清单（必须按此顺序全部应用）

| 顺序 | 补丁 | 加的扩展点 | 谁在用 |
|---|---|---|---|
| 1 | `0001-add-wal-insert-hook.patch` | `wal_insert_hook`（`xloginsert.c/.h`） | `PartWALInsert()` 捕获 WAL 记录写 pg_parwal |
| 2 | `0001v2-wal-insert-hook-smgr.patch` | 上者的**增量**：让 hook 对 `RM_SMGR` 这类**无块引用**的记录也触发（`nblocks == 0`） | 物理回放副本必须捕获 smgr create/truncate，否则 VACUUM 尾部截断后副本文件静默分叉（FRD §5.2） |
| 3 | `0002-flushbuffer-lsn-exempt-hook.patch` | `buffer_flush_lsn_exempt_hook`（`bufmgr.c/.h`） | 物理回放副本的页携带 **origin 坐标**的 LSN，本地 pg_wal 没有那个位置；`FlushBuffer` 对这类页跳过 `XLogFlush`（FRD §8.3） |
| 4 | `0004-pre-record-commit-hook.patch` | `pre_record_commit_hook`（`xact.c/.h`） | DTX-2PC 的决议挂点：在 `CommitTransaction()` 里、**全部** `XACT_EVENT_PRE_COMMIT` 回调之后、`RecordTransactionCommit()` 之前（`DTX_2PC_DESIGN.md` §9.3） |
| 5 | `0005-shard-xid-stamping.patch` | `shard_relation_xid_hook`（新头文件 `access/shard_stamp.h`；改 `heapam.c`/`heapam_xlog.h`/`pruneheap.c`） | TX-TSO-MVCC P1：分片表元组 xmin/xmax 改盖**分片 xid**（记录头 xid 保持原生）；insert/multi_insert/update 主数据末尾追加 4 字节分片 xid 尾缀（标志位 `XLH_INSERT_SHARD_XID`/`XLH_UPDATE_SHARD_XID` = 1<<7），redo 从末尾取号保证三方页面逐字节一致；delete/lock 的 xmax 走既有记录体字段零格式变更；分片表禁 on-access 剪枝（P1_PRECHECK 结论 D）、禁行锁/COPY FREEZE/推测插入（`TX_TSO_MVCC_DEV_PLAN.md` T1.4/T1.5） |
| 6 | `0006-shard-visibility-hooks.patch` | `shard_visibility_hooks`（扩展 `shard_stamp.h`；改 `heapam_visibility.c`/`heapam.c`） | TX-TSO-MVCC P1（T1.6/T1.7 内核部分）：六个 Satisfies* 入口按钩子分叉——MVCC/Self/Dirty/Update 由扩展裁决（分片 xid 绝不进原生 clog/procarray），VacuumHorizon/HistoricMVCC 对分片元组防御性 ERROR，Toast 不分叉（原生路径对正常元组不查 clog，天然安全）；heap_delete/heap_update 冲突等待臂：分片 xmax 经 `xmax_wait` 钩子翻译成持有者**原生 xid** 再等待，然后 `goto l1/l2` 重评（判定收敛在分叉的 SatisfiesUpdate）；`compute_new_xmax_infomask` 三处调用点 + 新元组 xmax 继承对分片表强制 `HEAP_XMAX_INVALID` 简单路径——TM_Ok 到达即旧 xmax 已死且无锁者，原生机器会拿分片 xid 误组 multixact（实测缺陷：`new multixact has more than one updating member`）；heap_delete/update 的 `PageSetPrunable` 对分片表改用原生 xid——redo 用记录头原生 xid 设此提示，普通路径若用分片 xid 则页头分叉（T1.9 实测） |
| 7 | `0007-shard-xact-record-xids.patch` | `shard_xact_wal_list_hook` + `shard_xact_redo_hook`（扩展 `shard_stamp.h`；改 `xact.h`/`xact.c`/`rmgrdesc/xactdesc.c`） | TX-TSO-MVCC P2（T2.2）：`xl_xact_commit/abort` 增 xinfo **bit 9** 可选块，携带本事务 (分片oid, 分片xid) 对列表（无分片写时记录体逐字节零变化）；`xact_redo_commit/abort` 尾部钩子把列表交扩展重做分片 clog 落账（幂等）——闭环"提交/中止记录落盘后、clog 标记前崩溃"的窗口；`ParseCommit/AbortRecord` 解析新块，desc 打印 `shard xids: oid/xid`（pg_waldump 可辨）。正常路径 COMMITTED/ABORTED 标记在扩展 XACT_EVENT_COMMIT/ABORT 回调里做——该回调先于行锁释放（xact.c CommitTransaction 实测行序），等待者唤醒即见终态；崩溃隐式中止无记录，由 T2.4 无主 RUNNING 认领兜底 |
| 8 | `0008-shard-vacuum-read-hook.patch` | `shard_vacuum_read_hook`（扩展 `shard_stamp.h`；改 `heapam_visibility.c`） | TX-TSO-MVCC P2（T2.6）：`HeapTupleSatisfiesVacuumHorizon` 的分片分支从 0006 的防御性 ERROR 改为交扩展裁决（ANALYZE 读侧，"只判不收"：committed-deleted 给 RECENTLY_DEAD 且分叉点配新鲜原生 xid 作 dead_after，一切提升检查落保守分支；中止插入给 DEAD 只进统计）；钩子未装时保持 0006 fail-closed ERROR；回收类动作者不变（剪枝屏蔽、VACUUM/CLUSTER/CIC 仍禁，autovacuum 由扩展侧硬盾拦截） |

> **为什么 0004 不能用 XactCallback 代替**：回调是 LIFO 顺序，后加载的扩展反而先
> 被调用，因此扩展无法表达"在**所有**其它扩展的 PRE_COMMIT 动作都完成之后再做
> 一件事"。而 Citus 强制要求自己排 `shared_preload_libraries` 第一位，调换加载
> 顺序这条路是堵死的。跨分区事务的决议恰恰必须发生在"Citus 发完全部
> `PREPARE TRANSACTION` 之后、本地 commit record 落盘之前"。

> 3 号位空缺：`0003` 曾用于一版被放弃的尝试，编号保留不复用，避免与历史记录混淆。

## 0001v2-wal-insert-hook-smgr.patch（R1）

让 `RM_SMGR` 这类**无块引用**的记录也能触发钩子（以 `nblocks == 0` 调用）。
没有它，`XLOG_SMGR_TRUNCATE` 永远捕获不到 —— `storage.c` 只 `XLogRegisterData`
不 `XLogRegisterBuffer`，`blocks[]` 恒空，而 FRD §5.3 要求捕获它。

## 0002-flushbuffer-lsn-exempt-hook.patch（R1）

`FlushBuffer` 对命中副本文件集合的页跳过 `XLogFlush(页LSN)`。物理回放的副本页
携带的是 **leader 坐标**的 LSN，本地 pg_wal 里根本没有那个位置（FRD §8.3）。
缺它则刷脏时 `XLogFlush` 会等一个永远不会到来的 LSN。

> **★★ 0001/0001v2/0002 加上 0005/0006/0007/0008 都是 `pg_partdist` 的编译期硬依赖，缺
> 任何一个都编不过。** 实测缺 0002 时的报错：
> `src/pg_partdist.c: error: 'buffer_flush_lsn_exempt_hook' undeclared`；
> 缺 0005 时 `src/shard_xid.c` 会因找不到 `access/shard_stamp.h` 直接编译失败；
> 缺 0006 时 `src/shard_visibility.c` 会因 `ShardVisibilityHooks` 未定义编译失败
> （2026-08-13 起，两文件分别实现这两个钩子）；
> 缺 0007 时 `src/shard_xid.c`/`src/shard_clog.c` 会因
> `shard_xact_wal_list_hook`/`shard_xact_redo_hook` 未声明编译失败；
> 缺 0008 时 `src/shard_visibility.c` 会因 `shard_vacuum_read_hook` 未声明编译失败。

## 补丁与仓库里 `pg-install/` 的关系（复现路径的关键）

`pg-raft-src/reproduce-env.sh` **不打补丁、也不重编 PostgreSQL**：它把仓库里
**已经打过补丁并编译好**的 `pg-install/` 整棵树 `docker cp` 进容器，然后只编
`pg_partdist` / `pg_raft` 两个扩展。也就是说——

> **能不能从零 clone 复原，完全取决于仓库里这份 `pg-install/` 是不是打过补丁的构建。**

2026-08-04 踩过一次：仓库里那份是 8/1 的构建，**只含 0001，缺 0001v2 和 0002**，
于是 `reproduce-env.sh destroy` 之后 `up` 出来的环境编不过 pg_partdist
（销毁旧容器之后才发现，只能在容器里手工重编 PostgreSQL 抢救）。
2026-08-05 已把容器里那份带全部三个补丁的构建同步回仓库。

**改动内核补丁之后必须做的事**（否则复现路径又会退化）：

```bash
# 1. 在容器里重编 PostgreSQL（源码 = postgres-src 基线 + 三个补丁）
docker exec -u postgres <容器> bash -lc '
  cd /work/postgres-src &&
  ./configure --prefix=/work/pg-install --with-openssl --with-icu --with-readline &&
  make -j2 && make install'

# 2. ★ 把结果同步回仓库并提交，否则仓库里还是旧构建
docker cp <容器>:/work/pg-install /tmp/pg-install-new
cp /tmp/pg-install-new/bin/postgres                                   pg-install/bin/
cp /tmp/pg-install-new/include/postgresql/server/storage/bufmgr.h     pg-install/include/postgresql/server/storage/
cp /tmp/pg-install-new/include/postgresql/server/access/shard_stamp.h pg-install/include/postgresql/server/access/   # 0005 新增（0006/0007/0008 改动）
cp /tmp/pg-install-new/include/postgresql/server/access/heapam_xlog.h pg-install/include/postgresql/server/access/   # 0005 改动
cp /tmp/pg-install-new/include/postgresql/server/access/xact.h        pg-install/include/postgresql/server/access/   # 0007 改动
# （新增/改动的头文件按补丁涉及范围补齐）
```

**自检**（`reproduce-env.sh up` 之前值得先跑一遍，比编译失败早得多）：

```bash
grep -c wal_insert_hook               pg-install/include/postgresql/server/access/xloginsert.h  # 0001  应 >0
nm -D pg-install/bin/postgres | grep -c wal_insert_hook                                          # 0001v2 应 =1
grep -c buffer_flush_lsn_exempt_hook  pg-install/include/postgresql/server/storage/bufmgr.h      # 0002  应 >0
nm -D pg-install/bin/postgres | grep -c buffer_flush_lsn_exempt                                  # 0002  应 =1
grep -c shard_relation_xid_hook       pg-install/include/postgresql/server/access/shard_stamp.h  # 0005  应 >0
nm -D pg-install/bin/postgres | grep -c shard_relation_xid_hook                                  # 0005  应 =1
grep -c ShardVisibilityHooks          pg-install/include/postgresql/server/access/shard_stamp.h  # 0006  应 >0
nm -D pg-install/bin/postgres | grep -c shard_visibility_hooks                                   # 0006  应 =1
grep -c XACT_XINFO_HAS_SHARD_XIDS     pg-install/include/postgresql/server/access/xact.h         # 0007  应 >0
nm -D pg-install/bin/postgres | grep -cE 'shard_xact_wal_list_hook|shard_xact_redo_hook'         # 0007  应 =2
grep -c shard_vacuum_read_hook        pg-install/include/postgresql/server/access/shard_stamp.h  # 0008  应 >0
nm -D pg-install/bin/postgres | grep -c shard_vacuum_read_hook                                   # 0008  应 =1
```

源码基线 = `postgres-src` submodule 的 `.gitlink` commit + 上述三个补丁。

## 应用方式

```bash
cd postgres-src
for p in 0001-add-wal-insert-hook \
         0001v2-wal-insert-hook-smgr \
         0002-flushbuffer-lsn-exempt-hook \
         0004-pre-record-commit-hook \
         0005-shard-xid-stamping \
         0006-shard-visibility-hooks \
         0007-shard-xact-record-xids \
         0008-shard-vacuum-read-hook; do
  patch -p1 --forward < ../pg-partdist-src/patches/$p.patch || exit 1
done
./configure --prefix=/work/pg-install --with-openssl --with-icu --with-readline
make -j$(nproc) && make install
```

`configure` 参数必须与现有 `pg-install` 一致（`pg_config --configure` 可查），
否则会毁掉环境的可复现性。

验证补丁都生效：

```bash
nm -D /work/pg-install/bin/postgres | grep -E \
  'wal_insert_hook|buffer_flush_lsn_exempt_hook|pre_record_commit_hook|shard_relation_xid_hook|shard_visibility_hooks|shard_xact_wal_list_hook|shard_xact_redo_hook|shard_vacuum_read_hook'
# 应输出八行
```

## ★ `pg-install/` 必须与 `patches/` 同步提交

仓库跟踪了一份预编译的 `pg-install/`（含 `postgres` 二进制与 server 头文件）。
**改内核补丁就必须重编 PG 并把整个 `pg-install/` 一并提交**——两者不同步会让
一键复现直接断在编译（`468a518` 的教训）。回归里的守卫是 `raft_22` A 段：
它断言当前二进制导出 `pre_record_commit_hook`，没有就直接判失败并指出
"内核补丁 0004 未落地，跨分区事务不会走 2PC"。
