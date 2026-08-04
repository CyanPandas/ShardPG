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

> **为什么 0004 不能用 XactCallback 代替**：回调是 LIFO 顺序，后加载的扩展反而先
> 被调用，因此扩展无法表达"在**所有**其它扩展的 PRE_COMMIT 动作都完成之后再做
> 一件事"。而 Citus 强制要求自己排 `shared_preload_libraries` 第一位，调换加载
> 顺序这条路是堵死的。跨分区事务的决议恰恰必须发生在"Citus 发完全部
> `PREPARE TRANSACTION` 之后、本地 commit record 落盘之前"。

> 3 号位空缺：`0003` 曾用于一版被放弃的尝试，编号保留不复用，避免与历史记录混淆。

## 应用方式

```bash
cd postgres-src
for p in 0001-add-wal-insert-hook \
         0001v2-wal-insert-hook-smgr \
         0002-flushbuffer-lsn-exempt-hook \
         0004-pre-record-commit-hook; do
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
  'wal_insert_hook|buffer_flush_lsn_exempt_hook|pre_record_commit_hook'
# 应输出三行
```

## ★ `pg-install/` 必须与 `patches/` 同步提交

仓库跟踪了一份预编译的 `pg-install/`（含 `postgres` 二进制与 server 头文件）。
**改内核补丁就必须重编 PG 并把整个 `pg-install/` 一并提交**——两者不同步会让
一键复现直接断在编译（`468a518` 的教训）。回归里的守卫是 `raft_22` A 段：
它断言当前二进制导出 `pre_record_commit_hook`，没有就直接判失败并指出
"内核补丁 0004 未落地，跨分区事务不会走 2PC"。
