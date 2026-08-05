# PostgreSQL 内核补丁

`postgres-src` 是指向官方 `github.com/postgres/postgres.git` 的 submodule，无法
向上游推送自定义改动。pg_partdist 依赖的 `wal_insert_hook` 机制是对 PostgreSQL
内核的直接修改，因此以 patch 文件的形式保存在这里，作为可复现的构建依赖。

## 0001-add-wal-insert-hook.patch

在 `src/backend/access/transam/xloginsert.c` / `src/include/access/xloginsert.h`
里新增 `wal_insert_hook` 扩展点：每次 `XLogInsert()` 成功写入一条 WAL 记录后，
在释放 `registered_buffers` 之前回调该 hook，把记录的 end LSN、rmid、info、
block 引用列表和完整记录字节传给已安装的扩展。这是 pg_partdist 里
`PartWALInsert()`（`wal_insert_hook = PartWALInsert;`）能够捕获 WAL 记录、
写入 pg_parwal 的前提。

**基线 commit**：`e9067809ee722ed18152baed2191eaf92d770b29`
（`postgres-src` 当前 `.gitlink` 指向的同一个 commit，`REL_16_STABLE` 分支）

## 0001v2-wal-insert-hook-smgr.patch（R1）

让 `RM_SMGR` 这类**无块引用**的记录也能触发钩子（以 `nblocks == 0` 调用）。
没有它，`XLOG_SMGR_TRUNCATE` 永远捕获不到 —— `storage.c` 只 `XLogRegisterData`
不 `XLogRegisterBuffer`，`blocks[]` 恒空，而 FRD §5.3 要求捕获它。

## 0002-flushbuffer-lsn-exempt-hook.patch（R1）

`FlushBuffer` 对命中副本文件集合的页跳过 `XLogFlush(页LSN)`。物理回放的副本页
携带的是 **leader 坐标**的 LSN，本地 pg_wal 里根本没有那个位置（FRD §8.3）。
缺它则刷脏时 `XLogFlush` 会等一个永远不会到来的 LSN。

> **★★ 三个补丁全是 `pg_partdist` 的编译期硬依赖，缺任何一个都编不过。**
> 实测缺 0002 时的报错：
> `src/pg_partdist.c: error: 'buffer_flush_lsn_exempt_hook' undeclared`。

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
# （新增/改动的头文件按补丁涉及范围补齐）
```

**自检**（`reproduce-env.sh up` 之前值得先跑一遍，比编译失败早得多）：

```bash
grep -c wal_insert_hook               pg-install/include/postgresql/server/access/xloginsert.h  # 0001  应 >0
nm -D pg-install/bin/postgres | grep -c wal_insert_hook                                          # 0001v2 应 =1
grep -c buffer_flush_lsn_exempt_hook  pg-install/include/postgresql/server/storage/bufmgr.h      # 0002  应 >0
nm -D pg-install/bin/postgres | grep -c buffer_flush_lsn_exempt                                  # 0002  应 =1
```

源码基线 = `postgres-src` submodule 的 `.gitlink` commit + 上述三个补丁。

## 应用方式

```bash
cd postgres-src
git apply ../pg-partdist-src/patches/0001-add-wal-insert-hook.patch
# 或
patch -p1 < ../pg-partdist-src/patches/0001-add-wal-insert-hook.patch
```

克隆本仓库、初始化 `postgres-src` submodule 到基线 commit 后，需先应用此 patch
再编译 PostgreSQL，pg_partdist 才能正常构建（依赖 `wal_insert_hook` 符号）。
