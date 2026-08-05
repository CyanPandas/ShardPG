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
>
> **`pg-raft-src/reproduce-env.sh` 不打这些补丁、也不重编 PostgreSQL** ——
> 它假设镜像里的 `/work/pg-install` 已经是打过补丁的构建。镜像
> `pg-partdist-raft4-env` 里那份**是干净的官方 16.14**，而旧环境里那份带补丁的
> 是当初手工构建的、只存在于容器可写层。
>
> 后果：**`reproduce-env.sh destroy` 之后 `up` 建不出可用环境**（2026-08-04 实测，
> 销毁后才发现）。重建流程必须补上这一步：
>
> ```bash
> # 容器起来之后、编 pg_partdist 之前
> docker cp <打过补丁的 postgres 源码树> <容器>:/work/postgres-src
> docker exec -u postgres <容器> bash -lc '
>   cd /work/postgres-src &&
>   ./configure --prefix=/work/pg-install --with-openssl --with-icu --with-readline &&
>   make -j2 && make install'
> ```
>
> 源码基线 = `postgres-src` submodule 的 `.gitlink` commit + 上述三个补丁。
> **待办**：把这一步并进 `reproduce-env.sh`，否则"一键复现"名不副实。

## 应用方式

```bash
cd postgres-src
git apply ../pg-partdist-src/patches/0001-add-wal-insert-hook.patch
# 或
patch -p1 < ../pg-partdist-src/patches/0001-add-wal-insert-hook.patch
```

克隆本仓库、初始化 `postgres-src` submodule 到基线 commit 后，需先应用此 patch
再编译 PostgreSQL，pg_partdist 才能正常构建（依赖 `wal_insert_hook` 符号）。
