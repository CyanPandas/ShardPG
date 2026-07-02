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

## 应用方式

```bash
cd postgres-src
git apply ../pg-partdist-src/patches/0001-add-wal-insert-hook.patch
# 或
patch -p1 < ../pg-partdist-src/patches/0001-add-wal-insert-hook.patch
```

克隆本仓库、初始化 `postgres-src` submodule 到基线 commit 后，需先应用此 patch
再编译 PostgreSQL，pg_partdist 才能正常构建（依赖 `wal_insert_hook` 符号）。
