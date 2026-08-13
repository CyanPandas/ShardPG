# TX-TSO-MVCC 开发实施计划（DEV PLAN v1）

> **定位**：本文是 `TX_TSO_MVCC_DESING.md`（方案定稿，下称"设计文档"）的**开发视角
> 配套文档**——任务分解、实施顺序、逐任务验收、工程纪律。设计文档回答"是什么/
> 为什么"，本文回答"先做哪个文件、怎么算做完"。
>
> **代码基线**：`shardpg-TX2` @ `7ff2656`。**P1 详细到任务粒度，P2–P6 到里程碑粒度**
> （每完成一期，下一期再展开成任务粒度——避免对着未落地的地基做过细规划）。
>
> 设计文档引用格式：§n = 设计文档章节。

---

## 0 开发前置（开工前逐项过）

### 0.0 工作规则（2026-08-12 用户裁定，全程有效）

1. **产出边界**：所有文档与代码改动（含补丁文件、扩展源码、测试脚本、pg-install
   二进制）只落在 `~/shardpg-tx2-work`；容器 `/work` 仅作构建/运行沙箱（源头在
   工作区，docker cp 同步进容器编译运行）；宿主其他目录一律不碰。
2. **文档同步**：方案或代码变更后立即回填设计文档 / 本文 / 相关 docs。
3. **测试策略**：每任务只跑该任务的**必要测试**并明确说明跑了什么；逻辑上无明显
   风险的部分可暂缓测试；全量 438 回归只在 P1 出口跑一次，不逐任务跑。

### 0.1 环境与分支

| 项 | 内容 |
|---|---|
| 工作区 | `~/shardpg-tx2-work` ↔ GitHub 分支 `shardpg-TX2` ↔ 容器环境 pg-citus-tx2（1 coordinator + 8 worker） |
| 互斥约束 | 三套 9 节点环境（pg-citus-replay / pg-citus-tx / pg-citus-tx2）互斥，开工前确认另两套已停 |
| 开发分支 | 从 `shardpg-TX2` 拉新分支（建议 `shardpg-tso`），**不在 TX2 基线分支上直接开发**；commit/push 按需人工决定 |
| 回归基线 | TX2 全量 438 项逐套件构成为底线：新开发不得使既有套件出现新 FAIL（TX1 夹具已知 1 失败除外，单跑 82/0） |

### 0.2 构建与二进制纪律

- 内核补丁（`pg-partdist-src/patches/`）与 `pg-install/` 预编译产物**成对提交**：
  每次改动内核补丁 → 重编 PG → 把 `pg-install/bin/postgres` 等同步回仓库
  （复现脚本 reproduce-env.sh 不重编 PG，全靠仓库内 pg-install——漏同步 = 克隆复现即坏）。
- 新增补丁沿用现有编号与 README 记载惯例：本项目新补丁从 **0005** 起，
  `patches/README.md` 同步登记应用顺序与理由。

### 0.3 工程教训清单（历次开发实measured的坑，全部当纪律执行）

1. **验收脚本零检查会静默 PASS**：逐项循环必须加计数守卫（断言"实际检查条数 = 期望条数"）。
2. **`docker exec -i` 吞 stdin**：容器内执行 SQL 的管道写法按既有测试脚本的模式抄。
3. **relnum 不是 OID**：涉及 relfilenode/表 OID 的断言逐个核对。
4. **节点崩溃不可见**："结果每轮都不一样 ≈ 有节点崩了"；PG 默认无 SIGSEGV 栈，
   开发环境先装好 backtrace 手段再开始调试。
5. **钩子过滤用白名单不用黑名单**（PRE_COMMIT 教训）；本方案 §9.2 安全网同款哲学。
6. **pg_raft 模块当前冻结**：P1–P3 不碰它可正常开发；**P4 涉及 raft_consensus.c
   搬迁，进入 P4 前需正式解冻立项**。

---

## 1 总路线图（P1–P6，入口/出口）

| 期 | 内容（设计文档 §12） | 入口条件 | 出口门禁 |
|---|---|---|---|
| **P1** | 内核原型：分片发号器 + heapam 打标 + WAL 记录体扩展（单分片、无 2PC） | 本文 §2 就绪 | pagecmp 逐字节一致；自见性/行锁回归；§4.5 hint 核查落档 |
| P2 | 分片 clog + 可见性分叉 + 只读路径 + xid_map 摘除 | P1 出口 | 单分片读写混合 + 崩溃恢复可见性正确；无主 RUNNING 认领生效 |
| P3 | master 薄化 + TSO（内存计数器，§2.4）+ GlobalSafeTs | P2 出口 | 竞态注入测试；boot 防呆生效 |
| P4 | 多分片 2PC + 决议搬迁（Citus MX） | P3 出口 + §9.1 三条验证实验通过 + §9.2 门禁用例集就绪 + **pg_raft 解冻** | 崩溃矩阵逐格演练 |
| P5 | vacuum/GC 全章 + 回卷护栏 | P4 出口 | §6.4 三类动作注入测试；两态恢复；age 护栏触发 |
| P6 | 切主/恢复全链路联测 + 并入基线 | P5 出口 | 全量零 FAIL + §10 负向用例 |

---

## 2 P1 详细任务分解

P1 目标一句话：**在单个分片上，让"xmin/xmax = 分片 xid"的写入、崩溃恢复 redo、
物理回放三者产生逐字节相同的页面，且本地事务自见性与行锁不回归。**

任务依赖图：

```
T1.0（前置核查）
T1.1（分片表判定）──┬── T1.3（xact 绑定）── T1.4（heapam 打标）── T1.5（WAL/redo）── T1.9（pagecmp 验收）
T1.2（分片发号器）──┘                          │
                                               ├── T1.6（可见性临时桩）
                                               └── T1.7（行锁反查）
T1.8（安全网骨架，随 T1.6 一起）
```

### T1.0 前置核查（不写代码，产出结论文档）——✅ 已完成（2026-08-12）

- 产出：`pg-partdist-src/docs/P1_PRECHECK.md`，四条结论：
  - **A**：pagecmp 验收自带内核背书的 hint 掩码（heap_mask 口径），§4.5"事实 3"
    不成立已修正，禁用决策不变；
  - **B**：断言 xmin/xid 的既有用例仅 3 个脚本，P1 均不受影响（受影响期 P2/P5）；
  - **C（新约束）**：P1 打标**必须**显式 GUC 白名单门控（按 partition_map 门控会
    报废 438 基线）——已并入 T1.1；
  - **D（新约束）**：P1 必须屏蔽分片表剪枝与 vacuum（on-access prune 会按原生
    clog 误删活元组，分片 xid 低号段撞 initdb 时代已提交号）——已并入 T1.6/T1.9。

### T1.1 分片表判定标志（O(1) 谓词）——✅ 已完成（2026-08-13，`src/shard_xid.c`）

- **改**：pg-partdist 侧 metadata_cache 暴露"relid 是否分片表"查询；内核侧
  RelationData 增缓存字段（补丁的一部分）或经 rendezvous 变量挂接口，relcache
  失效时同步。**P1 必须用 GUC 白名单（`pg_partdist.shard_relids`）门控，仅 P1
  新建测试表入名单**——按 partition_map 门控会让全部既有套件的表改用分片 xid、
  报废 438 回归基线（P1_PRECHECK 结论 C）；partition_map 驱动推迟到 P2 随
  可见性/clog 整体切换。
- **验收**：谓词单测——分片表/普通表/系统目录/TOAST 各判定正确；热路径无每元组
  SPI 查询（perf 采样确认）。
- **实施记要**：白名单 GUC（PGC_SIGHUP，check 钩子解析成升序去重数组挂 extra，
  assign 换指针，热路径无锁读）；TOAST 归属走 `pg_toast_<owner>` 命名规约从
  relcache 现有字段解出——**构造上不存在每元组 catalog/SPI 查询**，perf 采样
  免做；白名单为空时谓词一次比较即返回（438 基线路径零开销）。验收实测：
  分片表命中、TOAST 随属主同号、普通表走原生 xid、系统目录不受影响（DROP/
  ALTER SYSTEM 均正常）。

> **补丁编号整合（2026-08-12 实施中修订）**：原计划 0005=发号器 / 0006=xact /
> 0007=heapam。实际设计把发号器与事务绑定**全部放到扩展侧**（内核只留一个
> `shard_relation_xid_hook`，惰性领取在钩子实现里完成，不改 xact.c）——内核面
> 缩到最小，与 0001/0002/0004 的钩子风格一致。因此：**0005 = T1.4+T1.5 内核部分
> （打标+WAL 尾缀+redo+剪枝屏蔽+行锁禁令，已完成，见 patches/README 第 5 行）**；
> **0006 = T1.6/T1.7 内核部分（可见性分叉+等待翻译+xmax 组装屏蔽，已完成，
> 见 patches/README 第 6 行）**；T1.2/T1.3 无内核补丁。

### T1.2 每分片 xid 分配器（扩展侧）——✅ 已完成（2026-08-13，`src/shard_xid.c`）

- **改**：共享内存每分片 `{next_xid, watermark}`（P1 单分片可用固定槽位起步，
  P2 换 DSA/dshash）；批量水位持久化到该分片目录下的独立小文件（`pg_parwal/<oid>/`
  旁，复用其 fsync 纪律）；0/1/2 保留、从 3 起（§5.1）。
- **崩溃恢复**：P1 用"重启读水位文件、从水位起发"即可（跳号无害）；
  按 WAL 记录体推进的精确恢复留到 T1.5 一并做。
- **验收**：发号单调/并发安全（多后端压测无重号）；kill -9 后重启从水位续发不重号；
  0/1/2 永不出现。
- **实施记要（与原计划的一处偏差）**：水位文件放 **`$PGDATA/pg_shard_xid/<oid>`**
  而不是 `pg_parwal/<oid>/` 旁——P1 白名单表不在 partition_map、没有 parwal
  目录可依附；fsync 纪律照抄（tmp 写入 + pg_fsync + durable_rename）。固定
  64 槽 + 单 LWLock（发号频率是每事务每分片，锁不在热路径）；批量 4096，
  水位推进先落盘后发号，任何 ERROR 都在槽位状态推进之前（fail-closed）；
  逼近 0xFFFF0000 拒发（P1 无回卷）。验收实测：首号 3、单调续发、水位文件
  4099→崩溃重启后续发 4099、水位推至 8195、无重号。多后端并发压测推迟到
  T1.9 一并跑（单 LWLock 下逻辑无并发缺口）。**遗留（P2 登记）**：DROP TABLE
  不回收水位文件，孤儿文件需随 P2 分片 clog 生命周期一起管。

### T1.3 事务↔分片 xid 绑定（扩展侧，无内核补丁）——✅ 已完成（2026-08-13，`src/shard_xid.c`）

- **改**：~~`xact.c` 增~~（编号整合后全在扩展侧）"首写某分片时惰性领取该分片
  xid"，每后端 `{分片 → 本事务分片 xid}` 映射（top-level 先做，子事务 P1 明确
  **禁用**——分片表事务内 SAVEPOINT 直接报错，子事务逐分片领号推 P2+）。
- **验收**：单事务写单分片领一个号；只读事务不领号；事务中止后号不复用（跳号）。
- **实施记要**：钩子 assign 路径先查嵌套层级（>1 即 ERROR，已领过号的复用也禁
  ——单 xid 表达不了子事务局部回滚），再查后端映射（上限 16 分片/事务），未命中
  才进共享内存发号；XactCallback 在 COMMIT/ABORT/PREPARE（含 PARALLEL 变体）
  清映射。验收实测：单事务 3 行同号（xmin=3）、第二事务 4、只读事务不领号
  （后继插入拿 5 不是 6）、SAVEPOINT 内写报错、DELETE 盖 xmax=7。

> **T1.6 的 D 屏蔽已提前交付一半（2026-08-13）**：`ShardXidUtilityGuard` 挂在
> ProcessUtility 上，白名单非空时拦 VACUUM/ANALYZE/CLUSTER 点名分片表（整库
> VACUUM 一律拒绝，fail-closed；TOAST 直接点名也拦）；配合 0005 的剪枝屏蔽，
> "原生 clog 误删活元组"两条路径（on-access prune / vacuum 回收）都已封死。
> T1.6 剩余部分 = 可见性分叉骨架 + 临时提交表（未来补丁 0006）。

### T1.4 heapam 打标（原计划补丁 0007 前半 → 并入 0005）——✅ 已完成（2026-08-12，补丁 0005）

- **改**：`heapam.c` 的 `heap_insert/update/delete/lock_tuple/multi_insert`：
  目标是分片表时，盖 xmin/xmax 用后端映射里的分片 xid（不是
  `GetCurrentTransactionId()`）。**页格式不动、infomask 处置按 §4.5（不写 hint 位）**。
- **验收**：pageinspect 直接看页面：xmin=分片 xid 且从 3 连续；普通表行为完全不变
  （回归全量既有套件）。
- **实施记要**：随补丁 0005 交付（heap_insert/multi_insert/delete/update 顶部
  换号，`heap_lock_tuple` 直接 ERROR 而非打标——P1 禁行锁）。验收实测
  （2026-08-13 T1.1–T1.3 验收 B 段）：xmin 从 3 连续（3/4/5/6）、DELETE 盖
  xmax=7、TOAST 随属主同号、对照表原生 xid 不变；**全量既有套件回归留 P1 出口**。

### T1.5 WAL 记录体扩展与 redo（原计划补丁 0007 后半 → 并入 0005）——✅ 代码完成（2026-08-12，补丁 0005；正式验收随 T1.9）

- **改**：`xl_heap_insert/update/delete/lock/multi_insert` 记录体增分片 xid 字段
  （**记录头 xid 保持原生**，§8-②）；`heap_xlog_*` 各 redo 用记录体分片 xid 盖章；
  分配器恢复推进（重放中见到分片 xid ⇒ 推该分片 next_xid，对应
  `PartDistAdvanceNextXidPastXid` 的新语义，§5.5）。
- **三方一致性目标**（P1 的灵魂）：同一批写入，① leader 正常路径页面、
  ② leader kill-9 后崩溃恢复 redo 页面、③ follower rm_redo 回放页面，**三者逐字节相同**。
- **验收**：T1.9 的 pagecmp 套件；另加"崩溃点扫描"用例（在 flush 前后多个位置
  kill -9，恢复后 pagecmp）。
- **实施记要**：随补丁 0005 交付，实际形态比原计划更省——delete/lock 的 xmax
  本就走记录体既有字段（零格式变更），只有 insert/multi_insert/update 的新元组
  xmin 需要"主数据末尾 4 字节尾缀 + 0x80 标志位"，redo 从末尾 memcpy 取号
  （布局无关）；**分配器恢复推进（重放见分片 xid ⇒ 推 next_xid）未做**——P1
  裁定用"重启读水位文件续发"替代（T1.2 记要），精确恢复推进随 P2/回放接线再做。
  已有证据：immediate 崩溃恢复后页面逐值一致且续发不重号（2026-08-13 两轮验收
  的 D 段）；**三方 pagecmp + 崩溃点扫描 = T1.9 正式验收，本节勾选以"代码完成"
  为准，逐字节一致性结论以 T1.9 为准**。

### T1.6 可见性临时桩（结构永久、后端临时）——✅ 已完成（2026-08-13，补丁 0006 + `src/shard_visibility.c`）

- **背景**：xmin 一旦是分片 xid，原生 `HeapTupleSatisfiesMVCC` 会拿它查原生 clog
  ——错。P1 没有分片 clog（P2 交付），必须先立**分叉骨架 + 临时后端**。
- **改**：`heapam_visibility.c` 各 Satisfies* 入口按 T1.1 谓词分叉（**分叉结构是
  永久交付物**）；分片表路径 P1 接一个共享内存临时提交表（分片 xid → committed
  标志，提交时置位），P2 原位替换为分片 clog + ts 判定。
  `TransactionIdIsCurrentTransactionId` 分片路径查后端映射（自见性，永久交付物）。
- **剪枝/清理屏蔽（P1_PRECHECK 结论 D，数据损坏级）**：分片表（白名单命中）直接
  跳过 `heap_page_prune_opt`（on-access 剪枝会按原生 clog 把分片 xid 误判——
  低号段撞 initdb 时代已提交号，可能物理清除活元组）；分片表建表即设
  `autovacuum_enabled=off` reloption，拦截手动 VACUUM/ANALYZE 报错；防回卷
  autovacuum 的残余风险 P1 接受（测试时窗内原生 nextXid 龄不触阈值），P5 解除。
- **验收**：单分片单事务/两并发事务的读正确（自见、未提交不可见、提交后可见）；
  明确标注临时表的 P2 替换边界（代码注释 + 本文勾销）。
- **实施记要**：内核补丁 **0006**（patches/README 第 6 行）= 六个 Satisfies* 入口
  分叉（MVCC/Self/Dirty/Update 交扩展、VacuumHorizon/HistoricMVCC 防御性 ERROR、
  **Toast 不分叉**——PG16 SatisfiesToast 对正常元组不查 clog，天然安全）+
  delete/update 等待臂 + `compute_new_xmax_infomask` 屏蔽。扩展侧临时提交表：
  {(分片,sxid) → RUNNING+原生xid | ABORTED}，**缺席=已提交**（COMMIT 删条目，
  自清理，反查表与提交表合一）。**P2 替换边界**：`shard_visibility.c` 的
  shard_xid_state()/ShardCommit* 全套换成分片 clog + commit_ts<start_ts；
  sv_satisfies_* 分叉骨架与 0006 内核面不动。
  **P1 桩已知限制（P2 解除）**：① 崩溃后哈希清空 ⇒ 崩溃时未提交事务的行会被
  误判可见（页面字节不受影响）；② 已提交即全局可见（无 ts，近似 read-
  committed）；③ 同事务"插入后又更新/删除同行"的 cid 判定退化（内核
  AdjustCmax 对分片 xid 不生成 combo cid，cmin 被覆盖）——验收用例避开。

### T1.7 行锁反查表——✅ 已完成（2026-08-13，并入临时提交表）

- **改**：共享内存"活跃 (分片, 分片xid) → 持有者原生 xid"表（领号时登记、事务结束
  清除）；`XactLockTableWait` 调用点（heapam 冲突等待路径）先反查再按原生 xid 等待。
  FOR SHARE 多锁者/SERIALIZABLE 按 §10 直接报错拦截（负向用例）。
- **验收**：两事务同行更新——后者阻塞、前者提交后后者继续（P1 无 ts，冲突中止规则
  §4.4 推 P3 接 TSO 后生效，此处先保证"会等、不错乱"）。
- **实施记要（验收语义修订）**：前者**提交**后，后者等待结束拿到 TM_Updated →
  EPQ 需行锁 → 撞 0005 行锁禁令报错——这恰是 §4.4 **first-committer-wins** 的
  正确终态，P1 借行锁禁令提前交付（P3 换成 ts 冲突中止）；前者**回滚**后，
  后者正常继续（实测阻塞 3020ms 后 UPDATE 成功）。
  **实施中抓到并修复一个数据损坏级缺陷**：覆盖 ABORTED 分片 xmax 时
  `compute_new_xmax_infomask` 拿分片 xid 查原生 clog（initdb 时代=已提交）
  误判"旧更新者还活着"，把两个分片 xid 组进 multixact
  （`new multixact has more than one updating member: 0 2[9,10]`）——修法：
  分片表上 TM_Ok 到达即旧 xmax 已死且无锁者，三处调用点 + 新元组 xmax 继承
  强制走 HEAP_XMAX_INVALID 简单路径（0006 内）。

### T1.8 fail-closed 安全网骨架（§9.2 第 1 层，P1 埋点）——✅ 已完成（2026-08-13）

- **改**：可见性分叉与打标路径埋守卫点。P1 为**宽松模式**（本地会话默认放行，
  仅记录）；P3 接入 TSO 后切**严格模式**（无 start_ts 读 / 无 gxid 写 ⇒ ERROR）。
  模式用 GUC 切换，默认值随阶段推进收紧。
- **验收**：宽松模式不拦任何 P1 用例；人工开严格模式时全部分片表访问被拦（证明
  守卫点位置正确）。
- **实施记要**：GUC `pg_partdist.shard_safety_mode`（enum，PGC_SUSET，默认
  permissive）；守卫点 = 四个 sv_satisfies_* 分叉入口（读）+
  shard_xid_for_current_xact（写），同点顺带执行 SERIALIZABLE 禁令；另
  PRE_PREPARE 回调拦"含分片写的事务 PREPARE TRANSACTION"（最后一个能安全
  ERROR 的点），CREATE INDEX/REINDEX 并入 utility 守卫（索引构建走
  SatisfiesVacuum 判活，对分片 xid 是结论 D 级误判）。验收实测：strict 读/写
  均拦、RESET 恢复、SERIALIZABLE/PREPARE 均报错。

### T1.9 P1 验收套件（与 T1.5 并行开发）——✅ 已完成（2026-08-13，两套件 45/45 + 49/49 全绿）

- 新建 `pg-partdist-src/tests/`下 P1 套件（沿用现有分目录与命名惯例）：
  打标正确性、发号崩溃安全、三方 pagecmp、自见性/行锁、普通表回归、负向用例
  （SAVEPOINT/FOR SHARE/SERIALIZABLE 报错 + 分片表 VACUUM/ANALYZE 被拦截报错、
  SELECT 不触发剪枝——P1_PRECHECK 结论 D）。
- **纪律**：每个循环断言配计数守卫；期望值动态计算不写死拓扑。
- **交付物（2026-08-13）**：`tests/test_shard_xid_p1.sh`（打标/发号崩溃安全/
  可见性/冲突等待/负向 13 条/剪枝屏蔽/普通表对照，45 检查）+
  `tests/test_shard_pagecmp_p1.sh`（阶段 1 = 三崩溃点 ①vs② 扫描：insert+COPY /
  update+delete+abort / checkpoint 后小批，主堆+TOAST堆+TOAST索引逐 fork md5；
  阶段 2 = R1 配方分布表（无 PK）+ raft 组 + 白名单打标 → ①vs② → follower
  replay_catchup → 逐 fileset 成员 pagecmp ②vs③）。
- **套件抓出并修复的产品缺陷/边界（按发现序）**：
  1. **pd_prune_xid 分叉**（数据页级）：heap_delete/update 正常路径用分片 xid
     设剪枝提示、redo 用记录头原生 xid → 页头 4 字节分叉。修：分片表正常路径
     也用原生 xid（并入 0006；该字段对分片表本就不被消费）。
  2. **TOAST 删除写 hint 位**（数据页级）：toast 删除走 SatisfiesUpdate，
     by-OID 谓词不认 TOAST → 原生路径拿分片 xid 查 clog 并写 hint。修：
     打标钩子命中 TOAST Relation 时登记后端 {toast→属主} 映射（heap_delete
     顶部必先于可见性判定），ShardXidLookupByOid 兼查。§4.5"TOAST 天然安全"
     论断修订为"SatisfiesToast 不分叉安全，toast 删除路径靠映射分叉"。
  3. **回调注册序**（流完整性）：PRE_PREPARE 的含分片写禁令原先晚于
     PartWALXactCallback 发作（LIFO），被判死刑的事务字节已刷进分区流且中止
     打断在途复制 → leader plsn 跑到多数派前头，follower 追平永差一条。修：
     ShardXidInstallHook 挪到 PartWAL 回调注册之后（先发作，字节不进流）。
  4. **三方判据边界**（非缺陷，判据修订）：parwal 在 ABORT 丢弃未刷记录
     （设计如此）→ 中止残留字节只在 leader 本地与原生 WAL（①②有③无）。
     三方"逐字节相同"以 parwal 流内容（提交路径字节）为界；中止残留清理归
     P5 vacuum。①vs② 的中止覆盖在阶段 1 CP2。
  5. **Citus 路由写 = 2PC**（P1 限制，设计 §10 已登记）：本分支 coordinator
     经手的分布式写一律 PREPARE TRANSACTION → 撞禁令中止（页面只剩中止残留，
     两轮实测；还曾以"xmin=3 存在"骗过弱断言——套件已补"提交数据真实可见"
     强校验）。P1 验收全程 leader 直写；2PC×打标 = P4 正题。
  6. **PD_PAGE_FULL 提示位**（判据修订，非产品缺陷）：页写满后 heap_update
     普通路径设 `pd_flags` 的 PD_PAGE_FULL，redo 从不设——单字节分叉（实测
     offset 10：0x02 vs 0x00）。内核 heap_mask/wal_consistency_checking 本就
     把 pd_flags 提示位列为可合法分歧域并掩掉。⇒ ①vs② 比对从裸 md5 改为
     pagecmp.py 掩码口径（与 ②vs③ 同一判据）；"三方逐字节相同"的精确含义
     = **heap_mask 掩码域之外逐字节相同**（§4.5/T1.5 措辞以此为准）。

### P1 出口清单（全部勾掉才进 P2）

- [x] T1.0 核查结论落档（docs/P1_PRECHECK.md），§4.5【需核实】标注解除
- [x] 三方 pagecmp 全绿（含崩溃点扫描）
      （2026-08-13：`test_shard_pagecmp_p1.sh` 49/49——三崩溃点 ①vs② 扫描 +
      分布表 ①vs②vs③（follower 追平 406/406、逐 fileset 成员 pagecmp 全过，
      含 TOAST 堆与 TOAST btree 索引）；判据 = heap_mask 掩码域之外逐字节相同）
- [x] 自见性/行锁用例全绿；普通表全量回归零新增 FAIL
      （2026-08-13：`test_shard_xid_p1.sh` 45/45 绿（自见性/行锁/剪枝屏蔽/
      普通表对照全过）；全量 438 十套件回归 **438/438 FAIL=0**——
      local_wal_conflict 9 + r2 50 + l1 56 + d1 83 + d2 21 + r1 57 +
      tx1 82 + tx2 42 + tx3 22 + tx4 16，TX1 已知赛跑本轮未触发，
      比基线 437/1 还干净，零新增 FAIL）
- [x] 负向用例全绿（该报错的都报错）
      （2026-08-13：正式套件 13 条负向 + strict 两向全过，含计数守卫）
- [x] patches/README 更新 + pg-install 同步提交成对完成
      （2026-08-13：README 已更新、bin/postgres + shard_stamp.h + heapam_xlog.h
      已同步进工作区，随 P1 出口提交成对入库 shardpg-TX2；nm 五钩子符号自检过）

---

## 3 P2–P6 里程碑级分解（进入前一期再细化）

- **P2**：已进期，细化为任务级分解见 **§3.1**（T2.0–T2.8，2026-08-13）。
- **P3**：已进期，细化为任务级分解见 **§3.2**（T3.0–T3.7，2026-08-13）。
- **P4**（前置：§9.1 三实验 + §9.2 门禁用例 + pg_raft 解冻）：协调者选定（首写分片）；
  globalXID 分配器；commit_ts 两时机；`dtx_master_pre_record_commit` 搬迁；
  连接加入协议 `partdist_join_global_txn`；三态问询；崩溃矩阵演练。
- **P5**：vacuum 三变量 + 前缀规则 + 页面三类动作 + 洁净位图 + CTRL 水位落地 +
  回卷两阶段护栏（§6/§7 全章）。
- **P6**：切主全链路（含 §6.6 切主认领、§5.5 水位新语义）联测；全部负向用例并入
  门禁；文档终检。

### 3.1 P2 详细任务分解（2026-08-13 进期细化）

**目标**：把 P1 的临时提交表桩换成**持久分片 clog**，跑通"任何崩溃点后可见性
都正确"这条主线（P2 出口门禁原文：单分片读写混合 + 崩溃恢复可见性正确；无主
RUNNING 认领生效），并清偿 P1 出口登记的四笔债。P2 结束时分片表在单分片、无
2PC 前提下具备生产形态的事务状态管理（仍无 ts：隔离近似 RC，SI 语义等 P3 TSO；
ts 两列只填占位不参与判定）。

**依赖图**：

```
T2.0 ──┬─ T2.1 ─┬─ T2.3 ─┬─ T2.4
       ├─ T2.2 ─┘        ├─ T2.6（含 V4 裁定）
       └─ T2.5           └─ T2.7
T2.8 验收套件与 T2.3 起并行开发，出口统跑
```

#### T2.0 前置核查（不写代码，产出 `docs/P2_PRECHECK.md`）——✅ 已完成（2026-08-13）

- ① **EnhancedClog 复用面**：现状 = 24B 槽 {start_ts, commit_ts, status,
  parent_xid}、`pg_gclog/<node>/` 稀疏段文件直接寻址、全零槽=RUNNING=未决
  （语义与 §5.3 天然同向）。审计四个消费者（dtx_participant / enhanced_clog /
  replay_worker / shard_replay）对"每来源节点"作用域的耦合点；裁定新增"每分片"
  域是**共存新实例**还是参数化改造（倾向共存：旧域服务既有回放/DTX 基线，动它
  就是动 438）。
- ② **shard_xidmap 消费者与基线依赖**：设计终态"整体删除"（§9），但基线回放/
  DTX 套件疑似依赖 → 拆成两问："分片表路径不碰 xid_map"（P2 必须保证）与
  "物理删除"（预计挪 P6 基线并入，理由落档）。
- ③ **补丁 0007 机制核定**：`xl_xact_commit/abort` 的 xinfo 可选块机制（仿
  `XACT_XINFO_HAS_*`）能否无侵入追加 (分片oid, 分片xid) 列表；填充挂点与
  redo 挂点（`xact_redo_commit/abort`）钩子形态；确认崩溃隐式中止（无 abort
  记录）由 T2.4 认领兜底、无需记录。
- ④ **认领触发点与扫描下界**：启动后每分片首访触发 vs 显式函数；认领水位持久化
  位置（水位文件扩列 vs clog 段头）。
- ⑤ **partition_map 门控字段方案**：加列还是伴表、注册 DDL 入口、基线既有
  partition_map 表默认不打标的保证（P1_PRECHECK 结论 C 的 438 保护延续）。
- **验收**：五问皆有结论落档；风险登记簿更新。
- **实施记要（五问定案，全文见 `docs/P2_PRECHECK.md`）**：
  ① EnhancedClog **共存新实例**——分片键 (Oid, sxid) 塞不进旧域 uint16 node_id
  编码，参数化=动基线磁盘格式；新建 `shard_clog.c/.h` 克隆 I/O 纪律，旧域零改动。
  两处实施偏差：目录用独立顶层 **`pg_shard_clog/<oid>/`**（与 pg_shard_xid 对称、
  DROP GC 一次删两个）；槽 32B = §5.3 五列原文（P2 只用 status）。
  ② xid_map 是 apply checkpoint **磁盘格式**的一部分（CRC 覆盖快照），删=动基线；
  分片表路径隔离已 grep 实证成立，物理删除挪 P6。
  ③ 0007 定案：xinfo **bit 9** = SHARD_XIDS 可选块（bit 0–8 已占）；挂点
  XactLogCommitRecord:5663 / XactLogAbortRecord:5835 填充钩子 +
  xact_redo_commit:5979 / xact_redo_abort:6133 落账钩子 + ParseCommit/
  AbortRecord 各一个 if 块；实测行序 **XACT_EVENT_COMMIT(2335) 在
  RESOURCE_RELEASE_LOCKS(2360) 之前** ⇒ 既有回调里标记 COMMITTED 即"记录已
  持久之后、行锁释放之前"，等待者唤醒即见终态；三段闭环（RUNNING 首写落账/
  记录后标记+redo 补齐/隐式中止靠认领）确认无缝——分片写必有原生 xid ⇒ 必有
  commit/abort 记录。
  ④ 认领**懒触发双入口**（发号器槽位首初始化 + 可见性首咨询；只挂写路径不够，
  UPDATE 撞无主 xmax 先于取号）+ 显式 SQL 函数；扫描上界 = **冻结的启动恢复
  上限**（取动态水位会误杀重启后新活事务）；认领水位并入水位文件（4B→8B，
  兼容旧格式）。
  ⑤ partition_map **尾部加列** `shard_mvcc BOOLEAN DEFAULT false`（实证 tests/sim
  零位置式 INSERT，加列无破坏）+ 注册函数 bump version + metadata cache O(1)；
  GUC 白名单降级为并集通道。
  测试：本任务不写代码，零测试；两条新风险 R-P2-1/R-P2-2 已入 §5。

#### T2.1 分片域 clog（存储层，扩展侧）——✅ 已完成（2026-08-13，`src/shard_clog.c` + `include/shard_clog.h`）

- **改**：enhanced_clog 增"**每分片**"域 `pg_gclog/shard_<oid>/`，与既有每来源
  节点域并存互不干扰（T2.0 ① 定案为准）；槽扩 32B {globalXID, start_ts,
  commit_ts, status, parent_xid}——P2 只用 status（globalXID 恒 0 待 P4、ts 两列
  占位待 P3、parent 待 P4 子事务）；槽号 = 分片 xid 直接寻址（稠密即行号，无
  xid_map）；**全零槽 = RUNNING = 不可见**（与桩的"缺席=已提交"相反，这正是
  崩溃正确性的来源）；写入幂等。
- **生命周期**：DROP TABLE 打标表在**事务提交时点**删除 `pg_gclog/shard_<oid>/`
  与 `pg_shard_xid/<oid>` 水位文件（借 smgr pending-deletes 的"提交才删"纪律，
  回滚不删）——P1 登记的孤儿文件债在此清偿。
- **验收**：落 RUNNING / 改 COMMITTED / 改 ABORTED / 崩溃重读；稀疏段寻址正确；
  DROP 提交后两类文件消失、回滚不消失；既有每来源节点域回归零扰动。
- **实施记要（2026-08-13）**：按 P2_PRECHECK 结论一落地——32B 槽（StaticAssert
  钉死）、`pg_shard_clog/<oid>/` 独立顶层目录、无锁幂等 pwrite。**持久化契约**
  写进头文件：判决写**立即 fsync**（本目录对 checkpointer 不可见，检查点推过
  提交记录后崩溃将无 redo 补标——每提交一次 fsync 是 P2 接受的代价）；RUNNING
  写不 fsync（洞=RUNNING，丢失恒安全，且同段判决 fsync 顺带刷下）；新建段
  fsync 分片目录一次。DROP GC 挂 ShardXidUtilityGuard 的 DropStmt 分支（标准
  ProcessUtility **之前**解析名字，执行后 catalog 已查不到）+ shard_xid
  XactCallback 提交结算（提交时点不许 ERROR，删不掉降 WARNING）；PRE_PREPARE
  禁令扩含挂起 DROP（GC 无法跟去别的会话结算）。验收 28/28 绿（裸层读写/保留号
  拒写/跨段寻址两段文件/immediate 崩溃后判决存活/水位文件/DROP 回滚不删提交删/
  DROP+PREPARE 拒绝）；旧域金丝雀 test_txn_layer_r2 单跑 50/0。测试中修掉
  1 个实现小缺陷（rmtree 对不存在目录自打 WARNING——先 stat 再删）和 1 个
  测试法陷阱（**`SELECT fn() IS NULL OR true` 被规划器常量折叠，volatile 函数
  根本不执行**——验收断言全体改"多语句 + tail -1"，已入坑清单）。其余套件
  按规则③不跑（分叉路径未动，白名单空=零开销谓词一次比较）。

#### T2.2 内核补丁 0007（commit/abort 记录体扩展 + redo 落账）——✅ 已完成（2026-08-13，`patches/0007-shard-xact-record-xids.patch` 269 行）

- **改**：`xl_xact_commit/abort` 增可选块携带本事务 (分片oid, 分片xid) 列表
  （新 xinfo 位，无分片写时记录体零变化）；填充与提交后标记由扩展侧钩子完成
  （挂点按 T2.0 ③ 核定）；redo 侧新钩子把列表交扩展重做 clog 落账（§8-②）。
- **顺序契约**（三段闭环后任何崩溃点可见性都正确）：RUNNING 落账在首写取号时
  （T2.3）；COMMITTED/ABORTED 标记在提交/中止记录**落盘之后**（正常路径钩子
  执行，崩在记录与标记之间由 redo 重做补齐）；崩溃隐式中止无记录 → T2.4 认领
  兜底。
- **验收**：补丁可独立反打；无分片写事务记录体零变化（普通表零扰动）；带分片写
  的 commit/abort 记录 pg_waldump 可辨；崩溃点注入后 redo 补齐 clog 状态。
- **实施记要（2026-08-13）**：按 P2_PRECHECK 结论三落地——xinfo bit 9 +
  `xl_xact_shard_xids`（紧凑 uint32 交错对，无补齐），块插在 DROPPED_STATS 之后
  两侧一致（origin 之后不保证对齐，避开）；改 4 文件：xact.h（位/结构/parsed 两
  结构扩列）、shard_stamp.h（两钩子声明）、xact.c（钩子变量 + 两构造函数决策/
  注册 + 两 redo 尾）、rmgrdesc/xactdesc.c（两解析 + desc 打印）。**收集钩子在
  临界区内被调**（XactLogCommitRecord 有 Assert(CritSectionCount>0)），契约
  写死：不得 palloc/ereport，扩展侧返回静态缓冲。扩展接线：shard_xid.c 收集
  impl + ShardXidInstallHook 装两钩子；shard_clog.c `ShardClogXactRedo`（startup
  进程跑，SetVerdict 自带 fsync，失败即中止恢复=正确失败方式）。
  验收 17/17 绿：nm 双符号、COMMIT/ABORT 记录 pg_waldump 均见 `shard xids:
  oid/xid`、原生事务零块、崩溃 redo 后 COMMITTED/ABORTED 双向补齐、二次重启
  判决持久、DROP GC 回归。**桩缺陷实景复现**：崩后临时提交表清零致回滚行漏判
  可见（count 4≠3）而 clog 已有正解 ABORTED——T2.3 切可见性到 clog 即收口，
  该期望移入 T2.3 验收。金丝雀 4 套全绿（local_wal_conflict 9/0、r2 50/0、
  P1 两套件 45/45+49/49——三方 pagecmp 带 0007 依然成立）。补丁导出用
  "编辑脚本替换表反向重建基线再 diff"法（正/反 dry-run 自检过）；pg-install
  同步 bin/postgres + xact.h + shard_stamp.h，README 七行 nm 自检过。

#### T2.3 可见性桩原位替换（`src/shard_visibility.c`）——✅ 已完成（2026-08-13）

- **改**：临时提交表退役，判定改查分片 clog：RUNNING（含全零）= 不可见（非本
  事务）、COMMITTED = 可见、ABORTED = 不可见；自见性仍走后端映射（不变）；
  行锁等待反查 {(分片, 分片xid) → 持有者原生 xid} 保留共享内存活跃表（事务态
  不持久、不进 clog）；RUNNING 落账挪到首写取号处（发号成功后、返回调用者前，
  fail-closed 顺序）。
- **语义清偿**：桩已知限制"崩溃后缺席=已提交漏判"在此消失——未决事务崩后保持
  RUNNING → 不可见 → T2.4 判 ABORTED。
- **验收**：P1 两套件全绿（45/45 + 49/49，判据不变）；新增崩溃点用例：RUNNING
  落账后崩 → 数据不可见；提交记录落盘后、clog 标记前崩 → 恢复后数据可见。
- **实施记要（2026-08-13）**：改动收敛在 `shard_xid_state()` 一个函数——裁决
  顺序改为 后端映射(自见) → 共享内存活跃表(RUNNING+持有者原生xid，兼行锁
  反查) → 后端终局缓存(COMMITTED/ABORTED 不可变，TopMemoryContext 本地
  hash，免每元组文件 I/O) → 分片 clog 真相源(全零/空洞=RUNNING=不可见)。
  保留号 0/1/2 防御性恒可见（原生 Frozen/Bootstrap 语义）。活跃表条目瘦身
  （去 status 列，表内即 RUNNING）；RegisterRunning 先落 clog RUNNING 账再进
  表（此刻 sxid 未进任何元组，无竞态窗）；MarkEnded 先写判决后摘条目——
  **判决写失败在 COMMIT/ABORT 回调里没有可用的 ERROR 语义（提交后 ERROR 会
  递归中止已提交事务），升 PANIC 借崩溃恢复走 0007 redo 补齐**；无主 RUNNING
  在等待路径立即 ERROR（静默返回会 BeingModified→等待→重评无限自旋），T2.4
  改为触发认领。验收 15/15 绿：未提交崩→count=0 且 clog 挂 RUNNING 待认领；
  提交+回滚后崩→**count=3（桩时代 4，T2.2 复现的漏判在此收口）**；崩前正常
  路径回调即时判决可查（COMMITTED/ABORTED）；活体跨会话 RUNNING 不可见/提交
  后可见。P1 两套件回归 **45/45 + 49/49 全绿**（判据不变，三方 pagecmp 含
  follower 追平不受扰）。测试说明（规则③）：白名单空时新增路径不可达，
  未跑其余基线套件；438 全量留 P2 出口。

#### T2.4 无主 RUNNING 认领（恢复期，普通事务分支）——✅ 已完成（2026-08-13）

- **改**：启动后每分片首次访问时扫 [认领水位, 发号水位) 的 RUNNING 槽改
  ABORTED（写入幂等）；认领水位持久化（位置按 T2.0 ④）；**PREPARED 分支
  （2PC in-doubt 不许动）留 P4；切主分支（流内无提交标记 ⇒ ABORTED）留 P6**
  （§6.6 三分支只做第一支）。
- **验收**：kill -9 重启后未决事务的行不可见且槽变 ABORTED；跳号留下的全零洞
  被一并判 ABORTED 无副作用；二次重启幂等。
- **实施记要（2026-08-13）**：核心不变式 = **"发号器槽位存在 ⇒ 本次启动已认领"**
  ——建槽统一走 `shard_xid_slot_attach()`：读水位文件 → 认领 [claim_wm,
  文件 alloc_wm)（`ShardClogClaimRange`：RUNNING/洞→ABORTED，终局与 PREPARED
  保留，每段一次 fsync）→ 持久化 claim_wm → 挂槽。上限即"冻结的启动恢复上限"
  （挂槽前本分片必无新号已发，R-P2-2 结构性成立）；双入口归一：发号路径经
  attach 天然触发，可见性读路径在咨询 clog 前调 `ShardXidEnsureClaimed`（共享
  锁扫槽快路径）。水位文件 **4B→8B** {alloc_wm, claim_wm}，读侧兼容旧 4B
  （claim_wm 按 3 全量补认领，安全方向）；新增 SQL 函数 `partdist_shard_claim`
  （测试/运维显式触发）。验收 24/24 绿：读路径触发认领（遗留 sxid 与全部跳号
  洞改判 ABORTED、上限之上不动、水位文件 {4099,4099}）；R-P2-2 实测（重启后
  新活事务 4099 不进认领范围、照常提交）；干净重启二轮认领保留终局判决只清
  新洞；旧 4B 文件全量补认领后数据完好且文件升 8B。回归 test_shard_xid_p1
  **45/45**（套件 [4] 段水位解析随格式升级取第一个 uint32——3 个 FAIL 全是
  套件解析、非产品缺陷）。测试期教训：未提交事务不 CHECKPOINT 就 immediate
  崩溃，行根本不落盘，"读路径触发"无从验证——用例已加崩前 CHECKPOINT。
  t23 场景脚本的"遗留=RUNNING 待认领"断言被本任务取代（T2.8 统一收编）。

#### T2.5 发号器恢复推进走 WAL（P1 债，§5.1/§8-①）——✅ 已完成（2026-08-13，验收范围修订）

- **改**：redo 从记录体尾缀取分片 xid 时同步推进该分片分配器影子水位；启动后
  发号起点 = max(水位文件, WAL 推进值)；水位文件保留为下界兜底。
- **验收**：~~崩溃重启后首号 = 崩前最大已用号 +1（不再整批跳 4096）~~（修订，
  见记要）；水位文件缺失/落后时仍不重号。
- **实施记要（2026-08-13，含一处验收目标修订）**：
  **"消除 4096 跳号"被裁定为不做**，理由两条：① 原式"起点 = max(文件, WAL
  推进)"自相矛盾——文件值天然 ≥ 一切已发号，max 永远取文件，跳号不可能缩小；
  真正消跳号需要覆盖"检查点之前已持久、未提交、无判决"的孤儿号（t24 [1] 场景
  实测存在），这只能靠检查点时刻的精确快照，而扩展没有 checkpoint 钩子。
  ② T2.4 的认领把 [claim_wm, 文件水位) 的洞全部烧成 ABORTED——文件之下已无号
  可发，低于文件的续发点会被终局槽逐个跳过、落点仍是文件值。跳号 ≤4096/崩溃、
  号空间 32 位 + P5 回卷，代价可接受（维持 T1.2"跳号无害"裁定）。
  **实际交付（"文件缺失/落后仍不重号"做实）**：① WAL 影子推进——0007 redo
  钩子逐对喂 `ShardXidRedoAdvance`（shmem 影子数组，startup 进程只记影子不建
  槽，保 T2.4 不变式），attach 时 ceiling = Max(文件, 影子)，文件健在时影子
  ≤ 文件零扰动；文件缺失时抬高发号起点与认领上限，落盘时 alloc 位同步抬
  （防写回 {0, ceiling} 畸形档）。② **终局槽跳过守卫**——发号后查 clog，
  已有判决的号跳过绝不重发（重发会让新事务结局"复活"同号历史元组）；正常
  路径是洞一次通过，每分配多一次 pread 无 fsync。验收 13/13：崩溃续发 4099
  （基线语义不变）；删水位文件后仍不重号（影子推进 4100、老数据/老判决完好）；
  伪造 4101/4102 终局槽实测绕行发 4103。回归 test_shard_xid_p1 45/45。
  残余风险（登记不修）：文件缺失 + 检查点窗口外未提交持久孤儿 双重故障下
  仍可能重号——文件缺失属运维事故，且该号历史无判决、复活面有限。

#### T2.6 ANALYZE 读侧分叉 + V4 裁定（§8-⑤ 读侧）——✅ 已完成（2026-08-13，`patches/0008-shard-vacuum-read-hook.patch` 72 行）

- **改**：SatisfiesVacuum 入口对分片元组按 clog 分叉（RUNNING→进行中、
  COMMITTED→活、ABORTED→死判定，但 P2 **只判不收**——回收页面动作是 P5）；
  utility guard 对 ANALYZE 放行；VACUUM 维持禁；**V4（CIC/CLUSTER 分叉 vs
  禁用）在此裁定并落设计 §11 + 核实点表**。
- **验收**：ANALYZE 分片表成功且统计行数近实际；VACUUM/CLUSTER/CIC 仍报错；
  V4 状态更新。
- **实施记要（2026-08-13）**：补丁 0008 = `HeapTupleSatisfiesVacuumHorizon`
  分片分支从 0006 防御 ERROR 改为 `shard_vacuum_read_hook` 裁决，钩子未装保持
  fail-closed。**"只判不收"的机械保证**：committed-deleted 给 RECENTLY_DEAD，
  分叉点配 `ReadNextTransactionId()` 作 dead_after——两个调用方
  （HeapTupleSatisfiesVacuum 的 OldestXmin 比较、NonVacuumable 的 GlobalVis
  提升）拿新鲜原生 xid 必落保守分支，永不提升 DEAD；中止插入给 DEAD（语义
  准确，回收动作者全被禁/屏蔽，只进 ANALYZE 死行统计）。扩展侧
  `sv_satisfies_vacuum` 复用 shard_xid_state 四态；**autovacuum 硬盾**——
  autovacuum 不经 ProcessUtility guard，钩子里 IsAutoVacuumWorkerProcess 即
  ERROR（分片表纪律 autovacuum_enabled=off，撞进来 fail-closed）。guard 改动：
  ANALYZE（含整库）放行，VACUUM/VACUUM ANALYZE/FULL 维持禁（P5）。V4 裁定
  **第一期禁用**落档设计 §10/§11 + 核实点表（CIC 随索引专项、CLUSTER/FULL
  P5 出口再评估）。验收 17/17：混合状态表（20 活+5 删+3 回滚）ANALYZE 成功
  且 reltuples=20 精确、活动事务行不入统计、整库 ANALYZE 放行、七条禁令面
  维持。P1 套件"ANALYZE 点名"负向翻正向（计数守卫 13→12），回归 45/45。
  pg-install 成对同步（bin/postgres + shard_stamp.h），README 至 0008
  （nm 八行）。

#### T2.7 门控切换 partition_map 驱动（P1 债）——✅ 已完成（2026-08-13）

- **改**：分片打标身份从手工 GUC 白名单迁到 partition_map（方案按 T2.0 ⑤：
  加列或伴表 + 注册 DDL）；metadata cache 提供 O(1) 判定；GUC 白名单降级为
  测试便捷通道（两者取并集）；基线既有 partition_map 表默认不打标。
- **验收**：注册后无 GUC 也打标；未注册表零行为变化；438 门控路径回归。
- **实施记要（2026-08-13，一处方案落地调整）**：既有 metadata cache 是
  "懒填 + SPI 加载"，钩子上下文（heapam 深处）用不了——O(1) 判定改为
  **ShardXidState 内的 mvcc 集合**（64 槽 + `mvcc_n` 无锁快门：0 = 全库无
  登记表，438 写路径只多一次整型读）。三层结构：**真相 =
  `partdist.partition_map.shard_mvcc` 列**（尾部加列 DEFAULT false，9 节点
  已 ALTER 部署 + 建表 SQL 更新）；**启动登记表 = `pg_shard_xid/` 目录**
  （注册函数预创建 8B 水位文件，ShardXidShmemInit 在 postmaster 启动期扫目录
  重建集合——不引入第二份持久结构，DROP GC 天然除名）；**运行时 = shmem
  集合**。注册入口 `partdist_set_shard_mvcc(regclass)`（C+SPI，超级用户）：
  ① 真相列 UPDATE（无行即拒，登记的必须是已注册分区）② 预创建水位文件
  ③ 进集合——②③ 不随回滚撤销，失败方向 = 多打标（事实白名单），语义安全；
  **P2 只进不出**（撤销=DROP，消灭"回滚后列真/集合无"的错配面）。统一谓词
  `shard_oid_is_mvcc` = GUC 名单 ∪ mvcc 集合，九处调用点收编（含 TOAST 归属、
  utility guard、DROP GC）。验收 19/19：未登记零变化（原生 xmin）；无
  partition_map 行拒登记；登记后无 GUC 打标 xmin=3、可见性/ANALYZE/VACUUM
  禁令全走分片路径；**干净重启后集合从目录重建、新写仍打标**；DROP 全清。
  金丝雀 lwc 9/0 + r2 50/0 + P1 45/45。已知边界：集合上限 64 张（超限启动
  WARNING 且该表不再打标——禁用继续使用）；schema 硬限定 partdist. 前缀随
  既有约定；flag 的跨节点传播沿用 partition_map 既有同步通道（P2 单 leader
  场景手工/控制面按节点执行）。

#### T2.8 P2 验收套件（与 T2.3 起并行开发）——✅ 已完成（2026-08-13，`tests/test_shard_clog_p2.sh` 64 项）

- `tests/test_shard_clog_p2.sh`：读写混合并发、崩溃点扫描（RUNNING 落账后 /
  提交记录后标记前 / 中止路径 / 认领幂等 / 二次崩溃）、ANALYZE、DROP GC、
  门控迁移。沿用 P1 套件工程纪律（计数守卫 / docker cp 不走 heredoc / 输出
  格式校验 + 重试）。
- P1 两套件全量保持绿（回归门禁）。
- **实施记要（2026-08-13）**：t21–t27 七个散装验收收编为 64 项正式套件，
  七段：[1] 存储层+DROP GC；[2] 0007 waldump 取证；[3] 崩溃可见性+认领
  （核心门禁，t23/t24 叙事线合并省重启）；[4] 恢复推进不重号；[5] ANALYZE+
  禁令面（负向计数守卫 10 条）；[6] partition_map 门控含重启存续；[7] 健康
  收尾。**收编抓出一个 flaky**：abort 记录不同步刷盘而 `pg_current_wal_lsn`
  是写出位置——ROLLBACK 后立即取右边界，中止记录还在 wal_buffers，waldump
  段文件里看不到（t22 当时通过纯属时序运气）；修法 = ROLLBACK 后用一笔提交
  事务驱动刷盘再取界。单跑 **64/64 绿**；出口 13 套件全量见出口清单。

#### P2 出口清单（全部勾掉才进 P3）——✅ 全勾（2026-08-13，P2 结项）

- [x] 单分片读写混合 + 全崩溃点可见性正确（提交者崩后可见、未决者崩后不可见）
      （T2.3 验收 15/15 + 套件 [3] 核心门禁：count=3 收口、redo 落账双向）
- [x] 无主 RUNNING 认领生效（里程碑门禁原文）
      （T2.4 验收 24/24：读路径触发、洞改判、防误杀、幂等；套件 [3] 收编）
- [x] 临时提交表代码退役删除（风险登记簿 4"临时桩滞留"清偿）
      （T2.3："缺席=已提交"桩语义整体移除；共享内存哈希保留为**活跃表**——
      RUNNING 事务态 + 行锁反查，是设计内永久组件而非桩）
- [x] ANALYZE 解禁；V4 裁定落档设计 §11
      （T2.6 补丁 0008"只判不收"；V4 = CIC/CLUSTER 第一期禁用，三处落档）
- [x] 四笔 P1 债清偿（DROP GC / partition_map 门控 / WAL 恢复推进 / xid_map
      路径隔离判定落档）
      （T2.1 / T2.7 / T2.5（验收修订：跳号保留，交付缺失文件不重号）/
      T2.0 结论二落档）
- [x] 全量 438 + P1 两套件 + P2 新套件零新增 FAIL
      （2026-08-13 出口回归 **13 套件 596/596 FAIL=0**：438 基线十套 +
      45 + 49 + 64；TX1 已知赛跑本轮未触发，比允许基线还干净）
- [x] patches/README 更新 + pg-install 同步提交成对完成（0007、0008）
      （随 T2.2/T2.6 逐任务成对入库，nm 八行自检过）

### 3.2 P3 详细任务分解（2026-08-13 进期细化）

**目标**：master 薄化起步——TSO 内存计数器落地（§2.4 v1 裁定：不做 TSO 自身
HA，boot 防呆兜底），ts 进入判定，隔离从"近似 RC"升为**真 SI**（§4.1：可见 ⇔
COMMITTED 且 commit_ts < start_ts；§4.4 first-committer-wins 串行化冲突）；
GlobalSafeTs 机制就位（§6.2 双通道/租约/栅栏——消费方 vacuum 在 P5，本期交付
机制与读出）。出口门禁（里程碑原文）：**竞态注入测试；boot 防呆生效**。

**依赖图**：

```
T3.0 ──┬─ T3.1 ─┬─ T3.2 ─┬─ T3.3 ─┬─ T3.4
       │        └─ T3.5  └─ T3.6  │
       └───────────────────────────┘
T3.7 验收套件与 T3.3 起并行开发，出口统跑
```

#### T3.0 前置核查（不写代码，产出 `docs/P3_PRECHECK.md`）——✅ 已完成（2026-08-13）

- ① **ts 载体与既有字段核查**：TSO 是逻辑单调计数器（int64，从 1 起）还是
  混合时戳；盘点既有占位字段（ShardClogSlot.start_ts/commit_ts uint64、
  `DtxRecordPayload.commit_ts`、`TxnMarkerPayload.start_ts/commit_ts` 及其
  "本地时钟占位"注释）——换值来源的兼容面。
- ② **TSO 通路载体**：coordinator 上 pg_partdist shmem 计数器 + SQL 函数
  （worker 后端经 libpq 缓存连接调用）vs 独立协议 bgworker；连接管理
  （每后端缓存、断线 fail-closed 绝不本地时钟顶替——§2.2 纪律 2）；
  取号 RPC 延迟对单分片事务的成本预估。
- ③ **commit_ts 进提交记录体的补丁方案**：redo 必须能重做带 ts 的落账 ⇒
  commit 记录要携带 commit_ts。0007 的 xl_xact_shard_xids 块头扩一列
  （重生成 0007）vs 新补丁 0009 追加块；abort 无需 ts（ABORTED 不参与
  ts 判定）。
- ④ **start_ts 懒取偏差论证**：设计 §2.2"事务开始时取"；实现拟在**首次触达
  分片表时**懒取（每事务至多一次 TSO 交互，纯原生事务零成本）——需论证
  与时机定理不冲突（懒取点即本事务分片快照点，单调性论证原样成立）。
- ⑤ **GlobalSafeTs v1 最小面**：登记表（master shmem）+ 搭车/心跳双通道 +
  租约清除齐备；**栅栏**（worker 提前量 ε 自作废本地快照）在 P3 无 vacuum
  消费方时做到什么程度（拟：读路径检查本地 start_ts 对应租约是否仍有效，
  失效即 snapshot too old——机制完整、代价一次内存比较）。
- ⑥ **Proxy 粘性路由 + 会话映射是否推迟 P4**：P3 无多分片事务、无 MX，
  该组件在 P3 没有消费方——拟推迟并入 P4（届时与 partdist_join_global_txn
  连接加入协议一起做），本期只留接口位。裁定理由落档。
- **验收**：六问皆有结论落档；风险登记簿更新。
- **实施记要（六问定案，全文见 `docs/P3_PRECHECK.md`）**：
  ① ts = **int64 逻辑计数器从 1 起，0=无 ts**；既有占位字段盘点四类，
  MARKER/DTX 本地时钟值 P3 不动 P4 换源（双宇宙并存但实证零比较点）；
  **兼容红利：P2 历史提交行 ts=0 在新判据下恰好恒可见，无需迁移**。
  ② 通路 = coordinator SQL 函数 + worker 后端 **pg_partdist 自链 libpq**
  缓存连接（pg_raft 持有 libpq 但模块冻结，借道不可行）；地址走 node_map，
  节点号走 partdist_node_id；成本 ~+1ms/写事务可接受。
  ③ **0007 收集钩子在临界区内，不可能做 RPC**——commit_ts 在
  XACT_EVENT_PRE_COMMIT 取号暂存（恰是"尽晚取"落点），钩子只拷；块头扩
  {nxids, commit_ts_lo, commit_ts_hi}（两 uint32 规避 4 字节对齐指针转型
  的非对齐 uint64 读）；**原位改造 0007 重生成**，不开 0009。
  ④ start_ts 首触分片表懒取——时机定理逐字成立，纯原生事务零 TSO 成本。
  ⑤ GlobalSafeTs 登记/双通道/租约全量做；心跳 = **新注册轻量 bgworker**
  （demux 是 one-shot、replay 按分区，均不可搭车）；栅栏最小面 = 心跳
  新鲜度检查（now < last_beat + lease − ε 否则 snapshot too old）。
  ⑥ Proxy 粘性路由**推迟并入 P4**（P3 无消费方，与连接加入协议一体设计）。
  两条新风险 R-P3-1（无 ts 判决落账 = SI 静默破坏，PRE_COMMIT ERROR +
  钩子 PANIC 双防线）/R-P3-2（双 ts 宇宙串线）已入 §5。本任务零代码零测试。

#### T3.1 TSO 服务（coordinator：内存计数器 + 发号即登记 + boot 防呆）——✅ 已完成（2026-08-13，`src/tso.c` + `include/tso.h`）

- **改**：coordinator shmem 单调 int64 计数器；SQL 接口
  `partdist_tso_start_ts(node, oldest_or_null)`（**发号即登记**：先把该节点
  登记更新为 min(携带值, 新号)，后返回新号——一次调用原子，§2.3 铁律）与
  `partdist_tso_commit_ts()`；**boot 防呆**：首次服务落 `$PGDATA/pg_tso_boot`
  标记，启动时检测到标记即拒绝发号（fail-closed 响亮停摆，重建流程删标记），
  §2.4 配套 2。
- **验收**：单调性（并发取号无重复无回退）；发号即登记原子可见；重启后
  拒发号且报错指明重建流程；删标记后恢复服务。
- **实施记要（2026-08-13）**：新 GUC `pg_partdist.tso_master`（默认 off，
  只有 master 置 on，非 master 收到调用一律 ERROR——防误连开出第二纪元）+
  `pg_partdist.tso_lease_ms`（登记租约，T3.5 消费，本期落库即填）。
  boot 标记顺序铁律：**标记持久（文件+目录 fsync）先于第一个号发出**——
  崩在中间只多一次"误拦重启"（删标记恢复），绝不"发过号却检测不到"。
  发号即登记语义：oldest=0（无活跃）⇒ 新号即成为该节点最老活跃；否则
  min(携带值, 新号)；同一排它锁临界区内完成，无竞态窗。观测入口
  `partdist_tso_status()`（counter/served/blocked/逐节点登记，验收与 T3.5
  断言用）。验收 19/19：非 master 拒服务；首号=1 且标记落盘；发号即登记
  两态（无携带→新号、携带 2→min=2）；**并发唯一性 2 后端×200 全唯一且
  会话内单调**；重启拒发号响亮停摆（start/commit 双入口 + status
  blocked=t，里程碑门禁实测）；删标记重启新纪元从 1 起。金丝雀 r2 单跑
  50/0（shmem 布局/GUC 面变更零扰动）。内核零改动。

#### T3.2 worker 取号通路（后端 libpq + 懒取 + fail-closed）——✅ 已完成（2026-08-13，`src/tso_client.c`）

- **改**：worker 后端缓存到 coordinator 的 libpq 连接（会话生存期，断线
  重连一次，仍失败即 ERROR——绝不本地时钟顶替）；首次触达分片表懒取
  start_ts 存后端事务态（XactCallback 清理）；提交路径在提交记录**之前**
  取 commit_ts（P3 单分片：合法窗口 = 写集确定后、决议持久化前，"尽晚取"）。
- **验收**：每事务恰一次 start_ts RPC（含只读）；TSO 停摆时分片表读写
  fail-closed 报错、原生表不受扰；连接断后自愈一次。
- **实施记要（2026-08-13，两处方案落地调整）**：① 地址来源从 node_map 改
  **GUC `pg_partdist.tso_conninfo`**——node_map 查询要 SPI，可见性钩子上下文
  用不了（与 T2.7 同因）；② **遗留模式**：conninfo 空 ⇒ 不 RPC、ts 一律 0
  （P1/P2 语义原样），否则 596 基线的分片写会在 PRE_COMMIT 撞 TSO 报错；
  fail-closed 只对"已配置但不可达"生效，strict 收紧归 T3.6。
  pg_partdist 自链 libpq（PGXS `SHLIB_LINK += -lpq`，ldd 实证）。发号即登记
  携带值 = 本节点活跃快照集合（shmem 每后端一槽，取号前算 min 随行上报，
  事务结束清槽；槽满只降 GlobalSafeTs 精度不拦事务——偏小=安全方向）。
  commit_ts 挂 `XACT_EVENT_PRE_COMMIT`（xact_map_n>0 才取；R-P3-1 第一道
  防线：此处 ERROR=干净中止）。**实测修掉两个通路 bug**：RPC SQL 字面量
  `0` 解析成 int4 撞不上 (int,bigint) 签名（显式 CAST）；节点号误用 GUC
  原始值 -1（改走既有 `PartDistLocalNodeId()` 解析）——顺带把错误路径改成
  "先存 libpq 错误串再弃连接"，否则 errdetail 永远只剩"连接建立失败"。
  验收 13/13：同事务缓存命中/跨事务递增/commit>start；发号即登记携带实测
  （B 携带 A 的活跃快照上报）；TSO 停摆 fail-closed 且原生表零扰；boot
  防呆经 RPC 传导仍 fail-closed；重建后断连自愈；遗留模式双 0。金丝雀
  test_shard_xid_p1 45/45（遗留模式含新 PRE_COMMIT 臂）+ r2 50/0。

#### T3.3 ts 落账与真 SI 可见性——✅ 已完成（2026-08-13，0007 原位改造 v2 296 行）

- **改**：commit 记录体携带 commit_ts（方案按 T3.0 ③）；RUNNING 落账填
  start_ts、判决落账填 commit_ts（正常路径回调 + 0007 redo 双路一致）；
  可见性判定换 §4.1 原文：COMMITTED 且 commit_ts < start_ts 才可见
  （xmax 对称）；后端终局缓存连 ts 一起缓存。
- **验收**：**不可重复读消失**（A 开始后 B 提交，A 反复读同快照不变——
  P2 近似 RC 时代读得到，P3 读不到）；跨事务序一致；崩溃后 redo 重建的
  ts 与崩前一致（值级断言）。
- **实施记要（2026-08-13）**：内核 = 0007 原位改造重生成（反打 0008→反打
  0007→v2 替换表→重打 0008，两补丁均正/反 dry-run 自检）：块头
  {nxids, commit_ts_lo, commit_ts_hi}；收集钩子签名扩 `uint64 *commit_ts`
  （临界区内只拷 PRE_COMMIT 暂存，abort 路径暂存缺席=0 属正常，由 redo 的
  committed 标志区分——**原拟的钩子内 PANIC 第二防线撤销**：钩子分不清
  commit/abort 构造路径，而 PRE_COMMIT 回调在一切提交路径必先行，第一道
  防线已完备）；redo 钩子扩 `uint64 commit_ts`；desc 打印
  `shard commit_ts:`。扩展 = SetRunning 带 start_ts、SetVerdict 带
  commit_ts 且**读改写保留 start_ts 列**（§5.3 五列）、新增 ReadSlot 整槽
  读 + `partdist_shard_clog_read_full` 观测函数；`shard_xid_state` 扩
  cts 出参、终局缓存带 ts；SI 判定：my_ts=TsoGetStartTs() 懒取（首触可能
  在持缓冲区锁下一次 RPC——P3 接受，已注释），COMMITTED 且 cts≥my_ts ⇒
  不可见，xmax 对称（删除在快照后 ⇒ 行仍可见）；遗留双向兼容：my_ts=0
  读者退回 P2 语义、cts=0 历史行对一切快照可见。验收 14/14：ts 三列落账
  （cts>sts>0）；**不可重复读消失实测**（并发插入 2,2 不变）；**删除对称
  实测**（3,3 不变）；崩溃 redo commit_ts 值级一致 + waldump 可辨；遗留
  读者看全量。金丝雀五套（lwc/r2/P1×2/P2）见下。修掉一个隐患：
  cstring_to_text 隐式声明（缺 builtins.h，int 截断指针）。

#### T3.4 §4.4 冲突中止（first-committer-wins）

- **改**：行锁等待唤醒后加判定：xmax 持有者已提交且 commit_ts >
  本事务 start_ts ⇒ `serialization failure` 中止（SQLSTATE 40001）；
  不提供 RC 式 EPQ 重读（挂点在既有 xmax_wait/satisfies_update 臂上收敛）。
- **验收**：经典 SI 双写用例（并发 UPDATE 同行，后提交者 40001）；
  不冲突路径（对方先回滚 / commit_ts < start_ts 的历史提交）不误报。

#### T3.5 GlobalSafeTs（机制就位，消费方 P5）

- **改**：master 登记表（节点→最老活跃 ts + 租约期限）；搭车通道（T3.1 已
  含）+ 周期心跳（worker 侧挂现成 bgworker 周期任务，无事务也报"最老或
  '无'"）；租约到期清登记；栅栏最小面（按 T3.0 ⑤）；读出函数
  `partdist_global_safe_ts()`（P5 vacuum 的地基 + 验收观测点）。
- **验收**：GlobalSafeTs 单调不减；≤ 全集群活跃快照最小 start_ts（并发
  churn 下断言不变式）；停心跳 → 租约到期后该节点被剔除、SafeTs 恢复推进；
  栅栏先于剔除生效。

#### T3.6 安全网严格模式收紧（§9.2 第 1 层语义到位）

- **改**：strict 语义从"分片表一切读写拦截"收紧为设计原文"**无 start_ts 读 /
  无 gxid 写才拦**"（P4 前 gxid 判据 = 分片 xid 绑定存在）；正常路径自动取号
  后 strict 下应全部放行，只有 TSO 停摆/旁路访问才拦。
- **验收**：strict 下正常读写全通过；人为清后端 ts 状态/停 TSO 后读写被拦；
  permissive 行为不变。

#### T3.7 P3 验收套件（出口门禁：竞态注入 + boot 防呆）

- `tests/test_tso_si_p3.sh`：SI 语义矩阵（不可重复读消失/40001 双写/只读
  零协调）+ **竞态注入**（并发提交 vs 快照的时机定理断言：C < S ⇒ 必可见，
  循环压测零异常；GlobalSafeTs 不变式并发断言）+ boot 防呆（重启拒发号）
  + TSO 停摆 fail-closed。工程纪律沿用（计数守卫/</dev/null/健康收尾）。
- 596 基线（13 套件）保持绿。

#### P3 出口清单（全部勾掉才进 P4）

- [ ] 真 SI 语义用例全绿（不可重复读消失；40001 first-committer-wins）
- [ ] 竞态注入零异常（时机定理断言 + GlobalSafeTs 不变式，里程碑门禁）
- [ ] boot 防呆生效（重启拒发号响亮停摆，里程碑门禁）
- [ ] TSO 停摆 fail-closed（绝不本地时钟顶替）；严格模式收紧语义到位
- [ ] GlobalSafeTs 机制就位（双通道/租约/栅栏最小面 + 读出函数）
- [ ] 全量 596（13 套件）+ P3 新套件零新增 FAIL
- [ ] 文档/补丁/pg-install 成对；Proxy 推迟裁定落档（若 T3.0 ⑥ 定案）

---

## 4 未决核实点跟踪表

| 编号 | 内容 | 归属 | 状态 |
|---|---|---|---|
| V1 | 原生表 hint 位 vs pagecmp 既有处理（§4.5） | T1.0 | ✅ 已完成（P1_PRECHECK 结论 A） |
| V2 | Citus 连接建立点枚举完备性（§9.2 ①） | P4 前 | 未做 |
| V3 | 引用表使用现状与只读裁定（§9.2 ②） | P4 前 | 未做 |
| V4 | CIC/CLUSTER 分叉 vs 禁用裁定（§11） | P2 期间定 | ✅ 已裁定（2026-08-13 T2.6：第一期禁用，理由入设计 §10；P5 出口再评估 CLUSTER/VACUUM FULL，CIC 随索引专项） |
| V5 | Citus 13.1 worker 驱动 2PC 行为一致性（§9.1 实验一） | P4 前 | 未做 |

---

## 5 风险登记簿（P1 视角 Top 4）

1. **redo 确定性**（T1.5）：任何"leader 路径写了、记录体没带全"的字段都会让三方
   pagecmp 撕裂——开发顺序上先写 redo 侧再写正常侧，逼自己把信息全部进记录体。
2. **"一事务一 xid"的隐蔽消费者**：内核里按 `GetCurrentTransactionId()` 参与分片表
   逻辑的调用点可能有漏网（组合索引：combocid、toast、trigger 路径）——P1 用例
   必须覆盖 TOAST 列与触发器写。
3. **普通表回归面**：所有分叉必须"分片表判定为假 ⇒ 与原逐字节同路径"，靠全量
   既有套件兜底，每个补丁提交前跑。
4. **临时桩滞留**：T1.6 临时提交表若拖过 P2 不替换，会被后续代码依赖——出口清单
   与代码注释双重标记。

P2 增补（2026-08-13，T2.0 核查产出）：

5. **R-P2-1 解析面回归**：0007 触碰 commit/abort 记录解析（xactdesc/redo/解码
   三处共用）——无分片块的记录必须逐字节零行为变化；438 全量兜底。
6. **R-P2-2 认领误杀**：认领扫描上界一旦取成动态发号水位，会把重启后新活事务
   判 ABORTED（数据静默消失）。上界必须是冻结的启动恢复上限（P2_PRECHECK
   结论四）；T2.8 必须含"重启后立即开新事务再触发认领"用例。

P3 增补（2026-08-13，T3.0 核查产出）：

7. **R-P3-1 无 ts 判决落账**：分片写事务的判决若带 commit_ts=0 落账，该行对
   一切快照恒可见（0 < 任何 start_ts）= SI 静默破坏。双防线：PRE_COMMIT 取号
   失败即 ERROR（临界区外干净中止）；收集钩子（临界区内）发现暂存缺失即
   PANIC——宁可崩溃恢复也不写 0 值判决。T3.7 须含"停 TSO 提交分片写"用例。
8. **R-P3-2 双 ts 宇宙串线**：TSO 逻辑值与 MARKER/DTX 本地时钟占位值在 P3
   并存（实证当前零比较点）；任何新代码不得让两者进入同一比较；P4 换源前
   全量重审计。

---

*本文与 TX_TSO_MVCC_DESING.md 配套使用；方案性问题以设计文档为准，本文只管
"怎么做、什么顺序、怎么验收"。每期出口时回填本文勾选项与跟踪表。*
