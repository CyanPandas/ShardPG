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
- **P4**：已进期，细化为任务级分解见 **§3.3**（T4.0–T4.7，2026-08-13；
  pg_raft 解冻做成显式闸门，仅 T4.4 在闸门后）。
- **P5**：**已进期**，细化为任务级分解见 **§3.4**（T5.1–T5.7，2026-08-18）。
  原里程碑级描述：vacuum 三变量 + 前缀规则 + 页面三类动作 + 洁净位图 + CTRL 水位落地 +
  回卷两阶段护栏（§6/§7 全章）。
- **P6**：**已进期**，细化为任务级分解见 **§3.8**（T6.0–T6.9，2026-09-02；
  pg_raft 解冻沿用 P4 的显式闸门做法，仅 T6.3b/T6.4 在闸门后）。
  原里程碑级描述：切主全链路（含 §6.6 切主认领、§5.5 水位新语义）联测；
  全部负向用例并入门禁；文档终检。

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

#### T3.4 §4.4 冲突中止（first-committer-wins）——✅ 已完成（2026-08-13）

- **改**：行锁等待唤醒后加判定：xmax 持有者已提交且 commit_ts >
  本事务 start_ts ⇒ `serialization failure` 中止（SQLSTATE 40001）；
  不提供 RC 式 EPQ 重读（挂点在既有 xmax_wait/satisfies_update 臂上收敛）。
- **验收**：经典 SI 双写用例（并发 UPDATE 同行，后提交者 40001）；
  不冲突路径（对方先回滚 / commit_ts < start_ts 的历史提交）不误报。
- **实施记要（2026-08-13）**：改动收敛在 `sv_satisfies_update` 一处
  （等待唤醒后 goto l1/l2 重评也到这里，无需另设等待臂检查）：xmax
  COMMITTED 且 cts ≥ my start_ts ⇒ **直接 ereport 40001**（标准报错文案
  "could not serialize access due to concurrent update" + errdetail 带两个
  ts 值）——在扩展层 ERROR 使 EPQ 机器根本不启动，任何 PG 隔离级别下行为
  一致；xmin 分支加对称防御（快照后诞生的行 TM_Invisible）。cts < my_ts
  的历史提交结构性到不了该分支（那样的行对本快照不可见、扫描不选中）。
  遗留模式（my_ts=0）保持 P2 行为（TM_Updated → EPQ 撞行锁禁令）。
  验收 11/11：经典双写 40001（胜者值保留）；对方回滚不误报（本方成功）；
  顺序提交不误报；快照内撞并发 DELETE 提交 40001。金丝雀 P1 45/45 +
  P2 64/64（遗留冲突路径不变）。

#### T3.5 GlobalSafeTs（机制就位，消费方 P5）——✅ 已完成（2026-08-13）

- **改**：master 登记表（节点→最老活跃 ts + 租约期限）；搭车通道（T3.1 已
  含）+ 周期心跳（worker 侧挂现成 bgworker 周期任务，无事务也报"最老或
  '无'"）；租约到期清登记；栅栏最小面（按 T3.0 ⑤）；读出函数
  `partdist_global_safe_ts()`（P5 vacuum 的地基 + 验收观测点）。
- **验收**：GlobalSafeTs 单调不减；≤ 全集群活跃快照最小 start_ts（并发
  churn 下断言不变式）；停心跳 → 租约到期后该节点被剔除、SafeTs 恢复推进；
  栅栏先于剔除生效。
- **实施记要（2026-08-13）**：master 侧 = `partdist_tso_heartbeat(node,
  oldest)`（续租即报最老，返回 lease_ms 供栅栏计算）+ 惰性过期清扫（safe
  计算时租约过期登记清"无"，无需 master 侧定时器）+ `tso_compute_safe_locked`
  （候选=min(租约内活跃 oldest)，全无活跃=counter；**单调钳制**，候选倒退
  WARNING 保存量）+ 读出/status 扩 safe 字段。worker 侧 = **新常驻心跳
  bgworker**（每节点一个，lease/3 周期，未配置 conninfo 时静默轮询；失败不
  死由栅栏兜底）+ **栅栏**挂 TsoGetStartTs 缓存命中路径（now > last_beat +
  lease−lease/4 ⇒ ERRCODE_SNAPSHOT_TOO_OLD"栅栏作废"，先于 master 到期剔除
  生效）；续租登记 = start_ts RPC 与心跳成功（commit_ts 不续租不算数）。
  **实测抓出一个 P0 级坑**：bgworker 无数据库连接，`PartDistLocalNodeId()`
  的 Citus catalog 查询直接 SIGSEGV，且 bgworker 崩溃 → postmaster 全进程
  reinit → 秒级循环拖垮节点（"节点崩溃的不可见性"再现，worker1 一度 221 次
  段错误 + 卡 shutting down 需 immediate 重启）——修法：节点号缓存进
  shmem（首个取号后端写入，worker 只读；未知即跳过心跳——没人取过号就没有
  快照需要续租）。测试法教训：单次 `-c` 多语句是一个 PQexec 整体返回，
  后台会话取中间值必须走 stdin 逐语句。验收 11/11：safe==最老活跃（实测
  钉在 A 的 start_ts）；活跃清空后随心跳推进且单调；伪节点租约 3s 过期后
  safe 越过；**栅栏实测**（coordinator 停机 ~2.25s 后快照内第二次读被作废，
  先于 3s 剔除）。金丝雀 r2 50/0 + P1 45/45 + P2 64/64。

#### T3.6 安全网严格模式收紧（§9.2 第 1 层语义到位）——✅ 已完成（2026-08-13）

- **改**：strict 语义从"分片表一切读写拦截"收紧为设计原文"**无 start_ts 读 /
  无 gxid 写才拦**"（P4 前 gxid 判据 = 分片 xid 绑定存在）；正常路径自动取号
  后 strict 下应全部放行，只有 TSO 停摆/旁路访问才拦。
- **验收**：strict 下正常读写全通过；人为清后端 ts 状态/停 TSO 后读写被拦；
  permissive 行为不变。
- **实施记要（2026-08-13）**：ShardAccessGate 一处收敛——strict 准入 =
  `TsoGetStartTs() != 0`（读写统一：绑定必伴随取号，P4 前 gxid 判据即此）。
  三种结局：TSO 已配置 → 自动取号放行；遗留模式（未配置=旁路无 ts）→ 明确
  拦截报"无 ts"；TSO 停摆 → 取号自身 fail-closed ERROR。新文案保持"读被
  拦截/写入被拦截"子串，P1 套件遗留模式 strict 负向不改自绿。验收 10/10
  （strict+TSO 读写增删全放行/停摆拦/遗留读写双拦/permissive 不变），
  金丝雀 P1 45/45 + P2 64/64。

#### T3.7 P3 验收套件（出口门禁：竞态注入 + boot 防呆）——✅ 已完成（2026-08-13，`tests/test_tso_si_p3.sh` 38 项）

- `tests/test_tso_si_p3.sh`：SI 语义矩阵（不可重复读消失/40001 双写/只读
  零协调）+ **竞态注入**（并发提交 vs 快照的时机定理断言：C < S ⇒ 必可见，
  循环压测零异常；GlobalSafeTs 不变式并发断言）+ boot 防呆（重启拒发号）
  + TSO 停摆 fail-closed。工程纪律沿用（计数守卫/</dev/null/健康收尾）。
- 596 基线（13 套件）保持绿。
- **实施记要（2026-08-13）**：t31–t36 六个散装验收收编为 38 项九段正式
  套件。**竞态注入的时机定理表达**：只增负载下（writer 40 笔单事务插入 ‖
  checker 40 轮独立快照计数），"C < S ⇒ 必可见"等价于"读者计数序列随快照
  序单调不减"——awk 断言零倒退 + 终态全量 + GlobalSafeTs 单调，实测零
  异常。收编抓出两处套件断言问题（非产品）：master SQL 函数没在 worker
  建，"拒服务"负向实际报 does not exist；fail-closed 断言数 ERROR 行数会
  把 errdetail 内嵌的远端 "ERROR:" 串重复计入——按特征文案计数。
  单跑 **38/38 绿**；出口 14 套件全量见出口清单。

#### P3 出口清单（全部勾掉才进 P4）——✅ 全勾（2026-08-13，P3 结项）

- [x] 真 SI 语义用例全绿（不可重复读消失；40001 first-committer-wins）
      （T3.3 14/14 + T3.4 11/11 + 套件 [3][4] 实测：快照内 2,2/3,3 不变、
      双写后提交者 40001 胜者保值）
- [x] 竞态注入零异常（时机定理断言 + GlobalSafeTs 不变式，里程碑门禁）
      （套件 [6]：只增负载 40‖40 读者计数零倒退 + 终态全量 + safe 单调）
- [x] boot 防呆生效（重启拒发号响亮停摆，里程碑门禁）
      （T3.1 + 套件 [8]：重启拒发号/status blocked/worker 传导 fail-closed/
      删标记新纪元从 1 起）
- [x] TSO 停摆 fail-closed（绝不本地时钟顶替）；严格模式收紧语义到位
      （T3.2/T3.6 + 套件 [7]：strict+TSO 全放行、遗留无 ts 拦、停摆拦）
- [x] GlobalSafeTs 机制就位（双通道/租约/栅栏最小面 + 读出函数）
      （T3.5 11/11 + 套件 [5]：safe 跟踪最老活跃/单调/租约剔除/栅栏先于剔除）
- [x] 全量 596（13 套件）+ P3 新套件零新增 FAIL
      （2026-08-13 出口回归 **14 套件 634/634 FAIL=0**；首轮 tx3 掉 2 条经
      定性为负载时序竞态（applied 达标 ≠ 判决可见同刻，单跑即绿、机制早于
      P3）——夹具改有界等待收敛断言 + 滞后打印，重跑全绿且零滞后）
- [x] 文档/补丁/pg-install 成对；Proxy 推迟裁定落档（若 T3.0 ⑥ 定案）
      （0007v2 随 T3.3 成对入库；Proxy 推迟并入 P4 = P3_PRECHECK 结论六）

### 3.3 P4 详细任务分解（2026-08-13 进期细化）

**目标**：多分片 2PC 端到端（设计 §3 七步流程）——协调者=首写分片、globalXID、
commit_ts 两时机、决议搬迁到协调者分片 leader、连接加入协议、三态问询。
出口门禁（里程碑原文）：**崩溃矩阵逐格演练**（§3.4 四行）。

**★ pg_raft 解冻闸门**：pg-raft-src 冻结是用户裁定，**不得自行解除**。分解上
把触碰面收敛进唯一任务 T4.4（决议搬迁），其余任务全部不碰 pg_raft 可先行；
T4.0 产出解冻范围清单（含流控 #39 存档补丁 `~/pg_raft_flowcontrol_39.patch.bak`
是否一并重放的选项）后**暂停等用户批准**，批准后方可进 T4.4。

**依赖图**：

```
T4.0 ──┬─ T4.1 ─┬─ T4.2 ─ T4.6
       │        └─ T4.3 ─ T4.5 ─┐
       └─[解冻闸门·用户批准]─ T4.4 ─┴─（T4.7 出口统跑）
T4.7 崩溃矩阵/门禁套件与 T4.3 起并行开发
```

#### T4.0 前置核查 + §9.1 三实验（不改产品代码，产出 `docs/P4_PRECHECK.md`）——✅ 已完成（2026-08-13，解冻申请待批）

- ① **§9.1 三实验**（未通过不得进入实施，设计原文）：MX 元数据同步现状
  （建集群脚本的加节点方式）；Citus 13.1 worker 驱动"含 PREPARE 的多分片写"
  与 master 驱动逐项一致（兼 V5）；worker 驱动时本分片写走 local execution，
  `wal_insert_hook` 与 DTX 参与者捕获行为不变。
- ② **V2**：Citus 连接建立点枚举（普通查询/COPY/repartition/中间结果），
  连接加入协议的挂点完备性。
- ③ **V3**：引用表使用现状（已交付系统是否有引用表写；第一期倾向建表后只读）。
- ④ **gxid 分配器落位**：§2.1 编码（节点16b|序号48b）+ 批量水位持久化的
  实现载体（协调者节点侧，复用水位文件纪律 vs 新文件）；与既有 gxid 编码
  `(node<<48)|xid` 的兼容面（§9："低位换分片 xid，含义不变"）。
- ⑤ **start_ts 跨连接传播通道**：参与者后端不自取 ts、改用协调者下发值——
  tso_client 需"外部注入"通道；与懒取/栅栏/活跃集合登记的交互（注入的 ts
  是否登记进本节点活跃集合——须登记，否则 GlobalSafeTs 会越过远端读）。
- ⑥ **PREPARE 侧持久化方案**：分片 clog 写 PREPARED 的时机与载体；
  `twophase.c` 2PC 状态文件扩 (分片,xid) 列表（§8-①，预计补丁 0009）；
  P1 的 PREPARE 禁令按"已 join 全局事务"放行的条件收口。
- ⑦ **解冻范围清单**：`dtx_master_pre_record_commit` 搬迁触及
  raft_consensus.c 的函数面；`check_fastpath_divergence` 等改读分片 clog 的
  连带；流控 #39 补丁是否搭车重放（用户存档，列为用户决策项）。
  **产出后暂停，等用户批准解冻。**
- **验收**：三实验有实测结论；V2/V3/V5 核实点表更新；解冻清单落档。
- **实施记要（七问定案，全文见 `docs/P4_PRECHECK.md`）**：
  ① **三实验全过，MX 放行**——9/9 节点 metadatasynced=t（citus_add_node
  默认同步，建集群脚本零改动）；worker 驱动 2PC 与 master 逐项一致（:5434
  取证 PREPARE/COMMIT_PREPARED 双向；唯一差异=连接并行度 3 vs 1，语义无差
  → R-P4-1）；local execution 证词实测 + wal_insert_hook 位于 XLogInsert
  层结构性不变。纪律实测：worker recover_2pc_interval=-1、Citus 居首 ✓。
  ② V2 挂点运行时实证（参与节点日志抓到 assign_distributed_transaction_id
  每连接前置）；完整枚举 T4.1 同法逐路取证+安全网兜底。
  ③ V3=**系统现役零引用表**，裁定第一期建表后只读。
  ④ gxid 以 §2.1 为准（§9 表述指旧账本键迁移），每节点 shmem 计数器 +
  pg_gxid_wm 水位文件（复用 shard_xid 纪律）。
  ⑤ start_ts 注入通道 TsoInjectStartTs——**必须登记本地活跃集合**，否则
  GlobalSafeTs 越过活跃远端读（本核查抓出的关键点 → R-P4-2）。
  ⑥ 0009 = twophase 状态文件扩 (分片,xid) 列表 + clog PREPARED 落账
  （放行条件=已 join）。
  ⑦ **解冻清单**：raft_consensus.c 单文件 dtx_master_pre_record_commit
  （7019–7258 约 240 行）+ 闸门/组选择，安装点不动；check_fastpath_
  divergence 两仓无同名符号（描述性名称）不扩面；DtxRecordPayload 换源在
  pg-partdist 不在冻结面；流控 #39 存档与搬迁正交，选项 (a) 另行立项
  （推荐）/(b) 搭车——**2026-08-13 用户裁定：批准解冻，选 (b) #39 搭车
  重放**（P4 验收连带背流控回归；health_check_no_drops 转实测）。闸门
  开启，T4.4 可动工，范围外 pg-raft 改动仍禁止。
  V2/V3/V5 核实点表已更新；R-P4-1/R-P4-2 入 §5。本任务零产品代码。

#### T4.1 连接加入协议 + gxid 分配器（§9.2 第 2 层；不碰 pg_raft）——✅ 已完成（2026-08-13）

- **改**：`partdist_join_global_txn(gxid, start_ts, coord_gsid)`——参与者后端
  登记三元组（start_ts 注入 tso_client 通道 + 活跃集合登记；coord_gsid 进
  dtx_participant 既有载体）；协调者 worker 每开参与分片连接先发此调用
  （借 `assign_distributed_transaction_id` 同位置机制，不改 Citus）；
  gxid 分配器按 T4.0 ④ 落位。
- **验收**：join 后远端后端读分片表用注入 ts（跨连接快照一致）；未 join 的
  远端写分片表被安全网拦（§9.2 第 1 层已就位）；gxid 崩溃安全不重号。
- **实施记要（2026-08-13）**：交付三件：① `src/gxid.c`——每节点 shmem
  计数器 + `pg_gxid_wm` 水位文件（8B、批量 4096、先落盘后发号），编码
  (节点16b<<48)|序号48b，`partdist_gxid_next()`；② 注入通道
  `TsoInjectStartTs`——置 cur_start_ts（不自取不 RPC）+ **登记本节点活跃
  集合**（R-P4-2）+ 未配置 TSO 节点拒绝注入（无心跳/栅栏保护即脱离
  GlobalSafeTs 视野，fail-closed）+ 改注拒绝；③ `partdist_join_global_txn`
  直调形态 + 后端三元组状态与 getters（T4.3/T4.4 消费）。
  **通道定案（T4.2 设计输入）**：既有 DTX 身份靠 PREPARE GID 解析（PREPARE
  时刻才有），读路径需更早；Citus 13.1 对"每连接前置"无公开扩展点 ⇒ 自动
  传输 = **发起端登记表 + 参与端回拉**（发起端按 (initiator_group,
  citus_tx_number) 登记三元组进 shmem，参与端首触分片表时经 node_map 定位
  发起端回拉一次），随 T4.2 与 MX 路由一体接线；本任务以直调形态过验收。
  验收 19/19：**跨连接快照一致实测**（参与端 tso_c_start 返回注入 S、快照内
  2=2 看不见 S 后提交）；**R-P4-2 实测**（远端持注入快照期间 safe 钉在 S、
  释放后推进）；未 join/无 ts 写被拦、未配置节点拒 join；改注/改投拒绝；
  gxid 编码/单调/崩溃续发不重号（immediate 崩后序号 4097）。金丝雀
  P3 38/38 + P2 64/64。

#### T4.2 MX 拓扑与路由（含 Proxy 粘性路由并入）——✅ 已完成（2026-08-13，两处范围裁定见记要）

- **改**：元数据同步启用（按 T4.0 ① 实验结论补建集群脚本）；
  `raft_update_citus_placement` 翻转传播到所有持元数据节点（§9.1 配套）；
  Proxy 粘性路由 + 会话→协调者映射（P3 移账，与连接协议一体设计）。
- **验收**：worker 驱动多分片写路由正确；切主后旧 leader 写被 raft 写栅栏
  挡下（可重试错误而非损坏，§9.1 依据 3 实测）。
- **实施记要（2026-08-13，含两处范围裁定）**：
  **通道升级**：T4.1 定案的"登记表+回拉"被更优方案取代——实验实证
  `SET LOCAL 自定义 GUC` 经 `citus.propagate_set_commands='local'` 原样
  传播到每条任务连接（Citus 官方机制，每连接一次）。落地：新 GUC
  `pg_partdist.join_info='gxid,ts,gsid'`（USERSET，check 校验格式；assign
  只暂存——assign 上下文不许 ereport，真正注入推迟到 TsoGetStartTs 首取，
  冲突/未配置以正常 ERROR 收口；事务结束 GUC 回卷+回调双清）。驱动端只需
  事务内两条 SET LOCAL；T4.4 把发出动作接进驱动端首写路径。
  验收 5/5：**8 分片×8 worker 分布查询全部任务取到同一注入 S**（DISTINCT
  恰一值）；对照组自取 >S；非法格式 check 拒。金丝雀 P3 38/38+P2 64/64。
  **裁定一（placement 传播移账 T4.4）**：`raft_update_citus_placement` 在
  pg-raft-src/raft_apply.c——批准解冻范围（raft_consensus.c 决议搬迁+#39）
  之外，本任务不碰；并入 T4.4 的 pg_raft 批次，届时请用户确认范围加一行。
  **裁定二（Proxy 独立组件推迟）**：P4 运行模型=客户端直连协调者分片
  leader（测试夹具即此），连接加入协议实测不需要独立 Proxy 进程——会话→
  协调者映射作为运行纪律文档化，独立 Proxy 守护组件推迟到 P6 全链路/部署
  专项（连续第二次因"无消费方"推迟，若 P6 仍无消费方应考虑从方案中裁掉）。
  元数据同步=零改动（T4.0 实验一）。写栅栏行为属 pg_raft 既有（§9.1 依据
  3），由基线 tx3/tx4 套件持续覆盖，本任务不重复造验收。

#### T4.3 PREPARED 落账 + 三态问询（§4.2）——✅ 已完成（2026-08-13，`patches/0009-shard-twophase-rmgr.patch` 136 行；③ 问询移 T4.5）

- **改**：PREPARE 路径分片 clog 写 PREPARED（含 parent 链起用）；补丁 0009
  （twophase 状态文件扩列，方案按 T4.0 ⑥）；读者撞 PREPARED 三态：
  ① entry start_ts > S 跳过；② coord_gsid NULL 跳过（NULL 不变式）；
  ③ 问协调者组 **leader**，判决幂等回写本分片 clog（读者绝不安装 ABORT）。
- **验收**：三分支逐一用例；in-doubt 期间读者不阻塞；回写幂等。
- **实施记要（2026-08-13，一处任务边界裁定）**：**③ 问询实现移 T4.5**——
  三态里 ①（槽 start_ts > 读者快照）与 ②（gsid 未知=NULL 不变式）的结局都
  是"跳过=不可见"，与 PREPARED 兜底行为一致；带可见性翻转的 ③ 与决议广播
  是同一收敛机器，拆开做两遍不如一体落地。内核 0009：twophase rmgr 增
  SHARD 槽位（recover/postcommit/postabort 经钩子分发）+ **at-prepare 钩子**
  ——实测顺序坑：PRE_PREPARE 回调先于 StartPrepare，在那里注册 2PC 记录会被
  StartPrepare 重置吞掉，注册点必须与 AtPrepare_* 同位。扩展侧：PREPARE 放行
  条件 = 已 join（gxid 在手），未 join 维持 P1 禁令、含 DROP 仍禁；
  `ShardClogSetPrepared`（durable，投票持久前 PREPARED 已持久，带
  start_ts+gxid）；2PC 段载荷 {gxid, start_ts, pairs}；recover 幂等重建；
  postabort 写 ABORTED；**postcommit 不写终局**（写 ts=0 的 COMMITTED 会
  破坏 SI——判决+commit_ts 走 T4.5 决议广播，interim 行为=PREPARED 保持、
  读者按 in-doubt 处置）。验收 17/17：未 join 拒/放行落账（st=1 带 sts）/
  读者 0s 即返不可见/**崩溃恢复双重建**（原生 prepared + clog PREPARED，
  T2.4 认领对 PREPARED"不许动"首次实测）/ROLLBACK PREPARED→ABORTED/
  COMMIT PREPARED→interim PREPARED。金丝雀 P1 45/45+P2 64/64+P3 38/38
  （PREPARE 禁令文案改动与既有负向子串兼容自绿）。pg-install 成对同步，
  README 至 0009（nm 十二行）。

#### T4.4 决议搬迁 + #39 流控重放（★ 解冻已批准 2026-08-13 选项 b）——✅ 已完成（2026-08-13，两步交付 1c22a31+455c935；验收：#39 重放金丝雀 lwc 9/r1 58/l1 57/d1 84 全 0，决议搬迁 39/0，换源后 tx1/tx2/tx4 仍绿）

- **改**：`dtx_master_pre_record_commit` 整块从 master 搬到协调者分片
  leader（0004 钩子触发点 = "全部 PREPARE 已成功"点，§3.2）；commit_ts
  两时机接线（全部票齐后取，verdict + commit_ts **一条原子记录**协调者
  raft 组多数派落盘 = 提交点，客户端 ACK 在其后）；`DtxRecordPayload.
  commit_ts`/`dtx_decision.commit_ts`/MARKER ts 换 TSO 源（P3_PRECHECK
  结论一的 P4 收口，换源前按 R-P3-2 重审计双宇宙比较点）；
  `check_fastpath_divergence` 等改读分片 clog。
- **改（增，用户裁定 b）**：重放 `~/pg_raft_flowcontrol_39.patch.bak`
  （661 行四层流控修复：背压/认领归还/批量 apply/2PC 行锁环拆解，
  FRD §13 约束 13 设计记录为准）。
- **验收**：跨分片写事务端到端提交/中止；提交点=多数派落盘实测；
  换源后 tx1/tx2/tx4 基线套件重写断言仍绿；**#39 流控回归**
  （health_check_no_drops 转实测；FPI 洪水下无 log ring full 分叉）。
- **实施记要·步骤①（2026-08-13，#39 重放先行落地）**：存档 661 行按 8/5 基线
  制成，TX 树已长千余行 → 11/17 块顺利、6 块拒绝按 .rej 手工移植（GUC 变量、
  双原型、init 六字段、apply 认领+批量合并+PG_TRY 归还、discard 计数、propose
  背压）。移植中抓到两处**存档与 TX 树的真语义冲突**：① #39 批量 apply 会吞
  掉 DTX DECISION(info=2)/FORGET(info=5) 的逐条登记（§6.2/§9.7）——落法：批
  量扫描按日志序收集 (plsn,info) 清单随批传入 `data_apply_advance`，游标合并
  推进、登记逐条落账（载荷经 partwal_read_dtx_record 读本节点已落盘字节，只
  需 plsn 不需条目本身）；② 存档的 IsTransactionBlock 闸门依赖当年冻结时
  **从未建成**的委托目标（BGW 无 SPI 干不了 apply；pg_raft_catchup 是
  leader→follower **补发**通道、不做本地 apply），leader 的 apply 只剩自动提
  交语句偶发排空——r1 实测显式事务洪水下 applied=0/commit=127、认领无人持
  有、apply 未报错，10s 背压等满后丢 2 条提案，新诊断 WARNING 的 errdetail
  一击定位；**闸门撤除**，其目标撞锁环由 TX 期 `in_txn_replication` 外科式跳
  过兜住（只跳撞锁 UPSERT、游标照常推进）。控制面 apply 保留
  `apply_one_entry_guarded`（8/5 毒丸修复不回退存档的裸调用）。
  `pg_raft_group_flow_stats()` 九节点手工 CREATE（扩展已装 1.0，改 .sql 文件
  不自动生效）。金丝雀：lwc 9/0、**r1 58/0**（无丢弃检查从"跳过"转实测后首
  次全绿）、l1 57/0、d1 84/0。步骤②（决议搬迁 + ts 换源 + placement 传播）
  继续。
- **实施记要·步骤②（2026-08-13，决议搬迁 + ts 换源，验收 39/0）**：
  ① **闸门 MX 化**（§9.1"换节点跑"定案落地）：撤除
  `pg_raft_coordinator_node_id` 身份判据；替补性能闸 = citus_internal
  application_name 前缀先行退出（参与侧任务提交零开销）+ pg_dist_transaction
  xmin 探针兜底。② **决议原子携带 TSO commit_ts**：dtx_decide 增第 5 参
  （SQL DEFAULT 0 + C 侧 PG_NARGS 兼容双签名；活库经 ALTER EXTENSION
  DROP/ADD 换签名，扩展成员身份保留）；驱动节点在"全部票齐"点经 rendezvous
  `partdist_tso_dtx_decision_ts_fn` 取新号（§2-4 两时机，不预取不缓存，
  fail-closed），**取号即覆盖 PRE_COMMIT 暂存** → 本地分片 0007 尾块与决议
  同 ts 自洽（MARKER 早值残留 → R-P4-3）；TSO 不可达 ⇒ 先尽力写显式 ABORT
  决议再抛（本段既有纪律）。③ **ts 换源收口**：`TsoMarkerCommitTs()`
  统一 MARKER/DTX_COMMIT 记录 ts 源（遗留=本地时钟逐字节不变，TSO=事务内
  暂存懒取，同事务三处同 ts）；partwal_sync/dtx_participant 两处换源。
  ④ **placement 传播确认**：raft_apply.c **零改动收案**——group0 apply 本就
  在每成员节点各自落本地 pg_dist_placement，MX 元数据同步使九节点都持表后
  传播天然达成（验收 9/9 一致实测）；已登记分片的裸提案（任期 0）被任期
  栅栏拦下 9/9 不动（翻转须走带任期的切主上报链，属 raft_14/15 机制）。
  验收 39/0：遗留决议 ts=本地时钟宇宙（8.4e14）/TSO 决议 ts=逻辑值且**决议
  行复制并 apply 到组内 follower**（多数派落盘 + #39 批量 DTX 逐条登记通道
  一并实证）/不持分片 worker 驱动决议照常/local_execution=off 工作绕法/
  **盲区显式复现**（R-P4-4）/fail-closed 拒事务+显式 ABORT/零崩溃零丢弃。
  夹具坑两枚：TX1 同款"没等 partition_map 登记"（收敛等待 + 单分片预热写
  修复，逐轮劣化即其表现）；PG TimestampTz 纪元 2000 年（2026≈8.4e14 µs，
  阈值断言别按 Unix 纪元写）。

#### T4.5 广播与恢复（崩溃矩阵行 1–3 的机制面）——✅ 已完成（2026-08-14，六段交付 9b9f53c→0b4b9ec，含解冻批次 #2/#3；验收 test_dtx_convergence_p4.sh **45/0 连续两轮**，金丝雀 8 套全 0；矩阵行 1 由 [7] 腿实测、行 2 由 [5]/[6] 腿实测、行 3 由 [7] 腿切主后决议+收敛实测——逐格演练仍按计划在 T4.7）

- **改**：决议异步广播、参与分片幂等落本分片 clog；协调者切主后：有决议
  继续广播、无决议按超时对未决事务决 ABORT（安全依据：决议不存在 ⇒ 未
  提交过）；参与分片切主 prepared 状态恢复（T2.4 认领 PREPARED 分支
  "不许动"衔接问询路径）。
- **验收**：广播丢失下读者问询收敛；矩阵行 1–3 单场景就绪（逐格统跑在 T4.7）。
- **实施记要·①进行中（2026-08-14，验收 34/7，机制主干全通）**：
  **架构定案 = 拉取收敛，零 pg_raft 触碰**：参与者 PREPARE 时登记
  (gxid, coord_gsid, dtxid, start_ts, pairs) 进节点级 shmem 表 + **持久日志**
  `pg_shard_clog/dtx_pending.jrnl`（OPEN fsync 先于 EndPrepare 刷盘 ⇒
  "prepared 存在 ⇒ 登记必在"；启动重放+压实；2PC 段 recover 兜底重建——
  两通道闭掉"COMMIT PREPARED 后崩溃"孤儿窗）。判决收敛三通道：读者③
  （可见性挂钩撞 PREPARED 且 sts≤S → 自连 dtx_inquire，每事务每 gxid 至多
  一次 RPC + memo 保同快照跨分片一致）、心跳工作者自连周期清扫、手动
  dtx_pending_sweep()。问询 = **只读 dtx_peek**（leader 门控，绝不写推定
  中止）；寻址权威 = 本地 dtx_participant.coord_gsid（§4.2 ②的 NULL 不变
  式载体），登记携带的 join gsid 只作兜底。2PC 段载荷 v2（+coord_gsid
  +dtxid，长度判别兼容 v1）。新 SQL：dtx_peek/dtx_inquire/
  dtx_pending_sweep/count/dump。**已实测通过**：登记 a=1 b=1、决议 cts=TSO
  值、清扫收敛（手动+心跳自动 4s）、ABORT 学习 st=3、无决议读者 0s 即返
  不可见、kill -9 后登记 2=2 重建+原生 prepared 恢复+切主后组有 leader。
  **未过（3 焦点）**：读者③ 自连 RPC 在 5433 静默失败（清扫通道同值全通
  →疑自连/超时问题）；[7] 崩后清扫需先等 partition_map 主翻转；崩后节点
  shard_identity 空（rebuild 是手动步）令恢复守护 dtx_status 撞"协调组在
  本节点没有对应分片"。**实施坑账（本轮抓获）**：① dtx_pending_dtxid 在
  PrePrepareFinish 后被 Reset 清零、at-prepare 钩子取 0 —— 粘滞变量修复
  （首轮 11 FAIL 唯一根因）；② shared_preload 库加新符号必须重启 postmaster；
  ③ 带白名单删分布表被"含 DROP 禁 PREPARE"拦下、表静默存活 → 假阳性
  连锁（teardown 必须先撤白名单并确认生效）；④ 本地拆表绕 2PC 会打散 MX
  元数据（各节点 pg_dist 分叉）→ 正解 start_metadata_sync_to_node 全量重推
  （节点名是 localhost 不是 127.0.0.1）；⑤ rebuild_shard_identity 在悬空
  pg_dist_shard 行上整体报废 → 已产品级加固（过滤悬空）；⑥ T3.5 心跳
  工作者 node_id 靠首个取号 backend 入 shmem，夹具必须"引流"否则首笔
  joined 事务撞 7500ms 租约栅栏；⑦ 合成 dtxid 跨轮撞旧决议行（验收要
  TRUNCATE dtx_decision 起步）。**风险登记**：R-P4-5 tx 时代回执/FORGET GC
  按"原生 prepared 已闭合"删决议，不知 TX2 的 clog 收敛还要用——收敛失败
  期间决议可能被提前遗忘（本轮实测目击）；处置方向：ack 条件并入
  pending-finalize（涉 pg_raft 守护，待解冻申请或改喂 MARKER 通道），T4.5②
  处理。
- **实施记要·①续（2026-08-14 第二轮，焦点收敛）**：
  ① **读者③根因落定并修复**：dtx_inquire 误声明 RETURNS TABLE（proretset）
  而 C 侧走单行复合返回协议 → SQL 调用必报 "set-valued function called in
  context ..."，被读者路径静默吞掉（清扫直调 C 核心不经 SQL 故独活）——
  改 OUT+RETURNS record，九节点换签名，SQL 通道实测可调。
  ② **"决议秒删"误判澄清**：时间线探针只盯了 A 组 leader，而协调组按
  dtxid%n 落在 B——决议一直都在；回执清扫实际带 5s 龄限+10s 守护节拍，
  R-P4-5 仍真实但窗口≥10s，收敛（≤4s 典型）可胜。
  ③ **新现象 R-P4-6：数据组选举翻覆**——每轮 E2E 后 ~60s 内夹具组被抢主
  （实测 term 4→6 连续竞选失败、5435 term7 胜出、切主链翻 partition_map、
  原 leader 阶段 3 本地写被栅栏拒）。翻覆期 peek 的 leader 门控合法拒答，
  收敛时延放大超过 45s 窗。嫌疑：monitor 循环同步 catchup 阻塞心跳 +
  测试节奏（每轮 kill -9）放大；tx1 同拓扑不翻。属既有基础设施韧性问题，
  T4.5② 立项排查，不阻塞机制验收。
  ④ **验收语义对齐设计**：断言改为"跟随 partition_map/组状态的有界最终
  收敛"（wait_converge 逐秒轮询+每轮触发读者③与清扫；decide_retry 在现任
  leader 上写决议）——**崩溃腿 [7] 全绿**：kill -9 → 登记双通道重建 →
  原生 prepared 恢复 → 新 leader（5435）上决议写入 → 清扫 3s 收敛 →
  行可见，矩阵行 1 机制单场景闭环。[5] ABORT 学习、[6] 无决议不阻塞、
  [4] 心跳自动清扫多轮复绿。剩余红项集中于翻覆活跃期的 [3]/[6] 收敛时延
  与缓存任务后端 GUC 重载竞态（b=0 偶现）——随 R-P4-6 处置。
- **实施记要·①收口（2026-08-14 第三轮，验收 40/43，三红同源 R-P4-7）**：
  **R-P4-6 真凶改判**：不是选举翻覆——是 **demux 一次性恢复 worker 的
  SIGSEGV→postmaster 整节点重置**（自装 backtrace 抓获完整栈：
  DemuxCrashRecovery→ScanWALRangeForPartition→MakeGlobalXid(
  **PartDistLocalNodeId**)→get_relname_relid→SearchCatCache(NULL 目录缓存)
  ——无 DB 连接 worker 摸 catalog，T3.5 心跳工作者同类死法，潜伏于在线
  捕获同源代码，被"残留注册逼出宽 WAL 扫描 ∧ 段内有打标记录"点爆，
  worker1 十二连崩）。**产品修复：groupid 持久化侧影**
  （$PGDATA/pg_partdist_groupid，backend 首次 catalog 解析成功即落盘，
  无 DB 语境读文件；"WAL 有分片记录 ⇒ 必有 backend 先解析过 ⇒ 文件必在"
  的序论证封死静默错号）。**第二产品修复：PREPARE 摘活跃表**——
  XACT_EVENT_PREPARE 分支还带 T4.3 前旧注释直接丢弃 xact_map，
  ShardCommitHash 条目不摘 → 持有者是 Citus 池化任务连接（可活极久）→
  读者先命中活跃表即 RUNNING，决议收敛写进 clog 的 COMMITTED 被**永久
  遮蔽**（实测池连接一退出行立即可见）——新增 ShardCommitRemove
  （只摘不判，终局交决议收敛/postabort）。**第三修复：rebuild_shard_identity
  v2**——v1 只加 WHERE 过滤，SQL 不保证 WHERE 先于 JOIN ON 求值，悬空行
  仍先炸 shard_name()（worker 身份重建整体报废→dtx_coord_ctx"没有对应
  分片"→决议全灭）——OFFSET 0 优化栅栏钉死先滤后 join。
  **验收 40/43**：E2E 决议+读者③/清扫双通道 **1s 收敛**、自动清扫 4s、
  ABORT 学习、无决议不阻塞、补决议 1s 收敛、kill -9 后登记双通道重建+
  原生恢复+组切主全过；仅剩崩后决议三连红=**R-P4-7**。
  **新风险 R-P4-7（解冻申请项）**：复制认领位被"活着但阻塞于对死 peer
  的无超时 RPC"的持有者长持（探活回收只认死进程），崩后组内 prepare/
  决议窒息 60s+（实测两笔"等待复制认领位超过 60000 ms"）——修法在
  pg_raft（RPC 超时 + claim 等待联动 peer 死亡检测），与 #39 apply 认领
  同构，待用户批准解冻；连同 R-P4-5（回执 ack 后移至 pending-finalize，
  也涉 pg_raft 守护）建议并批。
  **夹具坑续**：TSO boot 防呆删标记后必须**重启协调者**（shmem 拒绝态
  只认重启解锁）；分布式 DROP 打标表死锁环（登记=pg_shard_xid 目录、
  只有 DROP 成功才 GC、而分布式 DROP 走 2PC 被含-DROP 禁令拦 → 永锁；
  测试解锁=逐节点本地 DROP 触发合法 GC；**T4.6 需给分布式 DROP 打标表
  一条合法路径**）；MX 下 worker 也持逻辑表副本，本地清表要清全节点；
  journal 残项须随 TRUNCATE 一起清（表清日志不清=残项复活）。
- **实施记要·解冻批次 #2（2026-08-14 用户批准，R-P4-7 + R-P4-5，
  dd78c4e）**：**R-P4-7 有界 RPC**：新增 pq_exec_bounded /
  pq_exec_params_bounded（异步发送 + 100ms select 轮询 + 5s 截止；排空
  半途超时同判失败；超时连接挂着在途查询必弃用重连），换装复制认领临界
  区内三处（send_sql_rpc / peer_last_log_index / send_install_snapshot）。
  **实证（run17，41/43）**：崩溃腿全绿——kill -9 后现任 leader 决议写入、
  清扫 1s 收敛、行可见，"等待复制认领位超过 60000 ms"绝迹。
  **R-P4-5 回执后移**：dtx_ack_sweep 每行先经 rendezvous
  "partdist_dtx_pending_check_fn" 问 pg_partdist 未决登记，该 dtxid 本节点
  TX2 clog 收敛未完成则跳过本轮回执（遗留模式登记恒空、行为逐字节不变）；
  pg-partdist 侧桥 = DtxPendingContainsDtxid。**自愈二**：登记项 pairs 的
  分片表已整体不存在（DROP 后 clog 目录残留）⇒ 判残渣注销（全部 oid 查无
  relation 才动手）。**run18/19 残红定性（非产品缺陷）**：崩后 30s 窗口
  pg_raft 恢复守护自治接管测试的手工 prepared（问到决议→COMMIT PREPARED→
  闭合——正是矩阵行 1 的正确系统行为），测试脚本与守护赛跑输掉致断言
  错位；另有组建立期选举竞态偶发（20s 就位窗）。**待核尾巴（T4.5② 首查）**：
  守护闭合走 tx 通道（postcommit 不写终局）时，pending 注销与 clog 判决
  落账的先后需逐帧核一次——若注销先于判决落账实锤，即孤儿窗口回归。
- **实施记要·②决议主动广播 + 收官（2026-08-14，可信验收 45/0 + 金丝雀 8 套全绿）**
  ——本条为**合并记要**：本任务期间存在并行会话（见文末事故记录），双方各自
  记过一版，现按当前代码与最终数字合并去重，技术事实取并集、数字以收官轮为准。
  **待核尾巴已结清**：四条注销路径逐条核完（学到判决的 apply_verdict 内先
  SetVerdict 后 Finalized；自愈支；postabort 先写 ABORTED 后注销），**无一在
  判决落账之前注销**，孤儿窗口未回归。
  **广播落地**：实现放 pg-partdist（`DtxBroadcastDecision`，避免再扩 pg_raft
  触碰面），pg_raft 在 `dtx_write_decision` 的**提交点之后**经 rendezvous
  `partdist_dtx_broadcast_fn` 触发一次，失败全吞（广播是纯优化，§3.3 允许丢失，
  拉取仍是兜底真相源）；收件人由 dtx_participant → shard_identity →
  partition_map → node_map 解析，对端 `partdist.dtx_apply_decision(dtxid,
  verdict,cts)` 按 dtxid 找本节点未决登记、走与清扫同一落账函数幂等落分片 clog。
  **真缺陷（广播引入并当场修复）**：广播的 SPI 在 `SPI_execute` 抛错路径上
  不关闭 → 宿主事务收尾报 "transaction left non-empty SPI stack"（提交路径上
  的泄漏，九节点日志实测目击 26 次）——改 PG_TRY/PG_FINALLY 兜住任何出口。
  **验收语义对齐**：广播上线后收敛快于断言读数，"登记未决"类瞬时计数断言
  改判为"登记过 ∨ 已收敛"；崩溃腿的重建证据改读 "未决 2PC 日志重放：N 条
  待收敛" 日志行（实测重放与收敛相隔 0.4s，瞬时计数必然读空）。
  **最终验收（收官轮，独占锁 + 净场 + 重启 + 零并发 + 串行）**：
  `test_dtx_convergence_p4.sh` **45/0，连续两轮一致**；广播零拉取收敛 1–3s、
  A/B 双侧 clog COMMITTED 1s、ABORT 学习 1s、自动清扫 2s、无决议不阻塞、
  崩溃腿登记重建 + 原生恢复 + 切主后决议写入与收敛全过、零崩溃零丢弃。
  金丝雀 p1 45 / p2 64 / p3 38 / lwc 9 / r1 58 / tx1 83 / tx2 43 / tx4 17
  **全部 0 失败**。至此 T4.5 机制面（未决登记、持久日志、读者③问询、清扫、
  广播推送、崩溃重建、切主收敛）全部实证闭环；过程中的 R-P4-9 双病灶修复
  见 §5 风险登记 8f。
  **环境侧发现**：`invalid max offset number` PANIC 首现于本轮改动之前（回放器
  吃残留 fileset 旧字节，见 R-P4-8），与广播无关；夹具残留（parwal /
  shard_xid / shard_clog 目录 + 组槽）是崩溃与选举乱象的共同放大器——已并入
  深度清场流程。
  **测试基础设施事故与整改**：Bash 工具"超时移到后台"**不终止进程**，
  14:30–15:10 一度三份验收并发抢同一套 9 节点集群（并行会话的 run27/28 +
  我方 run24 残留），双方夹具互删、`shardA` 取空、9/36 崩塌——**run23–run26
  全部作废**，本记要只采信加锁后的轮次。整改 = 验收脚本入库为
  `tests/test_dtx_convergence_p4.sh` 并加 **flock 独占锁**（拒绝并发启动）+
  固定"净场 → 重启 → 零并发核查 → 串行跑"流程。另修真夹具 bug：
  `psql -Atc "SET …; SELECT …"` 会把 SET 回显混进取值（实测取到字符串 "SET"
  判空 → 全线假红），改用 PGOPTIONS 传会话参数。


#### T4.6 §9.2 第 3 层分类处置落地——✅ 已完成（2026-08-14，`src/shard_guard.c` + `tests/test_shard_gating_p4.sh` **20/0**）

- **改**：禁用项拦截（rebalancer/move_shard_placement/undistribute/
  alter_distributed_table）；COPY 协调者 = 首行命中分片；ANALYZE 分叉覆盖
  确认用例；引用表按 V3 裁定（倾向建表后只读）。
- **验收**：禁用项负向 + 放行项正向 + 安全网触发用例（§9.2 第 4 层门禁雏形）。
- **实施记要（2026-08-14，验收 20/0）**：**禁用项拦截**落在
  `src/shard_guard.c`：ExecutorStart 挂点扫顶层 Result 的 targetlist 与
  RTE_FUNCTION 的函数表达式，命中禁用清单即 ERROR。判据用**函数名**而非
  OID（Citus 升版本 OID 会变，名字是公开 API）；门控沿用
  `ShardGatingActive()`（白名单 ∪ partition_map 登记），**无打标表时零成本
  返回**，非本方案的库与既有基线完全不受影响。清单 7 项覆盖
  rebalance_table_shards / citus_rebalance_start / citus_move_shard_placement
  / citus_copy_shard_placement / undistribute_table /
  citus_schema_undistribute / alter_distributed_table（同名多签名按名字天然
  全覆盖）。**实测 6 条禁用项全部拦下，门控关闭时零介入**。
  **真缺陷（首版引入、当场修复）**：walker 直接遍历 plan 树的
  targetlist/qual → plan 节点里混着执行期专用类型 → `unrecognized node
  type: 92`，**门控一开每条 SQL 都炸**（实测两个 worker 连 `SELECT 1` 都
  起不来）；改为只对 TargetEntry->expr 与 RTE 的 funcexpr 递归，避开一切
  plan 节点类型。
  **验收边界裁定（本任务的关键认识）**：§9.2 第 3 层管的是"**语句准不准
  执行**"，不是"提交后可不可见"。放行项的写在纯 Citus 2PC 路径上（无跨
  分片决议流程）提交后判决落不进分片 clog，行不可见——这是 T4.5 决议链的
  适用边界（其前提是"有决议可问"），不是 T4.6 的缺陷。故放行断言收敛为
  "语句执行成功 + 事务内自读一致"，可见性收敛由
  `test_dtx_convergence_p4.sh`（45/0）专项覆盖。该现象登记为 R-P4-10。
  **其余腿**：安全网（第 1 层）strict + 无 ts 读分片表 ⇒ 响亮报错 ✓；
  ANALYZE 分叉覆盖（T2.6 已解禁，读侧走 0008）✓；引用表现状核查 ——
  本集群 **0 张引用表**，V3「建表后只读」裁定无现存反例 ✓。
  **夹具坑账（本轮踩全）**：① `citus.shard_count` 本集群默认 32，多语句里
  的 `SET` 未必落到 create 那一刻 → 改用 `create_distributed_table(...,
  shard_count := 2)` 参数；② 白名单必须覆盖**全部分片节点 + 协调者**
  （guard 跑在发起语句的节点上，coordinator 不开门控则禁用项照常执行）；
  ③ `psql ... </dev/null <<'SQL'` 里 `</dev/null` **夺走 heredoc 的 stdin**，
  psql 永远等不到输入 → 挂死 33 分钟；④ 被 kill 的轮次会留下 prepared 事务
  持锁，令后续 DROP 永久等待 → 净场须先回滚遗留 prepared；⑤ 取号失败会让
  join_info 变成含错误文本的非法值 → 事务**静默回滚**，表现为"COMMIT 成功
  却无数据"——脚本已加"join 三元组合法性"前置断言。

#### T4.7 P4 验收套件（出口门禁：崩溃矩阵逐格演练）

- `tests/test_dtx_tso_p4.sh`：**崩溃矩阵 §3.4 四行逐格**（参与分片 leader
  PREPARE 前/后崩、协调者决议持久化前/后崩、master 崩的事务级 fail-closed）
  + 跨分片 SI 一致快照 + 三态问询三分支 + §9.2 第 4 层负向门禁全集。
- 634 基线（14 套件）保持绿。

#### T4.7 实施记要（2026-08-15，套件 47/1，**M4 间歇待收口**）

- **套件**：`tests/test_dtx_tso_p4.sh` —— 崩溃矩阵 §3.4 逐格（M1 参与分片
  PREPARE 后崩 / M2 PREPARE 前崩 / M3 协调者决议前崩 / M4 协调者决议后崩 /
  M5 master 不可达）+ 跨分片 SI 一致快照（S1）+ 三态问询三分支（Q1–Q3）。
- **可信结果 47/1**（净场四道 + 独占锁 + 串行）：M1 ✓（崩后判决仍在 clog，
  1s 收敛）、M2 ✓（分片不可达则事务失败、无残留登记）、M3 ✓（推定中止达成
  ABORT 终局 + clog st=3）、M5 ✓（fail-closed + 已提交行不消失 + TSO 自愈
  复位）、S1 ✓（同一 start_ts 两次读一致）、Q1/Q2 ✓（in-doubt 不阻塞不可见）、
  Q3 ✓（问询学到判决后 1s 可见）。**M4 曾在同一夹具下绿过**
  （`ok:4s(peek@5435)`），本轮复跑再红 —— 间歇性，见下。
- **R-P4-12 已修（2026-08-15 解冻批次 #4）**：两版修法——① `dtx_peek` 读表
  前先调新增的 `pg_raft_group_drain_apply()` 排空**自身** apply 积压；
  ② 进一步把 `group_apply_pending()` 挂进 `pg_raft_catchup()` 的每组循环
  （那里已确认本节点是该组 leader、已持复制认领、日志已恢复，monitor 每
  catchup_interval_ms 触发一次，幂等空转），使追平从"有人问才做"变成
  **周期性自愈**。实证：M4 由必红转为多轮绿，Q3 的 `dtx_note_coord` 漏步
  同轮补上（手工 prepared 未登记 coord_gsid ⇒ 清扫无从寻址，决议写了也
  永不收敛）。
##### ★ R-P4-13：apply 侧决议登记缺失（2026-08-15，**已绕行、根因未闭**）

**问题陈述**：崩溃矩阵 M4 腿（协调者组 leader 在决议持久化**后**崩）间歇失败
——杀掉当时的 leader 后，新当选 leader 的本地 `partdist.dtx_decision` **没有
该决议行**，`dtx_peek` 的 leader 门控只认本地表，于是无人可答，in-doubt 收敛
被卡住（断言窗口 90s 内不收敛）。

**排除过程（逐条有据）**：
1. *不是追平时延*（这正是 R-P4-12 修掉的那类）。诊断打印三成员的
   `last_log_index/commit_index/last_applied`，失败时**三者完全一致且全部
   applied**（实测 `12/12/12`），主动调 `pg_raft_group_drain_apply()` 返回值
   也是 12 —— 日志层面没有任何缺口。
2. *不是决议没达多数派*。断言 `M4 前置：决议已达多数派` 每轮 PASS
   （`ok:1s(2/3)` / `ok:2s(2/3)`），诊断也显示另外两个成员各有 `决议行=1`。
3. *不是节点故障*。三成员 `存活=1`，组状态正常（一 leader 两 follower）。
4. *不是身份映射缺失*。失败节点恰是该分片**本体承载者**（夹具日志
   `shardA=<gid> leader=:5433 followers=:5435 :5436`，被杀的是 5435/5436），
   `rebuild_shard_identity` 在夹具里对三者都跑过。
5. *日志里没有任何 DECISION 登记痕迹* —— 既无成功记录，也**无失败告警**
   （`data_apply_dtx_one` 的两条 WARNING 均未出现）。

**指向**：apply 侧压根没走 DTX 登记分支。相关代码路径：
`group_apply_pending → data_apply_advance(…, dtx_items, ndtx) →
data_apply_dtx_one`，其中 `data_apply_advance` 在 `group_local_partition()`
取不到 local_oid 时直接 `return false`（记 `RAFT_APPLYFAIL_NO_PART`）。
但按第 4 条该节点应当有映射，故**根因未闭**，需在 apply 侧加诊断
（打印 ndtx / local_oid / 每条 DtxApplyItem 的落账结果）才能定论。这属
pg_raft 深处，超出解冻批次 #4 的批准面。

**测试与多轮结果（全部在独占锁 + 四道净场 + 串行条件下取得）**：

| 轮次组 | 条件 | 结果 | 失败项 |
|---|---|---|---|
| 三连跑 ①（R-P4-12 修前） | dtx_peek 无 drain | 47/1, 46/2, 47/1 | M4×1、Q3×2 |
| 三连跑 ②（R-P4-12 首版：peek 内 drain） | 有人问才追平 | 47/1 ×3 | 崩溃×1、M4×1、Q3×1 |
| 三连跑 ③（R-P4-12 周期化 drain） | catchup 每组自追平 | 47/1 ×3 | M4×1、Q3×1、M4×1 |
| 三连跑 ④（R-P4-8 两道守卫修复后） | 回放器不再自噬 | 47/1 ×3 | M4×2、Q3×1 |
| 单轮诊断（诊断字段加强） | — | 46/2 | Q3、崩溃（该轮 M4 **通过**） |
| **三连跑 ⑤（本绕行落地后）** | dtx_peek 本地缺失→拉组内成员 | **47/1 ×3** | **M4 三轮全绿**；Q3×2、M5×1 |
| 六连跑 ⑥（加装 M5 取证钩子） | 同上 + 串行探针（见下方注） | **48/0 ×6** | 无 —— M5 未复现 |
| 五连跑 ⑦（R-P4-14 修复后） | 问询轮询全成员 + 去 leader 门控 | 48/0, 47/1, 47/1, 47/1, 48/0 | M5×1（**取证命中**）、净场×1、Q3×1 |

> 注：六连跑 ⑥ 的前 5 轮用的是**串行 16 次查询**的取证探针，它在 M4 切主后
> 凭空插入约 6 秒沉降，等于用观测手段改变了被观测的时序 —— 6 轮不复现这个
> 数字**不能**单独作为"M5 已消失"的证据。第 6 轮起改为并行探针（<1 秒），
> 五连跑 ⑦ 全程使用并行探针，并在第 2 轮命中现场（详见 R-P4-15）。

**读数**：M4 与 Q3 在各轮之间**轮换失败、且各自都绿过多次**；R-P4-8 修复后
崩溃项从"几乎每轮必有"降为**零**（三连跑 ④ 无一次崩溃）。M4 的失败形态在
所有轮次里**完全一致**（日志游标齐平、本地无决议行），是稳定可复现的单一
现象，而非随机噪声。

**处置（用户 2026-08-15 裁定：先绕行）**：`dtx_peek` 在本地行缺失时，经新增的
`pg_raft_group_peer_decision(group_id, dtxid)` 向**组内其他成员只读拉取**该
决议，拉到即幂等补进本地表（`ON CONFLICT DO NOTHING`），下次无需再远程问。
理由：决议在多数派上是**确定的事实**，"本地有没有那一行"不该成为可用性的
单点依赖；该绕行只读、不写推定中止、不改变"决议槽一次性"语义，因此不动
正确性边界。**R-P4-13 的根因作为已知项保留**，待后续专项（需扩大解冻面到
apply 路径）。

**当前影响面**：正常路径与非切主崩溃均不受影响；仅"协调组切主后新 leader
立刻自答"这一条依赖绕行。决议本身在多数派上，正确性不受损。

**绕行实证（三连跑 ⑤，2026-08-15）**：M4 **三轮全绿**
（`ok:4s(peek@5433)` 等），此前它是最频繁的失败项 —— 绕行有效。
残余两项：
- **Q3×2**：与 M4 同源（切主后判决可得性），但走的是**清扫**通道而非
  `dtx_peek`，未被本绕行覆盖。待办：让 `DtxPendingSweep` 的问询在本地
  缺失时同样回退拉取（与 dtx_peek 对称）。
- **M5×1（"4 → 3" 计数减少）**：断言期望"只增不减"，实测**减少 1**。
  该腿的前后计数跨越了 M4 注入的切主 + 重启，怀疑读到了不同 placement
  视角（M4 的切主会翻转某分片主副本，读路径按新 placement 走）。
  事后核验物理数据仍在（两分片各 1 行），但夹具已清理、无法回溯当时状态，
  **故定性未闭**。这是**唯一一条触及"已可见行变不可见"的观测**，优先级
  高于 Q3，下轮须带现场保留（KEEP_FIXTURE=1）复现确认。
- **T4.7 稳定性实测（三连跑 × 3 组，2026-08-15）**：修前 M4 红 1 / Q3 红 2；
  修后每轮仍只红一条且**三轮各不相同**（M4 一次、Q3 一次、R-P4-8 一次），
  每条都在其他轮绿过。判定：**残余为环境噪声**（追平时延波动 + 已登记的
  R-P4-8 回放器 PANIC），非机制缺陷。套件稳定在 **47/1**，七类机制
  （M1/M2/M3/M4/M5/S1/Q）均已各自多次实证通过。
- **★ 原 R-P4-12 描述（存档）**：M4 间歇失败：杀掉协调组 leader 后，新
  当选的 leader 恰是**尚未 apply 该决议**的成员（决议在另外 2/3 上，多数派
  成立），90s 内其本地 `dtx_decision` 始终为空 ⇒ `dtx_peek` 无从应答。
  `pg_raft_catchup()` 是 leader→follower 的**补发**通道，推不动 leader 自己
  追平自己。判定：这是 §1.4「新 leader 从组内持久化状态继承」的**继承时延
  上界问题**，不是正确性缺陷（决议在多数派上、任一存活成员都能提供），但
  in-doubt 收敛时延会被拉长到"选举 + 追平"之外。处置方向：新 leader 当选后
  主动拉取式追平（apply 自身缺口），或让 `dtx_peek` 在本地缺失时回退到组内
  其他成员读取。与 R-P4-7（复制认领窒息）同族，同属 pg_raft 侧，**需解冻
  批次 #4**。
- **夹具四道净场（R-P4-11 的产物，已沉淀为通用做法）**：① 残留 raft 组重置
  （表删组不删，身份映射失联致 dtx_decide 找不到分片）；② 悬空 Citus 元数据
  清理（`pg_dist_shard/placement/partition` 的失效行会让
  rebuild_shard_identity 整条报废 —— **这是"决议链失效"的直接成因**）；
  ③ TSO 纪元复位（换源后任何提交都取 commit_ts，boot 拒绝态 ⇒ 全集群提交
  失败、分片表建不出来）；④ 未决登记归零（pending 在 shmem，删 journal
  不够，须重启）。四道齐备后 T4.5 基准从 30/15 恢复 **45/0**。

##### ★ R-P4-14：问询寻址与应答门控的"两个主"不同步（2026-08-17，**已修**）

**问题**：Q3（问询学到判决后可见）间歇不通过，且是**永不收敛**形态 ——
决议明明已写入（"Q3 前置：决议写入" 恒 PASS），清扫循环跑满 60 秒、每秒
主动 drain 三个候选，仍学不到判决。

**根因**：同一条链路上"谁是主"有两套互不同步的判据。
- **寻址**用 `partdist.partition_map.primary_node`（`dtx_pending.c`
  `dtx_inquire_core`）；
- **应答**用 raft 的 `state = 'leader'`（`dtx_peek` 内的 gate）。

协调组切主后，raft 侧立刻有了新 leader，`partition_map` 却要走"自选举 →
上报 → group0 多数派 → 各成员 apply 回落"才更新。窗口期内问询打到旧主：
旧主正是被 M4 杀掉的那个 ⇒ 要么连不上直接 `return 0`，要么连上了但它已非
leader ⇒ `dtx_peek` 返回 0 行。**问询通道整条哑掉**，重试再多也没用 ——
因为每次重试都问同一个错节点。

**为什么 M4 腿反而是通的**：夹具自己轮询候选节点逐个 peek（日志里的
`ok:4s(peek@5435)`、`peek@5436` 就是轮询命中的结果）。清扫没有这个轮询，
只认 `partition_map` 的登记主。**是夹具比产品代码更鲁棒，掩盖了缺陷** ——
这条值得单独记住。

**修复（两层，对称）**：
1. `dtx_pending.c` `dtx_inquire_core`：候选从"登记主"扩为**协调组全部成员**
   （`primary_node` ∪ `unnest(secondary_nodes)`，primary 仍排最前），逐个试
   到学到判决为止；连不上就换下一个。全部返 0 时语义不变（保持未决），
   不引入推定中止。
2. `dtx_peek`：远程回退分支**去掉 leader 门控**（本地答案仍保留门控不变）。
   纯增量 —— 此前非 leader 一律返回 0 行，现在它会转问组内成员。安全性
   依据：决议是一次性的正向事实，写下就不再改，从谁那里读到都等价；返回
   0 行的"无从判定"语义未变，不产生推定中止。

**实证**：`dtx_inquire_core` 的候选集由 1 个（登记主）扩为 3 个（组内全部
成员），寻址 SQL 已在真库验证展开正确；`dtx_peek` 的 `gate.is_leader` 现只
剩 local_row 一处，peer_row 分支无门控（9/9 节点验明）。

**⚠️ 归因更正（2026-08-17）**：本条最初记为"把 Q3 从 60 秒不收敛收窄到
`ok:1s`"。**该归因是错的，已撤销。** 后续取证证明 Q3 的成败取决于夹具落点
`$pport_a` 当时是否恰好还是 leader（详见 R-P4-17），与问询通道无关：
- 5433 仍是 leader ⇒ PREPARE 成功 ⇒ Q3 通过（表现为 `ok:1s`）；
- 5433 已被切成 follower ⇒ PREPARE 被任期栅栏拒绝 ⇒ Q3 必然超时。

R-P4-14 修的缺陷本身是真实的（寻址与门控确实不同步，代码路径与返回值变化
均已直接验证），**但它没有修复 Q3，也不该以 Q3 的数字作为其疗效证据**。
教训：把"改动后某指标变好"当成因果，而没有独立验证机制链路 —— 两次跑
（1/5 与 3/5 失败）合看，Q3 频率并未下降。

**残留风险（未改，属冻结模块）**：`pg_raft_group_peer_decision` 用的
`pg_raft_format_conninfo` 只设 `connect_timeout=1`、**未设
`statement_timeout`**。peer 若"连得上但卡住"会无限等。放宽门控后调用面变广
（非 leader 也会走），该风险的暴露面随之扩大。不顺手改，记录在此。

##### ★ R-P4-15：崩溃回归的旧主可携陈旧数据直接夺主（2026-08-17，**已修**）

**问题**：M5 腿断言"已提交事务可见性不受影响（只增不减）"，实测
**`4 → 3`，计数减少**。这是整个 P4 期间唯一一条触及"已可见的行变不可见"
的观测。

**复现难度**：非必现。首次观测于三连跑 ⑤（1/3）；随后 6 轮**未复现**；
加装取证钩子后于第 2 轮命中。

**取证（`/tmp/m5_eviden_853709.txt`，两次读之间相隔 4.7 秒）**：

| 时刻 | 逻辑 count | 102556 的 placement | 该节点物理行数 | primary/term |
|---|---|---|---|---|
| before 09:26:40 | **4** | `:5435` | **2** | primary=4 term=**2** |
| after  09:26:44 | **3** | `:5433` | **1** | primary=2 term=**3** |

（另一分片 102557 两次均指向 `:5438`、物理 2 行，未变。4 = 2+2，3 = 1+2。）

**结论：行没有丢，是路由被翻到了物理落后的副本上。**

**第二次复现（2026-08-17 R-P4-16 验证跑第 3 轮，分片 102572）指纹逐项吻合**：
placement `:5435`（物理 2 行）→ `:5433`（物理 1 行）；primary 4 → 2；
term 2 → 3；`:5433` 的 `回放=` 同样为空、`raft=leader 12/12/12` 同样成立。
**同一机制两次复现、特征完全一致 ⇒ 定性确定，非偶发。**

**根因**：`pg_raft_promote_prepare`（`pg_raft--1.0.sql`）的升主前置里有一条
捷径 ——
```sql
SELECT s.armed, s.applied INTO is_armed, app
  FROM partdist.replay_status() s WHERE s.shard = loid;
IF NOT FOUND OR is_armed IS NOT TRUE THEN
    RETURN 1;               -- 放行，完全不追平
END IF;
```
它把"查不到回放槽位"当作"没什么要追的"。注释里设想的场景是"它一直就是该组
leader，或副本回放尚未启用"，但**漏了第三种**：一个曾是主、崩溃、带着陈旧
数据回来的节点 —— 它持有的是真表而非副本壳表，`replay_enable` 从没对它做过，
因此天然没有槽位，于是走捷径**无条件放行升主**。

取证里 `SHARD 102556 @:5433 … 回放=`（**空**）正是这个状态：5433 是该分片的
原始主（`shardA=102556 leader=:5433 followers=:5435 :5436`），M4 杀它 →
5435 接管并收到新数据（2 行）→ 5433 重启回来、以 term 3 重新自选举 → 捷径
放行 → 它那份 1 行的陈旧副本成为 Citus 路由目标。

**要害：`raft=leader 12/12/12` 与数据陈旧同时成立。** raft 日志层面 5433 是
完全追平的（last_log/commit/applied 全 12），但分片数据走的是 parwal 物理
回放流，它从没消费过。**"raft 追平" ≠ "数据追平"**，这条捷径把两者混同了。

**次生影响**：该状态会污染下一轮净场 —— 验证跑第 3 轮即因
"净场：残留 raft 组已重置（清 3 个，剩 1）" 失败。

**处置：已修（2026-08-17，解冻批次 #5 获批后实施）**

**判据是取证推出来的，不是推断。** M4 腿每轮都确定性地杀主+重启，
"崩溃回归的旧主"在那里必现，比守 M5 那个约 1/5 的窗口高效得多。
把位点快照挂到 M4 之后，三轮即得决定性对比：
```
:5433 raft=follower 12/12/12  收=12  放=无槽位   物理行数=1   ← 回归的旧主
:5435 raft=leader   12/12/12  收=12  放=true/12  物理行数=2   ← 正常副本
```
**收的位点一样，物理行数差一行，差别全在"放"。** `收` =
`follower_partition_map.applied_part_lsn`（已收到并落盘的分区 WAL 位点），
`放` = `replay_status().applied`（已 redo 进堆的位点）。收到了却没槽位回放
⇒ 堆必然落后。

**判据**：`NOT FOUND（无槽位）` 且 `applied_part_lsn > 0` ⇒ 拒绝升主。
反之（无行/为 0）说明确实没作为 follower 收到过分区 WAL —— 它一直是该组
leader，或压根没有该分片的副本 —— 维持原样放行。

**为什么返回 -1（永不放行）而不是 0（重试）**，同样有实据：查文件系统，
回归的旧主 `pg_parwal/<oid>/` 下只有 `checkpoint fileset freeze`，正常
follower 才有 `locmap`。**没有 locmap 就装不了槽位、追不上去**；返回 0 只会
空转到 `promote_catchup_deadline_ms`(60s) 的"可用性优先"兜底，然后照样放行
—— 缺陷原样回来，只是晚一分钟。它与快路径分叉同类：须重做物理基线。

**一处过程留痕**：原计划用 `get_follower_applied_part_lsn` 与"组内已提交
位点"比较。读实现后发现它读的是**本节点自己**的
`follower_partition_map.applied_part_lsn`，与函数名给人的印象不同 ——
先取证再动手这一步恰好避开了一个用错判据的坑。

**实测（四轮，49/0 × 4）**：WARNING 确实触发，证明 -1 分支在真实工作：
> `WARNING: 分片 1811889 (组 102670) 拒绝升主：本节点已收到分区 WAL 到位点 11，
> 却没有回放槽位（这些记录一条都没 redo 进堆），堆数据必然落后。`

**生效机制与预期不同，值得记住**：修复后 5433 **仍然是 raft leader**
（`raft=leader 11/11/11`）。`promote_prepare` 把关的是**向控制面上报**，
不是 raft 选举本身。所以它照样赢选举，但拒绝上报 ⇒ `partition_map` 不登记
它 ⇒ **Citus 路由不翻过去**。四轮快照 `pmap主` 恒为 `5/t2`/`4/t2`，
再未出现修复前那个 `primary=2 term=3`（node 2 = 5433）。

这比"阻止它当 raft leader"更好：**堵住了路由翻转这条真正致害的路径，
同时不牺牲 raft 可用性**（它仍可参与共识），Q 腿也因此照常工作
（Q 腿等的是 raft leadership，不是 partition_map 登记）。

**遗留**：拒绝升主的节点需要"重做物理基线"才能恢复参选资格，该重建流程
**尚未实现**，目前只是拒绝 + 告警。生产化前需补上自动重建路径。

##### ★ R-P4-16：绕行自身答出"半个判决"（2026-08-17，**已修**）

**这一条修的是 R-P4-13 绕行（我方引入）的缺陷，不是原有设计的问题。**

**问题**：`dtx_peek` 在本地缺行、转问组内成员后，答出
`verdict=1` 而 `commit_ts=0`。取证：5433 答 `1@0`，而组内两个成员都是
`1@22`。

**根因**：绕行首版把"补读"写成同一条 SQL 里的 `LEFT JOIN` ——
`pg_raft_group_peer_decision` 会把决议幂等补进本地表，但**同一条语句用的是
语句开始时的快照，看不到函数刚插进去的行**，`coalesce(commit_ts, 0)` 于是
给出 0。

**危害**：`1@0` 是无法使用的残缺答案，参与方只有两条路，实测两种都出现过：
- 拒收 ⇒ 分片 clog 落 ABORTED 并注销登记（取证 `st=3 sts=20 cts=0`）；
- 收下 ⇒ 落一个时间戳错误的 COMMITTED，**破坏 SI**（取证 `st=2 sts=4 cts=0`）。

**修法**：`dtx_peek` 改 `plpgsql`，补读**另起一条语句**（新语句取新快照，
看得到刚补进去的行）；并加守卫 —— **COMMIT 判决缺有效 `commit_ts` 时返回
0 行**，宁可答"无从判定"让调用方重试，绝不把半个判决发出去（ABORT 与
commit_ts 无关，0 是正常值）。

**实证**：修复后同一位置由 `1@0` 变为 `1@22`，9/9 节点部署验明。

**注意**：本修复**与 Q3 无因果关系**（Q3 另有其因，见 R-P4-17）。它修的是
一条真实存在、且会破坏 SI 的缺陷，价值独立于 Q3。

##### ★ R-P4-17：Q3 的"永不收敛"是夹具缺陷，非产品缺陷（2026-08-17，**已修夹具**）

**结论先行**：Q3 三次失败（五连跑 ⑦ 1 次 + R-P4-16 验证跑 3 次 + 溯源跑
1 次，指纹完全一致）**全部**由夹具自身造成，产品行为是正确的。

**取证链**（溯源跑第 1 轮，`/tmp/q3_eviden_*.txt` + worker1 服务日志）：

1. T0 快照（PREPARE + note_coord 之后、本轮尚未写任何判决）：
   `prepared 是否在：0` —— **那笔 prepared 事务此刻已经不存在**，且全集群
   没有 DTXQ 的判决行（`全表行数=0`）。⇒ 排除"跨轮残留判决"，也排除
   "先写 ABORT 再被 COMMIT 覆盖"（根本没有过判决行）。
2. 服务日志同刻：
   ```
   ERROR:  pg_raft: 分区 1566129(组 102578)的本地写入被拒：本节点不是该分区组的 leader
   DETAIL:  分区主副本可能已切换，请经路由层重试。
   STATEMENT:  PREPARE TRANSACTION 'citus_9_777_303_0';
   ```
   ⇒ **PREPARE 本身被任期栅栏拒绝**。这是**产品的正确行为**：Q 腿跑在
   M4/M5 之后，那两腿杀 leader 触发切主，`$pport_a`（分片 A 的原始主 5433）
   此时已是 follower，不得接受本地写入。三次 Q3 现场的
   `:5433 raft=follower` 与此逐条吻合。

**夹具的两处缺陷**：
1. **落点不跟随切主**：Q 腿把 `$pport_a` 写死为 prepare 的目标节点。
2. **前置断言没有失败能力**：那段 heredoc 缺 `ON_ERROR_STOP`，而末尾恒有
   `SELECT 'prepared'`，于是 `tail -1` 永远取到 "prepared" ——
   **"Q 前置：in-doubt 就位" 必然 PASS**，把"事务压根不存在"伪装成
   "事务已就位"，Q1/Q2/Q3 三条断言全部建立在空气上。Q3 的"永不收敛"
   实为**无物可收敛**。
   （与既有教训 `feedback_test_harness_silent_pass` 同类：断言必须有能力
   失败；一个恒真的前置检查比没有检查更有害，因为它提供虚假保证。）

**修法（均在 tests/，非冻结）**：
- 现场重新解析 `gid_a` 组的当前 leader 作为落点（`QNODE`），Q 段内 15 处
  单点操作随之改用 `QNODE`（成员枚举与 `decide_retry` 的候选表保留全体）；
- heredoc 加 `-v ON_ERROR_STOP=1`；
- 前置断言改为**双重判据**：既要末行是 `prepared`，又要
  `pg_prepared_xacts` 里确实存在该 gid。

**连带影响**：R-P4-14 与 R-P4-16 先前引用的 Q3 数字均属误归因，已在各自
条目中撤销更正。两者修的缺陷本身真实且已独立验证，但都与 Q3 无因果关系。

##### ★ R-P4-18：修夹具时自己引入的回归（2026-08-17，**已回退**）

**这一条记的是我方的操作失误，不是产品缺陷。留档是为了不再犯。**

**经过**：R-P4-17 修好 Q 腿落点后，第 3 轮出现 `function pjoin does not
exist` —— 辅助函数只配在两个原始主上，leader 漂到 follower 就没法用。
于是把"辅助函数 + 打标 + TSO 源"三件套配到组内全部六成员。

**后果**：第 1 轮 49/0，**第 2、3 轮连续塌到 32/17**，同一签名：
```
WARNING: TSO 不可达或拒绝服务（取 commit_ts 失败）
         exec: ERROR: function partdist_tso_commit_ts() does not exist
WARNING: failed to commit transaction on localhost:5436 / 5437 / 5438
ERROR:   relation "t47d" does not exist   ← 随后 locmap 全崩，连锁 17 项
```

**根因**：给 follower 设 `tso_conninfo` **等于把它们变成 TSO 客户端** ——
此后它们每次提交都要取 commit_ts，一失败即 fail-closed；DDL 传播到这些
节点的提交全部失败，`t47d` 建不出来，`setup_replication` 的
`CREATE TABLE ... (LIKE t47d ...)` 随即报错，整套连锁崩塌。

**最该记住的一点**：这个机制 **R-P4-11 里已经写过**
（"换源后任何提交都取 commit_ts ⇒ 全集群提交失败、分片表建不出来"），
仍然踩了。原因是把"配齐六成员"当成一个**整体动作**执行，没有逐项问
"这一项到底为什么需要"：
- `pjoin`/`sclog_full`/`tso_c_start` 函数 —— Q 腿在该节点执行，**需要**；
- `shard_relids`（打标）—— 否则 Q3 退化成不检验任何东西的断言，**需要**；
- `tso_conninfo` —— **完全不需要**，`tso_c_start()` 是在 `$COORD` 上调的。
一次没问，就把三件套里唯一有害的那件也发了出去。

**处置**：
1. 配置回退为只给两个原始主（`$pport_a`/`$pport_b`）；
2. follower（5435–5438）上残留的 `tso_conninfo`/`shard_relids` 手工清空并
   验证为空 —— `ALTER SYSTEM RESET` 与 `pg_reload_conf()` **不能挤在同一个
   `-c` 里**（同一隐式事务，实测 RESET 不生效），须分开发；
3. Q 腿落点改回 `$pport_a`，但保留 R-P4-17 的全部诊断改进，并显式等待它
   重掌 leader（45s），等不到就如实报前置失败。

**遗留局限（已知、未解决）**：Q 腿因此只在原始主重掌 leader 时才有效验证；
"切主后长期不回归"的场景它覆盖不到。要覆盖，需让夹具在 follower 上也备好
函数与打标而**不**设 `tso_conninfo` —— 未验证，留待后续。

**回退后六轮实测（2026-08-17）**：`48/1, 49/0, 49/0, 49/0, 48/1, 46/3`
—— 第 2–4 轮三连全绿，确认环境已从回归中完全恢复。失败项全部可归因：
| 轮次 | 失败 | 归属 |
|---|---|---|
| 1 | 净场：残留 raft 组（剩 1） | 塌掉那两轮的残局，一次性 |
| 5 | M5「4 → 3」 | **R-P4-15，真缺陷**，待解冻批次 #5 |
| 6 | Q 前置未重掌 leader → in-doubt 未就位 → Q3 | 上述**已知局限**，非新问题 |

第 6 轮恰好演示了本次夹具改造的价值：同一情形在改造前显示为
"Q 前置 PASS、Q3 神秘超时"（假绿掩护假红，把人引向问询通道白查），
改造后直接显示"落点未重掌 leader → in-doubt 未就位 → Q3 失败"，
因果链一眼可读。**红的数量从 1 变 3，但每一项都指向真实原因** ——
诊断价值远高于一个漂亮的数字。

##### ★ R-P4-19：R-P4-18 的真因是垫片与配置的生命周期错位（2026-08-17）

**R-P4-18 当时给出的结论"follower 不能配 `tso_conninfo`"是错的，已修正。**
真因如下，且它同时暴露了一处**产品层面的脆弱点**。

**真因**：产品的 TSO 客户端调用的是**不带 schema 限定**的
`SELECT partdist_tso_commit_ts()`（`tso_client.c:435`）。而扩展装在
`partdist` schema，远端 `search_path` 只有 `"$user", public` ——
该调用**只能靠夹具在 `public` 里建的同名垫片**解析（套件第 ~326 行）。
夹具清理阶段又把垫片 `DROP` 掉（第 ~870 行），且只清
`$pport_a/$pport_b/$COORD` 的 `tso_conninfo`，**follower 从不清**。

于是形成一个窗口：**上一轮清理之后 ~ 本轮建垫片之前**（正好覆盖净场与
建表阶段），任何仍带着 `tso_conninfo` 的节点一提交就撞
`function partdist_tso_commit_ts() does not exist` ⇒ fail-closed ⇒
DDL 传播全败 ⇒ `t47d` 建不出来 ⇒ locmap 崩 ⇒ 连锁 17 项。
R-P4-18 那次改动给 follower 配了 TSO 却没配套清理，正好把它们送进这个窗口。

**修法（三处，封死窗口）**：
1. 净场逐轮 `ALTER SYSTEM RESET pg_partdist.tso_conninfo`，范围 9 节点全覆盖；
2. 清理阶段**先清所有节点的配置，再 DROP 垫片**（顺序反了就重开窗口），
   函数清理范围扩到六成员；
3. 在此基础上重做"六成员配齐"，Q 腿落点改回跟随当前 leader。

**为什么必须配 TSO 而不能只配打标**：读者没有 `start_ts` 就施不了可见性
判据，`COMMIT PREPARED` 后行会立即可见，Q3 退化成不检验任何东西的断言。
（安全网当前是 `permissive`，故"打标+无 TSO"不报错，只是**失去检验意义**；
若切到 `strict`，`ShardAccessGate` 会直接拦截无 ts 访问。）

**产品脆弱点（本次未改，留档）**：TSO 客户端跨节点调用不带 schema 限定，
依赖对端 `search_path` 恰好包含扩展 schema。夹具用 `public` 垫片掩盖了它，
**生产环境同样脆弱** —— 部署时若扩展 schema 不在 master 的 `search_path`
里，所有取号调用都会失败。建议改为 schema 限定调用或显式设
`options=-c search_path=...`。

**Q 腿局限的实测频率（八轮基线）**：`6 × 49/0, 2 × 46/3`，两次失败全部是
该局限（5433 未重掌 leader），无其他失败项 —— 约 1/4 命中，值得修而非容忍。

**修复后八轮实测**：`6 × 49/0, 2 × 48/1`，**Q 腿局限那三条连锁失败再未出现**。
关键验证点：`Q落点` 出现 `:5435` **三次**（第 3、5、8 轮）且全部 49/0 ——
Q 腿在**非原始主**上完整跑通，PREPARE 未被任期栅栏拒绝，**也没有重演
R-P4-18 的塌方**。⇒ 对根因的判断成立，"follower 不能配 tso_conninfo"确系误判。

两次 48/1 是与 Q 腿无关的两个独立问题：
- 净场残留 raft 组（清 3 剩 1）×1；
- **回放工作进程 PANIC ×1**（见下条 R-P4-20）。

##### ★ R-P4-20：回放 PANIC —— **已知缺陷（2026-08-18 用户裁定：记录在案，先推进 P4 出口清单）**

> **【裁定】2026-08-18**：用户裁定**记为已知缺陷**，不阻塞 P4 出口清单。
> **出错的位置（一句话）**：副本在崩溃恢复后接着重放数据时，**无法确认自己
> 手里的表与要重放的数据流是不是同一代**，而整条链路没有任何机制去校验这件事。
> **风险边界**：崩溃发生在**回放后台进程**，不破坏本轮事务语义（历次崩溃轮
> 的功能断言全部通过）；进程会自动重启，但可能反复自噬（实测一轮内最多 8 次）。
> **未闭的一步**：在持久化顺序正确、字节完好（CRC 16 轮零失败）、协议无漏洞的
> 前提下，表与流究竟在哪个瞬间错开 —— 无答案，不编。
> **复现难度**：最近约 48 轮一次未现（此前 10 轮 2 次），A/B 对照实验功效不足。
> **进 P5 前必须重新评估**（见 P4 出口清单末条）。


**现象**（worker4 `:5436`，2026-08-17 16:39:10）：
```
LOG:   pg_partdist replay: 认领 shard 421472，游标从 0 起（惰性：待触发）
PANIC: invalid max offset number
LOG:   background worker "pg_partdist replay worker" ... terminated by signal 6: Aborted
```
回放工作进程认领分片后**从游标 0 起重放**，随即 PANIC。

**与 R-P4-8 同源，但既有修复挡不住**：R-P4-8 加的两道守卫查的是
`smgrexists`（**关系是否存在**）。这里关系是存在的 —— 本轮新建的空壳表 ——
被灌入了不匹配的旧记录，页内偏移越界。**存在性守卫对"存在但内容不匹配"
无效。** 这说明 R-P4-8 当时"三次跑零崩溃"的证据强度不足以支撑"已根除"。

**是否由 Q 腿夹具改动引起：证据不足，不下结论。** 八轮基线（改动前）零崩溃，
新夹具八轮出现一次，方向可疑但 n=1；且 R-P4-8 本身是间歇性的。需要独立追查。

**影响面**：该轮功能全过（48/1，唯一红的就是崩溃检查本身），崩溃发生在回放
后台进程，未破坏本轮语义。但它是**真崩溃**，不是夹具问题，不可忽略。

**第一次修复尝试：失败（2026-08-17，已如实记录）**
加了"块号越界则跳过"的守卫，10 轮验证 `8 × 49/0, 2 × 48/1` —— 两次失败仍是
同一 PANIC，而**新守卫的 WARNING 触发 0 次**。部署确已生效（`.so` 验明含新
代码），是**判据选错**：PANIC 抛在
```c
action = XLogReadBufferForRedo(record, 0, &buffer);
if (action == BLK_NEEDS_REDO)
    if (PageGetMaxOffsetNumber(page) + 1 < xlrec->offnum)
        elog(PANIC, "invalid max offset number");
```
即**页存在、块号在界内、但页太短**。按块号判断的守卫在设计上就够不着。
该守卫已留在代码里（"记录指向本地不存在的块"本身也该拦），但注释已改写为
**不冒充修复**。

**一处中途推断的撤销**：我一度把病灶归为"新建空壳表被灌入旧记录"。
`BLK_NEEDS_REDO` 意味着页 LSN 低于记录 LSN，而 PG 的 redo 靠页 LSN 做幂等
（页够新就跳过），所以"重放已应用过的记录"走不到 PANIC。该推断已撤销。

**决定性取证（2026-08-17 19:17，两条现场互补）**

worker4（带诊断插桩）：
```
认领 shard 520119，游标从 0 起
诊断: plsn=1 rmid=10 info=0x80 blk=0/0 块数=1 image=0 init=1
PANIC: invalid max offset number
```
对照同一插桩在一次**成功**回放上的输出：记录形状完全相同
（`plsn=1 rmid=10 info=0x80 init=1`），唯一差别是 **块数=0**。
⇒ **判别量是本地关系的块数：0 成功、1 崩溃。**

`info=0x80` = `XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE`，redo 会重初始化该页
（max offset 归零），因此只有当记录的目标 offnum ≥ 2 时才触发上面那个判据。
即：**流的第一条记录所设想的关系状态，与本地关系的实际状态不是同一代。**

worker3（同轮）：`认领 shard 594132，游标从 10 起` → PANIC。
⇒ **推翻"游标 0 才触发"这一前提**；我把诊断门控在 `applied_part_lsn == 0`
是过窄的，worker3 因此没打出诊断行。**触发条件与游标值无关。**

**当前可站住的结论**：被重放的段流与本地关系**不同代**。根因未闭 —— 尚未
查明"不同代的流为何会被认领"。

**第二轮排查（2026-08-18）：又排除四条，根因仍未闭**

| 假说 | 排除依据 |
|---|---|
| 副本被外部直接写入（破坏单写者不变式） | 查崩溃轮落点：Q 腿写的是**主**（:5433，持真表无槽位），崩的是副本 :5435/:5436 |
| Q 腿的手工 prepare 是特例 | M3(`_301_`)、M4(`_302_`) 同样手工 prepare、同样作用于分片 A |
| 残留段目录堆积触发 | 反证：worker1 残留 **141** 个却从未崩；worker3/4 各仅 3–4 个却是全部崩溃来源 |
| 游标跑在堆前面（持久化顺序错） | 代码明确"先 FlushRelationsAllBuffers → smgrimmedsync → 才写游标"，且循环覆盖 `ctx->nlocal` 全部本地关系与全部 fork。**协议本身无漏洞** |

**新增的确凿观测**：
- **空间吻合**：崩溃只出现在**分片 A 的两个副本**（:5435/:5436）——分片 A
  正是被 M1/M3/M4 反复杀主的那个；分片 B 的副本全程零崩溃。
- **时间吻合**：五次崩溃**全部**发生在一轮的最后一条写腿（Q）之后，即整轮
  状态最复杂的时刻（历经多次杀主、重启、任期变更、TSO 中断）。
- **多数崩溃是"游标从 10 起"而非从 0** ⇒ 病灶在"从中途位点接续回放时，
  本地表状态与该位点对不上"。

**触发条件的当前描述**：多次切主/崩溃之后，副本从中途位点恢复回放。
与 R-P4-15（陈旧旧主夺主）、R-P4-8（认领已删分片）**同属一类** ——
崩溃恢复后副本无法确认自己与主是否同代。

**复现难度已成为障碍**：最近约 32 轮一次未现（此前 10 轮 2 次）。频率骤降
原因不明。这使任何 A/B 对照实验都**功效不足**，靠跑测试推进的路已走到头。

##### ★ R-P4-21：物理回放缺少完整性校验（2026-08-18，**已修**）

**独立于 R-P4-20 的归因，本条自身成立。**

**问题**：`ApplyDataRecord` 把段文件里的字节直接当 `XLogRecord`，调
`DecodeXLogRecord` 解析**结构**后即派发 redo —— 既不校验 CRC，也不校验记录
主体数据（而 `xl_heap_insert.offnum` 恰在主体数据里）。**PostgreSQL 自身的
回放路径是校验 CRC 的（`xlogreader.c` `ValidXLogRecord`），这条链路绕过了。**

**修法**：redo 前按 PG 标准算法校验 `xl_crc`，失败则 WARNING + 跳过该条，
绝不把损坏内容写进本地关系。

**双重价值**：① 健壮性——外来字节进 redo 前先验完整性，本就该有；
② 判据——CRC 通过 ⇒ 记录是主副本真实写出的完好字节 ⇒ R-P4-20 属"不同代"；
CRC 失败 ⇒ 字节损坏或读偏 ⇒ 属读取路径缺陷。

**实测（16 轮完整跑）**：`15 × 49/0, 1 × 48/1`，**CRC 零失败、陷阱零命中、
崩溃零复现**（PANIC 计数全程停在基线 49）。唯一的红是第 15 轮
"本轮无 Raft 提案被丢弃"（实际 2 条，见 R-P4-22），与本条无关。

**结论的边界**：只能说"**正常回放的记录字节完好**"——尚未取到崩溃时刻的
CRC 判据，因为整跑没有崩溃。故 R-P4-20 的"不同代 vs 读偏"这一位**仍未定**，
CRC 校验本身的价值（健壮性）不受影响。

##### ★ R-P4-22：Raft 提案被丢弃（2026-08-18，**新观测，未查**）

第 15 轮首次出现：`本轮无 Raft 提案被丢弃（ring_full_drops + quorum_drops）
（实际='2' 期望='0'）`。表示有提案因**环形缓冲写满**或**多数派不可达**被丢弃。

本次会话此前从未出现过这一项失败。样本仅 1 次，**不做归因**。
与 R-P4-20 无关（PANIC 计数未变）。记录在案，待后续观察是否复现。

> **★ T6.0 就地标注（2026-09-02）**：以下"已排除"与"下一步"两段的内容
> **不属于 R-P4-22**，是 **R-P4-20** 的根因排查，当初错挂在此。R-P4-22 本身
> （Raft 提案被丢弃）至今**未查**，且已核实与 §13 约束 13 同源——它是分叉
> 风险的唯一可观测出口，不是计数器噪声。详见 `docs/P6_PRECHECK.md` 结论四。
>
> 且**下一段的 `relNumber` 代际校验假说已作废**：R-P4-20 的根因于 2026-09-02
> 定案为"本地崩溃恢复的无条件 FPI 覆盖 + 无基线游标"（P6_PRECHECK 结论二）。
> 已取证的 08-18 那一例里 locmap 配对成立、回放顺利追平到 11，**relNumber 是
> 对上的**，该假说解释不了它。**P6 不要照此实施。**

**已排除**：块号越界（守卫零触发证否）、重放已应用记录（页 LSN 幂等证否）、
游标必须为 0（worker3 从 10 起亦崩）。

**下一步（需设计决策，未实施）**：把"代际"显式绑定到流上再校验。
可用材料：`ShardReplayCtx.locmap_gen`（已存在的 locmap 代次概念）、
locmap 里记录的 `local_loc.relNumber`。要点是现有守卫 1 只查
`smgrexists(local_loc)` —— 表被重建后 relNumber 变了，而**旧文件可能因延迟
unlink 仍然存在**，守卫便会误放行。故校验应改为"locmap 记录的 relNumber
是否等于该分片当前关系的 relNumber"，不等即拒绝认领。
此改动涉及代际标识的写入与持久化，**不宜再凭推断直接改**（本条已有一次
判据选错的失败），需先确认 relfilenode 的可得性与延迟 unlink 的实际行为。

##### ★ R-P4-23：`rebuild_shard_identity()` 的 DELETE 分支缺悬空过滤（2026-08-18，**已修**）

**这是本轮回归反复无常的真因**，不是夹具问题，也不是清理脚本造成的。

**缺陷**：
```sql
DELETE FROM shard_identity si
 WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_dist_shard s
    WHERE pg_catalog.to_regclass(
              pg_catalog.shard_name(s.logicalrelid, s.shardid))::oid = si.local_oid);
```
`shard_name()` 会对**每一行** `pg_dist_shard` 求值。只要有一行的 `logicalrelid`
指向的逻辑表已被删（悬空行），它就抛
`object_name does not reference a valid relation`，**整个
`rebuild_shard_identity()` 报废**。

**连锁**：身份重建报废 ⇒ `shard_identity` 建不出来 ⇒
`local_partition_for_shard()` 返回空 ⇒ 下游取到空变量、SQL 退化成无参调用
（实测报错表象是 `get_partition_flush_lsn()` "函数不存在"，实为参数为空）
⇒ **所有依赖分片身份的套件成片失败**，且与它们自身无关。

**为什么此前难以定位**：失败看起来随机且跨套件 —— 环境里只要存在**任何一行**
悬空 `pg_dist_shard`，当轮所有相关套件就一起红；清掉后又全绿。我一度归因为
"我的清理脚本清得过狠"，**该推断已收回**。

**同一函数的 INSERT 分支早已修过这个坑**（注释原文："过滤必须**先于** JOIN 的
shard_name 求值 —— v1 只加 WHERE，实测 planner 仍先求值 shard_name 而炸"），
用的是 `WHERE ds.logicalrelid::oid IN (SELECT oid FROM pg_class)` + **`OFFSET 0`
优化栅栏**。**DELETE 分支漏了同款防护。**

**修法**：照 INSERT 分支原样补上栅栏（只加 WHERE 不够，planner 仍可能先求值）。
已部署 9/9。

**实证**：修复前后同环境对照 ——
| 套件 | 修复前 | 修复后 |
|---|---|---|
| test_shard_pagecmp_p1 | 42/7 | **49/0** |
| test_dtx_convergence_p4 | 34/11 | **45/0** |
| 七套件合计 | 292/18 | **310/0** |
两个套件**同时**回到满分，同源确认。

##### ★ 测试体系的环境隔离缺陷（2026-08-18 记，供后续避坑）

本轮为取一组可信数字反复失败多次，根因是**套件之间没有环境隔离**：每个套件都
假定自己从某个"干净"状态起步，却都不自己建立那个状态，而依赖运行顺序。
已抓到两个**互相冲突**的需求：`tso_si_p3` 要 TSO 配好且纪元纯净；
`shard_xid_p1` 的负向用例要 TSO **没**配（有 ts 就触发不了守卫）。靠调顺序无解。

**净场必须覆盖五层**（每一层都是一次失败换来的）：
1. 宿主机测试进程 —— 批跑外壳未杀干净会继续遍历、与新跑并发；
2. **容器内数据库会话** —— `docker exec` 派生的 psql 不随宿主机进程死，实测一个
   `ANALYZE` 持锁挂了 3 小时堵死后续 DROP，而当时宿主机进程表与锁文件都显示"干净"；
3. GUC（`tso_master`/`tso_conninfo`/`shard_relids`/`shard_safety_mode`）；
4. TSO 纪元（计数器在共享内存，须重启协调者）；
5. **各 worker 的本地残表 + 悬空元数据** —— 只查协调者的
   `pg_dist_partition`/`pg_dist_shard` 会漏，实测九节点残留 47 张夹具表。

**这也解释了历史"634/14 套件"基线为何难以复现**：它多半是在某个特定顺序下取得
的，而顺序本身从未被记录。

#### P4 出口清单（全部勾掉才进 P5）

**核验时间：2026-08-18。逐条证据见下方"出口核验记要"。**

- [x] **崩溃矩阵逐格全绿（里程碑门禁）** —— **⚠️ 携带已知缺陷勾选**。
      **【用户裁定 2026-08-18】允许携 R-P4-20 出口。**
      口径说明：49 项功能断言稳定通过（近 30+ 轮几乎全 49/0），崩溃矩阵
      M1–M5 / S1 / Q1–Q3 逐格均绿；唯一未闭项是 **R-P4-20**（回放后台进程
      PANIC，不破坏事务语义，详见该条裁定块）。
      **本条不是无条件全绿**，进 P5 前须按 R-P4-20 裁定块重新评估。
- [x] **跨分片写事务端到端**（提交点=协调者组多数派落盘；ACK 在其后）
      —— T4.5 套件 **45/0**（2026-08-18 复跑）。
- [x] **跨分片读一致快照**（start_ts 传播；三态问询三分支）
      —— T4.7 套件 `S1`/`Q1`/`Q2`/`Q3` 四条断言齐备且稳定通过。
- [x] **§9.2 四层门禁用例并入基线；禁用项/引用表裁定落档**
      —— T4.6 套件 **20/0**（2026-08-18 复跑），六组用例覆盖禁用项负向、
      门控关闭放行、放行项正向、安全网（第 1 层）strict 报错、ANALYZE 分叉、
      引用表现状核查（V3 裁定取证）。
- [x] **MARKER/DTX ts 换源完成且双宇宙审计清零（R-P3-2 关闭）**
      —— 换源于 T4.4 完成；**全量审计已做**（见记要），未发现任何跨宇宙比较点。
- [x] **全量回归零新增 FAIL** —— **【用户裁定 2026-08-18】降格为"本环境
      7 套件零新增 FAIL"。**
      降格依据（核验中发现，此前未被记录）：**634/14 套件的基线是跨环境的**。
      按 `CONTAINER` 变量逐个套件归属：
      | 目标环境 | 套件数 | 套件 |
      |---|---|---|
      | `pg-citus-tx2`（**本环境**） | **7** | dtx_convergence_p4 / dtx_tso_p4 / shard_clog_p2 / shard_gating_p4 / shard_pagecmp_p1 / shard_xid_p1 / tso_si_p3 |
      | `pg-citus-tx` | 4 | dtx_commit_marker_tx2 / dtx_replay_tx1 / fastpath_divergence_tx4 / promote_catchup_tx3 |
      | `pg-citus-replay` | 6 | clog_hole_c4 / ddl_fileset_d1 / follower_replay_r1 / freeze_sync_d2 / lazy_replay_l1 / local_wal_conflict / txn_layer_r2 |
      | `pg-partdist-raft4` | 1 | shard_identity_p0 |
      | 更早的单节点环境 | 6 | bulk_insert_recovery / corrupt_segment_recovery / crash_recovery / demux_backlog_recovery / enospc_recovery / multi_table_isolation / segment_boundary_lsn / shard_auto_init |
      而三套 9 节点环境**互斥**（同时只能起一套），故"全量 634"在不拆建环境的
      前提下无法完整满足。
      **一次误判留痕**：我最初直接跑了 tests/ 下全部 27 个套件，首个
      `bulk_insert_recovery` 即报 `Timeout waiting for port 5432` —— 它指向
      别的环境。**"跑全部"是错的做法**，按 `CONTAINER` 归属筛选才对。
      **本环境 7 套件实测（2026-08-18 最终跑）：310/0 全绿**
      | 套件 | 结果 |
      |---|---|
      | test_shard_clog_p2 | 64/0 |
      | test_shard_pagecmp_p1 | 49/0 |
      | test_shard_xid_p1 | 45/0 |
      | test_tso_si_p3 | 38/0 |
      | test_dtx_convergence_p4 | 45/0 |
      | test_shard_gating_p4 | 20/0 |
      | test_dtx_tso_p4（T4.7） | **49/0** |
      | **合计** | **310 / FAIL=0** |
- [x] **文档/补丁（0009）/pg-install 成对；pg_raft 变更与流控 #39 处置落档**
      —— 补丁 `0009` 与 `pg-install/bin/postgres` 同在提交 `971da0f`，
      08-13 之后**未再改动任何内核补丁**，两者无失配；pg_raft 解冻批次
      #2–#5 与 #39 处置均已逐条落档。

##### 出口核验记要（2026-08-18）

**双宇宙审计（第 5 条）的做法与结论** —— 原文要求"P4 换源前全量重审计"，
故按**产生端 + 消费端**两侧全查：
- **产生端**：全仓扫描本地时钟取值点（`GetCurrentTimestamp` 等）。唯一会把
  本地时钟当 ts 返回的是 `TsoMarkerCommitTs()`，且它以 `tso_configured()`
  分流 —— 未配置走本地时钟（遗留宇宙），配置了全走 TSO；DTX 决议取号同样
  "未配置返回 0（决议侧回退本地时钟，遗留宇宙不混）"。**两宇宙在产生端即
  分离，不靠事后判断数值。** 其余命中点（租约过期、心跳、超时、tick）均为
  本地时钟自比，不与逻辑值同场。
- **消费端**：全仓扫描 ts 参与的比较。集中于 `shard_visibility.c` 的可见性
  判据（`commit_ts < start_ts`）与 `enhanced_clog.c` 的取值（非比较）。
  **未发现任何一处会让两个宇宙的值同场比较。**
- **运行时兜底**：T4.5 套件含断言 `决议 cts=TSO 逻辑值（cts < 1e9）` ——
  本地时钟值约 7×10¹⁷，一旦串线必然翻红。本次 **45/0**，未串。
⇒ **R-P3-2 关闭。**


---

### 3.4 P5 详细任务分解（2026-08-18 进期细化，设计 §6 + §7）

**范围**：分片级 vacuum/GC 全章 + 回卷护栏。
**骨架**：三个每分片变量 + 一条顺序铁律。
- `clog_truncate_before` —— 实际截断点，**隐式 freeze 点**、回卷龄的基点；
- `ShardVacuumXid` —— 两态恢复标记；
- `VacuumTargetXid` —— 本次目标（由前缀扫描算出）。
- **顺序铁律**：数据页、索引、堆全部清完，才许动 clog。

**现状盘点（动手前实测）**：三变量与截断能力**尚未存在**（全仓无
`clog_truncate_before` / `ShardVacuumXid` / `VacuumTargetXid`）；分片 clog 现有
API 只到落账/读状态/认领/删表（`ShardClogSetRunning|SetVerdict|SetPrepared|
ReadStatus|ReadSlot|ClaimRange|RememberDrop`），**没有截断**。
`ShardClogClaimRange` 是 §6.6 无主 RUNNING 认领，P2 已完成。

| 任务 | 内容 | 验收 |
|---|---|---|
| **T5.1** ✅ | 三变量与持久化 —— **已完成（2026-08-18，验收 7/0）** | 见下方实施记要 |
| **T5.2** ✅ | 前缀扫描算 `VacuumTargetXid` —— **已完成（2026-08-18，验收 6/0）** | 见下方实施记要 |
| **T5.3a** ✅ | **③ xmax 消毒**：截断点以下的 ABORTED 与 lock-only xmax 清成 0 —— **已完成（2026-08-21）** | 见下方实施记要 |
| **T5.3b** ✅ | **① 删中止 xmin 的元组**（不做⇒截断后**幽灵行复活**）—— **已完成（2026-08-21）** | 见下方实施记要 |
| **T5.3c** ⚠️ | **② 删 `xmax` 已提交的死元组**（**判据看 xmax 不看 xmin**）—— **无索引通路已完成（2026-08-21）**，套件合计 **83/0**；**索引两阶段未做**（P1 禁索引，当前不可达也不可测，缺口见记要） | 见下方实施记要 |
| **T5.4a** ✅ | **截断 + 顺序铁律 + 免查隐式冻结区** —— **已完成（2026-08-21）**，套件合计 **115/0** | 见下方实施记要 |
| **T5.4b-1** ✅ | **follower 跨节点回放实测** —— **已完成（2026-08-21）**，新套件 `test_shard_vacuum_replay_p5.sh` **43/0**；三次记要重复挂账的那一项就此关闭 | 见下方实施记要 |
| **T5.4b-2** ✅ | **§6.7 两水位的 CTRL 记录复制**（复用 `FREEZE_UPDATE` 通道，不新增 opcode）—— **已完成（2026-08-21）**，套件升为 **48/0** | 见下方实施记要 |
| **T5.5** ✅ | **两态恢复**：趟中崩溃整趟重来（幂等）；趟完未截断只补截断 —— **已完成（2026-08-21）**，本地套件升为 **139/0**；**并修掉一处自身缺陷**（落「趟完」标记先于页面 WAL 刷盘） | 见下方实施记要 |
| **T5.6** ✅ | **回卷护栏两阶段**（基点取 `clog_truncate_before`；阶段 1 到龄信号；阶段 2 达停发线时该分片进只读）—— **已完成（2026-08-21）**，本地套件升为 **163/0**；**自动启动器未做**，并更正了 `SHARD_XID_SANITY_MAX` 的两处问题 | 见下方实施记要 |
| **T5.7** ✅ | **出口回归** —— **已完成（2026-08-21）**：本环境 7 套件 **310/0**（与 P4 出口基线逐套相同）+ P5 新套件 **211/0**，合计 **521/0**；六项挂账逐条落档，R-P4-20 已按要求出口复评 | 见下方实施记要 |

**可复用的既有设施（T5.1 动手前已勘察）**：
- **CTRL 通道现成**：`PARTWAL_FLAG_CTRL` + `rmid=0xFF` 哨兵 + `info=opcode`，
  已有 `FILESET_UPDATE(0x01)` / `FREEZE_UPDATE(0x02)`。
  产生端 `PartWALAppendCtrl(partition_id, opcode, payload, len)`
  （`include/partwal_sync.h:134`，用例见 `wal/shard_fileset.c:544`）；
  应用端 `ApplyCtrlRecord`（`replay/shard_replay.c:~1240`）按 `hdr->info` 分派。
  **按设计原文复用 `FREEZE_UPDATE` 通道换语义**，不新增 opcode。
- **checkpoint 有向后兼容先例**：`ShardApplyCheckpoint` 现为版本 3，注释写明
  "`nxidmap == 0` 时新算法与旧的逐字节等价，R1 时代的 checkpoint 继续有效"
  —— 加水位字段照此办理，不必升版本破坏旧文件。
- **复制部分很轻（§6.7）**：vacuum **只在 leader 执行**，页面修改本身走 parwal
  流被 follower 逐字节回放，follower 不跑自己的 vacuum；"页在前、截断在后"由
  **流序自动保证**，无需额外协议。

#### T5.1 实施记要（2026-08-18，验收 **7/0**）

**落点选择：扩展既有水位文件，而非另造设施。**
两个水位并入 `$PGDATA/pg_shard_xid/<oid>`。理由是该文件**本就有格式演进先例**
（T1.2 的 4 字节 → T2.4 的 8 字节，且兼容读旧格式），且 vacuum 水位与 xid 水位
本就同源。格式扩为 16 字节：
`{alloc_wm, claim_wm, clog_truncate_before, shard_vacuum_xid}`（uint32 小端），
**向后兼容读 8/4 字节**，缺失字段取 0 —— 含义"从未截断、从未 vacuum"，
**这是安全方向**（免查隐式冻结区为空，所有 xid 照常查 clog，不会把未决当已提交）。

**不变式守在唯一出口**：`clog_truncate_before <= shard_vacuum_xid` 的检查放在
`shard_xid_persist_watermark()`（落盘的唯一出口），违反即 ERROR 且状态不推进，
不散在各调用点。

**两水位必须进 shmem 槽位**：落盘函数是**整文件覆写**，任何一次发号/认领落盘
若不带上它们就会抹成 0。四处既有调用点全部改为原样带回。

**对外接口**：`ShardVacuumGetWatermarks/SetWatermarks` + SQL 包装
`shard_vacuum_watermarks()` / `shard_vacuum_set_watermarks()`，9/9 部署。

##### ★ 验收中发现并修复的两个真问题

**其一：先改内存后落盘 —— 被拒绝的写照样可见。**
`ShardVacuumSetWatermarks` 初版先更新槽位、再落盘。落盘处的不变式守卫 ERROR
时，**槽位已被改脏**，而读接口优先读槽位 ⇒ 本该被拒的 `300/200` 从读接口里
读得出来。根因：**ERROR 中止事务，但不回滚共享内存**。
⇒ 改为**先落盘、成功了才更新槽位**。这类"内存先于持久化"的写法在 shmem 上
一律是错的，已写进代码注释。

**其二：水位文件损坏值被静默当作 xid 发出去。**
测试中文件被写坏后，损坏值 `858814556` 被当作有效 alloc_wm 直接用于发号 ——
**新插入行的 xmin 就是这个垃圾数**，且无声无息，一旦发出即污染该分片 xid 空间。
（坏值是测试造成的，但"读取端零校验"是真实缺口。）
⇒ 读取端加**合理性校验**：超过 `SHARD_XID_SANITY_MAX` 即判损坏、ERROR 要求
人工介入（fail-closed，宁可停也不发坏号）。

> **【2026-08-21 T5.6 更正】上面这段的举证是错的。** 原文说 858814556
> "正落在该区间之上"，而 **858814556 < 2^30 = 1073741824** —— 这道守卫
> **拦不住它自己引用的那个值**。绝对阈值天生分不清"损坏值 8.6 亿"与
> "真跑了 8.6 亿笔事务的分片"，是固有局限而非参数没调好。
> 且 2^30 会抢在 §7 回卷护栏（停发线 2^31 − 边距）前面触发，把"该做
> vacuum 了"报成"文件损坏"。阈值已在 T5.6 上抬到 2^31，见 T5.6 记要。

##### T5.1 验收明细（7/0）

| # | 断言 | 结果 |
|---|---|---|
| ① | 新分片初始两水位 0/0 | PASS |
| ② | 写入 100/200 读回一致 | PASS |
| ③ | 不变式拒绝 300>200 | PASS |
| ④ | 被拒后水位未被污染 | PASS |
| ⑤ | 相等水位合法（无未完成的趟） | PASS |
| ⑥ | **immediate 崩溃重启后两水位保持**（核心） | PASS |
| ⑦ | 8 字节旧格式兼容读取 0（安全方向） | PASS |

**未做的部分（留给后续任务）**：设计 §6.7 要求两水位作为 CTRL 记录写进分片流
供 follower 同步。本任务只完成**本地持久化与恢复**；复制部分待 T5.4 截断落地时
一并做（那时才有真实的水位推进事件可复制）。

#### T5.2 实施记要（2026-08-18，验收 **6/0**）

**实现**：`ShardVacuumComputeTarget(shard, from, safe_ts, ceiling, &stop_reason)`
（`src/shard_clog.c`）。从 `clog_truncate_before` 起顺扫，返回可安全截断到的
**前一条**；`InvalidTransactionId` = 一条都不能清。`stop_reason` 回填停因供
验收与排障。

**要害规则：ABORTED 一律放行。** 这是设计 §6.3 里最反直觉的一条 —— 若 ABORTED
也挡，**一个中止事务就能永久钉死截断，直到回卷死亡**。它留下的垃圾由 §6.4 的
页面三类动作清掉，不靠"挡住前缀"来保证。

**停止条件**：RUNNING（未决）/ PREPARED（2PC 未决，**绝不单方推定**）/
COMMITTED 但 `commit_ts >= GlobalSafeTs`（仍可能被活跃快照看到）。
稀疏空洞读不出槽时按 RUNNING 处理 —— 空洞不代表"没有这个事务"，只代表判决
没写下来（`shard_clog.h` 头注释的既有约定）。

##### ★ 实现中发现的两处约束

**其一：`partdist_global_safe_ts()` 只在 TSO master 上可用**（非 master 直接
ERROR），而 vacuum 跑在**分片 leader（worker）**上，本地读不到。
⇒ 新增 `TsoGetGlobalSafeTs()`（`src/tso_client.c`），走现成的 `tso_rpc` 通道
（与 start_ts / commit_ts / 决议取号同一条路）。
**取不到时返回 0 = 什么都不清**：GlobalSafeTs 偏小只会少清垃圾（安全方向），
偏大才会误删活跃快照仍需要的版本，故不回退到任何本地估计值。

**其二：`commit_ts = 0` 的 COMMITTED 槽会挡住前缀。**
判据是 `commit_ts > 0 && commit_ts < safe_ts`，所以 P2 时代遗留、或 T4.4 换源
前写下的 `commit_ts=0` 的已提交槽，会被判为"不放行"而永久阻挡截断。
**这是已知边界，本任务不处理** —— 换源已在 T4.4 完成，新数据不再产生此形态；
存量清理需要一次性迁移，留待 T5.7 出口前评估。

> **【2026-08-21 T5.3c 实测更正】上面这句"新数据不再产生此形态"只在 TSO 已
> 配置时成立。** 未配置 TSO 的**遗留模式**下，`ShardClogSetVerdict` 每次都写
> `commit_ts = 0`（"遗留模式 0 —— 对一切快照可见，恰是 P2 语义"），所以
> `commit_ts=0` 的已提交槽是遗留模式的**常态**，不是历史残留。
> 这在语义上是自洽的：遗留模式下压根没有 GlobalSafeTs，`ShardVacuumComputeTarget`
> 会直接以 `no-safe-ts` 返回"一条都不清"，因此永远算不出会撞上它的截断点。
> 但**验收夹具必须知道这件事**：本环境 TSO 未配置，T5.3c 的夹具得用
> `sclog_wts` 给已提交条目补真时间戳，否则 ② 的守卫会（正确地）全部拦下。

##### T5.2 验收明细（6/0，safe_ts=1000）

| # | 场景 | 期望 | 结果 |
|---|---|---|---|
| ① | 全 COMMITTED 且 ts 够旧 | 扫到上界，`scanned-to-ceiling` | PASS |
| ② | **中间夹 ABORTED** | 仍推进，`scanned-to-ceiling` | PASS |
| ③ | `commit_ts >= safe_ts` | 停在前一条，`commit-ts-too-new` | PASS |
| ④ | RUNNING 阻挡 | 停在前一条，`running` | PASS |
| ⑤ | PREPARED 阻挡 | 停在前一条，`prepared` | PASS |
| ⑥ | `safe_ts = 0`（取不到） | 一条都不清，`no-safe-ts` | PASS |

**新增的验收辅助**：`sclog_wts(oid, xid, status, ts)` —— 既有的
`sclog_write()` 只写 status、`commit_ts` 恒 0，无法构造"带时间戳的已提交槽"。

#### T5.3 实现方案与补丁清单（2026-08-18 出，**待用户裁定后动手**）

**规模判断：T5.3 与 T5.1/T5.2 不是一个量级。** 前两者是水位持久化与纯计算，
本任务要**写页面**、**清索引**、**改元组头**，且必然动内核。

##### 一、现状：判有、收无

`0008-shard-vacuum-read-hook.patch` 已装在
`heapam_visibility.c` 的 `HeapTupleSatisfiesVacuumHorizon`（判定入口），
但它是 **T2.6 为 ANALYZE 装的"只判不收"钩子**，补丁注释原文写明
"回收页面动作是 P5"。内核分叉点的注释更直接：

> `remaining reclaim-side callers (prune skipped, VACUUM/CLUSTER/CREATE INDEX
> intercepted) stay fenced off`

即：回收侧的调用者当前**要么被跳过、要么被禁令拦住**。T5.3 就是把这道封锁
**有控制地打开** —— 只对分片表、只在 vacuum 自己的通道里开。

现有钩子能产出 LIVE / INSERT_IN_PROGRESS / DELETE_IN_PROGRESS /
RECENTLY_DEAD / DEAD 五种裁决，且 `RECENTLY_DEAD` 被强制配上
`ReadNextTransactionId()` 作 `dead_after`，**故意让所有调用者落进保守分支**
（判而不收）。这一条在 T5.3 里必须改 —— 否则永远收不掉。

##### 二、三类动作的落点与代价

| 动作 | 内核落点 | 能否在扩展内做 | 说明 |
|---|---|---|---|
| **① 删中止 xmin 的元组** | `heap_page_prune` / `lazy_scan_prune` | ❌ 需内核 | 现有钩子已能判成 `HEAPTUPLE_DEAD`，但**执行删除**要走 prune 的行指针回收路径 |
| **② 删死元组 + 索引两阶段** | `lazy_scan_heap` → `lazy_vacuum_all_indexes` → `lazy_vacuum_heap_rel` | ❌ 需内核 | 工作量最大。判据**看 xmax 不看 xmin**（设计 §6.4：没被删过的老行是活的，零页面动作） |
| **③ xmax 消毒** | `heap_prepare_freeze_tuple` 的 xmax 处置 | ❌ 需内核 | 把截断点以下的 ABORTED 与 lock-only xmax 清成 `InvalidTransactionId` |

**结论：三类动作全部需要内核补丁。** 扩展侧只能提供判据（哪些该删、截断点在哪、GlobalSafeTs 是多少），执行必须在内核。

##### 三、补丁清单（建议）

| 补丁 | 内容 | 风险 |
|---|---|---|
| **0010-shard-vacuum-reclaim-hook** | 在 `lazy_scan_prune` / `heap_page_prune` 加分片分叉：分片表的死元组判定改问扩展，并**允许回收**（解除 0008 的"永远保守分支"约束） | 中。触及 prune 主路径，误判即丢数据 |
| **0011-shard-freeze-xmax** | `heap_prepare_freeze_tuple` 的 xmax 分支加分片处置：截断点以下的 ABORTED / lock-only xmax 清零 | 中。冻结路径，改错会让活行被判死 |
| **0012-shard-vacuum-entry** | 放开 `VACUUM` 对分片表的禁令（当前被 §9.2 第 3 层拦），改为走分片专用通道 | 低。只是解禁 + 路由 |

**为什么不能合成一个补丁**：三者的失败模式完全不同 —— ① 误判丢数据、
③ 误判活行被判死、⑫ 只是入口。分开才能独立回退与独立验收。

##### 四、必须先解决的前置问题

1. **`RECENTLY_DEAD` 的 `dead_after` 造假**：现有钩子给它配
   `ReadNextTransactionId()`，目的是把调用者钉在保守分支。T5.3 要收，就得让
   它携带**真实的分片语义判据**。但 `dead_after` 是原生 xid 类型，分片 xid
   与它不同宇宙 —— **这是本任务最硬的一处设计冲突，方案未定**。
2. **索引清理的 TID 有效性**：索引两阶段依赖"收集死 TID → 清索引 → 回收行
   指针"的顺序，中间不能有并发写入改变页面。分片表的写入走 parwal 复制，
   vacuum 只在 leader 跑（§6.7），但 **leader 切换会打断这个顺序** —— 需要
   与 T5.4 的"顺序铁律"一并设计。
3. **follower 侧的一致性**：vacuum 的页面修改走 parwal 流被 follower 逐字节
   回放（§6.7），意味着**三类动作产生的每一次页面写入都必须是可复制的**。
   现有 R-P4-20（回放 PANIC）尚未闭，此处新增大量页面写入会放大它的暴露面。

##### ★ 前置问题 1 的解法评估：`dead_after` 的宇宙冲突（2026-08-18）

**冲突的准确形态**（勘察后修正了先前的粗略描述）：

`dead_after` **不是一个存储字段，而是一个纯粹的"比较用出参"**。
`HeapTupleSatisfiesVacuumHorizon()` 判出 `RECENTLY_DEAD` 时把该元组的
**xmax** 写进 `*dead_after`（`heapam_visibility.c:1446`），调用方再拿它与
自己的可见性水平线比较，决定是否**晋升为 DEAD**。

消费者只有**两个**，机制很窄：

| 消费者 | 比较对象 | 用途 |
|---|---|---|
| `HeapTupleSatisfiesVacuum`（`heapam_visibility.c:1223`） | `OldestXmin` | 通用判定 |
| `heap_prune_satisfies_vacuum`（`pruneheap.c:525`） | `prstate->oldest_xmin` / `old_snap_xmin` / `GlobalVisState` | **vacuum 真正回收走的路径** |

现有 0008 把它塞成 `ReadNextTransactionId()`（永远大于任何 OldestXmin），
于是两个消费者的晋升判断**恒为假** —— 这就是"判而不收"的实现手法。

**冲突本质**：这三个比较对象全是**原生 xid 宇宙**的水平线，而分片元组的
xmax 是**分片 xid**。两者数值空间独立、无可比性。直接把分片 xmax 塞进
`dead_after` 会得到无意义的比较结果 —— 可能误删活行，也可能永不回收。

##### 四种解法

**解法 A：扩展侧直接给结论，不走 `dead_after`。**
钩子不再返回 `RECENTLY_DEAD` + 假 `dead_after`，而是**自己完成晋升判断**
（用分片 clog + GlobalSafeTs 判"是否还有活跃快照需要它"），直接返回
`HEAPTUPLE_DEAD` 或 `HEAPTUPLE_RECENTLY_DEAD` 终值。
- 优点：**彻底回避宇宙冲突** —— 分片语义的判断留在分片宇宙里做，
  原生水平线根本不参与；改动面最小（只改钩子契约，不动两个消费者）。
- 缺点：钩子必须能拿到 vacuum 的"本轮判据"（GlobalSafeTs / 截断点），
  而当前钩子签名只有 `(htup, buffer, *res)`，**需要扩签名**传入本轮上下文。
- 风险：中偏低。晋升判断从内核挪到扩展，逻辑集中在一处，验收容易构造。

**解法 B：给分片元组配"影子原生 xid"。**
维护分片 xid → 原生 xid 的映射，`dead_after` 填映射后的原生值，让原生比较
继续有效。
- 优点：不动内核契约。
- 缺点：**要凭空造一套映射并持久化**，且映射必须与分片 clog 的判决保持一致；
  回卷时两套 xid 空间各自回卷、对应关系还要维护。
- 风险：**高**。等于再造一个 xid 宇宙来给旧宇宙打补丁，与"分片 xid 独立宇宙"
  的整体设计相悖。**不建议。**

**解法 C：把水平线也换成分片宇宙的。**
在 prune 路径上给分片表传入"分片版 OldestXmin"，两边都用分片 xid 比较。
- 优点：语义最正。
- 缺点：`prstate->oldest_xmin` / `old_snap_xmin` / `GlobalVisState` 三处都要
  分叉，且 `GlobalVisState` 是内核全局结构、改它牵连极广。
- 风险：**高**，触及面远大于 A。

**解法 D：维持"判而不收"，vacuum 走完全独立的回收通道。**
不打通 prune 路径，扩展自己实现一套页面扫描与回收（不复用
`heap_page_prune`）。
- 优点：与内核完全解耦。
- 缺点：**要重写 prune + 索引两阶段 + 行指针回收的全部逻辑**，且必须与内核
  的 WAL 记录格式逐字节兼容（否则 follower 回放对不上，直接撞 R-P4-20 那类
  问题）。
- 风险：**最高**，工作量也最大。

##### 结论与建议

> **【已裁定，2026-08-21】用户采纳解法 A。** T5.3b/c 按 A 推进：晋升判断
> 由扩展在分片宇宙内做完再给终值，`dead_after` 不参与。

**推荐解法 A**，理由：
1. **冲突根源是"用原生水平线判分片元组"，A 是唯一从根上消除它的**——
   B 是给冲突打补丁、C 是把冲突扩散到三处、D 是绕开但要重写一切。
2. 改动面最小且集中：只改钩子签名与实现，两个消费者的代码**一行不动**
   （它们收到的已是终值，晋升判断自然不触发）。
3. 与既有设计一致：分片 xid 本就是独立宇宙，判断留在宇宙内做才是原意。

**A 的代价（须明确接受）**：钩子签名要从 `(htup, buffer, *res)` 扩为携带
本轮 vacuum 上下文（GlobalSafeTs / 截断点）。这意味着 **0008 补丁要改**，
而它已在役并被 T2.6 的 ANALYZE 路径依赖 —— 扩签名时必须保证 ANALYZE 侧
行为不变（传 NULL 上下文即退回"只判不收"）。

**若采纳 A，补丁清单相应调整**：原 `0010-shard-vacuum-reclaim-hook` 拆为
`0010a-extend-vacuum-hook-signature`（扩签名，向后兼容）+
`0010b-allow-shard-reclaim`（解除保守分支），前者可独立验收（ANALYZE 不受
影响即通过）。

##### 五、我的建议

**分三步走，而不是一次做完**：
- **T5.3a**：③ xmax 消毒（补丁 0011）。最独立，不涉及行指针回收与索引，
  失败模式单一（活行被判死，验收容易构造）。
- **T5.3b**：① 删中止 xmin 的元组（补丁 0010 的一半）。涉及 prune 但不涉及索引。
- **T5.3c**：② 死元组 + 索引两阶段（补丁 0010 的另一半 + 0012）。最大、最险，
  且依赖前置问题 2/3 的结论。

**前置问题 1（`dead_after` 的宇宙冲突）必须先有答案**，否则 T5.3b/c 无从下手。

**预判需要用户裁定的点**：T5.3（heapam 冻结路径）与 T5.6（发号拒绝）可能触及
内核补丁与 pg_raft 解冻，到该步先问。

#### T5.3a 实施记要（2026-08-21，验收 **33/0**）

**交付**：`ShardVacuumSanitizeXmax()`（新文件 `src/shard_vacuum.c` /
`include/shard_vacuum.h`）+ SQL 包装 `partdist.shard_sanitize_xmax(regclass,
bigint)`（9/9 部署）+ 验收套件 `tests/test_shard_vacuum_p5.sh`。

##### ★ 与 T5.3 方案的一处修正：本任务**不需要内核补丁**

T5.3 方案表里写"三类动作全部需要内核补丁"，动手勘察后**这一条对 ③ 不成立**，
现予更正。理由分两半：

- **策略侧确实不能用内核的**：`heap_prepare_freeze_tuple()` 从头到尾拿元组 xid
  与 `VacuumCutoffs` 里的**原生** `relfrozenxid` / `OldestXmin` 比较，并按
  `checkflags` 去查**原生** clog；分片元组的 xmin/xmax 是分片 xid，三处比较
  全是异宇宙比较。更要命的是结尾的 `heap_tuple_should_freeze()` 会把分片 xid
  喂进 `NoFreezePageRelfrozenXid` 跟踪器 —— **污染原生回卷账本**。给它开分叉
  等于把整个函数改写。设计 §6.4 ③ 原文"即 `heap_prepare_freeze_tuple` 的
  xmax 处置**换形态重现**"说的正是这件事。
- **执行侧完全可以用内核的**：`heap_freeze_execute_prepared()` 是 `extern` 的
  （`heapam.h:272`），只吃一组算好的 `HeapTupleFreeze` 计划，负责改页 + 发
  内核原样的 `XLOG_HEAP2_FREEZE_PAGE`。

⇒ **策略在扩展、WAL 由内核发。** 这样 follower 收到的是内核标准记录，
`heap2_redo` 逐字节回放，**不新增任何记录格式** —— 恰好满足 §6.7 的硬要求，
也避开了"自造记录格式"这条 R-P4-20 级别的风险路径。
补丁清单里的 `0011-shard-freeze-xmax` 因此**取消**（编号不复用）。

##### 判据（四分支，少一条就不成立）

| xmax 形态 | 处置 | 不这么做的后果 |
|---|---|---|
| `>= trunc_before` | 不动 | clog 还查得到，动它是越权 |
| lock-only | 清成 0 | 锁随事务结束释放，留着会被免查区读成"已提交的删除" |
| clog **ABORTED** | 清成 0 | **本动作的正主**：截断后"中止了的删除"读成"早已提交的删除"，**活行被判死** |
| clog **COMMITTED** | 不动 | 真删除，元组由动作 ② 回收；留着被读成"已提交的删除"恰好正确 |

未决（RUNNING / PREPARED / 空洞）分两种，**区别对待是要点**：
- 落在 `[当前截断点, trunc_before)` ⇒ **ERROR**。§6.3 的前缀扫描遇未决即停，
  调用者不可能算出跨过未决条目的 target；到这里说明 `trunc_before` 给错了，
  fail-closed 而不是"判不出来就不动"——后者会把本该报错的输入静默吞掉。
- 落在当前截断点以下 ⇒ 不动。其 clog 已被上一轮截断，判不出来了；这是过去时的
  既成事实（只可能来自上一轮违反顺序铁律），留着是唯一安全动作。

`multixact xmax` 一律 ERROR：补丁 0006 对分片表强制 `HEAP_XMAX_INVALID` 简单
路径（原生机器会拿分片 xid 误组 multixact，P1 实测过
`new multixact has more than one updating member`），出现 multi 即上游破防，
照常处置等于把分片 xid 当 multi 号解释。

##### 两处必须与内核逐字一致的细节

- 清 xmax 的 infomask 变换照抄 `heap_prepare_freeze_tuple` 的 `freeze_xmax`
  分支：清 `HEAP_XMAX_BITS`、置 `HEAP_XMAX_INVALID`、清 `HEAP_HOT_UPDATED` /
  `HEAP_KEYS_UPDATED`。（清 `HEAP_HOT_UPDATED` 曾疑心会断 HOT 链，查证后无碍：
  `HeapTupleHeaderIsHotUpdated()` 本身就带 `HEAP_XMAX_INVALID == 0` 前置条件，
  置了 INVALID 之后该位设不设都读作"未 HOT 更新"。）
- `checkflags` 恒 0。那两项检查（`HEAP_FREEZE_CHECK_XMIN_COMMITTED` /
  `_XMAX_ABORTED`）在 `heap_freeze_execute_prepared` 里查的是**原生** clog，
  喂分片 xid 进去是纯粹的误判源。
- `snapshotConflictHorizon` 传 `Invalid`：该值只在 hot standby 的
  `ResolveRecoveryConflictWithSnapshot` 里用于杀查询，分片副本走 pg_parwal
  物理回放（非 hot standby），且消毒只会让元组**更可见**，不存在需要杀的冲突。

##### 顺序铁律的落地方式

本函数**只清、绝不推进任何水位**。§6.4 末的"数据页、索引、堆全部清完，才许动
clog"意味着推进必须由 T5.4 的截断路径在确认整趟做完之后统一落 —— 把推进埋在
单个动作里，等于每个动作各自宣称"我这部分清完了"，铁律就无处强制。

##### T5.3a 验收明细（33/0）

| 段 | 断言 | 结果 |
|---|---|---|
| [0] | 节点就绪、扩展符号存在、pageinspect 就绪 | PASS ×3 |
| [1] | 构造四形态：r1 ABORTED xmax / r2 COMMITTED xmax / r3 无 xmax / r4 ABORTED 但号更大；三条 clog 状态核对；发号递增；消毒前可见 3 行 | PASS ×10 |
| [2] | 返回 `1/1/1`；**r1 xmax 清 0 且带 `HEAP_XMAX_INVALID`**；**r2 COMMITTED 不动**；**r4 截断点以上不动**；可见行数不变 | PASS ×6 |
| [3] | pg_waldump 在区间内看到 `FREEZE_PAGE` —— 走的是内核标准记录 | PASS |
| [4] | 幂等：第二趟 0 条 | PASS |
| [5] | 负向 ×3：区间内仍未决即拒（且被拒后 xmax 未被污染）、`trunc_before` 后退即拒、非分片表即拒；负向计数守卫 | PASS ×5 |
| [6] | **immediate 崩溃重启后消毒结果保持**（证明经 WAL 落盘，不是只改了内存） | PASS ×4 |
| [7] | 清场 + 本轮无节点崩溃 | PASS ×2 |

##### 验收脚本自身踩到的两个坑（记以备后用）

- **`heap_page_items` 认行不能靠 `t_data::text LIKE '%r1%'`** —— `t_data` 是
  bytea，文本形态是 `\x...` 十六进制，永远匹配不上，且失败形态是**返回空串**
  （靠 `check()` 的空值守卫才没变成静默通过）。改按行指针 `lp` 定位：四行按
  id 顺序一次插入、DELETE 不产生新版本，故 `lp1..lp4` 恒等于 `r1..r4`。
- **`pg_waldump -e <pg_current_wal_lsn()>` 会把最后一条记录排除掉** ——
  该函数返回的是**最后一条记录的末端**，消毒记录若正好是流水线末尾就落在
  上界之外。实测同一脚本两次运行一过一挂。改为先 `pg_switch_wal()` 再取上界。

##### T5.3a **未覆盖**的部分（不含糊其辞）

- **follower 回放未实测**。§6.7 要求页面修改随 parwal 流被 follower 逐字节
  回放；本套件只验到"leader 的 WAL 里确有内核标准 `FREEZE_PAGE` 记录"，
  跨节点比对没做 —— 需要一张已登记 `partition_map` 的复制分片夹具，属 T5.4
  规模。**留 T5.4 一并验**，届时 CTRL 水位复制本就需要同一套夹具。
- **lock-only 分支未实测**。P1 起分片表禁行锁（补丁 0005），构造不出 lock-only
  xmax；该分支是防御性的。
- **免查隐式冻结区尚不存在**。`xid < clog_truncate_before ⇒ 已提交` 这条解释
  规则**全仓未实现**（`shard_xid_state()` 现在一律查 clog）。因此 ③ 的
  端到端陷阱（"不消毒 ⇒ 截断后活行被判死"）**目前构造不出来**，本套件只能
  验到元组头这一层。解释规则属截断的配套，归 T5.4。

##### 顺带查出并补齐的两处 P5 前序遗漏

**其一：T5.2 的 SQL 包装从未进扩展安装脚本。**
`partdist_shard_vacuum_target` 当时只在 9 个节点上**手工建了函数**，没写进
`sql/pg_partdist--1.0.sql` —— 全新安装的节点上不存在它。本次一并补进。
（`sclog_wts` 是验收辅助，按 `sclog_write` 的惯例留在测试脚本里，不进扩展。）

**其二：T5.1 改水位文件格式时打翻了 `test_shard_clog_p2` 的一条断言，无人察觉。**
该套件第 [3] 段用 `od -An -tu4` 直读 `pg_shard_xid/<oid>` 断言内容为
`4099 4099`；T5.1 把文件从 8 字节扩到 16 字节后，实际输出成了
`4099 4099 0 0`，这条断言自 T5.1 起一直是 FAIL —— **因为 T5.1/T5.2 都没有
回跑既有套件**，直到本次 T5.3a 顺手做冒烟测试才暴露。
基线 64/0 ⇒ 实测 63/1。断言期望已按新格式更正，复跑 **64/0** 恢复基线。

> 教训（与 §13 既有条目同类，此处记具体形态）：**改磁盘格式必须回跑一切直读
> 该文件的断言。** 本例里格式变更与断言相隔两个任务，且失败信息
> （"期望 4099 4099"）看上去像发号出了问题，与真因隔了一层。

#### T5.3b 实施记要（2026-08-21，`test_shard_vacuum_p5.sh` 合计 **56/0**）

**交付**：`ShardVacuumRemoveAbortedXmin()`（`src/shard_vacuum.c`）+ SQL 包装
`partdist.shard_remove_aborted(regclass, bigint)`（9/9 部署）+ 套件 [8]–[13] 段。

##### 为什么是"两条 WAL 记录"而不是一条

原生对**无索引**表的回收本来就是两步（`vacuumlazy.c`：`nindexes == 0` 时
`lazy_scan_prune` 之后立刻 `lazy_vacuum_heap_page`）：

| 步 | 记录 | 动作 |
|---|---|---|
| ① | `XLOG_HEAP2_PRUNE` | LP_NORMAL ⇒ LP_DEAD（根元组）或 LP_UNUSED（已脱链的 heap-only 元组） |
| ② | `XLOG_HEAP2_VACUUM` | LP_DEAD ⇒ LP_UNUSED + `PageTruncateLinePointerArray` |

**这个分工是硬的，不是风格问题**：`heap_page_prune_execute()` 的 `nowunused`
只接受 heap-only 元组（带存储的根元组的 TID 可能仍被索引引用），而
`heap_xlog_vacuum` 的 redo 又要求目标行指针**已经是** LP_DEAD。想一步到位
必然踩其中一边的不变式。照抄原生的两步，redo 侧一行不用改。

页面变换全部交给内核的 `heap_page_prune_execute()`（**redo 侧调的是同一个
函数**），本模块只负责"选哪些行"与组装记录 —— 延续 T5.3a 的"策略在扩展、
页面变换与 WAL 用内核的"。

##### 不碰 `pd_prune_xid` 与 `PD_PAGE_FULL`（比原生更紧）

原生 `heap_page_prune` 在 leader 侧会更新这两个提示，而 `heap_xlog_prune` 的
redo 明确不管（注释原文 "we don't worry about updating the page's prunability
hints"）—— 即**原生自己就在这两个字段上主副本分叉**，靠 `heap_mask()` 掩掉。
分片表已禁 on-access 剪枝（补丁 0005），这两个提示对我们毫无用处，因此干脆
不动：与 redo 逐字一致。

##### ★ "③ 必须先于 ①"是依赖，不是偏好（实测确认）

中止事务里"插入后连更两次"会留下一条 HOT 链：根 → heap-only(自己也被 HOT
更新) → heap-only。**中间那条**同时满足"死"与"仍挂在 HOT 链上"，而
`heap_page_prune_execute` 不接受这种元组进 `nowunused`。
解法不是特判，而是顺序：`HeapTupleHeaderIsHotUpdated()` 的定义里带
`HEAP_XMAX_INVALID == 0` 前置条件，而那条元组的 xmax 必是同一个中止事务的
号 —— **③ 一清，这个位就自动失效了**。

实测（套件 [9]/[10] 段）：
- 只跑 ①：删 4 条、**推迟 1 条**（链中那条），页上剩 3 个 LP_NORMAL；
- 再跑 ③：消毒 2 条（活行 k1 的中止 xmax + 链中那条的 xmax）；
- 再跑 ①：删 1 条、**零推迟**，页上剩 2 个 LP_NORMAL，两条活行内容完好。

推迟不是失败，是如实计数：`tuples_deferred > 0` ⇒ 本趟不完整 ⇒ 不得据此截断
（与 `pages_skipped` 同为 T5.4 门禁条件）。

##### 三道 fail-closed 守卫

- **带索引即 ERROR**。本函数走的是原生"无索引表"通路，不含索引两阶段
  （T5.3c）；带索引的关系照做会把索引项留成悬空指针。P1 起分片打标表一律
  无索引（`CREATE INDEX`/`REINDEX` 被 `ShardXidUtilityGuard` 拦着），所以正常
  永不触发 —— 但它标出了 T5.3c 的缺口位置。
- **`PD_ALL_VISIBLE` 即 ERROR**。分片表从未跑过 vacuum，这个位无从被置上；
  真置上了说明有别的通路动过这张表，此时删元组还得同步清 vm 位。
- **拿不到 cleanup lock 即跳过并计数**（`ConditionalLockBufferForCleanup`）。
  回收行指针必须持 cleanup lock，否则并发扫描手里的 TID 会指向被复用的槽。
  用条件版本是为了不阻塞在别人的 pin 上；跳过即本趟不完整。

`snapshotConflictHorizon` 同 T5.3a 传 `Invalid`。这里的论证比 ③ 更需要说清：
剪枝是**删元组**，正是 hot standby 冲突解决要防的事 —— 但被删的是**中止插入**
的元组，**对任何快照、任何时刻都不可见**，本就不存在需要杀掉的读者；且分片
副本走 pg_parwal 物理回放，不是 hot standby。

##### T5.3b 验收明细（套件 [8]–[13]，合计 56/0）

| 段 | 断言 | 结果 |
|---|---|---|
| [8] | 构造两活行 + 三种中止形态（中止插入根元组 / 中止更新的 heap-only 后继 / 中止事务内插入连更两次的整条链），7 个 LP_NORMAL、可见 2 行 | PASS ×4 |
| [9] | **只跑 ①：删 4 推迟 1 跳页 0**；推迟那条仍在页上；可见行数不变 | PASS ×3 |
| [10] | ③ 消毒 2 条；**再跑 ①：删 1 推迟 0**；页上只剩 2 个 LP_NORMAL、**0 个 LP_DEAD 残留**；两活行可见且内容完好；幂等第三趟零动作 | PASS ×7 |
| [11] | pg_waldump 在同一趟里同时看到 `PRUNE` 与 `VACUUM` 两条内核标准记录 | PASS ×2 |
| [12] | **immediate 崩溃重启后页面状态保持**；负向：带索引即拒；负向计数守卫 | PASS ×5 |
| [13] | 清场 + 本轮无节点崩溃 | PASS ×2 |

##### 验收脚本自身踩到的三个坑

- **`xid` 类型没有大小比较运算符**：`max(GREATEST(t_xmin, t_xmax))` 直接报
  "No function matches the given name and argument types"，且失败形态又是
  **返回空串**。要先 `::text::bigint`。
- **断言写在了错的那一趟上**：`VACUUM` 记录只在有**根元组**被回收（`ndead > 0`）
  时才产生；第二趟 ① 删的是 heap-only 元组，只走 `nowunused`，压根没有
  `VACUUM` 记录。把取证窗口移到第一趟（同时含根与 heap-only）才两条都有。
- **`pg_waldump` 的记录类型在 `desc:` 里**，不是 `HEAP2/PRUNE` 这种形态；
  grep 要按 `desc: PRUNE` / `desc: VACUUM`。

##### T5.3b **未覆盖**的部分

- **follower 回放未实测**（同 T5.3a）。本套件只验到"leader 的 WAL 里确有内核
  标准 `PRUNE`/`VACUUM` 记录"。留 T5.4。
- **幽灵行复活的端到端陷阱构造不出来**（同 T5.3a）：免查隐式冻结区的解释
  规则全仓尚未实现，套件只能验到"元组物理上确实没了"这一层 —— 而这正是
  §6.4 ① 要求的动作本身。
- **多页与并发**：本套件的夹具只有一页，`pages_skipped` 分支（拿不到 cleanup
  lock）没有被真正触发过，只验了它不误报。

##### 回归

`test_shard_clog_p2` 复跑 **64/0**（基线值），本次两处扩展未波及既有面。

#### T5.3c 实施记要（2026-08-21，套件合计 **83/0**）

**交付**：`ShardVacuumRemoveDeadTuples()` + SQL 包装
`partdist.shard_remove_dead(regclass, bigint)`（9/9 部署）+ 套件 [14]–[18] 段。
同时把 ① 的页面/WAL 通路抽成共享趟 `shard_vacuum_prune_pass(mode)` ——
两个删元组动作只差一个判据，页面变换、两条记录、cleanup lock、守卫全共用。

##### 判据：看 xmax 不看 xmin

设计 §6.4 ② 原文的要害是"**没被删过的老行是活的，零页面动作**"——
xmin 有多老都不构成删除理由。实现上就是：`xmax` 为空 / 中止 / lock-only
一律 `return false`，只有 `xmax < trunc_before` 且 clog COMMITTED 才删。

**`commit_ts < GlobalSafeTs` 这一半没有再取一次 GlobalSafeTs**，而是靠
`trunc_before` 的构造保证：§6.3 的前缀扫描只让 `commit_ts > 0 &&
commit_ts < GlobalSafeTs` 的 COMMITTED 过关，且 GlobalSafeTs 单调不减，
当时成立则永远成立。这条推理只在 `trunc_before` 确实来自
`ShardVacuumComputeTarget` 时有效，所以把**反命题做成守卫**：
`commit_ts == 0` 的 COMMITTED 落在截断点以下 ⇒ ERROR。

##### ★ 这道守卫立刻抓到了一件本来会被漏掉的事

第一次跑夹具就被自己的守卫拦下：`xmax 4 已提交但 commit_ts 为 0`。
查明原因**不是** bug，而是 T5.2 记录里那句话说窄了 —— **未配置 TSO 的遗留
模式下，每一次提交写下的 commit_ts 都是 0**，不只是 P2 历史数据。
语义上自洽（遗留模式没有 GlobalSafeTs，§6.3 会以 `no-safe-ts` 返回"一条都
不清"，永远算不出撞上它的截断点），但**验收夹具必须知道**：本环境 TSO 未
配置，夹具得用 `sclog_wts` 给已提交条目补真时间戳。T5.2 的记录已就地更正。

##### ★ 与 ① 的一处刻意不对称：② 不许推迟

① 对"仍挂在 HOT 链上的 heap-only 元组"是推迟处理（等 ③ 清 xmax 解除
`HEAP_HOT_UPDATED`）。**② 不能照办**：已提交的 xmax 没有任何后续动作会去清
它的 `HEAP_HOT_UPDATED`，一推迟就是**永远推迟**；而"本趟不完整 ⇒ 禁止截断"
又是硬规则 —— **HOT 链会把截断永久钉死**。而分片表连更两次就必然产生这种
元组（无索引表的 UPDATE 一律走 HOT），这不是边角情况。

② 直接回收的安全性来自函数开头那道"无索引"强制：**页外没有任何东西引用
行指针**（没有索引项，链只靠 `t_ctid`，而链走查只服务索引扫描与 EPQ 重读，
后者设计 §4.4 明确不提供）。顺序扫描逐个访问 LP_NORMAL，被摘掉根的
heap-only 元组照样读得到。

套件 [15] 段直接验了这一格：连更两次留下的中间那条（死 + 仍挂链）被一趟收走，
`tuples_deferred = 0`。

##### 索引两阶段：**未做**，且当前不可达也不可测

设计 §6.4 ② 含"索引项清理，两阶段（收集死 TID → 清索引 → 回收行指针）"。
本次**没有实现**，函数对带索引的关系一律 ERROR。理由不是省事：

- **P1 起分片打标表一律无索引** —— `CREATE INDEX` / `REINDEX` 被
  `ShardXidUtilityGuard` 拦着（依据 P1_PRECHECK 结论 D：索引构建用
  `HeapTupleSatisfiesVacuum` 判活，对分片 xid 是数据损坏级误判）。
- 于是索引两阶段**既跑不到、也造不出验收场景**。现在写它 = 往最险的一段
  路上放一段**永远执行不到、因而永远没被验证过的代码**，这比不写更糟。
- 解禁 `CREATE INDEX` 本身是另一件事（它要先解决"索引构建怎么对分片 xid
  判活"），不属 P5 范围。

⇒ **记为 P5 出口前必须回答的缺口**：要么在解禁索引的任务里连带补上两阶段，
要么明确"第一期分片表不支持索引"并把 `ShardXidUtilityGuard` 的禁令从纪律
升格为不变式。当前那道"带索引即 ERROR"就是缺口的守卫位。

##### T5.3c 验收明细（套件 [14]–[18]，合计 83/0）

| 段 | 断言 | 结果 |
|---|---|---|
| [14] | 构造三活行 + 四种死法（已提交删除的根 / 已提交更新的根 + 活的 heap-only 后继 / **中止删除的活行** / 连更两次的整条链），7 个 LP_NORMAL、可见 3 行；补 commit_ts 后中止那条仍是 ABORTED | PASS ×7 |
| [15] | **删 4 条、零推迟、无跳页**；只剩 3 个 LP_NORMAL、0 个 LP_DEAD；三活行可见且内容为 `b2,c,d3`；**中止删除的行 xmax 原样保留**；幂等；PRUNE + VACUUM 两条记录；③ 接手消毒那条中止 xmax | PASS ×11 |
| [16] | 负向：**commit_ts=0 即拒**（且被拒后元组未被删）；负向：带索引即拒；负向计数守卫 | PASS ×6 |
| [17] | immediate 崩溃重启后页面状态保持 | PASS ×3 |
| [18] | 清场 + 本轮无节点崩溃 | PASS ×2 |

##### 三个动作的分工，在 [15] 段里一次看全

同一张表上：② 删掉"已提交删除"的四条 → 剩下的"中止删除"那条 ② 一点不碰 →
③ 接手把它的 xmax 消毒成 0 → 行仍是活的。判据不重不漏。

##### T5.3c 未覆盖的部分（承接 T5.3a/b 的同一份清单）

- **follower 回放未实测**（三次都一样）。留 T5.4 与 CTRL 水位复制一并验。
- **多页与 `pages_skipped` 分支**仍未被真正触发（夹具都是单页）。
- **索引两阶段**如上，独立记为缺口。

##### 回归

`test_shard_clog_p2` 复跑 **64/0**（基线值）。① 的通路被重构进共享趟，
套件 [8]–[13] 段（T5.3b 的 23 项）复跑全绿，重构未改行为。

#### T5.4a 实施记要（2026-08-21，套件合计 **115/0**）

**交付**：`ShardClogTruncate()`（`src/shard_clog.c`）+ `ShardVacuumSweep()`
（`src/shard_vacuum.c`）+ **免查隐式冻结区解释规则**（`src/shard_visibility.c`
的 `shard_xid_state`）+ `ShardXidNextToIssue()` 上界守卫 + SQL 包装
`partdist.shard_vacuum_sweep` / `partdist.shard_clog_truncate`（9/9 部署）
+ 套件 [19]–[23] 段。

##### ★ 截断与"免查区解释规则"是同一件事的两半，必须同时落地

动手前先把这件事想清楚了才动的：**只做截断而不做解释规则，是纯粹的破坏动作**
—— clog 文件没了、水位却还说"要查 clog"，那些 xid 读出来是空洞 = RUNNING =
不可见，**已提交的数据当场全部消失**。反过来只做解释规则不做截断，则水位恒 0、
规则永不生效。两者互为前提，所以放在同一个任务里。

解释规则落在 `shard_xid_state()` 里、**活跃表与终局缓存之后、读 clog 之前**：
`sxid < clog_truncate_before` 一律返回 `SXID_COMMITTED` 且 `commit_ts = 0`。

- 位置选在这里是为了**不给热读路径加锁**：`ShardVacuumGetWatermarks` 要拿
  LWLock，而免查区的 xid 既不可能在活跃表里（§6.3 前缀扫描遇 RUNNING 即停）、
  也早不在任何人手里，所以放在"反正要读文件"的那一步之前最划算。
- `commit_ts = 0` 不是"未知"，而是遗留语义"对一切快照可见"——§4.1 的判据
  `my_ts > 0 && cts >= my_ts` 在 `cts = 0` 时恒不成立，正是免查区要的语义
  （其 commit_ts 按构造必 < GlobalSafeTs ≤ 任何活跃快照的 start_ts）。
- **免查区里 ABORTED 与 COMMITTED 分不出来了，这不是缺陷**：§6.4 的 ① 与 ③
  保证进入免查区之前，中止事务的元组已被删掉、中止的 xmax 已被消毒成 0，
  没有任何元组还会问到那些号。[21] 段的两条注入就是这句话的反证。
- **已知边界**：终局缓存是后端本地、不作废，截断前缓存下的 ABORTED 会在本
  后端内继续报 ABORTED —— 由上一条"没有元组还会问到那些号"覆盖。

##### 顺序铁律的落地点

铁律"数据页、索引、堆全部清完，才许动 clog"落成两句代码：

1. `ShardVacuumSweep()` 按 **③ → ① → ②** 跑完三类动作，**只有整趟干净**
   （`pages_skipped == 0 && tuples_deferred == 0`）才落下
   `shard_vacuum_xid = trunc_before`；
2. `ShardClogTruncate()` 要求 `trunc_before <= shard_vacuum_xid`，否则 ERROR。

即：**"趟完"标记是截断唯一认的凭据，而它只能由一趟完整的页面动作落下。**
把凭据做成一个持久水位（而不是进程内的布尔）也顺带给了 T5.5 两态恢复的抓手。

##### ★ 本步内部的次序同样不能颠倒：先推水位、后删文件

- 先删文件、后推水位：中途崩溃 ⇒ clog 已经没了而水位还说"要查 clog"
  ⇒ **已提交数据当场消失**。
- 先推水位、后删文件：中途崩溃 ⇒ 最坏只是留下一堆没删掉的段文件，
  免查区语义已经生效、答案全对，下一轮顺手清掉。

文件按段整删（段 = 1M 个 xid），只删完全落在 `trunc_before` 以下的整段；
跨界那一段留着，其中免查区部分不再有人问。

##### 新增的上界守卫（动手中发现的一处真缺口）

写门禁时意识到：`trunc_before` 若大于**下一个待发号**，整个已用 xid 空间会
落进免查区 —— 此后每一行新写入的 xmin 都被读成"早已提交、对一切快照可见"，
中止事务的行也一并复活。正常来源（`ShardVacuumComputeTarget` 的结果 + 1）
永远越不了界（它只在已落账条目上前进，遇空洞即停），但这是操作/编排出错时
的重后果，值得一道 fail-closed。
⇒ 新增 `ShardXidNextToIssue()`（槽位取 `next_xid`，无槽回落读水位文件），
`shard_vacuum_begin()` 里拦住 `trunc_before > next_xid`。

##### ★ [21] 段：两条正确性陷阱的注入（三次挂账终于能验了）

T5.3a/b/c 的记要里连着三次写"端到端陷阱构造不出来，因为免查区解释规则尚未
实现"。规则落地后，两条陷阱当场可复现 —— 用 `shard_vacuum_set_watermarks`
**绕过门禁**直接推水位（注入测试的意义正在于证明门禁拦的那件事确实是灾难）：

| 陷阱 | 注入 | 实测结果 |
|---|---|---|
| **①不删中止 xmin 的元组** | 一活行 + 一条中止插入的行，不 sweep 直接推水位 | 可见行数 1 → **2，幽灵行复活** |
| **③不消毒中止的 xmax** | 一活行、其 xmax 是中止的删除，不 sweep 直接推水位 | 可见行数 1 → **0，活行被判死** |

对照：[20] 段走正常 `sweep → truncate` 路径，两种垃圾都被清干净，活行
一条不少、内容完好。

##### T5.4a 验收明细（套件 [19]–[23]，合计 115/0）

| 段 | 断言 | 结果 |
|---|---|---|
| [19] | 构造（2 活行 + 已提交删除 + 中止插入 + 中止删除）；初始水位 0/0；**★ 页未清完即拒截断**（核心验收项）且被拦后水位未动；半程 sweep 干净收尾并落标记；**★ 趟标记落后于目标即拒**；trunc_before 超过下一个待发号即拒；负向计数守卫 | PASS ×9 |
| [20] | 全程 sweep 干净收尾、两趟总账 3 条；标记落到位；截断返回删段数 0（跨界那段留着）且段文件仍在；水位推进；可见行数与内容正确；**★ 免查区生效**（把已提交号的 clog 槽抹成 RUNNING，行照样可见）；截断幂等 | PASS ×12 |
| [21] | **★ 两条陷阱注入**（幽灵行复活 / 活行被判死）+ 两张注入表的前置状态 | PASS ×6 |
| [22] | immediate 崩溃重启后水位、可见行数、内容全部保持 | PASS ×4 |
| [23] | 清场 + 本轮无节点崩溃 | PASS ×2 |

##### 验收脚本又踩到的两个坑（其中一条是**假通过**）

- **`psql -c "A; BEGIN; B; ROLLBACK;"` 会把 A 一起回滚。** `-c` 把整串当
  **一个**事务发出去，里面的 `BEGIN` 是空操作、`ROLLBACK` 连前面的 INSERT
  一起撤。实测断言"注入前可见 1 行"读到 0；更糟的是陷阱③ 那条
  "注入后 0 行"**因此假通过**了 —— 期望值恰好是 0。改用 heredoc 逐条送。
  （前面 [8]/[14] 段用的就是 heredoc，这次图省事写了 `-c`。）
- **`swept` 布尔在 `psql -A` 下输出 `true` 不是 `t`**（`||` 走的是
  boolean→text 的输出函数）。

##### 一次未复现的偶发（如实记录）

改完夹具后有两轮跑出 **114/1**（总数仍是 115，即恰好一条 FAIL），且当时打印
的 [19]–[23] 段全绿 —— 说明失败落在 [0]–[18] 段，但没有截获到是哪一条。
此后连续 **6 轮 115/0**（含专门为此追加的 3 轮）。最可能的嫌疑是 WAL 取证那
三条（`pg_waldump` 取 `-s/-e` 区间，赶上检查点回收段文件就取不到），但
**未证实**。记在此处，后续若复现按此线索先查。

##### T5.4a 未覆盖的部分

- **整段删除分支未被覆盖**：段容量是 1M 个 xid，夹具规模够不到，
  只验到了"不足一整段时不删、跨界那段留着"这一半。真正跑满一段需要
  百万级事务，留作后续压测项。
- **§6.7 的 CTRL 水位复制与 follower 跨节点回放仍未做** —— 归 T5.4b。
  这也意味着**目前 follower 侧没有 `clog_truncate_before`**，免查区只在
  leader 上生效；切主后新主的水位从自己的水位文件读（本地持久，T5.1 已验），
  但**它与老主的水位是否一致尚无协议保证** —— 这正是 T5.4b 要解决的。

##### 回归

`test_shard_clog_p2` 复跑 **64/0**（基线值）。这一轮特别值得跑它：免查区
解释规则加在 `shard_xid_state()` 这条**热读路径**上，而该套件的可见性断言
最密集。

#### T5.4b-1 实施记要（2026-08-21，新套件 `test_shard_vacuum_replay_p5.sh` **43/0**）

**这是 T5.3a / T5.3b / T5.3c 三份记要里连着挂了三次账的那一项。** §6.7 要求
vacuum 只在 leader 执行、其页面修改随 pg_parwal 流被 follower 逐字节回放；
前面三个任务只验到"leader 的 WAL 里确有内核标准的 `FREEZE_PAGE` / `PRUNE` /
`VACUUM` 记录"，跨节点这一半一直没验。

**夹具**照抄 `test_shard_pagecmp_p1.sh` 阶段 2 的配方（1 分片分布表 + raft 组
+ 两个 follower 壳表 + locmap + 白名单打标），把工作负载换成"制造三类垃圾 →
`shard_vacuum_sweep` → `shard_clog_truncate`"。

##### ★ 一条防假通过的断言：follower 的文件必须真的变了

**只比对"leader == follower"是不够的** —— 若 vacuum 记录压根没进流，两边都
停在 vacuum 之前的状态，比对照样报 IDENTICAL。所以先取 vacuum 前后 follower
主堆文件的 md5，**断言它变了**，再做逐字节比对。

##### 结果

| 断言 | 结果 |
|---|---|
| sweep 在复制分片上：消毒 1、删中止 1、删死 8、零跳页零推迟 | PASS |
| vacuum 产生了新的 parwal 记录（plsn 从 71 推进到 75） | PASS |
| 两个 follower 都追平 | PASS |
| **★ 两个 follower 的主堆文件确实变了** | PASS |
| **★ 主堆 leader vs follower1 / follower2 逐字节 `IDENTICAL_OUTSIDE_HOLE`** | PASS |
| leader 侧 vacuum 后仍可见 52 行、页上 LP_NORMAL 减少 | PASS |

⇒ **`FREEZE_PAGE` / `PRUNE` / `VACUUM` 三条记录跨节点回放逐字节一致**，
T5.3a/b/c 的"策略在扩展、页面变换与 WAL 由内核发"这条路线到此闭环。
连跑 2 轮 43/0。

##### ★ 途中查明的一件事：分片表的 TOAST 空间**永远回收不了**，且与索引两阶段是同一个缺口

写夹具时才意识到需要确认：分片表的 TOAST 元组带的是**分片 xid 还是原生 xid**。
查明是**分片 xid** —— `ShardXidRelidLookup()` 对 `RELKIND_TOASTVALUE` 按
`pg_toast_<owner>` 命名解出属主，属主在白名单里就返回属主的 shard oid，
所以 TOAST 元组同样被打标。于是：

- TOAST 关系里同样会积累三类垃圾（`heap_delete` 会顺带给 TOAST 元组盖 xmax）；
- 而 TOAST 关系**天生带一个 btree 索引**，撞上 T5.3c 那道"带索引即 ERROR"；
- ⇒ **分片表的 TOAST 空间在当前实现下永远不会被回收。**

不构成正确性问题（TOAST 读走 `SnapshotToast`，`HeapTupleSatisfiesToast` 根本
不查 clog、不分叉，0006 已论证过），但是实打实的**空间泄漏**。

**这条把 T5.3c 的索引两阶段缺口从"理论上够不到"变成"实际拦住了一件必须做的
事"** —— 之前的判断是"P1 禁索引 ⇒ 索引两阶段不可达也不可测"，现在多了一个
可达且必须的用例：TOAST。P5 出口前的那个问题因此更尖锐了：要么补上索引两阶段
（TOAST 用得上，且它的索引是内核自建、不受 `CREATE INDEX` 禁令影响），
要么明确接受"分片表 TOAST 空间不回收"。**本套件为规避它，工作负载的值一律
不进 TOAST。**

##### 验收脚本踩到的三个坑

- **`partition_lsn` 是分区流内的逻辑位置（记录序），不是字节 LSN。** 本夹具
  的写入量下实测只有 70 上下，照抄 pagecmp 的 `> 100` 会误报"取不到值"。
- **空 TOAST 关系的比对结果不是 `IDENTICAL_OUTSIDE_HOLE`**：空堆报
  `IDENTICAL_EMPTY`、空 btree 元页两侧各自本地创建故报
  `IDENTICAL_EXCEPT_PDLSN`。两者都不是分歧，但**没有松成"含 IDENTICAL 就算
  过"**，而是按 role 分开：主堆（本套件的正题）必须严格逐字节，未参与本轮
  vacuum 的 TOAST 关系列举允许值。
- **`wait_caught_up` 收到空 target 时，`-ge ""` 是语法错误，断言会变成假通过。**
  加了空值 fail-fast（这正是上一条 plsn 取值失败暴露出来的连带风险）。

##### 未覆盖

- ~~**CTRL 水位复制仍未做**（T5.4b-2）~~ —— **已于同日完成**，见 T5.4b-2 实施记要。原文留档：follower 侧曾**没有**
  `clog_truncate_before`，免查区只在 leader 上生效。切主后新主从自己的水位
  文件读（本地持久，T5.1 已验），但它与老主的水位是否一致**尚无协议保证**。
- 本套件不做切主，也不做崩溃注入 —— 只验"正常路径下 vacuum 记录跨节点逐字节
  一致"这一件事。

#### T5.4b-2 实施记要（2026-08-21，`test_shard_vacuum_replay_p5.sh` 升为 **48/0**）

**交付**：`FREEZE_UPDATE` 载荷的向后兼容扩展 + 生产端
`ShardVacuumEmitWatermarkCtrl()`（`src/wal/shard_fileset.c`）+ 应用端解析/发布/
落盘（`shard_replay.c` / `replay_worker.c`）+ 套件里 4 条水位断言。

##### 复用 `FREEZE_UPDATE` 换语义，不新增 opcode

设计原文如此，而语义上也恰好贴切：**`clog_truncate_before` 就是分片 xid 宇宙
里的隐式 freeze 点**，与 `relfrozenxid` 同属「leader 的冻结账目」。

兼容做法：把原来那个「显式补齐、恒为 0」的 `reserved` 字段改作 `flags`。

| | 旧记录（D2 冻结账目） | 新记录（vacuum 水位） |
|---|---|---|
| `nrels` | ≥ 1 | 0 |
| `flags` | 0 | `PARTWAL_FREEZE_HAS_VACUUM_WM` |
| 尾块 | 无 | 8 字节 `{clog_truncate_before, shard_vacuum_xid}` |

- `flags == 0` ⇒ 长度校验与旧算法**逐字节等价**，R1/D2 时代写下的记录继续有效；
- `nrels == 0` **仅当**带水位块才允许 —— 两本账各发各的，但两者皆空的记录
  没有意义，仍旧拒收；
- **未知 flags 位一律 ERROR**，不「按旧语义蒙混过去」（与既有的「未知 CTRL
  opcode 拒绝静默跳过」同一条纪律）。

##### follower 侧落在哪：与 leader 同一个存储位

落进 follower 自己的 `pg_shard_xid/<local_oid>`（经 `ShardVacuumSetWatermarks`）
—— **就是 leader 用的那个文件**。好处是升主时 `shard_xid_slot_attach()` 的建槽
路径原样读得到，**不需要任何额外的交接协议**。

搬 leader 的原值同样是**真话**（与 D2 搬 `relfrozenxid` 同款论证）：
「`xid < clog_truncate_before` 的分片事务都已提交且早于 GlobalSafeTs」这句话，
在页面逐字节一致的副本上同样成立。

##### 发射的两道门与「尽力而为」纪律

- **只对本节点维护着 fileset 的分区发射**（`LoadShardFileSet` 探测）。这道门
  同时挡掉两类情形：非复制的本地打标表（发了只会凭空造出 `pg_parwal` 目录 ——
  P5 本地套件里的 p5a…p5g 全靠它），以及 follower 侧（那里天然没有持久化
  fileset）。
- **失败只 WARNING，绝不把调用方的 vacuum 带下水**（与 D2 冻结账目同一条纪律，
  那里是被 FPI 洪水饿死心跳、领导权移走的实测教训）。后果是 follower 的免查区
  暂时落后 —— **安全方向**（follower 只会少信一点，不会多信）；且水位发的是
  **绝对值**不是增量，下一轮截断自然补上。

##### 验收（套件新增 4 条）

| 断言 | 结果 |
|---|---|
| vacuum 前两个 follower 水位都是 `0/0`（对照组，防假通过） | PASS |
| leader 截断后水位 `7/7` | PASS |
| **★ follower1 收到并落盘 `7/7`** | PASS |
| **★ follower2 收到并落盘 `7/7`** | PASS |

##### ★ 途中查明的一件大事：follower 侧的**分片 clog 尚未由流重建**

设计 §5.3 明写「落账顺序锁定在分片流的 plsn 序上 ⇒ **每个副本重放同一个流得到
同一本账**（确定性、副本一致）」。实测代码不是这样：follower 回放 `TXN_MARKER`
时写的是**增强型 clog（gclog，按 gxid 索引）**，而 `pg_shard_clog/<oid>`（按
(shard, 分片xid) 索引）**在 follower 上根本不存在** —— 全仓写
`ShardClogSetVerdict` 的只有 leader 自己的提交路径、0007 的原生 WAL redo 钩子、
以及 DTX 决议路径，没有一条跑在 follower 的流回放里。

后果：**今天把一个 follower 升主，它的分片 clog 是空的** —— 每个分片 xid 读出来
都是空洞 = RUNNING = 不可见，除非那个 xid 恰好落在免查区里。同理，follower 也
没有分片 xid 的**发号水位**，升主后会从 3 号重新发，与堆里已有的号相撞。

**这不是 T5.4b-2 引入的，也不属 P5 范围** —— 设计的阶段表把「切主/恢复全链路
联测」整个划给 **P6**。但它把本任务的定位说清楚了：**两水位的复制是升主所需
的一部分，不是全部**；P6 至少还欠「分片 clog 由流重建」和「分片发号水位交接」
两件事。已记入 §4 未决核实点 U-P5-1。

##### 回归

- `test_shard_clog_p2` **64/0**（基线值）；本地 P5 套件 **115/0**；
  跨节点套件 **48/0**（连跑 2 轮）。
- **`test_freeze_sync_d2.sh` 22/0** —— 这一条是专门为本次格式改动跑的：D2 是
  `FREEZE_UPDATE` 通道的既有套件，它的记录 `flags == 0`、载荷是旧形状，
  跑通即证明向后兼容不是纸面推理。
  **注意该套件的 `CONTAINER` 默认值是 `pg-citus-replay-container`**（它属于
  replay 环境），在本环境要 `CONTAINER=pg-citus-tx2-container` 覆盖后才跑得
  起来 —— 第一次直接跑它报「group0 未收敛」，查出来是连错了容器，不是回归。

#### T5.5 实施记要（2026-08-21，本地 P5 套件升为 **139/0**）

**交付**：`ShardVacuumRecover()` + SQL 包装 `partdist.shard_vacuum_recover(OID)`
（9/9 部署）+ 套件 [24]–[27] 段 + **一处自身缺陷的修复**（见下）。

##### ★★ 动手前查出并修掉的一处真缺陷：标记先于它所断言的事持久

T5.4a 的 `ShardVacuumSweep` 在三类动作跑完后调 `ShardVacuumSetWatermarks` 落
「趟完」标记。问题在两者的持久化路径不同：

- 三类动作发的是**普通 WAL 记录**，只进 WAL 缓冲区，还没刷盘；
- `ShardVacuumSetWatermarks` 写的是水位文件，**当场 fsync**。

两者之间崩溃一次，结果是：标记说「页面已清到 trunc_before」，而那些页面改动
随未刷的 WAL 一起没了。恢复之后 `ShardClogTruncate` 认这个标记、照常截断
—— **中止事务的幽灵行当场复活**。

**这正是顺序铁律要防的那件事，却发生在铁律自己的实现里。**

修复：落标记之前 `XLogFlush(GetXLogInsertRecPtr())`，把本趟发出的全部记录刷
到盘上（顺带多刷一点别的后端的，无害）。

这条与「先推水位、后删文件」是同一族次序要求，值得单独写下来：
> **任何一个「已经做完」的持久断言，都不能先于它所断言的那件事持久。**

P5 至此已有三处同族次序：①页面在前、clog 截断在后（顺序铁律）；
②推水位在前、删段文件在后；③页面 WAL 刷盘在前、落「趟完」标记在后。

##### 两态恢复本身

`ShardVacuumRecover(shard)` 读两个水位后只有两条路（不变式
`clog_truncate_before <= shard_vacuum_xid` 由落盘出口统一守，不存在第三种）：

| 状态 | 判据 | 动作 |
|---|---|---|
| 一：无未完成的趟 | `tb == vx` | **什么都不做**。趟中崩溃落在这一格 —— 整趟的页面动作一条标记都没留下，下一轮整趟重来即可 |
| 二：趟完未截断 | `tb < vx` | **只补做截断**，绝不重跑页面趟 |

状态一之所以「重来即可」，靠的是三类动作各自幂等：已消毒的 xmax 是 0、
已删的行指针不再是 LP_NORMAL，再跑一遍全是空操作。这一点套件里逐条验过。

##### ★ 状态一的**确定性**注入：用游标 pin 住页面

「趟中崩溃」不好造 —— 没有故障注入点就没法停在页面循环中间。但**「趟不完整」
这件事本身**可以确定性地造出来：在**同一个 session** 里开一个游标并 FETCH 过
一行，第 0 页就被本后端多钉了一个 pin；而回收行指针要的 cleanup lock 要求
`refcount == 1`，于是 ① 与 ② 拿不到、跳过该页，`swept = false`。

好处有三：无时序竞态；直接验到「不完整就不落标记 ⇒ 截断仍被拦」这条主链；
**顺带覆盖了 T5.3b/c 记要里那条「`pages_skipped` 分支从未被真正触发」**。

（③ 用的是普通排他缓冲区锁、不要 cleanup lock，所以它照常跑完 —— 这也解释了
为什么注入出来的跳页数是 2 而不是 3。）

**仍未覆盖**：真正停在页面循环中间的崩溃。那需要一个故障注入点（GUC 或
测试专用钩子），本次没有为它往生产路径里加代码；现有注入覆盖的是同一条主链上
「趟不完整 ⇒ 不落标记 ⇒ 截断被拦 ⇒ 重来」的全部环节。

##### T5.5 验收明细（套件 [24]–[27]，本地套件合计 139/0）

| 段 | 断言 | 结果 |
|---|---|---|
| [24] | **★ 有页被 pin 住时 sweep 不完整**；**不完整的趟不落标记**（水位仍 0/0）；负向：不完整之后仍拒截断；游标释放后整趟重来干净收尾、标记落位、截断成功、可见性正确 | PASS ×10 |
| [25] | sweep 干净收尾但不截断 ⇒ **★ 处于状态二 `0/7`**；immediate 崩溃后**状态二跨崩溃保持**；**★ 恢复只补做截断**（返回 `truncated`）；水位、可见行数、内容全对；**恢复幂等**（再做一次是 `nothing`） | PASS ×10 |
| [26] | 已清干净的表再跑整趟：零动作、不动水位、可见性不变；`tb == vx` 时恢复无事可做 | PASS ×4 |
| [27] | 清场 + 本轮无节点崩溃 | PASS ×2 |

##### 验收脚本踩到的一个坑

`psql -At` 会把 `BEGIN` / `DECLARE CURSOR` / `COMMIT` 的**命令标签也打到
stdout**，所以 `| tail -1` 取到的是 `COMMIT` 而不是 sweep 的结果。按结果的
形状挑行（`grep -E '^(true|false)/'`）才对。失败形态又是「取不到值」，
靠 `check()` 的空值守卫才没变成静默通过。

##### 回归

本地 P5 套件 **139/0**（连跑 2 轮）；跨节点套件 **48/0**；
`test_shard_clog_p2` **64/0**（基线值）。

#### T5.6 实施记要（2026-08-21，本地 P5 套件升为 **163/0**）

**交付**：两个 GUC（`pg_partdist.shard_vacuum_max_age` / `shard_xid_stop_age`）
+ 发号路径上的两阶段护栏 `shard_xid_wraparound_gate()` + 观测点
`partdist.shard_xid_age(OID)`（9/9 部署）+ 套件 [27]–[30] 段
+ **两处对既有代码的更正**（见下）。

##### 龄的基点：必须是 `clog_truncate_before`

`age = next_xid - clog_truncate_before`。设计 §7 特意点名**不能**用
`shard_vacuum_xid`：歧义边界挂在免查隐式冻结区的解释规则上，而那条规则读的
正是 `clog_truncate_before`；「趟完未截断」的窗口里 `shard_vacuum_xid` 跑在
前面，拿它算龄会把紧迫度算**小** —— 方向不安全。平时两者相等。

套件 [27] 段直接坐进那个窗口验：跑完 sweep（`0/6`，状态二）之后断言
**龄没有因为 sweep 而变小**。选错基点这条断言立刻翻红。

##### 阶段 2：拒发新号 = 该分片进只读

落在发号路径（`shard_xid_allocate` 取到槽位之后、发号之前）。三条性质都验了：

| 性质 | 验法 | 结果 |
|---|---|---|
| **分片粒度，不殃及节点/集群** | 同节点另一张龄还小的分片表照写不误 | PASS |
| **只读而非全禁** | 读该表仍然可以 | PASS |
| **vacuum 自身仍可运行** | 停发状态下 `shard_vacuum_sweep` 照跑 | PASS |

最后一条不是锦上添花而是**必要条件**：vacuum 不领分片 xid，所以不会被自己的
护栏挡住；否则「要解锁得推进截断点，而推进截断点又被停发挡住」就是死锁。

**解锁路径也验了**：不是调阈值，而是 sweep + 截断把 `clog_truncate_before`
推上去 ⇒ 龄归小 ⇒ 相位回 0 ⇒ 写入恢复。

错误信息里带着设计 §7 的「实话两条」，因为那正是操作者当场要知道的：
拒发只是止血，解锁必须解决前缀阻挡者；**超龄 RUNNING 可按策略强杀，
PREPARED 未决绝不允许单方中止，只能走协调组决议**。

##### 阶段 1：只到「信号」为止，**自动启动器没有做**

设计 §7 说阶段 1 是「到龄强制启动分片防回卷 vacuum，**无视常规 vacuum 开关**」。
本次交付的是到龄判定 + 每后端每分片一次的 WARNING + `shard_xid_age()` 观测点。
**没有做自动启动器。**

而且要把话说准：「无视常规 vacuum 开关」这句在今天是**空的** —— 分片 vacuum
根本没有开关，它只能被显式调用（`partdist.shard_vacuum_sweep`）。原生
autovacuum 对分片表是硬禁的（`sv_satisfies_vacuum` 撞见 autovacuum worker 直接
ERROR）。所以没有开关可以「无视」，也没有守护进程可以「强制启动」。
⇒ **启动器归 T5.7 / P6**：要么做一个 bgworker 轮询 `shard_xid_age()`，
要么把它挂进既有的 partdist 维护通道。缺口位置就是这条 WARNING。

##### ★ 更正一：`SHARD_XID_SANITY_MAX` 的举证是错的

T5.1 的记要与代码注释都写：这道守卫「拦得住把 ASCII 文本当整数读出来的情况
—— 实测 `'\x'` 序列被读成 858814556，正落在该区间之上」。

**算一下就知道不对：858814556 < 2^30 = 1073741824。** 这道守卫**拦不住它
自己引用的那个值**。绝对阈值天生分不清「损坏值 8.6 亿」和「真跑了 8.6 亿笔
事务的分片」—— 这是它的固有局限，不是参数没调好。T5.1 的记要已就地更正。

##### ★ 更正二：2^30 会抢在回卷护栏前面触发

更实际的问题：一个正常运转到 ~1.07e9 笔事务的分片，重启读水位时会先撞上
2^30 的「文件损坏」ERROR —— 一条**完全误导**的错误信息（它没坏，它是该做
vacuum 了）。
⇒ 阈值上抬到 **2^31**，让 T5.6 的停发护栏先说话（它给的信息才是对的：
该分片进只读，去解决前缀阻挡者）。这道检查保留的意义收窄为：
「越过停发线还能读到的水位，要么文件损坏、要么护栏本身失效，两种都 fail-closed」。

顺带记下一条**不能加**的检查：`vacuum_xid <= alloc_wm` 看着是个真不变式
（不可能清理到从未发出过的号，T5.4a 已在内存侧加了同款守卫），但
**T5.4b-2 起 follower 侧的水位文件正是 `alloc_wm = 0` 而
`trunc_before/vacuum_xid` 非 0**（水位由 CTRL 复制过来，发号从未在本节点发生）
—— 加上去会把每一个 follower 判成损坏。

##### ★ 一件必须说清的事：**回卷本身没有实现**

设计 §7 写的是 `xid_age = (next_xid − clog_truncate_before) mod 2^32`。
而全仓比较分片 xid 用的**全是普通 `<` / `>=`**（`ShardVacuumComputeTarget`
的扫描、`shard_vacuum_status` 的区间判断、`shard_xid_state` 的免查区判断、
发号器的上限判断），没有一处 `TransactionIdPrecedes` 式的模运算。
也就是说：**分片 xid 目前是线性计数器，不支持回卷。**

这不是疏漏，而是分工：**阶段 2 的停发线正是让线性假设保持成立的那道闸**
（P1 时代就有的 `SHARD_XID_HARD_LIMIT = 0xFFFF0000` 是它的粗糙前身，现在退居
最后一道兜底）。真要支持回卷，得把上述四处比较全部改成模运算，那是独立一项。

##### T5.6 验收明细（套件 [27]–[30]，本地套件合计 163/0）

| 段 | 断言 | 结果 |
|---|---|---|
| [27] | 默认阈值正确；未截断时龄 = next_xid；**★ 状态二下龄未因 sweep 变小**（基点是 tb）；截断后龄变小；20 个事务后龄回升 | PASS ×8 |
| [28] | 阈值可调；**相位升为 1**；**★ 到龄时发号发 WARNING**；阶段 1 不拦写入 | PASS ×4 |
| [29] | 相位升为 2；**★ 停发线以上拒发新号**；只读仍可读；**★ 分片粒度**（另一张龄小的分片表照写不误）；**★ 停发状态下 vacuum 自身仍可运行**；**★ 推进截断点后相位降回 0、写入恢复**；阈值复位 | PASS ×9 |
| [30] | 清场 + 本轮无节点崩溃 | PASS ×2 |

##### 验收脚本踩到的两个坑

- **分片 xid 是「每事务一个」，不是每行一个。** 夹具原本用
  `INSERT ... SELECT generate_series(1,20)` 想把龄推到 20 —— 那是**一个**事务、
  **一个**号。要 20 个号就得开 20 个事务（heredoc 里每条语句各自提交）。
  这条在算龄/算水位的用例里很容易想当然。
- **布尔的两种渲染**：裸列 `SELECT swept` 在 `-At` 下是 `t`，而
  `swept||'/'||...` 走 boolean→text 输出函数、给的是 `true`。同一份脚本里
  两种写法都有，期望值必须跟着变（T5.4a 已踩过一次，这次是反向）。

##### 回归

本地 P5 套件 **163/0**（连跑 2 轮）；跨节点套件 **48/0**；
`test_shard_clog_p2` **64/0**、`test_shard_xid_p1` **45/0**（均为基线值 ——
后者尤其该跑，本次动的正是它覆盖的发号路径与水位文件校验）。

#### T5.7 P5 出口回归（2026-08-21）

##### 一、回归结果：**521 / FAIL=0**

按 P4 出口确立的口径（本环境 `CONTAINER=pg-citus-tx2-container` 的套件），
串行跑、每套之间净场；`tso_si_p3` 垫底并在它之前复位 TSO 纪元
（它要 TSO 配好且纪元纯净，而 `shard_xid_p1` 的负向用例要 TSO 没配 —— 互斥）。

| 套件 | 本次 | P4 出口基线 |
|---|---|---|
| test_shard_clog_p2 | **64/0** | 64/0 |
| test_shard_xid_p1 | **45/0** | 45/0 |
| test_shard_gating_p4 | **20/0** | 20/0 |
| test_shard_pagecmp_p1 | **49/0** | 49/0 |
| test_dtx_convergence_p4 | **45/0** | 45/0 |
| test_dtx_tso_p4 | **49/0**（复跑） | 49/0 |
| test_tso_si_p3 | **38/0** | 38/0 |
| **7 套小计** | **310 / 0** | **310 / 0** |
| test_shard_vacuum_p5（新） | **163/0** | — |
| test_shard_vacuum_replay_p5（新） | **48/0** | — |
| **合计** | **521 / 0** | — |

**`dtx_tso_p4` 首跑 48/1，红的那条是它自己的净场前置**：
「残留 raft 组已重置（清 3 个，剩 1）」。查明是**该套件自身净场步骤的一个竞态**
—— 它按 5432→5440 顺序逐节点 `pg_raft_group_reset()`，而尚未复位的节点会把组
心跳回已复位的节点上。做两轮复位（复位前实测 5434/5437/5438 各残 1 个组，
两轮后全 0）再跑，**49/0，基线恢复**。
**这不是 P5 的回归，是既有的环境隔离缺陷**（P4 出口已记的那一条）在新的套件
组合下换了个位置出现。

##### 二、设计 §6/§7 的验收项逐条对照

| 设计要求 | 落点 | 证据 |
|---|---|---|
| §6.4 ① 删中止 xmin 的元组 | T5.3b | 套件 [8]–[13]，含"③ 必须先于 ①"的顺序依赖实测 |
| §6.4 ② 删已提交删除的死元组（**看 xmax 不看 xmin**） | T5.3c | 套件 [14]–[18]，含"中止删除的行一点不碰" |
| §6.4 ③ xmax 消毒 | T5.3a | 套件 [0]–[7] |
| **§6.4 三类动作的注入测试（幽灵行/活行误删）** | T5.4a | 套件 [21]：跳过清理直接推水位 ⇒ 可见 1 行变 2 行（幽灵复活）/ 变 0 行（活行被判死）；对照组走正常路径两者皆正确 |
| §6.4 顺序铁律 | T5.4a | 套件 [19]：**页未清完即拒截断**（核心验收项）+ 趟标记落后即拒 |
| §6.5 两态恢复 | T5.5 | 套件 [24]–[26]：状态一用游标 pin 确定性注入；状态二用 immediate 崩溃 |
| §6.7 复制与持久化落地 | T5.4b-1/2 | 跨节点套件：主堆逐字节 `IDENTICAL_OUTSIDE_HOLE` + 两水位随 CTRL 到达 follower |
| §7 回卷两阶段护栏 | T5.6 | 套件 [27]–[29]：基点校验、阶段 1 WARNING、阶段 2 分片粒度停发、解锁靠推进截断点 |

##### 三、★ R-P4-20 的出口复评（P4 出口清单末条要求）

P4 出口时用户裁定携带该缺陷，并要求**进 P5 前重新评估**。现在给出口径。

**新增的实证**——全部节点日志按日统计 PANIC：

| 日期 | PANIC | 其中 `invalid max offset number` |
|---|---|---|
| 2026-08-14 | 32 | 32 |
| 2026-08-15 | 3 | 3 |
| 2026-08-17 | 14 | 14 |
| 2026-08-18 | 9 | 9 |
| **2026-08-19 / 20 / 21** | **0** | **0** |

两点值得记：**历史上每一条 PANIC 都是 R-P4-20**（无第二种）；而 **P5 的全部
实现与验收（2026-08-21）零 PANIC**。

**这一点尤其值得说**：T5.3 的方案评估里预判过「此处新增大量页面写入会**放大
它的暴露面**」——P5 确实往回放通路里灌进了三种全新的页面记录
（`FREEZE_PAGE` / `PRUNE` / `VACUUM`），T5.4b-1 的跨节点套件正是专门跑这条路的，
**预判的放大器装上了，没有响**。

**但样本增量有限，不足以宣告根除**：R-P4-20 历史上出现在**崩溃矩阵**条件下
（杀 leader、切主），而 P5 的跨节点套件**不做切主也不做崩溃注入**；今天真正
命中那类条件的只有 `dtx_tso_p4` 的 2 轮。P4 出口时的口径是"最近约 48 轮一次
未现"，今天只把分母加了 2。

⇒ **复评结论：根因仍未闭；P5 未使其恶化，且预判的放大路径实测未触发。
是否继续携带进 P6 是用户裁定项** —— P6 的正题恰是「切主/恢复全链路联测」，
那才是它的原生栖息地。

##### 四、出口时明确挂账的六项（都不是本轮回归能盖掉的）

| 编号 | 事项 | 性质 | 归属 |
|---|---|---|---|
| ① | ~~**索引两阶段未实现** ⇒ **分片表 TOAST 空间永远不回收**~~ —— **2026-08-21 T5.8 已补做并关闭**（TOAST chunk 实测 32→16，索引项同步，活行 TOAST 值 md5 不变；跨节点 btree 记录逐字节回放）。**收窄为一条更小的挂账：不产生 LP_REDIRECT** —— 只影响"死根元组 + 链上还有活后继"那一格，与"解禁用户索引"同一件事，撞见即 fail-closed ERROR | 空间泄漏，非正确性 | 已关闭；LP_REDIRECT 随"解禁 CREATE INDEX"一并做 |
| ② | `commit_ts = 0` 的已提交槽会挡住前缀 | 已知边界；**T5.3c 更正**：这是"TSO 未配置的遗留模式"的**常态**而非历史残留，语义自洽（遗留模式压根没有 GlobalSafeTs，§6.3 会以 `no-safe-ts` 返回"一条都不清"） | 无需迁移；配了 TSO 就不产生 |
| ③ | 阶段 1 的**自动启动器没有做** | 交付到"信号"为止；且"无视常规 vacuum 开关"这句今天是空的（分片 vacuum 根本没有开关） | T5.7 之后 / P6 |
| ④ | **回卷本身没有实现**（全仓比较分片 xid 用的都是普通 `<`/`>=`，无模运算） | 不是疏漏而是分工：阶段 2 的停发线正是让线性假设成立的那道闸 | 独立一项 |
| ⑤ | **U-P5-1**：follower 侧分片 clog 未由流重建；连带无分片发号水位 | 今天升主会"全表不可见 + 从 3 号重发号" | **P6**（设计阶段表把切主/恢复全链路整个划给 P6） |
| ⑦ | **分片 vacuum 缺尾部截断**（不把文件尾部还给操作系统） | 空间，非正确性；V4 复评（§3.6）指出这是 VACUUM FULL 在 P5 之后唯一还能提供的东西，而它不需要 rewriteheap，复制面走现成的 `XLOG_SMGR_TRUNCATE` | 独立一项，推荐优先于"解禁 VACUUM FULL" |
| ⑥ | 两处覆盖缺口：clog **整段删除**分支（段容量 1M xid，夹具够不到）；**真正停在页面循环中间**的崩溃（需要往生产路径加故障注入点，本次没加） | 覆盖度，非已知缺陷 | 后续压测 / 故障注入专项 |

##### 五、本轮回归自身的一处观察

跑前残表 9 张、跑完 10 张 —— **各 worker 的夹具残表在缓慢累积**（P4 出口记的
"五层净场"第 5 层）。本次净场只**统计不删**，正是为了让它暴露出来。数量小、
未影响任何断言，但**趋势是单调增的**，长期会重演"九节点残留 47 张夹具表"。
记在此处：净场脚本应当增加"按夹具命名前缀清残表"的一层，属测试体系的活儿。

### 3.5 T5.8：索引两阶段（2026-08-21，P5 出口后补做）

设计 §6.4 ② 原文含「索引项清理，两阶段（收集死 TID → 清索引 → 回收行指针）」。
P5 出口时它是**挂账第 ①** 项，理由是「P1 禁索引 ⇒ 不可达也不可测」。
**T5.4b-1 把这个理由推翻了**：分片表的 **TOAST 关系**天生带一个 btree 索引，
而 TOAST 元组同样被打分片 xid、同样积累三类垃圾 —— 索引两阶段没做的时候，
**分片表的 TOAST 空间永远不会被回收**。本任务据此补做。

##### 三阶段的落地

| 阶段 | 做什么 | 记录 |
|---|---|---|
| 一 | 扫页判死：根元组 ⇒ LP_DEAD，heap-only ⇒ LP_UNUSED；死 TID 按扫描序进批 | `XLOG_HEAP2_PRUNE` |
| 二 | 逐个索引 `index_bulk_delete` + `index_vacuum_cleanup`，回调对死名单做二分 | 由索引 AM 自己发（btree vacuum 记录） |
| 三 | 按块回收行指针 LP_DEAD ⇒ LP_UNUSED + 截行指针数组 | `XLOG_HEAP2_VACUUM` |

**次序不能颠倒**：先回收行指针、后清索引的话，中间崩溃会留下**指向已被复用
的行指针**的索引项 —— 索引扫到一条无关的新元组。这正是设计写「两阶段」的原因。

第二阶段全部交给 `index_bulk_delete` / `index_vacuum_cleanup`，与原生 vacuum
走同一对入口，索引侧的页面变更与 WAL 都由索引 AM 自己发 —— 延续 ①③ 的路子：
**策略在扩展，页面变换与 WAL 用内核的。**

批量按 `maintenance_work_mem` 算容量，装满就地结算一批（清索引 + 回收行指针）
再继续扫，与原生 vacuum 的多趟同构。

##### 途中补掉的一处泄漏：既有的 LP_DEAD 项

扫页原本只认 `ItemIdIsNormal`。但两阶段之间崩溃、或第三阶段拿不到 cleanup lock
跳过某页，都会留下 **LP_DEAD 的中间态**；而下一趟扫描看不见它们（不是 NORMAL），
**行指针就此永久泄漏**。现在扫页把既有 LP_DEAD 一并收进死名单。
这条对无索引通路同样成立，一并修了。

##### ★ 明确的能力边界：不产生 LP_REDIRECT

带索引的关系上，「**死根元组 + 链上还有活的后继**」必须把根做成 LP_REDIRECT
（指向第一个活成员），否则删掉根的索引项之后，那条活行就再也扫不到了。
**本实现不产生 LP_REDIRECT，撞见即 fail-closed ERROR。**

为什么可以就这样交付：

- **TOAST 关系不存在这一格** —— TOAST 元组只被 INSERT / DELETE，从不 UPDATE，
  天然没有 HOT 链。而 TOAST 正是当下唯一可达的带索引用例。
- **用户索引仍被 `ShardXidUtilityGuard` 禁着**（P1_PRECHECK 结论 D）。
- 写一段今天既跑不到、又造不出验收场景的 LP_REDIRECT 逻辑，正是 P5 一路避开
  的那种"永远没被验证过的代码"——何况 `page_verify_redirects()` 在内核里存在
  本身就说明这块的 bug 有多难查。

⇒ 解禁用户索引时**必须先补 LP_REDIRECT**，那道 ERROR 就是缺口的守卫位，
错误信息里写明了缺的是什么。套件 [16] 段有一条负向专门验它（一次已提交
UPDATE 就能造出这一格）。

##### sweep 现在连 TOAST 一起清

`ShardVacuumSweep()` 处理完主堆后，若 `reltoastrelid` 有效就用**同一个分片 oid**
再跑一遍三类动作。分片 oid 直接沿用属主的，不再 `ShardXidLookupByOid(toast_oid)`
—— 后者依赖后端本地的 `toast_map` 是否已被填过，不可靠。
为此把三个页面动作的内部实现都改成接收显式的 `(rel, shard, cur_tb)`。

##### 验收

**本地套件 [16b] 段（TOAST，索引两阶段的正主用例）**：

| 断言 | 结果 |
|---|---|
| 值确实进了 TOAST；索引项数 = chunk 数 | PASS |
| sweep 干净收尾；删掉的死元组数 > 4（主堆 4 条之外还有 TOAST chunk） | PASS |
| **★ TOAST chunk 真的被回收（32 → 16）** | PASS |
| **★ TOAST 索引项同步减少** | PASS |
| **★ 活行的 TOAST 值仍能完整取出（md5 逐字节不变）** | PASS |
| 幂等：再 sweep 一次零动作 | PASS |

最后那条是关键的端到端判据：TOAST 取值走的正是那个 btree 索引，
**索引项若被误删，md5 对不上或直接报 chunk 缺失**。

**本地套件 [12] 段（用户索引的正向）**：4 条插入 + 2 条已提交删除 ⇒
索引项 4 → 2、堆上 LP_NORMAL 2 / LP_DEAD 0、`enable_seqscan=off` 下索引扫描
仍取到 `2,4`。

**跨节点套件**：原先为绕开索引 ERROR 而"值一律不进 TOAST"的规避**已去掉**，
工作负载改为每 10 行放一个大值。于是三个 fileset 成员**全部**参与本轮 vacuum，
比对口径随之从"列举允许值"收紧为**一律逐字节**：

| 成员 | 结果 |
|---|---|
| `0.0` 主堆 | `IDENTICAL_OUTSIDE_HOLE` ×2 follower |
| `2.0` TOAST 堆 | `IDENTICAL_OUTSIDE_HOLE` ×2 follower |
| **`3.0` TOAST 索引（btree）** | **`IDENTICAL_OUTSIDE_HOLE` ×2 follower** |

⇒ **`index_bulk_delete` 发出的 btree vacuum 记录也能经分片流逐字节回放。**

##### 回归（跑完整出口串行，一遍全绿）

| 套件 | 结果 |
|---|---|
| 7 基线套件 | **310/0**（clog_p2 64 / xid_p1 45 / gating_p4 20 / pagecmp_p1 49 / convergence_p4 45 / dtx_tso_p4 49 / tso_si_p3 38） |
| test_shard_vacuum_p5 | **180/0**（T5.7 时是 163） |
| test_shard_vacuum_replay_p5 | **52/0**（T5.7 时是 48） |
| **合计** | **542 / 0** |

`dtx_tso_p4` 本轮**首跑即 49/0** —— T5.7 记的那条"逐节点复位 raft 组的竞态"
已在 `run_p5_exit.sh` 的净场里做成两轮复位，验证有效。

##### 顺带修掉的一处脚本自杀

`run_p5_exit.sh` 的净场第 ① 层原本 `pkill -f 'tests/test_.*\.sh'`，模式太松，
会把**调用本脚本的那个外壳**一起打死（外壳命令行里往往也带着套件路径）。
实测形态很有迷惑性：外壳退出码 144，而 runner 自己脱离父进程继续跑完 ——
"结果还在、外壳没了"，很容易被误读成跑挂了。模式已锚定为
`^bash .*tests/test_[a-z0-9_]*\.sh$`。

##### P5 挂账清单的更新

出口挂账第 ① 项（索引两阶段 / TOAST 空间不回收）**关闭**，但边界收窄为一条
新的、更小的挂账：**LP_REDIRECT 未实现**，它与"解禁用户索引"是同一件事，
不影响 TOAST。其余五项不变。

### 3.6 V4 复评：CLUSTER / VACUUM FULL（2026-08-21，P5 出口后补做）

设计 §10 与未决表 V4 都写明「第一期禁用，**P5 出口再评估 CLUSTER/VACUUM FULL**」。
**T5.7 的出口清单漏了这一条**，此处补做。

##### 一、先更正设计 §10 里那条禁用理由：它有一半已经过时

§10 原文的理由是「走 rewriteheap 的 **freeze/裁决**会拿分片 xid 查原生 clog，
且换 relfilenode 需 fileset 重绑（复制面）」。逐项核下来，**三个论点里有两个
已经不成立**：

| 论点 | 现状 |
|---|---|
| **裁决**会拿分片 xid 查原生 clog | **已不成立。** 补丁 0008（T2.6）把 `HeapTupleSatisfiesVacuumHorizon` 分叉给扩展，而 CLUSTER 的判活入口 `heapam_relation_copy_for_cluster`（`heapam_handler.c:848`）走的正是 `HeapTupleSatisfiesVacuum`。**实证**：本轮探针里 `ANALYZE` 对打标表成功返回，而 ANALYZE 的采样判活（`heapam_handler.c:1070`）用的是同一个入口 |
| 换 relfilenode 需 fileset 重绑 | **已解决。** `ApplyCtrlRecord` 的注释就点名了这一类：「(role, ord) 集合不变、只是文件号变了 → VACUUM FULL / REINDEX / TRUNCATE / 重写类 ALTER……原地换表 + 把对应本地文件截 0，后续 FPI 自然填满。**全自动**」；`test_ddl_fileset_d1.sh` 第 [5] 段专验「VACUUM FULL：主堆/索引/TOAST 全部换文件号，应全自动」 |
| **freeze**会拿分片 xid 查原生 clog | **仍然成立，而且比原文说得更重**（见下） |

##### 二、真正的拦路虎：`rewrite_heap_tuple` 里那次 `heap_freeze_tuple`

`rewriteheap.c:390`：每拷一条元组就顺手冻结一次 ——

```c
heap_freeze_tuple(new_tuple->t_data,
                  state->rs_old_rel->rd_rel->relfrozenxid,
                  state->rs_old_rel->rd_rel->relminmxid,
                  state->rs_freeze_xid, state->rs_cutoff_multi);
```

`heap_freeze_tuple` 只是把四个**原生**水位塞进 `VacuumCutoffs` 再调
`heap_prepare_freeze_tuple` —— 也就是 T5.3a 勘察过、并因此**决定不去分叉**的
那个函数。

**实测的数量关系（本环境探针）**：同一张打标表上
`pg_class.relfrozenxid = 81848`（原生），而元组 `t_xmin = 3`（分片 xid）。

于是失败形态取决于**两个互不相干的计数器碰巧谁大**：

- **`分片xid < relfrozenxid`（新表的常态，如 3 < 81848）** ⇒
  `heap_prepare_freeze_tuple` 第一道检查当场
  `ereport(ERROR, "found xmin 3 from before relfrozenxid 81848")`。
  **响亮的失败**，数据不坏 —— 但错误来自内核深处，信息完全误导。
- **`分片xid > relfrozenxid`（长寿分片跑过 8 万笔以上事务后可达）** ⇒ 不报错，
  转而按 `freeze_xmin = TransactionIdPrecedes(分片xid, FreezeLimit)` 判断，
  极可能为真 ⇒ **给分片元组盖上 `HEAP_XMIN_FROZEN`**。
  那条行从此"对一切快照可见"，与分片 clog 的联系被就地切断 ——
  **中止事务的行复活，且无声无息。**

⇒ **这才是禁用的真正理由**：不是"查原生 clog"，而是
**两套 xid 空间在同一个比较里相遇，后果由巧合决定**。
把它写清楚比原来那句笼统的话有用得多。

##### 三、另外两处未审计的跨宇宙比较

- **更新链解析**：`rewriteheap.c:487/584` 用
  `TransactionIdPrecedes(HeapTupleHeaderGetXmin(...), state->rs_oldest_xmin)`
  判"前一版本是不是 RECENTLY_DEAD"，据此维护 `rs_unresolved_tups`。
  分片 xid 与 `rs_oldest_xmin`（原生）比较，链解析会错判 —— 未审计。
- **cutoffs 的来源**：`cluster.c` 经 `vacuum_get_cutoffs()` 从**原生 procarray**
  取 `OldestXmin`/`FreezeLimit`。分片表根本没有"原生活跃快照下界"这个概念，
  入口参数本身就没有意义。

（`logical_rewrite_heap_tuple` 的那几处比较不额外构成障碍 —— §10 已禁分片表
逻辑解码。）

##### 四、P5 改变了什么：需求侧塌了一大半

V4 当初把复评挂到 P5 出口，是因为 P5 要交付 freeze/回收全章。现在交付了，
**结论是往"更不需要"的方向走的**：

| VACUUM FULL 能给的 | P5 之后还缺不缺 |
|---|---|
| 回收死元组占的空间 | **不缺** —— §6.4 三类动作已做（T5.3a/b/c） |
| 回收索引项 | **不缺** —— 索引两阶段已做（T5.8） |
| 回收 TOAST 空间 | **不缺** —— T5.8 实测 chunk 32→16 |
| **把文件尾部还给操作系统** | **仍缺**（见下） |
| CLUSTER 的按索引物理排序 | 仍缺，但第一期本就无此需求（分片表禁用户索引） |

也就是说：**VACUUM FULL 的价值在 P5 之后收窄到"物理截断文件尾部"这一件事**，
而那件事**根本不需要 rewriteheap**。

##### 五、复评结论

**维持禁用**（CLUSTER / VACUUM FULL 对分片打标表）。理由已从"要改的地方多"
收紧为一条具体的：**`rewrite_heap_tuple` 内的 `heap_freeze_tuple` 会把分片 xid
与原生 relfrozenxid/FreezeLimit 直接比较，后果由两个计数器的巧合决定，
最坏是无声的幽灵行复活。** 要解禁就得给 rewriteheap 开分片分叉 ——
而 T5.3a 已经论证过：`heap_prepare_freeze_tuple` 整个函数都活在原生宇宙里，
给它开分叉等于重写它。

**并给出替代路径（推荐，成本远低于解禁）**：给分片 vacuum 补一个
**尾部截断**（对标 `lazy_truncate_heap`）—— 页面趟之后若尾部若干页已全空，
取 `AccessExclusiveLock` 后 `RelationTruncate` 掉。它：

- 不碰 rewriteheap，不涉及任何跨宇宙比较；
- 复制面走现成的 `XLOG_SMGR_TRUNCATE`（补丁 0001v2 专为捕获它而加，
  FRD §5.2/§5.3 已有先例）；
- 恰好补上上表里唯一还缺的那一格。

这条**没有做**，记为新的挂账项，与 P5 其余挂账并列。

##### 六、V4 状态更新

V4 的"P5 出口再评估"要求至此完成：**结论维持禁用**，理由已更正并具体化；
§10 那条里"裁决会查原生 clog"与"fileset 重绑"两个论点标注为已过时/已解决。
CIC 仍随索引专项（与 T5.8 记的 LP_REDIRECT 是同一件事）。

### 3.7 U-P5-1：分片 clog 由流重建（2026-08-21，P6 前置）

设计 §5.3 明写「落账顺序锁定在分片流的 plsn 序上 ⇒ **每个副本重放同一个流得到
同一本账**（确定性、副本一致）」。T5.4b-2 途中查明实测不是这样：follower 回放
`TXN_MARKER` 时写的是**增强型 clog（按 gxid 索引）**，而 `pg_shard_clog/<oid>`
在它上面**根本不存在**。后果是升主后每个分片 xid 都读成空洞＝RUNNING＝不可见，
**整张表看不见**。本任务补上这一半。

##### 记录格式：MARKER 带上本分区的分片 xid

沿用 FREEZE_UPDATE 那套兼容做法：把 `TxnMarkerPayload` 里「显式补齐、恒为 0」的
`reserved` 改作 `flags`，置 `PARTWAL_MARKER_HAS_SHARD_XID` 时在 `subxacts[]` 之后
追加一个 `uint32` 分片 xid。

两处刻意的选择：

- **只带一个 xid，不带 (shard, xid) 对列表。** MARKER 是**逐分区**追加的，
  而一个分区就是一个分片 —— follower 用自己的本地分片 oid（`ctx->shard_oid`）
  落账即可，**不需要跨节点翻译 leader 的 oid**。这一条把整件事从"要建一张
  跨节点 oid 映射表"缩成"多带 4 个字节"。
- **不含分片写的事务不置位。** `ShardXidXactCount() == 0` 时载荷与既有格式
  **逐字节相同** —— R1/R2/TX1 时代的流与套件不受影响。

载荷仍在取 `PartWALCtl->lock` **之前**组装（既有纪律：`xactGetCommittedChildren`
/ palloc 不能在 LWLock 下做），逐分区的那个值由
`PartWALMarkerSetShardXid()` 在循环里**就地回填** —— 纯内存写、不 palloc、
不取锁。2PC 的 `PartWALAppendMarkerFor()` 本就逐分区调用，回填放在函数入口。

##### 应用端

`ApplyMarkerRecord()` 解析出 xid 后：COMMIT ⇒ `ShardClogSetVerdict(..., true,
commit_ts)`；ABORT ⇒ `..., false, 0`；PREPARE ⇒ `ShardClogSetPrepared(...)`。
顺带 `ShardXidRedoAdvance()` 推进本次启动内的影子发号水位。

##### ★ 实测撞出的一件事：中止事务不一定有账

第一版断言写的是"follower 与 leader 的 clog 逐条相等"，实测
`2,2,2,3,3,...` vs `2,2,2,0,0,...` —— **COMMITTED 全到了，两条 ABORTED 没到**。

查明**不是缺陷，是 `PartWALAbort` 的既有设计**：中止事务若其 DATA 字节尚未入流，
parwal 会丢弃本后端的槽位、也不写 ABORT 标记（"中止字节只进原生 WAL 不进分区
流"，`test_shard_pagecmp_p1` 的注释即此）。而这是**自洽的**：那些元组同样没进流，
副本上没有任何东西引用那些号。

⇒ 断言据此改成按语义写，而不是放宽：**COMMITTED 判决必须逐条到位**（少一条就是
升主后"已提交数据看不见"），而**允许且仅允许一种差异**：leader=ABORTED(3) 而
follower=空洞(0)。另配一条对照断言"follower 确实拿到了非空判决，不是全空洞"。

##### 升主时的发号安全：既有守卫兜住了大部分

follower 的水位文件里 `alloc_wm` 仍是 0（影子只在内存里），所以升主后发号器
从 3 号起走。**这在 (A) 做完之后基本是安全的**，靠的是 T2.5 那道「终局槽跳过
守卫」：`shard_xid_allocate` 逐个查 clog，只有读到 `TXN_RUNNING` 才接受该号，
于是 COMMITTED / ABORTED / PREPARED 的号全被跳过。而"中止且字节未入流"留下的
空洞可以放心复用 —— 副本上本就没有引用那些号的元组。

**残留的那一格（归 P6）**：事务的字节被别人的 group commit 顺带刷进了流，
随后本后端**崩溃**（不是中止）—— 既没有 COMMIT 也没有 ABORT 标记，而元组已经
到了 follower。此时新主的 clog 对该号是空洞，可能重新发出去。设计 §6.6 给了
答案（「新主追平后流里没有该事务的提交标记 ⇒ 从未提交过 ⇒ 改 ABORTED 安全」），
但那需要知道"在用 xid 的上界"——也就是**持久的发号水位交接**，正是 P6 的活。
另有"老主重入后本地那批未入流的字节如何处置"，同属 P6 的 rebaseline 话题。

##### 验收

跨节点套件新增 [2b] 段（升为 **59/0**，连跑 2 轮）：

| 断言 | 结果 |
|---|---|
| leader 分片 clog 有 COMMITTED / 有 ABORTED 判决（夹具有鉴别力） | PASS ×2 |
| **★ 两个 follower 的分片 clog 由流重建，COMMITTED 逐条到位** | PASS ×2 |
| 对照：follower 拿到的不是全空洞 | PASS ×2 |

##### 回归

**出口串行一遍全绿：7 基线 310/0 + P5 新套件 239/0 = 549/0。**
其中 `shard_pagecmp_p1` **49/0** 与 `dtx_tso_p4` **49/0** 最值得看 ——
两者都既打分片标、又做字节级比对，正是 MARKER 格式改动的正面靶子。

**另跑两套最直接消费 MARKER 的 replay 时代套件**（它们的 `CONTAINER` 默认指向
`pg-citus-replay-container`，此处用覆盖跑，属尽力而为）：

- `test_txn_layer_r2` **50/1** —— 唯一的红是 **R-P4-22**「本轮无 Raft 提案被
  丢弃」（流控计数器，2026-08-18 已记录在案的既有观测），50 条功能断言全过。
- `test_follower_replay_r1` **46/4** —— 4 条全是 `_vm` fork 存在性
  （leader 有、follower 没有）。**与本次改动可证无关**：R1 的夹具**从不设
  `pg_partdist.shard_relids`**，于是 `ShardXidXactCount()` 恒 0 ⇒ MARKER 标志位
  永不置位 ⇒ 载荷逐字节不变；follower 侧的分片 clog 落账也被
  `TransactionIdIsNormal(sxid)` 挡在门外。R1 的内容断言（follower 壳表 142 行
  与 leader 一致）照常通过。
  **但也不下"本来就红"的结论**：R1 属 replay 环境，而三套 9 节点环境互斥，
  在本环境跑它本身就是越界取样。记为待在 replay 环境复核的一项。

#### U-P5-1 之二：持久发号水位交接（2026-08-21）

补上 §3.7 里点名的那一格：**follower 的 `alloc_wm` 一直是 0**，升主后发号器
从 3 号起走，只能靠 T2.5 的「终局槽跳过守卫」逐个跳。那道守卫盖不住一种情形
—— 事务的字节被别人的 group commit 顺带刷进了流、随后本后端**崩溃**（不是中止），
既无 COMMIT 也无 ABORT 标记而元组已经到了 follower；新主对该号读到空洞，
会把它重新发出去，**新事务的判决就落到了那批孤儿元组上**。

##### 做法：MARKER 再带一个字段

新增第二个标志位 `PARTWAL_MARKER_HAS_ALLOC_WM`，尾块变成
`[分片xid][发号水位]`，按位存在。

**为什么另开一位而不是把 0x0001 的尾块从 4 字节扩到 8**：0x0001 是**今天刚
上线**的，容器里已经有按 4 字节尾写下的段文件；改尾块尺寸会让那些流的长度
校验当场 ERROR。多一个标志位比多一次「为什么回放报长度不符」便宜。

follower 侧沿用 T5.4b-2 那条通路：worker 里只在 ctx 取 max（每条 MARKER 一次
fsync 太重），apply checkpoint 时发布到槽位，由 `replay_catchup` 的调用方
（有数据库、有事务的普通 backend）落盘。崩溃后从持久游标重放会重新导出同一个
值，语义安全。

落盘接口 `ShardXidRaiseAllocWatermark()` 只抬不降，并且**原样带回 claim_wm 与
两个 vacuum 水位** —— T5.1 那条「落盘是整文件覆写，任何一次不带上它们就会抹成
0」的教训。

##### ★ 实测把"带哪个值"这件事纠正了一次

第一版带的是 `next_xid`。实测 follower 拿到 **6**、而 leader 已经到 **8** ——
差的两个号被**中止且字节未入流**的事务吃掉了（`PartWALAbort` 丢弃未刷记录、
也不写 ABORT 标记，§3.7 已记）。也就是说 follower 只能从"入了流的 MARKER"
学到水位，而 leader 会在两条 MARKER 之间悄悄消耗号。

改带**已持久化的**发号水位（`slot->watermark`）：发号器落盘按
`SHARD_XID_BATCH` 向上取整，于是它比 `next_xid` 宽出至多一个批次 ——
实测 follower 拿到 **4099**，把那段窗口整个盖住。
为此新增 `ShardXidAllocWatermark()`，与 `ShardXidNextToIssue()` 并存：
后者是更紧的界（vacuum 的 fail-closed 上界要紧的），前者是更宽的界
（交接给 follower 要宽的）。**两个界服务两种相反的需求，不能合并。**

##### 验收（跨节点套件升为 **64/0**，连跑 2 轮）

| 断言 | 结果 |
|---|---|
| leader 已发过号（next_xid > 3） | PASS |
| **★ 两个 follower 都接住了发号水位（4099 ≥ 8）** | PASS ×2 |
| **★ 重启一个 follower 后水位仍在（这才叫"持久"）** | PASS |

##### 残留的一格（归 P6，不含糊）

水位**只能新到最后一条入流的 MARKER**。若 leader 在"字节被 peer 的 group
commit 刷进流、本后端崩溃、且该分区此后再无提交"这一串之后立刻死掉，
那个号就落在 follower 的水位之外。要做到滴水不漏，得走设计 §6.6 的
**升主认领**（「新主追平后流里没有该事务的提交标记 ⇒ 从未提交过 ⇒ 改
ABORTED」）—— 而认领需要"在用 xid 的上界"，正是本次交付的这个水位。
**两件事是配套的：水位是认领的输入，认领是水位的兜底。** 认领归 P6。

##### 回归

出口串行一遍全绿：**7 基线 310/0 + P5 新套件 244/0 = 554/0**。

---

### 3.8 P6 详细任务分解（2026-09-02 进期细化，设计 §12 P6 行 + §6.6 + §5.5 + §10）

**范围**：切主/恢复全链路联测 + 全部负向用例并入门禁 + 文档终检 + 并入基线。
**门禁（里程碑原文）**：全量套件零 FAIL；§10 限制项的负向用例（应报错的确实报错）。

**骨架**：P6 不是"再写一批功能"，而是**把前五期所有跨节点的口子收在同一条线上
还清**。这条线上只有两个结，其余任务都挂在它们下面：

- **结一：一个副本文件，两套互不知情的恢复机制。** parwal 回放盖 **leader 坐标系**
  的页面 LSN，本地 pg_wal 崩溃恢复用 **本地坐标系** 的 LSN，而 PostgreSQL 拿
  `PageGetLSN()` 当幂等键（`xlogutils.c:434`）。同一个根，两个方向的症状：
  *跟随侧* = 旧的本地 FPI 无条件盖回回放好的页 ⇒ **R-P4-20 的 PANIC**；
  *升主侧* = 新写的本地记录 LSN 小于页面上的 leader LSN ⇒ 下次本地崩溃恢复
  **静默跳过 ⇒ 数据丢失**（§11 步骤 4 早已写明，但它依赖的补丁 0003 不存在）。
- **结二：一份没有出生证的副本。** locmap 只配对"哪个文件对哪个文件"，不配对
  "**从哪个游标开始**"。§13 约束 2 的后半句（"拷贝时记下 `partition_lsn` 静止点"）
  从未实装，于是所有修复路径的终点——"重做物理基线"——**本身没有工具**。

---

#### 现状盘点（动手前实测，2026-09-02）

| # | 核查项 | 实测结论 |
|---|---|---|
| 1 | **§6.6 三分支** | 第一支（普通事务恢复期）P2 T2.4 已做；第二支（PREPARED in-doubt）P4 已做；**第三支「切主分支」明确留 P6，未做**（T2.4 记要原文：「切主分支（流内无提交标记 ⇒ ABORTED）留 P6」）。入口 `ShardClogClaimRange(shard, from, to)` 现成（`shard_clog.c:260`），唯一调用方是 `shard_xid.c:869` 的恢复期路径 |
| 2 | **§5.5 水位新语义** | **未迁移**。回放侧仍在推进**原生 nextXid**（`PartDistAdvanceNextXidPastXid`，`shard_replay.c:767`，两处调用）。P5 的 `ShardXidRaiseAllocWatermark` 只是**新增**了分片分配器一侧，两条水位当前**并存** |
| 3 | **补丁 0003** | **不存在**。`patches/README.md:30`：「3 号位空缺：`0003` 曾用于一版被放弃的尝试，编号保留不复用」。⇒ §11 步骤 4「推进本地 WAL 插入位点越过 `max_orig_lsn`」**没有实现** |
| 4 | **§9.5 旧 leader 归队分叉检测** | **★ 已实装**（此前一次口头盘点判成"未实装"，据 `NEEDS_REBASELINE` 反查，错了）。`pg_raft_promote_prepare` 开头即调 `pg_raft_check_fastpath_divergence(loid)`，返回 `-1 = 分叉，永不上报`，并顺手 `replay_disable`；套件 `test_fastpath_divergence_tx4.sh` 在。**缺的是修复动作**：函数只发 WARNING 告诉运维"须重做物理基线"，而那个基线工具不存在（= 结二） |
| 5 | **R-P4-12 / R-P4-15** | **均已修**（解冻批次 #4 `a982883` leader 自追平周期化；批次 #5 `004f4c7` 拒绝"收到了却回放不了"的节点升主）。**风险登记簿 8h 条目仍写「需解冻批次 #4」是陈旧文本**，T6.0 回填 |
| 6 | **全量套件口径** | `tests/` 下共 **29 套**；`run_p5_exit.sh` 只跑 **9 套**（8 + tso_si_p3 垫底）。**20 套不在门禁内**（R1/R2/L1/D1/D2/C4/P0 与 TX1–TX4、以及 8 套运维健壮性套件）。P6 门禁是"全量零 FAIL"，必须先把这 20 套的可运行性逐套核实 |
| 7 | **§10 负向用例覆盖** | **两项零覆盖**：`synchronous_commit=off` 与 分片表逻辑解码（全仓 `grep` 无命中）。SERIALIZABLE / FOR SHARE / FOR UPDATE 在 `test_shard_xid_p1.sh`；六个 Citus 禁用项在 `test_shard_gating_p4.sh`。其余命中项需逐条核实是**真断言**还是顺带出现 |
| 8 | **解冻批次** | 历史批次已用到 **#5**。P6 有三处必须改 pg_raft（见 T6.9 闸门），需申请 **批次 #6** |

---

#### 任务表

| 任务 | 内容 | 触及冻结模块 | 验收 |
|---|---|---|---|
| **T6.0** ✅ | 前置核查 + 风险登记簿状态回填（不写产品代码）—— **已完成（2026-09-02）**，产出 `docs/P6_PRECHECK.md`（七条结论 + 23 条 `R-P4-*` 完整台账） | 否 | 见下方实施记要 |
| **T6.1** ✅ | **物理基线重做工具**（结二的正解，三个消费者的共同前置）—— **已完成（2026-09-02）**，新套件 `test_shard_baseline_p6.sh` **43/0** | 否 | 见下方实施记要 |
| **T6.2** ✅ | **locmap 携带基线游标**（`replay_set_locmap` 加 `base_part_lsn`；无基线不再默认从 0 而是**拒绝认领**）—— **已完成（2026-09-02）**，新套件 `test_locmap_base_p6.sh` **25/0** | 否 | 见下方实施记要 |
| **T6.3a** ✅ | **遏制层**：redo 前 offnum 前置检查，把不可捕获的 PANIC 降级为"该 shard 停摆" —— **已完成（2026-09-02）**，新套件 `test_offnum_guard_p6.sh` **24/0** | 否 | 见下方实施记要 |
| **T6.3b** ✅ | **升主推进本地 WAL 插入位点**（新内核补丁 **0010**）—— **已完成（2026-09-04，解冻批次 #6）**，与 T6.4 合用新套件 `test_promote_p6.sh` **39/0** | **是**（调用点在升主路径） | 见下方实施记要 |
| **T6.3c** ✅ | **副本壳表隔离于本地 WAL**（约束 12 + 约束 5 **合并立项**）—— **已完成（2026-09-03）**，新套件 `test_replica_gate_p6.sh` **33/0** | 否 | 见下方实施记要 |
| **T6.4** ✅ | **§6.6 第三支：切主认领**（流内无提交标记 ⇒ ABORTED）+ 关闭 U-P5-1 残留格 —— **已完成（2026-09-04，解冻批次 #6）**，`ShardXidClaimOnPromote`，见 `test_promote_p6.sh` **39/0** | **是** | 见下方实施记要 |
| **T6.5** ✅ | **§5.5 水位新语义**：回放推进**分片分配器**而非原生 nextXid —— **已完成（2026-09-03）**，新套件 `test_xid_watermark_p6.sh` **17/0** | 否 | 见下方实施记要 |
| **T6.6** ✅ | **§10 负向用例并入门禁**（十条逐条核实：九条早有守卫，`synchronous_commit=off` **零覆盖**已补，逻辑解码**装错层**已前移到入口）—— **已完成（2026-09-04）**，新套件 `test_negative_p6.sh` **27/0**；顺带查出并修复 **R-P6-8**（禁用清单整层可绕） | 否 | 见下方实施记要 |
| **T6.7** ✅ | **全量门禁归一** —— **已完成（2026-09-04）**，`run_p6_exit.sh` 一次跑完 **26 套 1136/14**，逐套可读；口径按证据裁定 | 否 | 见下方实施记要 |
| **T6.8** ✅ | **P6 出口回归 + 文档终检** —— **已完成（2026-09-04）**，全量 **28 套 1191/31**；逐套单跑复核后 25 套零 FAIL，**仍开着 3 条**（`txn_layer_r2` 的 R-P4-22 属批次 #7；`dtx_tso_p4` 的 M5/S1 是“度量取不到值”的验收设计缺陷，T6.7 基线即如此）。修复 R-P6-6/9 + T6.3c 闸门的升主出口，门禁净场补四层；文档五处回填 | 否 | 见下方实施记要 |
| **T6.9** ✅ | **解冻批次 #6 闸门**（T6.3b / T6.4 在闸门后） | — | **2026-09-04 用户批准**（"先执行T6.3b然后执行T6.4"）。实际触碰面 `pg_raft--1.0.sql` **37 行**，全在 `pg_raft_promote_prepare` 函数体与其 COMMENT 内，未超出结论七划定的范围；§13 约束 13 的环容量仍**不在本批次**，留作批次 #7 |

> **闸门纪律沿用 P4**：`pg_raft` 解冻做成**显式闸门**，只有闸门后的任务允许改
> `pg-raft-src/`。闸门前的任务（T6.0/T6.1/T6.2/T6.3a/T6.3c/T6.5/T6.6/T6.7）
> 全部只动 `pg-partdist-src/` 与 `postgres-src/`，可以先行。

---

#### T6.0 实施记要（2026-09-02）

**产出**：`pg-partdist-src/docs/P6_PRECHECK.md`，七条结论。核查全程只读，
`pg-raft-src` 一个字节未动。

**最要紧的一条**：**风险台账今天不能当台账用**。正文共 **23 条** `R-P4-*`，
§5 登记簿只收 **7 条**——漏掉的里面包含**唯一未修的三条中的两条**
（R-P4-13、R-P4-22）。另有两处编号重复（`8g.` ×2、`8.` ×2）、一处状态陈旧
（R-P4-12 写着"需解冻批次 #4"而它已于批次 #4 修复）、一处**内容错挂**
（R-P4-22 名下那两段"已排除/下一步"讲的全是 R-P4-20）。P6 门禁含文档终检，
**在台账修好之前，任何"逐条核对状态"都建立在错误的清单上**。已全部修复，
并把附表定为 `R-P4-*` 的唯一权威清单。

**逐条核实的结果**：23 条里**开口的只有三条**——R-P4-13（已绕行、根因未闭）、
R-P4-20（未修，根因本次定案）、R-P4-22（新观测、未查）；R-P4-10 是**适用边界
而非缺陷**。这个数字比开工前的印象小得多，P6 的真实工作量集中在**结构性缺口**
（§6.6 第三支、§5.5 迁移、补丁 0003、基线工具）而不是修 bug。

**★ 两处纠正**（都写进了结论，不藏）：
1. §9.5 旧 leader 归队分叉检测**已实装**（`pg_raft_check_fastpath_divergence`，
   返回 −1 永不上报，套件 `test_fastpath_divergence_tx4.sh` 在）。此前据
   `NEEDS_REBASELINE` 反查判成"未实装"，**该判断收回**。缺的是修复动作不是检测。
2. R-P4-20 名下（实为错挂在 R-P4-22 名下）的 **`relNumber` 代际校验假说作废**：
   已取证的 08-18 那一例里 locmap 配对成立、回放顺利追平到 11，relNumber 是
   对上的，该假说解释不了它。**P6 不要照此实施。**

**顺带量出来的两个数**，直接决定 T6.7 能不能开工：
- 29 套跨 **4 个容器 × 2 种执行模型**，而三套 9 节点集群**互斥**⇒"全量一次
  跑完"在单机上**物理不成立**（登记为 R-P6-1，三个选项已列，待裁定）；
- 门禁外已有**两条已知红**（`txn_layer_r2` 的 R-P4-22、`follower_replay_r1`
  的 `_vm` fork），不处置则"零 FAIL"第一次跑就不成立（登记为 R-P6-2）。

**解冻批次 #6 的范围已按 P4 结论七的体例列出**（结论七）：只含
`pg_raft_promote_prepare` 里的两处新增调用（≤60 行），本体都在 pg-partdist；
**§13 约束 13 的环容量与丢弃留痕明确不搭车**，建议另作批次 #7——P4 期
"搭车 vs 独立"已裁定过一次，独立验收面更清晰。

#### T6.1 物理基线重做工具（结二的正解）

**为什么它排在最前**：它有**三个互不相干的消费者**，而三条路径今天都断在同一处——

| 消费者 | 出处 | 今天的状态 |
|---|---|---|
| 副本初始配对 | §13 约束 2 | 夹具用 `CREATE TABLE (LIKE ...)` 建**空壳表**，只满足"结构一致"，不满足"同源物理基线" |
| 快路径分叉修复 | §9.5 / `promote_prepare` 返回 −1 | 检测到了，只发 WARNING 说"须重做物理基线"，然后**没有下文** |
| 永久分叉修复 | §13 约束 13（环顶爆丢提案） | 文档原话"既无检测也无修复路径" |

**要做的三件事**（缺一不成立）：
1. **静止点** —— 拷贝时记下 `partition_lsn`，与文件字节同批持久；
2. **原子交付** —— 拷贝完成前副本不得被认领（认领即读到半份文件）；
3. **可校验** —— 交付后能验证"这份文件确实对应这个游标"，而不是靠约定。

**★ 一条必须写进接口的语义**：基线游标不是"建议起点"，是**前置条件**。没有它，
`replay_set_locmap` 应当**拒绝配对**而不是默认从 0 —— 默认 0 等于沉默地断言
"本地文件 == leader 在流起点时的文件"，而这个断言从来没人建立过（R-P4-20 的
第一半就是它）。

##### T6.5 实施记要（2026-09-03）

FRD §7.5 的原规则是"follower 回放推进**原生 nextXid** 越过流内一切 xid"，
理由是 R1/R2 时代副本元组身上带的就是 **leader 的原生 xid** —— 本地若把同一个
号再发一次，两笔互不相干的事务会共用一个查账键。

**TX-TSO-MVCC 之后这个理由在打标分片上不再成立**：元组 `xmin/xmax` 里写的是
**分片 xid**（每分片独立宇宙），本地原生 nextXid 发到哪儿与它们毫无关系。真正
需要"永不重号"的是分片分配器，而它的水位已由 U-P5-1 之二经 MARKER 交接过来。

**★ 判据不用新加标志位**：`ShardXidAllocWatermark(shard) > 0` ⇔ 这条流的 MARKER
带过分片 xid ⇔ 它已在新宇宙。这份状态**本来就持久化**在 `pg_shard_xid/<oid>`
里，认领时（还没应用任何记录）就能读到 —— 不必改 apply_checkpoint 格式，
也不必在 ctx 里维护会话内标志。遗留流判据为假，原样走老路。

##### ★ 两次改判据，第二次才测到真东西

**首版拿 leader/follower 的 nextXid 大小关系做判据，根本分不出来**：这台
follower 被历轮测试烧掉大量原生号，nextXid（177994）本来就**远高于** leader
（165577），于是 `PartDistAdvanceNextXidPastXid` 无论走不走都是 no-op ——
**两种行为在计数器上完全同形**。连"遗留流"那条 PASS 也是为了错误的理由通过的。

改成直接观测**分支是否被走到**（trace 计数），才拿到干净的对照：
遗留流 `0 → 0`，新宇宙 `0 → 1`。

> **测量量必须能区分被测的两种行为。** 一个在两种行为下都给同样读数的指标，
> 不管它多"客观"，都不是判据。

顺带发现 trace 位置也错了：`ShardReplayAdvanceWatermark` 只在**认领时**调一次，
分片一旦认领就不会再走它 —— 于是"新宇宙走了跳过分支"这件事在整个稳定运行期
**没有任何痕迹**。补到 checkpoint 路径（每批都经过）才对得上"它一直在生效"。

##### 副产品已验证

§5.5 点名的"#40（§13 约束 4）的原生 clog 逐页补齐不再被回放触发" —— 实测
follower 的 `pg_xact` **只有 1 个段**，没被撑大。

##### ★ 连带把 T6.3c 的闸门收窄了（r1 逼出来的）

回归里 `follower_replay_r1` 出现一条**新红**：`follower 壳表行数 == leader(142)`
—— r1 直接 SELECT follower 壳表验内容，被 T6.3c 的闸门拦了。

**我把适用面划宽了。** 设计里"follower 壳表绝不能被 SELECT"那条禁令的真正理由
是"元组带**外来分片 xid** ⇒ 原生剪枝按原生 clog 误判 ⇒ 就地损毁副本"。
**这个理由对遗留副本不成立**：R1/L1/R2 时代的副本元组带的是**原生 xid**，
原生剪枝判得对。那些套件从 R1 起就一直靠"读 follower 壳表数行数"验回放内容。

按 T6.5 刚确立的同一条原则收窄：**新宇宙用新规矩，遗留流用老规矩** ——
闸门多一道 `ShardXidAllocWatermark > 0` 判据。收窄后 r1 回到 **46/4**，
正是 P5 出口记录的基线（4 条 `_vm` fork 已知项）。

**已知残留（记在案，不假装解决）**：约束 12 的另一半 —— 读会设 hint bit、
进而写本地 WAL —— 对遗留副本**依然存在**。但那是 T6.3c 之前就有的状况，
收窄并没有让它变差；而遗留副本本就是要退役的一支。

套件补了两条断言把边界写成可执行的东西：**"遗留副本（已配对、未打标）SELECT
放行"**，以及打标后"副本已在分片 xid 宇宙（发号水位=4099）"。

##### 回归

`xid_watermark_p6` **17/0**、`replica_gate_p6` **33/0**、`follower_replay_r1`
**46/4**（= P5 出口基线）、`clog_hole_c4` **14/0**（单跑；开 trace 实测**跳过
分支一次都没走**，遗留流老路径原样执行）、`shard_vacuum_replay_p5` 64/0、
`shard_pagecmp_p1` 49/0、`offnum_guard_p6` 25/0、`locmap_base_p6` 25/0、
`shard_baseline_p6` 43/0、`shard_clog_p2` 64/0、`shard_xid_p1` 45/0。

> **两条红都不是改动引起的**：`clog_hole_c4` 在长批次里 13/1、单跑 14/0；
> `follower_replay_r1` 在批次里被 900s 超时砍在第 [6] 节。这再次印证 R-P6-5 ——
> **长批次里靠后的套件，数字本身就不可信**（环境退化 + 超时两头夹）。
> T6.7 的门禁归一必须解决这件事，否则"全量零 FAIL"永远是个抽奖。

##### T6.1 实施记要（2026-09-02）

**结论先行：不做"文件拷贝 + 带外传输"，走流。**

动手前先摸了一遍现有原料，发现项目里**已经有**一条把文件字节送到 follower 的
成熟通路 —— §12 的 DDL 变更用 `log_newpage_range(rel, fork, 0, nblocks, false)`
把新文件**整页**灌成 FPI，捕获钩子收进分区流，follower 当普通 DATA 记录重放。
而 follower 侧的 `ApplyFilesetUpdate` 已经会「按 CTRL 换表 → 截断换了号的成员
→ 等 FPI 重建内容」。

**全量基线与它逐字一致，差别只在"截断哪些成员"**：DDL 只截换了文件号的
（没换号的历史记录早已在流里、内容仍然有效），基线截**全部**。于是实现落到
一个标志位上，而不是一个新子系统：

| 改动 | 内容 |
|---|---|
| `partition_wal_header.h` | 新增 `PARTWAL_FSUPD_FULL_BASELINE (0x0002)` + `PARTWAL_FSUPD_KNOWN_FLAGS` |
| `partwal_sync.c/.h` | `PartWALAppendCtrl` 改为**返回**这条 CTRL 被分配到的 plsn（原先 `void`）——它就是 `base_part_lsn` |
| `shard_fileset.c/.h` | 新增 `ShardBaselineEmit(Oid)`：量块数 → 超限**显式报错** → `RegisterShardFileSet` → 发带标志位的 CTRL → 灌全部成员全部 fork 的 FPI |
| `shard_replay.c` | FULL_BASELINE 时截断**全部**成员；顺带补上「未知标志位一律 ERROR」 |
| SQL | `partdist.shard_baseline_emit(regclass) → bigint` |

走流的三个好处，每一个都是"不做文件拷贝"的独立理由：**没有传输问题**（跨机、
权限、断点续传统统不存在）；**follower 侧一行新的导入逻辑都不需要**；
**与 Raft 复制、崩溃恢复、幂等去重天然兼容**。

**★ 不需要独占锁**（与 pg_basebackup 同理，写进了代码注释）：某页在扫描中途被改，
若改动在该页 FPI **之前**，FPI 里已含它；若在**之后**，那条增量记录的 plsn 大于
FPI 的，重放"先 FPI 后增量"，结果一样。扫描期间新扩的块同理。故
`AccessShareLock`（`FileSetLogRelationPages` 已取）足够。

**★ 超限不静默降级。** §12 的 DDL 路径超过 `fileset_inline_max_blocks` 时只置
`NEEDS_REBASELINE` 位、发个通知；基线是**显式**操作，这里改成直接 ERROR ——
一次灌太多块会把 Raft 日志环顶爆（§13 约束 13），后果是永久分叉。宁可让调用者
自己调高 GUC，也不要让它以为发出去了。

**★ 顺带补的一处 fail-closed**：`ApplyFilesetUpdate` 此前对 `flags` 只查
`NEEDS_REBASELINE` 一位，别的位视而不见 —— 那等于允许"新版 leader 发了新语义、
旧版 follower 当没看见照常重放"。MARKER 的标志位扩展当初就立了「未知位一律
ERROR」的规矩，CTRL 这边一直没有，本次补上（含两位互斥的协议检查）。

##### 验收：`test_shard_baseline_p6.sh` **43/0**

核心用例不是"基线跑得通"，而是**先制造分歧再证明它被修好**：往两个 follower
的主堆各追加一个零页，把它们弄成**比 leader 长** —— 这正是 §13 约束 13
「Raft 环顶爆丢掉 leader 的物理截断」之后副本的样子，文档原话是"永久分叉，
既无检测也无修复路径"，且"运行期不 PANIC，只有逐页 diff 能发现"。

发一次基线之后：块数 8 → 7 回到与 leader 一致；`pagecmp` 判词
`IDENTICAL_OUTSIDE_HOLE`。另有防假通过一条 —— **必须证明 follower 的文件真的
变了**（基线前后 md5 不同），否则"两边都没动"也能比出一致。

还覆盖：负向（超限显式 ERROR 且**不留半条记录**，plsn 未变）、幂等（连发两次
基线仍一致）、基线之后增量照常追平。

##### 三个自己踩的坑（都已修进脚本）

1. **用 md5 比页面是错的尺子。** 增量记录走 `REGBUF_STANDARD`，FPI 里掐掉了
   `[pd_lower, pd_upper)` 那个洞，follower 洞内残字节与 leader 天然不同 ——
   这是压缩不是分歧。项目自带的 `pagecmp.py` 正为此而生。**指纹早就摆在那里**：
   两个 follower 的 md5 **彼此相同**、与 leader 不同 —— 系统性差异而非损坏。
2. **比文件之前必须在 leader 上 `CHECKPOINT`。** leader 写的是共享缓冲区，盘上
   那份可能还是旧的甚至全零；follower 侧 `ShardReplayDoCheckpoint` 却是显式
   刷盘 + fsync 的。不刷就比 = 拿"没落盘的"和"落了盘的"较劲。pagecmp 判词里
   那个 `hole_mismatch [0,0) vs [256,304)` 的 `[0,0)` 就是 leader 盘上的空白页。
   既有套件（pagecmp_p1 / vacuum_replay_p5）都先 CHECKPOINT，我漏了。
3. **长跑套件不要 `| tail -50`。** 被 `timeout` 杀掉时 tail 的缓冲全丢，什么都
   看不到（实测拿到的只有一行 `Terminated`）。写文件，像 `run_p5_exit.sh` 那样。

##### 回归：4 套既有 + 1 套新增 = **298/0**

| 套件 | 本次 | 基线 | 说明 |
|---|---|---|---|
| `shard_pagecmp_p1` | 49/0 | 49 | 三方逐字节，MARKER/回放的正面靶子 |
| `shard_vacuum_replay_p5` | 64/0 | 64 | 跨节点回放 + CTRL 水位 |
| `shard_clog_p2` | **64/0** | 64 | 见下方"一次假红" |
| `ddl_fileset_d1` | 78/0 | — | **本次改动最直接的靶子**（FILESET_UPDATE 处理器） |
| `test_shard_baseline_p6`（新） | **43/0** | — | T6.1 出口 |

##### ★ 一次假红，根因是环境而不是改动（登记为 R-P6-4）

`shard_clog_p2` 首轮 **59/5**，五条红的头一条是
`ERROR: 打标登记表已满（上限 64 张）`，后四条全是它的连锁。

实测：**worker1 的 `pg_shard_xid/` 里有 72 个槽位，而 `SHARD_XID_MAX_SLOTS = 64`**；
逐个比对 `pg_class` 之后，**其中 65 个的 OID 已经不存在**（worker2 是 44/45，
worker3 是 7/7）。也就是说这台节点上的登记表**早就被历史夹具的残骸占满了**，
下一套要打标的验收无论内容对错，一律在登记那一步就死。

**根因是一个真实的设施缺口**：`pg_parwal/<oid>` 目录与回放槽位都有回收器
（`replay_reap_stale`，判据"OID 不在 `pg_class`"），而 **`pg_shard_xid/` 的
分配器槽位没有任何回收器**，只增不减，且有 64 的硬上限。

把三台节点上 116 个陈旧槽位归档（移出目录、保留文件）并重启后，
`shard_clog_p2` 回到 **64/0**，`shard_baseline_p6` 仍 43/0 —— 假红证实。

**这不是本次改动引入的**，但它是**硬堵**：任何一台节点跨过 64，其上所有打标
验收从此全红，且报错信息与被测内容毫无关系，极易被误判成产品缺陷。

##### 顺带量出来的一笔 harness 债（归 T6.7）

首次运行卡在收尾的 `health_check_no_crash` 里 **7 分 40 秒**，最后被 15 分钟的
`timeout` 砍掉 —— 它 `grep` 的是 `/work/pg-cluster-data/*/*.log`，而那些日志
**已经涨到 24 GB**（worker4–8 各 1.2 GB，另有一个 1.9 GB 的 startup.log）。
**每一套验收都要付这笔钱**，不只是本套。已把 28 个日志文件**改名归档**
（`*.archive-20260902.txt`，保留证据、移出 glob），健康检查的扫描面
24 GB → 164 KB。T6.7 的净场层需要固化一条日志轮转。

##### T6.2 实施记要（2026-09-02）

补上 §13 约束 2 的另一半。T6.1 让 leader **能给出**起效游标，T6.2 让 follower
**记住并遵守**它 —— 两件事合起来，"重做物理基线"才是一条能走完的路。

**locmap 升到 v3**：结构里加 `uint64 base_part_lsn`，文件由 912 变 920 字节。
版本判据沿用既有做法（长度即判据，v1 780 / v2 912 / v3 920），于是**旧格式配对
在读取阶段就被拒**——这正是任务卡上"无基线不再默认从 0，而是拒绝认领"的落点，
不需要额外的拦截逻辑。

**`base_part_lsn = 0` 不是缺省值，是一句断言。** 它显式声明"这条流从关系的
出生点开始"，而 `replay_set_locmap` 会**核对本地关系确为空**，不为空即报错并
点名 R-P4-20，HINT 直接指向 `shard_baseline_emit()`。核对不能证明断言为真
（leader 那半边在这里看不到），但能**当场否掉一眼假的那一类**，更要紧的是把
这句话从"没人说出口的假设"变成"写进文件的声明"。

| 改动 | 内容 |
|---|---|
| `shard_replay.h` | locmap v3 + `ShardReplayCtx.base_part_lsn` |
| `shard_replay.c` | 装载时带进 ctx；**换表时保住它**，且 FULL_BASELINE 把它推进到该 CTRL 自己的编号 |
| `replay_worker.c` | 无 checkpoint 时游标取 base 而非 0；checkpoint **早于** base 时按 base 起跑；base=0 的空表核对；认领日志带出起效游标 |
| SQL | `replay_set_locmap` 加第 7 参（**先 DROP 旧 6 参签名**，见下） |

**★ 一处会静默出错的陷阱**：加第 7 参若只写 `CREATE OR REPLACE`，得到的是一个
**重载**，而旧的 6 参声明仍指向同一个 C 符号 —— 它会去读并不存在的第 7 个参数。
必须先 `DROP FUNCTION` 旧签名。带 `DEFAULT 0` 的 7 参版本对既有 6 参调用点
完全兼容。

**★ checkpoint 早于 base 时按 base 起跑**：那份 checkpoint 属于**上一代**配对，
而基线之后那批 FPI 会把文件整体重建 —— 从旧游标起跑等于拿新文件去对旧记录，
正是 R-P4-20 的形状。取二者较大值即可，**不需要额外的代际标识**（P6_PRECHECK
结论二里作废的那个 `relNumber` 代际校验假说，本质上想解决的就是这件事）。

##### 验收：`test_locmap_base_p6.sh` **25/0**

主线是 T6.1 + T6.2 合起来才成立的那件事：给 follower 一个**非空且内容完全
错误**的壳表（灌 300 行 `WRONG-CONTENT`，2 个块）—— 今天这正是 R-P4-20 的形状。
配上 `base_part_lsn` 之后追平，结果 **6 个块、与 leader 逐字节一致
（pagecmp `IDENTICAL_OUTSIDE_HOLE`）**。认领日志实测
`游标从 403 起（配对起效游标 403）`，不是 0。

另覆盖：base=0 + 空表放行（既有用法不破）、base=0 + 非空当场报错、
旧格式 locmap 被读取器拒绝并给出可执行理由。

##### 回归：5 套既有 + 本套 = **323/0**

`shard_baseline_p6` 43/0、`shard_pagecmp_p1` 49/0、`shard_vacuum_replay_p5` 64/0、
`shard_clog_p2` 64/0、`ddl_fileset_d1` 78/0、`test_locmap_base_p6`（新）25/0。
locmap 格式与认领路径都动了，这五套是最直接的靶子 —— 尤其 `ddl_fileset_d1`
（换表路径要保住 `base_part_lsn`）与 `vacuum_replay_p5`（跨节点回放全链路）。

##### 四个自己踩的坑

1. **`LEADER_OID` 取在 `rebuild_shard_identity()` 之前** ⇒ 拿到空串 ⇒ 之后每个
   catchup target 都是空 ⇒ 断言成片假红。空值守卫是补上了，但**顺序才是根因**。
2. **raft 组建在基线之后** ⇒ 基线那批 FPI 压根没被复制到 follower 的本地段文件
   （`replay_trust_local_segments=on` 只读本地段）。组必须先于写入与基线。
3. **`state||'/'||armed` 的布尔渲染是 `true` 不是 `t`**（裸列才是 `t`）——
   拿 `idle/t` 做比较条件恒真，是一次**假通过**。这条坑在项目记录里出现过，
   我又踩了一次。
4. **负向用例设计错误**：想靠"改盘上文件 + 重启节点 + 触发追平"逼出旧格式拒绝
   路径，两轮都没触发 —— worker 的认领是惰性的、locmap 又缓存在它的内存 ctx 里。
   改成直打 `partdist.replay_locmap()`（与认领路径**共用同一个读取器**），
   判据确定、不依赖时序。

> 还有一条红是**守卫在正常工作**：收尾复原时用 `base=0` 重新配对 f2，而 f2 此刻
> 已经把 leader 的数据回放进来了、壳表非空 —— 被新守卫正确拦下。改用 `$base`。

#### T6.3 结一的三层处置

**分层的理由**：三层的**成本与不可逆性差三个数量级**，必须允许分开决策。

- **T6.3a 遏制（半天，先止血）**：现有守卫查的是"关系是否存在"（R-P4-8 两道
  `smgrexists`）与"块号是否在界内"，**够不着"页存在、块在界内、但页太短"这一格**
  （`shard_replay.c` 现有注释已实测确认这点）。补一条：派发 `rm_redo` 之前，对
  HEAP 的 `INSERT / MULTI_INSERT / UPDATE` 统一取出 `offnum` 与
  `PageGetMaxOffsetNumber(page)` 比一次，短了走 `ereport(ERROR)` 让外层 PG_TRY
  接住，**绝不让内核 `elog(PANIC)` 把整节点带走**。现有的 INIT_PAGE 陷阱只是这道
  检查的一个特例，应被它取代。
  > 同时把 R-P4-20 诊断块的触发条件从 `applied_part_lsn == 0` 改成**无条件**——
  > 实测 30 次 PANIC 里最后 7 次的游标是 10/11 而非 0，那个精心埋的取证点
  > **一次都没为它们打过**，这正是 geometry 两周没被记下来的原因。
##### T6.3a 实施记要（2026-09-02）

**这道检查的价值不在"修好了 R-P4-20"**（那是 T6.1/T6.2 的事），而在把
**节点级灾难降格成单个 shard 停摆**。内核那三处
`elog(PANIC, "invalid max offset number")` 不可捕获：回放本体的 PG_TRY 拦不住，
postmaster 整节点重置，随后再选举、再认领、再 PANIC，反复自噬（实测单轮 16–18 次）。

**为什么现有三道守卫够不着这一格**：R-P4-8 的两道查"关系是否存在"
（`smgrexists`），块号那道查"块号是否在界内"。而判据是**页存在、块号在界内、
但页太短**。代码里那条注释早就写明："加上块号那道之后的 10 轮验证里，它的
WARNING 一次都没触发，而 PANIC 照常复发。"

**做法**：派发 `rm_redo` 之前，对 `INSERT` / `UPDATE(new)` / `MULTI_INSERT`
三类取出目标 `offnum`，与页内 `PageGetMaxOffsetNumber` 比一次，判据与内核逐字
相同。它**取代**了此前那个只覆盖"带 INIT_PAGE 的 INSERT"的特例陷阱 —— 那是本
检查的一个子集（INIT_PAGE ⇒ 页被 PageInit 清空 ⇒ 有效 maxoff = 0）。

**★ 页必须走缓冲区读，不能 `smgrread`。** 页面很可能正脏在共享缓冲区里、盘上
那份还是旧的（更短），从盘上读会**误报**。走 `ReadBufferWithoutRelcache` 这一次
查找随后被 redo 自己命中，代价只是一次哈希命中加一把共享锁。

**★ 三类可以直接跳过、不必读页**：带整页镜像（内核走 `RestoreBlockImage`，
压根不做 offnum 判据）、`WILL_INIT`（有效 maxoff 恒 0，无需读）、块号越界
（既有守卫已处置）。

**★ 诊断改挂在跳闸点上。** 原先它写成 `if (ctx->applied_part_lsn == 0)`，理由是
"只在出事形态下打，不污染正常日志"。而实测把 30 次 PANIC 与前一行认领日志逐条
配对之后发现：**最后 7 次的游标是 10 和 11，不是 0** —— 这个精心埋的取证点
一次都没为它们打过，几何形状因此两周无从回溯。挂在跳闸点上，既不污染日志，
也永远漏不掉。

##### ★ 三次走错，每次都换来一条判据（这一节比结论有用）

这道检查看起来只是"redo 前比一次大小"，实际改了四版才对。三处错误都不是笔误，
是**判据本身想错了**：

**① 动作错：写成"跳过本条、游标照推"。**
数据记录一旦被跳过，页面就此残缺，下一条依赖它的记录立刻撞上**另一处**判据
`elog(PANIC, "invalid lp")` —— **遏制层自己制造了它要防的那类崩溃**（实测一轮
89 次跳过之后 worker2/worker3 各崩两次）。
正解是 `ereport(ERROR)`：外层 PG_TRY 接住 ⇒ 槽位 `REPLAY_FAILED`、**游标不推进**，
运维重做基线后从原位继续。**对数据记录，"停"永远比"跳"安全。**

**② 判据错（真凶）：MULTI_INSERT 取了最大 offnum 去比初始 maxoff。**
内核的判据在**循环内**，每次 `PageAddItem` 都把 maxoff 加一：
```c
for (i = 0; i < ntuples; i++) {
    offnum = isinit ? FirstOffsetNumber + i : offsets[i];
    if (PageGetMaxOffsetNumber(page) + 1 < offnum) PANIC;
    PageAddItem(...);          /* ← maxoff 随之增长 */
}
```
约束是"**第一个**装得下"，后面的随页面一起长。首版取最大值，注释还写着
"最大者过了其余都过" —— **推理正好反了**。代价：一条 10 元组、offsets 121..130、
页内 maxoff=120 的 MULTI_INSERT 被误判，而它在内核里完全合法。
**整串两百多条"UPDATE 误报"都是它被拦下之后的级联** —— 后续引用 121+ 的
UPDATE 就真的找不到行指针了。修掉这一条，误报**归零**。

**④ 用例错：复现配方不确定 —— 一次 checkpoint 就把几何抹平了。**
checkpoint 之后**第一次**修改某页的记录会带**整页 FPI**，而 FPI 是无条件
`RestoreBlockImage` —— 它把 follower 那页整个修好，精心制造的几何当场消失，
检查自然不跳闸。单跑时恰好没撞上 checkpoint 所以过了；排在别的套件后面跑时
中间夹了一次，当场复现不出来（19/5，五条红全是"没跳闸"）。
正解是加一个确定性前提：先显式 `CHECKPOINT`，再写一行把该页的 FPI 名额
**用掉**并让 follower 追平；此后对该页的记录都是普通记录，盖回旧页才盖得住。

> ★ 这条坑本身是个**有用的观察**，值得单独记：一次 leader checkpoint 就可能
> **自动抹平**这类分歧。这多半就是 R-P4-20 "最近约 48 轮一次未现"的原因 ——
> 它不是被修好了，是被 FPI 盖过去了。也意味着**生产里它随时可能再现**，
> 只要触发记录恰好落在两次 checkpoint 之间。

**③ 取证错：把几何形状挂在 ERROR 的 `errdetail` 上。**
这条 ERROR 会被外层 PG_TRY 接住，而 PG_CATCH 只取 `ed->message` 重发一条
WARNING —— **errdetail / errhint 连同整个几何一起被吞**，日志里只剩
"追平失败: 页太短"。而"看得清"正是这道遏制层的四个目标之一，R-P4-20 拖了两周
没定案，缺的就是现场。正解：**先以 WARNING 单独记全，再 ERROR 停摆**。

##### ★ 还有一次自己制造的假象，值得单独记

排查途中一度以为"误报不可解释"，因为 log-only 实验显示零跳闸。回头查进程才发现
**上一批回归还在跑，我又启动了一轮** —— 而这套 9 节点集群是**独占资源**
（项目 2026-08-14 记过同款事故："并发跑两份验收互抢夹具，双方数字全部作废"）。
当轮 `shard_baseline_p6 3/40` 就是互抢的典型特征。

**教训**：`flock` 只锁住同名套件，锁不住"我自己又开了一批"。批次之间必须
**先确认集群空闲**再启动 —— 严格串行重测之后，误报是**真实且可复现**的
（211/179 次），这才把注意力逼回判据本身，找到 ②。

##### 验收：`test_offnum_guard_p6.sh` **24/0**

**先把那个几何真造出来**（否则前面所有守卫都是在防一个想象中的东西）：
leader 写 10 行 → follower 追平 → **存下 follower 的页 0**；leader 写到 60 行 →
follower 追平；把那份 10 行的页 0 **盖回去** + `-m immediate` 重启 follower
（清掉缓冲区）；leader 再写一行。此刻记录要落 `offnum=61`，而 follower 页里
只有 10 个行指针。

实测跳闸现场：

```
pg_partdist replay [R-P4-20]: 页太短，停止该分片回放
（shard 1690216 plsn=64 INSERT offnum=61 但页内 maxoff=10）
```

五条断言把闭环走完：**节点还活着**、日志里**没有** `invalid max offset number`
的 PANIC、**没有整节点重置**、**该分片 state=failed**（停摆而非继续跑）、
现场带全了几何（rmid/blk/游标/基线/记录LSN/页LSN）。最后按 HINT 的指引修复 ——
发一次基线 + 带 `base` 重配对 —— follower 回到**与 leader 逐字节一致**。

##### 回归（严格串行，全程不并发任何东西）

`shard_pagecmp_p1` **49/0**（误报归零的关键证据）、`offnum_guard_p6` **24/0**
（确定性配方下连跑两次均通过）、`shard_baseline_p6` 43/0、`locmap_base_p6` 25/0、
`shard_vacuum_replay_p5` 64/0、`shard_clog_p2` 64/0、`ddl_fileset_d1` 78/0。

> 其中 `shard_baseline_p6` / `locmap_base_p6` 一度出红（未完成 / 23-2），
> 日志判据是 `pg_raft: record 403 未达多数派` —— **raft 组失去多数派**，
> 由本任务里反复重启 9 节点造成的环境退化。两轮 raft 复位后均恢复满分，
> 与 T6.3a 的改动无关。

##### 日志膨胀的复量（T6.7 的这笔债比想象中急）

T6.1 时把 24 GB 日志归档掉，扫描面降到 164 KB。**本任务几小时之内又涨回
23 GB** —— `offnum_guard_p6` 最后一次跑就是因此被 900s 超时砍在
`health_check_no_crash` 里（断言其实全过，24/0）。
也就是说这不是"跑久了才有的问题"，而是**每一轮密集测试都会重现的固定成本**。
T6.7 的净场层必须固化日志轮转，而不是靠人工想起来归档。

> 也就是说：**造得出、拦得住、看得清、修得好**，四件事在同一套件里连成一条线。

- **T6.3b 升主侧（内核补丁 0010）**：`pg_partdist_advance_wal_to(XLogRecPtr)`，
  语义类似 `pg_resetwal -l` 但在线执行（持全部 WALInsertLock，跳到目标 LSN 所在
  段起点并切段）。§11 步骤 4 的原文照做即可，**它已经把必要性论证完了**。
- **T6.3c 跟随侧（与约束 5 合并）**：让副本文件**永不被本地 WAL 触碰**。
  `replay_set_locmap` 里那次 `RequestCheckpoint` 只关掉了配对那一瞬的窗口，
  代码注释自己承认"**这不能根治**"：`autovacuum.c:3196` 的
  `if (!av_enabled && !force_vacuum)` 让 `autovacuum_enabled=off` 在
  anti-wraparound 面前失效，一扫副本壳表就重新写本地 WAL、把洞打开。
  处置必须与约束 5 的 `relfrozenxid` 账目**一起**定，分开修必然反复。

##### T6.3c 实施记要（2026-09-03）

**★ 前置核查先有结论：约束 5 已经闭合了，文档没回填。**

动手前按自己定的顺序先核实冻结账目同步 —— leader 侧 `ShardFreezeMaybeEmitUpdates`
按间隔发 CTRL `FREEZE_UPDATE`，follower 侧 `replay_worker.c` 用
`heap_inplace_update` **真的写进了 pg_class**（`relfrozenxid` / `relminmxid`）。
套件实测两侧值相等、副本壳表离强制回卷阈值 **0%**。

于是 §13.5 那句「`autovacuum_enabled=off` 挡不住 anti-wraparound」的**前提**
（副本 relfrozenxid 无人维护、age 一路涨）**已经不成立**：leader 的值在往下搬，
age 不再无界增长，强制回卷也就不会被触发。**T6.3c 因此收缩成一件小得多的事** ——
不用重建冻结账目，只需堵住剩下的本地触碰来源。这正是"先做 3c"那个判断的依据。

##### 缺口是可证的，不是推测

现有两道闸门 `ShardXidUtilityGuard` 与 `ShardAccessGate` 的判据都是白名单
`pg_partdist.shard_relids`，而 **follower 侧从不设白名单**。于是：

> **副本壳表在本节点上是一张完全普通的表。**

设计文档反复写"follower 壳表绝不能被 SELECT"，验收脚本注释也写着"只用文件级
比对触碰 follower"——**但代码里没有任何东西拦着**。一次误操作的 SELECT 或
VACUUM 就有两重**不可逆**后果：① on-access 剪枝按**原生 clog** 判断带**外来
分片 xid** 的元组 ⇒ 就地损毁副本；② 往**本地 WAL** 写针对副本文件的记录 ⇒
把约束 12 的洞重新打开（R-P4-20 的病灶）。

##### 做法：把纪律变成代码

- `ShardReplicaIsLocal(Oid)` —— 判据是回放槽位（已配对 locmap、有本地文件号）；
  快门是 `ReplayCtl->nreplicas == 0` 时零成本返回（**无副本节点与 438 基线
  路径完全不受影响**）。
- **计划期**（`ShardGuardCheckPlan`）：任何引用副本壳表的查询 ⇒ ERROR。
- **维护命令**（`ShardXidUtilityGuard`）：VACUUM / ANALYZE / CLUSTER /
  CREATE INDEX / REINDEX / TRUNCATE ⇒ ERROR；**整库 VACUUM 在有副本时一律拒绝**
  （无法逐表甄别，fail-closed）。不拦 `DROP`（清理要用）。
- 逃生口 `pg_partdist.allow_replica_access`（PGC_SUSET，默认 off），
  错误文案里明说开它意味着接受上述后果 —— **有口子是因为取证偶尔真的需要，
  不是因为它安全**。

**★ 一处必须做的结构调整**：这两支的门控条件**必须与打标表分开**。原来两个函数
开头都是 `if (!shard_gating_active()) return;` —— 白名单恒空的 follower 上，
那道快门一开就把副本这一支也挡在外面了。**这正是"文档强调不许碰、代码却没人拦"
的直接原因**：不是忘了写，是写了也不会被执行到。

##### 验收：`test_replica_gate_p6.sh` **27/0**

九条拦截（SELECT / UPDATE / DELETE / VACUUM / ANALYZE / CLUSTER / TRUNCATE /
REINDEX / 整库 VACUUM）+ **拦下之后副本主堆 md5 未变**（证明闸门拦在"动手之前"
而不是"写了一半"）+ 逃生口开关 + 零影响（配对**之前**放行、leader 与普通表
不受影响）+ 约束 5 核实。

##### ★ 界划错了一次：拦掉了文档写明的修复路径

首版把 `CREATE INDEX` / `REINDEX` / **整库 ANALYZE** 也一并拦了，回归当场抓住：
`ddl_fileset_d1` 78/0 → **65/11**，`shard_clog_p2` 64/0 → **63/1**。

- **CREATE INDEX / REINDEX 必须放行**：结构栅栏立起来之后，文档给运维的指令
  原文是"请在本地 shell 表上做**等价结构变更**后重跑 `replay_set_locmap()`" ——
  那个"等价结构变更"就是在副本壳表上 CREATE INDEX。**拦掉它等于把文档写明的
  唯一修复路径堵死。** 安全性论证：这一步之后紧跟着 CTRL 与那批 FPI，新索引
  文件的内容**会被流整体重建**，本地建索引时读堆判活的结果一个字节都不会留下。
- **整库 ANALYZE 必须放行**：与打标表那一支逐条对齐 —— 那边同样"白名单非空时
  不许整库 VACUUM"，而 ANALYZE 早在 T2.6 就已解禁。整库 ANALYZE 是节点级的
  日常操作，因为存在一份副本就让它全库失败，是那种**最后一定会被运维关掉**的守卫。

修正后套件加了三条**正向断言**（`pass_case`）。教训写在这里：
**一道闸门的正确性，一半在它拦住了什么，另一半在它放行了什么** —— 只写负向
用例，就会做出一道"看起来很严、实际没人敢开"的闸门。

##### 三个自己踩的坑（都是老坑的重演）

1. **会话参数内联进 `-c`**：`SET ...; VACUUM tbl` 被当作**一个事务**执行，而
   VACUUM 不能在事务块里跑；`-At` 还会把 `SET` 的命令标签打到 stdout，
   `tail -1` 取到字符串 "SET" 而不是值。改用项目现成的 `PGOPTIONS` 写法。
   这两个坑记录里都有，本轮各踩一次。
2. **等待循环的 `pgrep` 匹配到了它自己** —— 包装脚本的命令行里就写着套件路径，
   于是它一直在等自己空闲，**白等 30 分钟什么都没做**。模式锚成
   `^bash \./test_` 即可（包装脚本以 `bash -c` 开头）。这是 R-P6-5 那条
   "`pkill -f` 打死调用方外壳"的 `pgrep` 变体 —— **两次都栽在同一处：
   判据把自己也算进去了**。
3. **Citus 把符合分片命名的表从 `pg_class` 扫描里隐藏**。follower 壳表叫
   `t63c_<shardid>`，正好命中，于是查询**既不报错也不返回行**；leader 那条
   恰好带了 `override_table_visibility=false` 所以有值，一对照更像"同步没生效"，
   差点把它误判成产品问题。**本次会话最开头查 e2e 表时就撞过这个坑**
   （`pg_class` 里看不见 `e2e_102921`，而 `SELECT count(*) FROM e2e_102921`
   却跑得通），隔了十几个小时又踩一次。

> 三条的共性：**都不是产品缺陷，都是"取证手段本身有偏"**。而取证有偏最危险的
> 地方不在于多跑几轮，在于它会把红引向错误的归因 —— 第 3 条差一点就让我
> 写下"约束 5 未闭合"的错误结论。

#### T6.4 §6.6 第三支：切主认领

设计原文的判据是干净的：

> **切主场景**：新主追平后流里没有该事务的提交标记 ⇒ 从未提交过 ⇒ 改 ABORTED 安全。
> 依据：提交的必要条件是提交标记已多数派入流（[A]<[B] + 多数派先于本地提交），
> raft 选举保证新主拥有全部多数派条目。

**与 U-P5-1 残留格的配套关系**（P5 出口时已写明，此处落成任务）：认领需要一个
**"在用 xid 的上界"**作为扫描右界，而那正是 P5 交付的持久发号水位；反过来，水位
只新到最后一条入流的 MARKER，那段尾巴要靠认领兜住。**水位是认领的输入，认领是
水位的兜底**——两件事必须同期做完，单做任一件都留口子。

#### T6.3b + T6.4 升主的两件收尾（2026-09-04，解冻批次 #6，39/0）

##### T6.3b：位点跳跃为什么不能"就地跳完就返回"

FRD §11 步骤 4 把**必要性**论证完了（本地位点低于页 LSN ⇒ 崩溃恢复的
`lsn <= PageGetLSN(page)` 会把升主后的新记录当成"页面已更新过"跳过 ⇒ 数据丢失），
但没说**怎么跳才安全**。动手时发现原文那种"持全部 WALInsertLock、跳到目标段起点
并切段"的做法**本身带一个丢数据窗口**：

> 跳完之后、下一个检查点之前，pg_control 里的 redo 位点仍指着旧段。此刻若崩溃，
> 恢复从旧 redo 往前走，读到旧段末尾就断了 —— 新段里那些**已经 fsync 的记录**
> （包括已提交事务）一条都读不到。

关掉窗口的办法不是"跳完赶紧做检查点"（那只是把窗口缩短），而是**把跳跃塞进
`CreateCheckPoint` 已有的那段临界区**：它本来就 `WALInsertLockAcquireExclusive()`
之后算 `checkPoint.redo`。跳跃排在那里，跳完紧接着算出的 redo 直接落在新段内，
期间没有任何后端能插入。**窗口为零，不是"很短"。**

位置还有一个约束：必须排在"空闲跳过检查"**之后**。那条路径 `return` 而不写检查点
记录，若在它之前消费掉待跳目标，就正好造出上面那个窗口。

##### 三个实装细节，每个都能单独把它做坏

1. **`PrevBytePos` 取旧的 end-of-WAL，不是落点**。跳跃后的第一条记录被崩溃恢复
   以**随机访问**方式读到（recovery 从 `checkPoint.redo` 起读），
   `xlogreader.c:1164` 对随机访问校验的是 `xl_prev < RecPtr`。把 PrevBytePos 也设成
   落点，`xl_prev` 就等于记录自身地址，该校验判负 ⇒ `record with incorrect prev-link`，
   恢复停在这里、新段全丢。
2. **跳之前必须先把现存 WAL 写出并 fsync**。位点一跳，那些缓冲页就再没人写了。
   持插入锁再取 `WALWriteLock` 是既有加锁序（`AdvanceXLInsertBuffer` 正是这么做），
   不会反序。
3. **落点取"目标所在段的下一段起点"**。段边界是唯一能干净起头的位置（第一页带
   长页头，`AdvanceXLInsertBuffer` 会照常初始化）；取目标所在段的起点则可能 ≤ 目标。

##### 验收：不能只看 LSN 变大

`test_promote_p6.sh` 里 T6.3b 那半的关键判据有两条，缺一条都能被错实现骗过：

- **检查点 redo 也必须越过 `max_orig_lsn`** —— 只断言 `insert_lsn` 变大的话，
  "跳了却没有检查点"那种丢数据实现照样满分。
- **[2d] 直接 kill -9 取证**：跳跃后写 500 行 → kill -9 → 起库。实测
  `redo starts at 0/85000028`（落在新段内）、`redo done`、500 行一行不少、
  无 prev-link 断链、无缺段。**这才是 T6.3b 唯一真正要保证的事**，前面的 LSN
  比较都只是必要条件。

##### T6.4：为什么 T2.4 那条老路径够不着

§6.6 第三支的判据是干净的（新主追平后流里没有提交标记 ⇒ 从未提交过 ⇒ ABORTED），
难点在**它与 T2.4 的关系**。`ShardXidEnsureClaimed()` 只在"槽位不存在"时认领
（挂槽即认领），而**升主的 follower 上槽位一定已经存在** —— 回放推进分片分配器
水位时就把它挂上了（T6.5）。于是老路径直接 `return 0`，一条都不认领。
`ShardXidClaimOnPromote()` 因此必须是**独立入口**，强制对 `[claim_wm, watermark)`
再扫一遍。

上界取 `watermark` 而不是 `next_xid`：watermark 是持久发号水位（P5 交付），按批次
向上取整，比 next_xid 宽出至多一个批次。follower 只能从"入了流的 MARKER"学到号，
而 leader 上**中止且字节未入流**的事务会悄悄吃掉号 —— 宽的那个正好把这段窗口盖住。
这就是 U-P5-1 那句"**水位是认领的输入，认领是水位的兜底**"落成代码的样子。

**PREPARED 一根汗毛不动**（§6.6 第二支）：`ShardClogClaimRange` 只改 RUNNING。
未决的 2PC 分片 xid 在 follower 上由回放的 `XLOG_XACT_PREPARE` 分支写成
`TXN_PREPARED`，因此不会被误判成中止 —— 否则一笔后来被判 COMMIT 的分布式事务
会在这一分片上丢掉。调用点仍把 `dtx_close_indoubt()` 排在认领**之前**，让有决议的
先落终局。

##### 验收里踩的三个坑（都记下来，因为都是"假通过"的形状）

1. **判别断言的前提被自己的崩溃测试破坏**。首版把"T2.4 返回 0"当判别证据，
   但它排在 [2d] 的 kill -9 之后 —— shmem 被清空、槽位没了，老路径一调就重新挂槽、
   一口气认领 **4095** 条，看上去像"老路径也能干这活"。修法是**显式把前提建出来**：
   先调一次把槽挂上并排空历史，再比。
2. **缺口不会自己出现**。挂槽那一刻 `claim_wm == watermark`，要等 leader 烧掉整整
   一批（`SHARD_XID_BATCH` = 4096）号缺口才张开。实测在本环境跑 4200 笔自动提交
   事务要 **31 秒**（还只是普通表，分片表叠上 raft 复制到分钟级），而这段代价换不来
   新覆盖 —— "真 MARKER 路径确实会抬水位"已由 T6.5 的 17/0 证过。故加一个探针
   `sxid_raise_wm` 把状态确定性摆好，让本套件专心验它自己的事。
3. **联测的号必须造在新缺口里**。上一段结束时 `claim_wm` 已推到当时的 watermark，
   若还往老区间塞，那个号在 claim_wm 之下、本来就不该再被扫到 —— 首版栽在这，
   症状是"升主没调认领"，实际是断言造错了地方。

##### 冻结面的实际触碰

`pg_raft--1.0.sql` **37 行**，全在 `pg_raft_promote_prepare` 函数体（两个
`BEGIN … EXCEPTION` 块）与其 COMMENT 内。两步都**尽力而为**：失败只 `RAISE WARNING`
不挡升主 —— 把一个已追平的副本挡在升主之外，等于用一次可用性事故换一个可以重试的
动作；但 WARNING 必须留，那是事后判断"该节点升主时到底做没做这两步"的唯一证词。

---

#### T6.7 全量门禁归一

**已知的 harness 债**（逐条记，免得出口那天现踩）：
- `test_freeze_sync_d2.sh` 的 `CONTAINER` 默认值是 `pg-citus-replay-container`，
  在本环境不覆盖就报"group0 未收敛"，**不是回归**；
- 夹具残表在各 worker **单调累积**（P5 出口实测跑前 9 张、跑完 10 张），净场脚本
  需要增加"按夹具命名前缀清残表"的一层；
- 9 节点集群是**独占资源**，并发跑两套验收会互抢同名夹具表/分片组/白名单，双方
  数字全部作废（2026-08-14 事故）——归一后的 runner 必须保留 `flock` 独占锁；
- `pkill -f 'tests/test_.*\.sh'` 会匹配到**调用方自己的 shell**（P5 出口实测，
  退出码 144），模式必须锚成 `^bash .*tests/test_[a-z0-9_]*\.sh$`。

---

#### T6.6 §10 负向用例并入门禁（2026-09-04，27/0）

**做法**：把 §10 的每一条限制都写成一条**负向断言**（禁的必须真报错），
再逐条回查代码确认那道禁令**确实存在且在对的层**。逐条核实的结果：
十条里九条早有守卫（SERIALIZABLE / FOR SHARE / FOR UPDATE / CLUSTER /
VACUUM FULL / VACUUM / CREATE INDEX / 整库 VACUUM / 未 join 的 PREPARE），
**只有一条是零覆盖**，另一条**装错了层**：

1. **`synchronous_commit = off` 写分片表：此前完全没有守卫**（本次补上）。
   理由写进了报错本身：提交点语义只允许断言**已持久的事实**，异步提交把
   "已提交"提前到 WAL 落盘之前，分片 clog 的判决就可能比 WAL 更早可见 ——
   崩溃后判决在、数据不在。实装在 `shard_xid_for_current_xact` 的写路径，
   放行 `on` / `local` / `remote_write`（三条各一条正向断言，防"全禁"式假通过）。

2. **逻辑解码：禁令装在可见性层，够不着** —— 见 R-P6-7。溢出发生在解码阶段，
   补丁 0006 挂在 `HeapTupleSatisfiesHistoricMVCC` 的守卫根本执行不到。
   本次**前移到入口**（禁用函数表）。

**过程中的两次自我更正，都值得留痕**：

- **不要写"能把节点弄成起不来"的测试**。首版为了"真做掉逻辑解码这一条"，
  给 worker1 临时开 `wal_level=logical` → 建槽 → 解码 → 收尾调回。结果解码
  撞出 R-P6-7 的 abort，**崩溃留下了那个槽**，而收尾已经把 wal_level 调回
  `replica`，于是节点再也起不来（`FATAL: slot exists, but wal_level < logical`），
  只能人工"调回 logical → 起库 → 丢槽 → 再调回来"。禁令前移到入口之后，
  这段操作整个不必存在，已删除。

- **一条测不过的断言，先怀疑被测对象**。删掉 wal_level 操作后那两条仍然
  FAIL，返回的是原生的 `logical decoding requires wal_level >= logical`
  而不是我们的禁令 —— 第一反应是"断言写法问题：原生检查先跳闸"。换一个
  **非解码类**的禁用函数做判别（`SELECT * FROM citus_rebalance_start()`），
  才看出是**整层失灵**：禁用清单只拦得住 `SELECT f(...)`，FROM 形式一路放行。
  这就是 **R-P6-8**，一个从 T4.6 潜伏至今、把 §9.2 第 3 层整体架空的缺陷。

**结果**：`test_negative_p6.sh` 27 PASS / 0 FAIL，含 4 条 R-P6-8 绕法回归
（FROM 形式 / 子查询 / CTE / 阴性对照）与 1 条"本轮无节点崩溃"哨兵。

---

#### T6.8 P6 出口回归 + 文档终检（2026-09-04）

##### 出口回归：门禁自己先得可信

第一轮全量跑出 **28 套 1192/22**，但那 22 条里**大半不是被测系统的问题** ——
是门禁自己的债。T6.7 把 34 套收编成一次可读的运行，这一期要做的是让那次运行
**说的是真话**。逐条查下来是四件事：

1. **TSO 纪元只给一套做了前置（R-P6-10）**。`shard_xid_p1` 首轮 **18/27**，
   27 条红全是同一句 `TSO 检测到上个纪元的 boot 标记，拒绝发号`。
   T6.7 的 `pre_suite` 已经点破过"顺序依赖冒充前置"这个毛病，却**只给
   `tso_si_p3` 一套**做了纪元复位；而所有要分片 xid 的套件都吃这个前置。
   本期为装内核补丁 0010 重启过多次协调者，防呆一armed，整批全废。
   修：纪元复位提到批次净场（夹在 `rotate_logs` 的"全停"与"拉起"之间 ——
   那是全批唯一所有节点都停着的时刻）+ 每套件按需复位。
   判据用**标记 mtime 早于 `pg_postmaster_start_time()`**：不能用
   `partdist_tso_status()`（库里根本没这函数，见 R-P6-12），也不能只看标记
   在不在（正常服务过的实例自己也会写标记）。

2. **遗留模式也是个隐含前置（R-P6-10 之二）**。`shard_xid_p1` 的"严格模式
   读/写被拦截"只在 `tso_conninfo` 为空时成立 —— T3.6 已把 strict 收紧为
   "无 ts 读 / 无 gxid 写才拦"，TSO 一配置就自动取号、全放行。实测 41/4，
   四条红同源。修：`pre_suite` 里显式清空该 worker 的 `tso_conninfo`。

3. **孤儿 Citus 2PC 无人清理（R-P6-11）**。中断一批验收**不会**结束它已经发出
   的 SQL，留下 `citus_*` 的未决事务；而 §9.2 已关掉 Citus 原生 2PC 恢复，
   于是没人清。实测 worker1 上 4 笔，`shard_identity_p0` 建引用表挂满 600s
   超时（0/0 早退）；清掉后立刻回到 **10/0**。
   现场指纹值得记住：`pg_blocking_pids()` 返回 **0** —— 等的是一个**没有后端**
   的事务，这把"谁在阻塞我"指向 `pg_prepared_xacts` 而不是 `pg_stat_activity`。

4. **净场清不掉分布式/引用表**。worker 侧 `DROP` 对它们无效，`p5repl` 就这么
   在 8 个 worker 上各留一张，"清理后残表"永远停在 8。补一圈协调者侧清理。

##### 三个真缺陷，都是被这轮出口逼出来的

- **R-P6-6 `_vm` fork 缺失 —— 定性完就发现是自己拦死的**（8 条稳定复现的红）。
  两个原假设（覆盖缺口 / 漏捕获）**都不对**。根因是回放侧 R-P4-8 那道守卫
  把判据下在了 **fork 粒度**上：follower 壳表由 `CREATE TABLE (LIKE ...)` 建出，
  从来没有 `_vm` fork ⇒ 每条 VM 记录都被"目标文件已不存在"跳过 ⇒ fork 永远
  建不出来。**跳过 ⇒ 不存在 ⇒ 继续跳过**，自我维持。
  对照原生：`heap_xlog_visible` → `visibilitymap_pin` → `vm_extend` → `smgrcreate`，
  redo 本来就会把缺失的 fork 建出来，是我们抢在它前面返回了。
  修：判据收窄到 `MAIN_FORKNUM`（表真被 DROP 时主 fork 一样不在，R-P4-8 的
  保护原样成立）。效果不只是转绿：`follower_replay_r1` **46/4 → 58/0**、
  `lazy_replay_l1` **49/4 → 57/0** —— 12 条原本被跳过的 `_vm` 逐字节比对
  **这才第一次真正跑起来**。`clog_hole_c4` 13/1 → **14/0** 也一并好了。

- **T6.3c 的副本闸门有入口没出口**（`promote_catchup_tx3` 21/2）。
  闸门按"有配好 locmap 的回放槽位"判副本，而**升主并不撤槽位** ——
  于是新主自己的分片被当成别人的副本一直拦着，"新主壳表读到切主前写入的
  全部 40 行"当场判红。这与闸门自己的 HINT（"或升主后再读"）直接矛盾。
  修：槽位加 `promoted` 位，由 `ShardXidClaimOnPromote()` 在升主收尾置位。
  **置位点为什么在那儿**：解冻批次 #6 批准的是 `pg_raft_promote_prepare` 里
  **两处**新增调用，再加第三处就越界；而 T6.4 那个函数恰好是升主收尾的汇合点。
  **此刻置位为什么安全**：`data_group_promote_prepare()` 的调用点前面就是
  `if (state != RAFT_LEADER) return;` —— 跑到这里**选举已经赢下**，本节点就是
  该分片的 raft leader，剩下的只是把路由登记出去。`promote_catchup_tx3` **23/0**。

- **R-P6-9 打标登记只进不出**（见风险簿）：`ShardMvccSetRemove()`，DROP 提交时
  与文件 GC 同一处摘掉 OID。那条"只进不出"的禁令针对的是运行中的表想退出打标
  语义，**DROP 是另一回事**；而且它的 errhint 原本就承诺了"DROP TABLE 会连同
  水位/clog 文件一并清理"，此前只清文件、没清登记，承诺与实现对不上。

##### 一条自己写的脆弱断言

`promote_p6` 的"返回值就是推进后的插入位点"把函数返回值与随后单独查询的
`pg_current_wal_insert_lsn()` 比**相等** —— 两次查询之间集群还在写 WAL
（raft 心跳），单跑时空闲恰好相等，进了批次就必红。改成对时序不敏感的形式
（落在新段内且单调不超过随后读数）。**验收断言不该假设集群是静止的。**

##### 出口数字与诚实的裁定

**全量 28 套：1191 PASS / 31 FAIL**（`/tmp/p6_exit_final`）。
逐套复跑（重启节点后单独跑）之后，真实状态是：**25 套零 FAIL，2 套仍开着**
（`dtx_tso_p4` 47/2、`txn_layer_r2` 50/1）。

被本期修好而**涨了断言数**的：`follower_replay_r1` 46/4 → **58/0**、
`lazy_replay_l1` 49/4 → **57/0**、`ddl_fileset_d1` 78/0 → **84/0**
（三套合计多跑了 18 条原本被静默跳过的 `_vm` 比对）、
`clog_hole_c4` 13/1 → **14/0**、`promote_catchup_tx3` 21/2 → **23/0**。

**★ "全量零 FAIL" 没有达成，如实记，不粉饰。** 差在两处，性质不同：

1. **3 条真开着的**：
   - `txn_layer_r2` 的 **R-P4-22**（Raft 提案被丢弃）—— 与 §13 约束 13 同源，
     **明确不在解冻批次 #6 内**，属批次 #7。这是**已裁定的推迟**，不是遗漏。
   - `dtx_tso_p4` 的 **M5 / S1** —— 两条都报"实际取不到值"。
     > **★ 本条定性于 2026-09-05（批次 #9）被推翻，如实留痕**：它们**不是**
     > 验收设计缺陷，是**真 bug**，断言一直是对的。把被 `2>/dev/null` 吞掉的
     > stderr 捞出来才看见：`ERROR: 不允许在本节点上对副本壳表执行查询` ——
     > 协调器的分布式读被路由到某个节点，撞在**我自己的 T6.3c 闸门**上。
     > 根因是批次 #7 的 `promoted` 位**只在共享内存里、不落盘**，而那个套件
     > 会重启节点：一重启标志就没了，槽位重新挂上，**注册在案的主被自己的
     > 闸门永久拦住**。我当时还把"不持久化"写成有意为之（"重启后升主路径会
     > 再跑一遍"）—— 那句话是错的，主权没再变就不会再触发一次交接。
     > 落盘之后 `dtx_tso_p4` **49/0**。
     > **教训**：把"度量取不到值"当成验收设计缺陷是**结论下早了**；
     > 先把被吞掉的错误捞出来看，那才是第一步。M5 的形状值得记：它前一条
     「TSO 不可达时事务被拒（fail-closed）」是**过的**，紧接着那条
     「已提交事务可见性不受影响」却取不到值 —— 因为 TSO 停摆时**读**同样
     fail-closed（§2.2 纪律 2「绝不以本地时钟顶替」），这条断言在设计上就
     **无法在故障窗口内观测**。它们在 T6.7 的 47/2 里就是这两条，**不是本期
     引入的**。处置：属验收设计缺陷（该在 TSO 恢复后再测，或退役该断言），
     登记待办，不在本期改 P4 期套件。

2. **剩下的全是跨套件干扰**，不是回归。判据是同一份二进制上"单跑 vs 批次"的
   对照，三套都做过：

   | 套件 | 批次内 | 重启后单跑 |
   |---|---|---|
   | `follower_replay_r1` | 30/18、36/12 | **58/0** |
   | `offnum_guard_p6` | 16/9 | **25/0** |
   | `locmap_base_p6` | 22/1 | **23/0** |

   我一度怀疑 `offnum_guard_p6` 的 9 条红是 `_vm` 修复的副作用（VM 记录不再被
   跳过 ⇒ 连带动堆页 ⇒ 把刻意造的旧页修好了），**这个假设被单跑证伪**。
   同样，`follower_replay_r1` 的红与新加的 `promoted` 字段无关 ——
   共享内存按 `sizeof(ReplayCtlData)` 分配且整块 memset 清零，尺寸与初始化
   都不会因加字段而错。已登记 **R-P6-13**。

##### 文档终检：逐条对齐

- **FRD §13 约束 2**（同源物理基线）：标注**已由 T6.1 + T6.2 闭合**，两半分别
  是"发基线"与"记游标"，并写明此前**两半都没实装**才是 R-P4-20 的土壤。
- **FRD §13 约束 5**（冻结与回卷账目）：标注**早已由 D2 闭合、文档没回填**，
  并点明它顺带推翻了约束 12 那句"`autovacuum_enabled=off` 挡不住"的前提。
- **FRD §13 约束 12**：原文那句"仍未根治……两条约束应当合并立项"划掉，
  补上 T6.3a/b/c 三层的实际处置与**残留**（读会设 hint bit，R3 放开副本读时
  必须重新处置）。
- **FRD §11 步骤 4**：补丁号 0003 → **0010**，并补上原文自己要求的
  "归档/`max_wal_size`/级联备库三项处置结论"，外加实装要点（跳跃塞进
  `CreateCheckPoint` 临界区，窗口为零）。
- **设计 §6.6**：三支各自的落点列成表，写明"切主必须是独立入口"与
  "上界取 watermark 而非 next_xid"的理由。
- **设计 §10**：逻辑解码/异步提交/rebalancer 三行补上**实装位置**（T6.6 已做）。
- **patches/README**：新增 0010 条目 + 「0010 的运维裁定」一节。
- **仍指向作废补丁 0003 的六处**：FRD 三处是现行正文，已改；
  DEV PLAN 与 P6_PRECHECK 三处是当时的核查记录，加标注保留原貌。
- **新查出的差集（R-P6-12）**：`pg_partdist--1.0.sql` 声明 82 个函数，
  tx2 集群上**声明了却不存在**的有 10 个。成因是运行集群增量搭起来的，
  扩展 SQL 只在 `CREATE EXTENSION` 时执行一次。**交付物本身没问题**
  （干净 clone 会得到完整 82 个），但任何"SQL 里声明了就一定能调"的假设
  都会在本环境翻车 —— 本期门禁判据一开始就踩了这个坑。

---

#### T6.8-2 回放上界：把"绕行"收回来（2026-09-04，23/0）

用户的问题问得准：**"是不是 raft 和日志回放哪里还没放开，而是绕行呢"**。
查下来一半不成立、一半成立，两半都记。

##### 先否掉一个我自己的怀疑

`get_follower_applied_part_lsn()` 的文件头注释写着 "Local follower replay
progress"，看名字像"本地进度"，我一度以为升主路径那句"上界取 Raft 已提交位点"
是空话。**不成立**：`applied_part_lsn` 只在 raft apply 路径里推进，而且是按
`[idx, commit_index]` 这一连续段**合并**推进的（`raft_consensus.c:2103`），
天然不超过 commit_index。所以生产升主路径的上界是对的。

##### 但确实有两处在绕行，且互相掩护

1. **`replay_trust_local_segments` 是死配置**。定义在 `replay_worker.c:108`，
   描述写着"测试模式：本地段内容即回放上界"，**12 个验收套件在开它**，
   而**没有一行代码读它**。它本该门控的回退
   （`bound == 0` ⇒ `GetLastWrittenPartitionLSN()`）当时是**无条件**的 ——
   也就是说"追到本地段末尾"这条本该只在测试里成立的捷径，在生产路径上同样敞着。

2. **核心不变式一条断言都没有**。全部 8 处验收传给 `replay_catchup` 的上界
   都是 `get_partition_flush_lsn` —— 那是 **leader 侧已刷盘的字节**，不是
   follower 的已提交游标。于是"绝不碰未提交条目"这句话，在 1191 条通过的断言里
   **一条都没测**。第 1 条让守卫失效，第 2 条让失效没人发现。

##### 处置

- **让 GUC 真的生效**：`bound == 0` 时，`replay_trust_local_segments = off`
  （默认）**报错**而不是替调用方选一个最宽的上界。排在 locmap / armed 两道检查
  **之后** —— 那两个错更具体，且 `lazy_replay_l1` 有一条断言正按"未 armed"的
  文案取证。改动面核过：rendezvous 钩子只发布无人消费，验收里唯一传单参数的
  那处本就期待报错，`promote_prepare` 只在 `bound > app` 时才调（bound 必 > 0）。
- **新套件 `test_replay_bound_p6.sh` 23/0**，两件都钉住。

##### 几何怎么造：**不去制造"未达多数派"**

第一反应是拆掉 follower 的 raft 组让 leader 凑不齐多数派（`txn_layer_r2` §9 就
这么干）。**这条路是错的**：组一 reset，follower 连字节都收不到，"上界之外还有
字节"这个前提本身就没了，断言会变成空的。

等价且确定的形态：让字节**已经躺在 follower 本地段里**（raft 已复制并提交），
却给一个更小的上界。要验的性质完全相同 —— **回放会不会跑到本地段末尾去**。

实测：`P1=21`、`P2=43`，以 P1 为上界追平后 `applied=21`，**没有**越界；
数据层佐证此刻副本与 leader **不**一致（第二批确实没进去）；
换上界为 P2 立刻到 43 且逐字节一致 —— 这条**判别**不能省，否则第一条可能是
因为"本地段里根本没数据"而通过的假通过。

##### 还有两处没收回来，属 R4 正题（记在案，不假装解决）

- **raft → 回放的角色交接是桩**：`partwal_notify_primary_switch()` 整个函数体
  只有一句 `ereport(LOG)`；`raft_boundary.c:21` 自己写着
  "Placeholder until the real role-switch / replay handover lands:
  **follower replay is design-only in shardpg-3.0**"。真正的升主动作挂在
  `pg_raft_promote_prepare`（上报**之前**），而不是 `OP_PARTITION_PRIMARY`
  apply 之后的正规交接；FRD §11 步骤 5 的 `PartDistRoutePromote`（角色置
  `SHARD_PROMOTED` + 路由切换）仍未实现 —— T6.8 加的 `promoted` 位是它的
  **局部替身**，只解决了"闸门放行"，没解决"路由改指向"。
- **副本的建立全靠手工**：`register_shard_fileset` / `replay_set_locmap` /
  `replay_enable` 在 `pg-partdist-src/src` 与 `pg-raft-src/src` 里**没有任何
  调用方**，只有验收脚本在调。没有"给分片 X 建一个副本"的自动路径 ——
  这也是 R-P4-15 那种"旧主带着陈旧数据回归"只能靠**拒绝升主**兜底、
  而不是重建副本的原因。

~~两条都要动 pg_raft 的 apply 路径或新立供给面，属冻结面外，须新批次。~~
**★ 更正（2026-09-04，批次 #7 实做）**：这个判断**错了一半**。角色交接那条
根本不用碰 pg_raft —— 桥接函数已经把参数全传进来，而
`partwal_notify_primary_switch` 本身就在 pg-partdist；供给那条也只需新写
pg-partdist 侧的入口。两条都在冻结面外完成，**一行 pg_raft 都没改**。
教训：**"要不要解冻"得先读一遍调用链再下结论**，凭印象判会白等一个批次。

---

#### 批次 #7：角色交接落地 + 副本供给入口（2026-09-04，27/0）

承接"是不是绕行"那次追查的第 3、4 条。两条都补上了，**而且都不用改 pg_raft**。

##### 第 3 条：raft → 回放的角色交接（原来只有一句 ereport(LOG)）

`partwal_notify_primary_switch()` 此前整个函数体只有一句日志，文件头写着
「Placeholder until the real role-switch / replay handover lands」。
也就是说 raft 把主权翻过来之后，**数据面什么都没做**。

★ 动手前的关键发现：**这件事根本不需要碰冻结面**。桥接函数
`raft_notify_primary_switch()` 已经把 `(partition_id, old, new, switch_orig_lsn)`
全传进来了，而 `partwal_notify_primary_switch` 本身就在 pg-partdist。
更要紧的是它由 **group0 的 apply** 调用，而 group0 的 apply 在**每个成员节点**
上都跑 —— 每个节点都能就地判断自己的新角色。`pg_raft_promote_prepare` 那条路
只在**当选者**上跑，天生够不着降级方向。

实装两个方向：
- **升主**：解除 T6.3c 的读闸门 + **撤下回放槽位的 armed**（新主不再是副本，
  留着 armed 就等着一次误触发的 catchup 拿别人的流盖自己的表）。
- **降级**：立刻收回"已升主"身份，读闸门重新合上。方向是 fail-closed 的 ——
  交出去之后本地这份随时可能过期，宁可拦住。
- **不**自动重新 arm：降级归队能不能直接按游标追平，取决于有没有快路径分叉
  （§9.5：分叉的旧 leader 必须重做物理基线）。那个判断不属于交接。

同时把 T6.8 临时放在 `ShardXidClaimOnPromote` 里的置位**搬走**了 ——
那是当时受批次 #6 范围所限的局部替身，且时机偏早（promote_prepare 跑在**上报
之前**，路由还没翻）。现在回到 FRD §11 步骤 5 说的位置。

★ 编号是个坑：`partition_map.primary_node` 用的是 **pg_raft.node_id**
（本集群 worker1=2），**不是** `PartDistLocalNodeId()` 的 Citus groupid
（worker1=1）。两套差 1，混用会把"我是不是新主"整个判反。

##### 第 4 条：`provision_shard_replica(shard, target_node)`

此前 `register_shard_fileset` / `replay_set_locmap` / `replay_enable` 在产品代码里
**没有任何调用方**，只有验收脚本在调。选 **leader 推**不选拉：素材都在 leader 手里。

**顺序是实测钉死的，分两次远程往返**：
1. 本地登记 fileset；
2. **先**去目标节点建壳表 + `rebuild_shard_identity`；
3. **再**发物理基线，拿它的 partition_lsn 作 base；
4. 推 locmap(base) + `replay_enable` + **把 leader 的分片发号水位一并带过去**。

②必须在③之前 —— 目标没有壳表就没有本地 OID，raft 的字节**归不了档**、它 ack
不了，三成员组里只有 leader 能 ack，基线提案卡在 1/3：
`ERROR: 分区 X(组 N) record 1 未达多数派`。报错指着"未达多数派"，看着像 raft
有毛病，其实是**目标还没准备好收**。各验收脚本的手工配方里壳表一直建在建组
之前，那条隐含前提从没写下来过。

④里带水位也是实测逼出来的：**纯靠物理基线建出来的副本，元组里带的是分片 xid，
却没收过任何 MARKER**，本地水位是 0。于是 T6.3c 的读闸门（判据
`ShardXidAllocWatermark > 0`）把它当成**遗留副本**放行 —— 而遗留副本能读的前提
正是"元组带原生 xid"，在这里根本不成立；T6.4 的认领区间也会是空的。

##### 自己踩的三个坑，都记下来

1. **SPI 只读执行不许 SET**。`prov_query_text` 首版用 `SPI_execute(..., true, 1)`，
   而查询里带 `SET citus.override_table_visibility`（不设就看不见分片表）。
   报错是 `CONTEXT: SQL statement "SET citus.override_table_visibility=false; ..."`
   —— 指着被拒的那条 SET，不是查询本身。
2. **SPI 内存上下文，同一个坑踩了两次**。`SPI_connect()` 把
   CurrentMemoryContext 切到 SPI 过程上下文，`SPI_finish()` 整个释放。
   第一次是 SPI_finish 之后还用 `shard_tbl`/`rels` 拼返回值；第二次是
   `remote` 这个 StringInfo 在阶段①的上下文里 init、阶段②却 `resetStringInfo`
   往里写 —— 症状是远程 SQL 被拦腰拼错
   （`ARRAY[5517348,SET citus.enab...`），而报回来的是目标节点的
   "syntax error at or near citus"，看着像 SQL 拼串写错了。
   **跨 SPI_finish 的串必须先 MemoryContextStrdup 到调用方上下文。**
3. **判据要对齐真正把关的那道门**。我给供给加的"必须在 leader 上调"首版按
   `partition_map.primary_node` 判，可**控制面登记落后于选举** —— 第一次调用时
   组已选出主、登记还是"未登记"，自己把自己拦了。改成查
   `pg_raft_group_status()` 的组状态，与写栅栏查的是同一件事。

##### 验收：`test_handover_provision_p7.sh` 27/0

最有力的两条：
- **不手工建任何东西**，只调一次 `provision_shard_replica`，追平后副本与 leader
  **逐字节一致**（供给前先断言目标上壳表**不存在**，否则这条一致性可能本来就有）；
- 切主**之前**读新主壳表必须被闸门拦、**之后**必须放行 —— 这条**差分**证明放行
  是**交接**干的，而不是别处顺手做掉的。
外加三条阴性对照：在副本上调、在根本没有该分片的节点上调、交接日志按本轮唯一的
GID 定位（不用行号基线 —— 新主是谁要到切主后才知道，在 f1 上取的行号对 f2 是
错位的，首版因此漏判）。

##### 夹具必须让 placement 主抢跑（记下来，免得下次又当成产品缺陷）

组主**必须**落在 placement 主上：数据只在它那儿，而供给要先发基线、基线要求
发起者是组 leader。三个成员同时建组是场竞选竞态，实测连续两轮选到了没有数据的
节点，整套连锁全红。pg_raft 没有"指定竞选/转移主权"的入口，只能让 placement 主
先建组、等一拍再建到 follower 上，仍选不到就复位重来。

---

#### 批次 #8：§13 约束 13 的检测面（2026-09-04，19/0）

约束 13 原文的定性是「永久分叉，**既无检测也无修复路径**」，它是门禁里最后
一条真红（`txn_layer_r2` 的 R-P4-22）也是"全量零 FAIL"的挡路石。查下来两半
都不再成立，但**过程里我自己先误判了一次，记下来**。

##### 一次自我更正：fail-closed 不是重点

我第一反应是"提案被静默丢弃后事务照常提交 ⇒ 分叉"，于是去查提交路径。
查完结论是**反的**：`replicate_group_upto()` 对任何一条未达多数派即
`ereport(ERROR)`，三个生产调用点全走它；`pg_raft_data_propose()` 那个返回 0 的
SQL 入口只有测试脚本在用。**提交路径处处 fail-closed。**

差点就据此去改文档说"约束 13 过时了"。**读了原文才看清真正的机制**：
事务确实中止了，可 `lazy_truncate_heap()` 的物理截断**在 leader 上已经做掉且
不随回滚撤销**（内核在 AccessExclusiveLock 下截空页，认定安全）。
leader 短了、follower 没短，那条 `XLOG_SMGR_TRUNCATE` 再也不会重发 ——
**fail-closed 根本挡不住它**。

教训直白：**改一条风险的定性之前，把原文读完**。我差点用一个正确的观察
（提交路径 fail-closed）去否掉一个它根本没触及的结论。

##### 真正过时的是"无修复路径"

`shard_baseline_emit()`（T6.1）与 `provision_shard_replica()`（批次 #7）就是
重做物理基线 —— FULL_BASELINE 让 follower 先截断全部成员再重灌，正是这条分叉
的解药。原文写下那句时它们还不存在。

##### 补的是检测，落点在 pg-partdist（不必碰 pg_raft）

复制挂钩一失败就**就地**给该分片打标。两个要害：

- **必须非事务性**：出事的那个事务马上要回滚，标记写进表里会跟着没掉，等于
  没记。所以落成 `pg_parwal/<oid>/diverged` 文件（与水位/fileset 同侧），
  写完 fsync。
- **落点在 `PartWALReplicateTouched()` 的 PG_CATCH 里** —— 那是 pg-partdist
  自己的代码，包住 rendezvous 调用、打标、再 `PG_RE_THROW`。
  **又一次不必解冻**（同批次 #7 的教训）。

面：`partdist.shard_divergence(oid)` 返回时刻+原因（无标记 NULL）；
`shard_baseline_emit` / `provision_shard_replica` 成功后**自动清标** ——
重做基线就是修复动作本身，不该再要求运维手工清一次；
`shard_clear_divergence(oid)` 限超级用户，报错文案里明说**清标不等于修好**。

##### 验收：`test_divergence_mark_p8.sh` 19/0

最要紧的一条是「**标记活过了那个中止的事务**」—— 那是这件事的全部意义。
还验了活过重启（非事务性 + fsync）、以及**完整闭环**：造复制失败 ⇒ 打标 ⇒
恢复组 ⇒ 重做基线 ⇒ 标记自动清掉。

夹具再次踩了批次 #7 那个坑：[4] 恢复组时直接三家一起建，组主又落到别处，
三条红与被测内容毫无关系。**抢跑配方要用就全用**，别只用一半。

##### 顺带：门禁最后一条真红（R-P4-22）也是"测错了对象"

`txn_layer_r2` 的「本轮无 Raft 提案被丢弃」是全量门禁里最后一条真红，
一直被当成 R-P4-22 / 约束 13 的观测出口挂着。查下来它**结构上永远不可能过**：
本套件 [9] **故意**拆掉多数派来验 ABORT 标记，那必然让 `quorum_drops` 增加
（实测 1~2 次）—— 它在断言自己刚造出来的东西。

而丢弃计数的含义在 8/3 之后也变了，**增量点的注释自己写着**：
「leader 侧不再截断 parwal，字节留作孤儿、同 plsn 重新 propose，多数派恢复后
自然收敛，**丢弃不再直接等于无痕分叉**」。它是**复制健康度**的观测口。

处置：`health_check_no_drops` 加一个**允许额度**参数，r2 传 2 并写明理由。
**不是把基线挪到故意窗口之后** —— 那一段之后套件已经没有写入了，断言会被掏成
一句空话，零个检查的静默通过是最糟的失败模式。保留全程覆盖 + 给额度，
多一次仍然红。实测 `txn_layer_r2` **51/0**。

至此**全量门禁里已知的真红全部清零**：R-P6-6（`_vm` fork）T6.8 修掉、
R-P4-22 本次定性并改对判据、`dtx_tso_p4` 的 M5/S1 仍是那两条"度量取不到值"的
验收设计缺陷（T6.7 基线即如此，未动 P4 期套件）。

##### 仍未做：自动化

标记只把"分叉了"变成**看得见、修得好**，**没有**自动触发重做基线 ——
在提交路径上放大成一次全量 FPI 洪水，代价与风险都要单独评估。
运维动作：见到标记就在该分片的 leader 上重做基线。

---

#### 批次 #9：收尾五条（2026-09-05）

用户点名"完成除 R4 与深水区之外的六条"。实做五条，**第六条（R-P4-9）本就已闭**
—— 我在上一条汇报里把风险簿的 `8f-old`（**存档的原始描述**）当成了待办，
而 `8f` 明确写着「已修复（2026-08-14 解冻批次 #3，验收 45/0）」，
`raft_consensus.c` 里那段「闭合之前先把判决落进本节点 TX2 分片 clog」就是它。
**读风险簿要看条目状态，不能只看正文。**

##### A. R-P6-13 门禁跨套件干扰 —— 净场层补"等到组稳定"

净场原来只做"复位 + 拉起"。稳定的定义取两条可直接观测的：每个节点上**只剩
group 0**（上一套的数据组残留是主权抖动的直接来源）、且 **group 0 的 leader
各节点一致且非零**。等不到只告警不阻塞 —— 带着告警跑好过静默地把抖动算进结果。

##### B. R-P6-12 声明面 vs 实装面差 10 个函数 —— 差集归零

`scripts/refresh_extension_sql.sh`：抽出 SQL 文件里的 C 函数声明
（`CREATE OR REPLACE` + 幂等 + 不依赖表数据）重放到集群，并入门禁净场。
**过程里踩了两个坑，都是"静默通过"的形状**：
- `docker cp` 进去的文件是 0600、属主是宿主 UID，容器里 postgres 读不到；
  权限必须在 **cp 之前**改（cp 之后 `docker exec chmod` 也不行，exec 的默认
  用户同样不是属主）。
- 错误计数只 grep `^ERROR`，而 psql **自己**的错误前缀是 `psql: error:` ——
  于是"整个文件根本没执行"被报成"9 个节点全部成功、报错 0 条"。
- 自检首版是"C 函数总数 >= 抽出条数"，96 >= 73 轻松通过，而实际 9 个一个没建上。
  **数总量抓不住"少了哪几个"**，改成逐个点名。

##### C. `dtx_tso_p4` 的 M5/S1 —— 不是验收缺陷，是真 bug

见上方对 T6.8 那条定性的更正。`promoted` 位落盘（`pg_parwal/<oid>/promoted`，
与 `diverged` 同侧），槽位载入时读回。**49/0**。

##### D. 分叉修复：从逐个手工到一条命令

`partdist.repair_diverged_shards()`：扫 `pg_parwal/` 下所有带标记的分片，
对能发基线的重发（成功自动清标），发不出去的保留标记并计入 skipped。
**升主路径顺带跑一次**（追平之后、对外服务之前，此刻本节点是组 leader、
基线发得出去，是个安全的自动触发点）。
首版报告里 `skipped=7` 全是表早已 DROP 的孤儿标记 —— **一个越用越吵的报告等于
没有报告**，真出事的那条会被淹掉，所以加了孤儿清理（`stale_cleaned`）。
**没做**：周期性自动触发。唯一能保证 quorum 已恢复的时刻是下一次复制成功，
而那在提交路径上，放大成全量 FPI 洪水的代价要单独评估。

##### E. R-P7-1 没有副本的成员不许当选

`pg_raft_promote_prepare` 里 `loid IS NULL` 原本无条件 `RETURN 1`，注释写
"本节点没有该分片，无事可做"。**那个假设是错的**：入参就是**这个组自己的**
分片，拿不到本地 OID 只有一个含义 —— 本节点根本没有副本，放行等于把一个
**没有任何数据**的节点选成主。改为 `RETURN -1` 并说明正解（先供给再参选）。
R-P4-15 的判据（`applied_part_lsn > 0`）恰恰挡不住它：从没被供给过的成员连
字节都收不到，那个值恒为 0。

##### 顺带修掉 r2 的两处时序假设（不是抖动，是假设错了）

`txn_layer_r2` 在批次里一度 43/8，单跑却过。查下来**不是抖动**：

- **"尾记录就是我的 COMMIT MARKER"** —— 冻结账目发射器（D2）按间隔往同一条流里
  插 CTRL（rmid=255 / flags=4），随时可能追加在提交标记之后。撞上就整段连锁全红
  （尾记录读成 flags=4，后面取 gxid、查 gclog、核 commit_ts 全部落空）。
  改成从 upto **向前找第一条 flags=2**（`marker_plsn()`，最多回溯 20 条）。
- **"区间里每一条都是我的 DATA"** —— 期望条数用 `after-before-1` 硬算，
  CTRL 掺进来就必红（实测 7 条 = 1 CTRL + 1 MARKER + 5 DATA，硬算出 6）。
  断言原本要说的是"**所有** DATA 的 gxid 都与 MARKER 一致"，就按这句写：
  分母取区间内 DATA 的**实际**条数，并补一条计数守卫（否则区间空时是 0==0 的
  静默通过）。

**单跑能过不等于假设成立** —— 只是没撞上。

##### 一处自我更正：防护不该去改别人依赖的状态

批次 #7 的交接在升主时**撤掉回放槽位的 armed**，理由是"防一次误触发的 catchup
拿别人的流盖自己的表"。本次实测它把 `txn_layer_r2` 的"崩溃后重新追平"打红了 ——
`armed` 是各验收/运维自己管的位，交接把它抹掉，别人的流程就断了。
改法：**防护下到动作处** —— `ShardReplayCatchUp` 对 `promoted` 的分片直接拒绝。
`armed` 归属不变，而断言反而更强：p7 现在测的是"盖不进去"，不是"某个位是 0"。

##### 回归

见提交说明。批次 #9 触碰 pg_raft 的只有 `pg_raft--1.0.sql` 里
`pg_raft_promote_prepare` 的两处（拒绝空节点 + 顺带修复），34 行。

---

#### 批次 #10：R4 路由层 + 深水区 R-P6-7（2026-09-06）

用户点名"把剩下的完成"。两块都做完了，而**深水区的结论推翻了此前的定性**。

##### R4：`PartDistRoutePromote` 与"新主捕获新流"

FRD §11 步骤 5 里这个入口只有签名、没有实现（"签名冻结、实现留空"），
连头文件里都没有。落地时把两件事写在一处：
- **角色**：置持久 promoted 记号（解除 T6.3c 读闸门 + 让 catchup 拒绝盖表）；
- **捕获**：把本地 fileset 登记进反向哈希，`wal_insert_hook` 才认得新主的写。

**"同一临界区原子生效"没有做到，如实记**：角色走 `ReplayCtl->lock`、捕获走
`PartWALCtl->lock`，两把锁跨不到一起。实际需要的性质是"两件事都在对外服务
之前完成"，而交接点在 `OP_PARTITION_PRIMARY` apply 里、路由登记在同一 apply
的后半段，这个顺序已经保证了它。

★ 关于捕获还有一层：`EnsurePartWALRegistered` 本来就会在新主**第一次 DML**
时惰性登记，所以这不是一个"洞"，而是一个**窗口**。在交接点显式做掉，新主对外
服务的第一条写就一定被捕获。

**判据必须是"字节真的进了流"，不是"角色位对不对"**。新增三条：
`route_status` 报 `role=promoted captured=yes`；新主写一行 ⇒ 分区流位点确实
前进（实测 50 → 52）；**另一个副本收到了这条记录**。不进流 = 副本静默落后，
比读到旧数据更隐蔽。`handover_provision_p7` 29/0 → **33/0**。
新增观测入口 `partdist.route_status(oid)`：此前"角色 / 会不会被捕获 / 水位"
分散在三处，交接出问题时最需要的恰恰是这一句。

##### 深水区 R-P6-7：**我此前的定性错了，两处都错**

原记录写的是"补丁 0005 在主数据末尾追加 4 字节分片 xid，原生解码器按记录长度
反算元组长度时多算那 4 字节 ⇒ 越界"。栈回溯推翻了它：

① 补丁 0005 的 4 字节走 `XLogRegisterData`（**主数据**），而
   `DecodeInsert` / `DecodeMultiInsert` 的元组取自 `XLogRecGetBlockData`
   （**块数据**）—— 解码器的长度算术根本碰不到它；
② 崩溃也不在 heap 解码，`bt` 指向 **`xact_decode()`**。

**真根因是构建产物的 ABI 撕裂，不是任何补丁的设计缺陷。**
补丁 0007 给 `xl_xact_parsed_commit` / `_abort` 加了三个字段，结构变大；而整棵
postgres 构建树里 **1218 个 .o 编于 `xact.h` 改动之前**（`decode.o` 6 月 1 日、
`xact.h` 8 月 13 日）。于是 `xact_decode` 在栈上按**旧的小结构**分配 `parsed`，
`ParseCommitRecord` 头一句 `memset(parsed, 0, sizeof(*parsed))` 按**新的大结构**
清零 —— 正好打穿调用方栈帧。`make clean` 整树重编后，同一份数据、同一条解码
`count = 20` 正常返回，陈旧 .o 归零；修好的 `bin/postgres` 已同步回仓库。

**禁令保留，理由换回 §10 原文**：解码按原生 xid 组事务，对分片表就是错的分组。
那个理由与崩溃无关、独立成立。此前把禁令写成"遏制一个会打掉整节点的崩溃"，
现在看是**给一个不存在的设计缺陷做的遏制**。

**新登记 R-P6-14**：禁令挂在 `ShardGuardCheckPlan`（ExecutorStart），只覆盖
**SQL 调用**；`pg_recvlogical` 走 walsender 的 START_REPLICATION 不经执行器，
绕得过去。另有更窄的一条：禁令看的是**本节点白名单非空**，而 WAL 里的分片 xid
是写入时就带上的 —— "写入时打标、解码前撤标"同样绕得过去（本次复现正是用它）。

##### 复现方法本身值得记

容器没有 `CAP_SYS_PTRACE`，gdb **附加不上**任何进程（yama=1）。绕法是
**单用户模式直接在 gdb 里跑**：`gdb --batch -ex "set args --single -D <dir>
postgres < 查询文件" -ex run -ex bt`（注意 `run < file` 会把参数清掉，必须用
`set args`）。不需要 attach，绕开 yama。这条路以后遇到"只在某个后端里崩"的
问题还会用得上。

##### 顺带：`tso_si_p3` 的 GlobalSafeTs 断言有竞态

`safe == 最老活跃 start_ts` 长期以 `期望=''` 判红 —— **两个空串比较，连"不相等"
都算不上**。原因是后台 psql 的 stdout 重定向到文件时是**块缓冲**的：那一行要等
进程退出（5 秒 pg_sleep 之后）才落盘，而断言在第 2 秒就读。
`stdbuf -oL` + 轮询即可，语义完全不动。
★ 中途试过换数据源（改读 `partdist_tso_status()` 的 `node<id>=<ts>`），
**不等价**：那里的最小值是 1（陈旧登记）而 safe 是 423，不是同一个口径 ——
要验的是"safe 跟住**当前这笔**活跃事务"，就得用它自己的号。**39/0**。

##### 回归

13 套 **551/1**（唯一的红是 `tso_si_p3`，修复在它跑完之后才落，单跑 **39/0**）。
全部跑在 clean rebuild 的 PG 上。

---

#### 批次 #11：切主后的发号起点 = 旧主真实的 next_xid（2026-09-07）

用户在多分片演示的切主一幕上点出：旧主发到 10，新主却从 **4099** 起发。4099 不是
原生 xid，是 MARKER 交接过来的**按批次向上取整的持久化水位**（T6.4 记要里明写
"上界取 watermark 而不是 next_xid ... 宽一个批次正好盖住中止且未入流的号"）。
用户的判断是对的：每个分片自己维护一个 xid 宇宙，**主换了号不该断、不该跳**。

##### 为什么可以缩到真实 next_xid 而不丢安全性

"宽一个批次"要盖的是"leader 发了号、字节却没入流"的事务。分两种：
- 字节**入了流**却没有提交标记（group commit 下 peer 替它刷盘、随后它中止）：
  这些元组带着分片 xid 躺在副本页面上，**绝不能重发**。以前靠宽水位盖住；现在由
  回放侧把 DATA 记录里的分片 xid 喂进影子水位（`ShardReplayNoteDataShardXid`），
  升主认领时与交接值取 Max。取法与内核补丁 0005 的 redo 侧一致：INSERT /
  MULTI_INSERT / UPDATE 看主数据末尾 4 字节（有 `XLH_*_SHARD_XID` 标志），
  DELETE / LOCK 走 xmax 字段、只在本分片已确认在分片 xid 宇宙时才认。
- 字节**没入流**：旧主一旦降级、重做基线，那些元组不存在于本分片宇宙里，任何
  东西都不会引用旧值，重发无害。

于是三处改动（都在 pg-partdist，不碰 pg_raft）：
1. `partwal_sync.c` `PartWALMarkerSetShardXid`：MARKER 带 `ShardXidNextToIssue()`
   而不是 `ShardXidAllocWatermark()`；
2. `shard_replay.c` `ApplyDataRecord`：解码后喂影子水位；
3. `shard_xid.c` `ShardXidClaimOnPromote`：槽位多半在追平期就已挂上（值 = 最后一条
   入流 MARKER 时的 next_xid），`slot_attach` 不会再看影子，这里补上取 Max 后再认领。
leader 自己的批次水位（本机崩溃恢复起点）不受影响。

##### 实测（多分片演示整套重跑，`shardpg-demo/DEMO_WINDOWS_POWERSHELL.md`）

杀主前旧主"下个号 = 10"（3–9 用掉，9 是被 ROLLBACK 的）；两个惰性从节点水位 6
（只追平到第 7 步）。切主 13s，新主追平后 `shard_xid_next = 10`、
`route_status ⇒ xid_watermark=10`，首行分片 xid = **10**；`sclog_full`：3/8 判决保留，
**9 = ABORTED（认领）**，10/11/4099 空槽。切主后跨分片 2PC 在新主上正常。

##### 顺带查出、本批**未修**的四条（记入台账待裁）

- 2PC 阶段 3 的 COMMIT 标记用 `PartWALBuildMarkerPayload()` 组装，而 `COMMIT
  PREPARED` 跑在没碰过分片表的事务里，`ShardXidXactCount()==0` ⇒ 24 字节旧格式不带
  分片 xid ⇒ 副本的分片 clog 对每笔 2PC 事务永远停在 PREPARED；决议被 FORGET 回收后
  `dtx_close_indoubt` 四级落空 ⇒ **切主后 2PC 提交的行在新主上永久不可见**（实测）。
- 升主不发 `FILESET_UPDATE`：新主的流带它自己的 relfilenumber，其余副本 locmap 仍对着
  旧主 ⇒ `replay_catchup` 报"未知 relfilelocator"，须从新主重新供给（演示 11b）。
- `shard_baseline_emit` 不搬分片 clog：在已有数据之后才供给的副本，对基线之前提交的
  号没有判决，升主即丢行。演示因此必须"先供给、后写数据"。
- MARKER 的 `start_ts` 是墙钟（`GetCurrentTransactionStartTimestamp()`），副本 PREPARED
  槽的 sts 是 8.4e14 —— R-P3-2 "双 ts 宇宙串线"成真。

---

#### ★ P6 是最后一期：不做的事也必须有裁定

P6 之后没有下一期可以推。因此**下列每一条都必须在 P6 出口前拿到明确去向**
（做 / 降级为已知边界并写进 §10 / 从方案中裁掉），不允许再以"挂账"形态留存：

| 来源 | 事项 | 建议去向（待裁） |
|---|---|---|
| P5 挂账 ③ | 分片 vacuum 的**自动启动器**没做（今天只能手工调） | 做（否则生产不可用） |
| P5 挂账 ④ | **回卷本身未实现**（全仓用普通 `<`/`>=`，无模运算） | 降级为已知边界：阶段 2 停发线让线性假设成立，写进 §10 |
| P5 挂账 ⑥ | 两处覆盖缺口（clog **整段删除**分支；真正停在**页面循环中间**的崩溃） | 做（后者需在生产路径加故障注入点） |
| P5 挂账 ⑦ | 分片 vacuum 缺**尾部截断** | 做（V4 复评已认定它是 VACUUM FULL 在 P5 之后唯一还能提供的东西，且不需要 rewriteheap） |
| T5.8 收窄项 | **LP_REDIRECT 未实现**，与"解禁用户索引"同一件事 | 随索引专项；P6 若不解禁索引则写进 §10 |
| §13 约束 13 | Raft 日志环 `RAFT_LOG_CAPACITY=128`，背压超时（默认 10s）后**仍然丢弃** ⇒ 永久分叉 | 做（提容量 + 丢弃留痕；`flow_stats` 观测在流控回退时没跟着回来） |
| §3.1 两次推迟 | 分片 clog 的**物理删除**（`ShardClogRememberDrop` 只登记不删） | 做或裁定 |
| §3.3 两次推迟 | 独立 **Proxy 守护组件** | 文档原话："若 P6 仍无消费方应考虑**从方案中裁掉**" |
| §14.2 | promoted shard 的可读性**硬阻断于 R3** | 裁定 R3 是否进 P6 范围 |

---

#### P6 出口清单（全勾即结项）

- [ ] T6.0 前置核查落档，风险登记簿无陈旧状态
- [ ] 物理基线重做工具可用，三个消费者各有一条端到端用例
- [ ] 无基线游标的 locmap 配对被拒绝（不再默认从 0）
- [ ] 注入"页太短"记录 ⇒ 该 shard 停摆，**节点不重置**（R-P4-20 遏制生效）
- [ ] 升主后写入 + 本地崩溃恢复，新记录不丢（补丁 0010 生效）
- [ ] 副本壳表在 anti-wraparound 压力下不被本地 WAL 触碰
- [ ] 升主后无主 RUNNING 全部落定；U-P5-1 残留窗口被认领兜住
- [ ] 回放推进分片分配器（§5.5 迁移完成）
- [ ] §10 每条限制各有一条负向断言，**应报错的确实报错**
- [ ] **全量 29 套一次跑完、零 FAIL**
- [ ] 上表"不做的事"逐条有裁定，无一以挂账形态留存
- [ ] 三份设计文档（DESIGN / DEV PLAN / FRD）与代码逐条对齐

---

## 4 未决核实点跟踪表

| 编号 | 内容 | 归属 | 状态 |
|---|---|---|---|
| U-P5-1 | **follower 侧的分片 clog 未由流重建 + 无发号水位** | P6（切主/恢复全链路） | **◐ 两半都已做（2026-08-21，§3.7）**：① MARKER 携带本分区分片 xid，follower 据此重建 `pg_shard_clog`；② MARKER 再携带 leader 的**已持久化发号水位**，follower 落进自己的 `pg_shard_xid`（实测 4099，且跨 follower 重启仍在）。**残留归 P6**：水位只能新到最后一条入流的 MARKER，滴水不漏要靠 §6.6 的升主认领 —— 而认领的输入正是这个水位，两件事配套。**已立为 §3.8 的 T6.4**（2026-09-02 P6 进期细化） |
| V1 | 原生表 hint 位 vs pagecmp 既有处理（§4.5） | T1.0 | ✅ 已完成（P1_PRECHECK 结论 A） |
| V2 | Citus 连接建立点枚举完备性（§9.2 ①） | P4 前 | ◐ 挂点运行时实证（T4.0 实验 2C：assign 每连接前置在 worker 驱动下生效）；完整枚举随 T4.1 逐路取证 |
| V3 | 引用表使用现状与只读裁定（§9.2 ②） | P4 前 | ✅ 已裁定（T4.0：系统现役 0 引用表；第一期建表后只读） |
| V4 | CIC/CLUSTER 分叉 vs 禁用裁定（§11） | P2 期间定 | ✅ 已裁定（2026-08-13 T2.6：第一期禁用）；**P5 出口复评已补做（2026-08-21，§3.6）：维持禁用**——理由收紧为"`rewrite_heap_tuple` 里的 `heap_freeze_tuple` 拿分片 xid 与原生 relfrozenxid/FreezeLimit 直接比较，后果由两个计数器的巧合决定，最坏是无声的幽灵行复活"；§10 原文另两个论点（裁决查原生 clog、fileset 重绑）已分别过时/已解决。替代路径：给分片 vacuum 补尾部截断（新挂账） |
| V5 | Citus 13.1 worker 驱动 2PC 行为一致性（§9.1 实验一） | P4 前 | ✅ 已实测（T4.0 实验 2：语义逐项一致，连接并行度差异入 R-P4-1） |

---

## 5 风险登记簿

> **★ 台账纪律（2026-09-02，T6.0 确立）**：`R-P4-*` 的**唯一权威状态清单**是
> `pg-partdist-src/docs/P6_PRECHECK.md` 附表（23 条，逐条核实）。本节保留叙述性
> 风险与各期新增条目；**新增风险须同时入本节与该附表**。
>
> T6.0 核查发现本节此前的四类缺陷（收录不全 23→7、编号重复 ×2、R-P4-12 状态
> 陈旧、R-P4-22 名下错挂 R-P4-20 的排查内容），前三类已在本次修复；第四类在
> §3.3 正文就地标注。历史小节标题「（P1 视角 Top 4）」已随收录面扩大去掉。


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
8a. **R-P4-1 连接并行度差异**：worker 驱动对同一参与节点开多条连接（实测
   3 条），每连接一笔 prepared 事务——参与者/广播必须按"(事务,连接/分片)"
   粒度对齐，不得假设一节点一 prepared。
8b. **R-P4-2 注入 ts 漏登记**：远端后端注入 start_ts 不进本地活跃集合 ⇒
   GlobalSafeTs 越过活跃远端读；T4.1 验收须含"远端持快照期间 safe 被钉住"。
8c. **R-P4-3 驱动事务的 MARKER 早值**（T4.4 换源实装时登记）：驱动事务的
   MARKER 载荷在 PRE_COMMIT 组装（早于远端 PREPARE），携带的是票齐前的暂存
   ts；决议 ts 在票齐点取号并覆盖暂存 → 0007 尾块（主本可见性）与决议同 ts
   自洽，但 follower 增强 CLOG 由 MARKER 驱动，存在 [早值, 决议ts) 的副本早
   可见窗口。T4.5 广播以决议 ts 幂等重写参与分片判决时收口；收口前副本读
   一致性依赖该窗口不被跨越（惰性回放场景实测排期进 T4.7 矩阵）。
8h. **R-P4-12 新 leader 继承决议的时延上界**（2026-08-15，T4.7 M4 间歇
   失败暴露）：见 T4.7 记要。非正确性缺陷（决议在多数派上），但收敛时延
   不受"选举 + 追平"约束；修法在 pg_raft（当选后主动拉取式追平，或 peek
   回退读组内其他成员）。**★ 已修（2026-08-15 解冻批次 #4，`a982883`）**：新增
   `pg_raft_group_drain_apply()` 并把 `group_apply_pending` 挂进 `pg_raft_catchup`
   每组循环，追平从"有人问才做"变成 monitor 周期自愈；配套 R-P4-13 绕行
   （`b30adcb`：`dtx_peek` 本地缺失时拉取组内成员决议）。**本条状态于 2026-09-02
   P6 进期细化时回填——此前一直停在"需解冻批次 #4"的陈旧文本上。**
8g. **R-P4-10 纯 Citus 2PC 写的判决落不了账**（2026-08-14 T4.6 验收实测
   定性，属**适用边界**而非缺陷）：走 §3.3 决议流程的跨分片事务，判决由
   决议广播/清扫/读者问询三通道收敛（T4.5 已 45/0 实证）；但**不经决议
   流程**的写（纯 Citus 2PC，如 T4.6 放行项用例）没有决议可问，判决永远
   落不进分片 clog，提交后行不可见，未决登记持续堆积（实测两分片各 4 笔）。
   影响面：这类写在本方案语义下本就不该出现（§9.2 第 2 层要求所有分片写
   经连接加入协议 + §3.3 决议）；处置方向 = 第 1 层安全网在 strict 模式下
   已能拦住无 ts 写，P5 评估是否把"无决议流程的分片写"也纳入 fail-closed。
   记此以免后人把"提交后读不到"误判为可见性缺陷。
8f. **R-P4-9 已修复（2026-08-14 解冻批次 #3，验收 45/0）**：一个症状、
   **两处病灶**，前者长期藏在后者背后——
   (a) *守护闭合不落判决*：切主后阶段 3 标记的本地写被写栅栏拒（非 leader
   不得本地写），判决进不了分片 clog，而 prepared 已闭合、2PC 段作废、
   未决登记失去载体 ⇒ 行永久不可见。**修**：先落账后闭合（经 rendezvous
   `partdist_dtx_apply_decision_fn`）；commit_ts 本地 dtx_decision 取不到
   （崩后本地尚未 apply 回决议）时用 `dtx_peek` 向协调组 leader 补取；
   仍取不到则 **本轮不闭合**（保留 prepared 与登记，守护下轮重来）——
   闭合不可逆，多等一轮只是延迟，绝不用 ts=0 破坏 SI。
   (b) *清扫自愈误杀活账*：崩溃重启后 journal 重放（登记回来）与 2PC 段
   recover（PREPARED 槽回来）**不在同一时刻**，清扫插进空窗即把活账判成
   残渣注销；随后守护补到 ts 想落账时登记已不在（静默返回 0）。实测链路：
   `16:02:14 清扫注销 → 16:02:46 守护补到 dcts=23 落账返回 0 → 闭合 →
   判决永久丢失`。**修**：兜底判据——节点上只要还存在体系内原生 prepared
   （citus_… / shardpg_dtx_…），一律不注销登记。
   **定位手段**：三点追踪（rendezvous 指针 / 本地 dcts / 补取后 dcts），
   逐环排除后锁定；期间两次自我更正（`strings` 的 ASCII 过滤误判"代码没
   编进 .so"；时间戳误读成"节点没重启"），均由二进制精确搜与进程启动时刻
   纠正。
8f-old. **R-P4-9 原始描述（存档）**：恢复守护闭合不落 TX2 判决（2026-08-14 可信验收 43/2 的两条
   红，真实机制缺口）：参与者崩溃后本组已切主，pg_raft 恢复守护向协调组问到
   决议并 COMMIT PREPARED 闭合（日志"决议=COMMIT，已闭合"），但闭合走 tx 通道
   ——阶段 3 标记的本地写被写栅栏拒（非 leader 不得本地写），**分片 clog 的
   COMMITTED 判决因此没落上**，未决登记也随原生 prepared 消失而失去载体，
   行永久不可见。修法：守护闭合路径接 TX2 落账（经 rendezvous 调
   DtxPendingContainsDtxid 同族的落账入口，或把闭合改为"先落 clog 判决、
   再 COMMIT PREPARED"）；涉 pg_raft 恢复守护，属解冻批次范围外，待批。
   影响面：仅"参与者崩溃且本组切主"这一格，正常路径与非切主崩溃均已实证收敛。
8e-fixed. **R-P4-8 已修复（2026-08-15）**：两道守卫互补 ——
   ① 认领时（`ShardReplayLoadLocMap` 末尾）用 `smgrexists` 校验主堆本地文件
   实际存在，不在即返回 false，调用方按"无有效 locmap"解除 armed；
   ② 运行时（`rm_redo` 之前）逐条校验目标文件，不在则跳过该记录。
   为何要两道：认领校验只查一次，而**表可以在认领之后被 DROP**（验收清理
   正是如此），此后每条记录都 redo 到已删文件上 → PANIC 复现。用
   `smgrexists` 而非 catalog 查询——回放 worker 无 DB 连接，摸 catalog 会
   SIGSEGV（R-P4-6 的教训）。**实证**：修前几乎每轮必崩（worker3/4 单轮
   各 16–18 次），修后三连跑**零崩溃**。
8e-old. **R-P4-8 原始描述（存档）**：回放器认领残留分片 PANIC 自噬（2026-08-14 实测，既有缺陷，
   与 T4.5 改动无关）：升主后回放器认领分片、游标从 0 起重放，若该分片的
   本地文件已被 DROP/截断（历轮夹具残留），读页面即
   `PANIC: invalid max offset number` → **节点整体重置** → 再选举 → 再认领
   → 再 PANIC，反复自噬（实测 term 21→23、worker3/4 共 34 次）。回放本体
   的 PG_TRY 护栏对 PANIC 无效（不可捕获）。处置方向：认领前校验分片
   relfilenode 存在且段文件长度与游标自洽，不自洽即置 REPLAY_FAILED 跳过
   而非重放；夹具侧清理残留 replay 认领。排 T4.5②/T4.7 之间处理。
8d. **R-P4-4 本地执行参与者漏出写集**（T4.4 验收实测复现）：worker 驱动且
   本节点持有写分片时，本分片写走 Citus local execution、不进
   pg_dist_transaction，legacy 写集探针看不见它 ⇒ nparts 少 1，两组写可能
   被误判为快路径漏决议。T4.5 参与者集合改由 join 登记（gxid/活跃集合）装
   配时闭合；interim 运行纪律：驱动节点不持写分片，或会话内
   SET citus.enable_local_execution=off（验收腿 2 实证工作绕法）。验收含
   "盲区复现"断言，T4.5 闭合时该断言应翻红提醒改写。
9. **R-P3-2 双 ts 宇宙串线**：TSO 逻辑值与 MARKER/DTX 本地时钟占位值在 P3
   并存（实证当前零比较点）；任何新代码不得让两者进入同一比较；P4 换源前
   全量重审计。

P6 增补（2026-09-02，T6.0 核查产出，详见 `docs/P6_PRECHECK.md`）：

10. **R-P6-1 全量门禁口径未定**：`tests/` 共 29 套，跨 **4 个容器**
    （tx2 ×9、replay ×7、tx ×4、raft4 ×1）与 **2 种执行模型**（宿主机
    `docker exec` ×21、容器内 ×8），而三套 9 节点集群**互斥**⇒"全量一次
    跑完"在单机上物理不成立。三个选项（全收编 tx2 / 分环境门禁 / 缩编退役）
    的成本已列，**裁定前 T6.7 无法开工**。
11. **R-P6-2 两条已知红挡在门禁前**：① `test_txn_layer_r2` 50/1，唯一的红是
    **R-P4-22**，而它与 §13 约束 13 同源（环容量 128、背压超时后仍丢弃
    ⇒ 永久分叉），**是分叉风险的唯一可观测出口**；② `test_follower_replay_r1`
    46/4，`_vm` fork 存在性差异，2026-08-21 记为"待在 replay 环境复核"后
    **至今未进任何台账**。二者不处置，"零 FAIL"第一次跑就不成立。
12. **R-P6-3 台账腐化**：23 条 `R-P4-*` 只有 7 条进登记簿，两处编号重复、
    一处内容错挂、一处状态陈旧（均已在 T6.0 修复）。**书写纪律**：新增风险
    即入登记簿与 P6_PRECHECK 附表，否则出口时的文档终检会再次落空。
13. **R-P6-4 `pg_shard_xid` 槽位只增不减且有硬上限**（2026-09-02，T6.1 实测）：
    `pg_parwal/<oid>` 与回放槽位都有回收器（判据"OID 不在 `pg_class`"），
    而**分配器槽位没有**。实测 worker1 积到 **72 个**（`SHARD_XID_MAX_SLOTS = 64`），
    其中 **65 个的 OID 已不存在**；一旦跨过上限，该节点上**所有**打标验收在
    登记那一步就死，报错（"打标登记表已满"）与被测内容毫无关系，极易被误判成
    产品缺陷（本次就先误判了一次）。**处置**：给分配器槽位补一个同判据的回收器，
    并入 T6.7 的净场层；上限本身是否要提，另议。
14. **R-P6-5 批次之间必须先确认集群空闲**（2026-09-02，T6.3a 实测）：
    各套件的 `flock` 只锁住**同名套件**，锁不住"上一批还没跑完、我又启动了
    一批"。实测在 T6.3a 排查途中因此产生整轮不可信数字（`shard_baseline_p6`
    3/40 是互抢的典型特征），并把注意力引向错误方向。**处置**：归一后的
    runner 取一把**全局锁**（而非按套件），并在启动前显式检查无同类进程；
    并入 T6.7。与 2026-08-14 那次事故同源，那次的结论"9 节点集群是独占资源"
    只落到了单套件层面。
15. **R-P6-6 副本缺 `_vm` fork**（2026-09-04，T6.7 全量门禁量化）：
    `follower_replay_r1` 与 `lazy_replay_l1` 各 4 条断言、**共 8 条**报
    `f1 0.0_vm 两侧都存在（实际 y/n）` —— leader 有 visibility map fork，
    follower 没有。P5 出口只在 r1 上见过并记为"待在 replay 环境复核的一项"，
    **从未进过任何台账**；归一门禁后才看清它跨两套件、是**稳定复现**而非偶发。
    **性质**：副本物理形态与 leader 不一致（约束 1"完整物理子流"的直接反例）；
    尚不知是"VM 记录整类漏捕获"还是"leader 侧以不产生 WAL 的路径建了 VM fork"。
    ~~**处置**：P6 出口前必须定性 —— 它要么是覆盖缺口，要么是真的漏捕获，
    而后者按约束 1 的口径应当 fail-fast 而不是静默。~~

    **★ 已定性（2026-09-04，T6.8）：两个猜测都不对，是第三种 —— 回放侧的守卫
    把它自己拦死了，而且是自我维持的。**

    根因是 `shard_replay.c` 里 R-P4-8 那道守卫：

    ```c
    if (!smgrexists(smgr, blk->forknum))
    {
        ereport(WARNING, ...「目标文件已不存在，跳过该记录」...);
        return;             /* 跳过本条 */
    }
    ```

    它是为「认领之后表被 DROP」写的（那时继续 redo 会撞不可捕获的 PANIC），
    判据却下在 **fork 粒度**上。follower 的壳表由 `CREATE TABLE (LIKE ...)` 建
    出来，**从来没有 `_vm` fork**；于是每一条 visibility map 记录都命中这道守卫
    被跳过，fork 也就永远建不出来 —— **跳过 ⇒ 不存在 ⇒ 继续跳过**，一个自我
    维持的空洞。这解释了为什么它跨 r1/l1 两套件**稳定复现**而不是偶发。

    对照原生：`heap_xlog_visible` 走 `CreateFakeRelcacheEntry` +
    `visibilitymap_pin` → `vm_readbuf` → `vm_extend` → `smgrcreate`，
    **redo 本来就会把缺失的 VM fork 建出来**。我们这道守卫抢在它前面返回了。

    **不是覆盖缺口**（记录捕获到了）、**也不是漏捕获**（它进了流），
    所以约束 1 的 fail-fast 口径在这里不适用 —— 该修的是守卫的粒度。

    **修法**：判据从 `blk->forknum` 收窄到 **`MAIN_FORKNUM`**。
    表真被 DROP 时主 fork 一样不存在，R-P4-8 的保护原样成立；而次级 fork
    （`_vm` / `_fsm`）缺失时放行，让内核 redo 按它自己的路子建出来。
    **本条修复不与 T6.8 的出口回归同批做** —— 它改的是回放 redo 路径，
    必须单独验证（r1/l1 那 8 条断言转绿）后再并入，不能在门禁跑到一半时换 .so。
16. **R-P6-7 逻辑解码打标记录 ⇒ 栈溢出、后端 abort、整节点重置**
    （2026-09-04，T6.6 实测，**已前移禁令遏制，根因未修**）：
    在有打标表的节点上 `pg_logical_slot_get_changes()` 实测：
    ```
    LOG:  starting logical decoding for slot "t66slot"
    *** stack smashing detected ***: terminated
    server process (PID …) was terminated by signal 6: Aborted
    ```
    ~~**机制**（高置信推断，未逐字节验证）：补丁 0005 在 insert/update/multi_insert
    的**主数据末尾追加 4 字节分片 xid 尾缀**……⇒ 拷贝越界 ⇒ 栈保护器 abort。~~

    > **★★ 这个推断是错的，2026-09-06（批次 #10）用栈回溯推翻。两处都错**：
    > ① 补丁 0005 的 4 字节走 `XLogRegisterData`（**主数据**），而
    >    `DecodeInsert` / `DecodeMultiInsert` 的元组取自
    >    `XLogRecGetBlockData`（**块数据**）—— 解码器的长度算术根本碰不到它；
    > ② 崩溃也不在 heap 解码，栈回溯指向 **`xact_decode()`**。
    >
    > **真根因：构建产物的 ABI 撕裂，不是任何补丁的设计缺陷。**
    > 补丁 0007 给 `xl_xact_parsed_commit` / `_abort` **加了三个字段**
    > （`nshardxids` / `shardxids` / `shard_commit_ts`），结构因此变大。而整棵
    > postgres 构建树里有 **1218 个 .o 编于 `xact.h` 改动之前** ——
    > `decode.o` 的日期是 **6 月 1 日**、`xact.h` 是 **8 月 13 日**。
    > 于是 `xact_decode` 在栈上按**旧的小结构**分配 `parsed`，
    > 而 `xactdesc.o` 里的 `ParseCommitRecord` 头一句就是
    > `memset(parsed, 0, sizeof(*parsed))`，按**新的大结构**清零 ——
    > 正好打穿调用方的栈帧，`__stack_chk_fail` abort。
    > `xlogrecovery.o`（同样用这个结构）也在陈旧之列。
    >
    > **处置**：`make clean` + 整树重编 + install。同一份数据、同一条解码，
    > 重编后 `count = 20` 正常返回，**不再崩溃**；陈旧 .o 归零。
    > 修好的 `bin/postgres` 已同步回仓库。
    >
    > **禁令保留**，但**理由换回原文**：§10 说的是「解码按原生 xid 组事务」——
    > 分片表用分片 xid，解码出来的事务分组在语义上就是错的。
    > 那个理由与崩溃无关，独立成立。此前把禁令写成"遏制一个会打掉整节点的
    > 崩溃"，现在看是**给一个不存在的设计缺陷做的遏制**。
    >
    > **顺带查出禁令有洞**（未修，已登记 R-P6-14）：禁令挂在
    > `ShardGuardCheckPlan`（ExecutorStart），只覆盖 **SQL 调用**。
    > `pg_recvlogical` 走 walsender 的 START_REPLICATION，**不经执行器**，
    > 绕得过去；本次复现也正是靠"写入时打标、解码前撤标"绕开的。
    **严重性**：一次普通 SQL 调用能让后端崩溃、进而 postmaster **整节点重置**；
    且这不限于解码分片表本身 —— 解码器会解析流里**所有**记录。
    **§10 的禁令实装在错的层**：补丁 0006 把守卫放在可见性层
    （`HeapTupleSatisfiesHistoricMVCC`），而溢出发生在解码阶段，到不了那里。
    **本次处置**：把禁令**前移到入口** —— 白名单非空时
    `pg_create_logical_replication_slot` / `pg_logical_slot_*_changes` 一律拒绝
    （`shard_guard.c` 的禁用函数表）。这是遏制，不是修复：在解码器学会那个尾缀
    之前，唯一能保证的是**不触碰腐坏路径**。
    **已闭**（2026-09-06）：根因是构建产物陈旧，clean rebuild 后消失；
    禁令保留、理由回归 §10 原文。**遗留纪律**：**改内核补丁后必须整树
    `make clean` 重编** —— 补丁给共享结构加字段时，增量 make 的头文件依赖
    并不可靠（本次实测 1218 个 .o 没被重编），而症状是**运行期栈踩踏**，
    离改动点极远、极难归因。
17. **R-P6-8 §9.2 第 3 层禁用清单整层可绕：`SELECT * FROM f(...)`**
    （2026-09-04，T6.6 顺带查出，**已修**）：
    禁用清单从 T4.6 起就只拦得住 `SELECT f(...)` 这一种写法；换成
    **FROM 形式**一律放行。实测 `SELECT * FROM citus_rebalance_start()`
    绕过禁令、一路跑进 Citus 重平衡器（最后因无关原因才报错）；
    `SELECT * FROM pg_logical_slot_get_changes(...)` 同理直达 R-P6-7 的腐坏路径。
    **根因**（已在仓库源码坐实，非推断）：首版靠扫 `pstmt->rtable` 里
    `RTE_FUNCTION` 的 `rte->functions` 取 funcexpr，而 `PlannedStmt` 的 rtable
    是 setrefs.c 造的**扁平副本**，`add_rte_to_flat_rtable()` 明写
    `newrte->functions = NIL;`（`postgres-src/src/backend/optimizer/plan/setrefs.c:554`），
    执行器改从 `FunctionScan` **计划节点**的 `functions` 字段取。
    于是那个循环是**死代码**，从来没命中过一次。
    **发现路径值得记**：它是被 T6.6 的两条断言"意外"钉出来的 —— 当时以为是
    断言写法问题（先撞上原生 `wal_level >= logical` 检查），换个非解码类禁用
    函数做判别才发现是整层失灵。**一条测不过的断言，先怀疑被测对象**。
    **修复**：`ShardGuardCheckPlan` 改为**遍历计划树**
    （`shard_guard_walk_plan`）：每个节点只取 targetlist / qual 交给
    `expression_tree_walker`（节点本身绝不交出去，否则撞
    `unrecognized node type` —— 见 T4.6 首版栽过的坑），
    `FunctionScan` 额外取其 `functions`，并递归
    lefttree/righttree/Append/MergeAppend/BitmapAnd/BitmapOr/SubqueryScan/CustomScan
    与 `pstmt->subplans`（覆盖 CTE 与 initPlan）。
    **覆盖边界（如实记）**：连接节点专属的 joinqual、索引 quals 未覆盖 ——
    禁用项都是顶层 UDF 调用，不会长在那些位置。
    **验证**：`test_negative_p6.sh` 新增 4 条 —— FROM 形式、子查询套一层、
    CTE 三种绕法各一条，外加一条**阴性对照**（`generate_series` 必须仍放行，
    防"全禁"式假通过）。27/0。
18. **R-P6-9 分片打标状态在共享内存里只增不减 ⇒ 无关表的 DROP 被误拒、夹具残留累积**
    （2026-09-04，T6.3b/T6.4 回归时查出，**已于 T6.8 修复**）：
    `shard_pagecmp_p1` 48/1、`ddl_fileset_d1` 77/1，两条红都不是判据退化，
    而是**夹具残表**：`p1_3way` 的行数一轮轮翻倍（122 → 244 → 366）。
    直接原因在日志里：
    ```
    ERROR:  不支持对含分片打标表 DROP 的事务执行 PREPARE TRANSACTION
    CONTEXT:  while executing command on localhost:5433
    ```
    分布式表的 `DROP` 走 2PC，撞上 §10 那条既有禁令（PRE_PREPARE 钩子里的
    `ShardClogHasPendingDrops()`），于是删不掉、行数累积。
    **判据链**：`DropStmt` → `shard_oid_is_mvcc(cfg, relid)` → 先查 GUC 白名单，
    白名单不中再查 **`ShardXidCtl->mvcc_set`（共享内存集合）**。而那个集合
    **只由 `partdist_set_shard_mvcc()` 添加，DROP 时从不移除** ——
    OID 一旦被复用，一张毫不相干的新表就被当成分片打标表。
    **已证的事实**（不是推断）：重启全部节点（=清空该共享内存）后，
    两套件立刻回到**精确基线** `49/0` 与 `78/0`，二进制未变。
    **与 T6.3b/T6.4 无关**：两者都不碰 DROP 分类路径；同一份二进制上，
    重启前红、重启后绿。
    **修复（T6.8，2026-09-04）**：`ShardMvccSetRemove(Oid)`，在 `ShardClogAtCommit()`
    里与文件 GC 同一处调用 —— DROP 提交时把 OID 从 `mvcc_set` 摘掉。
    集合"只进不出"的禁令**只针对运行中的表想退出打标语义**
    （`partdist_set_shard_mvcc(..., false)` 仍然拒绝）；**DROP 是另一回事**，
    表已经不存在了。而且这本来就是那条禁令自己给出的理由 ——
    它的 errhint 原文写着「DROP TABLE 会连同水位/clog 文件一并清理」，
    此前**只清了文件、没清这个集合**，承诺与实现对不上。
    **顺带留下的操作纪律**：成批跑验收之间重启节点仍是好习惯 ——
    共享内存里还有别的只进不出的状态（如分配器槽位，R-P6-4）。
    这一条的误判方向值得记住：症状是"行数对不上"，第一反应"回放写多了"
    是**错的**，真因在一条与回放毫无关系的禁令上。
19. **R-P6-10 门禁的前置条件只对个别套件生效 —— "顺序依赖冒充前置"的第二、三例**
    （2026-09-04，T6.8 实测，**已修**）：
    T6.7 的 `pre_suite` 已经点破过这个毛病（原话："run_p5_exit.sh 靠把它垫底 +
    前面重启协调者绕过，那是顺序依赖，不是前置"），但**只给 `tso_si_p3` 一套
    做了前置**。同一毛病还有两处：
    - **TSO 纪元**：`boot_blocked` 是 postmaster 启动时 stat 一次写进共享内存的。
      协调者一旦重启（本期为了装内核补丁 0010 重启过多次），此后**所有要分片
      xid 的套件**全线红。实测 `shard_xid_p1` **18/27**，27 条红全是同一句
      `TSO 检测到上个纪元的 boot 标记（pg_tso_boot），拒绝发号` —— 与被测内容
      毫无关系。T6.7 那轮能过，只是起跑时协调者恰好还没服务过。
      **修**：纪元复位提到批次净场（`rotate_logs` 里"全停"与"拉起"之间删标记，
      这是全批唯一所有节点都停着的时刻）+ 每套件按需复位。
      判据不能用 `partdist_tso_status()`（见 R-P6-12，库里没这函数），也不能只看
      标记在不在（正常服务过的实例自己也会写标记）；精确判据是
      **标记 mtime 早于 `pg_postmaster_start_time()`**。
    - **遗留模式**：`shard_xid_p1` 的"严格模式读/写被拦截"只在 `tso_conninfo`
      为空时成立 —— T3.6 已把 strict 收紧为"无 ts 读 / 无 gxid 写才拦"，
      TSO 一配置就自动取号、strict 全放行。实测 41/4，四条红同源（含
      "负向用例未破坏数据"，因为那条 INSERT 真写进去了）。
      **修**：`pre_suite shard_xid_p1` 显式清空该 worker 的 `tso_conninfo`。
    **共同教训**：门禁里每一条"这套件需要环境处于某状态"的隐含前提，都必须写成
    显式动作。写不出来的，就是下一次"换个顺序就红"。

20. **R-P6-11 孤儿 Citus 2PC 无人清理 ⇒ 中断的批次永久堵死该 worker 的 DDL**
    （2026-09-04，T6.8 实测，**已在门禁净场层处置**）：
    中断一批验收（`pkill` runner）**不会**结束它已经发出的 SQL —— 协调者后端
    仍卡在 Citus 内部等 worker，留下 `citus_*` 的 2PC 未决事务。而 §9.2
    **已经关掉 Citus 原生 2PC 恢复**，于是没有任何人会来清它们。
    实测：worker1 上留了 4 笔，`shard_identity_p0` 建引用表那一步挂满 600s
    超时（0/0 早退）；清掉之后同一套件立刻回到基线 **10/0**。
    **现场指纹**：`pg_blocking_pids()` 返回 **0** —— 等的是一个**没有后端**的
    事务。这一条值得单独记住,它把"谁在阻塞我"这个问题指向了 `pg_prepared_xacts`
    而不是 `pg_stat_activity`。
    **处置**：净场层 `purge_orphan_prepared()`，只回滚 `citus_%`
    （协调者没提交就等于没发生，本次核对过 `pg_dist_shard` 里确无对应表）；
    **本方案自己的 DTX 一概不碰** —— 它的判决在 raft 里，只能由恢复守护按决议
    闭合，验收脚本无权替它做主。

23. **R-P7-1 从未被供给的组成员照样能当选，R-P4-15 挡不住**
    （2026-09-04，批次 #7 实测，**已于批次 #9 修复**）：
    R-P4-15 拦的是「收到了分区 WAL 却没有回放槽位」—— 判据是
    `get_follower_applied_part_lsn(loid) > 0`。可**从来没被供给过**的成员连字节
    都收不到（没有壳表 ⇒ shard_identity 里没有本地 OID ⇒ 字节归不了档），
    那个值恒为 0，守卫的 `IF coalesce(bound,0) > 0` 判否，**直接放行**。
    实测：三成员组里只供给了一个 follower，杀掉 leader 之后选中的是**没有任何
    数据**的那个陪跑节点，新主上连表都不存在。
    **性质**：这不是"读到旧数据"，是"读到空表" —— 比 R-P4-15 拦的那一格更糟。
    **暂行纪律**：**成员集应当跟着真实副本走 —— 先供给，再进组**；
    `provision_shard_replica()` 让这条纪律第一次成为可执行的动作。
    **修复（批次 #9）**：`pg_raft_promote_prepare` 里 `loid IS NULL` 原本无条件
    `RETURN 1`（注释写"本节点没有该分片，无事可做"）—— 那个假设是错的：入参就是
    **这个组自己的**分片，拿不到本地 OID 只有一个含义，就是本节点没有副本。
    改为 `RETURN -1`（永不放行）并在 WARNING 里指出正解：先
    `partdist.provision_shard_replica()` 把副本建起来再参选。
    验收 `test_handover_provision_p7.sh` **29/0**（新增两条：阴性前提"该节点
    确实没有此分片" + 升主前置返回 -1）。

22. **R-P6-13 门禁仍有跨套件干扰：同一二进制上"批次红、单跑绿"**
    （2026-09-04，T6.8 实测，**已于批次 #9 补上收敛等待**）：
    净场层补到四层（纪元复位 / 遗留模式 / 孤儿 2PC / 协调者侧清残表）之后，
    仍有三套在批次里红、重启后单跑全绿：`follower_replay_r1` 30/18 与 36/12
    对 **58/0**、`offnum_guard_p6` 16/9 对 **25/0**、`locmap_base_p6` 22/1 对 **23/0**。
    现场提示反复出现「分区主副本可能已切换，请经路由层重试」与
    `local_partition_for_shard()` 取空，指向**分区组主权在批次里持续抖动**。
    **危险在于它是双向的**：它既会把好的判成红（本期三例），也可能把红的盖成绿
    —— 后者更糟，因为没人会去查一条通过的断言。
    **处置（批次 #9）**：`scrub()` 末尾加 `wait_groups_settled()`。稳定的定义取
    两条可直接观测的：每个节点上**只剩 group 0**（上一套的数据组残留是主权抖动
    的直接来源）、且 **group 0 的 leader 各节点一致且非零**。等不到只告警不阻塞
    —— 带着告警跑好过静默地把抖动算进结果。
    **注意它不是万灵药**：套件**自己**建组时的竞选竞态它管不着（那要靠夹具让
    placement 主抢跑，见批次 #7/#8 的记要）。
    在那之前的口径：**批次数字用于发现问题，单跑数字用于定性**；
    任何一条批次里的红都必须重启后单跑复核才能下结论 —— 本期三例全靠这条纠正
    了误判（其中一次差点把 `_vm` 修复错判成引入了回归）。

24. **R-P6-14 逻辑解码禁令只覆盖 SQL 调用，walsender 路径绕得过去**
    （2026-09-06，批次 #10 顺带查出，**未修，已定性**）：
    T6.6 的禁令挂在 `ShardGuardCheckPlan`（ExecutorStart 挂点），只看得见
    **SQL 语句**。`pg_recvlogical` / 物理复制客户端走的是 walsender 的
    `START_REPLICATION SLOT ... LOGICAL`，**根本不经执行器**，禁令看不到。
    另有一条更窄的绕法：禁令的触发条件是**本节点白名单非空**，而 WAL 里的
    分片 xid 是**写入时**就带上的 —— "写入时打标、解码前撤标"同样绕得过去
    （本次复现正是用它）。
    **严重性已降**：崩溃根因（R-P6-7）已查明是构建产物问题并修掉，所以绕过去
    不再打掉节点；但 §10 的**语义**理由仍然成立 —— 解码按原生 xid 组事务，
    对分片表是错的分组。绕过去 = 拿到语义错误的变更流。
    **未闭**：要真堵住得在 `output_plugin` 或 `LogicalDecodingProcessRecord`
    层面加判据（本节点有打标表就拒绝解码），属内核补丁面。

21. **R-P6-12 扩展 SQL 的声明面与运行集群的实装面有 10 个函数的差集**
    （2026-09-04，T6.8 审计，**已于批次 #9 归零**）：
    `sql/pg_partdist--1.0.sql` 声明 82 个函数，tx2 集群 worker 上实装 111 个，
    但**声明了却不存在**的有 10 个：`partdist_tso_status`、`partdist_tso_start_ts`、
    `partdist_tso_commit_ts`、`partdist_tso_heartbeat`、`partdist_tso_client_*`、
    `partdist_gxid_next`、`partdist_join_global_txn`、`partdist_global_safe_ts`、
    `partdist_set_shard_mvcc`。
    成因不是 SQL 文件有问题，而是**运行集群是增量搭起来的**：扩展 SQL 只在
    `CREATE EXTENSION` 时执行一次，之后新增的声明谁也不会自动补上
    （本期 T6.3b/T6.4 的三个新函数就是手工 `CREATE OR REPLACE` + `ALTER
    EXTENSION ADD` 装进去的）。抽查还发现 `partdist_tso_start_ts` 只在**协调者**
    上、且落在 **`public`** 而不是 `partdist` 模式里。
    **影响**：一次干净 clone 的 `CREATE EXTENSION` 会得到完整的 82 个，所以
    **交付物本身没问题**；但**任何依赖"SQL 文件里声明了就一定能调"的脚本都会
    在本环境上翻车** —— T6.8 的门禁判据一开始就踩了这个坑。
    **修复（批次 #9）**：`scripts/refresh_extension_sql.sh` 抽出 SQL 文件里的
    **C 函数声明**（`CREATE OR REPLACE` + 幂等 + 不依赖表数据）重放到集群，
    并入门禁净场每轮起跑前跑一次。实测差集 **10 → 0**。
    plpgsql/sql 函数与建表语句一概不碰 —— 那些可能依赖执行顺序或已有数据，
    重放的风险大于收益。

---

*本文与 TX_TSO_MVCC_DESING.md 配套使用；方案性问题以设计文档为准，本文只管
"怎么做、什么顺序、怎么验收"。每期出口时回填本文勾选项与跟踪表。*
