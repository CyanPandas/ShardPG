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

- **P2**：EnhancedClog 改域"每来源节点→每分片"并扩 globalXID 列（§5.3）；
  T1.6 临时表原位替换；`shard_xidmap.*` 摘除；无主 RUNNING 认领（恢复期）；
  ANALYZE/vacuum 可见性分叉（§8-⑤ 的读侧部分）。
- **P3**：Proxy 粘性路由 + 会话映射；TSO 内存计数器 + boot 防呆（§2.4）；
  发号即登记 + GlobalSafeTs 双通道/租约/栅栏（§6.2）；安全网切严格模式；
  §4.4 冲突中止规则生效。
- **P4**（前置：§9.1 三实验 + §9.2 门禁用例 + pg_raft 解冻）：协调者选定（首写分片）；
  globalXID 分配器；commit_ts 两时机；`dtx_master_pre_record_commit` 搬迁；
  连接加入协议 `partdist_join_global_txn`；三态问询；崩溃矩阵演练。
- **P5**：vacuum 三变量 + 前缀规则 + 页面三类动作 + 洁净位图 + CTRL 水位落地 +
  回卷两阶段护栏（§6/§7 全章）。
- **P6**：切主全链路（含 §6.6 切主认领、§5.5 水位新语义）联测；全部负向用例并入
  门禁；文档终检。

---

## 4 未决核实点跟踪表

| 编号 | 内容 | 归属 | 状态 |
|---|---|---|---|
| V1 | 原生表 hint 位 vs pagecmp 既有处理（§4.5） | T1.0 | ✅ 已完成（P1_PRECHECK 结论 A） |
| V2 | Citus 连接建立点枚举完备性（§9.2 ①） | P4 前 | 未做 |
| V3 | 引用表使用现状与只读裁定（§9.2 ②） | P4 前 | 未做 |
| V4 | CIC/CLUSTER 分叉 vs 禁用裁定（§11） | P2 期间定 | 未做 |
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

---

*本文与 TX_TSO_MVCC_DESING.md 配套使用；方案性问题以设计文档为准，本文只管
"怎么做、什么顺序、怎么验收"。每期出口时回填本文勾选项与跟踪表。*
