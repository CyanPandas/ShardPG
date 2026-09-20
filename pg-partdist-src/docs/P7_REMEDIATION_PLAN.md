# P7 补足计划：把 P6 审计出的缺陷逐条收口（2026-09-09）

> **这份计划怎么来的**：`P6_EXIT_AUDIT.md`（2026-09-06 全项目缺陷盘点）+
> `P6_PENDING_DECISIONS.md`（2026-09-07 待裁清单，含演示实跑新撞出的 7 条）。
> 本文在 **2026-09-09 逐条复核代码之后**成文 —— 审计给的是"当时的现状"，本文
> 给的是"**今天的现状 + 接下来怎么补**"。复核结论见 §1 的"09-09 复核"列。
>
> **执行环境**：容器 `pg-test-container`（1 coordinator + 8 worker），工作区
> `~/shardpg-test-work`，分支 `shardpg-test`。**不碰 `shardpg-TX2` 与演示材料**
> （见根目录 `PG_TEST_ENV.md`）。
>
> **一句话结论**：P6 没有结项，而且**结不了**——三条 ❌ 里有两条是"出口动作
> 没做"（全量没跑、裁定没下），第三条是文档没对齐（本次已补，见 §5）。真正的
> 产品缺陷有 **22 条**，其中 **4 条是"已提交的数据在切主后不可见/丢失"级别**，
> 必须先修，且都在 pg-partdist 侧、不需要解冻 raft。

---

## 0 口径与优先级依据

排序只用一个判据：**这条缺陷会不会让已经提交成功的数据变得不正确或不可见**。

| 级别 | 定义 | 处置 |
|---|---|---|
| ★★★ | 已提交数据在正常运维动作（切主/供给）后**不可见或丢失**，且不自愈 | 批次 1，先于一切 |
| ★★ | 集群进入**不可用/不可恢复**状态（节点停摆、资源耗尽、分片永久失去 HA） | 批次 2 |
| ★ | 限制面/守卫面有洞：能绕过去，绕过去就踩上面两级 | 批次 3 |
| ◐ | 出口动作、门禁、文档 | 批次 4 |
| ○ | 生产化缺件（自动启动器、截断），有手工替代 | 批次 5 |
| ⏸ | 需要解冻 raft 或属独立专项 | 批次 6，须用户裁定 |

---

## 1 缺陷总账（**22 条**产品缺陷 + 8 条工程债；R-P6-22 于 2026-09-09 查证时新增）

### 1.1 ★★★ 数据正确性（批次 1）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **R-P6-15** | **2PC 阶段 3 的 COMMIT 标记不带分片 xid**：`dtx_participant.c:467` 在 `COMMIT PREPARED` 里调 `PartWALBuildMarkerPayload()`，而该函数按 `ShardXidXactCount() > 0` 决定带不带分片 xid 尾（`partwal_sync.c:723-727`）——`COMMIT PREPARED` 跑在另一个没碰过分片表的事务里，计数恒 0 ⇒ 24 字节旧格式 | **✅ 已修（2026-09-09，T7.1）**：判决标记改由判决落账那一刻补发（`dtx_pending.c` `dtx_replicate_verdict()`）。验收 `test_dtx_verdict_marker_p7.sh` **34/0**，决定性取证：副本 `st=2 cts=13` 与 leader 的 `cts=13` 逐字节相同（修复前恒为 st=1） | 副本分片 clog 对每笔跨分片事务**永远停在 PREPARED**；决议被 FORGET 回收后 `dtx_close_indoubt` 四级全落空 ⇒ **切主后 2PC 提交的行在新主上永久不可见**（09-06 实跑取证） |
| **R-P6-17** | **物理基线不搬分片 clog**：`shard_fileset.c` 全文零处引用 `pg_shard_clog`/`ShardClog`，`shard_baseline_emit` 只灌页面 + 抬发号水位 | **✅ 已修（2026-09-09，T7.2）**：新 CTRL 子类型 `PARTWAL_CTRL_SHARD_CLOG`，基线按 256 槽/块切、只发有内容的块；follower 原样落进本地 clog（键是分片 xid，与 oid 无关）。验收 `test_baseline_clog_p7.sh` **18/0**（2026-09-10 在从零重建的干净集群上复跑）：**先写数据、后供给**的姿势下，副本对基线**之前**的 sxid 判 `st=2 cts=3`，与 leader 一致（修复前恒 st=0） | 在已有数据之后才供给的副本，对基线之前提交的分片 xid **没有判决**；升主后那些行是 RUNNING ⇒ 被 `shard_claim_on_promote` 改判 ABORTED ⇒ **丢行** |
| **R-P6-16** | **升主不发 `FILESET_UPDATE`**：`PartDistRoutePromote()`（`shard_fileset.c:1838`）只做"角色 + 捕获"两件事，不广播新主的 relfilenumber | **✅ 已修（2026-09-09，T7.3）**：`PartDistEmitFilesetHandover()` + 新标志位 `PARTWAL_FSUPD_PRIMARY_HANDOVER`（只重绑、不截断、不发 FPI）。验收 `test_promote_handover_p7.sh` **35/0**（2026-09-10 连跑两遍复核，两遍同数） | 其余副本 locmap 仍对着旧主文件号 ⇒ `replay_catchup` 报"未知 relfilelocator" ⇒ **切主一次，该分片其余副本全部失去再次当选资格**，直到从新主重新供给 |
| **R-P6-21** | **供给/升主不携带"打标身份"**：`shard_fileset.c`/`raft_boundary.c` 均无 `ShardMvccSetAdd`；`mvcc_set` 只由 `partdist_set_shard_mvcc()` 或重启扫目录装载 | **✅ 已修（2026-09-09，T7.4）**：升主时按持久证据（`pg_shard_clog/<oid>` 存在）继承打标身份。验收同上，实测新主写入 xmin=4（分片 xid，非原生大 xid） | 升主后的新主若未事先手工加白名单又未重启，**写入不打标、读走原生路径** —— `handover_provision_p7` [3b] 与 `promote_catchup_tx3` [4] 都是在这个状态下通过的，**通过的原因是错的** |

| **P7-P1**<br>（2026-09-12 新登记） | **on-access 剪枝的豁免判据漏掉"回放壳表"**：补丁 0005 在 `heap_page_prune_opt()` 开头对分片关系直接 return，但判据是 `shard_relation_xid_hook`（= 在 `partition_map` 里**登记过打标**）。**没打过标**的分布表，其副本壳表不在这道豁免里 —— 而 R-P6-21 的打标身份继承靠的是"`pg_shard_clog/<oid>` 存在"，没打标的分片压根没有这个目录，继承不到 | **✅ 已修（2026-09-13，T7.26）**，**未动内核补丁**。<br>根因：回放把 leader 的元组原样落到本节点页面上，元组头里是**外来** xid；一条普通 `SELECT` 触发的 on-access 剪枝拿**本机 clog** 去判那些外来 xid 的死活，判成 dead 即就地清掉、还写本地 WAL（副本从此分叉）。补丁 0005 的剪枝豁免只认"打过标"的关系，没打标的分布表副本壳表不在其中。<br>修法：把"回放槽位里配过对的壳表主堆 + 其 TOAST 表"也认成受管关系（`ShardReplayProtectedRel`，无 catalog、按代次缓存），在补丁 0006/0008 已有的 `HeapTupleSatisfiesVacuumHorizon` 钩子里对它们的元组一律判**不可回收**（未删⇒LIVE、被删⇒RECENTLY_DEAD）。剪枝、VACUUM、建索引扫描从此一条都删不掉；回收推迟到 R4 或重做基线（代价是膨胀，不是正确性）。TOAST 表 OID 经旁路文件 `protect_toast` 交给判活路径（判活不许碰 catalog）。<br>★ **验收 `test_replica_prune_guard_p7.sh` 先复现后修复**：修复前一条 `SELECT count(*)` 就把副本页 0 从 46 个正常行指针剪成 **0 正常 / 33 dead**（33 行全丢、大字段读不出、与 leader 逐字节 DIFF）；修复后 **39/0**，同一场景关掉守卫（`pg_partdist.replica_prune_guard=off`）仍被剪 —— 证明"没变"是守卫挡的。夹具关键：leader 半满页永不触发自身剪枝、副本壳表单设 `fillfactor=10` 抬高触发线、副本本机 xid 计数烧过回放号。<br>**仍未覆盖**：R4 写路径（升主后对这些行做 UPDATE/DELETE 仍走本机语义，见 `shard_route.h`）。 | 已提交数据在**正常读取**时被就地清掉，不自愈 |

### 1.2 ★★ 可用性 / 资源（批次 2）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **R-P6-19** | 回放 worker 泄漏描述符：`exceeded maxAllocatedDescs (328)` | **✅ 已修（2026-09-09，T7.6）**：真因不是 `AllocateDir` 少配对，而是**回放主循环的段文件 fd 在 ERROR 时没人关** —— 调用方 `replay_worker.c` 用 `PG_TRY/PG_CATCH` 接住错误**并在同一事务里继续**（"追平失败不能拖垮 worker"），于是 `AtEOXact_Files()` 永远轮不到执行，每失败一次漏一个 fd。改法：主循环包 `PG_FINALLY` 收 `cur_fd`，跨 longjmp 的局部量标 `volatile`。**无直接用例**（要制造 300+ 次被 worker 接住的失败），靠代码走查 + 机制说明 | 该节点此后**所有回放失败**，直到 worker 重启 |
| **R-P6-20** | TSO 客户端 RPC 用裸函数名：`tso_client.c` 5 处发 `SELECT partdist_tso_start_ts(...)`，而函数装在 `partdist` 模式 | **✅ 已修（2026-09-09，T7.5）**：5 处改全限定名。修前在 pg-test 环境实测复现（`function partdist_tso_start_ts(integer, bigint) does not exist` → `TSO 不可达或拒绝服务`）；修后默认 `search_path` 下取号 1→2→3→4 单调，日志零 `does not exist` | 协调者默认 `search_path` 不含 `partdist` ⇒ 取号/心跳/commit_ts/safe_ts 全部 `function does not exist` ⇒ **全簇分片写 fail-closed**。演示环境靠 `ALTER DATABASE ... SET search_path` 绕过 |
| **R-P6-4** | 分配器槽位 64/节点无回收 | **✅ 已修（2026-09-09，T7.7）**：`ShardXidSlotRelease()` 挂在 DROP 提交时点（与 `ShardMvccSetRemove` 同源），槽位与影子一并归还。验收 `test_slot_reclaim_p7.sh` **5/0**：70 轮「建表→写入→DROP」全过（修复前第 65 轮必报"槽位用尽"） | 建删 64 个分片后该节点**再也建不了分片**；门禁靠"移走水位文件 + 重启"规避（`run_p6_exit.sh:165-170` 直接 `mv`） |
| **P7-D1** | leader DROP ⇒ 副本侧静默 | **✅ 已修（2026-09-10，T7.8）**：验收 `test_baseline_clog_p7.sh` 在**从零重建**的干净集群上 **18/0**，副本日志直接取证 `shard 17530 收到 leader 的 DROP 通知，停止回放并摘除槽位`，1 s 内 `armed=f`。实装如下：新 CTRL `PARTWAL_CTRL_SHARD_DROP`；leader 侧在"fileset 里有、catalog 里没了"时发射（日志实测有 `分片 … 已 DROP，已通知副本停流`），follower 收到即 `ShardReplaySetArmed(false)` 停流摘槽位、**刻意不删壳表与目录**（删表是 DDL，回放侧不代替运维做，回收因此可审计）。**真因已定位（2026-09-10，改正 09-09 那句"疑为时序"的猜测）**：发射器**只挂在 `COMMIT PREPARED` 之后**。那一支覆盖的是 Citus DDL（worker 上一律走 2PC），而**普通 `DROP TABLE` 根本没有发射点** —— 我 09-09 在夹具注释里写的"没有 2PC 就走 PRE_COMMIT 那条常规路径，两条路径发同一条 CTRL"是错的：PRE_COMMIT 上只有 fileset 发射器，没有 DROP 扫描。实测佐证：leader 日志里对当轮刚删的分片零命中，只有后来某笔事务顺手给**残留旧 fileset** 补发的那几条。<br>**修法：提交后惰性补发。** 三个看起来更自然的挂点都不成立 —— ① DROP 语句执行完那一刻事务还没提交，仍可回滚，副本一旦停流就再也追不上（这正是 §12 把 fileset 发射放在 PRE_COMMIT 的理由）；② `PRE_PREPARE` 同理，`ROLLBACK PREPARED` 还开着口子；③ `XACT_EVENT_COMMIT` 那一刻已退出 ProcArray、快照不作数，而扫描要走 `BuildShardFileSetEx()` 的 syscache，且该回调里 ERROR 会升成 FATAL。于是拆成两步：提交回调只做一次 **shmem 原子自增**（`DemuxState->drop_notice_gen`，无 IO、不查 catalog、不会 ERROR），真正的目录扫描推迟到本节点**下一条语句**开头。代次放 shmem 而不是后端局部变量，是因为**删表的连接往往当场就断了**，局部标志会随它一起消失。gen/swept 一对计数器用 CAS 配对，保证一代只扫一次（写成"每后端记一份已消费代次"实测会让同一条通知在 1 ms 内被 4 个后端各发一遍）。<br>**发射器的容错也一并改正**：原来只是 `PG_TRY/PG_CATCH + FlushErrorState`，那**不会**把事务恢复成可用状态（PG 里只有子事务能）。挂在 `COMMIT PREPARED` 之后时勉强不出事（后面没别的事要做了），一挪到任意用户语句开头就现形 —— 整条用户语句被带挂，实测夹具里 `replay_set_locmap` / `replay_enable` 全线报 "relation does not exist"。触发条件还特别普通：本节点留着某个已删分片的 fileset，而本节点**不是那个分区组的 leader**。现在每个分片一个内部子事务，判据（`LoadShardFileSet` / `BuildShardFileSetEx`，两者都能抛）也包在里面；失败日志从 WARNING 降为 LOG —— 扫描是搭在别人的语句上跑的，把它的失败推给一个毫不相干的客户端是错的。<br>**★ 副本侧还有一个更隐蔽的真 bug（2026-09-10 实测）**：DROP 的 CTRL 没有载荷（opcode 自己就是全部信息），而回放里有一条排在 CTRL 分派**之前**的 `data_len == 0 → 旧流占位记录，跳过` 兼容分支，把整条通知吞掉了。现象是 leader 日志白纸黑字写着"已通知副本停流"，副本却一直 `armed=t`，而且**连个报错都没有**，只有一行看着像历史遗留的 WARNING。这正是 T6.1 立的"未知记录一律 ERROR、绝不静默跳过"要防的形态，只是它从"占位记录"这个后门溜了进来。判据改成 flags（`PartWALRecordIsCtrl`）而不是"有没有 payload"。<br>⚠️ 回收链条只闭合到"副本停流"这一格；协调者侧那张分布表仍然删不掉，另登记为 **P7-D3** | 壳表 + 回放槽位 + `pg_parwal/<oid>` **永不回收**（回收判据是"OID 不在本地 `pg_class`"，而壳表是本地真表） |

### 1.3 ★ 限制面 / 守卫面（批次 3）

| ID | 缺陷 | 09-09 复核 | 影响 |
|---|---|---|---|
| **P7-G1** | §9.2 第 3 层禁用清单**漏 8 个同类 Citus UDF**：`citus_split_shard_by_split_points`、`isolate_tenant_to_new_shard`、`citus_drain_node`、`master_move_shard_placement`、`master_copy_shard_placement`、`replicate_table_shards`、`citus_schema_move`、`alter_table_set_access_method` | **✅ 已修（2026-09-10，T7.9，`bed624f`）**：8 个同类 UDF 补进 `shard_guard.c` 清单，实测 Citus 13.1 里都存在且都是 C 函数。前提 R-P6-22 同批先修（闸门改集群级），否则补了名字在协调者上也不触发。<br>（09-09 复核原文：未修，清单实为 7 个 citus UDF + 5 个逻辑解码入口） | 它们同样亲手搬/读分片数据，绕过去就是静默错读或与 raft 放置冲突。**注意补名字只堵入口，不堵通路**（§3.1），而且在补名字之前先得让守卫在协调者上真的生效（R-P6-22，§3.2） |
| **P7-G2** | **引用表运行期写：零守卫、零断言**，DESIGN §10 那行还挂着"【需核实现状】" | **✅ 已修（2026-09-10，T7.10，`bed624f` + `3a89e10`）**：挂 `planner_hook`，按原始 Query 的 `resultRelation` 查 `pg_dist_partition.partmethod='n'`；协调者上 INSERT / UPDATE / DELETE 全拦，SELECT 放行（`negative_p6` 含阴性对照）。★ 第一版挂 `ExecutorStart` 判 `resultRelations`，被 Citus 改写成 CustomScan 绕过，实测**完全不生效**。覆盖边界：worker 上的 `<ref>_<shardid>` 分片不在 `pg_dist_partition` 里，看不见（DESIGN §10）。<br>（09-09 复核原文：未修，`shard_guard.c` 零命中 `reference`） | §10 写"建表后只读"，实际拦不住 |
| **R-P6-18** | MARKER 的 `start_ts` 是墙钟（`partwal_sync.c:733` `GetCurrentTransactionStartTimestamp()`），不是 TSO start_ts | **✅ 已修（T7.11）**：新增标志位 `PARTWAL_MARKER_STS_IS_TSO`，消费侧只认带位的值，不带位一律落 0（遗留模式） | R-P3-2「双 ts 宇宙串线」成真：升主后 §4.2 三态处置拿它与 TSO 快照比，**恒为"跳过"** |
| **P7-G4**<br>（2026-09-12 新登记） | **`commit_ts` 没有对应的宇宙标志位**，与 R-P6-18 是同一件事的另一半 | **✅ 已修（2026-09-13，T7.29）**。<br>**根因比登记时更具体**：leader 本地的账在遗留模式下 commit_ts 一律是 **0**（`TsoStashedCommitTs` / 0007 提交记录 / DTX 决议 ts），唯独 MARKER 这一条路（`TsoMarkerCommitTs`）遗留模式填**墙钟** —— 同一笔提交 leader 记 0、副本记墙钟。<br>**修法**：①MARKER 新增 `PARTWAL_MARKER_CTS_IS_TSO`（0x0008），值来自 TSO 才置（`TsoMarkerCommitTsEx` 告知宇宙；判决标记按 commit_ts>0 置）；②回放落分片 clog：不带位落 0（与 leader 同一本账）；③gclog：**原值照存**（`gclog_status` 与 tx2/r2 的"commit_ts 非 0"断言不受影响），`status` 第 8 位记 `GCLOG_STATUS_CTS_IS_TSO`，读写 API 屏蔽该位；R3 的 `gvis_committed` 对不带位的 commit_ts 按 0 比较。不带位的历史标记/历史槽按遗留语义（对一切快照可见）—— 方向是"不让行消失"。<br>★ 与 09-12 那次"改成返回 0"的区别：那次改的是**值**（打破诊断面与既有断言），这次改的是**来源标记**，只改变判定。<br>**验收 `test_cts_universe_p7.sh` 先复现后修复**（已入门禁）：leader 遗留模式写 40 行 → 杀 leader、副本当选追平 → 前提断言 gclog commit_ts=842636643536110（墙钟量级）、遗留读者读到 40 行 → 给新主配上 TSO（快照号 1）再读：修复前 **0 行（21/1）**，修复后 **40 行（22/0）**，判决与 gclog 原值不变。<br>**覆盖边界（如实）**：分片 clog 路径（打标分片，回放落 0）在遗留模式下**无直接用例** —— 现有比对副本/leader 分片 clog 的套件全部配了 TSO，它们证明的是"TSO 宇宙的值未被改动"；遗留模式落 0 与 T7.11 的 start_ts 处理同构。<br>**不改的一处（已知边界，写进 TX_TSO_MVCC_DESING §10）**：vacuum clog 截断判据把"COMMITTED 且 commit_ts=0"判为 commit-ts-too-new 停下 —— 中途启用 TSO 后，遗留模式留下的提交会挡住截断前缀（少清不错清）。"TSO 模式提交永不存 0"这条前提未逐路径证明、截断不可逆，故不放宽。<br>**回归**（G4 单独的二进制，门禁 11 套 504/6，批次红逐套单跑复核）：`tso_si_p3` 39/0、`dtx_tso_p4` 批 48/1 → **单跑 49/0**、`shard_vacuum_replay_p5` 64/0、`follower_replay_r1` 58/0、`lazy_replay_l1` 57/0、`dtx_replay_tx1` 批 79/4 → **单跑 83/0**、`dtx_commit_marker_tx2` 40/0（副本 gclog "commit_ts 非 0" 断言仍过 —— 原值照存的设计由此得证）、`promote_catchup_tx3` 24/0（R3）、`replay_spin_p7` 22/0、`baseline_clog_p7` 18/0。`dtx_verdict_marker_p7` 的"判决标记 flags"断言按设计由 7 改为 15（多 CTS_IS_TSO，与 T7.11 时 3→7 同一类演进）；它与 `promote_handover_p7` 在脏环境里被 P7-D3 残表拖红，重建数据目录后（G4+V4+D3 合并二进制）分别 **38/0、35/0**。`txn_layer_r2` 恒 51/1（丢提案 :5433+3 > 额度 2），**A/B 证明与本条无关**：不含 G4 的 HEAD 上同一套 49/3、同一签名 —— 另登记 P7-T8 | 只在中途改 TSO 配置时发作：已提交的行永久不可见 |
| **R-P6-22** | **禁用清单在协调者上根本不生效**：`ShardGuardCheckPlan` 的快门是 `ShardGatingActive()`（本节点 `shard_relids` 非空 或 shmem `mvcc_n>0`），而打标表长在 worker、协调者两者皆空 ⇒ 只在协调者上调用的 `citus_rebalance_start` / `citus_drain_node` / `undistribute_table` 等**一条都不触发**。`negative_p6` 全绿是因为夹具用一个 worker 上的表 OID 给协调者开了闸门（`:64-66`） | **✅ 已修（2026-09-10，`bed624f`；夹具拐杖 `97632ce` 撤除）**：禁用清单拆两张 —— Citus 运维 / 搬运类改走集群级闸门 `ShardGuardClusterManaged()`（= `pg_raft.raft_enabled`），逻辑解码入口仍按打标判据。`negative_p6` 撤掉给协调者设白名单的两行、改为断言"协调者白名单为空"后 **40/0**：`citus_rebalance_start` / `SELECT * FROM` 形式 / `citus_drain_node` / `undistribute_table` 四条全部触发，修复前全部静默放行。<br>（09-09 登记原文：新发现，实测三节点白名单全空） | §9.2 第 3 层禁用整层在生产形态下熄火 —— 比"漏 8 个名字"严重得多。见 §3.2 |
| **P7-G3** | DESIGN §10 缺 4 行、错 1 行 | **本次已补**，见 §5 | — |

### 1.4 ◐ 出口动作 / 门禁（批次 4）

| ID | 事项 | 09-09 复核 |
|---|---|---|
| **P7-E1** | **31 套全量在最新二进制上一次没跑过**（`run_p6_exit.sh` `SUITES` 实数 31；最近一次全量是 T6.8 的 28 套 1191/31，其后批次 #7–#11 改了回放 redo、路由层、`promote_prepare`、整个 PG 二进制、发号起点） | **◐ 分段跑完，未一次跑完**：T7.12（2026-09-11/12）分四段 ≈1595 条 / FAIL 17，全部定因清零（§2「T7.12 收口」）；口径已由 31 扩到 **47 套**（OPS 8 套 + P7 新套件）。不分段全量属出口动作，**2026-09-13 用户裁定暂缓**（宿主机无 swap，见 P7-E7） |
| **P7-E2** | OPS 8 套按 3 节点布局写，9 节点上会停错节点且不复原 | **已裁定：改造进门禁**（2026-09-09）。拓扑无关化后并入 `SUITES`，口径 31 → 39 套。T7.13 |
| **P7-E6**<br>（2026-09-10 新登记） | **同一个毛病出现在 P7 自己的新套件里**：`test_promote_handover_p7.sh` [5] 杀掉旧主之后**从不复原**。实测后果是后面每个套件都被毒化 —— 紧接着跑的 `test_slot_reclaim_p7` 第一条断言就死在"节点 :5433 可连"，而那和它要验的槽位回收毫无关系，很容易被误判成"槽位回收回归了" | **已修（2026-09-10）**：复原挂进该套件的 `cleanup()`（EXIT trap，中途 Ctrl-C 也能复原），并在**杀之前**先登记 `KILLED_NODE`。纪律：**停节点的套件必须自带复原**，这条同样适用于 T7.13 的 OPS 8 套改造。<br>⚠️ 光复原**还不够**：该套件的不可重复有**两个**独立原因，另一个是旧主上留下的壳表删不掉（P7-D3），已一并改成每轮换表名。**验收：干净集群上连跑两遍，两遍都 35/0**（2026-09-10）——只补一个的时候第二遍必红 |
| **P7-E3** | 三个基线消费者 e2e：初始配对 ✓、永久分叉 ✓、**快路径分叉 ✗**（`test_fastpath_divergence_tx4.sh` 只验到 `promote_prepare` 返回 -1，无重做基线步骤） | **✅ 已补（2026-09-11，T7.14）**：`test_fastpath_divergence_tx4.sh` **20/0**，取证 `p=1`（重做基线后 `promote_prepare` 由 -1 转正值，副本重新获得参选资格）。<br>★ **补测试的过程挖出并修掉了一个真缺陷** —— 见 T7.14 |
| **P7-E4** | `replica_gate_p6` **没有施加 anti-wraparound 压力**，验的是"relfrozenxid 同步 + 闸门拦截" | **已裁定接受**（2026-09-09）：采用 T6.3c 改写后的命题，出口清单原文改掉，本条关闭 |
| **P7-E5** | R-P6-13 拓扑抖动"等不到只告警不阻塞"，批次里带着"60s 未收敛、存疑"跑完 | **✅ 已改判据（2026-09-11，T7.16）**：取消"存疑"这个中间态 —— 等待 60s→180s 且其间主动清残留数据组；仍不收敛则该套件记 **BLOCKED 且不执行**，与 FAIL 分开统计，并同样让门禁不通过 |

### 1.5 ○ 生产化缺件（批次 5）

| ID | 事项 | 09-09 复核 |
|---|---|---|
| **P7-V1** | 分片 vacuum **无自动启动器**，到龄只 WARNING（`shard_xid.c:1381`） | **✅ 已做（2026-09-12，T7.17）**：`partdist.shard_vacuum_auto()` 把三步（算目标 → 趟页面 → 截断）串成无人值守版本，由 TSO 心跳工作者按 `pg_partdist.shard_vacuum_auto`（默认 on）自连触发。判据与阶段 1 护栏同源（`age >= shard_vacuum_max_age`），不会出现"警告了却不动手"。验收 `test_shard_vacuum_auto_p7.sh` **29/0**，含"不手工调、心跳 2 s 内自己把截断点从 6 推到 9 + 节点日志留痕"与"开关 off 时同样条件下一动不动、同一时刻手工调用仍能推进"两条对照。<br>★ **顺带补掉一格**：`ShardVacuumRecover()`（§6.5「趟完未截断」）此前**只有手工入口**，崩在那一格的分片要一直挂到它到龄才有人管 —— 自动启动器现在把这一格也一并捞起来（与龄无关，补一次几乎零成本的截断）。<br>★ **实测暴露的依赖**：截断放行判据是 `commit_ts < GlobalSafeTs`，而 commit_ts 在写入那一刻定宇宙 —— **先写数据后配 TSO**，clog 里存的是墙钟，拿去和 TSO 号比恒不放行，detail 一路 `commit-ts-too-new`、截断点一步不动。这就是 **P7-G4** 在 vacuum 路径上的样子，用例头注释已写明"TSO 必须在写入之前上线" |
| **P7-V2** | 分片 vacuum **无尾部截断**（`shard_vacuum.c` 无 `smgrtruncate`/`RelationTruncate`） | **✅ 已做（2026-09-12，T7.18）**：三类页面动作干净收尾后，`shard_vacuum_truncate_tail()` 把尾部连续空页经 `RelationTruncate()` 还给文件系统（一并处理 FSM/VM 并发 `XLOG_SMGR_TRUNCATE`，副本侧靠 `ApplySmgrRecord` 原样回放 —— 补丁 0001v2 专门保证 RM_SMGR 这类"无块引用"的记录也会被捕获进分区流）。阈值照抄内核 vacuumlazy.c（1000 块或 1/16）。GUC `pg_partdist.shard_vacuum_truncate` 默认 on。实测 **116 → 58 块、3000 行一条不少**。<br>★ **与内核不同的一处取舍**：抢不到排他锁就**让路、下一轮再说**（`ConditionalLockRelation`，绝不等待）—— 截断是纯空间回收，推迟无代价，而在 vacuum 路径上阻塞用户查询有代价。拿到锁后**必须重数一遍**：第一遍是在 ShareUpdateExclusiveLock 下数的，并发写入完全可能刚在尾页插了一行。<br>★ **实测踩过的坑（第一版就错在这里）**：空页判据不能用 `PageIsEmpty` —— 它要求 `pd_lower` 退回页头即**一个行指针都不剩**，而三类动作清完一页留下的是一排 `LP_UNUSED`，行指针数组多半还在。症状是 `removed_dead=3000`（元组确实删干净了）、`pg_relation_size` 却一块没少，**而且连一条 DEBUG 都不打**（早退路径是静默的），看起来就像"截断压根没接上"。改用内核 `count_nondeletable_pages` 的"逐个 `ItemIdIsUsed`"。 |
| **P7-V3** | 两处覆盖缺口：clog 整段删除分支；停在页面循环中间的崩溃 | **✅ 已做（2026-09-12，T7.19）**。<br>**故障注入点**：GUC `pg_partdist.shard_vacuum_fault` = `off`（默认）/ `mid_prune`（页面循环跑到第 2 页时中止）/ `after_mark`（落完「趟完」标记、截断之前中止）。判据是一次整型比较，热路径上等于不存在；只在 `shard_vacuum_*` 这一族里生效。**抛普通 ERROR 而非 PANIC** —— 两态的判据是「标记有没有落盘」，而标记走的是带 fsync 的水位文件，事务中止即与真崩溃等价，不必真把节点打死。<br>**为什么非要开这个口子**：§6.5 的两条恢复路此前**没有任何用例能走到** —— 那两个时刻都在一个 C 函数内部，从 SQL 面够不着。没人走过的恢复路径，等于没有。<br>**验收**（并入 `test_shard_vacuum_auto_p7.sh`）：<br>· 两态之一（`mid_prune`）：中止后**标记一个都没落下**（水位原样），关掉注入点后整趟重来一次就干净（`true\|3000`），数据完好；<br>· 两态之二（`after_mark`）：中止后水位呈 `tb < vx`，`shard_vacuum_recover()` 返回 `truncated`、两水位重新相等；**"只补截断、不重跑页面趟"的取证** = 紧接着再跑一整趟必然一条都删不动（`true\|0`）—— 删得动就说明上一趟其实没清完、标记是假的；<br>· clog 整段删除分支：段 = 2^20 个 xid，靠真发号跨段要烧一百万个、跑不起；前置门禁（趟完才许截断）已由两态之二单独验过，故直接把「趟完」标记抬到段边界之上，只驱动那个 unlink 循环 —— 实测删掉 ≥1 个整段、段文件数下降，且**截断点之下的老数据仍可见**（免查隐式冻结区语义）。<br>★ 三节都必须在**心跳关闭**下跑：它们验的是 sweep 内部，场上只能有一个清扫者。开着跑过一轮，心跳抢先清完并把水位推到 5/5，两条断言连带红而原因与注入点毫无关系 |
| **P7-D3**<br>（2026-09-10 新登记） | **半删的分片会把那张分布表锁死在协调者上，永远删不掉**。在 worker 上直删分片表（运维绕过 §10 的常规手法，T7.8 夹具用的也是它）之后：① 协调者侧 `DROP TABLE` 走 Citus 2PC，被 §10「含分片打标表 DROP 禁 PREPARE」拦下 —— 拦的是**另一个**还留着壳表的副本；② 绕到各节点本地删（`enable_ddl_propagation=off`）同样不行 —— 那个分区组这时往往已经因为主副本的表先没了而凑不齐多数派，`PartWALAppendCtrl` 的 propose 直接失败（实测 `组 102008 propose plsn=58 失败 … record 58 未达多数派`）。于是那张表**两条路都走不通**。<br>与 P7-D1 是一体两面：D1 管的是"副本知不知道分片没了"，D3 管的是"分片没了之后那张分布表还能不能删掉"。发通知修好了不等于回收闭环了 | **✅ 已修（2026-09-14，T7.31）**。<br>**根因比登记时更深一层**：锁死不是"半删"造成的，半删只是运维被逼出来的绕法。真正的根因是 PRE_PREPARE 里**一刀切禁止含分片打标表 DROP 的事务 PREPARE**（理由：DROP 后的文件回收——clog 目录、水位文件、打标集合、分配器槽位——记在本进程内存里，COMMIT PREPARED 在别的会话执行，结算不了）。可 Citus 把分布表的 DROP 以 2PC 下发到每个 placement ⇒ **任何打标分布表都无法从协调者删除**；T7.4 升主继承身份后撤白名单也没用；夹具里 `DROP ... IF EXISTS >/dev/null 2>&1` 把报错吞掉 ⇒ 下一轮 `already exists` ⇒ 沿用旧表连锁红（`dtx_verdict_marker_p7` / `promote_handover_p7` 实测，见 T7.29 回归）。<br>**修法**：删除该禁令；at_prepare 钩子把挂起的回收清单写进分片 2PC 段（`RegisterTwoPhaseRecord` info=`SHARD_2PC_INFO_DROPS`，排在"无分片写即返回"之前）；postcommit 回调据清单执行 `ShardClogGcDropped()`（与本地提交路径共用，从 `ShardClogAtCommit` 抽出）；postabort / recover 对该 info 什么都不做。已知边界同非 2PC 路径：崩在 COMMIT PREPARED 记录与回调之间 ⇒ 孤儿文件，注册前清目录兜底 OID 复用。<br>**先复现后修复**：修复前在真实残表上 `DROP TABLE t71_mk` ⇒ `不支持对含分片打标表 DROP 的事务执行 PREPARE TRANSACTION`（四节点白名单全空）；修复后同批 4 张残表全部删除、节点日志 6 条"COMMIT PREPARED 回收"、无残留 prepared 事务。<br>**验收 `test_drop_mvcc_2pc_p7.sh` 34/0**（已入门禁）：[A] 单 worker 本地打标表 PREPARE→ROLLBACK PREPARED 不回收、PREPARE→另一会话 COMMIT PREPARED 跨会话回收、PREPARE→**重启节点**→COMMIT PREPARED 仍回收；[B] 协调者 DROP 2 分片打标分布表成功、两 placement 回收干净、BEGIN/DROP/ROLLBACK 对照不回收、同名重建成功。<br>`test_shard_clog_p2.sh` 原负向断言"含分片 DROP 的事务禁 PREPARE"改为正向（可 PREPARE、提交前目录在、COMMIT PREPARED 后回收）—— 首次在新二进制跑旧断言时 PREPARE 成功留下悬空事务、紧跟的 DROP 等锁到套件超时，已在同段结算。<br>**回归（V4+D3 合并二进制，门禁 13 套批次 412/12，4 套红逐套复核）**：`shard_xid_p1` 45/0、`shard_gating_p4` 20/0、`shard_baseline_p6` 45/0（基线末尾发 SHARD_MVCC）、`negative_p6` 40/0、`divergence_mark_p8` 21/0、`dtx_replay_tx1` 83/0、`raft_groups_p7` 11/0、`partwal_ring_p7` 14/0（自动修复重做基线）、`slot_reclaim_p7` 5/0（70 轮建删走本地提交回收）；`shard_clog_p2` 负向断言随禁令解除改正向后 **66/0**、`dtx_convergence_p4` 批 39/6（partition_map 登记超时）→ **单跑 45/0**、`dtx_tso_p4` 批 46/3（找不到分片 leader）→ **单跑 49/0**、`handover_provision_p7` 批 31/2 → **33/0**（此前门禁里最不稳定的一套，D3 修后首次跑满）。另在干净集群上 `dtx_verdict_marker_p7` 38/0、`promote_handover_p7` 35/0 |
| **P7-V4** | 打标登记全手工（`partdist_set_shard_mvcc` 逐分片逐节点，无建表钩子/事件触发器） | **✅ 已做（2026-09-14，T7.30）**。<br>**实际形态比登记的更糟**：`partdist_set_shard_mvcc(regclass)` 按本地 OID 查 `partition_map`，而分布式流程写进去的 `partition_id` 是 Citus shardid ⇒ **对分布表恒报"没有登记行"**，只剩 GUC 白名单一条测试通道（组内每个成员各自 ALTER SYSTEM、列本节点 OID）。其后还藏着一个缺口：副本"这是打标表"的持久证据 `pg_shard_clog/<oid>` 只在回放到带分片 xid 的 MARKER 时才建 ⇒ **刚打标、还没写就切主，新主继承不到身份**，写入悄悄走原生路径。<br>**修法**：①协调者 `partdist.set_table_shard_mvcc(regclass)` 逐 placement 下发 worker 侧 `partdist.shard_mvcc_register(shardid)`（shard_identity 翻本地 OID；拒绝副本壳表——副本进打标集合会让 vacuum 自动启动器动副本文件；首次登记要求主堆 0 块；登记 = 水位文件 + 打标集合 + 本地证据目录）；②新 CTRL `SHARD_MVCC`(0x06)：登记时发、物理基线末尾也发（`ShardRelIsMvcc` 为真时，白名单打标同样生效）；副本收到只建证据目录、不进打标集合，身份仍按 T7.4 升主时继承；③观测点 `partdist.shard_mvcc_status(oid)`。不做建表钩子：打标后禁 CREATE INDEX，建表即打标会让用户建不了索引，时机应由用户在"建完索引、写入之前"一次调用。<br>**验收 `test_shard_mvcc_register_p7.sh` 41/0**（已入门禁）：一条命令登记 2 分片表、leader `registered=yes evidence=yes`、两个副本 `registered=no evidence=yes`、重跑 already；副本上登记被拒；★★ **一行未写就杀 leader ⇒ 新主继承身份、写入 xmin 仍是分片 xid**；非空表拒绝；先登记后供副本 ⇒ 证据随物理基线到副本；对照（带"设白名单后无新基线"前提守卫）：旧白名单方式下副本**没有**证据。夹具教训：分区组须 3 成员（2 成员组杀主选不出新主、供副本途中凑不齐多数派会触发自动重做基线污染对照）。<br>回归与 T7.31 合并进行，见 P7-D3 行。 |

### 1.6 ✅ 独立专项（批次 6，2026-09-09 裁定做；**2026-09-13 六条全部落地**）

| ID | 事项 | 为什么单列 |
|---|---|---|
| **P7-R1** | Raft **成员变更无安全路径**（无 joint consensus）⇒ 连带 §9.2 禁掉全部 Citus 搬运器 ⇒ **集群拓扑事实上不可变更** | **✅ 已做（2026-09-12，T7.20）**：`partdist.pg_raft_group_change_member(gid, node, add)` 走 Raft 日志做成员变更。<br>**走的是论文 §4.1 的单节点变更，不做 joint consensus** —— 单节点变更下相邻两个配置的多数派**必然相交**（只差一个成员），安全性由此而来；joint consensus 只在需要原子换多个成员时才必要，实现面大一个数量级。在全簇共识层上这个取舍偏向能审查得过来的那一边，代价是"三换三"要做三次。<br>机制：新条目 `OP_CONFIG`；**append-time 生效**（等提交再生效不安全 —— 提交这条 CONFIG 本身所需的多数派会仍按旧集算，leader 侧因此必须重算 majority）；同时只允许一个未提交变更，于是未决状态只需 `pending_cfg_index` + 回滚目标；截断时回滚；提交后落注册表。<br>四道门禁：必须是 leader / 成员集必须已知 / 已有未提交变更即拒 / 一次只动一个、不许减到空。<br>验收 `test_raft_membership_r1.sh` **18/0**，关键断言不是"接口返回成功"而是**各节点对 cluster_size 的看法同步变化** —— "两套多数派定义"只有在这个观测面上才看得出来。<br>★ 实测踩到三处，全部记在 `pg-raft-src/docs/raft_module_revision_plan.md`：①注册表恢复路径（每后端静态量）会把日志成员集**无声覆盖**；②被移除的节点收不到"把它移除"的记录会一直竞选（论文 §4.2.2 的 removed server disruption）；③**把全局自旋锁压进提交路径导致选举抖动** —— 靠 `git stash` 做 A/B 对照才定因，`replica_gate_p6` 33/0 → 23/10 → 修回 33/0 |
| **P7-R2** | `RAFT_MAX_GROUPS = 32` 定长 shmem ⇒ 每节点 ≤ 31 个数据组；且"给一张分布表的全部分片建组 + 供副本"无自动化 | **✅ 已做（2026-09-13，T7.23）**。<br>①上限改为 GUC `pg_raft.max_groups`（默认 32，`PGC_POSTMASTER`，2–4096）：共享内存按它算尺寸（`RaftGroupTable` 末尾改柔性数组），`promote_prepare` 的本地截止期表改为首次使用时按上限分配。**不顺手把默认调高**：每组带一个 `RAFT_LOG_CAPACITY` 条的日志环，默认值翻倍就是每节点常驻内存翻倍，而本机 3.9 GB 跑 9 节点已经在饿死心跳（P7-E7）—— 该调的节点单独调。<br>**计划里写的前置"日志外部化"没有做，也不需要**：上限问题的本体是"编译期常量"，不是"日志在 shmem 里"；后者只决定每组的内存代价，那由调用方按节点规划承担。<br>②协调者上一条命令 `partdist.raft_replicate_table_shards(表, 副本数)`：固化已踩实的顺序「placement 先建组领先 3 s → 副本入组 → 等 placement 当选，落到副本上**只拆这个组**重来（不能用 `pg_raft_group_reset`，它清的是整节点）→ 供给前再确认组主 → leader 逐个 `provision_shard_replica`」，逐分片返回状态。<br>★ **首稿写成了"等 placement 当选之后再让副本入组"** —— 配置里已含副本，单个 placement 凑不够多数派，**永远选不上**；上机前对照 `test_handover_provision_p7` 的既有顺序查出，未进过集群。<br>★ **首稿还叫 `replicate_table_shards`，与 Citus 自带的搬运器同名**：§9.2 守卫按 `proname` 精确匹配、**不看模式**，会把它当 Citus 搬运器拦掉；不带模式调用则落到 `pg_catalog` 里 Citus 那个 —— 一个是功能不可用，一个是调到了被禁的搬运器。装上集群后查 `pg_proc` 发现同名，改名 `raft_replicate_table_shards`。<br>★ **首轮验收 9/2**：分组、选主、拆组重来都走通了（日志里有一次"组主没落在 placement，拆组重来"），但 4 个分片的供给全报「本节点没有分片」—— `provision_shard_replica` 第一步按 `shard_identity` 认主，而建分布表之后**没有任何路径自动登记分片身份**（与 P7-V4"打标登记全手工"同源）。`test_handover_provision_p7` 的夹具里手工调了 `rebuild_shard_identity()`，编排函数漏了。已补为第 ⓪ 步（每个 worker 一次，幂等）。<br>验收 `test_raft_groups_p7.sh` **11/0**（2026-09-13）：默认 32 且为 postmaster 级；一个 worker 调到 40 重启后建出 36 个组，默认节点在第 32 个被拒（对照）；4 分片表一条命令全部 `ok`（其中一个分片第 1 轮组主落在副本上、按设计拆组重来后成功），4 个副本全部 armed 并追平到 leader 位点 |
| **R-P4-13** | apply 侧决议登记缺失，仅 `dtx_peek` 绕行（`pg_raft--1.0.sql:146, 1349`） | **✅ 根因已定、已修（2026-09-13，T7.24）**。**根因不在 apply，在副本追加的去重**：`AppendPartWALRecordAt` 对 `plsn <= 本地最大编号` 一律当"重传"丢弃，**只看编号不看内容**。而某节点当 leader 时写了本地 DATA、提案没提交（失多数派 / 丢领导权），`discard_uncommitted_entry` 按设计**不截 parwal 字节**；新 leader 把同一个 plsn 分给了另一条已提交记录（实测是 DTX 判决），复制回旧 leader 时被当成重传吞掉 ⇒ **副本流里躺着一条从未提交的 DATA 冒充已提交的判决**，apply 按 plsn 读判决读到 DATA，决议行登记不上，且**一声不响**。实测现场：`:5433` 的 plsn=15 本地是 `flags=1 info=0`，raft 日志说它是 `flags=8 info=2`。<br>决议行缺失只是露出来的症状 —— 同一机制下被顶替的若是 DATA，**副本会回放一条从未提交的物理变更**。<br>修法（`raft_boundary.c` + `raft_consensus.c`）：编号已存在时逐字段核对（flags/info/rmid/orig_lsn/长度/字节）。不一致时按**本条是不是落在本节点 raft 日志末尾之后**分开处置：新追加 ⇒ 流里更靠后的必然是孤儿，截到 plsn-1 重写；重传 ⇒ **不截**，标记分叉交给重做基线、照常 ack。新追加走另起的 `partwal_follower_append_fresh` 入口（不给原函数加参数，避免滚动升级的重载歧义窗口）。<br>★ **第一版不分情形一律截断，实测打坏了重传路径**：一次重传 plsn=2 触发替换，把已提交已 ack 的 3..51 一并截掉，而 leader 按 match_index 不会重发 —— `dtx_replay_tx1` 83/0 → 76/7，改成分情形后恢复。<br>apply 侧同时补了诊断：插入 0 行时区分"本节点字节里读不出"与"读得出却插不进"，打 WARNING（此前 `SPI_OK_INSERT` 照样返回、零告警）。修前批次里该 WARNING 2 条 → 修后 0 条。<br>**批次复核（2026-09-13）**：`shard_baseline_p6` 45/0、`replica_gate_p6` 33/0、`promote_p6` 39/0、`dtx_convergence_p4` 45/0、`dtx_tso_p4` 49/0、`dtx_replay_tx1` **83/0**、`raft_membership_r1` 18/0、`logical_repl_guard_p7` 14/0；同轮节点日志孤儿替换 **7** 次（机制确实在起作用）、重传不一致 0、R-P4-13 诊断 0 |
| **P7-R3** | 物理基线**上限 1 GB**（`fileset_inline_max_blocks = 131072`，超出显式 ERROR）⇒ 大于 1 GB 的分片既不能供给也不能修复分叉 | **✅ 已做（2026-09-13，T7.21）**：流式基线。按块（GUC `pg_partdist.fileset_baseline_chunk_blocks`，默认 16384 块 = 128 MB）灌 FPI，**每块灌完即 `PartWALFlush` 排空捕获环并复制**，1 GB 硬上限只在 chunk=0（关闭流式）时保留。<br>★ **原上限的真实含义（纠正一处我先前的错误表述）**：FPI 先经 `wal_insert_hook` 写描述符进**全节点共享**的捕获环（8192 槽），`log_newpage_range` 每条 XLOG_FPI 带 32 块 ⇒ 一次灌完的基线占 `块数/32` 槽，**1 GB = 4096 槽 = 半个环**。环满时 `PartWALInsert` **只打 WARNING 就覆盖未消费条目** ⇒ 基线缺页、静默物理分歧。所以 1 GB 上限是"恰好卡在环容量一半、且只在无并发写入时成立"的偶然安全。我此前向用户说过"64 MB–1 GB 之间会静默丢 FPI"，那是按一块一条算的，**说错了**：安静节点上 1 GB 以内不会溢出，溢出要么需要并发写入分享同一个环、要么单次 > 2 GB。分块后环占用压到 `chunk/32` = 512 槽（≈6%），与基线总大小无关。<br>跨块并发写入仍物理一致：每张 FPI 是此刻全像，之后的修改 LSN 更大，回放按页 LSN 跳过已含进全像的修改 —— 与 PG 基础备份同理。<br>验收 `test_shard_baseline_p6.sh` [3][4] 改为：chunk=0 时仍按上限拒绝；chunk=2 块时一个多块关系成功发射并留痕"块流式发射"。**45/0**（2026-09-13 批次复核）<br>⚠️ 捕获环"满即覆盖只告警"本身是**潜伏缺陷**，见 §1.9 **P7-W2** |
| **P7-R4** | leader 任何一次 DDL ⇒ follower 停在结构栅栏，需人工等价 DDL + 重跑 `replay_set_locmap` | **✅ 已做（2026-09-13，T7.25）**：DDL 自动跟随。<br>①**leader**：成员结构变化时，在 `FILESET_UPDATE` 之前随流发一条新 CTRL `PARTWAL_CTRL_DDL_HINT`(0x05)，载荷是按 fileset ord 顺序排好的 `pg_get_indexdef_string` 清单。<br>②**副本回放**：回放进程没有 catalog，不在那里做 DDL —— 只把提示落成分片目录下的 `ddl_hint`，撞结构栅栏时把 leader 的新 fileset 落成 `pending_fileset`，照旧停在栅栏。<br>③**回放启动器**：每轮扫 `NEEDS_STRUCT` 且有 `pending_fileset` 的槽位，自连 backend 调 `partdist.replay_auto_follow(oid)`：按清单对账本地壳表（**多重集**匹配，去掉索引名后比对；多的删、缺的按清单顺序建）→ **按本地真实 OID 顺序再核对一遍与清单一致才配对**（ord 是 `RelationGetIndexList` 的 OID 序；顺序对不上就按 ord 配对等于把 A 索引的页放进 B 的文件，宁可回滚留在栅栏）→ `replay_set_locmap` → 清待办。每槽 5 s 限速，未跟上退避 60 s。开关 `pg_partdist.replay_auto_follow_ddl`（默认开，SIGHUP）。<br>两侧必须用**同一个**索引定义生成器（`partdist.indexdef_string` 包装内核 `pg_get_indexdef_string`）：SQL 的 `pg_get_indexdef()` 格式化参数不同，文本对账会把同一个索引判成不同、删了重建。<br>★ **首轮验收 65/22 全红在同一处，而且挖出一条旧缺陷**：重配调 `replay_set_locmap()` 时第 7 参 `base_part_lsn` 用了缺省 0 —— 0 是"从流起点开始"的**断言**（T6.2），本地关系非空即拒（`base_part_lsn=0 声明…但本地关系已有 1 个块`），副本上一有数据就永远跟不上，启动器每 60 s 重试一次。**人工恢复步骤一直带着同一个洞**：FRD §12.2 写的是"补齐本地结构 + 重跑 `replay_set_locmap()` 就能原地继续"，D1 [9] 也这么验、而且是绿的 —— 只因为它紧跟在 [7] TRUNCATE 后面，主堆恰好 0 块。修法：撞栅栏时回放进程把 `max(配对起效游标, 已落 checkpoint 游标)` 落成 `pending_base`（重配后 worker 重新认领本来就按这个 max 起跑，所以传它不改变行为；不能传 applied —— applied 与 durable 之间的页面修改未必落盘），新增 `partdist.replay_pending_base(oid)`，自动跟随与人工步骤都把它作第 7 参。D1 [9] 改为走这条正确步骤。<br>同轮还改了一处：重配后 worker 从 durable 游标重跑，会**再消费一遍**那条 `DDL_HINT` 把 `ddl_hint` 写回来；改为 `FILESET_UPDATE` 真正应用时由回放进程清掉全部旁路待办，免得下一次不带提示的栅栏拿到过期清单。<br>`test_ddl_fileset_d1.sh` 验的是"栅栏 + 人工恢复"，[0] 改为关掉自动跟随。验收 `test_ddl_auto_follow_p7.sh` **118/0**（2026-09-13）：CREATE INDEX / DROP INDEX 两个副本都无人工动作越过栅栏、locmap 5↔4 对、壳表索引确被建/删、待办文件清空、逐文件掩码外字节一致；开关 off 时停在栅栏、打开后自动跟上；[8b] 分块 VACUUM FULL（chunk=2）leader 分块排空 3 次、两副本不撞栅栏也不触发跟随、7 个文件逐字节一致。同轮 `ddl_fileset_d1` **85/0**（含"游标只越过 DDL_HINT 那一条"的收紧判据与人工重配走 `replay_pending_base`）<br>⚠️ **观察（未定因，不记缺陷）**：[8b] 首版一条事务写 3000 行，套件因此超 1500 s 预算；压到 150 行后这一步仍耗 79 s（≈4 条 WAL 记录/秒）。不是本批引入：`ddl_fileset_d1` 历次耗时 214–343 s、本批 244–306 s，没有变慢。数据组 raft 日志环 128 条（`RAFT_LOG_CAPACITY`）是嫌疑，未取证<br>**不覆盖**：①列变更 —— ADD/DROP COLUMN 不改 fileset、不触发结构栅栏，本通道根本不会被叫起；副本壳表的列定义跟不跟得上**本条没有验证，不作声明**；②TOAST 表的新增（如给没有 TOAST 的表加变长列）同样会触发结构栅栏，但提示里只有索引定义，本通道不会替副本补 TOAST。按代码读：`replay_set_locmap` 对本地找不到的 leader `(role, ord)` 直接 ERROR（`replay_worker.c` 配对循环），整个跟随事务回滚、留在栅栏等人工 —— **此情形未实测**；③提示缺失（升级前的 leader 发的栅栏）时返回 `no-hint` 留给人工 |
| **R-P6-14** | 逻辑解码禁令只覆盖 SQL，walsender `START_REPLICATION` 绕得过 | **✅ 已堵（2026-09-13，T7.22），没有动内核**。计划写的是"在 `LogicalDecodingProcessRecord` 加判据 = 内核补丁面"，实际不需要：复制协议上的逻辑解码**只有一个入口** —— `replication=database` 的 walsender 连接。<br>①`ClientAuthentication_hook`：本节点有打标表（`ShardGatingActive()`）时对 `am_db_walsender` 连接 FATAL；物理复制（`replication=true`）与普通连接不受影响。<br>②**已连着的**逻辑 walsender 认证钩子管不到 ⇒ 登记打标的那一刻（`ShardMvccSetAdd` 末尾）遍历 `ReplicationSlotCtl`，对逻辑槽的 `active_pid` 发 SIGTERM；它重连时由①接管。<br>③**逻辑解码围栏**（同日补）：DEV PLAN 第 24 条早记着一条"更窄的绕法" —— 禁令只在"本节点**此刻**有打标表"时成立，而 WAL 里的打标记录是写入时就带上的。打标 → 写 → 删掉最后一张打标表 → **跨越打标期存活的旧逻辑槽**重连，解码的正是那段 WAL。①②都拦不住它。修法：打标集合变空的那一刻（DROP 提交回调里的 `ShardMvccSetRemove`）记下 WAL 插入位点，持久化到数据目录 `pg_partdist_logical_fence`、启动时装回、只增不减；此后认证钩子与 SQL 面禁令在集合为空时改看"有没有逻辑槽的 `confirmed_flush` 早于围栏"，有则拒并点名该槽。新建的槽从当前位点起解码，不受影响。槽扫描只在真的调到解码函数/建逻辑连接时才做。<br>★ **顺带纠正一处理由**：首版认证钩子的报错正文与套件头注释写的是"4 字节尾缀让解码器越界拷贝、整节点重置"—— 那是 R-P6-7 当年的定性，2026-09-06 已查明崩溃是**构建产物 ABI 撕裂**、clean rebuild 后消失（`TX_TSO_MVCC_DESING.md` §10 那一行写着）。禁令成立的理由是**语义**的：解码器按原生 xid 组事务，对分片表是错的分组。已改。<br>为什么不在解码器里判：解码器里能做的只有"遇到打标记录报错"，那时 walsender 已经在读了，报错也只是断连 —— 效果同上，却要多维护一个内核补丁。<br>验收 `test_logical_repl_guard_p7.sh`：**20/0**（2026-09-13）：①② 14 条（[4] 临时把一个 worker 切到 `wal_level=logical` 建真实活槽，EXIT 复原）；③ 6 条 —— 删掉最后一张打标表后围栏立起、旧槽的协议连接与 SQL 取变更都被拒、**重启后仍被拒**（持久化）、删旧槽后放行、新槽可正常解码。同轮 `shard_gating_p4` 20/0、`negative_p6` 40/0（SQL 面解码禁令与打标表 DROP 路径未受影响）<br>**仍不覆盖**：测试通道 GUC 白名单 `pg_partdist.shard_relids` 被撤下时不立围栏（生产不走它） |

### 1.7 工程债（穿插在各批次里做，不单列批次）

`write_router.c` milestone 1.2 死代码仍编入并暴露为 UDF；`kill(pid,0)` 探活在回放侧
仍在用（raft 侧已因僵尸失真而弃用，两处口径不一）；改共享结构体后增量 make 不可靠
（R-P6-7 根因）~~无自动守卫~~ **已加守卫**（`scripts/sync_build.sh`，2026-09-13，见 §1.9 P7-W5）；`REPLAY_MAX_SHARDS = 64` 定长槽位；单事务 DROP > 16 张
打标表即 ERROR；`dtx_close_indoubt` 四级落空无告警面；GlobalSafeTs 被钉死无监控面；
根 `README.md` 仍是 2.0 三节点文档（**本次已补导读**，见 §5）。

### 1.8 2026-09-12 定因 17 条失败时新登记的缺陷（除 P7-G4 外全部已修；P7-P1 于 2026-09-13 T7.26 修复）

| ID | 缺陷与**具体原因** | 处置 |
|---|---|---|
| **P7-W1** | **仓库里的 `pg-install/bin/pg_waldump` 停留在补丁 0007 之前**。原因：`src/backend/access/rmgrdesc/*.c` 在 PG 的构建里被编译**两次** —— 一次进 `bin/postgres`，一次经 `src/bin/pg_waldump/` 下的符号链接**单独编一份**。补丁 0007 改的正是 `xactdesc.c`，而"改完同步 `bin/postgres`"的既定流程只覆盖了前者。后果不是报错，是**静静地少打一段注解**：版本号一样、能跑、`COMMIT` 记录里的 `; shard xids:` 就是不出现。`test_shard_clog_p2` 两条取证断言因此恒红，而产品侧一直是对的 | **✅ 已修**：容器内离线重编该前端二进制并同步回仓库（办法记在 memory `project-repro-pg-install`）；守卫 `check_pg_install_patched.sh` 新增 `need_str_in "bin/pg_waldump" "shard xids:"`，并把"补丁在不在要按**每个产物**分别验"写进注释 |
| **P7-T1** | **夹具与选举较劲，反而把环境搞坏**（`test_clog_hole_c4.sh`）。原因：旧版顺序是"按 placement 定主从 → 铺 fileset/locmap → 建组 → 主没落在 placement 节点就 drop 重来"。每重来一次就是一次选举；当选者一旦是另一个成员，切主链路（自治选举 → 上报 → group0 登记 → **落路由层**）就把 Citus 的 placement **迁到当选者身上**。等循环把 raft leader 逼回原节点，placement 已经留在对面 ⇒ 经协调者的 INSERT 被路由到对面撞"本节点不是该分区组的 leader"，对面又已置 `role=promoted` 直接拒绝回放 | **✅ 已修**：建组排到 fileset/locmap **之前**，**谁当选认谁**，等 placement 收敛到当选者之后再按收敛后的真相铺。12/2 → 15/0 |
| **P7-T2** | **夹具把"raft 组主"当成"能在本地读写这份分片的节点"**（`test_dtx_tso_p4.sh`）。原因：组主身份来自 raft 选举，而该节点本地那一份可能只是**副本壳表** —— 对它读写会被访问闸正确拒绝。实测 QNODE 选中 `:5435`（确是组主），PREPARE 落不下去、读者拿回的是访问闸的 HINT 而不是计数 | **✅ 已修**：候选除"是组主"外，再**当场探一次本地读**（那正是后续步骤要用的前提，与其推断角色不如直接验）。48/1 → 49/0 |
| **P7-T3** | **两处"吞错误"把前置失败伪装成产品缺陷**。① `test_shard_identity_p0.sh` 的 `DROP TABLE ... >/dev/null 2>&1`：净场刚全停全起时慢一拍的 worker 会让分布式 DDL 报错，分片表还在 ⇒ 剪枝自然剪不掉 ⇒ 报成 `demo shard rows survived prune`。② `test_dtx_tso_p4.sh` 的读者 `2>/dev/null`：一红只剩一句"实际取不到值"，"被挡住"与"查询报错"这两个方向完全相反却分不出来 | **✅ 已修**：①错误可见 + 重试 + 显式验"分片表真没了"（9/1 → 11/0）；②合并捕获，错误首行带进断言文本 —— 改完当轮就把访问闸那条真因照了出来 |
| **P7-T4** | **取证只认两种日志布局**（`test_dtx_convergence_p4.sh`）。原因：`setup-raft.sh` 写 `<datadir>/startup.log`、`reproduce-env.sh` 写 `<datadir>.log`，而**出口门禁净场用 `-l <datadir>/pg.log` 拉起节点** —— 第三种布局两个 glob 都不沾，表现为"单跑绿、进批红"，而它上面那条功能断言是 PASS 的（清扫确实发生了）。<br>顺带修掉一个潜伏 bug：`grep -c … \|\| echo 0` 在**计数为 0 时 grep 退出码是 1**，`\|\|` 也触发，变量拿到两行 `0\n0`，把 `[[ ]]` 打成 `syntax error in expression` | **✅ 已修**：按"数据目录下任何 `.log` + 同名 `.log`"一网打尽，别再按文件名猜布局；计数一律 `\| tail -1`。44/1 → 45/0 |
| **P7-T5** | **夹具地基绑在实现的欠账上**（`test_dtx_commit_marker_tx2.sh`）。原因：它取被 `ROLLBACK TO` 的子事务 xid 的办法是"在 follower 上 SELECT 壳表读 xmin"，理由写着「follower 的 SELECT 尚未接 gclog（那是 R3），所以行读得出来」。欠账一还，行就读不出来了 —— 而**读不出来恰恰是正确行为** | **✅ 已修**：取号挪到**写入时点、写者自己的会话里**（子事务读自己刚写的行必然可见），与判决逻辑、与读侧做到哪一步都无关。39/2 → 40/0 |
| **P7-E7** | **宿主机无 swap，Raft 心跳被饿死导致自发选举**。原因：总内存 3.9 GB、`Swap: 0`，9 节点常驻 + 长会话之后可用内存一度只剩 122 MB。表现是**批次里随机某套因"主漂"全红，单跑必绿** —— 2026-09-12 实测 `dtx_replay_tx1` 批次 73/10 / 单跑 83/0，`promote_catchup_tx3` 批次 21/3 / 单跑 24/0 | **环境事项，非产品缺陷**。处置：宿主机加 2 GB swap（需 root）。在那之前，**批次里的主漂红必须用单跑复核后才能定性**，不得直接当产品缺陷记账 |


### 1.9 2026-09-13 批次 6 施工中新登记的缺陷（W2–W7/T6/T7/T8/T9/T10/P2/R5 **全部已修**）

| ID | 缺陷与**具体原因** | 处置 |
|---|---|---|
| **P7-W2** | **捕获环满时只打 WARNING 就覆盖未消费条目**（`partwal_sync.c` `PartWALInsert`）。原因：它跑在 `XLogInsert` 的钩子里，既不能等也不能排空，于是写死了"8192 槽正常负载下不会满"的假设。被覆盖的那条记录从此不在分区流里 —— 对 DATA 是**副本静默缺一条物理变更**，对 FPI 是基线缺页。T7.21 查 1 GB 上限来历时撞见 | **✅ 已修（2026-09-13，T7.27）**。三层：①**背压** —— 补丁 0005 的写路径钩子（heap 插入/更新/删除开头，不在临界区、不持 buffer 锁）在环占用过 `pg_partdist.partwal_ring_high_water`（默认 50%）时就地 `PartWALFlush`；单个大事务再也写不爆环。②**记账** —— 真被覆盖（不经写钩子的批量写入，如大表 VACUUM）时把分区号记进 shmem，下一次排空给它 `ShardMarkDiverged`；受影响分区超上限则对全部捕获分区打标。③**自愈** —— 心跳工作者按 `pg_partdist.auto_repair_interval_s` 自动调 `repair_diverged_shards()` 重做物理基线（每分片包子事务，原先裸 `PG_TRY`+`FlushErrorState` 在半路 ERROR 后不放锁/pin 继续跑，自动周期化后必须收干净）。观测面 `partdist.partwal_ring_stats()`。<br>★ 顺带修 `PartWALAbort`：事务中途排空过（背压/基线分块）时 `already_on_disk` 要认 `partwal_my_flushed_lsn`，否则前面已进流的 DATA 不补 ABORT 标记、gxid 永远无终态。<br>验收 `test_partwal_ring_p7.sh`：单事务 6000 行（≈12000 记录）关背压→覆盖发生+打标+自愈；开背压→零覆盖、背压排空发生、无新标记、12000 行齐。 |
| **P7-W3** | **仓库里的 `pg-install/include/.../access/xlog.h` 缺补丁 0010 的原型**。原因与 P7-W1 同一类：补丁 0010 同时改了 `xlog.c` 和 `xlog.h`，而"改完同步回仓库"只同步了 `bin/postgres`，**头文件是另一个产物**。扩展里 `replay_worker.c` 调 `XLogRequestInsertPositionAdvance` 因此一直是**隐式声明** —— 在 x86-64 上 64 位参数恰好按寄存器传过去所以能跑，换 GCC 14（隐式声明默认报错）直接编不过 | **✅ 已修**：头文件补上原型并同步进容器；守卫 `check_pg_install_patched.sh` 新增 `need_grep "0010 … 原型（xlog.h）"` |
| **P7-T6** | **健康检查把守卫报错正文里的 "PANIC:" 当成节点崩溃**（`lib_node_health.sh`）。原因：崩溃判据是日志里 grep `PANIC:`，而 R-P4-20 守卫的 `DETAIL` 正文里恰好引用了这个词。一次批次里报出 4 次"崩溃"，节点其实都活着 | **✅ 已修**：只认日志**级别位**上的 PANIC（`UTC [pid] PANIC:`）与 `terminated by signal 11/6/4/7` |
| **P7-T7** | **门禁净场的"扩展函数面对齐"只重放 `LANGUAGE c` 函数**（`refresh_extension_sql.sh`）。原因：抽取用的 awk 以"遇到行尾分号即结束"切块，plpgsql 函数体里满是分号，于是整类函数被跳过 —— 新增的 `raft_replicate_table_shards` / `replay_auto_follow` 在已建好的节点上**永远不会出现**，依赖它们的套件会报"函数不存在"，看起来像功能没做 | **✅ 已修**：awk 识别 `$tag$` 美元引用，引用内的分号不切块；抽出块数 78 → 96 |
| **P7-W4**<br>（2026-09-13 新登记） | **leader 顶层 `ROLLBACK` 之后，副本物理页面与 leader 分叉，且回放"成功"不报错**。原因：原生 WAL 对 INSERT **不论提交/回滚都照写**（回滚只追加 ABORT 记录，堆元组留在页上、之后才被标 dead），所以原生备库页面恒与主一致；而 pg_partdist 的捕获环在 `PartWALAbort` 里把本事务未排空的槽位**直接丢弃** —— 那些被回滚的元组在 leader 页面上是物理事实（`heap_insert` 不因回滚撤销页面改动），副本却收不到，页内行号出现断档，之后同页再插入就错位。<br>**实测取证**（`tests/probe_abort_gap_p7w4.sh`，1 分片 + 2 副本）：leader `CHECKPOINT; BEGIN; INSERT 5 行; ROLLBACK; INSERT 5 行(提交)` 后，副本 `replay_catchup` 返回 applied=13、state=idle、**零报错**，而副本主堆 **0 块** vs leader 1 块 10 个行指针，PK 索引 8192 vs 16384，逐字节 DIFF。**已提交的后 5 行在副本上丢失，且无任何告警** | **✅ 已修（2026-09-14，T7.32）**。<br>**机理比登记时多查清一层**：丢的不只是回滚元组本身。CHECKPOINT 之后页上第一条插入带"建新页"标记，若恰好属于回滚事务，之后**已提交**的插入在副本上找不到这一页，redo 当作页面不存在静默跳过 ⇒ 已提交行丢失（修复前实测副本主堆 0 字节）。<br>**修法**：`PartWALAbort` 不再丢弃未排空槽位，调排空主体 `PartWALFlushImpl(上界, 不写 COMMIT 标记, 不复制)` 把它们写进分区流，再照旧补 ABORT 标记。三条约束：先 `XLogFlush`（流不超前于已落盘 pg_wal）；连同 LSN 不超过上界的他人槽位一起排（避免同分区 plsn 与 orig_lsn 倒挂）；中止回调里合成 gxid 取节点号禁读 catalog（新增 `partdist_nodeid_catalog_forbidden`，只认侧影文件）。失败不抛（中止回调 ERROR 升 FATAL）：丢弃槽位 + 给相关分区打分叉标记，交心跳重做物理基线。<br>**当初"与 2PC / R-P4-13 强耦合"的担忧怎么落地的**：2PC 事务在 PRE_PREPARE 就已排空，ROLLBACK PREPARED 不走本路径；中止路径不做复制，字节随同分区下一次写入的区间式复制送出，重传与孤儿替换仍由 T7.24 的追加去重处理 —— 与 group commit 下"他人顺手把我的槽位写进流、之后我回滚"的既有路径完全同构，没有引入新的流形态。<br>**验收 `test_abort_page_p7.sh` 先复现后修复**（已入门禁）：修复前 **31/11**，修复后 **97/0**（[A] 回滚后同页提交 [B] 跨页 + TOAST [C] 副本 gclog=aborted [D] 并发同分区回滚 [E] ROLLBACK TO SAVEPOINT [F] 挪走侧影文件制造排空失败 ⇒ WARNING + 分叉标记 + 心跳自动重做基线后一致）。比对判据：非主堆的 IDENTICAL_EXCEPT_PDLSN 按 tx1 先例放宽（回放未写过的 TOAST 索引元页，修复前后形态相同；重做基线后连它也对齐，印证了原因）。<br>**回归（W4 二进制，门禁 22 套：回放 / 2PC / TSO 14 套 + ops 8 套，批次 864/1）**：`shard_xid_p1` 45/0、`shard_pagecmp_p1` 49/0、`tso_si_p3` 39/0、`dtx_convergence_p4` 45/0、`dtx_tso_p4` 49/0、`follower_replay_r1` 58/0、`txn_layer_r2` 批次 52/0、复跑 51/1（含"中止事务尾部是 ABORT MARKER"断言，两次都过；唯一的红是 P7-T8 丢提案额度，与本条无关）、`lazy_replay_l1` 57/0、`dtx_replay_tx1` 83/0、`dtx_commit_marker_tx2` 40/0、`promote_catchup_tx3` 24/0、`fastpath_divergence_tx4` 20/0、`replica_prune_guard_p7` 39/0、`partwal_ring_p7` 14/0（背压中途排空后中止）、`multi_table_isolation` 146/0、`segment_boundary_lsn` 6/0、`bulk_insert_recovery` 9/0、`demux_backlog_recovery` 29/0、`corrupt_segment_recovery` 44/0、`enospc_recovery` 12/0（磁盘满时中止）；`crash_recovery` 批次早退（场景 C 建表时 2PC 参与登记失败）→ **单跑 36/0**；`shard_auto_init` 4/1 → 查明是 `08_schema_existence` 期望文件自 09-10 未更新（13 个新函数：R3/T7.17/批次 6/T7.26/T7.27/V4，纯新增零删除，逐个核到引入提交），重建后 **5/0** —— 该测试自 09-12 `508c411` 起就红着，ops 套件此后没再跑过所以没人发现 |
| **P7-W5**<br>（2026-09-13 施工中） | **改共享结构体后增量 `make` 不可靠**（R-P6-7 根因）在 T7.27 又咬了一次：`PartWALCtlData` 加了 `nvalid` 等字段，容器里 `partwal_sync.h` 没同步，`make -s ... 2>&1 \| grep error` 把编译错误**滤掉了**，紧接着的 `make install` 用**上一次的 stale `.o` 重新链接**成功，`.so` 里没有新符号却报 installed，直到 `refresh_extension_sql.sh` 的逐名自检报"缺 partwal_ring_stats"才发现 | **✅ 已修（2026-09-13）**：新增 `pg-partdist-src/scripts/sync_build.sh`，取代手工 `docker cp` + 增量 make，两个扩展通用。六道守卫：①按内容（md5 清单）只同步变了的文件，`tar -m` 刷新 mtime；②同步后逐文件核对容器 == 工作区；③头文件清单与上次成功构建不同即 `make clean`；④编译输出不过滤，rc≠0 或出现 `error:` / `implicit declaration` / `undefined reference` 即失败；⑤已装 .so 与构建产物逐字节相同；⑥源码每个 `PG_FUNCTION_INFO_V1(f)` 的 `f` 与 `pg_finfo_f`、SQL 里每个 `MODULE_PATHNAME` 引用的符号都必须在 `.so` 动态导出表里。<br>★ 写守卫时又查出第三个成因：**`docker cp` / `tar` 保留宿主机 mtime**，宿主机源文件比容器 `.o` 旧时 make 直接判"已是最新"；以及 PGXS 未开 autodepend、头文件变了不触发重编。<br>**故障演示（容器临时副本，未碰 `/work`）**：A 源码加函数且 mtime 偏旧 → 手工 make 报 `Nothing to be done`、rc=0，守卫⑥报缺 `sb_demo_probe`/`pg_finfo_sb_demo_probe`；B 只改宿主机头文件 → 守卫②报 `include/demux_worker.h` md5 不一致；C 走正常同步构建 → 同步头文件、全量重编、符号 182 个齐；C′ 只改 `.c` 且宿主机 mtime 更旧 → 按内容同步、增量重编带上新函数；D 调用未声明函数 → make rc=0 仅 warning，守卫④判失败且不写构建基线。<br>真实环境：两扩展首跑建基线（pg_partdist 180 符号、pg_raft 60 符号，0 warning），次跑无变更 25 s 通过。`PG_TEST_ENV.md` 构建步骤已改为本脚本 |
| **P7-P2**<br>（2026-09-13 新登记） | **回放 worker 等尾部时零间隔空转，日志刷爆磁盘**（`replay_worker.c` `ReplayWorkerMain`）。原因：`ShardReplayRun` 读到"尾部尚未到达"是**正常返回**，主循环却写的是 `did_work = ok`，没抛错就算干了活 ⇒ 循环末尾 `WaitLatch` 超时取 0 ⇒ 立刻重来。每圈重建一次段索引、打一行 `追平至 N`。`replay_catchup` 给的上界一旦超过本地已收到的字节（字节在路上，或组已经没了、永远不会来），调用方超时走人之后 `target_plsn` 与 `CATCHING_UP` 仍挂在槽位上，worker **永远**转下去。<br>**实测取证**：tx2 容器 worker3 一个日志 11 小时刷到 **20.7 GB**，全是同两个分片的 `追平至 0`；整个容器 68.5 GB 可写层里 60.5 GB 是日志（叠加 `rotate_logs` 只改名归档、不删），宿主机磁盘到 94%。pg-test 上复现：catchup 超时后 10 秒 `追平至` 新增 **5246 行**、worker 约 17% 单核 | **✅ 已修（2026-09-13，T7.28）**。只有 `applied_part_lsn` 真的前进了才置 `did_work`、才打 `追平至`；没进展按 `replay_naptime_ms` 轮询，同一目标只打一行"字节尚未到达"（不刷屏也不静默）。顺带修 `did_work = ok` 是赋值、多槽位时只有最后一个算数。代价：晚到的尾部最多晚一个 naptime（默认 200 ms）。回填 FRD §13 约束 16 + 惰性形态"实现落点"。<br>★ **验收 `test_replay_spin_p7.sh` 先复现后修复**（已入门禁）：修复前 **18/2**（两条红即复现），修复后 **22/0**。判据前置"确实停在等待态"守卫，另验字节晚到后不重新触发即自行追到上界不越界、追满逐字节一致。<br>**回归**（`run_p6_exit.sh` 净场后按套件跑，覆盖回放主循环的全部调用面）：`locmap_base_p6` 23/0、`replay_bound_p6` 23/0、`follower_replay_r1` 58/0、`lazy_replay_l1` 57/0、`ddl_fileset_d1` 85/0（结构栅栏 NEEDS_STRUCT 路径）、`promote_catchup_tx3` 24/0、`ddl_auto_follow_p7` 118/0、`replica_prune_guard_p7` 39/0、`replay_spin_p7` 22/0 —— **9 套 449/0**。<br>**2026-09-13 已补**：门禁 `rotate_logs` 之后接 `prune_log_archives`，归档按代（`archive-<时间戳>`）只留最近 `KEEP_LOG_ARCHIVES` 代（默认 3，`all` 全留；按代删保证留下的每代都是完整一轮现场）。模拟目录验证：5 代留 3、`all`/非法值不删、非归档文件不动、重复执行删 0 代。pg-test 当时积压 43 代 / 331 MB |
| **P7-T8**<br>（2026-09-13 新登记） | **`txn_layer_r2` 的"丢提案额度"与实际恒差 1**：用例 [10] 故意停掉两个 follower 拆多数派，`health_check_no_drops 2` 给了 2 次额度，实测 `:5433` 恒丢 **3** 次（干净集群、多轮稳定）。原因未定：可能是额度按 T7.27 背压之前的提案次数标定，或拆多数派期间多了一次 CTRL 重试 | **✅ 已修（2026-09-14，T7.33）**。<br>**"多出来的丢弃"其实是两处，逐条拆出**（取证副本在每节开头打 `quorum_drops / last_drop_plsn` 快照，对照节点日志的 `reject propose`）：<br>① **[3] 建组首次登记时的交接广播（产品缺陷）**：T7.3 让新主在 `OP_PARTITION_PRIMARY` apply 里广播 PRIMARY_HANDOVER，但**首次登记（old_primary=0）且本节点就是 fileset 源头**时，副本 locmap 本来就按它的文件号配对，广播零信息量；而此刻副本常常还没就绪 ⇒ plsn=1 被拒两次（取证轮 [3]→[4] 之间 `quorum_drops` 0→2、`last_drop_plsn=1`），孤儿重推的 ERROR 还会打断这次 apply。出不出现取决于副本就绪时序 —— 登记时"偶发不超额"的观测与此一致。<br>② **[9] 窗口里的 T7.27 心跳自动修复（夹具问题，稳定来源）**：[9] 故意拆多数派 ⇒ 中止事务复制挂钩失败 ⇒ leader 打分叉标记 ⇒ 心跳约 4 s 后自连调 `repair_diverged_shards()` ⇒ 基线发不出去，同一个 backend 两次 `reject propose`（门禁日志：05:06:34 [9] 本身 1 次，05:06:38 自动修复 2 次，合计正是 `:5433+3`）；它的 FILESET_UPDATE 还可能落在 ABORT 标记之后（取证轮流尾读到 `4\|255\|1\|72`，"中止事务尾部是 ABORT MARKER"随之红）。额度 2 是 T7.27 之前按"1~2 次"标定的；A/B 用的 `5268690` 已含 T7.27、签名同为 `:5433+3`，吻合。<br>**修法**：①产品侧新增 `PartDistRoutePromoteEx(oid, emit_handover)`，`partwal_notify_primary_switch` 按 `old_primary != 0 \|\| ShardReplicaIsLocal(oid)` 决定发不发（必须在置 promoted 记号**之前**取）；真切换、首任主被副本抢到，照发。②夹具侧 [9] 拆多数派前在 leader 上关 `pg_partdist.auto_repair_diverged`（带 SHOW 断言，防 ALTER SYSTEM 静默失败），收尾丢弃检查**之后**才还原，EXIT 钩子与门禁净场（GUC 复位清单）各兜一层。**额度不动，仍是 2** —— 自动修复在故意拆多数派时失败是设计行为（修不掉留待下一轮），不是本节要验的对象；非预期的丢弃照样抓。<br>**验收**：`txn_layer_r2` **53/0**（新增 1 条"已关闭自动修复"断言），丢弃 **`:5433+1`**（恰为 [9] 的中止事务），"中止事务尾部是 ABORT MARKER"通过。回归（`election_timeout_ms` 为环境默认 6000）：`promote_handover_p7` 35/0（真切换仍广播，[6] PRIMARY_HANDOVER plsn=8）、`handover_provision_p7` 33/0、`shard_mvcc_register_p7` 41/0、`promote_catchup_tx3` 24/0 —— **4 套 133/0**；节点日志计数：首次登记跳过 4 次、真切换广播 5 次、广播失败 0 次。<br>**如实记验证条件**：`txn_layer_r2` 的 53/0 是把 `pg_raft.election_timeout_ms` **临时调到 20000** 跑出来的（跑完 9 节点复位回 6000 并逐节点核对）。同一时段 6000 下门禁连跑 5 轮，4 轮在 [3]/[4] 发生数据组主漂移、从 [4] 起连锁红（34/18、36/14、36/14、39/11），原因是另一个缺陷 P7-R5，漂移都发生在本条改动执行之前（例：05:27:49 副本发起选举，worker1 的首次登记 apply 在 05:28:00）；唯一没漂移的一轮（夹具修改之前）就是拆出 ② 的那轮 51/1。|
| **P7-T9**<br>（2026-09-14 新登记） | **门禁净场的 GUC 复位自写成以来从未生效**：`scrub()` 把五条 `ALTER SYSTEM RESET` 写在同一个 `-c` 里 ⇒ 隐式事务块 ⇒ `ALTER SYSTEM cannot run inside a transaction block`，输出丢进 `/dev/null`。前一套留下的白名单 / TSO 配置 / replay_trust_local_segments 一直漏进后一套（节点日志实测可见该报错）| **✅ 已修**：逐条执行、失败计数打印。验证：净场前在 :5440 设 `shard_relids=12345`，第一次净场后读回 为空（修复生效） |
| **P7-T10**<br>（2026-09-14 新登记） | **门禁按工作区的 SQL 文件重放扩展声明，而不是按已装的二进制**：工作区领先于容器 .so 时，plpgsql 函数被提前装进库（实测 `set_table_shard_mvcc` 在对应 C 函数还不存在时就已在库），C 函数则因符号缺失装不上。门禁汇总不会因此变红 | **✅ 已修（2026-09-14）**，两层：①`scripts/refresh_extension_sql.sh` 的 SQL 来源默认改为**容器里已安装的** `$(pg_config --sharedir)/extension/pg_partdist--1.0.sql`（与已装 .so 同一次 make install，也正是 CREATE EXTENSION 执行的内容），`SQL_SOURCE=workspace` 保留为显式选项；默认容器由 tx2 改为 pg-test。②门禁起跑前（净场之前）跑 `sync_build.sh --check`：容器源码与工作区按内容不一致、或已装 .so 缺应导出符号，即 FATAL（退出码 3，`ALLOW_STALE_BUILD=1` 可跳过；A/B 应从对应版本的 worktree 跑门禁）。<br>**演示（临时 worktree = HEAD + 本次两个脚本 + SQL 末尾加一个 plpgsql 演示函数）**：a）门禁在净场前 FATAL，指出 `sql/pg_partdist--1.0.sql` md5 不一致；b）默认来源重放 ⇒ 演示函数**未**入库；c）`SQL_SOURCE=workspace` ⇒ 复现旧行为、演示函数入库（随即清理）。<br>顺带修自检的潜伏缺陷：函数名正则 `[a-z_]+` 不认数字，演示函数 `t10_demo_probe` 被截成 `t` 报"缺 t"（现有函数名恰好都不含数字）；删掉一行读临时文件先于写出的死代码。 |
| **P7-R5**<br>（2026-09-14 新登记） | **数据组新 leader 在 tick 循环里同步做"升主前置 + 向控制面登记"，期间本节点所有组都不发心跳；耗时一旦超过选举超时，副本就把它推翻**。原因（`raft_consensus.c` 登记路径 + `data_group_promote_prepare`）：当选后在同一个 tick 里依次 ① `PQconnectdb` 自连本节点（新 backend 要加载 Citus / partdist / raft）② `SELECT partdist.pg_raft_promote_prepare(gid, 2000)` ③ 对 group0 leader `PQexec pg_raft_report_data_leader`（其内是一次同步复制到 8 个节点的 group0 提案），全程阻塞。<br>**实测**（pg-test，2026-09-14 下午；e2-medium 2 vCPU，跑套件时 vmstat idle 0–2%）：当选→登记 **10.0 / 10.2 / 12.5 / 14.2 s**，每次都在当选后 8.5–11 s 被副本发起选举推翻（`election_timeout_ms=6000`，随机化上限 12 s）；同一 .so、同一套件把 `election_timeout_ms` 临时调到 20000 后，当选→登记 8.5 s、零漂移、`txn_layer_r2` 53/0 —— 机理坐实。表现：新建分片组后第一笔写入被判中止（`txn_layer_r2` [4] 的提交得到 ABORT 标记，之后连锁红），主在建组后几十秒内反复换人。夹具规则 8 记的"分区主自治漂移"至少有一部分就是它 | **✅ 已修（2026-09-15，T7.34）**。**修法取"登记期间照发心跳"，没有搬走 tick**：升主前置与登记的两条语句改走**专用缓存连接**（`self_conn` / `report_conn`，与 `peer_conn[]` 分开 —— 等待期间要拿 peer_conn 继续发心跳，同一连接上不能挂两条在途查询），`PQsendQuery` + `select(2)` 100 ms 轮询，每 `heartbeat_ms` 调一次 `heartbeat_all_leader_groups()` 给本节点**全部** leader 组发心跳；各自带上界（升主前置 = slice+8 s，登记 = 30 s），超时即重置该连接、保留 `report_pending` 下个 tick 重来。<br>顺带去掉"每 tick 新起一个 backend"：原先升主前置每次 `PQconnectdb` 自连，现在连接缓存复用。<br>**为什么不搬去独立 worker**：登记必须在"本节点仍是该 term 的 leader"这个前提下发出，搬走就要把 term 栅栏与 `report_pending` 的所有权跨进程重做；而阻塞的根因只是"等待期间不发心跳"，就地修掉更小更可验。<br>**验收**：`election_timeout_ms` **保持环境默认 6000**（不再需要 T7.33 那样临时调大），`txn_layer_r2` 连跑两轮 **56/0 + 56/0**，零数据组主漂移；日志实测当选→登记 3.5 s 与 13 s 两种都照常登记、期间无副本发起选举（13 s 那轮修复前必被推翻）。夹具驱动的 follower 重启窗口里出现过一次"登记超过 30000 ms"告警 + 下个 tick 重试成功 —— 上界生效的正常形态（对端 `the database system is starting up`）|
| **P7-W7**<br>（2026-09-15 新登记，Codex 复核提出） | **高并发多机写入下写路径两处健壮性缺陷**（外部复核指出"fileset 异常、peer-WAL 异常、延迟暴涨"，在 pg-test 32 客户端 60 s 复现）：<br>① **group-commit 回读 peer 槽位拿到零字节，失败后还往流里写空记录**。回读用的 `XLogReader` 是 backend 级 static、跨事务复用；页读回调把整页 8192 字节（含未写到的零尾）交给 reader，它把 `readLen=8192` 缓存起来，下次读同一页后半的 peer 记录时字节已 `XLogFlush` 落盘、reader 却拿缓存里的零交差（`无法读取 peer WAL 记录 … expected at least 24, got 0`，实测 **42 次/分钟**）。失败路径原先照样 `AppendPartWALRecord(…, NULL, 0, …)` 写一条 `data_len=0` 的 DATA 记录，副本回放到它只打一句 WARNING 就跳过并推进游标 —— **那条物理变更在副本上静默缺失、没有任何标记**。<br>② **fileset 持久化并发互踩**：多个 backend 首次 DML 各自登记同一分片（去重只在 backend 本地），共用一个 `fileset.tmp`，`O_TRUNC` 截掉别人写好的内容、rename 抢先者把 tmp 挪走后落后者报 ENOENT（`无法就位 fileset 文件 … No such file or directory`，实测 **90 次/分钟**），期间 fileset 文件瞬时 0 字节 ⇒ `LoadShardFileSet` 的探测者（DDL 变更发射、冻结账目、升主）把该分片当"不归本节点维护" | **✅ 已修（2026-09-15，T7.34）**。①`PartWALReadPage` 按 `GetFlushRecPtr()` 截断返回长度（取 flush 而非 write 位点：分区流不许领先已落盘 pg_wal，§13 约束 18 ①），reader 缓存的 readLen 从此诚实；`AssembleRawWALRecord` 改为只要求"到本记录页内末尾"的字节（要整页在当前页必然短读 —— 只做截断不改 Assemble 时回读失败反升到 **407 次**，正好反证它也在当前页上读）；`PartWALOpenSegment` 的时间线不再写死 1，取 `GetWALInsertionTimeLine()`。回读失败**不再写记录**：锁内记下分区、锁外打分叉标记交心跳重做基线（与 W2 覆盖、W4 中止排空失败同一处置）。<br>②`fileset.tmp` / `freeze.tmp` 名带 PID（rename 仍是原子替换，谁后谁赢、内容一致），rename 失败清 tmp，就位后 `fsync` 目录（ext4 不保证 rename 的目录项随文件一起落盘，而 fileset 文件是重启后重建捕获登记的唯一依据）；**内容与已持久化的一致时直接返回不重写** —— 连接池一抖动就是一批 backend 各自"首次登记"，每次都是写+fsync+rename，全在写路径上白花，也正是竞争窗口本身。两处失败路径都补 errhint 说明"shmem 登记已生效、本次运行照常捕获，重启后才需要重新登记"。<br>**验收 `tests/test_highload_w7.sh`**（已入门禁）：同负载下两类告警增量 **0 次**（修复前 90 / 42），`fileset.tmp` 残留 0，无空 DATA 记录、无崩溃。<br>**延迟不是产品问题**：p99 4.2 s，等待事件采样（0.5 s 一次）显示 8 客户端时 idle 已 0%、86% `Client:ClientRead`、`LWLock:pg_partdist_sync` 仅 2 次 —— 2 vCPU 机器饱和（tps 30–40），不是写路径锁队列；套件只打印不断言。32 客户端还会把协调者 `max_connections=100` 打满 ⇒ worker 侧 TSO 自连失败 ⇒ 事务报 `TSO 不可达或拒绝服务`，同属环境配置 |
| **P7-W6**<br>（2026-09-14 新登记） | **分叉标记自动修复在多数派缺失期间照样周期性重发基线**（P7-T8 取证时撞见）。T7.27 的心跳按 `auto_repair_interval_s`（默认 60 s）调 `repair_diverged_shards()`，它对每个带标记的分片直接 `ShardBaselineEmit`、失败了由子事务吞掉 —— 没有"本组此刻凑不凑得齐多数派"的前置判断（函数注释说"只对本节点是组 leader 的分片动手"，代码里其实没有 leader 判断）。而"复制挂钩失败"这类标记恰恰意味着多数派刚刚没了，马上重试几乎必败：门禁日志里标记后约 4 s 即尝试，被拒两次提案（`quorum_drops` +2）。<br>**未量**：失败的基线尝试在 leader 本地流里留下多少孤儿字节。按代码与日志推断，CTRL 与 FPI 在复制失败前已写入 pg_parwal，大分片单次最多一个分块（`fileset_baseline_chunk_blocks`=16384 块 = 128 MB），停机期间每个间隔一轮。partdist 侧也没有现成判据：pg_raft 不对外暴露对端可达性（`peer_backoff_until` 是 tick 进程本地变量） | **✅ 已修（2026-09-15，T7.34）**。取"读多数派可达性"这条：pg_raft 新增 `pg_raft_group_quorum_alive(group_id)` —— 本节点是该组 leader，且"自己 + 最近 2 个选举超时内**应答过** AppendEntries 的成员"够多数派才为真。判据用**时间**而非 `match_index`（停机的 follower 停机前把一切都 ack 过了，match_index 看着很健康），shmem 里每组每对端记 `peer_last_ack` / `peer_last_fail` 两个时刻（RPC 有回应就算活着 —— 接受或拒绝都算；连不上/超时记 fail），fail 晚于 ack 即判死，一个心跳周期内就能判出停机的 follower。<br>`repair_diverged_shards()` 在发基线**之前**问它：不满足就记 `quorum_wait:<oid>`、计入 skipped、留标记给下一轮，**一条提案都不发**。顺带兑现了那句"只对本节点是组 leader 的分片动手"的注释（此前无代码）。<br>**验收**：`txn_layer_r2` [9] 拆多数派窗口内新增三条断言，连跑两轮全过 —— `pg_raft_group_quorum_alive` 判 false、`repair_diverged_shards()` 返回 `repaired=0 skipped=1 quorum_wait:<oid>`、`quorum_drops` **1 → 1**（修复前每次尝试 +2）。T7.33 当天为此在夹具里关 `auto_repair_diverged` 的绕行**已撤**，改成断言产品行为；额度仍是 2。<br>**孤儿字节增长未再单独实测**：修完之后失败尝试根本不发生（发基线前就返回），原先"每 60 s 一个分块"的路径已不可达 |


**T7.26 / T7.27 落地后的热路径回归（2026-09-13）**：改动落在每次 heap 写（`shard_stamp_xid` 里的背压钩子）、判活（`HeapTupleSatisfiesVacuumHorizon` 里的剪枝守卫）、事务中止（`PartWALAbort`）三条热路径，跑了 10 套回归：`shard_xid_p1` 45/0、`shard_pagecmp_p1` 49/0、`shard_clog_p2` 64/0、`tso_si_p3` 39/0、`shard_vacuum_p5` 180/0、`follower_replay_r1` 58/0、`replica_gate_p6` 33/0 —— **七套零 FAIL**。另三套红均为 **P7-E7（宿主机无 swap）主漂/环满假红**，按纪律单跑复核：`dtx_replay_tx1` 批 73/10、**单跑 83/0**（与 P7-E7 记录的 73/10→83/0 逐字吻合，已复核）、`txn_layer_r2` 批 51/1（:5433 raft 日志环满丢 3 > 额度 2）、`shard_vacuum_auto_p7` 批 61/1（心跳截断点受 GlobalSafeTs 限流、40s 窗口内 `commit-ts-too-new`）。背压只在单事务 4096 条记录以上触发，这三套的事务都远小于此，机理上排除本次改动。

### 1.10 2026-09-16 4 节点（1c+3w）+ max_groups=64 环境下的复核

用 `reproduce-env.sh N_WORKERS=3` 重建为 4 节点、`pg_raft.max_groups=64`，跑了主从架构
新用例与 2.0 时代的 demo 套件。结论分三类：

**一、架构命题成立（新用例）**
`test_mixed_role_p7.sh` **29/0**：同一节点 M 同时是分片 A 的 `native_leader` 与分片 B 的
`replica`；M 作主读写正常；往 A 写 250 行 + 改 100 行期间 B 的从副本主堆**字节不变**
（同机多分片物理隔离）；往 B 的主写后 M 的从追平且与主逐字节一致。
★ 最有说服力的一条是 [7] 的取证——同一节点上两套 xid 空间的**解析路径**不同：

    A(oid=60669)  自家号 16217 ⇒ NONE          拿 B 的号 16922 查 ⇒ NONE
    B从(oid=60699) 自家号 16922 ⇒ committed     拿 A 的号 16217 查 ⇒ not_replayed

A 的号走本机原生语义、B-从的号走回放两跳（xid_map→gclog）拿到判决，互相拿对方的号
查都查不出东西 —— 按各自 OID 编址，互不串线（规则 6）。

**二、多主多从同样成立（纠正我先前的一个错误结论）**
`test_multi_role_p7.sh`（M 同时持 2 个主 + 2 个从、4 组并存）**31/0**，日志零 ERROR。
四重身份的取证（同一节点、四个不同本地 OID）：

    A分片 102394(oid=60765) xmin=17081 ⇒ NONE        A分片 102397(oid=60772) xmin=17090 ⇒ NONE
    B从   102395(oid=60787) xmin=18011 ⇒ committed   B从   102396(oid=60794) xmin=15769 ⇒ committed

两个从各自追平到主的 307/307 并与主逐字节一致（掩码外）。

我一度把它写成"2 vCPU 的容量上限"，**那是错的**，在此纠正：写入阶段的
`record N 未达多数派` 与随后的主漂，真正的原因是 **A 组的 follower 没有本地分片站点**
——数据组的 follower 必须先有壳表 + 分片身份 + locmap 才能落盘并 ack，光当"raft 成员"
不行（follower 日志刷 `group N 在本节点没有对应分片，无法落盘`，提案恒 1/2 票）。
给每个组都用产品入口 `partdist.provision_shard_replica()` 供好副本之后，写入一次全绿。
与硬件无关。

另外两个曾被怀疑的方向也已排除：① Citus 的 `multi_shard_modify_mode=sequential`
不解决问题（串的是语句，提交时的多组复制仍同时发生）；②
`another command is already in progress` 是 **Citus** 侧连接在 worker 报错后的残留状态，
不是 pg_raft 的 peer 连接缺陷（pg_raft 两个有界 RPC 调用点都正确调了 `peer_conn_reset`）。

★ **再纠正一处（2026-09-17 复核日志）**：我 09-16 在这里写过"仍然成立的真限制：多组同时提交时
复制共用一条 peer 连接，4 组并发提交实测一起报 `未达多数派`"。**这条没有实测支撑，撤回。**
逐轮核对：`未达多数派` / `another command is already in progress` **只出现在 multi1–3**，
而那三轮 `provision_shard_replica` 的 PASS 数都是 **0**（副本根本没供）；供好副本的 multi4–6
两类报错都是 **0**，其中 multi4–6 的写入还是一条 INSERT 横跨 6 个分片（4 个有组）。
所以那些现象全部归因于"follower 没有本地站点"，**不能**拿来推"共用 peer 连接是瓶颈"。
"多组跨分片事务在 1c+3w、同节点多主多从布局下是否正常"**尚未做正式验收**（9 节点上
dtx_* 套件在 W4 二进制时是绿的），列为待验项。

**二之补、夹具上踩到的一个硬事实：Citus 的落位不能靠"多建几张表"碰运气**
单分片表**恒落第一个 worker**：建 9 张单分片表、连 `colocate_with=>'none'` 把共置组
全拆开，9 张仍然全落 `:5433`，于是"建一堆再挑一张主在别处的"根本挑不出来
（现象是 `NID[$C]: unbound variable` 崩在夹具里）。`citus_move_shard_placement()` 也走
不通——产品**有意禁用**（T4.6/§9.2：打标集群上搬分片用原生快照读写 = 静默错读）。
正解是**用分片数造拓扑**：
* mixed：A 用单分片表（必落 W1 ⇒ M 定在 W1），B 用 2 分片表（两片必分两台），
  取 B 里 placement ≠ M 的那片 ⇒ `M≠C` 是拓扑保证的。
* multi：一张 `worker 数 ×2` 分片表，轮转后每台正好 2 片 ⇒ M 天然有 2 个主，
  另两台各出 1 片当 2 个从。

**二之补二、写入必须"定点"才只碰一个 raft 组**
`INSERT ... SELECT generate_series` 会同时写到**全部**分片 ⇒ 进 DTX 决议路径找协调组，
而兄弟分片没建 raft 组，实测报 `协调组 N 查不到现任 leader / partition_map 尚未追平`，
**整条 INSERT 失败**、目标分片零行，后面取 `min(xmin)` 拿到 NULL 跟着塌
（26/1 那个 FAIL 就是这么来的，之前误记成"副本读不到"）。
Citus 对**多行 VALUES** 和 **`IN (常量列表)`** 会逐行剪枝，全落一片时直接路由成
`Task Count: 1`（EXPLAIN 实测）；`INSERT ... SELECT` 不会（走
`Custom Scan (Citus INSERT ... SELECT)`）。所以两个用例的写入一律拼 VALUES / IN 列表，
按 `get_shard_id_for_distribution_column()` 先挑出只落目标分片的 id。

**三、2.0 时代 demo 套件（10 项）在新配置下的结果**

两个新用例跑绿**之后**又在同一配置上整套复跑了一遍（用例收尾会拆掉全部数据组、删掉夹具表，
这一轮就是验"没留残渣"）。两轮结果一致：

| # | 项目 | 首轮 | 复跑（两个用例绿之后） |
|---|---|---|---|
| 1 | 写入连续性 & 崩溃恢复 | 20✓/11✗ | 20✓/11✗ —— **陈旧期望**，非回归：它按 2.0 口径断言"插 3 行 = 3 条 parwal 记录"，而现在一行会产生堆记录 + 索引记录 + 提交 MARKER（实测 10 条）。索引/TOAST 与主堆走同一条捕获链是设计（FRD §5.2），期望值该更新 |
| 2 | 分片自动初始化（含 37 项回归） | ✓ | **5/0** |
| 3 | 多分布表隔离性 & 持久性 | 146/0 | **146/0** |
| 4 | 崩溃恢复专项（A/B/C） | ✓ | **PASS** |
| 5 | 批量 COPY 路径恢复 | 9/0 | **8/1** —— 唯一的 FAIL 是**性能阈值**（hook 开销 26.5% vs 阈值 <20%），功能项（COPY 恢复、37 项回归）全过。与 #9 同类的 2 vCPU 噪声，两轮之间只是计时波动 |
| 6 | 跨段边界 LSN 连续性 | ✓ | **6/0** |
| 7 | 段文件损坏恢复（C1–C4） | 44/0 | **44/0** |
| 8 | Demux 高积压崩溃恢复（S1–S5） | ✓ | **29 PASS / 0 FAIL** |
| 9 | 端到端延迟 p99 | ✗ 749 ms | ✗ **p99 489 ms / avg 23.7 ms**（阈值 10 / 5 ms）—— 32 并发 pgbench 压在 2 vCPU 上，与 memory `codex-highload-findings` 同源，非产品缺陷 |
| 10 | 磁盘满 ENOSPC 容错恢复 | 12/0 | **12/0** |

口径：**功能项 10 项里 9 项全绿**；两处不绿分别是"陈旧期望"（#1）与"硬件饱和的性能阈值"
（#5 的第 4 项、#9），都不是产品缺陷。
★ 一个会绊人的坑：`tests/perf_latency.sh` 是**宿主机侧**脚本，默认 `CONTAINER=pg-partdist-raft4-container`
（旧 raft4 环境）。在本环境跑必须显式 `CONTAINER=pg-test-container`，否则报
`container ... is not running`。其余 demo 脚本是**容器内**跑（它们用 `psql -h localhost`），
在宿主机跑会全片报"协调者 :5432 连不上"。

**四、顺带修掉的测试面缺陷**
`reproduce-env.sh` V6 的 parwal 指纹断言**各自按自己的 flush_lsn 取上界**、且先采 leader
后采 follower：两次采样之间后台发射器（D2 冻结账目同步、分叉自愈基线、收尾 SHARD_DROP）
随时多落一条，就报"指纹不等"。实测 leader 9 条 / follower 10 条，而 1..9 与 1..10
两侧逐条相同 —— 纯采样竞态。改为在**三方共同区间**上算，V1–V6 **20/0**。


### 1.11 2026-09-17 复核：全量回归 18 条 FAIL 分类 + 日志环实测 + 跨组事务试跑查出的新缺陷

**一、09-15 不分段全量（56 套，PASS=2211 FAIL=18，二进制 c815487）当时没人分析，本次补上**
（证据取自已停的 `pg-test-container-9w` 各节点日志；9 节点日志里无 signal 11/6、无真 PANIC）：

| 类别 | 条数 | 套件 |
|---|---|---|
| 真产品缺陷 | 1 | `replica_gate_p6`：副本本地放行的 CREATE INDEX/REINDEX/ANALYZE 写出孤儿记录，两次"孤儿截断重写"后出现 `流内空洞：期望 plsn 244，下一条已存在的是 245`，回放 FAILED；缺的 244 是 leader 发来的冻结账目记录。与 T7.24 孤儿替换相关，**新登记 P7-N1**。〔2026-09-18 再现（另一条路）：N25 回归里原始主 A 被自动归队重供（base=141）后追到 148，随即 `流内空洞：期望 plsn 149，下一条已存在的是 150`、永远追不上；那次重供与新主的首批写入交错（工作者首轮 5 s 延迟因 latch 预置位失效，0.2 s 就开供，已改）。**已定因并修（P7-N1 之二）**：`TruncatePartWALTo`（孤儿替换"截到 plsn-1"与 raft 截断共用）把段文件**按名字排序**，在名字序里第一条 plsn > keep 处截断、并**整段删掉名字更靠后的段**——前提"段名升序即 plsn 升序"对**被降级的原始主**不成立：它自己当主时的记录按本机 LSN 落段、降级后收的新主记录按新主 LSN 落段。A 的孤儿（降级后仍尝试发冻结账目、本地写下又被拒）在 …36 段、B 的已提交 141–147 在 …47 段，截到 147 时先在 …36 碰到孤儿 ⇒ 截 …36、删 …47 ⇒ 已提交记录丢失。改为逐段逐记录：无 >keep 不动 / 全 >keep 删 / >keep 在尾部截 / 夹杂则只留 ≤keep 重写；checkpoint 回退取全体段里 plsn≤keep 最大那条的 orig_lsn〕 |
| 疑似产品问题、证据不足 | 8 | `dtx_tso_p4` ×6：协调者在 node9 当 group0 leader 期间约 7 分钟**一条条目都没 apply**（心跳在、条目不到），跨分片事务按 fail-closed 全失败（另含夹具等待 20s 过短与 `$6: unbound variable`）——**新登记 P7-N2**；`txn_layer_r2` ×1（丢提案 3>2，与 P7-T8 同签名，:5433 日志缺失无法定因）；`ddl_fileset_d1` ×1（栅栏前游标多推 1 条，plsn 218 类型不明） |
| 夹具/断言问题 | 9 | `shard_vacuum_replay_p5` ×5：T7.32 之后中止事务的字节随下一次写入才复制，拿 `get_partition_flush_lsn` 当上界调 `replay_catchup` 会等到超时（接口语义待定，**P7-N3**）；`clog_hole_c4` ×3：非落位节点当选（规则 22/25 同族）；`replay_spin_p7` ×1：判据没排除后台 CTRL 记录 |

**二、日志环容量（运维规程 §2.1）实测结论：不是跨组事务的限制，不改**
1c+3w、3 分片表每片一个 3 成员组（每台都是一个组的主 + 两个组的从）：单事务 1500 行横跨 3 组成功，每组日志推进
517–534 条；8 会话并发跨组事务；全程 0.5 s 采样 `ring_depth` 峰值 **2**，环满等待/丢弃、多数派丢弃、捕获环覆盖全为 0。
机理：数据复制按组串行（`replicate_claim`）且逐条同步提交，环只约束"已追加未应用"条数；环满只发生在 apply 卡住
（副本没供好）或单组并发提案 >127。调大容量不解决这两种情形。

**三、跨组事务并发试跑查出的新缺陷**

| 编号 | 缺陷 | 证据 | 严重度 |
|---|---|---|---|
| **P7-N4** | **升主登记活锁**：高负载下切主后新 leader 永远登记不上，路由一直指向旧主，写该分片全失败，且会连锁到同节点其他组 | 组 102616 08:31:43 漂到 :5435 后，每 ~17 s `升主前置超过 10000 ms 未返回或连接失效`，从未登记；`track_functions` 量得升主前置平均 **69 s/次**（`dtx_close_indoubt` 占 51 s），tick 只等 10 s。读码确认三处叠加：①结果被丢弃重来；②超时路径不计截止期，60 s 兜底永不触发；③超时只 `PQfinish` 不 `PQcancel`，同组堆积到 6 个并发升主前置。随后 102617、102616 再漂到 :5434、:5433，陷入同样循环 | **P0** → **✅ 已修（2026-09-17，T7.35，`d9e792a`）**。**先确定性复现再修**：`test_promote_register_p7n4.sh`（1 分片 3 成员组、流长 1569 条，旧主 heartbeat_ms 抬到 10 s、指定 follower 选举超时压到 1.5 s、另一台抬到 60 s）修复前 **10/6**（150 s 未登记、6 条超时告警、会话峰值 6、切主后写入被拒），修复后 **19/0**（当选→登记 12.7 s、零告警、峰值 1、新主数据完整）。修法两层：① tick 改跨 tick 非阻塞状态机（非阻塞建连、结果一定收下、在途绑定 (组, term) 失主即 PQcancel、同 term 已准备只重发登记、进度槽满回收）；② `partwal_find_record` 加后端本地稀疏索引（检查点核对记录头、失败退回全扫描；GUC `pg_partdist.partwal_record_index` 逃生口），升序/降序/乱序全量读与关索引读逐条指纹一致 |
| **P7-N5** | 升主瞬间副本回放跳过记录 | :5434 接手 102617 时先收全量基线截空 4 个文件，plsn 1249–1252 以 `记录越出本地关系边界，跳过该记录（实际块数=0）` 跳过，随后升主；夹具已拆，未能核实数据是否缺失 | 待定因（可能是数据缺失） |
| **P7-N6** | 截止期设 0 实为"立即放行"，与 GUC 描述/设计文档"永不放行"相反；兜底放行那轮不执行推进 WAL/闭合 in-doubt/修复/认领四步 | PG16 `TimestampDifferenceExceeds` 为 `diff >= msec*1000`；SQL 在 `RETURN 0` 之后才走四步 | P1 → **✅ 已修（T7.35，随 N4 一起）**：0 = 永不兜底；兜底改调 `pg_raft_promote_prepare_ex(gid, slice, force=true)`，仍尽力追一片、不以追平为前提、其余四步照做，-1 检查绕不过；截止期计时按 (组, term)，换 term 自动重置（顺带修掉"丢 leader 时计时槽不清零"）。**兜底路径尚无专门用例**（需要构造追不平的副本），列入待补 |
| **P7-N9** | **未打标的分布表可以建 raft 组、切主，切主后静默返回不一致的数据，没有任何拦截**。设计上未打标分片副本属"遗留宇宙"，升主后元组带旧主原生 xid、新主按本地 clog 判可见性 —— FOLLOWER_REPLAY_DESIGN §14.2 早已写明"升主后不可读"（R3 从未实装、建议遗留副本退役），但运行期既不拒绝给未打标表建组/供副本，也不拒绝升主后的读写 | 09-17 跨组事务验收首跑（表未打标）：[9] 负载下切主后账户总额 24000 → **26000**，新主本地读该分片 sum=7001、经协调者读同一分片 sum=8002（行数相同）；改为打标后重跑见 T7.35 验收 | **P1**（"假可用"：接口允许、结果错误、无告警） |
| **P7-N10** | **打标表上的全局事务（带 join_info），写集只有一个分片却走 2PC 时（典型是协调者侧 `INSERT…SELECT`），客户端报告提交成功，分片 clog 却永远停在 RUNNING，行永久不可见**；切主时 `shard_claim_on_promote` 还会把它改判 ABORTED ⇒ 静默丢数据。无任何 WARNING。推测机理：写集单组 ⇒ 协调者判决走 §3.4 快路径（`nparts <= 1` 直接 return）不产生判决，参与者等不到判决（夹具规则 5 早有记录，但当时当成夹具问题） | 09-17 跨组验收三跑现场（1c+3w，TSO 全栈）：同一分片上 A=`INSERT…SELECT unnest` 带 join → `INSERT 0 3`+`COMMIT` 成功，30 s/30 轮 dtx_pending_sweep 后仍 0 行；pageinspect 见 3 个元组 xmin=分片 xid 8 物理在盘，`partdist_shard_clog_read_full` → `st=1 sts=413 cts=0`；对照 B=多行 VALUES 带 join（1PC）→ st=2 可见；C=`INSERT…SELECT` 不带 join → PREPARE 被拒（fail-closed，正确）；D=VALUES 不带 join → 可见。验收 `test_crossgroup_txn_p7.sh` [9b] 为它专设断言 | **P0** → **✅ 已修（2026-09-17，T7.36，`4ce9775`）**：协调者决议挂钩 `nparts <= 1 ⇒ return` 改为 `nparts == 0`；单组 2PC 照常下发协调组、做决议、广播。**先复现再修**：`test_single_group_2pc_p7.sh`（打标 + TSO + join，[A] 单分片 INSERT…SELECT、[B] 广播 UPDATE 只改一片、[C] 1PC 对照、[D] 副本一致）修前 **24/7**（提交成功、收敛 60 s 后 0 行可见、决议 +0），修后 **31/0**（可见、决议 +1、副本 clog COMMITTED、1PC 仍不产生决议） |
| **P7-N11** | **跨组 2PC 负载下 TSO 续租栅栏频繁触发**：读报 `分片快照被栅栏作废：本节点已 7500 ms 未能向 TSO 续租`，事务失败（未切主的并发转账 60 笔里 27 笔因此失败）。TSO 源自身不慢；疑似各节点心跳工作者在**同一循环里同步**跑 `dtx_pending_sweep` 自连清扫、分片 vacuum 自动启动、`repair_diverged_shards`（发基线），把下一次续租推迟过 lease−lease/4 | 验收四跑：协调者 `partdist_tso_heartbeat` 1027 次、均 0 ms，而按 lease/3 应约 2400 次；worker 上 `dtx_pending_sweep` 各 200+ 次、均 0.3 s，`repair_diverged_shards` :5433 均 1.3 s。**根因未坐实**（缺心跳间隔直方图） | P1（可用性，同 N4 形态：阻塞活放进心跳循环） |
| **P7-N12** | **跨组事务 + 切主后账户总额不守恒，且新旧主/副本数据不一致**。根因未定 | 验收四跑（打标 + TSO + join 全栈，4 会话账户不重叠的跨组转账）：[6] 期间组 102633 自发漂主（:5435→:5434，当选→登记 32 s，**未走兜底**），判决收敛 90 s 后总额 24000→**23998**；[9] 指定 102631 切主（当选→登记 28 s）后 23998→**23999**。取证：各节点本地读同一分片余额和不同（102631 旧主 :5433=7998、新主 :5434=7997；102633 新主 8015、旧主 7999）；**旧主切主后打标状态仍为 registered=yes replica=no**；102633 副本 :5433 追平到 251/251 后主堆与新主 hole_mismatch；**102633 分片 xid 10 在旧主 sts=89、新主 sts=103**（疑似切主后分片 xid 重复分配）；运行期间自动修复重发基线 12 次、孤儿记录替换 8 次、`事务中止时排空分区记录失败：could not open partition WAL segment` 与 `无法写分叉标记 …: Not a directory` 各 2 次 | **P0** → **✅ 已修（两段机理，2026-09-17）**。**之一**（`3a2fc1f`）：参与者判决标记复制失败（典型是刚丢主、写栅栏拒）时原本照样注销未决登记 ⇒ 回执 ⇒ 协调组 FORGET 决议 ⇒ 新主永远找不到判决；现复制失败保留登记，主权登记转走后才注销（`dtx_pending.c`）。**之二**（本批）：升主序列的 `dtx_close_indoubt` 原判据"流里有 DTX_PREPARE、无闭合记录"，闭合动作只往流里追加一条 DTX 记录 —— 分片 clog 从不更新、副本也不认那条记录（分片 clog 只在回放带分片 xid 的 MARKER 时落判决），于是"流里已闭合、clog 仍 PREPARED"的事务永远不可见且再也不会被看见（实测 3 个 PREPARED 残留只报 1 笔 in-doubt）。现改为**从分片 clog 出发**：列全部 DTX_PREPARE，按 DTX_PREPARE 与 PREPARE MARKER 共享的头部 gxid 映射回分片 xid（新 `partdist.partwal_prepare_sxid`），clog 仍 PREPARED 的一律处理，判决优先取流里已有的闭合记录（本地权威）、没有才 `dtx_inquire` / 四级寻址，落账走新 `partdist.shard_verdict_apply`（本地 clog + 带分片 xid 的判决 MARKER 复制给副本，与 leader 侧 `dtx_replicate_verdict` 同构）。验收 `test_failover_verdict_p7n12.sh`（TX2 全栈、4 会话跨组转账 + 2 次确定性切主）：修前 run 2 总额 24000→23999、PREPARED 残留 13；修后 run 4 **总额守恒、残留 0**，:5433 复制失败 16 条全部保留到交接（2 条"视为已交接"）。**两个丢失机理已修并验证**。run 5（N17 二进制、一组内连切两次）复现出**第三条、更深的机理 → 另立 P7-N18**：残留 ±小额总额漂移（run 5 +2，run 2 −1），各主本地无 PREPARED 残留、各主内容与协调者一致，但已提交数据本身多出 2 —— 属分片 xid 跨任期重号，非本项判决通道 |
| **P7-N13** | **2PC 回滚的判决不进副本分片 clog**：主上 st=3（ABORTED）的分片 xid，副本上永远 st=1（**PREPARED**，此前误写成 RUNNING；状态码 0=RUNNING 1=PREPARED 2=COMMITTED 3=ABORTED）⇒ 副本升主后这些 xid 靠升主序列的 in-doubt 闭合找决议，决议已 FORGET 就永远未决；[6]/[9] 里 `分片 N 的 xid X 是无主 RUNNING` 报错另有来源（st=0 空洞）。与 P7-N12 是同一条通道（判决标记复制）的两个方向 | 验收四跑逐 xid 对照：102631 xid 14/18/25 主 st=3、两副本 st=1；102632 xid 12/22 主 st=3、两副本 st=1；102633 xid 10 主 st=3、副本 :5433 st=1。[9] 窗口 60 笔失败中 24 笔是"无主 RUNNING" | **P1** → **✅ 已修（2026-09-18）**：根因不是"回滚判决没发"，而是**终局标记是 24 字节旧格式、不带分片 xid**——`PartDistDtxOnFinishPrepared` 用 `PartWALBuildMarkerPayload`，它按 `ShardXidXactCount()>0` 决定带不带尾，而该钩子跑在 COMMIT/ROLLBACK PREPARED **自己那个事务**里、计数恒 0 ⇒ 回放侧 `ShardClogSetVerdict` 被跳过 ⇒ 主 st=3、副本永远 st=1。取证：gxid=281474976770860 的 PREPARE 标记 32 字节、plsn 105 的 ABORT 标记仅 24 字节。修法：ABORT 方向按 dtxid 从流里回查 PREPARE 标记的分片 xid（`partwal_prepare_sxid`），再走 `shard_verdict_apply`（落本地 clog + 追加带分片 xid 的判决标记复制给副本）；COMMIT 方向仍由 `dtx_replicate_verdict` 落账（还缺权威 commit_ts，见 T7.1）。**★ 闸门**：只在该 xid 仍是 PREPARED(st=1) 时才落 ABORT——`shard_verdict_apply` 无条件覆写，漏闸会把已 COMMITTED 翻成 ABORT（插入的行消失、被 UPDATE 掉的旧版本复活），实测 **总额 +1001、冒出重复行**，已由闸门修掉。验收：N12 连切两次，修前稳定 1 笔残留 ⇒ 修后**三组 0 残留、行数 8/8 无重复、无崩溃** |
| **P7-N14** | **不可升主的节点赢得选举后占着 leader 位不放，整组无主**：升主前置返回 -1（没有本地副本 / 快路径分叉 / 带陈旧数据回归的旧主，R-P4-15）本身是对的，但该节点仍是 raft leader、照发心跳，其余有资格的成员选不上；partition_map 指向前任、写全被写栅栏拒。设计原话"只排除这一个节点，同组其余成员照常可当选"实际不成立 | N12 复现首跑（09-17 15:33）：第 2 轮把主切回最初的真表主 :5433，它当选 term=5 后 `拒绝升主：本节点已收到分区 WAL 到位点 152，却没有回放槽位` ×9，120 s 未登记，选举靠随机抖动才换到 term 6/7 | **P1** → **✅ 已修（T7.37，随 N12 部署）**：`data_group_try_report` 收到 -1 即 `raft_abdicate_unpromotable()`：降为 follower、清 leader_id、复制游标作废、选举截止期推远 5 个选举周期（期间只投票），日志 `组 N 本节点不可升主（升主前置返回 -1），主动让位并退避 X ms`。term 不动、HardState 不变。**尚无专门用例**（需要构造"旧主回归赢选举"的确定性场景；N12 用例改为只挑从未当过主的节点后不再触发） |
| **P7-N15** | **供副本期间主漂走（发基线时心跳断档）**：leader 在 `provision_shard_replica` 发物理基线期间 follower 收不到心跳，选举超时（默认 6 s）到期后另一 follower 当选；进行中的供给以"本节点不是该分区组的 leader"失败，已收到 FULL_BASELINE 的副本停在"截断待 FPI 重建"半程（见 N16） | N12 复现 run 3（09-17 17:06）：组 102649 主 :5434 正供 :5435 时，:5433 于 17:06:28.99 发起选举 term=3 当选（3/3 票）；:5434 从未登记（partition_map old_primary=0）；随后 :5434 上 `shard 20295 冻结账目发射失败…本节点不是该分区组的 leader`。用例原有的漂主守卫查完之后才漂，收敛等待盯着旧主白等 60 s | P1 → **夹具侧已规避**：供副本期间把全体 worker 选举超时抬到 15 s、收敛等待每轮重认主并从新主补供（`test_failover_verdict_p7n12.sh`）。产品侧待定：发基线期间由 BGW 维持心跳，或供给分块让出 tick |
| **P7-N16** | **升主前置放行了"基线已截断、FPI 未到齐"的副本 ⇒ 新主是空壳**。FULL_BASELINE 的语义是"先把全部成员截成 0 块，后面那批 FPI 就是全部真相"，但流里没有"基线结束"记录、`PartWALCtrlFilesetUpdate` 头里也没有块数；FPI 未到齐时副本看起来 armed 且 applied == Raft 提交上界（那些 FPI 从未被提交），`pg_raft_promote_prepare_ex` 的四道判据（有本地副本 / 无分叉 / 有槽位 / 已追平）全过，放行升主。新主的 btree 索引文件 0 字节（连 metapage 都没有），任何走索引的语句报 `could not read block 0 in file … read only 0 of 8192 bytes`；若基线前表里已有行，堆同样 0 块 —— **升主即丢整片数据**，其余副本随后按新主的 fileset 交接与之"同源" | run 3 同一现场：:5433 `shard 102748 @plsn 5 收到全量物理基线，已截断全部 4 个本地文件，等待 FPI 重建`（17:06:30.22）→ `追平至 5` → 17:06:32.25 `分片 102649（本地 OID 102748）已接管为主`。升主后 `base/5/102754`（fv_102649 的 pkey）0 字节、堆 102748 仅 1 页（失败 INSERT 扩出来的）；协调者 `INSERT INTO fv_102649` 与 `SELECT sum(n) FROM fv` 全部 could not read block 0，run 3 作废。OPS_RUNBOOK §7.2"升主瞬间副本回放跳过记录（实际块数=0）"是同一机理的另一个截面 | **P0** → **✅ 已修（2026-09-18，`905dc9c`）**：`ShardBaselineEmit` 末尾追加 `PARTWAL_CTRL_BASELINE_END`（携 base_plsn）；副本收到 FULL_BASELINE 置 `ReplayShardSlot.baseline_pending`、收到匹配 END 清零；新增 `partdist.shard_baseline_pending(oid)`；`promote_prepare_ex` 追平后见 pending 为真即 RETURN 0（force 也不绕过）。整簇重启部署（shmem 布局变） |
| **P7-N17** | **2 vCPU 饱和时共识 tick 串行同步 RPC 被慢 peer 拖住 ⇒ leader 心跳断档 ⇒ 全簇改选风暴**：一次 tick 串行推进全部组，每组对每个 peer 的 AppendEntries 是阻塞 PQexec；协调者被 4 个跨组 2PC 会话打满时一条 RPC 就是数秒，tick 超过选举超时（6 s）后本节点作为 leader 的所有组（含 group 0）的 follower 全部超时改选。叠加 N14 的让位退避只有 5 个周期（30 s）：让位的旧主一到期就再赢选举（日志最长）→ 再 -1 → 再让位，真正的新主每轮被打回 follower、在途升主前置作废重来 | N12 复现 run 4（09-17 17:23–17:25）：group 0 在 17:23:20 / :38 / :50 / 17:24:06 连续四次改选（term 229→233，四票全票）；数据组 102651 目标 :5434 赢 term 2（17:23:57）、term 4（17:24:26），两次都在 14–21 s 内被旧主 :5433 以 term 3 / 5 抢走、旧主随即 `不可升主…主动让位并退避 30000 ms`，直到 :5435 term 6 才登记（13 s）；[F] 第 1 轮 180 s 未登记。:5434 两段 leader 期日志里没有任何升主前置的痕迹（静默排队/连接中） | **P1** → **已做（T7.38，随 N12 之二部署）**：① 让位后把 election_deadline 推远 5 个选举周期（时间退避，旧主退避期内不参选）；〔2026-09-18 更正：①的"到期先查回放槽位是否 armed"最初用了 **BGW tick 里直接 SPI**，而 tick 无活动事务 → `GetTransactionSnapshot()` 段错误、pg_raft BGW 反复 signal 11；且与退避冗余，已撤除，回到纯时间退避〕；② `pg_raft_consensus_tick` 计时，超过选举超时一半即 WARNING `共识 tick 耗时 N ms…最慢的组 G`（10 s 限流），让心跳断档在日志里可见；③ 升主前置发出 / 排队 / 返回各打一行 LOG。**同步 RPC 本身没改**（改成逐 peer 非阻塞是 Raft 核心改动，另立项）；CPU 饱和下的改选风暴仍会发生，只是不再由旧主反复打断新主 |
| **P7-N18** | **分片 xid 跨任期重号 ⇒ 判决落到错误事务 ⇒ 2PC 跨分片原子性被破坏（切主后小额总额漂移）**：新主发号起点本应取 Max(交接来的 next_xid, 影子)（ShardXidClaimOnPromote），但一组内**连续两次切主**时，第二任新主在尚未把第一任主的已提交尾部复制干净就升主（可用性优先放行），分片 xid 水位陈旧 ⇒ 重发第一任已用过的 xid。分片 clog 以分片 xid 为键，重号 ⇒ 后到判决盖掉前一笔 ⇒ 转账一侧借记丢、贷记留 | run 5（09-17）：组 102654 连切 :5433→:5434→:5435，跨节点对读分片 clog：同一 xid 在 :5434=COMMITTED(sts=149)、当前主 :5435=ABORTED(sts=0)，xid 36–43 相反，start_ts 各不同 ⇒ 两笔不同事务撞同一 xid。协调者 sum(n)=24002（期望 24000），逐分片 −1/+1/+2；[G]/[H] 全绿（各主自洽、无残留）。run 2 同类漂移 −1，原始 N12 首查即疑"切主后分片 xid 重复分配" | **P0** → **✅ 已修（2026-09-18，两处，`905dc9c`）**：① follower append 收到带发号水位的 MARKER 即抬水位（覆盖 MARKER 类未 apply 尾巴，`raft_boundary.c`）；② 升主前置认领前调 `shard_absorb_tail_xids` 扫 (applied,flush] 解码 DATA 记录取 xmin/xmax 分片 xid + MARKER 尾块、一并抬过水位（覆盖单分片 heap 写的号），ClaimOnPromote 随后把无提交标记的改判 ABORTED。**验收**：hardened `test_failover_verdict_p7n12.sh` 一组连切两次+并发，修前重复行/总额 +1006，修后**三分片各 8 行零重复、零 reissue 撞号**（残留 -1 = 一笔 in-doubt，属 N13） |
| **P7-N19** | **激进连切下总额偶发 -1（2PC 跨分片原子性剩余缺口）**：某笔跨组转账**一侧生效、另一侧没有**，净少 1。与 in-doubt 无关——现场 **0 PREPARED 残留、行数 8/8 无重复行、无节点崩溃、无 reissue 撞号**，即判决通道、重号、崩溃三条已知路径都已排除。发生条件与 N12/N13 同款：一组内连续两次切主 + 4 会话并发跨组转账 | N13 修复后的验收（2026-09-18）：`test_failover_verdict_p7n12.sh` 连切两次，[H] 三组全部无 PREPARED 残留、逐分片行数 8/8/8 且 count=count(distinct)，但协调者 `sum(n)` = 23999（期望 24000），逐分片 7997/7998/8004。此前同一用例在 N13 未修时的 -1 可由"1 笔 in-doubt 挂着"解释，现在解释不通了 | **P1** → **未修，仅登记（2026-09-18 用户指示：先记录不定因）**。下一步定因方向：①逐笔比对转账流水与两侧账户增量，定位是哪一笔、哪一侧丢；②看是否落在"切主瞬间已 PREPARE 但决议下发撞上任期栅栏"的窗口；③排除测试夹具自身（失败重试的幂等性）|
| **P7-N20** | **旧主残留的"未入流号"本地 clog 与新主重发的同号冲突，且终局态不可覆写 ⇒ 该节点再升主会把已提交行读成 ABORTED（潜伏）**：旧主发了分片 xid N、本地 clog 已落终局（如 ABORTED），但它那批 raft 条目**没提交就丢了领导权被丢弃**——号没入流。新主的发号水位（含 N18 的 append 抬水位 + 升主吸收尾巴）只能覆盖**入过流**的号，于是合法地重发 N 给一笔新事务并提交。新主与其余追平副本记 N=COMMITTED；**旧主本地那条 N=ABORTED 是终局态，`ShardClogSetVerdict` 不覆写终局**，即便它已作为 follower 追平也留着。此刻无害（重发号的设计论证成立：旧元组不在本分片宇宙里）；但**一旦该节点再升主，用 N=ABORTED 解释重发事务的元组，已提交的行就会整片不可见** | 扩展验收 [9] 同组连切两次（2026-09-18）实测：组 102741，三节点 applied 均已追平 flush=109，仍有 2 处——`xid 6`：`:5433 st=2 sts=55 cts=58`、`:5434 st=3 sts=37 cts=0`、当前主 `:5435 st=2 sts=0 cts=58`；`xid 7` 同形。同轮 **无重复行、总额守恒、无 PREPARED 残留**，即尚未显形 | **P1（潜伏）** → **未修，仅登记（用户指示：先记录不定因）**。定因/修法方向：①升主前把"本地终局态与流里判决冲突"的分片 xid 找出来并以流为准重写（终局不可覆写这条规则要为"本节点从未入流的号"开一个例外）；②或发号时把"本地 clog 已用过的号"也纳入水位下界（代价是跳号）|
| **P7-N21** | **`DROP TABLE` 可能落进不可中断的热自旋 ⇒ backend 永久占用且 `pg_terminate_backend` 无效**：DROP 在 pg_partdist 的删表通知/交接路径里原地空转，该循环**不含 `CHECK_FOR_INTERRUPTS`**，SIGINT/SIGTERM 均无效——`pg_terminate_backend()` 返回 `t`，backend 继续跑；**唯一回收手段是 `pg_ctl -m immediate` 重启该节点**（已实测有效）。〔2026-09-18 补正：最初记为"清理顺序颠倒（组先于表）必然挂死"，**过强**——同日手工清场正是先拆组后删表，9 张残壳全部删成，只是每张约 30 s。故触发条件尚未锁定，顺序只是相关因素之一〕 | 扩展验收收尾（2026-09-18）:5434 上 `SET citus.enable_ddl_propagation=off; DROP TABLE …` 持续 56 分钟未返回；`pg_stat_activity` 里 `state=active`、`wait_event_type`/`wait_event` **均为 NULL**（不是在等锁，`pg_blocking_pids` 为空）；`ps` 显示 `STAT=Rs`、`%CPU=55.8`、累计 CPU **31 分 24 秒** ⇒ 用户态热循环。连发两次 `pg_terminate_backend(657811)` 均返回 `t`，3 秒后进程仍在 | **P2（可用性/可运维，非数据正确性）** → **未修，仅登记（延续用户"先记录不定因"指示）**。已排除：`ShardFilesetEmitDropNotices` 的目录扫描有界、`EmitShardDropNotice` 全程包在子事务 + `PG_TRY` 里且失败即 `return false`，自旋不在这两处。下一步定因方向：①`PartWALAppendCtrl` → raft 追加/等多数派在组已不存在时是否无限重试；②给该重试循环补 `CHECK_FOR_INTERRUPTS` + 上限。〔**2026-09-18 二次现场**（N22 负向对照收尾）：`SET statement_timeout='30s'; … DROP TABLE IF EXISTS rn22_102757` 在 :5434 上 active 10 分 25 秒、STAT=Rs、CPU 67%；**5 秒内 syscr/syscw 零增长**（`/proc/<pid>/io`）⇒ 纯用户态死循环，连 syscall 都不发（排除自旋锁：s_lock 退避会 `pg_usleep`）。**`statement_timeout` 同样无效**（它也是中断，不查中断就收不到）。进入自旋前该 backend 刚刷出一串 `分片 53631/61715/70058/61673… 已 DROP，已通知副本停流`（ProcessUtility 开头 `PartDistMaybeEmitPendingDropNotices` 给**历史残留**分片补发停流通知，每条都 `PartWALNoteTouchedPartition`）⇒ 疑点收窄到提交期对这批触达分区的逐个复制（`PartWALReplicateTouched` → `pg_raft_partwal_replicate`）。无栈可取：容器无 CAP_SYS_PTRACE、`perf_event_paranoid=4`、宿主机无 sudo。〕**规避**：❌ `statement_timeout` **不管用**（本条原写法作废）；挂住只能 `pg_ctl -D <datadir> -m immediate restart` 回收（已写入夹具规则 38）。〔**2026-09-18 深挖（用户指示"看看能不能深入挖掘原因修好"）**：① `statement_timeout` 无效的原因已定：PG16 `exec_simple_query` 在 `CommitTransactionCommand()` **之前**就 `disable_statement_timeout()`，PRE_COMMIT 回调里的任何循环都不受它约束——不是"不查中断"单独造成的；② 两次卡死前都刚刷完一串给**历史残留分片**的停流通知，定位到前置缺陷 **P7-N28**（见下，已修）；③ 逐个排查了 DROP 提交路径上的循环：目录扫描、EmitShardDropNotice、ShardFreezeEmitOne、PartWALFlushImpl、AssembleRawWALRecord、replicate_group_upto / data_propose_one（每轮都有 SPI+RPC）、wait_for_log_room（10 s 上限 + CFI）、replicate_claim（60 s 上限 + CFI）、PartWALSyncListPartitions（读写同锁）——均有界或带超时，**零 syscall 的那个循环没有在代码里找到**；④ 写了两套看门狗复现脚本（常规清场 / "失败保留现场 + 下一个用例拆全部组"）共 7 轮，**未复现卡死**；⑤ 取证工具已就位：新 GUC `pg_partdist.debug_sigusr2_backtrace`（`kill -USR2 <pid>` 把栈打进日志、进程继续跑，已验证能在 pg_sleep 中取到完整栈，偏移用容器内 gdb 对 .so 解析），pg-test 环境常开，下次复现即可采样定位。**根因仍未抓到，本条保持未修**〕⚠️ 注意"先删表后删组"并非总能做到——**组还在时，副本残壳的 DROP 会被路由守卫拒掉**（`分区主副本可能已切换，请经路由层重试`），只能先拆组再删壳，正好落进本项的风险面。〔**2026-09-19 ✅ 已修（根因已定）**：不是"零 syscall 的纯用户态死循环"——那条判断是**我取证错了**：`/proc/<pid>/io` 的 syscr/syscw **不统计 socket 上的 send/recv**，而 `pg_terminate_backend` 之后我只等了 3 s。真实机制：**拆掉的组被在途 RPC 按 hearsay 再建成空壳**（日志空、last_data_plsn=0），清场时正在 DROP 该组分片表壳的那条事务，提交路径上的停流通知要经这个"僵尸组"复制 ⇒ `replicate_group_upto` 从 plsn 1 起把整条分区流历史逐条重提（rn22 800 条、fm_* 几千条），期间领导权还在三台之间来回抖，每条都是一次 RPC——所以 CPU 高、syscr 不动、看起来像自旋；该循环不查中断、`statement_timeout` 又在提交前就被关掉，所以杀不掉。证据：09-18 15:16:20 开始的 DROP 期间 15:16:22 / 15:16:35 两次"创建 Raft 组"日志，:5434 在 flush 800 时当上 102757 的 leader，worker1 报 `propose plsn=5 失败`；N28 的通知风暴把它放大成分钟级。**修法**：①`pg_raft_group_drop` / `pg_raft_group_reset` 记**墓碑**（10 分钟），`raft_group_ensure` 的 hearsay 建组（AE / RPC 两个入口）见墓碑即拒、回 `0 0`；显式 `pg_raft_group_create` 与重启恢复清墓碑，合法重建不受影响；②`replicate_group_upto` 循环补 `CHECK_FOR_INTERRUPTS`（取消 / 终止收得到）。**回归** `test_group_drop_tombstone_p7n21.sh` 20/0：只在 C 上拆组、leader 心跳 15 s 不复活；**同一条合成心跳**对从没存在过的组会建出来（证明 hearsay 路径是通的、用例不空转）、拆过之后被拒；显式建组后 C 以 follower 追平；拆组后 DROP TABLE 1 s 返回。〕 |
| **P7-N22** | **回放 worker 的块数缓存在「副本 → 主 → 副本」后陈旧 ⇒ 截断留下残 buffer / redo 把真实页当成"文件外"**：回放 worker 进程级 `InRecovery=true`，`smgrnblocks()` 与 `DropRelationBuffers()` 都改信本进程的 `smgr_cached_nblocks`（`smgrnblocks_cached` 只在 InRecovery 下生效）。这个缓存成立的前提是"worker 是唯一写者"——节点当主那段时间扩文件的是普通 backend（只扩、不发 smgr inval），worker 手里停着当副本时的旧块数（偏小）。再降回副本后：① FULL_BASELINE 截断按旧块数只丢 `[0, 旧值)` 的 buffer，当主时写出的尾部 buffer 仍 valid，FPI 重建一扩就撞 `unexpected data beyond EOF in block N`，每 250 ms 重试永远失败；② 主权交接（一个字节都不截）后的普通 redo，块号 ≥ 旧值即被当成文件外的页——`RBM_NORMAL` 下 `log_invalid_page` 后**静默跳过**，或扩文件时撞同一个 EOF 报错 | 扩展验收 [9] 第 2 轮（2026-09-18 12:03）:5435 分片 62231（组 102751）`@plsn 14 收到全量物理基线，已截断全部 4 个本地文件` 之后 `追平失败: unexpected data beyond EOF in block 1 of relation base/5/62237`（62237 = 该分片主键索引），同一条报错每 250 ms 一次；:5435 早先经"稳定化"当过 102751 的主。其升主前置因此 `返回 0` 连续 65 s、走兜底 force | **P0（副本静默不一致 / 升主卡死）** → **✅ 已修（2026-09-18）**：新增 `ReplayForgetCachedSizes()`，在三处把本进程对这些关系各 fork 的块数缓存置 `InvalidBlockNumber`——认领加载 locmap 时、FILESET_UPDATE 换表时（含交接）、`ReplayTruncateLocalRel` 量块数之前；下一次 `smgrnblocks` 取真值，`DropRelationBuffers` 按真实块数丢 buffer。回归 `test_replay_stale_nblocks_p7n22.sh`（副本→主→副本→交接追平→重供基线，逐字节比堆与主键索引）：**修后 29/0；负向对照（把 ReplayForgetCachedSizes 改成空函数）20/9**——交接后回放卡在 59/790、日志 934 条 `unexpected data beyond EOF`，重供基线后卡在 793/798、1725 条，堆与索引逐字节均不一致 |
| **P7-N23** | **未交出主权的前任主再次当选，升主前置空转 60 s**：数据组里 A 是在册主（本地"已升主"）；B 赢得选举但**还没登记就丢了领导权**，A 没收到"主权已交给 B"，本地仍是已升主；A 再次当选时 `promote_prepare_ex` 见 `bound > app` 就去 `replay_catchup`——而已升主的槽位 `replay_catchup` **一律拒**（`已升主，不再回放别人的流`），于是每轮 RETURN 0，空转满 `promote_catchup_deadline_ms`（60 s）才走兜底 force。等待毫无收益：追平在此状态下**注定失败**，兜底最后也是跳过它 | 扩展验收 [9] 第 2 轮：:5434 于 12:03:45 以 term 6 夺回组 102747，12:03:57–12:04:59 连续 16 次 `升主追平 shard 62161 (组 102747) 失败: replay_catchup: shard 62161 已升主，不再回放别人的流`，12:05:03 兜底放行、12:05:05 登记 | **P1（可用性：一组 60 s 无主）** → **✅ 已修并实测（2026-09-18）**：槽位新增 `promoted_clean`——本进程生命期内升主时置真，降级或 follower append **真正落下**一条他人写的 DATA/MARKER（`AppendPartWALRecordAt` 返回真，重传去重不算）即置假，重启后不恢复（未知按不干净处理）。新函数 `partdist.shard_promoted_selfheld(oid)`；`promote_prepare_ex` 在 `bound > app` 且 selfheld 为真时判定 `(app, bound]` 全是本节点当主时自写、已在堆里，跳过追平并打 LOG；收到过他人记录则照旧追平/兜底（与修前一致，不放宽）。**实测**：`test_repromote_selfheld_p7n23.sh`——单组用例里两种自然构造都推不动 `follower_applied`（leader 自写记录走 in_txn apply 不写这一列；中途当选者追加 DTX 也没推动），故用产品函数 `follower_set_applied_part_lsn` 把 A 的 raft 已应用游标推到它自己的 flush，合成 [9] 现场的前提（bound=96 > app=57、尾巴全是自写、selfheld=t）；再 disarm B 让它当选后 -1 让位、从不登记，随即切回 A。**修后 20/0：3 s 重新登记、日志有"跳过追平"**；**负向对照（换回修前 promote_prepare_ex）17/3：63 s 才登记、56 条"已升主，不再回放别人的流"**，与 [9] 现场一致 |
| **P7-N24** | **只有节点级切主手段 ⇒ "切一个组"实际切了全节点所有组**（验收夹具缺陷，根在缺一个产品接口）：此前受控切主只能把旧主 `pg_raft.heartbeat_ms` 调到 60 s、目标 `pg_raft.election_timeout_ms` 调到 300 ms——两个 GUC 都作用于**节点上的全部组**：旧主当主的每个组一起断心跳，目标又以 300 ms 超时在 2 vCPU 抖动下顺手抢走第三台的组。升主前置在同一条异步连接上**串行**排队 | 扩展验收 [9] 第 2 轮：一次"切组 102747 到 :5435"把 102747/102748/102749/102750/102751/102752 六个组全压到 :5435（12:03:10–12:03:22 逐个当选），102747 的升主前置排在 102751（卡在 N22 上 65 s）后面、始终没轮到；:5435 在 102747 上无心跳 26 s 后被 :5434 以 term 6 夺回 ⇒ **"第 2 轮：新主登记"FAIL 的直接触发** | **P2（可运维）** → **✅ 已补（2026-09-18）**：pg_raft 新增 `partdist.pg_raft_group_campaign(group_id)`——给**该组**置 `campaign_pending`，本节点下一个 tick 无条件发起一次选举（tick 读后清零）。〔首版只把选举截止期置到期，被现任 leader 的下一次心跳 `reset_election_deadline_locked` 推回去、选举根本没发起（N23 回归首跑实测），改为独立标志，`RaftConsensusShmem` 加一个字段，整簇重启部署〕；投票规则不变（更高任期 + 日志至少一样新即投），旧主见更高任期自行退位，别的组不受影响。本节点已是 leader / 非成员时不动。全方位套件 [6]/[9]/[11] 与 N22 回归改用它（`switch_to`） |
| **P7-N25** | **被降级的原始主不会自动变回副本 ⇒ 切主一次后，该分片的候选池少一台，直到人工重供** | 原始 placement 主从没当过副本，没有回放槽位/locmap；被新主登记降级后它照收新主的流、却无从回放，按 R-P4-15 设计升主前置对它恒返回 -1（`本节点已收到分区 WAL 到位点 N，却没有回放槽位…须重做物理基线后才能重新参选`）并主动让位。没有任何自动路径替它重做基线（新主的 fileset 交接广播到它那里也没人用）。3 成员组里等于第一次切主后只剩一个合格候选 | N22 回归首跑（2026-09-18）：原始主 :5433 被 :5434 登记降级后，受控切回它 ⇒ `分片 177230 (组 102754) 拒绝升主：本节点已收到分区 WAL 到位点 578，却没有回放槽位` ⇒ -1 让位，:5434 夺回；全方位套件 [6]/[9]/[11] 的受控切主目标若恰是原始主同样会中招（首批跑碰巧被"稳定化"重供过才没暴露） | **P2（可用性）** → **✅ 已修（2026-09-18）**：新主在"本节点被登记为主"的那次 apply 里拉起**一次性动态后台工作者**（不走 TopologyMonitor——那条路阻塞 PQexec、与共识 tick 同一循环，一次供给数秒会让本节点所有组心跳断档），经 libpq 自连调新函数 `partdist.reprovision_demoted(gsid, 0)`——**逐个检查全部从副本**（不依赖条目里会失真的 old_primary，P7-N27）：本节点不是在册主 ⇒ not_leader；成员有 armed 且回放不在 failed 的槽位 ⇒ not_needed；否则 `provision_shard_replica` 替它重供基线（首轮等 5 s，之后每 15 s 重试，约 5 分钟，覆盖旧主崩溃后晚归）。`provision_shard_replica` 加分片级事务咨询锁，与人工/夹具供给串行。GUC `pg_partdist.auto_reprovision_demoted`（默认 on）。回归 `test_demoted_primary_rejoin_p7n25.sh`：**AUTO=on 28/0（P7-N1 之二修后连跑 3 次均 28/0；修前 4 次里 2 次撞流内空洞）**（旧主 3 s 得到 armed 槽位、追平、堆与主键索引逐字节一致、再写照常追平、切回旧主当选并登记、立即可写）；**负向对照 AUTO=off 21/7**（旧主一直无槽位、追不上、再写后逐字节不一致、切回当选但登记不上）。〔原登记：受控切主前 `ensure_candidate` 检查目标有无 armed 槽位，没有就先从当前主 `provision_shard_replica` 重供。产品侧方向：新主登记后对"无 locmap 的前任主"自动发起一次 `provision_shard_replica`（与 T7.27 心跳自动重做基线同一条路）〕 |
| **P7-N26** | **当过主又当回主的节点把前任写的整段尾巴逐条重新 propose ⇒ 持复制认领位数十秒到数分钟，期间写入全部排队超时** | `last_data_plsn`（本组已 propose 的最大 plsn，prepare 复制的增量下界）是运行期游标，只在本节点当 leader 且 propose 成功时推进，**丢主不清零**。再次当选后 `replicate_group_upto` 从上一任期的旧值 +1 起，把前任主写的、本节点早已作为 follower 收进日志的记录逐条再提一遍，每条一次同步往返 | N25 回归（2026-09-18）切回原始主 A：`partdist.raft_log` 里 term 3 的 93 条 OP_PARWAL 中 **77 条与 term 2（B 任期）plsn 140–216 同号**；A 的主权交接广播持复制认领位 110 s，协调者 `INSERT` 报 `等待组 102768 的复制认领位超过 60000 ms，prepare 失败` | **P1（可用性）** → **✅ 已修（2026-09-18）**：当选即 `data_group_refresh_last_plsn()`——从环内最后一条 OP_PARWAL 取 plsn，只抬不降（那段尾巴由常规 AppendEntries 复制，不该作为新条目再提）；日志 `当选后把复制下界从 plsn X 抬到 Y`。N25 回归实测 138→214，切回后立即可写 |
| **P7-N27** | **登记条目里的"前任主"取自控制面 leader 当时的 partition_map，apply 滞后时为 0 ⇒ 旧主从不被降级** | `pg_raft_report_data_leader` 在当时的 group0 leader 上读 partition_map 填 `old_primary_node`；group0 leader 的 apply 可以落后于 commit（实测 worker1/2 把 A 的 term 1 与 B 的 term 2 登记 76 s 后同批 apply），B 的上报落在还没 apply A 登记的 leader 上 ⇒ 条目写 `old_primary=0`。后果：旧主 A 的"主权已交出"分支从不执行，**本地一直以为自己是主**（读闸门开着、拒绝回放）；新主侧也因"没有前任主"不拉自动归队 | N25 回归第 2 跑：各节点 `apply partition primary partition=102769 old_primary=0 new_primary=3 primary_term=2`，A 无"主权已交给"日志，自动归队未启动；第 3 跑同样的滞后再次出现（说明是常态），已被修复纠正 | **P0（角色错乱：被取代的主不自知）** → **✅ 已修（2026-09-18，二改）**：①降级不再只看条目里的 old_primary——`partwal_notify_primary_switch` 在"新主不是我、而我本地'已升主'持久标记为真"时同样降级（本地标记才是要纠正的真状态）；②`pg_raft_apply_partition_primary` 只在条目的 old_primary=0 时用本地已应用的上一条登记**补缺**、不覆盖；③P7-N25 的自动归队改为**逐个检查全部从副本**的 armed 槽位，不依赖 old_primary。〔首版"一律以本地行为准"在最终回归里栽了：本地行也会陈旧（P7-N29），worker3 读到的上一条是被回滚的 term 2 主，反把真正被取代的 C 漏掉了降级〕 |
| **P7-N28** | **停流通知不收敛：每个会改 fileset 的 DDL 都给全部历史残留分片重发一遍 DROP 通知** | `ShardFilesetMaybeEmitUpdates`（PRE_COMMIT）遍历共享内存分区清单，表没了就 `EmitShardDropNotice`，**不看 `dropped` 哨兵**（目录扫描那条路径看）；清单里的残留分区永不移出。每条通知 = 一次 CTRL 追加 + fsync + `PartWALReplicateTouched`（对本事务已触达的全部分区各调一次复制挂钩，N 条就是 O(N²)） | 2026-09-18：分片 61673 的哨兵 07:34 已在，11:27、15:16 仍被重发；N21 复现脚本 15 分钟内三台 worker 各发 **700–1000 条**停流通知，绝大多数给早已通知过的残留分片；这些死分区的 parwal 段随每次 DDL 继续增长。两次 N21 卡死前都刚刷完这样一串 | **P2（资源/日志/复制风暴，N21 的前置）** → **✅ 已修（2026-09-18）**：两个发射点共用 `EmitShardDropNoticeOnce()`——先由调用方确认表没了，再看哨兵、发射、落哨兵；OID 被复用时旧哨兵也挡不住新表的 fileset 变更 |
| **P7-N29** | **group0 条目的 apply 跑在承载它的 RPC 会话事务里，事务后来回滚 ⇒ 本地 partition_map 撤销、raft last_applied 却已推过去 ⇒ 该节点元数据与日志永久分叉** | apply 的 SQL 效果（partition_map UPSERT、路由层改写等）与 raft 的内存游标不在同一个原子单元里。同一会话事务里后续动作一 ERROR（实测：apply 里的主权交接广播等复制认领位 60 s 超时），前面已 apply 的条目的 SQL 效果全部回滚，而共享内存里的 last_applied 不回退、不会重放 | N25 回归最终批（2026-09-18）：worker3 的 backend 784502 于 18:12:22 apply 了 C 的 term 3 登记（3→4），18:13:37 同一会话 `ERROR: 等待组 102792 的复制认领位超过 60000 ms` + `FATAL: connection to client lost`；此后该节点读到的"上一条登记"仍是 term 2 的主 3 | **P0（元数据静默分叉）** → **✅ 已修（2026-09-19）**：group0 条目在 RPC 事务里 apply 时记下"本事务从哪个 last_applied 起推进的"，挂事务 / 子事务回调——**事务（或 apply 所在层级的子事务）回滚时把 last_applied 退回原值并落 hardstate**，WARNING `承载控制面 apply 的事务回滚，apply 游标从 X 退回 Y，这些条目将重新 apply`，下一轮 apply 把这些条目重放；提交 / PREPARE 时清记录。**实测含负向对照**：旧版本在 group0 leader 上 `BEGIN; SELECT partdist.pg_raft_propose_partition_primary(999000001,3,ARRAY[4]); ROLLBACK;` ⇒ leader 本地没有这一行、其余节点有，四台 applied 都是 638（永久分叉）；新版本同样操作（999000002）⇒ WARNING「apply 游标从 639 退回 638」、随即重放，四台都有 002/003 两行、applied=640。假行已清。 |
| **P7-N30** | **兜底放行（force）的升主照做"顺带修复分叉标记"：从没追平的本地数据发全量物理基线，覆盖健康副本 ⇒ 已提交数据丢失** | `promote_prepare_ex` 在追平之后调 `repair_diverged_shards()`，注释的前提是"追平刚做完"——force 分支恰恰没追平；`repair_diverged_shards` 本身也只看"本节点是 raft leader + 多数派在"，不看本地数据是否权威（在册主）。心跳自动修复（T7.27）走的也是它 | N25 回归最终批（2026-09-18）：原始主 A 自动归队后流内空洞（期望 plsn 149、下一条 150）追不上；受控切回 A，A 当选、60 s 后转兜底，force 这一轮在 plsn 221 发出全量基线；副本 C 截空后按 A 的 120 行重建，随后 C 当选登记，经协调者只读到 **120 行（应为 180）**——B 任期内写的 60 行已提交数据不可见 | **P0（丢已提交数据）** → **✅ 已修（2026-09-18）**：①`promote_prepare_ex` 记 `caught_up`，兜底放行未追平时跳过顺带修复并打 LOG；②`repair_diverged_shards` 只让**在册主**（本地'已升主'标记为真）发基线，其余记 `not_primary` 留待之后；③`reprovision_demoted` 同样要求在册主。残留风险：人工 `provision_shard_replica` 只查 raft leader，须按规程在在册主上调用 |
| **P7-N31** | **切主登记生效的一瞬，协调者路由已翻到新主、新主自己那份登记还没 apply ⇒ 读闸门仍当它是副本、拒读** | 各节点独立 apply 同一条 group0 登记，协调者先 apply 就先改 pg_dist_placement；新主的"已接管为主、解除副本读闸门"要等它自己 apply 到这条 | N25 回归（2026-09-18 18:42:48）：协调者读 rn25 被新主 :5433 拒（`不允许在本节点上对副本壳表（OID 202056）执行查询`），同批 apply 在其后 ~0.3 s | **P2（可用性，亚秒级读失败）** → **✅ 已修（2026-09-19）**：升主前置返回 1 时标"升主在途"15 s（`partdist.shard_mark_promotion_pending`）；读闸门见到在途标记就**等本地登记 apply 把"已升主"置上**（20 ms 轮询、可中断、15 s 上限），等到即放行，等不到（被任期栅栏挡下 / 别人当选）照旧拒——没有提前放开闸门，登记失败时不会读到副本。**回归** `test_failover_read_gate_p7n31.sh`（确定性构造）：在新主 T 上开事务 `SELECT … FROM partdist.partition_map WHERE partition_id=<分片> FOR UPDATE` 持锁 5 s，挡住 T 自己那份登记的 apply；协调者照常 apply、路由翻到 T（断言此刻 T 本地仍记旧主 = 窗口构造成功）；窗口内每 200 ms 经协调者读 ⇒ **0 次拒读**、最长一次读等了 5.1 s（走的是等待路径），16/0。**负向对照** `NEG=1`（只在 T 上换一版不打标记的 promote_prepare_ex，清理时换回）⇒ 窗口内 11 次拒读。〔用例第一版的"0 拒读"是**假绿**：`tail -3` 截掉了 ERROR 行，拒读全被记进"其它错误"；第二版按读的次数而非墙钟放锁，第一条读在闸门里等满 15 s 上限后才开始拒——都已改掉〕 |
| **P7-N32** | **追平失败一律当"还没追完"：流内空洞这种永久失败也会在 60 s 后被兜底 force 登记成主 ⇒ 登记的是明知残缺的数据** | `promote_prepare_ex` 捕获 replay_catchup 的错误后 RETURN 0，满期限转 force、照做其余步骤后返回 1。force 的本意是"追得慢"时保可用；空洞等多久都补不上，force 等于把残缺数据登记成主。在册之后心跳自动修复还会把这份残缺基线推给健康副本 | N25 回归（2026-09-18）：A 流内空洞，被切回后 60 s force 登记，协调者只读到 120 行（应为 180）；随后 A 以在册主身份发出基线（plsn 225），B/C 被覆盖 | **P0（丢已提交数据）** → **✅ 已修（2026-09-18）**：追平报"流内空洞"即 RETURN -1（兜底也不放行）⇒ 主动让位、由追平的成员当选；自动归队把"armed 但回放 failed"的成员也视为要重供。回归 `test_hole_no_force_promote_p7n32.sh`（在 A 上删分区流段注入空洞，反复受控切向 A 跨过 60 s 兜底：A 不得登记、协调者读到全部行） |
| **P7-N33** | **切主的那 1–2 s 里旧主拒读**：各节点各自 apply 同一条「谁是主」的登记，先后差 0.2–1.9 s（实测）；旧主一 apply 就关上自己的副本读闸门，而协调者那份路由可能还指着它 ⇒ 这段时间落到该分片的读全被判死 | 与 N31 是同一条缝的另一侧：N31 修的是**新主**还没 apply 就被路由指过来，N33 是**旧主**已经 apply 了、路由还没挪走 | 演示第 13 步手工切主（2026-09-19）：旧主关门 0.9 s 后协调者仍在往它身上路由，读报 `不允许在本节点上对副本壳表（OID …）执行查询` | **P2（可用性，1–2 s 读失败）** → **✅ 已修（2026-09-20）**：刚降级、且本地还没回放过新主的流时放行读（这一格就是它交出主权那一刻的已提交状态）；一旦回放推进（applied 变了）或宽限 `pg_partdist.demoted_read_grace_ms`（默认 10 s）到期，立即恢复拒读。**回归** `test_demoted_read_grace_p7n33.sh`：在协调者上持 `partition_map` 行锁 + `lock_timeout=300ms`，让它的登记 apply 反复失败、路由停在旧主身上，构造出真实量级（1.5 s）的窗口 ⇒ 2 轮切主 **0 次拒读**，26/0；**负向对照** `NEG=1`（宽限=0）⇒ 同样窗口里 **20 次拒读**。〔已知边界：宽限只保「降级 → 新主把它重新供成副本」这一段；副本流一到、本地回放一推进，宽限按设计立即失效——那之后它的页正在被新基线覆盖，本来就不能读〕 |
| **P7-N34** | **宕机切主后新主被「自己等自己」卡成分钟级不可写**：控制面 apply 的主权交接段**握着 `partition_map` 的行锁**去等该分片的复制认领位；认领位是本扩展在 shmem 里的位，PG 的死锁检测看不见，只能干等 60 s。更糟的是认领位不可重入，同一个 backend（升主的 `repair_diverged_shards` → 控制面 apply）再申请一次就是在等自己 | 认领位（`replicate_claim`）只记「有没有人持有」，不记持有者是不是自己；控制面 apply 与写入路径抢同一个位，且前者还握着行锁 | 演示第 14 步（2026-09-19）：`1139380`（group0 的 AE apply）握着 partition_map 行锁等认领位，30 个 AE RPC 排队，`repair_diverged_shards` 持位，连撞三轮 60 s 超时，约 **6 分钟不可写** | **P1（可用性，分钟级）** → **✅ 已修（2026-09-20）**：①认领位可重入（同 backend 记深度，不等自己）；②持有者就是自己时直接回收；③控制面 apply 里的等待上限压到 3 s，等不到就放手回滚、由下一次 AE 重做（这条「重做」要靠 N37 才真正成立）。**回归** `test_claim_reentrancy_p7n34.sh`：3 分片打标表 + 持续写入 + immediate 停主 ⇒ 宕机后 **2.0 s** 恢复可写、全集群 **0 条** `复制认领位超过 60000 ms`，24/0 |
| **P7-N35** | **切主丢已确认的提交（P0）**：升主追平的上界取的是「本节点 `applied_part_lsn`」，而这个游标可能落在「日志里已有、也已提交」的尾巴后面 ⇒ 那截不回放 ⇒ 随后 `shard_claim_on_promote` 按「流里没有提交标记 ⇒ 从未提交」把它的分片 xid 改判 ABORTED，客户端已收到成功的那笔就此消失 | 两处叠加：①从节点的 apply 可能滞后于 commit_index（apply 是一次 SQL UPSERT，会被锁或错误挡住）；②旧主死在「多数派已确认、commit 通知还没送到」的那一拍时，新主压根不知道尾巴已提交（Raft 教科书情形，缺本任期空条目） | 演示第 14 步（2026-09-19）：标记 plsn 64 三个节点都有、带 `shard_xid=10 cts=55 flags=15`；新主日志「追平至 63」，其分片 clog 里 xid 10 = `st=3 ABORTED`；该行切主后先可见、新主第一次写触发认领后消失 | **P0（丢已确认的提交）** → **✅ 已修（2026-09-20）**：①升主前置先提交一条**本任期**的空条目（Raft 标准动作），上一任期的尾巴随之提交；②上界改取 `max(applied_part_lsn, pg_raft_group_committed_plsn())`——后者是新加的函数，从环里扫出「已提交的最大数据位点」，不依赖 apply 游标。**回归** `test_commit_survives_failover_p7n35.sh`：在从节点上持 `follower_partition_map` 行锁 + `lock_timeout=300ms` 让它的 apply 快速失败（条目照进环、commit_index 照推进、只有 applied 卡住），确定性造出「已提交未应用」窗口后停主 ⇒ 该笔提交存活、分片 clog `st=2`，31/0（另含 2 轮「写完立刻宕机」的端到端轮次）；**负向对照** `NEG=1`（换回不含本修的 `promote_prepare_ex`）⇒ 同样窗口下「追平至 33」、xid 判 `st=3 ABORTED`、**该行丢失** |
| **P7-N36** | **重启后同一个 Raft 组被建出两个槽位**：其中一份是没有日志的空壳，它发起竞选把全组任期顶上去（实测 2→12） | `raft_group_ensure` 先在锁外扫一遍「这个组在不在」，没找到就进锁分配槽位——两个 backend 同时进来就各分到一个槽位（经典的缺「锁内复查」） | 演示重启阶段（2026-09-19）：同一毫秒两行 `创建 Raft 组 102895（槽位 2）`/`（槽位 4）`，3 次重启复现 2 次 | **P1（选举被干扰、组状态分裂）** → **✅ 已修（2026-09-20）**：进锁之后再复查一遍，已经有了就直接返回那一份。**回归** `test_group_slot_dup_p7n36.sh`：3 分片 3 组，连续 3 轮「整簇 worker 重启 + 8 个并发会话同时唤醒组」 ⇒ 重复槽位 **0**、日志里同组被创建两次 **0**、任期每轮只 +1（正常选举），23/0 |
| **P7-N37** | **控制面 apply 抛错即跳过条目 ⇒ 一次瞬时失败让该节点永久错过一条登记**（修 N33 的回归时揪出）：`ok = (group_id == RAFT_CONTROL_GROUP)`——控制面条目 apply 失败也算「已应用」，游标照推，这条「谁是主」在该节点上再不会重放 | 数据面是「apply 成功才推游标」，控制面为了「毒条目不能永久堵死控制面」选择直接跳过，但没有区分**瞬时**失败与**永久**失败 | 2026-09-20 构造 N33 窗口时：协调者 apply `OP_PARTITION_PRIMARY` 撞上一把行锁、300 ms 锁超时 ⇒ 日志 `控制面按既有语义跳过`，放锁之后登记**再没追上来**，它的路由从此指着旧主 | **P1（控制面静默分歧）** → **✅ 已修（2026-09-20）**：同一条目连续失败先保留游标重试，只有连续失败超过 60 s 才按老语义跳过并打醒目 WARNING（重试账本放共享内存——每次 AE RPC 是**不同的 backend**，进程内静态变量等于永不放弃）。**回归**：并入 `test_demoted_read_grace_p7n33.sh`（该用例的窗口正是靠「控制面 apply 反复失败」撑开的）——断言放锁后登记追上、且日志里确有 `P7-N37` 重试告警；修前同一构造下登记永不追上 |
| **P7-N38** | **拆组风暴里 `pg_raft_group_status()` 段错误（待定因，只登记）** | 未定因。形态是"一边拆组/删表、一边有会话按 group_id 查状态"，崩的那条是 `SELECT last_log_index FROM partdist.pg_raft_group_status() WHERE group_id = <gid>`；同一后端此前刚打过 `冻结账目发射失败…本节点不是该分区组的 leader` | 2026-09-20 05:45:58 worker1：`server process (PID 1364971) was terminated by signal 11`，DETAIL 即上述语句；当时 `test_full_mixed_multiprimary_p7.sh` 正在清场（连续 DROP 分片壳表 + 拆组）。**这是本轮 4 小时高强度测试里唯一一次**（其余套件的 `health_check_no_crash` 全绿），该节点 09-18 另有同类崩溃史（多为 pg_raft BGW） | **待定因** → 已打开 `pg_partdist.debug_segv_backtrace`（PGC_POSTMASTER，全节点 on，需整簇重启才生效——2026-09-20 已重启）以便下次崩溃留下栈；定向复现（6 轮 × 6 组："并发查 status + 同时拆组"）未复现。**排除项**：共享内存尺寸不是原因（`pg_raft_consensus_shmem_size()` 按 `sizeof(RaftGroupState)` 推导，N37 给 `RaftLogShmem` 加字段会自动变大）；N36 的锁内复查在 `raft_group_ensure` 里，与 `group_status` 不同路径 |
| **P7-N8** | DTX 参与登记、pg_raft 各处自连本节点用 `connect_timeout=1`；负载高时连自己超 1 s 即 `参与登记失败，中止 prepare`，整条分布式事务失败 | T7.35 回归批量跑 `raft_groups_p7` [3] 实测（单跑不复现） | 可用性，P2 |
| **P7-N7** | 跨组事务在 2 vCPU 上极慢，且 Citus 分片修改锁把并发跨分片事务串成一条队 | 单事务 1500 行 229 s；8×10 个小跨组事务 843 s；协调者 7 个会话排在 `0/<shardid>/5` advisory 锁上 | 性能，待与不并跑 tx2 的干净环境对照 → **◐ 根因之一已修（T7.35）**：复制取字节/取头、follower 查重都经 `partwal_find_record`，每条 O(n)，整体 O(n²)；稀疏索引之后同一用例写 1400 行 497 s → 294 s（tx2 已停的同一环境）。**剩下的大头是逐条同步往返**（每条记录：写 SQL 日志 + 同步复制等多数派 + 标记提交 + 应用），未动 |

**四、T7.35（P7-N4/N6）回归（2026-09-17，9 节点 `pg-test-container-9w`，新二进制 `d9e792a`）**

按改动面挑 24 套（切主/升主 9、复制与读流 9、分布式事务 6），`run_p6_exit.sh` 一次串行跑完：
**PASS=943 FAIL=3，仅 `raft_groups_p7` 不干净**（8/3）。同样 24 套在 09-15 基线（c815487）是 12 个 FAIL。

| 套件 | 09-15 基线 | 本次 | 备注 |
|---|---|---|---|
| promote_p6 / handover_provision_p7 / promote_catchup_tx3 | 39/0 · 33/0 · 24/0 | 39/0 · 33/0 · 24/0 | 纪律要求必跑的三套切主 |
| promote_handover_p7 / fastpath_divergence_tx4 / negative_p6 / raft_membership_r1 | 35/0 · 20/0 · 40/0 · 18/0 | 同左 | 分叉 -1 永不放行路径仍有效 |
| replica_gate_p6 | 32/1 | **33/0** | P7-N1 本轮未触发，**不算已修** |
| follower_replay_r1 / lazy_replay_l1 / replay_bound_p6 / abort_page_p7 / partwal_ring_p7 / highload_w7 / divergence_mark_p8 | 58 · 57 · 23 · 97 · 14 · 12 · 21（均 0 FAIL） | 同左 | 读流索引下回放、逐字节比对不变 |
| txn_layer_r2 / ddl_fileset_d1 | 55/1 · 84/1 | **56/0 · 85/0** | |
| dtx_convergence_p4 / dtx_tso_p4 / dtx_replay_tx1 / dtx_commit_marker_tx2 / dtx_verdict_marker_p7 / clog_hole_c4 | 45/0 · 43/6 · 83/0 · 40/0 · 38/0 · 12/3 | 45/0 · **49/0** · 83/0 · 40/0 · 38/0 · **15/0** | |
| raft_groups_p7 | 11/0 | **8/3 → 单跑 11/0** | 见下 |

`raft_groups_p7` 的 3 条红都是连带：[2] 刚在 9 节点上把组建满（默认节点各 32 个、测试组到收尾才删），[3] 建表时 :5438 的
DTX 参与登记自连本节点报 `connection to server at "127.0.0.1", port 5438 failed: timeout expired` —— 该自连接的
`connect_timeout=1`（`pg_raft_format_conninfo`），2 vCPU 上几百个组并存时超 1 s 可达。该时段 :5438 日志无任何升主相关告警。
单跑 11/0。判为批量负载下的 1 s 自连接超时，**与本次改动无证据关联**，但"1 s 自连接超时导致 DTX 整条中止"本身值得登记（P7-N8）。

**五、跨组分布式事务验收（2026-09-17，`test_crossgroup_txn_p7.sh`，1c+3w，每台一主两从）**

前三跑作废、原因都在用例：一跑表未打标（撞 P7-N9）；二跑没配 TSO/没加 join 传播，2PC 在 PREPARE 被拒；三跑账户用
`INSERT…SELECT` 插入，撞出 P7-N10。四跑为有效结果：**63/6**。

| 段 | 结果 | 说明 |
|---|---|---|
| [1] 夹具 / [1b] 打标 / [1c] TSO | 全过 | |
| [2] 跨组提交、[3] 跨组 UPDATE+DELETE、[4] 显式 ROLLBACK、[5] 中途失败原子性 | **全过** | 单事务跨组语义在无并发、无切主时成立 |
| [6] 并发跨组转账 | ✗ 35/60 失败；✗ 总额 23998 | 失败：27 续租栅栏（P7-N11）、5 无主 RUNNING（P7-N13）、2 自发漂主期间写被拒；总额见 P7-N12 |
| [7] 副本逐字节 | 4/6 过 | 102633 在 [6] 漂主后副本 :5433 与新主不一致（P7-N12）；另一条是用例没跟随漂主（夹具问题） |
| [8] 无残留 prepared | 过 | |
| [9] 负载下切主 | 登记 28 s、零堆积、负载恢复 4/4（**P7-N4 修复在真实负载下成立**）；✗ 总额 23999 | P7-N12 |
| [9b] P7-N10 | ✗ 稳定复现 | |
| [10] 健康 | 无崩溃；捕获环零覆盖；日志环丢弃 3 | |

#### 1.11.1 P7-N18 根因固化（2026-09-18，`test_shard_xid_reissue_p7n18.sh`）

N12 两段丢失机理修完后（`98dac27`），run 5（一组内连切两次）仍见 +2 漂移。独立深挖后定因：
**"未追平就升主"窗口里的负载门控竞态**，四处代码对得上——

1. **重号护栏取的是 applied、不是 flush**。`ShardXidClaimOnPromote`（`shard_xid.c:1229`）把新主发号起点置为
   `Max(交接来的发号水位, 影子 next_hint)`。发号水位由 `replay_worker.c:510` 在 **apply 已提交条目**时
   `ShardXidRaiseAllocWatermark` 抬；影子由 `shard_replay.c:719` `ShardReplayNoteDataShardXid` 在 redo 时喂。
   两者都只覆盖本节点**已 apply**的分片 xid。
2. **升主追平上界是本节点自己的 applied 游标**。`pg_raft_promote_prepare_ex`（`pg_raft--1.0.sql:512`）
   `bound := get_follower_applied_part_lsn(loid)` = `follower_partition_map.applied_part_lsn`，
   `replay_catchup(bound)` 只追到这里 —— 不是流里已复制到的字节（flush）。
3. **applied 会落在 flush 后面**。follower 的 apply 跑在 `pg_raft_append_entries` backend 里，纯 2PC 负载下
   `data_apply_advance` 的 `in_txn_replication` 分支**跳过**推进 `applied_part_lsn`（`raft_consensus.c:1326`，
   为解 prepared 行锁死环，TX 期决策）；2 vCPU 饱和时 apply 本就滞后；availability 兜底（`p_force` 过
   `promote_catchup_deadline_ms=60000`）更是整段跳过追平。⇒ 升主时水位是**陈旧值**。
4. **重发 ⇒ 撞号 ⇒ 判决覆盖**。流里"已复制未 apply 的尾巴"带着旧主发过、已提交的 PREPARE MARKER 分片 xid，
   没进水位/影子 ⇒ 新主 `next_xid`（`shard_xid.c:1143`）偏小 ⇒ 重发旧主已用过的分片 xid。分片 clog 以
   分片 xid 为键（`shard_clog.c`），两笔不同事务（`start_ts` 不同）撞同一槽、判决互相覆盖；N12 之二的
   in-doubt 闭合按 `dtxid→sxid`（`partwal_prepare_sxid`）落账时更会把判决写到新事务上 ⇒ 2PC 跨分片原子性
   破坏、总额漂移。

**为什么静默集群下复现不出来**（`test_shard_xid_reissue_p7n18.sh` run 1/2 实测 `flush_B == applied_B`，无尾巴）：
不加负载时复制+apply 是亚毫秒级，早在目标选举超时（~1.5 s）之前就追平，尾巴不存在 ⇒ **单次干净切主不重号**
（该用例 [A] 段全绿即此负向对照）。N18 是负载 + 未追平升主下的竞态，正例为 run 5（总额 +2、跨节点同
分片 xid 不同 `start_ts`）。

**修法候选（供决策）**：
- **① 推荐 —— 护栏抬过"流里"而非 applied 的最大分片 xid**。`ShardXidClaimOnPromote`（或升主前置收尾）多扫一遍
  本节点分片流里 `[applied+1, flush]` 的 PREPARE/DATA 记录，把水位/影子抬过其中最大的分片 xid，再放行发号。
  正确性优先、不牺牲可用性（不阻塞升主，只保证不重号），改动集中在 `shard_xid.c` + 回放侧扫描，**触分片 xid 分配核心但外科式**。
- ② 未追平不得对外发号：把发号资格绑定 `applied == committed`。可用性代价（未追平的新主不能立即服务打标分片写）。
- ③ 分片 xid 高位嵌任期号，跨任期天然不撞。最彻底但改动面大（分配/clog/回放/可见性多处），回归成本高。
- 三者都落在 raft/2PC **冻结模块**核心，动手前须经用户批准解冻范围（见 [[feedback_raft_module_frozen]]）。

#### 1.11.2 剩余问题清单 + 全方位（混合主从 / 多主多从）测试就绪门（2026-09-18）

P7-N* 逐条现状（N1–N3 见 §1.10；此处列 N4 起）：

| 缺陷 | 优先级 | 现状 | 是否挡"全方位功能测试" |
|---|---|---|---|
| N4 升主登记活锁 | P0 | ✅ 已修 `d9e792a` | 否 |
| N5 升主瞬间副本回放跳过记录 | P?（待定因） | 观测到、未定因；疑与 N16 同源（截断待 FPI） | 仅"供副本期切主"路径，受 N15 夹具规避 |
| N6 兜底语义 | P1 | ✅ 已修 `d9e792a` | 否 |
| N7 跨组事务在 2 vCPU 上极慢 + Citus 分片修改锁串行 | 性能 | 环境（2 vCPU），非产品缺陷 | 否（只影响吞吐/耗时，不影响正确性） |
| N8 1 s 自连接超时中止 DTX | P2 | 未修；`connect_timeout=1` 负载高时自连超时 | 否（偶发失败，应用层重试；可调大超时规避） |
| N9 未打标分布表可建组/切主、切主后静默返回不一致 | P1 | 未修（**护栏缺失**） | **是（软门）**：测试必须只对**已打标**表建组/切主；未打标表纳入测试即踩坑 |
| N10 单组 2PC 静默丢 | P0 | ✅ 已修 `4ce9775` | 否 |
| N11 TSO 续租栅栏风暴 | P1 | 未修；2 vCPU 饱和下续租失败 | 否（环境；抬租约/降并发规避，见 N12 用例 60 s 租约） |
| N12 切主判决守恒（两段丢失机理） | P0 | ✅ 已修 `3a2fc1f`+`98dac27` | 否（单切守恒已验证） |
| N13 2PC 回滚判决不进副本分片 clog | P1 | 未修；ABORT 方向副本永久 PREPARED，副本升主后决议已 FORGET 则永远未决 | **是（失败/回滚 + 切主组合）**：功能测试若覆盖"事务回滚后对该分片切主"会命中 |
| N14 不可升主节点占 leader 位 | P1 | ✅ 已修 `15fbf5d` | 否 |
| N15 供副本期主漂走 | P1 | 夹具侧规避 | 否（建集群阶段抬选举超时即可，测试夹具已含此法） |
| N16 升主放行"基线已截断、FPI 未到齐"的副本 ⇒ 新主空壳 | **P0** | **待修** | **是（多主多从 + 供副本/切主）**：供副本与切主交叠时新主索引 0 字节、整片不可读 |
| N17 tick 被慢 peer 拖住 ⇒ 改选风暴（+ 旧主反复抢主） | P1 | ✅ 已做（仪表化 + 不可升主不参选）`98dac27` | 否（2 vCPU 下仍会慢，但不再由旧主打断新主） |
| N18 分片 xid 跨任期重号 ⇒ 判决落错事务 | **P0** | **待修（根因已固化 §1.11.1，修法待批）** | **是（负载 + 一组内连切两次）**：连续快速切主 + 并发下总额漂移 |

**计数：未修 8 条**（N5、N7、N8、N9、N11、N13、N16、N18），其中——
- **真正挡"全方位功能测试正确性"的产品缺陷 = 2 条 P0：N16、N18**，都只在**切主/供副本与负载/并发交叠**时发生。
- **软门 2 条：N9、N13** —— 靠测试纪律绕开（只对已打标表操作；回滚+切主组合暂不纳入或标记预期）。
- **环境/性能 3 条：N7、N8、N11** —— 2 vCPU 本机限制，非产品缺陷，抬超时/降并发即可，不挡功能正确性。
- **N5** 待定因，疑与 N16 同源，随 N16 一并复核。

**全方位测试就绪门（混合主从 / 多主多从）**：
- **绿区（现在即可全面测，不受未修项影响）**：集群搭建（1c + Nw、每分片一 Raft 组一主两从）、
  混合主从（部分分片打标走分片 MVCC、部分分片原生）、多主多从（多组各自选主、主分布在不同 worker）、
  读写与路由、跨组分布式事务语义（提交/回滚/失败原子性）、副本物理逐字节一致、
  **单次受控切主**（静默或轻并发，已证不重号、判决守恒）、TSO/join 全栈。
- **红区（须先修 N16、N18 才能纳入且判绿）**：**供副本与切主交叠**（N16）、
  **负载下一组内连续快速切主**（N18）。在修好前，这两类只作"已知缺陷"标记，不作门禁判绿项。

**结论（2026-09-18 更新）：N16、N18 已修（`905dc9c`），红区两类现已可纳入验收。** 绿区+红区全部可测；
供副本×切主（原 N16）与负载下一组连切两次（原 N18）均已验证：N18 零重复行/零 reissue 撞号（两次独立
hardened 跑），N16 空壳主由 baseline_pending 前置挡下。**P7-N13 亦已修（2026-09-18）**：残留断言现为 0（三组全部无 PREPARED 残留）。
**剩余缺口（均按用户指示"只登记不定因"，验收里标为已知残留，不作门禁判绿项）**：
- **P7-N19（总额偶发 ±1~2）**：0 残留、无重复行、无崩溃、无重号，即某笔转账一侧生效另一侧没有。
- **P7-N20（潜伏）**：旧主"未入流号"的终局态 clog 与新主重发同号冲突，当前无害、该节点再升主才显形。
- **P7-N21（可运维）**：~~DROP TABLE 可能落进零 syscall 的纯用户态死循环~~ → **2026-09-19 已修**：根因是拆掉的组被 hearsay 复活成空壳、DROP 的停流通知经它把整条分区流历史重提（"零 syscall"是取证错误：syscr 不计 socket 收发）；拆组墓碑 + 重提循环查中断。

新增全方位套件 `test_full_mixed_multiprimary_p7.sh`（12 节）：打标+原生分片并存的多主多从拓扑上跑
读写路由、副本物理逐字节、跨组分布式事务（commit/rollback/中途失败原子）、受控切主、**并发跨组事务**、
**同组连续两次切主**、**崩溃恢复**、**切主后立即读写**。
**上一轮结果 PASS=73 / FAIL=1 的更正与定因（2026-09-18）**：
- **6 条"副本 0 块（2 vCPU 漂移）"只报是夹具缺陷，不是环境漂移**：`replica_heap_bytes` 的内联 `psql -Atc "SET …; SELECT pg_relation_filepath(…)"` 没带 `-q`，先打印命令标签 `SET`，路径变量成了两个词 ⇒ `wc -c <` 报 ambiguous redirect ⇒ 空串被当成 0 块 ⇒ 降级只报。前 4 轮逐字节比对**一次都没真做过**。已修：内联 psql 一律 `-q`，量不到判红；`ensure_replica_synced` 的追平判据改用回放游标、无 armed 槽位也重供。
- **唯一真 FAIL（[9] 第 2 轮新主登记）的因果链**：① 切主手段是节点级 GUC（P7-N24），切一个组把 6 个组全压到 :5435；② 升主前置在一条异步连接上串行排队，102747 排在 102751 后面；③ 102751 卡在 P7-N22（回放块数缓存陈旧 ⇒ `unexpected data beyond EOF`）上 65 s 走兜底；④ :5435 在 102747 上心跳断档 26 s 被 :5434 以 term 6 夺回，而 :5434 从没被告知交出主权，升主前置按 P7-N23 空转 60 s 才兜底登记——等不到 :5435 登记，FAIL。
- **修复**：N22（产品，已实测含负向对照）、N24（产品新接口 `pg_raft_group_campaign` + 夹具改用）、N23（产品，逻辑修复未实测）；另登记 P7-N25（被降级的原始主不会自动变回副本）。N21 二次现场补证：零 syscall 纯用户态死循环，`statement_timeout` 同样无效。
- **修后出口（2026-09-18）：`test_full_mixed_multiprimary_p7.sh` PASS=79 / FAIL=0（全绿）**——[9] 两轮都当选并登记；[4]/[6] 的 6 条逐字节比对第一次真正执行、全部一致；只报仅剩 P7-N20（4 处跨节点 start_ts 不一致）；本轮 [9] 总额守恒（N19 未现形）。另：N22 回归 29/0（负向对照 20/9），N23 回归见上条（前提构造不出，只报）。

**2026-09-18 续（用户："做 23 实测，修 25，21 看看能不能深入挖掘原因修好"）**：
- **N23 实测通过**（合成 [9] 前提，修后 20/0、负向对照 17/3）；**N25 已修**（自动归队，28/0 ×3，负向对照 21/7）。
- 修 N25 途中顺藤摸出并修掉：**N26**（当回主逐条重提前任尾巴）、**N27**（登记条目 old_primary 失真 ⇒ 旧主不降级；二改为本地持久标记兜底）、**N30**（force 顺带修复从落后数据发基线覆盖健康副本，P0）、**N32**（空洞节点被 force 登记成主，P0，故障注入回归 18/1（仅净场）、负向对照丢一半行）、**P7-N1 之二**（截断按段名顺序删掉了已提交记录——空洞的真正成因）。新登记未修：**N29**（group0 apply 随 RPC 事务回滚而撤销、游标不退）、**N31**（切主瞬间亚秒级拒读）。
- **N21**：未抓到自旋根因；定因了 statement_timeout 无效的原因；修掉前置 **N28**（停流通知不收敛，700–1000 条/15 分钟 → 4/2/0）；SIGUSR2 取栈工具就位。
- **最终回归（全部修复在位）**：全方位套件 **79/0**、N22 **29/0**、N23 **20/0**、N25 **28/0 ×3**、N32 **18/1**（唯一红为上一轮残留组的净场检查）；修后各 worker **流内空洞 0 次**。

**2026-09-19（用户："把 N21，29，31 修好"）**：
- **N29 ✅**：控制面 apply 的事务 / 子事务回滚时退回 last_applied 并落 hardstate，条目重放；负向对照（旧版本 ROLLBACK 后 leader 永久缺行）→ 新版本自愈。
- **N31 ✅**：升主在途标记 + 读闸门等本地登记生效；确定性回归（新主上持 partition_map 行锁撑开窗口）0 拒读，负向对照 11 次拒读。
- **N21 ✅ 根因已定**：不是零 syscall 自旋（取证错误：syscr 不计 socket 收发、terminate 后只等了 3 s），是拆掉的组被 hearsay 复活成空壳、DROP 的停流通知经它从 plsn 1 起重提整条历史；拆组墓碑 + 重提循环查中断。
- **回归（全部修复在位）**：全方位 **79/0**、N22 **29/0**、N23 **20/0**、N25 **28/0**、N32 **19/0**（上轮唯一的净场红也消失了——墓碑之后清场不再有组被复活）、N31 **16/0**、N21 **20/0**，合计 211/0。

**2026-09-20（用户：「修演示中新发现的 4 个产品缺陷」）**：
- 演示（`demo/shardpg_demo.sh`）实跑中新揪出 4 条：**N33**（切主窗口旧主拒读）、**N34**（认领位自己等自己，6 分钟不可写）、**N35**（**P0：切主丢已确认的提交**）、**N36**（重启后同组两个槽位）。
- 修 N33 的回归时又揪出第 5 条 **N37**（控制面 apply 一错就跳过条目 ⇒ 永久错过一条登记），一并修掉。
- 五条全部带**确定性构造的回归**：N35 31/0、N33+N37 26/0、N36 23/0、N34 24/0；N33/N35 另有可跑的负向对照（N34/N36/N37 的修复在 C 里没有开关，对照取修前的实测记录）。
- 另登记 **N38**（拆组风暴里 `pg_raft_group_status()` 段错误，本轮唯一一次，定向复现未果）：只登记、未定因，已打开 `pg_partdist.debug_segv_backtrace` 以便下次留栈。
- N35/N37 动了 pg_raft 的共享结构体（`RaftLogShmem` 增加控制面 apply 的重试账本），按规程 clean rebuild + 整簇重启后部署。

结论：**同节点多主多从布局下，跨组分布式事务的基本语义成立**（提交、回滚、失败原子性、副本物理一致）；
**并发 + 切主时不成立**（P7-N11/N12/N13），另有一条与切主无关的静默丢数据（P7-N10）。优先级建议：N10 = N12 > **N18** > N16 > N11 > N13（N10/N12/N14/N17 已修）。

读码推断、尚未实测的风险（运维规程里已标 ⚠️）：副本侧分叉标记自动修不掉；分片 xid 持久化水位在累计 ~2^31 后重启被判损坏；
带索引的打标表一次 HOT UPDATE 即卡住分片 vacuum；截止期计时槽在丢 leader 时不清零。

---

## 2 批次分解

### 批次 1：数据正确性四条（★★★，最高优先）

**入口条件**：`pg-test-container` 就绪；不需要解冻 raft（四条全在 pg-partdist 侧）。

| 任务 | 修法 | 验收 |
|---|---|---|
| **T7.1** R-P6-15 COMMIT 标记带分片 xid | `COMMIT PREPARED` 路径不能靠 `ShardXidXactCount()`——它跑在别的事务里。改为**从 2PC 状态文件 / 参与登记里取回该分片在 PREPARE 时用的分片 xid**，显式置 `PARTWAL_MARKER_HAS_SHARD_XID` 并回填。注意 `PartWALMarkerSetShardXid` 是逐分区回填的，取值要按分区来 | 新套件：跨分片 2PC 提交 → 副本 `sclog_full` 该 xid = COMMITTED（不是 PREPARED）→ 杀主 → 新主上那两行**可见**。并入 `dtx_replay_tx1` 的切主联测 |
| **T7.2** R-P6-17 基线带分片 clog | 二选一：① 基线 CTRL 增子类型，随页面一起搬 `pg_shard_clog/<oid>` 段；② 供给流程里单独传 clog 文件并在落地前校验。**推荐 ①**（与 D1 的 fileset 灌注同源，且天然覆盖"修复分叉"消费者） | 新套件：先写 40 行 → 再供给副本 → 副本 `sclog_full(基线前的 xid)` 有判决 → 升主后 40 行**全部可见**（现状是丢） |
| **T7.3** R-P6-16 升主发 `FILESET_UPDATE` | `PartDistRoutePromote` 用 D1 机制发 `FILESET_UPDATE{old = 本地 locmap 里旧主的文件号, new = 本地 fileset}`。注意发射点要在"捕获登记"之后、对外服务之前 | 扩 `handover_provision_p7` [3b]：不止断言"另一副本**收到**新主的记录"，要断言**回放成功**（`replay_catchup` 无"未知 relfilelocator"），且该副本随后**仍有当选资格** |
| **T7.4** R-P6-21 + P7-V4 打标身份随供给/升主走 | `provision_shard_replica` / `PartDistRoutePromote` 里 `ShardMvccSetAdd` + 落白名单；顺带把"建分布表即自动打标"接到事件触发器或 `create_distributed_table` 挂点上 | 新套件：**不手工加白名单、不重启**，供给 → 升主 → 新主写入**走打标路径**（`route_status` 显示 promoted + captured，写入带分片 xid）。同时把 p7 [3b] / tx3 [4] 的"通过原因错了"这一层补上断言 |

**批次出口**：四条各有新套件；`dtx_replay_tx1` / `promote_catchup_tx3` /
`handover_provision_p7` / `dtx_commit_marker_tx2` 四套回归零 FAIL。

### 批次 2：可用性与资源三条 + DROP 回收

| 任务 | 修法 | 验收 |
|---|---|---|
| **T7.5** R-P6-20 TSO RPC 加 schema 前缀 | 5 处 RPC 文本改成 `partdist.partdist_tso_*`。**一行改，收益最大**：修完演示文档里的 `ALTER DATABASE ... SET search_path` 绕法可以删掉 | 协调者 `search_path` 恢复默认后，取号/心跳/commit_ts/safe_ts 全通；`tso_si_p3` 单跑绿 |
| **T7.6** R-P6-19 目录 fd 泄漏 | 逐条走查六个文件的 `AllocateDir` 调用点，找异常提前返回（`continue`/`ereport(ERROR)`/早 `return`）没配 `FreeDir` 的那条；修完顺手把这类路径统一成 `PG_TRY` 或 `on_proc_exit` | 新用例：同节点连续 50 轮 `replay_catchup`（三槽位），`maxAllocatedDescs` 不涨 |
| **T7.7** R-P6-4 分配器槽位回收 | 判据与 R-P6-9 的 `ShardMvccSetRemove()` 同源：DROP 提交时摘 OID，这里同时**放槽位 + 删水位文件**。做完把 `run_p6_exit.sh:165-170` 的 `mv` 净场层撤掉 | 新用例：建删 100 个分片，槽位占用回到基线；撤掉净场后门禁仍绿 |
| **T7.8** P7-D1 leader DROP 的副本侧回收 | 新 CTRL opcode `FILESET_DROP{oid}`：follower 收到后停流、摘槽位、删壳表与 `pg_parwal/<oid>`。**注意**：不能沿用"OID 不在本地 pg_class"判据（壳表是本地真表） | 新用例：leader `DROP TABLE` → 副本壳表消失、槽位释放、目录回收；未收到 CTRL 的副本保持现状（不误删） |

### 批次 3：守卫面与限制面（**2026-09-10 全部闭合**）

| 任务 | 状态 |
|---|---|
| **R-P6-22** 闸门改集群级 | ✅ 实测：协调者白名单为空（生产形态）时 `citus_rebalance_start` / `SELECT * FROM` 形式 / `citus_drain_node` / `undistribute_table` **四条全部触发禁令**；修复前全部静默放行 |
| **T7.9** 补 8 个 UDF | ✅ 与上同批实测（`negative_p6` **40/0**，协调者白名单为空） |
| **T7.10** 引用表守卫 | ✅ 实测 INSERT/UPDATE/DELETE 三种写全部拦下。**第一版挂错了位置**：挂在 `ExecutorStart` 判 `pstmt->resultRelations` 完全不生效 —— Citus 把引用表的写重写成自己的 CustomScan，顶层已不是普通 ModifyTable。改挂 `planner_hook`（拿到的是 Citus 改写**之前**的原始 Query）才生效 |
| **T7.11** MARKER 用 TSO start_ts | **✅ 已修并取证（2026-09-10）**：`test_dtx_verdict_marker_p7.sh` **38/0**（原 34 条 + 新增 4 条），决定性取证：标记 `start_ts=111` == 本事务 TSO 号，副本 clog `sts=111` 同号。**复核后发现上一版是半成品**，已补齐。新增 `TsoPeekStartTs()`（只看不取）—— 不能用 `TsoGetStartTs()`，它会给没取过号的事务凭空发起一次 RPC。<br>**漏掉的那一半**：`start_ts` 是**双宇宙**字段（TSO 小整数 / 墙钟 ~8.4e14），而上一版留了个"取不到就退回墙钟"的兜底、**没有任何位能分辨**——计划本条的"注意兼容"说的正是这个，我当时没做。后果不是"存了个没用的数"：消费侧 `slot.start_ts <= my_ts` 拿墙钟跟 TSO 比恒假 ⇒ §4.2 三态的**第三支「问协调者」永远不执行** ⇒ 已提交的 in-doubt 行一直不可见；而 leader 自己遗留模式存的是 **0**（`shard_clog.h` 约定），同一个事务两边对不上且都不报错。<br>**修法**：新增 `PARTWAL_MARKER_STS_IS_TSO`（0x0004，不影响载荷长度，纯增量）。值来自 TSO 才置位；回放侧**没标位就给分片 clog 落 0**（与 leader 一致），墙钟原值仍进增强型 CLOG 供诊断。<br>**取证**（并入 `test_dtx_verdict_marker_p7.sh`，四条新断言，均为决定性判据而非量级猜测）：标记 flags=0x7（含新位，修复前 0x3）、标记 `start_ts` **等于**本事务 TSO 号、该值 <1e9、副本 clog 的 `sts` 等于同一个号。<br>★ 副本侧不必抢 PREPARED 那个瞬态窗口：分片 clog 的 `start_ts` 列在判决落账时是**保留**的，判决后读到的仍是 PREPARE 时刻写进去的值。<br>**★ 顺带修掉一个会伪装成"修复失效"的夹具缺陷**：`[5][6]` 段原先用"取流里**最后一条** COMMIT 标记"来指代"本事务的判决标记"。以前碰巧成立；一旦同分区上有另一笔**普通本地事务**跟在后面（24 字节、flags=0、start_ts 是墙钟 —— 按新设计它被如实标成非 TSO 宇宙，行为正确），断言就对着一个**陌生事务**做，报出「长度=24 期望 32 / flags=0 期望 7 / start_ts 是墙钟」三连红。实测把流里两条标记逐字节列出来才看清：`g=9 len=24 pflags=0`（陌生事务）、`g=7 len=32 pflags=7 sxid=3 start_ts=88`（本事务的判决标记，完全正确），只是被 g=9 挡在后面。改为按**载荷尾部的分片 xid == 本行 xmin** 定位 —— 这个判据是自证的（找得到即说明尾部真带了这一笔的分片 xid，正是 R-P6-15 的判据）。<br>纪律：**绿转红且原因说不通时，先证伪自己的假设**——那条分片 xid 尾在本次改动前是绿的，而本次改动不可能删掉它 |



| 任务 | 修法 | 验收 |
|---|---|---|
| **T7.9** P7-G1 禁用清单补 8 个 UDF + **R-P6-22 闸门改判据** | **先做 R-P6-22**：Citus 运维类禁令的闸门从 `ShardGatingActive()`（本节点有没有打标表）改为**集群级判据** —— 否则补再多名字，在协调者上也是熄火的（§3.2）。再补 8 个名字进 `shard_banned_funcs[]` | `negative_p6` **撤掉给协调者设白名单那两行**（`:64-66`）之后，六项运维禁令断言仍全绿；每个新名字一条负向断言 |
| **T7.10** P7-G2 引用表运行期写 | 先裁定"拦"还是"接受"（§3）。若拦：在 `shard_guard` 里按 `pg_dist_partition.partmethod='n'` 判引用表并拒写 | 拦则一条负向断言；接受则 §10 改写为"不拦，靠规程"并给出后果 |
| **T7.11** R-P6-18 MARKER 用 TSO start_ts | `PartWALBuildMarkerPayload` 改用 TSO 的 start_ts。**注意兼容**：旧流里的墙钟值要能被识别（加 flag 位或按量级判别），否则升主后三态处置会把老记录判错 | 副本 PREPARED 槽 `sts` 与 leader TSO 同宇宙；§4.2 三态处置第一支不再恒"跳过" |

### 批次 4：出口动作（**T7.13 / T7.14 / T7.16 已于 2026-09-11 全部完成；只余 T7.12 全量**）

| 任务 | 内容 |
|---|---|
| **T7.12 分段跑实录（2026-09-11 起）** | **P1 段（P0–P5 主线 10 套）首轮：PASS=526 FAIL=28，BLOCKED=0**（拓扑正常，非环境问题）。逐条定因：<br>· **`shard_vacuum_p5` 13 条 + `shard_vacuum_replay_p5` 1 条 = 用例对现场的隐藏依赖，非回归**。两套共 10 处把 `commit_ts` 写死成 **1000**（`sclog_wts(..., 2, 1000)`），而 SI 判据是 `commit_ts < 读者 start_ts`、`start_ts` 来自 TSO 计数器 —— 等于隐含假设"TSO 已涨过 1000"。集群跑久了成立，**刚重置过的集群 TSO 只有一两百**（实测 160），判据当场翻转。诊断上最易误导的是：**vacuum 本身的断言全过**（索引项同步减少、行指针回收），只有"读回活行"那几条红，看着像可见性回归。已改为 `commit_ts=1`（不取当前 TSO —— 那是移动目标，读的时刻与断言的时刻之间还会涨）。**验证：`shard_vacuum_p5` 167/13 → 180/0。** 与 R-P6-20 同类："此前能跑只因现场有垫片"，本轮多次重置集群才把它逼出来。<br>· `shard_identity_p0` **单跑 10/0、批次 9/1** —— 典型"批次红、单跑绿"，[D] 段 DROP 引用表后 prune 仍见残行。事后在干净现场复测 DROP **成功**，故"被 §10 打标禁令拦下"的猜测**不成立**；实际原因是批次跑在**有打标残留的现场**上，待在有残留时复现定因。<br>· **第二轮（修完 commit_ts 后）：PASS=559 FAIL=6、BLOCKED=0**，`shard_identity_p0` / `dtx_tso_p4` / `shard_vacuum_p5` 三套自行转绿 —— 印证前两者是批次污染、后者是 commit_ts 修复生效。<br>· 余 6 条未定因：`shard_clog_p2` 2（`pg_waldump` 里 grep 不到 `shard xids:` 注解；已排除"补丁缺失"——十个补丁的特征符号在二进制里**全都在**。<br>　**顺带修掉守卫本身的漏检**：`check_pg_install_patched.sh` 原先只验 0001/0002/0004 三个补丁，却输出"四个补丁齐全" —— 一个只验 3/10 的守卫给出"齐全"的结论，正是 §6.2 那类"看起来验过了"；而它是 `reproduce-env.sh` 在**建容器之前**拦截"pg-install 没打补丁"的唯一关口，漏检的 0005–0010 恰好覆盖分片 xid 打标、可见性、2PC rmgr 这些 TX-TSO-MVCC 的根基。已补齐至十个。<br>　★ 补的过程中还自我纠了一次：给 0010 的 `XLogAdvancePendingInsertPosition` 先用了 `need_nm`（`nm -D` 查**动态**符号表），立刻报"缺失"—— 而仓库与容器的 `bin/postgres` **md5 完全相同**且都含该字符串，是**判据选错**（static 函数不进动态符号表），不是补丁缺失。已加 `need_str` 按 strings 验。**守卫产生假警报比漏检更糟：它会让人去重编一个本来就好的二进制。**）、`dtx_convergence_p4` 1（清扫日志留痕）、`shard_vacuum_replay_p5` 3（发号水位取空）。<br>★ **分段跑之间应重启一次节点**：P1 跑完可用内存 112 MB，重启九节点后回到 1492 MB（释放 1.4 GB）。说明**密集批次之后集群侧才是内存大头**，此前"主要是会话自身"的判断需按场景修正 |
| **T7.12 分段跑（2026-09-11/12）四段全部跑完** | **合计：PASS≈1595、FAIL=17、BLOCKED=0**。<br>· **P1**（P0–P5 主线 10 套）首轮 526/28 → 修完 commit_ts 后 **559/6**；<br>· **P2**（P6/P7/P8 10 套）**297/0 全绿** —— 该段含 `negative_p6`/`promote_p6`/`handover_provision_p7`/`divergence_mark_p8`，正是本轮 T7.8/T7.11/T7.14 生产代码影响面最大的区域，全过说明未引入回归；<br>· **P3**（replay+TX 11 套）**452/11**，`fastpath_divergence_tx4` 在批次下也通过（T7.14 的修复站得住）；<br>· **P4**（ops 8 套）**287/0 全绿**。<br>★ **P4 一度被误报成"5 套超时或早退"**：那 5 套实际全绿（6/36/29/44/12，与单跑一致），只是各用各的汇总格式（`PASSED : n`、`Tests passed: n`、`通过: n`、`PASS: n FAIL: n`），而门禁只认两种。这是**最坏的一类 harness 债 —— 把通过的套件报成异常，人会去查一段其实没问题的代码**。已补齐解析并用真实日志逐套验证取数正确。<br>**定因进展（2026-09-12）**：<br>· ✅ **`txn_layer_r2` 4 条 → 52/0**。根因是**用例的手段被产品演进作废**：它用 `pg_raft_group_reset()` 拆两个 follower 的组来制造"多数派不足"，而 `raft_consensus.c:4280` 起有一段**有意设计** —— follower 收到 AppendEntries 会按 leader 通告**自动建组**，注释写明「成员集未知只剥夺**主动**参与（竞选/当选/提案），不剥夺被动接收 —— 否则 hearsay 引导路径被砍，全新分片永远建不起来」。于是 reset 后第一条 AppendEntries 就把组重建了，follower 照常 ack，多数派毫发无损 ⇒ 写入成功 ⇒ 期望的 ABORT MARKER(info=32) 实得 COMMIT(info=0)。**它测的是一个已经不存在的失败模式。** 改用自动建组救不回来的手段（直接停那两个节点），并补齐"先登记再停 + 停完必复原"，且**本段结束即复原**（[10] 之后还要用这两个 follower）。<br>· ❌ **`commit_ts` 遗留分支改返回 0：试过，错的，已回退**（如实留痕于 `tso_client.c`）。推理看似结实：commit_ts 与 start_ts 同为双宇宙字段、无标志位可辨、判据 §4.1 是 `commit_ts < 读者 start_ts`、墙钟值（实测标记里 2114118804）恒大于 TSO 号，且**同文件 `PartDistTsoDtxDecisionTs()` 正是"未配置即返回 0"并注明"遗留宇宙不混"**。**实测否证**：`promote_catchup_tx3` 依旧 21/2（不是它的病根），`dtx_commit_marker_tx2` 由 39/2 **恶化到 33/6** —— 它有一条断言就叫「顶层事务 commit_ts 非 0」。原注释"tx1/tx2/tx4 基线断言不动"的警告是对的，我"那几套都配了 TSO、碰不到这一支"的推断是错的。<br>　★ 教训：**看起来自洽的推理 + 同文件的"正确先例"，都不能替代实测。** commit_ts 该不该有类似 `STS_IS_TSO` 的标志位仍是开放问题，但答案不是"遗留分支改成 0"。<br>· 🔍 **`promote_catchup_tx3` 2 条：已排除"本轮改动引入"，是先前就存在的真回归。**<br>　症状：新主 `applied=48` 已追平切主前位点、字节全到，40 行却一行读不到（等满 15 s 仍为 0）。<br>　用例注释里记着「**P3 出口全量跑实测 0 行、单跑即绿**，TX1 夹具竞态同族」，并把判据写死为"15 s 仍不收敛才是真回归"。**实测单跑也红（21/2）**，所以按它自己的定义就是真回归，而不是那个已知的批次竞态。<br>　**决定性对照**：把 `97632ce^`（本轮生产代码改动之前）的 `src/` + `include/` 编进容器重跑 —— **基线版同样 21/2**。故与 T7.8/T7.11/T7.14 那批改动无关。（测完已还原容器源码到 HEAD 并重编重启，`STS_IS_TSO` 3 处命中确认。）<br>　★ 之所以做这个对照而不是继续按代码推断：本轮刚在 `commit_ts` 上栽过一次 —— 推理链完整、还有同文件的"正确先例"，结果实测否证还弄坏了 tx2。**证据看起来越齐全，越要先验。**<br>　下一步：按用例注释的指引深查 R3 读路径的 `xid_map`/`gclog` 快照依赖。<br>### ★ 定因完成（2026-09-12）：**余下全部失败经基线对照确认为"先前就存在"，本轮生产代码改动一条也没引入**

对照方法：把 `97632ce^`（本轮生产代码改动之前）的 `src/` + `include/` 编进容器，
**测试脚本保持 HEAD**（这样夹具修复不受影响，隔离的纯粹是产品代码），跑同一批套件。

| 套件 | HEAD | 基线 | 判定 |
|---|---|---|---|
| `shard_clog_p2` | 62/2 | **62/2** | 完全一致 ⇒ 先前就有 |
| `shard_vacuum_replay_p5` | 61/3 | **61/3** | 完全一致 ⇒ 先前就有 |
| `lazy_replay_l1` | 56/1 | **56/1** | 完全一致 ⇒ 先前就有 |
| `clog_hole_c4` | 12/2 | 13/1 | 一致（有浮动 ⇒ 时序相关） |
| `dtx_commit_marker_tx2` | 37/3 | 35/4 | 一致（有浮动 ⇒ 时序相关） |
| `promote_catchup_tx3` | 21/2 | **21/2** | 完全一致 ⇒ 先前就有 |

（每次对照后都已还原容器源码到 HEAD 并重编重启，`STS_IS_TSO` 3 处命中确认。）

**按性质分四类**（这决定了各自该怎么修）：

| 类别 | 套件 | 说明 |
|---|---|---|
| **A. 前提被产品演进作废**（改用例） | `txn_layer_r2`（✅已修 52/0）、`dtx_commit_marker_tx2` | r2 假设"reset 组即毁多数派"，而 follower 收 AppendEntries 会**自动建组**（有意设计）；tx2 注释写着"follower 的 SELECT 尚未接 gclog（那是 R3），所以行读得出来"，而分片可见性钩子早已生效 |
| **B. 批次噪声 / 角色变位**（改判据或夹具） | `dtx_convergence_p4`（单跑 **45/0**）、`clog_hole_c4`、`shard_identity_p0`（第二轮自绿） | c4 在 [3] 段写入被拒（"本节点不是该分区组的 leader"）、被当 follower 的节点报"已升主" ⇒ 角色在跑动中变位 |
| **C. 游标到了、数据没到** —— **深挖后定性：不是缺陷，是 R3 读路径未实装** | `promote_catchup_tx3` 2、`lazy_replay_l1` 1 | 见下方「C 类深挖取证」 |
| **D. 取证手段/判据问题** | `shard_clog_p2` 2、`shard_vacuum_replay_p5` 3 | p2 靠 `pg_waldump \| grep "shard xids:"` 取证（补丁在、注解没打出来）；vacuum_replay 的三条实为"**follower 发号水位 6 < leader 8**"，因 `&& echo ok` 写法显示成"取不到值"；尾块偏移只由 `HAS_SHARD_XID`/`HAS_ALLOC_WM` 决定，**与本轮新增的 `STS_IS_TSO` 无关**（已核实） |已排除的方向：P3 那批**不是**分区组反复改选（节点日志里"当选 LEADER"**零次**）；`clog_hole_c4` 报的"已升主"是套件**自己**做的升主，只是后续仍把该节点当 follower 用 —— 方向转为"套件内部状态依赖"。 |
| **T7.12** 全量一次跑完 | **⏸ 已裁定暂缓（2026-09-09）**：批次 1–3 落地后再排。口径 = 31 套 + OPS 8 套改造后 = **39 套**。跑前：停 `pg-citus-tx2-container` 腾内存、先清它 68.5 GB 可写层腾磁盘（当前 / 已用 93%）。最坏 9.5 h+，建议分 P 段跑并逐段记录。<br>**★ 2026-09-10 实测补充两条硬约束，会直接影响排期**：<br>① **静息负载 6–7 是这套环境的常态，不是异常** —— 9 节点各带 raft topology monitor / replay worker / demux worker / TSO 心跳等约 5 个后台进程，2 核机上 36+ 个进程周期性唤醒。任何以"负载低于 X"为前提的等待都**不成立**（本次就写了个 `<3.0` 的门，它永远达不到，空转到被杀）。要等就等**拓扑稳定**（`wait_topology_stable()`），别等负载。<br>② **内存才是先断的那根，但"谁在吃"和我先说的相反**（2026-09-10 复核纠正）：实测 `docker stats` 显示**容器内全部 9 个节点合计只用 998 MB**（每节点 `shared_buffers` 仅 40 MB，集群本身很省）；而**长时间运行的 Claude 会话自身占约 1.17 GB**（claude 508 MB + 工作线程 376/201/81 MB），比整个数据库集群还多。所以后台任务被系统以"低内存"杀掉时，最大的消耗方是**会话自己**，不是集群。<br>推论：全量跑之前不需要缩集群，需要的是**用一个新会话来跑**（工作产物都在磁盘上，可无损接续）；把长会话和长跑任务放在一起，才是这次连续三轮被杀的原因。<br>⚠️ 中途被杀会留下 prepared 事务堵住后续 DDL，也会留下 `kill -9` 的残留锁 —— 本次实测前者侥幸没发生（各节点检查为干净），后者发生了两次并已在 `lib_topology.sh` 里处理 |
| **T7.13** OPS 8 套改造 | **已裁定：改造进门禁**（2026-09-09）。改成拓扑无关：节点按 `pg_dist_node` 动态取，停/起节点后必须复原。并入 `run_p6_exit.sh` 的 `SUITES`，门禁口径 31 → 39 套。<br>**✅ 已完成（2026-09-11）：8 套全部改造并跑绿，已并入门禁，口径 31 → 39。**<br>· ✅ `test_shard_auto_init.sh` **5/0**（含 All 37 tests passed）<br>· ✅ `test_segment_boundary_lsn.sh` **6/0**<br>· ✅ `test_crash_recovery.sh` **36/0（rc=0，干净集群）**。过程中经历一次**错误归因**，如实记下：<br>　09-10 那几轮停在「12 条 PASS、零 FAIL、rc=1」，当时正赶上宿主机连续以低内存 SIGKILL 打断后台任务，我便归因为"被杀断"。**这是错的。** 09-11 在新会话里任务**正常结束**（非 SIGKILL）却仍停在同一处，说明与内存无关。<br>　真因用 `set -E` + ERR 陷阱定位到 `lib_topology.sh` 的 `pg_ctl start` 那一行：`kill -9` 之后第 1 次启动必然失败（残留锁），而**调用方的 `set -e` 会因函数内部的失败终止整个脚本**，我写的"重试 3 次"循环一次都轮不到。`|| true` 当初加在了 `topo_stop` 上，漏了 `topo_start`。<br>　★ 两条可复用的教训：①「12 条 PASS、零 FAIL、rc=1」**看起来像跑完了且全过**，只有 rc 泄露真相 —— 这正是本项目一直在防的静默形态；② 第一次装 ERR 陷阱时一声不响，因为 **`trap ... ERR` 默认不被函数继承，必须同时 `set -E`** —— 诊断手段本身失效，比没有诊断更误导。<br>　纪律：**供 `set -e` 脚本 source 的库，凡"预期会失败且已被处理"的命令都不能裸写**（库里另一处裸 AND 链也一并改成显式 `if`）。<br>　**还改正了两处判据本身**：① B 段原用「记录条数相等」代理"无重复" —— 9 节点活集群上不成立（读计数与 kill -9 之间，raft 复制/标记会合法地再追一条，实测 16→17，而 LSN 序列 1..N 连续全 valid，**多出来的不是重复**）；现直接验 `count(*) - count(DISTINCT partition_lsn) = 0`，外加"不丢失"。② C 段原判据 `count <= 3` 仍是"1 行 1 条记录"老假设（实得 11）；现改为"无重号 + 确有记录被补写 + 流有效"。**两处都比原判据更强、更贴近命题**。<br>　★ **"1 行 1 条记录"这个错误期望已在 4 套里出现 7 次**（crash_recovery 3 处、demux_backlog 5 处、shard_auto_init 1 处间接）。这批套件写于 parwal-2.0 时代，当时每行大致确实只产生一条记录；之后加了主键索引写入与提交标记，比值变成约 3，而它们被移出门禁 ⇒ 没人跑 ⇒ 错了也没人知道。**T7.13 的真正价值不在"支持 9 节点"，而在把这批套件重新纳入会被执行的范围** —— 移出门禁那天起，它们就停止提供任何信号了。<br>　★ **这 8 套必须跑在干净集群上**：累积状态（shmem 打标集只增不减、被 DROP 的 OID 可能被新表复用）会让多语句 DDL 撞上「分布式事务参与登记失败，中止 prepare」，同一集群上时好时坏、换节点复现。`reproduce-env.sh reset` 之后 36/0 一次通过。门禁本就是干净起跑，正好匹配 —— 但**不要在跑过别的套件的现场上单跑它们**。<br>· ✅ `test_demux_backlog_recovery.sh` **29/0（rc=0）**。此前三次被宿主机低内存 SIGKILL 打断在 S3 的 `kill -9` 场景入口；**根因是这台机器没有 swap**（`Swap: 0`）—— 套件在 `kill -9` 后重启节点那一瞬，旧节点残留 + 新 postmaster + 崩溃恢复 BGW 同时存在，没有交换空间时内核对这种短促尖峰没有缓冲，直接触发低内存处置。把 9 个节点重启一遍回收约 160 MB（806→647 MiB，可用 1370→1519 MB）后一次跑完。<br>　改造要点：工作节点动态取；建表分片数提到 **2×worker 数**（S2「多分片隔离性」需要同一节点上有两个分片 —— 3 节点下 `shard_count=4` 天然满足，8 个 worker 下不成立，会以 `SHARDS2[1]: unbound variable` 崩掉）；8 条计数断言全部改为验性质（其中 S3/S4 的"无重复"由"条数相等"代理改为**直接数重号 LSN** + 单独验"不丢失" —— 正常重启同样会有后台活动追加记录，实测 19→20，相等这个前提本就不成立）。<br>· ✅ `test_corrupt_segment_recovery.sh` **44/0（rc=0）**，一次通过 —— 本套件**不停节点**（全程只读写段文件），改造只需换工作节点 + 分片数提到 worker 数。<br>· ✅ `test_bulk_insert_recovery.sh` **9/0（rc=0）**。<br>　★ **这一套的错法与前几套相反，更危险**：它的 `count_all_parwal`（跨 worker 求和）与 `verify_monotone`（跨 worker 求与）只统计 worker1/worker2 —— 9 节点上 `bulk_t` 的分片散在 8 个 worker，于是**求和偏小、求与漏检，而断言仍"通过"**。前几套是**假红**（看起来像产品缺陷，会被查），这套是**假绿**（漏掉大部分分片却显示全过，没人会去查一个通过的用例）。已改为遍历全部 worker。<br>　另修两处：TEST 3 的 `kill -9` 改走 `topo_start`（裸 `pg_ctl start` 会被残留锁挡住）；`restart_all` 每条 `pg_ctl` 补 `\|\| true`（`set -euo pipefail` 下一个节点返回非零就静默退出，实测停在 TEST 2、日志最后一行是"Records before restart"，看着像卡住其实是死了），并补上**协调者重启后删 `pg_tso_boot`**（本环境规程，原套件写于无 TSO 的 parwal-2.0 时代，压根没这一步）。<br>· ✅ `test_enospc_recovery.sh` **12/0（rc=0）**。本套件建的是各 worker 上的**本地表**（非分布表），不依赖 Citus 落点，只需"两个不同 worker"。<br>　★ **整批里最隐蔽的一处：注入库的 C 源码 `enospc_inject.c` 里也写死了 worker1**（`TARGET_PREFIX`）。只改 `.sh` 不改它，就会"脚本在 :5437 注入 ENOSPC、库却只拦 worker1 的写"—— **注入根本不生效，用例照样跑完并显示通过**。故障注入类用例一旦注入失效，它验的所有恢复行为都成了空转，而表面毫无异样，比 bulk_insert 那个假绿更难查。已改为编译期参数 `-DTARGET_PREFIX=...`，脚本每轮按实际节点重编。<br>　**教训：拓扑无关化不能只看 `.sh`** —— 凡参与测试的辅助程序（这里是 C 注入库）同样可能钉着旧假设。<br>· ✅ `test_multi_table_isolation.sh` **146/0（rc=0）**。改动最多的一套：50 处写死端口、8 条计数断言、以及同样那个不存在的 `$DATA/master` 分支。<br>　另修两处**非拓扑**问题：① 崩溃恢复段用裸 `start_node`（= `pg_ctl start`），`kill -9` 的残留锁把它挡住 ⇒ 节点没起来、`wait_demux` 超时 `exit 1`，表现为「84 条 PASS、零 FAIL、rc=1」—— **我自己漏改的一处**，已改走 `topo_start`；② 段目录白名单只有 `.demux_progress`/`checkpoint`（parwal-2.0 时代的全部内容），不认 TX 时代新增的 `fileset`（升主交接登记）与 `freeze`（冻结账目），实测 16 条齐红；顺带把 `dropped`（T7.8 哨兵，**本期新增的落盘文件**）也列入 —— 新增落盘文件时要想到有用例在校验目录内容。<br><br>**并入门禁（2026-09-11）**：8 套已写进 `run_p6_exit.sh` 的 `SUITES`，实测数组长度 **39**。`--with-ops` 保留作向后兼容，但改为**按名字去重**，否则这 8 套会被跑两遍（时长翻倍，且第二遍在第一遍留下的现场上跑，结果不可信）。<br>**★ 顺带修掉一个隔离隐患**：门禁默认容器原为 `pg-citus-tx2-container` —— 在 `shardpg-test` 分支上不带 `CONTAINER=` 直接跑，会**打到承载 demo 的 tx2 去**。已改为 `pg-test-container`。（tx2 当前停着，此前误触只是 `docker exec` 全部失败、汇总全 0，未造成损害；但它一旦启动，同样的误触就是真事故。）<br>**接续者必读**：这 3 套暴露的 4 类问题会在剩下 5 套里重复出现 ——（a）写死 worker1/2；（b）用启发式猜分片 OID（共享集群上会挑到别的表）；（c）期望值写死内部比值（如"1 行 1 条记录"）；（d）停/杀节点后不复原，`kill -9` 还会留下 `postmaster.pid` 与 `/tmp/.s.PGSQL.<port>.lock`，PID 一旦被复用节点就再也起不来。(d) 已在 `lib_topology.sh` 里统一处理。<br>新增共享库 `tests/lib_topology.sh`（容器内用；`topo_ports_for_table` / `topo_datadir` / `topo_stop`+EXIT 复原），已单独验证通过。`test_shard_auto_init.sh` 改造完成 —— 决定性证据：`shard_auto_t1` 的 4 个分片落在 **5433/5434/5435/5436**，而原版只看 worker1/worker2，**对一半分片是瞎的**。<br>**★ 改造中挖出两件超出"拓扑"范围的事**：<br>① **`08_schema_existence` 长期红着没人知道** —— expected 文件停在 parwal-2.0 时代（3 张表），实际 schema 已有 **69 个新对象**。这 8 套 09-09 被移出门禁后就没人跑过：**移出门禁 = 停止发现问题**，这正是要收编回去的理由。基线已重建，重建前确认 diff **纯新增、零删除**（有删除就是真回归，不能一键接受）。<br>② **原用例注释里有一个从未验证过的错误说法** —— 它写着「pg_parwal 不可写时 INSERT 仍应成功，只是建目录静默失败」，却用 `\|\| true` 把 INSERT 结果丢掉。实测 INSERT **报错中止**，而这才是对的：写不进分区 WAL 就必须拒绝写入，否则等于接受一笔**无法复制**的写入（静默分歧）。判据已改为断言"写入被**响亮地**拒绝"。我最初也把期望写反了（期待 INSERT 成功），是实测把两处错误一起纠了过来。这类「注释声称 A、代码做 B、断言什么都不查」与 §6.2「不许假声明」同族 |
| **T7.14** P7-E3 tx4 补基线闭环 | **✅ 已完成（2026-09-11）**，`test_fastpath_divergence_tx4.sh` **20/0**。<br>**★★ 缺的不是测试，是功能。** 计划原文说"分叉是既成事实，**只有重做物理基线才行**" —— 这句话在本次实装前**不成立**：`pg_raft_check_fastpath_divergence()` 只从流尾向前扫 `partwal_read_record`，**完全不看基线**，所以基线做了判据依然是 1、`promote_prepare` 依然返回 -1。实测佐证：基线发射成功（`base_plsn=35`）而 `d` 仍为 1。<br>后果是**快路径分叉没有任何归队路径**：副本被永久判死，该分片从此少一个副本 —— 一个只会判死、没有复活路径的规则，在生产上就是这个代价。<br>**修法**（`pg_raft--1.0.sql`）：扫描向前走时遇到带 `PARTWAL_FSUPD_FULL_BASELINE`(0x0002) 的 `FILESET_UPDATE`(opcode 0x01) 即停 —— 基线之后 follower 从新游标起放，陈旧 COMMIT 标记永远不会被重放，故"基线之前"确实与分叉无关。载荷布局 `PartWALCtrlFilesetUpdate`：nrels(4B)+flags(2B)，flags 在偏移 4 的两字节小端。<br>顺带给该套件加了**断言数守卫**（<16 条即 FATAL）—— 本轮已三次栽在"静默退出但 PASS/FAIL 统计正常"上。<br>⚠️ **操作教训**：首次装载函数时我用 `psql -f ... >/dev/null 2>&1 && ok++` 统计，报"9/9 成功"，实则每个节点都被 Citus 以 `operation is not allowed on this node` 拦下 —— **psql 默认遇错继续且仍返回 0**。改为 `-v ON_ERROR_STOP=1` + `citus.enable_ddl_propagation=off`，并**逐节点核对 `prosrc LIKE '%FULL_BASELINE%'`** 拿到 9/9 才继续。纪律同 §6.2：声称做了某事，要有那件事**本身**的证据，不能只看命令返回值 |
| ~~T7.15~~ P7-E4 replica_gate 压力 | **已裁定接受改写后的命题（2026-09-09），本条关闭**。动作只剩一个：把 DEV PLAN §3.8 出口清单该条原文改掉（本次已改），不补压力注入 |
| **T7.16** P7-E5 拓扑抖动判据 | **✅ 已完成（2026-09-11）**。门禁 `run_p6_exit.sh` 的 `wait_groups_settled()` 改造：<br>① 等待 60s → **180s**，且每 5 轮**主动 `pg_raft_group_drop` 清残留数据组** —— 它就是主权抖动的直接来源，光等等于指望它自己好；<br>② 仍不收敛 ⇒ 返回非零，`run_one` 据此把该套件记 **BLOCKED 且根本不执行** —— 带着抖动跑出来的数字与正常结论**无法区分**，那比不跑更坏；<br>③ 汇总里 **BLOCKED 与 FAIL 分开统计**：前者是"环境没资格给结论"，后者是"代码有问题"，混为一谈会让人去查一段**其实没执行过**的代码；BLOCKED 同样让门禁红（没跑完不能算过）。<br>实跑验证：拓扑正常时 `shard_identity_p0` 照常执行，`BLOCKED 0 套`、`rc=0`，无误判。<br>**2026-09-10 已在 `test_dtx_verdict_marker_p7.sh` 先落一处**：2PC 写入前调 `wait_topology_stable()`（两个分片的 placement 连续 4 次取样不变才放行），重试次数 3 → 6。触发它的是一次实测：宿主机 load average 11+（2 核跑 9 节点）时 group0 连着 term=38/41/44 改选，三次重试全撞「分区主副本可能已切换」，套件红了 —— 而这与被测的 R-P6-15/R-P6-18 毫无关系。<br>⚠️ **同一现场还暴露了一个操作纪律问题**：诊断期间高频轮询（每几秒一次 `docker exec` + `psql`，每次在 2 核机上 fork 一个后端）本身就是负载来源，会饿死 raft 心跳 —— **观察行为把被测系统搞不稳了**。后续等待一律走后台通知，不轮询 |

#### C 类深挖取证（2026-09-12，`KEEP_FIXTURE=1` 保住 tx3 现场后直接取证）

| 证据 | 实测值 |
|---|---|
| 新主页面上的 40 个元组 | **都在**（`lp_flags=1`，关系 1 页） |
| 它们的 `xmin` | **88745 —— 原生大 xid，不是分片 xid** |
| 该 xid 的原生判决 | **`aborted`** |
| 分片 clog 里 88745 / 3 / 4 | **全部 `st=0`（无判决）** |
| `pg_shard_clog/117472` 目录 | **不存在** ⇒ 新主并未被判为打标分片 |
| `route_status(117472)` | `role=promoted captured=yes` —— `captured` 指**捕获登记**，不是打标 |

用例**从头到尾没有打标**（`shard_relids` / `set_shard_mvcc` 一次都没出现），
所以那 40 行本来就该是原生 xid —— 这一点没有异常。

**病根**：回放把 leader 的字节物理落到副本页面上，元组因此携带**leader 的原生 xid**；
而该 xid 在本节点的原生 clog 里是**空洞**（读作 aborted）。这正是 R1 时代就写明的约束
——「回放引入的 xid 在本地原生 clog 中是空洞……任何原生可见性例程都不得触碰这些 xid，
**升主后由路由表接管（§9.4）**」。接管的裁决要靠**增强型 clog（gclog）**。

**而 R3 读路径尚未实装**，`FOLLOWER_REPLAY_DESIGN.md` 的文件清单写得很清楚：
`shard_route.c` 是「共享内存路由表（注册/换表/promote；**读侧 stub**）」，
`enhanced_clog.c` 的读接口是「**R3 读路径的入口**」。`dtx_commit_marker_tx2`
的注释也写着「follower 的 SELECT 尚未接 gclog（**那是 R3**）」。

**结论**：`promote_catchup_tx3` 的这两条断言（以及 `lazy_replay_l1` 的数据比对）
**在断言一个尚未实装的功能**。它们不是回归、也不该靠改用例来"修绿" ——
正确处置是**标注为依赖 R3、在 R3 落地前不计入出口**，并把 R3 读路径**单独立项**。
（tx2 的那两条则是**前提过期**：它假设"follower 读不接 gclog 所以行读得出来"，
而分片可见性钩子早已生效 —— 那一条属 A 类，改用例即可。）

### ★★ T7.12 收口（2026-09-12）：17 条 → 0 条，R3 读路径按 C 类裁定当场实装

用户裁定「C 类现在实装，先把其他三类修完后装」。执行顺序与结果：

| 类 | 套件 | 修前 | 修后 | 根因（一句话） |
|---|---|---|---|---|
| D | `shard_clog_p2` | 62/2 | **64/0** | 取证工具陈旧，产品一直是对的 —— 见 **P7-W1** |
| D | `shard_vacuum_replay_p5` | 61/3 | **64/0** | 旧判据与它自己的注释自相矛盾（中止事务会在 leader 上悄悄吃号，使 leader 的 `next_xid` 更大，`follower >= leader` 必不成立）。改为「follower 水位 > 本节点已判定的最大分片 xid」 |
| A | `dtx_commit_marker_tx2` | 39/2 | **40/0** | 前提被产品演进作废 —— 见 **P7-T5** |
| B | `clog_hole_c4` | 12/2 | **15/0** | 夹具与选举较劲 —— 见 **P7-T1** |
| B | `shard_identity_p0` | 9/1 | **11/0** | 吞错误把前置失败伪装成产品缺陷 —— 见 **P7-T3**① |
| B | `dtx_convergence_p4` | 44/1 | **45/0** | 取证只认两种日志布局 —— 见 **P7-T4** |
| C | `promote_catchup_tx3` | 21/2 | **24/0** | **R3 读路径实装**（下详） |
| C | `lazy_replay_l1` | 56/1 | **57/0** | 同上 |
| — | `dtx_tso_p4` | 48/1 | **49/0** | 本轮新暴露，非原 17 条 —— 见 **P7-T2** |

**R3 读路径实装要点**（完整说明见 `FOLLOWER_REPLAY_DESIGN.md` §10.1）：
新增 `include/shard_route.h` + `src/replay/shard_route.c`，两跳解析
`tuple.xmin → xid_map → gxid → pg_gclog → 判决`，接在补丁 0006 的
`sv_satisfies_mvcc` 上。**该钩子不由 `is_shard_rel` 把门、返回 false 即退回本机
语义**，所以接管读路径没有动内核的 delete/update/prune 分支 —— 这也正是
**P7-P1** 仍然成立的原因（剪枝那一格没被这次改动碰到）。
与 §9.4 预留签名的三处有意偏差（键换本地关系 OID、xid_map 用升序数组而非
dshash、并入既有钩子而非独立入口）连同理由写在 §10.1。
取证面 `partdist.route_resolve(rel, xid)` 把两跳摊开，`promote_catchup_tx3` 用它
断言"**是走 R3 读到的**"而不是"碰巧读到"——那张表没打过标，元组 xmin 是旧
leader 的原生 xid，本机 clog 里碰巧同号且同为 committed 的事务会让行数断言在
R3 一步没走的情况下照样绿。实测串 `promoted|562949953568093|committed`。

**交付面 = MVCC 读，仅此一条。** `satisfies_self / dirty / update` 仍走本机语义 ⇒
§14.2 的「R4 硬阻断于 R3」只松了**读**的那一半。

**门禁复核**：P1 段 10 套 **565/1** →（`dtx_tso_p4` 修完）全绿；P3 段 11 套逐套
单跑/净场跑全绿。批次内 `dtx_replay_tx1`(73/10) 与 `promote_catchup_tx3`(21/3)
曾因主漂红，单跑 83/0、24/0 —— 原因见 **P7-E7**（宿主机无 swap）。

### 批次 5：生产化缺件

`T7.17` 分片 vacuum 自动启动器（P7-V1）；`T7.18` 尾部截断（P7-V2）；
`T7.19` 两处覆盖缺口的故障注入点（P7-V3）。三条都是"生产不可用的硬伤"里
最不紧急的一档 —— 有手工替代，但没有它们不能说交付。

**✅ 2026-09-12 三条全部完成**，验收合并在一套里：`test_shard_vacuum_auto_p7.sh`
**62/0**，已并入门禁（口径 39 → 40）。逐条根因与取舍见 §1.5 表。

**上线后的回归复核（2026-09-12）**：`shard_clog_p2` 64/0、`tso_si_p3` 39/0、
`shard_vacuum_p5` 180/0、`shard_vacuum_replay_p5` 64/0、`shard_vacuum_auto_p7` 62/0
（后四套在门禁下复跑）。过程中暴露并修掉两条：
· **自动启动器的公平性缺陷**：一次只处理有限个分片，而"到龄"名单按槽位顺序扫，
  固定从 0 开始 ⇒ 到龄分片多于一批时**后面的永远轮不到**（把 `shard_vacuum_max_age`
  调到 1 时全节点都到龄，新建那个排在第 5 位之后，40 s 没被服务过一次，看起来
  像"心跳没干活"）。改为共享内存里的轮转游标 —— 放 shmem 而不是后端局部，
  是因为启动器每次都是心跳**新自连**出来的 backend，局部变量活不过一次调用。
· **`shard_vacuum_p5` 必须整套关掉自动启动器**：它验的是手工三步的内部，多处
  刻意停在中间态取证（最典型是「趟完标记落到 N」期望 `0/N` —— 标记落了、截断
  **故意**还没做），而启动器会在后台把这一格补掉，读出 `N/N`。

★ 三条做下来复现了同一个夹具陷阱四次：**自动启动器一旦打开，它就是场上的第二个
清扫者**。凡是验"sweep 内部做了什么"的小节，都必须先把 `pg_partdist.shard_vacuum_auto`
关掉 —— 否则心跳抢先清完，手工那一次拿到 `removed_dead=0`、水位已被推走，
断言变成在验"谁先动手"，而红出来的样子与被测内容毫无关系。
这条已写进用例的分节注释里。

### 批次 6：独立专项（**已裁定：做，排在批次 1–5 之后**）

P7-R1 成员变更（joint consensus）、P7-R2 组数上限与"给一张表全部分片建组 + 供副本"
的自动化、R-P4-13 apply 侧决议登记、P7-R3 流式基线、P7-R4 DDL 自动跟随、
R-P6-14 在 `LogicalDecodingProcessRecord` 层堵解码。

**★ 2026-09-09 起 `pg-raft-src` 在本线不再冻结**：该改就改，不必逐次申请。
代价是 raft 是全局共识层，一处改错影响面是整簇 —— 纪律见 §6。

**批次内建议顺序**：P7-R1（成员变更，解掉"拓扑不可变更"）→ P7-R2（组数上限，
前置是日志外部化 E1–E4，那条整条回滚过，要重新立项）→ R-P4-13 → P7-R3 → P7-R4
→ R-P6-14（内核补丁面，放最后）。

**实际落地（2026-09-12/13）**：T7.20 P7-R1 → T7.21 P7-R3 → T7.22 R-P6-14 → T7.23 P7-R2 →
T7.24 R-P4-13 → T7.25 P7-R4，逐条结论见 §1.6。与计划的两处出入：
P7-R2 没有做"日志外部化"（上限问题的本体是编译期常量，与日志放哪正交）；
R-P6-14 没有动内核（复制协议上的逻辑解码只有 `replication=database` 一个入口）。

---

## 3 裁定结果（2026-09-09 用户逐条裁定，已生效）

| # | 事项 | **裁定** | 落地 |
|---|---|---|---|
| 1 | 批次 6（成员变更 / 组数上限 / R-P4-13 等需动 `pg-raft-src` 的） | **做，排在批次 1–5 之后开工**；★ **raft 改动不再需要逐次申请解冻** —— 本线（`shardpg-test`）上"该改就改" | 批次 6 由"待批准"变为**已排期**；`pg-raft-src` 的修改纪律见 §6 |
| 2 | 引用表运行期写 | **拦** | T7.10 定为实装守卫；DESIGN §10 该行由"纸面约定"改为"禁（守卫 P7 T7.10 实装中）" |
| 3 | OPS 8 套 | **改造进门禁**（拓扑无关化，按 `pg_dist_node` 动态取节点） | T7.13；门禁口径由 31 套变为 **39 套** |
| 4 | `replica_gate_p6` 的 anti-wraparound 压力 | **接受 T6.3c 改写后的命题**（relfrozenxid 已同步 + 闸门拦截），把出口清单原文改掉 | T7.15 关闭；P6 出口清单该条改判 ✅ |
| 5 | 31 套全量什么时候跑 | **暂时先不跑** | T7.12 挂起，等批次 1–3 落地后再排 |
| 6 | `citus_drain_node` 这类"加名字挡不住"的 | **先搁置**（本次只解释清楚 + 留证据，不动代码） | 见 §3.1；新发现另登记 **R-P6-22** |

### 3.1 关于第 6 条：为什么"加名字"这件事本身是次要问题

**先纠正我上一版的措辞。** 我在 §1.3 与 DESIGN §10 里写过"`citus_drain_node`
内部直接调 C 函数、不经 ExecutorStart，**加名字也挡不住**"——**这句话不准确**，
本次查证后改正：把 `citus_drain_node` 加进清单，`SELECT citus_drain_node(...)`
这条 SQL **是挡得住的**（守卫按函数名匹配计划树里的 `FuncExpr`，这一条走
FunctionScan / targetlist，都在覆盖面内）。

"挡不住"的真正含义是**守卫的性质**：它是**入口黑名单**，不是**数据通路守卫**。

1. **同一个物理动作有多个入口**。11 个 UDF 在 Citus 13.1 里**全是 C 函数**
   （实测 `pg_proc.prolang = c`，`prosrc` 就是函数名）。`citus_drain_node` 的函数体
   在 C 层直接走搬迁逻辑，**不会**再发一条 `SELECT citus_move_shard_placement(...)`
   的 SQL —— 所以只禁 `citus_move_shard_placement` 时，drain 照搬不误。加了名字
   只是把**这一个**入口堵上；下一个我们没想到的入口（或 Citus 升级新增的）照旧。
2. **真正搬数据的是 worker 侧的内部函数**（`worker_*` / `citus_internal_*` 系列），
   它们不在清单里。清单拦的是"发起动作的那句话"，不是"动作本身"。

### 3.2 ★ 查证时撞出的新缺陷：R-P6-22 —— 这道守卫在**协调者上根本不生效**

比第 6 条严重得多，**本次新发现**，已登记：

`ShardGuardCheckPlan()` 在走禁用清单之前有一道快门（`shard_guard.c:238`）：

```c
if (!ShardGatingActive())
    return;                 /* 无打标表：下面的禁用项检查零成本返回 */
```

而 `ShardGatingActive()` = `shard_relids` GUC 非空 **或** shmem `mvcc_n > 0`
（`shard_xid.c:248`）。**分片打标表长在 worker 上，协调者既没有打标表、也没人给它
设白名单** —— 实测本环境三个节点 `SHOW pg_partdist.shard_relids` 全为空。

⇒ **`citus_rebalance_start` / `citus_drain_node` / `undistribute_table` 这些
恰恰只在协调者上调用的 UDF，禁令在生产形态下一条都不会触发。**

**那 `negative_p6` 为什么全绿？** 因为夹具**替它把闸门打开了**
（`test_negative_p6.sh:64-66`）：

```bash
# ★ 协调者侧也要打标：Citus 运维六项的闸门判据是 ShardGatingActive()，
#   白名单只设在 worker 上时，协调者那支根本不生效（首版因此取空）。
PSQL $COORD -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OID}'"
```

而这个 `$OID` 是 **worker 5433 上那张本地表 `t66neg` 的 OID** —— 在协调者上它压根
不指向任何打标表。**夹具用一个不相干的 OID 把守卫开起来，然后断言守卫生效。**
脚本作者当时就把机制写在注释里了（"首版因此取空"），但没有意识到这句注释同时
证明了**生产形态下这道守卫是熄火的**。

这正是"**假声明**"的另一种形态：不是函数声明了却不存在（R-P6-12），而是
**守卫存在、测试全绿、生产不生效**。

**修法方向**（归入 T7.9，与补 8 个名字同批）：Citus 运维类禁令的闸门不能用
`ShardGatingActive()`（那是"本节点有没有打标表"），应改为**集群级判据** ——
本集群是否存在打标分片（查 `partdist.partition_map` / `pg_dist_partition`
或一个显式的集群级开关 GUC）。改完 `negative_p6` 必须**撤掉给协调者设白名单
那两行**，断言仍然绿才算数。

## 4 出口标准（P7 什么时候算完）

- [x] 批次 1 四条各有**新套件**，且"切主后已提交数据可见"这条命题有端到端取证
      · T7.1 `dtx_verdict_marker_p7` 38/0、T7.2 `baseline_clog_p7` 18/0、T7.3/T7.4 `promote_handover_p7` 35/0 × 2 轮；
        端到端：新主写入带分片 xid + 另一副本回放新主的流（`promote_handover_p7`），新主经 R3 读出回放数据（`promote_catchup_tx3` 24/0，`route_resolve` 取证串 `promoted|…|committed`）
- [ ] 批次 2 四条修完，`run_p6_exit.sh` 的净场层（`mv` 水位文件）可以**撤掉**且门禁仍绿
      · 四条已修（T7.5–T7.8）；净场层 `reap_xid_slots` **仍在**，"撤掉后门禁仍绿"未验 —— 需整批门禁，随出口动作暂缓
- [ ] 批次 3 修完：R-P6-22 闸门改判据（`negative_p6` **撤掉给协调者设白名单那两行**
      后仍全绿）+ 8 个 UDF 名字 + 引用表守卫 + MARKER 用 TSO start_ts；
      §10 每条限制各有一条负向断言
      · 前四项已落地并实测（`bed624f` / `3a89e10` / `97632ce`，`negative_p6` 40/0 且协调者白名单为空）；
        **"§10 每条限制各有一条负向断言"未逐条核对**，故本格暂不勾
- [x] OPS 8 套改造成拓扑无关并入门禁（口径 31 → **39 套**，T7.13，2026-09-11）
- [ ] **全量一次跑完、零 FAIL**（口径 2026-09-13 起 **47 套** —— T7.17 起陆续并入
      `shard_vacuum_auto_p7` 及批次 6 / T7.26–T7.28 的新套件），且是在最终二进制上跑的（时机已裁定为
      批次 1–3 落地之后）
      · 2026-09-11/12 分四段跑完：**≈1595 条 / FAIL 17**，全部定因并修完 → 见 §2「T7.12 收口」
      · **仍差"一次跑完"**：四段是分开跑的，且批次内有主漂假红（**P7-E7**，宿主机无 swap）。
        加 swap 后需再来一次**不分段**的全量
- [x] 批次 5 生产化三条（vacuum 自动启动器 / 尾部截断 / 覆盖缺口）——
      T7.17/T7.18/T7.19，2026-09-12，`test_shard_vacuum_auto_p7.sh` 62/0
- [x] 批次 6 六条（含 raft 侧）—— T7.20–T7.25，2026-09-12/13。另登记施工中新发现（§1.9）：P7-W2（T7.27）、W3、T6、T7、P7-P2（T7.28）已修；P7-W4 当时未修（**2026-09-14 T7.32 已修**，见 §1.9）、P7-W5 为工程债（已修）
      · 同样**不是"一次跑完"**：各条验收与 raft/回放回归是按批次分开跑的，宿主机无 swap 期间（P7-E7）
        后台任务被系统按低内存反复杀掉，门禁改为脱离会话运行（`setsid nohup`）才跑完
- [x] 本文 §3 六条**已逐条裁定**（2026-09-09）
- [x] "不做的事"九行：去向已定（裁掉 2 / 划掉 1 / 接受 2 / 做 3 / 归批次 6 一条）；
      "接受"类运维规程 **2026-09-17 已写**：`docs/OPS_RUNBOOK.md`（升主截止期放行、日志环与丢弃/分叉修复、
      回卷停发线、升主后重取基础备份、打标禁建索引 + LP_REDIRECT、子事务禁写）。原列的"1 GB 基线上限"
      已随 T7.21 流式基线过期，不再需要规程。规程里登记了 7 处文档与代码不一致，待回填三份设计文档
- [ ] 三份设计文档（DESIGN / DEV PLAN / FRD）与代码逐条对齐（本次已做一轮，见 §5；
      **此后每改一处代码同步回填**，见 §6）

---

## 5 本次（2026-09-09）已完成的文档回填

审计 §四列的 10 处不对齐，本次逐处复核后回填：

| # | 位置 | 处置 |
|---|---|---|
| 1 | `TX_TSO_MVCC_DESING.md:12` 头部"未实施，未动任何代码" | **已改**为真实实施状态 + 指向本文 |
| 2 | `FOLLOWER_REPLAY_DESIGN.md` §10 `PartDistRoutePromote` 签名 | **已改**为实装签名 `(Oid)` + 说明水位为何不进签名 |
| 3 | `FOLLOWER_REPLAY_DESIGN.md:1101` "仍未实装：路由层本身" | **已改**：批次 #10 已实装；并就地记下它仍缺 `FILESET_UPDATE`（R-P6-16） |
| 4 | `FOLLOWER_REPLAY_DESIGN.md` §14.2 "R4 硬阻断于 R3" | **已改**：新宇宙（打标分片）不再依赖 R3，只剩遗留宇宙 |
| 5 | `FOLLOWER_REPLAY_DESIGN.md` §13 约束 13 "pg_raft 侧修法已整体回退" | **已改**：T4.4 已重放 #39，`wait_for_log_room` / `apply_owner_pid` / `flow_stats` 都在当前代码里（实测命中 4/11/13 处） |
| 6 | `raft_module_revision_plan.md` §0.2 "尚未做的"表 | **已改**：#1/#2/#4/#7 四行已被取代或已做，逐行标注 |
| 7 | `TX_TSO_MVCC_DEV_PLAN.md` "不做的事"表 | **已改**：补"实况（09-09 复核）"列；分片 clog 物理删除已做、Proxy 已被 MX 取代 |
| 8 | `TX_TSO_MVCC_DESING.md` §10 | **已补** 4 行（子事务禁写、打标后禁 CREATE INDEX + LP_REDIRECT、回卷未实现、物理基线 1 GB 上限），**已改** 2 行（Citus 路由写、引用表） |
| 9 | `P6_PRECHECK.md:226` R-P4-20 "★ 未修" | **已改**为"已处置（遏制层 + 基线工具），无根除" |
| 10 | 根 `README.md` | **已补**导读段：当前形态（9 节点 / TX-TSO-MVCC）、门禁入口、文档地图 |

**审计 §四之外，本次顺带查出并回填的 5 处**（都是"做完没回填"或"状态已过时"）：

| 位置 | 处置 |
|---|---|
| `DTX_2PC_DESIGN.md` §9.6 标题「机制已落地，**接线未做**」 | **已改**：`pg_raft--1.0.sql:539` 的升主路径里有 `PERFORM partdist.dtx_close_indoubt(...)`，且顺序正确（排在 `shard_claim_on_promote` 之前）。**接线做了** |
| `DTX_2PC_DESIGN.md` §9.5「(a) 与归队规则仍未实装」 | **已细化**：归队规则做了一半 —— 分叉**检测**已落地，**重做基线仍全靠人工** |
| `P6_PRECHECK.md` 结论段「23 条里开口的只有三条」 | **已改**：今天是"一条半"——R-P4-20 已处置、R-P4-22 已定性，只有 R-P4-13 完全开口 |
| `P6_PRECHECK.md` 附表 R-P4-22 行「★ 新观测、未查」 | **已改**为已定性（`ring_full_drops` 归零、`quorum_drops` 未消除） |
| `FOLLOWER_REPLAY_DESIGN.md` §13「`flow_stats` 不存在时打跳过」 | **已改**：`flow_stats` 已在库，`health_check_no_drops` **真正生效**。⚠️ 连带一条要复核的：此前"全 PASS"的轮次里，丢弃断言到底跑没跑过 |

另外：`P6_PENDING_DECISIONS.md` 附录里的 7 条演示缺陷，本次**正式登记进
DEV PLAN §5 风险登记簿**（R-P6-15 ~ R-P6-21），不再只存在于附录里；
`P6_EXIT_AUDIT.md` 与 `P6_PENDING_DECISIONS.md` 各加了一段"本文之后发生的事"，
免得下次有人拿旧结论当现状。

---

## 6 执行纪律（2026-09-09 用户明确要求，本期全程有效）

### 6.1 改完就回填文档，不许攒

**每一条缺陷修完的收尾动作里必须包含"改哪份文档的哪一段"**，与代码同一个提交。
理由是本次回填自己给出的证据：审计 §四那 10 处不对齐里，**有两处是批次 #10 自己
做完没回填**——路由层实装了、文档还写着"仍未实装"；一个月后再看，谁也分不清
哪句是现状、哪句是历史。攒着回填 = 下一次审计。

对应关系（改这里 ⇒ 回填那里）：

| 改动落点 | 必须同步的文档 |
|---|---|
| `src/wal/`、`src/replay/`、locmap / fileset / 基线 | `FOLLOWER_REPLAY_DESIGN.md`（§10 接口签名、§11 升主序列、§12 限制、§13 约束） |
| `src/dtx/`、决议 / 2PC / MARKER | `DTX_2PC_DESIGN.md`（§9 各小节的"落地现状"块） |
| `src/shard_xid.c`、`shard_clog.c`、`shard_visibility.c`、`tso*.c`、`shard_vacuum.c` | `TX_TSO_MVCC_DESING.md`（§5/§6/§7；**限制项一律进 §10**） |
| `src/shard_guard.c`、禁用清单、守卫闸门 | `TX_TSO_MVCC_DESING.md` §9.2 + §10 |
| `pg-raft-src/` 任何改动 | `raft_module_revision_plan.md` §0.1/§0.2（把行从"尚未做"挪走，别只在提交说明里写） |
| 任务完成、验收数字 | `TX_TSO_MVCC_DEV_PLAN.md`（批次记要 + §5 台账状态） |

### 6.2 接口必须**真的可用**，不许假声明

**判据：新加/改动的任何 SQL 可见接口，必须在真集群上被调用成功过一次，
并在验收脚本里留下断言。** 只写 `CREATE FUNCTION` 声明、只改头文件、只在
设计文档里写签名——都不算做完。

这条是踩出来的，本项目已有**三种**"假可用"形态：

1. **声明了但库里没有**（R-P6-12）：`sql/pg_partdist--1.0.sql` 声明 82 个函数，
   集群里 10 个不存在 —— 因为扩展 SQL 只在 `CREATE EXTENSION` 时执行一次，
   后加的声明没人补。任何"SQL 文件里声明了就一定能调"的脚本都会翻车。
   **动作**：改完 SQL 文件跑 `scripts/refresh_extension_sql.sh`，再从**每个相关
   节点**实调一次。
2. **函数在、但调用方拼错了名字**（R-P6-20）：`tso_client.c` 发的是裸函数名，
   而函数装在 `partdist` 模式里 ⇒ 全簇取号 fail-closed。**动作**：跨模式调用一律
   写全限定名，并在默认 `search_path` 下实测。
3. **守卫在、测试绿、生产不生效**（R-P6-22，本次新发现）：`negative_p6` 靠夹具
   给协调者设了个不相干的白名单 OID 才让守卫触发。**动作**：负向用例的前置条件
   必须是**生产上真会成立的状态**；靠夹具"把闸门掰开"再断言，等于没验。

配套的两条老纪律继续有效：**零个检查会静默 PASS**（逐项循环必须有计数守卫）、
**`docker exec` 会吞掉 while 循环的 stdin**（容器调用一律 `</dev/null`）。

### 6.3 `pg-raft-src` 解冻后的修改纪律

冻结取消**不等于**可以随手改。raft 是全簇共识层，一处改错的影响面是整个集群：

- 改共享结构体（`RaftGroupState` / `RaftLogEntry` / shmem 布局）**必须 clean rebuild**
  ——增量 make 会留下按旧结构编译的 .o，运行期栈踩踏，症状伪装成"回放缺陷"
  （R-P6-7 的根因就是这个，查了一整轮才用栈回溯定位）。
- 每次改动至少跑一轮 raft 侧回归 + 一轮切主套件（`promote_p6` / `promote_catchup_tx3`
  / `handover_provision_p7`），**不能只跑改动点附近那一套**。
- 历史上 pg_raft 的修改被整条回滚过一次（2026-08-05 流控 #39），回滚又在 T4.4 被
  重放 —— 每次改动在 `raft_module_revision_plan.md` 里留一行状态，别让"在库 / 已回退"
  再次错位。

---

*本文是 P6 审计的执行侧续篇。审计只盘点、不修改；本文负责把每一条带到收口。
§3 的裁定已生效，§6 的纪律与代码改动同等约束力。*
