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
| **P7-G1** | §9.2 第 3 层禁用清单**漏 8 个同类 Citus UDF**：`citus_split_shard_by_split_points`、`isolate_tenant_to_new_shard`、`citus_drain_node`、`master_move_shard_placement`、`master_copy_shard_placement`、`replicate_table_shards`、`citus_schema_move`、`alter_table_set_access_method` | 未修（`shard_guard.c:50-80` 清单实为 7 个 citus UDF + 5 个逻辑解码入口；11 个 UDF 在 Citus 13.1 里全是 C 函数，实测在库） | 它们同样亲手搬/读分片数据，绕过去就是静默错读或与 raft 放置冲突。**注意补名字只堵入口，不堵通路**（§3.1），而且在补名字之前先得让守卫在协调者上真的生效（R-P6-22，§3.2） |
| **P7-G2** | **引用表运行期写：零守卫、零断言**，DESIGN §10 那行还挂着"【需核实现状】" | 未修（`shard_guard.c` 零命中 `reference`） | §10 写"建表后只读"，实际拦不住 |
| **R-P6-18** | MARKER 的 `start_ts` 是墙钟（`partwal_sync.c:733` `GetCurrentTransactionStartTimestamp()`），不是 TSO start_ts | **未修** | R-P3-2「双 ts 宇宙串线」成真：升主后 §4.2 三态处置拿它与 TSO 快照比，**恒为"跳过"** |
| **R-P6-22** | **禁用清单在协调者上根本不生效**：`ShardGuardCheckPlan` 的快门是 `ShardGatingActive()`（本节点 `shard_relids` 非空 或 shmem `mvcc_n>0`），而打标表长在 worker、协调者两者皆空 ⇒ 只在协调者上调用的 `citus_rebalance_start` / `citus_drain_node` / `undistribute_table` 等**一条都不触发**。`negative_p6` 全绿是因为夹具用一个 worker 上的表 OID 给协调者开了闸门（`:64-66`） | **2026-09-09 新发现**（本次查证第 6 条时撞出，实测三节点白名单全空） | §9.2 第 3 层禁用整层在生产形态下熄火 —— 比"漏 8 个名字"严重得多。见 §3.2 |
| **P7-G3** | DESIGN §10 缺 4 行、错 1 行 | **本次已补**，见 §5 | — |

### 1.4 ◐ 出口动作 / 门禁（批次 4）

| ID | 事项 | 09-09 复核 |
|---|---|---|
| **P7-E1** | **31 套全量在最新二进制上一次没跑过**（`run_p6_exit.sh` `SUITES` 实数 31；最近一次全量是 T6.8 的 28 套 1191/31，其后批次 #7–#11 改了回放 redo、路由层、`promote_prepare`、整个 PG 二进制、发号起点） | 未跑。出口清单原文的"29 套"口径也已过时 |
| **P7-E2** | OPS 8 套按 3 节点布局写，9 节点上会停错节点且不复原 | **已裁定：改造进门禁**（2026-09-09）。拓扑无关化后并入 `SUITES`，口径 31 → 39 套。T7.13 |
| **P7-E6**<br>（2026-09-10 新登记） | **同一个毛病出现在 P7 自己的新套件里**：`test_promote_handover_p7.sh` [5] 杀掉旧主之后**从不复原**。实测后果是后面每个套件都被毒化 —— 紧接着跑的 `test_slot_reclaim_p7` 第一条断言就死在"节点 :5433 可连"，而那和它要验的槽位回收毫无关系，很容易被误判成"槽位回收回归了" | **已修（2026-09-10）**：复原挂进该套件的 `cleanup()`（EXIT trap，中途 Ctrl-C 也能复原），并在**杀之前**先登记 `KILLED_NODE`。纪律：**停节点的套件必须自带复原**，这条同样适用于 T7.13 的 OPS 8 套改造。<br>⚠️ 光复原**还不够**：该套件的不可重复有**两个**独立原因，另一个是旧主上留下的壳表删不掉（P7-D3），已一并改成每轮换表名。**验收：干净集群上连跑两遍，两遍都 35/0**（2026-09-10）——只补一个的时候第二遍必红 |
| **P7-E3** | 三个基线消费者 e2e：初始配对 ✓、永久分叉 ✓、**快路径分叉 ✗**（`test_fastpath_divergence_tx4.sh` 只验到 `promote_prepare` 返回 -1，无重做基线步骤） | **✅ 已补（2026-09-11，T7.14）**：`test_fastpath_divergence_tx4.sh` **20/0**，取证 `p=1`（重做基线后 `promote_prepare` 由 -1 转正值，副本重新获得参选资格）。<br>★ **补测试的过程挖出并修掉了一个真缺陷** —— 见 T7.14 |
| **P7-E4** | `replica_gate_p6` **没有施加 anti-wraparound 压力**，验的是"relfrozenxid 同步 + 闸门拦截" | **已裁定接受**（2026-09-09）：采用 T6.3c 改写后的命题，出口清单原文改掉，本条关闭 |
| **P7-E5** | R-P6-13 拓扑抖动"等不到只告警不阻塞"，批次里带着"60s 未收敛、存疑"跑完 | 未改判据 |

### 1.5 ○ 生产化缺件（批次 5）

| ID | 事项 | 09-09 复核 |
|---|---|---|
| **P7-V1** | 分片 vacuum **无自动启动器**，到龄只 WARNING（`shard_xid.c:1381`） | 未做 |
| **P7-V2** | 分片 vacuum **无尾部截断**（`shard_vacuum.c` 无 `smgrtruncate`/`RelationTruncate`） | 未做 |
| **P7-V3** | 两处覆盖缺口：clog 整段删除分支；停在页面循环中间的崩溃 | 未做（需生产路径故障注入点） |
| **P7-D3**<br>（2026-09-10 新登记） | **半删的分片会把那张分布表锁死在协调者上，永远删不掉**。在 worker 上直删分片表（运维绕过 §10 的常规手法，T7.8 夹具用的也是它）之后：① 协调者侧 `DROP TABLE` 走 Citus 2PC，被 §10「含分片打标表 DROP 禁 PREPARE」拦下 —— 拦的是**另一个**还留着壳表的副本；② 绕到各节点本地删（`enable_ddl_propagation=off`）同样不行 —— 那个分区组这时往往已经因为主副本的表先没了而凑不齐多数派，`PartWALAppendCtrl` 的 propose 直接失败（实测 `组 102008 propose plsn=58 失败 … record 58 未达多数派`）。于是那张表**两条路都走不通**。<br>与 P7-D1 是一体两面：D1 管的是"副本知不知道分片没了"，D3 管的是"分片没了之后那张分布表还能不能删掉"。发通知修好了不等于回收闭环了 | **未修，仅登记**。当前只能靠"整簇重建"清掉，验收因此不可重复跑。<br>**★ 它还会伪装成别的缺陷**：`test_promote_handover_p7` 第二轮报的是 `replay_set_locmap: base_part_lsn=0 声明"从流起点开始"，但本地关系 17522 已有 1 个块` —— 看着像回放侧的基线缺陷，实际链条是「旧主上的壳表删不掉 → 夹具里 `DROP TABLE IF EXISTS` 先失败 → 跟在同一个 `ON_ERROR_STOP` 块里的 `CREATE TABLE` 没执行 → `replay_set_locmap` 对上了**上一轮的旧表**」。这类"报错点离病根三跳远"的形态最费诊断时间，登记在案。<br>`test_baseline_clog_p7.sh` 与 `test_promote_handover_p7.sh` 均已改成每轮 `TBASE="…_$(date +%H%M%S)"` 规避。修法方向：DROP 的 §10 禁令对"分片表已经不在本地"的节点应放行；或给分区组一条"成员已不存在"的退出路径，让 propose 不必凑多数派 |
| **P7-V4** | 打标登记全手工（`partdist_set_shard_mvcc` 逐分片逐节点，无建表钩子/事件触发器） | 未做；与 R-P6-21 同源，一并做 |

### 1.6 ⏸ 独立专项（批次 6，**2026-09-09 已裁定：做，排在批次 1–5 之后**）

| ID | 事项 | 为什么单列 |
|---|---|---|
| **P7-R1** | Raft **成员变更无安全路径**（无 joint consensus）⇒ 连带 §9.2 禁掉全部 Citus 搬运器 ⇒ **集群拓扑事实上不可变更** | 改 `pg-raft-src` 核心。★ 2026-09-09 起本线**不再需要逐次申请解冻**，按 §6 纪律直接改 |
| **P7-R2** | `RAFT_MAX_GROUPS = 32` 定长 shmem ⇒ 每节点 ≤ 31 个数据组；且"给一张分布表的全部分片建组 + 供副本"无自动化 | 同上；前置是日志外部化（已整条回滚） |
| **R-P4-13** | apply 侧决议登记缺失，仅 `dtx_peek` 绕行（`pg_raft--1.0.sql:146, 1349`） | 根因在 pg_raft |
| **P7-R3** | 物理基线**上限 1 GB**（`fileset_inline_max_blocks = 131072`，超出显式 ERROR）⇒ 大于 1 GB 的分片既不能供给也不能修复分叉 | 需流式基线，独立专项 |
| **P7-R4** | leader 任何一次 DDL ⇒ follower 停在结构栅栏，需人工等价 DDL + 重跑 `replay_set_locmap` | 需 DDL 自动跟随通道，独立专项 |
| **R-P6-14** | 逻辑解码禁令只覆盖 SQL，walsender `START_REPLICATION` 绕得过 | 真堵要在 `LogicalDecodingProcessRecord` 加判据 = 内核补丁面 |

### 1.7 工程债（穿插在各批次里做，不单列批次）

`write_router.c` milestone 1.2 死代码仍编入并暴露为 UDF；`kill(pid,0)` 探活在回放侧
仍在用（raft 侧已因僵尸失真而弃用，两处口径不一）；改共享结构体后增量 make 不可靠
（R-P6-7 根因）**无自动守卫**；`REPLAY_MAX_SHARDS = 64` 定长槽位；单事务 DROP > 16 张
打标表即 ERROR；`dtx_close_indoubt` 四级落空无告警面；GlobalSafeTs 被钉死无监控面；
根 `README.md` 仍是 2.0 三节点文档（**本次已补导读**，见 §5）。

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

### 批次 4：出口动作（做完前三批再跑，否则白跑）

| 任务 | 内容 |
|---|---|
| **T7.12** 全量一次跑完 | **⏸ 已裁定暂缓（2026-09-09）**：批次 1–3 落地后再排。口径 = 31 套 + OPS 8 套改造后 = **39 套**。跑前：停 `pg-citus-tx2-container` 腾内存、先清它 68.5 GB 可写层腾磁盘（当前 / 已用 93%）。最坏 9.5 h+，建议分 P 段跑并逐段记录。<br>**★ 2026-09-10 实测补充两条硬约束，会直接影响排期**：<br>① **静息负载 6–7 是这套环境的常态，不是异常** —— 9 节点各带 raft topology monitor / replay worker / demux worker / TSO 心跳等约 5 个后台进程，2 核机上 36+ 个进程周期性唤醒。任何以"负载低于 X"为前提的等待都**不成立**（本次就写了个 `<3.0` 的门，它永远达不到，空转到被杀）。要等就等**拓扑稳定**（`wait_topology_stable()`），别等负载。<br>② **内存才是先断的那根，但"谁在吃"和我先说的相反**（2026-09-10 复核纠正）：实测 `docker stats` 显示**容器内全部 9 个节点合计只用 998 MB**（每节点 `shared_buffers` 仅 40 MB，集群本身很省）；而**长时间运行的 Claude 会话自身占约 1.17 GB**（claude 508 MB + 工作线程 376/201/81 MB），比整个数据库集群还多。所以后台任务被系统以"低内存"杀掉时，最大的消耗方是**会话自己**，不是集群。<br>推论：全量跑之前不需要缩集群，需要的是**用一个新会话来跑**（工作产物都在磁盘上，可无损接续）；把长会话和长跑任务放在一起，才是这次连续三轮被杀的原因。<br>⚠️ 中途被杀会留下 prepared 事务堵住后续 DDL，也会留下 `kill -9` 的残留锁 —— 本次实测前者侥幸没发生（各节点检查为干净），后者发生了两次并已在 `lib_topology.sh` 里处理 |
| **T7.13** OPS 8 套改造 | **已裁定：改造进门禁**（2026-09-09）。改成拓扑无关：节点按 `pg_dist_node` 动态取，停/起节点后必须复原。并入 `run_p6_exit.sh` 的 `SUITES`，门禁口径 31 → 39 套。<br>**✅ 已完成（2026-09-11）：8 套全部改造并跑绿，已并入门禁，口径 31 → 39。**<br>· ✅ `test_shard_auto_init.sh` **5/0**（含 All 37 tests passed）<br>· ✅ `test_segment_boundary_lsn.sh` **6/0**<br>· ✅ `test_crash_recovery.sh` **36/0（rc=0，干净集群）**。过程中经历一次**错误归因**，如实记下：<br>　09-10 那几轮停在「12 条 PASS、零 FAIL、rc=1」，当时正赶上宿主机连续以低内存 SIGKILL 打断后台任务，我便归因为"被杀断"。**这是错的。** 09-11 在新会话里任务**正常结束**（非 SIGKILL）却仍停在同一处，说明与内存无关。<br>　真因用 `set -E` + ERR 陷阱定位到 `lib_topology.sh` 的 `pg_ctl start` 那一行：`kill -9` 之后第 1 次启动必然失败（残留锁），而**调用方的 `set -e` 会因函数内部的失败终止整个脚本**，我写的"重试 3 次"循环一次都轮不到。`|| true` 当初加在了 `topo_stop` 上，漏了 `topo_start`。<br>　★ 两条可复用的教训：①「12 条 PASS、零 FAIL、rc=1」**看起来像跑完了且全过**，只有 rc 泄露真相 —— 这正是本项目一直在防的静默形态；② 第一次装 ERR 陷阱时一声不响，因为 **`trap ... ERR` 默认不被函数继承，必须同时 `set -E`** —— 诊断手段本身失效，比没有诊断更误导。<br>　纪律：**供 `set -e` 脚本 source 的库，凡"预期会失败且已被处理"的命令都不能裸写**（库里另一处裸 AND 链也一并改成显式 `if`）。<br>　**还改正了两处判据本身**：① B 段原用「记录条数相等」代理"无重复" —— 9 节点活集群上不成立（读计数与 kill -9 之间，raft 复制/标记会合法地再追一条，实测 16→17，而 LSN 序列 1..N 连续全 valid，**多出来的不是重复**）；现直接验 `count(*) - count(DISTINCT partition_lsn) = 0`，外加"不丢失"。② C 段原判据 `count <= 3` 仍是"1 行 1 条记录"老假设（实得 11）；现改为"无重号 + 确有记录被补写 + 流有效"。**两处都比原判据更强、更贴近命题**。<br>　★ **"1 行 1 条记录"这个错误期望已在 4 套里出现 7 次**（crash_recovery 3 处、demux_backlog 5 处、shard_auto_init 1 处间接）。这批套件写于 parwal-2.0 时代，当时每行大致确实只产生一条记录；之后加了主键索引写入与提交标记，比值变成约 3，而它们被移出门禁 ⇒ 没人跑 ⇒ 错了也没人知道。**T7.13 的真正价值不在"支持 9 节点"，而在把这批套件重新纳入会被执行的范围** —— 移出门禁那天起，它们就停止提供任何信号了。<br>　★ **这 8 套必须跑在干净集群上**：累积状态（shmem 打标集只增不减、被 DROP 的 OID 可能被新表复用）会让多语句 DDL 撞上「分布式事务参与登记失败，中止 prepare」，同一集群上时好时坏、换节点复现。`reproduce-env.sh reset` 之后 36/0 一次通过。门禁本就是干净起跑，正好匹配 —— 但**不要在跑过别的套件的现场上单跑它们**。<br>· ✅ `test_demux_backlog_recovery.sh` **29/0（rc=0）**。此前三次被宿主机低内存 SIGKILL 打断在 S3 的 `kill -9` 场景入口；**根因是这台机器没有 swap**（`Swap: 0`）—— 套件在 `kill -9` 后重启节点那一瞬，旧节点残留 + 新 postmaster + 崩溃恢复 BGW 同时存在，没有交换空间时内核对这种短促尖峰没有缓冲，直接触发低内存处置。把 9 个节点重启一遍回收约 160 MB（806→647 MiB，可用 1370→1519 MB）后一次跑完。<br>　改造要点：工作节点动态取；建表分片数提到 **2×worker 数**（S2「多分片隔离性」需要同一节点上有两个分片 —— 3 节点下 `shard_count=4` 天然满足，8 个 worker 下不成立，会以 `SHARDS2[1]: unbound variable` 崩掉）；8 条计数断言全部改为验性质（其中 S3/S4 的"无重复"由"条数相等"代理改为**直接数重号 LSN** + 单独验"不丢失" —— 正常重启同样会有后台活动追加记录，实测 19→20，相等这个前提本就不成立）。<br>· ✅ `test_corrupt_segment_recovery.sh` **44/0（rc=0）**，一次通过 —— 本套件**不停节点**（全程只读写段文件），改造只需换工作节点 + 分片数提到 worker 数。<br>· ✅ `test_bulk_insert_recovery.sh` **9/0（rc=0）**。<br>　★ **这一套的错法与前几套相反，更危险**：它的 `count_all_parwal`（跨 worker 求和）与 `verify_monotone`（跨 worker 求与）只统计 worker1/worker2 —— 9 节点上 `bulk_t` 的分片散在 8 个 worker，于是**求和偏小、求与漏检，而断言仍"通过"**。前几套是**假红**（看起来像产品缺陷，会被查），这套是**假绿**（漏掉大部分分片却显示全过，没人会去查一个通过的用例）。已改为遍历全部 worker。<br>　另修两处：TEST 3 的 `kill -9` 改走 `topo_start`（裸 `pg_ctl start` 会被残留锁挡住）；`restart_all` 每条 `pg_ctl` 补 `\|\| true`（`set -euo pipefail` 下一个节点返回非零就静默退出，实测停在 TEST 2、日志最后一行是"Records before restart"，看着像卡住其实是死了），并补上**协调者重启后删 `pg_tso_boot`**（本环境规程，原套件写于无 TSO 的 parwal-2.0 时代，压根没这一步）。<br>· ✅ `test_enospc_recovery.sh` **12/0（rc=0）**。本套件建的是各 worker 上的**本地表**（非分布表），不依赖 Citus 落点，只需"两个不同 worker"。<br>　★ **整批里最隐蔽的一处：注入库的 C 源码 `enospc_inject.c` 里也写死了 worker1**（`TARGET_PREFIX`）。只改 `.sh` 不改它，就会"脚本在 :5437 注入 ENOSPC、库却只拦 worker1 的写"—— **注入根本不生效，用例照样跑完并显示通过**。故障注入类用例一旦注入失效，它验的所有恢复行为都成了空转，而表面毫无异样，比 bulk_insert 那个假绿更难查。已改为编译期参数 `-DTARGET_PREFIX=...`，脚本每轮按实际节点重编。<br>　**教训：拓扑无关化不能只看 `.sh`** —— 凡参与测试的辅助程序（这里是 C 注入库）同样可能钉着旧假设。<br>· ✅ `test_multi_table_isolation.sh` **146/0（rc=0）**。改动最多的一套：50 处写死端口、8 条计数断言、以及同样那个不存在的 `$DATA/master` 分支。<br>　另修两处**非拓扑**问题：① 崩溃恢复段用裸 `start_node`（= `pg_ctl start`），`kill -9` 的残留锁把它挡住 ⇒ 节点没起来、`wait_demux` 超时 `exit 1`，表现为「84 条 PASS、零 FAIL、rc=1」—— **我自己漏改的一处**，已改走 `topo_start`；② 段目录白名单只有 `.demux_progress`/`checkpoint`（parwal-2.0 时代的全部内容），不认 TX 时代新增的 `fileset`（升主交接登记）与 `freeze`（冻结账目），实测 16 条齐红；顺带把 `dropped`（T7.8 哨兵，**本期新增的落盘文件**）也列入 —— 新增落盘文件时要想到有用例在校验目录内容。<br><br>**并入门禁（2026-09-11）**：8 套已写进 `run_p6_exit.sh` 的 `SUITES`，实测数组长度 **39**。`--with-ops` 保留作向后兼容，但改为**按名字去重**，否则这 8 套会被跑两遍（时长翻倍，且第二遍在第一遍留下的现场上跑，结果不可信）。<br>**★ 顺带修掉一个隔离隐患**：门禁默认容器原为 `pg-citus-tx2-container` —— 在 `shardpg-test` 分支上不带 `CONTAINER=` 直接跑，会**打到承载 demo 的 tx2 去**。已改为 `pg-test-container`。（tx2 当前停着，此前误触只是 `docker exec` 全部失败、汇总全 0，未造成损害；但它一旦启动，同样的误触就是真事故。）<br>**接续者必读**：这 3 套暴露的 4 类问题会在剩下 5 套里重复出现 ——（a）写死 worker1/2；（b）用启发式猜分片 OID（共享集群上会挑到别的表）；（c）期望值写死内部比值（如"1 行 1 条记录"）；（d）停/杀节点后不复原，`kill -9` 还会留下 `postmaster.pid` 与 `/tmp/.s.PGSQL.<port>.lock`，PID 一旦被复用节点就再也起不来。(d) 已在 `lib_topology.sh` 里统一处理。<br>新增共享库 `tests/lib_topology.sh`（容器内用；`topo_ports_for_table` / `topo_datadir` / `topo_stop`+EXIT 复原），已单独验证通过。`test_shard_auto_init.sh` 改造完成 —— 决定性证据：`shard_auto_t1` 的 4 个分片落在 **5433/5434/5435/5436**，而原版只看 worker1/worker2，**对一半分片是瞎的**。<br>**★ 改造中挖出两件超出"拓扑"范围的事**：<br>① **`08_schema_existence` 长期红着没人知道** —— expected 文件停在 parwal-2.0 时代（3 张表），实际 schema 已有 **69 个新对象**。这 8 套 09-09 被移出门禁后就没人跑过：**移出门禁 = 停止发现问题**，这正是要收编回去的理由。基线已重建，重建前确认 diff **纯新增、零删除**（有删除就是真回归，不能一键接受）。<br>② **原用例注释里有一个从未验证过的错误说法** —— 它写着「pg_parwal 不可写时 INSERT 仍应成功，只是建目录静默失败」，却用 `\|\| true` 把 INSERT 结果丢掉。实测 INSERT **报错中止**，而这才是对的：写不进分区 WAL 就必须拒绝写入，否则等于接受一笔**无法复制**的写入（静默分歧）。判据已改为断言"写入被**响亮地**拒绝"。我最初也把期望写反了（期待 INSERT 成功），是实测把两处错误一起纠了过来。这类「注释声称 A、代码做 B、断言什么都不查」与 §6.2「不许假声明」同族 |
| **T7.14** P7-E3 tx4 补基线闭环 | **✅ 已完成（2026-09-11）**，`test_fastpath_divergence_tx4.sh` **20/0**。<br>**★★ 缺的不是测试，是功能。** 计划原文说"分叉是既成事实，**只有重做物理基线才行**" —— 这句话在本次实装前**不成立**：`pg_raft_check_fastpath_divergence()` 只从流尾向前扫 `partwal_read_record`，**完全不看基线**，所以基线做了判据依然是 1、`promote_prepare` 依然返回 -1。实测佐证：基线发射成功（`base_plsn=35`）而 `d` 仍为 1。<br>后果是**快路径分叉没有任何归队路径**：副本被永久判死，该分片从此少一个副本 —— 一个只会判死、没有复活路径的规则，在生产上就是这个代价。<br>**修法**（`pg_raft--1.0.sql`）：扫描向前走时遇到带 `PARTWAL_FSUPD_FULL_BASELINE`(0x0002) 的 `FILESET_UPDATE`(opcode 0x01) 即停 —— 基线之后 follower 从新游标起放，陈旧 COMMIT 标记永远不会被重放，故"基线之前"确实与分叉无关。载荷布局 `PartWALCtrlFilesetUpdate`：nrels(4B)+flags(2B)，flags 在偏移 4 的两字节小端。<br>顺带给该套件加了**断言数守卫**（<16 条即 FATAL）—— 本轮已三次栽在"静默退出但 PASS/FAIL 统计正常"上。<br>⚠️ **操作教训**：首次装载函数时我用 `psql -f ... >/dev/null 2>&1 && ok++` 统计，报"9/9 成功"，实则每个节点都被 Citus 以 `operation is not allowed on this node` 拦下 —— **psql 默认遇错继续且仍返回 0**。改为 `-v ON_ERROR_STOP=1` + `citus.enable_ddl_propagation=off`，并**逐节点核对 `prosrc LIKE '%FULL_BASELINE%'`** 拿到 9/9 才继续。纪律同 §6.2：声称做了某事，要有那件事**本身**的证据，不能只看命令返回值 |
| ~~T7.15~~ P7-E4 replica_gate 压力 | **已裁定接受改写后的命题（2026-09-09），本条关闭**。动作只剩一个：把 DEV PLAN §3.8 出口清单该条原文改掉（本次已改），不补压力注入 |
| **T7.16** P7-E5 拓扑抖动判据 | "等不到就阻塞"而不是"只告警"，消灭"批次红、单跑绿"的伪信号。<br>**2026-09-10 已在 `test_dtx_verdict_marker_p7.sh` 先落一处**：2PC 写入前调 `wait_topology_stable()`（两个分片的 placement 连续 4 次取样不变才放行），重试次数 3 → 6。触发它的是一次实测：宿主机 load average 11+（2 核跑 9 节点）时 group0 连着 term=38/41/44 改选，三次重试全撞「分区主副本可能已切换」，套件红了 —— 而这与被测的 R-P6-15/R-P6-18 毫无关系。<br>⚠️ **同一现场还暴露了一个操作纪律问题**：诊断期间高频轮询（每几秒一次 `docker exec` + `psql`，每次在 2 核机上 fork 一个后端）本身就是负载来源，会饿死 raft 心跳 —— **观察行为把被测系统搞不稳了**。后续等待一律走后台通知，不轮询 |

### 批次 5：生产化缺件

`T7.17` 分片 vacuum 自动启动器（P7-V1）；`T7.18` 尾部截断（P7-V2）；
`T7.19` 两处覆盖缺口的故障注入点（P7-V3）。三条都是"生产不可用的硬伤"里
最不紧急的一档 —— 有手工替代，但没有它们不能说交付。

### 批次 6：独立专项（**已裁定：做，排在批次 1–5 之后**）

P7-R1 成员变更（joint consensus）、P7-R2 组数上限与"给一张表全部分片建组 + 供副本"
的自动化、R-P4-13 apply 侧决议登记、P7-R3 流式基线、P7-R4 DDL 自动跟随、
R-P6-14 在 `LogicalDecodingProcessRecord` 层堵解码。

**★ 2026-09-09 起 `pg-raft-src` 在本线不再冻结**：该改就改，不必逐次申请。
代价是 raft 是全局共识层，一处改错影响面是整簇 —— 纪律见 §6。

**批次内建议顺序**：P7-R1（成员变更，解掉"拓扑不可变更"）→ P7-R2（组数上限，
前置是日志外部化 E1–E4，那条整条回滚过，要重新立项）→ R-P4-13 → P7-R3 → P7-R4
→ R-P6-14（内核补丁面，放最后）。

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

- [ ] 批次 1 四条各有**新套件**，且"切主后已提交数据可见"这条命题有端到端取证
- [ ] 批次 2 四条修完，`run_p6_exit.sh` 的净场层（`mv` 水位文件）可以**撤掉**且门禁仍绿
- [ ] 批次 3 修完：R-P6-22 闸门改判据（`negative_p6` **撤掉给协调者设白名单那两行**
      后仍全绿）+ 8 个 UDF 名字 + 引用表守卫 + MARKER 用 TSO start_ts；
      §10 每条限制各有一条负向断言
- [ ] OPS 8 套改造成拓扑无关并入门禁（口径 31 → **39 套**）
- [ ] **39 套全量一次跑完、零 FAIL**，且是在最终二进制上跑的（时机已裁定为
      批次 1–3 落地之后）
- [ ] 批次 5 生产化三条（vacuum 自动启动器 / 尾部截断 / 覆盖缺口）
- [ ] 批次 6 六条（含 raft 侧）
- [x] 本文 §3 六条**已逐条裁定**（2026-09-09）
- [ ] "不做的事"九行：去向已定（裁掉 2 / 划掉 1 / 接受 2 / 做 3 / 归批次 6 一条），
      **差一份把"接受"类写清楚的运维规程**（升主 60s deadline 放行、环容量靠手工修复、
      1 GB 基线上限、回卷停发线）
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
