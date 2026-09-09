# P6 待裁定清单（2026-09-07）

本文只列**需要用户裁定**的事项，不重复分析；每条给出处、建议与"要裁的是什么"。
证据与推理在 `P6_EXIT_AUDIT.md`（2026-09-06 审计）与 `TX_TSO_MVCC_DEV_PLAN.md` 批次 #11 记要；
2026-09-06/07 多分片演示实跑新撞出的 7 条缺陷，其详细条目未并入审计文档（按当时"只写演示文档"的
指示撤回），**全文附在本文附录**，免得丢。

裁定选项统一用：**做**（本期修/补）/ **推后**（记台账、归 P7 或专项）/ **接受**（降级为已知边界，写进
DESIGN §10 或运维规程）/ **裁掉**（从方案中移除）。

> **★ 2026-09-09 进展**：本文 §三 + 附录那 7 条**已正式登记进 DEV PLAN §5 台账**
> （**R-P6-15 ~ R-P6-21**），不再只存在于附录；§一-7"三份文档与代码对齐"**已由
> 批次 #12 一次性回填完毕**；§二"不做的事"九行**已补上逐行实况**（DEV PLAN §3.8 表新增列）。
> **★ 2026-09-09 当日续：6 条裁定已全部拿到**，记录在
> `P7_REMEDIATION_PLAN.md` §3（批次 6 做、排在批次 1–5 之后且 **pg-raft 解冻**；
> 引用表**拦**；OPS 8 套**改造进门禁**；`replica_gate_p6` **接受改写后的命题**；
> 全量**暂缓**；`citus_drain_node` **搁置**）。
> **本文自此只作历史留档，现行去向以 `P7_REMEDIATION_PLAN.md` 为准。**
> 另：解释"`citus_drain_node` 加名字挡不住"时撞出 **R-P6-22**（禁用清单在协调者上
> 根本不生效），见该文 §3.2。

---

## 一、P6 出口清单里没勾的项（12 条中的 7 条需裁）

| # | 事项 | 现状 | 建议 | 要裁的 |
|---|---|---|---|---|
| 1 | **全量门禁一次跑完零 FAIL** | 31 套在最新二进制（含批次 #11 的分配器改动）上一次没跑过；最坏 9.5h；宿主 3.9 GB 内存本周 OOM 杀过后台任务 | 跑一次，跑前确认内存；跑完才谈结项 | 何时跑；OPS 8 套（3 节点布局）的去向：改造进门禁 / 退役 |
| 2 | 三个基线消费者各一条 e2e | 快路径分叉（tx4）只验到拒升主，没有重做基线的闭环 | 给 tx4 补"重做基线后重新参选"一段 | 做 / 推后 |
| 3 | 副本壳表在 anti-wraparound 压力下不被本地 WAL 触碰 | 套件没有施加回卷压力，验的是"relfrozenxid 同步 + 闸门" | 接受改写后的命题并把清单原文改掉 | 接受 / 做（补真实压力注入） |
| 4 | §10 每条限制一条负向断言 | 引用表写：代码零守卫、测试零断言；§10 缺 3 行（子事务禁写、打标后禁 CREATE INDEX + LP_REDIRECT、回卷未实现）；"Citus 路由写 P1 禁"一行已过时 | 补守卫 + 补 3 行 + 删 1 行 | 引用表写是"守卫拦"还是"接受不拦"；§10 三行的措辞 |
| 5 | R-P4-20 收口 | 根因两面已处置（T6.1/6.2/6.3a/b/c），台账仍"★ 未修" | 改为"已处置，遏制层 + 基线工具，无根除" | 收口措辞 |
| 6 | R-P4-13 apply 侧决议登记缺失 | 仅 `dtx_peek` 绕行；根因在 pg_raft，需扩大解冻面 | 推后到 raft 专项 | 是否解冻 |
| 7 | 三份文档与代码对齐 | 审计 §四列了 10 处 | 一次性回填 | 由我一并回填 / 逐条过 |

## 二、"不做的事"九行（DEV PLAN §3.8 表，至今表头仍是"待裁"）

| 行 | 实况 | 建议裁定 |
|---|---|---|
| 分片 vacuum 自动启动器 | 未做，只有 WARNING | **做**（生产不可用的硬伤） |
| 回卷 | 未做，线性 xid + 停发线 | **接受**，写进 §10 |
| 两处覆盖缺口（clog 整段删除 / 页面循环中断崩溃） | 未做 | **推后**：故障注入专项 |
| vacuum 尾部截断 | 未做 | **做** |
| LP_REDIRECT | 未做，撞见即 ERROR | **接受**，与"打标后禁 CREATE INDEX"一起写进 §10 |
| §13 约束 13 环容量 | 检测 + 一键修复已做；容量 128 未提、无自动修复 | **接受**"检测 + 手工修复"为 v1 终态，写运维规程 |
| 分片 clog 物理删除 | **已做**（`ShardClogAtCommit` 提交时 rmtree） | 划掉 |
| Proxy 守护组件 | 实质已由 Citus MX 取代 | **裁掉**，DESIGN §1.1 同步改 |
| R3 可读性 | 新宇宙已不依赖；只剩遗留宇宙副本 | **裁掉**：遗留副本退役，R3 不进 P6 |

## 三、演示实跑新撞出的 7 条缺陷（09-06/07，详见附录）

| # | 缺陷 | 严重度 | 建议修法 | 要裁的 |
|---|---|---|---|---|
| A | **2PC 阶段 3 的 COMMIT 标记不带分片 xid** ⇒ 副本分片 clog 对每笔跨分片事务永远 PREPARED ⇒ 切主后已提交的跨分片行永久不可见 | ★★ 数据不可见且不自愈 | `COMMIT PREPARED` 路径从参与登记 / 2PC 状态文件取该分片的 prepared 分片 xid 回填标记；tx1 + 切主联测 | 做（建议本期） |
| B | **升主不发 FILESET_UPDATE** ⇒ 其余副本对新主的流"收得到、放不了"，且失去再次当选资格 | ★★ | `PartDistRoutePromote` 发 `FILESET_UPDATE{old=locmap 里旧主文件号, new=本地 fileset}`（D1 机制现成）；修好前运维动作 = 从新主重新供给 | 做（建议本期） |
| C | **物理基线不搬分片 clog** ⇒ 数据之后才供给的副本，对基线前提交的号没有判决，升主即丢行 | ★★ | 基线附带 clog 段（新 CTRL 子类型），或供给时随基线传 clog 文件；修好前纪律 = 先供给后写 | 做 / 接受为供给纪律 |
| D | MARKER `start_ts` 是墙钟，副本 PREPARED 槽 sts=8.4e14（R-P3-2 成真） | ★ | `PartWALBuildMarkerPayload` 用 TSO start_ts | 做 |
| E | 回放 worker 泄漏目录 fd（`exceeded maxAllocatedDescs`） | ★ | 找出缺 `FreeDir` 的 `AllocateDir` | 做 |
| F | TSO 客户端 RPC 裸函数名，`partdist` 不在 search_path 即全簇断号 | ★ 一行改 | RPC 文本加 `partdist.` 前缀（5 处） | 做 |
| G | 供给 / 升主不携带"打标身份"（mvcc_set 只由 set_shard_mvcc 或重启装载） | ★ | provision / promote 时 `ShardMvccSetAdd` + 写白名单 | 做 |

## 四、审计 §三 里其余需裁"修 / 推后 / 接受"的缺陷（按生产影响排序）

| # | 事项 | 建议 |
|---|---|---|
| 1 | leader `DROP TABLE` ⇒ 副本侧静默，壳表/槽位/目录永不回收 | 做（新 opcode） |
| 2 | leader 任何 DDL ⇒ follower 停栅栏，人工等价 DDL | 推后（专项），先写运维规程 |
| 3 | 物理基线 1 GB 上限（`fileset_inline_max_blocks`） | 推后（流式基线专项）；先写进 §10 |
| 4 | 分配器 shmem 槽位 64/节点、无产品侧回收 | 做（同判据回收器） |
| 5 | 禁用清单漏 8 个 Citus UDF（split / isolate_tenant / drain_node / master_* / replicate_table_shards / schema_move / set_access_method） | 做（加名字） |
| 6 | 引用表运行期写无守卫 | 见 §一-4 |
| 7 | Raft 成员变更缺失 ⇒ 拓扑不可变更 | 推后（raft 专项，需解冻） |
| 8 | `RAFT_MAX_GROUPS=32`；每分片建组/供副本全手工 | 推后；先做"给一张表全部分片建组+供副本"的脚本 |
| 9 | 升主 deadline 60s 兜底放行 | 接受，写运维规程 |
| 10 | 子事务禁写分片表（设计 §5.4 未实现） | 接受，写进 §10 |
| 11 | R-P6-14 逻辑解码禁令绕过（walsender） | 推后（内核补丁面） |

## 五、环境与流程

| 事项 | 建议 |
|---|---|
| 宿主 3.9 GB 内存跑 9 节点 + 全量门禁 | 跑门禁前腾内存或分批 |
| 演示环境的 `ALTER DATABASE ... SET search_path` 是绕法（§三-F 修好即可撤） | 修 F 后从演示文档删掉这一步 |
| 门禁的 OPS 8 套 | 同 §一-1 |

---

## 附录：7 条新缺陷的原始条目（09-06 审计补充，原拟并入 `P6_EXIT_AUDIT.md` §3.2/§3.4/§3.5）

- **★★ 2PC 阶段 3 的 COMMIT 标记不带分片 xid ⇒ 每个副本的分片 clog 对每笔跨分片事务永远停在 PREPARED**。
  `dtx_participant.c` 在 `COMMIT PREPARED` 时用 `PartWALBuildMarkerPayload()` 组装终局标记，而该函数按
  `ShardXidXactCount() > 0` 决定带不带分片 xid 尾（`partwal_sync.c:721–727`）——`COMMIT PREPARED` 跑在**另一个**
  没碰过分片表的事务里，计数恒 0 ⇒ 24 字节旧格式（实测：协调分片流 plsn 26、参与分片流 plsn 34 都是
  `flags=2 len=24`，普通事务的标记是 `len=32`）⇒ 回放侧 `sxid` 非法、`ShardClogSetVerdict` 被跳过
  （`shard_replay.c:1340–1351`），只有 gclog 得到 COMMITTED。**后果（09-06 实跑）**：切主后新主上那笔 2PC
  插入的行 `st=1` 不可见；协调组的 DECISION 已被 FORGET 回收、`pg_dist_transaction` 已 GC ⇒
  `dtx_close_indoubt` 四级全落空、返回 0 ⇒ **已提交的行在新主上永久不可见**（三个参与分片的从节点追平后同样
  `st=1`）。方向是 fail-safe（未误判 ABORT），但不可恢复。
- **★★ 升主不发 `FILESET_UPDATE` ⇒ 其余副本对新主的流全部回放失败**：新主的写入进流时带的是它自己的
  relfilenumber（实测 `1663/5/70580`），其余副本的 locmap 仍按旧主文件号配对 ⇒ `replay_catchup` 报
  `未知 relfilelocator ... @plsn 30（fileset 漏登记）`。批次 #10 的 p7 [3b] 只断言了"另一个副本**收到**了新主的
  记录"，没断言回放——收到了却放不了正是 R-P4-15 拦升主的那一格 ⇒ 切主一次后，该分片其余副本全部失去再次
  当选资格，直到从新主重新供给。修法方向：`PartDistRoutePromote` 用 D1 机制发一条
  `FILESET_UPDATE{old=本地 locmap 里旧主的文件号, new=本地 fileset}`。
- **★★ 物理基线不带分片 clog**：`shard_baseline_emit` 只灌页面 + 抬发号水位，不搬 `pg_shard_clog/<oid>`；
  基线游标之前的 MARKER 不再回放 ⇒ 在已有数据之后才供给的副本，对基线之前提交的每个分片 xid 都没有判决
  （实测：从新主重新供给后 `sclog_full(<新主写的 xid>) = st=0`，更早经流回放得到的号仍在）。这样的副本一旦
  升主，那些行是 RUNNING（不可见），再被 `shard_claim_on_promote` 改判 ABORTED。供给只对"先供给、后写数据"
  的分片是安全的；也解释了 p7 "新主读到 40 行" 为何只能靠原生路径的巧合通过。
- **MARKER 的 `start_ts` 是本地墙钟**（`partwal_sync.c:730` `GetCurrentTransactionStartTimestamp()`），不是 TSO
  的 start_ts；回放侧原样写进分片 clog 的 PREPARED 槽（`ShardClogSetPrepared(..., m->start_ts)`）。实测副本上
  `sts=842001420732582`（2026-09-06 的微秒时间戳）。这正是 R-P3-2「双 ts 宇宙串线」——当时记"实证零比较点"，
  现在有了一个：升主后 §4.2 三态处置的第一支拿它与 TSO 快照比，恒为"跳过"。
- **回放 worker 泄漏目录描述符**：同一节点连续多次 `replay_catchup`（三个槽位）后报
  `exceeded maxAllocatedDescs (328) while trying to open directory ".../pg_parwal/<oid>"`，该节点此后所有回放都
  失败，直到 worker 重启。某条 `AllocateDir` 路径缺 `FreeDir`。
- **TSO 客户端的 RPC 不带 schema 前缀**：`tso_client.c:416/442/454/502/556` 发的是
  `SELECT partdist_tso_start_ts(...)` 等裸函数名；批次 #9 的 `refresh_extension_sql.sh` 把这些 C 函数装进了
  `partdist` 模式，协调者默认 `search_path = "$user", public` ⇒ 取号、心跳、commit_ts、safe_ts 的 RPC 全部
  `function does not exist` ⇒ 全簇分片写 fail-closed。此前能跑只因 P4 期各套件在 public 里现建同名垫片。
  临时绕法：`ALTER DATABASE postgres SET search_path TO "$user", public, partdist`。
- **供给 / 升主不携带"打标身份"**：判定一张表是否打标只看 GUC 白名单 `shard_relids` 或 shmem `mvcc_set`
  （`shard_oid_is_mvcc`），而 `mvcc_set` 只由 `partdist_set_shard_mvcc()`（`shard_xid.c:2034`）或重启时扫
  `pg_shard_xid/` 目录装载；`provision_shard_replica` / `PartDistRoutePromote` 都不加。follower 从不设白名单 ⇒
  升主后的新主若未事先手工加白名单又未重启，写入不打标、读走原生路径。p7 [3b] 与 tx3 [4] 都是在这个状态下
  通过的——通过的原因是错的。演示文档 6c 因此要求给组内全部成员配白名单。
