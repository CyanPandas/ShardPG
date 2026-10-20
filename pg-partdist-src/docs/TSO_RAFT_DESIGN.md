# TSO 随控制面 leader 发号 + 切主后分片 xid 不重发（2026-10-19）

路线 A（每个节点一主两备 PG 物理流复制 + Raft 控制面）上的两处改动：

1. **TSO 不再是协调者独有**：发号者 = 控制面（组 0）的 leader。协调者宕机后，控制面在其余节点里选出新 leader，它接着发号，集群继续读写。
2. **切主后分片 xid 不重发**：分片 xid 的发号水位推进时记一条 WAL，物理备库提升后从回放到的水位之后发号。

开关：`pg_partdist.tso_raft = on`（POSTMASTER）。关着时行为与原来逐字节相同（`pg_partdist.tso_master` 单点发号）。分片 xid 的修复不受开关控制，始终生效。

## 1. 为什么要改

| 问题 | 原来的表现 |
|---|---|
| 协调者是数据路径上的单点 | 每个事务开始都要向 TSO 取 start_ts，写事务提交还要取 commit_ts；TSO 只在协调者上，取不到号就报错（fail-closed，绝不用本地时钟顶替）。协调者一挂，3 个 worker 都活着也不能读写。只有给协调者配了物理备库（COORD_HA=1）才能由备库接替，现有节点补不上。 |
| 切主后分片 xid 重发（2026-10-18 晚发现） | 分片 xid 的发号水位只写本地文件，不进 WAL，物理备库上是旧值。备库提升后，发号起点只从回放的提交 / 判决记录里学到，旧主"写了元组但没提交"的事务用过的号会被重发；新事务一提交，旧主没提交的元组也跟着"复活"。lab 实测：仓 54 订单 1409 的崩溃事务与切主后仓 14 的 Delivery 共用分片 xid 269327。 |

## 2. TSO 随控制面 leader

### 2.1 角色与状态

- **发号者**：控制面 leader 上的 TSO（`tso.c`）。共享内存里多了 `raft_term`（正在以哪个任期发号，0 = 不发）、`raft_lease_term / raft_lease_until`（租约，单调时钟微秒）、`raft_applied_hwm`（本节点已 apply 的预留上界）。
- **预留上界**：经控制面日志提交的 `OP_TSO_HWM {"term":T,"hwm":H}`。每个节点 apply 时把 `raft_applied_hwm` 只抬不降地落进 `$PGDATA/pg_tso_raft_hwm`（fsync + rename），并记 WAL（`XLOG_SHARD_TSO_RAFT_HWM`，物理备库回放时同样抬高）。日志压缩后，控制面快照的 node_map 每行带 `"tso_hwm"`，落后成员装快照时一并抬高。
- **TSO 管理进程**（`pg_raft tso`，`raft_consensus.c` 的 `pg_raft_tso_main`）：每个节点一个，20 ms 一轮；只有控制面 leader 上的那个在干活。

### 2.2 租约

leader（任期 T）每 100 ms 用非阻塞连接向组 0 的其它成员发 `pg_raft_tso_lease_check(T)`，对方返回自己当前的控制面任期。过半数（含自己）回答 `<= T`，就把租约延到**这一轮发出之前的时刻** + `pg_raft.tso_lease_ms`（默认 2000）。发号路径在 TSO 锁里检查租约，过期即拒绝。

为什么成立：成员回答"任期 <= T"之后才可能给任期 > T 的候选人投票。任何新 leader 的选票多数派与这一轮的确认多数派至少有一个交集，所以新 leader 必然在这一轮**发出之后**才当选。

一轮里没回话的连接直接断开，不发 `PQcancel`（它是阻塞的，一个卡死的跟随者会拖住续租，反而停掉全簇发号）。连不上的成员 10 s 后再试。

### 2.3 接任

1. 管理进程看到本节点成为任期 T 的 leader：先停发（若在发），记下单调时钟时刻。
2. 等满 `tso_lease_ms × 1.25 + 200 ms`。旧 leader 的租约从它最后一轮确认的发出时刻算起，新 leader 又一定在那之后当选，所以此时旧 leader 必已停发。1.25 倍与 200 ms 吸收两台机器时钟速率的差异。
3. 本任期的租约确认通过后，自连调用 `pg_raft_tso_takeover(T, batch)`：
   - 提交 `OP_TSO_HWM hwm=0`（屏障）并等它在本节点 apply。按 Raft 规则，前任任期的所有条目随之提交、按序 apply；
   - 起点 A0 = 已 apply 的最大上界（与本节点计数器取大）。前任发出的号都小于它自己已提交的上界，而那个上界不大于 A0；
   - 再提交 `OP_TSO_HWM hwm=A0+batch`，apply 后开始在 [A0, A0+batch) 内发号。
4. 剩余号数不到半批（`pg_raft.tso_batch` 默认 400 万）时，`pg_raft_tso_extend` 续批。
5. 下台或任期变化：立即停发；发号路径本身也按租约判断，续不上就停。

号的单调性：新 leader 在旧 leader 停发之后才发号，且起点不小于旧 leader 发过的任何号。所以对任意两个取号请求，只要 R2 在 R1 拿到回应之后发出，就有 ts(R2) > ts(R1)。

### 2.4 客户端

`pg_partdist.tso_conninfo` 可以写成分号分隔的多个连接串（列出全部节点，每个带 `connect_timeout`）。

- 取号失败（连不上，或对方答"不是发号者 / 租约过期"）就换下一个地址。
- 最近一次成功的地址记在共享内存里，作为新连接的起点。
- 全部地址都拒绝时（换届那几秒），在 `pg_partdist.tso_retry_ms`（默认 20 s）内每 100 ms 重扫一遍，超时才报错。只有一个地址时行为不变。
- GlobalSafeTs 取不到时返回 0（不回收，安全方向），不报错，也不等待。

节点地址随切主迁移时（`OP_NODE_ADDRESS`），维护进程按 `partdist.node_map` 重写全部节点的列表（`pg_raft_routea_reconcile_tso`）。协调者切主（COORD_HA）时不再把各节点的连接串改成单一的新协调者。

**节点内代理（2026-10-20，`pg_partdist.tso_proxy`，默认 on）**。原来每个取号的后端各自连一条到发号者，发号者身上的连接数 = 客户端连接 + 全簇取号后端数。lab 4 台机器 384 终端时协调者 `max_connections = 1500` 被打满（日志里 1 万到 2 万次 `too many clients already`），事务取不到号就失败。现在的做法：

- 每个节点起一个后台进程 "pg_partdist tso proxy"，用一条连接替本节点所有后端取号。它不连本地库，所以 `pg_stat_activity` 里看不到，要按进程名查。
- 后端在共享内存里自己的槽（按 `pgprocno`）登记请求（start / commit），设代理的 latch 后睡在自己的 latch 上。
- 代理每轮把已登记的请求一次收走，发一条 `partdist_tso_start_ts_n(node, oldest, ns) || partdist_tso_commit_ts_n(nc)`。发号者在同一临界区里发 ns / nc 个连续的号，代理逐个回填并唤醒。`oldest` 由代理按本节点活跃快照集合算，start_ts 的"发号即登记"照旧。
- 单调性：号只分给本轮 RPC 发出**之前**已登记的请求，多出来的不留。请求 R2 若在 R1 拿到号之后才登记，它所在的那一轮一定在登记之后才发出，拿到的号一定更大，与逐个取号是同一个保证。
- 换届期间代理在 `tso_retry_ms` 内重试同一批。代理不在（关掉 / 还没起来 / 崩了正在拉起，`bgw_restart_time` = 1 s）时，后端退回直连。
- 代价：每次取号多两次进程唤醒，外加最多一轮在途 RPC 的等待（亚毫秒级）。好处：发号者身上每个节点只有 1 条取号连接；负载高时一轮能合并多个请求。
- 同时修了一个隐患：本节点活跃快照集合原来固定 256 槽，满了就不登记。没登记的快照不进 oldest，GlobalSafeTs 可能越过它。现在按 PGPROC 总数开槽、按 `pgprocno` 定位，不会满。

### 2.5 发号者优先在协调者上

`pg_raft.tso_prefer_coordinator`（默认 on）：协调者追平日志、跟随着一个活着的 leader 持续 20 s 以上，就置 `campaign_pending` 发起一次受控选举，把领导权拿回来（限频 60 s 一次）。原因：TSO 每笔事务约 3.4 次往返，放在协调者上不占 worker 的 CPU，常态下的性能与原来相同。换回来时有一次几秒的发号空窗。控制面 leader 原本就偏向协调者（它的选举超时更短）。

### 2.6 GUC

| GUC | 默认 | 说明 |
|---|---|---|
| `pg_partdist.tso_raft` | off | 打开后 `tso_master` 不再起作用 |
| `pg_partdist.tso_retry_ms` | 20000 | 多地址时找不到发号者的最长等待 |
| `pg_partdist.tso_proxy` | on | 节点内 TSO 代理（见 2.4），改了要重启 |
| `pg_raft.tso_lease_ms` | 2000 | 发号租约；也决定新 leader 接任前的等待。全簇一致 |
| `pg_raft.tso_batch` | 4000000 | 每批预留的号数 |
| `pg_raft.tso_prefer_coordinator` | on | 见 2.5 |

部署：`tpcc/scripts/gen_node_conf.sh` 设 `TSO_RAFT=1`；lab 上用 `tpcc/lab/tsoraft_conf.sh on|off` 改四个节点的配置。

### 2.7 限制

- 换届空窗 ≈ 控制面选举时间 + 1.25 × 租约 + 200 ms。lab 配置 `pg_raft.election_timeout_ms = 6000`（worker 的超时窗口是 6 到 12 s），实测协调者宕机后约 8 s 没有吞吐，期间事务等待、不失败。调小选举超时可以缩短空窗，但控制面在高负载下更容易误选举，没有测。
- Citus 的元数据权威（DDL、加删节点、`citus_update_node`）仍只在协调者上。协调者宕机期间不能改表结构；如果此时 worker 也要切主，改路由那一步做不了。
- 客户端连 worker 走函数下推（元数据已同步到所有 worker）；没有 VIP，连接串要列多个节点。
- 从单点发号的集群直接改成 `tso_raft` 需要整簇重建（起点只与本节点的旧计数器取大）。

## 3. 切主后分片 xid 不重发

- `shard_xid_allocate`：水位推进一批（`SHARD_XID_BATCH` = 65536 个号）时，先插一条 `XLOG_SHARD_XID_WM {shard, wm}`，再写水位文件。这一批里任何号写进元组，元组的 WAL 都排在这条记录之后。所以备库只要见到了元组，就一定先回放过水位。不等刷盘：主库自己重启靠水位文件。
- redo（startup 进程）：`ShardXidRedoRaiseWatermark` 抬高影子，并把水位文件只抬不降地写到 wm，其余三个字段原样带回。影子活不过备库重启，文件可以。
- 提升后第一次碰到该分片，`shard_xid_slot_attach` 取 Max(文件, 影子) 作起点，并把 [claim_wm, 起点) 里仍是 RUNNING 的号判 ABORTED：旧主没提交的元组从此不可见，号也不会再发。
- hot standby 上的读不再挂槽（`ShardXidEnsureClaimed` 恢复期直接返回）。挂槽即认领，会在备库上把主库还在跑的号判 ABORTED，而且槽一旦挂上，提升后就不会再按回放来的水位重新认领。

## 4. 验证（lab 三机，64 迷你仓，路线 A 一主两备）

见 `TSO_RAFT_REPORT_20261019.md`（仓库根目录）。
