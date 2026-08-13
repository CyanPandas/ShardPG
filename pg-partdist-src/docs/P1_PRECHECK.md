# P1 前置核查报告（T1.0）

> 隶属：`TX_TSO_MVCC_DEV_PLAN.md` 任务 T1.0。核查日期 2026-08-12，基线
> `shardpg-TX2` @ `7ff2656`。**本核查未改任何代码。**
> 四条结论：A/B 回答立项时的两个核查问题，C/D 是核查过程中暴露的 P1 新增约束。

---

## 结论 A：hint 位与 pagecmp——设计文档 §4.5"事实 3"不成立，已修正

**核查问题**：已交付系统在原生表上如何通过 pagecmp 逐字节验收（hint 位是不写
WAL 的页面修改，理论上应造成副本分叉）？

**答案：验收从设计上就兼容 hint 位分叉，且有内核背书。** 证据链：

1. `tests/pagecmp.py`（头部说明 + `masked_offsets`/`infomask_violations`）：
   比对**照抄内核 `wal_consistency_checking` 的 `heap_mask()` + `bufmask.c` 掩码
   集合**——未冻结元组掩 `t_infomask & HEAP_XACT_MASK`（可见性提示位，即 hint 位）、
   掩 t_cid / 空洞 / pd_prune_xid / pd_flags 三提示位 / 对齐填充；掩码之外的
   infomask 位**单独严格比对**（HEAP_UPDATED 等真实差异不放过）。比内核更严的
   两条：pd_lsn 刻意不掩（orig_lsn 盖页主张）；"leader 已冻结而 follower 未冻结"
   判差异（冻结记录写 WAL，丢失是真缺陷）。
2. FRD §14.2 判据演化记录（第 ⑨/⑩ 条）：判据从"逐次补例外"演化为"对齐内核
   heap_mask"，R2 实测的对齐填充 4 字节差异即由此收敛。
3. FRD RM_XLOG 表：`FPI_FOR_HINT`（checksum/hint 触发的全页镜像）已在流水线的
   记录清单内，由反向映射表处理——hint 引发的 FPI 早已被消化。
4. 历史拐杖已退役：早期用例靠"末尾 VACUUM (FREEZE) 把 t_infomask 洗成 canonical"
   过比对（r1 夹具仍保留此步），L1 起明确"不再依赖这根拐杖"（掩码集合接管）。

**对 §4.5 决策的影响**：hint 位**不**破坏 pagecmp——原"事实 3（杀手问题）"作废。
但**决策不变**（分片表第一期禁用 hint）：事实 1（免查区无需 hint）、事实 2
（新鲜区一次访问拿 status+commit_ts，hint 省不了）与重定义的三处改动量仍然成立；
禁用的额外收益收窄为"分片表页面零非 WAL 修改，少一份对掩码集合的依赖"。
设计文档 §4.5 已同步修正。

## 结论 B：断言 xmin/xid 的既有用例清单（P1 改打标的受影响面）

全 tests/ 目录 grep（xmin|xmax|txid_current|infomask），命中 3 个脚本：

| 脚本 | 位置 | 用法 | 受影响期 |
|---|---|---|---|
| test_dtx_commit_marker_tx2.sh | :229–236 | 从壳表元组 `xmin::text::bigint` 取 leader xid，再对账 | **P2**（数值空间变为分片 xid，取值手法保留，对账目标改分片 clog） |
| test_lazy_replay_l1.sh | :17,:20,:230,:236 | 注释性假设："follower 元组 xmin 是 leader xid 故不可读"；已弃 freeze 拐杖 | **P2/P5** 复审（可见性接管后"不可读"假设改变） |
| test_follower_replay_r1.sh | :13,:332–333 | 夹具末尾 VACUUM (FREEZE)；"冻结可见/未冻结不可读"断言 | **P5** 复审（freeze 语义被隐式冻结取代）；P1 不受影响 |

**P1 期间以上用例全部不受影响**——前提是结论 C 的门控成立。

## 结论 C（新增约束）：P1 打标必须用显式白名单门控，不得按 partition_map 门控

既有 438 项基线套件的表**全部登记在 partition_map**。若 P1 的打标改造按
"partition_map 成员"生效，全部既有套件的表立即改用分片 xid ⇒ 上表之外的隐性
假设（gclog 按来源节点、freeze 夹具、可见性行为）全面塌方，回归基线报废。

**约束**：P1 的分片表判定（DEV PLAN T1.1）必须用**显式 GUC 白名单**
（`pg_partdist.shard_relids` 之类）门控，仅 P1 新建测试表入名单；既有套件的表
不入名单 ⇒ 走原路径，字节不变。"按 partition_map 驱动"推迟到 P2 随可见性/clog
整体切换时进行。（DEV PLAN T1.1 原文"允许简化为 GUC 白名单"升级为**必须**。）

## 结论 D（新增约束）：P1 必须屏蔽分片表的剪枝与 vacuum，否则数据损坏

核查中推演出的 P1 陷阱：分片 xid 从 3 起小值发号，而原生 clog 的低号段是 initdb
时代的已提交事务——

- **on-access 剪枝**（`heap_page_prune_opt`，普通 SELECT 途中就会触发）：会拿分片
  xid 与原生地平线做模比较、按原生 clog 判死活——一个"被分片事务删除"的元组，
  其分片 xmax（如 5）在原生 clog 里查出"已提交"⇒ 被当死元组**物理清除**，即使
  那个分片事务实际中止。这是 P1 期间最隐蔽的数据损坏路径，因为它不需要任何人
  执行 VACUUM。
- **VACUUM/autovacuum**：同因；且 CREATE TABLE 时 relfrozenxid≈当前原生 nextXid
  （大值），native freeze 会把 xmin=3 判为超龄并重写。

**约束**（并入 DEV PLAN T1.6/T1.9）：
1. T1.6 的可见性分叉必须**同时覆盖剪枝入口**：分片表（白名单命中）直接跳过
   `heap_page_prune_opt`；
2. 分片表建表即设 `autovacuum_enabled=off` reloption，并拦截手动 VACUUM/ANALYZE
   （报错），负向用例进 T1.9；
3. 防回卷 autovacuum 无视 reloption（§13 约束 5 教训），但测试时窗内原生 nextXid
   龄不会触及阈值——P1 接受此残余风险，P5 交付真正的分片级 vacuum 后解除。

---

## 回填清单

- [x] 设计文档 §4.5：事实 3 修正 + 【需核实】标注解除
- [x] DEV PLAN：T1.1 白名单升级为必须；T1.6 增剪枝屏蔽；T1.9 增 vacuum 负向用例；
      V1 勾销；P1 出口清单 T1.0 项勾选
