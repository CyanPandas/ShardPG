#!/usr/bin/env bash
# [宿主机] 批次 5 生产化两条的验收：
#   T7.17（P7-V1）分片 vacuum **自动启动器**
#   T7.18（P7-V2）分片 vacuum **尾部截断**（见 [7]）
#
# 在此之前，分片 xid 到龄只发一条 WARNING，三步（算目标 → 趟页面 → 截断 clog）
# 全靠人手工敲；运维没看见那条 WARNING，龄就一路涨到停发线，**该分片进只读**。
# 本套件验的是"到龄了真的会自己跑"，以及它在各种不该跑的情形下**真的不跑**。
#
# 覆盖：
#   [1] 夹具：登记打标表 + 制造三类垃圾（中止 xmin / 已提交删除 / lock-only xmax）
#   [2] ★ 负向：取不到 GlobalSafeTs 一律停在 no-safe-ts —— 少清一轮无害，
#       拿不可信的安全线截断 clog 不可逆
#   [3] 手工调用：截断点推进、数据完好
#   [4] ★ 核心：不手工调，心跳自己干（限时收敛 + 节点日志留痕）
#   [5] 开关 off ⇒ 同样条件下水位一动不动（证明 [4] 是启动器干的，不是别的机制）
#   [6] 两态恢复：「趟完未截断」只补截断，不重跑页面趟
#   [7] 表没了但槽位还在 ⇒ 记 gone，不报错、不拖累同批其它分片
# 工程纪律：容器调用一律 </dev/null；断言空值即 FAIL；负向配计数守卫；
#       GUC 改动挂 EXIT 复原；结尾节点健康检查。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
WPORT="${WPORT:-5433}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }

check() {
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

NEG_RUN=0
neg() {  # neg <名字> <期望片段> <SQL>
  local hit
  hit=$(PSQL "$WPORT" -Atc "$3" </dev/null 2>&1 | grep -c "$2")
  check "负向:$1" "$hit" "1"
  NEG_RUN=$((NEG_RUN+1))
}

# ★ 函数一律写**全限定名**。扩展装在 partdist schema 而 search_path 只有
#   "$user",public —— 不限定就得靠夹具在 public 建同名垫片（test_shard_clog_p2
#   与 test_dtx_tso_p4 都这么干过），那等于把用例绑在另一套用例的残留上。
source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

# ---- GUC 改动一律挂 EXIT 复原 ----
# ★ 本套件把 shard_vacuum_max_age 调到 1，也就是让**本节点所有分片**都算到龄。
#   不复原的话，之后每一套都会被心跳顺手 vacuum，红起来的样子与被测内容毫无
#   关系 —— 与 lib_topology.sh 头注释里"停节点必复原"是同一条纪律。
restore_gucs() {
  # ★ ALTER SYSTEM 必须**各自一条** -c：多条语句挤进一个 -c 会被 psql 当作
  #   隐式事务块发出去，而 ALTER SYSTEM 在事务块里直接 ERROR。
  PSQL "$WPORT" -q \
    -c "ALTER SYSTEM RESET pg_partdist.shard_vacuum_max_age" \
    -c "ALTER SYSTEM RESET pg_partdist.shard_vacuum_auto" \
    -c "ALTER SYSTEM RESET pg_partdist.shard_vacuum_truncate" \
    -c "ALTER SYSTEM RESET pg_partdist.shard_vacuum_fault" \
    -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo" \
    -c "SELECT pg_reload_conf()" </dev/null >/dev/null 2>&1 || true
  PSQL "$COORD" -q -c "ALTER SYSTEM RESET pg_partdist.tso_master" \
                -c "SELECT pg_reload_conf()" </dev/null >/dev/null 2>&1 || true
  echo "  [复原] GUC 已 RESET（max_age / auto / tso_conninfo）"
}
trap restore_gucs EXIT

set_guc() {  # set_guc <名> <值>；等 SHOW 真的变了才返回
  local n=$1 v=$2 got="" t
  PSQL "$WPORT" -q -c "ALTER SYSTEM SET $n = $v" -c "SELECT pg_reload_conf()" </dev/null >/dev/null
  for t in $(seq 1 15); do
    got=$(PSQL "$WPORT" -Atc "SHOW $n" </dev/null | tail -1)
    [[ -n "$got" ]] && break
    sleep 1
  done
  echo "$got"
}

WM()  { PSQL "$WPORT" -Atc "SELECT clog_truncate_before||'/'||shard_vacuum_xid FROM partdist.shard_vacuum_watermarks($1::oid)" </dev/null | tail -1; }
TB()  { PSQL "$WPORT" -Atc "SELECT clog_truncate_before FROM partdist.shard_vacuum_watermarks($1::oid)" </dev/null | tail -1; }
AGE() { PSQL "$WPORT" -Atc "SELECT age||'/'||phase FROM partdist.shard_xid_age($1::oid)" </dev/null | tail -1; }

echo "========== [0] 前置：节点与函数面 =========="
up=$(PSQL "$WPORT" -Atc "SELECT 1" </dev/null)
check "worker :$WPORT 可用" "$up" "1"
fn=$(PSQL "$WPORT" -Atc "SELECT count(*) FROM pg_proc WHERE proname IN ('shard_vacuum_auto','shard_xid_age','shard_vacuum_watermarks')" </dev/null)
check "自动启动器与观测函数在位（3）" "$fn" "3"

echo "========== [1] 夹具：TSO 上线 → 打标表 → 三类垃圾 =========="
# ★★ TSO 必须在**写入之前**上线，顺序不能反。
#   截断前缀扫描的放行判据是 `commit_ts < GlobalSafeTs`，而 commit_ts 是写入
#   那一刻由 TsoMarkerCommitTs() 定的：配了 TSO 是 TSO 号，没配是本地墙钟
#   （~2.1e9 / ~8.4e14）。先写后配，clog 里存的就是墙钟，拿去和 TSO 号比
#   大小恒不放行 —— 实测 detail 一路 `commit-ts-too-new`，截断点一步不动。
#   这就是台账上的 **P7-G4（commit_ts 无宇宙标志位）**在 vacuum 路径上的样子。
PSQL "$COORD" -q -c "ALTER SYSTEM SET pg_partdist.tso_master = on" -c "SELECT pg_reload_conf()" </dev/null >/dev/null
PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=$COORD dbname=postgres user=postgres'" -c "SELECT pg_reload_conf()" </dev/null >/dev/null
sts=""
for t in $(seq 1 20); do
  sts=$(PSQL "$COORD" -Atc "SELECT partdist.partdist_global_safe_ts()" </dev/null 2>/dev/null | tail -1)
  [[ -n "$sts" && "$sts" != "0" ]] && break
  sleep 1
done
check "TSO 已上线、GlobalSafeTs 就绪（${sts:-∅}）" \
      "$([[ -n "$sts" && "$sts" != "0" ]] && echo ok)" "ok"

PSQL "$WPORT" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
DROP TABLE IF EXISTS va_auto;
CREATE TABLE va_auto(id int primary key, v text) WITH (autovacuum_enabled=off);
SQL
OA=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='va_auto'" </dev/null)
check "夹具表就位（oid=${OA:-∅}）" "$([[ -n "$OA" ]] && echo ok)" "ok"

PSQL "$WPORT" -q -c "INSERT INTO partdist.partition_map (partition_id, primary_node) VALUES ($OA, 1) ON CONFLICT DO NOTHING;" </dev/null >/dev/null
reg=$(PSQL "$WPORT" -Atc "SELECT partdist.partdist_set_shard_mvcc('va_auto'::regclass); SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "打标登记成功" "$reg" "ok"

# 三类垃圾各造一点：
#   ① 中止 xmin：整事务回滚的插入
#   ② 已提交删除的死元组：插入并提交，再删除并提交
#   ③ lock-only xmax：SELECT FOR UPDATE 后提交（xmax 有值但不是删除）
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
INSERT INTO va_auto SELECT g, 'keep'||g FROM generate_series(1,20) g;
BEGIN; INSERT INTO va_auto SELECT g, 'abort'||g FROM generate_series(101,110) g; ROLLBACK;
DELETE FROM va_auto WHERE id BETWEEN 1 AND 5;
BEGIN; SELECT * FROM va_auto WHERE id = 10 FOR UPDATE; COMMIT;
SQL
alive=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM va_auto" </dev/null | tail -1)
check "夹具活行数（20 插入 − 5 删除 = 15）" "$alive" "15"
nx=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT max(xmin::text::bigint) FROM va_auto" </dev/null | tail -1)
check "元组带的是分片 xid（max(xmin) < 1000）" "$([[ -n "$nx" && "$nx" -lt 1000 ]] && echo ok)" "ok"

echo "========== [2] ★ 负向：取不到 GlobalSafeTs 就不清 =========="
# 临时撤掉本节点的 tso_conninfo —— 数据已经写完（commit_ts 是 TSO 号），
# 这里只让**读安全线**这一步失败，单独验 fail-closed 那一支。
PSQL "$WPORT" -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo" -c "SELECT pg_reload_conf()" </dev/null >/dev/null
got=$(set_guc pg_partdist.shard_vacuum_max_age 1)
check "max_age 调为 1（实得 $got）" "$got" "1"
ph=$(AGE "$OA")
check "分片已到龄（age/phase=${ph}，phase 应为 1）" "${ph##*/}" "1"

tb0=$(TB "$OA")
d0=$(PSQL "$WPORT" -Atc "SELECT detail FROM partdist.shard_vacuum_auto(8)" </dev/null | tail -1)
check "无 GlobalSafeTs 时 detail 记 no-safe-ts（detail=${d0}）" \
      "$(grep -qc "no-safe-ts" <<<"$d0" >/dev/null && echo ok)" "ok"
check "无 GlobalSafeTs 时截断点一动不动（$tb0）" "$(TB "$OA")" "$tb0"

echo "========== [3] 手工调用：截断点推进、数据完好 =========="
PSQL "$WPORT" -q -c "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=$COORD dbname=postgres user=postgres'" -c "SELECT pg_reload_conf()" </dev/null >/dev/null
# 等本节点的心跳把 GlobalSafeTs 重新抬起来（安全线在 master 侧，worker 侧读不到，
# 所以这里问协调者；partdist_global_safe_ts() 本身就是 master-only 的）
sts=""
for t in $(seq 1 25); do
  sts=$(PSQL "$COORD" -Atc "SELECT partdist.partdist_global_safe_ts()" </dev/null 2>/dev/null | tail -1)
  [[ -n "$sts" && "$sts" != "0" ]] && break
  sleep 1
done
check "GlobalSafeTs 重新就绪（${sts:-∅}）" "$([[ -n "$sts" && "$sts" != "0" ]] && echo ok)" "ok"

d1=$(PSQL "$WPORT" -Atc "SELECT shards_truncated||'|'||detail FROM partdist.shard_vacuum_auto(8)" </dev/null | tail -1)
tb1=$(TB "$OA")
check "手工调用后截断点推进（$tb0 → $tb1；detail=${d1}）" \
      "$([[ -n "$tb1" && -n "$tb0" && "$tb1" -gt "$tb0" ]] && echo ok)" "ok"
check "清完数据仍完好（15 行）" \
      "$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM va_auto" </dev/null | tail -1)" "15"
check "两水位相等（趟完且已截断）" "$(WM "$OA")" "$tb1/$tb1"

echo "========== [4] ★ 核心：不手工调，心跳自己干 =========="
# 再制造一批可清的账目，让截断点还能往前走
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
INSERT INTO va_auto SELECT g, 'r2-'||g FROM generate_series(201,215) g;
DELETE FROM va_auto WHERE id BETWEEN 201 AND 205;
BEGIN; INSERT INTO va_auto SELECT g, 'r2a'||g FROM generate_series(301,305) g; ROLLBACK;
SQL
# ★ 日志文件名不能猜（P7-T4 的同一个坑，我在这套里又踩了一次）：
#   setup-raft.sh 写 <datadir>/startup.log；reproduce-env.sh 写 <datadir>.log；
#   而**出口门禁净场用 `-l <datadir>/pg.log` 拉起节点**。写死任何一种，
#   单跑绿、进批就红（实测 `worker1.log: No such file or directory`）。
#   按"数据目录下任何 .log + 同名 .log"一网打尽。
NODELOGS="/work/pg-cluster-data/worker$((WPORT-5432))/*.log /work/pg-cluster-data/worker$((WPORT-5432)).log"
logmark=$(DEX bash -c "cat $NODELOGS 2>/dev/null | wc -l" </dev/null | tr -d '[:space:]')
logmark=${logmark:-0}
tb2=$(TB "$OA")
auto_ok=""; waited=0
for t in $(seq 1 40); do
  cur=$(TB "$OA")
  if [[ -n "$cur" && -n "$tb2" && "$cur" -gt "$tb2" ]]; then auto_ok=ok; waited=$t; break; fi
  sleep 1
done
check "★ 心跳在 ${waited}s 内自动推进了截断点（$tb2 → $(TB "$OA")）" "$auto_ok" "ok"
hit=$(DEX bash -c "cat $NODELOGS 2>/dev/null | tail -n +$((logmark + 1)) | grep -c '分片 vacuum 自动启动器' || true" </dev/null | tail -1 | tr -d '[:space:]')
check "★ 节点日志留痕（≥1，实际 $hit）" "$([[ -n "$hit" && "$hit" -ge 1 ]] && echo ok)" "ok"

echo "========== [5] 开关 off：同样条件下一动不动 =========="
got=$(set_guc pg_partdist.shard_vacuum_auto off)
check "开关已关（实得 $got）" "$got" "off"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
INSERT INTO va_auto SELECT g, 'r3-'||g FROM generate_series(401,410) g;
DELETE FROM va_auto WHERE id BETWEEN 401 AND 405;
SQL
tb3=$(TB "$OA")
sleep 20
check "关掉之后截断点不动（$tb3）" "$(TB "$OA")" "$tb3"
# 而手工调用仍然有效 —— 证明"不动"是开关造成的，不是没东西可清
PSQL "$WPORT" -Atc "SELECT partdist.shard_vacuum_auto(8)" </dev/null >/dev/null
check "同一时刻手工调用仍能推进（证明上一条是开关起的作用）" \
      "$([[ "$(TB "$OA")" -gt "$tb3" ]] && echo ok)" "ok"
got=$(set_guc pg_partdist.shard_vacuum_auto on)
check "开关已恢复" "$got" "on"

echo "========== [6] 两态恢复：趟完未截断，只补截断 =========="
# ★ 先把心跳关掉再造场景：开着的话它会抢在我的手工调用之前把这一格补掉，
#   于是 detail 里什么都没有 —— 上一轮实测就这么红过一次（水位确实收敛了，
#   只是不是我这次调用干的）。要验"谁干的"，就得让场上只有一个人。
got=$(set_guc pg_partdist.shard_vacuum_auto off)
check "两态场景前：心跳已关（实得 $got）" "$got" "off"
tb4=$(TB "$OA")
# ★ 目标必须 > FIRST_SHARD_XID(3)：`ShardClogTruncate` 对更小的目标直接返回 0
#   （"没什么可截的"，设计如此）。前面几节没能把截断点推起来时，这一节会拿
#   tb=0 去造场景，然后把一个**设计上的空操作**报成缺陷 —— 所以先把前提挑明。
check "两态恢复的前提：前面已把截断点推到 > 3（tb=${tb4}）" \
      "$([[ -n "$tb4" && "$tb4" -gt 3 ]] && echo ok)" "ok"
# 人为造出「趟完、截断没做」：把 shard_vacuum_xid 抬到 trunc_before 之上
PSQL "$WPORT" -q -c "SELECT partdist.shard_vacuum_set_watermarks($OA::oid, $tb4::bigint, ($tb4+1)::bigint)" </dev/null >/dev/null
check "已造出两态之二（tb < vx）" "$(WM "$OA")" "$tb4/$((tb4+1))"
d6=$(PSQL "$WPORT" -Atc "SELECT detail FROM partdist.shard_vacuum_auto(8)" </dev/null | tail -1)
check "detail 记 recover（detail=${d6}）" \
      "$(grep -qc "${OA}:recover" <<<"$d6" >/dev/null && echo ok)" "ok"
check "截断点补到 vx（两水位重新相等）" "$(WM "$OA")" "$((tb4+1))/$((tb4+1))"
got=$(set_guc pg_partdist.shard_vacuum_auto on)
check "两态场景后：心跳已恢复" "$got" "on"

echo "========== [7] ★ T7.18（P7-V2）：尾部截断把空间还回去 =========="
# 在此之前，分片 vacuum 只清元组、不还空间：`shard_vacuum.c` 全文没有
# smgrtruncate/RelationTruncate，删掉整表尾部之后关系文件仍是原大小。
#
# ★ 整节把心跳关掉：本节验的是 **sweep 的收尾动作**，场上只能有一个清扫者 ——
#   否则心跳会抢在手工 sweep 之前把元组清掉，手工那一次拿到 removed_dead=0，
#   断言就变成在验"谁先动手"而不是"截断做没做"（上一轮实测红过一次）。
got=$(set_guc pg_partdist.shard_vacuum_auto off)
check "截断一节前：心跳已关（实得 $got）" "$got" "off"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
DROP TABLE IF EXISTS va_trunc;
CREATE TABLE va_trunc(id int primary key, v text) WITH (autovacuum_enabled=off);
SQL
OT=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='va_trunc'" </dev/null)
PSQL "$WPORT" -q -c "INSERT INTO partdist.partition_map (partition_id, primary_node) VALUES ($OT, 1) ON CONFLICT DO NOTHING" </dev/null >/dev/null
regt=$(PSQL "$WPORT" -Atc "SELECT partdist.partdist_set_shard_mvcc('va_trunc'::regclass); SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "截断夹具：打标登记成功" "$regt" "ok"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
INSERT INTO va_trunc SELECT g, repeat('x', 120) FROM generate_series(1, 6000) g;
SQL
sz0=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_size('va_trunc')/8192" </dev/null | tail -1)
check "截断夹具：关系已有足够多的页（${sz0} 块，需 >= 16）" \
      "$([[ -n "$sz0" && "$sz0" -ge 16 ]] && echo ok)" "ok"
# 删掉**尾部**那一半 —— id 大的是后插入的，落在尾页上
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation=off; DELETE FROM va_trunc WHERE id > 3000" </dev/null >/dev/null
# ★ 这一节直接驱动 sweep，不走心跳：截断是 sweep 的收尾动作，与"什么时候被
#   触发"是两件事；混在一起验，时序一抖就分不清是截断没做还是压根没触发。
#   （触发那一半已由 [4] 单独验过。）
swp=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off;
        SELECT swept||'|'||removed_dead FROM partdist.shard_vacuum_sweep('va_trunc'::regclass,
          partdist.shard_xid_next('va_trunc'::regclass::oid)::bigint)" </dev/null | tail -1)
check "整趟干净且删掉了尾部那 3000 行（${swp}）" "$swp" "true|3000"
sz1=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_size('va_trunc')/8192" </dev/null | tail -1)
check "★ 尾部截断把关系缩小了（${sz0} → ${sz1} 块）" \
      "$([[ -n "$sz1" && -n "$sz0" && "$sz1" -lt "$sz0" ]] && echo ok)" "ok"
check "截断之后剩下的行一条不少（3000）" \
      "$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM va_trunc" </dev/null | tail -1)" "3000"
check "截断之后仍能按主键读到边界行（id=3000）" \
      "$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM va_trunc WHERE id=3000" </dev/null | tail -1)" "1"
# 关掉开关：同样造一段空尾，大小不许再变
got=$(set_guc pg_partdist.shard_vacuum_truncate off)
check "截断开关已关（实得 $got）" "$got" "off"
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation=off; DELETE FROM va_trunc WHERE id > 1500" </dev/null >/dev/null
sz2=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_size('va_trunc')/8192" </dev/null | tail -1)
swp2=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off;
        SELECT swept||'|'||removed_dead FROM partdist.shard_vacuum_sweep('va_trunc'::regclass,
          partdist.shard_xid_next('va_trunc'::regclass::oid)::bigint)" </dev/null | tail -1)
check "关掉截断后元组照样被清掉（${swp2}）" "$swp2" "true|1500"
check "关掉之后关系大小不变（${sz2} 块）—— 只清元组、不还空间" \
      "$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT pg_relation_size('va_trunc')/8192" </dev/null | tail -1)" "$sz2"
got=$(set_guc pg_partdist.shard_vacuum_truncate on)
check "截断开关已恢复" "$got" "on"
# ★ 心跳留到 [10] 结束再开 —— [8]/[9] 验的同样是 sweep 内部，场上仍只能有
#   一个清扫者。实测开着跑过一轮：心跳抢先把 va_fault 清完并把水位推到 5/5，
#   于是"标记一个都没落下"和"整趟重来一次就干净"两条连带红，而红的原因与
#   注入点毫无关系。
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE va_trunc" </dev/null >/dev/null

echo "========== [8] ★ T7.19（P7-V3）两态之一：页面循环中间崩 → 整趟重来 =========="
# 设计 §6.5 的两条恢复路此前**没有任何用例能走到** —— 它们要求"在页面循环中间
# 崩一次"和"落完标记、截断之前崩一次"，这两个时刻都在一个 C 函数内部，从 SQL
# 面够不着。没人走过的恢复路径等于没有，所以在产品路径上开了显式注入点。
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
DROP TABLE IF EXISTS va_fault;
CREATE TABLE va_fault(id int primary key, v text) WITH (autovacuum_enabled=off);
SQL
OF=$(PSQL "$WPORT" -Atc "SELECT oid FROM pg_class WHERE relname='va_fault'" </dev/null)
PSQL "$WPORT" -q -c "INSERT INTO partdist.partition_map (partition_id, primary_node) VALUES ($OF, 1) ON CONFLICT DO NOTHING" </dev/null >/dev/null
regf=$(PSQL "$WPORT" -Atc "SELECT partdist.partdist_set_shard_mvcc('va_fault'::regclass); SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "故障注入夹具：打标登记成功" "$regf" "ok"
PSQL "$WPORT" -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
SET citus.enable_ddl_propagation TO off;
INSERT INTO va_fault SELECT g, repeat('y',120) FROM generate_series(1,6000) g;
DELETE FROM va_fault WHERE id > 3000;
SQL
wm_before=$(WM "$OF")
got=$(set_guc pg_partdist.shard_vacuum_fault mid_prune)
check "注入点已设为 mid_prune（实得 $got）" "$got" "mid_prune"
neg "页面循环中间被注入中止" "注入的 vacuum 故障点" \
    "SET citus.enable_ddl_propagation=off; SELECT swept FROM partdist.shard_vacuum_sweep('va_fault'::regclass, partdist.shard_xid_next('va_fault'::regclass::oid)::bigint)"
check "★ 两态之一：标记一个都没落下（水位原样 ${wm_before}）" "$(WM "$OF")" "$wm_before"
got=$(set_guc pg_partdist.shard_vacuum_fault off)
check "注入点已关" "$got" "off"
swpf=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off;
        SELECT swept||'|'||removed_dead FROM partdist.shard_vacuum_sweep('va_fault'::regclass,
          partdist.shard_xid_next('va_fault'::regclass::oid)::bigint)" </dev/null | tail -1)
check "★ 整趟重来一次就干净（${swpf}）" "$swpf" "true|3000"
check "重来之后数据完好（3000 行）" \
      "$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM va_fault" </dev/null | tail -1)" "3000"

echo "========== [9] ★ T7.19 两态之二：落完标记崩 → 只补截断 =========="
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation=off; DELETE FROM va_fault WHERE id > 1500" </dev/null >/dev/null
got=$(set_guc pg_partdist.shard_vacuum_fault after_mark)
check "注入点已设为 after_mark（实得 $got）" "$got" "after_mark"
neg "落完标记、截断之前被注入中止" "注入的 vacuum 故障点" \
    "SET citus.enable_ddl_propagation=off; SELECT swept FROM partdist.shard_vacuum_sweep('va_fault'::regclass, partdist.shard_xid_next('va_fault'::regclass::oid)::bigint)"
wm2=$(WM "$OF"); tb2=${wm2%%/*}; vx2=${wm2##*/}
check "★ 两态之二成立：标记落了、截断没做（${wm2}）" \
      "$([[ -n "$tb2" && -n "$vx2" && "$vx2" -gt "$tb2" ]] && echo ok)" "ok"
got=$(set_guc pg_partdist.shard_vacuum_fault off)
check "注入点已关（二）" "$got" "off"
act=$(PSQL "$WPORT" -Atc "SELECT partdist.shard_vacuum_recover($OF::oid)" </dev/null | tail -1)
check "恢复动作 = truncated（只补截断）" "$act" "truncated"
check "两水位重新相等（${vx2}/${vx2}）" "$(WM "$OF")" "$vx2/$vx2"
# ★ "只补截断、不重跑页面趟"的取证：页面早在崩之前就清完了，所以紧接着再跑
#   一整趟必然一条都删不动。删得动就说明上一趟其实没清完，标记是假的。
swpf2=$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off;
        SELECT swept||'|'||removed_dead FROM partdist.shard_vacuum_sweep('va_fault'::regclass,
          partdist.shard_xid_next('va_fault'::regclass::oid)::bigint)" </dev/null | tail -1)
check "★ 页面确实早已清完（再跑一趟删不动：${swpf2}）" "$swpf2" "true|0"
check "恢复之后数据完好（1500 行）" \
      "$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM va_fault" </dev/null | tail -1)" "1500"

echo "========== [10] ★ T7.19：clog 整段删除分支 =========="
# 段 = 2^20 个 xid，靠真发号跨段要烧一百万个，跑不起。这一支验的是
# `ShardClogTruncate` 的**整段 unlink 循环**，前置门禁（趟完才许截断）由 [9]
# 单独验过，所以这里直接把"趟完"标记抬到段边界之上，只驱动那个循环。
segs0=$(DEX bash -c "ls /work/pg-cluster-data/worker$((WPORT-5432))/pg_shard_clog/$OF 2>/dev/null | wc -l" </dev/null | tr -d '[:space:]')
check "截断前该分片有 clog 段文件（${segs0} 个）" \
      "$([[ -n "$segs0" && "$segs0" -ge 1 ]] && echo ok)" "ok"
PSQL "$WPORT" -q -c "SELECT partdist.shard_vacuum_set_watermarks($OF::oid, $vx2::bigint, 2100000::bigint)" </dev/null >/dev/null
nseg=$(PSQL "$WPORT" -Atc "SELECT partdist.shard_clog_truncate($OF::oid, 1500000::bigint)" </dev/null | tail -1)
check "★ 整段删除分支：删掉了 ≥1 个整段（实删 ${nseg}）" \
      "$([[ -n "$nseg" && "$nseg" -ge 1 ]] && echo ok)" "ok"
segs1=$(DEX bash -c "ls /work/pg-cluster-data/worker$((WPORT-5432))/pg_shard_clog/$OF 2>/dev/null | wc -l" </dev/null | tr -d '[:space:]')
check "段文件确实少了（${segs0} → ${segs1}）" \
      "$([[ -n "$segs1" && -n "$segs0" && "$segs1" -lt "$segs0" ]] && echo ok)" "ok"
# 截断点之下进免查隐式冻结区 = "已提交且对一切快照可见"，所以老行照样读得到
check "截断之后老数据仍可见（免查区语义，1500 行）" \
      "$(PSQL "$WPORT" -Atc "SET citus.enable_ddl_propagation=off; SELECT count(*) FROM va_fault" </dev/null | tail -1)" "1500"
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE va_fault" </dev/null >/dev/null
got=$(set_guc pg_partdist.shard_vacuum_auto on)
check "sweep 内部诸节结束：心跳已恢复" "$got" "on"

echo "========== [11] 表没了但槽位还在：记 gone，不报错 =========="
PSQL "$WPORT" -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE va_auto;" </dev/null >/dev/null
# DROP 会一并归还槽位（T7.7），所以这里不强求 detail 里一定出现 gone ——
# 要验的是**不报错、不拖累同批**：函数照常返回一行。
d7=$(PSQL "$WPORT" -Atc "SELECT shards_considered FROM partdist.shard_vacuum_auto(8)" </dev/null 2>&1 | tail -1)
check "夹具表 DROP 之后自动启动器仍正常返回（considered=${d7}）" \
      "$([[ "$d7" =~ ^[0-9]+$ ]] && echo ok)" "ok"
# ★ 真负向：自动启动器整条链的地基是不变式 clog_truncate_before <= shard_vacuum_xid
#   （截断门禁只认 shard_vacuum_xid 这个"趟完"凭据）。违反它必须当场 ERROR，
#   否则自动启动器就可能拿一个假凭据去截断。
neg "水位不变式 tb <= vx 违反即拒" "不变式" \
    "SELECT partdist.shard_vacuum_set_watermarks($OA::oid, 10::bigint, 5::bigint)"

echo "========== [12] 负向计数守卫 + 节点健康 =========="
check "负向用例计数守卫（应跑 3 条）" "$NEG_RUN" "3"
health_check_no_crash

echo ""
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
if [[ "$FAIL" -eq 0 ]]; then echo "T7.17 分片 vacuum 自动启动器：全部通过"; else echo "T7.17 分片 vacuum 自动启动器：存在 FAIL"; fi
