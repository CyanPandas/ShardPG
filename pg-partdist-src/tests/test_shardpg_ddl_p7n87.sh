#!/usr/bin/env bash
# ============================================================================
# [宿主机] P7-N87 / N88 / N89 / N90 验收：ShardPG 建表 / 删表一条龙 + shardpg 模式 + \dt 子表
#
#   N87  CREATE TABLE ... SHARD KEY (列) [SHARDS n] [REPLICAS n] [COLOCATE WITH 表]
#   N88  DROP TABLE 自动拆组 / 删副本表 / 删登记；事务块里删表走清理队列
#   N89  shardpg.shards 视图、shardpg.* 参数、shardpg.cleanup_dropped()
#   N90  psql \dt 显示分片（子表）与主从
#
# 前提：内核补丁 0016、psql 补丁 0017、pg_partdist（shardpg_ddl.c）都已部署，TSO 已开。
# 用法：CONTAINER=pg-test-container bash test_shardpg_ddl_p7n87.sh
#       （从 scratchpad 副本跑，见夹具规则 43）
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
PASS=0; FAIL=0

# 合流（2>&1 在容器里做，规则 48），-q 去掉命令标签（规则 40）
q()    { docker exec -u postgres "$C" bash -c "exec $BIN/psql -h /tmp -p ${2:-5432} -U postgres -d postgres -X -qAt -c \"\$0\" 2>&1" "$1" </dev/null; }
run()  { docker exec -u postgres "$C" bash -c "exec $BIN/psql -h /tmp -p ${2:-5432} -U postgres -d postgres -X -q -v ON_ERROR_STOP=0 -c \"\$0\" 2>&1" "$1" </dev/null; }
runf() { docker exec -i -u postgres "$C" bash -c "exec $BIN/psql -h /tmp -p ${1:-5432} -U postgres -d postgres -X -q 2>&1"; }

ok()   { PASS=$((PASS + 1)); printf '  PASS  %s\n' "$1"; }
bad()  { FAIL=$((FAIL + 1)); printf '  FAIL  %s\n' "$1"; [[ -n "${2:-}" ]] && printf '%s\n' "$2" | sed 's/^/        | /'; }
expect_eq()   { [[ "$2" == "$3" ]] && ok "$1（= $3）" || bad "$1：期望 [$3]，实际 [$2]"; }
expect_has()  { grep -qF -- "$3" <<<"$2" && ok "$1" || bad "$1：输出里没有 [$3]" "$2"; }
expect_hasnt(){ grep -qF -- "$3" <<<"$2" && bad "$1：输出里不该有 [$3]" "$2" || ok "$1"; }
step() { printf '\n[%s] %s\n' "$1" "$2"; }

WORKERS=$(q "SELECT string_agg(nodeport::text, ' ' ORDER BY nodeport) FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive")
NW=$(wc -w <<<"$WORKERS")

# 各 worker 上指定分片号的 Raft 组 / 物理表 / 登记；master 登记 + 清理队列
residue() {   # $1 = 逗号分隔分片号  $2 = 逗号分隔物理表名
    local p out all=""
    for p in $WORKERS; do
        out=$(q "SELECT (SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN ($1)) || '|' || (SELECT count(*) FROM unnest(string_to_array('$2', ',')) t WHERE to_regclass(t) IS NOT NULL) || '|' || (SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN ($1))" "$p")
        all="$all :$p=$out"
    done
    all="$all master=$(q "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN ($1)")"
    all="$all queue=$(q "SELECT count(*) FROM partdist.shardpg_drop_queue WHERE shardid IN ($1)")"
    printf '%s' "$all"
}
clean_expect() { local w e=""; for w in $WORKERS; do e="$e :$w=0|0|0"; done; printf '%s master=0 queue=0' "$e"; }

ids_of()   { q "SELECT string_agg(shardid::text, ',' ORDER BY shardid) FROM pg_dist_shard WHERE logicalrelid IN ($1)"; }
names_of() { q "SELECT string_agg(shard_name(logicalrelid, shardid), ',' ORDER BY shardid) FROM pg_dist_shard WHERE logicalrelid IN ($1)"; }

# ---------------------------------------------------------------------------
step 0 "前置：节点、部署产物、TSO"
expect_eq "worker 数" "$NW" "4"
expect_eq "shardpg.replicas 参数已注册" "$(q 'SHOW shardpg.replicas')" "2"
expect_eq "shardpg.shard_count 参数已注册" "$(q 'SHOW shardpg.shard_count')" "0"
expect_eq "shardpg.shards 视图存在" "$(q "SELECT to_regclass('shardpg.shards') IS NOT NULL")" "t"
expect_eq "清理队列表存在" "$(q "SELECT to_regclass('partdist.shardpg_drop_queue') IS NOT NULL")" "t"
ts=$(q "SELECT partdist.partdist_tso_client_start_ts()")
[[ "$ts" =~ ^[0-9]+$ ]] && ok "TSO 发号（$ts）" || bad "TSO 发号" "$ts"
CRASH0=$(docker exec "$C" bash -c "cat /work/pg-cluster-data/*/pg.log 2>/dev/null | grep -c 'terminated by signal'")
for t in customer orders oorder t_gucs t_tx t_plain t_local t_citus t_n1 t_n2 t_n3 t_n4 t_n5 t_n6 t_n7 t_n8; do
    [[ "$(q "SELECT to_regclass('public.$t') IS NOT NULL")" == f ]] || bad "开跑前 $t 不该存在（先清场）"
done

# ---------------------------------------------------------------------------
step 1 "语法与前置校验（全部应在建表前报错，不留下表）"
out=$(runf <<'SQL'
BEGIN;
CREATE TABLE t_n1 (a int PRIMARY KEY, SHARD KEY (a));
ROLLBACK;
SQL
)
expect_has "事务块里不能执行" "$out" "cannot run inside a transaction block"
out=$(run "CREATE TABLE t_n2 (a int, b int, PRIMARY KEY (a), SHARD KEY (b))")
expect_has "主键不含分片键（表级主键）" "$out" "分片键 b 必须包含在主键里"
out=$(run "CREATE TABLE t_n3 (a int PRIMARY KEY, b int, SHARD KEY (b))")
expect_has "主键不含分片键（列级主键）" "$out" "分片键 b 必须包含在主键里"
out=$(run "CREATE TABLE t_n4 (a int PRIMARY KEY, SHARD KEY (a) FOO 3)")
expect_has "不认识的选项" "$out" "SHARD KEY 不认识的选项 \"foo\""
out=$(run "CREATE TABLE t_n5 (a int PRIMARY KEY, SHARD KEY (a), SHARD KEY (a))")
expect_has "SHARD KEY 写两次" "$out" "SHARD KEY 只能指定一次"
out=$(run "CREATE TABLE t_n6 (a int PRIMARY KEY, SHARD KEY (zz))")
expect_has "分片键列不存在" "$out" "分片键列 \"zz\" 不存在"
out=$(run "CREATE TABLE t_n7 (a int PRIMARY KEY, SHARD KEY (a) REPLICAS 9)")
expect_has "副本数超上限" "$out" "副本数 9 超出上限"
out=$(run "CREATE TABLE t_n8 (a int PRIMARY KEY, SHARD KEY (a) COLOCATE WITH no_such_tbl)")
expect_has "共置目标不存在" "$out" "共置目标 no_such_tbl 不存在或不是分片表"
out=$(run "CREATE TABLE t_n8 (a int PRIMARY KEY, SHARD KEY (a) SHARDS 2 SHARDS 3)")
expect_has "选项重复" "$out" "SHARD KEY 选项 \"shards\" 重复"
out=$(run "CREATE TABLE t_n8 (a int PRIMARY KEY, SHARD KEY (a) SHARDS 2 COLOCATE WITH pg_class)")
expect_has "COLOCATE 与 SHARDS 不能同时写" "$out" "COLOCATE WITH 时分片数随共置表，不能再写 SHARDS"
out=$(run "CREATE TABLE t_n8 (a int PRIMARY KEY, SHARD KEY a)")
expect_has "SHARD KEY 缺括号是语法错" "$out" "syntax error"
left=$(q "SELECT count(*) FROM pg_class WHERE relname ~ '^t_n[0-9]$' AND relnamespace = 'public'::regnamespace")
expect_eq "校验失败的都没留下表" "$left" "0"
# 不写 SHARD KEY 的表，以及列名 shard / key 的表：行为与原生一致
out=$(run "CREATE TABLE t_plain (shard int, key int, PRIMARY KEY (shard))")
expect_eq "列名 shard / key 的普通表照常建" "$(q "SELECT to_regclass('t_plain') IS NOT NULL")" "t"
expect_eq "普通表不被分布" "$(q "SELECT count(*) FROM pg_dist_partition WHERE logicalrelid = 't_plain'::regclass")" "0"
run "DROP TABLE t_plain" >/dev/null

# ---------------------------------------------------------------------------
step 2 "CREATE TABLE ... SHARD KEY：建表 + 分布 + 建组 + 打标一条语句"
t0=$(date +%s)
out=$(runf <<'SQL'
CREATE TABLE customer (
    c_w_id    int           NOT NULL,
    c_d_id    int           NOT NULL,
    c_id      int           NOT NULL,
    c_first   varchar(16)   NOT NULL,
    c_last    varchar(16)   NOT NULL,
    c_balance numeric(12,2) NOT NULL DEFAULT 0,
    c_since   timestamp     NOT NULL DEFAULT now(),
    PRIMARY KEY (c_w_id, c_d_id, c_id),
    SHARD KEY (c_w_id) SHARDS 4 REPLICAS 2
);
SQL
)
printf '%s\n' "$out" | sed 's/^/        > /'
expect_has "customer 分布" "$out" "分布：按 c_w_id 哈希切成 4 个分片"
expect_has "customer 建组" "$out" "建组：每个分片 1 主 2 从"
expect_has "customer 打标" "$out" "打标：4 个分片已接入分布式事务"
expect_hasnt "customer 无报错" "$out" "ERROR"
expect_hasnt "customer 无内部噪音 NOTICE" "$out" "raft_replicate_table_shards_bulk"
out=$(runf <<'SQL'
CREATE TABLE orders (
    o_w_id    int       NOT NULL,
    o_d_id    int       NOT NULL,
    o_id      int       NOT NULL,
    o_c_id    int       NOT NULL,
    o_ol_cnt  int       NOT NULL,
    o_entry_d timestamp NOT NULL DEFAULT now(),
    PRIMARY KEY (o_w_id, o_d_id, o_id),
    SHARD KEY (o_w_id) COLOCATE WITH customer
);
SQL
)
printf '%s\n' "$out" | sed 's/^/        > /'
expect_has "orders 分布并共置" "$out" "分布：按 o_w_id 哈希切成 4 个分片，与 customer 共置"
expect_has "orders 打标" "$out" "打标：4 个分片已接入分布式事务"
expect_hasnt "orders 无报错" "$out" "ERROR"
printf '        （两张表建表用时 %s s）\n' "$(( $(date +%s) - t0 ))"

expect_eq "两张表都是哈希分布" "$(q "SELECT string_agg(logicalrelid::text || ':' || partmethod::text, ',' ORDER BY 1) FROM pg_dist_partition WHERE logicalrelid IN ('customer'::regclass, 'orders'::regclass)")" "customer:h,orders:h"
expect_eq "分布键" "$(q "SELECT string_agg(column_to_column_name(logicalrelid, partkey), ',' ORDER BY logicalrelid::text) FROM pg_dist_partition WHERE logicalrelid IN ('customer'::regclass, 'orders'::regclass)")" "c_w_id,o_w_id"
expect_eq "共置在同一组" "$(q "SELECT count(DISTINCT colocationid) FROM pg_dist_partition WHERE logicalrelid IN ('customer'::regclass, 'orders'::regclass)")" "1"
expect_eq "Citus 只放一份（replication_factor 1）" "$(q "SELECT count(*) FROM pg_dist_placement pl JOIN pg_dist_shard s USING (shardid) WHERE s.logicalrelid IN ('customer'::regclass, 'orders'::regclass)")" "8"
IDS=$(ids_of "'customer'::regclass, 'orders'::regclass")
NAMES=$(names_of "'customer'::regclass, 'orders'::regclass")
expect_eq "8 个分片都在控制面登记、各 2 个从" "$(q "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN ($IDS) AND cardinality(secondary_nodes) = 2")" "8"
# 每个分片：所有 worker 上恰好 1 leader + 2 follower
roles=""
for p in $WORKERS; do
    roles="$roles$(q "SELECT string_agg(group_id || ':' || state, ' ') FROM partdist.pg_raft_group_status() WHERE group_id IN ($IDS)" "$p") "
done
nl=$(grep -o ':leader' <<<"$roles" | wc -l); nf=$(grep -o ':follower' <<<"$roles" | wc -l)
expect_eq "Raft 组：8 个 leader" "$nl" "8"
expect_eq "Raft 组：16 个 follower" "$nf" "16"
# 打标：在当前 leader 上查 shard_mvcc_status
nreg=0
for sid in ${IDS//,/ }; do
    for p in $WORKERS; do
        st=$(q "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id = $sid ORDER BY (state = 'leader') DESC LIMIT 1" "$p")
        if [[ "$st" == leader ]]; then
            r=$(q "SELECT partdist.shard_mvcc_status(partdist.local_partition_for_shard($sid))" "$p")
            grep -q "registered=yes" <<<"$r" && nreg=$((nreg + 1))
        fi
    done
done
expect_eq "8 个分片在 leader 上都已打标" "$nreg" "8"

# ---------------------------------------------------------------------------
step 3 "写数据（全局事务）并读回"
out=$(runf <<'SQL'
BEGIN;
SELECT partdist.partdist_gxid_next() AS gxid, partdist.partdist_tso_client_start_ts() AS start_ts \gset
SET LOCAL citus.propagate_set_commands = 'local';
\set join_info :gxid,:start_ts,0
SET LOCAL pg_partdist.join_info = :'join_info';
INSERT INTO customer (c_w_id, c_d_id, c_id, c_first, c_last)
SELECT w, d, c, 'first' || c, 'last' || c FROM generate_series(1, 4) w, generate_series(1, 2) d, generate_series(1, 3) c;
INSERT INTO orders (o_w_id, o_d_id, o_id, o_c_id, o_ol_cnt)
SELECT w, d, c, c, 5 FROM generate_series(1, 4) w, generate_series(1, 2) d, generate_series(1, 3) c;
COMMIT;
SQL
)
expect_hasnt "灌数无报错" "$out" "ERROR"
sleep 5
for i in 1 2 3 4 5 6; do
    nc=$(q "SELECT count(*) FROM customer"); no=$(q "SELECT count(*) FROM orders")
    [[ "$nc|$no" == "24|24" ]] && break
    q "SELECT count(*) FROM run_command_on_workers('SELECT partdist.dtx_pending_sweep()')" >/dev/null; sleep 5
done
expect_eq "customer 读回 24 行" "$nc" "24"
expect_eq "orders 读回 24 行" "$no" "24"

# ---------------------------------------------------------------------------
step 3b "shardpg.global_txn()：一个事务写多个分片"
out=$(run "SELECT shardpg.global_txn()")
expect_has "不在事务块里调用会报错" "$out" "请先 BEGIN，再在事务里调用 shardpg.global_txn()"
out=$(runf <<'SQL'
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = 100;
COMMIT;
SQL
)
expect_has "加入全局事务" "$out" "已加入全局事务"
expect_hasnt "跨 4 个分片的 UPDATE 无报错" "$out" "ERROR"
expect_eq "4 个分片都已更新（24 × 100）" "$(q "SELECT sum(c_balance) FROM customer")" "2400.00"
out=$(runf <<'SQL'
BEGIN;
SELECT shardpg.global_txn();
UPDATE customer SET c_balance = c_balance - 10 WHERE c_w_id = 1;
UPDATE customer SET c_balance = c_balance + 10 WHERE c_w_id = 2;
ROLLBACK;
SQL
)
expect_hasnt "跨分片事务回滚无报错" "$out" "ERROR"
expect_eq "回滚后两个分片都恢复" "$(q "SELECT string_agg(c_w_id || ':' || sum, ',' ORDER BY c_w_id) FROM (SELECT c_w_id, sum(c_balance) FROM customer WHERE c_w_id IN (1, 2) GROUP BY 1) x")" "1:600.00,2:600.00"

# ---------------------------------------------------------------------------
step 4 "shardpg.shards 视图"
expect_eq "8 行" "$(q "SELECT count(*) FROM shardpg.shards WHERE table_name IN ('customer'::regclass, 'orders'::regclass)")" "8"
expect_eq "主都有值、从都是 2 个" "$(q "SELECT count(*) FROM shardpg.shards WHERE table_name IN ('customer'::regclass, 'orders'::regclass) AND \"primary\" IS NOT NULL AND cardinality(string_to_array(replicas, ',')) = 2")" "8"
expect_eq "主与控制面登记一致" "$(q "SELECT count(*) FROM shardpg.shards v JOIN pg_dist_shard s ON shard_name(s.logicalrelid, s.shardid) = v.shard_name JOIN partdist.partition_map m ON m.partition_id::bigint = s.shardid JOIN partdist.node_map nm ON nm.node_id = m.primary_node WHERE v.\"primary\" = ':' || nm.port AND s.logicalrelid IN ('customer'::regclass, 'orders'::regclass)")" "8"
expect_eq "分片键列" "$(q "SELECT string_agg(DISTINCT shard_key, ',') FROM shardpg.shards WHERE table_name = 'customer'::regclass")" "c_w_id"
run "SELECT * FROM shardpg.shards WHERE table_name IN ('customer'::regclass, 'orders'::regclass)" | sed 's/^/        > /'

# ---------------------------------------------------------------------------
step 5 "psql \\dt 显示子表"
out=$(run "\\dt customer")
printf '%s\n' "$out" | sed 's/^/        > /'
expect_has "表头有 Hash range" "$out" "Hash range"
expect_has "父表汇总" "$out" "shard key: c_w_id, 4 shards, 2 replicas, colocated with orders"
expect_eq "4 个子表行" "$(grep -cE '(\+--|`--) customer_[0-9]+ ' <<<"$out")" "4"
expect_eq '最后一个子表以 `-- 开头' "$(grep -c -- '`-- customer_' <<<"$out")" "1"
for sid in ${IDS//,/ }; do
    nm=$(q "SELECT shard_name(logicalrelid, shardid) FROM pg_dist_shard WHERE shardid = $sid")
    [[ "$nm" == customer_* ]] || continue
    pp=$(q "SELECT ':' || nm.port FROM partdist.partition_map m JOIN partdist.node_map nm ON nm.node_id = m.primary_node WHERE m.partition_id::bigint = $sid")
    grep -E "$nm +\|" <<<"$out" | grep -qF "| $pp " && ok "$nm 的主 $pp 与登记一致" || bad "$nm 的主应为 $pp" "$(grep "$nm" <<<"$out")"
done
out=$(run "\\dt")
expect_has "不带模式：有汇总" "$out" "shard key: o_w_id, 4 shards, 2 replicas, colocated with customer"
expect_has "不带模式：也展开子表" "$out" "+-- customer_"
out=$(run "\\dt customer*")
expect_eq "\\dt customer* 展开 4 个子表" "$(grep -cE '(\+--|`--) customer_[0-9]+ ' <<<"$out")" "4"
# 安静模式（-q）下原版 psql 找不到时只印空表、不报错，提示也随之不出现；这一项不加 -q
out=$(docker exec -u postgres "$C" bash -c "exec $BIN/psql -h /tmp -p 5432 -U postgres -d postgres -X -c \"\\\\dt customer_1*\" 2>&1" </dev/null)
expect_has "按子表名查：提示属于哪张表" "$out" "是分布式表 customer 的分片（子表），用 \\dt customer 查看。"
out=$(run "\\dt+ customer")
printf '%s\n' "$out" | sed 's/^/        > /'
expect_has "\\dt+ 有 Rows 列" "$out" "Rows"
expect_eq "\\dt+ 父表 Rows = 24" "$(grep -E '^ public \| customer ' <<<"$out" | awk -F'|' '{gsub(/ /,"",$8); print $8}')" "24"
run "CREATE TABLE t_local (id int PRIMARY KEY)" >/dev/null
out=$(run "\\dt t_local")
expect_hasnt "本地表：输出与原版一致（无 Hash range）" "$out" "Hash range"
run "DROP TABLE t_local" >/dev/null

# ---------------------------------------------------------------------------
step 6 "shardpg.shard_count / shardpg.replicas 默认值"
out=$(runf <<'SQL'
SET shardpg.shard_count = 2;
SET shardpg.replicas = 1;
CREATE TABLE t_gucs (id int PRIMARY KEY, v text, SHARD KEY (id));
SQL
)
expect_has "按参数切 2 片" "$out" "分布：按 id 哈希切成 2 个分片"
expect_has "按参数 1 主 1 从" "$out" "建组：每个分片 1 主 1 从"
GIDS=$(ids_of "'t_gucs'::regclass"); GNAMES=$(names_of "'t_gucs'::regclass")
expect_eq "t_gucs 2 个分片、各 1 个从" "$(q "SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint IN ($GIDS) AND cardinality(secondary_nodes) = 1")" "2"

# ---------------------------------------------------------------------------
step 7 "DROP TABLE：自动拆组、删副本表、删登记"
before=$(residue "$IDS" "$NAMES")
printf '        删前：%s\n' "$before"
out=$(run "DROP TABLE orders, customer")
printf '%s\n' "$out" | sed 's/^/        > /'
expect_has "拆组" "$out" "清理：已拆除 8 个分片的 Raft 组"
expect_has "删副本表" "$out" "清理：已删除各节点上的副本表"
expect_has "删登记" "$out" "清理：已删除主从登记，回收残留"
expect_hasnt "无告警" "$out" "WARNING"
expect_eq "删后各节点无残留" "$(residue "$IDS" "$NAMES")" "$(clean_expect)"
out=$(run "DROP TABLE t_gucs")
expect_eq "t_gucs 删后无残留" "$(residue "$GIDS" "$GNAMES")" "$(clean_expect)"

# ---------------------------------------------------------------------------
step 8 "事务块里 DROP：只记队列，提交后 shardpg.cleanup_dropped() 清"
run "CREATE TABLE t_tx (id int PRIMARY KEY, SHARD KEY (id) SHARDS 2 REPLICAS 2)" >/dev/null
TIDS=$(ids_of "'t_tx'::regclass"); TNAMES=$(names_of "'t_tx'::regclass")
out=$(runf <<'SQL'
BEGIN;
DROP TABLE t_tx;
COMMIT;
SQL
)
expect_has "提示提交后清理" "$out" "SELECT shardpg.cleanup_dropped();"
expect_eq "队列里 2 个分片" "$(q "SELECT count(*) FROM partdist.shardpg_drop_queue WHERE shardid IN ($TIDS)")" "2"
ng=0; for p in $WORKERS; do ng=$((ng + $(q "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN ($TIDS)" "$p"))); done
expect_eq "清理前 Raft 组还在（2 片 × 3 成员）" "$ng" "6"
out=$(q "SELECT shardpg.cleanup_dropped()")
expect_has "cleanup_dropped 返回" "$out" "已清理 2 个分片"
expect_eq "清理后无残留" "$(residue "$TIDS" "$TNAMES")" "$(clean_expect)"
expect_eq "再执行一次是空操作" "$(q "SELECT shardpg.cleanup_dropped()" | tail -1)" "没有待清理的分片"
# DROP 回滚：队列一起回滚，组不拆
run "CREATE TABLE t_tx (id int PRIMARY KEY, SHARD KEY (id) SHARDS 2 REPLICAS 2)" >/dev/null
TIDS=$(ids_of "'t_tx'::regclass"); TNAMES=$(names_of "'t_tx'::regclass")
runf >/dev/null <<'SQL'
BEGIN;
DROP TABLE t_tx;
ROLLBACK;
SQL
expect_eq "DROP 回滚后表还在" "$(q "SELECT to_regclass('t_tx') IS NOT NULL")" "t"
expect_eq "DROP 回滚后队列为空" "$(q "SELECT count(*) FROM partdist.shardpg_drop_queue WHERE shardid IN ($TIDS)")" "0"
ng=0; for p in $WORKERS; do ng=$((ng + $(q "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id IN ($TIDS)" "$p"))); done
expect_eq "DROP 回滚后 Raft 组都在" "$ng" "6"
expect_eq "清理函数不碰活表" "$(q "SELECT shardpg.cleanup_dropped()" | tail -1)" "没有待清理的分片"
run "DROP TABLE t_tx" >/dev/null
expect_eq "t_tx 删后无残留" "$(residue "$TIDS" "$TNAMES")" "$(clean_expect)"

# ---------------------------------------------------------------------------
step 9 "与 ShardPG 无关的 DROP 照常（没有清理提示）"
run "CREATE TABLE t_citus (id int PRIMARY KEY)" >/dev/null
q "SELECT create_distributed_table('t_citus', 'id', shard_count => 2, colocate_with => 'none')" >/dev/null
out=$(run "DROP TABLE t_citus")
expect_hasnt "普通 Citus 分布表 DROP 不走清理" "$out" "清理"
expect_eq "t_citus 已删" "$(q "SELECT to_regclass('t_citus') IS NULL")" "t"

# ---------------------------------------------------------------------------
step 10 "健康：无残留 prepared、无崩溃"
np=0; for p in 5432 $WORKERS; do np=$((np + $(q "SELECT count(*) FROM pg_prepared_xacts" "$p"))); done
expect_eq "各节点无 prepared 事务" "$np" "0"
nc=$(docker exec "$C" bash -c "cat /work/pg-cluster-data/*/pg.log 2>/dev/null | grep -c 'terminated by signal'")
expect_eq "测试期间无进程崩溃（terminated by signal 条数不变）" "$nc" "$CRASH0"

# ---------------------------------------------------------------------------
TOTAL=$((PASS + FAIL))
printf '\n==== 合计 PASS=%d FAIL=%d（共 %d 项）====\n' "$PASS" "$FAIL" "$TOTAL"
[[ $TOTAL -gt 0 && $FAIL -eq 0 ]]
