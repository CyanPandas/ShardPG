#!/usr/bin/env bash
# [宿主机] TPC-C 分支：SI "提交中窗口" 回归（shard_visibility.c 的 committing 标记 +
# ShardSnapshotEagerStart）。
#
# 缺陷原形（原版实测，1 行热点、4 个并发更新者、3 个读者，20 s 内 2–4 次）：
#   持有者 T 在 PRE_COMMIT 取到 commit_ts c，判决尚未写进 clog；读者此刻拿到
#   start_ts s > c，读旧版本时 T 判 RUNNING ⇒ 旧版本可见，稍后读新版本时 T 已落定
#   且 c < s ⇒ 新版本也可见 —— 一条语句里同一行出现两次（反向时序则一行都没有）。
#   test_tso_si_p3 的竞态注入只用"只增"负载，碰不到更新链上的这一格。
# 断言：读者每次读到的行数恰为 1；异常明细落一张**未打标**的日志表。
#   [1] 纯 UPDATE 更新者；[2] SELECT ... FOR UPDATE + UPDATE 更新者（补丁 0011 路径）。
#
# 前置：目标节点已配 TSO（pg_partdist.tso_conninfo 非空、能取到 start_ts）。
# 用法：CONTAINER=pg-test-container WPORT=5433 bash test_si_committing_window_tpcc.sh
#       PG_BIN 可覆盖（默认 /work/pg-install/bin），DUR 每段秒数（默认 30）。
set -u
CONTAINER="${CONTAINER:-pg-test-container}"
WPORT="${WPORT:-5433}"
PG_BIN="${PG_BIN:-/work/pg-install/bin}"
DUR="${DUR:-30}"
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { DEX "$PG_BIN/psql" -h /tmp -p "$WPORT" -U postgres -d postgres -X "$@"; }
check() {
  if [[ -z "$2" ]]; then echo "  FAIL  $1（取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

echo "========== [0] 前置 =========="
ts=$(PSQL -Atc "SELECT partdist.partdist_tso_client_start_ts()" </dev/null 2>&1)
check "节点 :$WPORT 已配 TSO（能取到 start_ts）" "$([[ "$ts" =~ ^[0-9]+$ ]] && echo ok)" "ok"
[[ "$ts" =~ ^[0-9]+$ ]] || { echo "== 合计 PASS=$PASS FAIL=$FAIL"; exit 1; }

TBL="sicw_$(date +%H%M%S)"
OLD_RELIDS=$(PSQL -Atc "SHOW pg_partdist.shard_relids" </dev/null)
restore() {
  PSQL -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OLD_RELIDS}'" </dev/null >/dev/null 2>&1
  PSQL -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null 2>&1
  PSQL -q -c "SET citus.enable_ddl_propagation TO off; DROP TABLE IF EXISTS ${TBL}, sicw_anom" </dev/null >/dev/null 2>&1
  DEX rm -rf "/tmp/${TBL}" </dev/null
}
trap restore EXIT

PSQL -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.enable_ddl_propagation TO off;
CREATE TABLE ${TBL}(id int primary key, v int) WITH (fillfactor = 10, autovacuum_enabled = off);
DROP TABLE IF EXISTS sicw_anom;
CREATE TABLE sicw_anom(kind text, n int, detail text);
CREATE OR REPLACE FUNCTION sicw_chk(kind text) RETURNS int LANGUAGE plpgsql AS \$\$
DECLARE n int; d text;
BEGIN
  IF kind = 'idx' THEN
    SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off;
    SELECT count(*), string_agg(ctid::text || ' x' || xmin || ' m' || xmax, ' ') INTO n, d
      FROM ${TBL} WHERE id = 1;
  ELSE
    SELECT count(*), string_agg(ctid::text || ' x' || xmin || ' m' || xmax, ' ') INTO n, d
      FROM (SELECT ctid, xmin, xmax FROM ${TBL}) s;
  END IF;
  IF n <> 1 THEN INSERT INTO sicw_anom VALUES (kind, n, d); END IF;
  RETURN n;
END \$\$;
SQL
OID=$(PSQL -Atc "SELECT oid FROM pg_class WHERE relname='${TBL}'" </dev/null)
PSQL -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${OLD_RELIDS:+${OLD_RELIDS},}${OID}'" </dev/null >/dev/null
PSQL -q -c "SELECT pg_reload_conf()" </dev/null >/dev/null
sleep 2
PSQL -q -c "INSERT INTO ${TBL} VALUES (1, 0)" </dev/null >/dev/null
check "夹具已打标（xmin 是分片 xid）" \
  "$(PSQL -Atc "SELECT partdist.shard_mvcc_status(${OID})" </dev/null | grep -c 'registered=yes')" "1"

DEX bash -c "mkdir -p /tmp/${TBL} && cd /tmp/${TBL} &&
  echo 'UPDATE ${TBL} SET v = v + 1 WHERE id = 1;' > upd.sql &&
  printf 'BEGIN;\nSELECT v FROM ${TBL} WHERE id = 1 FOR UPDATE;\nUPDATE ${TBL} SET v = v + 1 WHERE id = 1;\nCOMMIT;\n' > lockupd.sql &&
  echo \"SELECT sicw_chk('idx');\" > idx.sql && echo \"SELECT sicw_chk('seq');\" > seq.sql" </dev/null

run_case() {  # run_case <标签> <更新者脚本>
  local tag=$1 upd=$2 n0 n1 reads
  PSQL -q -c "TRUNCATE sicw_anom" </dev/null >/dev/null
  n0=$(PSQL -Atc "SELECT v FROM ${TBL} WHERE id = 1" </dev/null)
  DEX bash -c "cd /tmp/${TBL} &&
    ($PG_BIN/pgbench -h /tmp -p $WPORT -U postgres -n -c 4 -j 4 -T $DUR --max-tries=1000 -f $upd postgres > u.out 2>&1 &) ;
    $PG_BIN/pgbench -h /tmp -p $WPORT -U postgres -n -c 3 -j 3 -T $DUR -f idx.sql -f seq.sql postgres > r.out 2>&1;
    sleep 2" </dev/null
  reads=$(DEX bash -c "sed -n 's/^number of transactions actually processed: \([0-9]*\).*/\1/p' /tmp/${TBL}/r.out" </dev/null)
  n1=$(PSQL -Atc "SELECT v FROM ${TBL} WHERE id = 1" </dev/null)
  check "[$tag] 更新者确实推进了（v ${n0} → ${n1}）" "$([[ -n "$n1" && "$n1" -gt "$n0" ]] && echo ok)" "ok"
  check "[$tag] 读者确实跑了（${reads:-0} 次读 > 1000）" "$([[ "${reads:-0}" -gt 1000 ]] && echo ok)" "ok"
  check "[$tag] 每次读都恰好 1 行（异常数）" "$(PSQL -Atc "SELECT count(*) FROM sicw_anom" </dev/null)" "0"
  PSQL -Atc "SELECT kind || ' n=' || n || ' ' || left(detail, 160) FROM sicw_anom LIMIT 3" </dev/null | sed 's/^/        /'
}

echo "========== [1] 纯 UPDATE 更新者 =========="
run_case update upd.sql
echo "========== [2] FOR UPDATE + UPDATE 更新者（补丁 0011） =========="
run_case lockupd lockupd.sql

echo "== 合计 PASS=$PASS FAIL=$FAIL"
[[ $FAIL -eq 0 ]]
