#!/usr/bin/env bash
# [宿主机] T7.7 验收：分配器 shmem 槽位在 DROP 提交时归还（R-P6-4）。
#
# 缺陷：`SHARD_XID_MAX_SLOTS = 64`/节点是定长 shmem，而全仓**没有任何释放路径**
# （grep 零命中）—— 建删 64 张打标表之后，该节点再也建不出第 65 张，报
# 「分片 xid 槽位用尽（上限 64）」。门禁一直靠"移走水位文件 + 重启"绕过去
# （`run_p6_exit.sh` 直接 mv），等于把这条缺陷藏在净场脚本里。
#
# 判据（单节点，不需要 raft/2PC）：一次性建 70 张打标表，逐张"写一行 → DROP"。
# 每次写入都会挂一个槽位；只有归还生效，第 65 张往后才写得进去。
# 修复前：第 65 张必然 ERROR「分片 xid 槽位用尽」。
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
CONTAINER="${CONTAINER:-pg-test-container}"
PORT="${PORT:-5433}"
N=70
PASS=0; FAIL=0; NCHECK=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { DEX /work/pg-install/bin/psql -h /tmp -p "$PORT" -U postgres -d postgres -X "$@"; }
check() {
  NCHECK=$((NCHECK+1))
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

exec 9>/tmp/t77_slot.lock
if ! flock -n 9; then echo "FATAL: 另一个 T7.7 验收正在运行"; exit 99; fi

cleanup() {
  local i
  PSQL -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null 2>&1
  PSQL -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  for i in $(seq 1 $N); do
    PSQL -q -c "DROP TABLE IF EXISTS t77_$i;" </dev/null >/dev/null 2>&1
  done
}
trap cleanup EXIT

echo "========== [0] 前置：撤残留白名单 =========="
PSQL -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null 2>&1
PSQL -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
sleep 1
up=$(PSQL -Atc "SELECT 1" </dev/null 2>&1 | tail -1)
check "节点 :$PORT 可连" "$up" "1"

echo "========== [1] 建 $N 张本地表并一次性打标 =========="
for i in $(seq 1 $N); do
  PSQL -q -c "DROP TABLE IF EXISTS t77_$i; CREATE TABLE t77_$i(id int, v text) WITH (autovacuum_enabled=off);" </dev/null >/dev/null 2>&1
done
oids=$(PSQL -Atc "SELECT string_agg(oid::text, ',' ORDER BY relname) FROM pg_class WHERE relname LIKE 't77\_%' AND relkind='r'" </dev/null)
nold=$(awk -F, '{print NF}' <<< "$oids")
check "$N 张表就绪（白名单里 $nold 个 oid）" "$nold" "$N"
PSQL -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${oids}';" </dev/null >/dev/null
PSQL -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 2
guc=$(PSQL -Atc "SHOW pg_partdist.shard_relids" </dev/null | head -c 20)
check "白名单已生效（前 20 字符：$guc…）" "$([[ -n "$guc" ]] && echo ok)" "ok"

echo "========== [2] 逐张「写一行 → DROP」共 $N 轮 =========="
firstfail=0; lastok=0; errtext=""
for i in $(seq 1 $N); do
  out=$(PSQL -Atc "INSERT INTO t77_$i VALUES ($i,'x'); SELECT 'w'" </dev/null 2>&1 | tail -1)
  if [[ "$out" != "w" ]]; then
    [[ "$firstfail" == "0" ]] && { firstfail=$i; errtext=$(PSQL -Atc "INSERT INTO t77_$i VALUES ($i,'x')" </dev/null 2>&1 | head -2 | tr '\n' ' '); }
    break
  fi
  lastok=$i
  PSQL -q -c "DROP TABLE t77_$i;" </dev/null >/dev/null 2>&1
done
check "★ $N 轮「写入 + DROP」全部成功（最后成功第 $lastok 轮；修复前第 65 轮起报槽位用尽）" \
      "$lastok" "$N"
[[ "$firstfail" != "0" ]] && echo "        第 $firstfail 轮失败：$errtext"

echo "========== [3] 槽位确实回到可用状态：再来一轮 =========="
PSQL -q -c "DROP TABLE IF EXISTS t77_extra; CREATE TABLE t77_extra(id int) WITH (autovacuum_enabled=off);" </dev/null >/dev/null 2>&1
xoid=$(PSQL -Atc "SELECT 't77_extra'::regclass::oid" </dev/null)
PSQL -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${xoid}';" </dev/null >/dev/null
PSQL -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
sleep 2
x=$(PSQL -Atc "INSERT INTO t77_extra VALUES (1); SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "全部归还后仍能新建打标表并写入" "$x" "ok"
PSQL -q -c "DROP TABLE IF EXISTS t77_extra;" </dev/null >/dev/null 2>&1

echo
echo "结果：PASS=${PASS} FAIL=${FAIL}"
if [[ "$NCHECK" -lt 5 ]]; then
  echo "FATAL: 只跑了 ${NCHECK} 条断言（应 >=5）——夹具中途退出，结果不可信"; exit 98
fi
exit $FAIL
