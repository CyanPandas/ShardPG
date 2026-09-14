#!/usr/bin/env bash
# [宿主机] P7-D3 验收：含分片打标表 DROP 的事务可以 PREPARE，文件回收随 COMMIT PREPARED 结算。
#
# 缺陷：PRE_PREPARE 里一刀切地禁止"含分片打标表 DROP 的事务"执行 PREPARE —— 理由是 DROP
# 之后的文件回收（分片 clog 目录、水位文件、打标集合、分配器槽位）记在**本进程内存**里，
# COMMIT PREPARED 在别的会话执行，结算不了。可 Citus 把分布表的 DROP 以 2PC 下发到每个
# placement，于是**任何打标分布表都无法从协调者删除**。运维只能去 worker 上本地删分片表，
# 那条路正是 P7-D3"半删分片把分布表永久锁死在协调者上"的起点。
#
# 修法：回收清单在 at_prepare 写进 2PC 记录（info=SHARD_2PC_INFO_DROPS），COMMIT PREPARED
# 回调据此回收；ROLLBACK PREPARED 什么都不做；prepared 事务跨重启存活，回收照样结算。
#
#   [A] 机制（单 worker 本地打标表，不经 Citus）：
#       A1 PREPARE → ROLLBACK PREPARED ⇒ 表与文件原样
#       A2 PREPARE（会话结束）→ 另一会话 COMMIT PREPARED ⇒ 跨会话回收完成
#       A3 PREPARE → 重启节点 → COMMIT PREPARED ⇒ 2PC 记录挺过重启，回收完成
#   [B] 真实场景：协调者上 DROP 一张 2 分片的打标分布表 ⇒ 成功；两个 placement 回收干净；
#       ROLLBACK 对照组不回收；同名重建成功（不再被锁死）
# 修复前 A1/A2/A3 的 PREPARE 与 B 的 DROP 均报「不支持对含分片打标表 DROP 的事务执行
# PREPARE TRANSACTION」。
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
W=${W:-5439}
WDIR="worker$((W - 5432))"
TS=$(date +%H%M%S)
PASS=0; FAIL=0
DEX()  { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
PSQLO() { local port=$1 opts=$2; shift 2
  docker exec -i -u postgres -e HOME=/var/lib/postgresql -e PGOPTIONS="$opts" \
    "$CONTAINER" /work/pg-install/bin/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
NOPROP='-c citus.enable_ddl_propagation=off'
check() {
  if [[ -z "${2:-}" ]]; then echo "  FAIL  $1（实际取不到值）"; FAIL=$((FAIL+1)); return; fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}
exec 9>/tmp/p7_drop_mvcc_2pc.lock
if ! flock -n 9; then echo "FATAL: 另一个 D3 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

datadir() { PSQL "$1" -Atc "SHOW data_directory" </dev/null | tail -1; }
has() { DEX test -e "$1" </dev/null && echo yes || echo no; }
set_whitelist() {  # <port> <oid 列表，逗号分隔；空 = RESET>
  if [[ -z "$2" ]]; then PSQL "$1" -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
  else PSQL "$1" -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '$2';" </dev/null >/dev/null; fi
  PSQL "$1" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
  local t g=""; for t in $(seq 1 10); do g=$(PSQL "$1" -Atc "SHOW pg_partdist.shard_relids" </dev/null); [[ "$g" == "$2" ]] && break; sleep 1; done
  [[ "$g" == "$2" ]]
}

# 在 :W 上建一张本地打标表并写几行（打标后写的行才带分片 xid）。回显 OID。
mk_local() {  # <表名>
  PSQLO $W "$NOPROP" -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
DROP TABLE IF EXISTS $1;
CREATE TABLE $1(id int, v text) WITH (autovacuum_enabled=off);
SQL
  PSQLO $W "$NOPROP" -Atc "SELECT '$1'::regclass::oid" </dev/null | tail -1
}
write_rows() {  # <port> <表名>
  PSQLO "$1" "$NOPROP" -q -c "INSERT INTO $2 SELECT g, 'v'||g FROM generate_series(1,5) g;" </dev/null >/dev/null
}

echo "================ [0] 前置 ================"
WD=$(datadir $W)
check "worker :$W 在线（$WD）" "$([[ "$WD" == /* ]] && echo ok || echo no)" "ok"
mpx=$(PSQL $W -Atc "SHOW max_prepared_transactions" </dev/null | tail -1)
check "max_prepared_transactions > 0（$mpx）" "$([[ "$mpx" =~ ^[0-9]+$ && "$mpx" -gt 0 ]] && echo ok || echo no)" "ok"

echo "================ [A] 机制：单 worker 本地打标表 ================"
A1="t7d3a1_$TS"; A2="t7d3a2_$TS"; A3="t7d3a3_$TS"
O1=$(mk_local $A1); O2=$(mk_local $A2); O3=$(mk_local $A3)
check "三张本地表就位（$O1 $O2 $O3）" "$([[ "$O1$O2$O3" =~ ^[0-9]+$ ]] && echo ok || echo no)" "ok"
set_whitelist $W "$O1,$O2,$O3" ; check "三张表进白名单（打标生效）" "$?" "0"
for t in $A1 $A2 $A3; do write_rows $W $t; done
xm=$(PSQLO $W "$NOPROP" -Atc "SELECT max(xmin::text::bigint) FROM $A1" </dev/null | tail -1)
sx=$(PSQL $W -Atc "SELECT partdist.shard_xid_next(${O1})" </dev/null | tail -1)
check "打标前提：行 xmin 是分片 xid（xmin=${xm} < 下一号 ${sx}）" \
      "$([[ "$xm" =~ ^[0-9]+$ && "$sx" =~ ^[0-9]+$ && "$xm" -lt "$sx" && "$sx" -lt 100000 ]] && echo ok || echo no)" "ok"
for o in $O1 $O2 $O3; do
  check "  oid $o 的分片 clog 目录与水位文件都在" "$(has $WD/pg_shard_clog/$o)$(has $WD/pg_shard_xid/$o)" "yesyes"
done

prep_drop() {  # <表名> <gid>  ——  DROP + PREPARE，回显 PREPARE 的结果（成功为 PREPARE TRANSACTION）
  PSQLO $W "$NOPROP" -Atc "BEGIN; DROP TABLE $1; PREPARE TRANSACTION '$2';" </dev/null 2>&1 | tail -1
}

echo "---- A1：PREPARE → ROLLBACK PREPARED ⇒ 什么都不回收 ----"
r=$(prep_drop $A1 "t7d3_a1_$TS")
check "★ 含打标表 DROP 的事务 PREPARE 成功（修复前报「不支持…PREPARE TRANSACTION」）" "$r" "PREPARE TRANSACTION"
PSQL $W -q -c "ROLLBACK PREPARED 't7d3_a1_$TS';" </dev/null >/dev/null 2>&1
check "  ROLLBACK PREPARED 后表仍在" "$(PSQLO $W "$NOPROP" -Atc "SELECT count(*) FROM $A1" </dev/null | tail -1)" "5"
check "  ROLLBACK PREPARED 后 clog 目录/水位文件原样" "$(has $WD/pg_shard_clog/$O1)$(has $WD/pg_shard_xid/$O1)" "yesyes"

echo "---- A2：PREPARE（会话结束）→ 另一会话 COMMIT PREPARED ⇒ 跨会话回收 ----"
r=$(prep_drop $A2 "t7d3_a2_$TS")
check "★ PREPARE 成功" "$r" "PREPARE TRANSACTION"
check "  PREPARE 之后、提交之前：文件还在（回收必须等提交）" "$(has $WD/pg_shard_clog/$O2)$(has $WD/pg_shard_xid/$O2)" "yesyes"
c=$(PSQL $W -Atc "COMMIT PREPARED 't7d3_a2_$TS';" </dev/null 2>&1 | tail -1)
check "  另一会话 COMMIT PREPARED" "$c" "COMMIT PREPARED"
check "★★ 跨会话回收完成：clog 目录与水位文件都已删除" "$(has $WD/pg_shard_clog/$O2)$(has $WD/pg_shard_xid/$O2)" "nono"
check "  表已不存在" "$(PSQLO $W "$NOPROP" -Atc "SELECT count(*) FROM pg_class WHERE oid=$O2" </dev/null | tail -1)" "0"

echo "---- A3：PREPARE → 重启节点 → COMMIT PREPARED ⇒ 2PC 记录挺过重启 ----"
r=$(prep_drop $A3 "t7d3_a3_$TS")
check "★ PREPARE 成功" "$r" "PREPARE TRANSACTION"
DEX /work/pg-install/bin/pg_ctl -D "$WD" -m fast -l "$WD/pg.log" restart -w -t 60 </dev/null >/dev/null 2>&1
up=""; for t in $(seq 1 30); do up=$(PSQL $W -Atc "SELECT 1" </dev/null 2>/dev/null); [[ "$up" == "1" ]] && break; sleep 1; done
check "  节点重启完成" "$up" "1"
check "  prepared 事务挺过重启" "$(PSQL $W -Atc "SELECT count(*) FROM pg_prepared_xacts WHERE gid='t7d3_a3_$TS'" </dev/null | tail -1)" "1"
c=$(PSQL $W -Atc "COMMIT PREPARED 't7d3_a3_$TS';" </dev/null 2>&1 | tail -1)
check "  重启后 COMMIT PREPARED" "$c" "COMMIT PREPARED"
check "★★ 重启后回收照样完成" "$(has $WD/pg_shard_clog/$O3)$(has $WD/pg_shard_xid/$O3)" "nono"

WLOG=$(health_node_log "$WDIR")
nlog=$(DEX grep -c "COMMIT PREPARED 回收" "$WLOG" </dev/null 2>/dev/null || true)
check "  节点日志留痕（COMMIT PREPARED 回收，≥1 行，实际 ${nlog}）" "$([[ "${nlog:-0}" -ge 1 ]] && echo ok || echo no)" "ok"
PSQLO $W "$NOPROP" -q -c "DROP TABLE IF EXISTS $A1" </dev/null >/dev/null 2>&1
set_whitelist $W ""

echo "================ [B] 真实场景：协调者 DROP 一张 2 分片打标分布表 ================"
TB="t7d3b_$TS"
PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;
CREATE TABLE $TB(id int primary key, v text);
SELECT create_distributed_table('$TB', 'id');
SQL
mapfile -t PL < <(PSQL $COORD -Atc "SELECT s.shardid||' '||n.nodeport FROM pg_dist_shard s JOIN pg_dist_placement p ON p.shardid=s.shardid JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE s.logicalrelid='$TB'::regclass ORDER BY s.shardid" </dev/null)
check "2 个分片 placement 就位" "${#PL[@]}" "2"
declare -A WL=()   # port → oid 列表
declare -a SIDS=() PORTS=() OIDS=() DDS=()
for row in "${PL[@]}"; do
  sid=${row% *}; port=${row#* }
  oid=$(PSQLO $port "$NOPROP -c citus.override_table_visibility=false" -Atc "SELECT '${TB}_${sid}'::regclass::oid" </dev/null | tail -1)
  SIDS+=("$sid"); PORTS+=("$port"); OIDS+=("$oid"); DDS+=("$(datadir $port)")
  WL[$port]="${WL[$port]:+${WL[$port]},}$oid"
done
for port in "${!WL[@]}"; do set_whitelist $port "${WL[$port]}"; done
for i in 0 1; do
  write_rows ${PORTS[$i]} "${TB}_${SIDS[$i]}"
  check "  分片 ${SIDS[$i]}@:${PORTS[$i]} 打标写入后 clog/水位就位" \
        "$(has ${DDS[$i]}/pg_shard_clog/${OIDS[$i]})$(has ${DDS[$i]}/pg_shard_xid/${OIDS[$i]})" "yesyes"
done

echo "---- B1 对照：BEGIN; DROP; ROLLBACK ⇒ 不回收 ----"
PSQL $COORD -q -c "BEGIN; DROP TABLE $TB; ROLLBACK;" </dev/null >/dev/null 2>&1
check "  ROLLBACK 后分布表仍在" "$(PSQL $COORD -Atc "SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::text='$TB'" </dev/null | tail -1)" "1"
for i in 0 1; do
  check "  分片 ${SIDS[$i]} 文件原样" "$(has ${DDS[$i]}/pg_shard_clog/${OIDS[$i]})$(has ${DDS[$i]}/pg_shard_xid/${OIDS[$i]})" "yesyes"
done

echo "---- B2 ★★ DROP TABLE（Citus 2PC 下发）----"
d=$(PSQL $COORD -Atc "DROP TABLE $TB;" </dev/null 2>&1 | tail -1)
echo "  DROP ⇒ $d"
check "★★ 协调者 DROP 打标分布表成功（修复前：不支持…PREPARE TRANSACTION）" "$d" "DROP TABLE"
for i in 0 1; do
  check "★ 分片 ${SIDS[$i]}@:${PORTS[$i]} 回收完成（clog 目录/水位文件已删）" \
        "$(has ${DDS[$i]}/pg_shard_clog/${OIDS[$i]})$(has ${DDS[$i]}/pg_shard_xid/${OIDS[$i]})" "nono"
done
left=0
for port in "${!WL[@]}"; do
  k=$(PSQL $port -Atc "SELECT count(*) FROM pg_prepared_xacts" </dev/null | tail -1); left=$((left + ${k:-1}))
done
check "  placement 上没有残留 prepared 事务" "$left" "0"

echo "---- B3 同名重建（不再被锁死）----"
for port in "${!WL[@]}"; do set_whitelist $port ""; done
rc=$(PSQL $COORD -Atc "SET citus.shard_count = 2; CREATE TABLE $TB(id int primary key, v text); SELECT create_distributed_table('$TB', 'id'); SELECT 'ok'" </dev/null 2>&1 | tail -1)
check "同名重建并分布成功" "$rc" "ok"
PSQL $COORD -q -c "DROP TABLE IF EXISTS $TB;" </dev/null >/dev/null 2>&1

health_check_no_crash
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
