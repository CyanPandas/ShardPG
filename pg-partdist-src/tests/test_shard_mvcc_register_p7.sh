#!/usr/bin/env bash
# [宿主机] P7-V4 验收：分布表打标登记一条命令完成，且打标身份能跟着切主走（哪怕还没写过一行）。
#
# 缺陷（P7-V4）：打标登记只有两条路，对分布表都不可用 ——
#   · partdist_set_shard_mvcc(regclass) 按本地 OID 查 partition_map，而分布式流程写进去的
#     partition_id 是 Citus shardid ⇒ 分布表恒报"没有登记行"；
#   · GUC 白名单 pg_partdist.shard_relids：组内每个成员各自 ALTER SYSTEM，列本节点 OID
#     （同一分片各节点 OID 不同）—— "逐分片逐节点手工"。
# 另有一个藏在它后面的缺口：副本"这是打标表"的持久证据（pg_shard_clog/<oid>）只在回放到
# 带分片 xid 的 MARKER 时才建 ⇒ **刚打标、还没写过就切主**，新主继承不到身份，之后的写入
# 悄悄走原生路径。
#
# 修法：协调者 partdist.set_table_shard_mvcc(regclass) 逐 placement 下发
# partdist.shard_mvcc_register(shardid)；登记时发 CTRL SHARD_MVCC、物理基线末尾也发，
# 副本收到只建证据目录、不进打标集合（避免 vacuum 自动启动器动副本文件）。
#
#   [1] 表 A（2 分片）先供副本            [2] 登记前状态 + 副本上登记被拒
#   [3] ★ 协调者一条命令登记；leader 已登记、副本有证据不登记；重跑幂等
#   [4] leader 写入带分片 xid
#   [5] ★★ 分片 A2 一行未写就杀 leader ⇒ 新主继承身份，写入仍带分片 xid
#   [6] 表 B 非空 ⇒ 拒绝登记            [7] 表 C 先登记后供副本 ⇒ 证据随物理基线到副本
#   [8] 对照：表 D 用旧的白名单方式打标 ⇒ 副本**没有**证据（修复前的缺口本身）
set -u

CONTAINER="${CONTAINER:-pg-test-container}"
COORD=5432
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
exec 9>/tmp/p7_shard_mvcc_register.lock
if ! flock -n 9; then echo "FATAL: 另一个 V4 验收正在运行"; exit 99; fi
source "$(dirname "$0")/lib_node_health.sh"; health_mark_start

LOID()  { PSQL "$1" -Atc "SELECT partdist.local_partition_for_shard($2)" </dev/null | tail -1; }
MSTAT() { PSQL "$1" -Atc "SELECT partdist.shard_mvcc_status($2)" </dev/null 2>&1 | tail -1; }
FLUSH() { PSQL "$1" -Atc "SELECT partdist.get_partition_flush_lsn($2)" </dev/null | tail -1; }
CATCH() { PSQL "$1" -Atc "SELECT partdist.replay_catchup($2::regclass, $3, 120000)" </dev/null 2>&1 | tail -1; }
SHELL() { echo "$1_$2"; }   # <表> <shardid> → 分片表名
mkdist() {  # <表> <shard_count>
  PSQL $COORD -v ON_ERROR_STOP=1 -q <<SQL >/dev/null
SET citus.shard_count = $2;
SET citus.shard_replication_factor = 1;
CREATE TABLE $1(id int primary key, v text);
SELECT create_distributed_table('$1', 'id');
ALTER TABLE $1 SET (autovacuum_enabled = off);
SQL
}
# 在 <port> 上本地写一行，回显 "xmin|写入之后的下一分片号"
# （写入**之后**取：分片还没发过号时 shard_xid_next 返回 0，首版写入前取就恒判 no）
local_write() {  # <port> <分片表> <oid> <id>
  local nx xm
  PSQLO "$1" "$NOPROP" -q -c "INSERT INTO $2 VALUES ($4, 'x')" </dev/null >/dev/null 2>&1
  xm=$(PSQLO "$1" "$NOPROP -c citus.override_table_visibility=false" -Atc "SELECT xmin::text::bigint FROM $2 WHERE id=$4" </dev/null 2>/dev/null | tail -1)
  nx=$(PSQL "$1" -Atc "SELECT partdist.shard_xid_next($3)" </dev/null | tail -1)
  echo "${xm}|${nx}"
}
is_shard_xid() {  # "xmin|next" → ok：xmin 是本分片发出的号（< 下一号，且在分片 xid 的小整数宇宙里）
  local xm=${1%|*} nx=${1#*|}
  [[ "$xm" =~ ^[0-9]+$ && "$nx" =~ ^[0-9]+$ && "$xm" -ge 3 && "$xm" -lt "$nx" && "$nx" -lt 100000 ]] && echo ok || echo no
}
declare -a CLEAN_TABLES=() CLEAN_GROUPS=()   # 组记为 "sid:port,port,..."
# ★ 一律 2 个副本（3 成员组）。首版用 1 个副本：2 成员组的多数派是 2，
#   ① 杀 leader 后剩 1 个成员永远选不出新主（[5] 无从谈起）；
#   ② 供副本途中副本还没跟上时 propose 就凑不齐 1/2 ⇒ 打分叉标记 ⇒ 心跳自动重做基线，
#      那次基线会把"之后才设的白名单"也带过去，[8] 对照组因此被污染（实测）。
replicate() {  # <表> → 行 "sid|leader|r1,r2|status"
  PSQL $COORD -Atc "SELECT shardid||'|'||leader_port||'|'||array_to_string(replica_ports,',')||'|'||status FROM partdist.raft_replicate_table_shards('$1', 2)" </dev/null 2>&1
}

echo "================ [0] 前置：接口在库 ================"
for p in $COORD 5433; do
  n=$(PSQL $p -Atc "SELECT count(*) FROM pg_proc p JOIN pg_namespace ns ON ns.oid=p.pronamespace WHERE ns.nspname='partdist' AND p.proname IN ('set_table_shard_mvcc','shard_mvcc_register','shard_mvcc_status')" </dev/null | tail -1)
  check "  :$p 上三个新函数都在（refresh_extension_sql 已重放）" "$n" "3"
done
c=$(PSQL $COORD -Atc "SELECT count(*) FROM pg_proc WHERE proname IN ('set_table_shard_mvcc','shard_mvcc_register','shard_mvcc_status') AND pronamespace <> 'partdist'::regnamespace" </dev/null | tail -1)
check "  函数名不与其它模式撞名（§9.2 守卫按 proname 匹配）" "$c" "0"

echo "================ [1] 表 A：2 分片，先供副本 ================"
TA="t7v4a_$TS"; mkdist $TA 2; CLEAN_TABLES+=("$TA")
mapfile -t RA < <(replicate $TA)
printf '    %s\n' "${RA[@]}"
check "两个分片都供副本成功" "$(printf '%s\n' "${RA[@]}" | grep -c '|ok$')" "2"
declare -a SID=() LP=() RPS=() RP=() LO=() RO=()
for row in "${RA[@]}"; do
  IFS='|' read -r s l r st <<<"$row"; [[ "$st" == ok ]] || continue
  r1=${r%%,*}
  SID+=("$s"); LP+=("$l"); RPS+=("$r"); RP+=("$r1"); LO+=("$(LOID $l $s)"); RO+=("$(LOID $r1 $s)")
  CLEAN_GROUPS+=("$s:$l,$r")
done

echo "================ [2] 登记前 ================"
check "leader :${LP[0]} 登记前 registered=no" "$(MSTAT ${LP[0]} ${LO[0]})" "registered=no evidence=no replica=no"
neg=$(PSQL ${RP[0]} -Atc "SELECT partdist.shard_mvcc_register(${SID[0]})" </dev/null 2>&1 | tr '\n' ' ')
check "★ 在副本 :${RP[0]} 上登记被拒（副本进打标集合会让 vacuum 动副本文件）" \
      "$([[ "$neg" == *"副本壳表"* ]] && echo ok || echo no)" "ok"

echo "================ [3] ★ 协调者一条命令登记整张表 ================"
mapfile -t GA < <(PSQL $COORD -Atc "SELECT shardid||'|'||node_port||'|'||status FROM partdist.set_table_shard_mvcc('$TA')" </dev/null 2>/dev/null)
printf '    %s\n' "${GA[@]}"
check "★ 返回 2 行，全部 registered 且副本通知已发出" \
      "$(printf '%s\n' "${GA[@]}" | grep -cE '\|registered oid=[0-9]+ replica_notice=sent$')" "2"
for i in 0 1; do
  check "  leader :${LP[$i]} 分片 ${SID[$i]}：已登记、有证据、不是副本" \
        "$(MSTAT ${LP[$i]} ${LO[$i]})" "registered=yes evidence=yes replica=no"
  lf=$(FLUSH ${LP[$i]} ${LO[$i]})
  for rp in ${RPS[$i]//,/ }; do
    ro=$(LOID $rp ${SID[$i]}); a=$(CATCH $rp $ro $lf)
    check "  副本 :$rp 追平到 leader 位点（${a}/${lf}）" "$([[ "$a" =~ ^[0-9]+$ && "$a" -ge "$lf" ]] && echo ok || echo no)" "ok"
    check "★ 副本 :$rp：有证据、**不**登记（vacuum 碰不到副本文件）" \
          "$(MSTAT $rp $ro)" "registered=no evidence=yes replica=yes"
  done
done
again=$(PSQL $COORD -Atc "SELECT string_agg(split_part(status,' ',1), ',' ORDER BY shardid) FROM partdist.set_table_shard_mvcc('$TA')" </dev/null 2>/dev/null | tail -1)
check "  重跑幂等（already,already）" "$again" "already,already"

echo "================ [4] leader 写入带分片 xid ================"
w=$(local_write ${LP[0]} "$(SHELL $TA ${SID[0]})" ${LO[0]} 1)
check "★ 分片 ${SID[0]} leader 写入 xmin 是分片 xid（${w}）" "$(is_shard_xid "$w")" "ok"

echo "================ [5] ★★ 分片 ${SID[1]} 一行未写就杀 leader ================"
OLD=${LP[1]}; OLDDIR="worker$((OLD - 5432))"; OLDNODE=$((OLD - 5431))
check "  前提：该分片 leader 上一行都没写过（shard_xid_next 仍是初值）" \
      "$(PSQL $OLD -Atc "SELECT partdist.shard_xid_next(${LO[1]}) <= 3" </dev/null | tail -1)" "t"
DEX /work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/$OLDDIR -m immediate -w -t 60 stop </dev/null >/dev/null 2>&1
newp=0
for t in $(seq 1 90); do
  newp=$(PSQL $COORD -Atc "SELECT primary_node FROM partdist.partition_map WHERE partition_id=${SID[1]}" </dev/null 2>/dev/null)
  [[ -n "$newp" && "$newp" != "$OLDNODE" && "$newp" != "0" ]] && break; sleep 2
done
check "控制面登记了新主（node ${newp}）" "$([[ -n "$newp" && "$newp" != "$OLDNODE" && "$newp" != "0" ]] && echo ok || echo no)" "ok"
if [[ -n "$newp" && "$newp" != "$OLDNODE" && "$newp" != "0" ]]; then
  NP=$((newp + 5431)); NO=$(LOID $NP ${SID[1]})
  st=""
  for t in $(seq 1 20); do st=$(MSTAT $NP $NO); [[ "$st" == registered=yes* ]] && break; sleep 1; done
  check "★★ 新主 :$NP 继承了打标身份（${st}）" "$([[ "$st" == registered=yes\ evidence=yes* ]] && echo ok || echo no)" "ok"
  w=$(local_write $NP "$(SHELL $TA ${SID[1]})" $NO 2)
  check "★★ 新主写入 xmin 仍是分片 xid（${w}；修复前走原生路径 = 大 xid）" "$(is_shard_xid "$w")" "ok"
fi
DEX /work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/$OLDDIR -l /work/pg-cluster-data/$OLDDIR/pg.log -w -t 60 start </dev/null >/dev/null 2>&1
for t in $(seq 1 30); do [[ "$(PSQL $OLD -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 2; done
check "  原 leader :$OLD 已恢复" "$(PSQL $OLD -Atc 'SELECT 1' </dev/null 2>/dev/null)" "1"

echo "================ [6] 表 B 非空 ⇒ 拒绝登记 ================"
TB="t7v4b_$TS"; mkdist $TB 1; CLEAN_TABLES+=("$TB")
PSQL $COORD -q -c "INSERT INTO $TB VALUES (1,'x')" </dev/null >/dev/null
gb=$(PSQL $COORD -Atc "SELECT status FROM partdist.set_table_shard_mvcc('$TB')" </dev/null 2>/dev/null | tail -1)
check "★ 非空表登记失败且说明原因（${gb:0:60}…）" "$([[ "$gb" == FAILED:*"堆块"* ]] && echo ok || echo no)" "ok"
sb=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='$TB'::regclass" </dev/null | tail -1)
pb=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=$sb" </dev/null | tail -1)
check "  表 B 的分片没有被登记" "$(MSTAT $pb $(LOID $pb $sb) | cut -d' ' -f1)" "registered=no"

echo "================ [7] 表 C 先登记、后供副本 ⇒ 证据随物理基线到副本 ================"
TC="t7v4c_$TS"; mkdist $TC 1; CLEAN_TABLES+=("$TC")
gc=$(PSQL $COORD -Atc "SELECT status FROM partdist.set_table_shard_mvcc('$TC')" </dev/null 2>/dev/null | tail -1)
echo "    登记：$gc"
check "  表 C 登记成功（此时还没有分区组）" "$([[ "$gc" == registered* ]] && echo ok || echo no)" "ok"
rc=$(replicate $TC | tail -1); echo "    供副本：$rc"
IFS='|' read -r cs cl crs cst <<<"$rc"; cr=${crs%%,*}
check "  表 C 供副本成功" "$cst" "ok"
if [[ "$cst" == ok ]]; then
  CLEAN_GROUPS+=("$cs:$cl,$crs")
  clo=$(LOID $cl $cs); cro=$(LOID $cr $cs)
  a=$(CATCH $cr $cro $(FLUSH $cl $clo))
  check "★ 后加入的副本 :$cr 追平后有证据（来自物理基线末尾的 SHARD_MVCC）" \
        "$(MSTAT $cr $cro)" "registered=no evidence=yes replica=yes"
fi

echo "================ [8] 对照：表 D 用旧的白名单方式打标 ⇒ 副本没有证据 ================"
TD="t7v4d_$TS"; mkdist $TD 1; CLEAN_TABLES+=("$TD")
rd=$(replicate $TD | tail -1)
IFS='|' read -r ds dl drs dst <<<"$rd"; dr=${drs%%,*}
if [[ "$dst" == ok ]]; then
  CLEAN_GROUPS+=("$ds:$dl,$drs")
  dlo=$(LOID $dl $ds); dro=$(LOID $dr $ds)
  DLOG=$(health_node_log "worker$((dl - 5432))")
  NBASE() { DEX grep -c "shard ${dlo} 物理基线已发射" "$DLOG" </dev/null 2>/dev/null || true; }
  sleep 5   # 让供副本途中可能触发的自动修复先落定，再开始对照
  nb0=$(NBASE)
  PSQL $dl -q -c "ALTER SYSTEM SET pg_partdist.shard_relids = '${dlo}';" </dev/null >/dev/null
  PSQL $dl -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
  sleep 2
  check "  leader :$dl 经白名单打标（registered=yes）" "$(MSTAT $dl $dlo | cut -d' ' -f1)" "registered=yes"
  a=$(CATCH $dr $dro $(FLUSH $dl $dlo))
  nb1=$(NBASE)
  # 守卫：设白名单之后若又发过物理基线（自动修复/重新供给），凭据会**按设计**随基线过去 ——
  #   那时"副本没有证据"不成立不代表缺口不存在，只代表对照被污染，必须显式判红而不是默默放过
  check "  对照前提：设白名单之后该分片没有再发物理基线（前 ${nb0} / 后 ${nb1}）" "${nb1:-x}" "${nb0:-y}"
  check "★ 对照成立：旧方式下副本 :$dr 追平后仍**没有**证据（切主会丢身份）" \
        "$(MSTAT $dr $dro)" "registered=no evidence=no replica=yes"
  PSQL $dl -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;" </dev/null >/dev/null
  PSQL $dl -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null
else
  check "  表 D 供副本成功（对照组前提）" "$dst" "ok"
fi

echo "================ [9] 清理 ================"
for g in "${CLEAN_GROUPS[@]}"; do
  sid=${g%%:*}; ports=${g#*:}
  for p in ${ports//,/ }; do
    o=$(LOID $p $sid); [[ -n "$o" ]] && PSQL $p -q -c "SELECT partdist.replay_disable(${o}::regclass)" </dev/null >/dev/null 2>&1
  done
done
for t in "${CLEAN_TABLES[@]}"; do
  d=$(PSQL $COORD -Atc "DROP TABLE IF EXISTS $t;" </dev/null 2>&1 | tail -1)
  check "  协调者 DROP $t（打标表经 2PC 下发，依赖 T7.31）" "$d" "DROP TABLE"
done
for g in "${CLEAN_GROUPS[@]}"; do
  sid=${g%%:*}; ports=${g#*:}
  left=1
  for t in $(seq 1 10); do
    for p in ${ports//,/ }; do PSQL $p -q -c "SELECT partdist.pg_raft_group_drop($sid);" </dev/null >/dev/null 2>&1; done
    sleep 2; left=0
    for p in ${ports//,/ }; do
      k=$(PSQL $p -Atc "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=$sid" </dev/null | tail -1); left=$((left + ${k:-1}))
    done
    [[ "$left" == 0 ]] && break
  done
  for p in ${ports//,/ }; do
    PSQL $COORD -q -c "DELETE FROM partdist.partition_map WHERE partition_id=$sid;" </dev/null >/dev/null 2>&1
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=$sid;" </dev/null >/dev/null 2>&1
  done
  check "  分片组 $sid 在各成员上已拆净" "$left" "0"
done
# 副本壳表在 SHARD_DROP 通知后是普通本地表，由运维回收 —— 这里替运维删掉
for g in "${CLEAN_GROUPS[@]}"; do
  sid=${g%%:*}; ports=${g#*:}
  for p in ${ports//,/ }; do
    for t in "${CLEAN_TABLES[@]}"; do
      PSQLO $p "$NOPROP" -q -c "DROP TABLE IF EXISTS ${t}_${sid};" </dev/null >/dev/null 2>&1
    done
  done
done
health_check_no_crash
echo "========== 结果：PASS=$PASS FAIL=$FAIL =========="
[[ $FAIL -eq 0 ]]
