#!/usr/bin/env bash
# [宿主机] L1 惰性回放验收。
#
# 与 R1（持续回放）的区别就是本用例的核心断言：
#   **写入之后、触发之前，follower 必须一条记录都没回放。**
# 副本此刻只有 P2 平凡 apply 落下的字节，壳表是空的、applied 停在 0。
# 只有 replay_catchup 被调用，redo 才发生；追平后页面与 leader 洞外逐字节一致。
#
# 覆盖：
#   V1 armed 不等于在回放（零 redo 证明）
#   V2 触发一次追平 → 页面一致（含索引/TOAST/VM）
#   V3 增量：再写一批 → 仍然零回放 → 再触发 → 再一致
#   V4 追平中途 kill -9 → 重新触发仍能从 durable 游标续上
#   V5 未 armed 的 shard 拒绝触发
#
# 全程**不做 VACUUM (FREEZE)**（R2-f 起）：页面判据已对齐内核 heap_mask()，
# 不再依赖"冻结把 t_infomask 洗成 canonical 状态"这根拐杖。所以本用例证明的是
# **普通事务的物理回放**结果与 leader 一致。
#
# 本用例**不涉及可见性**：follower 上这些元组的 xmin 是 leader 的 xid，本地原生
# clog 对它一无所知。判决记在 pg_gclog（R2-d，见 test_txn_layer_r2.sh 的核账），
# 读路径查它是 R3 的事。R2 的验收标准是账本正确，不是可见（FRD §14.2 R2 行）。
set -u

CONTAINER="${CONTAINER:-pg-citus-replay-container}"
COORD=5432
PASS=0; FAIL=0

DEX()  { docker exec -i -u postgres "$CONTAINER" "$@"; }
PSQL() { local port=$1; shift; DEX /work/pg-install/bin/psql -p "$port" -U postgres -d postgres "$@"; }

check() {
  # ★ 空值守卫：两个命令替换都失败时 "" == "" 会静默判通过。
  # 典型漏网：D1 用 leader_relnum / locmap_leader_relnum 互比文件号，
  # 若 shard_fileset() 因登记被丢弃而返回 0 行，两边都是空串 ⇒
  # "文件号确实换了"与"locmap 已换到新文件号"**双双恒真**。
  # 期望值本身就是空串的场景本项目里不存在，所以一律要求非空。
  if [[ -z "$2" ]]; then
    echo "  FAIL  $1（实际取不到值：命令替换返回空串）"; FAIL=$((FAIL+1)); return
  fi
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; PASS=$((PASS+1));
  else echo "  FAIL  $1（实际='$2' 期望='$3'）"; FAIL=$((FAIL+1)); fi
}

# 节点崩溃检查（见 lib_node_health.sh 头部：验收脚本原本对"节点崩了"是瞎的）
source "$(dirname "$0")/lib_node_health.sh"
health_mark_start

# 数据组领导权守卫：一旦 placement 节点丢主，leader 的写入会被 pg_raft 拒绝，
# 后续记录不再复制 —— follower 的"落后"就不是回放缺陷了。必须把这种情况
# 单独报出来，否则会被误判成页面比对失败（实测踩过：hardstate tmp 竞态导致
# 连环改选，一路误导到页面结构性差异）。
assert_group_leader() {  # assert_group_leader <阶段名>
  local st
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null)
  if [[ "$st" != "leader" ]]; then
    echo "  FAIL  [$1] 数据组领导权已漂移（placement 节点 :${pport} 现为 '${st}'）"
    echo "        → 这是 Raft 层问题，非回放缺陷；后续断言不可信"
    FAIL=$((FAIL+1))
    return 1
  fi
  return 0
}

# 追平到 leader 的当前位置：先等字节到齐，再用**显式 bound** 触发。
# 显式传 bound 才是生产语义（升主时传 commit_index）；用 NULL 会把
# "本地已有多少字节"当上界，字节还在路上时会提前返回，看起来成功实则落后。
catchup_to_leader() {  # catchup_to_leader <fport> <标签>
  local fp=$1 tag=$2 lp foid recv t app
  lp=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
  foid=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${gid})")
  for t in $(seq 1 60); do
    recv=$(PSQL $fp -Atc "SELECT partdist.get_partition_flush_lsn(${foid})")
    [[ -n "$recv" && "$recv" -ge "$lp" ]] && break
    sleep 1
  done
  if [[ -z "$recv" || "$recv" -lt "$lp" ]]; then
    echo "  FAIL  [$tag] 字节未复制齐（收到 ${recv} / leader ${lp}）—— 复制层问题"
    FAIL=$((FAIL+1)); echo ""; return 1
  fi
  app=$(PSQL $fp -Atc "SELECT partdist.replay_catchup('${shard_tbl}', ${lp}, 300000)" 2>&1)
  # 触发失败时把 psql 的原话打出来 —— 只报"实际=''"没法定位是拒绝、超时还是崩溃
  if [[ ! "$app" =~ ^[0-9]+$ ]]; then
    echo "  [$tag] replay_catchup 未返回数字，原话：" >&2
    sed 's/^/      /' <<< "$app" >&2
  fi
  echo "$app|$lp"
}

echo "========== [0] 前置 =========="
leader=$(PSQL $COORD -Atc "SELECT leader_node_id FROM partdist.pg_raft_get_cluster_status()")
check "group0 有 leader" "$([[ -n "$leader" && "$leader" != "0" ]] && echo ok)" "ok"

echo "========== [1] 夹具：1 分片分布表（PK 索引 + TOAST） =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
DROP TABLE IF EXISTS l1_lazy;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 1;
CREATE TABLE l1_lazy(id int primary key, v text, big text);
SELECT create_distributed_table('l1_lazy', 'id');
ALTER TABLE l1_lazy SET (autovacuum_enabled = off);
SQL

gid=$(PSQL $COORD -Atc "SELECT shardid FROM pg_dist_shard WHERE logicalrelid='l1_lazy'::regclass")
pport=$(PSQL $COORD -Atc "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid=${gid}")
pnode=$((pport - 5431))
f1=""; f2=""
for p in 5433 5434 5435 5436 5437 5438 5439 5440; do
  [[ "$p" == "$pport" ]] && continue
  if [[ -z "$f1" ]]; then f1=$p; elif [[ -z "$f2" ]]; then f2=$p; else break; fi
done
f1node=$((f1 - 5431)); f2node=$((f2 - 5431))
shard_tbl="l1_lazy_${gid}"
echo "  shard=${gid} leader=:${pport} followers=:${f1} :${f2}"

echo "========== [2] fileset / locmap / arm =========="
nrels=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT partdist.register_shard_fileset('${shard_tbl}')" | tail -1)
check "leader fileset 注册" "$nrels" "4"

fsrows=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; SELECT role||','||ord||','||spc||','||db||','||relnum FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord" | grep ',')
roles=$(echo "$fsrows" | cut -d, -f1 | paste -sd,); ords=$(echo "$fsrows" | cut -d, -f2 | paste -sd,)
spcs=$(echo "$fsrows" | cut -d, -f3 | paste -sd,);  dbs=$(echo "$fsrows" | cut -d, -f4 | paste -sd,)
rels=$(echo "$fsrows" | cut -d, -f5 | paste -sd,)

for fp in $f1 $f2; do
  PSQL $fp -v ON_ERROR_STOP=1 -q <<SQL
SET citus.enable_ddl_propagation = off;
DROP TABLE IF EXISTS ${shard_tbl};
CREATE TABLE ${shard_tbl} (LIKE l1_lazy INCLUDING ALL);
ALTER TABLE ${shard_tbl} SET (autovacuum_enabled = off);
SQL
  np=$(PSQL $fp -Atc "SELECT partdist.replay_set_locmap('${shard_tbl}', ARRAY[${roles}], ARRAY[${ords}], ARRAY[${spcs}]::oid[], ARRAY[${dbs}]::oid[], ARRAY[${rels}]::oid[])")
  check "follower :$fp locmap 配对" "$np" "4"
done
for p in $pport $f1 $f2; do
  PSQL $p -q -c "SELECT partdist.rebuild_shard_identity();" >/dev/null
done

echo "========== [3] Raft 数据组 + arm（注意：arm ≠ 开始回放） =========="
members="ARRAY[${pnode}, ${f1node}, ${f2node}]"
PSQL $pport -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
st=""
for t in $(seq 1 20); do
  st=$(PSQL $pport -Atc "SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=${gid}" 2>/dev/null)
  [[ "$st" == "leader" ]] && break; sleep 1
done
check "分区组 leader 就位" "$st" "leader"
for fp in $f1 $f2; do
  PSQL $fp -q -c "SELECT partdist.pg_raft_group_create(${gid}, ${members});" >/dev/null
  PSQL $fp -q -c "SELECT partdist.replay_enable('${shard_tbl}');" >/dev/null
done
sleep 1
foid=$(PSQL $f1 -Atc "SELECT partdist.local_partition_for_shard(${gid})")
check "arm 后 state 仍为 idle（未开始回放）" \
      "$(PSQL $f1 -Atc "SELECT state FROM partdist.replay_status() WHERE shard=${foid}")" "idle"

echo "========== [V1] 写入后、触发前：必须零回放 =========="
PSQL $COORD -v ON_ERROR_STOP=1 -q <<'SQL'
INSERT INTO l1_lazy
SELECT g, 'v'||g,
       CASE WHEN g % 10 = 0
            THEN (SELECT string_agg(md5((g*1000+i)::text), '') FROM generate_series(1,300) i)
            ELSE 'small' END
FROM generate_series(1, 200) g;
SQL
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "UPDATE l1_lazy SET v = v||'-u1' WHERE id % 3 = 0;"

leader_oid=$(PSQL $pport -Atc "SELECT partdist.local_partition_for_shard(${gid})")
lead_plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
check "leader 已产生 parwal 记录 (${lead_plsn})" "$([[ "$lead_plsn" -gt 50 ]] && echo ok)" "ok"

# 字节确实复制过来了（P2 平凡 apply），但一条都没回放
f1_bytes=$(PSQL $f1 -Atc "SELECT partdist.get_partition_flush_lsn(${foid})")
check "follower 已收到字节 (${f1_bytes})" "$([[ "$f1_bytes" -ge "$lead_plsn" ]] && echo ok)" "ok"
sleep 3   # 给"如果它会自己回放"留出充足时间
check "★ 触发前 applied 仍为 0（零 redo）" \
      "$(PSQL $f1 -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}")" "0"
check "★ 触发前 state 仍为 idle" \
      "$(PSQL $f1 -Atc "SELECT state FROM partdist.replay_status() WHERE shard=${foid}")" "idle"
check "★ 触发前壳表为空" \
      "$(PSQL $f1 -Atc "SELECT count(*) FROM ${shard_tbl}")" "0"
check "★ 触发前主堆文件为 0 字节（一页都没写过）" \
      "$(PSQL $f1 -Atc "SELECT pg_relation_size('${shard_tbl}')")" "0"

echo "========== [V5] 未 armed 的 shard 拒绝触发 =========="
PSQL $f2 -q -c "SELECT partdist.replay_disable('${shard_tbl}');" >/dev/null
rej=$(PSQL $f2 -Atc "SELECT partdist.replay_catchup('${shard_tbl}')" 2>&1 | grep -c "未 armed")
check "未 armed 时 replay_catchup 被拒" "$rej" "1"
PSQL $f2 -q -c "SELECT partdist.replay_enable('${shard_tbl}');" >/dev/null

echo "========== [V2] 触发一次追平 =========="
assert_group_leader "V2 前"
t0=$(date +%s)
r=$(catchup_to_leader $f1 "V2"); app1=${r%%|*}; tgt=${r##*|}
t1=$(date +%s)
check "replay_catchup 追平到 ${tgt} (返回 ${app1}，耗时 $((t1-t0))s)" \
      "$([[ "$app1" =~ ^[0-9]+$ && -n "$tgt" && "$app1" -ge "$tgt" ]] && echo ok)" "ok"
check "追平后 state 回到 idle" \
      "$(PSQL $f1 -Atc "SELECT state FROM partdist.replay_status() WHERE shard=${foid}")" "idle"
check "追平后主堆文件非空" \
      "$([[ "$(PSQL $f1 -Atc "SELECT pg_relation_size('${shard_tbl}')")" -gt 0 ]] && echo ok)" "ok"

echo "========== [V3] 增量：再写一批仍然零回放，再触发再一致 =========="
before_app=$(PSQL $f1 -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}")
PSQL $COORD -v ON_ERROR_STOP=1 -q -c \
  "INSERT INTO l1_lazy SELECT g, 'b'||g, 'small-b' FROM generate_series(501, 600) g;"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c "DELETE FROM l1_lazy WHERE id % 17 = 0;"

# ★ 先证明"有东西可回放"，再谈"没有回放"。
# 只断言 applied 未动是不够的：若第二批的字节压根没复制到 follower（复制层断了、
# 分区组丢主……），applied 当然不会动 —— 这条 ★ 断言就平凡通过，而"惰性"
# 一个字节都没被检验。V1 对第一批有这条到货断言，V3 漏了。
lead_plsn2=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})")
f1_bytes2=""
for t in $(seq 1 30); do
  f1_bytes2=$(PSQL $f1 -Atc "SELECT partdist.get_partition_flush_lsn(${foid})")
  [[ -n "$f1_bytes2" && "$f1_bytes2" -ge "$lead_plsn2" ]] && break
  sleep 1
done
check "第二批字节已复制到 follower（${f1_bytes2} ≥ leader ${lead_plsn2}）" \
      "$([[ "$f1_bytes2" =~ ^[0-9]+$ && "$f1_bytes2" -ge "$lead_plsn2" ]] && echo ok || echo "no(${f1_bytes2}/${lead_plsn2})")" "ok"
check "第二批确实带来了新字节（${lead_plsn2} > 上轮游标 ${before_app}）" \
      "$([[ "$lead_plsn2" =~ ^[0-9]+$ && "$lead_plsn2" -gt "$before_app" ]] && echo ok || echo "no")" "ok"
sleep 3   # 字节已到齐，再给"如果它会自己回放"留出充足时间
check "★ 第二批写入后 applied 未动（仍是 ${before_app}）" \
      "$(PSQL $f1 -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=${foid}")" "$before_app"

# 这里原本是一句 VACUUM (FREEZE)（R2-f 移除）。
#
# 它当初的作用是**页面比对的拐杖**：freeze 记录整体重写 t_infomask，把 leader
# 扫描期顺手设上的提示位覆盖成 canonical 状态，两侧才对得上。判据改为对齐内核
# heap_mask() 之后（提示位、t_cid、元组对齐填充等本来就在掩码里），这根拐杖不再
# 需要 —— 换成一批**普通事务**，V3 的增量语义不变，而且顺带证明了页面判据在
# 不冻结的情况下同样成立。
#
# 注意它**不能**证明"普通事务可见"：follower 上这些元组的 xmin 是 leader 的 xid，
# 本地原生 clog 对它一无所知，判定要走 pg_gclog（R2-d 已记账）+ 读路径（R3 未实装）。
# R2 的验收标准是**账本正确**，不是可见（FRD §14.2 R2 行）。
assert_group_leader "普通事务批次前" && \
PSQL $COORD -v ON_ERROR_STOP=1 -q \
  -c "UPDATE l1_lazy SET v = v||'-u' WHERE id % 7 = 0;"
# 保留一次**普通** VACUUM（不带 FREEZE）：它照样建/更新 VM fork 并产生
# XLOG_HEAP2_VISIBLE 记录，VM 的比对覆盖不丢；而 vacuum_freeze_min_age 默认
# 5000 万，本用例的元组全都够不着，所以一个都不会被冻结 —— 拐杖去掉了，覆盖还在。
assert_group_leader "VACUUM 前" && \
PSQL $pport -v ON_ERROR_STOP=1 -q \
  -c "SET citus.override_table_visibility=false;" \
  -c "VACUUM ${shard_tbl};"
assert_group_leader "普通事务批次后"

r=$(catchup_to_leader $f1 "V3-f1"); app1=${r%%|*}; tgt=${r##*|}
check "第二次触发追平到 ${tgt}" \
      "$([[ "$app1" =~ ^[0-9]+$ && -n "$tgt" && "$app1" -ge "$tgt" ]] && echo ok)" "ok"
r=$(catchup_to_leader $f2 "V3-f2"); app2=${r%%|*}; tgt2=${r##*|}
check "follower2 触发追平到 ${tgt2}" \
      "$([[ "$app2" =~ ^[0-9]+$ && -n "$tgt2" && "$app2" -ge "$tgt2" ]] && echo ok)" "ok"

echo "========== [V2/V3 校验] 页面洞外逐字节一致 =========="
PSQL $pport -q -c "CHECKPOINT;" >/dev/null
docker cp "$(dirname "${BASH_SOURCE[0]}")/pagecmp.py" "$CONTAINER":/tmp/pagecmp.py >/dev/null 2>&1
sleep 3

# 按 fileset 的 role 与 fork 推出 pagecmp 的掩码口径。
#   role 0/2 = 主堆 / TOAST 堆 → heap
#   role 1/3 = 索引 / TOAST 索引 → btree（**不能**用堆规则：IndexTuple 只有
#              8 字节头，堆元组那套偏移掩到的是索引键本身）
#   _vm fork → vm（VM 页没有"空闲空洞"，套堆规则会把整张位图掩掉）
pagecmp_kind() {   # $1=key(role.ord)  $2=fork("" 或 "_vm")
  [[ "$2" == "_vm" ]] && { echo vm; return; }
  case "${1%%.*}" in
    1|3) echo btree ;;
    *)   echo heap ;;
  esac
}


pdata=$(PSQL $pport -Atc "SHOW data_directory")
# relnum 是 relfilenode 不是关系 OID：一旦 leader 做过 VACUUM FULL/REINDEX/
# TRUNCATE 两者就不再相等，`relnum::regclass` 会取到别的关系或 NULL。
# 本用例虽不做那些 DDL，仍统一走 pg_filenode_relation，免得将来加一条 DDL
# 就让整片比对静默变成零个检查。
FILESET_PATHS_SQL="SELECT role||'.'||ord||','||
       pg_relation_filepath(pg_filenode_relation(
           CASE WHEN spc = 1663 THEN 0 ELSE spc END, relnum))
  FROM partdist.shard_fileset('${shard_tbl}') ORDER BY role, ord"

lead_paths=$(PSQL $pport -Atc "SET citus.override_table_visibility=false; ${FILESET_PATHS_SQL}" | grep ',')

diff_follower() {  # <fport> <标签>
  local fp=$1 tag=$2 fdata frows ncmp=0 nempty=0
  fdata=$(PSQL $fp -Atc "SHOW data_directory")
  frows=$(PSQL $fp -Atc "${FILESET_PATHS_SQL}" | grep ',')

  # 必须先读进数组：循环体里的 `docker exec -i` 会吞掉 `while read` 的标准输入，
  # 第一行之后的 fileset 成员被静默跳过 —— 表现是"全 PASS 但只比了主堆"。
  local -a rows; local lrow
  mapfile -t rows <<< "$lead_paths"

  for lrow in "${rows[@]}"; do
    local key lrel frel fork lpath fpath lex fex same
    [[ -n "$lrow" ]] || continue
    key=${lrow%%,*}; lrel=${lrow#*,}
    frel=$(echo "$frows" | grep "^${key}," | cut -d, -f2)
    for fork in "" "_vm"; do
      lpath="${pdata}/${lrel}${fork}"; fpath="${fdata}/${frel}${fork}"
      lex=$(DEX bash -c "test -f '$lpath' && echo y || echo n" </dev/null)
      fex=$(DEX bash -c "test -f '$fpath' && echo y || echo n" </dev/null)
      [[ "$lex" == "n" && "$fex" == "n" ]] && continue
      check "${tag} ${key}${fork:-.main} 两侧都存在" "$lex/$fex" "y/y"
      [[ "$lex" == "y" && "$fex" == "y" ]] || continue
      local errf; errf=$(mktemp)
      local kind; kind=$(pagecmp_kind "$key" "$fork")
      same=$(DEX python3 /tmp/pagecmp.py --kind="$kind" "$lpath" "$fpath" </dev/null 2>"$errf")
      ncmp=$((ncmp + 1))
      # ★ IDENTICAL_EMPTY = 两侧都是 0 字节，**比较了零个页面**，不构成一致的证据。
      # 主堆（role 0）必须有内容 —— 空了说明整条重填/回放路径失效；
      # TOAST 堆与索引可以合法为空（没有超长值就不会有 TOAST 页）。
      # 旧版本把这种情况一律算作"逐字节一致"，等于给零覆盖发通行证。
      local want="IDENTICAL_OUTSIDE_HOLE"
      if [[ "${key%.*}" != "0" && "$same" == "IDENTICAL_EMPTY" ]]; then
        want="IDENTICAL_EMPTY"; nempty=$((nempty + 1))
      fi
      check "${tag} ${key}${fork:-.main} 洞外逐字节一致" "$same" "$want"
      if [[ "$same" != "IDENTICAL_OUTSIDE_HOLE" ]]; then
        echo "        ---- 差异定性（leader=${lpath##*/} follower=${fpath##*/}）----"
        sed 's/^/        /' "$errf"
        echo "        ---- 追平位置：follower applied=$(PSQL $fp -Atc "SELECT applied FROM partdist.replay_status() WHERE shard=$(PSQL $fp -Atc "SELECT partdist.local_partition_for_shard(${gid})")" 2>/dev/null) leader plsn=$(PSQL $pport -Atc "SELECT partdist.get_partition_flush_lsn(${leader_oid})" 2>/dev/null) ----"
      fi
      rm -f "$errf"
    done
  done

  # 守卫：真正比过的文件数必须覆盖全部 fileset 成员。没有这条，
  # "循环提前退出"会表现为全 PASS —— 零个检查是静默通过的。
  check "${tag} 实际比对了 ${ncmp} 个文件（>= 4 个成员）" \
        "$([[ "$ncmp" -ge 4 ]] && echo ok)" "ok"
}
diff_follower $f1 "f1"
diff_follower $f2 "f2"

echo "========== [V4] 追平中途 kill -9 → 重新触发从 durable 游标续上 =========="
assert_group_leader "V4 前"
fdir="worker$((f1 - 5432))"

# 分两批写。第一批先老老实实追平，落下一个**已持久化的中间游标**；第二批留作
# 崩溃后的欠账。这样"从 durable 游标续上"是被真正测到的 —— 原来的写法指望
# kill 恰好落在 apply 中途，那是时序赌博：150 行的回放是毫秒级，sleep 2 之后
# worker 多半已经退出，pgrep 抓到的是正在消失的进程，kill 落空、节点根本没
# 重置，而下面的断言照样全 PASS。
PSQL $COORD -v ON_ERROR_STOP=1 -q -c \
  "INSERT INTO l1_lazy SELECT g, 'c'||g, 'small-c' FROM generate_series(1001, 1075) g;"
r=$(catchup_to_leader $f1 "V4-mid"); mid_app=${r%%|*}; mid_tgt=${r##*|}
check "V4 中间游标已推进到 ${mid_tgt}" \
      "$([[ "$mid_app" =~ ^[0-9]+$ && -n "$mid_tgt" && "$mid_app" -ge "$mid_tgt" ]] \
        && echo ok || echo "no(applied=${mid_app})")" "ok"
PSQL $COORD -v ON_ERROR_STOP=1 -q -c \
  "INSERT INTO l1_lazy SELECT g, 'c'||g, 'small-c' FROM generate_series(1076, 1150) g;"

# kill 的确定性：worker 干完活立刻退出，抓它是跟时序赛跑。用调试 GUC 给它一个
# 确定的存活窗口（进主循环前先睡 delay_ms），kill 必然落在一个**活着的 shmem
# worker** 上；而 shmem worker 崩溃 = 整个节点被 postmaster 拖进崩溃恢复
# （PG 语义，replay_worker.c 里注释写了"实测踩到"）。
delay_ms=5000
PSQL $f1 -q -c "ALTER SYSTEM SET pg_partdist.replay_debug_delay_ms=${delay_ms};" >/dev/null 2>&1
PSQL $f1 -q -c "SELECT pg_reload_conf();" >/dev/null 2>&1
# 日志文件按 /proc/<postmaster>/fd/2 解析，不按 <节点>.log 猜 —— 该约定路径
# 会被别的用例（LWC 的 pg_ctl -l）改道，改道后老文件永久冻结，在它上面 grep
# 恒为 0 条，这条断言就成了"永远失败"或（若判据写反）"永远通过"。
nlog=$(health_node_log "$fdir")
[[ -z "$nlog" ]] && nlog="/work/pg-cluster-data/${fdir}.log"
echo "  f1 节点日志：${nlog}"
logmark=$(DEX bash -c "wc -l < '${nlog}' 2>/dev/null || echo 0" | tr -d '[:space:]')

( PSQL $f1 -Atc "SELECT partdist.replay_catchup('${shard_tbl}', NULL, 60000)" >/dev/null 2>&1 ) &
wpid=""
for t in $(seq 1 50); do   # 10s 上限，覆盖 5s 存活窗口
  wpid=$(DEX bash -c "
    for pid in \$(pgrep -f '[r]eplay worker'); do
      [ \"\$(readlink /proc/\$pid/cwd 2>/dev/null)\" = \"/work/pg-cluster-data/${fdir}\" ] && { echo \$pid; break; }
    done")
  [[ -n "$wpid" ]] && break
  sleep 0.2
done
# ★ 必须断言"确实找到了 worker"（R1/R2 在同一位置都下了这条，只有 L1 漏了）。
# 找不到就静默跳过 kill 的话，V4 整段退化成"连续触发两次 catchup"。
check "找到 f1 节点的 replay worker（存活窗口内）" \
      "$([[ -n "$wpid" ]] && echo ok || echo "未找到")" "ok"
if [[ -n "$wpid" ]]; then
  DEX kill -9 "$wpid" 2>/dev/null
  echo "  已 kill -9 worker(pid=$wpid)，节点将整体重置"
fi
wait 2>/dev/null

for t in $(seq 1 60); do
  [[ "$(PSQL $f1 -Atc 'SELECT 1' 2>/dev/null)" == "1" ]] && break
  sleep 1
done
check "f1 节点从重置中恢复" "$(PSQL $f1 -Atc 'SELECT 1' 2>/dev/null)" "1"
# 崩溃证据不能用 pg_postmaster_start_time()：kill -9 一个挂 shmem 的 bgworker
# 会让 postmaster **重新初始化整个集群**（杀光 backend、走崩溃恢复），但
# postmaster 进程自己不重启 —— 启动时刻纹丝不动。（金丝雀那边用它是对的，
# 因为那里做的是真正的 pg_ctl stop + start。）
# 这里改取节点日志里内核自己的证词，并只扫 kill 之后新增的部分，避免被本轮
# 之前的重置蒙混过关。
reinit=$(DEX bash -c "tail -n +$((logmark + 1)) '${nlog}' 2>/dev/null | grep -cE 'reinitializing|was not properly shut down'" | tr -d '[:space:]')
check "节点确实走了一次崩溃重置（日志证据 ${reinit} 条）" \
      "$([[ "$reinit" =~ ^[0-9]+$ && "$reinit" -ge 1 ]] && echo ok || echo "no(${reinit})")" "ok"

# 关掉存活窗口，否则 postgresql.auto.conf 会让崩溃后每个 worker 都白睡 5s
PSQL $f1 -q -c "ALTER SYSTEM RESET pg_partdist.replay_debug_delay_ms;" >/dev/null 2>&1
PSQL $f1 -q -c "SELECT pg_reload_conf();" >/dev/null 2>&1

# ★ 这才是 V4 的题眼：崩溃前已持久化的游标不能回退。
#
# 取值必须看 durable 这一列，不是 applied：
#   applied —— 共享内存里的**活进度**，由在跑的 worker 维护；节点整体重置后
#              没有 worker，读出 0 是正确语义，拿它断言等于必然失败。
#   durable —— replay_status() 现读 apply_checkpoint 文件（replay_worker.c 里
#              直接 ReadApplyCheckpoint），才是"崩溃能续上"所依赖的那个游标。
# 每轮 apply 收尾都会落一次 checkpoint（shard_replay.c 的"每轮收尾"），所以
# 上面那次追平返回之后，durable 就应当已经 ≥ mid_tgt。
post=$(PSQL $f1 -Atc "SELECT applied||'/'||durable FROM partdist.replay_status() WHERE shard=${foid}" 2>&1)
post_app=${post%%/*}; post_dur=${post##*/}
check "★ 崩溃后落盘游标未回退（durable=${post_dur} ≥ 中间游标 ${mid_tgt}；shmem applied=${post_app} 归零属正常）" \
      "$([[ "$post_dur" =~ ^[0-9]+$ && "$post_dur" -ge "$mid_tgt" ]] \
        && echo ok || echo "no(${post})")" "ok"

r=$(catchup_to_leader $f1 "V4"); app1=${r%%|*}; tgt=${r##*|}
check "重新触发后追平到 ${tgt}" \
      "$([[ "$app1" =~ ^[0-9]+$ && -n "$tgt" && "$app1" -ge "$tgt" ]] && echo ok || echo "no(applied=${app1})")" "ok"
# 崩溃 + 续上之后数据必须与 leader 完全一致 —— 只看游标数字不看内容，
# 回放漏行/重放导致主键冲突这类问题会从眼皮底下溜过去。
lead_sum=$(PSQL $pport -Atc "SELECT count(*)||'/'||coalesce(sum(id),0) FROM ${shard_tbl}")
foll_sum=$(PSQL $f1   -Atc "SELECT count(*)||'/'||coalesce(sum(id),0) FROM ${shard_tbl}")
check "★ 崩溃续上后 follower 数据与 leader 一致（${lead_sum}）" "$foll_sum" "$lead_sum"

echo
health_check_no_crash
health_check_no_drops
health_check_worker_pool
echo "========== 结果：PASS=${PASS} FAIL=${FAIL} =========="
[[ "$FAIL" -eq 0 ]] && echo "L1 惰性回放验收：全部通过" || echo "L1 惰性回放验收：存在 FAIL"

if [[ "${KEEP_FIXTURE:-0}" != "1" ]]; then
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SELECT partdist.replay_disable('${shard_tbl}');" >/dev/null 2>&1
  done
  for p in $pport $f1 $f2; do
    PSQL $p -q -c "SELECT partdist.pg_raft_group_reset();" >/dev/null 2>&1
  done
  for fp in $f1 $f2; do
    PSQL $fp -q -c "SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${shard_tbl};" >/dev/null 2>&1
  done
  PSQL $COORD -q -c "DROP TABLE IF EXISTS l1_lazy;" >/dev/null 2>&1
  for p in $COORD $pport $f1 $f2; do
    PSQL $p -q -c "DELETE FROM partdist.partition_map WHERE partition_id=${gid};" >/dev/null 2>&1
  done
fi
exit $FAIL
