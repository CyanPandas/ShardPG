#!/usr/bin/env bash
# [宿主机] P6 出口门禁：**全量套件归一到一个 runner**（DEV PLAN §3.8 T6.7）。
#
# 里程碑门禁原文是"全量套件零 FAIL"。此前这句话没有落点：34 套散在 4 个容器、
# 2 种执行模型上，run_p5_exit.sh 只跑其中 9 套。本脚本把它们收编成一次可读的
# 运行，并把这一期反复踩到的 harness 债一次性做进净场层。
#
# ────────────────────────────────────────────────────────────────
# 口径（R-P6-1 的裁定）：**全收编到 tx2**。
#   依据是实测而非假设 —— replay/tx 时代的套件用 CONTAINER 覆盖在 tx2 上跑得通
#   （ddl_fileset_d1 78/0、follower_replay_r1 46/4、clog_hole_c4 14/0 均已实证）。
#   另两个选项的代价：分环境门禁要起停三套 9 节点集群、"全量零 FAIL"退化成四份
#   报告的合取；缩编退役需要逐套论证覆盖关系，值得做但不该挡住出口。
# ────────────────────────────────────────────────────────────────
#
# ★ 净场层解决的债（每一条都是本期实测踩出来的）：
#   ① 全局独占锁 —— 各套件自己的 flock 只锁**同名套件**，锁不住"上一批还没跑完、
#      我又启动了一批"（R-P6-5）。本期因此产生过整轮不可信数字。
#   ② 空闲判据不许自匹配 —— `pgrep -f 'test_.*\.sh'` 会匹配到**本脚本自己**
#      （命令行里就写着套件路径），实测白等 30 分钟。模式锚成 `^bash \./test_`。
#   ③ 日志轮转 —— health_check_no_crash 要 grep /work/pg-cluster-data/*/*.log，
#      实测这些日志几小时内涨到 24 GB，单次健康检查耗时 7 分 40 秒并把套件撑爆
#      900s 超时。跑前归档（改名保留，不删）。
#   ④ 陈旧 pg_shard_xid 槽位回收（R-P6-4）—— 分配器槽位只增不减且有 64 硬上限，
#      跨过之后该节点所有打标验收在登记那步就死，报错与被测内容毫无关系。
#   ⑤ 残表按前缀清 —— 夹具残表单调累积（P5 出口记的"跑前 9 张、跑完 10 张"）。
#   ⑥ 两轮 raft 复位 —— 单轮按端口顺序复位时，未复位的节点会把组心跳回已复位的
#      节点上（P5 出口实测：残 1 个组即把 dtx_tso_p4 打红）。
#   ⑦ 逐套件独立超时 —— 900s 对 follower_replay_r1 不够（实测被砍在第 [6] 节）。
#
# 用法：bash run_p6_exit.sh [套件名...]    # 不给参数 = 全量
set -u

C="${CONTAINER:-pg-citus-tx2-container}"
T="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT_DIR:-/tmp/p6_exit_$(date +%Y%m%d-%H%M%S)}"
PORTS="$(seq 5432 5440)"
COORD_PORT=5432
mkdir -p "$OUT"

DEX() { docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" "$@"; }
PS()  { local p=$1; shift; DEX /work/pg-install/bin/psql -h /tmp -p "$p" -U postgres -d postgres -X "$@"; }

# ── 套件清单：名字 超时秒 出身 ────────────────────────────────────
# 出身只是注释，用来在结果里看出"哪一代的东西红了"。
SUITES=(
  # ── P1–P5 主线（本环境原生）
  "shard_identity_p0      600  P0"
  "shard_xid_p1           900  P1"
  "shard_pagecmp_p1       900  P1"
  "shard_clog_p2          900  P2"
  "tso_si_p3              900  P3"
  "shard_gating_p4        900  P4"
  "dtx_convergence_p4    1500  P4"
  "dtx_tso_p4            1800  P4"
  "shard_vacuum_p5        900  P5"
  "shard_vacuum_replay_p5 900  P5"
  # ── P6 新增
  "shard_baseline_p6      900  P6"
  "locmap_base_p6         900  P6"
  "offnum_guard_p6        900  P6"
  "replica_gate_p6        900  P6"
  "xid_watermark_p6       900  P6"
  "negative_p6            900  P6"
  "promote_p6            1200  P6"
  "replay_bound_p6        900  P6"
  # ── replay 时代（CONTAINER 覆盖到 tx2）
  "follower_replay_r1    1800  R1"
  "txn_layer_r2          1800  R2"
  "lazy_replay_l1        1200  L1"
  "ddl_fileset_d1        1200  D1"
  "freeze_sync_d2        1200  D2"
  "clog_hole_c4           900  C4"
  "local_wal_conflict     900  ops"
  # ── TX 时代（CONTAINER 覆盖到 tx2）
  "dtx_replay_tx1        1200  TX1"
  "dtx_commit_marker_tx2 1200  TX2"
  "promote_catchup_tx3   1200  TX3"
  "fastpath_divergence_tx4 1200 TX4"
)
# ── ops 时代那 8 套：**默认不进门禁**（R-P6-1 的口径，第二次修订）────────
#
# 它们是为**旧的 3 节点布局**（master / worker1 / worker2）写的，在 9 节点集群上
# 会按那套名字停节点**且不复原**，把之后每一套都变成 `connection refused`。
# 首版把它们收编进来，实测直接毒化整批（tso_si_p3 17/21、连带 7 套全红）。
#
# 处置：从默认门禁移出，需要时用 `--with-ops` 显式带上。要真正收编，得先把它们
# 适配到 9 节点布局并保证"停了必复原"——那是独立一项，不该挡住 P6 出口。
# 这正是 R-P6-1 三个选项里的 (c) 缩编，且是**有证据的缩编**而不是嫌麻烦。
OPS_SUITES=(
  "shard_auto_init          600  ops"
  "multi_table_isolation    600  ops"
  "segment_boundary_lsn     600  ops"
  "bulk_insert_recovery     900  ops"
  "crash_recovery           900  ops"
  "demux_backlog_recovery   900  ops"
  "corrupt_segment_recovery 900  ops"
  "enospc_recovery          900  ops"
)
# 容器内执行的那 8 套（没有 CONTAINER 变量，直接用 /work/pg-install/bin）
# ★ 必须写成单行（或事后归一空白）：跨行写时**行尾那个名字后面是换行不是空格**，
#   `case " $INSIDE " in *" $name "*` 就匹配不上 —— 首版因此把
#   segment_boundary_lsn / demux_backlog_recovery 当宿主机套件跑，
#   报出一堆 `psql: command not found`，看起来像套件坏了，其实是 runner 坏了。
INSIDE="shard_auto_init multi_table_isolation segment_boundary_lsn bulk_insert_recovery crash_recovery demux_backlog_recovery corrupt_segment_recovery enospc_recovery"

# ── ① 全局独占锁：整轮持有，不是逐套件 ──────────────────────────
exec 8>/tmp/p6_exit_global.lock
if ! flock -n 8; then
  echo "FATAL: 另一轮 P6 出口门禁正在运行（/tmp/p6_exit_global.lock 被占）"
  echo "       各套件自己的 flock 只锁同名套件，锁不住整批——R-P6-5 就是这么来的。"
  exit 99
fi

# ── ② 空闲判据：锚定，绝不自匹配 ────────────────────────────────
wait_idle() {
  local n=0
  # ★ `^bash \./test_` 只匹配"直接 bash 起来的套件脚本"；本脚本自己的命令行是
  #   `bash run_p6_exit.sh ...`，不会自匹配。松成 'test_.*\.sh' 会把自己算进去，
  #   于是永远在等自己空闲（实测白等 30 分钟）。
  while pgrep -f "^bash \./test_[a-z_0-9]*\.sh" >/dev/null 2>&1; do
    n=$((n+1)); [[ $((n % 12)) -eq 1 ]] && echo "  [等待] 集群上仍有套件在跑…"
    sleep 5
  done
}

# ── ③ 日志轮转 ──────────────────────────────────────────────────
rotate_logs() {
  local ts sz
  sz=$(docker exec -i "$C" bash -lc 'du -cb /work/pg-cluster-data/*/*.log 2>/dev/null | tail -1 | cut -f1' 2>/dev/null)
  ts=$(date +%Y%m%d-%H%M%S)
  docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" bash -lc '
    for d in coordinator worker1 worker2 worker3 worker4 worker5 worker6 worker7 worker8; do
      /work/pg-install/bin/pg_ctl -D /work/pg-cluster-data/$d -m fast stop -w -t 30 >/dev/null 2>&1
    done' >/dev/null 2>&1
  # ★★ 纪元复位必须夹在"全停"与"拉起"之间。
  #   boot_blocked 是 **postmaster 启动时** stat 一次写进共享内存的；只删文件
  #   不重启，当前实例照样拒发号。而这里刚好是全批唯一一次"所有节点都停着"的
  #   时刻，删完再拉起，整批就以一个干净纪元起跑。
  docker exec -i -u postgres "$C" bash -lc \
    'rm -f /work/pg-cluster-data/coordinator/pg_tso_boot' >/dev/null 2>&1
  docker exec -i -u postgres "$C" bash -lc "
    n=0
    for f in /work/pg-cluster-data/*/*.log /work/pg-cluster-data/*.log; do
      [ -f \"\$f\" ] || continue
      mv \"\$f\" \"\${f%.log}.archive-${ts}.txt\" && n=\$((n+1))
    done
    echo \"  [净场] 归档 \$n 个日志（改名保留，不删除）\"" 2>/dev/null
  docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" bash -lc '
    for d in coordinator worker1 worker2 worker3 worker4 worker5 worker6 worker7 worker8; do
      /work/pg-install/bin/pg_ctl start -D /work/pg-cluster-data/$d \
        -l /work/pg-cluster-data/$d/pg.log -w -t 40 >/dev/null 2>&1
    done' >/dev/null 2>&1
  echo "  [净场] 健康检查扫描面：$(( ${sz:-0} / 1048576 )) MB → $(docker exec -i "$C" bash -lc 'du -cb /work/pg-cluster-data/*/*.log 2>/dev/null | tail -1 | cut -f1' 2>/dev/null | awk '{printf "%d", $1/1024}') KB"
}

# ── ④ 陈旧 pg_shard_xid 槽位回收（R-P6-4）────────────────────────
reap_xid_slots() {
  local ts total=0 p d live n
  ts=$(date +%Y%m%d-%H%M%S)
  for p in $(seq 5433 5440); do
    d="worker$((p - 5432))"
    live=$(PS "$p" -Atc "SELECT string_agg(oid::text,' ') FROM pg_class WHERE relkind IN ('r','i','t')" </dev/null 2>/dev/null)
    [[ -n "$live" ]] || continue
    n=$(docker exec -i -u postgres "$C" bash -lc "
      mkdir -p /work/reaped-shard-xid-$ts/$d; k=0
      for f in \$(ls /work/pg-cluster-data/$d/pg_shard_xid/ 2>/dev/null); do
        case \" $live \" in *\" \$f \"*) ;; *) mv /work/pg-cluster-data/$d/pg_shard_xid/\$f /work/reaped-shard-xid-$ts/$d/ 2>/dev/null && k=\$((k+1));; esac
      done; echo \$k" 2>/dev/null)
    total=$((total + ${n:-0}))
  done
  echo "  [净场] 回收陈旧分配器槽位 $total 个（上限 64，跨过即该节点所有打标验收在登记那步死）"
}

# ── ⑤⑥ 残表 + raft 复位 + GUC ──────────────────────────────────
# ★★ 每套件之前把 9 个节点都拉起来。
#
# 这是"长批次里靠后的套件数字不可信"的**通解**。实测链路：ops 时代那批套件
# 按**旧的 3 节点布局**（master/worker1/worker2）停节点，在 9 节点集群上
# **停了不复原** —— 此后每一套都以 `connection refused` 全线红，而报错内容与
# 被测对象毫无关系（tso_si_p3 的 17/21 就是这么来的，我一度误判成 TSO 纪元问题）。
#
# 不检查节点存活的门禁，等于把"上一套有没有留下烂摊子"当成被测系统的一部分。
ensure_nodes_up() {
  local d n=0
  # ★★ 光 `pg_ctl start` 是不够的：`kill -9` 打死的 postmaster 在**本容器里会
  #    变成僵尸**（PID 1 不回收，项目记录里早有这一条），而 PostgreSQL 判断
  #    "陈旧锁文件"用的是 `kill(pid,0)` —— 对僵尸照样返回成功，于是它认定
  #    "另一个 postmaster 还在跑"并拒绝启动：
  #      FATAL: lock file "postmaster.pid" already exists
  #      FATAL: lock file "/tmp/.s.PGSQL.5433.lock" already exists
  #    **两个锁都要清**（数据目录里的 + /tmp 下的 socket 锁），少清一个照样起不来。
  #
  #    判活判据取 /proc/<pid>/cmdline 里含 "postgres" —— 僵尸的 cmdline 是**空的**，
  #    这一条能把"真活着"和"僵尸"分开，而 kill(pid,0) 不能。
  docker exec -i -u postgres "$C" bash -lc '
    alive() { [ -r /proc/$1/cmdline ] && tr "\0" " " < /proc/$1/cmdline | grep -q postgres; }
    for d in coordinator worker1 worker2 worker3 worker4 worker5 worker6 worker7 worker8; do
      f=/work/pg-cluster-data/$d/postmaster.pid
      [ -f "$f" ] || continue
      pid=$(head -1 "$f"); alive "$pid" && continue
      rm -f "$f"
    done
    for p in 5432 5433 5434 5435 5436 5437 5438 5439 5440; do
      f=/tmp/.s.PGSQL.$p.lock
      [ -f "$f" ] || continue
      pid=$(head -1 "$f"); alive "$pid" && continue
      rm -f "$f" /tmp/.s.PGSQL.$p
    done' >/dev/null 2>&1

  for d in coordinator worker1 worker2 worker3 worker4 worker5 worker6 worker7 worker8; do
    if ! docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" \
           /work/pg-install/bin/psql -h /tmp -p "$((5432 + $(echo "$d" | grep -oE '[0-9]+$' || echo 0)))" \
           -U postgres -d postgres -X -Atc 'SELECT 1' </dev/null >/dev/null 2>&1; then
      docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" \
        /work/pg-install/bin/pg_ctl start -D "/work/pg-cluster-data/$d" \
        -l "/work/pg-cluster-data/$d/pg.log" -w -t 40 >/dev/null 2>&1 && n=$((n+1))
    fi
  done
  [[ "$n" -gt 0 ]] && echo "  [净场] 拉起了 $n 个掉线节点（上一套留下的）"
  return 0
}

# ── ⑧ TSO 纪元按需复位（T6.8 补）──────────────────────────────
#
# ★★ 这是 T6.7 只做了一半的一件事。原来的 pre_suite 只给 tso_si_p3 复位纪元，
#   可**所有要分片 xid 的套件**都吃这个前置：shard_xid_p1 / shard_clog_p2 /
#   shard_gating_p4 / dtx_* / shard_vacuum_* / 全部 P6 套件。T6.7 那轮能过，
#   是因为起跑时协调者恰好还没服务过、标记不在 —— 又是一次"顺序依赖冒充前置"，
#   正是那段注释自己批评过的东西。
#
# 实测代价（2026-09-04 T6.8 首轮）：协调者在 T6.3b 重编 PG 时被重启过，
# shard_xid_p1 当场 **18/27**，27 条红全是同一句
#   `TSO 检测到上个纪元的 boot 标记（pg_tso_boot），拒绝发号`
# —— 与被测内容毫无关系。
#
# ★ 判据不能用 partdist_tso_status()：它在 pg_partdist--1.0.sql 里**声明了，
#   但本集群的扩展从没重建过，库里根本没有这个函数**（T6.8 审计出的
#   "声明 vs 实装"差集之一）。
#
# 也不能只看"标记文件在不在"：`boot_blocked` 是 postmaster **启动那一刻**
# stat 一次的结果。正常服务过的实例自己也会写下标记，此时文件在、却没被拦。
#
# 精确且零依赖的判据：**标记的 mtime 早于协调者的 postmaster 启动时刻**
# ⇔ 标记在启动时就已存在 ⇔ boot_blocked。只在真被拦时才动手，
# 不给每套件白搭一次协调者重启。
tso_epoch_reset_if_blocked() {
  local cdata i mtime started
  cdata=$(PS 5432 -Atc "SHOW data_directory" </dev/null 2>/dev/null)
  [[ -n "$cdata" ]] || return 0
  mtime=$(DEX stat -c %Y "$cdata/pg_tso_boot" </dev/null 2>/dev/null)
  [[ -n "$mtime" ]] || return 0            # 标记不在：本来就是干净纪元
  started=$(PS 5432 -Atc "SELECT floor(extract(epoch FROM pg_postmaster_start_time()))::bigint" </dev/null 2>/dev/null)
  [[ "$started" =~ ^[0-9]+$ ]] || return 0
  [[ "$mtime" -lt "$started" ]] || return 0 # 标记是本实例自己写的：没被拦
  DEX rm -f "$cdata/pg_tso_boot" </dev/null 2>/dev/null
  DEX /work/pg-install/bin/pg_ctl -D "$cdata" -m fast -l "$cdata/pg.log" \
      restart -w -t 40 </dev/null >/dev/null 2>&1
  for i in $(seq 1 30); do
    [[ "$(PS 5432 -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break
    sleep 1
  done
  echo "  [净场] TSO 纪元被拦，已删标记并重启协调者（换新纪元）"
}

# ── ⑨ 孤儿 Citus prepared 事务（T6.8 补，实证）──────────────────
#
# ★★ 被中断的一批会留下 `citus_*` 的 2PC 未决事务，而 §9.2 **已经关掉了
#   Citus 原生 2PC 恢复** —— 于是没有任何人会来清它们，那个 worker 上的 DDL
#   从此永久阻塞。
#
# 实测（2026-09-04 T6.8）：worker1 上留了 4 个 `citus_0_*`，
# `shard_identity_p0` 建引用表那一步直接挂满 600s 超时（0/0 早退）。
# 现场指纹很好认：`pg_blocking_pids()` 返回 **0** —— 等的是一个**没有后端**
# 的事务，也就是 prepared 事务。清掉之后同一套件立刻回到基线 10/0。
#
# 只回滚 `citus_%`：那是 Citus 内部 DDL 的 2PC，协调者没提交就等于没发生
# （本次核对过协调者 `pg_dist_shard` 里确实没有对应表）。
# **本方案自己的 DTX（gid 前缀不同）一概不碰** —— 它的判决在 raft 里，
# 只能由恢复守护按决议闭合，验收脚本无权替它做主。
purge_orphan_prepared() {
  local p n total=0 g
  for p in $PORTS; do
    n=$(PS "$p" -Atc "SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'citus\\_%'" </dev/null 2>/dev/null)
    [[ "$n" =~ ^[0-9]+$ && "$n" -gt 0 ]] || continue
    for g in $(PS "$p" -Atc "SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'citus\\_%'" </dev/null 2>/dev/null); do
      PS "$p" -q -c "ROLLBACK PREPARED '$g'" </dev/null >/dev/null 2>&1 && total=$((total+1))
    done
  done
  [[ "$total" -gt 0 ]] && echo "  [净场] 回滚孤儿 Citus 2PC $total 笔（Citus 原生恢复已关，没人会清）"
  return 0
}

scrub() {
  local p round
  ensure_nodes_up
  tso_epoch_reset_if_blocked
  purge_orphan_prepared
  for p in $PORTS; do
    # ★ replay_trust_local_segments 也要复位（T6.8-2）：它现在**真的有效力**了
    #   —— 开着就允许 replay_catchup 不给上界、直接追到本地段末尾。
    #   12 个套件在自己开头把它设 on 且从不还原，不在这里复位的话，
    #   后面套件的 fail-closed 守卫会被前面那套悄悄遮掉。需要它的套件自己开。
    PS "$p" -q -c "ALTER SYSTEM RESET pg_partdist.shard_relids;
                   ALTER SYSTEM RESET pg_partdist.tso_conninfo;
                   ALTER SYSTEM RESET pg_partdist.allow_replica_access;
                   ALTER SYSTEM RESET pg_partdist.replay_trust_local_segments;
                   ALTER SYSTEM RESET pg_partdist.replay_debug_trace;" </dev/null >/dev/null 2>&1
    PS "$p" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
  done
  # 两轮：单轮按端口顺序复位时，未复位的节点会把组心跳回已复位的节点上
  for round in 1 2; do
    for p in $PORTS; do
      PS "$p" -q -c "SELECT partdist.pg_raft_group_reset();
                     DELETE FROM partdist.partition_map
                      WHERE partition_id NOT IN (SELECT shardid FROM pg_dist_shard);" </dev/null >/dev/null 2>&1
    done
    sleep 2
  done
}

# ★ 真的清，不只是数 —— 头注释里承诺了 ⑤，只数不清就是文不对题。
#   判据取夹具命名前缀，只清 public 下的普通表；不碰 partdist/citus 的东西。
#   **跑前清一次**即可：跑中清会踩到正在用的夹具。
purge_leftovers() {
  local p n total=0
  for p in $(seq 5433 5440); do
    n=$(PS "$p" -Atc "DO \$do\$ DECLARE r record; k int := 0; BEGIN
        FOR r IN SELECT c.relname FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace
                  WHERE ns.nspname='public' AND c.relkind='r'
                    AND (c.relname ~ '^t[0-9]{2}[a-z]' OR c.relname ~ '^p[0-9]') LOOP
          BEGIN EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', r.relname); k := k + 1;
          EXCEPTION WHEN OTHERS THEN NULL; END;
        END LOOP; RAISE NOTICE 'dropped %', k; END \$do\$;" </dev/null 2>&1 \
        | grep -oE 'dropped [0-9]+' | grep -oE '[0-9]+')
    total=$((total + ${n:-0}))
  done
  # ★ 分布式表 / 引用表 **从 worker 上 DROP 不掉**，必须走协调者。
  #   实测残留 `p5repl`（P5 期的引用表）在 8 个 worker 上各留一张，
  #   上面那圈 worker 侧 DROP 一张也清不掉，"清理后残表"永远停在 8。
  local c
  c=$(PS "$COORD_PORT" -Atc "DO \$do\$ DECLARE r record; k int := 0; BEGIN
      FOR r IN SELECT c.relname FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace
                WHERE ns.nspname='public' AND c.relkind='r'
                  AND (c.relname ~ '^t[0-9]{2}[a-z]' OR c.relname ~ '^p[0-9]') LOOP
        BEGIN EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE', r.relname); k := k + 1;
        EXCEPTION WHEN OTHERS THEN NULL; END;
      END LOOP; RAISE NOTICE 'dropped %', k; END \$do\$;" </dev/null 2>&1 \
      | grep -oE 'dropped [0-9]+' | grep -oE '[0-9]+')
  echo "  [净场] 清理夹具残表 $total 张（worker 侧）+ ${c:-0} 张（协调者侧，分布式/引用表）"
}

leftovers() {
  local p n=0 v
  for p in $(seq 5433 5440); do
    v=$(PS "$p" -Atc "SELECT count(*) FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace
                       WHERE ns.nspname='public' AND c.relkind='r'
                         AND (c.relname ~ '^t[0-9]{2}[a-z]' OR c.relname ~ '^p[0-9]')" </dev/null 2>/dev/null)
    [[ "$v" =~ ^[0-9]+$ ]] && n=$((n+v))
  done
  echo "$n"
}

# ── 主流程 ──────────────────────────────────────────────────────
echo "================= P6 出口门禁 ================="
echo "容器=$C   结果目录=$OUT"
# ★ 函数必须**先定义后调用**：首版把这两个定义插在主流程之后，
#   bash 直接报 `prepare_env: command not found` 并继续跑 —— 于是"修好了"
#   的适配一次都没生效，而结果看起来和没修一模一样。

# ── 环境适配：让旧套件跑得起来，而不是去改 8 个旧套件 ────────────
prepare_env() {
  # ★ 旧布局兼容：crash_recovery / bulk_insert_recovery / multi_table_isolation
  #   写死 /work/pg-cluster-data/**master**，而本环境叫 coordinator。
  #   做一个软链，比逐个改旧套件的路径安全得多（改了它们在原环境里就坏了）。
  docker exec -i -u postgres "$C" bash -lc '
    [ -e /work/pg-cluster-data/master ] || ln -s coordinator /work/pg-cluster-data/master
    echo "  [适配] master -> coordinator 软链就绪"' 2>/dev/null

  # ★ enospc_recovery 要一个注入用的 .so，缺了它连自检都过不去
  docker exec -i "$C" bash -lc '
    if [ ! -f /tmp/libenospc_inject.so ] && [ -f /work/pg-partdist-src/enospc_inject.c ]; then
      gcc -shared -fPIC -o /tmp/libenospc_inject.so /work/pg-partdist-src/enospc_inject.c -ldl 2>/dev/null \
        && echo "  [适配] libenospc_inject.so 已构建" \
        || echo "  [适配] libenospc_inject.so 构建失败（enospc_recovery 会自检退出）"
    else
      echo "  [适配] libenospc_inject.so 已在位或无源码"
    fi' 2>/dev/null
}

# ★ 逐套件前置：原来那 9 套的**运行顺序里藏着未言明的前置条件**，
#   归一之后必须显式化，否则"换个顺序就红"这种事会一直发生。
pre_suite() {
  case "$1" in
    shard_xid_p1)
      # ★★ 这套 P1 期的负向断言（"严格模式读/写被拦截"）**只在遗留模式下成立**。
      #   ShardAccessGate 的 strict 分支判据是 `TsoGetStartTs() == 0` —— T3.6 已把
      #   strict 从"一切拦截"收紧为"无 ts 读 / 无 gxid 写才拦"，TSO 一配置就自动
      #   取号、strict 全放行。所以 tso_conninfo 非空时这几条必红，且连带
      #   "负向用例未破坏数据"也红（那条 INSERT 真的写进去了）。
      #   T6.7 那轮能过，只是因为跑到它时 worker1 的 tso_conninfo 恰好是空的
      #   —— 又一次"顺序依赖冒充前置"（实测 T6.8 首轮 41/4，四条红同源）。
      PS "${WPORT:-5433}" -q -c "ALTER SYSTEM RESET pg_partdist.tso_conninfo;" </dev/null >/dev/null 2>&1
      PS "${WPORT:-5433}" -q -c "SELECT pg_reload_conf();" </dev/null >/dev/null 2>&1
      ;;
    tso_si_p3)
      # TSO 计数器在共享内存 + boot 防呆标记：纪元必须干净，否则整套取号全红。
      # run_p5_exit.sh 靠"把它垫底 + 前面重启协调者"绕过，那是顺序依赖，不是前置。
      local cdata
      cdata=$(PS 5432 -Atc "SHOW data_directory" </dev/null 2>/dev/null)
      [[ -n "$cdata" ]] && DEX rm -f "$cdata/pg_tso_boot" </dev/null 2>/dev/null
      DEX /work/pg-install/bin/pg_ctl -D "$cdata" -m fast -l "$cdata/pg.log" restart -w -t 40 </dev/null >/dev/null 2>&1
      local i
      for i in $(seq 1 30); do [[ "$(PS 5432 -Atc 'SELECT 1' </dev/null 2>/dev/null)" == "1" ]] && break; sleep 1; done
      ;;
  esac
}


wait_idle
rotate_logs
prepare_env
reap_xid_slots
echo "  [净场] 起跑前残表：$(leftovers)"
purge_leftovers
echo "  [净场] 清理后残表：$(leftovers)"

WITH_OPS=0
ARGS=()
for a in "$@"; do
  if [[ "$a" == "--with-ops" ]]; then WITH_OPS=1; else ARGS+=("$a"); fi
done
[[ "$WITH_OPS" == "1" ]] && SUITES+=("${OPS_SUITES[@]}")
WANT=("${ARGS[@]+"${ARGS[@]}"}")
declare -a NAMES=() RESULTS=()
run_one() {  # <名字> <超时> <出身>
  local name=$1 tmo=$2 era=$3 log="$OUT/$name.log" env_pfx=""
  case " $INSIDE " in *" $name "*) env_pfx="inside" ;; esac
  scrub
  pre_suite "$name"
  printf "  [%-4s] %-26s " "$era" "$name"
  if [[ "$env_pfx" == "inside" ]]; then
    # 容器内执行模型：没有 CONTAINER 变量，直接在容器里跑
    timeout "$tmo" docker exec -i -u postgres -e HOME=/var/lib/postgresql "$C" \
      bash "/work/pg-partdist-src/tests/test_$name.sh" > "$log" 2>&1
  else
    CONTAINER="$C" timeout "$tmo" bash "$T/test_$name.sh" > "$log" 2>&1
  fi
  local r
  # ★ 两种汇总格式并存：宿主机套件用 `PASS=n FAIL=n`，容器内那 8 套里有的用
  #   `PASSED: n  FAILED: n`（shard_auto_init）。首版只认前者，把后者报成
  #   "无汇总行"，看起来像超时 —— **格式不统一本身就是一笔 harness 债**，
  #   收编的第一步是先能正确读出它们的结论。
  r=$(grep -oE 'PASS=[0-9]+ +FAIL=[0-9]+' "$log" | tail -1)
  if [[ -z "$r" ]]; then
    local pp ff
    pp=$(grep -oE 'PASSED: *[0-9]+' "$log" | tail -1 | grep -oE '[0-9]+')
    ff=$(grep -oE 'FAILED: *[0-9]+' "$log" | tail -1 | grep -oE '[0-9]+')
    [[ -n "$pp" && -n "$ff" ]] && r="PASS=$pp FAIL=$ff"
  fi
  if [[ -z "$r" ]]; then
    r="PASS=$(grep -c '  PASS ' "$log") FAIL=$(grep -c '  FAIL ' "$log") (无汇总行:超时或早退)"
  fi
  echo "$r"
  NAMES+=("$name"); RESULTS+=("$r")
}

for entry in "${SUITES[@]}"; do
  set -- $entry
  name=$1 tmo=$2 era=$3
  if [[ ${#WANT[@]} -gt 0 ]]; then
    case " ${WANT[*]} " in *" $name "*) ;; *) continue ;; esac
  fi
  [[ -f "$T/test_$name.sh" ]] || { echo "  [skip] $name（脚本不存在）"; continue; }
  run_one "$name" "$tmo" "$era"
done

echo "================= 汇总 ================="
tp=0; tf=0; bad=0
for i in "${!NAMES[@]}"; do
  p=$(echo "${RESULTS[$i]}" | grep -oE 'PASS=[0-9]+' | cut -d= -f2)
  f=$(echo "${RESULTS[$i]}" | grep -oE 'FAIL=[0-9]+' | cut -d= -f2)
  tp=$((tp + ${p:-0})); tf=$((tf + ${f:-0}))
  [[ "${f:-0}" != "0" || "${RESULTS[$i]}" == *"无汇总行"* ]] && { printf "  ✗ %-26s %s\n" "${NAMES[$i]}" "${RESULTS[$i]}"; bad=$((bad+1)); }
done
echo "  收尾残表：$(leftovers)"
echo "  套件 ${#NAMES[@]} 套，其中不干净 $bad 套；断言合计 PASS=$tp FAIL=$tf"
echo "  逐套日志：$OUT"
[[ "$bad" -eq 0 ]]
