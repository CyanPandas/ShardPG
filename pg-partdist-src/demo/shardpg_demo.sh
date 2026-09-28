#!/usr/bin/env bash
# ============================================================================
# shardpg_demo.sh —— ShardPG 演示启动器（只用于演示，不改动项目任何文件）
#
#   bash shardpg_demo.sh start      准备演示环境：装 demo 函数库、记下将被改动的参数、打开 TSO
#   bash shardpg_demo.sh sql A      打开会话 A（连 master 的交互式 psql，提示符 A>）
#   bash shardpg_demo.sh sql B      打开会话 B（第二个窗口，演示并发事务）
#   bash shardpg_demo.sh stop       演示结束：删表、拆组、删登记、参数还原、删 demo 函数库，恢复原环境
#
# 从 Windows 11 PowerShell：
#   ssh -t zhanhao@34.31.210.7 "bash ~/shardpg-test-work/pg-partdist-src/demo/shardpg_demo.sh sql A"
# ============================================================================
set -u
C="${CONTAINER:-pg-test-container}"
BIN=/work/pg-install/bin
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_TABLES="${DEMO_TABLES:-account}"
# 演示期间 worker 的 Raft 选举超时。产品默认 6000；这台 2 vCPU 机器上放宽到 15000,
# 免得负载抖动（共识 tick 偶尔卡 5–9 s）误判主死、冒出多余的选举。想完全按产品默认跑：ELECTION_MS=6000
ELECTION_MS="${ELECTION_MS:-15000}"
# TSO 租约：灌数那一批事务本身要跑几十秒，60 s 的租约会让快照被栅栏作废
# （"分片快照被栅栏作废：本节点已 45000 ms 未能向 TSO 续租"）。演示期间放宽到 5 分钟。
TSO_LEASE_MS="${TSO_LEASE_MS:-300000}"

psqlc() { local port=$1; shift; docker exec -i -u postgres "$C" $BIN/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
q()     { psqlc "$1" -qAtc "$2" </dev/null 2>/dev/null | tail -1; }
qe()    { psqlc "$1" -qAtc "$2" </dev/null 2>&1 | tail -3; }   # 保留错误文本，报错时用
say()   { printf '%s\n' "$*"; }
step()  { [[ "${VERBOSE:-0}" == 1 ]] && printf '%s\n' "$*"; return 0; }   # 只有 VERBOSE=1 才打过程
die()   { printf '启动失败：%s\n' "$1" >&2; [[ -n "${2:-}" ]] && printf '%s\n' "$2" >&2; exit 1; }
taillog() {     # 某个节点起不来时，把它日志最后几行打出来
    local p=$1 d
    d=$(docker exec "$C" bash -c "ls -d /work/pg-cluster-data/*/ | while read x; do grep -q \"^port *= *$p\" \$x/postgresql.conf 2>/dev/null && echo \$x; done" 2>/dev/null | head -1)
    [[ -z "$d" ]] && return 0
    docker exec "$C" bash -c "tail -5 '$d/pg.log' 2>/dev/null" 2>/dev/null
}

workers() { q 5432 "SELECT string_agg(nodeport::text, ' ' ORDER BY nodeport) FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive"; }

ensure_up() {   # 有被演示停掉的 worker 就拉起来
    local p d
    for p in 5432 $(workers); do
        [[ "$(q $p 'SELECT 1')" == 1 ]] && continue
        d=$(docker exec "$C" bash -c "ls -d /work/pg-cluster-data/*/ | while read x; do grep -q \"^port *= *$p\" \$x/postgresql.conf 2>/dev/null && echo \$x; done" | head -1)
        [[ -z "$d" ]] && { [[ $p == 5432 ]] && d=/work/pg-cluster-data/coordinator || d="/work/pg-cluster-data/worker$((p - 5432))"; }
        step "  拉起 :$p（$d）"
        docker exec -u postgres "$C" $BIN/pg_ctl start -D "$d" -l "$d/pg.log" -o "-p $p" -w -t 60 >/dev/null 2>&1
    done
}

cmd_start() {
    docker inspect -f '{{.State.Running}}' "$C" 2>/dev/null | grep -q true \
        || die "容器 $C 没在运行" "先执行：docker start $C"
    ensure_up
    local p down=""
    for p in 5432 $(q 5432 "SELECT string_agg(nodeport::text, ' ' ORDER BY nodeport) FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive"); do
        [[ "$(q $p 'SELECT 1')" == 1 ]] || down="$down :$p"
    done
    [[ -z "$down" ]] || die "这些节点没起来：$down" "$(for p in $down; do taillog "${p#:}"; done)"

    local W NW; W=$(workers); NW=$(wc -w <<<"$W")
    [[ $NW -ge 3 ]] \
        || die "至少需要 3 台 worker，现在只有「$W」" "（演示按 1 master + N worker 自适应，推荐 4 台）"
    if [[ "$(q 5432 "SELECT count(*) FROM pg_namespace WHERE nspname='demo'")" != 0 ]]; then
        die "上一次演示还没收尾（demo 模式还在）" "先执行：bash $0 stop"
    fi
    local ng=0 t n
    for p in $W; do
        n=$(q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0")
        ng=$((ng + ${n:-0}))
    done
    [[ $ng -eq 0 ]] \
        || die "worker 上还有 $ng 个数据 Raft 组（别的测试在用这套集群？）" "先清场再演示：bash $0 stop"
    for t in $DEMO_TABLES; do
        [[ "$(q 5432 "SELECT count(*) FROM pg_class WHERE relname='$t' AND relnamespace='public'::regnamespace")" == 0 ]] \
            || die "表 $t 已存在" "先执行：bash $0 stop（或手工 DROP TABLE $t）"
    done

    # 有一部分 CREATE FUNCTION 会被 Citus 传播到 worker；worker 上若留着旧签名，
    # 下次装库会报 "cannot change return type of existing function"。先清干净。
    for p in $W; do q $p "SET citus.enable_ddl_propagation=off; DROP SCHEMA IF EXISTS demo CASCADE" >/dev/null; done
    step "① 安装 demo 函数库（只装在 master 上）"
    local out
    out=$(psqlc 5432 -q -v ON_ERROR_STOP=1 < "$HERE/shardpg_demo_functions.sql" 2>&1 >/dev/null) \
        || die "装 demo 函数库失败" "$(tail -5 <<<"$out")"

    step "② 记下演示会改动的参数在各节点 postgresql.auto.conf 里的原样（stop 时原样还原）"
    out=$(qe 5432 "SELECT demo._save_gucs()")      # 返回 void：成功时无输出
    [[ -z "${out//[[:space:]]/}" ]] || die "记录参数原样失败" "$out"

    step "③ 打开 TSO（全局时间戳服务，跑在 master 上）：删 boot 标记 → 重启 master → tso_master=on → 各节点指向它"
    local cd; cd=$(q 5432 "SHOW data_directory")
    docker exec -u postgres "$C" rm -f "$cd/pg_tso_boot" 2>/dev/null
    docker exec -u postgres "$C" $BIN/pg_ctl -D "$cd" -m fast -l "$cd/pg.log" restart -w -t 60 >/dev/null 2>&1
    for t in $(seq 1 40); do [[ "$(q 5432 'SELECT 1')" == 1 ]] && break; sleep 1; done
    [[ "$(q 5432 'SELECT 1')" == 1 ]] || die "master 重启后没起来" "$(taillog 5432)"
    q 5432 "ALTER SYSTEM SET pg_partdist.tso_master = on" >/dev/null
    for p in 5432 $W; do
        q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null
        q $p "ALTER SYSTEM SET pg_partdist.tso_lease_ms = $TSO_LEASE_MS" >/dev/null
        q $p "SELECT pg_reload_conf()" >/dev/null
    done
    step "④ 把 3 台 worker 的 Raft 选举超时设为 ${ELECTION_MS} ms（产品默认 6000；2 vCPU 上放宽避免误选主。stop 时还原）"
    for p in $W; do q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = $ELECTION_MS" >/dev/null; q $p "SELECT pg_reload_conf()" >/dev/null; done
    sleep 2
    for p in $W; do q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done
    sleep 3
    local ts; ts=$(qe 5432 "SELECT partdist.partdist_tso_client_start_ts()")
    [[ "$ts" =~ ^[0-9]+$ ]] || die "TSO 取号失败" "$ts"
    docker exec "$C" test -f /tmp/pagecmp.py || docker cp "$HERE/../tests/pagecmp.py" "$C":/tmp/pagecmp.py >/dev/null 2>&1
    # 业务数据文件送进容器，演示里用 \i /tmp/demo_data.sql 读它
    [[ -f "$HERE/demo_data.sql" ]] && docker cp "$HERE/demo_data.sql" "$C":/tmp/demo_data.sql >/dev/null 2>&1
    docker exec -u postgres "$C" chmod 644 /tmp/demo_data.sql 2>/dev/null
    say "准备好了"
}

cmd_sql() {
    local who="${1:-A}"
    exec docker exec -it -u postgres "$C" $BIN/psql -h /tmp -p 5432 -U postgres -d postgres -X \
        -P pager=off -v "PROMPT1=${who}> " -v "PROMPT2=${who}…> " -v VERBOSITY=default -v HISTFILE=/dev/null
}

cmd_stop() {
    say "恢复演示前的环境"
    ensure_up
    local W; W=$(workers)
    local p g t s sids="" tables have_demo killed=0 n
    have_demo=$(q 5432 "SELECT count(*) FROM pg_namespace WHERE nspname='demo'")
    [[ "$have_demo" == 1 ]] || say "  （demo 函数库不在：按缺省清单清理，参数做 RESET 兜底）"

    # 回滚残留的 prepared 事务
    for p in 5432 $W; do
        for g in $(q $p "SELECT string_agg(gid, ' ') FROM pg_prepared_xacts"); do q $p "ROLLBACK PREPARED '$g'" >/dev/null; done
    done

    # 要清的表：函数库在就用它记的，不在就用缺省清单（DEMO_TABLES）
    if [[ "$have_demo" == 1 ]]; then
        tables=$(q 5432 "SELECT string_agg(DISTINCT t, ' ') FROM (SELECT tbl t FROM demo.managed UNION SELECT unnest(string_to_array('$DEMO_TABLES', ' '))) x
                         WHERE EXISTS (SELECT 1 FROM pg_class WHERE relname = x.t AND relnamespace = 'public'::regnamespace)")
    else
        tables=$(q 5432 "SELECT string_agg(x, ' ') FROM unnest(string_to_array('$DEMO_TABLES', ' ')) x
                         WHERE EXISTS (SELECT 1 FROM pg_class WHERE relname = x AND relnamespace = 'public'::regnamespace)")
    fi

    # ★ 先清挡路的会话：演示窗口里留着没结束的事务（BEGIN 之后没 COMMIT/ROLLBACK）会一直
    #   持着演示表的锁，DROP TABLE 就卡在那儿超时 —— 实测一个 idle in transaction 的窗口
    #   把 stop 的 DROP 拖到 120 s 超时，表留了下来、而后面的步骤照常跑完，于是 stop 报"完成"、
    #   下次 start 却说"表 account 已存在"。
    for t in $tables; do
        n=$(q 5432 "SELECT count(pg_terminate_backend(l.pid)) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
                     WHERE c.relname = '$t' AND l.pid <> pg_backend_pid()")
        killed=$((killed + ${n:-0}))
    done
    for p in 5432 $W; do
        n=$(q $p "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
                   WHERE pid <> pg_backend_pid() AND backend_type = 'client backend'
                     AND state = 'idle in transaction' AND now() - state_change > interval '30 seconds'")
        killed=$((killed + ${n:-0}))
    done
    [[ ${killed:-0} -gt 0 ]] && say "  清掉 $killed 个挡路的会话（演示窗口里没结束的事务会锁住演示表）"

    for t in $tables; do
        s=$(q 5432 "SELECT string_agg(shardid::text, ' ') FROM pg_dist_shard WHERE logicalrelid = '$t'::regclass")
        sids="$sids $s"
        for p in $W; do for g in $s; do q $p "SELECT partdist.replay_disable('${t}_${g}'::regclass)" >/dev/null; done; done
        q 5432 "SET statement_timeout = '120s'; DROP TABLE IF EXISTS $t" >/dev/null
        if [[ "$(q 5432 "SELECT count(*) FROM pg_class WHERE relname='$t' AND relnamespace='public'::regnamespace")" == 0 ]]; then
            say "  删表 $t（分片 $s）"
        else
            say "  ⚠ 表 $t 没删掉（120 s 超时）—— 见结尾提示"
        fi
    done

    for s in 1 2; do     # 拆组 → 删副本残壳，两轮
        for p in $W; do q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done
        sleep 2
        for t in $tables; do for g in $sids; do for p in $W; do
            q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${t}_${g}" >/dev/null
        done; done; done
    done
    # 兜底：按名字扫一遍各 worker 上剩下的分片/副本壳表（分片号对不上时也能清掉）
    for t in $tables; do for p in $W; do
        for g in $(q $p "SELECT string_agg(relname, ' ') FROM pg_class WHERE relname LIKE '${t}\_%' AND relkind='r'"); do
            q $p "SELECT partdist.replay_disable('$g'::regclass)" >/dev/null
            q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS $g" >/dev/null
        done
    done; done

    [[ "$have_demo" == 1 ]] && sids="$sids $(q 5432 "SELECT string_agg(x::text, ' ') FROM demo.managed, unnest(shardids) x")"
    local ids; ids=$(tr -s ' ' '\n' <<<"$sids" | grep -E '^[0-9]+$' | sort -u | paste -sd, -)
    if [[ -n "$ids" ]]; then
        for p in 5432 $W; do q $p "DELETE FROM partdist.partition_map WHERE partition_id IN ($ids)" >/dev/null; done
        say "  删控制面登记（partition_map）里演示分片的行：$ids"
    fi

    if [[ "$have_demo" == 1 ]]; then
        say "  $(q 5432 "SELECT demo._restore_gucs()")（TSO、选举超时）"
        q 5432 "SET citus.enable_ddl_propagation = off; DROP SCHEMA demo CASCADE" >/dev/null
    else
        # 没有 saved_guc 可依：把演示会改的那几个 GUC 一律 RESET（演示前的常态就是"没有这几行"）
        for p in 5432 $W; do
            for g in pg_partdist.tso_master pg_partdist.tso_conninfo pg_partdist.tso_lease_ms pg_raft.election_timeout_ms; do
                q $p "ALTER SYSTEM RESET $g" >/dev/null
            done
            q $p "SELECT pg_reload_conf()" >/dev/null
        done
        say "  参数已 RESET（TSO、选举超时）"
    fi
    for p in $W; do q $p "SET citus.enable_ddl_propagation=off; DROP SCHEMA IF EXISTS demo CASCADE" >/dev/null; done
    say "  demo 函数库已删除（master 与各 worker）"

    # P7-N40：把本轮演示留下的打标登记收干净（DROP 现在会自己清，这里是净场兜底）
    local orph=0
    for p in $W; do orph=$((orph + $(q $p "SELECT partdist.shard_mvcc_gc_orphans()"))); done
    [[ $orph -gt 0 ]] && say "  清掉 $orph 个残留打标登记（pg_shard_xid）"

    local ng=0 left shells=0
    for p in $W; do
        ng=$((ng + $(q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0")))
        for t in $tables; do shells=$((shells + $(q $p "SELECT count(*) FROM pg_class WHERE relname LIKE '${t}\_%' AND relkind='r'"))); done
    done
    left=$(q 5432 "SELECT count(*) FROM pg_class WHERE relname IN ('$(sed "s/ /','/g" <<<"$DEMO_TABLES")') AND relnamespace='public'::regnamespace")
    if [[ "$ng" == 0 && "$left" == 0 && "$shells" == 0 ]]; then
        say "完成：已净场（0 个数据 Raft 组、0 张演示表、0 张分片壳表）"
    else
        say "⚠ 没清干净：数据 Raft 组 $ng 个、演示表 $left 张、分片壳表 $shells 张"
        say "  多半是还有会话占着锁。看谁占着："
        say "    SELECT pid, state, now()-state_change AS 闲了多久, query FROM pg_stat_activity WHERE state <> 'idle';"
        say "  处理完再跑一次 bash $0 stop（幂等）。"
        return 1
    fi
}


case "${1:-}" in
    start) cmd_start ;;
    sql)   shift; cmd_sql "$@" ;;
    stop)  cmd_stop ;;
    *)     sed -n '3,12p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
