#!/usr/bin/env bash
# ============================================================================
# shardpg_demo.sh —— ShardPG 演示启动器（只用于演示，不改动项目任何文件）
#
#   bash shardpg_demo.sh start      准备演示环境：装 demo 函数库、记下将被改动的参数、打开 TSO
#   bash shardpg_demo.sh sql A      打开会话 A（连协调者的交互式 psql，提示符 A>）
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

psqlc() { local port=$1; shift; docker exec -i -u postgres "$C" $BIN/psql -h /tmp -p "$port" -U postgres -d postgres -X "$@"; }
q()     { psqlc "$1" -qAtc "$2" </dev/null 2>/dev/null | tail -1; }
say()   { printf '%s\n' "$*"; }

workers() { q 5432 "SELECT string_agg(nodeport::text, ' ' ORDER BY nodeport) FROM pg_dist_node WHERE noderole='primary' AND groupid<>0 AND isactive"; }

ensure_up() {   # 有被演示停掉的 worker 就拉起来
    local p d
    for p in 5432 $(workers); do
        [[ "$(q $p 'SELECT 1')" == 1 ]] && continue
        d=$(docker exec "$C" bash -c "ls -d /work/pg-cluster-data/*/ | while read x; do grep -q \"^port *= *$p\" \$x/postgresql.conf 2>/dev/null && echo \$x; done" | head -1)
        [[ -z "$d" ]] && { [[ $p == 5432 ]] && d=/work/pg-cluster-data/coordinator || d="/work/pg-cluster-data/worker$((p - 5432))"; }
        say "  拉起 :$p（$d）"
        docker exec -u postgres "$C" $BIN/pg_ctl start -D "$d" -l "$d/pg.log" -o "-p $p" -w -t 60 >/dev/null 2>&1
    done
}

cmd_start() {
    docker inspect -f '{{.State.Running}}' "$C" 2>/dev/null | grep -q true || { say "容器 $C 没在运行"; exit 1; }
    ensure_up
    local W; W=$(workers)
    [[ $(wc -w <<<"$W") -eq 3 ]] || { say "需要 1 协调者 + 3 worker，现在 worker 是：$W"; exit 1; }
    if [[ "$(q 5432 "SELECT count(*) FROM pg_namespace WHERE nspname='demo'")" != 0 ]]; then
        say "上一次演示还没 stop（demo 模式还在），先运行：bash $0 stop"; exit 1
    fi
    local p ng=0 t
    for p in $W; do ng=$((ng + $(q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"))); done
    [[ $ng -eq 0 ]] || { say "worker 上还有 $ng 个数据 Raft 组（别的测试在用这套集群？），不能开始演示"; exit 1; }
    for t in $DEMO_TABLES; do
        [[ "$(q 5432 "SELECT count(*) FROM pg_class WHERE relname='$t' AND relnamespace='public'::regnamespace")" == 0 ]] \
            || { say "表 $t 已存在，不能开始演示"; exit 1; }
    done

    say "① 安装 demo 函数库（只装在协调者上）"
    psqlc 5432 -q -v ON_ERROR_STOP=1 < "$HERE/shardpg_demo_functions.sql" >/dev/null || { say "安装失败"; exit 1; }

    say "② 记下演示会改动的参数在各节点 postgresql.auto.conf 里的原样（stop 时原样还原）"
    q 5432 "SELECT demo._save_gucs()" >/dev/null

    say "③ 打开 TSO（全局时间戳服务，跑在协调者上）：删 boot 标记 → 重启协调者 → tso_master=on → 各节点指向它"
    local cd; cd=$(q 5432 "SHOW data_directory")
    docker exec -u postgres "$C" rm -f "$cd/pg_tso_boot"
    docker exec -u postgres "$C" $BIN/pg_ctl -D "$cd" -m fast -l "$cd/pg.log" restart -w -t 60 >/dev/null 2>&1
    for t in $(seq 1 40); do [[ "$(q 5432 'SELECT 1')" == 1 ]] && break; sleep 1; done
    q 5432 "ALTER SYSTEM SET pg_partdist.tso_master = on" >/dev/null
    for p in 5432 $W; do
        q $p "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres'" >/dev/null
        q $p "ALTER SYSTEM SET pg_partdist.tso_lease_ms = 60000" >/dev/null
        q $p "SELECT pg_reload_conf()" >/dev/null
    done
    say "④ 演示期间把 3 台 worker 的 Raft 选举超时放宽到 15 s（2 vCPU 上避免负载抖动误选主；stop 时还原）"
    for p in $W; do q $p "ALTER SYSTEM SET pg_raft.election_timeout_ms = 15000" >/dev/null; q $p "SELECT pg_reload_conf()" >/dev/null; done
    sleep 2
    for p in $W; do q $p "SELECT partdist.partdist_tso_client_start_ts()" >/dev/null; done
    sleep 3
    local ts; ts=$(q 5432 "SELECT partdist.partdist_tso_client_start_ts()")
    [[ "$ts" =~ ^[0-9]+$ ]] || { say "TSO 取号失败"; exit 1; }
    docker exec "$C" test -f /tmp/pagecmp.py || docker cp "$HERE/../tests/pagecmp.py" "$C":/tmp/pagecmp.py
    say "准备好了。打开会话：bash $0 sql A（第二个窗口：bash $0 sql B）；演示函数一览：SELECT * FROM demo.help();"
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
    if [[ "$(q 5432 "SELECT count(*) FROM pg_namespace WHERE nspname='demo'")" == 0 ]]; then
        say "  demo 模式不在（没 start 过，或已经 stop 过）"; return 0
    fi
    local p g t s sids="" tables
    for p in 5432 $W; do
        for g in $(q $p "SELECT string_agg(gid, ' ') FROM pg_prepared_xacts"); do q $p "ROLLBACK PREPARED '$g'" >/dev/null; done
    done
    tables=$(q 5432 "SELECT string_agg(DISTINCT t, ' ') FROM (SELECT tbl t FROM demo.managed UNION SELECT unnest(string_to_array('$DEMO_TABLES', ' '))) x
                     WHERE EXISTS (SELECT 1 FROM pg_class WHERE relname = x.t AND relnamespace = 'public'::regnamespace)")
    for t in $tables; do
        s=$(q 5432 "SELECT string_agg(shardid::text, ' ') FROM pg_dist_shard WHERE logicalrelid = '$t'::regclass")
        sids="$sids $s"
        for p in $W; do for g in $s; do q $p "SELECT partdist.replay_disable('${t}_${g}'::regclass)" >/dev/null; done; done
        q 5432 "SET statement_timeout = '120s'; DROP TABLE IF EXISTS $t" >/dev/null
        say "  删表 $t（分片 $s）"
    done
    for s in 1 2; do     # 拆组 → 删副本残壳，两轮
        for p in $W; do q $p "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0" >/dev/null; done
        sleep 2
        for t in $tables; do for g in $sids; do for p in $W; do
            q $p "SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS ${t}_${g}" >/dev/null
        done; done; done
    done
    sids="$sids $(q 5432 "SELECT string_agg(x::text, ' ') FROM demo.managed, unnest(shardids) x")"
    local ids; ids=$(tr -s ' ' '\n' <<<"$sids" | grep -E '^[0-9]+$' | sort -u | paste -sd, -)
    if [[ -n "$ids" ]]; then
        for p in 5432 $W; do q $p "DELETE FROM partdist.partition_map WHERE partition_id IN ($ids)" >/dev/null; done
        say "  删控制面登记（partition_map）里演示分片的行：$ids"
    fi
    say "  $(q 5432 "SELECT demo._restore_gucs()")（TSO、选举超时）"
    q 5432 "SET citus.enable_ddl_propagation = off; DROP SCHEMA demo CASCADE" >/dev/null
    say "  demo 函数库已删除"
    local ng=0 left
    for p in $W; do ng=$((ng + $(q $p "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0"))); done
    left=$(q 5432 "SELECT count(*) FROM pg_class WHERE relname IN ('$(sed "s/ /','/g" <<<"$DEMO_TABLES")') AND relnamespace='public'::regnamespace")
    say "完成：残留数据 Raft 组 $ng 个，残留演示表 $left 张"
}

case "${1:-}" in
    start) cmd_start ;;
    sql)   shift; cmd_sql "$@" ;;
    stop)  cmd_stop ;;
    *)     sed -n '3,12p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
