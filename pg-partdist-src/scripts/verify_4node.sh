#!/usr/bin/env bash
# verify_4node.sh — 从全新 clone 验证 1 coordinator + 3 worker 拓扑：
# clone -> docker build -> 编译安装 pg_partdist -> 初始化 4 节点 -> 配置 Citus
# -> 一组手动测试用例 -> 清理。
#
# 不跑现有 run_production_sim.sh（已知对"恰好 2 个 worker"有硬编码依赖，
# 4 节点下会产生大量非环境问题的 FAIL，参考 pg-partdist-src/scripts/
# verify_from_clean_clone.sh 头部注释)。这里用手写的手动测试用例覆盖同一批
# 核心能力，验证它们在 3-worker 拓扑下同样成立：
#   阶段 6 — 跨 4 节点分布式表读写
#   阶段 7 — 分区 PartWAL 目录自动创建（懒初始化，首次 DML 触发）
#   阶段 8 — 表间隔离性（两张分布式表的 PartWAL 记录互不串扰）
#   阶段 9 — PartWAL 内容完整性：独立 Perl 解析段文件原始字节 vs SQL 计数交叉验证
#
# 用法（在任何一台新机器上都应该零配置直接跑）：
#   bash verify_4node.sh [branch]
#
#   REPO_URL   要验证的仓库地址，默认不带凭据的公开 HTTPS 地址
#              （https://github.com/CyanPandas/ShardPG.git），不需要 SSH
#              key / token / known_hosts，换一台全新机器也能直接跑。
#              需要指向别的仓库/私有仓库时再显式覆盖。
#
# ── 写手动用例时踩的两个坑（供以后维护参考）───────────────────────────────
#   1. LIKE 'tablename_%' 里表名自带的下划线会被 SQL LIKE 当成"匹配任意
#      单字符"的通配符，不是字面下划线，可能误配/漏配分片表名。
#      worker_shard_oids() 里已经转义成 'tablename\_%'。
#   2. PartWALRecord 头部固定 40 字节，但紧跟 data_len 字节的变长 payload
#      （一次批量 INSERT 常常整批落进同一条底层 WAL 记录，记录数和行数
#      不是 1:1）。不能假设"磁盘字节数 / 40 = 记录数"，必须按头部里的
#      data_len 字段动态跳过 payload 才能正确数出记录数（阶段 9 的 Perl
#      解析器已经这样做，逻辑对齐 src/worker/demux_worker.c 里
#      count_parwal_records 的实现）。

set -uo pipefail

REPO_URL="${REPO_URL:-https://github.com/CyanPandas/ShardPG.git}"
BRANCH="${1:-shardpg-2.0}"

WORKDIR="$(mktemp -d /tmp/pg-partdist-verify4.XXXXXX)"
LOGFILE="${WORKDIR}.log"
REPO_DIR="$WORKDIR/ShardPG"

IMAGE_NAME="pg-partdist-verify4-env"
CONTAINER_NAME="pg-partdist-verify4-container"

HOST_UID="$(id -u)"
HOST_GID="$(id -g)"

PG=/work/pg-install/bin
DATA=/work/pg-cluster-data

exec > >(tee "$LOGFILE") 2>&1

FINAL_STATUS="UNKNOWN"

cleanup() {
    echo ""
    echo "=== 清理阶段 ==="
    if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
        docker exec -u root "$CONTAINER_NAME" \
            chown -R "$HOST_UID:$HOST_GID" /work/pg-install /work/pg-cluster-data /work/pg-partdist-src \
            >/dev/null 2>&1 || true
        docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 && echo "  容器已删除: $CONTAINER_NAME"
    else
        echo "  容器不存在，跳过"
    fi
    docker rmi -f "$IMAGE_NAME" >/dev/null 2>&1 && echo "  镜像已删除: $IMAGE_NAME" || echo "  镜像不存在或已删除"
    rm -rf "$WORKDIR" && echo "  克隆目录已删除: $WORKDIR"
    echo ""
    echo "=== 最终结果: $FINAL_STATUS ==="
}
trap cleanup EXIT

banner() { echo ""; echo "======================================================"; echo "  $1"; echo "======================================================"; }

dexec() { docker exec -u postgres "$CONTAINER_NAME" "$@"; }
dexec_root() { docker exec -u root "$CONTAINER_NAME" "$@"; }
dpsql() { docker exec -u postgres "$CONTAINER_NAME" "$PG/psql" -U postgres "$@"; }

fail_exit() { FINAL_STATUS="FAIL ($1)"; exit 1; }

# 在某个 worker 上取某张分布式表本地分片的 OID 列表。
# citus.override_table_visibility=off 关掉 MX 模式对 pg_class 的可见性
# 过滤，分片表能像普通表一样按 relname 直接查到（比翻 pg_toast 孤儿表更
# 直接，用法取自 test_shard_auto_init.sh）。
worker_shard_oids() {
    local port=$1 tbl=$2
    dpsql -p "$port" -d postgres -At -c "
        SET citus.override_table_visibility TO off;
        SELECT oid FROM pg_class WHERE relname LIKE '${tbl}\_%' AND relkind='r' ORDER BY oid;" 2>/dev/null
}

# 同上，但顺带返回 relname（oid,relname 用 | 分隔），用于按本地分片表名
# 直接查行数。
worker_shard_oid_names() {
    local port=$1 tbl=$2
    dpsql -p "$port" -d postgres -At -c "
        SET citus.override_table_visibility TO off;
        SELECT oid || '|' || relname FROM pg_class
        WHERE relname LIKE '${tbl}\_%' AND relkind='r' ORDER BY oid;" 2>/dev/null
}

# ════════════════════════════════════════════════════════════════
banner "阶段 1 — 全新 clone ${BRANCH} 分支"
# ════════════════════════════════════════════════════════════════
git clone --branch "$BRANCH" --single-branch "$REPO_URL" "$REPO_DIR" || fail_exit "clone失败"
echo "clone 完成: $REPO_DIR"

# ════════════════════════════════════════════════════════════════
banner "阶段 2 — 构建 Docker 镜像"
# ════════════════════════════════════════════════════════════════
cd "$REPO_DIR"
docker build -t "$IMAGE_NAME" . || fail_exit "docker build失败"
echo "镜像构建完成: $IMAGE_NAME"

# ════════════════════════════════════════════════════════════════
banner "阶段 3 — 启动容器"
# ════════════════════════════════════════════════════════════════
docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d \
    --init \
    --name "$CONTAINER_NAME" \
    -v "$REPO_DIR/pg-install:/work/pg-install" \
    -v "$REPO_DIR/pg-cluster-data:/work/pg-cluster-data" \
    "$IMAGE_NAME" \
    sleep infinity || fail_exit "docker run失败"
sleep 1
echo "容器已启动: $CONTAINER_NAME"

dexec_root chown -R postgres:postgres /work/pg-install /work/pg-cluster-data
docker cp "$REPO_DIR/pg-partdist-src/." "$CONTAINER_NAME:/work/pg-partdist-src/"
dexec_root chown -R postgres:postgres /work/pg-partdist-src
echo "属主已修正，pg-partdist-src 已拷入容器"

# ════════════════════════════════════════════════════════════════
banner "阶段 4 — 编译安装 pg_partdist"
# ════════════════════════════════════════════════════════════════
docker exec "$CONTAINER_NAME" bash -c "
    cd /work/pg-partdist-src
    make PG_CONFIG=$PG/pg_config
    make install PG_CONFIG=$PG/pg_config
" || fail_exit "pg_partdist 编译失败"
[ -f "$REPO_DIR/pg-install/lib/postgresql/pg_partdist.so" ] || fail_exit "pg_partdist.so 未生成"
echo "pg_partdist.so 已生成并安装"

# ════════════════════════════════════════════════════════════════
banner "阶段 5 — 初始化 4 节点集群 (coordinator + 3 worker)"
# ════════════════════════════════════════════════════════════════
for n in master worker1 worker2 worker3; do
    dexec "$PG/initdb" -D "$DATA/$n" --encoding=UTF8 || fail_exit "$n initdb失败"
done
echo "4 个数据目录已 initdb"

dexec bash -c "cat >> $DATA/master/postgresql.conf <<'EOF'
port = 5432
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
EOF"
i=1
for w in worker1 worker2 worker3; do
    port=$((5432 + i))
    dexec bash -c "cat >> $DATA/$w/postgresql.conf <<EOF
port = $port
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
pg_partdist.local_node_id = $i
EOF"
    i=$((i+1))
done
echo "postgresql.conf 已配置 (master:5432, worker1:5433, worker2:5434, worker3:5435)"

dexec "$PG/pg_ctl" start -D "$DATA/master"  -l "$DATA/master/pg.log"  -w -t 30 \
    || { dexec cat "$DATA/master/pg.log"; fail_exit "master 启动失败"; }
for w in worker1 worker2 worker3; do
    dexec "$PG/pg_ctl" start -D "$DATA/$w" -l "$DATA/$w/pg.log" -w -t 30 \
        || { dexec cat "$DATA/$w/pg.log"; fail_exit "$w 启动失败"; }
done
echo "4 个节点已启动 (5432/5433/5434/5435)"

dpsql -p 5432 -d postgres -c "CREATE EXTENSION citus;" || fail_exit "coordinator CREATE EXTENSION citus失败"
dpsql -p 5432 -d postgres -c "ALTER SYSTEM SET citus.node_conninfo = 'sslmode=prefer';" \
    || fail_exit "设置 citus.node_conninfo 失败"
dpsql -p 5432 -d postgres -c "SELECT pg_reload_conf();" || fail_exit "pg_reload_conf失败"
dpsql -p 5432 -d postgres -c "SELECT citus_set_coordinator_host('localhost', 5432);" || fail_exit "citus_set_coordinator_host失败"

for port in 5433 5434 5435; do
    dpsql -p "$port" -d postgres -c "CREATE EXTENSION pg_partdist;" || fail_exit "worker($port) CREATE EXTENSION pg_partdist失败"
    dpsql -p "$port" -d postgres -c "CREATE EXTENSION citus;" || fail_exit "worker($port) CREATE EXTENSION citus失败"
done

for port in 5433 5434 5435; do
    dpsql -p 5432 -d postgres -c "SELECT citus_add_node('localhost', $port);" || fail_exit "citus_add_node($port)失败"
done
echo "Citus 已配置：coordinator + 3 worker 全部注册"

dpsql -p 5432 -d postgres -c "CREATE EXTENSION pg_partdist;" || fail_exit "coordinator CREATE EXTENSION pg_partdist失败"
echo "pg_partdist 扩展已在全部 4 节点创建"

echo ""
echo "验证 4 节点集群状态："
dpsql -p 5432 -d postgres -At -c "SELECT nodename, nodeport, isactive FROM pg_dist_node ORDER BY nodeport;" \
    || fail_exit "无法查询 pg_dist_node，集群状态异常"

NACTIVE=$(dpsql -p 5432 -d postgres -At -c "SELECT count(*) FROM pg_dist_node WHERE isactive AND nodeport IN (5433,5434,5435);")
[ "$NACTIVE" = "3" ] || fail_exit "期望 3 个 active worker，实际 $NACTIVE"

# ════════════════════════════════════════════════════════════════
banner "阶段 6 — 手动建分布式表，验证跨 4 节点读写"
# ════════════════════════════════════════════════════════════════
dpsql -p 5432 -d postgres -c "
    DROP TABLE IF EXISTS manual_4node_test CASCADE;
    CREATE TABLE manual_4node_test (id int PRIMARY KEY, val text);
    SELECT create_distributed_table('manual_4node_test','id',shard_count=>8);
" || fail_exit "建分布式表失败"

dpsql -p 5432 -d postgres -c "
    INSERT INTO manual_4node_test SELECT i, 'v'||i FROM generate_series(1,200) i;
" || fail_exit "写入失败"

TOTAL=$(dpsql -p 5432 -d postgres -At -c "SELECT count(*) FROM manual_4node_test;")
echo "  coordinator 端总行数: $TOTAL (期望 200)"
[ "$TOTAL" = "200" ] || fail_exit "行数不符：期望200实际$TOTAL"

echo "  每个 worker 上的分片分布 (来自 pg_dist_shard_placement)："
for port in 5433 5434 5435; do
    n=$(dpsql -p 5432 -d postgres -At -c "
        SELECT count(*) FROM pg_dist_shard s JOIN pg_dist_shard_placement sp USING(shardid)
        WHERE s.logicalrelid='manual_4node_test'::regclass AND sp.nodeport=$port;" 2>/dev/null || echo "?")
    echo "    worker($port): $n 个分片"
done

echo "  验证 pg_partdist PartWAL 是否在每个 worker 上都生成了记录："
WROTE_ANY=0
for port in 5433 5434 5435; do
    OIDS=$(worker_shard_oids "$port" manual_4node_test)
    for oid in $OIDS; do
        cnt=$(dpsql -p "$port" -d postgres -At -c "SELECT partdist.count_parwal_records(${oid}::oid);" 2>/dev/null || echo "")
        if [ -n "$cnt" ] && [ "$cnt" -gt 0 ] 2>/dev/null; then
            echo "    worker($port) OID=$oid: $cnt 条 PartWAL 记录"
            WROTE_ANY=1
        fi
    done
done
[ "$WROTE_ANY" = "1" ] || fail_exit "没有任何 worker 上观察到 PartWAL 记录"

# ════════════════════════════════════════════════════════════════
banner "阶段 7 — 分区 PartWAL 目录自动创建（懒初始化）"
# ════════════════════════════════════════════════════════════════
# parwal-2.0：pg_parwal/<oid>/ 目录不是建表时创建，而是在该分片第一次
# DML 时由 ExecutorStart/ProcessUtility 钩子懒创建。这里验证 3 个 worker
# 上都遵守这个行为：建表时目录还不存在，首次 INSERT 后才出现。
dpsql -p 5432 -d postgres -c "
    DROP TABLE IF EXISTS auto_init_test CASCADE;
    CREATE TABLE auto_init_test (id int PRIMARY KEY, val text);
    SELECT create_distributed_table('auto_init_test','id',shard_count=>6);
" || fail_exit "auto_init_test 建表失败"

BEFORE_ANY_DIR=0
for port in 5433 5434 5435; do
    for oid in $(worker_shard_oids "$port" auto_init_test); do
        w=$((port - 5432))
        if dexec test -d "$DATA/worker${w}/pg_parwal/$oid" 2>/dev/null; then
            BEFORE_ANY_DIR=1
        fi
    done
done
[ "$BEFORE_ANY_DIR" = "0" ] || fail_exit "建表阶段就出现了 pg_parwal 目录，懒初始化行为被破坏"
echo "  建表后、写入前：3 个 worker 上均无 pg_parwal 目录（符合懒初始化预期）"

dpsql -p 5432 -d postgres -c "
    INSERT INTO auto_init_test SELECT i, 'v'||i FROM generate_series(1,600) i;
" || fail_exit "auto_init_test 写入失败"

# 按"该本地分片表实际是否有行"来判断目录该不该存在，不假设分片数/行数
# 的分布方式——不管 shard_count、replication_factor 具体怎么分，只要一个
# 本地分片表有数据，它对应的 pg_parwal 目录就必须自动出现。
ALL_CREATED=1
for port in 5433 5434 5435; do
    w=$((port - 5432))
    n_shards=0; n_with_rows=0; n_dirs=0
    for pair in $(worker_shard_oid_names "$port" auto_init_test); do
        oid="${pair%%|*}"; relname="${pair##*|}"
        n_shards=$((n_shards+1))
        rows=$(dpsql -p "$port" -d postgres -At -c "SELECT count(*) FROM \"$relname\";" 2>/dev/null || echo 0)
        has_dir=0
        dexec test -d "$DATA/worker${w}/pg_parwal/$oid" 2>/dev/null && has_dir=1
        if [ "${rows:-0}" -gt 0 ] 2>/dev/null; then
            n_with_rows=$((n_with_rows+1))
            if [ "$has_dir" = "1" ]; then
                n_dirs=$((n_dirs+1))
            else
                echo "    FAIL: worker($port) $relname(oid=$oid) 有 $rows 行但 pg_parwal 目录未自动创建"
                ALL_CREATED=0
            fi
        fi
    done
    echo "    worker($port): 本地分片=$n_shards, 有数据的分片=$n_with_rows, 对应自动创建目录=$n_dirs"
done
[ "$ALL_CREATED" = "1" ] || fail_exit "有数据的分片里，仍有分片未自动创建 pg_parwal 目录"
echo "  写入后：每个 worker 上所有【有数据】的本地分片都自动创建了 pg_parwal 目录"

# ════════════════════════════════════════════════════════════════
banner "阶段 8 — 表间隔离性（两张分布式表的 PartWAL 互不串扰）"
# ════════════════════════════════════════════════════════════════
dpsql -p 5432 -d postgres -c "
    DROP TABLE IF EXISTS iso_a CASCADE;
    CREATE TABLE iso_a (id int PRIMARY KEY, val text);
    SELECT create_distributed_table('iso_a','id',shard_count=>6);
" || fail_exit "iso_a 建表失败"
dpsql -p 5432 -d postgres -c "
    DROP TABLE IF EXISTS iso_b CASCADE;
    CREATE TABLE iso_b (id int PRIMARY KEY, val text);
    SELECT create_distributed_table('iso_b','id',shard_count=>6);
" || fail_exit "iso_b 建表失败"

dpsql -p 5432 -d postgres -c "INSERT INTO iso_a SELECT i, 'a'||i FROM generate_series(1,30) i;" \
    || fail_exit "iso_a 写入失败"

# 记录仅 A 写入后，各 worker 上 A/B 分片的 PartWAL 计数
declare -A CNT_A_AFTER_A CNT_B_AFTER_A
ISO_OK=1
for port in 5433 5434 5435; do
    for oid in $(worker_shard_oids "$port" iso_a); do
        c=$(dpsql -p "$port" -d postgres -At -c "SELECT partdist.count_parwal_records(${oid}::oid);" 2>/dev/null || echo 0)
        CNT_A_AFTER_A["$port:$oid"]="$c"
    done
    for oid in $(worker_shard_oids "$port" iso_b); do
        c=$(dpsql -p "$port" -d postgres -At -c "SELECT partdist.count_parwal_records(${oid}::oid);" 2>/dev/null || echo 0)
        CNT_B_AFTER_A["$port:$oid"]="$c"
        [ "${c:-0}" = "0" ] || { echo "    FAIL: iso_b OID=$oid 在只写 iso_a 后计数=$c (期望0)"; ISO_OK=0; }
    done
done
echo "  只写 iso_a 后：iso_b 所有分片的 PartWAL 计数均为 0（互不串扰，符合预期）"

dpsql -p 5432 -d postgres -c "INSERT INTO iso_b SELECT i, 'b'||i FROM generate_series(1,30) i;" \
    || fail_exit "iso_b 写入失败"

for port in 5433 5434 5435; do
    for oid in $(worker_shard_oids "$port" iso_a); do
        c=$(dpsql -p "$port" -d postgres -At -c "SELECT partdist.count_parwal_records(${oid}::oid);" 2>/dev/null || echo 0)
        before="${CNT_A_AFTER_A["$port:$oid"]:-0}"
        [ "$c" = "$before" ] || { echo "    FAIL: iso_a OID=$oid 在写 iso_b 后计数从 $before 变成 $c（被串扰）"; ISO_OK=0; }
    done
done
[ "$ISO_OK" = "1" ] || fail_exit "表间隔离性验证失败，PartWAL 记录发生了串扰"
echo "  写 iso_b 后：iso_a 所有分片的 PartWAL 计数保持不变（互不串扰，符合预期）"

# ════════════════════════════════════════════════════════════════
banner "阶段 9 — PartWAL 内容完整性：独立解析段文件 vs SQL 计数"
# ════════════════════════════════════════════════════════════════
# PartWALRecord 头部固定 40 字节，但后面紧跟 data_len 字节的变长 payload
# （src/worker/demux_worker.c 的 count_parwal_records 原话："Reads the
# fixed-size header and skips the variable-length data payload"）。
# 一次批量 INSERT 常常整批落在同一条底层 WAL 记录里，所以"记录数"和
# "行数"不是 1:1；不能简单假设定长 40 字节。
# 这里不复用产品自己的计数函数，而是用一段独立的 Perl 按照相同的头部
# 布局重新解析段文件原始字节（含 data_len 变长 payload 的正确跳转），
# 与 count_parwal_records()/verify_partition_wal() 交叉验证，确认磁盘上
# 的段文件内容和 SQL 层看到的完全一致、没有多写/少写/损坏。
CONTENT_OK=1
PARSER='
my ($dir) = @ARGV;
my $MAGIC = 0x50415254;
my $HDR_FMT = "VV VV VV CCCC VV x4";  # 40 字节，字段顺序见 partition_wal_header.h
my $total = 0;
opendir(my $dh, $dir) or exit 0;
for my $f (sort grep { /^[0-9A-Fa-f]{24}$/ } readdir($dh)) {
    open(my $fh, "<:raw", "$dir/$f") or next;
    while (1) {
        my $hdr;
        my $n = read($fh, $hdr, 40);
        last unless defined($n) && $n == 40;
        my ($magic,$pid,$olo,$ohi,$plo,$phi,$rmid,$info,$ver,$flags,$data_len,$xid)
            = unpack($HDR_FMT, $hdr);
        last unless $magic == $MAGIC;
        $total++;
        seek($fh, $data_len, 1) if $data_len > 0;
    }
    close($fh);
}
print "$total\n";
'
for port in 5433 5434 5435; do
    w=$((port - 5432))
    for oid in $(worker_shard_oids "$port" manual_4node_test) $(worker_shard_oids "$port" auto_init_test); do
        sql_cnt=$(dpsql -p "$port" -d postgres -At -c "SELECT partdist.count_parwal_records(${oid}::oid);" 2>/dev/null || echo "")
        [ -z "$sql_cnt" ] && continue
        disk_cnt=$(dexec perl -e "$PARSER" "$DATA/worker${w}/pg_parwal/$oid" 2>/dev/null || echo "?")
        verify=$(dpsql -p "$port" -d postgres -At -c "SELECT partdist.verify_partition_wal(${oid}::oid);" 2>/dev/null || echo "")
        status="OK"
        if [ "$sql_cnt" != "$disk_cnt" ]; then
            status="FAIL(count)"; CONTENT_OK=0
        fi
        if [ "$verify" != "t" ]; then
            status="$status FAIL(verify=$verify)"; CONTENT_OK=0
        fi
        echo "    worker($port) OID=$oid: SQL计数=$sql_cnt 独立解析磁盘计数=$disk_cnt verify=$verify [$status]"
    done
done
[ "$CONTENT_OK" = "1" ] || fail_exit "PartWAL 内容完整性验证失败：独立解析的磁盘记录数与 SQL 计数不一致，或 verify_partition_wal 未通过"
echo "  所有已检查分区：SQL 计数与磁盘段文件字节数完全一致，verify_partition_wal 全部通过"

FINAL_STATUS="SUCCESS — 4 节点 (coordinator+3worker): 集群搭建/扩展注册/跨节点读写/PartWAL自动创建/表间隔离性/PartWAL磁盘内容完整性 全部验证通过（未跑现有 2-worker 硬编码的自动化测试套件）"
exit 0
