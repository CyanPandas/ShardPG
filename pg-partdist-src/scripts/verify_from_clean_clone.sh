#!/usr/bin/env bash
# verify_from_clean_clone.sh
#
# 从一个全新 git clone 开始，端到端验证本仓库能否正常工作：
#   clone -> 构建 Docker 镜像 -> 编译安装 pg_partdist ->
#   初始化 3 节点 Citus 集群 (coordinator + 2 worker) -> 运行生产环境模拟测试。
#
# 使用独立的容器名/镜像名，不会影响任何已在运行的开发容器（如
# pg-citus-cluster-container）。测试跑完（无论成功失败）不会自动清理容器/
# 镜像/克隆目录，方便结束后手动进容器查状态、看日志；需要清理时手动执行
# 同目录下的 verify_cleanup.sh。
#
# 用法（在任何一台新机器上都应该零配置直接跑）：
#   bash verify_from_clean_clone.sh [branch]
#   bash verify_cleanup.sh   # 测试结束后需要清理容器/镜像/克隆目录时再跑
#
#   REPO_URL   要验证的仓库地址。默认是不带凭据的公开 HTTPS 地址
#              （https://github.com/CyanPandas/ShardPG.git），ShardPG 是
#              公开仓库，匿名 HTTPS clone 不需要任何 SSH key / token /
#              known_hosts 配置，换一台全新机器也能直接跑。
#              早期版本默认用过 SSH（git@github.com:...），但那需要执行
#              机器上已经配好、且已加到某个有权限的 GitHub 账号的 SSH
#              key——本地开发机上有，其他机器上没有，会报
#              "Host key verification failed" / "无法读取远程仓库"，
#              不满足"一键在任意机器上跑"的要求，已改回 HTTPS 默认值。
#              如果仓库以后转为私有仓库，可以显式传 SSH 或带 token 的
#              HTTPS 覆盖默认值：
#                REPO_URL=git@github.com:CyanPandas/ShardPG.git bash ...
#                REPO_URL=https://<user>:<token>@github.com/CyanPandas/ShardPG.git bash ...
#   [branch]   要验证的分支，默认 shardpg-2.0。
#
# ── 已知的环境搭建坑（本脚本已修复，供以后维护参考）──────────────────────
#   1. pg-install/lib/postgresql/pg_partdist.so 被 .gitignore 排除，clone
#      后不存在。必须在第一次 pg_ctl start 之前完成编译安装，否则
#      shared_preload_libraries='citus,pg_partdist' 会导致节点全部起不来
#      (FATAL: could not access file "pg_partdist")。
#   2. libenospc_inject.so（ENOSPC 测试依赖）不在 make install 范围内，
#      需要单独编译：gcc -shared -fPIC -o /tmp/libenospc_inject.so
#      enospc_inject.c -ldl
#   3. 长期开发用的容器手动配置了 ssl=on + 证书，未生成证书的新环境要用
#      citus.node_conninfo = 'sslmode=prefer' 绕过，否则 citus_add_node
#      会报 "server does not support SSL, but SSL was required"。
#   4. psql -c 里塞多条语句会被当成一个隐式事务，ALTER SYSTEM 不能在事务
#      块里跑，必须拆成多次 -c 调用。
#   5. worker 节点设置了 pg_partdist.local_node_id 后，pg_partdist 的
#      ExecutorStart 钩子从进程启动起就是激活状态（与 SQL 层是否已
#      CREATE EXTENSION 无关）。必须先在 worker 上 CREATE EXTENSION
#      pg_partdist 再建 citus，否则 citus_columnar 的内部迁移语句会被钩子
#      拦截去查询尚不存在的 partdist.partition_map 表而报错。
#   6. docker run 必须带 --init。容器 PID 1 是 sleep infinity，所有
#      postgres 进程通过一次性 docker exec 启动后都会被过继给 PID 1。
#      测试脚本里对 worker 做 kill -9 崩溃模拟时，被杀的 postmaster 会变成
#      僵尸进程——sleep infinity 从不 wait()，僵尸永远不会被回收。新
#      postmaster 启动时对 postmaster.pid 里旧 PID 做 kill(pid,0) 存活检测，
#      僵尸进程仍然"存在"，于是报 "lock file already exists / Is another
#      postmaster running?" 并立即拒绝启动，导致该 worker 之后的全部测试
#      都是 Connection refused。--init 会引入一个真正的 init 进程（tini）
#      负责回收僵尸，行为与长期开发容器（PID 1 是常驻 bash，本身就会回收
#      自己的子进程）一致。这不是 pg_partdist 或 PostgreSQL 的 bug。
#   7. verify_continuity_and_crash.sh 的 newest_oids() 和 test_crash_recovery.sh
#      的 get_w1_shard_oids() 都曾经用硬编码 `> 50000` 筛选"新建的分片表
#      OID"，这个阈值只在 OID 计数器已经很高的长期开发容器上成立。在一个
#      刚 initdb 的新鲜集群上 OID 还在几千到几万区间，筛选结果为空，报
#      "OIDS[0]: unbound variable"。已修复：两处都去掉了这个多余的下限，
#      因为查询本身已经用 ORDER BY oid DESC LIMIT 2 取"最新的两个"，不需要
#      额外的绝对阈值。
#   8. postgresql.conf 里必须显式写 port = 5432/5433/5434，不能只靠
#      pg_ctl start -o "-p ...".  -o 参数是一次性命令行覆盖，不会写回
#      postgresql.conf；pg_ctl restart 会读 postmaster.opts 记住上次端口，
#      所以"restart"可以不写端口也正常工作，但个别测试脚本（如
#      test_bulk_insert_recovery.sh 崩溃恢复步骤、test_enospc_recovery.sh
#      的 LD_PRELOAD 重启步骤）在 kill -9 或 fast stop 后用的是不带 -o 的
#      纯 "pg_ctl start"，此时会退回 postgresql.conf 里的端口（默认
#      5432），与 coordinator 抢占同一端口，报 "Address already in use"，
#      pg_ctl start 立即失败。长期开发容器的 postgresql.conf 里手动写了
#      port=xxxx 所以从未暴露这个问题；本脚本已在 initdb 后直接把 port
#      写进三个节点的 postgresql.conf，从根上解决，不需要逐个改测试脚本。
#
# ── 已知不支持的拓扑 ───────────────────────────────────────────────────
#   现有测试脚本（尤其 test_multi_table_isolation.sh 等）对"恰好 2 个
#   worker"有 150+ 处硬编码依赖（不只是断言层面，也有真正的逻辑依赖），
#   无法直接套用在 coordinator+3 worker 等其他拓扑上运行现有测试。

set -uo pipefail

REPO_URL="${REPO_URL:-https://github.com/CyanPandas/ShardPG.git}"
BRANCH="${1:-shardpg-2.0}"

WORKDIR="$(mktemp -d /tmp/pg-partdist-verify.XXXXXX)"
LOGFILE="${WORKDIR}.log"
REPO_DIR="$WORKDIR/ShardPG"

IMAGE_NAME="pg-partdist-verify-env"
CONTAINER_NAME="pg-partdist-verify-container"

PG=/work/pg-install/bin
DATA=/work/pg-cluster-data

exec > >(tee "$LOGFILE") 2>&1

FINAL_STATUS="UNKNOWN"

# 测试跑完不自动清理容器/镜像/克隆目录，方便结束后手动进容器看状态、查
# 日志。需要清理时手动执行 verify_cleanup.sh（同目录下）。
report_status() {
    echo ""
    echo "=== 最终结果: $FINAL_STATUS ==="
    echo "=== 完整日志: $LOGFILE ==="
    echo "=== 容器/镜像/克隆目录未清理，容器名: $CONTAINER_NAME，克隆目录: $REPO_DIR ==="
    echo "=== 需要清理时执行: bash $(dirname "${BASH_SOURCE[0]}")/verify_cleanup.sh ==="
}
trap report_status EXIT

banner() { echo ""; echo "======================================================"; echo "  $1"; echo "======================================================"; }

dexec() { docker exec -u postgres "$CONTAINER_NAME" "$@"; }
dexec_root() { docker exec -u root "$CONTAINER_NAME" "$@"; }
dpsql() { docker exec -u postgres "$CONTAINER_NAME" "$PG/psql" -U postgres "$@"; }

fail_exit() { FINAL_STATUS="FAIL ($1)"; exit 1; }

# 部分网络环境（尤其某些代理/防火墙后面的机器）在 HTTPS clone 时会报
# "GnuTLS recv error (-54): Error in the pull function"——curl 的 GnuTLS
# 后端在 HTTP/2 协商或长连接上抽风，和仓库大小无关（这个分支只有 24 个
# commit、几十 MB）。这里不改全局 git 配置，只在这一次 clone 上用 -c
# 参数强制 HTTP/1.1、加大缓冲区，并失败自动重试几次（很多情况下是网络
# 抖动，重试就过了）。
#
# 如果强制 HTTP/1.1 之后仍然反复报 GnuTLS recv error，大概率不是 HTTP/2
# 协商问题，而是这台机器到 github.com 的链路存在 TCP/MTU 层面的丢包黑洞
# （常见于某些 VPN/代理网络，ICMP 被墙导致 PMTU 发现失效，git 智能 HTTP
# 协议的长连接大包传输容易触发）。这种情况下换一种传输方式往往能绕过：
# 用 curl 走 codeload.github.com 下载分支 tarball（单次简单 GET + 可续
# 传），不走 git smart-http 协议的 pack 协商，再本地解包。这里作为 git
# clone 重试 3 次仍失败后的兜底方案，不是默认路径。
tarball_fallback_clone() {
    local url=$1 branch=$2 dest=$3
    # 从 REPO_URL 提取 owner/repo（支持 https://github.com/owner/repo.git 或 owner/repo）
    local owner_repo
    owner_repo=$(echo "$url" | sed -E 's#^(git@|https://)github\.com[:/]##; s#\.git$##')
    local tar_url="https://codeload.github.com/${owner_repo}/tar.gz/refs/heads/${branch}"
    local tmp_tar
    tmp_tar=$(mktemp)
    echo "  尝试兜底方案：curl 下载 tarball ($tar_url) ..."
    if ! curl -fL --retry 5 --retry-delay 3 -C - -o "$tmp_tar" "$tar_url"; then
        rm -f "$tmp_tar"
        return 1
    fi
    rm -rf "$dest"
    mkdir -p "$dest"
    if ! tar -xzf "$tmp_tar" -C "$dest" --strip-components=1; then
        rm -f "$tmp_tar"
        return 1
    fi
    rm -f "$tmp_tar"
    return 0
}

robust_git_clone() {
    local url=$1 branch=$2 dest=$3 tries=0
    while [ $tries -lt 3 ]; do
        tries=$((tries+1))
        rm -rf "$dest"
        if git -c http.version=HTTP/1.1 -c http.postBuffer=524288000 \
               -c http.lowSpeedLimit=1000 -c http.lowSpeedTime=60 \
               clone --branch "$branch" --single-branch "$url" "$dest"; then
            return 0
        fi
        echo "  clone 第 $tries 次失败，${tries}/3 ..."
        sleep 3
    done
    echo "  git clone 重试 3 次仍失败，改用 tarball 方式..."
    tarball_fallback_clone "$url" "$branch" "$dest"
}

# ════════════════════════════════════════════════════════════════
banner "阶段 1 — 全新 clone ${BRANCH} 分支"
# ════════════════════════════════════════════════════════════════
robust_git_clone "$REPO_URL" "$BRANCH" "$REPO_DIR" || fail_exit "clone失败（git clone 重试3次 + tarball 兜底均失败，请检查该机器能否访问 github.com/codeload.github.com，或是否需要配置代理：export https_proxy=...）"
echo "clone 完成: $REPO_DIR"

# ════════════════════════════════════════════════════════════════
banner "阶段 2 — 构建 Docker 镜像"
# ════════════════════════════════════════════════════════════════
cd "$REPO_DIR"
docker build -t "$IMAGE_NAME" . || fail_exit "docker build失败"
echo "镜像构建完成: $IMAGE_NAME"

# ════════════════════════════════════════════════════════════════
banner "阶段 3 — 启动容器（独立命名，不影响其他已在运行的容器）"
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
banner "阶段 4 — 编译安装 pg_partdist（必须先于节点启动）"
# ════════════════════════════════════════════════════════════════
docker exec "$CONTAINER_NAME" bash -c "
    cd /work/pg-partdist-src
    make PG_CONFIG=$PG/pg_config
    make install PG_CONFIG=$PG/pg_config
" || fail_exit "pg_partdist 编译失败"
[ -f "$REPO_DIR/pg-install/lib/postgresql/pg_partdist.so" ] || fail_exit "pg_partdist.so 未生成"
echo "pg_partdist.so 已生成并安装"

dexec_root bash -c "gcc -shared -fPIC -o /tmp/libenospc_inject.so /work/pg-partdist-src/enospc_inject.c -ldl" \
    || fail_exit "libenospc_inject.so 编译失败"
[ -n "$(dexec_root bash -c 'test -f /tmp/libenospc_inject.so && echo ok')" ] || fail_exit "libenospc_inject.so 未生成"
echo "libenospc_inject.so 已编译"

# ════════════════════════════════════════════════════════════════
banner "阶段 5 — 初始化 3 节点集群 (coordinator + 2 worker)"
# ════════════════════════════════════════════════════════════════
dexec "$PG/initdb" -D "$DATA/master"  --encoding=UTF8 || fail_exit "master initdb失败"
dexec "$PG/initdb" -D "$DATA/worker1" --encoding=UTF8 || fail_exit "worker1 initdb失败"
dexec "$PG/initdb" -D "$DATA/worker2" --encoding=UTF8 || fail_exit "worker2 initdb失败"
echo "3 个数据目录已 initdb"

dexec bash -c "cat >> $DATA/master/postgresql.conf <<'EOF'
port = 5432
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
EOF"
dexec bash -c "cat >> $DATA/worker1/postgresql.conf <<'EOF'
port = 5433
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
pg_partdist.local_node_id = 1
EOF"
dexec bash -c "cat >> $DATA/worker2/postgresql.conf <<'EOF'
port = 5434
shared_preload_libraries = 'citus,pg_partdist'
max_prepared_transactions = 200
pg_partdist.local_node_id = 2
EOF"
echo "postgresql.conf 已配置"

dexec "$PG/pg_ctl" start -D "$DATA/master"  -l "$DATA/master/pg.log"  -o '-p 5432' -w -t 30 \
    || { dexec cat "$DATA/master/pg.log"; fail_exit "master 启动失败"; }
dexec "$PG/pg_ctl" start -D "$DATA/worker1" -l "$DATA/worker1/pg.log" -o '-p 5433' -w -t 30 \
    || { dexec cat "$DATA/worker1/pg.log"; fail_exit "worker1 启动失败"; }
dexec "$PG/pg_ctl" start -D "$DATA/worker2" -l "$DATA/worker2/pg.log" -o '-p 5434' -w -t 30 \
    || { dexec cat "$DATA/worker2/pg.log"; fail_exit "worker2 启动失败"; }
echo "3 个节点已启动 (5432/5433/5434)"

dpsql -p 5432 -d postgres -c "CREATE EXTENSION citus;" || fail_exit "coordinator CREATE EXTENSION citus失败"
dpsql -p 5432 -d postgres -c "ALTER SYSTEM SET citus.node_conninfo = 'sslmode=prefer';" \
    || fail_exit "设置 citus.node_conninfo 失败"
dpsql -p 5432 -d postgres -c "SELECT pg_reload_conf();" || fail_exit "pg_reload_conf失败"
dpsql -p 5432 -d postgres -c "SELECT citus_set_coordinator_host('localhost', 5432);" || fail_exit "citus_set_coordinator_host失败"

dpsql -p 5433 -d postgres -c "CREATE EXTENSION pg_partdist;" || fail_exit "worker1 CREATE EXTENSION pg_partdist失败"
dpsql -p 5433 -d postgres -c "CREATE EXTENSION citus;" || fail_exit "worker1 CREATE EXTENSION citus失败"
dpsql -p 5434 -d postgres -c "CREATE EXTENSION pg_partdist;" || fail_exit "worker2 CREATE EXTENSION pg_partdist失败"
dpsql -p 5434 -d postgres -c "CREATE EXTENSION citus;" || fail_exit "worker2 CREATE EXTENSION citus失败"

dpsql -p 5432 -d postgres -c "SELECT citus_add_node('localhost', 5433);" || fail_exit "citus_add_node worker1失败"
dpsql -p 5432 -d postgres -c "SELECT citus_add_node('localhost', 5434);" || fail_exit "citus_add_node worker2失败"
echo "Citus 已配置：coordinator + 2 worker 全部注册"

dpsql -p 5432 -d postgres -c "CREATE EXTENSION pg_partdist;" || fail_exit "coordinator CREATE EXTENSION pg_partdist失败"
echo "pg_partdist 扩展已在全部 3 节点创建"

echo ""
echo "验证 3 节点集群状态："
dpsql -p 5432 -d postgres -At -c "SELECT nodename, nodeport, isactive FROM pg_dist_node ORDER BY nodeport;" \
    || fail_exit "无法查询 pg_dist_node，集群状态异常"

# ════════════════════════════════════════════════════════════════
banner "阶段 6 — 运行生产环境模拟测试"
# ════════════════════════════════════════════════════════════════
# run_production_sim.sh 硬编码 CONTAINER="pg-citus-cluster-container"，
# 测试 9（端到端延迟性能）又是单独的 perf_latency.sh，在宿主机上跑
# （host_cmd=true），它自己也有一份独立的 CONTAINER="pg-citus-cluster-
# container" 硬编码，不会被 run_production_sim.sh 的 sed 覆盖。如果不
# 单独改这一份，perf_latency.sh 就会去连一个不存在（或者错误地连到宿主机
# 上其他同名容器）的容器，报 "container is not running"。两处都只在
# 【克隆副本】里改指向本次的独立容器名，不影响仓库里的真实脚本。
sed -i "s/CONTAINER=\"pg-citus-cluster-container\"/CONTAINER=\"$CONTAINER_NAME\"/" \
    "$REPO_DIR/pg-partdist-src/run_production_sim.sh" \
    "$REPO_DIR/pg-partdist-src/perf_latency.sh"

bash "$REPO_DIR/pg-partdist-src/run_production_sim.sh"
PROD_RC=$?

if [ "$PROD_RC" -eq 0 ]; then
    FINAL_STATUS="SUCCESS — 全新 clone 的 3 节点集群上生产模拟测试全部通过"
else
    FINAL_STATUS="FAIL (生产模拟测试未全部通过, exit=$PROD_RC)"
fi

exit "$PROD_RC"
