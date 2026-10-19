#!/usr/bin/env bash
# cloud/*.sh 的公共部分：读 cloud.env，封装"在某台机器上执行 / 传文件 / 在客户端容器里跑"。
# 部署 / 压测脚本在客户端机 CLIENT_HOST 上执行（客户端容器在那台；默认 = A）；镜像构建脚本在 A 上执行。
# 目标机器是本机就本地跑，否则走 ssh。由其它脚本 source。
set -uo pipefail

CLOUD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TPCC_DIR="$(cd "$CLOUD_DIR/.." && pwd)"
CLOUD_ENV="${CLOUD_ENV:-$CLOUD_DIR/cloud.env}"
[[ -f "$CLOUD_ENV" ]] || { echo "缺 $CLOUD_ENV（从 cloud.env.example 复制一份改）" >&2; exit 2; }
# shellcheck disable=SC1090
source "$CLOUD_ENV"

PREFIX=${PREFIX:-sp}
NATIVE_DIR="$BASE/tpcc-native"     # 单机 PG 对照套件（deploy_native.sh 用 make_native.sh 生成）
CITUS_DIR="$BASE/tpcc-citus"       # 原版 Citus 对照套件（deploy_citus.sh 用 make_citus.sh 生成）
CLIENT="$PREFIX-client"            # 客户端容器：/t = ShardPG 套件，/tn = 单机 PG 套件，/tc = Citus 套件
CLIENT_HOST=${CLIENT_HOST:-$A}      # 压测客户端所在机器（内网 IP）；单独一台时标作 D
HOSTS=("$A" "$B" "$C"); LABELS=(A B C)                       # 数据库机器
ALL_HOSTS=("${HOSTS[@]}"); ALL_LABELS=("${LABELS[@]}")       # 再加上单独的客户端机
if [[ " ${HOSTS[*]} " != *" $CLIENT_HOST "* ]]; then ALL_HOSTS+=("$CLIENT_HOST"); ALL_LABELS+=(D); fi
# 三套系统的容器名（$PREFIX- 之后的部分，正则）：同一时刻只允许一套在跑
declare -A SYS_CONTAINERS=([shardpg]='n[0-9]+' [native]='native' [citus]='c[0-9]+')
# ShardPG / Citus 节点：node_id → 所在机器 / 端口 / 角色（两套布局相同）。worker 端口互不相同（见 cloud.env.example 的 WORKER_PORT 说明）
#   COORD_DEDICATED=0（默认）：A = 协调者 + worker，B、C 各一个 worker（3 个 worker）
#   COORD_DEDICATED=1        ：A 只放协调者，B、C 各一个 worker（2 个 worker）
#   COORD_ON_CLIENT=1        ：协调者放在客户端机 CLIENT_HOST 上，A、B、C 各一个 worker（3 个 worker）
if [[ "${COORD_ON_CLIENT:-0}" == 1 ]]; then
  NODE_ID_LIST=(1 2 3 4)
  NODE_HOST_LIST=("$CLIENT_HOST" "$A" "$B" "$C")
  NODE_PORT_LIST=("$COORD_PORT" "$WORKER_PORT" "$((WORKER_PORT + 1))" "$((WORKER_PORT + 2))")
  NODE_ROLE_LIST=(coordinator worker worker worker)
elif [[ "${COORD_DEDICATED:-0}" == 1 ]]; then
  NODE_ID_LIST=(1 2 3)
  NODE_HOST_LIST=("$A" "$B" "$C")
  NODE_PORT_LIST=("$COORD_PORT" "$WORKER_PORT" "$((WORKER_PORT + 1))")
  NODE_ROLE_LIST=(coordinator worker worker)
else
  NODE_ID_LIST=(1 2 3 4)
  NODE_HOST_LIST=("$A" "$A" "$B" "$C")
  NODE_PORT_LIST=("$COORD_PORT" "$WORKER_PORT" "$((WORKER_PORT + 1))" "$((WORKER_PORT + 2))")
  NODE_ROLE_LIST=(coordinator worker worker worker)
fi
COORD_NODE_HOST=${NODE_HOST_LIST[0]}   # ShardPG / Citus 协调者所在机器
# 两边共用的资源参数文件（deploy_*.sh 用）；某一版要改公共参数时另写一份，用 COMMON_CONF=... 指定
COMMON_CONF=${COMMON_CONF:-$CLOUD_DIR/common.conf}
# ports_on <host>：本套件在这台机器上要用的端口
ports_on() { local h=$1 k ps=""
  for k in "${!NODE_ID_LIST[@]}"; do [[ "${NODE_HOST_LIST[$k]}" == "$h" ]] && ps+=" ${NODE_PORT_LIST[$k]}"; done
  [[ "$h" == "$NATIVE_HOST" && " $ps " != *" $NATIVE_PORT "* ]] && ps+=" $NATIVE_PORT"
  echo $ps; }

log() { echo "[$(date '+%F %T')] $*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10)
# 首次连接自动记下 B / C 的主机密钥（老版本 OpenSSH 不认 accept-new，就不加）
ssh -G -o StrictHostKeyChecking=accept-new localhost >/dev/null 2>&1 && SSH_OPTS+=(-o StrictHostKeyChecking=accept-new)
# is_local <ip>：这个地址是不是本机的
LOCAL_IPS=" $(ip -4 -o addr show 2>/dev/null | awk '{ split($4, a, "/"); printf "%s ", a[1] }')"
is_local() { [[ "$LOCAL_IPS" == *" $1 "* ]]; }
# on <host> <命令串>
on() { local h=$1; shift
  if is_local "$h"; then bash -c "$*"; else ssh -n "${SSH_OPTS[@]}" "$SSH_USER@$h" "$*"; fi; }
# put <host> <本地文件> <目标路径>
put() { local h=$1 src=$2 dst=$3
  if is_local "$h"; then
    [[ "$(realpath -m "$src")" == "$(realpath -m "$dst")" ]] || cp -r "$src" "$dst"
  else scp -q -r "${SSH_OPTS[@]}" "$src" "$SSH_USER@$h:$dst"; fi; }

# 客户端容器（CLIENT_HOST 上，即运行本脚本的机器）。以宿主机用户身份 exec，结果文件归宿主机用户所有。
# 已在跑但缺挂载（老版本建的，没有 /tc）就重建 —— 它只是个 sleep，没有状态
ensure_client() {
  is_local "$CLIENT_HOST" || die "部署 / 压测脚本要在客户端机 $CLIENT_HOST 上运行（客户端容器在那台）"
  docker image inspect "$IMG" >/dev/null 2>&1 || die "本机（客户端机）没有镜像 $IMG"
  mkdir -p "$TPCC_DIR/results" "$NATIVE_DIR/results" "$CITUS_DIR/results" || die "建结果目录失败"
  if docker ps --format '{{.Names}}' | grep -qx "$CLIENT"; then
    [[ " $(docker inspect -f '{{range .Mounts}}{{.Destination}} {{end}}' "$CLIENT") " == *" /tc "* ]] && return 0
  fi
  docker rm -f "$CLIENT" >/dev/null 2>&1
  docker run -d --name "$CLIENT" --network host -v "$TPCC_DIR:/t" -v "$NATIVE_DIR:/tn" -v "$CITUS_DIR:/tc" \
    "$IMG" sleep infinity >/dev/null || die "起客户端容器 $CLIENT 失败"
}
cx() { docker exec -u "$(id -u):$(id -g)" -e HOME=/tmp "$CLIENT" bash -c "$*"; }
# cpsql <host> <port> <psql 参数...>（SQL 可从标准输入给）
cpsql() { local h=$1 p=$2; shift 2
  docker exec -i -u "$(id -u):$(id -g)" -e HOME=/tmp "$CLIENT" /work/pg-install/bin/psql \
    -h "$h" -p "$p" -U postgres -d postgres -X -v ON_ERROR_STOP=1 "$@"; }

# 正在运行的本套件容器（某台机器上），用于互斥检查
running_on() { on "$1" "docker ps --format '{{.Names}}' | grep -E '^$PREFIX-($2)\$'" 2>/dev/null; }
# others_running <系统>：各台上其它两套系统还在跑的容器（空 = 没有）。协调者可能在客户端机上，所以查全部机器
others_running() { local s h out=""
  for s in "${!SYS_CONTAINERS[@]}"; do
    [[ $s == "$1" ]] && continue
    for h in "${ALL_HOSTS[@]}"; do out+=$(running_on "$h" "${SYS_CONTAINERS[$s]}" | sed "s|\$| ($h)|" | tr '\n' ' '); done
  done
  echo "$out"; }
# stop_others <系统>：停掉其它两套系统的容器（数据保留）。ShardPG 停了不能原地再起（协调者重启后 TSO 拒绝发号）
stop_others() { local s h names
  for s in "${!SYS_CONTAINERS[@]}"; do
    [[ $s == "$1" ]] && continue
    for h in "${ALL_HOSTS[@]}"; do
      names=$(running_on "$h" "${SYS_CONTAINERS[$s]}" | tr '\n' ' ')
      [[ -n "${names// /}" ]] || continue
      log "停 $s 的容器 $names（$h；数据保留）"
      on "$h" "docker stop $names >/dev/null" || die "停 $names 失败"
    done
  done; }
# remove_own <系统>：删掉本系统在各台上的旧节点容器（换布局后旧位置上的容器不会被 node.sh 覆盖）；数据目录保留
remove_own() { local h names
  for h in "${ALL_HOSTS[@]}"; do
    names=$(on "$h" "docker ps -a --format '{{.Names}}' | grep -E '^$PREFIX-(${SYS_CONTAINERS[$1]})\$' | tr '\n' ' '" 2>/dev/null)
    [[ -n "${names// /}" ]] || continue
    log "删除 $1 的旧节点容器 $names（$h；数据目录保留）"
    on "$h" "docker rm -f $names >/dev/null" || die "删除 $names 失败"
  done; }
# conf_args <配置文件...>：key = value 行转成 postgres 的 -c 参数串（每个参数 %q 转义，经 ssh 再解析一次也不散）
conf_args() { local line v args=""
  while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*([a-z_.]+)[[:space:]]*=[[:space:]]*(.*[^[:space:]])[[:space:]]*$ ]] || continue
    v=${BASH_REMATCH[2]}; v=${v#\'}; v=${v%\'}
    args+=" -c $(printf %q "${BASH_REMATCH[1]}=$v")"
  done < <(cat "$@" | grep -vE '^[[:space:]]*(#|$)')
  echo "$args"; }
# hba_file <host> <目录>：在该机器上写一份只放行 HBA_CIDR 的 pg_hba.conf（与 ShardPG 节点 node.sh 写的相同）
hba_file() { on "$1" "mkdir -p $2 && printf 'local all all trust\nhost all all 127.0.0.1/32 trust\nhost all all $HBA_CIDR trust\n' > $2/pg_hba.conf"; }
