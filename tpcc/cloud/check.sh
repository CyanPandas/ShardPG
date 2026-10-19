#!/usr/bin/env bash
# 部署前自检（在客户端机上跑，只读）：各台的 ssh / docker / 核数 / 内存 / 数据盘空间 / 镜像 / 端口。
# 用法：bash cloud/check.sh
source "$(dirname "$0")/lib.sh"

bad=0
warn() { echo "  !! $*"; bad=1; }
for i in "${!ALL_HOSTS[@]}"; do
  h=${ALL_HOSTS[$i]}; echo "== ${ALL_LABELS[$i]} $h"
  on "$h" true || { warn "ssh $SSH_USER@$h 不通（A 要能免密登录 B / C）"; continue; }
  on "$h" "docker info >/dev/null 2>&1" || { warn "docker 不可用（没装 / 当前用户不在 docker 组）"; continue; }
  on "$h" "mkdir -p $BASE" || warn "建不了 $BASE"
  read -r cpu mem disk < <(on "$h" "echo \$(nproc) \$(awk '/MemTotal/{printf \"%.1f\", \$2/1048576}' /proc/meminfo) \$(df -BG --output=avail $BASE | tail -1 | tr -dc 0-9)")
  echo "  核数 $cpu  内存 ${mem} GB  $BASE 可用 ${disk} GB"
  if [[ " ${HOSTS[*]} " == *" $h "* ]]; then
    [[ "${disk:-0}" -ge 40 ]] || warn "$BASE 所在盘可用空间不足 40 GB（单机库 60 仓装载约 7 GB、压测中还会涨，外加 WAL）"
  fi
  on "$h" "command -v swapon >/dev/null && swapon --show --noheadings | grep -q ." && echo "  （开了 swap：内存吃紧时会变慢而不是 OOM，结果里留意 vmstat 的 si/so）"
  imgs="$IMG $NATIVE_IMG ${CITUS_IMG:-}"; [[ " ${HOSTS[*]} " == *" $h "* ]] || imgs=$IMG   # 客户端机只要 $IMG
  for img in $imgs; do
    on "$h" "docker image inspect $img >/dev/null 2>&1" && echo "  镜像 $img 在" || warn "缺镜像 $img（先跑 install_images.sh）"
  done
  for p in $(ports_on "$h"); do
    if on "$h" "(exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null"; then
      who=$(on "$h" "docker ps --format '{{.Names}}' | grep -E '^$PREFIX-' | tr '\n' ' '")
      echo "  端口 $p 已有进程在听（本套件正在运行的容器：${who:-无}；不是本套件的就换端口）"
    fi
  done
done
echo "== 客户端条件（$CLIENT_HOST）"
is_local "$CLIENT_HOST" || warn "本脚本要在客户端机 $CLIENT_HOST 上运行"
[[ "$(realpath -m "$TPCC_DIR")" == "$(realpath -m "$BASE")"/* ]] || echo "  （提示）套件不在 $BASE 下：$TPCC_DIR —— 能用，但结果会写在系统盘"
for h in "${ALL_HOSTS[@]}"; do on "$h" "command -v vmstat >/dev/null" || warn "$h 上没有 vmstat（apt install procps）"; done
[[ $bad -eq 0 ]] && echo "自检通过" || { echo "自检有问题（见上面 !! 行）"; exit 1; }
