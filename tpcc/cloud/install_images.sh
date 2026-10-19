#!/usr/bin/env bash
# 把 lab 导出的镜像包（docker save shardpg:prof postgres:16 | gzip）装到三台：A 本地 load，
# B / C 先 scp 到 $BASE 再 load；单机 PG 镜像补打 $NATIVE_IMG 标签；最后核对版本。
# 用法：bash cloud/install_images.sh <镜像包.tar.gz>
source "$(dirname "$0")/lib.sh"
f=${1:?用法：install_images.sh <镜像包.tar.gz>}
[[ -f "$f" ]] || die "没有 $f"
f=$(realpath "$f")
for i in "${!ALL_HOSTS[@]}"; do
  h=${ALL_HOSTS[$i]}; path=$f
  if ! is_local "$h"; then
    log "${ALL_LABELS[$i]} $h：传镜像包"
    on "$h" "mkdir -p $BASE" && put "$h" "$f" "$BASE/" || die "传到 $h 失败"
    path="$BASE/$(basename "$f")"
  fi
  log "${ALL_LABELS[$i]} $h：docker load"
  on "$h" "gunzip -c $path | docker load" || die "$h 上 docker load 失败"
  # lab 的单机对照镜像是 postgres:16（= 16.14）；打上带小版本的标签，避免以后误拉到别的小版本
  on "$h" "docker image inspect $NATIVE_IMG >/dev/null 2>&1 || docker tag postgres:16 $NATIVE_IMG" \
    || die "$h 上没有 postgres:16，打不了 $NATIVE_IMG 标签"
  sp=$(on "$h" "docker run --rm $IMG /work/pg-install/bin/postgres --version")
  np=$(on "$h" "docker run --rm $NATIVE_IMG postgres --version")
  echo "  ShardPG：$sp"; echo "  单机 PG：$np"
  [[ "$sp" == *" 16.14"* && "$np" == *" 16.14"* ]] || die "$h 上内核版本不是 16.14，两边不可比"
done
log "三台镜像就绪"
