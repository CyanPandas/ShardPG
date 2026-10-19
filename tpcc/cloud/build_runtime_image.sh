#!/usr/bin/env bash
# 云上拉不到 Docker Hub、整包镜像（约 740 MB）跨境又传不动时用，在 A 上现做两个镜像并经内网分发到 B / C：
#   $IMG        = 本机 apt 源 debootstrap 出的 Ubuntu 22.04 + 运行库 + lab 镜像里的 /work/pg-install（只传它，约 12 MB）
#                 —— 二进制与 lab 的 shardpg:prof 逐字节相同
#   $NATIVE_IMG = 同一基础 + PGDG 的 postgresql-16，小版本取自 pg-install（官方 postgres 镜像装的也是 PGDG 包）
#                 —— 名字不能用官方的 postgres:*，cloud.env 里改成如 sp-pgdg:16.14
# pg-install 包在 lab 上导出：docker run --rm shardpg:prof tar czf - -C /work pg-install > pg-install.tar.gz
# 用法（A 上、root）：bash cloud/build_runtime_image.sh <pg-install.tar.gz>
#   可选环境变量：MIRROR（Ubuntu 源，默认取 /etc/apt/sources.list 第一条）、PGDG_MIRROR（默认 <MIRROR 同站>/postgresql/repos/apt）
source "$(dirname "$0")/lib.sh"
f=${1:?用法：build_runtime_image.sh <pg-install.tar.gz>}
[[ -f "$f" ]] || die "没有 $f"
tar tzf "$f" pg-install/bin/postgres >/dev/null 2>&1 || die "$f 里没有 pg-install/bin/postgres（包不完整？）"
[[ "$NATIVE_IMG" != postgres:* ]] || die "自建的单机镜像别冒用官方名字：cloud.env 里把 NATIVE_IMG 改成如 sp-pgdg:16.14"
MIRROR=${MIRROR:-$(awk '$1 == "deb" { print $2; exit }' /etc/apt/sources.list)}
[[ -n "$MIRROR" ]] || die "从 /etc/apt/sources.list 取不到 apt 源，用 MIRROR=... 指定"
PGDG_MIRROR=${PGDG_MIRROR:-${MIRROR%/ubuntu}/postgresql/repos/apt}
PGDG_FPR=B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8   # PGDG 仓库签名密钥（ACCC4CF8）的指纹
W=$(mktemp -d); trap 'rm -rf "$W"' EXIT

if ! docker image inspect sp-base:jammy >/dev/null 2>&1; then
  log "debootstrap Ubuntu 22.04 基础镜像（源 $MIRROR）"
  command -v debootstrap >/dev/null || DEBIAN_FRONTEND=noninteractive apt-get install -y -qq debootstrap >/dev/null \
    || die "装 debootstrap 失败"
  debootstrap --variant=minbase jammy "$W/rootfs" "$MIRROR" > "$W/debootstrap.log" 2>&1 \
    || { tail -20 "$W/debootstrap.log"; die "debootstrap 失败"; }
  tar -C "$W/rootfs" -c . | docker import - sp-base:jammy >/dev/null || die "docker import 失败"
  rm -rf "$W/rootfs"
fi

log "构建 $IMG"
mkdir -p "$W/sp" && cp "$CLOUD_DIR/runtime/Dockerfile" "$W/sp/" && cp "$f" "$W/sp/pg-install.tar.gz"
docker build --network host --build-arg MIRROR="$MIRROR" -t "$IMG" "$W/sp" > "$W/build.log" 2>&1 \
  || { tail -30 "$W/build.log"; die "docker build $IMG 失败"; }
docker run --rm "$IMG" bash -c '/work/pg-install/bin/postgres --version
  miss=$(find /work/pg-install -type f \( -perm -u+x -o -name "*.so*" \) -exec ldd {} \; 2>/dev/null | grep -c "not found")
  echo "缺失动态库 $miss 个"; [ "$miss" -eq 0 ]' || die "$IMG 有动态库缺失"
PG_VERSION=$(docker run --rm "$IMG" /work/pg-install/bin/postgres --version | grep -oE '[0-9]+\.[0-9]+$')
[[ "$PG_VERSION" == 16.* ]] || die "取不到 pg-install 的版本（得到 '$PG_VERSION'）"

log "构建 $NATIVE_IMG（PGDG postgresql-16 $PG_VERSION，源 $PGDG_MIRROR）"
mkdir -p "$W/native" && cp "$CLOUD_DIR/runtime/Dockerfile.native" "$W/native/Dockerfile" \
  && cp "$CLOUD_DIR/runtime/entrypoint-native.sh" "$W/native/"
curl -fsS -m 60 -o "$W/native/pgdg.asc" "${PGDG_KEY_URL:-https://www.postgresql.org/media/keys/ACCC4CF8.asc}" \
  || die "取不到 PGDG 签名密钥（用 PGDG_KEY_URL=... 换地址）"
fpr=$(gpg --show-keys --with-colons "$W/native/pgdg.asc" 2>/dev/null | awk -F: '/^fpr/ { print $10; exit }')
[[ "$fpr" == "$PGDG_FPR" ]] || die "PGDG 密钥指纹不对：$fpr"
docker build --network host --build-arg MIRROR="$MIRROR" --build-arg PGDG_MIRROR="$PGDG_MIRROR" \
  --build-arg PG_VERSION="$PG_VERSION" -t "$NATIVE_IMG" "$W/native" > "$W/build-native.log" 2>&1 \
  || { tail -30 "$W/build-native.log"; die "docker build $NATIVE_IMG 失败"; }
nv=$(docker run --rm "$NATIVE_IMG" postgres --version)
echo "  $nv"
[[ "$nv" == *" $PG_VERSION "* || "$nv" == *" $PG_VERSION" ]] || die "$NATIVE_IMG 版本不是 $PG_VERSION"

for h in "${ALL_HOSTS[@]}"; do
  is_local "$h" && continue   # 本机不用分发
  log "分发两个镜像到 $h（内网）"
  docker save "$IMG" "$NATIVE_IMG" | ssh "${SSH_OPTS[@]}" "$SSH_USER@$h" docker load >/dev/null || die "分发到 $h 失败"
done
log "$IMG、$NATIVE_IMG 就绪（三台）"
