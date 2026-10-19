# ShardPG 运行镜像（云上现做）：系统层 = 本机 apt 源 debootstrap 出的 Ubuntu 22.04（与 lab 镜像同一发行版），
# 二进制 = lab 镜像 shardpg:prof 里的 /work/pg-install 原样拷进来。用户 / 环境变量 / CMD 与 lab 镜像一致。
# 由 cloud/build_runtime_image.sh 构建；基础镜像 sp-base:jammy 也由它生成。
FROM sp-base:jammy
ARG MIRROR
# 运行库：按 lab 镜像里 ldd 全部可执行文件和 .so 的结果列的顶层包（其余依赖 apt 自动带上）
RUN printf 'deb %s jammy main\ndeb %s jammy-updates main\ndeb %s jammy-security main\n' "$MIRROR" "$MIRROR" "$MIRROR" \
      > /etc/apt/sources.list \
 && apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      libcurl4 libicu70 libssl3 libreadline8 liblz4-1 libzstd1 zlib1g libgssapi-krb5-2 libldap-2.5-0 libstdc++6 \
 && rm -rf /var/lib/apt/lists/*
RUN groupadd -r postgres && useradd -r -g postgres postgres
ADD pg-install.tar.gz /work/
RUN chown -R postgres:postgres /work
ENV LD_LIBRARY_PATH=/work/pg-install/lib
WORKDIR /work
USER postgres
CMD ["/bin/sleep", "infinity"]
