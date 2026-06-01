FROM ubuntu:22.04
# 安装所有编译依赖
RUN apt-get update && apt-get install -y \
    build-essential make bison flex libreadline-dev \
    zlib1g-dev libssl-dev libxml2-dev libxslt1-dev \
    libicu-dev libcurl4-openssl-dev liblz4-dev libzstd-dev \
    autoconf automake libtool pkg-config git gdb
# 创建postgres用户
RUN groupadd -r postgres && useradd -r -g postgres postgres
WORKDIR /work
RUN chown -R postgres:postgres /work
USER postgres
CMD ["/bin/bash"]
