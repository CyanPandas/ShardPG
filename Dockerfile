FROM ubuntu:22.04
# 安装所有编译依赖
# python3 + python3-psycopg2：perf_latency.sh（生产模拟测试第9项，端到端
# 延迟性能）需要在容器内跑 python3 脚本连接数据库采样延迟。长期开发容器
# 是手动装的，Dockerfile 一直没补，导致全新 clone 出来的容器缺 python3，
# 报 "python3: command not found"。
RUN apt-get update && apt-get install -y \
    build-essential make bison flex libreadline-dev \
    zlib1g-dev libssl-dev libxml2-dev libxslt1-dev \
    libicu-dev libcurl4-openssl-dev liblz4-dev libzstd-dev \
    autoconf automake libtool pkg-config git gdb \
    python3 python3-psycopg2
# 创建postgres用户
RUN groupadd -r postgres && useradd -r -g postgres postgres
WORKDIR /work
RUN chown -R postgres:postgres /work
USER postgres
CMD ["/bin/bash"]
