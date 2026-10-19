# 云上三机 TPC-C：单副本 ShardPG 对单机 PostgreSQL、三机单副本原版 Citus

三台 4 核 8 GB 云服务器，标准规模 100 仓（装载后约 10 GB）：

- **ShardPG**：三机单副本（`REPLICAS=0`，单副本快速提交，无备库、无 COORD_HA），数据分到三台，每台约 3.5 GB，放得进内存。
- **单机 PG**：PGDG 编译的 PostgreSQL 16.14（官方 `postgres:16.14` 镜像，或在云上用 PGDG 包现做的 `sp-pgdg:16.14`），10 GB 数据放在一台 8 GB 机器上，放不进内存。

- **原版 Citus**：Citus 13.1.0（与 lab 对照同一份源码）+ 同一个 PG 16.14，三机单副本（`shard_replication_factor = 1`），布局、端口、函数下推与 ShardPG 相同。

三套用同一份事务函数和 pgbench 脚本（单机、Citus 套件分别由 `scripts/make_native.sh`、`scripts/make_citus.sh` 从本套件生成），资源参数也是同一份 `common.conf`。

两边的库都是 `-E UTF8 --no-locale`（C 排序规则）的前提是单机 PG 用的是现做的 `sp-pgdg` 镜像。官方 `postgres` 镜像初始化出来的库是 `en_US.utf8`：lab 的单机对照就是这样，比 ShardPG 节点的 C 多了排序规则开销。

| 机器 | ShardPG | 单机 PG 轮次 | Citus 轮次 |
|---|---|---|---|
| A | 协调者 n1 :5432 + worker n2 :5433 + 客户端容器 | 单机 PG :5432 + 客户端容器（默认 `NATIVE_HOST=A`） | 协调者 c1 :5432 + worker c2 :5433 + 客户端容器 |
| B | worker n3 :5434 | 空闲 | worker c3 :5434 |
| C | worker n4 :5435 | 空闲 | worker c4 :5435 |

三个 worker 的端口必须互不相同。原因是建分片组的 `partdist.raft_replicate_table_shards[_bulk]` 只按端口号认 worker：端口相同时，所有分片都被当成第一台的，选主永远等不到。

三套不同时跑：每个 `deploy_<系统>.sh` 都会先停掉另外两套，`bench.sh` 起跑前也会检查。

## 文件

| 文件 | 作用 |
|---|---|
| `cloud.env.example` | 三台 IP、网段、目录、规模、终端序列（复制成 `cloud.env` 再改）。其它脚本都读它 |
| `common.conf` | 两边共用的参数（shared_buffers 2GB、max_wal_size 4GB、RC……）。**只改这里** |
| `shardpg.conf` / `coord.conf` | 只给 ShardPG 节点的参数 / 只给协调者的参数（协调者和 n2 同机，缓冲池压到 512MB） |
| `check.sh` | 部署前自检：ssh、docker、核数、内存、磁盘、镜像、端口 |
| `install_images.sh <包>` | 把镜像包装到三台，核对内核都是 16.14 |
| `build_runtime_image.sh <pg-install 包>` | 云上拉不到 Docker Hub、跨境传不动整包镜像时，在 A 上现做两个镜像并分发到 B / C（见第 1 步的第二种做法） |
| `runtime/` | 现做镜像用的 Dockerfile（ShardPG 运行镜像、PGDG 单机镜像、Citus 镜像）和单机 / Citus 镜像共用的入口脚本 |
| `citus.conf` | 只给 Citus 节点的参数（与 lab 的 Citus 对照相同：`shared_preload_libraries`、prepared 上限、嵌套分布式执行） |
| `build_citus_image.sh <Citus 源码包>` | 以单机镜像为底编译原版 Citus（先核对源码包 sha256），分发到 B / C |
| `deploy_citus.sh` | 停另两套 → 生成 Citus 套件 → 按 ShardPG 的布局起 4 个节点 → 接线 → 装载（函数下推） |
| `deploy_shardpg.sh [--conf-only]` | ShardPG 全新部署 + 装载 + 打印各 worker 分到几个仓 |
| `deploy_native.sh` | 停 ShardPG → 生成单机套件 → 起单机 PG → 装载 |
| `bench.sh <shardpg\|native\|citus> [终端...]` | 热身一轮，再按终端数 × `REPEAT` 跑；每轮采三台 vmstat |
| `report.sh` | 每轮一行（tpmC、延迟、重试、中止、违例、三台 CPU / iowait），再按终端数列三套的中位数和比值 |
| `stop_all.sh` | 删掉本套件的全部容器（数据和结果保留） |

## 步骤

### 0. 机器准备（三台都做）

- Ubuntu 22.04，同一可用区、同一 VPC；数据盘（≥ 80 GB 云 SSD，三台同型号）挂到 `/data`。
- 安全组：VPC 网段内全部放通，公网只给你自己的 IP 开 22。**数据库都是 trust 免密认证，5432–5435 绝不能对公网开放。**
- 装 docker：`apt-get update && apt-get install -y docker.io`。镜像从镜像包 load，不需要访问 Docker Hub。
- A 免密登录 B、C：在 A 上 `ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519`，再把 `~/.ssh/id_ed25519.pub` 追加到 B、C 的 `~/.ssh/authorized_keys`。

### 1. 本机：导出镜像、打包套件

```bash
# 经隧道从 lab 145 导出（shardpg:prof = 仓库 HEAD 的代码；postgres:16 = 16.14），约 2 GB 未压缩
ssh -p 2222 yyt@127.0.0.1 'docker save shardpg:prof postgres:16 | gzip -1' > shardpg-images.tar.gz
cd ~/shardpg-tpcc-work && tar czf tpcc.tgz tpcc
scp shardpg-images.tar.gz tpcc.tgz root@<A 公网 IP>:/data/
```

lab 上的 `tpcc`、`tpcc-native` 是 `make_mini.sh` 改过的迷你版，不要拿来用。用仓库里的 `tpcc/`（标准规模）。

**第二种做法：网络受限时只传 12 MB。** 云服务器在国内、本机在境外时，跨境带宽只有约 50–130 KB/s，740 MB 的整包要传一两个小时，而国内云又拉不到 Docker Hub。这时只传镜像里真正运行用的 `/work/pg-install`，在 A 上用本机 apt 源现做镜像：

```bash
# 本机：从 lab 导出 pg-install（约 12 MB）
ssh -p 2222 yyt@127.0.0.1 'docker run --rm shardpg:prof tar czf - -C /work pg-install' > pg-install.tar.gz
scp pg-install.tar.gz tpcc.tgz root@<A 公网 IP>:/data/
# A 上（第 2 步填 cloud.env 时把 NATIVE_IMG 改成 sp-pgdg:16.14），代替第 3 步的 install_images.sh：
bash cloud/build_runtime_image.sh /data/pg-install.tar.gz
```

这一步做出两个镜像：

- `shardpg:prof`：Ubuntu 22.04（debootstrap）+ 运行库 + 原样的 `pg-install`，二进制与 lab 镜像逐字节相同。
- `sp-pgdg:16.14`：同一基础 + PGDG 的 `postgresql-16`。小版本从 `pg-install` 里取；PGDG 签名密钥会先核对指纹。

两个镜像都经内网分发到 B、C。在阿里云上整个过程约 3 分钟（2026-10-18 实测）。

### 2. A：解包、填 cloud.env

```bash
mkdir -p /data/sp && cd /data/sp && tar xzf /data/tpcc.tgz
cp tpcc/cloud/cloud.env.example tpcc/cloud/cloud.env
vi tpcc/cloud/cloud.env        # 填 A/B/C 内网 IP、HBA_CIDR、SSH_USER；BASE 保持 /data/sp
```

选哪台当 A：Citus 按 IP 的**字符串**顺序轮流放分片。100 仓、6 分片时，排在中间的那台只分到 28 个仓，另两台分到 34 / 38 个。把排在中间的那台当 A，能给协调者和客户端腾出 CPU。

### 3. A：装镜像、自检

```bash
cd /data/sp/tpcc
bash cloud/install_images.sh /data/shardpg-images.tar.gz
bash cloud/check.sh
```

### 4. ShardPG（建议在 tmux 里跑）

```bash
bash cloud/deploy_shardpg.sh          # 全新部署 + 装载 100 仓；最后打印各 worker 的仓库数和库大小
bash cloud/bench.sh shardpg           # 热身 5 min，再 32/64/128 终端各 2 轮（每轮预热 3 + 测量 10 min）
```

### 5. 单机 PG

```bash
bash cloud/deploy_native.sh           # 停 ShardPG、起单机 PG、装载；打印实际参数，应与 worker 一致
bash cloud/bench.sh native
```

### 6. 原版 Citus（三机单副本）

```bash
# 本机：下载 Citus 13.1.0 源码包（sha256 见 cloud.env 的 CITUS_SHA256），传到 A
curl -fsSL -o citus-13.1.0.tar.gz https://github.com/citusdata/citus/archive/refs/tags/v13.1.0.tar.gz
scp citus-13.1.0.tar.gz root@<A 公网 IP>:/data/
# A 上（要先有单机镜像 NATIVE_IMG）
bash cloud/build_citus_image.sh /data/citus-13.1.0.tar.gz
bash cloud/deploy_citus.sh            # 停另两套、起 4 个 Citus 节点、装载；打印实际参数与各 worker 的仓库数
bash cloud/bench.sh citus
```

Citus 和 ShardPG 用同样的布局、端口和分片数（6），函数下推也一样（`06_delegate.sql`）。Citus 走它自己的 2PC，2PC 恢复保持默认开启；ShardPG 必须关掉这个恢复机制。

### 7. 汇总

```bash
bash cloud/report.sh
```

每轮的明细在 `tpcc/results/<时间>_shardpg-t64-r1/` 和 `/data/sp/tpcc-native/results/<时间>_native-t64-r1/`，包括：

- `summary.txt`
- `pgbench.out` / `pgbench.err`
- `consistency_*.txt`
- `settings_node*.tsv`（实际生效的参数）
- `vmstat_{A,B,C}.txt`
- `nodes.tsv` / `shards.tsv`（ShardPG 监控）

## 时间预估

- 装载：100 个标准仓在 4 核机器上还没测过，估计要几十分钟到一小时，两边各一次。
- `bench.sh`：每套约 85 min（热身 5 min，加上 3 个终端数 × 2 轮 × 13 min）。

## 判读与注意

- **结果有效的前提**：`report.sh` 里每一轮"中止"和"违例"都是 0。中止大于 0，说明出现了不可重试的错误，看该轮的 `pgbench.err`。
- 两边都是先热身再在同一个库上连续跑，终端序列相同。轮次之间有 ±5–10% 的波动，所以看中位数。库在压测中会增长（订单表），两边增长量相当。
- **CPU 列**：A 上同时有客户端（以及 ShardPG 的协调者），所以 A 偏高是正常的。单机 PG 轮次里 B、C 应该接近空闲。单机 PG 的 iowait（`wa`）高，说明数据放不进内存，正是这次要看的场景。
- **内存**：8 GB 下 A 最紧张（协调者的连接数最多）。若 vmstat 的 `free` 掉到底或出现 OOM（`dmesg | grep -i oom`），就把 `TERMINAL_LIST` 降一档。
- **重测 ShardPG 必须重新 `deploy_shardpg.sh`**（整簇重装）。协调者重启后 TSO 会因 `pg_tso_boot` 标记拒绝发号，停掉的节点不能原地再起。
- 改参数只改 `cloud.env` / `common.conf`，再重新 deploy。`tpcc/cluster.env` 和 `conf/out/` 由脚本生成，每次 deploy 都会覆盖。

## 2026-10-18 实测的经验

完整报告在仓库根目录的 `TPCC_CLOUD_REPORT_20261018.md`。

- **客户端单独一台**：在 `cloud.env` 设 `CLIENT_HOST`，部署和压测脚本都在客户端机上运行。数据库机器不必和 pgbench 抢 CPU。
- **协调者单独一台**：设 `COORD_DEDICATED=1`，A 只放协调者，B、C 各一个 worker。协调者单独放时只用 1–1.5 核；和 worker 同放 A 时，A 会先满。
- **规模**：8 GB 机器上，100 仓三机也装不下（压测后每台 4.6–6.2 GB）；60–64 仓时单机装不下，三机勉强装得下。
- **检查点**：`max_wal_size = 4 GB` 时，单机的检查点一个接一个、全靠 WAL 触发。测量窗口最好覆盖至少一个完整的检查点周期（10 分钟以上），否则单轮结果受检查点时机影响很大。
- **不要用 1 分钟窗口**：每轮开头的一致性检查会把缓存冲掉，1 分钟的预热恢复不过来。
- **热身终端数**：默认取终端序列的中间一档，几套系统之间比较时用 `WARMUP_T=` 固定成同一个值。
