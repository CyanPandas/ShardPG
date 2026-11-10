系统功能演示报告：new1自动验收脚本

来源：pg-partdist-src/docs/integration/new1/系统功能演示报告.docx。
编写时业务代码基线：new1 fd784bd，已接入N01、N05、N06、N07。
脚本本身不编译、不部署；按用户约定，本轮未连接数据库或执行功能测试。
运行时核对当时的HEAD，所以后续接入更多编号后仍可用于最终统一验收。

入口

  python3 pg-partdist-src/tests/demo_report/run.py
  python3 pg-partdist-src/tests/demo_report/run.py --help

以上只打印计划/帮助，不连接数据库、不启动或停止节点。
可提前验证配置结构（仍不连接数据库）：

  python3 pg-partdist-src/tests/demo_report/run.py --config /path/to/config.json

测试环境

1. 使用专用、已部署最终new1代码的1 master + 3 worker空测试集群。
   可以同机多端口，也可以跨机器；结果只代表实际运行的部署方式。
   配置中的host必须与node_map登记一致；fa23管理接口按端口定位，因此五节点
   端口须各不相同。连接需要能读取系统状态和执行报告DDL/管理函数的数据库用户。
   主节点及worker的TSO、Raft、Citus、分片MVCC等应按现有部署流程初始化。
   first_shard_coordinator必须保持on，不能为了过测试切回旧事务实现。
2. 另准备worker4独立实例。它尚未加入master的Citus/node_map目录，
   但须有同版二进制、三个扩展和TSO配置；它的pg_raft.peers等于现有列表追加自己。
   在它的本地node_map预置五节点地址（参照fa23验收evidence/add_node.py）。
   可先停止，prepare_worker4_command负责启动；已经就绪时配置空数组。
   脚本随后亲自调用shardpg.add_node，不把任意外部命令成功当作入组通过。
3. Python 3.9+，执行--run时需要psycopg2；计划/帮助/配置检查只依赖标准库。
   口令使用.pgpass/PGPASSFILE；不要把口令放在JSON命令参数中，命令会进入证据日志。
4. 运行机器能直接连接配置中的所有数据库节点。跨机器的pg_ctl、psql和文件校验
   可通过配置中的argv数组调用ssh/docker exec。脚本不会自行猜测实验室容器名。
   config.example.json为/srv/new1-demo下的同机示例，须替换成自己的专用路径。
5. 原演示集群、数据库和容器不应填入这份测试配置。完整范围会真实immediate停主、
   恢复节点、加入第五节点和迁移副本；它不是仅执行只读SQL的检查器。

构建与部署版本校验

完成当前HEAD的正式构建后，在构建源目录及安装产物上生成一次清单：

  python3 pg-partdist-src/tests/demo_report/artifacts.py record \
    --source /path/to/new1-checkout \
    --prefix /path/to/built/pg-install \
    --manifest /path/to/release.json

必须在实际编译完成后使用record，不能用旧二进制生成清单冒充新版本。
该清单是构建者的来源声明与文件摘要，不是可复现构建证明；应一并保留构建日志。
record拒绝覆盖已有清单；检查已跟踪源码无未提交修改。
将同一份清单、artifacts.py及这批产物部署到所有测试节点，然后完整重启。
N07改变了共享内存布局，不能只替换.so后复用旧postmaster。

每个节点的probe_command运行如下命令（可外包ssh/docker argv）：

  python3 /path/to/artifacts.py probe \
    --prefix /path/to/installed/pg-install --manifest /path/to/release.json

脚本检查PostgreSQL、修改过的psql、三个扩展.so和安装SQL的文件摘要，
清单提交必须等于运行脚本的HEAD，postmaster启动时间必须晚于产物mtime及清单时间。
安装目录使用标准lib/postgresql和share/postgresql/extension布局。
所有数据库应在部署同版安装SQL后初始化/升级；文件校验本身不能证明旧库已升级。
各机器时钟需同步；跨机器时间偏差会使版本/重启检查保守失败。

实际执行（本次未执行）

先复制config.example.json并修改连接、控制命令和artifact_manifest绝对路径。
stop_command必须使用pg_ctl stop -m immediate，模拟报告中的突然停机。
start_command应是同一节点同一数据目录的pg_ctl start，不得初始化或清空目录。
psql_command必须使用清单中相同的patched psql，连接配置中的master。

  python3 pg-partdist-src/tests/demo_report/run.py \
    --run --config /path/to/config.json --scope full \
    --output /path/to/results/new1-demo-001

仅跑建删表、事务与收尾（会跳过HA，整体功能结果为INCOMPLETE）：

  python3 pg-partdist-src/tests/demo_report/run.py \
    --run --config /path/to/config.json --scope core \
    --output /path/to/results/new1-core-001

output目录必须不存在，防止覆盖证据。每次生成唯一表名前缀new1demo_<时间>_。
成功后删除自己的九张表和日志展示函数，保留入组后的第五节点与测试集群。
报告未要求缩容，本脚本不自动移除第五节点或重启全簇；再次运行须另备干净夹具。
失败时停止后续步骤、保留表/事务/节点变更及证据，不重试不确定结果的写入，
不手工COMMIT/ROLLBACK PREPARED、不删孤立路由冒充恢复成功。
若本次停过节点且尚未恢复，finally会尝试重新启动它并记录是否成功。
外部命令超时可能已发生部分效果；按events.jsonl和服务器状态排查，不能盲目重跑。

覆盖与断言

1.1–1.2：版本、节点地址/身份、三worker拓扑、无既存业务数据组和事务残留。
     fa23预建的永久DDL元数据组允许存在，不当作业务残留。
1.3：报告中的九张表完整字段/约束；仅customer/orders分布式，四个共置哈希段，
     每片1主2副，主从成员及任期均已登记；其余七表保持普通表。
1.4–1.6：真实patched psql的\dt、\dt+、\d、\d+输出；shardpg视图；
     每片各成员的物理关系文件和pg_parwal/<local_oid>目录。
2.1–2.2：独立后台连接循环三种写入，另一会话观察两个计数增长；
     Tom/Bob/Ann/Eve插入、余额更新、共置JOIN、删除Bob/Eve订单，逐值检查。
     停止写入后核对成功提交数与实际可见行数。
2.3：真实跨worker的500元提交、50元回滚；重复订单报23505后，
     连同此前已成功的跨片更新一起回滚；两个真实会话按报告的普通BEGIN检查快照；
     并发INSERT同一主键，观察第二backend正在执行后提交第一笔，第二笔必须冲突。
     23505/40001算正确拒绝；超时、未知错误或两笔都成功均失败。
3.1–3.2：停止Tom实际所在分片的主节点，等待新主任期增大，
     核对九表完整内容摘要未变，再检查新主读写和跨片提交/回滚。
3.3–3.4：启动故障节点，确认从角色、commit/last_applied及物理回放追到捕获的下界；
     手动切回并核对共置两个分片、完整数据摘要和切主后的读写。
3.4入组/迁移：新节点版本检查、真实shardpg.add_node、五节点控制组；
     持续写入期间调用move_replica，共置两片成员同时变化，源组和物理表撤除；
     写入精确计数、旧行摘要与Tom/Ann余额保持正确，随后继续提交/回滚。
3.5：安装报告中的日志函数（独立名字），检查真实1主2副、任期/日志序号/解码结果；
     追平使用固定下界，不要求副本追上不断变化的最新tip。
1.7：最后删除全部九表，检查八个业务组、partition_map、逻辑表和物理表消失。
补充：等待prepared/pending/coord_tx/coord_branch/dtx_route归零。
     已知K1若再次出现，drain步骤必须FAIL，不因业务SQL正确而忽略残留。

报告修正

- 仓库号1和2可能落同一片或同一worker。优先保留1，再动态选另一个真实跨片且
  跨worker的键；选取结果记录在events.jsonl，不把单片操作当作跨片事务。
- 后半段Ann仓库号写成1，与前文插入不符；全程使用Ann实际选定的仓库号。
- 两次切主后插入使用不同订单号73001/73002，避免重复主键遮盖HA结果。
- 先完成事务/HA再删表。文档1.7先删再操作的演示章节顺序不能照抄为脚本顺序。
- 日志函数按host+port关联节点，避免只按端口匹配；单机别名须与节点目录一致。

第4节性能阶段

new1尚未接入N02的TPC-C套件，功能写入器也不是TPC-C。默认跳过第4节，
whole_report_status保持INCOMPLETE；不能据此宣称报告的性能结论成立。
需要真实TPC-C驱动，配置benchmark对象中的environment_command、prepare_command、
warmup_command、measure_command四个argv数组，再加--benchmark。
参数模板支持{system}（postgresql/shardpg）、{warehouses}、{terminals}、{seconds}；
environment_command只使用{system}。这些适配命令由所用TPC-C工具提供，非本脚本伪造负载。

environment_command向stdout输出一个JSON对象；其他诊断输出到stderr：
  公共：shared_buffers_bytes=8589934592、cpu_memory_unlimited=true、physical_hosts=3。
  PostgreSQL：topology="1 primary + 2 standbys"、synchronous_commit="on"、
              synchronous_standby_names以"ANY 1"开头。
  ShardPG：topology="1 master + 3 workers"、shard_replicas=2、source_commit=当前HEAD。
性能环境与功能HA后的五节点环境应独立，不能复用改变过的拓扑来冒充报告条件。
prepare负责创建/装载指定仓库数并设置该系统；warmup执行指定终端数/秒数；
measure stdout输出JSON：system、warehouses、terminals、seconds（实际测量秒数）、
new_order_commits、unexpected_errors、consistency_passed；必须通过真实TPC-C一致性校验。
顺序为每系统每仓库档（30、300）先200终端预热180秒，再依次100/200/300/400终端，
每档预热60秒、测量180秒；计算tpmC并保存原始返回值。
报告未给机器可读吞吐阈值，本脚本不编造“必须快于PostgreSQL”等标准。

结果

results.json：每阶段PASS/FAIL/SKIP/BLOCKED、源码提交、北京时间、表名前缀、
functional_status（第1–3节及收尾）与whole_report_status（包括性能）。
events.jsonl：实际SQL、参数、结果、SQLSTATE、外部命令及stdout/stderr、失败栈。
返回码0：完整功能范围通过（未请求的性能阶段仍可SKIP）；
返回码1：任何已请求阶段失败或被前序阻塞；返回码2：只执行core或参数/依赖不完整。
客户端网络黑洞仍可能长于服务器statement_timeout；本脚本不提供跨机器硬截止保证。

本次验证边界

仅做Python语法、CLI帮助/计划/配置解析、SQL与报告字段的静态对照和git diff检查。
没有启动数据库、执行SQL、停启节点、部署二进制或运行TPC-C。
因此当前产物是可执行的验收脚本，不是new1功能已经通过的结论。
