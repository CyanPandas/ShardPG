#include "postgres.h"

#include "pg_raft.h"

#include "libpq-fe.h"
#include "miscadmin.h"
#include "access/xact.h"           /* 2026-10-17：节点地址同步的短事务 */
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

/*
 * BGWorker 入口必须导出到动态符号表，否则 PostgreSQL 的 load_external_function
 * (dlsym) 找不到它（PGXS 默认 -fvisibility=hidden 会隐藏普通函数）。
 */
PGDLLEXPORT void pg_raft_topology_monitor_main(Datum main_arg);

/*
 * 通过 libpq 自连本实例，在一个【真正的 client backend】里触发探测+failover。
 *
 * 为什么不在 BGWorker 内直接 SPI：探测会查询 partdist 元数据表，SPI_execute 会
 * 经过 Citus 的 planner/executor hook，而这些 hook 依赖只在 client backend 初始化
 * 的状态（如 Citus 的 per-backend 数据），在 BGWorker 上下文中会段错误。改为 libpq
 * 自连后，pg_raft_force_probe() 在常规 client backend 中运行，Citus hook 一切正常
 * —— 这与一直可用的手动 SELECT pg_raft_force_probe() 完全同构。
 */
static void
pg_raft_self_trigger_probe(void)
{
    char        conninfo[256];
    PGconn     *conn;
    PGresult   *res;

    pg_raft_format_conninfo("127.0.0.1", PostPortNumber, conninfo, sizeof(conninfo));
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(DEBUG1, "pg_raft: self-probe connect failed: %s",
             PQerrorMessage(conn));
        PQfinish(conn);
        return;
    }

    res = PQexec(conn, "SELECT partdist.pg_raft_force_probe()");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        elog(DEBUG1, "pg_raft: self-probe exec failed: %s",
             PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
}

/*
 * DTX-2PC 参与者恢复守护的自动接线（DTX_2PC_DESIGN.md §7）。
 *
 * 与 self-probe 同一手法：BGW 内不做 SPI，经 libpq 连回本节点，让
 * partdist.dtx_recover_prepared() 作为**顶层 SQL** 执行 —— 它内部还要再开
 * libpq 自连接跑 COMMIT/ROLLBACK PREPARED（不允许出现在事务块里），
 * 天然要求顶层语境。
 *
 * 每个节点都跑：恢复是参与者本地的事，与本节点是否 group0 leader 无关；
 * 没有 prepared 事务时守护空转一条 SELECT，成本可忽略。
 * 这是 §7 设计里"master 挂了之后由守护兜底"的兜底方——没有它，master 崩溃后
 * 参与者的 in-doubt 事务（连同行锁）会一直挂到有人手工调 SQL 为止。
 */
static void
pg_raft_self_trigger_dtx_recover(void)
{
    char      conninfo[256];
    char      qry[96];
    PGconn   *conn;
    PGresult *res;

    pg_raft_format_conninfo("127.0.0.1", PostPortNumber, conninfo, sizeof(conninfo));
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(DEBUG1, "pg_raft: dtx 恢复自触发连接失败: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return;
    }
    snprintf(qry, sizeof(qry), "SELECT partdist.dtx_recover_prepared(%d)",
             pg_raft_dtx_recover_timeout_ms);
    res = PQexec(conn, qry);
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        elog(DEBUG1, "pg_raft: dtx 恢复自触发执行失败: %s", PQerrorMessage(conn));
    PQclear(res);
    PQfinish(conn);
}

/*
 * 后台追平通道的自触发（计划文档 §12.4 #5）。
 *
 * 与 self-probe / dtx 恢复同一手法，理由却更硬：追平**必须**在 client backend
 * 里跑。BGW tick 没有 SPI，既读不到数据组条目的 parwal 字节，也回读不了已滑出
 * 环窗口的老条目 —— 这正是"无写入流量时落后 follower 不自行收敛"的成因，
 * 落后超过环容量（128）时更是永久卡死。
 *
 * 每个节点都跑，但只对**本节点是 leader 的组**做事；一个 leader 组都没有时
 * 连接都不开（pg_raft_any_group_leader_local 是纯 shmem 读）。
 */
static void
pg_raft_self_trigger_catchup(void)
{
    char      conninfo[256];
    PGconn   *conn;
    PGresult *res;

    pg_raft_format_conninfo("127.0.0.1", PostPortNumber, conninfo, sizeof(conninfo));
    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        elog(DEBUG1, "pg_raft: 追平通道自连接失败: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return;
    }

    res = PQexec(conn, "SELECT partdist.pg_raft_catchup()");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
        elog(DEBUG1, "pg_raft: 追平通道执行失败: %s", PQerrorMessage(conn));
    else if (PQntuples(res) > 0 && strcmp(PQgetvalue(res, 0, 0), "0") != 0)
        elog(LOG, "pg_raft: 后台追平通道补发 %s 条", PQgetvalue(res, 0, 0));
    PQclear(res);

    /*
     * P7-N53（2026-10-03）：顺带做一次路由对账（partition_map → pg_dist_placement）。
     * 主权登记 apply 时漏拨的（控制面快照安装那一刻实测漏过）在这里补上；函数不存在（老版本 SQL）就忽略。
     */
    res = PQexec(conn, "SELECT CASE WHEN to_regprocedure('partdist.reconcile_routing()') IS NULL THEN 0 "
                       "ELSE partdist.reconcile_routing() END");
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0 &&
        strcmp(PQgetvalue(res, 0, 0), "0") != 0)
        elog(LOG, "pg_raft: 路由对账补拨 %s 个分片", PQgetvalue(res, 0, 0));
    PQclear(res);
    PQfinish(conn);
}

void
pg_raft_topology_monitor_main(Datum main_arg)
{
    TimestampTz last_probe = 0;

    (void) main_arg;

    /*
     * ★ SIGHUP 必须显式接（2026-08-04 审查修正）：BGWorker 默认不处理它，
     * 没有这两行时所有 pg_raft.* 的 SIGHUP 级 GUC（raft_enabled、心跳/选举
     * 超时、dtx 守护参数……）对本进程 `pg_reload_conf()` 都**静默无效**，
     * 只有重启才生效——raft_21 H 在套件负载下超时才把这个缺口暴露出来
     * （守护一直按旧的 dtx_recover_timeout_ms 算年龄）。
     */
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    elog(LOG, "pg_raft: topology monitor started (node_id=%d, raft=%s)",
         pg_raft_node_id, pg_raft_raft_enabled ? "on" : "off");

    /*
     * ★ 2026-10-12：本进程**只推进共识 tick**（选举计时 / 心跳 / 复制），不再做任何阻塞的自连 SQL。
     *
     * 原来探测、后台追平（pg_raft_catchup）、DTX 恢复守护都在这个循环里用阻塞的
     * PQconnectdb + PQexec 跑。演示二断电的 w2 拉起后，pg_raft_catchup 要给它补几十个组、
     * 一跑十几二十秒；CPU 打满时连自连握手都要几秒。这期间 tick 不转，本节点当主的
     * **所有**组都断心跳 —— follower 15 s 超时、预选也通过（它确实没收到心跳），
     * 于是整片改选；每次换主又给旧主排物理基线，CPU 更满，切主风暴自我放大，
     * 最后还把重供打断在半截、留下无主的组（P059）。
     * 这三件事挪到独立的 pg_raft maintenance 进程（pg_raft_maintenance_main），
     * 阻塞多久都不碍心跳。非 Raft 开发模式下没有那个进程，探测仍在这里做。
     */
    for (;;)
    {
        long wait_ms = pg_raft_raft_enabled
            ? pg_raft_heartbeat_ms
            : pg_raft_probe_interval_ms;

        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         wait_ms,
                         PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        CHECK_FOR_INTERRUPTS();

        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (pg_raft_raft_enabled)
        {
            /*
             * 真 Raft：推进状态机（选举超时 / 心跳）。仅读写本节点共享内存 +
             * 通过 libpq 与对端的 pg_raft_rpc() 收发，不在 BGWorker 内做 SPI，安全。
             */
            pg_raft_consensus_tick();
        }
        else
        {
            TimestampTz now = GetCurrentTimestamp();

            /* 非 Raft 开发模式：维持 node 1 的本地 lease；探测仍在本进程做（没有 maintenance 进程）。 */
            pg_raft_try_acquire_leader();
            if (pg_raft_is_leader_local() &&
                (last_probe == 0 || now - last_probe >= pg_raft_probe_interval_ms * 1000L))
            {
                pg_raft_self_trigger_probe();
                last_probe = now;
            }
        }
    }
}

/*
 * pg_raft maintenance —— 2026-10-12 从 topology monitor 拆出来的"慢活"进程。
 *
 * 跑的是原来夹在共识 tick 之间的三件阻塞自连 SQL：控制面探测（本节点是 group0 leader 时）、
 * 后台追平 + 路由对账（本节点当着任何组的主时）、DTX 恢复守护（每个节点）。
 * 节流间隔与原来相同；它们自己慢多久都只拖本进程，不拖心跳。
 * 与 topology monitor 一起注册（见 pg_raft.c register_topology_worker）；Raft 关着时空转。
 */
PGDLLEXPORT void pg_raft_maintenance_main(Datum main_arg);

/*
 * ★ 2026-10-12：自动归队扫尾（partdist.reprovision_sweep）。用**常驻**连接调：扫尾要"连续两轮
 * 不健康才重供"，上一轮的结果记在那个后端里。连接坏了就丢掉重建（只是从头记，最多晚一轮）；
 * 函数还没装（老版本 SQL）就什么都不做。
 */
static PGconn *sweep_conn = NULL;

static void
pg_raft_self_trigger_reprovision_sweep(void)
{
    PGresult *res;

    if (sweep_conn != NULL && PQstatus(sweep_conn) != CONNECTION_OK)
    {
        PQfinish(sweep_conn);
        sweep_conn = NULL;
    }
    if (sweep_conn == NULL)
    {
        char conninfo[256];

        pg_raft_format_conninfo("127.0.0.1", PostPortNumber, conninfo, sizeof(conninfo));
        sweep_conn = PQconnectdb(conninfo);
        if (PQstatus(sweep_conn) != CONNECTION_OK)
        {
            elog(DEBUG1, "pg_raft: 归队扫尾自连接失败: %s", PQerrorMessage(sweep_conn));
            PQfinish(sweep_conn);
            sweep_conn = NULL;
            return;
        }
    }
    res = PQexec(sweep_conn,
                 "SELECT CASE WHEN to_regprocedure('partdist.reprovision_sweep()') IS NULL THEN 0 "
                 "ELSE partdist.reprovision_sweep() END");
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {
        elog(LOG, "pg_raft: 归队扫尾执行失败: %s", PQerrorMessage(sweep_conn));
        PQfinish(sweep_conn);
        sweep_conn = NULL;
    }
    else if (PQntuples(res) > 0 && strcmp(PQgetvalue(res, 0, 0), "0") != 0)
        elog(LOG, "pg_raft: 归队扫尾排上 %s 个重供", PQgetvalue(res, 0, 0));
    PQclear(res);
}


void
pg_raft_maintenance_main(Datum main_arg)
{
    TimestampTz last_probe = 0;
    TimestampTz last_dtx_recover = 0;
    TimestampTz last_catchup = 0;
    TimestampTz last_sweep = 0;

    (void) main_arg;

    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    elog(LOG, "pg_raft: maintenance worker started (node_id=%d)", pg_raft_node_id);

    for (;;)
    {
        /*
         * 固定 1 s 一圈，不跟 pg_raft.heartbeat_ms 走：心跳间隔会被调大（测试里调到 60 s 造"提交后不再有
         * 心跳推送已提交"的窗口），那时本进程的追平 / 路由对账 / DTX 恢复也跟着一分钟才转一圈。
         * 各项自己的节流间隔在下面。
         */
        (void) WaitLatch(MyLatch,
                         WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                         1000L,
                         PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        CHECK_FOR_INTERRUPTS();

        if (ConfigReloadPending)
        {
            ConfigReloadPending = false;
            ProcessConfigFile(PGC_SIGHUP);
        }

        if (!pg_raft_raft_enabled)
            continue;

        /*
         * 2026-10-17（路线 A）：每 5 s 按 partdist.node_address 重灌节点地址覆盖（重启 / 切主后
         * 第一时间生效；控制面 apply 也会直接改，这里是兜底）。本进程连着库，直接起一个短事务。
         */
        {
            static TimestampTz last_addr_sync = 0;
            TimestampTz now = GetCurrentTimestamp();
            volatile int n_addr = 0;

            if (last_addr_sync == 0 || now - last_addr_sync >= 5000 * 1000L)
            {
                last_addr_sync = now;
                PG_TRY();
                {
                    StartTransactionCommand();
                    n_addr = pg_raft_sync_node_addresses_internal();
                    CommitTransactionCommand();
                }
                PG_CATCH();
                {
                    EmitErrorReport();
                    FlushErrorState();
                    AbortCurrentTransaction();
                }
                PG_END_TRY();
                /* 有地址覆盖（切过主）才需要对齐 TSO 连接串；平时零开销 */
                if (n_addr > 0)
                    pg_raft_routea_reconcile_tso();
            }
        }

        /* 2026-10-17（路线 A）：每 10 s 核对本实例是否已被新主接替（被取代的旧主自行停机） */
        if (pg_raft_auto_failover)
        {
            static TimestampTz last_fence = 0;
            TimestampTz now = GetCurrentTimestamp();

            if (last_fence == 0 || now - last_fence >= 10 * 1000000L)
            {
                pg_raft_routea_fence_check();
                last_fence = GetCurrentTimestamp();
            }
        }

        /* 2026-10-17（路线 A）：控制面 leader 上的自动切主（全程 libpq，不在本进程开事务） */
        if (pg_raft_auto_failover && pg_raft_is_leader_local())
        {
            static TimestampTz last_fo = 0;
            TimestampTz now = GetCurrentTimestamp();

            if (last_fo == 0 || now - last_fo >= pg_raft_failover_probe_ms * 1000L)
            {
                pg_raft_routea_failover_tick();
                last_fo = GetCurrentTimestamp();
            }
        }

        /*
         * 仅当本节点是 Leader 才触发控制面探测。探测较重（查 node_map + 连各
         * 节点），按 probe_interval_ms 节流。
         */
        if (pg_raft_is_leader_local())
        {
            TimestampTz now = GetCurrentTimestamp();

            if (last_probe == 0 ||
                now - last_probe >= pg_raft_probe_interval_ms * 1000L)
            {
                pg_raft_self_trigger_probe();
                last_probe = now;
            }
        }

        /*
         * 后台追平通道：按 catchup_interval_ms 节流。只有本节点确实是某个组的
         * leader 时才开连接 —— 否则每个节点每 5 秒空转一次自连接。
         */
        if (pg_raft_catchup_interval_ms > 0 && pg_raft_any_group_leader_local())
        {
            TimestampTz now = GetCurrentTimestamp();

            if (last_catchup == 0 ||
                now - last_catchup >= pg_raft_catchup_interval_ms * 1000L)
            {
                pg_raft_self_trigger_catchup();
                last_catchup = GetCurrentTimestamp();
            }
        }

        /* 自动归队扫尾：本节点当着任何组的主时，每 60 s 一轮（见 pg_raft_self_trigger_reprovision_sweep） */
        if (pg_raft_any_group_leader_local())
        {
            TimestampTz now = GetCurrentTimestamp();

            if (last_sweep == 0 || now - last_sweep >= 60 * 1000000L)
            {
                pg_raft_self_trigger_reprovision_sweep();
                last_sweep = GetCurrentTimestamp();
            }
        }

        /* DTX-2PC 恢复守护：按 dtx_recover_interval_ms 节流，每个节点都跑 */
        if (pg_raft_dtx_2pc_enabled && pg_raft_dtx_recover_interval_ms > 0)
        {
            TimestampTz now = GetCurrentTimestamp();

            if (last_dtx_recover == 0 ||
                now - last_dtx_recover >= pg_raft_dtx_recover_interval_ms * 1000L)
            {
                pg_raft_self_trigger_dtx_recover();
                last_dtx_recover = GetCurrentTimestamp();
            }
        }
    }
}
