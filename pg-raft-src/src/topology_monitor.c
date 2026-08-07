#include "postgres.h"

#include "pg_raft.h"

#include "libpq-fe.h"
#include "miscadmin.h"
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
    PQfinish(conn);
}

void
pg_raft_topology_monitor_main(Datum main_arg)
{
    TimestampTz last_probe = 0;
    TimestampTz last_dtx_recover = 0;
    TimestampTz last_catchup = 0;

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
            /* 非 Raft 开发模式：维持 node 1 的本地 lease。 */
            pg_raft_try_acquire_leader();
        }

        /*
         * 仅当本节点是 Leader 才触发控制面探测。探测较重（查 node_map + 连各
         * 节点），按 probe_interval_ms 节流，避免每个心跳 tick 都执行。
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
        if (pg_raft_raft_enabled && pg_raft_catchup_interval_ms > 0 &&
            pg_raft_any_group_leader_local())
        {
            TimestampTz now = GetCurrentTimestamp();

            if (last_catchup == 0 ||
                now - last_catchup >= pg_raft_catchup_interval_ms * 1000L)
            {
                pg_raft_self_trigger_catchup();
                last_catchup = now;
            }
        }

        /* DTX-2PC 恢复守护：按 dtx_recover_interval_ms 节流，每个节点都跑 */
        if (pg_raft_raft_enabled && pg_raft_dtx_2pc_enabled &&
            pg_raft_dtx_recover_interval_ms > 0)
        {
            TimestampTz now = GetCurrentTimestamp();

            if (last_dtx_recover == 0 ||
                now - last_dtx_recover >= pg_raft_dtx_recover_interval_ms * 1000L)
            {
                pg_raft_self_trigger_dtx_recover();
                last_dtx_recover = now;
            }
        }
    }
}
