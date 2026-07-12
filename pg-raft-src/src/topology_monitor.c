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

void
pg_raft_topology_monitor_main(Datum main_arg)
{
    TimestampTz last_probe = 0;

    (void) main_arg;

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
    }
}
