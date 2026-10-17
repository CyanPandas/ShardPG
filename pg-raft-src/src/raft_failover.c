/*
 * raft_failover.c —— 路线 A 的自动切主（2026-10-17）
 *
 * 路线 A：每个 worker 一主两备（PG 物理流复制，synchronous_standby_names = ANY 1），
 * 节点号不变、物理地址随切主迁移。本文件是控制面 leader 上的切主控制器：
 *
 *   · 每 pg_raft.failover_probe_ms 探一次登记了备库的各节点主库（连上并确认不在恢复中）；
 *   · 连续 pg_raft.failover_fail_threshold 次探不通 ⇒ 切主：
 *       1. 该节点**全部**登记备库都要可达且仍在恢复中 —— ANY 1 下每笔已确认的提交至少在
 *          一个备库上，取收到 WAL 最多的那个才能保证不丢；只有一个备库可达时证明不了它最新，
 *          不切（与 Raft 多数派的可用性边界相同：三份里要有两份活着）。
 *       2. 提升收到 WAL 最多的备库（pg_promote），其余备库改跟新主（在新主上建复制槽、
 *          ALTER SYSTEM 改 primary_conninfo / primary_slot_name 并 reload），新主设 ANY 1。
 *       3. 经控制面日志提议新地址（OP_NODE_ADDRESS：各节点改 node_address / node_map /
 *          pg_raft 地址覆盖）、把被提升的备库从登记里移除（OP_NODE_STANDBY），
 *          在协调者上 citus_update_node 改路由。
 *   · 旧主回来后不会再被路由到（地址已迁走，Citus 元数据已改）；它的同步备库已改跟新主，
 *     本地提交会卡在同步复制等待上（天然隔离）。重建为新主的备库目前由运维脚本做。
 *
 * 全部动作经 libpq 执行（自连本节点 / 连备库 / 连协调者），不在本进程里开事务。
 */
#include "postgres.h"

#include "pg_raft.h"

#include <signal.h>

#include "access/xlog.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

bool    pg_raft_auto_failover = false;
int     pg_raft_failover_probe_ms = 2000;
int     pg_raft_failover_fail_threshold = 3;

#define FO_MAX_NODES     64
#define FO_MAX_STANDBYS  8

typedef struct FoStandby
{
    char    name[64];
    char    host[256];
    int     port;
    bool    ok;
    bool    in_recovery;
    bool    streaming;      /* WAL 接收进程正连着某个上游在收 */
    double  lsn;            /* pg_last_wal_receive_lsn 的字节数 */
} FoStandby;

static int  fo_fail[FO_MAX_NODES];
static TimestampTz fo_last_switch[FO_MAX_NODES];

#define FO_COOLDOWN_US   (30 * 1000000L)

/* stmt_ms：语句超时 —— 探测用短的（主库卡死时别把维护进程拖住），提升要等回放完用长的 */
static PGconn *
fo_connect(const char *host, int port, int timeout_s, int stmt_ms)
{
    char    conninfo[512];
    PGconn *c;

    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%d dbname=postgres user=postgres connect_timeout=%d "
             "application_name=pg_raft_failover options='-c statement_timeout=%d'",
             host, port, timeout_s, stmt_ms);
    c = PQconnectdb(conninfo);
    if (PQstatus(c) != CONNECTION_OK)
    {
        PQfinish(c);
        return NULL;
    }
    return c;
}

/* 执行一条语句，返回第一行第一列（调用方 pfree）；失败返回 NULL 并把错误放进 err */
static char *
fo_exec(PGconn *c, const char *sql, char *err, size_t errlen)
{
    PGresult *r = PQexec(c, sql);
    char     *v = NULL;

    if (PQresultStatus(r) == PGRES_TUPLES_OK)
        v = pstrdup(PQntuples(r) > 0 && !PQgetisnull(r, 0, 0) ? PQgetvalue(r, 0, 0) : "");
    else if (PQresultStatus(r) == PGRES_COMMAND_OK)
        v = pstrdup("");
    else if (err != NULL)
        strlcpy(err, PQerrorMessage(c), errlen);
    PQclear(r);
    return v;
}

static PGconn *
fo_self(void)
{
    return fo_connect("127.0.0.1", PostPortNumber, 2, 120000);
}

/* 把某节点的 pg_partdist.tso_conninfo 改成 want（ALTER SYSTEM + reload；连不上就算了，维护进程会兜底） */
static void
fo_set_tso_conninfo(const char *host, int port, const char *want)
{
    PGconn *c = fo_connect(host, port, 2, 10000);
    char    sql[512];
    char    err[256];

    if (c == NULL)
        return;
    err[0] = '\0';
    snprintf(sql, sizeof(sql), "ALTER SYSTEM SET pg_partdist.tso_conninfo = '%s'", want);
    if (fo_exec(c, sql, err, sizeof(err)) == NULL ||
        fo_exec(c, "SELECT pg_reload_conf()", err, sizeof(err)) == NULL)
        elog(WARNING, "pg_raft: 改 %s:%d 的 TSO 连接串失败：%s", host, port, err);
    PQfinish(c);
}

/* 主库可用：连得上且不在恢复中 */
static bool
fo_probe_primary(const char *host, int port)
{
    PGconn *c = fo_connect(host, port, 2, 2000);
    char   *v;
    bool    ok = false;

    if (c == NULL)
        return false;
    v = fo_exec(c, "SELECT pg_is_in_recovery()::text", NULL, 0);
    ok = (v != NULL && strcmp(v, "false") == 0);
    PQfinish(c);
    return ok;
}

/* 返回是否真的提升了备库（拒绝切主时 false，下一次探测失败会再评估） */
static bool
fo_do_failover(int node_id, const char *old_host, int old_port, FoStandby *sb, int nsb)
{
    int         i, best = -1;
    PGconn     *np, *c;
    char        sql[1024];
    char        err[512];
    char       *v;
    char        others[512];
    int         new_tli = 0;
    PGresult   *nodes = NULL;

    /* 1. 全部备库可达、在恢复中、且都已断开上游，取收到 WAL 最多的 */
    for (i = 0; i < nsb; i++)
    {
        PGconn *s = fo_connect(sb[i].host, sb[i].port, 2, 5000);

        sb[i].ok = false;
        if (s == NULL)
            continue;
        v = fo_exec(s, "SELECT pg_is_in_recovery()::text || ' ' || "
                       "(SELECT count(*) FROM pg_stat_wal_receiver WHERE status = 'streaming')::text || ' ' || "
                       "coalesce(pg_wal_lsn_diff(pg_last_wal_receive_lsn(), '0/0'), 0)::text",
                    NULL, 0);
        if (v != NULL)
        {
            char rec[16];
            int  nstream = 0;

            if (sscanf(v, "%15s %d %lf", rec, &nstream, &sb[i].lsn) == 3)
            {
                sb[i].in_recovery = (strcmp(rec, "true") == 0);
                sb[i].streaming = (nstream > 0);
                sb[i].ok = true;
            }
        }
        PQfinish(s);
    }
    for (i = 0; i < nsb; i++)
    {
        if (!sb[i].ok || !sb[i].in_recovery)
        {
            elog(WARNING, "pg_raft: 节点 %d 主库 %s:%d 不可达，但备库 %s（%s:%d）%s —— "
                 "证明不了哪个备库含全部已确认提交，不切主（等它回来或人工处理）",
                 node_id, old_host, old_port, sb[i].name, sb[i].host, sb[i].port,
                 !sb[i].ok ? "不可达" : "已不在恢复中");
            return false;
        }
        /*
         * 备库还连着上游在收 WAL ⇒ 主库其实活着（只是本控制器连不上它：网络分区 / 连接打满），
         * 或者这个备库已经跟上了别的主（上一轮切主刚做完、地址还没生效）。两种都不能切。
         * 主库真宕时备库的接收进程立刻断（干净关闭 / RST），硬断电则等 wal_receiver_timeout。
         */
        if (sb[i].streaming)
        {
            elog(LOG, "pg_raft: 节点 %d 主库 %s:%d 探测不通，但备库 %s 仍在从上游收 WAL —— 不切主",
                 node_id, old_host, old_port, sb[i].name);
            return false;
        }
        if (best < 0 || sb[i].lsn > sb[best].lsn)
            best = i;
    }
    if (best < 0)
        return false;

    elog(LOG, "pg_raft: 节点 %d 主库 %s:%d 连续 %d 次不可达，提升备库 %s（%s:%d，收到 WAL %.0f）",
         node_id, old_host, old_port, pg_raft_failover_fail_threshold,
         sb[best].name, sb[best].host, sb[best].port, sb[best].lsn);

    /* 2. 提升 */
    np = fo_connect(sb[best].host, sb[best].port, 5, 120000);
    if (np == NULL)
    {
        elog(WARNING, "pg_raft: 连不上要提升的备库 %s:%d", sb[best].host, sb[best].port);
        return false;
    }
    err[0] = '\0';
    v = fo_exec(np, "SELECT pg_promote(true, 60)::text", err, sizeof(err));
    if (v == NULL || strcmp(v, "true") != 0)
    {
        elog(WARNING, "pg_raft: 提升备库 %s 失败：%s", sb[best].name, err);
        PQfinish(np);
        return false;
    }

    /* 新主的 WAL 时间线：登记进 node_address，旧主回来时据此认出自己已被取代 */
    {
        char *t = fo_exec(np, "SELECT ('x' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8))::bit(32)::int::text",
                          NULL, 0);

        new_tli = (t != NULL) ? atoi(t) : 0;
    }

    /* 其余备库改跟新主；新主设 ANY 1 */
    others[0] = '\0';
    for (i = 0; i < nsb; i++)
    {
        if (i == best)
            continue;
        snprintf(sql, sizeof(sql),
                 "SELECT pg_create_physical_replication_slot('%s', true) "
                 "WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s')",
                 sb[i].name, sb[i].name);
        (void) fo_exec(np, sql, NULL, 0);
        c = fo_connect(sb[i].host, sb[i].port, 2, 10000);
        if (c != NULL)
        {
            snprintf(sql, sizeof(sql),
                     "ALTER SYSTEM SET primary_conninfo = 'host=%s port=%d user=postgres application_name=%s'",
                     sb[best].host, sb[best].port, sb[i].name);
            (void) fo_exec(c, sql, NULL, 0);
            snprintf(sql, sizeof(sql), "ALTER SYSTEM SET primary_slot_name = '%s'", sb[i].name);
            (void) fo_exec(c, sql, NULL, 0);
            (void) fo_exec(c, "SELECT pg_reload_conf()", NULL, 0);
            PQfinish(c);
        }
        else
            elog(WARNING, "pg_raft: 备库 %s 暂时连不上，未能改跟新主（运维脚本补做）", sb[i].name);
        snprintf(others + strlen(others), sizeof(others) - strlen(others), "%s%s",
                 others[0] ? ", " : "", sb[i].name);
    }
    if (others[0])
        snprintf(sql, sizeof(sql), "ALTER SYSTEM SET synchronous_standby_names = 'ANY 1 (%s)'", others);
    else
        snprintf(sql, sizeof(sql), "ALTER SYSTEM SET synchronous_standby_names = ''");
    (void) fo_exec(np, sql, NULL, 0);
    /* 新主也要参与自动切主 / 旧主自我隔离（备库是在打开开关之前从主库拷的，配置里没有） */
    (void) fo_exec(np, "ALTER SYSTEM SET pg_raft.auto_failover = on", NULL, 0);
    (void) fo_exec(np, "SELECT pg_reload_conf()", NULL, 0);
    PQfinish(np);

    /* 3. 控制面：新地址、备库登记；协调者：Citus 路由 */
    c = fo_self();
    if (c != NULL)
    {
        err[0] = '\0';
        snprintf(sql, sizeof(sql), "SELECT partdist.pg_raft_propose_node_address(%d, '%s', %d, %d)::text",
                 node_id, sb[best].host, sb[best].port, new_tli);
        if (fo_exec(c, sql, err, sizeof(err)) == NULL)
            elog(WARNING, "pg_raft: 提议节点 %d 新地址失败：%s", node_id, err);
        snprintf(sql, sizeof(sql), "SELECT partdist.pg_raft_propose_node_standby(%d, '%s', '', 0, true)::text",
                 node_id, sb[best].name);
        (void) fo_exec(c, sql, NULL, 0);

        /* 协调者地址：node_map（已随 OP_NODE_ADDRESS 更新） */
        snprintf(sql, sizeof(sql),
                 "SELECT hostname || ' ' || port FROM partdist.node_map WHERE node_id = %d",
                 pg_raft_coordinator_node_id);
        v = fo_exec(c, sql, NULL, 0);
        nodes = PQexec(c, "SELECT node_id, hostname, port FROM partdist.node_map ORDER BY node_id");
        if (PQresultStatus(nodes) != PGRES_TUPLES_OK)
        {
            PQclear(nodes);
            nodes = NULL;
        }
        PQfinish(c);
        if (node_id == pg_raft_coordinator_node_id)
        {
            /*
             * 切的就是协调者：新主是旧协调者的物理副本（pg_dist_* 全在），把 Citus 元数据里
             * 协调者自己的位置改掉（citus_set_coordinator_host 经 2PC 同步到带元数据的 worker）。
             *
             * 顺序要紧：**先**把所有存活节点（含新协调者自己）的 TSO 连接串改到新协调者，
             * 再跑任何分布式事务。否则 worker 上 COMMIT PREPARED 取 commit_ts 还去连旧协调者，
             * 失败后留下悬挂的 prepared（实测 pg_dist_node 被它锁死）。各节点维护进程的
             * 定期对齐（pg_raft_routea_reconcile_tso）是兜底，赶不上这一步。
             */
            PGconn *cc;
            char    want[256];

            snprintf(want, sizeof(want), "host=%s port=%d dbname=postgres user=postgres",
                     sb[best].host, sb[best].port);
            fo_set_tso_conninfo(sb[best].host, sb[best].port, want);
            if (nodes != NULL)
            {
                int k;

                for (k = 0; k < PQntuples(nodes); k++)
                {
                    if (atoi(PQgetvalue(nodes, k, 0)) == node_id)
                        continue;
                    fo_set_tso_conninfo(PQgetvalue(nodes, k, 1), atoi(PQgetvalue(nodes, k, 2)), want);
                }
            }
            pg_usleep(1000000L);    /* postmaster 处理 SIGHUP，之后新开的会话都带新连接串 */

            cc = fo_connect(sb[best].host, sb[best].port, 3, 120000);

            if (cc != NULL)
            {
                err[0] = '\0';
                snprintf(sql, sizeof(sql), "SELECT citus_set_coordinator_host('%s', %d)",
                         sb[best].host, sb[best].port);
                if (fo_exec(cc, sql, err, sizeof(err)) == NULL)
                    elog(WARNING, "pg_raft: 新协调者上改 Citus 协调者位置失败：%s", err);
                PQfinish(cc);
            }
        }
        else if (v != NULL && v[0])
        {
            char chost[256];
            int  cport = 0;

            if (sscanf(v, "%255s %d", chost, &cport) == 2)
            {
                PGconn *cc = fo_connect(chost, cport, 3, 120000);

                if (cc != NULL)
                {
                    err[0] = '\0';
                    snprintf(sql, sizeof(sql),
                             "SELECT citus_update_node(nodeid, '%s', %d) FROM pg_dist_node "
                             "WHERE nodename = '%s' AND nodeport = %d",
                             sb[best].host, sb[best].port, old_host, old_port);
                    if (fo_exec(cc, sql, err, sizeof(err)) == NULL)
                        elog(WARNING, "pg_raft: 协调者上改 Citus 路由失败：%s", err);
                    PQfinish(cc);
                }
                else
                    elog(WARNING, "pg_raft: 连不上协调者 %s:%d，Citus 路由未改", chost, cport);
            }
        }
    }
    if (nodes != NULL)
        PQclear(nodes);
    elog(LOG, "pg_raft: 节点 %d 切主完成：新主 %s:%d，其余备库已改跟（%s）",
         node_id, sb[best].host, sb[best].port, others[0] ? others : "无");
    return true;
}

/* 控制面 leader 的维护进程按节流间隔调用 */
void
pg_raft_routea_failover_tick(void)
{
    static MemoryContext fo_cxt = NULL;
    MemoryContext old;
    PGconn     *self;
    PGresult   *r;
    int         i, n;

    if (!pg_raft_auto_failover)
        return;
    /* 维护进程常驻：每轮的字符串都放在这个上下文里，下一轮开头整体清掉 */
    if (fo_cxt == NULL)
        fo_cxt = AllocSetContextCreate(TopMemoryContext, "pg_raft failover", ALLOCSET_SMALL_SIZES);
    MemoryContextReset(fo_cxt);
    old = MemoryContextSwitchTo(fo_cxt);
    self = fo_self();
    if (self == NULL)
    {
        MemoryContextSwitchTo(old);
        return;
    }
    r = PQexec(self,
               "SELECT s.node_id, m.hostname, m.port, "
               "       string_agg(s.name || ' ' || s.host || ' ' || s.port, ';' ORDER BY s.name) "
               "  FROM partdist.node_standby s JOIN partdist.node_map m USING (node_id) "
               " WHERE to_regclass('partdist.node_standby') IS NOT NULL "
               " GROUP BY 1, 2, 3 ORDER BY 1");
    if (PQresultStatus(r) != PGRES_TUPLES_OK)
    {
        PQclear(r);
        PQfinish(self);
        MemoryContextSwitchTo(old);
        return;
    }
    n = PQntuples(r);
    for (i = 0; i < n; i++)
    {
        int         node_id = atoi(PQgetvalue(r, i, 0));
        const char *host = PQgetvalue(r, i, 1);
        int         port = atoi(PQgetvalue(r, i, 2));
        char       *list = pstrdup(PQgetvalue(r, i, 3));
        FoStandby   sb[FO_MAX_STANDBYS];
        int         nsb = 0;
        char       *tok, *save = NULL;

        if (node_id <= 0 || node_id >= FO_MAX_NODES || node_id == pg_raft_node_id)
            continue;
        if (fo_last_switch[node_id] != 0 &&
            GetCurrentTimestamp() - fo_last_switch[node_id] < FO_COOLDOWN_US)
            continue;
        if (fo_probe_primary(host, port))
        {
            fo_fail[node_id] = 0;
            continue;
        }
        fo_fail[node_id]++;
        elog(LOG, "pg_raft: 节点 %d 主库 %s:%d 探测失败（连续 %d 次）", node_id, host, port, fo_fail[node_id]);
        if (fo_fail[node_id] < pg_raft_failover_fail_threshold)
            continue;
        for (tok = strtok_r(list, ";", &save); tok != NULL && nsb < FO_MAX_STANDBYS;
             tok = strtok_r(NULL, ";", &save))
        {
            memset(&sb[nsb], 0, sizeof(FoStandby));
            if (sscanf(tok, "%63s %255s %d", sb[nsb].name, sb[nsb].host, &sb[nsb].port) == 3)
                nsb++;
        }
        if (nsb > 0 && fo_do_failover(node_id, host, port, sb, nsb))
        {
            fo_fail[node_id] = 0;
            fo_last_switch[node_id] = GetCurrentTimestamp();
        }
    }
    PQclear(r);
    PQfinish(self);
    MemoryContextSwitchTo(old);
}

PG_FUNCTION_INFO_V1(pg_raft_propose_node_standby);
/* pg_raft_propose_node_standby(node_id, name, host, port, remove)：经控制面日志登记 / 注销备库 */
Datum
pg_raft_propose_node_standby(PG_FUNCTION_ARGS)
{
    int     node_id = PG_GETARG_INT32(0);
    char   *name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char   *host = text_to_cstring(PG_GETARG_TEXT_PP(2));
    int     port = PG_GETARG_INT32(3);
    bool    remove = PG_GETARG_BOOL(4);
    StringInfoData payload;
    int64   idx;

    if (strpbrk(name, "\"' ;\\") != NULL || strpbrk(host, "\"' ;\\") != NULL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("pg_raft: 备库名 / 地址里不能有引号、空格、分号")));
    initStringInfo(&payload);
    appendStringInfo(&payload,
                     "{\"node_id\": %d, \"name\": \"%s\", \"host\": \"%s\", \"port\": %d, \"remove\": %d}",
                     node_id, name, host, port, remove ? 1 : 0);
    idx = pg_raft_consensus_propose(PG_RAFT_OP_NODE_STANDBY, payload.data);
    if (idx <= 0)
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("pg_raft: 备库登记提议失败（本节点不是控制面 leader，或无多数派）")));
    PG_RETURN_INT64(idx);
}

/*
 * 2026-10-17（路线 A：协调者切主）：本节点的 pg_partdist.tso_conninfo 跟着协调者的地址走。
 * node_address 里有协调者的条目（= 协调者切过主）且与当前连接串不同 ⇒ ALTER SYSTEM + reload。
 * 各节点的维护进程定期调（节点当时宕着、事后回来也能补上）。遗留模式（连接串为空）不动。
 */
void
pg_raft_routea_reconcile_tso(void)
{
    static MemoryContext cxt = NULL;
    MemoryContext old;
    PGconn     *c;
    char        sql[512];
    char       *v;

    if (pg_raft_coordinator_node_id <= 0)
        return;
    if (cxt == NULL)
        cxt = AllocSetContextCreate(TopMemoryContext, "pg_raft tso reconcile", ALLOCSET_SMALL_SIZES);
    MemoryContextReset(cxt);
    old = MemoryContextSwitchTo(cxt);
    c = fo_self();
    if (c != NULL)
    {
        snprintf(sql, sizeof(sql),
                 "SELECT host || ' ' || port || ' ' || coalesce(current_setting('pg_partdist.tso_conninfo', true), '') "
                 "  FROM partdist.node_address WHERE node_id = %d",
                 pg_raft_coordinator_node_id);
        v = fo_exec(c, sql, NULL, 0);
        if (v != NULL && v[0])
        {
            char    host[256];
            int     port = 0;
            int     off = 0;

            if (sscanf(v, "%255s %d %n", host, &port, &off) == 2 && off > 0 && v[off] != '\0')
            {
                const char *cur = v + off;
                char        want[512];

                snprintf(want, sizeof(want), "host=%s port=%d dbname=postgres user=postgres", host, port);
                if (strcmp(cur, want) != 0)
                {
                    char err[256];

                    err[0] = '\0';
                    snprintf(sql, sizeof(sql), "ALTER SYSTEM SET pg_partdist.tso_conninfo = '%s'", want);
                    if (fo_exec(c, sql, err, sizeof(err)) != NULL &&
                        fo_exec(c, "SELECT pg_reload_conf()", err, sizeof(err)) != NULL)
                        elog(LOG, "pg_raft: 协调者地址已迁移，TSO 连接串 \"%s\" → \"%s\"", cur, want);
                    else
                        elog(WARNING, "pg_raft: 改 TSO 连接串失败：%s", err);
                }
            }
        }
        PQfinish(c);
    }
    MemoryContextSwitchTo(old);
}

/*
 * 2026-10-17（路线 A）：旧主自我隔离。
 *
 * 切主后旧主若又起来（宕机重启、网络分区愈合），它仍以同一节点号运行：Citus 已不路由给它、
 * 它的同步备库已改跟新主（本地提交会卡在同步复制等待上），PreVote 也挡住了它扰乱控制面；
 * 但它的 TSO 心跳仍以同一节点号向协调者登记活跃快照，会和新主的登记互相覆盖，
 * GlobalSafeTs 可能越过新主上的活跃快照 —— 必须让它停下来。
 *
 * 判据：控制面给本节点号登记的接替者时间线（node_address.tli）高于本实例的 WAL 时间线
 * ⇒ 本实例是被取代的旧主。本地表是旧的（它收不到控制面日志），所以去问其他节点。
 * 动作：快速停机（SIGINT 给 postmaster），等运维按新主的备库重建后再加入。
 * 正常重启的主（没被切过）时间线不变，新主自己的时间线等于登记值，都不会误判。
 */
void
pg_raft_routea_fence_check(void)
{
    static MemoryContext cxt = NULL;
    MemoryContext old;
    PGconn     *self;
    PGresult   *r;
    TimeLineID  mytli;
    int         i, n;
    int         rec = -1;
    char        sql[256];

    if (!pg_raft_auto_failover || RecoveryInProgress())
        return;
    if (cxt == NULL)
        cxt = AllocSetContextCreate(TopMemoryContext, "pg_raft fence check", ALLOCSET_SMALL_SIZES);
    MemoryContextReset(cxt);
    old = MemoryContextSwitchTo(cxt);
    mytli = GetWALInsertionTimeLine();
    self = fo_self();
    if (self == NULL)
    {
        MemoryContextSwitchTo(old);
        return;
    }
    snprintf(sql, sizeof(sql),
             "SELECT hostname, port FROM partdist.node_map WHERE node_id <> %d ORDER BY node_id",
             pg_raft_node_id);
    r = PQexec(self, sql);
    n = (PQresultStatus(r) == PGRES_TUPLES_OK) ? PQntuples(r) : 0;
    for (i = 0; i < n && rec < 0; i++)
    {
        PGconn *c = fo_connect(PQgetvalue(r, i, 0), atoi(PQgetvalue(r, i, 1)), 2, 2000);
        char   *v;

        if (c == NULL)
            continue;
        snprintf(sql, sizeof(sql),
                 "SELECT coalesce((SELECT tli FROM partdist.node_address WHERE node_id = %d), 0)::text",
                 pg_raft_node_id);
        v = fo_exec(c, sql, NULL, 0);
        if (v != NULL && v[0])
            rec = atoi(v);
        PQfinish(c);
    }
    PQclear(r);
    PQfinish(self);
    MemoryContextSwitchTo(old);

    if (rec > 0 && (TimeLineID) rec > mytli)
    {
        ereport(LOG,
                (errmsg("pg_raft: 节点 %d 已由时间线 %d 的新主接替（本实例时间线 %u）—— 本实例是被取代的旧主，快速停机",
                        pg_raft_node_id, rec, mytli),
                 errhint("按新主的备库重建本实例（pg_basebackup / pg_rewind）后再加入。")));
        kill(PostmasterPid, SIGINT);
    }
}
