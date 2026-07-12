/*
 * raft_consensus.c — 纯 C Raft：Leader 选举 + 日志复制 + 多数派提交
 *
 * 节点间 RPC（libpq → 普通 client backend，不碰 SPI/Citus/Go）：
 *   RequestVote : pg_raft_rpc('RV <term> <candidate> <last_idx> <last_term>')
 *   AppendEntries(心跳): pg_raft_rpc('AE <term> <leader>')
 *   AppendEntries(带日志): pg_raft_append_entries(term, leader, prev_idx, prev_term,
 *                     leader_commit, entry_idx, entry_term, entry_op, entry_payload)
 *
 * 日志存本节点共享内存 pg_raft_log；提交后通过 pg_raft_apply_payload_sql 写入
 * partdist 元数据表，使任意 Leader 节点都能接管控制面。
 */
#include "pg_raft.h"

#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#define RAFT_FOLLOWER  0
#define RAFT_CANDIDATE 1
#define RAFT_LEADER    2

#define RAFT_MAX_PEERS     16
#define RAFT_LOG_CAPACITY  128
#define RAFT_OP_LEN        32
#define RAFT_PAYLOAD_MAX   768
#define RAFT_HARDSTATE_MAGIC   UINT32_C(0x52484654)
#define RAFT_HARDSTATE_VERSION 1

typedef struct RaftConsensusShmem
{
    slock_t      mutex;
    int          state;
    int64        current_term;
    int          voted_for;
    int          leader_id;
    TimestampTz  election_deadline;
} RaftConsensusShmem;

typedef struct RaftLogEntry
{
    int64  index;
    int64  term;
    char   op_type[RAFT_OP_LEN];
    char   payload[RAFT_PAYLOAD_MAX];
} RaftLogEntry;

typedef struct RaftLogShmem
{
    slock_t      mutex;
    int64        last_log_index;
    int64        commit_index;
    int64        last_applied;
    int64        peer_next_index[RAFT_MAX_PEERS];
    int64        peer_match_index[RAFT_MAX_PEERS];
    bool         repl_inited;
    RaftLogEntry ring[RAFT_LOG_CAPACITY];
} RaftLogShmem;

typedef struct RaftPeer
{
    int   node_id;
    char  host[256];
    int   port;
} RaftPeer;

typedef struct RaftHardStateFile
{
    uint32 magic;
    uint32 version;
    int64  current_term;
    int32  voted_for;
    int64  commit_index;
} RaftHardStateFile;

bool  pg_raft_raft_enabled = false;
char *pg_raft_peers = NULL;
int   pg_raft_election_timeout_ms = 1500;
int   pg_raft_heartbeat_ms = 400;

RaftConsensusShmem *RaftConsensus = NULL;
static RaftLogShmem *RaftLog = NULL;

static RaftPeer peers[RAFT_MAX_PEERS];
static int      n_peers = 0;
static bool     peers_parsed = false;
static bool     hard_state_loaded = false;

PG_FUNCTION_INFO_V1(pg_raft_rpc);
PG_FUNCTION_INFO_V1(pg_raft_append_entries);
PG_FUNCTION_INFO_V1(pg_raft_apply_committed);

/* ---- 共享内存 ---- */

static void restore_hard_state_if_needed(void);
static bool persist_hard_state_values(int64 current_term, int voted_for,
                                      int64 commit_index);
static void persist_hard_state_unlocked(void);
static void current_last_log_info_locked(int64 *last_idx, int64 *last_term);
static bool candidate_log_is_up_to_date_locked(int64 cand_last_idx,
                                               int64 cand_last_term);

Size
pg_raft_consensus_shmem_size(void)
{
    return MAXALIGN(sizeof(RaftConsensusShmem)) + MAXALIGN(sizeof(RaftLogShmem));
}

void
pg_raft_consensus_shmem_init(void)
{
    bool found;

    RaftConsensus = (RaftConsensusShmem *)
        ShmemInitStruct("pg_raft_consensus",
                        MAXALIGN(sizeof(RaftConsensusShmem)), &found);

    if (!found)
    {
        SpinLockInit(&RaftConsensus->mutex);
        RaftConsensus->state = RAFT_FOLLOWER;
        RaftConsensus->current_term = 0;
        RaftConsensus->voted_for = 0;
        RaftConsensus->leader_id = 0;
        RaftConsensus->election_deadline = 0;
    }

    RaftLog = (RaftLogShmem *)
        ShmemInitStruct("pg_raft_log",
                        MAXALIGN(sizeof(RaftLogShmem)), &found);

    if (!found)
    {
        SpinLockInit(&RaftLog->mutex);
        RaftLog->last_log_index = 0;
        RaftLog->commit_index = 0;
        RaftLog->last_applied = 0;
        RaftLog->repl_inited = false;
    }

    restore_hard_state_if_needed();
}

/* ---- 日志辅助 ---- */

static RaftLogEntry *log_slot(int64 index);

static void
hard_state_path(char *path, size_t pathlen)
{
    snprintf(path, pathlen, "%s/pg_raft_hardstate", DataDir);
}

static bool
persist_hard_state_values(int64 current_term, int voted_for, int64 commit_index)
{
    char              path[MAXPGPATH];
    char              tmppath[MAXPGPATH];
    RaftHardStateFile hs;
    int               fd;
    ssize_t           written;

    hard_state_path(path, sizeof(path));
    snprintf(tmppath, sizeof(tmppath), "%s.tmp", path);

    hs.magic = RAFT_HARDSTATE_MAGIC;
    hs.version = RAFT_HARDSTATE_VERSION;
    hs.current_term = current_term;
    hs.voted_for = voted_for;
    hs.commit_index = commit_index;

    fd = open(tmppath, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0)
    {
        elog(WARNING, "pg_raft: 无法打开 hardstate 临时文件 \"%s\": %m", tmppath);
        return false;
    }

    written = write(fd, &hs, sizeof(hs));
    if (written != (ssize_t) sizeof(hs))
    {
        elog(WARNING, "pg_raft: 写入 hardstate 失败 \"%s\": %m", tmppath);
        close(fd);
        unlink(tmppath);
        return false;
    }

    if (pg_fsync(fd) != 0)
    {
        elog(WARNING, "pg_raft: fsync hardstate 失败 \"%s\": %m", tmppath);
        close(fd);
        unlink(tmppath);
        return false;
    }

    close(fd);

    if (rename(tmppath, path) != 0)
    {
        elog(WARNING, "pg_raft: 重命名 hardstate 文件失败 \"%s\": %m", path);
        unlink(tmppath);
        return false;
    }

    return true;
}

static void
persist_hard_state_unlocked(void)
{
    int64 current_term;
    int   voted_for;
    int64 commit_index = 0;

    if (RaftConsensus == NULL)
        return;

    SpinLockAcquire(&RaftConsensus->mutex);
    current_term = RaftConsensus->current_term;
    voted_for = RaftConsensus->voted_for;
    SpinLockRelease(&RaftConsensus->mutex);

    if (RaftLog != NULL)
    {
        SpinLockAcquire(&RaftLog->mutex);
        commit_index = RaftLog->commit_index;
        SpinLockRelease(&RaftLog->mutex);
    }

    (void) persist_hard_state_values(current_term, voted_for, commit_index);
}

static void
restore_hard_state_if_needed(void)
{
    char              path[MAXPGPATH];
    RaftHardStateFile hs;
    int               fd;
    ssize_t           nread;

    if (hard_state_loaded)
        return;

    hard_state_loaded = true;
    hard_state_path(path, sizeof(path));

    fd = open(path, O_RDONLY, 0);
    if (fd < 0)
        return;

    nread = read(fd, &hs, sizeof(hs));
    close(fd);

    if (nread != (ssize_t) sizeof(hs) ||
        hs.magic != RAFT_HARDSTATE_MAGIC ||
        hs.version != RAFT_HARDSTATE_VERSION)
    {
        elog(WARNING, "pg_raft: 忽略损坏的 hardstate 文件 \"%s\"", path);
        return;
    }

    if (RaftConsensus != NULL)
    {
        SpinLockAcquire(&RaftConsensus->mutex);
        if (hs.current_term > RaftConsensus->current_term)
            RaftConsensus->current_term = hs.current_term;
        RaftConsensus->voted_for = hs.voted_for;
        SpinLockRelease(&RaftConsensus->mutex);
    }

    if (RaftLog != NULL)
    {
        SpinLockAcquire(&RaftLog->mutex);
        if (hs.commit_index > RaftLog->commit_index)
            RaftLog->commit_index = hs.commit_index;
        if (RaftLog->last_applied > RaftLog->commit_index)
            RaftLog->last_applied = RaftLog->commit_index;
        SpinLockRelease(&RaftLog->mutex);
    }
}

static bool
raft_persist_spi_begin(bool *spi_owned)
{
    if (!pg_raft_spi_begin(spi_owned))
        return false;
    if (*spi_owned)
        PushActiveSnapshot(GetTransactionSnapshot());
    return true;
}

static void
raft_persist_spi_end(bool spi_owned)
{
    if (spi_owned)
    {
        PopActiveSnapshot();
        pg_raft_spi_end(true);
    }
}

static void
delete_log_entry_sql(int64 index)
{
    StringInfoData sql;
    bool           spi_owned;

    if (index <= 0 || !raft_persist_spi_begin(&spi_owned))
        return;

    if (SPI_execute("SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = 'partdist' "
                    "AND table_name = 'raft_log' "
                    "AND column_name = 'log_index'",
                    true, 1) != SPI_OK_SELECT || SPI_processed == 0)
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "DELETE FROM partdist.raft_log WHERE log_index = %lld",
                     (long long) index);
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);

    raft_persist_spi_end(spi_owned);
}

static RaftLogEntry *
log_slot(int64 index)
{
    if (index <= 0)
        return NULL;
    return &RaftLog->ring[(index - 1) % RAFT_LOG_CAPACITY];
}

static bool
log_get_entry_locked(int64 index, RaftLogEntry *out)
{
    RaftLogEntry *e;

    if (index <= 0 || index > RaftLog->last_log_index)
        return false;
    e = log_slot(index);
    if (e->index != index)
        return false;
    *out = *e;
    return true;
}

static void
current_last_log_info_locked(int64 *last_idx, int64 *last_term)
{
    RaftLogEntry entry;

    *last_idx = 0;
    *last_term = 0;

    if (RaftLog == NULL)
        return;

    *last_idx = RaftLog->last_log_index;
    if (*last_idx <= 0)
        return;

    if (log_get_entry_locked(*last_idx, &entry))
        *last_term = entry.term;
}

static bool
candidate_log_is_up_to_date_locked(int64 cand_last_idx, int64 cand_last_term)
{
    int64 local_last_idx;
    int64 local_last_term;

    current_last_log_info_locked(&local_last_idx, &local_last_term);

    if (cand_last_term != local_last_term)
        return cand_last_term > local_last_term;
    return cand_last_idx >= local_last_idx;
}

static int64
log_append_locked(int64 term, const char *op_type, const char *payload)
{
    int64         idx;
    RaftLogEntry *e;

    if (RaftLog->last_log_index - RaftLog->last_applied >= RAFT_LOG_CAPACITY - 1)
    {
        elog(WARNING, "pg_raft: log ring full, cannot append");
        return 0;
    }

    idx = RaftLog->last_log_index + 1;
    e = log_slot(idx);
    e->index = idx;
    e->term = term;
    strlcpy(e->op_type, op_type, RAFT_OP_LEN);
    strlcpy(e->payload, payload, RAFT_PAYLOAD_MAX);
    RaftLog->last_log_index = idx;
    return idx;
}

static void
log_truncate_after_locked(int64 index)
{
    if (index < RaftLog->last_log_index)
        RaftLog->last_log_index = index;
    if (RaftLog->commit_index > index)
        RaftLog->commit_index = index;
    if (RaftLog->last_applied > index)
        RaftLog->last_applied = index;
}

static void
apply_one_entry(const RaftLogEntry *e)
{
    (void) pg_raft_apply_payload_sql(e->op_type, e->payload);
}

static void
persist_log_entry_sql(int64 index, int64 term, const char *op_type,
                      const char *payload, bool committed)
{
    StringInfoData sql;
    bool           spi_owned;

    if (!raft_persist_spi_begin(&spi_owned))
        return;

    if (SPI_execute("SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = 'partdist' "
                    "AND table_name = 'raft_log' "
                    "AND column_name = 'log_index'",
                    true, 1) != SPI_OK_SELECT || SPI_processed == 0)
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "INSERT INTO partdist.raft_log "
                     "(log_index, term, op_type, payload, committed) "
                     "VALUES (%lld, %lld, %s, %s::jsonb, %s) "
                     "ON CONFLICT (log_index) DO UPDATE SET "
                     "term = EXCLUDED.term, op_type = EXCLUDED.op_type, "
                     "payload = EXCLUDED.payload, committed = partdist.raft_log.committed OR EXCLUDED.committed",
                     (long long) index,
                     (long long) term,
                     quote_literal_cstr(op_type),
                     quote_literal_cstr(payload),
                     committed ? "true" : "false");
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);

    raft_persist_spi_end(spi_owned);
}

static void
mark_log_committed_sql(int64 upto_index)
{
    StringInfoData sql;
    bool           spi_owned;

    if (upto_index <= 0 || !raft_persist_spi_begin(&spi_owned))
        return;

    if (SPI_execute("SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = 'partdist' "
                    "AND table_name = 'raft_log' "
                    "AND column_name = 'log_index'",
                    true, 1) != SPI_OK_SELECT || SPI_processed == 0)
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    initStringInfo(&sql);
    appendStringInfo(&sql,
                     "UPDATE partdist.raft_log SET committed = true "
                     "WHERE log_index <= %lld AND committed = false",
                     (long long) upto_index);
    (void) SPI_execute(sql.data, false, 0);
    pfree(sql.data);

    raft_persist_spi_end(spi_owned);
}

/*
 * Load durable log state into shared memory after postmaster restart.
 * This is intentionally called only from SQL/client backend paths, not from
 * the Raft BGWorker, because restoring requires SPI and Citus hooks.
 */
static void
restore_persistent_log_if_needed(void)
{
    bool spi_owned;
    int  ret;
    int  i;
    bool clamped_hardstate = false;

    if (RaftLog == NULL)
        return;

    SpinLockAcquire(&RaftLog->mutex);
    if (RaftLog->last_log_index > 0)
    {
        SpinLockRelease(&RaftLog->mutex);
        return;
    }
    SpinLockRelease(&RaftLog->mutex);

    if (!raft_persist_spi_begin(&spi_owned))
        return;

    if (SPI_execute("SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = 'partdist' "
                    "AND table_name = 'raft_log' "
                    "AND column_name = 'log_index'",
                    true, 1) != SPI_OK_SELECT || SPI_processed == 0)
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    ret = SPI_execute("SELECT log_index, term, op_type, payload::text, committed "
                      "FROM partdist.raft_log ORDER BY log_index",
                      true, 0);
    if (ret != SPI_OK_SELECT)
    {
        raft_persist_spi_end(spi_owned);
        return;
    }

    SpinLockAcquire(&RaftLog->mutex);
    for (i = 0; i < (int) SPI_processed; i++)
    {
        HeapTuple     tup = SPI_tuptable->vals[i];
        TupleDesc     desc = SPI_tuptable->tupdesc;
        bool          isnull;
        int64         idx;
        int64         term;
        char         *op;
        char         *payload;
        bool          committed;
        RaftLogEntry *e;

        idx = DatumGetInt64(SPI_getbinval(tup, desc, 1, &isnull));
        term = DatumGetInt64(SPI_getbinval(tup, desc, 2, &isnull));
        op = TextDatumGetCString(SPI_getbinval(tup, desc, 3, &isnull));
        payload = TextDatumGetCString(SPI_getbinval(tup, desc, 4, &isnull));
        committed = DatumGetBool(SPI_getbinval(tup, desc, 5, &isnull));

        e = log_slot(idx);
        e->index = idx;
        e->term = term;
        strlcpy(e->op_type, op, RAFT_OP_LEN);
        strlcpy(e->payload, payload, RAFT_PAYLOAD_MAX);

        if (idx > RaftLog->last_log_index)
            RaftLog->last_log_index = idx;
        if (committed && idx > RaftLog->commit_index)
            RaftLog->commit_index = idx;

        pfree(op);
        pfree(payload);
    }
    /*
     * 如果 SQL 日志被重建或清空，但 hardstate 文件仍保留旧 commit_index，
     * 不能让 last_applied 越过真实日志末端，否则新日志会永远跳过 apply。
     */
    if (RaftLog->commit_index > RaftLog->last_log_index)
    {
        RaftLog->commit_index = RaftLog->last_log_index;
        clamped_hardstate = true;
    }
    if (RaftLog->last_applied > RaftLog->commit_index)
        RaftLog->last_applied = RaftLog->commit_index;

    /*
     * Metadata tables are durable too. Avoid replaying old committed entries
     * on every restart; missed entries are still appended by the current leader
     * through AppendEntries and then applied normally.
     */
    RaftLog->last_applied = RaftLog->commit_index;
    SpinLockRelease(&RaftLog->mutex);

    raft_persist_spi_end(spi_owned);
    if (clamped_hardstate)
        persist_hard_state_unlocked();
}

void
pg_raft_consensus_apply_pending(void)
{
    RaftLogEntry e;

    if (RaftLog == NULL)
        return;

    for (;;)
    {
        int64 idx;

        SpinLockAcquire(&RaftLog->mutex);
        if (RaftLog->last_applied >= RaftLog->commit_index)
        {
            SpinLockRelease(&RaftLog->mutex);
            break;
        }
        idx = RaftLog->last_applied + 1;
        if (!log_get_entry_locked(idx, &e))
        {
            SpinLockRelease(&RaftLog->mutex);
            break;
        }
        RaftLog->last_applied = idx;
        SpinLockRelease(&RaftLog->mutex);

        apply_one_entry(&e);
    }
}

static void
advance_commit_index_locked(int64 leader_commit)
{
    int64 n = leader_commit;

    if (n > RaftLog->last_log_index)
        n = RaftLog->last_log_index;
    if (n > RaftLog->commit_index)
        RaftLog->commit_index = n;
}

static int
cluster_majority(void)
{
    int cluster = (n_peers > 0) ? n_peers : 1;
    return cluster / 2 + 1;
}

static void
init_leader_replication(void)
{
    int64 last;
    int   i;

    SpinLockAcquire(&RaftLog->mutex);
    last = RaftLog->last_log_index + 1;
    for (i = 0; i < RAFT_MAX_PEERS; i++)
    {
        RaftLog->peer_next_index[i] = last;
        RaftLog->peer_match_index[i] = 0;
    }
    RaftLog->repl_inited = true;
    SpinLockRelease(&RaftLog->mutex);
}

/* ---- peers ---- */

static void
parse_peers(void)
{
    char *buf;
    char *tok;
    char *saveptr = NULL;

    if (peers_parsed)
        return;

    n_peers = 0;
    if (pg_raft_peers == NULL || pg_raft_peers[0] == '\0')
    {
        peers_parsed = true;
        return;
    }

    buf = pstrdup(pg_raft_peers);
    for (tok = strtok_r(buf, ",", &saveptr);
         tok != NULL && n_peers < RAFT_MAX_PEERS;
         tok = strtok_r(NULL, ",", &saveptr))
    {
        int  id = 0;
        int  port = 0;
        char host[256];

        if (sscanf(tok, " %d@%255[^:]:%d", &id, host, &port) == 3)
        {
            peers[n_peers].node_id = id;
            strlcpy(peers[n_peers].host, host, sizeof(peers[n_peers].host));
            peers[n_peers].port = port;
            n_peers++;
        }
    }
    pfree(buf);
    peers_parsed = true;
}

static void
reset_election_deadline_locked(void)
{
    long base = pg_raft_election_timeout_ms;
    long jitter = (base > 0) ? (random() % base) : 0;

    RaftConsensus->election_deadline =
        GetCurrentTimestamp() + (base + jitter) * 1000L;
}

/* ---- libpq RPC ---- */

static bool
parse_resp2(const char *val, int64 *term, int *flag)
{
    return (val != NULL &&
            sscanf(val, "%ld %d", (long *) term, flag) == 2);
}

static bool
send_sql_rpc(RaftPeer *p, const char *sql, int64 *resp_term, int *resp_flag)
{
    char        conninfo[512];
    PGconn     *conn;
    PGresult   *res;
    bool        ok = false;

    snprintf(conninfo, sizeof(conninfo),
             "host=%s port=%d dbname=postgres user=postgres "
             "connect_timeout=1 application_name=pg_raft_rpc",
             p->host, p->port);

    conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK)
    {
        PQfinish(conn);
        return false;
    }

    res = PQexec(conn, sql);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1)
        ok = parse_resp2(PQgetvalue(res, 0, 0), resp_term, resp_flag);

    PQclear(res);
    PQfinish(conn);
    return ok;
}

static bool
send_rpc_msg(RaftPeer *p, const char *msg, int64 *resp_term, int *resp_flag)
{
    char sql[256];

    snprintf(sql, sizeof(sql), "SELECT partdist.pg_raft_rpc('%s')", msg);
    return send_sql_rpc(p, sql, resp_term, resp_flag);
}

static bool
send_append_entries_rpc(RaftPeer *p, int64 term, int leader_id,
                        int64 prev_idx, int64 prev_term, int64 leader_commit,
                        int64 entry_idx, int64 entry_term,
                        const char *entry_op, const char *entry_payload,
                        int64 *resp_term, int *resp_flag)
{
    StringInfoData sql;
    bool           ok;

    initStringInfo(&sql);
    if (entry_idx > 0 && entry_op != NULL && entry_payload != NULL)
    {
        appendStringInfo(&sql,
                         "SELECT partdist.pg_raft_append_entries("
                         "%lld::bigint, %d, %lld::bigint, %lld::bigint, %lld::bigint, "
                         "%lld::bigint, %lld::bigint, %s, %s)",
                         (long long) term, leader_id,
                         (long long) prev_idx, (long long) prev_term,
                         (long long) leader_commit,
                         (long long) entry_idx, (long long) entry_term,
                         quote_literal_cstr(entry_op),
                         quote_literal_cstr(entry_payload));
    }
    else
    {
        appendStringInfo(&sql,
                         "SELECT partdist.pg_raft_append_entries("
                         "%lld::bigint, %d, %lld::bigint, %lld::bigint, %lld::bigint, "
                         "NULL, NULL, NULL, NULL)",
                         (long long) term, leader_id,
                         (long long) prev_idx, (long long) prev_term,
                         (long long) leader_commit);
    }

    ok = send_sql_rpc(p, sql.data, resp_term, resp_flag);
    pfree(sql.data);
    return ok;
}

static void
step_down_if_higher(int64 their_term)
{
    bool need_persist = false;

    SpinLockAcquire(&RaftConsensus->mutex);
    if (their_term > RaftConsensus->current_term)
    {
        RaftConsensus->current_term = their_term;
        RaftConsensus->state = RAFT_FOLLOWER;
        RaftConsensus->voted_for = 0;
        RaftConsensus->leader_id = 0;
        SpinLockAcquire(&RaftLog->mutex);
        RaftLog->repl_inited = false;
        SpinLockRelease(&RaftLog->mutex);
        reset_election_deadline_locked();
        need_persist = true;
    }
    SpinLockRelease(&RaftConsensus->mutex);

    if (need_persist)
        persist_hard_state_unlocked();
}

/* 根据 match_index 计算可提交的最大 index（多数派已复制） */
static int64
compute_new_commit_index(int64 current_term)
{
    int64 matches[RAFT_MAX_PEERS + 1];
    int   n = 0;
    int   i;
    int64 idx;
    RaftLogEntry e;

    matches[n++] = RaftLog->last_log_index;
    for (i = 0; i < n_peers; i++)
    {
        if (peers[i].node_id == pg_raft_node_id)
            continue;
        matches[n++] = RaftLog->peer_match_index[i];
    }

    /* 简单选择排序找第 majority 大的值 */
    for (i = 0; i < n - 1; i++)
    {
        int j;
        for (j = i + 1; j < n; j++)
        {
            if (matches[j] > matches[i])
            {
                int64 t = matches[i];
                matches[i] = matches[j];
                matches[j] = t;
            }
        }
    }

    idx = matches[cluster_majority() - 1];
    if (idx <= RaftLog->commit_index)
        return RaftLog->commit_index;

    if (log_get_entry_locked(idx, &e) && e.term == current_term)
        return idx;
    return RaftLog->commit_index;
}

static void
replicate_to_peer(int peer_slot)
{
    int64 term;
    int   leader_id = pg_raft_node_id;
    int64 prev_idx;
    int64 prev_term = 0;
    int64 leader_commit;
    RaftLogEntry entry;
    int64 rt;
    int   ok_flag;
    bool  has_entry;
    bool  repl_ready;

    SpinLockAcquire(&RaftConsensus->mutex);
    if (RaftConsensus->state != RAFT_LEADER)
    {
        SpinLockRelease(&RaftConsensus->mutex);
        return;
    }
    term = RaftConsensus->current_term;
    SpinLockRelease(&RaftConsensus->mutex);

    SpinLockAcquire(&RaftLog->mutex);
    if (!RaftLog->repl_inited)
    {
        int64 last = RaftLog->last_log_index + 1;
        int   j;

        for (j = 0; j < RAFT_MAX_PEERS; j++)
        {
            RaftLog->peer_next_index[j] = last;
            RaftLog->peer_match_index[j] = 0;
        }
        RaftLog->repl_inited = true;
    }
    repl_ready = RaftLog->repl_inited;
    leader_commit = RaftLog->commit_index;
    prev_idx = RaftLog->peer_next_index[peer_slot] - 1;
    if (prev_idx > 0)
    {
        RaftLogEntry *pe = log_slot(prev_idx);
        if (pe->index == prev_idx)
            prev_term = pe->term;
    }
    has_entry = (RaftLog->peer_next_index[peer_slot] <= RaftLog->last_log_index &&
                 log_get_entry_locked(RaftLog->peer_next_index[peer_slot], &entry));
    SpinLockRelease(&RaftLog->mutex);

    if (!repl_ready)
        return;

    if (has_entry)
    {
        if (!send_append_entries_rpc(&peers[peer_slot], term, leader_id,
                                     prev_idx, prev_term, leader_commit,
                                     entry.index, entry.term,
                                     entry.op_type, entry.payload,
                                     &rt, &ok_flag))
            return;
    }
    else
    {
        if (!send_append_entries_rpc(&peers[peer_slot], term, leader_id,
                                     prev_idx, prev_term, leader_commit,
                                     0, 0, NULL, NULL, &rt, &ok_flag))
            return;
    }

    if (rt > term)
    {
        step_down_if_higher(rt);
        return;
    }

    if (ok_flag)
    {
        SpinLockAcquire(&RaftLog->mutex);
        if (has_entry)
        {
            RaftLog->peer_match_index[peer_slot] = entry.index;
            RaftLog->peer_next_index[peer_slot] = entry.index + 1;
        }
        else
            RaftLog->peer_match_index[peer_slot] = prev_idx;
        SpinLockRelease(&RaftLog->mutex);
    }
    else
    {
        SpinLockAcquire(&RaftLog->mutex);
        if (RaftLog->peer_next_index[peer_slot] > 1)
            RaftLog->peer_next_index[peer_slot]--;
        SpinLockRelease(&RaftLog->mutex);
    }
}

static void
leader_replicate_and_commit(void)
{
    int   i;
    int64 new_commit;
    int64 term;
    bool  ready;

    SpinLockAcquire(&RaftLog->mutex);
    ready = RaftLog->repl_inited;
    SpinLockRelease(&RaftLog->mutex);

    if (!ready)
        init_leader_replication();

    for (i = 0; i < n_peers; i++)
    {
        if (peers[i].node_id == pg_raft_node_id)
            continue;
        replicate_to_peer(i);
    }

    SpinLockAcquire(&RaftConsensus->mutex);
    term = RaftConsensus->current_term;
    SpinLockRelease(&RaftConsensus->mutex);

    SpinLockAcquire(&RaftLog->mutex);
    new_commit = compute_new_commit_index(term);
    if (new_commit > RaftLog->commit_index)
        RaftLog->commit_index = new_commit;
    SpinLockRelease(&RaftLog->mutex);
}

static void
send_heartbeats(void)
{
    leader_replicate_and_commit();
}

/* ---- 选举 ---- */

static void
start_election(void)
{
    int64 term;
    int64 last_log_idx = 0;
    int64 last_log_term = 0;
    int   votes = 1;
    int   majority = cluster_majority();
    char  msg[128];
    int   i;
    bool  won = false;

    SpinLockAcquire(&RaftLog->mutex);
    RaftLog->repl_inited = false;
    current_last_log_info_locked(&last_log_idx, &last_log_term);
    SpinLockRelease(&RaftLog->mutex);

    SpinLockAcquire(&RaftConsensus->mutex);
    RaftConsensus->current_term += 1;
    RaftConsensus->state = RAFT_CANDIDATE;
    RaftConsensus->voted_for = pg_raft_node_id;
    RaftConsensus->leader_id = 0;
    term = RaftConsensus->current_term;
    reset_election_deadline_locked();
    SpinLockRelease(&RaftConsensus->mutex);
    persist_hard_state_unlocked();

    elog(LOG, "pg_raft: node %d 发起选举 term=%ld (需 %d 票)",
         pg_raft_node_id, (long) term, majority);

    snprintf(msg, sizeof(msg), "RV %lld %d %lld %lld",
             (long long) term, pg_raft_node_id,
             (long long) last_log_idx, (long long) last_log_term);

    for (i = 0; i < n_peers; i++)
    {
        int64 rt = 0;
        int   granted = 0;

        if (peers[i].node_id == pg_raft_node_id)
            continue;

        if (!send_rpc_msg(&peers[i], msg, &rt, &granted))
            continue;

        if (rt > term)
        {
            step_down_if_higher(rt);
            return;
        }
        if (granted)
            votes++;
    }

    if (votes >= majority)
    {
        SpinLockAcquire(&RaftConsensus->mutex);
        if (RaftConsensus->state == RAFT_CANDIDATE &&
            RaftConsensus->current_term == term)
        {
            RaftConsensus->state = RAFT_LEADER;
            RaftConsensus->leader_id = pg_raft_node_id;
            won = true;
        }
        SpinLockRelease(&RaftConsensus->mutex);
    }

    if (won)
    {
        elog(LOG, "pg_raft: node %d 当选 LEADER term=%ld (%d/%d 票)",
             pg_raft_node_id, (long) term, votes, n_peers);
        persist_hard_state_unlocked();
        init_leader_replication();
        send_heartbeats();
    }
}

void
pg_raft_consensus_tick(void)
{
    int         state;
    TimestampTz deadline;
    TimestampTz now;

    if (!pg_raft_raft_enabled || RaftConsensus == NULL)
        return;

    parse_peers();
    if (n_peers == 0)
        return;

    now = GetCurrentTimestamp();

    SpinLockAcquire(&RaftConsensus->mutex);
    state = RaftConsensus->state;
    deadline = RaftConsensus->election_deadline;
    if (deadline == 0)
    {
        reset_election_deadline_locked();
        deadline = RaftConsensus->election_deadline;
    }
    SpinLockRelease(&RaftConsensus->mutex);

    if (state == RAFT_LEADER)
    {
        send_heartbeats();
        return;
    }

    if (now >= deadline)
        start_election();
}

/* ---- Propose（Leader client backend 调用） ---- */

/* 同步复制单条日志；返回收到确认的节点数（含 Leader 自身） */
static int
sync_replicate_index(int64 idx, int64 term)
{
    RaftLogEntry entry;
    int64        prev_idx;
    int64        prev_term = 0;
    int64        leader_commit;
    int          acks = 1;
    int          i;

    SpinLockAcquire(&RaftLog->mutex);
    if (!log_get_entry_locked(idx, &entry))
    {
        SpinLockRelease(&RaftLog->mutex);
        return 0;
    }
    prev_idx = idx - 1;
    if (prev_idx > 0)
    {
        RaftLogEntry *pe = log_slot(prev_idx);
        if (pe->index == prev_idx)
            prev_term = pe->term;
    }
    leader_commit = RaftLog->commit_index;
    if (!RaftLog->repl_inited)
    {
        int64 last = RaftLog->last_log_index + 1;
        int   j;

        for (j = 0; j < RAFT_MAX_PEERS; j++)
        {
            RaftLog->peer_next_index[j] = last;
            RaftLog->peer_match_index[j] = 0;
        }
        RaftLog->repl_inited = true;
    }
    SpinLockRelease(&RaftLog->mutex);

    for (i = 0; i < n_peers; i++)
    {
        int64 rt = 0;
        int   ok = 0;

        if (peers[i].node_id == pg_raft_node_id)
            continue;

        if (!send_append_entries_rpc(&peers[i], term, pg_raft_node_id,
                                     prev_idx, prev_term, leader_commit,
                                     entry.index, entry.term,
                                     entry.op_type, entry.payload,
                                     &rt, &ok))
            continue;

        if (rt > term)
        {
            step_down_if_higher(rt);
            return acks;
        }
        if (ok)
        {
            acks++;
            SpinLockAcquire(&RaftLog->mutex);
            RaftLog->peer_match_index[i] = idx;
            RaftLog->peer_next_index[i] = idx + 1;
            SpinLockRelease(&RaftLog->mutex);
        }
    }
    return acks;
}

/*
 * 提交后尽力推送到所有可达 Follower（不仅多数派）。
 * 控制面元数据应在各存活节点最终一致，便于任意节点当选 Leader 后接管。
 */
static void
flush_replication(int64 idx, int64 term)
{
    int round;
    int i;

    for (round = 0; round < 10; round++)
    {
        bool pending = false;

        for (i = 0; i < n_peers; i++)
        {
            int64 match;

            if (peers[i].node_id == pg_raft_node_id)
                continue;

            SpinLockAcquire(&RaftLog->mutex);
            match = RaftLog->peer_match_index[i];
            if (match < idx)
            {
                if (RaftLog->peer_next_index[i] > idx)
                    RaftLog->peer_next_index[i] = idx;
                pending = true;
            }
            SpinLockRelease(&RaftLog->mutex);

            if (match < idx)
                replicate_to_peer(i);
        }

        if (!pending)
            break;
    }
}

static void
discard_uncommitted_entry(int64 idx)
{
    int i;

    if (RaftLog == NULL || idx <= 0)
        return;

    SpinLockAcquire(&RaftLog->mutex);
    if (idx > RaftLog->commit_index && idx == RaftLog->last_log_index)
    {
        log_truncate_after_locked(idx - 1);
        for (i = 0; i < RAFT_MAX_PEERS; i++)
        {
            if (RaftLog->peer_match_index[i] > RaftLog->last_log_index)
                RaftLog->peer_match_index[i] = RaftLog->last_log_index;
            if (RaftLog->peer_next_index[i] > RaftLog->last_log_index + 1)
                RaftLog->peer_next_index[i] = RaftLog->last_log_index + 1;
        }
    }
    SpinLockRelease(&RaftLog->mutex);

    delete_log_entry_sql(idx);
    persist_hard_state_unlocked();
}

int64
pg_raft_consensus_propose(const char *op_type, const char *payload)
{
    int64 idx;
    int64 term;
    int64 committed_upto;
    int   acks;
    int   majority;

    if (!pg_raft_raft_enabled || RaftConsensus == NULL || RaftLog == NULL)
        return 0;

    if (!pg_raft_consensus_is_leader())
        return 0;

    restore_persistent_log_if_needed();
    parse_peers();
    majority = cluster_majority();

    SpinLockAcquire(&RaftConsensus->mutex);
    term = RaftConsensus->current_term;
    SpinLockRelease(&RaftConsensus->mutex);

    SpinLockAcquire(&RaftLog->mutex);
    idx = log_append_locked(term, op_type, payload);
    SpinLockRelease(&RaftLog->mutex);

    if (idx <= 0)
        return 0;

    persist_log_entry_sql(idx, term, op_type, payload, false);
    acks = sync_replicate_index(idx, term);

    SpinLockAcquire(&RaftLog->mutex);
    if (acks >= majority && idx > RaftLog->commit_index)
        RaftLog->commit_index = idx;
    else
    {
        int64 nc = compute_new_commit_index(term);
        if (nc > RaftLog->commit_index)
            RaftLog->commit_index = nc;
    }
    SpinLockRelease(&RaftLog->mutex);
    persist_hard_state_unlocked();

    committed_upto = pg_raft_consensus_commit_index();

    if (committed_upto < idx)
    {
        elog(LOG,
             "pg_raft: reject propose idx=%lld term=%lld because quorum ack is insufficient (%d/%d)",
             (long long) idx,
             (long long) term,
             acks,
             majority);
        discard_uncommitted_entry(idx);
        return 0;
    }

    /* 多数派提交后，继续推送到所有 Follower 再 apply（控制面需全节点一致） */
    mark_log_committed_sql(committed_upto);
    flush_replication(idx, term);
    pg_raft_consensus_apply_pending();
    return idx;
}

/* ---- 观测 ---- */

bool
pg_raft_consensus_is_leader(void)
{
    bool r;

    if (RaftConsensus == NULL)
        return false;

    SpinLockAcquire(&RaftConsensus->mutex);
    r = (RaftConsensus->state == RAFT_LEADER);
    SpinLockRelease(&RaftConsensus->mutex);
    return r;
}

int
pg_raft_consensus_leader_id(void)
{
    int id;

    if (RaftConsensus == NULL)
        return 0;

    SpinLockAcquire(&RaftConsensus->mutex);
    id = (RaftConsensus->state == RAFT_LEADER)
        ? pg_raft_node_id : RaftConsensus->leader_id;
    SpinLockRelease(&RaftConsensus->mutex);
    return id;
}

int64
pg_raft_consensus_term(void)
{
    int64 t;

    if (RaftConsensus == NULL)
        return 0;

    SpinLockAcquire(&RaftConsensus->mutex);
    t = RaftConsensus->current_term;
    SpinLockRelease(&RaftConsensus->mutex);
    return t;
}

int64
pg_raft_consensus_commit_index(void)
{
    int64 c;

    if (RaftLog == NULL)
        return 0;

    SpinLockAcquire(&RaftLog->mutex);
    c = RaftLog->commit_index;
    SpinLockRelease(&RaftLog->mutex);
    return c;
}

int64
pg_raft_consensus_last_applied(void)
{
    int64 a;

    if (RaftLog == NULL)
        return 0;

    SpinLockAcquire(&RaftLog->mutex);
    a = RaftLog->last_applied;
    SpinLockRelease(&RaftLog->mutex);
    return a;
}

/* ---- AppendEntries 接收方 ---- */

static bool
handle_append_entries(int64 in_term, int leader_id,
                      int64 prev_idx, int64 prev_term,
                      int64 leader_commit,
                      int64 entry_idx, int64 entry_term,
                      const char *entry_op, const char *entry_payload,
                      int64 *out_term, int *success)
{
    RaftLogEntry prev;
    bool         has_entry = (entry_idx > 0 && entry_op != NULL && entry_payload != NULL);
    int64        commit_to_mark;

    *success = 0;
    restore_persistent_log_if_needed();

    SpinLockAcquire(&RaftConsensus->mutex);
    if (in_term > RaftConsensus->current_term)
    {
        RaftConsensus->current_term = in_term;
        RaftConsensus->state = RAFT_FOLLOWER;
        RaftConsensus->voted_for = 0;
        RaftConsensus->leader_id = 0;
    }
    *out_term = RaftConsensus->current_term;

    if (in_term < RaftConsensus->current_term)
    {
        SpinLockRelease(&RaftConsensus->mutex);
        return true;
    }

    RaftConsensus->state = RAFT_FOLLOWER;
    RaftConsensus->leader_id = leader_id;
    reset_election_deadline_locked();
    SpinLockRelease(&RaftConsensus->mutex);

    SpinLockAcquire(&RaftLog->mutex);

    if (prev_idx > 0)
    {
        if (!log_get_entry_locked(prev_idx, &prev) || prev.term != prev_term)
        {
            SpinLockRelease(&RaftLog->mutex);
            return true;
        }
    }

    if (has_entry)
    {
        if (entry_idx <= RaftLog->last_log_index)
        {
            RaftLogEntry *exist = log_slot(entry_idx);
            if (exist->index == entry_idx &&
                (exist->term != entry_term ||
                 strcmp(exist->op_type, entry_op) != 0 ||
                 strcmp(exist->payload, entry_payload) != 0))
                log_truncate_after_locked(entry_idx - 1);
        }
        if (entry_idx == RaftLog->last_log_index + 1)
        {
            log_append_locked(entry_term, entry_op, entry_payload);
            SpinLockRelease(&RaftLog->mutex);
            persist_log_entry_sql(entry_idx, entry_term, entry_op, entry_payload, false);
            SpinLockAcquire(&RaftLog->mutex);
        }
        else if (entry_idx <= RaftLog->last_log_index)
        {
            RaftLogEntry *e = log_slot(entry_idx);
            if (e->index != entry_idx)
            {
                SpinLockRelease(&RaftLog->mutex);
                return true;
            }
            SpinLockRelease(&RaftLog->mutex);
            persist_log_entry_sql(entry_idx, entry_term, entry_op, entry_payload, false);
            SpinLockAcquire(&RaftLog->mutex);
        }
        else
        {
            SpinLockRelease(&RaftLog->mutex);
            return true;
        }
    }

    advance_commit_index_locked(leader_commit);
    commit_to_mark = RaftLog->commit_index;
    SpinLockRelease(&RaftLog->mutex);
    persist_hard_state_unlocked();

    if (commit_to_mark > 0)
        mark_log_committed_sql(commit_to_mark);

    *success = 1;
    pg_raft_consensus_apply_pending();
    return true;
}

Datum
pg_raft_append_entries(PG_FUNCTION_ARGS)
{
    int64  in_term = PG_GETARG_INT64(0);
    int    leader_id = PG_GETARG_INT32(1);
    int64  prev_idx = PG_GETARG_INT64(2);
    int64  prev_term = PG_GETARG_INT64(3);
    int64  leader_commit = PG_GETARG_INT64(4);
    int64  entry_idx = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT64(5);
    int64  entry_term = PG_ARGISNULL(6) ? 0 : PG_GETARG_INT64(6);
    char  *entry_op = PG_ARGISNULL(7) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(7));
    char  *entry_payload = PG_ARGISNULL(8) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(8));
    int64  out_term;
    int    success = 0;
    char   out[64];

    if (RaftConsensus == NULL || !pg_raft_raft_enabled)
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));

    (void) handle_append_entries(in_term, leader_id, prev_idx, prev_term,
                                 leader_commit, entry_idx, entry_term,
                                 entry_op, entry_payload, &out_term, &success);

    if (entry_op)
        pfree(entry_op);
    if (entry_payload)
        pfree(entry_payload);

    snprintf(out, sizeof(out), "%lld %d", (long long) out_term, success);
    PG_RETURN_TEXT_P(cstring_to_text(out));
}

Datum
pg_raft_apply_committed(PG_FUNCTION_ARGS)
{
    (void) fcinfo;
    pg_raft_consensus_apply_pending();
    PG_RETURN_BOOL(true);
}

Datum
pg_raft_rpc(PG_FUNCTION_ARGS)
{
    char  *msg;
    char   type[8];
    int64  in_term = 0;
    int    in_node = 0;
    int64  in_last_idx = 0;
    int64  in_last_term = 0;
    int64  my_term;
    int    flag = 0;
    bool   need_persist = false;
    char   out[64];

    if (RaftConsensus == NULL || !pg_raft_raft_enabled)
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));

    msg = text_to_cstring(PG_GETARG_TEXT_PP(0));

    if (sscanf(msg, "%7s %lld %d %lld %lld",
               type,
               (long long *) &in_term,
               &in_node,
               (long long *) &in_last_idx,
               (long long *) &in_last_term) < 3)
    {
        pfree(msg);
        PG_RETURN_TEXT_P(cstring_to_text("0 0"));
    }

    restore_hard_state_if_needed();
    SpinLockAcquire(&RaftConsensus->mutex);
    SpinLockAcquire(&RaftLog->mutex);

    if (in_term > RaftConsensus->current_term)
    {
        RaftConsensus->current_term = in_term;
        RaftConsensus->state = RAFT_FOLLOWER;
        RaftConsensus->voted_for = 0;
        RaftConsensus->leader_id = 0;
        need_persist = true;
    }

    my_term = RaftConsensus->current_term;

    if (strcmp(type, "RV") == 0)
    {
        if (in_term >= my_term &&
            (RaftConsensus->voted_for == 0 ||
             RaftConsensus->voted_for == in_node) &&
            candidate_log_is_up_to_date_locked(in_last_idx, in_last_term))
        {
            RaftConsensus->voted_for = in_node;
            RaftConsensus->state = RAFT_FOLLOWER;
            reset_election_deadline_locked();
            need_persist = true;
            flag = 1;
        }
    }
    else if (strcmp(type, "AE") == 0)
    {
        if (in_term >= my_term)
        {
            RaftConsensus->state = RAFT_FOLLOWER;
            RaftConsensus->leader_id = in_node;
            reset_election_deadline_locked();
            need_persist = true;
            flag = 1;
        }
    }

    my_term = RaftConsensus->current_term;
    SpinLockRelease(&RaftLog->mutex);
    SpinLockRelease(&RaftConsensus->mutex);

    if (need_persist)
        persist_hard_state_unlocked();

    pfree(msg);
    snprintf(out, sizeof(out), "%ld %d", (long) my_term, flag);
    PG_RETURN_TEXT_P(cstring_to_text(out));
}
