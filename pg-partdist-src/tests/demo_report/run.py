#!/usr/bin/env python3
"""系统功能演示报告验收。默认只打印计划；--run 才连接配置中的专用测试集群。"""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
import traceback

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
CASES = [
    ("preflight", "1.1–1.2", "当前构建、默认协议、1 master + 3 worker、空测试环境"),
    ("ddl", "1.3", "报告原始九表DDL，customer/orders各4片、1主2副、共置"),
    ("catalog_files", "1.4–1.6", "psql展示、节点/分片视图、物理表与分区日志目录"),
    ("writer_crud", "2.1–2.2", "三种持续写入与观察会话、四人增删改查和关联结果"),
    ("transactions", "2.3", "跨worker提交、回滚、错误回滚、快照隔离、并发写冲突"),
    ("failover", "3.1–3.2", "停实际分片主、自动选主、读写与跨分片事务"),
    ("rejoin_transfer", "3.3–3.4", "节点重启、追平、作为从返回、手动切主后读写"),
    ("add_migrate", "3.4 增加节点与副本迁移", "加入worker4、带持续写入迁移两个共置分片"),
    ("raft_logs", "3.5", "报告日志查看函数、主从角色与日志追平"),
    ("drop", "1.7", "删除九表、清理业务物理表/分片登记/Raft组"),
    ("drain", "补充收尾", "prepared、pending、协调登记和路由归零；K1如实失败"),
    ("benchmark", "4", "30/300仓库、PG与ShardPG、100/200/300/400终端；独立性能阶段"),
]


class CheckFailed(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise CheckFailed(message)


def now():
    return datetime.now(timezone(timedelta(hours=8))).isoformat()


def validate_config(cfg, full):
    require(set(cfg["nodes"]) == {"master", "worker1", "worker2", "worker3", "worker4"},
            "nodes必须配置master和worker1–4；worker4为待加入的独立测试节点")
    ports, ids = [], []
    for name, node in cfg["nodes"].items():
        require(isinstance(node["port"], int) and 0 < node["port"] < 65536, "invalid port: " + name)
        require(isinstance(node["node_id"], int) and node["node_id"] > 0, "invalid node_id: " + name)
        ports.append(node["port"])
        ids.append(node["node_id"])
        for key in ("probe_command", "psql_command"):
            require(isinstance(node.get(key), list) and node[key] and all(isinstance(x, str) for x in node[key]),
                    name + " requires argv array: " + key)
        if full and name != "master":
            for key in ("stop_command", "start_command"):
                require(isinstance(node.get(key), list) and node[key], name + " requires " + key)
    require(len(set(ports)) == 5, "fa23的move_primary/move_replica按端口定位，五节点端口须不同")
    require(len(set(ids)) == 5, "node_id must be distinct")
    require(cfg.get("dedicated_test_cluster") is True, "配置须明确指向专用空测试集群")
    require(isinstance(cfg.get("artifact_manifest"), str), "artifact_manifest is required")
    if full:
        require(isinstance(cfg.get("prepare_worker4_command"), list),
                "prepare_worker4_command须为argv数组；worker4已就绪时可为空数组")


class Runner:
    def __init__(self, cfg, args):
        self.cfg, self.args = cfg, args
        self.out = args.output.resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.lock = threading.Lock()
        self.results = []
        self.nodes = cfg["nodes"]
        self.active = ["master", "worker1", "worker2", "worker3"]
        self.down = set()
        self.prefix = "new1demo_" + datetime.now(timezone.utc).strftime("%m%d%H%M%S") + "_"
        self.c, self.o = self.prefix + "customer", self.prefix + "orders"
        self.tables = [self.prefix + x for x in ("customer", "warehouse", "district", "history",
                                               "new_order", "orders", "order_line", "item", "stock")]
        self.expected = [Decimal(1000), Decimal(1000)]
        self.sids = []
        self.physical = []
        self.commit = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
        self.manifest = None
        self.manifest_hash = None
        self.writer_stop = threading.Event()
        self.writer = None
        self.writer_error = None
        self.writer_counts = [0, 0, 0]
        self.writer_total = 0
        self.writer_rows = [0, 0]
        self.writer_offset = 100000
        self.writer_start = threading.Event()
        self.event("run_start", commit=self.commit, prefix=self.prefix)

    def event(self, kind, **fields):
        with self.lock:
            with (self.out / "events.jsonl").open("a") as stream:
                stream.write(json.dumps({"time_beijing": now(), "event": kind, **fields}, default=str,
                                        ensure_ascii=False) + "\n")

    def command(self, label, argv, timeout=180, stdin=None):
        # Commands are argv arrays, not shell fragments. Config may invoke ssh/docker explicitly.
        self.event("command_start", label=label, argv=argv)
        result = subprocess.run(argv, input=stdin, text=True, capture_output=True, timeout=timeout)
        self.event("command_result", label=label, returncode=result.returncode,
                   stdout=result.stdout, stderr=result.stderr)
        require(result.returncode == 0, label + ": command failed; see events.jsonl")
        return result.stdout

    @contextmanager
    def session(self, node="master", timeout_ms=90000):
        import psycopg2
        n = self.nodes[node]
        conn = psycopg2.connect(host=n["host"], port=n["port"], user=n.get("user", "postgres"),
                                dbname=n.get("database", "postgres"), connect_timeout=5,
                                application_name=self.prefix + "test",
                                options=f"-c statement_timeout={timeout_ms} -c lock_timeout={timeout_ms}")
        conn.autocommit = True
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, conn, query, params=None):
        self.event("sql_start", sql=query, params=params)
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall() if cur.description else []
                self.event("sql_result", rows=rows, rowcount=cur.rowcount, status=cur.statusmessage)
                return rows
        except Exception as error:
            self.event("sql_error", sqlstate=getattr(error, "pgcode", None), error=str(error))
            raise

    def q(self, query, params=None, node="master", timeout_ms=90000):
        with self.session(node, timeout_ms) as conn:
            return self.execute(conn, query, params)

    def scalar(self, query, params=None, node="master"):
        rows = self.q(query, params, node)
        require(len(rows) == 1 and len(rows[0]) == 1, "expected exactly one scalar row")
        return rows[0][0]

    def wait(self, label, read, predicate, seconds=None):
        end = time.monotonic() + (seconds or self.cfg.get("converge_seconds", 180))
        last = None
        while time.monotonic() < end:
            try:
                last = read()
                if predicate(last):
                    return last
            except Exception as error:
                last = str(error)
            time.sleep(1)
        raise CheckFailed(label + ": timeout; last=" + repr(last))

    def check_artifacts(self, name):
        found = json.loads(self.command("artifacts_" + name, self.nodes[name]["probe_command"]))
        require(found["source_commit"] == self.commit == self.manifest["source_commit"],
                name + ": deployed release is not current HEAD")
        require(found["manifest_sha256"] == self.manifest_hash and found["files"] == self.manifest["files"],
                name + ": artifact manifest mismatch")
        started = self.scalar("SELECT extract(epoch FROM pg_postmaster_start_time())", node=name)
        require(float(started) >= max(found["latest_artifact_mtime"], found["recorded_at"]),
                name + ": restart after installing the recorded release (N07 changes shared memory)")

    def health(self, name):
        return self.q("SELECT (SELECT count(*) FROM pg_prepared_xacts), partdist.dtx_pending_count(),"
                      "(SELECT count(*) FROM partdist.dtx_coord_tx),"
                      "(SELECT count(*) FROM partdist.dtx_coord_branch),"
                      "(SELECT count(*) FROM partdist.dtx_route)", node=name)[0]

    def preflight(self):
        manifest_bytes = Path(self.cfg["artifact_manifest"]).read_bytes()
        self.manifest = json.loads(manifest_bytes)
        self.manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        subprocess.run(["git", "-C", str(ROOT), "diff", "--quiet", "HEAD"], check=True)
        for name in self.active:
            self.check_artifacts(name)
            require(int(self.scalar("SHOW pg_raft.node_id", node=name)) == self.nodes[name]["node_id"],
                    name + ": node identity mismatch")
            require(self.scalar("SHOW shardpg.first_shard_coordinator", node=name) == "on",
                    name + ": first-shard coordination must stay enabled")
            require(self.health(name) == (0, 0, 0, 0, 0), name + ": test cluster has prior transaction residue")
            require(self.scalar("SELECT count(*) FROM partdist.pg_raft_group_status() g WHERE group_id>0 "
                                "AND NOT EXISTS (SELECT FROM partdist.dtx_ddl_shard d WHERE d.group_id=g.group_id)",
                                node=name) == 0, name + ": test cluster has existing data groups")
        rows = self.q("SELECT name,node_id,host,port,status FROM shardpg.nodes ORDER BY node_id")
        require({(r[1], r[2], r[3]) for r in rows} == {
            (self.nodes[n]["node_id"], self.nodes[n]["host"], self.nodes[n]["port"]) for n in self.active},
                "initial node directory must contain exactly the configured four nodes")
        require(self.scalar("SELECT count(*) FROM pg_dist_node WHERE groupid<>0 AND isactive AND noderole='primary'") == 3,
                "expected three active Citus workers")
        for table in self.tables:
            require(self.scalar("SELECT to_regclass(%s) IS NULL", ("public." + table,)), "test name already exists")

    def shards(self):
        return self.q("SELECT s.logicalrelid::regclass::text, s.shardid, pg_catalog.shard_name(s.logicalrelid,s.shardid),"
                      "s.shardminvalue,s.shardmaxvalue,m.primary_node,m.secondary_nodes,m.primary_term "
                      "FROM pg_dist_shard s LEFT JOIN partdist.partition_map m ON m.partition_id::bigint=s.shardid "
                      "WHERE s.logicalrelid IN (%s::regclass,%s::regclass) ORDER BY s.logicalrelid,s.shardminvalue::bigint",
                      (self.c, self.o))

    def layout_ok(self, rows):
        if len(rows) != 8:
            return False
        by_range = {}
        for table, sid, physical, low, high, leader, replicas, term in rows:
            if leader is None or replicas is None or len(set([leader] + replicas)) != 3 or len(replicas) != 2 or not term:
                return False
            by_range.setdefault((low, high), []).append((leader, sorted(replicas)))
        return len(by_range) == 4 and all(len(v) == 2 and v[0] == v[1] for v in by_range.values())

    def ddl(self):
        text = (HERE / "schema.sql").read_text().replace("{{prefix}}", self.prefix)
        with self.session(timeout_ms=900000) as conn:
            for statement in text.split(";"):
                if statement.strip():
                    self.execute(conn, statement)
        rows = self.wait("four colocated shard pairs", self.shards, self.layout_ok, 600)
        self.sids = [r[1] for r in rows]
        self.physical = [r[2] for r in rows]
        require(all(sum(r[0] == table for r in rows) == 4 for table in (self.c, self.o)),
                "customer and orders must each have four shards")
        require(self.scalar("SELECT count(*) FROM pg_dist_partition WHERE logicalrelid=ANY(%s::regclass[])",
                            (self.tables,)) == 2, "exactly two distributed tables; seven ordinary tables")
        keys = self.q("SELECT logicalrelid::regclass::text,pg_catalog.column_to_column_name(logicalrelid,partkey),colocationid "
                      "FROM pg_dist_partition WHERE logicalrelid IN (%s::regclass,%s::regclass)", (self.c, self.o))
        require({r[0]: r[1] for r in keys} == {self.c: "c_w_id", self.o: "o_w_id"} and
                len({r[2] for r in keys}) == 1, "distribution keys or colocation group differ from report")
        # Prefer literal report keys 1/2, but do not mistake two keys in one shard for a distributed transaction.
        routes = self.q("SELECT k,get_shard_id_for_distribution_column(%s,k),m.primary_node "
                        "FROM generate_series(1,256) k JOIN partdist.partition_map m "
                        "ON m.partition_id=get_shard_id_for_distribution_column(%s,k) ORDER BY k", (self.c, self.c))
        first = routes[0]
        second = next((r for r in routes[1:] if r[1] != first[1] and r[2] != first[2]), None)
        require(second is not None, "cannot find keys crossing both shard and primary worker")
        self.ka, self.kb, self.sid = first[0], second[0], first[1]
        self.shard = next(r[2] for r in rows if r[1] == self.sid)
        self.bucket = self.q("SELECT sids,names FROM partdist.colocated_bucket(%s)", (self.shard,))[0]
        require(len(self.bucket[0]) == 2, "expected customer/orders colocated pair")
        self.event("actual_test_keys", tom=first, ann=second, bucket=self.bucket)

    def catalog_files(self):
        self.q("SELECT * FROM shardpg.shards WHERE table_name IN (%s::regclass,%s::regclass)", (self.c, self.o))
        psql = self.nodes["master"]["psql_command"] + ["-X", "-v", "ON_ERROR_STOP=1", "-P", "pager=off"]
        display = self.command("psql_catalog_display", psql, stdin=f"\\dt {self.prefix}*\n\\dt+ {self.c}\n\\d {self.c}\n\\d+ {self.c}\n")
        require(self.c in display and self.prefix + "warehouse" in display, "psql table display missing tables")
        require("distributed_table" in display and self.shard in display, "patched psql must display distributed type and shards")
        for row in self.shards():
            sid, members = row[1], [row[5]] + row[6]
            for member in members:
                name = next(n for n in self.active if self.nodes[n]["node_id"] == member)
                oid = self.scalar("SELECT partdist.local_partition_for_shard(%s)", (sid,), name)
                require(oid is not None and oid > 0, f"{name}: no physical shard {sid}")
                path = self.scalar("SELECT pg_relation_filepath(%s::oid)", (oid,), name)
                require(path and self.scalar("SELECT (pg_stat_file(%s,true)).size IS NOT NULL", (path,), name),
                        "physical relation file missing")
                files = self.q("SELECT pg_ls_dir(%s)", ("pg_parwal/" + str(oid),), name)
                require(bool(files), f"{name}: shard {sid} has no partition log files")

    def balances(self, conn=None):
        sql = f"SELECT c_balance FROM {self.c} WHERE (c_w_id,c_d_id,c_id) IN ((%s,1,91001),(%s,2,92001)) ORDER BY c_id"
        rows = self.execute(conn, sql, (self.ka, self.kb)) if conn else self.q(sql, (self.ka, self.kb))
        require(len(rows) == 2, "both Tom and Ann must exist")
        return [r[0] for r in rows]

    def expect_balances(self):
        require(self.balances() == self.expected, f"balances: expected {self.expected}")

    def data_digest(self, exclude_writer=False):
        result = {}
        for table in self.tables:
            where, params = "", None
            if exclude_writer and table in (self.c, self.o):
                col = "c" if table == self.c else "o"
                where = f" WHERE NOT ({col}_d_id=9 AND {col}_id >= %s)"
                params = (self.writer_offset,)
            result[table] = self.q(
                "SELECT count(*),md5(coalesce(string_agg(row_to_json(t)::text,chr(10) "
                f"ORDER BY row_to_json(t)::text),'')) FROM {table} t" + where, params)[0]
        return result

    def transfer(self, amount, end="COMMIT"):
        with self.session() as conn:
            self.execute(conn, "BEGIN")
            self.transfer_sql(conn, amount)
            self.execute(conn, end)
        if end == "COMMIT":
            self.expected[0] -= Decimal(amount)
            self.expected[1] += Decimal(amount)
        self.expect_balances()

    def transfer_sql(self, conn, amount):
        for key, district, cid, delta in ((self.ka, 1, 91001, -amount), (self.kb, 2, 92001, amount)):
            rows = self.execute(conn, f"UPDATE {self.c} SET c_balance=c_balance+%s "
                                "WHERE c_w_id=%s AND c_d_id=%s AND c_id=%s RETURNING c_id",
                                (delta, key, district, cid))
            require(rows == [(cid,)], "transfer must update exactly the intended row")

    def start_writer(self):
        self.writer_stop.clear()
        self.writer_start.clear()
        self.writer_counts, self.writer_rows, self.writer_total = [0, 0, 0], [0, 0], 0
        self.writer_error = None
        self.writer_offset += 100000

        def work():
            try:
                with self.session() as conn:
                    i = 0
                    while not self.writer_stop.is_set():
                        key = [self.ka, self.kb][(i // 3) % 2]
                        cid, mode = self.writer_offset + i, i % 3
                        self.execute(conn, "BEGIN")
                        if mode in (0, 2):
                            self.execute(conn, f"INSERT INTO {self.c}(c_w_id,c_d_id,c_id,c_first,c_balance) VALUES(%s,9,%s,'Writer',10)", (key, cid))
                        if mode in (1, 2):
                            self.execute(conn, f"INSERT INTO {self.o}(o_w_id,o_d_id,o_id,o_c_id) VALUES(%s,9,%s,%s)",
                                         (key, cid, cid if mode == 2 else cid - 1))
                        self.execute(conn, "COMMIT")
                        with self.lock:
                            self.writer_counts[mode] += 1
                            self.writer_total += 1
                            self.writer_rows[0] += int(mode in (0, 2))
                            self.writer_rows[1] += int(mode in (1, 2))
                        if i >= 5:
                            self.writer_start.set()
                        i += 1
                        self.writer_stop.wait(self.cfg.get("writer_interval", 0.2))
            except Exception as error:
                self.writer_error = str(error)
                self.writer_start.set()

        self.writer = threading.Thread(target=work, name="demo-writer", daemon=True)
        self.writer.start()
        require(self.writer_start.wait(180), "continuous writer did not become ready")
        require(self.writer_error is None, "writer failed: " + str(self.writer_error))

    def stop_writer(self):
        if self.writer is None:
            return
        self.writer_stop.set()
        self.writer.join(100)
        require(not self.writer.is_alive(), "writer still running; preserve scene and inspect")
        self.writer = None
        require(self.writer_error is None, "writer failed (writes never retried): " + str(self.writer_error))
        require(all(n > 0 for n in self.writer_counts), "all three write modes must complete")

    def writer_visible(self):
        return [self.scalar(f"SELECT count(*) FROM {table} WHERE {column}_d_id=9 AND {column}_id >= %s",
                            (self.writer_offset,)) for table, column in ((self.c, "c"), (self.o, "o"))]

    def writer_crud(self):
        self.start_writer()
        try:
            before = self.writer_visible()
            self.q(f"INSERT INTO {self.c}(c_w_id,c_d_id,c_id,c_first,c_balance) VALUES"
                   "(%s,1,91001,'Tom',0),(%s,1,91002,'Bob',0),(%s,2,92001,'Ann',0),(%s,2,92002,'Eve',0)",
                   (self.ka, self.ka, self.kb, self.kb))
            rows = self.q(f"UPDATE {self.c} SET c_balance=1000 WHERE c_id IN(91001,91002,92001,92002) AND c_d_id IN(1,2) RETURNING c_id")
            require(len(rows) == 4, "four customers must be updated")
            require(self.q(f"SELECT c_first,c_balance FROM {self.c} WHERE c_d_id IN(1,2) ORDER BY c_id") ==
                    [(n, Decimal(1000)) for n in ("Tom", "Bob", "Ann", "Eve")], "CRUD customer values")
            self.q(f"INSERT INTO {self.o}(o_w_id,o_d_id,o_id,o_c_id) VALUES"
                   "(%s,1,71001,91001),(%s,1,71002,91002),(%s,2,72001,92001),(%s,2,72002,92002)",
                   (self.ka, self.ka, self.kb, self.kb))
            join = f"SELECT c.c_first,o.o_id FROM {self.c} c JOIN {self.o} o ON " \
                   "(o.o_w_id,o.o_d_id,o.o_c_id)=(c.c_w_id,c.c_d_id,c.c_id) WHERE c.c_d_id IN(1,2) ORDER BY c.c_id"
            require(self.q(join) == [("Tom", 71001), ("Bob", 71002), ("Ann", 72001), ("Eve", 72002)], "four joined orders")
            rows = self.q(f"DELETE FROM {self.o} WHERE o_c_id IN(91002,92002) AND o_d_id IN(1,2) RETURNING o_id")
            require(len(rows) == 2, "Bob/Eve delete count")
            require(self.q(join) == [("Tom", 71001), ("Ann", 72001)], "Tom/Ann orders remain")
            self.wait("observer sees both tables grow", self.writer_visible,
                      lambda x: x[0] > before[0] and x[1] > before[1])
        finally:
            self.stop_writer()
        require(self.writer_visible() == self.writer_rows, "writer committed totals must equal visible rows")
        self.expect_balances()

    def transactions(self):
        self.transfer(500)
        self.transfer(50, "ROLLBACK")
        with self.session() as conn:
            self.execute(conn, "BEGIN")
            self.transfer_sql(conn, 25)  # Prove that earlier successful changes also roll back.
            try:
                self.execute(conn, f"INSERT INTO {self.o}(o_w_id,o_d_id,o_id,o_c_id) VALUES(%s,1,71001,91001)", (self.ka,))
            except Exception as error:
                require(getattr(error, "pgcode", None) == "23505", "expected duplicate-key SQLSTATE 23505")
            else:
                raise CheckFailed("duplicate key was accepted")
            finally:
                self.execute(conn, "ROLLBACK")
        self.expect_balances()
        require(self.scalar(f"SELECT count(*) FROM {self.o} WHERE o_w_id=%s AND o_d_id=1 AND o_id=71001 AND o_c_id=91001", (self.ka,)) == 1,
                "original order must remain exactly once")
        with self.session() as reader, self.session() as writer:
            self.execute(reader, "BEGIN")  # Test the report's plain BEGIN semantics; do not force REPEATABLE READ.
            snapshot = self.balances(reader)
            self.execute(writer, "BEGIN")
            self.transfer_sql(writer, 100)
            require(self.balances(reader) == snapshot, "uncommitted transfer became visible")
            self.execute(writer, "COMMIT")
            self.expected = [self.expected[0] - 100, self.expected[1] + 100]
            require(self.balances(reader) == snapshot, "snapshot changed inside reader transaction")
            self.execute(reader, "COMMIT")
            require(self.balances(reader) == self.expected, "new transaction did not see committed transfer")
        self.conflict()

    def conflict(self):
        done, sent = threading.Event(), threading.Event()
        outcome = {}
        sql = f"INSERT INTO {self.o}(o_w_id,o_d_id,o_id,o_c_id) VALUES(%s,1,71991,91001)"

        def second():
            try:
                with self.session() as conn:
                    self.execute(conn, "BEGIN")
                    outcome["pid"] = self.execute(conn, "SELECT pg_backend_pid()")[0][0]
                    sent.set()
                    try:
                        self.execute(conn, sql, (self.ka,))
                        outcome["sqlstate"] = "SUCCESS"
                    except Exception as error:
                        outcome["sqlstate"] = getattr(error, "pgcode", None)
                    finally:
                        self.execute(conn, "ROLLBACK")
            except Exception as error:
                outcome["error"] = str(error)
                sent.set()
            finally:
                done.set()

        with self.session() as first:
            self.execute(first, "BEGIN")
            self.execute(first, sql, (self.ka,))
            thread = threading.Thread(target=second, daemon=True)
            thread.start()
            try:
                require(sent.wait(15), "conflicting session did not start")
                # Observe the second backend executing the INSERT while B still owns the key.
                self.wait("overlapping INSERT", lambda: self.q(
                    "SELECT state,query FROM pg_stat_activity WHERE pid=%s", (outcome.get("pid", -1),)),
                    lambda r: bool(r) and r[0][0] == "active" and "71991" in r[0][1], 20)
                require(not done.is_set(), "conflict completed before the owning transaction committed")
                self.execute(first, "COMMIT")
            finally:
                self.execute(first, "ROLLBACK")
                thread.join(100)
            require(done.is_set() and not thread.is_alive(), "conflicting INSERT did not resolve")
        require(outcome.get("sqlstate") in ("23505", "40001") and "error" not in outcome,
                "expected duplicate/serialization conflict, not timeout or arbitrary error: " + repr(outcome))
        require(self.scalar(f"SELECT count(*) FROM {self.o} WHERE o_w_id=%s AND o_d_id=1 AND o_id=71991", (self.ka,)) == 1,
                "concurrent duplicate produced wrong row count")
        self.expect_balances()

    def primary(self, sid=None):
        return self.scalar("SELECT primary_node FROM partdist.partition_map WHERE partition_id=%s", (sid or self.sid,))

    def node_name(self, node_id):
        return next(n for n, v in self.nodes.items() if v["node_id"] == node_id)

    def rw_after_change(self, oid):
        self.expect_balances()
        self.q(f"UPDATE {self.c} SET c_balance=c_balance+5 WHERE c_w_id=%s AND c_d_id=1 AND c_id=91001", (self.ka,))
        self.expected[0] += 5
        self.expect_balances()
        self.q(f"INSERT INTO {self.o}(o_w_id,o_d_id,o_id,o_c_id,o_ol_cnt) VALUES(%s,1,%s,91001,7)", (self.ka, oid))
        require(self.q(f"SELECT o_c_id,o_ol_cnt FROM {self.o} WHERE o_w_id=%s AND o_d_id=1 AND o_id=%s", (self.ka, oid)) == [(91001, 7)], "write on new primary not visible")
        self.transfer(100)
        self.transfer(50, "ROLLBACK")

    def failover(self):
        before = self.data_digest()
        self.failed_node = self.node_name(self.primary())
        require(self.failed_node != "master", "master must not own business shards")
        old_term = self.scalar("SELECT primary_term FROM partdist.partition_map WHERE partition_id=%s", (self.sid,))
        self.down.add(self.failed_node)  # Even a timed-out stop command may have stopped the server.
        self.command("immediate_stop_" + self.failed_node, self.nodes[self.failed_node]["stop_command"])
        self.wait("automatic failover", lambda: self.q("SELECT primary_node,primary_term FROM partdist.partition_map WHERE partition_id=%s", (self.sid,)),
                  lambda r: len(r) == 1 and r[0][0] != self.nodes[self.failed_node]["node_id"] and r[0][1] > old_term)
        self.wait("reads converge after failover", self.balances, lambda x: x == self.expected)
        require(self.data_digest() == before, "failover changed existing data")
        self.rw_after_change(73001)

    def catchup(self):
        # Capture a stable lower bound; do not demand equality with a moving leader tip.
        for sid in self.bucket[0]:
            leader = self.node_name(self.primary(sid))
            target = self.scalar("SELECT commit_index FROM partdist.pg_raft_group_status() WHERE group_id=%s", (sid,), leader)
            plsn = self.scalar("SELECT partdist.pg_raft_group_committed_plsn(%s)", (sid,), leader)
            members = self.q("SELECT ARRAY[primary_node]||secondary_nodes FROM partdist.partition_map WHERE partition_id=%s", (sid,))[0][0]
            for member in members:
                name = self.node_name(member)
                self.wait(f"{name} group {sid} catches commit {target}",
                          lambda n=name, s=sid: self.q("SELECT commit_index,last_applied,state FROM partdist.pg_raft_group_status() WHERE group_id=%s", (s,), n),
                          lambda r: len(r) == 1 and r[0][0] >= target and r[0][1] >= target)
                if name != leader:
                    self.wait(f"{name} group {sid} physically replays {plsn}",
                              lambda n=name, s=sid: self.scalar(
                                  "SELECT partdist.get_follower_applied_part_lsn(partdist.local_partition_for_shard(%s))", (s,), n),
                              lambda x: x is not None and x >= plsn)

    def rejoin_transfer(self):
        self.command("restart_" + self.failed_node, self.nodes[self.failed_node]["start_command"])
        self.wait("node SQL ready", lambda: self.scalar("SELECT 1", node=self.failed_node), lambda x: x == 1)
        self.down.remove(self.failed_node)
        self.check_artifacts(self.failed_node)
        self.catchup()
        require(self.scalar("SELECT state FROM partdist.pg_raft_group_status() WHERE group_id=%s", (self.sid,), self.failed_node) == "follower", "returning node did not remain follower")
        before = self.data_digest()
        self.q("SELECT shardpg.move_primary(%s,%s)", (self.shard, self.nodes[self.failed_node]["port"]), timeout_ms=900000)
        for sid in self.bucket[0]:
            self.wait("colocated manual transfer", lambda s=sid: self.primary(s),
                      lambda x: x == self.nodes[self.failed_node]["node_id"])
        self.wait("reads converge after transfer", self.balances, lambda x: x == self.expected)
        require(self.data_digest() == before, "manual transfer changed existing data")
        self.rw_after_change(73002)

    def add_migrate(self):
        if self.cfg["prepare_worker4_command"]:
            self.command("prepare_worker4", self.cfg["prepare_worker4_command"], timeout=1200)
        self.check_artifacts("worker4")
        require(self.scalar("SHOW shardpg.first_shard_coordinator", node="worker4") == "on", "new worker protocol must stay on")
        self.q("SELECT * FROM shardpg.add_node(%s,%s)",
               (self.nodes["worker4"]["host"], self.nodes["worker4"]["port"]), timeout_ms=900000)
        self.active.append("worker4")
        require(self.scalar("SELECT count(*) FROM pg_dist_node WHERE nodename=%s AND nodeport=%s AND isactive AND noderole='primary'",
                            (self.nodes["worker4"]["host"], self.nodes["worker4"]["port"])) == 1, "new worker missing from Citus")
        for name in self.active:
            self.wait("five control members " + name,
                      lambda n=name: self.scalar("SELECT cluster_size FROM partdist.pg_raft_group_status() WHERE group_id=0", node=n), lambda x: x == 5)
        before = self.scalar(f"SELECT count(*) FROM {self.c}")
        digest_before = self.data_digest()
        source_id = self.q("SELECT secondary_nodes[1] FROM partdist.partition_map WHERE partition_id=%s", (self.sid,))[0][0]
        source = self.node_name(source_id)
        self.start_writer()
        try:
            count_start = self.writer_total
            self.q("SELECT * FROM shardpg.move_replica(%s,%s,%s)",
                   (self.shard, self.nodes[source]["port"], self.nodes["worker4"]["port"]), timeout_ms=900000)
            require(self.writer_total > count_start, "no committed writes overlapped migration")
        finally:
            self.stop_writer()
        require(self.scalar(f"SELECT count(*) FROM {self.c}") == before + self.writer_rows[0], "migration changed customer count")
        require(self.writer_visible() == self.writer_rows, "migration lost/duplicated writer rows")
        require(self.data_digest(exclude_writer=True) == digest_before, "migration changed pre-existing rows")
        for sid, physical in zip(*self.bucket):
            self.wait("replica membership changed", lambda s=sid: self.q("SELECT ARRAY[primary_node]||secondary_nodes FROM partdist.partition_map WHERE partition_id=%s", (s,)),
                      lambda r: bool(r) and source_id not in r[0][0] and self.nodes["worker4"]["node_id"] in r[0][0])
            self.wait("source group removed", lambda s=sid: self.scalar("SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=%s", (s,), source), lambda x: x == 0)
            require(self.scalar("SELECT to_regclass(%s) IS NULL", (physical,), source), "source physical table remains")
        self.catchup()
        self.expect_balances()
        self.transfer(10)
        self.transfer(10, "ROLLBACK")

    def raft_logs(self):
        self.q((HERE / "raft_log.sql").read_text().replace("{{prefix}}", self.prefix))
        self.catchup()
        rows = self.q(f"SELECT * FROM public.{self.prefix}raft_log(%s::regclass,%s,5)", (self.c, self.ka))
        require(bool(rows), "raft_log returned no records")
        roles = {}
        for worker, role, index, term, record, committed in rows:
            require(role in ("primary", "replica") and index is not None and term is not None and record is not None,
                    "raft_log has unreachable/undecodable entries")
            roles[worker] = role
        require(list(roles.values()).count("primary") == 1 and list(roles.values()).count("replica") == 2,
                "raft_log must show one primary and two replicas")
        self.event("raft_log_observed", rows=rows)

    def drop(self):
        self.q("DROP TABLE " + ",".join(self.tables), timeout_ms=900000)
        for name in self.active:
            self.wait("drop groups " + name, lambda n=name: self.scalar(
                "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id=ANY(%s::bigint[])", (self.sids,), n), lambda x: x == 0)
            require(self.scalar("SELECT count(*) FROM partdist.partition_map WHERE partition_id::bigint=ANY(%s::bigint[])", (self.sids,), name) == 0,
                    name + ": dropped partition map remains")
            for physical in self.physical:
                require(self.scalar("SELECT to_regclass(%s) IS NULL", (physical,), name), name + ": dropped physical table remains")
        for table in self.tables:
            require(self.scalar("SELECT to_regclass(%s) IS NULL", (table,)), "logical table remains")
        if self.args.scope == "full":
            self.q(f"DROP FUNCTION public.{self.prefix}raft_log(regclass,integer,integer)")

    def drain(self):
        for name in self.active:
            self.wait("transaction drain (including K1 route) " + name, lambda n=name: self.health(n),
                      lambda r: r == (0, 0, 0, 0, 0))

    def benchmark(self):
        bench = self.cfg.get("benchmark")
        require(bench is not None, "第4节缺少TPC-C驱动配置；N02尚未接入，不能用功能写入器代替TPC-C")
        for system in ("postgresql", "shardpg"):
            environment = json.loads(self.command("tpcc_environment_" + system,
                [arg.format(system=system) for arg in bench["environment_command"]]))
            require(environment.get("shared_buffers_bytes") == 8 * 1024**3 and
                    environment.get("cpu_memory_unlimited") is True and
                    environment.get("physical_hosts") == 3, "benchmark environment differs from report")
            if system == "postgresql":
                require(environment.get("topology") == "1 primary + 2 standbys" and
                        environment.get("synchronous_commit") == "on" and
                        environment.get("synchronous_standby_names", "").startswith("ANY 1"),
                        "PostgreSQL benchmark replication configuration differs from report")
            else:
                require(environment.get("topology") == "1 master + 3 workers" and
                        environment.get("shard_replicas") == 2 and environment.get("source_commit") == self.commit,
                        "ShardPG benchmark topology/build differs from report")
            for warehouses in (30, 300):
                values = dict(system=system, warehouses=str(warehouses), terminals="200", seconds="180")
                def invoke(action, **changes):
                    variables = {**values, **changes}
                    argv = [arg.format(**variables) for arg in bench[action + "_command"]]
                    return self.command(f"tpcc_{system}_{warehouses}_{action}_{variables['terminals']}", argv,
                                        timeout=int(variables["seconds"]) + bench.get("overhead_seconds", 900))
                invoke("prepare")
                invoke("warmup")
                for terminals in (100, 200, 300, 400):
                    invoke("warmup", terminals=str(terminals), seconds="60")
                    result = json.loads(invoke("measure", terminals=str(terminals)))
                    require(result["warehouses"] == warehouses and result["terminals"] == terminals and
                            result["seconds"] >= 180 and result["new_order_commits"] > 0 and
                            result["consistency_passed"] is True and result["unexpected_errors"] == 0,
                            "invalid/inconsistent benchmark result")
                    require(result.get("system") == system, "benchmark system mismatch")
                    self.event("benchmark_result", **result,
                               calculated_tpmC=result["new_order_commits"] * 60 / result["seconds"])

    def run(self):
        failed = False
        try:
            for name, section, title in CASES:
                status, detail = "PASS", ""
                if name == "benchmark" and not self.args.benchmark:
                    status, detail = "SKIP", "性能阶段未请求；功能结果不代表性能验收"
                elif self.args.scope == "core" and name in ("failover", "rejoin_transfer", "add_migrate", "raft_logs"):
                    status, detail = "SKIP", "--scope core未执行高可用/迁移"
                elif failed:
                    status, detail = "BLOCKED", "前序失败，未执行"
                else:
                    print(f"RUN {section} {title}", flush=True)
                    try:
                        getattr(self, name)()
                    except Exception as error:
                        status, detail, failed = "FAIL", str(error), True
                        self.event("case_failure", case=name, traceback=traceback.format_exc())
                self.results.append(dict(case=name, section=section, title=title, status=status, detail=detail))
                self.write_results()
                print(f"{status} {name} {detail}", flush=True)
        finally:
            # Preserve failed tables/transactions for diagnosis; only restore a node this run stopped.
            if self.writer is not None:
                try:
                    self.stop_writer()
                except Exception as error:
                    self.results.append(dict(case="writer_cleanup", status="FAIL", detail=str(error)))
            for node in list(self.down):
                try:
                    self.command("restore_stopped_node_" + node, self.nodes[node]["start_command"])
                    self.wait("restore node ready", lambda n=node: self.scalar("SELECT 1", node=n), lambda x: x == 1)
                    self.down.remove(node)
                except Exception as error:
                    self.results.append(dict(case="node_restore", status="FAIL", detail=str(error)))
            self.write_results()
        return 1 if any(r["status"] in ("FAIL", "BLOCKED") for r in self.results) else (2 if self.args.scope != "full" else 0)

    def write_results(self):
        functional = [r for r in self.results if r["case"] != "benchmark"]
        status = "FAIL" if any(r["status"] in ("FAIL", "BLOCKED") for r in functional) else (
            "PASS" if len(functional) == len(CASES)-1 and all(r["status"] == "PASS" for r in functional) else "INCOMPLETE")
        report = "FAIL" if any(r["status"] == "FAIL" for r in self.results) else (
            "PASS" if len(self.results) == len(CASES) and all(r["status"] == "PASS" for r in self.results) else "INCOMPLETE")
        result = dict(source_commit=self.commit, time_beijing=now(), prefix=self.prefix,
                      functional_status=status, whole_report_status=report, cases=self.results,
                      stopped_nodes=list(self.down), preserved_on_failure=True)
        temp = self.out / "results.tmp"
        temp.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n")
        temp.replace(self.out / "results.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="store_true", help="执行；缺省仅列计划，不连接数据库")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--output", type=Path, default=Path("new1-demo-results-" + datetime.now().strftime("%Y%m%d-%H%M%S")))
    parser.add_argument("--scope", choices=("core", "full"), default="full")
    parser.add_argument("--benchmark", action="store_true", help="另运行报告第4节的外部TPC-C驱动")
    args = parser.parse_args()
    if args.config:
        cfg = json.loads(args.config.read_text())
        validate_config(cfg, args.scope == "full")
    elif args.run:
        parser.error("--run requires --config")
    if not args.run:
        for name, section, title in CASES:
            print(f"{name:18} {section}: {title}")
        print("计划模式：未连接数据库、未构建、未部署。完整功能验收需要已部署当前HEAD的专用四节点集群和待加入的第五节点。")
        return 0
    try:
        import psycopg2  # noqa: F401; optional until --run
    except ImportError:
        parser.error("--run requires psycopg2 (e.g. python3-psycopg2)")
    return Runner(cfg, args).run()


if __name__ == "__main__":
    sys.exit(main())
