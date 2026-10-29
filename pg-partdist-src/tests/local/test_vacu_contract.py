#!/usr/bin/env python3
"""Behavioral tests against a freshly built VACU extension in an isolated lab.

VACU_TEST_DSN points to a worker; VACU_TEST_TSO_DSN to its TSO master.
PSQL defaults to psql. No server restart or cluster-wide GUC changes are made.
"""
import os
import selectors
import shutil
import subprocess
import time
import unittest
import uuid


class VacuContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.worker = os.environ.get("VACU_TEST_DSN")
        cls.master = os.environ.get("VACU_TEST_TSO_DSN")
        cls.psql = os.environ.get("PSQL", "psql")
        if not cls.worker or not cls.master:
            raise unittest.SkipTest("set VACU_TEST_DSN and VACU_TEST_TSO_DSN for an isolated lab")
        if not shutil.which(cls.psql) or not shutil.which("stdbuf"):
            raise unittest.SkipTest("psql and stdbuf are required")
        cls.sql("CREATE EXTENSION IF NOT EXISTS pageinspect")
        if cls.sql("SELECT current_setting('pg_partdist.tso_conninfo') <> ''") != "t":
            raise RuntimeError("worker TSO must be configured before creating any test data")
        cls.sql("SELECT partdist.partdist_global_safe_ts()", master=True)

    @classmethod
    def args(cls, master=False):
        return [cls.psql, cls.master if master else cls.worker,
                "-X", "-qAt", "-v", "ON_ERROR_STOP=1"]

    @classmethod
    def sql(cls, query, master=False, check=True):
        result = subprocess.run(cls.args(master), input=query + ";\n", text=True,
                                capture_output=True, timeout=45)
        if check and result.returncode:
            raise AssertionError(result.stderr)
        return result.stdout.strip() if check else result

    def setUp(self):
        self.table = "vacu_contract_" + uuid.uuid4().hex[:12]
        self.helper = self.table + "_write"
        self.sql(f"SET citus.enable_ddl_propagation=off; CREATE TABLE {self.table} "
                 "(id int, payload text) WITH (autovacuum_enabled=off, toast.autovacuum_enabled=off)")
        self.oid = int(self.sql(f"SELECT '{self.table}'::regclass::oid"))
        self.sql(f"INSERT INTO partdist.partition_map(partition_id,primary_node) VALUES({self.oid},1); "
                 f"SELECT partdist.partdist_set_shard_mvcc({self.oid}::oid,true)")
        self.sql(f"CREATE FUNCTION {self.helper}(oid,bigint,int) RETURNS void "
                 "AS '$libdir/pg_partdist','partdist_shard_clog_write' LANGUAGE C STRICT")

    def tearDown(self):
        self.sql(f"SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS {self.table}; "
                 f"DELETE FROM partdist.partition_map WHERE partition_id={self.oid}; "
                 f"DROP FUNCTION IF EXISTS {self.helper}(oid,bigint,int)")

    def insert(self, ident=1):
        self.sql(f"INSERT INTO {self.table} VALUES ({ident},'retained')")

    def next_xid(self):
        return int(self.sql(f"SELECT partdist.shard_xid_next({self.oid}::oid)"))

    def watermarks(self):
        return self.sql(f"SELECT clog_truncate_before||'/'||shard_vacuum_xid "
                        f"FROM partdist.shard_vacuum_watermarks({self.oid}::oid)")

    def page(self):
        return self.sql(f"SELECT t_xmin::text||'/'||((t_infomask & 768)=768)::text "
                        f"FROM heap_page_items(get_raw_page('{self.table}',0)) "
                        "WHERE lp_flags=1 ORDER BY lp")

    def sweep(self, cutoff):
        return self.sql(f"SELECT swept FROM partdist.shard_vacuum_sweep('{self.table}',{cutoff})")

    def wait_safe(self, cutoff):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            safe = int(self.sql("SELECT partdist.partdist_global_safe_ts()", master=True))
            target = int(self.sql(f"SELECT target_xid FROM partdist.shard_vacuum_target("
                                  f"{self.oid}::oid,{safe},{cutoff})"))
            if target == cutoff - 1:
                return
            time.sleep(0.2)
        self.fail("safe prefix did not advance after releasing all test snapshots")

    def test_frozen_row_ignores_reused_slot_and_still_obeys_delete(self):
        self.insert()
        original = self.page().split("/")[0]
        cutoff = self.next_xid()
        self.wait_safe(cutoff)
        self.assertEqual(self.sweep(cutoff), "t")
        self.assertEqual(self.page(), original + "/true")  # raw xmin is retained
        self.sql(f"SELECT partdist.shard_clog_truncate({self.oid}::oid,{cutoff})")
        # Simulate the old slot now describing a different, aborted transaction.
        self.sql(f"SELECT {self.helper}({self.oid}::oid,{original},3)")
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}"), "1")
        self.sql(f"ANALYZE {self.table}")
        self.sql(f"DELETE FROM {self.table} WHERE id=1")
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}"), "0")

    def test_toast_is_frozen_without_changing_live_value(self):
        self.sql(f"INSERT INTO {self.table} SELECT 1,string_agg(md5(g::text),'') "
                 "FROM generate_series(1,1000) AS g")
        digest = self.sql(f"SELECT md5(payload) FROM {self.table}")
        cutoff = self.next_xid()
        self.wait_safe(cutoff)
        self.assertEqual(self.sweep(cutoff), "t")
        toast = self.sql(f"SELECT reltoastrelid::regclass FROM pg_class WHERE oid={self.oid}")
        frozen = self.sql(f"SELECT bool_and((p.t_infomask & 768)=768) "
                          f"FROM generate_series(0,(pg_relation_size('{toast}')/8192)::int-1) AS g "
                          f"CROSS JOIN LATERAL heap_page_items(get_raw_page('{toast}',g)) AS p "
                          "WHERE p.lp_flags=1")
        self.assertEqual(frozen, "t")
        self.assertEqual(self.sql(f"SELECT md5(payload) FROM {self.table}"), digest)

    def test_manual_cutoff_cannot_override_old_snapshot(self):
        self.insert()
        reader = subprocess.Popen(["stdbuf", "-oL", *self.args()], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            reader.stdin.write(f"BEGIN; SELECT count(*) FROM {self.table};\n\\echo snapshot_ready\n")
            reader.stdin.flush()
            with selectors.DefaultSelector() as ready:
                ready.register(reader.stdout, selectors.EVENT_READ)
                deadline = time.monotonic() + 15
                output = b""
                while time.monotonic() < deadline:
                    if ready.select(timeout=1):
                        output += os.read(reader.stdout.fileno(), 4096)
                    if b"snapshot_ready" in output:
                        break
                else:
                    self.fail("snapshot reader did not become ready")
            self.insert(2)
            cutoff = self.next_xid()
            before = self.watermarks()
            result = self.sql(f"SELECT * FROM partdist.shard_vacuum_sweep('{self.table}',{cutoff})", check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("全局快照安全检查", result.stderr)
            self.assertEqual(self.watermarks(), before)
        finally:
            try:
                reader.communicate("COMMIT;\n", timeout=15)
            except subprocess.TimeoutExpired:
                reader.kill()
                reader.communicate(timeout=5)
                raise
        self.wait_safe(cutoff)
        self.assertEqual(self.sweep(cutoff), "t")

    def test_sweep_and_recovery_are_idempotent(self):
        self.insert()
        cutoff = self.next_xid()
        self.wait_safe(cutoff)
        self.assertEqual(self.sweep(cutoff), "t")
        tb, vx = map(int, self.watermarks().split("/"))
        self.assertEqual(vx, cutoff)
        self.assertLessEqual(tb, vx)
        self.assertEqual(self.sweep(cutoff), "t")  # page pass is idempotent
        self.sql(f"SELECT partdist.shard_vacuum_recover({self.oid}::oid)")
        self.assertEqual(self.watermarks(), f"{cutoff}/{cutoff}")
        self.assertEqual(self.sql(f"SELECT partdist.shard_vacuum_recover({self.oid}::oid)"), "nothing")


if __name__ == "__main__":
    unittest.main(verbosity=2)
