#!/usr/bin/env python3
"""Real wrap contracts for the isolated VACU32 lab; no transaction epochs.

32-bit build: seed an EMPTY fixture near UINT32_MAX, cross the real boundary.
8-bit test build: exercise the same uint32 arithmetic through repeated circles.
Only the fixture seeding/restart code touches files, inside its labelled lab.
"""
import os
from pathlib import Path
import selectors
import struct
import subprocess
import time
import unittest
import uuid

from test_vacu_contract import VacuContract


class Session:
    def __init__(self, args):
        self.proc = subprocess.Popen(["stdbuf", "-oL", *args], stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.seq = 0
        self.buffer = b""

    def sql(self, query):
        self.seq += 1
        token = ("xid_done_" + str(self.seq)).encode()
        self.proc.stdin.write(query + ";\n\\echo " + token.decode() + "\n")
        self.proc.stdin.flush()
        deadline = time.monotonic() + 45
        with selectors.DefaultSelector() as ready:
            ready.register(self.proc.stdout, selectors.EVENT_READ)
            while time.monotonic() < deadline:
                if token + b"\n" in self.buffer:
                    result, self.buffer = self.buffer.split(token + b"\n", 1)
                    return result.decode().strip()
                if ready.select(timeout=0.2):
                    chunk = os.read(self.proc.stdout.fileno(), 8192)
                    if not chunk:
                        raise AssertionError(self.proc.stderr.read())
                    self.buffer += chunk
        raise AssertionError("persistent SQL session timed out")

    def close(self):
        if self.proc.poll() is None:
            try:
                self.proc.stdin.write("ROLLBACK;\n\\q\n")
                self.proc.stdin.flush()
                self.proc.communicate(timeout=5)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                self.proc.kill()
                self.proc.communicate()


class XidWrapContract(VacuContract):
    # Inherited VACU contracts run separately through the original driver.
    test_frozen_row_ignores_reused_slot_and_still_obeys_delete = None
    test_toast_is_frozen_without_changing_live_value = None
    test_manual_cutoff_cannot_override_old_snapshot = None
    test_sweep_and_recovery_are_idempotent = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.bits = int(os.environ["VACU_RING_BITS"])
        cls.mask = (1 << cls.bits) - 1
        cls.lab = Path(os.environ["VACU_LAB_ROOT"]).resolve()
        cls.container = os.environ["VACU_CONTAINER"]
        cls.bindir = os.environ.get("VACU_PG_BINDIR", "/work/pg-install/bin")
        cls.datadir = os.environ.get("VACU_PG_DATADIR", "/work/pg-cluster-data/worker1")
        if not str(cls.lab).startswith("/home/yyt/vacu32-test-"):
            raise RuntimeError("wrap tests require their dedicated lab root")
        label = cls.docker("inspect", "--format", '{{index .Config.Labels "yyt.vacu.root"}}', cls.container)
        if label.strip() != str(cls.lab):
            # Reuse the original container only with a private relocated install,
            # data directory and socket; never restart its original worker1.
            expected = f"/tmp/{cls.lab.parent.name}/{cls.lab.name}/work"
            if (label.strip() != "/home/yyt/vacu-bb62a57-20261029T074050Z" or
                    not cls.lab.parent.name.startswith("vacu32-test-") or
                    cls.lab.name not in ("production", "ring8") or
                    cls.bindir != expected + "/pg-install/bin" or
                    cls.datadir != expected + "/pg-cluster-data/worker1"):
                raise RuntimeError("container ownership or scratch paths differ from lab root")
        if cls.sql("SHOW data_directory") != cls.datadir:
            raise RuntimeError("worker DSN does not point to the owned test data directory")
        cls.sql("ALTER SYSTEM SET pg_partdist.shard_vacuum_auto=off")
        cls.sql("SELECT pg_reload_conf()")
        time.sleep(1)

    @classmethod
    def docker(cls, *args):
        result = subprocess.run(["docker", *args], text=True, capture_output=True, timeout=90)
        if result.returncode:
            raise AssertionError(result.stderr + result.stdout)
        return result.stdout

    @classmethod
    def restart(cls, immediate=True):
        cls.docker("exec", "-u", "postgres", cls.container, cls.bindir + "/pg_ctl",
                   "-D", cls.datadir, "-m", "immediate" if immediate else "fast", "-w", "stop")
        cls.docker("exec", "-u", "postgres", cls.container, cls.bindir + "/pg_ctl",
                   "-D", cls.datadir, "-l", cls.datadir + "/wrap-startup.log", "-w", "start")
        cls.sql("SELECT 1")

    def seed_empty(self, start):
        self.assertEqual(self.sql(f"SELECT pg_relation_size('{self.table}')"), "0")
        self.docker("exec", "-u", "postgres", self.container, self.bindir + "/pg_ctl",
                    "-D", self.datadir, "-m", "fast", "-w", "stop")
        # Legacy format is intentionally used only for an empty seeded fixture.
        # Production writes migrate it to the versioned checksummed format.
        target = self.lab / "work/pg-cluster-data/worker1/pg_shard_xid" / str(self.oid)
        with target.open("wb") as out:
            out.write(struct.pack("<IIII", start, start, start, start))
            out.flush()
            os.fsync(out.fileno())
        self.docker("exec", "-u", "postgres", self.container, self.bindir + "/pg_ctl",
                    "-D", self.datadir, "-l", self.datadir + "/wrap-startup.log", "-w", "start")
        self.assertEqual(self.next_xid(), start)

    def wait_safe(self, cutoff):
        deadline = time.monotonic() + 45
        last = "not checked"
        safe = 0
        while time.monotonic() < deadline:
            safe = int(self.sql("SELECT partdist.partdist_global_safe_ts()", master=True))
            last = self.sql(f"SELECT target_xid||'|'||stop_reason FROM partdist.shard_vacuum_target("
                            f"{self.oid}::oid,{safe},{cutoff})")
            target = int(last.split("|", 1)[0])
            if target and max(3, (target + 1) & self.mask) == cutoff:
                return
            time.sleep(0.2)
        self.fail(f"safe prefix did not reach cutoff={cutoff}: shard={self.oid}, "
                  f"safe_ts={safe}, target/reason={last}, watermarks={self.watermarks()}")

    def reclaim(self):
        cutoff = self.next_xid()
        # An already reclaimed empty interval has no target XID. Still repeat
        # sweep/truncate below to check their idempotence and safety gates.
        if self.watermarks() != f"{cutoff}/{cutoff}":
            self.wait_safe(cutoff)
        self.assertEqual(self.sweep(cutoff), "t")
        self.sql(f"SELECT partdist.shard_clog_truncate({self.oid}::oid,{cutoff})")
        self.assertEqual(self.watermarks(), f"{cutoff}/{cutoff}")

    def set_age_thresholds(self, max_age, stop_age):
        """Reload each setting before validating the dependent threshold."""
        self.assertLess(max_age, stop_age)
        max_name = "pg_partdist.shard_vacuum_max_age"
        stop_name = "pg_partdist.shard_xid_stop_age"
        current_max = int(self.sql(f"SHOW {max_name}"))
        settings = [(stop_name, stop_age), (max_name, max_age)]
        if stop_age <= current_max:
            settings.reverse()
        for name, value in settings:
            self.sql(f"ALTER SYSTEM SET {name}={value}")
            self.sql("SELECT pg_reload_conf()")
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if int(self.sql(f"SHOW {name}")) == value:
                    break
                time.sleep(0.1)
            else:
                self.fail(f"{name} did not reload to {value}")

    def test_10_cross_real_boundary_and_crash(self):
        self.seed_empty(self.mask - 12)
        old = int(self.sql(f"INSERT INTO {self.table} VALUES(0,'retained') RETURNING xmin::text"))
        self.assertEqual(old, self.mask - 12)
        self.reclaim()
        self.assertTrue(self.page().endswith("/true"))
        seen = []
        for i in range(1, 25):
            xid = int(self.sql(f"INSERT INTO {self.table} VALUES({i},'v{i}') RETURNING xmin::text"))
            seen.append(xid)
            self.assertGreaterEqual(xid, 3)
            self.assertLessEqual(xid, self.mask)
        self.assertIn(self.mask, seen)
        self.assertIn(3, seen)
        # Crossing a reservation through zero must preserve all still-live
        # CLOG entries, including those in the same file as both arc endpoints.
        xid_array = ",".join(map(str, seen))
        self.assertEqual(self.sql(f"SELECT count(*) FROM unnest(ARRAY[{xid_array}]::bigint[]) x "
                                  f"WHERE partdist.shard_clog_status({self.oid}::oid,x)<>2"), "0")
        self.reclaim()
        digest = self.sql(f"SELECT md5(string_agg(id::text||payload,',' ORDER BY id)) FROM {self.table}")
        self.restart()
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}"), "25")
        self.assertEqual(self.sql(f"SELECT md5(string_agg(id::text||payload,',' ORDER BY id)) FROM {self.table}"), digest)
        self.sql(f"DELETE FROM {self.table} WHERE id=0")
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table} WHERE id=0"), "0")

    def test_20_repeated_reuse_with_toast_and_long_lived_connection(self):
        if self.bits != 8:
            self.skipTest("multiple complete circles use the accelerated build")
        self.sql(f"INSERT INTO {self.table} SELECT 0,string_agg(md5(g::text),'') FROM generate_series(1,1000) g")
        digest = self.sql(f"SELECT md5(payload) FROM {self.table} WHERE id=0")
        expected = {0}
        seen = set()
        reused = 0
        observer = Session(self.args())
        try:
            for i in range(1, 601):
                abort = i % 7 == 0
                query = f"INSERT INTO {self.table} VALUES({i},'v{i}') RETURNING xmin::text"
                xid = int(self.sql("BEGIN; " + query + "; ROLLBACK" if abort else query))
                self.assertGreaterEqual(xid, 3)
                self.assertLessEqual(xid, self.mask)
                reused += xid in seen
                seen.add(xid)
                if not abort:
                    expected.add(i)
                self.assertEqual(observer.sql(f"SELECT count(*) FROM {self.table} WHERE id={i}"), "0" if abort else "1")
                if i % 11 == 0:
                    self.sql(f"BEGIN; DELETE FROM {self.table} WHERE id=0; ROLLBACK")
                if i % 8 == 0:
                    self.reclaim()
                    actual = self.sql(f"SELECT string_agg(id::text,',' ORDER BY id) FROM {self.table}")
                    self.assertEqual(actual, ",".join(map(str, sorted(expected))))
                    self.assertEqual(self.sql(f"SELECT md5(payload) FROM {self.table} WHERE id=0"), digest)
                if i % 100 == 0:
                    print(f"wrap progress: {i}/600 transactions, {reused} repeated raw XIDs", flush=True)
            self.assertGreater(reused, 300)
            self.reclaim()
        finally:
            observer.close()
        self.restart()
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}"), str(len(expected)))
        self.assertEqual(self.sql(f"SELECT md5(payload) FROM {self.table} WHERE id=0"), digest)

    def test_30_old_snapshot_blocks_reclaim_and_stop_is_shard_local(self):
        self.insert()
        self.reclaim()
        original_max = int(self.sql("SHOW pg_partdist.shard_vacuum_max_age"))
        original_stop = int(self.sql("SHOW pg_partdist.shard_xid_stop_age"))
        self.addCleanup(self.set_age_thresholds, original_max, original_stop)
        self.set_age_thresholds(10, 80)
        reader = Session(self.args())
        other_oid = None
        blocked = False
        try:
            try:
                self.assertEqual(reader.sql(f"BEGIN; SELECT count(*) FROM {self.table}"), "1")
                for i in range(2, 100):
                    result = self.sql(f"INSERT INTO {self.table} VALUES({i},'v')", check=False)
                    if result.returncode:
                        self.assertIn("停发", result.stderr)
                        blocked = True
                        break
                self.assertTrue(blocked)
                cutoff = self.next_xid()
                before = self.watermarks()
                denied = self.sql(f"SELECT swept FROM partdist.shard_vacuum_sweep('{self.table}',{cutoff})", check=False)
                self.assertNotEqual(denied.returncode, 0)
                self.assertEqual(self.watermarks(), before)
                self.assertGreater(int(self.sql(f"SELECT count(*) FROM {self.table}")), 1)
                self.sql(f"CREATE TABLE {self.table}_other(id int)")
                other_oid = int(self.sql(f"SELECT '{self.table}_other'::regclass::oid"))
                self.sql(f"INSERT INTO partdist.partition_map(partition_id,primary_node) VALUES({other_oid},1); "
                         f"SELECT partdist.partdist_set_shard_mvcc({other_oid}::oid,true)")
                self.sql(f"INSERT INTO {self.table}_other VALUES(1)")
                self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}_other"), "1")
            finally:
                reader.close()
            self.reclaim()
            self.insert(1000)
        finally:
            self.sql(f"DROP TABLE IF EXISTS {self.table}_other")
            if other_oid is not None:
                self.sql(f"DELETE FROM partdist.partition_map WHERE partition_id={other_oid}")

    def test_35_cleared_reuse_batch_crash_before_authorization(self):
        self.seed_empty(self.mask - 12)
        # A terminal verdict left in a safely retired low slot must be erased
        # before the allocator publishes a reservation that crosses zero.
        self.sql(f"SELECT {self.helper}({self.oid}::oid,3,2)")
        batch = 4096 if self.bits == 32 else 8
        # Move exactly to a reservation edge. For the real 32-bit build the
        # first batch already crosses zero; the accelerated build needs 8 IDs.
        if self.bits == 8:
            for i in range(batch):
                self.insert(i + 1)
        self.sql("ALTER SYSTEM SET pg_partdist.shard_vacuum_fault='before_reserve'")
        self.sql("SELECT pg_reload_conf()")
        time.sleep(1)
        try:
            result = self.sql(f"INSERT INTO {self.table} VALUES(999,'must not be issued')", check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("before_reserve", result.stderr)
        finally:
            self.sql("ALTER SYSTEM SET pg_partdist.shard_vacuum_fault='off'")
            self.sql("SELECT pg_reload_conf()")
        self.restart()
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}"), str(8 if self.bits==8 else 0))
        # The restart resumes from the previous durable reservation, with no
        # tuple using the unacknowledged number and no old COMMITTED slot leak.
        for i in range(30):
            self.insert(1000 + i)
        self.reclaim()
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}"), str(38 if self.bits==8 else 30))

    def test_40_reclaim_publish_crash_completes_clear(self):
        self.insert()
        cutoff = self.next_xid()
        self.wait_safe(cutoff)
        self.assertEqual(self.sweep(cutoff), "t")
        self.sql("ALTER SYSTEM SET pg_partdist.shard_vacuum_fault='after_reclaim_publish'")
        self.sql("SELECT pg_reload_conf()")
        time.sleep(1)
        try:
            result = self.sql(f"SELECT partdist.shard_clog_truncate({self.oid}::oid,{cutoff})", check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("after_reclaim_publish", result.stderr)
        finally:
            self.sql("ALTER SYSTEM SET pg_partdist.shard_vacuum_fault='off'")
            self.sql("SELECT pg_reload_conf()")
        self.restart()
        self.assertEqual(self.sql(f"SELECT partdist.shard_vacuum_recover({self.oid}::oid)"), "truncated")
        self.assertEqual(self.watermarks(), f"{cutoff}/{cutoff}")
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}"), "1")
        self.assertEqual(self.sql(f"SELECT partdist.shard_vacuum_recover({self.oid}::oid)"), "nothing")

    def test_50_native_prepared_blocks_even_after_terminal_decision(self):
        gxid = self.sql("SELECT partdist.partdist_gxid_next()", master=True)
        sts = self.sql("SELECT partdist.partdist_tso_client_start_ts()", master=True)
        gid = "wrap_probe_" + uuid.uuid4().hex[:12]
        prepared = False
        try:
            xid = int(self.sql(f"BEGIN; SET LOCAL pg_partdist.join_info='{gxid},{sts},0'; "
                               f"INSERT INTO {self.table} VALUES(1,'prepared') RETURNING xmin::text; "
                               f"PREPARE TRANSACTION '{gid}'"))
            prepared = True
            self.assertEqual(self.sql(f"SELECT count(*) FROM pg_prepared_xacts WHERE gid='{gid}'"), "1")
            # Test injection: decision has arrived, native prepared transaction
            # has not closed. The pending sweep can close a terminal dtxid=0
            # registration, leaving the native-prepared blocker as the proof.
            self.sql(f"SELECT {self.helper}({self.oid}::oid,{xid},3)")
            self.sql("SELECT partdist.dtx_pending_sweep()")
            self.assertEqual(self.sql("SELECT partdist.dtx_pending_count()"), "0")
            for after_restart in (False, True):
                if after_restart:
                    self.restart()
                cutoff = self.next_xid()
                safe = self.sql("SELECT partdist.partdist_global_safe_ts()", master=True)
                target = self.sql(f"SELECT target_xid FROM partdist.shard_vacuum_target({self.oid}::oid,{safe},{cutoff})")
                self.assertEqual(target, "0")
                denied = self.sql(f"SELECT swept FROM partdist.shard_vacuum_sweep('{self.table}',{cutoff})", check=False)
                self.assertNotEqual(denied.returncode, 0)
                self.assertEqual(self.watermarks(), "0/0")
        finally:
            if prepared:
                self.sql(f"ROLLBACK PREPARED '{gid}'")
        self.reclaim()
        self.assertEqual(self.sql(f"SELECT count(*) FROM {self.table}"), "0")


if __name__ == "__main__":
    unittest.main(verbosity=2)
