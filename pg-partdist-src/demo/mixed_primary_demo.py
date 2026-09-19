#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ShardPG 演示：同一节点上的混合主从（1 协调者 + 3 worker）

一张 3 分片的表，每个分片一个 Raft 组、成员 = 全部 3 台 worker。组主落在不同 worker 上，
于是**每台 worker 同时是一个分片的主、另外两个分片的从**。在这个拓扑上依次演示：
TSO、分片 xid 宇宙、同节点读闸门、Raft 复制 + 惰性回放 + 逐字节一致、两个并发事务的快照隔离
与写写冲突、受控切主（读不中断、发号不断档、旧主自动归队）、宕机切主（两组同时切、只当从的
分片不受影响）、跨分片 2PC（守恒 / 回滚 / 原子性）、终检。

两种跑法，步骤定义是**同一份**：
  python3 mixed_primary_demo.py --auto      全自动：执行每步的建议 SQL 并逐项断言（PASS/FAIL）
  python3 mixed_primary_demo.py             交互式：每步讲解 + 建议 SQL，你可以逐条执行，
                                             也可以**自己写 SQL** 在任意节点 / 会话上验证
  python3 mixed_primary_demo.py --cleanup   只做清理

其它选项：--keep（自动模式跑完保留现场）、--from N（交互式从第 N 步开始，前面的状态须已在）
环境变量：CONTAINER（默认 pg-test-container）
"""
import argparse
import os
import re
import select
import subprocess
import sys
import threading
import time
import unicodedata

try:
    import readline  # noqa: F401  —— 让 input() 有历史与行编辑
except ImportError:
    pass

CONTAINER = os.environ.get("CONTAINER", "pg-test-container")
PSQL_BIN = "/work/pg-install/bin/psql"
PG_CTL = "/work/pg-install/bin/pg_ctl"
COORD = 5432
TABLE = "mx_stock"
NROWS = 24                     # 第 5 步写入 sku 101..100+NROWS
HERE = os.path.dirname(os.path.abspath(__file__))
PAGECMP = os.path.join(HERE, "..", "tests", "pagecmp.py")
VIS = "PGOPTIONS=-c citus.override_table_visibility=false"   # 让 worker 上的分片表对 SQL 可见

TTY = sys.stdout.isatty()


def col(code, s):
    return f"\033[{code}m{s}\033[0m" if TTY else s


def bold(s): return col("1", s)
def dim(s): return col("2", s)
def green(s): return col("32", s)
def red(s): return col("31", s)
def yellow(s): return col("33", s)
def cyan(s): return col("36", s)
def magenta(s): return col("35", s)


def dwidth(s):
    s = re.sub(r"\033\[[0-9;]*m", "", s)
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in s)


def pad(s, w):
    return s + " " * max(0, w - dwidth(s))


# ─────────────────────────── 一次性查询（脚本内部用） ───────────────────────────

def dexec(args, timeout=120, stdin_data=None):
    cmd = ["docker", "exec", "-i", "-u", "postgres", CONTAINER] + args
    try:
        return subprocess.run(cmd, input=stdin_data, capture_output=True, text=True,
                              timeout=timeout)
    except subprocess.TimeoutExpired:
        return None


def qraw(port, sql, timeout=60):
    """→ (rc, stdout, stderr)；超时 rc=-1"""
    cmd = ["docker", "exec", "-i", "-u", "postgres", "-e", VIS, CONTAINER, PSQL_BIN,
           "-h", "/tmp", "-p", str(port), "-U", "postgres", "-d", "postgres",
           "-X", "-q", "-At", "-c", sql]
    try:
        r = subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                           timeout=timeout)
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    return r.returncode, r.stdout, r.stderr


def q(port, sql, timeout=60):
    """最后一行；出错 / 连不上返回 None，无行返回 ''"""
    rc, out, _ = qraw(port, sql, timeout)
    if rc != 0:
        return None
    lines = out.splitlines()
    return lines[-1] if lines else ""


def qlines(port, sql, timeout=60):
    rc, out, _ = qraw(port, sql, timeout)
    if rc != 0:
        return None
    return [l for l in out.splitlines() if l != ""]


# ─────────────────────────── 常驻 psql 会话（交互 / 并发事务用） ───────────────────────────

MARK_RE = re.compile(r"__MX_END_\d+__\n?")


class Session:
    """一个常驻的 psql 进程：事务可以跨多次输入；语句被锁住时不会卡死界面。"""

    def __init__(self, name, port):
        self.name, self.port = name, port
        self.proc = None
        self.seq = 0
        self.pending = None      # 尚未等到的结束标记
        self.buf = b""
        self.last_async = None   # 挂起语句后来返回的输出（给 \wait 用）

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def spawn(self):
        self.close()
        # ★ stderr 必须在**容器里**并进 stdout：docker exec（无 -t）把两个流分开复用传输，二者之间的先后
        #   没有保证 —— 实测报错（stderr）偶尔晚于结束标记（stdout）到达，被算到下一条语句头上。
        cmd = ["docker", "exec", "-i", "-u", "postgres", "-e", VIS, CONTAINER, "bash", "-c",
               f"exec {PSQL_BIN} -h /tmp -p {self.port} -U postgres -d postgres -X "
               f"-P pager=off -v ON_ERROR_STOP=0 2>&1"]
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, bufsize=0)
        self.pending, self.buf = None, b""

    def close(self):
        if self.proc is not None:
            try:
                self.proc.stdin.close()
            except Exception:
                pass
            try:
                self.proc.kill()
            except Exception:
                pass
            self.proc = None
        self.pending, self.buf = None, b""

    def run(self, sql, timeout=60):
        """→ (输出, 是否已结束, 附注)"""
        note, prev = "", ""
        if self.pending:
            out, done = self.collect(0.5)
            if out.strip():                  # 上一条挂起的语句这时才返回：别丢，跟新输出一起给出去
                prev = f"（{self.name} 上一条挂起语句的结果）\n{out.rstrip()}\n（以下是本条）\n"
                self.last_async = (self.last_async or "") + out
            if not done:
                note = f"（会话 {self.name} 上一条语句还没返回，新语句排在它后面）"
        if not self.alive():
            if self.proc is not None:
                note += f"（会话 {self.name} 的连接已断开，已重新连接）"
            self.spawn()
        self.seq += 1
        marker = f"__MX_END_{self.seq}__"
        body = sql.rstrip()
        last = body.splitlines()[-1].strip() if body else ""
        if body and not body.endswith(";") and not last.startswith("\\") and "\\g" not in last:
            body += ";"
        try:
            self.proc.stdin.write((body + "\n\\echo " + marker + "\n").encode())
        except (BrokenPipeError, OSError):
            self.spawn()
            note += f"（会话 {self.name} 已重新连接）"
            self.proc.stdin.write((body + "\n\\echo " + marker + "\n").encode())
        self.pending = marker
        out, done = self.collect(timeout)
        return prev + out, done, note

    def collect(self, timeout):
        """读到当前结束标记为止；超时返回已到的部分，done=False（timeout=0 = 只取管道里现成的）"""
        t0 = time.time()
        while True:
            if self.pending and self.pending.encode() in self.buf:
                idx = self.buf.index(self.pending.encode())
                out = self.buf[:idx]
                self.buf = self.buf[idx + len(self.pending):].lstrip(b"\n")
                self.pending = None
                return MARK_RE.sub("", out.decode(errors="replace")), True
            left = timeout - (time.time() - t0)
            r, _, _ = select.select([self.proc.stdout], [], [], max(0.0, min(0.2, left)))
            if r:
                d = os.read(self.proc.stdout.fileno(), 65536)
                if not d:          # EOF：psql 退出了（连接断开 / 节点宕机）
                    self.proc.wait()
                    out = self.buf.decode(errors="replace")
                    self.buf, self.pending = b"", None
                    return MARK_RE.sub("", out) + "\n[连接已断开]\n", True
                self.buf += d
                continue
            if left <= 0:
                out, self.buf = self.buf, b""
                return MARK_RE.sub("", out.decode(errors="replace")), False


# ─────────────────────────── 集群视图 ───────────────────────────

class Cluster:
    def __init__(self):
        ws = qlines(COORD, "SELECT nodeport FROM pg_dist_node WHERE noderole='primary' "
                           "AND groupid<>0 AND isactive ORDER BY nodeport")
        if ws is None:
            sys.exit(red(f"连不上协调者 :{COORD}（容器 {CONTAINER} 在跑吗？）"))
        self.workers = [int(x) for x in ws]
        self.name = {COORD: "coord"}
        for i, p in enumerate(self.workers):
            self.name[p] = f"w{i + 1}"
        self.nid, self.ddir = {}, {}
        for p in [COORD] + self.workers:
            self.nid[p] = int(q(p, "SHOW pg_raft.node_id") or 0)
            self.ddir[p] = q(p, "SHOW data_directory")
        self.port_of_nid = {v: k for k, v in self.nid.items()}
        self.port_of_name = {v: k for k, v in self.name.items()}
        self.allmem = "ARRAY[" + ",".join(str(self.nid[p]) for p in self.workers) + "]"

    def up(self, port):
        return q(port, "SELECT 1", timeout=15) == "1"

    def shards(self):
        r = qlines(COORD, f"SELECT shardid FROM pg_dist_shard WHERE logicalrelid='{TABLE}'::regclass "
                          "ORDER BY shardid")
        return [int(x) for x in r] if r else []

    def placement(self, sid):
        v = q(COORD, "SELECT n.nodeport FROM pg_dist_placement p JOIN pg_dist_node n "
                     f"ON n.groupid=p.groupid AND n.noderole='primary' WHERE p.shardid={sid}")
        return int(v) if v else None

    def state(self, port, sid):
        return q(port, f"SELECT state FROM partdist.pg_raft_group_status() WHERE group_id={sid}",
                 timeout=15)

    def leader(self, sid):
        for p in self.workers:
            if self.state(p, sid) == "leader":
                return p
        return None

    def registered(self, sid, at=COORD):
        v = q(at, f"SELECT primary_node FROM partdist.partition_map WHERE partition_id={sid}")
        return self.port_of_nid.get(int(v)) if v else None

    def loid(self, port, sid):
        return q(port, f"SELECT partdist.local_partition_for_shard({sid})")

    def flush(self, port, sid):
        v = q(port, f"SELECT partdist.get_partition_flush_lsn(partdist.local_partition_for_shard({sid}))")
        return int(v) if v and v.lstrip("-").isdigit() else None

    def slot(self, port, sid):
        """→ (armed, applied) 或 None（没有回放槽位）"""
        v = q(port, "SELECT armed::text||'|'||applied FROM partdist.replay_status() "
                    f"WHERE shard=partdist.local_partition_for_shard({sid})", timeout=15)
        if not v:
            return None
        a, ap = v.split("|")
        return a == "true", int(ap)

    def keys_in(self, sid, lo, hi, n):
        r = qlines(COORD, f"SELECT g FROM generate_series({lo},{hi}) g WHERE "
                          f"get_shard_id_for_distribution_column('{TABLE}', g)={sid} ORDER BY g LIMIT {n}")
        return [int(x) for x in r] if r else []

    def set_workers(self, guc, val):
        for p in self.workers:
            if val is None:
                q(p, f"ALTER SYSTEM RESET {guc}")
            else:
                q(p, f"ALTER SYSTEM SET {guc} = {val}")
            q(p, "SELECT pg_reload_conf()")

    def node_stop(self, port, mode="immediate"):
        d = self.ddir[port]
        return dexec(["bash", "-c", f"{PG_CTL} -D '{d}' -m {mode} stop -w -t 60"], timeout=90)

    def node_start(self, port):
        d = self.ddir[port]
        r = dexec(["bash", "-c", f"{PG_CTL} -D '{d}' status >/dev/null 2>&1 || "
                                 f"{PG_CTL} start -D '{d}' -l '{d}/pg.log' -o '-p {port}' -w -t 60"],
                  timeout=90)
        for _ in range(60):
            if self.up(port):
                return True
            time.sleep(1)
        return False


# ─────────────────────────── 输出解析 / 期望 ───────────────────────────

SEP_RE = re.compile(r"^-+(\+-+)*$")


def nrows(out):
    m = re.search(r"\((\d+) rows?\)", out)
    return int(m.group(1)) if m else None


def table_rows(out):
    """psql 对齐输出 → [[列...], ...]"""
    lines = out.splitlines()
    for i, l in enumerate(lines):
        if SEP_RE.match(l.strip()):
            rows = []
            for r in lines[i + 1:]:
                if re.match(r"^\(\d+ rows?\)$", r.strip()) or r.strip() == "":
                    break
                rows.append([c.strip() for c in r.split("|")])
            return rows
    return []


def scalar(out):
    rows = table_rows(out)
    return rows[0][0] if rows and rows[0] else None


def has_error(out):
    return bool(re.search(r"^(ERROR|FATAL):", out, re.M))


def X_ok(desc="无报错"):
    return desc, lambda o, c: not has_error(o)


def X_has(s, desc=None):
    return desc or f"输出含「{s}」", lambda o, c: s in o and not has_error(o)


def X_err(s, desc=None):
    return desc or f"报错，含「{s}」", lambda o, c: has_error(o) and s in o


def X_rows(n, desc=None):
    return desc or f"返回 {n} 行", lambda o, c: nrows(o) == n and not has_error(o)


def X_val(pred, desc):
    def f(o, c):
        v = scalar(o)
        try:
            return v is not None and pred(v, c)
        except Exception:
            return False
    return desc, f


def X_col(i, want, desc):
    return desc, lambda o, c: (lambda r: bool(r) and len(r[0]) > i and r[0][i] == want)(table_rows(o))


def X_fn(desc, fn):
    def f(o, c):
        try:
            return bool(fn(o, c)) and not has_error(o)
        except Exception:
            return False
    return desc, f


def row_of(o, key):
    return next((r for r in table_rows(o) if r and r[0] == str(key)), None)


class Sug:
    """一条建议：在哪个会话上执行什么；可带期望 / 等待重试 / 取值保存"""

    def __init__(self, sess, sql, note="", expect=None, retry=0, pending=False, capture=None,
                 timeout=60, capture_col=0):
        self.sess, self.sql, self.note = sess, sql.strip(), note
        self.expect, self.retry, self.pending = expect, retry, pending
        self.capture, self.capture_col, self.timeout = capture, capture_col, timeout
        self.done = False


class Step:
    def __init__(self, title, intro, action=None, action_desc=None, suggest=None, checks=None,
                 finish=None):
        self.title, self.intro = title, intro
        self.action, self.action_desc = action, action_desc
        self.suggest, self.checks, self.finish = suggest, checks, finish
        self.sugs = []


# ─────────────────────────── 后台读探针（切主 / 宕机时量读可用性） ───────────────────────────

class BgReader:
    """后台读探针。★ 用**常驻连接**，并让数据库自己报服务端耗时：
    每读一次就起一个 docker exec + 新连接的话，测到的是"起进程 + 建连 + 查询"的总时间 ——
    实测 docker exec 空载 ~300 ms、2 vCPU 饱和时 ~1.1 s（切主后旧主自动重供基线会把 CPU 吃满），
    早先报过的"读用了 5.3 s"就是这个测量开销，数据库那侧那条读只用了几百毫秒。"""

    def __init__(self, probes):
        self.probes = probes          # [(标签, sql)]
        self.samples = []             # (t, 标签, 状态, 服务端毫秒, 客户端毫秒, 报错)
        self.stop_ev = threading.Event()
        self.t0 = time.time()
        self.sess = Session("bg", COORD)
        self.th = threading.Thread(target=self.loop, daemon=True)
        self.th.start()

    def loop(self):
        while not self.stop_ev.is_set():
            for label, sql in self.probes:
                q2 = sql.rstrip(";").replace("SELECT count(*)",
                     "SELECT count(*), round(extract(epoch from clock_timestamp() - statement_timestamp()) * 1000)", 1) + ";"
                a = time.time()
                out, done, _ = self.sess.run(q2, timeout=60)
                cms = int((time.time() - a) * 1000)
                if not done:
                    self.samples.append((a - self.t0, label, "fail", None, cms, "60 s 未返回"))
                    self.sess.close()
                    continue
                if has_error(out):
                    msg = next((l for l in out.splitlines() if l.startswith("ERROR")), out.strip()[:160])
                    st = "refused" if "副本壳表" in out else "fail"
                    self.samples.append((a - self.t0, label, st, None, cms, msg))
                else:
                    rows = table_rows(out)
                    sms = int(float(rows[0][1])) if rows and len(rows[0]) > 1 and rows[0][1] else None
                    self.samples.append((a - self.t0, label, "ok", sms, cms, ""))
            self.stop_ev.wait(0.2)

    def stop(self):
        self.stop_ev.set()
        self.th.join(timeout=70)
        self.sess.close()

    def summary(self):
        res = {}
        for label, _ in self.probes:
            ss = [s for s in self.samples if s[1] == label]
            bad = [s for s in ss if s[2] != "ok"]
            res[label] = dict(
                n=len(ss), ok=len(ss) - len(bad), fail=len(bad),
                refused=sum(1 for s in bad if s[2] == "refused"),
                window=(bad[-1][0] - bad[0][0] + bad[-1][4] / 1000.0) if bad else 0.0,
                maxms=max((s[3] for s in ss if s[3] is not None), default=0),
                maxclient=max((s[4] for s in ss), default=0),
                msgs=list(dict.fromkeys(s[5][:120] for s in bad))[:2])
        return res


# ─────────────────────────── 演示主体 ───────────────────────────

class Demo:
    def __init__(self, auto=False, keep=False):
        self.auto, self.keep = auto, keep
        self.cl = Cluster()
        self.sessions = {}
        self.cur = "coord"
        self.results = []
        self.ctx = {}
        self.bg = None
        self.steps = build_steps(self)
        self.idx = 0
        if os.path.exists(PAGECMP):
            subprocess.run(["docker", "cp", PAGECMP, f"{CONTAINER}:/tmp/pagecmp.py"],
                           capture_output=True)

    # —— 会话 ——
    def sess(self, name):
        if name not in self.sessions:
            if name in ("A", "B"):
                port = COORD
            elif name in self.cl.port_of_name:
                port = self.cl.port_of_name[name]
            else:
                return None
            self.sessions[name] = Session(name, port)
        return self.sessions[name]

    def drop_sessions(self, port=None):
        for s in self.sessions.values():
            if port is None or s.port == port:
                s.close()

    # —— 结果记账 ——
    def record(self, desc, ok, detail=""):
        self.results.append((desc, ok))
        if ok:
            print("  " + green("PASS") + f"  {desc}" + (dim(f"（{detail}）") if detail else ""))
        else:
            print("  " + red("FAIL") + f"  {desc}" + (f"（{detail}）" if detail else ""))

    def check(self, desc, actual, expected):
        ok = actual is not None and str(actual) == str(expected)
        self.record(desc, ok, "" if ok else f"实际={actual!r} 期望={expected!r}")
        return ok

    # —— 上下文里常用的名字 ——
    def n(self, port):
        return "（无）" if port is None else self.cl.name.get(port, f":{port}")

    def S(self, i):
        return self.ctx["S"][i]

    def sname(self, sid):
        return f"S{self.ctx['S'].index(sid) + 1}" if sid in self.ctx.get("S", []) else str(sid)

    def load_topology(self):
        s = self.cl.shards()
        if len(s) == 3:
            self.ctx["S"] = s
            self.ctx.setdefault("P0", {sid: self.cl.placement(sid) for sid in s})
        return len(s) == 3

    # ═══════════════ 执行一条建议 ═══════════════
    def run_sug(self, sg, show=True):
        sname = sg.sess
        if sg.sql.startswith("\\") and self.is_meta(sg.sql):
            if show:
                print(cyan(f"[{sname}]") + bold(f"> {sg.sql}") + (dim(f"   -- {sg.note}") if sg.note else ""))
            out = self.run_meta(sg.sql, sname)
            ok = self.judge(sg, out)
            sg.done = True
            return out, ok
        s = self.sess(sname)
        if s is None:
            print(red(f"没有会话 {sname}"))
            return "", False
        t_end = time.time() + sg.retry
        first = True
        while True:
            if show and first:
                print(cyan(f"[{sname}]") + bold("> " + sg.sql.replace("\n", "\n" + " " * (len(sname) + 4)))
                      + (dim(f"   -- {sg.note}") if sg.note else ""))
            first = False
            out, done, note = s.run(sg.sql, timeout=(4 if sg.pending else sg.timeout))
            if sg.retry and sg.expect and not sg.expect[1](out, self.ctx) and time.time() < t_end:
                sys.stdout.write(dim("  …等待中（每 2 s 重试）\r"))
                sys.stdout.flush()
                time.sleep(2)
                continue
            break
        if show:
            if note:
                print(yellow(note))
            if out.strip():
                print(out.rstrip())
            if not done:
                print(yellow(f"  ⏳ 会话 {sname} 的语句还没返回（在等锁？）。它返回后，你下一次输入时会显示；"
                             f"也可以用  @{sname} \\wait  等它。"))
        if sg.pending:
            ok = self.judge_pending(sg, done)
        else:
            if sg.capture and done:
                rows = table_rows(out)
                self.ctx[sg.capture] = rows[0][sg.capture_col] if rows and len(rows[0]) > sg.capture_col else None
            ok = self.judge(sg, out)
        sg.done = True
        return out, ok

    def judge(self, sg, out):
        if not sg.expect:
            return True
        desc, fn = sg.expect
        ok = fn(out, self.ctx)
        if self.auto:
            self.record(f"[{sg.sess}] {desc}", ok)
        else:
            print(("  " + green("✓ 符合预期：") if ok else "  " + red("✗ 与预期不符：")) + desc)
        return ok

    def judge_pending(self, sg, done):
        desc = "语句被挡住等待（没有立即返回）"
        ok = not done
        if self.auto:
            self.record(f"[{sg.sess}] {desc}", ok)
        else:
            print(("  " + green("✓ 符合预期：") if ok else "  " + red("✗ 与预期不符：")) + desc)
        return ok

    def poll_async(self):
        """交互式：挂起的语句有了输出就打印出来（同时攒给 \\wait）"""
        for s in self.sessions.values():
            if s.pending and s.alive():
                out, done = s.collect(0)
                if out.strip():
                    print(magenta(f"\n[{s.name}] 之前挂起的语句返回了：\n") + out.rstrip())
                    s.last_async = (s.last_async or "") + out

    # ═══════════════ 元命令 ═══════════════
    META = ("\\roles", "\\cmp", "\\gbegin", "\\wait", "\\bg", "\\node", "\\where", "\\cheat",
            "\\next", "\\n", "\\back", "\\b", "\\step", "\\list", "\\show", "\\run", "\\all",
            "\\check", "\\use", "\\sessions", "\\reset", "\\help", "\\?", "\\q", "\\quit",
            "\\cleanup", "\\c", "\\connect")

    def is_meta(self, line):
        w = line.split()[0] if line.split() else ""
        return w in self.META

    def run_meta(self, line, sname=None):
        parts = line.split()
        cmd, args = parts[0], parts[1:]
        sname = sname or self.cur
        if cmd == "\\roles":
            return self.show_roles()
        if cmd == "\\cmp":
            sids = [self.parse_sid(a) for a in args] if args else None
            return self.compare(sids)
        if cmd == "\\gbegin":
            sid = self.parse_sid(args[0]) if args else self.S(0)
            return self.gbegin(sname, sid)
        if cmd == "\\wait":
            s = self.sess(args[0] if args else sname)
            if s is None:
                return ""
            if s.pending:
                out, done = s.collect(30)
                print(out.rstrip())
                if not done:
                    print(yellow("  还在等……"))
                return out
            out = s.last_async or ""
            print(out.rstrip() if out.strip() else dim("  （没有挂起的语句）"))
            s.last_async = None
            return out
        if cmd == "\\bg":
            return self.bg_cmd(args[0] if args else "status")
        if cmd == "\\node":
            if len(args) != 2 or args[1] not in self.cl.port_of_name:
                print("用法：\\node stop|start w1|w2|w3")
                return ""
            port = self.cl.port_of_name[args[1]]
            if args[0] == "stop":
                self.drop_sessions(port)
                r = self.cl.node_stop(port)
                msg = f"{args[1]}（:{port}）已按 immediate 停止（模拟宕机）" if r is not None and r.returncode == 0 \
                    else f"停止失败：{r.stderr if r else 'timeout'}"
            else:
                ok = self.cl.node_start(port)
                self.drop_sessions(port)
                msg = f"{args[1]}（:{port}）已拉起" if ok else f"{args[1]} 拉起失败"
            print(msg)
            return msg
        if cmd == "\\where":
            if not args or not args[0].isdigit():
                print("用法：\\where <sku>")
                return ""
            sid = q(COORD, f"SELECT get_shard_id_for_distribution_column('{TABLE}', {args[0]})")
            port = self.cl.registered(int(sid)) if sid else None
            msg = f"sku {args[0]} → 分片 {sid}（{self.sname(int(sid)) if sid else '?'}）→ 当前主 {self.n(port) if port else '?'}"
            print(msg)
            return msg
        if cmd == "\\cheat":
            print(CHEAT)
            return ""
        print(yellow(f"{cmd} 只能在交互式提示符下用"))
        return ""

    def parse_sid(self, a):
        a = a.strip()
        if a.upper().startswith("S") and a[1:].isdigit() and "S" in self.ctx:
            return self.S(int(a[1:]) - 1)
        return int(a)

    # —— \gbegin：开一个全局事务（跨分片写要用） ——
    def gbegin(self, sname, sid):
        s = self.sess(sname)
        sql = (f"SELECT partdist.partdist_gxid_next()||','||partdist.partdist_tso_client_start_ts()"
               f"||',{sid}' AS ji \\gset\n"
               "BEGIN;\n"
               "SET LOCAL citus.propagate_set_commands = 'local';\n"
               "SET LOCAL pg_partdist.join_info = :'ji';\n"
               "\\echo 全局事务已开始 join_info = :ji   (gxid, start_ts, 协调者分片)")
        out, done, note = s.run(sql)
        print(dim("  等价于：SELECT gxid_next()||','||tso_start_ts()||',<分片>' AS ji \\gset; BEGIN; "
                  "SET LOCAL citus.propagate_set_commands='local'; SET LOCAL pg_partdist.join_info=:'ji';"))
        print(out.rstrip())
        return out

    # —— \roles：同一节点混合主从一览 ——
    def show_roles(self):
        if not self.load_topology():
            print(yellow(f"表 {TABLE} 还没建（或不是 3 分片）"))
            return ""
        S = self.ctx["S"]
        arr = "ARRAY[" + ",".join(map(str, S)) + "]::bigint[]"
        sql = ("SELECT g.sid, coalesce(s.state,'-'), coalesce(s.current_term::text,''), "
               "coalesce(r.armed::text,''), coalesce(r.applied::text,'') "
               f"FROM unnest({arr}) g(sid) "
               "LEFT JOIN partdist.pg_raft_group_status() s ON s.group_id=g.sid "
               "LEFT JOIN LATERAL (SELECT armed, applied FROM partdist.replay_status() "
               "  WHERE shard=partdist.local_partition_for_shard(g.sid)) r ON true ORDER BY g.sid")
        W = 22
        lines = [bold(pad("  节点", 14) + "".join(pad(f"S{i + 1}={sid}", W) for i, sid in enumerate(S)))]
        counts = {}
        for p in self.cl.workers:
            rows = qlines(p, sql, timeout=20)
            cells = []
            nl = nf = 0
            if rows is None:
                cells = [red("（宕机/连不上）")] + [""] * (len(S) - 1)
            else:
                for r in rows:
                    sid, st, term, armed, applied = r.split("|")
                    if st == "leader":
                        cells.append(green(f"★主 任期{term}"))
                        nl += 1
                    elif st == "follower":
                        tag = "armed" if armed == "true" else ("未armed" if armed else "无回放槽")
                        cells.append(f"从 {tag} 回放到{applied}" if applied else f"从 {tag}")
                        nf += 1
                    elif st == "-":
                        cells.append(dim("—"))
                    else:
                        cells.append(yellow(st))
            counts[p] = (nl, nf, rows is not None)
            lines.append(pad(f"  {self.n(p)} :{p}", 14) + "".join(pad(c, W) for c in cells))
        reg = []
        rt = []
        for sid in S:
            rp = self.cl.registered(sid)
            reg.append(self.n(rp) if rp else "?")
            pp = self.cl.placement(sid)
            rt.append(f":{pp}" if pp else "?")
        lines.append(pad(dim("  控制面登记"), 14) + "".join(pad(x, W) for x in reg))
        lines.append(pad(dim("  Citus 路由"), 14) + "".join(pad(x, W) for x in rt))
        summ = "；".join(f"{self.n(p)} = {c[0]} 主 + {c[1]} 从" if c[2] else f"{self.n(p)} 宕机"
                        for p, c in counts.items())
        lines.append(dim("  每台角色：") + summ)
        out = "\n".join(lines)
        print(out)
        self.ctx["roles"] = counts
        return out

    # —— \cmp：各副本追平后与当前主逐字节比对（主堆 + 主键索引） ——
    def compare(self, sids=None, heal=True):
        if not self.load_topology():
            print(yellow("表还没建"))
            return ""
        sids = sids or self.ctx["S"]
        outl = []
        allok = True
        for sid in sids:
            lp = self.cl.leader(sid)
            if lp is None:
                outl.append(f"  {self.sname(sid)}：找不到当前主")
                allok = False
                continue
            for rp in self.cl.workers:
                if rp == lp or not self.cl.up(rp):
                    continue
                healed = ""
                sl = self.cl.slot(rp, sid)
                pend = q(rp, f"SELECT partdist.shard_baseline_pending(partdist.local_partition_for_shard({sid})::oid)")
                if heal and (sl is None or not sl[0] or pend == "t"):
                    self.cl.set_workers("pg_raft.election_timeout_ms", "30000")
                    rc, o, e = qraw(lp, f"SELECT partdist.provision_shard_replica({sid}::bigint, {self.cl.nid[rp]})")
                    self.cl.set_workers("pg_raft.election_timeout_ms", None)
                    healed = "（先从当前主重供了基线）" if rc == 0 else f"（重供失败：{e.strip()[:80]}）"
                tp = self.cl.flush(lp, sid)
                lo = f"partdist.local_partition_for_shard({sid})"
                recv = None
                for _ in range(30):                      # 先等 Raft 把主的流复制过来
                    v = q(rp, f"SELECT partdist.get_follower_applied_part_lsn({lo})", timeout=15)
                    recv = int(v) if v and v.isdigit() else None
                    if recv is not None and tp is not None and recv >= tp:
                        break
                    time.sleep(1)
                if recv is not None and tp is not None and recv < tp:
                    healed += f"（从只收到 {recv}：主的流尾有还没复制的记录，比如中止事务留下的，下一笔提交时会一并复制）"
                    tp = recv
                a = None
                t_end = time.time() + 60                 # 回放追平，总时长有上限
                while time.time() < t_end:
                    sl = self.cl.slot(rp, sid)
                    a = sl[1] if sl else None
                    if a is not None and tp is not None and a >= tp:
                        break
                    q(rp, f"SELECT partdist.replay_catchup({lo}::regclass, {tp}, 15000)", timeout=30)
                    time.sleep(1)
                res = {}
                for kind in ("heap", "btree"):
                    res[kind] = self.pagecmp(lp, rp, sid, kind)
                good = res["heap"] == "IDENTICAL_OUTSIDE_HOLE" and res["btree"] == "IDENTICAL_OUTSIDE_HOLE"
                allok &= good
                mark = green("一致") if good else red("不一致")
                outl.append(f"  {self.sname(sid)} 主 {self.n(lp)} ↔ 从 {self.n(rp)}：回放 {a}/{tp}  "
                            f"主堆={res['heap']}  主键索引={res['btree']}  {mark}{healed}")
        out = "\n".join(outl) + "\n" + ("  " + green("全部副本与各自的主逐字节一致（掩码外）") if allok
                                         else "  " + red("有副本不一致"))
        print(out)
        self.ctx["cmp_ok"] = allok
        return out

    def pagecmp(self, lp, rp, sid, kind):
        rel = f"{TABLE}_{sid}"
        if kind == "heap":
            sql = f"SELECT pg_relation_filepath('{rel}')"
        else:
            sql = (f"SELECT pg_relation_filepath(indexrelid) FROM pg_index "
                   f"WHERE indrelid='{rel}'::regclass AND indisprimary")
        q(lp, "CHECKPOINT")
        q(rp, "CHECKPOINT")
        time.sleep(0.5)
        a = q(lp, sql)
        b = q(rp, sql)
        if not a or not b:
            return "取不到文件路径"
        r = dexec(["python3", "/tmp/pagecmp.py", f"--kind={kind}",
                   f"{self.cl.ddir[lp]}/{a}", f"{self.cl.ddir[rp]}/{b}"], timeout=120)
        if r is None:
            return "比对超时"
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        return lines[-1] if lines else (r.stderr.strip()[:60] or "无输出")

    # —— \bg：后台读探针 ——
    def bg_start(self):
        if self.bg:
            self.bg.stop()
        probes = []
        for i, sid in enumerate(self.ctx["S"]):
            ks = self.ctx["keys"][sid][:3]
            probes.append((f"S{i + 1}", f"SELECT count(*) FROM {TABLE} WHERE sku IN ({','.join(map(str, ks))})"))
        self.bg = BgReader(probes)

    def bg_cmd(self, sub):
        if sub == "start":
            self.bg_start()
            msg = "后台读探针已启动：每 200 ms 经协调者对 S1/S2/S3 各读一次"
            print(msg)
            return msg
        if not self.bg:
            print(dim("  后台读探针没在跑（\\bg start 启动）"))
            return ""
        if sub == "stop":
            self.bg.stop()
        summ = self.bg.summary()
        lines = []
        for label, d in summ.items():
            s = (f"  {label}：读 {d['n']} 次，成功 {d['ok']}，失败 {d['fail']}（其中副本闸门拒读 {d['refused']}）"
                 f"，服务端单次最长 {d['maxms']} ms（客户端往返最长 {d['maxclient']} ms）")
            if d["fail"]:
                s += f"，不可用窗口约 {d['window']:.1f} s；报错示例：{d['msgs'][0]}"
            lines.append(s)
        out = ("后台读探针结果：\n" if sub == "stop" else "后台读探针（仍在跑）：\n") + "\n".join(lines)
        print(out)
        self.ctx["bg"] = summ
        if sub == "stop":
            self.bg = None
        return out

    # ═══════════════ 步骤驱动 ═══════════════
    def enter_step(self, i):
        self.idx = i
        st = self.steps[i]
        print()
        print(bold(cyan(f"═══════════ 第 {i} 步 / 共 {len(self.steps) - 1} 步：{st.title} ═══════════")))
        text = st.intro(self) if callable(st.intro) else st.intro
        print(text.rstrip())
        if st.action:
            desc = st.action_desc(self) if callable(st.action_desc) else st.action_desc
            if not self.auto:
                a = input(yellow(f"\n▶ 本步先要做：{desc}\n  回车执行（输入 s 跳过）：")).strip().lower()
                if a == "s":
                    print(dim("  已跳过"))
                else:
                    st.action(self)
            else:
                print(yellow(f"\n▶ {desc}"))
                st.action(self)
        st.sugs = st.suggest(self) if st.suggest else []
        self.print_sugs()

    def print_sugs(self):
        st = self.steps[self.idx]
        if not st.sugs:
            return
        print(bold("\n建议的 SQL") + dim("（交互式：回车逐条执行、\\all 全部执行；也可以直接写你自己的 SQL，"
                                       "@会话 前缀换会话，\\help 看全部命令）"))
        nxt = next((k for k, s in enumerate(st.sugs) if not s.done), None)
        for k, s in enumerate(st.sugs):
            ptr = green("▶") if k == nxt else (dim("✓") if s.done else " ")
            first = s.sql.splitlines()[0]
            more = dim(" …") if len(s.sql.splitlines()) > 1 else ""
            print(f" {ptr}{k + 1:>2} " + cyan(f"[{s.sess}]") + f" {first}{more}" + (dim(f"   -- {s.note}") if s.note else ""))

    def leave_step(self):
        st = self.steps[self.idx]
        if st.checks:
            print(bold("\n本步验证："))
            st.checks(self)
        if st.finish:
            st.finish(self)

    # ═══════════════ 全自动 ═══════════════
    def run_auto(self, start=0):
        t0 = time.time()
        try:
            for i in range(start, len(self.steps)):
                self.enter_step(i)
                st = self.steps[i]
                for sg in st.sugs:
                    print()
                    self.run_sug(sg)
                self.leave_step()
        except KeyboardInterrupt:
            print(red("\n中断"))
        finally:
            if self.bg:
                self.bg.stop()
            npass = sum(1 for _, ok in self.results if ok)
            nfail = sum(1 for _, ok in self.results if not ok)
            print()
            print(bold(f"══════ 全自动演示结束：PASS={npass} FAIL={nfail}，用时 {int(time.time() - t0)} s ══════"))
            for d, ok in self.results:
                if not ok:
                    print(red("  FAIL ") + d)
            if not self.keep:
                cleanup(self)
            else:
                print(yellow("  --keep：现场保留（python3 mixed_primary_demo.py --cleanup 清理）"))
            self.drop_sessions()
        return nfail == 0

    # ═══════════════ 交互式 ═══════════════
    def run_interactive(self, start=0):
        print(bold(BANNER))
        self.load_topology()
        self.enter_step(start)
        buf = []
        while True:
            self.poll_async()
            prompt = (cyan(f"[第{self.idx}步] {self.cur}") + "> ") if not buf else " " * (len(self.cur) + 8) + "…> "
            try:
                line = input(prompt)
            except EOFError:
                print()
                break
            except KeyboardInterrupt:
                print(dim("  （已清空当前输入；\\q 退出）"))
                buf = []
                continue
            s = line.strip()
            if not buf:
                if s == "":
                    self.run_next_sug()
                    continue
                if s.startswith("@"):
                    m = re.match(r"@(\S+)\s*(.*)", s)
                    target, rest = m.group(1), m.group(2)
                    if self.sess(target) is None:
                        print(red(f"没有会话 {target}（可用：{', '.join(self.session_names())}）"))
                        continue
                    if not rest:
                        self.cur = target
                        continue
                    if rest.startswith("\\") and self.is_meta(rest):
                        self.interactive_meta(rest, target)
                        continue
                    self.exec_user(target, rest, buf_mode=True)
                    continue
                if s.startswith("\\"):
                    if self.is_meta(s):
                        if self.interactive_meta(s, self.cur) == "quit":
                            break
                        continue
                    self.exec_user(self.cur, s)   # 其它 psql 元命令（\d、\x…）原样交给 psql
                    continue
            buf.append(line)
            if s.endswith(";") or (s.startswith("\\") and buf):
                sql = "\n".join(buf)
                buf = []
                self.exec_user(self.cur, sql)
        if self.bg:
            self.bg.stop()
        a = input(yellow("退出前清理演示现场（删表、拆组、复位 TSO）？[y/N] ")).strip().lower() if TTY else "n"
        if a == "y":
            cleanup(self)
        self.drop_sessions()

    def session_names(self):
        return ["coord"] + [self.n(p) for p in self.cl.workers] + ["A", "B"]

    def exec_user(self, sname, sql, buf_mode=False):
        if buf_mode and not sql.rstrip().endswith(";") and not sql.startswith("\\"):
            sql = sql.rstrip() + ";"
        s = self.sess(sname)
        out, done, note = s.run(sql, timeout=15)   # 自己写的语句：15 s 没返回就先把提示符还给你
        if note:
            print(yellow(note))
        if out.strip():
            print(out.rstrip())
        if not done:
            print(yellow(f"  ⏳ 会话 {sname} 的语句还没返回（在等锁？）。它返回后，你下一次输入时会显示；或 @{sname} \\wait 等它。"))

    def run_next_sug(self):
        st = self.steps[self.idx]
        nxt = next((s for s in st.sugs if not s.done), None)
        if nxt is None:
            print(dim("  本步建议都执行过了。继续写你自己的 SQL，或 \\next 进入下一步（\\check 先验证本步）。"))
            return
        self.run_sug(nxt)
        if not nxt.sql.startswith("\\") or nxt.sql.startswith("\\gbegin"):
            self.cur = nxt.sess        # 你接着写的 SQL 默认落在刚才那条建议的会话上
        rest = [s for s in st.sugs if not s.done]
        if rest:
            print(dim(f"  （下一条：[{rest[0].sess}] {rest[0].sql.splitlines()[0][:90]}）"))
        else:
            print(dim("  本步建议已全部执行；\\check 验证，\\next 下一步。"))

    def interactive_meta(self, s, sname):
        parts = s.split()
        cmd, args = parts[0], parts[1:]
        if cmd in ("\\q", "\\quit"):
            return "quit"
        if cmd in ("\\next", "\\n"):
            self.leave_step()
            if self.idx + 1 < len(self.steps):
                self.enter_step(self.idx + 1)
            else:
                print(green("已经是最后一步。\\q 退出。"))
            return None
        if cmd in ("\\back", "\\b"):
            if self.idx > 0:
                self.enter_step(self.idx - 1)
            return None
        if cmd == "\\step":
            if args and args[0].isdigit() and int(args[0]) < len(self.steps):
                self.enter_step(int(args[0]))
            else:
                print(f"用法：\\step 0..{len(self.steps) - 1}")
            return None
        if cmd == "\\list":
            for k, st in enumerate(self.steps):
                print((green("▶ ") if k == self.idx else "  ") + f"{k:>2}  {st.title}")
            return None
        if cmd == "\\show":
            st = self.steps[self.idx]
            print(st.intro(self) if callable(st.intro) else st.intro)
            self.print_sugs()
            return None
        if cmd == "\\run":
            self.run_next_sug()
            return None
        if cmd == "\\all":
            for sg in self.steps[self.idx].sugs:
                if not sg.done:
                    print()
                    self.run_sug(sg)
            return None
        if cmd == "\\check":
            st = self.steps[self.idx]
            if st.checks:
                st.checks(self)
            else:
                print(dim("  本步没有额外的验证项"))
            return None
        if cmd == "\\use":
            if args and self.sess(args[0]) is not None:
                self.cur = args[0]
            else:
                print(f"用法：\\use {'|'.join(self.session_names())}")
            return None
        if cmd == "\\sessions":
            for nm in self.session_names():
                s = self.sessions.get(nm)
                port = COORD if nm in ("A", "B") else self.cl.port_of_name[nm]
                stt = "未打开" if s is None else ("挂起中" if s.pending else ("已连接" if s.alive() else "已断开"))
                print(f"  {'▶' if nm == self.cur else ' '} {nm:<6} :{port}  {stt}")
            return None
        if cmd == "\\reset":
            s = self.sess(args[0] if args else sname)
            if s:
                s.close()
                print(f"  会话 {s.name} 已关闭（进行中的事务随之回滚），下次使用时重新连接")
            return None
        if cmd in ("\\c", "\\connect"):
            print(yellow("  换节点请用 \\use <会话> 或 @<会话> 前缀（会话：" + ", ".join(self.session_names()) + "）"))
            return None
        if cmd == "\\cleanup":
            if input(yellow("确认清理（删表、拆组、复位 TSO）？[y/N] ")).strip().lower() == "y":
                cleanup(self)
            return None
        if cmd in ("\\help", "\\?"):
            print(HELP)
            return None
        self.run_meta(s, sname)
        return None


# ─────────────────────────── 夹具：建组 / 供副本 / 稳定化 ───────────────────────────

def build_group(demo, sid, want):
    """建组并让 want 当选。调用前全体选举超时已冻结在 30 s（没人会自发竞选），所以只有被点名
    campaign 的节点发起选举；新组日志全空，它必然当选 —— 不用再赌"谁先超时"。"""
    cl = demo.cl
    for p in cl.workers:
        q(p, f"SELECT partdist.pg_raft_group_create({sid}, {cl.allmem})")
    for t in range(40):
        if cl.state(want, sid) == "leader":
            print(f"  组 {demo.sname(sid)}={sid}：主就位于 {demo.n(want)}（{t} s）")
            return True
        if t % 5 == 0:
            q(want, f"SELECT partdist.pg_raft_group_campaign({sid})")
        time.sleep(1)
    print(red(f"  组 {sid} 40 s 内没在 {demo.n(want)} 上选出主（{cl.state(want, sid)}）"))
    return False


def stabilize(demo):
    """供副本后：凡没有 armed 槽位的从，都从**当前主**补供一遍；再等控制面登记收敛"""
    cl = demo.cl
    cl.set_workers("pg_raft.election_timeout_ms", "30000")
    for sid in demo.ctx["S"]:
        cur = cl.leader(sid)
        if cur is None:
            time.sleep(3)
            cur = cl.leader(sid)
        for p in cl.workers:
            if p == cur or cur is None:
                continue
            sl = cl.slot(p, sid)
            if sl and sl[0]:
                continue
            rc, o, e = qraw(cur, f"SELECT partdist.provision_shard_replica({sid}::bigint, {cl.nid[p]})")
            print(f"  [补供] {demo.sname(sid)} → {demo.n(p)}：" + (o.strip()[:90] if rc == 0 else red(e.strip()[:120])))
    cl.set_workers("pg_raft.election_timeout_ms", None)
    for sid in demo.ctx["S"]:
        for _ in range(60):
            lp = cl.leader(sid)
            if lp and cl.registered(sid) == lp and cl.registered(sid, at=lp) == lp:
                break
            time.sleep(1)


def cleanup(demo):
    cl = demo.cl
    print(bold("\n清理演示现场…"))
    if demo.bg:
        demo.bg.stop()
        demo.bg = None
    demo.drop_sessions()
    for p in cl.workers:
        if not cl.up(p):
            print(f"  拉起 {demo.n(p)}")
            cl.node_start(p)
    sids = cl.shards()
    for p in [COORD] + cl.workers:
        for g in qlines(p, "SELECT gid FROM pg_prepared_xacts") or []:
            q(p, f"ROLLBACK PREPARED '{g}'")
    for sid in sids:
        for p in cl.workers:
            q(p, f"SELECT partdist.replay_disable('{TABLE}_{sid}'::regclass)")
    q(COORD, f"SET statement_timeout='60s'; DROP TABLE IF EXISTS {TABLE}", timeout=90)
    for p in cl.workers:
        for sid in sids:
            q(p, f"SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS {TABLE}_{sid}")
    for _ in range(2):
        for p in cl.workers:
            q(p, "SELECT count(partdist.pg_raft_group_drop(group_id)) FROM partdist.pg_raft_group_status() WHERE group_id<>0")
        time.sleep(2)
        for p in cl.workers:
            for sid in sids:
                q(p, f"SET statement_timeout='30s'; SET citus.enable_ddl_propagation=off; DROP TABLE IF EXISTS {TABLE}_{sid}")
    for p in [COORD] + cl.workers:
        for g in ("pg_partdist.tso_conninfo", "pg_partdist.tso_lease_ms", "pg_raft.election_timeout_ms"):
            q(p, f"ALTER SYSTEM RESET {g}")
        q(p, "SELECT pg_reload_conf()")
    q(COORD, "ALTER SYSTEM RESET pg_partdist.tso_master")
    q(COORD, "SELECT pg_reload_conf()")
    ng = sum(int(q(p, "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0") or 0)
             for p in cl.workers)
    left = q(COORD, f"SELECT count(*) FROM pg_dist_partition WHERE logicalrelid::text='{TABLE}'")
    print(f"  完成：残留数据组 {ng} 个，逻辑表残留 {left}；TSO 已复位")
    return ng == 0 and left == "0"


# ─────────────────────────── 各步骤 ───────────────────────────

def build_steps(demo):
    steps = []
    cl = demo.cl
    W = cl.workers

    # ── 第 0 步 ──
    def s0_intro(d):
        rows = "\n".join(f"    {d.n(p):<6} :{p}   pg_raft 节点号 {d.cl.nid[p]}" for p in [COORD] + W)
        return f"""
本演示在 {CONTAINER}（1 协调者 + {len(W)} worker）上建一张 3 分片的表 {TABLE}，每个分片一个 Raft 组、
成员 = 全部 {len(W)} 台 worker；三个组的主分别落在三台 worker 上。于是：

    每台 worker = 一个分片的「主」 + 另外两个分片的「从」   ← 同一节点混合主从

节点与编号（partition_map.primary_node、provision_shard_replica 用的都是 **pg_raft 节点号**）：
{rows}

你手上有这些会话（常驻 psql，事务可以跨多次输入）：
    coord         协调者（走 Citus 路由，写 {TABLE} 就行，不用管分片落在哪）
    w1 w2 w3      直连各 worker（分片表 {TABLE}_<分片号> 对 SQL 可见）
    A  B          两个独立的协调者会话（第 8 步演示两个并发事务）
"""

    def s0_action(d):
        if d.cl.shards():
            print(yellow(f"  发现上一次演示留下的 {TABLE}，先清理"))
            cleanup(d)

    def s0_sug(d):
        return [
            Sug("coord", "SELECT nodeid, nodeport, groupid, nodeport-5431 AS raft节点号 FROM pg_dist_node ORDER BY nodeid;",
                "三套编号：端口 / Citus groupid / pg_raft 节点号", X_fn(f"≥ {len(W)} 行", lambda o, c: nrows(o) >= len(W))),
            Sug("coord", "SELECT group_id, state, leader_node_id, current_term, commit_index FROM partdist.pg_raft_group_status();",
                "0 号组 = 控制面（登记每个分片的主是谁）", X_has("0 |", "能看到 0 号控制面组")),
            Sug("w1", "SHOW pg_raft.node_id;", "w1 的 pg_raft 节点号", X_val(lambda v, c: v == str(cl.nid[W[0]]), f"= {cl.nid[W[0]]}")),
        ]

    def s0_checks(d):
        d.check(f"有 {len(W)} 个 worker（≥3）", len(W) >= 3, True)
        ng = sum(int(q(p, "SELECT count(*) FROM partdist.pg_raft_group_status() WHERE group_id<>0") or 0) for p in W)
        d.check("净场：没有残留数据组", ng, 0)

    steps.append(Step("前置：集群、节点编号、控制面", s0_intro, s0_action,
                      "检查有没有上次演示的残留（有就清理）", s0_sug, s0_checks))

    # ── 第 1 步：TSO ──
    s1_intro = """
TSO（全局时间戳服务）是快照隔离的心脏：每个事务的 start_ts / commit_ts 都从它取号，可见性规则只有
一条 —— 行版本的 commit_ts < 我的 start_ts 才可见。TSO 跑在协调者上（内存计数器）：

  · boot 防呆：计数器在内存里，重启后拒绝发号，须先删 pg_tso_boot 标记再重启协调者；
  · 协调者开 tso_master；每个节点把 tso_conninfo 指向协调者（worker 经 libpq 取号）。
"""

    def s1_action(d):
        cdir = d.cl.ddir[COORD]
        d.drop_sessions(COORD)
        dexec(["rm", "-f", f"{cdir}/pg_tso_boot"])
        dexec(["bash", "-c", f"{PG_CTL} -D '{cdir}' -m fast -l '{cdir}/pg.log' restart -w -t 60 >/dev/null 2>&1"], timeout=90)
        for _ in range(40):
            if d.cl.up(COORD):
                break
            time.sleep(1)
        print("  协调者已重启")
        q(COORD, "ALTER SYSTEM SET pg_partdist.tso_master = on")
        for p in [COORD] + W:
            q(p, "ALTER SYSTEM SET pg_partdist.tso_conninfo = 'host=/tmp port=5432 dbname=postgres user=postgres'")
            q(p, "ALTER SYSTEM SET pg_partdist.tso_lease_ms = 60000")
            q(p, "SELECT pg_reload_conf()")
        time.sleep(2)
        for p in W:
            q(p, "SELECT partdist.partdist_tso_client_start_ts()")
        time.sleep(4)
        print("  tso_master=on；各节点 tso_conninfo 已指向协调者")

    def s1_sug(d):
        return [
            Sug("coord", "SHOW pg_partdist.tso_master;", "应为 on", X_val(lambda v, c: v == "on", "on")),
            Sug("coord", "SELECT partdist.partdist_tso_client_start_ts() AS 取一个号;", "", X_val(lambda v, c: v.isdigit(), "拿到一个数"), capture="ts1"),
            Sug("w1", "SELECT partdist.partdist_tso_client_start_ts() AS w1取号;", "worker 经 libpq 找协调者取号",
                X_val(lambda v, c: v.isdigit() and int(v) > int(c.get("ts1") or 0), "比刚才协调者那个号大（单调递增）")),
            Sug("coord", "SELECT partdist.partdist_tso_status();", "TSO 计数器状态", X_ok()),
        ]

    steps.append(Step("TSO：全局时间戳服务", s1_intro, s1_action,
                      "删 pg_tso_boot → 重启协调者 → 开 tso_master → 各节点指向 TSO", s1_sug))

    # ── 第 2 步：建表 ──
    s2_intro = f"""
建一张 3 分片的表。Citus 会把 3 个分片的 placement 轮流放到 3 台 worker 上 —— placement 所在节点
就是该分片**最初的主**（数据在那儿）。worker 上分片的真名是 {TABLE}_<分片号>。
"""

    def s2_sug(d):
        return [
            Sug("coord", f"""SET citus.shard_count = 3;
SET citus.shard_replication_factor = 1;
CREATE TABLE {TABLE} (sku int PRIMARY KEY, item text, qty int NOT NULL DEFAULT 0);
SELECT create_distributed_table('{TABLE}', 'sku', colocate_with => 'none');
ALTER TABLE {TABLE} SET (autovacuum_enabled = off);""", "建表（先建好索引：打标之后禁 CREATE INDEX）", X_ok()),
            Sug("coord", f"""SELECT s.shardid AS 分片号, n.nodeport AS placement端口, n.nodeport-5431 AS raft节点号,
       '{TABLE}_'||s.shardid AS worker上的表名
  FROM pg_dist_shard s JOIN pg_dist_placement p USING (shardid)
  JOIN pg_dist_node n ON n.groupid = p.groupid AND n.noderole = 'primary'
 WHERE s.logicalrelid = '{TABLE}'::regclass ORDER BY 1;""", "三个分片各落一台 worker", X_rows(3)),
            Sug("coord", f"SELECT sku, get_shard_id_for_distribution_column('{TABLE}', sku) AS 落在哪个分片 FROM generate_series(101,109) sku;",
                "每个 sku 落在哪个分片，Citus 按哈希算", X_rows(9)),
        ]

    def s2_finish(d):
        if not d.cl.shards():
            if d.auto or input(yellow("  表还没建，自动建上？[Y/n] ")).strip().lower() != "n":
                for sg in s2_sug(d)[:1]:
                    d.run_sug(sg, show=False)
        if not d.load_topology():
            print(red("  表没建成，后面的步骤跑不了"))
            return
        for p in W:
            q(p, "SELECT partdist.rebuild_shard_identity()")
            q(p, "SET citus.enable_ddl_propagation TO off; CREATE EXTENSION IF NOT EXISTS pageinspect")
        S = d.ctx["S"]
        # 每个分片留一批 sku，后面各步都用它们
        d.ctx["keys"] = {sid: d.cl.keys_in(sid, 101, 100 + NROWS, NROWS) for sid in S}
        d.ctx["spare"] = {sid: d.cl.keys_in(sid, 301, 600, 20) for sid in S}
        print("  " + "；".join(f"S{i + 1}={sid} placement 在 {d.n(d.ctx['P0'][sid])}" for i, sid in enumerate(S)))

    def s2_checks(d):
        ok = d.load_topology()
        d.check("表有 3 个分片", ok, True)
        if ok:
            d.check("3 个分片的 placement 分在 3 台不同 worker", len(set(d.ctx["P0"].values())), 3)

    steps.append(Step("建一张 3 分片的表，看分片落在哪", s2_intro, None, None, s2_sug, s2_checks, s2_finish))

    # ── 第 3 步：每分片一个 Raft 组 + 供副本 ⇒ 混合主从 ──
    def s3_intro(d):
        S, P0 = d.ctx["S"], d.ctx["P0"]
        plan = "\n".join(f"    S{i + 1}={sid}：主 {d.n(P0[sid])}，从 " +
                         "、".join(d.n(p) for p in W if p != P0[sid]) for i, sid in enumerate(S))
        return f"""
每个分片建一个 Raft 组，成员 = 全部 3 台 worker，组主落在该分片的 placement 节点上：
{plan}

然后在**主**上调一条命令把副本供到另外两台：
    SELECT partdist.provision_shard_replica(<分片号>, <目标 pg_raft 节点号>);
它做的事：到目标节点建壳表 → 发一份物理基线进分区流（走 Raft）→ 目标按基线配对文件号、arm 回放槽位。

供完之后，**同一台 worker 上同时有：自己当主的那个分片（可读可写）+ 两个副本壳表（只收流、惰性回放）**。
{dim("（演示环境 2 vCPU：本步把全体选举超时冻结在 30 s，再在 placement 节点上 pg_raft_group_campaign 点名当选，")}
{dim(" 供副本期间也保持冻结，防止 CPU 饱和时组主漂移；本步结束复位。这只是夹具手法，不影响被演示的机制。）")}
"""

    def s3_action(d):
        d.cl.set_workers("pg_raft.election_timeout_ms", "30000")
        for sid in d.ctx["S"]:
            build_group(d, sid, d.ctx["P0"][sid])
        for sid in d.ctx["S"]:
            lp = d.cl.leader(sid)
            if lp != d.ctx["P0"][sid]:
                print(yellow(f"  注意：{d.sname(sid)} 的主在 {d.n(lp) if lp else '（无）'}，不在 placement 上"))
        print("  三个组已建好；选举超时暂时冻结在 30 s（本步结束时复位）")

    def s3_sug(d):
        out = []
        for sid in d.ctx["S"]:
            lp = d.cl.leader(sid) or d.ctx["P0"][sid]
            for p in W:
                if p == lp:
                    continue
                out.append(Sug(d.n(lp), f"SELECT partdist.provision_shard_replica({sid}, {d.cl.nid[p]});",
                               f"{d.sname(sid)} 的副本供到 {d.n(p)}", X_has("shard=", "返回 shard=… base=…（基线游标）"), timeout=180))
        w1 = W[0]
        out += [
            Sug("coord", "\\roles", "★ 同一节点混合主从一览", X_has("主", "打印出角色矩阵")),
            Sug("w1", "SELECT group_id AS 分片组, state AS w1在组里的角色, leader_node_id AS 组主, current_term AS 任期\n"
                      "  FROM partdist.pg_raft_group_status() WHERE group_id <> 0 ORDER BY 1;",
                "w1 在三个组里：一个 leader、两个 follower",
                X_fn("1 个 leader + 2 个 follower", lambda o, c: [r[1] for r in table_rows(o)].count("leader") == 1 and [r[1] for r in table_rows(o)].count("follower") == 2)),
            Sug("w1", "SELECT shard AS 本地OID, armed, state, applied AS 已回放到 FROM partdist.replay_status() ORDER BY 1;",
                "w1 上的回放槽位 = 它当从的两个分片", X_fn("至少 2 个 armed 槽位", lambda o, c: sum(1 for r in table_rows(o) if len(r) > 1 and r[1] == "t") >= 2)),
            Sug("coord", "SELECT partition_id AS 分片号, primary_node AS 主, secondary_nodes AS 从, primary_term AS 任期\n"
                         f"  FROM partdist.partition_map WHERE partition_id IN ({','.join(map(str, d.ctx['S']))}) ORDER BY 1;",
                "控制面登记（group 0 里的 partition_map）", X_rows(3)),
        ]
        return out

    def s3_finish(d):
        stabilize(d)
        print("  选举超时已复位")

    def s3_checks(d):
        S = d.ctx["S"]
        for p in W:
            roles = [d.cl.state(p, sid) for sid in S]
            d.check(f"{d.n(p)}：1 个分片当主 + 2 个分片当从", f"{roles.count('leader')}+{roles.count('follower')}", "1+2")
        for sid in S:
            lp = d.cl.leader(sid)
            for p in W:
                if p != lp:
                    sl = d.cl.slot(p, sid)
                    d.check(f"{d.sname(sid)} 的从 {d.n(p)} 回放槽位 armed", bool(sl and sl[0]), True)
            d.check(f"{d.sname(sid)} 控制面登记的主 = 组主", d.cl.registered(sid), lp)

    steps.append(Step("每分片一个 Raft 组 + 供副本 ⇒ 同一节点混合主从", s3_intro, s3_action,
                      lambda d: "冻结选举超时 → 建 3 个 Raft 组 → 在各自 placement 上 campaign 点名当选", s3_sug, s3_checks, s3_finish))

    # ── 第 4 步：打标 ──
    s4_intro = f"""
这一步是开关：不打标，它就是三张普通的 Citus 分片表，底下的分片 xid / 分片 clog / TSO 一行都不跑。
协调者上一条命令打标整张表（逐 placement 下发）：

    SELECT * FROM partdist.set_table_shard_mvcc('{TABLE}');

副本不用各自登记 —— 身份随控制记录（CTRL SHARD_MVCC）经 Raft 流到副本，升主时继承。
"""

    def s4_sug(d):
        S = d.ctx["S"]
        w1 = W[0]
        mine = next((sid for sid in S if d.cl.leader(sid) == w1), S[0])
        return [
            Sug("coord", f"SELECT * FROM partdist.set_table_shard_mvcc('{TABLE}');", "3 个 placement 都应 registered",
                X_fn("3 行都是 registered/already", lambda o, c: len([r for r in table_rows(o) if len(r) > 2 and re.match(r"^(registered|already)", r[2])]) == 3)),
            Sug("w1", f"SELECT partdist.shard_mvcc_status(partdist.local_partition_for_shard({mine})) AS w1当主的{d.sname(mine)};",
                "主：registered=yes", X_has("registered=yes")),
        ]

    def s4_finish(d):
        st = [q(d.cl.leader(sid), f"SELECT partdist.shard_mvcc_status(partdist.local_partition_for_shard({sid}))") or ""
              for sid in d.ctx["S"]]
        if not all("registered=yes" in x for x in st):
            if d.auto or input(yellow("  还有分片没打标，自动打上？[Y/n] ")).strip().lower() != "n":
                q(COORD, f"SELECT count(*) FROM partdist.set_table_shard_mvcc('{TABLE}')")

    def s4_checks(d):
        for sid in d.ctx["S"]:
            lp = d.cl.leader(sid)
            st = q(lp, f"SELECT partdist.shard_mvcc_status(partdist.local_partition_for_shard({sid}))") or ""
            d.check(f"{d.sname(sid)} 在主 {d.n(lp)} 上已打标", "registered=yes" in st, True)

    steps.append(Step("接入事务系统：打标", s4_intro, None, None, s4_sug, s4_checks, s4_finish))

    # ── 第 5 步：写数据 + 两个 xid 宇宙 ──
    def s5_intro(d):
        return f"""
经协调者写 {NROWS} 行（Citus 自动路由到各分片的主）。先看一条规矩：

  · 只落**一个**分片的写走 1PC，直接写就行；
  · 跨分片的写要走 2PC，**必须加入全局事务**（带上 gxid 与 TSO 的 start_ts）——没加入的，PREPARE 一律被拒。
    交互式里 \\gbegin 一条命令帮你开好（等价的 SQL 会打印出来）。

然后到 worker 上看一眼元组身上的 xmin：

  · worker 自己的原生事务号（txid_current）早就烧到几十万；
  · 但打标分片的元组上写的是 3、4、5… —— **每个分片有一个只属于自己的 xid 宇宙**，
    号在分片上不在节点上：分片换主、节点重装、原生 xid 回卷都动不了它们。
"""

    def s5_sug(d):
        S = d.ctx["S"]
        allv = ",".join(f"({k},'item-{k}',{k * 10})" for k in range(101, 101 + NROWS))
        rest = ",".join(f"({k},'item-{k}',{k * 10})" for k in range(102, 101 + NROWS))
        out = [
            Sug("coord", f"INSERT INTO {TABLE} VALUES {allv};",
                "一条语句写 24 行 = 跨 3 个分片 ⇒ 要走 2PC；没加入全局事务，PREPARE 被拒",
                X_err("未加入全局事务", "报错「未加入全局事务的分片写不允许 PREPARE TRANSACTION」")),
            Sug("coord", f"INSERT INTO {TABLE} VALUES (101, 'item-101', 1010);",
                "只落一个分片的写走 1PC，不用加入全局事务", X_ok()),
            Sug("coord", "\\gbegin", "开全局事务：取 gxid + start_ts，SET LOCAL join_info", X_has("join_info")),
            Sug("coord", f"INSERT INTO {TABLE} VALUES {rest};\nCOMMIT;", "剩下 23 行跨 3 个分片，这次能提交", X_ok()),
            Sug("coord", f"SELECT * FROM {TABLE} ORDER BY sku;", "经协调者读回", X_rows(NROWS)),
        ]
        for p in W:
            mine = next((sid for sid in S if d.cl.leader(sid) == p), None)
            if mine is None:
                continue
            out += [
                Sug(d.n(p), f"SELECT txid_current() AS {d.n(p)}的原生xid, partdist.shard_xid_next(partdist.local_partition_for_shard({mine})::oid) AS {d.sname(mine)}的下一个分片xid;",
                    f"{d.n(p)} 是 {d.sname(mine)} 的主", X_fn("原生 xid 远大于分片 xid", lambda o, c: int(table_rows(o)[0][0]) > 100 * int(table_rows(o)[0][1]))),
                Sug(d.n(p), f"SELECT sku, xmin AS 分片xid FROM {TABLE}_{mine} ORDER BY sku;",
                    f"{d.n(p)} 直接读自己当主的分片", X_fn("xmin 都是小号（分片 xid 宇宙）", lambda o, c: bool(table_rows(o)) and all(int(r[1]) < 1000 for r in table_rows(o)))),
            ]
        return out

    def s5_finish(d):
        n = q(COORD, f"SELECT count(*) FROM {TABLE}")
        if n != str(NROWS):
            if d.auto or input(yellow(f"  表里现在 {n} 行，自动补齐到 {NROWS} 行？[Y/n] ")).strip().lower() != "n":
                vals = ",".join(f"({k},'item-{k}',{k * 10})" for k in range(101, 101 + NROWS))
                s = d.sess("coord")
                s.run("ROLLBACK;", timeout=10)
                d.gbegin("coord", d.S(0))
                s.run(f"INSERT INTO {TABLE} VALUES {vals} ON CONFLICT (sku) DO NOTHING;\nCOMMIT;")

    def s5_checks(d):
        d.check(f"协调者读到 {NROWS} 行", q(COORD, f"SELECT count(*) FROM {TABLE}"), str(NROWS))

    steps.append(Step("写数据 + 看见两个 xid 宇宙", s5_intro, None, None, s5_sug, s5_checks, s5_finish))

    # ── 第 6 步：复制 + 惰性回放 + 逐字节一致 ──
    def s7_intro(d):
        S = d.ctx["S"]
        s1 = S[0]
        lp = d.cl.leader(s1)
        rp = next(p for p in W if p != lp)
        tip = d.cl.flush(lp, s1)
        d.ctx["s7"] = (s1, lp, rp, tip)
        return f"""
主的每一笔写先进它自己的分区流（pg_parwal），经该分片的 Raft 组复制到两个从（多数派 ack）；
从节点**只落字节不 redo**（惰性回放：armed ≠ 正在回放），有人触发才追平。追平后页面与主逐字节一致
（按内核 heap_mask 口径，掩码外），分片 clog（判决 + commit_ts）也经流里的提交标记同步过去。

以 {d.sname(s1)} 为例：主 {d.n(lp)}，看从 {d.n(rp)}。主的分区流位点此刻 = {tip}。
"""

    def s7_sug(d):
        s1, lp, rp, tip = d.ctx["s7"]
        lo = f"partdist.local_partition_for_shard({s1})"
        x = q(lp, f"SELECT min(xmin::text::bigint) FROM {TABLE}_{s1}") or "3"
        return [
            Sug(d.n(lp), f"SELECT partdist.get_partition_flush_lsn({lo}) AS 主的分区流位点;", "", X_val(lambda v, c: v.isdigit(), "一个位点")),
            Sug(d.n(rp), f"SELECT partdist.get_follower_applied_part_lsn({lo}) AS 从经Raft收到的位点;",
                "= 主的位点 ⇒ 多数派复制完成", X_val(lambda v, c, t=tip: int(v) >= t, f"≥ {tip}"), retry=30),
            Sug(d.n(rp), f"SELECT armed, state, applied AS 已回放到 FROM partdist.replay_status() WHERE shard = {lo};",
                "惰性：收到了不等于回放了", X_col(0, "t", "armed = t")),
            Sug(d.n(rp), f"SELECT partdist.replay_catchup({lo}::regclass, {tip}, 60000) AS 追平到;", "触发一次追平",
                X_val(lambda v, c, t=tip: int(v) >= t, f"≥ {tip}")),
            Sug(d.n(lp), f"SELECT partdist.shard_clog_status_full({lo}::oid, {x}) AS 主的账本;", f"分片 xid {x} 的判决",
                X_has("st=2", "st=2（已提交）"), capture="clog_p"),
            Sug(d.n(rp), f"SELECT partdist.shard_clog_status_full({lo}::oid, {x}) AS 从的账本;",
                "判决与 commit_ts 同主（从上 sts=0：提交标记只带 commit_ts，可见性只看它）",
                X_fn("st 与 cts 同主", lambda o, c: re.search(r"st=\d+", scalar(o)).group() == re.search(r"st=\d+", c["clog_p"]).group()
                     and re.search(r"cts=\d+", scalar(o)).group() == re.search(r"cts=\d+", c["clog_p"]).group())),
            Sug("coord", "\\cmp", "★ 6 个副本各自追平并与当前主逐字节比对", X_has("全部副本与各自的主逐字节一致", "全部 IDENTICAL_OUTSIDE_HOLE")),
        ]

    steps.append(Step("Raft 复制 + 惰性回放 + 逐字节一致", s7_intro, None, None, s7_sug))

    # ── 第 7 步：同节点读闸门 ──
    def s6_intro(d):
        w1 = W[0]
        S = d.ctx["S"]
        mine = next((sid for sid in S if d.cl.leader(sid) == w1), S[0])
        rep = next(sid for sid in S if sid != mine)
        d.ctx["s6"] = (mine, rep)
        return f"""
在 **同一台** w1 上：{TABLE}_{mine}（{d.sname(mine)}，w1 是主）可以直接读；
{TABLE}_{rep}（{d.sname(rep)}，w1 只是从）是副本壳表 —— 内容由别人的分区流逐字节回放而来、元组带的是
**外来的分片 xid**，本地读会触发按原生 clog 的剪枝（就地损毁副本），所以被读闸门拒绝。
经协调者读则永远路由到各分片当前的主。

闸门的判据是「这个副本上有没有分片 xid 水位」—— 回放过打标数据才会有（上一步的追平已经做过）。
一个还没回放过任何东西的空壳里没有外来 xid 的元组，读它不损毁什么，所以闸门不拦（读出来是空表）。
route_status 里的 xid_watermark 就是这个水位；role=replica_or_plain 表示"不是经切主接管来的"
（最初的 placement 主也显示它，切主上来的新主显示 role=promoted）。
"""

    def s6_sug(d):
        mine, rep = d.ctx["s6"]
        return [
            Sug("w1", f"SELECT count(*) AS w1当主的分片 FROM {TABLE}_{mine};", f"{d.sname(mine)}：w1 是主，可读", X_val(lambda v, c: v.isdigit(), "读到行数")),
            Sug("w1", f"SELECT count(*) FROM {TABLE}_{rep};", f"{d.sname(rep)}：w1 是从 → 读闸门拒绝", X_err("副本壳表")),
            Sug("w1", f"SELECT partdist.route_status(partdist.local_partition_for_shard({mine})) AS 主分片,\n"
                      f"       partdist.route_status(partdist.local_partition_for_shard({rep})) AS 副本;",
                "主：captured=yes（写入被捕获进分区流）；副本：captured=no、带着回放学到的水位",
                X_fn("主 captured=yes、副本 captured=no", lambda o, c: "captured=yes" in table_rows(o)[0][0] and "captured=no" in table_rows(o)[0][1])),
            Sug("coord", f"SELECT count(*) FROM {TABLE};", "经协调者：自动落到各分片的主", X_val(lambda v, c: v == str(NROWS), str(NROWS))),
        ]

    steps.append(Step("同一节点：自己的主分片可读，别人的副本壳表不可读", s6_intro, None, None, s6_sug))

    # ── 第 8 步：两个并发事务 ──
    def s8_intro(d):
        s1 = d.S(0)
        k = d.ctx["keys"][s1]
        knew = d.ctx["spare"][s1][0]
        d.ctx["s8"] = (s1, k[:3], knew)
        return f"""
会话 A、B 是两个独立的协调者连接（交互式里用 @A / @B 前缀，或 \\use A）。数据取 {d.sname(s1)} 上的
sku {k[0]}、{k[1]}、{k[2]}（它们落在同一个分片，当前主 {d.n(d.cl.leader(s1))}）。**严格按顺序交替执行**：

  幕1 两边各开事务、各看一眼      幕2 A 改一行、增一行（不提交）   幕3 B 看不见
  幕4 ★ A 提交后 B 仍看不见（SI）  幕5 B 删一行并提交               幕6 新快照看见全部
  幕7 页面上读出整个故事          幕8 查判决账本                   幕9 写写冲突（first-committer-wins）
"""

    def s8_sug(d):
        s1, (k1, k2, k3), knew = d.ctx["s8"]
        lp = d.cl.leader(s1)
        ks = f"{k1},{k2},{k3},{knew}"
        sel = f"SELECT sku, item, qty FROM {TABLE} WHERE sku IN ({ks}) ORDER BY sku;"
        q1 = int(q(COORD, f"SELECT qty FROM {TABLE} WHERE sku={k1}") or 0)
        d.ctx["q1"] = q1
        lo = f"partdist.local_partition_for_shard({s1})"
        return [
            Sug("A", "BEGIN;\n" + sel, "幕1 A 开事务", X_rows(3)),
            Sug("B", "BEGIN;\n" + sel, "幕1 B 开事务", X_rows(3)),
            Sug("A", f"UPDATE {TABLE} SET qty = qty - 10 WHERE sku = {k1};\nINSERT INTO {TABLE} VALUES ({knew}, '卷尺', 15);\n" + sel,
                "幕2 A 改+增，自己看得见（自见性）", X_fn("A 看到 4 行、改后的值", lambda o, c: nrows(o) == 4 and row_of(o, k1)[2] == str(q1 - 10))),
            Sug("B", sel, "幕3 B 看不见 A 未提交的改动", X_fn("仍 3 行、原值", lambda o, c: nrows(o) == 3 and row_of(o, k1)[2] == str(q1))),
            Sug("A", "COMMIT;", "幕4 A 提交", X_ok()),
            Sug("B", sel, "幕4 ★ A 已提交，B 在自己的事务里仍看不见（快照在 BEGIN 后第一条语句就定了）",
                X_fn("仍 3 行、原值", lambda o, c: nrows(o) == 3 and row_of(o, k1)[2] == str(q1))),
            Sug("B", f"DELETE FROM {TABLE} WHERE sku = {k2};\nCOMMIT;", "幕5 B 删一行并提交", X_ok()),
            Sug("A", sel, "幕6 新快照：A 的改+增、B 的删全部生效",
                X_fn("3 行：改后值 + 新行，删掉的那行没了", lambda o, c: nrows(o) == 3 and row_of(o, k1)[2] == str(q1 - 10)
                     and row_of(o, knew) is not None and row_of(o, k2) is None)),
            Sug(d.n(lp), f"SELECT lp AS 行指针, t_xmin::text::bigint AS xmin, t_xmax::text::bigint AS xmax\n"
                         f"  FROM heap_page_items(get_raw_page('{TABLE}_{s1}', 0)) WHERE lp_len > 0;",
                "幕7 页面上：旧版本被作废（xmax）、A 的改和增共用一个分片 xid", X_ok()),
            Sug(d.n(lp), f"SELECT x AS 分片xid, partdist.shard_clog_status_full({lo}::oid, x) AS 判决\n"
                         f"  FROM generate_series(3::bigint, partdist.shard_xid_next({lo}::oid) - 1) x;",
                "幕8 st=2 已提交；sts=start_ts、cts=commit_ts —— B 的 sts < A 的 cts ⇒ 幕4 看不见", X_has("st=2")),
            Sug("A", f"BEGIN;\nUPDATE {TABLE} SET qty = qty + 5 WHERE sku = {k1};", f"幕9 A 改 {k1}（不提交）", X_ok()),
            Sug("B", f"BEGIN;\nUPDATE {TABLE} SET qty = qty + 7 WHERE sku = {k1};", "幕9 B 改同一行 → 被行锁挡住", pending=True),
            Sug("A", "COMMIT;", "幕9 A 提交", X_ok()),
            Sug("B", "\\wait", "幕9 B 的等待结束：first-committer-wins，B 被中止",
                X_err("could not serialize access", "报 could not serialize access due to concurrent update")),
            Sug("B", "ROLLBACK;", "", None),
        ]

    def s8_finish(d):
        for nm in ("A", "B"):
            s = d.sessions.get(nm)
            if s and s.alive():
                s.run("ROLLBACK;", timeout=10)

    steps.append(Step("两个并发事务：快照隔离与写写冲突（会话 A / B）", s8_intro, None, None, s8_sug, None, s8_finish))

    # ── 第 9 步：受控切主 ──
    def s9_intro(d):
        S = d.ctx["S"]
        s1 = S[0]
        old = d.cl.leader(s1)
        tgt = next((d.cl.leader(sid) for sid in S[1:] if d.cl.leader(sid) not in (None, old)), None) \
            or next(p for p in W if p != old)
        d.ctx["s9"] = (s1, old, tgt)
        return f"""
把 {d.sname(s1)} 的主从 {d.n(old)} 挪到 {d.n(tgt)} —— 而 {d.n(tgt)} 本来就是另一个分片的主。切完之后
{d.n(tgt)} 同时当两个分片的主，{d.n(old)} 只剩从的身份：**同一节点上的角色组合在线变化**。

受控切主 = 在目标节点上对**这一个组**发起选举：
    SELECT partdist.pg_raft_group_campaign(<分片号>);
当选者先追平已落盘的记录、认领无主 xid，再上报控制面；各节点 apply 登记后 Citus 路由随之切换。
要看的三件事：
  ① 切主全程后台每 200 ms 经协调者读一次 —— **一次都不应失败**（新主登记生效的那一瞬也不拒读）；
  ② 新主的「下一个分片 xid」与旧主被换下时一致 —— 号在分片上，不断档、不重发；
  ③ 旧主被自动重新供给成新主的副本（约 5–30 s），不用手工善后。
"""

    def s9_action(d):
        d.bg_start()
        print("  后台读探针已启动（每 200 ms 经协调者对 S1/S2/S3 各读一次）")

    def s9_sug(d):
        s1, old, tgt = d.ctx["s9"]
        lo = f"partdist.local_partition_for_shard({s1})"
        knew = d.ctx["spare"][s1][1]
        d.ctx["s9_knew"] = knew
        return [
            Sug(d.n(old), f"SELECT partdist.shard_xid_next({lo}::oid) AS 旧主下一个分片xid;", "切之前记下", X_val(lambda v, c: v.isdigit(), "一个号"), capture="xid_old"),
            Sug(d.n(tgt), f"SELECT partdist.pg_raft_group_campaign({s1});", f"★ 让 {d.n(tgt)} 对 {d.sname(s1)} 的组发起选举", X_col(0, "t", "t")),
            Sug("coord", f"SELECT partition_id AS 分片号, primary_node AS 主, secondary_nodes AS 从, primary_term AS 任期\n"
                         f"  FROM partdist.partition_map WHERE partition_id = {s1};",
                f"控制面登记翻到 {d.n(tgt)}（节点号 {d.cl.nid[tgt]}）",
                X_col(1, str(d.cl.nid[tgt]), "主 = 新节点"), retry=90),
            Sug("coord", f"SELECT shardid, nodeport FROM pg_dist_shard_placement WHERE shardid = {s1};", "Citus 路由已指向新主",
                X_has(str(tgt), f"nodeport = {tgt}"), retry=30),
            Sug(d.n(tgt), f"SELECT partdist.shard_xid_next({lo}::oid) AS 新主下一个分片xid;", "与旧主换下时一致",
                X_val(lambda v, c: int(v) == int(c.get("xid_old") or -1), "= 旧主的下一个号"), capture="xid_new"),
            Sug("coord", f"INSERT INTO {TABLE} VALUES ({knew}, 'after-switch', 1);", "经协调者写一行（落到新主）", X_ok()),
            Sug(d.n(tgt), f"SELECT sku, xmin AS 分片xid FROM {TABLE}_{s1} WHERE sku = {knew};", "新主写的第一行拿到的正是那个号",
                X_fn("xmin = 旧主换下时的下一个号", lambda o, c: table_rows(o)[0][1] == c.get("xid_old"))),
            Sug("coord", "\\bg stop", "① 切主期间的读：0 次失败", X_fn("S1/S2/S3 都 0 次失败", lambda o, c: all(v["fail"] == 0 for v in c["bg"].values()))),
            Sug(d.n(old), f"SELECT armed, state, applied FROM partdist.replay_status() WHERE shard = {lo};",
                f"③ 旧主 {d.n(old)} 被自动重新供给成副本（armed = t）", X_col(0, "t", "armed = t"), retry=180),
            Sug("coord", "\\roles", f"{d.n(tgt)} 现在 2 主 + 1 从，{d.n(old)} 0 主 + 3 从", X_has("主")),
            Sug("coord", f"\\cmp S1", f"新主与两个从逐字节一致（含旧主）", X_has("全部副本与各自的主逐字节一致", "全部一致")),
        ]

    def s9_finish(d):
        if d.bg:
            d.bg.stop()
            d.bg = None

    def s9_checks(d):
        s1, old, tgt = d.ctx["s9"]
        d.check(f"{d.sname(s1)} 当前主 = {d.n(tgt)}", d.cl.leader(s1), tgt)
        d.check(f"{d.n(tgt)} 同时当 2 个分片的主", sum(1 for sid in d.ctx["S"] if d.cl.leader(sid) == tgt), 2)
        d.check("新主下一个分片 xid = 旧主被换下时的值", d.ctx.get("xid_new"), d.ctx.get("xid_old"))

    steps.append(Step("受控切主：同一节点的角色在线变化，读不中断、发号不断档", s9_intro, s9_action,
                      "启动后台读探针（量切主期间的读可用性）", s9_sug, s9_checks, s9_finish))

    # ── 第 10 步：宕机 ──
    def s10_intro(d):
        S = d.ctx["S"]
        cnt = {p: sum(1 for sid in S if d.cl.leader(sid) == p) for p in W}
        vic = max(W, key=lambda p: cnt[p])
        mine = [sid for sid in S if d.cl.leader(sid) == vic]
        rep = [sid for sid in S if sid not in mine]
        d.ctx["s10"] = (vic, mine, rep)
        return f"""
杀掉 {d.n(vic)}（immediate stop = 模拟宕机）。它此刻是 {'、'.join(d.sname(s) for s in mine)} 的主、
{'、'.join(d.sname(s) for s in rep)} 的从。要看：

  ① 它当主的 {len(mine)} 个分片**各自**选出新主（各组独立选举、互不等待），短暂不可用后恢复；
  ② 它只当从的分片**完全不受影响**（3 成员剩 2 个仍是多数派）—— 后台读探针应 0 失败；
  ③ 宕机期间照常读写全部分片；
  ④ 把它拉起来：它以从的身份归队，被新主自动重新供给，最后与新主逐字节一致。
"""

    def s10_action(d):
        vic = d.ctx["s10"][0]
        d.bg_start()
        time.sleep(1.5)
        d.drop_sessions(vic)
        r = d.cl.node_stop(vic)
        d.ctx["t_kill"] = time.time()
        print(f"  {d.n(vic)}（:{vic}）已 immediate 停止；后台读探针在跑")

    def s10_sug(d):
        vic, mine, rep = d.ctx["s10"]
        S = d.ctx["S"]
        ids = ",".join(map(str, S))
        nv = d.cl.nid[vic]
        ins = [d.ctx["spare"][sid][2] for sid in S]
        return [
            Sug("coord", f"SELECT partition_id AS 分片号, primary_node AS 主, primary_term AS 任期\n"
                         f"  FROM partdist.partition_map WHERE partition_id IN ({ids}) ORDER BY 1;",
                f"① 等 {d.n(vic)}（节点号 {nv}）当主的分片被登记到别的节点",
                X_fn("没有分片的主还是宕机节点", lambda o, c, nv=nv: len(table_rows(o)) == 3 and all(r[1] != str(nv) for r in table_rows(o))), retry=180),
            Sug("coord", f"SELECT count(*) FROM {TABLE};", "③ 宕机期间经协调者读全部分片", X_val(lambda v, c: v.isdigit(), "读得到")),
            Sug("coord", f"INSERT INTO {TABLE} VALUES ({ins[0]}, 'during-outage', 1);", "③ 宕机期间往 S1 写", X_ok()),
            Sug("coord", f"INSERT INTO {TABLE} VALUES ({ins[1]}, 'during-outage', 1);", "③ 往 S2 写", X_ok()),
            Sug("coord", f"INSERT INTO {TABLE} VALUES ({ins[2]}, 'during-outage', 1);", "③ 往 S3 写", X_ok()),
            Sug("coord", "\\bg stop", "① 当主的分片有一段不可用窗口；② 只当从的分片 0 失败",
                X_fn("只当从的分片 0 失败", lambda o, c, rep=rep: all(c["bg"][d.sname(s)]["fail"] == 0 for s in rep))),
            Sug("coord", "\\roles", "宕机节点显示为连不上；其余两台分担了全部主", X_has("宕机")),
            Sug("coord", f"\\node start {d.n(vic)}", f"④ 拉起 {d.n(vic)}", X_has("已拉起")),
            Sug(d.n(vic), "SELECT group_id, state, leader_node_id FROM partdist.pg_raft_group_status() WHERE group_id <> 0 ORDER BY 1;",
                "④ 它在三个组里都以 follower 身份归队", X_fn("3 个组都是 follower", lambda o, c: [r[1] for r in table_rows(o)].count("follower") == 3), retry=90),
            Sug(d.n(vic), f"SELECT count(*) AS armed槽位 FROM partdist.replay_status() r\n"
                          f" WHERE r.armed AND r.shard IN (SELECT partdist.local_partition_for_shard(g) FROM unnest(ARRAY[{ids}]::bigint[]) g);",
                "④ 三个分片的回放槽位都 armed（原来当主的那几个是被新主自动重新供给的）",
                X_val(lambda v, c: v == "3", "3"), retry=240),
            Sug("coord", "\\cmp", "④ 全部副本（含归队的节点）与各自的主逐字节一致", X_has("全部副本与各自的主逐字节一致", "全部一致")),
        ]

    def s10_finish(d):
        if d.bg:
            d.bg.stop()
            d.bg = None
        vic = d.ctx["s10"][0]
        if not d.cl.up(vic):
            print(yellow(f"  {d.n(vic)} 还没拉起，自动拉起"))
            d.cl.node_start(vic)

    def s10_checks(d):
        vic, mine, rep = d.ctx["s10"]
        for sid in mine:
            lp = d.cl.leader(sid)
            d.check(f"{d.sname(sid)} 已由别的节点接管", lp is not None and lp != vic, True)
        d.check("宕机期间写的 3 行都在", q(COORD, f"SELECT count(*) FROM {TABLE} WHERE item='during-outage'"), "3")

    steps.append(Step("宕机：杀掉身兼多个主的节点，再拉起来归队", s10_intro, s10_action,
                      lambda d: f"启动后台读探针，然后 immediate stop {d.n(d.ctx['s10'][0])}", s10_sug, s10_checks, s10_finish))

    # ── 第 11 步：跨分片 2PC ──
    def s11_intro(d):
        S = d.ctx["S"]
        a, b, c = (d.ctx["keys"][S[0]][2], d.ctx["keys"][S[1]][0], d.ctx["keys"][S[2]][0])
        d.ctx["s11"] = (a, b, c)
        return f"""
一笔事务改三个分片：走协调者，**加入全局事务** —— 先取全局事务号 gxid 与 start_ts，用
pg_partdist.join_info = '<gxid>,<start_ts>,<协调者分片>' 带进事务（交互式里一条 \\gbegin 就帮你做完）。
提交走 2PC，决议先落在协调者分片的 Raft 组多数派上（全局提交点），三个参与分片的分片 clog 上会是
**同一个 commit_ts**。用 sku {a}（S1）、{b}（S2）、{c}（S3）演示「转账」：总量守恒。
"""

    def s11_sug(d):
        a, b, c = d.ctx["s11"]
        S = d.ctx["S"]
        tot = f"SELECT sum(qty) AS 总库存 FROM {TABLE};"
        three = f"SELECT sku, get_shard_id_for_distribution_column('{TABLE}', sku) AS 分片, qty FROM {TABLE} WHERE sku IN ({a},{b},{c}) ORDER BY sku;"
        out = [
            Sug("coord", three, "三个 sku 各在一个分片", X_rows(3)),
            Sug("coord", tot, "转账前总量", X_val(lambda v, c: v.isdigit(), "一个数"), capture="tot0"),
            Sug("coord", "\\gbegin", "开全局事务", X_has("join_info")),
            Sug("coord", f"UPDATE {TABLE} SET qty = qty - 30 WHERE sku = {a};\nUPDATE {TABLE} SET qty = qty + 10 WHERE sku = {b};\n"
                         f"UPDATE {TABLE} SET qty = qty + 20 WHERE sku = {c};\nCOMMIT;", "一笔事务跨三个分片", X_ok()),
            Sug("coord", tot, "转账后总量不变", X_val(lambda v, c: v == c.get("tot0"), "= 转账前")),
        ]
        for sid, k in zip(S, (a, b, c)):
            lp = d.cl.leader(sid)
            lo = f"partdist.local_partition_for_shard({sid})"
            out.append(Sug(d.n(lp), f"SELECT sku, xmin AS 分片xid, partdist.shard_clog_status_full({lo}::oid, xmin::text::bigint) AS 判决\n"
                                    f"  FROM {TABLE}_{sid} WHERE sku = {k};",
                           f"{d.sname(sid)} 的主 {d.n(lp)}：各领自己的分片 xid，commit_ts 三处相同", X_has("st=2"),
                           capture=f"cts_{d.sname(sid)}", capture_col=2))
        out += [
            Sug("coord", "\\gbegin", "再开一个，这次回滚", X_has("join_info")),
            Sug("coord", f"UPDATE {TABLE} SET qty = qty - 999 WHERE sku = {a};\nUPDATE {TABLE} SET qty = qty + 999 WHERE sku = {b};\nROLLBACK;",
                "ROLLBACK：两边都不生效", X_ok()),
            Sug("coord", three, "值没变", X_has(f"{a} ")),
            Sug("coord", "\\gbegin", "再开一个，中途失败", X_has("join_info")),
            Sug("coord", f"UPDATE {TABLE} SET qty = qty - 5 WHERE sku = {a};\nINSERT INTO {TABLE} VALUES ({b}, 'dup', 1);\nCOMMIT;",
                "第二句主键冲突 ⇒ 整笔中止（COMMIT 变 ROLLBACK）；已写的那行在主上成了死元组", X_err("duplicate key")),
            Sug("coord", tot, "原子性：借记一侧没有单独生效，总量不变", X_val(lambda v, c: v == c.get("tot0"), "= 转账前")),
        ]
        return out

    def s11_checks(d):
        cts = []
        for k in ("cts_S1", "cts_S2", "cts_S3"):
            m = re.search(r"cts=(\d+)", d.ctx.get(k) or "")
            cts.append(m.group(1) if m else None)
        d.check("三个分片账本上的 commit_ts 相同", len(set(cts)) == 1 and cts[0] is not None, True)
        np = sum(int(q(p, "SELECT count(*) FROM pg_prepared_xacts") or 0) for p in [COORD] + W)
        d.check("没有 PREPARED 残留", np, 0)

    steps.append(Step("跨分片事务：2PC、守恒、回滚、原子性", s11_intro, None, None, s11_sug, s11_checks))

    # ── 第 12 步：终检 ──
    s12_intro = """
终检：角色一览、全部副本逐字节一致、各节点无 PREPARED 残留、数据完整。

先提交一笔跨三个分片的写：上一步「中途失败」那笔事务被中止了，它在主上留下的记录还躺在主的分区流尾部
（中止的事务不单独复制），要等下一笔提交才随之复制到从 —— 不先提交一笔，主页面上多一个死元组，逐字节比对会不一致。
"""

    def s12_sug(d):
        S = d.ctx["S"]
        ks = [d.ctx["spare"][sid][3] for sid in S]
        vals = ",".join(f"({k}, 'final-check', 1)" for k in ks)
        return [
            Sug("coord", "\\gbegin", "", X_has("join_info")),
            Sug("coord", f"INSERT INTO {TABLE} VALUES {vals};\nCOMMIT;", "一笔跨三个分片的提交，把各主流尾的记录一并复制出去", X_ok()),
            Sug("coord", "\\roles", "", X_has("主")),
            Sug("coord", "\\cmp", "全部副本与各自的主逐字节一致", X_has("全部副本与各自的主逐字节一致", "全部一致")),
            Sug("coord", f"SELECT count(*) AS 行数, sum(qty) AS 总量 FROM {TABLE};", "", X_ok()),
        ]

    def s12_checks(d):
        np = sum(int(q(p, "SELECT count(*) FROM pg_prepared_xacts") or 0) for p in [COORD] + W)
        d.check("各节点无 PREPARED 残留", np, 0)
        d.check("三个分片都有主", all(d.cl.leader(sid) for sid in d.ctx["S"]), True)

    steps.append(Step("终检", s12_intro, None, None, s12_sug, s12_checks))
    return steps


BANNER = r"""
╔══════════════════════════════════════════════════════════════════════╗
║   ShardPG 交互式演示：同一节点上的混合主从（1 协调者 + 3 worker）   ║
╚══════════════════════════════════════════════════════════════════════╝
  回车 = 执行下一条建议 SQL      直接输入 SQL（以 ; 结尾）= 在当前会话执行
  @w2 SELECT …;  = 在 w2 上执行   \use A = 切到会话 A      \help = 全部命令
"""

HELP = r"""
执行
  <回车>             执行本步的下一条建议 SQL           \all          执行本步剩下的全部建议
  SQL…;              在当前会话执行（可多行，以 ; 结尾）  @会话 SQL…;   在指定会话执行
  \use 会话           切换当前会话（coord w1 w2 w3 A B）   \sessions     会话列表
  \wait [会话]        收某会话挂起语句的结果              \reset 会话   关掉会话（事务回滚）
  其它 psql 元命令（\d  \dt  \x …）原样交给当前会话
步骤
  \next / \n  下一步（先跑本步验证）    \back / \b  上一步    \step N  跳到第 N 步
  \list  步骤列表    \show  重看本步讲解和建议    \check  验证本步
观测与动作
  \roles              同一节点混合主从一览（每台 worker 在每个分片组里的角色）
  \cmp [S1|分片号]     副本追平后与当前主逐字节比对（主堆 + 主键索引）
  \gbegin [S1|分片号]  在当前会话开一个全局事务（跨分片写用；默认协调者分片 = S1）
  \where <sku>        这个 sku 在哪个分片、当前主是谁
  \bg start|stop|status  后台读探针（每 200 ms 经协调者读 S1/S2/S3）
  \node stop|start w2  停 / 起一个 worker（stop = immediate，模拟宕机）
  \cheat              观测函数速查        \cleanup  清理现场        \q  退出
"""

CHEAT = r"""
观测点速查（本地 OID 一律用 partdist.local_partition_for_shard(<分片号>) 取）
  分片落在哪 / sku 落在哪          pg_dist_shard ⋈ pg_dist_placement；get_shard_id_for_distribution_column('mx_stock', sku)
  控制面登记的主 / 从 / 任期       partdist.partition_map
  本节点在各组里的角色             partdist.pg_raft_group_status()   （state = leader / follower）
  本节点的回放槽位（从）           partdist.replay_status()          （armed / applied）
  本节点对某分片的角色与发号水位   partdist.route_status(<本地OID>)
  主的分区流位点 / 从收到的位点     partdist.get_partition_flush_lsn(<OID>) / partdist.get_follower_applied_part_lsn(<OID>)
  触发一次惰性回放                 partdist.replay_catchup(<OID>::regclass, <位点>, <超时ms>)
  该分片发到几号了                 partdist.shard_xid_next(<OID>::oid)
  某分片 xid 的判决 + 两个 ts      partdist.shard_clog_status_full(<OID>::oid, <分片xid>)   st: 0 空 1 PREPARED 2 提交 3 中止
  打标状态                         partdist.shard_mvcc_status(<OID>)
  元组的 xmin / xmax（页面级）      heap_page_items(get_raw_page('mx_stock_<分片号>', 0))
  TSO                              partdist.partdist_tso_client_start_ts()、partdist.partdist_tso_status()
  受控切主                         在目标节点：partdist.pg_raft_group_campaign(<分片号>)
  供副本                           在主上：partdist.provision_shard_replica(<分片号>, <目标 pg_raft 节点号>)
"""


def main():
    ap = argparse.ArgumentParser(description="ShardPG 同节点混合主从演示")
    ap.add_argument("--auto", action="store_true", help="全自动跑一遍并断言")
    ap.add_argument("--keep", action="store_true", help="自动模式跑完不清理")
    ap.add_argument("--cleanup", action="store_true", help="只清理演示现场")
    ap.add_argument("--from", dest="start", type=int, default=0, help="交互式从第 N 步开始")
    a = ap.parse_args()
    d = Demo(auto=a.auto, keep=a.keep)
    if a.cleanup:
        sys.exit(0 if cleanup(d) else 1)
    if a.auto:
        sys.exit(0 if d.run_auto(a.start) else 1)
    if a.start:
        if not d.load_topology():
            sys.exit(red(f"--from {a.start} 需要表 {TABLE} 已建好"))
        d.ctx["keys"] = {sid: d.cl.keys_in(sid, 101, 100 + NROWS, NROWS) for sid in d.ctx["S"]}
        d.ctx["spare"] = {sid: d.cl.keys_in(sid, 301, 600, 20) for sid in d.ctx["S"]}
    d.run_interactive(a.start)


if __name__ == "__main__":
    main()
