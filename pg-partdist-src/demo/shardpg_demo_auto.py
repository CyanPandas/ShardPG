#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
shardpg_demo_auto.py —— ShardPG 全自动演示（只用于演示，不改动项目任何文件）

它做的事，和你在两个 psql 窗口里照着 shardpg_demo_steps.sql 一条条输入完全一样：
  · 每条 SQL 像有人在敲键盘一样逐字打出来（A> 是窗口 A，B> 是窗口 B），旁白以 "--" 注释打出；
  · 打完回车，真的发给 master 上对应的常驻会话执行，结果（包括 NOTICE 时间线）实时打出来；
  · 事务可以跨多条输入，B 的语句被 A 的行锁挡住时也和手工一样："先别等它，回到 A 继续"。

默认一条龙：准备环境（start，含提前建表/建组/选主/供副本/打标，约 2–5 分钟）→ 播放全部步骤 → 恢复环境（stop）。

用法：
  bash shardpg_demo.sh auto                    # 一条龙
  bash shardpg_demo.sh auto --no-prepare       # 环境已经 start 过，直接播放
  bash shardpg_demo.sh auto --keep             # 播完不 stop（留着环境给你继续手敲）
  bash shardpg_demo.sh auto --speed slow       # 打字慢一点（fast / normal / slow）
  bash shardpg_demo.sh auto --pause 3          # 每条语句之间停 3 秒（给观众看结果）
  bash shardpg_demo.sh auto --from 4           # 从第 4 步开始播（前面几步的结果已在环境里）
"""
import argparse
import codecs
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
CONTAINER = os.environ.get("CONTAINER", "pg-test-container")
PSQL = "/work/pg-install/bin/psql"
LAUNCHER = os.path.join(HERE, "shardpg_demo.sh")
# 两套演示共用启动器（DEMO_PROFILE=4 / 128，由 shardpg_demo.sh / shardpg_demo128.sh 传进来）
PROFILE = os.environ.get("DEMO_PROFILE", "4")
LAUNCH_NAME = "shardpg_demo128.sh" if PROFILE == "128" else "shardpg_demo.sh"
DEMO_TITLE = ("演示二：128 个分区组上的分布式事务" if PROFILE == "128"
              else "演示一：4 个分片的一张业务表")
DEFAULT_STEPS = os.path.join(HERE, "shardpg_demo_steps.sql")

SPEED = {"fast": (0.003, 0.4), "normal": (0.014, 1.6), "slow": (0.035, 3.0)}   # (每字符秒, 语句间停顿秒)
STMT_TIMEOUT = 1800        # 单条语句最长等多久（灌数据要几分钟）
BLOCKED_WAIT = 4           # "-- @B &" 的语句等这么久还没返回，就当它被挡住了
# 2026-10-13：读到"判决暂时无法确定"时，事务外的读按报错的 HINT「稍后重试」，最多这么多次。
#   实测：刚 COMMIT 完紧接着读，偶尔（机器忙时更常见）读路径向协调组问判决拿不到答复，持续十几秒后自愈；
#   产品侧同 P7-N52 一族，未定因。重试前顺手 demo.settle() 触发一次待应用判决清扫。
INDOUBT_MARK = "判决暂时无法确定"
INDOUBT_RETRIES = 6
INDOUBT_SLEEP = 5


# ───────────────────────────── 终端输出 ─────────────────────────────
class Term:
    def __init__(self, color, delay, pause):
        self.color, self.delay, self.pause = color, delay, pause

    def c(self, code, s):
        return f"\033[{code}m{s}\033[0m" if self.color else s

    def w(self, s=""):
        sys.stdout.write(s)
        sys.stdout.flush()

    def line(self, s=""):
        self.w(s + "\n")

    def type_text(self, s, factor=1.0):
        """逐字打出 s（不含换行）"""
        d = self.delay * factor
        if d <= 0:
            self.w(s)
            return
        for ch in s:
            self.w(ch)
            time.sleep(d if ch != " " else d * 0.6)

    def banner(self, title):
        bar = "═" * 78
        self.line()
        self.line(self.c("1;36", bar))
        self.line(self.c("1;36", "  " + title))
        self.line(self.c("1;36", bar))

    def sleep(self, sec):
        if sec > 0:
            time.sleep(sec)


PROMPT_COLOR = {"A": "1;32", "B": "1;33"}


# ───────────────────────────── 常驻 psql 会话 ─────────────────────────────
class Session:
    """一个常驻的 psql 进程：事务可以跨多次输入；输出实时读取；语句被挡住时不会卡死界面。"""

    def __init__(self, name, port=5432):
        self.name, self.port = name, port
        self.proc = None
        self.q = queue.Queue()
        self.seq = 0
        self.pending = None      # 被挡住、还没返回的语句的结束标记
        self.in_txn = False      # 是否在 BEGIN … COMMIT/ROLLBACK 之间（按敲进去的语句粗略跟踪）

    def spawn(self):
        # ★ stderr 必须在容器里并进 stdout：docker exec（无 -t）把两个流分开复用传输，二者先后没有保证，
        #   报错（stderr）偶尔会晚于结束标记（stdout）到达，被算到下一条语句头上。
        cmd = ["docker", "exec", "-i", "-u", "postgres", CONTAINER, "bash", "-c",
               f"exec {PSQL} -h /tmp -p {self.port} -U postgres -d postgres -X "
               f"-P pager=off -v ON_ERROR_STOP=0 -v VERBOSITY=default 2>&1"]
        self.proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, bufsize=0)
        self.q = queue.Queue()
        self.pending = None
        self.in_txn = False
        threading.Thread(target=self._reader, args=(self.proc, self.q), daemon=True).start()

    @staticmethod
    def _reader(proc, q):
        dec = codecs.getincrementaldecoder("utf-8")("replace")
        buf = ""
        while True:
            try:
                data = os.read(proc.stdout.fileno(), 65536)
            except OSError:
                break
            if not data:
                break
            buf += dec.decode(data)
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                q.put(line)
        if buf:
            q.put(buf)
        q.put(None)      # EOF

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def close(self):
        if self.proc is not None:
            try:
                self.proc.stdin.write(b"\\q\n")
                self.proc.stdin.close()
            except Exception:
                pass
            try:
                self.proc.wait(timeout=3)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
            self.proc = None

    def send(self, stmt):
        """发一条语句（末尾自动补 ;），后面跟一个结束标记；返回标记"""
        if not self.alive():
            self.spawn()
        self.seq += 1
        marker = f"__SHARDPG_END_{self.name}_{self.seq}__"
        body = stmt.rstrip()
        if not body.lstrip().startswith("\\") and not body.endswith(";"):
            body += ";"
        self.proc.stdin.write((body + "\n\\echo " + marker + "\n").encode("utf-8"))
        kw = body.lstrip().split(None, 1)[0].upper().rstrip(";") if body.strip() else ""
        if kw in ("BEGIN", "START"):
            self.in_txn = True
        elif kw in ("COMMIT", "ROLLBACK", "END", "ABORT"):
            self.in_txn = False
        return marker

    def wait(self, marker, timeout, emit):
        """读到 marker 为止，每行交给 emit。→ (行列表, 状态)；状态 True=完成 / False=超时 / None=连接断了"""
        end = time.time() + timeout
        out = []
        while True:
            left = end - time.time()
            if left <= 0:
                return out, False
            try:
                line = self.q.get(timeout=min(0.25, left))
            except queue.Empty:
                continue
            if line is None:
                return out, None
            s = line.strip()
            if s.startswith("__SHARDPG_END_"):
                if s == marker:
                    return out, True
                continue                       # 更早那条挂起语句的标记：它的输出已经在 out 里了
            out.append(line)
            emit(line)


# ───────────────────────────── 解析步骤脚本 ─────────────────────────────
def parse_steps(path):
    """→ [{title, blocks:[{sess, mode, sql, narr}]}]（与 shardpg_demo_steps.sql 的标注一一对应）"""
    steps, cur, narr, block = [], None, [], None

    def flush():
        nonlocal block
        if block is not None:
            block["sql"] = "\n".join(block["sql"]).strip()
            cur["blocks"].append(block)
        block = None

    for line in open(path, encoding="utf-8").read().splitlines():
        m = re.match(r"^-- ════ (第 \d+ 步：.*?)\s*════$", line)
        if m:
            flush()
            cur = {"title": m.group(1), "blocks": []}
            steps.append(cur)
            narr = []
            continue
        if cur is None:
            continue
        m = re.match(r"^-- @([AB])\s*(&|wait)?\s*$", line)
        if m:
            flush()
            block = {"sess": m.group(1), "mode": m.group(2) or "", "sql": [], "narr": narr}
            narr = []
            continue
        if line.startswith("-- ") or line.strip() == "--":
            flush()
            narr.append(line[3:] if line.startswith("-- ") else "")
            continue
        if block is not None and line.strip():
            block["sql"].append(line)
    flush()
    return steps


def split_statements(sql):
    """一个块里可能有好几条语句：像手敲一样，一条一条打、一条一条执行"""
    stmts, cur = [], []
    for line in sql.splitlines():
        if not line.strip():
            continue
        cur.append(line)
        end = line.rstrip()
        if end.endswith(";") or end.lstrip().startswith("\\"):
            stmts.append("\n".join(cur))
            cur = []
    if cur:
        stmts.append("\n".join(cur))
    return stmts


# ───────────────────────────── 播放 ─────────────────────────────
def play(steps, term, sessions, first_step, rec):
    t_all = time.time()
    for st in steps:
        num = int(re.match(r"第 (\d+) 步", st["title"]).group(1))
        if num < first_step:
            continue
        term.banner(st["title"])
        rec_step = {"title": st["title"], "blocks": []}
        for b in st["blocks"]:
            sess = sessions[b["sess"]]
            color = PROMPT_COLOR[b["sess"]]
            term.line()
            for n in b["narr"]:                                 # 旁白：像敲注释一样快速打出
                term.w(term.c("2;37", "-- "))
                term.type_text(n, 0.25)
                term.line()
            t0 = time.time()
            outs = []

            def emit(line):
                term.line(line)

            if b["mode"] == "wait":
                term.line(term.c("2;37", f"-- （回到窗口 {b['sess']}，看刚才被挡住的那条语句的结果）"))
                if sess.pending:
                    out, done = sess.wait(sess.pending, 180, emit)
                    outs += out
                    sess.pending = None
                    if done is not True:
                        term.line(term.c("1;31", "（这条语句 180 s 还没返回）"))
                else:
                    term.line(term.c("2;37", "（没有挂起的语句）"))
                term.sleep(term.pause)
            else:
                stmts = split_statements(b["sql"])
                for stmt in stmts:
                    # 上一条挂起的语句这时才返回？别丢
                    if sess.pending:
                        out, done = sess.wait(sess.pending, 0.3, emit)
                        if done:
                            sess.pending = None
                    lines = stmt.splitlines()
                    for i, ln in enumerate(lines):
                        prompt = f"{b['sess']}> " if i == 0 else f"{b['sess']}…> "
                        term.w(term.c(color, prompt))
                        term.type_text(ln)
                        term.line()
                    term.sleep(0.25)
                    marker = sess.send(stmt)
                    blocked = b["mode"] == "&"
                    out, done = sess.wait(marker, BLOCKED_WAIT if blocked else STMT_TIMEOUT, emit)
                    outs += out
                    if done is False and blocked:
                        sess.pending = marker
                        term.line(term.c("2;37", f"-- （{b['sess']} 这条被行锁挡住了，终端停在这里 —— 先别等它，切回窗口 A）"))
                        break
                    if done is False:
                        term.line(term.c("1;31", f"-- （等了 {STMT_TIMEOUT} s 还没返回，放弃等待）"))
                        sess.pending = marker
                        break
                    if done is None:
                        term.line(term.c("1;31", f"-- （会话 {b['sess']} 的连接断了）"))
                        break
                    # 事务外的读撞上"判决暂时无法确定"：补判后按提示重试（事务内报错整笔已中止，不能重试）
                    tries = 0
                    while (done is True and not sess.in_txn and tries < INDOUBT_RETRIES
                           and any(INDOUBT_MARK in o for o in out)):
                        tries += 1
                        term.line(term.c("2;37", f"-- （刚提交那笔的判决还没落到这个分片上，向协调组问询也没拿到答复 —— "
                                                 f"按提示 {INDOUBT_SLEEP} s 后重试，第 {tries} 次）"))
                        settle()        # 顺手触发一次待应用判决清扫（能落定的先落定）
                        term.sleep(INDOUBT_SLEEP)
                        for i, ln in enumerate(lines):
                            prompt = f"{b['sess']}> " if i == 0 else f"{b['sess']}…> "
                            term.w(term.c(color, prompt))
                            term.type_text(ln)
                            term.line()
                        marker = sess.send(stmt)
                        out, done = sess.wait(marker, STMT_TIMEOUT, emit)
                        outs += out
                    term.sleep(term.pause)
            rec_step["blocks"].append({
                "sess": b["sess"], "mode": b["mode"], "sql": b["sql"], "narr": b["narr"],
                "out": "\n".join(outs).rstrip("\n"), "secs": round(time.time() - t0, 1), "done": True})
        rec["steps"].append(rec_step)
    rec["total"] = round(time.time() - t_all)


def settle():
    """在 master 上跑 demo.settle()（各 worker 触发一次待应用判决清扫），返回它的一行结果"""
    try:
        r = subprocess.run(["docker", "exec", "-i", "-u", "postgres", CONTAINER, PSQL, "-h", "/tmp", "-p", "5432",
                            "-U", "postgres", "-d", "postgres", "-X", "-qAt", "-c", "SELECT demo.settle()"],
                           stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
        return r.stdout.decode("utf-8", "replace").strip().splitlines()[-1] if r.stdout.strip() else ""
    except Exception:
        return ""


def run_launcher(cmd, term, label, log):
    """跑启动器的 start/stop，输出实时转出来并记下来"""
    term.line(term.c("2;37", f"$ bash {LAUNCH_NAME} {cmd}"))
    p = subprocess.Popen(["bash", LAUNCHER, cmd], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         text=True, encoding="utf-8", errors="replace", bufsize=1)
    for ln in p.stdout:
        term.w(ln)
        log.append(ln.rstrip("\n"))
    rc = p.wait()
    if rc != 0:
        term.line(term.c("1;31", f"{label}失败（退出码 {rc}），上面就是原因。"))
    return rc


def main():
    ap = argparse.ArgumentParser(description="ShardPG 全自动演示：自己敲 SQL、自己看结果",
                                 formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    ap.add_argument("--steps", default=DEFAULT_STEPS, help="步骤脚本（默认 shardpg_demo_steps.sql）")
    ap.add_argument("--no-prepare", action="store_true", help="环境已经 start 过，跳过准备")
    ap.add_argument("--keep", action="store_true", help="播完不 stop（保留环境）")
    ap.add_argument("--speed", choices=list(SPEED), help="打字速度（终端里默认 normal，非终端默认 fast）")
    ap.add_argument("--pause", type=float, help="每条语句之间停顿秒数")
    ap.add_argument("--from", dest="first_step", type=int, default=1, metavar="N", help="从第 N 步开始")
    ap.add_argument("--no-color", action="store_true")
    ap.add_argument("--record", metavar="FILE", help="把每段 SQL 与真实输出记成 JSON（生成教程用）")
    args = ap.parse_args()

    tty = sys.stdout.isatty()
    speed = args.speed or ("normal" if tty else "fast")
    delay, pause = SPEED[speed]
    if not tty and not args.speed:
        delay = 0
        pause = 0.0
    if args.pause is not None:
        pause = args.pause
    term = Term(color=tty and not args.no_color, delay=delay, pause=pause)

    steps = parse_steps(args.steps)
    if not steps:
        print("步骤脚本里一步都没解析出来：" + args.steps, file=sys.stderr)
        return 2

    rec = {"steps": [], "total": 0, "start_log": "", "stop_log": "", "help": ""}
    sessions = {"A": Session("A"), "B": Session("B")}
    rc = 0
    interrupted = False
    try:
        term.banner(f"ShardPG 全自动演示 · {DEMO_TITLE} · 5 个节点：1 master + 4 worker")
        term.line(term.c("2;37", "  下面每一行都是自动“敲”出来的 SQL：A> 是窗口 A，B> 是窗口 B；-- 开头的是旁白。"))
        if not args.no_prepare:
            term.line()
            log = []
            if run_launcher("start", term, "准备环境", log) != 0:
                return 1
            rec["start_log"] = "\n".join(log)
        play(steps, term, sessions, args.first_step, rec)
        if args.record:
            p = subprocess.run(["docker", "exec", "-i", "-u", "postgres", CONTAINER, PSQL, "-h", "/tmp", "-p", "5432",
                                "-U", "postgres", "-d", "postgres", "-X", "-c", "SELECT * FROM demo.help();"],
                               stdin=subprocess.DEVNULL, capture_output=True, text=True, encoding="utf-8")
            rec["help"] = p.stdout.strip()
    except KeyboardInterrupt:
        interrupted = True
        term.line(term.c("1;31", "\n已中断。"))
        rc = 130
    finally:
        for s in sessions.values():
            s.close()

    if args.record:
        json.dump(rec, open(args.record, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    if interrupted:
        term.line(f"环境保留着；要恢复请运行：bash {LAUNCH_NAME} stop")
        return rc
    term.banner("演示结束")
    if args.keep:
        term.line(f"环境保留着（--keep）；要恢复请运行：bash {LAUNCH_NAME} stop")
        return rc
    log = []
    rc2 = run_launcher("stop", term, "恢复环境", log)
    rec["stop_log"] = "\n".join(log)
    if args.record:
        json.dump(rec, open(args.record, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    return rc or rc2


if __name__ == "__main__":
    sys.exit(main())
