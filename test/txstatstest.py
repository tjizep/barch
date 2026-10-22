#!/usr/bin/env python3
"""
Statistics for transactions - TODO 706.

INFO transactions shows tx_live (open now), of those tx_active (running a command or
their COMMITTX) and tx_idle, and since the last CONFIG RESETSTAT tx_begins, tx_commits,
tx_aborts and tx_conflicts.

Checked against one barchd, a space without Raft, with every commit that has writes
paused a second and a half halfway (BARCH_TEST_TX_HALF_PAUSE_MS), so a COMMITTX can be
caught running:
  - BEGINTX: one live, idle, one begin;
  - a COMMITTX caught in its pause: one live, active, none idle;
  - after it: none live, one commit;
  - BEGINTX then ABORTTX: an abort, none live;
  - two transactions that read a key and then both commit: a commit, then an abort that
    is a conflict;
  - a connection closed with one open: an abort, none live;
  - CONFIG RESETSTAT zeroes the counts and leaves the gauges.
"""
import os
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txstats_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txstats.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log,
                     env=dict(os.environ, BARCH_TEST_TX_HALF_PAUSE_MS="1500"))
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txst" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def stats():
    text = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True).execute_command("INFO", "transactions")
    # the counters: each open transaction's own line, tx0 on, isn't one - TODO 720
    if isinstance(text, dict):
        return {k: int(v) for k, v in text.items() if not isinstance(v, dict)}
    out = {}
    for line in str(text).splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            if k.strip()[:2] == "tx" and k.strip()[2:].isdigit():
                continue
            out[k.strip()] = int(v)
    return out


def changed(before, after):
    return {k: after[k] - before.get(k, 0) for k in after if after[k] != before.get(k, 0)}


try:
    admin = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    admin.execute_command("CONFIG", "RESETSTAT")
    c = client()
    s0 = stats()
    check(s0.get("tx_live") == 0 and s0.get("tx_begins") == 0, "nothing open or counted to start (%s)" % s0)

    print("a transaction")
    c.execute_command("BEGINTX")
    c.execute_command("SET", "k", "1")
    s = stats()
    check(s["tx_live"] == 1 and s["tx_idle"] == 1 and s["tx_active"] == 0 and s["tx_begins"] == 1,
          "after BEGINTX: one live and idle, one begin (%s)" % s)
    t = threading.Thread(target=lambda: c.execute_command("COMMITTX"))
    t.start()
    time.sleep(0.5)                      # in its pause
    s = stats()
    check(s["tx_live"] == 1 and s["tx_active"] == 1 and s["tx_idle"] == 0,
          "during COMMITTX: one live, active (%s)" % s)
    t.join()
    s = stats()
    check(s["tx_live"] == 0 and s["tx_active"] == 0 and s["tx_commits"] == 1, "after: none live, one commit (%s)" % s)

    print("ABORTTX")
    before = stats()
    c.execute_command("BEGINTX")
    c.execute_command("ABORTTX")
    check(changed(before, stats()) == {"tx_begins": 1, "tx_aborts": 1}, "a begin and an abort, none live (%s)"
          % changed(before, stats()))

    print("a conflict")
    before = stats()
    a, b = client(), client()
    for x in (a, b):
        x.execute_command("BEGINTX")
        x.execute_command("INCR", "n")
    got = [answer(a, "COMMITTX"), answer(b, "COMMITTX")]
    d = changed(before, stats())
    check(got[0] == "OK" and "TXCONFLICT" in str(got[1]), "the second answers TXCONFLICT (%s)" % got)
    check(d == {"tx_begins": 2, "tx_commits": 1, "tx_aborts": 1, "tx_conflicts": 1},
          "two begins, a commit, an abort that's a conflict (%s)" % d)

    print("a connection closed with one open")
    before = stats()
    e = client()
    e.execute_command("BEGINTX")
    check(stats()["tx_live"] == 1, "one live while it's open")
    e.close()
    time.sleep(0.5)
    d = changed(before, stats())
    check(d == {"tx_begins": 1, "tx_aborts": 1} and stats()["tx_live"] == 0, "an abort, none live (%s)" % d)

    print("INFO memory")
    info = admin.execute_command("INFO", "memory")
    text = info if isinstance(info, str) else "\n".join("%s:%s" % kv for kv in info.items())
    check("tx_" not in text, "doesn't carry them: they're INFO transactions' own")

    print("CONFIG RESETSTAT")
    f = client()
    f.execute_command("BEGINTX")
    admin.execute_command("CONFIG", "RESETSTAT")
    s = stats()
    check(s["tx_begins"] == 0 and s["tx_commits"] == 0 and s["tx_aborts"] == 0 and s["tx_live"] == 1,
          "zeroes the counts and leaves the gauges (%s)" % s)
    f.execute_command("ABORTTX")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
