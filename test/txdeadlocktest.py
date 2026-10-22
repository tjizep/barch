#!/usr/bin/env python3
"""
Deadlock detection for pessimistic transactions - TODO 724.

A transaction that starts waiting for a lock is checked, there and then, for closing a
ring of transactions each waiting on the next. If it would, it answers TXDEADLOCK at
once instead of waiting out tx_lock_wait_ms, its locks are let go of so the others go
on, and it can only be ended.

Checked against one barchd with no Raft, tx_lock_wait_ms 1500:
  - two in a ring: the second to wait answers TXDEADLOCK at once, the first goes on;
  - three in a ring, the TXDEADLOCK naming the others in order;
  - two holding a key shared, both wanting to write it;
  - a ring closed while the other waits inside EXEC, on its thread, not parked;
  - a plain chain of waits isn't taken for a ring;
  - one that timed out and gave up isn't part of a ring any more;
  - tx_deadlocks counts them, and nothing is left waiting.
"""
import os
import re
import socket
import subprocess
import sys
import threading
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14815)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-74s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


WAIT_MS = 1500
DATA = os.path.join(os.getcwd(), "txdeadlock_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txdeadlock.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=%d" % WAIT_MS],
                     stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txdeadlock_" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.response_callbacks.pop("INFO", None)
    c.execute_command("USE", SPACE)
    return c


def run(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERROR " + str(e)


def later(c, out, name, *cmd):
    def go():
        t0 = time.time()
        out[name] = run(c, *cmd)
        out[name + "_s"] = time.time() - t0
    t = threading.Thread(target=go)
    t.start()
    return t


def timed(c, *cmd):
    t0 = time.time()
    r = run(c, *cmd)
    return r, time.time() - t0


def counters(watch):
    return {k: int(v) for k, v in re.findall(r"^(tx_[a-z_]+):(\d+)$", watch.execute_command("INFO", "transactions"),
                                              re.M)}


def waiting(watch):
    return [line for line in watch.execute_command("INFO", "transactions", "waiting").splitlines()
            if line.startswith("tx") and line[2:3].isdigit()]


def ids(watch):
    """each open transaction's id, by the key it holds first"""
    out = {}
    for line in watch.execute_command("INFO", "transactions").splitlines():
        m = re.match(r"tx\d+:id=([0-9a-f]+),.*,held=([^,|]*)", line)
        if m:
            out[m.group(2)] = m.group(1)
    return out


def begin(*pairs):
    """a pessimistic transaction on each client, writing its key"""
    for c, k in pairs:
        run(c, "BEGINTX", "PESSIMISTIC")
        run(c, "SET", k, "held")


def is_deadlock(r):
    return str(r).startswith("ERROR TXDEADLOCK")


watch = client()
try:
    start = counters(watch).get("tx_deadlocks", 0)
    check("tx_deadlocks" in counters(watch), "INFO transactions has tx_deadlocks")

    print("two in a ring")
    a, b = client(), client()
    begin((a, "x"), (b, "y"))
    out = {}
    t = later(a, out, "a", "SET", "y", "a")
    time.sleep(0.3)
    check("a" not in out, "the first waits")
    r, s = timed(b, "SET", "x", "b")
    check(is_deadlock(r) and s < 0.5, "the second answers TXDEADLOCK at once (%.2f s)" % s)
    check("is locked by transaction" in str(r) and "which waits on this one" in str(r), "naming the one in the way")
    t.join()
    check(out.get("a") is True and out.get("a_s", 9) < 1.0, "the first goes on (%s, %.2f s)" % (out.get("a"), out.get("a_s", 9)))
    check(is_deadlock(run(b, "GET", "q")), "the second can only end")
    check(run(a, "COMMITTX") == "OK", "the first commits")
    run(b, "ABORTTX")
    check(run(watch, "GET", "y") == "a" and run(watch, "GET", "x") == "held", "with its writes, not the second's")

    print("three in a ring")
    a, b, c = client(), client(), client()
    begin((a, "x"), (b, "y"), (c, "z"))
    who = ids(watch)
    out = {}
    ta = later(a, out, "a", "SET", "y", "a")
    time.sleep(0.2)
    tb = later(b, out, "b", "SET", "z", "b")
    time.sleep(0.2)
    check("a" not in out and "b" not in out, "two wait, one on the next")
    r, s = timed(c, "SET", "x", "c")
    check(is_deadlock(r) and s < 0.5, "the third answers TXDEADLOCK at once (%.2f s)" % s)
    said = "is locked by transaction %s, which waits on %s, which waits on this one" % (who.get("x"), who.get("y"))
    check(said in str(r), "naming the ring in order (%s)" % str(r)[:120])
    tb.join()
    check(out.get("b") is True and out.get("b_s", 9) < 1.0, "the one it held up goes on")
    check(run(b, "COMMITTX") == "OK", "and commits")
    ta.join()
    check(out.get("a") is True, "then the first")
    check(run(a, "COMMITTX") == "OK", "and commits")
    run(c, "ABORTTX")

    print("two holding a key shared, both writing it")
    a, b = client(), client()
    for x in (a, b):
        run(x, "BEGINTX", "PESSIMISTIC")
        run(x, "GET", "s")
    out = {}
    t = later(a, out, "a", "SET", "s", "a")
    time.sleep(0.3)
    check("a" not in out, "the first waits for the other reader")
    r, s = timed(b, "SET", "s", "b")
    check(is_deadlock(r) and s < 0.5, "the second answers TXDEADLOCK at once (%.2f s)" % s)
    t.join()
    check(out.get("a") is True and out.get("a_s", 9) < 1.0, "the first goes on")
    check(run(a, "COMMITTX") == "OK", "and commits")
    run(b, "ABORTTX")

    print("closed while the other waits inside EXEC")
    a, b = client(), client()
    begin((a, "x"), (b, "y"))
    out = {}

    def run_exec():
        t0 = time.time()
        run(a, "MULTI")
        run(a, "SET", "y", "a")
        out["exec"] = run(a, "EXEC")
        out["exec_s"] = time.time() - t0
    t = threading.Thread(target=run_exec)
    t.start()
    time.sleep(0.3)
    check("exec" not in out, "EXEC waits")
    r, s = timed(b, "SET", "x", "b")
    check(is_deadlock(r) and s < 0.5, "the other answers TXDEADLOCK at once (%.2f s)" % s)
    t.join()
    check(out.get("exec_s", 9) < 1.0 and isinstance(out.get("exec"), list), "EXEC goes on (%s, %.2f s)"
          % (out.get("exec"), out.get("exec_s", 9)))
    check(run(a, "COMMITTX") == "OK", "and its transaction commits")
    run(b, "ABORTTX")

    print("a chain, not a ring")
    before = counters(watch).get("tx_deadlocks", 0)
    a, b, c = client(), client(), client()
    begin((a, "x"), (b, "y"))
    out = {}
    tb = later(b, out, "b", "SET", "x", "b")
    time.sleep(0.2)
    run(c, "BEGINTX", "PESSIMISTIC")
    tc = later(c, out, "c", "SET", "y", "c")
    time.sleep(0.3)
    check("b" not in out and "c" not in out, "both wait")
    check(len(waiting(watch)) == 2, "both listed as waiting")
    check(run(a, "COMMITTX") == "OK", "the first commits")
    tb.join()
    check(out.get("b") is True, "the second goes on (%s)" % out.get("b"))
    check(run(b, "COMMITTX") == "OK", "and commits")
    tc.join()
    check(out.get("c") is True, "then the third (%s)" % out.get("c"))
    check(run(c, "COMMITTX") == "OK", "and commits")
    check(counters(watch).get("tx_deadlocks", 0) == before, "no deadlock counted")

    print("one that timed out isn't in a ring")
    a, b = client(), client()
    begin((a, "x"), (b, "y"))
    r, s = timed(b, "SET", "x", "b")
    check(str(r).startswith("ERROR TXLOCKTIMEOUT"), "the second times out (%.2f s)" % s)
    out = {}
    t = later(a, out, "a", "SET", "y", "a")
    time.sleep(0.3)
    check("a" not in out, "the first waits for its key, no TXDEADLOCK")
    run(b, "ABORTTX")
    t.join()
    check(out.get("a") is True and out.get("a_s", 9) < 1.0, "and goes on when it ends (%s)" % out.get("a"))
    check(run(a, "COMMITTX") == "OK", "and commits")

    print("afterwards")
    check(counters(watch).get("tx_deadlocks", 0) - start == 4,
          "tx_deadlocks counts four (%d)" % (counters(watch).get("tx_deadlocks", 0) - start))
    check(waiting(watch) == [], "nothing waiting")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
