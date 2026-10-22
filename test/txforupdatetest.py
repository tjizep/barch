#!/usr/bin/env python3
"""
GETFORUPDATE and MGETFORUPDATE - TODO 729.

In a pessimistic transaction a read takes a shared lock and a write an exclusive one.
A transaction that reads a key and then writes it holds the shared lock in between,
so two doing that at once each wait for the other's to go, and one gets TXDEADLOCK.
GETFORUPDATE and MGETFORUPDATE read like GET and MGET but take the exclusive lock at
once, so the second waits for the first to finish instead.

Checked against one barchd with no Raft, tx_lock_wait_ms 1500:
  - outside a transaction, and in an optimistic one, they answer as GET and MGET;
  - GET then SET in two transactions at once deadlocks, GETFORUPDATE then SET doesn't:
    the second waits, is listed waiting on GETFORUPDATE, and then reads the first's
    write, so no update is lost;
  - a GETFORUPDATE waits for a shared reader, and a reader waits for it;
  - MGETFORUPDATE locks every key it reads;
  - clients doing read-then-write increments with GETFORUPDATE never deadlock, and
    every increment is kept.
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
PORT = scale.port(default=14818)
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
DATA = os.path.join(os.getcwd(), "txforupdate_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txforupdate.log"), "w")
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

SPACE = "txforupdate_" + str(int(time.time()))


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


def deadlocks(watch):
    m = re.search(r"^tx_deadlocks:(\d+)$", watch.execute_command("INFO", "transactions"), re.M)
    return int(m.group(1)) if m else -1


def is_deadlock(r):
    return str(r).startswith("ERROR TXDEADLOCK")


def increments(read, clients=4, each=25):
    """clients adding one to n, read then write, retried; the deadlocks it met"""
    met = [0]
    lock = threading.Lock()

    def worker():
        c = client()
        for _ in range(each):
            for attempt in range(200):
                run(c, "BEGINTX", "PESSIMISTIC")
                v = run(c, read, "n")
                r = run(c, "SET", "n", int(v) + 1) if not str(v).startswith("ERROR") else v
                if r is True and run(c, "COMMITTX") == "OK":
                    break
                if is_deadlock(r) or is_deadlock(v):
                    with lock:
                        met[0] += 1
                run(c, "ABORTTX")
                time.sleep(0.001 * attempt)
    threads = [threading.Thread(target=worker) for _ in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return met[0]


watch = client()
try:
    print("outside a pessimistic transaction")
    watch.set("a", "1")
    watch.set("b", "2")
    watch.rpush("l", "x")
    check(run(watch, "GETFORUPDATE", "a") == "1", "GETFORUPDATE reads as GET")
    check(run(watch, "GETFORUPDATE", "nope") is None, "a missing key is nil")
    check(run(watch, "MGETFORUPDATE", "a", "nope", "b") == ["1", None, "2"], "MGETFORUPDATE reads as MGET")
    check(str(run(watch, "GETFORUPDATE", "l")).startswith("ERROR WRONGTYPE"), "a list is the wrong type, as with GET")
    check(str(run(watch, "GETFORUPDATE")).startswith("ERROR"), "no key is an error")
    o = client()
    run(o, "BEGINTX")
    check(run(o, "GETFORUPDATE", "a") == "1", "in an optimistic transaction too")
    watch.set("a", "changed")
    check(run(o, "GETFORUPDATE", "a") == "1", "which reads its snapshot")
    check(run(o, "COMMITTX") == "OK", "and commits")
    watch.set("a", "1")

    print("GET then SET deadlocks")
    before = deadlocks(watch)
    a, b = client(), client()
    for x in (a, b):
        run(x, "BEGINTX", "PESSIMISTIC")
        run(x, "GET", "s")
    out = {}
    t = later(a, out, "a", "SET", "s", "a")
    time.sleep(0.3)
    r = run(b, "SET", "s", "b")
    t.join()
    check(is_deadlock(r), "one of the two answers TXDEADLOCK")
    run(a, "COMMITTX")
    run(b, "ABORTTX")
    check(deadlocks(watch) == before + 1, "and it's counted")

    print("GETFORUPDATE then SET doesn't")
    before = deadlocks(watch)
    watch.set("s", "10")
    a, b = client(), client()
    run(a, "BEGINTX", "PESSIMISTIC")
    run(b, "BEGINTX", "PESSIMISTIC")
    check(run(a, "GETFORUPDATE", "s") == "10", "the first reads")
    out = {}
    t = later(b, out, "b", "GETFORUPDATE", "s")
    time.sleep(0.3)
    check("b" not in out, "the second waits at its read")
    listed = watch.execute_command("INFO", "transactions", "waiting")
    check("command=GETFORUPDATE" in listed and "waiting=a lock on s" in listed,
          "listed waiting on GETFORUPDATE")
    check(run(a, "SET", "s", "11") is True, "the first writes, no wait")
    check(run(a, "COMMITTX") == "OK", "and commits")
    t.join()
    check(out.get("b") == "11", "the second reads the first's write (%s)" % out.get("b"))
    check(run(b, "SET", "s", "12") is True and run(b, "COMMITTX") == "OK", "and writes and commits")
    check(watch.get("s") == "12", "no update lost")
    check(deadlocks(watch) == before, "no deadlock")

    print("against shared readers")
    r1, w1 = client(), client()
    run(r1, "BEGINTX", "PESSIMISTIC")
    run(r1, "GET", "s")
    run(w1, "BEGINTX", "PESSIMISTIC")
    out = {}
    t = later(w1, out, "w", "GETFORUPDATE", "s")
    time.sleep(0.3)
    check("w" not in out, "GETFORUPDATE waits for a reader")
    run(r1, "COMMITTX")
    t.join()
    check(out.get("w") == "12" and out.get("w_s", 9) < 1.0, "and goes on when it's done")
    run(r1, "BEGINTX", "PESSIMISTIC")
    out = {}
    t = later(r1, out, "r", "GET", "s")
    time.sleep(0.3)
    check("r" not in out, "a reader waits for GETFORUPDATE")
    run(w1, "COMMITTX")
    t.join()
    check(out.get("r") == "12", "and goes on when it's done")
    run(r1, "COMMITTX")

    print("MGETFORUPDATE")
    m, other = client(), client()
    run(m, "BEGINTX", "PESSIMISTIC")
    check(run(m, "MGETFORUPDATE", "a", "b") == ["1", "2"], "reads both")
    run(other, "BEGINTX", "PESSIMISTIC")
    out = {}
    t = later(other, out, "o", "GET", "b")
    time.sleep(0.3)
    check("o" not in out, "and locks the second for writing too")
    run(m, "COMMITTX")
    t.join()
    check(out.get("o") == "2", "let go at COMMITTX")
    run(other, "COMMITTX")

    print("read then write, four clients")
    watch.set("n", "0")
    before = deadlocks(watch)
    met = increments("GET")
    check(int(watch.get("n")) == 100, "with GET every increment is kept (%s)" % watch.get("n"))
    check(met > 0, "but they meet deadlocks (%d)" % met)
    watch.set("n", "0")
    before = deadlocks(watch)
    met = increments("GETFORUPDATE")
    check(int(watch.get("n")) == 100, "with GETFORUPDATE every increment is kept (%s)" % watch.get("n"))
    check(met == 0 and deadlocks(watch) == before, "and none deadlock (%d)" % met)
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
