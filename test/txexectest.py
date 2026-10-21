#!/usr/bin/env python3
"""
MULTI/EXEC as a transaction - TODO 710, the last of TODO 697.

When every queued command is one a transaction takes, all in one space, and the
connection has no transaction open, EXEC runs them as one transaction. The client saw
nothing in between, so a TXCONFLICT reruns the whole batch. Anything else keeps the old
EXEC: one command after another.

Checked against one barchd, a space without Raft:
  - EXEC answers each queued command's reply, as before;
  - four clients each running MULTI; INCR a; INCR b; EXEC while a fifth reads MGET a b:
    the reader never sees a and b differ, and nothing is lost;
  - an EXEC with a command a transaction doesn't take (HSET) still runs;
  - MULTI/EXEC inside an open BEGINTX runs in that transaction, seen only at COMMITTX.
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
PORT = scale.port(default=14740)
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


DATA = os.path.join(os.getcwd(), "txexec_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txexec.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

TAG = str(int(time.time()))
SPACE = "txe" + TAG
ROUNDS = max(50, int(400 * float(os.environ.get("BARCH_TEST_SCALE", "1"))))


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    return c


def stats():
    text = client().execute_command("INFO", "transactions")
    if isinstance(text, dict):
        return {k: int(v) for k, v in text.items()}
    out = {}
    for line in str(text).splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            out[k.strip()] = int(v)
    return out


def multi(c, *cmds):
    c.execute_command("MULTI")
    for cmd in cmds:
        c.execute_command(*cmd)
    return c.execute_command("EXEC")


try:
    c = client()
    print("EXEC answers each reply")
    before = stats()
    got = multi(c, ("SET", "k", "1"), ("INCR", "k"), ("GET", "k"), ("GET", "none"))
    check(list(got) == ["OK", 2, "2", None], "SET, INCR, GET, GET of nothing (%s)" % got)
    check(stats()["tx_commits"] == before.get("tx_commits", 0) + 1, "and it ran as a transaction")

    print("concurrent EXECs and a reader")
    c.execute_command("MSET", "a", "0", "b", "0")
    errors = []
    torn = []
    stop = threading.Event()

    def writer():
        w = client()
        for _ in range(ROUNDS):
            try:
                got = multi(w, ("INCR", "a"), ("INCR", "b"))
                if len(got) != 2 or got[0] != got[1]:
                    torn.append(("writer", got))
            except redis.ResponseError as e:
                errors.append(str(e))

    def reader():
        r = client()
        while not stop.is_set():
            a, b = r.execute_command("MGET", "a", "b")
            if a != b:
                torn.append((a, b))

    before = stats()
    rt = threading.Thread(target=reader)
    rt.start()
    ws = [threading.Thread(target=writer) for _ in range(4)]
    for t in ws:
        t.start()
    for t in ws:
        t.join()
    stop.set()
    rt.join()
    after = stats()
    check(not errors, "every EXEC answered (%s)" % errors[:2])
    check(not torn, "the reader never saw half an EXEC (%s)" % torn[:3])
    check(c.get("a") == str(4 * ROUNDS) and c.get("b") == str(4 * ROUNDS),
          "nothing lost: a and b are %d (%s %s)" % (4 * ROUNDS, c.get("a"), c.get("b")))
    print("    conflicts rerun: %d" % (after["tx_conflicts"] - before.get("tx_conflicts", 0)))

    print("an EXEC with a command a transaction doesn't take")
    got = multi(c, ("HSET", "h", "f", "v"), ("SET", "plain", "1"))
    check(len(got) == 2 and c.execute_command("HGET", "h", "f") == "v" and c.get("plain") == "1",
          "still runs (%s)" % got)

    print("MULTI/EXEC inside a BEGINTX")
    c.execute_command("BEGINTX")
    got = multi(c, ("SET", "in", "1"), ("INCR", "in"))
    check(list(got) == ["OK", 2], "answers (%s)" % got)
    check(client().get("in") is None, "isn't seen before COMMITTX")
    check(c.execute_command("COMMITTX") in ("OK", True) and client().get("in") == "2", "COMMITTX makes it")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
