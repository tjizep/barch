#!/usr/bin/env python3
"""
DEL of a list takes its header too - TODO 891.

A list keeps a header key beside its items. DEL took the items and left the header, so
after RPUSH l a b; DEL l, LLEN l still answered 2. Redis answers 0.

Checked against one barchd, each case in a space of its own:
  - DEL after RPUSH: LLEN, LRANGE, EXISTS, KEYS and DBSIZE all see nothing, and a new
    RPUSH starts from one;
  - DEL after a pop, and DEL of several lists at once;
  - DEL inside a transaction of a list made before it, seen inside and after COMMITTX.
"""
import os
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14891)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "listdel_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)

failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def client(space):
    c = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    return c


def gone(r, name, what):
    check(r.llen(name) == 0, "%s: LLEN is 0 (%s)" % (what, r.llen(name)))
    check(r.lrange(name, 0, -1) == [], "%s: LRANGE is empty" % what)
    check(r.exists(name) == 0, "%s: EXISTS is 0" % what)


out = open(os.path.join(os.getcwd(), "listdel.log"), "w")
proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                         "-c", "save_interval=86400000"], stdout=out, stderr=out)
try:
    end = time.time() + 60
    while True:
        if proc.poll() is not None:
            raise AssertionError("barchd exited with %s, see listdel.log" % proc.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            if time.time() > end:
                raise
            time.sleep(0.1)

    print("DEL after RPUSH")
    r = client("ld1")
    r.rpush("l", "a", "b")
    check(r.delete("l") == 1, "DEL answers 1")
    gone(r, "l", "after DEL")
    check(r.dbsize() == 0, "DBSIZE is 0 (%s)" % r.dbsize())
    check(r.execute_command("KEYS", "*") == [], "KEYS * is empty")
    check(r.delete("l") == 0, "a second DEL answers 0")
    check(r.rpush("l", "x") == 1, "a new RPUSH answers 1")
    check(r.lrange("l", 0, -1) == ["x"], "and the list holds only the new item")
    # the list has an item, so this answers at once. Over the old header it took an
    # empty slot from the front
    got = r.blpop(["l"], 0)
    check(got is not None and list(got) == ["l", "x"], "BLPOP answers the new item (%s)" % (got,))
    check(r.llen("l") == 0, "and leaves the list empty")

    print("DEL after a pop")
    r = client("ld2")
    r.lpush("l", "a", "b", "c")
    r.lpop("l")
    check(r.delete("l") == 1, "DEL answers 1")
    gone(r, "l", "after pop and DEL")
    check(r.dbsize() == 0, "DBSIZE is 0 (%s)" % r.dbsize())

    print("DEL of several lists")
    r = client("ld3")
    for n in ("p", "q", "s", "t"):
        r.rpush(n, "a", "b")
    check(r.delete("p", "q", "s", "t") == 4, "DEL of four lists answers 4")
    check(r.dbsize() == 0, "DBSIZE is 0 (%s)" % r.dbsize())

    print("DEL in a transaction of a list made before it")
    r = client("ld4")
    r.rpush("l", "a", "b")
    tx = client("ld4")
    tx.execute_command("BEGINTX")
    check(tx.delete("l") == 1, "DEL in the transaction answers 1")
    # list reads aren't taken in a transaction, so inside it only EXISTS and KEYS look
    check(tx.exists("l") == 0, "inside the transaction: EXISTS is 0")
    check(tx.execute_command("KEYS", "*") == [], "inside the transaction: KEYS * is empty")
    check(r.llen("l") == 2, "outside, before COMMITTX, the list is whole (%s)" % r.llen("l"))
    tx.execute_command("COMMITTX")
    gone(r, "l", "after COMMITTX")
    check(r.dbsize() == 0, "DBSIZE is 0 (%s)" % r.dbsize())
    check(r.rpush("l", "x") == 1, "a new RPUSH answers 1")

    check(r.execute_command("PING") is True, "the server answers")
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        scale.stop_hung(proc, signal.SIGTERM, 60)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
