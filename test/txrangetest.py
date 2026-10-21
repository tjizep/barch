#!/usr/bin/env python3
"""
RANGE, COUNT, KEYS and SCAN inside a transaction - TODO 701.

A walk in a transaction merges, for each shard, its overlay, the layers of its snapshot
(oldest first) and the space, so it sees its own writes over the store as it was at
BEGINTX. Each range it walks is kept, and COMMITTX answers TXCONFLICT if a key has been
written into it, or out of it, since the snapshot: a phantom.

Checked against one barchd, a space without Raft:
  - RANGE and COUNT in a transaction see the keys as they were at BEGINTX, after another
    client adds one in the range and deletes another;
  - a transaction that walked that range and writes answers TXCONFLICT, and so does one
    whose COUNT's range only gained a key that wasn't there before;
  - one that walked a range nothing was written into commits, though a key outside it
    was;
  - a transaction's own SET and DEL show in its RANGE and COUNT, and COMMITTX makes them;
  - KEYS and SCAN see the keys as at BEGINTX, and its own writes, and one that walked
    them and writes answers TXCONFLICT;
  - one that only walks commits, whatever changed.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14770)
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


DATA = os.path.join(os.getcwd(), "txrange_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txrange.log"), "a")
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
SPACE = "txg" + TAG
FIVE = ["r1", "r2", "r3", "r4", "r5"]


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def reset(c):
    c.execute_command("DEL", "r1", "r2", "r2x", "r3", "r4", "r5", "r6", "w", "s1")
    c.execute_command("MSET", *[x for k in FIVE for x in (k, "v")])


try:
    tx, other = client(), client()
    reset(other)

    print("a walk as of BEGINTX")
    tx.execute_command("BEGINTX")
    first = tx.execute_command("RANGE", "r0", "r9")
    other.execute_command("SET", "r2x", "new")
    other.execute_command("DEL", "r4")
    again = tx.execute_command("RANGE", "r0", "r9")
    count = tx.execute_command("COUNT", "r0", "r9")
    check(list(first) == FIVE and list(again) == FIVE,
          "RANGE before and after another client's writes is the same (%s, %s)" % (first, again))
    check(count == 5, "and COUNT (%s)" % count)
    tx.execute_command("SET", "w", "1")
    got = answer(tx, "COMMITTX")
    check("TXCONFLICT" in str(got) and other.get("w") is None,
          "one that walked the range and writes answers TXCONFLICT (%s)" % str(got)[:50])

    print("only a new key in the range")
    reset(other)
    tx.execute_command("BEGINTX")
    tx.execute_command("COUNT", "r0", "r9")
    other.execute_command("SET", "r2x", "new")
    tx.execute_command("SET", "w", "1")
    got = answer(tx, "COMMITTX")
    check("TXCONFLICT" in str(got), "a key that wasn't there, added to a range COUNT walked, is a conflict (%s)"
          % str(got)[:50])

    print("a write outside the range walked")
    reset(other)
    tx.execute_command("BEGINTX")
    tx.execute_command("RANGE", "r0", "r9")
    other.execute_command("SET", "s1", "outside")
    tx.execute_command("SET", "w", "1")
    got = answer(tx, "COMMITTX")
    check(got in ("OK", True) and other.get("w") == "1", "commits (%s)" % got)

    print("its own writes")
    reset(other)
    tx.execute_command("BEGINTX")
    tx.execute_command("SET", "r6", "mine")
    tx.execute_command("DEL", "r1")
    got = tx.execute_command("RANGE", "r0", "r9")
    count = tx.execute_command("COUNT", "r0", "r9")
    check(list(got) == ["r2", "r3", "r4", "r5", "r6"] and count == 5,
          "its SET and DEL show in its RANGE and COUNT (%s, %s)" % (got, count))
    check(list(other.execute_command("RANGE", "r0", "r9")) == FIVE, "and nowhere else before COMMITTX")
    got = answer(tx, "COMMITTX")
    check(got in ("OK", True) and list(other.execute_command("RANGE", "r0", "r9")) == ["r2", "r3", "r4", "r5", "r6"],
          "COMMITTX makes them (%s)" % got)

    print("KEYS and SCAN")

    def scan_all(c, pattern):
        cursor, out = "0", set()
        for _ in range(1000):
            cursor, keys = c.execute_command("SCAN", cursor, "MATCH", pattern, "COUNT", 100)
            out.update(keys)
            if str(cursor) == "0":
                break
        return sorted(out)

    reset(other)
    tx.execute_command("BEGINTX")
    first = sorted(tx.execute_command("KEYS", "r*"))
    other.execute_command("SET", "r2x", "new")
    other.execute_command("DEL", "r4")
    again = sorted(tx.execute_command("KEYS", "r*"))
    scanned = scan_all(tx, "r*")
    check(first == FIVE and again == FIVE and scanned == FIVE,
          "KEYS and SCAN see the keys as at BEGINTX (%s, %s, %s)" % (first, again, scanned))
    tx.execute_command("SET", "w", "1")
    got = answer(tx, "COMMITTX")
    check("TXCONFLICT" in str(got), "and one that writes answers TXCONFLICT (%s)" % str(got)[:50])
    reset(other)
    tx.execute_command("BEGINTX")
    tx.execute_command("SET", "r6", "mine")
    tx.execute_command("DEL", "r1")
    got = [sorted(tx.execute_command("KEYS", "r*")), scan_all(tx, "r*")]
    tx.execute_command("ABORTTX")
    want = ["r2", "r3", "r4", "r5", "r6"]
    check(got == [want, want], "they show its own SET and DEL (%s)" % got)

    print("a transaction that only walks")
    reset(other)
    tx.execute_command("BEGINTX")
    tx.execute_command("RANGE", "r0", "r9")
    other.execute_command("DEL", "r3")
    got = answer(tx, "COMMITTX")
    check(got in ("OK", True), "commits, whatever changed (%s)" % got)
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
