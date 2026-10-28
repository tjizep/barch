#!/usr/bin/env python3
"""
A transaction's overlay used again - TODO 711.

An overlay that's done with is emptied and kept, one a space, for the space's next
transaction, up to tx_overlay_pool_bytes of mapped pages; making one afresh maps and
unmaps arena pages, which was most of what a small transaction cost. So a transaction can
get an overlay another one used, and nothing of that one may show.

Checked against one barchd, a space without Raft, each check in a transaction that comes
right after another, so it gets the overlay that one left:
  - a key an aborted transaction wrote isn't there;
  - a key the last one read while it was missing, then set by another client, reads as
    set: the tomb the read left is gone;
  - a key the last one read, then changed by another client, reads as changed: the copy
    the read left is gone, and COMMITTX doesn't answer TXCONFLICT for it;
  - a committed transaction's write is made once, and the next transaction can delete it;
  - tx_overlay_pool_bytes is 41943040 to start, and with 0 transactions still work.

Each server thread keeps its own share of the pool, so threads don't queue for one lock
to take and give back an overlay - TODO 777. One thread's share is still there for the
others: tx_overlays_made in INFO transactions counts the overlays made because none was
kept, and it has to stay put when
  - one client runs transactions one after another;
  - eight clients, on eight different server threads, take turns;
  - eight have one open at once, after the first time;
and with seventy open at once, the 64 kept are all found again, so only six are made.
With 0 kept, most transactions make one.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14750)
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


DATA = os.path.join(os.getcwd(), "txreuse_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txreuse.log"), "a")
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
SPACE = "txu" + TAG


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


def made(c):
    return c.execute_command("INFO", "transactions")["tx_overlays_made"]


def one_tx(c, key):
    c.execute_command("BEGINTX")
    c.execute_command("SET", key, "v")
    return c.execute_command("COMMITTX")


def at_once(cs, label):
    # all open, then all committed: as many overlays out as clients
    for i, x in enumerate(cs):
        x.execute_command("BEGINTX")
        x.execute_command("SET", "%s%d" % (label, i), "v")
    return all(x.execute_command("COMMITTX") in ("OK", True) for x in cs)


def rounds(c, other, label):
    # a key an aborted transaction wrote
    c.execute_command("BEGINTX")
    c.execute_command("SET", "w" + label, "x")
    c.execute_command("ABORTTX")
    c.execute_command("BEGINTX")
    got = c.execute_command("GET", "w" + label)
    c.execute_command("ABORTTX")
    check(got is None, "%s: an aborted transaction's write isn't there (%r)" % (label, got))

    # a tomb a read left
    c.execute_command("BEGINTX")
    c.execute_command("GET", "t" + label)
    c.execute_command("ABORTTX")
    other.set("t" + label, "set")
    c.execute_command("BEGINTX")
    got = c.execute_command("GET", "t" + label)
    c.execute_command("ABORTTX")
    check(got == "set", "%s: a key read while missing, then set, reads as set (%r)" % (label, got))

    # a copy a read left
    other.set("c" + label, "1")
    c.execute_command("BEGINTX")
    c.execute_command("GET", "c" + label)
    c.execute_command("ABORTTX")
    other.set("c" + label, "2")
    c.execute_command("BEGINTX")
    got = c.execute_command("GET", "c" + label)
    c.execute_command("SET", "d" + label, got or "")
    done = answer(c, "COMMITTX")
    check(got == "2" and done in ("OK", True), "%s: a key read, then changed, reads as changed and commits (%r %r)"
          % (label, got, done))

    # a commit, then a delete in the next
    c.execute_command("BEGINTX")
    c.execute_command("INCR", "n" + label)
    check(answer(c, "COMMITTX") in ("OK", True) and other.get("n" + label) == "1",
          "%s: a committed INCR is made once" % label)
    c.execute_command("BEGINTX")
    c.execute_command("DEL", "n" + label)
    check(answer(c, "COMMITTX") in ("OK", True) and other.get("n" + label) is None,
          "%s: and the next transaction deletes it" % label)


try:
    c, other = client(), client()
    other.set("seed", "1")
    got = c.execute_command("CONFIG", "GET", "tx_overlay_pool_bytes")
    check("41943040" in str(got), "tx_overlay_pool_bytes is 41943040 to start (%s)" % got)

    print("with overlays kept")
    # warm: enough transactions over enough keys that the kept overlay has written most shards
    for i in range(64):
        c.execute_command("BEGINTX")
        c.execute_command("SET", "warm%d" % i, "v")
        c.execute_command("COMMITTX")
    for r in range(3):
        rounds(c, other, "k%d" % r)

    print("kept for any thread")
    m = made(c)
    ok = all(one_tx(c, "s%d" % i) in ("OK", True) for i in range(20))
    check(ok and made(c) == m, "20 one after another make none (%d)" % (made(c) - m))
    # the server hands connections to its threads in turn, so eight in a row are on eight
    eight = [client() for _ in range(8)]
    m = made(c)
    ok = all(one_tx(x, "e%d" % i) in ("OK", True) for i in range(10) for x in eight)
    check(ok and made(c) == m, "eight clients taking turns make none (%d)" % (made(c) - m))
    ok = at_once(eight, "a")
    m = made(c)
    ok = ok and all(at_once(eight, "a") for _ in range(3))
    check(ok and made(c) == m, "eight at once make none after the first time (%d)" % (made(c) - m))
    seventy = [client() for _ in range(70)]
    ok = at_once(seventy, "b")
    m = made(c)
    ok = ok and at_once(seventy, "b")
    check(ok and made(c) - m == 6, "seventy at once find the 64 kept and make 6 (%d)" % (made(c) - m))
    for x in seventy + eight:
        x.close()

    print("with none kept")
    check(c.execute_command("CONFIG", "SET", "tx_overlay_pool_bytes", "0") in ("OK", True), "CONFIG SET to 0")
    rounds(c, other, "z")
    m = made(c)
    ok = all(one_tx(c, "z%d" % i) in ("OK", True) for i in range(80))
    # the 64 already kept are handed out first, and then each makes one
    check(ok and made(c) - m >= 16, "80 transactions make at least 16 (%d)" % (made(c) - m))

    t = time.time()
    for i in range(300):
        c.execute_command("BEGINTX")
        c.execute_command("SET", "p%d" % i, "v")
        c.execute_command("COMMITTX")
    none_kept = time.time() - t
    c.execute_command("CONFIG", "SET", "tx_overlay_pool_bytes", "41943040")
    for i in range(64):
        c.execute_command("BEGINTX")
        c.execute_command("SET", "q%d" % i, "v")
        c.execute_command("COMMITTX")
    t = time.time()
    for i in range(300):
        c.execute_command("BEGINTX")
        c.execute_command("SET", "p%d" % i, "w")
        c.execute_command("COMMITTX")
    kept = time.time() - t
    print("    300 transactions: %.0f us each with none kept, %.0f us kept" % (none_kept * 1e6 / 300, kept * 1e6 / 300))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
