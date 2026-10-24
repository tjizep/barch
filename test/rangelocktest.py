#!/usr/bin/env python3
"""
A range read over ordered shards holds only the shards it reads - TODO 703.

A range sharded space splits the key order between its shards, so a read of [lo, hi)
only needs the shards whose spans overlap it. They used to hold every shard of the
space, so a small RANGE waited on a write anywhere in it. Now RANGE, COUNT, MIN, MAX,
LB and UB take the shards they read, in shard order, and check the route again under
the locks.

Checked against one barchd, a range sharded space of 8 shards:
  - while a script holds a middle shard's write lock (barch.store.locked, kept busy
    until function_deadline_ms ends it), RANGE, RANGE LIMIT, COUNT, MIN, MAX, LB and UB
    over keys in other shards answer at once;
  - a RANGE over keys in the held shard does wait, so the hold is real;
  - while one client churns keys below the checked ones, so the rebalancer moves
    boundaries under them, the same reads over a fixed set of keys stay exact.

RANGE also reads the composite region (split keys, TODO 260), which sorts above every
string key and so belongs to the shard holding the top of the strings. That shard isn't
the one held here. A RANGE does wait on it, and should.
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
PORT = scale.port(default=14800)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "rangelock_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "rangelock.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "rlock" + str(int(time.time()))
KEYS = ["k%05d" % i for i in range(8000)]
HOLD_MS = 1500
# a read that holds nothing in the held shard answers well inside this, and one that
# waits for it takes most of HOLD_MS
QUICK = 0.4


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def shard_of(c, key):
    return int(c.execute_command("CALLF", "sn", key))


try:
    c = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    for k, v in ((".ordered", "1"), (".shards", "8"), (".range_sharded", "1")):
        c.execute_command("configuration:SET", SPACE + k, v)
    c.execute_command("CONFIG", "SET", "function_slice_insns", "4000000000")
    c.execute_command("CONFIG", "SET", "function_deadline_ms", str(HOLD_MS))
    c = client()
    for i in range(0, len(KEYS), 500):
        c.execute_command("MSET", *[x for k in KEYS[i:i + 500] for x in (k, "v")])
    c.execute_command("SETF", "sn", "function call(k) return barch.store.shardNumber(k) end\n")
    # busy in the region until the deadline ends the call: there's no clock to wait on
    c.execute_command("SETF", "hold", "function call(k) return barch.store.locked(k, function()"
                      " local n = 0 while true do n = n + 1 end return n end) end\n")

    print("the space")
    deadline = time.time() + 30
    while time.time() < deadline and not (shard_of(c, KEYS[0]) == 0 and shard_of(c, KEYS[-1]) == 7
                                           and shard_of(c, "k04000") in (3, 4)):
        time.sleep(0.2)
    HELD = "k04000"
    held = shard_of(c, HELD)
    check(shard_of(c, KEYS[0]) == 0 and shard_of(c, KEYS[-1]) == 7 and held in (3, 4),
          "balanced: k00000 in 0, k07999 in 7, k04000 in %d" % held)

    # stored functions sort above every key, so MAX is the top function's key
    top = c.execute_command("MAX")
    reads = [
        ("RANGE in shard 0", ("RANGE", "k00000", "k00100", 1000), KEYS[0:100]),
        ("RANGE LIMIT in shard 0", ("RANGE", "k00000", "k00100", "LIMIT", 10, 20), KEYS[10:30]),
        ("COUNT in shard 0", ("COUNT", "k00000", "k00100"), 100),
        ("MIN", ("MIN",), KEYS[0]),
        ("MAX", ("MAX",), top),
        ("LB in shard 0", ("LB", "k00050"), "k00050"),
        ("UB in shard 0", ("UB", "k00050"), "k00051"),
        ("COUNT in shard 7", ("COUNT", "k07500", "k07600"), 100),
    ]

    # one connection for every hold, made straight after the reader's. Each connection
    # is served by one of the server's RESP threads, given out in turn, and a locked
    # region runs on its connection's thread until it ends: a holder that landed on the
    # reader's thread would hold up the read without any lock being involved
    holding = client()

    def _hold(ev):
        try:
            holding.execute_command("CALLF", "hold", HELD)
        except redis.ResponseError:
            pass                    # FUNCTION timeout, which is how it ends
        ev.set()

    print("a write held in shard %d" % held)
    for what, cmd, want in reads:
        done = threading.Event()
        holder = threading.Thread(target=_hold, args=(done,))
        holder.start()
        time.sleep(0.2)
        t0 = time.time()
        got = c.execute_command(*cmd)
        took = time.time() - t0
        still = not done.is_set()
        holder.join()
        check(got == want and took < QUICK and still, "%s answers at once (%.2fs, %s)" % (what, took, str(got)[:30]))

    done = threading.Event()
    holder = threading.Thread(target=_hold, args=(done,))
    holder.start()
    time.sleep(0.2)
    t0 = time.time()
    got = c.execute_command("RANGE", "k04000", "k04010", 100)
    took = time.time() - t0
    holder.join()
    check(got == KEYS[4000:4010] and took >= QUICK, "RANGE in the held shard waits for it (%.2fs)" % took)

    print("boundaries moving under load")
    stop = threading.Event()
    churned = [0]

    def churn():
        w = client()
        n = 0
        while not stop.is_set():
            # a block of keys below every checked one, then gone again: the rebalancer
            # moves boundaries up through the shards and back
            batch = ["a%07d" % (n * 2000 + i) for i in range(2000)]
            w.execute_command("MSET", *[x for k in batch for x in (k, "v")])
            w.execute_command("DEL", *batch)
            n += 1
            churned[0] = n

    worker = threading.Thread(target=churn)
    worker.start()
    seen = {}
    wrong = []
    rounds = 0
    # at least the scaled window, then on until a boundary has moved: under TSan at a
    # small scale the window alone could end before the rebalancer moved one - TODO 747.
    # The cap is only reached when nothing ever moves, which is the failure
    started = time.time()
    enough = started + scale.scaled_seconds(20.0, floor=4.0)
    cap = started + max(90.0, scale.scaled_seconds(20.0, floor=4.0))
    moved = False
    try:
        while time.time() < cap and (time.time() < enough or not moved):
            for k in ("k01000", "k04000", "k06000"):
                seen.setdefault(k, set()).add(shard_of(c, k))
            moved = any(len(s) > 1 for s in seen.values())
            lo = (rounds * 397) % 7000
            want = KEYS[lo:lo + 1000]
            got = c.execute_command("RANGE", KEYS[lo], "k%05d" % (lo + 1000), 2000)
            if got != want:
                wrong.append(("RANGE", lo, len(got)))
            n = c.execute_command("COUNT", KEYS[lo], "k%05d" % (lo + 1000))
            if n != 1000:
                wrong.append(("COUNT", lo, n))
            if c.execute_command("LB", "k%05d" % lo) != KEYS[lo]:
                wrong.append(("LB", lo))
            if c.execute_command("UB", "k%05d" % lo) != KEYS[lo + 1]:
                wrong.append(("UB", lo))
            if c.execute_command("MAX") != top:
                wrong.append(("MAX",))
            rounds += 1
    finally:
        stop.set()
        worker.join()
    moved = any(len(s) > 1 for s in seen.values())
    check(moved, "the boundaries moved in %.1fs, %d churn rounds (%s)"
          % (time.time() - started, churned[0], {k: sorted(v) for k, v in seen.items()}))
    check(not wrong, "%d rounds of RANGE, COUNT, LB, UB and MAX stayed exact (%s)" % (rounds, wrong[:3]))
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
