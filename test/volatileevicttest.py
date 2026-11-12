#!/usr/bin/env python3
"""
The volatile eviction policies take only keys with an expiry - TODO 893.

volatile-lru took keys with no expiry too, volatile-random took nothing, and
CONFIG SET refused volatile-ttl. The LRU sweeps they ran read a page source
that was a stub, so only the random page walks ever evicted.

Checked against one barchd. Each policy gets a space of its own with one shard,
made after CONFIG SET maxmemory-policy so it starts with that policy:
  - allkeys-lru and allkeys-random take every key;
  - volatile-lru, volatile-random and volatile-ttl take every key with an
    expiry and leave every key without one;
  - volatile-ttl takes the nearer expiries first. The keys fit on one page, and
    each pass takes the half of that page that expires soonest, so at any point
    every key taken expires before every key left. That's checked each time
    the count is seen between full and done.

The spaces under test hold about 24 KB each, and the count max_memory goes by is
only right to within 16 KB per thread (TODO 868). So a ballast space, made
before any policy is set and so never evicted, holds 4 MB the whole time. With
it the count stays over the limit until the last key under test has gone.
Without it the count could read 0 with keys still there, and eviction stopped
for good - TODO 897.
"""
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=19720)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "volatileevict_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)

N = 400                     # keys of each kind: small enough for one page
BALLAST, BALLAST_VALUE = 256, 16384     # 4 MB that no policy takes - TODO 897
HIGH = str(1 << 34)
failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def left(r, space, prefix):
    p = r.pipeline(transaction=False)
    for i in range(N):
        p.execute_command(space + ":EXISTS", "%s%d" % (prefix, i))
    return [i for i, e in enumerate(p.execute()) if e]


out = open(os.path.join(os.getcwd(), "volatileevict.log"), "w")
proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                         "-c", "save_interval=86400000"], stdout=out, stderr=out)
try:
    end = time.time() + 60
    while True:
        if proc.poll() is not None:
            raise AssertionError("barchd exited with %s, see volatileevict.log" % proc.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            if time.time() > end:
                raise
            time.sleep(0.1)
    r = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=60)
    # a server already on the port answers the connect, and this one exits. Only
    # barchd knows SPACES
    try:
        r.execute_command("SPACES")
    except redis.exceptions.ResponseError:
        raise AssertionError("something other than this barchd is on port %d" % PORT)

    # before the first policy is set: a space keeps the policy it was made with
    p = r.pipeline(transaction=False)
    for i in range(BALLAST):
        p.execute_command("ev_ballast:SET", "b%d" % i, "x" * BALLAST_VALUE)
    p.execute()

    # the expiries in an order unrelated to the order written, so the page holds
    # near and far ones side by side
    ttl = list(range(N))
    random.Random(893).shuffle(ttl)

    for policy in ("allkeys-lru", "allkeys-random", "volatile-lru", "volatile-random", "volatile-ttl"):
        print(policy, flush=True)
        space = "ev_" + policy.replace("-", "_")
        r.execute_command("CONFIG", "SET", "maxmemory", HIGH)
        try:
            r.execute_command("CONFIG", "SET", "maxmemory-policy", policy)
            accepted = True
        except redis.exceptions.ResponseError:
            accepted = False
        check(accepted, "CONFIG SET maxmemory-policy %s is accepted" % policy)
        if not accepted:
            continue
        r.execute_command("configuration:SET", space + ".shards", "1")
        for i in range(N):
            r.execute_command(space + ":SET", "p%d" % i, "x" * 16)
            r.execute_command(space + ":SET", "v%d" % i, "x" * 16, "EX", 100000 + ttl[i])
        volatile = policy.startswith("volatile")
        done = N if volatile else 0
        r.execute_command("CONFIG", "SET", "maxmemory", "4096")
        midway, ordered = 0, True
        # watched by DBSIZE, which reads no key and so stamps no LRU bit - TODO 892
        deadline = time.time() + 60
        while time.time() < deadline:
            size = r.execute_command(space + ":DBSIZE")
            if size <= done:
                break
            if policy == "volatile-ttl" and size < 2 * N:
                # held still while the keys are read
                r.execute_command("CONFIG", "SET", "maxmemory", HIGH)
                kept = left(r, space, "v")
                if kept:
                    midway += 1
                    taken = sorted(set(range(N)) - set(kept))
                    if taken and max(ttl[i] for i in taken) > min(ttl[i] for i in kept):
                        ordered = False
                r.execute_command("CONFIG", "SET", "maxmemory", "4096")
            time.sleep(0.05)
        r.execute_command("CONFIG", "SET", "maxmemory", HIGH)
        p_left, v_left = len(left(r, space, "p")), len(left(r, space, "v"))
        if volatile:
            check(p_left == N, "every key without an expiry is left (%d of %d)" % (p_left, N))
        else:
            check(p_left == 0, "every key without an expiry is taken (%d left)" % p_left)
        check(v_left == 0, "every key with an expiry is taken (%d left)" % v_left)
        if policy == "volatile-ttl":
            check(midway > 0, "seen part way through (%d times)" % midway)
            check(ordered, "and each time, every key taken expires before every key left")

    kept = r.execute_command("ev_ballast:DBSIZE")
    check(kept == BALLAST, "the ballast is all there (%d of %d)" % (kept, BALLAST))
    check(r.execute_command("PING") is True, "the server answers")
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        scale.stop_hung(proc, signal.SIGTERM, 60)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
