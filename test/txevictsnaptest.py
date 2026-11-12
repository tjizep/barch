#!/usr/bin/env python3
"""
An open snapshot still reads a key that memory takes - TODO 886.

A write saves its key's before-image in the current version's layer, so a transaction
that began earlier reads the key as it was. An eviction didn't. A transaction saw a key
in its snapshot, the sweep took it for memory, and the next GET in the same transaction
answered nil.

Its own barchd, since it drops max memory far enough to take most of the space:
  - a transaction begins on a full space;
  - max memory comes down under what is held, and the LRU sweep takes keys;
  - outside the transaction some keys are gone;
  - inside it, every key reads as it was at BEGINTX.
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
PORT = scale.port(default=14837)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

DATA = os.path.join(os.getcwd(), "txevictsnap_data")
shutil.rmtree(DATA, ignore_errors=True)
os.makedirs(DATA)
SPACE = "es"
N = 20000
VALUE = "v" * 200

failures = 0


def check(ok, what):
    global failures
    print("  %-70s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, protocol=2, socket_timeout=120, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def stat(r, name):
    s = r.execute_command("STATS")
    for i in range(0, len(s) - 1, 2):
        if str(s[i]).lstrip("$") == name:
            return int(s[i + 1])
    raise AssertionError("no such stat: " + name)


def key(i):
    return "es%06d" % i


out = open(os.path.join(os.getcwd(), "txevictsnap.log"), "w")
proc = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                         "-c", "save_interval=86400000"], stdout=out, stderr=out)
try:
    end = time.time() + 60
    while True:
        if proc.poll() is not None:
            raise AssertionError("barchd exited with %s, see txevictsnap.log" % proc.returncode)
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            if time.time() > end:
                raise
            time.sleep(0.1)

    r = client()
    # a pipeline takes its own connection, which USE never reached, so its commands
    # name the space
    pipe = r.pipeline(transaction=False)
    for i in range(N):
        pipe.execute_command(SPACE + ":SET", key(i), VALUE)
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()
    size = r.execute_command("DBSIZE")
    check(size == N, "the space is filled (%s)" % size)

    print("a transaction begins, then the sweep takes keys")
    tx = client()
    tx.execute_command("BEGINTX")
    got = tx.execute_command("GET", key(0))
    check(got == VALUE, "the transaction reads a key (%s)" % str(got)[:40])

    r.execute_command("KSPACE", "OPTION", "SET", "LRU", "ON")
    held = stat(r, "logical_allocated")
    limit = r.execute_command("CONFIG", "GET", "max_memory_bytes")[1]
    r.execute_command("CONFIG", "SET", "max_memory_bytes", str(max(held // 2, 1 << 20)))
    # waits for the sweep to take something, not for an amount of time
    evicted0 = stat(r, "keys_evicted")
    deadline = time.time() + 60
    while time.time() < deadline and stat(r, "keys_evicted") == evicted0:
        time.sleep(0.2)
    took = stat(r, "keys_evicted") - evicted0
    check(took > 0, "the sweep evicted something (%d keys)" % took)
    # stop it, so what is gone outside stays gone while the transaction reads
    r.execute_command("KSPACE", "OPTION", "SET", "LRU", "OFF")
    # and the reads below aren't refused for memory
    r.execute_command("CONFIG", "SET", "max_memory_bytes", limit)

    pipe = r.pipeline(transaction=False)
    for i in range(N):
        pipe.execute_command(SPACE + ":EXISTS", key(i))
    gone = [i for i, v in enumerate(pipe.execute()) if not v]
    check(len(gone) > 0, "outside the transaction some keys are gone (%d)" % len(gone))

    lost = []
    for start in range(0, len(gone), 500):
        part = gone[start:start + 500]
        got = tx.execute_command("MGET", *[key(i) for i in part])
        lost += [i for i, v in zip(part, got) if v != VALUE]
    check(not lost, "inside it every evicted key reads as at BEGINTX (%d lost)" % len(lost))
    if gone:
        check(tx.execute_command("GET", key(gone[0])) == VALUE, "and through GET")
        check(tx.execute_command("EXISTS", key(gone[0])) == 1, "and EXISTS")
    tx.execute_command("ABORTTX")
    check(r.execute_command("PING") is True, "the server answers")
finally:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        scale.stop_hung(proc, signal.SIGTERM, 60)

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
