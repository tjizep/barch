#!/usr/bin/env python3
"""
KEYS in a transaction with a writer waiting on the shard - TODO 825.

A glob in a transaction holds each shard for reading while it walks. The walk used to
hand its page reads to worker threads. Theirs were new read locks, so they waited behind
any writer in the queue, and the writer waited for the hold the walk kept. Nothing
moved until the writer gave up. On CI the writer was the save after a dictionary was
trained, which waits 60 s, and the transaction had expired by then.

BARCH_TEST_WALK_WRITER_MS puts a writer in the queue the first time a held walk starts,
so the order is certain here. It waits 30 s and the transaction may only live 5, so a
build with the stall refuses the GET after KEYS with TXABORTED. ABORTTX answers OK
either way, so it can't tell. The 30 s is the cap: a
fixed build is done long before it.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14798)
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


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


DATA = os.path.join(os.getcwd(), "txwalkwriter_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txwalkwriter.log"), "a")
env = dict(os.environ, BARCH_TEST_WALK_WRITER_MS="30000")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_max_age_ms=5000"],
                     stdout=log, stderr=log, env=env)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

try:
    space = "txw" + str(int(time.time()))
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    many = 200
    c.execute_command("MSET", *[x for i in range(many) for x in ("k%04d" % i, "v")])

    print("KEYS in a transaction, with a writer queued on the shard")
    c.execute_command("BEGINTX")
    got = answer(c, "KEYS", "k*")
    check(isinstance(got, list) and len(set(got)) == many,
          "every key comes back (%s of %d)" % (len(set(got)) if isinstance(got, list) else got, many))
    got = answer(c, "GET", "k0000")
    check(got == "v", "the transaction is still open after it (%s)" % got)
    answer(c, "ABORTTX")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
