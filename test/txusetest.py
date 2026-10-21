#!/usr/bin/env python3
"""
COMMITTX and a USE of a new space at the same time - TODO 714.

COMMITTX held the space's tx_gate alone while it made the writes, and each write looked
its space up by name, which takes the keyspace registry lock. A USE of a space that
doesn't exist yet holds that registry lock while it builds the space, and building one
runs AUTH through an internal caller, which takes the default space's tx_gate shared
once a transaction has run there. So the two could wait on each other for good. TSan
reported it as a lock-order inversion in TestRespClientLocal. The same went for any
read that takes the gate and looks a space up, SPACES say: building a space no longer
takes a gate under the registry lock.

Checked against one barchd: one client commits transactions in the default space, with
many writes each so the gate is held a while, while another keeps USE-ing spaces that
don't exist yet and a third lists the spaces from the default space. All three have to
finish, every COMMITTX answering OK, and the keys of the last transaction have to be
there. The SPACES half only hangs where a waiting writer holds readers back, so it's
mostly for a TSan run to check the lock order.
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
PORT = scale.port(default=14714)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROUNDS = scale.scaled(400, floor=20)
WRITES = 50
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


DATA = os.path.join(os.getcwd(), "txuse_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txuse.log"), "a")
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


def client():
    # a hang is what's being looked for, so it shows up as a timeout, not a stuck test
    return redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=20, decode_responses=True,
                       single_connection_client=True)


results = {"commits": 0, "commit_errors": [], "uses": 0, "use_errors": [], "lists": 0, "list_errors": []}


def committing():
    c = client()
    c.execute_command("USE", "0")
    try:
        for i in range(ROUNDS):
            c.execute_command("BEGINTX")
            for j in range(WRITES):
                c.execute_command("SET", "tu%s:%d" % (TAG, j), str(i))
            got = c.execute_command("COMMITTX")
            if got != "OK":
                results["commit_errors"].append(str(got))
            results["commits"] += 1
    except Exception as e:
        results["commit_errors"].append(repr(e))


def using():
    c = client()
    try:
        for i in range(ROUNDS * 2):
            c.execute_command("USE", "tu%s_%d" % (TAG, i))
            results["uses"] += 1
    except Exception as e:
        results["use_errors"].append(repr(e))


def listing():
    c = client()
    c.execute_command("USE", "0")
    try:
        for i in range(ROUNDS):
            c.execute_command("SPACES")
            results["lists"] += 1
    except Exception as e:
        results["list_errors"].append(repr(e))


try:
    print("COMMITTX in the default space while new spaces are made (%d rounds)" % ROUNDS)
    threads = [threading.Thread(target=t) for t in (committing, using, listing)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(300)
    check(not any(t.is_alive() for t in threads), "all three finish")
    check(results["commits"] == ROUNDS and not results["commit_errors"],
          "every COMMITTX answers OK (%d, %s)" % (results["commits"], results["commit_errors"][:2]))
    check(results["uses"] == ROUNDS * 2 and not results["use_errors"],
          "every USE answers (%d, %s)" % (results["uses"], results["use_errors"][:2]))
    check(results["lists"] == ROUNDS and not results["list_errors"],
          "every SPACES answers (%d, %s)" % (results["lists"], results["list_errors"][:2]))
    try:
        c = client()
        c.execute_command("USE", "0")
        last = [c.get("tu%s:0" % TAG), c.get("tu%s:%d" % (TAG, WRITES - 1))]
    except redis.RedisError as e:
        last = [repr(e)]               # a deadlocked server answers nobody
    check(last == [str(ROUNDS - 1)] * 2, "the last transaction's writes are there (%s)" % last[:1])
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
