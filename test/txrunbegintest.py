#!/usr/bin/env python3
"""
A function's deadline running out just as barch.tx.run's BEGINTX opens - TODO 758.

run used to mark its transaction as its own (so a failed CALLF aborts it, TODO 743)
only after BEGINTX had returned. A timeout that fired in between left the transaction
open on the client's connection, and a pessimistic one kept its locks. Now run marks
the BEGINTX it's about to make, so it's run's from the moment it opens.

One barchd, whose BEGINTX takes longer than the function deadline
(BARCH_TEST_TX_BEGIN_PAUSE_MS), so the timeout fires right after BEGINTX every time:
  - the CALLF answers with the timeout, and wrote nothing;
  - the connection has no transaction open, and another can lock the key;
  - the next BEGINTX the client makes itself is still the client's: a CALLF that
    fails inside it leaves it open.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14823)
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


DEADLINE_MS = 300
PAUSE_MS = 600
DATA = os.path.join(os.getcwd(), "txrunbegin_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txrunbegin.log"), "w")
env = dict(os.environ, BARCH_TEST_TX_BEGIN_PAUSE_MS=str(PAUSE_MS))
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=1500"],
                     stdout=log, stderr=log, env=env)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txrunbegin_" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def run(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERROR " + str(e)


try:
    watch = client()
    check(run(watch, "SETF", "quick_set", """
function call(k)
    return barch.tx.run(function() barch.store.set(k, "run") return "done" end, true)
end""") == "OK", "SETF quick_set")
    check(run(watch, "SETF", "raises", """
function call()
    error("not today", 0)
end""") == "OK", "SETF raises")
    check(run(watch, "CONFIG", "SET", "function_deadline_ms", str(DEADLINE_MS)) == "OK", "a short deadline")

    print("the deadline runs out as run's BEGINTX returns")
    for attempt in range(3):
        c, other = client(), client()
        key = "k%d" % attempt
        r = run(c, "CALLF", "quick_set", key)
        check("FUNCTION timeout" in str(r), "it times out (%s)" % str(r)[:50])
        r = run(c, "ABORTTX")
        check("without BEGINTX" in str(r), "and leaves no transaction open (%s)" % str(r)[:50])
        run(other, "BEGINTX", "PESSIMISTIC")
        t0 = time.time()
        r = run(other, "SET", key, "other")
        took = time.time() - t0
        check(r is True and took < 1.0, "nor a lock on the key (%s, %.2f s)" % (str(r)[:50], took))
        run(other, "COMMITTX")
        check(watch.get(key) == "other", "and nothing of it was kept")
        c.close()
        other.close()

    print("the client's own BEGINTX after it")
    c = client()
    run(c, "CALLF", "quick_set", "first")
    check(run(c, "BEGINTX", "PESSIMISTIC") == "OK", "BEGINTX")
    run(c, "SET", "mine", "1")
    r = run(c, "CALLF", "raises")
    check(str(r).startswith("ERROR"), "a CALLF in it fails (%s)" % str(r)[:50])
    check(run(c, "GET", "mine") == "1" and run(c, "COMMITTX") == "OK",
          "and the client's transaction is still open, and commits")
    check(watch.get("mine") == "1", "with its write")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
