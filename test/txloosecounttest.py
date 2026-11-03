#!/usr/bin/env python3
"""
barch.store.looseCount stays out of the transaction - TODO 810.

A query planner only wants a rough idea of what a range holds, so it shouldn't lock the
range or be on the hook for it at commit. Each check runs next to barch.store.count,
which does both, so a pass can't just mean the setup stopped showing the difference.

One barchd, tx_lock_wait_ms 300:
  - pessimistic: a plain SET into the range goes through while the transaction is open
    after looseCount, and times out after count;
  - pessimistic: looseCount gets past another transaction's write lock inside the range,
    where count times out;
  - optimistic: another client's write into the range doesn't fail COMMITTX after
    looseCount, and does after count;
  - it counts the space: it leaves out the transaction's own writes, which count sees,
    and takes in another client's, which count doesn't;
  - a space handle's looseCount does the same, and outside a transaction it's count.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14829)
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


DATA = os.path.join(os.getcwd(), "txloosecount_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txloosecount.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=300"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txloose_" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


FUNCTIONS = {
    "cnt": "function call(lo, hi) return barch.store.count(lo, hi) end\n",
    "loose": "function call(lo, hi) return barch.store.looseCount(lo, hi) end\n",
    "hloose": "function call(lo, hi) return barch.space['%s']:looseCount(lo, hi) end\n" % SPACE,
}

try:
    c = client()
    other = client()
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)
    for i in range(10):
        c.set("k%02d" % i, "v")

    for fn in ("cnt", "loose", "hloose"):
        check(answer(c, "CALLF", fn, "k", "l") == 10, "%s counts 10 outside a transaction" % fn)

    # a plain write into a range the transaction counted
    for fn, passes in (("loose", True), ("hloose", True), ("cnt", False)):
        answer(c, "BEGINTX", "PESSIMISTIC")
        check(answer(c, "CALLF", fn, "k", "l") == 10, "%s in a pessimistic transaction" % fn)
        got = answer(other, "SET", "k05", "changed")
        check((got is True) == passes, "SET into the range after %s (%s)" % (fn, str(got)[:50]))
        answer(c, "ABORTTX")

    # another transaction's write lock inside the range
    for fn, passes in (("loose", True), ("cnt", False)):
        answer(other, "BEGINTX", "PESSIMISTIC")
        answer(other, "SET", "k05", "locked")
        answer(c, "BEGINTX", "PESSIMISTIC")
        got = answer(c, "CALLF", fn, "k", "l")
        check((got == 10) == passes, "%s with k05 write locked (%s)" % (fn, str(got)[:50]))
        answer(c, "ABORTTX")
        answer(other, "ABORTTX")

    # a commit after another client wrote into the range
    for i, (fn, passes) in enumerate((("loose", True), ("cnt", False))):
        answer(c, "BEGINTX")
        answer(c, "CALLF", fn, "k", "l")
        answer(c, "SET", "outside%d" % i, "v")
        other.set("k1%d" % i, "new")
        got = answer(c, "COMMITTX")
        check((got == "OK") == passes, "COMMITTX after %s and a write (%s)" % (fn, str(got)[:50]))
        other.delete("k1%d" % i)

    # what each one sees
    for kind in ((), ("PESSIMISTIC",)):
        answer(c, "BEGINTX", *kind)
        answer(c, "SET", "k20", "mine")
        check(answer(c, "CALLF", "loose", "k", "l") == 10, "looseCount leaves out the transaction's write")
        check(answer(c, "CALLF", "cnt", "k", "l") == 11, "count takes it in")
        answer(c, "ABORTTX")
    answer(c, "BEGINTX")
    check(answer(c, "CALLF", "cnt", "k", "l") == 10, "count before another client's write")
    other.set("k21", "theirs")
    check(answer(c, "CALLF", "loose", "k", "l") == 11, "looseCount takes in another client's write")
    check(answer(c, "CALLF", "cnt", "k", "l") == 10, "count keeps its snapshot")
    answer(c, "ABORTTX")
finally:
    p.terminate()
    p.wait(timeout=30)
    log.close()

print("FAILED: %d" % failures if failures else "OK")
sys.exit(1 if failures else 0)
