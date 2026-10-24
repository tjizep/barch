#!/usr/bin/env python3
"""
Range locks for walks in a pessimistic transaction - TODO 749.

RANGE, COUNT, KEYS and SCAN in a BEGINTX PESSIMISTIC lock the range they walk, shared,
until the transaction ends. A write into it by anyone else waits, and a walk waits for
another transaction's write lock on a key inside it.

Checked against one barchd with no Raft, tx_lock_wait_ms 1500:
  - a plain SET into a walked range waits for COMMITTX, and one outside it doesn't;
  - the walk sees the same keys when run again: nothing slips in (no phantom);
  - a plain SET that waits too long gets TXLOCKTIMEOUT;
  - another pessimistic transaction's SET into it waits, and goes on after;
  - an optimistic transaction writing into it gets TXCONFLICT;
  - a walk waits for another transaction's write lock inside its range, then sees it;
  - two walking transactions each writing into the other's range: one TXDEADLOCK;
  - COUNT locks its range, KEYS and SCAN lock every key;
  - a stored function writing into it is refused at the shard, TXLOCKED;
  - ABORTTX lets the range go too, and several parked writers all go on.
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


WAIT_MS = 1500
# how long a write is given to show it's waiting: well under WAIT_MS
HELD = 0.4
DATA = os.path.join(os.getcwd(), "txrangelock_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txrangelock.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000", "-c", "tx_lock_wait_ms=%d" % WAIT_MS],
                     stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txrange_" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=60, decode_responses=True,
                    single_connection_client=True, retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0))
    c.execute_command("USE", SPACE)
    return c


def run(c, *cmd):
    """the answer, or the error's text"""
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERROR " + str(e)


def later(c, out, name, *cmd):
    """`cmd` on its own thread, its answer and when it came in out[name]"""
    def go():
        out[name] = run(c, *cmd)
        out[name + "_at"] = time.time()
    t = threading.Thread(target=go)
    t.start()
    return t


def waiting(t, out, name):
    """still waiting after HELD"""
    t.join(HELD)
    return t.is_alive() and name not in out


try:
    watch = client()
    for k in ("b1", "b5", "c2", "c8", "x1"):
        watch.set(k, "0")

    print("a plain write into a walked range")
    a, plain, other = client(), client(), client()
    run(a, "BEGINTX", "PESSIMISTIC")
    first = run(a, "RANGE", "b", "d")
    check(first == ["b1", "b5", "c2", "c8"], "RANGE answers (%s)" % first)
    t0 = time.time()
    check(run(other, "SET", "x2", "1") is True and time.time() - t0 < HELD, "a SET outside it doesn't wait")
    out = {}
    t = later(plain, out, "set", "SET", "c5", "plain")
    check(waiting(t, out, "set"), "a SET into it waits")
    again = run(a, "RANGE", "b", "d")
    check(again == first, "the walk sees the same keys (%s)" % again)
    check(run(a, "COUNT", "b", "d") == 4, "and counts the same")
    committed = time.time()
    check(run(a, "COMMITTX") == "OK", "COMMITTX")
    t.join(10)
    check(out.get("set") is True and out.get("set_at", 0) >= committed, "then the SET goes on (%s)" % out.get("set"))
    check(run(watch, "GET", "c5") == "plain", "and stands")

    print("waited for too long")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "RANGE", "b", "d")
    t0 = time.time()
    got = run(plain, "SET", "c6", "late")
    took = time.time() - t0
    check(str(got).startswith("ERROR TXLOCKTIMEOUT") and took >= WAIT_MS / 1000.0 * 0.9,
          "TXLOCKTIMEOUT after tx_lock_wait_ms (%.1fs, %s)" % (took, str(got)[:70]))
    check(run(a, "ABORTTX") == "OK", "ABORTTX")
    t0 = time.time()
    check(run(plain, "SET", "c6", "now") is True and time.time() - t0 < HELD, "which lets the range go")

    print("another pessimistic transaction")
    b = client()
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "RANGE", "b", "d")
    run(b, "BEGINTX", "PESSIMISTIC")
    out = {}
    t = later(b, out, "set", "SET", "b7", "tx")
    check(waiting(t, out, "set"), "its SET into the range waits")
    check(run(a, "COMMITTX") == "OK", "the walker commits")
    t.join(10)
    check(out.get("set") is True, "then the SET goes on (%s)" % out.get("set"))
    check(run(b, "COMMITTX") == "OK" and run(watch, "GET", "b7") == "tx", "and commits")

    print("an optimistic transaction")
    o = client()
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "RANGE", "b", "d")
    run(o, "BEGINTX")
    run(o, "SET", "c9", "opt")
    got = run(o, "COMMITTX")
    check(str(got).startswith("ERROR TXCONFLICT"), "TXCONFLICT writing into the range (%s)" % str(got)[:70])
    run(o, "BEGINTX")
    run(o, "SET", "x3", "opt")
    check(run(o, "COMMITTX") == "OK", "outside it, it commits")
    check(run(a, "COMMITTX") == "OK", "the walker commits")

    print("a walk waits for a write lock inside its range")
    run(b, "BEGINTX", "PESSIMISTIC")
    run(b, "SET", "c4", "new")
    run(a, "BEGINTX", "PESSIMISTIC")
    out = {}
    t = later(a, out, "range", "RANGE", "b", "d")
    check(waiting(t, out, "range"), "RANGE waits for the writer")
    check(run(b, "COMMITTX") == "OK", "the writer commits")
    t.join(10)
    check(isinstance(out.get("range"), list) and "c4" in out["range"], "then RANGE sees its key (%s)" % out.get("range"))
    check(run(a, "COMMITTX") == "OK", "COMMITTX")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(b, "BEGINTX", "PESSIMISTIC")
    run(b, "SET", "x4", "new")
    t0 = time.time()
    got = run(a, "RANGE", "b", "d")
    check(isinstance(got, list) and time.time() - t0 < HELD, "a write lock outside it doesn't hold it up")
    run(a, "ABORTTX")
    run(b, "ABORTTX")

    print("a ring through ranges")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(b, "BEGINTX", "PESSIMISTIC")
    run(a, "RANGE", "b", "c")
    run(b, "RANGE", "c", "d")
    out = {}
    ta = later(a, out, "a", "SET", "c3", "a")
    time.sleep(0.1)
    tb = later(b, out, "b", "SET", "b2", "b")
    ta.join(10)
    tb.join(10)
    said = [str(out.get("a")), str(out.get("b"))]
    dead = [s for s in said if "TXDEADLOCK" in s]
    check(len(dead) == 1 and "True" in said, "one TXDEADLOCK and one goes on (%s)" % said)
    run(a, "ABORTTX")
    run(b, "ABORTTX")

    print("COUNT, KEYS and SCAN")
    for cmd, inside, outside in ((["COUNT", "b", "d"], "c1", "y1"),
                                 (["KEYS", "*"], "y2", None),
                                 (["SCAN", "0"], "y3", None)):
        run(a, "BEGINTX", "PESSIMISTIC")
        got = run(a, *cmd)
        check(not str(got).startswith("ERROR"), "%s answers (%s)" % (cmd[0], str(got)[:40]))
        if outside:
            t0 = time.time()
            check(run(other, "SET", outside, "1") is True and time.time() - t0 < HELD, "  a SET outside it doesn't wait")
        out = {}
        t = later(plain, out, "set", "SET", inside, "1")
        check(waiting(t, out, "set"), "  a SET into what %s read waits" % cmd[0])
        run(a, "COMMITTX")
        t.join(10)
        check(out.get("set") is True, "  then goes on (%s)" % out.get("set"))

    print("a stored function's write")
    run(watch, "SETF", "stamp", "function call() barch.store.set('c7', 'fn') return 1 end")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "RANGE", "b", "d")
    got = run(other, "CALLF", "stamp")
    check(str(got).startswith("ERROR") and "TXLOCKED" in got, "refused at the shard (%s)" % str(got)[:70])
    run(a, "COMMITTX")
    check(run(other, "CALLF", "stamp") == 1, "then it writes")

    print("several parked writers")
    run(a, "BEGINTX", "PESSIMISTIC")
    run(a, "KEYS", "*")
    writers = [client() for _ in range(4)]
    out = {}
    ts = [later(w, out, "w%d" % i, "SET", "p%d" % i, "1") for i, w in enumerate(writers)]
    time.sleep(HELD)
    check(not any(("w%d" % i) in out for i in range(4)), "all wait")
    run(a, "ABORTTX")
    for t in ts:
        t.join(10)
    check(all(out.get("w%d" % i) is True for i in range(4)), "and all go on after ABORTTX (%s)" % out)
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
