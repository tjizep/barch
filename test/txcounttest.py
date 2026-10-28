#!/usr/bin/env python3
"""
Counts and offsets inside a transaction, without walking every key - TODO 765.

A transaction reads through an overlay that sits on the space, so barch.store.count
and a range with an offset used to walk the whole range key by key. lent saw about
8-10 ms against 1-2 ms over 20,000 keys, which every `desc` page and offset paid.
Now the space's shard is counted from its node counts and only the keys the levels
above it hold (the transaction's writes, and the snapshot's copies of what other
clients changed since BEGINTX) are looked at one by one.

One barchd, 20,000 keys:
  - inside a transaction that writes, overwrites, deletes, deletes what isn't there,
    adds then deletes, and deletes then adds again, while another client adds,
    deletes and changes keys in the same range after BEGINTX: count and every offset
    tried agree with what the transaction should see, through a script and through
    COUNT;
  - an offset past the end answers nothing;
  - a count over all 20,000 keys and an offset near the end take about as long as
    ones over a few hundred keys, where they used to grow with the range. The near
    offset is 300, past the 256 that are stepped over instead of searched for, so
    both are searched - TODO 770.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14827)
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


DATA = os.path.join(os.getcwd(), "txcount_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txcount.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txcount_" + str(int(time.time()))
N = 20000


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
    "page": """
function call(lo, hi, n, offset)
    return table.concat(barch.store.range(lo, hi, tonumber(n), tonumber(offset)), ",")
end""",
}


def key(i):
    return "c%05d" % i


def median_ms(c, *cmd, runs=15):
    took = []
    for _ in range(runs):
        t = time.perf_counter()
        c.execute_command(*cmd)
        took.append((time.perf_counter() - t) * 1000)
    took.sort()
    return took[len(took) // 2]


try:
    c = client()
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)
    # MSET on this connection: a pipeline would take another one, without the USE
    for at in range(0, N, 1000):
        c.mset({key(i): "v" for i in range(at, min(N, at + 1000))})
    c.mset({"a": "below", "d": "above"})
    check(answer(c, "CALLF", "cnt", "c", "d") == N, "%d keys outside a transaction" % N)

    # what the transaction should see, kept alongside
    seen = set(key(i) for i in range(N))
    c.execute_command("BEGINTX")
    other = client()
    other.set(key(5), "changed")              # a value only: still seen, as it was
    other.delete(key(6))                      # still seen: the snapshot has it
    other.set("c05000x", "added")             # not seen: came after BEGINTX
    other.set("c19999z", "added")
    other.delete(key(7000))
    for cmd in [("SET", key(10), "mine"),                 # overwrite
                ("SET", "c00010a", "new"),                # add
                ("SET", "c12345b", "new"),
                ("DEL", key(11)),                         # delete
                ("DEL", "c00011q"),                       # delete what isn't there
                ("SET", "c00500t", "tmp"), ("DEL", "c00500t"),   # add then delete
                ("DEL", key(600)), ("SET", key(600), "again"),   # delete then add
                ("DEL", key(N - 1)),                      # the last one
                ("SET", "c99999", "past the rest")]:      # above every other
        c.execute_command(*cmd)
    seen |= {"c00010a", "c12345b", "c99999"}
    seen -= {key(11), key(N - 1)}
    order = sorted(seen)

    print("counts")
    for lo, hi in [("c", "d"), ("c00000", "c00012"), ("c000", "c001"), ("c00500", "c00700"),
                   ("c05000", "c05001"), ("c12345", "c12346"), ("c19990", "c99999"),
                   ("c19990", "c999999"), ("c1", "c2")]:
        want = sum(1 for k in order if lo <= k < hi)
        got = answer(c, "CALLF", "cnt", lo, hi)
        check(got == want, "count [%s, %s) is %d (%s)" % (lo, hi, want, got))
    got = answer(c, "COUNT", "c", "d")
    check(got == len(order), "COUNT c d is %d (%s)" % (len(order), got))

    print("offsets")
    bad = []
    # around every key the transaction or the other client touched; an offset over 256
    # is searched for, a shorter one stepped over
    near = []
    for k in ["c00010", "c00010a", "c00011", "c00500t", "c00600", "c05000", "c05000x", "c07000",
              "c12345b", "c19998", "c19999z", "c99999"]:
        at = sum(1 for o in order if o < k)
        near += [at + d for d in range(-3, 4)]
    tried = sorted(set(o for o in list(range(0, 25)) + near + list(range(250, 262))
                       + list(range(len(order) - 5, len(order))) + list(range(0, len(order), 997))
                       if o >= 0))
    for off in tried:
        got = answer(c, "CALLF", "page", "c", "d", 3, off)
        want = ",".join(order[off:off + 3])
        if got != want:
            bad.append((off, want, got))
    check(not bad, "%d offsets from c agree with the walk %s" % (len(tried), bad[:2]))
    sub = [k for k in order if "c00300" <= k < "c00900"]
    bad = [o for o in range(0, len(sub) + 1)
           if answer(c, "CALLF", "page", "c00300", "c00900", 2, o) != ",".join(sub[o:o + 2])]
    check(not bad, "every offset in [c00300, c00900) agrees %s" % bad[:3])
    check(answer(c, "CALLF", "page", "c", "d", 3, len(order)) == "", "an offset past the end answers nothing")

    print("timing")
    small = median_ms(c, "CALLF", "cnt", "c00100", "c00400")
    big = median_ms(c, "CALLF", "cnt", "c", "d")
    check(big < small * 3 + 1, "a count over all of them is about one over 300 (%.2f ms against %.2f)"
          % (big, small))
    # 300, not 50: an offset of 256 or less is stepped over, and that's no measure of
    # a search - TODO 770
    small = median_ms(c, "CALLF", "page", "c", "d", 3, 300, runs=31)
    big = median_ms(c, "CALLF", "page", "c", "d", 3, N - 100, runs=31)
    check(big < small * 3 + 1, "an offset near the end is about one 300 in (%.2f ms against %.2f)"
          % (big, small))
    c.execute_command("ABORTTX")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
