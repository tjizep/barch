#!/usr/bin/env python3
"""
Walks over a dependent space see the merged view - TODO 701.

A space that DEPENDS on another reads through it: a key it has is its own, a key it
deleted is a tomb that hides the source's, and a key it doesn't have is the source's.
Point reads have always done that. A walk has to do the same: every key once, the
dependant's value where both have it, nothing the dependant deleted, and the keys
only the dependant has. And it has to hold for a chain - c on b on a - which is what a
transaction's version stack will be.

Checked against one barchd, with a (k0..k9), b on a (k3 changed, k5 deleted, n1
added) and c on b (k4 changed, k3 deleted, n2 added):
  - KEYS, SCAN (to the end), RANGE, COUNT and DBSIZE on b and on c give each key once,
    as the merge says;
  - GET of every key on b and c gives the merged value;
  - a and b themselves are unchanged by what's above them.
"""
import os
import subprocess
import socket
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14670)
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


DATA = os.path.join(os.getcwd(), "dependsiter_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "dependsiter.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)


def space(name):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", name)
    return c


def scan_all(c):
    out, cursor = [], "0"
    for _ in range(1000):
        cursor, page = c.execute_command("SCAN", cursor, "COUNT", "3")
        out += page
        if str(cursor) == "0":
            break
    return out


def walks(c):
    return {
        "KEYS": sorted(c.execute_command("KEYS", "*")),
        "SCAN": sorted(scan_all(c)),
        "RANGE": sorted(c.execute_command("RANGE", "a", "zz", "100")),
        "COUNT": c.execute_command("COUNT", "a", "zz"),
        "DBSIZE": c.execute_command("DBSIZE"),
    }


try:
    tag = str(int(time.time()))
    A, B, C = "ia" + tag, "ib" + tag, "ic" + tag
    a, b, c = space(A), space(B), space(C)
    for i in range(10):
        a.execute_command("SET", "k%d" % i, "a%d" % i)
    a.execute_command("SPACES", "DEPENDS", B, "ON", A)
    b.execute_command("SET", "k3", "b3")
    b.execute_command("DEL", "k5")
    b.execute_command("SET", "n1", "b-n1")
    a.execute_command("SPACES", "DEPENDS", C, "ON", B)
    c.execute_command("SET", "k4", "c4")
    c.execute_command("DEL", "k3")
    c.execute_command("SET", "n2", "c-n2")

    want_b = {"k%d" % i: "a%d" % i for i in range(10)}
    want_b.update({"k3": "b3", "n1": "b-n1"})
    del want_b["k5"]
    want_c = dict(want_b)
    want_c.update({"k4": "c4", "n2": "c-n2"})
    del want_c["k3"]

    for name, cl, want in (("b on a", b, want_b), ("c on b on a", c, want_c)):
        print(name)
        got = walks(cl)
        keys = sorted(want)
        for walk in ("KEYS", "SCAN", "RANGE"):
            check(got[walk] == keys, "%s gives each key once (%s)" % (walk, got[walk]))
        check(got["COUNT"] == len(keys), "COUNT is %d (%s)" % (len(keys), got["COUNT"]))
        check(got["DBSIZE"] == len(keys), "DBSIZE is %d (%s)" % (len(keys), got["DBSIZE"]))
        vals = {k: cl.execute_command("GET", k) for k in sorted(set(want) | {"k3", "k5"})}
        check(all(vals[k] == want.get(k) for k in vals), "GET gives the merged value (%s)" % vals)

    print("what's beneath is unchanged")
    check(sorted(a.execute_command("KEYS", "*")) == ["k%d" % i for i in range(10)]
          and a.execute_command("GET", "k3") == "a3", "a holds its own ten")
    check(b.execute_command("GET", "k3") == "b3" and b.execute_command("GET", "k4") == "a4",
          "b holds its own, under c")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
