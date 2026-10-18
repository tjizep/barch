#!/usr/bin/env python3
"""
Commands on a dependent space read through it - TODO 701.

A transaction's overlay is a space that DEPENDS on the one the transaction is in: its
commands run against the overlay, reading through to the space beneath for any key the
overlay doesn't hold, and writing only into the overlay. So every command a transaction
takes has to do that right on a dependent space, for a key that is only beneath: read
the value there, and leave what's beneath alone.

Checked against one barchd, b on a, each key set in a and not in b:
  - INCR, INCRBY, DECR, DECRBY on a number beneath add to it;
  - SETNX, SET NX and SET XX see the key beneath, and SET GET and GETSET answer its value;
  - SET KEEPTTL keeps the expiry beneath;
  - APPEND appends to the value beneath, STRLEN and EXISTS see it;
  - DEL of a key beneath answers 1 and hides it, and again answers 0;
  - MSET, SETEX and PSETEX write into b;
  - after all of it, a holds exactly what it did.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14680)
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


DATA = os.path.join(os.getcwd(), "dependswrite_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "dependswrite.log"), "a")
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


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


try:
    tag = str(int(time.time()))
    A, B = "wa" + tag, "wb" + tag
    a, b = space(A), space(B)
    beneath = {"n1": "10", "n2": "10", "n3": "10", "n4": "10", "s1": "x", "s2": "x", "s3": "x",
               "s4": "x", "s5": "x", "s6": "abc", "d1": "gone"}
    for k, v in beneath.items():
        a.execute_command("SET", k, v)
    a.execute_command("SET", "t1", "ttl", "EX", "1000")
    a.execute_command("SPACES", "DEPENDS", B, "ON", A)

    print("numbers beneath")
    check(answer(b, "INCR", "n1") == 11, "INCR adds to it (%s)" % answer(b, "GET", "n1"))
    check(answer(b, "INCRBY", "n2", "5") == 15, "INCRBY adds to it (%s)" % answer(b, "GET", "n2"))
    check(answer(b, "DECR", "n3") == 9, "DECR takes from it (%s)" % answer(b, "GET", "n3"))
    check(answer(b, "DECRBY", "n4", "3") == 7, "DECRBY takes from it (%s)" % answer(b, "GET", "n4"))

    print("sets that look first")
    check(answer(b, "SETNX", "s1", "y") == 0 and b.get("s1") == "x", "SETNX sees the key beneath")
    check(answer(b, "SET", "s2", "y", "NX") is None and b.get("s2") == "x", "SET NX sees it")
    check(answer(b, "SET", "s3", "y", "XX") is True and b.get("s3") == "y", "SET XX sees it, and writes")
    # redis-py's own set(get=True): through execute_command its SET callback turns any answer to a bool
    check(b.set("s4", "y", get=True) == "x" and b.get("s4") == "y", "SET GET answers what was beneath")
    check(answer(b, "GETSET", "s5", "y") == "x" and b.get("s5") == "y", "GETSET answers what was beneath")
    b.execute_command("SET", "t1", "new", "KEEPTTL")
    check(b.get("t1") == "new" and 0 < b.ttl("t1") <= 1000, "SET KEEPTTL keeps the expiry beneath (%s)" % b.ttl("t1"))

    print("reads and appends")
    check(answer(b, "APPEND", "s6", "def") == 6 and b.get("s6") == "abcdef", "APPEND appends to it (%s)"
          % b.get("s6"))
    check(answer(b, "STRLEN", "s1") == 1 and answer(b, "EXISTS", "s1", "nope") == 1, "STRLEN and EXISTS see it")

    print("deletes")
    check(answer(b, "DEL", "d1") == 1 and b.get("d1") is None, "DEL of a key beneath answers 1 and hides it")
    check(answer(b, "DEL", "d1") == 0 and answer(b, "EXISTS", "d1") == 0, "and again answers 0")

    print("plain writes")
    b.execute_command("MSET", "m1", "1", "m2", "2")
    b.execute_command("SETEX", "e1", "100", "v")
    b.execute_command("PSETEX", "e2", "100000", "v")
    check(b.execute_command("MGET", "m1", "m2", "e1", "e2") == ["1", "2", "v", "v"], "MSET, SETEX, PSETEX write")

    print("what's beneath is unchanged")
    got = {k: a.get(k) for k in beneath}
    check(got == beneath, "a holds what it did (%s)" % {k: v for k, v in got.items() if v != beneath[k]})
    check(a.get("t1") == "ttl" and all(a.get(k) is None for k in ("m1", "m2", "e1", "e2")),
          "and nothing written above it")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
