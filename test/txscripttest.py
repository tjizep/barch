#!/usr/bin/env python3
"""
A script's transaction is its connection's - TODO 705, the first part of TODO 697.

A script's `barch.call('BEGINTX')` used to open a transaction on the script's own
caller, which the connection keeps between calls. A script that stopped before its
COMMITTX left it open there, where the client couldn't see or end it: every later
script on the connection answered "a transaction is already open", and its writes
through barch.call went into the abandoned overlay and were never made.

Now a script's commands share the connection's transaction. Checked against one
barchd, a space without Raft:
  - a script runs BEGINTX, SET, GET, COMMITTX: the write is made, and its GET saw it;
  - a script that raises with a transaction open leaves the connection's transaction
    open: the client's BEGINTX says so, its ABORTTX ends it, and nothing of it is made;
  - after that, later scripts open and commit transactions of their own, and their
    writes through barch.call are made;
  - a script that opens one and returns: the client's own writes join it, and the
    client's COMMITTX makes both;
  - a closed connection drops a transaction a script left open.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14690)
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


DATA = os.path.join(os.getcwd(), "txscript_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txscript.log"), "a")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
end = time.time() + 60
while time.time() < end:
    try:
        socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
        break
    except OSError:
        time.sleep(0.1)

SPACE = "txs" + str(int(time.time()))


def client():
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", SPACE)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


try:
    c = client()
    for name, body in {
        "whole": "function call(k)\n barch.call('BEGINTX')\n barch.call('SET', k, 'in-tx')\n"
                 " local g = barch.call('GET', k)\n barch.call('COMMITTX')\n return g\nend\n",
        "raises": "function call(k)\n barch.call('BEGINTX')\n barch.call('SET', k, 'lost')\n"
                  " error('stopped')\nend\n",
        "opens": "function call(k)\n barch.call('BEGINTX')\n barch.call('SET', k, 'from-script')\n"
                 " return 'open'\nend\n",
    }.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)

    print("a script's whole transaction")
    check(answer(c, "CALLF", "whole", "w1") == "in-tx" and c.get("w1") == "in-tx",
          "its GET saw its SET, and COMMITTX made it")

    print("a script that raises with one open")
    got = answer(c, "CALLF", "raises", "r1")
    check(str(got).startswith("ERR") and "stopped" in str(got), "CALLF answers the error (%s)" % str(got)[:40])
    check("already open" in str(answer(c, "BEGINTX")), "the client sees the connection's transaction open")
    check(answer(c, "ABORTTX") == "OK" and c.get("r1") is None, "its ABORTTX ends it, and nothing of it is made")
    check(answer(c, "CALLF", "whole", "w2") == "in-tx" and c.get("w2") == "in-tx",
          "a later script opens and commits one of its own, and its write is made")

    print("a script that opens one and returns")
    check(answer(c, "CALLF", "opens", "o1") == "open", "CALLF answers")
    check(answer(c, "GET", "o1") == "from-script", "the client, in the same transaction, reads the script's write")
    check(answer(c, "SET", "o2", "from-client") is True, "the client's own write joins it")
    check(answer(c, "COMMITTX") == "OK", "the client's COMMITTX answers OK")
    check(c.get("o1") == "from-script" and c.get("o2") == "from-client", "and both writes are made")

    print("a closed connection drops one a script left open")
    d = client()
    d.execute_command("CALLF", "opens", "o3")
    d.close()
    time.sleep(0.2)
    e = client()
    check(e.get("o3") is None and answer(e, "BEGINTX") == "OK", "nothing of it is made, and a new one opens")
    e.execute_command("ABORTTX")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
