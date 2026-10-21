#!/usr/bin/env python3
"""
What a script can't reach while its session has a transaction open - TODO 708, the rest
of TODO 697's group 1.

barch.store goes through the transaction's overlay (TODO 707). The rest of what a script
reaches either writes past the overlay or can't be undone if the transaction aborts, so
inside a transaction it's refused, saying so: barch.fs and barch.graph (files and edges
are written in batches of their own), barch.queue (a message is handled later by another
function), and barch.call to another space (a transaction covers one space). A nested
CALLF joins the transaction.

Checked against one barchd, spaces without Raft:
  - barch.fs.put and barch.graph.put work outside a transaction, and inside one are
    refused, leaving nothing;
  - barch.queue inside one is refused with the transaction's reason;
  - barch.call to another space works outside one, and inside one is refused, leaving
    nothing there;
  - a nested CALLF's barch.call and barch.store writes inside a transaction land only at
    COMMITTX, and not at all on ABORTTX.
"""
import os
import socket
import subprocess
import sys
import time

import scale
import redis

scale.workdir()
PORT = scale.port(default=14720)
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


DATA = os.path.join(os.getcwd(), "txscriptapi_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txscriptapi.log"), "a")
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
SPACE, OTHER = "txa" + TAG, "txb" + TAG


def client(space=SPACE):
    c = redis.Redis(host="127.0.0.1", port=PORT, socket_timeout=30, decode_responses=True,
                    single_connection_client=True)
    c.execute_command("USE", space)
    return c


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


def refused(got):
    return str(got).startswith("ERR") and "transaction" in str(got)


FUNCTIONS = {
    "fsput": "function call(path) return barch.fs.put(path, 'body', 'text/plain', 65536) end\n",
    "fsget": "function call(path) local v = barch.fs.get(path) return v == nil and 'nil' or 'there' end\n",
    "gput": "function call(path) barch.graph.put(path, 'edge', 'text/plain') return 'put' end\n",
    "qpush": "function call(m) return barch.queue('work', m) end\n",
    "other": "function call(space, k, v) return barch.space[space]:call('SET', k, v) end\n",
    "inner": "function call(k, v) barch.call('SET', k, v) barch.store.set(k .. '_s', v) return 'inner' end\n",
    "outer": "function call(k, v) return barch.call('CALLF', 'inner', k, v) end\n",
}

try:
    c = client()
    client(OTHER).execute_command("SET", "seed", "1")
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)

    print("barch.fs and barch.graph")
    got = answer(c, "CALLF", "fsput", "/a.txt")
    check(not str(got).startswith("ERR"), "barch.fs.put works outside a transaction (%s)" % str(got)[:50])
    check(not str(answer(c, "CALLF", "gput", "/g1.txt")).startswith("ERR"), "and barch.graph.put")
    c.execute_command("BEGINTX")
    got = [answer(c, "CALLF", "fsput", "/b.txt"), answer(c, "CALLF", "gput", "/g2.txt")]
    c.execute_command("ABORTTX")
    check(all(refused(g) for g in got), "inside one both are refused, saying why (%s)" % [str(g)[:70] for g in got])
    check(answer(c, "CALLF", "fsget", "/b.txt") == "nil", "and nothing was written")

    print("barch.queue")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "qpush", "m1")
    c.execute_command("ABORTTX")
    check(refused(got) and "barch.queue" in str(got), "inside one is refused, saying why (%s)" % str(got)[:70])

    print("barch.call to another space")
    check(answer(c, "CALLF", "other", OTHER, "o1", "x") is not None and client(OTHER).get("o1") == "x",
          "works outside a transaction")
    c.execute_command("BEGINTX")
    got = answer(c, "CALLF", "other", OTHER, "o2", "x")
    c.execute_command("ABORTTX")
    check(refused(got) and client(OTHER).get("o2") is None, "inside one is refused, leaving nothing (%s)"
          % str(got)[:70])

    print("a nested CALLF")
    c.execute_command("BEGINTX")
    check(answer(c, "CALLF", "outer", "n1", "v") == "inner", "answers through both")
    check(client().get("n1") is None and client().get("n1_s") is None, "nothing seen outside before COMMITTX")
    check(answer(c, "COMMITTX") == "OK" and client().get("n1") == "v" and client().get("n1_s") == "v",
          "COMMITTX makes its barch.call and barch.store writes")
    c.execute_command("BEGINTX")
    answer(c, "CALLF", "outer", "n2", "v")
    c.execute_command("ABORTTX")
    check(client().get("n2") is None and client().get("n2_s") is None, "ABORTTX makes neither")
finally:
    p.kill()
    p.wait()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
