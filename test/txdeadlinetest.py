#!/usr/bin/env python3
"""
A deadline written over a key that's already there - TODO 768.

It showed up as a transaction losing a PSETEX's deadline at COMMITTX, but the
transaction wasn't it. `art::key_options` starts with keep_ttl set, and SETEX/PSETEX
only set the expiry, so writing over a leaf that's already there kept that leaf's
deadline instead. A transaction always has a leaf in its overlay first (a tomb for a
key it found missing, or the copy it read), so there it lost the deadline every time.
GETSET had the same default and kept a deadline that redis drops.

What has to hold, in a plain space and in a Raft space (one node):
  - SETEX, PSETEX, SET PX and SET EX in a transaction, on keys nobody has, on keys
    without a deadline and on keys with a short one: once committed, each has the
    deadline it was given;
  - the same writes outside a transaction;
  - GETSET drops the deadline, in a transaction and outside one;
  - APPEND, SETRANGE and INCR keep theirs, as redis does (SETRANGE outside a
    transaction only, since one doesn't take it);
  - from a function, barch.store.set, setJson and barch.space[name][k] = v drop it,
    as SET does - TODO 769. They kept it before.

A build without the cluster skips the Raft half.
"""
import os
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
PORT = scale.port(default=14830)
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


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


LONG = 600000           # the deadline written, in ms
SHORT = 300000          # one a key already had

# each write, and the deadline in ms it should leave, near enough
WRITES = [
    (["PSETEX", "{k}", str(LONG), "new"], LONG),
    (["SETEX", "{k}", str(LONG // 1000), "new"], LONG),
    (["SET", "{k}", "new", "PX", str(LONG)], LONG),
    (["SET", "{k}", "new", "EX", str(LONG // 1000)], LONG),
    (["GETSET", "{k}", "new"], -1),
]
# what the key held before: nothing, a plain value, or one with a short deadline
BEFORE = ["missing", "plain", "short"]


def prepare(c, key, before):
    c.delete(key)
    if before == "plain":
        c.set(key, "old")
    elif before == "short":
        c.set(key, "old", px=SHORT)


def near(got, want):
    if want < 0:
        return got == want
    return want - 5000 < got <= want


def run(c, other, where):
    print(where)
    for name, body in FUNCTIONS.items():
        check(answer(c, "SETF", name, body) == "OK", "SETF %s" % name)
    n = 0
    for in_tx in (False, True):
        for cmd, want in WRITES:
            for before in BEFORE:
                n += 1
                key = "d%d" % n
                prepare(c, key, before)
                cmd_k = [a.replace("{k}", key) for a in cmd]
                if in_tx:
                    answer(c, "BEGINTX")
                got = answer(c, *cmd_k)
                committed = answer(c, "COMMITTX") if in_tx else "OK"
                ttl = other.pttl(key)
                value = other.get(key)
                check(not str(got).startswith("ERR") and committed == "OK" and value == "new" and near(ttl, want),
                      "%s %s over a key %s: pttl %s" % ("tx" if in_tx else "  ", cmd[0], before, ttl))
        # the ones that keep the deadline the key had
        for name, cmd, value in (("APPEND", ["APPEND", "{k}", "x"], "1x"),
                                 ("SETRANGE", ["SETRANGE", "{k}", "0", "2"], "2"),
                                 ("INCR", ["INCR", "{k}"], "2")):
            if in_tx and name == "SETRANGE":
                continue        # not taken in a transaction
            n += 1
            key = "d%d" % n
            c.delete(key)
            c.set(key, "1", px=SHORT)
            if in_tx:
                answer(c, "BEGINTX")
            got = answer(c, *[a.replace("{k}", key) for a in cmd])
            committed = answer(c, "COMMITTX") if in_tx else "OK"
            ttl = other.pttl(key)
            check(not str(got).startswith("ERR") and committed == "OK" and other.get(key) == value
                  and near(ttl, SHORT), "%s %s keeps the deadline it had: pttl %s" % ("tx" if in_tx else "  ", name, ttl))
        # a function's whole-value writes drop it, as SET does - TODO 769
        for name in FUNCTIONS:
            for before in BEFORE:
                n += 1
                key = "d%d" % n
                prepare(c, key, before)
                if in_tx:
                    answer(c, "BEGINTX")
                got = answer(c, "CALLF", name, key, SPACE)
                committed = answer(c, "COMMITTX") if in_tx else "OK"
                ttl = other.pttl(key)
                check(not str(got).startswith("ERR") and committed == "OK" and other.get(key) is not None
                      and ttl == -1, "%s %s over a key %s: pttl %s %s" % ("tx" if in_tx else "  ", name, before, ttl,
                                                                        str(got)[:30] if got else ""))


# barch.store.set and the writes that share it: a function's version of SET - TODO 769
# (a Raft space runs one in a transaction only with its writes replicated - TODO 697)
FX = '--@barch {"replicate": "effects"}\n'
FUNCTIONS = {
    "store_set": FX + "function call(k) barch.store.set(k, 'new') end",
    "set_json": FX + "function call(k) barch.store.setJson(k, {a = 1}) end",
    "space_assign": FX + "function call(k, sp) barch.space[sp][k] = 'new' end",
}


SPACE = "txdeadline_" + str(int(time.time()))
DATA = os.path.join(os.getcwd(), "txdeadline_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txdeadline.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
node = None
try:
    end = time.time() + 60
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", PORT), timeout=0.5).close()
            break
        except OSError:
            time.sleep(0.1)

    def client(port):
        c = redis.Redis(host="127.0.0.1", port=port, socket_timeout=60, decode_responses=True,
                        single_connection_client=True)
        c.execute_command("USE", SPACE)
        return c

    run(client(PORT), client(PORT), "a plain space")

    # one node is a cluster of its own, and a space with raft on writes through Raft
    node = clusternodes.Node(0, PORT + 1, BINARY)
    node.start()
    init = answer(node.client(), "CLUSTER", "INIT")
    if "no cluster" in str(init):
        print("a Raft space: SKIP, this build has no cluster")
    else:
        node.client("configuration").execute_command("SET", SPACE + ".raft", "on")
        ok = wait_for(60, lambda: leader_of([node], SPACE) is not None)
        check(ok, "the Raft space has a leader")
        if ok:
            run(node.client(SPACE), node.client(SPACE), "a Raft space")
finally:
    p.kill()
    p.wait()
    if node:
        node.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
