#!/usr/bin/env python3
"""
A space ROLLBACK under an open transaction - TODO 772.

BEGIN and ROLLBACK put a whole space back the way it was. ROLLBACK does that by putting
the shards' pages back, not by writing each key, so it left nothing in a snapshot's
layers, and COMMITTX didn't see that a key it read had been taken back. It committed as
if nothing had happened. Now it's treated like a shard clear or load: a transaction
open across one conflicts.

What has to hold, in a plain space and in a Raft space (one node):
  - one client BEGINs and writes a key, another BEGINTXes and reads it, and the first
    ROLLBACKs. COMMITTX is a TXCONFLICT and writes nothing. The same for a key the
    ROLLBACK puts back to an older value, for a key it takes out of a range the
    transaction walked, and for a transaction that only read;
  - a pessimistic one too (not in a Raft space, which doesn't take them yet);
  - in a Raft space, the same with the node's local check off
    (BARCH_TEST_TX_NO_LOCAL_CHECK). The groups' prepares can't see a ROLLBACK: COMMITTX
    sends them the hash of each range as its snapshot saw it, worked out from the
    layers, and a ROLLBACK leaves nothing there. So the count of clears is checked even
    then.
  - a ROLLBACK with nothing to roll back, or a BEGIN and ROLLBACK over before BEGINTX,
    leaves the transaction alone: it commits.

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
PORT = scale.port(default=14833)
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


def ok(got):
    return got in ("OK", True)


def run(a, b, raft=False):
    """a runs BEGIN and ROLLBACK on the space, b the transactions"""
    a.set("k5", "base")
    a.delete("r1", "p5", "w1", "w2", "w3", "w4", "w5", "w6", "w7")

    def across(begin, read, write, what):
        """b's transaction, open across a BEGIN, a write by a, and a ROLLBACK"""
        check(ok(answer(a, "BEGIN")), "BEGIN")
        a.set(*write)
        check(ok(answer(b, *begin)), " ".join(begin))
        seen = read()
        b.set("w%d" % run.n, "tx")
        check(ok(answer(a, "ROLLBACK")), "ROLLBACK")
        got = answer(b, "COMMITTX")
        check("TXCONFLICT" in str(got) and a.get("w%d" % run.n) is None,
              "%s: TXCONFLICT, nothing written (%s, read %s)" % (what, str(got)[:40], seen))
        run.n += 1

    run.n = 1
    across(["BEGINTX"], lambda: b.get("r1"), ("r1", "late"), "a key the ROLLBACK takes out")
    across(["BEGINTX"], lambda: b.get("k5"), ("k5", "late"), "a key it puts back as it was")
    across(["BEGINTX"], lambda: b.execute_command("RANGE", "p", "q", 10), ("p5", "late"),
           "a range it takes a key out of")
    if raft:
        check("doesn't take BEGINTX PESSIMISTIC" in str(answer(b, "BEGINTX", "PESSIMISTIC")),
              "BEGINTX PESSIMISTIC is refused in a Raft space")
    else:
        across(["BEGINTX", "PESSIMISTIC"], lambda: b.get("r1"), ("r1", "late"), "pessimistic")

    # a transaction that only reads
    check(ok(answer(a, "BEGIN")), "BEGIN")
    a.set("r1", "late")
    b.execute_command("BEGINTX")
    seen = b.get("r1")
    check(ok(answer(a, "ROLLBACK")), "ROLLBACK")
    got = answer(b, "COMMITTX")
    check("TXCONFLICT" in str(got), "one that only read: TXCONFLICT (%s, read %s)" % (str(got)[:40], seen))

    # nothing rolled back while it was open
    b.execute_command("BEGINTX")
    b.get("k5")
    b.set("w6", "tx")
    check(ok(answer(a, "ROLLBACK")), "ROLLBACK with no BEGIN")
    got = answer(b, "COMMITTX")
    check(ok(got) and a.get("w6") == "tx", "a ROLLBACK with nothing to roll back: it commits (%s)" % got)
    check(ok(answer(a, "BEGIN")), "BEGIN")
    a.set("r1", "late")
    check(ok(answer(a, "ROLLBACK")), "ROLLBACK")
    b.execute_command("BEGINTX")
    b.get("r1")
    b.set("w7", "tx")
    got = answer(b, "COMMITTX")
    check(ok(got) and a.get("w7") == "tx", "one begun after the ROLLBACK: it commits (%s)" % got)
    check(a.get("k5") == "base" and a.get("r1") is None, "and the space is as the ROLLBACKs left it")


SPACE = "txspacerb_" + str(int(time.time()))
DATA = os.path.join(os.getcwd(), "txspacerb_data")
os.makedirs(DATA, exist_ok=True)
log = open(os.path.join(os.getcwd(), "txspacerb.log"), "w")
p = subprocess.Popen([BINARY, "--port", str(PORT), "--bind", "127.0.0.1", "--dir", DATA, "--no-save-on-exit",
                      "-c", "save_interval=86400000"], stdout=log, stderr=log)
nodes = []
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

    print("a plain space")
    run(client(PORT), client(PORT))

    # one node is a cluster of its own, and a space with raft on writes through Raft
    for i, (what, env) in enumerate((("a Raft space", {}),
                                     ("a Raft space, the local check off", {"BARCH_TEST_TX_NO_LOCAL_CHECK": "1"}))):
        node = clusternodes.Node(i, PORT + 1, BINARY, env=env)
        nodes.append(node)
        node.start()
        init = answer(node.client(), "CLUSTER", "INIT")
        if "no cluster" in str(init):
            print("a Raft space: SKIP, this build has no cluster")
            break
        print(what)
        node.client("configuration").execute_command("SET", SPACE + ".raft", "on")
        up = wait_for(60, lambda: leader_of([node], SPACE) is not None)
        check(up, "the Raft space has a leader")
        if up:
            run(node.client(SPACE), node.client(SPACE), raft=True)
        node.stop()
finally:
    p.kill()
    p.wait()
    for node in nodes:
        node.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
