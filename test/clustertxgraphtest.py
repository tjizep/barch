#!/usr/bin/env python3
"""
GRAPH in a transaction in a Raft space - TODO 751.

TODO 750 let GRAPH run inside BEGINTX, on the transaction's overlay, with its node, edge
and inode ids reserved from the space itself rather than the overlay. That was tested
without Raft only. Here a graph lives in a replicated space on three nodes, and what has
to hold is:
  - on the leader and on a follower, a transaction's GRAPH writes are seen inside it,
    not outside, and are on every member after COMMITTX;
  - ABORTTX drops them everywhere;
  - on the leader, a transaction's PUT while plain MKDIRs go into another directory
    commits, and every node and edge has its own id;
  - on a follower, which can't move the space's id counter, the same either commits
    with ids of its own or is a TXCONFLICT - never ids handed out twice - and once
    nothing races it, it commits;
  - a plain write into a directory the transaction listed: TXCONFLICT;
  - every member's digest is the same at the end.

Plain GRAPH reads on a follower answer NOTLEADER, even with CLUSTER READS FOLLOWER,
so what's outside a transaction is read on the leader, and members are shown to
agree by their digests.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=27460)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "graphs"
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def answer(c, *cmd):
    try:
        return c.execute_command(*cmd)
    except redis.ResponseError as e:
        return "ERR " + str(e)


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]


def settled():
    def done():
        tops = [n.field(SPACE, "applied") for n in nodes]
        return min(tops) >= 0 and len(set(tops)) == 1
    return wait_for(60, done)


def client(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def on_leader(*cmd):
    """a plain write, sent to each node until one takes it"""
    end = time.time() + 60
    while True:
        for n in nodes:
            try:
                return n.client(SPACE).execute_command(*cmd)
            except redis.RedisError:
                if time.time() > end:
                    raise
        time.sleep(0.2)


def names(listed):
    """the names from GRAPH LS lines"""
    return sorted(line.split(" ")[-1] for line in listed) if isinstance(listed, list) else listed


def read(*cmd):
    """a plain read, on the leader"""
    return answer(leader_of(nodes, SPACE).client(SPACE), *cmd)


def agree():
    """every member has applied the same, and holds the same"""
    return settled() and wait_for(30, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1)


def everywhere(cmd, want):
    """`cmd` answers `want` on the leader, and every member holds the same"""
    return agree() and read(*cmd) == want


try:
    print("three nodes and a replicated space")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: all("members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
                                   for n in nodes) and leader_of(nodes, SPACE) is not None),
          "three voters, and a leader")
    leader = leader_of(nodes, SPACE)
    follower = [n for n in nodes if n is not leader][0]
    check(on_leader("GRAPH", "MKDIR", "/a") == "OK" and on_leader("GRAPH", "MKDIR", "/b") == "OK",
          "a plain MKDIR")

    for where, n, tag in (("on the leader", leader, "l"), ("on a follower", follower, "f")):
        print("a transaction %s" % where)
        tx = client(n)
        check(answer(tx, "BEGINTX") == "OK", "BEGINTX")
        check(answer(tx, "GRAPH", "MKDIR", "/b/%sdir" % tag) == "OK"
              and answer(tx, "GRAPH", "PUT", "/b/%sdir/file" % tag, "from %s" % tag) == "OK", "MKDIR and PUT")
        check(answer(tx, "GRAPH", "GET", "/b/%sdir/file" % tag) == "from %s" % tag
              and "%sdir" % tag in names(answer(tx, "GRAPH", "LS", "/b")), "it sees its own writes")
        outside = read("GRAPH", "GET", "/b/%sdir/file" % tag)
        check(outside is None or "NOTLEADER" not in str(outside) and str(outside).startswith("ERR"),
              "nobody else does (%s)" % outside)
        got = answer(tx, "COMMITTX")
        check(got == "OK", "COMMITTX (%s)" % str(got)[:60])
        check(everywhere(("GRAPH", "GET", "/b/%sdir/file" % tag), "from %s" % tag), "then every member has it")

    print("ABORTTX")
    tx = client(follower)
    tx.execute_command("BEGINTX")
    answer(tx, "GRAPH", "PUT", "/b/gone", "x")
    answer(tx, "GRAPH", "RM", "/b/fdir/file")
    check(answer(tx, "ABORTTX") == "OK", "ABORTTX")
    check(everywhere(("GRAPH", "GET", "/b/fdir/file"), "from f"), "the RM isn't anywhere")
    check(agree() and "gone" not in names(read("GRAPH", "LS", "/b")), "nor the PUT")

    print("ids")
    for where, n in (("on the leader", leader), ("on a follower", follower)):
        d = "/ids_" + ("l" if n is leader else "f")
        on_leader("GRAPH", "MKDIR", d)
        on_leader("GRAPH", "MKDIR", d + "_tx")
        tx = client(n)
        tx.execute_command("BEGINTX")
        answer(tx, "GRAPH", "PUT", d + "_tx/mine", "from the transaction")
        for i in range(80):
            on_leader("GRAPH", "MKDIR", "%s/p%02d" % (d, i))
        got = answer(tx, "COMMITTX")
        if n is leader:
            check(got == "OK", "%s, COMMITTX after 80 plain MKDIRs (%s)" % (where, str(got)[:50]))
        else:
            check(got == "OK" or "TXCONFLICT" in str(got),
                  "%s, COMMITTX after 80 plain MKDIRs: OK or TXCONFLICT (%s)" % (where, str(got)[:50]))
            if got != "OK":
                tx.execute_command("BEGINTX")
                answer(tx, "GRAPH", "PUT", d + "_tx/mine", "from the transaction")
                got = answer(tx, "COMMITTX")
                check(got == "OK", "  with nothing racing it, it commits (%s)" % str(got)[:50])
            # and plain MKDIRs after it don't collide with what it took
            for i in range(80, 90):
                on_leader("GRAPH", "MKDIR", "%s/p%02d" % (d, i))
        want = 81 if n is leader else 91
        check(agree(), "  every member holds the same")
        lines = read("GRAPH", "LS", d)
        mine = read("GRAPH", "LS", d + "_tx")
        lines = (lines if isinstance(lines, list) else []) + (mine if isinstance(mine, list) else [])
        ids = [line.split(" ")[1] for line in lines] + [line.split(" ")[2] for line in lines]
        check(len(lines) == want and len(set(ids)) == len(ids),
              "  each node and edge its own id (%d lines)" % len(lines))
        check(read("GRAPH", "GET", d + "_tx/mine") == "from the transaction", "  and the PUT's bytes are its own")

    print("a conflict")
    tx = client(follower)
    tx.execute_command("BEGINTX")
    answer(tx, "GRAPH", "LS", "/a")
    on_leader("GRAPH", "MKDIR", "/a/sneaked")
    settled()
    answer(tx, "GRAPH", "PUT", "/b/late", "x")
    got = answer(tx, "COMMITTX")
    check("TXCONFLICT" in str(got), "a plain write into a listed directory (%s)" % str(got)[:50])
    check(agree() and "late" not in names(read("GRAPH", "LS", "/b")), "  and nothing of it is anywhere")

    check(agree(), "digests match")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
