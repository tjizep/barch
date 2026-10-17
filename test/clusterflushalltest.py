#!/usr/bin/env python3
"""
FLUSHALL and CLEARALL in a cluster - TODO 680.

They clear every space, and a node in a cluster refused them, because they cleared a
replicated space on that node only (and `cluster` with it). Now a replicated space is
cleared through its group, as FLUSHDB clears it, which takes the node leading it; one
that isn't replicated is that node's own and is cleared there. `configuration` and,
in a cluster, `cluster` are left alone.

What has to hold, on a three-node cluster with one replicated space and a space each
node holds on its own:
  - FLUSHALL sent to a follower answers NOTLEADER and clears nothing, there or
    anywhere;
  - FLUSHALL on the leader empties the replicated space on every member, clears the
    leader's own space and leaves the followers' own spaces as they were, and the
    cluster and its configuration go on as before;
  - CLEARALL does the same.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=33300)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
OWN = "mine"
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


def local_get(n, space, key):
    c = n.client(space)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c.execute_command("GET", key)


def settled(nodes):
    l = leader_of(nodes, SPACE)
    top = l.field(SPACE, "applied")
    return wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in nodes))


def fill(nodes, leader):
    c = leader.client(SPACE)
    for i in range(20):
        c.execute_command("SET", "k%d" % i, "v")
    for n in nodes:
        n.client(OWN).execute_command("SET", "m", "node%d" % n.i)
    settled(nodes)


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
try:
    print("a replicated space and a space of each node's own, on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    leader = leader_of(nodes, SPACE)
    followers = [n for n in nodes if n is not leader]

    for command in ("FLUSHALL", "CLEARALL"):
        print(command)
        fill(nodes, leader)
        got = answer(followers[0].client(), command)
        check(str(got).startswith("ERR NOTLEADER"), "on a follower: NOTLEADER (%r)" % str(got)[:70])
        settled(nodes)
        check(all(local_get(n, SPACE, "k5") == "v" for n in nodes), "and the replicated space is untouched")
        check(local_get(followers[0], OWN, "m") == "node%d" % followers[0].i, "and so is that follower's own space")
        got = answer(leader.client(), command)
        check(got in ("OK", True), "on the leader: OK (%r)" % got)
        settled(nodes)
        check(all(n.digest(SPACE)[0].startswith("keys 0 ") for n in nodes),
              "the replicated space is empty on every member")
        check(local_get(leader, OWN, "m") is None, "the leader's own space is cleared")
        check(all(local_get(n, OWN, "m") == "node%d" % n.i for n in followers),
              "the followers' own spaces are as they were")
        check(all("members 3" in n.group_line(SPACE) for n in nodes)
              and nodes[0].client("configuration").execute_command("GET", SPACE + ".raft") == "on",
              "the cluster and its configuration go on as before")
        c = leader.client(SPACE)
        c.execute_command("SET", "after", "1")
        settled(nodes)
        check(all(local_get(n, SPACE, "after") == "1" for n in nodes), "and the space takes writes everywhere")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
