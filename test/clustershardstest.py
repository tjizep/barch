#!/usr/bin/env python3
"""
The shard count of a replicated space - TODO 631.

A replicated write holds its shard until it commits, so a space has as many writes
committing at once as it has shards. The cluster leader decides a space's count once,
when it starts replicating it, and keeps it in the configuration space, so every
member opens it the same.

What has to hold, with raft_shards 32:
  - a new replicated space has 32 shards on every member;
  - a space that already had keys on the leader keeps its count (17) everywhere,
    and its keys;
  - a member that made the space itself before it was replicated stays out of its
    group, rather than take a copy that won't fit, and says why in its log, once.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of, on_leader  # noqa: E402

scale.workdir()
BASE = scale.port(default=28300)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def space_shards(n, space):
    m = re.search(r"\bspace_shards (\d+)", n.group_line(space))
    return int(m.group(1)) if m else -1


def configured(n, space):
    # configuration is replicated, so a follower answers only a follower read
    c = n.client("configuration")
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c.execute_command("GET", space + ".shards")


def replicated(nodes, space):
    return lambda: leader_of(nodes, space) is not None and all(
        "members 3" in n.group_line(space) and "learners 0" in n.group_line(space) for n in nodes)


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_shards=32"]) for i in range(3)]
try:
    print("three nodes, raft_shards 32")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    check(wait_for(60, lambda: all("members 3" in n.group_line("cluster") and "learners 0" in n.group_line("cluster")
                                   for n in nodes)), "a cluster of three")
    # a space with keys on the leader, made before it's replicated: 17 shards
    nodes[0].client("old").execute_command("SET", "kept", "yes")
    conf = nodes[0].client("configuration")
    # one a follower makes for itself first, which the leader doesn't have
    nodes[2].client("early").execute_command("SET", "x", "1")
    time.sleep(1)
    # three: the test's nodes have Raft ports for the cluster group and three more
    for space in ("fresh", "old", "early"):
        conf.execute_command("SET", space + ".raft", "on")
    for space in ("fresh", "old"):
        check(wait_for(90, replicated(nodes, space)), "%s is replicated on all three" % space)
    # node 3's own copy of early has the wrong count, so it can't take the leader's
    # (the leader still adds it, as a learner that never catches up)
    check(wait_for(90, lambda: leader_of(nodes[:2], "early") is not None
                   and all("members" in n.group_line("early") for n in nodes[:2])),
          "early is replicated on the other two")

    print("what each space got")
    check(all(configured(n, "fresh") == "32" for n in nodes) and all(space_shards(n, "fresh") == 32 for n in nodes),
          "a new space has 32 shards everywhere (%s)" % [space_shards(n, "fresh") for n in nodes])
    check(all(configured(n, "old") == "17" for n in nodes) and all(space_shards(n, "old") == 17 for n in nodes),
          "a space with keys keeps its 17 (%s)" % [space_shards(n, "old") for n in nodes])
    l = leader_of(nodes, "old")
    check(l is not None and l.client("old").execute_command("GET", "kept") == "yes", "and its keys")
    log = open(nodes[2].dir + ".log").read()
    said = log.count("space early has 17 shards here and 32 in the cluster")
    check(said == 1 and "members" not in nodes[2].group_line("early"),
          "a member that made the space first stays out, and says why once (%d times)" % said)

    print("writes to the new space")
    w = clusternodes.Writers(nodes, "fresh", count=4, prefix="f")
    w.start()
    time.sleep(scale.scaled_seconds(2.0))
    w.stop()
    missing = on_leader(nodes, "fresh", lambda n: sum(
        1 for k, v in w.acked.items() if n.client("fresh").execute_command("GET", k) != v))
    missing = len(w.acked) if missing is None else missing
    check(len(w.acked) > 0 and missing == 0, "every acknowledged write is there (%d, %d missing)" % (len(w.acked), missing))
    check(wait_for(60, lambda: len(set(n.digest("fresh")[0] for n in nodes)) == 1), "and the copies match")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
