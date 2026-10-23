#!/usr/bin/env python3
"""A split space's record, read by a member that applies the cluster log slowly - TODO 735.

The cluster leader records a replicated space in the cluster space: its first group,
its creator, how many groups it's split over and its shard count. A member sees an
HSET's fields arrive one at a time, so one that read the record halfway through
could see the group and creator but not the group count yet. It took the space as
unsplit and opened its first group over all of it, while every other member had
that group own its first run of shards. A running group isn't opened again, so it
stayed that way: under `ctest -j4` this was TestClusterSplit's "three groups, three
voters each" failing now and then.

Node 2 here takes BARCH_TEST_CLUSTER_APPLY_GAP_MS=200, a pause before each cluster
log entry it applies, which makes the half-applied record last long enough to be
read. Each of four rounds, a new cluster with one split space, is another chance to
catch it. (One space a cluster: a node has four raft ports, one a group.)

What has to hold, in each round, on every member: three groups, each with its own
label and run of shards, and none of them over the whole space.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

ROOT = scale.workdir()
BASE = scale.port(default=23700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "rec"
ROUNDS = 4
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def groups_of(n, space):
    """this node's group lines for the space"""
    return [l for l in n.info() if re.match(r"group \d+ spaces %s( |$)" % re.escape(space), l)]


def split_right(space):
    def done():
        for n in nodes:
            lines = groups_of(n, space)
            labels = sorted(m.group(1) for l in lines for m in [re.search(r" label (\S+)", l)] if m)
            if len(lines) != 3 or labels != ["%s/%d" % (space, k) for k in range(3)]:
                return False
            if not all(" members 3 " in l and " learners 0 " in l for l in lines):
                return False
        return True
    return done


SLOW = {"BARCH_TEST_CLUSTER_APPLY_GAP_MS": "200"}
for round in range(ROUNDS):
    # each round in its own directory, so a failed one's logs are kept
    os.makedirs(os.path.join(ROOT, "round%d" % round), exist_ok=True)
    os.chdir(os.path.join(ROOT, "round%d" % round))
    print("round %d: three nodes, node 2 applying the cluster log slowly" % (round + 1))
    # heartbeats every 3s rather than 500ms: each is a few cluster log entries, and
    # more than five a second would leave node 2 ever further behind
    nodes = [clusternodes.Node(i, BASE, BINARY, settings=["cluster_heartbeat_ms=3000"], env=SLOW if i == 1 else {})
             for i in range(3)]
    try:
        for n in nodes:
            n.start()
        nodes[0].client().execute_command("CLUSTER", "INIT")
        for n in nodes[1:]:
            n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
        conf = nodes[0].client("configuration")
        conf.execute_command("SET", SPACE + ".raft_groups", "3")
        conf.execute_command("SET", SPACE + ".raft", "on")
        ok = wait_for(90, split_right(SPACE))
        check(ok, "three groups with their own runs, on every member")
        if not ok:
            for n in nodes:
                print("    node %d:" % (n.i + 1), groups_of(n, SPACE))
    finally:
        for n in nodes:
            n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
