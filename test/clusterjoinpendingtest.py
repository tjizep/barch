#!/usr/bin/env python3
"""
CLUSTER JOIN waits out a membership change that hasn't committed yet - TODO 797.

The cluster group makes one membership change at a time: NuRaft refuses the next
until the last has committed. A node joins as a learner, and the leader's tick
makes it a voter once it's caught up. A JOIN that landed while the last node's
promotion was still committing was refused with "Previous configuration change
has not been committed yet", although the leader was only busy for a moment.
Tests that join their nodes back to back failed that way now and then, 2s in: in
two of four runs of the cluster suite, TestClusterQueue once and TestClusterLease
once, and very likely TestClusterTx's one-off failure before that.

Every node's log syncs take 150ms here, in every group, which leaves a promotion
uncommitted for long enough that back to back JOINs often land on one. (The
failures above had no slow syncs in the cluster group, just a busy machine.)
It's a race, so it's run a few times over, each time on a new cluster:
  - INIT, then three JOINs back to back, each answered OK;
  - and the cluster group ends up with four voters.
The old build had a JOIN refused in one round in four or so, which ten rounds
all but always catch.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=19660)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

ROUNDS = 10
SLOW = {"BARCH_TEST_LOG_SYNC_DELAY_MS": "150"}
failures = 0
busy = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def join(n, leader):
    """what node n's JOIN said, and how long it took"""
    t = time.time()
    try:
        said = str(n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(leader.port)))
    except redis.ResponseError as e:
        said = "ERR " + str(e)
    return said, time.time() - t


def voters(n, count):
    line = n.group_line("cluster")
    return ("members %d" % count) in line and "learners 0" in line


for r in range(ROUNDS):
    print("round %d: four nodes, every log sync 150ms, three JOINs back to back" % (r + 1))
    nodes = [clusternodes.Node(i, BASE, BINARY, env=SLOW) for i in range(4)]
    try:
        for n in nodes:
            n.start()
        nodes[0].client().execute_command("CLUSTER", "INIT")
        joined = True
        for n in nodes[1:]:
            said, took = join(n, nodes[0])
            joined = joined and said in ("OK", "True")
            check(said in ("OK", "True"), "node %d joins (%.1fs, %s)" % (n.i + 1, took, said[:40]))
        # one that wasn't let in won't turn up, so don't wait for it
        if joined:
            check(wait_for(60, lambda: voters(nodes[0], 4)), "the cluster group has four voters")
    finally:
        for n in nodes:
            n.stop()
    # NuRaft says so in the leader's log each time it turns a change down
    with open(nodes[0].dir + ".log", errors="replace") as f:
        busy += f.read().count("previous config has not committed yet")

# not checked, it's a race, but it shows the JOINs above had something to wait for
print("the leader turned a change down %d time%s for another still going" % (busy, "" if busy == 1 else "s"))
print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
