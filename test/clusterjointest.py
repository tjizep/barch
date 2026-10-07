#!/usr/bin/env python3
"""
Snapshots, catching up from one, and joining under load - TODO 611.

With raft_snapshot_entries small, a replicated space's group saves its spaces and
compacts its log every few hundred entries. What has to hold:

  - a follower kept down while the log is compacted past it comes back, copies a
    snapshot from the leader, and then holds the same copy as the others;
  - a node restarted after a snapshot applies only the entries after it, not the
    whole log;
  - a fourth node that joins while four clients write is added as a learner,
    made a voter once it has caught up, and ends with the same copy as the rest,
    every acknowledged write in it.

Ports are BARCH_TEST_PORT and the 15 after it: four RESP ports, and four raft bases
with two groups each.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=24900)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
EVERY = 200             # raft_snapshot_entries
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=%d" % EVERY]) for i in range(4)]
three = nodes[:3]


def write(n, count, prefix):
    l = leader_of(three, SPACE)
    c = l.client(SPACE)
    for i in range(count):
        c.execute_command("SET", "%s%d" % (prefix, i), str(i))


def same_copy(group):
    def done():
        applied = [n.field(SPACE, "applied") for n in group]
        if len(set(applied)) != 1 or applied[0] <= 0:
            return False
        return len(set(n.digest(SPACE)[0] for n in group)) == 1
    return done


try:
    print("a cluster of three that snapshots every %d entries" % EVERY)
    for n in three:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in three[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(40, lambda: leader_of(three, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in three)),
          "three voters in the cluster group and in %s's" % SPACE)

    print("a follower is down while the log is compacted past it")
    write(three, 100, "a")
    leader = leader_of(three, SPACE)
    down = [n for n in three if n is not leader][0]
    down_at = down.field(SPACE, "applied")
    down.kill()
    write(three, scale.scaled(1500, 6 * EVERY), "b")
    check(wait_for(30, lambda: leader.field(SPACE, "snapshot") > down_at
                   and leader.field(SPACE, "log_start") > down_at + 1),
          "the leader snapshotted and compacted its log past where it was (%d, %d > %d)"
          % (leader.field(SPACE, "snapshot"), leader.field(SPACE, "log_start"), down_at))
    down.start()
    check(wait_for(60, same_copy(three)), "it comes back and holds the same copy as the others")
    check(down.field(SPACE, "snapshots_installed") >= 1,
          "by copying a snapshot (%d installed)" % down.field(SPACE, "snapshots_installed"))

    print("a restart after a snapshot")
    write(three, 50, "c")
    check(wait_for(30, lambda: down.field(SPACE, "snapshot") > 0), "the node has a snapshot of its own")
    wait_for(30, same_copy(three))
    down.stop()
    down.start()
    check(wait_for(60, same_copy(three)), "it comes back and holds the same copy")
    snap = down.field(SPACE, "snapshot")
    applied = down.field(SPACE, "applied")
    here = down.field(SPACE, "applied_here")
    check(0 < snap and here <= applied - snap,
          "applying only what came after its snapshot (%d entries, snapshot %d, applied %d)"
          % (here, snap, applied))

    print("a fourth node joins while four clients write")
    writers = clusternodes.Writers(nodes, SPACE)
    writers.start()
    time.sleep(scale.scaled_seconds(1.0))
    fourth = nodes[3]
    fourth.start()
    joined = fourth.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[1].port))
    check(joined == "OK", "it joins through a node that may not lead")
    check(wait_for(60, lambda: all("members 4" in n.group_line(SPACE) for n in nodes)),
          "and is added to %s's group" % SPACE)
    check(wait_for(60, lambda: all("learners 0" in n.group_line(SPACE)
                                   and "learners 0" in n.group_line("cluster") for n in nodes)),
          "and made a voter in both groups")
    time.sleep(scale.scaled_seconds(1.0))
    writers.stop()
    print("    acknowledged %d, uncertain %d, told %s" % (len(writers.acked), len(writers.maybe),
                                                          writers.errors))
    check(len(writers.acked) > 0, "writes went on while it joined")
    check(wait_for(60, same_copy(nodes)), "all four hold the same copy")
    l = leader_of(nodes, SPACE)
    c = l.client(SPACE)
    missing = [k for k, v in writers.acked.items() if c.execute_command("GET", k) != v]
    check(not missing, "with every acknowledged write in it (%d missing)" % len(missing))
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
