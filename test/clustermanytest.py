#!/usr/bin/env python3
"""
Several replicated spaces led by different nodes, a node killed under writes to all
of them, and a member that copies key by key - TODO 612.

What has to hold:
  - three spaces each get a group, and the groups' leaders spread over the three
    nodes;
  - with writers on every space, killing a node loses no acknowledged write in
    any of them, and once it's back every member holds the same copy of each;
  - a fourth node whose layout tag differs (BARCH_TEST_LAYOUT) joins while clients
    write, copies every space key by key rather than page by page, and ends with
    the same copy of each as the rest.

Ports are BARCH_TEST_PORT and the 19 after it: four RESP ports, and four raft
bases with four groups each.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=25100)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACES = ["alpha", "beta", "gamma"]
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


# snapshots every 200 entries, so the log is compacted long before the fourth node
# joins and what it holds has to come from its copy, not from the log
SETTINGS = ["raft_snapshot_entries=200"]
nodes = [clusternodes.Node(i, BASE, BINARY, SETTINGS) for i in range(3)]
nodes.append(clusternodes.Node(3, BASE, BINARY, SETTINGS, env={"BARCH_TEST_LAYOUT": "test-other-layout"}))
three = nodes[:3]


def same_copy(group, space):
    def done():
        applied = [n.field(space, "applied") for n in group]
        if len(set(applied)) != 1 or applied[0] <= 0:
            return False
        return len(set(n.digest(space)[0] for n in group)) == 1
    return done


def missing(writers, group, space):
    l = leader_of(group, space)
    c = l.client(space)
    return [k for k, v in writers.acked.items() if c.execute_command("GET", k) != v]


try:
    print("three spaces in a cluster of three")
    for n in three:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in three[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    for s in SPACES:
        conf.execute_command("SET", s + ".raft", "on")
    check(wait_for(60, lambda: all("members 3" in n.group_line(s) and "learners 0" in n.group_line(s)
                                   for n in three for s in SPACES + ["cluster"])),
          "each gets a group of three voters")
    check(wait_for(60, lambda: len({leader_of(three, s) for s in SPACES} - {None}) == 3),
          "and their leaders are three different nodes")
    print("    leaders:", {s: (leader_of(three, s).i + 1 if leader_of(three, s) else None) for s in SPACES})

    print("a node is killed while clients write to every space")
    writers = {s: clusternodes.Writers(three, s, count=2, prefix=s[0]) for s in SPACES}
    for w in writers.values():
        w.start()
    time.sleep(scale.scaled_seconds(2.0))
    victim = nodes[1]
    led = [s for s in SPACES if leader_of(three, s) is victim]
    victim.kill()
    check(wait_for(30, lambda: all(leader_of(three, s) not in (None, victim) for s in SPACES)),
          "every space has a leader among the other two (node 2 led %s)" % led)
    time.sleep(scale.scaled_seconds(2.0))
    for w in writers.values():
        w.stop()
    for s in SPACES:
        lost = missing(writers[s], three, s)
        check(len(writers[s].acked) > 0 and not lost,
              "%s: every acknowledged write is there (%d, %d missing)" % (s, len(writers[s].acked), len(lost)))
    victim.start()
    for s in SPACES:
        check(wait_for(60, same_copy(three, s)), "%s: node 2 comes back and the three copies match" % s)

    print("a fourth node with another page layout joins under writes")
    w = clusternodes.Writers(nodes, SPACES[0], count=2, prefix="j")
    w.start()
    time.sleep(scale.scaled_seconds(1.0))
    fourth = nodes[3]
    fourth.start()
    check(all(n.field(s, "log_start") > 1 for n in three for s in SPACES),
          "every group's log has been compacted")
    check(fourth.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port)) == "OK",
          "it joins")
    check(wait_for(90, lambda: all("members 4" in n.group_line(s) and "learners 0" in n.group_line(s)
                                   for n in nodes for s in SPACES + ["cluster"])),
          "and is a voter in every group")
    time.sleep(scale.scaled_seconds(1.0))
    w.stop()
    copies = int(fourth.info()[1].split("logical_copies ")[1].split()[0])
    check(copies >= 2 + len(SPACES),
          "having copied the cluster, configuration and every data space key by key (%d)" % copies)
    for s in SPACES:
        check(wait_for(60, same_copy(nodes, s)), "%s: all four copies match" % s)
    lost = missing(w, nodes, SPACES[0])
    check(not lost, "%s: every write acknowledged while it joined is there (%d, %d missing)"
          % (SPACES[0], len(w.acked), len(lost)))
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
