#!/usr/bin/env python3
"""
A member back from a rebuild catches up - TODO 691.

Seen once, under TSan: a cluster leader had a commit time out, rebuilt group 0, and its
new instance denied the new leader's first append (the leader was ahead of its log) and
then caught up with nothing for 90 s. This tries to make that happen: the node leading
the cluster group is paused under writes to both a replicated space and
`configuration` (group 0), long enough for the others to elect a new leader, so it
comes back unsure of what it wrote and rebuilds. Eight rounds, with several writers
on each so that writes are in flight when it stops.

What has to hold, each round: within 60 s of the paused node coming back, every member
has applied as far as the leader in both groups.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=35700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
ROUNDS = int(os.environ.get("CATCHUP_ROUNDS", "8"))
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def caught_up(members, space):
    l = leader_of(members, space)
    if l is None:
        return False
    top = l.field(space, "applied")
    return all(n.field(space, "applied") >= top for n in members)


nodes = [clusternodes.Node(i, BASE, BINARY, ["raft_snapshot_entries=50"]) for i in range(3)]
try:
    print("three nodes and a replicated space")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters and a leader")

    stop = threading.Event()

    def writer(space, prefix):
        i = 0
        while not stop.is_set():
            for n in nodes:
                if n.proc is None:
                    continue
                try:
                    n.client(space).execute_command("SET", "%s%d" % (prefix, i % 200), str(i))
                    break
                except redis.RedisError:
                    continue
            i += 1
            time.sleep(0.005)

    ts = [threading.Thread(target=writer, args=(SPACE, "k%d_" % w)) for w in range(2)] + \
         [threading.Thread(target=writer, args=("configuration", "probe%d.k" % w)) for w in range(3)]
    [t.start() for t in ts]
    for r in range(ROUNDS):
        lead = leader_of(nodes, "cluster")
        if lead is None:
            time.sleep(1)
            lead = leader_of(nodes, "cluster")
        print("round %d: node %d leads the cluster group; paused for 4 s" % (r + 1, lead.i + 1))
        lead.pause()
        time.sleep(4)
        lead.resume()
        started = time.time()
        ok = wait_for(60, lambda: caught_up(nodes, "cluster") and caught_up(nodes, SPACE))
        check(ok, "round %d: every member catches up in both groups (%.1f s)" % (r + 1, time.time() - started))
        if not ok:
            for n in nodes:
                print("    node %d: %s | %s" % (n.i + 1, n.group_line("cluster")[:120], n.group_line(SPACE)[:120]))
    stop.set()
    [t.join() for t in ts]
    rebuilds = sum(open(n.dir + ".log", errors="replace").read().count("rebuilding group") for n in nodes)
    print("    %d rebuilds over %d rounds" % (rebuilds, ROUNDS))
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
