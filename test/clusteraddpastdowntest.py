#!/usr/bin/env python3
"""
A member that's down doesn't keep the others out of a new group - TODO 793.

When a space is replicated, its groups start on one node and their leader adds the
cluster's other nodes one at a time, in the order the cluster space lists them (by
node id). An add that failed ended that tick's turn for the group, so with a down
node listed first, every tick tried it again and the live node after it never got
its turn. The groups ran with their leader as the only voter, acknowledging writes
with one copy, until the down node came back.

The add that failed also held the cluster's tick thread for its 5s timeout - TODO
794. Nothing else in the tick ran meanwhile, for any group, and that includes the
node's own heartbeat. So node 1's heartbeat times in the cluster space (`seen`, every
500ms here) are watched the whole time, and a gap of seconds is a stalled tick.

The nodes' ids are chosen here so the cluster space lists node 3 before node 2. What
has to hold, on three nodes with node 3 stopped while a space is split over three
groups:
  - node 2 is a voter in each of them within 30s, with node 3 still down;
  - once node 3 is back, all three are voters in each;
  - and once the groups are led, node 1's heartbeats are never more than 2s apart.

Ports are BARCH_TEST_PORT and the 19 after it.
"""
import os
import re
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for  # noqa: E402

scale.workdir()
BASE = scale.port(default=23900)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
LABELS = ["%s/%d" % (SPACE, k) for k in range(3)]
# KEYS lists node:<id> in id order: node 3, node 1, then node 2
IDS = ["1000000000000001", "f000000000000002", "0000000000000003"]
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
for n, node_id in zip(nodes, IDS):
    with open(os.path.join(n.dir, "repl_node.id"), "w") as f:
        f.write(node_id + "\n")


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def led(n, label):
    return re.search(r"\bleader [1-9]", line_of(n, label)) is not None


def voters(label, count):
    line = line_of(nodes[0], label)
    return ("members %d " % count) in line and "learners 0" in line


class Heartbeats:
    """the longest wait between new values of node 1's `seen`, read every 50ms"""

    def __init__(self, node, node_id):
        self.node = node
        self.key = "node:%s:seen" % node_id
        self.longest = 0.0
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        c = self.node.client("cluster")
        last, since = None, time.time()
        while not self.stopping.is_set():
            try:
                v = c.get(self.key)
            except redis.RedisError:
                v = last
            now = time.time()
            if v != last:
                if last is not None:
                    self.longest = max(self.longest, now - since)
                last, since = v, now
            time.sleep(0.05)
        self.longest = max(self.longest, time.time() - since)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopping.set()
        self.thread.join()


try:
    print("three nodes, and node 3 stopped")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    check(wait_for(60, lambda: all("members 3" in n.group_line("cluster") for n in nodes)),
          "the cluster group has all three")
    nodes[2].stop()

    print("a space split over three groups while it's down")
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "3")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: all(led(nodes[0], l) for l in LABELS)), "three groups, led, on node 1")
    # from here: starting the groups holds the tick too, ~650ms each for its first
    # election, but that's not a membership change
    beats = Heartbeats(nodes[0], IDS[0])
    beats.start()
    started = time.time()
    ok = wait_for(30, lambda: all(voters(l, 2) for l in LABELS))
    check(ok, "node 2 is a voter in each, with node 3 down (%.1fs)" % (time.time() - started))

    print("node 3 back")
    nodes[2].start()
    check(wait_for(90, lambda: all(voters(l, 3) for l in LABELS)), "all three are voters in each")
    beats.stop()
    check(beats.longest < 2.0, "node 1's heartbeats were never 2s apart (longest %.1fs)" % beats.longest)
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
