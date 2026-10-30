#!/usr/bin/env python3
"""
A SAVE of the cluster space and a heartbeat batch don't deadlock - TODO 795.

The cluster group's leader writes each heartbeat as one batch, and marks the batch's
shards of the cluster space as committing until it's in (`command_begin`), so a write
that keeps its latch through its commit can't block it. A mark takes the shard's latch
for a moment. A SAVE of the space takes every shard's write latch in shard order and,
at a shard that's committing, lets that one go and waits, still holding the ones
before it. The group's snapshot is such a SAVE.

The batch marked its shards in the order of its writes. So it could mark a shard, then
wait for the latch of an earlier one that the SAVE held, while the SAVE waited for the
mark to go: both for good. A node hung like that in TestClusterUsers under TSan, its
heartbeats and the cluster space stopped, and it didn't stop within 30s.

Every log sync here is 700ms slower, heartbeats come every 250ms, and two Raft spaces
give each heartbeat three records. A SAVE waits out a sync at each shard that's
committing, which leaves plenty of time for a batch to come in between. A client
keeps SAVEing the cluster space on its leader. What has to hold:
  - each SAVE finishes, within 20s;
  - heartbeats still land afterwards: every node's `seen` moves on;
  - and every node stops within 30s (checked when they're stopped).

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
BASE = scale.port(default=24200)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACES = ("orders", "stock")
SYNC_MS = 700
WINDOW_S = 20
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def seen(leader):
    try:
        c = leader.client("cluster")
        return {k: c.execute_command("GET", k) for k in c.execute_command("KEYS", "node:*:seen")}
    except redis.RedisError:
        return {}       # a hung leader times out here


class Saver:
    """SAVEs of the cluster space on its leader, one after the other, timing each"""

    def __init__(self, node):
        self.node = node
        self.took = []
        self.errors = []
        self.stopping = threading.Event()
        # a daemon, so a SAVE that never comes back doesn't keep the test from exiting
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        c = self.node.client("cluster")
        while not self.stopping.is_set():
            started = time.time()
            try:
                c.execute_command("SAVE")
            except redis.RedisError as e:
                self.errors.append("%s: %s" % (type(e).__name__, e))
                return
            self.took.append(time.time() - started)


nodes = [clusternodes.Node(i, BASE, BINARY, settings=["cluster_heartbeat_ms=250"],
                           env={"BARCH_TEST_LOG_SYNC_DELAY_MS": str(SYNC_MS)})
         for i in range(3)]
try:
    print("three nodes and two spaces, every log sync %dms, heartbeats every 250ms" % SYNC_MS)
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    for s in SPACES:
        nodes[0].client("configuration").execute_command("SET", s + ".raft", "on")
    check(wait_for(120, lambda: all(
        "members 3" in n.group_line(g) and "learners 0" in n.group_line(g)
        for n in nodes for g in ("cluster",) + SPACES)), "three voters in every group")

    leader = leader_of(nodes, "cluster")
    check(leader is not None, "the cluster group has a leader")
    print("SAVEs of the cluster space on node %d for %ds" % (leader.i + 1, WINDOW_S))
    saver = Saver(leader)
    saver.thread.start()
    time.sleep(WINDOW_S)
    saver.stopping.set()
    saver.thread.join(40)
    check(not saver.thread.is_alive() and not saver.errors,
          "each SAVE came back (%d, errors %s)" % (len(saver.took), saver.errors[:1]))
    longest = max(saver.took or [0])
    check(len(saver.took) > 0 and longest < 20, "each SAVE took under 20s (longest %.1fs)" % longest)

    before = seen(leader)

    def moved():
        now = seen(leader)
        return len(now) == 3 and all(now[k] != v for k, v in before.items())
    check(len(before) == 3 and wait_for(30, moved), "every node's seen moves on: heartbeats still land")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
