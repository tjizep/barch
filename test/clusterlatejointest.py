#!/usr/bin/env python3
"""
A member that was down when a space was first replicated opens it with the cluster's
shard count when it comes back, even when a client reads the space first - TODO 792.

TestClusterRestartShards covers a member that was in the space's groups before it
went down: each group's own file keeps the space's count, and the member reads it
back before its listener opens (TODO 791). A member that was down when the space
started being replicated never joined those groups, so it has no such file. Until its
cluster group catches up with the leader, it doesn't know the space is replicated at
all, and a read in that gap opens the space with the default 17 shards.

What has to hold, on three nodes, with node 3 stopped while a space is split over
three groups at 32 shards and written to:
  - with a client reading the space on node 3 from the moment it answers, node 3
    joins each of the space's groups, with 32 shards;
  - its copy matches the other two;
  - and its log doesn't say its copy won't match.
Done twice, on a fresh cluster each time, since the read wins the race most of the
time but not every time.

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
BASE = scale.port(default=23500)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
LABELS = ["%s/%d" % (SPACE, k) for k in range(3)]
SHARDS = int(os.environ.get("BARCH_TEST_RAFT_SHARDS", "32"))
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = []


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def shards_of(n, label):
    m = re.search(r"\bspace_shards (\d+)", line_of(n, label))
    return int(m.group(1)) if m else -1


def led(n, label):
    return re.search(r"\bleader [1-9]", line_of(n, label)) is not None


def same_copies():
    return len(set(n.digest(SPACE)[0] for n in nodes)) == 1


class Reader:
    """CLUSTER DIGEST on a node as fast as it answers, from before it's started"""

    def __init__(self, node):
        self.node = node
        self.answers = 0
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stopping.is_set():
            try:
                c = redis.Redis(host="127.0.0.1", port=self.node.port, socket_timeout=5,
                                socket_connect_timeout=0.2, decode_responses=True)
                while not self.stopping.is_set():
                    c.execute_command("CLUSTER", "DIGEST", SPACE)
                    self.answers += 1
            except redis.RedisError:
                time.sleep(0.005)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopping.set()
        self.thread.join()


def log_says(n, what):
    with open(n.dir + ".log", "rb") as f:
        return what.encode() in f.read()


def late_join():
    global nodes
    # a fresh cluster each time: every node has room for four groups' ports
    nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    check(wait_for(60, lambda: all("members 3" in n.group_line("cluster") for n in nodes)),
          "the cluster group has all three")
    late = nodes[2]
    late.stop()

    print("the space split over three groups while it's down")
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "3")
    conf.execute_command("SET", SPACE + ".raft", "on")
    # node 1 starts each group, and node 2 joins them within a few seconds (TODO 793)
    check(wait_for(90, lambda: all(led(nodes[0], l) for l in LABELS)), "three groups, led, on node 1")
    check(all(shards_of(nodes[0], l) == SHARDS for l in LABELS), "every group has %d shards there" % SHARDS)
    w = clusternodes.Writers(nodes[:1], SPACE, count=4, prefix="k")
    w.start()
    time.sleep(scale.scaled_seconds(2.0))
    w.stop()
    check(len(w.acked) > 0, "keys written (%d acknowledged)" % len(w.acked))

    print("node 3 started again while a client reads the space there")
    r = Reader(late)
    r.start()
    late.start()
    ok = wait_for(90, lambda: all("members 3" in line_of(n, l) for n in nodes for l in LABELS)
                  and all(led(late, l) for l in LABELS))
    r.stop()
    check(ok, "it's in each of the space's groups, read %d times meanwhile" % r.answers)
    got = [shards_of(late, l) for l in LABELS]
    check(got == [SHARDS] * 3, "each with %d shards (%s)" % (SHARDS, got))
    check(wait_for(90, same_copies), "and its copy matches the other two")
    check(not log_says(late, "its copy here won't match"), "and its log doesn't say it won't match")
    for n in nodes:
        n.stop()


try:
    # twice: the read wins the race most of the time, not every time
    for round in (1, 2):
        print("three nodes, and node 3 stopped, round %d" % round)
        late_join()
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
