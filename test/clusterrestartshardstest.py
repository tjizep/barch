#!/usr/bin/env python3
"""
A member that comes back opens a replicated space with the cluster's shard count,
even when a client reads the space first - TODO 791.

A node killed before it saved knows about a space only from its cluster group's log,
and doesn't have that until the group catches up with its leader, a few hundred ms
after the node starts listening. A read in between (TestClusterSplit's CLUSTER DIGEST
did it) opened the space with the default 17 shards, against the cluster's 32. The
node then bound its groups to that copy, with the shard runs worked out for 17, and
its copy never matched the others again.

What has to hold, on three nodes with a space split over three groups:
  - with a client reading the space on node 2 from the moment it answers, a kill and
    a restart of node 2 leave each of its groups with the cluster's 32 shards;
  - its copy, which missed writes while it was down, matches the other two;
  - and its log doesn't say its copy won't match.
Done twice, since the read wins the race most of the time but not every time.

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
BASE = scale.port(default=23300)
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


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]


def line_of(n, label):
    for l in n.info():
        if (" label %s" % label) in l:
            return l
    return ""


def shards_of(n, label):
    m = re.search(r"\bspace_shards (\d+)", line_of(n, label))
    return int(m.group(1)) if m else -1


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


try:
    print("a space split over three groups")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    conf = nodes[0].client("configuration")
    conf.execute_command("SET", SPACE + ".raft_groups", "3")
    conf.execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: all("members 3" in line_of(n, l) and re.search(r"\bleader [1-9]", line_of(n, l))
                                   for n in nodes for l in LABELS)),
          "three groups, three voters each, led")
    check(all(shards_of(n, l) == SHARDS for n in nodes for l in LABELS),
          "every group has %d shards on every node" % SHARDS)
    w = clusternodes.Writers(nodes, SPACE, count=4, prefix="k")
    w.start()
    time.sleep(scale.scaled_seconds(2.0))
    w.stop()
    check(len(w.acked) > 0 and wait_for(60, same_copies),
          "keys written and the copies match (%d acknowledged)" % len(w.acked))

    victim = nodes[1]
    for round in (1, 2):
        print("node 2 killed and started again while a client reads, round %d" % round)
        victim.kill()
        # and writes it misses, so its copy has to catch up to match
        w = clusternodes.Writers([n for n in nodes if n is not victim], SPACE, count=2, prefix="r%d-" % round)
        w.start()
        time.sleep(scale.scaled_seconds(1.0))
        w.stop()
        r = Reader(victim)
        r.start()
        victim.start()
        ok = wait_for(60, lambda: all(re.search(r"\bleader [1-9]", line_of(victim, l)) for l in LABELS))
        r.stop()
        check(ok and r.answers > 0, "its groups are back, read %d times meanwhile" % r.answers)
        got = [shards_of(victim, l) for l in LABELS]
        check(got == [SHARDS] * 3, "each of its groups has %d shards (%s)" % (SHARDS, got))
        check(wait_for(90, same_copies), "and its copy matches the other two")
        check(not log_says(victim, "its copy here won't match"), "and its log doesn't say it won't match")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
