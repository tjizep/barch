#!/usr/bin/env python3
"""
A cluster group on slow disks keeps up with heartbeats, and its nodes stop promptly
- TODO 642.

Every node sends CLUSTER HEARTBEAT to the cluster group's leader (every 500ms in the
tests), and the leader writes each as a Raft commit for `node:<id>:seen` plus one per
space. With the followers' syncs slowed, they came faster than they committed, RESP
threads piled up waiting on them, and at shutdown, with the followers gone first, each
sat out NuRaft's 3s timeout: node 1 "didn't stop within 30s". Now the leader writes one
heartbeat per node at a time and skips one that comes while the last is still being
written (the next carries the whole state), and a node that's shutting down sends and
takes no heartbeats and refuses new Raft writes before its server stops.

BARCH_TEST_LOG_SYNC_DELAY_MS makes every log sync on the two followers 400ms slower,
the cluster group's included.

What has to hold:
  - the followers become voters in every group, the cluster group included (TODO 653:
    the promotion rule didn't allow for a slow disk);
with eight clients writing:
  - every node's `seen` keeps moving on: heartbeats still get written;
  - every acknowledged write is there;
  - 100 heartbeats for one node at once (what a follower's resends add up to: it waits
    3s for an answer, then sends again): the leader writes one and answers the rest at
    once, rather than starting 100 Raft commits;
  - with those still being written, each node stops within 5s of being asked (without
    TODO 642's changes, up to 6.8s here; at most 1.7s with them).

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
from clusternodes import wait_for, leader_of, on_leader  # noqa: E402

scale.workdir()
BASE = scale.port(default=30100)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
SLOW_MS = int(os.environ.get("BARCH_TEST_SLOW_SYNC_MS", "1000"))
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


slow = {"BARCH_TEST_LOG_SYNC_DELAY_MS": str(SLOW_MS)}      # every group, the cluster's too
nodes = [clusternodes.Node(i, BASE, BINARY, env=None if i == 0 else slow) for i in range(3)]
try:
    print("three nodes, the followers' syncs %dms slower in every group" % SLOW_MS)
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        # a join copies the cluster's spaces and waits to be added, every sync on the
        # joining node a second slower: past the 30s a test client waits, under TSan
        slow_client = redis.Redis(host="127.0.0.1", port=n.port, socket_timeout=180,
                                  socket_connect_timeout=5, decode_responses=True)
        # and with a slow member already in, the join's own call to the leader can
        # time out ("timed out waiting for the reply"); an operator sends it again
        for attempt in range(4):
            try:
                slow_client.execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
                break
            except redis.ResponseError as e:
                print("    JOIN for node %d, try %d: %s" % (n.i + 1, attempt + 1, e), flush=True)
                time.sleep(2)
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    # every sync on two of three nodes a second slower: under load, a learner can be
    # more than 3s behind for a while, and is promoted once it isn't - TODO 654
    check(wait_for(240, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    # the cluster group too: with the old promotion rule a follower on a slow disk was
    # never made a voter, and the leader voted alone - TODO 653
    check(wait_for(60, lambda: all("learners 0" in n.group_line("cluster") for n in nodes)),
          "and three voters in the cluster group (%s)" % [n.field("cluster", "learners") for n in nodes])
    ids = [n.info()[0].split()[1] for n in nodes]

    print("eight clients write")
    w = clusternodes.Writers(nodes, SPACE, count=8, prefix="b")
    w.start()
    time.sleep(scale.scaled_seconds(8.0, floor=6.0))

    def seen():
        def read(n):
            c = n.client("cluster")
            return [int(c.execute_command("GET", "node:%s:seen" % i) or 0) for i in ids]
        return on_leader(nodes, "cluster", read, seconds=30)

    # every node's seen moves on: heartbeats still get written. Not an age: under TSan
    # on this disk each entry is slow, and the leader's own heartbeat waits its turn on
    # its tick, so they're tens of seconds apart there - TODO 654
    first = seen() or [0, 0, 0]
    later = []

    def moved():
        later[:] = seen() or []
        return len(later) == 3 and all(b > a for a, b in zip(first, later))

    check(wait_for(60, moved), "every node's seen moves on: heartbeats get written (%s then %s)"
          % (first, later))
    w.stop()
    print("    acknowledged %d, uncertain %d, told %s" % (len(w.acked), len(w.maybe), w.errors))
    missing = on_leader(nodes, SPACE, lambda n: sum(
        1 for k, v in w.acked.items() if n.client(SPACE).execute_command("GET", k) != v))
    missing = len(w.acked) if missing is None else missing
    check(len(w.acked) > 0 and missing == 0, "every acknowledged write is there (%d missing)" % missing)

    print("100 heartbeats for one node arrive at once, as a follower's resends would")
    boss = leader_of(nodes, "cluster")
    conns = [boss.client() for _ in range(100)]
    replied = []
    lock = threading.Lock()

    def beat(c):
        t = time.time()
        try:
            c.execute_command("CLUSTER", "HEARTBEAT", ids[1])
        except redis.RedisError:
            pass
        with lock:
            replied.append(time.time() - t)

    ts = [threading.Thread(target=beat, args=(c,), daemon=True) for c in conns]
    for t in ts:
        t.start()
    time.sleep(0.5)
    with lock:
        quick = sum(1 for d in replied if d < 0.5)
    check(quick >= 90, "the leader writes one and answers the rest at once (%d of 100 within 0.5s)" % quick)

    print("the nodes stop, the followers first, with those heartbeats still being written")
    took = []
    for n in reversed(nodes):
        t = time.time()
        n.stop()
        took.append(round(time.time() - t, 1))
    check(max(took) < 5 and not clusternodes.bad_exits,
          "each stops within 5s (%s, the followers first; %s)" % (took, clusternodes.bad_exits))
    del clusternodes.bad_exits[:]
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
