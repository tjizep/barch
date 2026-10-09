#!/usr/bin/env python3
"""
A member rebuilding its copy of a space - TODO 621.

A leader paused with a write in flight comes back not knowing whether that write
committed, so it rebuilds its copy of the space from the new leader. While it does,
its space has to keep refusing writes, as a joining node's does. It used to be
unbound for the length of the copy, so a client still writing to it got plain local
writes: acknowledged, and never replicated.

What has to hold:
  - the paused leader rebuilds its copy;
  - a client that keeps writing to that node, whatever it's told, has no write
    acknowledged that the cluster doesn't have;
  - every acknowledged write is on the leader, and every member ends with the same
    copy.

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
BASE = scale.port(default=26700)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "orders"
failures = 0


def check(ok, what):
    global failures
    print("  %-62s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]


def rebuilds(n):
    try:
        return open(n.dir + ".log").read().count("rebuilding group")
    except OSError:
        return 0


class Hasher:
    """HSETs to the leader. A composite write applies before it commits, so it's what
    leaves a leader with an outcome it doesn't know, and a rebuild to do - TODO 632:
    single-key writes take theirs back out of the tree and let the log decide"""

    def __init__(self, node, prefix):
        self.node = node
        self.prefix = prefix
        self.acked = {}
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.run)

    def run(self):
        n = 0
        cl = None
        while not self.stopping.is_set():
            field = "%s-%d" % (self.prefix, n)
            n += 1
            try:
                if cl is None:
                    cl = self.node.client(SPACE)
                cl.execute_command("HSET", "h-" + self.prefix, field, field)
                self.acked[field] = field
            except redis.ResponseError:
                time.sleep(0.002)
            except redis.RedisError:
                cl = None
                time.sleep(0.05)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopping.set()
        self.thread.join()


class Sticky:
    """writes to one node only, and keeps at it whatever it's told"""

    def __init__(self, node, prefix):
        self.node = node
        self.prefix = prefix
        self.acked = {}
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self.run)

    def run(self):
        n = 0
        cl = None
        while not self.stopping.is_set():
            key = "%s-%d" % (self.prefix, n)
            n += 1
            try:
                if cl is None:
                    cl = self.node.client(SPACE)
                cl.execute_command("SET", key, key)
                self.acked[key] = key
            except redis.ResponseError:
                time.sleep(0.002)
            except redis.RedisError:
                cl = None
                time.sleep(0.05)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopping.set()
        self.thread.join()


try:
    print("a replicated space on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")

    print("the leader is paused under writes, and a client stays with it")
    w = clusternodes.Writers(nodes, SPACE, count=4, prefix="w")
    w.start()
    stickies = []
    hashers = []
    rebuilt = None
    for attempt in range(5):
        l = leader_of(nodes, SPACE)
        if l is None:
            time.sleep(1)
            continue
        before = rebuilds(l)
        s = Sticky(l, "s%d" % attempt)
        s.start()
        stickies.append(s)
        hs = [Hasher(l, "h%d-%d" % (attempt, j)) for j in range(4)]
        for h in hs:
            h.start()
        hashers.extend(hs)
        time.sleep(scale.scaled_seconds(1.0))
        l.pause()
        wait_for(30, lambda: leader_of([n for n in nodes if n is not l], SPACE) is not None)
        time.sleep(0.5)
        l.resume()
        if wait_for(15, lambda: rebuilds(l) > before):
            rebuilt = l
        time.sleep(scale.scaled_seconds(2.0))
        s.stop()
        for h in hs:
            h.stop()
        if rebuilt:
            break
    w.stop()
    print("    acknowledged %d, uncertain %d, told %s; to the paused node: %s" % (
        len(w.acked), len(w.maybe), w.errors, [len(s.acked) for s in stickies]))
    check(rebuilt is not None, "the paused leader rebuilt its copy")

    print("what the cluster holds")
    check(wait_for(30, lambda: leader_of(nodes, SPACE) is not None), "a leader")
    c = leader_of(nodes, SPACE).client(SPACE)
    acked = dict(w.acked)
    for s in stickies:
        acked.update(s.acked)
    lost_sticky = sum(1 for s in stickies for k, v in s.acked.items() if c.execute_command("GET", k) != v)
    lost = sum(1 for k, v in acked.items() if c.execute_command("GET", k) != v)
    check(lost_sticky == 0, "the client that stayed lost nothing it was told was written (%d)" % lost_sticky)
    lost_fields = sum(1 for h in hashers for f, v in h.acked.items()
                      if c.execute_command("HGET", "h-" + h.prefix, f) != v)
    check(lost_fields == 0, "and no acknowledged HSET is missing (%d of %d)"
          % (lost_fields, sum(len(h.acked) for h in hashers)))
    check(len(acked) > 0 and lost == 0, "every acknowledged write is on the leader (%d, %d missing)"
          % (len(acked), lost))
    check(wait_for(60, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1),
          "every member holds the same copy")
    if failures:
        for n in nodes:
            print("    node %d: %s | digest %s" % (n.i + 1, n.group_line(SPACE), n.digest(SPACE)))
finally:
    for n in nodes:
        try:
            n.resume()
        except Exception:
            pass
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
