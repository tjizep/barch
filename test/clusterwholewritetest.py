#!/usr/bin/env python3
"""
A multi-field write goes in a replicated space's log as one entry - TODO 738.

An HSET of twenty fields used to be twenty entries, one a field, since each insert
under the command's shard lock committed on its own. The leader held the lock
throughout, so a read there saw all of it or none, but a follower applied the
entries one at a time and could be read between them. Now the command's writes
under that lock are held back and go in as one entry as the lock is let go, and a
member applies that entry under one hold of the shard.

The leader overwrites one hash, all twenty fields at once, again and again, while
clients on both followers read it with CLUSTER READS FOLLOWER.

What has to hold:
  - one HSET of twenty fields moves the group's log on by one entry, not twenty;
  - no follower read ever sees fields from two different HSETs;
  - after the writes, every member holds the last one (their digests match);
  - an RPUSH of several values reads whole on a follower too.

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
BASE = scale.port(default=32100)
BINARY = os.environ.get("BARCHD", os.path.join(os.getcwd(), "barchd"))
if not os.path.exists(BINARY):
    print("SKIP: no barchd at %s" % BINARY)
    sys.exit(0)

SPACE = "whole"
FIELDS = 20
ROUNDS = 400
failures = 0


def check(ok, what):
    global failures
    print("  %-66s %s" % (what, "pass" if ok else "FAIL"), flush=True)
    if not ok:
        failures += 1


def follower_client(n):
    c = n.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    return c


def settled(nodes):
    top = leader_of(nodes, SPACE).field(SPACE, "applied")
    return wait_for(30, lambda: all(n.field(SPACE, "applied") >= top for n in nodes))


def hash_of(c, key):
    """HGETALL as a dict: some redis-py versions hand back the flat list"""
    got = c.execute_command("HGETALL", key)
    return dict(zip(got[0::2], got[1::2])) if isinstance(got, list) else got


def fields(k):
    out = []
    for f in range(FIELDS):
        out += ["f%d" % f, "v%d" % k]
    return out


nodes = [clusternodes.Node(i, BASE, BINARY) for i in range(3)]
try:
    print("a replicated space on three nodes")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(90, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE) for n in nodes)),
        "three voters and a leader")
    leader = leader_of(nodes, SPACE)
    followers = [n for n in nodes if n is not leader]
    c = leader.client(SPACE)

    # the first write makes the hash, so it's measured on the second
    c.execute_command("HSET", "h", *fields(0))
    check(settled(nodes), "every member has applied the first write")
    before = leader.field(SPACE, "applied")
    c.execute_command("HSET", "h", *fields(1))
    check(settled(nodes), "every member has applied the second")
    moved = leader.field(SPACE, "applied") - before
    check(1 <= moved <= 2, "an HSET of %d fields is one entry (the log moved %d)" % (FIELDS, moved))

    torn = []
    reads = [0]
    stop = threading.Event()

    def read_on(n):
        r = follower_client(n)
        while not stop.is_set():
            got = hash_of(r, "h")
            reads[0] += 1
            values = set(got.values())
            if len(values) > 1 or len(got) != FIELDS:
                torn.append((n.i + 1, sorted(values)))

    readers = [threading.Thread(target=read_on, args=(n,)) for n in followers for _ in range(2)]
    for t in readers:
        t.start()
    print("%d HSETs of %d fields on node %d, read on the followers meanwhile" % (ROUNDS, FIELDS, leader.i + 1))
    for k in range(2, ROUNDS + 2):
        c.execute_command("HSET", "h", *fields(k))
    stop.set()
    for t in readers:
        t.join(30)
    check(reads[0] > 0, "the followers were read (%d reads)" % reads[0])
    check(not torn, "no follower read saw two HSETs at once (%d did%s)"
          % (len(torn), ", e.g. node %d: %s" % (torn[0][0], torn[0][1][:4]) if torn else ""))
    check(settled(nodes), "every member has applied the writes")
    last = "v%d" % (ROUNDS + 1)
    check(all(set(hash_of(follower_client(n), "h").values()) == {last} for n in nodes),
          "every member holds the last HSET")
    check(wait_for(30, lambda: len(set(n.digest(SPACE)[0] for n in nodes)) == 1), "every member's digest matches")

    # a list's values as one entry too: each RPUSH adds ten, so a whole read is a
    # multiple of ten long
    torn_list = []
    stop.clear()

    def list_on(n):
        r = follower_client(n)
        while not stop.is_set():
            got = r.execute_command("LLEN", "l")
            if got % 10:
                torn_list.append((n.i + 1, got))

    readers = [threading.Thread(target=list_on, args=(n,)) for n in followers]
    for t in readers:
        t.start()
    for k in range(200):
        c.execute_command("RPUSH", "l", *["x%d" % j for j in range(10)])
    stop.set()
    for t in readers:
        t.join(30)
    check(not torn_list, "no follower read saw part of an RPUSH (%d did%s)"
          % (len(torn_list), ", e.g. node %d: %d long" % torn_list[0] if torn_list else ""))
    check(settled(nodes) and all(follower_client(n).llen("l") == 2000 for n in nodes),
          "every member holds all 2000 values")
finally:
    for n in nodes:
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
