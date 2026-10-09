#!/usr/bin/env python3
"""
Routing a replicated space's reads and writes - TODO 613.

What has to hold:
  - a follower's NOTLEADER names the leader and the group's epoch (its term), and
    CLUSTER ROUTES on every node says the same;
  - a connection that asked for follower reads, told the index of a write with
    CLUSTER AFTER, reads that write on a follower, every time;
  - a follower that hears from no leader refuses follower reads;
  - a leader paused past its lease, while the others elect another and take a
    write, doesn't answer with the value from before that write when it resumes.

Ports are BARCH_TEST_PORT and the 15 after it.
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import scale  # noqa: E402
import redis  # noqa: E402
import clusternodes  # noqa: E402
from clusternodes import wait_for, leader_of  # noqa: E402

scale.workdir()
BASE = scale.port(default=25300)
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
try:
    print("a replicated space in a cluster of three")
    for n in nodes:
        n.start()
    nodes[0].client().execute_command("CLUSTER", "INIT")
    for n in nodes[1:]:
        n.client().execute_command("CLUSTER", "JOIN", "127.0.0.1", str(nodes[0].port))
    nodes[0].client("configuration").execute_command("SET", SPACE + ".raft", "on")
    check(wait_for(60, lambda: leader_of(nodes, SPACE) is not None and all(
        "members 3" in n.group_line(SPACE) and "learners 0" in n.group_line(SPACE)
        and "learners 0" in n.group_line("cluster") for n in nodes)), "three voters")
    # let a leader handover settle before reading the epoch
    time.sleep(2)
    leader = leader_of(nodes, SPACE)
    followers = [n for n in nodes if n is not leader]
    term = leader.field(SPACE, "term")

    print("redirects and routes")
    try:
        followers[0].client(SPACE).execute_command("GET", "x")
        check(False, "a follower refuses a read")
    except redis.ResponseError as e:
        check(str(e) == "NOTLEADER 127.0.0.1:%d %d" % (leader.port, term),
              "a follower's NOTLEADER names the leader and epoch (%s)" % e)
    want = "%s group 1 leader 127.0.0.1:%d epoch %d" % (SPACE, leader.port, term)
    routes = [n.client().execute_command("CLUSTER", "ROUTES") for n in nodes]
    check(all(want in r for r in routes), "CLUSTER ROUTES on every node says the same (%s)" % want)

    print("follower reads see the writes they were told about")
    w = leader.client(SPACE)
    f = followers[0].client(SPACE)
    check(f.execute_command("CLUSTER", "READS", "FOLLOWER") == "OK", "a connection asks for follower reads")
    rounds = scale.scaled(300, 50)
    stale = 0
    for i in range(rounds):
        w.execute_command("SET", "k", str(i))
        at = w.execute_command("CLUSTER", "INDEX", SPACE)
        f.execute_command("CLUSTER", "AFTER", SPACE, str(at))
        if f.execute_command("GET", "k") != str(i):
            stale += 1
    check(stale == 0, "%d rounds of write on the leader, read on a follower: %d stale" % (rounds, stale))
    at = int(f.execute_command("CLUSTER", "INDEX", SPACE))
    check(at >= rounds, "and the follower connection's index moved with its reads (%d)" % at)
    try:
        f.execute_command("SET", "k", "x")
        check(False, "a follower still refuses a write")
    except redis.ResponseError as e:
        check(str(e).startswith("NOTLEADER"), "a follower still refuses a write")

    print("a leader paused past its lease")
    w.execute_command("SET", "lease", "old")
    old = leader
    held = old.client(SPACE)               # opened before the pause, used after it
    old.pause()
    check(wait_for(30, lambda: (lambda l: l is not None and l is not old)(leader_of(followers, SPACE))),
          "the other two elect a new leader")
    new = leader_of(followers, SPACE)
    new.client(SPACE).execute_command("SET", "lease", "new")
    old.resume()
    try:
        got = held.execute_command("GET", "lease")
        check(got == "new", "the old leader answers the new value, not the old one (%s)" % got)
    except redis.ResponseError as e:
        check(not str(e).startswith("ERR"), "the old leader refuses the read instead (%s)" % e)
    check(wait_for(10, lambda: old.field(SPACE, "term") > term), "and learns of the new term")

    print("a follower that hears from no leader")
    last = leader_of(nodes, SPACE)
    lone = [n for n in nodes if n is not last][0]
    others = [n for n in nodes if n is not lone]
    for n in others:
        n.stop()
    time.sleep(2)
    c = lone.client(SPACE)
    c.execute_command("CLUSTER", "READS", "FOLLOWER")
    try:
        c.execute_command("GET", "k")
        check(False, "refuses follower reads")
    except redis.ResponseError as e:
        check(re.match(r"NOTLEADER|TRYAGAIN", str(e)) is not None, "refuses follower reads (%s)" % e)
finally:
    for n in nodes:
        if n.proc:
            n.resume()
        n.stop()

print("FAILED" if failures else "all passed")
sys.exit(1 if failures else 0)
